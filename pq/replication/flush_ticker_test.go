package replication

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/jackc/pgx/v5/pgproto3"
)

// lagCapturingMetric wraps a real metric.Metric and records the last value
// passed to SetUnackedLSNLag, so tests can assert on it without reaching into
// the metric package's unexported prometheus internals. Guarded by a mutex:
// the value is set from the stream's process/flushTicker goroutine and read
// from the test goroutine.
type lagCapturingMetric struct {
	metric.Metric
	mu      sync.Mutex
	lastLag float64
}

func (m *lagCapturingMetric) SetUnackedLSNLag(lag float64) {
	m.mu.Lock()
	m.lastLag = lag
	m.mu.Unlock()
	m.Metric.SetUnackedLSNLag(lag)
}

func (m *lagCapturingMetric) LastLag() float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLag
}

// syncBuffer is a bytes.Buffer safe for one writer goroutine and one reader
// goroutine, which is all these tests need (pgproto3.Frontend writes to it
// off the test goroutine that polls its contents).
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

func (b *syncBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

// TestFlushTickerSendsPeriodicStandbyStatusUpdate verifies that flushTicker
// writes a standby status update on its own schedule, independent of the read
// loop -- the fix for confirmed_flush_lsn stalling under sustained throughput
// when neither the sink's idle timeout nor a server keepalive fires.
func TestFlushTickerSendsPeriodicStandbyStatusUpdate(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	written := &syncBuffer{}
	conn := &standbyCaptureConn{
		fe: pgproto3.NewFrontend(strings.NewReader(""), written),
	}

	s := NewStream("", config.Config{LSNFlushInterval: 10 * time.Millisecond}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.conn = conn
	s.UpdateXLogPos(500)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.flushTicker(ctx)
	}()

	deadline := time.After(500 * time.Millisecond)
	for written.Len() == 0 {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("flushTicker did not send a standby status update within the deadline")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	<-done

	if !bytes.Contains(written.Bytes(), []byte{StandbyStatusUpdateByteID}) {
		t.Fatal("expected a standby status update payload")
	}
}

// TestFlushTickerSkipsWhenNothingReceivedYet verifies flushTicker does not
// write before any WAL position has been received (LoadXLogPos()==0), which
// would otherwise send a meaningless all-zero status update on every tick.
func TestFlushTickerSkipsWhenNothingReceivedYet(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	written := &syncBuffer{}
	conn := &standbyCaptureConn{
		fe: pgproto3.NewFrontend(strings.NewReader(""), written),
	}

	s := NewStream("", config.Config{LSNFlushInterval: 5 * time.Millisecond}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.conn = conn

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.flushTicker(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	<-done

	if written.Len() != 0 {
		t.Fatal("expected no standby status update before any WAL position was received")
	}
}

// TestUpdateUnackedLagMetric verifies the gap between the highest received
// and highest acked WAL position is reported, and drops back to zero once
// the consumer catches up.
func TestUpdateUnackedLagMetric(t *testing.T) {
	lm := &lagCapturingMetric{Metric: metric.NewMetric("test_slot")}
	s := NewStream("", config.Config{}, lm, func(*ListenerContext) {}).(*stream)

	s.UpdateXLogPos(100)
	s.UpdateConfirmedXLogPos(40)
	s.updateUnackedLagMetric()
	if got := lm.LastLag(); got != 60 {
		t.Fatalf("expected lag 60, got %v", got)
	}

	s.UpdateConfirmedXLogPos(100)
	s.updateUnackedLagMetric()
	if got := lm.LastLag(); got != 0 {
		t.Fatalf("expected lag 0 once fully acked, got %v", got)
	}
}

// TestAckUpdatesUnackedLagMetric verifies that acking a message through the
// process loop's Ack closure refreshes the lag metric immediately, rather
// than waiting for the next flush tick.
func TestAckUpdatesUnackedLagMetric(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	lm := &lagCapturingMetric{Metric: metric.NewMetric("test_slot")}
	s := NewStream("", config.Config{}, lm, func(ctx *ListenerContext) {
		_ = ctx.Ack()
	}).(*stream)
	s.UpdateXLogPos(500)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.process(ctx)

	s.messageCH <- &Message{message: "irrelevant", walStart: 100}

	deadline := time.After(200 * time.Millisecond)
	for lm.LastLag() != 400 {
		select {
		case <-deadline:
			t.Fatalf("lag metric was not updated to 400 after ack, last value %v", lm.LastLag())
		case <-time.After(5 * time.Millisecond):
		}
	}
}
