package replication

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
)

// captureListenerContext runs process() against a fresh stream and returns the
// first ListenerContext delivered for msg (plus the stream itself, so tests
// can assert on state AckLSN/Ack mutate), without needing a real replication
// connection.
func captureListenerContext(t *testing.T, msg *Message) (*ListenerContext, *stream) {
	t.Helper()
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	captured := make(chan *ListenerContext, 1)
	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(ctx *ListenerContext) {
		captured <- ctx
	}).(*stream)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.process(ctx)

	s.messageCH <- msg

	select {
	case lCtx := <-captured:
		return lCtx, s
	case <-time.After(time.Second):
		t.Fatal("listenerFunc was not invoked within the deadline")
		return nil, nil
	}
}

// TestListenerContextExposesWALStartAndCommitLSN verifies T2.4: a DML
// message's own WAL position and stamped CommitLSN are surfaced on
// ListenerContext, not just baked into the Ack closure.
func TestListenerContextExposesWALStartAndCommitLSN(t *testing.T) {
	insert := &format.Insert{CommitLSN: pq.LSN(777)}
	lCtx, _ := captureListenerContext(t, &Message{message: insert, walStart: 1234})

	if lCtx.WALStart != pq.LSN(1234) {
		t.Fatalf("expected WALStart 1234, got %v", lCtx.WALStart)
	}
	if lCtx.CommitLSN != pq.LSN(777) {
		t.Fatalf("expected CommitLSN 777, got %v", lCtx.CommitLSN)
	}
}

// TestListenerContextCommitLSNZeroForNonDML verifies commitLSNOf returns 0 for
// message types that carry no commit context (e.g. Relation), while WALStart
// is still populated since it comes from the envelope, not the message itself.
func TestListenerContextCommitLSNZeroForNonDML(t *testing.T) {
	relation := &format.Relation{OID: 42}
	lCtx, _ := captureListenerContext(t, &Message{message: relation, walStart: 500})

	if lCtx.WALStart != pq.LSN(500) {
		t.Fatalf("expected WALStart 500, got %v", lCtx.WALStart)
	}
	if lCtx.CommitLSN != 0 {
		t.Fatalf("expected CommitLSN 0 for a non-DML message, got %v", lCtx.CommitLSN)
	}
}

// TestAckLSNAdvancesConfirmedPosition verifies T2.5: AckLSN confirms an
// arbitrary caller-supplied LSN through the same path Ack uses, independent
// of the message's own walStart.
func TestAckLSNAdvancesConfirmedPosition(t *testing.T) {
	lCtx, s := captureListenerContext(t, &Message{message: &format.Insert{}, walStart: 100})

	if err := lCtx.AckLSN(pq.LSN(900)); err != nil {
		t.Fatalf("AckLSN returned error: %v", err)
	}
	if got := s.LoadConfirmedXLogPos(); got != pq.LSN(900) {
		t.Fatalf("expected confirmed position 900, got %v", got)
	}
}

// TestAckLSNIsNoOpForNonMonotonicValue verifies a lower-or-equal AckLSN value
// never rewinds the confirmed position, matching UpdateConfirmedXLogPos's
// existing monotonic guard.
func TestAckLSNIsNoOpForNonMonotonicValue(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.UpdateConfirmedXLogPos(500)

	captured := make(chan *ListenerContext, 1)
	s.listenerFunc = func(ctx *ListenerContext) { captured <- ctx }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.process(ctx)

	s.messageCH <- &Message{message: &format.Insert{}, walStart: 100}
	lCtx := <-captured

	if err := lCtx.AckLSN(pq.LSN(200)); err != nil {
		t.Fatalf("AckLSN returned error: %v", err)
	}
	if got := s.LoadConfirmedXLogPos(); got != pq.LSN(500) {
		t.Fatalf("expected confirmed position to stay at 500 after a lower AckLSN, got %v", got)
	}

	if err := lCtx.AckLSN(pq.LSN(900)); err != nil {
		t.Fatalf("AckLSN returned error: %v", err)
	}
	if got := s.LoadConfirmedXLogPos(); got != pq.LSN(900) {
		t.Fatalf("expected confirmed position to advance to 900, got %v", got)
	}
}
