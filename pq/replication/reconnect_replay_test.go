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
)

// replayCapturingMetric wraps a real metric.Metric and counts calls to
// ReplayedMessageIncrement, guarded by a channel since it's set from the
// stream's process goroutine and read from the test goroutine.
type replayCapturingMetric struct {
	metric.Metric
	countCh chan struct{}
}

func newReplayCapturingMetric() *replayCapturingMetric {
	return &replayCapturingMetric{Metric: metric.NewMetric("test_slot"), countCh: make(chan struct{}, 1000)}
}

func (m *replayCapturingMetric) ReplayedMessageIncrement() {
	m.countCh <- struct{}{}
	m.Metric.ReplayedMessageIncrement()
}

func (m *replayCapturingMetric) count() int {
	// Give process() a moment to drain messageCH before counting.
	time.Sleep(20 * time.Millisecond)
	return len(m.countCh)
}

// TestMarkReplayFloorTracksDeliveredHighWaterMark verifies markReplayFloor
// captures whatever has been delivered to the listener so far.
func TestMarkReplayFloorTracksDeliveredHighWaterMark(t *testing.T) {
	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)

	if got := s.loadReplayFloor(); got != 0 {
		t.Fatalf("expected replay floor 0 before any delivery/reconnect, got %v", got)
	}

	s.updateDeliveredHighWaterMark(500)
	s.markReplayFloor()

	if got := s.loadReplayFloor(); got != pq.LSN(500) {
		t.Fatalf("expected replay floor 500, got %v", got)
	}
}

// TestMarkReplayFloorNeverDecreases verifies a later reconnect can only raise
// the replay floor, even if the delivered high-water mark it reads at that
// moment happens to be lower than a previously recorded floor (e.g. a second
// disconnect before the consumer caught back up past the first floor).
func TestMarkReplayFloorNeverDecreases(t *testing.T) {
	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)

	s.updateDeliveredHighWaterMark(500)
	s.markReplayFloor()
	if got := s.loadReplayFloor(); got != pq.LSN(500) {
		t.Fatalf("expected replay floor 500, got %v", got)
	}

	// Simulate re-reading a lower high-water mark directly (deliveredHighWaterMark
	// itself is monotonic via updateDeliveredHighWaterMark, so this bypasses that
	// guard on purpose to prove markReplayFloor has its own).
	s.mu.Lock()
	s.deliveredHighWaterMark = 300
	s.mu.Unlock()
	s.markReplayFloor()

	if got := s.loadReplayFloor(); got != pq.LSN(500) {
		t.Fatalf("expected replay floor to stay at 500, got %v", got)
	}
}

// TestReconnectSetsReplayFloorOnSuccess verifies a successful in-process
// reconnect records the replay floor from whatever was delivered so far.
func TestReconnectSetsReplayFloorOnSuccess(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s := newTestReconnectStream(config.ReconnectConfig{
		InitialDelay: time.Millisecond,
		MaxDelay:     time.Millisecond,
		MaxElapsed:   time.Second,
	})
	s.conn = newReconnectFakeConn(0)
	s.updateDeliveredHighWaterMark(1000)

	if !s.reconnect(context.Background()) {
		t.Fatal("expected reconnect to succeed against a fake conn with no connect failures")
	}

	if got := s.loadReplayFloor(); got != pq.LSN(1000) {
		t.Fatalf("expected replay floor 1000 after a successful reconnect, got %v", got)
	}
}

// TestProcessCountsReplayedMessagesBelowFloor verifies process() counts a
// delivered message as replayed exactly when its walStart is at or below the
// replay floor left by the last reconnect, and not once walStart moves past it.
func TestProcessCountsReplayedMessagesBelowFloor(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	rm := newReplayCapturingMetric()
	s := NewStream("", config.Config{}, rm, func(*ListenerContext) {}).(*stream)

	// Simulate a reconnect that happened after delivering up through LSN 300.
	s.updateDeliveredHighWaterMark(300)
	s.markReplayFloor()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.process(ctx)

	s.messageCH <- &Message{message: "irrelevant", walStart: 100} // replay
	s.messageCH <- &Message{message: "irrelevant", walStart: 300} // replay (floor is inclusive)
	s.messageCH <- &Message{message: "irrelevant", walStart: 500} // new data, not a replay

	if got := rm.count(); got != 2 {
		t.Fatalf("expected 2 replayed messages counted, got %v", got)
	}
}
