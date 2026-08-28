package cdc

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
)

// CaptureSlot polls until the slot goes idle; a cancelled context must break
// that loop instead of spinning for the rest of the process lifetime.
func TestCaptureSlotStopsOnContextCancel(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	sl := slot.NewSlot("", "", slot.Config{Name: "test_slot", SlotActivityCheckerInterval: 1000}, nil, nil)
	c := &connector{slot: sl}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		c.CaptureSlot(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("CaptureSlot did not return after context cancellation")
	}
}
