package cdc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
)

func TestMain(m *testing.M) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.Run()
}

func TestSuperviseReturnsNilOnCleanAttempt(t *testing.T) {
	var calls atomic.Int32
	err := supervise(context.Background(), SuperviseOpts{}, func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 attempt, got %d", got)
	}
}

func TestSuperviseReturnsNonRetryableErrorImmediately(t *testing.T) {
	nonRetryable := errors.New("bad password")
	var calls atomic.Int32
	err := supervise(context.Background(), SuperviseOpts{InitialBackoff: time.Hour}, func(context.Context) error {
		calls.Add(1)
		return nonRetryable
	})
	if !errors.Is(err, nonRetryable) {
		t.Fatalf("expected the non-retryable error back, got %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 attempt (no retry), got %d", got)
	}
}

func TestSuperviseRetriesRetryableErrorThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	err := supervise(context.Background(), SuperviseOpts{InitialBackoff: time.Millisecond, MaxBackoff: 5 * time.Millisecond}, func(context.Context) error {
		n := calls.Add(1)
		if n < 3 {
			return context.DeadlineExceeded // classified retryable by IsRetryableStartupError
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected eventual success, got %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("expected exactly 3 attempts, got %d", got)
	}
}

func TestSuperviseStopsPromptlyOnContextCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := supervise(ctx, SuperviseOpts{InitialBackoff: time.Hour}, func(context.Context) error {
		calls.Add(1)
		return context.DeadlineExceeded
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil on context cancellation, got %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("context cancellation should interrupt the backoff wait almost immediately, took %s", elapsed)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 attempt before the cancel fired, got %d", got)
	}
}

func TestSuperviseStopsWhenContextAlreadyCancelledAfterRetryableAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := supervise(ctx, SuperviseOpts{}, func(context.Context) error {
		return context.DeadlineExceeded
	})
	if err != nil {
		t.Fatalf("expected nil when ctx is already cancelled, got %v", err)
	}
}

func TestSuperviseInvokesOnRetryHook(t *testing.T) {
	type call struct {
		attempt int
		backoff time.Duration
	}
	var got []call

	calls := 0
	err := supervise(context.Background(), SuperviseOpts{
		InitialBackoff: time.Millisecond,
		MaxBackoff:     4 * time.Millisecond,
		OnRetry: func(attempt int, _ error, backoff time.Duration) {
			got = append(got, call{attempt, backoff})
		},
	}, func(context.Context) error {
		calls++
		if calls < 3 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected OnRetry called exactly twice (2 failed attempts before success), got %d: %+v", len(got), got)
	}
	if got[0].attempt != 1 || got[1].attempt != 2 {
		t.Fatalf("expected attempt numbers 1 then 2, got %+v", got)
	}
}

func TestSuperviseOptsDefaults(t *testing.T) {
	o := SuperviseOpts{}.withDefaults()
	if o.InitialBackoff != 250*time.Millisecond {
		t.Fatalf("expected default InitialBackoff 250ms, got %s", o.InitialBackoff)
	}
	if o.MaxBackoff != 30*time.Second {
		t.Fatalf("expected default MaxBackoff 30s, got %s", o.MaxBackoff)
	}
}
