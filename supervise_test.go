package cdc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/prometheus/client_golang/prometheus"
)

// fakeSupervisedConnector is a minimal Connector fake for testing runAttempt's
// OnReady wiring without a real database. WaitUntilReady and Run each return
// their configured error immediately (no goroutine synchronization needed
// since callers already run WaitUntilReady on its own goroutine).
type fakeSupervisedConnector struct {
	readyErr error
	runErr   error
	closed   atomic.Bool
}

func (f *fakeSupervisedConnector) Start(ctx context.Context)                     { _ = f.Run(ctx) }
func (f *fakeSupervisedConnector) Run(context.Context) error                     { return f.runErr }
func (f *fakeSupervisedConnector) WaitUntilReady(context.Context) error          { return f.readyErr }
func (f *fakeSupervisedConnector) Close()                                        { f.closed.Store(true) }
func (f *fakeSupervisedConnector) GetConfig() *config.Config                     { return nil }
func (f *fakeSupervisedConnector) SetMetricCollectors(_ ...prometheus.Collector) {}

var _ Connector = (*fakeSupervisedConnector)(nil)

func waitForSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("expected OnReady to be called")
	}
}

func TestRunAttemptCallsOnReadyOnSuccessfulReady(t *testing.T) {
	fc := &fakeSupervisedConnector{}
	onReady := make(chan struct{}, 1)

	err := runAttempt(context.Background(), SuperviseOpts{OnReady: func() { onReady <- struct{}{} }}, func(context.Context) (Connector, error) {
		return fc, nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	waitForSignal(t, onReady)
	if !fc.closed.Load() {
		t.Fatal("expected the connector to be closed")
	}
}

func TestRunAttemptRecoversOnReadyPanic(t *testing.T) {
	fc := &fakeSupervisedConnector{}
	onReadyCalled := make(chan struct{})

	err := runAttempt(context.Background(), SuperviseOpts{OnReady: func() {
		close(onReadyCalled)
		panic("boom")
	}}, func(context.Context) (Connector, error) {
		return fc, nil
	})
	if err != nil {
		t.Fatalf("expected nil, a panicking OnReady must not surface as an attempt error, got %v", err)
	}

	select {
	case <-onReadyCalled:
	case <-time.After(time.Second):
		t.Fatal("expected OnReady to be called")
	}
	// If the panic weren't recovered, it would have already crashed the
	// whole test binary by now rather than merely failing an assertion.
	time.Sleep(50 * time.Millisecond)
}

func TestRunAttemptDoesNotCallOnReadyWhenNeverReady(t *testing.T) {
	fc := &fakeSupervisedConnector{
		readyErr: ErrConnectorClosedBeforeReady,
		runErr:   errors.New("bootstrap failed"),
	}
	onReady := make(chan struct{}, 1)

	err := runAttempt(context.Background(), SuperviseOpts{OnReady: func() { onReady <- struct{}{} }}, func(context.Context) (Connector, error) {
		return fc, nil
	})
	if err == nil {
		t.Fatal("expected the connector's Run error back")
	}
	select {
	case <-onReady:
		t.Fatal("OnReady must not be called when the connector never became ready")
	case <-time.After(100 * time.Millisecond):
	}
}

func TestRunAttemptWithNilOnReadyDoesNotPanic(t *testing.T) {
	fc := &fakeSupervisedConnector{}
	err := runAttempt(context.Background(), SuperviseOpts{}, func(context.Context) (Connector, error) {
		return fc, nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestRunAttemptPropagatesNewConnectorError(t *testing.T) {
	wantErr := errors.New("dial failed")
	err := runAttempt(context.Background(), SuperviseOpts{}, func(context.Context) (Connector, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the NewConnector error back, got %v", err)
	}
}

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
