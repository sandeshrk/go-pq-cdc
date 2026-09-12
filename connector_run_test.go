package cdc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

// fatalErrStreamer is a minimal replication.Streamer fake whose Err()
// channel is controlled directly by the test; every other method is an
// unused no-op since waitForShutdownOrFatal never calls them.
type fatalErrStreamer struct{ errCh chan error }

func (fatalErrStreamer) Connect(context.Context) error           { return nil }
func (fatalErrStreamer) Open(context.Context) error              { return nil }
func (fatalErrStreamer) Close(context.Context) error             { return nil }
func (fatalErrStreamer) GetSystemInfo() *pq.IdentifySystemResult { return nil }
func (fatalErrStreamer) GetMetric() metric.Metric                { return nil }
func (fatalErrStreamer) OpenFromSnapshotLSN()                    {}
func (f fatalErrStreamer) Err() <-chan error                     { return f.errCh }

var _ replication.Streamer = fatalErrStreamer{}

func TestRunGuardedRecoversPanicAndReportsFatal(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	c := &connector{fatalCh: make(chan error, 1)}

	c.runGuarded("test goroutine", func() {
		panic("boom")
	})

	select {
	case err := <-c.fatalCh:
		if err == nil {
			t.Fatal("expected a non-nil error funnelled from the recovered panic")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("panic was not recovered and funnelled into fatalCh")
	}
}

func TestRunGuardedDoesNotBlockWhenFatalChIsFull(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	c := &connector{fatalCh: make(chan error, 1)}
	c.fatalCh <- errors.New("already full")

	done := make(chan struct{})
	go func() {
		c.runGuarded("second panic", func() { panic("also boom") })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runGuarded should not block when fatalCh has no room")
	}
}

func TestRunReturnsErrConnectorConsumedOnSecondCall(t *testing.T) {
	c := &connector{fatalCh: make(chan error, 1)}
	c.consumed.Store(true) // simulate a connector that already ran once

	err := c.Run(context.Background())
	if !errors.Is(err, ErrConnectorConsumed) {
		t.Fatalf("expected ErrConnectorConsumed, got %v", err)
	}
}

func TestCloseMarksConnectorConsumedForSubsequentRun(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	registry := metric.NewRegistry(metric.NewMetric("test_close_consumed"))
	c := &connector{
		fatalCh:  make(chan error, 1),
		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
		server:   http.NewServer(config.Config{Metric: config.MetricConfig{Port: 0}}, registry, nil),
	}

	c.Close()

	err := c.Run(context.Background())
	if !errors.Is(err, ErrConnectorConsumed) {
		t.Fatalf("expected ErrConnectorConsumed after Close(), got %v", err)
	}
}

// TestWaitForShutdownOrFatalReturnsStreamFatalError verifies that a fatal
// error reported by the replication stream (T3.3: replaces the panic that
// used to crash the process on an unrecoverable connection loss) is
// returned by Run(), the same way a recovered background-goroutine panic
// already is.
func TestWaitForShutdownOrFatalReturnsStreamFatalError(t *testing.T) {
	streamErrCh := make(chan error, 1)
	c := &connector{
		fatalCh:  make(chan error, 1),
		cancelCh: make(chan os.Signal, 1),
		stream:   fatalErrStreamer{errCh: streamErrCh},
	}

	wantErr := errors.New("replication stream corrupted")
	streamErrCh <- wantErr

	err := c.waitForShutdownOrFatal(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the stream's fatal error to be returned, got %v", err)
	}
}

// TestWaitForShutdownOrFatalReturnsNilOnCancel verifies a clean shutdown via
// cancelCh still returns nil, unaffected by the new stream.Err() case.
func TestWaitForShutdownOrFatalReturnsNilOnCancel(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	c := &connector{
		fatalCh:  make(chan error, 1),
		cancelCh: make(chan os.Signal, 1),
		stream:   fatalErrStreamer{errCh: make(chan error, 1)},
	}
	c.cancelCh <- os.Interrupt

	if err := c.waitForShutdownOrFatal(context.Background()); err != nil {
		t.Fatalf("expected nil on a clean cancel, got %v", err)
	}
}

// TestWaitUntilReadySucceedsOnGenuineReadySignal verifies WaitUntilReady
// returns nil once run() actually signals readiness.
func TestWaitUntilReadySucceedsOnGenuineReadySignal(t *testing.T) {
	c := &connector{readyCh: make(chan struct{}), closedCh: make(chan struct{})}
	close(c.readyCh)

	if err := c.WaitUntilReady(context.Background()); err != nil {
		t.Fatalf("expected nil on a genuine ready signal, got %v", err)
	}
}

// TestWaitUntilReadyReturnsErrConnectorClosedBeforeReady verifies that a
// Close() before the connector ever becomes ready (e.g. bootstrap failed) is
// distinguishable from a genuine ready signal -- Close() closes a dedicated
// closedCh to unblock waiters, entirely separate from readyCh (which only
// run() ever closes), so the two can never be confused for one another.
func TestWaitUntilReadyReturnsErrConnectorClosedBeforeReady(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	registry := metric.NewRegistry(metric.NewMetric("test_wait_ready_closed"))
	c := &connector{
		fatalCh:  make(chan error, 1),
		cancelCh: make(chan os.Signal, 1),
		readyCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
		server:   http.NewServer(config.Config{Metric: config.MetricConfig{Port: 0}}, registry, nil),
	}

	c.Close() // simulates a connector torn down before ever becoming ready

	err := c.WaitUntilReady(context.Background())
	if !errors.Is(err, ErrConnectorClosedBeforeReady) {
		t.Fatalf("expected ErrConnectorClosedBeforeReady, got %v", err)
	}
}

// TestCloseDoesNotRaceReadyChClose is a regression test for a TOCTOU flagged
// in review: Close() used to close readyCh itself (guarded by a racy
// isClosed peek-then-close), which could panic with "close of closed
// channel"/"send on closed channel" if Close() ran concurrently with run()
// signaling readiness. Close() and run() now close two entirely separate
// channels (closedCh and readyCh respectively), so running them concurrently
// must never panic, run under -race across many iterations to build
// confidence beyond a single lucky interleaving.
func TestCloseDoesNotRaceReadyChClose(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	for i := 0; i < 100; i++ {
		registry := metric.NewRegistry(metric.NewMetric(fmt.Sprintf("test_close_race_%d", i)))
		c := &connector{
			fatalCh:  make(chan error, 1),
			cancelCh: make(chan os.Signal, 1),
			readyCh:  make(chan struct{}),
			closedCh: make(chan struct{}),
			server:   http.NewServer(config.Config{Metric: config.MetricConfig{Port: 0}}, registry, nil),
		}

		done := make(chan struct{})
		go func() {
			defer close(done)
			close(c.readyCh) // simulates run() reaching bootstrap success concurrently
		}()

		c.Close()
		<-done
	}
}
