package cdc

import (
	"context"
	"errors"
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
		readyCh:  make(chan struct{}, 1),
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
