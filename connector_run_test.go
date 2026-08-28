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
)

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
