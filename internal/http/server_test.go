package http

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/Trendyol/go-pq-cdc/logger"
)

// TestShutdownDoesNotPanicOnError guards against B2: Shutdown used to panic
// when the underlying http.Server.Shutdown returned an error, meaning a
// graceful shutdown could itself crash the process -- the exact failure
// mode it exists to avoid. The error is already logged; that must be
// enough.
func TestShutdownDoesNotPanicOnError(t *testing.T) {
	logger.InitLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))

	s := &server{
		shutdownFunc: func(context.Context) error {
			return errors.New("boom")
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Shutdown panicked on a shutdown error: %v", r)
		}
	}()

	s.Shutdown()

	if !s.closed {
		t.Fatal("expected Shutdown to mark the server closed even when the underlying shutdown errors")
	}
}

func TestShutdownDoesNotPanicOnSuccess(t *testing.T) {
	s := &server{
		shutdownFunc: func(context.Context) error {
			return nil
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Shutdown panicked on a successful shutdown: %v", r)
		}
	}()

	s.Shutdown()
}

func TestShutdownOnNilServerDoesNotPanic(t *testing.T) {
	var s *server

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Shutdown panicked on a nil server: %v", r)
		}
	}()

	s.Shutdown()
}
