package cdc

import (
	"context"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
)

// SuperviseOpts configures Supervise's retry backoff.
type SuperviseOpts struct {
	// InitialBackoff is the delay before the first retry. Defaults to 250ms.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential backoff between retries. Defaults to 30s.
	MaxBackoff time.Duration
	// OnRetry, if set, is called before each backoff wait, in addition to the
	// package's own logging.
	OnRetry func(attempt int, err error, backoff time.Duration)
}

func (o SuperviseOpts) withDefaults() SuperviseOpts {
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 250 * time.Millisecond
	}
	if o.MaxBackoff <= 0 {
		o.MaxBackoff = 30 * time.Second
	}
	return o
}

// Supervise runs a connector built from cfg and handler until ctx is
// cancelled or a non-retryable error occurs, rebuilding it from scratch
// after every retryable failure. This is the D1-a/D4 recipe: since a
// connector is one-shot (Run/Start never restart themselves), the
// application is what owns recovery from bootstrap failures and slot
// contention. Supervise implements that loop so callers do not each have to
// get its four subtleties right themselves:
//
//  1. A fresh connector every attempt -- no reset logic, so no in-process
//     state (A3's class of bugs) can leak across a rebuild.
//  2. The previous connector's Close() completes before the next
//     NewConnector, so the replication slot is released first.
//  3. Retryable errors (IsRetryableStartupError) back off and retry;
//     anything else (bad credentials, permissions) is returned immediately.
//  4. ctx cancellation (e.g. a caller's signal.NotifyContext) always ends the
//     loop cleanly, never as a retry.
//
// Supervise returns nil on a clean, signal- or ctx-driven shutdown, or the
// first non-retryable error. It does not return on a retryable error; it
// keeps retrying with exponential backoff until ctx is cancelled.
func Supervise(ctx context.Context, cfg config.Config, handler replication.ListenerFunc, opts SuperviseOpts) error {
	return supervise(ctx, opts, func(ctx context.Context) error {
		c, err := NewConnector(ctx, cfg, handler)
		if err != nil {
			return err
		}
		defer c.Close()
		return c.Run(ctx)
	})
}

// supervise is the retry/backoff engine behind Supervise, factored out so
// its exact semantics can be unit-tested against a fake attempt instead of a
// real database.
func supervise(ctx context.Context, opts SuperviseOpts, attempt func(ctx context.Context) error) error {
	opts = opts.withDefaults()
	backoff := opts.InitialBackoff

	for i := 1; ; i++ {
		err := attempt(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}
		if !IsRetryableStartupError(err) {
			logger.Error("supervise: non-retryable error, giving up", "error", err)
			return err
		}

		logger.Warn("supervise: retryable error, backing off", "attempt", i, "backoff", backoff.String(), "error", err)
		if opts.OnRetry != nil {
			opts.OnRetry(i, err, backoff)
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff = min(backoff*2, opts.MaxBackoff)
	}
}
