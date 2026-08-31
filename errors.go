package cdc

import (
	"context"
	goerrors "errors"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
)

// IsRetryableStartupError reports whether a NewConnector error is worth
// retrying: the database is unreachable, still starting up, or the tables the
// publication needs have not been created yet (common when migrations run
// after this process boots).
//
// Errors returned by this package are wrapped in a go-playground/errors Chain,
// which has no Unwrap method, so the standard errors.Is/As never see the cause
// on their own. This unwraps first, so callers do not have to know that.
func IsRetryableStartupError(err error) bool {
	if err == nil {
		return false
	}

	cause := errors.Cause(err)

	// A cancelled parent context means the caller is shutting down, not that
	// the database is unavailable; a deadline is the caller's own attempt
	// budget and is worth another try.
	if goerrors.Is(cause, context.Canceled) {
		return false
	}
	if goerrors.Is(cause, context.DeadlineExceeded) {
		return true
	}

	if goerrors.Is(cause, publication.ErrorTablesNotExists) {
		return true
	}

	// PgError/ConnectError/net.Error classification is shared with the
	// replication stream's mid-stream reconnect loop; see
	// pq.IsRetryableConnectionError.
	return pq.IsRetryableConnectionError(cause)
}
