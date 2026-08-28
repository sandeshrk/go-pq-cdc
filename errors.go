package cdc

import (
	"context"
	goerrors "errors"
	"net"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
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

	// Check for a specific Postgres error code first, even when it arrives
	// wrapped inside a *pgconn.ConnectError -- pgx wraps EVERY startup
	// handshake failure in ConnectError, including permanent rejections like
	// wrong credentials (28P01) or insufficient privilege (42501), not just
	// transient network blips. Checking ConnectError before PgError (as this
	// used to) treats those permanent failures as retryable too.
	var pgErr *pgconn.PgError
	if goerrors.As(cause, &pgErr) {
		switch pgErr.Code {
		case "57P03", // cannot_connect_now: server is starting up or in recovery
			"57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"53300", // too_many_connections
			"53200", // out_of_memory
			"55P03", // lock_not_available: publication DDL hit lock_timeout
			"40P01", // deadlock_detected
			"08000", // connection_exception
			"08003", // connection_does_not_exist
			"08006", // connection_failure
			"3D000": // invalid_catalog_name: database not created yet
			return true
		}
		return false
	}

	var connErr *pgconn.ConnectError
	if goerrors.As(cause, &connErr) {
		return true
	}

	if goerrors.Is(cause, publication.ErrorTablesNotExists) {
		return true
	}

	var netErr net.Error
	return goerrors.As(cause, &netErr)
}
