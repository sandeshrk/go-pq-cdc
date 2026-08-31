package pq

import (
	goerrors "errors"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
)

// IsRetryableConnectionError reports whether err indicates a transient
// PostgreSQL connection problem worth retrying, as opposed to a permanent
// condition (bad credentials, insufficient privilege, dropped database, ...)
// that will not resolve itself no matter how many times the connection is
// retried.
//
// Shared by cdc.IsRetryableStartupError (startup) and the replication
// stream's in-process reconnect loop (mid-stream): both need the same
// PgError/ConnectError classification, and pq/replication cannot import the
// cdc package without an import cycle.
//
// This does not unwrap a go-playground/errors Chain; callers holding a
// Chain-wrapped error must call errors.Cause(err) first.
func IsRetryableConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// Check for a specific Postgres error code first, even when it arrives
	// wrapped inside a *pgconn.ConnectError -- pgx wraps EVERY connection
	// handshake failure in ConnectError, including permanent rejections like
	// wrong credentials (28P01) or insufficient privilege (42501), not just
	// transient network blips. Checking ConnectError before PgError would
	// treat those permanent failures as retryable too.
	var pgErr *pgconn.PgError
	if goerrors.As(err, &pgErr) {
		switch pgErr.Code {
		case "57P03", // cannot_connect_now: server is starting up or in recovery
			"57P01", // admin_shutdown
			"57P02", // crash_shutdown
			"53300", // too_many_connections
			"53200", // out_of_memory
			"55P03", // lock_not_available
			"40P01", // deadlock_detected
			"08000", // connection_exception
			"08003", // connection_does_not_exist
			"08006", // connection_failure
			"3D000": // invalid_catalog_name: database dropped/not created
			return true
		}
		return false
	}

	var connErr *pgconn.ConnectError
	if goerrors.As(err, &connErr) {
		return true
	}

	var netErr net.Error
	return goerrors.As(err, &netErr)
}
