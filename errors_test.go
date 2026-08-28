package cdc

import (
	"context"
	goerrors "errors"
	"net"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsRetryableStartupError(t *testing.T) {
	tests := []struct {
		err  error
		name string
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{
			name: "wrapped dial failure",
			err:  errors.Wrap(&pgconn.ConnectError{Config: &pgconn.Config{}}, "postgres connection"),
			want: true,
		},
		{
			name: "server starting up",
			err:  errors.Wrap(&pgconn.PgError{Code: "57P03"}, "postgres connection"),
			want: true,
		},
		{
			name: "too many connections",
			err:  errors.Wrap(&pgconn.PgError{Code: "53300"}, "postgres connection"),
			want: true,
		},
		{
			name: "publication ddl hit lock_timeout",
			err:  errors.Wrap(&pgconn.PgError{Code: "55P03"}, "apply replica identities"),
			want: true,
		},
		{
			name: "permission denied is permanent",
			err:  errors.Wrap(&pgconn.PgError{Code: "42501"}, "publication create"),
			want: false,
		},
		{
			name: "tables not created yet",
			err:  publication.ErrorTablesNotExists,
			want: true,
		},
		{
			name: "attempt deadline",
			err:  errors.Wrap(context.DeadlineExceeded, "postgres connection"),
			want: true,
		},
		{
			name: "caller shutting down",
			err:  errors.Wrap(context.Canceled, "postgres connection"),
			want: false,
		},
		{
			name: "network error",
			err:  errors.Wrap(&net.DNSError{Err: "no such host", IsNotFound: true}, "postgres connection"),
			want: true,
		},
		{
			name: "config validation",
			err:  errors.Wrap(goerrors.New("host cannot be empty"), "config validation"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRetryableStartupError(tt.err); got != tt.want {
				t.Fatalf("IsRetryableStartupError() = %v, want %v", got, tt.want)
			}
		})
	}
}
