package snapshot

import (
	"fmt"
	"testing"

	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestInvalidSnapshotError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil"},
		{name: "connection failure", err: &pgconn.ConnectError{Config: &pgconn.Config{}}},
		{name: "invalidation sentinel", err: ErrSnapshotInvalidated, want: true},
		{name: "standard wrapper around chain", err: fmt.Errorf("worker: %w", errors.Wrap(&pgconn.PgError{Code: "42704", Message: `snapshot "expired" does not exist`}, "import")), want: true},
		{name: "missing export", err: &pgconn.PgError{Code: "42704", Message: `snapshot "expired" does not exist`}, want: true},
		{name: "wrapped missing export", err: errors.Wrap(fmt.Errorf("import: %w", &pgconn.PgError{Code: "42704", Message: `snapshot "expired" does not exist`}), "worker"), want: true},
		{name: "invalid identifier", err: &pgconn.PgError{Code: "22023", Message: "invalid snapshot identifier: bad"}, want: true},
		{name: "missing publication", err: &pgconn.PgError{Code: "42704", Message: `publication "expired" does not exist`}},
		{name: "unrelated parameter", err: &pgconn.PgError{Code: "22023", Message: "invalid parameter"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsInvalidSnapshotError(tc.err))
		})
	}
}
