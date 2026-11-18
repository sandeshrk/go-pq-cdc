package slot

import (
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestDecodeSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name, snapshot, lsn string
		valid               bool
	}{
		{"boundary", "00000003-00000002-1", "0/1234", true},
		{"missing snapshot", "", "0/1234", false},
		{"invalid LSN", "snapshot", "invalid", false},
		{"zero LSN", "snapshot", "0/0", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := &pgconn.Result{
				FieldDescriptions: []pgconn.FieldDescription{{Name: "snapshot_name"}, {Name: "consistent_point"}},
				Rows:              [][][]byte{{[]byte(tc.snapshot), []byte(tc.lsn)}},
			}
			snapshot, err := decodeSnapshot([]*pgconn.Result{result})
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.snapshot, snapshot.Name)
			require.Equal(t, pq.LSN(0x1234), snapshot.LSN)
		})
	}
	for _, results := range [][]*pgconn.Result{nil, {{}}, {{FieldDescriptions: []pgconn.FieldDescription{{Name: "snapshot_name"}}, Rows: [][][]byte{{}}}}} {
		_, err := decodeSnapshot(results)
		require.Error(t, err)
	}
}
