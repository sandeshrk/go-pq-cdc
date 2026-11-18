package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/require"
)

// The slot export needs one idle walsender until all per-chunk imports finish.
// Once the snapshot ends, only the live replication stream should remain.
func TestSnapshotExportSessionLifetime(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := Config
	cfg.Slot.Name = "slot_export_lifetime"
	cfg.Publication.Name = "pub_export_lifetime"
	table := "snapshot_export_lifetime"
	cfg.Publication.Tables = publication.Tables{{Name: table, Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull}}
	cfg.Snapshot.Enabled = true
	cfg.Snapshot.Mode = "initial"
	cfg.Snapshot.ChunkSize = 1
	admin, err := newPostgresConn()
	require.NoError(t, err)
	defer admin.Close(context.Background())
	require.NoError(t, createTestTable(ctx, admin, table))
	require.NoError(t, pgExec(ctx, admin, fmt.Sprintf("INSERT INTO %s SELECT n, $$row$$, n FROM generate_series(1,3) n", table)))
	t.Cleanup(func() { cleanupSnapshotTest(t, context.Background(), table, cfg.Slot.Name, cfg.Publication.Name) })
	observed := make(chan int, 1)
	stream := make(chan struct{}, 1)
	var countErr error
	connector, err := cdc.NewConnector(ctx, cfg, func(lc *replication.ListenerContext) {
		switch m := lc.Message.(type) {
		case *format.Snapshot:
			if m.EventType == format.SnapshotEventTypeBegin {
				var count int
				count, countErr = countWalsendersForSlot(ctx, admin, cfg.Slot.Name)
				observed <- count
			}
		case *format.Insert:
			stream <- struct{}{}
		}
		_ = lc.Ack()
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- connector.Run(ctx) }()
	defer func() { cancel(); require.ErrorIs(t, <-done, context.Canceled); connector.Close() }()
	require.NoError(t, connector.WaitUntilReady(ctx))
	require.NoError(t, countErr)
	require.Equal(t, 1, <-observed, "the slot export session must remain open during snapshot imports")
	count, err := countWalsendersForSlot(ctx, admin, cfg.Slot.Name)
	require.NoError(t, err)
	require.Equal(t, 1, count, "only the live stream remains after the export closes")
	require.NoError(t, pgExec(ctx, admin, fmt.Sprintf("INSERT INTO %s VALUES (4, $$live$$, 4)", table)))
	select {
	case <-stream:
	case <-ctx.Done():
		t.Fatal("stream did not deliver insert")
	}
}
