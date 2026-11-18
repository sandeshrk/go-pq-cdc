package snapshot

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/stretchr/testify/require"
)

type coordinatorWaitLogger struct {
	logger.Logger
	waiting chan struct{}
	once    sync.Once
}

func (l *coordinatorWaitLogger) Debug(msg string, args ...any) {
	if msg == "[worker] waiting for coordinator" {
		l.once.Do(func() { close(l.waiting) })
	}
	l.Logger.Debug(msg, args...)
}

// Invoked by the integration suite so this package can shorten its private poll
// period without exposing a production timeout or requiring Docker in unit runs.
func TestWorkerWaitsForBlockedSlotCreation(t *testing.T) {
	dsn := os.Getenv("CDC_REVIEW_WAIT_DSN")
	if dsn == "" {
		t.Skip("requires integration suite database")
	}
	// Not parallel: the private poll period is process-wide.
	previous := coordinatorWaitInterval
	coordinatorWaitInterval = time.Millisecond
	t.Cleanup(func() { coordinatorWaitInterval = previous })
	waiting := make(chan struct{})
	logger.InitLogger(&coordinatorWaitLogger{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), waiting: waiting})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pq.NewConnection(ctx, dsn)
	require.NoError(t, err)
	defer admin.Close(context.Background())
	const name = "review_blocked_slot"
	require.NoError(t, pq.ExecSQL(ctx, admin, "CREATE TABLE "+name+" (id integer primary key); INSERT INTO "+name+" VALUES (1), (2)"))
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		require.NoError(t, pq.ExecSQL(cleanupCtx, admin, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $$"+name+"$$; DROP TABLE "+name+"; DELETE FROM cdc_snapshot_chunks WHERE slot_name = $$"+name+"$$; DELETE FROM cdc_snapshot_job WHERE slot_name = $$"+name+"$$"))
	}()
	var cfg config.Config
	cfg.Snapshot.Enabled = true
	cfg.Snapshot.Mode = config.SnapshotModeInitial
	cfg.Snapshot.ChunkSize = 1
	cfg.Slot.Name = name
	cfg.Slot.CreateIfNotExists = true
	cfg.SetDefault()
	tables := publication.Tables{{Name: name, Schema: "public"}}
	newSnapshotter := func() (*Snapshotter, *slot.Slot) {
		s, err := New(ctx, cfg.Snapshot, tables, dsn, metric.NewMetric(name))
		require.NoError(t, err)
		sl := slot.NewSlot(os.Getenv("CDC_REVIEW_WAIT_REPLICATION_DSN"), dsn, cfg.Slot, metric.NewMetric(name), nil)
		return s, sl
	}
	coordinator, coordinatorSlot := newSnapshotter()
	defer func() { coordinator.Close(context.Background()); coordinatorSlot.Close(context.Background()) }()
	worker, workerSlot := newSnapshotter()
	defer func() { worker.Close(context.Background()); workerSlot.Close(context.Background()) }()
	// This assigned transaction ID makes logical slot initialization wait.
	blocker, err := pq.NewConnection(ctx, dsn)
	require.NoError(t, err)
	defer blocker.Close(context.Background())
	require.NoError(t, pq.ExecSQL(ctx, blocker, "BEGIN; SELECT pg_current_xact_id()"))
	coordinatorDone := make(chan error, 1)
	go func() { coordinatorDone <- coordinator.PrepareWithSlot(ctx, name, coordinatorSlot) }()
	require.Eventually(t, func() bool {
		results, err := admin.Exec(ctx, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE query LIKE $$CREATE_REPLICATION_SLOT%review_blocked_slot%$$ AND wait_event_type = $$Lock$$)").ReadAll()
		return err == nil && string(results[0].Rows[0][0]) == "t"
	}, 5*time.Second, time.Millisecond)
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.PrepareWithSlot(ctx, name, workerSlot) }()
	select {
	case <-waiting:
	case err := <-workerDone:
		t.Fatalf("worker never waited for the coordinator: %v", err)
	case <-ctx.Done():
		t.Fatal("worker never entered its coordinator wait")
	}
	// Exceed the former deadline, scaled by the test-shortened polling period.
	select {
	case err := <-workerDone:
		require.NoError(t, pq.ExecSQL(ctx, blocker, "COMMIT"))
		require.NoError(t, <-coordinatorDone)
		t.Fatalf("worker stopped while coordinator was healthy and slot creation was blocked: %v", err)
	case <-time.After(600 * coordinatorWaitInterval):
	}
	t.Log("worker still waiting beyond the former coordinator deadline while CREATE_REPLICATION_SLOT is blocked")
	require.NoError(t, pq.ExecSQL(ctx, blocker, "COMMIT"))
	require.NoError(t, <-coordinatorDone)
	require.NoError(t, <-workerDone)
	rows := 0
	require.NoError(t, worker.Execute(ctx, func(event *format.Snapshot) error {
		if event.EventType == format.SnapshotEventTypeData {
			rows++
		}
		return nil
	}, name))
	require.NoError(t, coordinator.Execute(ctx, func(*format.Snapshot) error { return nil }, name))
	require.Equal(t, 2, rows)
}
