package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"
	"github.com/stretchr/testify/require"
)

func snapshotLifecycleSetup(t *testing.T, name string) (context.Context, pq.Connection, config.Config) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	admin, err := newPostgresConn()
	require.NoError(t, err)
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	cfg := Config
	cfg.Slot.Name = name
	cfg.Snapshot.Enabled = true
	cfg.Snapshot.Mode = config.SnapshotModeInitial
	cfg.Snapshot.ChunkSize = 1
	cfg.Publication.Tables = publication.Tables{{Name: name, Schema: "public"}}
	cfg.SetDefault()
	require.NoError(t, createTestTable(ctx, admin, name))
	require.NoError(t, pgExec(ctx, admin, fmt.Sprintf("INSERT INTO %s VALUES (1, $$original$$, 1), (2, $$original$$, 2)", name)))
	t.Cleanup(func() { cleanupSnapshotTest(t, context.Background(), name, name, name) })
	return ctx, admin, cfg
}

func newLifecycleSnapshotter(ctx context.Context, t *testing.T, cfg config.Config, instance string) (*snapshot.Snapshotter, *slot.Slot) {
	t.Helper()
	cfg.Snapshot.InstanceID = instance
	m := metric.NewMetric(cfg.Slot.Name)
	s, err := snapshot.New(ctx, cfg.Snapshot, cfg.Publication.Tables, cfg.DSN(), m)
	require.NoError(t, err)
	sl := slot.NewSlot(cfg.ReplicationDSN(), cfg.DSN(), cfg.Slot, m, nil)
	t.Cleanup(func() { s.Close(context.Background()); sl.Close(context.Background()) })
	return s, sl
}

func TestSnapshotLateWorkerUsesOriginalExport(t *testing.T) {
	ctx, admin, cfg := snapshotLifecycleSetup(t, "snapshot_late_worker")
	coordinator, sl := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	require.NoError(t, coordinator.PrepareWithSlot(ctx, cfg.Slot.Name, sl))
	job, err := coordinator.LoadJob(ctx, cfg.Slot.Name)
	require.NoError(t, err)
	require.NoError(t, pgExec(ctx, admin, "UPDATE snapshot_late_worker SET name = $$changed$$; INSERT INTO snapshot_late_worker VALUES (3, $$late$$, 3)"))
	worker, workerSlot := newLifecycleSnapshotter(ctx, t, cfg, "late-worker")
	require.NoError(t, worker.PrepareWithSlot(ctx, cfg.Slot.Name, workerSlot))
	rows := map[int32]string{}
	require.NoError(t, worker.Execute(ctx, func(event *format.Snapshot) error {
		require.Equal(t, job.SnapshotLSN, event.LSN)
		if event.EventType == format.SnapshotEventTypeData {
			rows[event.Data["id"].(int32)] = event.Data["name"].(string)
		}
		return nil
	}, cfg.Slot.Name))
	require.Equal(t, map[int32]string{1: "original", 2: "original"}, rows)
	require.NoError(t, coordinator.Execute(ctx, func(*format.Snapshot) error { return nil }, cfg.Slot.Name))
}

func TestSnapshotExpiredExportFailsAndRestartsFresh(t *testing.T) {
	ctx, admin, cfg := snapshotLifecycleSetup(t, "snapshot_restart_export")
	coordinator, sl := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	require.NoError(t, coordinator.PrepareWithSlot(ctx, cfg.Slot.Name, sl))
	job, err := coordinator.LoadJob(ctx, cfg.Slot.Name)
	require.NoError(t, err)
	// Ending just the replication export leaves the coordinator lock held, so
	// a late worker must fail instead of substituting a new SQL snapshot.
	require.NoError(t, pgExec(ctx, admin, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = $$walsender$$ AND query LIKE $$%snapshot_restart_export%$$"))
	worker, workerSlot := newLifecycleSnapshotter(ctx, t, cfg, "late-worker")
	require.NoError(t, worker.PrepareWithSlot(ctx, cfg.Slot.Name, workerSlot))
	emitted := false
	err = worker.Execute(ctx, func(event *format.Snapshot) error {
		if event.EventType == format.SnapshotEventTypeData {
			emitted = true
		}
		return nil
	}, cfg.Slot.Name)
	require.ErrorIs(t, err, snapshot.ErrSnapshotInvalidated)
	require.False(t, emitted)
	coordinator.Close(ctx)
	require.NoError(t, pgExec(ctx, admin, "UPDATE snapshot_restart_export SET name = $$fresh$$; INSERT INTO snapshot_restart_export VALUES (3, $$fresh$$, 3)"))
	restarted, newSlot := newLifecycleSnapshotter(ctx, t, cfg, "restarted")
	require.NoError(t, restarted.PrepareWithSlot(ctx, cfg.Slot.Name, newSlot))
	fresh, err := restarted.LoadJob(ctx, cfg.Slot.Name)
	require.NoError(t, err)
	require.NotEqual(t, job.SnapshotID, fresh.SnapshotID)
	require.Greater(t, fresh.SnapshotLSN, job.SnapshotLSN)
	require.Zero(t, fresh.CompletedChunks)
	rows := 0
	require.NoError(t, restarted.Execute(ctx, func(event *format.Snapshot) error {
		if event.EventType == format.SnapshotEventTypeData {
			rows++
			require.Equal(t, "fresh", event.Data["name"])
		}
		return nil
	}, cfg.Slot.Name))
	require.Equal(t, 3, rows)
}

func TestSnapshotRefusesExistingSlotWithoutExport(t *testing.T) {
	ctx, _, cfg := snapshotLifecycleSetup(t, "snapshot_existing_slot")
	s, sl := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	_, err := sl.Create(ctx)
	require.NoError(t, err)
	require.ErrorContains(t, s.PrepareWithSlot(ctx, cfg.Slot.Name, sl), "exact snapshot requires a new replication slot")
	// Explicit resnapshot authorizes replacement and gives the job a fresh export.
	cfg.Snapshot.Resnapshot = true
	fresh, freshSlot := newLifecycleSnapshotter(ctx, t, cfg, "resnapshot")
	require.NoError(t, fresh.PrepareWithSlot(ctx, cfg.Slot.Name, freshSlot))
	require.NoError(t, fresh.Execute(ctx, func(*format.Snapshot) error { return nil }, cfg.Slot.Name))
}

func TestSnapshotCoordinatorWaitsForOtherWorkers(t *testing.T) {
	ctx, admin, cfg := snapshotLifecycleSetup(t, "snapshot_wait_workers")
	coordinator, sl := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	require.NoError(t, coordinator.PrepareWithSlot(ctx, cfg.Slot.Name, sl))
	worker, workerSlot := newLifecycleSnapshotter(ctx, t, cfg, "worker")
	require.NoError(t, worker.PrepareWithSlot(ctx, cfg.Slot.Name, workerSlot))
	reading := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker.Execute(ctx, func(event *format.Snapshot) error {
			if event.EventType == format.SnapshotEventTypeData {
				reading <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			return nil
		}, cfg.Slot.Name)
	}()
	select {
	case <-reading:
	case <-ctx.Done():
		t.Fatal("worker never read its chunk")
	}
	coordinatorDone := make(chan error, 1)
	go func() {
		coordinatorDone <- coordinator.Execute(ctx, func(*format.Snapshot) error { return nil }, cfg.Slot.Name)
	}()
	require.Eventually(t, func() bool {
		result, err := execQuery(ctx, admin, "SELECT completed_chunks = total_chunks - 1 FROM cdc_snapshot_job WHERE slot_name = $$snapshot_wait_workers$$")
		return err == nil && len(result[0].Rows) == 1 && string(result[0].Rows[0][0]) == "t"
	}, 5*time.Second, 10*time.Millisecond)
	select {
	case err := <-coordinatorDone:
		t.Fatalf("coordinator exited before the worker finished: %v", err)
	default:
	}
	count, err := countWalsendersForSlot(ctx, admin, cfg.Slot.Name)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	release <- struct{}{}
	require.NoError(t, <-workerDone)
	require.NoError(t, <-coordinatorDone)
}

func TestSnapshotInterruptedProgressRestartsAllChunks(t *testing.T) {
	for _, mode := range []config.SnapshotMode{config.SnapshotModeInitial, config.SnapshotModeSnapshotOnly} {
		t.Run(string(mode), func(t *testing.T) {
			ctx, admin, cfg := snapshotLifecycleSetup(t, "snapshot_interrupted_"+string(mode))
			cfg.Snapshot.Mode = mode
			first, sl := newLifecycleSnapshotter(ctx, t, cfg, "first")
			prepare := func(s *snapshot.Snapshotter, slot *slot.Slot) error {
				if mode == config.SnapshotModeSnapshotOnly {
					return s.Prepare(ctx, cfg.Slot.Name)
				}
				return s.PrepareWithSlot(ctx, cfg.Slot.Name, slot)
			}
			require.NoError(t, prepare(first, sl))
			old, err := first.LoadJob(ctx, cfg.Slot.Name)
			require.NoError(t, err)
			interrupted, stop := context.WithCancel(ctx)
			defer stop()
			rows := 0
			require.Error(t, first.Execute(interrupted, func(event *format.Snapshot) error {
				if event.EventType == format.SnapshotEventTypeData {
					rows++
					if rows == 2 {
						stop()
					}
				}
				return nil
			}, cfg.Slot.Name))
			progress, err := execQuery(ctx, admin, fmt.Sprintf("SELECT completed_chunks FROM cdc_snapshot_job WHERE slot_name = %s", "$$"+cfg.Slot.Name+"$$"))
			require.NoError(t, err)
			require.Equal(t, "1", string(progress[0].Rows[0][0]))
			fresh, freshSlot := newLifecycleSnapshotter(ctx, t, cfg, "fresh")
			require.NoError(t, prepare(fresh, freshSlot))
			job, err := fresh.LoadJob(ctx, cfg.Slot.Name)
			require.NoError(t, err)
			require.NotEqual(t, old.SnapshotID, job.SnapshotID)
			require.Zero(t, job.CompletedChunks)
			rows = 0
			require.NoError(t, fresh.Execute(ctx, func(event *format.Snapshot) error {
				if event.EventType == format.SnapshotEventTypeData {
					rows++
				}
				return nil
			}, cfg.Slot.Name))
			require.Equal(t, 2, rows, "all chunks must be read again under the fresh export")
		})
	}
}

func TestSnapshotResnapshotRefusesActiveSlot(t *testing.T) {
	ctx, admin, cfg := snapshotLifecycleSetup(t, "snapshot_active_slot")
	s, sl := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	_, err := sl.Create(ctx)
	require.NoError(t, err)
	require.NoError(t, pgExec(ctx, admin, "CREATE PUBLICATION snapshot_active_slot FOR TABLE snapshot_active_slot"))
	conn, err := pq.NewConnection(ctx, cfg.ReplicationDSN())
	require.NoError(t, err)
	defer conn.Close(context.Background())
	stream := replication.New(conn)
	require.NoError(t, stream.Start(cfg.Slot.Name, cfg.Slot.Name, 0, 1, false, false))
	require.NoError(t, stream.Test(ctx))
	// The first snapshotter is not used; the resnapshotter must refuse the
	// active slot even when recreation was explicitly requested.
	s.Close(ctx)
	cfg.Snapshot.Resnapshot = true
	resnapshot, resnapshotSlot := newLifecycleSnapshotter(ctx, t, cfg, "resnapshot")
	require.ErrorContains(t, resnapshot.PrepareWithSlot(ctx, cfg.Slot.Name, resnapshotSlot), "cannot recreate active replication slot")
	info, err := sl.Info(ctx)
	require.NoError(t, err)
	require.True(t, info.Active)
	require.NoError(t, conn.Close(ctx))
	require.Eventually(t, func() bool {
		info, err := sl.Info(ctx)
		return err == nil && !info.Active
	}, time.Second, 10*time.Millisecond)
}
