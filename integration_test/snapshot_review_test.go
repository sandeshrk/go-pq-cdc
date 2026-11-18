package integration

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"
	"github.com/stretchr/testify/require"
)

func setReviewBarrier(t *testing.T, hook func(string)) {
	t.Helper()
	boundaryTestLogger.mu.Lock()
	boundaryTestLogger.hook = hook
	boundaryTestLogger.mu.Unlock()
	t.Cleanup(func() { boundaryTestLogger.mu.Lock(); boundaryTestLogger.hook = nil; boundaryTestLogger.mu.Unlock() })
}

// A subprocess contains the same-goroutine deadlock on the unfixed driver. It
// reuses the parent database; CommandContext kills only that child PID on timeout.
func TestReviewSnapshotCloseFromEndHandler(t *testing.T) {
	if os.Getenv("CDC_REVIEW_CHILD_PORT") == "" {
		executable, err := os.Executable()
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestReviewSnapshotCloseFromEndHandler$", "-test.timeout=20s", "-test.v")
		cmd.Env = append(os.Environ(), "CDC_REVIEW_CHILD_PORT="+strconv.Itoa(Config.Port))
		output, err := cmd.CombinedOutput()
		t.Logf("child PID=%d result=%v context=%v\n%s", cmd.Process.Pid, err, ctx.Err(), output)
		cleanupSnapshotTest(t, context.Background(), "review_close_end", "review_close_end", "review_close_end")
		require.NoError(t, err, "Close called by the END listener must return")
		return
	}
	ctx, _, cfg := snapshotLifecycleSetup(t, "review_close_end")
	cfg.Publication.Name = cfg.Slot.Name
	cfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityFull
	cfg.Snapshot.Mode = "snapshot_only"
	cfg.Snapshot.ID = cfg.Slot.Name
	cfg.Snapshot.Tables = cfg.Publication.Tables
	var connector cdc.Connector
	var err error
	connector, err = cdc.NewConnector(ctx, cfg, func(lc *replication.ListenerContext) {
		if m, ok := lc.Message.(*format.Snapshot); ok && m.EventType == format.SnapshotEventTypeEnd {
			fmt.Println("END listener calling Close")
			connector.Close()
			fmt.Println("END listener Close returned")
		}
	})
	require.NoError(t, err)
	_ = connector.Run(ctx)
	connector.Close()
}

func TestReviewSnapshotCloseDeadlineWithBlockedHandler(t *testing.T) {
	ctx, _, cfg := snapshotLifecycleSetup(t, "review_close_deadline")
	s, sl := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	require.NoError(t, s.PrepareWithSlot(ctx, cfg.Slot.Name, sl))
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.Execute(ctx, func(event *format.Snapshot) error {
			if event.EventType == format.SnapshotEventTypeBegin {
				close(entered)
				<-release
			}
			return nil
		}, cfg.Slot.Name)
	}()
	<-entered
	closeCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	closed := make(chan struct{})
	go func() { s.Close(closeCtx); close(closed) }()
	returned := false
	select {
	case <-closed:
		returned = true
	case <-time.After(time.Second):
	}
	close(release)
	<-closed
	require.Error(t, <-done)
	require.True(t, returned, "Close ignored its canceled context while the handler was blocked")
}

func TestReviewSnapshotListenerGetsOperationCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	setupCtx, _, cfg := snapshotLifecycleSetup(t, "review_listener_cancel")
	cfg.Publication.Name = cfg.Slot.Name
	cfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityFull
	entered := make(chan struct{})
	var once sync.Once
	connector, err := cdc.NewConnector(setupCtx, cfg, func(lc *replication.ListenerContext) {
		if _, ok := lc.Message.(*format.Snapshot); ok {
			once.Do(func() { close(entered); <-lc.Context.Done() })
		}
	})
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- connector.Run(ctx) }()
	<-entered
	closed := make(chan struct{})
	go func() { connector.Close(); close(closed) }()
	returned := false
	select {
	case <-closed:
		returned = true
	case <-time.After(time.Second):
	}
	cancel()
	<-closed
	require.Error(t, <-done)
	require.True(t, returned, "operation cancellation never reached ListenerContext")
}

func TestReviewSnapshotResnapshotPublishesPendingBeforeSlotCreation(t *testing.T) {
	ctx, admin, cfg := snapshotLifecycleSetup(t, "review_pending_generation")
	cfg.Publication.Name = cfg.Slot.Name
	cfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityFull
	require.NoError(t, pgExec(ctx, admin, "ALTER TABLE review_pending_generation REPLICA IDENTITY FULL"))
	first, sl := newLifecycleSnapshotter(ctx, t, cfg, "first")
	require.NoError(t, first.PrepareWithSlot(ctx, cfg.Slot.Name, sl))
	require.NoError(t, first.Execute(ctx, func(*format.Snapshot) error { return nil }, cfg.Slot.Name))
	cfg.Snapshot.Resnapshot = true
	coordinator, newSlot := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	created, release := make(chan struct{}), make(chan struct{})
	observed := make(chan bool, 1)
	var observe sync.Once
	setReviewBarrier(t, func(msg string) {
		if msg == "replication slot created" {
			close(created)
			<-release
		}
		if msg == "[worker] coordinator ready, starting work" {
			observe.Do(func() { observed <- true })
		}
		if msg == "[worker] waiting for coordinator" {
			observe.Do(func() { observed <- false })
		}
	})
	coordinatorDone := make(chan error, 1)
	go func() { coordinatorDone <- coordinator.PrepareWithSlot(ctx, cfg.Slot.Name, newSlot) }()
	<-created
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	markers := make(chan struct{}, 4)
	worker, err := cdc.NewConnector(ctx, cfg, func(lc *replication.ListenerContext) {
		if m, ok := lc.Message.(*format.Snapshot); ok && m.EventType == format.SnapshotEventTypeEnd {
			markers <- struct{}{}
		}
		_ = lc.Ack()
	})
	require.NoError(t, err)
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(runCtx) }()
	acceptedOld := <-observed
	if acceptedOld {
		require.NoError(t, worker.WaitUntilReady(ctx))
		require.NotEmpty(t, markers)
		t.Log("worker emitted END and opened CDC for the previous completed generation during slot creation")
	}
	rows, err := execQuery(ctx, admin, "SELECT snapshot_id, completed FROM cdc_snapshot_job WHERE slot_name = $$review_pending_generation$$")
	require.NoError(t, err)
	pending := strings.HasPrefix(string(rows[0].Rows[0][0]), "PENDING") && string(rows[0].Rows[0][1]) == "f"
	close(release)
	require.NoError(t, <-coordinatorDone)
	require.NoError(t, worker.WaitUntilReady(ctx))
	cancel()
	require.ErrorIs(t, <-workerDone, context.Canceled)
	worker.Close()
	require.False(t, acceptedOld, "worker must not finish the old generation or start CDC during slot creation")
	require.True(t, pending, "the incomplete attempt must be durable before slot creation")
}

func TestReviewSnapshotSlotCreationFailureRestartsWithoutOperator(t *testing.T) {
	ctx, _, cfg := snapshotLifecycleSetup(t, "review_creation_failure")
	first, sl := newLifecycleSnapshotter(ctx, t, cfg, "first")
	attempt, stop := context.WithCancel(ctx)
	defer stop()
	var once sync.Once
	setReviewBarrier(t, func(msg string) {
		if msg == "replication slot created" {
			once.Do(stop)
		}
	})
	require.Error(t, first.PrepareWithSlot(attempt, cfg.Slot.Name, sl))
	fresh, newSlot := newLifecycleSnapshotter(ctx, t, cfg, "fresh")
	require.NoError(t, fresh.PrepareWithSlot(ctx, cfg.Slot.Name, newSlot), "a failed attempt after slot creation must retain ownership")
	require.NoError(t, fresh.Execute(ctx, func(*format.Snapshot) error { return nil }, cfg.Slot.Name))
}

func TestReviewSnapshotPreparedWorkerRejectsReplacementGeneration(t *testing.T) {
	ctx, _, cfg := snapshotLifecycleSetup(t, "review_stale_worker")
	coordinator, sl := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	require.NoError(t, coordinator.PrepareWithSlot(ctx, cfg.Slot.Name, sl))
	worker, workerSlot := newLifecycleSnapshotter(ctx, t, cfg, "worker")
	require.NoError(t, worker.PrepareWithSlot(ctx, cfg.Slot.Name, workerSlot))
	coordinator.Close(ctx)
	fresh, freshSlot := newLifecycleSnapshotter(ctx, t, cfg, "fresh")
	require.NoError(t, fresh.PrepareWithSlot(ctx, cfg.Slot.Name, freshSlot))
	emitted := false
	err := worker.Execute(ctx, func(event *format.Snapshot) error {
		if event.EventType == format.SnapshotEventTypeData {
			emitted = true
		}
		return nil
	}, cfg.Slot.Name)
	require.ErrorIs(t, err, snapshot.ErrSnapshotInvalidated)
	require.False(t, emitted)
}

func TestReviewSnapshotSuperviseRejoinsAfterCoordinatorLoss(t *testing.T) {
	ctx, admin, cfg := snapshotLifecycleSetup(t, "review_supervise_rejoin")
	cfg.Publication.Name = cfg.Slot.Name
	cfg.Publication.Tables[0].ReplicaIdentity = publication.ReplicaIdentityFull
	require.NoError(t, pgExec(ctx, admin, "ALTER TABLE review_supervise_rejoin REPLICA IDENTITY FULL"))
	coordinator, sl := newLifecycleSnapshotter(ctx, t, cfg, "coordinator")
	require.NoError(t, coordinator.PrepareWithSlot(ctx, cfg.Slot.Name, sl))
	var once sync.Once
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	retry, ready := make(chan error, 8), make(chan struct{}, 1)
	done := make(chan error, 1)
	rows := map[int32]bool{}
	go func() {
		done <- cdc.Supervise(runCtx, cfg, func(lc *replication.ListenerContext) {
			if m, ok := lc.Message.(*format.Snapshot); ok && m.EventType == format.SnapshotEventTypeData {
				rows[m.Data["id"].(int32)] = true
				once.Do(func() { coordinator.Close(ctx) })
			}
			_ = lc.Ack()
		}, cdc.SuperviseOpts{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
			OnRetry: func(_ int, err error, _ time.Duration) { retry <- err }, OnReady: func() { ready <- struct{}{} },
		})
	}()
	select {
	case <-ready:
		cancel()
		require.NoError(t, <-done)
		require.NotEmpty(t, retry)
		require.Equal(t, map[int32]bool{1: true, 2: true}, rows)
	case err := <-done:
		t.Fatalf("Supervise gave up instead of rejoining: %v", err)
	case <-ctx.Done():
		cancel()
		require.NoError(t, <-done)
		t.Fatal("Supervise never completed the replacement snapshot")
	}
}
