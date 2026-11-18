package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/stretchr/testify/require"
)

// The synchronous logger gives the test a barrier between slot creation and
// snapshot preparation, without timing sleeps or production-only test hooks.
var boundaryTestLogger = &snapshotBarrierLogger{Logger: logger.NewSlog(0)}

type snapshotBarrierLogger struct {
	logger.Logger
	mu   sync.Mutex
	hook func(string)
}

func (l *snapshotBarrierLogger) barrier(msg string) {
	l.mu.Lock()
	hook := l.hook
	l.mu.Unlock()
	if hook != nil {
		hook(msg)
	}
}
func (l *snapshotBarrierLogger) Debug(msg string, args ...any) {
	l.barrier(msg)
	l.Logger.Debug(msg, args...)
}
func (l *snapshotBarrierLogger) Info(msg string, args ...any) {
	l.barrier(msg)
	l.Logger.Info(msg, args...)
}

func TestSnapshotExactBoundary(t *testing.T) {
	for _, kind := range []string{"insert", "delete", "after_boundary"} {
		t.Run(kind, func(t *testing.T) { testSnapshotExactBoundary(t, kind) })
	}
}

func testSnapshotExactBoundary(t *testing.T, kind string) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := newPostgresConn()
	require.NoError(t, err)
	defer admin.Close(context.Background())
	writer, err := newPostgresConn()
	require.NoError(t, err)
	defer writer.Close(context.Background())
	name := "exact_boundary_" + kind
	cfg := Config
	cfg.Slot.Name = name
	cfg.Publication.Name = name
	cfg.Publication.Tables = publication.Tables{{Name: name, Schema: "public", ReplicaIdentity: publication.ReplicaIdentityFull}}
	cfg.Snapshot.Enabled = true
	cfg.Snapshot.Mode = "initial"
	cfg.Snapshot.ChunkSize = 1
	require.NoError(t, createTestTable(ctx, admin, name))
	require.NoError(t, pgExec(ctx, admin, fmt.Sprintf("INSERT INTO %s VALUES (1, $$seed$$, 1)", name)))
	t.Cleanup(func() { cleanupSnapshotTest(t, context.Background(), name, name, name) })
	rows, err := execQuery(ctx, writer, "SELECT pg_backend_pid()")
	require.NoError(t, err)
	pid := string(rows[0].Rows[0][0])
	writeDone := make(chan error, 1)
	var barrierErr error
	var committedWAL pq.LSN
	var once sync.Once
	var release sync.Once

	if kind != "after_boundary" {
		require.NoError(t, pgExec(ctx, admin, "SET synchronous_commit = off"))
		require.NoError(t, pgExec(ctx, admin, "ALTER SYSTEM SET synchronous_commit = off"))
		require.NoError(t, pgExec(ctx, admin, "ALTER SYSTEM SET synchronous_standby_names = $$*$$"))
		require.NoError(t, pgExec(ctx, admin, "SELECT pg_reload_conf()"))
		require.NoError(t, pgExec(ctx, writer, "SET synchronous_commit = on"))
		defer func() {
			_ = pgExec(context.Background(), admin, "ALTER SYSTEM RESET synchronous_standby_names")
			_ = pgExec(context.Background(), admin, "ALTER SYSTEM RESET synchronous_commit")
			_ = pgExec(context.Background(), admin, "SELECT pg_reload_conf()")
		}()
	}
	boundaryTestLogger.mu.Lock()
	boundaryTestLogger.hook = func(msg string) {
		if msg == "replication slot created" && kind != "after_boundary" {
			once.Do(func() {
				query := fmt.Sprintf("INSERT INTO %s VALUES (2, $$boundary$$, 2)", name)
				if kind == "delete" {
					query = "DELETE FROM " + name + " WHERE id = 1"
				}
				go func() { writeDone <- pgExec(ctx, writer, query) }()
				for ctx.Err() == nil {
					r, e := execQuery(ctx, admin, "SELECT wait_event = $$SyncRep$$ FROM pg_stat_activity WHERE pid = "+pid)
					if e != nil {
						barrierErr = e
						return
					}
					if len(r[0].Rows) > 0 && string(r[0].Rows[0][0]) == "t" {
						r, e = execQuery(ctx, admin, "SELECT pg_current_wal_insert_lsn()")
						if e != nil {
							barrierErr = e
							return
						}
						committedWAL, barrierErr = pq.ParseLSN(string(r[0].Rows[0][0]))
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
				barrierErr = ctx.Err()
			})
		}
		if msg == "[coordinator] initializing job" && kind == "after_boundary" {
			once.Do(func() { barrierErr = pgExec(ctx, writer, fmt.Sprintf("INSERT INTO %s VALUES (2, $$after$$, 2)", name)) })
		}
		if msg == "[coordinator] metadata committed" && kind != "after_boundary" {
			release.Do(func() {
				_, barrierErr = execQuery(ctx, admin, "SELECT pg_cancel_backend("+pid+")")
				if barrierErr == nil {
					barrierErr = <-writeDone
				}
			})
		}
	}
	boundaryTestLogger.mu.Unlock()
	defer func() { boundaryTestLogger.mu.Lock(); boundaryTestLogger.hook = nil; boundaryTestLogger.mu.Unlock() }()
	messages := make(chan any, 32)
	connector, err := cdc.NewConnector(ctx, cfg, func(lc *replication.ListenerContext) {
		messages <- lc.Message
		_ = lc.Ack()
	})
	require.NoError(t, err)
	runDone := make(chan error, 1)
	go func() { runDone <- connector.Run(ctx) }()
	defer func() { cancel(); require.ErrorIs(t, <-runDone, context.Canceled); connector.Close() }()
	require.NoError(t, connector.WaitUntilReady(ctx))
	require.NoError(t, barrierErr)
	require.NoError(t, pgExec(ctx, writer, "SET synchronous_commit = off"))
	require.NoError(t, pgExec(ctx, writer, fmt.Sprintf("INSERT INTO %s VALUES (3, $$fence$$, 3)", name)))
	state := map[int32]bool{}
	snapshotIDs := map[int32]bool{}
	streamIDs := map[int32]int{}
	var boundary pq.LSN
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for stream fence")
		case event := <-messages:
			switch m := event.(type) {
			case *format.Snapshot:
				boundary = m.LSN
				if m.EventType == format.SnapshotEventTypeData {
					id := m.Data["id"].(int32)
					snapshotIDs[id] = true
					state[id] = true
				}
			case *format.Delete:
				delete(state, m.OldDecoded["id"].(int32))
			case *format.Insert:
				id := m.Decoded["id"].(int32)
				state[id] = true
				streamIDs[id]++
				if id == 3 {
					t.Logf("snapshot boundary=%s, locally committed WAL=%s, snapshot=%v, stream inserts=%v", boundary, committedWAL, snapshotIDs, streamIDs)
					if kind == "delete" {
						require.NotContains(t, state, int32(1), "a pre-boundary delete must not resurrect its row")
					} else {
						require.Contains(t, state, int32(2), "no insert may be missing from snapshot plus stream")
					}
					if kind == "after_boundary" {
						require.NotContains(t, snapshotIDs, int32(2), "snapshot must exclude rows committed after its boundary")
						require.Equal(t, 1, streamIDs[2])
					}
					return
				}
			}
		}
	}
}
