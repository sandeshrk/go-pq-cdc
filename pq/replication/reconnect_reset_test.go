package replication

import (
	"log/slog"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/stretchr/testify/assert"
)

// dirtyStream builds a stream with every piece of A3-relevant state set to a
// non-zero/non-default value, as if it had been decoding for a while before
// an unexpected disconnect.
func dirtyStream(t *testing.T) *stream {
	t.Helper()
	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)

	s.txCommit.time = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s.txCommit.lsn = 500
	s.txCommit.xid = 42

	s.openFromSnapshotLSN = true

	s.UpdateXLogPos(1000)
	s.UpdateConfirmedXLogPos(900)

	s.relation[16390] = testRelation()[16390]

	return s
}

func TestResetForReconnectClearsTxCommit(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	s := dirtyStream(t)

	s.resetForReconnect()

	assert.True(t, s.txCommit.time.IsZero(), "txCommit.time must be cleared")
	assert.Equal(t, pq.LSN(0), s.txCommit.lsn, "txCommit.lsn must be cleared")
	assert.Equal(t, uint32(0), s.txCommit.xid, "txCommit.xid must be cleared")
}

func TestResetForReconnectClearsOpenFromSnapshotLSN(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	s := dirtyStream(t)
	assert.True(t, s.openFromSnapshotLSN, "test setup should have set this")

	s.resetForReconnect()

	assert.False(t, s.openFromSnapshotLSN, "openFromSnapshotLSN must be cleared, or a reconnect replays from the snapshot LSN")
}

// TestResetForReconnectStartsFromZeroNotLastReceived is the data-loss guard:
// the reconnect's start LSN must come from the server (confirmed_flush_lsn
// via 0/0), not from the highest position this process happened to receive.
// A naive implementation that reuses lastXLogPos as the start position would
// silently skip every message that was received but not yet acked.
func TestResetForReconnectStartsFromZeroNotLastReceived(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	s := dirtyStream(t)
	assert.Equal(t, pq.LSN(1000), s.LoadXLogPos(), "test setup should have set a received position ahead of the confirmed one")

	s.resetForReconnect()

	assert.Equal(t, pq.LSN(0), s.LoadXLogPos(), "lastXLogPos must be reset to 0 so setup() sends START_REPLICATION 0/0")
}

func TestResetForReconnectPreservesConfirmedXLogPos(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	s := dirtyStream(t)

	s.resetForReconnect()

	assert.Equal(t, pq.LSN(900), s.LoadConfirmedXLogPos(), "confirmedXLogPos must survive a reconnect: it is never used as the start position, only as a floor for standby status updates")
}

func TestResetForReconnectPreservesRelationCache(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))
	s := dirtyStream(t)
	before := s.relation[16390]
	assert.NotNil(t, before, "test setup should have populated the relation cache")

	s.resetForReconnect()

	assert.Same(t, before, s.relation[16390], "the relation (OID->schema) cache must survive a reconnect unchanged")
}
