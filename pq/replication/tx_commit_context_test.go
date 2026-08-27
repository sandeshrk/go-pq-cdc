package replication

import (
	"encoding/binary"
	"log/slog"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/message/tuple"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// xlogDataFrame wraps a decoded pgoutput payload in the XLogData header
// ParseXLogData expects: WALStart(8) + ServerWALEnd(8) + ServerTime(8).
func xlogDataFrame(walStart pq.LSN, payload []byte) []byte {
	buf := make([]byte, 24+len(payload))
	binary.BigEndian.PutUint64(buf[0:], uint64(walStart))
	binary.BigEndian.PutUint64(buf[8:], uint64(walStart))
	copy(buf[24:], payload)
	return buf
}

func testRelation() map[uint32]*format.Relation {
	return map[uint32]*format.Relation{
		16390: {
			OID:           16390,
			Namespace:     "public",
			Name:          "t",
			ColumnNumbers: 2,
			Columns: []tuple.RelationColumn{
				{Name: "id", DataType: 23, TypeModifier: 4294967295},
				{Name: "name", DataType: 25, TypeModifier: 4294967295},
			},
		},
	}
}

func TestHandleXLogDataStampsNonStreamedTransactionCommitContext(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	beginData := []byte{
		66,                          // 'B'
		0, 0, 0, 0, 1, 150, 157, 24, // FinalLSN: 26647832
		0, 2, 234, 4, 120, 77, 196, 132, // CommitTime
		0, 0, 2, 249, // Xid: 761
	}
	insertData := []byte{73, 0, 0, 64, 6, 78, 0, 2, 116, 0, 0, 0, 3, 54, 48, 53, 116, 0, 0, 0, 3, 102, 111, 111}
	commitData := []byte{
		67, 0, // 'C', flags
		0, 0, 0, 0, 1, 150, 157, 24, // CommitLSN
		0, 0, 0, 0, 1, 150, 157, 72, // TransactionEndLSN: 26647880
		0, 2, 234, 4, 120, 77, 196, 132, // CommitTime
	}

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.relation = testRelation()

	out := make(chan *Message, 10)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	s.handleXLogData(xlogDataFrame(1000, beginData), buf, streamBuf)
	s.handleXLogData(xlogDataFrame(1100, insertData), buf, streamBuf)
	s.handleXLogData(xlogDataFrame(1200, insertData), buf, streamBuf)
	s.handleXLogData(xlogDataFrame(1300, commitData), buf, streamBuf)

	require.Len(t, out, 2)
	first := <-out
	second := <-out

	firstInsert := first.message.(*format.Insert)
	secondInsert := second.message.(*format.Insert)

	assert.Equal(t, pq.LSN(1100), firstInsert.LSN)
	assert.Equal(t, pq.LSN(1200), secondInsert.LSN)
	assert.Equal(t, int64(1100), first.walStart)
	assert.Equal(t, int64(26647880), second.walStart)

	assert.False(t, firstInsert.LastInTransaction)
	assert.True(t, secondInsert.LastInTransaction)

	assert.Equal(t, uint32(761), firstInsert.XID)
	assert.Equal(t, uint32(761), secondInsert.XID)
	assert.Equal(t, pq.LSN(26647832), firstInsert.CommitLSN)
	assert.Equal(t, pq.LSN(26647832), secondInsert.CommitLSN)
	assert.False(t, firstInsert.CommitTime.IsZero())
	assert.Equal(t, firstInsert.CommitTime, secondInsert.CommitTime)
}

func TestHandleXLogDataResetsCommitContextBetweenTransactions(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	firstBegin := []byte{
		66,
		0, 0, 0, 0, 1, 150, 157, 24, // FinalLSN
		0, 2, 234, 4, 120, 77, 196, 132, // CommitTime
		0, 0, 2, 249, // Xid: 761
	}
	firstCommit := []byte{
		67, 0,
		0, 0, 0, 0, 1, 150, 157, 24,
		0, 0, 0, 0, 1, 150, 157, 72, // TransactionEndLSN
		0, 2, 234, 4, 120, 77, 196, 132,
	}
	secondBegin := []byte{
		66,
		0, 0, 0, 0, 2, 0, 0, 0, // FinalLSN: distinct value
		0, 0, 0, 0, 0, 0, 0, 1, // CommitTime: distinct value
		0, 0, 3, 232, // Xid: 1000
	}
	secondCommit := []byte{
		67, 0,
		0, 0, 0, 0, 2, 0, 0, 0,
		0, 0, 0, 0, 2, 0, 1, 0, // TransactionEndLSN
		0, 0, 0, 0, 0, 0, 0, 1,
	}
	insertData := []byte{73, 0, 0, 64, 6, 78, 0, 2, 116, 0, 0, 0, 3, 54, 48, 53, 116, 0, 0, 0, 3, 102, 111, 111}

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.relation = testRelation()

	out := make(chan *Message, 10)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	s.handleXLogData(xlogDataFrame(1000, firstBegin), buf, streamBuf)
	s.handleXLogData(xlogDataFrame(1100, insertData), buf, streamBuf)
	s.handleXLogData(xlogDataFrame(1200, firstCommit), buf, streamBuf)

	s.handleXLogData(xlogDataFrame(2000, secondBegin), buf, streamBuf)
	s.handleXLogData(xlogDataFrame(2100, insertData), buf, streamBuf)
	s.handleXLogData(xlogDataFrame(2200, secondCommit), buf, streamBuf)

	require.Len(t, out, 2)
	firstTxInsert := (<-out).message.(*format.Insert)
	secondTxInsert := (<-out).message.(*format.Insert)

	assert.Equal(t, uint32(761), firstTxInsert.XID)
	assert.Equal(t, uint32(1000), secondTxInsert.XID)
	assert.NotEqual(t, firstTxInsert.CommitTime, secondTxInsert.CommitTime)
	assert.NotEqual(t, firstTxInsert.CommitLSN, secondTxInsert.CommitLSN)
}

func TestStreamedTransactionStampsCommitContextAtStreamCommit(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 10)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	insert1 := &format.Insert{LSN: 500}
	insert2 := &format.Insert{LSN: 600}
	commitTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	s.dispatchMessage(&format.StreamStart{Xid: 42}, XLogData{}, buf, streamBuf)
	s.dispatchMessage(insert1, XLogData{WALStart: 500}, buf, streamBuf)
	s.dispatchMessage(insert2, XLogData{WALStart: 600}, buf, streamBuf)
	s.dispatchMessage(&format.StreamStop{}, XLogData{}, buf, streamBuf)
	// CommitLSN (the commit record's own position) and TransactionEndLSN (a
	// slightly later position) are deliberately distinct here to prove the
	// two are not conflated: only the latter may rewrite the acknowledgement
	// position, only the former is stamped onto each message.
	s.dispatchMessage(&format.StreamCommit{Xid: 42, TransactionEndLSN: 700, CommitLSN: 690, CommitTime: commitTime}, XLogData{}, buf, streamBuf)

	require.Len(t, out, 2)
	first := <-out
	second := <-out

	assert.Same(t, insert1, first.message)
	assert.Same(t, insert2, second.message)
	assert.Equal(t, int64(500), first.walStart)
	assert.Equal(t, int64(700), second.walStart)

	assert.Equal(t, commitTime, insert1.CommitTime)
	assert.Equal(t, commitTime, insert2.CommitTime)
	assert.Equal(t, pq.LSN(690), insert1.CommitLSN)
	assert.Equal(t, pq.LSN(690), insert2.CommitLSN)
	assert.Equal(t, uint32(42), insert1.XID)
	assert.False(t, insert1.LastInTransaction)
	assert.True(t, insert2.LastInTransaction)
}

func TestStreamAbortDiscardsBufferedMessagesWithoutStamping(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 10)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}

	insert := &format.Insert{LSN: 10}
	s.dispatchMessage(&format.StreamStart{Xid: 7}, XLogData{}, buf, streamBuf)
	s.dispatchMessage(insert, XLogData{WALStart: 10}, buf, streamBuf)
	s.dispatchMessage(&format.StreamAbort{Xid: 7}, XLogData{}, buf, streamBuf)

	assert.Empty(t, out)
	assert.NotContains(t, streamBuf.txns, uint32(7))
	assert.True(t, insert.CommitTime.IsZero())
}

func TestSingleChangeTransactionMarksLastInTransaction(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	out := make(chan *Message, 10)
	buf := &messageBuffer{outCh: out}
	streamBuf := &streamTxBuffer{}
	commitTime := time.Date(2026, 2, 2, 0, 0, 0, 0, time.UTC)

	s.dispatchMessage(&format.Begin{CommitTime: commitTime, FinalLSN: 60, Xid: 9}, XLogData{}, buf, streamBuf)
	insert := &format.Insert{LSN: 20}
	stampCommit(insert, s.txCommit.time, s.txCommit.lsn, s.txCommit.xid)
	s.dispatchMessage(insert, XLogData{WALStart: 20}, buf, streamBuf)
	s.dispatchMessage(&format.Commit{TransactionEndLSN: 60}, XLogData{}, buf, streamBuf)

	require.Len(t, out, 1)
	got := (<-out).message.(*format.Insert)
	assert.True(t, got.LastInTransaction)
	assert.Equal(t, commitTime, got.CommitTime)
	assert.Equal(t, uint32(9), got.XID)
	assert.Equal(t, pq.LSN(60), got.CommitLSN)
}

func TestRelationAndKeepaliveMessagesAreUnaffectedByCommitStamping(t *testing.T) {
	logger.InitLogger(logger.NewSlog(slog.LevelError))

	s := NewStream("", config.Config{}, metric.NewMetric("test_slot"), func(*ListenerContext) {}).(*stream)
	s.txCommit.time = time.Now()
	s.txCommit.lsn = 123
	s.txCommit.xid = 456

	assert.False(t, stampCommit(&format.Relation{}, s.txCommit.time, s.txCommit.lsn, s.txCommit.xid))
}
