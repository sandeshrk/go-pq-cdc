package replication

import (
	"context"
	"encoding/binary"
	goerrors "errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/avast/retry-go/v4"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

var (
	// ErrorSlotInUse and ErrorNotConnected are stdlib errors (not
	// go-playground/errors.Chain) deliberately: Chain is a slice type
	// ([]*Link), which Go's stdlib errors.Is/As treat as non-comparable and
	// therefore never match -- even against the exact same value. These two
	// are meant to be checked by callers via errors.Is, so they must stay
	// plain stdlib errors.
	ErrorSlotInUse    = goerrors.New("replication slot in use")
	ErrorNotConnected = goerrors.New("stream is not connected")
	// ErrStreamCorrupted is sent on Err() when the sink goroutine gives up
	// after an unrecoverable connection loss: Reconnect is disabled (the
	// default), or its budget was exhausted. The replication stream is dead
	// at that point; a caller must build a new connector/stream to recover.
	ErrStreamCorrupted = goerrors.New("replication stream corrupted: connection lost and reconnect is disabled or exhausted")
)

const (
	StandbyStatusUpdateByteID = 'r'
	streamShutdownTimeout     = 30 * time.Second
	postgresAdminShutdown     = "57P01"
	postgresCrashShutdown     = "57P02"
)

type ListenerContext struct {
	Context context.Context
	Message any
	// WALStart is this message's own WAL position: Ack confirms this value,
	// rewritten to the transaction-end LSN for the last change in a
	// transaction. See the Delivery Guarantees section of the README.
	WALStart pq.LSN
	// CommitLSN is the start of the commit record of the transaction that
	// produced Message (pgoutput Begin.FinalLSN, StreamCommit.CommitLSN for
	// streamed transactions). Zero for snapshot events and for message types
	// that carry no commit context.
	//
	// A standby has applied that transaction only once
	// pg_last_wal_replay_lsn() > CommitLSN. The comparison is strict:
	// pg_last_wal_replay_lsn() reports the end of the last replayed record, so
	// it equals CommitLSN right before the commit record itself is applied.
	CommitLSN pq.LSN
	Ack       func() error
	// AckLSN confirms an arbitrary LSN instead of only WALStart, letting a
	// transactional sink confirm a position it computed itself (e.g. after
	// writing a whole transaction to its own store). A non-monotonic value
	// (<= the already-confirmed position) is a silent no-op.
	AckLSN func(lsn pq.LSN) error
	// Xid is the top-level transaction that produced Message (Begin.Xid, or
	// StreamStart.Xid for a streamed transaction). Zero when the message is
	// not part of a transaction, including snapshot events.
	Xid uint32
}

type ListenerFunc func(ctx *ListenerContext)

type Message struct {
	message  any
	walStart int64
	// xid is the top-level transaction the message belongs to (Begin.Xid or
	// StreamStart.Xid); the visibility guard gates on it.
	xid uint32
	// commitLSN is the start of the transaction's commit record, exposed as
	// ListenerContext.CommitLSN.
	commitLSN pq.LSN
}

type Streamer interface {
	Connect(ctx context.Context) error
	Open(ctx context.Context) error
	Close(ctx context.Context) error
	GetSystemInfo() *pq.IdentifySystemResult
	GetMetric() metric.Metric
	OpenFromSnapshotLSN()
	// Err returns a channel that receives at most one value: a fatal error
	// if the sink goroutine gives up after an unrecoverable connection loss
	// (Reconnect disabled, or its budget exhausted). A caller running the
	// stream (see cdc.Connector.Run) should select on this alongside its own
	// shutdown signals; nothing is sent here on a deliberate Close().
	Err() <-chan error
}

type stream struct {
	metric           metric.Metric
	conn             pq.Connection
	guard            *visibilityGuard
	replicas         *replicaGuard
	cancel           context.CancelFunc
	system           *pq.IdentifySystemResult
	relation         map[uint32]*format.Relation
	messageCH        chan *Message
	listenerFunc     ListenerFunc
	sinkEnd          chan struct{}
	processEnd       chan struct{}
	flushTickerEnd   chan struct{}
	fatalCh          chan error
	mu               *sync.RWMutex
	config           config.Config
	lastXLogPos      pq.LSN
	snapshotLSN      pq.LSN
	confirmedXLogPos pq.LSN
	// deliveredHighWaterMark is the highest walStart ever handed to
	// listenerFunc. Used to compute replayFloor on a reconnect; see T2.2 in
	// the Delivery Guarantees README section.
	deliveredHighWaterMark pq.LSN
	// replayFloor is set to deliveredHighWaterMark on a successful reconnect.
	// Every delivered message with walStart <= replayFloor was already
	// delivered before the disconnect, i.e. it's a replay, not new data.
	replayFloor         pq.LSN
	messageCloseOnce    sync.Once
	connMu              sync.Mutex
	closed              atomic.Bool
	sinkStarted         atomic.Bool
	processStarted      atomic.Bool
	flushTickerStarted  atomic.Bool
	openFromSnapshotLSN bool
	// txCommit holds the commit context of the non-streamed transaction
	// currently being decoded, so each of its change messages can be stamped
	// with it. Written and read only on the sink (reader) goroutine.
	txCommit struct {
		time time.Time
		lsn  pq.LSN
		xid  uint32
	}
}

func NewStream(dsn string, cfg config.Config, m metric.Metric, listenerFunc ListenerFunc) Streamer {
	return &stream{
		conn:         pq.NewConnectionTemplate(dsn),
		metric:       m,
		config:       cfg,
		relation:     make(map[uint32]*format.Relation),
		messageCH:    make(chan *Message, 1000),
		listenerFunc: listenerFunc,
		// lastXLogPos:0 is not magical, 0 means, create replication starts with confirmed_flush_lsn
		// https://github.com/postgres/postgres/blob/master/src/include/access/xlogdefs.h#L28
		// https://github.com/postgres/postgres/blob/master/src/backend/replication/logical/logical.c#L540
		lastXLogPos:    0,
		sinkEnd:        make(chan struct{}, 1),
		processEnd:     make(chan struct{}, 1),
		flushTickerEnd: make(chan struct{}, 1),
		fatalCh:        make(chan error, 1),
		mu:             &sync.RWMutex{},
	}
}

func (s *stream) Connect(ctx context.Context) error {
	if err := s.conn.Connect(ctx); err != nil {
		return errors.Wrap(err, "stream connection")
	}

	system, err := pq.IdentifySystem(ctx, s.conn)
	if err != nil {
		_ = s.conn.Close(ctx)
		return errors.Wrap(err, "identify system")
	}

	s.system = &system
	logger.Info("system identification", "systemID", system.SystemID, "timeline", system.Timeline, "xLogPos", system.LoadXLogPos(), "database:", system.Database)
	return nil
}

func (s *stream) Open(ctx context.Context) error {
	if s.conn.IsClosed() {
		return ErrorNotConnected
	}

	if err := s.setup(ctx); err != nil {
		var v *pgconn.PgError
		if goerrors.As(err, &v) && v.Code == "55006" {
			return ErrorSlotInUse
		}
		return errors.Wrap(err, "replication setup")
	}

	// Opened after setup so the ErrorSlotInUse retry loop never churns guard
	// connections; bound to this replication session (same host, same timeline).
	if s.config.VisibilityGuard.Enabled {
		guard, err := openVisibilityGuard(ctx, s.config.DSN(), s.config.VisibilityGuard, s.system.Timeline, s.metric)
		if err != nil {
			return errors.Wrap(err, "visibility guard")
		}
		s.guard = guard
		logger.Info("visibility guard enabled",
			"failMode", s.config.VisibilityGuard.FailMode,
			"timeout", s.config.VisibilityGuard.Timeout,
			"pollInterval", s.config.VisibilityGuard.PollInterval,
		)
		// Standbys share the guard's budget and failMode; they need the primary
		// guard because P5 (sync rep + fail-closed) is what closes the timeline
		// race the replica poll cannot see. See docs/replica-guard-design.md.
		if len(s.config.VisibilityGuard.Replicas) > 0 {
			replicas, err := openReplicaGuard(ctx, s.config, s.system, s.metric)
			if err != nil {
				_ = s.guard.close(ctx)
				s.guard = nil
				return errors.Wrap(err, "replica guard")
			}
			s.replicas = replicas
			logger.Info("replica guard enabled", "replicas", s.config.VisibilityGuard.Replicas)
		}
	}

	s.sinkStarted.Store(true)
	s.processStarted.Store(true)
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	go s.sink(runCtx)
	go s.process(runCtx)
	if s.config.LSNFlushInterval > 0 {
		s.flushTickerStarted.Store(true)
		go s.flushTicker(runCtx)
	}

	logger.Info("cdc stream started")

	return nil
}

// walBacklogWarnStallTicks is how many consecutive flush ticks must pass
// with WAL received but confirmedXLogPos making no forward progress before
// flushTicker warns. At the default 5s LSNFlushInterval that's ~50s -- long
// enough to not fire on ordinary processing latency, short enough to catch
// a genuinely stuck consumer or an idle-but-unheartbeated publication before
// WAL retention becomes an incident. It re-warns every walBacklogWarnStallTicks
// ticks thereafter rather than only once, so the condition isn't reported and
// then forgotten.
const walBacklogWarnStallTicks = 10

// flushTicker periodically sends a standby status update, independent of the
// sink's read-idle timeout and PostgreSQL's own keepalive replies. Under
// sustained throughput (especially with wal_sender_timeout disabled) neither
// of those triggers fires reliably, so confirmed_flush_lsn can stall while
// WAL keeps accumulating. Survives in-process reconnects: s.conn is replaced
// in place, and sendStandbyStatusUpdate always reads the current field.
//
// It also watches for a stalled consumer: WAL keeps arriving but nothing is
// being acked, which prevents the slot from ever advancing (see
// walBacklogWarnStallTicks).
func (s *stream) flushTicker(ctx context.Context) {
	ticker := time.NewTicker(s.config.LSNFlushInterval)
	defer ticker.Stop()
	defer func() { s.flushTickerEnd <- struct{}{} }()

	var lastConfirmed pq.LSN
	var stalledTicks int

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			received := s.LoadXLogPos()
			if received == 0 {
				continue
			}
			if err := s.sendStandbyStatusUpdate(ctx); err != nil {
				logger.Debug("periodic standby status update failed", "error", err)
				continue
			}
			s.updateUnackedLagMetric()

			confirmed := s.LoadConfirmedXLogPos()
			if confirmed >= received || confirmed != lastConfirmed {
				stalledTicks = 0
				lastConfirmed = confirmed
				continue
			}
			stalledTicks++
			if stalledTicks%walBacklogWarnStallTicks == 0 {
				logger.Warn("WAL received but nothing acked for a while; the replication slot cannot advance and WAL will accumulate -- "+
					"if the captured tables are idle while the database is busy, enable heartbeat (config.Heartbeat) so the slot has something to confirm",
					"receivedLSN", received.String(), "confirmedLSN", confirmed.String(),
					"stalledFor", (time.Duration(stalledTicks) * s.config.LSNFlushInterval).String())
				s.metric.WALBacklogWarningIncrement()
			}
		}
	}
}

func (s *stream) setup(ctx context.Context) error {
	replication := New(s.conn)

	replicationStartLsn := s.lastXLogPos
	if s.openFromSnapshotLSN {
		snapshotLSN, err := s.fetchSnapshotLSN(ctx)
		if err != nil {
			return errors.Wrap(err, "fetch snapshot LSN")
		}
		replicationStartLsn = snapshotLSN
	}

	if err := replication.Start(s.config.Publication.Name, s.config.Slot.Name, replicationStartLsn, s.config.Slot.ProtoVersion, s.config.Slot.Streaming, s.config.Slot.Messages); err != nil {
		return err
	}

	if err := replication.Test(ctx); err != nil {
		return err
	}

	if s.openFromSnapshotLSN {
		logger.Info("replication started from snapshot LSN", "slot", s.config.Slot.Name, "lsn", replicationStartLsn.String())
	} else {
		logger.Info("replication started from confirmed_flush_lsn", "slot", s.config.Slot.Name)
	}

	return nil
}

// resetForReconnect clears the in-memory state that must not survive an
// in-process reconnect of the replication stream.
//
//   - txCommit describes the non-streamed transaction currently being
//     decoded. A stale value here would silently stamp the next
//     transaction's changes with the wrong commit context.
//   - openFromSnapshotLSN only applies to the very first connect; leaving it
//     set would make the reconnect replay from the snapshot LSN instead of
//     resuming normally.
//   - lastXLogPos is reset to 0 so setup()'s existing
//     `replicationStartLsn := s.lastXLogPos` sends START_REPLICATION 0/0,
//     telling PostgreSQL to resume from the slot's durable
//     confirmed_flush_lsn. lastXLogPos is the highest position this process
//     has *received*, which can be ahead of what was actually acked;
//     reusing it as the start position would silently skip the un-acked
//     window instead of redelivering it. confirmedXLogPos is left
//     untouched -- it only affects standby status updates this process
//     sends, never the start position.
//
// The messageBuffer and streamTxBuffer used by the sink loop are not reset
// here: the reconnect loop constructs fresh instances per attempt instead,
// which is trivially correct rather than relying on clearing every field of
// reused ones.
func (s *stream) resetForReconnect() {
	s.txCommit.time = time.Time{}
	s.txCommit.lsn = 0
	s.txCommit.xid = 0

	s.openFromSnapshotLSN = false

	s.mu.Lock()
	s.lastXLogPos = 0
	s.mu.Unlock()
}

// messageBuffer manages a one-message look-ahead buffer.
//
// The last DML message in each transaction is held back so its WAL position
// can be rewritten to the transaction-end LSN (from COMMIT / STREAM COMMIT).
// All preceding messages are emitted immediately with their original position.
// This keeps memory usage O(1) regardless of transaction size.
type messageBuffer struct {
	pending   *Message
	outCh     chan<- *Message
	ctx       context.Context
	xid       uint32 // current transaction, from Begin
	commitLSN pq.LSN // current transaction's commit record, from Begin.FinalLSN
}

func (b *messageBuffer) send(msg *Message) bool {
	ctx := b.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case b.outCh <- msg:
		return true
	case <-ctx.Done():
		return false
	}
}

// flush emits the pending message (if any) with its original WAL position.
func (b *messageBuffer) flush() {
	if b.pending != nil {
		if b.send(b.pending) {
			b.pending = nil
		}
	}
}

// flushWithLSN emits the pending message (if any), rewriting its WAL position
// to the given transaction-end LSN. Used at COMMIT.
func (b *messageBuffer) flushWithLSN(lsn pq.LSN) {
	if b.pending != nil {
		markLastInTransaction(b.pending.message)
		if !b.send(&Message{
			message:   b.pending.message,
			walStart:  int64(lsn),
			xid:       b.pending.xid,
			commitLSN: b.pending.commitLSN,
		}) {
			return
		}
		b.pending = nil
	}
}

// discard drops the pending message without emitting.
// Used at BEGIN to reset state.
func (b *messageBuffer) discard() {
	b.pending = nil
}

// buffer stores a new DML message, first flushing any previously pending one.
func (b *messageBuffer) buffer(msg *Message) {
	b.flush()
	b.pending = msg
}

// streamTxBuffer accumulates messages from streaming in-progress transactions.
//
// PostgreSQL streams large transactions in chunks (STREAM START / STREAM STOP)
// before the transaction is committed. Chunks from different transactions may
// be interleaved (e.g. TX-A chunk, TX-B chunk, TX-A chunk, …), so messages
// are stored per-XID in a map.
//
// Messages must NOT be delivered to the consumer until STREAM COMMIT arrives,
// because the transaction may still be rolled back (STREAM ABORT). This mirrors
// how PostgreSQL's own logical replication worker handles streaming: it writes
// to temporary storage and only applies on commit.
type streamTxBuffer struct {
	ctx  context.Context
	txns map[uint32][]*Message
	// subXacts records, per top-level XID, every sub-transaction XID in the
	// order it first appeared together with the txns index of its first
	// message. STREAM ABORT for a sub-transaction truncates txns at that
	// index, mirroring PostgreSQL's worker.c stream_abort_internal.
	subXacts  map[uint32][]subXact
	activeXid uint32
	streaming bool
}

type subXact struct {
	xid   uint32
	index int
}

// startTx marks the beginning of a streaming chunk for the given XID.
func (s *streamTxBuffer) startTx(xid uint32) {
	if s.txns == nil {
		s.txns = make(map[uint32][]*Message)
		s.subXacts = make(map[uint32][]subXact)
	}
	s.activeXid = xid
	s.streaming = true
}

// append adds a message to the currently active streaming transaction.
// xid is the (sub)transaction the message was decoded with; a value other
// than the active top-level XID marks a sub-transaction.
func (s *streamTxBuffer) append(msg *Message, xid uint32) {
	if msg == nil {
		return
	}
	if xid != s.activeXid && !s.hasSubXact(xid) {
		s.subXacts[s.activeXid] = append(s.subXacts[s.activeXid], subXact{xid: xid, index: len(s.txns[s.activeXid])})
	}
	s.txns[s.activeXid] = append(s.txns[s.activeXid], msg)
}

func (s *streamTxBuffer) hasSubXact(xid uint32) bool {
	subs := s.subXacts[s.activeXid]
	// Scan from the tail: a message almost always belongs to the most recent sub-transaction.
	for i := len(subs) - 1; i >= 0; i-- {
		if subs[i].xid == xid {
			return true
		}
	}
	return false
}

// stopTx marks the end of the current streaming chunk.
func (s *streamTxBuffer) stopTx() {
	s.streaming = false
}

// flushTx emits every accumulated message for the given XID through outCh.
// The last message's WAL position is rewritten to the transaction-end LSN
// (endLSN). Every message is stamped with commitLSN, the commit record's own
// LSN, matching what BEGIN.FinalLSN supplies on the non-streamed path -- the
// two positions are not the same value. The wrapper also stores commitLSN so
// the visibility guard and ListenerContext can read it without type-switching.
func (s *streamTxBuffer) flushTx(xid uint32, outCh chan<- *Message, endLSN, commitLSN pq.LSN, commitTime time.Time) {
	s.streaming = false
	msgs := s.txns[xid]
	n := len(msgs)
	for i, msg := range msgs {
		stampCommit(msg.message, commitTime, commitLSN, xid)
		msg.commitLSN = commitLSN
		out := msg
		if i == n-1 {
			markLastInTransaction(msg.message)
			out = &Message{
				message:   msg.message,
				walStart:  int64(endLSN),
				xid:       msg.xid,
				commitLSN: commitLSN,
			}
		}
		ctx := s.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		select {
		case outCh <- out:
		case <-ctx.Done():
			return
		}
	}
	delete(s.txns, xid)
	delete(s.subXacts, xid)
}

// abortTx handles STREAM ABORT. subXid == xid rolls back the whole
// transaction. Otherwise only the sub-transaction is rolled back: every
// message from its first appearance onwards is dropped (later sub-transactions
// are nested in or follow it, so they are gone too); messages before it stay.
func (s *streamTxBuffer) abortTx(xid, subXid uint32) {
	s.streaming = false
	// A top-level abort discards the whole transaction. PostgreSQL sets
	// SubXid equal to Xid for that case. A zero SubXid is the same outcome:
	// the abort did not name a subtransaction, so the buffered changes for
	// the transaction are dropped. A distinct SubXid rolls back only that
	// subtransaction and keeps the parent transaction's earlier rows.
	if subXid == 0 || subXid == xid {
		delete(s.txns, xid)
		delete(s.subXacts, xid)
		return
	}
	subs := s.subXacts[xid]
	for i := len(subs) - 1; i >= 0; i-- {
		if subs[i].xid == subXid {
			s.txns[xid] = s.txns[xid][:subs[i].index]
			s.subXacts[xid] = subs[:i]
			return
		}
	}
	// Unknown sub-transaction: none of its changes reached us, nothing to drop.
}

func (s *stream) sink(ctx context.Context) {
	logger.Info("postgres message sink started")

	var corrupted bool
	for {
		buf := &messageBuffer{outCh: s.messageCH, ctx: ctx}
		streamBuf := &streamTxBuffer{ctx: ctx}
		corrupted = s.sinkLoop(ctx, buf, streamBuf)

		if !corrupted || s.closed.Load() || !s.config.Reconnect.Enabled {
			break
		}
		if !s.reconnect(ctx) {
			break
		}
		logger.Info("replication stream reconnected, resuming sink loop")
	}

	s.messageCloseOnce.Do(func() {
		close(s.messageCH)
	})

	s.sinkEnd <- struct{}{}
	if !s.closed.Load() {
		// The reader can also initiate shutdown after an unexpected disconnect.
		// Do not let a synchronous application listener turn that path into an
		// unbounded wait when the stream context has no deadline.
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), streamShutdownTimeout)
		defer cancel()
		if err := s.Close(closeCtx); err != nil {
			logger.Error("replication stream shutdown failed", "error", err)
		}
		if corrupted {
			// Buffered by 1 and sent at most once per stream, so this never
			// blocks even if nothing is listening on Err().
			s.fatalCh <- ErrStreamCorrupted
		}
	}
}

// reconnect attempts to reopen the replication connection after sinkLoop
// reports a corrupted connection, following s.config.Reconnect's backoff and
// budget (wall-clock MaxElapsed, additionally bounded by MaxAttempts if set).
// It reports whether the connection was successfully reopened; the caller
// falls through to the existing panic when it returns false, exactly as if
// reconnecting were never attempted. Only called when Reconnect.Enabled.
func (s *stream) reconnect(ctx context.Context) bool {
	cfg := s.config.Reconnect
	start := time.Now()

	s.metric.SetReconnecting(true)
	defer s.metric.SetReconnecting(false)

	// retry-go's RandomDelay (part of its default DelayType) calls
	// rand.Int63n(maxJitter) unconditionally and panics if maxJitter is 0.
	// MaxJitter: 0 is a valid, deliberate config choice (no jitter), so fall
	// back to plain exponential backoff instead of feeding it to RandomDelay.
	delayType := retry.BackOffDelay
	if cfg.MaxJitter > 0 {
		delayType = retry.CombineDelay(retry.BackOffDelay, retry.RandomDelay)
	}

	options := []retry.Option{
		retry.Context(ctx),
		retry.Attempts(cfg.MaxAttempts), // 0 == unbounded by count, matches retry-go's own convention
		retry.Delay(cfg.InitialDelay),
		retry.MaxDelay(cfg.MaxDelay),
		retry.MaxJitter(cfg.MaxJitter),
		retry.DelayType(delayType),
		retry.LastErrorOnly(true),
		// MaxElapsed is the meaningful ceiling: a replication slot that is
		// not advancing prevents PostgreSQL from recycling WAL, so retrying
		// forever is not safe here even though MaxAttempts may be 0.
		//
		// A permanent failure (bad credentials after a rotation, dropped
		// database, insufficient privilege, ...) is classified the same way
		// as a startup failure (see pq.IsRetryableConnectionError, shared
		// with cdc.IsRetryableStartupError) and stops retrying immediately
		// instead of burning the whole MaxElapsed budget on an attempt that
		// cannot succeed.
		retry.RetryIf(func(err error) bool {
			return time.Since(start) < cfg.MaxElapsed && pq.IsRetryableConnectionError(errors.Cause(err))
		}),
		retry.OnRetry(func(n uint, err error) {
			logger.Warn("replication stream reconnect attempt failed",
				"attempt", n+1, "elapsed", time.Since(start).String(), "error", err)
		}),
	}

	err := retry.Do(func() error {
		s.metric.ReconnectAttemptIncrement()

		if s.closed.Load() {
			return retry.Unrecoverable(goerrors.New("stream closed"))
		}

		s.resetForReconnect()

		// setup() talks to s.conn's frontend directly (START_REPLICATION,
		// then the handshake read in replication.Test), same as Connect just
		// did. Unlike the very first setup() call from Open (before sink/
		// process exist), this one runs from the live sink goroutine while
		// Close() can be called concurrently by the application at any time
		// -- so the lock must cover setup() too, not just Close+Connect.
		s.connMu.Lock()
		defer s.connMu.Unlock()

		_ = s.conn.Close(ctx)
		if err := s.conn.Connect(ctx); err != nil {
			return err
		}

		return s.setup(ctx)
	}, options...)

	if err != nil {
		if !pq.IsRetryableConnectionError(errors.Cause(err)) {
			logger.Error("giving up on in-process reconnect: permanent error, not retrying", "elapsed", time.Since(start).String(), "error", err)
		} else {
			logger.Error("giving up on in-process reconnect", "elapsed", time.Since(start).String(), "error", err)
		}
		s.metric.ReconnectFailureIncrement()
		return false
	}

	logger.Info("replication stream reconnected", "elapsed", time.Since(start).String())
	s.metric.ReconnectSuccessIncrement()
	s.markReplayFloor()
	return true
}

// sinkLoop reads raw replication messages and dispatches them until the
// connection is closed or a fatal error occurs. It returns true when the
// connection is in a corrupted state and the caller should panic.
func (s *stream) sinkLoop(ctx context.Context, buf *messageBuffer, streamBuf *streamTxBuffer) (corrupted bool) {
	for {
		msgCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), time.Now().Add(300*time.Millisecond))
		// Hold connMu only for the read itself. ReceiveMessage's deferred Unwatch
		// (which clears the socket deadline) has run by the time it returns, so the
		// deadline-toggle window is fully contained here; releasing before the
		// channel sends in handleXLogData keeps acks from blocking the sink and
		// vice versa.
		s.connMu.Lock()
		rawMsg, err := s.conn.ReceiveMessage(msgCtx)
		s.connMu.Unlock()
		cancel()

		if err != nil {
			if s.closed.Load() {
				logger.Info("stream stopped")
				return false
			}
			if handleReplicationConnectionTermination(err) {
				// Connection is gone while we are still running. Close and panic
				// so the process restarts with a fresh replication connection.
				return true
			}
			if pgconn.Timeout(err) {
				if s.LoadXLogPos() > 0 {
					if err = s.sendStandbyStatusUpdate(ctx); err != nil {
						if !handleReplicationConnectionTermination(err) {
							logger.Error("send stand by status update", "error", err)
						}
						return true
					}
					logger.Debug("send stand by status update")
				}
				continue
			}
			logger.Error("receive message error", "error", err)
			return true
		}

		copyData, ok := s.extractCopyData(rawMsg)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case message.PrimaryKeepaliveMessageByteID:
			if err := s.handleKeepalive(ctx, copyData.Data[1:]); err != nil {
				handleReplicationConnectionTermination(err)
				return true
			}
		case message.XLogDataByteID:
			s.handleXLogData(copyData.Data[1:], buf, streamBuf)
		}
	}
}

// extractCopyData validates a raw backend message. It returns the CopyData
// payload and true on success, or (nil, false) for protocol-level errors and
// unexpected message types which are logged and skipped.
func (s *stream) extractCopyData(rawMsg pgproto3.BackendMessage) (*pgproto3.CopyData, bool) {
	if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
		res, _ := errMsg.MarshalJSON()
		logger.Error("receive postgres wal error: " + string(res))
		return nil, false
	}

	msg, ok := rawMsg.(*pgproto3.CopyData)
	if !ok {
		logger.Warn(fmt.Sprintf("received unexpected message: %T", rawMsg))
		return nil, false
	}

	return msg, true
}

// handleKeepalive processes a primary keepalive message, updating the WAL
// position and responding with a standby status update when requested.
// A non-nil return signals a corrupted connection.
func (s *stream) handleKeepalive(ctx context.Context, data []byte) error {
	pkm, err := format.NewPrimaryKeepaliveMessage(data)
	if err != nil {
		logger.Error("decode primary keepalive message", "error", err)
		return nil // non-fatal, skip
	}

	if pkm.ServerWALEnd > 0 {
		s.UpdateXLogPos(pkm.ServerWALEnd)
		logger.Debug("updated xlog position from keepalive", "serverWALEnd", pkm.ServerWALEnd.String())
	}

	if pkm.ReplyRequested {
		if err = s.sendStandbyStatusUpdate(ctx); err != nil {
			logger.Error("standby status update", "error", err)
			return err
		}
		logger.Debug("standby status update sent on keepalive request")
	}

	return nil
}

// handleXLogData parses a WAL data message, decodes the logical replication
// event, and dispatches it through the message buffer.
func (s *stream) handleXLogData(data []byte, buf *messageBuffer, streamBuf *streamTxBuffer) {
	xld, err := ParseXLogData(data)
	if err != nil {
		logger.Error("parse xLog data", "error", err)
		return
	}

	logger.Debug("wal received",
		"walDataLen", len(xld.WALData),
		"walStart", xld.WALStart,
		"walEnd", xld.ServerWALEnd,
		"serverTime", xld.ServerTime,
	)

	s.UpdateXLogPos(xld.ServerWALEnd)
	s.metric.SetCDCLatency(time.Now().UTC().Sub(xld.ServerTime).Nanoseconds())

	decodedMsg, err := message.New(xld.WALData, streamBuf.streaming, xld.ServerTime, s.relation, s.config.SkipTupleMapDecode)
	if err != nil || decodedMsg == nil {
		logger.Debug("wal data message parsing error", "error", err)
		return
	}

	// add LSN to insert/update/delete messages
	switch m := decodedMsg.(type) {
	case *format.Insert:
		m.LSN = xld.WALStart
	case *format.Update:
		m.LSN = xld.WALStart
	case *format.Delete:
		m.LSN = xld.WALStart
	}

	// Streamed changes have no BEGIN to draw commit context from; they are
	// stamped later, at STREAM COMMIT, in streamTxBuffer.flushTx.
	if !streamBuf.streaming {
		if stampCommit(decodedMsg, s.txCommit.time, s.txCommit.lsn, s.txCommit.xid) && s.txCommit.time.IsZero() {
			logger.Warn("change message has no preceding transaction commit context", "lsn", xld.WALStart.String())
		}
	}

	s.dispatchMessage(decodedMsg, xld, buf, streamBuf)
}

// commitLSNOf returns a DML message's stamped CommitLSN, or 0 for message
// types that carry no commit context (e.g. Relation, Truncate).
func commitLSNOf(msg any) pq.LSN {
	switch m := msg.(type) {
	case *format.Insert:
		return m.CommitLSN
	case *format.Update:
		return m.CommitLSN
	case *format.Delete:
		return m.CommitLSN
	default:
		return 0
	}
}

// stampCommit sets the transaction commit context on a DML change message.
// It reports whether msg was a DML message.
func stampCommit(msg any, commitTime time.Time, commitLSN pq.LSN, xid uint32) bool {
	switch m := msg.(type) {
	case *format.Insert:
		m.CommitTime, m.CommitLSN, m.XID = commitTime, commitLSN, xid
	case *format.Update:
		m.CommitTime, m.CommitLSN, m.XID = commitTime, commitLSN, xid
	case *format.Delete:
		m.CommitTime, m.CommitLSN, m.XID = commitTime, commitLSN, xid
	default:
		return false
	}
	return true
}

// markLastInTransaction flags a DML change message as the final one emitted
// for its transaction.
func markLastInTransaction(msg any) {
	switch m := msg.(type) {
	case *format.Insert:
		m.LastInTransaction = true
	case *format.Update:
		m.LastInTransaction = true
	case *format.Delete:
		m.LastInTransaction = true
	}
}

// dispatchMessage routes a decoded logical replication event to the correct
// buffer action.
//
// For regular (non-streaming) transactions the messageBuffer provides a
// one-message look-ahead so the last DML's WAL position can be rewritten to
// the transaction-end LSN at COMMIT.
//
// For streaming transactions (proto v2) messages are accumulated in the
// streamTxBuffer across STREAM START / STREAM STOP chunks. They are only
// emitted to the consumer on STREAM COMMIT and discarded on STREAM ABORT
// (whole transaction or, for a sub-transaction abort, only its own changes).
// This prevents uncommitted data from being delivered.
func (s *stream) dispatchMessage(decodedMsg any, xld XLogData, buf *messageBuffer, streamBuf *streamTxBuffer) {
	switch msg := decodedMsg.(type) {
	case *format.Begin:
		s.txCommit.time = msg.CommitTime
		s.txCommit.lsn = msg.FinalLSN
		s.txCommit.xid = msg.Xid
		buf.discard()
		buf.xid = msg.Xid
		buf.commitLSN = msg.FinalLSN

	case *format.Commit:
		buf.flushWithLSN(msg.TransactionEndLSN)

	case *format.StreamStart:
		// Beginning of a streaming chunk – DML events that follow belong
		// to an in-progress transaction and must be buffered per-XID.
		streamBuf.startTx(msg.Xid)

	case *format.StreamStop:
		// End of a streaming chunk. Nothing is emitted to the consumer.
		streamBuf.stopTx()

	case *format.StreamCommit:
		// Final commit of a streamed transaction – emit all messages for this XID.
		streamBuf.flushTx(msg.Xid, buf.outCh, msg.TransactionEndLSN, msg.CommitLSN, msg.CommitTime)

	case *format.StreamAbort:
		// Whole transaction (SubXid == Xid) or a single sub-transaction
		// (ROLLBACK TO SAVEPOINT, plpgsql EXCEPTION block) rolled back.
		streamBuf.abortTx(msg.Xid, msg.SubXid)

	default:
		// DML event (Insert, Update, Delete, Relation, …)
		m := &Message{
			message:  decodedMsg,
			walStart: int64(xld.WALStart),
		}
		if streamBuf.streaming {
			m.xid = streamBuf.activeXid
			streamBuf.append(m, messageXid(decodedMsg))
		} else {
			m.xid = buf.xid
			m.commitLSN = buf.commitLSN
			buf.buffer(m)
		}
	}
}

// messageXid returns the (sub)transaction XID a streamed message was decoded
// with (proto v2 prefixes every message inside STREAM START/STOP with it).
func messageXid(msg any) uint32 {
	switch m := msg.(type) {
	case *format.Insert:
		return m.XID
	case *format.Update:
		return m.XID
	case *format.Delete:
		return m.XID
	case *format.Truncate:
		return m.XID
	case *format.Relation:
		return m.XID
	}
	return 0
}

func (s *stream) process(ctx context.Context) {
	logger.Info("postgres message process started")
	err := s.processLoop(ctx)
	s.processEnd <- struct{}{}
	if err == nil || s.closed.Load() {
		return
	}

	// Same restart path as the sink's corrupted connection: shut the stream
	// down (flushing the confirmed LSN) and report the error on Err(). The
	// host restarts with fresh connections. The gated message was never
	// acked, so it is redelivered. The error is reported instead of panicking,
	// because a panic in this goroutine kills the host process.
	logger.Error("visibility guard failed, restarting stream", "error", err)
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), streamShutdownTimeout)
	defer cancel()
	if cErr := s.Close(closeCtx); cErr != nil {
		logger.Error("replication stream shutdown failed", "error", cErr)
	}
	select {
	case s.fatalCh <- err:
	default:
	}
}

// processLoop delivers messages to the listener until the context is done or
// the message channel is closed. It returns an error only when the visibility
// guard can no longer certify visibility (failMode: closed, or a guard error).
//
// Everything in messageCH is committed on the server as long as two_phase is
// off: ReorderBufferCommit is reached only from DecodeCommit, and this library
// never requests two_phase. The gate therefore only waits for the commit to
// become visible, never for it to happen.
func (s *stream) processLoop(ctx context.Context) error {
	// The transaction gated last, as (xid, commitLSN): xid alone repeats after
	// wraparound, the pair never does. gated is false until the first gate so
	// that a zero pair is never mistaken for "already gated".
	var lastGated struct {
		commitLSN pq.LSN
		xid       uint32
		gated     bool
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("postgres message process stopped")
			return nil
		case msg, ok := <-s.messageCH:
			if !ok {
				logger.Info("postgres message process stopped")
				return nil
			}
			if msg == nil {
				continue
			}

			// Ack/AckLSN only advance the confirmed position in memory; the standby
			// status update is flushed to Postgres by the sink loop (idle-timeout
			// and keepalive-reply paths) or by flushTicker (config.LSNFlushInterval).
			// Sending it here per message serializes every ack behind the sink's
			// connMu-held read, collapsing throughput to a few messages/sec while
			// a buffered transaction is drained.
			ackLSNFunc := func(lsn pq.LSN) error {
				s.UpdateConfirmedXLogPos(lsn)
				s.updateUnackedLagMetric()
				return nil
			}
			ackFunc := func() error {
				return ackLSNFunc(pq.LSN(msg.walStart))
			}

			if s.isHeartbeatMessage(msg.message) {
				if err := ackFunc(); err != nil {
					logger.Error("heartbeat auto-ack failed", "error", err)
				}
				continue
			}

			// Gate once per transaction, on its first message.
			if s.guard != nil && (!lastGated.gated || msg.xid != lastGated.xid || msg.commitLSN != lastGated.commitLSN) {
				if err := s.gate(ctx, msg.xid, msg.commitLSN); err != nil {
					if ctx.Err() != nil {
						// Shutting down mid-wait: never dispatch an uncertified message.
						// It stays un-acked and is redelivered after restart.
						logger.Info("postgres message process stopped")
						return nil
					}
					return err
				}
				lastGated.xid, lastGated.commitLSN, lastGated.gated = msg.xid, msg.commitLSN, true
			}

			commitLSN := msg.commitLSN
			if commitLSN == 0 {
				commitLSN = commitLSNOf(msg.message)
			}
			lCtx := &ListenerContext{
				Context:   ctx,
				Message:   msg.message,
				WALStart:  pq.LSN(msg.walStart),
				CommitLSN: commitLSN,
				Ack:       ackFunc,
				AckLSN:    ackLSNFunc,
				Xid:       msg.xid,
			}

			if pq.LSN(msg.walStart) <= s.loadReplayFloor() {
				s.metric.ReplayedMessageIncrement()
			}
			s.updateDeliveredHighWaterMark(pq.LSN(msg.walStart))

			switch lCtx.Message.(type) {
			case *format.Insert:
				s.metric.InsertOpIncrement(1)
			case *format.Delete:
				s.metric.DeleteOpIncrement(1)
			case *format.Update:
				s.metric.UpdateOpIncrement(1)
			}

			start := time.Now().UTC()
			s.listenerFunc(lCtx)
			s.metric.SetProcessLatency(time.Since(start).Nanoseconds())
		}
	}
}

// gate waits until xid is visible on the primary and, when replicas are
// configured, until every listed standby has replayed commitLSN; both waits
// share one deadline of visibilityGuard.timeout from the start of the gate.
// A timeout is fatal under failMode closed and a logged, counted pass-through
// under failMode open; any other guard error is fatal in both modes.
func (s *stream) gate(ctx context.Context, xid uint32, commitLSN pq.LSN) error {
	if xid == 0 || commitLSN == 0 {
		// Every decoded BEGIN / STREAM COMMIT carries a valid xid and the commit
		// record's LSN; snapshot events, the only messages without them, never
		// pass through here (connector.snapshotHandler). A zero here means the
		// message was decoded without its BEGIN, so nothing can be certified:
		// fail closed instead of gating xid 0 and skipping the standbys.
		return fmt.Errorf("%w: message without a decoded BEGIN (xid %d, commitLSN %s)", ErrVisibilityGuard, xid, commitLSN)
	}
	start := time.Now()
	err := s.guard.wait(ctx, xid)
	if err == nil && s.replicas != nil {
		err = s.replicas.wait(ctx, xid, commitLSN, start.Add(s.config.VisibilityGuard.Timeout))
	}
	s.metric.ObserveVisibilityWait(time.Since(start))
	switch {
	case err == nil:
		return nil
	case goerrors.Is(err, ErrVisibilityTimeout) && s.config.VisibilityGuard.FailMode == config.VisibilityFailOpen:
		logger.Warn("visibility guard timeout, dispatching anyway (failMode: open)", "xid", xid, "error", err)
		s.metric.VisibilityFailOpenIncrement()
		return nil
	default:
		return fmt.Errorf("%w: %w", ErrVisibilityGuard, err)
	}
}

func (s *stream) isHeartbeatMessage(msg any) bool {
	if !s.config.IsHeartbeatEnabled() {
		return false
	}

	hbSchema := s.config.Heartbeat.Table.Schema
	hbTable := s.config.Heartbeat.Table.Name

	switch m := msg.(type) {
	case *format.Insert:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	case *format.Update:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	case *format.Delete:
		return m.TableNamespace == hbSchema && m.TableName == hbTable
	}

	return false
}

func (s *stream) Close(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, streamShutdownTimeout)
		defer cancel()
	}
	var errs []error

	// Cancel first so a process goroutine blocked in the visibility guard (or a
	// context-aware listener) returns instead of waiting out its timeout.
	if s.cancel != nil {
		s.cancel()
	}

	// Close the PostgreSQL socket before waiting on the goroutines. A listener
	// callback is synchronous and may be blocked outside this package; waiting
	// for it before closing the socket can leave the walsender connection open
	// for the whole shutdown timeout.
	s.connMu.Lock()
	if !s.conn.IsClosed() {
		s.flushFinalStandbyStatusUpdateLocked(ctx)
		if err := s.conn.Close(ctx); err != nil {
			errs = append(errs, err)
		}
		logger.Info("postgres connection closed")
	}
	s.connMu.Unlock()

	if s.sinkStarted.Load() {
		select {
		case <-s.sinkEnd:
			logger.Info("postgres message sink stopped")
		case <-ctx.Done():
			logger.Warn("timed out waiting for postgres message sink", "error", ctx.Err())
			errs = append(errs, errors.Wrap(ctx.Err(), "wait for postgres message sink"))
		}
	}

	if s.processStarted.Load() {
		select {
		case <-s.processEnd:
			logger.Info("postgres message process stopped")
		case <-ctx.Done():
			logger.Warn("timed out waiting for postgres message process", "error", ctx.Err())
			errs = append(errs, errors.Wrap(ctx.Err(), "wait for postgres message process"))
		}
	}

	if s.flushTickerStarted.Load() {
		select {
		case <-s.flushTickerEnd:
			logger.Info("postgres flush ticker stopped")
		case <-ctx.Done():
			logger.Warn("timed out waiting for postgres flush ticker", "error", ctx.Err())
			errs = append(errs, errors.Wrap(ctx.Err(), "wait for postgres flush ticker"))
		}
	}

	// Only the process goroutine uses the guard connection; close it after that
	// goroutine has stopped so the close never races an in-flight poll.
	if s.guard != nil {
		if err := s.guard.close(ctx); err != nil {
			errs = append(errs, errors.Wrap(err, "close visibility guard"))
		}
		logger.Info("visibility guard connection closed")
	}
	if s.replicas != nil {
		if err := s.replicas.close(ctx); err != nil {
			errs = append(errs, errors.Wrap(err, "close replica guard"))
		}
		logger.Info("replica guard connections closed")
	}

	return goerrors.Join(errs...)
}

func isReplicationConnectionTerminationError(err error) bool {
	if goerrors.Is(err, io.EOF) ||
		goerrors.Is(err, io.ErrUnexpectedEOF) ||
		goerrors.Is(err, net.ErrClosed) ||
		goerrors.Is(err, syscall.ECONNRESET) ||
		goerrors.Is(err, syscall.EPIPE) {
		return true
	}

	return false
}

func isPostgresShutdownError(err error) bool {
	if pgErr, ok := goerrors.AsType[*pgconn.PgError](err); ok {
		switch pgErr.Code {
		case postgresAdminShutdown, postgresCrashShutdown:
			return true
		default:
			return false
		}
	}

	return false
}

func handleReplicationConnectionTermination(err error) bool {
	if isPostgresShutdownError(err) {
		logger.Info("postgres replication connection closed", "error", err)
		return true
	}
	if isReplicationConnectionTerminationError(err) {
		if goerrors.Is(err, io.ErrUnexpectedEOF) {
			logger.Error("postgres replication connection terminated unexpectedly", "error", err)
		} else {
			logger.Info("postgres replication connection closed", "error", err)
		}
		return true
	}
	return false
}

func (s *stream) GetSystemInfo() *pq.IdentifySystemResult {
	return s.system
}

func (s *stream) GetMetric() metric.Metric {
	return s.metric
}

func (s *stream) Err() <-chan error {
	return s.fatalCh
}

func (s *stream) SetSnapshotLSN(lsn pq.LSN) {
	s.snapshotLSN = lsn
}

func (s *stream) UpdateXLogPos(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.lastXLogPos < l {
		s.lastXLogPos = l
	}
}

func (s *stream) LoadXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastXLogPos
}

func (s *stream) UpdateConfirmedXLogPos(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.confirmedXLogPos < l {
		s.confirmedXLogPos = l
	}
}

func (s *stream) LoadConfirmedXLogPos() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.confirmedXLogPos
}

func (s *stream) updateDeliveredHighWaterMark(l pq.LSN) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.deliveredHighWaterMark < l {
		s.deliveredHighWaterMark = l
	}
}

func (s *stream) loadDeliveredHighWaterMark() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.deliveredHighWaterMark
}

// markReplayFloor records the current delivered high-water mark as the
// replay floor after a successful reconnect. It never decreases:
// deliveredHighWaterMark itself is monotonic, so a later reconnect can only
// raise the floor, matching the same update-if-greater pattern used
// elsewhere in this file.
func (s *stream) markReplayFloor() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.replayFloor < s.deliveredHighWaterMark {
		s.replayFloor = s.deliveredHighWaterMark
	}
}

func (s *stream) loadReplayFloor() pq.LSN {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.replayFloor
}

// updateUnackedLagMetric reports how far the highest received WAL position is
// ahead of the highest position the consumer has acked. Unlike slot_lag (a
// server-reported gauge refreshed on a polling interval), this is computed
// from this process's own in-memory state and distinguishes "the consumer is
// behind" from "we simply haven't flushed yet".
func (s *stream) updateUnackedLagMetric() {
	received, confirmed := s.LoadXLogPos(), s.LoadConfirmedXLogPos()
	lag := float64(0)
	if received > confirmed {
		lag = float64(received - confirmed)
	}
	s.metric.SetUnackedLSNLag(lag)
}

func (s *stream) OpenFromSnapshotLSN() {
	s.openFromSnapshotLSN = true
}

// fetchSnapshotLSN queries the database to get the snapshot LSN from cdc_snapshot_job table
// Uses infinite retry with exponential backoff for resilience against transient database errors
func (s *stream) fetchSnapshotLSN(ctx context.Context) (pq.LSN, error) {
	logger.Info("fetching snapshot LSN from database", "slotName", s.config.Slot.Name)

	var snapshotLSN pq.LSN

	err := retry.Do(
		func() error {
			// Create a separate connection for querying metadata
			// Use regular DSN (not replication DSN) for normal SQL queries
			conn, err := pq.NewConnection(ctx, s.config.DSN())
			if err != nil {
				return errors.Wrap(err, "create connection for snapshot LSN query")
			}
			defer conn.Close(ctx)

			query := fmt.Sprintf(`
				SELECT snapshot_lsn, completed 
				FROM cdc_snapshot_job 
				WHERE slot_name = '%s'
			`, s.config.Slot.Name)

			resultReader := conn.Exec(ctx, query)
			results, err := resultReader.ReadAll()
			if err != nil {
				resultReader.Close()
				return errors.Wrap(err, "execute snapshot LSN query")
			}

			if err = resultReader.Close(); err != nil {
				return errors.Wrap(err, "close result reader")
			}

			if len(results) == 0 || len(results[0].Rows) == 0 {
				return retry.Unrecoverable(errors.New("no snapshot job found for slot: " + s.config.Slot.Name))
			}

			row := results[0].Rows[0]

			completed := string(row[1]) == "true" || string(row[1]) == "t"
			if !completed {
				return errors.New("snapshot job not completed yet for slot: " + s.config.Slot.Name)
			}

			lsnStr := string(row[0])
			if lsnStr == "" {
				return retry.Unrecoverable(errors.New("empty snapshot LSN result"))
			}

			snapshotLSN, err = pq.ParseLSN(lsnStr)
			if err != nil {
				return retry.Unrecoverable(errors.Wrap(err, "parse snapshot LSN: "+lsnStr))
			}

			return nil
		},
		retry.Attempts(0),                   // 0 means infinite retries
		retry.DelayType(retry.BackOffDelay), // Exponential backoff
		retry.OnRetry(func(n uint, err error) {
			logger.Error("error in snapshot LSN fetch, retrying",
				"attempt", n+1,
				"error", err,
				"slotName", s.config.Slot.Name)
		}),
	)
	if err != nil {
		return 0, errors.Wrap(err, "failed to fetch snapshot LSN")
	}

	logger.Info("fetched snapshot LSN from database", "slotName", s.config.Slot.Name, "snapshotLSN", snapshotLSN.String())
	return snapshotLSN, nil
}

// sendStandbyStatusUpdate writes a standby status update under connMu so it can
// never overlap the sink loop's ReceiveMessage, which toggles the connection's
// socket deadline. Every status-update write — idle keepalive, reply-on-request,
// and consumer Ack — must go through here rather than calling
// SendStandbyStatusUpdate directly. See the connMu field comment.
func (s *stream) sendStandbyStatusUpdate(ctx context.Context) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	return SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()), uint64(s.LoadConfirmedXLogPos()))
}

// flushFinalStandbyStatusUpdateLocked sends the final confirmed position.
// The caller must hold connMu.
func (s *stream) flushFinalStandbyStatusUpdateLocked(ctx context.Context) {
	if s.LoadConfirmedXLogPos() == 0 {
		return
	}
	if err := SendStandbyStatusUpdate(ctx, s.conn, uint64(s.LoadXLogPos()), uint64(s.LoadConfirmedXLogPos())); err != nil {
		logger.Warn("final standby status update failed, updates may duplicate on restart", "error", err)
		return
	}
	logger.Debug("final standby status update sent")
}

func SendStandbyStatusUpdate(_ context.Context, conn pq.Connection, walReceivedPosition, walFlushedPosition uint64) error {
	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = AppendUint64(data, walReceivedPosition)
	data = AppendUint64(data, walFlushedPosition)
	data = AppendUint64(data, walFlushedPosition)
	data = AppendUint64(data, timeToPgTime(time.Now()))
	data = append(data, 0)

	cd := &pgproto3.CopyData{Data: data}
	buf, err := cd.Encode(nil)
	if err != nil {
		return err
	}

	return conn.Frontend().SendUnbufferedEncodedCopyData(buf)
}

func AppendUint64(buf []byte, n uint64) []byte {
	wp := len(buf)
	buf = append(buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(buf[wp:], n)
	return buf
}

func timeToPgTime(t time.Time) uint64 {
	return uint64(t.UTC().UnixMicro() - microSecFromUnixEpochToY2K)
}
