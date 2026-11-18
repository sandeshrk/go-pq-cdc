package snapshot

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/go-playground/errors"
	"github.com/jackc/pgx/v5/pgtype"
)

// Sentinel errors for snapshot operations
var (
	// ErrSnapshotInvalidated indicates the snapshot transaction was closed (coordinator restart)
	ErrSnapshotInvalidated = goerrors.New("snapshot invalidated by coordinator restart")
)

// errSnapshotClosed distinguishes requested shutdown from caller cancellation.
var errSnapshotClosed = goerrors.New("snapshot close requested")

// Handler SnapshotHandler is a function that handles snapshot events
type Handler func(event *format.Snapshot) error

// ContextHandler receives the snapshot operation context, canceled by Close.
// Handlers should honor cancellation when blocked on downstream work.
type ContextHandler func(context.Context, *format.Snapshot) error

type Snapshotter struct {
	operationMu        sync.Mutex
	cancelMu           sync.Mutex
	operationCancel    context.CancelCauseFunc
	operationDone      chan struct{}
	resourcesClosed    bool
	closed             bool
	slotSnapshot       *slot.Snapshot
	healthcheckConn    pq.Connection
	exportSnapshotConn pq.Connection
	metric             metric.Metric
	metadataConn       pq.Connection
	keepaliveCancel    context.CancelFunc
	typeMap            *pgtype.Map
	decoderCache       *DecoderCache
	connectionPool     *ConnectionPool
	orderByCache       map[string]orderByCacheEntry
	keepaliveDone      chan struct{}
	dsn                string
	cachedSnapshotID   string
	tables             publication.Tables
	config             config.SnapshotConfig
	orderByMu          sync.RWMutex
	keepaliveMu        sync.Mutex
	exportConnClosed   bool
}

type orderByCacheEntry struct {
	clause  string
	columns []string
}

func New(ctx context.Context, snapshotConfig config.SnapshotConfig, tables publication.Tables, dsn string, m metric.Metric) (*Snapshotter, error) {
	metadataConn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		return nil, errors.Wrap(err, "create metadata connection")
	}

	healthcheckConn, err := pq.NewConnection(ctx, dsn)
	if err != nil {
		_ = metadataConn.Close(ctx)
		return nil, errors.Wrap(err, "create healthcheck connection")
	}

	// Create connection pool for chunk processing (5 connections)
	connectionPool, err := NewConnectionPool(ctx, dsn, 5)
	if err != nil {
		_ = metadataConn.Close(ctx)
		_ = healthcheckConn.Close(ctx)
		return nil, errors.Wrap(err, "create connection pool")
	}

	// Create decoder cache for efficient type decoding
	decoderCache := NewDecoderCache()

	return &Snapshotter{
		dsn:             dsn,
		metadataConn:    metadataConn,
		healthcheckConn: healthcheckConn,
		connectionPool:  connectionPool,
		decoderCache:    decoderCache,
		config:          snapshotConfig,
		tables:          tables,
		typeMap:         pgtype.NewMap(),
		metric:          m,
		orderByCache:    make(map[string]orderByCacheEntry),
	}, nil
}

// Prepare sets up a snapshot-only export without a replication boundary.
// CDC callers must use PrepareWithSlot instead. Execute reads the prepared view.
func (s *Snapshotter) Prepare(ctx context.Context, slotName string) error {
	if s.config.Mode != config.SnapshotModeSnapshotOnly {
		return errors.New("initial snapshots require PrepareWithSlot and a fresh slot export")
	}
	return s.prepare(ctx, slotName, nil)
}

// PrepareWithSlot elects a coordinator and creates an atomic slot snapshot.
// Workers join the same export; it is retained until all chunks finish or Close.
func (s *Snapshotter) PrepareWithSlot(ctx context.Context, slotName string, sl *slot.Slot) error {
	if sl == nil {
		return errors.New("snapshot replication slot is required")
	}
	return s.prepare(ctx, slotName, sl)
}

// prepare initializes a coordinated generation. A failed setup releases all
// sessions and the coordinator lock; callers must build a new snapshotter.
func (s *Snapshotter) prepare(ctx context.Context, slotName string, sl *slot.Slot) error {
	return s.runOperation(ctx, func(ctx context.Context) error {
		instanceID := generateInstanceID(s.config.InstanceID)
		if _, err := s.setupJob(ctx, slotName, instanceID, sl); err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			s.closeAllConnections(cleanupCtx, false)
			return errors.Wrap(err, "setup job")
		}
		return nil
	})
}

// Execute performs the actual snapshot data collection
// Call Prepare or PrepareWithSlot first. It returns after every chunk completes,
// or fails if the original snapshot can no longer be imported.
func (s *Snapshotter) Execute(ctx context.Context, handler Handler, slotName string) error {
	return s.ExecuteWithContext(ctx, func(_ context.Context, event *format.Snapshot) error { return handler(event) }, slotName)
}

// ExecuteWithContext reads the prepared snapshot with cancelable, serialized
// callbacks. A callback may call Close. An uncooperative callback cannot prevent
// session cleanup, but its goroutine remains until the callback returns.
func (s *Snapshotter) ExecuteWithContext(ctx context.Context, handler ContextHandler, slotName string) error {
	return s.runOperation(ctx, func(ctx context.Context) error {
		dispatchCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		dispatch := snapshotDispatcher(dispatchCtx, handler)
		return s.execute(ctx, dispatch, slotName)
	})
}

// snapshotDispatcher serializes callbacks outside the database operation. The
// operation can leave on cancellation without waiting on the callback that
// requested Close, or on a downstream listener that ignores cancellation.
func snapshotDispatcher(ctx context.Context, handler ContextHandler) Handler {
	requests := make(chan *format.Snapshot)
	results := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-requests:
				if ctx.Err() != nil {
					return
				}
				err := handler(ctx, event)
				select {
				case results <- err:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return func(event *format.Snapshot) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {
		case requests <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-results:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// execute reads one prepared generation while runOperation owns its sessions.
func (s *Snapshotter) execute(ctx context.Context, handler Handler, slotName string) error {
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.closeAllConnections(cleanupCtx, false)
	}()
	if s.exportSnapshotConn != nil {
		s.startSnapshotKeepalive(ctx, s.exportSnapshotConn)
	}
	startTime := time.Now()
	instanceID := generateInstanceID(s.config.InstanceID)
	logger.Debug("[snapshot] executing", "instanceID", instanceID)

	// Load job
	job, err := s.loadJob(ctx, slotName)
	if err != nil || job == nil {
		return errors.New("job not found - Prepare() must be called first")
	}

	if job.SnapshotID != s.cachedSnapshotID {
		return ErrSnapshotInvalidated
	}

	// Execute worker processing (ALL instances work, including coordinator)
	if err := s.executeWorker(ctx, slotName, instanceID, job, handler, startTime); err != nil {
		return fmt.Errorf("execute worker: %w", err)
	}

	// Finalize (check completion, send END marker)
	if err := s.finalizeSnapshot(ctx, slotName, job, handler); err != nil {
		return errors.Wrap(err, "finalize snapshot")
	}

	logger.Info("[snapshot] execution completed", "instanceID", instanceID, "duration", time.Since(startTime))
	return nil
}

// finalizeSnapshot checks completion, closes connections, and sends END marker
func (s *Snapshotter) finalizeSnapshot(ctx context.Context, slotName string, job *Job, handler Handler) error {
	allCompleted, err := s.checkJobCompleted(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "check job completed")
	}

	if !allCompleted {
		return errors.New("snapshot still has unfinished chunks")
	}

	logger.Info("[snapshot] all chunks completed, finalizing snapshot")

	// Mark job as completed (idempotent - safe for multiple workers)
	if err := s.markJobAsCompleted(ctx, slotName, job.SnapshotID); err != nil {
		logger.Warn("[snapshot] failed to mark job as completed", "error", err)
		return err
	}

	// Close all connections now that snapshot is complete
	s.closeAllConnections(ctx, true)

	// The job is durable before END. Only an explicit Close may turn callback
	// cancellation into success; caller cancellation and delivery errors remain.
	return sendSnapshotEnd(ctx, handler, &format.Snapshot{
		EventType:  format.SnapshotEventTypeEnd,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	})
}

// sendSnapshotEnd delivers END for a completed job. Close from that callback is
// a clean shutdown; other callback errors and caller cancellation are returned.
func sendSnapshotEnd(ctx context.Context, handler Handler, event *format.Snapshot) error {
	err := handler(event)
	if goerrors.Is(err, context.Canceled) && goerrors.Is(context.Cause(ctx), errSnapshotClosed) {
		return nil
	}
	return err
}

// closeAllConnections closes all snapshot connections
// commitExport: if true, commits the export snapshot transaction; if false, rolls it back
func (s *Snapshotter) closeAllConnections(ctx context.Context, commitExport bool) {
	s.cancelMu.Lock()
	s.closed = true
	if s.resourcesClosed {
		s.cancelMu.Unlock()
		return
	}
	s.resourcesClosed = true
	s.cancelMu.Unlock()
	logger.Info("[snapshot] closing all connections")

	// Close export snapshot connection (coordinator only)
	s.closeExportSnapshotConnection(ctx, commitExport)
	if s.slotSnapshot != nil {
		_ = s.slotSnapshot.Close(ctx)
		s.slotSnapshot = nil
	}

	// Close connection pool
	if s.connectionPool != nil {
		s.connectionPool.Close(ctx)
		s.connectionPool = nil
	}

	// Close metadata connection
	if s.metadataConn != nil {
		if err := s.metadataConn.Close(ctx); err != nil {
			logger.Warn("[snapshot] error closing metadata connection", "error", err)
		}
		s.metadataConn = nil
	}

	// Close healthcheck connection
	if s.healthcheckConn != nil {
		if err := s.healthcheckConn.Close(ctx); err != nil {
			logger.Warn("[snapshot] error closing healthcheck connection", "error", err)
		}
		s.healthcheckConn = nil
	}

	logger.Info("[snapshot] all connections closed")
}

// closeExportSnapshotConnection commits or rolls back and closes the export snapshot connection
func (s *Snapshotter) closeExportSnapshotConnection(ctx context.Context, commit bool) {
	s.stopSnapshotKeepalive()

	s.keepaliveMu.Lock()
	if s.exportConnClosed {
		s.keepaliveMu.Unlock()
		return
	}
	s.exportConnClosed = true
	exportConn := s.exportSnapshotConn
	s.keepaliveMu.Unlock()

	if exportConn == nil {
		return
	}

	if commit {
		logger.Info("[coordinator] committing and closing snapshot export connection")
		if err := s.execSQL(ctx, exportConn, "COMMIT"); err != nil {
			logger.Warn("[coordinator] failed to commit snapshot transaction, attempting rollback", "error", err)
			if rollbackErr := s.execSQL(ctx, exportConn, "ROLLBACK"); rollbackErr != nil {
				logger.Error("[coordinator] failed to rollback snapshot transaction", "error", rollbackErr)
			}
		}
	} else {
		logger.Info("[coordinator] rolling back and closing snapshot export connection")
		if err := s.execSQL(ctx, exportConn, "ROLLBACK"); err != nil {
			logger.Warn("[coordinator] failed to rollback snapshot transaction", "error", err)
		}
	}

	if err := exportConn.Close(ctx); err != nil {
		logger.Warn("[coordinator] error closing export snapshot connection", "error", err)
	}

	s.keepaliveMu.Lock()
	s.exportSnapshotConn = nil
	s.keepaliveMu.Unlock()
}

// Close cancels snapshot work and releases its sessions. It waits for the
// operation only until ctx is done; deferred cleanup continues after a timeout.
// Calling Close from a snapshot callback is safe and does not join that callback.
func (s *Snapshotter) Close(ctx context.Context) {
	if s == nil {
		return
	}
	s.cancelMu.Lock()
	s.closed = true
	done := s.operationDone
	if s.operationCancel != nil {
		s.operationCancel(errSnapshotClosed)
	}
	s.cancelMu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
	s.closeAllConnections(ctx, false)
}

// runOperation serializes access to snapshot sessions. Close cancels an active
// operation and waits for it to leave before closing its connections.
func (s *Snapshotter) runOperation(ctx context.Context, fn func(context.Context) error) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	s.cancelMu.Lock()
	if s.closed {
		s.cancelMu.Unlock()
		return errors.New("snapshotter is closed; build a new one to restart")
	}
	ctx, cancel := context.WithCancelCause(ctx)
	s.operationCancel = cancel
	done := make(chan struct{})
	s.operationDone = done
	s.cancelMu.Unlock()
	defer func() {
		cancel(nil)
		s.cancelMu.Lock()
		s.operationCancel = nil
		s.operationDone = nil
		close(done)
		s.cancelMu.Unlock()
	}()
	return fn(ctx)
}

func (s *Snapshotter) startSnapshotKeepalive(parentCtx context.Context, conn pq.Connection) {
	keepaliveCtx, keepaliveCancel := context.WithCancel(parentCtx)
	done := make(chan struct{})

	s.keepaliveMu.Lock()
	// Ensure previous keepalive is not left around during retries.
	if s.keepaliveCancel != nil {
		s.keepaliveCancel()
	}
	s.keepaliveCancel = keepaliveCancel
	s.keepaliveDone = done
	s.exportConnClosed = false
	s.keepaliveMu.Unlock()

	go s.snapshotTransactionKeepalive(keepaliveCtx, conn, done)
}

func (s *Snapshotter) stopSnapshotKeepalive() {
	s.keepaliveMu.Lock()
	cancel := s.keepaliveCancel
	done := s.keepaliveDone
	s.keepaliveCancel = nil
	s.keepaliveDone = nil
	s.keepaliveMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

// decodeColumnData decodes PostgreSQL column data using cached decoder
func (s *Snapshotter) decodeColumnData(data []byte, dataTypeOID uint32) (any, error) {
	// Use cached decoder (optimization: avoid reflection overhead)
	decoder := s.decoderCache.Get(dataTypeOID)
	return decoder.Decode(s.typeMap, data)
}

// generateInstanceID generates a unique instance identifier
func generateInstanceID(configuredID string) string {
	if configuredID != "" {
		return configuredID
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	pid := os.Getpid()
	return fmt.Sprintf("%s-%d", hostname, pid)
}
