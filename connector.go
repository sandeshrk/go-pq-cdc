package cdc

import (
	"context"
	goerrors "errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/heartbeat"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot"

	"github.com/Trendyol/go-pq-cdc/pq/timescaledb"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/internal/http"
	"github.com/Trendyol/go-pq-cdc/internal/metric"
	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/avast/retry-go/v4"
	"github.com/go-playground/errors"
	"github.com/prometheus/client_golang/prometheus"
)

type Connector interface {
	Start(ctx context.Context)
	// Run blocks until shutdown, returning nil on a clean signal-driven or
	// caller-context-cancelled shutdown, or the fatal error that stopped the
	// pipeline (a bootstrap failure, or a recovered panic from one of the
	// connector's background goroutines). Start is a thin wrapper around Run
	// that discards the error, for backward compatibility.
	//
	// A connector is one-shot: once Run/Start returns for any reason, a
	// second call returns ErrConnectorConsumed immediately. Build a new
	// connector to retry.
	Run(ctx context.Context) error
	WaitUntilReady(ctx context.Context) error
	Close()
	GetConfig() *config.Config
	SetMetricCollectors(collectors ...prometheus.Collector)
}

type connector struct {
	heartbeat          *heartbeat.Heartbeat
	prometheusRegistry metric.Registry
	server             http.Server
	stream             replication.Streamer
	timescaleDB        *timescaledb.TimescaleDB
	slot               *slot.Slot
	cancelCh           chan os.Signal
	readyCh            chan struct{}
	// fatalCh receives a recovered panic from any of the connector's
	// background goroutines (see runGuarded), so Run can report it instead of
	// the whole process crashing invisibly to the caller.
	fatalCh      chan error
	cfg          *config.Config
	snapshotter  *snapshot.Snapshotter
	listenerFunc replication.ListenerFunc
	once         sync.Once
	closeOnce    sync.Once
	// consumed makes Run one-shot: internal latches (closeOnce, messageCH,
	// signal registration) cannot be safely reused across a second call.
	consumed atomic.Bool
}

func NewConnectorWithConfigFile(ctx context.Context, configFilePath string, listenerFunc replication.ListenerFunc) (Connector, error) {
	var cfg config.Config
	var err error

	if strings.HasSuffix(configFilePath, ".json") {
		cfg, err = config.ReadConfigJSON(configFilePath)
	}

	if strings.HasSuffix(configFilePath, ".yml") || strings.HasSuffix(configFilePath, ".yaml") {
		cfg, err = config.ReadConfigYAML(configFilePath)
	}

	if err != nil {
		return nil, err
	}

	return NewConnector(ctx, cfg, listenerFunc)
}

func NewConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	cfg.SetDefault()
	if err := cfg.Validate(); err != nil {
		return nil, errors.Wrap(err, "config validation")
	}
	cfg.Print()

	logger.InitLogger(cfg.Logger.Logger)

	// Snapshot-only mode: minimal setup without CDC components
	if cfg.IsSnapshotOnlyMode() {
		return newSnapshotOnlyConnector(ctx, cfg, listenerFunc)
	}

	// Normal CDC mode: full setup with publication, slot, stream
	// Normal connection for publication setup
	// This uses regular DSN (no replication parameter) to avoid consuming max_wal_senders limit
	conn, err := pq.NewConnection(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}

	hb, publicationInfo, err := setupHeartbeatAndPublication(ctx, cfg, conn)
	if err != nil {
		conn.Close(ctx)
		return nil, err
	}
	conn.Close(ctx)

	m := metric.NewMetric(cfg.Slot.Name)

	// Get tables to snapshot (either from snapshot.tables or publication.tables)
	snapshotTables, err := cfg.GetSnapshotTables(publicationInfo)
	if err != nil {
		return nil, errors.Wrap(err, "get snapshot tables")
	}

	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables, m)
	if err != nil {
		return nil, err
	}

	stream := replication.NewStream(cfg.ReplicationDSN(), cfg, m, listenerFunc)

	sl := slot.NewSlot(cfg.ReplicationDSN(), cfg.DSN(), cfg.Slot, m, stream.(slot.XLogUpdater))

	prometheusRegistry := metric.NewRegistry(m)

	tdb, err := initializeTimescaleDB(ctx, cfg)
	if err != nil {
		if snapshotter != nil {
			snapshotter.Close(ctx)
		}
		return nil, err
	}

	return &connector{
		cfg:                &cfg,
		stream:             stream,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry, sl),
		slot:               sl,
		heartbeat:          hb,
		timescaleDB:        tdb,
		snapshotter:        snapshotter,
		listenerFunc:       listenerFunc,
		cancelCh:           make(chan os.Signal, 1),
		readyCh:            make(chan struct{}, 1),
		fatalCh:            make(chan error, 4),
	}, nil
}

// newSnapshotOnlyConnector creates a minimal connector for snapshot-only mode
// without CDC components (publication, slot, replication stream)
func newSnapshotOnlyConnector(ctx context.Context, cfg config.Config, listenerFunc replication.ListenerFunc) (Connector, error) {
	// Use a dummy metric name since we don't have a slot
	m := metric.NewMetric("snapshot_only")

	// Get tables to snapshot from snapshot.tables
	snapshotTables, err := cfg.GetSnapshotTables(nil) // nil publicationInfo for snapshot_only mode
	if err != nil {
		return nil, errors.Wrap(err, "get snapshot tables")
	}

	// Initialize snapshotter with tables from snapshot config
	snapshotter, err := initializeSnapshot(ctx, cfg, snapshotTables, m)
	if err != nil {
		return nil, err
	}

	prometheusRegistry := metric.NewRegistry(m)

	logger.Info("snapshot-only mode enabled", "tables", len(snapshotTables))

	return &connector{
		cfg:                &cfg,
		prometheusRegistry: prometheusRegistry,
		server:             http.NewServer(cfg, prometheusRegistry, nil),
		snapshotter:        snapshotter,
		listenerFunc:       listenerFunc,
		cancelCh:           make(chan os.Signal, 1),
		readyCh:            make(chan struct{}, 1),
		fatalCh:            make(chan error, 4),
		// CDC components left nil: system, stream, slot
	}, nil
}

func setupHeartbeatAndPublication(ctx context.Context, cfg config.Config, conn pq.Connection) (*heartbeat.Heartbeat, *publication.Config, error) {
	var hb *heartbeat.Heartbeat
	if cfg.IsHeartbeatEnabled() {
		hb = heartbeat.New(cfg.DSN(), cfg.Heartbeat)
		if err := hb.EnsureTable(ctx, conn); err != nil {
			return nil, nil, errors.Wrap(err, "create heartbeat table")
		}
	}

	publicationInfo, err := initializePublication(ctx, cfg, conn)
	if err != nil {
		return nil, nil, err
	}
	if err := cfg.ValidateHeartbeatInPublication(publicationInfo); err != nil {
		return nil, nil, err
	}
	logger.Info("publication", "info", publicationInfo)
	return hb, publicationInfo, nil
}

func initializeTimescaleDB(ctx context.Context, cfg config.Config) (*timescaledb.TimescaleDB, error) {
	if !cfg.ExtensionSupport.EnableTimeScaleDB {
		return nil, nil
	}
	tdb, err := timescaledb.NewTimescaleDB(ctx, cfg.DSN())
	if err != nil {
		return nil, err
	}
	if _, err = tdb.FindHyperTables(ctx); err != nil {
		return nil, err
	}
	return tdb, nil
}

// initializePublication sets up and creates the publication.
//
// Replica identity is applied or checked depending on CreateIfNotExists:
// - When CreateIfNotExists is true: ApplyReplicaIdentities may ALTER TABLE.
// - When CreateIfNotExists is false: CheckReplicaIdentities is read-only; any
//   failure is logged as Error with a manual-action hint and does not stop startup.
//   See #158 for rationale.
func initializePublication(ctx context.Context, cfg config.Config, conn pq.Connection) (*publication.Config, error) {
	pub := publication.New(cfg.Publication, conn)
	if cfg.Publication.CreateIfNotExists {
		if err := pub.ApplyReplicaIdentities(ctx); err != nil {
			return nil, err
		}
	} else if err := pub.CheckReplicaIdentities(ctx); err != nil {
		logger.Error("replica identity check failed; take manual action to ALTER TABLE ... REPLICA IDENTITY to match the config", "error", err)
	}
	return pub.Create(ctx)
}

// initializeSnapshot creates snapshot if enabled
// tables parameter should come from publicationInfo (not from config) to support both scenarios:
// 1. When user provides tables in config (createIfNotExists: true)
// 2. When user uses existing publication without specifying tables (createIfNotExists: false)
func initializeSnapshot(ctx context.Context, cfg config.Config, tables publication.Tables, m metric.Metric) (*snapshot.Snapshotter, error) {
	if !cfg.Snapshot.Enabled {
		return nil, nil
	}
	return snapshot.New(ctx, cfg.Snapshot, tables, cfg.DSN(), m)
}

// ErrConnectorConsumed is returned by Run/Start when called on a connector
// that has already run once (Run/Start already returned, for any reason).
// A connector is one-shot: internal latches (closeOnce, the stream's
// messageCH, OS signal registration) cannot be safely reused. Build a new
// connector via NewConnector to retry.
//
// This is a plain stdlib error (not go-playground/errors.Chain) deliberately
// -- Chain is a slice type, which stdlib errors.Is/As treat as non-comparable
// and so can never match, even against the exact same value. Callers are
// meant to check this with errors.Is, so it must stay a stdlib error.
var ErrConnectorConsumed = goerrors.New("connector already consumed by a previous Run/Start call; build a new one")

// Start is a thin wrapper around Run that discards the error, kept for
// backward compatibility with existing callers.
func (c *connector) Start(ctx context.Context) {
	_ = c.Run(ctx)
}

func (c *connector) Run(ctx context.Context) error {
	if !c.consumed.CompareAndSwap(false, true) {
		return ErrConnectorConsumed
	}
	return c.run(ctx)
}

// runGuarded runs fn in its own goroutine, recovering any panic and
// funnelling it into fatalCh instead of letting it crash the whole process
// invisibly to Run's caller.
func (c *connector) runGuarded(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("%s panicked: %v", name, r)
				logger.Error("recovered panic in background goroutine", "goroutine", name, "error", err)
				select {
				case c.fatalCh <- err:
				default:
				}
			}
		}()
		fn()
	}()
}

func (c *connector) run(ctx context.Context) error {
	c.once.Do(func() {
		c.runGuarded("http server", c.server.Listen)
	})

	// Snapshot-only mode: execute snapshot and exit
	if c.cfg.IsSnapshotOnlyMode() {
		// Check if snapshot already completed (resume capability)
		if !c.shouldTakeSnapshotOnly(ctx) {
			logger.Info("snapshot-only already completed, exiting")
			return nil
		}

		if err := c.executeSnapshotOnly(ctx); err != nil {
			logger.Error("snapshot-only execution failed", "error", err)
			return errors.Wrap(err, "snapshot-only execution")
		}
		logger.Info("snapshot-only completed successfully, exiting")
		return nil
	}

	// Snapshot Pre-phase (optional): Prepare → CreateSlot → Execute
	// This happens BEFORE the normal CDC flow to avoid data loss
	if c.cfg.Snapshot.Enabled && c.shouldTakeSnapshot(ctx) {
		if err := c.prepareSnapshotAndSlot(ctx); err != nil {
			logger.Error("snapshot preparation failed", "error", err)
			return errors.Wrap(err, "snapshot preparation")
		}
	} else {
		// No snapshot: Create slot normally before starting CDC
		logger.Info("creating replication slot for CDC")
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			logger.Error("slot creation failed", "error", err)
			return errors.Wrap(err, "slot creation")
		}
		logger.Info("slot info", "info", slotInfo)
	}

	if err := c.slot.Connect(ctx); err != nil {
		logger.Error("slot connection failed", "error", err)
		return errors.Wrap(err, "slot connection")
	}

	if err := c.captureAndOpenStream(ctx); err != nil {
		if ctx.Err() != nil {
			logger.Info("slot capture cancelled")
			return ctx.Err()
		}
		return err
	}

	logger.Info("slot captured")
	c.runGuarded("slot metrics", func() { c.slot.Metrics(ctx) })

	// Start heartbeat loop only for CDC mode when enabled
	if c.heartbeat != nil {
		c.runGuarded("heartbeat", func() { c.heartbeat.Run(ctx) })
	}

	if c.timescaleDB != nil {
		c.runGuarded("timescaledb sync", func() { c.timescaleDB.SyncHyperTables(ctx) })
	}

	signal.Notify(c.cancelCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGABRT, syscall.SIGQUIT)

	c.readyCh <- struct{}{}

	select {
	case <-c.cancelCh:
		logger.Debug("cancel channel triggered")
		return nil
	case err := <-c.fatalCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// captureAndOpenStream waits for the replication slot to become available
// and opens the replication stream. If the slot is actively held by another
// process (ErrorSlotInUse), it retries CaptureSlot+Connect+Open with backoff
// bounded only by ctx, rather than unboundedly recursing back into the top
// of Start/run: that would also redo slot creation and snapshot prep on
// every retry, which is wasteful and, for snapshot mode, wrong.
func (c *connector) captureAndOpenStream(ctx context.Context) error {
	return retry.Do(func() error {
		c.CaptureSlot(ctx)
		if ctx.Err() != nil {
			return retry.Unrecoverable(ctx.Err())
		}

		if err := c.stream.Connect(ctx); err != nil {
			logger.Error("stream connection failed", "error", err)
			return retry.Unrecoverable(errors.Wrap(err, "stream connection"))
		}

		if err := c.stream.Open(ctx); err != nil {
			if goerrors.Is(err, replication.ErrorSlotInUse) {
				logger.Info("capture failed, slot in use, retrying")
				return err
			}
			logger.Error("postgres stream open", "error", err)
			return retry.Unrecoverable(errors.Wrap(err, "postgres stream open"))
		}
		return nil
	},
		retry.Context(ctx),
		retry.Attempts(0), // unbounded by count; ctx is the only ceiling
		retry.DelayType(retry.BackOffDelay),
		retry.Delay(time.Second),
		retry.MaxDelay(30*time.Second),
		retry.RetryIf(func(err error) bool { return goerrors.Is(err, replication.ErrorSlotInUse) }),
		retry.LastErrorOnly(true),
	)
}

func (c *connector) shouldTakeSnapshot(ctx context.Context) bool {
	if !c.cfg.Snapshot.Enabled {
		return false
	}

	switch c.cfg.Snapshot.Mode {
	case config.SnapshotModeNever:
		return false
	case config.SnapshotModeInitial:
		// If resnapshot is enabled, clean metadata for THIS slot only and return true
		if c.cfg.Snapshot.Resnapshot {
			logger.Info("resnapshot enabled, cleaning metadata for slot", "slotName", c.cfg.Slot.Name)
			if err := c.snapshotter.CleanupJobForSlot(ctx, c.cfg.Slot.Name); err != nil {
				logger.Warn("failed to cleanup job for resnapshot", "error", err)
			}
			return true
		}

		job, err := c.snapshotter.LoadJob(ctx, c.cfg.Slot.Name)
		if err != nil {
			logger.Debug("failed to load snapshot job state, will take snapshot", "error", err)
			return true
		}
		return job == nil || !job.Completed
	default:
		logger.Warn("invalid snapshot mode, skipping snapshot", "mode", c.cfg.Snapshot.Mode)
		return false
	}
}

// prepareSnapshotAndSlot handles the snapshot preparation with two-phase approach:
// 1. Prepare: Capture LSN and create metadata
// 2. Create replication slot immediately (to preserve WAL)
// 3. Execute: Collect snapshot data
// This ensures no WAL changes are lost during snapshot execution
func (c *connector) prepareSnapshotAndSlot(ctx context.Context) error {
	return c.retryOperation("snapshot", 3, func(_ int) error {
		// Phase 1: Create replication slot immediately (CRITICAL - preserves WAL)
		slotInfo, err := c.slot.Create(ctx)
		if err != nil {
			return errors.Wrap(err, "create slot")
		}
		logger.Debug("replication slot created, WAL preserved", "slotName", slotInfo.Name, "restartLSN", slotInfo.RestartLSN.String())

		// Phase 2: Prepare snapshot (capture LSN, create metadata, export snapshot)
		err = c.snapshotter.Prepare(ctx, c.cfg.Slot.Name)
		if err != nil {
			return errors.Wrap(err, "prepare snapshot")
		}
		c.stream.OpenFromSnapshotLSN()

		// Phase 3: Execute snapshot (collect data from all chunks)
		// This may fail if coordinator restarts during execution - retry with backoff
		if err := c.executeSnapshotWithRetry(ctx); err != nil {
			if c.isSnapshotInvalidationError(err) {
				logger.Error("snapshot invalidated and retries exhausted; giving up on this attempt", "error", err)
			}
			return errors.Wrap(err, "execute snapshot")
		}

		logger.Info("snapshot completed successfully")
		return nil
	})
}

// executeSnapshotOnly executes snapshot without creating a replication slot
// Used for snapshot_only mode (finite data export without CDC)
// Multi-pod safe: uses consistent slot name for coordinator election
func (c *connector) executeSnapshotOnly(ctx context.Context) error {
	slotName := c.getSnapshotOnlySlotName()

	logger.Info("starting snapshot-only execution", "slotName", slotName)

	// Prepare snapshot (capture LSN, create metadata, export snapshot)
	err := c.snapshotter.Prepare(ctx, slotName)
	if err != nil {
		return errors.Wrap(err, "prepare snapshot")
	}

	// Execute snapshot (collect data from all chunks)
	// Note: We call Execute directly with the slotName we prepared with,
	// not through executeSnapshotWithRetry which uses c.cfg.Slot.Name
	if err := c.snapshotter.Execute(ctx, c.snapshotHandler, slotName); err != nil {
		return errors.Wrap(err, "execute snapshot")
	}

	logger.Info("snapshot data collection completed")
	return nil
}

// getSnapshotOnlySlotName returns a consistent slot name for snapshot_only mode
// This ensures multi-pod deployments work together instead of duplicating work
// If user defines a custom snapshot ID, use it; otherwise generate one based on database name
func (c *connector) getSnapshotOnlySlotName() string {
	if c.cfg.Snapshot.ID != "" {
		return c.cfg.Snapshot.ID
	}
	return fmt.Sprintf("snapshot_only_%s", c.cfg.Database)
}

// shouldTakeSnapshotOnly checks if snapshot_only should run
// Returns false if snapshot already completed (resume capability)
func (c *connector) shouldTakeSnapshotOnly(ctx context.Context) bool {
	slotName := c.getSnapshotOnlySlotName()

	// If resnapshot is enabled, clean metadata for THIS slot only
	if c.cfg.Snapshot.Resnapshot {
		logger.Info("resnapshot enabled, cleaning metadata for slot", "slotName", slotName)
		if err := c.snapshotter.CleanupJobForSlot(ctx, slotName); err != nil {
			logger.Warn("failed to cleanup job for resnapshot", "error", err)
		}
		return true
	}

	job, err := c.snapshotter.LoadJob(ctx, slotName)
	if err != nil {
		logger.Debug("failed to load snapshot job, will take snapshot", "error", err)
		return true
	}

	// If job doesn't exist or not completed, take snapshot
	if job == nil || !job.Completed {
		return true
	}

	// Job exists and completed
	logger.Info("snapshot-only already completed, skipping", "slotName", slotName)
	return false
}

// executeSnapshotWithRetry executes snapshot with retry on coordinator failures
// When coordinator restarts, it creates a new snapshot - workers should retry to join the new snapshot
func (c *connector) executeSnapshotWithRetry(ctx context.Context) error {
	const (
		maxRetries        = 5
		initialDelay      = 10 * time.Second
		maxDelay          = 60 * time.Second
		backoffMultiplier = 2
	)

	retryDelay := initialDelay

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err := c.snapshotter.Execute(ctx, c.snapshotHandler, c.cfg.Slot.Name)
		if err == nil {
			return nil // Success
		}

		// Check if this is a recoverable error (snapshot invalidation)
		if !c.isSnapshotInvalidationError(err) {
			// Non-recoverable error
			return err
		}

		// Last attempt exhausted
		if attempt >= maxRetries {
			return errors.Wrap(err, "snapshot execution failed after maximum retries")
		}

		// Log and wait before retry
		c.logRetryAttempt(attempt, maxRetries, retryDelay)

		if waitErr := c.waitWithContext(ctx, retryDelay); waitErr != nil {
			return waitErr
		}

		// Exponential backoff
		retryDelay = c.calculateNextDelay(retryDelay, maxDelay, backoffMultiplier)
	}

	return errors.New("snapshot execution failed: unexpected exit from retry loop")
}

// isSnapshotInvalidationError checks if error is due to snapshot invalidation
func (c *connector) isSnapshotInvalidationError(err error) bool {
	if err == nil {
		return false
	}

	// Check for typed error first (preferred)
	if goerrors.Is(err, snapshot.ErrSnapshotInvalidated) {
		return true
	}

	// Fallback to string matching for wrapped errors
	return strings.Contains(err.Error(), "snapshot invalidated")
}

// logRetryAttempt logs snapshot retry attempt with context
func (c *connector) logRetryAttempt(attempt, maxRetries int, delay time.Duration) {
	logger.Warn("[snapshot] snapshot invalidated, coordinator likely restarted",
		"attempt", attempt,
		"maxRetries", maxRetries,
		"waitTime", delay)

	logger.Info("[snapshot] waiting for coordinator to create new snapshot",
		"retryIn", delay)
}

// waitWithContext waits for duration or context cancellation
func (c *connector) waitWithContext(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(duration):
		return nil
	}
}

// calculateNextDelay calculates next retry delay with exponential backoff
func (c *connector) calculateNextDelay(currentDelay, maxDelay time.Duration, multiplier int) time.Duration {
	nextDelay := currentDelay * time.Duration(multiplier)
	if nextDelay > maxDelay {
		return maxDelay
	}
	return nextDelay
}

// retryOperation executes an operation with retry logic
func (c *connector) retryOperation(operationName string, maxRetries int, operation func(attempt int) error) error {
	var lastErr error
	retryDelay := 5 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		if attempt > 1 {
			logger.Info("retrying operation", "operation", operationName, "attempt", attempt, "maxRetries", maxRetries)
		}

		if err := operation(attempt); err != nil {
			lastErr = err
			logger.Warn("operation failed", "operation", operationName, "attempt", attempt, "error", err)

			if attempt < maxRetries {
				logger.Info("waiting before retry", "retryDelay", retryDelay.String())
				time.Sleep(retryDelay)
			}
			continue
		}

		return nil
	}

	return errors.Wrapf(lastErr, "%s failed after %d retries", operationName, maxRetries)
}

func (c *connector) snapshotHandler(event *format.Snapshot) error {
	c.listenerFunc(&replication.ListenerContext{
		Message: event,
		Ack: func() error {
			return nil // ACK isn't required for snapshot
		},
	})
	return nil
}

func (c *connector) WaitUntilReady(ctx context.Context) error {
	select {
	case <-c.readyCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *connector) Close() {
	// Make close idempotent
	c.closeOnce.Do(func() {
		// Mark the connector consumed so a Run/Start call made after Close
		// (whether Run already returned or never started) fails fast with
		// ErrConnectorConsumed instead of running against closed resources.
		c.consumed.Store(true)

		// Create a context with timeout for graceful cleanup
		// 30 seconds should be sufficient for closing connections and cleanup operations
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		logger.Debug("[connector] closing connector")

		// Close signal channels
		signal.Stop(c.cancelCh)
		if !isClosed(c.cancelCh) {
			close(c.cancelCh)
		}
		if !isClosed(c.readyCh) {
			close(c.readyCh)
		}

		// Close snapshotter connections if still open (fallback for crash/error scenarios)
		// Normal flow: connections are already closed in finalizeSnapshot() when snapshot completes
		if c.snapshotter != nil {
			c.snapshotter.Close(ctx)
		}

		// Close replication slot and stream (nil in snapshot_only mode)
		if c.slot != nil {
			c.slot.Close(ctx)
		}
		if c.heartbeat != nil {
			c.heartbeat.Close(ctx)
		}
		if c.stream != nil {
			if err := c.stream.Close(ctx); err != nil {
				logger.Error("replication stream shutdown failed", "error", err)
			}
		}

		// Shutdown HTTP server
		c.server.Shutdown()

		logger.Info("[connector] connector closed successfully")
	})
}

func (c *connector) GetConfig() *config.Config {
	return c.cfg
}

func (c *connector) SetMetricCollectors(metricCollectors ...prometheus.Collector) {
	c.prometheusRegistry.AddMetricCollectors(metricCollectors...)
}

func (c *connector) CaptureSlot(ctx context.Context) {
	logger.Info("slot capturing...")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		info, err := c.slot.Info(ctx)
		if err != nil {
			if goerrors.Is(err, slot.ErrorSlotClosed) {
				return
			}
			logger.Warn("slot info failed on capture slot", "error", err)
			continue
		}

		if info.Active {
			continue
		}

		logger.Debug("capture slot", "slotInfo", info)
		break
	}
}

func isClosed[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}
