package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-pq-cdc/logger"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/go-playground/errors"
)

var coordinatorWaitInterval = time.Second

// waitForCoordinator waits for published metadata and chunks while the
// coordinator holds its lock. Slot creation and discovery have no fixed deadline;
// caller cancellation or loss of the coordinator ends the wait.
func (s *Snapshotter) waitForCoordinator(ctx context.Context, slotName string) error {
	for {
		// Check if both job and chunks are ready
		yes, status, err := s.isCoordinatorDidItsJob(ctx, slotName)
		switch {
		case err != nil:
			logger.Debug("[worker] waiting for coordinator", "status", status, "error", err)
		case yes:
			logger.Debug("[worker] coordinator ready, starting work", "status", status)
			return nil
		default:
			logger.Debug("[worker] waiting for coordinator", "status", status)
		}

		// A vanished coordinator cannot publish this attempt. Release any
		// acquired lock through prepare cleanup and let Supervise re-elect.
		acquired, lockErr := s.tryAcquireCoordinatorLock(ctx, slotName)
		if lockErr != nil {
			return lockErr
		}
		if acquired {
			return ErrSnapshotInvalidated
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(coordinatorWaitInterval):
			// Continue waiting
		}
	}
}

// isCoordinatorDidItsJob checks if both job and chunks are ready for processing
// Returns (ready, status_message, error)
func (s *Snapshotter) isCoordinatorDidItsJob(ctx context.Context, slotName string) (bool, string, error) {
	// Step 1: Check if job exists
	job, err := s.loadJob(ctx, slotName)
	if err != nil {
		return false, "job not found", err
	}
	if job == nil {
		return false, "job not created yet", nil
	}
	if job.Completed {
		return false, "previous snapshot is complete", nil
	}

	// Step 2: Check if snapshot ID is set (coordinator has exported snapshot)
	if job.SnapshotID == "" || job.SnapshotID == "PENDING" {
		return false, "snapshot not exported yet", nil
	}

	// Step 3: Check if chunks are available
	hasChunks, err := s.hasChunksReady(ctx, slotName)
	if err != nil {
		return false, "error checking chunks", err
	}
	if !hasChunks {
		return false, "chunks not created yet", nil
	}

	// Pin this view so Execute cannot silently join a replacement generation.
	s.cachedSnapshotID = job.SnapshotID
	return true, fmt.Sprintf("ready (job=%s, chunks=available)", job.SnapshotID), nil
}

// hasChunksReady checks if there are chunks available for processing
func (s *Snapshotter) hasChunksReady(ctx context.Context, slotName string) (bool, error) {
	query := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM %s
			WHERE slot_name = '%s'
		)
	`, chunksTableName, slotName)

	results, err := s.execQuery(ctx, s.metadataConn, query)
	if err != nil {
		return false, errors.Wrap(err, "check chunks ready")
	}

	if len(results) == 0 || len(results[0].Rows) == 0 || len(results[0].Rows[0]) == 0 {
		return false, nil
	}

	// Parse boolean result
	hasChunks := string(results[0].Rows[0][0]) == "t"
	return hasChunks, nil
}

// executeWorker sets up metrics, transactions, and processes chunks
func (s *Snapshotter) executeWorker(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, startTime time.Time) error {
	// Set metrics
	s.metric.SetSnapshotInProgress(true)
	s.metric.SetSnapshotTotalTables(len(s.tables))
	s.metric.SetSnapshotTotalChunks(job.TotalChunks)
	defer func() {
		s.metric.SetSnapshotInProgress(false)
		s.metric.SetSnapshotDurationSeconds(time.Since(startTime).Seconds())
	}()

	// Send BEGIN marker
	_ = handler(&format.Snapshot{
		EventType:  format.SnapshotEventTypeBegin,
		ServerTime: time.Now().UTC(),
		LSN:        job.SnapshotLSN,
	})

	// Process chunks (each chunk will have its own transaction)
	if err := s.workerProcess(ctx, slotName, instanceID, job, handler); err != nil {
		return fmt.Errorf("worker process: %w", err)
	}

	return nil
}

// workerProcess processes chunks as a worker
func (s *Snapshotter) workerProcess(ctx context.Context, slotName, instanceID string, job *Job, handler Handler) error {
	heartbeatCtx, currentChunk := s.startHeartbeat(ctx)
	defer heartbeatCtx()

	for {
		hasMore, err := s.processNextChunk(ctx, slotName, instanceID, job, handler, currentChunk)
		if err != nil {
			return err
		}
		if !hasMore {
			completed, err := s.checkJobCompleted(ctx, slotName)
			if err != nil {
				return err
			}
			if completed {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

// startHeartbeat initializes and starts the heartbeat goroutine
func (s *Snapshotter) startHeartbeat(ctx context.Context) (cancel context.CancelFunc, chunkChan chan<- int64) {
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	currentChunk := make(chan int64, 1)
	done := make(chan struct{})
	go func() { defer close(done); s.heartbeatWorker(heartbeatCtx, currentChunk, s.config.HeartbeatInterval) }()
	return func() { cancelHeartbeat(); <-done }, currentChunk
}

// processNextChunk claims and processes a single chunk
// Returns (hasMore, error) where hasMore indicates if there are more chunks to process
func (s *Snapshotter) processNextChunk(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, chunkChan chan<- int64) (bool, error) {
	// Check context cancellation
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// Refuse to mix chunks from a replacement snapshot generation.
	current, err := s.loadJob(ctx, slotName)
	if err != nil {
		return false, err
	}
	if current == nil || current.SnapshotID != job.SnapshotID {
		return false, ErrSnapshotInvalidated
	}

	// Claim next chunk
	chunk, err := s.claimNextChunk(ctx, slotName, instanceID, job.SnapshotID, s.config.ClaimTimeout)
	if err != nil {
		return false, errors.Wrap(err, "claim next chunk")
	}
	if chunk == nil {
		return false, nil // No more chunks available
	}

	// Setup chunk processing
	s.prepareChunkProcessing(instanceID, chunk, chunkChan)

	// Process chunk and handle errors
	return s.executeChunkProcessing(ctx, slotName, instanceID, job, handler, chunk)
}

// prepareChunkProcessing logs and notifies heartbeat for chunk
func (s *Snapshotter) prepareChunkProcessing(instanceID string, chunk *Chunk, chunkChan chan<- int64) {
	s.logChunkStart(instanceID, chunk)
	s.notifyHeartbeat(chunkChan, chunk.ID)
}

// executeChunkProcessing processes a chunk and handles errors appropriately
func (s *Snapshotter) executeChunkProcessing(ctx context.Context, slotName, instanceID string, job *Job, handler Handler, chunk *Chunk) (bool, error) {
	rowsProcessed, err := s.processChunkWithTransaction(ctx, chunk, job.SnapshotID, job.SnapshotLSN, handler)
	if err != nil {
		return s.handleChunkProcessingError(ctx, instanceID, chunk, job.SnapshotID, err)
	}

	current, loadErr := s.loadJob(ctx, slotName)
	if loadErr != nil {
		return false, loadErr
	}
	if current == nil || current.SnapshotID != job.SnapshotID {
		return false, ErrSnapshotInvalidated
	}

	// Success: mark chunk as completed
	if err := s.completeChunk(ctx, slotName, instanceID, chunk, rowsProcessed); err != nil {
		return false, err
	}
	return true, nil // More chunks may be available
}

// handleChunkProcessingError handles different types of chunk processing errors
func (s *Snapshotter) handleChunkProcessingError(ctx context.Context, instanceID string, chunk *Chunk, snapshotID string, err error) (bool, error) {
	// Invalid snapshot error: coordinator restarted
	if IsInvalidSnapshotError(err) {
		return s.handleInvalidSnapshot(ctx, instanceID, chunk, snapshotID)
	}

	// A failed chunk cannot be skipped or replaced with a different snapshot.
	_ = s.releaseChunk(ctx, chunk.ID)
	return false, err
}

// handleInvalidSnapshot handles the case when snapshot becomes invalid
// Returns (false, error) to stop worker loop and trigger retry
func (s *Snapshotter) handleInvalidSnapshot(ctx context.Context, instanceID string, chunk *Chunk, snapshotID string) (bool, error) {
	logger.Warn("[worker] invalid snapshot detected, coordinator likely restarted",
		"chunkID", chunk.ID,
		"snapshotID", snapshotID,
		"instanceID", instanceID)

	// Release chunk back to pending so it can be reprocessed
	if err := s.releaseChunk(ctx, chunk.ID); err != nil {
		logger.Error("[worker] failed to release chunk after invalid snapshot",
			"chunkID", chunk.ID,
			"error", err)
	} else {
		logger.Info("[worker] chunk released back to pending", "chunkID", chunk.ID)
	}

	// Signal worker to stop and trigger retry at connector level
	logger.Info("[worker] stopping worker due to invalid snapshot, will restart", "instanceID", instanceID)
	return false, ErrSnapshotInvalidated
}

// logChunkStart logs the start of chunk processing
func (s *Snapshotter) logChunkStart(instanceID string, chunk *Chunk) {
	args := []any{
		"instanceID", instanceID,
		"table", fmt.Sprintf("%s.%s", chunk.TableSchema, chunk.TableName),
		"chunkIndex", chunk.ChunkIndex,
		"chunkStart", chunk.ChunkStart,
		"chunkSize", chunk.ChunkSize,
	}

	if hasRange := chunk.hasRangeBounds(); hasRange {
		args = append(args,
			"rangeStart", *chunk.RangeStart,
			"rangeEnd", *chunk.RangeEnd,
		)
	}

	logger.Debug("[worker] processing chunk", args...)
}

// notifyHeartbeat sends chunk ID to heartbeat worker
func (s *Snapshotter) notifyHeartbeat(chunkChan chan<- int64, chunkID int64) {
	select {
	case chunkChan <- chunkID:
	default:
	}
}

// completeChunk marks chunk as completed and updates metrics
func (s *Snapshotter) completeChunk(ctx context.Context, slotName, instanceID string, chunk *Chunk, rowsProcessed int64) error {
	// Mark chunk as completed
	if err := s.markChunkCompleted(ctx, slotName, chunk.ID, rowsProcessed); err != nil {
		return errors.Wrap(err, "mark chunk completed")
	}

	// Update metrics
	s.metric.SnapshotRowsIncrement(rowsProcessed)
	s.updateCompletedChunksMetric(ctx, slotName)

	// Log completion
	logger.Debug("[worker] chunk completed",
		"instanceID", instanceID,
		"chunkID", chunk.ID,
		"rowsProcessed", rowsProcessed)
	return nil
}

// updateCompletedChunksMetric updates the completed chunks metric
func (s *Snapshotter) updateCompletedChunksMetric(ctx context.Context, slotName string) {
	if job, _ := s.loadJob(ctx, slotName); job != nil {
		s.metric.SetSnapshotCompletedChunks(job.CompletedChunks)
	}
}

// processChunkWithTransaction processes a single chunk within its own transaction
// This allows each chunk to have an independent transaction lifecycle with retry support
func (s *Snapshotter) processChunkWithTransaction(ctx context.Context, chunk *Chunk, snapshotID string, lsn pq.LSN, handler Handler) (int64, error) {
	var rowsProcessed int64

	err := s.retryDBOperation(ctx, func() error {
		rows, err := s.executeInTransaction(ctx, snapshotID, func(conn pq.Connection) (int64, error) {
			return s.processChunk(ctx, conn, chunk, lsn, handler)
		})
		if err != nil {
			return err
		}
		rowsProcessed = rows
		return nil
	})

	if err != nil {
		return 0, errors.Wrap(err, "process chunk with transaction")
	}

	return rowsProcessed, nil
}

// executeInTransaction executes a function within a snapshot transaction
// Uses a connection from the pool for efficient reuse
func (s *Snapshotter) executeInTransaction(ctx context.Context, snapshotID string, fn func(pq.Connection) (int64, error)) (int64, error) {
	// Get connection from pool (optimization: avoid connection create/destroy overhead)
	chunkConn, err := s.connectionPool.Get(ctx)
	if err != nil {
		return 0, errors.Wrap(err, "get connection from pool")
	}
	defer s.connectionPool.Put(chunkConn)

	tx := &snapshotTransaction{
		snapshotter: s,
		ctx:         ctx,
		snapshotID:  snapshotID,
		conn:        chunkConn, // Use dedicated connection for this transaction
	}

	if err := tx.begin(); err != nil {
		return 0, err
	}
	defer tx.rollbackIfNeeded()

	rows, err := fn(chunkConn)
	if err != nil {
		return 0, errors.Wrap(err, "execute function")
	}

	if err := tx.commit(); err != nil {
		return 0, err
	}

	return rows, nil
}

// snapshotTransaction manages a single snapshot transaction lifecycle
type snapshotTransaction struct {
	ctx         context.Context
	conn        pq.Connection
	snapshotter *Snapshotter
	snapshotID  string
	committed   bool
}

// begin starts the transaction and sets the snapshot
func (tx *snapshotTransaction) begin() error {
	if err := tx.snapshotter.execSQL(tx.ctx, tx.conn, "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return errors.Wrap(err, "begin transaction")
	}

	if err := tx.snapshotter.setTransactionSnapshot(tx.ctx, tx.conn, tx.snapshotID); err != nil {
		// BEGIN succeeded but snapshot failed - must rollback
		_ = tx.snapshotter.execSQL(tx.ctx, tx.conn, "ROLLBACK")
		return errors.Wrap(err, "set transaction snapshot")
	}

	return nil
}

// commit commits the transaction
func (tx *snapshotTransaction) commit() error {
	if err := tx.snapshotter.execSQL(tx.ctx, tx.conn, "COMMIT"); err != nil {
		return errors.Wrap(err, "commit transaction")
	}
	tx.committed = true
	return nil
}

// rollbackIfNeeded rolls back the transaction if not committed
func (tx *snapshotTransaction) rollbackIfNeeded() {
	if !tx.committed {
		_ = tx.snapshotter.execSQL(tx.ctx, tx.conn, "ROLLBACK")
	}
}

// heartbeatWorker periodically updates the heartbeat for the current chunk
func (s *Snapshotter) heartbeatWorker(ctx context.Context, currentChunk <-chan int64, interval time.Duration) {
	var activeChunkID int64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case chunkID := <-currentChunk:
			activeChunkID = chunkID
		case <-ticker.C:
			if activeChunkID > 0 {
				if err := s.updateChunkHeartbeat(ctx, activeChunkID); err != nil {
					logger.Warn("[heartbeat] failed to update", "chunkID", activeChunkID, "error", err)
				} else {
					logger.Debug("[heartbeat] updated", "chunkID", activeChunkID)
				}
			}
		}
	}
}

// markJobAsCompleted marks the job as completed (safe to call multiple times)
func (s *Snapshotter) markJobAsCompleted(ctx context.Context, slotName, snapshotID string) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET completed = true
			WHERE slot_name = '%s' AND snapshot_id = '%s'
		`, jobTableName, slotName, snapshotID)

		results, err := s.execQuery(ctx, s.metadataConn, query)
		if err != nil {
			return errors.Wrap(err, "mark job as completed")
		}
		if len(results) != 1 || results[0].CommandTag.RowsAffected() != 1 {
			return ErrSnapshotInvalidated
		}

		logger.Info("[metadata] job marked as completed", "slotName", slotName)
		return nil
	})
}

// claimNextChunk attempts to claim a pending chunk using SELECT FOR UPDATE SKIP LOCKED
func (s *Snapshotter) claimNextChunk(ctx context.Context, slotName, instanceID, snapshotID string, claimTimeout time.Duration) (*Chunk, error) {
	var chunk *Chunk

	err := s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()
		query := s.buildClaimChunkQuery(slotName, instanceID, snapshotID, now, claimTimeout)

		results, err := s.execQuery(ctx, s.metadataConn, query)
		if err != nil {
			return errors.Wrap(err, "claim chunk")
		}

		if len(results) == 0 || len(results[0].Rows) == 0 {
			chunk = nil
			return nil // No chunks available (not an error)
		}

		row := results[0].Rows[0]
		if len(row) < 13 {
			return errors.New("invalid chunk row: expected 13 columns")
		}

		chunk, err = s.parseClaimedChunk(row, slotName, instanceID, now)
		return err
	})

	return chunk, err
}

// buildClaimChunkQuery builds the SQL query for claiming a chunk
func (s *Snapshotter) buildClaimChunkQuery(slotName, instanceID, snapshotID string, now time.Time, claimTimeout time.Duration) string {
	timeoutThreshold := now.Add(-claimTimeout)
	return fmt.Sprintf(`
		WITH available_chunk AS (
			SELECT id FROM %s
			WHERE slot_name = '%s'
			  AND EXISTS (SELECT 1 FROM cdc_snapshot_job j WHERE j.slot_name = '%s' AND j.snapshot_id = '%s')
			  AND (
				  status = 'pending'
				  OR (status = 'in_progress' AND heartbeat_at < '%s')
			  )
			ORDER BY chunk_index
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s c
		SET status = 'in_progress',
		    claimed_by = '%s',
		    claimed_at = '%s',
		    heartbeat_at = '%s'
		FROM available_chunk
		WHERE c.id = available_chunk.id
		RETURNING c.id, c.table_schema, c.table_name, 
		          c.chunk_index, c.chunk_start, c.chunk_size, 
		          c.range_start, c.range_end, c.block_start, c.block_end,
		          c.is_last_chunk, c.partition_strategy, c.rows_processed
	`, chunksTableName,
		slotName, slotName, snapshotID,
		timeoutThreshold.Format(postgresTimestampFormat),
		chunksTableName,
		instanceID,
		now.Format(postgresTimestampFormat),
		now.Format(postgresTimestampFormat),
	)
}

// parseClaimedChunk parses the chunk row data
func (s *Snapshotter) parseClaimedChunk(row [][]byte, slotName, instanceID string, now time.Time) (*Chunk, error) {
	if len(row) < 13 {
		return nil, errors.New("invalid chunk row: expected 13 columns")
	}

	chunk := &Chunk{
		SlotName:    slotName,
		Status:      ChunkStatusInProgress,
		ClaimedBy:   instanceID,
		ClaimedAt:   &now,
		HeartbeatAt: &now,
	}

	// Parse each field with validation
	if _, err := fmt.Sscanf(string(row[0]), "%d", &chunk.ID); err != nil {
		return nil, errors.Wrap(err, "parse chunk ID")
	}
	chunk.TableSchema = string(row[1])
	chunk.TableName = string(row[2])
	if _, err := fmt.Sscanf(string(row[3]), "%d", &chunk.ChunkIndex); err != nil {
		return nil, errors.Wrap(err, "parse chunk index")
	}
	if _, err := fmt.Sscanf(string(row[4]), "%d", &chunk.ChunkStart); err != nil {
		return nil, errors.Wrap(err, "parse chunk start")
	}
	if _, err := fmt.Sscanf(string(row[5]), "%d", &chunk.ChunkSize); err != nil {
		return nil, errors.Wrap(err, "parse chunk size")
	}

	// Parse nullable int64 fields for range
	rangeStart, err := parseNullableInt64(row[6])
	if err != nil {
		return nil, errors.Wrap(err, "parse range start")
	}
	rangeEnd, err := parseNullableInt64(row[7])
	if err != nil {
		return nil, errors.Wrap(err, "parse range end")
	}
	chunk.RangeStart = rangeStart
	chunk.RangeEnd = rangeEnd

	// Parse CTID block fields
	blockStart, err := parseNullableInt64(row[8])
	if err != nil {
		return nil, errors.Wrap(err, "parse block start")
	}
	blockEnd, err := parseNullableInt64(row[9])
	if err != nil {
		return nil, errors.Wrap(err, "parse block end")
	}
	chunk.BlockStart = blockStart
	chunk.BlockEnd = blockEnd

	// Parse is_last_chunk (boolean)
	if len(row[10]) > 0 {
		chunk.IsLastChunk = string(row[10]) == "t" || string(row[10]) == "true"
	}

	// Parse partition strategy
	if len(row[11]) > 0 {
		chunk.PartitionStrategy = PartitionStrategy(string(row[11]))
	} else {
		chunk.PartitionStrategy = PartitionStrategyOffset
	}

	return chunk, nil
}

// updateChunkHeartbeat updates the heartbeat timestamp for a chunk with retry
func (s *Snapshotter) updateChunkHeartbeat(ctx context.Context, chunkID int64) error {
	return s.retryDBOperation(ctx, func() error {
		now := time.Now().UTC()
		query := fmt.Sprintf(`
			UPDATE %s SET heartbeat_at = '%s' WHERE id = %d
		`, chunksTableName, now.Format(postgresTimestampFormat), chunkID)

		_, err := s.execQuery(ctx, s.healthcheckConn, query)
		return err
	})
}

// markChunkCompleted records chunk completion and progress in one statement.
// A reclaimed or repeated completion must not increment the job counter twice.
func (s *Snapshotter) markChunkCompleted(ctx context.Context, slotName string, chunkID, rowsProcessed int64) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			WITH finished AS (
				UPDATE %s SET status = 'completed', completed_at = '%s', rows_processed = %d
				WHERE id = %d AND slot_name = '%s' AND status <> 'completed'
				RETURNING slot_name
			)
			UPDATE %s j SET completed_chunks = completed_chunks + 1
			FROM finished WHERE j.slot_name = finished.slot_name
		`, chunksTableName, time.Now().UTC().Format(postgresTimestampFormat), rowsProcessed, chunkID, slotName, jobTableName)
		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return errors.Wrap(err, "complete chunk and update progress")
		}
		return nil
	})
}

// releaseChunk releases a claimed chunk back to pending status
// This allows other workers to reclaim and process the chunk
func (s *Snapshotter) releaseChunk(ctx context.Context, chunkID int64) error {
	return s.retryDBOperation(ctx, func() error {
		query := fmt.Sprintf(`
			UPDATE %s
			SET status = 'pending',
			    claimed_by = NULL,
			    claimed_at = NULL,
			    heartbeat_at = NULL
			WHERE id = %d
		`, chunksTableName, chunkID)

		if _, err := s.execQuery(ctx, s.metadataConn, query); err != nil {
			return errors.Wrap(err, "release chunk")
		}

		return nil
	})
}
