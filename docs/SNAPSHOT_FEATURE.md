# Snapshot Feature Documentation

## Table of Contents

- [Overview](#overview)
- [Why Snapshot?](#why-snapshot)
- [Architecture](#architecture)
  - [High-Level Design](#high-level-design)
  - [Key Components](#key-components)
  - [Lifecycle Phases](#lifecycle-phases)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Basic Configuration](#basic-configuration)
  - [Simple Example](#simple-example)
- [Configuration Reference](#configuration-reference)
- [How It Works](#how-it-works)
  - [Phase 1: Coordinator Election](#phase-1-coordinator-election)
  - [Phase 2: Snapshot Export](#phase-2-snapshot-export)
  - [Phase 3: Metadata Creation](#phase-3-metadata-creation)
  - [Phase 4: Chunk Processing](#phase-4-chunk-processing)
  - [Phase 5: CDC Continuation](#phase-5-cdc-continuation)
  - [Partitioning Strategies](#partitioning-strategies)
- [Advanced Topics](#advanced-topics)
  - [Multi-Instance Deployment](#multi-instance-deployment)
  - [Crash Recovery](#crash-recovery)
  - [Performance Tuning](#performance-tuning)
- [Monitoring & Observability](#monitoring--observability)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)
- [FAQ](#faq)

---

## Overview

The **Snapshot Feature** enables **initial data capture** from PostgreSQL tables before starting Change Data Capture (CDC). This ensures that your downstream systems receive both:

1. **Existing data** (via snapshot)
2. **Real-time changes** (via CDC)

Without snapshot support, CDC only captures changes that occur *after* the replication slot is created, missing all pre-existing data.

### Key Features

✅ **Consistent Point-in-Time Snapshot**: Uses the snapshot and `consistent_point` returned together by logical slot creation
✅ **Chunk-Based Processing**: Splits large tables into manageable chunks for memory efficiency  
✅ **Smart Partitioning**: Auto-selects optimal strategy (Integer Range, CTID Block, or Offset) based on table structure  
✅ **Snapshot-Consistent Chunk Creation**: All size queries (`pg_relation_size`) run within snapshot transaction for consistency  
✅ **Multi-Instance Support**: Multiple instances can collaborate to process chunks in parallel  
✅ **Crash Recovery**: Reclaims failed worker chunks while the export is alive; restarts the full snapshot if the coordinator is lost
✅ **Zero Data Loss**: Ensures no data is missed between snapshot and CDC phases  
✅ **Observability**: Built-in metrics and progress tracking  

---

## Why Snapshot?

### Problem Statement

Consider this scenario:

```
Timeline:
  T0: Table "users" has 1M rows
  T1: You start go-pq-cdc (CDC only)
  T2: 10 new rows inserted
  
Result: You only get 10 rows, missing the initial 1M rows!
```

### Solution

With snapshot enabled:

```
Timeline:
  T0: Table "users" has 1M rows
  T1: You start go-pq-cdc with snapshot enabled
      - Snapshot captures 1M rows at LSN=X
      - Replication slot created at LSN=X
  T2: 10 new rows inserted (at LSN=Y, Y > X)
  T3: Snapshot completes, CDC starts from LSN=X
      - CDC captures the 10 new rows
  
Result: You get all 1M + 10 rows, with no duplicates!
```

### Use Cases

- **Initial Data Migration**: Migrate existing data to Elasticsearch, Kafka, or other systems
- **New Consumer Bootstrap**: Bootstrap new consumers with historical data
- **Disaster Recovery**: Rebuild downstream systems from scratch
- **Analytics**: Sync full dataset for analytics pipelines

---

## Architecture

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL Database                       │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐     │
│  │  Table 1     │    │  Table 2     │    │  Table N     │     │
│  │  (1M rows)   │    │  (500K rows) │    │  (2M rows)   │     │
│  └──────────────┘    └──────────────┘    └──────────────┘     │
│         │                    │                    │             │
│         └────────────────────┴────────────────────┘             │
│                              │                                  │
│                    ┌─────────▼─────────┐                       │
│                    │  Replication Slot │                       │
│                    │   (LSN: 0/12345)  │                       │
│                    └───────────────────┘                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ Logical Replication
                              │
        ┌─────────────────────┴─────────────────────┐
        │                                            │
┌───────▼────────┐                          ┌───────▼────────┐
│   Instance 1   │                          │   Instance 2   │
│  (Coordinator) │◄────Collaboration────────│    (Worker)    │
│                │                          │                │
│ ┌────────────┐ │                          │ ┌────────────┐ │
│ │ Snapshot   │ │                          │ │ Snapshot   │ │
│ │ Engine     │ │                          │ │ Engine     │ │
│ └────────────┘ │                          │ └────────────┘ │
│ ┌────────────┐ │                          │ ┌────────────┐ │
│ │ CDC Stream │ │                          │ │ CDC Stream │ │
│ └────────────┘ │                          │ └────────────┘ │
└────────┬───────┘                          └────────┬───────┘
         │                                           │
         │                                           │
         └─────────────┬───────────────┬────────────┘
                       │               │
                ┌──────▼─────┐  ┌──────▼─────┐
                │   Kafka    │  │ Elastic    │
                └────────────┘  └────────────┘
```

### Key Components

#### 1. **Snapshotter**
The main orchestrator that manages the snapshot lifecycle.

```go
type Snapshotter struct {
    metadataConn       pq.Connection  // Manages job/chunk metadata
    healthcheckConn    pq.Connection  // Heartbeat updates
    exportSnapshotConn pq.Connection  // Keeps snapshot transaction open for only coordinator
    
    dsn     string
    metric  metric.Metric
    typeMap *pgtype.Map
    tables  publication.Tables
    config  config.SnapshotConfig
}
```

#### 2. **Coordinator**
- Elected via PostgreSQL advisory locks
- Creates job metadata and chunks
- Exports snapshot transaction
- Participates in chunk processing (like any other worker)

#### 3. **Worker**
- Claims available chunks (using `SELECT FOR UPDATE SKIP LOCKED`)
- Processes chunk data within snapshot transaction
- Updates progress and heartbeats
- All instances (including coordinator) act as workers

#### 4. **Metadata Tables**

The snapshot feature uses two metadata tables to track progress. These tables are automatically created and migrated when needed.

**Job Table** (`cdc_snapshot_job`):
```sql
CREATE TABLE cdc_snapshot_job (
    slot_name        TEXT PRIMARY KEY,
    snapshot_id      TEXT NOT NULL,
    snapshot_lsn     TEXT NOT NULL,
    started_at       TIMESTAMP NOT NULL,
    completed        BOOLEAN DEFAULT FALSE,
    total_chunks     INT NOT NULL DEFAULT 0,
    completed_chunks INT NOT NULL DEFAULT 0
);
```

**Chunks Table** (`cdc_snapshot_chunks`):
```sql
CREATE TABLE cdc_snapshot_chunks (
    id              SERIAL PRIMARY KEY,
    slot_name       TEXT NOT NULL,
    table_schema    TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    chunk_index     INT NOT NULL,
    chunk_start     BIGINT NOT NULL,
    chunk_size      BIGINT NOT NULL,
    -- Partitioning strategy fields (added via migration)
    range_start     BIGINT,              -- For integer_range strategy
    range_end       BIGINT,              -- For integer_range strategy
    block_start     BIGINT,              -- For ctid_block strategy
    block_end       BIGINT,              -- For ctid_block strategy
    is_last_chunk   BOOLEAN DEFAULT FALSE,
    partition_strategy TEXT DEFAULT 'offset',
    -- Tracking fields
    status          TEXT NOT NULL DEFAULT 'pending',
    claimed_by      TEXT,
    claimed_at      TIMESTAMP,
    heartbeat_at    TIMESTAMP,
    completed_at    TIMESTAMP,
    rows_processed  BIGINT DEFAULT 0,
    UNIQUE(slot_name, table_schema, table_name, chunk_index)
);
```

### Lifecycle Phases

1. Elect a coordinator with a session advisory lock.
2. Create the logical slot with `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput (SNAPSHOT 'export')`.
3. Retain `snapshot_name` and `consistent_point`. Keep the replication export session open and idle.
4. Open a separate `REPEATABLE READ` transaction and import `snapshot_name` before any query. Discover chunks in this view.
5. Store that snapshot name and consistent point in the job. Each chunk reader imports the same snapshot in its own `REPEATABLE READ` transaction.
6. Wait for every chunk to complete. Mark the job complete, close the discovery and export sessions, and send END.
7. Start logical replication from the stored `consistent_point`.

For `snapshot_only`, use a SQL `pg_export_snapshot()` transaction instead. No slot or CDC boundary is needed; event and metadata LSNs are `0/0`.

## Getting Started

### Prerequisites

The replication settings and REPLICATION privilege below apply to `initial` and
CDC modes. `snapshot_only` uses ordinary SQL connections and needs table reads
and permission to create/write its metadata tables, not logical replication.

1. **[Supported PostgreSQL server](../README.md#compatibility)** with logical replication enabled:
   ```sql
   -- Check wal_level
   SHOW wal_level;  -- Must be 'logical'
   
   -- Check max_replication_slots
   SHOW max_replication_slots;  -- Must be > 0
   ```

2. **User Permissions**:
   ```sql
   -- Grant replication privilege
   ALTER USER your_user WITH REPLICATION;
   
   -- Grant access to tables
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_user;
   ```

3. **Configure PostgreSQL** (`postgresql.conf`):
   ```ini
   wal_level = logical
   max_replication_slots = 10
   max_wal_senders = 10
   ```

### Basic Configuration

```yaml
# config.yml
host: localhost
database: mydb
port: 5432
username: cdc_user
password: cdc_pass

publication:
  name: cdc_publication
  createIfNotExists: true
  operations:
    - INSERT
    - UPDATE
    - DELETE
  tables:
    - name: users
      schema: public
      replicaIdentity: FULL
    - name: orders
      schema: public
      replicaIdentity: FULL

slot:
  name: cdc_slot
  createIfNotExists: true
  slotActivityCheckerInterval: 3000

# Snapshot Configuration
snapshot:
  enabled: true                # Enable snapshot feature
  mode: initial                # Take snapshot only if not completed before
  chunkSize: 10000             # Rows per chunk
  claimTimeout: 30s            # Reclaim stale chunks after 30s
  heartbeatInterval: 10s       # Send heartbeat every 10s

logger:
  logLevel: info

metric:
  port: 8081
```

### Simple Example

```go
package main

import (
    "context"
    "log"
    "encoding/json"
    
    cdc "github.com/Trendyol/go-pq-cdc"
    "github.com/Trendyol/go-pq-cdc/config"
    "github.com/Trendyol/go-pq-cdc/pq/message/format"
    "github.com/Trendyol/go-pq-cdc/pq/replication"
)

func main() {
    ctx := context.Background()
    
    // Load configuration
    cfg, err := config.ReadConfigYAML("config.yml")
    if err != nil {
        log.Fatal(err)
    }
    
    // Create connector with snapshot support
    connector, err := cdc.NewConnector(ctx, cfg, handleMessage)
    if err != nil {
        log.Fatal(err)
    }
    defer connector.Close()
    
    // Start (snapshot + CDC)
    connector.Start(ctx)
}

func handleMessage(ctx *replication.ListenerContext) {
    switch msg := ctx.Message.(type) {
    
    // Snapshot events
    case *format.Snapshot:
        handleSnapshot(msg)
        
    // CDC events
    case *format.Insert:
        log.Printf("INSERT: %v", msg.Decoded)
    case *format.Update:
        log.Printf("UPDATE: %v -> %v", msg.OldDecoded, msg.NewDecoded)
    case *format.Delete:
        log.Printf("DELETE: %v", msg.OldDecoded)
    }
    
    if err := ctx.Ack(); err != nil {
        log.Printf("ACK error: %v", err)
    }
}

func handleSnapshot(s *format.Snapshot) {
    switch s.EventType {
    case format.SnapshotEventTypeBegin:
        log.Printf("📸 Snapshot BEGIN | LSN: %s", s.LSN.String())
        
    case format.SnapshotEventTypeData:
        data, _ := json.Marshal(s.Data)
        log.Printf("📸 Snapshot DATA | Table: %s.%s | Data: %s",
            s.Schema, s.Table, string(data))
        
    case format.SnapshotEventTypeEnd:
        log.Printf("📸 Snapshot END | LSN: %s", s.LSN.String())
    }
}
```

### Expected Output

```
INFO: publication created: cdc_publication
INFO: system identification: systemID=... timeline=1 xLogPos=0/12345678
INFO: [coordinator] elected as coordinator instanceID=host-12345
INFO: [coordinator] chunks created table=users chunks=10
INFO: [coordinator] chunks created table=orders chunks=5
INFO: [coordinator] snapshot exported snapshotID=00000003-00000002-1
INFO: replication slot created: cdc_slot restartLSN=0/12345678
📸 Snapshot BEGIN | LSN: 0/12345678
📸 Snapshot DATA | Table: public.users | Data: {"id":1,"name":"Alice"}
📸 Snapshot DATA | Table: public.users | Data: {"id":2,"name":"Bob"}
...
📸 Snapshot DATA | Table: public.orders | Data: {"id":101,"user_id":1}
...
📸 Snapshot END | LSN: 0/12345678
INFO: snapshot completed successfully
INFO: CDC will continue from LSN: 0/12345678
INSERT: map[id:1001 name:Charlie]  <-- New data after snapshot
```

---

## Configuration Reference

### Snapshot Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `false` | Enable/disable snapshot feature |
| `mode` | string | `never` | Snapshot mode: `initial`, `never`, or `snapshot_only` |
| `chunkSize` | int64 | `8000` | Number of rows per chunk |
| `claimTimeout` | duration | `30s` | Timeout to reclaim stale chunks |
| `heartbeatInterval` | duration | `5s` | Interval for worker heartbeat updates |
| `instanceId` | string | `hostname-pid` | Custom instance identifier (optional) |
| `resnapshot` | bool | `false` | Force reprocessing by cleaning metadata for this slot (see below) |
| `queryCondition` | string | `""` | Global query condition applied to all snapshot queries (SNAPSHOT-ONLY, see [Custom Query Conditions](#custom-query-conditions)) |

### Resnapshot

Sometimes you need to reprocess all snapshot data again (e.g., after schema changes, data corrections, or to rebuild downstream systems). The `resnapshot` option allows you to do this safely.

#### Why Resnapshot?

By default, the library checks `cdc_snapshot_job.completed` field. If a snapshot was already completed, it skips to CDC mode. This is efficient but prevents reprocessing.

#### How It Works

When `resnapshot: true`:
1. **Cleans metadata for THIS slot only** - Deletes rows from `cdc_snapshot_job` and `cdc_snapshot_chunks` where `slot_name` matches
2. **Does NOT affect other connectors** - Multiple teams can use different connectors in the same database safely
3. **Preserves table structure** - Uses DELETE, not DROP TABLE, so indexes and schema remain intact
4. **Creates a fresh boundary** - In `initial` mode, drops and recreates the configured inactive replication slot. This discards its old stream position. An active slot is refused. In `snapshot_only` mode, creates a fresh SQL export without a slot.

#### Configuration

```yaml
snapshot:
  enabled: true
  mode: initial
  resnapshot: true  # Clean metadata and reprocess snapshot
  chunkSize: 10000
```

#### Use Cases

- **Schema changes**: After adding/removing columns, reprocess to capture new structure
- **Data corrections**: After fixing data issues in source, rebuild downstream
- **Disaster recovery**: Rebuild downstream systems from scratch
- **Testing**: Repeatedly test snapshot behavior during development

#### Multi-Connector Safety

If multiple teams use different connectors (slots) on the same database:

```
Database: shared_db
├── Team A: slot_name = "team_a_slot"  ← resnapshot only affects this
├── Team B: slot_name = "team_b_slot"  ← Unaffected
└── Team C: slot_name = "team_c_slot"  ← Unaffected
```

**Important**: `resnapshot` only deletes metadata WHERE `slot_name = '<your_slot>'`. Other connectors' data remains intact.

#### Example: Reprocessing After Data Fix

```yaml
# Step 1: Fix data in source database
# Step 2: Deploy with resnapshot=true
snapshot:
  enabled: true
  mode: initial
  resnapshot: true

# Step 3: After successful reprocessing, set back to false
snapshot:
  enabled: true
  mode: initial
  resnapshot: false  # Normal operation
```

### Custom Query Conditions

The `queryCondition` feature allows you to add custom WHERE clause conditions to snapshot queries. This is useful for filtering data during snapshot (e.g., excluding soft-deleted records, filtering by status, date ranges, etc.).

#### Important: Snapshot-Only Feature

> This feature is **SNAPSHOT-ONLY** and does **NOT** affect CDC (publication).
>
> - **Snapshot**: Uses SQL `SELECT` queries — `queryCondition` is applied.
> - **CDC**: Uses the WAL replication stream — `queryCondition` is **not** used.
>
> CDC will capture every change to tables in the publication regardless of the snapshot `queryCondition`. The condition only filters which existing rows are captured during the initial snapshot phase.

#### Configuration

You can specify query conditions in two places:

1. **Global Condition** (`snapshot.queryCondition`) — applies to all snapshot tables:
   ```yaml
   snapshot:
     enabled: true
     mode: initial
     queryCondition: "status = 'active'"  # Global condition
   ```

2. **Per-Table Condition** (`publication.tables[].queryCondition` or `snapshot.tables[].queryCondition`) — applies to a specific table:
   ```yaml
   publication:
     tables:
       - name: users
         schema: public
         replicaIdentity: FULL
         queryCondition: "deleted_at IS NULL"  # Per-table condition
   ```

**Priority:** a per-table condition fully overrides the global `snapshot.queryCondition` for that table.

#### How It Works

The query condition is appended to the WHERE clause with `AND`:

```sql
-- Without queryCondition
SELECT * FROM public.users WHERE id >= 1 AND id <= 1000 ORDER BY id LIMIT 1000

-- With queryCondition: "deleted_at IS NULL"
SELECT * FROM public.users WHERE id >= 1 AND id <= 1000 AND deleted_at IS NULL ORDER BY id LIMIT 1000
```

The condition is applied to **all** snapshot query strategies (`integer_range`, `ctid_block`, `offset`) and to the row-count query used for chunk estimation.

#### Use Cases

- **Filter soft-deleted records:** `queryCondition: "deleted_at IS NULL"`
- **Filter by status:** `queryCondition: "status = 'active'"`
- **Date range filtering:** `queryCondition: "created_at >= '2024-01-01'"`
- **Multiple conditions:** `queryCondition: "status = 'active' AND archived = false"`
- **Exclude test data:** `queryCondition: "environment != 'test'"`

#### Example Configuration

```yaml
publication:
  createIfNotExists: true
  name: cdc_publication
  operations:
    - INSERT
    - UPDATE
    - DELETE
  tables:
    - name: users
      schema: public
      replicaIdentity: FULL
      queryCondition: "deleted_at IS NULL"      # Per-table override
    - name: orders
      schema: public
      replicaIdentity: FULL
      queryCondition: "status != 'cancelled'"   # Per-table override
    - name: products
      schema: public
      replicaIdentity: FULL                     # Uses global condition

snapshot:
  enabled: true
  mode: initial
  queryCondition: "created_at >= '2024-01-01'"  # Global fallback
  chunkSize: 10000
```

In this example:

- `users` uses `"deleted_at IS NULL"` (per-table takes precedence).
- `orders` uses `"status != 'cancelled'"` (per-table takes precedence).
- `products` uses `"created_at >= '2024-01-01'"` (global condition).

#### Notes

- The condition must be valid SQL that can be appended with `AND`.
- If you also need to filter CDC events, filter inside your application handler — this feature does not influence the replication stream.
- The condition is applied to the row-count query too, so chunk estimation reflects the filtered set.

---

#### Configuration

```yaml
snapshot:
  enabled: true
  mode: initial
  forceResnapshot: true  # Clean metadata and reprocess snapshot
  chunkSize: 10000
```

#### Use Cases

- **Schema changes**: After adding/removing columns, reprocess to capture new structure
- **Data corrections**: After fixing data issues in source, rebuild downstream
- **Disaster recovery**: Rebuild downstream systems from scratch
- **Testing**: Repeatedly test snapshot behavior during development

#### Multi-Connector Safety

If multiple teams use different connectors (slots) on the same database:

```
Database: shared_db
├── Team A: slot_name = "team_a_slot"  ← forceResnapshot only affects this
├── Team B: slot_name = "team_b_slot"  ← Unaffected
└── Team C: slot_name = "team_c_slot"  ← Unaffected
```

**Important**: `forceResnapshot` only deletes metadata WHERE `slot_name = '<your_slot>'`. Other connectors' data remains intact.

#### Example: Reprocessing After Data Fix

```yaml
# Step 1: Fix data in source database
# Step 2: Deploy with forceResnapshot=true
snapshot:
  enabled: true
  mode: initial
  forceResnapshot: true

# Step 3: After successful reprocessing, set back to false
snapshot:
  enabled: true
  mode: initial
  forceResnapshot: false  # Normal operation
```

---

### Snapshot Modes

#### `initial` Mode
```yaml
snapshot:
  mode: initial
```
- Takes snapshot **only once** (on first run)
- Checks if job is already completed
- If completed, skips snapshot and goes directly to CDC
- **Recommended for production**
- Use `resnapshot: true` to override and reprocess

#### `never` Mode
```yaml
snapshot:
  mode: never
```
- Never takes snapshot
- Always starts CDC from current point
- Use when you don't need historical data

#### `snapshot_only` Mode

Exports table data and exits without CDC, a publication, or a replication slot.
Specify source tables in `snapshot.tables`. Set `snapshot.id` to a shared job name
when several instances work on the same export; otherwise the database name
identifies the job. Completed jobs are skipped unless `resnapshot` is enabled.

```yaml
snapshot:
  enabled: true
  mode: snapshot_only
  tables:
    - name: users
      schema: public
```

All readers import one SQL-exported snapshot. LSNs are `0/0`, not a position from
which CDC can start. A failed worker chunk can be reclaimed while the coordinator
is alive. If the export session is gone, restart all participants; the new
coordinator clears all chunk progress and starts a fresh snapshot. Consumers
must discard incomplete exports rather than combine rows from different attempts.

## How It Works

### Phase 1: Coordinator Election

The coordinator holds a session advisory lock through the full snapshot.
Before slot creation, it commits a PENDING job and removes the previous chunks
in one metadata transaction. A process exit after slot creation therefore leaves
a recoverable attempt, and other instances cannot accept the previous completed
generation while slot creation waits.
Other instances wait for its job metadata, then claim chunks from the same job.
This wait has no fixed deadline: slot creation can wait for open transactions,
and chunk discovery can take a long time. Workers stop if their context is
canceled or the coordinator lock is lost.
A worker must use a distinct `snapshot.instanceId` when several workers run on
one host. All instances must use the same source tables and snapshot settings.

### Phase 2: Snapshot Export

In `initial` mode, slot creation returns the exported snapshot and its stream
boundary atomically. The discovery connection imports this exact snapshot:

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION SNAPSHOT '<snapshot_name from slot creation>';
```

The slot export session must remain idle. **Do not send `SELECT 1`, or any other
command, on it.** A new command or session close makes the snapshot unavailable
to new importers. The library retains this session until all chunks finish,
because chunk retries and late workers also need to import it. This uses one
walsender during the snapshot, in addition to the normal SQL connections.

The discovery transaction uses separate SQL connections for metadata writes.
Its keepalive is stopped before its transaction closes. The slot export session
never receives SQL keepalives.

PostgreSQL documents these rules in the
[slot creation protocol](https://www.postgresql.org/docs/18/protocol-replication.html#PROTOCOL-REPLICATION-CREATE-REPLICATION-SLOT)
and [exported snapshots](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-EXPLANATION-EXPORTED-SNAPSHOTS).
The slot command uses the parenthesized `SNAPSHOT 'export'` option.

### Slot Operations

The export-holding replication session preserves the snapshot, but **does not
keep the slot active**. Slot creation releases slot ownership when the command
returns. In `pg_replication_slots`, `active` is false, `active_pid` is NULL, and
`inactive_since` is set even while the export session remains open. Do not send
a keepalive command on that session: it would end the exported snapshot.

PostgreSQL's [`idle_replication_slot_timeout`](https://www.postgresql.org/docs/18/runtime-config-replication.html#GUC-IDLE-REPLICATION-SLOT-TIMEOUT)
is disabled by default. If enabled, it invalidates an inactive slot at a
checkpoint after the timeout expires. **This includes the entire initial
snapshot**, the gap between export close and CDC startup, and shutdown or
restart. A slow END handler or a long outage extends the inactive period.
Keep this timeout disabled for initial snapshots, or set it above the longest
snapshot plus handoff delay and planned downtime. An idle export session alone
is not protection. Monitor `inactive_since`, `invalidation_reason`, and
`wal_status`; see
[`pg_replication_slots`](https://www.postgresql.org/docs/18/view-pg-replication-slots.html).
An invalidated slot cannot resume its old WAL position. Recover with an explicit
resnapshot (and replace downstream state), or use a new slot and full snapshot.
The library does not silently replace a completed job's invalidated stream slot.
WAL retention limits still apply during a snapshot; size and monitor
`max_slot_wal_keep_size` for the expected snapshot duration and write rate.

The library does not enable failover slots. Slot creation uses the server's
`FAILOVER false` default. A server-side failover slot does not preserve the
exported snapshot, SQL transactions, or coordinator lock across promotion.
In-progress snapshots must start a fresh attempt after failover. Slot recreation
for an interrupted job or explicit resnapshot also resets slot options to their
defaults; it does not preserve an administratively enabled failover flag.
Operators who configure failover slots must manage synchronization and WAL
availability themselves. The library makes no automatic failover guarantee.

### Phase 3: Metadata Creation

The job stores the slot-provided `snapshot_name` and `consistent_point` as
`snapshot_id` and `snapshot_lsn`. No separate WAL-position sample is used.
Chunk discovery reads through the imported discovery transaction. Metadata
writes occur outside that read transaction so other workers can see them.

### Phase 4: Chunk Processing

Each reader starts `REPEATABLE READ`, imports the job snapshot before querying,
reads its claimed chunk, and completes that transaction. Completed chunks can
be skipped only while the same export remains available. A late worker or a
reconnected reader must import the original snapshot; an import failure stops
the snapshot. It never substitutes a newer SQL snapshot.

Workers that temporarily have no unclaimed chunks wait for the remaining
workers rather than close the export early. Chunk claims are tied to the job
snapshot identifier so workers cannot read replacement-job chunks with an old
snapshot. A prepared worker also pins the snapshot identifier; if it changes
before execution, the worker must prepare a new attempt. Errors are returned
by `Run`; `Start` discards its returned error. Once the job is marked complete,
`Close` from its END callback is a successful shutdown in both snapshot modes.
Other END delivery errors and caller cancellation still fail the operation.

### Phase 5: CDC Continuation

After all chunks finish, the job is marked complete and END carries the stored
consistent point. The coordinator closes the snapshot sessions. CDC starts from
that same point. PostgreSQL supplies all changes after the exported view,
including transactions that were committed in WAL but not yet visible when a
separate ordinary snapshot would have been taken.

This boundary does not promise exactly-once delivery after crashes or worker
retries. Consumers must handle repeated events and discard incomplete snapshot
attempts before publishing a replacement snapshot.

### Partitioning Strategies

The snapshot feature automatically selects the most efficient partitioning strategy based on your table structure. You can also override this behavior per-table.

#### Strategy Overview

| Strategy | Best For | Performance | Query Pattern |
|----------|----------|-------------|---------------|
| **Integer Range** | Sequential integer PKs (id, serial) | ⚡ Fastest | `WHERE id >= X AND id <= Y` |
| **CTID Block** | String/UUID PKs, hash-based PKs, any table | 🚀 Very Fast | `WHERE ctid >= '(block,0)'::tid` |
| **Offset** | Fallback when others fail | 🐌 Slow | `ORDER BY pk LIMIT X OFFSET Y` |

#### 1. Integer Range Partitioning

**When Used:** Tables with a single, sequential integer primary key (e.g., `SERIAL`, `BIGSERIAL`).

```sql
-- Auto-detected for tables like:
CREATE TABLE users (
    id SERIAL PRIMARY KEY,  -- ✅ Sequential integer PK
    name TEXT
);

-- Chunk queries:
SELECT * FROM users WHERE id >= 1 AND id <= 10000;      -- Chunk 0
SELECT * FROM users WHERE id >= 10001 AND id <= 20000;  -- Chunk 1
```

**Advantages:**
- Uses index scan (very fast)
- Predictable chunk sizes
- No sorting overhead

#### 2. CTID Block Partitioning

**When Used:** Tables with string PKs, UUID PKs, composite PKs, hash-based integer PKs, or any table where integer range isn't optimal.

```sql
-- Auto-detected for tables like:
CREATE TABLE documents (
    id UUID PRIMARY KEY,  -- ✅ Non-integer PK → CTID
    content TEXT
);

CREATE TABLE events (
    id INT PRIMARY KEY,   -- Integer, but could be hash-based
    data JSONB
);
-- User can force CTID for hash-based PKs via snapshotPartitionStrategy

-- Chunk queries (using PostgreSQL physical blocks):
SELECT * FROM documents WHERE ctid >= '(0,0)'::tid AND ctid < '(100,0)'::tid;   -- Block 0-99
SELECT * FROM documents WHERE ctid >= '(100,0)'::tid AND ctid < '(200,0)'::tid; -- Block 100-199
SELECT * FROM documents WHERE ctid >= '(200,0)'::tid;  -- Last chunk (no upper bound)
```

**Advantages:**
- Works for any table type
- Physical scan (no index needed)
- Efficient for large tables with non-integer PKs
- **10-100x faster** than OFFSET for string PKs

**Block discovery:**

Block-size queries run on the imported discovery connection. Physical relation
size is not MVCC-versioned. The final CTID chunk has no upper bound so rows
visible to the snapshot remain covered even if physical size changes.

**Last Chunk Safety:**

The last CTID chunk has no upper bound (`block_end = NULL`), ensuring all rows from the starting block onwards are captured:

```sql
-- Regular chunk (has upper bound):
SELECT * FROM table WHERE ctid >= '(0,0)'::tid AND ctid < '(100,0)'::tid;

-- Last chunk (NO upper bound - captures everything from block 200+):
SELECT * FROM table WHERE ctid >= '(200,0)'::tid;
```

#### 3. Offset Partitioning

**When Used:** Fallback when other strategies fail (rare).

```sql
-- Chunk queries:
SELECT * FROM table ORDER BY pk LIMIT 10000 OFFSET 0;      -- Chunk 0
SELECT * FROM table ORDER BY pk LIMIT 10000 OFFSET 10000;  -- Chunk 1
```

**⚠️ Warning:** Offset-based queries are slow for large tables because PostgreSQL must scan and skip rows.

#### Override Strategy Per-Table

You can explicitly set the partitioning strategy for each table:

```yaml
publication:
  tables:
    # Auto-detect (default)
    - name: users
      schema: public
      replicaIdentity: FULL
      # snapshotPartitionStrategy: auto  # implicit default

    # Force CTID for hash-based integer PK
    - name: events
      schema: public
      replicaIdentity: FULL
      snapshotPartitionStrategy: ctid_block  # Override: use CTID even for int PK

    # Force integer range (not recommended unless you know PK is sequential)
    - name: orders
      schema: public
      replicaIdentity: FULL
      snapshotPartitionStrategy: integer_range
```

**When to Override:**

| Scenario | Recommended Override |
|----------|---------------------|
| Integer PK but values are hash-based (non-sequential) | `ctid_block` (also auto-detected when PK span ≫ row count) |
| Sparse integer PK (large gaps / deleted IDs) | Auto falls back to `ctid_block`; force `integer_range` only if you need range scans |
| UUID/string PK | Auto-detected as `ctid_block` |
| Sequential integer PK | Auto-detected as `integer_range` |
| Want maximum control | Explicit strategy per table |

---

## Advanced Topics

### Multi-Instance Deployment

Deploy multiple instances for faster snapshot processing:

```yaml
# Instance 1
snapshot:
  instanceId: "instance-1"  # Optional: auto-generated (hostname & pid) if not set
  chunkSize: 10000

# Instance 2
snapshot:
  instanceId: "instance-2"
  chunkSize: 10000

# Instance 3
snapshot:
  instanceId: "instance-3"
  chunkSize: 10000
```

**How It Works:**

```
┌─────────────────────────────────────────────────────────────┐
│                     Chunk Work Queue                        │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐  │
│  │ C0  │ C1  │ C2  │ C3  │ C4  │ C5  │ C6  │ C7  │ C8  │  │
│  │ ◄   │     │     │     │     │     │     │     │     │  │
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘  │
└─────────────────────────────────────────────────────────────┘
     ▲       ▲       ▲
     │       │       │
┌────┴────┐ ┌┴───────┴┐ ┌─────────┐
│Instance1│ │Instance2│ │Instance3│
│Coord+   │ │Worker   │ │Worker   │
│Worker   │ │         │ │         │
└─────────┘ └─────────┘ └─────────┘

Timeline:
  T0: Instance1 claims C0, Instance2 claims C1, Instance3 claims C2
  T1: Instance1 completes C0, claims C3
  T2: Instance2 completes C1, claims C4
  ...
  TN: All chunks completed → CDC starts
```

**Benefits:**
- **Parallelism**: 3x faster with 3 instances
- **Fault Tolerance**: If one crashes, others continue
- **Auto-Recovery**: Crashed chunks are reclaimed

### Crash Recovery

- **Worker loss, coordinator alive:** Other workers reclaim stale chunks after
  `claimTimeout` and import the same retained snapshot. A retried chunk can emit
  repeated rows.
- **Export session lost:** New imports fail. `Run` returns an error instead of
  reading under a different snapshot. `Supervise` classifies snapshot invalidation
  and PostgreSQL invalid-snapshot-identifier import errors as retryable bootstrap
  errors. It closes the failed connector and builds a fresh one that becomes
  coordinator or joins the new job. Direct `Run` callers must rebuild it.
  Caller cancellation and unrelated invalid parameters are not retried.
- **Coordinator or full deployment restart:** The new coordinator finds the
  incomplete job, recreates the configured inactive slot, deletes all job/chunk
  progress, and starts with a fresh export and consistent point. It does not
  resume completed chunks under a different snapshot.
- **Completed initial job:** Normal restart skips snapshotting and resumes CDC
  from the slot position. `resnapshot: true` explicitly requests a new snapshot
  and slot boundary.
- **Existing slot without a usable snapshot job:** Initial capture is refused
  with an error. Use a fresh slot name or explicitly set `resnapshot: true` to
  authorize recreation. No existing active slot is dropped.

Slot recreation discards the old CDC position. Consumers must replace or clear
partial snapshot state; restarting the source alone cannot retract rows already
sent to a downstream system. Use `Run` (or `Supervise`) to observe bootstrap
errors, and build a new connector for a retry.

### Snapshot callback shutdown

Snapshot callbacks run serially, outside the database operation. The connector
passes the operation context as `ListenerContext.Context`; `Close` cancels it,
so handlers must use that context for backpressure and other blocking work.
Calling `connector.Close()` from a snapshot callback, including END, is safe.

`Snapshotter.Close(ctx)` waits for the database operation only until the caller's
context ends. Database cleanup can finish after that deadline. It does not wait
on the callback that requested shutdown. A callback that ignores cancellation
can leave its own goroutine blocked; Go cannot stop arbitrary user code. No more
callbacks are delivered for that operation, and its database sessions are closed.
Direct snapshot users can use `ExecuteWithContext` for cancellation-aware handlers.

### Schema Migration & Backward Compatibility

The snapshot feature includes **automatic schema migration** support to ensure seamless upgrades when new features are added.

#### How It Works

When the snapshot system initializes, it automatically applies idempotent schema migrations:

```go
// Applied on every startup - safe and idempotent
ALTER TABLE cdc_snapshot_chunks ADD COLUMN IF NOT EXISTS block_start BIGINT;
ALTER TABLE cdc_snapshot_chunks ADD COLUMN IF NOT EXISTS block_end BIGINT;
ALTER TABLE cdc_snapshot_chunks ADD COLUMN IF NOT EXISTS is_last_chunk BOOLEAN DEFAULT FALSE;
ALTER TABLE cdc_snapshot_chunks ADD COLUMN IF NOT EXISTS partition_strategy TEXT DEFAULT 'offset';
ALTER TABLE cdc_snapshot_chunks ADD COLUMN IF NOT EXISTS range_start BIGINT;
ALTER TABLE cdc_snapshot_chunks ADD COLUMN IF NOT EXISTS range_end BIGINT;
```

#### Benefits

✅ **Zero-downtime upgrades**: Upgrade go-pq-cdc without manual schema changes  
✅ **Backward compatible**: Old metadata tables work with new code  
✅ **Idempotent**: Safe to run multiple times  
✅ **Non-blocking**: Migrations run automatically on startup  

#### Upgrade Scenario

```
Timeline:
  T0: Running go-pq-cdc v1.0 (only offset partitioning)
      - Metadata tables have basic schema
      - Snapshot works fine
  
  T1: Upgrade to v1.1 (adds CTID block partitioning)
      - On startup, migrations run automatically
      - New columns added: block_start, block_end, is_last_chunk, partition_strategy
      - Existing data preserved
      - New snapshots can use CTID strategy
  
  T2: Upgrade to v1.2 (adds integer range partitioning)
      - On startup, migrations run automatically
      - New columns added: range_start, range_end
      - All previous data still works
      - New snapshots can use all 3 strategies
```

#### Migration Safety

- **IF NOT EXISTS**: Columns are only added if they don't exist
- **Non-destructive**: Never drops columns or data
- **Logged**: Migration failures are logged but don't stop startup
- **Tested**: All migrations are tested in integration tests

**Note**: This feature was added in PR #76 to support seamless version upgrades.

---

### Performance Tuning

#### 1. Chunk Size Tuning

```yaml
# Small tables (< 100K rows)
snapshot:
  chunkSize: 5000  # Smaller chunks, more overhead but fine-grained

# Medium tables (100K - 1M rows)
snapshot:
  chunkSize: 10000  # Balanced

# Large tables (> 1M rows)
snapshot:
  chunkSize: 50000  # Larger chunks, less overhead
```

**Trade-offs:**
- **Smaller chunks**: More parallelism, more metadata overhead, easier recovery
- **Larger chunks**: Less parallelism, less overhead, longer per-chunk time

#### 2. Heartbeat & Timeout Tuning

```yaml
# Fast failure detection (aggressive)
snapshot:
  heartbeatInterval: 5s
  claimTimeout: 15s

# Slow failure detection (conservative)
snapshot:
  heartbeatInterval: 30s
  claimTimeout: 120s
```

**Guidelines:**
- `claimTimeout` should be at least 3x `heartbeatInterval`
- Shorter intervals = faster failure detection but more DB load
- Longer intervals = less DB load but slower recovery

#### 3. Ordering Strategy

Tables are processed in order, but chunks within a table can be parallel:

```go
// Primary key ordering (preferred)
SELECT * FROM users ORDER BY id LIMIT 10000 OFFSET 0

// Composite key ordering
SELECT * FROM orders ORDER BY user_id, created_at LIMIT 10000 OFFSET 0

// No primary key (fallback to ctid)
SELECT * FROM logs ORDER BY ctid LIMIT 10000 OFFSET 0
```

**Best Practice:** Ensure tables have primary keys for optimal performance.

---

## Monitoring & Observability

### Metrics

All metrics are exposed at `/metrics` endpoint (Prometheus format):

| Metric | Type | Description |
|--------|------|-------------|
| `go_pq_cdc_snapshot_in_progress` | Gauge | 1 if snapshot is running, 0 otherwise |
| `go_pq_cdc_snapshot_total_tables` | Gauge | Total number of tables to snapshot |
| `go_pq_cdc_snapshot_total_chunks` | Gauge | Total number of chunks to process |
| `go_pq_cdc_snapshot_completed_chunks` | Gauge | Number of completed chunks |
| `go_pq_cdc_snapshot_total_rows` | Counter | Total rows processed in snapshot |
| `go_pq_cdc_snapshot_duration_seconds` | Gauge | Duration of last snapshot in seconds |

**Example Query:**

```promql
# Snapshot progress percentage
100 * (
  go_pq_cdc_snapshot_completed_chunks{slot_name="cdc_slot"} 
  / 
  go_pq_cdc_snapshot_total_chunks{slot_name="cdc_slot"}
)

# Snapshot throughput (rows/second)
rate(go_pq_cdc_snapshot_total_rows{slot_name="cdc_slot"}[1m])
```

### Logs

Key log messages to monitor:

```
# Startup
INFO [coordinator] elected as coordinator instanceID=instance-1
INFO [coordinator] chunks created table=users chunks=100
INFO [coordinator] snapshot exported snapshotID=00000003-00000002-1

# Progress
DEBUG [worker] processing chunk table=users chunkIndex=5 chunkStart=50000
DEBUG [worker] chunk completed chunkID=123 rowsProcessed=10000

# Completion
INFO [snapshot] all chunks completed
INFO snapshot completed successfully snapshotLSN=0/12345678

# Errors
WARN [heartbeat] failed to update chunkID=456 error=connection timeout
ERROR [worker] chunk processing failed chunkID=789 error=query timeout
```

### Database Queries for Monitoring

```sql
-- Overall progress
SELECT 
    slot_name,
    completed,
    completed_chunks,
    total_chunks,
    ROUND(100.0 * completed_chunks / NULLIF(total_chunks, 0), 2) as progress_pct,
    snapshot_lsn
FROM cdc_snapshot_job;

-- Chunk breakdown by status
SELECT 
    table_name,
    status,
    COUNT(*) as chunk_count,
    SUM(rows_processed) as total_rows
FROM cdc_snapshot_chunks
WHERE slot_name = 'cdc_slot'
GROUP BY table_name, status
ORDER BY table_name, status;

-- Find stale chunks (potential issues)
SELECT 
    id,
    table_name,
    chunk_index,
    claimed_by,
    status,
    NOW() - heartbeat_at as stale_duration
FROM cdc_snapshot_chunks
WHERE slot_name = 'cdc_slot'
  AND status = 'in_progress'
  AND heartbeat_at < NOW() - INTERVAL '1 minute'
ORDER BY heartbeat_at;

-- Processing timeline
SELECT 
    table_name,
    chunk_index,
    status,
    claimed_by,
    claimed_at,
    completed_at,
    EXTRACT(EPOCH FROM (completed_at - claimed_at)) as duration_seconds,
    rows_processed
FROM cdc_snapshot_chunks
WHERE slot_name = 'cdc_slot'
  AND status = 'completed'
ORDER BY completed_at DESC
LIMIT 20;
```

---

## Troubleshooting

### Problem: Snapshot Takes Too Long

**Symptoms:**
- Snapshot running for hours
- Low throughput (< 1000 rows/sec)

**Causes & Solutions:**

1. **Chunk size too small**
   ```yaml
   # Before
   snapshot:
     chunkSize: 1000  # ❌ Too small, high overhead
   
   # After
   snapshot:
     chunkSize: 10000  # ✅ Better
   ```

2. **Table has no primary key**
   ```sql
   -- Check
   SELECT tablename, indexname 
   FROM pg_indexes 
   WHERE schemaname = 'public' AND indexdef LIKE '%PRIMARY KEY%';
   
   -- Fix
   ALTER TABLE your_table ADD PRIMARY KEY (id);
   ```

3. **Single instance processing**
   ```bash
   # Deploy more instances for parallel processing
   ./app --config config1.yml &
   ./app --config config2.yml &
   ./app --config config3.yml &
   ```

### Problem: Duplicate Data in Consumer

**Symptoms:**
- Same rows appear in both snapshot and CDC events

**Cause:**
- LSN not properly set for CDC continuation

**Debug:**
```sql
-- Check snapshot LSN
SELECT snapshot_lsn FROM cdc_snapshot_job WHERE slot_name = 'cdc_slot';

-- Check replication slot position
SELECT 
    slot_name,
    restart_lsn,
    confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'cdc_slot';

-- They should match!
```

### Problem: Snapshot slot creation is denied

`initial` mode needs REPLICATION privilege and logical replication settings.
Check these settings with the database administrator. `snapshot_only` uses SQL
snapshot export and does not require the REPLICATION attribute.

### Problem: Chunks Stuck in "in_progress"

**Symptoms:**
```sql
SELECT status, COUNT(*) 
FROM cdc_snapshot_chunks 
WHERE slot_name = 'cdc_slot'
GROUP BY status;

-- Result:
-- pending:      50
-- in_progress:  10  ← Stuck for 30+ minutes
-- completed:    40
```

**Cause:**
Worker crashed without cleanup.

**Solution:**

1. **Wait for auto-recovery** (after `claimTimeout`)
2. **Manual recovery**:
   ```sql
   -- Force reset stale chunks
   UPDATE cdc_snapshot_chunks
   SET status = 'pending',
       claimed_by = NULL,
       heartbeat_at = NULL
   WHERE slot_name = 'cdc_slot'
     AND status = 'in_progress'
     AND heartbeat_at < NOW() - INTERVAL '5 minutes';
   ```

### Problem: "snapshot ... does not exist"

The original export session is no longer available. Stop all participants and
restart with new connectors. Keep the incomplete job metadata: it lets the
coordinator detect the interrupted attempt and recreate its slot and export.
Do not manually clear metadata and reuse the old slot; that slot has no
importable snapshot. Discard partial downstream snapshot state.

---

## Best Practices

### 1. Use `initial` Mode in Production

```yaml
snapshot:
  mode: initial  # ✅ Safe: takes snapshot only once
```

This prevents re-snapshotting on every restart.

### 2. Monitor Snapshot Progress

Set up alerts:
```yaml
# Prometheus alerting rule
groups:
- name: snapshot_alerts
  rules:
  - alert: SnapshotStalled
    expr: |
      go_pq_cdc_snapshot_in_progress == 1
      and
      rate(go_pq_cdc_snapshot_completed_chunks[10m]) == 0
    for: 30m
    annotations:
      summary: "Snapshot has not progressed in 30 minutes"
```

### 3. Tune Chunk Size Based on Table Size

```yaml
# Small tables (< 100K rows)
snapshot:
  chunkSize: 5000

# Large tables (> 10M rows)
snapshot:
  chunkSize: 50000
```

**Note:** Ensure replica is up-to-date to avoid stale data.

### 4. Clean Up After Successful Snapshot

Once snapshot is completed and stable:
```sql
-- Optional: Keep metadata for audit
-- Or delete to reduce clutter
DELETE FROM cdc_snapshot_chunks WHERE slot_name = 'old_slot';
DELETE FROM cdc_snapshot_job WHERE slot_name = 'old_slot';
```

### 5. Test Disaster Recovery

Periodically test recovery scenarios:
```bash
# 1. Start snapshot
./go-pq-cdc --config config.yml &
PID=$!

# 2. Kill mid-snapshot
sleep 10 && kill -9 $PID

# 3. Verify state
psql -c "SELECT * FROM cdc_snapshot_chunks WHERE status = 'in_progress';"

# 4. Restart and verify recovery
./go-pq-cdc --config config.yml
```

---

## FAQ

### Q: Can I change chunk size after snapshot starts?

**A:** No. Chunk size is determined when chunks are created. To change:
1. Complete or cancel current snapshot
2. Clean up metadata
3. Restart with new chunk size

### Q: Does snapshot affect database performance?

**A:** Minimal impact:
- Read-only queries (no writes)
- Uses efficient scans (index for Integer Range, physical for CTID)
- No table locks
- Impact similar to a slow analytical query

**Tip:** Run snapshot during off-peak hours for very large datasets.

### Q: My table has string/UUID primary keys. Will snapshot be slow?

**A:** No! The system automatically uses **CTID Block Partitioning** for non-integer PKs, which is 10-100x faster than offset-based queries.

```yaml
publication:
  tables:
    - name: documents
      schema: public
      replicaIdentity: FULL
      # CTID is auto-detected for UUID/string PKs - no config needed!
```

### Q: Can I force a specific partitioning strategy?

**A:** Yes! Use `snapshotPartitionStrategy` per-table:

```yaml
publication:
  tables:
    - name: events
      schema: public
      replicaIdentity: FULL
      snapshotPartitionStrategy: ctid_block  # Force CTID even for integer PK
```

**Available strategies:**
- `auto` (default): Auto-detect best strategy
- `integer_range`: For sequential integer PKs
- `ctid_block`: For string/UUID/hash PKs
- `offset`: Slow fallback (not recommended)

### Q: What happens if I add a new table to publication during snapshot?

**A:** The new table is ignored for the current snapshot. Options:
1. Finish the current snapshot, then request `resnapshot: true` for a new full snapshot
2. Manually snapshot new table using a different slot

### Q: Can I pause and resume snapshot?

**A:** No.

### Q: How do I know when snapshot is complete?

**Check logs:**
```
INFO snapshot completed successfully snapshotLSN=0/12345678
```

**Check database:**
```sql
SELECT completed FROM cdc_snapshot_job WHERE slot_name = 'cdc_slot';
-- Result: true
```

**Check metrics:**
```promql
go_pq_cdc_snapshot_in_progress{slot_name="cdc_slot"} == 0
```

### Q: What's the maximum table size supported?

**A:** No hard limit. 

**Considerations:**
- Larger tables = more chunks = longer snapshot time
- Chunk size should be tuned appropriately
- More instances = faster processing

### Q: Does snapshot work with TimescaleDB hypertables?

**A:** Yes! Hypertables are supported. The system treats them as regular tables.

```yaml
publication:
  tables:
    - name: metrics  # Hypertable
      schema: public
      replicaIdentity: FULL

snapshot:
  enabled: true
  chunkSize: 10000  # Works with TimescaleDB chunks internally
```

### Q: Can I snapshot only specific tables?

**A:** Yes, snapshot includes only tables in `publication.tables`:

```yaml
publication:
  tables:
    - name: users      # ✅ Included in snapshot
    - name: orders     # ✅ Included in snapshot
    # products table NOT listed → NOT in snapshot
```

### Q: What about very wide tables (many columns)?

**A:** The system handles wide tables well:
- Uses streaming result sets (low memory)
- Encodes each column appropriately
- No column limit

**Tip:** For tables with 100+ columns, consider reducing `chunkSize` to avoid large memory spikes.

---

## Summary

The Snapshot Feature provides a robust, scalable solution for initial data capture in PostgreSQL CDC pipelines:

✅ **Zero Data Loss**: Consistent snapshot + CDC continuation  
✅ **Smart Partitioning**: Auto-selects optimal strategy (Integer Range, CTID Block, Offset)  
✅ **Snapshot-Consistent**: Size queries run within snapshot transaction
✅ **Scalable**: Chunk-based, multi-instance processing  
✅ **Resilient**: Chunk reclamation within a live export, and a fresh snapshot after coordinator loss
✅ **Observable**: Rich metrics and progress tracking  
✅ **Production-Ready**: Battle-tested, handles edge cases  

**Get Started:**

```bash
# 1. Enable snapshot in config
vi config.yml  # Set snapshot.enabled=true, mode=initial

# 2. Run
./go-pq-cdc --config config.yml

# 3. Monitor
curl http://localhost:8081/metrics | grep snapshot
```

For questions or issues, please open a GitHub issue or refer to the main [README](../README.md).

---
