# Protocol Version Support and Streaming Transactions

## Overview

This document explains the changes introduced on `feature/protoversion1-support` compared with `main`.

Main improvements:

- Added configurable `slot.protoVersion` (`1` or `2`)
- Defaulted protocol version to `1` when not set
- Added `slot.streaming` and `slot.messages`, both opt-in (default `false`) and only meaningful for `protoVersion: 2`
- Updated replication startup arguments according to protocol version, streaming, and messages
- Added full decoding for `STREAM START` and `STREAM ABORT` payloads
- Fixed streamed transaction buffering so rolled-back streamed data is never emitted
- Expanded integration tests to validate both protocol versions and streaming rollback scenarios

## What Changed vs `main`

### 1) Configurable protocol version

`slot.Config` now includes:

- `protoVersion: 1` for compatibility mode (default)
- `protoVersion: 2` to unlock streaming and generic logical messages
- `streaming: true` to opt in to streamed in-progress transactions (requires `protoVersion: 2`)
- `messages: true` to opt in to generic logical decoding messages (requires `protoVersion: 2`; the driver does not decode them)

Validation now rejects unsupported `protoVersion` values.

### 2) Replication start behavior is version-aware

Replication startup now uses `slot.protoVersion`, `slot.streaming`, and `slot.messages`:

- Always sends `proto_version '<N>'`
- Sends `streaming 'true'` only when `protoVersion >= 2` **and** `streaming` is set
- Sends `messages 'true'` only when `protoVersion >= 2` **and** `messages` is set

Neither option is implied by the protocol version alone: `protoVersion: 2` only permits them, so a
caller who wants `proto_version 2` for another reason is not forced into the unbounded in-memory
buffering that streaming requires.

### 3) Stream message decoding is complete

`STREAM START` and `STREAM ABORT` messages are now parsed into typed structures:

- `StreamStart { Xid, FirstSegment }`
- `StreamAbort { Xid, SubXid }`

This allows transaction-level stream control using real XIDs.

### 4) Streamed transaction handling is now commit-safe

For `protoVersion: 2`, streamed transactions can be split and interleaved by PostgreSQL.
The stream sink now buffers messages per transaction XID and applies this policy:

- On `STREAM START`: begin/continue buffering for XID
- On `STREAM STOP`: keep buffered data, emit nothing
- On `STREAM COMMIT`: emit buffered data for XID, set last event WAL to commit end LSN
- On `STREAM ABORT`: discard buffered data for XID

Result: rolled-back streamed transactions are never delivered to handlers.

## Choosing `protoVersion`

### Use `protoVersion: 1` when:

- You need only basic protocol messages
- You do not require streamed in-progress transaction handling

### Use `protoVersion: 2` with `streaming: true` when:

- You want support for large/streamed in-progress transactions
- You want strict rollback safety for streamed transactions
- Your workload's largest transactions are known to fit in memory (streamed changes are buffered
  client-side, unbounded, until `STREAM COMMIT`/`STREAM ABORT`)

## Configuration Examples

### YAML

```yaml
slot:
  name: cdc_slot
  createIfNotExists: true
  slotActivityCheckerInterval: 3000
  protoVersion: 2
  streaming: true
```

### Go

```go
Slot: slot.Config{
    Name:                        "cdc_slot",
    CreateIfNotExists:           true,
    SlotActivityCheckerInterval: 3000,
    ProtoVersion:                2,
    Streaming:                   true,
},
```

## Compatibility Matrix

All modes require the [supported PostgreSQL server](../README.md#compatibility).

| slot.protoVersion | slot.streaming | Streaming protocol messages |
|-------------------|----------------|------------------------------|
| 1                 | n/a            | No                           |
| 2                 | false          | No                           |
| 2                 | true           | Yes                          |

## Test Coverage Added

The branch adds/updates tests to protect behavior:

- Unit tests for stream payload parsing:
  - `NewStreamStart`
  - `NewStreamAbort`
- Integration tests for streaming behavior:
  - Large streamed commit delivery
  - Interleaved streamed transactions
  - Streamed rollback emits no events
  - Rollback followed by commit emits only committed events
- Integration test helpers now run protocol-version subtests (`v1` and `v2`) where applicable

## Migration Notes

If you do not set `slot.protoVersion`, the default is `1`. Streaming is always off unless
`slot.streaming: true` is set explicitly, regardless of `protoVersion`.

Recommended migration path:

1. Set `protoVersion: 2` and `streaming: true` explicitly if you rely on streamed in-progress
   transaction handling.
2. Leave `protoVersion` unset (or `1`) otherwise.
3. Run your CDC integration tests with both versions if you support mixed environments.

