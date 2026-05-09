# Journal Rotation & Recovery

This document describes the journaling policy, rotation mechanism, and every recovery scenario the server handles.

## Overview

The trading engine uses a write-ahead journal for event sourcing and crash recovery. Every input command (order submit, cancel, deposit, etc.) is journaled before acknowledgement. The matching engine is deterministic, so replaying the journal from genesis reproduces the exact same state.

Snapshots capture the full exchange state at a known journal sequence boundary. Recovery from a snapshot skips replaying all events before that boundary, reducing startup time from O(total events) to O(events since snapshot).

Journal rotation prevents unbounded disk growth by archiving the active journal as a sealed segment and starting a fresh one. **Rotation runs while the engine is live** — no restart required.

## File Layout

The journal is a sequence of segments. The currently-written segment lives at the bare path; archived segments — produced by previous rotations — are named with a six-digit zero-padded monotonic suffix.

```
melin.journal              Live segment (active writes)
melin.snapshot             Latest shadow snapshot
melin.snapshot.prev        Penultimate snapshot (rollback target)
melin.journal.000001       First archived segment
melin.journal.000002       Second archived segment
...
```

Archive numbers are assigned in rotation order: `000001` is the oldest archive, the highest-numbered file is the most recent. Rotation never renames an existing archive — each rotation is a single rename of the live file to the next free number.

## Rotation Triggers

Two independent triggers fire rotation, both observed at the journal stage's fsync boundary so the live segment is durably committed before the rename:

### Size threshold

When `--max-journal-mib` is non-zero (default: 256 MiB) and the live segment crosses that size after an fsync batch, the journal stage rotates immediately. Tune this larger to reduce rotation frequency (and the brief stall described below) at the cost of more bytes per archive segment.

### Operator-driven (`ROTATE` admin command)

When `--admin-bind <addr>` is set, the server listens on that TCP address for operator commands (the same endpoint that accepts `PROMOTE` for replica → primary failover). An operator authenticates with an Ed25519 operator key (challenge-response, same scheme as all other admin handshakes) and sends `ROTATE\n`. The journal stage performs one rotation at the next fsync boundary. Concurrent or repeated `ROTATE` commands collapse into a single rotation rather than queueing.

This command is accepted on both primary and replica nodes; each side rotates its own local segments independently.

## Recovery: Multi-Segment Walk

Recovery walks every archived segment in monotonic order, then the live segment, replaying events on top of the recovered state.

```
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐   ┌──────────────┐
│ .journal.000001 │ → │ .journal.000002 │ → │ .journal.000003 │ → │ .journal     │
│ (sealed)        │   │ (sealed)        │   │ (sealed)        │   │ (live, may   │
│                 │   │                 │   │                 │   │  have torn   │
│                 │   │                 │   │                 │   │  tail)       │
└─────────────────┘   └─────────────────┘   └─────────────────┘   └──────────────┘
```

When a snapshot is available, recovery loads the snapshot first, then walks segments — skipping events with sequences less than or equal to the snapshot's recorded sequence. Segments fully covered by the snapshot are walked but not replayed; their per-segment hash chain is still verified for tamper-evidence.

### Sequence continuity

Each segment's first entry is a `GenesisHash` whose payload is the previous segment's final chain hash, anchoring the new segment to the chain it continues. Sequence numbers are monotonic across segment boundaries — no gaps.

### Cross-segment tamper-evidence

Recovery verifies that each segment's `GenesisHash` payload equals the previous segment's tail chain hash. A mismatch indicates either deliberate tampering with an archived segment or a missing segment between two surviving archives, and aborts recovery with an error identifying the boundary.

## Sequence Numbering

Journal entries carry monotonically increasing sequence numbers. After rotation, the new segment continues from where the old one left off:

```
Segment 000001:   seq 1, 2, 3, ..., 1000
                  (chain hash at end: H1)
Segment 000002:   seq 1001 = GenesisHash(H1), then 1002, 1003, ..., 2500
                  (chain hash at end: H2)
Live segment:     seq 2501 = GenesisHash(H2), then 2502, 2503, ...
```

A snapshot records both the sequence of the last event it captured and the chain hash at that point. Recovery from snapshot + segments skips events with `sequence <= snap_sequence`.

## Recovery Scenarios

### 1. Snapshot + segments + live segment all exist

**Recovery flow:**
1. Load snapshot (restores state as of sequence N)
2. Walk archived segments in order; skip events with `sequence <= N`, replay the rest
3. Verify each segment-boundary `GenesisHash` matches the previous tail
4. Walk the live segment; truncate at the last valid entry on torn tails
5. Open the live segment for append

**When:** Steady state on any restart.

### 2. Live segment missing, archives present (with or without snapshot)

**Recovery flow:** Walk archives in monotonic order, replaying events past the snapshot's sequence (when present). After the last archive, synthesize a fresh live segment continuing from the last archive's tail — its `GenesisHash` payload is the previous segment's final chain hash so the chain stays continuous. The next event written gets `last_archived_seq + 2` (the GenesisHash itself takes `+1`).

**When:** Crash between the live → archive rename and the new live file's creation. The just-archived segment captured every event acknowledged before the crash, so no committed event is lost. This used to require a recent snapshot to recover correctly; multi-segment recovery removed that requirement — operators no longer need to time their snapshots to the rotation cadence.

If no archives exist *and* no live segment exists, recovery falls through to the snapshot-only path handled separately by the server bootstrap (which has the snapshot's chain hash and starting sequence available).

### 3. Live segment exists, no snapshot, may have archives

**Recovery flow:** Walk archives (from segment 000001 onward), then the live segment. Replay everything from genesis.

**When:** First startup after the initial run, or the snapshot file was manually deleted.

### 4. Neither snapshot nor any journal file exists

**Recovery flow:** Create a fresh live journal at sequence 1; seed test instruments and accounts (configurable via `--accounts` / `--instruments`).

**When:** First-ever startup.

### 5. Crash during snapshot write

The snapshot is written atomically: data goes to `melin.snapshot.tmp`, is synced to disk, then renamed to `melin.snapshot`. If the server crashes during the write, the `.tmp` file contains partial data but the rename never happens. On restart, the previous `melin.snapshot` is still valid (or `.snapshot.prev` if the rename happened but the next save failed).

### 6. Crash during normal operation

The live segment may have a partially written final entry (torn write). The reader validates each entry's CRC32C checksum and treats a truncated or corrupt final entry as end-of-data; the writer truncates to the last valid entry on `open_append`. At most the in-flight event is lost — and since the response stage waits for journal durability before acknowledging to the client, the client never received confirmation for that event.

### 7. Crash mid-rotation

If the crash occurs after the live → archive rename but before the new live file is created, recovery follows scenario #2 above. If the crash occurs during the new file's open, the just-archived segment is intact and recovery walks it as the most recent archive; the live file is then created from scratch using the latest snapshot.

## Operational Impact

### Stall duration

Rotation pauses the journal stage for a single rename + open + sync operation — typically tens of microseconds on NVMe. Upstream stages briefly backpressure on the input ring during that window. The shadow snapshot stage is **not** synchronously involved: snapshot capture runs on its own cadence (`--snapshot-interval-ms`), and recovery's multi-segment walk means events written between the last snapshot and the rotation remain replayable from the just-archived segment.

This is materially different from the legacy startup-only rotation, where the snapshot and rename were coupled. Operators with strict tail-latency SLOs should still prefer larger `--max-journal-mib` so rotations are infrequent.

### Audit trail

Archived segments are append-only history and **not deleted automatically**. The full sequence of events from genesis is preserved across all archive files plus the live segment. Operators who need long-term retention should set up a separate workflow (e.g., copy `melin.journal.NNNNNN` to cold storage when their numeric suffix is sufficiently old).

Replays for forensic purposes can use the standalone reader against any archived segment in isolation: the chain hash is self-contained per segment, with cross-segment continuity validated against neighboring segments' GenesisHash payloads.

### Replication

Each node — primary or replica — manages its own journal segments independently. The replica's archive numbering is local and need not match the primary's; only the per-event sequence numbers (assigned by the primary) are common across nodes. Triggering `ROTATE` on the primary does not cause the replica to rotate, and vice versa.

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--journal <path>` | `melin.journal` | Path to the live journal segment. Archives use the same prefix with a `.NNNNNN` suffix. |
| `--snapshot <path>` | (derived: `<journal>.snapshot`) | Explicit snapshot path. If unset, the server uses `<journal-path-with-.snapshot-extension>`. |
| `--max-journal-mib <N>` | `256` | Rotate when the live segment exceeds N MiB at the next fsync boundary. Set to `0` to disable size-driven rotation. |
| `--admin-bind <addr>` | (unset) | TCP address for the operator admin endpoint. Authenticated with operator keys; accepts `ROTATE\n` (any node) and `PROMOTE\n` (replica nodes). |
| `--snapshot-interval-ms <ms>` | `3_000_000` | Cadence of background shadow snapshots. Snapshots are independent of rotation but recovery uses the most recent one. |

## Operational Notes

- **Archived segments are kept indefinitely.** Set up retention separately if disk usage matters.
- **`ROTATE` does not block on the snapshot stage.** The admin command sets a flag and returns; the actual rotation happens at the next fsync boundary (typically within a few milliseconds under load).
- **Both primary and replica honour `--admin-bind`.** Each node rotates locally. To rotate both ends of a 1+1 deployment, send `ROTATE` to each address.
- **Snapshot + segments must be on the same filesystem** for the atomic rename to work. Cross-filesystem `--snapshot` paths fall back to a copy that is not crash-safe.
- **Legacy `.1`, `.2` archives** from pre-monotonic builds are still discoverable by recovery — they are visible in the archive listing for forensic replay, though new rotations always use the monotonic naming scheme.
