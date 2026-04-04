# Journal Rotation & Recovery

This document describes the journaling policy, rotation mechanism, and every recovery scenario the server handles at startup.

## Overview

The trading engine uses a write-ahead journal for event sourcing and crash recovery. Every input command (order submit, cancel, deposit, etc.) is journaled before acknowledgement. The matching engine is deterministic, so replaying the journal from genesis reproduces the exact same state.

Snapshots capture the full exchange state at a known journal sequence boundary. Recovery from a snapshot skips replaying all events before that boundary, reducing startup time from O(total events) to O(events since snapshot).

Journal rotation prevents unbounded disk growth by periodically snapshotting and starting a fresh journal file.

## File Layout

```
melin.journal          Current journal (active writes)
melin.snapshot         Latest snapshot (from most recent rotation)
melin.journal.1        Previous journal (archived by last rotation)
melin.journal.2        Journal before that (archived by rotation before last)
...
```

## Rotation Trigger

Rotation happens **at server startup**, not during live operation. When `--max-journal-mib` is set (default: 256 MiB) and the journal exceeds that threshold after recovery, the server:

1. Saves a snapshot at the current sequence boundary
2. Renames `melin.journal` to `melin.journal.1` (bumping existing archives: `.1` -> `.2`, `.2` -> `.3`, etc.)
3. Creates a new `melin.journal` continuing from the next sequence number

The server then proceeds to start the pipeline with the fresh journal.

## Sequence Numbering

Journal entries carry monotonically increasing sequence numbers. After rotation, the new journal continues from where the old one left off. A snapshot records the sequence of the last event it captured.

```
Old journal:  seq 1, 2, 3, ..., 1000
Snapshot:     captured at seq 1000
New journal:  seq 1001, 1002, ...
```

Recovery from snapshot + new journal skips all entries with sequence <= the snapshot sequence.

## Recovery Scenarios

The server evaluates recovery mode at startup in this priority order:

### 1. Snapshot + journal both exist

**Files present:** `melin.snapshot`, `melin.journal`

**Recovery flow:**
1. Load snapshot (restores state as of sequence N)
2. Open journal, read all entries
3. Skip entries where `sequence <= N`
4. Replay entries where `sequence > N`
5. Truncate journal to last valid entry (crash recovery)
6. Open journal for append, continuing from last sequence + 1

**When this happens:** Normal restart after rotation. The snapshot captures pre-rotation state, the journal has post-rotation events.

### 2. Snapshot exists, journal missing

**Files present:** `melin.snapshot` (no `melin.journal`)

**Recovery flow:**
1. Load snapshot (restores state as of sequence N)
2. Create a new journal starting at sequence N + 1

**When this happens:** Crash between `rotate_file()` (old journal renamed to `.1`) and `create_continuing()` (new journal not yet created). The snapshot is complete since it was written and synced before the rename. No data loss — the snapshot captures all events from the old journal.

### 3. Journal exists, no snapshot

**Files present:** `melin.journal` (no `melin.snapshot`)

**Recovery flow:**
1. Open journal, replay all entries from sequence 1
2. Truncate to last valid entry
3. Open for append

**When this happens:** First startup after the initial run (no rotation has occurred yet), or if the snapshot file was manually deleted.

### 4. Neither exists

**Files present:** (none)

**Recovery flow:**
1. Create a new journal starting at sequence 1
2. Seed test instruments and accounts (configurable via `--accounts` / `--instruments`)

**When this happens:** First-ever startup.

### 5. Crash during snapshot write

The snapshot is written atomically: data goes to `melin.snapshot.tmp`, is synced to disk, then renamed to `melin.snapshot`. If the server crashes during the write, the `.tmp` file contains partial data but the rename never happens. On restart:

- If a previous `melin.snapshot` exists, it is used (still valid from the prior rotation)
- If no previous snapshot exists, falls through to journal-only recovery (#3)

No data loss in either case.

### 6. Crash during normal operation (no rotation in progress)

The journal may have a partially written final entry (torn write). On recovery:

- The journal reader validates each entry's CRC32C checksum
- A truncated or corrupt final entry is silently discarded
- `valid_file_end` points to the last complete entry
- `open_append` truncates the file to `valid_file_end` and continues

At most one event is lost (the one being written at crash time). Since the response stage waits for journal durability before acknowledging to the client, the client never received confirmation for that event. From the client's perspective, the event was never processed.

### 7. Crash with pre-allocated space

The journal pre-allocates 64 MiB chunks via `posix_fallocate()` to avoid extent allocation on the write path. Pre-allocated space is zero-filled. The reader detects zero magic bytes (`0x0000` where the entry magic `0x4A45` is expected) and treats them as end-of-data, not corruption.

## Configuration

| Flag | Default | Description |
|---|---|---|
| `--journal <path>` | `melin.journal` | Path to the active journal file |
| `--snapshot <path>` | (derived: `<journal>.snapshot`) | Explicit snapshot path. If not set, the server checks `<journal-path-with-.snapshot-extension>` |
| `--max-journal-mib <N>` | `256` | Rotate when journal exceeds N MiB at startup. Set to 0 to disable |

## Operational Notes

- **Archived journals are not automatically deleted.** The `.1`, `.2`, etc. files are kept for audit trail purposes. Operators should set up a retention policy (e.g., cron job to delete archives older than 30 days).
- **Rotation only happens at startup.** During a long-running session, the journal grows without bound. Restart the server to trigger rotation. Live rotation during operation is not yet supported (requires pausing the pipeline to take a consistent snapshot).
- **The snapshot file can be large.** It captures all account balances, resting orders, reservations, risk limits, and circuit breaker state. For a busy exchange, this can be tens of MiB.
- **Manual rotation** is possible by stopping the server, running a tool that calls `JournaledExchange::rotate()`, and restarting. The server will detect the snapshot and use it.
- **Snapshot + journal must be on the same filesystem** for the atomic rename to work. If `--snapshot` points to a different filesystem, the atomic write falls back to a copy (which is not crash-safe).
