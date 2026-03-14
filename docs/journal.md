# Journal & Event Sourcing

This document describes the write-ahead journal, snapshot system, crash recovery, and version migration procedures.

## Design Principles

1. **Input-only journaling** — only input commands are persisted (order submissions, cancellations, deposits, instrument creation). Execution reports are *not* journaled. The matching engine is deterministic: replaying the same inputs always produces identical outputs. This halves journal size and avoids coupling the journal format to execution report changes.

2. **Persist-before-ack** — no response is sent to a client until the corresponding journal entry is durable on disk. The LMAX disruptor pipeline enforces this: the response stage gates on the journal cursor, which advances only after `pwritev2 + RWF_DSYNC` completes.

3. **Manual binary codec** — no serde, no protobuf. Every field is encoded by hand in little-endian with known offsets. This gives predictable layout, zero allocations, and immunity to serialization library version changes.

4. **CRC32C integrity** — every journal entry and every snapshot file is checksummed with CRC32C (hardware-accelerated on x86). Corruption is detected on read, never silently replayed.

## Journal File Format

### File Header (8 bytes, written once)

```
Offset  Size  Field           Value
0       4     file_magic      0x4A4F5552 ("JOUR")
4       2     format_version  2
6       2     reserved        0
```

The header is written when the journal is created and never modified. `format_version` is checked on open; mismatches are rejected with `UnsupportedVersion`.

### Entry Layout (repeats after header)

```
Offset  Size  Field           Description
0       2     entry_magic     0x4A45 — misalignment / corruption detection
2       2     length          byte count of (event_tag + payload), excludes header and CRC
4       8     sequence        monotonically increasing, starts at 1, no gaps
12      8     timestamp_ns    wall-clock nanoseconds since Unix epoch (informational only)
20      1     event_tag       discriminant (1=AddInstrument, 2=Deposit, 3=SubmitOrder, 4=CancelOrder)
21      var   payload         event-specific fields (see below)
21+len  4     crc32c          CRC32C of all preceding bytes in this entry (offset 0 through 20+len)
```

Total entry size: `20 + length + 4` bytes. Typical entries are 40-85 bytes.

### Event Payloads

**AddInstrument (tag=1)** — 12 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |
| 4 | 4 | base_currency (u32) |
| 8 | 4 | quote_currency (u32) |

**Deposit (tag=2)** — 16 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | account_id (u32) |
| 4 | 4 | currency_id (u32) |
| 8 | 8 | amount (u64) |

**CancelOrder (tag=4)** — 12 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |
| 4 | 8 | order_id (u64) |

**SubmitOrder (tag=3)** — 4 + variable (order encoding)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |
| 4 | var | order (see below) |

**Order encoding** (variable length, 24-40 bytes):

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | order_id (u64) |
| 8 | 4 | account_id (u32) |
| 12 | 1 | side (0=Buy, 1=Sell) |
| 13 | 1 | order_type_tag (0=Market, 1=Limit, 2=Stop, 3=StopLimit) |
| 14 | var | order_type_fields — Market: 0 bytes; Limit: 8 (price); Stop: 8 (trigger); StopLimit: 16 (trigger + price) |
| 14+N | 1 | time_in_force (0=GTC, 1=IOC, 2=FOK) |
| 15+N | 8 | quantity (u64) |
| 23+N | 1 | self_trade_prevention (0=Allow, 1=CancelNewest, 2=CancelOldest, 3=CancelBoth) |

## Durability

### Write Path

The journal uses `pwritev2` with the `RWF_DSYNC` flag (Force Unit Access). On NVMe drives that support FUA, this persists the written sectors directly to non-volatile media in a single syscall, without flushing the entire drive write cache. Latency: ~10-100 us per write (vs ~1-7 ms for `write` + `fdatasync`).

### Pre-allocation

On creation and when space runs low, the writer calls `posix_fallocate` to extend the file by 64 MiB. This pre-allocates disk extents (blocks) without writing zeros. Subsequent syncs only flush data pages — no extent metadata updates are needed, which would otherwise require a more expensive metadata sync.

### Batch Amortization

In the pipeline architecture, the journal stage reads a batch of events from the disruptor, encodes them all into a contiguous buffer, and issues a single `pwritev2 + RWF_DSYNC` for the batch. Under load, one sync covers many events. The disruptor naturally accumulates events while the previous sync is in flight, providing implicit batching without any artificial delay.

An explicit group commit delay (`group_commit_delay`) can be configured but is set to zero for TCP. Testing showed that any delay hurts TCP throughput because it holds the journal cursor longer, stalling the response stage. It only helps with UDS transport where response sends are near-free.

## Crash Recovery

### What Can Go Wrong

1. **Clean shutdown** — all entries are complete and synced. No recovery needed.
2. **Crash mid-write** — the last entry may be partially written (truncated). The entry magic, length, or CRC will be invalid.
3. **Crash after write, before sync** — the kernel or drive may have reordered writes. With FUA, this should not happen for the written data, but pre-allocated zero-filled space beyond the last write is always present.
4. **Bit rot / storage corruption** — a previously valid entry has flipped bits. CRC32C detects this.

### Recovery Algorithm

```
JournaledExchange::recover(journal_path):
  1. Open journal file, validate file header (magic + version).
  2. Read entries sequentially:
     - Validate entry_magic (0x4A45).
     - Validate CRC32C.
     - Validate sequence continuity (expected = last + 1).
     - If entry_magic is 0x0000 → end of data (pre-allocated space). Stop.
     - If entry is truncated at EOF → partial write from crash. Stop.
     - If CRC mismatch or sequence gap → return error (real corruption).
  3. For each valid entry, replay the event on a fresh Exchange instance.
  4. Truncate the file to valid_file_end (remove trailing garbage).
  5. Re-allocate space from valid_file_end forward.
  6. Reopen writer for appending at next_sequence = last_sequence + 1.
```

**Key behaviors:**

- A truncated final entry is treated as harmless (crash during write) and silently discarded. The events it contained were never acknowledged to the client (persist-before-ack), so no client believes they succeeded.
- Zero-filled bytes (from `posix_fallocate`) are treated as end-of-data, not corruption.
- A CRC mismatch or sequence gap mid-stream is treated as real corruption and returns an error. The operator must investigate — this should never happen under normal operation.

### Recovery with Snapshots

```
JournaledExchange::recover_from_snapshot(snapshot_path, journal_path):
  1. Load snapshot → (Exchange state, snapshot_sequence).
  2. Open journal, validate header.
  3. Read entries sequentially. Skip all entries with sequence <= snapshot_sequence.
  4. Replay only entries after the snapshot.
  5. Truncate and reopen writer as above.
```

This avoids replaying the entire journal from genesis. Recovery time is proportional to the journal tail length (events since last snapshot), not total history.

## Snapshots

### File Format

```
Offset  Size  Field           Value
0       4     file_magic      0x534E4150 ("SNAP")
4       2     format_version  2
6       2     reserved        0
8       8     sequence        journal sequence number at snapshot time
16      var   data            serialized Exchange state (manual binary encoding)
EOF-4   4     crc32c          CRC32C of everything from offset 0 through EOF-4
```

Maximum file size enforced on load: 256 MiB (prevents OOM from corrupt/malicious files).

### Snapshot Contents

The snapshot captures the full Exchange state:

- **Instruments** — all registered trading pairs (symbol, base currency, quote currency)
- **Account balances** — per-account, per-currency available and reserved amounts
- **Balance reservations** — per-order reserved funds (order_id → account, currency, amount)
- **Order sides** — per-order buy/sell tracking (for fill processing)
- **Order books** — per-instrument:
  - Resting bids and asks at each price level (order_id, account, remaining quantity)
  - Order index (order_id → side, price) for O(1) cancel
  - Pending stop orders (buy and sell, with trigger price, quantity, TIF, limit price, quote budget, STP mode)
  - Stop index (order_id → side, trigger price)
  - Last trade price (for stop trigger evaluation)

### Atomic Writes

Snapshots are written atomically:

1. Serialize entire state into an in-memory buffer.
2. Write to a temporary file.
3. `fdatasync` the temporary file.
4. `rename` the temporary file to the final path (atomic on POSIX).

A crash during snapshot creation leaves only a temporary file, which is harmless. The previous snapshot (if any) remains intact.

### When to Snapshot

Snapshots are currently taken manually (via `save_snapshot()`). In production, they should be triggered:

- Before deploying a new engine version (version boundary — see Migration below).
- Periodically, to bound recovery time (e.g., every N million events or every M minutes).
- Before journal rotation (the snapshot makes the old journal dispensable for recovery).

Automatic snapshot triggering (journal compaction) is not yet implemented.

## Pipeline Architecture

The journal participates in a 3-stage LMAX disruptor pipeline:

```
            Input Disruptor (1M slots, lock-free ring buffer)
                    │
        ┌───────────┴───────────┐
        │                       │
   Journal Stage           Matching Stage
   (encode + sync)         (execute on Exchange)
   advances cursor ──┐     publishes to output SPSC
                     │           │
                     ▼           │
               Response Stage ◄──┘
               gates on journal cursor
               sends responses to clients
```

- **Journal and Matching run in parallel** on the same events. Matching does not wait for the journal — it executes immediately. This overlaps matching latency with journal I/O latency.
- **Response Stage gates on the journal cursor** — it will not send a response to the client until the journal stage has committed (synced) that event's sequence number. This enforces persist-before-ack without blocking the matching engine.
- **Input ring capacity**: 1,048,576 slots (~72 bytes each, ~72 MiB). At 10M orders/sec, this provides ~100 ms of buffering.
- **Max journal batch**: 1,024 events per sync. Limits encoding time before the sync call, bounding worst-case latency.

### Feature Gates

| Feature | Effect |
|---------|--------|
| `no-persist` | Journal stage skips all I/O. Cursor advances immediately. For benchmarking the pipeline + network ceiling. |
| `no-fsync` | Journal stage writes but does not sync. Data may be lost on crash. For testing only. |
| `pipeline-stats` | Prints per-stage busy/idle utilization percentages on shutdown. |
| `latency-trace` | Records per-event, per-stage latency in histograms (adds timestamp fields to slots). |

## Format Versioning

Both the journal and snapshot have independent `format_version` fields. The current version for both is **2**.

### Version History

| Version | Change |
|---------|--------|
| 1 | Initial format |
| 2 | Added `SelfTradeProtection` byte to Order encoding (journal) and `PendingStopSnapshot` (snapshot) |

### Compatibility Rules

- The reader rejects any file whose `format_version` does not match the compiled-in constant. There is no backwards-compatible reading of old versions.
- This is intentional: the journal is an internal persistence format, not an interchange format. Forward/backward compatibility adds complexity and is unnecessary when the operator controls both the writer and reader.

## Migration Procedure

When changing the journal or snapshot format (adding fields, changing encoding, bumping `format_version`):

### Standard Upgrade

1. **Take a snapshot** with the current (old) engine version. This captures the full Exchange state at a known journal sequence.
2. **Deploy the new engine version** with the bumped `format_version`.
3. **Start fresh**: the new engine creates a new journal file (new format) and loads the snapshot.
   - The snapshot must also be at the new version. If the snapshot format changed, the old engine must produce a snapshot in the new format, or a one-time migration tool must convert it.
4. **Archive the old journal** for audit purposes. It can only be replayed by the old engine version.

### Why This Works

- The snapshot captures the Exchange state completely — no journal replay needed for entries before the snapshot.
- The new journal starts from the snapshot sequence + 1, in the new format.
- Deterministic replay is preserved within each format version. Cross-version replay is not supported and not needed.

### What NOT to Do

- **Do not replay old-format journals with new-format code.** The reader will reject the version mismatch. Even if you bypassed the check, field layout differences would produce corrupt state.
- **Do not mix format versions in a single journal file.** Each file has one format version in its header.

### Adding New Event Types

Adding a new `JournalEvent` variant (e.g., `SetPriceBands`, `HaltInstrument`) requires:

1. Assign a new event tag (e.g., `TAG_SET_PRICE_BANDS = 5`).
2. Add encode/decode logic to the codec.
3. Bump `FORMAT_VERSION` (old readers will reject the new file, which is correct).
4. Follow the standard upgrade procedure above.

If the new event type is purely additive (old events unchanged, new tag added), it is possible to keep the same `FORMAT_VERSION` — old readers will fail with `CorruptEntry` on the unknown tag, which is the same outcome as a version mismatch. Bumping the version is preferred because it fails fast at file open rather than mid-replay.

### Adding Fields to Existing Events

Adding a field to an existing event (as was done in v1→v2 with `SelfTradeProtection`):

1. Append the field to the end of the event's binary layout.
2. Bump `FORMAT_VERSION`.
3. Follow the standard upgrade procedure.

Inserting a field in the middle or changing field sizes breaks all entries in the file. Appending is always safe and preferred.

## Operational Notes

### Journal File Growth

The journal grows monotonically. Pre-allocation extends it in 64 MiB chunks. A single entry is ~40-85 bytes, so 64 MiB covers roughly 800K-1.6M entries before the next allocation.

At sustained 830K orders/sec (with fsync), the journal grows at ~50-70 MB/sec. Without rotation or compaction, it will consume ~4 GB/min. **Journal rotation and compaction are not yet implemented** — monitor disk usage.

### Sequence Numbers

Sequences are `u64`, starting at 1, monotonically increasing, with no gaps. At 10M events/sec, the sequence space lasts ~58,000 years before wrapping. Wrapping is not handled (nor needed).

### Timestamps

The `timestamp_ns` field is wall-clock time from `clock_gettime(CLOCK_REALTIME)`. It is informational only — the journal never uses it for ordering or replay decisions. If the system clock jumps (NTP correction, daylight saving), timestamps may be non-monotonic. This is harmless.

### Error Handling

| Error | Cause | Action |
|-------|-------|--------|
| `InvalidFile` | Bad magic bytes | Wrong file, not a journal |
| `UnsupportedVersion` | Format version mismatch | Need matching engine version (see Migration) |
| `CorruptEntry` | Unknown tag, invalid field | Real corruption — investigate storage |
| `ChecksumMismatch` | CRC32C validation failed | Bit rot or partial write — investigate storage |
| `SequenceGap` | Non-contiguous sequence numbers | Corruption or file truncation — investigate |
| `TruncatedEntry` | Incomplete entry at EOF | Normal crash recovery — entry is discarded |
| `Io` | Underlying I/O error | Disk failure, permissions, full disk |

### Limitations

- **No journal rotation** — single file grows unbounded. Requires manual management.
- **No compaction** — old entries are never removed. Snapshot + rotate is the planned approach.
- **No client deduplication** — after crash recovery, clients that retry may cause duplicate executions. Sequence numbers / idempotency keys are planned.
- **No output event log** — execution reports are not persisted. Audit trail requires replaying the journal.
- **Single journal file** — no striping or parallel writes. The journal is single-threaded by design (LMAX architecture).
- **No encryption** — journal and snapshot files are plaintext binary. Sensitive data (account IDs, order details) is visible to anyone with file access.
