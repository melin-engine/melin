# Journal & Event Sourcing

This document describes the write-ahead journal, snapshot system, crash recovery, and version migration procedures.

## Design Principles

1. **Input-only journaling** — only input commands are persisted (order submissions, cancellations, deposits, instrument creation). Execution reports are *not* journaled. The matching engine is deterministic: replaying the same inputs always produces identical outputs. This halves journal size and avoids coupling the journal format to execution report changes.

2. **Persist-before-ack** — no response is sent to a client until the corresponding journal entry is durable on disk. The LMAX disruptor pipeline enforces this: the response stage gates on the journal cursor, which advances only after `pwritev2 + RWF_DSYNC` completes.

3. **Manual binary codec** — no serde, no protobuf. Every field is encoded by hand in little-endian with known offsets. This gives predictable layout, zero allocations, and immunity to serialization library version changes.

4. **CRC32C integrity** — every journal entry and every snapshot file is checksummed with CRC32C (hardware-accelerated on x86). Corruption is detected on read, never silently replayed.

5. **BLAKE3 hash chain** — each entry is hashed into a running BLAKE3 chain (`hash_n = BLAKE3(entry_bytes || hash_{n-1})`). Periodic checkpoint entries record the chain hash, enabling tamper detection and replica consistency verification without per-entry disk overhead.

## Journal File Format

### File Header (8 bytes, written once)

```
Offset  Size  Field           Value
0       4     file_magic      0x4A4F5552 ("JOUR")
4       2     format_version  9
6       2     reserved        0
```

The header is written when the journal is created and never modified. `format_version` is checked on open; v5, v7, v8, and v9 are accepted (v5 journals lack hash chain verification; v7-v8 lack newer event types).

### Entry Layout (repeats after header)

```
Offset  Size  Field           Description
0       2     entry_magic     0x4A45 — misalignment / corruption detection
2       2     length          byte count of (event_tag + payload), excludes header and CRC
4       8     sequence        monotonically increasing, starts at 1, no gaps
12      8     timestamp_ns    wall-clock nanoseconds since Unix epoch (informational only)
20      1     event_tag       discriminant (see Event Payloads below)
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

**Order encoding** (variable length, 24-48 bytes):

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | order_id (u64) |
| 8 | 4 | account_id (u32) |
| 12 | 1 | side (0=Buy, 1=Sell) |
| 13 | 1 | order_type_tag (0=Market, 1=Limit, 2=Stop, 3=StopLimit, 4=LimitPostOnly) |
| 14 | var | order_type_fields — Market: 0 bytes; Limit/PostOnly: 8 (price); Stop: 8 (trigger); StopLimit: 16 (trigger + price) |
| 14+N | 1 | time_in_force (0=GTC, 1=IOC, 2=FOK, 3=Day, 4=GTD) |
| 15+N | 8 | quantity (u64) |
| 23+N | 1 | self_trade_prevention (0=Allow, 1=CancelNewest, 2=CancelOldest, 3=CancelBoth) |
| 24+N | 0 or 8 | expiry_ns (u64, only present when tif=GTD) |

**SetRiskLimits (tag=5)** — 6-22 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |
| 4 | 1 | max_order_qty option tag (0=None, 1=Some) |
| 5 | 0 or 8 | max_order_qty value (u64, if Some) |
| 5+N | 1 | max_order_notional option tag |
| 6+N | 0 or 8 | max_order_notional value (u64, if Some) |

**CancelAll (tag=6)** — 4 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | account_id (u32) |

**SetCircuitBreaker (tag=7)** — 7-23 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |
| 4 | 1 | price_band_lower option tag (0=None, 1=Some) |
| 5 | 0 or 8 | price_band_lower value (u64, if Some) |
| 5+N | 1 | price_band_upper option tag |
| 6+N | 0 or 8 | price_band_upper value (u64, if Some) |
| 6+N+M | 1 | halted (0=false, 1=true) |

**CancelReplace (tag=8)** — 28 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |
| 4 | 8 | order_id (u64) |
| 12 | 8 | new_price (u64, NonZero) |
| 20 | 8 | new_quantity (u64, NonZero) |

**GenesisHash (tag=9)** — 32 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 32 | hash ([u8; 32]) — random bytes (fresh journal) or previous chain hash (rotation) |

First entry in every v6 journal. Seeds the BLAKE3 hash chain. Transparent to callers (reader skips it and only returns user events).

**Checkpoint (tag=10)** — 40 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 32 | chain_hash ([u8; 32]) — running BLAKE3 hash at this point |
| 32 | 8 | events_since_checkpoint (u64) |

Auto-emitted every 100K events. The reader verifies the chain hash and event count; mismatches produce `HashChainMismatch`. Transparent to callers.

**SetFeeSchedule (tag=11)** — 8 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |
| 4 | 2 | maker_fee_bps (i16) |
| 6 | 2 | taker_fee_bps (i16) |

**ProvisionAccount (tag=12)** — 12 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | account_id (u32) |
| 4 | 8 | amount (u64) |

Internal-only: bulk seeding event. Not exposed via the wire protocol.

**Withdraw (tag=13)** — 16 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | account_id (u32) |
| 4 | 4 | currency_id (u32) |
| 8 | 8 | amount (u64) |

**EndOfDay (tag=14)** — 0 bytes

Cancels all Day TIF orders across all instruments.

**ExpireOrders (tag=15)** — 8 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | timestamp_ns (u64) |

Expires all GTD orders with `expiry_ns <= timestamp_ns`.

**DisableInstrument (tag=16)** — 4 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |

**EnableInstrument (tag=17)** — 4 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |

**RemoveInstrument (tag=18)** — 4 bytes

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | symbol (u32) |

## Durability

### Write Path

The journal stage offers two write paths, selected at startup via `--journal-writer`:

- **`buffered`** *(default, production)* — `pwrite` plus `fdatasync` per batch. Honest durability on any drive: `fdatasync` flushes the page cache to the drive and waits for the drive to acknowledge a flush of its own write cache. Latency: ~10–30 µs per batch on PLP NVMe, ~50–200 µs on consumer NVMe.
- **`sector`** *(experimental)* — `pwrite` with `O_DIRECT`, no `fdatasync`. Bypasses the page cache and skips the device-level flush command. Durability depends entirely on the drive having capacitor-backed Power Loss Protection (PLP) with the volatile write cache disabled (`VWC=0`). Latency: ~5–15 µs per batch. **Silently loses acknowledged writes on power loss without PLP**, and shows unresolved ~1 Hz tail-latency spikes on some NVMe firmware. Not recommended for production.

See [Journal Writer Modes](journal-writer-modes.md) for the full operator decision guide, PLP verification commands, and migration procedure.

### Pre-allocation

On creation and when space runs low, the writer calls `posix_fallocate` to extend the file by 256 MiB. This pre-allocates disk extents (blocks) without writing zeros. Subsequent syncs only flush data pages — no extent metadata updates are needed, which would otherwise require a more expensive metadata sync.

### Batch Amortization

In the pipeline architecture, the journal stage reads a batch of events from the disruptor, encodes them all into a contiguous buffer, and issues a single `pwrite` for the batch. Under load, one write covers many events. The disruptor naturally accumulates events while the previous write is in flight, providing implicit batching without any artificial delay.

An explicit group commit delay (`group_commit_delay`) can be configured but is set to zero for TCP. Testing showed that any delay hurts TCP throughput because it holds the journal cursor longer, stalling the response stage. It only helps with UDS transport where response sends are near-free.

## Crash Recovery

### What Can Go Wrong

1. **Clean shutdown** — all entries are complete and synced. No recovery needed.
2. **Crash mid-write** — the last entry may be partially written (truncated). The entry magic, length, or CRC will be invalid.
3. **Crash after write** — pre-allocated zero-filled space beyond the last write is always present; PLP ensures the written data itself is durable.
4. **Bit rot / storage corruption** — a previously valid entry has flipped bits. CRC32C detects this.

### Recovery Algorithm

```
JournaledExchange::recover(journal_path):
  1. Open journal file, validate file header (magic + version).
  2. Read entries sequentially:
     - Validate entry_magic (0x4A45).
     - Validate CRC32C.
     - Validate sequence continuity (expected = last + 1).
     - On GenesisHash: initialize BLAKE3 hash chain, skip (transparent).
     - On Checkpoint: verify chain hash + event count, skip (transparent).
     - On normal entries: update hash chain, replay on Exchange.
     - If entry_magic is 0x0000 → end of data (pre-allocated space). Stop.
     - If entry is truncated at EOF → partial write from crash. Stop.
     - If CRC mismatch, sequence gap, or hash chain mismatch → return error.
  3. Truncate the file to valid_file_end (remove trailing garbage).
  4. Re-allocate space from valid_file_end forward.
  5. Reopen writer for appending, resuming the hash chain from reader's final state.
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
4       2     format_version  12
6       2     reserved        0
8       8     sequence        journal sequence number at snapshot time
16      32    chain_hash      BLAKE3 hash chain state (v6+; zeros for v5)
48      var   data            serialized Exchange state (manual binary encoding)
EOF-4   4     crc32c          CRC32C of everything from offset 0 through EOF-4
```

Maximum file size enforced on load: 256 MiB (prevents OOM from corrupt/malicious files).

### Snapshot Contents

The snapshot captures the full Exchange state:

- **Instruments** — all registered trading pairs (symbol, base currency, quote currency)
- **Account balances** — per-account, per-currency available and reserved amounts
- **Balance reservations** — per-order reserved funds (order_id → account, currency, amount)
- **Order sides** — per-order buy/sell tracking, keyed by (account_id, order_id)
- **Fee schedules** — per-instrument maker/taker fee rates in basis points
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

Snapshots are written exclusively by the shadow exchange on a configurable interval (`--snapshot-interval-ms`, default 50 minutes). The shadow runs as a dedicated consumer on the input ring, replays events through its own copy of the engine state, and writes a snapshot every interval — entirely off the matching thread.

Journal segment rotation is independent of snapshots. When the live journal exceeds `--max-journal-mib` (default 256 MiB), the segment is archived and a fresh live file opens; no snapshot is written at rotation. Recovery walks the archive chain forward from the latest shadow snapshot.

- **On interval** — shadow exchange writes snapshots every `--snapshot-interval-ms`.
- **Before deploying a new engine version** — version boundary (see Migration below).

## BLAKE3 Hash Chain

Every v6 journal maintains a running BLAKE3 hash chain for tamper evidence and replica consistency verification.

### How It Works

1. **Genesis entry** — the first entry in every v6 journal. Contains 32 random bytes (fresh journal) or the previous chain hash (rotated journal). Initializes the chain: `hash_0 = BLAKE3(genesis_entry_bytes)`.

2. **Normal entries** — each entry updates the chain: `hash_n = BLAKE3(entry_bytes_excl_CRC || hash_{n-1})`. The hash is computed over the raw encoded bytes (header + tag + payload) so it covers sequence, timestamp, and payload. Computed in-memory only — no extra disk I/O per entry (~15-30ns).

3. **Checkpoint entries** — auto-emitted every 100K events. Contains the current chain hash and event count. The reader verifies both at each checkpoint; mismatches produce `HashChainMismatch`. The checkpoint itself is hashed into the chain for continuity.

4. **Rotation continuity** — on journal rotation, the new journal's genesis hash is the old journal's final chain hash. This provides cryptographic linkage across rotation boundaries.

5. **Snapshot integration** — snapshots store the chain hash (v6+ header). Recovery from snapshot seeds the chain so verification continues without replaying from genesis.

### What It Detects

- **Tampered entries** — modifying any byte in any entry breaks the chain at the next checkpoint.
- **Reordered entries** — entries hashed in a different order produce a different chain hash.
- **Replica divergence** — a replica replaying events can compare checkpoint hashes to prove it processed the same events in the same order.

### What It Does NOT Detect

- **Tamper between the last checkpoint and EOF** — the chain diverges but there's no subsequent checkpoint to catch it. A final checkpoint on shutdown would close this gap.
- **Truncation attacks** — removing entries from the end produces a valid (shorter) chain. Sequence numbers detect this if the expected sequence is known.

## Journal Rotation

When the journal file exceeds the configured size threshold (`--max-journal-mib`, default 256 MiB), rotation triggers at startup:

1. **Save snapshot** at the current sequence boundary (includes chain hash).
2. **Archive old journal** by renaming: `melin.journal` → `melin.journal.1` (bumping existing archives: `.1` → `.2`, etc.).
3. **Create new journal** continuing from the same sequence with a genesis hash = old chain hash.

Recovery from snapshot + new journal produces identical state. Old journals are kept for audit.

## Pipeline Architecture

The journal participates in a 3-stage LMAX disruptor pipeline:

```
            Input Disruptor (1M slots, lock-free ring buffer)
                    │
        ┌───────────┴───────────┐
        │                       │
   Journal Stage           Matching Stage
   (encode + sync)         (execute on Exchange)
   advances cursor ──┐     publishes to output ring
                     │           │
                     ▼           │
               Output Disruptor Ring (multi-consumer)
                     │
            ┌────────┴────────┐
            │                 │
      Response Stage    Event Publisher
      gates on cursor   (optional, --event-bind)
      sends to clients  broadcasts to subscribers
```

- **Journal and Matching run in parallel** on the same events. Matching does not wait for the journal — it executes immediately. This overlaps matching latency with journal I/O latency.
- **Response Stage gates on the journal cursor** — it will not send a response to the client until the journal stage has committed (synced) that event's sequence number. This enforces persist-before-ack without blocking the matching engine.
- **Input ring capacity**: 1,048,576 slots (~72 bytes each, ~72 MiB). At 10M orders/sec, this provides ~100 ms of buffering.
- **Max journal batch**: 1,024 events per sync. Limits encoding time before the sync call, bounding worst-case latency.

### Feature Gates

| Feature | Effect |
|---------|--------|
| `no-persist` | Journal stage skips all I/O. Cursor advances immediately. For benchmarking the pipeline + network ceiling. |
| `pipeline-stats` | Prints per-stage busy/idle utilization percentages on shutdown. |
| `latency-trace` | Records per-event, per-stage latency in histograms (adds timestamp fields to slots). |

## Format Versioning

Both the journal and snapshot have independent `format_version` fields. Current journal version: **9**. Current snapshot version: **12**.

### Journal Version History

| Version | Change |
|---------|--------|
| 1 | Initial format |
| 2 | Added `SelfTradeProtection` byte to Order encoding |
| 3 | Added `SetRiskLimits` event (tag=5) for fat finger checks |
| 4 | Added `CancelAll` event (tag=6) for kill switch |
| 5 | Added `SetCircuitBreaker` event (tag=7) for price bands + trading halts |
| 6 | Added `GenesisHash` (tag=9), `Checkpoint` (tag=10) for BLAKE3 hash chain; `CancelReplace` (tag=8); `SetFeeSchedule` (tag=11) |
| 7 | Added `ProvisionAccount` (tag=12), `Withdraw` (tag=13); signed fees (i16); `CancelOrder` now includes `account_id` |
| 8 | Added `post_only` flag to Limit order type (wire tag=4); `LimitPostOnly` variant |
| 9 | Added `ExpireOrders` (tag=15), `EndOfDay` (tag=14), `DisableInstrument` (tag=16), `EnableInstrument` (tag=17), `RemoveInstrument` (tag=18); conditional `expiry_ns` in Order encoding for GTD; Day and GTD time-in-force variants |

### Snapshot Version History

| Version | Change |
|---------|--------|
| 1 | Initial format |
| 2 | Added `SelfTradeProtection` byte to `PendingStopSnapshot` |
| 3 | Added per-account OrderId high-water marks for client dedup |
| 4 | Added per-instrument `RiskLimits` for fat finger checks |
| 5 | Added per-instrument `CircuitBreakerConfig` for price bands + halts |
| 6 | Added `chain_hash` (32 bytes) in header for BLAKE3 hash chain continuity |
| 7 | Order sides keyed by `(AccountId, OrderId)` instead of `OrderId`; added per-instrument fee schedules |
| 8 | Added `Withdraw` event support; signed fee types (i16/i64); fee collection account |
| 9 | Added `post_only` flag to resting orders and pending stops |
| 10 | Added per-key request sequence HWM for idempotency dedup |
| 11 | Added `InstrumentStatus` per instrument; `order_counts` per account |
| 12 | Added `expiry_ns` to orders (GTD support); Day TIF resting orders |

### Compatibility Rules

- The journal reader accepts v5, v7, v8, and v9 files. V5 journals lack hash chain verification; v7-v8 lack newer event types but are otherwise compatible.
- The snapshot reader accepts recent versions with backward-compatible loading. Older snapshots may lack fields (fee schedules, key HWMs, instrument status, expiry) which default to safe values on load.
- Older versions are rejected with `UnsupportedVersion`.

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

1. Assign a new event tag (next available: 19).
2. Add encode/decode logic to the codec.
3. Bump `FORMAT_VERSION` (old readers will reject the new file, which is correct).
4. Follow the standard upgrade procedure above.

If the new event type is purely additive (old events unchanged, new tag added), it is possible to keep the same `FORMAT_VERSION` — old readers will fail with `CorruptEntry` on the unknown tag, which is the same outcome as a version mismatch. Bumping the version is preferred because it fails fast at file open rather than mid-replay.

### Adding Fields to Existing Events

Adding a field to an existing event (as done in v1→v2 with `SelfTradeProtection`):

1. Append the field to the end of the event's binary layout.
2. Bump `FORMAT_VERSION`.
3. Follow the standard upgrade procedure.

Inserting a field in the middle or changing field sizes breaks all entries in the file. Appending is always safe and preferred.

## Operational Notes

### Journal File Growth

The journal grows monotonically. Pre-allocation extends it in 256 MiB chunks. A single entry is ~40-85 bytes, so 256 MiB covers roughly 3.2M-6.4M entries before the next allocation.

At sustained 830K orders/sec (with fsync), the journal grows at ~50-70 MB/sec. Journal rotation triggers at startup when the file exceeds `--max-journal-mib` (default 256 MiB), creating a snapshot and archiving the old journal.

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
| `HashChainMismatch` | BLAKE3 chain hash verification failed at checkpoint | Tampered or corrupt entry between checkpoints — investigate |
| `Io` | Underlying I/O error | Disk failure, permissions, full disk |

### Limitations

- **Startup-only rotation** — journal rotation triggers at startup when the file exceeds the size threshold. Runtime rotation (during sustained load) is not yet implemented.
- **No output event log** — execution reports are not persisted. Audit trail requires replaying the journal.
- **Single journal file** — no striping or parallel writes. The journal is single-threaded by design (LMAX architecture).
- **No encryption** — journal and snapshot files are plaintext binary. Sensitive data (account IDs, order details) is visible to anyone with file access.
