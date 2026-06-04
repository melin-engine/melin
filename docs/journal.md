# Journal & Event Sourcing

This document describes the write-ahead journal, snapshot system, crash recovery, and version migration procedures.

## Design Principles

1. **Input-only journaling** — only input commands are persisted (order submissions, cancellations, deposits, instrument creation). Execution reports are *not* journaled. The matching engine is deterministic: replaying the same inputs always produces identical outputs. This halves journal size and avoids coupling the journal format to execution report changes.

2. **Persist-before-ack** — no response is sent to a client until the corresponding journal entry is durable on disk. The LMAX disruptor pipeline enforces this: the response stage gates on the journal cursor, which advances only after `pwritev2 + RWF_DSYNC` completes.

3. **Manual binary codec** — no serde, no protobuf. Every field is encoded by hand in little-endian with known offsets. This gives predictable layout, zero allocations, and immunity to serialization library version changes.

4. **CRC32C integrity** — every journal entry and every snapshot file is checksummed with CRC32C (hardware-accelerated on x86). Corruption is detected on read, never silently replayed.

5. **BLAKE3 hash chain, anchored per segment** — every journal segment carries a 32-byte chain **anchor** in its file header (random salt for a fresh journal, the previous segment's tail hash after rotation). The chain value after any entry is a pure function of the anchor and the raw on-disk bytes: `chain(S) = BLAKE3(entry bytes through S ‖ anchor)`. No chain metadata lives in the entry stream — sequence numbers are dense over real events, and a sealed segment can be verified with nothing but its own bytes and its successor's anchor.

## Journal File Format

### File Header (52 meaningful bytes, written once, padded to 4096)

```
Offset  Size  Field              Value
0       4     file_magic         0x4A4F5552 ("JOUR")
4       2     format_version     14
6       2     sector_size        4096
8       8     starting_sequence  sequence carried by this segment's first entry
16      32    anchor_hash        chain anchor (random salt or previous segment's tail hash)
48      4     header_crc         CRC32C of the preceding 48 bytes
```

The header is written when the journal is created and never modified. Its CRC protects the anchor — the root of all chain verification — against storage corruption. `format_version` is checked on open; only the current version is accepted (pre-production policy — see Migration below).

### Entry Layout (repeats after the 4096-byte header reservation)

```
Offset  Size  Field           Description
0       2     entry_magic     0x4A45 — misalignment / corruption detection
2       2     length          byte count of (key_hash + request_seq + event_tag + payload)
4       8     sequence        monotonically increasing, starts at 1, no gaps
12      8     timestamp_ns    wall-clock nanoseconds since Unix epoch (informational only)
20      8     key_hash        hash of the client's signing key (0 for server-internal events)
28      8     request_seq     per-key request sequence (idempotency dedup)
36      1     event_tag       discriminant (Tick, or App for exchange events)
37      var   payload         event-specific fields (see below)
37+len  4     crc32c          CRC32C of all preceding bytes in this entry
```

Total entry size: `20 + length + 4` bytes. Typical entries are 60-110 bytes. The first entry's `sequence` must equal the header's `starting_sequence` — a segment renamed into the wrong place in the lineage is rejected at the first read.

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
recover(journal_path):
  1. For each archived segment in monotonic order, then the live segment:
     a. Validate the file header (magic + version + header CRC).
     b. Verify lineage: the header's anchor must equal the previous
        segment's tail chain hash, and the header's starting_sequence
        must continue the sequence space exactly. Checked BEFORE any
        replay — a foreign or tampered segment never reaches the engine.
     c. Read entries sequentially:
        - Validate entry_magic (0x4A45), CRC32C, sequence continuity.
        - Absorb the entry's raw bytes into the segment hash chain.
        - Replay on the Exchange.
        - If entry_magic is 0x0000 → end of data (pre-allocated space). Stop.
        - If entry is truncated at EOF (live segment) → partial write
          from crash. Stop.
        - If CRC mismatch or sequence gap mid-archive → return error.
  2. Truncate the live file to valid_file_end (remove trailing garbage).
  3. Re-allocate space from valid_file_end forward.
  4. Reopen writer for appending. The writer rebuilds its chain state
     self-containedly: anchor from the header, hasher re-absorbed from
     the raw byte range — no chain state is handed over from the replay.
```

**Key behaviors:**

- A truncated final entry is treated as harmless (crash during write) and silently discarded. The events it contained were never acknowledged to the client (persist-before-ack), so no client believes they succeeded.
- Zero-filled bytes (from `posix_fallocate`) are treated as end-of-data, not corruption.
- A CRC mismatch or sequence gap mid-stream is treated as real corruption and returns an error. The operator must investigate — this should never happen under normal operation.

### Recovery with Snapshots

```
recover_from_snapshot(snapshot_path, journal_path):
  1. Load snapshot → (Exchange state, snapshot_sequence, snapshot_chain_hash).
  2. Walk segments as above. Skip events with sequence <= snapshot_sequence
     (still validated and absorbed into the chain).
  3. At the snapshot's anchor sequence, verify the journal's chain hash at
     that point matches the snapshot's recorded chain hash. Mismatch aborts
     recovery before any post-snapshot events are replayed.
  4. Replay only entries after the snapshot.
  5. Truncate and reopen writer as above.
```

This avoids replaying the entire journal from genesis. Recovery time is proportional to the journal tail length (events since last snapshot), not total history.

The chain-hash cross-check at the anchor sequence ensures the snapshot and the journal share the same history: it rejects a snapshot paired with another cluster's journal, a divergent history, or a journal whose entries up to the anchor were tampered with. A snapshot anchored exactly at a rotation boundary is verified against the successor segment's header anchor (which *is* the chain value at that boundary) — so the check holds even when the segment holding the anchor entry has been moved to cold storage.

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

Every journal segment maintains a BLAKE3 hash chain for tamper evidence. The chain is **schedule-free**: its value at any point depends only on the segment's header anchor and the raw bytes written so far — never on how writes were batched or when intermediate values were computed.

### How It Works

1. **Anchor** — each segment's file header carries a 32-byte anchor: random salt for a fresh journal (so two independent journal lineages can never share a chain value), or the previous segment's tail hash after rotation.

2. **Chain definition** — `chain(S) = BLAKE3(raw bytes of entries 1..=S ‖ anchor)`, where "raw bytes" are the entries exactly as written on disk, CRC trailers included. An empty segment's chain value is its anchor. Because the definition is over the byte stream, the chain over a sealed segment can be recomputed by any tool that can hash a byte range — no journal-aware decoding required.

3. **Cost** — entries are absorbed into an incremental hasher (~15-30 ns each, in memory only). The 32-byte value is finalized on demand — at fsync boundaries (for snapshot coordination), at snapshot saves, and at rotation — never per entry.

4. **Rotation continuity** — the new segment's header anchor is the outgoing segment's tail chain hash. Recovery verifies this link *before* replaying each segment, so a tampered, missing, or foreign archive is rejected before any of its events reach the engine.

5. **Snapshot integration** — snapshots store the chain hash at their anchor sequence; recovery cross-checks it against the journal (see Recovery with Snapshots above).

### What It Detects

- **Tampered entries** — even a CRC-consistent rewrite (payload altered, CRC recomputed) changes the segment's tail hash and breaks the link to the next segment's anchor or the snapshot cross-check. (Plain bit-flips are caught earlier, by per-entry CRC32C.)
- **Reordered, inserted, or removed entries** — any change to the byte stream changes the chain.
- **Snapshot/journal mismatch** — a snapshot paired with the wrong cluster's journal, a divergent history, or pre-anchor tampering is rejected before any state is restored.
- **Lineage breaks** — a missing archive between two surviving segments, or an archive from another deployment spliced into the directory.

### What It Does NOT Detect

- **Tamper in the live segment after the last snapshot anchor** — nothing has committed to those bytes yet. (An attacker with that level of access could equally truncate the tail, which is likewise undetectable in any design; sealing the segment — rotation — or the next snapshot closes the window.)
- **Truncation attacks** — removing entries from the end of the live segment produces a valid (shorter) chain. Sequence numbers detect this if the expected sequence is known externally.

## Journal Rotation

When the live segment exceeds the configured size threshold (`--max-journal-mib`, default 256 MiB), or an operator issues `ROTATE`, the journal stage rotates at the next fsync boundary — while the engine is live:

1. **Archive the live segment** by renaming it to the next monotonic slot (`melin.journal` → `melin.journal.000042`).
2. **Create a new live segment** at the original path. Its header records the continuing sequence number and an anchor equal to the old segment's tail chain hash. No snapshot is taken and **no sequence number is consumed** — the next event gets exactly the sequence it would have without the rotation.

Recovery walks archives in order, then the live segment. Old segments are kept for audit. See [Journal Rotation & Recovery](journal-rotation.md) for crash windows and operational guidance.

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

Both the journal and snapshot have independent `format_version` fields. Current journal version: **14**. Current snapshot version: **12**.

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
| 10-12 | Per-entry `key_hash` + `request_seq` metadata (idempotency dedup); transport/application event-tag split |
| 13 | Entry offset fixed at 4096 regardless of device sector size, so journals are interchangeable between the buffered and O_DIRECT writers |
| 14 | Chain metadata moved out of the entry stream: file header gained `starting_sequence`, `anchor_hash`, and a header CRC; `GenesisHash` and `Checkpoint` entry tags retired. The chain is anchored per segment and schedule-free; sequence numbers are dense over real events |

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

- **Pre-production policy:** the journal reader accepts only the current format version. Older versions are rejected with `UnsupportedVersion`; migrate via the snapshot-boundary procedure below.
- The snapshot reader accepts recent versions with backward-compatible loading. Older snapshots may lack fields (fee schedules, key HWMs, instrument status, expiry) which default to safe values on load.

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

At sustained 830K orders/sec (with fsync), the journal grows at ~50-70 MB/sec. Journal rotation triggers live, at the fsync boundary after the segment exceeds `--max-journal-mib` (default 256 MiB), archiving the old segment — see [Journal Rotation & Recovery](journal-rotation.md).

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
| `ChecksumMismatch` | CRC32C validation failed (entry or file header) | Bit rot or partial write — investigate storage |
| `SequenceGap` | Non-contiguous sequence numbers, or a segment's first entry disagreeing with its header | Corruption, file truncation, or a misplaced segment — investigate |
| `SequenceDuplicate` | A sequence number repeated | Writer bug or storage anomaly — investigate |
| `TruncatedEntry` | Incomplete entry at EOF | Normal crash recovery — entry is discarded |
| `SegmentChainBreak` | A segment's header anchor does not equal the previous segment's tail chain hash | Tampered archive, missing segment, or foreign segment spliced in — investigate before trusting the history |
| `MissingHistoryPrefix` | The oldest surviving segment starts after the history start recovery requires (sequence 1, or the snapshot's anchor + 1) | Archives trimmed without a covering snapshot — restore the trimmed segments or a snapshot that covers them; recovery refuses to build partial state |
| `Io` | Underlying I/O error | Disk failure, permissions, full disk |

### Limitations

- **No output event log** — execution reports are not persisted. Audit trail requires replaying the journal.
- **Single journal file** — no striping or parallel writes. The journal is single-threaded by design (LMAX architecture).
- **No encryption** — journal and snapshot files are plaintext binary. Sensitive data (account IDs, order details) is visible to anyone with file access.
- **No cross-node chain comparison at runtime** — each node verifies its own journal's integrity locally. Comparing chain values between primary and replica requires aligned segment boundaries (primary-driven rotation), tracked on the roadmap.
