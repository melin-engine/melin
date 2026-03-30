# Replication Design Document

## Overview

Synchronous journal replication from a primary server to a replica, providing zero-data-loss failover capability. The primary streams journal entries to the replica over TCP; the replica persists them locally and acknowledges. Client responses are gated on **both** local journal durability and replica acknowledgement — a client never learns about an event that the replica hasn't durably stored.

## Architecture

```
Primary:
  Readers → Disruptor → JournalStage  (consumer 0) → disk + replication channel
                       → MatchingStage (consumer 1) → OutputSPSC

  JournalStage: after flush_batch_sync(), sends a copy of the
  exact bytes written to disk to the replication sender thread.

  ReplicationSender thread: streams DataBatch frames to replica,
  processes acks, updates replication_cursor.

  ResponseStage gates on min(journal_cursor, replication_cursor)

Replica:
  TCP → ReplicationReceiver → decode entries, verify CRC
      → write_raw_sync() (byte-for-byte copy to local journal)
      → replay into Exchange (state)
      → ack sequence back to primary
```

### Pipeline integration

Replication is integrated into the `JournalStage` rather than running as a separate disruptor consumer. Before each `flush_batch_sync()`, the journal stage copies its pending batch buffer into a pre-allocated slot in a lock-free replication ring (64 slots × 128 KiB = 8 MiB). The replication sender thread consumes from this ring and streams batches to the replica. This guarantees the replicated bytes are **identical** to what was written to disk — same sequences, timestamps, CRC checksums, and checkpoint entries. No heap allocation on the journal thread — just a flat memcpy into the ring.

This design avoids a class of bugs where a separate replication consumer would re-encode events independently, producing different timestamps, missing auto-emitted checkpoint entries, and diverging BLAKE3 chain hashes.

### Ack-after-replicate

The response stage gates on `min(journal_cursor, replication_cursor)` instead of just `journal_cursor`. This ensures:

- A client only receives a response once the event is **locally durable** AND **replicated**.
- On failover, the replica has every event the client was told about.
- No data loss window — same guarantee as Raft commit.

**Latency impact**: adds ~100-200 µs (LAN round-trip) to client-perceived latency. Throughput is unaffected — batching amortizes the round-trip across many events.

### Replication cursor behavior

| Scenario | `replication_cursor` | Response gate effect |
|---|---|---|
| `--standalone` (dev/test) | `u64::MAX` | `min(journal, MAX) = journal` — no replication |
| `--replication-bind`, no replica connected | `u64::MAX` | Same as standalone — server works normally |
| Replica(s) connected, acking | Latest acked seq | Waits for both journal + replica |
| One replica disconnects (other still connected) | Maintained by surviving replica | Trading continues normally |
| All replicas disconnect | `u64::MAX` | Degrades to local-only, trading halted, operator alerted |
| Replica reconnects | Resumes from ack | Gate re-engages |

The cursor is **always initialized to `u64::MAX`**, even when replication is enabled. This ensures the server starts immediately and serves clients without waiting for a replica. The cursor only engages when a replica connects and starts sending acks. On all-disconnect, it resets to `u64::MAX`.

The cursor update is **monotonic** (`fetch_max`) — a stale ack (e.g., from a previous connection) cannot regress the cursor to a lower value.

## Wire Protocol

Length-prefixed frames, little-endian. Runs over a dedicated TCP connection separate from the client protocol.

### Replica → Primary

| Message | Layout | Purpose |
|---|---|---|
| Handshake | `[len:u32][type=0x01][last_sequence:u64][chain_hash:[u8;32]]` | Initial connection: replica reports its last durable sequence and chain hash |
| Ack | `[len:u32][type=0x02][acked_sequence:u64]` | Replica confirms durable write up to this sequence |

### Primary → Replica

| Message | Layout | Purpose |
|---|---|---|
| StreamStart | `[len:u32][type=0x10][start_sequence:u64][genesis_len:u32][genesis_entry_bytes...]` | Confirms handshake, includes raw genesis entry for byte-identical hash chain |
| NeedSnapshot | `[len:u32][type=0x11]` | Replica is too far behind; triggers snapshot transfer |
| SnapshotBegin | `[len:u32][type=0x13][snapshot_len:u64][snap_sequence:u64][snap_chain_hash:[u8;32]]` | Start of snapshot transfer with metadata |
| SnapshotChunk | `[len:u32][type=0x14][data...]` | Chunk of snapshot data (up to 64 KiB) |
| SnapshotEnd | `[len:u32][type=0x15][crc32c:u32]` | End of snapshot transfer with CRC32C for integrity |
| HashMismatch | `[len:u32][type=0x12]` | Chain hash doesn't match at the replica's reported sequence (not yet validated) |
| DataBatch | `[len:u32][type=0x20][end_sequence:u64][chain_hash:[u8;32]][journal_bytes...]` | Batch of encoded journal entries with trailing chain hash |
| Heartbeat | `[len:u32][type=0x30][sequence:u64][chain_hash:[u8;32]]` | Periodic idle keepalive (5-second interval) with current state |

### Design rationale

- **Journal byte reuse**: DataBatch payloads contain the exact bytes from the primary's journal file. The replica writes them directly via `write_raw_sync()`, producing a byte-for-byte copy. No re-encoding, no second serialization format.
- **Dual replication**: The primary accepts up to 2 concurrent replica connections, each with its own replication ring consumer and handler thread. If a replica disconnects, its slot becomes available for a new connection. Trading halts only when all replicas disconnect.

## Replica Mode

A server started with `--replica-of <primary_addr>` runs in replica mode:

- Authenticates with the primary via Ed25519 challenge-response (`--replication-key`).
- Connects to the primary and sends a `Handshake`.
- Receives `DataBatch` frames, decodes entries, verifies CRC per entry.
- Writes raw bytes to a local journal via `write_raw_sync()` for durability.
- Replays entries into a local `Exchange` for state.
- Sends `Ack` frames after each durable write.
- Saves periodic snapshots every 5M events during catch-up, so a crash doesn't require replaying from genesis.
- On restart, uses `recover_from_snapshot` if a snapshot exists alongside the journal.
- Does **not** accept client connections (read-only state).

## CLI Flags

| Flag | Required | Default | Purpose |
|---|---|---|---|
| `--replication-bind <addr>` | No | — | Address to listen for replica connections |
| `--standalone` | No | `false` | Explicitly disable replication (dev/test) |
| `--replica-of <addr>` | No | — | Run as a replica connected to the given primary |
| `--replication-key <path>` | Replica | — | Ed25519 private key for replication auth. Required when `--replica-of` is set. The corresponding public key must be in the primary's `authorized_keys` with `replication` permission. |

`--replication-bind` and `--standalone` are mutually exclusive. `--replica-of` is mutually exclusive with both. If none are specified, the server runs in standalone mode.

## Current Limitations (v1)

These are known limitations of the current implementation. Each is documented here with the reason it was deferred and the plan for resolution.

### ~~No catch-up from journal files~~ (IMPLEMENTED)

When a replica connects, the primary reads its journal archive files and streams historical entries as DataBatch frames before switching to live ring data. The `RawJournalScanner` reads entry boundaries without full decoding (no CRC validation, no event parsing) for efficient streaming. The replication ring is NOT consumed during catch-up — live data accumulates in the ring and overlapping entries are drained after catch-up completes.

This works for both reconnecting replicas (`last_sequence > 0`, catches up the gap) and fresh replicas (`last_sequence = 0`, streams the entire journal history). No operator intervention required — a new replica can join a running primary at any time.

### No chain hash verification on received DataBatch

**What**: The `chain_hash` field in DataBatch frames is populated by the primary but **not verified** by the replica. The replica decodes entries and checks individual CRC32C checksums but does not verify that the batch-level chain hash matches.

**Impact**: Corruption that preserves individual entry CRCs but reorders or drops entries within a batch would go undetected. In practice, TCP ordering guarantees make this extremely unlikely.

**Why deferred**: Verifying the chain hash requires the replica to maintain its own running hash state and compare after each batch. The journal's per-entry CRC32C provides entry-level integrity, and TCP provides ordering. Adding chain verification is a defense-in-depth measure, not a correctness requirement for the common case.

**Resolution**: After decoding all entries in a DataBatch, compute the BLAKE3 chain hash over the raw bytes and compare against `batch_chain_hash`. Reject the batch and disconnect on mismatch.

### No handshake chain hash validation

**What**: The primary does not validate the replica's `chain_hash` from the Handshake against its own journal at the replica's reported `last_sequence`. It unconditionally sends `StreamStart`.

**Impact**: A replica with divergent history (e.g., connected to a different primary previously, or with a corrupted journal) will be accepted without warning. The `HashMismatch` response type is defined in the protocol but never sent. (`NeedSnapshot` is now sent when journal archives are too old — see snapshot transfer below.)

**Why deferred**: Validating the chain hash at an arbitrary historical sequence requires either keeping a mapping of sequence→chain_hash (expensive) or re-reading the journal from genesis (slow). For v1, the assumption is that replicas are fresh or were connected to this primary.

**Resolution**: Store periodic chain hash checkpoints in a side index, or validate by reading the journal file at the reported sequence.

### ~~Fresh replica genesis hash diverges from primary~~ (FIXED)

The primary's raw genesis entry bytes (including the original timestamp) are sent in the `StreamStart` response. Fresh replicas write these bytes directly to the journal file, producing a byte-identical genesis entry. The BLAKE3 hash chain starts from the exact same encoded bytes, so checkpoint entries from the primary verify correctly on replica replay.

### ~~Single replica only~~ (FIXED)

Dual replication is now supported — the primary accepts up to 2 concurrent replica connections, each with its own replication ring consumer and handler thread. If one replica fails, trading continues with the surviving replica. Trading halts only when all replicas disconnect. The replication cursor uses `fetch_max` so either replica's acks advance the response gate.

### Backpressure from replication channel can stall the pipeline

**What**: The journal stage publishes to a lock-free replication ring (64 slots × 128 KiB). If the sender thread is slow (network saturated, replica not acking), the ring fills and the journal stage spins in `try_claim()`. This blocks the journal stage, which blocks the disruptor, which blocks all reader threads.

**Impact**: Under extreme replication lag, client request processing stalls. The 1M-slot disruptor ring provides substantial buffering before this happens (~100ms at 10M events/sec), but a multi-second network partition would trigger it.

**Mitigation**: On replica disconnect, the sender thread drains the ring (discards batches) and the cursor resets to `u64::MAX`, unblocking the pipeline.

**Resolution**: Consider a non-blocking publish with overflow detection, or increasing the ring capacity (currently 64 slots = 8 MiB).

### `read_frame` partial read on timeout

**What**: The ack reader socket has a 1ms read timeout. If `read_exact` partially reads a frame header (e.g., 2 of 4 bytes) before the timeout fires, the next `read_frame` call starts mid-frame, permanently desynchronizing the stream.

**Impact**: Extremely unlikely with TCP (kernel buffers ensure complete small reads), but theoretically possible under extreme memory pressure or with pathological packet fragmentation.

**Mitigation**: The 1ms timeout is short enough that ack frames (9 bytes) arrive atomically in practice. If desync occurs, the decode will fail and the connection will be dropped and re-established.

**Resolution**: Use a `BufReader` wrapper that preserves partial reads across calls, or switch to non-blocking I/O with explicit read state tracking.

## Future Work

- **Chain hash verification** — see limitation above
- **Automatic failover**: Leader election / consensus for automatic promotion. Requires fencing to prevent split-brain.
- **Async replication**: Optional mode where the response stage does not gate on the replication cursor (lower latency, data loss window).
