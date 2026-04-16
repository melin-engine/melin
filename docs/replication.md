# Replication Design Document

## Overview

Synchronous journal replication from a primary server to a replica, providing zero-data-loss failover capability. The primary streams journal entries to the replica over TCP; the replica persists them locally and acknowledges. With quorum durability (default), when 2 replicas are connected, client responses are gated on replication acknowledgement alone — the journal still writes for local crash recovery but does not block responses. When fewer than 2 replicas are connected, responses are gated on both local journal durability and replication. A client never learns about an event that isn't durably stored on at least two nodes.

## Architecture

```
Primary:
  Readers → Disruptor → JournalStage  (consumer 0) → disk + replication rings
                       → MatchingStage (consumer 1) → OutputSPSC

  JournalStage: after flush_batch_sync(), publishes batch bytes to
  two independent SPSC replication rings (one per replica slot).

  ReplicationSender threads: each consumes from its own ring,
  streams DataBatch frames to its replica via io_uring SEND,
  receives acks via io_uring RECV, updates replication_cursor.

  ResponseStage gates on replication_cursor (quorum mode, 2 replicas)
  or min(journal_cursor, replication_cursor) (degraded/no-quorum mode)

Replica:
  TCP → ReplicationReceiver → decode entries, publish to disruptor
                                (with pre-assigned sequences + timestamps)
      → JournalStage encodes independently (same sequences as primary)
      → MatchingStage processes through own Exchange
      → ack sequence back to primary after journal cursor advances
```

### Replication rings and fault isolation

Each replica slot has its own independent ring buffer (configurable via `--replication-ring-size`, default 64 slots x 512 KiB = 32 MiB per ring, 64 MiB total for dual replication). The replicated bytes are the encoded journal batches from the primary. The replica decodes them, publishes events to its own pipeline with the primary's pre-assigned sequences and timestamps, and encodes its own journal independently. Journals are logically identical (same sequences, same events) but each node encodes independently.

**Fault isolation**: a slow replica only stalls its own ring, not the other replica's. If a ring is full for longer than 500ms (replica not keeping up), the primary automatically disconnects that replica and frees the ring. The slot becomes available for a new connection. The surviving replica and client trading are unaffected.

### Ack-after-replicate and quorum durability

The response stage enforces durability before sending client responses. An event is durable when it exists on at least two nodes:

```
durable = max(both_replicas_acked, min(journal_synced, fastest_replica_acked))
```

This gives the best of both paths:

- If **both replicas ack before fsync** completes → respond immediately (NVMe off the critical path)
- If **one replica is slow but fsync is fast** → respond as soon as fsync + fast replica confirms (two durable copies via different routes)
- If **0-1 replicas connected** → fall back to `min(journal, replication)` (local fsync required)
- **`--no-quorum-durability`**: forces `min(journal, replication)` unconditionally, useful for debugging

In all modes, a client never receives a response for an event that isn't durably stored. On failover, the replica has every event the client was told about. Same guarantee as Raft commit.

**Latency impact**: quorum mode removes NVMe fsync tail variance (1-5ms GC spikes) from the critical path when both replicas are healthy. When one replica lags, fsync + the fast replica still provides two durable copies without waiting for the slow one. Throughput is unaffected — batching amortizes the round-trip across many events.

### Async replica ack (`--async-replica-ack`)

By default, each replica fsyncs an incoming batch to its local NVMe before sending the corresponding `Ack` to the primary. The replica's contribution to the durability gate (`replicas_acked`) therefore means "two physical disks confirm the data" — the strongest tier.

Setting `--async-replica-ack` on a replica makes it ack as soon as the batch is queued for its local journal stage, before fsync completes. This removes one NVMe write (~50–80µs on enterprise NVMe) from the replication round-trip. The local fsync still happens — just in parallel with the primary's response release rather than gating it.

**What changes for the durability gate:**

Pre-`--async-replica-ack` — `replicas_acked` means *both replicas have the data fsynced on disk*.

With `--async-replica-ack` — `replicas_acked` means *both replicas have the data in RAM and are committed to fsyncing it*. The primary's own journal fsync is still synchronous, so when the primary tells a client "your trade is filled" the data is durable on:

1. The primary's local NVMe (fsynced) — synchronously verified
2. Replica1's RAM, fsync in flight
3. Replica2's RAM, fsync in flight

**Failure modes:**

| Scenario | Sync ack (default) | `--async-replica-ack` |
|---|---|---|
| Primary crashes (recoverable) | Recovers from local journal. ✓ | Same. ✓ |
| One replica crashes alone | Catches up from primary on reconnect via the catch-up protocol. ✓ | Same — the in-flight (acked-but-not-fsynced) entries are missing from the dead replica's disk, but the primary still has them on disk and re-streams them via catch-up. ✓ |
| Both replicas crash simultaneously | Trading halts; replicas catch up on restart. ✓ | Same. ✓ |
| Primary disk fails, promote a replica | The promoted replica has every fill the client was told about. ✓ | The promoted replica may be missing the last ~50–80µs of fills, because those were "acked" without fsync and the primary's disk (the only other copy) is now gone. **Data loss window: ~50–80µs of recent fills.** |
| Primary AND a replica crash within ~80µs of a fill confirmation | Survivable: the surviving replica has every confirmed fill on disk. ✓ | Same surviving-replica caveat as the row above — there's a ~80µs window where the surviving replica may be missing the most recent fills. |

**When to use it:**

`--async-replica-ack` is appropriate when the primary's local disk write is *already redundant* (capacitor-backed enterprise NVMe with power-loss protection, or RAID-1/10 underneath the journal), so the replica's fsync is a defense-in-depth backup rather than the load-bearing durability mechanism. Under those conditions, the ~50–80µs latency improvement comes essentially free — the only failure mode it weakens (primary disk dies within 80µs of a fill) is already mitigated by the hardware.

Conversely, on commodity NVMe with no power-loss protection or RAID, the replica's fsync is genuinely the second copy of the data and removing it from the critical path is reckless. Leave the flag off.

The flag is set per-replica, not on the primary. Mixing modes across replicas is supported: one replica can run sync, the other async, and the response gate will use whichever ack arrives first via the `fastest_replica_acked` term.

### Replication cursor behavior

| Scenario | `replication_cursor` (min) | `fastest_replica_cursor` (max) | Response gate |
|---|---|---|---|
| `--standalone` | `u64::MAX` | `u64::MAX` | `min(journal, MAX) = journal` |
| No replicas connected | `u64::MAX` | `u64::MAX` | Same as standalone |
| 1 replica connected | Acked seq | `u64::MAX` (idle slot) | `min(journal, repl)` |
| 2 replicas, quorum mode | `min(slot0, slot1)` | `max(slot0, slot1)` | `max(min_repl, min(journal, max_repl))` |
| One replica disconnects (other still connected) | Maintained by surviving replica | Trading continues normally |
| All replicas disconnect | `u64::MAX` | Degrades to local-only, trading halted, operator alerted |
| Replica reconnects | Resumes from ack | Gate re-engages |

The cursor is **always initialized to `u64::MAX`**, even when replication is enabled. This ensures the server starts immediately and serves clients without waiting for a replica. The cursor only engages when a replica connects and starts sending acks. On all-disconnect, it resets to `u64::MAX`.

Each handler thread maintains a per-slot acked position and recomputes the shared cursors as `min`/`max` of both slots on every ack. This allows the cursors to decrease when a slower replica connects or a faster one disconnects.

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

- **Independent encoding**: DataBatch payloads contain encoded journal entries from the primary. The replica decodes them, extracts the pre-assigned sequences and timestamps, and re-encodes through its own JournalStage. Journals are logically identical across nodes (same sequences, same events), enabling deterministic replay and independent verification.
- **Dual replication**: The primary accepts up to 2 concurrent replica connections, each with its own replication ring consumer and handler thread. If a replica disconnects, its slot becomes available for a new connection. Trading halts only when all replicas disconnect.

## Replica Mode

A server started with `--replica-of <primary_addr>` runs in replica mode:

- Authenticates with the primary via Ed25519 challenge-response (`--replication-key`).
- Connects to the primary and sends a `Handshake`.
- Receives `DataBatch` frames, decodes entries, publishes them to a local disruptor pipeline.
- Uses the same pipeline architecture as the primary (journal stage → matching stage → shadow stage), with the replication receiver feeding the input disruptor instead of reader threads.
- The journal stage encodes events independently using the primary's pre-assigned sequences and timestamps (carried in each `InputSlot`). Each node produces its own journal — logically identical to the primary's but independently encoded.
- The matching stage processes events through its own `Exchange` independently, maintaining warm state for promotion.
- Sends `Ack` frames after the journal stage confirms durable write (cursor advance). Acks are pipelined: up to 8 batches can be submitted to the journal stage before the first ack is sent, overlapping NVMe writes with TCP receives. With `--async-replica-ack`, acks are sent the moment a batch is queued for the journal stage rather than after fsync — see the durability tradeoff section above.
- Both the primary sender and replica receiver use io_uring for TCP I/O (async RECV/SEND), eliminating poll/read/write syscalls from the streaming hot path.
- The replica pipeline threads (journal, matching, drain, shadow) are pinned to the same cores as the primary, matching the `--cores` layout.
- If the primary disconnects or evicts the replica, the receiver automatically reconnects with exponential backoff (1s → 30s cap), recovers state from the pipeline shutdown, and resumes from its last durable sequence.
- The shadow stage runs on a dedicated thread with a cloned `Exchange`, saving periodic snapshots so a crash doesn't require replaying from genesis.
- On restart, uses `recover_from_snapshot` if a snapshot exists alongside the journal.
- Does **not** accept client connections (read-only state).

### Replica pipeline topology

```
TCP Stream → Replication Receiver (decode + publish with pre-assigned seq/ts)
                    ↓
              Input Disruptor
              ┌─────┼─────┐
              │     │     │
          Journal Matching Shadow
          Stage   Stage   Stage
              │     │     │
           (cursor) │  (snapshots)
              │     ↓
              │  Output Disruptor → Drain (no clients on replica)
              ↓
         Ack to primary
```

The receiver thread uses io_uring for TCP I/O: a single RECV is always in-flight for DataBatch frames, and SEND is submitted when an ack becomes ready. It decodes events from DataBatch frames and publishes them to the input disruptor with the primary's pre-assigned sequences and timestamps. Checkpoint events are filtered out — each node auto-emits its own. The journal and matching stages consume events in parallel (same topology as the primary).

The pipelined ack queue (8 entries) decouples the receiver's TCP loop from NVMe write latency — the receiver can push up to 8 batches ahead while previous writes are in flight. Acks are sent as soon as the journal cursor confirms durability, checked on every event loop iteration with zero syscall overhead.

## CLI Flags

| Flag | Required | Default | Purpose |
|---|---|---|---|
| `--replication-bind <addr>` | No | — | Address to listen for replica connections |
| `--standalone` | No | `false` | Explicitly disable replication (dev/test) |
| `--replica-of <addr>` | No | — | Run as a replica connected to the given primary |
| `--replication-key <path>` | Replica | — | Ed25519 private key for replication auth. Required when `--replica-of` is set. The corresponding public key must be in the primary's `authorized_keys` with `replication` permission. |
| `--promote-bind <addr>` | Replica | — | Address to listen for promotion commands. An operator sends `PROMOTE\n` to trigger in-process transition from replica to primary. |
| `--async-replica-ack` | Replica | `false` | Ack incoming batches as soon as they are queued for the local journal stage instead of waiting for fsync. Removes ~50–80µs from the replication round-trip; documented durability tradeoff above. |

`--replication-bind` and `--standalone` are mutually exclusive. `--replica-of` is mutually exclusive with both. If none are specified, the server runs in standalone mode.

## Snapshot Transfer

When a replica is too far behind and the primary's journal archives have been purged (the needed entries no longer exist in any `.journal.N` file), the primary sends a `NeedSnapshot` message followed by a snapshot transfer:

1. **SnapshotBegin**: metadata frame with snapshot size, sequence, and chain hash.
2. **SnapshotChunk**: the snapshot data in 64 KiB chunks.
3. **SnapshotEnd**: CRC32C checksum of the entire snapshot payload for integrity verification.

The replica receives the chunks, writes the snapshot to a temporary file, verifies the CRC, atomically renames to the snapshot path, loads the snapshot into its Exchange, and resumes normal replication from the snapshot's sequence. This enables fresh replicas to bootstrap from a running primary without needing the full journal history.

The CRC32C is computed incrementally on the primary as chunks are read from disk, and verified incrementally on the replica as chunks are received — no need to buffer the entire snapshot in memory.

## Manual Promotion

A replica can be promoted to primary via the `--promote-bind` endpoint. The operator connects to the promotion port and sends `PROMOTE\n`. The replica:

1. Sets the `promote` flag, which causes the replication receiver to exit its main loop.
2. Transitions in-process from replica to primary: the warm Exchange state is reused directly — no journal re-replay, no snapshot reload.
3. Starts accepting client connections on the `--bind` address.

This achieves sub-second switchover. The `melin-promote` binary (in `crates/admin/`) automates this: it connects to the promotion endpoint, sends the promote command, and waits for confirmation.

**Important**: after promotion, the old primary must be stopped to prevent split-brain (two primaries accepting writes). Automatic fencing is not yet implemented — see Future Work.

## Cluster Recovery with Quorum Durability

With quorum durability, the primary can acknowledge events before the local journal fsyncs — durability is guaranteed by the two replica copies. This means after a crash, the three nodes may have different journal lengths. The quorum formula `max(repl_min, min(journal, repl_max))` computes the **median** of the three journal positions. The median is exactly the set of events acknowledged to clients.

### Which journal is authoritative?

After a cluster-wide outage, each node restarts with its own journal. The three journals may differ:

| Node | Journal length | Status |
|------|---------------|--------|
| **Shortest** | Behind the acked frontier | Missing events that were acked via the replication-only path (both replicas confirmed, local fsync hadn't completed) |
| **Middle** | Matches the acked frontier | Contains exactly the events clients were told about |
| **Longest** | Ahead of the acked frontier | Has extra entries that were replicated but not yet acked to clients (the other replica hadn't confirmed) |

**The middle journal is always correct.** It represents the quorum commit point — every event in it was confirmed on at least two nodes before the client was notified.

### Recovery procedure

1. **Stop all three nodes** if not already stopped.
2. **Compare journal end sequences** on all three nodes using `melin-admin journal-info`.
3. **Sort by sequence**: identify the shortest, middle, and longest.
4. **Promote the middle node** — its journal is authoritative. If two nodes have the same length, either is valid (they have the same entries).
5. **Start the middle node as primary.**
6. **Connect the other two nodes as replicas.** The shortest catches up from the new primary's journal (normal replica catch-up). The longest reconnects and its extra entries are harmlessly overwritten during catch-up (the primary's journal is authoritative after promotion).

### Single-node failures (no full outage)

Most failures don't require the full recovery procedure:

- **Primary crashes, both replicas alive**: promote either replica (both have all acked events). The one with the longer journal avoids catch-up, but either is safe. The old primary reconnects as a replica and catches up.
- **One replica crashes, primary + other replica alive**: the cluster continues in degraded mode (`min(journal, repl)` gating). The crashed replica reconnects and catches up automatically. No operator action needed.
- **Middle node crashes** (regardless of role): the shortest node already has the missing entries in its replication pipeline — they are in-flight or being fsynced. The system continues with the two surviving nodes. No data loss, no operator action.

### Non-quorum mode

With `--no-quorum-durability`, every acked event is both locally fsynced and replicated. The primary's journal is always the longest or tied. Recovery is simpler: promote any replica, reconnect the old primary as a replica. No journal comparison needed.

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

The primary's raw genesis entry bytes (including the original timestamp) are sent in the `StreamStart` response. Fresh replicas write these bytes directly to the journal file, producing a byte-identical genesis entry. The BLAKE3 hash chain starts from the exact same encoded bytes. Subsequent entries are encoded independently by each node using pre-assigned sequences from the primary, keeping journals logically aligned.

### ~~Single replica only~~ (FIXED)

Dual replication is now supported — the primary accepts up to 2 concurrent replica connections, each with its own replication ring consumer and handler thread. If one replica fails, trading continues with the surviving replica. Trading halts only when all replicas disconnect. Per-slot acked cursors track each replica's position independently; the shared cursors are recomputed as `min(slot0, slot1)` and `max(slot0, slot1)` on every ack.


### `read_frame` partial read on timeout

**What**: The ack reader socket has a 1ms read timeout. If `read_exact` partially reads a frame header (e.g., 2 of 4 bytes) before the timeout fires, the next `read_frame` call starts mid-frame, permanently desynchronizing the stream.

**Impact**: Extremely unlikely with TCP (kernel buffers ensure complete small reads), but theoretically possible under extreme memory pressure or with pathological packet fragmentation.

**Mitigation**: The 1ms timeout is short enough that ack frames (9 bytes) arrive atomically in practice. If desync occurs, the decode will fail and the connection will be dropped and re-established.

**Resolution**: Use a `BufReader` wrapper that preserves partial reads across calls, or switch to non-blocking I/O with explicit read state tracking.

## Future Work

- **Chain hash verification** — see limitation above
- **Automatic failover**: Leader election / consensus for automatic promotion. Requires fencing to prevent split-brain. Manual promotion via `--promote-bind` is implemented.
- **Fully async replication**: Optional mode where the primary's response stage does not gate on the replication cursor at all — only on local fsync. Larger data-loss window than `--async-replica-ack` (which still gates on the replica having the data in RAM). Useful for venues that treat replication purely as a hot standby and accept any post-crash divergence.
- **Split-brain fencing**: After manual promotion, the old primary must be stopped manually. Automatic fencing (STONITH, epoch-based fencing) is not yet implemented.
