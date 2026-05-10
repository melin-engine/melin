# Replication Design Document

## Overview

Synchronous journal replication from a primary server to a replica, providing zero-data-loss failover capability. The primary streams journal entries to the replica over TCP; the replica persists them locally and acknowledges. With quorum durability (default), when 2 replicas are connected, client responses are gated on replication acknowledgement alone — the journal still writes for local crash recovery but does not block responses. When fewer than 2 replicas are connected, responses are gated on both local journal durability and replication. A client never learns about an event that isn't durably stored on at least two nodes.

## Architecture

```
Primary:
  Readers → Disruptor → JournalStage  (consumer 0) → disk + replication rings
                       → MatchingStage (consumer 1) → OutputSPSC

  JournalStage: encodes each input slot into both the journal-codec
  buffer (for disk) and a parallel InputBatch buffer (for the
  replication ring), then publishes the InputBatch buffer to two
  independent SPSC replication rings (one per replica slot).

  ReplicationSender threads: each consumes from its own ring,
  forwards the wire-ready InputBatch frames to its replica via
  io_uring SEND, receives acks via io_uring RECV, updates
  replication_cursor.

  ResponseStage gates on replication_cursor (quorum mode, 2 replicas)
  or min(journal_cursor, replication_cursor) (degraded/no-quorum mode)

Replica:
  TCP → ReplicationReceiver → decode InputBatch frames, publish slots
                                directly to disruptor (sequences and
                                timestamps already on the wire)
      → JournalStage encodes independently (same sequences as primary)
      → MatchingStage processes through own Exchange
      → ack sequence back to primary after journal cursor advances
```

### Replication rings and fault isolation

Each replica slot has its own independent ring buffer (configurable via `--replication-ring-size`, default 64 slots x 512 KiB = 32 MiB per ring, 64 MiB total for dual replication). The replicated bytes are wire-ready `InputBatch` frames produced by the primary's JournalStage at fsync time — no decode + re-encode on the sender. The replica decodes the frames straight back into `InputSlot`s, publishes them to its own pipeline with the primary's pre-assigned sequences and timestamps, and encodes its own journal independently. Journals are logically identical (same sequences, same events) but each node encodes independently.

**Fault isolation**: a slow replica only stalls its own ring, not the other replica's. If a ring is full for longer than 500ms (replica not keeping up), the primary automatically disconnects that replica and frees the ring. The slot becomes available for a new connection. The surviving replica and client trading are unaffected.

### Durability policy

The response stage gates client responses on a configurable
durability policy expressed against per-node, per-level cursors.
Two levels are tracked on each node:

- **`persisted`** — event written to NVMe via `O_DIRECT` `pwrite`. With
  power-loss-protection (PLP) capacitors on the device, the bytes
  survive power loss without an explicit fsync round-trip.
- **`in_memory`** — event accepted into the node's pipeline. Survives
  process crashes only as long as the kernel page cache survives —
  i.e. process death loses it. Useful as a "received this far"
  signal in cross-node policies.

A policy is one or more `<level>>=<n>[ best_effort]` clauses joined
with `&&`. The optional `best_effort` keyword marks the clause as
degrade-friendly: when fewer than `n` nodes are connected the count
clamps to the connected cluster shape rather than failing closed.
Strict clauses (without `best_effort`) leave the gate stalled when
the target can't be met — appropriate for fail-closed compliance
scenarios.

#### Policy examples

| Policy string | Meaning | When to use |
|---|---|---|
| `persisted>=1` | At least one node persisted (effectively single-node durability). | Standalone or single-replica deployments. |
| `persisted>=2` | Strict two-node quorum; gate stalls if a replica disconnects. | Compliance-driven venues that prefer halt-on-degrade over availability. |
| `persisted>=2 best_effort` (default) | Two-node quorum when healthy; clamps to surviving cluster on a partial failure. | Typical exchange deployments wanting strong durability with availability through single-replica failures. |
| `in_memory>=2` | Confirm receipt on a second node; weaker durability, removes fsync latency from the critical path. | Co-location latency-sensitive paths where the primary's PLP NVMe is the load-bearing durability and the replica is a hot standby for failover. |
| `persisted>=1 && in_memory>=2` | Local commit + cross-node receipt — primary persisted plus one other node has the event in RAM. | Cheap-but-non-zero cross-node durability target. |

Counts include the primary plus every connected replica. The
primary's `in_memory` cursor is treated as always satisfied — by
construction, the response stage only gates events past matching, so
the primary trivially has them in memory.

#### Strict vs degrade-friendly behaviour by cluster shape

| Cluster | Health | `persisted>=2` (strict) | `persisted>=2 best_effort` (degrade) |
|---|---|---|---|
| 1 primary + 2 replicas | All up | Need 2-of-3 persisted | Same: 2-of-3 persisted |
| 1 primary + 2 replicas | 1 replica down | Stalls (only 2 nodes connected, clause unsatisfiable except by both) — actually still satisfiable as 2-of-2; gate runs requiring **both** survivors to persist | Same: requires both survivors to persist — strictly stronger than the legacy auto-degrade-to-1-node behaviour in the same shape |
| 1 primary + 2 replicas | Both replicas down | Gate stalls; matching stage halts independently when `replicas_connected==0` so client orders are rejected with `ReplicaDisconnected` before reaching the gate | Same matching-stage halt; gate would clamp to 1-of-1 if any traffic reached it |
| 1 primary + 1 replica | All up | Need 2-of-2 persisted | Same: 2-of-2 persisted |
| 1 primary + 1 replica | Replica down | Same matching-stage halt as above | Clamps to 1-of-1 — gate opens at primary alone (matches legacy behaviour exactly in this shape) |
| Standalone (no replication wired) | n/a | Stalls (1 node, clause requires 2) | Clamps to 1-of-1 — primary alone |

The matching stage's halt at `replicas_connected==0` is independent
of the durability gate — it rejects new orders with `ReplicaDisconnected`
regardless of policy. Standalone deployments (no replication
configured) skip this halt entirely.

#### Observability

When a degrade-friendly clause is actively clamping below its target
count, two signals are emitted:

- A `warn!` log on transition into the degraded state and every 5
  seconds while it persists, plus an `info!` on return to target.
  The log line includes the active policy.
- The `melin_durability_policy_degraded` Prometheus gauge on the
  health endpoint flips to `1`. Operators should alert on this
  transitioning to `1` for sustained periods.

On-the-wire, every replica → primary `Ack` carries both cursors
(`acked_sequence` and `in_memory_sequence`), so the primary can
evaluate any combination of levels without separate ack streams.
See [Wire Protocol](#wire-protocol) below.

### Replication cursor behavior

The legacy global `replication_cursor` (min) and `fastest_replica_cursor`
(max) atomics are still computed for the health endpoint's
backwards-compatibility surface, but the durability gate reads the
per-slot cursors out of `ReplicationMetrics` directly via the policy
view. Disconnected slots are filtered out via per-slot active flags
rather than being represented as `u64::MAX` sentinel values.

## Wire Protocol

Length-prefixed frames, little-endian. Runs over a dedicated TCP connection separate from the client protocol.

### Replica → Primary

| Message | Layout | Purpose |
|---|---|---|
| Handshake | `[len:u32][type=0x01][last_sequence:u64][chain_hash:[u8;32]]` | Initial connection: replica reports its last durable sequence and chain hash |
| Ack | `[len:u32][type=0x02][acked_sequence:u64][in_memory_sequence:u64]` | Replica confirms persisted write up to `acked_sequence` and pre-journal receipt up to `in_memory_sequence`. Both fields populated on every ack so the primary's policy gate can evaluate any combination of durability levels. |

### Primary → Replica

| Message | Layout | Purpose |
|---|---|---|
| StreamStart | `[len:u32][type=0x10][start_sequence:u64][genesis_len:u32][genesis_entry_bytes...]` | Confirms handshake, includes raw genesis entry for byte-identical hash chain |
| NeedSnapshot | `[len:u32][type=0x11]` | Replica is too far behind; triggers snapshot transfer |
| SnapshotBegin | `[len:u32][type=0x13][snapshot_len:u64][snap_sequence:u64][snap_chain_hash:[u8;32]]` | Start of snapshot transfer with metadata |
| SnapshotChunk | `[len:u32][type=0x14][data...]` | Chunk of snapshot data (up to 64 KiB) |
| SnapshotEnd | `[len:u32][type=0x15][crc32c:u32]` | End of snapshot transfer with CRC32C for integrity |
| HashMismatch | `[len:u32][type=0x12]` | Chain hash doesn't match at the replica's reported sequence (not yet validated) |
| InputBatch | `[len:u32][type=0x21][count:u16][slot...]` | Batch of `InputSlot` records (sequence + timestamp + key/request hash + journaled event); divergence is verified at Checkpoint events inside the slot stream |
| Heartbeat | `[len:u32][type=0x30][sequence:u64]` | Periodic idle keepalive (5-second interval) advertising the primary's last published sequence |

### Design rationale

- **Input replication**: The primary streams `InputBatch` frames carrying the events the replica must apply. Each slot already includes the primary's pre-assigned sequence and timestamp, so the replica's pipeline produces a logically identical journal — same sequences, same events — without round-tripping through the journal codec on the wire. Each node encodes its own on-disk journal independently, enabling deterministic replay and independent verification.
- **Dual replication**: The primary accepts up to 2 concurrent replica connections, each with its own replication ring consumer and handler thread. If a replica disconnects, its slot becomes available for a new connection. Trading halts only when all replicas disconnect.

## Replica Mode

A server started with `--replica-of <primary_addr>` runs in replica mode:

- Authenticates with the primary via Ed25519 challenge-response (`--replication-key`).
- Connects to the primary and sends a `Handshake`.
- Receives `InputBatch` frames, publishes the slots to a local disruptor pipeline.
- Uses the same pipeline architecture as the primary (journal stage → matching stage → shadow stage), with the replication receiver feeding the input disruptor instead of the reader thread.
- The journal stage encodes events independently using the primary's pre-assigned sequences and timestamps (carried in each `InputSlot`). Each node produces its own journal — logically identical to the primary's but independently encoded.
- The matching stage processes events through its own `Exchange` independently, maintaining warm state for promotion.
- Sends `Ack` frames after the journal stage confirms durable write (cursor advance). Acks are pipelined: up to 8 batches can be submitted to the journal stage before the first ack is sent, overlapping NVMe writes with TCP receives. Each ack carries both `acked_sequence` (highest seq persisted on this replica) and `in_memory_sequence` (highest seq received pre-journal), so the primary's policy can gate on either level without a separate ack stream.
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

The receiver thread uses io_uring for TCP I/O: multishot RECV is always in-flight against a 16-buffer provided buffer pool for incoming `InputBatch` frames, and SEND is submitted when an ack becomes ready. The receiver decodes each `InputBatch` directly into `InputSlot`s and publishes them to the input disruptor with the primary's pre-assigned sequences and timestamps. Checkpoint events are filtered by the journal stage — each node auto-emits its own. The journal and matching stages consume events in parallel (same topology as the primary).

The pipelined ack queue (8 entries) decouples the receiver's TCP loop from NVMe write latency — the receiver can push up to 8 batches ahead while previous writes are in flight. Acks are sent as soon as the journal cursor confirms durability, checked on every event loop iteration with zero syscall overhead.

## CLI Flags

| Flag | Required | Default | Purpose |
|---|---|---|---|
| `--replication-bind <addr>` | No | — | Address to listen for replica connections |
| `--standalone` | No | `false` | Explicitly disable replication (dev/test) |
| `--replica-of <addr>` | No | — | Run as a replica connected to the given primary |
| `--replication-key <path>` | Replica | — | Ed25519 private key for replication auth. Required when `--replica-of` is set. The corresponding public key must be in the primary's `authorized_keys` with `replication` permission. |
| `--admin-bind <addr>` | Any | — | Address for the operator admin endpoint. Accepts `PROMOTE\n` (replica → primary, replica only) and `ROTATE\n` (archive the live journal segment). |
| `--durability-policy <STRING>` | Primary | `persisted>=2 best_effort` | Policy that gates client responses. See [Durability policy](#durability-policy) above. |

`--replication-bind` and `--standalone` are mutually exclusive. `--replica-of` is mutually exclusive with both. If none are specified, the server runs in standalone mode.

## Snapshot Transfer

When a replica is too far behind and the primary's journal archives have been purged (the needed entries no longer exist in any `.journal.N` file), the primary sends a `NeedSnapshot` message followed by a snapshot transfer:

1. **SnapshotBegin**: metadata frame with snapshot size, sequence, and chain hash.
2. **SnapshotChunk**: the snapshot data in 64 KiB chunks.
3. **SnapshotEnd**: CRC32C checksum of the entire snapshot payload for integrity verification.

The replica receives the chunks, writes the snapshot to a temporary file, verifies the CRC, atomically renames to the snapshot path, loads the snapshot into its Exchange, and resumes normal replication from the snapshot's sequence. This enables fresh replicas to bootstrap from a running primary without needing the full journal history.

The CRC32C is computed incrementally on the primary as chunks are read from disk, and verified incrementally on the replica as chunks are received — no need to buffer the entire snapshot in memory.

## Manual Promotion

A replica can be promoted to primary via the `--admin-bind` endpoint. The operator connects to the admin port, authenticates with an operator key, and sends `PROMOTE\n`. The replica:

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
- **One replica crashes, primary + other replica alive**: under the default `persisted>=2 best_effort` policy the cluster continues in degraded mode — the gate clamps to "primary + surviving replica both persist" (still 2 nodes, just out of 2 instead of 3). The crashed replica reconnects and catches up automatically. No operator action needed. Under a strict `persisted>=2` policy the gate stalls until the replica returns; pick the policy that matches your availability/compliance preference.
- **Middle node crashes** (regardless of role): the shortest node already has the missing entries in its replication pipeline — they are in-flight or being fsynced. The system continues with the two surviving nodes. No data loss, no operator action.

### Single-node-durability mode

With `--durability-policy "persisted>=1"`, every acked event is locally
durable on at least one node — typically the primary. The primary's
journal is always the longest or tied. Recovery is simpler: promote
any replica, reconnect the old primary as a replica. No journal
comparison needed. This corresponds to the legacy
`--no-quorum-durability` semantic.

## Current Limitations (v1)

These are known limitations of the current implementation. Each is documented here with the reason it was deferred and the plan for resolution.

### ~~No catch-up from journal files~~ (IMPLEMENTED)

When a replica connects, the primary reads its journal archive files, decodes the historical entries into `InputSlot`s, and streams them as `InputBatch` frames before switching to live ring data. The `RawJournalScanner` reads entry boundaries without full validation (no per-entry CRC check on the streaming path — the replica's own journal stage will rewrite and verify each entry) for efficient streaming. The replication ring is NOT consumed during catch-up — live data accumulates in the ring and overlapping entries are drained after catch-up completes.

This works for both reconnecting replicas (`last_sequence > 0`, catches up the gap) and fresh replicas (`last_sequence = 0`, streams the entire journal history). No operator intervention required — a new replica can join a running primary at any time.

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

**Mitigation**: The 1ms timeout is short enough that ack frames (21 bytes — 4-byte length prefix, 1-byte tag, two `u64` cursors) arrive atomically in practice. If desync occurs, the decode will fail and the connection will be dropped and re-established.

**Resolution**: Use a `BufReader` wrapper that preserves partial reads across calls, or switch to non-blocking I/O with explicit read state tracking.

## Future Work

- **Chain hash verification** — see limitation above
- **Automatic failover**: Leader election / consensus for automatic promotion. Requires fencing to prevent split-brain. Manual promotion via the `--admin-bind` endpoint (`PROMOTE\n`) is implemented.
- **Ack-on-receive**: re-introduce the legacy `--async-replica-ack`
  optimisation as the default by sending acks the moment a batch
  lands in the receiver's input ring (carrying the current
  `in_memory_sequence` plus whatever the journal cursor has
  reached for `acked_sequence`). With this in place, an
  `in_memory>=2` policy actually saves the ~50–80µs of NVMe
  latency the legacy flag did, without an operator-visible knob.
  Currently the receiver still gates ack send on the local
  journal cursor, so `in_memory>=N` policies parse correctly but
  produce the same end-to-end latency as `persisted>=N`.
- **Fully async replication**: Optional policy where the gate does not require any replica acknowledgement — equivalent to `persisted>=1` running on the primary alone. Useful for venues that treat replication purely as a hot standby and accept any post-crash divergence.
- **Split-brain fencing**: After manual promotion, the old primary must be stopped manually. Automatic fencing (STONITH, epoch-based fencing) is not yet implemented.
