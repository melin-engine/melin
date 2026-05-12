# Input Replication — Complete

Migrated from output replication to input replication. Every node (primary and replicas) independently encodes and journals events. The primary stamps each input command with a wall-clock timestamp at ingress and assigns its journal sequence number in the journal stage at encode time, in disruptor cursor order. This aligns with the LMAX architecture where every node runs the same deterministic computation over the same ordered input stream.

## Motivation

The previous architecture replicated the primary's journal output. Replicas wrote those bytes verbatim and replayed them, but never ran the input-to-output business logic themselves. This had three problems:

1. **No independent verification.** A bug in journal encoding/decoding could silently corrupt replica state. Now each node processes independently and divergence is detected at every checkpoint (100K events).
2. **Promotion gap.** On failover, the promoted replica had never validated its own output. Now every replica runs the full pipeline — promotion is "start accepting clients."
3. **Not LMAX-canonical.** Sequences are now assigned by the journal stage in disruptor cursor order, matching the LMAX architecture.

## Target Architecture

```
Client → Primary
           ├─ stamps wall-clock timestamp at ingress
           ├─ assigns sequence number in the journal stage (cursor order)
           ├─ replicates sequenced input to replicas (post-journal)
           └─ processes through own Exchange (same as replicas)

Replica:
           ├─ receives sequenced input stream
           ├─ processes through own Exchange (independently)
           ├─ journals its own output (self-encoded, not byte-copied)
           └─ acks input persistence to primary
```

Every node — primary and replica — runs the full pipeline: encoding, matching, journaling. The journals are no longer byte-identical (each node encodes independently), but the logical content is identical because the Exchange is deterministic.

## Migration Steps

### ~~Step 1: Extract an Explicit Sequencer Stage~~ (DONE, then redesigned)

Separated `allocate_sequence()` from `encode_event()` on `SectorWriter`. Added `sequence` and `timestamp_ns` fields to `InputSlot`. JournalStage batch loops use the explicit two-step pattern.

Followup: the original implementation introduced a `Sequencer` type that producers called pre-publish. That created a window where a claimed sequence could fail to reach a slot (`try_publish` failure → journal gap on recovery). The `Sequencer` has since been retired; sequence allocation now happens inside the journal stage in disruptor cursor order. Producers publish `sequence: 0`; the journal stage either allocates from the writer (primary) or uses the wire-decoded value (replica).

### ~~Steps 2+3: Replicas Encode Independently~~ (DONE)

Combined Steps 2 and 3 into a single change. The wire format is unchanged — `JournalEvent` already contains only input commands, so the DataBatch payload is already a serialized input stream.

What changed:
- `submit_batch_to_pipeline` now populates `InputSlot.sequence` and `InputSlot.timestamp_ns` from decoded entries (previously ignored). Checkpoint events are filtered out.
- Replica JournalStage runs in normal encode mode (`run_sync`/`run_uring`) instead of `run_replica` raw-write mode.
- When `slot.sequence != 0`, the JournalStage uses the pre-assigned sequence and timestamp from the primary, keeping journals aligned.
- Removed the entire `RawBatch` infrastructure (~900 lines): `write_raw_sync`, `RawBatchSender/Receiver/Ring`, `run_replica`, `run_replica_uring`.

Every node now runs the full pipeline independently. Promotion means "start accepting clients."

**Known limitation**: journal rotation on the primary inserts a `GenesisHash` entry that may cause the replica's checkpoint counter to drift. Without rotation, sequences are guaranteed to align.

### ~~Step 4: Add Divergence Detection~~ (DONE)

Implemented checkpoint-based divergence detection. When the replica's JournalStage encounters a Checkpoint event from the primary (`slot.sequence != 0`), it compares the primary's `chain_hash` against its own `writer.chain_hash()`. A mismatch returns a fatal error, shutting down the pipeline. Fires every `CHECKPOINT_INTERVAL` (100K) events — deterministic, exact, zero false positives.

No new wire protocol messages needed — the primary's checkpoint entries already carry the chain hash in their event payload. They flow through `submit_batch_to_pipeline` to the disruptor, where the JournalStage verifies but does not encode them (each node auto-emits its own checkpoints).

### ~~Step 5: Update Catch-up and Snapshot Transfer~~ (NO-OP)

No code changes needed. The journal already contains only input commands (`JournalEvent` has no output events — see `event.rs:1-5`), so catch-up already streams the input log. `submit_batch_to_pipeline` handles catch-up DataBatch frames identically to live ones — decoding, publishing with pre-assigned sequences, and verifying checkpoints. Snapshot transfer sends Exchange state (not journal state) and works unchanged.

## What Stays the Same

- **Transport layer**: io_uring TCP, DPDK — unchanged, different payload.
- **Replication rings**: same lock-free producer/consumer, different data.
- **Ack pipelining**: same mechanism, acking input (or output) persistence.
- **Quorum durability**: same formula, same response gating. The definition of "durable" shifts from "replica wrote raw bytes" to "replica processed and journaled."
- **Auth, reconnection, exponential backoff**: unchanged.
- **Snapshot transfer**: unchanged (bootstraps Exchange state).
- **Promotion listener**: unchanged (but promotion is simpler — replica is already a full node).

## Key Design Decisions

### Primary-as-sequencer (not consensus)

The primary assigns sequence numbers (in the journal stage, in disruptor cursor order) and ingress timestamps, then replicates. This is the simplest approach and matches the current topology. The tradeoff is that the primary is a single point of failure — but promotion is fast since replicas are already running full pipelines.

Consensus (Raft/Paxos) can be layered on later if customers demand automated leader election. The input replication infrastructure is a prerequisite for consensus regardless.

### Timestamps assigned at ingress

Wall-clock time is the primary source of non-determinism. The primary stamps each input command with a canonical timestamp at ingress (the reader / DPDK poll thread) before publishing into the input ring. That timestamp is journaled and shipped to replicas; all nodes use this timestamp, never their local clock, for any time-dependent logic (order expiry, throttling, etc.).

### Journals are no longer byte-identical

With output replication, journals are byte-for-byte copies. With input replication, each node encodes its own journal output. The logical content is identical (deterministic processing guarantees this), but the raw bytes may differ if encoding is sensitive to node-local state (it shouldn't be, but this is a subtlety to verify).

If byte-identical journals are needed for operational tooling, the encoding must be fully deterministic given the same logical input — which it should be by construction.

## Risks

- **Determinism bugs surface as divergence.** This is actually a feature — they're invisible today. But it means the divergence detection (step 4) should be implemented before or alongside step 3, not deferred.
- **Input log retention.** The primary needs to retain sequenced inputs for catch-up. Today it retains journal output. The input log may be smaller (commands without fills/acks) or larger (depends on encoding). Needs sizing analysis.
- **Performance regression during migration.** Steps 1-2 add a serialization step (input commands) that doesn't exist today. Profile to ensure this stays within the ~100ns budget.
