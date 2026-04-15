# Input Replication Plan

Migrate from output replication (primary streams encoded journal entries to replicas) to input replication (primary sequences commands and streams them to all nodes, each processes independently). This aligns with the LMAX architecture where every node runs the same deterministic computation over the same ordered input stream.

## Motivation

The current architecture replicates the primary's journal output. Replicas write those bytes verbatim and replay them, but never run the input-to-output business logic themselves. This has three problems:

1. **No independent verification.** A bug in journal encoding/decoding can silently corrupt replica state. With input replication, each node processes independently and divergence is detectable.
2. **Promotion gap.** On failover, the promoted replica has never validated its own output against the primary's. It has warm state, but that state was built by trusting the primary unconditionally.
3. **Not LMAX-canonical.** The LMAX architecture replicates inputs so that any node can become primary with zero ambiguity. Output replication is closer to traditional primary-backup.

## Target Architecture

```
Client → Primary (sequencer)
           ├─ assigns sequence number + wall-clock timestamp
           ├─ replicates sequenced input to replicas (before processing)
           └─ processes through own Exchange (same as replicas)

Replica:
           ├─ receives sequenced input stream
           ├─ processes through own Exchange (independently)
           ├─ journals its own output (self-encoded, not byte-copied)
           └─ acks input persistence to primary
```

Every node — primary and replica — runs the full pipeline: encoding, matching, journaling. The journals are no longer byte-identical (each node encodes independently), but the logical content is identical because the Exchange is deterministic.

## Migration Steps

### Step 1: Extract an Explicit Sequencer Stage

**Goal**: Make the "assign sequence + timestamp" step a separable, identifiable stage on the primary, distinct from journal encoding.

Today, sequencing is implicit — events get sequence numbers as part of journal encoding in `JournalStage`. Extract this so that:

- Input commands receive a sequence number and a canonical timestamp before they enter the disruptor.
- The sequence + timestamp travel with the command through the pipeline.
- `JournalStage` and `MatchingStage` consume already-sequenced inputs.

This is a refactor with no behavioral change. The replication stream still carries journal output after this step.

### Step 2: Replicate Sequenced Inputs Instead of Journal Output

**Goal**: Change the replication stream from encoded journal bytes to sequenced input commands.

- The replication ring payload changes from `&[u8]` (journal bytes) to sequenced input commands.
- The primary publishes to the replication ring after sequencing but before processing.
- The wire protocol `DataBatch` frame carries serialized input commands instead of journal entry bytes.
- The replica receives input commands, not journal bytes.

This is the core change. After this step, the replica has the raw inputs but still needs to process them.

### Step 3: Replicas Run the Full Exchange Pipeline

**Goal**: Replicas process inputs through their own Exchange instead of replaying journal output.

- Remove `write_raw_sync` path from replica `JournalStage`.
- Replica runs the same pipeline as primary: sequenced input → MatchingStage → JournalStage (encodes its own output).
- Replica journals are self-encoded (no longer byte-identical to primary, but logically identical).
- Ack semantics change: the replica acks after its own journal stage confirms durable write of processed output, not after writing raw bytes.

After this step, every node is a full state machine. Promotion means "start accepting clients" — the Exchange is already running independently.

### Step 4: Add Divergence Detection

**Goal**: Detect determinism bugs by comparing output across nodes.

- After processing each batch, each node computes a hash over its output (e.g., BLAKE3 over the encoded journal bytes for that batch).
- The primary includes its output hash in a new field on the replication heartbeat or a dedicated message.
- Replicas compare their own output hash against the primary's. A mismatch means a determinism bug — log, alert, halt.

This is the key benefit of input replication: independent verification that was impossible with output replication.

### Step 5: Update Catch-up and Snapshot Transfer

**Goal**: Adapt recovery protocols to work with the input log instead of the output journal.

- Catch-up streams sequenced input commands from the primary's input log (not journal entries).
- The primary must retain a durable input log (or be able to reconstruct inputs from its journal) for catch-up.
- Snapshot transfer is unchanged — it bootstraps Exchange state regardless of how inputs arrive.
- Fresh replicas receive a snapshot + live input stream, same as today.

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

The primary assigns sequence numbers and timestamps, then replicates. This is the simplest approach and matches the current topology. The tradeoff is that the sequencer is a single point of failure — but promotion is fast since replicas are already running full pipelines.

Consensus (Raft/Paxos) can be layered on later if customers demand automated leader election. The input replication infrastructure is a prerequisite for consensus regardless.

### Timestamps assigned at sequencing time

Wall-clock time is the primary source of non-determinism. The sequencer stamps each input command with a canonical timestamp before replication. All nodes use this timestamp, never their local clock, for any time-dependent logic (order expiry, throttling, etc.).

### Journals are no longer byte-identical

With output replication, journals are byte-for-byte copies. With input replication, each node encodes its own journal output. The logical content is identical (deterministic processing guarantees this), but the raw bytes may differ if encoding is sensitive to node-local state (it shouldn't be, but this is a subtlety to verify).

If byte-identical journals are needed for operational tooling, the encoding must be fully deterministic given the same logical input — which it should be by construction.

## Risks

- **Determinism bugs surface as divergence.** This is actually a feature — they're invisible today. But it means the divergence detection (step 4) should be implemented before or alongside step 3, not deferred.
- **Input log retention.** The primary needs to retain sequenced inputs for catch-up. Today it retains journal output. The input log may be smaller (commands without fills/acks) or larger (depends on encoding). Needs sizing analysis.
- **Performance regression during migration.** Steps 1-2 add a serialization step (input commands) that doesn't exist today. Profile to ensure this stays within the ~100ns budget.
