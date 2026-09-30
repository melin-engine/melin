# Replica reconnect re-applying history (plan)

Status: **proposed, not started** (2026-09). Fixes findings 1 and 2 of
the [determinism audit](determinism-audit-2026-09.md), plus the
sequence half of finding 37, as the audit's suggested order groups them.
Part of the roadmap item "Fix the determinism and durability audit
findings" ([roadmap.md](roadmap.md)). Read the audit entries for the
reproductions; this document records the design decision and the order
of work.

The one-line goal: **a replica's reconnect handshake claims exactly the
state the replica holds, and its journal refuses any sequence that is not
the next one, in every build.**

## What the code shows

Re-checked against `main` after the audit; the replication and journal
code is unchanged since the audited commit.

- **The handshake pair comes from an unseeded seqlock.**
  `setup_chain_hash_publisher` creates the `FsyncState` seqlock as
  `FsyncState::default()`: sequence 0, zero chain hash. Only the disk
  thread stores to it, after a durable batch. On every reconnect after
  the first, the kernel-TCP receiver (`run_receiver` in
  `tcp_receiver.rs`) and the DPDK receiver (`dpdk.rs`) read the handshake
  pair from that seqlock. Until the rebuilt pipeline's first durable
  batch, the replica therefore claims to hold nothing, and the primary
  streams from sequence 1 (finding 1).
- **Durable is not what the replica holds.** Once seeded by a real
  batch, the seqlock still describes the durable prefix only. Entries the
  previous session published into the input ring but the journal has not
  written are applied by the matching stage regardless. Most session
  exits skip `drain_pending_acks` (a `poll_recv` error, which is every
  kernel-TCP disconnect; a `send_ack` error; a `StreamGap`), so the next
  handshake can claim less than the ring holds and the primary re-streams
  the difference (finding 2).
- **The contiguity gate skips re-delivery silently.** The streaming
  gate in `receiver_transport.rs` treats `seq <= accum` as idempotent
  re-delivery and drops it. That rule exists for the catch-up to live
  handoff, where the primary re-carries slots it already sent. It is why
  the design below does not simply anchor the gate higher.
- **Sequence checks are debug-only.** The replica journal stage stamps
  `slot.sequence` verbatim through `set_next_sequence(seq + 1)`, in both
  the run loop and the shutdown drain. `set_next_sequence` and
  `JournalEncoder::encode_event` guard against backward moves with
  `debug_assert!` only; `last_encoded_seq` does not exist in release
  builds, and `begin_segment` resets it to 0. In release, a re-streamed
  history lands in the journal after its tail and recovery then fails on
  `SequenceDuplicate` (finding 37).
- **The fallback branch is dead on replicas.** Both receivers fall back
  to the durable wire-seq cursor when `chain_hash_lock` is `None`, but
  `build_replica_pipeline` always creates the lock. The field is an
  `Option` only to mirror the primary, where it follows the shadow
  setting.

## Decisions

1. **Seed the seqlock at build time, on both builders.** The initial
   `FsyncState` is the writer's `(next_sequence - 1, chain hash)` at ring
   position 0. The shadow stays safe: its `has_events` guard blocks a
   snapshot before the first consumed batch, and after one its
   `next_read` is past 0, so the seeded pair can never pass the
   alignment check. Seeding the primary too keeps one behaviour for one
   type; nothing on the primary relies on the zero value.

2. **Wait for the journal to cover the ring, then read the pair.** Before
   each reconnect handshake, the receiver waits until
   `FsyncState.input_ring_seq` equals the input producer's cursor, then
   takes `journal_seq` and `chain_hash` from that same load. The pair then
   describes everything the replica holds, and the primary's existing
   handshake validation checks the chain at exactly that point.

   The pair must also be durable, not merely encoded. The primary takes
   the handshake's `last_sequence` as the replica's stream base and
   seeds that replica's acked and in-memory ack cursors from it
   (`seed_on_handshake`), so whatever the replica claims counts as
   already on its disk. That rules out a cheaper variant that publishes
   an encoded-but-not-durable pair from the journal-seq thread to avoid
   waiting on the disk.

   The audit's first suggestion, carrying the accepted position across
   the reconnect and anchoring the gate at the larger of it and the
   handshake position, is rejected. The handshake would still claim the
   durable position, so the primary would re-stream the range between
   the two, and the gate would drop it as re-delivery. After a failover
   the new primary's entries in that range can differ from what the
   replica accepted from the old one, and the fork would go unnoticed
   until the next rotation announce or chain check. Waiting surfaces it
   at the handshake.

   Cost: a replica whose disk is stalled cannot reconnect until the disk
   catches up. It could not ack anything in that state either. The wait
   aborts on `journal_failed`, shutdown and promotion, so it cannot wedge
   the loop. It sits on the reconnect path, not the hot path, so it
   polls with a short sleep, and it logs once if it runs long.

3. **Refuse a non-contiguous stamped sequence in release.** On a replica
   the journal stage requires `slot.sequence == encoder.next_sequence()`
   and fails with a new typed `JournalError` variant otherwise, at both
   stamping sites. That replaces `set_next_sequence`, which leaves the
   `JournalWrite` trait if nothing else calls it. The encoder keeps
   `last_encoded_seq` in every build and returns an error from
   `encode_event` on `seq <= last_encoded_seq`: one comparison per entry.
   It updates `last_encoded_seq` only after the capacity check, unlike
   today's debug code, so that a refused entry still leaves the encoder
   able to re-encode it, as its docs promise. `begin_segment` stops
   resetting it; the monotonic-time plan needs the same for its timestamp
   floor, which will sit beside this check.

   The shutdown drain logs an encode error and moves on to the next slot
   today. A sequence refusal there must stop the drain instead: skipping
   one entry and encoding the next after it is exactly the corruption
   this check exists to prevent.

   A new `JournalError` variant is a breaking change for a published
   crate. Neither this workspace nor exchange-core matches on it
   exhaustively, and exchange-core does not implement `JournalWrite`,
   so nothing breaks today; the CHANGELOG records the change.

4. **A sequence refusal stops the process.** The error routes through
   `SessionExit::Fatal`, and `handle_session_exit` returns it rather than
   taking the in-process divergence resync, which stays reserved for
   `ReplicaChainDivergence`. A refusal means a bug in this node, not a
   fork from the primary, and the refused entry never reached disk, so a
   restart recovers a clean journal.

   The matching stage is gated on the producer, not on the journal, so
   it may already have applied the refused entry in memory when the
   journal stage stops. That state dies with the process. The shadow is
   gated on journal progress, which never covers the refused entry, so
   it never reads it and no snapshot can hold it.

## Order of work

Each step lands as its own commit with its tests.

1. **Seed and wait** (decisions 1 and 2).
   - Pass the seed into `setup_chain_hash_publisher` from both
     builders.
   - Make `chain_hash_lock` non-optional on `ReplicaPipeline` and
     `ReplicaPipelineHandles`, and delete the dead fallback in both
     receivers. Drop the handles' `last_seq` field if nothing else reads
     it.
   - Add one helper in `replication/mod.rs` that waits for coverage and
     returns the pair, and run it inside `handle_session_exit`'s two
     reconnecting arms (`Disconnected` and `StreamGap`), after the
     backoff: they are the only exits that keep the pipeline, and
     nothing publishes into its ring until the next session, so the
     pair stays exact across failed connects. `AfterSession::Reconnect`
     carries it back to the receiver. A journal stage found dead while
     waiting takes the same teardown as the `Fatal` arm (one shared
     function), so a `ReplicaChainDivergence` that lands while
     disconnected still takes the in-process resync.
   - Update the comments that say a reconnect resumes "from the durable
     position" (`handle_session_exit`'s `StreamGap` arm among them).
2. **Release-mode sequence enforcement** (decisions 3 and 4). Tests
   that feed a replica stage non-contiguous sequences, if any, get fixed
   rather than the check loosened. The shutdown drain fails closed on
   any failure, not only a refused sequence, as the steady-state loop
   does; that also fixes audit finding 24.
3. **Docs.** A `Fixed` entry under `[Unreleased]` in the CHANGELOG, a
   `Changed` entry for the new `JournalError` variants, and a status
   line on findings 1, 2 and 37 in the audit. The audit text stays
   as the record.

## Tests

Ported from the reproductions on `scratch/determinism-repro`, now as
ordinary tests beside the code they cover.

- **transport-core, replica pipeline:** on a writer recovered at
  sequence 3, the handshake pair read from `chain_hash_lock` right after
  `build_replica_pipeline` is `(3, recovered chain)`. Publishing
  sequences 1 to 3 again is refused, the journal still holds a single 1
  to 3, and it recovers.
- **journal, encoder:** a backward or repeated sequence is an error in
  release builds too, including after `begin_segment`. An entry refused
  for lack of space can then be encoded into a fresh destination.
- **transport-core, shutdown drain:** a refused sequence stops the drain;
  no later slot is encoded.
- **server-runtime, wait helper:** with slots published and the journal
  stage held back, the helper does not return; once the stage runs, it
  returns the pair covering every published slot. It returns promptly on
  `journal_failed`, shutdown and promotion, and a journal failure during
  the wait reaches `handle_session_exit`'s `Fatal` arm.
- **server-runtime, end to end:** the counter scenario from the audit
  (primary under `two-disks` plus one replica, increments 1, 2 and 4,
  restart the replica, restart the idle primary, promote the replica,
  read the counter) reads 7 in both debug and release.

Finding 2 gets no end-to-end test: it needs a replica disk that stalls on
demand. The wait helper's test covers the mechanism.

## Out of scope

- **The primary's acceptance of a sequence 0 handshake.** Harmless once
  replicas stop claiming it wrongly; a fresh replica still needs it.
- **The rest of finding 37:** `from_halves` with a non-empty batch,
  `resume` and `begin_segment` sequence agreement, the format-15
  `sector_size` check, and the writer/reader chain equality at recovery.
