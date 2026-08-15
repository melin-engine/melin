# Async journal flush — watermark flush thread (spec)

Status: **superseded, not implemented** (2026-08). Follow-up to
[journal-fsync-beat-2026-08.md](journal-fsync-beat-2026-08.md).

The design below moves only `fdatasync` off the journal thread, keeping
`pwrite` inline and handing over a watermark rather than bytes. It was
built as far as the executor seam and tested, and did not hold up. What
shipped instead splits the stage properly: the sequencing thread encodes
into a hand-off ring and never touches the device, while a disk thread
owns `pwrite`, `fdatasync`, rotation, and every cursor that means
durable (`crates/core/transport-core/src/journal_disk.rs`).

Kept for the reasoning that survived the change and still explains the
implementation's shape: why publication must happen *after* the sync and
from the thread that performed it, why ring progress has to move with
durability rather than with submission, why rotation needs a drain, and
why a dedicated spinning core is the right default. Read it for those;
do not read it as a description of the code.

## Motivation

The fsync-beat investigation proved a structural limit of the durability
gate: `hybrid` (`persisted>=1 && in_memory>=2`) can only mask durability
latency for work that has already been *dispatched*. The journal stage's
loop is sequential — claim batch → encode (+ replication publish) →
`pwrite` → `fdatasync` → publish cursors — so a stalled `fdatasync`
also stalls encoding and the replication feed. Everything arriving
during the stall sits in the input ring: not journaled, not shipped to
replicas, so the gate's `in_memory>=2` clause cannot advance and no
policy over cursors can open it. A 2 ms stall at 1 M orders/s strands
~2,000 orders; the client-side signature is p99 ≈ p99.99 in the frozen
windows.

The pre-zero fix removed every *known* fdatasync stall class on the
bench fleet (XFS CIL forces, staging log-force collisions). This spec
removes the *architecture's* dependence on fdatasync being
well-behaved: with the flush asynchronous to encoding, a local disk
stall on one node no longer freezes dispatch, and `hybrid`'s masking
becomes a structural guarantee rather than one conditioned on the
filesystem. That matters commercially — customer deployments will not
all be XFS + PLP + a tuned kernel.

## Design overview

Split the journal stage's sync point into two halves:

- **Journal thread (unchanged core)**: encode, replication publish,
  `pwrite` (`write_all_at`), then *submit a watermark* instead of
  calling `fdatasync`. Continues immediately with the next batch.
- **Flush thread (new)**: consumes the latest watermark, runs
  `fdatasync`, and publishes everything that today follows the flush
  inline — the durable wire-seq cursor, `FsyncState`, and the input-ring
  progress cursor.

Only `fdatasync` moves. `pwrite` stays on the journal thread
deliberately: post-prezero it is a near-pure page-cache memcpy (no
extent conversion, no stable-page waits on this hardware), and every
stall traced in the beat investigation's endgame was in `fdatasync`,
none in `pwrite`. Keeping `pwrite` inline avoids any data handoff — the
flush thread never sees bytes, only a watermark tuple.

This applies to the buffered writer's synchronous path
(`JournalStage::run_sync`) on primaries **and replicas** — the same
stage code runs on both. On a replica the benefit is symmetric: a local
disk stall no longer stops the receiver's in-memory acks (until the
ring fills), so the primary's `in_memory>=2` clause keeps advancing
through a replica-side stall too.

## The watermark cell

The handoff is a single latest-value cell, not a queue. `fdatasync` is
cumulative — it covers all data written before the call — so
intermediate watermarks can always be coalesced into the newest one.
This mirrors the `pending_ack: Option<Ack>` pattern in the replication
receiver transport, and gives group durability for free during a slow
flush: when the flush thread comes back, one `fdatasync` covers every
batch written meanwhile.

The cell carries what `publish_fsync_state` + `sync_point` need at
publication time, captured at submit time on the journal thread:

| Field | Source | Consumed for |
| --- | --- | --- |
| `journal_seq` | `writer.next_sequence() - 1` after the batch's `pwrite` | durable wire-seq cursor, `FsyncState.journal_seq` |
| `chain_hash` | `writer.chain_hash()` at the same point | `FsyncState.chain_hash` (shadow snapshots, replica handshakes) |
| `ring_progress` | the `progress` value the caller passes to today's `sync_point` | `consumer.set_progress` (via a shared `Sequence` handle) |
| `generation` | rotation epoch (see below) | stale-publication guard |

Publication semantics (the invariant the whole design hangs on):

1. Flush thread reads the cell → local copy `W`.
2. If `W.journal_seq` ≤ last published, idle (spin/park).
3. `fdatasync(fd)`.
4. Publish `W` — cursor stores, `FsyncState` seqlock write,
   `set_progress(W.ring_progress)`.

The sample happens **before** the sync call: `fdatasync` only
guarantees data dirtied before the call, so a watermark read after the
syscall returns could claim durability for bytes the sync never
covered. Publishing `W` (not a re-read) after the sync is the
correctness rule.

Cell implementation: the fields must be read atomically together
(`journal_seq`/`chain_hash` tearing would hand replicas a mismatched
handshake hash — the exact TOCTOU `FsyncState`'s seqlock already
exists to prevent). Reuse the existing seqlock
(`SeqLockWriter`/`SeqLockReader`, `NoPadding`) with a `Copy` tuple
struct; journal thread is the single writer, flush thread the single
reader.

### Ring progress moves with durability — this is load-bearing

`Consumer::set_progress` publishes the `processed` sequence that (a)
producers gate slot reuse on and (b) **the replica ack path gates
persisted acks on** (`PendingAckQueue::pop_oldest_blocking(journal_cursor,
…)` in the receiver). Today it advances after `flush_batch_sync`
returns — that ordering *is* persist-before-ack on replicas. The flush
thread must therefore own `set_progress`; publishing it at submit time
would let a replica ack entries its disk has not persisted.

Consequence: the journal thread's private `next_read` runs ahead of the
published `processed` cursor during a flush stall. That gap is bounded
by input-ring capacity, which becomes the natural backpressure bound —
when the ring fills, producers stall, exactly as today, but only after
absorbing a full ring's worth of orders instead of stalling on the
first blocked flush. No new buffering, no new memory bound, no new
config knob.

## Rotation protocol

Rotation stays on the journal thread (`maybe_rotate` at what is now the
submit point). It needs the old segment quiesced — a drain-then-swap:

1. Journal thread sets a `quiesce` flag (or submits a drain-marked
   watermark) and waits for the flush thread to publish up to the
   current watermark.
2. Flush thread completes its cycle, publishes, and acks the quiesce.
3. Journal thread runs the rotation as today
   (`rotate_segment_with_prepared` / `rotate_segment` — these already
   begin with a full flush of the old segment, which after the drain is
   a no-op or near-no-op), installs the new live `File`, bumps the
   watermark `generation`, and resumes.

The flush thread accesses the live segment's fd through a handle the
journal thread swaps at step 3 (e.g. `Arc<File>` republished through
the cell or alongside it). The `generation` field makes any
theoretically stale publication inert: the flush thread never publishes
a watermark whose generation doesn't match the fd it synced. In
practice the drain in step 1 means no cross-generation flush is ever in
flight; the generation check is a cheap belt-and-suspenders invariant,
not a load-bearing protocol step.

The rotation-adjacent wait cost is one in-flight fdatasync (~30–300 µs
steady-state post-prezero) — same as today's inline flush at the top of
`rotate_segment_inner`.

## Failure semantics

An `fdatasync` error on the flush thread is exactly as fatal as it is
inline today (ENOSPC, EIO — a broken journal). The flush thread:

1. stops publishing (cursors freeze — no ack can ever cover
   non-durable data; the gate holds, which is correct),
2. sets a poison flag (same pattern as the existing `journal_failed`
   `AtomicBool` the replica receiver watches),
3. parks.

The journal thread checks the poison flag once per loop iteration and
surfaces the stored error through the existing fatal-shutdown path, so
operators see the same `error!` + teardown as an inline failure, one
batch later. Shutdown joins the flush thread after the journal thread
exits (drain first if healthy, abandon if poisoned).

## `no-persist` mode

`no-persist` replaces the flush with `discard_batch_buf` and publishes
immediately. That stays inline on the journal thread — the flush
executor is simply not engaged. (The replication publish already runs
regardless; see `sync_point`'s comment about the replication-cursor
gate.)

## Scheduling and configuration

Default: **dedicated pinned core, pure busy-spin**, SCHED_FIFO like the
other pipeline threads. This is load-bearing for the hot path, not a
tuning nicety: the flush handoff sits on the critical path of **every
ack's persisted leg**, and spinning is what makes both directions of it
free —

- **Submit side (journal thread)**: with a spinning consumer, submit is
  *only* the seqlock store the journal thread already performs today.
  No futex wake, no syscall, ever. Any parking variant puts a
  conditional `futex_wake` (~1 µs syscall) back on the journal
  thread's loop — the exact thread the whole design exists to unburden.
- **Receive side (flush thread)**: detection latency is one cross-core
  cache-line transfer, ~100–200 ns, deterministic. A parked thread adds
  ~2–5 µs of scheduler wake latency *plus jitter* to the persisted
  cursor — and it adds it precisely at low/bursty load, where a single
  order's ack latency is most visible.

Core assignment: extend the `--cores` list with an 11th entry,
`journal-flush` (precedent: the optional 10th, `journal-prep`), and
give it a real core in the `Default` layout. Placement rule: same CCD
as the journal core — the watermark cell bounces between the two
threads on every batch, and cross-CCD adds ~100 ns+ per transfer.

Constrained-footprint fallback (explicit opt-out, not the default):
`journal-flush = 0` follows the established "0 = unpinned" convention
and switches the thread to spin-then-park (bounded spin window, futex
park, journal thread wakes it on submit when parked). This trades the
handoff guarantees above for a core, for deployments where the core
budget genuinely doesn't exist. The `compact` layout (embedded bench,
small boxes) uses this mode. Document the cost honestly in the operator
docs: parked-mode handoff is ~2–5 µs plus scheduler jitter on the
persisted-ack path.

## Observability

- The thread is **named** (`journal-flush`) and pinned — deliberately.
  Every diagnosis in the beat investigation came from thread-targeted
  off-CPU tracing of named threads; this design keeps the durability
  path traceable that way, which is a primary reason it was chosen over
  the io_uring alternative (see below).
- New health gauge: flush lag, `submitted journal_seq − published
  journal_seq` (0 in steady state; a growing value is a stalling disk
  that the pipeline is riding through).
- `latency-trace` feature: add a watermark-submit → publish histogram
  next to the existing journal wakeup/batch recorders — this is the
  direct measure of "what would have been an inline stall".
- Existing metrics (`melin_journal_rotations_total`, gate blocker
  counters, replica ack gauges) are unchanged.

## Expected performance effects

- **Steady state**: neutral to slightly positive. Handoff adds
  ~100–200 ns (spinning default; the opt-out parked mode pays ~2–5 µs
  plus jitter instead). In exchange, encode/replication of batch N+1
  overlaps the fsync of batch N, so the persisted cursor's cadence
  becomes fsync-duration-limited instead of
  (encode+pwrite+fsync)-limited, and the `in_memory` leg of `hybrid`
  advances earlier than today.
- **Under a disk stall**: the pipeline keeps encoding, journaling
  (page-cache), and replicating up to ring capacity. Under `hybrid`,
  acks continue via the replica leg (`persisted>=1` can be satisfied by
  a replica's persisted cursor). Under `local`, acks stall on the
  frozen persisted cursor — correct, and now visible as flush lag
  rather than as a frozen pipeline.

## Acceptance

1. **No regression**: LAN bench suite (`tcp-dual-repl`, throughput
   workload) — p99.9/p99.99/max at or below current main numbers
   (p99.9 ~242 µs, max ~387 µs at 1.23 M orders/s), fast rotations
   still 100 %, zero sync fallbacks, journal verification MATCH on both
   replicas.
2. **Masking works**: fault-injected slow fsync on the primary (test
   hook delaying the flush thread, or device-level delay) while under
   load — client latency time series shows *no* corresponding freeze
   under `hybrid`; flush-lag gauge shows the stall being absorbed;
   under `local` the same injection produces the (expected,
   documented) ack stall.
3. **Unit/property coverage**: watermark cell (sample-before-sync
   publication, coalescing, seqlock atomicity), rotation drain protocol
   (no publication with a stale generation, drain always terminates),
   poison propagation (no cursor advance after a failed sync; journal
   thread surfaces the original error), replica persist-before-ack
   (acks never precede `set_progress`).

## Relation to option C (io_uring linked pwrite→fdatasync)

An alternative mechanism reaches the same property with no extra
thread: submit each batch as linked SQEs (pwrite → fdatasync), keep
encoding, reap CQEs to advance cursors; a blocked fsync sleeps in an
io-wq kernel worker instead of on a pipeline core. It additionally
hedges `pwrite`-side stalls, which the watermark design deliberately
leaves inline.

C is **not** the target of this spec, and is kept in mind best-effort
only. It was set aside because: buffered io_uring writes can complete
short, turning the synchronous `write_all_at` loop into an async
resubmission state machine inside the durability core; io-wq workers
are anonymous, breaking the thread-targeted tracing methodology this
project's diagnostics depend on; and post-prezero there is no observed
`pwrite` stall class to hedge (0/74 k bad windows across three
validation runs).

What this spec asks of the implementation, for C's sake, is only a
clean seam: keep the executor boundary narrow — *submit watermark →
completion published to cursors → drain() → poisoned?* — so the flush
thread and a future uring backend are two implementations behind the
same contract. Everything mechanism-agnostic that this spec builds
(sample-before-sync publication semantics, ring-progress ownership,
rotation drain, poison path, flush-lag observability, the acceptance
harness) transfers to C unchanged; only the mechanism behind the seam
would be swapped. No abstraction should be added *speculatively* beyond
that boundary — if C never happens, the seam must still be the natural
shape of the thread design.

Revisit C if: a post-prezero trace ever shows `pwrite`-side blocking;
the product invests in io_uring more deeply anyway (e.g. NVMe
passthrough); or the dedicated flush core's footprint cost bites on
customer minimum specs — C reaches the same decoupling with no extra
core, so core-budget pressure is the strongest realistic trigger.

## Out of scope

- The sector writer and the io_uring journal path (`run_uring`) — the
  roadmap already leans toward retiring the sector writer; this spec
  touches only the buffered writer's synchronous path.
- Any change to durability *guarantees*: acks are still gated on the
  configured policy over persisted/in-memory cursors. This spec changes
  when cursors advance relative to a stalled disk, never what they
  mean.
- Snapshot/shadow stages: they consume `FsyncState` through the same
  seqlock as today and see the same post-durability values, merely
  published from a different thread.
