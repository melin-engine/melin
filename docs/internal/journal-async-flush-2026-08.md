# Async journal flush — watermark flush thread (spec)

Status: **proposed** (2026-08), revised after a code audit. Follow-up to
[journal-fsync-beat-2026-08.md](journal-fsync-beat-2026-08.md). Not yet
implemented.

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

`sync_point` currently welds together two cadences that have no reason
to be the same:

1. **the dispatch cadence** — the replication publish (what feeds
   `in_memory>=2`) and the disk write (`pwrite`), which together bound
   journal-stage syscall cost and replication frame size,
2. **the durability cadence** — the flush (`fdatasync`), which gates
   every `persisted` clause.

Splitting them is the whole design:

- **Dispatch self-clocks against the flush.** The journal thread
  encodes continuously and fires a *submit* — publish the accumulated
  `InputBatch` frame to the replication rings, `pwrite`, hand off a
  watermark — when the flush executor is idle, when
  `pending >= max_batch`, or when the accumulated batch reaches
  `group_commit_delay` in age. It never blocks on the flush.
- **The flush moves to its own executor.** After `pwrite` the journal
  thread *submits a watermark* instead of calling `fdatasync`, and
  continues immediately. A flush thread consumes the latest watermark,
  runs `fdatasync`, and publishes everything that today follows the
  flush inline — the durable wire-seq cursor, `FsyncState`, the
  advertised journal tip, and the input-ring progress cursor.

`fdatasync` is the only I/O that leaves the journal thread. `pwrite`
stays inline deliberately: post-prezero it is a near-pure page-cache
memcpy (no extent conversion, no stable-page waits on this hardware),
and every stall traced in the beat investigation's endgame was in
`fdatasync`, none in `pwrite`. Keeping `pwrite` inline avoids any data
handoff — the flush thread never sees bytes, only a watermark tuple.
(`dir_fsync_retry.poll()` at `buffered_writer.rs:335` also stays
inline: it is a directory-fsync retry on the post-rotation path only,
rare and bounded, and moving it would put a second fd on the flush
thread for no benefit.)

This applies to the buffered writer's synchronous path
(`JournalStage::run_sync`) on primaries **and replicas** — the same
stage code runs on both. The replica-side benefit is real but much
smaller than the primary-side one, and the reason is worth stating
plainly rather than discovering during the bench: see *Replica-side
limit* below.

## Cadence decoupling

### Why dispatch must self-clock

Today the fsync duration *is* the group-commit window. `group_commit_us`
defaults to `0` (`server.rs:458`), so `should_sync` is true on every
iteration and the only thing fattening batches is that the journal
thread is blocked in `fdatasync` while the ring fills behind it. The
next `read_batch` then returns a large batch. Remove the block and the
thread free-runs: **read batches shrink to whatever one encode pass
takes**, roughly an order of magnitude at bench rates. Anything keyed
to the read batch inherits that shrinkage. The consequences all land on
the "no regression" criterion:

- more `pwrite` syscalls per event, on the exact thread this design
  exists to unburden;
- proportionally more `InputBatch` frames into a 256-entry replication
  ring (`server.rs:479`) whose overflow policy is immediate eviction
  (`pipeline.rs:1102-1113`) — a higher frame rate runs closer to that
  edge;
- more per-frame parse and `recvmsg` work on the replica receiver,
  which is the measured throughput ceiling in `tcp-dual-repl`;
- a smaller in-flight window against the replica's 8-entry pending-ack
  queue, which is counted in publish rounds rather than events.

This is why the publish rides the submit rather than the read batch.
Keying it to the read batch would produce exactly the frame flood
listed above — self-clocking paces `pwrite`, and would do nothing for a
publish clocked off a different trigger.

### The submit trigger

```
submit when:  flush executor idle
           || pending >= max_batch                     (size bound)
           || first_write_ts.elapsed() >= group_commit_delay   (age bound)
```

The first two clauses reproduce today's behaviour. At low load the
executor is always idle, so submit is immediate and latency is minimal.
Under fsync-bound load the executor is busy and the thread accumulates
to `max_batch` (1024, `server.rs:476`) — **the same batch-size
distribution as today**, because today's inline flush is itself a
self-clock. Accumulation is bounded by `max_batch`, well inside
`MAX_JOURNAL_BATCH` (4096, `pipeline.rs:197`), so `batch_buf` gains no
new growth path.

The age bound is what the size bound alone cannot give. During a stall
the executor stays busy, so a size-only trigger would advance the
`in_memory` leg in 1024-event steps — quantizing ack latency to
roughly a millisecond at bench rates, in exactly the window this design
exists to protect. Bounding batch *age* keeps the replication feed
flowing at a latency the tail budget can absorb while keeping frames
fat. This is the existing `group_commit_delay`
(`pipeline.rs:948-950`), reused rather than duplicated.

**Semantic shift to note:** `group_commit_us = 0` currently means
"submit on every iteration". Under this trigger it means "no age
bound", with the `flush idle` clause covering the low-load case that
`0` exists to serve today. The default therefore stops being obviously
correct — pick it from the acceptance runs (criterion 2 measures frame
size, criterion 3 measures stall-window ack latency; the default is
whatever satisfies both) rather than asserting one here.

Two follow-on edits: `maybe_publish_chain_check` counts
`batches_since_chain_check`, which now counts submits rather than sync
points; and `sync_point`'s doc comment about the replication-cursor
gate under `no-persist` moves with the publish.

### Replica-side limit

The claim that a replica rides out a local disk stall "until the ring
fills" does **not** hold as the receiver stands. The binding constraint
is not the 2²⁰-slot input ring but the pending-ack queue: on
`pending_acks.is_full()` the streaming loop blocks in
`pop_oldest_blocking(journal_cursor, …)`
(`receiver_transport.rs:734-752`) on the very cursor the flush thread
deliberately freezes during a stall, and that queue holds
`DEFAULT_REPLICATION_PIPELINE_DEPTH = 8` entries (`server.rs:79`).

So a replica absorbs 8 publish rounds' worth of a local stall and then
stops receiving entirely. With this spec's fat frames that is ~8 ×
`max_batch` events rather than ~8 × today's smaller frames, which is a
genuine improvement, but it is bounded by queue depth and nowhere near
ring capacity. The primary's `in_memory>=2` clause therefore keeps
advancing through a *short* replica-side stall, not an arbitrary one.

Two ways to lift it, both out of scope here and neither required for
the primary-side win:

- raise `--replication-pipeline-depth` (already an operator knob,
  power-of-two, trades replica memory for absorption); or
- coalesce into the queue tail when full instead of blocking. Acks are
  cumulative, so this only coarsens persisted-ack granularity, and it
  moves the backstop to the input ring — which is what the original
  claim assumed all along.

The second is the real fix and would make the replica side genuinely
symmetric with the primary side. Spec it separately, with the question
of what then bounds replica memory answered explicitly.

## The watermark cell

The handoff is a single latest-value cell, not a queue. `fdatasync` is
cumulative — it covers all data written before the call — so
intermediate watermarks can always be coalesced into the newest one.
This mirrors the `pending_ack: Option<Ack>` pattern in the replication
receiver transport, and gives group durability for free during a slow
flush: when the flush thread comes back, one `fdatasync` covers every
batch written meanwhile.

The cell carries everything `publish_fsync_state` + `sync_point` need
at publication time, captured at submit time on the journal thread.
**All five fields are required** — the flush thread has no access to
the writer or the consumer and cannot re-derive any of them:

| Field | Source | Consumed for |
| --- | --- | --- |
| `journal_seq` | `writer.next_sequence() - 1` after the batch's `pwrite` | durable wire-seq cursor, `FsyncState.journal_seq`, `advertised_tip.advance` |
| `chain_hash` | `writer.chain_hash()` at the same point | `FsyncState.chain_hash` (shadow snapshots, replica handshakes) |
| `input_ring_seq` | `consumer.next_read()` at the same point | `FsyncState.input_ring_seq` (shadow snapshot alignment) |
| `ring_progress` | the `progress` value the caller passes to today's `sync_point` | `consumer.set_progress` (via a shared `Sequence` handle) |
| `generation` | rotation epoch (see below) | stale-publication guard |

`input_ring_seq` and `ring_progress` are **not** the same value and
must both be carried. They coincide at the steady-state sync point,
where the caller passes `consumer.next_read()`, but diverge at a
replica's mid-batch mark barrier, where `ring_progress` is
`read_start + stop` while `next_read` already spans the whole read
batch. The shadow snapshot compares `input_ring_seq` for *exact
equality* against its own cursor (`shadow.rs:153`); substituting
`ring_progress` there would produce a snapshot whose header claims a
`journal_seq` behind the app state it contains, and a restore would
re-apply events already folded into that state.

`advertised_tip.advance` (`pipeline.rs:1201-1208`) moves to the flush
thread with the rest of `publish_fsync_state`. This is the correct
semantics — Raft vote filtering should advertise a durable tip — but it
is a behavioural change worth naming: on a primary the advertised tip
now trails by one flush.

Publication semantics (the invariant the whole design hangs on):

1. Flush thread reads the cell → local copy `W`.
2. If `W.journal_seq` ≤ last published, idle (see *Scheduling*).
3. `fdatasync(fd)`.
4. Publish `W` — cursor stores, `FsyncState` seqlock write,
   `advertised_tip.advance`, `set_progress(W.ring_progress)`.

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

`Consumer::set_progress` (`ring.rs:681`) publishes the `processed`
sequence that (a) producers gate slot reuse on and (b) **the replica
ack path gates persisted acks on**: `pop_oldest_blocking(journal_cursor,
…)` in the receiver (`receiver_transport.rs:752`) blocks on
`journal_ring_arc()` (`replication/mod.rs:572`), the journal consumer's
`processed` counter, comparing it against each pending ack's
`journal_target` (`ack_queue.rs:104`). Today it advances after
`flush_batch_sync` returns — that ordering *is* persist-before-ack on
replicas. The flush thread must therefore own `set_progress`;
publishing it at submit time would let a replica ack entries its disk
has not persisted.

Consequence: the journal thread's private `next_read` runs ahead of the
published `processed` cursor during a flush stall. That gap is bounded
by input-ring capacity (`INPUT_RING_CAPACITY`, 2²⁰ slots), which
becomes the natural backpressure bound — when the ring fills, producers
stall, exactly as today, but only after absorbing a full ring's worth
of orders instead of stalling on the first blocked flush. No new
buffering, no new memory bound, no new config knob.

The matching stage is unaffected: it is a *parallel* consumer of the
input ring gated on the producer, not on the journal
(`pipeline.rs:3376-3377`). Only the shadow consumer is chained behind
journal progress (`pipeline.rs:3379`), and holding it back through a
flush is precisely what keeps its snapshot alignment honest.

### Single writer to `processed`

`Consumer::commit` (`ring.rs:668`) also stores to `processed`, and
`run_sync` reaches it from **three** sites, all on shutdown paths:

| Site | Context |
| --- | --- |
| `pipeline.rs:805` | shutdown flag observed with `pending > 0` |
| `pipeline.rs:1018` | `Shutdown` sentinel seen in the batch |
| `pipeline.rs:1759` | inside `drain_remaining`, per drained batch — reached from `pipeline.rs:807`, i.e. immediately after the first site |

Once the flush thread owns publication these must go, or the guarantee
breaks at shutdown: `commit` publishes `next_read`, which under async
flush can sit arbitrarily far ahead of the last synced watermark — a
straight ack-before-persist on a replica. All three become "drain the
flush executor, let it publish, then exit". `drain_remaining` flushes
inline per batch and can stay synchronous once the executor is drained,
but it must not be left publishing on its own.

The invariant to hold and to test: **exactly one thread ever stores to
the journal consumer's `processed` counter, and under a durable build
it is the thread that just returned from `fdatasync`.** Under
`no-persist` there is no flush executor and the journal thread
publishes inline, which is the one legitimate exception — the
single-writer half of the invariant still holds, the post-`fdatasync`
half does not apply.

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

The flush thread accesses the live segment's fd as a **`RawFd` carried
in the cell**, not an `Arc<File>` — the cell is a `Copy` payload bound
by `NoPadding`, and `Arc<File>` is neither. Lifetime is safe by the
drain protocol rather than by refcount: the journal thread only swaps
the `File` at step 3, after the flush thread has published and
quiesced, so the fd in the cell is always open for the duration of any
sync that observes it. The `generation` field makes any
theoretically stale publication inert: the flush thread never publishes
a watermark whose generation doesn't match the fd it synced. In
practice the drain in step 1 means no cross-generation flush is ever in
flight; the generation check is a cheap belt-and-suspenders invariant,
not a load-bearing protocol step.

The rotation-adjacent wait cost is one in-flight fdatasync (~30–300 µs
steady-state post-prezero) — same as today's inline flush at the top of
`rotate_segment_inner`.

### Writer surface

`BufferedWriter::flush_batch_sync` (`buffered_writer.rs:332`) is
`dir_fsync_retry.poll()` + `ensure_allocated()` + `write_all_at` +
`sync_data()` + `write_pos`/`batch_len` advance. The split keeps
everything except `sync_data()` on the journal thread. The writer
therefore keeps sole ownership of its `File`, `write_pos`, `batch_len`
and chain; the flush thread only ever sees the `RawFd` the cell
carries, whose validity the rotation drain guarantees.

Expose this as a `JournalWrite` method that performs the write and
returns the watermark, rather than as a `BufferedWriter`-only API — the
sector writer keeps a synchronous implementation behind the same
signature, and the future uring backend drops into the same seam.

## Failure semantics

An `fdatasync` error on the flush thread is exactly as fatal as it is
inline today (ENOSPC, EIO — a broken journal). The flush thread:

1. stops publishing (cursors freeze — no ack can ever cover
   non-durable data; the gate holds, which is correct),
2. sets a poison flag (same pattern as the existing `journal_failed`
   `AtomicBool` the replica receiver watches),
3. stops syncing and falls into its normal `idle_wait` loop, watching
   only the shutdown flag.

Step 3 matters for shutdown: because the thread idles rather than
parking, it stays joinable in every state without a dedicated unpark or
exit signal. Shutdown always joins it after the journal thread exits —
draining first if healthy, and joining immediately without a drain if
poisoned, since a poisoned executor will never publish again.

The journal thread checks the poison flag once per loop iteration and
surfaces the stored error through the existing fatal-shutdown path, so
operators see the same `error!` + teardown as an inline failure, one
batch later.

During the one-iteration poison window the journal thread keeps
publishing to the replication rings. That is not a durability
violation — no `persisted` clause can be satisfied by the primary once
its cursor freezes — but it does mean a dead primary disk ships a
little further than it did inline. Under `hybrid` the `persisted>=1`
clause can still be satisfied by a *replica's* persisted cursor, so
acks continue until the journal thread tears the pipeline down. That is
the documented meaning of `hybrid`, not a regression; it is called out
here because the async flush makes the primary's own persisted cursor
lag more often, so `hybrid` leans on the replica leg more often than it
did.

## `no-persist` mode

`no-persist` replaces the flush with `discard_batch_buf` and publishes
immediately. That stays inline on the journal thread — the flush
executor is simply not engaged, so the submit trigger's `flush idle`
clause is always true and dispatch reduces to today's per-iteration
behaviour. The replication publish still runs regardless, now from the
submit rather than from `sync_point`; it is the reason the discard path
cannot simply skip everything downstream of the write.

## Interaction with the control plane

Raft carries election, membership and fencing epochs only — no journal
entry ever enters the raft log, so raft's log-matching properties say
nothing about journal recency. That job belongs to the **journal-tip
vote recency filter** (`crates/core/raft/src/recency.rs`): every RPC
envelope carries `(fencing epoch, advertised sequence)`, and a voter
drops vote requests from candidates behind its own tip. This spec
touches one input to that filter, so the effect is worth recording
rather than re-derived later.

The advertised tip is role-dependent, and only the primary's moves:

- **Replica** — the receive loop advances it to the in-memory
  *accepted* position (`receiver_transport.rs:611-614`), deliberately
  ahead of the fsynced position because a promotion drains the ring
  into the journal. The flush thread is not on that path; unchanged.
- **Primary** — the journal stage publishes its durable cursor
  (`pipeline.rs:1201-1208`), which now trails by one flush.

A primary therefore advertises slightly *less* than it holds. That is
the understating direction: it can only lose a vote it might have won,
never win one it should have lost, so the filter's safety argument is
untouched. The promotion refusals in `auto_promotion_decision` key off
link state, durability mode and epochs — none of which this spec
touches.

**Why the wider shipped-ahead window is not a fork risk.** Entries are
published to the rings in sequence order, so within an epoch every
node's journal remains a prefix of the same logical stream. A fork
requires a promoted node to reuse sequence numbers for different
content, which requires its tip to sit *below* a surviving node's. This
spec does not move replica tips at all, and moves the primary's durable
tip *down* — so a crashed primary recovers to a shorter prefix and is
strictly *less* likely to hold a unique suffix than it is today. The
cost is rejoin depth, not divergence.

The one path that does produce a true fork is unchanged and predates
this work: the recency filter's liveness escape can legitimately let a
behind node win, with `auto_promotion_decision` as the only remaining
guard. Tracked separately in [roadmap.md](roadmap.md) ("Close the
election fork window").

**One implementation check.** The replica tip's justification is that a
promotion drains the ring into the journal, so accepted events survive.
Under async flush that drain becomes durable one flush later. Anything
on the promotion path that treats "drained" as "durable" must observe
the journal ring cursor — which the flush thread publishes post-fsync,
so the property still holds — and not the journal thread's private
`next_read`. Promotion gains one flush of latency; cover it with a test
that promotion waits for the flush executor to drain.

## Scheduling and configuration

The flush thread uses the pipeline's existing idle discipline, not a
bespoke one: `idle_wait(&mut idle_spins, busy_spin)`
(`pipeline.rs:204`), with the same `busy_spin` flag every other stage
receives (`busy_spin = !config.yield_idle`, `server.rs:1502`). Spin by
default; spin 1000 then `yield_now()` under `--yield-idle`.

This is not merely consistency — it removes a mechanism the design
would otherwise need. `yield_now()` is not a futex park, so **the
journal thread's submit is a bare seqlock store with no wake syscall in
any mode**. There is no conditional `futex_wake` on the journal loop to
argue about, no parked-state protocol, and no spin-then-park window to
tune. The receive-side cost is one cross-core cache-line transfer while
spinning (~100–200 ns, deterministic), degrading to a `sched_yield`
round under `--yield-idle` — worse, but strictly better than a futex
park, and it costs the journal thread nothing either way.

### Core assignment

Extend `--cores` with a `journal-flush` entry following the
`journal-prep` precedent: `parse_cores` (`server.rs:624-630`) accepts
9 or 10 entries today and gains an 11th, with `0 = unpinned`.

**Ship the default as `0`.** An unpinned flush thread is correct and
costs nothing structurally; picking a pinned default requires a layout
decision this spec should not make, for a reason that only became clear
on audit:

The target part, the EPYC 9275F, is **8 CCDs × 3 cores** (256 MB L3 /
32 MB per CCD). With SMT off, CCD0 is cores 0–2, and the default layout
already fills it: core 0 for OS/IRQ, `journal` 1, `matching` 2. The
watermark cell is the first genuinely CCD-sensitive pair in the
pipeline — it bounces between the journal and flush threads on every
batch, and a cross-CCD transfer costs ~100 ns+ — so honouring "same CCD
as the journal core" on this part means displacing `matching` from
CCD0. That is *probably* harmless, since journal and matching are
parallel input-ring consumers that never talk to each other and
matching's real cache partners are the ring producer and the output
ring, but "probably" is not a basis for changing the default layout on
the part we recommend to customers.

Deriving that layout properly — including whether core 0's kernel and
IRQ work pollutes CCD0's 32 MB L3 badly enough to move the pair
elsewhere — is its own measurement exercise, tracked as **"Evidence-based
`--cores` layout"** in [roadmap.md](roadmap.md). This spec ships the
seam and the instruments; that item spends the bench time. Note also
that the control-plane pinning roadmap item claims an "eleventh
`--cores` entry" too — whichever lands second takes the twelfth.

Two consequences of the `0` default:

- **`compact` needs no core.** It reserves core 7 for the bench client
  specifically to avoid an HT collision (`server.rs:591-617`); an
  unpinned flush thread leaves its 8-logical-core minimum unchanged.
- **Existing explicit `--cores` configs are unaffected**, which is the
  point of the convention — but an operator who has pinned every other
  stage should know this thread is not pinned. Emit a startup `warn!`
  when `journal-flush` is unpinned while the rest of the layout is
  explicit: degraded-but-handled is exactly that level's remit.

When the flush thread *is* pinned, two placement rules hold regardless
of the layout chosen: same CCD as the journal core, and never an SMT
sibling of journal, matching, or response — it is a pure spinner and a
sibling spinner steals issue slots from the thread this design exists
to protect. (The bench fleet runs SMT off; the `compact` doc comment at
`server.rs:583-587` records that the `Default` layout has sibling
collisions on a 16-thread part, so the rule needs stating rather than
assuming.)

## Observability

- The thread is **named** (`journal-flush`) and pinnable —
  deliberately. Every diagnosis in the beat investigation came from
  thread-targeted off-CPU tracing of named threads; this design keeps
  the durability path traceable that way, which is a primary reason it
  was chosen over the io_uring alternative (see below).
- New health gauge: flush lag, `submitted journal_seq − published
  journal_seq` (0 in steady state; a growing value is a stalling disk
  that the pipeline is riding through).
- `latency-trace` feature: add a watermark-submit → publish histogram
  next to the existing journal wakeup/batch recorders — this is the
  direct measure of "what would have been an inline stall". Rename the
  batch recorder from "write + sync" to "encode + write", which is what
  it now covers.
- Both land **with** the flush thread, not as a follow-up. They are the
  instruments the `--cores` roadmap item needs: the submit→publish
  histogram is a direct readout of the journal↔flush edge, so the
  same-CCD question becomes a two-point experiment rather than a
  layout sweep. It also discriminates the L3-pollution hypothesis,
  which predicts a fatter tail rather than a worse median.
- Existing metrics (`melin_journal_rotations_total`, gate blocker
  counters, replica ack gauges) are unchanged.

## Expected performance effects

- **Steady state**: neutral to slightly positive. Handoff adds
  ~100–200 ns. Batch sizes should be unchanged, since self-clocked
  submit reproduces the same clocking today's inline flush provides. In
  exchange, encode/replication of batch N+1 overlaps the fsync of batch
  N, so the persisted cursor's cadence becomes fsync-duration-limited
  instead of (encode+pwrite+fsync)-limited.
- **Replication latency**: unchanged at steady state. The `InputBatch`
  frame leaves at submit, which is where it leaves today; the gain is
  that submit no longer waits behind an `fdatasync`.
- **Under a disk stall**: the pipeline keeps encoding, publishing and
  writing up to ring capacity, with the `in_memory` leg advancing on
  the age bound rather than freezing. Under `hybrid`, acks continue via
  the replica leg (`persisted>=1` satisfied by a replica's persisted
  cursor). Under `local`, acks stall on the frozen persisted cursor —
  correct, and now visible as flush lag rather than as a frozen
  pipeline.
- **Under a replica-side disk stall**: bounded by the replica's
  pending-ack queue depth, not by ring capacity — see *Replica-side
  limit*. Better than today because the rounds are fatter, but not
  unbounded.

## Acceptance

1. **No regression**: LAN bench suite (`tcp-dual-repl`, throughput
   workload) — p99.9/p99.99/max at or below current main numbers, fast
   rotations still 100 %, zero sync fallbacks, journal verification
   MATCH on both replicas. Interleave the A/B samples and report median
   + range; a single before-run followed by a single after-run cannot
   distinguish a regression from thermal drift.
2. **Batching did not collapse**: mean events per `pwrite` and
   `InputBatch` frames/sec at or near main's under the throughput
   workload. This is the specific failure mode the submit trigger
   exists to prevent, and an end-to-end throughput number can mask it
   until the replication ring starts evicting. Anything that keys the
   publish to the read batch rather than the submit fails this by
   construction.
3. **Masking works**: fault-injected slow fsync on the primary
   (feature-gated delay in the flush thread, or device-level delay)
   under load — client latency time series shows *no* corresponding
   freeze under `hybrid`; flush-lag gauge shows the stall being
   absorbed; under `local` the same injection produces the (expected,
   documented) ack stall. Record the in-stall ack latency
   distribution: it is bounded by the submit trigger's age clause, and
   together with criterion 2 it is what picks the `group_commit_us`
   default.
4. **Deep rejoin after an ungraceful primary crash**: the primary
   publishes to the replication rings before its own flush — true
   today, but bounded to one in-flight batch; under async flush the
   shipped-ahead window grows to input-ring capacity during a stall, so
   a crashed primary recovers to a *shorter* prefix than it would
   today. Kill the primary mid-stall and verify the rejoin: the old
   primary must catch up cleanly (it is a prefix, not a fork — see
   *Interaction with the control plane*), including the case where the
   gap outruns the retained segment window and the rejoin falls through
   to snapshot transfer. What is being tested is catch-up depth and
   re-seed behaviour, not divergence.
5. **Unit/property coverage**: watermark cell (sample-before-sync
   publication, coalescing, seqlock atomicity, all five fields carried),
   single-writer-to-`processed` (none of the three `commit` sites
   survives — `pipeline.rs:805`, `:1018`, `:1759`), rotation drain
   protocol (no publication with a stale generation, drain always
   terminates, the cell's `RawFd` is never observed across a swap),
   poison propagation (no cursor advance after a failed sync; journal
   thread surfaces the original error; a poisoned executor still
   joins), replica persist-before-ack (acks never precede
   `set_progress`), shadow alignment (a mid-batch mark barrier never
   publishes an `input_ring_seq` the shadow can reach ahead of the
   matching `journal_seq`).

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
(cadence decoupling, sample-before-sync publication semantics,
ring-progress ownership, rotation drain, poison path, flush-lag
observability, the acceptance harness) transfers to C unchanged; only
the mechanism behind the seam would be swapped. No abstraction should
be added *speculatively* beyond that boundary — if C never happens, the
seam must still be the natural shape of the thread design.

Revisit C if: a post-prezero trace ever shows `pwrite`-side blocking;
the product invests in io_uring more deeply anyway (e.g. NVMe
passthrough); or the flush thread's core footprint bites on customer
minimum specs — C reaches the same decoupling with no extra core, so
core-budget pressure is the strongest realistic trigger.

## Out of scope

- The sector writer and the io_uring journal path (`run_uring`) — the
  roadmap already leans toward retiring the sector writer; this spec
  touches only the buffered writer's synchronous path.
- Choosing a pinned default layout for `journal-flush` — see
  *Core assignment* and the roadmap item it points at.
- Any change to durability *guarantees*: acks are still gated on the
  configured policy over persisted/in-memory cursors, and every cursor
  still means exactly what it meant before. What does change is the
  *unacked shipped-ahead window* — entries published to the replication
  rings but not yet fsynced locally, bounded by one in-flight batch
  today and by input-ring capacity after this change. It costs rejoin
  depth (acceptance item 4), not fork risk.
- Snapshot/shadow stages: they consume `FsyncState` through the same
  seqlock as today and see the same post-durability values, merely
  published from a different thread.
