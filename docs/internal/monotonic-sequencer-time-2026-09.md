# Monotonic sequencer time (plan)

Status: **in progress** (2026-09). Implements the roadmap item
"Monotonic sequencer time, derived from the journal"
([roadmap.md](roadmap.md)); read that entry for the problem statement.
This document records what the code actually looks like against that
entry, the design decisions, and the order of work.

The one-line goal: **the sequencer assigns time the way it assigns
sequence, strictly monotonic by construction.** Every journaled
timestamp is strictly greater than the one before it, on every node and
across every restart, snapshot and failover, so the application's clock
is a pure function of the journal.

## What the code shows

Findings that confirm, sharpen or correct the roadmap entry.

- **The input ring has one producer at a time, handed off in sequence.**
  On a primary, `run_as_primary` publishes the promotion `EpochBump` and
  then the startup events (`journal_startup_events`) through
  `input_producer`, then moves that producer into the reader thread
  (`spawn_reader`) or the DPDK poll loop (`run_dpdk_poll`). Nothing
  publishes concurrently. The roadmap's open design question (how the
  producers other than the reader share one last-issued value) therefore
  has a simple answer: the clock travels with the producer.
- **Client events are published through the ring's batch API.**
  `process_client_frames` opens `producer.batch()` and fills each slot
  in a `try_push_with` closure; ticks go through `try_publish`, startup
  events and the epoch bump through `publish`. Any wrapper has to cover
  all three.
- **A promoted node does not re-recover.** The replica loop hands its
  live `(app, writer)` pair straight to `run_as_primary`. The seed for
  the clock must come from the writer, which already carries
  `next_sequence` and `chain_hash` across that handoff for the same
  reason. Seeding inside `run_as_primary` covers boot, snapshot boot
  and promotion with one code path.
- **Format 15 and protocol 5 are unreleased, but not unused.** Both sit
  under `[Unreleased]` in the CHANGELOG, and the journal reader accepts
  only `FORMAT_VERSION`, so there is no released journal to replay under
  the new rule. But `main` has been writing format 15 all along (bench
  fleet, CI, local runs), with batch-shared stamps and `Tick { now_ns }`
  payloads. Under the new rule such a journal would fail as a timestamp
  regression, "a bug or tampering", or as a corrupt `Tick` entry, when
  the honest answer is "written by an older build". Hence the bump in
  decision 7.
- **The "rare producer race" is no longer a multi-producer race.**
  Out-of-order event timestamps now come from two places:
  - the reader and the DPDK poll loop stamp client events with one
    wall-clock read per batch (`batch_wall_ns`), unclamped, so an NTP
    step back reaches the journal;
  - every request in one batch shares that timestamp.

  Ticks are clamped (`tick::clamp_monotonic`) but only against the
  previous tick, and the clamp state starts at zero in each loop.
- **The tick generator is optional.** With no tick cadence configured
  the reader never runs it (`tick_enabled`), so anything placed only in
  the generator never runs on such a node.
- **Snapshot-only boot has no time floor.** `JournaledApp::from_parts`
  (journal missing after rotation) and the documented upgrade path
  (snapshot on the old version, start on a fresh journal) both start
  from a snapshot whose header records sequence, chain hash and epoch,
  but no timestamp.
- **A snapshot's anchor comes from the journal stage, not the shadow.**
  `try_save_snapshot` takes `journal_seq` and `chain_hash` from the
  journal stage's `FsyncState` and saves only when the shadow's ring
  position matches that fsync boundary. The shadow's own last slot may
  be a query, which is never stamped.
- **Recovery walks entries before the snapshot too.** `replay_segment`
  visits every entry of every retained segment for chain validation and
  skips dispatching those at or below the anchor, comparing the chain
  hash when it passes the anchor entry.
- **Resume takes its position from the recovery walk.**
  `open_append` receives `last_seq` and `valid_end` from the walk. The
  chain rebuild in `JournalEncoder::resume` hashes the raw byte range
  `[ENTRY_OFFSET, valid_end)` without parsing entries, and does not read
  the file at all with `hash-chain` compiled out, so the encoder has no
  pass over the segment's entries to take a timestamp from. The format
  offers no backward scan either: an entry's length sits in its header,
  ahead of the trailing CRC.
- **The resync seed holds the anchor entry.** A replica resynced by
  snapshot transfer receives the primary's segment prefix ending at the
  snapshot's sequence and opens it with `open_append`. That prefix is
  empty of entries only when the snapshot sits at a segment boundary.
  `verify_segment_prefix` already walks the seed's entries to prove it
  ends at the snapshot's sequence.
- **Encoder errors lose their type in the journal stage.** The stage
  wraps every `encode_event` failure in `JournalError::Io`. On a replica
  a failed journal stage tears the session down for reconnect and resync
  (`receiver_transport.rs`); the chains still agree, catch-up resends the
  same entries, and the session fails the same way again.
- **The watermark lives in four places:** `dispatch` takes
  `last_drain_ns`, and the matching stage (`pipeline.rs`), recovery
  (`journaled_app.rs`) and the shadow stage (`shadow.rs`) each hold
  their own copy, all starting at zero.
- **Divergence does not need a clock fault.** Events of one reader
  batch share a stamp, and a snapshot's anchor is an fsync boundary,
  which falls anywhere relative to those batches. When it falls inside
  a run of equal stamps, the live node skips `tick` for the next entry
  (its stamp equals the watermark) while a node restored from the
  snapshot calls it (its watermark starts at zero). The same happens
  when a replica's recovered journal ends inside such a run, since the
  live matching stage also starts at zero after replay. The extra call
  changes state when an event inside the run scheduled work already
  due at that stamp: the restored node runs it before the next entry,
  the live node after. The `Application::tick` contract does not cover
  this: it forbids a repeated call from firing work *again*, and this
  work became due between the two calls. Narrow, but present on a
  healthy cluster with good clocks.
- **A replica's journal is a bitwise mirror of the primary's.** A fresh
  replica creates its segment from the `StreamStart` lineage (starting
  sequence and chain anchor), the snapshot resync path from the `Ready`
  decision, and adopted `Rotate` boundaries keep the files identical
  across rotations (`tcp_receiver.rs`, `replication/dpdk.rs`). Anything
  added to the segment header would have to ride those frames too,
  which is why this plan leaves the header alone (decision 3).
- **No replica-generated journal entries exist.** The tick generator
  runs only on a primary (reader thread, DPDK poll loop), so a replica's
  journal holds the primary's stamps and nothing else.

## Design decisions

### 1. Stamp at ingress, through one clock owned with the producer

A `SequencerClock` holds the last issued timestamp and issues
`max(wall_clock_reading, last + 1)`, where the reading has first passed
the jump guard (decision 5). It is owned by a wrapper around the
primary's input producer, and every publish goes through the wrapper:
client events, ticks, startup events and the epoch bump. The wrapper
covers the batch API as well as `publish` and `try_publish`, and
stamps inside the slot-filling closure, so stamps are issued in ring
order by construction. A stamp issued for a push that then fails (full
ring) is simply skipped; the rule needs strictness, not density.

Client events are stamped per event from the per-batch wall-clock read,
so the batch still costs one wall-clock read and each event costs one
compare.

The wrapper is the only stamping site. Today the frame decoder
(`client_frames.rs`) writes the batch stamp into the slot; it stops
doing so, and the slot reaches the wrapper unstamped. A second site
that agrees with the first by convention is the shape this item is
deleting elsewhere.

Queries stay unstamped and consume nothing: they are never journaled,
and `QueryCtx` carries no time field, so no query ever reads the slot's
stamp. An application that wants its clock in `query` already has it:
the time of its last `tick`, held in its own state. Halt refusals are
never published, so they consume nothing either. The replica pipeline
keeps the raw producer, since its slots carry the primary's stamps.

The clock reads its time through a source chosen at compile time (a
type parameter, defaulting to the system clocks), so the acceptance
tests can drive it (step 6) without an indirect call on the hot path.

A stamp is a `SequencerTime`, a newtype in `melin-app` beside `WireSeq`,
not a bare `u64`. Three u64 time spaces meet in this item: a raw
wall-clock read, a reading that has passed the jump guard
(`ClockReading`), and an issued stamp, and only the last carries the
guarantees. The floor, `TimeFloor::After`, `FsyncState` and the
snapshot's stamp (steps 2 and 3) all take the type, so handing a raw
reading where a stamp belongs fails to compile. It enters from the
clock, and from the decoders that read a stamp back from the journal or
the replication stream. Whether `ApplyCtx::now_ns` and
`Application::tick` take it too is a public API question, open until
step 4 (below).

The test-only helpers `apply_journaled` and `tick_journaled` stamp
through the same clock rather than a test-only variant, so two calls
inside one nanosecond stay strict and the helpers keep exercising the
production rule.

Not in the journal stage: the matching stage reads the slot, not the
journal stage's output, so stamping there would need a mirrored copy of
the rule in the matching stage. That is the lockstep shape the
"Unify sequence allocation" roadmap item exists to delete.

### 2. Strictly increasing, not merely non-decreasing

With strict time every journaled event is its own instant, so `dispatch`
calls `tick(ts)` before each event unconditionally and keeps no
watermark. There is nothing to seed, snapshot or reset on the consuming
side, which is what makes the three consumers agree by construction. A
`Tick` entry calls `tick` exactly once, which removes today's double
call.

Cost: under load `tick` fires once per batch today (the batch shares
one stamp); it now fires once per event. At low load, where each batch
holds one event, it already fires on nearly every event, so the change
only adds calls at peak throughput. The cost is the application's
"nothing is due" check, which `Application::tick`'s rustdoc already
asks to be cheap; the exchange's `tick` is `drain_due_scheduled_tasks`,
a store and a heap peek when nothing is due. The default `tick` inlines
to nothing.

Measured (2026-09, AMD Ryzen 7 5800X3D, one pinned core, a standalone
microbenchmark of the dispatch loop, best of seven runs of 20M
events): with the default `tick` the two rules are indistinguishable;
with an exchange-shaped `tick` (store the time, drain a min-heap, work
scheduled every 64th event) the per-event rule costs at most 0.4 ns
more per event at batch sizes 16 and 64, and is slightly faster at
batch size 1, where it drops the watermark branch. Warm caches flatter
a microbenchmark, but `apply` touches the same state right after, so
the pipeline should not see a different order of magnitude. Step 4
confirms it in the real pipeline: echo and counter for the stamping and
encoder compares (their `tick` is the default), and a pipeline
benchmark with a scheduler-shaped test application for the per-event
`tick`.

Decided: strict. Two alternatives were weighed and rejected:

- **Non-decreasing stamps**, with `tick` firing when an entry's
  timestamp exceeds the previous entry's. That value is derivable from
  the journal and carried by the snapshot (decision 3), so it would stay
  correct, but the matching stage, the shadow and recovery would each
  have to be seeded with it again, to save a fraction of a nanosecond.
- **`tick` only on journaled `Tick` entries.** Stateless too, but due
  work would fire at the tick cadence (250 ms by default) instead of at
  the first event past its deadline, ticks are dropped on a full ring,
  exactly under load, and `apply` would see times whose due work has not
  run, pushing deadline checks into every application.

### 3. The journal owns the time floor

The floor is the last journaled timestamp, held where the sequence and
chain hash already are.

- **`JournalEncoder` tracks `last_timestamp_ns`** as it encodes. It is
  the one encoder behind every write: `BufferedWriter` wraps it, the
  journal stage runs on it after `into_halves` splits the writer, and
  `from_halves` hands it back when a replica is promoted, so the floor
  moves with it through both handoffs with nothing to copy.
- **Every writer constructor takes the floor, typed, with no default.**
  `create`, `create_continuing` and `open_append` take a `TimeFloor`,
  threaded in beside `last_seq` with the same provenance, so one
  parameter covers an empty segment and a resumed one alike:
  - `Genesis`: a brand-new journal, nothing precedes it;
  - `After(ts)`: the stamp of the last entry before the write position.
    Recovery on the live segment passes the last stamp its walk read,
    the interrupted-rotation path the last stamp seen in the walked
    archives, and a snapshot-only boot or a resync the snapshot's stamp
    (a resync seed ends at the snapshot's anchor entry, or holds no
    entries when the snapshot sits at a segment boundary, so the
    snapshot's stamp is the floor either way);
  - `Unknown`: a v1 or v2 snapshot, which records no stamp.

  `Genesis` and `Unknown` both behave as zero, but they are distinct at
  the call site, so every place the guarantee is waived can be found by
  name. `Unknown` logs a `warn!`; it only happens once, on the upgrade
  boot. A caller that passes a floor too low lets at most a regressing
  entry through the encoder, and the reader's within-segment check
  (decision 4) fails recovery on it at the next boot: loud, if late.
- **The snapshot moves to transport version 3 with the timestamp at its
  anchor.** The journal stage publishes the floor in `FsyncState` beside
  `journal_seq` and `chain_hash`, under the same seqlock, and
  `try_save_snapshot` records all three together. It never uses the
  shadow's last slot, which may be an unstamped query.
- **The snapshot's stamp is checked wherever the anchor entry is at
  hand.** Recovery compares the anchor entry's stamp with the
  snapshot's beside the existing chain check
  (`SnapshotTimestampMismatch`), and the resync path compares the seed's
  last entry with the transferred snapshot beside its chain check, the
  stamp coming from the walk `verify_segment_prefix` already makes. A
  wrong stamp in a snapshot is otherwise invisible until a snapshot-only
  boot seeds a primary below its replicas' floors.

`run_as_primary` (kernel TCP and DPDK) seeds the clock from the floor
of the writer it receives.

Not in the segment header. Every path that opens a writer reaches the
floor from either an entry or a snapshot: recovery walks back to one or
the other, or refuses to start. An anchor timestamp in the header would
only let an empty segment state its own floor, and because a replica's
header is a bitwise copy of the primary's it would also have to ride
`StreamStart`, `Ready` and `Rotate`, reaching catch-up and both
receivers. That is a replication protocol change for a convenience.

### 4. Enforce on disk, not by convention

- **`JournalEncoder` refuses a timestamp that is not strictly greater
  than its last** (`JournalError::TimestampRegression`). Being the one
  encoder behind every write (decision 3), this single check covers the
  primary's journal stage, the replica's journal stage, `BufferedWriter`
  and the test helpers.
  The floor survives rotation, which is what covers segment boundaries
  on a replica: every entry it adopts, including the first after a
  `Rotate`, goes through that check. No separate check at `Rotate` is
  needed. That rests on one detail: `JournalEncoder::begin_segment`
  resets the starting sequence, the batch state and the chain, and the
  floor is the one piece of encoder state it must leave alone.
  Resetting it there alongside the rest is the natural mistake, and it
  would reopen the boundary on every replica with nothing failing. It
  has two callers, `BufferedWriter`'s rotation and the journal stage's
  (the one every running node, primary or replica, goes through), so
  the test that pins it lives in `encoder.rs`, on `begin_segment`
  itself: an entry stamped below the outgoing segment's last is refused
  as the first entry of the new one. A test on either caller would
  leave the other unpinned.
- **A refusal keeps its type up to the node's lifecycle.** The journal
  stage stops wrapping encoder errors in `JournalError::Io`, so the
  node can tell a regression from an I/O failure.
  - On a primary a refusal is a bug: the journal stage stops as it does
    on an I/O failure, and the regression never reaches disk.
  - On a replica it means the primary's stream breaks time order, which
    no resync repairs: catch-up would resend the same entry. The
    session ends with a distinct fatal exit that does not go through
    reconnect and resync, logs an `error!` naming the sequence and both
    stamps, and leaves the journal as it is for inspection.
- **The reader applies the same rule on replay and catch-up, within a
  segment, as a hard error.** Unlike `SequenceGap`, which recovery
  treats as a torn tail on the live segment and truncates at, a
  CRC-valid entry whose stamp regresses is not a torn write: it is a bug
  or tampering, and recovery fails on it rather than silently dropping
  the tail. The timestamp check runs after the sequence checks
  (`SequenceGap`, `SequenceDuplicate`), so stale bytes past the tail
  still take the truncate-at-gap path and never reach it. That relies
  on `open_append` scrubbing past `valid_end`, so no stale entry can
  carry the next expected sequence; a test pins that a CRC-valid
  regressing entry in the live tail fails recovery rather than
  truncating it.
- **On replay the segment boundary is checked by recovery, not by the
  reader.** With no floor in the header (decision 3) a reader opened on
  a segment cannot judge its first entry, so recovery carries the last
  stamp it has walked across the boundary and compares the next
  segment's first entry against it, exactly as it carries the tail hash
  and the expected starting sequence today. The carry comes from the
  walk, which includes the entries at and below the snapshot's anchor.
  The snapshot's stamp starts it only when the walk begins after the
  anchor (the first retained segment starts at the anchor plus one).
  Without this the first entry of every segment is the one unchecked
  point in replay.

This is the "assertion, not a variable" the roadmap asks for, and it
costs one compare per entry.

### 5. The clock does not follow a runaway wall clock

Monotonic time makes a forward jump permanent: every later stamp stays
at least as far ahead, and timers scheduled from `now_ns` wait until
the real clock catches up. A clock set to the wrong year (a bad time
source, a VM resumed with a stale clock, a node taking writes before
its time daemon has synced) would hold the application's time for
years, and winding the journal back would not help, because the
application has already absorbed the future time. Today a restart
undoes a forward jump; under strict time nothing does, so this plan
creates the hazard and has to answer it.

The answer is a guard on the running clock, and it is worth being plain
about its reach. It protects a node that is already running, and buys
the operator time to fix the clock. It cannot protect seeding: a
restart or a failover seeds from the wall clock, so a jump the guard
refused is accepted by the next boot if the clock is still wrong, and a
clock already wrong at boot is caught only by the sync-state warning
below. Comparing the wall clock with the journal's floor at boot cannot
tell a bad clock from a long downtime.

- **A reference on a clock that cannot jump.** Beside the wall-clock
  reading, the clock reads `CLOCK_BOOTTIME` and keeps the pair from its
  last accepted reading. The expected wall time is that reading plus
  the boot-time elapsed since. `CLOCK_BOOTTIME` rather than
  `CLOCK_MONOTONIC` (what `Instant` reads), so a suspended VM that
  resumes is not taken for a jump. It is read once per batch beside the
  wall clock (the reader and the DPDK loop already make one wall-clock
  read per batch), and once per tick: one more vDSO read per batch,
  never per event.
- **A reading more than the jump limit ahead of the expected time is
  refused.** The clock issues from the expected time instead, so time
  keeps flowing at the real rate rather than standing still, and it
  keeps its reference, so it follows the wall clock again as soon as a
  reading comes back within the limit. A refusal logs a `warn!` once per
  episode naming the jump, and counts in
  `melin_clock_jumps_refused_total`. While refused, the node's time runs
  behind the wall clock, which the offset gauge shows as a negative
  value (decision 6), so "behind" is as alertable as "held ahead". A
  backward reading is accepted as
  the reference (stamps are then held by `last + 1`, decision 6); only
  the forward direction is refused.
- **A deliberate jump is accepted by the operator.** A legitimate
  correction larger than the limit leaves the node issuing correct-rate
  time behind the wall clock. `CLOCK-ACCEPT` on the admin endpoint
  accepts the current wall clock; the wrapper reads the request from an
  atomic once per batch, as it reads the halt gate. A restart or a
  failover also re-seeds from the wall clock.
- **Seeding has no reference, so it checks the clock's sync state, and
  only warns.** At boot and promotion the clock seeds from
  `max(wall_clock, floor + 1)` with nothing to compare a jump against.
  It asks the kernel whether the clock is synchronized (`adjtimex`) and
  logs a `warn!` if not. A warning is the ceiling: time daemons (chrony,
  ntpd, ptp4l with phc2sys) do not report sync state to the kernel
  alike, and refusing to serve on that flag would turn a correctly
  disciplined node into an outage.

The jump limit is a server setting, defaulting to 5 s. The two ways to
get it wrong are not symmetric. Accepting a bad jump freezes timers for
its whole size once the clock is corrected, and nothing undoes it.
Refusing a legitimate one leaves the node's time running correctly but
behind, visible on the gauge, fixed by `CLOCK-ACCEPT`, and healed by the
next restart or failover. So the limit sits just above the largest
legitimate step on a running node, not far above it: chrony slews and
steps only at startup (`makestep`), ntpd steps past 128 ms, and a
live-migration pause advances the wall clock and `CLOCK_BOOTTIME`
together. 5 s clears those with room to spare and caps the worst freeze
a wrongly accepted jump can cause.

### 6. Time is never wound back, so say when it is held

After a failover to a node whose clock is behind, or a step back on a
running primary, stamps advance by one nanosecond per event until the
wall clock catches up. For that long the application's time stands
still: work that falls due inside the window waits (work already due
keeps firing), and anything measured in `now_ns` windows stops moving,
a rate limiter's refill for one. This is the intended trade (monotonic
over accurate), but the operator and the application author must know:

- **a `warn!` when the clock is seeded**, at boot or promotion, if the
  journal's floor leads the wall clock by more than 100 ms, naming the
  lead. A fixed constant, not a setting: in steady state the lead is a
  batch's worth of nanoseconds and after a failover the skew between
  two disciplined clocks, so 100 ms means the clocks have a problem,
  and it catches a one-second leap-second step that a 1 s threshold
  would miss. Operators who want a finer line set it on the gauge
  below;
- **the same `warn!` from the stamping wrapper on a running primary**,
  once per crossing, re-armed only after the lead falls back under half
  the threshold so it does not flap. The wrapper already holds the raw
  reading and the last issued stamp for every batch and tick, so this
  is one compare per batch, never per event, and it runs whether or not
  a tick cadence is configured;
- **a signed gauge, `melin_sequencer_clock_offset_seconds`**, on the
  health endpoint beside the other gauges: the last issued stamp minus
  the wall clock. Positive while the clock is held ahead, negative while
  a refused jump leaves it behind (decision 5), near zero otherwise.
  Operators alert on metrics, not log lines, and both states are
  otherwise silent;
- **the application contract says it**: `Application::tick`'s rustdoc
  states that time may stand still, advancing one nanosecond per event,
  for as long as the clock is held;
- **the operator docs say it**: nodes need disciplined clocks, the jump
  guard and `CLOCK-ACCEPT`, and what a held clock does to time-driven
  work.

### 7. Bump the journal format and the replication protocol

The new replay rule refuses what format-15 journals written by `main`
contain, and the `Tick` layout changes in both the journal and the
replication stream (step 4). Format 16 and protocol 6 turn a misleading
failure into the refusal an older build's journal or peer should get
(`UnsupportedVersion`, a handshake refusal). Released users are
unaffected: 0.17 writes format 14, so they cross 14 to 16 in the one
migration the CHANGELOG already describes (snapshot on the old version,
deploy, start on a fresh journal).

## Order of work

One commit per step, each reviewable on its own.

1. **Clock and stamping producer** (`transport-core`, `server-runtime`):
   `SequencerClock` with its compile-time time source and the jump
   guard (decision 5, without the admin command yet), and the producer
   wrapper over `publish`, `try_publish` and the batch API. The reader,
   the DPDK poll loop, `journal_startup_events`, the epoch bump and the
   test helpers publish through it, and the frame decoder stops
   stamping. Replaces the separate tick clamp state in `tick.rs`,
   `reader.rs` and `dpdk_transport.rs`. Seeded at zero for now, so the
   only behaviour changes are strict increase within one process
   lifetime and refused forward jumps.
2. **The journal carries the floor:** `JournalEncoder`'s
   `last_timestamp_ns` (encode path), `TimeFloor` on every writer
   constructor with recovery threading the walked stamp beside
   `last_seq`, the floor in `FsyncState`, snapshot v3, and
   `run_as_primary` seeding the clock from the writer. The snapshot
   stamp checks (recovery's anchor, the resync seed) land here, since
   they are what makes v3's new field trustworthy.
3. **Enforcement:** the encoder's refusal (its floor surviving
   rotation, with the `begin_segment` test in `encoder.rs` that pins
   it), the typed error through the journal stage and the replica's
   distinct fatal exit, reader validation within a segment, and the
   boundary check in recovery carried from the walk. Bumps the journal
   format to 16.
   Must not land before step 2: with the floor not yet carried across a
   restart, the first entry after a clock step back would be refused.
   Carries most of the test churn, since many tests hand-build slots
   with timestamp zero or repeated timestamps; they need a stamping
   helper.
4. **Dispatch:** remove `last_drain_ns` from `dispatch`, the matching
   stage, the shadow stage and recovery; `tick` before every journaled
   event and exactly once for a `Tick` entry, taking the time from the
   entry header. Drop the `Tick` payload in the same step:
   `JournalEvent::Tick { now_ns }` becomes `JournalEvent::Tick` in the
   journal codec and `replication_wire`, with the golden-byte tests
   updated, and the replication protocol moves to 6. Once dispatch
   reads the header, the payload is a second copy of the time that
   nothing reads and that can disagree with the first, which is the
   shape this item removes. Left as an optional follow-up it would
   likely ship, and removing it afterwards costs a format bump. Restore
   the strong contract in the rustdoc of `Application::tick` (strictly
   increasing, the same calls on every path, and time that may stand
   still while the clock is held) and `ApplyCtx::now_ns`. Confirm the
   microbenchmark in the real pipeline before merging (decision 2).
   `dispatch` must return before the clock step for a `Shutdown` slot:
   the matching stage's shutdown drain hands it the pipeline sentinel,
   whose zero time would otherwise reach `tick`.
   Decide here whether the application sees `SequencerTime` (decision 1):
   the type would carry the contract where application authors read
   it, at the cost of a public API break for every application's `tick`
   and every reader of `now_ns`.

   Guarded by the property test that proves the runtime's half of
   determinism: the sequence of calls into the application depends on
   the journal alone. A recording test application logs every call it
   receives (`tick` with its time, `apply` with its time, key and an
   identifier its event carries, since `ApplyCtx` has no sequence; an
   epoch bump reaches the fence, not the application, but still moves
   the clock). A generated journal (application
   events, `Tick` and `EpochBump` entries, rotations at random points)
   runs through the live pipeline, through recovery, and through a
   snapshot restore at every anchor followed by replay of the rest; for
   each anchor, the calls after it must equal the live run's calls
   after the same anchor. It was written before step 1 and seen failing
   on the code before this item, with runs of equal stamps across an
   anchor (the snapshot-inside-a-batch divergence above): it shrank to
   two writes sharing a stamp with the snapshot between them, where the
   restored node calls `tick` once more than the live one. With strictly
   increasing stamps it passes, so it entered history at the start of
   step 2, guarding steps 2 to 4; this step must keep it passing while
   the watermark goes.
5. **Operator surface:** the seeding and running lead warnings, the
   offset gauge, the jump counter, `CLOCK-ACCEPT`, and the sync-state
   warning at seeding.
6. **Acceptance tests**, each asserting the same `tick` sequence on the
   live, replay and snapshot-restore paths:
   - a restart across a clock step back;
   - a snapshot taken inside a regression window;
   - a failover to a node with a slower clock;
   - a forward jump beyond the limit, refused, with time still flowing
     at the reference rate, then accepted by `CLOCK-ACCEPT`;
   - a replica resynced from a snapshot at a segment boundary, seeded
     from the snapshot's stamp.

   Written at the `transport-core` level, driving a pipeline through
   the clock's test time source. Where step 4's property test covers
   every anchor of generated journals, these cover the clock scenarios
   the generator does not produce. They do not wait on the counter
   determinism test, which stays an independent roadmap item: it checks
   the application's half (identical calls give identical state), not
   the runtime's.
7. **Docs:** the timestamp field's meaning in `docs/journal.md`; the
   reader row in `docs/pipeline-architecture.md`; the operator notes of
   decision 6; `CLOCK-ACCEPT` beside the other admin commands in
   `docs/replication.md`; CHANGELOG under
   Unreleased (the format-15 and protocol-5 entries become 16 and 6);
   the note in
   [application-api-review-2026-09.md](application-api-review-2026-09.md);
   remove the roadmap entry.

Steps 1 to 5 ship in one release. After that, the replay rule and the
`Tick` layout each need their own version bump again.

## Downstream impact (Exchange Core)

Read, not changed here; the fixes belong to the exchange.

- **Broken since step 1.** Two bench binaries build `InputSlot`s by
  hand and publish them straight to the ring, bypassing the stamping
  producer: `crates/exchange/server/src/bin/replication-bench.rs` (two
  publish sites) and `crates/exchange/bench/src/main.rs` (one). The
  field is now `timestamp: SequencerTime`, so they no longer compile.
  Renaming the field is not enough: both stamp each event from a raw
  clock read, and from step 3 the encoder refuses equal stamps, which
  the replication bench reaches at bench rates. They need a strictly
  increasing stamp: the stamping producer, or at least `last + 1`.
- `ServerApp::tick` is compatible as is; it will run once per event
  instead of once per batch.
- `scheduler.rs`'s module doc refers to `Tick { now_ns }` and needs
  updating when the payload goes (step 4).
- The exchange's rate limiter reads the clock `tick` stamps, so it
  stops refilling while the sequencer clock is held (decision 6). Worth
  a line in its own docs; no code change.

## Not part of this item

- **An anchor timestamp in the segment header.** See decision 3: not
  needed for correctness, and it drags in a replication protocol change.
- **Slewing instead of holding.** Running the clock slightly slow while
  it leads the wall clock, instead of one nanosecond per event, would
  keep time moving after a failover to a slower clock. Only large leads
  make holding visible, and the jump guard plus disciplined clocks keep
  them rare; revisit if an operator hits one.
- **Dropping snapshot transport v1.** v2 has been written since the
  fencing-epoch release, so v1 files only come from much older nodes,
  and dropping them may well be right. But v1 and v2 share the same
  `TimeFloor::Unknown` branch here, so this item gives no reason to
  drop it; if it goes, it goes in its own commit with its own case.
- **A clock on `QueryCtx`.** See decision 1: an application API
  question, decided separately if anyone needs it.
