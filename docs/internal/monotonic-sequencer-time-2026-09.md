# Monotonic sequencer time (plan)

Status: **proposed, not started** (2026-09). Implements the roadmap item
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
- **A promoted node does not re-recover.** The replica loop hands its
  live `(app, writer)` pair straight to `run_as_primary`. The seed for
  the clock must come from the writer, which already carries
  `next_sequence` and `chain_hash` across that handoff for the same
  reason. Seeding inside `run_as_primary` covers boot, snapshot boot
  and promotion with one code path.
- **Journal format 15 and replication protocol 5 are unreleased**
  (both sit under `[Unreleased]` in the CHANGELOG), and the journal
  reader accepts only `FORMAT_VERSION`, so there is no legacy replay
  path to preserve. Any format or wire change made before the release is
  free.
- **The "rare producer race" is no longer a multi-producer race.**
  Out-of-order event timestamps now come from two places:
  - the reader and the DPDK poll loop stamp client events with one
    wall-clock read per batch (`batch_wall_ns`), unclamped, so an NTP
    step back reaches the journal;
  - every request in one batch shares that timestamp.
  Ticks are clamped (`tick::clamp_monotonic`) but only against the
  previous tick, and the clamp state starts at zero in each loop.
- **Snapshot-only boot has no time floor.** `JournaledApp::from_parts`
  (journal missing after rotation) and the documented upgrade path
  (snapshot on the old version, start on a fresh journal) both start
  from a snapshot whose header records sequence, chain hash and epoch,
  but no timestamp.
- **The watermark lives in four places:** `dispatch` takes
  `last_drain_ns`, and the matching stage (`pipeline.rs`), recovery
  (`journaled_app.rs`) and the shadow stage (`shadow.rs`) each hold
  their own copy, all starting at zero.

## Design decisions

### 1. Stamp at ingress, through one clock owned with the producer

A `SequencerClock` holds the last issued timestamp and issues
`max(raw_wall_clock, last + 1)`. It is owned by a wrapper around the
primary's input producer, and every publish goes through the wrapper:
client events, ticks, startup events and the epoch bump. Client events
are stamped per event from the per-batch wall-clock read, so the batch
still costs one clock read and each event costs one compare.

Queries keep timestamp `0` and consume nothing: they are never
journaled. Halt refusals are never published, so they consume nothing
either. The replica pipeline keeps the raw producer, since its slots
carry the primary's stamps.

Not in the journal stage: the matching stage reads the slot, not the
journal stage's output, so stamping there would need a mirrored copy of
the rule in the matching stage. That is the lockstep shape the
"Unify sequence allocation" roadmap item exists to delete.

### 2. Strictly increasing, not merely non-decreasing

With strict time every journaled event is its own instant, so `dispatch`
calls `tick(ts)` before each event unconditionally and keeps no
watermark. There is nothing to seed, snapshot or reset on the consuming
side, which is what makes the three consumers agree by construction.

Cost: `tick` runs once per event instead of once per batch under load.
`Application::tick`'s rustdoc already asks for a cheap "nothing is due"
check and says it "runs ahead of nearly every event"; the exchange's
`tick` is `drain_due_scheduled_tasks`. Measure with echo and counter
before merging.

Fallback if it measures badly: non-decreasing stamps, with `tick`
firing when an entry's timestamp exceeds the previous entry's. That
"previous entry" value is derivable from the journal (and carried by
the snapshot, see decision 3), so it stays correct, but it is state
again. The plumbing below is the same for both; only the dispatch rule
differs.

### 3. The journal owns the time floor

The floor is part of the lineage, next to sequence and chain hash:

- the writer tracks `last_timestamp_ns` as it encodes, and recovery
  restores it from the replayed tail;
- the segment file header gains an anchor timestamp beside
  `starting_sequence` and `anchor_hash`, so a segment on its own states
  its floor (useful for single-segment readers such as the planned
  `journal-info` inspector, and for checking the first entry across a
  rotation boundary);
- the snapshot moves to transport version 3 with the timestamp at its
  anchor. A v2 snapshot seeds zero and logs a `warn!`; that only happens
  once, on the upgrade boot.

`run_as_primary` (kernel TCP and DPDK) seeds the clock from the writer.

### 4. Enforce on disk, not by convention

- The writer refuses a timestamp that is not strictly greater than its
  last (`JournalError::TimestampRegression`). One check covers the
  primary's journal stage, the replica's journal stage and the test
  helpers. On a primary a refusal is a bug: the journal stage stops as
  it does on an I/O failure, and the regression never reaches disk.
- The reader applies the same rule on replay and catch-up, as it does
  for `SequenceGap`.

This is the "assertion, not a variable" the roadmap asks for, and it
costs one compare per entry.

### 5. Drop the `Tick` payload

`JournalEvent::Tick { now_ns }` duplicates the entry header's timestamp.
Make it `JournalEvent::Tick` with no payload, in the journal codec and
in `replication_wire`, so the header is the single source of time and a
tick cannot disagree with itself. This also removes the double `tick`
call. Free while format 15 and protocol 5 are unreleased.

### 6. Time is never wound back, so make a held clock visible

After a failover to a node whose clock is behind, stamps advance by one
nanosecond per event until the wall clock catches up, and due work does
not fire in that window. A forward clock step on a primary is permanent
for the same reason. Both are the intended trade (monotonic over
accurate), but the operator must see them:

- a health gauge for how far the issued stamp leads the wall clock;
- a `warn!` at boot or promotion when the lead exceeds a threshold
  (proposed default: 1 s);
- a note in the operator docs that nodes need disciplined clocks and
  that a forward step cannot be undone.

## Order of work

One branch per step, each a reviewable commit.

0. **Prerequisite:** the roadmap's "Determinism test in the counter
   example (snapshot vs replay)". Independent, and the acceptance tests
   below build on it.
1. **Clock and stamping producer** (`transport-core`, `server-runtime`):
   `SequencerClock` and the producer wrapper; the reader, the DPDK poll
   loop, `journal_startup_events` and the epoch bump publish through it.
   Replaces the separate tick clamp state in `tick.rs`, `reader.rs` and
   `dpdk_transport.rs`. Seeded at zero for now, so the only behaviour
   change is strict increase within one process lifetime.
2. **The journal carries the floor:** writer `last_timestamp_ns`, the
   header anchor timestamp, snapshot v3, recovery exposing the value,
   and `run_as_primary` seeding the clock from the writer.
3. **Enforcement:** writer refusal and reader validation. Carries most of
   the test churn, since many tests hand-build slots with timestamp zero
   or repeated timestamps; they need a stamping helper.
4. **Dispatch:** remove `last_drain_ns` from `dispatch`, the matching
   stage, the shadow stage and recovery; `tick` before every journaled
   event and exactly once for a `Tick` entry; drop the `Tick` payload
   and update the codec and wire golden-byte tests. Restore the strong
   contract in the rustdoc of `Application::tick` (strictly increasing,
   the same calls on every path) and `ApplyCtx::now_ns`. Measure before
   merging (decision 2).
5. **Observability and docs:** the gauge and warning; the timestamp
   field's meaning in `docs/journal.md`; the reader row in
   `docs/pipeline-architecture.md`; the operator note on clock
   discipline; CHANGELOG under Unreleased; the note in
   [application-api-review-2026-09.md](application-api-review-2026-09.md);
   remove the roadmap entry.
6. **Acceptance tests**, each asserting the same `tick` sequence on the
   live, replay and snapshot-restore paths:
   - a restart across a clock step back;
   - a snapshot taken inside a regression window;
   - a failover to a node with a slower clock.

   These need a test-only clock-source seam on the stamping producer,
   since ingress calls `unix_epoch_nanos` directly today.

Steps 1 to 4 must ship in the release that introduces format 15, or a
later release carries its own format bump.

## Downstream impact (Exchange Core)

Read, not changed:

- `ServerApp::tick` is compatible as is; it will run once per event
  instead of once per batch.
- `scheduler.rs`'s module doc and `tests/journal_recovery.rs` refer to
  `Tick { now_ns }` and need updating when the payload goes.

## Open decisions

- Strict (recommended) or non-decreasing, i.e. whether a `tick` per
  event is acceptable once measured.
- Dropping the `Tick` payload now (recommended).
- The clock-lead warning threshold.
