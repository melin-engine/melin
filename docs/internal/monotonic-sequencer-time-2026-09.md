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
- **Journal format 15 is unreleased** (it sits under `[Unreleased]` in
  the CHANGELOG), and the journal reader accepts only `FORMAT_VERSION`,
  so there is no legacy replay path to preserve. The replay rule can
  change before the release without a further format bump.
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
`max(raw_wall_clock, last + 1)`. It is owned by a wrapper around the
primary's input producer, and every publish goes through the wrapper:
client events, ticks, startup events and the epoch bump. Client events
are stamped per event from the per-batch wall-clock read, so the batch
still costs one clock read and each event costs one compare.

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

Cost: under load `tick` already fires once per batch (the batch shares
one stamp); it now fires once per event. `Application::tick`'s rustdoc
already asks for a cheap "nothing is due" check and says it "runs ahead
of nearly every event"; the exchange's `tick` is
`drain_due_scheduled_tasks`, a heap peek when nothing is due. Measure
with echo and counter before merging.

Fallback if it measures badly: non-decreasing stamps, with `tick`
firing when an entry's timestamp exceeds the previous entry's. That
"previous entry" value is derivable from the journal (and carried by
the snapshot, see decision 3), so it stays correct, but it is state
again. The plumbing below is the same for both; only the dispatch rule
differs.

### 3. The journal owns the time floor

The floor is the last journaled timestamp, held where the sequence and
chain hash already are:

- the writer tracks `last_timestamp_ns` as it encodes, and recovery
  restores it from the replayed tail: `open_append` takes it beside
  `last_seq` and `valid_end`, and the crash-interrupted-rotation path
  that synthesizes a live segment from `last_seq_seen` seeds the writer
  from the last stamp seen the same way;
- the snapshot moves to transport version 3 with the timestamp at its
  anchor, and a node that starts from a snapshot (snapshot-only boot, or
  a replica seeded by snapshot transfer) seeds its writer from it. A v2
  snapshot seeds zero and logs a `warn!`; that only happens once, on the
  upgrade boot. A v1 snapshot (no epoch) lacks the timestamp too and
  takes the same branch.

`run_as_primary` (kernel TCP and DPDK) seeds the clock from the writer.

Not in the segment header. Every path that opens a writer reaches the
floor from either a replayed entry or a snapshot: recovery walks back to
one or the other, or refuses to start. An anchor timestamp in the header
would only let a segment state its own floor, and because a replica's
header is a bitwise copy of the primary's it would also have to ride
`StreamStart`, `Ready` and `Rotate`, reaching catch-up and both
receivers. That is a replication protocol change for a convenience.

### 4. Enforce on disk, not by convention

- The writer refuses a timestamp that is not strictly greater than its
  last (`JournalError::TimestampRegression`). One check covers the
  primary's journal stage, the replica's journal stage and the test
  helpers. On a primary a refusal is a bug: the journal stage stops as
  it does on an I/O failure, and the regression never reaches disk.
- The reader applies the same rule on replay and catch-up, on every
  segment, as a hard error. Unlike `SequenceGap`, which recovery treats
  as a torn tail on the live segment and truncates at, a CRC-valid entry
  whose stamp regresses is not a torn write: it is a bug or tampering,
  and recovery fails on it rather than silently dropping the tail. The
  timestamp check runs after the sequence checks (`SequenceGap`,
  `SequenceDuplicate`), so stale bytes past the tail still take the
  truncate-at-gap path and never reach it.

This is the "assertion, not a variable" the roadmap asks for, and it
costs one compare per entry.

### 5. Time is never wound back, so say when it is held

After a failover to a node whose clock is behind, stamps advance by one
nanosecond per event until the wall clock catches up, so work that
falls due inside that window waits for it (work already due keeps
firing). A forward clock step on a primary is permanent for the same
reason. Both are the intended trade (monotonic over accurate), but the
operator must know:

- a `warn!` when the clock is seeded, at boot or promotion, if the
  journal's floor leads the wall clock by more than a threshold
  (proposed default: 1 s), naming the lead;
- a note in the operator docs that nodes need disciplined clocks and
  that a forward step cannot be undone.

## Order of work

One commit per step, each reviewable on its own.

1. **Clock and stamping producer** (`transport-core`, `server-runtime`):
   `SequencerClock` and the producer wrapper; the reader, the DPDK poll
   loop, `journal_startup_events`, the epoch bump and the test helpers
   publish through it, and the frame decoder stops stamping. Replaces
   the separate tick clamp state in `tick.rs`, `reader.rs` and
   `dpdk_transport.rs`. Seeded at zero for now, so the only behaviour
   change is strict increase within one process lifetime.
2. **The journal carries the floor:** writer `last_timestamp_ns`
   (encode path, `open_append`, the interrupted-rotation path),
   snapshot v3, recovery exposing the value, and `run_as_primary`
   seeding the clock from the writer, with the lead warning.
3. **Enforcement:** writer refusal and reader validation. Must not land
   before step 2: with the floor not yet carried across a restart, the
   first entry after a clock step back would be refused. Carries most
   of the test churn, since many tests hand-build slots with timestamp
   zero or repeated timestamps; they need a stamping helper.
4. **Dispatch:** remove `last_drain_ns` from `dispatch`, the matching
   stage, the shadow stage and recovery; `tick` before every journaled
   event and exactly once for a `Tick` entry. Restore the strong
   contract in the rustdoc of `Application::tick` (strictly increasing,
   the same calls on every path) and `ApplyCtx::now_ns`. Measure before
   merging (decision 2).
5. **Acceptance tests**, each asserting the same `tick` sequence on the
   live, replay and snapshot-restore paths:
   - a restart across a clock step back;
   - a snapshot taken inside a regression window;
   - a failover to a node with a slower clock.

   Written at the `transport-core` level, driving a pipeline through a
   test-only clock-source seam on the stamping producer (ingress calls
   `unix_epoch_nanos` directly today). They do not wait on the counter
   determinism test, which stays an independent roadmap item.
6. **Docs:** the timestamp field's meaning in `docs/journal.md`; the
   reader row in `docs/pipeline-architecture.md`; the operator note on
   clock discipline; CHANGELOG under Unreleased; the note in
   [application-api-review-2026-09.md](application-api-review-2026-09.md);
   remove the roadmap entry.
7. **Optional: drop the `Tick` payload.** `JournalEvent::Tick { now_ns }`
   duplicates the entry header's timestamp; making it `JournalEvent::Tick`
   in the journal codec and `replication_wire` leaves the header as the
   single source of time. Not needed for correctness (step 4 already
   fixes the double call), and it reaches the codec and wire golden-byte
   tests, a published crate's API and the Exchange Core's docs. Free
   only while format 15 and protocol 5 are unreleased; otherwise leave
   it.

Steps 1 to 4 must ship in the release that introduces format 15. After
that, the changed replay rule needs its own format bump.

## Downstream impact (Exchange Core)

Read, not changed:

- `ServerApp::tick` is compatible as is; it will run once per event
  instead of once per batch.
- If step 7 lands, `scheduler.rs`'s module doc refers to
  `Tick { now_ns }` and needs updating.

## Open decisions

- Strict (recommended: the only variant with no consuming-side state,
  which is the point of the item) or non-decreasing, i.e. whether a
  `tick` per event is acceptable once measured.
- The clock-lead warning threshold.
- Whether to take step 7 before the release.

## Not part of this item

- **An anchor timestamp in the segment header.** See decision 3: not
  needed for correctness, and it drags in a replication protocol change.
- **A health gauge for the clock's lead over the wall clock.** The
  warning at seeding tells the operator what they need; add a gauge if
  someone asks to watch it continuously.
- **Dropping snapshot transport v1.** v2 has been written since the
  fencing-epoch release, so v1 files only come from much older nodes,
  and dropping them may well be right. But v1 and v2 share the same
  "no timestamp, seed zero" branch here, so this item gives no reason
  to drop it; if it goes, it goes in its own commit with its own case.
- **A clock on `QueryCtx`.** See decision 1: an application API
  question, decided separately if anyone needs it.
