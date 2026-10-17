# Journaled ack-policy changes (plan)

Status: **done** (2026-10). Fixes finding 10 of the
[determinism audit](determinism-audit-2026-09.md), "A runtime ack-policy
change never reaches replicas while data flows", and the deeper problem
behind it: a replica that is disconnected when the policy is weakened can
never be told, by any frame, and may later auto-promote believing acked
events needed it.

The one-line goal: **the ack policy in force is part of the lineage,
journaled in order with the events it governs, and a change takes effect
only once the nodes the previous policy relied on hold it.** Then the
failover machinery that protects acked events (the election's recency
filter and the peer-tip veto) protects the knowledge of the policy too.

## What the code shows

- **The policy lives in a process-local atomic.** The admin `ACK-POLICY`
  command (`server-runtime/src/admin.rs`, `handle_ack_policy`) swaps it
  and replies `OK`. Both response stages read it through
  `DurabilityGate` (`observe_policy_swap` once per iteration, and on every
  spin inside `wait_durable`, which is what un-wedges a gate stuck under a
  policy no connected replica can satisfy).
- **Replicas learn it from `StreamStart` and `Heartbeat` only.** The
  senders send a heartbeat only on an idle link, and journaled ticks are
  data, so while ticks flow the replica keeps the policy it saw at connect.
  Auto-promotion (`raft_promotion.rs`, `effective_ack_policy`) judges by
  that byte, falling back to the node's own configuration while no
  primary has been observed.
- **`EpochBump` is the precedent.** It is lineage metadata journaled as a
  sequenced event: totally ordered against the event flow, replayed on
  recovery, catch-up and replication, never delivered to the application.
  A policy change has the same shape.
- **The input ring has one producer.** After startup it belongs to the
  io_uring reader or the DPDK poll thread, so the admin thread cannot
  publish an entry itself. `EpochBump` is published by the main thread at
  promotion, before the reader exists.
- **A connection-0 event produces no output slot** when it has no
  reports, so the response stage never sees an `EpochBump`. It would not
  see a policy change either, and could not learn where to switch.

## Design decisions

### 1. A journal event: `AckPolicyChange { policy, unwitnessed }`

A new `JournalEvent` variant, journal tag `0x05`, payload
`[policy: u8][flags: u8]` (bit 0: unwitnessed, see decision 5). The
policy byte is `AckPolicy::as_u8`, already a stable wire value; the
journal crate carries it as an opaque newtype and `server-runtime`
validates it with `AckPolicy::from_u8`. An unknown byte at boot refuses to
start; at promotion it refuses to promote, as today.

**No journal format bump**, following `EpochBump`'s precedent: a lineage
keeps the header layout it was created with, so a bump would not mark
existing lineages anyway. Rolling back is safe: an older reader treats a
CRC-valid entry with an unknown tag as corruption and refuses to start,
it does not truncate. The upgrade notes say so.

**Sequence allocation.** The entry is journaled, allocated a wire
sequence and never a query, exactly like `EpochBump`, so the journal
stage's allocator and the matching stage's shadow cannot drift on it:
both decide through `JournalEvent::is_query`. That predicate ends in a
`_ => false` wildcard today; make it an exhaustive match, so the next
variant has to decide at compile time. Extend the agreement tests
(`matching_stage_stamps_wire_seq_in_journal_lockstep`,
`allocator_wire_seq_and_gate_cursor_agree_across_rotation`,
`recovery_resumes_allocator_wire_and_gate_agreement`, the replica-side
and frozen-replica tests) with the new variant. This meets the roadmap
item "Unify sequence allocation" trigger's intent, which is about
variants with asymmetric allocate/skip rules; amend its text to say so
rather than doing the hot-path redesign first.

**One decoder.** `replication_wire.rs` duplicates the transport-event
tags and decoding (`SLOT_TAG_*`, `decode_slot_event`). Expose one journal
codec function for the non-application events and delete the copy, so
the new tag is added once.

### 2. The lineage tracks the policy alongside the epoch

`dispatch` hands lineage events to an `on_epoch` callback today. Replace
it with one lineage sink (`Lineage { epoch, ack_policy }`), so the two
are never tracked in parallel by hand. Every path that applies lineage
events goes through it: the matching stage, recovery
(`journaled_app.rs`), the shadow stage, and the replica's boot, snapshot
install and rebase (`replication/mod.rs`).

**Snapshots carry the policy.** Replay skips entries at or below the
snapshot's sequence, and earlier segments may be gone, so a snapshot
must record the policy in force at its sequence: snapshot framing v4.
Keep the rule that the layout follows the lineage (a lineage whose
genesis length is unknown still writes v2). `snapshot::load` returns its
header struct instead of a tuple.

**On a replica the policy is last-writer-wins, not a maximum** like the
epoch. After a recovery, a divergence rebase or a snapshot resync, the
cell is *stored* from the recovered value, so that a divergent tail's
change disappears with the tail.

### 3. The response stage switches in order, at the entry's slot

The matching stage emits an output slot for every `AckPolicyChange`
(`OutputPayload::AckPolicyChange`), at its exact position in the ring:
ordering holds by construction, and the ring's backpressure bounds it.
The alternative, a side queue like `RefusalQueue`, keeps the output API
but needs its own bound and ordering protocol, and must never merge
pending changes (see below); the variant is simpler and cannot be
reordered. It is breaking for event publishers (an `OutputPayload` arm),
by design: listed under **Changed**.

**One rule for every change**, with no ranking of policies (`disk` and
`ram` cannot be ranked): a change at wire sequence `w` takes effect for
every slot after it, once `w` itself is durable under the policy in force
before it. For a weakening, the nodes the old policy relied on then hold
the entry, and a replica that lacks it lacks every later event too, so it
either loses to the peer-tip veto or judges by the older, stricter
policy. For a strengthening, the old policy clears quickly, so the cost
is negligible.

**Changes are never merged.** `disk` → `disk+ram` at `w1` → `disk` at
`w2` must apply both in turn: merging would gate `(w1, w2]` under `disk`,
and a replica holding the first change but not the second would then
auto-promote over `disk`-acked events.

The gate (`DurabilityGate`) loses its atomic: `new(initial: AckPolicy)`
starts from the lineage's recovered policy (or the configured one when
the lineage has none), and one method both stages call admits each slot,
switching through a single private `switch_to` that resets the cached
durable position and re-seeds the degradation logger. The policy in
force and the latest journaled policy are exported on `/healthz` and
`/metrics`, so operators and tests can tell a pending change from one in
force.

### 4. The reader publishes the entry; the admin waits for it

An `AckPolicyControl` mailbox shared by the admin listener and the
reader: a generation counter, the requested policy, and a wake handle
(the io_uring reader parks in `submit_and_wait` and has no tick when
ticks are off, so it must be woken through its eventfd). The reader
checks it once per loop iteration; the DPDK poll thread every N
iterations, as it does for ticks. One shared helper for both. The admin
replies `OK` once the entry is published, within a bounded wait, and
refuses if the input ring stays full past it. Every command is journaled,
duplicates included: they are the audit trail, and re-sending is how a
waiting swap is forced (decision 5).

At boot and at promotion, `run_as_primary` publishes an entry from the
main thread (still the sole producer, after the `EpochBump`) when the
requested policy differs from the recovered lineage policy, or the
lineage has none yet: a new history right after genesis, or a lineage
written before this change on its first boot under it. The engine
returns the recovered policy next to the recovered epoch.

### 5. The forced swap: the one case no design can witness

A primary halted under a policy that needs a replica, with none
connected, can never get a change acked under the old policy, and the
reply that is stuck comes before the change in the ring, so an in-order
switch is never reached. Lifting the halt there means applying the
weaker policy with nobody to witness it. Any design has this gap; this
one makes it explicit and recorded.

- **Forced** means an explicit runtime `ACK-POLICY disk` sent while no
  replica is connected: exactly the consent of decision 8 of
  [the halt plan](halt-degraded-acks-2026-10.md). The reader decides at
  publish time, and it already owns the replica count through
  `HaltGate`. Any other swap made with no replica connected is witnessed
  and waits; if the replica leaves while a swap waits, the operator
  re-sends it, the new one is forced, and it overrides the waiting one.
  A boot-time or pre-staged entry is never forced: a policy staged on a
  replica before `PROMOTE` waits like any other, and the operator re-sends
  it after promotion if no replica is connected (a documented change).
- The entry is journaled with the unwitnessed flag, and a `warn!` names
  the risk.
- When the matching stage dispatches a forced entry, it writes a
  `ForcedPolicy { wire_seq, policy }` cell (single writer, published with
  Release/Acquire). The gate reads it inside its wait, replacing the
  atomic load, and applies the forced policy once the entry itself is
  durable on the primary's disk, so the record of the swap exists before
  any reply relies on it. It then ignores in-order change slots at or
  below that sequence, the forced entry's own slot included.
- **Standalone:** a node with replication disabled starts its gate at the
  configured policy and journals the entry as unwitnessed, so a lineage
  last run replicated can restart `--standalone --ack-policy disk`
  without stalling forever.
- **The halt latch** (step 0 of the halt plan) lands here, because the
  reader now owns both the replica count and policy publication: set when
  the reader publishes a forced `disk` swap, cleared when a replica
  connects or a policy that needs one is published.

The remaining exposure is the documented limitation: after a forced swap,
a replica that was offline does not hold the entry, and if the primary
dies before it rejoins, it may auto-promote over events acked under
`disk`. The guarantees and limitations page states it, with the
operational rule (keep auto-promotion from acting on that replica until
it has rejoined).

### 6. Replicas read the lineage, not the stream's advertisement

- The replica's cell is written at receive time, in
  `process_streaming_frames`, next to the journal tip's advance: the
  same point its advertised tip moves and its in-memory ack goes out.
  Not by its matching stage, which lags; two writers would let an older
  value overwrite a newer one.
- The policy byte leaves `StreamStart` and `Heartbeat`. Advertising a
  value before the replica holds the entries behind it is finding 14's
  mistake; the journal makes the advertisement unnecessary.
  `primary_observed` gets its own flag, set when a `StreamStart` is
  accepted.
- `effective_ack_policy` reads the lineage cell (renamed from
  `primary_ack_policy`), and falls back to the node's configured policy
  only while the lineage has no entry: lineages written before this
  change keep today's behaviour until their first boot under it.
- Admin `ACK-POLICY` on a replica only stages the policy for promotion.
- **Replication protocol 8**: an older replica would fail on the new
  entry mid-stream, and two frames change layout. The monotonic-time plan
  also claims protocol 8 and is not started; whichever lands first takes
  it, and its "unreleased v15" premise is stale (note it there).

## Order of work

Each step builds, passes the suite and clippy (with and without DPDK),
and is committed on its own. Tests land with the step they pin.

1. **The journal event.** `event.rs` (variant, exhaustive `is_query`),
   `codec.rs` (tag, encode/decode, length cap), one shared decoder in
   place of `replication_wire.rs`'s copy, `dispatch.rs`'s lineage sink,
   the pipeline's outlier label, `REPL_PROTOCOL_VERSION = 8`. Tests:
   codec round trip and golden bytes, wire decode, dispatch, the extended
   allocator agreement tests. Amend the roadmap trigger text.

   *As landed:* the variant is `AckPolicyChange(AckPolicyChange)`, a
   payload struct `{ policy: AckPolicyByte, unwitnessed: bool }`, so the
   later steps can pass the change around (lineage cell, forced cell)
   without destructuring it. Decode refuses reserved flag bits, so an
   entry decodes only if it re-encodes to the same bytes. The shared
   decoder is `codec::decode_event`, for every tag (application events
   included, so both paths report an application codec's refusal the
   same way), with its own `EventDecodeError`. The lineage sink is a
   `LineageEvent` enum (`Epoch`, `AckPolicy`) handed to one callback,
   which every consumer matches exhaustively; the `Lineage` struct that
   tracks both is step 2's, and until then the live stage, recovery and
   the shadow ignore `AckPolicy` (nothing journals one yet). The
   frozen-replica gate-stall integration test drives real nodes, so it
   cannot carry the entry before step 4 gives it a producer; it is
   extended there.
2. **Lineage tracking.** Recovery, shadow, snapshot framing v4, replica
   boot / snapshot install / rebase storing the cell. Tests: recovery
   with the entry, v1–v4 snapshot parsing, snapshot plus tail with the
   entry before the snapshot and its segment gone, rebase dropping a
   divergent change.

   *As landed:* `Lineage { epoch, ack_policy: Option<AckPolicyByte> }`
   lives in `transport-core`'s `lineage` module, with the one fold
   (`Lineage::observe`: epoch a maximum, policy last-writer-wins) that
   recovery and the shadow call; `JournaledApp::recovered_lineage`
   replaces `recovered_epoch`, and `from_parts` and `shadow::run` take a
   `Lineage`. `snapshot::load` returns `(A, SnapshotHeader)` (the
   header's `epoch` became `lineage`; `load_with_header` is gone) and
   `snapshot::save` takes a `Lineage`. The live matching stage still
   ignores the policy (step 3).

   Two departures from decision 2. First, **the v4 layout carries a
   flags byte** (`[genesis_entries: u64][lineage_flags: u8][ack_policy:
   u8]`, bit 0 genesis known, bit 1 policy known, an absent field
   written as zero, anything else refused): a lineage whose genesis
   length is unknown (pre-v16 headers) gets a policy entry on its first
   boot under step 4, and writing it as v2, as "keep the rule" read
   literally, would drop the policy from its snapshots. The rule kept is
   the intent: a lineage that knows neither field still writes v2; any
   other writes v4; v3 is read only. Second, **the replica's cell lands
   here**, as `ReplicaControlPlane::lineage_ack_policy`
   (`LineagePolicyCell`, an `Option<AckPolicyByte>` in an atomic), next
   to `primary_ack_policy`, which step 5 retires. It is stored whole
   from the recovered lineage at boot and after a divergence recovery,
   from the transferred snapshot at a resync, and cleared when the local
   lineage is archived or a transfer fails; it seeds the replica's
   shadow, and a promoted primary's (`run_as_primary*` take the policy,
   from the recovered lineage at boot). Until step 5 writes it at
   receive time it lags the stream, so a primary promoted between steps
   4 and 5 seeds its shadow with the policy as of the replica's last
   recovery or resync; nothing journals a change before step 4. The
   rebase test is the in-process divergence resync end to end
   (`mid_stream_divergence_resyncs_in_process`, whose forked entry is
   now a policy change and whose primary's snapshot records a different
   one: the cell must hold the primary's after the resync, and the
   rebuilt shadow's own snapshot must record it too). The cleared cell
   after a transfer that fails once the snapshot was adopted is pinned
   by `resync_transfer_failure_rebuilds_clean_not_over_stale_writer`,
   whose primary now drops before the segment seed.
3. **The gate.** The output slot and its arm in both response stages and
   the frame encoder; `DurabilityGate` from an initial policy, the
   in-order switch, the forced cell; the health exports. The admin path
   keeps working, transitionally. Tests: slot before / entry / slot
   after; a change held until the old policy clears its entry; never
   merging two pending changes; a forced swap freeing an earlier stuck
   slot only once its entry is on disk, and later stale slots ignored.

   *As landed:* `OutputPayload::AckPolicyChange(AckPolicyChange)`. The
   matching stage emits it for every change, on any connection (a
   report-less connection-0 event otherwise emits nothing), after any
   report the change's clock step produced, as the event's last slot;
   the shutdown drain does the same. Both response stages hand every
   slot to one method, `DurabilityGate::admit`, which waits (running a
   `before_wait` hook, where the kernel stage flushes) and then, for a
   change slot, switches through the private `switch_to`; the frame
   encoder gives the slot no frame. The gate is built from a
   `GatePolicy { initial, admin, admin_seen, forced }`.

   - **The forced cell** is `ForcedPolicyWriter` / `ForcedPolicyReader`
     in `transport-core`'s `lineage` module: a seqlock over `[wire_seq,
     present | policy]`, so a reader never pairs one change's sequence
     with another's policy. It is created with each `MatchingStage`, so
     a new primary tenure (a fresh pipeline) never inherits a forced
     change; a replica's matching stage writes its own, which nothing
     reads.
   - **One staleness mark for both paths.** The plan says the gate
     ignores in-order changes at or below an applied forced one. The
     reverse also needs saying: a forced change the gate reaches in
     order (its entry already durable, no wait) must not be applied a
     second time by a later wait, after an in-order change has moved
     on. The gate keeps `applied_through`, the wire seq of the newest
     change applied either way; a change at or below it, in order or
     forced, is stale. A stale change slot is neither waited on nor
     applied.
   - **The initial policy** is `initial_gate_policy`: the lineage's, or
     the requested one (the admin atomic: configured or staged) while
     the lineage holds none. An unknown lineage byte refuses the primary
     (boot and promotion alike, before any stage starts); decision 1's
     validation lands here because the gate is its first consumer. An
     unknown byte in a live change slot or the forced cell keeps the
     prior policy with an `error!` (this node journaled it: a bug, and
     refusing every later reply would turn it into an outage).
   - **The transitional admin path** follows *changes* of the atomic,
     against the byte the initial policy was resolved from
     (`admin_seen`, so a swap landing between that and the gate's start
     is not lost), not its value: otherwise a gate started from a
     lineage policy would be swapped straight back to the configured
     one. Until step 4, a lineage holding a policy different from the
     configured one therefore serves under the lineage's until an
     operator sends an `ACK-POLICY` that changes the atomic's byte: one
     naming the configured policy stores the byte already there, so the
     gate keeps the lineage's and the operator has to send another
     policy first. Nothing journals a change before step 4, so no lineage
     holds one yet, and step 4 removes the atomic.
   - **Health exports** are `/metrics` gauges, `melin_ack_policy_in_force`
     (written by the gate at start and on every switch) and
     `melin_ack_policy_journaled` (seeded from the recovered lineage at
     primary start, written by the matching stage per change), both as
     the policy's wire byte and omitted while unknown (no gate on a
     replica; no change in the lineage). They live in `StageUtilization`
     (`LineagePolicyCell`s), so no health wiring changed. The one-line
     status of `GET /` is not extended: probes parse it positionally.
     Step 7 documents the gauges, including that only the primary seeds
     the journaled gauge from the lineage: were a replica's matching
     utilization ever exported, its gauge would stay omitted until a
     change streamed in.
   - **Tests.** Gate unit tests for the four listed cases, plus the
     in-order forced change not re-applied, unknown bytes, the
     `before_wait` hook, the admin path following changes, and the
     initial policy; a kernel response-stage test driving a change slot
     and a forced swap through the running stage; matching-stage tests
     for the slot's position and wire seq (run loop, shutdown drain,
     after clock-step reports), the forced cell and the journaled cell;
     the gauges rendered and omitted; the primary's policy resolution;
     `halt_refusal` checks the gauges on a live node. The DPDK response
     stage calls the same `admit` and is lint-checked with the DPDK
     features; it gets no netns case in this step (step 7).
4. **The producers.** The mailbox and wake, for the io_uring reader and
   the DPDK poll thread; the forced decision; the boot and promotion
   entries on both transports; the standalone rule; the gate's atomic
   removed. Tests: admin replies; an integration test with a scripted
   replica (`common/raw_replica.rs`) where, under `disk+ram` then
   `ACK-POLICY disk`, writes are not acked on the primary's fsync alone
   until the replica holds the entry, and are afterwards; restart with a
   different `--ack-policy`; standalone restart; the frozen-replica
   gate-stall test with a change journaled (deferred from step 1).

   *As landed:* the mailbox is `AckPolicyControl` in `server-runtime`'s
   `policy_control` module, one per process, shared by the admin listener
   and both roles. Its state sits behind a mutex the producer only takes
   when a request waits; the producer's per-loop cost is one acquire load
   of a `pending` flag. A primary tenure takes a `PublisherClaim` (the
   policy to request as it starts) before its gate is resolved, journals
   the boot or promotion entry from the main thread after the
   `EpochBump` (`boot_change`, `journal_boot_policy_change`), and the
   claim is attached by the producer (`PublisherClaim::attach`, with the
   io_uring reader's eventfd as the wake handle, none on DPDK), which
   polls it once per loop (the DPDK poll thread on the tick check's
   counter, ticks on or off). The forced rule is one function,
   `is_unwitnessed`, over the request's `Origin` (runtime or staged) and
   the reader's `Witnesses` (`HaltGate::witnesses`: replication disabled,
   none connected, connected). The gate's atomic is gone: `GatePolicy` is
   `{ initial, forced }`, and `initial_gate_policy` takes the standalone
   rule (requested policy when replication is disabled). The atomic's
   other readers (the senders' `StreamStart`/`Heartbeat` byte and
   auto-promotion's fallback, both step 5's) read the mailbox's
   `requested_byte`, updated when a request is staged or journaled.

   Three things the plan did not settle. First, **a request with no
   producer attached is staged, and the reply says so**: `OK staged`, not
   `OK`, on a replica and on a primary still starting, so tooling that
   waits for `OK` after a `PROMOTE` (the failover tests, the notary
   example's) retries until the promoted node journals it, and a staged
   request is never mistaken for a forced one. Second, **a request staged
   between the claim and the attach** (a primary in bring-up) is queued at
   attach as a staged request rather than lost; a request staged before
   the claim is the claim's own policy. A runtime request made while such
   a request waits is published after it, never merged. Third, a refused
   request (input ring full past `PUBLISH_WAIT`, or the producer stopped)
   is withdrawn under the lock the producer publishes under, so a refusal
   is never followed by the entry. The frozen-replica gate-stall test
   named in step 1 no longer exists in this repository (it went with the
   Exchange Core, `ddf8d8cc`); the scripted-replica test that replaces
   it is `ack_policy_changes::a_weakening_waits_for_the_replica_to_hold_the_change`,
   whose replica holds nothing past the change: the write after it stays
   held though it is on the primary's disk.

   **The promoted primary judges by the receive-time cell.** A
   promotion's lineage policy is `ReplicaControlPlane::lineage_ack_policy`
   as the replica left it, and that one value feeds the shadow seed (step
   2's note), the promotion entry (`journal_boot_policy_change` writes
   none when the requested policy equals it) and the gate's initial
   policy. Now that changes are journaled, the cell lags them until
   step 5 writes it at receive time: a replica whose cell says `disk`
   while the stream's last change says `disk+ram`, promoted with `disk`
   requested, journals no entry and starts its gate under `disk`. That is
   harmless while replicas still judge by the heartbeat byte, and step 5
   closes it; its tests should include a promotion right after a
   streamed change, checking both the entry and the gate.

   **Not yet covered on DPDK.** The DPDK poll thread's mailbox poll
   (on the tick check's counter, ticks on or off) is covered under the
   netns runner by `tests/ack_policy_changes.rs`, whose nodes start
   through melin-test-node:
   `a_weakening_waits_for_the_replica_to_hold_the_change` sends
   `ACK-POLICY disk` to a primary with ticks off and needs `OK`, which on
   DPDK only the poll thread's publisher produces. The io_uring reader's
   parked case is tested too
   (`a_parked_reader_journals_an_ack_policy_request`). What DPDK still
   lacks is the forced case, owed to step 7: `ACK-POLICY disk` to a DPDK
   primary with no replica connected replies `OK` and the journaled gauge
   moves to `disk`.
5. **The replica side.** The cell written at receive time; the policy
   byte removed from `StreamStart` and `Heartbeat`, their senders and
   receivers; auto-promotion on the lineage cell and the new
   `primary_observed` flag. Tests: the audit's reproduction (ticks
   flowing, no heartbeat, the replica's promotion decision sees `disk`);
   the reverse direction (`disk+ram` restored, auto-failover proceeds);
   `replicated_failover` and `raft_failover` updated.

   *As landed:* the cell is written by one function, `accept_slot` in
   `receiver_transport.rs`, the only way a slot from the primary reaches
   the replica's input ring: it pushes the slot and, once it is on the
   ring, stores the policy of an `AckPolicyChange`
   (`LineagePolicyCell::change_in`, an exhaustive match over the journal
   variants, so a new one has to decide whether it moves the policy). So
   a duplicate re-delivered below the resume point or a change past a
   gap never moves it, and the advertised tip, advanced after the frames
   are processed, never runs ahead of it; auto-promotion loads the tip
   before the cell. `ReplicaControlPlane::primary_ack_policy` is gone, and
   with it `ack_policy::ACK_POLICY_UNKNOWN`. All public, so step 7's
   CHANGELOG lists each under **Changed**: the removed constant; the
   `ReplicaControlPlane` field `primary_ack_policy` removed and
   `primary_observed` added; the `ack_policy` field removed from
   `PrimaryMessage::StreamStart` and `PrimaryMessage::Heartbeat`; and the
   `encode_stream_start`, `encode_heartbeat` and `snapshot_transfer_with`
   signatures that lost the byte. The
   auto-promotion fallback, used only while the lineage holds no change,
   is the mailbox's `requested_byte`, its last reader.

   Two things the plan did not say. First, **the promotion drain follows
   changes too**: it publishes slots on the same `accept_slot`, so a
   change still in flight when a promotion starts is in the cell the
   promoted primary seeds its lineage, its promotion entry and its gate
   from. This closes step 4's note on the lagging cell. Second, **the
   reproductions are integration tests in `raft_failover.rs`** (ticks
   every few milliseconds on the primary, so the sender never heartbeats,
   and the replicas requesting the policy the stream does *not* end on):
   `a_weakening_streamed_between_ticks_stops_auto_promotion` (no replica
   serves for well past the grace and an election once the primary dies)
   and `a_strengthening_streamed_between_ticks_lets_the_failover_proceed`,
   whose replicas bind a replication address, so the winner replicates and
   the standalone rule does not apply: it journals its own `disk` as its
   promotion entry and its gate stays under the streamed `disk+ram`, since
   the other replica keeps following the dead primary and never witnesses
   the entry. Both fail on the step-4 tree. The unit level pins the same
   at the receiver (a change between ticks moves the cell in either
   direction; a duplicate or a change past a gap does not; the promotion
   drain moves it) and at the decision (`consider_auto_promotion` judges
   the cell over the node's own requested policy, both ways).
   `replicated_failover` checks that the promoted node's journaled gauge
   is the streamed `ram` and that `ACK-POLICY disk` puts `disk` in force
   on it. Both receivers share `receiver_transport`, and the cluster tests
   above pass under the netns runner with the `dpdk` feature as well.
   `docs/replication.md` still describes the advertised byte; step 7
   rewrites it.
6. **The halt latch** (halt-plan step 0). Tests per that plan's step-0
   list: a forced swap lifts the halt, a returning replica clears it.

   *As landed:* the replica count and the latch are one type,
   `ReplicaPresence` in `transport-core`'s `replica_presence` module, in
   place of the bare `Arc<AtomicU32>` (`Pipeline::replicas_connected`
   and `HealthState::replicas_connected` change type, and `HaltGate::new`
   takes an `Option<Arc<ReplicaPresence>>`, so step 7's CHANGELOG lists
   all three under **Changed**). It owns `lift`/`lower` (the
   senders' `ReplicaGate` is gone) and the one rule, `halted()`, that the
   readers' `HaltGate::verdict` and the health endpoint's `trading` flag
   both call, so probes report `trading` while the override holds.

   Two things the plan did not settle. First, **a connect does not clear
   a flag; it voids the latch by generation.** The producer decides on a
   replica count read before it publishes, so a replica connecting in
   between would clear a latch not yet set, and the latch would then
   outlive the connect: the next departure would not halt. Each `lift`
   bumps a connect generation, the latch records the generation its
   observation saw (`NoneSince`, made only by `ReplicaPresence::observe`
   when none was connected), and it holds only while the generation is
   unchanged. Second, **the producer cannot journal a change without
   recording it**: `AckPolicyPublisher::poll` takes the producer's
   `HaltGate` rather than a witnesses closure, observes once
   (`HaltGate::observe`), and decides the entry on that observation
   (`Observation::decide`, which yields a `halt::Decision` holding the
   policy, the entry and the observation, so the entry published and the
   policy the latch acts on cannot disagree); after the entry is on the
   ring it calls `HaltGate::record_journaled` with that decision before
   releasing the requester, so the
   operator's `OK` means the halt is already lifted (or restored). The
   rule there: a policy that needs a replica (`AckPolicy::needs_replica`,
   derived from the clauses) clears the latch; a forced change
   (`policy_control::is_forced`, the same predicate as the forced-swap
   warning) latches it; anything else (a witnessed or staged `disk`)
   leaves it. A refused request records nothing. Standalone records
   nothing (it never halts). The guarantee covers the publisher, the
   only path that journals while serving; the boot entry
   (`journal_boot_policy_change`) journals from `HaltGate::witnesses`
   without recording, which is sound because it runs before any
   publisher or reader, on a fresh `ReplicaPresence` with no override
   to clear, and a boot entry is never forced, so there is no latch to
   set.

   Tests: `ReplicaPresence` unit tests (count, override, a connect
   voiding it, the observe-then-connect race); `HaltGate` tests for the
   halt plan's step-0 list (a `disk` node halts; a forced `disk` lifts
   it; a returning replica voids it; each replica-backed policy restores
   it; a witnessed or staged `disk` lifts nothing; the fence wins); the
   publisher's tests check the verdict after each journaled change and
   after a refused one; the admin command's test checks the halt is
   lifted by the time `OK` is read; a reader test drives the forced swap
   through a parked io_uring reader; the health endpoint follows the
   override; and `ack_policy_changes::a_forced_disk_lifts_the_halt_until_a_replica_returns`
   runs the playbook on a live node with a scripted replica (passes under
   the netns runner with the `dpdk` feature as well, so the DPDK sender's
   `lift`/`lower` and the poll thread's publisher are covered). The
   operator docs (`docs/replication.md`'s halt and manual-promotion
   sections, which already describe the playbook that now works) and the
   CHANGELOG entry the halt plan's step 0 asks for are step 7's.
7. **Docs and DPDK coverage.** `docs/replication.md` (the policy section,
   runtime swap, the forced swap and its limitation, manual promotion,
   the wire table, upgrade notes, observability), `docs/journal.md`
   (event table, snapshot v4), CHANGELOG (Fixed: finding 10; Changed:
   protocol 8, snapshot v4, the new variants, `dispatch` and
   `snapshot::load` signatures, admin semantics), the audit's status,
   the roadmap (finding 10 done, halt step 0 done), the halt and
   monotonic-time plans. DPDK cases under the netns runner, if not
   already landed with steps 4–6.

   *As landed:* the DPDK case owed since step 4 is
   `ack_policy_changes::a_forced_disk_releases_a_write_held_under_disk_and_ram`:
   under `disk+ram`, a write the scripted replica took but never acked
   is held; the replica leaves; `ACK-POLICY disk` replies `OK`, the
   journaled gauge moves to `disk`, the held write is acked, and `disk`
   is in force. It is the first live-node test of the forced cell
   overtaking a held slot (step 3 had it at the gate and the kernel
   response stage only), so it covers the DPDK response stage's
   `admit` and the poll thread's forced publish as well. The whole
   `ack_policy_changes` binary passes on kernel TCP and under the netns
   runner with the `dpdk` feature. Departures from the list above:

   - **`dispatch` is crate-private**, so its signature is not in the
     CHANGELOG; the public breaks it lists are the ones reachable from
     outside (the new variants, `snapshot::load`/`save`, `shadow::run`,
     `JournaledApp`, the protocol frames and encoders, the server-runtime
     structs, the `ReplicaPresence` type changes, the removed constants).
   - **There is no guarantees and limitations page yet** (a roadmap
     item), so decision 5's limitation is a section of
     `docs/replication.md`'s Limitations, with the operational rule, and
     the roadmap item now lists it among what that page must state.
   - **Finding 29** ("Tightening the ack policy lags the in-flight
     batch") is fixed by step 3's in-order switch, which drops the cached
     position; the audit marks it so, citing the gate test that pins it.
   - **`docs/pipeline-architecture.md`** described the gate as reading
     the configured policy; its ack-gating section and slot tables now
     describe the in-order switch and the new slot.
   - **The monotonic-time plan** now names protocol 9 for itself; its
     "unreleased format 15" premise is marked stale (0.19.0 released
     format 16 and protocol 7) and to be re-derived before it starts.

   **A new tenure applies the newest change again (review fix).** An
   independent review found that a restart took a change still waiting
   for its witness as in force: recovery folded the policy
   last-writer-wins, and `initial_gate_policy` started the gate under
   it. Under `disk+ram` with a replica, `ACK-POLICY disk` journaled at
   `w` and waiting, the replica dropping before it held `w`, and the
   primary restarted with `--ack-policy disk` (equal to the lineage's,
   so no boot entry), the gate opened on the primary's disk as soon as a
   replica authenticated; had the primary died again before the replica
   reached `w`, the replica, still believing `disk+ram`, could
   auto-promote over events acked under `disk`, none of it marked
   unwitnessed. (A restart under a different policy was safe: its boot
   entry waits under the newest policy, and every policy but `disk`
   needs a replica, which then receives `w` first.) Whether a change took
   effect before a crash is not on disk, so the fix re-applies it:
   - `Lineage::ack_policy` is a `LineagePolicy { change, wire_seq,
     previous }`: the policy in force, the wire sequence of the change
     that moved it there, the policy before that change, and whether the
     transition was unwitnessed. One fold, `LineagePolicy::after`, for
     recovery, the shadow and the replica's receive thread
     (`SharedLineagePolicy`, a mutex taken only per accepted change,
     replacing the replica's byte cell; `LineagePolicyCell` remains for
     the two gauges). **Departure from the approved design:** the
     transition is the newest change that *moved* the policy, not the
     newest change. Every command is journaled, repeats included, so
     with "the policy before the newest change" `disk+ram`, `disk` at
     `w1` (waiting), `disk` again at `w2` would record `disk` → `disk`
     and the restart would apply it at once, the same bug. A repeat
     keeps the first change's sequence and the policy before the run,
     and marks the transition unwitnessed if it is (a forced re-send, the
     documented way to force a waiting change), so a forced change stays
     applied once on disk after a restart instead of waiting again.
   - The shadow counts wire sequences from the writer's next sequence
     (`shadow::run` takes it), since a primary's input slots carry none;
     a replica's slots carry the primary's, which a debug assertion
     checks the count against. The journal stage's allocator and the
     shadow's count decide "takes a sequence" through one predicate,
     `JournalEvent::takes_wire_seq`, so they cannot drift on a primary,
     where the assertion never fires.
   - Snapshot v4 (unreleased) gained `previous_policy: u8` and
     `policy_wire_seq: u64` after `ack_policy`, and two flag bits
     (previous known, unwitnessed), each valid only with the policy bit;
     no new framing version. v1 to v3 still read.
   - `GatePolicy` is `{ start: GateStart, forced }`, `GateStart {
     in_force, pending: Option<PendingChange> }`: `initial_gate_policy`
     returns the start, so the pending change cannot be dropped on the
     way to the gate, which consumes it. The fields and `PendingChange`
     are crate-private: outside the runtime (a harness driving the
     response stage) a start can only be `GateStart::settled`. A replicated primary starts
     under `previous` with the newest change pending; the lineage's first
     change (`previous` unknown) is the policy its gate started under
     when journaled, so it starts under it settled; standalone is
     unchanged (requested, settled). The gate applies the pending change
     before admitting any slot: it waits for the change's sequence under
     the policy in force (as for the change's own slot), or, unwitnessed,
     until it is on this node's disk (in the forced path of the wait); a
     forced change of the new tenure overtakes it like any in-order one.
     It is also applied when already durable at the start and from the
     idle recheck, so the gauge follows on a quiet node. The boot entry
     then comes in order after it.
   - A promotion seeds its gate the same way, from the replica's record,
     so a node promoted right after a weakening its primary was waiting
     on is held until another replica holds the change, or the operator
     forces `disk`. Auto-promotion still judges by the newest change: the
     old primary admitted no event after a change before applying it, so
     every event after it that a replica may lack was acked under it.

   Tests: the lineage fold (previous and sequence, repeats, unwitnessed),
   the shared record, recovery and the shadow recording the transition at
   the change's sequence (a query between, taking none), snapshot v4
   round trip, golden bytes and malformed flags, the gate starting with a
   pending change (held then applied in order; unwitnessed applied once
   on disk, at once if already there; already durable at the start;
   before a boot entry; from the idle recheck; overtaken by a forced
   change; shutdown), the resolution, and promotion seeding from the
   folded record; and
   `ack_policy_changes::a_restart_reapplies_a_change_the_replica_never_held`,
   which fails with the pending change dropped and passes on kernel TCP
   and under the netns runner with the `dpdk` feature.

   **Not fixed here (pre-existing, documented):** a runtime
   `ACK-POLICY` naming a policy that needs a replica, sent to a primary
   that accepts none (no `--replication-bind`), is journaled
   unwitnessed and applied once on disk, so the gate holds every reply
   until a restart under `disk` (the standalone rule then serves at
   once). The admin atomic did the same before this plan. Refusing such
   a request at the mailbox (`Witnesses::Disabled` with
   `AckPolicy::needs_replica`) would prevent it; `docs/replication.md`
   warns against it meanwhile, and the roadmap tracks the refusal as
   its own item ("Refuse an ack policy that needs a replica on a node
   that accepts none").

   **Not fixed here (standalone exception):** a lineage whose newest
   change is still waiting for its witness (`disk` after `disk+ram`),
   restarted standalone under that same policy, journals no boot entry
   (requested equals the newest), so the standalone gate serves under it
   at once with nothing in the journal marking those acknowledgements
   unwitnessed. This is within the documented standalone exception (no
   replica can witness anything), but the record is missing. Having the
   boot journal an unwitnessed entry when the newest change is pending
   and replication is disabled would close it.

   **The override ends on a first ack, not on authentication (review
   fix).** An independent review found that `ReplicaPresence::lift`
   bumped the connect generation when a replica *authenticated*, before
   its handshake was accepted, so a replica that authenticated and was
   then refused voided a forced `disk` override on its first retry and
   flapped the node back to halted. Two changes, on both transports:
   - The count (`lift`/`lower`) moves from authentication to the
     handshake's acceptance: the TCP sender lifts after the fence check,
     the chain validation and the catch-up probe, the DPDK driver as the
     join begins (`Handshaking` → `Joining`), and each lowers only a
     connection it counted (`counted` latch; `Joining`/`Streaming`). The
     halt, the forced-swap rule (`observe`) and `melin_replicas_connected`
     keep reading this one count, so they cannot disagree on "connected".
     Counting only from live was considered and rejected: it would halt
     the node through a long catch-up or snapshot transfer that kept it
     serving before, and the `dpdk_veth` joiner tests pin that a joining
     replica stays counted.
   - The generation (renamed `acked`) is bumped by a counted connection's
     *first ack*, not by the count: `lift` returns a `FirstAck` token
     (`#[must_use]`), and only `FirstAck::acked` bumps it, once per
     connection (one branch per ack after that). Accepting the handshake
     is not proof enough: a replica refuses a lineage (genesis mismatch,
     `StalePrimary`) on `StreamStart` and closes, which the primary learns
     only from the closed socket, possibly after it has gone live. TCP
     reports the acks its live loop records; DPDK carries the token in
     `PendingJoin` and then in `SlotState::Streaming`, so a slot cannot
     stream uncounted, and reports the acks the streaming arm records
     (an ack read during the join is kept for that arm). So only an ack
     on the live stream ends the override: a replica that acks part of a
     long catch-up or snapshot and leaves before going live leaves it
     standing. Kept deliberately, and documented as "streaming live":
     DPDK could report the `Joining` arm's acks, but TCP does not read
     catch-up acks until its live loop, and the two transports should
     agree. The race stays
     closed: an override decided before a first ack is void, and one
     latched before a counted replica that leaves unacked holds. A
     witnessed `disk` leaves any earlier override in place; the replica
     it was witnessed by ends it with its first ack.
     One consequence, documented in `docs/replication.md`: replicas ack
     only entries they publish, never heartbeats, so a replica that joins
     at the primary's tip during an idle period and leaves before any
     write reaches it never ends the override (with authentication-time
     presence its connect would have). The remedy is the failover
     workflow's step 5, a policy that needs a replica. An ack on
     accepting `StreamStart` would close it at the protocol level; left
     to a separate change, tracked in the roadmap's "Degraded acks on a
     halted node" row.

   Tests: `halt.rs`'s
   `a_replica_refused_after_authenticating_leaves_the_override` (counted,
   unacked, departed three times: the override holds and a decision
   meanwhile is witnessed; a replica that acks ends it), the existing
   gate and presence tests moved to first acks
   (`an_override_decided_before_a_first_ack_is_void`), and
   `ack_policy_changes::a_forced_disk_lifts_the_halt_until_a_replica_returns`,
   which now has a scripted replica stream and leave without acking (the
   override holds) before one that acks ends it. Both fail with the
   generation bumped at `lift`. The count's move is pinned by
   `ack_policy_changes::a_replica_counts_once_its_handshake_is_accepted`:
   a scripted replica authenticates and holds its handshake back, and a
   write from a client connected after its verdict is refused and the
   gauge reads 0; the handshake then lifts the halt. It fails with TCP's
   `lift` moved back to just after authentication, and under the netns
   runner with the DPDK driver's moved to the `Authenticated` arm (the
   `Handshaking` slot then counted by `go_idle`), each checked by hand; on
   a regression the primary's count races the client's own connect and
   write, which it wins by a wide margin, but a pass on fixed code is
   no race. The integration tests pass on kernel TCP and under the
   netns runner with the `dpdk` feature, as does the `dpdk_veth` suite.
   `docs/replication.md` now says what "connected" means, that the
   lifted halt does not survive a restart (re-send `ACK-POLICY disk`),
   and that a connected replica that never acks makes every `disk`
   witnessed (disconnect it, then re-send).

   **Not fixed here (pre-existing):** a replica that refuses the stream
   is still counted from its handshake's acceptance until the primary
   sees the socket close, so on a halted node without an override each
   of its retries lifts the halt for that moment, as authentication did
   before.

## Downstream impact (Exchange Core)

Breaking: an `OutputPayload` arm in its event publisher and anything else
that matches output slots, and a replication protocol bump, so mixed
versions do not stream. No change to the client protocol.

## Not part of this item

- Degraded acks themselves (the rest of the halt plan).
- A raft-held policy, which would also cover the forced case at the cost
  of the lever when no majority is reachable.
- The sequence-allocation unification, beyond the exhaustive predicate
  and the extended agreement tests.
