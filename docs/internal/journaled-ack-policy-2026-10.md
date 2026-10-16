# Journaled ack-policy changes (plan)

Status: **in progress** (2026-10). Fixes finding 10 of the
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
2. **Lineage tracking.** Recovery, shadow, snapshot framing v4, replica
   boot / snapshot install / rebase storing the cell. Tests: recovery
   with the entry, v1–v4 snapshot parsing, snapshot plus tail with the
   entry before the snapshot and its segment gone, rebase dropping a
   divergent change.
3. **The gate.** The output slot and its arm in both response stages and
   the frame encoder; `DurabilityGate` from an initial policy, the
   in-order switch, the forced cell; the health exports. The admin path
   keeps working, transitionally. Tests: slot before / entry / slot
   after; a change held until the old policy clears its entry; never
   merging two pending changes; a forced swap freeing an earlier stuck
   slot only once its entry is on disk, and later stale slots ignored.
4. **The producers.** The mailbox and wake, for the io_uring reader and
   the DPDK poll thread; the forced decision; the boot and promotion
   entries on both transports; the standalone rule; the gate's atomic
   removed. Tests: admin replies; an integration test with a scripted
   replica (`common/raw_replica.rs`) where, under `disk+ram` then
   `ACK-POLICY disk`, writes are not acked on the primary's fsync alone
   until the replica holds the entry, and are afterwards; restart with a
   different `--ack-policy`; standalone restart.
5. **The replica side.** The cell written at receive time; the policy
   byte removed from `StreamStart` and `Heartbeat`, their senders and
   receivers; auto-promotion on the lineage cell and the new
   `primary_observed` flag. Tests: the audit's reproduction (ticks
   flowing, no heartbeat, the replica's promotion decision sees `disk`);
   the reverse direction (`disk+ram` restored, auto-failover proceeds);
   `replicated_failover` and `raft_failover` updated.
6. **The halt latch** (halt-plan step 0). Tests per that plan's step-0
   list: a forced swap lifts the halt, a returning replica clears it.
7. **Docs and DPDK coverage.** `docs/replication.md` (the policy section,
   runtime swap, the forced swap and its limitation, manual promotion,
   the wire table, upgrade notes, observability), `docs/journal.md`
   (event table, snapshot v4), CHANGELOG (Fixed: finding 10; Changed:
   protocol 8, snapshot v4, the new variants, `dispatch` and
   `snapshot::load` signatures, admin semantics), the audit's status,
   the roadmap (finding 10 done, halt step 0 done), the halt and
   monotonic-time plans. DPDK cases under the netns runner, if not
   already landed with steps 4–6.

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
