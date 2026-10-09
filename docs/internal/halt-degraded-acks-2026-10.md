# Degraded acks on a halted node (plan)

Status: **proposed** (2026-10). Covers the roadmap item "Degraded acks on
a halted node", which replaces the former items "Halt refusals behind a
stalled ack gate" and "Answer queries on a halted node": both are
symptoms of the same stall, and one change resolves both.

The one-line goal: **a halted node always answers, and every answer says
what backs it.** Writes that were in flight when the last replica left
are answered once the primary's own disk holds them, with a reply that
is marked as backed by the primary alone. Queries are answered the same
way, and refusals stop waiting behind either.

## What the code shows

- **The halt ignores the ack policy.** `HaltGate::verdict`
  (`server-runtime/src/halt.rs`) refuses every client write with
  `ReplicaDisconnected` once the replica count is zero, under every
  policy, `disk` included. The module doc's rationale ("it can no longer
  promise what the ack policy promises") does not hold for `disk`, which
  the primary's fsync alone satisfies. Under `disk` the halt does a
  different job: it is the only thing that stops a primary a partition
  has cut off from its replica (see the next bullet).
- **Fencing is contact-based, and there is no lease.** A serving node is
  fenced only when it hears from a node that observed a promotion. The
  raft mesh carries that signal only under auto-promotion
  (`server-runtime/src/raft.rs`, `SupersessionPolicy`). A primary cut
  off by a partition hears nothing until the partition heals.
- **`ACK-POLICY disk` does not resume writes.** `docs/replication.md`
  ("Manual promotion") tells operators that a newly promoted primary
  with no replica can "send `ACK-POLICY disk` to resume writes under the
  single-copy policy". The swap releases replies held at the gate, but
  the verdict never reads the policy, so the halt continues. As
  documented, the playbook does not work, and a promoted primary with no
  replica has no way to resume writes short of a replica joining. Step 0
  below fixes it ahead of everything else.
- **The response stage parks in the gate.** One thread per stage
  (`response.rs`, `dpdk_response.rs`) waits inside
  `DurabilityGate` on each slot in order. Under `ram`, `disk+ram` or
  `two-disks`, the first slot in flight when the last replica leaves
  parks the stage until a replica returns or the policy is swapped.
  Every refusal (`RefusalQueue`) and every query reply queued after
  it waits for the whole halt.
- **Every slot is gated, queries included.** `slot_needs_gate` compares
  the slot's `wire_seq` with the durable position. A query's slot
  carries the last sequenced event's `wire_seq` (`pipeline.rs`:
  `next_wire_seq - 1`), so a query waits for everything sequenced
  before it, including the ticks a halted node keeps journaling.
- **During a halt, nothing is confirmed in full.** The policy evaluator
  (`transport-core/src/ack_policy.rs`, `evaluate_with_status`) returns a
  durable position of `0` whenever a clause needs more nodes than the
  cursor view holds, and an inactive replica's cursors are left out of
  the view. Under `ram`, `disk+ram` or `two-disks` with no replica
  connected, no slot passes the gate, including events a replica had
  fsynced before it died. That is conservative, and correct: only a
  returning replica re-opens the full path.
- **The gate caches its durable position.** `DurabilityGate` keeps
  `cached_durable_pos` so a slot already known durable passes without
  touching an atomic. Anything that releases a slot on a weaker
  condition must not write to it (decision 1).
- **The hang also reproduces under `ack-policy disk`** (Exchange Core
  report, primary + one replica, replica SIGKILLed, on 0.15.0, 0.16.0
  and `main` at `84d1b0eb`). There the primary's journal should confirm
  every slot, so something else is wrong. It is not explained by
  anything above, and the cause is **not yet known**. The gate's wait
  loop re-reads every cursor on every spin, which makes a gate that
  never re-evaluates unlikely. An untested hypothesis that fits the
  report: `disk` is `persisted>=1`, satisfied by whichever connected
  node fsynced furthest, so while the replica is connected its acks
  could mask a primary journal cursor that stops advancing on a
  tick-only batch, and killing the replica would expose the stall. Step
  1's test has to confirm or rule this out before anything is fixed.
- **When a replica counts as gone depends on the transport.** On DPDK
  the primary drops a replica at the replication liveness deadline
  (`REPLICATION_LIVENESS`, `replication/dpdk.rs`). Kernel TCP has no
  such deadline: the replica count drops on EOF, at once after a kill,
  but only after the kernel's retransmit timeout under a real
  partition.
- **Every request's reply ends in a `BatchEnd` frame**
  (`TAG_BATCH_END`), appended by the response stage from a pre-encoded
  buffer when `is_last_in_request` is set. One request is one event,
  so the terminator is a per-event slot in the wire format that costs
  nothing to vary.
- **`melin-client` rejects unknown reply tags.** `classify` turns any
  tag outside the reply set into `Error::Protocol`. A new terminator
  therefore fails loudly on an old client instead of being misread as a
  full ack.

## Why held writes cannot be refused or discarded

An event past ingress has a sequence number. By the time its reply waits
at the gate it is on the primary's journal (usually fsynced) and applied
to the application's state, and it may have reached a replica whose ack
never arrived. Refusing it with `ReplicaDisconnected`, which means "not
applied, safe to resend", would make a retrying client apply it twice.
Discarding it would mean truncating the journal and rolling the
application back, and would diverge from a replica that already holds
it. It can only end in one of two ways: confirmed later, or lost through
a failover to a node that never had it. The ingress refusal sits where
it does because ingress is the last point where "not applied" is true.

## Why pausing ticks does not unblock queries

The former queries item recommended stopping tick generation while
halted, on the grounds that the application state would then equal the
replicated prefix. That only holds if the halt lands on an idle node.
At load, the writes accepted just before the halt are applied but held,
so the state already contains events no replica has confirmed. A query
reading it must wait behind them or reveal them, ticks or not. Pausing
ticks also does nothing for the `disk` case, where ticks are not the
obstacle.

## Design decisions

### 1. Release on the primary's fsync, marked as degraded

While the node is halted for want of a replica, the gate gains a second
release condition: the slot's event is fsynced on the **primary's own
journal** (`journal_persisted_wire_seq`). A slot released that way is
answered normally, except that its request's terminator is a new frame,
`BatchEndDegraded`, instead of `BatchEnd`. While no replica is
connected, the evaluator confirms nothing in full (see "What the code
shows"), so every reply released during the halt is degraded. A full
`BatchEnd` comes back only once a replica is streaming again and the
policy confirms the slot.

It has to be the primary's journal cursor, not the policy evaluator's
`persisted>=1`, which counts any node's fsync. With no replica connected
the two agree, but the degraded condition is a statement about the
primary's disk and should read that cursor directly.

The degraded release keeps **its own position** and never writes to
`cached_durable_pos`. If it advanced that cache, every later slot would
pass the cached check without waiting and go out with a full
`BatchEnd`: a false ack, the one outcome this design exists to prevent.
The full path and the degraded path are evaluated separately on each
slot, and a unit test pins this ordering (step 3).

Under `ram`, a held event may still be only in the primary's memory. It
waits for the primary's fsync like any other (the journal fsyncs every
batch under `ram`; the fsync just stops gating replies). Before that
fsync nothing true can be said about it, and it stays held.

Under `disk` the condition never comes into play: the policy is met by
the same cursor, so every reply is a full `BatchEnd`.

The policy itself does not change. `/healthz` keeps reporting the
policy the operator set, and `melin_ack_policy_degraded` stays `1`.
Nothing is silently downgraded, because every reply that is weaker than
the policy says so.

### 2. Enter on the halt, after a grace period

Degraded release starts when the node has been halted (replica count
zero, the `HaltGate` condition) for a **grace period**, and stops as soon
as a replica is streaming again. It is driven by the halt, never by gate
lag: a slow but connected replica is backpressure, and its replies wait
as they do today.

The grace period keeps a short network blip from turning replies that
were about to be confirmed into degraded ones. A replica that comes
back within it, and catches up, confirms the held events in full. It is
configurable, with its default in a named constant of its own. It must
not borrow `REPLICATION_LIVENESS`, which exists only on DPDK. The grace
period starts when the replica count drops to zero, so on kernel TCP
under a real partition it runs after the retransmit timeout has already
passed. The plan accepts that: the halt itself starts no earlier.

When the halt ends, replies already sent as degraded are not followed by
a second, full ack (see "Not part of this item"). Events sequenced after
the replica is back are acked under the policy as usual.

### 3. Queries take the same release and the same mark

A query's slot follows the same rule as a write's: released in full when
the policy confirms its `wire_seq`, or, while halted past the grace
period, released once the primary's fsync covers it and terminated by
`BatchEndDegraded`. The mark then means "this answer may reflect events
that only the primary's disk holds". A position query on a halted venue
answers after one fsync instead of never.

This replaces the durable-prefix read path (serving queries from the
shadow stage's state) that the former item listed. That path gives a
fully confirmed but stale answer at the cost of a second read path. The
degraded answer is current, marked, and needs no new path.

### 4. Refusals stop waiting as a consequence

Refusals keep their place in each connection's order. They no longer
wait for the halt because the slots ahead of them now drain after one
primary fsync instead of waiting for a replica. The per-connection
release designs listed in the former item (a refusal released from inside
the gate wait, or a stage that parks gated replies per connection) are
then unnecessary.

A refusal always ends in a plain `BatchEnd`, never `BatchEndDegraded`.
It never enters the output ring and is never gated: it is a definite
answer ("not applied, safe to resend"), with nothing on any disk to
qualify. The response stages already terminate refusals this way, and
this design keeps it so.

### 5. One new reply frame, breaking by design

`TAG_BATCH_END_DEGRADED` is a new control tag, below `0x10`, neither
`0x00` nor `TAG_APP`, and added to `CONTROL_TAGS` so the existing
compile-time checks cover it. The server pre-encodes it beside
`batch_end_wire_frame` and picks one or the other per slot: a choice
between two buffers already in hand, with no cost on the normal path.

In `melin-client`, `Frame::BatchEnd` and `Reply::BatchEnd` take a value
saying what backs the reply. A suggested shape is
`BatchEnd(Ack)` with `Ack::Policy` and `Ack::PrimaryOnly`, rather
than a sibling variant or a `bool`, so every caller that matches a batch
end has to decide what a degraded one means to it. Both are breaking
changes, listed in `CHANGELOG.md` under **Changed**. An old client
connected to a new node gets `Error::Protocol` on its first degraded
reply, which fails loudly rather than reading it as a full ack.

### 6. The split-brain exposure is bounded and marked

Today, a primary partitioned from every replica under a replica-backed
policy can never ack anything, so a failover on the other side loses no
acked write. Degraded release changes that only for the writes the node
had already sequenced when the halt began (ingress refusal stops new
ones), and every such reply is marked. A client that needs the full
guarantee treats a degraded ack as unconfirmed and reconciles, which is
what a timeout forces it to do today. A client that accepts one disk
copy can treat it as done. The client decides, not the server.

### 7. An operator switch, on by default

Degraded release is a node setting, on by default. An operator who has
committed to strict fail-closed behavior, for instance to a regulator,
can turn it off. "Off" is today's behavior in full: held writes, queries
and refusals all wait for a replica or a policy swap. Releasing queries
and refusals while still holding writes would need a response stage that
parks each connection's gated reply separately, the redesign this plan
avoids. That option is left to whoever asks for it.

### 8. An explicit swap to `disk` lifts the halt

A node that loses its last replica halts under every policy, as today.
An operator's `ACK-POLICY disk`, sent while the node has no replica,
lifts the halt: it latches an override that the verdict reads beside
the replica count. That is the consent the manual-promotion playbook
already describes, and it makes the playbook work as documented. The
latch clears when a replica reconnects or the policy is swapped back to
one that needs a replica. After that, the next loss of the last replica
halts the node again.

A swap made while a replica is still connected sets no latch. Consent
counts only once the operator knows the node has none. So swapping to
`disk` ahead of maintenance that takes the replica down does not stop
the node from halting when the replica leaves; the operator swaps again
once it has.

Making the halt follow the policy value, so that `disk` never halts,
was considered and rejected. The halt is what stops a `disk` primary a
partition has cut off. Fencing reaches it only on contact (see "What
the code shows"), so without the halt it would keep sequencing and
sending full, unmarked acks for the whole partition while the other
side promoted: two primaries acking different histories, with nothing
marked. Whether an unattended, isolated primary should keep writing is
the lease item's question (see "Not part of this item"). An operator's
explicit swap answers it for one node, knowingly; a policy value set at
boot does not.

## Order of work

0. **An explicit swap lifts the halt (decision 8).** The admin
   `ACK-POLICY` handler sets the override latch when it swaps to `disk`
   while no replica is connected. The replication senders clear it when a
   replica starts streaming, and a swap to a replica-backed policy clears
   it too. `verdict` reads it beside the replica count: one more relaxed
   load on the reader path. Update the `halt.rs` module doc and
   `docs/replication.md`'s halt and manual-promotion sections, and add a
   `CHANGELOG.md` entry under **Changed**: operator-visible behavior
   changes. Tests: a node under `disk` that loses its replica halts; a
   swap to `disk` during the halt lifts it; a replica reconnecting
   clears the latch, so its next departure halts the node again; a swap
   back to a replica-backed policy with no replica halts again; a swap
   to `disk` made while a replica is connected does not stop the halt
   when it leaves. A bug fix on its own, landing first.
1. **Root-cause the `disk` hang.** Write the failing test first: a
   primary and one replica started through `melin-test-node`, policy
   `disk`, replica killed, then a query that must answer within a
   deadline. Also assert directly that the primary's own persisted
   cursor passes the last tick with no replica attached: the leading,
   untested hypothesis is that it stops advancing on tick-only batches,
   masked until now by the replica's acks. Fix whatever the test shows,
   whether or not that is the hypothesis. This is
   a bug fix on its own and lands separately, ahead of the steps below.
2. **The reply frame.** Add the tag in `melin-wire-protocol`, plus
   `Ack` and the changed `Frame` / `Reply` variants in `melin-client`,
   and update the example clients and test harnesses that match on
   batch ends. Add a `CHANGELOG.md` entry.
3. **The gate.** Add the degraded release condition to
   `DurabilityGate`, shared by the io_uring and DPDK stages, so they
   cannot drift. It reads the halt state (the replica count the
   `HaltGate` reads), the grace period and the operator switch, and
   returns which terminator the slot gets. The degraded release keeps
   its own position, separate from `cached_durable_pos`. Add a
   `melin_degraded_acks_total` counter and log at `warn!` on entering
   and leaving degraded release. Unit tests cover: no degraded release
   before the grace period, none under `disk`, none with the switch off,
   none while a replica is connected but behind, an un-fsynced slot
   under `ram` held until the primary's fsync, a full ack resuming once
   a replica streams again, and the cache trap. For the cache trap: after
   a degraded release, the next slot is still evaluated and still gets
   `BatchEndDegraded`, never a full `BatchEnd`.
4. **End-to-end tests, per policy, on both transports.** Under `ram`,
   `disk+ram` and `two-disks`, at load, kill the replica, and assert:
   the writes in flight get `BatchEndDegraded` once the grace period has
   passed; later writes get `ReplicaDisconnected`; queries answer with
   the degraded mark; refusals end in a plain `BatchEnd`; a replica that
   returns within the grace period produces only full acks; with the
   switch off, today's stall is unchanged. Run the same tests on DPDK
   through the netns runner.
5. **Docs.** In `docs/replication.md`: rewrite "Strict fail-closed
   semantics" (strict about full acks; after the grace period, a halted
   node answers with marked degraded acks), correct "Writes halt when all
   replicas disconnect" (refusals and queries are answered; remove the
   "does not wait on the ack policy" claim until it is true), and
   document the client-side meaning of a degraded ack. This is the
   reply-side half of the roadmap's "in-flight order contract on
   pipeline halt" item, and should land with it or link to it.

## Downstream impact (Exchange Core)

The gateway has to map a degraded ack onto its own client protocol,
either as a distinct execution status or as "unconfirmed, reconcile on
reconnect". Its position and stats queries gain a degraded flag, which
its operator tooling should show. Its test harness hits the new
`Frame::BatchEnd(Ack)` shape at compile time.

## Not part of this item

- **Taking new writes on replica loss under a replica-backed policy.**
  If held writes can be answered with degraded acks, so can new ones.
  That would make the halt an operator choice (`halt`, today's ingress
  refusal, or `degrade`, which keeps taking writes and marks every
  reply).
- **A leader lease as the split-brain guard.** Replica count is a poor
  proxy for "still the primary". A lease from the raft control plane
  would let a primary that holds quorum keep writing and stop an
  isolated one before the other side can promote. It is the real answer
  to whether an isolated `disk` primary should keep writing unattended,
  which decision 8 deliberately leaves alone, and it is what `degrade`
  above would need. It deserves its own item and design.
- **A later "now confirmed" notice.** When a replica catches up, events
  that got degraded acks become fully durable, but their clients have
  already been answered. A follow-up notice per write would add a
  second message on every such write and a new frame type. For now,
  clients reconcile on reconnect or watch the health endpoint.
