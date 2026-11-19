# Divergence: halting the whole cluster, an evaluation of two carriers

Status: **review**, 2026-10-27, revised 2026-10-28. Evaluates two proposals
to change [divergence-detection-2026-10.md](divergence-detection-2026-10.md)
(cited `plan:N`), in the light of its
[assessment](divergence-detection-assessment-2026-10.md) (`assessment:N`).

- **Part 1, raft as the carrier: rejected by the user.** The halt is not
  to ride raft: no raft log entry, no forwarding RPC, no halted flag in
  raft's state machine. Part 1 is kept because some of its findings do not
  depend on the carrier. Part 2 cites them rather than repeating them:
  problem 2's stale leader (now a prerequisite), problem 3's new-cluster
  traps, and problems 8 and 9 (clusters without raft, the manual
  `PROMOTE` override, who is trusted to stop the cluster).
- **Part 2, the data plane only: the design to build**, with the changes
  its own verdict lists. Start there.
- **What the plan adopted (2026-10-28).** Part 2's design, with problem
  1's halt-only mode reduced to a bounded linger: a stopped node keeps its
  replication endpoint up for `2 × MAX_BACKOFF` (60 s), then exits. A node
  that cannot reach anyone in that time stops alone, a limit the plan
  documents. Keeping the notice alive until it is delivered, across
  restarts, is a roadmap item. Problem 6 is the plan's "a failed marker
  write still lingers".
- **Since landed on `main` (2026-11-03).** The stale leader (Part 1,
  problem 2; Part 2, problem 2) is fixed by `fix/raft-promotion-lease`.
  An age filter refuses at decision time, and a fresh majority round
  trip confirms the leadership before the promotion journals its epoch.
  The confirmation starts after the grace ran out, so it covers Part 2's
  tightened-gate concern without the formula proposed there. The plan's
  open question 3 is closed on that basis.
- **The plan trimmed (2026-11-06).** The plan was cut to the essentials
  of detection, and renumbered. Every `plan:N`, step and decision number
  cited here refers to the full plan, in git history (the plan file at
  b403d38b). The trimmed plan keeps the data-plane stop but defers the
  stopped primary's linger: only replicas detect there, so a primary
  stops only once a replica has, and with voters capped at three
  (`MAX_VOTERS`), two stopped voters already leave no quorum.

Code is cited as `file:line` against branch `plan/state-divergence-detection`
at 3d0ec3d4. Bare names are in `crates/core/raft/src` (`driver.rs`,
`storage.rs`, `types.rs`, `wire.rs`, `rpc_server.rs`, `recency.rs`,
`network.rs`) or `crates/core/server-runtime/src` (`raft.rs`,
`raft_promotion.rs`, `server.rs`, `halt.rs`). openraft is cited from its
0.9.25 source, the version in `Cargo.lock`, as `openraft:path:line`.

## The decision this serves, and the proposal in one paragraph

Settled by the user: a detected output divergence means the application's
determinism contract is broken. Fault does not matter. The whole cluster
stops, raft included, and the only way back is a new cluster, on a fixed
build or with output digests off. No repair, no attribution. Settled
since: raft does not carry the halt.

The first proposal (Part 1) makes the halt a raft entry, `ControlRequest::Halt(record)`,
proposed by whichever node detects (and by the primary on a replica's
`Diverged` notice), forwarded to the leader by a new RPC, applied by every
node as "latch, marker, exit", persisted so raft never campaigns or votes
again, checked by a new leader before it promotes, and backed by a
data-plane halt frame from the primary to its replicas. It drops the
majority verdict, the fault-based runbook and snapshot archival.

The second proposal (Part 2) keeps the stop on the data plane alone, and
relies on raft's quorum rule only to keep stopped nodes from being
outvoted.

# Part 1. Raft as the carrier (rejected)

## Verdict

*Written before the user rejected raft as the carrier; Part 2 is the
design to build.* Raft is the right carrier, and the proposal is the right shape. It is the
only mechanism on offer that binds nodes which saw nothing (the primary
when a replica detects, a replica that was disconnected) and that holds
across a reboot. The simplifications it buys are real: the "same window"
aggregation, the `Departure` halt cause, the archive threshold and its
re-run, and the fault-branching runbook all go.

As written, it has three defects that would make it fail in ordinary
cases, and two claims that are stronger than what it delivers:

1. The stop and raft share one shutdown flag, and a fenced node stands
   down in raft, so the halt may never commit (problem 1).
2. Promotion reads the elected term and never the log, a stale leader cut
   off from its quorum can still promote (a gap that exists today), and
   the halted state would not be raised by a raft snapshot install or at
   boot (problem 2).
3. "Start a new cluster from the journal" has two traps today's code sets:
   a history that does not reach genesis, and a new raft cluster whose
   terms trail the journal's fencing epoch, which turns off automatic
   failover in the new cluster (problem 3).
4. "Every node sees it, including one that rejoins later" does not hold
   once the nodes that applied it have exited. What holds is weaker and
   enough: no raft promotion is possible after commit (problem 4).
5. The detection-to-commit window is narrower than the proposal fears,
   because the primary's 3 s link-down grace and the halt frame already
   cover most of it. What is left is a partition, which nothing closes
   (problems 5 and 6).

The recommended design at the end fixes all five, at roughly the cost the
proposal names.

## Ranked problems

| # | Problem | Harm | Fix size |
| --- | --- | --- | --- |
| 1 | Raft stops with the data plane and fenced nodes stand down, so the halt may never commit | High: the halt silently fails | Small |
| 2 | Promotion waits for nothing applied and ignores quorum contact; halted state not raised on snapshot install or boot | High: a new or stale leader promotes over a committed halt | Small |
| 3 | The new-cluster procedure: genesis coverage, and terms trailing epochs disable auto-failover | High: the only way back is a trap | Medium (procedure, test) |
| 4 | Commit does not reach every node; the guarantee to state is quorum intersection | Medium-High: wrong guarantee text, nodes left in limbo | Small |
| 5 | The detection-to-commit window | Medium | Small |
| 6 | Minority partition, and a detector killed before its halt commits | Medium | Medium |
| 7 | Raft storage and wire compatibility | Medium | Small, but must be in the first release |
| 8 | Clusters without raft, and raft down on some nodes | Medium (documentation) | Small |
| 9 | Raft's scope statement and the trust it now carries | Low-Medium | Text |

### 1. Raft stops with the data plane

**What the code does.** The raft driver is handed the process shutdown
flag (`raft.rs:213-217`, `raft.rs:268`, `server.rs:1135-1149`) and leaves
its loop on it within one 100 ms poll (`driver.rs:84`, `driver.rs:418`,
`driver.rs:485`). `RaftDriverGuard::drop` sets the same flag and joins the
driver (`raft.rs:186-205`). The plan's latch co-sets the node's shutdown
flag at the moment of detection (plan:1153-1157), the way
`fence_if_superseded` does (`transport-core/src/fence.rs:99-108`). So step 3
of the proposal, "keep raft running until the halt is committed", is
impossible as wired: the latch stops raft about as fast as a proposal can
cross into the raft thread, be forwarded and be committed.

**Why it bites in the common case.** Most nondeterminism differs between
two copies in one process, so every node's shadow tends to trip
(assessment:126-139). With raft stopping on each node's latch, nodes leave
the quorum one after another while their proposals are in flight. Whether
any commits is a race against a 100 ms poll.

**Options.** (a) Give the driver its own stop flag, set by the node's exit
helper after the halt is committed and lingered on (problem 4), or after
an ordinary shutdown. (b) Keep the shared flag and propose synchronously
before latching. That delays the latch by a raft round trip, which breaks
"no reply after the latch" (plan:1270-1275), and it blocks forever without
a quorum.

**Recommendation.** (a). The latch keeps setting the node's shutdown flag
for every data-plane stage; raft gets `raft_stop`, owned by the guard. The
guard's `Drop` sets `raft_stop` only after the exit helper has finished
its halt wait, so an early `?` return still stops raft.

**The stand-down is the second half of the same defect.** Decision 11 has a
fenced node stand down in raft, and the code does it unconditionally:
`election_should_stand_down` returns `FENCED` (`raft_promotion.rs:599-601`)
and the driver applies `elect(false)` (`driver.rs:445-448`). The latch
fences the node, so in the every-shadow-trips case every node stops
campaigning. If the leader is then lost, no leader is ever elected and no
Halt commits. That fails safe, since nothing promotes, but the detectors
wait forever and their `warn!` wrongly says the rest may be serving. The
fix is to tell a halting node from a superseded one: a node fenced by a
newer epoch stands down as today, while a halting node keeps campaigning
and voting. Its promotion is still refused by the `fenced` gate
(`raft_promotion.rs:240-242`), and while it holds leadership no other node
can auto-promote. This is the change to decision 11 the proposal flags.

### 2. Promotion reads the term, never the log, and the halted state has three entry points

**What the code does.** `ControlRequest` is a unit `Noop`, and its own
comment says "Auto-promotion reads the elected term, never the log"
(`types.rs:10-16`). `consider_auto_promotion` gates on `role == LEADER`
and loads `term` (`raft_promotion.rs:332-338`); nothing waits for an
applied index. openraft sets the leader state and appends the term's blank
entry in the same step (`openraft:src/engine/engine_impl.rs:656-678`), so a
node reports `Leader` before that entry commits, and so before any entry
committed by an earlier leader has been applied locally. A new leader can
therefore file a promotion in the milliseconds before it applies a Halt
that is already committed and already in its log.

**The precise check.** openraft's own linearizable-read rule is "applied
through `max(this leader's blank entry, committed)`"
(`openraft:src/engine/handler/leader_handler/mod.rs:116-120`,
`openraft:src/raft/mod.rs:572-591`). Entries apply in order and the blank
entry is the term's first, so with `single-term-leader` the equivalent
test is `metrics.last_applied.leader_id.term == metrics.current_term`
(`LogId` carries the committing term, `openraft:src/log_id/mod.rs:25-32`).
The driver already copies metrics into `RaftStatus` (`driver.rs:455-460`).
One more atomic, `applied_own_term`, lets the promotion thread stay
synchronous and its decision stay a pure function
(`raft_promotion.rs:236`). Once the Halt applies, the latch fences the
node and the existing `fenced` refusal does the rest
(`raft_promotion.rs:240-242`).

**A stale leader passes that check.** openraft 0.9 never steps a leader
down for losing its quorum: `leader_step_down` runs only when a membership
without the leader commits (`openraft:src/engine/engine_impl.rs:526`,
`openraft:src/core/raft_core.rs:1326`). Take three nodes, replica R1
leading at term t, then cut off. B and C elect a leader at t+1, a
divergence is detected, and the Halt commits on B and C. R1 still reports
`Leader`, applied its own blank entry long ago, has had its primary link
down past the grace, sees its peers as `Silent` (which does not block,
`raft_promotion.rs:268`), and its term is above the fence epoch. So
`consider_auto_promotion` (`raft_promotion.rs:313-389`) promotes a node
that never saw the Halt, after the commit. The gap exists today, halt or
not: an isolated stale leader can auto-promote beside a live majority. The
fix is a quorum-contact gate: openraft publishes
`RaftMetrics::millis_since_quorum_ack` (`openraft:src/metrics/raft_metrics.rs:76`);
the driver bridges it into `RaftStatus` beside `applied_own_term`, and
promotion is refused unless it is below the election timeout minimum
(1 s, `driver.rs:46`). A leader that cannot hear a majority within that
time may already have been replaced.

**The halted state must be raised from three places, not one.**
`FileStateMachine::apply` persists `SmData` on every apply
(`storage.rs:601-622`), so a `halted` field there is durable before the
node acts on it, and survives log compaction because the snapshot blob is
`SmData` (`storage.rs:571`). But a follower that falls behind the purge
point receives the state by `install_snapshot` (`storage.rs:632-660`), and
`apply` is never called for the entries a snapshot covers. A restarted node
loads `SmData` at `open` (`storage.rs:431-434`) and calls neither. The
"halt observed" hook must fire from all three. Compaction is rare here
(openraft's default `since_last:5000`, `openraft:src/config/config.rs:166-171`,
against a log that grows only at elections), which is exactly why a
missing install path would survive testing.

**Recommendation.** `applied_own_term` and recent quorum contact gate
auto-promotion, the latter fixed as its own bug first; the hook
fires from `apply`, `install_snapshot` and `open`; the first record wins
and later ones are kept only for the log. A unit test per entry point.

### 3. "Start a new cluster from the journal" with today's code

What it concretely means, and where it breaks:

- **A new cluster is fresh `--raft-dir`s.** That is already the documented
  procedure for replacing a voter (`docs/replication.md:1310-1315`), and it
  is safe here because no peer is live (the wiped-dir hazard,
  `docs/replication.md:790-795`, needs live peers). It discards the Halt
  with the old raft state, which is the intent.
- **The journal is reusable.** It is input-only, the new build reads it,
  and recovery verifies the chain. Genesis is not journaled again on an
  existing journal (`docs/journal.md:165`). The shape is the existing
  "Upgrading a Replicated Deployment" (`docs/journal.md:376-386`): one node
  keeps the history, the others start with clean directories and reseed.
  A replica that keeps its old journal is not refused: the connect-time
  chain check (`docs/replication.md:832-839`) accepts a prefix of the
  new primary's history and catches it up, and reseeds anything else
  automatically (`docs/replication.md:858-859`). But its own snapshots
  are as suspect as the primary's, so clean directories are the simpler
  instruction. The `--raft-peer` list can stay as it is: membership is
  read from the flags at a raft dir's first boot (`docs/replication.md:796-798`),
  so fresh dirs take the same list.
- **Trap 1: every snapshot is suspect, so the history must reach
  genesis.** With snapshots moved aside, recovery needs the journal from
  sequence 1 and refuses otherwise with `MissingHistoryPrefix`
  (`docs/journal.md:458`). A replica seeded by snapshot transfer holds only
  the segment from its snapshot on (`docs/replication.md:961-964`), and an
  operator may have trimmed archives. The runbook must say which node to
  start from (the one with the highest tip whose history reaches genesis),
  how to tell (it boots with no snapshot, or refuses with that error), and
  the fallback order if none does. First, copy the missing early segments
  into that node's dir from any node or archive that still has them: a
  healthy replica's segments are byte-for-byte the primary's
  (`docs/replication.md:816-818`), the segment chain check verifies the
  splice, and restoring them is that error's own remedy
  (`docs/journal.md:458`). Only if no copy survives anywhere is what is
  left a snapshot the broken build wrote, knowingly.
  Replay from genesis may be long; doing it once with `--standalone`
  and the fixed build, so its shadow writes a trustworthy snapshot, is
  the existing practice (`docs/replication.md:1318-1323`).
- **Trap 2: the new cluster has no automatic failover until raft terms
  pass the journal's epoch.** The journal keeps every `EpochBump`, and an
  auto-promotion journals `epoch = term` (`docs/internal/raft-control-plane.md:44-48`).
  Fresh raft dirs restart terms from the beginning, and
  `auto_promotion_decision` refuses while `term <= fence_epoch`
  (`raft_promotion.rs:292-297`). A cluster that ever failed over therefore
  comes back with manual failover only, until as many elections have
  happened as the old cluster's epoch. Voter replacement has the same
  defect today; this plan makes it the routine way back.
- **The served history and the replayed one may differ.** Clients were
  answered by a copy whose determinism was broken; the fixed build's
  replay is the state from now on. The runbook says so, and points at the
  journal as the audit trail. With digests off on the same build, the
  replay is one more draw of the nondeterminism, and the docs must not
  call that a recovery.

**Options.** (a) A documented procedure with an integration test that runs
it, the plan's existing standard for the way back (plan:1721-1728).
(b) A tool (`new-cluster`: verify genesis coverage, replay with the given
build, write a snapshot, provision raft dirs). (c) For trap 2: document
it; or raise a fresh raft cluster's term floor above the highest journal
epoch any peer advertises (each envelope already carries it, `wire.rs:38-44`),
for instance by triggering elections until the term passes it, or by
provisioning the vote file with that term.

**Recommendation.** (a) now, its test also pinning the term-below-epoch
refusal. (b) on the roadmap, merged with the assessment's offline
qualification tool (assessment:913). (c): document now; the term floor is
its own roadmap item, since it fixes voter replacement too.

### 4. Commit does not reach every node

The leader applies at commit; followers apply once they learn the commit
index. openraft's replication task queues a heartbeat carrying it as soon
as the commit advances (`openraft:src/replication/mod.rs:671-686`, the
`Replicate::Committed` arm), so a follower learns it within a round trip,
unless the leader exits before that heartbeat goes out. If every node
exits as soon as it applies, that is the race: the leader can leave before
any follower learns the index. The followers hold
the entry in their logs but have not applied it. Two can elect a new
leader, whose blank entry commits it, and then they apply and exit in
turn, until the last node alone, which has no quorum and never applies.
A node that was down at the time comes back to peers that have all exited
and cannot learn the Halt from anyone. So the proposal's "every node sees
it, including one that rejoins later" is false.

What does hold, and is the guarantee to state: a committed Halt is on a
majority, every member of that majority stops serving and refuses to
restart, and any later election needs votes from that majority. With
problem 2's two gates, **no raft promotion can happen after commit,
anywhere**: a leader elected later holds the Halt and applies it before it
may promote, and a leader elected earlier has lost quorum contact and is
refused. Without the quorum-contact gate the guarantee covers only later
leaders. Only a manual `PROMOTE` (problem 8) could bring a node left
behind into service.

**Recommendation.** A bounded linger: after applying, a leader keeps raft
running until its replication metrics show every reachable voter matched
at or past the Halt, plus one heartbeat so followers learn the commit
index. A follower lingers one heartbeat after applying. State the
guarantee as quorum intersection, not as universal delivery. A
"tombstone" responder that tells late nodes about the halt is not worth
building: the way back replaces every node anyway.

### 5. The detection-to-commit window

By case, with the raft path fixed as above:

- **A replica detects.** The primary is still serving. It stops on the
  replica's notice (data plane, one round trip) or on applying the Halt
  (milliseconds). No promotion can race, since the primary's links are up
  (`raft_promotion.rs:243-245`).
- **The primary detects.** Its replicas can promote only once their link
  has been down past `PRIMARY_DOWN_GRACE`, 3 s (`raft_promotion.rs:57`,
  `raft_promotion.rs:246-251`), and only on the raft leader. A commit
  takes a round trip and an fsync. The halt frame latches every connected
  replica first, so the existing `fenced` refusals apply to them
  (`raft_promotion.rs:240-242`, `raft_promotion.rs:600-601`).
- **What remains** is a replica that was already disconnected when the
  primary detected (so it missed the frame and its grace clock had started
  earlier), wins leadership, and passes its checks before the commit. The
  likeliest form is a leader change in the middle of the proposal, which
  drops an entry the old leader had not replicated. A stale leader
  (problem 2) widens this to after the commit unless the quorum-contact
  gate is in.

**Options.** (a) The leader refuses promotion while it holds a halt
proposal it has not committed (local state: it receives every forwarded
proposal). (b) A `halt_pending` field on every RPC envelope, gathered like
`PeerTips` (`recency.rs:114-162`), with promotion refused while any fresh
peer reports one. This covers leader changes too, but changes every
frame's encoding. (c) Accept and document.

**Recommendation.** (a) plus the halt frame plus problem 2's check. That
leaves a window only where the detector cannot reach the leader on raft
before the grace expires. (b) is not worth a wire change on every frame
for that residue. The docs state it as part of problem 6's limit.

### 6. Minority partition, and a detector killed before commit

- **A detecting replica cut off from the quorum.** Its notice usually
  still reaches the primary, since it just received the primary's digest
  on that link. If both the notice and raft fail, the majority keeps
  serving. Nothing can stop it: the evidence is on one unreachable node.
- **A detecting primary cut off from the quorum.** Its replicas promote
  after the grace and the majority serves, possibly on a copy that
  diverged, until the partition heals and the Halt commits and stops the
  new primary.
- **How long the detector waits.** As proposed, a detector that cannot
  commit waits forever without serving. An operator who kills it (or a
  host reboot) loses the proposal. The node then refuses to restart on its
  marker, and the rest of the cluster serves on, having been told nothing.

**Options.** (a) Wait indefinitely, `warn!` periodically. (b) Make the
marker the durable intent: it records `halt: pending`, and a node that
starts with a pending marker refuses the data plane but runs raft only to
propose and wait, then exits with the refusal. (c) Exit after a bound and
drop the proposal.

**Recommendation.** (b), as one function that both the exit helper and
startup call. Raft-only mode recovers its journal tip, so its vote filter
works (`rpc_server.rs:187-196`), and campaigns and votes as a halting node
(problem 1); the `fenced` gate refuses its promotion. The limit goes into
the guarantee as written: **a divergence seen only by a node cut off from
the quorum stops the cluster when the partition heals; until then the
majority may fail over and serve.**

### 7. Compatibility

- **Raft storage.** Adding `halted` to `SmData` (`storage.rs:292-298`)
  changes its postcard encoding. postcard is not self-describing, so
  `#[serde(default)]` cannot absorb a missing trailing field. `VERSION`
  must move (`storage.rs:62-64`), and `check_header` refuses every other
  version (`storage.rs:148-157`). If the new binary cannot read version 1,
  every upgrade re-provisions every raft dir, which resets terms and hits
  problem 3's trap 2 on every cluster that ever failed over. **The new
  release must read version-1 `sm`, `snapshot` and `log` files and write
  version 2.** That includes the blob inside the `snapshot` file: its
  `data` is an `SmData` encoding of its own (`storage.rs:571`), which
  `install_snapshot` decodes as `SmData` (`storage.rs:639`), so a
  version-1 snapshot's blob must be decoded as version 1 too, and in a
  mixed-version window an `InstallSnapshot` blob from one version fails
  to decode on the other even though its frame decodes. The flag day
  absorbs the second; the first is upgrade work to name. The log entries
  themselves do not change: postcard numbers
  enum variants by index, and `Noop` and openraft's payloads keep theirs.
- **Raft wire.** Frames carry no version (`wire.rs:36-58`;
  assessment:539-542). A `Halt` entry or a new `RpcBody` variant fails to
  decode on an older node (`wire.rs:113-114`), which closes the link. That
  happens only when a halt is in flight, and in a safe direction (the
  older node falls out of raft, so it cannot be promoted). The protocol-9
  flag day (assessment:533-557) covers it if 3.1 (a) stands. If 3.1 (b)'s
  N/N−1 negotiation is adopted, raft needs the same: add a version
  exchange to the raft handshake in this release, so the next raft change
  is not a flag day.
- **The record's encoding.** Raft should carry the record as opaque bytes
  in the replication codec (`transport-core`), shared with the data-plane
  halt frame, rather than a serde mirror type in `melin-raft`. serde stays
  confined to raft, and there is one encoding of the record.

### 8. Clusters without raft, and raft down

- **No raft.** The halt frame is the only cluster-wide signal. Connected
  replicas stop. A disconnected replica keeps no record, and an operator's
  `PROMOTE` brings it into service: the override the proposal names.
  Under raft, a manual `PROMOTE` overrides the halt in the same way,
  because the admin path never consults raft (`admin.rs:311-328`). The
  docs should say both, in the same words.
- **Raft down on some nodes.** The driver can stop on a fatal error while
  sequencing continues (`driver.rs:475-482`). On the detector: the halt
  rests on the notice and the halt frame. On another node: it does not
  apply the Halt, but it cannot win an election either. A node whose raft
  is down when it trips logs that its cluster halt rests on the data plane
  only, and its marker says so.
- **Raft down everywhere.** The cluster behaves as one without raft. The
  principle "raft quorum loss cannot halt trading"
  (`docs/internal/raft-control-plane.md:36-40`) survives. Only a committed
  Halt stops trading, never an unreachable raft.

### 9. Scope and trust

The crate doc, the type's comment and the contributor doc say raft carries
election, membership and epochs only (`lib.rs:3-8`, `types.rs:10-16`,
`docs/internal/raft-control-plane.md:10-14`); all three change. The
raft-to-data-plane fencing edge is armed only under auto-promotion
(`rpc_server.rs:143-182`, `raft.rs:250-254`); the halt hook must be armed
regardless, since an observational raft (`docs/replication.md:785-789`)
still commits it. Any raft peer or replication-key holder can now stop the
cluster for good; both are already trusted with failover, so this is
documentation, not a new boundary.

## Alternatives to raft as the carrier

- **Halt frame + marker, raft refusing promotion on a local marker.**
  Cheapest (a latched node is already fenced), but it binds only connected
  replicas, and not the primary when a replica's notice is lost: it fails
  the settled decision in that case.
- **Fencing-epoch bump.** Only a primary journals `EpochBump`, and an epoch
  fences primaries on contact only (`fence.rs:89-108`); it binds no idle
  replica, and it poisons the new cluster's epoch.
- **Poison journal entry.** Best reach (catch-up delivers it), but it
  breaks "input-only", needs the primary alive when a replica detects, and
  leaves the poison in the journal the new cluster restarts from.

Raft plus the halt frame is the only carrier whose guarantee (no promotion
after commit) follows from a property already relied on, at the moderate
cost the proposal names.

## Observability

- **Marker:** cause (`detected | cluster-halted`), detecting node, the
  record, `halt = pending | committed | no-raft`, time; no threshold or
  archive path. The startup refusal, from the marker or the raft state,
  names the node, check and window, and points at the new-cluster runbook.
- **Logs:** `error!` at detection and on each node that applies a Halt or
  receives a halt frame; `warn!` at intervals while a halt is uncommitted
  ("the rest of the cluster may still be serving"); `info!` at commit and
  exit. The "same window" `error!` (plan:1389) goes.
- **`/healthz` and metrics:** the stop cause beside the halt state, and a
  `melin_cluster_halt_state` gauge (none, pending, committed) to alert on
  a lasting pending.

## Test strategy

Raft is kernel TCP by design; the data plane runs on both transports, its
DPDK cases under the netns runner through `melin-test-node`.

- **Unit, ungated.** In `storage.rs`, `halted` survives reopen,
  `build_snapshot` → `install_snapshot`, and a purge behind the snapshot.
  Version-1 files are read and written back as version 2. The hook fires
  once from each of `apply`, `install_snapshot` and `open`, and the first
  record wins. openraft's `Suite::test_all` still passes. In `wire.rs`, the
  `Halt` entry and the forwarding body round-trip, and the forwarding body
  is pinned to its sender's key through `claimed_sender`, whose exhaustive
  match (`wire.rs:66-76`) forces the case. In `raft_promotion.rs`, the
  decision refuses without `applied_own_term`, without recent quorum
  contact, and while a local proposal is uncommitted; the stand-down
  refuses a superseded node and not a halting one. The linger's exit rule is a pure function, tested
  without tokio.
- **In-process raft clusters** (`src/tests/`): a follower proposes and
  every live node applies; the leader is killed in the middle of a
  proposal and the retry commits; a node lagging past the purge point
  receives the halt by snapshot. That last case needs a seam for the
  snapshot policy, which `driver.rs:287-293` fixes at openraft's default.
- **Integration** (`server-runtime/tests/output_divergence.rs`):
  - Three nodes, the primary's shadow trips. Every node exits, no replica
    journals an `EpochBump`, and every restart is refused.
  - Every node's shadow trips, and the raft leader is killed. A halting
    node is elected and the halt still commits, which pins problem 1.
  - A raft leader is cut off (proxy) and the others elect anew. It
    refuses to promote on quorum contact, which pins problem 2.
  - A replica detects. The primary stops on the notice and the other
    replica on the halt frame, on both transports.
  - The detector's raft port is cut through the test proxy
    (`tests/common/proxy.rs`). The majority promotes after the grace, and
    on heal the Halt commits and the new primary stops. This pins
    problem 6's documented limit.
  - A detector stopped before it can propose (a seam on the proposal)
    restarts in raft-only mode, and the others then stop.
  - Without raft, the halt frame stops the connected replicas, and a
    manual `PROMOTE` of a disconnected one works.
  - The way back: fresh raft dirs, markers removed, snapshots moved
    aside, the fault switched off through `Sizing`. The new primary's
    state equals a fresh replay, replicas reseed, the cluster serves, and
    auto-promotion refuses with the term-below-epoch reason.
- **Left untested:** a real power loss during the linger (the raft files'
  fsync discipline is tested as today), and asymmetric partitions, which
  the in-process tests cannot make (`docs/internal/raft-control-plane.md:169-175`).

## Recommended design

*Superseded by Part 2's design. Kept as the record of the raft carrier;
problems 2, 3, 8 and 9 above remain the parts Part 2 cites.*

1. **Detection** (unchanged): trip the latch, which fences the node and
   sets the data-plane shutdown flag, then `error!` and the marker, with
   `halt: pending`.
2. **Propagation, data plane:** the replica sends its `Diverged` notice,
   and a primary that receives one trips its own latch. On the way down a
   primary sends a halt frame (protocol 9, the same record encoding) on
   both senders, before they leave. A replica that receives one trips
   with cause `cluster-halted`.
3. **Propagation, raft:** every tripped node proposes
   `ControlRequest::Halt(record bytes)`. A follower forwards through a new
   `RpcBody` request to the leader, which calls `client_write` with a
   timeout and answers once committed. The proposer retries across leader
   changes, from `current_leader` in the metrics. Raft runs on its own
   stop flag, and a halting node keeps campaigning and voting; only a node
   superseded by a newer epoch stands down (problem 1).
4. **Apply:** `SmData.halted`, persisted, and the hook from `apply`,
   `install_snapshot` and `open`, which trips the latch with cause
   `cluster-halted`, armed whatever the auto-promotion setting. A leader
   lingers until its voters are matched, a follower for one heartbeat;
   then the marker reads `halt: committed`, and the node exits.
5. **Promotion:** refused unless `applied_own_term` and the leader heard
   a quorum within the election timeout, and on a leader while a halt
   proposal is uncommitted. The quorum-contact gate fixes a gap that
   exists today and ships first, on its own. The existing `fenced` refusals cover
   everything after the latch.
6. **Startup:** refuse when the marker exists or the raft dir's `sm` holds
   a halt (a read-only `melin_raft` function, called from
   `startup_store()`). A pending marker with raft configured runs
   raft-only mode, proposes and waits, then refuses.
7. **No archival, no threshold, no verdict, no `Departure` cause.**
   Snapshots are left in place; the runbook moves them aside.
8. **Compatibility:** raft storage version 2, which reads version 1. A
   raft version exchange if 3.1 (b) is adopted.
9. **The way back:** the documented, tested procedure of problem 3, with
   both traps stated. A roadmap row for the replay tool, merged with
   offline qualification, and one for the raft term floor.

The guarantee, as the operator docs would state it: once a divergence is
detected, the node that saw it never serves again. Once its halt commits,
no node can be promoted by raft, and every node that applies it stops and
refuses to restart. Connected replicas stop within a round trip. The
limits: a manual promotion overrides the halt, and a detector cut off from
the quorum stops the cluster only when the partition heals.

## Plan sections this changes

*Superseded by Part 2's list of the same name; do not apply this one.*

- **Guarantee and limits** (plan:32-80) and **finding 6** (plan:257-270):
  the cluster-wide stop and the limits above; problems 1, 2 and 4.
- **Decisions 8, 9** (plan:1023-1075, 1120-1124): the halt frame and one
  shared record encoding; the `Diverged` arm proposes the halt.
- **Decision 11** (plan:1148-1280): raft neither stops nor stands down at
  the latch; the "same window" test, the threshold and archival (step 5)
  go; the notice is a trigger; the exit waits for commit and linger. The
  token chain becomes `Halted → MarkerWritten → HaltProposed`.
- **Decisions 12-14, 16** (plan:1282-1347, 1379-1423): the refusal reads
  raft state and gives the new-cluster text; decision 14 becomes the raft
  design above; observability as above.
- **Steps 3-5** (plan:1647-2083): archival, aggregation (plan:1853) and the
  `Departure` cause (plan:1854-1873) go; raft storage, hook, forwarding,
  both promotion gates, the halt frame and the test cases above come in.
- **Step 6** (plan:2108-2134): the runbook becomes the new-cluster
  procedure, both traps stated. **Step 7**: roadmap rows for the replay
  tool and the term floor.
- **Open questions** (plan:2203-2336): 3 is moot, 7 now waits on raft;
  new ones for forwarding and compatibility. **Not part of this plan**:
  add attribution and repair.
- **Assessment:** 1.2 is resolved by the decision; 1.7 is unchanged; 3.1
  gains raft; 5.4 loses the "same window" definition and keeps the stop of
  snapshot transfers.

# Part 2. The data plane only

Cites code as Part 1 does, at the same commit. `tcp_sender.rs`,
`dpdk.rs`, `receiver_transport.rs` and `mod.rs` are in
`crates/core/server-runtime/src/replication`, `uring_teardown.rs` in
`crates/core/server-runtime/src`; `protocol.rs` and `cursors.rs` in
`crates/core/transport-core/src/replication`.

## Verdict

**The right direction, and simpler than Part 1, but not yet the
guarantee it claims.** In short: a detecting replica flags its acks, a
primary that detects or reads a flag sends a halt frame, and raft is
untouched because two stopped nodes out of three leave no quorum. What
holds, checked against the code:

- **A stopped node leaves raft at once, which is what this design wants.**
  The driver shares the node's shutdown flag (`server.rs:1133-1151`,
  `raft.rs:262-269`) and leaves within one 100 ms poll (`driver.rs:418-421`,
  `driver.rs:485`), and the latch sets it (plan:1153-1157). Part 1's
  problem 1 turns around: no stand-down change is needed.
- **A stopped node cannot come back into raft by restarting.** Both
  entries resolve the store (`server.rs:1039`, `server.rs:2563`) before
  any raft spawn (`server.rs:1133`, `1314`, `2636`, `2857`), and
  `melin_raft::spawn` has no other caller (`raft.rs:262`). The marker
  refusal sits right after the resolve (plan:1284-1299), so it comes
  before raft on all four paths.
- **A down voter still counts.** openraft's majority is over the configured
  set, `2 × granted > members` (`openraft:src/quorum/quorum_set_impl.rs:11-23`).
  With three voters and two stopped, the third never gathers a quorum.

What does not hold yet: "two rare events" is not the residue, because a
detector without a session is routine (problem 1); quorum loss blocks
elections, not a sitting leader (problem 2); and nothing caps the voters
at three (problem 3). With the changes below, the residue is the
partition the user accepted, and the design keeps its simplicity: no
raft change for the halt, one sticky bit, one new frame.

## Ranked problems

| # | Problem | Harm | Fix size |
| --- | --- | --- | --- |
| 1 | A detector with no live session loses its notice, and that state is routine | High: the cluster serves on | Medium |
| 2 | Quorum loss stops elections, not a sitting leader | High: an unaware leader promotes | Small (Part 1's gate, tightened) |
| 3 | Nothing caps the voters at three | High where configured: two stopped nodes are not a blocking minority | Small |
| 4 | The flag is read only where acks are read | Medium-High: the notice is lost in catch-up | Small-Medium |
| 5 | The halt frame on the way down | Medium | Small |
| 6 | A marker that did not persist | Medium | Small |
| 7 | Operator paths around the halt | Medium (documentation) | Small |
| 8 | Step 3 would ship a stop without the halt | Medium | Planning |
| 9 | Wire and upgrade | Low | Small |

### 1. A notice that misses its moment is lost, and the moments are routine

Each side tells only the sessions it has at the moment, and nobody
later. The shadow check (plan decision 10) trips regardless of the link,
and a replica is without a primary session after every failover (replicas
do not re-point, `docs/replication.md:799-802`), while a primary
restarts, during reconnect backoff of 1 s to 30 s (`mod.rs:864-869`),
after an eviction (`tcp_sender.rs:181-196`), and through a kernel-TCP
catch-up (problem 4). A primary with no replica connected stops alone,
and the two others are a quorum. "Already degraded" covers a replica
that is *down*, not one disconnected that comes back.

**Options.** (a) As proposed: wait within a bound, exit, accept the loss.
(b) **The notice outlives the session.** A tripped node stays up in a
*halt-only* mode: no pipeline, no clients, no raft. A replica redials
its `--replica-of` with the flag in its handshake; a primary keeps its
listener and answers every handshake with the halt frame. The marker
records `notice: pending | delivered`, and a restart with a marker enters
the same mode. (c) Rely on the operator seeing the refusal.

**Recommendation.** (b). It turns "connected at the instant of detection"
into "reachable at any point before the others fail over". The handshake
is the one frame both transports always read first (`tcp_sender.rs:479-486`,
`dpdk.rs:1026-1033`), and replicas point at the primary, so a halt-only
primary catches every replica that comes back, within its backoff. This
also answers Part 1's objection to this carrier ("Alternatives"). The
proposal's bound stays, but it only ends the current session, never the
notice. A forgotten halt-only node that halts a new cluster fails closed
and names itself.

Halt-only mode is not a second flag. The latch sets the shared shutdown
flag, every stage and raft leave on it (`receiver_transport.rs:975`,
`tcp_sender.rs:896`, `driver.rs:418`), and the exit helper then enters
halt-only as its own loop after the ordinary teardown. On DPDK that loop
reuses the port the node holds, since the EAL initialises once per
process (`crates/core/dpdk/src/dpdk/eal.rs:5`).

### 2. Quorum loss stops elections, not a sitting leader

A replica can hold raft leadership while the primary is healthy
(`raft_promotion.rs:301-311`). If that replica is the unaware node, it
needs no election: once its primary link has been down for
`PRIMARY_DOWN_GRACE` (`raft_promotion.rs:57`, `raft_promotion.rs:246-251`),
with its peers `Silent`, which does not block (`raft_promotion.rs:268`),
`consider_auto_promotion` promotes it (`raft_promotion.rs:313-389`).
Nothing keeps leadership off a replica, so this is not a corner case. Part 1,
problem 2 found the mechanism (openraft never steps down on quorum loss)
and a fix: refuse promotion unless `millis_since_quorum_ack` is below
the election timeout minimum (1 s).

**That fixed bound leaves a window here.** Take R1 leading, its primary
link down for 2.5 s (a reconnect backoff, an eviction) when the primary
halts. The primary and the other replica leave raft within 100 ms. R1's
grace runs out 0.5 s later, while its last quorum ack is still under
1 s old, and the peer-tip veto does not fire on a quiet cluster
(`raft_promotion.rs:259-268`). R1 promotes after the halt.

**Options.** (a) Part 1's gate as it stands, accepting the window.
(b) Make a leader step down on quorum loss, which openraft 0.9 does not
offer: a fork or an upgrade. (c) Tighten the gate to a quorum ack taken
after the grace ran out.

**Recommendation.** (c). The gate asks for a quorum ack *after the grace ran
out*: `millis_since_quorum_ack < primary_link_down_for − PRIMARY_DOWN_GRACE`,
as well as under the election timeout. A live majority answers within
one heartbeat, so a real failover waits that much longer; peers that
stopped before the grace ended never answer, and R1 refuses for good.
What is left is a halt inside one heartbeat after that ack. The gate
ships before the first release in which a divergence can stop a
replicated node, a blocker rather than a follow-up, on its own branch
since the stale-leader gap exists today.

### 3. Nothing caps the voters at three

`build_raft_config` checks only a minimum, under auto-promotion
(`raft.rs:81-98`), as do the docs (`docs/replication.md:784-788`), and
data-plane-less voters are a non-goal, not a refusal
(`docs/internal/raft-control-plane.md:13-14`). A fourth or fifth melin
node can be a voter, a standby replica waiting for one of the primary's
two slots (`tcp_sender.rs:271-277`, `transport-core/src/pipeline.rs:927`).
With five voters, two stopped nodes leave a majority; with four, they
still block.

**Options.** (a) Refuse more than three: one plus the slot array's
length, so the bound moves with it. (b) Allow four. (c) Document only.

**Recommendation.** (a), always, not only with digests on: a voter that
can never be a connected replica adds nothing. Checked twice: on the
`--raft-peer` flags in `build_raft_config`, and at driver start on the
*stored* membership, which overrides edited flags after first boot
(`driver.rs:351-400`, `docs/replication.md:796-798`); a dir initialised
with five otherwise keeps five. Breaking, called out under **Changed**.

With one configured replica there are two voters, and auto-promotion is
refused already (`raft.rs:91-98`). Without raft, or with it
observational, nothing auto-promotes; only problem 7's paths remain.

### 4. The flag is read only where acks are read

- **The replica acks only on progress** (`receiver_transport.rs:1037-1061`)
  and leaves its loop at the first shutdown check (`receiver_transport.rs:975-986`),
  so it needs a new loop after the latch. On a reliable stream repeating
  the flag adds nothing; what matters is who reads it.
- **Kernel TCP reads acks in the live loop only** (`tcp_sender.rs:1025-1051`),
  not through catch-up, snapshot transfer or bridge (`tcp_sender.rs:573-663`).
- **DPDK keeps only the newest ack while joining** (`keep_last_ack`,
  `dpdk.rs:231-282`, `dpdk.rs:1162-1179`): a sticky bit survives, a single
  flagged frame would not.
- **Every read site skips what it does not decode** (`tcp_sender.rs:1155`,
  `dpdk.rs:1338-1340`, `dpdk.rs:260`), so the plan's separate frame would
  have been dropped too: folding it into the ack is right.
- **`record_ack` stores, it does not take a maximum** (`cursors.rs:176-182`),
  so the frozen value must be the last one sent (`receiver_transport.rs:1055-1056`).

**Options.** (a) Each sender checks the flag where it reads acks today.
(b) Make kernel catch-up read acks too. (c) Trip in the one place every
ack passes, and leave unread stretches to the flagged handshake.

**Recommendation.** (c): `ReplicaCursors::record_ack` (`cursors.rs:149`)
trips the latch *before* it stores, so neither sender can forget it and a
flagged ack moves no cursor; `keep_last_ack` and the handshake do the
same, the handshake after authentication and before the epoch and chain
checks (`tcp_sender.rs:476`, `tcp_sender.rs:501-539`), so a flagged
replica is never sent a snapshot. Frozen positions satisfy nothing new,
so under every policy the gate runs on the other replica until the latch,
which hands no slot (plan:1170-1172), and no degraded stretch starts.
Kernel catch-up need not read acks: a replica that hears no halt within
its bound redials with the flag in its handshake (problem 1).

### 5. The halt frame on the way down

- **Kernel TCP has no single sender.** Each slot's handler owns its
  socket, returns at the first shutdown check (`tcp_sender.rs:894-898`),
  and its guard shuts the socket both ways (`uring_teardown.rs:49-53`).
  A close with unread acks buffered can reset the link and lose the
  frame. So each handler writes the halt, shuts its write side only, and
  reads until the replica closes or the bound passes.
- **DPDK** already lingers on the way out (`dpdk.rs:196-218`,
  `dpdk.rs:1526`); the halt is queued on every slot at a frame boundary
  (`dpdk.rs:1190-1192`).
- **A replica in a snapshot install** cannot read a halt there. The
  primary closes the link, and the replica meets the halt-only listener
  at its next handshake (problem 1).

**Options.** (a) Close the stream with a halt reason and no frame,
which the kernel guard's reset can lose. (b) A halt frame then the
guard's close, as today. (c) A halt frame, a half-close, then drain until
the replica closes or the bound passes.

**Recommendation.** (c): one "send halt, then close" per transport, on
every exit path once the latch is set. The halt frame is parsed in two places
only: the streaming loop, and the handshake answer.

### 6. A marker that did not persist

A node whose marker write fails exits with an I/O error (plan:1260-1263),
restarts, rejoins raft, and with the unaware node makes two of three.
**Options:** (a) exit, as the plan has it; (b) retry the write, then
exit; (c) never exit. **Recommendation:** (c), since only a running
process holds the halt once the disk will not: it stays halt-only, logs `error!`
at intervals and reports on `/healthz`. A host reboot still loses it: a
residue to document. Assessment 3.4's `<journal>.refuse` should land with
the first marker.

### 7. Operator paths around the halt

The docs name them in one place:

- **Manual `PROMOTE`** of the unaware node (Part 1, problem 8).
- **Observational raft steers the operator to `PROMOTE` the leader**
  (`docs/replication.md:684-688`), which may be stale during a halt.
- **Voter replacement** (`docs/replication.md:1310-1315`) looks like the
  fix for "two nodes refuse to start"; the refusal text must say the
  cluster is halted and point at the new-cluster procedure (plan:1302-1312).
- **The unaware node's snapshots are as suspect as the others'.** The
  runbook treats every node alike: stop all, halt-only ones included,
  move each marker aside, kept as the incident record, then follow Part 1,
  problem 3, both traps included.

**Options.** (a) Refuse `PROMOTE` on a node that has seen a halt, which
cannot cover the unaware node, the case that matters. (b) Document the
paths. **Recommendation.** (b), in one "what overrides the halt" section
of the operator docs, with the refusal text pointing at it; (a) adds
nothing, since an aware node has already stopped.

### 8. Step 3 would ship a stop without the halt

Step 3 adds the shadow check and the stop on every node (plan:1647-1763);
protocol 9 arrives in step 4. A primary whose shadow trips under step 3
alone stops, and its replicas fail over: the opposite of the decision,
and of plan:1341-1347, whose test pins exactly that failover.
**Options:** (a) keep the order, shipping step 3 with the shadow check
off; (b) reorder so protocol 9 lands first; (c) one release for steps 3
to 5. **Recommendation:** (c), since (a) ships a check whose stop
contradicts the decision once switched on, and (b) a protocol bump with
nothing to carry: the halt frame and the flags move into step 4, and
steps 3 to 5 share one release, together with problem 2's gate. Each stays
its own commit.

### 9. Wire and upgrade

All of it fits protocol 9's flag day (plan decision 8, assessment 3.1):
no second bump.

- `AckFrame` (`protocol.rs:227-233`, `protocol.rs:310`) gains `flags: u8`
  and `window_end: U64`, 17 to 26 bytes; an unknown bit is refused, as
  `genesis_known` is (`protocol.rs:290-293`).
- `HandshakeFrame` gains the same fields after `protocol_version`, so a
  mixed pair is still refused: an old primary parses the longer v9
  handshake's prefix and fails the version check (`protocol.rs:221-224`,
  `protocol.rs:604-608`); a v9 primary fails earlier on the shorter v8
  frame, with the "too short ... older, incompatible replication protocol
  version" error (`protocol.rs:594-602`).
- A new `MSG_HALT` carries them plus who detected; `MSG_DIVERGED` goes
  (plan:1028). Raft's wire and storage do not change (Part 1, problem 7
  is moot).

**Options.** (a) The flag alone, no window end. (b) Flag and window end.
**Recommendation.** (b): the cost is nine bytes on every ack, and it
lets the primary's log and marker name the position.

## The residue

As the operator docs would state it: once a divergence is detected, the
node that saw it never serves again, and every node it can reach,
directly or through the primary, stops. A detector cut off from both
others while they reach each other (a replica not yet re-pointed after a
failover, for one) stops the cluster only when it reaches one of them
again; until then they may fail over and serve. A manual promotion
overrides the halt. "Nondeterminism is systematic, so the others'
shadows trip too" is a probable backstop, not a guarantee
(assessment:126-139); "the common path implies the link just worked"
holds for the primary check only (problem 1).

## Observability

- **Marker:** `cause = detected(check) | cluster-halted(by)`, with `by`
  the replica's slot and key fingerprint or "primary", the path
  (`ack | handshake | halt frame`), `notice = pending | delivered | none`,
  the window end, time. No threshold and no archive path.
- **Logs:** `error!` at detection and on each node a flag or halt
  trips, naming who told it; `warn!` at intervals while a notice is
  pending; `info!` once delivered.
- **`/healthz` and metrics:** the stop cause, and
  `melin_cluster_halt_notice{state="pending"|"delivered"}` to alert on.

## Test strategy

Data-plane cases run on both transports, the DPDK ones under the netns
runner through `melin-test-node`. Raft is kernel TCP.

- **Unit, ungated.** `protocol.rs`: both frames round-trip with every
  flag combination, unknown bits are refused, a v8 handshake gets the
  too-short error and a v9-sized one with version 8 the version error.
  `cursors.rs`: a flagged ack trips before it stores. `keep_last_ack`:
  the flag survives the newest-ack rule. Its tests are DPDK-gated today,
  since `dpdk.rs` compiles only under the `dpdk` feature
  (`mod.rs:75-76`); hoist the function and its `JoinFrames`
  (`dpdk.rs:222`), which need no hardware, into `receiver_transport.rs`
  beside the `try_extract_frame` and `FrameResult` they use
  (`receiver_transport.rs:103`), ungated, so the test runs on every host.
  `receiver_transport.rs`, scripted transport: after the latch, one
  flagged ack with the last positions sent, the wait, then a flagged
  handshake. `raft.rs`: four voters refused from the flags, and the
  driver refuses a raft dir initialised with four. `raft_promotion.rs`:
  a quorum ack older than the grace's end refuses, a newer one passes.
  The halt-only mode's decisions and the marker's `notice` states, as
  pure functions.
- **Integration**, `output_divergence.rs`, faults through `Sizing`:
  - a replica detects: the primary stops on the flag, the other replica
    on the halt;
  - the primary detects: both replicas stop, nothing promotes;
  - a replica detects mid catch-up (kernel TCP, a large lag): the primary
    stops at its next handshake;
  - DPDK join phase (netns runner): a replica flags during catch-up and
    during a snapshot transfer, and `keep_last_ack` trips the primary; a
    primary halting while a slot is joining closes it, and the replica
    stops at its next handshake;
  - the primary detects while a replica, made raft leader first, is cut
    off by the test proxy (`tests/common/proxy.rs`), once well before the
    halt and once about 2.5 s before it: past the grace, it refuses to
    promote; released, it meets the halt-only primary and stops;
  - restart with a marker: a detector restarted with `notice: pending`
    stops a live primary through its flagged handshake, and a restarted
    halt-only primary stops a returning replica;
  - a failed marker write (a seam): the node stays halt-only and never
    rejoins raft;
  - the way back, Part 1 problem 3's test, every node stopped first.
- **Left untested:** a routing split with asymmetric reachability (the
  proxy cuts one link), and a host reboot while a notice is pending.

## Recommended design

1. **Detection** as the plan has it, the marker with `notice: pending`;
   raft leaves on the shutdown flag.
2. **Replica:** one flagged ack with its last positions; no halt within
   the bound, redial with a flagged handshake until one answers; then
   `notice: delivered` and exit. On a halt frame: `cluster-halted`,
   marker, exit.
3. **Primary:** trips in `record_ack`, `keep_last_ack` and the handshake
   (after authentication), sends the halt and closes on every slot, then
   stays halt-only.
4. **Startup** with a marker: halt-only, never raft, on a primary or a
   pending notice; otherwise the plan's refusal. A failed marker write
   stays halt-only.
5. **Raft:** no change for the halt; three voters at most, flags and
   stored membership; problem 2's gate first, on its own branch.
6. **Removed**, as proposed; **the way back** is Part 1, problem 3, every
   node stopped first.

## Plan sections this changes

- **Guarantee and limits** (plan:32-80), **finding 6** (plan:257-270):
  the cluster-wide stop and the residue; no raft change for the halt, but
  the quorum gate and the voter cap.
- **Decisions 8, 9** (plan:1023-1124): `MSG_DIVERGED` goes, the flags
  and `MSG_HALT` come in; the `Diverged` arm runs the notice loop.
- **Decisions 11, 12** (plan:1148-1323): the notice loop replaces step 4,
  archival (step 5) goes, the token chain ends at `MarkerWritten`, the
  exit helper and startup enter halt-only mode; the refusal points at the
  new-cluster procedure.
- **Decision 13's "A diverged node" row** (plan:1336): stopped, and
  halt-only rather than refusing while its notice is pending or it was
  primary.
- **Decisions 14, 16** (plan:1339-1347, 1379-1423): *nothing* promotes,
  inverting decision 14's test; observability as above.
- **Steps 3 to 5** (plan:1647-2083): the flags and the halt frame move
  into step 4, one release for the three; the aggregation (plan:1853),
  `Departure` (plan:1854-1873) and their test (plan:2013-2017) go.
- **Step 6** (plan:2108-2134): the new-cluster runbook, with problem 7's
  paths. **Step 7**: Part 1's term floor and replay tool.
- **What this leaves untested** (plan:2187): the two residues of the
  test strategy above.
- **Open questions**: 3 is moot; 4 is answered (markers go only as part
  of a new cluster); 7 (plan:2266-2273) is settled for raft, which leaves
  on the shared flag even if a stage hangs, and its "refuses to restart"
  becomes halt-only where problem 1 says so. **Assessment:** 1.2
  resolved; 3.4 joins problem 6.
