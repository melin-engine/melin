# Control-Plane Raft (contributor notes)

The `melin-raft` crate and its wiring in `melin-server-runtime` implement
the control plane described operationally in `docs/replication.md`: leader
election, static membership, and fencing-epoch allocation. This document
covers the design arguments a contributor needs before touching it;
decided but unbuilt work is in `raft-control-plane-follow-ups.md`.

## Scope and non-goals

The control plane carries **election, membership, and fencing epochs —
nothing else**. Order flow stays on the synchronous replication data
plane with its ack policies untouched. Non-goals on this iteration:
runtime voter changes (static `--raft-peer` lists only), client
redirects / follow-the-leader, witness (data-plane-less) voters.

## Architecture

One dedicated `raft-driver` thread per node owns:

- an [openraft](https://docs.rs/openraft/0.9) `Raft` instance (0.9.x),
- a **current-thread tokio runtime** — openraft requires an async
  runtime; confining it to this one thread keeps the rest of the
  codebase synchronous and the hot path untouched,
- the control-plane TCP listener (kernel TCP always, even on DPDK
  nodes — the control plane must not consume a DPDK queue),
- a metrics bridge copying `RaftMetrics` (term, leader, role, and the
  age of the leader's last majority acknowledgement) into the lock-free
  `RaftStatus` atomics the health endpoint and the auto-promotion
  thread read (`publish_metrics`),
- a confirmation task (`serve_confirmations`) answering the blocking
  `LeaderConfirm::confirm` the server thread calls when it mints a
  promotion's epoch (see "The leader-lease gate" below).

Everything that *acts* on elections lives outside the async world:
`raft_promotion.rs` is a plain `std` thread polling `RaftStatus` every
100 ms and filing a `PromotionRequest`. This keeps promotion policy —
whose inputs (ack policy, primary link state, fence state) are all
data-plane concepts — synchronous and unit-testable without tokio.

The data plane never calls into `melin-raft`. The only raft →
data-plane edges are the `PromotionRequest` (unfileable without an
election quorum) and fencing (which keys off journaled epochs, not raft
terms). **Raft quorum loss therefore cannot halt trading**; failover
degrades to the manual `PROMOTE` playbook.

## Term = fencing epoch

An auto-promotion journals `EpochBump { epoch: term }` — the election
term becomes the tenure's fencing epoch, so two promotions from
different elections always mint distinct epochs and the newer fences
the older. This closes the documented "fencing cannot distinguish
concurrent promotions" gap.

**This requires openraft's `single-term-leader` cargo feature.** The
default openraft leader id is the lexicographic pair `(term, node_id)`,
which deliberately allows more than one leader to be elected within one
term. `single-term-leader` restores the standard-Raft rule — at most
one leader per term, terms strictly increasing per leader tenure —
which is what makes the term usable as an epoch. The raft storage
format version byte covers the `Vote` layout, so a binary built with
the feature flipped refuses an existing `--raft-dir` instead of
misreading it.

Epoch allocation on promotion is `max(fence_epoch + 1, floor)`
(`server.rs`), the floor coming from the request's `PromotionOrigin`: a
manual `PROMOTE` files `Manual`, whose floor of 1 folds to the classic
`epoch + 1`; an auto-promotion files `Election { term }`, whose floor is
its term. Manual and automatic promotions coexist — first request wins
the one-shot, and a manual promotion that outruns raft terms simply
makes the policy refuse (`term <= fence_epoch`) until terms catch up.

## Journal-tip vote recency filter (`recency.rs`)

Melin replicates order data out-of-band from raft, so raft's own
log-recency vote rule says nothing about journal recency. Every RPC
envelope carries the sender's journal tip `(fencing epoch, advertised
sequence)`; a voter **drops** vote requests from candidates behind its
own tip, before they reach `Raft::vote`. Dropping is indistinguishable
from packet loss to raft, so safety is untouched — the same shape as
MongoDB's election over its oplog, PacificA, and Viewstamped
Replication. Epoch dominates sequence in the tip order: a long suffix
on an old epoch is a divergent lineage, not recency.

Tip sources (`AdvertisedJournalTip` in transport-core):

- **replica** — the receive loop advances it to its in-memory
  *accepted* position (a promotion drains the ring into the journal, so
  accepted events survive; advertising only the fsynced position would
  understate the tip),
- **primary** — the journal stage publishes its durable cursor per
  fsync batch,
- the one legitimate regression is a snapshot resync discarding a
  known-divergent suffix (explicit `reset`).

**Liveness escape.** The tip order and raft's log order are independent
orderings applied conjunctively, and they can veto each other into
leaderlessness. After `LIVENESS_ESCAPE_DROPS` (8) consecutively dropped
vote requests with no leader observed, the filter opens until a leader
is next seen. The driver re-arms it whenever its metrics show *any*
leader — a node that is itself the leader receives no appends and would
otherwise never re-arm. Two openraft-specific consequences:

- openraft has no pre-vote, so a filtered candidate still inflates its
  own term and can briefly churn an established leader; the leader
  lease and the re-arm bound the disruption.
- steering is therefore **best-effort**: under sustained churn the
  escape can legitimately let a behind node win. The authoritative
  safety checks live at promotion time (`auto_promotion_decision`),
  never at the ballot box. Tests that assert "behind node never wins"
  must give the caught-up side a quorum that does not need the behind
  node's cooperation (see `src/tests/election.rs`).

### Peer tips are ordered by a sender stamp

The same envelope tips feed `PeerTips`, the per-peer record the
promotion-time checks read (the peer-tip veto, the challenger nudge and the
stand-down rule in `raft_promotion.rs`). A peer may hold several
connections to a node (openraft opens a fresh client per voter to confirm
leadership, beside the long-lived ones), so a frame sent before another can
arrive after it: a TCP retransmission after a partition, say. Recorded
blindly, it would regress the peer's tip and stamp it with the arrival
time, and a faster replica could look level with a slower one while the
slower one decides, letting the veto pass and losing acknowledged writes.
The reply path is bounded by the RPC timeout; the request path is not.

So every envelope, request and reply, carries a `SenderStamp`
(`RpcFrame::stamp`): the sender's boot id, its process start, and a
per-frame counter.

- **Sender.** `TipSource::stamp` is the one place an outgoing envelope's
  tip is read: it takes the counter and reads the tip (two independent
  loads, fence epoch and advertised sequence) under one `Mutex`, so counter
  order is tip-read order. `RaftClient::request` and the RPC server's reply
  path both call it, sharing one counter per process. A `Mutex` is fine
  here: the raft send path, a few frames per heartbeat per peer, never the
  event hot path, never held across an `.await`.
- **Boot identity.** `melin_transport_core::boot_stamp::BootStamp`, read
  once at startup by `spawn_raft_driver`: the 128 bits of
  `/proc/sys/kernel/random/boot_id` and `CLOCK_BOOTTIME` in nanoseconds.
  Nothing is persisted. Within one host boot `CLOCK_BOOTTIME` is monotonic
  and never stepped, so a later process start reads a later value; across
  boots the old kernel is gone, so nothing it sent can still be in flight.
  An unreadable or malformed `boot_id` (or clock) refuses startup of a
  raft-enabled node: there is no weaker fallback that keeps the ordering.
- **Receiver.** `PeerTips::record` keeps, per peer, the newest stamp
  accepted and drops any frame not newer (`SenderStamp::is_newer_than`:
  another boot id is always newer; within a boot, a later process start,
  then a higher counter; an equal stamp is the same frame). A dropped
  frame refreshes nothing: neither the tip nor the last-heard time, or a
  stale frame would keep a dead peer `Fresh`. It is logged at `debug`.
- **What the stamp gates: the peer-tip record only.** The frame itself is
  still served (or, as a reply, returned to openraft); its epoch still goes
  to `SupersessionPolicy::observe`; a vote request's own tip still decides
  the vote filter. Both are harmless on an older frame: the fence epoch
  only rises, so an older frame's epoch is never higher than a newer one's
  from the same sender (fencing on it is never wrong), and an older
  candidate tip can only make the monotone filter stricter. Gating them
  would only add paths where a real frame is ignored.

Rejected, and why: a persisted counter (a restored disk or a deleted file
rewinds it, every peer then drops every frame from the node, it looks
`Silent`, the veto skips `Silent` peers, and the bug returns silently); a
random per-process nonce (unordered: after a restart a stale frame from the
previous process regresses the tip); heuristics without a stamp (ignoring a
tip behind the stored one within an epoch, or ordering by raft term:
neither orders frames within a term or an epoch, where the stale frame
sits).

Residual: a frame of the old boot that reached the receiver's socket before
the sender's host went down and is still unread when a frame of the new
boot is read would be recorded, since another boot id always wins; the
receiver would have to sit on a readable socket for longer than a host
reboot. A VM snapshot restore rewinds the clock, memory and disk together:
a hazard shared with the journal, and in the operator docs.

Tested in `recency.rs` (an older frame dropped without refreshing the peer,
a newer one recorded, a new boot id replacing the stored one, a later
process start winning, and counter order being tip-read order under
concurrent stamping), in `boot_stamp.rs` (startup refused through a seam
over the path, malformed ids), and in `src/tests/rpc_loopback.rs` (a real
server reading a stale frame on one connection after a newer one on
another, and a client reading a stale reply). The step 1b partition
proxies in `raft_failover.rs` do not reproduce a reorder end to end: a
blocked relay stalls and is closed on heal rather than released, and
`PeerTips` is not observable from an integration test; the loopback test
drives the same receive path over real connections.

## Auto-promotion policy (`raft_promotion.rs`)

`auto_promotion_decision` is a pure function; the refusal strings are
operator-facing. An election win is the data-safety proof (a quorum of
voters held no more data than this node); the rules cover what an
election cannot prove — see the rustdoc on the function for each rule's
argument. The `disk`-policy refusal is the load-bearing one: acks under
`disk` never waited for a second copy on another node, so no election
can prove data completeness, and C3's ack-policy propagation exists
precisely so the replica judges the policy the *dead primary* acked
under rather than its own configuration.

Under `--raft-auto-promote` the raft mesh doubles as a fencing channel
(`SupersessionPolicy` in `rpc_server.rs`): a serving node — a primary,
or a replica whose promotion is already in flight — that reads a peer
envelope (a request it serves, or the reply to one it sent: one policy,
shared by the RPC server and the client factory) advertising a strictly
higher fencing epoch self-fences and
shuts down, exactly like the data-plane handshake path but without
waiting for a data-plane connection to cross.

Fencing on replies as well as requests adds no new way to misfire,
because a node advertises a higher epoch only when a newer tenure
exists. The advertised epoch is `TipSource::local_tip`, which reads
`FenceState::epoch`, and that is written only by `FenceState::new` and
`FenceState::observe_epoch` (a `fetch_max`, so it never falls). Every
caller feeds it either a journaled `EpochBump` or the primary's own
advertised epoch. `EpochBump`s reach it through recovery
(`adopt_recovered_lineage`, or a primary's boot from its journal), the
matching stage (`observe_live_lineage`) and a transferred snapshot. Their
only producer is `journal_promotion_epoch_bump`: no client input can
carry one, and an abandoned promotion publishes nothing. A replica
adopts its primary's advertised epoch in `accept_stream_start`, which by
induction is a tenure that exists, and it refuses a lower one first
(`FenceState::refuses_primary`). It also adopts it in
`handle_resync_verdict`, after rebasing onto that primary's snapshot,
without the refusal (its prior state was discarded); the value is still
the primary's own tenure, and `fetch_max` cannot lower the epoch. If a
change gives the epoch any other writer, this argument has to be made
again for that writer.

### The leader-lease gate

openraft 0.9 has no check-quorum step-down: a leader that can reach no
other voter keeps `ServerState::Leader` and its term for as long as it is
partitioned. A *replica* holding raft leadership and cut off from both
peers would otherwise pass every rule above: its primary link is down
past `PRIMARY_DOWN_GRACE`, both peers digest to `Silent` once it has led
past `PEER_TIP_GRACE` (and the peer-tip veto skips `Silent`), and its
term is above the fence epoch. Before the gate such a node promoted
itself and served clients beside the healthy primary, which the other
replica still reached; on heal, either one of its raft requests made the
healthy primary fence (a needless failover onto the node holding the
least data, the other replica orphaned) or both kept serving. The lone
survivor, whose two peers are dead, is the same case seen from inside:
it cannot tell "both others dead" from "I am partitioned".

Two checks close it, one at each end of the promotion:

- a **decision-time filter** in `auto_promotion_decision`
  (`leader_lease_check`): refuse unless a majority acknowledged this
  leader within `QUORUM_ACK_MAX_AGE`, read from openraft's
  `RaftMetrics::millis_since_quorum_ack` as bridged into `RaftStatus`;
- a **mint-time confirmation** in `journal_promotion_epoch_bump`
  (`server.rs`): just before an election's `EpochBump` is published, a
  real quorum round trip (`LeaderConfirm::confirm`, openraft's
  `Raft::get_read_log_id` on the driver thread) must confirm that the node
  still leads in the term it was elected at. This one carries the safety
  claim; the filter makes none.

A promotion request carries its origin in its type
(`PromotionOrigin::Manual` or `PromotionOrigin::Election { term }`), so
the mint knows which term to confirm, and a manual `PROMOTE` is never
confused with an election at term 1.

#### The lease

What openraft 0.9.25 guarantees, and where:

- a follower whose vote is committed rejects every vote request while
  `now <= vote_utime + leader_lease` (`Engine::handle_vote_req`), and
  `leader_lease` is `election_timeout_max` (`EngineConfig::new`);
- a follower touches `vote_utime` on every accepted vote-bearing RPC,
  heartbeats included, at the time it processes it
  (`VoteHandler::update_vote`);
- the leader's clock for a follower is the **sending** time of the request
  it acknowledged (`ReplicationHandler::update_success_progress` →
  `update_leader_clock`), and the quorum-acknowledged time counts the
  leader itself (`Leader::last_quorum_acked_time`), so in a three-voter
  cluster it is the newest follower acknowledgement.

Let S be the sending time of the newest request a majority acknowledged,
and F a follower in that majority. F handled the request at some R ≥ S
and touched its `vote_utime` then, so until R + `leader_lease` ≥ S +
`leader_lease` it refuses every *other* node's vote request, nudged
candidates included. Every election needs a majority, which shares a
member with the acknowledging one, so before S + `leader_lease` a third
node can be elected only with the leader L's own vote, that is only by
deposing L. (L's own `vote_utime` is set when it becomes leader, in
`Engine::establish_leader`, and is not refreshed by acknowledgements, so
L may grant that vote; see below.)

F itself can still be elected. Its timer-driven campaign waits
`leader_lease + election_timeout` from `vote_utime`, but the challenger
nudge (`challenger_should_campaign` → `raft.trigger().elect()`) skips
that timer: F votes for itself and the third node may grant its vote. The
lease does not exclude this, and two older mechanisms cover it:

- **The peer-tip veto.** The nudge fires only while F's tip is ahead of
  the leader's tip as F last heard it within `PEER_TIP_GRACE`. F's
  acknowledgement is what keeps L's lease fresh, and the same reply
  carries F's tip, so L sees F as `PeerTipObservation::Fresh` and ahead of
  its own tip, which does not move (L's primary link is down). The veto in
  `auto_promotion_decision` refuses. The lease filter runs before the veto
  but does not replace it.
- **Epoch fencing.** If F wins anyway (L's veto passed before F's tip
  arrived, and F won after L's confirmation), L's epoch is its term T and
  F's promotion mints T + 1; `fence_if_superseded` fences L as soon as the
  higher epoch reaches it, through a raft request or reply or a
  data-plane handshake. L never deposes F.

So the claim is narrower than "no other leader exists": no other leader
can exist that L's peer-tip view and the epoch rule do not already
handle.

#### The mint-time confirmation carries the safety claim

Let C be the moment the driver starts `get_read_log_id`. openraft captures
the vote and the read log id before sending, so `Ok` means a majority
accepted heartbeats carrying this leader's vote, sent at or after C. By
the lease argument, until C + `leader_lease` no node but an acknowledging
follower can be elected without deposing L. `confirmation_outcome`
compares `read_log_id.leader_id.term` with the term the request was filed
for (the read log id is the newer of the leader's own blank entry and the
committed one, so it carries the confirming leader's term,
`LeaderHandler::get_read_log_id`), so a node that lost term t and won t′
in between gets `TermMoved` and refuses.

`LeaderConfirm::confirm` takes its deadline (`LEADER_CONFIRM_TIMEOUT`) as
its first statement, on the server thread, **before** sending the request,
so before C. It accepts a reply only on a clock reading taken after the
reply is in hand and still before the deadline (`accept_answer`):
`recv_timeout` hands back a queued message before it reads the clock and
times its wait from when it is called, so on its own it could accept a
reply that arrived late to a thread preempted before the wait. The driver
applies no timeout of its own; openraft bounds each voter by one
`heartbeat_interval`, and the caller's deadline bounds the whole.
`journal_promotion_epoch_bump` logs "promotion: confirming leadership
before the epoch bump" *before* calling `confirm`, and publishes straight
after it returns, with nothing that can block (a log write can, on a full
stderr pipe) in between. So the epoch is minted before C +
`LEADER_CONFIRM_TIMEOUT` + `SCHEDULING_SLACK_MS`, which the mint-time
safety assertion keeps under C + `leader_lease`. No metric enters this
argument, and a stalled raft runtime turns into a `TimedOut` refusal,
never a late mint.

What it leaves open: L itself may grant a higher-term vote and step down.
The confirmation then fails (openraft answers a non-leader with
`ForwardToLeader`, mapped to `NotLeader`). If it steps down after the
confirmation returned `Ok` and before the mint, the epoch rule fences L
when the newer leader promotes and mints a higher epoch; if the newer
leader is the primary, which never mints, nothing fences L's epoch and
L's mint deposes it. Reaching that between C and the mint needs a peer
that has not heard L for a lease while another still acknowledges L and
L's primary link is down: the one-sided partition recorded in
`raft-control-plane-follow-ups.md`, not a case the gate closes.

`LeaderConfirmError` is exhaustive: `NotLeader`, `TermMoved { now }`,
`NoQuorum` (openraft's `QuorumNotEnough`), `TimedOut`, and
`DriverStopped` (the request could not be sent, the driver dropped it,
or openraft returned `Fatal`). The caller treats them alike; they exist
for the log line and so tests can pin which path ran. Peers that are
stopped refuse the connection at once, so a leader with no live peer gets
exactly `NoQuorum` well inside the deadline; through a silent link it
takes one `heartbeat_interval`, still inside it.

#### The decision-time filter, and what it buys

`leader_lease_check` passes when this node leads and its last reported
quorum-ack age is at most `QUORUM_ACK_MAX_AGE`; `None` (never
acknowledged in this term) refuses. The reported age only ever
under-reports the truth (openraft computes it when it flushes metrics,
at least once per tick; the bridge stores it at least every
`SHUTDOWN_POLL`; it is floored to the millisecond), and the time from the
decision to the mint is unbounded (drain, `check_promotable`, prefault,
stage start-up), so a pass proves nothing about the mint. The filter
sits after the primary-link rules and before the genesis rule and the
peer-tip veto, so a healthy cluster still logs "the link is up" or the
grace, and an isolated leader is told it may be partitioned instead of
being shown a peer table of `Silent` peers. What it buys:

- an isolated leader's age only grows, so it refuses for good, with a
  reason that names the partition, instead of draining, prefaulting,
  failing the confirmation and exiting;
- a lone survivor refuses the same way without disturbing its replica
  pipeline. This is a deliberate behaviour change: a replica that cannot
  reach a majority never auto-promotes, and recovery is a manual
  `PROMOTE`;
- the operator sees the evidence: the refusal line carries the age, and
  `melin_raft_quorum_ack_age_ms` is the one gauge that shows a raft leader
  has lost its majority (openraft keeps the role).

Its one constraint is liveness: it must not refuse a healthy leader (the
liveness assertion below). A filter that wrongly passes costs one
abandoned promotion, never safety, so nothing argues a margin over metric
staleness. The age, the term and the role are separate `Relaxed` loads,
each read once (`consider_auto_promotion`): a torn read can at worst let
the filter pass for one poll or file a term the node no longer leads in,
and the confirmation checks the filed term exactly. openraft keeps the
quorum clock in its `Leader` value, so a new leadership reports `None`
until its first acknowledgement and never inherits an age.

The cost on a genuine failover: the new leader promotes only once a
majority has acknowledged it, one control-plane round trip after its
election plus at most one promotion poll (openraft replicates the new
term's blank entry at once and flushes metrics on every loop iteration).
That is inside the time the peer-tip veto already waits for `Unknown`
peers. At mint time it adds one confirmation round trip, on fresh
connections: `get_read_log_id` opens a new client per voter
(`RaftNetworkFactory::new_client`), so a connect and an Ed25519 handshake
each, which is why the deadline is not one round trip.

#### An abandoned promotion

On a refusal `journal_promotion_epoch_bump` publishes nothing, logs at
`warn!` (degraded operation, not a malfunction) with the error, the term
and the epoch, and returns the error. Both primary paths (`run_as_primary`,
`run_as_primary_dpdk`) turn it into "automatic promotion abandoned: …"
(`promotion_abandoned`, one wording for both transports) and return it,
so the node exits with an error; `PipelineHandles`'s drop stops and joins
the stages already spawned. The bump is the tenure's first entry, so the
journal holds nothing of the tenure, and once its supervisor restarts the
node it rejoins as a replica of its configured primary, as after a
`check_promotable` refusal. Undoing the handover in place, waiting for
the lease to return, and committing the promotion as a raft log entry
were all rejected (the first as complicated and risky, the second as
little gain for more code, the third as a control-plane redesign).

A manual `PROMOTE` is not confirmed, so manual failover stays available
during a control-plane outage. Both transports get the confirmation from
one helper, `leader_confirmation`, whose no-driver arm returns
`DriverStopped`: only the raft poller files elections, but the origin
crosses `PromotionRequest`'s atomic from another thread and cannot carry
the handle, so the type cannot rule that pairing out.

Residual liveness risk, with no mechanism against it: the filter reads
acknowledgements on raft's long-lived connections and the confirmation
opens fresh ones, so anything that breaks new connections but not
established ones (a firewall admitting only established flows,
ephemeral-port exhaustion, the raft bind IP removed while old sockets
live on) makes the node promote, abandon, exit, restart and possibly win
again. Acked data is never at risk; a cause local to one node resolves
itself when another node wins, a cluster-wide one blocks automatic
failover until fixed. `docs/replication.md` tells operators what that
looks like.

openraft logs "timeout while confirming leadership for read request" at
`error` for each voter whose confirming heartbeat fails, so every genuine
failover whose dead primary is a voter logs one. It is left as it is (the
subscriber is the application's, so the runtime has no filter to cap)
and documented for operators.

#### Constants and compile-time assertions

All in `crates/core/raft/src/driver.rs`, next to the values they derive
from, so every assertion sits in the crate that owns its inputs. The
derived ones are written as the expression, not a literal, so a change to
an input carries through; the assertions are the backstop for all of
them.

| Constant | Value | Derivation |
|---|---|---|
| `LEADER_LEASE_MS` | 2000 | `= ELECTION_TIMEOUT_MAX_MS` (openraft's `EngineConfig::new`) |
| `QUORUM_ACK_MAX_AGE_MS` | 1000 | `= LEADER_LEASE_MS / 2`: half the lease, a decided ratio (not forced by any input) written as the expression so it follows the lease; exported as `QUORUM_ACK_MAX_AGE` for the filter in `melin-server-runtime` |
| `MAX_TICK_PERIOD_MS` | derived | `= (HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS - 1) * 3 / 2`, the slowest node's tick (`heartbeat_interval_ms`'s maximum, times openraft's tick factor) |
| `RTT_BUDGET_MS` | 100 | one heartbeat round trip, for the liveness assertion only |
| `LEADER_CONFIRM_TIMEOUT_MS` | 500 | `confirm`'s deadline (`LEADER_CONFIRM_TIMEOUT`, crate-private). openraft gives each voter one `heartbeat_interval` (at most `heartbeat_interval_ms`'s maximum) including a fresh connect and handshake; later than this is a control plane in trouble |
| `SCHEDULING_SLACK_MS` | 200 | reply-to-publish on the server thread (preemption, the publish) plus clock-rate drift between the leader's clock and the followers' lease clocks |

- **Liveness**: `QUORUM_ACK_MAX_AGE_MS >= MAX_TICK_PERIOD_MS + RTT_BUDGET_MS`.
  A healthy leader's reported age peaks at about one tick plus a round
  trip, at the flush just before a heartbeat's reply lands.
- **Mint-time safety**: `LEADER_CONFIRM_TIMEOUT_MS + SCHEDULING_SLACK_MS < LEADER_LEASE_MS`.

Deliberately absent: any assertion over decision-time staleness (the
filter makes no safety claim). The threshold trade-off: one lost
heartbeat brings the reported age to about two ticks plus a round trip,
which on the slowest-ticking node passes 1000 ms and refuses
*transiently*; the 100 ms poll retries, so the cost is a short delay and
one log line, never a stuck or unsafe failover.

#### The openraft version is part of the argument

The lease semantics (`Engine::handle_vote_req`, `EngineConfig::new`) and
`get_read_log_id`'s behaviour (`RaftCore::handle_check_is_leader_request`:
one heartbeat per voter under a `heartbeat_interval` timeout, the vote and
read log id captured before sending) were read in openraft 0.9.25, and
everything above must be **re-derived on any upgrade** (0.10 changes
elections; see the roadmap's migration item). The comment beside the
`openraft = "0.9"` requirement in `crates/core/raft/Cargo.toml` says so.
The requirement is not an exact pin, which would make resolution brittle
for a consumer that also depends on openraft 0.9; `Cargo.lock` holds
0.9.25 for our builds, and
`a_leader_that_loses_both_peers_keeps_leading_and_its_ack_ages`
(`src/tests/election.rs`) fails if a release adds check-quorum or stops
ageing the acknowledgement. A downstream build resolving a later 0.9.x
patch is covered by none of these; openraft keeping lease semantics
stable within 0.9 is assumed.

#### Where it is tested, and what is not

- Unit: `leader_lease_check` and its placement in
  `auto_promotion_decision` (`raft_promotion.rs`); `accept_answer`,
  `confirmation_outcome` for every openraft answer, `publish_metrics` and
  `heartbeat_interval_ms` (`driver.rs`); `journal_promotion_epoch_bump` per
  origin and outcome, `leader_confirmation`'s no-handle arm and
  `promotion_abandoned`'s wording (`server.rs`, `promotion_epoch_tests`).
- `src/tests/election.rs`: a healthy leader's sampled age stays under the
  threshold (`a_healthy_leaders_quorum_ack_age_stays_under_the_gate`, the
  liveness assertion checked empirically), the no-check-quorum premise
  above, and the confirmation's pass, `NotLeader`, `TermMoved`, `NoQuorum`
  and `DriverStopped` paths.
- `crates/core/server-runtime/tests/raft_failover.rs`: an isolated replica
  leader never deposes a healthy primary, a lone survivor never
  auto-promotes, a leader cut from the primary alone does not promote
  while writes flow, and the genuine failovers
  (`replica_leader_promotes_when_the_primary_dies`,
  `killed_primary_triggers_exactly_one_auto_promotion`) run the real
  confirmation end to end, so a spurious refusal under load shows there
  first.
- Not tested: the decision-to-mint race end to end, and so an abandoned
  promotion and its restart (the quorum loss would have to be timed
  inside the drain/prefault window; the refusal is covered by the
  confirmation tests and the stub test, the rejoin is the ordinary replica
  restart); `confirm` taking its deadline before the send (a non-blocking
  enqueue, so no test can tell the orders apart: held by the statement
  order and its comment); the real-clock margin of the mint-time
  assertion; openraft's lease internals, since `Raft` handles stay inside
  the driver thread; the nudged follower winning while L was just
  acknowledged (covered by the peer-tip veto's and fencing's own tests);
  the one-sided partition with an idle primary, a known gap.

## RPC transport

Length-prefixed postcard frames (`wire.rs`) carrying the tip envelope
plus openraft's serde-enabled RPC types. Postcard over bincode: compact
varints and no encoder/decoder configuration knobs to diverge between
peers. Peer links authenticate **mutually** with the replication Ed25519
challenge-response (`auth.rs`; `Permission::Replication` on both keys;
codecs shared with the data plane so the two paths cannot diverge, and
one verification, `verify_challenge_response_identified`, for both
directions). The dialer proves first (`answer_challenge` against the
server's `challenge_peer`), and the server pins the authenticated key to
its configured node id — a request claiming another id closes the
connection. After the version exchange below, the dialer challenges the
responder (`challenge_responder`) and accepts only the key the
`--raft-peer` table maps to the node id it dialled: the same
key → id table (`peer_ids`, one `Arc` shared by `RpcServerConfig` and
`RaftClientFactory`) the server pins with. The stored membership's
`Node` is openraft's `BasicNode`, an address only, so the key always
comes from the live CLI list, and a stored node no `--raft-peer` entry
names is not dialled at all (`KeyRefusal::NoKeyConfigured`, before any
syscall; logged at `debug` per attempt, since it repeats at openraft's
backoff cadence and `driver_main` already warns once at startup that the
stored membership differs from the `--raft-peer` list). A refusal
(unknown key, another role, another peer's key, a bad signature) sends
`AuthFailed` and closes the link before any request is sent, so no reply
is read: nothing said by whatever answers at a peer's address can fence
this node or enter `PeerTips`. The dialer logs it
at `warn`, like the server a dialer's failed authentication
(misconfiguration or impersonation, not a malfunction of this node), and
`to_rpc_error` maps the typed `KeyRefusal` to `Unreachable` so openraft
backs off. The responder logs the dialer's refusal of its key at `debug`
only: the dialer, which knows why, has warned. The server logs a link
that fails *under* its own challenge (an I/O error, not a refusal) at
`debug` too. The second challenge costs one more round trip per new
connection; raft links are long-lived, and a leadership confirmation's
fresh connections are well inside its `heartbeat_interval` on a LAN.

**The link carries a protocol version.** Right after the dialer's
authentication, and after the server has pinned its key (and before the
dialer challenges the responder, so a peer on another release is refused
by version, both named), both ends send a version frame
carrying `RAFT_PROTOCOL_VERSION` (`wire.rs`) and read the other's
(`exchange_version`; both write before reading, the frames being a few
bytes). The frame is hand-encoded and fixed for good,
`[len: u32 LE = 3][tag 0x52][version: u16 LE]`, so any release can read
any other's version whatever else changes; bump the version whenever
what crosses the link changes (the envelope, the openraft types in it,
the handshake), once a release has shipped the current one. Version 1
came in unreleased with mutual authentication and the sender stamp above,
so it includes both. A mismatch is a typed `VersionRefusal` naming both
versions; a first frame of another length or tag is refused the same
way, which is what a release from before the version sends (an
`RpcFrame`). Both ends log it at `warn` (a peer on another release, not
a malfunction, but automatic failover is degraded while it lasts), the
server closes, and the dialer's `to_rpc_error` maps it, like a
`DialRefusal`, to `Unreachable` so openraft backs off. Releases from
before the version refuse the frame too, as an undecodable `RpcFrame`:
the shortest postcard envelope is four bytes, so a three-byte payload
runs out mid-frame. `src/tests/pre_version_wire.rs` pins this against a
frozen copy of their decoder, for every version the frame can carry; do
not update that copy when the envelope changes. Such an old dialer sees
its first reply fail to decode, an old responder hangs up, and this
release's dialer sees only the hang-up (a network error, retried as
one).

`--raft-peer` lists are identical on every node and include the node
itself (the self entry supplies the dialable address written into the
first-boot membership).

**Outbound connections are made from the listener's IP.** When
`RaftConfig::bind` is a specific IP (not `0.0.0.0` or `::`),
`RaftClient::ensure_connected` binds the outgoing socket to that IP, port
0 (a link-local IPv6 bind keeps its scope id), before connecting; with a
wildcard bind it dials as the kernel routes, and a peer address may be a
hostname. One pure helper, `plan_dial`, states the rule for both places
that apply it: `spawn` refuses at startup a configured peer that a
specific bind could never dial (not an IP literal, or the other address
family), naming the bind and the peer and skipping the node's own entry;
and every connect re-checks the stored membership, which is what is
dialled after first boot. A refusal there is a typed `DialRefusal` inside
the `io::Error`, and `to_rpc_error` maps it, like `AddrNotAvailable` (the
bind IP is no longer assigned), to `RPCError::Unreachable`, so openraft
backs off as for a dead peer instead of retrying every heartbeat (it backs
off only on `Unreachable`). On a multi-homed host this makes control-plane
traffic leave from the interface the listener is bound to, which is what
peers' firewall rules see. It is also what makes per-pair partition tests
possible: without it every loopback connection comes from 127.0.0.1, so a
test proxy cannot tell which node a raft connection comes from. In
`raft_failover.rs`, `Cluster::partitionable` binds node *i*'s raft
listener on `raft_ip(i)` = 127.0.0.(*i*+2) (Linux routes all of
127.0.0.0/8 to `lo`; no node takes 127.0.0.1, so a regression of the
source binding makes no block match rather than one match by accident),
puts a proxy on 127.0.0.1 in front of every raft listener, and stalls a
blocked source per connection, silently, like a partition; `partition`,
`isolate`, `isolate_from_primary`, `mute_replicas` and `heal` are built
on that.

## Storage (`storage.rs`)

File-backed `RaftLogStorage`/`RaftStateMachine` under `--raft-dir`:

| file | write discipline | damage policy |
|---|---|---|
| `vote` | atomic tmp + fsync + rename | refuse to open (a forgotten vote can double-vote) |
| `log` | append + fsync before the `LogFlushed` callback | torn tail truncated (never acked); header/purge-marker damage refuses (only ever written atomically — recovery there would be a log reversion) |
| `sm`, `snapshot` | atomic replace | refuse to open |

Every file carries a magic + format-version byte. Log truncate/purge
rewrite the file wholesale — the election-only log is dozens of
entries. Blocking fsync inline on the driver runtime is deliberate:
appends happen only at leader establishment and membership changes,
never per-heartbeat, and a ~ms fsync is harmless against the 200 ms
heartbeat / 1 s election floor. The primary storage test is openraft's
own `Suite::test_all` compliance suite plus a torn-tail byte-sweep.

Losing `--raft-dir` while peers are live can double-grant a vote in a
term (standard Raft hazard) and with it the term-uniqueness the epoch
design rests on — hence the operator rule in `docs/replication.md`.

## Election tuning

200 ms heartbeats, 1–2 s randomized election timeout — deliberately
slow. Control-plane latency only affects failover reaction time (an
election plus a 100 ms poll), never order flow; stability wins.

## What the in-process tests cannot cover

True power-loss fsync semantics of the raft files, network faults
finer than a silent per-pair or one-way partition (the proxies above
stall or cut whole connections; there is no selective packet loss,
reordering or delay), inter-node clock skew, and whole-process
`kill -9` mid-append (covered indirectly by the storage crash tests).
The venue for those is a manual multi-VM failover drill with the AWS
bench scripts.
