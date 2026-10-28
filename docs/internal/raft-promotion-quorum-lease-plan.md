# Plan: a leader-lease gate on raft auto-promotion

Branch `fix/raft-promotion-quorum-lease`, planned against `85075207`.
Contributor document: an implementation plan, not operator documentation.
Function names are the anchors. Line numbers are left out because they
go stale.

## 1. The bug

openraft 0.9.25 has no check-quorum step-down. A leader that can reach no
other voter keeps `ServerState::Leader` and its term for as long as it is
partitioned. A **replica** that holds raft leadership and gets isolated
from both the primary and the other replica therefore passes every rule in
`raft_promotion::auto_promotion_decision`:

- `primary_link_up` is false, and after `PRIMARY_DOWN_GRACE` (3 s)
  `primary_link_down_for` clears the grace;
- once `leader_for > PEER_TIP_GRACE` both peers digest to
  `PeerTipObservation::Silent`, and the peer-tip veto skips `Silent`
  (the `Fresh(_) | Silent { .. } => {}` arm);
- its term is still above the fence epoch.

`consider_auto_promotion` then calls `control.promote.request(term)`.

Inferred consequences, not yet proven by a test (step 1 proves or
disproves them):

- the isolated replica serves as a primary that no other node reaches;
- on heal, its epoch (= its term) is higher than the healthy primary's.
  Its raft appends trip `fence_if_superseded` on the primary, so the
  healthy primary shuts itself down. The result is a needless failover,
  the other replica orphaned (its `--replica-of` still names the dead
  primary), and the isolated node's journal diverged from the primary's
  (it journaled an `EpochBump` while the primary kept writing). When it
  is pointed back at the old lineage it is re-seeded and its journal
  archived.

## 2. The fix in one paragraph

Bridge openraft's `RaftMetrics::millis_since_quorum_ack` from the raft
driver into `RaftStatus`. Make it an input of the pure
`auto_promotion_decision`. Refuse unless a majority acknowledged this
leader within `QUORUM_ACK_MAX_AGE`. That threshold is derived from
openraft's follower lease and fixed by compile-time assertions over the
real constants. Re-run the same check where the epoch is actually minted
(`journal_promotion_epoch_bump`), because the time from request to mint is
unbounded. Before any of that, make a promotion request carry its origin
in its type: an election term of 1 is currently indistinguishable from a
manual `PROMOTE`.

## 3. Facts checked in the code (and where)

openraft paths are relative to
`/usr/local/cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.9.25/src`.

| Claim | Where it holds |
|---|---|
| A follower rejects any vote request while `now <= vote_utime + leader_lease`, if its vote is committed | `engine/engine_impl.rs`, `Engine::handle_vote_req` (the `vote.is_committed()` block) |
| `leader_lease = election_timeout_max` | `engine/engine_config.rs`, `EngineConfig::new` (`leader_lease: Duration::from_millis(config.election_timeout_max)`) |
| A follower's `vote_utime` is touched on every accepted vote-bearing RPC (appends and heartbeats included), at **processing** time on the follower | `engine/handler/vote_handler/mod.rs`, `VoteHandler::update_vote` (`self.state.vote.touch(C::now())`) |
| The leader's per-follower clock is the **sending time** of the acked request | `engine/handler/replication_handler/mod.rs`, `update_success_progress` → `update_leader_clock(target, result.sending_time)` |
| The quorum-acked time includes the leader's own clock (`now`), so in a 3-voter cluster it is the newest follower ack | `proposer/leader.rs`, `Leader::last_quorum_acked_time` (`clock_progress.increase_to(&node_id, Some(now))`) |
| `millis_since_quorum_ack` is `None` when not leader or never quorum-acked, computed at report time, floored to ms | `core/raft_core.rs`, `report_metrics` (`t.elapsed().as_millis() as u64`); docs in `metrics/raft_metrics.rs` |
| Metrics are flushed once per runtime-loop iteration, at the top of the loop before it blocks | `core/raft_core.rs`, `runtime_loop` (`self.flush_metrics()` first in `loop`) |
| The loop is woken at least once per tick; tick interval = `heartbeat_interval * 3 / 2`, re-armed `interval` after each send | `raft/mod.rs` (`Tick::spawn(Duration::from_millis(config.heartbeat_interval * 3 / 2), …)`), `core/tick.rs` `tick_loop` |
| A leader sends a heartbeat on every tick (next heartbeat = now + `heartbeat_interval` < next tick) | `core/raft_core.rs`, `Notify::Tick` arm |
| The election nudge (`trigger().elect()`) bypasses the candidate's *own* lease-extended timer (`handle_tick_election` is not consulted), so a follower whose ack keeps the leader's lease fresh can still campaign and vote for itself. Other voters still apply the lease in `handle_vote_req`. The nudge fires only while the nudging node's tip is ahead of the leader's tip as last heard within `PEER_TIP_GRACE`, with the primary link down past `PRIMARY_DOWN_GRACE` | `driver_main` nudge branch; `raft_promotion.rs`, `challenger_should_campaign`, `consider_challenger_nudge` |
| Melin's heartbeat interval is 200 ms + per-node jitter `(id % 150) * 37 % 150` ∈ [0, 149]. Election timeout 1000–2000 ms | `crates/core/raft/src/driver.rs`: `HEARTBEAT_INTERVAL_MS`, `HEARTBEAT_JITTER_MS`, `HEARTBEAT_JITTER_SPREAD`, `ELECTION_TIMEOUT_*_MS`, `driver_main` |
| The driver bridge waits on `metrics_rx.changed()` capped at `SHUTDOWN_POLL` (100 ms), then stores term/leader/role with `Relaxed` | `driver_main` bridge loop |
| The promotion poll is `POLL_INTERVAL` (100 ms); decision and `promote.request(term)` are back to back | `raft_promotion.rs`: `consider_auto_promotion`, `spawn_auto_promotion` |
| The request is consumed by the receive loop, then drain, `take_pipeline_for_promotion`, `check_promotable`, `prefault` (can take seconds), `run_as_primary[_dpdk]` (spawns stages), and finally `journal_promotion_epoch_bump` publishes the `EpochBump`. Nothing bounds this path in time | `replication/receiver_transport.rs`, `replication/tcp_receiver.rs`, `server.rs` (replica branch, `run_as_primary`, `run_as_primary_dpdk`, `journal_promotion_epoch_bump`) |
| An early `Err` return from `run_as_primary` after stages spawned is safe: `PipelineHandles`'s drop stops and joins them | `server.rs`, doc on `PipelineHandles` (`spawn_health_endpoint(..)?` already relies on it) |
| The raft driver keeps running through `run_as_primary` (the guard drops after it returns), so `RaftStatus` is live at mint time | `server.rs` replica branch comment ("The raft guard drops … after this returns") |
| `PromotionRequest::MANUAL == 1`, and the first election after `initialize` is term 1. So an auto-promotion at term 1 files the same value as a manual `PROMOTE` | `promotion.rs`; `journal_promotion_epoch_bump`'s `requested_epoch > MANUAL` test already misclassifies term 1 |
| `tip_ready` is set after **local** journal recovery, before the replica connects, so two replicas can elect a replica leader with the primary not yet up | `tcp_receiver.rs` (`tip_ready.store(true, …)` after `recover_replica_state`); `replication/dpdk.rs` likewise |
| A blank replica that never observed a primary refuses to promote, so a replicas-first test bring-up cannot trigger a promotion | `auto_promotion_decision` genesis rule |
| Raft dial addresses come from the stored membership, which is the same on every node. On loopback every source address is 127.0.0.1. So a test proxy cannot tell which node a raft connection comes from | `crates/core/raft/src/network.rs` (`addr: node.addr`); checked empirically |
| `RaftStatus` is a `pub` struct with `pub` fields in published `melin-transport-core`. Adding a field breaks external struct literals | `crates/core/transport-core/src/health.rs` |
| Raft health gauges are Prometheus-only (the one-line status omits them). Optional gauges are emitted only when `Some` (the `melin_ack_policy_journaled` pattern) | `HealthSnapshot::collect`, `write_metrics` in `health.rs` |
| openraft is required as `"0.9"` (not pinned). `Cargo.lock` has 0.9.25 | `crates/core/raft/Cargo.toml`, `Cargo.lock` |

## 4. Why the lease makes the gate sound

Let S be the leader-clock **sending** time of the newest request that a
majority acknowledged. In a 3-voter cluster that is the newest follower
ack, because the leader counts itself. That follower F handled the
request at real time R ≥ S and touched its `vote_utime` at R. Until
R + `leader_lease` ≥ S + `leader_lease`, F refuses every *other* node's
vote request, nudged candidates included. Any election needs 2 of 3
votes, so a third node cannot be elected before S + `leader_lease`.

F itself can be. Its timer-driven campaign waits
`leader_lease + election_timeout` from `vote_utime`, but Melin's
challenger nudge (`challenger_should_campaign` → `raft.trigger().elect()`)
skips that timer. F votes for itself, and the third node may grant its
vote (its own lease has lapsed, or its vote is not committed). F can then
win term T+1 while L's gate still reads fresh. The lease alone does not
exclude this case. Two existing mechanisms cover it:

- **The peer-tip veto.** The nudge fires only while F's tip is ahead of
  the leader's tip as F heard it within `PEER_TIP_GRACE`. F's ack is what
  keeps L's lease fresh, and the same exchange carries F's tip, so L sees
  F as `PeerTipObservation::Fresh(tip)` with `tip.is_ahead_of(local_tip)`.
  L is a replica whose primary link is down, so its own tip does not
  move. The veto (the first arm of the peer loop in
  `auto_promotion_decision`) refuses. The step 4 gate runs before this
  veto but does not replace it.
- **Epoch fencing.** If L mints anyway (it passed the decision before F's
  tip arrived, and the request-to-mint window let F win meanwhile), its
  epoch is T. F's promotion mints epoch T+1, and `fence_if_superseded`
  fences L as soon as the higher epoch reaches it. The two-primaries
  window is bounded by that propagation, and L never deposes F.

So the gate's claim is narrower than "no other leader exists": no leader
can exist that L's own peer-tip view and epoch rule do not already
handle.

The leader itself may still grant a higher-term vote and step down. That
moves `role` off leader, which the re-check sees within the staleness
bound below, and in any case the epoch rule makes the newer election
fence the older.

The gate therefore states, at the moment the epoch is minted: this node
leads in this term, and a majority acknowledged it less than
`QUORUM_ACK_MAX_AGE` + staleness ago. That is less than one lease, so it
is not partitioned from the majority. An isolated leader has no new acks,
its age only grows, and it refuses for good. It also refuses when both
other nodes are dead. That is a deliberate behaviour change (§8).

**Staleness only ever under-reports age.** The value read at decision time
D is the age at the last flush F_w, which the driver published at its last
store W:

- D − W ≤ `METRICS_BRIDGE_POLL` (the bridge stores at least every 100 ms);
- W − F_w ≤ `MAX_TICK_PERIOD` (the loop flushes at least once per tick);
- `as_millis` floors (< 1 ms).

So true age at D ≤ reported + `MAX_TICK_PERIOD` + `METRICS_BRIDGE_POLL` +
1 ms. `POLL_INTERVAL` does **not** add: the decision acts on what it read
at D and files the request in the same call. The interval from mint-time
re-check to publish is a straight-line non-blocking call (§6, step 5).
The interval from request to mint is unbounded, which is why the re-check
exists and the decision-time check is only an early filter.

## 5. Constants

All in `melin-raft`, next to the values they derive from. Every input is
a melin-raft constant, so the assertion sits in the same crate as what it
asserts over and no constant is copied across crates.

| Constant | Value | Derivation |
|---|---|---|
| `LEADER_LEASE_MS` | 2000 | `= ELECTION_TIMEOUT_MAX_MS` (openraft `EngineConfig::new`) |
| `MAX_TICK_PERIOD_MS` | 523 | `= (HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS - 1) * 3 / 2` = 349 × 3 / 2 |
| `METRICS_BRIDGE_POLL_MS` | 100 | the bridge cap. `SHUTDOWN_POLL` becomes `Duration::from_millis(METRICS_BRIDGE_POLL_MS)` |
| `SCHEDULING_SLACK_MS` | 200 | covers re-check-to-publish, current-thread runtime scheduling (raft core, RPC server and bridge share one thread), the `as_millis` floor, and clock-rate drift between nodes |
| `QUORUM_ACK_MAX_AGE_MS` | 1000 | `= LEADER_LEASE_MS / 2`, the gate threshold (`pub const QUORUM_ACK_MAX_AGE: Duration`). Defined as the expression, not a literal, so a lease change carries through; the assertions below are the backstop |

Compile-time assertions (`const _: () = assert!(…)`, each with a message):

1. Safety: `QUORUM_ACK_MAX_AGE_MS + MAX_TICK_PERIOD_MS + METRICS_BRIDGE_POLL_MS + SCHEDULING_SLACK_MS < LEADER_LEASE_MS`
   (1000 + 523 + 100 + 200 = 1823 < 2000).
2. Liveness: `QUORUM_ACK_MAX_AGE_MS >= MAX_TICK_PERIOD_MS + METRICS_BRIDGE_POLL_MS + RTT_BUDGET_MS`
   with `RTT_BUDGET_MS = 100`. A healthy leader's reported age peaks at
   about one tick, at the tick flush that comes just before the heartbeat
   reply. The gate must not refuse a healthy leader.
3. `HEARTBEAT_JITTER_SPREAD` is coprime with `HEARTBEAT_JITTER_MS` (the
   existing bijection claim, now asserted rather than just commented).

**Trade-off to confirm (open question Q1).** With 1000 ms, two
consecutive lost heartbeats (age ≈ 1046 ms) refuse *transiently*. The
100 ms poll retries, so the cost is a short delay and one log line, never
a stuck failover. The largest value the strict safety assertion allows is
1176 ms. It tolerates two missed ticks, but uses up the whole margin the
assertion leaves. 1000 (= lease / 2) is recommended.

The healthy-case figure (≈ one tick) is checked empirically in step 3;
see the election-test case there.

## 6. Steps

Each step builds, passes clippy/doc/tests (the pre-commit hook), and is
committed on its own, but only when the user asks. Step 1b is the one
uncommitted step, because the hook runs the suite and a red test cannot be
committed (`#[ignore]` is banned).

### Step 1: `test(server-runtime): drive raft failover through partitionable links`

The harness only. It passes on current code.

- `crates/core/server-runtime/tests/raft_failover.rs`:
  - `#[path = "common/proxy.rs"] mod proxy;` (as `degraded_acks.rs` does).
    Update `common/proxy.rs`'s header ("for the tests that need one") to
    name `raft_failover.rs` too.
  - Extend `Cluster` with an opt-in `Cluster::partitionable()`, sharing one
    constructor body with `new()` (no copy). It puts one `Proxy` in front
    of every node's raft listener (`Proxy::start(raft_addr)`) and writes
    the **proxy** addresses into `peers`, so every raft connection to node
    *i* crosses `raft_proxies[i]`. It also adds one replication `Proxy`
    per replica, used as that replica's `replica_of`.
  - A `start_replicas_first()` bring-up. Start nodes 1 and 2, wait until
    one of them reports `melin_raft_is_leader 1`, then start the primary.
    Neither replica can promote meanwhile (genesis rule). This puts raft
    leadership on a **replica** deterministically. A normal bring-up does
    not: node id 1 (the primary) has the shortest tick period and usually
    wins the first election. Return the leading replica's index.
  - `isolate(i)`: freeze **all three** raft proxies and cut replica *i*'s
    replication proxy. Because sources are indistinguishable (§3), cutting
    only node *i*'s inbound proxy would leave its outbound appends flowing
    and its quorum intact. Freezing all three also severs primary ↔
    other-replica raft traffic. That plays no part: nobody can then win an
    election, and the primary ↔ other-replica *replication* link is
    untouched, so the primary keeps serving under `disk+ram`. Using
    `freeze` for raft and `cut` for replication uses both proxy modes
    (`proxy.rs` carries no dead-code allowance), and a silent raft
    partition is the realistic case. `heal()` restores all proxies.
- New positive test, green before and after the fix:
  `replica_leader_promotes_when_the_primary_dies`. Replicas-first bring-up,
  stop the primary, `wait_one_promoted()`. This is the case where
  leadership is held *through* the failure, which the existing
  `killed_primary_triggers_exactly_one_auto_promotion` does not guarantee.
- Run the whole file, then commit.

### Step 1b: reproduce the bug (not committed on its own)

Write `an_isolated_replica_leader_never_deposes_a_healthy_primary` in
`raft_failover.rs`:

1. `Cluster::partitionable()`, primary under `disk+ram`, replicas-first
   bring-up. Remember leader index `L` and the other replica `O`.
   `wait_formed`.
2. `isolate(L)`. While isolated, keep writing through a
   `melin_client::Connection` to the primary (`increment_request`), so the
   primary's journal moves past `L`'s. Over `NO_PROMOTION_WATCH` (reuse
   the existing constant), **record** rather than assert whether `L`
   served clients (`serves_clients`). Recording lets one red run show
   every consequence.
3. `heal()`. Over a heal window (e.g. 15 s), record: `primary.is_finished()`
   / `primary.shutdown_requested()` (fenced), the primary still serving,
   the primary's `melin_replicas_connected 2`, the primary's
   `melin_replica_divergence_total` (open question Q6), and whether an
   archive directory `<journal-name>.<reason>.<n>/` appeared next to `L`'s
   journal (`crates/core/transport-core/src/replication/archive.rs`,
   `archive_dir`, in `melin-transport-core`, not server-runtime).
4. Assert all of it at the end in one message, which also carries
   `cluster_summary()`: `L` never served, the primary never fenced and
   still serving, `L` back as a replica (2 connected), no divergence, no
   archive.

Run it on current code. **Expected red**: `L` served, and the primary
fenced on heal. Paste the observed record into the step 4 commit body (one
line) and into the PR. If the primary is *not* fenced on heal, the inferred
consequence in §1 is wrong. Rewrite §1 and the CHANGELOG wording to what
was observed before going on. The fix stands on the first assertion alone.

### Step 2: `refactor(server-runtime): a promotion request carries its origin`

Groundwork for the mint-time re-check, and a bug class closed in the type.

- `crates/core/server-runtime/src/promotion.rs`: replace the raw `u64`
  contract with `pub enum PromotionOrigin { Manual, Election { term: u64 } }`.
  `request(PromotionOrigin) -> bool` and `pending() -> Option<PromotionOrigin>`.
  Keep one `AtomicU64`: 0 = none, `u64::MAX` = manual, any other value is
  an election term. Terms never reach `u64::MAX`. Debug-assert it in
  `request`; release refuses rather than clamps. Justify the encoding in a
  comment per CLAUDE.md. Add `PromotionOrigin::epoch_floor()` (`Manual` →
  1, `Election { term }` → term), so `journal_promotion_epoch_bump`'s
  `max(epoch + 1, floor)` is unchanged.
- Callers: `raft_promotion::consider_auto_promotion`
  (`Election { term }`), `admin.rs` `PROMOTE` (`Manual`), the
  `promotion_request.pending()` arguments in `server.rs`'s two replica
  branches, and `journal_promotion_epoch_bump`'s alignment warning, which
  becomes a `match` on the origin. That also fixes the warning being
  skipped for a term-1 election.
- Tests in `promotion.rs`: the existing four ported, plus "an election at
  term 1 is not a manual promotion" (the bug this closes), and
  `epoch_floor` per variant.
- No CHANGELOG entry: `mod promotion` is private in
  `crates/core/server-runtime/src/lib.rs`, so the type is not public API
  (Q7 rechecks that nothing re-exports it).

### Step 3: `feat(raft): publish the leader's quorum-ack age`

- `crates/core/raft/src/driver.rs`:
  - Add the §5 constants and assertions, with doc comments that cite the
    openraft functions they rely on.
  - Extract `fn heartbeat_interval_ms(node_id: NodeId) -> u64` from
    `driver_main` (used there; no copy of the jitter formula).
  - Extract `fn publish_metrics(status: &RaftStatus, m: &RaftMetrics<NodeId, Node>)`
    from the bridge loop (term, leader, role, quorum-ack age), so it is
    unit-testable without tokio.
  - Re-export `QUORUM_ACK_MAX_AGE` from `lib.rs`.
- `crates/core/transport-core/src/health.rs`, `RaftStatus`:
  - A new **private** field `millis_since_quorum_ack: AtomicU64`.
    Sentinel `u64::MAX` means none; **never 0**, because 0 means "acked
    just now".
  - `pub fn set_millis_since_quorum_ack(&self, v: Option<u64>)`.
  - `pub fn millis_since_quorum_ack(&self) -> Option<u64>`, which reads
    the sentinel back as `None`. `Some(u64::MAX)` also reads as `None`;
    both refuse, so the collision is harmless (say so in the comment).
  - The sentinel lives only behind these two methods, so no caller can
    misread it.
  - `new` initialises to the sentinel, and `mark_stopped` resets to it (a
    dead driver must not freeze a fresh age).
- Health exposure, following the `melin_ack_policy_journaled` pattern:
  - `RaftSnapshot.quorum_ack_age_ms: Option<u64>`.
  - In the raft block of `write_metrics`, emit
    `melin_raft_quorum_ack_age_ms` only when `Some` and the driver is
    running. HELP text: "Milliseconds since a majority of the control
    plane last acknowledged this node as leader; absent when not leading
    or not yet acknowledged."
  - The unit suffix follows `melin_replica_ack_latency_us`.
- `crates/core/raft/Cargo.toml`: keep the `openraft = "0.9"` requirement
  (an exact `=0.9.25` pin in a published library would make resolution
  brittle for any consumer that also depends on openraft 0.9.x, and
  `multiple-versions = "warn"` in `deny.toml` would not catch the
  duplicate). Add a comment beside it that `QUORUM_ACK_MAX_AGE` relies on
  the lease semantics read in 0.9.25 (`handle_vote_req`,
  `EngineConfig::new`) and must be re-derived on any upgrade (0.10 changes
  elections). The premise is pinned behaviourally rather than by version:
  `Cargo.lock` holds 0.9.25 for our own builds, and
  `a_leader_that_loses_both_peers_keeps_leading_and_its_ack_ages` fails if
  a future openraft adds check-quorum or stops ageing the ack.
  `Cargo.lock` should not move. Check, and stage it if it does.
- Tests:
  - `health.rs`: extend `raft_status_mark_stopped_clears_leadership` to
    cover the reset to `None`. Add a sentinel round trip: `None`,
    `Some(0)` stays `Some(0)`, `Some(1234)`, and `Some(u64::MAX)` reads
    `None`. Extend `replica_health_endpoint_serves_raft_gauges_and_reports_halted`
    (gauge present with value), and `stopped_driver_clears_is_leader_gauge`
    plus a never-acked leader (gauge absent).
  - `driver.rs` unit tests: `publish_metrics` with
    `RaftMetrics::new_initial` as a leader with `Some(7)` gives `Some(7)`;
    as a follower with `None` gives `None`. `heartbeat_interval_ms` over
    ids 0..150: the max equals `HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS - 1`,
    so `MAX_TICK_PERIOD_MS` is checked against the real formula, not
    restated.
  - `crates/core/raft/src/tests/election.rs`, using its `Cluster`,
    `await_single_leader` and `stop_node`:
    - `a_healthy_leaders_quorum_ack_age_stays_under_the_gate`: sample the
      leader's status every 50 ms for 3 s and assert the max ≤
      `QUORUM_ACK_MAX_AGE`. This pins liveness assertion 2 empirically.
    - `a_leader_that_loses_both_peers_keeps_leading_and_its_ack_ages`:
      stop both followers and wait `QUORUM_ACK_MAX_AGE` + 1 s. Assert the
      leader still reports `ROLE_LEADER` (pins the no-check-quorum
      premise; if it ever fails, openraft changed and this whole design
      must be revisited) and that its age exceeds the gate. Port range:
      the file's own 15000..20000.
- CHANGELOG **Added**: the `melin_raft_quorum_ack_age_ms` gauge.
  **Changed**: `RaftStatus` gains a field (breaks struct literals; build
  it with `RaftStatus::new`).
- `docs/replication.md` "Observability": add the gauge to the
  `melin_raft_*` list.

### Step 4: `fix(server-runtime): refuse auto-promotion without a majority-acknowledged lease`

- `raft_promotion.rs`:
  - A pure `fn leader_lease_check(quorum_ack_age: Option<Duration>) -> Result<(), &'static str>`:
    `None` refuses, `Some(age)` with `age > QUORUM_ACK_MAX_AGE` refuses,
    and `age <= QUORUM_ACK_MAX_AGE` passes. It has one refusal string,
    a named const like `TIP_NOT_READY` because step 5 reuses it:
    "a majority of the control plane has not acknowledged this node's
    leadership within the leader lease — it may be partitioned, and a
    partitioned leader must not depose the primary".
  - `AutoPromotionInputs` gains `quorum_ack_age: Option<Duration>`,
    commented on why `Option<Duration>` and not the raw atomic.
  - In `auto_promotion_decision`, place the check **after the
    primary-down grace and before the genesis rule and the peer-tip
    veto**. Reasons:
    - "the link is up" and "moments ago" stay the reasons a healthy
      cluster logs;
    - without a quorum the peer observations are meaningless (all
      `Silent`);
    - an isolated leader should be told *why* (partitioned), not shown a
      peer table.
    Add a bullet to the doc list on the function.
  - `consider_auto_promotion` fills it from
    `status.millis_since_quorum_ack().map(Duration::from_millis)`, reading
    `term` **before and after** the age and refusing on a change. With the
    `Relaxed` stores and loads kept from today's bridge, this is cheap
    insurance that catches most tears, not a guarantee. Relaxed loads of
    two locations give no cross-location ordering, so on aarch64 the age
    can come from either side of a term change. What makes acting on a
    torn read harmless is Q3's timing argument (an age from an older term
    cannot outlive a re-election) and the mint-time term comparison in
    step 5. If a guarantee is wanted, the bridge stores the age then the
    term with `Release`, and the reader loads term (`Acquire`), age, term
    (`Acquire`), seqlock-style. Q3 decides whether that is worth it.
- Tests (`raft_promotion.rs`):
  - `ok_inputs()` gets `Some(Duration::ZERO)`.
  - New `refuses_without_a_quorum_ack`: `None` refuses with the lease
    reason.
  - New `refuses_a_stale_quorum_ack_and_passes_at_the_threshold`:
    `QUORUM_ACK_MAX_AGE` passes, one millisecond more refuses.
  - New `an_isolated_leader_is_told_it_is_partitioned`: both peers
    `Silent`, link down past the grace, age `None`; the reason is the
    lease one, not the peer veto.
  - `silent_peers_do_not_block_promotion`: keep it, and comment that with
    the gate both peers silent cannot coexist with a fresh lease in
    production (a fresh lease means a peer answered well inside
    `PEER_TIP_GRACE`), so the decision is tested in isolation here.
  - `a_changed_refusal_reason_is_reported_within_the_same_term` and
    `auto_promotion_judges_by_the_lineage_policy_not_the_nodes_own`: set
    a fresh age on the `RaftStatus`, otherwise they would now stop at
    the lease.
  - New `consider_auto_promotion_files_nothing_without_a_lease`: a leader
    `RaftStatus` with no age leaves `promote.is_requested()` false and
    logs the lease reason.
- Integration (`raft_failover.rs`):
  - Step 1b's test, now green.
  - New `a_replica_leader_alone_never_auto_promotes`: replicas-first
    bring-up, then stop the primary **and** the other replica. Over
    `NO_PROMOTION_WATCH` the leader never serves. Red on current code
    too: this is the behaviour change, pinned.
  - Steps 1's positive test and `killed_primary_triggers_exactly_one_auto_promotion`
    must stay green: the gate must not cost a genuine failover.
- `docs/replication.md`, "Automatic failover", refusal list. New bullet:
  the elected replica refuses when a majority of the control plane has
  not acknowledged its leadership recently. A partitioned node can still
  believe it leads, and promoting it would depose a healthy primary that
  the rest of the cluster still sees. This **includes the case where both
  other nodes are dead**: a lone survivor never auto-promotes, and the
  operator decides with a manual `PROMOTE`. Also note the cost: failover
  waits for the new leader's first acknowledged heartbeat, about one
  heartbeat interval.
- `docs/replication.md` "Cluster recovery", "Primary crashes, one or both
  replicas alive": add that with the primary and the other replica both
  gone, recovery is manual.
- CHANGELOG **Fixed**: an isolated replica leader no longer deposes a
  healthy primary (wording per step 1b's observation). **Changed**: a
  replica that cannot reach a majority, including the last survivor, no
  longer auto-promotes.

### Step 5: `fix(server-runtime): re-check the leader lease when the promotion epoch is minted`

- `server.rs`, `journal_promotion_epoch_bump`:
  - Takes `origin: PromotionOrigin` and `raft: Option<&RaftStatus>` and
    returns `Result<(), PromotionAbandoned>`.
  - For `Election { term }`, immediately before
    `input_producer.publish(...)` with nothing blocking in between, call a
    new `raft_promotion::mint_time_lease_check(status, term)`. It requires
    `running`, `role == ROLE_LEADER`, `term` unchanged (read before and
    after), and `leader_lease_check(age)`, reusing step 4's pure function
    and refusal string.
  - On refusal, publish nothing, `warn!` (a degraded condition, not a
    malfunction, per the log-level rules) and return `Err`.
  - `Manual` skips the check: an operator's `PROMOTE` remains available
    during a control-plane outage (documented).
  - `raft == None` with `Election` cannot happen. Make it unrepresentable
    if it is cheap; otherwise refuse.
- Both call sites (`run_as_primary`, `run_as_primary_dpdk`) `?` the
  result. `PipelineHandles` drop joins the spawned stages (verified, §3).
  The node exits with an error, its journal untouched by the tenure (the
  bump is the tenure's first entry), and restarts as a replica of its
  configured primary. This follows the `check_promotable` precedent ("a
  promotion is refused, and the node exits with an error"). Q4 covers the
  alternative.
- Tests:
  - `raft_promotion.rs`: `mint_time_lease_check` cases for not leader,
    term moved, stale age, `None` age, driver stopped, and the pass case.
  - `server.rs` tests: `journal_promotion_epoch_bump` with an `Election`
    origin and a lease-less `RaftStatus` publishes nothing (the consumer
    sees no slot, the fence epoch is unchanged) and returns `Err`; with a
    fresh lease it publishes as before; with `Manual` it publishes with no
    status. Q5 asks whether building the ring producer there is
    practical.
- `docs/replication.md`, after the refusal list: a promotion that loses
  its control-plane majority between the decision and taking over is
  abandoned and the node exits with an error. Its journal is unchanged,
  and restarting it rejoins it as a replica.

### Step 6: `docs: record the quorum-lease gate for contributors`

- `docs/internal/raft-control-plane.md`, "Auto-promotion policy": the
  gate, the §4 lease argument (including the challenger-nudge case and
  the peer-tip veto plus epoch fencing that cover it), the §5 constants
  and assertions, the
  openraft-version dependency of the threshold (re-derive on upgrade),
  and the mint-time re-check.
- Delete this plan file, or reduce it to a pointer if the team keeps
  plans.

## 7. Open questions to verify before or while implementing

- **Q1 Threshold.** Choose 1000 ms (recommended) or up to 1176 ms (§5).
  Re-measure with the step 3 election test under `cargo nextest` load.
  If the healthy max gets near 1000, revisit.
- **Q2 Bridge latency.** The bridge shares the current-thread runtime
  with the raft core and RPC server, and can be held by
  `raft.trigger().elect().await`. Confirm that await is just a oneshot to
  the core and cannot stall for anywhere near `SCHEDULING_SLACK_MS`. If
  it can, move the stores ahead of the nudge in the loop.
- **Q3 Torn reads.** Leaving leadership and regaining it takes at least
  `election_timeout_min` (1 s), many bridge stores, so a fresh age from
  an older term cannot persist into a newer one. The term double-read is
  cheap insurance. Confirm no path goes leader(t) → leader(t+1) without
  an intermediate published state. The `single-term-leader` feature is
  on (`crates/core/raft/Cargo.toml`); check it does not change this. If
  the argument holds, keep `Relaxed`. If it does not, switch to the
  Release/Acquire seqlock-style ordering described in step 4.
- **Q4 Mint refusal policy.** Exit (recommended, simplest, follows
  `check_promotable`) or wait a bounded time for the lease to return in
  the same term. Waiting buys little: the request-to-mint window is
  normally sub-second, so losing the majority inside it is a real signal.
- **Q5 Testing `journal_promotion_epoch_bump`.** It has no unit test
  today. Check that a ring `Producer`/consumer pair and `FenceState` can
  be built in `server.rs` tests without a pipeline. If not, test
  `mint_time_lease_check` (pure) plus an integration case, and record the
  gap.
- **Q6 Post-heal evidence.** Confirm `melin_replica_divergence_total` is
  exported on the **primary** (it is in `HealthSnapshot`'s primary block)
  and that the archive directory naming in `archive_dir` is stable enough
  to assert on.
- **Q7 Public API.** `mod promotion` is private, so step 2 needs no
  CHANGELOG entry. Confirm with `rg "PromotionRequest"` that no `pub use`
  re-exports it.
- **Q8 DPDK.** `raft_failover.rs` also runs under the netns DPDK runner.
  Raft stays on kernel TCP. Confirm a raft `Proxy` bound at
  `melin_test_node::local_ip()` is reachable from the nodes' kernel raft
  sockets there (`degraded_acks.rs` proves this only for the replication
  link), and that the replication proxy works for `replica_of` (the
  launcher derives `peer_mac` from it). If raft proxies do not work under
  DPDK, gate the partition tests to kernel TCP and say why in the file
  header.
- **Q9 Frozen raft links on heal.** A restored frozen relay flushes
  stale raft frames. openraft should discard them by vote/term. Confirm
  the heal phase converges. If not, `cut` then `restore` the raft proxies
  on heal.
- **Q10 Late primary bring-up.** `start_replicas_first()` starts node 1
  after nodes 2 and 3 have elected a leader. A fresh node 1's
  `raft.initialize(members)` succeeds locally (`NotAllowed` fires only on
  a restart with stored state, `driver.rs` around the initialize call),
  so it writes its own membership entry and campaigns at term 1 while a
  leader at term >= 1 exists. openraft should reject its vote (follower
  lease, and a single-term leader id it cannot beat) and overwrite its log
  on the first append, but the existing tests exercise only concurrent
  initialisation or a stored-state restart. Confirm the bring-up converges
  to the replica leader without a re-election (assert the leader's id and
  term are unchanged after node 1 joins), since step 1's positive test and
  every later integration case depend on it. If it does not, start node 1
  with a delayed or skipped `initialize` and say why.

## 8. Behaviour change and residual risk

- **Changed:** a replica that cannot reach a majority never
  auto-promotes, including a lone survivor whose two peers are dead.
  Before, it promoted after the grace. Recovery there is a manual
  `PROMOTE`. That is the correct trade for a durability-critical system:
  a lone node cannot tell "both others dead" from "I am partitioned".
- **Cost:** a new leader promotes only after its first acknowledged
  heartbeat (about one tick). That is inside the time the peer-tip veto
  already waits for `Unknown` peers.
- **Not addressed (follow-up):** a *one-sided* partition where the leader
  replica loses only the primary while the other replica still reaches
  both. The leader then has a fresh lease, sees the other replica `Fresh`,
  and still deposes a primary the rest of the cluster can see. That is
  the documented one-sided-link limitation of `PRIMARY_DOWN_GRACE`.
  Closing it needs the other replica's primary-link state carried in the
  raft envelope, a separate design.

## 9. Coverage

After step 5, run
`cargo llvm-cov nextest -p melin-raft -p melin-transport-core -p melin-server-runtime --html`.
Cover every changed branch:

- `leader_lease_check` (three arms);
- `mint_time_lease_check` (each refusal);
- the sentinel accessors;
- `publish_metrics`;
- the gauge's present and absent branches;
- `PromotionOrigin` encoding.

Any changed line still unexecuted goes into §10 with a reason.

## 10. Left untested, and why

- **The decision-to-mint race itself** (lease lost after a passing
  decision but before the mint). Producing it end to end needs isolation
  timed inside a sub-second window. It is covered by the pure
  `mint_time_lease_check` tests and the `journal_promotion_epoch_bump`
  unit test (subject to Q5), not by an integration test.
- **A nudged follower winning while the leader's lease is fresh** (§4).
  No new test: the case is covered by mechanisms that predate this fix
  and have their own tests, the peer-tip veto (`auto_promotion_decision`
  unit tests with a `Fresh` peer ahead) and epoch fencing
  (`fence_if_superseded`). The gate changes neither.
- **The real-clock safety margin** (assertion 1). It is a compile-time
  proof over the constants. Nothing runtime exercises "a follower granted
  a vote at S + lease − ε". Checking it would mean instrumenting openraft;
  the derivation is instead tied to the locked 0.9.25 and the
  re-derive-on-upgrade comment beside the dependency.
- **openraft's lease internals** (`handle_vote_req`, `EngineConfig::new`).
  They are not tested from melin: `Raft` handles stay inside the driver
  thread, so a follower cannot be sent a crafted `VoteRequest` without
  widening `RaftHandles` for tests only. `Cargo.lock` (0.9.25 for our
  builds), the re-derive-on-upgrade comment on the `"0.9"` requirement,
  and the no-check-quorum election test bound the risk. A downstream
  build resolving a later 0.9.x patch is not covered by any of these;
  openraft treating lease semantics as stable within 0.9 is assumed.
- **The realistic partition** where the primary and the other replica
  still talk raft and elect a new leader while `L` is cut off. The proxy
  cannot tell sources apart on loopback (§3), so the test freezes the
  whole raft mesh. A namespace-per-node test (the netns runner) could do
  it later.
- **DPDK-specific partition behaviour**: only if Q8 forces the partition
  tests to kernel TCP. The gate itself is transport-independent (raft is
  kernel TCP everywhere), and the mint path is shared by both transports.
