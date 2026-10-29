# Plan: a leader-lease gate on raft auto-promotion

Branch `fix/raft-promotion-quorum-lease`, planned against `85075207`.
Contributor document: an implementation plan, not operator documentation.
Function names are the anchors. Line numbers are left out because they
go stale. The options behind each open question are analysed in
`raft-promotion-quorum-lease-open-questions.md`; this plan records what
was adopted.

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

Inferred consequences, not yet proven by a test (step 1c proves or
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

Where the promotion epoch is minted (`journal_promotion_epoch_bump`), the
node confirms that it still leads in the term it was elected at, with a
real quorum round trip (openraft's `Raft::get_read_log_id`, asked of the
driver thread under a deadline the server thread takes before asking),
and exits with an error if it cannot. That is the safety mechanism; one
compile-time assertion ties its deadline to openraft's follower lease.
In front of it, a cheap early filter: bridge openraft's
`RaftMetrics::millis_since_quorum_ack` into `RaftStatus` and make the
pure `auto_promotion_decision` refuse unless a majority acknowledged this
leader within `QUORUM_ACK_MAX_AGE` (half the lease). The filter makes no
safety claim (the time from request to mint is unbounded); it keeps an
isolated leader or a lone survivor from starting a promotion at all, and
tells the operator why. Before any of that (step 0), a promotion request
carries its origin in its type: an election term of 1 is currently
indistinguishable from a manual `PROMOTE`, and the mint needs the term.

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
| The election nudge (`trigger().elect()`) bypasses the candidate's *own* lease-extended timer (`handle_tick_election` is not consulted), so a follower whose ack keeps the leader's lease fresh can still campaign and vote for itself. Other voters still apply the lease in `handle_vote_req`. The nudge fires only while the nudging node's tip is ahead of the leader's tip as last heard within `PEER_TIP_GRACE`, with **its own** primary link down past `PRIMARY_DOWN_GRACE` | `driver_main` nudge branch; `raft_promotion.rs`, `challenger_should_campaign`, `consider_challenger_nudge` |
| Melin's heartbeat interval is 200 ms + per-node jitter `(id % 150) * 37 % 150` ∈ [0, 149]. Election timeout 1000–2000 ms | `crates/core/raft/src/driver.rs`: `HEARTBEAT_INTERVAL_MS`, `HEARTBEAT_JITTER_MS`, `HEARTBEAT_JITTER_SPREAD`, `ELECTION_TIMEOUT_*_MS`, `driver_main` |
| The driver bridge waits on `metrics_rx.changed()` capped at `SHUTDOWN_POLL` (100 ms), then stores term/leader/role with `Relaxed` | `driver_main` bridge loop |
| The promotion poll is `POLL_INTERVAL` (100 ms); decision and `promote.request(term)` are back to back | `raft_promotion.rs`: `consider_auto_promotion`, `spawn_auto_promotion` |
| The request is consumed by the receive loop, then drain, `take_pipeline_for_promotion`, `check_promotable`, `prefault` (can take seconds), `run_as_primary[_dpdk]` (spawns stages), and finally `journal_promotion_epoch_bump` publishes the `EpochBump`. Nothing bounds this path in time | `replication/receiver_transport.rs`, `replication/tcp_receiver.rs`, `server.rs` (replica branch, `run_as_primary`, `run_as_primary_dpdk`, `journal_promotion_epoch_bump`) |
| The bump is published before `on_primary` and before any client or replica is served, on both transports | `server.rs`, the doc on `journal_promotion_epoch_bump` and its two call sites |
| An early `Err` return from `run_as_primary` after stages spawned is safe: `PipelineHandles`'s drop stops and joins them | `server.rs`, doc on `PipelineHandles` (`spawn_health_endpoint(..)?` already relies on it) |
| The raft driver keeps running through `run_as_primary` (the guard drops after it returns), so the driver is live at mint time | `server.rs` replica branch comment ("The raft guard drops … after this returns") |
| `PromotionRequest::MANUAL == 1`, and the first election after `initialize` is term 1. So an auto-promotion at term 1 files the same value as a manual `PROMOTE` | `promotion.rs`; `journal_promotion_epoch_bump`'s `requested_epoch > MANUAL` test already misclassifies term 1 |
| `tip_ready` is set after **local** journal recovery, before the replica connects, so two replicas can elect a replica leader with the primary not yet up | `tcp_receiver.rs` (`tip_ready.store(true, …)` after `recover_replica_state`); `replication/dpdk.rs` likewise |
| A blank replica that never observed a primary refuses to promote, so a replicas-first test bring-up cannot trigger a promotion | `auto_promotion_decision` genesis rule |
| Raft dial addresses come from the stored membership, which is the same on every node, and the outgoing socket is not bound (`TcpStream::connect`), so on loopback every source address is 127.0.0.1. A test proxy therefore cannot tell which node a raft connection comes from, whatever the ports | `crates/core/raft/src/network.rs` (`addr: node.addr`, `RaftClient::ensure_connected`); checked empirically |
| Only `TimedOut`, `ConnectionRefused` and `NotFound` map to `RPCError::Unreachable`, and openraft's replication loop backs off only on `Unreachable`; any other connect error is retried on the next heartbeat | `network.rs`, `to_rpc_error`; openraft `replication/mod.rs` (`RPCError::Unreachable` arm sets `backoff`) |
| Linux routes all of 127.0.0.0/8 to `lo`, so nodes in one process (and one netns under the DPDK runner) can each take their own 127.0.0.*i* without setup | kernel behaviour; raft listeners are configured by each test, not by `melin-test-node` |
| `Raft::get_read_log_id` confirms leadership by sending one heartbeat per voter, each under `ttl = heartbeat_interval`, through a **fresh** client per voter (`new_client`, so a fresh connect and Ed25519 handshake). A non-leader answers `ForwardToLeader` at once; a follower's `HigherVote` also yields `ForwardToLeader`; no majority yields `QuorumNotEnough`. It does not wait for the state machine (`ensure_linearizable` adds that wait, which we do not need). openraft logs each failed voter at `error!` in its own target | `raft/mod.rs`, `Raft::get_read_log_id`, `Raft::ensure_linearizable`; `core/raft_core.rs`, `handle_check_is_leader_request` |
| On `Ok`, `read_log_id` is `max(leader.noop_log_id, committed)` captured with the vote before sending, and its `leader_id.term` is the confirming leader's term | `engine/handler/leader_handler/mod.rs`, `LeaderHandler::get_read_log_id`; `proposer/leader.rs`, `Leader::new`; open-questions Q11 |
| `RaftStatus` is a `pub` struct with `pub` fields in published `melin-transport-core`. Adding a field breaks external struct literals | `crates/core/transport-core/src/health.rs` |
| Raft health gauges are Prometheus-only (the one-line status omits them). Optional gauges are emitted only when `Some` (the `melin_ack_policy_journaled` pattern) | `HealthSnapshot::collect`, `HealthSnapshot::write_prometheus` in `health.rs` |
| openraft is required as `"0.9"` (not pinned). `Cargo.lock` has 0.9.25 | `crates/core/raft/Cargo.toml`, `Cargo.lock` |

## 4. Why the promotion is sound

**The lease.** Let S be the leader-clock **sending** time of the newest
request that a majority acknowledged. In a 3-voter cluster that is the
newest follower ack, because the leader counts itself. That follower F
handled the request at real time R ≥ S and touched its `vote_utime` at R.
Until R + `leader_lease` ≥ S + `leader_lease`, F refuses every *other*
node's vote request, nudged candidates included. Any election needs 2 of
3 votes, so before S + `leader_lease` a third node can be elected only
with L's own vote, that is only by deposing L. L's own `vote_utime` is
not refreshed by acks (openraft sets it in `VoteHandler::update_vote`,
from `Engine::establish_leader`; `ReplicationHandler::update_leader_clock`
touches only `clock_progress`), so L may grant it; that case is handled
below ("The leader itself may still grant…").

F itself can be elected, though. Its timer-driven campaign waits
`leader_lease + election_timeout` from `vote_utime`, but Melin's
challenger nudge (`challenger_should_campaign` → `raft.trigger().elect()`)
skips that timer. F votes for itself, and the third node may grant its
vote (its own lease has lapsed, or its vote is not committed). F can then
win term T+1 although L was just acknowledged. The lease alone does not
exclude this case. Two existing mechanisms cover it:

- **The peer-tip veto.** The nudge fires only while F's tip is ahead of
  the leader's tip as F heard it within `PEER_TIP_GRACE`. F's ack is what
  keeps L acknowledged, and the same exchange carries F's tip, so L sees
  F as `PeerTipObservation::Fresh(tip)` with `tip.is_ahead_of(local_tip)`.
  L is a replica whose primary link is down, so its own tip does not
  move. The veto (the first arm of the peer loop in
  `auto_promotion_decision`) refuses. The step 3 filter runs before this
  veto but does not replace it.
- **Epoch fencing.** If F wins anyway (L's veto passed before F's tip
  arrived, and F won after L's mint-time confirmation), L's epoch is T.
  F's promotion mints epoch T+1, and `fence_if_superseded` fences L as
  soon as the higher epoch reaches it. The two-primaries window is
  bounded by that propagation, and L never deposes F.

So the claim is narrower than "no other leader exists": no leader can
exist that L's own peer-tip view and epoch rule do not already handle.

**The mint-time confirmation carries the safety claim.** Let C be the
moment the driver starts `get_read_log_id`. openraft captures the vote
and `read_log_id` before sending, so `Ok` means a majority accepted
heartbeats carrying this leader's vote, sent at or after C. By the lease
argument, before C + `leader_lease` no node other than an acknowledging
follower can be elected except by deposing L with L's own vote; that
follower's nudged win is the case the two bullets above handle, and
L's deposition the paragraph below. The confirmation compares
`read_log_id.leader_id.term` with the term the request was filed for, so
a node that lost term t and won t′ in between refuses (Q11).
`LeaderConfirm::confirm` takes its deadline (`LEADER_CONFIRM_TIMEOUT`) on
the server thread **before** sending the request, so before C. A reply it
accepts arrived before C + `LEADER_CONFIRM_TIMEOUT`, and the publish
follows with nothing blocking in between, so the epoch is minted before
C + `LEADER_CONFIRM_TIMEOUT` + `SCHEDULING_SLACK` < C + `leader_lease`
(the mint-time safety assertion, §5). No metric enters this argument, and
a stalled raft runtime turns into a `TimedOut` refusal, never a late mint.

The leader itself may still grant a higher-term vote and step down. The
confirmation then fails (openraft answers a non-leader with
`ForwardToLeader`). If it steps down after the confirmation returned
`Ok`, before the mint, the epoch rule fences L only when the newer leader
promotes and mints a higher epoch. If the newer leader is the primary
(which never mints), nothing fences L's epoch, and L's mint deposes it;
but reaching that between C and the mint needs a peer that has not heard
L for a lease while another still acks L and L's primary link is down,
which is the one-sided partition of §8, not a case this plan closes.

**The decision-time gate is an early filter.** It states: this node
leads, and its last reported quorum-ack age is at most
`QUORUM_ACK_MAX_AGE`. The reported age only ever under-reports the truth
(it is the age at the last flush, which happens at least once per tick,
bridged at least every `SHUTDOWN_POLL`, floored to ms), and the interval
from request to mint is unbounded, so a pass proves nothing about the
mint. What the filter buys:

- an isolated leader has no new acks, its age only grows, and it refuses
  for good, with a reason that names the partition, instead of draining,
  prefaulting, failing the confirmation and exiting;
- a lone survivor whose two peers are dead refuses the same way, without
  disturbing its replica pipeline (a deliberate behaviour change, §8);
- the operator is not shown a peer table of `Silent` peers as the reason.

Its one constraint is liveness: it must not refuse a healthy leader (the
liveness assertion, §5, and the step 2 election test). A filter that
wrongly passes costs one abandoned promotion and an exit, never safety,
so the plan carries no margin argument over metric staleness and no
safety assertion over it.

## 5. Constants

All in `melin-raft`, next to the values they derive from. Every input is
a melin-raft constant, so each assertion sits in the same crate as what
it asserts over and no constant is copied across crates. The derived
ones (`LEADER_LEASE_MS`, `QUORUM_ACK_MAX_AGE_MS`, `MAX_TICK_PERIOD_MS`)
are defined as the expression in the table, not a literal, so a change
to their inputs carries through; the other three are literal allowances,
and the assertions are the backstop for all of them.

| Constant | Value | Derivation | Step |
|---|---|---|---|
| `LEADER_LEASE_MS` | 2000 | `= ELECTION_TIMEOUT_MAX_MS` (openraft `EngineConfig::new`) | 2 |
| `QUORUM_ACK_MAX_AGE_MS` | 1000 | `= LEADER_LEASE_MS / 2`, the filter threshold (`pub const QUORUM_ACK_MAX_AGE: Duration`). Decided with the user | 2 |
| `MAX_TICK_PERIOD_MS` | 523 | `= (HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS - 1) * 3 / 2`, the slowest node's tick | 2 |
| `RTT_BUDGET_MS` | 100 | allowance for one heartbeat round trip, liveness assertion only | 2 |
| `LEADER_CONFIRM_TIMEOUT_MS` | 500 | the mint-time confirmation's deadline, taken on the server thread before the request is sent (`const LEADER_CONFIRM_TIMEOUT: Duration`, crate-private: only the driver and its tests read it, step 4). openraft gives each voter one `heartbeat_interval` (at most 349 ms) including a fresh connect and handshake; anything past half a second is a control plane in trouble, and refusing is the right answer | 4 |
| `SCHEDULING_SLACK_MS` | 200 | covers reply-to-publish on the server thread (preemption, the publish itself) and clock-rate drift between the leader's clock and the followers' lease clocks | 4 |

Compile-time assertions (`const _: () = assert!(…)`, each with a message):

- **Liveness** (step 2): `QUORUM_ACK_MAX_AGE_MS >= MAX_TICK_PERIOD_MS + RTT_BUDGET_MS`
  (1000 ≥ 523 + 100). A healthy leader's reported age peaks at about one
  tick plus a round trip, at the flush that comes just before a
  heartbeat reply. The filter must not refuse a healthy leader.
- **Mint-time safety** (step 4): `LEADER_CONFIRM_TIMEOUT_MS + SCHEDULING_SLACK_MS < LEADER_LEASE_MS`
  (500 + 200 = 700 < 2000, §4).

Deliberately absent: a bridge-poll constant and a safety assertion over
decision-time staleness (the filter makes no safety claim, §4), and an
assertion that `HEARTBEAT_JITTER_SPREAD` is coprime with
`HEARTBEAT_JITTER_MS` (unrelated to this fix; step 2's unit test over
`heartbeat_interval_ms` checks the offsets are distinct directly, which is
the property the existing comment claims).

**Threshold trade-off (decided: 1000 ms).** A single lost heartbeat
brings the reported age to about two ticks plus a round trip. On the
slowest-ticking node (523 ms) that is past 1000 ms and refuses
*transiently*; on faster-ticking nodes it takes more consecutive losses.
The 100 ms poll retries, so the cost is a short delay and one log line,
never a stuck failover and never an unsafe one.

The healthy-case figure (≈ one tick) is checked empirically in step 2;
see the election-test case there.

## 6. Steps

Each step builds, passes clippy/doc/tests (the pre-commit hook), and is
committed on its own, but only when the user asks. Step 1c is the one
uncommitted step, because the hook runs the suite and a red test cannot be
committed (`#[ignore]` is banned).

### Step 0: `refactor(server-runtime): a promotion request carries its origin`

Independent of everything after it, so it goes first. Its standalone fix
is small: an auto-promotion needs `term > fence_epoch`, so at term 1 the
fence epoch is 0 and the minted epoch is 1 either way; the misclassified
alignment warning could only misfire if the fence epoch moved between
decision and mint. The real value is the type, which step 4's mint-time
confirmation needs ("an election at term t") and a raw `u64` cannot say.

- `crates/core/server-runtime/src/promotion.rs`: replace the raw `u64`
  contract with `pub enum PromotionOrigin { Manual, Election { term: u64 } }`.
  `request(PromotionOrigin) -> bool` and `pending() -> Option<PromotionOrigin>`.
  Keep one `AtomicU64` (still lock-free for the pollers, still one
  compare-exchange for first-request-wins): 0 = none, `u64::MAX` =
  manual, any other value is an election term. Terms never reach
  `u64::MAX`; `request` refuses `Election { term: u64::MAX }` (returns
  `false`, with a comment that the value is the manual sentinel) rather
  than asserting, so debug and release behave the same and the refusal is
  testable. Justify the encoding in a comment per CLAUDE.md. Add
  `PromotionOrigin::epoch_floor()` (`Manual` → 1, `Election { term }` →
  term), so `journal_promotion_epoch_bump`'s `max(epoch + 1, floor)` is
  unchanged.
- Callers: `raft_promotion::consider_auto_promotion`
  (`Election { term }`), `admin.rs` `PROMOTE` (`Manual`), the
  `promotion_request.pending()` arguments in `server.rs`'s two replica
  branches, and `journal_promotion_epoch_bump`'s alignment warning, which
  becomes a `match` on the origin. That also fixes the warning being
  skipped for a term-1 election. Test callers of the `u64` API port with
  the type: `admin.rs`, `replication/tcp_receiver.rs`,
  `replication/receiver_transport.rs` and `replication/mod.rs` tests
  (open-questions Q7; the compiler lists them).
- Tests in `promotion.rs`: the existing four ported, plus "an election at
  term 1 is not a manual promotion" (the bug this closes), "a term equal
  to the manual sentinel is refused", and `epoch_floor` per variant.
- No CHANGELOG entry: `mod promotion` is private in
  `crates/core/server-runtime/src/lib.rs`, nothing re-exports it, and the
  one public holder (`ReplicaControlPlane`) keeps it in a `pub(crate)`
  field (Q7).

### Step 1: `feat(raft): dial peers from the raft listener's address` + `test(server-runtime): drive raft failover through partitionable links`

Two commits. The first is a small production change that makes per-pair
partitions possible in tests (per-node loopback IPs with source-bound
raft dialing, decided with the user); the second is the harness, which
passes on current code.

**1a — source-bind outgoing raft connections.**

- `crates/core/raft/src/network.rs`, `RaftClient::ensure_connected`:
  when the node's raft `bind` IP is not unspecified (`0.0.0.0` / `::`),
  parse the peer's stored address as a `SocketAddr` (`TcpSocket::connect`
  needs one; the CLI already guarantees an IP literal, `parse_raft_peer`),
  build a `tokio::net::TcpSocket` of the bind's family, bind it to
  `(bind_ip, 0)`, then `connect` (still under `CONNECT_TIMEOUT`). When the
  bind is unspecified, keep today's `TcpStream::connect` (the kernel picks
  the source, and a hostname still resolves). Thread the bind IP from
  `RaftConfig::bind` through `RaftClientFactory` into each `RaftClient`.
- On the bound path, refuse before any syscall, with an `io::Error`
  naming the bind IP and the peer: an address that is not an IP literal
  (document on `RaftConfig::bind` that a specific bind needs IP-literal
  peers), and a family mismatch. Map `AddrNotAvailable` (the bind IP is
  no longer assigned) and these two refusals to `RPCError::Unreachable`
  in `to_rpc_error`, so openraft backs off as for a dead peer instead of
  retrying every heartbeat (§3). Log at `debug!` with the peer and the IP.
- Startup refusal (decided with the user, open-questions Q14): a node
  whose raft `bind` IP is specific refuses to start if any configured
  peer is not an IP literal or is of the other address family, naming the
  bind and the peer. Its control plane could never dial that peer, and
  the operator should learn it at boot rather than from logs. It goes in
  `melin_raft::spawn`'s config validation, beside the self-peer and
  duplicate-id checks, so library users get it too. One pure helper over
  `(bind IP, peer address)` serves both this check and the connect path
  (no second copy of the rule); the connect-path check stays because
  after first boot the stored membership, not the CLI list, is dialled.
  CHANGELOG **Changed**.
- Production effect: on a multi-homed host a node bound to one interface
  now sends its raft traffic from that interface too, matching what peers
  see in their firewall rules. A node whose raft listener is bound to a
  loopback IP can still only reach peers on the same host, which was
  already true (remote peers could not reach its listener). CHANGELOG
  **Changed**, and one sentence in the raft section of
  `docs/replication.md` (operator-facing: "a node sends its control-plane
  traffic from the address its control-plane listener is bound to").
- Tests (`crates/core/raft/src/network.rs` or `tests/election.rs`): a
  listener on 127.0.0.3 sees the dialer's source IP as 127.0.0.2 when the
  dialer's bind is 127.0.0.2, and as some local IP when the bind is
  unspecified; the helper per combination (v4/v4, v6/v6, unspecified with
  either pass; v4/v6, v6/v4 and a hostname with a specific bind refuse);
  `to_rpc_error` maps each of the new refusals and `AddrNotAvailable` to
  `Unreachable`; `spawn` refuses a mismatched configuration with the
  bind and the peer in the message.

**1b — the harness.**

- `crates/core/server-runtime/tests/common/proxy.rs`:
  - `Proxy::start_on(ip, upstream, stalled)`, with `Proxy::start(upstream)`
    becoming `start_on(melin_test_node::local_ip(), upstream, |_| false)`
    (open-questions Q8). `stalled` is a predicate over the connection's
    source IP: a stalled source is treated like `freeze`, per connection
    (stall, do not RST), so a blocked pair looks like a silent partition.
    The relay checks it on each pass, as it checks `mode`.
  - The file carries no dead-code allowance, so every item must be used
    by both includers (`degraded_acks.rs`, `raft_failover.rs`). That is
    why the block list is a predicate argument rather than
    `block_from`/`unblock_all` methods: the list, its two operations and
    its collection comment live in `raft_failover.rs`, its only user (a
    `Mutex<Vec<IpAddr>>`: a handful of entries, read per relay pass, never
    on a hot path). `start` delegating to `start_on` keeps `start_on` used
    by `degraded_acks.rs`.
  - An accepted connection whose source is stalled (blocked, or the proxy
    frozen) is held without relay threads: the socket goes into a
    `Mutex<Vec<TcpStream>>`, pushed once per accepted attempt and drained
    by the next `cut`, never iterated on a relay path; still silent (no
    read, no RST until then). Without it every dial attempt during an
    isolation costs two threads (open-questions Q9). A `restore` without
    a `cut` leaves held sockets held; neither includer relies on it
    (`degraded_acks.rs` cuts before it restores), and the doc on
    `restore` says so.
  - Update the header to name `raft_failover.rs` and the source predicate.
- `crates/core/server-runtime/tests/raft_failover.rs`:
  - `#[path = "common/proxy.rs"] mod proxy;` (as `degraded_acks.rs` does).
  - One header sentence: the partition tests assume Linux's routing of
    127.0.0.0/8 to `lo`; the crate builds on Linux only (io_uring), so
    there is no fallback (Q13).
  - Extend `Cluster` with an opt-in `Cluster::partitionable()`, sharing one
    constructor body with `new()` (no copy). Indices are `Cluster`'s
    (index 0 is the primary, raft id 1). Node *i* binds its raft listener
    on its own loopback IP `raft_ip(i) = 127.0.0.(i+2)`, so its outgoing
    connections carry that source IP (1a). No node takes 127.0.0.1, the
    source an unbound loopback socket gets, so under a regression of 1a
    no block matches and only the `freeze` on a node's own proxy bites.
    What catches that regression is the 1a unit tests on the observed
    source IP, plus step 1c's assertion that the primary and `O` elect a
    new raft leader while `L` is isolated: with the blocks ineffective,
    `L`'s outbound links stay up, it keeps its quorum, and that assertion
    fails (Q8). The §8 characterisation below asserts only an absence and
    so cannot detect it on its own. One
    `Proxy::start_on(127.0.0.1, …)` sits in front of every node's raft
    listener, so raft never touches the DPDK runner's client-facing
    address, and the **proxy** addresses go into `peers`. One replication
    `Proxy::start` per replica, used as that replica's `replica_of`.
  - `partition(a, b)`: block `raft_ip(b)` at node *a*'s raft proxy and
    `raft_ip(a)` at node *b*'s, which severs both directed connections of
    the pair and nothing else.
  - A `start_replicas_first()` bring-up. Start replicas 1 and 2, wait until
    one of them reports `melin_raft_is_leader 1`, then start the primary.
    Neither replica can promote meanwhile (genesis rule). This puts raft
    leadership on a **replica** deterministically. A normal bring-up does
    not: raft id 1 (the primary) has the shortest tick period and usually
    wins the first election. Assert the leader's id and term are unchanged
    once the primary has joined (Q10, decided: this assertion settles it),
    and return the leading replica's index.
  - `isolate(i)`: `freeze` node *i*'s raft proxy (nothing reaches *i*),
    block `raft_ip(i)` at both other raft proxies (*i* reaches nobody), and
    cut replica *i*'s replication proxy. The primary ↔ other-replica raft
    and replication links stay up, so this is the **realistic** partition:
    the primary keeps serving under `disk+ram`, and the primary and the
    other replica elect a new raft leader at a higher term while *i* still
    believes it leads. This uses `freeze`, `cut` and `restore` from
    `raft_failover.rs` too, as the no-dead-code rule needs.
  - `isolate_from_primary(i)`: `partition(i, primary)` and cut *i*'s
    replication proxy, leaving *i* ↔ other-replica up. The one-sided
    partition of §8.
  - `heal()`, in this order (Q9): `cut` every **raft** proxy, then clear
    the block list, then `restore` the raft proxies, then `restore` `L`'s
    replication proxy (already cut by `isolate`, so it holds nothing). The
    cut comes first so that a relay stalled for a blocked source exits on
    the generation change instead of resuming when the list empties and
    flushing its buffered frames. The other replica's replication proxy,
    which `isolate` never touched, is left alone: cutting it would drop
    the primary's in-memory replica under `disk+ram` exactly while step 1c
    checks that the primary keeps serving. The cut closes every held or
    stalled raft relay, so no stale raft frame is flushed into a healed
    link, matching what the raft clients already did (they dropped those
    connections on the RPC timeout). It also closes the healthy raft
    pair's relays once; they reconnect on the next heartbeat, well inside
    an election timeout.
  - `relay` in `proxy.rs` re-checks the generation after a read and
    before the write, dropping the chunk and closing the pair if it
    moved. Without it, a held relay that passed its generation check just
    before `heal()`'s cut would next see the block list cleared and the
    proxy restored, read the frames the kernel buffered during the
    isolation, and forward them into the healed link: the "no stale raft
    frame is flushed" claim above would not hold, and the step 1c heal
    phase would run with isolation-era frames (`L`'s old-term RPCs,
    replies carrying a pre-heal tip) arriving on some runs and not
    others. openraft refuses the RPCs by vote and the stale tip lasts
    about one heartbeat (Q9), so this keeps the heal deterministic rather
    than guarding a correctness property. One comparison buys it. A
    stall on `CUT` (not only on `FROZEN`) is not added: a relay held for a
    blocked source does not read while the cut runs (the block list is
    cleared only after it), and a forwarding relay reading in that
    instant carries live bytes, not buffered ones. `degraded_acks.rs`
    keeps its behaviour (a cut link carries nothing either way).
- New tests, green before and after the fix:
  - `replica_leader_promotes_when_the_primary_dies`. Replicas-first
    bring-up, stop the primary, `wait_one_promoted()`. This is the case
    where leadership is held *through* the failure, which the existing
    `killed_primary_triggers_exactly_one_auto_promotion` does not
    guarantee. Run it repeatedly before committing (Q10).
  - `a_replica_leader_cut_from_the_primary_alone_does_not_promote_while_writes_flow`
    (§8 characterisation). Replicas-first bring-up, keep writing to the
    primary through a `melin_client::Connection`, `isolate_from_primary(L)`.
    Over `NO_PROMOTION_WATCH`, `L` never serves and the primary keeps
    serving: the other replica's tip moves ahead of `L`'s and the
    peer-tip veto refuses.
- Run the whole file, then commit.

### Step 1c: reproduce the bug (not committed on its own)

Write `an_isolated_replica_leader_never_deposes_a_healthy_primary` in
`raft_failover.rs`:

1. `Cluster::partitionable()`, primary under `disk+ram`, replicas-first
   bring-up. Remember leader index `L` and the other replica `O`.
   `wait_formed`.
2. `isolate(L)`. While isolated, keep writing through a
   `melin_client::Connection` to the primary (`increment_request`), so the
   primary's journal moves past `L`'s. Over `NO_PROMOTION_WATCH` (reuse
   the existing constant), **record** rather than assert whether `L`
   served clients (`serves_clients`), and whether the primary and `O`
   elected a new raft leader at a higher term than `L`'s (their raft link
   is up and `L`'s heartbeats no longer reach them). Recording lets one
   red run show every consequence.
3. `heal()`. Over a heal window (e.g. 15 s), record:
   `primary.is_finished()` / `primary.shutdown_requested()` (fenced), the
   primary still serving, the primary's `melin_replicas_connected 2`, the
   primary's `melin_replica_divergence_total` (exported on the primary,
   Q6), and whether any sibling of `L`'s journal matches an archive form,
   `<journal file name>.divergent.<n>` or `<journal file name>.resync.<n>`
   (`archive_dir` and `ArchiveReason::tag` in
   `crates/core/transport-core/src/replication/archive.rs`). Not a bare
   prefix: rotated segments and snapshot files share the journal stem.
   The tag list is a copy of what the private `ArchiveReason::tag` owns;
   a comment beside it says so and why (Q6).
4. Assert all of it at the end in one message, which also carries
   `cluster_summary()`: the primary and `O` elected a new raft leader at
   a higher term while `L` was isolated (the §1 premise, and the proof
   that `isolate` took effect: without it every other assertion can hold
   because `L` was never cut off), `L` never served, the primary never
   fenced and still serving, `L` back as a replica (2 connected), no
   divergence, no archive.

Run it on current code. **Expected red**: `L` served, and the primary
fenced on heal. Paste the observed record into the step 3 commit body (one
line) and into the PR. If the primary is *not* fenced on heal, the inferred
consequence in §1 is wrong. Rewrite §1 and the CHANGELOG wording to what
was observed before going on. Given the re-election (which the red run
must also show, or the harness is at fault, not the code), the fix stands
on `L` never serving alone.
Divergence and archiving can only fire after the fix (in the red run
nobody points `L` back at the old lineage); their absence then shows `L`
rejoined cleanly.

### Step 2: `feat(raft): publish the leader's quorum-ack age`

- `crates/core/raft/src/driver.rs`:
  - Add the step 2 constants of §5 and the liveness assertion, with doc
    comments that cite the openraft functions they rely on.
  - Extract `fn heartbeat_interval_ms(node_id: NodeId) -> u64` from
    `driver_main` (used there; no copy of the jitter formula).
  - Extract `fn publish_metrics(status: &RaftStatus, m: &RaftMetrics<NodeId, Node>)`
    from the bridge loop (term, leader, role, quorum-ack age), so it is
    unit-testable without tokio.
  - Re-export `QUORUM_ACK_MAX_AGE` from `lib.rs`.
- `crates/core/transport-core/src/health.rs`, `RaftStatus`:
  - A new **private** field `millis_since_quorum_ack: AtomicU64`
    (an atomic like its siblings: written by the bridge, read by the
    poller and the health thread, never locked). Sentinel `u64::MAX`
    means none; **never 0**, because 0 means "acked just now".
  - `pub fn set_millis_since_quorum_ack(&self, v: Option<u64>)`.
  - `pub fn millis_since_quorum_ack(&self) -> Option<u64>`, which reads
    the sentinel back as `None`. `Some(u64::MAX)` also reads as `None`;
    both refuse, so the collision is harmless (say so in the comment).
  - The sentinel lives only behind these two methods, so no caller can
    misread it.
  - `new` initialises to the sentinel, and `mark_stopped` resets to it (a
    dead driver must not freeze a fresh age).
- Health exposure, following the `melin_ack_policy_journaled` pattern.
  The gauge is for operators: it is the one signal that a raft leader has
  lost its majority (openraft keeps the leader role), worth an alert on
  its own and the evidence behind the filter's refusal.
  - `RaftSnapshot.quorum_ack_age_ms: Option<u64>`.
  - In the raft block of `HealthSnapshot::write_prometheus`, emit
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
  `EngineConfig::new`) and must be re-derived on any upgrade (0.10
  changes elections). Step 4 extends the comment with
  `LEADER_CONFIRM_TIMEOUT` and `get_read_log_id`'s behaviour
  (`handle_check_is_leader_request`) when it adds them. The premise is pinned
  behaviourally rather than by version: `Cargo.lock` holds 0.9.25 for our
  own builds, and
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
    ids 0..`HEARTBEAT_JITTER_MS`: the offsets are pairwise distinct (the
    bijection the existing comment claims) and the max equals
    `HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS - 1`, so
    `MAX_TICK_PERIOD_MS` is checked against the real formula, not
    restated.
  - `crates/core/raft/src/tests/election.rs`, using its `Cluster`,
    `await_single_leader` and `stop_node`:
    - `a_healthy_leaders_quorum_ack_age_stays_under_the_gate`: sample the
      leader's status every 50 ms for 3 s and assert the max ≤
      `QUORUM_ACK_MAX_AGE`. This pins the liveness assertion empirically.
    - `a_leader_that_loses_both_peers_keeps_leading_and_its_ack_ages`:
      stop both followers and wait `QUORUM_ACK_MAX_AGE` + 1 s. Assert the
      leader still reports `ROLE_LEADER` (pins the no-check-quorum
      premise; if it ever fails, openraft changed and this whole design
      must be revisited) and that its age exceeds the gate. Port range:
      the file's own 15000..20000.
- CHANGELOG **Added**: the `melin_raft_quorum_ack_age_ms` gauge, and
  `QUORUM_ACK_MAX_AGE` exported from `melin-raft` (step 3's filter in
  `melin-server-runtime` reads it). **Changed**: `RaftStatus` gains a field (breaks struct literals; build
  it with `RaftStatus::new`).
- `docs/replication.md` "Observability": add the gauge to the
  `melin_raft_*` list.

### Step 3: `fix(server-runtime): refuse auto-promotion without a majority-acknowledged lease`

- `raft_promotion.rs`:
  - A pure `fn leader_lease_check(quorum_ack_age: Option<Duration>) -> Result<(), &'static str>`:
    `None` refuses, `Some(age)` with `age > QUORUM_ACK_MAX_AGE` refuses,
    and `age <= QUORUM_ACK_MAX_AGE` passes. It has one refusal string, a
    named const like `TIP_NOT_READY`:
    "a majority of the control plane has not acknowledged this node's
    leadership recently — it may be partitioned, and a
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
    Add a bullet to the doc list on the function, saying it is an early
    filter and the mint-time confirmation is the guarantee.
  - `consider_auto_promotion` fills it from
    `status.millis_since_quorum_ack().map(Duration::from_millis)`, read
    once, with the `Relaxed` loads kept from today's bridge. No term
    double-read and no stronger ordering (Q3): a torn read can at worst
    let the filter pass for one poll or file a term the node no longer
    leads in, and the mint-time confirmation checks the filed term
    exactly and refuses. A double-read would catch only a store landing
    between two loads nanoseconds apart, which guards nothing the
    confirmation does not.
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
    the filter both peers silent cannot coexist with a fresh lease in
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
  - Step 1c's test, now green.
  - New `a_replica_leader_alone_never_auto_promotes`: replicas-first
    bring-up, then stop the primary **and** the other replica. Over
    `NO_PROMOTION_WATCH` the leader never serves. Red on current code
    too: this is the behaviour change, pinned.
  - Step 1b's tests and `killed_primary_triggers_exactly_one_auto_promotion`
    must stay green: the filter must not cost a genuine failover.
- `docs/replication.md`, "Automatic failover", refusal list. New bullet:
  the elected replica refuses when a majority of the control plane has
  not acknowledged its leadership recently. A partitioned node can still
  believe it leads, and promoting it would depose a healthy primary that
  the rest of the cluster still sees. This **includes the case where both
  other nodes are dead**: a lone survivor never auto-promotes, and the
  operator decides with a manual `PROMOTE`. Also note the cost: failover
  waits until a majority has acknowledged the new leader, one
  control-plane round trip after its election (§8 has the derivation).
- `docs/replication.md` "Cluster recovery", "Primary crashes, one or both
  replicas alive": add that with the primary and the other replica both
  gone, recovery is manual.
- CHANGELOG **Fixed**: an isolated replica leader no longer deposes a
  healthy primary (wording per step 1c's observation). **Changed**: a
  replica that cannot reach a majority, including the last survivor, no
  longer auto-promotes.

### Step 4: `fix(server-runtime): confirm leadership with a quorum round trip when the promotion epoch is minted`

Decided with the user: a real quorum confirmation at mint time, and on
failure the node exits with an error. Rejected: waiting a bounded time for
the lease to return (little gain, more code), rolling back to replica mode
in place (undoing the pipeline handover is complicated and risky), and
committing the promotion as a raft log entry (strongest, but a
control-plane redesign; recorded as a possible follow-up).

- `crates/core/raft` — a blocking leadership confirmation the server
  thread can call without touching tokio:
  - Add `LEADER_CONFIRM_TIMEOUT`, `SCHEDULING_SLACK` and the mint-time
    safety assertion (§5), and extend step 2's comment beside the
    openraft requirement in `crates/core/raft/Cargo.toml`:
    `LEADER_CONFIRM_TIMEOUT` relies on `get_read_log_id`'s behaviour
    in 0.9.25 (`handle_check_is_leader_request`) and is re-derived on
    upgrade with the rest.
  - `RaftHandles` gains `leader_confirm: LeaderConfirm`, a cloneable
    handle wrapping a `tokio::sync::mpsc::UnboundedSender<ConfirmRequest>`.
    `UnboundedSender::send` is synchronous and needs no runtime, so tokio
    stays inside melin-raft's driver thread (CLAUDE.md). Comment the
    channel choice: unbounded because at most one promotion is ever in
    flight, so it cannot grow.
  - `pub fn LeaderConfirm::confirm(&self, term: u64) -> Result<(), LeaderConfirmError>`:
    take `deadline = Instant::now() + LEADER_CONFIRM_TIMEOUT` **before**
    sending (Q2), send `ConfirmRequest { term, reply }` with `reply` a
    `std::sync::mpsc::SyncSender` of capacity 1, and wait with
    `recv_timeout(deadline.saturating_duration_since(Instant::now()))`.
    This one deadline is the whole time bound: the driver applies no
    timeout of its own (openraft already bounds each voter by one
    `heartbeat_interval`, and a second, shorter timer would only duplicate
    this one). `LeaderConfirmError` is an exhaustive enum (no
    `#[non_exhaustive]`): `NotLeader` (openraft's `ForwardToLeader`),
    `TermMoved { now: u64 }` (the confirmed term is not the requested
    one), `NoQuorum` (`QuorumNotEnough`), `TimedOut` (the deadline passed),
    `DriverStopped` (the send failed, the reply sender was dropped, or
    openraft returned `Fatal`). The caller treats every variant alike;
    they exist for the log line and so tests can pin which path ran.
  - The driver serves the receiver from a task of its own, spawned next
    to the RPC server task, so the bridge loop is unchanged and never
    waits on a confirmation. Per request: `raft.get_read_log_id()`, then
    `Ok((read_log_id, _))` passes only if
    `read_log_id.map(|l| l.leader_id.term) == Some(term)` (Q11; `None` maps
    to `NotLeader`, with a comment that it is unreachable by that
    reading); a different term maps to `TermMoved`. No pre-check from
    metrics: a follower's `get_read_log_id` answers `ForwardToLeader` at
    once without a round trip, and a stale term is caught exactly by the
    result. A reply whose receiver has gone (the server gave up) is
    dropped with a comment saying so.
  - Re-export `LeaderConfirm` and `LeaderConfirmError` from `lib.rs`.
    `crates/core/server-runtime/src/raft.rs`: `RaftDriverGuard` hands the
    handle out like `status()`.
- `crates/core/server-runtime/src/server.rs`, `journal_promotion_epoch_bump`:
  - Takes `origin: PromotionOrigin` and
    `confirm: impl FnOnce(u64) -> Result<(), LeaderConfirmError>`, and
    returns `Result<(), LeaderConfirmError>`. A closure rather than a
    trait: there is one production closure, built by one helper that both
    call sites share, and one test stub.
  - For `Election { term }`, call `confirm(term)` directly before
    `input_producer.publish(...)`, after the function's existing alignment
    `warn!` and `info!("promotion: injecting epoch bump")`, so nothing
    blocking (a log write can block on a full stderr pipe) sits between
    the reply and the publish and `SCHEDULING_SLACK` covers only the
    publish. For an election the `info!` is reworded to announce the
    confirmation (`"promotion: confirming leadership before the epoch
    bump"`), so a refusal's log does not read as a bump that happened.
  - On refusal, publish nothing, `warn!` with the error, the term and the
    epoch (a degraded condition, not a malfunction, per the log-level
    rules) and return `Err`.
  - `Manual` skips the confirmation: an operator's `PROMOTE` remains
    available during a control-plane outage (documented).
  - `run_as_primary` and `run_as_primary_dpdk` take the
    `Option<LeaderConfirm>` beside `raft_status` and get the closure from
    one helper beside `journal_promotion_epoch_bump`,
    `fn leader_confirmation(handle: Option<&LeaderConfirm>) -> impl FnOnce(u64) -> Result<(), LeaderConfirmError> + '_`,
    so the no-handle arm is written once, not once per transport: with no
    handle the closure returns `DriverStopped`. The type cannot rule that
    pairing out: the origin crosses `PromotionRequest`'s atomic from
    another thread, so it cannot carry the handle, and only the raft
    poller files elections. One refusing arm is the cheapest honest
    answer.
- Both call sites `?` the result, wrapping it as "automatic promotion
  abandoned: …". `PipelineHandles` drop joins the spawned stages
  (verified, §3). The node exits with an error, its journal untouched by
  the tenure (the bump is the tenure's first entry, §3), and must be
  restarted by its supervisor, after which it rejoins as a replica of its
  configured primary. This follows the `check_promotable` precedent ("a
  promotion is refused, and the node exits with an error").
- Tests:
  - `crates/core/raft/src/tests/election.rs`: the leader's `confirm(term)`
    passes; a follower's returns `NotLeader`; the leader asked for an
    older term returns `TermMoved`; after stopping both followers the
    leader's returns exactly `NoQuorum` (refused connects involve no
    timer, Q12) within `LEADER_CONFIRM_TIMEOUT`; after driver shutdown,
    `DriverStopped`.
  - `driver.rs` unit test (crate-internal, no public seam): a
    `LeaderConfirm` over a channel whose receiver a test thread holds and
    answers just past the deadline returns `TimedOut`. This pins that a
    reply after the deadline is refused. It cannot pin that the deadline
    is taken before the send: `UnboundedSender::send` is a non-blocking
    enqueue, so the two orders differ by nanoseconds no test observes.
    The order is enforced by `confirm` taking `deadline` as its first
    statement, with a comment there citing §4 (listed in §10).
  - `server.rs` tests, with the ring fixture hoisted out of
    `primary_gate_policy_tests::matching_stage` rather than copied (Q5):
    an `Election` origin and a refusing stub publishes nothing
    (`try_consume` is `None`, the fence epoch is unchanged) and returns
    `Err`; an accepting stub publishes as before (pass a raised
    `shutdown` flag, or a thread that consumes and calls
    `FenceState::observe_epoch`, so the wait returns); `Manual` publishes
    without calling the stub; `leader_confirmation(None)` returns
    `DriverStopped` for any term.
- `docs/replication.md`, after the refusal list: before taking over, the
  promoting node confirms with a majority of the control plane that it
  still leads. If it cannot, the promotion is abandoned and the node exits
  with an error; its journal is unchanged, and once restarted (by its
  supervisor) it rejoins as a replica.
- CHANGELOG **Changed**: an automatic promotion is abandoned, and the node
  exits, if a majority does not confirm its leadership just before it
  takes over. **Changed**: `melin-raft`'s `RaftHandles` gains a
  `leader_confirm` field (breaks struct literals and exhaustive
  destructuring; it is in the crate's public exports). **Added**:
  `LeaderConfirm` and `LeaderConfirmError` exported from `melin-raft`.
  `LEADER_CONFIRM_TIMEOUT` stays crate-private: nothing outside the
  driver reads it.

### Step 5: `docs: record the quorum-lease gate for contributors`

- `docs/internal/raft-control-plane.md`, "Auto-promotion policy": the
  §4 argument (the lease, the challenger-nudge case and the peer-tip veto
  plus epoch fencing that cover it, the mint-time confirmation as the
  safety mechanism, the decision-time filter and what it buys), the §5
  constants and assertions, the openraft-version dependency (re-derive on
  upgrade), and the source-bound raft dialing that makes per-pair
  partition tests possible.
- Delete this plan file and the open-questions file, or reduce them to a
  pointer if the team keeps plans.

## 7. Open questions: where each landed

The options are analysed in the open-questions document; this is where
each answer went.

- **Q1 Threshold (decided: 1000 ms).** Re-measure with the step 2
  election test under `cargo nextest` load. If the healthy max gets near
  1000, revisit with the user.
- **Q2 Bridge latency (resolved).** The nudge cannot stall the bridge; no
  loop change. The confirmation's deadline is taken before the send
  (step 4).
- **Q3 Torn reads (resolved).** `Relaxed`, read once; no double-read
  (step 3).
- **Q4 Mint refusal policy (decided).** Quorum confirmation, exit on
  failure (step 4). A restart-after-abandon integration test is not
  added; see §10.
- **Q5 Testing `journal_promotion_epoch_bump` (resolved).** Unit tests
  with the hoisted ring fixture (step 4).
- **Q6 Post-heal evidence (resolved).** Divergence counter on the
  primary; archive scan over both tags (step 1c).
- **Q7 Public API (resolved).** No CHANGELOG entry; test callers listed
  (step 0).
- **Q8 DPDK (adopted, one CI run to confirm).** `Proxy::start_on`, raft
  proxies on 127.0.0.1, `raft_ip(i) = 127.0.0.(i+2)` (step 1b). Run the
  "Runtime suites on DPDK" CI step. If only the partition tests fail
  there, compare with the raft proxies on `local_ip()` before gating them
  to kernel TCP, and record why in the file header.
- **Q9 Frozen raft links on heal (resolved).** On heal, cut the raft
  proxies, then clear the block list, then restore (that order, so no
  stalled relay resumes first); blocked accepts held without threads; the
  relay re-checks the generation before writing (step 1b). No experiment needed,
  since unblock-only is not used.
- **Q10 Late primary bring-up (decided).** Settled by the step 1b
  assertion and repeated runs before committing 1b. The fallbacks
  (open-questions Q10) are deferred until that test says they are
  needed; one of them touches `RaftConfig`'s public API and would need the
  user.
- **Q11 Term of `read_log_id` (resolved).** Compare
  `read_log_id.leader_id.term` (step 4).
- **Q12 Confirmation latency without a quorum (resolved).** Exactly
  `NoQuorum` for stopped peers (step 4). The plan adds no confirmation
  test through a frozen link; if one is ever added, it asserts the set
  `{NoQuorum, TimedOut}` plus the time bound.
- **Q13 Non-Linux hosts (resolved).** No fallback; one header sentence
  (step 1b).
- **Q14 Bind-before-connect edge cases (decided).** Clear refusals mapped
  to `Unreachable`, and the startup refusal in `melin_raft::spawn`
  (step 1a).

## 8. Behaviour change and residual risk

- **Changed:** a replica that cannot reach a majority never
  auto-promotes, including a lone survivor whose two peers are dead.
  Before, it promoted after the grace. Recovery there is a manual
  `PROMOTE`. That is the correct trade for a durability-critical system:
  a lone node cannot tell "both others dead" from "I am partitioned".
- **Cost:** a new leader promotes only after a majority has acknowledged
  it: one control-plane round trip after its election, plus at most one
  promotion poll. openraft replicates the new term's blank entry as soon
  as leadership is established, `runtime_loop` calls `flush_metrics` on
  every iteration, so the ack's `millis_since_quorum_ack` is published at
  once, and the bridge wakes on `metrics_rx.changed()`. That is inside
  the time the peer-tip veto already waits for `Unknown` peers. (Step 3's
  `docs/replication.md` bullet states the same cost for operators.)
- **Cost at mint time:** one quorum round trip before the epoch bump, on
  fresh connections with an Ed25519 handshake each (openraft opens a new
  client per voter), so a few round trips on a LAN rather than one, on a
  path that already drains and prefaults.
- **Changed at mint time:** an abandoned promotion exits the process; it
  relies on a supervisor to restart the node (documented in step 4).
- **Residual liveness risk: a promotion crash loop.** The decision-time
  filter reads heartbeat acks on raft's long-lived connections; the
  mint-time confirmation goes through `get_read_log_id`, which opens a
  fresh connection per voter (connect plus Ed25519 handshake) within one
  `heartbeat_interval`. Anything that breaks new connections but not
  established ones (a firewall admitting only established flows,
  ephemeral-port exhaustion, the raft bind IP removed while old sockets
  live on) makes the filter pass and the confirmation fail every time:
  the node promotes, abandons, exits, restarts, and may win the election
  again. Acked data is never at risk, but failover stalls, and the logs
  show repeated "automatic promotion abandoned" lines with `NoQuorum` or
  `TimedOut`. A cause local to one node resolves itself (the others elect
  a different leader, which fails over); a cluster-wide one blocks
  automatic failover until fixed, leaving a manual `PROMOTE`. No
  mechanism is added for it: the genuine-failover tests (step 1b's
  `replica_leader_promotes_when_the_primary_dies` and
  `killed_primary_triggers_exactly_one_auto_promotion`) run the real
  confirmation end to end, so a spurious `NoQuorum` under CI load shows
  up there first. If it does, revisit (for example, a confirmation that
  reuses the replication streams' connections) rather than widen the
  timeout past the mint-time safety assertion.
- **Not addressed (follow-up):** a *one-sided* partition where the leader
  replica L loses only its link to the primary while the other replica O
  still reaches both. L has a fresh lease (O acks it), so the filter and
  the mint-time confirmation both pass. Whether L then deposes the
  primary depends on traffic, by reading the code:
  - if the primary takes writes during the grace, O's tip moves ahead of
    L's and the peer-tip veto refuses, for as long as O stays ahead. O
    does not campaign, because the challenger nudge needs O's own primary
    link down (`challenger_should_campaign`), so nobody promotes and the
    primary keeps serving. No harm; pinned by step 1b's
    characterisation test;
  - if the primary is idle, the tips stay equal, nothing vetoes, and L
    promotes and deposes a primary the rest of the cluster can see.
  That is the documented one-sided-link limitation of
  `PRIMARY_DOWN_GRACE`. Closing it needs O's primary-link state carried
  in the raft envelope (a replica refuses to promote while a fresh peer
  reports the primary up), a separate design. The idle case is recorded
  in §10 until the follow-up lands.
- **Not addressed (follow-up): stale peer tips.** `PeerTips::record_at`
  overwrites unconditionally, so an old frame arriving after a newer one
  on another connection to the same peer regresses the stored tip and
  looks fresh. If that makes the faster replica look level with the
  slower one when the slower one decides, the peer-tip veto passes and
  acknowledged writes can be lost. The reply path is bounded by the RPC
  timeout; the request path is not. Unlikely, but the outcome is the worst
  class, so it gets its own branch, **after everything in this plan**
  (decided with the user). The sender stamps every envelope with a boot
  identifier and a per-frame counter, the counter taken in the same
  critical section as the tip read so that counter order is tip-read
  order (decided). The receiver drops anything older than the newest it
  has seen, and a dropped frame does not refresh the peer's last-heard
  time. The boot identifier is a kernel boot stamp, not persisted
  (decided): the host's `/proc/sys/kernel/random/boot_id` plus the
  process start time from `CLOCK_BOOTTIME`. Within one host boot, process
  starts are strictly ordered (`CLOCK_BOOTTIME` is monotonic and not
  stepped by NTP), so a stale frame from an earlier process is dropped;
  across host boots, a different boot id replaces the stored one (the old
  kernel is gone, so no old-boot frame can still be in flight). Rejected:
  a persisted counter, because losing or rewinding that state makes
  peers drop every frame from the node, it looks `Silent`, the veto skips
  it, and the bug returns; a random nonce, because nonces are unordered
  (a stale frame after a restart regresses the tip for a heartbeat, and a
  "retire the old nonce" rule can drop a live process for good). The
  helper lives in `melin-transport-core` (which already depends on
  `libc`); an unreadable `boot_id` refuses startup rather than falling
  back. A VM snapshot restore rewinds clocks, memory and disk together;
  that is a hazard shared with the journal, for the operator docs, not
  this design.
  The release introducing the raft protocol version must be guaranteed
  undecodable by today's `RpcFrame` decoder, pinned by a test against a
  frozen copy of it.
- **Prerequisite of both follow-ups: a raft protocol version.** Neither
  `RpcFrame` (`wire.rs`) nor the raft handshake (`auth.rs`, which reuses
  only the replication challenge/response frames) carries a version, so a
  raft envelope change between releases is undiagnosable, and a postcard
  layout change could be misdecoded rather than refused. The follow-up
  branch first adds a `RAFT_PROTOCOL_VERSION` exchanged right after
  authentication, refusing a mismatch with an error naming both versions
  (the replication handshake's pattern), then changes the envelope
  (decided with the user).

## 9. Coverage

After step 4, run
`cargo llvm-cov nextest -p melin-raft -p melin-transport-core -p melin-server-runtime --html`.
Cover every changed branch:

- the source-bound and unbound connect paths in `RaftClient::ensure_connected`,
  the shared bind/peer helper (each refusal), and the new `to_rpc_error`
  arms;
- the startup refusal in `melin_raft::spawn`;
- the proxy's source predicate and held accepts;
- `leader_lease_check` (three arms);
- `LeaderConfirm::confirm` and the driver's confirmation task (each
  `LeaderConfirmError` variant and the pass);
- `journal_promotion_epoch_bump` per origin and outcome, and the
  no-handle arm of `leader_confirmation`;
- the sentinel accessors;
- `publish_metrics` and `heartbeat_interval_ms`;
- the gauge's present and absent branches;
- `PromotionOrigin` encoding.

Any changed line still unexecuted goes into §10 with a reason.

## 10. Left untested, and why

- **The decision-to-mint race itself** (majority lost after a passing
  decision but before the mint). Producing it end to end needs isolation
  timed inside the drain/prefault window. It is covered by the
  `election.rs` confirmation tests (a real quorum loss makes `confirm`
  fail) and the `journal_promotion_epoch_bump` unit test with a refusing
  stub, not by an integration test.
- **Restarting an abandoned node** (open-questions Q4). For the same
  reason no integration test can produce an abandoned promotion on
  demand. That its journal is untouched follows from the unit test (no
  slot published) and the bump being the tenure's first entry; the
  rejoin itself is the ordinary replica restart, which existing tests
  cover.
- **The one-sided partition with an idle primary** (§8): a known gap
  until the follow-up design; the writes-flowing case is tested.
- **A nudged follower winning while the leader was just acknowledged**
  (§4). No new test: the case is covered by mechanisms that predate this
  fix and have their own tests, the peer-tip veto (`auto_promotion_decision`
  unit tests with a `Fresh` peer ahead) and epoch fencing
  (`fence_if_superseded`). The fix changes neither.
- **The real-clock margin of the mint-time safety assertion.** It is a
  compile-time proof over the constants. Nothing runtime exercises "a
  follower granted a vote at C + lease − ε". Checking it would mean
  instrumenting openraft; the derivation is instead tied to the locked
  0.9.25 and the re-derive-on-upgrade comment beside the dependency.
- **`confirm` taking its deadline before the send.** The §4 mint-time
  argument rests on it, but sending is a non-blocking enqueue, so no test
  can tell "deadline, then send" from "send, then deadline". It is held
  by `deadline` being the first statement of `confirm`, with a comment
  citing §4, and by review; the step 4 unit test pins only that a late
  reply is refused.
- **The relay's post-read drop** (step 1b). It fires only when a heal
  lands inside one relay pass, which no test can time; it is a
  test-harness guard, not product code, and the step 1c heal phase runs
  through it without depending on hitting the window.
- **openraft's lease internals** (`handle_vote_req`, `EngineConfig::new`).
  They are not tested from melin: `Raft` handles stay inside the driver
  thread, so a follower cannot be sent a crafted `VoteRequest` without
  widening `RaftHandles` for tests only. `Cargo.lock` (0.9.25 for our
  builds), the re-derive-on-upgrade comment on the `"0.9"` requirement,
  and the no-check-quorum election test bound the risk. A downstream
  build resolving a later 0.9.x patch is not covered by any of these;
  openraft treating lease semantics as stable within 0.9 is assumed.
- **DPDK-specific partition behaviour**: only if Q8's CI run forces the
  partition tests to kernel TCP. Raft is kernel TCP everywhere, and the
  mint path is shared by both transports.
