//! The control-plane driver: one dedicated thread owning the raft node,
//! its RPC listener, and the election-state bridge into the health gauges.
//!
//! Deliberately its own thread rather than the admin listener (whose
//! blocking single-connection loop would stall heartbeats) and its own
//! **current-thread tokio runtime** — openraft requires an async runtime,
//! and confining it here keeps the rest of the codebase synchronous and the
//! data plane untouched: nothing on the hot path calls into this module,
//! and a control-plane outage (quorum loss, storage failure) degrades
//! failover to the manual `PROMOTE` playbook while sequencing continues.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use melin_app::auth::AuthorizedKeys;
use melin_transport_core::health::RaftStatus;
use openraft::Config;
use openraft::Raft;
use openraft::RaftMetrics;
use openraft::ServerState;
use openraft::error::InitializeError;
use openraft::error::RaftError;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::network::RaftClientFactory;
use crate::network::plan_dial;
use crate::recency::TipSource;
use crate::rpc_server::RpcServerConfig;
use crate::rpc_server::serve;
use crate::types::Node;
use crate::types::NodeId;
use crate::types::TypeConfig;

/// Deliberately slow election tuning: control-plane latency is irrelevant
/// (an election outcome only ever triggers a failover decision), so
/// stability wins. 200 ms heartbeats, 1–2 s randomized election timeout.
const HEARTBEAT_INTERVAL_MS: u64 = 200;
const ELECTION_TIMEOUT_MIN_MS: u64 = 1000;
const ELECTION_TIMEOUT_MAX_MS: u64 = 2000;

/// Range of the per-node offset added to `HEARTBEAT_INTERVAL_MS`,
/// breaking a split-vote livelock openraft 0.9 is prone to: it
/// randomizes a node's election timeout **once** (at `Raft::new`, not
/// per election round the way standard raft prescribes) and fires
/// elections only on its internal tick, whose period is
/// `heartbeat_interval * 3/2` anchored at startup. Two candidates whose
/// grids share a period and whose fixed timeouts land in the same grid
/// bucket campaign on the same edge, split the vote, re-arm on the same
/// edge, and repeat indefinitely. Distinct tick periods break this
/// structurally — grids with different periods drift apart, so an
/// aligned edge cannot stay aligned — which is why the offset is
/// **derived from the node id** rather than drawn at random: a random
/// draw leaves an equal-draw residual (~1/150 per candidate pair),
/// while the derivation guarantees distinct periods for any two nodes
/// whose ids differ mod 150 — i.e. every sane deployment; ids 150 apart
/// would share a period and fall back to the same odds raft's own
/// timeout randomization provides. The range trades period spread
/// against worst-case election-trigger delay (+225 ms on the tick
/// period), comfortably inside the failover grace.
///
/// Workaround, not root fix: the 0.10 line re-randomizes the timeout per
/// election round (`do_elect` → `resample_election_timeout`) and adds
/// pre-vote, which removes the livelock at the source; the 0.9 line does
/// not (checked through 0.9.25). Drop this on migration to 0.10.
const HEARTBEAT_JITTER_MS: u64 = 150;

/// Multiplier spreading consecutive node ids across the jitter range
/// (ids 1/2/3 → offsets 37/74/111 → tick periods ~356/411/467 ms).
/// Coprime with `HEARTBEAT_JITTER_MS`, so the map `id → id * 37 mod
/// 150` is a bijection on ids 0..150: distinct offsets are guaranteed,
/// and consecutive ids sit far apart instead of one tick-quantum
/// (1 ms) from each other.
const HEARTBEAT_JITTER_SPREAD: u64 = 37;

/// openraft's follower lease: a follower whose vote is committed refuses
/// every vote request until `leader_lease` has passed since it last
/// accepted a vote-bearing RPC (`Engine::handle_vote_req`), and
/// `EngineConfig::new` sets `leader_lease` to `election_timeout_max`.
/// Read in openraft 0.9.25; re-derive on any upgrade (see the comment on
/// the dependency in this crate's manifest).
const LEADER_LEASE_MS: u64 = ELECTION_TIMEOUT_MAX_MS;

/// The auto-promotion filter's threshold: a raft leader whose last
/// majority acknowledgement is older than this does not start a
/// promotion. Half the lease. The filter is an early refusal, not the
/// safety mechanism, so the only constraint on it is liveness (the
/// assertion below): it must not refuse a healthy leader.
const QUORUM_ACK_MAX_AGE_MS: u64 = LEADER_LEASE_MS / 2;

/// The oldest majority acknowledgement the automatic-promotion policy
/// accepts from a raft leader (half the control plane's leader lease),
/// compared with the age the driver publishes in `RaftStatus`. An early
/// filter, not a safety guarantee.
pub const QUORUM_ACK_MAX_AGE: Duration = Duration::from_millis(QUORUM_ACK_MAX_AGE_MS);

/// The slowest node's raft tick period: openraft ticks every
/// `heartbeat_interval * 3 / 2` (`Raft::new`, `Tick::spawn`), a leader
/// heartbeats on every tick (`RaftCore`'s `Notify::Tick` arm) and flushes
/// its metrics at least once per runtime-loop wake-up
/// (`RaftCore::runtime_loop`), and the largest heartbeat interval is
/// [`heartbeat_interval_ms`]'s maximum.
const MAX_TICK_PERIOD_MS: u64 = (HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS - 1) * 3 / 2;

/// Allowance for one heartbeat round trip, for the liveness assertion
/// only.
const RTT_BUDGET_MS: u64 = 100;

// A healthy leader's reported quorum-ack age peaks at about one tick plus
// a round trip (the flush just before a heartbeat's reply lands), so the
// filter must allow at least that, or it refuses a healthy leader.
const _: () = assert!(
    QUORUM_ACK_MAX_AGE_MS >= MAX_TICK_PERIOD_MS + RTT_BUDGET_MS,
    "QUORUM_ACK_MAX_AGE would refuse a healthy leader: it must cover the slowest tick plus a round trip"
);

/// Shutdown-flag poll cadence in the driver loop.
const SHUTDOWN_POLL: Duration = Duration::from_millis(100);

/// One control-plane peer as configured via `--raft-peer id@addr#pubkey`.
/// The list **includes this node itself**: the self entry supplies the
/// externally dialable address written into the raft membership (the bind
/// address may be a wildcard), and identical `--raft-peer` lists across the
/// cluster keep the first-boot `initialize` membership consistent.
#[derive(Debug, Clone)]
pub struct RaftPeer {
    pub id: NodeId,
    pub addr: String,
    pub pubkey: [u8; 32],
}

/// Static control-plane configuration for one node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: NodeId,
    /// The control-plane listener. When its IP is specific (not `0.0.0.0`
    /// or `::`), outbound control-plane connections are made from that IP
    /// too, so every other member's address in [`Self::peers`] must be an
    /// IP literal of the same family: [`spawn`] refuses anything else.
    /// A wildcard bind dials as the kernel routes, and a peer address may
    /// then be a hostname.
    pub bind: std::net::SocketAddr,
    pub dir: PathBuf,
    /// All cluster members including this node — see [`RaftPeer`].
    pub peers: Vec<RaftPeer>,
}

/// Handles returned by [`spawn`].
pub struct RaftHandles {
    /// Election state for the health gauges (and, later, auto-promotion).
    pub status: Arc<RaftStatus>,
    /// Last tip heard from each peer, fed from every RPC envelope in
    /// both directions. Read by the auto-promotion policy's
    /// journal-safety check.
    pub peer_tips: Arc<crate::recency::PeerTips>,
    /// One-shot election nudge: set by the promotion policy when this
    /// node holds a higher journal tip than the current control-plane
    /// leader during a failover window; the driver observes it within
    /// one poll and calls `Raft::trigger().elect()`.
    pub elect_requested: Arc<AtomicBool>,
    /// Election stand-down: while `false`, this node starts no
    /// timeout-driven elections (openraft's `enable_elect` runtime
    /// toggle). Cleared by the promotion policy on a replica that can
    /// see a fresh peer journal tip ahead of its own — the electoral
    /// dual of the vote filter in `crate::recency`: that filter says
    /// "don't vote for someone less caught up", this says "don't stand
    /// while someone more caught up is alive" — and restored the moment
    /// the condition no longer holds. The explicit nudge
    /// (`elect_requested`) bypasses it by design.
    pub elect_enabled: Arc<AtomicBool>,
    /// The driver thread. Join on shutdown so storage I/O finishes cleanly.
    pub join: std::thread::JoinHandle<()>,
}

/// Validate config coherence and spawn the driver thread.
///
/// Binds the RPC listener and opens raft storage *synchronously* so a bad
/// `--raft-bind` or unreadable `--raft-dir` fails startup with a clear
/// error instead of a background log line.
pub fn spawn(
    config: RaftConfig,
    signing_key: Arc<SigningKey>,
    authorized_keys: Arc<AuthorizedKeys>,
    tip: Arc<TipSource>,
    supersession: Option<crate::rpc_server::SupersessionPolicy>,
    shutdown: Arc<AtomicBool>,
) -> io::Result<RaftHandles> {
    let self_peer = config
        .peers
        .iter()
        .find(|p| p.id == config.node_id)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "--raft-peer list must include this node (id {}) with its dialable address",
                    config.node_id
                ),
            )
        })?;
    if self_peer.pubkey != signing_key.verifying_key().to_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "--raft-peer entry for this node (id {}) names a different public key than \
                 the configured signing key",
                config.node_id
            ),
        ));
    }
    {
        let mut ids: Vec<NodeId> = config.peers.iter().map(|p| p.id).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != config.peers.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "duplicate node id in --raft-peer list",
            ));
        }
        if ids.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "raft node ids must be non-zero (0 is the no-leader sentinel)",
            ));
        }
    }
    // A control plane bound to a specific IP dials from it, so a peer it
    // could never reach that way is a misconfiguration to refuse at boot,
    // not to retry in the background. This node's own entry is never
    // dialled by it. The stored membership, dialled after first boot, is
    // checked again on every connect by the same rule.
    for peer in config.peers.iter().filter(|p| p.id != config.node_id) {
        plan_dial(config.bind, &peer.addr).map_err(|refusal| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("--raft-peer id {}: {refusal}", peer.id),
            )
        })?;
    }

    // Fail startup on a bad bind/dir, not in the background.
    let listener = std::net::TcpListener::bind(config.bind)?;
    listener.set_nonblocking(true)?;
    let (log_store, state_machine) = crate::storage::open(&config.dir)
        .map_err(|e| io::Error::other(format!("raft storage open failed: {e}")))?;

    let status = Arc::new(RaftStatus::new(config.node_id));
    let peer_tips = Arc::new(crate::recency::PeerTips::new());
    let elect_requested = Arc::new(AtomicBool::new(false));
    let elect_enabled = Arc::new(AtomicBool::new(true));

    let thread_status = Arc::clone(&status);
    let thread_tip = Arc::clone(&tip);
    let thread_peer_tips = Arc::clone(&peer_tips);
    let thread_elect = Arc::clone(&elect_requested);
    let thread_elect_enabled = Arc::clone(&elect_enabled);
    let join = std::thread::Builder::new()
        .name("raft-driver".into())
        .spawn(move || {
            // The driver is spawned from the unpinned main thread today, but
            // clear affinity defensively: a future caller spawning it off a
            // pinned parent must not confine consensus to a hot-path core.
            if let Err(e) = melin_app::affinity::clear_affinity() {
                warn!(error = %e, "raft-driver: failed to clear CPU affinity");
            }
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    error!(error = %e, "raft-driver: tokio runtime construction failed");
                    thread_status.mark_stopped();
                    return;
                }
            };
            runtime.block_on(driver_main(
                config,
                listener,
                log_store,
                state_machine,
                signing_key,
                authorized_keys,
                thread_status,
                thread_tip,
                thread_peer_tips,
                thread_elect,
                thread_elect_enabled,
                supersession,
                shutdown,
            ));
        })
        .map_err(|e| io::Error::other(format!("failed to spawn raft-driver thread: {e}")))?;

    Ok(RaftHandles {
        status,
        peer_tips,
        elect_requested,
        elect_enabled,
        join,
    })
}

/// Map openraft's server state onto the gauge encoding.
fn role_of(state: ServerState) -> u8 {
    match state {
        ServerState::Learner => RaftStatus::ROLE_LEARNER,
        ServerState::Follower => RaftStatus::ROLE_FOLLOWER,
        ServerState::Candidate => RaftStatus::ROLE_CANDIDATE,
        ServerState::Leader => RaftStatus::ROLE_LEADER,
        // Shutdown is reported through `mark_stopped`, not the role gauge.
        ServerState::Shutdown => RaftStatus::ROLE_FOLLOWER,
    }
}

/// This node's raft heartbeat interval: [`HEARTBEAT_INTERVAL_MS`] plus a
/// deterministic per-node offset, so any two nodes with distinct ids
/// (mod 150) get distinct tick periods by construction (see
/// [`HEARTBEAT_JITTER_MS`]). Reduces before multiplying: node ids are
/// arbitrary `u64`s and must not overflow.
fn heartbeat_interval_ms(node_id: NodeId) -> u64 {
    HEARTBEAT_INTERVAL_MS
        + (node_id % HEARTBEAT_JITTER_MS) * HEARTBEAT_JITTER_SPREAD % HEARTBEAT_JITTER_MS
}

/// Publish what the gauges and the promotion policy read from one raft
/// metrics report: term, leader (0 for none), role, and the age of the
/// last majority acknowledgement (openraft reports `None` when this node
/// is not leading or was never acknowledged).
fn publish_metrics(status: &RaftStatus, m: &RaftMetrics<NodeId, Node>) {
    status.term.store(m.current_term, Ordering::Relaxed);
    status
        .leader_id
        .store(m.current_leader.unwrap_or(0), Ordering::Relaxed);
    status.role.store(role_of(m.state), Ordering::Relaxed);
    status.set_millis_since_quorum_ack(m.millis_since_quorum_ack);
}

#[allow(clippy::too_many_arguments)] // driver assembly point; a config struct would just restate it
async fn driver_main(
    config: RaftConfig,
    listener: std::net::TcpListener,
    log_store: crate::storage::FileLogStore,
    state_machine: crate::storage::FileStateMachine,
    signing_key: Arc<SigningKey>,
    authorized_keys: Arc<AuthorizedKeys>,
    status: Arc<RaftStatus>,
    tip: Arc<TipSource>,
    peer_tips: Arc<crate::recency::PeerTips>,
    elect_requested: Arc<AtomicBool>,
    elect_enabled: Arc<AtomicBool>,
    supersession: Option<crate::rpc_server::SupersessionPolicy>,
    shutdown: Arc<AtomicBool>,
) {
    let raft_config = Config {
        cluster_name: "melin-control-plane".to_owned(),
        heartbeat_interval: heartbeat_interval_ms(config.node_id),
        election_timeout_min: ELECTION_TIMEOUT_MIN_MS,
        election_timeout_max: ELECTION_TIMEOUT_MAX_MS,
        ..Config::default()
    };
    let raft_config = match raft_config.validate() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            error!(error = %e, "raft-driver: invalid raft config");
            status.mark_stopped();
            return;
        }
    };

    let network = RaftClientFactory::new(
        Arc::clone(&signing_key),
        Arc::clone(&tip),
        Arc::clone(&peer_tips),
        config.bind,
    );
    let raft: Raft<TypeConfig> = match Raft::new(
        config.node_id,
        raft_config,
        network,
        log_store,
        state_machine,
    )
    .await
    {
        Ok(raft) => raft,
        Err(e) => {
            error!(error = %e, "raft-driver: raft core failed to start");
            status.mark_stopped();
            return;
        }
    };

    // Serve peer RPCs. The listener was bound synchronously in `spawn`.
    let tokio_listener = match tokio::net::TcpListener::from_std(listener) {
        Ok(l) => l,
        Err(e) => {
            error!(error = %e, "raft-driver: listener registration failed");
            status.mark_stopped();
            return;
        }
    };
    let peer_ids: HashMap<[u8; 32], NodeId> =
        config.peers.iter().map(|p| (p.pubkey, p.id)).collect();
    let rpc_cfg = Arc::new(RpcServerConfig {
        authorized_keys,
        peer_ids: Arc::new(peer_ids),
        tip: Arc::clone(&tip),
        peer_tips: Arc::clone(&peer_tips),
        supersession,
        vote_filter: std::sync::Mutex::new(Default::default()),
    });
    let rpc_task = tokio::spawn(serve(
        tokio_listener,
        raft.clone(),
        Arc::clone(&rpc_cfg),
        Arc::clone(&shutdown),
    ));

    // First-boot initialization with the static membership. A cluster that
    // is already initialized (any restart) refuses with `NotAllowed`, which
    // simply means the stored membership is in force — the CLI peer list is
    // then advisory only.
    let members: BTreeMap<NodeId, Node> = config
        .peers
        .iter()
        .map(|p| {
            (
                p.id,
                Node {
                    addr: p.addr.clone(),
                },
            )
        })
        .collect();
    match raft.initialize(members).await {
        Ok(()) => info!(node_id = config.node_id, "control plane initialized"),
        Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
            // Already initialized (any restart): the stored membership is
            // authoritative and the CLI `--raft-peer` list is advisory.
            // Runtime membership changes are out of scope, so if the
            // operator edited the list their change silently has no
            // effect — surface that instead of logging an unconditional
            // "using stored membership" they might read as acceptance.
            let stored = raft.metrics().borrow().membership_config.clone();
            // id → dialable address, the two things the membership pins
            // (pubkeys are enforced separately from the live CLI table,
            // so a pubkey edit does take effect and isn't flagged here).
            let stored_nodes: BTreeMap<NodeId, String> = stored
                .nodes()
                .map(|(id, node)| (*id, node.addr.clone()))
                .collect();
            let configured_nodes: BTreeMap<NodeId, String> = config
                .peers
                .iter()
                .map(|p| (p.id, p.addr.clone()))
                .collect();
            if stored_nodes == configured_nodes {
                info!(
                    node_id = config.node_id,
                    "control plane already initialized — using stored membership"
                );
            } else {
                warn!(
                    node_id = config.node_id,
                    ?configured_nodes,
                    ?stored_nodes,
                    "control plane already initialized — the --raft-peer list differs from the \
                     stored membership and is being ignored (runtime membership changes are not \
                     supported); revert the flags, or re-provision --raft-dir to apply a new set"
                );
            }
        }
        Err(e) => {
            error!(error = %e, "raft-driver: initialize failed");
            status.mark_stopped();
            let _ = raft.shutdown().await;
            return;
        }
    }

    // Bridge raft metrics into the lock-free gauge atomics until shutdown.
    let mut metrics_rx = raft.metrics();
    // Last value handed to `runtime_config().elect()`; openraft starts
    // with elections enabled.
    let mut elect_applied = true;
    while !shutdown.load(Ordering::Relaxed) {
        // Wait for a metrics change, capped so the shutdown flag is polled
        // at the codebase's usual 100 ms listener cadence.
        let _ = tokio::time::timeout(SHUTDOWN_POLL, metrics_rx.changed()).await;
        // Election nudge from the promotion policy: this node holds a
        // higher journal tip than the current leader during a failover
        // window, so campaign now instead of waiting for a timeout that
        // will never fire (the lesser leader heartbeats happily). Swap,
        // not load+store: a request landing between the two must not be
        // lost. A failed trigger is not retried and the policy will not
        // re-nudge (it fires at most once per observed term, and a
        // trigger that never ran leaves the term where it was) — which
        // costs nothing, because `elect` only fails with `Fatal`: the
        // raft core has already stopped, so no election was going to
        // happen on this node regardless.
        if elect_requested.swap(false, Ordering::Relaxed)
            && let Err(e) = raft.trigger().elect().await
        {
            warn!(error = %e, "requested election trigger failed");
        }
        // Election stand-down (see `RaftHandles::elect_enabled`): apply
        // the flag on change only — the toggle is a plain atomic store
        // inside openraft, but calling it every poll would be noise.
        // Logged at debug and stated as the mechanical fact it is: the
        // flag arrives here as a bare bool, so this side cannot know
        // *why* the policy asked, and the policy logs the reason itself
        // at info (see `raft_promotion::consider_election_stand_down`).
        let elect_wanted = elect_enabled.load(Ordering::Relaxed);
        if elect_wanted != elect_applied {
            raft.runtime_config().elect(elect_wanted);
            elect_applied = elect_wanted;
            debug!(
                node_id = config.node_id,
                enabled = elect_wanted,
                "applied the election stand-down flag to the raft runtime config"
            );
        }
        let m = metrics_rx.borrow().clone();
        publish_metrics(&status, &m);
        if m.current_leader.is_some() {
            // A leader exists — elections are working, so re-arm the
            // journal-tip vote filter's liveness escape. Without this a
            // node that is *itself* the leader would never re-arm (it
            // receives no appends), and drops accumulated across
            // leadership churn would eventually open the escape while
            // the cluster is healthy. See `crate::recency`.
            // Poisoning unreachable under panic=abort.
            rpc_cfg
                .vote_filter
                .lock()
                .expect("vote filter mutex poisoned")
                .leader_observed();
        }
        if let Err(fatal) = &m.running_state {
            // The raft core died (e.g. persistent storage error). Sequencing
            // is unaffected by construction — the data plane never calls
            // into raft — but automatic failover is gone until the operator
            // intervenes, so this is a server malfunction worth an error.
            error!(error = %fatal, "raft core stopped — control plane down, sequencing unaffected");
            break;
        }
    }

    let _ = raft.shutdown().await;
    status.mark_stopped();
    // The RPC accept loop exits on the same shutdown flag; when the raft
    // core died instead (fatal above), abort it — its Raft handle is dead.
    rpc_task.abort();
    let _ = rpc_task.await;
    info!(node_id = config.node_id, "raft driver stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_offsets_are_distinct_and_bounded_by_the_slowest_tick() {
        // `BTreeSet` to count distinct offsets; order only makes the
        // failure message readable.
        let intervals: std::collections::BTreeSet<u64> = (0..HEARTBEAT_JITTER_MS)
            .map(heartbeat_interval_ms)
            .collect();
        assert_eq!(
            intervals.len() as u64,
            HEARTBEAT_JITTER_MS,
            "ids 0..{HEARTBEAT_JITTER_MS} must get pairwise distinct heartbeat intervals: {intervals:?}"
        );
        let max = *intervals.last().expect("a non-empty id range");
        assert_eq!(max, HEARTBEAT_INTERVAL_MS + HEARTBEAT_JITTER_MS - 1);
        // The liveness assertion's tick bound is the real formula's.
        assert_eq!(MAX_TICK_PERIOD_MS, max * 3 / 2);
        assert_eq!(
            intervals.first().copied(),
            Some(HEARTBEAT_INTERVAL_MS),
            "the offset range starts at 0"
        );
        // Any u64 id stays in range without overflowing.
        assert!(intervals.contains(&heartbeat_interval_ms(u64::MAX)));
    }

    #[test]
    fn publish_metrics_bridges_a_leaders_quorum_ack_age() {
        let status = RaftStatus::new(1);
        let mut m = RaftMetrics::<NodeId, Node>::new_initial(1);
        m.current_term = 3;
        m.current_leader = Some(1);
        m.state = ServerState::Leader;
        m.millis_since_quorum_ack = Some(7);

        publish_metrics(&status, &m);

        assert_eq!(status.term.load(Ordering::Relaxed), 3);
        assert_eq!(status.leader_id.load(Ordering::Relaxed), 1);
        assert_eq!(status.role.load(Ordering::Relaxed), RaftStatus::ROLE_LEADER);
        assert_eq!(status.millis_since_quorum_ack(), Some(7));
    }

    #[test]
    fn publish_metrics_clears_the_age_of_a_follower() {
        // The node led before (an age is published), then follows.
        let status = RaftStatus::new(1);
        status.set_millis_since_quorum_ack(Some(7));
        let mut m = RaftMetrics::<NodeId, Node>::new_initial(1);
        m.current_term = 4;
        m.current_leader = Some(2);
        m.state = ServerState::Follower;
        m.millis_since_quorum_ack = None;

        publish_metrics(&status, &m);

        assert_eq!(status.term.load(Ordering::Relaxed), 4);
        assert_eq!(status.leader_id.load(Ordering::Relaxed), 2);
        assert_eq!(
            status.role.load(Ordering::Relaxed),
            RaftStatus::ROLE_FOLLOWER
        );
        assert_eq!(status.millis_since_quorum_ack(), None);

        // No leader known maps to the 0 sentinel.
        m.current_leader = None;
        publish_metrics(&status, &m);
        assert_eq!(status.leader_id.load(Ordering::Relaxed), 0);
    }
}
