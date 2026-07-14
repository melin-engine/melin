//! The control-plane driver: one dedicated thread owning the raft node,
//! its RPC listener, and the election-state bridge into the health gauges.
//!
//! Deliberately its own thread rather than the admin listener (whose
//! blocking single-connection loop would stall heartbeats) and its own
//! **current-thread tokio runtime** — openraft requires an async runtime,
//! and confining it here keeps the rest of the codebase synchronous and the
//! data plane untouched: nothing on the hot path calls into this module,
//! and a control-plane outage (quorum loss, storage failure) degrades
//! failover to the manual `PROMOTE` playbook while trading continues.

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
use openraft::ServerState;
use openraft::error::InitializeError;
use openraft::error::RaftError;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::network::RaftClientFactory;
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
    pub bind: std::net::SocketAddr,
    pub dir: PathBuf,
    /// All cluster members including this node — see [`RaftPeer`].
    pub peers: Vec<RaftPeer>,
}

/// Handles returned by [`spawn`].
pub struct RaftHandles {
    /// Election state for the health gauges (and, later, auto-promotion).
    pub status: Arc<RaftStatus>,
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

    // Fail startup on a bad bind/dir, not in the background.
    let listener = std::net::TcpListener::bind(config.bind)?;
    listener.set_nonblocking(true)?;
    let (log_store, state_machine) = crate::storage::open(&config.dir)
        .map_err(|e| io::Error::other(format!("raft storage open failed: {e}")))?;

    let status = Arc::new(RaftStatus::new(config.node_id));

    let thread_status = Arc::clone(&status);
    let thread_tip = Arc::clone(&tip);
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
                supersession,
                shutdown,
            ));
        })
        .map_err(|e| io::Error::other(format!("failed to spawn raft-driver thread: {e}")))?;

    Ok(RaftHandles { status, join })
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
    supersession: Option<crate::rpc_server::SupersessionPolicy>,
    shutdown: Arc<AtomicBool>,
) {
    let raft_config = Config {
        cluster_name: "melin-control-plane".to_owned(),
        heartbeat_interval: HEARTBEAT_INTERVAL_MS,
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

    let network = RaftClientFactory::new(Arc::clone(&signing_key), Arc::clone(&tip));
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
            info!(
                node_id = config.node_id,
                "control plane already initialized — using stored membership"
            );
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
    while !shutdown.load(Ordering::Relaxed) {
        // Wait for a metrics change, capped so the shutdown flag is polled
        // at the codebase's usual 100 ms listener cadence.
        let _ = tokio::time::timeout(SHUTDOWN_POLL, metrics_rx.changed()).await;
        let m = metrics_rx.borrow().clone();
        status.term.store(m.current_term, Ordering::Relaxed);
        status
            .leader_id
            .store(m.current_leader.unwrap_or(0), Ordering::Relaxed);
        status.role.store(role_of(m.state), Ordering::Relaxed);
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
            // The raft core died (e.g. persistent storage error). Trading
            // is unaffected by construction — the data plane never calls
            // into raft — but automatic failover is gone until the operator
            // intervenes, so this is a server malfunction worth an error.
            error!(error = %fatal, "raft core stopped — control plane down, trading unaffected");
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
