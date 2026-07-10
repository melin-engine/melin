//! Control-plane raft driver — one thread per node owning the
//! [`melin_raft::ControlNode`], its peer connections, and the election
//! observability gauges.
//!
//! The evolution of the "reuse the admin thread" idea: the admin
//! listener's synchronous single-connection loop (100 ms accept poll,
//! 5 s blocking reads) cannot host raft timers — one slow operator
//! connection would stall heartbeats and fire spurious elections
//! cluster-wide. So the control plane gets its own thread with the
//! same *shape* as the admin/health listeners (plain `std::net`,
//! non-blocking accept, no async runtime), and raft drives the
//! existing admin machinery rather than living inside it.
//!
//! ## Connection topology
//!
//! Every node dials every peer: raft messages travel **outbound-only**
//! (node A → B messages ride the A→B connection A dialed; B's replies
//! ride B's own B→A connection). Inbound connections are read-only
//! after auth. This gives single-owner sockets with no tie-breaking
//! for simultaneous dials — at the cost of two TCP connections per
//! peer pair, irrelevant on the control plane.
//!
//! Peer links authenticate with the cluster's **replication** keys
//! (Ed25519 challenge-response, `replication` permission) — the same
//! trust domain as the replication data plane, distinct from operator
//! admin keys. Auth handshakes are blocking, so they run on short-lived
//! helper threads and deliver authenticated sockets back over a
//! channel; the driver loop itself never blocks on a peer.
//!
//! ## Timing
//!
//! The loop sleeps [`POLL_INTERVAL`] between iterations and advances
//! the raft clock every [`TICK_INTERVAL`]. With
//! [`melin_raft::node::HEARTBEAT_TICKS`] = 2 and `ELECTION_TICKS` = 10
//! that yields 200 ms heartbeats and 1–2 s election timeouts —
//! deliberately slow (see `node.rs`) and orders of magnitude above the
//! poll granularity, so scheduling jitter on the (unpinned) control
//! thread cannot fake a leader failure.

use std::collections::HashMap;
use std::io::{self, Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use tracing::{debug, error, info, warn};

use melin_app::auth::AuthorizedKeys;
use melin_raft::recency::{JournalTip, candidate_is_current, is_vote_request};
use melin_raft::wire::{FrameScan, encode_frame, scan_frame};
use melin_raft::{ControlNode, MemberRecord, Registry, StateRole};
use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;
use melin_transport_core::health::RaftStatus;

use crate::durability_policy::DurabilityMode;
use crate::promotion::PromotionRequest;
use crate::replication::auth::{authenticate_replica_identified, authenticate_with_primary};

/// Driver loop granularity. Bounds tick jitter and message latency;
/// 10 ms is 1/10 of a tick and costs nothing measurable on a control
/// thread that yields between iterations.
const POLL_INTERVAL: Duration = Duration::from_millis(10);
/// Raft logical-clock period (see module docs for the derived timings).
const TICK_INTERVAL: Duration = Duration::from_millis(100);
/// Backoff between outbound dial attempts to a down peer.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(1);
/// Dial + auth deadline for one outbound attempt.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
/// Auth deadline for one inbound connection.
const ACCEPT_AUTH_TIMEOUT: Duration = Duration::from_secs(3);
/// Cap on a peer's unflushed egress. A peer that stops reading gets its
/// connection reset (raft tolerates the message loss) instead of
/// growing an unbounded buffer.
const MAX_OUT_BUFFER: usize = 4 << 20;
/// Cap on buffered ingress from one peer before frame extraction —
/// matches the wire codec's frame cap plus one header.
const MAX_IN_BUFFER: usize = melin_raft::wire::MAX_FRAME + 8;
/// Slack multiplier over the peer count for the inbound-connection and
/// in-flight-auth caps: a healthy cluster holds exactly one inbound link
/// per peer, so 4x leaves room for reconnect overlap while still bounding
/// a flood. See [`inbound_cap`].
const INBOUND_SLACK: usize = 4;
/// Floor for the inbound/auth caps, so a tiny or misconfigured peer list
/// still tolerates a couple of concurrent reconnects.
const INBOUND_CAP_FLOOR: usize = 8;
/// Drop an inbound link that has produced no bytes for this long. A
/// connected peer sends heartbeats/appends far more often (sub-second at
/// the 200 ms heartbeat), so this only reaps half-open links left by a
/// peer that vanished without a FIN/RST — which raft's own timers never
/// close on their own.
const INBOUND_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Cadence for re-proposing this node's membership record while the
/// applied registry does not yet reflect it. Proposals are cheap but
/// can be lost to leader churn (raft gives no ack), so the announce
/// loop retries until it *sees* its record applied; a couple of
/// seconds keeps convergence prompt without spamming leaderless
/// clusters.
const ANNOUNCE_RETRY_INTERVAL: Duration = Duration::from_secs(2);
/// Overall deadline for one operator voter change. Each stage re-proposes
/// every [`ANNOUNCE_RETRY_INTERVAL`] until observed applied; after this
/// long the driver gives up and reports failure so the operator command
/// returns instead of hanging. Comfortably above a few election timeouts,
/// so a single leader churn mid-change does not trip it.
const VOTER_CHANGE_DEADLINE: Duration = Duration::from_secs(10);

/// One other cluster node, as configured on this node.
#[derive(Debug, Clone)]
pub struct RaftPeer {
    /// The peer's raft id.
    pub id: u64,
    /// The peer's raft RPC address (its `--raft-bind`).
    pub addr: SocketAddr,
    /// The peer's Ed25519 replication public key, used to pin its
    /// identity on inbound connections: a connection that authenticates
    /// with this key may only speak for node [`id`](Self::id).
    pub public_key: [u8; 32],
}

/// Static configuration for one node's control-plane raft.
#[derive(Debug, Clone)]
pub struct RaftDriverConfig {
    /// This node's raft id.
    pub node_id: u64,
    /// The full cluster membership (including this node) — every node
    /// must be configured with the same set.
    pub voters: Vec<u64>,
    /// The other cluster nodes, excluding this one.
    pub peers: Vec<RaftPeer>,
    /// Directory for the durable raft state file.
    pub dir: PathBuf,
    /// The raft RPC address this node announces into the membership
    /// registry — its `--raft-bind`, or the `--raft-advertise`
    /// override when binding a wildcard.
    pub advertise_raft_addr: SocketAddr,
    /// The replication data-plane address this node announces —
    /// where a replica dials to follow it when it leads. `None` when
    /// the node cannot serve replicas (no usable `--replication-bind`).
    pub advertise_replication_addr: Option<SocketAddr>,
    /// The client order-entry address this node announces — where a
    /// redirected client reconnects when this node leads. `None` when
    /// no routable client address is available.
    pub advertise_order_entry_addr: Option<SocketAddr>,
    /// Act on elections instead of only observing them
    /// (`--raft-auto-promote`): a replica that wins leadership files a
    /// promotion request (subject to [`auto_promotion_decision`]), and
    /// a node still acting as primary fences itself when a peer's tip
    /// carries a higher fencing epoch. Off = election stays purely
    /// observational, exactly as before.
    pub auto_promote: bool,
}

/// A runtime voter-set change an operator requests via the admin
/// endpoint (`RAFT-ADD-VOTER` / `RAFT-REMOVE-VOTER`).
#[derive(Debug, Clone)]
pub enum VoterChange {
    /// Grow the cluster: admit `node_id` as a voter. Carries the seed
    /// identity (raft dial address + replication public key) so the
    /// driver can propose a [`MemberRecord`] for the joiner *before* the
    /// `ConfChange` — existing members need that record to dial and
    /// authenticate the newcomer, and nobody else would propose it (the
    /// joiner cannot be dialed until its key is pinned).
    Add {
        node_id: u64,
        raft_addr: SocketAddr,
        public_key: [u8; 32],
    },
    /// Shrink the cluster: drop `node_id` from the voter set.
    Remove { node_id: u64 },
}

impl VoterChange {
    /// The node id this change targets.
    fn node_id(&self) -> u64 {
        match self {
            VoterChange::Add { node_id, .. } | VoterChange::Remove { node_id } => *node_id,
        }
    }

    /// The seed [`MemberRecord`] to propose before an `AddNode`
    /// `ConfChange` (addresses beyond the raft dial target are left
    /// `None` — the joiner's own announce loop fills them in once it is
    /// caught up). `None` for a removal, which proposes no record.
    fn seed_record(&self) -> Option<MemberRecord> {
        match self {
            VoterChange::Add {
                node_id,
                raft_addr,
                public_key,
            } => Some(MemberRecord {
                node_id: *node_id,
                raft_addr: *raft_addr,
                replication_addr: None,
                order_entry_addr: None,
                // A joiner is never serving at admission; its own
                // announce loop claims an epoch only if it later promotes.
                serving_epoch: None,
                public_key: *public_key,
            }),
            VoterChange::Remove { .. } => None,
        }
    }
}

/// One operator request to change the voter set, with a one-shot reply
/// channel. The driver answers exactly once: the resulting voter set on
/// success, or a human-readable refusal.
pub struct VoterChangeRequest {
    pub change: VoterChange,
    pub reply: Sender<Result<Vec<u64>, String>>,
}

/// Handles present only on a node that booted as a replica — the
/// levers auto-promotion pulls. A genesis primary has none (there is
/// nothing to promote).
pub struct ReplicaSignals {
    /// Promotion request shared with the admin endpoint and the
    /// replica's receive loop. Also how the driver knows the node's
    /// current data-plane role: a filed request means this node is
    /// (becoming) a primary.
    pub promote: PromotionRequest,
    /// `true` while the replica holds an authenticated replication
    /// connection to its primary — see
    /// `replication::ReplicaControlPlane::primary_link_up`.
    pub primary_link_up: Arc<AtomicBool>,
    /// The durability mode last advertised by the primary
    /// (`ACKING_MODE_UNKNOWN` until first contact) — see
    /// `replication::ReplicaControlPlane::primary_acking_mode`.
    pub primary_acking_mode: Arc<AtomicU8>,
}

/// Data-plane view of the applied membership registry: which address
/// to dial to follow a given node. Written by the driver on registry
/// changes, read by the replica receiver on reconnect attempts — both
/// control-plane-cold, so a `RwLock` (not a lock-free scheme) keeps
/// each update atomic across the whole directory with no cleverness.
#[derive(Debug, Default)]
pub struct ClusterDirectory {
    inner: RwLock<Registry>,
}

impl ClusterDirectory {
    /// Replace the directory with the latest applied registry.
    /// pub(crate) so the redirect acceptor's tests can stage a
    /// directory without a live raft driver.
    pub(crate) fn update(&self, registry: &Registry) {
        match self.inner.write() {
            Ok(mut guard) => *guard = registry.clone(),
            // Poisoning requires a panic under the lock; the driver is
            // the only writer and reads are infallible clones — warn so
            // a stale directory is at least visible.
            Err(_) => warn!("cluster directory lock poisoned — directory not updated"),
        }
    }

    /// One field of `node_id`'s committed record, selected by `pick` —
    /// the shared read for every per-node address accessor so the
    /// lock-poisoning policy lives in one place. A poisoned lock (a
    /// panic under the writer — see `update`, the only writer) reads as
    /// "unknown": callers treat a missing address and an unreadable
    /// directory identically, and `update` already warns when the
    /// directory goes stale.
    fn member_field(
        &self,
        node_id: u64,
        pick: impl Fn(&MemberRecord) -> Option<SocketAddr>,
    ) -> Option<SocketAddr> {
        match self.inner.read() {
            Ok(guard) => guard.get(node_id).and_then(pick),
            Err(_) => None,
        }
    }

    /// The announced replication address of `node_id`, if known.
    pub fn replication_addr(&self, node_id: u64) -> Option<SocketAddr> {
        self.member_field(node_id, |r| r.replication_addr)
    }

    /// The announced client order-entry address of `node_id`, if known.
    pub fn order_entry_addr(&self, node_id: u64) -> Option<SocketAddr> {
        self.member_field(node_id, |r| r.order_entry_addr)
    }

    /// The cluster's current serving primary — the live record with the
    /// highest `serving_epoch` that also announced an order-entry address
    /// clients can reach, returned as `(node_id, order_entry_addr)`.
    /// Fencing order (highest epoch), not announcement order, decides: a
    /// deposed primary that never learned of its supersession keeps its
    /// stale claim in the directory, but the promoted node announces a
    /// strictly higher epoch and outranks it — self-healing with no
    /// tombstones. Ties (two *manual* promotions colliding on one epoch,
    /// a pre-existing documented hazard) break deterministically to the
    /// lower node id so every node resolves identically. `None` when no
    /// node claims to serve or the sole claimant announced no client
    /// address, or the directory lock is poisoned (treated as unknown,
    /// matching `member_field`).
    pub fn serving_primary(&self) -> Option<(u64, SocketAddr)> {
        let guard = self.inner.read().ok()?;
        guard
            .iter()
            .filter_map(|r| Some((r.node_id, r.serving_epoch?, r.order_entry_addr?)))
            // Highest epoch wins; on a tie the lower node id wins, so the
            // reversed id comparison makes the lower id compare "greater".
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| b.0.cmp(&a.0)))
            .map(|(id, _epoch, addr)| (id, addr))
    }
}

/// A replica's handle for following the control-plane leader on the
/// data plane: resolves "whom should I be replicating from right now".
#[derive(Clone)]
pub struct LeaderFollow {
    /// This node's own raft id — a leader never follows itself (a
    /// replica that wins an election is about to promote instead).
    pub self_node_id: u64,
    /// Election gauges (the current leader id).
    pub status: Arc<RaftStatus>,
    /// The membership directory the leader id resolves through.
    pub directory: Arc<ClusterDirectory>,
}

impl LeaderFollow {
    /// The leader guard shared by every leader-address accessor: `None`
    /// while leaderless (id 0 sentinel) or while this node itself
    /// leads; otherwise the leader's record field selected by `pick`.
    /// One place for the sentinel/self-exclusion subtlety so a future
    /// correction cannot land on one address kind and miss the other.
    fn leader_field(
        &self,
        pick: impl Fn(&ClusterDirectory, u64) -> Option<SocketAddr>,
    ) -> Option<SocketAddr> {
        match self.status.leader_id.load(Ordering::Relaxed) {
            0 => None,
            id if id == self.self_node_id => None,
            id => pick(&self.directory, id),
        }
    }

    /// The current leader's announced replication address — `None`
    /// while leaderless, while this node itself leads, or while the
    /// leader has not announced a followable address.
    pub fn leader_replication_addr(&self) -> Option<SocketAddr> {
        self.leader_field(|d, id| d.replication_addr(id))
    }

    /// The current leader's announced client order-entry address —
    /// what a replica's redirect acceptor points clients at. Same
    /// `None` cases as [`Self::leader_replication_addr`]. Under
    /// `--raft-auto-promote` the post-failover leader IS the serving
    /// primary, which is when redirects matter; pre-failover a client
    /// pointed at a replica may bounce once via a replica-leader and
    /// then sees "busy" — bounded on the client side.
    pub fn leader_order_entry_addr(&self) -> Option<SocketAddr> {
        self.leader_field(|d, id| d.order_entry_addr(id))
    }

    /// The serving primary's order-entry address — what a replica's
    /// redirect acceptor should prefer over the raft leader's, since the
    /// leader and the serving primary can differ (a replica may hold raft
    /// leadership while the old primary keeps serving under the
    /// primary-link-up promotion veto). `None` while no node claims to
    /// serve, or when the claimant is *this* node — a redirecting replica
    /// must never point a client at itself (same self-exclusion as
    /// [`Self::leader_field`]).
    pub fn serving_primary_order_entry_addr(&self) -> Option<SocketAddr> {
        match self.directory.serving_primary() {
            Some((id, _)) if id == self.self_node_id => None,
            Some((_, addr)) => Some(addr),
            None => None,
        }
    }
}

/// What the server wiring gets back from spawning the driver: the
/// election gauges for health, and the data-plane leader-follow handle
/// (given to replicas when `--raft-auto-promote` is set).
pub struct RaftHandles {
    pub status: Arc<RaftStatus>,
    pub follow: LeaderFollow,
}

/// Everything the driver thread borrows from the server.
pub struct RaftDriverContext {
    /// This node's cluster identity key (the `--replication-key`),
    /// used to authenticate outbound peer connections.
    pub signing_key: SigningKey,
    /// Key table for authenticating inbound peers (`replication`
    /// permission required).
    pub authorized_keys: Arc<AuthorizedKeys>,
    /// Fencing state — supplies the epoch half of the journal tip
    /// advertised on every frame ([`local_tip`]).
    pub fence_state: Arc<FenceState>,
    /// Sequence half of the advertised journal tip: the highest
    /// contiguous wire seq this node would carry into a promotion.
    /// Maintained by the replication receiver (replica) or the journal
    /// stage (primary) — see [`AdvertisedJournalTip`] for the ownership
    /// rules and why the two roles advertise different cursors.
    pub journal_tip: AdvertisedJournalTip,
    /// `true` once this node's fence epoch reflects its own recovered
    /// journal, so the tip it advertises (and votes it grants) are
    /// trustworthy. A primary knows its epoch before the driver starts,
    /// so it passes an already-`true` flag; a replica seeds its epoch
    /// only after journal recovery, so it starts `false` and the
    /// receiver flips it once recovery has run. While `false` the driver
    /// refuses to grant votes (drops inbound vote requests) — advertising
    /// epoch 0 mid-recovery would otherwise make a caught-up replica vote
    /// for a stale peer. Dropping vote requests only delays an election,
    /// never affects safety.
    pub tip_ready: Arc<AtomicBool>,
    /// Election observability published to the health endpoint.
    pub status: Arc<RaftStatus>,
    /// Membership directory exported to the data plane (see
    /// [`ClusterDirectory`]); the driver refreshes it whenever the
    /// applied registry changes.
    pub directory: Arc<ClusterDirectory>,
    /// The active durability mode (`DurabilityMode::as_u8` encoding,
    /// runtime-retunable via the admin `DURABILITY` command). Read by
    /// the auto-promotion refusal rule: in `local` mode an election win
    /// proves nothing about acked orders, so the driver never
    /// auto-promotes past it.
    pub durability_mode: Arc<AtomicU8>,
    /// Present iff this node booted as a replica — see
    /// [`ReplicaSignals`]. `None` on a genesis primary.
    pub replica: Option<ReplicaSignals>,
    /// Operator voter-set changes from the admin endpoint. The driver
    /// drains this once per loop iteration and shepherds at most one
    /// change to commitment at a time (see [`drain_voter_changes`]); the
    /// admin handler holds the matching `Sender` as an optional
    /// capability. Always present on a raft node — a raft-less node has
    /// no driver and no `Sender`.
    pub voter_changes: Receiver<VoterChangeRequest>,
    /// Process-wide shutdown flag.
    pub shutdown: Arc<AtomicBool>,
}

/// An authenticated socket delivered by a helper auth thread.
enum AuthedSocket {
    /// Inbound peer link (read-only for the driver): the resolved peer
    /// id (from its pinned public key), the socket, and its address.
    Inbound(u64, TcpStream, SocketAddr),
    /// Outbound link to `peer_id` (write-only for the driver).
    Outbound(u64, TcpStream),
    /// An outbound dial/auth attempt failed; retry after backoff.
    OutboundFailed(u64),
}

/// One live inbound connection.
struct InboundConn {
    /// Raft id this connection authenticated as (via its pinned public
    /// key). Frames whose `from` field disagrees are dropped — a peer
    /// cannot speak for another node's id.
    peer_id: u64,
    stream: TcpStream,
    peer: SocketAddr,
    recv_buf: Vec<u8>,
    /// Last time this link produced bytes; drives idle reaping of
    /// half-open connections (see [`INBOUND_IDLE_TIMEOUT`]).
    last_activity: Instant,
}

/// The inbound-connection / concurrent-auth cap for a cluster with
/// `peer_count` peers: one legitimate inbound link per peer, times
/// [`INBOUND_SLACK`], floored at [`INBOUND_CAP_FLOOR`].
fn inbound_cap(peer_count: usize) -> usize {
    (peer_count * INBOUND_SLACK).max(INBOUND_CAP_FLOOR)
}

/// RAII counter for in-flight auth helper threads: [`AuthSlot::acquire`]
/// increments the shared count and the guard decrements it when dropped
/// (thread exit by any path — success, auth failure, timeout, panic), so
/// a hung or slow handshake still frees its slot. The accept loop refuses
/// new connections once the count reaches the cap, bounding the thread
/// fan-out an unauthenticated flood on the raft port can create.
struct AuthSlot(Arc<std::sync::atomic::AtomicUsize>);

impl AuthSlot {
    fn acquire(counter: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        AuthSlot(Arc::clone(counter))
    }
}

impl Drop for AuthSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// Outbound link state for one peer.
struct PeerLink {
    addr: SocketAddr,
    /// `None` while disconnected or a dial is in flight.
    stream: Option<TcpStream>,
    /// Unflushed egress bytes.
    out_buf: Vec<u8>,
    /// Earliest time of the next dial attempt.
    next_dial: Instant,
    /// A dial/auth helper thread is currently running for this peer.
    dialing: bool,
}

/// Bind the raft listener and spawn the driver thread.
///
/// Binding happens synchronously so configuration errors (port in use)
/// fail startup instead of surfacing as a log line from a background
/// thread — the same contract as `health::spawn`.
pub fn spawn(
    bind_addr: SocketAddr,
    config: RaftDriverConfig,
    context: RaftDriverContext,
) -> io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(bind_addr)?;
    listener.set_nonblocking(true)?;
    info!(
        addr = %bind_addr,
        node_id = config.node_id,
        voters = ?config.voters,
        "control-plane raft listening"
    );
    spawn_with_listener(listener, config, context)
}

/// Spawn the driver on an already-bound listener (tests bind port 0
/// first so peer addresses are known before any node starts).
pub fn spawn_with_listener(
    listener: TcpListener,
    config: RaftDriverConfig,
    context: RaftDriverContext,
) -> io::Result<JoinHandle<()>> {
    listener.set_nonblocking(true)?;
    let node = ControlNode::open(config.node_id, &config.dir, &config.voters)?;
    std::thread::Builder::new()
        .name("raft-driver".into())
        .spawn(move || run(listener, node, config, context))
        .map_err(io::Error::other)
}

/// The serving claim this node should announce right now: `Some(epoch)`
/// while it acts as the serving primary, `None` while it is a plain
/// replica. "Acts as primary" mirrors the supersession-fence predicate
/// in [`read_inbound`]: a genesis primary (no replica signals) always
/// serves; a replica serves once its promotion is requested (auto or
/// manual). Unlike that predicate this carries no `auto_promote` gate —
/// a serving node's claim is true regardless of how it was promoted.
///
/// The epoch is the current fence epoch. For a freshly promoted replica
/// the `EpochBump` can land a tick after `promote` flips, so the claim
/// may announce at the pre-bump epoch for one iteration and upgrade on
/// the next; the driver re-evaluates every tick and re-announces on any
/// change, so this converges without special-casing the race.
fn serving_claim(replica: Option<&ReplicaSignals>, fence_epoch: u64) -> Option<u64> {
    replica
        .is_none_or(|r| r.promote.is_requested())
        .then_some(fence_epoch)
}

fn run(
    listener: TcpListener,
    mut node: ControlNode,
    config: RaftDriverConfig,
    context: RaftDriverContext,
) {
    let (authed_tx, authed_rx): (Sender<AuthedSocket>, Receiver<AuthedSocket>) = channel();
    // `Vec` (not a keyed map): the inbound set is tiny (bounded by the
    // peer count) and only ever scanned/retained wholesale each tick, so
    // a linear scan beats map overhead. `links` below is a `HashMap`
    // because every raft message routes by peer id (`msg.to`), an
    // O(1)-lookup access pattern.
    let mut inbound: Vec<InboundConn> = Vec::new();
    // A healthy cluster holds one inbound link (and needs at most one
    // concurrent auth) per peer; the cap adds slack for reconnect overlap
    // and bounds a flood.
    let conn_cap = inbound_cap(config.peers.len());
    let inflight_auth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    // Pinned identity map: an inbound connection authenticated with a
    // peer's public key may only speak for that peer's id. `RwLock`
    // (not the previous immutable map): applied registry records can
    // move or add peers at runtime; auth helper threads take a read
    // snapshot per handshake (cold), the driver is the only writer.
    let pubkey_to_id: Arc<RwLock<HashMap<[u8; 32], u64>>> = Arc::new(RwLock::new(
        config.peers.iter().map(|p| (p.public_key, p.id)).collect(),
    ));
    let mut links: HashMap<u64, PeerLink> = config
        .peers
        .iter()
        .map(|p| {
            (
                p.id,
                PeerLink {
                    addr: p.addr,
                    stream: None,
                    out_buf: Vec::new(),
                    next_dial: Instant::now(),
                    dialing: false,
                },
            )
        })
        .collect();

    // The record this node announces into the registry. Re-proposed
    // until the applied registry reflects it (proposals can be lost to
    // leader churn), and again whenever a stale record for this id
    // shows up (e.g. this node restarted at a new address).
    // The static identity fields never change; only `serving_epoch` moves
    // at runtime (on promotion), recomputed each iteration before the
    // announce comparison below.
    let mut self_record = MemberRecord {
        node_id: config.node_id,
        raft_addr: config.advertise_raft_addr,
        replication_addr: config.advertise_replication_addr,
        order_entry_addr: config.advertise_order_entry_addr,
        serving_epoch: serving_claim(context.replica.as_ref(), context.fence_state.epoch()),
        public_key: context.signing_key.verifying_key().to_bytes(),
    };
    let mut next_announce = Instant::now();

    // The persisted registry may already know peers (or newer
    // addresses) the static flags don't — wire them in before the
    // first dial round.
    sync_registry(
        &node,
        &config,
        &mut links,
        &pubkey_to_id,
        &context.directory,
    );

    let mut next_tick = Instant::now() + TICK_INTERVAL;
    publish_status(&node, &context.status);
    // Term of the last logged auto-promotion refusal, so a standing
    // refusal (e.g. `local` durability) warns once per tenure instead
    // of every 10 ms poll.
    let mut last_refused_term: u64 = 0;
    // The operator voter change currently being shepherded to
    // commitment, if any — at most one at a time (raft serializes conf
    // changes; see `drain_voter_changes`).
    let mut pending_voter: Option<PendingVoterChange> = None;

    loop {
        if context.shutdown.load(Ordering::Relaxed) {
            break;
        }
        let now = Instant::now();

        // 1. Raft clock. Deadline-anchored (`+= TICK`, not `now + TICK`)
        // so ordinary slow iterations don't stretch the logical clock —
        // but tick at most once per loop and drop any backlog. Without
        // the resync, after a multi-second thread stall (VM pause, cgroup
        // throttle, a slow state-file fsync) `now` stays past `next_tick`
        // for many iterations and the raft clock runs at poll cadence
        // (~10x real time), compressing election timeouts and flapping
        // leadership on a node that never lost connectivity. A stalled
        // node genuinely didn't advance its clock, so replaying the
        // missed ticks is wrong.
        if now >= next_tick {
            node.tick();
            next_tick += TICK_INTERVAL;
            if now >= next_tick {
                next_tick = now + TICK_INTERVAL;
            }
        }

        // 2. New inbound connections → helper auth threads.
        accept_inbound(
            &listener,
            &context,
            &authed_tx,
            &inflight_auth,
            &pubkey_to_id,
            conn_cap,
        );

        // 3. Freshly authenticated sockets and dial results.
        drain_authed(&authed_rx, &mut inbound, &mut links, conn_cap);

        // 4. Kick off outbound dials that are due.
        dial_due_peers(&mut links, &config, &context, &authed_tx, now);

        // 5. Ingress: read peers, extract frames, filter, step raft.
        read_inbound(&mut inbound, &mut node, &config, &context);
        poll_outbound_liveness(&mut links);

        // 6. Drain raft readies (fsyncs inside) and route the egress.
        match drain_node(&mut node, &mut links, &context) {
            None => {
                // Storage failure: raft is inoperable by contract. The
                // control plane stops; trading continues on the data
                // plane.
                break;
            }
            Some(true) => {
                // The applied registry changed — refresh dial targets,
                // identity pins, and the data-plane directory.
                sync_registry(
                    &node,
                    &config,
                    &mut links,
                    &pubkey_to_id,
                    &context.directory,
                );
            }
            Some(false) => {}
        }

        // 7. Announce this node's record until the registry reflects
        // it. Refresh the serving claim first (cheap; the only field
        // that moves at runtime) so a promotion re-announces within one
        // interval. The comparison is a tiny map lookup; the timer only
        // paces the re-proposals.
        self_record.serving_epoch =
            serving_claim(context.replica.as_ref(), context.fence_state.epoch());
        if now >= next_announce && node.registry().get(config.node_id) != Some(&self_record) {
            // Dropped proposals (no leader yet) debug-log inside and
            // are retried on the next interval either way.
            node.propose_member(&self_record);
            next_announce = now + ANNOUNCE_RETRY_INTERVAL;
        }

        // 8. Operator voter-set changes: accept at most one in flight and
        // shepherd it (seed record → ConfChange) to commitment.
        drain_voter_changes(&context.voter_changes, &mut node, &mut pending_voter, now);

        publish_status(&node, &context.status);
        consider_auto_promotion(&node, &config, &context, &mut last_refused_term);
        std::thread::sleep(POLL_INTERVAL);
    }

    // The driver is exiting (clean shutdown or storage failure). Answer
    // any in-flight voter change so the admin handler returns at once
    // instead of blocking to its own timeout.
    if let Some(p) = pending_voter.take() {
        reply_voter(
            &p.reply,
            Err("control plane shutting down — voter change aborted".into()),
        );
    }
    // Clear leadership and drop the running flag so `/metrics` stops
    // reporting a stale leader on a node whose control plane is gone —
    // on a storage failure the process keeps serving trading and its
    // health endpoint, so these gauges would otherwise freeze forever.
    context.status.mark_stopped();
}

/// The control-node operations the voter-change state machine drives.
/// Abstracted into a trait so the machine is unit-testable against a
/// mock (see the `voter` tests) without a live raft cluster or sockets.
trait VoterOps {
    /// The current leader as this node sees it (`None` while leaderless).
    fn leader_id(&self) -> Option<u64>;
    /// The applied voter set, ascending.
    fn voters(&self) -> Vec<u64>;
    /// Whether `node_id` is in the applied voter set.
    fn is_voter(&self, node_id: u64) -> bool {
        self.voters().contains(&node_id)
    }
    /// Whether the applied registry holds a record for `node_id`.
    fn registry_has(&self, node_id: u64) -> bool {
        self.registry_record(node_id).is_some()
    }
    /// The applied directory record for `node_id`, if any — backs the
    /// re-key / re-address detection on add.
    fn registry_record(&self, node_id: u64) -> Option<MemberRecord>;
    /// The id currently pinned to `key` in the applied registry, if any —
    /// backs the "one key, one identity" rail.
    fn registry_id_for_key(&self, key: &[u8; 32]) -> Option<u64>;
    fn propose_member(&mut self, record: &MemberRecord) -> bool;
    fn propose_add_voter(&mut self, node_id: u64) -> bool;
    fn propose_remove_voter(&mut self, node_id: u64) -> bool;
}

impl VoterOps for ControlNode {
    fn leader_id(&self) -> Option<u64> {
        ControlNode::leader_id(self)
    }
    fn voters(&self) -> Vec<u64> {
        ControlNode::voters(self)
    }
    fn registry_record(&self, node_id: u64) -> Option<MemberRecord> {
        self.registry().get(node_id).cloned()
    }
    fn registry_id_for_key(&self, key: &[u8; 32]) -> Option<u64> {
        self.registry()
            .iter()
            .find(|r| &r.public_key == key)
            .map(|r| r.node_id)
    }
    fn propose_member(&mut self, record: &MemberRecord) -> bool {
        ControlNode::propose_member(self, record)
    }
    fn propose_add_voter(&mut self, node_id: u64) -> bool {
        ControlNode::propose_add_voter(self, node_id)
    }
    fn propose_remove_voter(&mut self, node_id: u64) -> bool {
        ControlNode::propose_remove_voter(self, node_id)
    }
}

/// How far a pending [`VoterChange`] has progressed. An `Add` is two
/// staged proposals — seed the joiner's record so peers can dial it,
/// *then* the `ConfChange` — because raft cannot deliver the log to a
/// node the cluster cannot yet authenticate. A `Remove` is a single
/// `ConfChange`.
enum VoterStage {
    /// `Add` phase 1: waiting for the seed [`MemberRecord`] to apply.
    SeedRecord,
    /// `Add` phase 2, or a `Remove`: waiting for the `ConfChange` to
    /// apply into the voter set.
    ConfChange,
}

/// An in-flight voter change the driver is shepherding to commitment.
/// Proposals are ack-less and can be lost to leader churn, so each stage
/// re-proposes on [`ANNOUNCE_RETRY_INTERVAL`] until the driver *observes*
/// it applied, bounded by an overall [`VOTER_CHANGE_DEADLINE`].
struct PendingVoterChange {
    change: VoterChange,
    stage: VoterStage,
    reply: Sender<Result<Vec<u64>, String>>,
    deadline: Instant,
    next_propose: Instant,
}

/// Send the one-shot voter-change reply, tolerating a closed channel:
/// the admin handler may have hit its own `recv_timeout` and dropped the
/// receiver before the driver answers. The operator sees a timeout
/// either way, so a failed send needs no handling beyond a debug note.
fn reply_voter(reply: &Sender<Result<Vec<u64>, String>>, outcome: Result<Vec<u64>, String>) {
    if reply.send(outcome).is_err() {
        debug!("voter-change reply dropped — admin handler already gone");
    }
}

/// Re-propose the current stage of a pending change. Idempotent: raft
/// serializes conf changes and overwrites a superseded record, so a
/// duplicate proposal after a leader change is harmless.
fn propose_stage(node: &mut impl VoterOps, change: &VoterChange, stage: &VoterStage) {
    match stage {
        VoterStage::SeedRecord => {
            if let Some(rec) = change.seed_record() {
                node.propose_member(&rec);
            }
        }
        VoterStage::ConfChange => match change {
            VoterChange::Add { node_id, .. } => {
                node.propose_add_voter(*node_id);
            }
            VoterChange::Remove { node_id } => {
                node.propose_remove_voter(*node_id);
            }
        },
    }
}

/// Validate a fresh request against the safety rails and either resolve
/// it immediately — an idempotent success or a refusal, reply already
/// sent, returns `None` — or kick off stage 1 and return the pending
/// change to shepherd. `now` anchors the deadline and re-propose timer.
fn begin_voter_change(
    node: &mut impl VoterOps,
    req: VoterChangeRequest,
    now: Instant,
) -> Option<PendingVoterChange> {
    let id = req.change.node_id();
    // Rail: raft's invalid-id sentinel is never a real node.
    if id == 0 {
        reply_voter(&req.reply, Err("node id 0 is reserved".into()));
        return None;
    }
    match &req.change {
        VoterChange::Add {
            node_id,
            raft_addr,
            public_key,
        } => {
            // Rail: one key, one identity — the registry is the auth
            // directory, so a key already speaking for a different id
            // cannot be reused for this one.
            if let Some(other) = node.registry_id_for_key(public_key)
                && other != *node_id
            {
                reply_voter(
                    &req.reply,
                    Err(format!("public key already pinned to node {other}")),
                );
                return None;
            }
            // Already a voter with a record: compare the recorded identity.
            if node.is_voter(*node_id)
                && let Some(rec) = node.registry_record(*node_id)
            {
                if &rec.public_key == public_key && rec.raft_addr == *raft_addr {
                    // Same identity → idempotent success (scripting-friendly).
                    reply_voter(&req.reply, Ok(node.voters()));
                } else {
                    // A different key or address is a re-key / re-address.
                    // `Add` cannot do it in place — the seed record carries
                    // only the raft addr, so an overwrite would wipe the
                    // replication / order-entry addresses the joiner
                    // announced. Steer to remove + re-add under the new
                    // identity (the remove now reclaims the record cleanly).
                    reply_voter(
                        &req.reply,
                        Err(format!(
                            "node {node_id} is already a voter under a different identity — RAFT-REMOVE-VOTER {node_id} first, then re-add"
                        )),
                    );
                }
                return None;
            }
        }
        VoterChange::Remove { node_id } => {
            // Idempotent only when truly gone: neither a voter nor a
            // lingering directory record. An orphaned record (an add whose
            // `AddNode` never committed) is *not* the desired state — fall
            // through so a `RemoveNode` reclaims it.
            if !node.is_voter(*node_id) && !node.registry_has(*node_id) {
                reply_voter(&req.reply, Ok(node.voters()));
                return None;
            }
            // The consensus-safety rails apply only when the target is an
            // actual voter; reclaiming an orphaned record never touches
            // the quorum and is always safe.
            if node.is_voter(*node_id) {
                // Rail: refuse removing the *live* leader — leadership must
                // move first, else the removal races the election it
                // forces. Removing a dead ex-leader is fine.
                if node.leader_id() == Some(*node_id) {
                    reply_voter(
                        &req.reply,
                        Err(format!(
                            "node {node_id} currently leads — stop it and let the cluster elect first"
                        )),
                    );
                    return None;
                }
                // Rail: never remove the last voter — it would brick consensus.
                if node.voters().len() <= 1 {
                    reply_voter(&req.reply, Err("cannot remove the last voter".into()));
                    return None;
                }
            }
        }
    }

    // Rails passed — kick off stage 1 and install the pending change.
    let stage = match &req.change {
        VoterChange::Add { .. } => VoterStage::SeedRecord,
        VoterChange::Remove { .. } => VoterStage::ConfChange,
    };
    propose_stage(node, &req.change, &stage);
    Some(PendingVoterChange {
        change: req.change,
        stage,
        reply: req.reply,
        deadline: now + VOTER_CHANGE_DEADLINE,
        next_propose: now + ANNOUNCE_RETRY_INTERVAL,
    })
}

/// Advance the pending change one step: promote `Add` phase 1→2 once the
/// seed record applies, reply + clear once the `ConfChange` is observed
/// applied (or the deadline passes), and re-propose the current stage on
/// its retry cadence. A no-op when nothing is pending.
fn advance_voter_change(
    node: &mut impl VoterOps,
    pending: &mut Option<PendingVoterChange>,
    now: Instant,
) {
    let Some(p) = pending.as_mut() else {
        return;
    };
    let id = p.change.node_id();

    // Observe progress first, so a change that applies on the same tick
    // it was proposed replies without waiting out the retry timer.
    match p.stage {
        VoterStage::SeedRecord => {
            if node.registry_has(id) {
                // Phase 1 done — propose the `ConfChange` and advance. It
                // needs a further round-trip to apply, so return and let a
                // later iteration observe the voter set.
                node.propose_add_voter(id);
                p.stage = VoterStage::ConfChange;
                p.next_propose = now + ANNOUNCE_RETRY_INTERVAL;
                return;
            }
        }
        VoterStage::ConfChange => {
            let applied = match &p.change {
                VoterChange::Add { .. } => node.is_voter(id),
                // Done once the voter is gone *and* its directory record
                // has been pruned — the committed `RemoveNode` does both in
                // one apply, so this also confirms an orphan was reclaimed.
                VoterChange::Remove { .. } => !node.is_voter(id) && !node.registry_has(id),
            };
            if applied {
                reply_voter(&p.reply, Ok(node.voters()));
                *pending = None;
                return;
            }
        }
    }

    // Deadline: give up so the operator command returns.
    if now >= p.deadline {
        reply_voter(
            &p.reply,
            Err(format!(
                "voter change for node {id} not committed within {}s — check cluster health and retry",
                VOTER_CHANGE_DEADLINE.as_secs()
            )),
        );
        *pending = None;
        return;
    }

    // Re-propose the current stage if the retry timer is due.
    if now >= p.next_propose {
        propose_stage(node, &p.change, &p.stage);
        p.next_propose = now + ANNOUNCE_RETRY_INTERVAL;
    }
}

/// Drain operator voter-change requests and advance the pending one.
/// Installs the first request when idle; refuses the rest while one is in
/// flight (raft admits a single pending conf change, and the two-stage
/// add machinery assumes exclusivity). Called once per loop iteration.
fn drain_voter_changes(
    rx: &Receiver<VoterChangeRequest>,
    node: &mut ControlNode,
    pending: &mut Option<PendingVoterChange>,
    now: Instant,
) {
    loop {
        match rx.try_recv() {
            Ok(req) => {
                if pending.is_some() {
                    reply_voter(&req.reply, Err("another voter change is in flight".into()));
                } else {
                    *pending = begin_voter_change(node, req, now);
                }
            }
            // No more requests this iteration.
            Err(TryRecvError::Empty) => break,
            // Every `Sender` dropped (admin torn down / raft-less): nothing
            // more will ever arrive, so stop draining.
            Err(TryRecvError::Disconnected) => break,
        }
    }
    advance_voter_change(node, pending, now);
}

/// Accept any pending inbound connections and hand each to a helper
/// thread for the blocking auth handshake. Refuses new connections once
/// `cap` handshakes are already in flight, so an unauthenticated flood
/// on the raft port cannot spawn unbounded OS threads.
fn accept_inbound(
    listener: &TcpListener,
    context: &RaftDriverContext,
    authed_tx: &Sender<AuthedSocket>,
    inflight_auth: &Arc<std::sync::atomic::AtomicUsize>,
    pubkey_to_id: &Arc<RwLock<HashMap<[u8; 32], u64>>>,
    cap: usize,
) {
    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                if inflight_auth.load(Ordering::Acquire) >= cap {
                    // At the auth-concurrency cap: drop the connection
                    // without spawning a thread. A legitimate peer
                    // re-dials with backoff; this only bites under a
                    // flood, which is exactly when we want to shed.
                    debug!(peer = %peer, cap, "raft auth cap reached — refusing connection");
                    drop(stream);
                    continue;
                }
                debug!(peer = %peer, "raft peer connection accepted — authenticating");
                // Reserve an auth slot; freed on thread exit by any path.
                let slot = AuthSlot::acquire(inflight_auth);
                let keys = Arc::clone(&context.authorized_keys);
                let ids = Arc::clone(pubkey_to_id);
                let tx = authed_tx.clone();
                let spawned = std::thread::Builder::new()
                    .name("raft-peer-auth".into())
                    .spawn(move || {
                        let _slot = slot;
                        let mut stream = stream;
                        // A failure to arm the deadline would let a
                        // silent peer block this helper thread forever,
                        // so treat it as an auth failure rather than
                        // proceeding without a timeout.
                        if stream.set_read_timeout(Some(ACCEPT_AUTH_TIMEOUT)).is_err()
                            || stream.set_write_timeout(Some(ACCEPT_AUTH_TIMEOUT)).is_err()
                        {
                            debug!(peer = %peer, "failed to arm raft auth timeout — dropping");
                            return;
                        }
                        match authenticate_replica_identified(&mut stream, &keys) {
                            // A poisoned lock (writer panicked) reads as
                            // "unknown key" — reject rather than guess.
                            Ok(pubkey) => match ids.read().ok().and_then(|m| m.get(&pubkey).copied())
                            {
                                Some(peer_id) => {
                                    // Receiver gone ⇒ the driver exited;
                                    // the socket just drops, which is the
                                    // correct teardown either way.
                                    let _ = tx.send(AuthedSocket::Inbound(peer_id, stream, peer));
                                }
                                None => {
                                    // A valid replication key, but not one
                                    // of this node's configured raft peers
                                    // (e.g. a data-plane-only replica key).
                                    // It has no place in consensus.
                                    debug!(peer = %peer, "raft peer key not a configured cluster member — rejecting");
                                }
                            },
                            Err(e) => {
                                debug!(peer = %peer, error = %e, "raft peer auth failed");
                            }
                        }
                    });
                if let Err(e) = spawned {
                    warn!(error = %e, "failed to spawn raft peer auth thread");
                    // `slot` was moved into the closure only on success;
                    // on spawn failure it is dropped here, releasing the
                    // reservation.
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return,
            Err(e) => {
                debug!(error = %e, "raft listener accept error");
                return;
            }
        }
    }
}

/// Absorb helper-thread results into the live connection sets. `cap`
/// bounds the live inbound set; excess links are dropped (the idle
/// reaper clears stale ones, so a healthy peer re-dials into a freed
/// slot).
fn drain_authed(
    authed_rx: &Receiver<AuthedSocket>,
    inbound: &mut Vec<InboundConn>,
    links: &mut HashMap<u64, PeerLink>,
    cap: usize,
) {
    while let Ok(authed) = authed_rx.try_recv() {
        match authed {
            AuthedSocket::Inbound(peer_id, stream, peer) => {
                if inbound.len() >= cap {
                    debug!(peer = %peer, cap, "inbound raft link cap reached — dropping");
                    drop(stream);
                    continue;
                }
                if let Err(e) = stream.set_nonblocking(true) {
                    debug!(peer = %peer, error = %e, "failed to set inbound raft socket non-blocking");
                    continue;
                }
                debug!(peer = %peer, peer_id, "raft peer link established (inbound)");
                inbound.push(InboundConn {
                    peer_id,
                    stream,
                    peer,
                    recv_buf: Vec::new(),
                    last_activity: Instant::now(),
                });
            }
            AuthedSocket::Outbound(peer_id, stream) => {
                let Some(link) = links.get_mut(&peer_id) else {
                    continue;
                };
                link.dialing = false;
                if let Err(e) = stream.set_nonblocking(true) {
                    debug!(peer_id, error = %e, "failed to set outbound raft socket non-blocking");
                    continue;
                }
                debug!(peer_id, "raft peer link established (outbound)");
                link.stream = Some(stream);
                link.out_buf.clear();
            }
            AuthedSocket::OutboundFailed(peer_id) => {
                if let Some(link) = links.get_mut(&peer_id) {
                    link.dialing = false;
                    link.next_dial = Instant::now() + RECONNECT_INTERVAL;
                }
            }
        }
    }
}

/// Start a dial+auth helper thread for every disconnected peer whose
/// backoff has elapsed.
fn dial_due_peers(
    links: &mut HashMap<u64, PeerLink>,
    config: &RaftDriverConfig,
    context: &RaftDriverContext,
    authed_tx: &Sender<AuthedSocket>,
    now: Instant,
) {
    for (&peer_id, link) in links.iter_mut() {
        if link.stream.is_some() || link.dialing || now < link.next_dial {
            continue;
        }
        link.dialing = true;
        let addr = link.addr;
        let key = context.signing_key.clone();
        let tx = authed_tx.clone();
        let node_id = config.node_id;
        let spawned = std::thread::Builder::new()
            .name("raft-peer-dial".into())
            .spawn(move || {
                let outcome = dial_and_auth(addr, &key);
                match outcome {
                    Ok(stream) => {
                        // Receiver gone ⇒ driver exited; drop the socket.
                        let _ = tx.send(AuthedSocket::Outbound(peer_id, stream));
                    }
                    Err(e) => {
                        debug!(node_id, peer_id, error = %e, "raft peer dial failed");
                        let _ = tx.send(AuthedSocket::OutboundFailed(peer_id));
                    }
                }
            });
        if let Err(e) = spawned {
            warn!(error = %e, "failed to spawn raft peer dial thread");
            link.dialing = false;
            link.next_dial = now + RECONNECT_INTERVAL;
        }
    }
}

/// Blocking dial + auth for one outbound attempt (helper thread only).
fn dial_and_auth(addr: SocketAddr, key: &SigningKey) -> io::Result<TcpStream> {
    let mut stream = TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)?;
    stream.set_read_timeout(Some(CONNECT_TIMEOUT))?;
    stream.set_write_timeout(Some(CONNECT_TIMEOUT))?;
    stream.set_nodelay(true)?;
    authenticate_with_primary(&mut stream, key)?;
    Ok(stream)
}

/// Read every inbound connection, extract complete frames, apply the
/// recency filter, and step the raft node. Dead or misbehaving
/// connections are dropped (the peer re-dials).
fn read_inbound(
    inbound: &mut Vec<InboundConn>,
    node: &mut ControlNode,
    config: &RaftDriverConfig,
    context: &RaftDriverContext,
) {
    let local_tip = local_tip(context);
    let tip_ready = context.tip_ready.load(Ordering::Acquire);
    // Control-plane fencing (auto-promote deployments only): a peer
    // whose envelope tip carries a higher fencing epoch has seen a
    // newer promotion. If this node still acts as a primary it has
    // been superseded and must stop acking. The data-plane handshake
    // fences too, but only when a connection crosses — a deposed
    // primary whose replicas all moved to the new one would otherwise
    // keep serving clients indefinitely; raft heartbeats reach it
    // regardless.
    let fence_on_supersession = config.auto_promote
        && context
            .replica
            .as_ref()
            .is_none_or(|r| r.promote.is_requested());
    let now = Instant::now();
    inbound.retain_mut(|conn| {
        let mut chunk = [0u8; 16 * 1024];
        let mut got_bytes = false;
        loop {
            match conn.stream.read(&mut chunk) {
                Ok(0) => {
                    debug!(peer = %conn.peer, "raft peer link closed");
                    return false;
                }
                Ok(n) => {
                    if conn.recv_buf.len() + n > MAX_IN_BUFFER {
                        debug!(peer = %conn.peer, "raft peer flooded the frame buffer — dropping link");
                        return false;
                    }
                    conn.recv_buf.extend_from_slice(&chunk[..n]);
                    got_bytes = true;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    debug!(peer = %conn.peer, error = %e, "raft peer read error — dropping link");
                    return false;
                }
            }
        }

        // Reap a half-open link: a peer that vanished without a FIN/RST
        // leaves a connection that only ever returns WouldBlock. Raft's
        // own timers re-elect around it but never close it, so without
        // this it would sit in the set forever, polled every tick.
        if got_bytes {
            conn.last_activity = now;
        } else if now.duration_since(conn.last_activity) > INBOUND_IDLE_TIMEOUT {
            debug!(peer = %conn.peer, "raft inbound link idle past timeout — reaping");
            return false;
        }

        // Extract every complete frame currently buffered.
        let mut consumed = 0;
        loop {
            match scan_frame(&conn.recv_buf[consumed..]) {
                Ok(FrameScan::Complete(envelope, used)) => {
                    consumed += used;
                    let msg = envelope.message;
                    // Identity binding: this connection authenticated as
                    // `conn.peer_id`, so it may not speak for any other
                    // node. Drop (don't tear down — a benign racing
                    // reconnect could momentarily carry a stale id) any
                    // frame whose `from` disagrees, so a peer cannot
                    // forge votes or messages as another node.
                    if msg.from != conn.peer_id {
                        debug!(
                            peer = %conn.peer,
                            authenticated_as = conn.peer_id,
                            claimed = msg.from,
                            "dropping raft frame with mismatched sender id"
                        );
                        continue;
                    }
                    // The supersession predicate itself lives inside
                    // `fence_if_superseded` (returns `None` for a
                    // non-superseding peer) so this call site cannot
                    // drift from the data-plane senders'.
                    if fence_on_supersession
                        && let Some(first) = context
                            .fence_state
                            .fence_if_superseded(envelope.tip.epoch, &context.shutdown)
                        && first
                    {
                        // warn!, not error!: an authenticated peer
                        // reporting a newer promotion is the cluster
                        // working as designed, not a malfunction.
                        warn!(
                            peer_epoch = envelope.tip.epoch,
                            our_epoch = context.fence_state.epoch(),
                            "fenced: a raft peer advertises a higher fencing epoch — this \
                             primary has been superseded; self-demoting and shutting down"
                        );
                    }
                    if is_vote_request(msg.msg_type())
                        && !vote_request_admitted(tip_ready, envelope.tip, local_tip)
                    {
                        // Refused because either our own tip isn't
                        // trustworthy yet or the candidate is behind our
                        // journal — see `vote_request_admitted`. Dropping
                        // is safe: it looks like packet loss to raft and
                        // can only delay an election.
                        debug!(
                            from = msg.from,
                            tip_ready,
                            candidate_tip = ?envelope.tip,
                            our_tip = ?local_tip,
                            "vote request filtered (tip not ready or candidate behind)"
                        );
                        continue;
                    }
                    node.step(msg);
                }
                Ok(FrameScan::Incomplete) => break,
                Err(e) => {
                    debug!(peer = %conn.peer, error = %e, "raft frame error — dropping link");
                    return false;
                }
            }
        }
        if consumed > 0 {
            conn.recv_buf.drain(..consumed);
        }
        true
    });
}

/// Detect closed outbound links (peers never send on them, so any
/// readable event is either EOF or an error) and flush pending egress.
fn poll_outbound_liveness(links: &mut HashMap<u64, PeerLink>) {
    for (&peer_id, link) in links.iter_mut() {
        let Some(stream) = link.stream.as_mut() else {
            continue;
        };
        let mut probe = [0u8; 64];
        let dead = match stream.read(&mut probe) {
            // Peers never write on our outbound link, so data here is a
            // protocol violation; treat like EOF.
            Ok(_) => true,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => false,
            Err(_) => true,
        };
        if dead {
            debug!(peer_id, "raft outbound link closed");
            link.stream = None;
            link.out_buf.clear();
            link.next_dial = Instant::now();
            continue;
        }
        flush_link(peer_id, link);
    }
}

/// Drain raft readies and route messages onto peer links. Returns
/// `None` on a storage failure (raft must stop), otherwise whether the
/// applied membership registry changed while draining.
fn drain_node(
    node: &mut ControlNode,
    links: &mut HashMap<u64, PeerLink>,
    context: &RaftDriverContext,
) -> Option<bool> {
    let tip = local_tip(context);
    // Chain hash rides the envelope for divergence *diagnostics* only (it
    // never affects vote filtering). Publishing the per-fsync BLAKE3 hash
    // to the control plane cheaply needs a seqlock hook the journal stage
    // doesn't expose yet, so it stays zeroed for now.
    let chain_hash = [0u8; 32];
    let mut registry_changed = false;
    while node.has_ready() {
        let drained = match node.drain_ready() {
            Ok(d) => d,
            Err(e) => {
                // Genuine server malfunction (fsync/rename failure on
                // the raft state file) — never client-triggerable.
                error!(
                    error = %e,
                    "control-plane raft storage failure — raft stops; trading continues without election support"
                );
                return None;
            }
        };
        registry_changed |= drained.registry_changed;
        for msg in drained.messages {
            let Some(link) = links.get_mut(&msg.to) else {
                debug!(to = msg.to, "raft message for unknown peer dropped");
                continue;
            };
            if link.stream.is_none() {
                // Down link: raft treats it as message loss and retries
                // via its own timers.
                continue;
            }
            if link.out_buf.len() > MAX_OUT_BUFFER {
                debug!(
                    peer_id = msg.to,
                    "raft egress buffer overflow — resetting link"
                );
                link.stream = None;
                link.out_buf.clear();
                link.next_dial = Instant::now();
                continue;
            }
            let to = msg.to;
            if let Err(e) = encode_frame(tip, &chain_hash, &msg, &mut link.out_buf) {
                // Backstop for a message larger than the frame cap
                // (Config.max_size_per_msg keeps appends well under it,
                // so this should not happen). Drop it rather than frame
                // a frame the peer will reject: a rejected oversized
                // frame resets the link, and raft would resend the
                // identical message, looping the link down forever.
                // Dropping keeps the link up; raft makes progress with
                // smaller messages.
                warn!(to, error = %e, "dropping oversized raft message");
                continue;
            }
            flush_link(to, link);
        }
    }
    Some(registry_changed)
}

/// Try to push a link's buffered egress onto the socket. Partial
/// writes keep the remainder buffered; hard errors reset the link.
fn flush_link(peer_id: u64, link: &mut PeerLink) {
    let Some(stream) = link.stream.as_mut() else {
        return;
    };
    while !link.out_buf.is_empty() {
        match stream.write(&link.out_buf) {
            Ok(0) => break,
            Ok(n) => {
                link.out_buf.drain(..n);
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => {
                debug!(peer_id, error = %e, "raft peer write error — resetting link");
                link.stream = None;
                link.out_buf.clear();
                link.next_dial = Instant::now();
                return;
            }
        }
    }
}

/// Whether an inbound vote request should be delivered to raft.
///
/// Refused (returns `false`) when either our own tip isn't trustworthy
/// yet (`!tip_ready` — a replica mid-recovery advertises epoch 0 and
/// must not vote until it knows its real tip) or the `candidate` is
/// behind our `local` journal (the recency rule). Both are safe to
/// enforce by dropping the request: to raft it is indistinguishable
/// from packet loss, so it can only delay an election, never split the
/// vote.
fn vote_request_admitted(tip_ready: bool, candidate: JournalTip, local: JournalTip) -> bool {
    tip_ready && candidate_is_current(candidate, local)
}

/// The durability mode the auto-promotion refusal judges: the mode
/// the *primary* last advertised on the replication stream — that is
/// the gate acked orders actually passed through — falling back to
/// this node's own configured mode while no primary has ever been
/// observed (`ACKING_MODE_UNKNOWN`), which is exactly the
/// pre-propagation behavior. `None` for an unrecognised byte (e.g. a
/// newer node's mode) — the caller refuses on it.
fn effective_acking_mode(observed: u8, local_fallback: u8) -> Option<DurabilityMode> {
    let byte = if observed == crate::durability_policy::ACKING_MODE_UNKNOWN {
        local_fallback
    } else {
        observed
    };
    DurabilityMode::from_u8(byte)
}

/// Everything the auto-promotion rule looks at, snapshotted from the
/// shared atomics so the decision itself is a pure function
/// ([`auto_promotion_decision`]) the tests can drive exhaustively.
struct AutoPromotionInputs {
    /// Journal recovery has seeded the fence epoch and advertised tip.
    tip_ready: bool,
    /// This node has been fenced (superseded) — it must never lead.
    fenced: bool,
    /// The acking durability mode ([`effective_acking_mode`]), `None`
    /// for an unrecognised byte.
    durability_mode: Option<DurabilityMode>,
    /// The replication link to the primary is authenticated and live.
    primary_link_up: bool,
    /// The term this node was elected at.
    term: u64,
    /// The fencing epoch currently in force.
    fence_epoch: u64,
}

/// Should a replica that just won a control-plane election promote
/// itself? `Err` carries the operator-facing refusal reason.
///
/// The election itself is the data-safety proof: the recency filter
/// means a quorum of voters held no more data than this node
/// ([`vote_request_admitted`]). The rules here cover what an election
/// cannot prove:
///
/// - `tip_ready` / `fenced` — the tip that won the election must be
///   real, and a superseded node must stay down.
/// - `primary_link_up` — a live authenticated link means the primary
///   is alive; leadership may still land here (e.g. the previous raft
///   leader was a *replica* whose process died), and promoting would
///   depose a healthy primary.
/// - `local` durability — acks in `local` mode never waited for this
///   replica, so no election can prove it holds every acked order.
///   Failover stays a manual, eyes-on decision.
/// - `term > fence_epoch` — the promotion journals `epoch = term`, so
///   the term must be strictly newer than every epoch already in
///   force; two auto-promotions from different elections then always
///   allocate distinct epochs and the newer fences the older. Epochs
///   outrunning terms (a history of manual promotions) breaks the
///   alignment until enough elections pass — refuse rather than risk
///   an epoch collision.
fn auto_promotion_decision(inputs: &AutoPromotionInputs) -> Result<(), &'static str> {
    if !inputs.tip_ready {
        return Err("journal recovery has not seeded this node's tip yet");
    }
    if inputs.fenced {
        return Err("node is fenced (superseded by a newer primary)");
    }
    if inputs.primary_link_up {
        return Err("replication link to the primary is up — refusing to depose a live primary");
    }
    match inputs.durability_mode {
        Some(DurabilityMode::Local) => {
            return Err(
                "the primary acks under `local` durability — an election win cannot prove \
                 this node holds every acked order; promote manually if the lag is acceptable",
            );
        }
        None => return Err("acking durability mode is unrecognised"),
        Some(DurabilityMode::Hybrid | DurabilityMode::DurablyReplicated) => {}
    }
    if inputs.term <= inputs.fence_epoch {
        return Err(
            "election term is not above the fencing epoch (manual promotions outran raft \
             terms) — promote manually; the alignment heals as terms advance",
        );
    }
    Ok(())
}

/// Act on leadership: if this node is a replica, currently leads, and
/// [`auto_promotion_decision`] allows it, file a promotion request
/// carrying the election term (the new tenure's fencing epoch).
/// Standing refusals are logged once per term, not once per poll.
fn consider_auto_promotion(
    node: &ControlNode,
    config: &RaftDriverConfig,
    context: &RaftDriverContext,
    last_refused_term: &mut u64,
) {
    if !config.auto_promote {
        return;
    }
    let Some(replica) = &context.replica else {
        return; // genesis primary — nothing to promote
    };
    if node.role() != StateRole::Leader || replica.promote.is_requested() {
        return;
    }
    let term = node.term();
    let inputs = AutoPromotionInputs {
        tip_ready: context.tip_ready.load(Ordering::Acquire),
        fenced: context.fence_state.is_fenced(),
        durability_mode: effective_acking_mode(
            replica.primary_acking_mode.load(Ordering::Acquire),
            context.durability_mode.load(Ordering::Relaxed),
        ),
        primary_link_up: replica.primary_link_up.load(Ordering::Acquire),
        term,
        fence_epoch: context.fence_state.epoch(),
    };
    match auto_promotion_decision(&inputs) {
        Ok(()) => {
            // `request` can only lose to a racing manual PROMOTE; either
            // way a promotion is now in flight.
            if replica.promote.request(term) {
                info!(
                    node_id = node.id(),
                    term, "elected leader — auto-promoting this replica"
                );
            }
        }
        Err(reason) => {
            if *last_refused_term != term {
                *last_refused_term = term;
                warn!(
                    node_id = node.id(),
                    term, reason, "elected leader but refusing auto-promotion"
                );
            }
        }
    }
}

/// The journal tip this node advertises: the fencing epoch plus the
/// advertised journal sequence (see [`RaftDriverContext::journal_tip`]).
/// Trustworthy only once `tip_ready` is set — the vote filter checks
/// that flag separately ([`vote_request_admitted`]).
fn local_tip(context: &RaftDriverContext) -> JournalTip {
    JournalTip {
        epoch: context.fence_state.epoch(),
        last_sequence: context.journal_tip.load().get(),
    }
}

/// Fold the applied membership registry into the driver's live wiring:
/// dial targets (`links`), the pinned-identity map, and the data-plane
/// directory. Static `--raft-peer` flags remain the bootstrap floor;
/// per node id, an applied record supersedes them — that is the whole
/// point of the registry (divergent or stale static configs converge
/// on the leader-serialized log).
fn sync_registry(
    node: &ControlNode,
    config: &RaftDriverConfig,
    links: &mut HashMap<u64, PeerLink>,
    pubkey_to_id: &Arc<RwLock<HashMap<[u8; 32], u64>>>,
    directory: &ClusterDirectory,
) {
    for record in node.registry().iter() {
        if record.node_id == config.node_id {
            continue;
        }
        match links.entry(record.node_id) {
            std::collections::hash_map::Entry::Occupied(mut e) => {
                let link = e.get_mut();
                if link.addr != record.raft_addr {
                    // An in-flight dial to the old address may still
                    // land a stale stream; it self-heals on the next
                    // write failure or idle reap.
                    info!(
                        peer_id = record.node_id,
                        addr = %record.raft_addr,
                        "raft peer moved — re-dialing at its announced address"
                    );
                    link.addr = record.raft_addr;
                    link.stream = None;
                    link.out_buf.clear();
                    link.next_dial = Instant::now();
                }
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                info!(
                    peer_id = record.node_id,
                    addr = %record.raft_addr,
                    "raft peer discovered via the membership registry"
                );
                v.insert(PeerLink {
                    addr: record.raft_addr,
                    stream: None,
                    out_buf: Vec::new(),
                    next_dial: Instant::now(),
                    dialing: false,
                });
            }
        }
    }

    // Prune dial targets that are neither a static bootstrap peer nor a
    // current registry record — e.g. a runtime-added node whose record
    // was reclaimed by `RAFT-REMOVE-VOTER` (an orphaned or decommissioned
    // node). Static `--raft-peer`s stay the floor, so genesis dialing is
    // untouched before any records are announced. `HashSet` for O(1)
    // membership over the tiny peer set.
    let live: std::collections::HashSet<u64> = config
        .peers
        .iter()
        .map(|p| p.id)
        .chain(node.registry().iter().map(|r| r.node_id))
        .collect();
    links.retain(|id, _| live.contains(id));

    // Identity pins: static config as the floor, applied records
    // superseding per id (an announced key rotation replaces the
    // boot-time pin for that node).
    let mut map: HashMap<[u8; 32], u64> =
        config.peers.iter().map(|p| (p.public_key, p.id)).collect();
    for record in node.registry().iter() {
        if record.node_id != config.node_id {
            map.insert(record.public_key, record.node_id);
        }
    }
    match pubkey_to_id.write() {
        Ok(mut guard) => *guard = map,
        Err(_) => warn!("raft identity map lock poisoned — pins not updated"),
    }

    directory.update(node.registry());
}

/// Publish term/leader/role to the health gauges.
fn publish_status(node: &ControlNode, status: &RaftStatus) {
    let role = match node.role() {
        StateRole::Follower => RaftStatus::ROLE_FOLLOWER,
        StateRole::PreCandidate => RaftStatus::ROLE_PRE_CANDIDATE,
        StateRole::Candidate => RaftStatus::ROLE_CANDIDATE,
        StateRole::Leader => RaftStatus::ROLE_LEADER,
    };
    let prev_role = status.role.swap(role, Ordering::Relaxed);
    status.term.store(node.term(), Ordering::Relaxed);
    status
        .leader_id
        .store(node.leader_id().unwrap_or(0), Ordering::Relaxed);
    if prev_role != role && role == RaftStatus::ROLE_LEADER {
        info!(
            node_id = node.id(),
            term = node.term(),
            "elected control-plane raft leader"
        );
    }
}

#[cfg(test)]
mod voter_change_tests {
    //! Unit tests for the voter-change state machine, driven against a
    //! mock [`VoterOps`] so the two-stage sequencing, safety rails, and
    //! deadline are exercised without a live raft cluster or sockets.
    //! Proposals only *record* the call; the test then mutates the mock
    //! to simulate raft applying them, giving precise control over
    //! timing.

    use super::*;
    use std::collections::HashMap;

    /// A scriptable stand-in for a `ControlNode`. Registry is a
    /// `HashMap<node_id, MemberRecord>` — a directory keyed by id,
    /// matching the real registry's shape closely enough for the rails.
    #[derive(Default)]
    struct MockNode {
        leader: Option<u64>,
        voters: Vec<u64>,
        records: HashMap<u64, MemberRecord>,
        proposed_members: Vec<u64>,
        proposed_adds: Vec<u64>,
        proposed_removes: Vec<u64>,
    }

    /// A directory record with the given id/key and a fixed raft address —
    /// the address only differs in the re-address test, which overrides it.
    fn mock_record(node_id: u64, key: [u8; 32]) -> MemberRecord {
        MemberRecord {
            node_id,
            raft_addr: "127.0.0.1:9000".parse().unwrap(),
            replication_addr: None,
            order_entry_addr: None,
            serving_epoch: None,
            public_key: key,
        }
    }

    impl VoterOps for MockNode {
        fn leader_id(&self) -> Option<u64> {
            self.leader
        }
        fn voters(&self) -> Vec<u64> {
            self.voters.clone()
        }
        fn registry_record(&self, node_id: u64) -> Option<MemberRecord> {
            self.records.get(&node_id).cloned()
        }
        fn registry_id_for_key(&self, key: &[u8; 32]) -> Option<u64> {
            self.records
                .values()
                .find(|r| &r.public_key == key)
                .map(|r| r.node_id)
        }
        fn propose_member(&mut self, record: &MemberRecord) -> bool {
            self.proposed_members.push(record.node_id);
            true
        }
        fn propose_add_voter(&mut self, node_id: u64) -> bool {
            self.proposed_adds.push(node_id);
            true
        }
        fn propose_remove_voter(&mut self, node_id: u64) -> bool {
            self.proposed_removes.push(node_id);
            true
        }
    }

    fn add_request(
        node_id: u64,
        key: [u8; 32],
    ) -> (VoterChangeRequest, Receiver<Result<Vec<u64>, String>>) {
        let (tx, rx) = channel();
        (
            VoterChangeRequest {
                change: VoterChange::Add {
                    node_id,
                    raft_addr: "127.0.0.1:9000".parse().unwrap(),
                    public_key: key,
                },
                reply: tx,
            },
            rx,
        )
    }

    fn remove_request(node_id: u64) -> (VoterChangeRequest, Receiver<Result<Vec<u64>, String>>) {
        let (tx, rx) = channel();
        (
            VoterChangeRequest {
                change: VoterChange::Remove { node_id },
                reply: tx,
            },
            rx,
        )
    }

    #[test]
    fn rejects_node_id_zero() {
        let mut node = MockNode::default();
        let (req, rx) = remove_request(0);
        assert!(begin_voter_change(&mut node, req, Instant::now()).is_none());
        assert!(rx.try_recv().unwrap().is_err());
    }

    #[test]
    fn rejects_key_pinned_to_a_different_id() {
        let mut node = MockNode {
            records: HashMap::from([(7, mock_record(7, [0xAB; 32]))]),
            ..Default::default()
        };
        // Try to add id 4 with a key already pinned to id 7.
        let (req, rx) = add_request(4, [0xAB; 32]);
        assert!(begin_voter_change(&mut node, req, Instant::now()).is_none());
        assert!(rx.try_recv().unwrap().is_err());
        assert!(node.proposed_members.is_empty());
    }

    #[test]
    fn add_is_idempotent_when_already_a_voter_with_record() {
        let mut node = MockNode {
            voters: vec![1, 2, 4],
            records: HashMap::from([(4, mock_record(4, [0xAB; 32]))]),
            ..Default::default()
        };
        // Same key and address as the record → idempotent success.
        let (req, rx) = add_request(4, [0xAB; 32]);
        assert!(begin_voter_change(&mut node, req, Instant::now()).is_none());
        assert_eq!(rx.try_recv().unwrap().unwrap(), vec![1, 2, 4]);
    }

    #[test]
    fn rejects_rekey_of_an_existing_voter() {
        // Node 4 is a voter recorded under key 0xAB; ADD-VOTER with a
        // *different* key must be refused (not a false OK), steering the
        // operator to remove + re-add.
        let mut node = MockNode {
            voters: vec![1, 2, 4],
            records: HashMap::from([(4, mock_record(4, [0xAB; 32]))]),
            ..Default::default()
        };
        let (req, rx) = add_request(4, [0xCD; 32]);
        assert!(begin_voter_change(&mut node, req, Instant::now()).is_none());
        let err = rx.try_recv().unwrap().unwrap_err();
        assert!(
            err.contains("different identity"),
            "unexpected error: {err}"
        );
        assert!(node.proposed_members.is_empty(), "must not seed a record");
        assert!(node.proposed_adds.is_empty());
    }

    #[test]
    fn rejects_readdress_of_an_existing_voter() {
        // Same key, different raft address → still a re-address ADD cannot
        // perform in place; refused rather than silently succeeding.
        let mut record = mock_record(4, [0xAB; 32]);
        record.raft_addr = "127.0.0.1:7777".parse().unwrap();
        let mut node = MockNode {
            voters: vec![1, 2, 4],
            records: HashMap::from([(4, record)]),
            ..Default::default()
        };
        // add_request uses 127.0.0.1:9000 — a different address.
        let (req, rx) = add_request(4, [0xAB; 32]);
        assert!(begin_voter_change(&mut node, req, Instant::now()).is_none());
        assert!(rx.try_recv().unwrap().is_err());
        assert!(node.proposed_members.is_empty());
    }

    #[test]
    fn remove_is_idempotent_when_absent() {
        // Node 9 is neither a voter nor in the registry → truly gone, so
        // the remove replies OK without proposing anything.
        let mut node = MockNode {
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        let (req, rx) = remove_request(9);
        assert!(begin_voter_change(&mut node, req, Instant::now()).is_none());
        assert_eq!(rx.try_recv().unwrap().unwrap(), vec![1, 2, 3]);
        assert!(node.proposed_removes.is_empty());
    }

    #[test]
    fn refuses_to_remove_the_live_leader() {
        let mut node = MockNode {
            leader: Some(2),
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        let (req, rx) = remove_request(2);
        assert!(begin_voter_change(&mut node, req, Instant::now()).is_none());
        assert!(rx.try_recv().unwrap().is_err());
        assert!(node.proposed_removes.is_empty());
    }

    #[test]
    fn refuses_to_remove_the_last_voter() {
        let mut node = MockNode {
            leader: Some(9),
            voters: vec![1],
            ..Default::default()
        };
        let (req, rx) = remove_request(1);
        assert!(begin_voter_change(&mut node, req, Instant::now()).is_none());
        assert!(rx.try_recv().unwrap().is_err());
    }

    #[test]
    fn add_flow_seeds_record_then_conf_change_then_replies() {
        let mut node = MockNode {
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        let now = Instant::now();
        let (req, rx) = add_request(4, [0xCD; 32]);
        // Stage 1: begin proposes the seed record only.
        let mut pending = begin_voter_change(&mut node, req, now);
        assert!(pending.is_some());
        assert_eq!(node.proposed_members, vec![4]);
        assert!(node.proposed_adds.is_empty());
        assert!(rx.try_recv().is_err(), "must not reply yet");

        // Advance before the record applies: nothing changes.
        advance_voter_change(&mut node, &mut pending, now);
        assert!(node.proposed_adds.is_empty());

        // Simulate the seed record applying → advance proposes AddNode.
        node.records.insert(4, mock_record(4, [0xCD; 32]));
        advance_voter_change(&mut node, &mut pending, now);
        assert_eq!(node.proposed_adds, vec![4]);
        assert!(pending.is_some(), "still waiting for the conf change");

        // Simulate the AddNode applying → advance replies OK and clears.
        node.voters.push(4);
        advance_voter_change(&mut node, &mut pending, now);
        assert!(pending.is_none());
        assert_eq!(rx.try_recv().unwrap().unwrap(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn remove_flow_proposes_conf_change_then_replies() {
        let mut node = MockNode {
            leader: Some(1),
            voters: vec![1, 2, 3],
            // A real voter carries a directory record; the committed
            // `RemoveNode` prunes it, and completion waits for that.
            records: HashMap::from([(3, mock_record(3, [0x33; 32]))]),
            ..Default::default()
        };
        let now = Instant::now();
        let (req, rx) = remove_request(3);
        let mut pending = begin_voter_change(&mut node, req, now);
        assert_eq!(node.proposed_removes, vec![3]);
        assert!(pending.is_some());

        // Voter gone but the record still lingers → not done yet.
        node.voters.retain(|v| *v != 3);
        advance_voter_change(&mut node, &mut pending, now);
        assert!(pending.is_some(), "waits for the record prune");
        assert!(rx.try_recv().is_err());

        // The committed RemoveNode also prunes the record → reply + clear.
        node.records.remove(&3);
        advance_voter_change(&mut node, &mut pending, now);
        assert!(pending.is_none());
        assert_eq!(rx.try_recv().unwrap().unwrap(), vec![1, 2]);
    }

    #[test]
    fn remove_reclaims_an_orphaned_record() {
        // Node 4 was seeded (record present) but its `AddNode` never
        // committed, so it is not a voter. `RAFT-REMOVE-VOTER 4` must not
        // short-circuit as idempotent — it proposes a `RemoveNode` that
        // no-ops on the voter set yet prunes the orphaned record.
        let mut node = MockNode {
            leader: Some(1),
            voters: vec![1, 2, 3],
            records: HashMap::from([(4, mock_record(4, [0x44; 32]))]),
            ..Default::default()
        };
        let now = Instant::now();
        let (req, rx) = remove_request(4);
        let mut pending = begin_voter_change(&mut node, req, now);
        assert!(pending.is_some(), "orphan must not short-circuit");
        assert_eq!(node.proposed_removes, vec![4]);

        // The RemoveNode prunes the record without touching the voter set.
        node.records.remove(&4);
        advance_voter_change(&mut node, &mut pending, now);
        assert!(pending.is_none());
        assert_eq!(rx.try_recv().unwrap().unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn deadline_replies_error_and_clears() {
        let mut node = MockNode {
            leader: Some(1),
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        let now = Instant::now();
        let (req, rx) = remove_request(3);
        let mut pending = begin_voter_change(&mut node, req, now);
        assert!(pending.is_some());

        // Never applies; advance past the deadline.
        advance_voter_change(&mut node, &mut pending, now + VOTER_CHANGE_DEADLINE);
        assert!(pending.is_none());
        assert!(rx.try_recv().unwrap().is_err());
    }

    #[test]
    fn re_proposes_the_current_stage_on_the_retry_timer() {
        let mut node = MockNode {
            leader: Some(1),
            voters: vec![1, 2, 3],
            ..Default::default()
        };
        let now = Instant::now();
        let (req, _rx) = remove_request(3);
        let mut pending = begin_voter_change(&mut node, req, now);
        assert_eq!(node.proposed_removes, vec![3]);

        // Before the retry interval: no re-propose.
        advance_voter_change(&mut node, &mut pending, now);
        assert_eq!(node.proposed_removes, vec![3]);

        // After it: one more proposal.
        advance_voter_change(&mut node, &mut pending, now + ANNOUNCE_RETRY_INTERVAL);
        assert_eq!(node.proposed_removes, vec![3, 3]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    #[test]
    fn inbound_cap_scales_with_peers_and_has_a_floor() {
        // Floor applies for tiny clusters.
        assert_eq!(inbound_cap(0), INBOUND_CAP_FLOOR);
        assert_eq!(inbound_cap(1), INBOUND_CAP_FLOOR);
        // Scales past the floor for larger ones.
        assert_eq!(inbound_cap(5), 5 * INBOUND_SLACK);
        assert!(inbound_cap(100) >= 100);
    }

    fn oe(s: &str) -> SocketAddr {
        s.parse().expect("addr")
    }

    fn claim_record(
        node_id: u64,
        serving_epoch: Option<u64>,
        order_entry: Option<&str>,
    ) -> MemberRecord {
        MemberRecord {
            node_id,
            raft_addr: format!("127.0.0.1:{}", 7000 + node_id)
                .parse()
                .expect("addr"),
            replication_addr: None,
            order_entry_addr: order_entry.map(oe),
            serving_epoch,
            public_key: [node_id as u8; 32],
        }
    }

    fn directory_of(records: &[MemberRecord]) -> Arc<ClusterDirectory> {
        let mut registry = Registry::default();
        for r in records {
            assert!(registry.apply(&r.encode()), "record must apply");
        }
        let dir = Arc::new(ClusterDirectory::default());
        dir.update(&registry);
        dir
    }

    fn replica_signals(promote_requested: bool) -> ReplicaSignals {
        let promote = PromotionRequest::new();
        if promote_requested {
            assert!(promote.request(PromotionRequest::MANUAL));
        }
        ReplicaSignals {
            promote,
            primary_link_up: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            primary_acking_mode: Arc::new(std::sync::atomic::AtomicU8::new(0)),
        }
    }

    #[test]
    fn serving_primary_resolves_highest_epoch_then_lowest_id() {
        // Empty directory: no serving primary.
        assert_eq!(directory_of(&[]).serving_primary(), None);

        // A single claim resolves to itself.
        let dir = directory_of(&[claim_record(2, Some(5), Some("10.0.0.2:80"))]);
        assert_eq!(dir.serving_primary(), Some((2, oe("10.0.0.2:80"))));

        // Competing claims: the highest epoch wins regardless of node-id
        // order — here the lower id holds the older epoch and must lose,
        // so announcement/id order cannot override fencing order.
        let dir = directory_of(&[
            claim_record(1, Some(4), Some("10.0.0.1:80")),
            claim_record(3, Some(9), Some("10.0.0.3:80")),
        ]);
        assert_eq!(dir.serving_primary(), Some((3, oe("10.0.0.3:80"))));

        // Tie on epoch (two manual promotions colliding): the lower id
        // wins so every node resolves the same primary.
        let dir = directory_of(&[
            claim_record(5, Some(7), Some("10.0.0.5:80")),
            claim_record(2, Some(7), Some("10.0.0.2:80")),
        ]);
        assert_eq!(dir.serving_primary(), Some((2, oe("10.0.0.2:80"))));

        // The highest-epoch claimant announced no client address — it is
        // skipped in favour of a lower claimant that did.
        let dir = directory_of(&[
            claim_record(4, Some(99), None),
            claim_record(1, Some(3), Some("10.0.0.1:80")),
        ]);
        assert_eq!(dir.serving_primary(), Some((1, oe("10.0.0.1:80"))));

        // No claim anywhere: nobody serves.
        let dir = directory_of(&[claim_record(1, None, Some("10.0.0.1:80"))]);
        assert_eq!(dir.serving_primary(), None);
    }

    #[test]
    fn serving_primary_accessor_excludes_self() {
        let records = [
            claim_record(2, Some(8), Some("10.0.0.2:80")),
            claim_record(3, Some(4), Some("10.0.0.3:80")),
        ];
        let dir = directory_of(&records);

        // Node 3 resolves the winner (node 2) — not itself.
        let follow = LeaderFollow {
            self_node_id: 3,
            status: Arc::new(RaftStatus::new(3)),
            directory: Arc::clone(&dir),
        };
        assert_eq!(
            follow.serving_primary_order_entry_addr(),
            Some(oe("10.0.0.2:80"))
        );

        // Node 2 IS the winner — a redirecting replica must never point a
        // client at itself, so the accessor returns None.
        let follow_self = LeaderFollow {
            self_node_id: 2,
            status: Arc::new(RaftStatus::new(2)),
            directory: dir,
        };
        assert_eq!(follow_self.serving_primary_order_entry_addr(), None);
    }

    #[test]
    fn serving_claim_tracks_role_and_epoch() {
        // Genesis primary (no replica signals) always claims, at the
        // current fence epoch.
        assert_eq!(serving_claim(None, 12), Some(12));
        // A plain replica claims nothing.
        assert_eq!(serving_claim(Some(&replica_signals(false)), 12), None);
        // Once promotion is requested it claims at the fence epoch.
        assert_eq!(serving_claim(Some(&replica_signals(true)), 12), Some(12));
    }

    #[test]
    fn vote_admitted_only_when_tip_ready_and_candidate_current() {
        let ours = JournalTip {
            epoch: 5,
            last_sequence: 100,
        };
        let ahead = JournalTip {
            epoch: 5,
            last_sequence: 200,
        };
        let behind = JournalTip {
            epoch: 5,
            last_sequence: 10,
        };
        // Ready + caught-up candidate: admitted.
        assert!(vote_request_admitted(true, ahead, ours));
        assert!(vote_request_admitted(true, ours, ours));
        // Ready but candidate behind: refused (recency rule).
        assert!(!vote_request_admitted(true, behind, ours));
        // Not ready: refused regardless of the candidate — a replica
        // mid-recovery advertising epoch 0 must not grant votes.
        assert!(!vote_request_admitted(false, ahead, ours));
    }

    /// Baseline inputs that PASS every auto-promotion rule; each test
    /// flips exactly one field to isolate the refusal it exercises.
    fn promotable() -> AutoPromotionInputs {
        AutoPromotionInputs {
            tip_ready: true,
            fenced: false,
            durability_mode: Some(DurabilityMode::Hybrid),
            primary_link_up: false,
            term: 7,
            fence_epoch: 3,
        }
    }

    #[test]
    fn auto_promotion_allowed_at_baseline() {
        assert_eq!(auto_promotion_decision(&promotable()), Ok(()));
        assert_eq!(
            auto_promotion_decision(&AutoPromotionInputs {
                durability_mode: Some(DurabilityMode::DurablyReplicated),
                ..promotable()
            }),
            Ok(())
        );
    }

    #[test]
    fn auto_promotion_refused_mid_recovery() {
        let inputs = AutoPromotionInputs {
            tip_ready: false,
            ..promotable()
        };
        assert!(auto_promotion_decision(&inputs).is_err());
    }

    #[test]
    fn auto_promotion_refused_when_fenced() {
        let inputs = AutoPromotionInputs {
            fenced: true,
            ..promotable()
        };
        assert!(auto_promotion_decision(&inputs).is_err());
    }

    #[test]
    fn auto_promotion_refused_while_primary_link_is_up() {
        // Leadership can land on a connected replica (e.g. the previous
        // raft leader was another replica whose process died); a live
        // link to the primary must veto the promotion.
        let inputs = AutoPromotionInputs {
            primary_link_up: true,
            ..promotable()
        };
        assert!(auto_promotion_decision(&inputs).is_err());
    }

    #[test]
    fn effective_acking_mode_prefers_the_observed_primary_mode() {
        use crate::durability_policy::ACKING_MODE_UNKNOWN;
        let local = DurabilityMode::Local.as_u8();
        let hybrid = DurabilityMode::Hybrid.as_u8();
        // Observed wins over the local fallback in both directions:
        // a primary retuned to `local` must veto promotion even though
        // this node is configured `hybrid` (the acked-order-loss case),
        // and a pre-staged `local` on this node must not veto when the
        // primary provably acked under `hybrid`.
        assert_eq!(
            effective_acking_mode(local, hybrid),
            Some(DurabilityMode::Local)
        );
        assert_eq!(
            effective_acking_mode(hybrid, local),
            Some(DurabilityMode::Hybrid)
        );
        // Never observed a primary: fall back to the local mode — the
        // pre-propagation behavior.
        assert_eq!(
            effective_acking_mode(ACKING_MODE_UNKNOWN, hybrid),
            Some(DurabilityMode::Hybrid)
        );
        // An unrecognised observed byte (newer node's mode) maps to
        // `None`, which the decision refuses on.
        assert_eq!(effective_acking_mode(200, hybrid), None);
    }

    #[test]
    fn auto_promotion_refused_in_local_durability() {
        let inputs = AutoPromotionInputs {
            durability_mode: Some(DurabilityMode::Local),
            ..promotable()
        };
        assert!(auto_promotion_decision(&inputs).is_err());
        let unknown = AutoPromotionInputs {
            durability_mode: None,
            ..promotable()
        };
        assert!(auto_promotion_decision(&unknown).is_err());
    }

    #[test]
    fn auto_promotion_refused_when_epochs_outran_terms() {
        // epoch == term collides with the tenure the epoch came from;
        // epoch > term would collide with a future one. Only a strictly
        // newer term may promote.
        for fence_epoch in [7, 8] {
            let inputs = AutoPromotionInputs {
                fence_epoch,
                ..promotable()
            };
            assert!(auto_promotion_decision(&inputs).is_err(), "{fence_epoch}");
        }
    }

    #[test]
    fn auth_slot_tracks_inflight_count() {
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let a = AuthSlot::acquire(&counter);
        let b = AuthSlot::acquire(&counter);
        assert_eq!(counter.load(Ordering::Acquire), 2);
        drop(a);
        assert_eq!(counter.load(Ordering::Acquire), 1);
        drop(b);
        assert_eq!(counter.load(Ordering::Acquire), 0);
    }

    /// Build one signing key per node plus a shared `AuthorizedKeys`
    /// table granting all of them `replication` permission.
    fn cluster_keys(ids: &[u64]) -> (HashMap<u64, SigningKey>, Arc<AuthorizedKeys>) {
        let mut keys = HashMap::new();
        let mut table = String::new();
        for &id in ids {
            let key = SigningKey::from_bytes(&[id as u8; 32]);
            let pub_b64 =
                base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
            table.push_str(&format!("replication {pub_b64} node-{id}\n"));
            keys.insert(id, key);
        }
        let table = AuthorizedKeys::parse(&table).expect("parse authorized_keys");
        (keys, Arc::new(table))
    }

    struct TestNode {
        status: Arc<RaftStatus>,
        /// Per-node shutdown flag (prod passes the process-wide flag;
        /// per-node here lets a test kill one driver cleanly).
        shutdown: Arc<AtomicBool>,
        _dir: tempfile::TempDir,
        handle: JoinHandle<()>,
    }

    impl TestNode {
        fn kill(self) {
            self.shutdown.store(true, Ordering::Release);
            self.handle.join().expect("driver thread panicked");
        }
    }

    /// Boot a full in-process cluster of raft drivers over loopback
    /// TCP, every node advertising journal tip 0.
    fn boot_cluster(ids: &[u64]) -> HashMap<u64, TestNode> {
        let tips: Vec<(u64, u64)> = ids.iter().map(|&id| (id, 0)).collect();
        boot_cluster_with_tips(&tips)
    }

    /// Boot a cluster with a fixed advertised journal tip per node
    /// (`(node_id, last_sequence)`), for recency-steering tests.
    fn boot_cluster_with_tips(tips: &[(u64, u64)]) -> HashMap<u64, TestNode> {
        let ids: Vec<u64> = tips.iter().map(|&(id, _)| id).collect();
        let ids = ids.as_slice();
        let (signing, authorized) = cluster_keys(ids);

        // Bind all listeners first so every node knows every address.
        let mut listeners = HashMap::new();
        let mut addrs = HashMap::new();
        for &id in ids {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            addrs.insert(id, listener.local_addr().expect("local addr"));
            listeners.insert(id, listener);
        }

        let mut nodes = HashMap::new();
        for &(id, tip) in tips {
            let dir = tempfile::tempdir().expect("tempdir");
            let status = Arc::new(RaftStatus::new(id));
            let shutdown = Arc::new(AtomicBool::new(false));
            let config = RaftDriverConfig {
                node_id: id,
                voters: ids.to_vec(),
                peers: ids
                    .iter()
                    .filter(|&&p| p != id)
                    .map(|&p| RaftPeer {
                        id: p,
                        addr: addrs[&p],
                        public_key: signing[&p].verifying_key().to_bytes(),
                    })
                    .collect(),
                dir: dir.path().to_path_buf(),
                advertise_raft_addr: addrs[&id],
                advertise_replication_addr: None,
                advertise_order_entry_addr: None,
                auto_promote: false,
            };
            let context = RaftDriverContext {
                signing_key: signing[&id].clone(),
                authorized_keys: Arc::clone(&authorized),
                fence_state: Arc::new(FenceState::new(0)),
                journal_tip: AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(tip)),
                // These test nodes act as always-recovered primaries.
                tip_ready: Arc::new(AtomicBool::new(true)),
                status: Arc::clone(&status),
                directory: Arc::new(ClusterDirectory::default()),
                durability_mode: Arc::new(AtomicU8::new(DurabilityMode::Hybrid.as_u8())),
                replica: None,
                // These nodes never receive voter changes; a disconnected
                // receiver just makes the drain a no-op.
                voter_changes: channel::<VoterChangeRequest>().1,
                shutdown: Arc::clone(&shutdown),
            };
            let handle =
                spawn_with_listener(listeners.remove(&id).expect("listener"), config, context)
                    .expect("spawn driver");
            nodes.insert(
                id,
                TestNode {
                    status,
                    shutdown,
                    _dir: dir,
                    handle,
                },
            );
        }
        nodes
    }

    fn wait_for_single_leader(
        nodes: &HashMap<u64, TestNode>,
        exclude: &[u64],
        deadline: Duration,
    ) -> u64 {
        let start = Instant::now();
        loop {
            let leaders: Vec<u64> = nodes
                .iter()
                .filter(|(id, _)| !exclude.contains(id))
                .filter(|(_, n)| n.status.role.load(Ordering::Relaxed) == RaftStatus::ROLE_LEADER)
                .map(|(id, _)| *id)
                .collect();
            if let [leader] = leaders.as_slice() {
                // All live nodes agree on the leader id.
                let agreed = nodes
                    .iter()
                    .filter(|(id, _)| !exclude.contains(id))
                    .all(|(_, n)| n.status.leader_id.load(Ordering::Relaxed) == *leader);
                if agreed {
                    return *leader;
                }
            }
            assert!(
                start.elapsed() < deadline,
                "no agreed leader within {deadline:?} (leaders seen: {leaders:?})"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Full-stack election over real sockets: three drivers, real auth,
    /// real fsyncs — exactly one leader, and every node agrees who it
    /// is.
    #[test]
    fn three_driver_cluster_elects_one_leader() {
        let nodes = boot_cluster(&[1, 2, 3]);
        let leader = wait_for_single_leader(&nodes, &[], Duration::from_secs(15));
        let term = nodes[&leader].status.term.load(Ordering::Relaxed);
        assert!(term >= 1);

        for (_, node) in nodes {
            node.kill();
        }
    }

    /// Kill the leader's driver; the surviving pair must elect a new
    /// leader at a strictly higher term (the future fencing epoch).
    #[test]
    fn surviving_quorum_elects_a_new_leader() {
        let mut nodes = boot_cluster(&[1, 2, 3]);
        let first = wait_for_single_leader(&nodes, &[], Duration::from_secs(15));
        let first_term = nodes[&first].status.term.load(Ordering::Relaxed);

        nodes.remove(&first).expect("leader node").kill();

        let second = wait_for_single_leader(&nodes, &[first], Duration::from_secs(20));
        assert_ne!(second, first);
        let second_term = nodes[&second].status.term.load(Ordering::Relaxed);
        assert!(
            second_term > first_term,
            "new tenure must carry a higher term ({second_term} vs {first_term})"
        );

        for (_, node) in nodes {
            node.kill();
        }
    }

    /// Recency steering over real sockets: a node whose advertised
    /// journal tip is behind can never assemble a quorum, so leadership
    /// always lands on a most-caught-up node — including across a
    /// re-election after the leader dies. This is the property
    /// auto-promotion relies on to never promote a lagging replica.
    #[test]
    fn behind_node_never_wins_an_election() {
        // Nodes 1 and 2 hold seq 100; node 3 is behind at seq 10. Node 3
        // can only win with a grant from 1 or 2, and both drop its vote
        // requests (candidate tip 10 < local tip 100).
        let mut nodes = boot_cluster_with_tips(&[(1, 100), (2, 100), (3, 10)]);
        let first = wait_for_single_leader(&nodes, &[], Duration::from_secs(15));
        assert_ne!(first, 3, "the behind node must not win the first election");

        // Kill the leader: the survivors are one caught-up node and the
        // behind node — only the caught-up one can win.
        nodes.remove(&first).expect("leader node").kill();
        let second = wait_for_single_leader(&nodes, &[first], Duration::from_secs(20));
        assert_ne!(second, 3, "the behind node must not win the re-election");

        for (_, node) in nodes {
            node.kill();
        }
    }
}
