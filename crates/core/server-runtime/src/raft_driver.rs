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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use tracing::{debug, error, info, warn};

use melin_app::auth::AuthorizedKeys;
use melin_raft::recency::{JournalTip, candidate_is_current, is_vote_request};
use melin_raft::wire::{FrameScan, encode_frame, scan_frame};
use melin_raft::{ControlNode, StateRole};
use melin_transport_core::fence::FenceState;
use melin_transport_core::health::RaftStatus;

use crate::replication::auth::{authenticate_replica, authenticate_with_primary};

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

/// Static configuration for one node's control-plane raft.
#[derive(Debug, Clone)]
pub struct RaftDriverConfig {
    /// This node's raft id.
    pub node_id: u64,
    /// The full cluster membership (including this node) — every node
    /// must be configured with the same set.
    pub voters: Vec<u64>,
    /// Peer id → raft RPC address, excluding this node.
    pub peers: Vec<(u64, SocketAddr)>,
    /// Directory for the durable raft state file.
    pub dir: PathBuf,
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
    /// advertised on every frame. The sequence half is wired in the
    /// auto-promotion step; until then all nodes advertise sequence 0
    /// and the recency filter degrades to epoch-only comparison.
    pub fence_state: Arc<FenceState>,
    /// Election observability published to the health endpoint.
    pub status: Arc<RaftStatus>,
    /// Process-wide shutdown flag.
    pub shutdown: Arc<AtomicBool>,
}

/// An authenticated socket delivered by a helper auth thread.
enum AuthedSocket {
    /// Inbound peer link (read-only for the driver).
    Inbound(TcpStream, SocketAddr),
    /// Outbound link to `peer_id` (write-only for the driver).
    Outbound(u64, TcpStream),
    /// An outbound dial/auth attempt failed; retry after backoff.
    OutboundFailed(u64),
}

/// One live inbound connection.
struct InboundConn {
    stream: TcpStream,
    peer: SocketAddr,
    recv_buf: Vec<u8>,
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

fn run(
    listener: TcpListener,
    mut node: ControlNode,
    config: RaftDriverConfig,
    context: RaftDriverContext,
) {
    let (authed_tx, authed_rx): (Sender<AuthedSocket>, Receiver<AuthedSocket>) = channel();
    let mut inbound: Vec<InboundConn> = Vec::new();
    let mut links: HashMap<u64, PeerLink> = config
        .peers
        .iter()
        .map(|&(id, addr)| {
            (
                id,
                PeerLink {
                    addr,
                    stream: None,
                    out_buf: Vec::new(),
                    next_dial: Instant::now(),
                    dialing: false,
                },
            )
        })
        .collect();

    let mut next_tick = Instant::now() + TICK_INTERVAL;
    publish_status(&node, &context.status);

    loop {
        if context.shutdown.load(Ordering::Relaxed) {
            return;
        }
        let now = Instant::now();

        // 1. Raft clock.
        if now >= next_tick {
            node.tick();
            // Deadline-anchored (not `now + TICK`) so a slow iteration
            // doesn't stretch the logical clock.
            next_tick += TICK_INTERVAL;
        }

        // 2. New inbound connections → helper auth threads.
        accept_inbound(&listener, &context, &authed_tx);

        // 3. Freshly authenticated sockets and dial results.
        drain_authed(&authed_rx, &mut inbound, &mut links);

        // 4. Kick off outbound dials that are due.
        dial_due_peers(&mut links, &config, &context, &authed_tx, now);

        // 5. Ingress: read peers, extract frames, filter, step raft.
        read_inbound(&mut inbound, &mut node, &context);
        poll_outbound_liveness(&mut links);

        // 6. Drain raft readies (fsyncs inside) and route the egress.
        if !drain_node(&mut node, &mut links, &context) {
            // Storage failure: raft is inoperable by contract. The
            // control plane stops; trading continues on the data plane.
            return;
        }

        publish_status(&node, &context.status);
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Accept any pending inbound connections and hand each to a helper
/// thread for the blocking auth handshake.
fn accept_inbound(
    listener: &TcpListener,
    context: &RaftDriverContext,
    authed_tx: &Sender<AuthedSocket>,
) {
    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                debug!(peer = %peer, "raft peer connection accepted — authenticating");
                let keys = Arc::clone(&context.authorized_keys);
                let tx = authed_tx.clone();
                let spawned = std::thread::Builder::new()
                    .name("raft-peer-auth".into())
                    .spawn(move || {
                        let mut stream = stream;
                        stream.set_read_timeout(Some(ACCEPT_AUTH_TIMEOUT)).ok();
                        stream.set_write_timeout(Some(ACCEPT_AUTH_TIMEOUT)).ok();
                        match authenticate_replica(&mut stream, &keys) {
                            Ok(()) => {
                                // Receiver gone ⇒ the driver exited; the
                                // socket just drops, which is the correct
                                // teardown either way.
                                let _ = tx.send(AuthedSocket::Inbound(stream, peer));
                            }
                            Err(e) => {
                                debug!(peer = %peer, error = %e, "raft peer auth failed");
                            }
                        }
                    });
                if let Err(e) = spawned {
                    warn!(error = %e, "failed to spawn raft peer auth thread");
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

/// Absorb helper-thread results into the live connection sets.
fn drain_authed(
    authed_rx: &Receiver<AuthedSocket>,
    inbound: &mut Vec<InboundConn>,
    links: &mut HashMap<u64, PeerLink>,
) {
    while let Ok(authed) = authed_rx.try_recv() {
        match authed {
            AuthedSocket::Inbound(stream, peer) => {
                if let Err(e) = stream.set_nonblocking(true) {
                    debug!(peer = %peer, error = %e, "failed to set inbound raft socket non-blocking");
                    continue;
                }
                debug!(peer = %peer, "raft peer link established (inbound)");
                inbound.push(InboundConn {
                    stream,
                    peer,
                    recv_buf: Vec::new(),
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
    context: &RaftDriverContext,
) {
    let local_tip = local_tip(context);
    inbound.retain_mut(|conn| {
        let mut chunk = [0u8; 16 * 1024];
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
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    debug!(peer = %conn.peer, error = %e, "raft peer read error — dropping link");
                    return false;
                }
            }
        }

        // Extract every complete frame currently buffered.
        let mut consumed = 0;
        loop {
            match scan_frame(&conn.recv_buf[consumed..]) {
                Ok(FrameScan::Complete(envelope, used)) => {
                    consumed += used;
                    let msg = envelope.message;
                    if is_vote_request(msg.msg_type())
                        && !candidate_is_current(envelope.tip, local_tip)
                    {
                        // The whole point of the tip envelope: a
                        // candidate behind our journal never gets our
                        // vote. Dropping the request is safe (it looks
                        // like packet loss to raft) — see melin-raft's
                        // `recency` docs.
                        debug!(
                            from = msg.from,
                            candidate_tip = ?envelope.tip,
                            our_tip = ?local_tip,
                            "vote request filtered: candidate journal is behind ours"
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
/// `false` on a storage failure (raft must stop).
fn drain_node(
    node: &mut ControlNode,
    links: &mut HashMap<u64, PeerLink>,
    context: &RaftDriverContext,
) -> bool {
    let tip = local_tip(context);
    // Chain hash rides the envelope for step-3 divergence diagnostics;
    // zero until the journal cursor is plumbed through (with sequence 0
    // it carries no information yet).
    let chain_hash = [0u8; 32];
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
                return false;
            }
        };
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
            encode_frame(tip, &chain_hash, &msg, &mut link.out_buf);
            flush_link(msg.to, link);
        }
        for payload in drained.committed {
            // Step 1 proposes nothing, so committed payloads can only
            // appear once config propagation (step 2) lands.
            debug!(
                bytes = payload.len(),
                "committed control-plane entry (unhandled in this phase)"
            );
        }
    }
    true
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

/// The journal tip this node advertises. Sequence is 0 until the
/// auto-promotion step wires the durable journal cursor through; the
/// epoch half is already live via the fencing state.
fn local_tip(context: &RaftDriverContext) -> JournalTip {
    JournalTip {
        epoch: context.fence_state.epoch(),
        last_sequence: 0,
    }
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
mod tests {
    use super::*;
    use base64::Engine as _;

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
    /// TCP.
    fn boot_cluster(ids: &[u64]) -> HashMap<u64, TestNode> {
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
        for &id in ids {
            let dir = tempfile::tempdir().expect("tempdir");
            let status = Arc::new(RaftStatus::new(id));
            let shutdown = Arc::new(AtomicBool::new(false));
            let config = RaftDriverConfig {
                node_id: id,
                voters: ids.to_vec(),
                peers: ids
                    .iter()
                    .filter(|&&p| p != id)
                    .map(|&p| (p, addrs[&p]))
                    .collect(),
                dir: dir.path().to_path_buf(),
            };
            let context = RaftDriverContext {
                signing_key: signing[&id].clone(),
                authorized_keys: Arc::clone(&authorized),
                fence_state: Arc::new(FenceState::new(0)),
                status: Arc::clone(&status),
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
}
