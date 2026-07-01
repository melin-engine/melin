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

fn run(
    listener: TcpListener,
    mut node: ControlNode,
    config: RaftDriverConfig,
    context: RaftDriverContext,
) {
    let (authed_tx, authed_rx): (Sender<AuthedSocket>, Receiver<AuthedSocket>) = channel();
    let mut inbound: Vec<InboundConn> = Vec::new();
    // A healthy cluster holds one inbound link (and needs at most one
    // concurrent auth) per peer; the cap adds slack for reconnect overlap
    // and bounds a flood.
    let conn_cap = inbound_cap(config.peers.len());
    let inflight_auth = Arc::new(std::sync::atomic::AtomicUsize::new(0));
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
        accept_inbound(&listener, &context, &authed_tx, &inflight_auth, conn_cap);

        // 3. Freshly authenticated sockets and dial results.
        drain_authed(&authed_rx, &mut inbound, &mut links, conn_cap);

        // 4. Kick off outbound dials that are due.
        dial_due_peers(&mut links, &config, &context, &authed_tx, now);

        // 5. Ingress: read peers, extract frames, filter, step raft.
        read_inbound(&mut inbound, &mut node, &context);
        poll_outbound_liveness(&mut links);

        // 6. Drain raft readies (fsyncs inside) and route the egress.
        if !drain_node(&mut node, &mut links, &context) {
            // Storage failure: raft is inoperable by contract. The
            // control plane stops; trading continues on the data plane.
            break;
        }

        publish_status(&node, &context.status);
        std::thread::sleep(POLL_INTERVAL);
    }

    // The driver is exiting (clean shutdown or storage failure). Clear
    // leadership and drop the running flag so `/metrics` stops
    // reporting a stale leader on a node whose control plane is gone —
    // on a storage failure the process keeps serving trading and its
    // health endpoint, so these gauges would otherwise freeze forever.
    context.status.mark_stopped();
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
            AuthedSocket::Inbound(stream, peer) => {
                if inbound.len() >= cap {
                    debug!(peer = %peer, cap, "inbound raft link cap reached — dropping");
                    drop(stream);
                    continue;
                }
                if let Err(e) = stream.set_nonblocking(true) {
                    debug!(peer = %peer, error = %e, "failed to set inbound raft socket non-blocking");
                    continue;
                }
                debug!(peer = %peer, "raft peer link established (inbound)");
                inbound.push(InboundConn {
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
    context: &RaftDriverContext,
) {
    let local_tip = local_tip(context);
    let tip_ready = context.tip_ready.load(Ordering::Acquire);
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

    #[test]
    fn inbound_cap_scales_with_peers_and_has_a_floor() {
        // Floor applies for tiny clusters.
        assert_eq!(inbound_cap(0), INBOUND_CAP_FLOOR);
        assert_eq!(inbound_cap(1), INBOUND_CAP_FLOOR);
        // Scales past the floor for larger ones.
        assert_eq!(inbound_cap(5), 5 * INBOUND_SLACK);
        assert!(inbound_cap(100) >= 100);
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
                // These test nodes act as always-recovered primaries.
                tip_ready: Arc::new(AtomicBool::new(true)),
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
