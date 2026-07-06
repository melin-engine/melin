//! Replica-side client redirect acceptor.
//!
//! A replica's `--bind` listener is otherwise silent until promotion —
//! a client pointed at it (stale config, or a failover the client
//! hasn't learned about) would hang in the accept backlog. With
//! follow-the-leader enabled, this acceptor answers instead: it runs
//! the normal client auth handshake and then, rather than `ServerReady`,
//! sends `Redirect { addr }` naming the cluster's current leader — so
//! the cluster is its own service discovery and clients reconnect to a
//! newly promoted primary with no VIP/DNS machinery.
//!
//! Auth comes first deliberately: an unauthenticated scanner learns
//! nothing about the cluster topology.
//!
//! Each accepted connection is handled on its own short-lived thread
//! under a whole-handshake deadline, with the number of concurrent
//! handshakes bounded. The acceptor thread itself only accepts and
//! spawns, so stopping it (at promotion) never waits on a client: a
//! reconnect storm, a slow WAN client, or a deliberate byte-dribbler
//! can each burn only their own thread and slot, never the listener
//! hand-back. This is a control-plane courtesy path, not a serving
//! path — threads are cheap here.
//!
//! At promotion the acceptor is stopped and hands the listener back so
//! `run_as_primary` can serve real traffic on the same socket.

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use melin_app::auth::AuthorizedKeys;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;
use melin_wire_protocol::transport::BlockingTransportListener;

use crate::raft_driver::LeaderFollow;

/// Whole-handshake deadline per connection, enforced by re-arming the
/// socket timeout with the *remaining* budget before every read/write
/// syscall — so a client dribbling one byte per syscall still cannot
/// stretch its handshake past this bound. Generous for a WAN client.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Accept-poll cadence while idle (the listener is non-blocking so the
/// stop flag is honoured promptly at promotion).
const ACCEPT_POLL: Duration = Duration::from_millis(100);
/// Concurrent handshake budget. Connections accepted while the budget
/// is exhausted are answered with a quick "busy, retry" and dropped.
/// Sized for a post-failover reconnect storm (handshakes are one round
/// trip, so slots recycle in milliseconds for honest clients) while
/// capping what a flood of half-open connections can pin: at most this
/// many threads for at most the handshake deadline each.
const MAX_CONCURRENT_HANDSHAKES: usize = 32;
/// Deadline for the pre-auth busy answer sent to shed connections. It
/// runs on the acceptor thread, so it is deliberately tight — see the
/// shed branch in [`spawn`] for why that is safe.
const SHED_BUSY_TIMEOUT: Duration = Duration::from_millis(100);

/// RAII handle on one concurrent-handshake slot: acquired against the
/// budget, released on drop however its thread ends — success, error,
/// or panic. Acquire and release live in the same type so no code path
/// can take the counter without pairing the give-back.
struct SlotGuard(Arc<AtomicUsize>);

impl SlotGuard {
    /// Claim a slot if the budget allows. Check-then-add is not atomic,
    /// but only the single acceptor thread acquires — the atomic exists
    /// for the handshake threads' releases.
    fn try_acquire(active: &Arc<AtomicUsize>, budget: usize) -> Option<Self> {
        if active.load(Ordering::Relaxed) >= budget {
            return None;
        }
        active.fetch_add(1, Ordering::Relaxed);
        Some(Self(Arc::clone(active)))
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Spawn the acceptor. Returns the join handle; joining (after setting
/// `stop`) yields the listener back for `run_as_primary`. The join is
/// bounded by [`ACCEPT_POLL`] — in-flight handshakes run on their own
/// detached threads and never hold the listener.
pub(crate) fn spawn<L: BlockingTransportListener>(
    mut listener: L,
    authorized_keys: Arc<AuthorizedKeys>,
    follow: LeaderFollow,
    stop: Arc<AtomicBool>,
) -> std::io::Result<JoinHandle<L>> {
    listener.set_nonblocking(true);
    info!("client redirect acceptor started (replica)");
    std::thread::Builder::new()
        .name("client-redirect".into())
        .spawn(move || {
            let active = Arc::new(AtomicUsize::new(0));
            loop {
                if stop.load(Ordering::Acquire) {
                    return listener;
                }
                match listener.accept() {
                    Ok((read, mut write, peer)) => {
                        let Some(slot) = SlotGuard::try_acquire(&active, MAX_CONCURRENT_HANDSHAKES)
                        else {
                            // Answer "busy, retry" before dropping — a
                            // bare close surfaces as a Disconnected
                            // error that clients do NOT retry, whereas
                            // ServerBusy re-enters their backoff loop.
                            // Written on the acceptor thread under a
                            // tight deadline: the 5-byte frame fits any
                            // real socket buffer instantly, so only a
                            // deliberately zero-window peer can burn
                            // the budget — and such a flood degrades
                            // exactly the path it already saturates.
                            debug!(peer = %peer, "redirect handshake budget exhausted — answering busy");
                            send_response(
                                &mut write,
                                &TransportResponse::ServerBusy,
                                Instant::now() + SHED_BUSY_TIMEOUT,
                            );
                            continue;
                        };
                        let keys = Arc::clone(&authorized_keys);
                        let follow = follow.clone();
                        debug!(peer = %peer, "redirecting client connection");
                        // Detached on purpose: the thread owns only its
                        // connection and slot, so promotion never joins
                        // it; it self-terminates within the handshake
                        // deadline. If the spawn fails (thread
                        // exhaustion), dropping the closure closes the
                        // connection and releases the slot via the guard.
                        if let Err(e) = std::thread::Builder::new()
                            .name("client-redirect-conn".into())
                            .spawn(move || {
                                let _slot = slot;
                                let deadline = Instant::now() + HANDSHAKE_TIMEOUT;
                                // Best-effort: a failed handshake only
                                // affects this client, which retries.
                                if let Err(e) = redirect_one(read, write, &keys, &follow, deadline)
                                {
                                    debug!(peer = %peer, error = %e, "client redirect handshake failed");
                                }
                            })
                        {
                            // warn: thread exhaustion is host-level
                            // resource starvation, not a client event —
                            // the budget cap means a client flood alone
                            // cannot trigger this.
                            warn!(peer = %peer, error = %e, "could not spawn redirect handshake thread");
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(ACCEPT_POLL);
                    }
                    Err(e) => {
                        debug!(error = %e, "redirect acceptor accept error");
                        std::thread::sleep(ACCEPT_POLL);
                    }
                }
            }
        })
        .map_err(std::io::Error::other)
}

/// Run one auth-then-redirect exchange. Every socket operation is
/// bounded by `deadline` (taken as a parameter so tests can shrink it).
fn redirect_one<R, W>(
    mut read: R,
    mut write: W,
    authorized_keys: &AuthorizedKeys,
    follow: &LeaderFollow,
    deadline: Instant,
) -> std::io::Result<()>
where
    R: Read + std::os::fd::AsRawFd,
    W: Write + std::os::fd::AsRawFd,
{
    // Challenge → signed response, the same handshake the reader runs.
    let nonce = crate::replication::auth::generate_challenge_nonce()?;
    // Encode scratch. The largest frame written here is Challenge
    // (4-byte length + 1 tag + 32 nonce = 37 bytes; Redirect is 24);
    // 64 leaves headroom for variant growth without heap allocation.
    let mut buf = [0u8; 64];
    let n =
        control_codec::encode_transport_response(&TransportResponse::Challenge { nonce }, &mut buf)
            .map_err(|e| std::io::Error::other(format!("encode challenge: {e:?}")))?;
    write_all_deadline(&mut write, &buf[..n], deadline)?;

    // Read one length-prefixed ChallengeResponse frame, under the same
    // size tolerance as the primary's accept loop — a frame the real
    // primary would accept must never be rejected here, or handshake
    // evolution breaks redirects only (and only during failovers).
    let mut len_bytes = [0u8; 4];
    read_exact_deadline(&mut read, &mut len_bytes, deadline)?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > crate::server::MAX_CLIENT_AUTH_FRAME {
        send_response(&mut write, &TransportResponse::AuthFailed, deadline);
        return Err(std::io::Error::other(format!(
            "auth frame too large: {len}"
        )));
    }
    let mut frame = [0u8; crate::server::MAX_CLIENT_AUTH_FRAME];
    read_exact_deadline(&mut read, &mut frame[..len], deadline)?;

    // Any authorized key (any permission) gets a redirect: the point
    // is routing an authenticated member of the venue, not gating what
    // it may do — the real primary enforces permissions. The
    // verification itself is shared with the primary's accept loop.
    let response = match crate::server::verify_client_challenge_response(
        &nonce,
        &frame[..len],
        authorized_keys,
    ) {
        Ok(_) => match follow.leader_order_entry_addr() {
            Some(addr) => TransportResponse::Redirect { addr },
            // Leaderless (mid-election) or the leader announced no
            // client address: tell the client to back off and retry.
            None => TransportResponse::ServerBusy,
        },
        Err(_) => TransportResponse::AuthFailed,
    };
    let n = control_codec::encode_transport_response(&response, &mut buf)
        .map_err(|e| std::io::Error::other(format!("encode redirect: {e:?}")))?;
    write_all_deadline(&mut write, &buf[..n], deadline)?;
    Ok(())
}

/// Best-effort response send on an already-failing handshake — the
/// error the caller returns is the interesting outcome; a failed send
/// here just means the peer is gone.
fn send_response<W>(write: &mut W, response: &TransportResponse, deadline: Instant)
where
    W: Write + std::os::fd::AsRawFd,
{
    let mut buf = [0u8; 64];
    if let Ok(n) = control_codec::encode_transport_response(response, &mut buf) {
        // Best-effort by contract (see fn doc) — the connection is
        // being torn down either way.
        let _ = write_all_deadline(write, &buf[..n], deadline);
    }
}

/// Budget left until `deadline`, erring once within a millisecond of
/// it: `SO_RCVTIMEO`/`SO_SNDTIMEO` treat a zero timeval as "block
/// forever", so a sub-millisecond remainder must round to expiry, never
/// to zero.
fn remaining_budget(deadline: Instant) -> std::io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining < Duration::from_millis(1) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "handshake deadline exceeded",
        ));
    }
    Ok(remaining)
}

/// Arm one socket timeout option with `dur`. Callers must pass a
/// `remaining_budget`-vetted duration — the shared helper preserves
/// sub-second precision, and the budget's 1ms floor is what keeps a
/// near-expired remainder from truncating to the zero timeval the
/// kernel reads as "no timeout".
fn arm_timeout(fd: std::os::fd::RawFd, opt: libc::c_int, dur: Duration) -> std::io::Result<()> {
    crate::server::set_socket_timeout(fd, opt, Some(dur))
}

/// `read_exact` under a whole-transfer deadline: the socket timeout is
/// re-armed with the *remaining* budget before every syscall, so
/// partial progress (a byte-dribbling peer) shrinks the budget instead
/// of resetting it — total wall time is bounded by the deadline no
/// matter how the bytes arrive.
fn read_exact_deadline<R>(read: &mut R, buf: &mut [u8], deadline: Instant) -> std::io::Result<()>
where
    R: Read + std::os::fd::AsRawFd,
{
    let mut filled = 0;
    while filled < buf.len() {
        arm_timeout(
            read.as_raw_fd(),
            libc::SO_RCVTIMEO,
            remaining_budget(deadline)?,
        )?;
        match read.read(&mut buf[filled..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "peer closed during handshake",
                ));
            }
            Ok(n) => filled += n,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::Interrupted =>
            {
                // Timeout tick or signal — the next remaining_budget()
                // call errors out if the deadline is spent.
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// `write_all` + flush under the same whole-transfer deadline contract
/// as [`read_exact_deadline`].
fn write_all_deadline<W>(write: &mut W, buf: &[u8], deadline: Instant) -> std::io::Result<()>
where
    W: Write + std::os::fd::AsRawFd,
{
    let mut written = 0;
    while written < buf.len() {
        arm_timeout(
            write.as_raw_fd(),
            libc::SO_SNDTIMEO,
            remaining_budget(deadline)?,
        )?;
        match write.write(&buf[written..]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "peer stopped accepting bytes during handshake",
                ));
            }
            Ok(n) => written += n,
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                    || e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    write.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::os::unix::net::UnixStream;

    use base64::Engine as _;
    use ed25519_dalek::{Signer, SigningKey};
    use melin_raft::registry::{MemberRecord, Registry};
    use melin_transport_core::health::RaftStatus;
    use melin_wire_protocol::control_codec::{
        TAG_AUTH_FAILED, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_REDIRECT, TAG_SERVER_BUSY,
    };

    use crate::raft_driver::ClusterDirectory;

    /// Server-to-client responses observed by the test client. The
    /// production decoder for these frames lives in the exchange-side
    /// `melin-protocol` crate, which this app-agnostic crate does not
    /// depend on — so the test decodes the wire layout directly.
    #[derive(Debug, PartialEq, Eq)]
    enum ClientSeen {
        Challenge { nonce: [u8; 32] },
        Redirect { addr: SocketAddr },
        ServerBusy,
        AuthFailed,
    }

    const SELF_NODE: u64 = 3;
    const LEADER_NODE: u64 = 2;

    fn authorized_for(key: &SigningKey) -> Arc<AuthorizedKeys> {
        let b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().as_bytes());
        Arc::new(AuthorizedKeys::parse(&format!("trader {b64} test\n")).expect("parse keys"))
    }

    /// A follow handle believing `LEADER_NODE` leads, with (optionally)
    /// an announced order-entry address for it in the directory.
    fn follow(leader_oe: Option<SocketAddr>) -> LeaderFollow {
        let status = Arc::new(RaftStatus::new(SELF_NODE));
        status.leader_id.store(LEADER_NODE, Ordering::Relaxed);
        let directory = Arc::new(ClusterDirectory::default());
        if let Some(addr) = leader_oe {
            let mut registry = Registry::default();
            let record = MemberRecord {
                node_id: LEADER_NODE,
                raft_addr: "127.0.0.1:1".parse().expect("addr"),
                replication_addr: None,
                order_entry_addr: Some(addr),
                public_key: [0u8; 32],
            };
            assert!(registry.apply(&record.encode()), "record must apply");
            directory.update(&registry);
        }
        LeaderFollow {
            self_node_id: SELF_NODE,
            status,
            directory,
        }
    }

    /// Drive the client half of the handshake: read the Challenge,
    /// answer with `sign(nonce)` under `key`, return the server's
    /// response.
    fn client_handshake(stream: &mut UnixStream, key: &SigningKey) -> ClientSeen {
        let nonce = read_challenge(stream);
        write_challenge_response(stream, key, &nonce);
        read_response(stream)
    }

    fn read_challenge(stream: &mut UnixStream) -> [u8; 32] {
        match read_response(stream) {
            ClientSeen::Challenge { nonce } => nonce,
            other => panic!("expected Challenge, got {other:?}"),
        }
    }

    fn read_response(stream: &mut UnixStream) -> ClientSeen {
        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes).expect("read frame len");
        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).expect("read frame payload");
        match payload[0] {
            TAG_CHALLENGE => {
                let mut nonce = [0u8; 32];
                nonce.copy_from_slice(&payload[1..33]);
                ClientSeen::Challenge { nonce }
            }
            TAG_REDIRECT => {
                // family(1) + ip(16, v4-mapped-v6) + port(2 LE)
                let family = payload[1];
                let mut ip = [0u8; 16];
                ip.copy_from_slice(&payload[2..18]);
                let port = u16::from_le_bytes([payload[18], payload[19]]);
                let v6 = std::net::Ipv6Addr::from(ip);
                let addr = match family {
                    4 => SocketAddr::new(v6.to_ipv4_mapped().expect("v4-mapped").into(), port),
                    6 => SocketAddr::new(v6.into(), port),
                    other => panic!("unknown redirect family {other}"),
                };
                ClientSeen::Redirect { addr }
            }
            TAG_SERVER_BUSY => ClientSeen::ServerBusy,
            TAG_AUTH_FAILED => ClientSeen::AuthFailed,
            other => panic!("unexpected response tag {other:#x}"),
        }
    }

    fn write_challenge_response(stream: &mut UnixStream, key: &SigningKey, nonce: &[u8; 32]) {
        let sig = key.sign(nonce);
        let mut frame = Vec::with_capacity(109);
        frame.extend_from_slice(&105u32.to_le_bytes());
        frame.extend_from_slice(&0u64.to_le_bytes()); // seq
        frame.push(TAG_CHALLENGE_RESPONSE);
        frame.extend_from_slice(&sig.to_bytes());
        frame.extend_from_slice(key.verifying_key().as_bytes());
        stream.write_all(&frame).expect("write challenge response");
    }

    /// Run `redirect_one` against one end of a socketpair on a helper
    /// thread, handing the test the client end.
    fn serve_one(
        keys: Arc<AuthorizedKeys>,
        follow: LeaderFollow,
        deadline: Instant,
    ) -> (UnixStream, std::thread::JoinHandle<std::io::Result<()>>) {
        let (server, client) = UnixStream::pair().expect("socketpair");
        let read = server.try_clone().expect("clone server half");
        let handle =
            std::thread::spawn(move || redirect_one(read, server, &keys, &follow, deadline));
        (client, handle)
    }

    fn far_deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[test]
    fn authorized_client_gets_redirect_to_leader() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let leader_addr: SocketAddr = "10.1.2.3:4567".parse().expect("addr");
        let (mut client, server) = serve_one(
            authorized_for(&key),
            follow(Some(leader_addr)),
            far_deadline(),
        );

        let response = client_handshake(&mut client, &key);
        assert_eq!(response, ClientSeen::Redirect { addr: leader_addr });
        server.join().expect("no panic").expect("handshake ok");
    }

    #[test]
    fn authorized_client_gets_busy_when_leaderless() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let (mut client, server) = serve_one(authorized_for(&key), follow(None), far_deadline());

        let response = client_handshake(&mut client, &key);
        assert_eq!(response, ClientSeen::ServerBusy);
        server.join().expect("no panic").expect("handshake ok");
    }

    #[test]
    fn unauthorized_key_gets_auth_failed_not_topology() {
        let authorized = SigningKey::from_bytes(&[7u8; 32]);
        let intruder = SigningKey::from_bytes(&[9u8; 32]);
        let leader_addr: SocketAddr = "10.1.2.3:4567".parse().expect("addr");
        let (mut client, server) = serve_one(
            authorized_for(&authorized),
            follow(Some(leader_addr)),
            far_deadline(),
        );

        let response = client_handshake(&mut client, &intruder);
        assert_eq!(
            response,
            ClientSeen::AuthFailed,
            "an unauthorized key must never learn the leader address"
        );
        server.join().expect("no panic").expect("handshake ok");
    }

    #[test]
    fn bad_signature_gets_auth_failed() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let leader_addr: SocketAddr = "10.1.2.3:4567".parse().expect("addr");
        let (mut client, server) = serve_one(
            authorized_for(&key),
            follow(Some(leader_addr)),
            far_deadline(),
        );

        // Sign the wrong bytes: authorized key, invalid proof.
        let _real_nonce = read_challenge(&mut client);
        write_challenge_response(&mut client, &key, &[0xAB; 32]);
        assert_eq!(read_response(&mut client), ClientSeen::AuthFailed);
        server.join().expect("no panic").expect("handshake ok");
    }

    #[test]
    fn undecodable_frame_gets_auth_failed_not_topology() {
        // A garbage frame within the size tolerance is answered exactly
        // like a bad signature — AuthFailed, no topology — matching the
        // primary accept loop's behavior for the same bytes.
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let (mut client, server) = serve_one(authorized_for(&key), follow(None), far_deadline());

        let _nonce = read_challenge(&mut client);
        let mut garbage = Vec::new();
        garbage.extend_from_slice(&42u32.to_le_bytes());
        garbage.extend_from_slice(&[0xEE; 42]);
        client.write_all(&garbage).expect("write garbage frame");
        assert_eq!(read_response(&mut client), ClientSeen::AuthFailed);
        server.join().expect("no panic").expect("answered exchange");
    }

    #[test]
    fn oversized_frame_gets_auth_failed_and_errors() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let (mut client, server) = serve_one(authorized_for(&key), follow(None), far_deadline());

        let _nonce = read_challenge(&mut client);
        client
            .write_all(&100_000u32.to_le_bytes())
            .expect("write oversize length");
        assert_eq!(read_response(&mut client), ClientSeen::AuthFailed);
        let err = server.join().expect("no panic").expect_err("must error");
        assert!(err.to_string().contains("auth frame too large"));
    }

    #[test]
    fn byte_dribble_cannot_outlive_the_deadline() {
        // A peer trickling bytes must be cut off at the whole-handshake
        // deadline: each partial read re-arms the socket timeout with
        // the *remaining* budget, so progress does not reset the clock.
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let deadline = Instant::now() + Duration::from_millis(300);
        let (mut client, server) = serve_one(authorized_for(&key), follow(None), deadline);

        let _nonce = read_challenge(&mut client);
        let started = Instant::now();
        // Dribble one length byte every 80ms — under any per-syscall
        // timeout, but collectively past the 300ms deadline.
        let dribble = std::thread::spawn(move || {
            for byte in [105u8, 0, 0, 0, 1, 2, 3] {
                if client.write_all(&[byte]).is_err() {
                    return; // server gave up — expected
                }
                std::thread::sleep(Duration::from_millis(80));
            }
        });

        let err = server.join().expect("no panic").expect_err("must time out");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "deadline must bound the dribbler (took {elapsed:?})"
        );
        dribble.join().expect("dribbler exits");
    }

    #[test]
    fn silent_peer_times_out_at_the_deadline() {
        let key = SigningKey::from_bytes(&[7u8; 32]);
        let deadline = Instant::now() + Duration::from_millis(200);
        let (mut client, server) = serve_one(authorized_for(&key), follow(None), deadline);

        let _nonce = read_challenge(&mut client);
        // Send nothing at all.
        let err = server.join().expect("no panic").expect_err("must time out");
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }
}
