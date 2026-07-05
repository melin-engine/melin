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
//! nothing about the cluster topology. Connections are handled inline
//! on the acceptor thread (one at a time) with socket timeouts — this
//! is a control-plane courtesy path, not a serving path, and a slow
//! client can at worst delay other redirects, never trading.
//!
//! At promotion the acceptor is stopped and hands the listener back so
//! `run_as_primary` can serve real traffic on the same socket.

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use tracing::{debug, info};

use melin_app::auth::AuthorizedKeys;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;
use melin_wire_protocol::transport::BlockingTransportListener;

use crate::raft_driver::LeaderFollow;

/// Per-connection handshake deadline. Generous for a WAN client,
/// bounded so a half-open connection cannot wedge the acceptor.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Accept-poll cadence while idle (the listener is non-blocking so the
/// stop flag is honoured promptly at promotion).
const ACCEPT_POLL: Duration = Duration::from_millis(100);

/// Spawn the acceptor. Returns the join handle; joining (after setting
/// `stop`) yields the listener back for `run_as_primary`.
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
            loop {
                if stop.load(Ordering::Acquire) {
                    return listener;
                }
                match listener.accept() {
                    Ok((read, write, peer)) => {
                        debug!(peer = %peer, "redirecting client connection");
                        // Best-effort: a failed handshake only affects
                        // this client, which retries.
                        if let Err(e) = redirect_one(read, write, &authorized_keys, &follow) {
                            debug!(peer = %peer, error = %e, "client redirect handshake failed");
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

/// Run one auth-then-redirect exchange.
fn redirect_one<R, W>(
    mut read: R,
    mut write: W,
    authorized_keys: &AuthorizedKeys,
    follow: &LeaderFollow,
) -> std::io::Result<()>
where
    R: Read + std::os::fd::AsRawFd,
    W: Write + std::os::fd::AsRawFd,
{
    set_rw_timeouts(read.as_raw_fd())?;
    set_rw_timeouts(write.as_raw_fd())?;

    // Challenge → signed response, the same handshake the reader runs.
    let nonce = crate::replication::auth::generate_challenge_nonce()?;
    let mut buf = [0u8; 64];
    let n =
        control_codec::encode_transport_response(&TransportResponse::Challenge { nonce }, &mut buf)
            .map_err(|e| std::io::Error::other(format!("encode challenge: {e:?}")))?;
    write.write_all(&buf[..n])?;
    write.flush()?;

    // `[len:4][seq:8][tag:1][sig:64][pk:32]` — read exactly one frame.
    let mut len_bytes = [0u8; 4];
    read.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len != 105 {
        return Err(std::io::Error::other(format!(
            "unexpected auth frame length {len}"
        )));
    }
    let mut frame = [0u8; 105];
    read.read_exact(&mut frame)?;
    let (_seq, cr) = control_codec::decode_challenge_response(&frame)
        .map_err(|e| std::io::Error::other(format!("decode challenge response: {e:?}")))?;

    // Any authorized key (any permission) gets a redirect: the point
    // is routing an authenticated member of the venue, not gating what
    // it may do — the real primary enforces permissions.
    let response = if authorized_keys.lookup(&cr.public_key).is_some()
        && VerifyingKey::from_bytes(&cr.public_key)
            .and_then(|vk| vk.verify(&nonce, &Signature::from_bytes(&cr.signature)))
            .is_ok()
    {
        match follow.leader_order_entry_addr() {
            Some(addr) => TransportResponse::Redirect { addr },
            // Leaderless (mid-election) or the leader announced no
            // client address: tell the client to back off and retry.
            None => TransportResponse::ServerBusy,
        }
    } else {
        TransportResponse::AuthFailed
    };
    let n = control_codec::encode_transport_response(&response, &mut buf)
        .map_err(|e| std::io::Error::other(format!("encode redirect: {e:?}")))?;
    write.write_all(&buf[..n])?;
    write.flush()?;
    Ok(())
}

/// Arm socket send/recv timeouts on a raw fd — the accepted halves are
/// generic over the transport, so this goes through `setsockopt`
/// directly (same approach as the busy-poll knob in `server.rs`).
fn set_rw_timeouts(fd: std::os::fd::RawFd) -> std::io::Result<()> {
    let tv = libc::timeval {
        tv_sec: HANDSHAKE_TIMEOUT.as_secs() as libc::time_t,
        tv_usec: 0,
    };
    for opt in [libc::SO_RCVTIMEO, libc::SO_SNDTIMEO] {
        // SAFETY: `tv` is a valid timeval for the duration of the call.
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                opt,
                &tv as *const libc::timeval as *const libc::c_void,
                std::mem::size_of::<libc::timeval>() as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}
