//! Operator admin endpoint — single TCP listener for all server-side
//! commands an exchange operator may send.
//!
//! Authentication is Ed25519 challenge-response with operator-only keys
//! (the same handshake used for trading sessions). After auth, the
//! client sends one ASCII command terminated by `\n`:
//!
//! - `PROMOTE` — replica → primary leadership transition. Files a
//!   manual [`PromotionRequest`]; the replica receive loop observes it
//!   and exits with the recovered state. Available only when the spawn
//!   caller wired a promotion handle (typically the replica path).
//! - `ROTATE` — archive the current journal segment at the next fsync
//!   boundary and start a fresh one. Available only when the spawn
//!   caller wired a rotation flag (any node with `--max-journal-mib >
//!   0` or runtime rotation enabled).
//! - `DURABILITY <local|hybrid|durably-replicated>` — atomically swap
//!   the active durability mode on a node running a response stage
//!   (primary, or post-promotion replica). Lets an operator resume
//!   trading at reduced durability immediately after a promotion (no
//!   restart, no client reconnects) and restore the target mode once
//!   replicas reattach. Available only when the spawn caller wired the
//!   shared mode atomic.
//! - `RAFT-ADD-VOTER <node_id> <raft_addr> <pubkey_b64>` /
//!   `RAFT-REMOVE-VOTER <node_id>` — change the control-plane voter set
//!   at runtime (grow, shrink, or replace a node under a new identity).
//!   Unlike the other commands these are not fire-and-forget: the driver
//!   shepherds the change to commitment and the handler blocks on its
//!   reply, answering `OK voters=<list>` or `ERR <reason>`. Available
//!   only on a node running control-plane raft.
//!
//! A command for which the corresponding flag is `None` is rejected
//! with `ERR <command> not available on this node\n` so operators get
//! a clear diagnostic instead of a silent no-op.
//!
//! The listener stays alive for the lifetime of the process — multiple
//! ROTATEs over a long run, and an eventual PROMOTE on a replica, all
//! flow through the same socket. Concurrent or repeated triggers
//! collapse via CAS in the journal stage / receive loop, so duplicate
//! commands do not queue.
//!
//! Each accepted connection is served on its own short-lived thread
//! (bounded by [`MAX_ADMIN_HANDLERS`]). The `RAFT-*` voter-change
//! commands block until their raft commit, so serving connections
//! inline would let a stuck voter change (e.g. a change issued while the
//! cluster is leaderless mid-failover) stall an urgent `PROMOTE` on the
//! same node; per-connection threads keep every command independent.

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc::{RecvTimeoutError, Sender, channel};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::durability_policy::DurabilityMode;
use crate::promotion::PromotionRequest;
use crate::raft_driver::{VoterChange, VoterChangeRequest};

use ed25519_dalek::{Verifier, VerifyingKey};
use tracing::{debug, error, info};

use melin_app::auth::{AuthorizedKeys, Permission};
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;

/// Spawn the admin listener on a dedicated thread.
///
/// Any of `promote` / `rotate_requested` / `durability_mode` may be
/// `None` to disable the corresponding command on this node. The
/// listener still accepts connections and authenticates them — a
/// disabled command is rejected at the command-dispatch step, not at
/// connect time, so operator tooling sees a structured ERR rather than
/// a TCP RST.
pub fn spawn(
    bind_addr: SocketAddr,
    promote: Option<PromotionRequest>,
    rotate_requested: Option<Arc<AtomicBool>>,
    durability_mode: Option<Arc<AtomicU8>>,
    voter_changes: Option<Sender<VoterChangeRequest>>,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("admin-listener".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run(
                    bind_addr,
                    promote,
                    rotate_requested,
                    durability_mode,
                    voter_changes,
                    shutdown,
                    authorized_keys,
                )
            }));
            if let Err(panic) = result {
                let msg = panic_message(&panic);
                error!(addr = %bind_addr, panic = %msg, "admin listener thread panicked");
            }
        })
        .expect("failed to spawn admin listener thread")
}

/// Cap on concurrently-served admin connections. Each command runs on its
/// own thread; this bounds the fan-out a burst (or a slow blocking voter
/// change) can create. The endpoint is operator-key-gated and low-traffic
/// — a healthy operator issues one command at a time — so this is far
/// above any legitimate load while still capping an abusive flood. Note
/// the driver serializes voter changes (a second concurrent one gets an
/// instant refusal), so at most one handler is ever blocked on a raft
/// commit at a time.
const MAX_ADMIN_HANDLERS: usize = 16;

/// RAII counter bounding concurrent admin handler threads: [`acquire`]
/// increments the shared count (refusing once [`MAX_ADMIN_HANDLERS`] is
/// reached) and the guard decrements it when the handler thread exits by
/// any path. The listener sheds new connections while at the cap.
///
/// [`acquire`]: HandlerSlot::acquire
struct HandlerSlot(Arc<AtomicUsize>);

impl HandlerSlot {
    fn acquire(counter: &Arc<AtomicUsize>) -> Option<Self> {
        let mut n = counter.load(Ordering::Acquire);
        loop {
            if n >= MAX_ADMIN_HANDLERS {
                return None;
            }
            match counter.compare_exchange_weak(n, n + 1, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Some(HandlerSlot(Arc::clone(counter))),
                Err(observed) => n = observed,
            }
        }
    }
}

impl Drop for HandlerSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

fn run(
    bind_addr: SocketAddr,
    promote: Option<PromotionRequest>,
    rotate_requested: Option<Arc<AtomicBool>>,
    durability_mode: Option<Arc<AtomicU8>>,
    voter_changes: Option<Sender<VoterChangeRequest>>,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
) {
    let listener = match TcpListener::bind(bind_addr) {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %bind_addr, error = %e, "admin listener bind failed");
            return;
        }
    };
    listener
        .set_nonblocking(true)
        .expect("set admin listener nonblocking");

    info!(
        addr = %bind_addr,
        promote_enabled = promote.is_some(),
        rotate_enabled = rotate_requested.is_some(),
        durability_enabled = durability_mode.is_some(),
        voter_changes_enabled = voter_changes.is_some(),
        "admin listener started"
    );

    // Bounds concurrent handler threads; see [`MAX_ADMIN_HANDLERS`].
    let active = Arc::new(AtomicUsize::new(0));

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        match listener.accept() {
            Ok((stream, peer)) => {
                let Some(slot) = HandlerSlot::acquire(&active) else {
                    // At the concurrency cap — shed rather than queue.
                    // The dropped stream RSTs; the operator retries.
                    debug!(peer = %peer, "admin connection shed — handler cap reached");
                    continue;
                };
                debug!(peer = %peer, "admin connection accepted");
                // Clone the capability handles into the handler thread so
                // a blocking command (a voter change awaiting its raft
                // commit) never stalls the accept loop or other commands.
                let promote = promote.clone();
                let rotate_requested = rotate_requested.clone();
                let durability_mode = durability_mode.clone();
                let voter_changes = voter_changes.clone();
                let authorized_keys = Arc::clone(&authorized_keys);
                let spawned =
                    std::thread::Builder::new()
                        .name("admin-conn".into())
                        .spawn(move || {
                            // Held for the thread's lifetime: frees the slot on
                            // exit by any path (return, error, or panic).
                            let _slot = slot;
                            handle_connection(
                                stream,
                                promote.as_ref(),
                                rotate_requested.as_deref(),
                                durability_mode.as_deref(),
                                voter_changes.as_ref(),
                                &authorized_keys,
                            );
                        });
                if let Err(e) = spawned {
                    // Thread creation failed (resource limits) — the slot
                    // guard was moved into the closure and dropped with it,
                    // so the counter is already restored. Drop the
                    // connection; the operator retries.
                    debug!(peer = %peer, error = %e, "failed to spawn admin handler thread");
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                debug!(error = %e, "admin listener accept error");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Authenticate a connection via Ed25519 challenge-response. Operator
/// keys only. Returns `Ok(())` on success, `Err(reason)` otherwise; the
/// caller has already sent an `AuthFailed` response on the error path.
fn authenticate(stream: &mut TcpStream, authorized_keys: &AuthorizedKeys) -> Result<(), String> {
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| format!("getrandom failed: {e}"))?;

    let mut buf = [0u8; 128];
    let written =
        control_codec::encode_transport_response(&TransportResponse::Challenge { nonce }, &mut buf)
            .map_err(|e| format!("encode Challenge: {e}"))?;
    stream
        .write_all(&buf[..written])
        .map_err(|e| format!("send Challenge: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("flush Challenge: {e}"))?;

    let mut len_buf = [0u8; 4];
    std::io::Read::read_exact(stream, &mut len_buf)
        .map_err(|e| format!("read auth frame length: {e}"))?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len > 256 {
        send_auth_failed(stream);
        return Err(format!("auth frame too large: {frame_len}"));
    }
    let mut frame_buf = [0u8; 256];
    std::io::Read::read_exact(stream, &mut frame_buf[..frame_len])
        .map_err(|e| format!("read auth frame payload: {e}"))?;

    let (_seq, cr) = match control_codec::decode_challenge_response(&frame_buf[..frame_len]) {
        Ok(pair) => pair,
        Err(e) => {
            send_auth_failed(stream);
            return Err(format!("decode ChallengeResponse: {e}"));
        }
    };

    let (signature_bytes, public_key_bytes) = (cr.signature, cr.public_key);

    let permission = match authorized_keys.lookup(&public_key_bytes) {
        Some(perm) => perm,
        None => {
            send_auth_failed(stream);
            return Err("unknown public key".into());
        }
    };
    if permission != Permission::Operator {
        send_auth_failed(stream);
        return Err(format!(
            "admin endpoint requires operator key, got {permission:?}"
        ));
    }

    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes).map_err(|e| {
        send_auth_failed(stream);
        format!("invalid public key: {e}")
    })?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key.verify(&nonce, &signature).map_err(|e| {
        send_auth_failed(stream);
        format!("signature verification failed: {e}")
    })?;

    let written =
        control_codec::encode_transport_response(&TransportResponse::ServerReady, &mut buf)
            .map_err(|e| format!("encode ServerReady: {e}"))?;
    stream
        .write_all(&buf[..written])
        .map_err(|e| format!("send ServerReady: {e}"))?;
    stream
        .flush()
        .map_err(|e| format!("flush ServerReady: {e}"))?;

    Ok(())
}

fn send_auth_failed(stream: &mut TcpStream) {
    let mut buf = [0u8; 8];
    if let Ok(written) =
        control_codec::encode_transport_response(&TransportResponse::AuthFailed, &mut buf)
    {
        // Best-effort: an unauthenticated peer may already be gone, and
        // we're about to drop the stream regardless. Write errors here
        // carry no actionable signal.
        send_best_effort(stream, &buf[..written]);
    }
}

/// Write `payload` to `stream` and flush, ignoring errors. Used for
/// terminal admin responses where the connection is about to close: the
/// client may already have disconnected, and there's nothing the server
/// can usefully do with a write error after the in-process side effect
/// (flag CAS, auth rejection) has already happened.
fn send_best_effort(stream: &mut TcpStream, payload: &[u8]) {
    if let Err(e) = stream.write_all(payload) {
        debug!(error = %e, "admin write failed");
        return;
    }
    if let Err(e) = stream.flush() {
        debug!(error = %e, "admin flush failed");
    }
}

/// Handle one authenticated admin connection. Reads a single command
/// line and dispatches it.
fn handle_connection(
    mut stream: TcpStream,
    promote: Option<&PromotionRequest>,
    rotate_requested: Option<&AtomicBool>,
    durability_mode: Option<&AtomicU8>,
    voter_changes: Option<&Sender<VoterChangeRequest>>,
    authorized_keys: &AuthorizedKeys,
) {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

    if let Err(reason) = authenticate(&mut stream, authorized_keys) {
        debug!(reason = %reason, "admin auth failed");
        return;
    }

    let cloned = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            debug!(error = %e, "failed to clone admin stream");
            return;
        }
    };
    let mut reader = BufReader::new(cloned);

    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        debug!("failed to read from admin connection");
        return;
    }

    let trimmed = line.trim();
    match trimmed {
        "PROMOTE" => match promote {
            Some(request) => {
                // First request wins; a duplicate (or a race with an
                // auto-promotion) leaves the in-flight transition
                // untouched — still OK from the operator's viewpoint.
                if request.request(PromotionRequest::MANUAL) {
                    info!("promotion triggered by operator");
                } else {
                    // debug!, not info!: an operator-caused no-op, not
                    // a lifecycle event (the in-flight promotion was
                    // already info-logged by whoever filed it).
                    debug!("PROMOTE received but a promotion is already in flight");
                }
                send_best_effort(&mut stream, b"OK\n");
            }
            None => {
                send_best_effort(&mut stream, b"ERR PROMOTE not available on this node\n");
                debug!("rejected PROMOTE — flag not wired (primary node?)");
            }
        },
        "ROTATE" => match rotate_requested {
            Some(flag) => {
                flag.store(true, Ordering::Release);
                send_best_effort(&mut stream, b"OK\n");
                info!("rotation requested by operator");
            }
            None => {
                send_best_effort(&mut stream, b"ERR ROTATE not available on this node\n");
                debug!("rejected ROTATE — flag not wired");
            }
        },
        cmd if cmd.starts_with("DURABILITY") => {
            // Parse `DURABILITY <mode>` with any positive whitespace
            // between the verb and the argument. `splitn(2, ' ')` is
            // intentional: future modes (e.g. multi-region variants)
            // will pass additional whitespace-separated parameters
            // through this same line and we don't want to lock in
            // single-space framing now.
            let mut parts = cmd.splitn(2, char::is_whitespace);
            // Discard the verb token; we already matched on it.
            let _ = parts.next();
            let arg = parts.next().map(str::trim).unwrap_or("");
            handle_durability(&mut stream, durability_mode, arg);
        }
        cmd if cmd.starts_with("RAFT-") => {
            handle_voter_change(&mut stream, voter_changes, parse_voter_command(cmd));
        }
        other => {
            debug!(received = %other, "unknown admin command");
            send_best_effort(&mut stream, b"ERR unknown command\n");
        }
    }
}

/// Apply a `DURABILITY <mode>` command. Validates the argument,
/// publishes the new mode through the shared atomic if the node has a
/// response stage wired, and emits an INFO log carrying the prev → next
/// transition for the audit trail. Auth is enforced upstream in
/// [`authenticate`], so reaching this point already implies an
/// operator-signed request.
fn handle_durability(stream: &mut TcpStream, durability_mode: Option<&AtomicU8>, arg: &str) {
    let Some(atomic) = durability_mode else {
        send_best_effort(stream, b"ERR DURABILITY not available on this node\n");
        debug!("rejected DURABILITY — atomic not wired (replica node?)");
        return;
    };
    if arg.is_empty() {
        send_best_effort(
            stream,
            b"ERR DURABILITY requires a mode (local|hybrid|durably-replicated)\n",
        );
        debug!("rejected DURABILITY — missing argument");
        return;
    }
    let Some(next) = DurabilityMode::parse(arg) else {
        // Build the diagnostic into a small stack buffer to avoid
        // allocating on the admin path. The longest valid name is
        // `durably-replicated` (18 chars); a 128-byte buffer covers
        // any reasonable bad input the operator might paste.
        let mut buf = [0u8; 128];
        let msg = format_unknown_mode(&mut buf, arg);
        send_best_effort(stream, msg);
        debug!(received = %arg, "rejected DURABILITY — unknown mode");
        return;
    };
    // Relaxed exchange: the only writer is the admin handler itself
    // (the response stage only reads), and only the current mode
    // matters — losing the ordering of prev observations relative to
    // unrelated events on other threads is fine.
    let prev_byte = atomic.swap(next.as_u8(), Ordering::Relaxed);
    let prev = DurabilityMode::from_u8(prev_byte)
        .map(|m| m.as_str())
        .unwrap_or("<corrupted>");
    send_best_effort(stream, b"OK\n");
    info!(
        prev = prev,
        next = next.as_str(),
        "durability mode changed by operator"
    );
}

/// Format an "unknown mode" diagnostic into `buf` without allocating.
/// Returns the populated subslice. The buffer is sized so the longest
/// realistic operator input fits; truncation is acceptable here since
/// the operator already knows what they typed.
fn format_unknown_mode<'a>(buf: &'a mut [u8], arg: &str) -> &'a [u8] {
    use std::io::Write as _;
    let mut cursor = std::io::Cursor::new(&mut buf[..]);
    // Best-effort write — if `arg` is pathologically long the write
    // truncates and the operator gets a partial message, which is
    // strictly better than allocating on the admin hot path.
    let _ = writeln!(
        cursor,
        "ERR DURABILITY unknown mode `{arg}` (expected local|hybrid|durably-replicated)"
    );
    let n = cursor.position() as usize;
    &cursor.into_inner()[..n]
}

/// How long the admin handler waits for the driver's voter-change reply
/// before giving up. The driver's own deadline ([`VOTER_CHANGE_DEADLINE`]
/// = 10 s) is shorter, so under normal operation the driver always
/// answers first; this is only the backstop for a driver that has died
/// or wedged. The operator's client read timeout must exceed this.
const VOTER_REPLY_TIMEOUT: Duration = Duration::from_secs(15);

/// Parse a `RAFT-ADD-VOTER` / `RAFT-REMOVE-VOTER` command line into a
/// [`VoterChange`]. Pure (no I/O) so the parsing is unit-testable in
/// isolation; the socket round-trip is covered by the E2E.
fn parse_voter_command(line: &str) -> Result<VoterChange, String> {
    let mut parts = line.split_whitespace();
    match parts.next() {
        Some("RAFT-ADD-VOTER") => {
            let node_id = parse_node_id(parts.next())?;
            let raft_addr = parts
                .next()
                .ok_or("RAFT-ADD-VOTER requires <node_id> <raft_addr> <pubkey_b64>")?
                .parse::<SocketAddr>()
                .map_err(|e| format!("invalid raft address: {e}"))?;
            let public_key = crate::server::parse_ed25519_pubkey_b64(
                parts
                    .next()
                    .ok_or("RAFT-ADD-VOTER requires <node_id> <raft_addr> <pubkey_b64>")?,
            )?;
            Ok(VoterChange::Add {
                node_id,
                raft_addr,
                public_key,
            })
        }
        Some("RAFT-REMOVE-VOTER") => Ok(VoterChange::Remove {
            node_id: parse_node_id(parts.next())?,
        }),
        Some(other) => Err(format!("unknown raft command `{other}`")),
        None => Err("empty command".into()),
    }
}

fn parse_node_id(tok: Option<&str>) -> Result<u64, String> {
    tok.ok_or("missing node id")?
        .parse::<u64>()
        .map_err(|e| format!("invalid node id: {e}"))
}

/// Render the driver's reply into the operator-facing response line.
/// Pure so the OK/ERR/timeout formatting is unit-testable without a
/// driver or a real 15 s wait.
fn format_voter_reply(outcome: Result<Result<Vec<u64>, String>, RecvTimeoutError>) -> String {
    match outcome {
        Ok(Ok(voters)) => {
            let list = voters
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            format!("OK voters={list}")
        }
        Ok(Err(reason)) => format!("ERR {reason}"),
        // No reply arrived before the deadline: the driver is alive (it
        // still holds the sender) but too busy or wedged to answer. The
        // change may yet apply — the operator should re-query, not assume
        // failure.
        Err(RecvTimeoutError::Timeout) => {
            "ERR voter change timed out waiting for the control plane".to_string()
        }
        // The driver dropped the reply sender without answering: the
        // control plane thread has exited (shutdown or crash). Distinct
        // from a timeout — the change definitively did not apply here.
        Err(RecvTimeoutError::Disconnected) => {
            "ERR control plane exited before answering the voter change".to_string()
        }
    }
}

/// Apply a parsed `RAFT-ADD-VOTER` / `RAFT-REMOVE-VOTER` command:
/// forward it to the driver and block on the one-shot reply, then write
/// the `OK voters=…` / `ERR …` line. Auth is enforced upstream in
/// [`authenticate`], so reaching here already implies an operator-signed
/// request.
fn handle_voter_change(
    stream: &mut TcpStream,
    voter_changes: Option<&Sender<VoterChangeRequest>>,
    parsed: Result<VoterChange, String>,
) {
    let Some(sender) = voter_changes else {
        send_best_effort(
            stream,
            b"ERR RAFT voter changes not available on this node\n",
        );
        debug!("rejected voter change — channel not wired (raft-less node?)");
        return;
    };
    let change = match parsed {
        Ok(c) => c,
        Err(e) => {
            debug!(error = %e, "rejected voter change — parse error");
            send_voter_line(stream, &format!("ERR {e}"));
            return;
        }
    };
    let (reply_tx, reply_rx) = channel();
    if sender
        .send(VoterChangeRequest {
            change,
            reply: reply_tx,
        })
        .is_err()
    {
        // Receiver gone ⇒ the driver has exited; report rather than block.
        send_best_effort(stream, b"ERR control plane unavailable\n");
        return;
    }
    let line = format_voter_reply(reply_rx.recv_timeout(VOTER_REPLY_TIMEOUT));
    send_voter_line(stream, &line);
}

/// Write a voter-change response line (appending the newline). A heap
/// allocation is fine here: this is a cold operator command path, not
/// the trading hot path, and the voters list is variable-length.
fn send_voter_line(stream: &mut TcpStream, line: &str) {
    let mut out = line.to_string();
    out.push('\n');
    send_best_effort(stream, out.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};

    use ed25519_dalek::{Signer, SigningKey};
    use melin_wire_protocol::control_codec::{
        TAG_AUTH_FAILED, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY,
    };

    #[test]
    fn handler_slot_caps_at_the_limit_and_releases_on_drop() {
        let counter = Arc::new(AtomicUsize::new(0));
        // Fill exactly to the cap.
        let mut slots: Vec<HandlerSlot> = (0..MAX_ADMIN_HANDLERS)
            .map(|_| HandlerSlot::acquire(&counter).expect("acquire under the cap"))
            .collect();
        assert_eq!(counter.load(Ordering::Acquire), MAX_ADMIN_HANDLERS);
        // At the cap the next acquire is shed (the listener drops the conn).
        assert!(
            HandlerSlot::acquire(&counter).is_none(),
            "acquire at the cap must be refused"
        );
        // Releasing one frees exactly one slot.
        slots.pop();
        assert_eq!(counter.load(Ordering::Acquire), MAX_ADMIN_HANDLERS - 1);
        let reacquired = HandlerSlot::acquire(&counter);
        assert!(reacquired.is_some(), "freed capacity must be re-acquirable");
        assert_eq!(counter.load(Ordering::Acquire), MAX_ADMIN_HANDLERS);
        // Every guard releases its slot when dropped.
        drop(reacquired);
        drop(slots);
        assert_eq!(counter.load(Ordering::Acquire), 0, "all slots released");
    }

    fn operator_keys() -> (SigningKey, Arc<AuthorizedKeys>) {
        let signing_key = SigningKey::from_bytes(&[0xAD; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signing_key.verifying_key().to_bytes(),
        );
        let content = format!("operator {pub_b64} test-ops\n");
        let keys = AuthorizedKeys::parse(&content).expect("parse authorized_keys");
        (signing_key, Arc::new(keys))
    }

    fn trader_keys() -> (SigningKey, Arc<AuthorizedKeys>) {
        let signing_key = SigningKey::from_bytes(&[0xBD; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signing_key.verifying_key().to_bytes(),
        );
        let content = format!("trader {pub_b64} test-trader\n");
        let keys = AuthorizedKeys::parse(&content).expect("parse authorized_keys");
        (signing_key, Arc::new(keys))
    }

    /// Perform the transport-level auth handshake on `stream`, returning
    /// the tag byte of the server's final response (`TAG_SERVER_READY` or
    /// `TAG_AUTH_FAILED`). Builds frames directly from the control-codec
    /// wire format so the test needs no exchange-protocol codec.
    fn client_authenticate(stream: &mut TcpStream, key: &SigningKey) -> u8 {
        // Read the Challenge: [len:u32][TAG_CHALLENGE][nonce:32].
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).expect("read challenge len");
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        let mut frame_buf = vec![0u8; frame_len];
        stream
            .read_exact(&mut frame_buf)
            .expect("read challenge payload");
        assert_eq!(frame_buf[0], TAG_CHALLENGE, "expected Challenge");
        let nonce = &frame_buf[1..33];

        // Reply with a ChallengeResponse:
        // [len:u32][seq:u64][TAG_CHALLENGE_RESPONSE][sig:64][pubkey:32].
        let signature = key.sign(nonce);
        let mut frame = Vec::with_capacity(105);
        frame.extend_from_slice(&0u64.to_le_bytes()); // request_seq
        frame.push(TAG_CHALLENGE_RESPONSE);
        frame.extend_from_slice(&signature.to_bytes());
        frame.extend_from_slice(&key.verifying_key().to_bytes());
        stream
            .write_all(&(frame.len() as u32).to_le_bytes())
            .expect("send result len");
        stream.write_all(&frame).expect("send");
        stream.flush().expect("flush");

        // Read the server's result frame and return its tag byte.
        let mut len_buf2 = [0u8; 4];
        stream.read_exact(&mut len_buf2).expect("read result len");
        let result_len = u32::from_le_bytes(len_buf2) as usize;
        let mut result_buf = vec![0u8; result_len];
        stream
            .read_exact(&mut result_buf)
            .expect("read result payload");
        result_buf[0]
    }

    fn ephemeral_listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    /// Helper: connect, authenticate, send a command, return the
    /// server's first response line.
    fn send_command(addr: SocketAddr, key: &SigningKey, command: &[u8]) -> String {
        let mut stream = TcpStream::connect(addr).unwrap();
        assert_eq!(client_authenticate(&mut stream, key), TAG_SERVER_READY);
        stream.write_all(command).unwrap();
        stream.flush().unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        line.trim().to_string()
    }

    #[test]
    fn promote_command_sets_flag_when_wired() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(send_command(addr, &key, b"PROMOTE\n"), "OK");
        assert_eq!(promote.pending(), Some(PromotionRequest::MANUAL));
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn rotate_command_sets_flag_when_wired() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(send_command(addr, &key, b"ROTATE\n"), "OK");
        assert!(rotate.load(Ordering::Acquire));
        assert!(!promote.is_requested());

        shutdown.store(true, Ordering::Release);
    }

    /// On a primary-only node (no promote flag wired), PROMOTE returns
    /// ERR rather than silently no-opping.
    #[test]
    fn promote_rejected_when_not_wired() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            None,
            Some(Arc::clone(&rotate)),
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let resp = send_command(addr, &key, b"PROMOTE\n");
        assert!(resp.starts_with("ERR"), "expected ERR, got {resp}");
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    /// On a node without runtime rotation enabled, ROTATE returns ERR.
    #[test]
    fn rotate_rejected_when_not_wired() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(promote.clone()),
            None,
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let resp = send_command(addr, &key, b"ROTATE\n");
        assert!(resp.starts_with("ERR"), "expected ERR, got {resp}");
        assert!(!promote.is_requested());

        shutdown.store(true, Ordering::Release);
    }

    /// The listener stays alive across multiple commands — important
    /// for ROTATE which an operator may issue many times over a long
    /// run.
    #[test]
    fn listener_handles_multiple_commands() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        // Three rotations, each consuming the flag (simulates the
        // journal stage's CAS).
        for _ in 0..3 {
            assert_eq!(send_command(addr, &key, b"ROTATE\n"), "OK");
            assert!(rotate.load(Ordering::Acquire));
            rotate
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
                .expect("flag should still be set");
            std::thread::sleep(Duration::from_millis(100));
        }

        // Final PROMOTE on the same listener still works.
        assert_eq!(send_command(addr, &key, b"PROMOTE\n"), "OK");
        assert!(promote.is_requested());

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn unknown_command_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let resp = send_command(addr, &key, b"INVALID\n");
        assert!(resp.starts_with("ERR"), "expected ERR, got {resp}");
        assert!(!promote.is_requested());
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn non_operator_key_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (trader_key, auth_keys) = trader_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &trader_key);
        assert_eq!(result, TAG_AUTH_FAILED);
        assert!(!promote.is_requested());
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    /// Driver: spawn an admin listener with only the durability-mode
    /// atomic wired (mirrors a primary-only node in commit 2), pre-seed
    /// it with `initial`, send the supplied command, and return
    /// `(response, mode_after)`.
    fn run_durability(initial: DurabilityMode, cmd: &[u8]) -> (String, Option<DurabilityMode>) {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let mode = Arc::new(AtomicU8::new(initial.as_u8()));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            None,
            None,
            Some(Arc::clone(&mode)),
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let resp = send_command(addr, &key, cmd);
        let after = DurabilityMode::from_u8(mode.load(Ordering::Relaxed));
        shutdown.store(true, Ordering::Release);
        (resp, after)
    }

    #[test]
    fn durability_command_swaps_mode() {
        let (resp, after) = run_durability(DurabilityMode::Hybrid, b"DURABILITY local\n");
        assert_eq!(resp, "OK");
        assert_eq!(after, Some(DurabilityMode::Local));
    }

    #[test]
    fn durability_command_accepts_each_mode() {
        for target in [
            DurabilityMode::Local,
            DurabilityMode::Hybrid,
            DurabilityMode::DurablyReplicated,
        ] {
            let cmd = format!("DURABILITY {}\n", target.as_str());
            let (resp, after) = run_durability(DurabilityMode::Local, cmd.as_bytes());
            assert_eq!(resp, "OK", "mode {target}");
            assert_eq!(after, Some(target));
        }
    }

    #[test]
    fn durability_command_rejects_unknown_mode() {
        let (resp, after) = run_durability(DurabilityMode::Hybrid, b"DURABILITY fast\n");
        assert!(
            resp.starts_with("ERR DURABILITY unknown mode"),
            "expected unknown-mode ERR, got {resp}"
        );
        // Atomic must NOT have been clobbered on a bad command.
        assert_eq!(after, Some(DurabilityMode::Hybrid));
    }

    #[test]
    fn durability_command_rejects_missing_argument() {
        let (resp, after) = run_durability(DurabilityMode::Hybrid, b"DURABILITY\n");
        assert!(
            resp.starts_with("ERR DURABILITY requires a mode"),
            "expected missing-arg ERR, got {resp}"
        );
        assert_eq!(after, Some(DurabilityMode::Hybrid));
    }

    #[test]
    fn durability_command_rejected_when_not_wired() {
        // On a pure-replica node (no response stage), DURABILITY must
        // not silently no-op — operators get a structured ERR.
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(promote.clone()),
            None,
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let resp = send_command(addr, &key, b"DURABILITY local\n");
        assert!(
            resp.starts_with("ERR DURABILITY not available"),
            "expected not-available ERR, got {resp}"
        );

        shutdown.store(true, Ordering::Release);
    }

    // ── Voter-change command tests ──────────────────────────────────

    /// A real Ed25519 public key (base64) derived from `seed`. Must be a
    /// valid curve point, since the parser now rejects non-points.
    fn pubkey_b64(seed: u8) -> String {
        base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            SigningKey::from_bytes(&[seed; 32])
                .verifying_key()
                .to_bytes(),
        )
    }

    #[test]
    fn parse_voter_command_accepts_valid_add_and_remove() {
        let expected_key = SigningKey::from_bytes(&[0x11; 32])
            .verifying_key()
            .to_bytes();
        let key_b64 = pubkey_b64(0x11);
        let add = parse_voter_command(&format!("RAFT-ADD-VOTER 4 127.0.0.1:9000 {key_b64}"))
            .expect("valid add");
        match add {
            VoterChange::Add {
                node_id,
                raft_addr,
                public_key,
            } => {
                assert_eq!(node_id, 4);
                assert_eq!(raft_addr, "127.0.0.1:9000".parse().unwrap());
                assert_eq!(public_key, expected_key);
            }
            other => panic!("expected Add, got {other:?}"),
        }
        match parse_voter_command("RAFT-REMOVE-VOTER 3").expect("valid remove") {
            VoterChange::Remove { node_id } => assert_eq!(node_id, 3),
            other => panic!("expected Remove, got {other:?}"),
        }
    }

    #[test]
    fn parse_voter_command_rejects_malformed_input() {
        // Bad node id.
        assert!(parse_voter_command("RAFT-ADD-VOTER x 127.0.0.1:9000 AAAA").is_err());
        // Bad address.
        assert!(
            parse_voter_command(&format!("RAFT-ADD-VOTER 4 not-an-addr {}", pubkey_b64(1)))
                .is_err()
        );
        // Bad base64 pubkey.
        assert!(parse_voter_command("RAFT-ADD-VOTER 4 127.0.0.1:9000 not-base64!!").is_err());
        // Right base64 but wrong length.
        let short = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, [0u8; 16]);
        assert!(parse_voter_command(&format!("RAFT-ADD-VOTER 4 127.0.0.1:9000 {short}")).is_err());
        // Missing args.
        assert!(parse_voter_command("RAFT-ADD-VOTER 4").is_err());
        assert!(parse_voter_command("RAFT-REMOVE-VOTER").is_err());
        // Unknown verb under the RAFT- prefix.
        assert!(parse_voter_command("RAFT-FROB 4").is_err());
    }

    #[test]
    fn format_voter_reply_renders_each_outcome() {
        assert_eq!(
            format_voter_reply(Ok(Ok(vec![1, 2, 3, 4]))),
            "OK voters=1,2,3,4"
        );
        assert_eq!(
            format_voter_reply(Ok(Err("node 2 currently leads".into()))),
            "ERR node 2 currently leads"
        );
        // Timeout and Disconnected are distinct operator-facing outcomes:
        // "timed out" (driver alive but slow) vs "control plane exited"
        // (driver gone). Both are ERR but must not read identically.
        let timed_out = format_voter_reply(Err(RecvTimeoutError::Timeout));
        let disconnected = format_voter_reply(Err(RecvTimeoutError::Disconnected));
        assert!(timed_out.starts_with("ERR"));
        assert!(disconnected.starts_with("ERR"));
        assert!(timed_out.contains("timed out"));
        assert!(disconnected.contains("exited"));
        assert_ne!(timed_out, disconnected);
    }

    #[test]
    fn voter_command_round_trips_through_the_driver_channel() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let shutdown = Arc::new(AtomicBool::new(false));
        let (voter_tx, voter_rx) = channel::<VoterChangeRequest>();
        // Canned "driver": answer one request with a fixed voter set.
        let driver = std::thread::spawn(move || {
            if let Ok(req) = voter_rx.recv() {
                let _ = req.reply.send(Ok(vec![1, 2, 4]));
            }
        });
        let _h = spawn(
            addr,
            None,
            None,
            None,
            Some(voter_tx),
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let cmd = format!("RAFT-ADD-VOTER 4 127.0.0.1:9000 {}\n", pubkey_b64(0x22));
        assert_eq!(send_command(addr, &key, cmd.as_bytes()), "OK voters=1,2,4");

        driver.join().expect("driver thread");
        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn voter_command_rejected_when_not_wired() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let shutdown = Arc::new(AtomicBool::new(false));
        // No voter channel wired (raft-less node).
        let _h = spawn(
            addr,
            None,
            None,
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let resp = send_command(addr, &key, b"RAFT-REMOVE-VOTER 3\n");
        assert!(
            resp.starts_with("ERR RAFT voter changes not available"),
            "expected not-available ERR, got {resp}"
        );

        shutdown.store(true, Ordering::Release);
    }

    /// A voter change blocked on its raft commit must not stall other
    /// admin commands — the head-of-line-blocking guarantee that
    /// per-connection handler threads provide. A canned driver holds the
    /// voter reply until released; meanwhile a PROMOTE on a second
    /// connection must still be served. On the old single-threaded
    /// listener the PROMOTE would not even be accepted until the voter
    /// change returned.
    #[test]
    fn a_blocked_voter_change_does_not_stall_other_commands() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let shutdown = Arc::new(AtomicBool::new(false));
        let promote = PromotionRequest::new();
        let (voter_tx, voter_rx) = channel::<VoterChangeRequest>();
        let (release_tx, release_rx) = channel::<()>();
        // Driver that receives the voter request but holds its reply
        // until released, keeping connection 1's handler blocked.
        let driver = std::thread::spawn(move || {
            if let Ok(req) = voter_rx.recv() {
                let _ = release_rx.recv();
                let _ = req.reply.send(Ok(vec![1, 2, 3, 4]));
            }
        });
        let _h = spawn(
            addr,
            Some(promote.clone()),
            None,
            None,
            Some(voter_tx),
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        // Connection 1: blocks in its handler until the driver is released.
        let key1 = key.clone();
        let cmd = format!("RAFT-ADD-VOTER 4 127.0.0.1:9000 {}\n", pubkey_b64(0x22));
        let conn1 = std::thread::spawn(move || send_command(addr, &key1, cmd.as_bytes()));
        // Let connection 1 reach the blocking wait.
        std::thread::sleep(Duration::from_millis(200));

        // Connection 2: a PROMOTE must be served while connection 1 is
        // still blocked — impossible without per-connection threads.
        assert_eq!(send_command(addr, &key, b"PROMOTE\n"), "OK");
        assert!(promote.is_requested());

        // Release connection 1 and confirm its reply landed too.
        release_tx.send(()).expect("release");
        assert_eq!(conn1.join().expect("conn1"), "OK voters=1,2,3,4");

        shutdown.store(true, Ordering::Release);
        driver.join().expect("driver");
    }
}
