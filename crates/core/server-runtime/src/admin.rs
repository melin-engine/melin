//! Operator admin endpoint — single TCP listener for all server-side
//! commands an operator may send.
//!
//! Authentication is Ed25519 challenge-response with operator-only keys
//! (the same handshake used for client sessions). After auth, the
//! client sends one ASCII command terminated by `\n`:
//!
//! - `PROMOTE` — replica → primary leadership transition. Sets the
//!   shared promote flag; the replica receive loop observes it and
//!   exits with the recovered state. Available only when the spawn
//!   caller wired a promote flag (typically the replica path).
//! - `ROTATE` — archive the current journal segment at the next fsync
//!   boundary and start a fresh one. Available only when the spawn
//!   caller wired a rotation flag (any node with `--max-journal-mib >
//!   0` or runtime rotation enabled).
//! - `ACK-POLICY <disk|ram|disk+ram|two-disks>` — change the ack policy.
//!   The change is journaled, in order with the events it governs, and
//!   takes effect once the nodes the previous policy relied on hold it
//!   (see [`crate::policy_control`]). On a primary the reply is `OK` once
//!   the entry is published, or an `ERR` if the input ring stays full
//!   past a bounded wait; with no replica connected, `ACK-POLICY disk` is
//!   forced and takes effect once on this node's disk. On a replica, or a
//!   primary still starting, the policy is staged and the reply is
//!   `OK staged`: it is journaled when the node is promoted (or its
//!   producer starts) — not forced, so an operator re-sends it once the
//!   node serves if it must take effect with no replica connected. Lets an
//!   operator resume taking writes under a weaker policy after a promotion
//!   (no restart, no client reconnects) and restore the target policy
//!   once replicas reattach. Available only when the spawn caller wired
//!   the policy mailbox. `DURABILITY <local|replicated|hybrid|durably-replicated>`
//!   is accepted as a deprecated alias for one release and logged at
//!   `warn!`.
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

use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::ack_policy::AckPolicy;
use crate::policy_control::{AckPolicyControl, Refused, RequestOutcome};
use crate::promotion::PromotionRequest;

use ed25519_dalek::{Verifier, VerifyingKey};
use tracing::{debug, error, info, warn};

use melin_app::auth::AuthorizedKeys;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;

/// Spawn the admin listener on a dedicated thread.
///
/// The listener is bound (and set non-blocking) here, before the thread
/// starts, so a taken or misconfigured admin port fails startup loudly —
/// bound inside the thread it died with an `error!` nobody joins on,
/// leaving `PROMOTE`/`ROTATE`/`ACK-POLICY` silently dead on a
/// healthy-looking node. Returns the bound address alongside the
/// handle (they differ from `bind_addr` when it carries port 0).
///
/// Any of `promote` / `rotate_requested` / `ack_policy` may be
/// `None` to disable the corresponding command on this node. The
/// listener still accepts connections and authenticates them — a
/// disabled command is rejected at the command-dispatch step, not at
/// connect time, so operator tooling sees a structured ERR rather than
/// a TCP RST.
pub fn spawn(
    bind_addr: SocketAddr,
    promote: Option<PromotionRequest>,
    rotate_requested: Option<Arc<AtomicBool>>,
    ack_policy: Option<Arc<AckPolicyControl>>,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
) -> Result<(JoinHandle<()>, SocketAddr), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(bind_addr)
        .map_err(|e| format!("failed to bind admin listener on {bind_addr}: {e}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to set non-blocking on admin listener: {e}"))?;
    let addr = listener
        .local_addr()
        .map_err(|e| format!("failed to read admin listener address: {e}"))?;
    let handle = std::thread::Builder::new()
        .name("admin-listener".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run(
                    listener,
                    addr,
                    promote.as_ref(),
                    rotate_requested.as_deref(),
                    ack_policy.as_deref(),
                    &shutdown,
                    &authorized_keys,
                )
            }));
            if let Err(panic) = result {
                let msg = panic_message(&panic);
                error!(addr = %addr, panic = %msg, "admin listener thread panicked");
            }
        })
        .map_err(|e| format!("failed to spawn admin listener thread: {e}"))?;
    Ok((handle, addr))
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

/// Accept loop over the listener pre-bound by [`spawn`]. `addr` is the
/// actual bound address, for logs.
fn run(
    listener: TcpListener,
    addr: SocketAddr,
    promote: Option<&PromotionRequest>,
    rotate_requested: Option<&AtomicBool>,
    ack_policy: Option<&AckPolicyControl>,
    shutdown: &AtomicBool,
    authorized_keys: &AuthorizedKeys,
) {
    info!(
        addr = %addr,
        promote_enabled = promote.is_some(),
        rotate_enabled = rotate_requested.is_some(),
        ack_policy_enabled = ack_policy.is_some(),
        "admin listener started"
    );

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        match listener.accept() {
            Ok((stream, peer)) => {
                debug!(peer = %peer, "admin connection accepted");
                handle_connection(
                    stream,
                    promote,
                    rotate_requested,
                    ack_policy,
                    authorized_keys,
                );
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

    let cr = match control_codec::decode_challenge_response(&frame_buf[..frame_len]) {
        Ok(cr) => cr,
        Err(e) => {
            send_auth_failed(stream);
            return Err(format!("decode ChallengeResponse: {e}"));
        }
    };

    let (signature_bytes, public_key_bytes) = (cr.signature, cr.public_key);

    let role = match authorized_keys.lookup(&public_key_bytes) {
        Some(role) => role,
        None => {
            send_auth_failed(stream);
            return Err("unknown public key".into());
        }
    };
    if !role.client().is_some_and(|role| role.is_operator()) {
        send_auth_failed(stream);
        return Err(format!(
            "admin endpoint requires an operator key, got a {} key",
            authorized_keys.token(role)
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
    ack_policy: Option<&AckPolicyControl>,
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
        cmd if cmd.starts_with("ACK-POLICY") => {
            // Parse `ACK-POLICY <policy>` with any positive whitespace
            // between the verb and the argument. `splitn(2, ' ')` is
            // intentional: future policies (e.g. multi-region variants)
            // will pass additional whitespace-separated parameters
            // through this same line and we don't want to lock in
            // single-space framing now.
            let mut parts = cmd.splitn(2, char::is_whitespace);
            // Discard the verb token; we already matched on it.
            let _ = parts.next();
            let arg = parts.next().map(str::trim).unwrap_or("");
            handle_ack_policy(&mut stream, ack_policy, arg);
        }
        cmd if cmd.starts_with("DURABILITY") => {
            // Deprecated spelling from before 0.15, kept for one release
            // because this command's documented use is post-failover
            // remediation — typed from memory, under pressure, when an
            // `ERR unknown command` is the last thing anyone needs. The
            // old policy names are mapped too: the alias is worthless
            // if `DURABILITY local` still fails on the argument.
            let mut parts = cmd.splitn(2, char::is_whitespace);
            let _ = parts.next();
            let arg = parts.next().map(str::trim).unwrap_or("");
            let mapped = AckPolicy::parse_legacy(arg).map(AckPolicy::as_str);
            warn!(
                received = %cmd,
                "DURABILITY is deprecated and will be removed in the next minor release; use ACK-POLICY {}",
                mapped.unwrap_or("<disk|ram|disk+ram|two-disks>")
            );
            handle_ack_policy(&mut stream, ack_policy, mapped.unwrap_or(arg));
        }
        other => {
            debug!(received = %other, "unknown admin command");
            send_best_effort(&mut stream, b"ERR unknown command\n");
        }
    }
}

/// Apply an `ACK-POLICY <policy>` command. Validates the argument, has
/// the change journaled (or staged, see the module docs), and replies
/// with the outcome. The journal entry is the change's audit trail; the
/// log line says who asked. Auth is enforced upstream in
/// [`authenticate`], so reaching this point already implies an
/// operator-signed request.
fn handle_ack_policy(stream: &mut TcpStream, ack_policy: Option<&AckPolicyControl>, arg: &str) {
    let Some(control) = ack_policy else {
        send_best_effort(stream, b"ERR ACK-POLICY not available on this node\n");
        debug!("rejected ACK-POLICY — policy mailbox not wired");
        return;
    };
    if arg.is_empty() {
        send_best_effort(
            stream,
            b"ERR ACK-POLICY requires a policy (disk|ram|disk+ram|two-disks)\n",
        );
        debug!("rejected ACK-POLICY — missing argument");
        return;
    }
    let Some(next) = AckPolicy::parse(arg) else {
        // Build the diagnostic into a small stack buffer to avoid
        // allocating on the admin path. The longest valid name is
        // `two-disks` (9 chars); a 128-byte buffer covers any
        // reasonable bad input the operator might paste.
        let mut buf = [0u8; 128];
        let msg = format_unknown_policy(&mut buf, arg);
        send_best_effort(stream, msg);
        debug!(received = %arg, "rejected ACK-POLICY — unknown policy");
        return;
    };
    let outcome = control.request(next);
    send_best_effort(stream, ack_policy_reply(outcome));
    match outcome {
        RequestOutcome::Published => {
            info!(
                policy = next.as_str(),
                "ack policy change requested by operator"
            );
        }
        RequestOutcome::Staged => {
            info!(
                policy = next.as_str(),
                "ack policy staged by operator: journaled when this node serves as primary"
            );
        }
        RequestOutcome::Refused(Refused::RingFull) => {
            // A full input ring is the pipeline stalled behind its gate:
            // degraded operation, not an operator mistake.
            warn!(
                policy = next.as_str(),
                "ack policy change refused: the input ring stayed full, so it could not be journaled in time"
            );
        }
        RequestOutcome::Refused(Refused::ProducerStopped) => {
            info!(
                policy = next.as_str(),
                "ack policy change refused: the node stopped serving as primary (stepping down or shutting down) before journaling it"
            );
        }
        RequestOutcome::Refused(Refused::AnotherWaiting) => {
            // The admin listener serves one command at a time, so a
            // second requester is unexpected: handled, but worth a look.
            warn!(
                policy = next.as_str(),
                "ack policy change refused: another request was still waiting to be journaled"
            );
        }
        RequestOutcome::Refused(Refused::NoReplicaCanAttach) => {
            // Journaled, it would have held every reply: worth a look at
            // whoever sent it.
            warn!(
                policy = next.as_str(),
                "ack policy change refused: the policy needs a replica and this node accepts none (no --replication-bind), so it could never be met"
            );
        }
    }
}

/// The admin reply to an `ACK-POLICY` request's outcome. Each refusal
/// names its cause, since each calls for something different: retry once
/// the pipeline drains, find the new primary, wait for the request
/// already in flight, or pick a policy this node can meet.
fn ack_policy_reply(outcome: RequestOutcome) -> &'static [u8] {
    match outcome {
        RequestOutcome::Published => b"OK\n",
        RequestOutcome::Staged => b"OK staged\n",
        RequestOutcome::Refused(Refused::RingFull) => {
            b"ERR ACK-POLICY not journaled: the input ring stayed full; retry once the pipeline drains\n"
        }
        RequestOutcome::Refused(Refused::ProducerStopped) => {
            b"ERR ACK-POLICY not journaled: the node stopped serving as primary (stepping down or shutting down)\n"
        }
        RequestOutcome::Refused(Refused::AnotherWaiting) => {
            b"ERR ACK-POLICY not journaled: another ACK-POLICY request is still waiting\n"
        }
        RequestOutcome::Refused(Refused::NoReplicaCanAttach) => {
            b"ERR ACK-POLICY not journaled: this policy needs a replica and this node accepts none (no --replication-bind), so it could never be met\n"
        }
    }
}

/// Format an "unknown policy" diagnostic into `buf` without allocating.
/// Returns the populated subslice. The buffer is sized so the longest
/// realistic operator input fits; truncation is acceptable here since
/// the operator already knows what they typed.
fn format_unknown_policy<'a>(buf: &'a mut [u8], arg: &str) -> &'a [u8] {
    use std::io::Write as _;
    let mut cursor = std::io::Cursor::new(&mut buf[..]);
    // Best-effort write — if `arg` is pathologically long the write
    // truncates and the operator gets a partial message, which is
    // strictly better than allocating on the admin hot path.
    let _ = writeln!(
        cursor,
        "ERR ACK-POLICY unknown policy `{arg}` (expected disk|ram|disk+ram|two-disks)"
    );
    let n = cursor.position() as usize;
    &cursor.into_inner()[..n]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};

    use ed25519_dalek::{Signer, SigningKey};
    use melin_wire_protocol::control_codec::{
        TAG_AUTH_FAILED, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY,
    };

    use crate::test_roles::desk_keys;

    fn operator_keys() -> (SigningKey, Arc<AuthorizedKeys>) {
        let signing_key = SigningKey::from_bytes(&[0xAD; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signing_key.verifying_key().to_bytes(),
        );
        (signing_key, Arc::new(desk_keys("operator", &pub_b64)))
    }

    /// A key under an application role: refused, like any key that is not
    /// the operator's.
    fn trader_keys() -> (SigningKey, Arc<AuthorizedKeys>) {
        let signing_key = SigningKey::from_bytes(&[0xBD; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signing_key.verifying_key().to_bytes(),
        );
        (signing_key, Arc::new(desk_keys("trader", &pub_b64)))
    }

    /// Perform the transport-level auth handshake on `stream`, returning
    /// the tag byte of the server's final response (`TAG_SERVER_READY` or
    /// `TAG_AUTH_FAILED`). Builds frames directly from the control-codec
    /// wire format so the test needs no application codec.
    fn client_authenticate(stream: &mut TcpStream, key: &SigningKey) -> u8 {
        // Read the Challenge, through the wire protocol's one parser of
        // it, which also checks the listener speaks this build's client
        // protocol.
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).expect("read challenge len");
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        let mut frame_buf = vec![0u8; frame_len];
        stream
            .read_exact(&mut frame_buf)
            .expect("read challenge payload");
        let nonce = melin_wire_protocol::control_codec::decode_challenge(&frame_buf)
            .expect("expected Challenge");

        // Reply with a ChallengeResponse:
        // [len:u32][TAG_CHALLENGE_RESPONSE][sig:64][pubkey:32].
        let signature = key.sign(&nonce);
        let mut frame = Vec::with_capacity(97);
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

    /// A taken admin port must fail `spawn` loudly. The old
    /// bind-inside-the-thread pattern died with an `error!` nobody
    /// joins on, leaving PROMOTE/ROTATE/ACK-POLICY silently dead on a
    /// healthy-looking node.
    #[test]
    fn spawn_errs_when_port_taken() {
        let (_key, auth_keys) = operator_keys();
        let holder = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = holder.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let err = spawn(addr, None, None, None, shutdown, auth_keys)
            .expect_err("bind on a taken port must fail");
        assert!(
            err.to_string().contains("failed to bind admin listener"),
            "{err}"
        );
    }

    #[test]
    fn promote_command_sets_flag_when_wired() {
        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");

        assert_eq!(send_command(addr, &key, b"PROMOTE\n"), "OK");
        assert_eq!(promote.pending(), Some(PromotionRequest::MANUAL));
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn rotate_command_sets_flag_when_wired() {
        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");

        assert_eq!(send_command(addr, &key, b"ROTATE\n"), "OK");
        assert!(rotate.load(Ordering::Acquire));
        assert!(!promote.is_requested());

        shutdown.store(true, Ordering::Release);
    }

    /// On a primary-only node (no promote flag wired), PROMOTE returns
    /// ERR rather than silently no-opping.
    #[test]
    fn promote_rejected_when_not_wired() {
        let (key, auth_keys) = operator_keys();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            None,
            Some(Arc::clone(&rotate)),
            None,
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");

        let resp = send_command(addr, &key, b"PROMOTE\n");
        assert!(resp.starts_with("ERR"), "expected ERR, got {resp}");
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    /// On a node without runtime rotation enabled, ROTATE returns ERR.
    #[test]
    fn rotate_rejected_when_not_wired() {
        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Some(promote.clone()),
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");

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
        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");

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
        assert_eq!(promote.pending(), Some(PromotionRequest::MANUAL));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn unknown_command_rejected() {
        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");

        let resp = send_command(addr, &key, b"INVALID\n");
        assert!(resp.starts_with("ERR"), "expected ERR, got {resp}");
        assert!(!promote.is_requested());
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn non_operator_key_rejected() {
        let (trader_key, auth_keys) = trader_keys();
        let promote = PromotionRequest::new();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Some(promote.clone()),
            Some(Arc::clone(&rotate)),
            None,
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");

        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &trader_key);
        assert_eq!(result, TAG_AUTH_FAILED);
        assert!(!promote.is_requested());
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    /// Spawn an admin listener with only the ack-policy mailbox wired.
    fn spawn_with_policy(
        control: &Arc<AckPolicyControl>,
    ) -> (SigningKey, SocketAddr, Arc<AtomicBool>) {
        let (key, auth_keys) = operator_keys();
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            None,
            None,
            Some(Arc::clone(control)),
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");
        (key, addr, shutdown)
    }

    /// Driver: spawn an admin listener whose mailbox starts at `initial`
    /// with no producer attached (a replica, or a primary still
    /// starting), send the supplied command, and return `(response,
    /// policy_after)`: the policy the next promotion would request.
    fn run_ack_policy(initial: AckPolicy, cmd: &[u8]) -> (String, AckPolicy) {
        run_ack_policy_on(
            crate::policy_control::test_support::replicated(initial),
            cmd,
        )
    }

    /// As [`run_ack_policy`], on `control`.
    fn run_ack_policy_on(control: Arc<AckPolicyControl>, cmd: &[u8]) -> (String, AckPolicy) {
        let (key, addr, shutdown) = spawn_with_policy(&control);
        let resp = send_command(addr, &key, cmd);
        let after = control.claim().policy();
        shutdown.store(true, Ordering::Release);
        (resp, after)
    }

    #[test]
    fn ack_policy_command_stages_the_policy_with_no_producer() {
        let (resp, after) = run_ack_policy(AckPolicy::DiskAndRam, b"ACK-POLICY disk\n");
        assert_eq!(resp, "OK staged");
        assert_eq!(after, AckPolicy::Disk);
    }

    /// On a node that accepts no replica, a policy that needs one is
    /// refused with the reason, and nothing is staged.
    #[test]
    fn ack_policy_command_refuses_a_policy_no_replica_can_meet() {
        let standalone =
            || AckPolicyControl::new(AckPolicy::Disk, crate::halt::Replication::Disabled);
        let (resp, after) = run_ack_policy_on(standalone(), b"ACK-POLICY disk+ram\n");
        assert_eq!(
            format!("{resp}\n").as_bytes(),
            ack_policy_reply(RequestOutcome::Refused(Refused::NoReplicaCanAttach)),
            "{resp}"
        );
        assert_eq!(after, AckPolicy::Disk, "nothing staged");
        let (resp, _) = run_ack_policy_on(standalone(), b"ACK-POLICY disk\n");
        assert_eq!(resp, "OK staged", "`disk` is still taken");
    }

    /// On a serving primary the command replies `OK` once its producer has
    /// journaled the change: the entry is on the input ring.
    #[test]
    fn ack_policy_command_is_journaled_by_the_producer() {
        let control = crate::policy_control::test_support::replicated(AckPolicy::DiskAndRam);
        let (mut producer, mut consumer) =
            crate::policy_control::test_support::ring::<counter_server::CounterEvent>(8);
        // No replica connected: a runtime `disk` is forced.
        let replicas = crate::halt::test_support::presence(0);
        let halt = crate::halt::test_support::gate_over(Some(replicas));
        let mut publisher = crate::policy_control::test_support::booted(&control).attach(
            &mut producer,
            &halt,
            None,
        );
        let (key, addr, shutdown) = spawn_with_policy(&control);
        let polling = Arc::new(AtomicBool::new(true));
        let poller = {
            let polling = Arc::clone(&polling);
            std::thread::spawn(move || {
                while polling.load(Ordering::Relaxed) {
                    publisher.poll(&mut producer, &halt);
                    std::thread::yield_now();
                }
            })
        };

        assert_eq!(send_command(addr, &key, b"ACK-POLICY disk\n"), "OK");
        polling.store(false, Ordering::Relaxed);
        poller.join().expect("the producer panicked");
        let (_, slot) = consumer.try_consume().expect("the entry is on the ring");
        let melin_journal::JournalEvent::AckPolicyChange(change) = slot.event else {
            panic!("not a policy change");
        };
        assert_eq!(
            change,
            melin_journal::AckPolicyChange {
                policy: AckPolicy::Disk.to_byte(),
                unwitnessed: true,
            },
            "a runtime `disk` with no replica connected is forced"
        );
        shutdown.store(true, Ordering::Release);
    }

    /// A producer that never publishes (its ring full) gets the command
    /// refused once the bounded wait is out, and nothing changes.
    #[test]
    fn ack_policy_command_refused_when_it_cannot_be_journaled() {
        use crate::policy_control::test_support::{attach, control_with_publish_wait, ring};
        let control = control_with_publish_wait(AckPolicy::DiskAndRam, Duration::from_millis(50));
        let (mut producer, _consumer) = ring::<counter_server::CounterEvent>(8);
        let _publisher = attach(&control, &mut producer);
        let (key, addr, shutdown) = spawn_with_policy(&control);
        let resp = send_command(addr, &key, b"ACK-POLICY disk\n");
        assert_eq!(
            format!("{resp}\n").as_bytes(),
            ack_policy_reply(RequestOutcome::Refused(Refused::RingFull)),
            "{resp}"
        );
        assert_eq!(control.requested(), AckPolicy::DiskAndRam);
        shutdown.store(true, Ordering::Release);
    }

    /// Each outcome has its own reply, and each refusal names its cause.
    #[test]
    fn the_ack_policy_reply_names_each_outcome() {
        let reply = |outcome| std::str::from_utf8(ack_policy_reply(outcome)).expect("ASCII");
        assert_eq!(reply(RequestOutcome::Published), "OK\n");
        assert_eq!(reply(RequestOutcome::Staged), "OK staged\n");
        for (cause, names) in [
            (Refused::RingFull, "the input ring stayed full"),
            (
                Refused::ProducerStopped,
                "stopped serving as primary (stepping down or shutting down)",
            ),
            (Refused::AnotherWaiting, "another ACK-POLICY request"),
            (
                Refused::NoReplicaCanAttach,
                "needs a replica and this node accepts none (no --replication-bind), so it could never be met",
            ),
        ] {
            let text = reply(RequestOutcome::Refused(cause));
            assert!(
                text.starts_with("ERR ACK-POLICY not journaled: ") && text.contains(names),
                "{cause:?}: {text}"
            );
            assert!(text.ends_with('\n') && !text[..text.len() - 1].contains('\n'));
        }
    }

    #[test]
    fn deprecated_durability_alias_maps_old_names() {
        // `DURABILITY local` from muscle memory must still work, and
        // land on the policy the old name meant.
        let (resp, after) = run_ack_policy(AckPolicy::DiskAndRam, b"DURABILITY local\n");
        assert_eq!(resp, "OK staged");
        assert_eq!(after, AckPolicy::Disk);

        let (resp, after) = run_ack_policy(AckPolicy::Disk, b"DURABILITY durably-replicated\n");
        assert_eq!(resp, "OK staged");
        assert_eq!(after, AckPolicy::TwoDisks);

        // The alias also takes the new names, and still rejects junk.
        let (resp, after) = run_ack_policy(AckPolicy::Disk, b"DURABILITY ram\n");
        assert_eq!(resp, "OK staged");
        assert_eq!(after, AckPolicy::Ram);
        let (resp, after) = run_ack_policy(AckPolicy::Disk, b"DURABILITY fast\n");
        assert!(resp.starts_with("ERR ACK-POLICY unknown policy"), "{resp}");
        assert_eq!(after, AckPolicy::Disk);
    }

    #[test]
    fn ack_policy_command_accepts_each_policy() {
        for target in [
            AckPolicy::Disk,
            AckPolicy::Ram,
            AckPolicy::DiskAndRam,
            AckPolicy::TwoDisks,
        ] {
            let cmd = format!("ACK-POLICY {}\n", target.as_str());
            let (resp, after) = run_ack_policy(AckPolicy::Disk, cmd.as_bytes());
            assert_eq!(resp, "OK staged", "policy {target}");
            assert_eq!(after, target);
        }
    }

    #[test]
    fn ack_policy_command_rejects_unknown_policy() {
        let (resp, after) = run_ack_policy(AckPolicy::DiskAndRam, b"ACK-POLICY fast\n");
        assert!(
            resp.starts_with("ERR ACK-POLICY unknown policy"),
            "expected unknown-policy ERR, got {resp}"
        );
        // The requested policy must NOT have been clobbered on a bad command.
        assert_eq!(after, AckPolicy::DiskAndRam);
    }

    #[test]
    fn ack_policy_command_rejects_missing_argument() {
        let (resp, after) = run_ack_policy(AckPolicy::DiskAndRam, b"ACK-POLICY\n");
        assert!(
            resp.starts_with("ERR ACK-POLICY requires a policy"),
            "expected missing-arg ERR, got {resp}"
        );
        assert_eq!(after, AckPolicy::DiskAndRam);
    }

    #[test]
    fn ack_policy_command_rejected_when_not_wired() {
        // On a pure-replica node (no response stage), ACK-POLICY must
        // not silently no-op — operators get a structured ERR.
        let (key, auth_keys) = operator_keys();
        let promote = PromotionRequest::new();
        let shutdown = Arc::new(AtomicBool::new(false));
        let (_h, addr) = spawn(
            "127.0.0.1:0".parse().unwrap(),
            Some(promote.clone()),
            None,
            None,
            Arc::clone(&shutdown),
            auth_keys,
        )
        .expect("spawn admin listener");

        let resp = send_command(addr, &key, b"ACK-POLICY disk\n");
        assert!(
            resp.starts_with("ERR ACK-POLICY not available"),
            "expected not-available ERR, got {resp}"
        );

        shutdown.store(true, Ordering::Release);
    }
}
