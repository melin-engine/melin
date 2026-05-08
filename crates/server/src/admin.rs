//! Operator admin endpoint — single TCP listener for all server-side
//! commands an exchange operator may send.
//!
//! Authentication is Ed25519 challenge-response with operator-only keys
//! (the same handshake used for trading sessions). After auth, the
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

use ed25519_dalek::{Verifier, VerifyingKey};
use tracing::{debug, error, info};

use melin_protocol::auth::{AuthorizedKeys, Permission};
use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};

/// Spawn the admin listener on a dedicated thread.
///
/// Either or both of `promote` / `rotate_requested` may be `None` to
/// disable the corresponding command on this node. The listener still
/// accepts connections and authenticates them — a disabled command is
/// rejected at the command-dispatch step, not at connect time, so
/// operators tooling sees a structured ERR rather than a TCP RST.
pub fn spawn(
    bind_addr: SocketAddr,
    promote: Option<Arc<AtomicBool>>,
    rotate_requested: Option<Arc<AtomicBool>>,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("admin-listener".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run(
                    bind_addr,
                    promote.as_deref(),
                    rotate_requested.as_deref(),
                    &shutdown,
                    &authorized_keys,
                )
            }));
            if let Err(panic) = result {
                let msg = panic_message(&panic);
                error!(addr = %bind_addr, panic = %msg, "admin listener thread panicked");
            }
        })
        .expect("failed to spawn admin listener thread")
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
    promote: Option<&AtomicBool>,
    rotate_requested: Option<&AtomicBool>,
    shutdown: &AtomicBool,
    authorized_keys: &AuthorizedKeys,
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
        "admin listener started"
    );

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        match listener.accept() {
            Ok((stream, peer)) => {
                debug!(peer = %peer, "admin connection accepted");
                handle_connection(stream, promote, rotate_requested, authorized_keys);
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

    // X25519 ephemerals are rumcast-only; admin uses TCP and sends zeros
    // here — see [`melin_protocol::auth::auth_signing_payload`].
    let server_x25519_eph = [0u8; 32];
    let mut buf = [0u8; 128];
    let written = codec::encode_response(
        &ResponseKind::Challenge {
            nonce,
            server_x25519_eph,
        },
        &mut buf,
    )
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

    let (_seq, request) = match codec::decode_request(&frame_buf[..frame_len]) {
        Ok(pair) => pair,
        Err(e) => {
            send_auth_failed(stream);
            return Err(format!("decode ChallengeResponse: {e}"));
        }
    };

    let (signature_bytes, public_key_bytes, client_x25519_eph) = match request {
        Request::ChallengeResponse {
            signature,
            public_key,
            client_x25519_eph,
        } => (signature, public_key, client_x25519_eph),
        _ => {
            send_auth_failed(stream);
            return Err("expected ChallengeResponse".into());
        }
    };

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
    let signing_payload =
        melin_protocol::auth::auth_signing_payload(&nonce, &server_x25519_eph, &client_x25519_eph);
    verifying_key
        .verify(&signing_payload, &signature)
        .map_err(|e| {
            send_auth_failed(stream);
            format!("signature verification failed: {e}")
        })?;

    let written = codec::encode_response(&ResponseKind::ServerReady, &mut buf)
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
    if let Ok(written) = codec::encode_response(&ResponseKind::AuthFailed, &mut buf) {
        let _ = stream.write_all(&buf[..written]);
        let _ = stream.flush();
    }
}

/// Handle one authenticated admin connection. Reads a single command
/// line and dispatches it.
fn handle_connection(
    mut stream: TcpStream,
    promote: Option<&AtomicBool>,
    rotate_requested: Option<&AtomicBool>,
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

    match line.trim() {
        "PROMOTE" => match promote {
            Some(flag) => {
                flag.store(true, Ordering::Release);
                let _ = stream.write_all(b"OK\n");
                let _ = stream.flush();
                info!("promotion triggered by operator");
            }
            None => {
                let _ = stream.write_all(b"ERR PROMOTE not available on this node\n");
                let _ = stream.flush();
                debug!("rejected PROMOTE — flag not wired (primary node?)");
            }
        },
        "ROTATE" => match rotate_requested {
            Some(flag) => {
                flag.store(true, Ordering::Release);
                let _ = stream.write_all(b"OK\n");
                let _ = stream.flush();
                info!("rotation requested by operator");
            }
            None => {
                let _ = stream.write_all(b"ERR ROTATE not available on this node\n");
                let _ = stream.flush();
                debug!("rejected ROTATE — flag not wired");
            }
        },
        other => {
            debug!(received = %other, "unknown admin command");
            let _ = stream.write_all(b"ERR unknown command\n");
            let _ = stream.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};

    use ed25519_dalek::{Signer, SigningKey};

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

    fn client_authenticate(stream: &mut TcpStream, key: &SigningKey) -> ResponseKind {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).expect("read challenge len");
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        let mut frame_buf = vec![0u8; frame_len];
        stream
            .read_exact(&mut frame_buf)
            .expect("read challenge payload");
        let response = codec::decode_response(&frame_buf).expect("decode challenge");
        let (nonce, server_eph) = match response {
            ResponseKind::Challenge {
                nonce,
                server_x25519_eph,
            } => (nonce, server_x25519_eph),
            other => panic!("expected Challenge, got {other:?}"),
        };

        let client_x25519_eph = [0u8; 32];
        let signing_payload =
            melin_protocol::auth::auth_signing_payload(&nonce, &server_eph, &client_x25519_eph);
        let signature = key.sign(&signing_payload);
        let request = Request::ChallengeResponse {
            signature: signature.to_bytes(),
            public_key: key.verifying_key().to_bytes(),
            client_x25519_eph,
        };
        let mut encode_buf = [0u8; 256];
        let written = codec::encode_request(&request, 0, &mut encode_buf).expect("encode");
        stream.write_all(&encode_buf[..written]).expect("send");
        stream.flush().expect("flush");

        let mut len_buf2 = [0u8; 4];
        stream.read_exact(&mut len_buf2).expect("read result len");
        let result_len = u32::from_le_bytes(len_buf2) as usize;
        let mut result_buf = vec![0u8; result_len];
        stream
            .read_exact(&mut result_buf)
            .expect("read result payload");
        codec::decode_response(&result_buf).expect("decode result")
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
        assert!(matches!(
            client_authenticate(&mut stream, key),
            ResponseKind::ServerReady
        ));
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
        let promote = Arc::new(AtomicBool::new(false));
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(Arc::clone(&promote)),
            Some(Arc::clone(&rotate)),
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(send_command(addr, &key, b"PROMOTE\n"), "OK");
        assert!(promote.load(Ordering::Acquire));
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn rotate_command_sets_flag_when_wired() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(Arc::clone(&promote)),
            Some(Arc::clone(&rotate)),
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        assert_eq!(send_command(addr, &key, b"ROTATE\n"), "OK");
        assert!(rotate.load(Ordering::Acquire));
        assert!(!promote.load(Ordering::Acquire));

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
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(Arc::clone(&promote)),
            None,
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let resp = send_command(addr, &key, b"ROTATE\n");
        assert!(resp.starts_with("ERR"), "expected ERR, got {resp}");
        assert!(!promote.load(Ordering::Acquire));

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
        let promote = Arc::new(AtomicBool::new(false));
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(Arc::clone(&promote)),
            Some(Arc::clone(&rotate)),
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
        assert!(promote.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn unknown_command_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(Arc::clone(&promote)),
            Some(Arc::clone(&rotate)),
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let resp = send_command(addr, &key, b"INVALID\n");
        assert!(resp.starts_with("ERR"), "expected ERR, got {resp}");
        assert!(!promote.load(Ordering::Acquire));
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn non_operator_key_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (trader_key, auth_keys) = trader_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _h = spawn(
            addr,
            Some(Arc::clone(&promote)),
            Some(Arc::clone(&rotate)),
            Arc::clone(&shutdown),
            auth_keys,
        );
        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &trader_key);
        assert!(matches!(result, ResponseKind::AuthFailed));
        assert!(!promote.load(Ordering::Acquire));
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }
}
