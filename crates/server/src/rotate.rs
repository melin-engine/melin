//! Runtime journal rotation endpoint — TCP listener that signals the
//! journal stage to rotate the live segment.
//!
//! An operator connects, authenticates via Ed25519 challenge-response
//! (operator keys only), and sends `ROTATE\n`. The listener flips a
//! shared `AtomicBool` that the journal stage observes at the next
//! fsync boundary, archives the live segment to a fresh monotonic
//! number, and opens a new live segment continuing the chain.
//!
//! Unlike [`crate::promote`], this listener stays alive across
//! rotations: an exchange operator may rotate many times over the life
//! of a process. The flag is consumed by the journal stage with a CAS
//! so concurrent ROTATEs collapse to a single rotation rather than
//! queueing.
//!
//! Recovery walks all archived segments before the live segment, so no
//! handshake with the snapshot stage is needed at rotation time —
//! events written before the rotation remain replayable from disk.

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

/// Spawn the rotation listener on a dedicated thread.
///
/// The listener accepts connections one at a time, authenticates
/// operator keys via Ed25519 challenge-response, and on `ROTATE\n`
/// sets the `rotate_requested` flag. Returns immediately. The thread
/// runs until `shutdown` flips. Panics inside `run` are caught and
/// surfaced via `tracing::error!` so a crashed listener doesn't leave
/// the operator wondering why ROTATE stopped working.
pub fn spawn(
    bind_addr: SocketAddr,
    rotate_requested: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("rotate-listener".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run(bind_addr, &rotate_requested, &shutdown, &authorized_keys)
            }));
            if let Err(panic) = result {
                let msg = panic_message(&panic);
                error!(addr = %bind_addr, panic = %msg, "rotate listener thread panicked");
            }
        })
        .expect("failed to spawn rotate listener thread")
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
    rotate_requested: &AtomicBool,
    shutdown: &AtomicBool,
    authorized_keys: &AuthorizedKeys,
) {
    let listener = match TcpListener::bind(bind_addr) {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %bind_addr, error = %e, "rotate listener bind failed");
            return;
        }
    };
    listener
        .set_nonblocking(true)
        .expect("set rotate listener nonblocking");

    info!(addr = %bind_addr, "rotate listener started");

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        match listener.accept() {
            Ok((stream, peer)) => {
                debug!(peer = %peer, "rotate connection accepted");
                if handle_connection(stream, rotate_requested, authorized_keys) {
                    info!("rotation requested by operator");
                    // Loop back to accept further commands.
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                debug!(error = %e, "rotate listener accept error");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Authenticate an operator connection. Mirrors `promote::authenticate`;
/// kept as a separate copy because the protocol surface is small and
/// duplicating it avoids cross-module coupling on the auth code path.
fn authenticate(stream: &mut TcpStream, authorized_keys: &AuthorizedKeys) -> Result<(), String> {
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| format!("getrandom failed: {e}"))?;

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
            "rotation requires operator key, got {permission:?}"
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

/// Handle a single connection. Returns `true` when the rotation flag
/// was successfully set.
fn handle_connection(
    mut stream: TcpStream,
    rotate_requested: &AtomicBool,
    authorized_keys: &AuthorizedKeys,
) -> bool {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

    if let Err(reason) = authenticate(&mut stream, authorized_keys) {
        debug!(reason = %reason, "rotate auth failed");
        return false;
    }

    let cloned = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            debug!(error = %e, "failed to clone rotate stream");
            return false;
        }
    };
    let mut reader = BufReader::new(cloned);

    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        debug!("failed to read from rotate connection");
        return false;
    }

    if line.trim() == "ROTATE" {
        rotate_requested.store(true, Ordering::Release);
        let _ = stream.write_all(b"OK\n");
        let _ = stream.flush();
        true
    } else {
        debug!(received = %line.trim(), "unexpected rotate command");
        let _ = stream.write_all(b"ERR unknown command\n");
        let _ = stream.flush();
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};

    use ed25519_dalek::{Signer, SigningKey};

    fn operator_keys() -> (SigningKey, Arc<AuthorizedKeys>) {
        let signing_key = SigningKey::from_bytes(&[0xAC; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signing_key.verifying_key().to_bytes(),
        );
        let content = format!("operator {pub_b64} test-ops\n");
        let keys = AuthorizedKeys::parse(&content).expect("parse authorized_keys");
        (signing_key, Arc::new(keys))
    }

    fn trader_keys() -> (SigningKey, Arc<AuthorizedKeys>) {
        let signing_key = SigningKey::from_bytes(&[0xBC; 32]);
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

    #[test]
    fn rotate_command_sets_flag() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&rotate), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &key);
        assert!(matches!(result, ResponseKind::ServerReady));

        stream.write_all(b"ROTATE\n").unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        assert_eq!(response.trim(), "OK");

        assert!(rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn listener_handles_multiple_rotations() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&rotate), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        // First rotation.
        let mut stream = TcpStream::connect(addr).unwrap();
        assert!(matches!(
            client_authenticate(&mut stream, &key),
            ResponseKind::ServerReady
        ));
        stream.write_all(b"ROTATE\n").unwrap();
        stream.flush().unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "OK");
        assert!(rotate.load(Ordering::Acquire));

        // Simulate the journal stage consuming the flag (CAS true→false).
        rotate
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
            .expect("flag should still be set");

        std::thread::sleep(Duration::from_millis(200));

        // Second rotation on the same listener.
        let mut stream = TcpStream::connect(addr).unwrap();
        assert!(matches!(
            client_authenticate(&mut stream, &key),
            ResponseKind::ServerReady
        ));
        stream.write_all(b"ROTATE\n").unwrap();
        stream.flush().unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "OK");
        assert!(rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn non_operator_key_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (trader_key, auth_keys) = trader_keys();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&rotate), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &trader_key);
        assert!(matches!(result, ResponseKind::AuthFailed));
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn invalid_command_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let rotate = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&rotate), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();
        assert!(matches!(
            client_authenticate(&mut stream, &key),
            ResponseKind::ServerReady
        ));
        stream.write_all(b"INVALID\n").unwrap();
        stream.flush().unwrap();
        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        assert!(response.starts_with("ERR"));
        assert!(!rotate.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }
}
