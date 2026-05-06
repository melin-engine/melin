//! Promotion trigger endpoint — plain TCP listener that signals a replica
//! to promote itself to primary.
//!
//! An operator connects, authenticates via Ed25519 challenge-response
//! (same scheme as all other connections), and sends `PROMOTE\n`. Only
//! keys with `Operator` permission are accepted. The listener sets an
//! `AtomicBool` flag that the replica's receive loop checks, then
//! responds with `OK\n` and closes.

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

/// Spawn the promotion listener on a dedicated thread.
///
/// Returns the join handle. The listener accepts one connection at a time,
/// authenticates via Ed25519 challenge-response (operator keys only),
/// checks for the "PROMOTE" command, and sets the flag. The thread exits
/// when `shutdown` is set or after a successful promotion.
///
/// Both call sites (TCP `run_with_shutdown`, rumcast `run_rumcast_replica`)
/// drop the returned handle without joining — the listener runs for the
/// lifetime of the process and exits when `shutdown` flips. Without
/// special handling a panic inside `run` would be silently swallowed by
/// the never-joined handle. We wrap `run` in `catch_unwind` here so the
/// panic surfaces as a `tracing::error!` line; the rest of the process
/// keeps running (a panicking listener doesn't compromise replica
/// correctness, just blocks future promotion attempts until restart).
pub fn spawn(
    bind_addr: SocketAddr,
    promote: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("promote-listener".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run(bind_addr, &promote, &shutdown, &authorized_keys)
            }));
            if let Err(panic) = result {
                let msg = panic_message(&panic);
                error!(addr = %bind_addr, panic = %msg, "promote listener thread panicked");
            }
        })
        .expect("failed to spawn promote listener thread")
}

/// Best-effort extraction of a panic payload's display message. Most
/// panics carry a `&'static str` or `String`; anything else falls back
/// to a placeholder so we still get a log line.
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
    promote: &AtomicBool,
    shutdown: &AtomicBool,
    authorized_keys: &AuthorizedKeys,
) {
    let listener = match TcpListener::bind(bind_addr) {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %bind_addr, error = %e, "promote listener bind failed");
            return;
        }
    };
    // Non-blocking accept so we can check the shutdown flag periodically.
    listener
        .set_nonblocking(true)
        .expect("set promote listener nonblocking");

    info!(addr = %bind_addr, "promote listener started");

    loop {
        if shutdown.load(Ordering::Relaxed) || promote.load(Ordering::Relaxed) {
            return;
        }

        match listener.accept() {
            Ok((stream, peer)) => {
                debug!(peer = %peer, "promote connection accepted");
                if handle_connection(stream, promote, authorized_keys) {
                    info!("promotion triggered");
                    return;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                debug!(error = %e, "promote listener accept error");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Authenticate a connection via Ed25519 challenge-response.
///
/// Returns `Ok(())` if the connection authenticated with an operator key.
/// Returns `Err` with a reason string on any failure.
fn authenticate(stream: &mut TcpStream, authorized_keys: &AuthorizedKeys) -> Result<(), String> {
    // Generate a 32-byte random nonce.
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| format!("getrandom failed: {e}"))?;

    // Send Challenge. X25519 ephemerals are rumcast-only; promote
    // (TCP) uses zeros — see [`melin_protocol::auth::auth_signing_payload`].
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

    // Read ChallengeResponse frame (length-prefixed).
    let mut len_buf = [0u8; 4];
    std::io::Read::read_exact(stream, &mut len_buf)
        .map_err(|e| format!("read auth frame length: {e}"))?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    // ChallengeResponse: 1 (tag) + 64 (signature) + 32 (public key) = 97 bytes.
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

    // Look up the public key — must be an operator key.
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
            "promotion requires operator key, got {permission:?}"
        ));
    }

    // Verify the Ed25519 signature over `nonce ‖ server_eph ‖
    // client_eph` (TCP path's ephs are zeros — see Challenge above).
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

    // Auth succeeded — send ServerReady.
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

/// Send AuthFailed response (best-effort).
fn send_auth_failed(stream: &mut TcpStream) {
    let mut buf = [0u8; 8];
    if let Ok(written) = codec::encode_response(&ResponseKind::AuthFailed, &mut buf) {
        let _ = stream.write_all(&buf[..written]);
        let _ = stream.flush();
    }
}

/// Handle a single connection. Returns `true` if promotion was triggered.
fn handle_connection(
    mut stream: TcpStream,
    promote: &AtomicBool,
    authorized_keys: &AuthorizedKeys,
) -> bool {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();

    // Authenticate via Ed25519 challenge-response (operator only).
    if let Err(reason) = authenticate(&mut stream, authorized_keys) {
        debug!(reason = %reason, "promote auth failed");
        return false;
    }

    // Read the PROMOTE command (plain text after auth).
    let cloned = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            debug!(error = %e, "failed to clone promote stream");
            return false;
        }
    };
    let mut reader = BufReader::new(cloned);

    let mut line = String::new();
    if reader.read_line(&mut line).is_err() {
        debug!("failed to read from promote connection");
        return false;
    }

    if line.trim() == "PROMOTE" {
        promote.store(true, Ordering::Release);
        let _ = stream.write_all(b"OK\n");
        let _ = stream.flush();
        true
    } else {
        debug!(received = %line.trim(), "unexpected promote command");
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
    use melin_protocol::codec;
    use melin_protocol::message::ResponseKind;

    /// Create an `AuthorizedKeys` with one operator key. Returns the signing
    /// key and the authorized keys.
    fn operator_keys() -> (SigningKey, Arc<AuthorizedKeys>) {
        let signing_key = SigningKey::from_bytes(&[0xAA; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signing_key.verifying_key().to_bytes(),
        );
        let content = format!("operator {pub_b64} test-ops\n");
        let keys = AuthorizedKeys::parse(&content).expect("parse authorized_keys");
        (signing_key, Arc::new(keys))
    }

    /// Create a trader (non-operator) key registered in authorized_keys.
    fn trader_keys() -> (SigningKey, Arc<AuthorizedKeys>) {
        let signing_key = SigningKey::from_bytes(&[0xBB; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            signing_key.verifying_key().to_bytes(),
        );
        let content = format!("trader {pub_b64} test-trader\n");
        let keys = AuthorizedKeys::parse(&content).expect("parse authorized_keys");
        (signing_key, Arc::new(keys))
    }

    /// Perform the client side of the Ed25519 challenge-response handshake.
    fn client_authenticate(stream: &mut TcpStream, key: &SigningKey) -> ResponseKind {
        // Read Challenge frame.
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

        // Sign nonce + ephemerals (zeros for TCP).
        let client_x25519_eph = [0u8; 32];
        let signing_payload =
            melin_protocol::auth::auth_signing_payload(&nonce, &server_eph, &client_x25519_eph);
        let signature = key.sign(&signing_payload);
        let request = melin_protocol::message::Request::ChallengeResponse {
            signature: signature.to_bytes(),
            public_key: key.verifying_key().to_bytes(),
            client_x25519_eph,
        };
        let mut encode_buf = [0u8; 256];
        let written = codec::encode_request(&request, 0, &mut encode_buf).expect("encode");
        // Send full frame including the 4-byte length prefix — the server's
        // authenticate() reads length + payload separately.
        stream.write_all(&encode_buf[..written]).expect("send");
        stream.flush().expect("flush");

        // Read auth result.
        let mut len_buf2 = [0u8; 4];
        stream.read_exact(&mut len_buf2).expect("read result len");
        let result_len = u32::from_le_bytes(len_buf2) as usize;
        let mut result_buf = vec![0u8; result_len];
        stream
            .read_exact(&mut result_buf)
            .expect("read result payload");
        codec::decode_response(&result_buf).expect("decode result")
    }

    /// Helper: bind to an ephemeral port and return the listener + address.
    fn ephemeral_listener() -> (TcpListener, SocketAddr) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        (listener, addr)
    }

    #[test]
    fn promote_command_sets_flag() {
        let (listener, addr) = ephemeral_listener();
        drop(listener); // free the port for the promote listener

        let (key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown), auth_keys);

        // Give listener time to start.
        std::thread::sleep(Duration::from_millis(200));

        // Connect and authenticate.
        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &key);
        assert!(matches!(result, ResponseKind::ServerReady));

        // Send PROMOTE.
        stream.write_all(b"PROMOTE\n").unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        assert_eq!(response.trim(), "OK");

        // Flag should be set.
        assert!(promote.load(Ordering::Acquire));

        handle.join().unwrap();
    }

    #[test]
    fn invalid_command_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &key);
        assert!(matches!(result, ResponseKind::ServerReady));

        stream.write_all(b"INVALID\n").unwrap();
        stream.flush().unwrap();

        let mut reader = BufReader::new(stream);
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        assert!(response.starts_with("ERR"));

        // Flag should NOT be set.
        assert!(!promote.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn unauthenticated_connection_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (_key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        // Send PROMOTE without authenticating — should be rejected.
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.write_all(b"PROMOTE\n").unwrap();
        stream.flush().unwrap();

        // The server sends a Challenge first; the raw PROMOTE bytes will
        // fail to parse as a valid ChallengeResponse. Connection should
        // be dropped without setting the flag.
        std::thread::sleep(Duration::from_millis(200));
        assert!(!promote.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn unknown_key_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (_key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        // Authenticate with a key that's not in authorized_keys.
        let unknown_key = SigningKey::from_bytes(&[0xFF; 32]);
        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &unknown_key);
        assert!(matches!(result, ResponseKind::AuthFailed));

        assert!(!promote.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    #[test]
    fn non_operator_key_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (trader_key, auth_keys) = trader_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        // Authenticate with a trader key — should be rejected (operator required).
        let mut stream = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream, &trader_key);
        assert!(matches!(result, ResponseKind::AuthFailed));

        assert!(!promote.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    /// Send a ChallengeResponse with the correct operator public key but
    /// a signature over wrong data (simulates replay / forged signature).
    #[test]
    fn bad_signature_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (operator_key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();

        // Read the Challenge.
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        let mut frame_buf = vec![0u8; frame_len];
        stream.read_exact(&mut frame_buf).unwrap();
        let _nonce = match codec::decode_response(&frame_buf).unwrap() {
            ResponseKind::Challenge { nonce, .. } => nonce,
            other => panic!("expected Challenge, got {other:?}"),
        };

        // Sign wrong data (all zeros instead of the nonce) — the public
        // key is valid and authorized, but the signature won't verify.
        let wrong_data = [0u8; 32];
        let bad_sig = operator_key.sign(&wrong_data);
        let request = melin_protocol::message::Request::ChallengeResponse {
            signature: bad_sig.to_bytes(),
            public_key: operator_key.verifying_key().to_bytes(),
            client_x25519_eph: [0u8; 32],
        };
        let mut encode_buf = [0u8; 256];
        let written = codec::encode_request(&request, 0, &mut encode_buf).unwrap();
        stream.write_all(&encode_buf[..written]).unwrap();
        stream.flush().unwrap();

        // Read auth result — must be AuthFailed.
        stream.read_exact(&mut len_buf).unwrap();
        let result_len = u32::from_le_bytes(len_buf) as usize;
        let mut result_buf = vec![0u8; result_len];
        stream.read_exact(&mut result_buf).unwrap();
        let result = codec::decode_response(&result_buf).unwrap();
        assert!(matches!(result, ResponseKind::AuthFailed));

        assert!(!promote.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    /// An oversized ChallengeResponse frame (> 256 bytes) must be rejected
    /// without reading the payload, preventing memory abuse.
    #[test]
    fn oversized_frame_rejected() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (_key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let _handle = spawn(addr, Arc::clone(&promote), Arc::clone(&shutdown), auth_keys);

        std::thread::sleep(Duration::from_millis(200));

        let mut stream = TcpStream::connect(addr).unwrap();

        // Read and discard the Challenge.
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let frame_len = u32::from_le_bytes(len_buf) as usize;
        let mut frame_buf = vec![0u8; frame_len];
        stream.read_exact(&mut frame_buf).unwrap();

        // Send an oversized frame length (> 256).
        let fake_len: u32 = 512;
        stream.write_all(&fake_len.to_le_bytes()).unwrap();
        stream.flush().unwrap();

        // Server should send AuthFailed.
        stream.read_exact(&mut len_buf).unwrap();
        let result_len = u32::from_le_bytes(len_buf) as usize;
        let mut result_buf = vec![0u8; result_len];
        stream.read_exact(&mut result_buf).unwrap();
        let result = codec::decode_response(&result_buf).unwrap();
        assert!(matches!(result, ResponseKind::AuthFailed));

        assert!(!promote.load(Ordering::Acquire));

        shutdown.store(true, Ordering::Release);
    }

    /// After a failed auth attempt, the listener must remain available
    /// for a subsequent valid promotion.
    #[test]
    fn listener_accepts_after_failed_auth() {
        let (listener, addr) = ephemeral_listener();
        drop(listener);

        let (operator_key, auth_keys) = operator_keys();
        let promote = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));
        let handle = spawn(
            addr,
            Arc::clone(&promote),
            Arc::clone(&shutdown),
            Arc::clone(&auth_keys),
        );

        std::thread::sleep(Duration::from_millis(200));

        // First attempt: unknown key — must fail.
        let bad_key = SigningKey::from_bytes(&[0xFF; 32]);
        let mut stream1 = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream1, &bad_key);
        assert!(matches!(result, ResponseKind::AuthFailed));
        drop(stream1);

        assert!(!promote.load(Ordering::Acquire));

        // Brief pause for the listener to loop back to accept.
        std::thread::sleep(Duration::from_millis(200));

        // Second attempt: valid operator key — must succeed.
        let mut stream2 = TcpStream::connect(addr).unwrap();
        let result = client_authenticate(&mut stream2, &operator_key);
        assert!(matches!(result, ResponseKind::ServerReady));

        stream2.write_all(b"PROMOTE\n").unwrap();
        stream2.flush().unwrap();

        let mut reader = BufReader::new(stream2);
        let mut response = String::new();
        reader.read_line(&mut response).unwrap();
        assert_eq!(response.trim(), "OK");

        assert!(promote.load(Ordering::Acquire));
        handle.join().unwrap();
    }
}
