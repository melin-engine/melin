//! Full round-trip integration test: start notary-server, connect a TCP
//! client with Ed25519 auth, notarize a series of digests, and check the
//! chain head the server reports against one folded independently on the
//! client — the same check an auditor holding the original documents
//! would perform.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};

use melin_server_runtime::server::{self, ServerConfig};
use melin_wire_protocol::control_codec::{
    TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY,
};
use melin_wire_protocol::tcp::BlockingTcpListener;

use notary_server::{
    GENESIS_HEAD, HEAD_LEN, LEAF_LEN, NotaryFactory, RequestDecoder, ResponseEncoder, TAG_GET_HEAD,
    TAG_NOTARIZE, TAG_RESP_HEAD, TAG_RESP_RECEIPT,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).expect("read frame length");
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).expect("read frame payload");
    payload
}

fn write_frame(stream: &mut TcpStream, payload: &[u8]) {
    let len = (payload.len() as u32).to_le_bytes();
    stream.write_all(&len).expect("write frame length");
    stream.write_all(payload).expect("write frame payload");
    stream.flush().expect("flush");
}

fn send_request(stream: &mut TcpStream, seq: u64, tag: u8, payload: &[u8]) {
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.push(tag);
    frame.extend_from_slice(payload);
    write_frame(stream, &frame);
}

fn read_until_batch_end(stream: &mut TcpStream) -> Vec<Vec<u8>> {
    let mut responses = Vec::new();
    loop {
        let frame = read_frame(stream);
        if frame[0] == TAG_BATCH_END {
            break;
        }
        responses.push(frame);
    }
    responses
}

/// One response frame, asserting the batch carried exactly one.
fn single_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut responses = read_until_batch_end(stream);
    assert_eq!(responses.len(), 1, "expected exactly one response frame");
    responses.pop().expect("one response")
}

/// The digest a client would submit for `document` — hashed client-side,
/// which is the whole point: the notary never sees the document.
fn digest(document: &[u8]) -> [u8; LEAF_LEN] {
    *blake3::hash(document).as_bytes()
}

/// The client-side model of the server's chain: `BLAKE3(prev || leaf)`.
/// Kept independent of the server implementation on purpose — if the two
/// ever disagree, the test should fail rather than agree by construction.
fn fold(prev: &[u8; HEAD_LEN], leaf: &[u8; LEAF_LEN]) -> [u8; HEAD_LEN] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prev);
    hasher.update(leaf);
    *hasher.finalize().as_bytes()
}

fn head_of(frame: &[u8]) -> [u8; HEAD_LEN] {
    frame[9..9 + HEAD_LEN].try_into().expect("32-byte head")
}

fn count_of(frame: &[u8]) -> u64 {
    u64::from_le_bytes(frame[1..9].try_into().expect("8-byte count"))
}

/// Connect and authenticate, retrying until the server is ready.
/// The kernel backlog accepts the TCP SYN before the server's accept
/// loop starts, so a successful `connect` doesn't mean the server is
/// ready — we must also read the Challenge to confirm.
fn connect_authenticated(addr: SocketAddr, key: &SigningKey) -> TcpStream {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .expect("set timeout");
            let mut stream = stream;
            if stream.read(&mut [0u8; 0]).is_ok() {
                // Try reading the Challenge — timeout means server hasn't accepted yet.
                let mut len_buf = [0u8; 4];
                if stream.read_exact(&mut len_buf).is_ok() {
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut payload = vec![0u8; len];
                    stream.read_exact(&mut payload).expect("read challenge");
                    assert_eq!(payload[0], TAG_CHALLENGE, "expected Challenge");

                    let nonce = &payload[1..33];
                    let signature = key.sign(nonce);
                    let pubkey = key.verifying_key().to_bytes();

                    let mut frame = Vec::with_capacity(105);
                    frame.extend_from_slice(&0u64.to_le_bytes());
                    frame.push(TAG_CHALLENGE_RESPONSE);
                    frame.extend_from_slice(&signature.to_bytes());
                    frame.extend_from_slice(&pubkey);
                    write_frame(&mut stream, &frame);

                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .expect("set timeout");
                    let ready = read_frame(&mut stream);
                    assert_eq!(ready[0], TAG_SERVER_READY, "expected ServerReady");

                    return stream;
                }
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for server"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn start_server() -> (
    Arc<AtomicBool>,
    SocketAddr,
    std::thread::JoinHandle<Result<(), String>>,
) {
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let pubkey_b64 =
        base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());

    let tmp = tempfile::tempdir().expect("tempdir");
    let auth_path = tmp.path().join("authorized_keys");
    // `trader`, not `operator`: this example gates submissions on a
    // writing role, so the round trip must authenticate as one.
    std::fs::write(&auth_path, format!("trader {pubkey_b64} test\n")).expect("write auth keys");

    let journal_path = tmp.path().join("notary.journal");

    let listener =
        BlockingTcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().expect("parse addr"))
            .expect("bind");
    let server_addr = listener.local_addr().expect("local_addr");

    let config = ServerConfig {
        bind: server_addr,
        journal: journal_path,
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: melin_server_runtime::ack_policy::AckPolicy::Disk,
        no_mlock: true,
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        accounts: 0,
        instruments: 0,
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();

    // tempdir must outlive the server thread (journal lives inside it).
    let handle = std::thread::spawn(move || -> Result<(), String> {
        let _tmp = tmp;
        server::run_with_listener(
            listener,
            config,
            NotaryFactory,
            RequestDecoder,
            ResponseEncoder,
            None,
            sd,
        )
        .map_err(|e| e.to_string())
    });

    (shutdown, server_addr, handle)
}

fn stop_server(
    shutdown: Arc<AtomicBool>,
    addr: SocketAddr,
    handle: std::thread::JoinHandle<Result<(), String>>,
) {
    shutdown.store(true, Ordering::Relaxed);
    // Poke the accept loop so it notices the shutdown flag.
    let _ = TcpStream::connect_timeout(&addr, Duration::from_millis(100));
    handle
        .join()
        .expect("server thread panicked")
        .expect("server returned error");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn an_empty_log_reports_genesis() {
    let (shutdown, addr, handle) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let mut stream = connect_authenticated(addr, &key);

    send_request(&mut stream, 1, TAG_GET_HEAD, &[]);
    let response = single_response(&mut stream);
    assert_eq!(response[0], TAG_RESP_HEAD);
    assert_eq!(count_of(&response), 0);
    assert_eq!(head_of(&response), GENESIS_HEAD);

    drop(stream);
    stop_server(shutdown, addr, handle);
}

#[test]
fn notarize_builds_a_chain_the_client_can_reproduce() {
    let (shutdown, addr, handle) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let mut stream = connect_authenticated(addr, &key);

    // Stand-in documents. Only their digests ever leave the client.
    let documents: [&[u8]; 4] = [b"", b"the quick brown fox", b"contract v1", b"contract v2"];
    let mut expected = GENESIS_HEAD;

    for (i, document) in documents.iter().enumerate() {
        let leaf = digest(document);
        expected = fold(&expected, &leaf);

        send_request(&mut stream, i as u64 + 1, TAG_NOTARIZE, &leaf);
        let response = single_response(&mut stream);

        assert_eq!(response[0], TAG_RESP_RECEIPT);
        assert_eq!(count_of(&response), i as u64 + 1, "entry position");
        assert_eq!(
            head_of(&response),
            expected,
            "server head diverged from the client's fold at entry {}",
            i + 1
        );
    }

    // The query must agree with the last receipt.
    send_request(&mut stream, 100, TAG_GET_HEAD, &[]);
    let response = single_response(&mut stream);
    assert_eq!(response[0], TAG_RESP_HEAD);
    assert_eq!(count_of(&response), documents.len() as u64);
    assert_eq!(head_of(&response), expected);

    drop(stream);
    stop_server(shutdown, addr, handle);
}

#[test]
fn a_malformed_leaf_is_refused_without_dropping_the_connection() {
    let (shutdown, addr, handle) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let mut stream = connect_authenticated(addr, &key);

    // Wrong digest width: the runtime drops the frame and logs at debug,
    // leaving the connection usable — a malformed client request is not a
    // server fault and must not cost the session.
    send_request(&mut stream, 1, TAG_NOTARIZE, &[0u8; LEAF_LEN - 1]);

    // The connection still serves the next request, and nothing was
    // committed.
    send_request(&mut stream, 2, TAG_GET_HEAD, &[]);
    let response = single_response(&mut stream);
    assert_eq!(response[0], TAG_RESP_HEAD);
    assert_eq!(count_of(&response), 0, "a refused leaf must not be folded");
    assert_eq!(head_of(&response), GENESIS_HEAD);

    drop(stream);
    stop_server(shutdown, addr, handle);
}

#[test]
fn second_connection_sees_persisted_chain() {
    let (shutdown, addr, handle) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);

    let leaf = digest(b"a document worth attesting");
    let expected = fold(&GENESIS_HEAD, &leaf);

    // First connection: notarize once.
    {
        let mut s = connect_authenticated(addr, &key);
        send_request(&mut s, 1, TAG_NOTARIZE, &leaf);
        let r = single_response(&mut s);
        assert_eq!(r[0], TAG_RESP_RECEIPT);
        assert_eq!(head_of(&r), expected);
    }

    // Second connection: the chain survives the first one closing.
    {
        let mut s = connect_authenticated(addr, &key);
        send_request(&mut s, 1, TAG_GET_HEAD, &[]);
        let r = single_response(&mut s);
        assert_eq!(r[0], TAG_RESP_HEAD);
        assert_eq!(count_of(&r), 1);
        assert_eq!(head_of(&r), expected);
    }

    stop_server(shutdown, addr, handle);
}
