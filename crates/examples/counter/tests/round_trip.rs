//! Full round-trip integration test: start counter-server, connect a
//! TCP client with Ed25519 auth, send Increment + GetValue, verify
//! responses, shut down cleanly.

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

use counter_server::{CounterFactory, RequestDecoder, ResponseEncoder};

const TAG_INCREMENT: u8 = 0x10;
const TAG_GET_VALUE: u8 = 0x11;
const TAG_RESP_ACK: u8 = 0x30;
const TAG_RESP_VALUE: u8 = 0x31;

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
    std::fs::write(&auth_path, format!("operator {pubkey_b64} test\n")).expect("write auth keys");

    let journal_path = tmp.path().join("counter.journal");

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
            CounterFactory,
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

#[test]
fn full_round_trip() {
    let (shutdown, addr, handle) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);
    let mut stream = connect_authenticated(addr, &key);

    // --- Increment by 10 ---
    send_request(&mut stream, 1, TAG_INCREMENT, &10u64.to_le_bytes());
    let responses = read_until_batch_end(&mut stream);
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0][0], TAG_RESP_ACK);
    let value = u64::from_le_bytes(responses[0][1..9].try_into().unwrap());
    assert_eq!(value, 10);

    // --- Increment by 32 ---
    send_request(&mut stream, 2, TAG_INCREMENT, &32u64.to_le_bytes());
    let responses = read_until_batch_end(&mut stream);
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0][0], TAG_RESP_ACK);
    let value = u64::from_le_bytes(responses[0][1..9].try_into().unwrap());
    assert_eq!(value, 42);

    // --- GetValue query ---
    send_request(&mut stream, 3, TAG_GET_VALUE, &[]);
    let responses = read_until_batch_end(&mut stream);
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0][0], TAG_RESP_VALUE);
    let value = u64::from_le_bytes(responses[0][1..9].try_into().unwrap());
    assert_eq!(value, 42);

    drop(stream);
    stop_server(shutdown, addr, handle);
}

#[test]
fn second_connection_sees_persisted_state() {
    let (shutdown, addr, handle) = start_server();
    let key = SigningKey::from_bytes(&[0xAA; 32]);

    // First connection: increment to 100.
    {
        let mut s = connect_authenticated(addr, &key);
        send_request(&mut s, 1, TAG_INCREMENT, &100u64.to_le_bytes());
        let r = read_until_batch_end(&mut s);
        assert_eq!(u64::from_le_bytes(r[0][1..9].try_into().unwrap()), 100);
    }

    // Second connection: query — should see 100 (state survives connections).
    {
        let mut s = connect_authenticated(addr, &key);
        send_request(&mut s, 1, TAG_GET_VALUE, &[]);
        let r = read_until_batch_end(&mut s);
        assert_eq!(r[0][0], TAG_RESP_VALUE);
        assert_eq!(u64::from_le_bytes(r[0][1..9].try_into().unwrap()), 100);
    }

    stop_server(shutdown, addr, handle);
}
