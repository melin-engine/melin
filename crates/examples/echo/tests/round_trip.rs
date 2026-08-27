//! The example's behaviour, end to end, in three sections:
//!
//! 1. Over raw frames: start echo-server, connect a TCP client with
//!    Ed25519 auth, and check that what goes in comes back — at every
//!    size, and not for what the server must refuse.
//! 2. Against the journal on disk: every echo is there, in order, and the
//!    node comes back from a snapshot plus the tail.
//! 3. Through the command-line client, run as a real process.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};

use melin_journal::{JournalEvent, JournalReader};
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::server::{self, ServerConfig};
use melin_wire_protocol::control_codec::{
    TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY,
};
use melin_wire_protocol::tcp::BlockingTcpListener;

use echo_server::{
    EchoFactory, MAX_PAYLOAD, Payload, RequestDecoder, ResponseEncoder, TAG_ECHO, TAG_RESP_ECHO,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn try_read_frame(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    try_read_frame(stream).expect("read frame")
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

/// Echo `payload` and return the reply's `(tag, bytes)`.
fn exchange(stream: &mut TcpStream, seq: u64, payload: &[u8]) -> (u8, Vec<u8>) {
    send_request(stream, seq, TAG_ECHO, payload);
    let reply = single_response(stream);
    (reply[0], reply[1..].to_vec())
}

/// `len` bytes that no other length or seed produces, so a reply can only
/// match the request it answers.
fn bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
}

/// Answer the server's Challenge on a connected stream and consume the
/// ServerReady. `None` if the handshake stalls or the frames are not the
/// expected tags — the node is not serving yet — so callers retry.
fn answer_challenge(stream: &mut TcpStream, key: &SigningKey) -> Option<()> {
    let challenge = try_read_frame(stream).ok()?;
    if challenge.first() != Some(&TAG_CHALLENGE) {
        return None;
    }
    let signature = key.sign(&challenge[1..33]);
    let mut frame = Vec::with_capacity(105);
    frame.extend_from_slice(&0u64.to_le_bytes());
    frame.push(TAG_CHALLENGE_RESPONSE);
    frame.extend_from_slice(&signature.to_bytes());
    frame.extend_from_slice(&key.verifying_key().to_bytes());
    write_frame(stream, &frame);
    let ready = try_read_frame(stream).ok()?;
    (ready.first() == Some(&TAG_SERVER_READY)).then_some(())
}

/// Connect and authenticate, retrying until the server is ready.
/// The kernel backlog accepts the TCP SYN before the server's accept
/// loop starts, so a successful `connect` doesn't mean the server is
/// ready — we must also read the Challenge to confirm.
fn connect_authenticated(addr: SocketAddr, key: &SigningKey) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            stream
                .set_read_timeout(Some(Duration::from_millis(500)))
                .expect("set timeout");
            if answer_challenge(&mut stream, key).is_some() {
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .expect("set timeout");
                return stream;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for a serving node at {addr}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The writing identity. `trader`, not `operator`: echoing is gated on a
/// writing role, so the round trip must authenticate as one.
fn trader_key() -> SigningKey {
    SigningKey::from_bytes(&[0xAA; 32])
}

/// May ping, may not echo.
fn readonly_key() -> SigningKey {
    SigningKey::from_bytes(&[0xBB; 32])
}

fn pubkey_b64(key: &SigningKey) -> String {
    base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes())
}

/// A running server: its shutdown flag, listening address, and thread.
struct Server {
    shutdown: Arc<AtomicBool>,
    addr: SocketAddr,
    handle: std::thread::JoinHandle<Result<(), String>>,
}

impl Server {
    fn stop(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Best-effort poke so the accept loop wakes and sees the flag;
        // whether the connect itself succeeds is irrelevant.
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(100));
        self.handle
            .join()
            .expect("server thread panicked")
            .expect("server returned error");
    }
}

/// Start a server whose journal and `authorized_keys` live in `dir`.
///
/// Starting twice on the same `dir` restarts the node on its existing
/// journal (and snapshot, if one was taken), which is how the recovery
/// test exercises replay. Both test identities are authorized every
/// time, so a test picks its role by picking its key.
fn start_server_in(dir: &Path) -> Server {
    start_server_with(dir, |_| {})
}

/// [`start_server_in`], with `configure` applied to the config first.
fn start_server_with(dir: &Path, configure: impl FnOnce(&mut ServerConfig)) -> Server {
    let auth_path = dir.join("authorized_keys");
    std::fs::write(
        &auth_path,
        format!(
            "trader {} test\nreadonly {} watch\n",
            pubkey_b64(&trader_key()),
            pubkey_b64(&readonly_key()),
        ),
    )
    .expect("write auth keys");

    let listener =
        BlockingTcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().expect("parse addr"))
            .expect("bind");
    let mut config = ServerConfig {
        bind: listener.local_addr().expect("local_addr"),
        journal: dir.join("echo.journal"),
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: AckPolicy::Disk,
        no_mlock: true,
        // Test servers share the machine with the rest of the suite; a
        // busy-spinning pipeline per node starves the clients (and the
        // other nodes) of CPU time under full-suite load.
        yield_idle: true,
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        accounts: 0,
        instruments: 0,
        ..ServerConfig::default()
    };
    configure(&mut config);

    let addr = config.bind;
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let handle = std::thread::spawn(move || -> Result<(), String> {
        server::run_with_listener(
            listener,
            config,
            EchoFactory,
            RequestDecoder,
            ResponseEncoder,
            None,
            sd,
        )
        .map_err(|e| e.to_string())
    });
    Server {
        shutdown,
        addr,
        handle,
    }
}

/// Start a server in a fresh temporary directory. The directory is
/// returned so the caller keeps it alive for as long as the server runs —
/// the journal lives inside it.
fn start_server() -> (tempfile::TempDir, Server) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let server = start_server_in(tmp.path());
    (tmp, server)
}

/// The payloads of every journaled echo, in journal order.
fn journaled_echoes(dir: &Path) -> Vec<Vec<u8>> {
    let mut reader =
        JournalReader::<Payload>::open(&dir.join("echo.journal")).expect("open journal");
    let mut echoes = Vec::new();
    while let Some(entry) = reader.next_entry().expect("read entry") {
        if let JournalEvent::App(payload) = entry.event {
            echoes.push(payload.as_bytes().to_vec());
        }
    }
    echoes
}

// ---------------------------------------------------------------------------
// Over raw frames
// ---------------------------------------------------------------------------

#[test]
fn an_echo_returns_the_bytes_it_was_sent() {
    let (_tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr, &trader_key());

    // Sizes either side of the `u8` boundary included: the length is
    // carried in two bytes, and 256 is where a one-byte length would wrap.
    for (i, len) in [0, 1, 7, 255, 256, MAX_PAYLOAD].into_iter().enumerate() {
        let sent = bytes(len, i as u8);
        let (tag, back) = exchange(&mut stream, i as u64 + 1, &sent);
        assert_eq!(tag, TAG_RESP_ECHO, "{len} bytes");
        assert_eq!(back, sent, "{len} bytes");
    }

    drop(stream);
    server.stop();
}

#[test]
fn an_oversized_payload_is_refused_without_dropping_the_connection() {
    let (_tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr, &trader_key());

    // One byte past the cap. The runtime drops the frame at the decoder
    // without a response and keeps the connection, so the refusal is
    // observable only as the next reply answering the next request rather
    // than this one.
    send_request(&mut stream, 1, TAG_ECHO, &bytes(MAX_PAYLOAD + 1, 1));

    let sent = bytes(MAX_PAYLOAD, 2);
    let (tag, back) = exchange(&mut stream, 2, &sent);
    assert_eq!((tag, back), (TAG_RESP_ECHO, sent));

    drop(stream);
    server.stop();
}

#[test]
fn a_read_only_key_cannot_echo() {
    let (tmp, server) = start_server();

    // Refused at the decoder: no reply, connection kept — there is no
    // request a read-only key can make that would be answered, so the
    // refusal is observable only in the journal afterwards.
    let mut watcher = connect_authenticated(server.addr, &readonly_key());
    send_request(&mut watcher, 1, TAG_ECHO, b"not mine to journal");

    let sent = bytes(MAX_PAYLOAD, 9);
    let mut stream = connect_authenticated(server.addr, &trader_key());
    let (tag, back) = exchange(&mut stream, 1, &sent);
    assert_eq!((tag, back), (TAG_RESP_ECHO, sent.clone()));

    drop(stream);
    drop(watcher);
    server.stop();
    assert_eq!(
        journaled_echoes(tmp.path()),
        [sent],
        "a read-only key must not be able to write the journal"
    );
}

// ---------------------------------------------------------------------------
// Against the journal
// ---------------------------------------------------------------------------

#[test]
fn the_journal_holds_every_echo_in_order() {
    let (tmp, server) = start_server();
    let echoes = [bytes(0, 0), bytes(MAX_PAYLOAD, 1), bytes(5, 2)];
    {
        let mut stream = connect_authenticated(server.addr, &trader_key());
        for (i, sent) in echoes.iter().enumerate() {
            exchange(&mut stream, i as u64 + 1, sent);
        }
    }
    server.stop();

    // Under the `disk` ack policy a reply means the entry is fsynced, so
    // every echo replied to above is on disk — the audit trail of what
    // was sequenced, and in what order.
    assert_eq!(journaled_echoes(tmp.path()), echoes);
}

#[test]
fn the_node_recovers_from_a_snapshot_and_the_journal_tail() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let snapshot = tmp.path().join("echo.snapshot");
    let before = [bytes(16, 1), bytes(16, 2)];
    let after = bytes(16, 3);

    // First life, with the shadow stage taking snapshots often. A
    // snapshot of this application is zero bytes of payload inside the
    // runtime's framing — the case worth proving restores.
    let server = start_server_with(tmp.path(), |config| config.snapshot_interval_ms = 50);
    {
        let mut stream = connect_authenticated(server.addr, &trader_key());
        for (i, sent) in before.iter().enumerate() {
            exchange(&mut stream, i as u64 + 1, sent);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while !snapshot.exists() {
            assert!(Instant::now() < deadline, "no snapshot was taken");
            std::thread::sleep(Duration::from_millis(20));
        }
        // Journaled after the snapshot's anchor, or at worst inside a
        // later one: either way, the tail replay has work to do.
        exchange(&mut stream, 3, &after);
    }
    server.stop();

    // Second life: the runtime finds the snapshot next to the journal,
    // restores the (empty) state from it and replays the tail. If either
    // step failed the node would not come up; the echo after it shows
    // the pipeline is whole, and the journal that it lost nothing.
    let server = start_server_in(tmp.path());
    let mut stream = connect_authenticated(server.addr, &trader_key());
    let sent = bytes(MAX_PAYLOAD, 4);
    let (tag, back) = exchange(&mut stream, 1, &sent);
    assert_eq!((tag, back), (TAG_RESP_ECHO, sent.clone()));
    drop(stream);
    server.stop();

    let mut expected = before.to_vec();
    expected.push(after);
    expected.push(sent);
    assert_eq!(journaled_echoes(tmp.path()), expected);
}

// ---------------------------------------------------------------------------
// The command-line client
// ---------------------------------------------------------------------------

/// Run the client with `args`, returning `(exit code, stdout, stderr)`.
fn echo_client(args: &[&str]) -> (i32, String, String) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_echo-client"))
        .args(args)
        .output()
        .expect("spawn echo-client");
    (
        output.status.code().expect("exited normally"),
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        String::from_utf8(output.stderr).expect("utf-8 stderr"),
    )
}

#[test]
fn the_client_measures_a_closed_loop() {
    let (tmp, server) = start_server();
    // The binary connects once, without retrying, so wait for the server
    // to be ready the way the in-process tests do before spawning it.
    drop(connect_authenticated(server.addr, &trader_key()));

    let key = tmp.path().join("trader.key");
    std::fs::write(&key, trader_key().to_bytes()).expect("write key");
    let addr = server.addr.to_string();
    let key_arg = key.to_str().expect("utf-8 path");
    let common = ["--server", &addr, "--key", key_arg];

    // A single request reports its one round trip.
    let (code, stdout, stderr) = echo_client(&[&common[..], &["--size", "8"]].concat());
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.starts_with("8 bytes back in "), "{stdout}");

    // A run reports the distribution. The default size is the cap.
    let (code, stdout, stderr) = echo_client(&[&common[..], &["--count", "200"]].concat());
    assert_eq!(code, 0, "{stderr}");
    assert!(
        stdout.starts_with(&format!("200 × {MAX_PAYLOAD}-byte echoes to ")),
        "{stdout}"
    );
    for label in ["min", "p50", "p90", "p99", "p99.9", "max"] {
        assert!(stdout.contains(&format!("  {label}")), "{label}: {stdout}");
    }

    // Everything the runs sent is on disk.
    server.stop();
    assert_eq!(journaled_echoes(tmp.path()).len(), 1 + 200);
}

#[test]
fn the_client_refuses_a_size_the_server_would_drop() {
    // Checked before connecting: no server is needed, and none is running
    // at this address.
    let (code, _, stderr) = echo_client(&[
        "--server",
        "127.0.0.1:1",
        "--key",
        "/nonexistent",
        "--size",
        &(MAX_PAYLOAD + 1).to_string(),
    ]);
    assert_eq!(code, 2, "{stderr}");
    assert!(
        stderr.contains(&format!("at most {MAX_PAYLOAD} bytes")),
        "{stderr}"
    );
}

#[test]
fn the_client_reports_an_unauthorized_key() {
    let (tmp, server) = start_server();
    drop(connect_authenticated(server.addr, &trader_key()));

    // A key the server has never heard of: the handshake fails, and the
    // client says which public key to authorize.
    let key = tmp.path().join("stranger.key");
    std::fs::write(&key, [0xCC; 32]).expect("write key");
    let (code, _, stderr) = echo_client(&[
        "--server",
        &server.addr.to_string(),
        "--key",
        key.to_str().expect("utf-8 path"),
    ]);
    assert_eq!(code, 2, "{stderr}");
    assert!(
        stderr.contains(&pubkey_b64(&SigningKey::from_bytes(&[0xCC; 32]))),
        "{stderr}"
    );

    server.stop();
}
