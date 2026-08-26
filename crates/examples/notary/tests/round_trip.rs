//! The example's guarantees, end to end, in four sections:
//!
//! 1. Over raw frames: start notary-server, connect a TCP client with
//!    Ed25519 auth, notarize a series of digests, and check the chain head
//!    the server reports against one folded independently on the client —
//!    the same check an auditor holding the original documents would
//!    perform. Then recovery: the head survives a restart because it is
//!    rebuilt from the journal.
//! 2. Through the command-line client, run as a real process.
//! 3. Through the auditor, against a rotated journal on disk.
//! 4. Across nodes: a replica promoted after the primary's death must
//!    report the head the primary receipted, and chain onto it.

use std::io::{BufRead, BufReader, Read, Write};
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
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::control_codec::{
    TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY,
};
use melin_wire_protocol::tcp::BlockingTcpListener;

use notary_server::receipt::Receipt as SavedReceipt;
use notary_server::{
    GENESIS_HEAD, HEAD_LEN, LEAF_LEN, NotaryEvent, NotaryFactory, RequestDecoder, ResponseEncoder,
    TAG_GET_HEAD, TAG_NOTARIZE, TAG_RESP_HEAD, TAG_RESP_RECEIPT,
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

/// The digest a client would submit for `document` — hashed client-side,
/// which is the whole point: the notary never sees the document.
fn digest(document: &[u8]) -> [u8; LEAF_LEN] {
    *blake3::hash(document).as_bytes()
}

/// The client-side model of the server's chain:
/// `BLAKE3(prev || leaf || timestamp_ns)`. Kept independent of the server
/// implementation on purpose — if the two ever disagree, the test should
/// fail rather than agree by construction.
fn fold(prev: &[u8; HEAD_LEN], leaf: &[u8; LEAF_LEN], timestamp_ns: u64) -> [u8; HEAD_LEN] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prev);
    hasher.update(leaf);
    hasher.update(&timestamp_ns.to_le_bytes());
    *hasher.finalize().as_bytes()
}

fn unix_now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after the epoch")
        .as_nanos() as u64
}

/// A decoded receipt frame:
/// `[tag][entry: u64][timestamp_ns: u64][prev: 32][head: 32]`.
struct Receipt {
    entry: u64,
    timestamp_ns: u64,
    prev: [u8; HEAD_LEN],
    head: [u8; HEAD_LEN],
}

fn receipt_of(frame: &[u8]) -> Receipt {
    assert_eq!(frame[0], TAG_RESP_RECEIPT, "expected a receipt frame");
    Receipt {
        entry: u64::from_le_bytes(frame[1..9].try_into().expect("8-byte entry")),
        timestamp_ns: u64::from_le_bytes(frame[9..17].try_into().expect("8-byte time")),
        prev: frame[17..49].try_into().expect("32-byte prev"),
        head: frame[49..81].try_into().expect("32-byte head"),
    }
}

/// `(entries, head)` from a head frame: `[tag][entries: u64][head: 32]`.
fn head_of(frame: &[u8]) -> (u64, [u8; HEAD_LEN]) {
    assert_eq!(frame[0], TAG_RESP_HEAD, "expected a head frame");
    (
        u64::from_le_bytes(frame[1..9].try_into().expect("8-byte count")),
        frame[9..41].try_into().expect("32-byte head"),
    )
}

/// Answer the server's Challenge on a connected stream and consume the
/// ServerReady. `None` if the handshake stalls or the frames are not the
/// expected tags — the node is not serving yet — so callers retry. The
/// client and admin listeners share this handshake.
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

/// The writing identity. `trader`, not `operator`: this example gates
/// submissions on a writing role, so the round trip must authenticate as
/// one.
fn trader_key() -> SigningKey {
    SigningKey::from_bytes(&[0xAA; 32])
}

/// The auditing identity: may read the head, may not extend the chain.
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
/// journal, which is how the recovery test exercises replay. Every test
/// identity is authorized every time, so a test picks its role by
/// picking its key.
fn start_server_in(dir: &Path) -> Server {
    start_server_with(dir, |_| {})
}

/// [`start_server_in`], with `configure` applied to the config first.
fn start_server_with(dir: &Path, configure: impl FnOnce(&mut ServerConfig)) -> Server {
    let auth_path = dir.join("authorized_keys");
    std::fs::write(
        &auth_path,
        format!(
            "trader {} test\nreadonly {} audit\noperator {} ops\n",
            pubkey_b64(&trader_key()),
            pubkey_b64(&readonly_key()),
            pubkey_b64(&operator_key())
        ),
    )
    .expect("write auth keys");

    let listener = bind_client_listener();
    let mut config = ServerConfig {
        bind: listener.local_addr().expect("local_addr"),
        journal: dir.join("notary.journal"),
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
    spawn_node(listener, config)
}

/// The client listener is bound here and handed to the runtime, so it
/// can take a kernel-assigned port; the listeners the runtime binds
/// itself (replication, admin) cannot — see `free_addr`.
fn bind_client_listener() -> BlockingTcpListener {
    BlockingTcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().expect("parse addr"))
        .expect("bind")
}

/// Run a node on `listener` with `config`, on its own thread.
fn spawn_node(listener: BlockingTcpListener, config: ServerConfig) -> Server {
    let addr = config.bind;
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let handle = std::thread::spawn(move || -> Result<(), String> {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn an_empty_log_reports_genesis() {
    let (_tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr, &trader_key());

    send_request(&mut stream, 1, TAG_GET_HEAD, &[]);
    assert_eq!(head_of(&single_response(&mut stream)), (0, GENESIS_HEAD));

    drop(stream);
    server.stop();
}

#[test]
fn notarize_builds_a_chain_the_client_can_reproduce() {
    let (_tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr, &trader_key());

    // Stand-in documents. Only their digests ever leave the client.
    let documents: [&[u8]; 4] = [b"", b"the quick brown fox", b"contract v1", b"contract v2"];
    let mut expected = GENESIS_HEAD;
    let started = unix_now_ns();

    for (i, document) in documents.iter().enumerate() {
        let leaf = digest(document);

        send_request(&mut stream, i as u64 + 1, TAG_NOTARIZE, &leaf);
        let receipt = receipt_of(&single_response(&mut stream));

        assert_eq!(receipt.entry, i as u64 + 1, "entry position");
        assert!(
            receipt.timestamp_ns >= started,
            "the receipt's time must be the sequencer's clock, not a placeholder"
        );
        assert_eq!(receipt.prev, expected, "receipts must chain");
        expected = fold(&expected, &leaf, receipt.timestamp_ns);
        assert_eq!(
            receipt.head,
            expected,
            "server head diverged from the client's fold at entry {}",
            i + 1
        );
    }

    // The query must agree with the last receipt.
    send_request(&mut stream, 100, TAG_GET_HEAD, &[]);
    assert_eq!(
        head_of(&single_response(&mut stream)),
        (documents.len() as u64, expected)
    );

    drop(stream);
    server.stop();
}

#[test]
fn an_independent_client_verifies_its_own_receipt() {
    let (_tmp, server) = start_server();

    // Someone else's history, unknown to the verifier below.
    {
        let mut other = connect_authenticated(server.addr, &trader_key());
        for i in 1..=3u64 {
            send_request(&mut other, i, TAG_NOTARIZE, &digest(&i.to_le_bytes()));
            receipt_of(&single_response(&mut other));
        }
    }

    // The verifier holds only its document and its receipt — no earlier
    // leaves, no query — and that is enough to check the commitment.
    let mut stream = connect_authenticated(server.addr, &trader_key());
    let leaf = digest(b"my document");
    send_request(&mut stream, 1, TAG_NOTARIZE, &leaf);
    let receipt = receipt_of(&single_response(&mut stream));
    assert_eq!(receipt.entry, 4);
    assert_eq!(
        fold(&receipt.prev, &leaf, receipt.timestamp_ns),
        receipt.head
    );
    assert_ne!(
        fold(
            &receipt.prev,
            &digest(b"my document, altered"),
            receipt.timestamp_ns
        ),
        receipt.head
    );
    assert_ne!(
        fold(&receipt.prev, &leaf, receipt.timestamp_ns + 1),
        receipt.head,
        "the time is attested, not merely reported"
    );

    drop(stream);
    server.stop();
}

#[test]
fn a_malformed_leaf_is_refused_without_dropping_the_connection() {
    let (_tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr, &trader_key());

    // Wrong digest width: the runtime drops the frame and logs at debug,
    // leaving the connection usable — a malformed client request is not a
    // server fault and must not cost the session.
    send_request(&mut stream, 1, TAG_NOTARIZE, &[0u8; LEAF_LEN - 1]);

    // The connection still serves the next request, and nothing was
    // committed.
    send_request(&mut stream, 2, TAG_GET_HEAD, &[]);
    assert_eq!(
        head_of(&single_response(&mut stream)),
        (0, GENESIS_HEAD),
        "a refused leaf must not be folded"
    );

    drop(stream);
    server.stop();
}

#[test]
fn a_read_only_key_can_audit_but_not_notarize() {
    let (_tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr, &readonly_key());

    // The submission is refused at the decoder: the runtime drops it
    // without a response and keeps the connection, so the refusal is
    // observable only as the chain not having moved. Had the gate let it
    // through, the next frame read would be a receipt, not the head.
    send_request(&mut stream, 1, TAG_NOTARIZE, &digest(b"not mine to attest"));

    send_request(&mut stream, 2, TAG_GET_HEAD, &[]);
    let response = single_response(&mut stream);
    assert_eq!(
        response[0], TAG_RESP_HEAD,
        "a read-only key must still be able to audit"
    );
    assert_eq!(
        head_of(&response),
        (0, GENESIS_HEAD),
        "a read-only key must not be able to extend the chain"
    );

    drop(stream);
    server.stop();
}

#[test]
fn second_connection_sees_persisted_chain() {
    let (_tmp, server) = start_server();

    let leaf = digest(b"a document worth attesting");

    // First connection: notarize once.
    let expected = {
        let mut s = connect_authenticated(server.addr, &trader_key());
        send_request(&mut s, 1, TAG_NOTARIZE, &leaf);
        let receipt = receipt_of(&single_response(&mut s));
        assert_eq!(
            fold(&GENESIS_HEAD, &leaf, receipt.timestamp_ns),
            receipt.head
        );
        receipt.head
    };

    // Second connection: the chain survives the first one closing.
    {
        let mut s = connect_authenticated(server.addr, &trader_key());
        send_request(&mut s, 1, TAG_GET_HEAD, &[]);
        assert_eq!(head_of(&single_response(&mut s)), (1, expected));
    }

    server.stop();
}

#[test]
fn the_chain_survives_a_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let documents: [&[u8]; 3] = [b"minutes 2026-08-26", b"invoice 1041", b"invoice 1042"];
    let mut expected = GENESIS_HEAD;

    // First life: notarize three documents. Under the `disk` ack policy a
    // receipt means the leaf is fsynced, so everything receipted here is
    // on disk before the node goes down.
    let server = start_server_in(tmp.path());
    {
        let mut stream = connect_authenticated(server.addr, &trader_key());
        for (i, document) in documents.iter().enumerate() {
            let leaf = digest(document);
            send_request(&mut stream, i as u64 + 1, TAG_NOTARIZE, &leaf);
            let receipt = receipt_of(&single_response(&mut stream));
            expected = fold(&expected, &leaf, receipt.timestamp_ns);
            assert_eq!(receipt.head, expected);
        }
    }
    server.stop();

    // Second life, same journal: the head is not stored anywhere the
    // restarted node can read it from — it is rebuilt by replaying the
    // journaled leaves in order. Matching the client's fold is the
    // determinism the example exists to demonstrate, applied to recovery.
    let server = start_server_in(tmp.path());
    let mut stream = connect_authenticated(server.addr, &trader_key());
    send_request(&mut stream, 1, TAG_GET_HEAD, &[]);
    assert_eq!(
        head_of(&single_response(&mut stream)),
        (documents.len() as u64, expected),
        "recovered head diverged from the client's fold"
    );

    drop(stream);
    server.stop();
}

#[test]
fn the_journal_carries_the_runtime_hash_chain() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let server = start_server_in(tmp.path());
    {
        let mut stream = connect_authenticated(server.addr, &trader_key());
        for i in 1..=3u64 {
            send_request(&mut stream, i, TAG_NOTARIZE, &digest(&i.to_le_bytes()));
            assert_eq!(single_response(&mut stream)[0], TAG_RESP_RECEIPT);
        }
    }
    server.stop();

    // `hash-chain` is a hard dependency of this crate, not a feature: the
    // reader's chain accessors are `None` only when the runtime was built
    // without it, which is the regression this guards against. The value
    // is then checked against the documented formula, recomputed from the
    // raw file bytes: `BLAKE3(entry bytes || anchor)`.
    let path = tmp.path().join("notary.journal");
    let mut reader = JournalReader::<NotaryEvent>::open(&path).expect("open journal");
    let anchor = reader
        .anchor()
        .expect("journal must be built with hash-chain");
    let mut leaves = 0;
    while let Some(entry) = reader.next_entry().expect("read entry") {
        if matches!(entry.event, JournalEvent::App(NotaryEvent::Notarize { .. })) {
            leaves += 1;
        }
    }
    assert_eq!(leaves, 3);

    let bytes = std::fs::read(&path).expect("read journal");
    let mut hasher = blake3::Hasher::new();
    hasher.update(&bytes[reader.sector_size()..reader.valid_file_end() as usize]);
    hasher.update(&anchor);
    assert_eq!(reader.chain_hash(), Some(*hasher.finalize().as_bytes()));
}

// ---------------------------------------------------------------------------
// The command-line client
// ---------------------------------------------------------------------------

/// Run one of this crate's binaries with `args`, returning
/// `(exit code, stdout, stderr)`.
fn run_bin(exe: &str, args: &[&str]) -> (i32, String, String) {
    let output = std::process::Command::new(exe)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {exe}: {e}"));
    (
        output.status.code().expect("exited normally"),
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        String::from_utf8(output.stderr).expect("utf-8 stderr"),
    )
}

fn notary_client(args: &[&str]) -> (i32, String, String) {
    run_bin(env!("CARGO_BIN_EXE_notary-client"), args)
}

fn notary_audit(args: &[&str]) -> (i32, String, String) {
    run_bin(env!("CARGO_BIN_EXE_notary-audit"), args)
}

#[test]
fn the_client_notarizes_a_file_and_verifies_it_offline() {
    let (tmp, server) = start_server();
    // The binary connects once, without retrying, so wait for the server
    // to be ready the way the in-process tests do before spawning it.
    drop(connect_authenticated(server.addr, &trader_key()));

    let key = tmp.path().join("trader.key");
    std::fs::write(&key, trader_key().to_bytes()).expect("write key");
    let document = tmp.path().join("contract.txt");
    std::fs::write(&document, b"I, the undersigned, ...").expect("write document");
    let receipt = tmp.path().join("contract.txt.receipt");
    let addr = server.addr.to_string();
    let document_arg = document.to_str().expect("utf-8 path");
    let key_arg = key.to_str().expect("utf-8 path");

    let (code, stdout, stderr) = notary_client(&[
        "notarize",
        document_arg,
        "--server",
        &addr,
        "--key",
        key_arg,
    ]);
    assert_eq!(code, 0, "notarize failed: {stderr}");
    assert!(stdout.contains("entry: 1"), "{stdout}");
    assert!(receipt.exists(), "the receipt lands next to the document");

    // The server's view agrees with the receipt the client kept.
    let (code, stdout, stderr) = notary_client(&["head", "--server", &addr, "--key", key_arg]);
    assert_eq!(code, 0, "head failed: {stderr}");
    let receipt_text = std::fs::read_to_string(&receipt).expect("read receipt");
    let head_line = receipt_text
        .lines()
        .find(|l| l.starts_with("head: "))
        .expect("receipt has a head");
    let head_hex = &head_line["head: ".len()..];
    assert!(stdout.contains("entries: 1"), "{stdout}");
    assert!(
        stdout.contains(head_hex),
        "server head must match the receipt: {stdout}"
    );

    // Verification is offline: the server is gone, and no key is given.
    server.stop();
    let (code, stdout, _) = notary_client(&["verify", document_arg]);
    assert_eq!(code, 0, "{stdout}");
    assert!(stdout.starts_with("OK: "), "{stdout}");

    // A changed document no longer matches its receipt.
    std::fs::write(&document, b"I, the undersigned, ... (amended)").expect("amend document");
    let (code, stdout, _) = notary_client(&["verify", document_arg]);
    assert_eq!(code, 1, "{stdout}");
    assert!(stdout.starts_with("MISMATCH: "), "{stdout}");
    assert!(stdout.contains("changed"), "{stdout}");

    // A receipt whose attested time was edited no longer folds, even
    // though the document is the original again.
    std::fs::write(&document, b"I, the undersigned, ...").expect("restore document");
    let forged = receipt_text
        .lines()
        .map(|l| match l.strip_prefix("time_ns: ") {
            Some(ns) => format!("time_ns: {}", ns.parse::<u64>().unwrap() + 1),
            None => l.to_string(),
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&receipt, forged).expect("forge receipt");
    let (code, stdout, _) = notary_client(&["verify", document_arg]);
    assert_eq!(code, 1, "{stdout}");
    assert!(stdout.contains("does not fold"), "{stdout}");
}

#[test]
fn the_client_reports_an_unauthorized_key() {
    let (tmp, server) = start_server();
    drop(connect_authenticated(server.addr, &trader_key()));

    // A key the server has never heard of: the handshake fails, and the
    // client says which public key to authorize.
    let key = tmp.path().join("stranger.key");
    std::fs::write(&key, [0xCC; 32]).expect("write key");
    let (code, _, stderr) = notary_client(&[
        "head",
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

/// A key the server knows but whose role may not write: the runtime
/// drops the submission without a reply and keeps the connection, so the
/// client has only silence to go on. It must turn that silence into an
/// error that says where to look, not hang. Waits out the client's
/// reply timeout — the one slow test in this file, by design.
#[test]
fn the_client_explains_a_silently_dropped_request() {
    let (tmp, server) = start_server();
    drop(connect_authenticated(server.addr, &trader_key()));

    let key = tmp.path().join("readonly.key");
    std::fs::write(&key, readonly_key().to_bytes()).expect("write key");
    let document = tmp.path().join("audit-only.txt");
    std::fs::write(&document, "may read, may not attest").expect("write document");

    let started = Instant::now();
    let (code, _, stderr) = notary_client(&[
        "notarize",
        document.to_str().expect("utf-8 path"),
        "--server",
        &server.addr.to_string(),
        "--key",
        key.to_str().expect("utf-8 path"),
    ]);
    assert_eq!(code, 2, "{stderr}");
    assert!(
        stderr.contains("no reply within") && stderr.contains("authorized_keys"),
        "{stderr}"
    );
    assert!(
        started.elapsed() >= Duration::from_secs(5),
        "the client gave up before its reply timeout"
    );
    assert!(
        !tmp.path().join("audit-only.txt.receipt").exists(),
        "no receipt may be written for a refused submission"
    );

    server.stop();
}

// ---------------------------------------------------------------------------
// The auditor
// ---------------------------------------------------------------------------

/// The auditor's claim: the journal alone refolds to the head the server
/// reported, and a receipt is found at the position it names. Exercised
/// across a rotation so the walk covers an archived segment as well as
/// the live one, then each way the evidence can fail: an edited receipt,
/// a receipt past the end of the log, a wrong expected head, and a
/// journal edited after the fact.
#[test]
fn the_auditor_refolds_the_head_from_the_journal_alone() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let admin_addr = free_addr(PORT_BASE);
    let server = start_server_with(tmp.path(), |config| config.admin_bind = Some(admin_addr));
    drop(connect_authenticated(server.addr, &trader_key()));

    let key = tmp.path().join("trader.key");
    std::fs::write(&key, trader_key().to_bytes()).expect("write key");
    let path_of = |name: &str| tmp.path().join(name);
    let arg = |path: &Path| path.to_str().expect("utf-8 path").to_owned();
    let server_arg = server.addr.to_string();

    // Three documents; the journal is rotated after the first so the
    // second and third land in a new segment.
    for (i, (name, body)) in [
        ("deed", "the deed"),
        ("codicil", "the codicil"),
        ("witness", "the witness statement"),
    ]
    .into_iter()
    .enumerate()
    {
        std::fs::write(path_of(name), body).expect("write document");
        let (code, out, err) = notary_client(&[
            "notarize",
            &arg(&path_of(name)),
            "--server",
            &server_arg,
            "--key",
            &arg(&key),
        ]);
        assert_eq!(code, 0, "{out}{err}");
        if i == 0 {
            admin_until_ok(admin_addr, &operator_key(), "ROTATE");
        }
    }
    let (code, out, err) = notary_client(&["head", "--server", &server_arg, "--key", &arg(&key)]);
    assert_eq!(code, 0, "{out}{err}");
    let head = out
        .lines()
        .find_map(|line| line.strip_prefix("head:"))
        .expect("the client prints the head")
        .trim()
        .to_owned();
    server.stop();

    let journal = arg(&path_of("notary.journal"));
    let deed_receipt = arg(&path_of("deed.receipt"));
    let witness_receipt = arg(&path_of("witness.receipt"));

    // Everything checks out: two segments, three leaves, the server's
    // head, and both receipts at their positions.
    let (code, out, err) = notary_audit(&[
        &journal,
        "--receipt",
        &deed_receipt,
        "--receipt",
        &witness_receipt,
        "--expect-head",
        &head,
    ]);
    assert_eq!(code, 0, "{out}{err}");
    for line in [
        "segments: 2",
        "notarized: 3",
        &format!("head: {head}"),
        "entry 1 OK",
        "entry 3 OK",
        "expected head: matches",
    ] {
        assert!(out.contains(line), "missing `{line}` in:\n{out}");
    }

    // A forged receipt: the time moved and the head recomputed, so it
    // still folds — `notary-client verify` would accept it. Only the
    // journal can say the sequencer never attested that time.
    let witness =
        SavedReceipt::from_text(&std::fs::read_to_string(&witness_receipt).expect("read receipt"))
            .expect("parse receipt");
    let edited = SavedReceipt {
        timestamp_ns: witness.timestamp_ns + 1,
        head: fold(&witness.prev, &witness.leaf, witness.timestamp_ns + 1),
        ..witness
    };
    assert!(edited.verifies(), "the forgery must pass the offline check");
    let edited_path = path_of("edited.receipt");
    std::fs::write(&edited_path, edited.to_text()).expect("write receipt");
    let (code, out, _) = notary_audit(&[&journal, "--receipt", &arg(&edited_path)]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("entry 3 differs from the journal in time_ns, head"),
        "{out}"
    );

    // A receipt for an entry the log does not have.
    let beyond = SavedReceipt {
        entry: 4,
        ..witness
    };
    let beyond_path = path_of("beyond.receipt");
    std::fs::write(&beyond_path, beyond.to_text()).expect("write receipt");
    let (code, out, _) = notary_audit(&[&journal, "--receipt", &arg(&beyond_path)]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("claims entry 4 but the log has 3"), "{out}");

    // A head the log does not refold to.
    let (code, out, _) = notary_audit(&[&journal, "--expect-head", &"0".repeat(64)]);
    assert_eq!(code, 1, "{out}");
    assert!(out.contains("FAIL: the log refolds to"), "{out}");

    // A journal edited after the fact: one bit flipped inside the first
    // entry of the archived segment. The runtime's own chain catches it
    // before the notary's is even refolded.
    let archive = path_of("notary.journal.000001");
    let sector_size = JournalReader::<NotaryEvent>::open(&archive)
        .expect("open archive")
        .sector_size();
    let mut bytes = std::fs::read(&archive).expect("read archive");
    bytes[sector_size + 16] ^= 0x01;
    std::fs::write(&archive, bytes).expect("write archive");
    let (code, out, _) = notary_audit(&[&journal]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("FAIL: the journal's own hash chain is broken"),
        "{out}"
    );

    // History that no longer starts at genesis: the archive removed, the
    // live segment alone is internally consistent, but a head refolded
    // from it would be a head of the wrong log. The auditor must refuse
    // rather than report one. (Which sequence the live segment starts at
    // depends on the fsync the rotation landed on; what matters is that
    // it is not 1.)
    std::fs::remove_file(&archive).expect("remove archive");
    let (code, out, _) = notary_audit(&[&journal]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("FAIL: the on-disk history starts at sequence")
            && out.contains("not 1: the head cannot be refolded from genesis"),
        "{out}"
    );
}

/// The audit needs no receipt and no head to be useful: a bare run
/// reports what the log holds. And a missing journal is an error, not a
/// finding — the audit did not run.
#[test]
fn the_auditor_reports_an_empty_log_and_a_missing_one() {
    let (tmp, server) = start_server();
    drop(connect_authenticated(server.addr, &trader_key()));
    server.stop();

    let journal = tmp.path().join("notary.journal");
    let (code, out, err) = notary_audit(&[journal.to_str().expect("utf-8 path")]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.contains("notarized: 0"), "{out}");
    assert!(
        out.contains(&format!("head: {}", "0".repeat(64))),
        "an empty log refolds to genesis:\n{out}"
    );

    let missing = tmp.path().join("nothing.journal");
    let (code, _, err) = notary_audit(&[missing.to_str().expect("utf-8 path")]);
    assert_eq!(code, 2, "{err}");
    assert!(err.starts_with("error: cannot read"), "{err}");
}

// ---------------------------------------------------------------------------
// Replication
// ---------------------------------------------------------------------------

/// Port range this file owns for `free_addr` (5000..10000). The
/// runtime's cluster tests own 10000..30000 in 5000-port ranges, which
/// is all the room below the ephemeral floor; `test_ports`' own unit
/// tests probe a couple of ports in this range too, and bind nothing
/// for longer than the probe.
const PORT_BASE: u16 = 5_000;

/// The replica's identity on the replication link.
fn node_key() -> SigningKey {
    SigningKey::from_bytes(&[0xDD; 32])
}

/// The operator: the only role the admin endpoint accepts.
fn operator_key() -> SigningKey {
    SigningKey::from_bytes(&[0xEE; 32])
}

/// Send one admin command over a fresh authenticated connection and
/// return the reply line, or `None` if the connection or handshake
/// failed (the node is not up yet — callers retry).
fn admin_command(addr: SocketAddr, key: &SigningKey, command: &str) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(300)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    answer_challenge(&mut stream, key)?;
    stream.write_all(command.as_bytes()).ok()?;
    stream.write_all(b"\n").ok()?;
    stream.flush().ok()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).ok()?;
    Some(line.trim_end().to_owned())
}

/// Retry `command` until the node answers `OK`. The admin listener is up
/// from boot, but a promotion may still be settling when a command lands.
fn admin_until_ok(addr: SocketAddr, key: &SigningKey, command: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match admin_command(addr, key, command) {
            Some(reply) if reply == "OK" => return,
            reply => {
                assert!(
                    Instant::now() < deadline,
                    "`{command}` never accepted; last reply: {reply:?}"
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

/// The claim in the crate docs — two nodes that applied the same events
/// agree on the head byte for byte — checked the way an operator meets
/// it. The primary dies, the replica is promoted, and the new primary
/// hands out the head the old one receipted, then chains onto it. The
/// time in each receipt is part of what must agree: it is folded into
/// the head, and the replica never took a clock reading of its own.
#[test]
fn a_promoted_replica_reports_the_head_the_primary_receipted() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Three roles: the trader submits, the replica authenticates its
    // link as `replication`, and the operator drives the admin endpoint.
    let auth_path = tmp.path().join("authorized_keys");
    std::fs::write(
        &auth_path,
        format!(
            "trader {} test\nreplication {} replica\noperator {} ops\n",
            pubkey_b64(&trader_key()),
            pubkey_b64(&node_key()),
            pubkey_b64(&operator_key())
        ),
    )
    .expect("write auth keys");
    let key_path = tmp.path().join("replica.key");
    std::fs::write(&key_path, node_key().to_bytes()).expect("write replica key");

    let replication_addr = free_addr(PORT_BASE);
    let admin_addr = free_addr(PORT_BASE);

    // `disk+ram`, the default and the typical deployment: a receipt means
    // one fsynced copy plus a second copy in the replica's memory. That
    // is what makes the primary's death below safe to reason about —
    // every receipted leaf is already on the replica.
    let node_config = |journal: &str, listener: &BlockingTcpListener| ServerConfig {
        bind: listener.local_addr().expect("local_addr"),
        journal: tmp.path().join(journal),
        authorized_keys: auth_path.clone(),
        ack_policy: AckPolicy::DiskAndRam,
        no_mlock: true,
        yield_idle: true,
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        accounts: 0,
        instruments: 0,
        ..ServerConfig::default()
    };

    let primary = {
        let listener = bind_client_listener();
        let mut config = node_config("primary.journal", &listener);
        config.replication_bind = Some(replication_addr);
        spawn_node(listener, config)
    };
    let replica = {
        let listener = bind_client_listener();
        let mut config = node_config("replica.journal", &listener);
        config.replica_of = Some(replication_addr);
        config.replication_key = Some(key_path);
        config.admin_bind = Some(admin_addr);
        spawn_node(listener, config)
    };

    // Notarize on the primary. Under `disk+ram` the first receipt is
    // released only once the replica has attached, so nothing polls for
    // readiness — but that wait is longer than a local round trip.
    let documents: [&[u8]; 3] = [b"deed", b"codicil", b"witness statement"];
    let mut last: Option<Receipt> = None;
    {
        let mut stream = connect_authenticated(primary.addr, &trader_key());
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .expect("set timeout");
        let mut expected = GENESIS_HEAD;
        for (i, document) in documents.iter().enumerate() {
            let leaf = digest(document);
            send_request(&mut stream, i as u64 + 1, TAG_NOTARIZE, &leaf);
            let receipt = receipt_of(&single_response(&mut stream));
            expected = fold(&expected, &leaf, receipt.timestamp_ns);
            assert_eq!(receipt.head, expected, "primary head at entry {}", i + 1);
            last = Some(receipt);
        }
    }
    let last = last.expect("notarized at least one document");

    // The primary dies. The replica loses its link and waits to
    // reconnect; the operator promotes it instead. Its gate still
    // demands a second node, so the operator relaxes it to `disk` — the
    // documented post-failover workflow.
    primary.stop();
    admin_until_ok(admin_addr, &operator_key(), "PROMOTE");
    admin_until_ok(admin_addr, &operator_key(), "ACK-POLICY disk");

    let mut stream = connect_authenticated(replica.addr, &trader_key());
    send_request(&mut stream, 1, TAG_GET_HEAD, &[]);
    assert_eq!(
        head_of(&single_response(&mut stream)),
        (documents.len() as u64, last.head),
        "the promoted replica's head diverged from the primary's receipts"
    );

    // The chain continues where the old primary left off: the new
    // primary's first receipt chains onto the old primary's last.
    let leaf = digest(b"first deed after the failover");
    send_request(&mut stream, 2, TAG_NOTARIZE, &leaf);
    let receipt = receipt_of(&single_response(&mut stream));
    assert_eq!(receipt.entry, documents.len() as u64 + 1);
    assert_eq!(
        receipt.prev, last.head,
        "the first receipt after failover must chain onto the last before it"
    );
    assert_eq!(receipt.head, fold(&last.head, &leaf, receipt.timestamp_ns));

    drop(stream);
    replica.stop();
}
