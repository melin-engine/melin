//! Full round-trip integration test: start notary-server, connect a TCP
//! client with Ed25519 auth, notarize a series of digests, and check the
//! chain head the server reports against one folded independently on the
//! client — the same check an auditor holding the original documents
//! would perform.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};

use melin_journal::{JournalEvent, JournalReader};
use melin_server_runtime::server::{self, ServerConfig};
use melin_wire_protocol::control_codec::{
    TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY,
};
use melin_wire_protocol::tcp::BlockingTcpListener;

use notary_server::{
    GENESIS_HEAD, HEAD_LEN, LEAF_LEN, NotaryEvent, NotaryFactory, RequestDecoder, ResponseEncoder,
    TAG_GET_HEAD, TAG_NOTARIZE, TAG_RESP_HEAD, TAG_RESP_RECEIPT,
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
/// journal, which is how the recovery test exercises replay. Both test
/// identities are authorized every time, so a test picks its role by
/// picking its key.
fn start_server_in(dir: &Path) -> Server {
    let auth_path = dir.join("authorized_keys");
    std::fs::write(
        &auth_path,
        format!(
            "trader {} test\nreadonly {} audit\n",
            pubkey_b64(&trader_key()),
            pubkey_b64(&readonly_key())
        ),
    )
    .expect("write auth keys");

    let listener =
        BlockingTcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().expect("parse addr"))
            .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let config = ServerConfig {
        bind: addr,
        journal: dir.join("notary.journal"),
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

/// Run `notary-client` with `args`, returning `(exit code, stdout, stderr)`.
fn notary_client(args: &[&str]) -> (i32, String, String) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_notary-client"))
        .args(args)
        .output()
        .expect("spawn notary-client");
    (
        output.status.code().expect("client exited normally"),
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        String::from_utf8(output.stderr).expect("utf-8 stderr"),
    )
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
