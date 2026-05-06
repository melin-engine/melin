//! Failover integration test: multi-process primary → replica promotion.
//!
//! 1. Spawn a primary server process with replication enabled
//! 2. Spawn a replica server process connected to the primary
//! 3. Submit orders via the client library, record last acked sequence
//! 4. SIGKILL the primary (simulates crash, no graceful shutdown)
//! 5. Promote the replica via the promotion endpoint
//! 6. Reconnect a client to the promoted replica (now primary)
//! 7. Verify the promoted replica's journal sequence >= last acked sequence
//!    and that the exchange state is consistent (balances, order placement)
//!
//! Uses actual child processes (`melin-server` binary) and TCP so the test
//! exercises the real replication and promotion code paths.
//!
//! Trading-only — the scenarios under test (order submit / balance /
//! matching invariants) are meaningful only against the real engine. The
//! noop build's promoted replica would trivially pass because every order
//! is rejected with `NoLiquidity`. When running `cargo test` against the
//! noop build this file is compiled as an empty test crate.

#![cfg(all(feature = "trading", not(feature = "noop")))]

use ed25519_dalek::Signer;
use serial_test::serial;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use melin_client::Client;
use melin_protocol::message::Request;
use melin_protocol::types::{
    AccountId, Order, OrderId, OrderType, Price, Quantity, Side, Symbol, TimeInForce,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Path to the `melin-server` binary, resolved at compile time. `env!` works
/// under any runner; `std::env::var` only works under `cargo test`, where it
/// would otherwise silently pick up a stale release binary.
fn server_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_melin-server"))
}

/// Allocate a unique TCP port for a test-spawned server.
///
/// Previous implementation bound an ephemeral port (`127.0.0.1:0`) and
/// returned it after dropping the listener. That left a race window: the
/// kernel could hand the same port to a concurrent test before the
/// spawned child actually bound it, producing flakes that look like
/// `Address already in use`, `Connection reset`, or — most insidiously
/// — wrong-protocol bytes leaking between tests (e.g. text from the
/// health endpoint hitting a replication socket and surfacing as
/// `frame too large: <ascii-as-int>`).
///
/// nextest spawns each test in its own process, so a per-process atomic
/// can't coordinate across tests either. Use a file-locked counter in
/// `/tmp` — every test process across the binary's run shares the same
/// counter, advancing it monotonically. Ports live in 20000..32000,
/// below the kernel's ephemeral range (default 32768..60999) so the
/// kernel never hands out a port we've already reserved.
fn free_port() -> u16 {
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::fd::AsRawFd;

    const PORT_FILE: &str = "/tmp/melin_test_port_alloc";
    const PORT_FLOOR: u16 = 20_000;
    const PORT_CEILING: u16 = 32_000;

    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(PORT_FILE)
        .expect("open port allocator file");

    // SAFETY: flock(2) on a valid fd; LOCK_EX blocks until acquired.
    let rc = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX) };
    assert!(rc == 0, "flock failed: {}", std::io::Error::last_os_error());

    let mut s = String::new();
    let _ = f.read_to_string(&mut s);
    let next: u16 = s.trim().parse().unwrap_or(PORT_FLOOR);
    let port = if next >= PORT_CEILING {
        PORT_FLOOR
    } else {
        next
    };
    let after = port + 1;

    f.seek(SeekFrom::Start(0)).expect("seek port file");
    f.set_len(0).expect("truncate port file");
    write!(f, "{after}").expect("write port file");

    // SAFETY: same fd we LOCK_EX'd above.
    let _ = unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_UN) };

    port
}

/// Write an authorized_keys file for multiple test keys, an operator key
/// (for promotion auth), plus a replication key.
/// Returns (authorized_keys_path, replication_key_path).
fn write_auth_keys_multi(
    dir: &Path,
    keys: &[&SigningKey],
    operator_key: &SigningKey,
    repl_key: &SigningKey,
) -> (PathBuf, PathBuf) {
    let path = dir.join("authorized_keys");
    let mut content = String::new();
    for (i, key) in keys.iter().enumerate() {
        let pub_key_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            key.verifying_key().to_bytes(),
        );
        // Use trader permission so orders can be submitted.
        content.push_str(&format!("trader {pub_key_b64} test-key-{i}\n"));
    }
    // Add operator key (used for authenticated promotion).
    let ops_pub_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        operator_key.verifying_key().to_bytes(),
    );
    content.push_str(&format!("operator {ops_pub_b64} ops\n"));
    // Add replication key.
    let repl_pub_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        repl_key.verifying_key().to_bytes(),
    );
    content.push_str(&format!("replication {repl_pub_b64} replication\n"));
    std::fs::write(&path, content).expect("write authorized_keys");

    // Write the replication private key seed to a file.
    let repl_key_path = dir.join("replication.key");
    std::fs::write(&repl_key_path, repl_key.to_bytes()).expect("write replication key");
    (path, repl_key_path)
}

/// Poll the health endpoint until it responds or timeout.
/// Returns the parsed status line: (conns, journal_seq, repl_lag, trading).
fn wait_healthy(addr: SocketAddr, timeout: Duration) -> (u64, u64, u64, bool) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("health endpoint {addr} did not respond within {timeout:?}");
        }
        if let Ok(status) = query_health(addr) {
            return status;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Poll the health endpoint until the server is ready to accept client
/// traffic (pipeline up, `trading` flag set), not just responding to
/// /healthz.
///
/// Replaces the `wait_healthy(...) + sleep(1s)` pattern where the sleep
/// was a fixed-duration workaround for the gap between the health
/// endpoint answering and the pipeline being ready. With a real readiness
/// check the wait is bounded by actual readiness, not a magic constant.
fn wait_ready(addr: SocketAddr, timeout: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("server {addr} did not become ready within {timeout:?}");
        }
        if let Ok((_, _, _, true)) = query_health(addr) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Inverse of `wait_ready` — poll until the server has flipped to
/// `trading == false` (e.g. after losing all replicas in a replication
/// quorum). Replaces a fixed `sleep(1s)` placed after the kill that
/// triggers the halt.
fn wait_halted(addr: SocketAddr, timeout: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("server {addr} did not halt within {timeout:?}");
        }
        if let Ok((_, _, _, false)) = query_health(addr) {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Wait for the primary's replication endpoint to be ready to accept
/// inbound connections from replicas. Polls the *health* endpoint
/// rather than the replication port directly so the same helper works
/// regardless of replication transport — TCP (default features) and
/// rumcast (UDP) both bind their replication socket during the same
/// startup phase as the health endpoint, so a responsive `/healthz`
/// is a reliable proxy for "replica may now connect".
///
/// Replaces older per-call `TcpStream::connect_timeout` probes that
/// silently failed under `--features rumcast` because rumcast
/// replication binds UDP, not TCP.
fn wait_for_primary_repl_ready(health_addr: SocketAddr, timeout: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("primary {health_addr} never became ready for replica connections");
        }
        if query_health(health_addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Query the health endpoint once. Returns (conns, journal_seq, repl_lag, trading).
fn query_health(addr: SocketAddr) -> Result<(u64, u64, u64, bool), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buf = [0u8; 256];
    let n = stream.read(&mut buf)?;
    let line = std::str::from_utf8(&buf[..n])?.trim().to_string();
    // Format: "OK <conns> <journal_seq> <repl_lag> trading|halted"
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() < 5 || parts[0] != "OK" {
        return Err(format!("unexpected health response: {line}").into());
    }
    Ok((
        parts[1].parse()?,
        parts[2].parse()?,
        parts[3].parse()?,
        parts[4] == "trading",
    ))
}

/// Wait for a freshly-spawned replacement replica to fully catch up via
/// the primary's lag metric. The primary's `replication_lag` is
/// `journal_seq - min(slot0, slot1)`, with disconnected slots pinned to
/// `u64::MAX` (and thus excluded from the min). After a replica is killed
/// its slot is excluded, so lag can read 0 from the surviving replica
/// alone — even before the new replacement has connected. To avoid
/// promoting a not-yet-caught-up replica, wait for lag to first transition
/// to a nonzero value (replacement connected with a behind handshake) and
/// then back to zero (caught up).
fn wait_for_replacement_catchup(primary_health: SocketAddr) {
    let start = Instant::now();
    let mut saw_nonzero = false;
    loop {
        if let Ok((_, _, lag, _)) = query_health(primary_health) {
            if lag > 0 {
                saw_nonzero = true;
            } else if saw_nonzero {
                return;
            }
        }
        if start.elapsed() > Duration::from_secs(30) {
            panic!("replacement catch-up timeout (saw_nonzero={saw_nonzero})");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Authenticate and send PROMOTE to the promotion endpoint.
fn promote(addr: SocketAddr, operator_key: &SigningKey) {
    use melin_protocol::codec;
    use melin_protocol::message::{Request, ResponseKind};

    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .expect("connect to promotion endpoint");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");

    // Step 1: Receive Challenge.
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).expect("read challenge len");
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    let mut frame_buf = vec![0u8; frame_len];
    stream
        .read_exact(&mut frame_buf)
        .expect("read challenge payload");
    let (nonce, server_eph) = match codec::decode_response(&frame_buf).expect("decode challenge") {
        ResponseKind::Challenge {
            nonce,
            server_x25519_eph,
        } => (nonce, server_x25519_eph),
        other => panic!("expected Challenge, got {other:?}"),
    };

    // Step 2: Sign nonce + ephemerals (TCP path uses zero ephs).
    let client_x25519_eph = [0u8; 32];
    let signing_payload =
        melin_protocol::auth::auth_signing_payload(&nonce, &server_eph, &client_x25519_eph);
    let signature = operator_key.sign(&signing_payload);
    let request = Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: operator_key.verifying_key().to_bytes(),
        client_x25519_eph,
    };
    let mut encode_buf = [0u8; 256];
    let written = codec::encode_request(&request, 0, &mut encode_buf).expect("encode");
    stream
        .write_all(&encode_buf[..written])
        .expect("send ChallengeResponse");
    stream.flush().expect("flush");

    // Step 3: Read auth result.
    stream
        .read_exact(&mut len_buf)
        .expect("read auth result len");
    let result_len = u32::from_le_bytes(len_buf) as usize;
    let mut result_buf = vec![0u8; result_len];
    stream
        .read_exact(&mut result_buf)
        .expect("read auth result payload");
    match codec::decode_response(&result_buf).expect("decode auth result") {
        ResponseKind::ServerReady => {}
        ResponseKind::AuthFailed => panic!("promotion auth failed"),
        other => panic!("unexpected auth response: {other:?}"),
    }

    // Step 4: Send PROMOTE command.
    stream.write_all(b"PROMOTE\n").expect("send PROMOTE");
    let mut reader = BufReader::new(&stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .expect("read promotion response");
    assert!(response.trim() == "OK", "promotion failed: {response}");
}

struct ServerProcess {
    child: Child,
    client_addr: SocketAddr,
    health_addr: SocketAddr,
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        // Best-effort cleanup: try SIGKILL if still running.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn a primary server process.
fn spawn_primary(
    bin: &Path,
    tmp_dir: &Path,
    keys_path: &Path,
    client_port: u16,
    health_port: u16,
    replication_port: u16,
) -> ServerProcess {
    let journal = tmp_dir.join("primary.journal");
    let child = Command::new(bin)
        .args([
            "--bind",
            &format!("127.0.0.1:{client_port}"),
            "--health-bind",
            &format!("127.0.0.1:{health_port}"),
            "--replication-bind",
            &format!("127.0.0.1:{replication_port}"),
            "--journal",
            journal.to_str().expect("valid path"),
            "--authorized-keys",
            keys_path.to_str().expect("valid path"),
            "--accounts",
            "10",
            "--instruments",
            "2",
            "--connection-timeout-secs",
            "0",
            "--yield-idle",
            // Reduce core count to avoid conflicts in CI.
            "--cores",
            "0,0,0,0,0,0,0,0",
            "--reader-cores",
            "0",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
        .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
        .spawn()
        .expect("spawn primary server");

    ServerProcess {
        child,
        client_addr: format!("127.0.0.1:{client_port}").parse().unwrap(),
        health_addr: format!("127.0.0.1:{health_port}").parse().unwrap(),
    }
}

/// Spawn a replica server process.
fn spawn_replica(
    bin: &Path,
    tmp_dir: &Path,
    keys_path: &Path,
    repl_key_path: &Path,
    primary_repl_port: u16,
    client_port: u16,
    health_port: u16,
    promote_port: u16,
) -> ServerProcess {
    spawn_replica_named(
        bin,
        tmp_dir,
        keys_path,
        repl_key_path,
        primary_repl_port,
        client_port,
        health_port,
        promote_port,
        "replica",
    )
}

#[allow(clippy::too_many_arguments)]
fn spawn_replica_named(
    bin: &Path,
    tmp_dir: &Path,
    keys_path: &Path,
    repl_key_path: &Path,
    primary_repl_port: u16,
    client_port: u16,
    health_port: u16,
    promote_port: u16,
    name: &str,
) -> ServerProcess {
    spawn_replica_named_with_extra(
        bin,
        tmp_dir,
        keys_path,
        repl_key_path,
        primary_repl_port,
        client_port,
        health_port,
        promote_port,
        name,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
fn spawn_replica_named_with_extra(
    bin: &Path,
    tmp_dir: &Path,
    keys_path: &Path,
    repl_key_path: &Path,
    primary_repl_port: u16,
    client_port: u16,
    health_port: u16,
    promote_port: u16,
    name: &str,
    extra_args: &[&str],
) -> ServerProcess {
    let journal = tmp_dir.join(format!("{name}.journal"));
    // Vec<String> chosen so we can grow with extra_args at runtime; the
    // base args are pushed first, then any test-supplied flags.
    let mut args: Vec<String> = vec![
        "--bind".into(),
        format!("127.0.0.1:{client_port}"),
        "--health-bind".into(),
        format!("127.0.0.1:{health_port}"),
        "--replica-of".into(),
        format!("127.0.0.1:{primary_repl_port}"),
        "--replication-key".into(),
        repl_key_path.to_str().expect("valid path").into(),
        "--promote-bind".into(),
        format!("127.0.0.1:{promote_port}"),
        "--journal".into(),
        journal.to_str().expect("valid path").into(),
        "--authorized-keys".into(),
        keys_path.to_str().expect("valid path").into(),
        "--connection-timeout-secs".into(),
        "0".into(),
        "--yield-idle".into(),
        "--cores".into(),
        "0,0,0,0,0,0,0,0".into(),
        "--reader-cores".into(),
        "0".into(),
    ];
    for a in extra_args {
        args.push((*a).into());
    }
    let child = Command::new(bin)
        .args(&args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
        .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
        .spawn()
        .expect("spawn replica server");

    ServerProcess {
        child,
        client_addr: format!("127.0.0.1:{client_port}").parse().unwrap(),
        health_addr: format!("127.0.0.1:{health_port}").parse().unwrap(),
    }
}

fn qty(n: u64) -> Quantity {
    Quantity(std::num::NonZeroU64::new(n).unwrap())
}

fn price(n: u64) -> Price {
    Price(std::num::NonZeroU64::new(n).unwrap())
}

// ---------------------------------------------------------------------------
// Shared test harness
// ---------------------------------------------------------------------------

struct TestCluster {
    primary: ServerProcess,
    replica: ServerProcess,
    promote_port: u16,
    key: SigningKey,
    key2: SigningKey,
    operator_key: SigningKey,
    bin: PathBuf,
    keys_path: PathBuf,
    #[allow(dead_code)] // Available for replacement replica spawns.
    repl_key_path: PathBuf,
    _tmp: tempfile::TempDir,
}

impl TestCluster {
    /// Spin up a primary + replica pair, wait for both to be healthy.
    fn start() -> Self {
        let bin = server_bin();
        assert!(
            bin.exists(),
            "melin-server binary not found at {bin:?}. Run `cargo build --release` first."
        );

        let tmp = tempfile::tempdir().expect("create temp dir");
        let key = SigningKey::from_bytes(&[0xFA; 32]);
        let key2 = SigningKey::from_bytes(&[0xFB; 32]);
        let operator_key = SigningKey::from_bytes(&[0xFD; 32]);
        let repl_key = SigningKey::from_bytes(&[0xFC; 32]);
        let (keys_path, repl_key_path) =
            write_auth_keys_multi(tmp.path(), &[&key, &key2], &operator_key, &repl_key);

        let primary_client_port = free_port();
        let primary_health_port = free_port();
        let primary_repl_port = free_port();
        let replica_client_port = free_port();
        let replica_health_port = free_port();
        let replica_promote_port = free_port();

        let primary = spawn_primary(
            &bin,
            tmp.path(),
            &keys_path,
            primary_client_port,
            primary_health_port,
            primary_repl_port,
        );

        // Wait for the primary to be ready to accept replica connections.
        wait_for_primary_repl_ready(primary.health_addr, Duration::from_secs(10));

        let replica = spawn_replica(
            &bin,
            tmp.path(),
            &keys_path,
            &repl_key_path,
            primary_repl_port,
            replica_client_port,
            replica_health_port,
            replica_promote_port,
        );

        wait_healthy(primary.health_addr, Duration::from_secs(30));

        Self {
            primary,
            replica,
            promote_port: replica_promote_port,
            key,
            key2,
            operator_key,
            bin,
            keys_path,
            repl_key_path,
            _tmp: tmp,
        }
    }

    /// Connect a client to the primary using the first key.
    fn connect_primary(&self) -> Client {
        Client::connect(self.primary.client_addr, &self.key).expect("connect to primary")
    }

    /// Wait for replication lag to reach 0.
    fn wait_replicated(&self) {
        let start = Instant::now();
        loop {
            if let Ok((_, _, 0, _)) = query_health(self.primary.health_addr) {
                return;
            }
            if start.elapsed() > Duration::from_secs(10) {
                panic!("replication lag did not reach 0 within 10s");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// SIGKILL the primary and promote the replica. Returns a client
    /// connected to the promoted replica.
    fn kill_and_promote(&mut self) -> Client {
        unsafe {
            libc::kill(self.primary.child.id() as i32, libc::SIGKILL);
        }
        let _ = self.primary.child.wait();

        let promote_addr: SocketAddr = format!("127.0.0.1:{}", self.promote_port).parse().unwrap();
        promote(promote_addr, &self.operator_key);

        wait_ready(self.replica.health_addr, Duration::from_secs(30));

        Client::connect(self.replica.client_addr, &self.key2).expect("connect to promoted replica")
    }
}

fn submit_order(
    client: &mut Client,
    id: u64,
    account: u32,
    symbol: u32,
    side: Side,
    price_val: u64,
    qty_val: u64,
) -> Vec<melin_protocol::message::ResponseKind> {
    client
        .send_request(&Request::SubmitOrder {
            symbol: Symbol(symbol),
            order: Order {
                id: OrderId(id),
                account: AccountId(account),
                side,
                order_type: OrderType::Limit {
                    price: price(price_val),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(qty_val),
                stp: melin_protocol::types::SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
        })
        .expect("submit order")
}

fn has_report(
    responses: &[melin_protocol::message::ResponseKind],
    pred: fn(&melin_protocol::types::ExecutionReport) -> bool,
) -> bool {
    responses.iter().any(|r| {
        if let melin_protocol::message::ResponseKind::Report(report) = r {
            pred(report)
        } else {
            false
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
#[serial]
fn kill_primary_promote_replica_no_data_loss() {
    let mut cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Place 50 resting buy orders.
    for i in 1..=50u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response");
    }

    cluster.wait_replicated();
    let mut client2 = cluster.kill_and_promote();

    // New order on promoted replica must succeed (proves full state replicated).
    let r = submit_order(&mut client2, 51, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected Placed, got: {r:?}"
    );

    // Crossing sell fills against the buy — proves matching works.
    let r = submit_order(&mut client2, 52, 1, 1, Side::Sell, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Fill { .. }
        )),
        "expected Fill, got: {r:?}"
    );
}

/// Kill the primary while fills are actively happening. Verifies that
/// balance conservation holds after promotion — no phantom fills or
/// leaked reservations.
#[test]
#[serial]
fn kill_during_active_fills() {
    let mut cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Place resting sells from account 2 at various prices.
    // Account 2 was seeded with balances for instrument 1 (base currency).
    for i in 1..=20u64 {
        let r = submit_order(&mut client, i, 2, 1, Side::Sell, 100 + i, 10);
        assert!(!r.is_empty());
    }

    // Aggressive buys from account 1 that cross the spread — generates fills.
    // Each buy at price 200 sweeps all resting sells (100..120).
    for i in 21..=40u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 200, 5);
        assert!(!r.is_empty());
    }

    cluster.wait_replicated();
    let mut client2 = cluster.kill_and_promote();

    // Verify the promoted replica can still trade.
    // Place a new sell — must succeed (account 2 should have remaining
    // base currency from partial fills).
    let r = submit_order(&mut client2, 41, 2, 1, Side::Sell, 300, 1);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected Placed after fill-heavy workload, got: {r:?}"
    );

    // Place a matching buy — proves matching is operational.
    let r = submit_order(&mut client2, 42, 1, 1, Side::Buy, 300, 1);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Fill { .. }
        )),
        "expected Fill after fill-heavy workload, got: {r:?}"
    );
}

/// Kill the primary IMMEDIATELY after submitting orders, without waiting
/// for replication lag to reach 0. The response gate guarantees that every
/// acked response was replicated — verify the promoted replica has at
/// least the acked state.
#[test]
#[serial]
fn kill_without_waiting_for_replication() {
    let mut cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Submit orders — each response is gated on replication, so every
    // acked order is guaranteed to be on the replica's journal.
    let mut last_acked_id = 0u64;
    for i in 1..=30u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        if !r.is_empty() {
            last_acked_id = i;
        }
    }
    assert!(last_acked_id > 0, "no orders were acked");

    // Kill immediately — do NOT wait for replication lag to reach 0.
    // The response gate already ensures durability.
    drop(client);
    let mut client2 = cluster.kill_and_promote();

    // The promoted replica must accept an order with ID > last_acked_id.
    // If it does, the HWM was replayed correctly (all acked orders are present).
    let r = submit_order(&mut client2, last_acked_id + 1, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected Placed with id={}, got: {r:?}",
        last_acked_id + 1
    );

    // Verify a duplicate of the last acked order is rejected (proves
    // the replica has the exact same dedup state).
    let r = submit_order(&mut client2, last_acked_id, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "expected DuplicateOrderId for id={last_acked_id}, got: {r:?}"
    );
}

/// After killing the primary and promoting the replica, restart the old
/// primary from its journal in standalone mode. Verify it recovers to the
/// same state (can place orders, rejects duplicates). This tests journal
/// crash recovery on a server that was SIGKILLed.
#[test]
#[serial]
fn crashed_primary_recovers_from_journal() {
    let mut cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Submit orders with fills to create interesting state.
    for i in 1..=10u64 {
        submit_order(&mut client, i, 2, 1, Side::Sell, 100 + i, 5);
    }
    for i in 11..=20u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 200, 3);
    }

    cluster.wait_replicated();

    // SIGKILL the primary (unclean shutdown — partial writes possible).
    unsafe {
        libc::kill(cluster.primary.child.id() as i32, libc::SIGKILL);
    }
    let _ = cluster.primary.child.wait();

    // Restart the old primary from its journal in standalone mode.
    // The journal may have a trailing partial write from the SIGKILL —
    // recovery must truncate it and continue.
    let primary_journal = cluster._tmp.path().join("primary.journal");
    assert!(primary_journal.exists(), "primary journal must exist");

    let recovered_client_port = free_port();
    let recovered_health_port = free_port();
    let recovered = {
        let child = Command::new(&cluster.bin)
            .args([
                "--bind",
                &format!("127.0.0.1:{recovered_client_port}"),
                "--health-bind",
                &format!("127.0.0.1:{recovered_health_port}"),
                "--standalone",
                "--journal",
                primary_journal.to_str().expect("valid path"),
                "--authorized-keys",
                cluster.keys_path.to_str().expect("valid path"),
                "--accounts",
                "10",
                "--instruments",
                "2",
                "--connection-timeout-secs",
                "0",
                "--yield-idle",
                "--cores",
                "0,0,0,0,0,0,0,0",
                "--reader-cores",
                "0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
            .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
            .spawn()
            .expect("spawn recovered primary");
        ServerProcess {
            child,
            client_addr: format!("127.0.0.1:{recovered_client_port}")
                .parse()
                .unwrap(),
            health_addr: format!("127.0.0.1:{recovered_health_port}")
                .parse()
                .unwrap(),
        }
    };

    wait_ready(recovered.health_addr, Duration::from_secs(30));

    let mut client3 = Client::connect(recovered.client_addr, &cluster.key2)
        .expect("connect to recovered primary");

    // New order must succeed — proves recovery restored instruments + balances.
    // May fill against resting orders that survived recovery, so accept
    // either Placed or Fill.
    let r = submit_order(&mut client3, 21, 1, 1, Side::Buy, 300, 1);
    let accepted = has_report(&r, |rep| {
        matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })
    }) || has_report(&r, |rep| {
        matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. })
    });
    assert!(
        accepted,
        "expected Placed or Fill on recovered primary, got: {r:?}"
    );

    // Duplicate of an old order must be rejected — proves HWM recovered.
    let r = submit_order(&mut client3, 10, 2, 1, Side::Sell, 100, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "expected DuplicateOrderId on recovered primary, got: {r:?}"
    );
}

/// End-to-end repro attempt for the checkpoint-boundary sequence
/// corruption seen by `journal_verify` after LAN benches:
///
///     error at entry 10001: sequence gap: expected 10003, got 10002
///
/// Spins up a real primary + replica pair (`melin-server` binary, TCP
/// loopback), submits enough orders to cross the first auto-emitted
/// checkpoint boundary, waits for replication to catch up, then walks
/// both journal files end-to-end with `JournalReader`. Any
/// `SequenceGap` surfaced by the strict-continuity check is the same
/// corruption mode reported by the external verifier.
///
/// Spawned servers are launched with `MELIN_JOURNAL_CHECKPOINT_INTERVAL=100`
/// (see the test setup — env vars below `Command::new`), so the test only
/// has to push a few hundred orders to cross multiple checkpoint boundaries
/// instead of 100_000+ at the production default.
#[test]
#[serial]
fn journals_contiguous_across_checkpoint_boundary() {
    use melin_journal::JournalReader;

    let cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Server seeds 10 accounts + 2 instruments → 12 seed events before the
    // first order. With CHECKPOINT_INTERVAL=100 (set via env on the spawned
    // server), 250 orders cross the boundary at least twice — exercising
    // first-segment and post-checkpoint-segment both.
    const ORDERS: u64 = 250;
    for i in 1..=ORDERS {
        let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
        let r = submit_order(&mut client, i, 1, 1, side, 100, 1);
        assert!(!r.is_empty(), "order {i}: no response");
    }

    // Drain every ack so the journal and the replica have caught up.
    cluster.wait_replicated();
    drop(client);

    // Give the journal stages one last moment to fsync their current
    // batch (acks are sent after the persist-before-ack boundary, so
    // this is belt-and-suspenders).
    std::thread::sleep(Duration::from_millis(250));

    let primary_journal = cluster._tmp.path().join("primary.journal");
    let replica_journal = cluster._tmp.path().join("replica.journal");

    let walk = |label: &str, path: &Path| -> u64 {
        let mut reader = JournalReader::<melin_trading::trading_event::TradingEvent>::open(path)
            .unwrap_or_else(|e| panic!("{label}: open {}: {e}", path.display()));
        let mut count = 0u64;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => count += 1,
                Ok(None) => break,
                Err(e) => panic!(
                    "{label}: read error after {count} user entries \
                     (last_sequence = {:?}): {e}",
                    reader.last_sequence()
                ),
            }
        }
        count
    };

    let primary_count = walk("primary", &primary_journal);
    let replica_count = walk("replica", &replica_journal);

    // Counts must cover at least the submitted orders. Seed events (12)
    // plus orders (ORDERS) — equality would be too strict if the server
    // emits additional internal events (ticks), so use a lower bound.
    assert!(
        primary_count >= ORDERS,
        "primary journal recovered {primary_count} entries, expected >= {ORDERS}"
    );
    assert!(
        replica_count >= ORDERS,
        "replica journal recovered {replica_count} entries, expected >= {ORDERS}"
    );
}

/// Same invariant as above (`journals_contiguous_across_checkpoint_boundary`),
/// but drives load through the real `melin-bench` binary instead of a
/// synchronous in-test client. This matches the LAN bench's publisher
/// shape: multiple concurrent clients, deep in-flight window, real
/// io_uring on both sides. If the checkpoint-boundary duplicate is
/// timing-sensitive to concurrency (and the single-client test above
/// can't trigger it), this is the version that should.
#[test]
#[serial]
fn bench_binary_journals_contiguous_across_checkpoint_boundary() {
    use melin_journal::JournalReader;

    // Locate (or build) the `melin-bench` binary using the same target
    // profile Cargo picked for `melin-server` — `CARGO_BIN_EXE_melin-server`
    // gives us the profile directory to infer from.
    let server_bin_path = PathBuf::from(env!("CARGO_BIN_EXE_melin-server"));
    let profile_dir = server_bin_path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .expect("server binary path has profile component");
    let target_dir = server_bin_path
        .parent()
        .and_then(|p| p.parent())
        .expect("server binary path has target dir");
    let bench_bin = target_dir.join(profile_dir).join("melin-bench");

    let mut build = Command::new(env!("CARGO"));
    build.args(["build", "-p", "melin-bench"]);
    if profile_dir == "release" {
        build.arg("--release");
    }
    // Mirror the server's transport feature so bench speaks the same
    // wire protocol. Under `--features rumcast` the server binds UDP;
    // the bench must also be built with rumcast to connect.
    #[cfg(feature = "rumcast")]
    build.args(["--features", "rumcast", "--no-default-features"]);
    let build_status = build.status().expect("spawn cargo build melin-bench");
    assert!(
        build_status.success(),
        "cargo build melin-bench failed (status {build_status})"
    );
    assert!(
        bench_bin.exists(),
        "melin-bench binary missing at {}",
        bench_bin.display()
    );

    let cluster = TestCluster::start();

    // The bench needs the trader key on disk (32 raw Ed25519 bytes).
    let key_path = cluster._tmp.path().join("bench.key");
    std::fs::write(&key_path, cluster.key.to_bytes()).expect("write bench key");

    // Run the bench against the primary. The spawned server uses
    // CHECKPOINT_INTERVAL=100 (env var below), so a few hundred journaled
    // orders is enough to cross several boundaries. 4 clients × window
    // 128, 250 pairs = 500 journaled orders — crosses ~5 boundaries.
    //
    // --warmup 0: the default 100_000 warmup orders would dominate runtime.
    // --accounts 10 / --instruments 2: match the server defaults used in
    // `spawn_primary` so the generator doesn't send orders for symbols
    // the server never created.
    let mut bench_cmd = Command::new(&bench_bin);
    bench_cmd.args([
        "--mode=roundtrip",
        "--addr",
        &cluster.primary.client_addr.to_string(),
        "--health-addr",
        &cluster.primary.health_addr.to_string(),
        "--key",
        key_path.to_str().expect("key path utf-8"),
        "--clients",
        "4",
        "--window",
        "128",
        "--warmup",
        "0",
        "--accounts",
        "10",
        "--instruments",
        "2",
        "250",
    ]);
    // Under rumcast the bench must bind a local UDP socket so the server
    // can send response frames back. Port 0 lets the OS assign an
    // ephemeral port; the server learns the real address from the Setup
    // packet source.
    #[cfg(feature = "rumcast")]
    bench_cmd.args(["--rumcast-bind", "127.0.0.1:0"]);
    let bench_status = bench_cmd
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
        .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
        .status()
        .expect("spawn melin-bench");
    assert!(
        bench_status.success(),
        "melin-bench exited with {bench_status}"
    );

    cluster.wait_replicated();
    // Extra slack for the journal stages to finalize their current batch.
    std::thread::sleep(Duration::from_millis(500));

    let walk = |label: &str, path: &Path| -> u64 {
        let mut reader = JournalReader::<melin_trading::trading_event::TradingEvent>::open(path)
            .unwrap_or_else(|e| panic!("{label}: open {}: {e}", path.display()));
        let mut count = 0u64;
        loop {
            match reader.next_entry() {
                Ok(Some(_)) => count += 1,
                Ok(None) => break,
                Err(e) => panic!(
                    "{label}: read error after {count} entries \
                     (last_sequence = {:?}): {e}",
                    reader.last_sequence()
                ),
            }
        }
        count
    };

    let primary_count = walk("primary", &cluster._tmp.path().join("primary.journal"));
    let replica_count = walk("replica", &cluster._tmp.path().join("replica.journal"));

    // Lower bound: 500 orders from the bench (250 pairs × 2). The server
    // may add internal events (ticks, seed) — so use >= rather than equality.
    assert!(
        primary_count >= 500,
        "primary journal only has {primary_count} entries"
    );
    assert!(
        replica_count >= 500,
        "replica journal only has {replica_count} entries"
    );
}

/// Reconnect with the SAME key after failover and retry the last request.
/// The per-key request sequence HWM must reject it as DuplicateRequest
/// (not re-execute it). This tests that the per-key dedup state survives
/// replication and promotion.
#[test]
#[serial]
fn same_key_retry_after_failover_is_rejected() {
    let mut cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Submit 10 orders. The client's internal next_seq reaches 10.
    for i in 1..=10u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
    }

    cluster.wait_replicated();

    // Record the client's last request sequence (next_seq was incremented
    // to 10 after the 10th send_request). We'll need to replicate this
    // exact state on the new connection.
    drop(client);

    // Kill + promote.
    let promote_addr: SocketAddr = format!("127.0.0.1:{}", cluster.promote_port)
        .parse()
        .unwrap();
    unsafe {
        libc::kill(cluster.primary.child.id() as i32, libc::SIGKILL);
    }
    let _ = cluster.primary.child.wait();
    promote(promote_addr, &cluster.operator_key);
    wait_ready(cluster.replica.health_addr, Duration::from_secs(30));

    // Reconnect with the SAME key (key, not key2). The promoted replica's
    // per-key HWM for this key should be 10 (from the 10 requests above).
    // A fresh Client starts at next_seq=0, so the first send uses seq=1.
    // Since 1 <= 10 (the HWM), it should be rejected as DuplicateRequest.
    let mut client_retry = Client::connect(cluster.replica.client_addr, &cluster.key)
        .expect("reconnect with same key");
    let r = submit_order(&mut client_retry, 11, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateRequest,
                ..
            }
        )),
        "expected DuplicateRequest for stale seq on same key, got: {r:?}"
    );
}

// ---------------------------------------------------------------------------
// Dual replication helpers
// ---------------------------------------------------------------------------

struct DualCluster {
    primary: ServerProcess,
    primary_repl_port: u16,
    replica1: ServerProcess,
    replica2: ServerProcess,
    replica1_promote_port: u16,
    replica2_promote_port: u16,
    key: SigningKey,
    key2: SigningKey,
    operator_key: SigningKey,
    repl_key_path: PathBuf,
    _tmp: tempfile::TempDir,
}

impl DualCluster {
    fn start() -> Self {
        Self::start_with_replica_args(&[])
    }

    fn start_with_replica_args(replica_extra_args: &[&str]) -> Self {
        let bin = server_bin();
        assert!(bin.exists(), "melin-server binary not found");

        let tmp = tempfile::tempdir().expect("create temp dir");
        let key = SigningKey::from_bytes(&[0xFA; 32]);
        let key2 = SigningKey::from_bytes(&[0xFB; 32]);
        let operator_key = SigningKey::from_bytes(&[0xFD; 32]);
        let repl_key = SigningKey::from_bytes(&[0xFC; 32]);
        let (keys_path, repl_key_path) =
            write_auth_keys_multi(tmp.path(), &[&key, &key2], &operator_key, &repl_key);

        let primary_client_port = free_port();
        let primary_health_port = free_port();
        let primary_repl_port = free_port();
        let r1_client = free_port();
        let r1_health = free_port();
        let r1_promote = free_port();
        let r2_client = free_port();
        let r2_health = free_port();
        let r2_promote = free_port();

        let primary = spawn_primary(
            &bin,
            tmp.path(),
            &keys_path,
            primary_client_port,
            primary_health_port,
            primary_repl_port,
        );

        // Wait for the primary to be ready to accept replica connections.
        wait_for_primary_repl_ready(primary.health_addr, Duration::from_secs(10));

        let replica1 = spawn_replica_named_with_extra(
            &bin,
            tmp.path(),
            &keys_path,
            &repl_key_path,
            primary_repl_port,
            r1_client,
            r1_health,
            r1_promote,
            "replica1",
            replica_extra_args,
        );
        let replica2 = spawn_replica_named_with_extra(
            &bin,
            tmp.path(),
            &keys_path,
            &repl_key_path,
            primary_repl_port,
            r2_client,
            r2_health,
            r2_promote,
            "replica2",
            replica_extra_args,
        );

        wait_healthy(primary.health_addr, Duration::from_secs(30));

        Self {
            primary,
            primary_repl_port,
            replica1,
            replica2,
            replica1_promote_port: r1_promote,
            replica2_promote_port: r2_promote,
            key,
            key2,
            operator_key,
            repl_key_path,
            _tmp: tmp,
        }
    }

    fn connect_primary(&self) -> Client {
        Client::connect(self.primary.client_addr, &self.key).expect("connect to primary")
    }

    fn wait_replicated(&self) {
        let start = Instant::now();
        loop {
            if let Ok((_, _, 0, _)) = query_health(self.primary.health_addr) {
                return;
            }
            if start.elapsed() > Duration::from_secs(10) {
                panic!("replication lag did not reach 0 within 10s");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn kill_primary(&mut self) {
        unsafe {
            libc::kill(self.primary.child.id() as i32, libc::SIGKILL);
        }
        let _ = self.primary.child.wait();
    }

    fn kill_replica1(&mut self) {
        unsafe {
            libc::kill(self.replica1.child.id() as i32, libc::SIGKILL);
        }
        let _ = self.replica1.child.wait();
    }

    fn kill_replica2(&mut self) {
        unsafe {
            libc::kill(self.replica2.child.id() as i32, libc::SIGKILL);
        }
        let _ = self.replica2.child.wait();
    }

    fn promote_replica1(&self) -> Client {
        let addr: SocketAddr = format!("127.0.0.1:{}", self.replica1_promote_port)
            .parse()
            .unwrap();
        promote(addr, &self.operator_key);
        wait_ready(self.replica1.health_addr, Duration::from_secs(30));
        Client::connect(self.replica1.client_addr, &self.key2)
            .expect("connect to promoted replica 1")
    }

    fn promote_replica2(&self) -> Client {
        let addr: SocketAddr = format!("127.0.0.1:{}", self.replica2_promote_port)
            .parse()
            .unwrap();
        promote(addr, &self.operator_key);
        wait_ready(self.replica2.health_addr, Duration::from_secs(30));
        Client::connect(self.replica2.client_addr, &self.key2)
            .expect("connect to promoted replica 2")
    }

    fn primary_trading(&self) -> bool {
        query_health(self.primary.health_addr)
            .map(|(_, _, _, t)| t)
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Dual replication tests
// ---------------------------------------------------------------------------

/// Kill one replica, verify trading continues. Kill primary, promote the
/// surviving replica, verify no data loss.
#[test]
#[serial]
fn dual_replication_survives_one_replica_failure() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    for i in 1..=20u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response");
    }
    cluster.wait_replicated();

    // Kill replica 1 — trading should continue.
    cluster.kill_replica1();
    wait_ready(cluster.primary.health_addr, Duration::from_secs(5));

    // Submit more orders with only replica 2 alive.
    for i in 21..=40u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(
            !r.is_empty(),
            "order {i}: no response after replica 1 death"
        );
    }
    cluster.wait_replicated();

    // Kill primary, promote replica 2.
    drop(client);
    cluster.kill_primary();
    let mut client2 = cluster.promote_replica2();

    // All 40 orders must be present.
    let r = submit_order(&mut client2, 41, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected Placed, got: {r:?}"
    );
}

/// Kill BOTH replicas — trading must halt. Verify orders are rejected
/// with ReplicaDisconnected.
#[test]
#[serial]
fn dual_replication_halts_when_both_disconnect() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    // Submit orders while both replicas are up.
    for i in 1..=10u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
    }
    cluster.wait_replicated();

    // Kill both replicas, then wait for the primary to register the loss
    // and flip to halted (no quorum of replicas → no acks).
    cluster.kill_replica1();
    cluster.kill_replica2();
    wait_halted(cluster.primary.health_addr, Duration::from_secs(5));

    // Trading should be halted.
    assert!(
        !cluster.primary_trading(),
        "should be halted with no replicas"
    );

    // Orders should be rejected.
    let r = submit_order(&mut client, 11, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::ReplicaDisconnected,
                ..
            }
        )),
        "expected ReplicaDisconnected, got: {r:?}"
    );
}

/// Kill one replica, submit orders, kill primary, promote the OTHER
/// replica (the one that was alive the whole time). Symmetric test —
/// proves either replica can be promoted.
#[test]
#[serial]
fn dual_replication_promote_replica1_after_replica2_dies() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    for i in 1..=15u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
    }
    cluster.wait_replicated();

    // Kill replica 2 this time (previous test killed replica 1).
    cluster.kill_replica2();
    wait_ready(cluster.primary.health_addr, Duration::from_secs(5));

    for i in 16..=30u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty());
    }
    cluster.wait_replicated();

    // Kill primary, promote replica 1.
    drop(client);
    cluster.kill_primary();
    let mut client2 = cluster.promote_replica1();

    let r = submit_order(&mut client2, 31, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected Placed on promoted replica 1, got: {r:?}"
    );
}

/// Active fills during dual replication — crossing orders generate fills,
/// then failover. Verifies the promoted replica's exchange state is
/// consistent (balances correct, can continue trading).
#[test]
#[serial]
fn dual_replication_with_fills_then_failover() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    // Resting sells from account 2.
    for i in 1..=10u64 {
        submit_order(&mut client, i, 2, 1, Side::Sell, 100 + i, 5);
    }
    // Aggressive buys from account 1 — generates fills.
    for i in 11..=20u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 200, 3);
    }
    cluster.wait_replicated();

    // Kill replica 1, submit more fills with only replica 2.
    cluster.kill_replica1();
    std::thread::sleep(Duration::from_millis(500));

    for i in 21..=25u64 {
        submit_order(&mut client, i, 2, 1, Side::Sell, 300, 2);
    }
    for i in 26..=30u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 300, 2);
    }
    cluster.wait_replicated();

    // Failover to replica 2.
    drop(client);
    cluster.kill_primary();
    let mut client2 = cluster.promote_replica2();

    // Place + fill on promoted replica — proves matching state is correct.
    let r = submit_order(&mut client2, 31, 2, 1, Side::Sell, 500, 1);
    let accepted = has_report(&r, |rep| {
        matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })
    }) || has_report(&r, |rep| {
        matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. })
    });
    assert!(accepted, "expected Placed or Fill, got: {r:?}");

    let r = submit_order(&mut client2, 32, 1, 1, Side::Buy, 500, 1);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Fill { .. }
        )),
        "expected Fill on promoted replica, got: {r:?}"
    );
}

/// Async ack mode: replicas are started with `--async-replica-ack`, which
/// makes them ack the primary as soon as a batch is queued for the local
/// journal stage rather than after fsync. End-to-end this should still
/// produce identical journals after a graceful shutdown (the journal
/// stage drains and fsyncs everything before the receiver exits), and
/// failover via promotion must still see every fill the client was told
/// about (the promotion path also drains the pipeline).
///
/// Mirrors `dual_replication_with_fills_then_failover` but exercises the
/// async path; if either path silently dropped data, the post-promotion
/// fill on the new primary would fail to find its counterparty.
#[test]
#[serial]
fn async_ack_dual_replication_with_failover() {
    let mut cluster = DualCluster::start_with_replica_args(&["--async-replica-ack"]);
    let mut client = cluster.connect_primary();

    // Resting sells from account 2.
    for i in 1..=10u64 {
        submit_order(&mut client, i, 2, 1, Side::Sell, 100 + i, 5);
    }
    // Aggressive buys from account 1 — generates fills.
    for i in 11..=20u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 200, 3);
    }
    cluster.wait_replicated();

    // Kill replica 1, submit more fills with only replica 2.
    cluster.kill_replica1();
    std::thread::sleep(Duration::from_millis(500));

    for i in 21..=25u64 {
        submit_order(&mut client, i, 2, 1, Side::Sell, 300, 2);
    }
    for i in 26..=30u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 300, 2);
    }
    cluster.wait_replicated();

    // Failover to replica 2.
    drop(client);
    cluster.kill_primary();
    let mut client2 = cluster.promote_replica2();

    // Place + fill on promoted replica — proves the matching state of the
    // promoted node has every event the original primary acknowledged,
    // including the ones from after replica1 died.
    let r = submit_order(&mut client2, 31, 2, 1, Side::Sell, 500, 1);
    let accepted = has_report(&r, |rep| {
        matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })
    }) || has_report(&r, |rep| {
        matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. })
    });
    assert!(accepted, "expected Placed or Fill, got: {r:?}");

    let r = submit_order(&mut client2, 32, 1, 1, Side::Buy, 500, 1);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Fill { .. }
        )),
        "expected Fill on promoted replica, got: {r:?}"
    );
}

/// Journal catch-up: kill a replica, submit more orders, copy the dead
/// replica's journal to a replacement, start the replacement. The primary
/// streams the gap (orders the replacement missed) via journal catch-up.
/// Kill primary, promote replacement, verify ALL orders are present.
#[test]
#[serial]
fn replacement_replica_catches_up_from_journal() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    // Phase 1: submit orders while both replicas are connected.
    for i in 1..=20u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response");
    }
    cluster.wait_replicated();

    // SIGKILL replica 1. Its journal may have gaps from interrupted writes.
    // Recovery tolerates this by truncating at the gap.
    let replica1_journal = cluster._tmp.path().join("replica1.journal");
    cluster.kill_replica1();
    std::thread::sleep(Duration::from_millis(500));

    // Phase 2: submit more orders that replica 1 misses.
    for i in 21..=40u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response");
    }
    cluster.wait_replicated();

    // Copy the dead replica's journal to a new path for the replacement.
    // The replacement will recover from this stale journal, connect to the
    // primary, and catch up the missed orders (21-40) via journal streaming.
    let replacement_journal = cluster._tmp.path().join("replacement.journal");
    std::fs::copy(&replica1_journal, &replacement_journal).expect("copy replica journal");
    assert!(
        replacement_journal.exists(),
        "replacement journal must exist after copy"
    );
    // Verify the copied journal exists and has meaningful size.
    // We don't call JournaledExchange::recover() here because the
    // replica may have been killed mid-write, leaving a truncated entry
    // that recovery would reject. The replacement replica's run_receiver
    // handles recovery gracefully (truncates and continues).
    let copy_len = std::fs::metadata(&replacement_journal)
        .expect("replacement journal metadata")
        .len();
    assert!(copy_len > 100, "replacement journal too small: {copy_len}");

    // Start replacement replica with the copied (stale) journal.
    let r3_client = free_port();
    let r3_health = free_port();
    let r3_promote = free_port();
    let bin = server_bin();
    let _replacement = {
        let child = Command::new(&bin)
            .args([
                "--bind",
                &format!("127.0.0.1:{r3_client}"),
                "--health-bind",
                &format!("127.0.0.1:{r3_health}"),
                "--replica-of",
                &format!("127.0.0.1:{}", cluster.primary_repl_port),
                "--replication-key",
                cluster.repl_key_path.to_str().unwrap(),
                "--promote-bind",
                &format!("127.0.0.1:{r3_promote}"),
                "--journal",
                replacement_journal.to_str().expect("valid path"),
                "--authorized-keys",
                cluster
                    ._tmp
                    .path()
                    .join("authorized_keys")
                    .to_str()
                    .expect("valid path"),
                "--connection-timeout-secs",
                "0",
                "--yield-idle",
                "--cores",
                "0,0,0,0,0,0,0,0",
                "--reader-cores",
                "0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
            .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
            .spawn()
            .expect("spawn replacement replica");
        ServerProcess {
            child,
            client_addr: format!("127.0.0.1:{r3_client}").parse().unwrap(),
            health_addr: format!("127.0.0.1:{r3_health}").parse().unwrap(),
        }
    };

    wait_for_replacement_catchup(cluster.primary.health_addr);
    eprintln!("Replacement replica caught up.");

    // Phase 3: submit orders after catch-up to verify live streaming works.
    for i in 41..=50u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response after catch-up");
    }
    cluster.wait_replicated();

    // Kill primary, promote the replacement replica.
    drop(client);
    cluster.kill_primary();

    let promote_addr: SocketAddr = format!("127.0.0.1:{r3_promote}").parse().unwrap();
    promote(promote_addr, &cluster.operator_key);
    let r3_health_addr: SocketAddr = format!("127.0.0.1:{r3_health}").parse().unwrap();
    wait_ready(r3_health_addr, Duration::from_secs(30));

    let mut client2 = Client::connect(
        format!("127.0.0.1:{r3_client}").parse().unwrap(),
        &cluster.key2,
    )
    .expect("connect to promoted replacement");

    // All 50 orders must be present — ID 51 succeeds, duplicate of 50 rejected.
    let r = submit_order(&mut client2, 51, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected Placed on promoted replacement, got: {r:?}"
    );

    let r = submit_order(&mut client2, 50, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "expected DuplicateOrderId for id=50, got: {r:?}"
    );

    eprintln!("PASS: replacement replica caught up from journal and has all 50 orders.");
}

/// Catch-up with fills during the gap. Replica misses crossing orders
/// that generate fills. After catch-up + promotion, verify balances are
/// correct (place + fill works on promoted replacement).
#[test]
#[serial]
fn catchup_with_fills_during_gap() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    // Place resting sells from account 2 while both replicas are up.
    for i in 1..=10u64 {
        submit_order(&mut client, i, 2, 1, Side::Sell, 100 + i, 5);
    }
    cluster.wait_replicated();

    // SIGKILL replica 1 and copy its journal (may have gaps — recovery tolerates).
    let replica1_journal = cluster._tmp.path().join("replica1.journal");
    cluster.kill_replica1();
    std::thread::sleep(Duration::from_millis(200));

    let replacement_journal = cluster._tmp.path().join("replacement_fills.journal");
    std::fs::copy(&replica1_journal, &replacement_journal).expect("copy journal");

    // Aggressive buys from account 1 during the gap — generates fills
    // that replica 1 misses.
    for i in 11..=20u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 200, 3);
    }
    cluster.wait_replicated();

    // Start replacement with the pre-kill journal snapshot.
    let r3_client = free_port();
    let r3_health = free_port();
    let r3_promote = free_port();
    let bin = server_bin();
    let _replacement = {
        let child = Command::new(&bin)
            .args([
                "--bind",
                &format!("127.0.0.1:{r3_client}"),
                "--health-bind",
                &format!("127.0.0.1:{r3_health}"),
                "--replica-of",
                &format!("127.0.0.1:{}", cluster.primary_repl_port),
                "--replication-key",
                cluster.repl_key_path.to_str().unwrap(),
                "--promote-bind",
                &format!("127.0.0.1:{r3_promote}"),
                "--journal",
                replacement_journal.to_str().unwrap(),
                "--authorized-keys",
                cluster
                    ._tmp
                    .path()
                    .join("authorized_keys")
                    .to_str()
                    .unwrap(),
                "--connection-timeout-secs",
                "0",
                "--yield-idle",
                "--cores",
                "0,0,0,0,0,0,0,0",
                "--reader-cores",
                "0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
            .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
            .spawn()
            .expect("spawn replacement");
        ServerProcess {
            child,
            client_addr: format!("127.0.0.1:{r3_client}").parse().unwrap(),
            health_addr: format!("127.0.0.1:{r3_health}").parse().unwrap(),
        }
    };

    // Wait for replacement to actually catch up. See
    // `wait_for_replacement_catchup` for why polling primary lag once is
    // insufficient (disconnected slot pinned to u64::MAX excludes it from
    // the min cursor).
    wait_for_replacement_catchup(cluster.primary.health_addr);

    // Kill primary, promote replacement.
    drop(client);
    cluster.kill_primary();
    promote(
        format!("127.0.0.1:{r3_promote}").parse().unwrap(),
        &cluster.operator_key,
    );
    wait_ready(
        format!("127.0.0.1:{r3_health}").parse().unwrap(),
        Duration::from_secs(30),
    );

    let mut client2 = Client::connect(
        format!("127.0.0.1:{r3_client}").parse().unwrap(),
        &cluster.key2,
    )
    .expect("connect to promoted replacement");

    // Place a sell + matching buy to verify balances are correct.
    let r = submit_order(&mut client2, 21, 2, 1, Side::Sell, 500, 1);
    let accepted = has_report(&r, |rep| {
        matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })
    }) || has_report(&r, |rep| {
        matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. })
    });
    assert!(accepted, "expected Placed or Fill, got: {r:?}");

    let r = submit_order(&mut client2, 22, 1, 1, Side::Buy, 500, 1);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Fill { .. }
        )),
        "expected Fill after catch-up with fills, got: {r:?}"
    );

    eprintln!("PASS: catch-up with fills — balances correct after promotion.");
}

/// Catch-up completes, kill primary immediately (no more orders after
/// catch-up). Promote replacement. Verifies catch-up data survives.
#[test]
#[serial]
fn catchup_then_immediate_failover() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    for i in 1..=15u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
    }
    cluster.wait_replicated();
    // SIGKILL replica 1 and copy its journal (may have gaps — recovery tolerates).
    let replica1_journal = cluster._tmp.path().join("replica1.journal");
    cluster.kill_replica1();
    std::thread::sleep(Duration::from_millis(200));

    let replacement_journal = cluster._tmp.path().join("replacement_imm.journal");
    std::fs::copy(&replica1_journal, &replacement_journal).expect("copy journal");

    // Submit orders that replica 1 misses.
    for i in 16..=30u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
    }
    cluster.wait_replicated();

    let r3_client = free_port();
    let r3_health = free_port();
    let r3_promote = free_port();
    let bin = server_bin();
    let _replacement = {
        let child = Command::new(&bin)
            .args([
                "--bind",
                &format!("127.0.0.1:{r3_client}"),
                "--health-bind",
                &format!("127.0.0.1:{r3_health}"),
                "--replica-of",
                &format!("127.0.0.1:{}", cluster.primary_repl_port),
                "--replication-key",
                cluster.repl_key_path.to_str().unwrap(),
                "--promote-bind",
                &format!("127.0.0.1:{r3_promote}"),
                "--journal",
                replacement_journal.to_str().unwrap(),
                "--authorized-keys",
                cluster
                    ._tmp
                    .path()
                    .join("authorized_keys")
                    .to_str()
                    .unwrap(),
                "--connection-timeout-secs",
                "0",
                "--yield-idle",
                "--cores",
                "0,0,0,0,0,0,0,0",
                "--reader-cores",
                "0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
            .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
            .spawn()
            .expect("spawn replacement");
        ServerProcess {
            child,
            client_addr: format!("127.0.0.1:{r3_client}").parse().unwrap(),
            health_addr: format!("127.0.0.1:{r3_health}").parse().unwrap(),
        }
    };

    // Wait for replacement_imm to actually catch up.
    //
    // Polling the primary's lag is insufficient on its own: replica1's slot
    // is pinned to u64::MAX after disconnect (excluded from the min cursor),
    // so the primary reports lag==0 from replica2 alone — even before
    // replacement_imm has connected. Wait for lag to first transition to
    // nonzero (replacement_imm connected and behind), then back to zero
    // (caught up). The replica doesn't spawn a health endpoint of its own,
    // so primary's view is the only signal available.
    wait_for_replacement_catchup(cluster.primary.health_addr);

    // Kill primary IMMEDIATELY — no more orders after catch-up.
    drop(client);
    cluster.kill_primary();
    promote(
        format!("127.0.0.1:{r3_promote}").parse().unwrap(),
        &cluster.operator_key,
    );
    wait_ready(
        format!("127.0.0.1:{r3_health}").parse().unwrap(),
        Duration::from_secs(30),
    );

    let mut client2 = Client::connect(
        format!("127.0.0.1:{r3_client}").parse().unwrap(),
        &cluster.key2,
    )
    .expect("connect to promoted replacement");

    // All 30 orders must be present.
    let r = submit_order(&mut client2, 31, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected Placed, got: {r:?}"
    );

    // Duplicate of last order rejected.
    let r = submit_order(&mut client2, 30, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "expected DuplicateOrderId, got: {r:?}"
    );

    eprintln!("PASS: catch-up then immediate failover — all 30 orders survived.");
}

/// Fresh replica with NO journal copy joins a running primary. The primary
/// streams the entire journal history via catch-up. Kill primary, promote
/// the fresh replacement, verify all orders are present.
#[test]
#[serial]
fn fresh_replica_full_catchup() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    // Submit orders while both initial replicas are connected.
    for i in 1..=25u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response");
    }
    cluster.wait_replicated();

    // Kill replica 1 to free a slot.
    cluster.kill_replica1();
    std::thread::sleep(Duration::from_millis(500));

    // Start a FRESH replacement with no journal at all.
    // The primary will stream the entire journal history via catch-up.
    let fresh_journal = cluster._tmp.path().join("fresh_replacement.journal");
    let r3_client = free_port();
    let r3_health = free_port();
    let r3_promote = free_port();
    let bin = server_bin();
    let _replacement = {
        let child = Command::new(&bin)
            .args([
                "--bind",
                &format!("127.0.0.1:{r3_client}"),
                "--health-bind",
                &format!("127.0.0.1:{r3_health}"),
                "--replica-of",
                &format!("127.0.0.1:{}", cluster.primary_repl_port),
                "--replication-key",
                cluster.repl_key_path.to_str().unwrap(),
                "--promote-bind",
                &format!("127.0.0.1:{r3_promote}"),
                "--journal",
                fresh_journal.to_str().unwrap(),
                "--authorized-keys",
                cluster
                    ._tmp
                    .path()
                    .join("authorized_keys")
                    .to_str()
                    .unwrap(),
                "--connection-timeout-secs",
                "0",
                "--yield-idle",
                "--cores",
                "0,0,0,0,0,0,0,0",
                "--reader-cores",
                "0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
            .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
            .spawn()
            .expect("spawn fresh replacement");
        ServerProcess {
            child,
            client_addr: format!("127.0.0.1:{r3_client}").parse().unwrap(),
            health_addr: format!("127.0.0.1:{r3_health}").parse().unwrap(),
        }
    };

    wait_for_replacement_catchup(cluster.primary.health_addr);
    eprintln!("Fresh replica caught up.");

    // Submit more orders after catch-up (proves live streaming works).
    for i in 26..=35u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response after catch-up");
    }
    cluster.wait_replicated();

    // Kill primary, promote the fresh replacement.
    drop(client);
    cluster.kill_primary();
    promote(
        format!("127.0.0.1:{r3_promote}").parse().unwrap(),
        &cluster.operator_key,
    );
    wait_ready(
        format!("127.0.0.1:{r3_health}").parse().unwrap(),
        Duration::from_secs(30),
    );

    let mut client2 = Client::connect(
        format!("127.0.0.1:{r3_client}").parse().unwrap(),
        &cluster.key2,
    )
    .expect("connect to promoted fresh replacement");

    // All 35 orders must be present.
    let r = submit_order(&mut client2, 36, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected Placed on promoted fresh replacement, got: {r:?}"
    );

    let r = submit_order(&mut client2, 35, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "expected DuplicateOrderId for id=35, got: {r:?}"
    );

    eprintln!("PASS: fresh replica caught up from primary's journal — all 35 orders present.");
}

/// Snapshot transfer: primary's journal archives are deleted while a snapshot
/// exists. A new replica connects — the primary detects journals are too old,
/// transfers the snapshot, then catches up from the current journal. The
/// replica ends up with all orders.
#[test]
#[serial]
fn snapshot_transfer_when_archives_purged() {
    let bin = server_bin();
    let tmp = tempfile::tempdir().unwrap();

    // Deterministic keys (same pattern as TestCluster::start).
    let key = SigningKey::from_bytes(&[0xFA; 32]);
    let key2 = SigningKey::from_bytes(&[0xFB; 32]);
    let operator_key = SigningKey::from_bytes(&[0xFD; 32]);
    let repl_key = SigningKey::from_bytes(&[0xFC; 32]);
    let (keys_path, repl_key_path) =
        write_auth_keys_multi(tmp.path(), &[&key, &key2], &operator_key, &repl_key);

    let primary_client_port = free_port();
    let primary_health_port = free_port();

    // Start primary with --snapshot-interval-ms 100 to trigger periodic
    // shadow snapshots, so a .snapshot file exists for transfer.
    let primary_journal = tmp.path().join("primary.journal");
    let mut primary = {
        let child = Command::new(&bin)
            .args([
                "--bind",
                &format!("127.0.0.1:{primary_client_port}"),
                "--health-bind",
                &format!("127.0.0.1:{primary_health_port}"),
                "--journal",
                primary_journal.to_str().unwrap(),
                "--authorized-keys",
                keys_path.to_str().unwrap(),
                "--accounts",
                "10",
                "--instruments",
                "2",
                "--connection-timeout-secs",
                "0",
                "--yield-idle",
                "--cores",
                "0,0,0,0,0,0,0,0",
                "--reader-cores",
                "0",
                "--standalone",
                "--snapshot-interval-ms",
                "100",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
            .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
            .spawn()
            .expect("spawn primary");
        ServerProcess {
            child,
            client_addr: format!("127.0.0.1:{primary_client_port}").parse().unwrap(),
            health_addr: format!("127.0.0.1:{primary_health_port}").parse().unwrap(),
        }
    };

    wait_healthy(primary.health_addr, Duration::from_secs(30));

    // Connect and send orders.
    let mut client = Client::connect(primary.client_addr, &key).expect("connect");
    for i in 1..=20u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response");
    }
    drop(client);

    // All 20 orders are now committed (each submit_order waited for a
    // response gated on journal fsync). Remove any snapshot taken before
    // this point — in yield-idle mode the timer fires promptly, so a
    // partial snapshot (e.g. only orders 1–N) may already exist. The
    // next snapshot is guaranteed to include all 20 orders.
    let snap_path = primary_journal.with_extension("snapshot");
    let _ = std::fs::remove_file(&snap_path);

    // Wait for a fresh snapshot that captures the full committed state.
    let start = Instant::now();
    while !snap_path.exists() {
        if start.elapsed() > Duration::from_secs(60) {
            panic!(
                "snapshot was not created within 60s at {}",
                snap_path.display()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    eprintln!("Snapshot created at {}", snap_path.display());

    // Stop the standalone primary. `wait()` already blocks until the
    // process exits and its files are flushed by the kernel — no extra
    // sleep needed.
    unsafe { libc::kill(primary.child.id() as i32, libc::SIGINT) };
    let _ = primary.child.wait();

    // Delete journal archive files (simulate archive purge).
    // Keep the current journal and snapshot, delete .1, .2, etc.
    for i in 1..=10 {
        let archive = tmp.path().join(format!("primary.journal.{i}"));
        if archive.exists() {
            std::fs::remove_file(&archive).unwrap();
            eprintln!("Deleted archive: {}", archive.display());
        }
    }

    // Also delete the main journal to force the primary to recover from
    // snapshot only. Then re-creating the journal means the replica's
    // last_sequence=0 will predate the current journal's start sequence.
    std::fs::remove_file(&primary_journal).ok();
    eprintln!("Deleted main journal to force snapshot-only recovery");

    // Restart primary with replication enabled (not standalone).
    let primary_repl_port2 = free_port();
    let primary_client_port2 = free_port();
    let primary_health_port2 = free_port();
    let mut primary2 = {
        let child = Command::new(&bin)
            .args([
                "--bind",
                &format!("127.0.0.1:{primary_client_port2}"),
                "--health-bind",
                &format!("127.0.0.1:{primary_health_port2}"),
                "--replication-bind",
                &format!("127.0.0.1:{primary_repl_port2}"),
                "--journal",
                primary_journal.to_str().unwrap(),
                "--authorized-keys",
                keys_path.to_str().unwrap(),
                "--accounts",
                "10",
                "--instruments",
                "2",
                "--connection-timeout-secs",
                "0",
                "--yield-idle",
                "--cores",
                "0,0,0,0,0,0,0,0",
                "--reader-cores",
                "0",
                "--snapshot-interval-ms",
                "100",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
            .env("MELIN_JOURNAL_CHECKPOINT_INTERVAL", "100")
            .spawn()
            .expect("spawn primary2");
        ServerProcess {
            child,
            client_addr: format!("127.0.0.1:{primary_client_port2}").parse().unwrap(),
            health_addr: format!("127.0.0.1:{primary_health_port2}").parse().unwrap(),
        }
    };

    // Wait for the primary to be ready to accept replica connections.
    wait_for_primary_repl_ready(primary2.health_addr, Duration::from_secs(10));

    // Start a fresh replica. It has last_sequence=0, but the primary
    // recovered from snapshot (journal starts after snapshot sequence).
    // Catch-up will fail → snapshot transfer kicks in.
    let replica_client_port = free_port();
    let replica_health_port = free_port();
    let replica_promote_port = free_port();
    let _replica = spawn_replica(
        &bin,
        tmp.path(),
        &keys_path,
        &repl_key_path,
        primary_repl_port2,
        replica_client_port,
        replica_health_port,
        replica_promote_port,
    );

    // Wait for the primary to become healthy (seeding done, replica connected).
    wait_healthy(primary2.health_addr, Duration::from_secs(30));
    eprintln!("Primary healthy with replica connected");

    wait_for_replacement_catchup(primary2.health_addr);
    eprintln!("Replica caught up via snapshot transfer.");

    // Submit a new order to verify the primary is functional.
    let mut client2 = Client::connect(primary2.client_addr, &key2).expect("connect to primary2");
    let r = submit_order(&mut client2, 21, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )) || has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Fill { .. }
        )),
        "expected Placed or Fill after snapshot transfer, got: {r:?}"
    );

    // Verify dedup: replay order 20 (from before snapshot) must be rejected.
    let r = submit_order(&mut client2, 20, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "expected DuplicateOrderId for id=20 after snapshot transfer, got: {r:?}"
    );

    // Cleanup.
    drop(client2);
    unsafe { libc::kill(primary2.child.id() as i32, libc::SIGINT) };
    let _ = primary2.child.wait();

    eprintln!("PASS: snapshot transfer — replica caught up after archive purge.");
}
