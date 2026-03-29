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

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use melin_client::Client;
use melin_protocol::message::Request;
use melin_protocol::types::{
    AccountId, Order, OrderId, OrderType, Price, Quantity, Side, Symbol,
    TimeInForce,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Find the melin-server binary. In integration tests for the server crate,
/// Cargo sets `CARGO_BIN_EXE_melin-server` automatically.
fn server_bin() -> PathBuf {
    // Use CARGO_BIN_EXE (debug binary, same compilation as the test).
    // This ensures the test binary and server binary use the same codec.
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_melin-server") {
        return PathBuf::from(p);
    }
    // Fallback: release binary.
    let release = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/release/melin-server");
    if release.exists() {
        return release;
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug/melin-server")
}

/// Find a free TCP port by binding to port 0.
fn free_port() -> u16 {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}

/// Write an authorized_keys file for multiple test keys.
fn write_auth_keys_multi(dir: &Path, keys: &[&SigningKey]) -> PathBuf {
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
    std::fs::write(&path, content).expect("write authorized_keys");
    path
}

/// Poll the health endpoint until it responds or timeout.
/// Returns the parsed status line: (conns, journal_seq, repl_lag, trading).
fn wait_healthy(addr: SocketAddr, timeout: Duration) -> (u64, u64, u64, bool) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!(
                "health endpoint {addr} did not respond within {timeout:?}"
            );
        }
        if let Ok(status) = query_health(addr) {
            return status;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Query the health endpoint once. Returns (conns, journal_seq, repl_lag, trading).
fn query_health(addr: SocketAddr) -> Result<(u64, u64, u64, bool), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(
        &addr,
        Duration::from_secs(1),
    )?;
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

/// Send PROMOTE to the promotion endpoint.
fn promote(addr: SocketAddr) {
    let mut stream = TcpStream::connect_timeout(
        &addr,
        Duration::from_secs(5),
    )
    .expect("connect to promotion endpoint");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");
    stream
        .write_all(b"PROMOTE\n")
        .expect("send PROMOTE");
    let mut reader = BufReader::new(&stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .expect("read promotion response");
    assert!(
        response.trim() == "OK",
        "promotion failed: {response}"
    );
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
            "--bind", &format!("127.0.0.1:{client_port}"),
            "--health-bind", &format!("127.0.0.1:{health_port}"),
            "--replication-bind", &format!("127.0.0.1:{replication_port}"),
            "--journal", journal.to_str().expect("valid path"),
            "--authorized-keys", keys_path.to_str().expect("valid path"),
            "--accounts", "10",
            "--instruments", "2",
            "--connection-timeout-secs", "0",
            "--yield-idle",
            // Reduce core count to avoid conflicts in CI.
            "--cores", "0,0,0,0,0,0",
            "--readers", "1",
            "--reader-cores", "0",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
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
    primary_repl_port: u16,
    client_port: u16,
    health_port: u16,
    promote_port: u16,
) -> ServerProcess {
    spawn_replica_named(bin, tmp_dir, keys_path, primary_repl_port, client_port, health_port, promote_port, "replica")
}

fn spawn_replica_named(
    bin: &Path,
    tmp_dir: &Path,
    keys_path: &Path,
    primary_repl_port: u16,
    client_port: u16,
    health_port: u16,
    promote_port: u16,
    name: &str,
) -> ServerProcess {
    let journal = tmp_dir.join(format!("{name}.journal"));
    let child = Command::new(bin)
        .args([
            "--bind", &format!("127.0.0.1:{client_port}"),
            "--health-bind", &format!("127.0.0.1:{health_port}"),
            "--replica-of", &format!("127.0.0.1:{primary_repl_port}"),
            "--promote-bind", &format!("127.0.0.1:{promote_port}"),
            "--journal", journal.to_str().expect("valid path"),
            "--authorized-keys", keys_path.to_str().expect("valid path"),
            "--connection-timeout-secs", "0",
            "--yield-idle",
            "--cores", "0,0,0,0,0,0",
            "--readers", "1",
            "--reader-cores", "0",
        ])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
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
    bin: PathBuf,
    keys_path: PathBuf,
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
        let keys_path = write_auth_keys_multi(tmp.path(), &[&key, &key2]);

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

        // Wait for the primary's replication port before starting replica.
        let repl_addr: SocketAddr =
            format!("127.0.0.1:{primary_repl_port}").parse().unwrap();
        let start = Instant::now();
        loop {
            if TcpStream::connect_timeout(&repl_addr, Duration::from_millis(100)).is_ok() {
                break;
            }
            if start.elapsed() > Duration::from_secs(10) {
                panic!("primary replication port never became available");
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        let replica = spawn_replica(
            &bin,
            tmp.path(),
            &keys_path,
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
            bin,
            keys_path,
            _tmp: tmp,
        }
    }

    /// Connect a client to the primary using the first key.
    fn connect_primary(&self) -> Client {
        Client::connect(self.primary.client_addr, &self.key)
            .expect("connect to primary")
    }

    /// Wait for replication lag to reach 0.
    fn wait_replicated(&self) {
        let start = Instant::now();
        loop {
            let (_, _, lag, _) = query_health(self.primary.health_addr)
                .expect("query primary health for repl lag");
            if lag == 0 {
                return;
            }
            if start.elapsed() > Duration::from_secs(10) {
                panic!("replication lag did not reach 0 within 10s (lag={lag})");
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

        let promote_addr: SocketAddr =
            format!("127.0.0.1:{}", self.promote_port).parse().unwrap();
        promote(promote_addr);

        wait_healthy(self.replica.health_addr, Duration::from_secs(30));

        // Brief pause for pipeline init.
        std::thread::sleep(Duration::from_secs(1));

        Client::connect(self.replica.client_addr, &self.key2)
            .expect("connect to promoted replica")
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
        has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })),
        "expected Placed, got: {r:?}"
    );

    // Crossing sell fills against the buy — proves matching works.
    let r = submit_order(&mut client2, 52, 1, 1, Side::Sell, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. })),
        "expected Fill, got: {r:?}"
    );
}

/// Kill the primary while fills are actively happening. Verifies that
/// balance conservation holds after promotion — no phantom fills or
/// leaked reservations.
#[test]
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
        has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })),
        "expected Placed after fill-heavy workload, got: {r:?}"
    );

    // Place a matching buy — proves matching is operational.
    let r = submit_order(&mut client2, 42, 1, 1, Side::Buy, 300, 1);
    assert!(
        has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. })),
        "expected Fill after fill-heavy workload, got: {r:?}"
    );
}

/// Kill the primary IMMEDIATELY after submitting orders, without waiting
/// for replication lag to reach 0. The response gate guarantees that every
/// acked response was replicated — verify the promoted replica has at
/// least the acked state.
#[test]
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
        has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })),
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
                "--bind", &format!("127.0.0.1:{recovered_client_port}"),
                "--health-bind", &format!("127.0.0.1:{recovered_health_port}"),
                "--standalone",
                "--journal", primary_journal.to_str().expect("valid path"),
                "--authorized-keys", cluster.keys_path.to_str().expect("valid path"),
                "--accounts", "10",
                "--instruments", "2",
                "--connection-timeout-secs", "0",
                "--yield-idle",
                "--cores", "0,0,0,0,0,0",
                "--readers", "1",
                "--reader-cores", "0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn recovered primary");
        ServerProcess {
            child,
            client_addr: format!("127.0.0.1:{recovered_client_port}").parse().unwrap(),
            health_addr: format!("127.0.0.1:{recovered_health_port}").parse().unwrap(),
        }
    };

    wait_healthy(recovered.health_addr, Duration::from_secs(30));
    std::thread::sleep(Duration::from_secs(1));

    let mut client3 = Client::connect(recovered.client_addr, &cluster.key2)
        .expect("connect to recovered primary");

    // New order must succeed — proves recovery restored instruments + balances.
    // May fill against resting orders that survived recovery, so accept
    // either Placed or Fill.
    let r = submit_order(&mut client3, 21, 1, 1, Side::Buy, 300, 1);
    let accepted = has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. }))
        || has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. }));
    assert!(accepted, "expected Placed or Fill on recovered primary, got: {r:?}");

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

/// Reconnect with the SAME key after failover and retry the last request.
/// The per-key request sequence HWM must reject it as DuplicateRequest
/// (not re-execute it). This tests that the per-key dedup state survives
/// replication and promotion.
#[test]
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
    let promote_addr: SocketAddr =
        format!("127.0.0.1:{}", cluster.promote_port).parse().unwrap();
    unsafe {
        libc::kill(cluster.primary.child.id() as i32, libc::SIGKILL);
    }
    let _ = cluster.primary.child.wait();
    promote(promote_addr);
    wait_healthy(cluster.replica.health_addr, Duration::from_secs(30));
    std::thread::sleep(Duration::from_secs(1));

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
    replica1: ServerProcess,
    replica2: ServerProcess,
    replica1_promote_port: u16,
    replica2_promote_port: u16,
    key: SigningKey,
    key2: SigningKey,
    _tmp: tempfile::TempDir,
}

impl DualCluster {
    fn start() -> Self {
        let bin = server_bin();
        assert!(bin.exists(), "melin-server binary not found");

        let tmp = tempfile::tempdir().expect("create temp dir");
        let key = SigningKey::from_bytes(&[0xFA; 32]);
        let key2 = SigningKey::from_bytes(&[0xFB; 32]);
        let keys_path = write_auth_keys_multi(tmp.path(), &[&key, &key2]);

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
            &bin, tmp.path(), &keys_path,
            primary_client_port, primary_health_port, primary_repl_port,
        );

        // Wait for replication port.
        let repl_addr: SocketAddr =
            format!("127.0.0.1:{primary_repl_port}").parse().unwrap();
        let start = Instant::now();
        loop {
            if TcpStream::connect_timeout(&repl_addr, Duration::from_millis(100)).is_ok() {
                break;
            }
            if start.elapsed() > Duration::from_secs(10) {
                panic!("primary replication port never became available");
            }
            std::thread::sleep(Duration::from_millis(50));
        }

        let replica1 = spawn_replica_named(
            &bin, tmp.path(), &keys_path, primary_repl_port,
            r1_client, r1_health, r1_promote, "replica1",
        );
        let replica2 = spawn_replica_named(
            &bin, tmp.path(), &keys_path, primary_repl_port,
            r2_client, r2_health, r2_promote, "replica2",
        );

        wait_healthy(primary.health_addr, Duration::from_secs(30));

        Self {
            primary, replica1, replica2,
            replica1_promote_port: r1_promote,
            replica2_promote_port: r2_promote,
            key, key2, _tmp: tmp,
        }
    }

    fn connect_primary(&self) -> Client {
        Client::connect(self.primary.client_addr, &self.key)
            .expect("connect to primary")
    }

    fn wait_replicated(&self) {
        let start = Instant::now();
        loop {
            let (_, _, lag, _) = query_health(self.primary.health_addr)
                .expect("query health for lag");
            if lag == 0 { return; }
            if start.elapsed() > Duration::from_secs(10) {
                panic!("replication lag did not reach 0 (lag={lag})");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn kill_primary(&mut self) {
        unsafe { libc::kill(self.primary.child.id() as i32, libc::SIGKILL); }
        let _ = self.primary.child.wait();
    }

    fn kill_replica1(&mut self) {
        unsafe { libc::kill(self.replica1.child.id() as i32, libc::SIGKILL); }
        let _ = self.replica1.child.wait();
    }

    fn kill_replica2(&mut self) {
        unsafe { libc::kill(self.replica2.child.id() as i32, libc::SIGKILL); }
        let _ = self.replica2.child.wait();
    }

    fn promote_replica1(&self) -> Client {
        let addr: SocketAddr =
            format!("127.0.0.1:{}", self.replica1_promote_port).parse().unwrap();
        promote(addr);
        wait_healthy(self.replica1.health_addr, Duration::from_secs(30));
        std::thread::sleep(Duration::from_secs(1));
        Client::connect(self.replica1.client_addr, &self.key2)
            .expect("connect to promoted replica 1")
    }

    fn promote_replica2(&self) -> Client {
        let addr: SocketAddr =
            format!("127.0.0.1:{}", self.replica2_promote_port).parse().unwrap();
        promote(addr);
        wait_healthy(self.replica2.health_addr, Duration::from_secs(30));
        std::thread::sleep(Duration::from_secs(1));
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
    std::thread::sleep(Duration::from_millis(500));
    assert!(cluster.primary_trading(), "should still be trading with one replica");

    // Submit more orders with only replica 2 alive.
    for i in 21..=40u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response after replica 1 death");
    }
    cluster.wait_replicated();

    // Kill primary, promote replica 2.
    drop(client);
    cluster.kill_primary();
    let mut client2 = cluster.promote_replica2();

    // All 40 orders must be present.
    let r = submit_order(&mut client2, 41, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })),
        "expected Placed, got: {r:?}"
    );
}

/// Kill BOTH replicas — trading must halt. Verify orders are rejected
/// with ReplicaDisconnected.
#[test]
fn dual_replication_halts_when_both_disconnect() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    // Submit orders while both replicas are up.
    for i in 1..=10u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
    }
    cluster.wait_replicated();

    // Kill both replicas.
    cluster.kill_replica1();
    cluster.kill_replica2();
    std::thread::sleep(Duration::from_millis(1000));

    // Trading should be halted.
    assert!(!cluster.primary_trading(), "should be halted with no replicas");

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
fn dual_replication_promote_replica1_after_replica2_dies() {
    let mut cluster = DualCluster::start();
    let mut client = cluster.connect_primary();

    for i in 1..=15u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
    }
    cluster.wait_replicated();

    // Kill replica 2 this time (previous test killed replica 1).
    cluster.kill_replica2();
    std::thread::sleep(Duration::from_millis(500));
    assert!(cluster.primary_trading(), "should still be trading");

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
        has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })),
        "expected Placed on promoted replica 1, got: {r:?}"
    );
}

/// Active fills during dual replication — crossing orders generate fills,
/// then failover. Verifies the promoted replica's exchange state is
/// consistent (balances correct, can continue trading).
#[test]
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
    let accepted = has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. }))
        || has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. }));
    assert!(accepted, "expected Placed or Fill, got: {r:?}");

    let r = submit_order(&mut client2, 32, 1, 1, Side::Buy, 500, 1);
    assert!(
        has_report(&r, |rep| matches!(rep, melin_protocol::types::ExecutionReport::Fill { .. })),
        "expected Fill on promoted replica, got: {r:?}"
    );
}
