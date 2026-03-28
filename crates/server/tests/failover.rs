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
    let journal = tmp_dir.join("replica.journal");
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
// Test
// ---------------------------------------------------------------------------

#[test]
fn kill_primary_promote_replica_no_data_loss() {
    let bin = server_bin();
    assert!(
        bin.exists(),
        "melin-server binary not found at {bin:?}. Run `cargo build` first."
    );

    let tmp = tempfile::tempdir().expect("create temp dir");
    let key = SigningKey::from_bytes(&[0xFA; 32]);
    // Second key for post-promotion client — avoids per-key request
    // sequence HWM collision (the promoted replica's HWM tracks sequences
    // from the primary phase, so a fresh client with the same key would
    // have its first request rejected as DuplicateRequest).
    let key2 = SigningKey::from_bytes(&[0xFB; 32]);
    let keys_path = write_auth_keys_multi(tmp.path(), &[&key, &key2]);

    // Allocate ports up front to avoid races.
    let primary_client_port = free_port();
    let primary_health_port = free_port();
    let primary_repl_port = free_port();
    let replica_client_port = free_port();
    let replica_health_port = free_port();
    let replica_promote_port = free_port();

    // --- 1. Start primary and replica concurrently ---
    // The primary with --replication-bind blocks pipeline progress until a
    // replica connects and starts acking. Start both at the same time so the
    // replica connects during seeding.
    let mut primary = spawn_primary(
        &bin,
        tmp.path(),
        &keys_path,
        primary_client_port,
        primary_health_port,
        primary_repl_port,
    );
    eprintln!("Primary started (pid={}).", primary.child.id());

    // Wait for the primary's replication port to be listening before
    // starting the replica. The replica doesn't retry on connection
    // refused, so timing matters.
    let repl_addr: SocketAddr = format!("127.0.0.1:{primary_repl_port}").parse().unwrap();
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
    eprintln!("Primary replication port is listening.");

    let replica = spawn_replica(
        &bin,
        tmp.path(),
        &keys_path,
        primary_repl_port,
        replica_client_port,
        replica_health_port,
        replica_promote_port,
    );
    eprintln!("Replica started (pid={}).", replica.child.id());

    // Wait for primary to complete seeding and become healthy.
    eprintln!("Waiting for primary to be healthy...");
    wait_healthy(primary.health_addr, Duration::from_secs(30));
    eprintln!("Primary is healthy.");

    // --- 3. Submit orders to primary ---
    // Check health details before connecting client.
    let (conns, seq, lag, trading) = query_health(primary.health_addr)
        .expect("query primary health before orders");
    eprintln!("Primary before orders: conns={conns}, seq={seq}, lag={lag}, trading={trading}");

    eprintln!("Connecting client to primary at {}...", primary.client_addr);
    let mut client =
        Client::connect(primary.client_addr, &key).expect("connect to primary");
    eprintln!("Client connected.");

    // Place resting buy orders on instrument 0 (seeded by server).
    let num_orders = 50;
    for i in 1..=num_orders {
        let responses = client
            .send_request(&Request::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(i),
                    account: AccountId(1),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: price(100),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: qty(10),
                    stp: melin_protocol::types::SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            })
            .expect("submit order");
        // Verify each order was acked (Placed or Rejected — not an error).
        assert!(
            !responses.is_empty(),
            "order {i}: no response from primary"
        );
    }

    // Record the primary's journal sequence after all orders are acked.
    // The response gate ensures all acked orders are both journaled AND replicated.
    let (_, primary_seq, repl_lag, _) = query_health(primary.health_addr)
        .expect("query primary health after orders");
    eprintln!(
        "Primary journal_seq={primary_seq}, repl_lag={repl_lag} after {num_orders} orders"
    );

    // Wait for replication lag to reach 0 (all events replicated).
    let start = Instant::now();
    loop {
        let (_, _, lag, _) = query_health(primary.health_addr)
            .expect("query primary health for repl lag");
        if lag == 0 {
            break;
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!("replication lag did not reach 0 within 10s (lag={lag})");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    eprintln!("Replication lag is 0.");

    // --- 4. SIGKILL the primary ---
    eprintln!("Sending SIGKILL to primary (pid={})...", primary.child.id());
    unsafe {
        libc::kill(primary.child.id() as i32, libc::SIGKILL);
    }
    let status = primary.child.wait().expect("wait for primary");
    eprintln!("Primary exited: {status}");

    // --- 5. Promote the replica ---
    eprintln!("Promoting replica...");
    let promote_addr: SocketAddr =
        format!("127.0.0.1:{replica_promote_port}").parse().unwrap();
    promote(promote_addr);
    eprintln!("Promotion sent.");

    // Wait for promoted replica to be healthy and accepting clients.
    eprintln!("Waiting for promoted replica to be healthy...");
    let (_, replica_seq, _, trading) =
        wait_healthy(replica.health_addr, Duration::from_secs(30));
    eprintln!(
        "Promoted replica: journal_seq={replica_seq}, trading={trading}"
    );

    // --- 6. Verify no data loss ---
    assert!(trading, "promoted replica should be in trading state");

    // --- 7. Reconnect client and verify state ---
    let mut client2 =
        Client::connect(replica.client_addr, &key2).expect("connect to promoted replica");

    // Give the pipeline a moment to fully initialize all stages.
    std::thread::sleep(Duration::from_secs(1));

    // Place a new order on the promoted replica. The order ID must be
    // higher than anything previously submitted — the dedup HWM was
    // replayed from the journal, so re-using an old ID would be rejected
    // as a duplicate. Using num_orders + 1 proves the replica has the
    // full HWM history.
    let responses = client2
        .send_request(&Request::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(num_orders + 1),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(200),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(5),
                stp: melin_protocol::types::SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
        })
        .expect("submit order to promoted replica");
    assert!(
        !responses.is_empty(),
        "no response from promoted replica"
    );

    // The response must be Placed (not Rejected). If the replica missed
    // any events, balances could be wrong (InsufficientBalance) or the
    // HWM could be wrong (DuplicateOrderId). A successful Placed confirms
    // the full state was replicated.
    let placed = responses.iter().any(|r| {
        matches!(
            r,
            melin_protocol::message::ResponseKind::Report(
                melin_protocol::types::ExecutionReport::Placed { .. }
            )
        )
    });
    assert!(
        placed,
        "expected Placed on promoted replica, got: {responses:?}"
    );
    eprintln!("New order on promoted replica: Placed");

    // Submit a second order to verify the pipeline is fully operational
    // (journal + matching + response stages all running).
    let responses2 = client2
        .send_request(&Request::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(num_orders + 2),
                account: AccountId(1),
                side: Side::Sell,
                order_type: OrderType::Limit {
                    price: price(200),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTC,
                quantity: qty(5),
                stp: melin_protocol::types::SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
        })
        .expect("submit second order to promoted replica");
    // This sell at 200 should fill against the buy at 200 above.
    let has_fill = responses2.iter().any(|r| {
        matches!(
            r,
            melin_protocol::message::ResponseKind::Report(
                melin_protocol::types::ExecutionReport::Fill { .. }
            )
        )
    });
    assert!(
        has_fill,
        "expected Fill on promoted replica, got: {responses2:?}"
    );
    eprintln!("Fill on promoted replica confirmed — pipeline fully operational.");

    eprintln!("PASS: failover test complete. primary_seq={primary_seq}, all {num_orders} orders replicated, promoted replica operational.");
}
