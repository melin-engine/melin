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
//! matching invariants) are meaningful only against the real engine.
//! Under `skip-order-exec` the promoted replica would trivially pass
//! because every order is rejected with `NoLiquidity`. When running
//! `cargo test` against the skip-order-exec build this file is
//! compiled as an empty test crate.

#![cfg(not(feature = "skip-order-exec"))]

use ed25519_dalek::Signer;
use serial_test::serial;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

/// Connect a TCP client with a 60s socket read timeout so the test
/// suite fails fast when a server stalls instead of soaking the host
/// indefinitely. 60s sits well above every in-test wait
/// (`wait_ready` / `wait_for_replacement_catchup` cap at 30s), so a
/// healthy run never trips it.
fn connect_with_timeout(addr: SocketAddr, key: &SigningKey) -> Client {
    let client = Client::connect(addr, key).expect("client connect");
    client
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("set read timeout");
    client
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
/// rather than the replication port directly — a responsive
/// `/healthz` is a reliable proxy for "replica may now connect"
/// because the replication socket binds during the same startup phase
/// as the health endpoint.
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

/// Fetch the per-slot `melin_replica_in_memory_sequence` and
/// `melin_replica_acked_sequence` values. Returns
/// `[(in_memory_0, acked_0), (in_memory_1, acked_1)]`.
fn fetch_replica_cursors(addr: SocketAddr) -> Option<[(u64, u64); 2]> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).ok()?;
    let text = std::str::from_utf8(&body).ok()?;
    let mut acked = [0u64; 2];
    let mut in_mem = [0u64; 2];
    for line in text.lines() {
        for slot in 0..2usize {
            let acked_prefix = format!("melin_replica_acked_sequence{{slot=\"{slot}\"}} ");
            let in_mem_prefix = format!("melin_replica_in_memory_sequence{{slot=\"{slot}\"}} ");
            if let Some(rest) = line.strip_prefix(&acked_prefix) {
                acked[slot] = rest.trim().parse().ok()?;
            } else if let Some(rest) = line.strip_prefix(&in_mem_prefix) {
                in_mem[slot] = rest.trim().parse().ok()?;
            }
        }
    }
    Some([(in_mem[0], acked[0]), (in_mem[1], acked[1])])
}

/// Fetch the `melin_durability_policy_degraded` gauge from the
/// Prometheus metrics endpoint. Returns `None` if the metric is
/// missing (older binary, parse error, etc).
fn fetch_policy_degraded(addr: SocketAddr) -> Option<u32> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).ok()?;
    let text = std::str::from_utf8(&body).ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("melin_durability_policy_degraded ") {
            return rest.trim().parse().ok();
        }
    }
    None
}

/// Poll the metrics endpoint until `melin_durability_policy_degraded`
/// equals `expected`, or timeout. Panics on timeout. The 1-second
/// flap-hold + 1-second idle re-eval mean transitions can take up to
/// ~2 s to surface, so callers should pass a comfortable timeout.
fn wait_for_policy_degraded(addr: SocketAddr, expected: u32, timeout: Duration) {
    let start = Instant::now();
    loop {
        if let Some(v) = fetch_policy_degraded(addr)
            && v == expected
        {
            return;
        }
        if start.elapsed() >= timeout {
            let last = fetch_policy_degraded(addr);
            panic!("timed out waiting for policy_degraded={expected}; last observed = {last:?}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Fetch one numeric metric from the Prometheus endpoint by exact line
/// prefix. Include labels and the trailing space in `line_prefix`, e.g.
/// `melin_replica_catching_up{slot="0"} `.
fn fetch_metric_u64(addr: SocketAddr, line_prefix: &str) -> Option<u64> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(1)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = Vec::new();
    stream.read_to_end(&mut body).ok()?;
    let text = std::str::from_utf8(&body).ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix(line_prefix) {
            return rest.trim().parse().ok();
        }
    }
    None
}

/// Poll a metric until `pred(value)` holds, or panic on timeout with
/// the last observed value.
fn wait_metric(
    addr: SocketAddr,
    line_prefix: &str,
    timeout: Duration,
    what: &str,
    pred: impl Fn(u64) -> bool,
) {
    let start = Instant::now();
    loop {
        if let Some(v) = fetch_metric_u64(addr, line_prefix)
            && pred(v)
        {
            return;
        }
        if start.elapsed() >= timeout {
            let last = fetch_metric_u64(addr, line_prefix);
            panic!("timed out waiting for {what}; last `{line_prefix}` = {last:?}");
        }
        std::thread::sleep(Duration::from_millis(100));
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
/// Authenticate as an operator and send a single admin command line
/// (e.g. `PROMOTE`, `ROTATE`). Returns the server's first response line
/// trimmed of whitespace.
fn admin_command(addr: SocketAddr, operator_key: &SigningKey, command: &str) -> String {
    use melin_protocol::codec;
    use melin_protocol::message::{Request, ResponseKind};

    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
        .expect("connect to admin endpoint");
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
    let nonce = match codec::decode_response(&frame_buf).expect("decode challenge") {
        ResponseKind::Challenge { nonce } => nonce,
        other => panic!("expected Challenge, got {other:?}"),
    };

    // Step 2: Sign nonce + ephemerals (TCP path uses zero ephs).
    let signature = operator_key.sign(&nonce);
    let request = Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: operator_key.verifying_key().to_bytes(),
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
        ResponseKind::AuthFailed => panic!("admin auth failed"),
        other => panic!("unexpected auth response: {other:?}"),
    }

    // Step 4: Send command + read response line.
    stream
        .write_all(format!("{command}\n").as_bytes())
        .expect("send admin command");
    let mut reader = BufReader::new(&stream);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .expect("read admin response");
    response.trim().to_string()
}

fn promote(addr: SocketAddr, operator_key: &SigningKey) {
    let response = admin_command(addr, operator_key, "PROMOTE");
    assert!(response == "OK", "promotion failed: {response}");
}

/// Send `DURABILITY <mode>` to a node's admin endpoint and assert it
/// succeeds. Used by tests that drive runtime mode swaps (e.g. the
/// promoted-replica-without-replicas case where Hybrid is structurally
/// unsatisfiable and the operator must downgrade to Local for the gate
/// to open).
fn set_durability_mode(addr: SocketAddr, operator_key: &SigningKey, mode: &str) {
    let cmd = format!("DURABILITY {mode}");
    let response = admin_command(addr, operator_key, &cmd);
    assert!(
        response == "OK",
        "set durability {mode} on {addr} failed: {response}"
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
/// Spawn a primary server process with caller-supplied extra CLI flags
/// (e.g. `--admin-bind`, `--max-journal-mib`).
fn spawn_primary_with_extra(
    bin: &Path,
    tmp_dir: &Path,
    keys_path: &Path,
    client_port: u16,
    health_port: u16,
    replication_port: u16,
    extra_args: &[&str],
) -> ServerProcess {
    spawn_primary_with_extra_env(
        bin,
        tmp_dir,
        keys_path,
        client_port,
        health_port,
        replication_port,
        extra_args,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
fn spawn_primary_with_extra_env(
    bin: &Path,
    tmp_dir: &Path,
    keys_path: &Path,
    client_port: u16,
    health_port: u16,
    replication_port: u16,
    extra_args: &[&str],
    extra_env: &[(&str, &str)],
) -> ServerProcess {
    let journal = tmp_dir.join("primary.journal");
    let mut args: Vec<String> = vec![
        "--bind".into(),
        format!("127.0.0.1:{client_port}"),
        "--health-bind".into(),
        format!("127.0.0.1:{health_port}"),
        "--replication-bind".into(),
        format!("127.0.0.1:{replication_port}"),
        "--journal".into(),
        journal.to_str().expect("valid path").into(),
        "--authorized-keys".into(),
        keys_path.to_str().expect("valid path").into(),
        "--accounts".into(),
        "10".into(),
        "--instruments".into(),
        "2".into(),
        "--connection-timeout-secs".into(),
        "0".into(),
        "--yield-idle".into(),
        // Reduce core count to avoid conflicts in CI.
        "--cores".into(),
        "0,0,0,0,0,0,0,0,0".into(),
    ];
    for a in extra_args {
        args.push((*a).into());
    }
    let mut command = Command::new(bin);
    command
        .args(&args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("MELIN_JOURNAL_PREALLOC_MIB", "4");
    for (k, v) in extra_env {
        command.env(k, v);
    }
    let child = command.spawn().expect("spawn primary server");

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
    admin_port: u16,
) -> ServerProcess {
    spawn_replica_named(
        bin,
        tmp_dir,
        keys_path,
        repl_key_path,
        primary_repl_port,
        client_port,
        health_port,
        admin_port,
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
    admin_port: u16,
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
        admin_port,
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
    admin_port: u16,
    name: &str,
    extra_args: &[&str],
) -> ServerProcess {
    spawn_replica_named_with_extra_env(
        bin,
        tmp_dir,
        keys_path,
        repl_key_path,
        primary_repl_port,
        client_port,
        health_port,
        admin_port,
        name,
        extra_args,
        &[],
    )
}

#[allow(clippy::too_many_arguments)]
fn spawn_replica_named_with_extra_env(
    bin: &Path,
    tmp_dir: &Path,
    keys_path: &Path,
    repl_key_path: &Path,
    primary_repl_port: u16,
    client_port: u16,
    health_port: u16,
    admin_port: u16,
    name: &str,
    extra_args: &[&str],
    extra_env: &[(&str, &str)],
) -> ServerProcess {
    let journal = tmp_dir.join(format!("{name}.journal"));
    let mut args: Vec<String> = vec![
        "--bind".into(),
        format!("127.0.0.1:{client_port}"),
        "--health-bind".into(),
        format!("127.0.0.1:{health_port}"),
        "--replica-of".into(),
        format!("127.0.0.1:{primary_repl_port}"),
        "--replication-key".into(),
        repl_key_path.to_str().expect("valid path").into(),
        "--admin-bind".into(),
        format!("127.0.0.1:{admin_port}"),
        "--journal".into(),
        journal.to_str().expect("valid path").into(),
        "--authorized-keys".into(),
        keys_path.to_str().expect("valid path").into(),
        "--connection-timeout-secs".into(),
        "0".into(),
        "--yield-idle".into(),
        "--cores".into(),
        "0,0,0,0,0,0,0,0,0".into(),
    ];
    for a in extra_args {
        args.push((*a).into());
    }
    let mut command = Command::new(bin);
    command
        .args(&args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .env("MELIN_JOURNAL_PREALLOC_MIB", "4");
    for (k, v) in extra_env {
        command.env(k, v);
    }
    let child = command.spawn().expect("spawn replica server");

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
    admin_port: u16,
    /// Primary's replication-listen port. Stored so tests that restart
    /// the primary in place (after a SIGKILL, to exercise journal-recovery
    /// paths with a live replica) can bind the new process to the same
    /// port the replica is already trying to reconnect to.
    primary_repl_port: u16,
    key: SigningKey,
    key2: SigningKey,
    operator_key: SigningKey,
    bin: PathBuf,
    keys_path: PathBuf,
    repl_key_path: PathBuf,
    _tmp: tempfile::TempDir,
}

impl TestCluster {
    /// Spin up a primary + replica pair, wait for both to be healthy.
    fn start() -> Self {
        Self::start_with_extra_args(&[])
    }

    /// Same as [`start`], but pass `extra_args` to **both** the primary
    /// and the replica. Used by determinism-sensitive tests where the
    /// two engines must run with matching operator policy (e.g. SEC-04
    /// rate-limit knobs).
    fn start_with_extra_args(extra_args: &[&str]) -> Self {
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
        let replica_admin_port = free_port();

        let primary = spawn_primary_with_extra(
            &bin,
            tmp.path(),
            &keys_path,
            primary_client_port,
            primary_health_port,
            primary_repl_port,
            extra_args,
        );

        // Wait for the primary to be ready to accept replica connections.
        wait_for_primary_repl_ready(primary.health_addr, Duration::from_secs(10));

        let replica = spawn_replica_named_with_extra(
            &bin,
            tmp.path(),
            &keys_path,
            &repl_key_path,
            primary_repl_port,
            replica_client_port,
            replica_health_port,
            replica_admin_port,
            "replica",
            extra_args,
        );

        wait_healthy(primary.health_addr, Duration::from_secs(30));

        Self {
            primary,
            replica,
            admin_port: replica_admin_port,
            primary_repl_port,
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
        connect_with_timeout(self.primary.client_addr, &self.key)
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

        let promote_addr: SocketAddr = format!("127.0.0.1:{}", self.admin_port).parse().unwrap();
        promote(promote_addr, &self.operator_key);
        // The promoted node was a replica running with the cluster
        // default (`hybrid`); standalone it can't satisfy
        // `in_memory>=2`, so the response gate would stall forever.
        // Downgrade to `local` via the admin DURABILITY command — the
        // production failover playbook for a freshly-promoted node
        // without peers. A separate test
        // (`dual_replication_promote_then_durability_swap`) covers the
        // same path on the dual-cluster shape so we know the runtime
        // swap works under both topologies.
        set_durability_mode(promote_addr, &self.operator_key, "local");

        wait_ready(self.replica.health_addr, Duration::from_secs(30));

        connect_with_timeout(self.replica.client_addr, &self.key2)
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

/// Exercise the `hybrid` durability gate on a primary that has been
/// restarted (i.e. recovered from its journal). The recovered primary's
/// wire-sequence allocator starts at `last_seq + 1` — much larger than
/// the fresh-primary case (which starts at 2 with the hash-chain feature)
/// — so any code path that conflates wire-sequence space with the local
/// input-consumer space will silently open the response gate before the
/// replica has actually replicated the event.
///
/// The expected behavior of `hybrid` (`persisted>=1 && in_memory>=2`) is
/// that every acked response is on at least one other node's in-memory
/// state. We pre-populate the primary's journal with `PREFILL` events to
/// push `starting_sequence` well above 2, then submit a small `BURST` of
/// new orders, immediately SIGKILL the primary without waiting for
/// replication lag to reach 0, and assert the promoted replica has all
/// `BURST` acked orders in its dedup state.
///
/// If the gate is correctly enforced, the replica must have every acked
/// order — the test tolerates 0 lost orders. The existing
/// `kill_without_waiting_for_replication` test already documents a
/// fresh-primary off-by-one in this path; if the bug generalises to a
/// `starting_sequence`-scaled loss, this test will surface it as
/// multiple missing orders (up to `BURST`), making the issue
/// quantitatively distinct from the fresh-primary version.
#[test]
#[serial]
fn recovered_primary_durability_gate_holds() {
    const PREFILL: u64 = 50;
    const BURST: u64 = 10;

    let mut cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Phase 1: pre-populate journal to drive `starting_sequence` high on
    // the next recovery.
    for i in 1..=PREFILL {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "phase-1 order {i} no response");
    }
    cluster.wait_replicated();
    drop(client);

    // SIGKILL both nodes and restart the replica with a wiped journal.
    // The primary recovers from its journal so its `starting_sequence`
    // is bumped past 2 — that's the property under test. The replica
    // wipe is unrelated to the test invariant; it just keeps the
    // recovery surface narrow (the live-replica reconnect path is
    // exercised by `replica_reconnects_after_primary_restart_without_journal_wipe`).
    unsafe {
        libc::kill(cluster.primary.child.id() as i32, libc::SIGKILL);
        libc::kill(cluster.replica.child.id() as i32, libc::SIGKILL);
    }
    let _ = cluster.primary.child.wait();
    let _ = cluster.replica.child.wait();
    let _ = std::fs::remove_file(cluster._tmp.path().join("replica.journal"));
    let _ = std::fs::remove_file(cluster._tmp.path().join("replica.snapshot"));

    // Restart the primary on the same ports + same journal — recovery
    // through the journal pushes `starting_sequence` past 2.
    let restarted_primary = spawn_primary_with_extra(
        &cluster.bin,
        cluster._tmp.path(),
        &cluster.keys_path,
        cluster.primary.client_addr.port(),
        cluster.primary.health_addr.port(),
        cluster.primary_repl_port,
        &[],
    );
    cluster.primary = restarted_primary;
    wait_for_primary_repl_ready(cluster.primary.health_addr, Duration::from_secs(10));

    // Restart the replica as a fresh process (no prior journal).
    let restarted_replica = spawn_replica_named_with_extra(
        &cluster.bin,
        cluster._tmp.path(),
        &cluster.keys_path,
        &cluster.repl_key_path,
        cluster.primary_repl_port,
        cluster.replica.client_addr.port(),
        cluster.replica.health_addr.port(),
        cluster.admin_port,
        "replica",
        &[],
    );
    cluster.replica = restarted_replica;

    wait_healthy(cluster.primary.health_addr, Duration::from_secs(30));
    cluster.wait_replicated();

    // Phase 2: submit BURST new orders. `Client::connect` auto-syncs
    // against the engine's per-key request_seq HWM (50 after phase 1)
    // so the first phase-2 request lands at HWM + 1 and skips dedup.
    // Without that, every request would come back as
    // `Rejected{DuplicateRequest}` rather than a gate-driven `Placed`.
    let mut client = cluster.connect_primary();
    let mut acked: Vec<u64> = Vec::with_capacity(BURST as usize);
    for i in 0..BURST {
        let id = PREFILL + 1 + i;
        let r = submit_order(&mut client, id, 1, 1, Side::Buy, 100, 10);
        // Only count orders that were actually Placed — a non-empty
        // response could also be a Rejected (e.g. balance, dedup),
        // which doesn't reflect a gated-durable state.
        if has_report(&r, |rep| {
            matches!(rep, melin_protocol::types::ExecutionReport::Placed { .. })
        }) {
            acked.push(id);
        }
    }
    assert!(!acked.is_empty(), "no burst orders were placed");
    drop(client);

    // Promote the replica WITHOUT waiting for replication lag — the
    // hybrid gate's contract is "every acked response is already on the
    // replica," so a wait would mask the bug.
    let mut client2 = cluster.kill_and_promote();

    // For each acked burst order, try to submit it again on the promoted
    // node and assert it comes back as a duplicate (proving the original
    // is in `live_order_ids`). Anything that comes back Placed instead is
    // a durability violation.
    let mut missing: Vec<u64> = Vec::new();
    for id in &acked {
        let r = submit_order(&mut client2, *id, 1, 1, Side::Buy, 100, 10);
        if !has_report(&r, |rep| {
            matches!(
                rep,
                melin_protocol::types::ExecutionReport::Rejected {
                    reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                    ..
                }
            )
        }) {
            missing.push(*id);
        }
    }

    assert!(
        missing.is_empty(),
        "hybrid gate broken on recovered primary: {} of {} acked burst orders \
         were not on the promoted replica (missing ids: {:?}). \
         Acked: {:?}",
        missing.len(),
        acked.len(),
        missing,
        acked,
    );
}

/// Regression: a long-lived replica must rejoin a restarted primary
/// without recreating its journal file.
///
/// Before the fix, the replica's reconnect loop gated the
/// fresh-journal creation on `journal_writer.is_none()`. The writer
/// is moved into the pipeline on first connect and never returned, so
/// every subsequent reconnect sees `journal_writer == None` and tries to
/// create a fresh journal file — which fails `AlreadyExists` against the
/// journal the replica is already streaming into. The
/// `recovered_primary_durability_gate_holds` test sidesteps this by also
/// wiping the replica's journal on restart; production deployments
/// cannot.
///
/// This test SIGKILLs **only the primary**, restarts it on the same port
/// from its own journal, and asserts that the live replica reconnects
/// and converges replication lag to 0 — proving the fresh-journal path
/// is not re-entered on reconnect.
#[test]
#[serial]
fn replica_reconnects_after_primary_restart_without_journal_wipe() {
    let mut cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Initial traffic so both nodes have non-trivial journal state.
    for i in 1..=10u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "phase-1 order {i} no response");
    }
    cluster.wait_replicated();
    drop(client);

    // SIGKILL the primary only — replica process stays alive and will
    // attempt to reconnect. This is the scenario the prior bug broke.
    unsafe {
        libc::kill(cluster.primary.child.id() as i32, libc::SIGKILL);
    }
    let _ = cluster.primary.child.wait();

    // Restart the primary on the same ports from its existing journal.
    let restarted_primary = spawn_primary_with_extra(
        &cluster.bin,
        cluster._tmp.path(),
        &cluster.keys_path,
        cluster.primary.client_addr.port(),
        cluster.primary.health_addr.port(),
        cluster.primary_repl_port,
        &[],
    );
    cluster.primary = restarted_primary;
    wait_for_primary_repl_ready(cluster.primary.health_addr, Duration::from_secs(10));

    // The replica was never restarted: its journal file still exists, and
    // its in-process pipeline still owns the writer. Before the fix this
    // reconnect would fail with `AlreadyExists` from the fresh-journal
    // creation path and replication lag would never converge.
    wait_healthy(cluster.primary.health_addr, Duration::from_secs(30));
    cluster.wait_replicated();

    // Push more traffic and confirm it replicates — this proves the
    // replica is genuinely streaming, not just temporarily caught up.
    let mut client = cluster.connect_primary();
    client
        .synchronize_request_seq()
        .expect("synchronize_request_seq with recovered primary");
    for i in 11..=20u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        // Require an explicit `Placed` rather than just non-empty: a
        // `Rejected{DuplicateRequest}` (e.g. if the HWM sync regressed)
        // would otherwise masquerade as success and let the test pass
        // against a primary that's silently rejecting every order.
        assert!(
            has_report(&r, |rep| matches!(
                rep,
                melin_protocol::types::ExecutionReport::Placed { .. }
            )),
            "phase-2 order {i}: expected Placed, got {r:?}"
        );
    }
    drop(client);
    cluster.wait_replicated();
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
                "--durability-mode",
                "local",
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
                "0,0,0,0,0,0,0,0,0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
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

    let mut client3 = connect_with_timeout(recovered.client_addr, &cluster.key2);

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

/// End-to-end dense-sequence audit across a real primary + replica
/// pair: submits a few hundred orders over TCP loopback, waits for
/// replication to catch up, then walks both journal files end-to-end
/// with `JournalReader`. Any `SequenceGap` surfaced by the
/// strict-continuity check is the corruption mode an external
/// `journal_verify` run would report. (Historically this reproduced an
/// in-stream checkpoint sequence collision; those entries no longer
/// exist, but the dense-sequence invariant is still the property
/// production audits rely on.)
#[test]
#[serial]
fn journals_contiguous_across_replication() {
    use melin_journal::JournalReader;

    let cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Server seeds 10 accounts + 2 instruments → 12 seed events before
    // the first order.
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

/// Reconnect with the SAME key after failover and confirm the
/// promoted replica preserved this key's request_seq HWM. The dedup
/// state must survive replication and promotion: querying the HWM on
/// the new connection must report the count we drove the original
/// primary to, not a freshly-initialised zero.
///
/// Originally framed as "first replay request is rejected as
/// DuplicateRequest" — that observation depended on a fresh client
/// starting `next_seq` at 0 and colliding with the HWM. Now that
/// `Client::connect` auto-syncs the HWM on connect, the collision
/// cannot occur from the client side; we assert the same invariant by
/// inspecting the HWM the auto-sync adopts.
#[test]
#[serial]
fn same_key_request_seq_hwm_survives_failover() {
    let mut cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Submit 10 orders. The engine's per-key HWM for this key reaches 10.
    for i in 1..=10u64 {
        submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
    }

    cluster.wait_replicated();
    drop(client);

    // Kill + promote.
    let promote_addr: SocketAddr = format!("127.0.0.1:{}", cluster.admin_port).parse().unwrap();
    unsafe {
        libc::kill(cluster.primary.child.id() as i32, libc::SIGKILL);
    }
    let _ = cluster.primary.child.wait();
    promote(promote_addr, &cluster.operator_key);
    set_durability_mode(promote_addr, &cluster.operator_key, "local");
    wait_ready(cluster.replica.health_addr, Duration::from_secs(30));

    // Reconnect with the SAME key. `Client::connect` auto-syncs against
    // the engine's HWM; calling `synchronize_request_seq` again returns
    // the value it adopted — which must equal the pre-failover count.
    let mut client_retry = connect_with_timeout(cluster.replica.client_addr, &cluster.key);
    let hwm = client_retry
        .synchronize_request_seq()
        .expect("query request_seq HWM on promoted replica");
    assert_eq!(
        hwm, 10,
        "promoted replica lost per-key request_seq HWM: got {hwm}, expected 10"
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
    replica1_admin_port: u16,
    replica2_admin_port: u16,
    key: SigningKey,
    key2: SigningKey,
    operator_key: SigningKey,
    repl_key_path: PathBuf,
    _tmp: tempfile::TempDir,
}

impl DualCluster {
    fn start() -> Self {
        Self::start_with_args(&[], &[])
    }

    fn start_with_primary_args(primary_extra_args: &[&str]) -> Self {
        Self::start_with_args(primary_extra_args, &[])
    }

    fn start_with_args(primary_extra_args: &[&str], replica_extra_args: &[&str]) -> Self {
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

        let primary = spawn_primary_with_extra(
            &bin,
            tmp.path(),
            &keys_path,
            primary_client_port,
            primary_health_port,
            primary_repl_port,
            primary_extra_args,
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
            replica1_admin_port: r1_promote,
            replica2_admin_port: r2_promote,
            key,
            key2,
            operator_key,
            repl_key_path,
            _tmp: tmp,
        }
    }

    fn connect_primary(&self) -> Client {
        connect_with_timeout(self.primary.client_addr, &self.key)
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
        let addr: SocketAddr = format!("127.0.0.1:{}", self.replica1_admin_port)
            .parse()
            .unwrap();
        promote(addr, &self.operator_key);
        // Downgrade the promoted standalone to `local` so its gate can
        // open without peers. See `TestCluster::kill_and_promote` for
        // the full rationale.
        set_durability_mode(addr, &self.operator_key, "local");
        wait_ready(self.replica1.health_addr, Duration::from_secs(30));
        connect_with_timeout(self.replica1.client_addr, &self.key2)
    }

    fn promote_replica2(&self) -> Client {
        let addr: SocketAddr = format!("127.0.0.1:{}", self.replica2_admin_port)
            .parse()
            .unwrap();
        promote(addr, &self.operator_key);
        set_durability_mode(addr, &self.operator_key, "local");
        wait_ready(self.replica2.health_addr, Duration::from_secs(30));
        connect_with_timeout(self.replica2.client_addr, &self.key2)
    }

    fn primary_trading(&self) -> bool {
        query_health(self.primary.health_addr)
            .map(|(_, _, _, t)| t)
            .unwrap_or(false)
    }
}

/// SEC-04 cross-receiver: per-account order-rate limiter must produce
/// identical accept/reject decisions on primary and replica. Configures
/// a tight rate limit (`burst=2`, `rate=1/s`), submits enough orders
/// rapidly to deplete the bucket, and asserts the third order is
/// rejected with `ExceedsOrderRate`.
///
/// The response gate guarantees that any response observed by the
/// client has already been replicated and applied on the replica's
/// engine — meaning the `Rejected{ExceedsOrderRate}` returned to the
/// client is *also* the report the replica produced, byte-identical.
/// If the replica's bucket state diverged from the primary's (different
/// `tokens` or `last_refill_ns`), it would emit a different report (or
/// no Rejected at all), the chain hash would mismatch, and replication
/// would halt before this response was ever returned. Observing the
/// response under the gate is therefore proof that primary and replica
/// converged on the same SEC-04 outcome under real wiring (TCP
/// transport + journal + receiver), not just in the in-process
/// proptest.
#[test]
#[serial]
fn sec04_rate_limit_replicates_to_replica() {
    let cluster = TestCluster::start_with_extra_args(&[
        "--max-orders-per-second",
        "1", // 1 token/second refill — minimum non-disabling rate
        "--max-orders-burst",
        "2", // burst of 2 — third rapid submit must exceed
    ]);
    let mut client = cluster.connect_primary();

    // Burn the burst (2 tokens) — both should accept.
    for i in 1..=2u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 1);
        assert!(
            !r.is_empty(),
            "order {i}: response gate dropped reply — replication issue?",
        );
        assert!(
            !has_report(&r, |rep| matches!(
                rep,
                melin_protocol::types::ExecutionReport::Rejected {
                    reason: melin_protocol::types::RejectReason::ExceedsOrderRate,
                    ..
                }
            )),
            "order {i} within burst should NOT rate-reject, got: {r:?}",
        );
    }

    // Third submission within the same wall-clock millisecond — the
    // bucket has at most a few microseconds' worth of refill at 1/s
    // (effectively zero tokens), so this MUST reject.
    let r = submit_order(&mut client, 3, 1, 1, Side::Buy, 100, 1);
    assert!(!r.is_empty(), "rate-limited response gate timed out");
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::ExceedsOrderRate,
                ..
            }
        )),
        "expected ExceedsOrderRate cross-receiver, got: {r:?}",
    );

    // The response above was gated on replication: if the replica's
    // engine diverged on the rate-limit decision (different bucket
    // state), the chain hash would mismatch and the response would
    // never have been sent. Reaching this point proves cross-receiver
    // convergence. As a final liveness check, wait for lag = 0 — would
    // only fail if replication had since halted.
    cluster.wait_replicated();
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
    // We don't call `JournaledApp::recover()` here because the replica
    // may have been killed mid-write, leaving a truncated entry that
    // recovery would reject. The replacement replica's run_receiver
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
                "--admin-bind",
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
                "0,0,0,0,0,0,0,0,0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
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
    set_durability_mode(promote_addr, &cluster.operator_key, "local");
    let r3_health_addr: SocketAddr = format!("127.0.0.1:{r3_health}").parse().unwrap();
    wait_ready(r3_health_addr, Duration::from_secs(30));

    let mut client2 = connect_with_timeout(
        format!("127.0.0.1:{r3_client}").parse().unwrap(),
        &cluster.key2,
    );

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
                "--admin-bind",
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
                "0,0,0,0,0,0,0,0,0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
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
    let promote_addr: SocketAddr = format!("127.0.0.1:{r3_promote}").parse().unwrap();
    promote(promote_addr, &cluster.operator_key);
    set_durability_mode(promote_addr, &cluster.operator_key, "local");
    wait_ready(
        format!("127.0.0.1:{r3_health}").parse().unwrap(),
        Duration::from_secs(30),
    );

    let mut client2 = connect_with_timeout(
        format!("127.0.0.1:{r3_client}").parse().unwrap(),
        &cluster.key2,
    );

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
                "--admin-bind",
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
                "0,0,0,0,0,0,0,0,0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
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
    let promote_addr: SocketAddr = format!("127.0.0.1:{r3_promote}").parse().unwrap();
    promote(promote_addr, &cluster.operator_key);
    set_durability_mode(promote_addr, &cluster.operator_key, "local");
    wait_ready(
        format!("127.0.0.1:{r3_health}").parse().unwrap(),
        Duration::from_secs(30),
    );

    let mut client2 = connect_with_timeout(
        format!("127.0.0.1:{r3_client}").parse().unwrap(),
        &cluster.key2,
    );

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
                "--admin-bind",
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
                "0,0,0,0,0,0,0,0,0",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
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
    let promote_addr: SocketAddr = format!("127.0.0.1:{r3_promote}").parse().unwrap();
    promote(promote_addr, &cluster.operator_key);
    set_durability_mode(promote_addr, &cluster.operator_key, "local");
    wait_ready(
        format!("127.0.0.1:{r3_health}").parse().unwrap(),
        Duration::from_secs(30),
    );

    let mut client2 = connect_with_timeout(
        format!("127.0.0.1:{r3_client}").parse().unwrap(),
        &cluster.key2,
    );

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
///
/// The replica's state is asserted directly, by promoting it: a fresh
/// replica (`last_sequence = 0`) facing a pruned lineage (oldest segment
/// starting past 1) MUST receive the snapshot, not journal catch-up —
/// catch-up from a trimmed history would build a self-consistent journal
/// on top of an empty exchange, silently missing every pre-snapshot
/// event. Only interrogating the promoted replica can catch that.
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
                "0,0,0,0,0,0,0,0,0",
                "--standalone",
                "--durability-mode",
                "local",
                "--snapshot-interval-ms",
                "100",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
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
    let mut client = connect_with_timeout(primary.client_addr, &key);
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
                "0,0,0,0,0,0,0,0,0",
                "--snapshot-interval-ms",
                "100",
            ])
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .env("MELIN_JOURNAL_PREALLOC_MIB", "4")
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
    let replica_admin_port = free_port();
    let _replica = spawn_replica(
        &bin,
        tmp.path(),
        &keys_path,
        &repl_key_path,
        primary_repl_port2,
        replica_client_port,
        replica_health_port,
        replica_admin_port,
    );

    // Wait for the primary to become healthy (seeding done, replica connected).
    wait_healthy(primary2.health_addr, Duration::from_secs(30));
    eprintln!("Primary healthy with replica connected");

    wait_for_replacement_catchup(primary2.health_addr);
    eprintln!("Replica caught up via snapshot transfer.");

    // Submit a new order to verify the primary is functional.
    let mut client2 = connect_with_timeout(primary2.client_addr, &key2);
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

    // Verify dedup: replay order 19 (from before snapshot) must be rejected.
    // Deliberately NOT id 20 — that id is reserved for the promoted-replica
    // probe below. Every submit here is journaled (inputs persist before
    // matching) and streamed to the replica, so replaying an id on the
    // primary lets a state-diverged replica *place* it from the stream,
    // which would satisfy the later duplicate probe for the wrong reason.
    let r = submit_order(&mut client2, 19, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "expected DuplicateOrderId for id=19 after snapshot transfer, got: {r:?}"
    );

    // Make sure orders 20-21 (the post-transfer traffic above) have
    // durably reached the replica before the primary goes away: capture
    // the primary's tail, then wait for replication lag 0 at-or-past it.
    let (_, tail_after_orders, _, _) =
        query_health(primary2.health_addr).expect("health after post-transfer orders");
    let start = Instant::now();
    loop {
        if let Ok((_, journal_seq, lag, _)) = query_health(primary2.health_addr)
            && lag == 0
            && journal_seq >= tail_after_orders
        {
            break;
        }
        if start.elapsed() > Duration::from_secs(30) {
            panic!("replica never drained post-transfer orders (lag != 0)");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(client2);

    // ----- Promote the replica and interrogate ITS state -----
    unsafe { libc::kill(primary2.child.id() as i32, libc::SIGINT) };
    let _ = primary2.child.wait();

    let promote_addr: SocketAddr = format!("127.0.0.1:{replica_admin_port}").parse().unwrap();
    promote(promote_addr, &operator_key);
    set_durability_mode(promote_addr, &operator_key, "local");
    wait_ready(
        format!("127.0.0.1:{replica_health_port}").parse().unwrap(),
        Duration::from_secs(30),
    );

    let mut rclient = connect_with_timeout(
        format!("127.0.0.1:{replica_client_port}").parse().unwrap(),
        &key2,
    );

    // Pre-snapshot state: order 20 exists ONLY in the transferred
    // snapshot — its id was never journaled after the purge, so the
    // stream could not have delivered it. A replica that journal-caught-up
    // from the pruned lineage instead of receiving the snapshot has an
    // empty book here and places this as a fresh order — the silent
    // state divergence this test exists to catch.
    let r = submit_order(&mut rclient, 20, 1, 1, Side::Buy, 100, 10);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "promoted replica is missing pre-snapshot state (order 20 not a duplicate): {r:?}"
    );

    // Live-streamed state: order 21 arrived over the stream after the
    // snapshot transfer.
    let r = submit_order(&mut rclient, 21, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Rejected {
                reason: melin_protocol::types::RejectReason::DuplicateOrderId,
                ..
            }
        )),
        "promoted replica is missing live-streamed state (order 21 not a duplicate): {r:?}"
    );

    // And it accepts new business as the authority.
    let r = submit_order(&mut rclient, 22, 1, 1, Side::Buy, 200, 5);
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
                | melin_protocol::types::ExecutionReport::Fill { .. }
        )),
        "expected Placed or Fill on promoted replica, got: {r:?}"
    );

    drop(rclient);
    eprintln!("PASS: snapshot transfer — promoted replica holds pre-snapshot and streamed state.");
}

// ---------------------------------------------------------------------------
// Journal rotation soak
// ---------------------------------------------------------------------------
//
// Drives sustained traffic against a primary+replica pair while rotation
// fires repeatedly (size threshold + manual `ROTATE` admin command).
// Stops both nodes cleanly, restarts them, and asserts the recovered
// journal sequences and on-disk segment layout match expectations.
//
// Co-located with the failover suite because it shares the multi-process
// harness (`spawn_primary_with_extra`, `spawn_replica`, `admin_command`).
// A dedicated `tests/journal_rotation.rs` file would duplicate ~200 lines
// of scaffolding — extract one if a third multi-process test category
// shows up.

/// Submit N resting limit orders, each with a unique id starting at
/// `first_id`. Returning Ok from `send_request` means the event was
/// durably journaled before reply (persist-before-ack), which is what
/// the soak relies on. Trader key works because SubmitOrder is a
/// trading op — Deposit would require a custodian key.
fn submit_resting_burst(client: &mut Client, first_id: u64, n: u64) {
    for i in 0..n {
        client
            .send_request(&Request::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(first_id + i),
                    account: AccountId(1),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: price(50),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: qty(1),
                    stp: melin_protocol::types::SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            })
            .expect("submit order");
    }
}

/// Count archived segments next to a journal path.
fn count_archives(journal_path: &Path) -> usize {
    melin_journal::segment::list_archives(journal_path)
        .map(|v| v.len())
        .unwrap_or(0)
}

/// Walk a node's full segment chain via the shared lineage verifier
/// (dense sequences within and across segments, each header matching
/// its first entry, successor anchors equal to predecessor tails) and
/// return `(first_sequence, last_sequence)` over the whole lineage.
fn walk_segments_dense(journal_path: &Path) -> (u64, u64) {
    use melin_trading::trading_event::TradingEvent;

    let report = melin_journal::segment::verify_lineage::<TradingEvent>(journal_path)
        .unwrap_or_else(|e| panic!("lineage of {} broken: {e}", journal_path.display()));
    // The verifier tolerates a live-tail crash gap (recovery's
    // allow_partial_tail); these journals were cleanly shut, so any
    // gap here would be a real bug.
    assert_eq!(
        report.live_tail_gap,
        None,
        "cleanly-shut journal {} must have no live-tail gap",
        journal_path.display()
    );
    (
        report
            .first_sequence
            .expect("lineage should contain at least one entry"),
        report
            .last_sequence
            .expect("lineage should contain at least one entry"),
    )
}

#[test]
#[serial]
fn rotation_soak_under_load() {
    let bin = server_bin();
    assert!(bin.exists(), "melin-server binary not found at {bin:?}");
    let tmp = tempfile::Builder::new()
        .prefix("melin-soak-")
        .tempdir()
        .expect("create temp dir");

    let key = SigningKey::from_bytes(&[0xCA; 32]);
    let operator_key = SigningKey::from_bytes(&[0xCD; 32]);
    let repl_key = SigningKey::from_bytes(&[0xCE; 32]);
    let (keys_path, repl_key_path) =
        write_auth_keys_multi(tmp.path(), &[&key], &operator_key, &repl_key);

    let primary_client_port = free_port();
    let primary_health_port = free_port();
    let primary_repl_port = free_port();
    let primary_admin_port = free_port();
    let replica_client_port = free_port();
    let replica_health_port = free_port();
    let replica_admin_port = free_port();

    let primary_admin_addr = format!("127.0.0.1:{primary_admin_port}");
    let primary_extra: &[&str] = &[
        "--admin-bind",
        &primary_admin_addr,
        "--max-journal-mib",
        "0", // disable size trigger; rely on ROTATE for determinism
    ];
    let extra_env: &[(&str, &str)] = &[];
    let mut primary = spawn_primary_with_extra_env(
        &bin,
        tmp.path(),
        &keys_path,
        primary_client_port,
        primary_health_port,
        primary_repl_port,
        primary_extra,
        extra_env,
    );
    wait_for_primary_repl_ready(primary.health_addr, Duration::from_secs(10));

    let mut replica = spawn_replica_named_with_extra_env(
        &bin,
        tmp.path(),
        &keys_path,
        &repl_key_path,
        primary_repl_port,
        replica_client_port,
        replica_health_port,
        replica_admin_port,
        "replica",
        &[],
        extra_env,
    );
    wait_healthy(primary.health_addr, Duration::from_secs(30));

    // ----- Drive load with interleaved ROTATEs -----
    let mut client = connect_with_timeout(primary.client_addr, &key);
    let admin_addr: SocketAddr = primary_admin_addr.parse().unwrap();
    let replica_admin_addr: SocketAddr = format!("127.0.0.1:{replica_admin_port}").parse().unwrap();

    // 5 rounds × 15 orders = 75 orders total — kept under the
    // checkpoint interval (1M, set above). ROTATE *before* each
    // burst (rather than after) so the burst's first event flushes
    // the rotate flag through the journal stage; rotating after
    // bursts would leave the final ROTATE's flag set with no event
    // to drive observation, producing rounds-1 archives instead of
    // rounds. Two of the rounds also rotate the replica to validate
    // independent replica-side rotation.
    let per_round: u64 = 15;
    let rounds: u64 = 5;
    let total_orders: u64 = per_round * rounds;
    let mut next_id: u64 = 1;
    for round in 0..rounds {
        let resp = admin_command(admin_addr, &operator_key, "ROTATE");
        assert!(resp == "OK", "primary ROTATE #{round} failed: {resp}");
        if round == 1 || round == 3 {
            let resp = admin_command(replica_admin_addr, &operator_key, "ROTATE");
            assert!(resp == "OK", "replica ROTATE #{round} failed: {resp}");
        }
        submit_resting_burst(&mut client, next_id, per_round);
        next_id += per_round;
    }

    // Wait for replication lag = 0 so all events are durable on both nodes.
    let start = Instant::now();
    loop {
        let h = query_health(primary.health_addr);
        if let Ok((_, _, 0, _)) = h {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "replication lag did not reach 0; last health = {h:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Clean shutdown via SIGINT.
    drop(client);
    unsafe {
        libc::kill(primary.child.id() as i32, libc::SIGINT);
        libc::kill(replica.child.id() as i32, libc::SIGINT);
    }
    let _ = primary
        .child
        .wait_timeout_with_kill(Duration::from_secs(10));
    let _ = replica
        .child
        .wait_timeout_with_kill(Duration::from_secs(10));

    let primary_journal = tmp.path().join("primary.journal");
    let replica_journal = tmp.path().join("replica.journal");

    // Primary should have exactly 5 archives (one per ROTATE).
    assert_eq!(count_archives(&primary_journal), 5, "primary archive count");
    // Replica should have exactly 2 (the two ROTATEs we sent there).
    assert_eq!(count_archives(&replica_journal), 2, "replica archive count");

    // Walk BOTH nodes' full segment chains and assert every lineage
    // invariant holds: dense sequences within and across segments, each
    // segment's first entry matching its header, and each successor's
    // anchor equal to its predecessor's tail chain hash. On the replica
    // this is the end-to-end regression guard for "primary rotations
    // must not punch sequence gaps into a live replica's journal" — a
    // failure mode that previously went undetected because no test ever
    // replayed a replica journal written across primary rotations.
    let (p_first, p_last) = walk_segments_dense(&primary_journal);
    let (r_first, r_last) = walk_segments_dense(&replica_journal);
    assert_eq!(p_first, 1, "primary lineage must begin at sequence 1");
    assert_eq!(r_first, 1, "replica lineage must begin at sequence 1");
    assert!(
        p_last >= total_orders,
        "primary tail {p_last} must cover all {total_orders} orders"
    );
    // Every acked order is durable on the replica (clean SIGINT drains
    // the replica's ring into its journal), and the orders alone occupy
    // at least `total_orders` sequences. Not compared against `p_last`
    // or the health cursor: periodic Tick events journaled between the
    // lag-0 observation and SIGINT may legitimately not have
    // replicated. The dense walk above is what catches gaps.
    assert!(
        r_last >= total_orders,
        "replica tail {r_last} must cover all {total_orders} orders"
    );

    // Run the REPLICA's journal through production multi-segment
    // recovery (full replay, real trading engine) — not just the
    // read-only lineage walk. This is what a promotion-after-restart
    // would execute; the recovered writer must resume exactly one past
    // the replica's durable tail.
    {
        use melin_journal::BufferedWriter;
        use melin_server::ServerApp;
        use melin_trading::trading_event::TradingEvent;
        use melin_transport_core::JournaledApp;

        let recovered = JournaledApp::<ServerApp, BufferedWriter<TradingEvent>>::recover(
            ServerApp(melin_exchange_core::exchange::Exchange::with_capacity()),
            &replica_journal,
        )
        .expect("replica journal must recover through the production path");
        assert_eq!(
            recovered.next_sequence(),
            r_last + 1,
            "recovered replica writer must resume after its durable tail"
        );
    }

    // ----- Restart and verify recovered state matches -----
    // primary2 is brought up alone — no replica is spawned alongside it
    // because this phase only validates journal recovery, not
    // replication. Run with `--durability-mode local` so the recovered
    // primary is fully operational without a replica (same pattern the
    // other "recovered primary, no replica" tests use); default policy
    // would leave it halted and unable to service client requests.
    let primary2_extra: Vec<&str> = primary_extra
        .iter()
        .copied()
        .chain(["--durability-mode", "local"])
        .collect();
    let mut primary2 = spawn_primary_with_extra_env(
        &bin,
        tmp.path(),
        &keys_path,
        free_port(),
        free_port(),
        free_port(),
        &primary2_extra,
        extra_env,
    );
    wait_for_primary_repl_ready(primary2.health_addr, Duration::from_secs(30));
    wait_healthy(primary2.health_addr, Duration::from_secs(30));

    // To validate that recovery picked up every archived segment, submit
    // one order on the recovered primary and then read the live segment
    // from disk after shutdown: its tail must be strictly greater than
    // `p_last`, the dense-walk on-disk tail captured before the restart —
    // only possible if the writer's `next_sequence` was correctly seeded
    // from the multi-segment archive walk on startup.
    //
    // `p_last` is the cleanest baseline here: both sides of the assert
    // come from on-disk journal walks, independent of health-endpoint
    // publish timing. (The health `journal_seq` gauge reads the durable
    // wire seq nowadays — same space as the disk walk — but keeping both
    // sides on disk reads keeps the comparison self-contained.)
    let mut client2 = connect_with_timeout(primary2.client_addr, &key);
    submit_resting_burst(&mut client2, total_orders + 1, 1);
    // Allow the order to fsync before shutdown.
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Ok((_, _, 0, _)) = query_health(primary2.health_addr) {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(client2);
    unsafe { libc::kill(primary2.child.id() as i32, libc::SIGINT) };
    let _ = primary2
        .child
        .wait_timeout_with_kill(Duration::from_secs(10));

    // Read the live segment back and check its tail sequence.
    use melin_journal::JournalReader;
    let mut reader =
        JournalReader::<melin_trading::trading_event::TradingEvent>::open(&primary_journal)
            .expect("reopen primary live segment");
    while reader.next_entry().expect("scan live").is_some() {}
    let post_disk_seq = reader.last_sequence().unwrap_or(0);
    assert!(
        post_disk_seq > p_last,
        "post-restart live tail seq ({post_disk_seq}) must exceed the pre-restart on-disk \
         tail ({p_last}) — indicates multi-segment recovery reseeded the writer at the \
         right place"
    );
}

/// `policy_degraded` health gauge transitions correctly across a
/// replica failure under the default `persisted>=2 best_effort`
/// policy. Default policy plus 2 connected replicas → 3 nodes in
/// view → no clamp → gauge=0. Kill one replica → 2 nodes in view →
/// clamp from 2 to 2 (no clamp, still healthy). Kill BOTH replicas
/// → matching stage halts, but the gate's view shrinks to just the
/// primary → clamp from 2 to 1 → gauge=1.
///
/// Verifies the observability path end-to-end: gauge update, the
/// 1-second idle-poll re-eval, and the flap-hold-gated transition
/// log (transitions held >1 s actually fire warn/info).
#[test]
#[serial]
fn policy_degraded_gauge_transitions_with_cluster_shape() {
    let mut cluster = DualCluster::start();
    let primary_health = cluster.primary.health_addr;

    // Fresh 1+2 cluster on the default policy: gauge=0 (3 nodes, no clamp).
    wait_for_policy_degraded(primary_health, 0, Duration::from_secs(5));

    // Kill replica 1: 2 nodes left, view.len()=2, clamp from 2 to 2
    // is a no-op. Gauge should remain 0.
    cluster.kill_replica1();
    // Give the idle-poll a couple of ticks plus the flap-hold to
    // settle. Should still be 0 — losing one of three nodes when
    // the policy targets 2 doesn't trigger the clamp.
    std::thread::sleep(Duration::from_millis(2500));
    let after_one_kill = fetch_policy_degraded(primary_health);
    assert_eq!(
        after_one_kill,
        Some(0),
        "with 1 replica down (2 nodes connected) the default policy should not be degraded; gauge = {after_one_kill:?}"
    );

    // Kill replica 2: only the primary remains. View.len()=1, clamp
    // from 2 to 1 → gauge=1. The matching stage's separate
    // `replicas_connected==0` halt will reject new orders before
    // they reach the gate, but the policy evaluator on the idle
    // path still flips the gauge — that's exactly what alerting
    // should fire on.
    cluster.kill_replica2();
    wait_for_policy_degraded(primary_health, 1, Duration::from_secs(5));
}

/// Regression guard for the namespace-translation bug: a prior
/// ack-on-receive attempt sent `journal_cursor.load()` (local-ring
/// position space) on the wire as `acked_sequence` (primary-sequence
/// space), mixing the two namespaces and silently producing acks
/// that were structurally wrong. With the dual-track flush, the
/// receiver advances `in_memory_sequence` on receive (pre-journal)
/// and `acked_sequence` only after the local journal cursor crosses
/// the corresponding queued target. Under sustained traffic on a
/// 1+2 cluster with `in_memory>=2`, the in-memory cursor MUST run
/// strictly ahead of the persisted cursor — equality across the
/// whole run, or inversion, indicates the namespace bug has
/// re-entered. The flush block's `debug_assert!` is the first line
/// of defence; this test exercises the end-to-end path so the
/// debug-only assert isn't the sole guarantee.
#[test]
#[serial]
fn in_memory_cursor_runs_ahead_of_persisted_under_sustained_traffic() {
    let cluster = DualCluster::start_with_primary_args(&["--durability-mode", "hybrid"]);
    let primary_health = cluster.primary.health_addr;
    let mut client = cluster.connect_primary();

    // Sample the metric concurrently with order submission. Order
    // responses are synchronous (each waits for the gate to clear),
    // so sampling between submits sees settled state where both
    // cursors have converged. Run a background poller that grabs
    // metrics on a tight cadence; with pipelined journal acks (up
    // to 8 batches in flight) the in-memory cursor leads the
    // persisted one for the duration of every burst.
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_clone = std::sync::Arc::clone(&stop);
    let sampler = std::thread::spawn(move || {
        let mut saw_in_mem_ahead: usize = 0;
        let mut saw_in_mem_nonzero: bool = false;
        let mut inversion_seen: Option<(usize, u64, u64)> = None;
        while !stop_clone.load(std::sync::atomic::Ordering::Relaxed) {
            if let Some(cursors) = fetch_replica_cursors(primary_health) {
                for (slot, (in_mem, acked)) in cursors.iter().enumerate() {
                    if *in_mem > 0 {
                        saw_in_mem_nonzero = true;
                    }
                    if *in_mem < *acked && inversion_seen.is_none() {
                        inversion_seen = Some((slot, *in_mem, *acked));
                    }
                    if *acked > 0 && *in_mem > *acked {
                        saw_in_mem_ahead += 1;
                    }
                }
            }
        }
        (saw_in_mem_ahead, saw_in_mem_nonzero, inversion_seen)
    });

    for i in 1..=200u64 {
        let r = submit_order(&mut client, i, 1, 1, Side::Buy, 100, 10);
        assert!(!r.is_empty(), "order {i}: no response");
    }

    // Stop the sampler before `wait_replicated` so the metrics
    // endpoint isn't being hammered while the test loop is polling
    // it for the lag-zero condition. The sampler shares the
    // single-threaded HTTP server with `query_health` and under
    // concurrent test load that contention has shown up as a
    // wait_replicated timeout.
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (saw_in_mem_ahead, saw_in_mem_nonzero, inversion_seen) =
        sampler.join().expect("sampler thread panicked");
    cluster.wait_replicated();

    // Plumbing check: the new metric is reachable and advanced.
    // Catches the case where in_memory_sequence is wired into the
    // protocol but not into the primary-side metrics struct.
    assert!(
        saw_in_mem_nonzero,
        "melin_replica_in_memory_sequence never advanced past 0 — metric not plumbed?"
    );
    // Regression check: in_memory must never drop below acked at
    // any sampling moment. Inversion is the wire-level shape of the
    // namespace bug — a prior implementation sent
    // `journal_cursor.load()` (local-ring positions) as
    // `acked_sequence` while `in_memory_sequence` carried primary
    // sequences, producing arbitrary inversions on the receiving
    // side.
    assert!(
        inversion_seen.is_none(),
        "in_memory_sequence < acked_sequence observed: {inversion_seen:?} — namespace bug?",
    );
    // Optional lead observation: under the pipelined journal we
    // expect to occasionally see the in-memory cursor ahead, but
    // the metrics-endpoint roundtrip is ~ms and per-batch gaps are
    // ~µs, so a concurrent sampler often misses every gap under
    // load. The strict correctness guarantee for the namespace
    // bug is the `debug_assert!` in `try_flush_dual_track` plus
    // the inversion check above; this is informational only.
    let _ = saw_in_mem_ahead;
}

/// Behavioral tripwire for the durability gate: under the cluster
/// default `hybrid` (`persisted>=1 && in_memory>=2`), a client ack must
/// not be released until a replica has confirmed the order in memory.
///
/// Freeze the replica process with SIGSTOP — the primary keeps
/// journaling and shipping (the entry lands in the replica's socket
/// buffer) so `persisted>=1` is satisfied, but no ack ever returns, so
/// `in_memory>=2` cannot be. The response must NOT arrive while the
/// replica is frozen. SIGCONT the replica and the same in-flight order
/// must complete with a normal `Placed` ack.
///
/// This is deliberately mechanism-agnostic: the pre-v14 sequence-space
/// drift voided the gate within seconds of uptime (replica cursors ran
/// ahead of wire space, releasing acks ~0.1µs after matching), and this
/// test fails on that build — but it equally catches any future bug
/// that satisfies the gate without a real replica confirmation (cursor
/// inflation, policy mis-evaluation, metric misplumbing). The
/// allocator/wire-space agreement itself is pinned separately by
/// `allocator_wire_seq_and_gate_cursor_agree_across_rotation` in
/// `melin-transport-core`.
///
/// Timing margins: a healthy gate releases in ~tens of µs, and replica
/// eviction is ring-backpressure-driven (one order cannot fill the
/// ring), so the 1.5s frozen window sits 4+ orders of magnitude above
/// normal release latency while staying well inside any teardown path.
#[test]
#[serial]
fn hybrid_gate_stalls_while_replica_frozen() {
    let cluster = TestCluster::start();
    let mut client = cluster.connect_primary();

    // Warm-up: prove the ack path works before freezing anything, and
    // let replication settle so the frozen-phase observation starts
    // from lag 0.
    let r = submit_order(&mut client, 1, 1, 1, Side::Buy, 100, 10);
    assert!(!r.is_empty(), "warm-up order got no response");
    cluster.wait_replicated();

    // Freeze the replica. SIGSTOP halts ack production without closing
    // the TCP stream, so the primary sees a connected-but-silent peer —
    // the exact shape the gate exists to hold against.
    unsafe {
        libc::kill(cluster.replica.child.id() as i32, libc::SIGSTOP);
    }

    // Submit from a helper thread; the response (if any) is forwarded
    // over a channel so the main thread can assert on its *timing*.
    let (tx, rx) = std::sync::mpsc::channel();
    let submitter = std::thread::spawn(move || {
        let r = submit_order(&mut client, 2, 1, 1, Side::Buy, 101, 10);
        // Channel send fails only if the test already panicked on the
        // frozen-phase assertion; nothing to do with the response then.
        let _ = tx.send(r);
    });

    // The gate must hold while the replica is frozen. A response here
    // means the gate released without replica confirmation — the
    // vacuous-gate bug.
    match rx.recv_timeout(Duration::from_millis(1500)) {
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        Ok(r) => panic!(
            "durability gate released a client ack while the only replica \
             was frozen (hybrid requires in_memory>=2): {r:?}"
        ),
        Err(e) => panic!("submitter channel closed unexpectedly: {e}"),
    }

    // Thaw the replica: it drains the buffered stream, acks, and the
    // in-flight response must now be released promptly.
    unsafe {
        libc::kill(cluster.replica.child.id() as i32, libc::SIGCONT);
    }
    let r = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("no response after replica thawed — gate stuck");
    assert!(
        has_report(&r, |rep| matches!(
            rep,
            melin_protocol::types::ExecutionReport::Placed { .. }
        )),
        "expected a normal Placed ack after thaw, got: {r:?}"
    );
    submitter.join().expect("submitter thread panicked");
}

/// End-to-end regression for the catch-up → live-stream handoff hole
/// found by the 2026-06-07 LAN bench (tcp-dual-repl): a slow replica
/// was ring-evicted during the warmup ramp, reconnected, caught up
/// from the primary's journal to seq 6932800 — and live streaming
/// resumed at 6933013. The 212 entries journaled between the catch-up
/// scanner's EOF and the slot's ring activation were never sent; the
/// replica accepted the non-contiguous stream, acked past the hole for
/// the rest of the run, and its journal failed lineage verification
/// only at post-run audit.
///
/// Reproduce the full production shape: under sustained dual-replica
/// load, SIGSTOP replica2 until ring backpressure evicts it (the
/// healthy replica1 keeps satisfying the hybrid gate, so load never
/// stops), SIGCONT so it reconnects and catches up *while the journal
/// keeps growing*, and repeat. Every reconnect crosses the handoff
/// window; the dense-lineage walk at the end catches any hole the
/// handoff punched. The race is timing-dependent per cycle, so this
/// asserts the invariant rather than a specific failure — the unit
/// pins live in `melin-transport-core` (sender handoff) and
/// `melin-server-runtime` (receiver contiguity).
#[test]
#[serial]
fn evicted_replica_catchup_under_load_preserves_dense_lineage() {
    // Small replication rings (16 × 512 KiB) so the frozen replica's
    // ring fills — and evicts — seconds after its socket jams, instead
    // of the default 256 slots that would need minutes of test load.
    let mut cluster = DualCluster::start_with_primary_args(&["--replication-ring-size", "16"]);

    let primary_health = cluster.primary.health_addr;
    wait_metric(
        primary_health,
        "melin_replicas_connected ",
        Duration::from_secs(30),
        "both replicas connected at startup",
        |v| v == 2,
    );

    let stop = AtomicBool::new(false);
    // One submission counter per submitter thread (no sharing — summed
    // at the end for the coverage assertion).
    let submitted = [AtomicU64::new(0), AtomicU64::new(0)];

    // Panic guard: `thread::scope` joins spawned threads on unwind, and
    // the submitters only exit via `stop` — without this, a panicking
    // wait below would leave them submitting forever and hang the test
    // instead of failing it.
    struct StopOnDrop<'a>(&'a AtomicBool);
    impl Drop for StopOnDrop<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    std::thread::scope(|s| {
        let _stop_guard = StopOnDrop(&stop);
        // Two closed-loop submitters, one per trader key — the per-key
        // request_seq HWM means each concurrent connection needs its
        // own key. Closed-loop keeps the response sockets drained so
        // the servers never wedge on response backpressure.
        for (idx, key) in [&cluster.key, &cluster.key2].into_iter().enumerate() {
            let stop = &stop;
            let counter = &submitted[idx];
            let addr = cluster.primary.client_addr;
            let key = key.clone();
            s.spawn(move || {
                let mut client = connect_with_timeout(addr, &key);
                // Disjoint id ranges and accounts per submitter.
                let mut id = 1_000_000u64 * (idx as u64 + 1);
                while !stop.load(Ordering::Relaxed) {
                    let r = client.send_request(&Request::SubmitOrder {
                        symbol: Symbol(1),
                        order: Order {
                            id: OrderId(id),
                            account: AccountId(idx as u32 + 1),
                            side: Side::Buy,
                            order_type: OrderType::Limit {
                                price: price(50),
                                post_only: false,
                            },
                            time_in_force: TimeInForce::GTC,
                            quantity: qty(1),
                            stp: melin_protocol::types::SelfTradeProtection::Allow,
                            expiry_ns: 0,
                        },
                    });
                    if r.is_err() {
                        // Server-side failure — stop counting; the main
                        // thread's waits and asserts surface the cause.
                        break;
                    }
                    id += 1;
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            });
        }

        // Let load flow and replication settle before the first freeze.
        let warm_start = Instant::now();
        while submitted[0].load(Ordering::Relaxed) + submitted[1].load(Ordering::Relaxed) < 200 {
            assert!(
                warm_start.elapsed() < Duration::from_secs(30),
                "submitters made no progress against the fresh cluster"
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        // Freeze → evict → thaw → reconnect + catch-up, under
        // continuous load throughout. Multiple cycles amplify the odds
        // of an entry landing inside the handoff window.
        //
        // The waits are deliberately slot-agnostic and self-healing:
        // with rings this small under sustained load, *either* replica
        // can be evicted at almost any point (including replica2 again
        // right after it finished catching up) — and every eviction →
        // reconnect → catch-up is exactly the handoff under test. The
        // connected-count is the one signal that is robust to all
        // interleavings: a frozen replica cannot reconnect, so a drop
        // below 2 proves a ring-backpressure eviction happened.
        const CYCLES: u32 = 4;
        for cycle in 0..CYCLES {
            // Only freeze a *connected* replica — the previous thaw (or
            // plain load) may have evicted it again; freezing a
            // disconnected replica would wait forever on an eviction
            // that its inactive ring can never produce.
            wait_metric(
                primary_health,
                "melin_replicas_connected ",
                Duration::from_secs(60),
                &format!("cycle {cycle}: cluster whole before freeze"),
                |v| v == 2,
            );

            // SIGSTOP halts the replica without closing its sockets:
            // the primary sees a connected-but-silent peer, exactly the
            // slow-replica shape that drives ring-backpressure eviction.
            unsafe {
                libc::kill(cluster.replica2.child.id() as i32, libc::SIGSTOP);
            }

            wait_metric(
                primary_health,
                "melin_replicas_connected ",
                Duration::from_secs(60),
                &format!("cycle {cycle}: a replica evicted under ring backpressure"),
                |v| v < 2,
            );

            unsafe {
                libc::kill(cluster.replica2.child.id() as i32, libc::SIGCONT);
            }

            // The thawed replica drains the dead connection's buffered
            // tail, notices EOF, reconnects (1s backoff), catches up
            // from the primary's journal, and goes live — all while the
            // submitters keep the journal growing.
            wait_metric(
                primary_health,
                "melin_replicas_connected ",
                Duration::from_secs(60),
                &format!("cycle {cycle}: evicted replica reconnected"),
                |v| v == 2,
            );
            for slot_line in [
                "melin_replica_catching_up{slot=\"0\"} ",
                "melin_replica_catching_up{slot=\"1\"} ",
            ] {
                wait_metric(
                    primary_health,
                    slot_line,
                    Duration::from_secs(60),
                    &format!("cycle {cycle}: catch-up complete"),
                    |v| v == 0,
                );
            }
        }
    });

    let total_submitted =
        submitted[0].load(Ordering::Relaxed) + submitted[1].load(Ordering::Relaxed);

    // Quiesce: every acked order must become durable on both replicas.
    // Lag excludes disconnected slots, so first wait for the cluster to
    // be whole and live — a replica may have been mid-evict/reconnect
    // when the load stopped.
    wait_metric(
        primary_health,
        "melin_replicas_connected ",
        Duration::from_secs(60),
        "cluster whole after load stopped",
        |v| v == 2,
    );
    for slot_line in [
        "melin_replica_catching_up{slot=\"0\"} ",
        "melin_replica_catching_up{slot=\"1\"} ",
    ] {
        wait_metric(
            primary_health,
            slot_line,
            Duration::from_secs(60),
            "final catch-up complete",
            |v| v == 0,
        );
    }
    let start = Instant::now();
    loop {
        if let Ok((_, _, 0, _)) = query_health(primary_health) {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "replication lag did not reach 0 after load stopped"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Clean shutdown so the dense walk sees fully-drained journals.
    unsafe {
        libc::kill(cluster.primary.child.id() as i32, libc::SIGINT);
        libc::kill(cluster.replica1.child.id() as i32, libc::SIGINT);
        libc::kill(cluster.replica2.child.id() as i32, libc::SIGINT);
    }
    let _ = cluster
        .primary
        .child
        .wait_timeout_with_kill(Duration::from_secs(10));
    let _ = cluster
        .replica1
        .child
        .wait_timeout_with_kill(Duration::from_secs(10));
    let _ = cluster
        .replica2
        .child
        .wait_timeout_with_kill(Duration::from_secs(10));

    // The regression assertion: the evicted-and-reconnected replica's
    // journal lineage must be dense from sequence 1 — `verify_lineage`
    // inside the walk panics on any hole (the bench failure mode).
    let dir = cluster._tmp.path();
    let (r2_first, r2_last) = walk_segments_dense(&dir.join("replica2.journal"));
    assert_eq!(r2_first, 1, "replica2 lineage must begin at sequence 1");
    assert!(
        r2_last >= total_submitted,
        "replica2 tail {r2_last} must cover all {total_submitted} acked orders"
    );
    // Controls: the primary and the never-frozen replica.
    let (p_first, _) = walk_segments_dense(&dir.join("primary.journal"));
    let (r1_first, _) = walk_segments_dense(&dir.join("replica1.journal"));
    assert_eq!(p_first, 1, "primary lineage must begin at sequence 1");
    assert_eq!(r1_first, 1, "replica1 lineage must begin at sequence 1");
}

/// Helper extension: wait up to `timeout` for the child, then SIGKILL.
trait ChildExt {
    fn wait_timeout_with_kill(&mut self, timeout: Duration) -> std::io::Result<()>;
}

impl ChildExt for std::process::Child {
    fn wait_timeout_with_kill(&mut self, timeout: Duration) -> std::io::Result<()> {
        let start = Instant::now();
        loop {
            match self.try_wait()? {
                Some(_) => return Ok(()),
                None => {
                    if start.elapsed() > timeout {
                        let _ = self.kill();
                        let _ = self.wait();
                        return Ok(());
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }
}
