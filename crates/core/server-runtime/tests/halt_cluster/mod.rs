//! A primary and its replicas for the halt tests (`halt_refusal.rs`,
//! `degraded_acks.rs`): the counter application, started through
//! `melin-test-node` (kernel TCP by default, DPDK under its feature and
//! the netns runner), plus the probes those tests read the node through.

// Each test binary that includes this module uses part of it.
#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, GET_VALUE_REQUEST, KIND_RESP_VALUE, RequestDecoder, ResponseEncoder,
};
use ed25519_dalek::SigningKey;
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;

/// Port range for `free_addr`, shared with `replicated_failover.rs`: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because every binary using it is in nextest's `cluster-serial` group,
/// so they never run at the same time.
pub const PORT_BASE: u16 = 10_000;

/// A free loopback address from this range, for an endpoint that is
/// kernel TCP on either transport (health, admin).
pub fn free() -> SocketAddr {
    free_addr(PORT_BASE)
}

/// Node `slot`'s addresses (`melin_test_node::addrs`), from this file's
/// port range on kernel TCP.
pub fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, || free_addr(PORT_BASE))
}

pub fn spawn_node(addrs: &Addrs, config: ServerConfig) -> Node {
    melin_test_node::start_at::<Counter>(
        addrs,
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

/// The keys and files a cluster test shares: one replication key for the
/// primary and one for its replicas, and one operator key, which serves
/// both as a client and on the admin endpoint.
pub struct Fixture {
    pub tmp: tempfile::TempDir,
    pub auth_path: PathBuf,
    pub primary_key: SigningKey,
    pub replica_key: SigningKey,
    pub client_key: SigningKey,
}

impl Fixture {
    /// Keys derived from `seed`, so two tests in one binary do not share
    /// them.
    pub fn new(seed: u8) -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let primary_key = SigningKey::from_bytes(&[seed; 32]);
        let replica_key = SigningKey::from_bytes(&[seed.wrapping_add(1); 32]);
        let client_key = SigningKey::from_bytes(&[seed.wrapping_add(2); 32]);
        let b64 = |k: &SigningKey| {
            base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
        };
        let auth_path = tmp.path().join("authorized_keys");
        std::fs::write(
            &auth_path,
            format!(
                "replication {} primary\nreplication {} replica\noperator {} client\n",
                b64(&primary_key),
                b64(&replica_key),
                b64(&client_key)
            ),
        )
        .expect("write authorized_keys");
        Self {
            tmp,
            auth_path,
            primary_key,
            replica_key,
            client_key,
        }
    }

    /// The configuration every node of these tests shares: journal `name`,
    /// the policy, no ticks, no snapshots, unpinned (every node shares this
    /// process with the test's own clients), no health endpoint.
    pub fn node_config(&self, name: &str, key: &SigningKey, policy: AckPolicy) -> ServerConfig {
        let key_path = self.tmp.path().join(format!("{name}.key"));
        std::fs::write(&key_path, key.to_bytes()).expect("write node key");
        ServerConfig {
            journal: self.tmp.path().join(format!("{name}.journal")),
            authorized_keys: self.auth_path.clone(),
            ack_policy: policy,
            cores: PipelineCores::unpinned(),
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            health_bind: None,
            replication_key: Some(key_path),
            ..melin_test_node::config()
        }
    }

    /// A primary at `addrs`, taking replicas on its replication address,
    /// with the health endpoint the tests read it through.
    pub fn primary_config(&self, addrs: &Addrs, policy: AckPolicy) -> ServerConfig {
        let mut config = self.node_config("primary", &self.primary_key, policy);
        config.replication_bind = Some(addrs.replication());
        config.health_bind = Some(free_addr(PORT_BASE));
        config
    }

    /// A replica of the primary at `primary`, journaling to `name`. No
    /// health endpoint: the tests read the primary's, and a port saved
    /// per replica keeps a binary run in one process (plain `cargo test`)
    /// inside its port block.
    pub fn replica_config(&self, name: &str, primary: &Addrs, policy: AckPolicy) -> ServerConfig {
        let mut config = self.node_config(name, &self.replica_key, policy);
        config.replica_of = Some(primary.replication());
        config
    }

    /// A client of `node`, connected within the startup limit.
    pub fn client(&self, node: &Node) -> Connection {
        let deadline = Instant::now() + Duration::from_secs(30);
        Connection::connect_by(node.addr(), &self.client_key, deadline)
            .expect("client connects to the node")
    }
}

/// The metrics body of a node's health endpoint, or `None` while it is
/// not up.
fn metrics(health: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&health, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    Some(body)
}

/// The value of one unlabelled Prometheus series on a node's health
/// endpoint, or `None` while the endpoint is not up.
pub fn gauge(health: SocketAddr, name: &str) -> Option<u64> {
    metrics(health)?.lines().find_map(|line| {
        line.strip_prefix(name)?
            .strip_prefix(' ')?
            .trim()
            .parse()
            .ok()
    })
}

/// The sum of every sample of a labelled Prometheus series (one per
/// replica slot, say), or `None` while the endpoint is not up.
pub fn series_sum(health: SocketAddr, name: &str) -> Option<u64> {
    Some(
        metrics(health)?
            .lines()
            .filter_map(|line| line.strip_prefix(name)?.strip_prefix('{'))
            .filter_map(|rest| rest.rsplit_once(' ')?.1.trim().parse::<u64>().ok())
            .sum(),
    )
}

pub fn wait_for_gauge(health: SocketAddr, name: &str, value: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while gauge(health, name) != Some(value) {
        assert!(
            Instant::now() < deadline,
            "{name} never reached {value} (last: {:?})",
            gauge(health, name)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Wait until the one replica of `primary_health`, attached to a primary
/// whose journal is not empty, is streaming live: connected, past its
/// catch-up, and acknowledging the primary's whole journal. A replica
/// authenticates (the count) before it catches up, and only a streaming
/// one clears the operator's halt override, so a test that means to stop
/// a streaming replica waits for this rather than for the count alone.
pub fn wait_until_streaming(primary_health: SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let connected = gauge(primary_health, "melin_replicas_connected");
        let catching_up = series_sum(primary_health, "melin_replica_catching_up");
        let acked = series_sum(primary_health, "melin_replica_acked_sequence");
        let journal = gauge(primary_health, "melin_journal_sequence");
        if connected == Some(1)
            && catching_up == Some(0)
            && journal.is_some_and(|j| j > 0 && acked.is_some_and(|a| a >= j))
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no replica started streaming (connected {connected:?}, catching up \
             {catching_up:?}, acked {acked:?}, journal {journal:?})"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// One admin command, as an operator sends it: authenticate, a line in,
/// a line out.
pub fn admin(addr: SocketAddr, key: &SigningKey, command: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    let conn = Connection::connect_by(addr, key, deadline).expect("admin connects");
    let mut stream = conn.into_stream();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set the admin read timeout");
    stream
        .write_all(format!("{command}\n").as_bytes())
        .expect("send the admin command");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("read the admin reply");
    line.trim_end().to_owned()
}

/// Send one request and return the single frame of its reply.
pub fn one_reply(conn: &mut Connection, body: &[u8]) -> Vec<u8> {
    conn.request_one(body)
        .unwrap_or_else(|e| panic!("request {body:02x?} failed: {e}"))
}

pub fn value_of(conn: &mut Connection) -> u64 {
    let reply = one_reply(conn, &GET_VALUE_REQUEST);
    assert_eq!(reply[0], KIND_RESP_VALUE);
    u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"))
}

/// A replication link the test can break the way a crash or a partition
/// does: a TCP relay between a replica and its primary, bound where the
/// nodes reach a test's own sockets (`melin_test_node::local_ip`), so
/// the same test runs on both transports. The replica takes
/// [`Proxy::addr`] as its `replica_of`.
///
/// - [`freeze`](Proxy::freeze): nothing is relayed either way, as on a
///   link gone silent: the primary's entries stop reaching the replica
///   and its acks stop coming back, so the writes in flight stay
///   unconfirmed.
/// - [`cut`](Proxy::cut): every relayed connection is closed, and new
///   ones are refused, so the primary loses the replica for good and the
///   replica's reconnects fail.
/// - [`restore`](Proxy::restore): new connections are relayed again; the
///   replica gets back in on its next reconnect.
pub struct Proxy {
    addr: SocketAddr,
    /// `FORWARDING`, `FROZEN` or `CUT`.
    mode: std::sync::Arc<std::sync::atomic::AtomicU8>,
    /// Bumped by every cut: a relay serves only the generation it was
    /// opened in.
    generation: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

const FORWARDING: u8 = 0;
const FROZEN: u8 = 1;
const CUT: u8 = 2;

impl Proxy {
    /// Relay connections to `upstream`, on a thread that runs as long as
    /// the test process.
    pub fn start(upstream: SocketAddr) -> Self {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};

        let listener =
            std::net::TcpListener::bind((melin_test_node::local_ip(), 0)).expect("bind the proxy");
        let addr = listener.local_addr().expect("the proxy's address");
        let mode = Arc::new(AtomicU8::new(FORWARDING));
        let generation = Arc::new(AtomicU64::new(0));
        let (accept_mode, accept_generation) = (Arc::clone(&mode), Arc::clone(&generation));
        std::thread::spawn(move || {
            for downstream in listener.incoming() {
                let Ok(downstream) = downstream else {
                    continue;
                };
                if accept_mode.load(Ordering::Acquire) == CUT {
                    // Refused: dropped unanswered, as a dead host's port.
                    continue;
                }
                let Ok(upstream) = TcpStream::connect(upstream) else {
                    continue;
                };
                let born = accept_generation.load(Ordering::Acquire);
                for (from, to) in [
                    (downstream.try_clone(), upstream.try_clone()),
                    (upstream.try_clone(), downstream.try_clone()),
                ] {
                    let (Ok(from), Ok(to)) = (from, to) else {
                        continue;
                    };
                    let (mode, generation) =
                        (Arc::clone(&accept_mode), Arc::clone(&accept_generation));
                    std::thread::spawn(move || relay(from, to, &mode, &generation, born));
                }
            }
        });
        Self {
            addr,
            mode,
            generation,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn freeze(&self) {
        self.mode
            .store(FROZEN, std::sync::atomic::Ordering::Release);
    }

    pub fn cut(&self) {
        self.mode.store(CUT, std::sync::atomic::Ordering::Release);
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    pub fn restore(&self) {
        self.mode
            .store(FORWARDING, std::sync::atomic::Ordering::Release);
    }
}

/// Copy `from` to `to` until either closes or the proxy cuts the
/// generation the relay was opened in; hold the bytes while frozen.
fn relay(
    mut from: TcpStream,
    mut to: TcpStream,
    mode: &std::sync::atomic::AtomicU8,
    generation: &std::sync::atomic::AtomicU64,
    born: u64,
) {
    use std::sync::atomic::Ordering;

    // A short timeout, so a cut is noticed on an idle link.
    if from
        .set_read_timeout(Some(Duration::from_millis(10)))
        .is_err()
    {
        return;
    }
    let mut buf = [0u8; 64 * 1024];
    loop {
        if generation.load(Ordering::Acquire) != born {
            // Both directions of the pair go: a cut link carries nothing.
            // Best-effort: either end may already be gone.
            let _ = from.shutdown(std::net::Shutdown::Both);
            let _ = to.shutdown(std::net::Shutdown::Both);
            return;
        }
        if mode.load(Ordering::Acquire) == FROZEN {
            std::thread::sleep(Duration::from_millis(5));
            continue;
        }
        match from.read(&mut buf) {
            Ok(0) => {
                // Best-effort, as above.
                let _ = to.shutdown(std::net::Shutdown::Write);
                return;
            }
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return,
        }
    }
}
