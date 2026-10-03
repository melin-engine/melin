//! The sizing contract, end to end: what a node passes as its
//! application's sizing reaches `Application::prefault` on that node, on
//! every instance the node builds, before the instance serves — and
//! never anywhere else. A replica gets its own sizing, not the
//! primary's, before the first streamed event is applied; a node
//! recovering a journal, primary or replica, gets it before the replay,
//! so nothing is replayed into unsized collections, and again on the
//! recovered state. A new primary is one of those: its journal is
//! created with the genesis in it and then recovered.
//!
//! A primary and one replica (a counter wrapped to observe its sizing,
//! `disk` ack policy) over real TCP, then both restarted on their own
//! journals.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-transparent-tests.md`).

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, CounterEvent, CounterQuery, CounterReport, GET_VALUE_REQUEST, KIND_RESP_ACK,
    KIND_RESP_VALUE, RequestDecoder, ResponseEncoder, increment_request,
};
use ed25519_dalek::SigningKey;
use melin_app::{Application, ApplyCtx, QueryCtx, RejectReason};
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;

/// Port range for `free_addr`, shared with the other cluster tests: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because they are all in nextest's `cluster-serial` group, so they never
/// run at the same time.
const PORT_BASE: u16 = 10_000;

// ---------------------------------------------------------------------------
// An application that reports how it was sized
// ---------------------------------------------------------------------------

/// One `prefault` call as the application saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sized {
    /// The node's sizing, as passed.
    reserve_for: u64,
    /// The counter's value when the call came: what state the instance
    /// held at that moment.
    value: u64,
}

/// A node's sizing: what to reserve for, plus where the application
/// reports every call. The log is the test's only view into the node, so
/// the probe rides on the sizing itself — the one thing the runtime hands
/// to `prefault`.
#[derive(Clone)]
struct Sizing {
    reserve_for: u64,
    log: Arc<Mutex<Vec<Sized>>>,
}

impl Sizing {
    fn new(reserve_for: u64) -> Self {
        Sizing {
            reserve_for,
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<Sized> {
        self.log.lock().expect("probe log").clone()
    }
}

/// The counter, with its sizing observed. Same events and same wire
/// codec, so the counter's decoder and encoder serve it unchanged.
#[derive(Default)]
struct SizedCounter(Counter);

impl SizedCounter {
    fn value(&self) -> u64 {
        // The counter keeps its value private; its snapshot is the value
        // in little-endian.
        let mut buf = Vec::new();
        self.0.snapshot(&mut buf).expect("snapshot to a vec");
        u64::from_le_bytes(buf[..8].try_into().expect("8-byte value"))
    }
}

impl Application for SizedCounter {
    type Event = CounterEvent;
    type Report = CounterReport;
    type QueryResponse = CounterQuery;
    type Sizing = Sizing;
    const APP_VERSION: u16 = Counter::APP_VERSION;

    fn apply(&mut self, event: Self::Event, ctx: &ApplyCtx, out: &mut Vec<Self::Report>) {
        self.0.apply(event, ctx, out)
    }

    fn query(&self, event: Self::Event, ctx: &QueryCtx) -> Option<Self::QueryResponse> {
        self.0.query(event, ctx)
    }

    fn tick(&mut self, now_ns: u64, out: &mut Vec<Self::Report>) {
        self.0.tick(now_ns, out)
    }

    fn build_reject(event: &Self::Event, reason: RejectReason) -> Self::Report {
        Counter::build_reject(event, reason)
    }

    fn snapshot<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        self.0.snapshot(w)
    }

    fn restore<R: std::io::Read>(r: &mut R) -> std::io::Result<Self> {
        Counter::restore(r).map(SizedCounter)
    }

    fn prefault(&mut self, sizing: &Self::Sizing) {
        sizing.log.lock().expect("probe log").push(Sized {
            reserve_for: sizing.reserve_for,
            value: self.value(),
        });
    }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn spawn_node(
    addrs: &Addrs,
    config: ServerConfig,
    startup: StartupEvents<CounterEvent>,
    sizing: &Sizing,
) -> Node {
    melin_test_node::start_at::<SizedCounter>(
        addrs,
        config,
        startup,
        sizing.clone(),
        RequestDecoder,
        ResponseEncoder,
    )
}

/// The value of one Prometheus gauge on a node's health endpoint, or `None`
/// while the endpoint is not up.
fn gauge(health: SocketAddr, name: &str) -> Option<u64> {
    let mut stream = TcpStream::connect_timeout(&health, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    body.lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse().ok())
}

fn wait_for_gauge(health: SocketAddr, name: &str, value: u64) {
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

fn connect(addr: SocketAddr, key: &SigningKey) -> Connection {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = Connection::connect_by(addr, key, deadline).expect("client connects");
    conn.set_read_timeout(Duration::from_secs(30))
        .expect("set timeout");
    conn
}

fn value_of(conn: &mut Connection) -> u64 {
    let reply = conn.request_one(&GET_VALUE_REQUEST).expect("query");
    assert_eq!(reply[0], KIND_RESP_VALUE);
    u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"))
}

fn increment(conn: &mut Connection, amount: u64) {
    let reply = conn
        .request_one(&increment_request(amount))
        .expect("increment");
    assert_eq!(reply[0], KIND_RESP_ACK, "the increment is acked");
}

const GENESIS: u64 = 1_000;
const PRIMARY_RESERVE: u64 = 4_096;
const REPLICA_RESERVE: u64 = 512;
const RESTART_RESERVE: u64 = 65_536;

#[test]
fn every_node_sizes_its_own_instances_before_serving() {
    // Opt-in diagnostics, as in `replicated_failover.rs`: with RUST_LOG
    // set, the nodes' tracing output says which recovery and reconnect
    // path each took. No-op when RUST_LOG is unset.
    if std::env::var_os("RUST_LOG").is_some() {
        // Error dropped deliberately: try_init fails only when a
        // subscriber is already installed, which is the state we want.
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let primary_key = SigningKey::from_bytes(&[0x81; 32]);
    let replica_key = SigningKey::from_bytes(&[0x82; 32]);
    let client_key = SigningKey::from_bytes(&[0x83; 32]);

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

    let node_config = |name: &str, key: &SigningKey| -> ServerConfig {
        let key_path = tmp.path().join(format!("{name}.key"));
        std::fs::write(&key_path, key.to_bytes()).expect("write node key");
        ServerConfig {
            journal: tmp.path().join(format!("{name}.journal")),
            authorized_keys: auth_path.clone(),
            ack_policy: AckPolicy::Disk,
            no_mlock: true,
            // Unpinned, and therefore yielding: both nodes share this
            // process with the test's own client.
            cores: PipelineCores::unpinned(),
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            health_bind: Some(free_addr(PORT_BASE)),
            replication_key: Some(key_path),
            ..ServerConfig::default()
        }
    };
    let genesis = || StartupEvents {
        genesis: vec![CounterEvent::Increment { amount: GENESIS }],
        on_primary: Vec::new(),
    };

    let primary_addrs = melin_test_node::addrs(0, || free_addr(PORT_BASE));
    let replica_addrs = melin_test_node::addrs(1, || free_addr(PORT_BASE));
    let mut primary_config = node_config("primary", &primary_key);
    primary_config.replication_bind = Some(primary_addrs.replication());
    let primary_health = primary_config.health_bind.expect("set above");
    let mut replica_config = node_config("replica", &replica_key);
    replica_config.replica_of = Some(primary_addrs.replication());

    let primary_sizing = Sizing::new(PRIMARY_RESERVE);
    let primary = spawn_node(&primary_addrs, primary_config, genesis(), &primary_sizing);
    let replica_sizing = Sizing::new(REPLICA_RESERVE);
    let replica = spawn_node(&replica_addrs, replica_config, genesis(), &replica_sizing);

    // --- The primary served, sized with its own sizing as a recovering
    // primary is: its new journal is created with the genesis in it, so
    // the genesis reaches the state by replay — sized on the genesis
    // instance before that replay, then again on the result. ---
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let mut conn = connect(primary.addr(), &client_key);
    assert_eq!(value_of(&mut conn), GENESIS, "genesis applied");
    assert_eq!(
        primary_sizing.calls(),
        vec![
            Sized {
                reserve_for: PRIMARY_RESERVE,
                value: 0,
            },
            Sized {
                reserve_for: PRIMARY_RESERVE,
                value: GENESIS,
            },
        ],
        "a new primary is sized before its genesis is applied and again before it serves"
    );

    // --- The replica streamed both events: sized once, with its own
    // sizing (not the primary's), before the first streamed event was
    // applied. Under `disk` the primary acks without the replica, and
    // counts it connected from the handshake, before its pipeline even
    // exists — so wait until the replica has journaled everything (it
    // acks only after its fsync) before reading its log or stopping it,
    // or the restart below would recover an empty journal. ---
    increment(&mut conn, 5);
    wait_for_gauge(
        primary_health,
        "melin_replica_acked_sequence{slot=\"0\"}",
        2,
    );
    assert_eq!(
        replica_sizing.calls(),
        vec![Sized {
            reserve_for: REPLICA_RESERVE,
            value: 0,
        }],
        "the replica is sized with its own sizing, before it applies the stream"
    );
    drop(conn);
    replica.stop();
    primary.stop();
    assert_eq!(
        primary_sizing.calls().len(),
        2,
        "nothing sizes the primary again while it serves"
    );

    // --- Both nodes restarted on their own journals, each with a new
    // sizing: sized before the replay, on the genesis state, so the
    // history lands in reserved collections; then again on the recovered
    // state — the primary at boot, the replica when its pipeline is
    // built on its first session — as a snapshot restart would be. ---
    let primary_addrs = melin_test_node::addrs(0, || free_addr(PORT_BASE));
    let replica_addrs = melin_test_node::addrs(1, || free_addr(PORT_BASE));
    let mut primary_config = node_config("primary", &primary_key);
    primary_config.replication_bind = Some(primary_addrs.replication());
    let primary_health = primary_config.health_bind.expect("set above");
    let mut replica_config = node_config("replica", &replica_key);
    replica_config.replica_of = Some(primary_addrs.replication());

    let primary_sizing = Sizing::new(RESTART_RESERVE);
    let primary = spawn_node(&primary_addrs, primary_config, genesis(), &primary_sizing);
    let replica_sizing = Sizing::new(RESTART_RESERVE + 1);
    let replica = spawn_node(&replica_addrs, replica_config, genesis(), &replica_sizing);
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let mut conn = connect(primary.addr(), &client_key);
    let replayed = value_of(&mut conn);
    drop(conn);
    replica.stop();
    primary.stop();

    assert_eq!(replayed, GENESIS + 5, "the journal replayed");
    let recovered = |reserve_for: u64| {
        vec![
            Sized {
                reserve_for,
                value: 0,
            },
            Sized {
                reserve_for,
                value: GENESIS + 5,
            },
        ]
    };
    assert_eq!(
        primary_sizing.calls(),
        recovered(RESTART_RESERVE),
        "a recovering primary is sized before the replay and on the recovered state, \
         with the sizing it was restarted with"
    );
    assert_eq!(
        replica_sizing.calls(),
        recovered(RESTART_RESERVE + 1),
        "a recovering replica is sized before the replay and on the recovered state, \
         with the sizing it was restarted with"
    );
}
