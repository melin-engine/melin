//! The sizing contract, end to end: what a node passes as its
//! application's sizing reaches `Application::prefault` on that node, on
//! every instance the node builds, before the instance serves — and
//! never anywhere else. A primary gets its own sizing once at boot, on
//! the state it starts from; a replica gets its own, not the primary's,
//! before the first streamed event is applied; a node recovering a
//! journal, primary or replica, gets it before the replay, so nothing is
//! replayed into unsized collections, and again on the recovered state.
//!
//! A primary and one replica (a counter wrapped to observe its sizing,
//! `disk` ack policy) over real TCP, then both restarted on their own
//! journals.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, CounterEvent, CounterQuery, CounterReport, RequestDecoder, ResponseEncoder,
    TAG_GET_VALUE, TAG_INCREMENT, TAG_RESP_ACK, TAG_RESP_VALUE,
};
use ed25519_dalek::SigningKey;
use melin_app::{Application, ApplyCtx, QueryCtx, RejectReason};
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{self, ServerConfig};
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::tcp::BlockingTcpListener;

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

type Node = JoinHandle<Result<(), String>>;

fn spawn_node(
    config: ServerConfig,
    startup: StartupEvents<CounterEvent>,
    sizing: &Sizing,
    shutdown: &Arc<AtomicBool>,
) -> Node {
    let listener = BlockingTcpListener::bind(config.bind).expect("bind client port");
    let shutdown = Arc::clone(shutdown);
    let sizing = sizing.clone();
    std::thread::spawn(move || {
        server::run_with_listener::<SizedCounter>(
            listener,
            config,
            startup,
            sizing,
            RequestDecoder,
            ResponseEncoder,
            None,
            shutdown,
        )
        .map_err(|e| e.to_string())
    })
}

fn stop_node(node: Node, shutdown: &AtomicBool, client_addr: SocketAddr) {
    shutdown.store(true, Ordering::Relaxed);
    // Wake the accept loop so it sees the flag. Dropped deliberately: a
    // node that already stopped listening refuses, which is fine.
    let _ = TcpStream::connect_timeout(&client_addr, Duration::from_millis(100));
    node.join()
        .expect("node thread panicked")
        .expect("node returned an error");
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
    let reply = conn.request_one(TAG_GET_VALUE, &[]).expect("query");
    assert_eq!(reply[0], TAG_RESP_VALUE);
    u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"))
}

fn increment(conn: &mut Connection, amount: u64) {
    let reply = conn
        .request_one(TAG_INCREMENT, &amount.to_le_bytes())
        .expect("increment");
    assert_eq!(reply[0], TAG_RESP_ACK, "the increment is acked");
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
            bind: free_addr(PORT_BASE),
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

    let replication_addr = free_addr(PORT_BASE);
    let mut primary_config = node_config("primary", &primary_key);
    primary_config.replication_bind = Some(replication_addr);
    let primary_client = primary_config.bind;
    let primary_health = primary_config.health_bind.expect("set above");
    let mut replica_config = node_config("replica", &replica_key);
    replica_config.replica_of = Some(replication_addr);
    let replica_client = replica_config.bind;

    let primary_sizing = Sizing::new(PRIMARY_RESERVE);
    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let primary = spawn_node(
        primary_config,
        genesis(),
        &primary_sizing,
        &primary_shutdown,
    );
    let replica_sizing = Sizing::new(REPLICA_RESERVE);
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica = spawn_node(
        replica_config,
        genesis(),
        &replica_sizing,
        &replica_shutdown,
    );

    // --- The primary served: sized once, with its own sizing, on the
    // genesis state — before the genesis events, which reach it through
    // the pipeline like any other. ---
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let mut conn = connect(primary_client, &client_key);
    assert_eq!(value_of(&mut conn), GENESIS, "genesis applied");
    assert_eq!(
        primary_sizing.calls(),
        vec![Sized {
            reserve_for: PRIMARY_RESERVE,
            value: 0,
        }],
        "the primary is sized once at boot, on the state it starts from"
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
    stop_node(replica, &replica_shutdown, replica_client);
    stop_node(primary, &primary_shutdown, primary_client);
    assert_eq!(
        primary_sizing.calls().len(),
        1,
        "nothing sizes the primary again while it serves"
    );

    // --- Both nodes restarted on their own journals, each with a new
    // sizing: sized before the replay, on the genesis state, so the
    // history lands in reserved collections; then again on the recovered
    // state — the primary at boot, the replica when its pipeline is
    // built on its first session — as a snapshot restart would be. ---
    let replication_addr = free_addr(PORT_BASE);
    let mut primary_config = node_config("primary", &primary_key);
    primary_config.replication_bind = Some(replication_addr);
    let primary_client = primary_config.bind;
    let primary_health = primary_config.health_bind.expect("set above");
    let mut replica_config = node_config("replica", &replica_key);
    replica_config.replica_of = Some(replication_addr);
    let replica_client = replica_config.bind;

    let primary_sizing = Sizing::new(RESTART_RESERVE);
    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let primary = spawn_node(
        primary_config,
        genesis(),
        &primary_sizing,
        &primary_shutdown,
    );
    let replica_sizing = Sizing::new(RESTART_RESERVE + 1);
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica = spawn_node(
        replica_config,
        genesis(),
        &replica_sizing,
        &replica_shutdown,
    );
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let mut conn = connect(primary_client, &client_key);
    let replayed = value_of(&mut conn);
    drop(conn);
    stop_node(replica, &replica_shutdown, replica_client);
    stop_node(primary, &primary_shutdown, primary_client);

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
