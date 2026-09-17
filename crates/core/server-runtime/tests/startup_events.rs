//! The startup-events contract, end to end on a standalone primary: the
//! genesis events are journaled once, when the journal is created; the
//! `on_primary` events on every boot as primary; both before the first
//! client is served. And a node with no startup events at all must still
//! reach its accept loop — an empty set once hung the boot, waiting for a
//! cursor that nothing would advance.
//!
//! Promotion, the third way into the primary role, is covered by
//! `replicated_failover.rs`.

use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use counter_server::{
    Counter, CounterEvent, RequestDecoder, ResponseEncoder, TAG_GET_VALUE, TAG_INCREMENT,
    TAG_RESP_VALUE,
};
use melin_client::{Connection, SigningKey, key};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{self, ServerConfig};
use melin_wire_protocol::tcp::BlockingTcpListener;

const CLIENT_KEY: [u8; 32] = [0xAA; 32];

struct Node {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    handle: JoinHandle<Result<(), String>>,
}

/// Boot a standalone primary on `dir`'s journal, keeping whatever the
/// directory already holds.
fn boot(dir: &Path, startup: StartupEvents<CounterEvent>) -> Node {
    let auth_path = dir.join("authorized_keys");
    let client_key = SigningKey::from_bytes(&CLIENT_KEY);
    std::fs::write(
        &auth_path,
        key::authorized_keys_line("operator", &client_key.verifying_key(), "test") + "\n",
    )
    .expect("write auth keys");

    let listener = BlockingTcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().expect("addr"))
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let config = ServerConfig {
        bind: addr,
        journal: dir.join("counter.journal"),
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: melin_server_runtime::ack_policy::AckPolicy::Disk,
        no_mlock: true,
        // Unpinned, and therefore yielding: the suite runs many nodes at
        // once, and the default layout would stack every node's same-role
        // thread on one core while a spinner would starve whatever shares
        // its core, the test's own client included.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || -> Result<(), String> {
        server::run_with_listener::<Counter>(
            listener,
            config,
            startup,
            RequestDecoder,
            ResponseEncoder,
            None,
            sd,
        )
        .map_err(|e| e.to_string())
    });
    Node {
        addr,
        shutdown,
        handle,
    }
}

impl Node {
    /// Connect, authenticate, and return the node's first answer: the
    /// counter's value. The deadline is the hang detector — a boot stuck
    /// before its accept loop never completes the handshake.
    fn value(&self) -> u64 {
        let mut node = Connection::connect_by(
            self.addr,
            &SigningKey::from_bytes(&CLIENT_KEY),
            Instant::now() + Duration::from_secs(10),
        )
        .expect("a serving node (boot stuck before the accept loop?)");
        node.set_read_timeout(Duration::from_secs(30))
            .expect("set timeout");
        let frame = node.request_one(1, TAG_GET_VALUE, &[]).expect("query");
        assert_eq!(frame[0], TAG_RESP_VALUE);
        u64::from_le_bytes(frame[1..9].try_into().expect("8-byte value"))
    }

    fn increment(&self, amount: u64) {
        let mut node = Connection::connect_by(
            self.addr,
            &SigningKey::from_bytes(&CLIENT_KEY),
            Instant::now() + Duration::from_secs(10),
        )
        .expect("a serving node");
        node.set_read_timeout(Duration::from_secs(30))
            .expect("set timeout");
        node.request_one(1, TAG_INCREMENT, &amount.to_le_bytes())
            .expect("increment");
    }

    fn stop(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Poke the accept loop so it notices the shutdown flag. A failed
        // connect is fine: the loop may already have noticed.
        let _ = TcpStream::connect_timeout(&self.addr, Duration::from_millis(100));
        self.handle
            .join()
            .expect("server thread panicked")
            .expect("server returned error");
    }
}

fn increments(amounts: &[u64]) -> Vec<CounterEvent> {
    amounts
        .iter()
        .map(|&amount| CounterEvent::Increment { amount })
        .collect()
}

#[test]
fn a_node_without_startup_events_serves() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = boot(dir.path(), StartupEvents::none());
    assert_eq!(node.value(), 0);
    node.stop();
}

#[test]
fn genesis_is_journaled_once_and_on_primary_on_every_boot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let startup = || StartupEvents {
        genesis: increments(&[1_000, 2_000]),
        on_primary: increments(&[1]),
    };

    // A new journal: genesis, then on_primary — already applied when the
    // first client asks.
    let node = boot(dir.path(), startup());
    assert_eq!(node.value(), 3_001);
    node.increment(10);
    node.stop();

    // The same journal recovered: the history replays (genesis included,
    // from the journal), and on_primary is journaled again on top.
    let node = boot(dir.path(), startup());
    assert_eq!(node.value(), 3_012);
    node.stop();
}
