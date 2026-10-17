//! What keeps a node's pipeline from stalling unseen, end to end, on both
//! primary and replica:
//!
//! - every thread with a placement starts in its own scheduling context,
//!   not its spawner's (a thread created on a core a real-time thread
//!   spins on never runs, and its node wedges when its ring fills);
//! - a primary whose startup fails after its stages started leaves none of
//!   them running;
//! - a primary warns about a replica that keeps taking entries in but
//!   persists none, and says when it persists again.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).
//! The tests share one process's threads and its log, so they run one at a
//! time (see [`serial`]).

use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{Counter, KIND_RESP_ACK, RequestDecoder, ResponseEncoder, increment_request};
use ed25519_dalek::SigningKey;
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::{PipelineCores, Placement};
use melin_server_runtime::server::{JournalStagingMode, ServerConfig};
use melin_test_node::probe::{gauge, wait_for_gauge};
use melin_test_node::sched::{first_pinnable_cpu, set_batch_policy, threads_named};
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;

#[path = "common/raw_replica.rs"]
mod raw_replica;
use raw_replica::RawReplica;

/// Port range for `free_addr`, shared with the other cluster tests: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because this binary is in nextest's `cluster-serial` group with them.
const PORT_BASE: u16 = 10_000;

/// How long anything a test waits on may take: generous, for a slow and
/// shared CI runner, and bounded, so a hang fails here with a message.
const LIMIT: Duration = Duration::from_secs(60);

/// How long to wait for a frame the primary owes the scripted replica.
const FRAME_LIMIT: Duration = Duration::from_secs(10);

/// One test at a time within this process: they read every thread of the
/// process and its one log. (Under nextest each test is a process of its
/// own; under `cargo test` they would otherwise share it.) Installs the
/// log (see [`log`]), so a failing test reports what its nodes did.
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    // A test that failed while holding the lock leaves nothing behind that
    // the next one depends on, so recover the guard rather than fail every
    // later test with the poison.
    let serial = SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    log();
    serial
}

/// Node `slot`'s addresses, from this file's port range on kernel TCP.
fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, || free_addr(PORT_BASE))
}

fn start_at(addrs: &Addrs, config: ServerConfig) -> Node {
    melin_test_node::start_at::<Counter>(
        addrs,
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

/// The keys of a test's cluster: two nodes' replication keys, one more
/// replication key (for a scripted replica), and a client's.
struct Keys {
    primary: SigningKey,
    replica: SigningKey,
    scripted: SigningKey,
    client: SigningKey,
}

impl Keys {
    fn new() -> Self {
        Keys {
            primary: SigningKey::from_bytes(&[0x81; 32]),
            replica: SigningKey::from_bytes(&[0x82; 32]),
            scripted: SigningKey::from_bytes(&[0x83; 32]),
            client: SigningKey::from_bytes(&[0x84; 32]),
        }
    }

    /// Write the `authorized_keys` file admitting all four, into `dir`.
    fn authorized_keys(&self, dir: &Path) -> std::path::PathBuf {
        let b64 = |k: &SigningKey| {
            base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
        };
        let path = dir.join("authorized_keys");
        std::fs::write(
            &path,
            format!(
                "replication {} primary\nreplication {} replica\nreplication {} scripted\n\
                 operator {} client\n",
                b64(&self.primary),
                b64(&self.replica),
                b64(&self.scripted),
                b64(&self.client)
            ),
        )
        .expect("write authorized_keys");
        path
    }
}

/// A node's configuration, in `dir`, under `name`, replicating with `key`.
fn node_config(dir: &Path, name: &str, key: &SigningKey, keys_file: &Path) -> ServerConfig {
    let key_path = dir.join(format!("{name}.key"));
    std::fs::write(&key_path, key.to_bytes()).expect("write node key");
    ServerConfig {
        journal: dir.join(format!("{name}.journal")),
        authorized_keys: keys_file.to_owned(),
        // Unpinned, and therefore yielding, unless a test places them:
        // the nodes share this process with the test's own client.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        // Off unless a test asks for one: the default is a fixed port,
        // which two nodes in one process would both take.
        health_bind: None,
        // Staged segments allocated, not zero-filled: a zero fill is a pass
        // over a whole segment, from every node at once, on a CI runner's
        // disk, and nothing here measures the journal.
        journal_staging_mode: JournalStagingMode::Allocate,
        replication_key: Some(key_path),
        ..melin_test_node::config()
    }
}

/// The threads a primary and a replica create through their placement,
/// by name, on this build's transport. Several have one name on both
/// nodes (`journal-seq`, `matching`, ...): every thread of the name is
/// checked, whichever node it belongs to.
fn placed_threads() -> Vec<&'static str> {
    let mut names = vec![
        // Both nodes.
        "journal-seq",
        "matching",
        "journal-disk",
        "journal-prep",
        // The primary.
        "response",
        "shadow",
        // The replica.
        "drain",
        "replica-shadow",
    ];
    // Kernel TCP only: on DPDK the client and replication I/O run in the
    // poll thread, which is the node's own thread, not a spawned one.
    if !cfg!(feature = "dpdk") {
        names.extend(["uring-reader", "repl-0", "replica-receiver"]);
    }
    names
}

/// Every thread with a placement starts in its own scheduling context,
/// not its spawner's: on a primary and on a replica, on this build's
/// transport.
///
/// A thread created in its spawner's context starts on its spawner's CPU,
/// and has to run once to move itself elsewhere. When a real-time thread
/// spins on that CPU (a pinned pipeline thread, on an isolated core), it
/// never does, and its node wedges when its ring fills. So the runtime
/// hands each placed thread its context before it exists. Where a thread
/// started cannot be read back once it has pinned itself, but its policy
/// can (see `melin_test_node::sched`): both nodes are started from this
/// thread at `SCHED_BATCH`, so every thread they spawn inherits it, and no
/// placed thread may be found at it.
///
/// The threads are placed on a real core, not left unpinned: an unpinned
/// thread drops to `SCHED_OTHER` itself, which would pass whatever its
/// spawner did.
#[test]
fn placed_threads_start_in_their_own_scheduling_context() {
    let _serial = serial();
    let Some(core) = first_pinnable_cpu() else {
        eprintln!("skipped: this process may run on CPU 0 only, and 0 means unpinned");
        return;
    };
    let tmp = tempfile::tempdir().expect("tempdir");
    let keys = Keys::new();
    let keys_file = keys.authorized_keys(tmp.path());

    // Every thread on one core, yielding: what matters is that each has a
    // core to be placed on, not which. DPDK's poll thread never yields,
    // so it cannot share one, and is the node's own thread anyway.
    let placed = Placement::yielding(core);
    let reader = if cfg!(feature = "dpdk") {
        Placement::unpinned()
    } else {
        placed
    };
    let cores = PipelineCores {
        journal_seq: placed,
        matching: placed,
        response: placed,
        reader,
        shadow: placed,
        repl_handler_0: placed,
        repl_handler_1: placed,
        journal_prep: placed,
        journal_disk: placed,
        ..PipelineCores::unpinned()
    };
    let config = |name: &str, key: &SigningKey| ServerConfig {
        cores,
        // A client write is acked only once the replica holds it too, so
        // one acked write means the replica's pipeline is built. Not
        // `TwoDisks`: waiting on the replica's fsync proves nothing more
        // about where its threads started, and on a slow disk it outlasts
        // a client's read.
        ack_policy: AckPolicy::Ram,
        // Non-zero so the shadow threads exist; long enough that neither
        // takes a snapshot.
        snapshot_interval_ms: 3_600_000,
        ..node_config(tmp.path(), name, key, &keys_file)
    };

    // The context no placed thread may start in.
    set_batch_policy();
    let primary_addrs = addrs(0);
    let mut primary_config = config("primary", &keys.primary);
    primary_config.replication_bind = Some(primary_addrs.replication());
    let primary = start_at(&primary_addrs, primary_config);
    let mut replica_config = config("replica", &keys.replica);
    replica_config.replica_of = Some(primary_addrs.replication());
    let replica = start_at(&addrs(1), replica_config);

    let deadline = Instant::now() + LIMIT;
    let mut conn = Connection::connect_by(primary.addr(), &keys.client, deadline)
        .expect("client connects to the primary");
    // Bounded by the test's own limit, not the client's default: two
    // nodes' threads share one core here, beside DPDK's busy-polling
    // threads on that build, on a runner with few cores.
    conn.set_read_timeout(LIMIT)
        .expect("set the client's read timeout");
    // Refused until the replica is attached.
    loop {
        let reply = conn
            .request_one(&increment_request(1))
            .expect("a reply to the write");
        if reply[0] == KIND_RESP_ACK {
            break;
        }
        assert!(Instant::now() < deadline, "the write was never acked");
        std::thread::sleep(Duration::from_millis(100));
    }

    let names = placed_threads();
    let mut found = threads_named(&names);
    while let Some(missing) = names.iter().find(|name| {
        let comm = melin_test_node::sched::comm(name);
        !found.iter().any(|(found, _)| found == comm)
    }) {
        assert!(Instant::now() < deadline, "no thread named {missing}");
        std::thread::sleep(Duration::from_millis(50));
        found = threads_named(&names);
    }
    let inherited: Vec<&str> = found
        .iter()
        .filter(|(_, policy)| *policy == libc::SCHED_BATCH)
        .map(|(name, _)| name.as_str())
        .collect();

    drop(conn);
    replica.stop();
    primary.stop();
    assert!(
        inherited.is_empty(),
        "{inherited:?} started in their spawner's scheduling context instead of their own \
         placement; on an isolated core held by a real-time spinner they would never run"
    );
}

/// A primary whose startup fails after its stage threads started leaves
/// none of them running: its health port is taken, so the endpoint, which
/// starts after the stages, cannot bind, and `run` returns the error.
///
/// In a process that exits on the error, a stage left running would end
/// with it. In one that hosts several nodes (`run_with_shutdown`), it
/// would outlive its node, spinning.
#[test]
fn a_primary_that_fails_to_start_leaves_no_stage_running() {
    let _serial = serial();
    let tmp = tempfile::tempdir().expect("tempdir");
    let keys = Keys::new();
    let keys_file = keys.authorized_keys(tmp.path());
    let taken = std::net::TcpListener::bind(free_addr(PORT_BASE)).expect("take the health port");
    let config = ServerConfig {
        standalone: true,
        ack_policy: AckPolicy::Disk,
        health_bind: Some(taken.local_addr().expect("the taken port")),
        // Non-zero so the shadow stage starts too.
        snapshot_interval_ms: 3_600_000,
        ..node_config(tmp.path(), "primary", &keys.primary, &keys_file)
    };

    let node = melin_test_node::start::<Counter>(
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    );
    let result = node.join();
    drop(taken);
    assert!(
        result.is_err(),
        "a primary whose health port is taken must not start"
    );

    let stages = [
        "journal-seq",
        "matching",
        "response",
        "shadow",
        "journal-disk",
        "journal-prep",
    ];
    let deadline = Instant::now() + LIMIT;
    loop {
        let left = threads_named(&stages);
        if left.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "stage threads still running after the failed start: {left:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// What the nodes in this process log, at `info` and above: kept for the
/// test that checks a warning, and copied to the test's output, which the
/// harness shows when a test fails. Installed once, by the first test to
/// take its turn (see [`serial`]). This file's own subscriber, not
/// `melin_test_node::logs`: a process has one, and this one also keeps
/// what it logs.
fn log() -> &'static Arc<Mutex<Vec<u8>>> {
    static LOG: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    LOG.get_or_init(|| {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&buffer);
        tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || LogSink(Arc::clone(&sink)))
            .try_init()
            .expect("the only subscriber in this process");
        buffer
    })
}

/// A `tracing` writer into the shared log, and to the test's output.
struct LogSink(Arc<Mutex<Vec<u8>>>);

impl Write for LogSink {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        // `print!`, which the harness captures, as the test writer of
        // `tracing_subscriber` does.
        print!("{}", String::from_utf8_lossy(bytes));
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn logged(text: &str) -> bool {
    let log = log()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    String::from_utf8_lossy(&log).contains(text)
}

/// A primary warns about a replica that takes entries in but persists
/// none, and says when it persists again.
///
/// The replica is scripted: it acks the entries it has read as taken into
/// its pipeline, and none as journaled, as a replica whose journal stage
/// never started (or whose disk hangs) does. Such a replica satisfies an
/// ack policy that counts in-memory copies, and used to go unnoticed until
/// its input ring filled and its primary evicted it. The primary's ack
/// policy here is `disk`, so the test's client does not depend on the
/// replica.
///
/// A few entries, written once, are enough: the warning is on entries held
/// unpersisted for `PERSIST_STALL_BOUND`, and the primary checks on every
/// ack, so the replica re-sends the same ack until the warning comes. No
/// stream runs meanwhile, so nothing can fill the primary's replication
/// ring and evict the replica before the bound, as a client writing flat
/// out did while this thread read behind it.
#[test]
fn a_replica_that_takes_entries_but_persists_none_is_reported() {
    const STALLED: &str = "replica accepts entries but persists none";
    const RESUMED: &str = "replica persisting again";
    /// Entries for the replica to take in: any number above zero holds the
    /// warning's condition.
    const WRITES: usize = 10;
    /// How often the replica re-sends its ack: well under the bound, so the
    /// warning comes soon after it.
    const ACK_INTERVAL: Duration = Duration::from_millis(100);

    let _serial = serial();
    let tmp = tempfile::tempdir().expect("tempdir");
    let keys = Keys::new();
    let keys_file = keys.authorized_keys(tmp.path());
    let primary_addrs = addrs(0);
    let health = free_addr(PORT_BASE);
    let config = ServerConfig {
        replication_bind: Some(primary_addrs.replication()),
        ack_policy: AckPolicy::Disk,
        health_bind: Some(health),
        ..node_config(tmp.path(), "primary", &keys.primary, &keys_file)
    };
    let primary = start_at(&primary_addrs, config);

    // Up, so its replication endpoint is listening.
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let mut replica = RawReplica::join(primary_addrs.replication(), &keys.scripted, FRAME_LIMIT);
    wait_for_gauge(health, "melin_replicas_connected", 1);

    let deadline = Instant::now() + LIMIT;
    let mut client =
        Connection::connect_by(primary.addr(), &keys.client, deadline).expect("client connects");
    // The test's own bound, as in the placement test: a CI runner's disk
    // sets the pace of a `disk` ack.
    client
        .set_read_timeout(LIMIT)
        .expect("set the client's read timeout");
    for _ in 0..WRITES {
        let reply = client
            .request_one(&increment_request(1))
            .expect("a reply to the write");
        assert_eq!(reply[0], KIND_RESP_ACK, "the write is acked");
    }
    let head = gauge(health, "melin_journal_sequence").expect("the primary's sequence");
    let taken = replica.read_through(head);

    // Takes them in, persists nothing past the handshake's zero.
    while !logged(STALLED) {
        assert!(
            Instant::now() < deadline,
            "the primary never warned about a replica that persists nothing"
        );
        replica.ack_tracks(0, taken);
        std::thread::sleep(ACK_INTERVAL);
    }
    assert!(!logged(RESUMED), "no recovery before the replica persists");

    // Persists again: everything it has taken in, both tracks level, as a
    // replica streaming acks.
    while !logged(RESUMED) {
        assert!(
            Instant::now() < deadline,
            "the primary never said the replica persists again"
        );
        replica.ack(taken);
        std::thread::sleep(ACK_INTERVAL);
    }

    drop(client);
    drop(replica);
    primary.stop();
}
