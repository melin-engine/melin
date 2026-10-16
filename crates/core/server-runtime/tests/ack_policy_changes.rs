//! Ack-policy changes end to end: journaled in order with the events they
//! govern, and in force only once the nodes the previous policy relied on
//! hold them.
//!
//! - Under `disk+ram`, `ACK-POLICY disk` with a replica connected: a write
//!   after the change is not acknowledged on the primary's disk alone
//!   until the replica holds the change, and is afterwards. The replica is
//!   scripted (`common/raw_replica.rs`), so what it holds is the test's to
//!   say; one that holds nothing past the change is a frozen replica, and
//!   the gate must stall on it.
//! - A restart under a different `--ack-policy` journals the change, which
//!   waits for a replica like any other: a boot entry is never forced.
//! - A lineage last run replicated under `disk+ram`, restarted
//!   `--standalone --ack-policy disk`, serves at once.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{Counter, KIND_RESP_ACK, RequestDecoder, ResponseEncoder, increment_request};
use ed25519_dalek::SigningKey;
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{JournalStagingMode, ServerConfig};
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;

#[path = "common/raw_replica.rs"]
mod raw_replica;
use raw_replica::RawReplica;

#[path = "common/probe.rs"]
mod probe;
use probe::{admin_command, gauge, wait_for_gauge};

/// Port range for `free_addr`, shared with the other cluster tests: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because this binary is in nextest's `cluster-serial` group with them.
const PORT_BASE: u16 = 10_000;

/// How long anything a test waits on may take: generous, for a slow and
/// shared CI runner, and bounded, so a hang fails here with a message.
const LIMIT: Duration = Duration::from_secs(60);

/// How long to wait for a frame the primary owes the scripted replica.
const FRAME_LIMIT: Duration = Duration::from_secs(10);

/// How long a write the gate must hold is watched: long past the
/// primary's fsync of it, so a gate opening on the disk alone would have
/// answered.
const HOLD: Duration = Duration::from_millis(500);

/// The policy gauges, as the policy's wire byte (`AckPolicy::as_u8`).
const IN_FORCE: &str = "melin_ack_policy_in_force";
const JOURNALED: &str = "melin_ack_policy_journaled";
/// The primary's durable journal sequence.
const DURABLE: &str = "melin_journal_sequence";

/// Node `slot`'s addresses, from this file's port range on kernel TCP.
fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, || free_addr(PORT_BASE))
}

/// The keys of a test's cluster: the primary's replication key, the
/// scripted replica's, and an operator's (client writes and admin).
struct Keys {
    primary: SigningKey,
    scripted: SigningKey,
    operator: SigningKey,
}

impl Keys {
    fn new() -> Self {
        Keys {
            primary: SigningKey::from_bytes(&[0x91; 32]),
            scripted: SigningKey::from_bytes(&[0x92; 32]),
            operator: SigningKey::from_bytes(&[0x93; 32]),
        }
    }

    /// Write the `authorized_keys` file admitting all three, into `dir`.
    fn authorized_keys(&self, dir: &Path) -> PathBuf {
        let b64 = |k: &SigningKey| {
            base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
        };
        let path = dir.join("authorized_keys");
        std::fs::write(
            &path,
            format!(
                "replication {} primary\nreplication {} scripted\noperator {} operator\n",
                b64(&self.primary),
                b64(&self.scripted),
                b64(&self.operator)
            ),
        )
        .expect("write authorized_keys");
        path
    }
}

/// One test's primary: its directory, keys, and the parts of its
/// configuration every boot shares.
struct Cluster {
    dir: tempfile::TempDir,
    keys: Keys,
    keys_file: PathBuf,
    addrs: Addrs,
}

impl Cluster {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let keys = Keys::new();
        let keys_file = keys.authorized_keys(dir.path());
        Cluster {
            dir,
            keys,
            keys_file,
            addrs: addrs(0),
        }
    }

    /// The primary's configuration under `ack_policy`: replicating on its
    /// replication address, with health and admin endpoints, unless
    /// `standalone`. The same journal on every boot.
    fn config(&self, ack_policy: AckPolicy, standalone: bool) -> ServerConfig {
        let key_path = self.dir.path().join("primary.key");
        std::fs::write(&key_path, self.keys.primary.to_bytes()).expect("write node key");
        ServerConfig {
            journal: self.dir.path().join("primary.journal"),
            authorized_keys: self.keys_file.clone(),
            ack_policy,
            standalone,
            replication_bind: (!standalone).then(|| self.addrs.replication()),
            replication_key: Some(key_path),
            health_bind: Some(free_addr(PORT_BASE)),
            admin_bind: Some(free_addr(PORT_BASE)),
            // Unpinned, and therefore yielding: the node shares this
            // process with the test's own client and replica.
            cores: PipelineCores::unpinned(),
            // No tick: every journaled entry is one the test made, so it
            // knows each one's sequence.
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            journal_staging_mode: JournalStagingMode::Allocate,
            ..melin_test_node::config()
        }
    }

    fn start(&self, config: ServerConfig) -> Node {
        melin_test_node::start_at::<Counter>(
            &self.addrs,
            config,
            StartupEvents::none(),
            (),
            RequestDecoder,
            ResponseEncoder,
        )
    }

    /// The scripted replica, joined and streaming.
    fn join_replica(&self, health: SocketAddr) -> RawReplica {
        // Up, so its replication endpoint is listening.
        wait_for_gauge(health, "melin_replicas_connected", 0);
        let replica = RawReplica::join(self.addrs.replication(), &self.keys.scripted, FRAME_LIMIT);
        wait_for_gauge(health, "melin_replicas_connected", 1);
        replica
    }

    fn client(&self, node: &Node) -> Connection {
        let deadline = Instant::now() + LIMIT;
        let mut conn = Connection::connect_by(node.addr(), &self.keys.operator, deadline)
            .expect("client connects");
        // A held write is held by design here; the test's own bound
        // decides when that is a failure.
        conn.set_read_timeout(LIMIT)
            .expect("set the client's read timeout");
        conn
    }
}

/// A write answered on another thread, so the test can watch whether it
/// is held.
struct Pending {
    reply: mpsc::Receiver<Vec<u8>>,
    thread: std::thread::JoinHandle<Connection>,
}

impl Pending {
    fn send(mut conn: Connection) -> Self {
        let (tx, reply) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let frame = conn
                .request_one(&increment_request(1))
                .expect("a reply to the write");
            // The receiver outlives the thread unless the test already
            // failed, and then there is no one to tell.
            let _ = tx.send(frame);
            conn
        });
        Pending { reply, thread }
    }

    /// Still unanswered after `hold`.
    fn held_for(&self, hold: Duration) -> bool {
        self.reply.recv_timeout(hold).is_err()
    }

    /// The reply, which must come within the test's bound; the connection
    /// back.
    fn acked(self) -> Connection {
        let frame = self
            .reply
            .recv_timeout(LIMIT)
            .expect("the write was never answered");
        assert_eq!(frame[0], KIND_RESP_ACK, "the write is acked");
        self.thread.join().expect("the client thread panicked")
    }
}

/// Wait until the primary's journal is durable through `sequence`.
fn wait_durable(health: SocketAddr, sequence: u64) {
    let deadline = Instant::now() + LIMIT;
    while gauge(health, DURABLE).is_none_or(|durable| durable < sequence) {
        assert!(
            Instant::now() < deadline,
            "the journal never reached {sequence} (last: {:?})",
            gauge(health, DURABLE)
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Under `disk+ram`, `ACK-POLICY disk` takes effect only once the replica
/// holds the change: a write after it waits on the replica, though it is
/// on the primary's disk; once the replica holds the change (and nothing
/// after it), the write is acked on the disk alone, and so is the next.
#[test]
fn a_weakening_waits_for_the_replica_to_hold_the_change() {
    let cluster = Cluster::new();
    let config = cluster.config(AckPolicy::DiskAndRam, false);
    let health = config.health_bind.expect("set");
    let admin = config.admin_bind.expect("set");
    let primary = cluster.start(config);
    let mut replica = cluster.join_replica(health);

    // The new lineage journaled its policy as it began (entry 1), and runs
    // it.
    wait_for_gauge(health, JOURNALED, AckPolicy::DiskAndRam.as_u8().into());
    wait_for_gauge(health, IN_FORCE, AckPolicy::DiskAndRam.as_u8().into());

    // A write under `disk+ram`: entry 2, acked once the replica holds it.
    let pending = Pending::send(cluster.client(&primary));
    wait_durable(health, 2);
    let taken = replica.read_through(2);
    replica.ack_tracks(0, taken);
    let conn = pending.acked();

    // The change: entry 3. A replica is connected, so it is witnessed.
    assert_eq!(
        admin_command(admin, &cluster.keys.operator, "ACK-POLICY disk").as_deref(),
        Some("OK")
    );
    wait_for_gauge(health, JOURNALED, AckPolicy::Disk.as_u8().into());
    wait_durable(health, 3);

    // A write after it, entry 4, durable on the primary's disk: held, as
    // the change itself is, while the replica holds nothing past entry 2.
    let pending = Pending::send(conn);
    wait_durable(health, 4);
    assert!(
        pending.held_for(HOLD),
        "the write was acked on the primary's disk before the replica held the change"
    );
    assert_eq!(
        gauge(health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into()),
        "the change is not in force before the replica holds it"
    );

    // The replica holds the change, and not the write: `disk` is in force
    // from the change on, and the write is acked on the disk alone.
    replica.read_through(3);
    replica.ack_tracks(0, 3);
    let mut conn = pending.acked();
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    let reply = conn
        .request_one(&increment_request(1))
        .expect("a reply to the write");
    assert_eq!(reply[0], KIND_RESP_ACK, "acked on the disk alone");

    drop(conn);
    drop(replica);
    primary.stop();
}

/// A first boot under `disk+ram`, replicated, with no replica ever
/// attaching: the lineage records `disk+ram` (its first entry).
fn begin_under_disk_and_ram(cluster: &Cluster) {
    let config = cluster.config(AckPolicy::DiskAndRam, false);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    wait_for_gauge(health, JOURNALED, AckPolicy::DiskAndRam.as_u8().into());
    wait_durable(health, 1);
    primary.stop();
}

/// Restarted under a different `--ack-policy`, a primary journals the
/// change, and it waits for the replica the lineage's policy relies on:
/// a boot entry is never forced. Once a replica holds it, it is in force.
#[test]
fn a_restart_under_another_policy_journals_the_change_and_waits_for_a_replica() {
    let cluster = Cluster::new();
    begin_under_disk_and_ram(&cluster);

    let config = cluster.config(AckPolicy::Disk, false);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    // Entry 2, journaled; the lineage's policy still in force.
    wait_for_gauge(health, JOURNALED, AckPolicy::Disk.as_u8().into());
    wait_durable(health, 2);
    std::thread::sleep(HOLD);
    assert_eq!(
        gauge(health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into()),
        "a boot entry must not take effect before a replica holds it"
    );

    let mut replica = cluster.join_replica(health);
    let taken = replica.read_through(2);
    replica.ack_tracks(0, taken);
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());

    // Under `disk` now: a write is acked without the replica.
    let mut conn = cluster.client(&primary);
    let reply = conn
        .request_one(&increment_request(1))
        .expect("a reply to the write");
    assert_eq!(reply[0], KIND_RESP_ACK, "acked on the disk alone");

    drop(conn);
    drop(replica);
    primary.stop();
}

/// A lineage last run replicated under `disk+ram`, restarted standalone
/// under `disk`, serves at once: its gate starts at the configured policy,
/// and the change is journaled unwitnessed, as the record of the switch.
#[test]
fn a_standalone_restart_serves_under_the_configured_policy() {
    let cluster = Cluster::new();
    begin_under_disk_and_ram(&cluster);

    let config = cluster.config(AckPolicy::Disk, true);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    wait_for_gauge(health, JOURNALED, AckPolicy::Disk.as_u8().into());

    let mut conn = cluster.client(&primary);
    let reply = conn
        .request_one(&increment_request(1))
        .expect("a reply to the write");
    assert_eq!(reply[0], KIND_RESP_ACK);
    drop(conn);
    primary.stop();

    // The lineage now holds `disk`: the next standalone boot journals
    // nothing more (the change, then the write, then nothing).
    let config = cluster.config(AckPolicy::Disk, true);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    wait_for_gauge(health, DURABLE, 3);
    std::thread::sleep(HOLD);
    assert_eq!(gauge(health, DURABLE), Some(3), "no second change");
    primary.stop();
}
