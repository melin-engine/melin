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
//! - A real replica promoted by hand right after the primary journaled
//!   `ACK-POLICY disk` serves under the `disk+ram` before it: writes are
//!   held until another replica holds the change, or the operator forces
//!   `disk` with none connected. The same holds after the replica restarts
//!   from its own shadow snapshot, the segments before it pruned: the
//!   snapshot records the transition, and seeds the replica's record.
//! - A restart under a different `--ack-policy` journals the change, which
//!   waits for a replica like any other: a boot entry is never forced.
//! - A lineage last run replicated under `disk+ram`, restarted
//!   `--standalone --ack-policy disk`, serves at once; so does one whose
//!   change to `disk` was still waiting for its witness, and its boot
//!   journals `disk` unwitnessed, once.
//! - A forced `ACK-POLICY disk` (no replica connected) lifts the halt on
//!   client writes until a streaming replica acks an entry or a policy that
//!   needs one is journaled; a replica that streams and leaves without
//!   acking leaves it standing.
//! - A replica counts toward the halt once its handshake is accepted: one
//!   that has only authenticated leaves the node halted.
//! - The same forced `disk` releases a write held under `disk+ram` by a
//!   replica that left without acking it: the change overtakes the write
//!   once its own entry is on the primary's disk.
//! - A node that accepts no replica refuses a policy that needs one: a
//!   standalone primary journals nothing and keeps acking, and a replica
//!   without `--replication-bind` stages nothing, so its promotion serves.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, CounterEvent, KIND_RESP_ACK, KIND_RESP_REJECTED, RequestDecoder, ResponseEncoder,
    increment_request,
};
use ed25519_dalek::SigningKey;
use melin_client::Connection;
use melin_journal::{AckPolicyByte, JournalEvent, JournalReader};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{JournalStagingMode, ServerConfig};
use melin_test_node::probe::{
    admin_command, admin_reply_when_up, admin_until_ok, gauge, wait_for_gauge,
};
use melin_test_node::{Addrs, Node};
use serial_test::serial;

#[path = "common/port_pool.rs"]
mod port_pool;
#[path = "common/raw_replica.rs"]
mod raw_replica;
use port_pool::give_back;
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

/// How long a write the gate must hold is watched: long past the
/// primary's fsync of it, so a gate opening on the disk alone would have
/// answered.
const HOLD: Duration = Duration::from_millis(500);

/// The policy gauges, as the policy's wire byte (`AckPolicy::as_u8`).
const IN_FORCE: &str = "melin_ack_policy_in_force";
const JOURNALED: &str = "melin_ack_policy_journaled";
/// The primary's durable journal sequence.
const DURABLE: &str = "melin_journal_sequence";

/// A loopback port no running test holds, from this file's port range:
/// tests hand theirs back (`port_pool`).
fn port() -> SocketAddr {
    port_pool::port(PORT_BASE)
}

/// Wait until a stopped node's health and admin endpoints are released.
/// Both are reused on every boot, but `Node::stop` does not join their
/// threads: the admin listener sees the shutdown flag only on its next
/// 100ms poll, and a node restarted while it is still bound fails to bind
/// it and never comes up. (Waiting here does not take the ports: each
/// probe listener is dropped at once.)
fn wait_released(endpoints: [SocketAddr; 2]) {
    let deadline = Instant::now() + LIMIT;
    for addr in endpoints {
        while std::net::TcpListener::bind(addr).is_err() {
            assert!(Instant::now() < deadline, "{addr} still bound");
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// Node `slot`'s addresses, from [`port`] on kernel TCP.
fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, port)
}

/// The keys of a test's cluster: the primary's replication key, a real
/// replica's, the scripted replica's, and an operator's (client writes
/// and admin).
struct Keys {
    primary: SigningKey,
    replica: SigningKey,
    scripted: SigningKey,
    operator: SigningKey,
}

impl Keys {
    fn new() -> Self {
        Keys {
            primary: SigningKey::from_bytes(&[0x91; 32]),
            replica: SigningKey::from_bytes(&[0x94; 32]),
            scripted: SigningKey::from_bytes(&[0x92; 32]),
            operator: SigningKey::from_bytes(&[0x93; 32]),
        }
    }

    /// Write the `authorized_keys` file admitting all four, into `dir`.
    fn authorized_keys(&self, dir: &Path) -> PathBuf {
        let b64 = |k: &SigningKey| {
            base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
        };
        let path = dir.join("authorized_keys");
        std::fs::write(
            &path,
            format!(
                "replication {} primary\nreplication {} replica\nreplication {} scripted\n\
                 operator {} operator\n",
                b64(&self.primary),
                b64(&self.replica),
                b64(&self.scripted),
                b64(&self.operator)
            ),
        )
        .expect("write authorized_keys");
        path
    }
}

/// A real replica's endpoints, the same on every boot (see
/// [`Cluster::health`]).
struct ReplicaEndpoints {
    addrs: Addrs,
    health: SocketAddr,
    admin: SocketAddr,
}

impl ReplicaEndpoints {
    fn new() -> Self {
        ReplicaEndpoints {
            addrs: addrs(1),
            health: port(),
            admin: port(),
        }
    }
}

impl Drop for ReplicaEndpoints {
    fn drop(&mut self) {
        give_back(&self.addrs, [self.health, self.admin]);
    }
}

/// One test's primary: its directory, keys, and the parts of its
/// configuration every boot shares.
struct Cluster {
    dir: tempfile::TempDir,
    keys: Keys,
    keys_file: PathBuf,
    addrs: Addrs,
    /// The primary's health and admin endpoints, the same on every boot
    /// as its other addresses are, so that restarts take no more ports
    /// (see [`port`]).
    health: SocketAddr,
    admin: SocketAddr,
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
            health: port(),
            admin: port(),
        }
    }

    /// The primary's configuration under `ack_policy`: replicating on its
    /// replication address, with health and admin endpoints, unless
    /// `standalone`. The same journal and addresses on every boot.
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
            health_bind: Some(self.health),
            admin_bind: Some(self.admin),
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
        wait_released([self.health, self.admin]);
        melin_test_node::start_at::<Counter>(
            &self.addrs,
            config,
            StartupEvents::none(),
            (),
            RequestDecoder,
            ResponseEncoder,
        )
    }

    /// A real replica of the primary at `endpoints`, under `ack_policy`
    /// (the policy it requests once promoted). It accepts replicas of its
    /// own once promoted when `replicating`, on its replication address.
    /// The same journal on every boot, and no shadow snapshot.
    fn replica_config(
        &self,
        endpoints: &ReplicaEndpoints,
        ack_policy: AckPolicy,
        replicating: bool,
    ) -> ServerConfig {
        let key_path = self.dir.path().join("replica.key");
        std::fs::write(&key_path, self.keys.replica.to_bytes()).expect("write replica key");
        ServerConfig {
            journal: self.replica_journal(),
            replica_of: Some(self.addrs.replication()),
            replication_bind: replicating.then(|| endpoints.addrs.replication()),
            replication_key: Some(key_path),
            health_bind: Some(endpoints.health),
            admin_bind: Some(endpoints.admin),
            ..self.config(ack_policy, false)
        }
    }

    fn replica_journal(&self) -> PathBuf {
        self.dir.path().join("replica.journal")
    }

    fn start_replica(&self, endpoints: &ReplicaEndpoints, config: ServerConfig) -> Node {
        wait_released([endpoints.health, endpoints.admin]);
        melin_test_node::start_at::<Counter>(
            &endpoints.addrs,
            config,
            StartupEvents::none(),
            (),
            RequestDecoder,
            ResponseEncoder,
        )
    }

    /// The scripted replica, joined to the primary and streaming.
    fn join_replica(&self, health: SocketAddr) -> RawReplica {
        self.join_replica_of(self.addrs.replication(), health)
    }

    /// The scripted replica, joined to the node replicating on
    /// `replication` (whose health endpoint is `health`) and streaming.
    fn join_replica_of(&self, replication: SocketAddr, health: SocketAddr) -> RawReplica {
        // Up, so its replication endpoint is listening.
        wait_for_gauge(health, "melin_replicas_connected", 0);
        let replica = RawReplica::join(replication, &self.keys.scripted, FRAME_LIMIT);
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

impl Drop for Cluster {
    fn drop(&mut self) {
        give_back(&self.addrs, [self.health, self.admin]);
    }
}

/// The reply to one increment: acked or refused.
fn write_reply(conn: &mut Connection) -> u8 {
    conn.request_one(&increment_request(1))
        .expect("a reply to the write")[0]
}

/// A write answered on another thread, so the test can watch whether it
/// is held.
struct Pending {
    /// The reply's kind byte.
    reply: mpsc::Receiver<u8>,
    thread: std::thread::JoinHandle<Connection>,
}

impl Pending {
    fn send(mut conn: Connection) -> Self {
        let (tx, reply) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let kind = write_reply(&mut conn);
            // The receiver outlives the thread unless the test already
            // failed, and then there is no one to tell.
            let _ = tx.send(kind);
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
        let kind = self
            .reply
            .recv_timeout(LIMIT)
            .expect("the write was never answered");
        assert_eq!(kind, KIND_RESP_ACK, "the write is acked");
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

/// Wait until the primary has recorded a replica's ack of `sequence`, on
/// whichever slot it took.
fn wait_for_replica_ack(health: SocketAddr, sequence: u64) {
    let deadline = Instant::now() + LIMIT;
    let acked = || {
        ["0", "1"]
            .iter()
            .filter_map(|slot| {
                gauge(
                    health,
                    &format!("melin_replica_acked_sequence{{slot=\"{slot}\"}}"),
                )
            })
            .max()
    };
    while acked().is_none_or(|acked| acked < sequence) {
        assert!(
            Instant::now() < deadline,
            "the primary never recorded the ack of {sequence} (last: {:?})",
            acked()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Under `disk+ram`, `ACK-POLICY disk` takes effect only once the replica
/// holds the change: a write after it waits on the replica, though it is
/// on the primary's disk; once the replica holds the change (and nothing
/// after it), the write is acked on the disk alone, and so is the next.
#[test]
#[serial]
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
    assert_eq!(
        write_reply(&mut conn),
        KIND_RESP_ACK,
        "acked on the disk alone"
    );

    drop(conn);
    drop(replica);
    primary.stop();
}

/// Under `disk+ram`, a first boot whose replica holds entry 2, then
/// `ACK-POLICY disk` journaled witnessed (entry 3) and still waiting when
/// the replica leaves without it and the primary stops: the lineage's
/// newest change is `disk+ram` → `disk` at 3, never witnessed.
fn leave_the_change_waiting(cluster: &Cluster) {
    let config = cluster.config(AckPolicy::DiskAndRam, false);
    let health = config.health_bind.expect("set");
    let admin = config.admin_bind.expect("set");
    let primary = cluster.start(config);
    let mut replica = cluster.join_replica(health);
    wait_for_gauge(health, IN_FORCE, AckPolicy::DiskAndRam.as_u8().into());

    // Entry 2, acked once the replica holds it; then the change, entry 3,
    // witnessed and waiting: the state the weakening test above holds.
    let pending = Pending::send(cluster.client(&primary));
    wait_durable(health, 2);
    let taken = replica.read_through(2);
    replica.ack_tracks(0, taken);
    drop(pending.acked());
    assert_eq!(
        admin_command(admin, &cluster.keys.operator, "ACK-POLICY disk").as_deref(),
        Some("OK")
    );
    wait_for_gauge(health, JOURNALED, AckPolicy::Disk.as_u8().into());
    wait_durable(health, 3);
    std::thread::sleep(HOLD);
    assert_eq!(
        gauge(health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into()),
        "the change waits for the replica"
    );

    // The replica leaves without the change, and the primary stops.
    drop(replica);
    primary.stop();
}

/// The restart that change is unsafe across without its own rule: under
/// `disk+ram`, `ACK-POLICY disk` is journaled (entry 3) while the replica
/// holds nothing past entry 2, and the primary stops before the replica
/// holds the change. Restarted with `--ack-policy disk`, the policy its
/// lineage already names (so it journals no boot entry), it must not
/// serve under `disk`: the replica never learnt of it, and would judge by
/// `disk+ram` had the primary died again. The gate starts under the
/// policy before the change, `disk+ram`, with the change pending; a write
/// is held, though on the primary's disk, until the replica holds the
/// change, then acked under `disk`.
#[test]
#[serial]
fn a_restart_reapplies_a_change_the_replica_never_held() {
    let cluster = Cluster::new();
    leave_the_change_waiting(&cluster);

    let config = cluster.config(AckPolicy::Disk, false);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    wait_durable(health, 3);
    wait_for_gauge(health, JOURNALED, AckPolicy::Disk.as_u8().into());
    let mut replica = cluster.join_replica(health);
    std::thread::sleep(HOLD);
    assert_eq!(
        gauge(health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into()),
        "the restart must not take the unconfirmed change as in force"
    );

    // A write, entry 4: no boot entry came before it. On the primary's
    // disk, and held while the replica holds only what it held before.
    let pending = Pending::send(cluster.client(&primary));
    wait_durable(health, 4);
    assert_eq!(gauge(health, DURABLE), Some(4), "no boot entry journaled");
    replica.read_through(4);
    replica.ack_tracks(0, 2);
    assert!(
        pending.held_for(HOLD),
        "the write was acked on the primary's disk before the replica held the change"
    );

    // The replica holds the change, and not the write: `disk` takes
    // effect, and the write is acked on the disk alone.
    replica.ack_tracks(0, 3);
    let conn = pending.acked();
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());

    drop(conn);
    drop(replica);
    primary.stop();
}

/// A primary under `disk+ram` whose replica (a real one, at `endpoints`)
/// holds `ACK-POLICY disk`, journaled witnessed at 3 after a write at 2,
/// then a manual `PROMOTE` of the replica as soon as the primary stops.
/// The replica requests `disk`, the policy its lineage now names, so its
/// promotion journals no policy entry: entry 4 is its `EpochBump`.
///
/// Whether the old primary had applied the change by then is not on the
/// promoted node's disk, and does not matter: it serves under the policy
/// before the change, `disk+ram`, with the change pending, so that a
/// replica judging by `disk+ram` (one that never received the change)
/// does not lose events acked under `disk`. Returns the promoted node.
fn promote_right_after_a_weakening(cluster: &Cluster, endpoints: &ReplicaEndpoints) -> Node {
    let config = cluster.config(AckPolicy::DiskAndRam, false);
    let health = config.health_bind.expect("set");
    let admin = config.admin_bind.expect("set");
    let primary = cluster.start(config);
    let replica = cluster.start_replica(
        endpoints,
        cluster.replica_config(endpoints, AckPolicy::Disk, true),
    );
    wait_for_gauge(health, "melin_replicas_connected", 1);
    wait_for_gauge(health, IN_FORCE, AckPolicy::DiskAndRam.as_u8().into());

    let mut conn = cluster.client(&primary);
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK, "entry 2, acked");
    drop(conn);
    assert_eq!(
        admin_command(admin, &cluster.keys.operator, "ACK-POLICY disk").as_deref(),
        Some("OK")
    );
    // The replica holds the change on its disk.
    wait_for_replica_ack(health, 3);
    primary.stop();

    assert_eq!(
        admin_reply_when_up(endpoints.admin, &cluster.keys.operator, "PROMOTE"),
        "OK"
    );
    // Its gate is up, and its journal holds the `EpochBump` and nothing
    // more.
    wait_for_gauge(endpoints.health, JOURNALED, AckPolicy::Disk.as_u8().into());
    wait_durable(endpoints.health, 4);
    std::thread::sleep(HOLD);
    assert_eq!(
        gauge(endpoints.health, DURABLE),
        Some(4),
        "no promotion entry"
    );
    assert_eq!(
        gauge(endpoints.health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into()),
        "the promoted node must not take the weakening as in force"
    );
    replica
}

/// A replica promoted right after a weakening serves under the policy
/// before it: a write is held, though on the promoted node's disk, until
/// another replica holds the change, and then acked under `disk`.
#[test]
#[serial]
fn a_promotion_right_after_a_weakening_waits_for_another_replica_to_hold_it() {
    let cluster = Cluster::new();
    let endpoints = ReplicaEndpoints::new();
    let promoted = promote_right_after_a_weakening(&cluster, &endpoints);
    let health = endpoints.health;
    let mut replica = cluster.join_replica_of(endpoints.addrs.replication(), health);

    // A write, entry 5, on the promoted node's disk; the scripted replica
    // holds everything before the change, and not the change.
    let pending = Pending::send(cluster.client(&promoted));
    wait_durable(health, 5);
    replica.read_through(5);
    replica.ack_tracks(0, 2);
    assert!(
        pending.held_for(HOLD),
        "the write was acked on the promoted node's disk before a replica held the change"
    );
    assert_eq!(
        gauge(health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into())
    );

    // The replica holds the change, and not the write: `disk` takes
    // effect, and the write is acked on the disk alone.
    replica.ack_tracks(0, 3);
    let conn = pending.acked();
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());

    drop(conn);
    drop(replica);
    promoted.stop();
}

/// The same promotion with no replica to hold the change: the operator's
/// `ACK-POLICY disk`, sent while none is connected, is forced, and takes
/// effect over the pending change once its own entry is on disk, which
/// no replica could ever witness.
#[test]
#[serial]
fn a_forced_disk_overtakes_a_promotions_pending_weakening() {
    let cluster = Cluster::new();
    let endpoints = ReplicaEndpoints::new();
    let promoted = promote_right_after_a_weakening(&cluster, &endpoints);
    let health = endpoints.health;

    // `OK staged` while the promoted node's producer is still attaching:
    // re-sent, as the operator would, until it is journaled.
    admin_until_ok(endpoints.admin, &cluster.keys.operator, "ACK-POLICY disk");
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    let mut conn = cluster.client(&promoted);
    assert_eq!(
        write_reply(&mut conn),
        KIND_RESP_ACK,
        "acked on the disk alone, with no replica"
    );

    drop(conn);
    promoted.stop();
}

/// A replica's own shadow snapshot records the lineage policy its
/// stream's changes fold into, transition included, and a restart from
/// that snapshot alone (the segments before it pruned) seeds the
/// replica's record from it: promoted, the replica starts its gate under
/// the policy before the change, with the change pending.
#[test]
#[serial]
fn a_replica_restarted_from_its_own_snapshot_keeps_the_change_pending() {
    use melin_transport_core::lineage::LineagePolicy;

    let cluster = Cluster::new();
    let endpoints = ReplicaEndpoints::new();
    let config = cluster.config(AckPolicy::DiskAndRam, false);
    let health = config.health_bind.expect("set");
    let admin = config.admin_bind.expect("set");
    let primary = cluster.start(config);
    let replica_config = || ServerConfig {
        snapshot_interval_ms: 50,
        ..cluster.replica_config(&endpoints, AckPolicy::Disk, true)
    };
    let replica = cluster.start_replica(&endpoints, replica_config());
    wait_for_gauge(health, "melin_replicas_connected", 1);
    wait_for_gauge(health, IN_FORCE, AckPolicy::DiskAndRam.as_u8().into());

    // A write at 2, `ACK-POLICY disk` at 3, held by the replica; then a
    // rotation, and a write at 4 for the rotation to land on.
    let mut conn = cluster.client(&primary);
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK);
    assert_eq!(
        admin_command(admin, &cluster.keys.operator, "ACK-POLICY disk").as_deref(),
        Some("OK")
    );
    wait_for_replica_ack(health, 3);
    assert_eq!(
        admin_command(admin, &cluster.keys.operator, "ROTATE").as_deref(),
        Some("OK")
    );
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK);
    drop(conn);
    wait_for_replica_ack(health, 4);

    // The replica's archive, and a snapshot of its own covering it,
    // recording the stream's transition at the change's sequence.
    let journal = cluster.replica_journal();
    let snapshot = melin_transport_core::NodeStore::resolve(journal.clone(), None)
        .expect("the replica's store")
        .snapshot()
        .to_path_buf();
    let deadline = Instant::now() + LIMIT;
    let lineage = loop {
        let archived = !melin_journal::segment::list_archives(&journal)
            .expect("list the replica's archives")
            .is_empty();
        if archived
            && let Ok((_, header)) = melin_transport_core::snapshot::load::<Counter>(&snapshot)
            && header.sequence >= 4
        {
            break header.lineage;
        }
        assert!(
            Instant::now() < deadline,
            "the replica never archived and snapshotted past the rotation"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        lineage.ack_policy,
        Some(LineagePolicy {
            change: melin_journal::AckPolicyChange {
                policy: AckPolicyByte(AckPolicy::Disk.as_u8()),
                unwitnessed: false,
            },
            wire_seq: melin_transport_core::WireSeq::new(3),
            previous: Some(AckPolicyByte(AckPolicy::DiskAndRam.as_u8())),
        }),
        "the replica's snapshot records the transition it received"
    );

    // Both stop; every segment before the snapshot is pruned, so the
    // change is the snapshot's alone.
    replica.stop();
    primary.stop();
    for (_, archive) in melin_journal::segment::list_archives(&journal).expect("list archives") {
        std::fs::remove_file(archive).expect("prune an archive");
    }

    let replica = cluster.start_replica(&endpoints, replica_config());
    assert_eq!(
        admin_reply_when_up(endpoints.admin, &cluster.keys.operator, "PROMOTE"),
        "OK"
    );
    wait_for_gauge(endpoints.health, JOURNALED, AckPolicy::Disk.as_u8().into());
    std::thread::sleep(HOLD);
    assert_eq!(
        gauge(endpoints.health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into()),
        "the restarted replica's record lost the transition its snapshot holds"
    );
    replica.stop();
}

/// Halt-plan step 0, the manual-promotion playbook: a primary with no
/// replica halts under every policy, `disk` included; the operator's
/// `ACK-POLICY disk` sent while it has none lifts the halt; a policy that
/// needs a replica restores it; a replica that streams and leaves without
/// acking (one refusing the stream) leaves the override standing, and one
/// that acks ends it, so its departure halts the node again; and a `disk`
/// sent while a replica was connected is no consent once it leaves.
#[test]
#[serial]
fn a_forced_disk_lifts_the_halt_until_a_replica_returns() {
    const TRADING: &str = "melin_trading_active";
    let cluster = Cluster::new();
    let config = cluster.config(AckPolicy::Disk, false);
    let health = config.health_bind.expect("set");
    let admin = config.admin_bind.expect("set");
    let primary = cluster.start(config);
    let ack_policy = |policy: &str| {
        assert_eq!(
            admin_command(
                admin,
                &cluster.keys.operator,
                &format!("ACK-POLICY {policy}")
            )
            .as_deref(),
            Some("OK"),
            "ACK-POLICY {policy}"
        );
    };

    // A swap to `disk` while a replica is connected: witnessed, so when the
    // replica leaves, the node halts like any other.
    let replica = cluster.join_replica(health);
    wait_for_gauge(health, TRADING, 1);
    ack_policy("disk");
    drop(replica);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    wait_for_gauge(health, TRADING, 0);
    let mut conn = cluster.client(&primary);
    assert_eq!(
        write_reply(&mut conn),
        KIND_RESP_REJECTED,
        "halted under `disk`"
    );

    // With none connected, the operator's `disk` is consent: forced, and
    // the halt is lifted by the time it is answered.
    ack_policy("disk");
    assert_eq!(gauge(health, TRADING), Some(1));
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK, "the halt is lifted");

    // A policy that needs a replica restores the halt; consent again lifts
    // it.
    ack_policy("disk+ram");
    assert_eq!(write_reply(&mut conn), KIND_RESP_REJECTED, "halted again");
    ack_policy("disk");
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK, "lifted again");

    // A replica that streams and leaves without acking anything, as one
    // refusing the primary's history on the stream's first frame does,
    // leaves the override standing.
    let replica = cluster.join_replica(health);
    drop(replica);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    assert_eq!(
        write_reply(&mut conn),
        KIND_RESP_ACK,
        "a replica that never acked ended the override"
    );
    assert_eq!(gauge(health, TRADING), Some(1));

    // A replica that acks ends the override: its departure halts the node.
    let mut replica = cluster.join_replica(health);
    let taken = replica.read_through(1);
    replica.ack(taken);
    wait_for_replica_ack(health, taken);
    drop(replica);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    assert_eq!(
        write_reply(&mut conn),
        KIND_RESP_REJECTED,
        "the override did not outlive the replica"
    );
    wait_for_gauge(health, TRADING, 0);

    drop(conn);
    primary.stop();
}

/// A replica counts toward the halt and `melin_replicas_connected` once
/// the primary accepts its handshake, not once it authenticates: one
/// admitted and not yet handshaken leaves a halted node halted, and the
/// same replica handshaking lifts the halt.
///
/// The authenticated replica holds its handshake back for as long as the
/// test needs (well inside the primary's handshake timeout), so the halted
/// reads are no race on this side; a primary counting the replica at
/// authentication would have counted it before the client's write, which
/// connects and authenticates after the replica's verdict.
#[test]
#[serial]
fn a_replica_counts_once_its_handshake_is_accepted() {
    const TRADING: &str = "melin_trading_active";
    let cluster = Cluster::new();
    let config = cluster.config(AckPolicy::Disk, false);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    // A new history serves no client before its first replica streams: one
    // comes and goes, leaving the node serving, and halted.
    let replica = cluster.join_replica(health);
    wait_for_gauge(health, TRADING, 1);
    drop(replica);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    wait_for_gauge(health, TRADING, 0);

    // Authenticated, no handshake: not counted, and the node stays halted.
    let admitted = RawReplica::authenticate(
        cluster.addrs.replication(),
        &cluster.keys.scripted,
        FRAME_LIMIT,
    );
    let mut conn = cluster.client(&primary);
    assert_eq!(
        write_reply(&mut conn),
        KIND_RESP_REJECTED,
        "an authenticated replica that never handshook lifted the halt"
    );
    assert_eq!(gauge(health, "melin_replicas_connected"), Some(0));
    assert_eq!(gauge(health, TRADING), Some(0));

    // Its handshake accepted, it counts, and the halt lifts.
    let replica = admitted.handshake();
    wait_for_gauge(health, "melin_replicas_connected", 1);
    wait_for_gauge(health, TRADING, 1);
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK, "the halt is lifted");

    drop(replica);
    drop(conn);
    primary.stop();
}

/// The forced swap's reason to exist: under `disk+ram`, a write the
/// replica never acked is held, and once the replica is gone nothing can
/// release it in order (the change would queue behind it). `ACK-POLICY
/// disk` sent with no replica connected is journaled unwitnessed, answered
/// `OK`, and releases the held write once its own entry is on the
/// primary's disk: `disk` is in force, journaled and applied.
#[test]
#[serial]
fn a_forced_disk_releases_a_write_held_under_disk_and_ram() {
    let cluster = Cluster::new();
    let config = cluster.config(AckPolicy::DiskAndRam, false);
    let health = config.health_bind.expect("set");
    let admin = config.admin_bind.expect("set");
    let primary = cluster.start(config);
    let mut replica = cluster.join_replica(health);
    wait_for_gauge(health, IN_FORCE, AckPolicy::DiskAndRam.as_u8().into());

    // A write the replica takes but never acks: entry 2, on the primary's
    // disk and held.
    let pending = Pending::send(cluster.client(&primary));
    wait_durable(health, 2);
    replica.read_through(2);
    assert!(
        pending.held_for(HOLD),
        "the write was acked under `disk+ram` without the replica"
    );

    // The replica leaves: the write stays held, under the old policy.
    drop(replica);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    assert!(
        pending.held_for(HOLD),
        "the write was acked with no replica to hold it"
    );
    assert_eq!(
        gauge(health, IN_FORCE),
        Some(AckPolicy::DiskAndRam.as_u8().into())
    );

    // The operator's consent: entry 3, forced. It overtakes the held
    // write, which is acked on the disk alone.
    assert_eq!(
        admin_command(admin, &cluster.keys.operator, "ACK-POLICY disk").as_deref(),
        Some("OK")
    );
    wait_for_gauge(health, JOURNALED, AckPolicy::Disk.as_u8().into());
    // Wait for entry 3 to be durable before taking the ack. That the gate
    // releases the held write only once the forced change is on disk is
    // pinned by the gate's own unit tests, not by this wait.
    wait_durable(health, 3);
    let conn = pending.acked();
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());

    drop(conn);
    primary.stop();
}

/// Assert `reply` is the refusal of a policy no replica can meet (the
/// admin crate pins its whole text).
fn assert_refused_for_want_of_a_replica(reply: Option<String>) {
    let reply = reply.expect("the admin endpoint answered");
    assert!(
        reply.starts_with("ERR ACK-POLICY not journaled: ")
            && reply.contains("needs a replica and this node accepts none"),
        "{reply}"
    );
}

/// A standalone primary refuses `ACK-POLICY disk+ram`: no replica can
/// ever attach to meet it. Nothing is journaled (neither the policy gauge
/// nor the journal moves), and writes keep being acked under `disk`.
#[test]
#[serial]
fn a_standalone_primary_refuses_a_policy_that_needs_a_replica() {
    let cluster = Cluster::new();
    let config = cluster.config(AckPolicy::Disk, true);
    let health = config.health_bind.expect("set");
    let admin = config.admin_bind.expect("set");
    let primary = cluster.start(config);
    wait_for_gauge(health, JOURNALED, AckPolicy::Disk.as_u8().into());

    // The lineage's policy (entry 1), then a write (entry 2).
    let mut conn = cluster.client(&primary);
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK);
    wait_durable(health, 2);

    assert_refused_for_want_of_a_replica(admin_command(
        admin,
        &cluster.keys.operator,
        "ACK-POLICY disk+ram",
    ));
    assert_eq!(
        gauge(health, JOURNALED),
        Some(AckPolicy::Disk.as_u8().into()),
        "the refused policy was journaled"
    );
    assert_eq!(
        write_reply(&mut conn),
        KIND_RESP_ACK,
        "writes keep being acked"
    );
    wait_durable(health, 3);
    assert_eq!(gauge(health, DURABLE), Some(3), "only the write journaled");
    assert_eq!(
        gauge(health, IN_FORCE),
        Some(AckPolicy::Disk.as_u8().into())
    );

    drop(conn);
    primary.stop();
}

/// A replica that will accept no replica once promoted (no
/// `--replication-bind`) refuses to stage a policy that needs one, rather
/// than have its promotion serve under a policy no replica can meet:
/// promoted, it serves under its own `disk` at once.
#[test]
#[serial]
fn a_replica_without_replication_refuses_to_stage_a_policy_that_needs_a_replica() {
    let cluster = Cluster::new();
    let config = cluster.config(AckPolicy::Disk, false);
    let primary_health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    wait_for_gauge(primary_health, "melin_replicas_connected", 0);

    let endpoints = ReplicaEndpoints::new();
    let (health, admin) = (endpoints.health, endpoints.admin);
    let replica = cluster.start_replica(
        &endpoints,
        cluster.replica_config(&endpoints, AckPolicy::Disk, false),
    );
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);

    assert_refused_for_want_of_a_replica(Some(admin_reply_when_up(
        admin,
        &cluster.keys.operator,
        "ACK-POLICY disk+ram",
    )));

    primary.stop();
    assert_eq!(
        admin_command(admin, &cluster.keys.operator, "PROMOTE").as_deref(),
        Some("OK")
    );
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    let mut conn = cluster.client(&replica);
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK, "served under `disk`");

    drop(conn);
    replica.stop();
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
#[serial]
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
    assert_eq!(
        write_reply(&mut conn),
        KIND_RESP_ACK,
        "acked on the disk alone"
    );

    drop(conn);
    drop(replica);
    primary.stop();
}

/// A lineage last run replicated under `disk+ram`, restarted standalone
/// under `disk`, serves at once: its gate starts at the configured policy,
/// and the change is journaled unwitnessed, as the record of the switch.
#[test]
#[serial]
fn a_standalone_restart_serves_under_the_configured_policy() {
    let cluster = Cluster::new();
    begin_under_disk_and_ram(&cluster);

    let config = cluster.config(AckPolicy::Disk, true);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    wait_for_gauge(health, JOURNALED, AckPolicy::Disk.as_u8().into());

    let mut conn = cluster.client(&primary);
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK);
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

/// The ack-policy changes in `cluster`'s journal, by sequence, as
/// `(sequence, policy, unwitnessed)`; and how many entries it holds.
fn journaled_changes(cluster: &Cluster) -> (Vec<(u64, AckPolicyByte, bool)>, u64) {
    let path = cluster.dir.path().join("primary.journal");
    let mut reader = JournalReader::<CounterEvent>::open(&path).expect("open the journal");
    let (mut changes, mut entries) = (Vec::new(), 0);
    while let Some(entry) = reader.next_entry().expect("a valid journal") {
        entries += 1;
        if let JournalEvent::AckPolicyChange(change) = entry.event {
            changes.push((entry.sequence, change.policy, change.unwitnessed));
        }
    }
    (changes, entries)
}

/// A lineage whose newest change (`disk+ram` → `disk`, entry 3) is still
/// waiting for its witness, restarted standalone under that same `disk`:
/// the gate serves under it at once, so the boot journals `disk`
/// unwitnessed (entry 4), the record that it took effect without a
/// witness. The next standalone boot finds the transition marked and
/// journals nothing more.
#[test]
#[serial]
fn a_standalone_restart_marks_a_waiting_change_unwitnessed() {
    let byte = |policy: AckPolicy| AckPolicyByte(policy.as_u8());
    let cluster = Cluster::new();
    leave_the_change_waiting(&cluster);

    let config = cluster.config(AckPolicy::Disk, true);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    let mut conn = cluster.client(&primary);
    // Acked under `disk`, so on disk: the boot entry, then the write.
    assert_eq!(write_reply(&mut conn), KIND_RESP_ACK, "served at once");
    drop(conn);
    primary.stop();
    let expected = [
        (1, byte(AckPolicy::DiskAndRam), false),
        (3, byte(AckPolicy::Disk), false),
        (4, byte(AckPolicy::Disk), true),
    ];
    assert_eq!(journaled_changes(&cluster), (expected.to_vec(), 5));

    let config = cluster.config(AckPolicy::Disk, true);
    let health = config.health_bind.expect("set");
    let primary = cluster.start(config);
    wait_for_gauge(health, IN_FORCE, AckPolicy::Disk.as_u8().into());
    wait_for_gauge(health, DURABLE, 5);
    std::thread::sleep(HOLD);
    primary.stop();
    assert_eq!(
        journaled_changes(&cluster),
        (expected.to_vec(), 5),
        "the second standalone boot journals nothing"
    );
}
