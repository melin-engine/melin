//! One snapshot location for every role, end to end: a node given
//! `--snapshot-path` boots from the snapshot there, serves it to a replica
//! that needs a resync, and a replica whose snapshot is on another
//! filesystem than its journal archives it beside itself when it resyncs.
//! And a node refuses to start on a snapshot path it could never write.
//!
//! Counter nodes, `disk` ack policy, short shadow intervals so snapshots
//! are written while the test waits.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use counter_server::{
    Counter, GET_VALUE_REQUEST, KIND_RESP_ACK, KIND_RESP_VALUE, RequestDecoder, ResponseEncoder,
    increment_request,
};
use melin_client::{Connection, SigningKey, key};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::probe::{gauge, wait_for_gauge, wait_for_gauge_within};
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;

/// Port range for `free_addr`, shared with the other cluster tests: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because they are all in nextest's `cluster-serial` group, so they never
/// run at the same time.
const PORT_BASE: u16 = 10_000;

/// How long the test waits for a node to serve, a snapshot to be written,
/// or a replica to catch up.
const LIMIT: Duration = Duration::from_secs(60);

/// The shadow stage's interval: short, so a snapshot follows the test's
/// writes within a fraction of a second.
const SHADOW_INTERVAL_MS: u64 = 50;

const CLIENT_KEY: [u8; 32] = [0x91; 32];
const PRIMARY_KEY: [u8; 32] = [0x92; 32];
const REPLICA_KEY: [u8; 32] = [0x93; 32];

/// A test's directory, with the `authorized_keys` every node reads.
struct Site {
    dir: tempfile::TempDir,
}

impl Site {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let lines = [
            key::authorized_keys_line("operator", &client_key().verifying_key(), "client"),
            key::authorized_keys_line(
                "replication",
                &SigningKey::from_bytes(&PRIMARY_KEY).verifying_key(),
                "primary",
            ),
            key::authorized_keys_line(
                "replication",
                &SigningKey::from_bytes(&REPLICA_KEY).verifying_key(),
                "replica",
            ),
        ];
        std::fs::write(dir.path().join("authorized_keys"), lines.join("\n"))
            .expect("write authorized_keys");
        Site { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    /// A node's configuration: journal `{name}.journal` here, snapshot at
    /// `snapshot`, its own health endpoint and replication key.
    fn config(&self, name: &str, key: [u8; 32], snapshot: Option<PathBuf>) -> ServerConfig {
        let key_path = self.path().join(format!("{name}.key"));
        std::fs::write(&key_path, key).expect("write node key");
        ServerConfig {
            journal: self.path().join(format!("{name}.journal")),
            snapshot_path: snapshot,
            snapshot_interval_ms: SHADOW_INTERVAL_MS,
            authorized_keys: self.path().join("authorized_keys"),
            ack_policy: AckPolicy::Disk,
            // Unpinned, and therefore yielding: the nodes share this
            // process with the test's own client.
            cores: PipelineCores::unpinned(),
            tick_interval_ms: 0,
            health_bind: Some(free_addr(PORT_BASE)),
            replication_key: Some(key_path),
            ..melin_test_node::config()
        }
    }

    /// A directory of its own under the site, for snapshots.
    fn subdir(&self, name: &str) -> PathBuf {
        let dir = self.path().join(name);
        std::fs::create_dir(&dir).expect("create the snapshot directory");
        dir
    }
}

fn client_key() -> SigningKey {
    SigningKey::from_bytes(&CLIENT_KEY)
}

fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, || free_addr(PORT_BASE))
}

fn start(addrs: &Addrs, config: ServerConfig) -> Node {
    melin_test_node::start_at::<Counter>(
        addrs,
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

/// A standalone primary: no replica to wait for under `disk`.
fn standalone(config: ServerConfig) -> ServerConfig {
    ServerConfig {
        standalone: true,
        ..config
    }
}

fn connect(addr: SocketAddr) -> Connection {
    let mut conn = melin_test_node::connect(addr, &client_key());
    conn.set_read_timeout(LIMIT).expect("set timeout");
    conn
}

fn value_of(addr: SocketAddr) -> u64 {
    let reply = connect(addr)
        .request_one(&GET_VALUE_REQUEST)
        .expect("query");
    assert_eq!(reply[0], KIND_RESP_VALUE);
    u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"))
}

fn increment(addr: SocketAddr, amounts: &[u64]) {
    let mut conn = connect(addr);
    for &amount in amounts {
        let reply = conn
            .request_one(&increment_request(amount))
            .expect("increment");
        assert_eq!(reply[0], KIND_RESP_ACK, "the increment is acked");
    }
}

/// Wait until the snapshot at `path` covers the node's whole journal, as
/// its health endpoint reports it.
fn wait_for_snapshot(health: SocketAddr, path: &Path) {
    let head = gauge(health, "melin_journal_sequence").expect("the node's sequence");
    let deadline = Instant::now() + LIMIT;
    loop {
        if let Ok((_, header)) = melin_transport_core::snapshot::load::<Counter>(path)
            && header.sequence >= head
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no snapshot at {} reached sequence {head}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A primary given `--snapshot-path` in a directory of its own, beside a
/// stale `<journal>.snapshot` of another history, recovers from its own
/// snapshot: with its journal, and with the snapshot alone (the standard
/// upgrade's layout). Before, a primary read only the derived file, and
/// would have booted from the stale one.
#[test]
fn a_primary_recovers_from_its_snapshot_path_not_the_derived_file() {
    let site = Site::new();

    // The primary: its snapshot in a directory of its own.
    let snapshot = site.subdir("snapshots").join("counter.snapshot");
    let config = || standalone(site.config("counter", PRIMARY_KEY, Some(snapshot.clone())));
    let first = config();
    let health = first.health_bind.expect("set");
    let node = start(&addrs(0), first);
    increment(node.addr(), &[1, 2, 3]);
    assert_eq!(value_of(node.addr()), 6);
    wait_for_snapshot(health, &snapshot);
    node.stop();

    // The stale file: a snapshot of another node's history (a value of
    // 1000), placed where the primary's journal derives its snapshot.
    let other = Site::new();
    let other_config = standalone(other.config("counter", PRIMARY_KEY, None));
    let other_health = other_config.health_bind.expect("set");
    let node = start(&addrs(0), other_config);
    increment(node.addr(), &[1_000]);
    wait_for_snapshot(other_health, &other.path().join("counter.snapshot"));
    node.stop();
    std::fs::copy(
        other.path().join("counter.snapshot"),
        site.path().join("counter.snapshot"),
    )
    .expect("place the stale snapshot");

    // Snapshot and journal.
    let node = start(&addrs(0), config());
    assert_eq!(value_of(node.addr()), 6, "recovered from its own snapshot");
    node.stop();

    // The snapshot alone: the journal moved away, as the standard upgrade
    // starts a new binary on a fresh journal beside the snapshot.
    let journal = site.path().join("counter.journal");
    std::fs::remove_file(&journal).expect("remove the journal");
    for (_, archive) in melin_journal::segment::list_archives(&journal).expect("list archives") {
        std::fs::remove_file(archive).expect("remove an archive");
    }
    let node = start(&addrs(0), config());
    assert_eq!(
        value_of(node.addr()),
        6,
        "booted from the snapshot at --snapshot-path, not the stale derived one"
    );
    node.stop();
}

/// A divergent replica resyncs from a primary that serves its snapshot
/// from `--snapshot-path`, and the replica's own snapshot, on another
/// filesystem than its journal, is archived there, beside itself. Before,
/// the primary served only `<journal>.snapshot` (it had none), and the
/// replica's archive renamed its snapshot into the journal's directory,
/// which fails across filesystems.
#[test]
fn a_divergent_replica_archives_its_snapshot_on_its_own_filesystem() {
    let site = Site::new();
    let Some(second_fs) = melin_transport_core::test_fs::second_filesystem_dir(site.path()) else {
        return;
    };
    let replica_snapshots = tempfile::tempdir_in(second_fs).expect("tempdir on the second fs");
    let replica_snapshot = replica_snapshots.path().join("replica.snapshot");
    let replica_config = || site.config("replica", REPLICA_KEY, Some(replica_snapshot.clone()));

    // The replica's own history first, as a standalone primary: a value of
    // 7, and a snapshot of it on the second filesystem.
    let config = standalone(replica_config());
    let health = config.health_bind.expect("set");
    let node = start(&addrs(1), config);
    increment(node.addr(), &[7]);
    wait_for_snapshot(health, &replica_snapshot);
    node.stop();
    let (_, old_header) =
        melin_transport_core::snapshot::load::<Counter>(&replica_snapshot).expect("old snapshot");

    // The primary: another history, its snapshot in a directory of its
    // own. Written standalone, so that it serves without a replica, then
    // restarted to replicate: a node that begins a history waits for a
    // replica before it serves, and the replica must find a snapshot to be
    // served.
    let primary_snapshot = site.subdir("primary-snapshots").join("primary.snapshot");
    let primary_config = || site.config("primary", PRIMARY_KEY, Some(primary_snapshot.clone()));
    let config = standalone(primary_config());
    let health = config.health_bind.expect("set");
    let node = start(&addrs(0), config);
    increment(node.addr(), &[1, 2, 3, 4]);
    wait_for_snapshot(health, &primary_snapshot);
    node.stop();

    let primary_addrs = addrs(0);
    let mut config = primary_config();
    config.replication_bind = Some(primary_addrs.replication());
    let primary_health = config.health_bind.expect("set");
    let primary = start(&primary_addrs, config);

    // The replica rejoins as one: divergent, so resynced from the primary's
    // snapshot.
    let mut config = replica_config();
    config.replica_of = Some(primary_addrs.replication());
    let replica = start(&addrs(1), config);
    wait_for_gauge_within(primary_health, "melin_replica_divergence_total", 1, LIMIT);
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    increment(primary.addr(), &[5]);
    let head = gauge(primary_health, "melin_journal_sequence").expect("the primary's sequence");
    wait_for_gauge_within(
        primary_health,
        "melin_replica_acked_sequence{slot=\"0\"}",
        head,
        LIMIT,
    );
    assert_eq!(value_of(primary.addr()), 15);

    // The old lineage: its journal beside the journal, its snapshot beside
    // the snapshot, on the second filesystem, under one index.
    let archived_snapshot = replica_snapshots
        .path()
        .join("replica.snapshot.divergent.0")
        .join("replica.snapshot");
    let (_, archived_header) = melin_transport_core::snapshot::load::<Counter>(&archived_snapshot)
        .expect("the archived snapshot");
    assert_eq!(
        (archived_header.sequence, archived_header.chain_hash),
        (old_header.sequence, old_header.chain_hash),
        "the archive holds the replica's own old snapshot"
    );
    assert!(
        site.path()
            .join("replica.journal.divergent.0")
            .join("replica.journal")
            .exists(),
        "the old journal is archived beside the journal"
    );

    replica.stop();
    primary.stop();
}

/// A snapshot location the node cannot use is refused at start, naming
/// it, before the node touches its journal: a snapshot path whose
/// directory does not exist, and a first `--snapshot-path` on a node that
/// has only a snapshot at the derived path (it would start a new history
/// beside it).
#[test]
fn a_snapshot_location_the_node_cannot_use_is_refused() {
    let site = Site::new();
    let missing = site.path().join("missing");
    let config = standalone(site.config(
        "counter",
        PRIMARY_KEY,
        Some(missing.join("counter.snapshot")),
    ));
    let err = start(&addrs(0), config)
        .join()
        .expect_err("the node refuses to start");
    assert!(err.contains(&missing.display().to_string()), "{err}");
    assert!(
        !site.path().join("counter.journal").exists(),
        "a refused boot creates no journal"
    );

    let derived = site.path().join("counter.snapshot");
    std::fs::write(&derived, b"the upgrade's snapshot").expect("write the derived snapshot");
    let explicit = site.subdir("snapshots").join("counter.snapshot");
    let config = standalone(site.config("counter", PRIMARY_KEY, Some(explicit.clone())));
    let err = start(&addrs(0), config)
        .join()
        .expect_err("the node refuses to start");
    assert!(err.contains(&derived.display().to_string()), "{err}");
    assert!(err.contains(&explicit.display().to_string()), "{err}");
    assert!(
        !site.path().join("counter.journal").exists(),
        "a refused boot creates no journal"
    );
}
