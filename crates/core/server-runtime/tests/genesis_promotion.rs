//! End-to-end contract for the promotion check on the genesis: a
//! replica promoted before it holds its lineage's whole genesis refuses,
//! and one that holds it is promoted — judged on the genesis length the
//! primary recorded in the lineage, never on the genesis the replica is
//! configured with.
//!
//! Two cases, each with one replica (counter app, `disk` ack policy)
//! promoted by the operator's `PROMOTE` over the admin endpoint:
//!
//! - The protected case. A brand-new cluster whose primary dies while
//!   its first replica is still copying the genesis entry by entry. The
//!   primary is scripted, so it can die exactly two entries into a
//!   three-entry genesis; the replica is configured with no genesis at
//!   all. It must refuse the promotion and exit with the reason.
//! - A replica configured with a larger genesis than its primary's, which
//!   holds the primary's whole genesis, against a real primary. It must
//!   be promoted and serve the primary's state.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`). The scripted primary is a
//! kernel socket of the test's own, on the address the nodes reach the
//! test at.

#[path = "common/scripted_primary.rs"]
mod scripted_primary;

use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{Counter, CounterEvent, RequestDecoder, ResponseEncoder};
use ed25519_dalek::SigningKey;
use melin_journal::JournalReader;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::probe::admin_reply_when_up;
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;

use scripted_primary::ScriptedPrimary;

/// Port range for `free_addr`, shared with the other cluster binaries,
/// whose tests may run at the same time: `free_addr` keeps concurrent
/// test processes apart within a range.
const PORT_BASE: u16 = 10_000;

/// Node `slot`'s addresses (`melin_test_node::addrs`), from this file's
/// port range on kernel TCP.
fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, || free_addr(PORT_BASE))
}

fn spawn_node(addrs: &Addrs, config: ServerConfig, startup: StartupEvents<CounterEvent>) -> Node {
    melin_test_node::start_at::<Counter>(
        addrs,
        config,
        startup,
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

fn genesis(amounts: &[u64]) -> StartupEvents<CounterEvent> {
    StartupEvents {
        genesis: amounts
            .iter()
            .map(|&amount| CounterEvent::Increment { amount })
            .collect(),
        on_primary: Vec::new(),
    }
}

/// Keys and configuration shared by a test's nodes.
struct Cluster {
    tmp: tempfile::TempDir,
    operator_key: SigningKey,
    auth_path: std::path::PathBuf,
}

impl Cluster {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let primary_key = SigningKey::from_bytes(&[0x81; 32]);
        let replica_key = SigningKey::from_bytes(&[0x82; 32]);
        let operator_key = SigningKey::from_bytes(&[0x83; 32]);
        let b64 = |k: &SigningKey| {
            base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
        };
        let auth_path = tmp.path().join("authorized_keys");
        std::fs::write(
            &auth_path,
            format!(
                "replication {} primary\nreplication {} replica\noperator {} operator\n",
                b64(&primary_key),
                b64(&replica_key),
                b64(&operator_key)
            ),
        )
        .expect("write authorized_keys");
        std::fs::write(tmp.path().join("primary.key"), primary_key.to_bytes())
            .expect("write primary key");
        std::fs::write(tmp.path().join("replica.key"), replica_key.to_bytes())
            .expect("write replica key");
        Self {
            tmp,
            operator_key,
            auth_path,
        }
    }

    fn config(&self, name: &str) -> ServerConfig {
        ServerConfig {
            journal: self.tmp.path().join(format!("{name}.journal")),
            authorized_keys: self.auth_path.clone(),
            ack_policy: AckPolicy::Disk,
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            // Off: the default port would collide between the nodes.
            health_bind: None,
            admin_bind: Some(free_addr(PORT_BASE)),
            replication_key: Some(self.tmp.path().join(format!("{name}.key"))),
            ..melin_test_node::config()
        }
    }
}

/// The counter's value, read by a client of the node at `addr` once it
/// serves.
fn value_at(addr: SocketAddr, key: &SigningKey) -> u64 {
    use counter_server::{GET_VALUE_REQUEST, KIND_RESP_VALUE};
    use melin_client::Connection;

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = Connection::connect_by(addr, key, deadline).expect("client connects");
    let reply = conn
        .request_one(&GET_VALUE_REQUEST)
        .expect("value request answered");
    assert_eq!(reply[0], KIND_RESP_VALUE);
    u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"))
}

/// How many entries the journal at `path` holds; 0 while it does not
/// exist yet.
fn journaled_entries(path: &std::path::Path) -> usize {
    let Ok(mut reader) = JournalReader::<CounterEvent>::open(path) else {
        return 0;
    };
    let mut entries = 0;
    while let Ok(Some(_)) = reader.next_entry() {
        entries += 1;
    }
    entries
}

/// The protected case: the primary dies two entries into a three-entry
/// genesis, and the replica — configured with no genesis at all — learned
/// the genesis length from it, so it refuses the promotion.
#[test]
fn a_replica_configured_without_genesis_refuses_promotion_mid_genesis() {
    let cluster = Cluster::new();

    // A scripted primary: a new cluster's, whose journal records a
    // genesis of three entries.
    let primary = ScriptedPrimary::bind();
    let mut replica_config = cluster.config("replica");
    replica_config.replica_of = Some(primary.addr());
    let replica_admin = replica_config.admin_bind.expect("set above");
    let replica_journal = replica_config.journal.clone();
    let replica = spawn_node(&addrs(0), replica_config, StartupEvents::none());
    let replica_client = replica.addr();

    let mut stream = primary.accept(Some(3));
    assert_eq!(stream.resumes_after(), 0, "a fresh replica");
    // The first two genesis entries.
    stream.send(
        1,
        1,
        &[
            CounterEvent::Increment { amount: 1_000 },
            CounterEvent::Increment { amount: 2_000 },
        ],
    );
    stream.wait_acked(2);
    // The primary dies.
    drop(stream);
    drop(primary);

    admin_reply_when_up(replica_admin, &cluster.operator_key, "PROMOTE");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !replica.is_finished() {
        assert!(
            Instant::now() < deadline,
            "the replica neither refused the promotion nor exited"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let err = replica.join().expect_err("the promotion must be refused");
    assert!(err.contains("refusing promotion"), "{err}");
    assert!(err.contains("genesis takes 3"), "{err}");
    assert_eq!(
        journaled_entries(&replica_journal),
        2,
        "the partial copy is left as it was"
    );
    assert!(
        TcpStream::connect_timeout(&replica_client, Duration::from_millis(300)).is_err(),
        "a refused promotion serves no client"
    );
}

/// A replica configured with a larger genesis than its primary's holds
/// the primary's whole genesis: it is promoted, and serves the primary's
/// state — its own configured genesis plays no part. On DPDK it serves
/// as a DPDK primary, at its own address.
#[test]
fn a_replica_configured_with_a_larger_genesis_is_promoted() {
    const GENESIS: u64 = 1_000_000;
    let cluster = Cluster::new();

    let primary_addrs = addrs(0);
    let mut primary_config = cluster.config("primary");
    primary_config.replication_bind = Some(primary_addrs.replication());
    let primary = spawn_node(&primary_addrs, primary_config, genesis(&[GENESIS]));

    let mut replica_config = cluster.config("replica");
    replica_config.replica_of = Some(primary_addrs.replication());
    let replica_admin = replica_config.admin_bind.expect("set above");
    let replica_journal = replica_config.journal.clone();
    let replica = spawn_node(&addrs(1), replica_config, genesis(&[1, 2, 4]));

    // The primary's whole genesis — one entry — is on the replica's disk.
    let deadline = Instant::now() + Duration::from_secs(60);
    while journaled_entries(&replica_journal) < 1 {
        assert!(
            !primary.is_finished(),
            "the primary exited: {:?}",
            primary.join()
        );
        assert!(
            !replica.is_finished(),
            "the replica exited: {:?}",
            replica.join()
        );
        assert!(
            Instant::now() < deadline,
            "the replica never copied the genesis"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    primary.stop();

    admin_reply_when_up(replica_admin, &cluster.operator_key, "PROMOTE");
    assert_eq!(
        value_at(replica.addr(), &cluster.operator_key),
        GENESIS,
        "the promoted replica serves the primary's genesis, not its own"
    );
    replica.stop();
}
