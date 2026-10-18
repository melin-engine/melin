//! A primary and its replicas for the halt tests (`halt_refusal.rs`,
//! `degraded_acks.rs`): the counter application, started through
//! `melin-test-node` (kernel TCP by default, DPDK under its feature and
//! the netns runner). The tests read the nodes through
//! `melin_test_node::probe`.
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy. It carries no dead-code allowance: an item
//! one includer leaves unused warns there, and is moved or dropped rather
//! than hidden.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{Counter, RequestDecoder, ResponseEncoder};
use ed25519_dalek::SigningKey;
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;

/// Port range for `free_addr`, shared with the other cluster tests: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because every binary using it is in nextest's `cluster-serial` group,
/// so they never run at the same time.
const PORT_BASE: u16 = 10_000;

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
/// as the client's.
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
    fn node_config(&self, name: &str, key: &SigningKey, policy: AckPolicy) -> ServerConfig {
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
