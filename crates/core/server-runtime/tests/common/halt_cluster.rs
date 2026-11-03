//! A primary and its replicas for the cluster tests (`halt_refusal.rs`,
//! `degraded_acks.rs`, `tick_watermark.rs`): the application a test
//! serves ([`Served`]), started through `melin-test-node` (kernel TCP by
//! default, DPDK under its feature and the netns runner). The tests read
//! the nodes through `melin_test_node::probe`.
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy. It carries no dead-code allowance: an item
//! one includer leaves unused warns there, and is moved or dropped rather
//! than hidden.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::Counter;
use ed25519_dalek::SigningKey;
use melin_app::Application;
use melin_app::decoder::RequestDecoder;
use melin_app::encoder::ResponseEncoder;
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::{Addrs, Node};
use melin_transport_core::test_ports::free_addr;
use melin_transport_core::test_support::{TickCountingApp, TickCountingCodec};

/// Port range for `free_addr`, shared with the other cluster tests: every
/// other range below the ephemeral floor is taken. Their tests may run at
/// the same time: `free_addr` keeps concurrent test processes apart
/// within a range.
const PORT_BASE: u16 = 10_000;

/// Node `slot`'s addresses (`melin_test_node::addrs`), from this file's
/// port range on kernel TCP.
pub fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, endpoint)
}

/// A free loopback address from this file's port range, for an endpoint
/// that is kernel TCP on either transport (health, admin).
pub fn endpoint() -> SocketAddr {
    free_addr(PORT_BASE)
}

/// An application the cluster tests serve, with the codec its clients
/// speak. A trait rather than a codec passed at every call, so that a
/// test names the application once, and the codec cannot be paired with
/// another application's.
pub trait Served: Application<Sizing = ()> + Send + 'static {
    fn decoder() -> impl RequestDecoder<Event = Self::Event> + 'static;
    fn encoder()
    -> impl ResponseEncoder<Report = Self::Report, Query = Self::QueryResponse> + 'static;
}

impl Served for Counter {
    fn decoder() -> impl RequestDecoder<Event = Self::Event> + 'static {
        counter_server::RequestDecoder
    }
    fn encoder()
    -> impl ResponseEncoder<Report = Self::Report, Query = Self::QueryResponse> + 'static {
        counter_server::ResponseEncoder
    }
}

impl Served for TickCountingApp {
    fn decoder() -> impl RequestDecoder<Event = Self::Event> + 'static {
        TickCountingCodec
    }
    fn encoder()
    -> impl ResponseEncoder<Report = Self::Report, Query = Self::QueryResponse> + 'static {
        TickCountingCodec
    }
}

/// A node of application `A` at `addrs`, with no startup events.
pub fn spawn_node<A: Served>(addrs: &Addrs, config: ServerConfig) -> Node
where
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    melin_test_node::start_at::<A>(
        addrs,
        config,
        StartupEvents::none(),
        (),
        A::decoder(),
        A::encoder(),
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
    /// replication key `key`, the policy, no ticks, no snapshots, no health
    /// endpoint, and no replication endpoint or primary.
    pub fn node_config(&self, name: &str, key: &SigningKey, policy: AckPolicy) -> ServerConfig {
        let key_path = self.tmp.path().join(format!("{name}.key"));
        std::fs::write(&key_path, key.to_bytes()).expect("write node key");
        ServerConfig {
            journal: self.tmp.path().join(format!("{name}.journal")),
            authorized_keys: self.auth_path.clone(),
            ack_policy: policy,
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
        config.health_bind = Some(endpoint());
        config
    }

    /// A replica of the primary whose replication endpoint is `primary`
    /// (a node's `Addrs::replication`, or a scripted primary's address),
    /// journaling to `name`. No health endpoint: the tests read the
    /// primary's, and a port saved per replica keeps a binary run in one
    /// process (plain `cargo test`) inside its port block.
    pub fn replica_config(
        &self,
        name: &str,
        primary: SocketAddr,
        policy: AckPolicy,
    ) -> ServerConfig {
        let mut config = self.node_config(name, &self.replica_key, policy);
        config.replica_of = Some(primary);
        config
    }

    /// A client of `node`, connected within the startup limit.
    pub fn client(&self, node: &Node) -> Connection {
        let deadline = Instant::now() + Duration::from_secs(30);
        Connection::connect_by(node.addr(), &self.client_key, deadline)
            .expect("client connects to the node")
    }
}
