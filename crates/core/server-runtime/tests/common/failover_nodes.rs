//! What the failover tests (`raft_failover`, `replicated_failover`) keep
//! about each node of their three-node clusters, and the diagnostics they
//! print when a wait times out.

use std::net::SocketAddr;

use melin_client::SigningKey;
use melin_test_node::Addrs;
use melin_test_node::probe::http_metrics;

pub struct NodeSetup {
    pub key: SigningKey,
    /// Client and replication addresses.
    pub addrs: Addrs,
    /// Where the node's raft listener binds.
    pub raft_addr: SocketAddr,
    pub health_addr: SocketAddr,
    pub admin_addr: SocketAddr,
}

/// Per-node control-plane state for deadline diagnostics: the raft and
/// replication gauges of every node, so a timed-out wait reports *which*
/// formation condition was stuck (replication attach vs. election) and
/// what each node believed at the time — the servers install no tracing
/// subscriber under test, so the panic message is the only record.
pub fn cluster_summary(nodes: &[NodeSetup]) -> String {
    nodes
        .iter()
        .enumerate()
        .map(|(i, n)| {
            let gauges = match http_metrics(n.health_addr) {
                None => "health endpoint unreachable".to_owned(),
                Some(body) => body
                    .lines()
                    .filter(|l| {
                        l.starts_with("melin_raft_") || l.starts_with("melin_replicas_connected")
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
            };
            format!("node {}: {gauges}", i + 1)
        })
        .collect::<Vec<_>>()
        .join("\n")
}
