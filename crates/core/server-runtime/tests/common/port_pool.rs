//! Loopback ports a finished test hands back for the next one to take, for
//! the test binaries whose tests together take more ports than
//! `free_addr` gives one process (`ack_policy_changes.rs`,
//! `raft_failover.rs`), and for every test on `halt_cluster.rs`, through
//! its lease.
//!
//! Under plain `cargo test` every test in a binary shares one process, and
//! so one block of `free_addr` ports, which a fresh set per test would
//! exhaust (see `melin_transport_core::test_ports::free_addr`). Reuse
//! bounds the block's use only because the tests run one at a time
//! (`#[serial]`, or the turn `halt_cluster`'s lease holds): run at once,
//! they would hold more ports than the block has, whatever is handed back.
//!
//! Each test binary that uses it includes this file as a module (`#[path]`,
//! directly or through `halt_cluster.rs`), and compiles its own copy. It
//! carries no dead-code allowance: an item one includer leaves unused warns
//! there, and is moved or dropped rather than hidden.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Mutex;

use melin_test_node::Addrs;
use melin_transport_core::test_ports::free_addr;

/// Ports finished tests handed back, each as the address its listener
/// bound, which is the one probed. A `Vec`: a handful of ports, scanned
/// for one that binds and taken out in no particular order.
static RETURNED_PORTS: Mutex<Vec<SocketAddr>> = Mutex::new(Vec::new());

/// A port on 127.0.0.1 no running test holds: one handed back that binds
/// now on the address it was bound on, or a new one from the caller's
/// `free_addr` range starting at `base`. A stopped node's endpoint threads
/// may outlive it briefly, still bound: such a port stays in the pool, for
/// a later test. Probing the bound address matters for a listener on
/// another loopback IP: binding 127.0.0.1 succeeds while 127.0.0.2 holds
/// the same port.
pub fn port(base: u16) -> SocketAddr {
    let reused = {
        // A test that panicked holding the lock left the pool whole: it
        // only scans and takes out.
        let mut pool = RETURNED_PORTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        pool.iter()
            .position(|addr| std::net::TcpListener::bind(addr).is_ok())
            .map(|index| pool.swap_remove(index))
            .map(|bound| SocketAddr::new(Ipv4Addr::LOCALHOST.into(), bound.port()))
    };
    // Outside the lock: an exhausted block panics this test alone.
    reused.unwrap_or_else(|| free_addr(base))
}

/// Hand a finished test's ports back: a node's addresses on kernel TCP (on
/// DPDK they are the runner slot's, from no pool) and `endpoints`, each
/// one taken from [`port`] and given as the address its listener bound. A
/// test that failed keeps them: it may have left a node bound to one.
pub fn give_back(addrs: &Addrs, endpoints: impl IntoIterator<Item = SocketAddr>) {
    let node = cfg!(not(feature = "dpdk")).then(|| [addrs.client(), addrs.replication()]);
    give_back_ports(node.into_iter().flatten().chain(endpoints));
}

/// Hand a finished test's `ports` back, each one taken from [`port`] and
/// given as the address its listener bound. A test that failed keeps
/// them: it may have left a node bound to one.
pub fn give_back_ports(ports: impl IntoIterator<Item = SocketAddr>) {
    if std::thread::panicking() {
        return;
    }
    RETURNED_PORTS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .extend(ports);
}
