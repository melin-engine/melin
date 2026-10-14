//! A node in port mode whose replication NIC cannot come up once its
//! client NIC has: the bring-up fails as a whole, the error saying it was
//! the replication NIC, rather than leave a node running on half its NICs.
//!
//! The client NIC is the runner's first node slot's af_packet device, the
//! only device allowed at EAL init (`melin_test_node::dpdk_slots_eal_args`),
//! so it comes up; the replication NIC names a second port, which does not
//! exist, so its group fails after the client's is up. The client group,
//! and the EAL with it, are then released in the order `NodeNics::init`
//! documents.
//!
//! A binary of its own, with one test: the process initializes EAL itself,
//! which it can do once, and so cannot also share the launcher's. Built
//! only with the `dpdk` feature (`required-features` in the manifest), and
//! run under `scripts/dpdk/netns-runner.sh`:
//!
//! ```sh
//! CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run --profile dpdk -p melin-test-node --features dpdk --test nic_groups_replication_fails
//! ```

use melin_dpdk::{DpdkConfig, NicConfig, NodeNics, ReplicationNic};
use melin_test_node::{dpdk_slot, dpdk_slots_eal_args};

#[test]
fn a_replication_nic_failing_after_the_client_nic_fails_the_bring_up() {
    let seat = dpdk_slot(0);
    let client = NicConfig {
        eal_args: dpdk_slots_eal_args(&[0]),
        port_ids: vec![0],
        ip_addr: seat.ip,
        prefix_len: seat.prefix_len,
        ..NicConfig::default()
    };
    // Port 1: past the one device EAL was given. Its own address, and no
    // EAL arguments, which it shares with the client NIC.
    let replication = NicConfig {
        eal_args: Vec::new(),
        port_ids: vec![1],
        ip_addr: std::net::Ipv4Addr::new(10, 99, 0, 250),
        ..client.clone()
    };
    let config = DpdkConfig {
        nic: client,
        listen_port: 9876,
        num_queues: 1,
        replication: ReplicationNic::Port(replication),
        ..DpdkConfig::default()
    };

    let Err(error) = NodeNics::init(&config) else {
        panic!("a replication NIC on a port that does not exist cannot come up");
    };
    let error = error.to_string();
    // The client NIC came up (its port exists): the failure is the
    // replication NIC's, after it.
    assert_eq!(
        error, "replication NIC: DPDK port 1 not found (available: 1)",
        "{error}"
    );
}
