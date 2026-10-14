//! Replication on a queue of its own, brought up on a NIC that cannot
//! steer: refused at boot, naming the port and what it lacks, rather than
//! run with client and replication traffic spread across both queues.
//!
//! The runner's af_packet devices are the only NICs here, and they have
//! neither `rte_flow` nor an RSS redirection table, so the steering itself
//! (the rules, and the table pinned to the client queue) is checked on an
//! mlx5 rig, not here. What does run here is everything before it: the
//! node's client NIC configured with the two queue pairs of the
//! replication split (the device is given two) in RSS mode hashing nothing,
//! and started; then the redirection table's pin, which the device
//! refuses, failing the bring-up.
//!
//! A binary of its own, with one test: the process initializes EAL itself,
//! which it can do once, and so cannot also share the launcher's. Built
//! only with the `dpdk` feature (`required-features` in the manifest), and
//! run under `scripts/dpdk/netns-runner.sh`:
//!
//! ```sh
//! CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run --profile dpdk -p melin-test-node --features dpdk --test queue_split
//! ```

use melin_dpdk::{DpdkConfig, NicConfig, NodeNics, ReplicationNic, ReplicationPorts};
use melin_test_node::{dpdk_slot, dpdk_slots_eal_args_with_queue_pairs};

#[test]
fn a_nic_without_a_redirection_table_cannot_split_off_replication() {
    let seat = dpdk_slot(0);
    let config = DpdkConfig {
        nic: NicConfig {
            eal_args: dpdk_slots_eal_args_with_queue_pairs(&[0], 2),
            port_ids: vec![0],
            ip_addr: seat.ip,
            prefix_len: seat.prefix_len,
            ..NicConfig::default()
        },
        listen_port: 9876,
        num_queues: 1,
        replication: ReplicationNic::Queue(ReplicationPorts {
            listen: Some(9877),
            primary: None,
        }),
    };

    let Err(error) = NodeNics::init(&config) else {
        panic!("an af_packet device has no redirection table to pin");
    };
    let error = error.to_string();
    // Past configure and start, with both queue pairs: the pin is the
    // first step to fail, and it names the port.
    assert!(
        error.starts_with("port 0: pinning the RSS redirection table to queue 0 failed: "),
        "{error}"
    );
}
