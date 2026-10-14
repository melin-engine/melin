//! Replication on a queue of its own, brought up on a port that offers
//! one queue pair: refused at configure, naming the port and the queue
//! pairs it lacks, rather than run with replication on the client queue.
//!
//! The runner's af_packet device is given one queue pair, so the port
//! reports one RX and one TX queue and the split's second pair is missing
//! before anything is configured. `queue_split` covers a port given both.
//!
//! A binary of its own, with one test: the process initializes EAL itself,
//! which it can do once, and so cannot also share the launcher's. Built
//! only with the `dpdk` feature (`required-features` in the manifest), and
//! run under `scripts/dpdk/netns-runner.sh`:
//!
//! ```sh
//! CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run --profile dpdk -p melin-test-node --features dpdk --test queue_split_one_queue
//! ```

use melin_dpdk::{DpdkConfig, NicConfig, NodeNics, ReplicationNic, ReplicationPorts};
use melin_test_node::{dpdk_slot, dpdk_slots_eal_args};

#[test]
fn a_port_with_one_queue_pair_cannot_split_off_replication() {
    let seat = dpdk_slot(0);
    let config = DpdkConfig {
        nic: NicConfig {
            eal_args: dpdk_slots_eal_args(&[0]),
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
        ..DpdkConfig::default()
    };

    let Err(error) = NodeNics::init(&config) else {
        panic!("a port with one queue pair cannot carry the replication split");
    };
    let error = error.to_string();
    assert!(
        error.starts_with("port 0 configure: 2 queue pairs are needed, and the NIC offers 1"),
        "{error}"
    );
}
