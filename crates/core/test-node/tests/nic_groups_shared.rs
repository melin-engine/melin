//! A node's NICs in shared mode: one group, the client NIC's, with one
//! client queue and no replication NIC, brought up by `melin_dpdk::NodeNics`
//! on an EAL the process owns, as a deployed node does. And a thread the
//! node spawns itself registered with that EAL as an lcore of its own, as
//! the replication poll thread is in the modes that run it, and refused
//! once every lcore id is taken.
//!
//! Queue mode, the other mode with no replication NIC, cannot come up on
//! the runner's af_packet devices (`queue_split`, `queue_split_one_queue`):
//! its client queue count is checked on every host, in
//! `melin_dpdk::steering`'s tests.
//!
//! A binary of its own, with one test: the process initializes EAL itself,
//! which it can do once, and so cannot also share the launcher's. Built
//! only with the `dpdk` feature (`required-features` in the manifest), and
//! run under `scripts/dpdk/netns-runner.sh`:
//!
//! ```sh
//! CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run --profile dpdk -p melin-test-node --features dpdk --test nic_groups_shared
//! ```

use std::sync::Arc;

use melin_dpdk::{DpdkConfig, DpdkShared, NicConfig, NodeNics, ReplicationNic};
use melin_test_node::{dpdk_slot, dpdk_slots_eal_args};

/// What `rte_lcore_id` reads on a thread with no lcore.
const LCORE_ID_ANY: u32 = u32::MAX;

#[test]
fn shared_mode_brings_up_the_client_nic_alone_and_a_thread_registers_on_it() {
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
        replication: ReplicationNic::Shared,
        ..DpdkConfig::default()
    };

    let NodeNics {
        client,
        replication,
    } = NodeNics::init(&config).expect("bring the client NIC up");
    assert!(replication.is_none(), "shared mode has no replication NIC");
    assert_eq!(client.mac, seat.mac);
    assert_eq!(client.num_queues, 1);
    assert_eq!(client.client_queues(), 1);

    // A thread of the node's own gets an lcore id of its own, and gives it
    // back when its registration drops, so the next thread gets one too.
    let first = registered_lcore(&client);
    let second = registered_lcore(&client);
    for lcore in [first, second] {
        assert_ne!(lcore, LCORE_ID_ANY, "registered, the thread has an lcore");
    }
    assert_eq!(
        first, second,
        "the first thread's lcore was released when its registration dropped"
    );
    // Both threads have returned, and with them their share of the group.
    assert_eq!(Arc::strong_count(&client), 1);

    // Once every lcore id is taken, EAL refuses the next thread, and the
    // error says why: the replication poll thread then runs without one.
    let error = exhaust_lcores(&client);
    assert_eq!(error.errno, libc::ENOMEM, "every lcore id is taken");
    assert_eq!(
        error.to_string(),
        format!(
            "rte_thread_register failed: {}",
            std::io::Error::from_raw_os_error(libc::ENOMEM)
        )
    );
    assert_eq!(Arc::strong_count(&client), 1);
}

/// Register threads with `group`'s EAL, each holding its registration,
/// until one is refused; return that refusal, once every thread has let
/// its lcore go. Bounded: EAL has a fixed table of lcores (`RTE_MAX_LCORE`).
fn exhaust_lcores(group: &Arc<DpdkShared>) -> melin_dpdk::LcoreRegisterError {
    use std::sync::mpsc;

    // Generous over any `RTE_MAX_LCORE` a distribution builds with.
    const MAX_THREADS: usize = 1024;
    // Nothing is ever sent: dropping the sender is the release, which
    // every holder sees as its `recv` failing. A `Mutex` around the one
    // receiver, as the holders share it.
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let mut holders = Vec::new();
    let refusal = loop {
        assert!(
            holders.len() < MAX_THREADS,
            "EAL never ran out of lcores in {MAX_THREADS} threads"
        );
        let group = Arc::clone(group);
        let release_rx = Arc::clone(&release_rx);
        let (registered_tx, registered_rx) = mpsc::channel();
        let holder = std::thread::spawn(move || {
            let registration = group.register_thread();
            let refused = registration.as_ref().err().copied();
            registered_tx
                .send(refused)
                .expect("report the registration");
            if refused.is_none() {
                // Hold the lcore until released. Dropped result: the
                // release is the `recv` failing, as nothing is sent.
                let _ = release_rx.lock().expect("the release channel").recv();
            }
            drop(registration);
        });
        holders.push(holder);
        if let Some(refusal) = registered_rx.recv().expect("the registration's outcome") {
            break refusal;
        }
    };
    drop(release_tx);
    for holder in holders {
        holder.join().expect("a registering thread");
    }
    refusal
}

/// The lcore id a new thread is given when it registers with `group`'s
/// EAL, the registration dropped as the thread ends.
fn registered_lcore(group: &Arc<DpdkShared>) -> u32 {
    let group = Arc::clone(group);
    std::thread::spawn(move || {
        let registration = group.register_thread().expect("register with EAL");
        registration.lcore_id()
    })
    .join()
    .expect("the registering thread")
}
