//! A transport dropped while its connections are open, and a new one built
//! on the same queue after it, as a promoted replica does in queue and port
//! modes: the receiver's transport on the replication queue or NIC goes,
//! and the replication poll thread builds its own there.
//!
//! The dropped transport holds received data it was never asked to read
//! (mbufs a zero-copy socket keeps until it is read or reset): dropping it
//! gives every one of them back to the pool, which the pool's own count
//! shows, and its stack state goes with it, so that the new transport, on
//! the same queue, dials and carries TCP as a fresh one does. And a
//! transport left unpolled after it is built, as the promoted replica's
//! client transport was, polls first at the time it does so, not at its
//! build's.
//!
//! On the runner's first two slots' devices, both allowed at EAL init
//! (`melin_test_node::dpdk_slots_eal_args`), brought up as a node in port
//! mode brings up its NICs: the client NIC listens, and the replication
//! NIC's transports dial it.
//!
//! A binary of its own, with one test: the process initializes EAL itself,
//! which it can do once, and so cannot also share the launcher's. Built
//! only with the `dpdk` feature (`required-features` in the manifest), and
//! run under `scripts/dpdk/netns-runner.sh`:
//!
//! ```sh
//! CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run --profile dpdk -p melin-test-node --features dpdk --test transport_rebuild
//! ```

mod common;

use std::net::SocketAddrV4;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{dial, exchange, poll_until};
use melin_dpdk::{DpdkConfig, DpdkTransport, NicConfig, NodeNics, ReplicationNic};
use melin_test_node::{DpdkSeat, dpdk_slot, dpdk_slots_eal_args};

/// The client NIC's listener.
const CLIENT_PORT: u16 = 9876;

/// What the client NIC sends the first transport and it never reads: many
/// segments, so many mbufs held.
const UNREAD: usize = 64 * 1024;

/// How long the client NIC's transport sits unpolled after it is built.
const UNPOLLED: Duration = Duration::from_millis(500);

/// What the stack's clock may trail the wall clock by over [`UNPOLLED`]
/// (the two drift apart), well short of it.
const CLOCK_SLACK: Duration = Duration::from_millis(100);

#[test]
fn a_transport_dropped_with_data_unread_gives_its_mbufs_back_and_its_queue_serves_again() {
    let client_seat = dpdk_slot(0);
    let replication_seat = dpdk_slot(1);
    let eal_args = dpdk_slots_eal_args(&[0, 1]);
    let nic = |seat: &DpdkSeat, port: u16, peer: &DpdkSeat| NicConfig {
        eal_args: eal_args.clone(),
        port_ids: vec![port],
        ip_addr: seat.ip,
        prefix_len: seat.prefix_len,
        peer_mac: Some(peer.mac),
        ..NicConfig::default()
    };
    let replication_nic = nic(&replication_seat, 1, &client_seat);
    let config = DpdkConfig {
        nic: nic(&client_seat, 0, &replication_seat),
        listen_port: CLIENT_PORT,
        num_queues: 1,
        replication: ReplicationNic::Port(replication_nic.clone()),
    };
    let nics = NodeNics::init(&config).expect("bring both NICs up on one EAL");
    let replication = nics.replication.expect("a replication NIC in port mode");
    let mut peer =
        DpdkTransport::from_shared(&nics.client, &config, 0).expect("the client NIC's transport");
    let listener = SocketAddrV4::new(client_seat.ip, CLIENT_PORT);

    // What the replication NIC's pool lends out with no transport on it:
    // nothing on af_packet, whose rings are the kernel's, but read rather
    // than assumed.
    let idle = replication.mbufs_in_use();

    // A transport left unpolled after it is built, as a replica's client
    // transport is until promotion, runs its first poll at the time it is
    // polled, not the time it was built: the frames that poll takes, and
    // the neighbours learned from them, are not stamped as long past. The
    // stack's clock starts at the wall clock's reading (in milliseconds),
    // so the wall clock read just before the poll bounds it from below,
    // give or take the two clocks' drift over the pause.
    std::thread::sleep(UNPOLLED);
    let before = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a wall clock past the epoch")
        .as_millis();
    let first_poll =
        u128::try_from(peer.poll().total_millis()).expect("a stack time past the epoch");
    assert!(
        first_poll + CLOCK_SLACK.as_millis() >= before,
        "the first poll ran at the stack's time {first_poll} ms, {} ms before it was polled",
        before - first_poll
    );

    // The first transport dials, and is sent what it never reads: its
    // socket holds the received mbufs, as many as the data took.
    let mut first = DpdkTransport::on_nic_unlistening(&replication, &replication_nic, 0)
        .expect("the first transport on the replication queue");
    let (_, abandoned) = dial(&mut first, &mut peer, replication_seat.ip, listener, 40_000);
    let unread = vec![0x5a; UNREAD];
    assert!(peer.queue_send(abandoned, &unread), "queue the unread data");
    poll_until(
        "the unread data to be acknowledged",
        &mut [&mut peer, &mut first],
        |transports| transports[0].tx_drained(abandoned),
    );
    let held = replication.mbufs_in_use();
    assert!(
        held > idle,
        "the first transport holds the data it has not read: {held} mbufs out, {idle} idle"
    );

    // Dropped, connection open and data unread: every mbuf comes back.
    drop(first);
    assert_eq!(
        replication.mbufs_in_use(),
        idle,
        "a dropped transport gives back every mbuf it held ({held} were out)"
    );

    // A new transport on the same queue carries TCP as a fresh one does,
    // from another local port: the peer still holds the first link.
    let mut second = DpdkTransport::on_nic_unlistening(&replication, &replication_nic, 0)
        .expect("a second transport on the same queue");
    let (dialed, accepted) = dial(
        &mut second,
        &mut peer,
        replication_seat.ip,
        listener,
        40_001,
    );
    exchange(&mut second, dialed, &mut peer, accepted, b"request");
    exchange(&mut peer, accepted, &mut second, dialed, b"reply");
    second.close(dialed);
    peer.close(accepted);
    // Nothing of the first transport's connection reached the second's
    // stack (it never knew it), so the peer's end is the peer's to end.
    peer.reset(abandoned);

    // Everything read, nothing left out of the pool once it goes too.
    drop(second);
    assert_eq!(replication.mbufs_in_use(), idle);

    drop(peer);
    drop(nics.client);
    drop(replication);
}
