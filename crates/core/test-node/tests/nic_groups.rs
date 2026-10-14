//! Two NIC groups on one EAL, as a node whose replication has a NIC of its
//! own runs them: the client NIC and the replication NIC, each with its
//! own port, pool and MAC, brought up by `melin_dpdk::NodeNics` on an EAL
//! the process owns, as a deployed node does.
//!
//! The NICs are the runner's first two node slots' af_packet devices, both
//! allowed at EAL init (`melin_test_node::dpdk_slots_eal_args`). Each dials
//! the other's listener and exchanges a message each way. The slots'
//! interfaces meet only on the runner's bridge, which never sends a frame
//! back out of the port it came in on, so a connection between the two
//! exists only if each group really transmits and receives on its own
//! port. Then the client group is dropped, and the replication group still
//! serves a kernel-TCP client: the EAL outlives the first group, and is
//! cleaned up with the last.
//!
//! A binary of its own, with one test: the process initializes EAL itself,
//! which it can do once, and so cannot also share the launcher's. Built
//! only with the `dpdk` feature (`required-features` in the manifest), and
//! run under `scripts/dpdk/netns-runner.sh`:
//!
//! ```sh
//! CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run --profile dpdk -p melin-test-node --features dpdk --test nic_groups
//! ```

use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use melin_dpdk::{DpdkConfig, DpdkTransport, NicConfig, NodeNics, ReplicationNic, SocketHandle};
use melin_test_node::{DpdkSeat, dpdk_slot, dpdk_slots_eal_args};

/// How long any one step may take: generous for a shared CI runner, and
/// bounded so that a hang fails here, naming the step.
const LIMIT: Duration = Duration::from_secs(20);

/// The ports the client NIC's and the replication NIC's transports listen
/// on. Any two would do: the process owns both addresses outright.
const CLIENT_PORT: u16 = 9876;
const REPLICATION_PORT: u16 = 9877;

const REQUEST: &[u8] = b"request";
const REPLY: &[u8] = b"reply";

#[test]
fn two_nic_groups_on_one_eal_each_carry_tcp_on_their_own_port() {
    let client_seat = dpdk_slot(0);
    let replication_seat = dpdk_slot(1);
    // Ports 0 and 1, in that order: the client NIC's first.
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

    let NodeNics {
        client,
        replication,
    } = NodeNics::init(&config).expect("bring both NICs up on one EAL");
    let replication = replication.expect("a replication NIC in port mode");
    // Each group drives its own port: each takes its own interface's MAC.
    assert_eq!(client.mac, client_seat.mac);
    assert_eq!(replication.mac, replication_seat.mac);
    assert_eq!(client.num_queues, 1);
    assert_eq!(replication.num_queues, 1);
    assert_ne!(client.mempool_raw, replication.mempool_raw);

    let mut client_transport =
        DpdkTransport::from_shared(&client, &config, 0).expect("the client NIC's transport");
    let replication_config = DpdkConfig {
        nic: replication_nic,
        listen_port: REPLICATION_PORT,
        num_queues: 1,
        replication: ReplicationNic::Shared,
    };
    let mut replication_transport =
        DpdkTransport::from_shared(&replication, &replication_config, 0)
            .expect("the replication NIC's transport");

    // Each NIC dials the other's listener.
    round_trip(
        &mut client_transport,
        &mut replication_transport,
        client_seat.ip,
        SocketAddrV4::new(replication_seat.ip, REPLICATION_PORT),
    );
    round_trip(
        &mut replication_transport,
        &mut client_transport,
        replication_seat.ip,
        SocketAddrV4::new(client_seat.ip, CLIENT_PORT),
    );

    // The client group goes first, transport then group, and with it its
    // port and pool; the EAL stays, for the replication group.
    drop(client_transport);
    assert_eq!(
        Arc::strong_count(&client),
        1,
        "the transport held the group"
    );
    drop(client);
    serve_kernel_client(
        &mut replication_transport,
        SocketAddrV4::new(replication_seat.ip, REPLICATION_PORT),
    );

    // The last group drops, and the EAL is cleaned up after it.
    drop(replication_transport);
    drop(replication);
}

/// Poll every transport in `transports` until `done` holds, failing the
/// test after [`LIMIT`], naming `what`.
fn poll_until(
    what: &str,
    transports: &mut [&mut DpdkTransport],
    mut done: impl FnMut(&mut [&mut DpdkTransport]) -> bool,
) {
    let deadline = Instant::now() + LIMIT;
    loop {
        for transport in transports.iter_mut() {
            transport.poll();
        }
        if done(transports) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
    }
}

/// Dial `to`, `acceptor`'s listener, from `dialer`, whose address is
/// `dialer_ip`, send [`REQUEST`] and answer it with [`REPLY`], then close
/// both ends.
fn round_trip(
    dialer: &mut DpdkTransport,
    acceptor: &mut DpdkTransport,
    dialer_ip: Ipv4Addr,
    to: SocketAddrV4,
) {
    // Any port: the dialer has no other connection.
    let local_port = 40_000;
    // As a replica dials its primary: the peer's MAC seeded, not resolved.
    assert_eq!(
        dialer.seed_peer_neighbor(*to.ip()),
        melin_dpdk::PeerMacSource::Supplied
    );
    let dialed = dialer
        .connect_to(*to.ip(), to.port(), local_port)
        .unwrap_or_else(|e| panic!("dial {to}: {e:?}"));
    let mut accepted = Vec::new();
    poll_until(
        &format!("the connection to {to}"),
        &mut [&mut *dialer, &mut *acceptor],
        |transports| {
            accepted.extend(transports[1].take_accepted());
            !accepted.is_empty() && transports[0].is_connected(dialed)
        },
    );
    assert_eq!(accepted.len(), 1, "one connection dialed");
    let accepted = accepted.remove(0);
    assert_eq!(accepted.listen_port, to.port());
    assert_eq!(accepted.peer, SocketAddr::from((dialer_ip, local_port)));

    exchange(dialer, dialed, acceptor, accepted.handle, REQUEST);
    exchange(acceptor, accepted.handle, dialer, dialed, REPLY);
    dialer.close(dialed);
    acceptor.close(accepted.handle);
}

/// Send `message` from `from`'s connection to `to`'s, and check that it
/// arrives whole.
fn exchange(
    from: &mut DpdkTransport,
    sender: SocketHandle,
    to: &mut DpdkTransport,
    receiver: SocketHandle,
    message: &[u8],
) {
    assert!(from.queue_send(sender, message), "queue {message:?}");
    let mut received = Vec::new();
    poll_until(
        &format!("{:?}", String::from_utf8_lossy(message)),
        &mut [&mut *from, &mut *to],
        |transports| {
            transports[1].recv_into_vec(receiver, &mut received);
            received.len() >= message.len()
        },
    );
    assert_eq!(received, message);
}

/// Accept a kernel-TCP client on `transport`'s listener at `addr`, from
/// the runner's bridge, and answer its [`REQUEST`] with [`REPLY`].
fn serve_kernel_client(transport: &mut DpdkTransport, addr: SocketAddrV4) {
    let client = std::thread::spawn(move || {
        let mut stream = TcpStream::connect_timeout(&addr.into(), LIMIT)
            .unwrap_or_else(|e| panic!("connect to {addr} from the kernel: {e}"));
        stream.set_read_timeout(Some(LIMIT)).expect("read timeout");
        stream.write_all(REQUEST).expect("send the request");
        let mut reply = vec![0; REPLY.len()];
        stream.read_exact(&mut reply).expect("read the reply");
        reply
    });

    let mut accepted = Vec::new();
    poll_until(
        "the kernel client's connection",
        &mut [&mut *transport],
        |transports| {
            accepted.extend(transports[0].take_accepted());
            !accepted.is_empty()
        },
    );
    let handle = accepted.remove(0).handle;
    let mut received = Vec::new();
    poll_until(
        "the kernel client's request",
        &mut [&mut *transport],
        |transports| {
            transports[0].recv_into_vec(handle, &mut received);
            received.len() >= REQUEST.len()
        },
    );
    assert_eq!(received, REQUEST);
    assert!(transport.queue_send(handle, REPLY), "queue the reply");
    poll_until(
        "the kernel client's reply",
        &mut [&mut *transport],
        |transports| transports[0].tx_drained(handle),
    );
    assert_eq!(client.join().expect("kernel client thread"), REPLY);
    transport.close(handle);
}
