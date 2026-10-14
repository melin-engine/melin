//! What the tests that bring NICs up themselves share: polling transports
//! to a deadline, a dial between two of them, and a message across.
//!
//! A module of each test binary that declares it (`mod common;`), not a
//! test of its own: Cargo builds no target from a file under a
//! subdirectory of `tests/`.

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::{Duration, Instant};

use melin_dpdk::{DpdkTransport, SocketHandle};

/// How long any one step may take: generous for a shared CI runner, and
/// bounded so that a hang fails here, naming the step.
pub const LIMIT: Duration = Duration::from_secs(20);

/// Poll every transport in `transports` until `done` holds, failing the
/// test after [`LIMIT`], naming `what`.
pub fn poll_until(
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
/// `dialer_ip`, from `local_port`, and return the dialed connection and
/// the accepted one, both established.
pub fn dial(
    dialer: &mut DpdkTransport,
    acceptor: &mut DpdkTransport,
    dialer_ip: Ipv4Addr,
    to: SocketAddrV4,
    local_port: u16,
) -> (SocketHandle, SocketHandle) {
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
    (dialed, accepted.handle)
}

/// Send `message` from `from`'s connection to `to`'s, and check that it
/// arrives whole.
pub fn exchange(
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
