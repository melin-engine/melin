//! How a long-lived link learns that its peer has gone, and how it tells
//! the peer when it goes itself.
//!
//! A kernel-TCP peer is told when a node's process stops, crashed or not:
//! its kernel closes the sockets and the peer sees EOF or a reset. A DPDK
//! node is its own TCP stack, so nothing speaks for it once it stops. Two
//! things make up for that:
//!
//! - **A deadline on a silent peer** ([`PeerLiveness`]): TCP keep-alive
//!   probes on an idle link and a timeout on the peer's silence, both
//!   handled by the TCP stack. A peer whose stack still answers is alive,
//!   however long its application stays quiet; a peer that answers
//!   nothing for the timeout is gone, and the socket is reset.
//! - **Telling the peer** (`abort_announced`): a socket being closed
//!   sends its RST before it is removed, rather than vanishing.
//!
//! `link_up` is the matching test on the receiving side: a link is up
//! while both halves are open, so a peer's FIN ends it as surely as an
//! RST, once the bytes sent before the FIN are read.
//!
//! Plain smoltcp with no libdpdk in it, so it is tested on every host,
//! like `mac` and `rx_checksum`, over an in-memory wire and a virtual
//! clock.

// Without `dpdk-sys` nothing but the tests calls into this module.
#![cfg_attr(not(feature = "dpdk-sys"), allow(dead_code))]

use std::time::Duration;

use smoltcp::iface::{Interface, SocketHandle, SocketSet};
use smoltcp::phy::Device;
use smoltcp::socket::tcp;
use smoltcp::time::Instant;

/// When a link's peer counts as gone: the socket probes an idle link every
/// `probe_interval` and resets the connection once the peer has sent
/// nothing for `timeout`.
///
/// A peer is gone at most `timeout` after its last packet, with one
/// exception that comes from the stack: when the link has been idle and the
/// node then queues data, the stack starts counting afresh from that send.
/// So a link that is sending into a dead peer can take up to twice
/// `timeout` to be declared gone, never longer (the count restarts only on
/// a send into an empty buffer, and with the peer dead the buffer never
/// empties again).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PeerLiveness {
    probe_interval: Duration,
    timeout: Duration,
}

impl PeerLiveness {
    /// The replication link's: a probe a second, gone after five seconds
    /// of silence.
    ///
    /// Five seconds is the kernel-TCP replica's read timeout on a quiet
    /// primary during the handshake, the one deadline that path has on a
    /// silent peer (during streaming it relies on the kernel's EOF, which
    /// a DPDK peer cannot send once it has crashed). A probe a second
    /// leaves room for four lost before a live peer could be declared gone.
    ///
    /// The deadline only fires on a peer whose TCP stack has stopped
    /// answering: a crashed or stopped node, a cut link, or a node whose
    /// poll thread has stalled for the whole timeout. A live node whose
    /// application is merely quiet keeps answering the probes, and so does
    /// one that has stopped reading: with its receive buffer full it
    /// advertises a zero window, and its stack answers the zero-window
    /// probes, which fastcp (from 0.13.2) sends at most a probe interval
    /// apart while keep-alive is set.
    pub const REPLICATION: Self = Self::new(Duration::from_secs(1), Duration::from_secs(5));

    /// A liveness rule probing every `probe_interval` and giving up after
    /// `timeout` of silence.
    ///
    /// # Panics
    ///
    /// If `probe_interval` is zero or not shorter than `timeout`: such a
    /// rule would either flood the link or declare a live idle peer gone
    /// before its first probe could be answered. (In a `const`, a compile
    /// error.)
    pub const fn new(probe_interval: Duration, timeout: Duration) -> Self {
        assert!(
            !probe_interval.is_zero() && probe_interval.as_millis() < timeout.as_millis(),
            "the probe interval must be non-zero and shorter than the timeout"
        );
        Self {
            probe_interval,
            timeout,
        }
    }

    /// How often an idle link is probed.
    pub const fn probe_interval(&self) -> Duration {
        self.probe_interval
    }

    /// How long a peer may stay silent before the link is reset.
    pub const fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Arm the rule on a socket. Done once the connection is established:
    /// set on a listening socket, the timeout would also cover a half-open
    /// handshake and close the listener itself, which nothing replaces.
    pub(crate) fn apply(&self, socket: &mut tcp::Socket<'_>) {
        socket.set_keep_alive(Some(self.probe_interval.into()));
        socket.set_timeout(Some(self.timeout.into()));
    }
}

/// Whether a connection still carries a session in both directions.
///
/// False once either side has closed: our own close, the peer's RST, the
/// liveness deadline, or the peer's FIN once every byte it sent before
/// the FIN has been read (smoltcp keeps a connection readable while
/// unread data remains). A peer that has sent its FIN can send nothing
/// more, so a session needing both directions is over.
pub(crate) fn link_up(socket: &tcp::Socket<'_>) -> bool {
    socket.may_send() && socket.may_recv()
}

/// Abort a connection and let the peer know: the socket is aborted and
/// one egress pass sends its RST (smoltcp sends a single RST for an
/// aborted socket and then forgets the peer). The caller then removes the
/// socket. Returns `false` if `handle` is no longer in the set.
///
/// Without the egress pass the socket would be removed with its RST
/// unsent, and the peer would learn nothing until its own deadline.
/// Best effort: a frame the device cannot take now is not retried, and
/// the peer's deadline covers that.
pub(crate) fn abort_announced<D: Device + ?Sized>(
    iface: &mut Interface,
    device: &mut D,
    sockets: &mut SocketSet<'_>,
    handle: SocketHandle,
    now: Instant,
) -> bool {
    let Some(socket) = sockets.try_get_mut::<tcp::Socket>(handle) else {
        return false;
    };
    socket.abort();
    // Bounded work, and the other sockets' pending segments going out
    // with it is what the next poll would have done anyway.
    iface.poll_egress(now, device, sockets);
    true
}

/// The TCP stack's clock: milliseconds that only move forward.
///
/// smoltcp's timers (retransmission, keep-alive, the liveness timeout)
/// need a monotonic clock, which the wall clock is not: stepped forward,
/// it would declare every live peer gone at once; stepped back, it would
/// hold a dead one for as long as the step. The count starts at the wall
/// clock's reading when the clock is made, so the values keep the
/// magnitude the stack has always seen, and moves with
/// [`std::time::Instant`] from then on.
#[derive(Clone, Copy, Debug)]
pub(crate) struct StackClock {
    origin: std::time::Instant,
    /// Milliseconds since the Unix epoch at `origin`. `i64` because that
    /// is what smoltcp's `Instant` counts in.
    base_millis: i64,
}

impl StackClock {
    /// A clock reading the wall clock's milliseconds now.
    pub(crate) fn start() -> Self {
        // A wall clock before the epoch reads as the epoch: the base only
        // sets the starting magnitude, and any value keeps the count
        // monotonic. A millisecond count past `i64::MAX` saturates, as in
        // `now`.
        let base_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX / 2));
        Self::starting_at(base_millis)
    }

    fn starting_at(base_millis: i64) -> Self {
        Self {
            origin: std::time::Instant::now(),
            base_millis,
        }
    }

    /// The stack's time now.
    pub(crate) fn now(&self) -> Instant {
        // Saturating: an elapsed time past `i64::MAX` milliseconds is
        // hundreds of millions of years away.
        let elapsed = i64::try_from(self.origin.elapsed().as_millis()).unwrap_or(i64::MAX / 2);
        Instant::from_millis(self.base_millis.saturating_add(elapsed))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::rc::Rc;

    use smoltcp::iface::Config;
    use smoltcp::phy::{DeviceCapabilities, Medium};
    use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address};

    use super::*;

    /// Two nodes' frames in flight. `VecDeque`: frames are delivered in
    /// the order they were sent, from the front.
    #[derive(Default)]
    struct Wire {
        inbox: [VecDeque<Vec<u8>>; 2],
        /// Frames sent while set are lost: a cut link.
        cut: bool,
    }

    /// One node's end of the wire.
    struct End {
        wire: Rc<RefCell<Wire>>,
        side: usize,
    }

    struct Rx(Vec<u8>);

    struct Tx {
        wire: Rc<RefCell<Wire>>,
        to: usize,
    }

    impl smoltcp::phy::RxToken for Rx {
        fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
            f(&self.0)
        }
    }

    impl smoltcp::phy::TxToken for Tx {
        fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
            let mut frame = vec![0u8; len];
            let r = f(&mut frame);
            let mut wire = self.wire.borrow_mut();
            if !wire.cut {
                wire.inbox[self.to].push_back(frame);
            }
            r
        }
    }

    impl Device for End {
        type RxToken<'a> = Rx;
        type TxToken<'a> = Tx;

        fn receive(&mut self, _: Instant) -> Option<(Rx, Tx)> {
            let frame = self.wire.borrow_mut().inbox[self.side].pop_front()?;
            Some((Rx(frame), self.tx()))
        }

        fn transmit(&mut self, _: Instant) -> Option<Tx> {
            Some(self.tx())
        }

        fn capabilities(&self) -> DeviceCapabilities {
            let mut caps = DeviceCapabilities::default();
            caps.medium = Medium::Ethernet;
            caps.max_transmission_unit = 1514;
            caps
        }
    }

    impl End {
        fn tx(&self) -> Tx {
            Tx {
                wire: Rc::clone(&self.wire),
                to: 1 - self.side,
            }
        }
    }

    struct Node {
        iface: Interface,
        device: End,
        sockets: SocketSet<'static>,
        handle: SocketHandle,
        /// A node that is not polled answers nothing: a stopped process.
        alive: bool,
    }

    impl Node {
        fn new(wire: &Rc<RefCell<Wire>>, side: usize, now: Instant) -> Self {
            let mut device = End {
                wire: Rc::clone(wire),
                side,
            };
            let mac = EthernetAddress([0x02, 0, 0, 0, 0, side as u8 + 1]);
            let mut iface = Interface::new(
                Config::new(HardwareAddress::Ethernet(mac)),
                &mut device,
                now,
            );
            iface.update_ip_addrs(|addrs| {
                addrs
                    .push(IpCidr::new(IpAddress::Ipv4(ip(side)), 24))
                    .unwrap();
            });
            let mut sockets = SocketSet::new(Vec::new());
            let socket = tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; 4096]),
                tcp::SocketBuffer::new(vec![0; 4096]),
            );
            let handle = sockets.add(socket);
            Self {
                iface,
                device,
                sockets,
                handle,
                alive: true,
            }
        }

        fn socket(&mut self) -> &mut tcp::Socket<'static> {
            self.sockets.get_mut::<tcp::Socket>(self.handle)
        }

        fn link_up(&mut self) -> bool {
            link_up(self.socket())
        }
    }

    fn ip(side: usize) -> Ipv4Address {
        Ipv4Address::new(10, 0, 0, side as u8 + 1)
    }

    const PORT: u16 = 7000;

    /// A connected pair on a virtual clock, node 0 having accepted
    /// node 1's connection, both armed with `liveness`.
    struct Pair {
        wire: Rc<RefCell<Wire>>,
        nodes: [Node; 2],
        now: Instant,
    }

    /// Virtual time per step: the granularity every bound below allows
    /// for on top of the rule's own.
    const STEP: Duration = Duration::from_millis(10);

    impl Pair {
        fn connected(liveness: PeerLiveness) -> Self {
            let wire = Rc::new(RefCell::new(Wire::default()));
            let now = Instant::from_millis(1_000_000);
            let mut pair = Self {
                nodes: [Node::new(&wire, 0, now), Node::new(&wire, 1, now)],
                wire,
                now,
            };
            pair.nodes[0].socket().listen(PORT).unwrap();
            let client = &mut pair.nodes[1];
            client
                .sockets
                .get_mut::<tcp::Socket>(client.handle)
                .connect(client.iface.context(), (ip(0), PORT), 40_000)
                .unwrap();
            pair.run_until(Duration::from_secs(1), |p| {
                p.nodes[0].socket().state() == tcp::State::Established
                    && p.nodes[1].socket().state() == tcp::State::Established
            })
            .expect("the pair connects");
            for node in &mut pair.nodes {
                liveness.apply(node.socket());
            }
            pair
        }

        fn step(&mut self) {
            self.now += STEP.into();
            for node in &mut self.nodes {
                if node.alive {
                    node.iface
                        .poll(self.now, &mut node.device, &mut node.sockets);
                }
            }
        }

        /// Step until `done` holds, for at most `limit`; how long it took.
        fn run_until(
            &mut self,
            limit: Duration,
            mut done: impl FnMut(&mut Self) -> bool,
        ) -> Option<Duration> {
            let start = self.now;
            loop {
                if done(self) {
                    return Some((self.now - start).into());
                }
                if Duration::from(self.now - start) >= limit {
                    return None;
                }
                self.step();
            }
        }

        /// Step for `span`, calling `each` after every step.
        fn run_for(&mut self, span: Duration, mut each: impl FnMut(&mut Self)) {
            let end = self.now + span.into();
            while self.now < end {
                self.step();
                each(self);
            }
        }
    }

    const RULE: PeerLiveness = PeerLiveness::REPLICATION;

    /// Probes are answered, so a peer whose application never says a word
    /// is never taken for gone, at either end.
    #[test]
    fn a_live_idle_peer_is_never_declared_gone() {
        let mut pair = Pair::connected(RULE);
        pair.run_for(RULE.timeout() * 10, |p| {
            assert!(p.nodes[0].link_up() && p.nodes[1].link_up());
        });
    }

    /// The peer stops (it is no longer polled) on an idle link: the link
    /// goes down within the timeout of the peer's last word, which was
    /// the answer to a probe at most one probe interval earlier.
    #[test]
    fn a_stopped_peer_is_declared_gone_within_the_timeout_on_an_idle_link() {
        for gone in [0, 1] {
            let mut pair = Pair::connected(RULE);
            // Settle into the probing rhythm first.
            pair.run_for(RULE.timeout() * 2, |_| {});
            pair.nodes[gone].alive = false;
            let survivor = 1 - gone;
            let took = pair
                .run_until(RULE.timeout() * 4, |p| !p.nodes[survivor].link_up())
                .expect("the stopped peer is declared gone");
            assert!(
                took <= RULE.timeout() + STEP,
                "node {survivor} took {took:?} to notice"
            );
            assert!(
                took + RULE.probe_interval() >= RULE.timeout(),
                "node {survivor} gave up after {took:?}, before the timeout"
            );
            assert!(!pair.nodes[survivor].socket().is_active());
        }
    }

    /// The node keeps sending (replication data, heartbeats) into a peer
    /// that has stopped: gone within twice the timeout (see
    /// `PeerLiveness`).
    #[test]
    fn a_stopped_peer_is_declared_gone_while_data_is_sent_to_it() {
        let mut pair = Pair::connected(RULE);
        pair.run_for(RULE.timeout(), |_| {});
        pair.nodes[1].alive = false;
        let mut since_send = Duration::ZERO;
        let took = pair
            .run_until(RULE.timeout() * 4, |p| {
                if !p.nodes[0].link_up() {
                    return true;
                }
                since_send += STEP;
                if since_send >= Duration::from_millis(300) {
                    since_send = Duration::ZERO;
                    // A full buffer refuses the bytes, which is fine:
                    // the point is that data is waiting.
                    let _ = p.nodes[0].socket().send_slice(b"heartbeat");
                }
                false
            })
            .expect("the stopped peer is declared gone");
        assert!(took <= RULE.timeout() * 2 + STEP, "took {took:?}");
    }

    /// A peer that is alive but has stopped reading (a replica installing
    /// a snapshot, or one whose journal has stalled) is not gone, however
    /// long it reads nothing. Its receive buffer fills and it advertises a
    /// zero window; the node, with data queued, probes it, and its stack
    /// answers every probe. fastcp before 0.13.2 doubled the probe delay
    /// without bound, so once it passed the timeout the deadline fired
    /// between two answers and reset a live peer, about 12 s in; 0.13.2
    /// caps it at the keep-alive interval.
    #[test]
    fn a_live_peer_that_has_stopped_reading_is_never_declared_gone() {
        let mut pair = Pair::connected(RULE);
        let mut window_closed = false;
        pair.run_for(RULE.timeout() * 6, |p| {
            // Node 1 never reads. Node 0 keeps data queued for it; once
            // the buffers are full the bytes are refused, which is the
            // point: data is waiting on a zero window.
            let _ = p.nodes[0].socket().send_slice(b"replication data");
            let peer = p.nodes[1].socket();
            window_closed |= peer.recv_queue() == peer.recv_capacity();
            assert!(
                p.nodes[0].link_up(),
                "a live peer that stopped reading was declared gone"
            );
        });
        assert!(window_closed, "the peer's receive buffer never filled");
        assert!(
            pair.nodes[0].socket().send_queue() > 0,
            "no data was left waiting"
        );
    }

    /// A cut link, both nodes running: each declares the other gone.
    #[test]
    fn a_cut_link_is_declared_gone_at_both_ends() {
        let mut pair = Pair::connected(RULE);
        pair.run_for(RULE.timeout(), |_| {});
        pair.wire.borrow_mut().cut = true;
        let took = pair
            .run_until(RULE.timeout() * 4, |p| {
                !p.nodes[0].link_up() && !p.nodes[1].link_up()
            })
            .expect("both ends notice");
        assert!(took <= RULE.timeout() + STEP, "took {took:?}");
    }

    /// The announced abort reaches the peer: its link is down in the time
    /// a frame takes to cross, not at its deadline.
    #[test]
    fn an_announced_abort_takes_the_peer_link_down_at_once() {
        let mut pair = Pair::connected(RULE);
        let node = &mut pair.nodes[0];
        assert!(abort_announced(
            &mut node.iface,
            &mut node.device,
            &mut node.sockets,
            node.handle,
            pair.now,
        ));
        node.sockets.remove(node.handle);
        // Polled no more, as a node that has closed and gone.
        node.alive = false;
        let took = pair
            .run_until(RULE.timeout(), |p| !p.nodes[1].link_up())
            .expect("the peer sees the reset");
        assert!(took <= STEP * 2, "took {took:?}");
    }

    /// An abort without the egress pass, as a plain removal does, tells
    /// the peer nothing: it is left to its deadline. This is what
    /// `abort_announced` is for.
    #[test]
    fn a_silent_removal_leaves_the_peer_to_its_deadline() {
        let mut pair = Pair::connected(RULE);
        let node = &mut pair.nodes[0];
        node.socket().abort();
        node.sockets.remove(node.handle);
        node.alive = false;
        let took = pair
            .run_until(RULE.timeout() * 4, |p| !p.nodes[1].link_up())
            .expect("the deadline fires");
        assert!(
            took + RULE.probe_interval() >= RULE.timeout(),
            "took {took:?}"
        );
    }

    #[test]
    fn announcing_a_removed_socket_does_nothing() {
        let mut pair = Pair::connected(RULE);
        let node = &mut pair.nodes[0];
        node.sockets.remove(node.handle);
        assert!(!abort_announced(
            &mut node.iface,
            &mut node.device,
            &mut node.sockets,
            node.handle,
            pair.now,
        ));
    }

    /// The peer sends its last bytes and a FIN: the link stays up until
    /// those bytes are read, then goes down, though the socket itself is
    /// still active (CLOSE-WAIT).
    #[test]
    fn a_fin_takes_the_link_down_once_the_data_before_it_is_read() {
        let mut pair = Pair::connected(RULE);
        assert_eq!(pair.nodes[1].socket().send_slice(b"last ack").unwrap(), 8);
        pair.nodes[1].socket().close();
        pair.run_until(Duration::from_secs(1), |p| {
            p.nodes[0].socket().state() == tcp::State::CloseWait
        })
        .expect("the FIN arrives");
        assert!(pair.nodes[0].link_up(), "unread data keeps the link up");
        let mut buf = [0u8; 16];
        let n = pair.nodes[0].socket().recv_slice(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"last ack");
        assert!(!pair.nodes[0].link_up());
        assert!(pair.nodes[0].socket().is_active());
    }

    /// The peer's RST, as from a node's own deadline or its announced
    /// abort, takes the link down.
    #[test]
    fn a_reset_from_the_peer_takes_the_link_down() {
        let mut pair = Pair::connected(RULE);
        pair.nodes[1].socket().abort();
        pair.run_until(Duration::from_secs(1), |p| !p.nodes[0].link_up())
            .expect("the RST arrives");
    }

    #[test]
    fn the_replication_rule_probes_well_within_its_timeout() {
        assert!(RULE.probe_interval() * 4 <= RULE.timeout());
    }

    #[test]
    #[should_panic(expected = "shorter than the timeout")]
    fn a_probe_interval_not_shorter_than_the_timeout_is_refused() {
        PeerLiveness::new(Duration::from_secs(5), Duration::from_secs(5));
    }

    #[test]
    #[should_panic(expected = "non-zero")]
    fn a_zero_probe_interval_is_refused() {
        PeerLiveness::new(Duration::ZERO, Duration::from_secs(5));
    }

    #[test]
    fn the_stack_clock_starts_at_its_base_and_only_moves_forward() {
        let clock = StackClock::starting_at(1_000);
        let first = clock.now();
        assert!(first.total_millis() >= 1_000);
        assert!(first.total_millis() < 1_000 + 60_000);
        let mut last = first;
        for _ in 0..1_000 {
            let now = clock.now();
            assert!(now >= last);
            last = now;
        }
        std::thread::sleep(Duration::from_millis(20));
        assert!(clock.now().total_millis() >= first.total_millis() + 20);
    }

    #[test]
    fn the_stack_clock_starts_near_the_wall_clock() {
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let stack = StackClock::start().now().total_millis();
        assert!((stack - wall).abs() < 60_000, "stack {stack}, wall {wall}");
    }
}
