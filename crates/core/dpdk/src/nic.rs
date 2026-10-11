//! The part of the client's NIC handle that needs no NIC: the stack's
//! clock as the caller drives it, the local ports dials take, the wait
//! for a dial to be answered, what a stream's send and receive do with a
//! socket, and how the handle's EAL arguments and errors read.
//!
//! Plain smoltcp and std with no libdpdk in it, so it is tested on every
//! host, like `peer_liveness`, over an in-memory wire and a virtual clock.
//! The handle itself (`dpdk::nic_handle`) is this logic bound to the node
//! transport's device and interface.

// Without `dpdk-sys` nothing but the tests calls into this module.
#![cfg_attr(not(feature = "dpdk-sys"), allow(dead_code))]

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};

use smoltcp::socket::tcp;

use crate::mac::{MacAddr, PeerMacSource, fmt_mac};

/// The file prefix a client's EAL takes when its arguments name none.
///
/// A DPDK process without a prefix of its own shares the runtime
/// directory with every other one on the host, so a client beside a node,
/// the usual local arrangement, fails to take the runtime lock.
pub(crate) const CLIENT_FILE_PREFIX: &str = "melin-client";

/// The `--file-prefix` among EAL arguments, in either of the forms EAL
/// accepts (`--file-prefix x` and `--file-prefix=x`). The last one wins,
/// as it does for EAL's own option parser.
pub(crate) fn file_prefix(eal_args: &[String]) -> Option<&str> {
    let mut found = None;
    let mut args = eal_args.iter();
    while let Some(arg) = args.next() {
        if arg == "--file-prefix" {
            found = args.next().map(String::as_str).or(found);
        } else if let Some(prefix) = arg.strip_prefix("--file-prefix=") {
            found = Some(prefix);
        }
    }
    found
}

/// A client's EAL arguments: `eal_args`, with the client's own file
/// prefix ([`CLIENT_FILE_PREFIX`]) added when they name none.
pub(crate) fn with_client_file_prefix(eal_args: &[String]) -> Vec<String> {
    let mut args = eal_args.to_vec();
    if file_prefix(eal_args).is_none() {
        args.push("--file-prefix".to_owned());
        args.push(CLIENT_FILE_PREFIX.to_owned());
    }
    args
}

/// The error for an EAL the handle could not bring up or share. It names
/// the file prefix, since a prefix shared with another DPDK process on
/// the host (its runtime lock already held) is the likeliest cause. On
/// the process-wide EAL the prefix is whoever initialised it's, so the
/// message says whose EAL it was instead.
pub(crate) fn eal_init_failed(
    eal_args: &[String],
    shared_eal: bool,
    err: &dyn std::fmt::Display,
) -> String {
    if shared_eal {
        format!("the process-wide DPDK EAL this NIC handle shares: {err}")
    } else {
        // EAL's own default when no prefix is given.
        let prefix = file_prefix(eal_args).unwrap_or("rte");
        format!(
            "DPDK EAL init failed (file prefix {prefix}; is another DPDK process on this host \
             using it?): {err}"
        )
    }
}

/// A monotonic nanosecond count from an origin fixed when the NIC handle
/// was opened: the clock a caller with none of its own drives the handle
/// with (see `NicHandle::dial` and `NicHandle::service`).
///
/// Built on [`std::time::Instant`], which never goes backwards, rather
/// than the wall clock, which can be stepped. `Copy`, so a caller can read
/// it while the handle is borrowed for a dial.
#[derive(Clone, Copy, Debug)]
pub struct NicClock {
    origin: std::time::Instant,
}

impl NicClock {
    pub(crate) fn start() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }

    /// Nanoseconds since the handle was opened. `u64`: the count a caller
    /// with its own clock passes too, and centuries from overflowing; it
    /// saturates rather than wrapping if it ever did.
    pub fn now_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// The instant [`now_ns`](Self::now_ns) counts from. For a caller that
    /// keeps a clock of its own on the same origin (a cached reading, say)
    /// so that its readings and this clock's agree: the handle's stack
    /// takes them as one clock, where readings from another origin would
    /// look like a clock stepping back and be clamped.
    pub fn origin(&self) -> std::time::Instant {
        self.origin
    }
}

/// The stack's clock, driven by the caller's readings.
///
/// The caller passes a monotonic nanosecond count whose origin is fixed
/// for the life of the handle (its own TSC reading, or [`NicClock`]); the
/// stack counts in microseconds from the time the interface was created
/// at. The first reading is anchored to that time, and each later one
/// moves the stack on by the difference, so the caller's origin never
/// matters. A reading that goes backwards (behind the last one, or behind
/// the anchor) leaves the stack's time where it was: a timer must never
/// run backwards, and a clock that stepped back would otherwise hold
/// every retransmission for as long as the step.
#[derive(Debug)]
pub(crate) struct StackTime {
    /// The stack's time at the anchor, in microseconds. `i64`: what the
    /// stack's own instants count in.
    base_micros: i64,
    /// The caller's first reading, once there has been one.
    anchor_ns: Option<u64>,
    /// The stack's time at the latest reading: never decreases.
    last_micros: i64,
}

impl StackTime {
    pub(crate) fn new(base_micros: i64) -> Self {
        Self {
            base_micros,
            anchor_ns: None,
            last_micros: base_micros,
        }
    }

    /// The stack's time at the caller's reading `now_ns`.
    pub(crate) fn at(&mut self, now_ns: u64) -> i64 {
        let anchor = *self.anchor_ns.get_or_insert(now_ns);
        if let Some(elapsed_ns) = now_ns.checked_sub(anchor) {
            // Saturating: an elapsed time past `i64::MAX` microseconds is
            // hundreds of thousands of years away.
            let elapsed = i64::try_from(elapsed_ns / 1_000).unwrap_or(i64::MAX);
            self.last_micros = self
                .last_micros
                .max(self.base_micros.saturating_add(elapsed));
        }
        self.last_micros
    }
}

/// The first port of the IANA ephemeral range, where dials take their
/// local ports.
const EPHEMERAL_FIRST: u16 = 49_152;
/// Where in the range a process starts: one of this many ports, picked
/// from the process ID, so that a process restarted on the same host does
/// not dial from the four-tuple its predecessor's connection may still
/// hold on the node. The shm-proxy and the exchange's bench have always
/// started there.
const EPHEMERAL_SPREAD: u32 = 8_192;

/// Local ports for dials: a fresh one per socket, not per process.
///
/// A DPDK node does not see a client's close until its next heartbeat, so
/// a reconnect from the four-tuple of a connection it may still hold would
/// be taken for that connection. Each dial therefore takes the next port
/// along the ephemeral range, wrapping at its end and skipping any port an
/// open stream holds, so a port comes round again only after the whole
/// range has been dialed from.
#[derive(Debug)]
pub(crate) struct LocalPorts {
    next: u16,
}

impl LocalPorts {
    /// Ports starting at a point picked from `seed`, the process ID.
    pub(crate) fn starting_from(seed: u32) -> Self {
        // In range by construction: the spread is under the range's size.
        let offset = (seed % EPHEMERAL_SPREAD) as u16;
        Self {
            next: EPHEMERAL_FIRST + offset,
        }
    }

    /// The next port no open stream holds, or `None` if every port of the
    /// range is held.
    pub(crate) fn take(&mut self, in_use: impl Fn(u16) -> bool) -> Option<u16> {
        // Every port of the range once: `u16::MAX` is its last port.
        for _ in EPHEMERAL_FIRST..=u16::MAX {
            let port = self.next;
            self.next = if port == u16::MAX {
                EPHEMERAL_FIRST
            } else {
                port + 1
            };
            if !in_use(port) {
                return Some(port);
            }
        }
        None
    }
}

/// Where a dial stands after a poll of the stack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DialProgress {
    /// The handshake is still under way.
    Pending,
    /// The connection is established.
    Established,
    /// The peer answered with a reset: nothing listens there, or a DPDK
    /// node is in another client's handshake (see `NicHandle`).
    Refused,
}

/// Where a dial stands, from its socket's state.
pub(crate) fn dial_progress(state: tcp::State) -> DialProgress {
    match state {
        tcp::State::Established => DialProgress::Established,
        // A dialing socket only gets back to closed through the peer's
        // RST: it was never closed or aborted from this side.
        tcp::State::Closed | tcp::State::TimeWait => DialProgress::Refused,
        _ => DialProgress::Pending,
    }
}

/// Why a dial failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DialFailure {
    /// No SYN-ACK before the deadline.
    TimedOut,
    /// The peer reset the connection attempt.
    Refused,
}

/// Wait for a dial: read the caller's clock, give up at `deadline_ns`,
/// otherwise run `step` (a poll of the stack at that reading) and see
/// where the dial stands.
///
/// The clock is read on every iteration, and every poll gets the fresh
/// reading: the handshake runs on retransmission timers, and a stale
/// timestamp here stalls it (the exchange's bench found as much).
pub(crate) fn wait_for_dial(
    deadline_ns: u64,
    now_ns: &mut impl FnMut() -> u64,
    mut step: impl FnMut(u64) -> DialProgress,
) -> Result<(), DialFailure> {
    loop {
        let now = now_ns();
        if now >= deadline_ns {
            return Err(DialFailure::TimedOut);
        }
        match step(now) {
            DialProgress::Established => return Ok(()),
            DialProgress::Refused => return Err(DialFailure::Refused),
            DialProgress::Pending => {}
        }
    }
}

/// Where a dial's first frame goes: the MAC it is addressed to, and where
/// that MAC came from. The error for a timed-out dial names it, and a
/// binary shows it before dialing (`NicHandle::next_hop`), since a wrong
/// MAC is silent until the dial times out.
///
/// `Display` renders it for a line of the form `server <addr> {next_hop}`:
/// `at <mac> (supplied)`, `at <mac> (derived from the IP by the SR-IOV
/// convention)`, or `via gateway <ip> at <mac> (supplied)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextHop {
    /// The peer itself, seeded from a supplied MAC or derived by the
    /// SR-IOV convention.
    Peer { mac: MacAddr, source: PeerMacSource },
    /// The gateway, in L3 mode, seeded from its supplied MAC.
    Gateway { ip: Ipv4Addr, mac: MacAddr },
}

impl NextHop {
    /// The warning to print beside a next hop whose MAC was derived rather
    /// than given, without its `warning: ` prefix: on a fabric that
    /// assigns MACs itself the derived one belongs to nobody, and the only
    /// symptom is a dial that times out. `None` when the MAC was supplied.
    pub fn warning(&self) -> Option<&'static str> {
        match self {
            NextHop::Peer {
                source: PeerMacSource::DerivedSrIov,
                ..
            } => Some(
                "no --dpdk-peer-mac; on a fabric that assigns MACs itself (an AWS ENI) the \
                 derived address is wrong and the connect below will time out",
            ),
            NextHop::Peer {
                source: PeerMacSource::Supplied,
                ..
            }
            | NextHop::Gateway { .. } => None,
        }
    }
}

impl std::fmt::Display for NextHop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NextHop::Peer {
                mac,
                source: PeerMacSource::Supplied,
            } => write!(f, "at {} (supplied)", fmt_mac(*mac)),
            NextHop::Peer {
                mac,
                source: PeerMacSource::DerivedSrIov,
            } => write!(
                f,
                "at {} (derived from the IP by the SR-IOV convention)",
                fmt_mac(*mac)
            ),
            NextHop::Gateway { ip, mac } => {
                write!(f, "via gateway {ip} at {} (supplied)", fmt_mac(*mac))
            }
        }
    }
}

/// The error for a failed dial: [`io::ErrorKind::TimedOut`] or
/// [`io::ErrorKind::ConnectionRefused`].
///
/// A timed-out dial names the MAC its frames were addressed to as the
/// first thing to check: a wrong MAC is silent, the frames go to an
/// address nothing owns, and a timeout is the only symptom.
pub(crate) fn dial_error(peer: SocketAddrV4, failure: DialFailure, next_hop: NextHop) -> io::Error {
    match failure {
        DialFailure::Refused => io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!("TCP connect to {peer} over DPDK was refused"),
        ),
        DialFailure::TimedOut => {
            let check = match next_hop {
                NextHop::Peer {
                    mac,
                    source: PeerMacSource::Supplied,
                } => format!("that {} (supplied) is the server's real MAC", fmt_mac(mac)),
                NextHop::Peer {
                    mac,
                    source: PeerMacSource::DerivedSrIov,
                } => format!(
                    "the server's MAC: {} was derived from its IP by the SR-IOV convention, which \
                     is wrong on a port that keeps a real hardware address; supply the real one",
                    fmt_mac(mac)
                ),
                NextHop::Gateway { ip, mac } => {
                    format!("that {} is the real MAC of the gateway {ip}", fmt_mac(mac))
                }
            };
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "TCP connect to {peer} over DPDK timed out with no SYN-ACK; check first \
                     {check}, then that the server's DPDK interface is up"
                ),
            )
        }
    }
}

/// Queue as much of `data` as the socket's transmit buffer has room for,
/// possibly none of it.
pub(crate) fn send(socket: &mut tcp::Socket<'_>, data: &[u8]) -> io::Result<usize> {
    if !socket.can_send() {
        return Ok(0);
    }
    socket
        .send_slice(data)
        .map_err(|e| io::Error::other(format!("send: {e:?}")))
}

/// Lend `fill` the contiguous free region of the socket's transmit
/// buffer, and queue the bytes it reports having written there, from the
/// region's start.
///
/// `fill` is not called when the buffer has no room or the connection can
/// no longer send, and `Ok(0)` is returned; it may be lent a region
/// shorter than the buffer's free space, when that space wraps round the
/// end of the ring. A count past the region's end is refused as
/// [`io::ErrorKind::InvalidInput`] and nothing is queued.
pub(crate) fn send_in_place(
    socket: &mut tcp::Socket<'_>,
    fill: impl FnOnce(&mut [u8]) -> usize,
) -> io::Result<usize> {
    if !socket.can_send() {
        return Ok(0);
    }
    let mut overrun = None;
    let queued = socket
        .send(|region| {
            let lent = region.len();
            let written = fill(region);
            if written > lent {
                overrun = Some((written, lent));
                (0, 0)
            } else {
                (written, written)
            }
        })
        .map_err(|e| io::Error::other(format!("send: {e:?}")))?;
    match overrun {
        Some((written, lent)) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{written} bytes reported written into a region of {lent}"),
        )),
        None => Ok(queued),
    }
}

/// Read what has arrived into `buf`. `Ok(0)` means nothing yet; a closed
/// connection is [`io::ErrorKind::UnexpectedEof`], never a zero, once
/// every byte the peer sent before closing has been read.
pub(crate) fn recv(socket: &mut tcp::Socket<'_>, buf: &mut [u8]) -> io::Result<usize> {
    if socket.can_recv() {
        return socket
            .recv_slice(buf)
            .map_err(|e| io::Error::other(format!("recv: {e:?}")));
    }
    // Nothing buffered. The receive half is open while the connection is
    // established (or we are the one closing); after the peer's FIN or
    // RST, or our own abort, nothing more can arrive.
    if socket.may_recv() {
        Ok(0)
    } else {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the server closed the connection",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use smoltcp::iface::{Interface, SocketHandle, SocketSet};
    use smoltcp::time::{Duration, Instant};

    use super::*;
    use crate::test_wire::{End, Wire, interface, ip};

    const PORT: u16 = 9876;

    // --- EAL arguments ---------------------------------------------------

    fn args(s: &str) -> Vec<String> {
        s.split_whitespace().map(String::from).collect()
    }

    #[test]
    fn the_file_prefix_is_found_in_either_form() {
        assert_eq!(
            file_prefix(&args("--huge-dir /dev/hugepages --file-prefix shm-proxy")),
            Some("shm-proxy")
        );
        assert_eq!(
            file_prefix(&args("-l 3 --file-prefix=bench")),
            Some("bench")
        );
        assert_eq!(file_prefix(&args("--huge-dir /dev/hugepages")), None);
        // A dangling flag names nothing.
        assert_eq!(file_prefix(&args("--file-prefix")), None);
        // The last one wins, as in EAL.
        assert_eq!(
            file_prefix(&args("--file-prefix a --file-prefix=b")),
            Some("b")
        );
    }

    #[test]
    fn a_client_without_a_file_prefix_gets_its_own() {
        let defaulted = with_client_file_prefix(&args("--huge-dir /dev/hugepages"));
        assert_eq!(
            defaulted,
            args("--huge-dir /dev/hugepages --file-prefix melin-client")
        );
        assert_eq!(file_prefix(&defaulted), Some(CLIENT_FILE_PREFIX));
        assert_eq!(
            with_client_file_prefix(&[]),
            args("--file-prefix melin-client")
        );
    }

    #[test]
    fn a_client_keeps_the_file_prefix_it_was_given() {
        let given = args("--huge-dir /dev/hugepages --file-prefix shm-proxy");
        assert_eq!(with_client_file_prefix(&given), given);
        let joined = args("--file-prefix=bench");
        assert_eq!(with_client_file_prefix(&joined), joined);
    }

    #[test]
    fn a_failed_eal_init_names_its_file_prefix() {
        let err = eal_init_failed(&args("--file-prefix shm-proxy"), false, &"code -1");
        assert!(err.contains("file prefix shm-proxy"), "{err}");
        assert!(err.contains("code -1"), "{err}");
        let err = eal_init_failed(&[], false, &"code -1");
        assert!(err.contains("file prefix rte"), "{err}");
        let err = eal_init_failed(&[], true, &"refused");
        assert!(err.contains("process-wide"), "{err}");
    }

    // --- Clocks ------------------------------------------------------------

    #[test]
    fn the_first_reading_is_anchored_to_the_stacks_time() {
        let mut time = StackTime::new(5_000_000);
        // Whatever the caller's origin: a TSC-derived Unix time here.
        let origin = 1_760_000_000_000_000_000;
        assert_eq!(time.at(origin), 5_000_000);
        assert_eq!(time.at(origin + 2_500_000), 5_002_500);
    }

    #[test]
    fn a_reading_that_goes_backwards_is_clamped() {
        let mut time = StackTime::new(1_000);
        time.at(10_000_000);
        assert_eq!(time.at(13_000_000), 4_000);
        // Behind the last reading: the stack stays where it was.
        assert_eq!(time.at(12_000_000), 4_000);
        // Behind the anchor itself.
        assert_eq!(time.at(0), 4_000);
        // And moves on once the readings pass it again.
        assert_eq!(time.at(13_500_000), 4_500);
    }

    #[test]
    fn a_huge_reading_saturates_rather_than_wrapping() {
        let mut time = StackTime::new(i64::MAX - 10);
        time.at(0);
        assert_eq!(time.at(u64::MAX), i64::MAX);
    }

    #[test]
    fn the_nic_clock_only_moves_forward() {
        let clock = NicClock::start();
        let mut last = clock.now_ns();
        for _ in 0..1_000 {
            let now = clock.now_ns();
            assert!(now >= last);
            last = now;
        }
    }

    /// A clock kept on the handle's origin reads what the handle's does.
    #[test]
    fn the_nic_clock_counts_from_its_origin() {
        let clock = NicClock::start();
        let before = clock.now_ns();
        let on_origin = u64::try_from(clock.origin().elapsed().as_nanos()).unwrap();
        let after = clock.now_ns();
        assert!(before <= on_origin && on_origin <= after);
    }

    // --- Local ports --------------------------------------------------------

    #[test]
    fn the_first_port_is_picked_from_the_process_id() {
        // As the shm-proxy and the exchange's bench always picked theirs.
        let seed = 123_456;
        let mut ports = LocalPorts::starting_from(seed);
        assert_eq!(ports.take(|_| false), Some(49_152 + (seed % 8_192) as u16));
        let mut highest = LocalPorts::starting_from(EPHEMERAL_SPREAD - 1);
        assert_eq!(highest.take(|_| false), Some(49_152 + 8_191));
    }

    /// A reconnect never reuses the four-tuple of the connection before it.
    #[test]
    fn every_dial_takes_a_fresh_port() {
        let mut ports = LocalPorts::starting_from(7);
        let first = ports.take(|_| false).expect("a port");
        let second = ports.take(|_| false).expect("a port");
        let third = ports.take(|_| false).expect("a port");
        assert_eq!(second, first + 1);
        assert_eq!(third, first + 2);
    }

    #[test]
    fn ports_wrap_at_the_end_of_the_range_and_skip_those_in_use() {
        let mut ports = LocalPorts { next: u16::MAX - 1 };
        assert_eq!(ports.take(|_| false), Some(u16::MAX - 1));
        assert_eq!(ports.take(|p| p == u16::MAX), Some(EPHEMERAL_FIRST));
        assert_eq!(ports.take(|_| false), Some(EPHEMERAL_FIRST + 1));
    }

    #[test]
    fn a_range_held_entirely_has_no_port() {
        let mut ports = LocalPorts::starting_from(0);
        assert_eq!(ports.take(|_| true), None);
        // And the range is still served once a port frees up.
        assert_eq!(ports.take(|p| p != 60_000), Some(60_000));
    }

    // --- Dials ---------------------------------------------------------------

    /// The caller's clock: readings that advance a millisecond per read.
    struct FakeClock {
        now_ns: u64,
        reads: usize,
    }

    impl FakeClock {
        fn new() -> Self {
            Self {
                now_ns: 1_000_000_000,
                reads: 0,
            }
        }

        fn read(&mut self) -> u64 {
            self.reads += 1;
            self.now_ns += 1_000_000;
            self.now_ns
        }
    }

    /// The clock is read on every iteration and every poll is handed the
    /// fresh reading.
    #[test]
    fn a_dial_refreshes_the_stacks_clock_on_every_iteration() {
        let clock = RefCell::new(FakeClock::new());
        let mut seen = Vec::new();
        let mut polls = 0;
        let outcome = wait_for_dial(u64::MAX, &mut || clock.borrow_mut().read(), |now| {
            seen.push(now);
            polls += 1;
            if polls == 5 {
                DialProgress::Established
            } else {
                DialProgress::Pending
            }
        });
        assert_eq!(outcome, Ok(()));
        assert_eq!(clock.borrow().reads, 5);
        assert_eq!(seen.len(), 5);
        assert!(seen.windows(2).all(|w| w[1] > w[0]), "{seen:?}");
    }

    #[test]
    fn a_dial_past_its_deadline_times_out_without_polling_again() {
        let mut clock = FakeClock::new();
        let deadline = clock.now_ns + 3_000_000;
        let mut polls = 0;
        let outcome = wait_for_dial(deadline, &mut || clock.read(), |_| {
            polls += 1;
            DialProgress::Pending
        });
        assert_eq!(outcome, Err(DialFailure::TimedOut));
        assert_eq!(polls, 2);
    }

    #[test]
    fn a_reset_is_a_refusal() {
        let mut clock = FakeClock::new();
        let outcome = wait_for_dial(u64::MAX, &mut || clock.read(), |_| DialProgress::Refused);
        assert_eq!(outcome, Err(DialFailure::Refused));
    }

    #[test]
    fn socket_states_map_onto_dial_progress() {
        assert_eq!(
            dial_progress(tcp::State::Established),
            DialProgress::Established
        );
        assert_eq!(dial_progress(tcp::State::Closed), DialProgress::Refused);
        assert_eq!(dial_progress(tcp::State::TimeWait), DialProgress::Refused);
        assert_eq!(dial_progress(tcp::State::SynSent), DialProgress::Pending);
        assert_eq!(
            dial_progress(tcp::State::SynReceived),
            DialProgress::Pending
        );
    }

    fn peer() -> SocketAddrV4 {
        SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), PORT)
    }

    #[test]
    fn a_timed_out_dial_names_the_peer_mac_first() {
        let mac = [0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80];
        for source in [PeerMacSource::Supplied, PeerMacSource::DerivedSrIov] {
            let err = dial_error(peer(), DialFailure::TimedOut, NextHop::Peer { mac, source });
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
            let msg = err.to_string();
            let named = msg.find("0c:42:a1:5b:2e:80").expect("the MAC is named");
            let interface = msg.find("interface is up").expect("then the interface");
            assert!(named < interface, "{msg}");
            assert!(msg.contains("10.0.0.1:9876"), "{msg}");
        }
        let err = dial_error(
            peer(),
            DialFailure::TimedOut,
            NextHop::Gateway {
                ip: Ipv4Addr::new(10, 0, 0, 254),
                mac,
            },
        );
        let msg = err.to_string();
        assert!(msg.contains("gateway 10.0.0.254"), "{msg}");
        assert!(msg.contains("0c:42:a1:5b:2e:80"), "{msg}");
    }

    #[test]
    fn a_refused_dial_is_told_from_a_timed_out_one() {
        let next_hop = NextHop::Peer {
            mac: [2, 0, 10, 0, 0, 1],
            source: PeerMacSource::DerivedSrIov,
        };
        let err = dial_error(peer(), DialFailure::Refused, next_hop);
        assert_eq!(err.kind(), io::ErrorKind::ConnectionRefused);
        assert!(err.to_string().contains("refused"), "{err}");
    }

    /// The next hop prints as the binaries' connect line has always said
    /// it, and warns only when the MAC was derived.
    #[test]
    fn the_next_hop_reads_as_the_connect_line_says() {
        let mac = [0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80];
        let supplied = NextHop::Peer {
            mac,
            source: PeerMacSource::Supplied,
        };
        assert_eq!(supplied.to_string(), "at 0c:42:a1:5b:2e:80 (supplied)");
        assert_eq!(supplied.warning(), None);

        let derived = NextHop::Peer {
            mac,
            source: PeerMacSource::DerivedSrIov,
        };
        assert_eq!(
            derived.to_string(),
            "at 0c:42:a1:5b:2e:80 (derived from the IP by the SR-IOV convention)"
        );
        assert!(
            derived
                .warning()
                .is_some_and(|w| w.starts_with("no --dpdk-peer-mac;")),
            "{:?}",
            derived.warning()
        );

        let gateway = NextHop::Gateway {
            ip: Ipv4Addr::new(10, 0, 0, 254),
            mac,
        };
        assert_eq!(
            gateway.to_string(),
            "via gateway 10.0.0.254 at 0c:42:a1:5b:2e:80 (supplied)"
        );
        assert_eq!(gateway.warning(), None);
    }

    // --- Streams over a wire --------------------------------------------------

    struct Side {
        iface: Interface,
        device: End,
        sockets: SocketSet<'static>,
        handle: SocketHandle,
    }

    impl Side {
        fn new(wire: &Rc<RefCell<Wire>>, side: usize, now: Instant, buffer: usize) -> Self {
            let mut device = End::new(wire, side);
            let iface = interface(&mut device, side, now);
            let mut sockets = SocketSet::new(Vec::new());
            let mut socket = tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; buffer]),
                tcp::SocketBuffer::new(vec![0; buffer]),
            );
            // Every segment acknowledged at once, as the transport tunes
            // its sockets, so a test knows which step an ACK lands in.
            socket.set_ack_delay(None);
            let handle = sockets.add(socket);
            Self {
                iface,
                device,
                sockets,
                handle,
            }
        }

        fn socket(&mut self) -> &mut tcp::Socket<'static> {
            self.sockets.get_mut::<tcp::Socket>(self.handle)
        }

        fn poll(&mut self, now: Instant) {
            self.iface.poll(now, &mut self.device, &mut self.sockets);
        }
    }

    /// A server (side 0) and a client (side 1) dialing it, on one wire.
    struct Link {
        wire: Rc<RefCell<Wire>>,
        server: Side,
        client: Side,
        now: Instant,
    }

    impl Link {
        fn new(listening: bool, buffer: usize) -> Self {
            let wire = Rc::new(RefCell::new(Wire::default()));
            let now = Instant::from_millis(1_000_000);
            let mut link = Self {
                server: Side::new(&wire, 0, now, buffer),
                client: Side::new(&wire, 1, now, buffer),
                wire,
                now,
            };
            if listening {
                link.server.socket().listen(PORT).unwrap();
            }
            let client = &mut link.client;
            client
                .sockets
                .get_mut::<tcp::Socket>(client.handle)
                .connect(client.iface.context(), (ip(0), PORT), 49_152)
                .unwrap();
            link
        }

        fn step(&mut self) {
            self.now += Duration::from_millis(1);
            self.server.poll(self.now);
            self.client.poll(self.now);
        }

        /// Dial the way the handle does: poll at each reading until the
        /// client's socket says where it stands.
        fn dial(&mut self, deadline_ns: u64) -> Result<(), DialFailure> {
            let mut clock = FakeClock::new();
            wait_for_dial(deadline_ns, &mut || clock.read(), |_| {
                self.step();
                dial_progress(self.client.socket().state())
            })
        }

        fn established(buffer: usize) -> Self {
            let mut link = Self::new(true, buffer);
            link.dial(u64::MAX).expect("the link connects");
            // The server's end is established by the dial's last ACK.
            while link.server.socket().state() != tcp::State::Established {
                link.step();
            }
            link
        }
    }

    #[test]
    fn a_dial_to_a_listener_is_established() {
        let mut link = Link::new(true, 4096);
        assert_eq!(link.dial(u64::MAX), Ok(()));
    }

    /// No listener: the server's stack answers the SYN with an RST, as a
    /// DPDK node does a SYN during another client's handshake.
    #[test]
    fn a_dial_to_no_listener_is_refused() {
        let mut link = Link::new(false, 4096);
        assert_eq!(link.dial(u64::MAX), Err(DialFailure::Refused));
    }

    /// Frames to a MAC nothing owns: lost, as on a cut wire.
    #[test]
    fn a_dial_nothing_answers_times_out() {
        let mut link = Link::new(true, 4096);
        link.wire.borrow_mut().cut = true;
        let deadline = FakeClock::new().now_ns + 50_000_000;
        assert_eq!(link.dial(deadline), Err(DialFailure::TimedOut));
    }

    #[test]
    fn bytes_cross_and_nothing_yet_is_a_zero() {
        let mut link = Link::established(4096);
        let mut buf = [0u8; 64];
        assert_eq!(recv(link.server.socket(), &mut buf).unwrap(), 0);
        assert_eq!(send(link.client.socket(), b"hello").unwrap(), 5);
        link.step();
        link.step();
        let n = recv(link.server.socket(), &mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");
        assert_eq!(recv(link.server.socket(), &mut buf).unwrap(), 0);
    }

    #[test]
    fn a_send_takes_what_the_buffer_has_room_for() {
        let mut link = Link::established(16);
        assert_eq!(send(link.client.socket(), &[7u8; 24]).unwrap(), 16);
        assert_eq!(send(link.client.socket(), &[7u8; 8]).unwrap(), 0);
    }

    /// The peer's RST, as from a node closing the connection.
    #[test]
    fn a_reset_connection_is_an_error_never_a_zero() {
        let mut link = Link::established(4096);
        link.server.socket().abort();
        link.step();
        link.step();
        let err = recv(link.client.socket(), &mut [0u8; 16]).expect_err("closed");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    /// The peer's FIN, as from a kernel node closing: what it sent before
    /// is read first, then the close is an error, not zeroes forever.
    #[test]
    fn a_finished_connection_is_read_to_its_end_then_an_error() {
        let mut link = Link::established(4096);
        assert_eq!(send(link.server.socket(), b"last").unwrap(), 4);
        link.server.socket().close();
        for _ in 0..4 {
            link.step();
        }
        let mut buf = [0u8; 16];
        let n = recv(link.client.socket(), &mut buf).unwrap();
        assert_eq!(&buf[..n], b"last");
        let err = recv(link.client.socket(), &mut buf).expect_err("closed");
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn an_in_place_send_queues_what_was_written_into_the_lent_region() {
        let mut link = Link::established(4096);
        let queued = send_in_place(link.client.socket(), |region| {
            assert_eq!(region.len(), 4096, "the whole free buffer, contiguous");
            region[..5].copy_from_slice(b"frame");
            5
        })
        .unwrap();
        assert_eq!(queued, 5);
        link.step();
        link.step();
        let mut buf = [0u8; 16];
        let n = recv(link.server.socket(), &mut buf).unwrap();
        assert_eq!(&buf[..n], b"frame");
    }

    /// Free space that wraps round the ring is lent in its contiguous
    /// part only: the encoder never sees two regions.
    #[test]
    fn an_in_place_send_lends_only_the_contiguous_region() {
        let mut link = Link::established(16);
        // Six bytes go out, six more are queued behind them, and the
        // first six are acknowledged: the ring holds bytes 6..12, so its
        // free space is 12..16 and then 0..6.
        assert_eq!(send(link.client.socket(), &[1u8; 6]).unwrap(), 6);
        link.step();
        assert_eq!(send(link.client.socket(), &[2u8; 6]).unwrap(), 6);
        link.step();
        assert_eq!(
            link.client.socket().send_queue(),
            6,
            "the first six acknowledged"
        );
        let lent = send_in_place(link.client.socket(), |region| region.len()).unwrap();
        assert_eq!(
            lent, 4,
            "the region from the write position to the ring's end"
        );
    }

    #[test]
    fn an_in_place_send_into_a_full_buffer_lends_nothing() {
        let mut link = Link::established(16);
        assert_eq!(send(link.client.socket(), &[1u8; 16]).unwrap(), 16);
        let mut called = false;
        let queued = send_in_place(link.client.socket(), |_| {
            called = true;
            0
        })
        .unwrap();
        assert_eq!(queued, 0);
        assert!(!called);
    }

    #[test]
    fn an_in_place_send_past_the_region_is_refused_and_queues_nothing() {
        let mut link = Link::established(16);
        let err = send_in_place(link.client.socket(), |region| region.len() + 1)
            .expect_err("an overrun is refused");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(link.client.socket().send_queue(), 0);
    }
}
