#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! A TCP connection to a Melin server, owned by this process and offered
//! to another through shared memory.
//!
//! The Aeron benchmark harness measures from a Java process; Melin's
//! kernel-bypass transport lives in a Rust one, needs root and a NIC of
//! its own, and is not something to load into a JVM. This is the seam
//! between them, shaped like the seam Aeron itself uses: the measuring
//! process writes into shared memory, and a separate process owns the
//! NIC. The Java side keeps its own framing, its own handshake and its
//! own clock; what crosses the seam is bytes.
//!
//! The file holds two rings and a state word (see `proxy/shm.rs` for the
//! layout). The loop here is one thread, pinned if asked, busy-spinning:
//! whatever the client has written goes to the socket, the stack is
//! serviced, whatever the server has answered goes back. Nothing is
//! framed and nothing is timed; a request written by the client is on
//! the wire in the iteration that finds it, and several written together
//! travel as one segment -- which is the coalescing a kernel does under
//! load, and what makes the packet rate a function of the backlog rather
//! than of the message rate. `--trace` is the one exception: it has the
//! loop time itself, and the round trip from a request reaching the
//! stack to its reply being in hand, reported on stderr at the end.
//!
//! ```sh
//! shm-proxy --server 10.0.1.30:9876 --shm /dev/shm/melin-client.shm \
//!     --transport dpdk --dpdk-ip 10.0.1.33 --dpdk-peer-mac 02:...
//! ```
//!
//! Kernel TCP by default, which is how the link is tested on a machine
//! with no DPDK. Exit status: 0 when the client asked for the close or
//! the server ended the connection cleanly, 1 when the connection
//! failed mid-run, 2 when it could not be set up.

use std::collections::VecDeque;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use echo_server::{TAG_ECHO, TAG_RESP_ECHO, TAG_RESP_REJECTED};
use hdrhistogram::Histogram;

// A binary's root resolves `mod` against its own directory, so the
// modules under `proxy/` are named by path. The clock carries a few
// conversions the loop here has no use for.
#[cfg(any(feature = "dpdk", test))]
#[path = "proxy/arp.rs"]
mod arp;
#[cfg(feature = "dpdk")]
#[path = "proxy/dpdk.rs"]
mod dpdk;
#[path = "proxy/shm.rs"]
mod shm;
#[path = "proxy/transport.rs"]
mod transport;
#[allow(dead_code)]
#[path = "proxy/tsc.rs"]
mod tsc;

use shm::{SharedMemory, State};
use transport::{KernelTcp, Transport};
use tsc::{TscClock, rdtscp};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TransportKind {
    Kernel,
    Dpdk,
}

#[derive(Parser)]
#[command(
    name = "shm-proxy",
    about = "Hold a connection to a Melin server and bridge it to shared memory"
)]
struct Cli {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:9876")]
    server: SocketAddr,
    /// The shared-memory file to create. Its directory should be a
    /// tmpfs; the file is world read-write so the client may run under
    /// another account.
    #[arg(long, default_value = "/dev/shm/melin-client.shm")]
    shm: PathBuf,
    #[arg(long, value_enum, default_value_t = TransportKind::Kernel)]
    transport: TransportKind,
    /// Capacity of the client-to-server ring, in KiB, a power of two.
    #[arg(long, default_value_t = 1024)]
    to_wire_kib: usize,
    /// Capacity of the server-to-client ring, in KiB, a power of two.
    #[arg(long, default_value_t = 1024)]
    from_wire_kib: usize,
    /// Pin the loop to this core.
    #[arg(long)]
    core: Option<usize>,
    /// Time the loop: its iteration, the stack's servicing, and the
    /// round trip from an echo request reaching the stack to its reply
    /// being in hand. Percentiles on stderr when the bridge ends.
    #[arg(long)]
    trace: bool,
    #[cfg(feature = "dpdk")]
    #[command(flatten)]
    dpdk: dpdk::DpdkArgs,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Setup(e)) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
        Err(Failure::Lost(e)) => {
            eprintln!("error: connection lost: {e}");
            ExitCode::from(1)
        }
    }
}

enum Failure {
    Setup(String),
    Lost(io::Error),
}

fn run(cli: Cli) -> Result<(), Failure> {
    if let Some(core) = cli.core {
        match melin_app::affinity::pin_to_core(core) {
            Ok(core) => eprintln!("pinned to core {core}"),
            Err(e) => eprintln!("warning: not pinned: {e}"),
        }
    }
    let clock = TscClock::calibrate();
    let mut link = SharedMemory::create(&cli.shm, cli.to_wire_kib * 1024, cli.from_wire_kib * 1024)
        .map_err(Failure::Setup)?;
    eprintln!(
        "link at {} ({} KiB to the wire, {} KiB from it)",
        cli.shm.display(),
        cli.to_wire_kib,
        cli.from_wire_kib
    );

    let result = match cli.transport {
        TransportKind::Kernel => match KernelTcp::connect(cli.server, CONNECT_TIMEOUT) {
            Ok(transport) => bridge(transport, &mut link, &cli, &clock),
            Err(e) => Err(Failure::Setup(format!(
                "cannot connect to {}: {e}",
                cli.server
            ))),
        },
        TransportKind::Dpdk => match connect_dpdk(&cli, &clock) {
            Ok(transport) => bridge(transport, &mut link, &cli, &clock),
            Err(e) => Err(Failure::Setup(e)),
        },
    };
    link.set_state(match result {
        Ok(()) => State::Closed,
        Err(Failure::Setup(_)) => State::Failed,
        Err(Failure::Lost(_)) => State::Closed,
    });
    result
}

#[cfg(feature = "dpdk")]
fn connect_dpdk(cli: &Cli, clock: &TscClock) -> Result<dpdk::DpdkTcp, String> {
    let server = match cli.server {
        SocketAddr::V4(v4) => v4,
        SocketAddr::V6(_) => return Err("the DPDK transport is IPv4 only".into()),
    };
    dpdk::DpdkTcp::connect(&cli.dpdk, server, clock)
}

#[cfg(not(feature = "dpdk"))]
fn connect_dpdk(_cli: &Cli, _clock: &TscClock) -> Result<KernelTcp, String> {
    Err("this shm-proxy was built without the dpdk feature; rebuild with --features dpdk".into())
}

/// Bridge until the client asks for the close or the server ends the
/// connection. `Ok` for either of those; `Err` when the connection
/// broke.
fn bridge<T: Transport>(
    mut transport: T,
    link: &mut SharedMemory,
    cli: &Cli,
    clock: &TscClock,
) -> Result<(), Failure> {
    link.set_state(State::Connected);
    eprintln!("connected to {} over {}", cli.server, transport.name());

    let mut trace = cli.trace.then(Trace::new);
    let result = bridge_loop(&mut transport, link, trace.as_mut(), clock);
    if let Some(trace) = &trace {
        trace.report();
    }
    result
}

fn bridge_loop<T: Transport>(
    transport: &mut T,
    link: &mut SharedMemory,
    mut trace: Option<&mut Trace>,
    clock: &TscClock,
) -> Result<(), Failure> {
    let mut iter_start = rdtscp();
    loop {
        // Client to server first, so a request found on this turn is on
        // the wire when the stack is serviced, not on the next.
        {
            let ring = link.outbound();
            let (first, second) = ring.readable();
            if !first.is_empty() {
                let mut sent = transport.send(first).map_err(Failure::Lost)?;
                if sent == first.len() && !second.is_empty() {
                    sent += transport.send(second).map_err(Failure::Lost)?;
                }
                if let Some(trace) = trace.as_deref_mut() {
                    trace.sent(first, second, sent);
                }
                ring.consumed(sent);
            }
        }

        let before_service = rdtscp();
        transport.service(clock.unix_ns(before_service));
        if let Some(trace) = trace.as_deref_mut() {
            trace
                .service
                .saturating_record(clock.elapsed_ns(before_service, rdtscp()));
        }

        {
            let ring = link.inbound();
            let (first, second) = ring.writable();
            if !first.is_empty() {
                match receive(transport, first, second) {
                    Ok(n) => {
                        if let Some(trace) = trace.as_deref_mut() {
                            trace.received(first, second, n, clock);
                        }
                        ring.produced(n);
                    }
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        eprintln!("the server closed the connection");
                        return Ok(());
                    }
                    Err(e) => return Err(Failure::Lost(e)),
                }
            }
        }

        if let Some(trace) = trace.as_deref_mut() {
            let now = rdtscp();
            trace
                .iteration
                .saturating_record(clock.elapsed_ns(iter_start, now));
            iter_start = now;
        }

        if link.close_requested() {
            eprintln!("close requested by the client");
            return Ok(());
        }
    }
}

/// The loop's own figures, on `--trace`: what an iteration costs, what
/// servicing the stack costs, and the round trip from an echo request
/// being handed to the stack to its reply being in hand -- the wire
/// twice, the server, and a turn of this loop on each side. Requests
/// and replies are matched by order: one connection, a server that
/// answers in order, and exactly one echo reply per echo request; the
/// batch-end and heartbeat frames around the replies, and the handshake
/// frames before them, are skipped by tag.
struct Trace {
    iteration: Histogram<u64>,
    service: Histogram<u64>,
    round_trip: Histogram<u64>,
    requests: FrameScanner,
    replies: FrameScanner,
    /// Send stamps of the echo requests not yet answered, oldest first.
    in_flight: VecDeque<u64>,
}

impl Trace {
    /// More than a server holds unanswered on one connection; past it
    /// the oldest stamp goes rather than the queue growing.
    const IN_FLIGHT_CAP: usize = 1 << 16;

    /// A request frame is `[seq: u64][tag][body]`, a reply `[tag][body]`.
    const REQUEST_TAG_OFFSET: usize = 8;
    const REPLY_TAG_OFFSET: usize = 0;

    fn new() -> Self {
        // A minute is past any round trip a connection survives.
        let histogram =
            || Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("the bounds are valid");
        Self {
            iteration: histogram(),
            service: histogram(),
            round_trip: histogram(),
            requests: FrameScanner::new(Self::REQUEST_TAG_OFFSET),
            replies: FrameScanner::new(Self::REPLY_TAG_OFFSET),
            in_flight: VecDeque::with_capacity(Self::IN_FLIGHT_CAP),
        }
    }

    /// `sent` bytes of `first` then `second` went to the stack just now.
    fn sent(&mut self, first: &[u8], second: &[u8], sent: usize) {
        let now = rdtscp();
        let Self {
            requests,
            in_flight,
            ..
        } = self;
        let mut on_frame = |tag: Option<u8>| {
            if tag == Some(TAG_ECHO) {
                if in_flight.len() == Self::IN_FLIGHT_CAP {
                    in_flight.pop_front();
                }
                in_flight.push_back(now);
            }
        };
        let from_first = sent.min(first.len());
        requests.feed(&first[..from_first], &mut on_frame);
        requests.feed(&second[..sent - from_first], &mut on_frame);
    }

    /// `n` bytes into `first` then `second` came from the stack just now.
    fn received(&mut self, first: &[u8], second: &[u8], n: usize, clock: &TscClock) {
        let now = rdtscp();
        let Self {
            replies,
            in_flight,
            round_trip,
            ..
        } = self;
        let mut on_frame = |tag: Option<u8>| {
            if matches!(tag, Some(TAG_RESP_ECHO | TAG_RESP_REJECTED))
                && let Some(sent) = in_flight.pop_front()
            {
                round_trip.saturating_record(clock.elapsed_ns(sent, now));
            }
        };
        let into_first = n.min(first.len());
        replies.feed(&first[..into_first], &mut on_frame);
        replies.feed(&second[..n - into_first], &mut on_frame);
    }

    fn report(&self) {
        eprintln!(
            "trace: {:<28} {:>10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            "µs", "samples", "min", "p50", "p90", "p99", "p99.9", "max"
        );
        for (name, histogram) in [
            ("loop iteration", &self.iteration),
            ("service()", &self.service),
            ("echo request → reply", &self.round_trip),
        ] {
            let micros = |ns: u64| ns as f64 / 1_000.0;
            eprintln!(
                "trace: {:<28} {:>10} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.1}",
                name,
                histogram.len(),
                micros(histogram.min()),
                micros(histogram.value_at_quantile(0.5)),
                micros(histogram.value_at_quantile(0.9)),
                micros(histogram.value_at_quantile(0.99)),
                micros(histogram.value_at_quantile(0.999)),
                micros(histogram.max()),
            );
        }
        if !self.in_flight.is_empty() {
            eprintln!(
                "trace: {} echo requests were never answered",
                self.in_flight.len()
            );
        }
    }
}

/// Walks a stream of `[len: u32 LE][payload]` frames as its bytes come,
/// in whatever pieces, and reports each complete frame's tag -- the
/// payload byte at a fixed offset, or `None` when the payload is too
/// short to have one.
struct FrameScanner {
    tag_offset: usize,
    state: Scan,
}

enum Scan {
    Header {
        got: usize,
        bytes: [u8; 4],
    },
    Payload {
        left: usize,
        offset: usize,
        tag: Option<u8>,
    },
}

impl FrameScanner {
    fn new(tag_offset: usize) -> Self {
        Self {
            tag_offset,
            state: Scan::Header {
                got: 0,
                bytes: [0; 4],
            },
        }
    }

    fn feed(&mut self, mut bytes: &[u8], on_frame: &mut impl FnMut(Option<u8>)) {
        while !bytes.is_empty() {
            match &mut self.state {
                Scan::Header { got, bytes: header } => {
                    let take = (4 - *got).min(bytes.len());
                    header[*got..*got + take].copy_from_slice(&bytes[..take]);
                    *got += take;
                    bytes = &bytes[take..];
                    if *got == 4 {
                        let len = u32::from_le_bytes(*header) as usize;
                        if len == 0 {
                            on_frame(None);
                            self.state = Scan::Header {
                                got: 0,
                                bytes: [0; 4],
                            };
                        } else {
                            self.state = Scan::Payload {
                                left: len,
                                offset: 0,
                                tag: None,
                            };
                        }
                    }
                }
                Scan::Payload { left, offset, tag } => {
                    let take = (*left).min(bytes.len());
                    if tag.is_none() && (*offset..*offset + take).contains(&self.tag_offset) {
                        *tag = Some(bytes[self.tag_offset - *offset]);
                    }
                    *left -= take;
                    *offset += take;
                    bytes = &bytes[take..];
                    if *left == 0 {
                        on_frame(*tag);
                        self.state = Scan::Header {
                            got: 0,
                            bytes: [0; 4],
                        };
                    }
                }
            }
        }
    }
}

/// Fill `first`, and `second` only if `first` filled entirely.
fn receive<T: Transport>(
    transport: &mut T,
    first: &mut [u8],
    second: &mut [u8],
) -> io::Result<usize> {
    let mut n = transport.recv(first)?;
    if n == first.len() && !second.is_empty() {
        n += transport.recv(second)?;
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut out = (payload.len() as u32).to_le_bytes().to_vec();
        out.extend_from_slice(payload);
        out
    }

    #[test]
    fn frames_are_tagged_whatever_the_pieces() {
        // Two requests -- `[seq][tag][body]` -- and an empty frame, fed a
        // byte at a time, then whole, then as one slice: the same tags.
        let mut stream = frame(&[7, 0, 0, 0, 0, 0, 0, 0, TAG_ECHO, b'h', b'i']);
        stream.extend(frame(&[]));
        stream.extend(frame(&[8, 0, 0, 0, 0, 0, 0, 0, 0x06]));
        let expected = vec![Some(TAG_ECHO), None, Some(0x06)];

        for pieces in [1usize, 3, stream.len()] {
            let mut scanner = FrameScanner::new(Trace::REQUEST_TAG_OFFSET);
            let mut tags = Vec::new();
            for piece in stream.chunks(pieces) {
                scanner.feed(piece, &mut |tag| tags.push(tag));
            }
            assert_eq!(tags, expected, "pieces of {pieces}");
        }
    }

    #[test]
    fn a_short_payload_has_no_tag() {
        let mut scanner = FrameScanner::new(Trace::REQUEST_TAG_OFFSET);
        let mut tags = Vec::new();
        scanner.feed(&frame(&[1, 2, 3]), &mut |tag| tags.push(tag));
        scanner.feed(&frame(&[TAG_RESP_ECHO]), &mut |tag| tags.push(tag));
        assert_eq!(tags, vec![None, None]);

        let mut replies = FrameScanner::new(Trace::REPLY_TAG_OFFSET);
        replies.feed(&frame(&[TAG_RESP_ECHO, b'x']), &mut |tag| tags.push(tag));
        assert_eq!(tags.last(), Some(&Some(TAG_RESP_ECHO)));
    }

    #[test]
    fn replies_answer_requests_in_order() {
        let mut trace = Trace::new();
        let request = frame(&[1, 0, 0, 0, 0, 0, 0, 0, TAG_ECHO, b'a']);
        let mut two = request.clone();
        two.extend(&request);
        trace.sent(&two, &[], two.len());
        assert_eq!(trace.in_flight.len(), 2);

        let clock = TscClock::calibrate();
        let mut replies = frame(&[TAG_RESP_ECHO, b'a']);
        replies.extend(frame(&[0x02])); // batch end: not an answer
        replies.extend(frame(&[TAG_RESP_REJECTED]));
        let n = replies.len();
        trace.received(&replies, &[], n, &clock);
        assert!(trace.in_flight.is_empty());
        assert_eq!(trace.round_trip.len(), 2);
    }
}
