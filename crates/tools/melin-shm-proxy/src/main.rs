#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! A TCP connection to a Melin node, owned by this process and offered to
//! another through shared memory.
//!
//! Melin's kernel-bypass transport lives in a Rust process, needs root
//! and a NIC of its own, and is not something to load into a JVM or a
//! Python interpreter. This is the seam between such a client and the
//! node, shaped like the seam Aeron's own DPDK media driver uses: the
//! client writes into shared memory, and a separate process owns the NIC.
//! The first such client is the Aeron benchmark harness's Java rig. The
//! client keeps its own framing, its own handshake and its own clock;
//! what crosses the seam is bytes, and the proxy knows nothing of the
//! application on either side of them.
//!
//! The file holds two rings and a state word (see `shm.rs` for the
//! layout). The loop here is one thread, pinned if asked, busy-spinning:
//! whatever the client has written goes to the socket, the stack is
//! serviced, whatever the node has answered goes back. Nothing is framed
//! and nothing is timed; a request written by the client is on the wire
//! in the iteration that finds it, and several written together travel
//! as one segment -- which is the coalescing a kernel does under load,
//! and what makes the packet rate a function of the backlog rather than
//! of the message rate. `--trace` is the one exception: it has the loop
//! time itself, and the round trip from a request reaching the stack to
//! the batch-end that closes its reply, reported on stderr at the end.
//! Even then it reads only the protocol's tags, never an application
//! body.
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
use hdrhistogram::Histogram;
use melin_wire_protocol::control_codec::{TAG_APP, TAG_BATCH_END};

#[cfg(any(feature = "dpdk", test))]
mod arp;
#[cfg(feature = "dpdk")]
mod dpdk;
mod shm;
mod transport;
// The clock carries a few conversions the loop here has no use for.
#[allow(dead_code)]
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
/// servicing the stack costs, and the round trip from a request being
/// handed to the stack to the batch-end that closes its reply being in
/// hand -- the wire twice, the node, and a turn of this loop on each
/// side. Requests and replies are matched by order: one connection, a
/// node that answers in order, and exactly one batch-end per request,
/// whatever the application put in the batch.
///
/// Every frame is `[len: u32 LE][tag][body]`, and the tag is all the
/// trace reads. On the way out, a frame with the protocol's application
/// tag is a request; the handshake's frames before it carry other tags.
/// On the way back, the protocol closes each request's reply batch --
/// empty, one response, several, or a rejection -- with a batch-end
/// frame of its own, so that frame is the answer and the application's
/// bodies are never looked at. Heartbeats carry nothing and are skipped
/// like every other tag.
struct Trace {
    iteration: Histogram<u64>,
    service: Histogram<u64>,
    round_trip: Histogram<u64>,
    requests: FrameScanner,
    replies: FrameScanner,
    /// Send stamps of the requests not yet answered, oldest first.
    in_flight: VecDeque<u64>,
}

impl Trace {
    /// More than a node holds unanswered on one connection; past it the
    /// oldest stamp goes rather than the queue growing.
    const IN_FLIGHT_CAP: usize = 1 << 16;

    fn new() -> Self {
        // A minute is past any round trip a connection survives.
        let histogram =
            || Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("the bounds are valid");
        Self {
            iteration: histogram(),
            service: histogram(),
            round_trip: histogram(),
            requests: FrameScanner::new(),
            replies: FrameScanner::new(),
            in_flight: VecDeque::with_capacity(Self::IN_FLIGHT_CAP),
        }
    }

    /// Whether a frame on its way out is a request: an application frame.
    fn is_request(tag: Option<u8>) -> bool {
        tag == Some(TAG_APP)
    }

    /// Whether a frame on its way back answers a request: the batch-end
    /// that closes a reply batch.
    fn is_answer(tag: Option<u8>) -> bool {
        tag == Some(TAG_BATCH_END)
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
            if Self::is_request(tag) {
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
            if Self::is_answer(tag)
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
            ("request → batch end", &self.round_trip),
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
                "trace: {} requests were never answered",
                self.in_flight.len()
            );
        }
    }
}

/// Walks a stream of `[len: u32 LE][tag][body]` frames as its bytes come,
/// in whatever pieces, and reports each complete frame's tag -- the first
/// payload byte, or `None` for an empty payload, which the protocol never
/// sends.
struct FrameScanner {
    state: Scan,
}

enum Scan {
    Header { got: usize, bytes: [u8; 4] },
    Payload { left: usize, tag: Option<u8> },
}

impl FrameScanner {
    fn new() -> Self {
        Self {
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
                                tag: None,
                            };
                        }
                    }
                }
                Scan::Payload { left, tag } => {
                    // The first payload byte is the tag, whichever piece
                    // carries it: the piece that starts the payload.
                    if tag.is_none() {
                        *tag = Some(bytes[0]);
                    }
                    let take = (*left).min(bytes.len());
                    *left -= take;
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

    use melin_wire_protocol::control_codec::{TAG_CHALLENGE_RESPONSE, TAG_RESPONSE_HEARTBEAT};

    #[test]
    fn frames_are_tagged_whatever_the_pieces() {
        // Two frames -- `[tag][body]` -- and an empty one, fed a byte at a
        // time, then in threes, then as one slice: the same tags.
        let mut stream = frame(&[TAG_APP, b'h', b'i']);
        stream.extend(frame(&[]));
        stream.extend(frame(&[TAG_CHALLENGE_RESPONSE]));
        let expected = vec![Some(TAG_APP), None, Some(TAG_CHALLENGE_RESPONSE)];

        for pieces in [1usize, 3, stream.len()] {
            let mut scanner = FrameScanner::new();
            let mut tags = Vec::new();
            for piece in stream.chunks(pieces) {
                scanner.feed(piece, &mut |tag| tags.push(tag));
            }
            assert_eq!(tags, expected, "pieces of {pieces}");
        }
    }

    #[test]
    fn requests_are_application_frames_and_answers_are_batch_ends() {
        // On the way out, the handshake's challenge response is not a
        // request; an application frame is, whatever its body.
        assert!(Trace::is_request(Some(TAG_APP)));
        assert!(!Trace::is_request(Some(TAG_CHALLENGE_RESPONSE)));
        assert!(!Trace::is_request(None));

        // On the way back, the application's responses and the heartbeats
        // are not the answer; the batch-end that closes the batch is.
        assert!(!Trace::is_answer(Some(TAG_APP)));
        assert!(!Trace::is_answer(Some(TAG_RESPONSE_HEARTBEAT)));
        assert!(!Trace::is_answer(None));
        assert!(Trace::is_answer(Some(TAG_BATCH_END)));
    }

    #[test]
    fn replies_answer_requests_in_order() {
        let mut trace = Trace::new();
        let request = frame(&[TAG_APP, b'a']);
        let mut two = request.clone();
        two.extend(&request);
        trace.sent(&two, &[], two.len());
        assert_eq!(trace.in_flight.len(), 2);

        // The first request's batch holds two application responses, the
        // second's none -- a rejection the application reported with no
        // body, say -- and a heartbeat sits between them. Two batch-ends,
        // two round trips.
        let clock = TscClock::calibrate();
        let mut replies = frame(&[TAG_APP, 0x30, b'a']);
        replies.extend(frame(&[TAG_APP, 0x31]));
        replies.extend(frame(&[TAG_BATCH_END]));
        replies.extend(frame(&[TAG_RESPONSE_HEARTBEAT]));
        replies.extend(frame(&[TAG_BATCH_END]));
        let n = replies.len();
        trace.received(&replies, &[], n, &clock);
        assert!(trace.in_flight.is_empty());
        assert_eq!(trace.round_trip.len(), 2);
    }
}
