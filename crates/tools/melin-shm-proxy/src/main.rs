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
//! A Rust client that can host DPDK itself has no need of it:
//! `melin-client`'s `dpdk` feature runs a connection in-process on the
//! same NIC handle the proxy dials through (`dpdk.rs`). The proxy stays the
//! harness's client all the same, a separate process owning the NIC being
//! the point, and what the harness scripts against is a contract: the
//! flags and their defaults; on stderr the `connected to` line and the
//! `error:` prefix, which its runner reads to sequence the rig's start; and
//! the `--trace` report, which its published results quote. A change to
//! any of them is a change to that harness.
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
//! the final frame that answers it, reported on stderr at the end. Even
//! then it reads only the protocol's tags and request IDs, never an
//! application body.
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

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use hdrhistogram::Histogram;
use melin_wire_protocol::control::RequestId;
use melin_wire_protocol::control_codec::{REQUEST_ID_LEN, TAG_LEN, terminal_request_id};
use melin_wire_protocol::framing::split_request;

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
    /// round trip from a request reaching the stack to the final frame
    /// that answers it being in hand. Percentiles on stderr when the
    /// bridge ends.
    #[arg(long)]
    trace: bool,
    #[cfg(feature = "dpdk")]
    #[command(flatten)]
    dpdk: dpdk::ProxyDpdkArgs,
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
    let server = melin_dpdk::dial_addr(cli.server)
        .map_err(|why| format!("--server {}: {why}", cli.server))?;
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
/// handed to the stack to the final frame that answers it being in hand
/// -- the wire twice, the node, and a turn of this loop on each side.
///
/// Requests and their answers are paired by the request's ID: a request
/// carries one the client chose, and the node names it on the one final
/// frame it sends for that request -- a batch end (whatever the
/// application put in the batch, nothing included), a busy, or a refusal.
/// A request the node did not accept may be answered ahead of earlier
/// ones, so the pairing does not rest on order.
///
/// Every frame is `[len: u32 LE][tag][body]`, and the head of it -- the
/// tag and the eight bytes after it -- is all the trace reads. On the way
/// out, a request frame (`[tag][id][body]`) is a request; the handshake's
/// frames before it carry other tags. On the way back, a final frame
/// (`[tag][id]...`) is the answer; the application's responses and the
/// heartbeats are skipped like every other tag, and the application's
/// bodies are never looked at.
struct Trace {
    iteration: Histogram<u64>,
    service: Histogram<u64>,
    round_trip: Histogram<u64>,
    requests: FrameScanner,
    replies: FrameScanner,
    /// Send stamps of the requests not yet answered, by ID. A map, not
    /// the client library's sorted queue: the IDs are the client's (the
    /// Java rig's), and the proxy cannot assume they increase. Reserved to
    /// [`IN_FLIGHT_CAP`](Self::IN_FLIGHT_CAP) up front, so the steady state
    /// allocates nothing; the default hasher, since this is a diagnostic
    /// path and not the node's.
    in_flight: HashMap<RequestId, u64>,
    /// Requests whose stamp the trace did not keep: sent at the cap, or
    /// under an ID already in flight (whose stamp the new one replaced).
    untracked: u64,
    /// Final frames for an ID the trace held no stamp for.
    unmatched: u64,
}

impl Trace {
    /// More than a node holds unanswered on one connection; past it a new
    /// request's stamp is not kept, rather than an old one evicted (which
    /// would need a second, ordered structure) or the map growing.
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
            in_flight: HashMap::with_capacity(Self::IN_FLIGHT_CAP),
            untracked: 0,
            unmatched: 0,
        }
    }

    /// A request on its way out, by its frame's head, sent at `now`.
    fn request(&mut self, head: &[u8], now: u64) {
        // Anything else -- the handshake's frames, a request too short
        // for its ID, which the node closes the connection on -- is no
        // request to time.
        let Ok((id, _)) = split_request(head) else {
            return;
        };
        // At the cap a new ID's stamp is not kept; an ID already in flight
        // has its stamp replaced, and the earlier one is lost. Either way a
        // request goes untimed.
        let at_cap = self.in_flight.len() == Self::IN_FLIGHT_CAP;
        let lost = if at_cap && !self.in_flight.contains_key(&id) {
            true
        } else {
            self.in_flight.insert(id, now).is_some()
        };
        if lost {
            self.untracked += 1;
        }
    }

    /// A frame on its way back, by its head, received at `now`.
    fn reply(&mut self, head: &[u8], now: u64, clock: &TscClock) {
        let Some(id) = terminal_request_id(head) else {
            return;
        };
        match self.in_flight.remove(&id) {
            Some(sent) => self
                .round_trip
                .saturating_record(clock.elapsed_ns(sent, now)),
            None => self.unmatched += 1,
        }
    }

    /// `sent` bytes of `first` then `second` went to the stack just now.
    fn sent(&mut self, first: &[u8], second: &[u8], sent: usize) {
        let now = rdtscp();
        let from_first = sent.min(first.len());
        let mut requests = std::mem::take(&mut self.requests);
        let mut on_frame = |head: &[u8]| self.request(head, now);
        requests.feed(&first[..from_first], &mut on_frame);
        requests.feed(&second[..sent - from_first], &mut on_frame);
        self.requests = requests;
    }

    /// `n` bytes into `first` then `second` came from the stack just now.
    fn received(&mut self, first: &[u8], second: &[u8], n: usize, clock: &TscClock) {
        let now = rdtscp();
        let into_first = n.min(first.len());
        let mut replies = std::mem::take(&mut self.replies);
        let mut on_frame = |head: &[u8]| self.reply(head, now, clock);
        replies.feed(&first[..into_first], &mut on_frame);
        replies.feed(&second[..n - into_first], &mut on_frame);
        self.replies = replies;
    }

    fn report(&self) {
        for line in self.report_lines() {
            eprintln!("{line}");
        }
    }

    /// The report's lines, as [`report`](Self::report) prints them on
    /// stderr.
    fn report_lines(&self) -> Vec<String> {
        let mut lines = vec![format!(
            "trace: {:<28} {:>10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            "µs", "samples", "min", "p50", "p90", "p99", "p99.9", "max"
        )];
        for (name, histogram) in [
            ("loop iteration", &self.iteration),
            ("service()", &self.service),
            ("request → batch end", &self.round_trip),
        ] {
            let micros = |ns: u64| ns as f64 / 1_000.0;
            lines.push(format!(
                "trace: {:<28} {:>10} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.1} {:>9.1}",
                name,
                histogram.len(),
                micros(histogram.min()),
                micros(histogram.value_at_quantile(0.5)),
                micros(histogram.value_at_quantile(0.9)),
                micros(histogram.value_at_quantile(0.99)),
                micros(histogram.value_at_quantile(0.999)),
                micros(histogram.max()),
            ));
        }
        if !self.in_flight.is_empty() {
            lines.push(format!(
                "trace: {} requests were never answered",
                self.in_flight.len()
            ));
        }
        if self.untracked > 0 {
            lines.push(format!(
                "trace: {} requests were not tracked (sent past {} in flight, or under an ID \
                 already in flight)",
                self.untracked,
                Self::IN_FLIGHT_CAP
            ));
        }
        if self.unmatched > 0 {
            lines.push(format!(
                "trace: {} answers named no request the trace held",
                self.unmatched
            ));
        }
        lines
    }
}

/// The head of a frame the trace reads: its tag, and the request ID that
/// follows it in a request and in every final frame -- the widest head
/// either direction needs.
const HEAD_LEN: usize = TAG_LEN + REQUEST_ID_LEN;

/// Walks a stream of `[len: u32 LE][tag][body]` frames as its bytes come,
/// in whatever pieces, and reports each complete frame's head: the first
/// [`HEAD_LEN`] payload bytes, kept across pieces and feeds, or fewer for
/// a shorter frame (none for an empty one, which the protocol never
/// sends). A fixed array, so scanning allocates nothing.
#[derive(Default)]
struct FrameScanner {
    state: Scan,
}

enum Scan {
    Header {
        got: usize,
        bytes: [u8; 4],
    },
    Payload {
        left: usize,
        head: [u8; HEAD_LEN],
        got: usize,
    },
}

impl Default for Scan {
    fn default() -> Self {
        Scan::Header {
            got: 0,
            bytes: [0; 4],
        }
    }
}

impl FrameScanner {
    fn new() -> Self {
        Self::default()
    }

    fn feed(&mut self, mut bytes: &[u8], on_frame: &mut impl FnMut(&[u8])) {
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
                            on_frame(&[]);
                            self.state = Scan::default();
                        } else {
                            self.state = Scan::Payload {
                                left: len,
                                head: [0; HEAD_LEN],
                                got: 0,
                            };
                        }
                    }
                }
                Scan::Payload { left, head, got } => {
                    let take = (*left).min(bytes.len());
                    // The head, from whichever pieces carry it.
                    let keep = take.min(HEAD_LEN - *got);
                    head[*got..*got + keep].copy_from_slice(&bytes[..keep]);
                    *got += keep;
                    *left -= take;
                    bytes = &bytes[take..];
                    if *left == 0 {
                        on_frame(&head[..*got]);
                        self.state = Scan::default();
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

    use melin_wire_protocol::control::{RefusalKind, TransportResponse};
    use melin_wire_protocol::control_codec::{
        MAX_CONTROL_FRAME_LEN, TAG_APP, TAG_CHALLENGE_RESPONSE, TAG_REQUEST,
        TAG_RESPONSE_HEARTBEAT, encode_transport_response,
    };
    use proptest::prelude::*;

    /// Request `id`'s frame carrying `body`.
    fn request(id: u64, body: &[u8]) -> Vec<u8> {
        frame(&[&[TAG_REQUEST][..], &id.to_le_bytes(), body].concat())
    }

    /// The node's `response`, length prefix included.
    fn control(response: TransportResponse) -> Vec<u8> {
        let mut buf = [0u8; MAX_CONTROL_FRAME_LEN];
        let n = encode_transport_response(&response, &mut buf).unwrap();
        buf[..n].to_vec()
    }

    /// Every head `scanner` reports for `stream` fed in `pieces`.
    fn heads(stream: &[u8], cuts: &[usize]) -> Vec<Vec<u8>> {
        let mut scanner = FrameScanner::new();
        let mut heads = Vec::new();
        let mut prev = 0;
        for &cut in cuts.iter().chain(std::iter::once(&stream.len())) {
            scanner.feed(&stream[prev..cut], &mut |head| heads.push(head.to_vec()));
            prev = cut;
        }
        heads
    }

    /// A stream of every shape of frame the trace meets: a request (its
    /// head the tag and the ID), a frame shorter than a head, an empty
    /// one, a final frame, and a long application response.
    fn every_shape() -> (Vec<u8>, Vec<Vec<u8>>) {
        let stream = [
            request(0x0102_0304_0506_0708, b"body"),
            frame(&[TAG_CHALLENGE_RESPONSE, 1, 2]),
            frame(&[]),
            control(TransportResponse::BatchEnd {
                id: RequestId(u64::MAX),
            }),
            frame(&[&[TAG_APP][..], &[0xEE; 40]].concat()),
        ]
        .concat();
        let expected = vec![
            [&[TAG_REQUEST][..], &0x0102_0304_0506_0708u64.to_le_bytes()].concat(),
            vec![TAG_CHALLENGE_RESPONSE, 1, 2],
            vec![],
            control(TransportResponse::BatchEnd {
                id: RequestId(u64::MAX),
            })[4..]
                .to_vec(),
            [&[TAG_APP][..], &[0xEE; 8]].concat(),
        ];
        (stream, expected)
    }

    #[test]
    fn frames_report_their_heads_whatever_the_pieces() {
        let (stream, expected) = every_shape();
        // A byte at a time (every head cut inside its ID), in threes, and
        // as one slice: the same heads.
        for pieces in [1usize, 3, stream.len()] {
            let cuts: Vec<usize> = (pieces..stream.len()).step_by(pieces).collect();
            assert_eq!(heads(&stream, &cuts), expected, "pieces of {pieces}");
        }
    }

    proptest! {
        /// Any split of the stream across feeds reports the same heads.
        #[test]
        fn any_split_reports_the_same_heads(
            raw_cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..12),
        ) {
            let (stream, expected) = every_shape();
            let mut cuts: Vec<usize> =
                raw_cuts.iter().map(|i| i.index(stream.len() + 1)).collect();
            cuts.sort_unstable();
            prop_assert_eq!(heads(&stream, &cuts), expected);
        }
    }

    /// `trace` fed `bytes` as the stack's receive.
    fn receive(trace: &mut Trace, bytes: &[u8]) {
        let clock = TscClock::calibrate();
        trace.received(bytes, &[], bytes.len(), &clock);
    }

    #[test]
    fn answers_pair_with_their_own_requests_whatever_the_order() {
        let mut trace = Trace::new();
        // The handshake's answer is no request.
        let out = [
            frame(&[TAG_CHALLENGE_RESPONSE; 97]),
            request(7, b"a"),
            request(3, b"b"),
            request(9, b""),
            request(11, b"c"),
        ]
        .concat();
        // Split across the ring's two halves, mid-request.
        trace.sent(&out[..60], &out[60..], out.len());
        assert_eq!(trace.in_flight.len(), 4);

        // Out of order, every kind of final frame, with responses and a
        // heartbeat among them: each pairs with its own request.
        let replies = [
            control(TransportResponse::ServerBusy { id: RequestId(9) }),
            frame(&[TAG_APP, 0x30, b'a']),
            frame(&[TAG_RESPONSE_HEARTBEAT]),
            control(TransportResponse::BatchEnd { id: RequestId(7) }),
            control(TransportResponse::Refused {
                id: RequestId(11),
                kind: RefusalKind::Malformed,
                reason: "no",
            }),
        ]
        .concat();
        receive(&mut trace, &replies);
        assert_eq!(trace.round_trip.len(), 3);
        assert_eq!(
            trace.in_flight.keys().copied().collect::<Vec<_>>(),
            [RequestId(3)],
            "only the unanswered request is left"
        );
        receive(
            &mut trace,
            &control(TransportResponse::BatchEndDegraded { id: RequestId(3) }),
        );
        assert!(trace.in_flight.is_empty());
        assert_eq!(trace.round_trip.len(), 4);
        assert_eq!((trace.untracked, trace.unmatched), (0, 0));
    }

    /// An answer for no request held, and a repeated ID, are counted, not
    /// paired.
    #[test]
    fn unknown_and_repeated_ids_are_counted() {
        let mut trace = Trace::new();
        let out = [request(1, b""), request(1, b"again")].concat();
        trace.sent(&out, &[], out.len());
        assert_eq!(trace.untracked, 1, "the repeated ID");
        assert_eq!(trace.in_flight.len(), 1);

        receive(
            &mut trace,
            &control(TransportResponse::BatchEnd { id: RequestId(2) }),
        );
        assert_eq!(trace.unmatched, 1);
        assert_eq!(trace.round_trip.len(), 0);
        // The report says so, beside the request never answered, after the
        // three histograms and their header.
        let lines = trace.report_lines();
        assert_eq!(lines.len(), 4 + 3, "{lines:#?}");
        assert!(lines[1].contains("loop iteration"), "{lines:#?}");
        assert_eq!(lines[4], "trace: 1 requests were never answered");
        assert!(lines[5].starts_with("trace: 1 requests were not tracked"));
        assert_eq!(lines[6], "trace: 1 answers named no request the trace held");
        // A request too short for its ID is no request.
        let short = frame(&[TAG_REQUEST, 1, 2]);
        trace.sent(&short, &[], short.len());
        assert_eq!(trace.in_flight.len(), 1);
    }

    /// At the cap a new request's stamp is not kept, and is counted; the
    /// stamps held stay.
    #[test]
    fn past_the_cap_a_new_stamp_is_not_recorded() {
        let mut trace = Trace::new();
        let now = rdtscp();
        for id in 0..Trace::IN_FLIGHT_CAP as u64 {
            trace.in_flight.insert(RequestId(id), now);
        }
        let past = request(u64::MAX, b"");
        trace.sent(&past, &[], past.len());
        assert_eq!(trace.untracked, 1);
        assert_eq!(trace.in_flight.len(), Trace::IN_FLIGHT_CAP);
        assert!(!trace.in_flight.contains_key(&RequestId(u64::MAX)));
        // An ID already held is still a repeat, counted the same way.
        let again = request(0, b"");
        trace.sent(&again, &[], again.len());
        assert_eq!(trace.untracked, 2);
    }
}
