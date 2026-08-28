#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Open-loop load generator for echo-server: a fixed schedule of requests
//! on one connection, each latency measured against the moment the
//! request was *due* rather than the moment it left, and the distribution
//! written as an HdrHistogram log.
//!
//! Where `echo-client` keeps one request in flight and measures the floor,
//! this drives a chosen rate and measures what the floor becomes under it.
//! Two things about the measurement are deliberate and worth knowing:
//!
//! - **Coordinated omission is corrected.** Every request carries the
//!   cycle-counter tick at which the schedule said it should go, and its
//!   latency is counted from that tick when the echo comes back. A stall
//!   anywhere -- the server, the wire, or this client -- therefore shows
//!   up as latency instead of hiding as a pause in the sending.
//! - **The run is shaped like the Aeron benchmark harness.** Warmup
//!   iterations at a warmup rate, then measured iterations at the target
//!   rate, one iteration being one second of the schedule; a receive
//!   deadline after the last send; the histogram named
//!   `<prefix>_rate=<r>_batch=<b>_length=<l>.hdr`, with `.FAIL` appended
//!   if a message went missing. The file is what that harness writes, so
//!   a run from here sits beside one made with it.
//!
//! One thread, pinned if asked, busy-spinning: nothing on the send or
//! receive path allocates, and time is read from the cycle counter.
//! Transport is kernel TCP by default; with the `dpdk` feature the same
//! loop runs over DPDK and a userspace TCP stack (`--transport dpdk`).
//!
//! ```sh
//! echo-bench --key trader.key --rate 100K --iterations 20 \
//!     --output-directory results --output-file melin-echo
//! ```
//!
//! Exit status: 0 when every message came back, 1 when the run completed
//! but some did not (the histogram is still written, as `.FAIL`), 2 when
//! it could not run at all.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, SystemTime};

use clap::{Parser, ValueEnum};
use ed25519_dalek::SigningKey;
use hdrhistogram::Histogram;

use echo_server::MAX_PAYLOAD;

// A binary's root resolves `mod` against its own directory, so the
// modules under `bench/` are named by path, as is the key loader shared
// with `echo-client`.
#[path = "keys.rs"]
mod keys;

#[cfg(any(feature = "dpdk", test))]
#[path = "bench/arp.rs"]
mod arp;
#[cfg(feature = "dpdk")]
#[path = "bench/dpdk.rs"]
mod dpdk;
#[path = "bench/hdr.rs"]
mod hdr;
#[path = "bench/pacing.rs"]
mod pacing;
#[path = "bench/transport.rs"]
mod transport;
#[path = "bench/tsc.rs"]
mod tsc;
#[path = "bench/wire.rs"]
mod wire;

use pacing::PaceClock;
use transport::{KernelTcp, Transport};
use tsc::{TscClock, rdtscp};
use wire::{Frame, Inbound, RequestFrame};

const NANOS_PER_SECOND: u64 = 1_000_000_000;
/// The histogram's ceiling, an hour in nanoseconds: what the Aeron
/// harness uses, so the two encode identically.
const MAX_LATENCY_NS: u64 = 3_600 * NANOS_PER_SECOND;
/// A send this far behind its slot is counted as late. Wider than any
/// jitter in the loop itself, so the count reports the client falling
/// structurally behind its schedule, not a microsecond here and there.
const LATE_SLACK_NS: u64 = 1_000_000;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
/// Inbound buffer. Many times the largest frame, and more than the
/// replies that can be in flight at any rate this measures.
const INBOUND_CAPACITY: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TransportKind {
    Kernel,
    Dpdk,
}

#[derive(Parser)]
#[command(
    name = "echo-bench",
    about = "Drive echo-server at a fixed rate and record the round-trip distribution"
)]
struct Cli {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:9876")]
    server: SocketAddr,
    /// Ed25519 private key: PEM as written by `openssl genpkey -algorithm
    /// ed25519`, or a raw 32-byte seed. Its role must be one that may
    /// write.
    #[arg(long)]
    key: PathBuf,
    #[arg(long, value_enum, default_value_t = TransportKind::Kernel)]
    transport: TransportKind,
    /// Messages per second during measurement: a number, or one with a K
    /// or M suffix.
    #[arg(long, value_parser = hdr::parse_rate)]
    rate: u64,
    /// Seconds of schedule to measure.
    #[arg(long, default_value_t = 20)]
    iterations: u64,
    #[arg(long, value_parser = hdr::parse_rate, default_value = "25K")]
    warmup_rate: u64,
    /// Seconds of schedule to run, and discard, before measuring.
    #[arg(long, default_value_t = 10)]
    warmup_iterations: u64,
    /// Messages sent together at each scheduled slot.
    #[arg(long, default_value_t = 1)]
    batch_size: u64,
    /// Bytes per message, 24 to the server's cap.
    #[arg(long, default_value_t = MAX_PAYLOAD)]
    message_length: usize,
    /// How long to wait for the last replies after the last send.
    #[arg(long, default_value_t = 3)]
    receive_deadline_secs: u64,
    /// Where to write the histogram. Needs --output-file.
    #[arg(long)]
    output_directory: Option<PathBuf>,
    /// Name prefix of the histogram file; the rate, batch and length are
    /// appended.
    #[arg(long)]
    output_file: Option<String>,
    /// Pin the loop to this core.
    #[arg(long)]
    core: Option<usize>,
    /// Print sent and received counts once per iteration.
    #[arg(long)]
    report_progress: bool,
    #[cfg(feature = "dpdk")]
    #[command(flatten)]
    dpdk: dpdk::DpdkArgs,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<bool, String> {
    if cli.batch_size == 0 {
        return Err("--batch-size must be at least 1".into());
    }
    if cli.iterations == 0 {
        return Err("--iterations must be at least 1".into());
    }
    if cli.output_directory.is_some() != cli.output_file.is_some() {
        return Err("--output-directory and --output-file go together".into());
    }
    // Pin before calibrating, so the counter measured is the core's own.
    if let Some(core) = cli.core {
        match melin_app::affinity::pin_to_core(core) {
            Ok(core) => eprintln!("pinned to core {core}"),
            Err(e) => eprintln!("warning: not pinned: {e}"),
        }
    }
    let key = keys::load_key(&cli.key)?;
    let clock = TscClock::calibrate();

    match cli.transport {
        TransportKind::Kernel => {
            let transport = KernelTcp::connect(cli.server, CONNECT_TIMEOUT)
                .map_err(|e| format!("cannot connect to {}: {e}", cli.server))?;
            execute(transport, &cli, &clock, &key)
        }
        TransportKind::Dpdk => run_dpdk(&cli, &clock, &key),
    }
}

#[cfg(feature = "dpdk")]
fn run_dpdk(cli: &Cli, clock: &TscClock, key: &SigningKey) -> Result<bool, String> {
    let server = match cli.server {
        SocketAddr::V4(v4) => v4,
        SocketAddr::V6(_) => return Err("the DPDK transport is IPv4 only".into()),
    };
    let transport = dpdk::DpdkTcp::connect(&cli.dpdk, server, clock)?;
    execute(transport, cli, clock, key)
}

#[cfg(not(feature = "dpdk"))]
fn run_dpdk(_cli: &Cli, _clock: &TscClock, _key: &SigningKey) -> Result<bool, String> {
    Err("this echo-bench was built without the dpdk feature; rebuild with --features dpdk".into())
}

/// Authenticate, warm up, measure, report. `Ok(true)` if every measured
/// message came back.
fn execute<T: Transport>(
    mut transport: T,
    cli: &Cli,
    clock: &TscClock,
    key: &SigningKey,
) -> Result<bool, String> {
    let inbound = Inbound::with_capacity(INBOUND_CAPACITY);
    let mut rx = Receiver {
        inbound,
        body_len: cli.message_length,
        next_seq: 1,
        received: 0,
    };
    transport::authenticate(&mut transport, key, clock, &mut rx.inbound, AUTH_TIMEOUT)?;
    eprintln!("connected to {} over {}", cli.server, transport.name());

    let mut frame = RequestFrame::new(cli.message_length)?;
    let mut histogram = Histogram::<u64>::new_with_bounds(1, MAX_LATENCY_NS, 3)
        .map_err(|e| format!("histogram: {e:?}"))?;
    // Request sequences start at one and never repeat on a connection;
    // zero was the authentication frame.
    let mut seq: u64 = 1;
    let deadline = Duration::from_secs(cli.receive_deadline_secs);

    if cli.warmup_iterations > 0 {
        let phase = Phase {
            rate: cli.warmup_rate,
            iterations: cli.warmup_iterations,
            batch: cli.batch_size,
            receive_deadline: deadline,
            report_progress: cli.report_progress,
        };
        println!(
            "Running warmup for {} iterations of {} messages each, with {} bytes payload and a burst size of {}...",
            phase.iterations, phase.rate, cli.message_length, phase.batch
        );
        let result = run_phase(
            &mut transport,
            clock,
            &mut histogram,
            &mut rx,
            &mut frame,
            &mut seq,
            &phase,
        )?;
        warn_if_incomplete(&result, deadline);
        histogram.reset();
    }

    let phase = Phase {
        rate: cli.rate,
        iterations: cli.iterations,
        batch: cli.batch_size,
        receive_deadline: deadline,
        report_progress: cli.report_progress,
    };
    println!(
        "Running measurement for {} iterations of {} messages each, with {} bytes payload and a burst size of {}...",
        phase.iterations, phase.rate, cli.message_length, phase.batch
    );
    let started_at = SystemTime::now();
    let start_tick = rdtscp();
    let result = run_phase(
        &mut transport,
        clock,
        &mut histogram,
        &mut rx,
        &mut frame,
        &mut seq,
        &phase,
    )?;
    let elapsed = Duration::from_nanos(clock.elapsed_ns(start_tick, rdtscp()));
    warn_if_incomplete(&result, deadline);

    println!();
    hdr::summary(&mut std::io::stdout(), &histogram).map_err(|e| format!("stdout: {e}"))?;
    println!(
        "\nsent {} received {} expected {} in {:.3}s ({:.0} msg/s achieved, {} target)",
        result.sent,
        result.received,
        result.expected,
        elapsed.as_secs_f64(),
        result.sent as f64 / elapsed.as_secs_f64(),
        phase.rate
    );
    println!(
        "late sends (>{} ms behind schedule): {}, max send delay {:.1} µs",
        LATE_SLACK_NS / 1_000_000,
        result.late,
        clock.elapsed_ns(0, result.max_delay_ticks) as f64 / 1_000.0
    );

    if let (Some(dir), Some(prefix)) = (&cli.output_directory, &cli.output_file) {
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("cannot create {}: {e}", dir.display()))?;
        let name = hdr::file_name(
            prefix,
            cli.rate,
            cli.batch_size,
            cli.message_length,
            result.ok(),
        );
        let path = dir.join(name);
        hdr::write(&path, &histogram, started_at, elapsed)?;
        println!("histogram written to {}", path.display());
    }
    Ok(result.ok())
}

struct Phase {
    rate: u64,
    iterations: u64,
    batch: u64,
    receive_deadline: Duration,
    report_progress: bool,
}

struct PhaseResult {
    expected: u64,
    sent: u64,
    received: u64,
    /// Slots sent more than `LATE_SLACK_NS` after they were due.
    late: u64,
    /// The largest gap between a slot's due tick and its send.
    max_delay_ticks: u64,
}

impl PhaseResult {
    fn ok(&self) -> bool {
        self.expected == self.sent && self.sent == self.received
    }
}

/// Replies: where they land, and the check every one of them passes
/// before it becomes a sample.
struct Receiver {
    inbound: Inbound,
    body_len: usize,
    /// The sequence the next reply must carry. One connection, requests
    /// in order, the server sequencing them in order: a reply out of
    /// order means one was lost or the stream is not what it seems.
    next_seq: u64,
    received: u64,
}

impl Receiver {
    /// Read everything that has arrived and record each echo. Returns
    /// when the transport has nothing more, or with the first thing that
    /// is not an echo of ours.
    #[inline(always)]
    fn drain<T: Transport>(
        &mut self,
        transport: &mut T,
        clock: &TscClock,
        histogram: &mut Histogram<u64>,
    ) -> Result<(), String> {
        loop {
            let space = self.inbound.space();
            if space.is_empty() {
                return Err(
                    "inbound buffer full: the server sent a frame larger than any of ours".into(),
                );
            }
            let n = transport
                .recv(space)
                .map_err(|e| format!("connection lost: {e}"))?;
            if n == 0 {
                return Ok(());
            }
            self.inbound.filled(n);

            while let Some(payload) = self.inbound.pop()? {
                match wire::decode(payload) {
                    Frame::Echo {
                        tick,
                        seq,
                        checksum,
                        body_len,
                    } => {
                        // The clock stops here, before any checking.
                        let now = rdtscp();
                        if checksum != wire::CHECKSUM {
                            return Err(format!(
                                "reply {seq} carries checksum {checksum:#x}, expected {:#x}",
                                wire::CHECKSUM
                            ));
                        }
                        if body_len != self.body_len {
                            return Err(format!(
                                "reply {seq} is {body_len} bytes, the request was {}",
                                self.body_len
                            ));
                        }
                        if seq != self.next_seq {
                            return Err(format!(
                                "reply {seq} arrived when {} was expected: a reply was lost or reordered",
                                self.next_seq
                            ));
                        }
                        self.next_seq += 1;
                        self.received += 1;
                        histogram.saturating_record(clock.elapsed_ns(tick, now));
                    }
                    Frame::BatchEnd | Frame::Heartbeat => {}
                    Frame::Rejected => {
                        return Err(
                            "the server rejected the request: does the key's role allow writing?"
                                .into(),
                        );
                    }
                    Frame::EngineError => return Err("the server reported an engine error".into()),
                    Frame::Busy => return Err("the server is busy".into()),
                    Frame::Malformed => return Err("an echo too short to be a reply to us".into()),
                    Frame::Empty => return Err("an empty frame from the server".into()),
                    Frame::Unknown(tag) => {
                        return Err(format!("unexpected frame from the server: tag {tag:#04x}"));
                    }
                    Frame::Challenge(_) | Frame::ServerReady | Frame::AuthFailed => {
                        return Err("an authentication frame after the handshake".into());
                    }
                }
            }
        }
    }
}

/// Send `rate * iterations` messages on the schedule, then wait for the
/// replies. The histogram receives every echo that arrives during the
/// phase; the caller resets it between warmup and measurement.
fn run_phase<T: Transport>(
    transport: &mut T,
    clock: &TscClock,
    histogram: &mut Histogram<u64>,
    rx: &mut Receiver,
    frame: &mut RequestFrame,
    seq: &mut u64,
    phase: &Phase,
) -> Result<PhaseResult, String> {
    let expected = phase.rate * phase.iterations;
    // The harness's expression, integer division included, so the two
    // schedules agree to the nanosecond.
    let period_ns = NANOS_PER_SECOND * phase.batch / phase.rate;
    let slack_ticks = clock.ticks(LATE_SLACK_NS);
    let stall_ticks = clock.ticks(phase.receive_deadline.as_nanos() as u64);
    let received_before = rx.received;

    let mut pacer = PaceClock::new(clock.ticks(period_ns), rdtscp());
    let mut sent = 0u64;
    let mut late = 0u64;
    let mut max_delay_ticks = 0u64;
    let mut next_report = phase.rate;

    while sent < expected {
        let now = rdtscp();
        let Some(due) = pacer.pop_due(now) else {
            transport.service(clock.unix_ns(now));
            rx.drain(transport, clock, histogram)?;
            continue;
        };
        let delay = now.saturating_sub(due);
        max_delay_ticks = max_delay_ticks.max(delay);
        if delay > slack_ticks {
            late += 1;
        }

        let burst = phase.batch.min(expected - sent);
        for _ in 0..burst {
            frame.stamp(*seq, due);
            push_all(transport, frame.bytes(), clock, rx, histogram, stall_ticks)?;
            *seq += 1;
            sent += 1;
        }
        // On the wire now, not on the next turn of the loop.
        transport.service(clock.unix_ns(rdtscp()));
        rx.drain(transport, clock, histogram)?;

        if phase.report_progress && sent >= next_report {
            eprintln!("  sent {sent} received {}", rx.received - received_before);
            next_report += phase.rate;
        }
    }

    let deadline = rdtscp().saturating_add(stall_ticks);
    while rx.received - received_before < sent {
        let now = rdtscp();
        if now >= deadline {
            break;
        }
        transport.service(clock.unix_ns(now));
        rx.drain(transport, clock, histogram)?;
    }

    Ok(PhaseResult {
        expected,
        sent,
        received: rx.received - received_before,
        late,
        max_delay_ticks,
    })
}

/// Push every byte of one frame, servicing the transport and draining
/// replies whenever it will not take more. Draining matters: a server
/// waiting on us to read is the one way a full socket stays full.
#[inline(always)]
fn push_all<T: Transport>(
    transport: &mut T,
    bytes: &[u8],
    clock: &TscClock,
    rx: &mut Receiver,
    histogram: &mut Histogram<u64>,
    stall_ticks: u64,
) -> Result<(), String> {
    let mut offset = 0;
    let mut stalled_since: Option<u64> = None;
    while offset < bytes.len() {
        let n = transport
            .send(&bytes[offset..])
            .map_err(|e| format!("cannot send: {e}"))?;
        offset += n;
        if n > 0 {
            stalled_since = None;
            continue;
        }
        let now = rdtscp();
        let since = *stalled_since.get_or_insert(now);
        if now.saturating_sub(since) > stall_ticks {
            return Err("the connection stopped taking data".into());
        }
        transport.service(clock.unix_ns(now));
        rx.drain(transport, clock, histogram)?;
    }
    Ok(())
}

fn warn_if_incomplete(result: &PhaseResult, deadline: Duration) {
    if result.sent != result.expected {
        println!(
            "\n*** WARNING: expected to send {} messages but sent {}",
            result.expected, result.sent
        );
    }
    if result.received != result.sent {
        println!(
            "\n*** WARNING: Not all messages were received after {}s deadline: expected {} vs received {} (loss {:.4}%)!",
            deadline.as_secs(),
            result.sent,
            result.received,
            100.0 - (100.0 * result.received as f64 / result.sent as f64)
        );
    }
}
