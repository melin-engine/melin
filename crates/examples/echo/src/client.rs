#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Command-line client for the echo example: a closed loop of requests
//! over one connection, one in flight at a time, and the round-trip
//! latency distribution they measured.
//!
//! One request in flight is deliberate. It is the shape that measures
//! latency rather than throughput: every sample is a complete trip through
//! the reader, the rings, the journal, the application and the response
//! stage, with nothing queued in front of it. Throughput under load is a
//! different measurement and needs a different tool.
//!
//! Every reply is checked against what was sent — the bytes differ per
//! request, so a stale or misrouted reply is a hard error, not a sample.
//!
//! The wire — framing, the Ed25519 handshake, the reply batch, a node's
//! silence — is `melin-client`'s; this file is the loop and its figures.
//!
//! ```sh
//! echo-client --key /tmp/echo-key.pem --size 288 --count 10000
//! ```
//!
//! # Transports
//!
//! `--transport kernel` (the default) is the blocking connection on a
//! kernel socket. `--transport dpdk`, in a build with the `dpdk` feature,
//! is the polled connection over kernel bypass, on a NIC the client
//! brings up from the `--dpdk-*` flags (see `client/dpdk.rs`). Both run
//! the one measurement loop below, through `melin-client`'s session trait,
//! so the two report the same figures in the same format.
//!
//! `--core` pins the measuring thread once the connection is up. It
//! matters most on DPDK, where the thread spins at full load polling the
//! NIC and its samples are only as steady as its core: give it one of its
//! own, off the server's.

use std::net::SocketAddr;
#[cfg(feature = "dpdk")]
use std::net::SocketAddrV4;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use melin_client::{Ack, Connection, Frame, Session, key};

use echo_server::{KIND_RESP_ECHO, KIND_RESP_REJECTED, MAX_PAYLOAD};

// The DPDK transport's flags and dial: compiled only with the `dpdk`
// feature, so a default build has no `--dpdk-*` flag it would ignore.
#[cfg(feature = "dpdk")]
#[path = "client/dpdk.rs"]
mod dpdk;

type Error = Box<dyn std::error::Error>;

// ---------------------------------------------------------------------------
// Command line
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
enum TransportKind {
    Kernel,
    Dpdk,
}

#[derive(Parser)]
#[command(
    name = "echo-client",
    about = "Send payloads to echo-server and report round-trip latency"
)]
struct Cli {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:9876")]
    server: SocketAddr,
    /// Ed25519 private key: PEM as written by `openssl genpkey -algorithm
    /// ed25519`, or a raw 32-byte seed.
    #[arg(long)]
    key: PathBuf,
    /// Bytes per request, up to the server's cap.
    #[arg(long, default_value_t = MAX_PAYLOAD)]
    size: usize,
    /// Requests to send, one after the other.
    #[arg(long, default_value_t = 1)]
    count: usize,
    /// What the connection runs on: kernel TCP, or DPDK kernel bypass
    /// (a build with the `dpdk` feature, configured by the `--dpdk-*`
    /// flags).
    #[arg(long, value_enum, default_value_t = TransportKind::Kernel)]
    transport: TransportKind,
    /// Pin the measuring thread to this core once connected. On DPDK the
    /// thread spins at full load: give it a core of its own.
    // `usize`: what the affinity helper takes a core as.
    #[arg(long)]
    core: Option<usize>,
    // The node's own `--dpdk-*` flags, `--dpdk-ip` required here.
    #[cfg(feature = "dpdk")]
    #[command(flatten)]
    dpdk: melin_dpdk::DpdkArgs,
}

fn main() -> ExitCode {
    // Diagnostics on stderr, warnings and up unless `RUST_LOG` says
    // otherwise: the affinity helper warns when `--core`'s pin is weaker
    // than asked (SCHED_FIFO refused), which explains a noisy measurement;
    // the DPDK NIC's bring-up logs at `debug`. stdout carries the figures
    // alone.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::WARN.into())
                .from_env_lossy(),
        )
        .with_writer(std::io::stderr)
        .init();

    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprint!("{}", report(e.as_ref()));
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<(), Error> {
    if cli.size > MAX_PAYLOAD {
        return Err(format!(
            "--size {}: the server takes at most {MAX_PAYLOAD} bytes per request",
            cli.size
        )
        .into());
    }
    if cli.count == 0 {
        return Err("--count must be at least 1".into());
    }
    let transport = check_transport(&cli)?;
    if cli.core.is_some() {
        // Before anything narrows this thread's CPU mask (EAL pins the
        // thread that initialises it), as the runtime's helper asks.
        melin_app::affinity::capture_home_mask();
    }
    let key = key::load_signing_key(&cli.key)?;

    let mut samples = match transport {
        Transport::Kernel => measure(&mut Connection::connect(cli.server, &key)?, &cli)?,
        #[cfg(feature = "dpdk")]
        Transport::Dpdk(server) => measure(&mut dpdk::connect(&cli.dpdk, server, &key)?, &cli)?,
    };

    if cli.count == 1 {
        println!(
            "{} bytes back in {}, sequenced and journaled",
            cli.size,
            micros(samples[0])
        );
        return Ok(());
    }

    samples.sort_unstable();
    println!(
        "{} × {}-byte echoes to {}, one in flight",
        cli.count, cli.size, cli.server
    );
    println!("  {:<6} {}", "min", micros(samples[0]));
    for (label, p) in [("p50", 50.0), ("p90", 90.0), ("p99", 99.0), ("p99.9", 99.9)] {
        println!("  {label:<6} {}", micros(percentile(&samples, p)));
    }
    println!("  {:<6} {}", "max", micros(samples[samples.len() - 1]));
    Ok(())
}

/// The transport a run goes over, once its flags have been checked: what
/// [`check_transport`] hands [`run`], so a check made there is not made
/// again to recover its result.
enum Transport {
    Kernel,
    /// The DPDK transport, with the server as a DPDK dial takes it.
    #[cfg(feature = "dpdk")]
    Dpdk(SocketAddrV4),
}

/// What `--transport dpdk` reports in a build without the feature.
#[cfg(not(feature = "dpdk"))]
const NO_DPDK: &str = "this echo-client was built without the `dpdk` feature: rebuild with \
                       `--features dpdk` (needs libdpdk) for --transport dpdk";

/// Refuse a transport this build cannot run, before the key is read or
/// anything is dialed.
#[cfg(not(feature = "dpdk"))]
fn check_transport(cli: &Cli) -> Result<Transport, Error> {
    match cli.transport {
        TransportKind::Kernel => Ok(Transport::Kernel),
        TransportKind::Dpdk => Err(NO_DPDK.into()),
    }
}

/// Refuse DPDK flags on a kernel run: any `--dpdk-*` flag given with the
/// kernel transport (the default) means `--transport dpdk` was forgotten,
/// and the run would measure the kernel path under the operator's nose.
/// And on a DPDK run, a server a NIC cannot reach (the loopback default),
/// before the key is read.
#[cfg(feature = "dpdk")]
fn check_transport(cli: &Cli) -> Result<Transport, Error> {
    match cli.transport {
        TransportKind::Kernel => {
            if let Some(flag) = cli.dpdk.first_given() {
                return Err(format!(
                    "{flag} is for --transport dpdk: without it this run would measure the \
                     kernel path"
                )
                .into());
            }
            Ok(Transport::Kernel)
        }
        TransportKind::Dpdk => Ok(Transport::Dpdk(dpdk::check_server(cli.server)?)),
    }
}

/// The measurement, whatever the transport: pin the thread if asked, then
/// `--count` round trips, one in flight, each sample its round trip in
/// nanoseconds, in the order taken.
///
/// Generic rather than over `&mut dyn Session`: each transport gets its
/// own copy of the loop with direct calls, so the kernel path runs the
/// code it ran before the session trait existed, and neither pays an
/// indirect call per frame.
fn measure<S: Session>(node: &mut S, cli: &Cli) -> Result<Vec<u64>, Error> {
    if let Some(core) = cli.core {
        // After the connection is up: a DPDK NIC's EAL pins the thread
        // that initialises it to its main lcore, which would undo a pin
        // taken before.
        let core =
            melin_app::affinity::pin_to_core(core).map_err(|e| format!("--core {core}: {e}"))?;
        eprintln!("pinned to core {core}");
    }
    // `u64` nanoseconds: what `Duration` converts to losslessly for any
    // trip shorter than centuries, and what sorts without a comparator.
    let mut samples: Vec<u64> = Vec::with_capacity(cli.count);
    let mut payload = vec![0u8; cli.size];
    for i in 1..=cli.count as u64 {
        fill(&mut payload, i);
        samples.push(echo(node, i, &payload)?.as_nanos() as u64);
    }
    Ok(samples)
}

/// One round trip: send `payload`, check the echo is it, and return how
/// long the echo took to arrive.
///
/// The clock stops at the reply frame, not at the batch end that follows
/// it — the reply is what the caller waited for — which is why this
/// reads frame by frame instead of asking the client for the batch.
fn echo<S: Session>(node: &mut S, request: u64, payload: &[u8]) -> Result<Duration, Error> {
    let started = Instant::now();
    let id = node.send(payload)?;
    let elapsed = match node.next_frame()? {
        Frame::Response(reply) => {
            let elapsed = started.elapsed();
            match reply.split_first() {
                Some((&KIND_RESP_ECHO, echoed)) if echoed == payload => {}
                Some((&KIND_RESP_REJECTED, _)) => {
                    return Err("the server rejected the request".into());
                }
                _ => {
                    return Err(
                        format!("the reply to request {request} is not what was sent").into(),
                    );
                }
            }
            elapsed
        }
        Frame::BatchEnd { .. } => {
            return Err("the server ended the batch without a reply".into());
        }
        Frame::ServerBusy { .. } => return Err(melin_client::Error::ServerBusy.into()),
        Frame::Refused { kind, reason, .. } => {
            return Err(melin_client::Error::Refused {
                kind,
                reason: reason.to_string(),
            }
            .into());
        }
    };
    match node.next_frame()? {
        Frame::BatchEnd {
            id: ended,
            ack: Ack::Policy,
        } if ended == id => Ok(elapsed),
        // A round trip the ack policy did not back is not the sequencer's
        // latency: the primary lost its replica mid-run. Say so rather
        // than count it.
        Frame::BatchEnd {
            id: ended,
            ack: Ack::PrimaryOnly,
        } if ended == id => Err(format!(
            "request {request} was answered by a primary that lost its replica: \
             only its own disk backs the reply"
        )
        .into()),
        other => Err(format!("expected request {request}'s batch to end, got {other:?}").into()),
    }
}

/// A pattern that differs per request, so the reply to one cannot pass
/// for the reply to another.
fn fill(payload: &mut [u8], request: u64) {
    for (j, byte) in payload.iter_mut().enumerate() {
        // Both truncations are deliberate: this is a fingerprint, not a
        // number.
        *byte = (request as u8).wrapping_add(j as u8);
    }
}

/// Nearest-rank percentile of a sorted sample.
fn percentile(sorted: &[u64], p: f64) -> u64 {
    // Rank rounds to the nearest sample; f64 → usize is exact here since
    // the product is at most `len - 1`.
    let rank = ((sorted.len() - 1) as f64 * p / 100.0).round() as usize;
    sorted[rank]
}

/// `12.3 µs` — microseconds with one decimal, the resolution that a
/// round trip over a socket meaningfully has.
fn micros(ns: u64) -> String {
    format!("{:.1} µs", ns as f64 / 1_000.0)
}

/// What a failed run prints: `error: ` and the error, then each cause in
/// its source chain that the level above does not already end with.
///
/// `melin-client`'s errors put their source at the end of their own
/// message (a failed dial reads `cannot connect to <addr>: <why>`, and
/// over DPDK the why names the MAC the SYN went to, the first thing to
/// check; a deadline reads `... (last attempt: <error>)`), so for them
/// this prints the one line it always did. The chain is walked anyway so
/// that a cause an error keeps to itself is never lost.
fn report(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = format!("error: {error}\n");
    let mut parent = error.to_string();
    let mut cause = error.source();
    while let Some(e) = cause {
        let text = e.to_string();
        if !ends_with_cause(&parent, &text) {
            out.push_str(&format!("  caused by: {text}\n"));
        }
        parent = text;
        cause = e.source();
    }
    out
}

/// Whether `parent`'s message already ends with `cause`'s, as
/// `<context>: <cause>`, or that in parentheses.
fn ends_with_cause(parent: &str, cause: &str) -> bool {
    let tail = format!(": {cause}");
    parent.ends_with(&tail)
        || parent
            .strip_suffix(')')
            .is_some_and(|inner| inner.ends_with(&tail))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use melin_client::RequestId;

    #[test]
    fn consecutive_requests_get_different_payloads() {
        let mut a = [0u8; 16];
        let mut b = [0u8; 16];
        fill(&mut a, 1);
        fill(&mut b, 2);
        assert_ne!(a, b);
        // Deterministic: the same request fills the same bytes.
        let mut again = [0u8; 16];
        fill(&mut again, 1);
        assert_eq!(a, again);
        // The empty payload is a valid, if uninformative, request.
        fill(&mut [], 3);
    }

    #[test]
    fn percentiles_use_the_nearest_rank() {
        let sorted: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&sorted, 0.0), 1);
        assert_eq!(percentile(&sorted, 50.0), 51);
        assert_eq!(percentile(&sorted, 99.0), 99);
        assert_eq!(percentile(&sorted, 100.0), 100);
        assert_eq!(percentile(&[42], 99.9), 42);
    }

    #[test]
    fn microseconds_keep_one_decimal() {
        assert_eq!(micros(12_345), "12.3 µs");
        assert_eq!(micros(0), "0.0 µs");
        assert_eq!(micros(1_000_000), "1000.0 µs");
    }

    /// The kernel path's error line is the one it always was: the client's
    /// errors carry their source in their own message, so the chain adds
    /// nothing to them.
    #[test]
    fn a_client_error_reports_on_one_line() {
        let addr: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let connect: Error = melin_client::Error::Connect {
            addr,
            source: std::io::Error::new(std::io::ErrorKind::TimedOut, "no SYN-ACK from 02:99"),
        }
        .into();
        assert_eq!(
            report(connect.as_ref()),
            "error: cannot connect to 127.0.0.1:9: no SYN-ACK from 02:99\n"
        );

        // Two levels: the deadline's last attempt, and that attempt's cause.
        let deadline: Error = melin_client::Error::Deadline {
            addr,
            last: Box::new(melin_client::Error::Connect {
                addr,
                source: std::io::ErrorKind::ConnectionRefused.into(),
            }),
        }
        .into();
        let line = report(deadline.as_ref());
        assert_eq!(line.lines().count(), 1, "{line}");
        assert!(
            line.starts_with("error: 127.0.0.1:9 was not serving"),
            "{line}"
        );

        let plain: Error = "--count must be at least 1".into();
        assert_eq!(
            report(plain.as_ref()),
            "error: --count must be at least 1\n"
        );
    }

    /// A cause the top-level message leaves out gets a line of its own, so
    /// a hint carried by a source (the peer MAC to check) reaches the
    /// operator whatever wraps it.
    #[test]
    fn a_cause_the_message_omits_is_printed() {
        #[derive(Debug)]
        struct Wrapper(std::io::Error);
        impl std::fmt::Display for Wrapper {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("the dial failed")
            }
        }
        impl std::error::Error for Wrapper {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let error: Error = Box::new(Wrapper(std::io::Error::other(
            "no SYN-ACK: is 02:99:01:00:00:01 the server's MAC?",
        )));
        assert_eq!(
            report(error.as_ref()),
            "error: the dial failed\n  caused by: no SYN-ACK: is 02:99:01:00:00:01 the server's MAC?\n"
        );

        // A cause whose words merely occur in the message still says
        // something of its own.
        assert!(!ends_with_cause("timed out waiting: no route", "timed out"));
        assert!(ends_with_cause("dial: timed out", "timed out"));
        assert!(ends_with_cause("x (last attempt: timed out)", "timed out"));
    }

    /// A session that echoes in memory, in the node's reply shape: one
    /// echo response, then the batch end. The measurement loop is the
    /// same code over any `Session`, so this exercises the one both
    /// transports run.
    struct Echoing {
        /// The reply being handed out: the kind byte and the request.
        reply: Vec<u8>,
        /// Frames left in the current reply batch.
        pending: u8,
        /// Corrupt the echo, for the mismatch check.
        corrupt: bool,
        /// The last request's ID: requests are numbered from 1, as a
        /// connection numbers them.
        id: u64,
        /// End the batch under another request's ID, for the pairing
        /// check.
        wrong_id: bool,
    }

    impl Echoing {
        fn new() -> Self {
            Self {
                reply: Vec::new(),
                pending: 0,
                corrupt: false,
                id: 0,
                wrong_id: false,
            }
        }
    }

    impl Session for Echoing {
        fn send(&mut self, body: &[u8]) -> Result<RequestId, melin_client::Error> {
            self.reply.clear();
            self.reply.push(KIND_RESP_ECHO);
            self.reply.extend_from_slice(body);
            if self.corrupt
                && let Some(last) = self.reply.last_mut()
            {
                *last ^= 0xff;
            }
            self.pending = 2;
            self.id += 1;
            Ok(RequestId(self.id))
        }

        fn next_frame(&mut self) -> Result<Frame<'_>, melin_client::Error> {
            let id = RequestId(self.id + u64::from(self.wrong_id));
            let frame = match self.pending {
                2 => Frame::Response(&self.reply),
                1 => Frame::BatchEnd {
                    id,
                    ack: Ack::Policy,
                },
                _ => return Err(melin_client::Error::Disconnected),
            };
            self.pending -= 1;
            Ok(frame)
        }
    }

    #[test]
    fn the_loop_takes_one_sample_per_round_trip_over_any_session() {
        let cli = Cli::try_parse_from(["echo-client", "--key", "k.pem", "--count", "5"]).unwrap();
        let mut session = Echoing::new();
        assert_eq!(measure(&mut session, &cli).unwrap().len(), 5);

        session.corrupt = true;
        let error = measure(&mut session, &cli).unwrap_err().to_string();
        assert_eq!(error, "the reply to request 1 is not what was sent");

        // A batch end naming another request is not this round trip's.
        let mut session = Echoing::new();
        session.wrong_id = true;
        let error = measure(&mut session, &cli).unwrap_err().to_string();
        assert!(
            error.starts_with("expected request 1's batch to end"),
            "{error}"
        );
    }

    #[test]
    fn the_transport_defaults_to_kernel_and_the_core_to_none() {
        let cli = Cli::try_parse_from(["echo-client", "--key", "k.pem"]).unwrap();
        assert_eq!(cli.transport, TransportKind::Kernel);
        assert_eq!(cli.core, None);
        let cli = Cli::try_parse_from([
            "echo-client",
            "--key",
            "k.pem",
            "--transport",
            "dpdk",
            "--core",
            "3",
        ])
        .unwrap();
        assert_eq!(cli.transport, TransportKind::Dpdk);
        assert_eq!(cli.core, Some(3));
    }

    /// Without the feature there are no `--dpdk-*` flags, and asking for
    /// the DPDK transport is an error naming the feature, before any
    /// connection is attempted.
    #[cfg(not(feature = "dpdk"))]
    #[test]
    fn without_the_feature_dpdk_is_refused_by_name() {
        assert!(
            Cli::try_parse_from(["echo-client", "--key", "k.pem", "--dpdk-ip", "10.0.0.2"])
                .is_err()
        );
        // Refused before the key is read: the key file does not exist.
        let cli = Cli::try_parse_from([
            "echo-client",
            "--key",
            "/nonexistent/k.pem",
            "--transport",
            "dpdk",
        ])
        .unwrap();
        let error = run(cli).unwrap_err().to_string();
        assert!(error.contains("`dpdk` feature"), "{error}");
    }

    /// With the feature, the DPDK flags on a kernel run are refused, before
    /// the key is read: the operator forgot `--transport dpdk`.
    #[cfg(feature = "dpdk")]
    #[test]
    fn dpdk_flags_on_a_kernel_run_are_refused() {
        let cli = Cli::try_parse_from([
            "echo-client",
            "--key",
            "/nonexistent/k.pem",
            "--dpdk-ip",
            "10.0.0.2",
        ])
        .unwrap();
        let error = run(cli).unwrap_err().to_string();
        assert!(
            error.starts_with("--dpdk-ip is for --transport dpdk"),
            "{error}"
        );

        // Any of them, not only the address.
        for flag in [
            "--dpdk-eal-args=--no-pci",
            "--dpdk-ports=1",
            "--dpdk-peer-mac=02:99:01:00:00:01",
            "--dpdk-mtu=9000",
        ] {
            let cli =
                Cli::try_parse_from(["echo-client", "--key", "/nonexistent/k.pem", flag]).unwrap();
            let error = run(cli).unwrap_err().to_string();
            let name = flag.split('=').next().unwrap();
            assert!(
                error.starts_with(&format!("{name} is for --transport dpdk")),
                "{error}"
            );
        }

        // The defaults alone are a plain kernel run: it gets as far as the
        // key.
        let cli = Cli::try_parse_from(["echo-client", "--key", "/nonexistent/k.pem"]).unwrap();
        let error = run(cli).unwrap_err().to_string();
        assert!(!error.contains("--transport dpdk"), "{error}");
    }
}
