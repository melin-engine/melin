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

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use clap::Parser;
use melin_client::{Connection, Frame, key};

use echo_server::{MAX_PAYLOAD, TAG_ECHO, TAG_RESP_ECHO, TAG_RESP_REJECTED};

type Error = Box<dyn std::error::Error>;

// ---------------------------------------------------------------------------
// Command line
// ---------------------------------------------------------------------------

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
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
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
    let key = key::load_signing_key(&cli.key)?;
    let mut node = Connection::connect(cli.server, &key)?;

    // `u64` nanoseconds: what `Duration` converts to losslessly for any
    // trip shorter than centuries, and what sorts without a comparator.
    let mut samples: Vec<u64> = Vec::with_capacity(cli.count);
    let mut payload = vec![0u8; cli.size];
    for i in 1..=cli.count as u64 {
        fill(&mut payload, i);
        samples.push(echo(&mut node, i, &payload)?.as_nanos() as u64);
    }

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

/// One round trip: send `payload`, check the echo is it, and return how
/// long the echo took to arrive.
///
/// The clock stops at the reply frame, not at the batch end that follows
/// it — the reply is what the caller waited for — which is why this
/// reads frame by frame instead of asking the client for the batch.
fn echo(node: &mut Connection, seq: u64, payload: &[u8]) -> Result<Duration, Error> {
    let started = Instant::now();
    node.send(seq, TAG_ECHO, payload)?;
    let elapsed = match node.next_frame()? {
        Frame::Response(reply) => {
            let elapsed = started.elapsed();
            if reply.first() == Some(&TAG_RESP_REJECTED) {
                return Err("the server rejected the request".into());
            }
            if reply.first() != Some(&TAG_RESP_ECHO) || reply[1..] != payload[..] {
                return Err(format!("the reply to request {seq} is not what was sent").into());
            }
            elapsed
        }
        Frame::BatchEnd => return Err("the server ended the batch without a reply".into()),
        Frame::ServerBusy => return Err(melin_client::Error::ServerBusy.into()),
        Frame::EngineError => return Err(melin_client::Error::EngineError.into()),
    };
    match node.next_frame()? {
        Frame::BatchEnd => Ok(elapsed),
        other => Err(format!("expected the batch to end, got {other:?}").into()),
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

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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
}
