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
//! ```sh
//! echo-client --key /tmp/echo-key.pem --size 288 --count 10000
//! ```

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use base64::Engine;
use clap::Parser;
use ed25519_dalek::Signer;
use melin_wire_protocol::control_codec::{
    TAG_AUTH_FAILED, TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_ENGINE_ERROR,
    TAG_RESPONSE_HEARTBEAT, TAG_SERVER_BUSY, TAG_SERVER_READY,
};

use echo_server::{MAX_PAYLOAD, TAG_ECHO, TAG_RESP_ECHO, TAG_RESP_REJECTED};

// Key loading is shared with `echo-bench`, which sends the same wire.
mod keys;
use keys::load_key;

type Error = Box<dyn std::error::Error>;

/// How long to wait for any single frame from the server (and for the
/// connection itself). Generous for a local round trip; short enough that
/// a request the server silently dropped (see [`request`]) turns into an
/// error rather than a hang.
const READ_TIMEOUT: Duration = Duration::from_secs(5);

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
    let mut stream = connect(&cli)?;
    // `u64` nanoseconds: what `Duration` converts to losslessly for any
    // trip shorter than centuries, and what sorts without a comparator.
    let mut samples: Vec<u64> = Vec::with_capacity(cli.count);
    let mut payload = vec![0u8; cli.size];
    for i in 1..=cli.count as u64 {
        fill(&mut payload, i);
        let (reply, elapsed) = request(&mut stream, i, &payload)?;
        if reply.first() != Some(&TAG_RESP_ECHO) || reply[1..] != payload[..] {
            return Err(format!("the reply to request {i} is not what was sent").into());
        }
        samples.push(elapsed.as_nanos() as u64);
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
// Wire
// ---------------------------------------------------------------------------

/// Connect and authenticate: read the challenge, sign its nonce, send the
/// signature with the public key, and wait for the server to say it is
/// ready.
fn connect(cli: &Cli) -> Result<TcpStream, Error> {
    let key = load_key(&cli.key)?;
    let mut stream = TcpStream::connect_timeout(&cli.server, READ_TIMEOUT)
        .map_err(|e| format!("cannot connect to {}: {e}", cli.server))?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    stream.set_nodelay(true)?;

    let challenge = read_frame(&mut stream)?;
    // `[tag][nonce: 32]`
    if challenge.first() != Some(&TAG_CHALLENGE) || challenge.len() != 33 {
        return Err("expected an auth challenge from the server".into());
    }
    let signature = key.sign(&challenge[1..]);

    // `[seq: u64][tag][signature: 64][public key: 32]`
    let mut response = Vec::with_capacity(8 + 1 + 64 + 32);
    response.extend_from_slice(&0u64.to_le_bytes());
    response.push(TAG_CHALLENGE_RESPONSE);
    response.extend_from_slice(&signature.to_bytes());
    response.extend_from_slice(&key.verifying_key().to_bytes());
    write_frame(&mut stream, &response)?;

    match read_frame(&mut stream)?.first() {
        Some(&TAG_SERVER_READY) => Ok(stream),
        Some(&TAG_AUTH_FAILED) => Err(format!(
            "authentication failed: is {} listed in the server's authorized_keys?",
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes())
        )
        .into()),
        other => Err(format!("unexpected reply to authentication: {other:?}").into()),
    }
}

/// Send one request and return its one domain response (tag included,
/// length prefix stripped) with the time it took to arrive, then drain the
/// batch it came in.
///
/// The clock stops at the response frame, not at the batch end that
/// follows it: the reply is what the caller waited for.
fn request(stream: &mut TcpStream, seq: u64, body: &[u8]) -> Result<(Vec<u8>, Duration), Error> {
    // `[request_seq: u64][tag][body]`
    let mut frame = Vec::with_capacity(8 + 1 + body.len());
    frame.extend_from_slice(&seq.to_le_bytes());
    frame.push(TAG_ECHO);
    frame.extend_from_slice(body);
    let started = Instant::now();
    write_frame(stream, &frame)?;

    let mut response = None;
    loop {
        let frame = read_frame(stream).map_err(|e| match e.kind() {
            // The server does not answer a request it refuses — a key
            // whose role may not write, or a payload past the cap — it
            // drops it and keeps the connection. Silence is the only
            // signal.
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => format!(
                "no reply within {}s: the server silently drops requests it refuses — \
                 check that the key's role in authorized_keys may write \
                 (operator, trader or custodian)",
                READ_TIMEOUT.as_secs()
            ),
            _ => format!("connection lost: {e}"),
        })?;
        match frame.first() {
            Some(&TAG_BATCH_END) => break,
            Some(&TAG_RESPONSE_HEARTBEAT) => {}
            Some(&TAG_SERVER_BUSY) => return Err("the server is busy, retry later".into()),
            Some(&TAG_ENGINE_ERROR) => return Err("the server reported an engine error".into()),
            Some(&TAG_RESP_REJECTED) => return Err("the server rejected the request".into()),
            Some(_) if response.is_none() => response = Some((frame, started.elapsed())),
            _ => return Err("unexpected frame from the server".into()),
        }
    }
    response.ok_or_else(|| "the server ended the batch without a response".into())
}

/// Frames are `[len: u32 LE][payload]`. Anything larger than this is not
/// an echo frame, so the length is refused before it drives an allocation.
const MAX_FRAME_LEN: usize = 4096;

fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len > MAX_FRAME_LEN {
        return Err(io::Error::other(format!(
            "frame of {len} bytes is not plausible"
        )));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> io::Result<()> {
    stream.write_all(&(payload.len() as u32).to_le_bytes())?;
    stream.write_all(payload)?;
    stream.flush()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Samples ---

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
