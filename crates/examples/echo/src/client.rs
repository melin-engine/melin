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
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

use base64::Engine;
use clap::Parser;
use ed25519_dalek::{Signer, SigningKey};
use melin_wire_protocol::control_codec::{
    TAG_AUTH_FAILED, TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_ENGINE_ERROR,
    TAG_RESPONSE_HEARTBEAT, TAG_SERVER_BUSY, TAG_SERVER_READY,
};

use echo_server::{MAX_PAYLOAD, TAG_ECHO, TAG_RESP_ECHO, TAG_RESP_REJECTED};

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
// Keys
// ---------------------------------------------------------------------------

/// The DER prefix `openssl genpkey -algorithm ed25519` puts in front of the
/// 32-byte seed: a PKCS#8 v1 `PrivateKeyInfo` for the Ed25519 OID
/// (1.3.101.112) with the seed wrapped in a `CurvePrivateKey` OCTET
/// STRING. Ed25519 has no parameters, so the encoding is fixed and the
/// whole key is this prefix plus the seed — no ASN.1 parser needed.
const PKCS8_ED25519_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// Load a signing key from a raw 32-byte seed (the runtime's own
/// convention, e.g. `--replication-key`) or a PKCS#8 PEM.
fn load_key(path: &Path) -> Result<SigningKey, Error> {
    let bytes =
        std::fs::read(path).map_err(|e| format!("cannot read key {}: {e}", path.display()))?;
    let seed = seed_from(&bytes).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(SigningKey::from_bytes(&seed))
}

fn seed_from(bytes: &[u8]) -> Result<[u8; 32], String> {
    if let Ok(seed) = <[u8; 32]>::try_from(bytes) {
        return Ok(seed);
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| "not a 32-byte seed, and not PEM text either".to_string())?;
    let body: String = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("-----"))
        .collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| format!("not a 32-byte seed, and PEM body is not base64: {e}"))?;
    der.strip_prefix(&PKCS8_ED25519_PREFIX)
        .and_then(|seed| <[u8; 32]>::try_from(seed).ok())
        .ok_or_else(|| "PEM is not an unencrypted PKCS#8 Ed25519 private key".to_string())
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

    // --- Keys ---

    /// As written by `openssl genpkey -algorithm ed25519`, with the seed
    /// and public key that `openssl pkey` reports for it.
    const OPENSSL_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
        MC4CAQAwBQYDK2VwBCIEIDclw/zwdZEQraidYISn+CjytFLopT9cneV0G7+MvdtR\n\
        -----END PRIVATE KEY-----\n";
    const OPENSSL_SEED: [u8; 32] = [
        0x37, 0x25, 0xc3, 0xfc, 0xf0, 0x75, 0x91, 0x10, 0xad, 0xa8, 0x9d, 0x60, 0x84, 0xa7, 0xf8,
        0x28, 0xf2, 0xb4, 0x52, 0xe8, 0xa5, 0x3f, 0x5c, 0x9d, 0xe5, 0x74, 0x1b, 0xbf, 0x8c, 0xbd,
        0xdb, 0x51,
    ];
    const OPENSSL_PUBKEY_B64: &str = "+tVsQuDHgy200knb+jTv5Zs6XAr4eV5crZS0j/578Ac=";

    #[test]
    fn a_pem_from_openssl_loads_as_the_key_openssl_derives() {
        let seed = seed_from(OPENSSL_PEM.as_bytes()).unwrap();
        assert_eq!(seed, OPENSSL_SEED);
        let key = SigningKey::from_bytes(&seed);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes()),
            OPENSSL_PUBKEY_B64,
            "the public key must match what the README's authorized_keys recipe produces"
        );
    }

    #[test]
    fn a_raw_seed_loads_as_is() {
        let seed = [0xAA; 32];
        assert_eq!(seed_from(&seed).unwrap(), seed);
    }

    #[test]
    fn other_key_material_is_refused() {
        assert!(seed_from(&[0u8; 31]).is_err(), "short seed");
        assert!(seed_from(&[0u8; 33]).is_err(), "long seed, not text");
        assert!(seed_from(b"not a key at all").is_err(), "text, not base64");
        // Valid base64 but not a PKCS#8 Ed25519 key.
        let rsa_ish = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n";
        assert!(seed_from(rsa_ish.as_bytes()).is_err(), "wrong DER prefix");
        // Right prefix, but the seed is one byte short.
        let short = base64::engine::general_purpose::STANDARD
            .encode([&PKCS8_ED25519_PREFIX[..], &[0u8; 31]].concat());
        assert!(seed_from(short.as_bytes()).is_err(), "truncated seed");
    }
}
