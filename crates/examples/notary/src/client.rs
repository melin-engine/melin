#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Command-line client for the notary example.
//!
//! Three commands, matching how a notary is actually used:
//!
//! - `notarize FILE` hashes the file with BLAKE3, submits the digest, and
//!   saves the receipt next to it. The file itself never leaves the
//!   machine — the server only ever sees 32 bytes.
//! - `verify FILE` checks the file against its receipt. This is offline:
//!   no key, no server, no other receipts. A receipt is a complete link of
//!   the chain, so `BLAKE3(prev ‖ digest ‖ time) == head` is the whole
//!   check, and anyone handed the file and the receipt can run it.
//! - `head` asks the server for the current commitment and entry count.
//!
//! The receipt is a small text file (`entry`, `time_ns`, `leaf`, `prev`,
//! `head`) so it can be read, mailed and diffed without tooling.
//!
//! ```sh
//! notary-client notarize contract.pdf --key /tmp/notary-key.pem
//! notary-client verify contract.pdf            # exit 0: attested, 1: mismatch
//! notary-client head --key /tmp/notary-key.pem
//! ```

use std::ffi::OsString;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use base64::Engine;
use clap::{Args, Parser, Subcommand};
use ed25519_dalek::{Signer, SigningKey};
use melin_wire_protocol::control_codec::{
    TAG_AUTH_FAILED, TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_ENGINE_ERROR,
    TAG_RESPONSE_HEARTBEAT, TAG_SERVER_BUSY, TAG_SERVER_READY,
};
use notary_server::receipt::{Receipt, hex};
use notary_server::{
    HEAD_LEN, LEAF_LEN, TAG_GET_HEAD, TAG_NOTARIZE, TAG_RESP_HEAD, TAG_RESP_REJECTED,
};

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
#[command(name = "notary-client", about = "Notarize files and verify receipts")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Hash FILE, submit the digest, and save the receipt.
    Notarize {
        file: PathBuf,
        /// Where to write the receipt. Defaults to `FILE.receipt`.
        #[arg(long)]
        receipt: Option<PathBuf>,
        #[command(flatten)]
        connection: Connection,
    },
    /// Check FILE against its receipt. Offline: needs neither key nor server.
    Verify {
        file: PathBuf,
        /// The receipt to check against. Defaults to `FILE.receipt`.
        #[arg(long)]
        receipt: Option<PathBuf>,
    },
    /// Print the chain head and entry count.
    Head {
        #[command(flatten)]
        connection: Connection,
    },
}

#[derive(Args)]
struct Connection {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:9876")]
    server: SocketAddr,
    /// Ed25519 private key: PEM as written by `openssl genpkey -algorithm
    /// ed25519`, or a raw 32-byte seed.
    #[arg(long)]
    key: PathBuf,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<ExitCode, Error> {
    match cli.command {
        Command::Notarize {
            file,
            receipt,
            connection,
        } => {
            let receipt_path = receipt.unwrap_or_else(|| default_receipt_path(&file));
            let receipt = notarize(&file, &connection)?;
            std::fs::write(&receipt_path, receipt.to_text())
                .map_err(|e| format!("cannot write {}: {e}", receipt_path.display()))?;
            println!("notarized {}", file.display());
            println!("  entry: {}", receipt.entry);
            println!(
                "  time:  {} ({} ns)",
                format_utc(receipt.timestamp_ns),
                receipt.timestamp_ns
            );
            println!("  head:  {}", hex(&receipt.head));
            println!("receipt written to {}", receipt_path.display());
            Ok(ExitCode::SUCCESS)
        }
        Command::Verify { file, receipt } => {
            let receipt_path = receipt.unwrap_or_else(|| default_receipt_path(&file));
            let text = std::fs::read_to_string(&receipt_path)
                .map_err(|e| format!("cannot read {}: {e}", receipt_path.display()))?;
            let receipt = Receipt::from_text(&text)
                .map_err(|e| format!("{}: {e}", receipt_path.display()))?;
            match verify(&file, &receipt)? {
                Ok(()) => {
                    println!(
                        "OK: {} is entry {}, sequenced at {}",
                        file.display(),
                        receipt.entry,
                        format_utc(receipt.timestamp_ns)
                    );
                    Ok(ExitCode::SUCCESS)
                }
                Err(why) => {
                    println!("MISMATCH: {why}");
                    Ok(ExitCode::from(1))
                }
            }
        }
        Command::Head { connection } => {
            let (entries, head) = query_head(&connection)?;
            println!("entries: {entries}");
            println!("head:    {}", hex(&head));
            Ok(ExitCode::SUCCESS)
        }
    }
}

/// `FILE.receipt`, appended rather than substituted so `contract.pdf`
/// and `contract.txt` do not share a receipt.
fn default_receipt_path(file: &Path) -> PathBuf {
    let mut path: OsString = file.into();
    path.push(".receipt");
    path.into()
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn notarize(file: &Path, connection: &Connection) -> Result<Receipt, Error> {
    let leaf = digest_file(file)?;
    let mut stream = connect(connection)?;
    let response = request(&mut stream, TAG_NOTARIZE, &leaf)?;
    let receipt = Receipt::from_frame(&response, leaf)?;
    // The server is not trusted blindly: a receipt that does not fold is
    // worthless, and better refused now than discovered at verification.
    if !receipt.verifies() {
        return Err("the server returned a receipt that does not verify".into());
    }
    Ok(receipt)
}

/// `Ok(Ok(()))` when the file is what the receipt attests to; the inner
/// `Err` names the first thing that does not match.
fn verify(file: &Path, receipt: &Receipt) -> Result<Result<(), String>, Error> {
    let leaf = digest_file(file)?;
    if leaf != receipt.leaf {
        return Ok(Err(format!(
            "{} does not hash to the receipt's leaf: the file has changed",
            file.display()
        )));
    }
    if !receipt.verifies() {
        return Ok(Err(
            "the receipt does not fold: BLAKE3(prev || leaf || time) != head".to_string(),
        ));
    }
    Ok(Ok(()))
}

fn query_head(connection: &Connection) -> Result<(u64, [u8; HEAD_LEN]), Error> {
    let mut stream = connect(connection)?;
    let response = request(&mut stream, TAG_GET_HEAD, &[])?;
    // `[tag][entries: u64][head: 32]`
    if response.first() != Some(&TAG_RESP_HEAD) || response.len() != 1 + 8 + HEAD_LEN {
        return Err(format!("unexpected response to a head query: {}", hex(&response)).into());
    }
    Ok((
        u64::from_le_bytes(response[1..9].try_into()?),
        response[9..].try_into()?,
    ))
}

/// BLAKE3 of the file's contents, streamed so a large file is not read
/// into memory.
fn digest_file(path: &Path) -> Result<[u8; LEAF_LEN], Error> {
    let mut file =
        std::fs::File::open(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    io::copy(&mut file, &mut hasher)?;
    Ok(*hasher.finalize().as_bytes())
}

// ---------------------------------------------------------------------------
// Wire
// ---------------------------------------------------------------------------

/// Connect and authenticate: read the challenge, sign its nonce, send the
/// signature with the public key, and wait for the server to say it is
/// ready.
fn connect(connection: &Connection) -> Result<TcpStream, Error> {
    let key = load_key(&connection.key)?;
    let mut stream = TcpStream::connect_timeout(&connection.server, READ_TIMEOUT)
        .map_err(|e| format!("cannot connect to {}: {e}", connection.server))?;
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
/// length prefix stripped), draining the batch it arrives in.
fn request(stream: &mut TcpStream, tag: u8, body: &[u8]) -> Result<Vec<u8>, Error> {
    // `[request_seq: u64][tag][body]`. The sequence is 1 rather than a
    // counter: the example accepts every request (see `check_request_seq`
    // in the library) and this client sends one per connection.
    let mut frame = Vec::with_capacity(8 + 1 + body.len());
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.push(tag);
    frame.extend_from_slice(body);
    write_frame(stream, &frame)?;

    let mut response = None;
    loop {
        let frame = read_frame(stream).map_err(|e| match e.kind() {
            // The server does not answer a request it refuses — a key
            // whose role may not write, or a malformed frame — it drops
            // it and keeps the connection. Silence is the only signal.
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
            Some(_) if response.is_none() => response = Some(frame),
            _ => return Err(format!("unexpected frame from the server: {}", hex(&frame)).into()),
        }
    }
    response.ok_or_else(|| "the server ended the batch without a response".into())
}

/// Frames are `[len: u32 LE][payload]`. Anything larger than this is not
/// a notary frame, so the length is refused before it drives an allocation.
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
// Formatting
// ---------------------------------------------------------------------------

/// `YYYY-MM-DDThh:mm:ss.nnnnnnnnnZ` for a time in nanoseconds since the
/// Unix epoch. Hand-rolled (Howard Hinnant's `civil_from_days`) rather
/// than pulling a date crate into an example for one line of output.
fn format_utc(ns: u64) -> String {
    let secs = ns / 1_000_000_000;
    let nanos = ns % 1_000_000_000;
    let (days, of_day) = (secs / 86_400, secs % 86_400);

    // Shift the epoch to 0000-03-01 so leap days fall at the end of a
    // year and each 400-year era has a fixed 146,097 days. `i64`: the
    // algorithm's intermediate terms are signed, and u64 nanoseconds only
    // reach the year 2554 so nothing here can overflow.
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);

    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{nanos:09}Z",
        of_day / 3600,
        (of_day % 3600) / 60,
        of_day % 60
    )
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Keys ---

    /// As written by `openssl genpkey -algorithm ed25519`, with the seed
    /// and public key that `openssl pkey` reports for it.
    const OPENSSL_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
        MC4CAQAwBQYDK2VwBCIEIDclw/zwdZEQraidYISn+CjytFLopT9cneV0G7+MvdtR\n\
        -----END PRIVATE KEY-----\n";
    const OPENSSL_SEED: &str = "3725c3fcf0759110ada89d6084a7f828f2b452e8a53f5c9de5741bbf8cbddb51";
    const OPENSSL_PUBKEY_B64: &str = "+tVsQuDHgy200knb+jTv5Zs6XAr4eV5crZS0j/578Ac=";

    #[test]
    fn a_pem_from_openssl_loads_as_the_key_openssl_derives() {
        let seed = seed_from(OPENSSL_PEM.as_bytes()).unwrap();
        assert_eq!(hex(&seed), OPENSSL_SEED);
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

    // --- Formatting ---

    #[test]
    fn utc_formatting() {
        assert_eq!(format_utc(0), "1970-01-01T00:00:00.000000000Z");
        assert_eq!(
            format_utc(1_700_000_000_000_000_000),
            "2023-11-14T22:13:20.000000000Z"
        );
        // A leap day, and the day after it.
        assert_eq!(
            format_utc(951_782_400_000_000_000),
            "2000-02-29T00:00:00.000000000Z"
        );
        assert_eq!(
            format_utc(951_868_799_999_999_999),
            "2000-02-29T23:59:59.999999999Z"
        );
        assert_eq!(
            format_utc(951_868_800_000_000_000),
            "2000-03-01T00:00:00.000000000Z"
        );
    }

    #[test]
    fn default_receipt_path_appends_rather_than_replaces() {
        assert_eq!(
            default_receipt_path(Path::new("dir/contract.pdf")),
            PathBuf::from("dir/contract.pdf.receipt")
        );
    }
}
