//! Server orchestrator — binds the accept loop, pipeline threads, and reader.
//!
//! On startup:
//! 1. Recovers or creates the `JournaledExchange`.
//! 2. Decomposes it into `(Exchange, JournalWriter)` via `into_parts()`.
//! 3. Builds the disruptor pipeline (input ring buffer + output SPSC).
//! 4. Spawns 4 OS threads: reader (epoll), journal, matching, response.
//! 5. Runs the accept loop, registering connections with the epoll reader.
//!
//! Fully synchronous — no async runtime needed. The single reader thread
//! uses epoll to multiplex all connections, eliminating thread oversubscription.
//! The response thread writes directly to sockets.

use std::net::SocketAddr;
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::{debug, error, info};

use trading_engine::journal::JournaledExchange;
use trading_engine::journal::pipeline::build_pipeline;

use trading_protocol::auth::{AuthorizedKeys, Permission};
use trading_protocol::message::ConnectionId;
use trading_protocol::transport::BlockingTransportListener;

#[cfg(not(feature = "io-uring"))]
use trading_protocol::blocking::BlockingFrameWriter;

#[cfg(not(feature = "io-uring"))]
use crate::reader::{self, ReaderRegistration};
#[cfg(not(feature = "io-uring"))]
use crate::response::ControlEvent;

#[cfg(feature = "io-uring")]
use crate::uring_reader::{self as reader, ReaderRegistration};
#[cfg(feature = "io-uring")]
use crate::uring_response::ControlEvent;

/// Server configuration, parsed from CLI arguments via clap.
#[derive(clap::Parser)]
#[command(name = "trading-server", about = "Low-latency matching engine server")]
pub struct ServerConfig {
    /// Address to bind the TCP listener.
    #[arg(long, default_value = "127.0.0.1:9876")]
    pub bind: SocketAddr,
    /// Path to the journal file for durable event sourcing.
    #[arg(long, default_value = "trading.journal")]
    pub journal: PathBuf,
    /// Path to a snapshot file for faster recovery.
    #[arg(long)]
    pub snapshot: Option<PathBuf>,
    /// Pipeline core IDs: journal,matching,response (comma-separated).
    /// Core 0 is reserved for OS/IRQ handling.
    #[arg(long, default_value = "1,2,3", value_parser = parse_cores)]
    pub cores: [usize; 3],
    /// Number of epoll reader threads.
    #[arg(long, default_value_t = 2)]
    pub readers: usize,
    /// First CPU core for reader thread pinning. Reader thread i is
    /// pinned to reader_cores + i.
    #[arg(long, default_value_t = 4)]
    pub reader_cores: usize,
    /// Group commit coalescing delay in microseconds. Keep at 0 for TCP.
    #[arg(long, default_value_t = 0)]
    pub group_commit_us: u64,
    /// Heartbeat interval in seconds. The server sends a heartbeat to idle
    /// connections after this many seconds of silence. Set to 0 to disable.
    #[arg(long, default_value_t = 10)]
    pub heartbeat_interval_secs: u64,
    /// Connection timeout in seconds. The server disconnects clients that
    /// have not sent any data within this window. Set to 0 to disable.
    #[arg(long, default_value_t = 30)]
    pub connection_timeout_secs: u64,
    /// Number of test accounts to seed on first startup.
    #[arg(long, default_value_t = 2)]
    pub accounts: u32,
    /// Number of test instruments to seed on first startup.
    #[arg(long, default_value_t = 2)]
    pub instruments: u32,
    /// Path to the authorized keys file for Ed25519 challenge-response
    /// authentication. Every connection must authenticate before trading.
    /// See `AuthorizedKeys` for file format.
    #[arg(long)]
    pub authorized_keys: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:9876".parse().expect("valid default addr"),
            journal: PathBuf::from("trading.journal"),
            snapshot: None,
            cores: [1, 2, 3],
            readers: 2,
            reader_cores: 4,
            group_commit_us: 0,
            heartbeat_interval_secs: 10,
            connection_timeout_secs: 30,
            accounts: 2,
            instruments: 2,
            authorized_keys: PathBuf::from("authorized_keys"),
        }
    }
}

impl ServerConfig {
    /// Group commit delay as a Duration.
    pub fn group_commit_delay(&self) -> std::time::Duration {
        std::time::Duration::from_micros(self.group_commit_us)
    }

    /// Heartbeat interval as a Duration. Returns `None` if disabled (0).
    pub fn heartbeat_interval(&self) -> Option<std::time::Duration> {
        if self.heartbeat_interval_secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(self.heartbeat_interval_secs))
        }
    }

    /// Connection timeout as a Duration. Returns `None` if disabled (0).
    pub fn connection_timeout(&self) -> Option<std::time::Duration> {
        if self.connection_timeout_secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(self.connection_timeout_secs))
        }
    }
}

/// Parse "j,m,r" into [usize; 3] for pipeline core affinity.
fn parse_cores(s: &str) -> Result<[usize; 3], String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 3 {
        return Err(format!(
            "expected 3 comma-separated core IDs, got {}",
            parts.len()
        ));
    }
    let mut cores = [0usize; 3];
    for (i, p) in parts.iter().enumerate() {
        cores[i] = p.parse().map_err(|_| format!("invalid core ID: {p}"))?;
    }
    Ok(cores)
}

/// Run the trading server.
///
/// 1. Initializes (or recovers) the `JournaledExchange`, then decomposes
///    it into `Exchange` and `JournalWriter` for the pipeline.
/// 2. Builds the disruptor pipeline (input ring + output SPSC + stages).
/// 3. Spawns 3 OS threads: journal, matching, response.
/// 4. Runs the accept loop, spawning a reader OS thread per connection.
///
/// Returns when the listener encounters a fatal error.
pub fn run<L: BlockingTransportListener>(
    listener: L,
    config: ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    run_with_shutdown(listener, config, Arc::new(AtomicBool::new(false)))
}

/// Run the trading server with an externally controlled shutdown flag.
///
/// Same as [`run`], but the caller can set `shutdown` to `true` to trigger
/// a clean shutdown of all pipeline threads (useful for benchmarks that need
/// to collect latency trace reports).
pub fn run_with_shutdown<L: BlockingTransportListener>(
    mut listener: L,
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Load authorized keys for challenge-response authentication.
    let authorized_keys = Arc::new(AuthorizedKeys::load(&config.authorized_keys)?);
    info!(
        keys = authorized_keys.len(),
        path = %config.authorized_keys.display(),
        "loaded authorized keys"
    );

    // Initialize or recover the exchange.
    let engine = init_engine(&config)?;

    // Decompose into parts for the pipeline.
    let (mut exchange, writer) = engine.into_parts();

    // Pre-fault all HashMap pages so page faults happen now, not on the hot path.
    exchange.prefault();

    // Build the disruptor pipeline.
    let (input_producer, journal_stage, matching_stage, output_consumer, journal_cursor) =
        build_pipeline(exchange, writer, config.group_commit_delay());

    // Control channel for connect/disconnect events → response stage.
    let (control_tx, control_rx) = std::sync::mpsc::channel();

    // Spawn the epoll reader thread pool. Connections are distributed
    // round-robin across reader threads. Each thread uses epoll to
    // multiplex its connections and MultiProducer to publish to the
    // disruptor. With 2 readers (cores 4-5) + 3 pipeline (cores 1-3) =
    // 5 pinned OS threads, no oversubscription even with hundreds of connections.
    let connection_timeout = config.connection_timeout();
    let heartbeat_interval = config.heartbeat_interval();

    let reader_shutdown = Arc::new(AtomicBool::new(false));
    let mut reader_handle = reader::spawn_reader_pool(
        config.readers,
        input_producer,
        control_tx.clone(),
        config.reader_cores,
        connection_timeout,
        Arc::clone(&reader_shutdown),
    );

    // Spawn pipeline OS threads.
    let cores = config.cores;

    let s1 = Arc::clone(&shutdown);
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || {
            apply_affinity("journal", cores[0]);
            journal_stage.run(&s1)
        })
        .expect("failed to spawn journal thread");

    let s2 = Arc::clone(&shutdown);
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            apply_affinity("matching", cores[1]);
            matching_stage.run(&s2)
        })
        .expect("failed to spawn matching thread");

    let s3 = Arc::clone(&shutdown);
    let response_handle = std::thread::Builder::new()
        .name("response".into())
        .spawn(move || {
            apply_affinity("response", cores[2]);
            #[cfg(not(feature = "io-uring"))]
            crate::response::run(
                output_consumer,
                control_rx,
                journal_cursor,
                &s3,
                heartbeat_interval,
            );
            #[cfg(feature = "io-uring")]
            crate::uring_response::run(
                output_consumer,
                control_rx,
                journal_cursor,
                &s3,
                heartbeat_interval,
            );
        })
        .expect("failed to spawn response thread");

    // Set the listener to non-blocking so accept() returns immediately
    // with WouldBlock when no connection is pending. This lets the accept
    // loop check the shutdown flag without blocking indefinitely.
    // Rust's std TcpListener retries on EINTR, so signals alone can't
    // interrupt a blocking accept().
    listener.set_nonblocking(true);

    info!(addr = %config.bind, "listening");

    // Monotonically increasing connection ID counter. AtomicU64 because
    // the accept loop is the only writer, but using atomic for future
    // flexibility (e.g., multiple listeners).
    let next_connection_id = AtomicU64::new(1);

    // Accept loop — non-blocking with 100ms sleep on WouldBlock. Each
    // accepted connection is registered with the reader thread (no
    // per-connection threads).
    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("shutdown signal received");
            break;
        }

        let (mut std_read, mut std_write, addr) = match listener.accept() {
            Ok(conn) => conn,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    // No pending connection — sleep briefly then retry.
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                }
                error!(error = %e, "accept error");
                continue;
            }
        };

        let connection_id = ConnectionId(next_connection_id.fetch_add(1, Ordering::Relaxed));

        debug!(connection_id = connection_id.0, addr = %addr, "new connection");

        // Set a 5-second read timeout for the auth handshake to prevent
        // slow/malicious clients from blocking the accept loop.
        if let Err(e) = set_read_timeout(&std_read, Some(std::time::Duration::from_secs(5))) {
            debug!(connection_id = connection_id.0, error = %e, "failed to set auth timeout");
        }

        // Challenge-response authentication handshake (cold path).
        // 1. Send Challenge with random nonce
        // 2. Read ChallengeResponse (signature + public key)
        // 3. Verify signature and look up key in authorized_keys
        // 4. Send ServerReady on success, AuthFailed on failure
        let permission = match authenticate_connection(
            connection_id,
            addr,
            &mut std_read,
            &mut std_write,
            &authorized_keys,
        ) {
            Ok(perm) => perm,
            Err(e) => {
                debug!(connection_id = connection_id.0, addr = %addr, error = %e, "auth failed, dropping");
                continue;
            }
        };

        // Clear the read timeout before handing to the epoll reader.
        // Epoll uses non-blocking I/O, so the timeout is irrelevant, but
        // clearing it avoids surprising behavior if the fd is ever used
        // in blocking mode again.
        if let Err(e) = set_read_timeout(&std_read, None) {
            debug!(connection_id = connection_id.0, error = %e, "failed to clear auth timeout");
        }

        // Set a write timeout on the response socket so a slow/stalled
        // client cannot block the response thread (SEC-01). If a write
        // takes longer than this, it returns EAGAIN and the response
        // stage drops the connection.
        if let Err(e) = set_write_timeout(&std_write, Some(std::time::Duration::from_secs(5))) {
            debug!(connection_id = connection_id.0, error = %e, "failed to set write timeout");
        }

        // Register the writer with the response thread before the reader.
        // This ensures the response stage has the writer before any
        // requests arrive from this connection.
        #[cfg(not(feature = "io-uring"))]
        let control_event = {
            let boxed_writer: Box<dyn std::io::Write + Send> = Box::new(std_write);
            ControlEvent::Connected {
                connection_id: connection_id.0,
                writer: BlockingFrameWriter::new(boxed_writer),
            }
        };
        #[cfg(feature = "io-uring")]
        let control_event = {
            let fd = std_write.as_raw_fd();
            let owner: Box<dyn Send> = Box::new(std_write);
            ControlEvent::Connected {
                connection_id: connection_id.0,
                fd,
                _owner: owner,
            }
        };
        if control_tx.send(control_event).is_err() {
            info!("response thread gone, shutting down");
            break;
        }

        // Register the reader fd with the epoll reader thread.
        reader_handle.register(ReaderRegistration {
            connection_id,
            reader: std_read,
            addr,
            permission,
        });
    }

    // --- Ordered shutdown sequence ---
    // 1. Stop readers first so no new events enter the disruptor.
    info!("shutdown: stopping reader threads");
    reader_handle.shutdown();
    reader_handle.join();

    // 2. Now signal the pipeline. The journal and matching stages will
    //    drain any remaining events before exiting.
    info!("shutdown: draining pipeline");
    shutdown.store(true, Ordering::Relaxed);

    let _ = journal_handle.join();
    let _ = matching_handle.join();
    let _ = response_handle.join();

    info!("shutdown complete");
    Ok(())
}

/// Initialize or recover the JournaledExchange from disk.
fn init_engine(config: &ServerConfig) -> Result<JournaledExchange, Box<dyn std::error::Error>> {
    if let Some(ref snap_path) = config.snapshot
        && snap_path.exists()
        && config.journal.exists()
    {
        info!("recovering from snapshot + journal");
        let engine = JournaledExchange::recover_from_snapshot(snap_path, &config.journal)?;
        return Ok(engine);
    }

    if config.journal.exists() {
        info!("recovering from journal");
        let engine = JournaledExchange::recover(&config.journal)?;
        Ok(engine)
    } else {
        info!("creating new journal");
        let mut engine = JournaledExchange::create(&config.journal)?;
        seed_test_data(&mut engine, config.accounts, config.instruments)?;
        Ok(engine)
    }
}

/// Apply CPU core affinity for a pipeline thread, logging the result.
fn apply_affinity(thread_name: &str, core_id: usize) {
    match crate::affinity::pin_to_core(core_id) {
        Ok(c) => info!(core = c, thread = thread_name, "pinned to core"),
        Err(e) => tracing::warn!(thread = thread_name, error = e, "core pinning failed"),
    }
}

/// Seed the exchange with test instruments and accounts so the TUI can
/// be used immediately. This runs only on first startup (fresh journal).
fn seed_test_data(
    engine: &mut JournaledExchange,
    num_accounts: u32,
    num_instruments: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    use trading_engine::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};

    // Register instruments. Each instrument has a unique base/quote currency
    // pair using the same convention as the bench generator:
    // symbol i → base = CurrencyId(i*2 - 1), quote = CurrencyId(i*2).
    for i in 1..=num_instruments {
        engine.add_instrument(InstrumentSpec {
            symbol: Symbol(i),
            base: CurrencyId(i * 2 - 1),
            quote: CurrencyId(i * 2),
        })?;
    }

    // Seed accounts with generous balances in all currencies.
    for acct in 1..=num_accounts {
        for i in 1..=num_instruments {
            engine.deposit(AccountId(acct), CurrencyId(i * 2 - 1), u64::MAX / 4)?;
            engine.deposit(AccountId(acct), CurrencyId(i * 2), u64::MAX / 4)?;
        }
    }

    info!(
        accounts = num_accounts,
        instruments = num_instruments,
        "seeded test data"
    );
    Ok(())
}

/// Perform challenge-response authentication on a new connection.
///
/// Runs on the accept thread (cold path, blocking). The caller must set
/// a read timeout on the stream before calling to prevent slow clients
/// from stalling the accept loop.
///
/// Uses raw `read_exact` instead of `BufReader` to avoid over-reading
/// bytes that belong to the first post-auth request.
///
/// Returns the authenticated `Permission` on success.
fn authenticate_connection<R: std::io::Read, W: std::io::Write>(
    connection_id: ConnectionId,
    addr: SocketAddr,
    reader: &mut R,
    writer: &mut W,
    authorized_keys: &AuthorizedKeys,
) -> Result<Permission, Box<dyn std::error::Error>> {
    use std::io;

    use ed25519_dalek::{Verifier, VerifyingKey};
    use trading_protocol::codec;
    use trading_protocol::message::{Request, ResponseKind};

    // Generate a 32-byte random nonce for this connection.
    let mut nonce = [0u8; 32];
    rand::fill(&mut nonce);

    // Send Challenge.
    let mut buf = [0u8; 64];
    let written = codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf)
        .map_err(|e| io::Error::other(format!("encode Challenge: {e}")))?;
    writer.write_all(&buf[..written])?;
    writer.flush()?;

    // Read ChallengeResponse frame directly (no BufReader). Using raw
    // read_exact avoids BufReader over-reading bytes that belong to the
    // first post-auth request — those bytes would be lost when the
    // BufReader is dropped and the fd moves to the epoll reader.
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .map_err(|e| io::Error::other(format!("read auth frame length: {e}")))?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    // ChallengeResponse is 1 (tag) + 64 (signature) + 32 (public key) = 97 bytes.
    if frame_len > 256 {
        send_auth_failed(writer);
        return Err(io::Error::other(format!("auth frame too large: {frame_len}")).into());
    }
    let mut frame_buf = [0u8; 256];
    reader
        .read_exact(&mut frame_buf[..frame_len])
        .map_err(|e| io::Error::other(format!("read auth frame payload: {e}")))?;

    let request = match codec::decode_request(&frame_buf[..frame_len]) {
        Ok(req) => req,
        Err(e) => {
            send_auth_failed(writer);
            return Err(io::Error::other(format!("decode ChallengeResponse: {e}")).into());
        }
    };

    let (signature_bytes, public_key_bytes) = match request {
        Request::ChallengeResponse {
            signature,
            public_key,
        } => (signature, public_key),
        other => {
            send_auth_failed(writer);
            return Err(format!(
                "expected ChallengeResponse, got {:?}",
                std::mem::discriminant(&other)
            )
            .into());
        }
    };

    // Look up the public key in authorized_keys.
    let permission = match authorized_keys.lookup(&public_key_bytes) {
        Some(perm) => perm,
        None => {
            send_auth_failed(writer);
            return Err("unknown public key".into());
        }
    };

    // Verify the Ed25519 signature over the nonce.
    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes).map_err(|e| {
        send_auth_failed(writer);
        io::Error::other(format!("invalid public key: {e}"))
    })?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key.verify(&nonce, &signature).map_err(|e| {
        send_auth_failed(writer);
        io::Error::other(format!("signature verification failed: {e}"))
    })?;

    // Auth succeeded — send ServerReady.
    let written = codec::encode_response(&ResponseKind::ServerReady, &mut buf)
        .map_err(|e| io::Error::other(format!("encode ServerReady: {e}")))?;
    writer.write_all(&buf[..written])?;
    writer.flush()?;

    debug!(
        connection_id = connection_id.0,
        addr = %addr,
        permission = ?permission,
        "authenticated"
    );

    Ok(permission)
}

/// Set a read timeout on a raw fd via `setsockopt(SO_RCVTIMEO)`.
///
/// Works for both TCP and UDS since both are sockets. Uses `AsRawFd`
/// to avoid requiring a concrete stream type.
fn set_read_timeout<F: std::os::unix::io::AsRawFd>(
    fd: &F,
    timeout: Option<std::time::Duration>,
) -> std::io::Result<()> {
    let tv = match timeout {
        Some(d) => libc::timeval {
            tv_sec: d.as_secs() as libc::time_t,
            tv_usec: d.subsec_micros() as libc::suseconds_t,
        },
        None => libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
    };
    let ret = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const libc::timeval as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Set `SO_SNDTIMEO` on a socket. Prevents blocking writes from stalling
/// the response thread when a client stops reading (SEC-01).
fn set_write_timeout<F: std::os::unix::io::AsRawFd>(
    fd: &F,
    timeout: Option<std::time::Duration>,
) -> std::io::Result<()> {
    let tv = match timeout {
        Some(d) => libc::timeval {
            tv_sec: d.as_secs() as libc::time_t,
            tv_usec: d.subsec_micros() as libc::suseconds_t,
        },
        None => libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
    };
    let ret = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDTIMEO,
            &tv as *const libc::timeval as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Best-effort send of AuthFailed before dropping a connection.
fn send_auth_failed(writer: &mut impl std::io::Write) {
    let mut buf = [0u8; 8];
    if let Ok(written) = trading_protocol::codec::encode_response(
        &trading_protocol::message::ResponseKind::AuthFailed,
        &mut buf,
    ) {
        let _ = writer.write_all(&buf[..written]);
        let _ = writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    use ed25519_dalek::{Signer, SigningKey};
    use trading_protocol::auth::{AuthorizedKeys, Permission};
    use trading_protocol::codec;
    use trading_protocol::message::{ConnectionId, Request, ResponseKind};

    use super::authenticate_connection;

    /// Deterministic test key.
    fn test_key() -> SigningKey {
        SigningKey::from_bytes(&[0xAA; 32])
    }

    /// Build an `AuthorizedKeys` containing the test key with the given permission.
    fn keys_with_test_key(perm: &str) -> AuthorizedKeys {
        // Use trading_protocol's base64 re-export via AuthorizedKeys::parse.
        // Encode the public key bytes as base64 manually using the simple
        // alphabet (all test keys produce valid base64).
        let pub_bytes = test_key().verifying_key().to_bytes();
        let pub_b64 = base64_encode(&pub_bytes);
        AuthorizedKeys::parse(&format!("{perm} {pub_b64} test\n")).unwrap()
    }

    /// Minimal base64 encoder for test use only. Avoids adding base64
    /// as a dev-dependency to the server crate.
    fn base64_encode(bytes: &[u8]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(ALPHABET[(n >> 18 & 0x3F) as usize] as char);
            out.push(ALPHABET[(n >> 12 & 0x3F) as usize] as char);
            if chunk.len() > 1 {
                out.push(ALPHABET[(n >> 6 & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
            if chunk.len() > 2 {
                out.push(ALPHABET[(n & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
        }
        out
    }

    /// Run `authenticate_connection` on one end of a `UnixStream::pair()`,
    /// returning the result. Maps the error to `String` so it's `Send`.
    fn run_server_auth(
        mut stream: UnixStream,
        keys: AuthorizedKeys,
    ) -> std::thread::JoinHandle<Result<Permission, String>> {
        std::thread::spawn(move || {
            // Clone the stream so we have independent read/write halves.
            let mut writer = stream.try_clone().unwrap();
            authenticate_connection(
                ConnectionId(1),
                "127.0.0.1:0".parse().unwrap(),
                &mut stream,
                &mut writer,
                &keys,
            )
            .map_err(|e| e.to_string())
        })
    }

    /// Read a Challenge frame from the client end, sign it, and write
    /// a ChallengeResponse back.
    fn client_sign_challenge(stream: &mut UnixStream, key: &SigningKey) {
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        stream.read_exact(&mut payload[..len]).unwrap();

        let resp = codec::decode_response(&payload[..len]).unwrap();
        let nonce = match resp {
            ResponseKind::Challenge { nonce } => nonce,
            other => panic!("expected Challenge, got {other:?}"),
        };

        let sig = key.sign(&nonce);
        let request = Request::ChallengeResponse {
            signature: sig.to_bytes(),
            public_key: key.verifying_key().to_bytes(),
        };
        let mut buf = [0u8; 256];
        let written = codec::encode_request(&request, &mut buf).unwrap();
        stream.write_all(&buf[..written]).unwrap();
        stream.flush().unwrap();
    }

    /// Like `client_sign_challenge` but corrupts the signature.
    fn client_sign_challenge_bad(stream: &mut UnixStream, key: &SigningKey) {
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        stream.read_exact(&mut payload[..len]).unwrap();

        let resp = codec::decode_response(&payload[..len]).unwrap();
        let nonce = match resp {
            ResponseKind::Challenge { nonce } => nonce,
            other => panic!("expected Challenge, got {other:?}"),
        };

        let mut sig_bytes = key.sign(&nonce).to_bytes();
        sig_bytes[0] ^= 0xFF;

        let request = Request::ChallengeResponse {
            signature: sig_bytes,
            public_key: key.verifying_key().to_bytes(),
        };
        let mut buf = [0u8; 256];
        let written = codec::encode_request(&request, &mut buf).unwrap();
        stream.write_all(&buf[..written]).unwrap();
        stream.flush().unwrap();
    }

    /// Read one length-prefixed frame and decode as a response.
    fn read_response(stream: &mut UnixStream) -> ResponseKind {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = [0u8; 64];
        stream.read_exact(&mut buf[..len]).unwrap();
        codec::decode_response(&buf[..len]).unwrap()
    }

    #[test]
    fn auth_success_returns_permission() {
        let keys = keys_with_test_key("trader");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::ServerReady));

        let result = handle.join().unwrap();
        assert_eq!(result.unwrap(), Permission::Trader);
    }

    #[test]
    fn auth_admin_permission() {
        let keys = keys_with_test_key("admin");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::ServerReady));

        assert_eq!(handle.join().unwrap().unwrap(), Permission::Admin);
    }

    #[test]
    fn auth_unknown_key_sends_auth_failed() {
        let keys = AuthorizedKeys::parse("").unwrap();
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_bad_signature_sends_auth_failed() {
        let keys = keys_with_test_key("admin");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge_bad(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_wrong_message_type_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard the Challenge.
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        s2.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        s2.read_exact(&mut payload[..len]).unwrap();

        // Send a Heartbeat instead of ChallengeResponse.
        let mut buf = [0u8; 16];
        let written = codec::encode_request(&Request::Heartbeat, &mut buf).unwrap();
        s2.write_all(&buf[..written]).unwrap();
        s2.flush().unwrap();

        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_client_disconnects_is_error() {
        let keys = keys_with_test_key("trader");
        let (s1, s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Drop immediately — server fails reading the ChallengeResponse.
        drop(s2);

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_different_key_than_authorized_is_rejected() {
        // Authorize the test key, but connect with a different key.
        let keys = keys_with_test_key("trader");
        let wrong_key = SigningKey::from_bytes(&[0xCC; 32]);
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &wrong_key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_oversized_frame_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard Challenge.
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        s2.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        s2.read_exact(&mut payload[..len]).unwrap();

        // Send a frame claiming to be 1000 bytes (way over the 256 limit).
        let fake_len: u32 = 1000;
        s2.write_all(&fake_len.to_le_bytes()).unwrap();
        s2.flush().unwrap();

        // Server should send AuthFailed before dropping.
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_zero_length_frame_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard Challenge.
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        s2.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        s2.read_exact(&mut payload[..len]).unwrap();

        // Send a zero-length frame — decode_request will fail on empty input.
        let zero_len: u32 = 0;
        s2.write_all(&zero_len.to_le_bytes()).unwrap();
        s2.flush().unwrap();

        // Server should send AuthFailed before dropping.
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_readonly_permission() {
        let keys = keys_with_test_key("readonly");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::ServerReady));

        let perm = handle.join().unwrap().unwrap();
        assert_eq!(perm, Permission::ReadOnly);
        assert!(!perm.can_trade());
    }
}
