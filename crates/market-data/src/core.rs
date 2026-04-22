//! MarketDataCore — connects to the event publisher, maintains mirrors,
//! and exposes read-only book state for gateway sessions.
//!
//! Runs on a dedicated thread. Communicates with the gateway event loop
//! through a shared `Arc<RwLock<MdState>>`.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use ed25519_dalek::{Signer, SigningKey};
use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};
use melin_trading::types::{ExecutionReport, Symbol};

use crate::mirror::BookMirror;

/// Shared state between the core thread and the gateway event loop.
///
/// RwLock: the core thread takes brief write locks to apply each event;
/// the event loop takes read locks when building FIX snapshots. Contention
/// is low — writes are sub-microsecond, reads are rare (one per V request).
pub struct MdState {
    /// Per-symbol book mirrors.
    pub mirrors: HashMap<Symbol, BookMirror>,
    /// Last applied ring sequence from the event publisher.
    pub last_seq: u64,
    /// Whether the core has completed its initial snapshot.
    pub ready: bool,
}

impl Default for MdState {
    fn default() -> Self {
        Self::new()
    }
}

impl MdState {
    pub fn new() -> Self {
        Self {
            mirrors: HashMap::new(),
            last_seq: 0,
            ready: false,
        }
    }
}

/// Configuration for the core thread.
pub struct CoreConfig {
    /// Event publisher address to connect to.
    pub event_publisher_addr: SocketAddr,
    /// Symbols to subscribe to (empty = all).
    pub symbols: Vec<Symbol>,
    /// Path to the Ed25519 private key (32-byte raw seed) for
    /// authenticating to the event publisher.
    pub key_path: PathBuf,
}

/// Run the MarketDataCore loop. Blocks the calling thread until shutdown.
///
/// Connects to the event publisher, authenticates, subscribes, parses
/// the snapshot, then enters the firehose loop applying every Report
/// to the shared mirrors.
pub fn run(config: CoreConfig, state: Arc<RwLock<MdState>>, shutdown: &AtomicBool) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        match run_session(&config, &state, shutdown) {
            Ok(()) => return, // clean shutdown
            Err(e) => {
                tracing::warn!(error = %e, "MarketDataCore disconnected, reconnecting in 1s");
                // Clear state on disconnect — the next connect will re-snapshot.
                if let Ok(mut s) = state.write() {
                    s.mirrors.clear();
                    s.ready = false;
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

/// Run one session: connect, auth, subscribe, snapshot, firehose.
fn run_session(
    config: &CoreConfig,
    state: &Arc<RwLock<MdState>>,
    shutdown: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut stream =
        TcpStream::connect_timeout(&config.event_publisher_addr, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_nodelay(true)?;

    tracing::info!(addr = %config.event_publisher_addr, "connected to event publisher");

    // Step 1: Auth handshake (client side).
    // Load the Ed25519 signing key.
    let signing_key = load_signing_key(&config.key_path)?;
    let public_key = signing_key.verifying_key();

    // Read Challenge from publisher.
    let challenge = read_response(&mut stream)?;
    let nonce = match challenge {
        ResponseKind::Challenge { nonce } => nonce,
        other => {
            return Err(format!(
                "expected Challenge, got {:?}",
                std::mem::discriminant(&other)
            )
            .into());
        }
    };

    // Sign the nonce and send ChallengeResponse.
    let signature = signing_key.sign(&nonce);
    let auth_request = Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: public_key.to_bytes(),
    };
    send_request(&mut stream, &auth_request, 0)?;

    // Read ServerReady or AuthFailed.
    let auth_result = read_response(&mut stream)?;
    match auth_result {
        ResponseKind::ServerReady => {
            tracing::info!("event publisher auth succeeded");
        }
        ResponseKind::AuthFailed => {
            return Err("event publisher auth failed".into());
        }
        other => {
            return Err(format!(
                "expected ServerReady, got {:?}",
                std::mem::discriminant(&other)
            )
            .into());
        }
    }

    // Step 2: Send Subscribe request.
    let mut symbols_arr = [Symbol(0); 8];
    let count = config.symbols.len().min(8) as u8;
    for (i, &sym) in config.symbols.iter().take(8).enumerate() {
        symbols_arr[i] = sym;
    }
    let subscribe = Request::Subscribe {
        symbols: symbols_arr,
        count,
    };
    send_request(&mut stream, &subscribe, 1)?;

    // Step 3: Parse snapshot.
    let snapshot = crate::cold_start::parse_snapshot(&mut stream)?;
    tracing::info!(
        symbols = snapshot.mirrors.len(),
        last_seq = snapshot.last_applied_seq,
        "snapshot received"
    );

    // Seed the shared state.
    {
        let mut s = state.write().map_err(|e| format!("lock poisoned: {e}"))?;
        s.mirrors.clear();
        for (sym, mirror) in snapshot.mirrors {
            s.mirrors.insert(sym, mirror);
        }
        s.last_seq = snapshot.last_applied_seq;
        s.ready = true;
    }

    // Step 4: Firehose loop.
    tracing::info!("entering firehose loop");
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        let (seq, response) = read_frame(&mut stream)?;

        if let ResponseKind::Report(ref report) = response {
            let sym = report_symbol(report);
            let mut s = state.write().map_err(|e| format!("lock poisoned: {e}"))?;
            let mirror = s.mirrors.entry(sym).or_insert_with(|| BookMirror::new(sym));
            mirror.apply(report);
            s.last_seq = seq;
        }
    }
}

/// Extract the symbol from an `ExecutionReport`.
///
/// Returns `Symbol(0)` for the query-response variants (`Stats`,
/// `Position`) which carry no instrument context — market-data never
/// sees these over the wire, but the match has to be exhaustive.
fn report_symbol(report: &ExecutionReport) -> Symbol {
    match *report {
        ExecutionReport::Placed { symbol, .. }
        | ExecutionReport::Fill { symbol, .. }
        | ExecutionReport::Cancelled { symbol, .. }
        | ExecutionReport::Triggered { symbol, .. }
        | ExecutionReport::Rejected { symbol, .. }
        | ExecutionReport::Replaced { symbol, .. }
        | ExecutionReport::InstrumentStatusChanged { symbol, .. } => symbol,
    }
}

/// Load a 32-byte Ed25519 signing key seed from a file.
fn load_signing_key(path: &std::path::Path) -> Result<SigningKey, Box<dyn std::error::Error>> {
    let seed = std::fs::read(path)?;
    if seed.len() != 32 {
        return Err(format!(
            "key file must be 32 bytes, got {} ({})",
            seed.len(),
            path.display()
        )
        .into());
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&seed);
    Ok(SigningKey::from_bytes(&bytes))
}

/// Read a single length-prefixed response from the stream.
fn read_response(stream: &mut TcpStream) -> Result<ResponseKind, Box<dyn std::error::Error>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len > 4096 {
        return Err(format!("frame too large: {frame_len}").into());
    }
    let mut frame_buf = vec![0u8; frame_len];
    stream.read_exact(&mut frame_buf)?;
    let response = codec::decode_response(&frame_buf)?;
    Ok(response)
}

/// Read a sequence-prefixed frame (8-byte seq + 4-byte len + payload).
fn read_frame(stream: &mut TcpStream) -> Result<(u64, ResponseKind), Box<dyn std::error::Error>> {
    let mut seq_buf = [0u8; 8];
    stream.read_exact(&mut seq_buf)?;
    let seq = u64::from_le_bytes(seq_buf);

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len > 4096 {
        return Err(format!("frame too large: {frame_len}").into());
    }
    let mut frame_buf = vec![0u8; frame_len];
    stream.read_exact(&mut frame_buf)?;
    let response = codec::decode_response(&frame_buf)?;
    Ok((seq, response))
}

/// Send a length-prefixed request.
fn send_request(stream: &mut TcpStream, request: &Request, seq: u64) -> io::Result<()> {
    let mut buf = [0u8; 256];
    let written = codec::encode_request(request, seq, &mut buf)
        .map_err(|e| io::Error::other(format!("encode: {e}")))?;
    stream.write_all(&buf[..written])?;
    stream.flush()
}
