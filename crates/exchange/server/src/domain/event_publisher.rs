//! Event publisher — broadcasts execution events to TCP subscribers
//! with book-snapshot-on-subscribe support.
//!
//! Consumes from the output disruptor ring (consumer 1) and maintains
//! a per-symbol `BookMirror`. New subscribers receive a snapshot of the
//! current book state before joining the live firehose.
//!
//! Wire format per firehose frame:
//! ```text
//! | sequence (u64 LE) | length (u32 LE) | tag (u8) | payload (var) |
//! ```
//! The sequence number is the output ring's monotonic sequence for gap
//! detection by subscribers. The rest is the standard response codec
//! from `crates/exchange/protocol/src/codec.rs`.
//!
//! Subscription protocol (after Ed25519 auth + ServerReady):
//! 1. Client sends `Subscribe { symbols, count }` (count=0 → all symbols)
//! 2. Server sends `BookSnapshotBegin/Level/End` per symbol, then `SnapshotComplete`
//! 3. Server switches to non-blocking and starts the live firehose
//!
//! Slow subscriber policy: if a TCP write returns `WouldBlock`, the subscriber
//! is disconnected immediately. The publisher must never block on a slow client.
//!
//! Ed25519 challenge-response auth is required (ReadOnly permission or above).

use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{debug, error, info, warn};

use melin_disruptor::ring;
use melin_market_data::mirror::BookMirror;
use melin_protocol::auth::AuthorizedKeys;
use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};
use melin_transport_core::pipeline::{
    OutputPayload as GenericOutputPayload, OutputSlot as GenericOutputSlot,
};
use melin_types::types::{ExecutionReport, QueryResponse, Symbol};

// Trading-bound shorthand for the wire-format types this publisher
// consumes. The runtime feeds it `Consumer<OutputSlot<A>>` where
// `A = ServerApp`, so binding `A::Report` / `A::QueryResponse` here
// keeps the rest of the file readable.
type OutputSlot = GenericOutputSlot<ExecutionReport, QueryResponse>;
type OutputPayload = GenericOutputPayload<ExecutionReport, QueryResponse>;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded frame size: 8 (sequence) + 512 (response) = 520 bytes.
/// PositionSnapshot is the largest variant at up to 330 bytes; 512 covers all.
const MAX_FRAME_BUF: usize = 520;

/// Subscriber lifecycle state.
enum SubscriberState {
    /// Waiting for `Subscribe` request (socket is blocking with read timeout).
    AwaitingSubscription,
    /// Active firehose (socket is non-blocking).
    Streaming,
}

/// Per-subscriber state.
struct Subscriber {
    stream: TcpStream,
    addr: SocketAddr,
    state: SubscriberState,
}

/// Extract the symbol from an `ExecutionReport`.
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

/// Convert an `OutputPayload` to the wire `ResponseKind`. Translates
/// query responses (`QueryResponse::Stats` / `::Position` /
/// `::RequestSeqHwm`) to the public wire variants.
///
/// Returns `None` for `OutputPayload::BatchEnd` — that slot carries
/// no payload of its own; the wire `BatchEnd` is emitted from the
/// `is_last_in_request` flag (see `slot_to_kinds`).
fn payload_to_response(payload: OutputPayload) -> Option<ResponseKind> {
    Some(match payload {
        OutputPayload::QueryResponse(QueryResponse::Stats {
            active_connections,
            events_processed,
            journal_sequence,
        }) => ResponseKind::StatsHeader {
            active_connections,
            events_processed,
            journal_sequence,
        },
        OutputPayload::QueryResponse(QueryResponse::Position {
            account,
            balances,
            count,
        }) => ResponseKind::PositionSnapshot {
            account,
            balances,
            count,
        },
        OutputPayload::QueryResponse(QueryResponse::RequestSeqHwm { hwm }) => {
            ResponseKind::RequestSeqHwm { hwm }
        }
        OutputPayload::Report(report) => ResponseKind::Report(report),
        OutputPayload::BatchEnd => return None,
        OutputPayload::EngineError => ResponseKind::EngineError,
    })
}

/// Expand a slot into the wire `ResponseKind`s subscribers should see:
/// the payload (if any) plus a trailing `BatchEnd` when
/// `is_last_in_request` is set. Returns the populated count
/// (`0..=2`); the array is filled from index 0.
fn slot_to_kinds(slot: &OutputSlot) -> ([ResponseKind; 2], usize) {
    let mut kinds = [ResponseKind::BatchEnd; 2];
    let mut len = 0;
    if let Some(k) = payload_to_response(slot.payload) {
        kinds[len] = k;
        len += 1;
    }
    if slot.is_last_in_request {
        kinds[len] = ResponseKind::BatchEnd;
        len += 1;
    }
    (kinds, len)
}

/// Run the event publisher loop. Blocks the calling thread until shutdown.
///
/// Binds a TCP listener, accepts subscribers with Ed25519 auth, waits
/// for a `Subscribe` request, sends a book snapshot, then streams the
/// live firehose.
pub fn run(
    mut consumer: ring::Consumer<OutputSlot>,
    bind_addr: SocketAddr,
    authorized_keys: Arc<AuthorizedKeys>,
    shutdown: &AtomicBool,
    busy_spin: bool,
) {
    let listener = match TcpListener::bind(bind_addr) {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %bind_addr, error = %e, "event publisher: failed to bind");
            return;
        }
    };
    // Non-blocking accept so we can interleave ring consumption.
    listener
        .set_nonblocking(true)
        .expect("set listener non-blocking");

    let mut subscribers: Vec<Subscriber> = Vec::new();
    // Per-symbol book mirrors, updated from every Report event.
    // rustc_hash::FxHashMap: fast non-crypto hash, no rehash spikes
    // (few symbols — typically < 100).
    let mut mirrors: rustc_hash::FxHashMap<Symbol, BookMirror> = rustc_hash::FxHashMap::default();
    // Tracks the last consumed ring sequence. Used as `last_applied_seq`
    // in snapshot frames so the subscriber knows where the firehose resumes.
    let mut last_seq: u64 = 0;

    let mut batch = [OutputSlot::default(); MAX_BATCH];
    let mut frame_buf = [0u8; MAX_FRAME_BUF];
    let mut idle_spins: u32 = 0;
    let mut last_broadcast = std::time::Instant::now();

    while !shutdown.load(Ordering::Relaxed) {
        // Accept new connections (non-blocking).
        accept_subscribers(&listener, &authorized_keys, &mut subscribers);

        // Process pending subscribers: read Subscribe request, send snapshot.
        process_pending_subscribers(&mut subscribers, &mirrors, last_seq);

        // Consume from the ring.
        let batch_start_seq = consumer.next_read();
        let count = consumer.consume_batch(&mut batch, MAX_BATCH);
        if count == 0 {
            // Send a heartbeat to streaming subscribers every 10s of idle
            // so their read timeouts don't fire on a quiet firehose.
            if last_broadcast.elapsed() >= std::time::Duration::from_secs(10) {
                frame_buf[..8].copy_from_slice(&last_seq.to_le_bytes());
                if let Ok(n) = codec::encode_response(&ResponseKind::Heartbeat, &mut frame_buf[8..])
                {
                    let frame = &frame_buf[..8 + n];
                    subscribers.retain_mut(|sub| {
                        if !matches!(sub.state, SubscriberState::Streaming) {
                            return true;
                        }
                        sub.stream.write_all(frame).is_ok()
                    });
                }
                last_broadcast = std::time::Instant::now();
            }

            if busy_spin || idle_spins < 1000 {
                idle_spins = idle_spins.wrapping_add(1);
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            continue;
        }
        idle_spins = 0;
        last_broadcast = std::time::Instant::now();

        // Process each event: update mirrors, then broadcast to streaming subscribers.
        for (idx, slot) in batch[..count].iter().enumerate() {
            let ring_seq = batch_start_seq + idx as u64;

            // Update mirror for Report events.
            if let OutputPayload::Report(ref report) = slot.payload {
                let sym = report_symbol(report);
                let mirror = mirrors.entry(sym).or_insert_with(|| BookMirror::new(sym));
                mirror.apply(report);
            }

            last_seq = ring_seq;

            // Encode and broadcast — one slot may expand to up to two
            // wire frames (payload + trailing BatchEnd).
            let (kinds, kinds_len) = slot_to_kinds(slot);
            for kind in &kinds[..kinds_len] {
                frame_buf[..8].copy_from_slice(&ring_seq.to_le_bytes());
                let response_len = match codec::encode_response(kind, &mut frame_buf[8..]) {
                    Ok(n) => n,
                    Err(e) => {
                        debug!(error = %e, "event publisher: encode failed, skipping");
                        continue;
                    }
                };
                let total_len = 8 + response_len;
                let frame = &frame_buf[..total_len];

                // Write to streaming subscribers only, removing failed ones.
                subscribers.retain_mut(|sub| {
                    if !matches!(sub.state, SubscriberState::Streaming) {
                        return true; // keep pending subscribers
                    }
                    match sub.stream.write_all(frame) {
                        Ok(()) => true,
                        Err(e) => {
                            if e.kind() == io::ErrorKind::WouldBlock {
                                debug!(addr = %sub.addr, "event subscriber too slow, disconnecting");
                            } else {
                                debug!(addr = %sub.addr, error = %e, "event subscriber write error");
                            }
                            false
                        }
                    }
                });
            }
        }
    }

    info!(
        subscribers = subscribers.len(),
        "event publisher shutting down"
    );
}

/// Accept pending connections, authenticate, and add as AwaitingSubscription.
fn accept_subscribers(
    listener: &TcpListener,
    authorized_keys: &AuthorizedKeys,
    subscribers: &mut Vec<Subscriber>,
) {
    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                match authenticate_subscriber(&stream, addr, authorized_keys) {
                    Ok(()) => {
                        // Leave in blocking mode with a read timeout for the
                        // Subscribe handshake. Switched to non-blocking after
                        // snapshot delivery.
                        info!(addr = %addr, "event subscriber authenticated, awaiting subscription");
                        subscribers.push(Subscriber {
                            stream,
                            addr,
                            state: SubscriberState::AwaitingSubscription,
                        });
                    }
                    Err(e) => {
                        debug!(addr = %addr, error = %e, "event subscriber auth failed");
                    }
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(e) => {
                warn!(error = %e, "event publisher: accept error");
                break;
            }
        }
    }
}

/// Check pending subscribers for incoming `Subscribe` requests.
/// On success, send snapshot and transition to Streaming.
fn process_pending_subscribers(
    subscribers: &mut Vec<Subscriber>,
    mirrors: &rustc_hash::FxHashMap<Symbol, BookMirror>,
    last_seq: u64,
) {
    subscribers.retain_mut(|sub| {
        if !matches!(sub.state, SubscriberState::AwaitingSubscription) {
            return true;
        }
        match try_read_subscribe(&sub.stream) {
            Ok(Some(symbols)) => {
                if let Err(e) = send_snapshot(&mut sub.stream, &symbols, mirrors, last_seq) {
                    debug!(addr = %sub.addr, error = %e, "snapshot send failed");
                    return false;
                }
                if let Err(e) = sub.stream.set_nonblocking(true) {
                    debug!(addr = %sub.addr, error = %e, "set non-blocking failed");
                    return false;
                }
                sub.state = SubscriberState::Streaming;
                info!(addr = %sub.addr, symbols = symbols.len(), "subscriber streaming");
                true
            }
            Ok(None) => true, // no data yet
            Err(e) => {
                debug!(addr = %sub.addr, error = %e, "subscribe read failed");
                false
            }
        }
    });
}

/// Try to read a `Subscribe` request from a pending subscriber.
///
/// Uses a 1ms read timeout to avoid blocking the main loop. Returns
/// `Ok(None)` if no data is available yet, `Ok(Some(symbols))` on
/// success, or `Err` on protocol violation / socket error.
fn try_read_subscribe(
    stream: &TcpStream,
) -> Result<Option<Vec<Symbol>>, Box<dyn std::error::Error>> {
    // Short timeout — this runs on every main-loop iteration for
    // pending subscribers, so it must not block.
    stream.set_read_timeout(Some(std::time::Duration::from_millis(1)))?;

    let mut len_buf = [0u8; 4];
    match io::Read::read_exact(&mut &*stream, &mut len_buf) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            return Ok(None);
        }
        Err(e) => return Err(e.into()),
    }

    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len > 256 {
        return Err(io::Error::other(format!("subscribe frame too large: {frame_len}")).into());
    }
    let mut frame_buf = [0u8; 256];
    io::Read::read_exact(&mut &*stream, &mut frame_buf[..frame_len])?;

    let (_seq, request) = codec::decode_request(&frame_buf[..frame_len])?;
    match request {
        Request::Subscribe { symbols, count } => {
            let n = count as usize;
            if n == 0 {
                // Wildcard — empty vec signals "all symbols".
                Ok(Some(Vec::new()))
            } else {
                Ok(Some(symbols[..n].to_vec()))
            }
        }
        other => Err(format!(
            "expected Subscribe, got {:?}",
            std::mem::discriminant(&other)
        )
        .into()),
    }
}

/// Send a book snapshot to a subscriber (blocking writes, cold path).
///
/// For each requested symbol (or all if `symbols` is empty), sends
/// `BookSnapshotBegin`, one `BookSnapshotLevel` per level, then
/// `BookSnapshotEnd`. Finally sends `SnapshotComplete`.
fn send_snapshot(
    stream: &mut TcpStream,
    requested: &[Symbol],
    mirrors: &rustc_hash::FxHashMap<Symbol, BookMirror>,
    last_seq: u64,
) -> io::Result<()> {
    use melin_types::types::Side;

    let mut buf = [0u8; MAX_FRAME_BUF];

    // Determine which symbols to snapshot.
    let symbols: Vec<Symbol> = if requested.is_empty() {
        // Wildcard: all known symbols.
        mirrors.keys().copied().collect()
    } else {
        requested.to_vec()
    };

    for &sym in &symbols {
        // Begin.
        let begin = ResponseKind::BookSnapshotBegin {
            symbol: sym,
            last_applied_seq: last_seq,
        };
        write_snapshot_frame(stream, &mut buf, last_seq, &begin)?;

        let mut level_count: u32 = 0;

        if let Some(mirror) = mirrors.get(&sym) {
            // Bids: descending price order (best bid first).
            for (&price, level) in mirror.bids().iter().rev() {
                let frame = ResponseKind::BookSnapshotLevel {
                    symbol: sym,
                    side: Side::Buy,
                    price,
                    qty: level.total_qty,
                    order_count: level.order_count,
                };
                write_snapshot_frame(stream, &mut buf, last_seq, &frame)?;
                level_count += 1;
            }

            // Asks: ascending price order (best ask first).
            for (&price, level) in mirror.asks().iter() {
                let frame = ResponseKind::BookSnapshotLevel {
                    symbol: sym,
                    side: Side::Sell,
                    price,
                    qty: level.total_qty,
                    order_count: level.order_count,
                };
                write_snapshot_frame(stream, &mut buf, last_seq, &frame)?;
                level_count += 1;
            }
        }
        // Empty mirror (no events yet) → Begin/End with zero levels.

        // End.
        let end = ResponseKind::BookSnapshotEnd {
            symbol: sym,
            level_count,
        };
        write_snapshot_frame(stream, &mut buf, last_seq, &end)?;
    }

    // Complete.
    let complete = ResponseKind::SnapshotComplete {
        last_applied_seq: last_seq,
    };
    write_snapshot_frame(stream, &mut buf, last_seq, &complete)?;
    stream.flush()?;

    Ok(())
}

/// Encode a snapshot response as a sequence-prefixed frame and write it.
fn write_snapshot_frame(
    stream: &mut TcpStream,
    buf: &mut [u8; MAX_FRAME_BUF],
    seq: u64,
    kind: &ResponseKind,
) -> io::Result<()> {
    buf[..8].copy_from_slice(&seq.to_le_bytes());
    let response_len = codec::encode_response(kind, &mut buf[8..])
        .map_err(|e| io::Error::other(format!("encode snapshot frame: {e}")))?;
    stream.write_all(&buf[..8 + response_len])
}

/// Run Ed25519 challenge-response authentication on a subscriber connection.
///
/// Reuses the same protocol as `server.rs` — Challenge/ChallengeResponse/ServerReady.
/// Accepts any permission level (ReadOnly or above). Blocks briefly during
/// handshake (cold path, before setting non-blocking for data).
fn authenticate_subscriber(
    stream: &TcpStream,
    addr: SocketAddr,
    authorized_keys: &AuthorizedKeys,
) -> Result<(), Box<dyn std::error::Error>> {
    use ed25519_dalek::{Verifier, VerifyingKey};

    // Set a read timeout for the auth handshake to prevent slow clients
    // from stalling the publisher.
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;
    stream.set_nonblocking(false)?;

    // Clone the stream so we have separate read and write handles.
    // The underlying fd is shared — both clones refer to the same socket.
    let mut write_stream = stream.try_clone()?;
    let mut read_stream = stream;

    // Generate a 32-byte random nonce (OsRng, SEC-10).
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("getrandom failed: {e}")))?;

    let mut buf = [0u8; 128];
    let written = codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf)
        .map_err(|e| io::Error::other(format!("encode Challenge: {e}")))?;
    write_stream.write_all(&buf[..written])?;
    write_stream.flush()?;

    // Read ChallengeResponse frame.
    let mut len_buf = [0u8; 4];
    io::Read::read_exact(&mut read_stream, &mut len_buf)?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len > 256 {
        send_auth_failed(&mut write_stream);
        return Err(io::Error::other(format!("auth frame too large: {frame_len}")).into());
    }
    let mut frame_buf = [0u8; 256];
    io::Read::read_exact(&mut read_stream, &mut frame_buf[..frame_len])?;

    let (_seq, request) = match codec::decode_request(&frame_buf[..frame_len]) {
        Ok(pair) => pair,
        Err(e) => {
            send_auth_failed(&mut write_stream);
            return Err(io::Error::other(format!("decode ChallengeResponse: {e}")).into());
        }
    };

    let (signature_bytes, public_key_bytes) = match request {
        Request::ChallengeResponse {
            signature,
            public_key,
        } => (signature, public_key),
        other => {
            send_auth_failed(&mut write_stream);
            return Err(format!(
                "expected ChallengeResponse, got {:?}",
                std::mem::discriminant(&other)
            )
            .into());
        }
    };

    // Look up the public key in authorized_keys.
    let _permission = match authorized_keys.lookup(&public_key_bytes) {
        Some(perm) => perm,
        None => {
            send_auth_failed(&mut write_stream);
            return Err("unknown public key".into());
        }
    };

    // Verify the Ed25519 signature over `nonce ‖ server_eph ‖
    // client_eph` (TCP path's ephs are zeros — see Challenge above).
    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes).map_err(|e| {
        send_auth_failed(&mut write_stream);
        io::Error::other(format!("invalid public key: {e}"))
    })?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    let signing_payload = melin_protocol::auth::auth_signing_payload(&nonce);
    verifying_key
        .verify(&signing_payload, &signature)
        .map_err(|e| {
            send_auth_failed(&mut write_stream);
            io::Error::other(format!("signature verification failed: {e}"))
        })?;

    // Auth succeeded — send ServerReady.
    let written = codec::encode_response(&ResponseKind::ServerReady, &mut buf)
        .map_err(|e| io::Error::other(format!("encode ServerReady: {e}")))?;
    write_stream.write_all(&buf[..written])?;
    write_stream.flush()?;

    debug!(addr = %addr, "event subscriber authenticated");
    Ok(())
}

/// Best-effort send of AuthFailed to the client.
fn send_auth_failed(writer: &mut dyn Write) {
    let mut buf = [0u8; 8];
    if let Ok(n) = codec::encode_response(&ResponseKind::AuthFailed, &mut buf) {
        // Best-effort: ignore write errors during auth failure notification.
        let _ = writer.write_all(&buf[..n]);
        let _ = writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_types::types::*;

    /// Helper to create a Streaming subscriber for tests that bypass
    /// the auth + subscribe handshake.
    fn test_subscriber(stream: TcpStream, addr: SocketAddr) -> Subscriber {
        Subscriber {
            stream,
            addr,
            state: SubscriberState::Streaming,
        }
    }

    #[test]
    fn frame_encoding_with_sequence_prefix() {
        // Test the frame wire format: u64 ring sequence + standard response.
        let kind = payload_to_response(OutputPayload::Report(ExecutionReport::Placed {
            order_id: OrderId(100),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Buy,
            price: Price(std::num::NonZeroU64::new(100).unwrap()),
            quantity: Quantity(std::num::NonZeroU64::new(50).unwrap()),
        }))
        .expect("Report payload always has a wire kind");

        let ring_seq: u64 = 42;
        let mut buf = [0u8; MAX_FRAME_BUF];
        buf[..8].copy_from_slice(&ring_seq.to_le_bytes());
        let response_len = codec::encode_response(&kind, &mut buf[8..]).unwrap();
        let total_len = 8 + response_len;

        // Verify sequence prefix.
        let decoded_seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
        assert_eq!(decoded_seq, 42);

        // Verify the response portion decodes correctly.
        let len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
        let decoded = codec::decode_response(&buf[12..12 + len]).unwrap();
        assert!(matches!(
            decoded,
            ResponseKind::Report(ExecutionReport::Placed {
                order_id: OrderId(100),
                ..
            })
        ));

        assert!(total_len <= MAX_FRAME_BUF);
        assert!(total_len > 8);
    }

    #[test]
    fn payload_to_response_all_variants() {
        let r = payload_to_response(OutputPayload::Report(ExecutionReport::Placed {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Buy,
            price: Price(std::num::NonZeroU64::new(50).unwrap()),
            quantity: Quantity(std::num::NonZeroU64::new(10).unwrap()),
        }));
        assert!(matches!(r, Some(ResponseKind::Report(_))));

        // BatchEnd-payload slots have no payload of their own; the
        // wire BatchEnd is emitted via `is_last_in_request`.
        assert!(payload_to_response(OutputPayload::BatchEnd).is_none());

        let r = payload_to_response(OutputPayload::EngineError);
        assert!(matches!(r, Some(ResponseKind::EngineError)));

        let r = payload_to_response(OutputPayload::QueryResponse(QueryResponse::Stats {
            active_connections: 5,
            events_processed: 1000,
            journal_sequence: 500,
        }));
        assert!(matches!(r, Some(ResponseKind::StatsHeader { .. })));
    }

    #[test]
    fn slow_subscriber_disconnected() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let _client = TcpStream::connect(addr).unwrap();
        let (server_stream, client_addr) = listener.accept().unwrap();
        server_stream.set_nonblocking(true).unwrap();

        unsafe {
            let size: libc::c_int = 1;
            libc::setsockopt(
                std::os::unix::io::AsRawFd::as_raw_fd(&server_stream),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &size as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }

        let mut sub = test_subscriber(server_stream, client_addr);

        let frame = [0u8; 4096];
        let mut got_error = false;
        for _ in 0..1000 {
            if sub.stream.write_all(&frame).is_err() {
                got_error = true;
                break;
            }
        }
        assert!(got_error, "expected WouldBlock from slow subscriber");
    }

    #[test]
    fn multiple_subscribers_receive_same_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client1 = TcpStream::connect(addr).unwrap();
        let (server1, addr1) = listener.accept().unwrap();
        server1.set_nonblocking(true).unwrap();

        let mut client2 = TcpStream::connect(addr).unwrap();
        let (server2, addr2) = listener.accept().unwrap();
        server2.set_nonblocking(true).unwrap();

        let mut subscribers = vec![
            test_subscriber(server1, addr1),
            test_subscriber(server2, addr2),
        ];

        // Use a wire BatchEnd frame directly — these tests just need a
        // small dummy payload to push to subscribers.
        let kind = ResponseKind::BatchEnd;
        let mut frame_buf = [0u8; MAX_FRAME_BUF];
        let ring_seq: u64 = 7;
        frame_buf[..8].copy_from_slice(&ring_seq.to_le_bytes());
        let response_len = codec::encode_response(&kind, &mut frame_buf[8..]).unwrap();
        let total_len = 8 + response_len;
        let frame = &frame_buf[..total_len];

        for sub in &mut subscribers {
            sub.stream.write_all(frame).unwrap();
        }

        client1
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        client2
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();

        let mut buf1 = [0u8; 64];
        let mut buf2 = [0u8; 64];
        let n1 = io::Read::read(&mut client1, &mut buf1).unwrap();
        let n2 = io::Read::read(&mut client2, &mut buf2).unwrap();

        assert_eq!(n1, total_len);
        assert_eq!(n2, total_len);
        assert_eq!(&buf1[..n1], &buf2[..n2]);

        let seq1 = u64::from_le_bytes(buf1[..8].try_into().unwrap());
        let seq2 = u64::from_le_bytes(buf2[..8].try_into().unwrap());
        assert_eq!(seq1, 7);
        assert_eq!(seq2, 7);
    }

    #[test]
    fn monotonic_sequence_numbers_in_frames() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client = TcpStream::connect(addr).unwrap();
        let (server_stream, client_addr) = listener.accept().unwrap();
        server_stream.set_nonblocking(true).unwrap();

        let mut sub = test_subscriber(server_stream, client_addr);

        for seq in 0u64..10 {
            // Wire BatchEnd frame directly — synthetic test payload.
            let kind = ResponseKind::BatchEnd;
            let mut frame_buf = [0u8; MAX_FRAME_BUF];
            frame_buf[..8].copy_from_slice(&seq.to_le_bytes());
            let response_len = codec::encode_response(&kind, &mut frame_buf[8..]).unwrap();
            sub.stream
                .write_all(&frame_buf[..8 + response_len])
                .unwrap();
        }

        client
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut buf = [0u8; 2048];
        let mut total_read = 0;
        while total_read < 10 * 13 {
            match io::Read::read(&mut client, &mut buf[total_read..]) {
                Ok(n) if n > 0 => total_read += n,
                _ => break,
            }
        }

        let mut offset = 0;
        let mut prev_seq: Option<u64> = None;
        let mut count = 0;
        while offset + 12 <= total_read {
            let seq = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
            let len = u32::from_le_bytes(buf[offset + 8..offset + 12].try_into().unwrap()) as usize;
            if offset + 12 + len > total_read {
                break;
            }
            if let Some(prev) = prev_seq {
                assert_eq!(
                    seq,
                    prev + 1,
                    "sequence gap detected: prev={prev}, current={seq}"
                );
            }
            prev_seq = Some(seq);
            offset += 8 + 4 + len;
            count += 1;
        }
        assert_eq!(count, 10, "expected 10 frames");
    }

    #[test]
    fn failed_subscriber_does_not_affect_others() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client1 = TcpStream::connect(addr).unwrap();
        let (server1, addr1) = listener.accept().unwrap();
        server1.set_nonblocking(true).unwrap();

        let _client2 = TcpStream::connect(addr).unwrap();
        let (server2, addr2) = listener.accept().unwrap();
        server2.set_nonblocking(true).unwrap();

        let mut subscribers = vec![
            test_subscriber(server1, addr1),
            test_subscriber(server2, addr2),
        ];

        drop(_client2);
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Use a wire BatchEnd frame directly — these tests just need a
        // small dummy payload to push to subscribers.
        let kind = ResponseKind::BatchEnd;
        let mut frame_buf = [0u8; MAX_FRAME_BUF];
        frame_buf[..8].copy_from_slice(&0u64.to_le_bytes());
        let response_len = codec::encode_response(&kind, &mut frame_buf[8..]).unwrap();
        let frame = &frame_buf[..8 + response_len];

        for _ in 0..10 {
            subscribers.retain_mut(|sub| sub.stream.write_all(frame).is_ok());
        }

        client1
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut buf = [0u8; 256];
        let n = io::Read::read(&mut client1, &mut buf).unwrap();
        assert!(n > 0, "client1 should receive data");

        assert_eq!(subscribers.len(), 1, "failed subscriber should be removed");
    }

    #[test]
    fn shutdown_stops_publisher() {
        use melin_protocol::auth::AuthorizedKeys;

        let (_, consumer) = ring::DisruptorBuilder::<OutputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumer.into_iter().next().unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let keys = Arc::new(AuthorizedKeys::parse("").unwrap());
        let handle = std::thread::Builder::new()
            .name("test-publisher".into())
            .spawn(move || {
                run(consumer, addr, keys, &shutdown2, false);
            })
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(50));
        shutdown.store(true, Ordering::Relaxed);

        handle.join().unwrap();
    }

    #[test]
    fn send_snapshot_empty_book() {
        // Snapshot of an empty mirror map should produce Begin/End/Complete.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client = TcpStream::connect(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();

        let mirrors = rustc_hash::FxHashMap::default();
        send_snapshot(&mut server, &[Symbol(1)], &mirrors, 42).unwrap();

        client
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut buf = [0u8; 512];
        let mut total = 0;
        loop {
            match io::Read::read(&mut client, &mut buf[total..]) {
                Ok(n) if n > 0 => total += n,
                _ => break,
            }
        }

        // Parse frames: expect Begin, End, Complete.
        let mut offset = 0;
        let mut responses = Vec::new();
        while offset + 12 <= total {
            let _seq = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap());
            let len = u32::from_le_bytes(buf[offset + 8..offset + 12].try_into().unwrap()) as usize;
            if offset + 12 + len > total {
                break;
            }
            let resp = codec::decode_response(&buf[offset + 12..offset + 12 + len]).unwrap();
            responses.push(resp);
            offset += 8 + 4 + len;
        }

        assert_eq!(responses.len(), 3);
        assert!(matches!(
            responses[0],
            ResponseKind::BookSnapshotBegin {
                symbol: Symbol(1),
                last_applied_seq: 42,
            }
        ));
        assert!(matches!(
            responses[1],
            ResponseKind::BookSnapshotEnd {
                symbol: Symbol(1),
                level_count: 0,
            }
        ));
        assert!(matches!(
            responses[2],
            ResponseKind::SnapshotComplete {
                last_applied_seq: 42,
            }
        ));
    }

    #[test]
    fn send_snapshot_with_levels() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client = TcpStream::connect(addr).unwrap();
        let (mut server, _) = listener.accept().unwrap();

        // Build a mirror with known state.
        let mut mirror = BookMirror::new(Symbol(1));
        mirror.apply(&ExecutionReport::Placed {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Buy,
            price: Price(std::num::NonZeroU64::new(100).unwrap()),
            quantity: Quantity(std::num::NonZeroU64::new(10).unwrap()),
        });
        mirror.apply(&ExecutionReport::Placed {
            order_id: OrderId(2),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Sell,
            price: Price(std::num::NonZeroU64::new(200).unwrap()),
            quantity: Quantity(std::num::NonZeroU64::new(5).unwrap()),
        });

        let mut mirrors = rustc_hash::FxHashMap::default();
        mirrors.insert(Symbol(1), mirror);

        send_snapshot(&mut server, &[Symbol(1)], &mirrors, 99).unwrap();

        client
            .set_read_timeout(Some(std::time::Duration::from_secs(1)))
            .unwrap();
        let mut buf = [0u8; 1024];
        let mut total = 0;
        loop {
            match io::Read::read(&mut client, &mut buf[total..]) {
                Ok(n) if n > 0 => total += n,
                _ => break,
            }
        }

        let mut offset = 0;
        let mut responses = Vec::new();
        while offset + 12 <= total {
            let len = u32::from_le_bytes(buf[offset + 8..offset + 12].try_into().unwrap()) as usize;
            if offset + 12 + len > total {
                break;
            }
            let resp = codec::decode_response(&buf[offset + 12..offset + 12 + len]).unwrap();
            responses.push(resp);
            offset += 8 + 4 + len;
        }

        // Begin, BidLevel, AskLevel, End, Complete = 5 frames.
        assert_eq!(responses.len(), 5);
        assert!(matches!(
            responses[0],
            ResponseKind::BookSnapshotBegin {
                symbol: Symbol(1),
                ..
            }
        ));
        // Bid level.
        assert!(matches!(
            responses[1],
            ResponseKind::BookSnapshotLevel {
                symbol: Symbol(1),
                side: Side::Buy,
                qty: 10,
                order_count: 1,
                ..
            }
        ));
        // Ask level.
        assert!(matches!(
            responses[2],
            ResponseKind::BookSnapshotLevel {
                symbol: Symbol(1),
                side: Side::Sell,
                qty: 5,
                order_count: 1,
                ..
            }
        ));
        assert!(matches!(
            responses[3],
            ResponseKind::BookSnapshotEnd {
                symbol: Symbol(1),
                level_count: 2,
            }
        ));
        assert!(matches!(
            responses[4],
            ResponseKind::SnapshotComplete {
                last_applied_seq: 99
            }
        ));
    }

    #[test]
    fn report_symbol_extracts_all_variants() {
        let sym = Symbol(42);
        assert_eq!(
            report_symbol(&ExecutionReport::Placed {
                order_id: OrderId(1),
                symbol: sym,
                account: AccountId(1),
                side: Side::Buy,
                price: Price(std::num::NonZeroU64::new(1).unwrap()),
                quantity: Quantity(std::num::NonZeroU64::new(1).unwrap()),
            }),
            sym
        );
        assert_eq!(
            report_symbol(&ExecutionReport::Rejected {
                order_id: OrderId(1),
                symbol: sym,
                account: AccountId(1),
                reason: melin_types::types::RejectReason::NoLiquidity,
            }),
            sym
        );
        assert_eq!(
            report_symbol(&ExecutionReport::InstrumentStatusChanged {
                symbol: sym,
                status: InstrumentStatus::Enabled,
            }),
            sym
        );
    }
}
