//! DPDK transport integration — single poll thread for NIC I/O + TCP.
//!
//! Replaces both the epoll reader pool and the response stage's socket
//! writes. A single DPDK poll thread owns all NIC I/O:
//!
//! - **Inbound**: `rx_burst` → smoltcp → frame decode → disruptor publish
//! - **Outbound**: response SPSC → per-connection TX queue → smoltcp → `tx_burst`
//!
//! The response stage still runs on its own pinned thread for cursor
//! gating and encoding, but instead of calling `write_all` on kernel
//! sockets, it pushes encoded frames into a lock-free SPSC queue per
//! connection. The DPDK poll thread drains these into smoltcp sockets.
//!
//! # Auth handshake
//!
//! New connections start in `AuthState::ChallengePending` — the poll loop
//! sends a Challenge frame and waits for the ChallengeResponse. Auth is
//! non-blocking: bytes accumulate in `parse_buf` across poll iterations
//! until a complete frame arrives. Connections that don't complete auth
//! within `AUTH_TIMEOUT` are dropped.
//!
//! # Thread model
//!
//! ```text
//! Core N:   DPDK poll thread  (rx_burst, smoltcp, frame decode, tx_burst)
//! Core 1:   Journal stage     (unchanged)
//! Core 2:   Matching stage    (unchanged)
//! Core 3:   Response stage    (encodes to SPSC queues instead of kernel sockets)
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use ed25519_dalek::{Verifier, VerifyingKey};
use melin_disruptor::ring;
use melin_dpdk::transport::DpdkTransport;
use melin_engine::journal::event::JournalEvent;
use melin_engine::journal::pipeline::InputSlot;
use melin_engine::journal::trace::trace_ts;
use melin_protocol::auth::{AuthorizedKeys, Permission};
use melin_protocol::codec;
use melin_protocol::message::{ConnectionId, Request, ResponseKind};
use smoltcp::iface::SocketHandle;
use tracing::debug;

use crate::dpdk_response::{ControlEvent, TxFrame};

/// Maximum frame payload size (matches epoll reader).
const MAX_FRAME_SIZE: usize = 1024;

/// Auth handshake timeout. Connections that don't complete auth within
/// this window are dropped.
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Auth handshake state machine. Drives the Challenge → ChallengeResponse
/// → ServerReady flow non-blockingly across poll iterations.
enum AuthState {
    /// Challenge frame has been queued for sending. Waiting for the
    /// ChallengeResponse frame from the client.
    WaitingForResponse {
        /// The nonce sent in the Challenge. Needed to verify the signature.
        nonce: [u8; 32],
        /// When the connection was accepted. Used for timeout.
        accepted_at: Instant,
    },
    /// Auth completed successfully. Connection is ready for trading.
    Authenticated { _permission: Permission },
}

/// Per-connection state in the DPDK poll thread.
struct ConnectionState {
    connection_id: ConnectionId,
    addr: SocketAddr,
    handle: SocketHandle,
    auth: AuthState,
    /// Incremental frame parsing state: accumulates bytes until a
    /// complete length-prefixed frame is available.
    parse_buf: Vec<u8>,
}

/// Run the DPDK poll loop.
///
/// This replaces the epoll reader pool. It accepts connections, drives
/// auth handshakes, parses frames, publishes events to the disruptor,
/// and drains the TX channel from the response stage into smoltcp sockets.
///
/// Called from a dedicated OS thread pinned to its own core.
pub fn run_dpdk_poll(
    mut transport: DpdkTransport,
    producer: ring::MultiProducer<InputSlot>,
    control_tx: mpsc::Sender<ControlEvent>,
    tx_rx: mpsc::Receiver<TxFrame>,
    shutdown: &AtomicBool,
    authorized_keys: Arc<AuthorizedKeys>,
) {
    // Map from smoltcp SocketHandle → connection state.
    let mut connections: HashMap<SocketHandle, ConnectionState> = HashMap::with_capacity(256);
    // Reverse map: connection_id → socket handle (for TX routing).
    let mut id_to_handle: HashMap<u64, SocketHandle> = HashMap::with_capacity(256);
    let mut next_connection_id: u64 = 1;

    // Scratch buffer for reading from smoltcp sockets.
    let mut read_buf = [0u8; MAX_FRAME_SIZE + 4];

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // 1. Poll NIC + smoltcp.
        transport.poll();

        // 2. Accept new connections and start auth handshake.
        for accepted in transport.take_accepted() {
            let conn_id = ConnectionId(next_connection_id);
            next_connection_id += 1;

            debug!(
                connection_id = conn_id.0,
                peer = %accepted.peer,
                "DPDK: new connection, starting auth"
            );

            // Generate a random nonce for the challenge.
            let mut nonce = [0u8; 32];
            getrandom::fill(&mut nonce).expect("getrandom failed");

            // Send the Challenge frame immediately.
            let mut challenge_buf = [0u8; 64];
            let written =
                codec::encode_response(&ResponseKind::Challenge { nonce }, &mut challenge_buf)
                    .expect("challenge encodes");
            transport.queue_send(accepted.handle, &challenge_buf[..written]);

            connections.insert(
                accepted.handle,
                ConnectionState {
                    connection_id: conn_id,
                    addr: accepted.peer,
                    handle: accepted.handle,
                    auth: AuthState::WaitingForResponse {
                        nonce,
                        accepted_at: Instant::now(),
                    },
                    parse_buf: Vec::with_capacity(MAX_FRAME_SIZE + 4),
                },
            );
        }

        // 3. Drain TX frames from the response stage into smoltcp sockets.
        while let Ok(frame) = tx_rx.try_recv() {
            if let Some(&handle) = id_to_handle.get(&frame.connection_id)
                && let Some(conn) = connections.get(&handle)
            {
                transport.queue_send(conn.handle, &frame.data);
            }
        }

        // 4. Read data from all connections and process.
        let handle_indices: Vec<SocketHandle> = connections.keys().copied().collect();

        for handle in handle_indices {
            let conn = match connections.get_mut(&handle) {
                Some(c) => c,
                None => continue,
            };

            // Check auth timeout for pending connections.
            if let AuthState::WaitingForResponse { accepted_at, .. } = &conn.auth
                && accepted_at.elapsed() > AUTH_TIMEOUT
            {
                debug!(
                    connection_id = conn.connection_id.0,
                    addr = %conn.addr,
                    "DPDK: auth timeout, dropping connection"
                );
                transport.close(conn.handle);
                connections.remove(&handle);
                continue;
            }

            // Read available data from smoltcp socket.
            let n = transport.recv(conn.handle, &mut read_buf);
            if n == 0 {
                if !transport.is_active(conn.handle) {
                    debug!(
                        connection_id = conn.connection_id.0,
                        addr = %conn.addr,
                        "DPDK: connection closed"
                    );
                    // Only notify response stage if auth completed.
                    if matches!(conn.auth, AuthState::Authenticated { .. }) {
                        let _ = control_tx.send(ControlEvent::Disconnected {
                            connection_id: conn.connection_id.0,
                        });
                    }
                    id_to_handle.remove(&conn.connection_id.0);
                    connections.remove(&handle);
                }
                continue;
            }

            conn.parse_buf.extend_from_slice(&read_buf[..n]);

            // Process frames based on auth state.
            match &conn.auth {
                AuthState::WaitingForResponse { .. } => {
                    // Try to extract the ChallengeResponse frame.
                    process_auth_frame(
                        conn,
                        &mut transport,
                        &authorized_keys,
                        &control_tx,
                        &mut id_to_handle,
                        handle,
                    );
                }
                AuthState::Authenticated { .. } => {
                    // Process trading frames.
                    process_trading_frames(
                        conn,
                        &mut transport,
                        &producer,
                        &control_tx,
                        &mut id_to_handle,
                    );
                }
            }
        }
    }
}

/// Process the auth handshake frame from a pending connection.
fn process_auth_frame(
    conn: &mut ConnectionState,
    transport: &mut DpdkTransport,
    authorized_keys: &AuthorizedKeys,
    control_tx: &mpsc::Sender<ControlEvent>,
    id_to_handle: &mut HashMap<u64, SocketHandle>,
    handle: SocketHandle,
) {
    // Need at least 4 bytes for the length prefix.
    if conn.parse_buf.len() < 4 {
        return;
    }

    let frame_len = u32::from_le_bytes([
        conn.parse_buf[0],
        conn.parse_buf[1],
        conn.parse_buf[2],
        conn.parse_buf[3],
    ]) as usize;

    // ChallengeResponse is 1 (tag) + 64 (signature) + 32 (pubkey) = 97 bytes.
    if frame_len > 256 {
        debug!(
            connection_id = conn.connection_id.0,
            frame_len, "DPDK: auth frame too large"
        );
        send_auth_failed(conn, transport);
        return;
    }

    if conn.parse_buf.len() < 4 + frame_len {
        return; // Incomplete — wait for more data.
    }

    let payload = conn.parse_buf[4..4 + frame_len].to_vec();
    conn.parse_buf.drain(..4 + frame_len);

    // Decode the ChallengeResponse.
    let request = match codec::decode_request(&payload) {
        Ok(req) => req,
        Err(e) => {
            debug!(
                connection_id = conn.connection_id.0,
                error = %e,
                "DPDK: auth decode error"
            );
            send_auth_failed(conn, transport);
            return;
        }
    };

    let (signature_bytes, public_key_bytes) = match request {
        Request::ChallengeResponse {
            signature,
            public_key,
        } => (signature, public_key),
        _ => {
            debug!(
                connection_id = conn.connection_id.0,
                "DPDK: expected ChallengeResponse, got something else"
            );
            send_auth_failed(conn, transport);
            return;
        }
    };

    // Look up the public key.
    let permission = match authorized_keys.lookup(&public_key_bytes) {
        Some(perm) => perm,
        None => {
            debug!(
                connection_id = conn.connection_id.0,
                "DPDK: unknown public key"
            );
            send_auth_failed(conn, transport);
            return;
        }
    };

    // Extract nonce from the current auth state.
    let nonce = match &conn.auth {
        AuthState::WaitingForResponse { nonce, .. } => *nonce,
        _ => unreachable!("process_auth_frame called in wrong state"),
    };

    // Verify the Ed25519 signature over the nonce.
    let verifying_key = match VerifyingKey::from_bytes(&public_key_bytes) {
        Ok(k) => k,
        Err(_) => {
            debug!(
                connection_id = conn.connection_id.0,
                "DPDK: invalid public key"
            );
            send_auth_failed(conn, transport);
            return;
        }
    };
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    if verifying_key.verify(&nonce, &signature).is_err() {
        debug!(
            connection_id = conn.connection_id.0,
            "DPDK: signature verification failed"
        );
        send_auth_failed(conn, transport);
        return;
    }

    // Auth succeeded — send ServerReady.
    let mut buf = [0u8; 16];
    let written =
        codec::encode_response(&ResponseKind::ServerReady, &mut buf).expect("ServerReady encodes");
    transport.queue_send(conn.handle, &buf[..written]);

    debug!(
        connection_id = conn.connection_id.0,
        addr = %conn.addr,
        permission = ?permission,
        "DPDK: authenticated"
    );

    // Transition to authenticated state.
    conn.auth = AuthState::Authenticated {
        _permission: permission,
    };

    // Register with the response stage and ID map.
    id_to_handle.insert(conn.connection_id.0, handle);
    let _ = control_tx.send(ControlEvent::Connected {
        connection_id: conn.connection_id.0,
    });
}

/// Send an AuthFailed response and close the connection.
fn send_auth_failed(conn: &ConnectionState, transport: &mut DpdkTransport) {
    let mut buf = [0u8; 16];
    if let Ok(written) = codec::encode_response(&ResponseKind::AuthFailed, &mut buf) {
        transport.queue_send(conn.handle, &buf[..written]);
    }
    // Don't close immediately — let smoltcp flush the AuthFailed frame first.
    // The connection will be cleaned up on the next poll when the client
    // disconnects or the auth timeout fires.
}

/// Process trading frames from an authenticated connection.
fn process_trading_frames(
    conn: &mut ConnectionState,
    transport: &mut DpdkTransport,
    producer: &ring::MultiProducer<InputSlot>,
    control_tx: &mpsc::Sender<ControlEvent>,
    id_to_handle: &mut HashMap<u64, SocketHandle>,
) {
    while conn.parse_buf.len() >= 4 {
        let frame_len = u32::from_le_bytes([
            conn.parse_buf[0],
            conn.parse_buf[1],
            conn.parse_buf[2],
            conn.parse_buf[3],
        ]) as usize;

        if frame_len > MAX_FRAME_SIZE {
            debug!(
                connection_id = conn.connection_id.0,
                frame_len, "DPDK: oversized frame, dropping connection"
            );
            transport.close(conn.handle);
            let _ = control_tx.send(ControlEvent::Disconnected {
                connection_id: conn.connection_id.0,
            });
            id_to_handle.remove(&conn.connection_id.0);
            // Caller must remove from connections map after return.
            conn.parse_buf.clear();
            break;
        }

        if conn.parse_buf.len() < 4 + frame_len {
            break; // Incomplete frame.
        }

        let payload = &conn.parse_buf[4..4 + frame_len];

        match codec::decode_request(payload) {
            Ok(request) => {
                if !matches!(
                    request,
                    Request::Heartbeat | Request::ChallengeResponse { .. }
                ) {
                    #[allow(clippy::let_unit_value)] // trace_ts() returns () without latency-trace
                    let recv_ts = trace_ts();
                    let event = request_to_event(&request);
                    producer.publish(InputSlot {
                        connection_id: conn.connection_id.0,
                        event,
                        #[allow(clippy::let_unit_value)]
                        publish_ts: trace_ts(),
                        recv_ts,
                    });
                }
            }
            Err(e) => {
                debug!(
                    connection_id = conn.connection_id.0,
                    error = %e,
                    "DPDK: decode error"
                );
            }
        }

        conn.parse_buf.drain(..4 + frame_len);
    }
}

/// Convert a decoded `Request` to a `JournalEvent`.
/// Mirrors the epoll reader's `request_to_event` — all variants are
/// mapped 1:1 except heartbeats/auth (filtered by the caller).
fn request_to_event(request: &Request) -> JournalEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => JournalEvent::SubmitOrder { symbol, order },
        Request::CancelOrder {
            symbol,
            account,
            order_id,
        } => JournalEvent::CancelOrder {
            symbol,
            account,
            order_id,
        },
        Request::CancelAll { account } => JournalEvent::CancelAll { account },
        Request::AddInstrument { spec } => JournalEvent::AddInstrument { spec },
        Request::Deposit {
            account,
            currency,
            amount,
        } => JournalEvent::Deposit {
            account,
            currency,
            amount,
        },
        Request::SetRiskLimits { symbol, limits } => JournalEvent::SetRiskLimits { symbol, limits },
        Request::SetCircuitBreaker { symbol, config } => {
            JournalEvent::SetCircuitBreaker { symbol, config }
        }
        Request::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => JournalEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        },
        Request::SetFeeSchedule { symbol, schedule } => {
            JournalEvent::SetFeeSchedule { symbol, schedule }
        }
        Request::QueryStats => JournalEvent::QueryStats,
        Request::Heartbeat | Request::ChallengeResponse { .. } => {
            unreachable!("heartbeats and auth filtered before request_to_event")
        }
    }
}
