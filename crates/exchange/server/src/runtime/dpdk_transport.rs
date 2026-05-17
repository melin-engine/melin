//! DPDK transport integration — single poll thread for NIC I/O + TCP.
//!
//! Replaces both the io_uring reader and the response stage's socket
//! writes. A single DPDK poll thread owns all NIC I/O:
//!
//! - **Inbound**: `rx_burst` → smoltcp → frame decode → disruptor publish
//! - **Outbound**: response SPSC → per-connection TX queue → smoltcp → `tx_burst`
//! - **Tick**: cadence comparison between bursts → `JournalEvent::Tick { now_ns }`
//!   onto the same input ring (see `run_dpdk_poll` for the single-poll-thread
//!   invariant assumed here).
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

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;

use crate::InputSlot;
use crate::JournalEvent;
use ed25519_dalek::{Verifier, VerifyingKey};
use melin_app::unix_epoch_nanos;
use melin_disruptor::ring;
use melin_dpdk::transport::DpdkTransport;
use melin_protocol::auth::{AuthorizedKeys, Permission};
use melin_protocol::codec;
use melin_protocol::message::{ConnectionId, Request, ResponseKind};
use melin_transport_core::trace::mono_trace_ns;
use rand::Rng;

use crate::request as shared_request;
use melin_dpdk::SocketHandle;
use tracing::{debug, warn};

use crate::dpdk_response::{ControlEvent, TxFrame};

/// Maximum frame payload size (matches reader).
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
    /// FxHash of the client's Ed25519 public key. Set after auth,
    /// copied into every InputSlot for per-key idempotency dedup.
    key_hash: u64,
    /// Incremental frame parsing state: accumulates bytes until a
    /// complete length-prefixed frame is available.
    parse_buf: Vec<u8>,
    /// Last time data was received from this connection. Used for
    /// idle timeout (drops connections that stop sending).
    last_activity: Instant,
}

/// Run the DPDK poll loop.
///
/// This replaces the io_uring reader. It accepts connections, drives
/// auth handshakes, parses frames, publishes events to the disruptor,
/// and drains the TX channel from the response stage into smoltcp sockets.
/// When `tick_cadence` is `Some`, this thread also generates the engine's
/// scheduler ticks via a wall-clock comparison between NIC bursts.
///
/// Called from a dedicated OS thread pinned to its own core.
///
/// **Single-poll-thread invariant.** Today `dpdk_num_queues` returns at
/// most 2 (one client queue + one replication-sender queue), so there is
/// always exactly one client poll thread. If we ever add multi-queue RSS
/// (multiple client poll threads), every poll thread would emit ticks
/// concurrently — re-introducing the multi-producer ordering race that
/// embedding tick into the poll thread was designed to eliminate. Gate
/// tick emission to a single thread (e.g. `thread_id == 0`) before
/// scaling out client queues.
///
/// Top-level thread entry point — the wide arg list mirrors transport
/// state owned elsewhere; bundling into a config struct adds indirection
/// without simplifying.
#[allow(clippy::too_many_arguments)]
pub fn run_dpdk_poll(
    mut transport: DpdkTransport,
    mut producer: ring::Producer<InputSlot>,
    control_tx: mpsc::Sender<ControlEvent>,
    mut tx_rx: melin_disruptor::spsc::Consumer<TxFrame>,
    shutdown: &AtomicBool,
    authorized_keys: Arc<AuthorizedKeys>,
    connection_timeout: Option<Duration>,
    tick_cadence: Option<Duration>,
    max_connections: u64,
    active_connections: Arc<std::sync::atomic::AtomicU64>,
    thread_id: u8,
    // Optional replication driver. When `Some`, this thread owns BOTH
    // client traffic on the trading port and replication traffic on the
    // configured replication listen port. Connections accepted on the
    // replication port are handed to the driver; per-iteration the
    // driver's `tick()` advances each replica slot's state machine
    // (handshake, journal catch-up, snapshot transfer, live streaming,
    // ack processing). Replaces the separate replication-sender thread —
    // see `feat/dpdk-single-queue` for the rationale (RSS routing on
    // iavf made the previous multi-queue split unworkable).
    repl_driver: Option<crate::replication::DpdkReplicationDriver>,
    // TCP port the replication driver listens on. Used to filter
    // `AcceptedConnection::listen_port` so client connections go to the
    // client handler and replication connections to the driver. Ignored
    // when `repl_driver` is None.
    repl_listen_port: u16,
) {
    let mut repl_driver = repl_driver;
    // Per-connection state indexed by `SocketHandle::index()`. Dense
    // `Vec<Option<_>>` beats a HashMap on the DPDK hot path: the HashMap
    // hashing + probe cost was the top remaining hotspot after the
    // clock-read cleanup. MAX_CONNECTIONS matches the SocketSet capacity,
    // so handle indices are always in-bounds.
    let mut connections: Vec<Option<ConnectionState>> =
        (0..melin_dpdk::MAX_CONNECTIONS).map(|_| None).collect();
    // Reverse lookup from response-stage connection_id to SocketHandle.
    // FxHash instead of SipHash — u64 keys, no HashDoS surface internally.
    let mut id_to_handle: FxHashMap<u64, SocketHandle> =
        FxHashMap::with_capacity_and_hasher(256, Default::default());
    let mut next_connection_id: u64 = 1;
    // Occupied-slot count for the max_connections gate. Cheaper than
    // scanning `connections` on every accept.
    let mut connection_count: usize = 0;
    // Exclusive upper bound of the `connections` range that has ever
    // been used. Lets the per-poll loop scan just the active range
    // instead of all MAX_CONNECTIONS slots — skipping ~1000 None slots
    // per poll was a chunk of `run_dpdk_poll` self-time. smoltcp reuses
    // closed slab indices before extending, so this grows to the steady-
    // state watermark and stays there.
    let mut conn_range_end: usize = 0;

    // Reader-stage histogram. Single sample per published frame
    // covering recv_ts → batch.push_with completion (decode + auth
    // dispatch + slot construction + push). DPDK has no separate
    // publish-call histogram because `batch.push_with` is a slot
    // assignment in a pre-allocated batch — the work is dominated by
    // the surrounding decode + dedup, which is what `ingest` measures.
    #[cfg(feature = "tick-to-trade")]
    let mut ingest_rec =
        melin_transport_core::trace::register_stage("reader: ingest (recv_ts → publish complete)");

    // Pre-allocated parse buffer pool. Avoids heap allocation on accept
    // by recycling buffers from disconnected connections.
    let mut parse_buf_pool: Vec<Vec<u8>> = (0..256)
        .map(|_| Vec::with_capacity(MAX_FRAME_SIZE + 4))
        .collect();

    // Fast PRNG for auth nonces. Seeded from OS entropy once at startup,
    // then generates nonces without blocking. Auth nonces don't need
    // CSPRNG-grade randomness — they prevent replay attacks within a
    // session, not cryptographic key derivation.
    let mut rng = rand::rng();

    // Tick generator state. The DPDK poll thread is a tight busy-spin
    // loop, so unlike the io_uring reader it does not need a timeout
    // primitive — a wall-clock comparison between bursts is enough.
    // `tick_check_interval` amortises `Instant::now()` over many poll
    // iterations: at typical ~5M iterations/sec, checking every 4096
    // iterations stays well within microseconds of the deadline while
    // making the per-iteration overhead unmeasurable.
    let tick_enabled = tick_cadence.is_some();
    let cadence = tick_cadence.unwrap_or(Duration::ZERO);
    let mut next_tick_deadline = Instant::now() + cadence;
    let mut last_tick_ns: u64 = 0;
    let mut tick_check_counter: u32 = 0;
    const TICK_CHECK_INTERVAL: u32 = 4096;

    // Auth / idle-timeout checks call `Instant::now()` via `elapsed()`,
    // which showed up as ~13% of the poll core in perf. The timeouts are
    // ~seconds, so firing the check once per ~1M polls (~100ms at 10M
    // polls/sec) is still orders of magnitude tighter than the deadline.
    let mut slow_check_counter: u32 = 0;
    const SLOW_CHECK_INTERVAL: u32 = 1024 * 1024;
    if tick_enabled {
        tracing::info!(
            cadence_ms = cadence.as_millis() as u64,
            thread_id,
            "tick generator integrated into DPDK poll thread"
        );
    }

    // Outer-loop wall-time histogram, gated on at least one byte received
    // this iteration. Idle iterations would otherwise drown the percentiles
    // in ~100ns samples; what we want is "how long does a poll cycle take
    // when there's actual work to do" — which is the cycle an in-flight
    // order experiences. Registered with the global stats registry; the
    // /stats-dump endpoint snapshots it alongside the other stages.
    #[cfg(feature = "latency-trace")]
    let mut poll_iter_rec = melin_transport_core::trace::register_stage(
        "dpdk poll: outer iteration (work-iterations only)",
    );
    #[cfg(feature = "latency-trace")]
    let mut poll_iter_start = mono_trace_ns();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Set on any bytes received this iteration; gates poll_iter_hist
        // recording so idle iterations don't drown the percentiles.
        #[cfg(feature = "latency-trace")]
        let mut work_done_this_iter = false;

        // Tick generator: compare wall clock to deadline once every
        // TICK_CHECK_INTERVAL poll iterations, emit if due.
        if tick_enabled {
            tick_check_counter = tick_check_counter.wrapping_add(1);
            if tick_check_counter >= TICK_CHECK_INTERVAL {
                tick_check_counter = 0;
                let now = Instant::now();
                if now >= next_tick_deadline {
                    let raw_now_ns = unix_epoch_nanos();
                    let now_ns =
                        melin_transport_core::tick::clamp_monotonic(raw_now_ns, last_tick_ns);
                    last_tick_ns = now_ns;
                    melin_transport_core::tick::publish_tick(&mut producer, now_ns);
                    let elapsed = Instant::now().saturating_duration_since(next_tick_deadline);
                    next_tick_deadline = if elapsed > cadence {
                        Instant::now() + cadence
                    } else {
                        next_tick_deadline + cadence
                    };
                }
            }
        }

        // 1. Poll NIC + smoltcp.
        transport.poll();

        // 2. Accept new connections — dispatch by listen port so the
        //    replication driver gets its own connections, client logic
        //    only sees trading-port connections.
        for accepted in transport.take_accepted() {
            if let Some(ref mut driver) = repl_driver
                && accepted.listen_port == repl_listen_port
            {
                driver.accept_connection(accepted.peer, accepted.handle, &mut transport);
                continue;
            }

            // Enforce max_connections limit.
            if max_connections > 0 && connection_count as u64 >= max_connections {
                warn!(
                    peer = %accepted.peer,
                    "DPDK: connection rejected: max_connections reached"
                );
                transport.close(accepted.handle);
                continue;
            }

            // Encode thread_id in bits 56..63 of connection_id for O(1)
            // response routing. Bits 0..55 are the per-thread sequence.
            let conn_id = ConnectionId((thread_id as u64) << 56 | next_connection_id);
            next_connection_id += 1;

            debug!(
                connection_id = conn_id.0,
                peer = %accepted.peer,
                "DPDK: new connection, starting auth"
            );

            // Generate a random nonce for the challenge. Uses a fast PRNG
            // instead of getrandom to avoid blocking the poll thread on
            // kernel entropy.
            let nonce: [u8; 32] = rng.random();

            // Send the Challenge frame immediately.
            let mut challenge_buf = [0u8; 128];
            let written =
                codec::encode_response(&ResponseKind::Challenge { nonce }, &mut challenge_buf)
                    .expect("challenge encodes");
            transport.queue_send(accepted.handle, &challenge_buf[..written]);

            let accepted_idx = accepted.handle.index();
            if accepted_idx + 1 > conn_range_end {
                conn_range_end = accepted_idx + 1;
            }
            connections[accepted_idx] = Some(ConnectionState {
                connection_id: conn_id,
                addr: accepted.peer,
                handle: accepted.handle,
                auth: AuthState::WaitingForResponse {
                    nonce,
                    accepted_at: Instant::now(),
                },
                key_hash: 0,
                // Reuse a pre-allocated buffer from the pool, or allocate
                // if the pool is exhausted (more connections than pre-allocated).
                parse_buf: parse_buf_pool
                    .pop()
                    .unwrap_or_else(|| Vec::with_capacity(MAX_FRAME_SIZE + 4)),
                last_activity: Instant::now(),
            });
            connection_count += 1;
        }

        // 3. Drain TX frames from the response stage into smoltcp sockets.
        // Lock-free SPSC — no mutex contention on the hot path.
        // Single HashMap lookup (id_to_handle) instead of two.
        while let Some((_seq, frame)) = tx_rx.try_consume() {
            if let Some(&handle) = id_to_handle.get(&frame.connection_id)
                && !transport.queue_send(handle, frame.as_bytes())
            {
                // TX queue overflow — client fell behind. Drop connection.
                debug!(
                    connection_id = frame.connection_id,
                    "DPDK: TX queue overflow, dropping connection"
                );
                transport.close(handle);
                let _ = control_tx.send(ControlEvent::Disconnected {
                    connection_id: frame.connection_id,
                });
                id_to_handle.remove(&frame.connection_id);
                if let Some(mut removed) = connections[handle.index()].take() {
                    removed.parse_buf.clear();
                    parse_buf_pool.push(removed.parse_buf);
                    connection_count -= 1;
                }
            }
        }

        // 4. Read data from all connections and process.
        // Mid-iteration poll every N connections to keep the NIC busy —
        // flush TX responses and receive new data without waiting for
        // the full connection iteration to complete.
        const POLL_EVERY_N_CONNS: usize = 4;

        // One wall-clock read per outer poll iteration, reused for
        // every request stamped in this pass. Sub-microsecond precision
        // loss at DPDK poll rates; order timestamps are for reporting,
        // not matching (the engine orders by sequence). Deferred until
        // we actually stamp a frame — `clock_gettime` dominates the
        // profile on idle polls with no traffic.
        let mut batch_wall_ns: Option<u64> = None;

        slow_check_counter = slow_check_counter.wrapping_add(1);
        let do_slow_checks = slow_check_counter.is_multiple_of(SLOW_CHECK_INTERVAL);

        // Batch all trading-frame publishes from this outer poll iteration
        // into a single disruptor cursor release. Perf annotate showed the
        // per-publish release store at ~9% of this core's cycles; one
        // store per outer iteration (covering all decoded events across
        // all connections) amortises that cost.
        let mut batch = producer.batch();

        // Counts occupied slots we actually process, to drive the
        // mid-iteration `transport.poll()` cadence.
        let mut active_idx: usize = 0;
        // Indexed loop, not iter_mut: remove paths do `connections[idx].take()`
        // which conflicts with an outer iterator borrow. Bounded by
        // `conn_range_end` (not `connections.len()`) so idle polls don't
        // scan MAX_CONNECTIONS empty slots.
        #[allow(clippy::needless_range_loop)]
        for idx in 0..conn_range_end {
            if connections[idx].is_none() {
                continue;
            }
            if active_idx > 0 && active_idx.is_multiple_of(POLL_EVERY_N_CONNS) {
                transport.poll();
                // Drive the replication driver at the same cadence as the
                // mid-loop NIC poll. Without this, `tick()` only fires once
                // per outer iteration; under heavy client load the journal
                // stage produces replication batches faster than the driver
                // drains the ring, and the journal evicts both replicas with
                // "ring backpressure timeout".
                if let Some(ref mut driver) = repl_driver {
                    driver.tick(&mut transport, shutdown);
                }
            }
            active_idx += 1;
            let conn = match connections[idx].as_mut() {
                Some(c) => c,
                None => continue,
            };
            // SocketHandle is Copy; capture before any &mut conn borrows
            // so process_auth_frame can take it after the conn borrow.
            let conn_handle = conn.handle;

            // Check auth timeout for pending connections. Throttled to
            // avoid a per-poll `Instant::now()` via `elapsed()`.
            if do_slow_checks
                && let AuthState::WaitingForResponse { accepted_at, .. } = &conn.auth
                && accepted_at.elapsed() > AUTH_TIMEOUT
            {
                debug!(
                    connection_id = conn.connection_id.0,
                    addr = %conn.addr,
                    "DPDK: auth timeout, dropping connection"
                );
                transport.close(conn.handle);
                if let Some(mut removed) = connections[idx].take() {
                    removed.parse_buf.clear();
                    parse_buf_pool.push(removed.parse_buf);
                    connection_count -= 1;
                }
                continue;
            }

            // Guard against unbounded buffer growth — check before recv.
            const MAX_PARSE_BUF: usize = 65536;
            if conn.parse_buf.len() >= MAX_PARSE_BUF {
                debug!(
                    connection_id = conn.connection_id.0,
                    buf_len = conn.parse_buf.len(),
                    "parse buffer exceeded limit, dropping connection"
                );
                transport.close(conn.handle);
                let _ = control_tx.send(ControlEvent::Disconnected {
                    connection_id: conn.connection_id.0,
                });
                id_to_handle.remove(&conn.connection_id.0);
                if let Some(mut removed) = connections[idx].take() {
                    removed.parse_buf.clear();
                    parse_buf_pool.push(removed.parse_buf);
                    connection_count -= 1;
                }
                continue;
            }

            // Read directly into parse_buf, skipping the intermediate
            // read_buf stack copy.
            let n = transport.recv_into_vec(conn.handle, &mut conn.parse_buf);
            if n > 0 {
                conn.last_activity = Instant::now();
                #[cfg(feature = "latency-trace")]
                {
                    work_done_this_iter = true;
                }
            }
            if n == 0 {
                if !transport.is_active(conn.handle) {
                    debug!(
                        connection_id = conn.connection_id.0,
                        addr = %conn.addr,
                        "DPDK: connection closed"
                    );
                    if matches!(conn.auth, AuthState::Authenticated { .. }) {
                        let _ = control_tx.send(ControlEvent::Disconnected {
                            connection_id: conn.connection_id.0,
                        });
                    }
                    transport.close(conn.handle);
                    id_to_handle.remove(&conn.connection_id.0);
                    if let Some(mut removed) = connections[idx].take() {
                        removed.parse_buf.clear();
                        parse_buf_pool.push(removed.parse_buf);
                        connection_count -= 1;
                    }
                }
                // Check idle timeout when no data was received. Throttled
                // to avoid a per-poll `Instant::now()` via `elapsed()`.
                else if do_slow_checks
                    && let Some(timeout) = connection_timeout
                    && matches!(conn.auth, AuthState::Authenticated { .. })
                    && conn.last_activity.elapsed() > timeout
                {
                    debug!(
                        connection_id = conn.connection_id.0,
                        addr = %conn.addr,
                        "DPDK: idle timeout, dropping connection"
                    );
                    transport.close(conn.handle);
                    let _ = control_tx.send(ControlEvent::Disconnected {
                        connection_id: conn.connection_id.0,
                    });
                    id_to_handle.remove(&conn.connection_id.0);
                    if let Some(mut removed) = connections[idx].take() {
                        removed.parse_buf.clear();
                        parse_buf_pool.push(removed.parse_buf);
                        connection_count -= 1;
                    }
                }
                continue;
            }

            // Process frames based on auth state.
            let was_waiting = matches!(conn.auth, AuthState::WaitingForResponse { .. });
            match &conn.auth {
                AuthState::WaitingForResponse { .. } => {
                    // Try to extract the ChallengeResponse frame.
                    process_auth_frame(
                        conn,
                        &mut transport,
                        &authorized_keys,
                        &control_tx,
                        &mut id_to_handle,
                        conn_handle,
                    );
                }
                AuthState::Authenticated { .. } => {
                    // Process trading frames.
                    process_trading_frames(
                        conn,
                        &mut transport,
                        &mut batch,
                        &control_tx,
                        &mut id_to_handle,
                        *batch_wall_ns.get_or_insert_with(unix_epoch_nanos),
                        #[cfg(feature = "tick-to-trade")]
                        &mut ingest_rec,
                    );
                }
            }

            // Track auth → authenticated transition for the connection counter.
            if was_waiting && matches!(conn.auth, AuthState::Authenticated { .. }) {
                active_connections.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Single release store advances the producer cursor by all events
        // batched across this outer poll iteration.
        batch.commit();

        // Drive the replication driver's per-iteration work — handshake
        // progression, journal catch-up (blocking on first connect),
        // ack processing, and live data-batch sends. With a single
        // queue + single thread, this is what replaces the previous
        // dedicated repl-sender thread; transport.poll() above flushed
        // any TX the driver queued on the prior iteration.
        if let Some(ref mut driver) = repl_driver {
            driver.tick(&mut transport, shutdown);
        }

        #[cfg(feature = "latency-trace")]
        {
            let now = mono_trace_ns();
            // Skip records once shutdown has been observed: matches the
            // gate on the journal / matching / response stages and keeps
            // diagnostic numbers comparable across runs.
            if work_done_this_iter && !shutdown.load(Ordering::Relaxed) {
                poll_iter_rec.record_elapsed(poll_iter_start, now);
            }
            poll_iter_start = now;
        }
    }
}

/// Process the auth handshake frame from a pending connection.
fn process_auth_frame(
    conn: &mut ConnectionState,
    transport: &mut DpdkTransport,
    authorized_keys: &AuthorizedKeys,
    control_tx: &mpsc::Sender<ControlEvent>,
    id_to_handle: &mut FxHashMap<u64, SocketHandle>,
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

    // Borrow the payload directly — no heap allocation. Compact after processing.
    let consumed = 4 + frame_len;

    // Decode the ChallengeResponse (seq is ignored during auth).
    let (_seq, request) = match codec::decode_request(&conn.parse_buf[4..consumed]) {
        Ok(req) => req,
        Err(e) => {
            debug!(
                connection_id = conn.connection_id.0,
                error = %e,
                "DPDK: auth decode error"
            );
            // Compact before returning.
            let remaining = conn.parse_buf.len() - consumed;
            conn.parse_buf.copy_within(consumed.., 0);
            conn.parse_buf.truncate(remaining);
            send_auth_failed(conn, transport);
            return;
        }
    };

    // Compact parse buffer now that the borrow is released.
    // Single memmove instead of drain()'s per-byte shift.
    let remaining = conn.parse_buf.len() - consumed;
    conn.parse_buf.copy_within(consumed.., 0);
    conn.parse_buf.truncate(remaining);

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

    // Extract the nonce captured at Challenge-send time and feed it
    // back into the signing payload now.
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
    let signing_payload = melin_protocol::auth::auth_signing_payload(&nonce);
    if verifying_key.verify(&signing_payload, &signature).is_err() {
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

    // Compute key hash for per-key idempotency dedup.
    use std::hash::{Hash, Hasher};
    let key_hash = {
        let mut hasher = rustc_hash::FxHasher::default();
        public_key_bytes.hash(&mut hasher);
        hasher.finish()
    };

    // Transition to authenticated state.
    conn.key_hash = key_hash;
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
///
/// Uses a cursor to avoid O(n) drain/memmove on every frame. The buffer
/// is compacted once after all frames in this batch are processed.
/// Extract trading frames from `conn.parse_buf` and publish decoded
/// `InputSlot`s directly into the disruptor batch. `batch_wall_ns` is
/// captured once per outer poll iteration by the caller; all
/// non-query requests stamped in this call share it, sparing a
/// per-request `clock_gettime(CLOCK_REALTIME)` on the hot path.
///
/// Uses `Batch::push_with` so all events from all connections committed
/// in this outer iteration advance the producer cursor with a single
/// release store — amortising the ~9% ingress-core cost perf annotate
/// pinned on the per-publish cursor write.
fn process_trading_frames(
    conn: &mut ConnectionState,
    transport: &mut DpdkTransport,
    batch: &mut ring::Batch<'_, InputSlot>,
    control_tx: &mpsc::Sender<ControlEvent>,
    id_to_handle: &mut FxHashMap<u64, SocketHandle>,
    batch_wall_ns: u64,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) {
    let mut cursor = 0;

    loop {
        let remaining = &conn.parse_buf[cursor..];
        let frame_len = match try_extract_frame(remaining) {
            FrameResult::Complete(payload) => payload.len(),
            FrameResult::Incomplete => break,
            FrameResult::Oversized(len) => {
                debug!(
                    connection_id = conn.connection_id.0,
                    frame_len = len,
                    "DPDK: oversized frame, dropping connection"
                );
                transport.close(conn.handle);
                let _ = control_tx.send(ControlEvent::Disconnected {
                    connection_id: conn.connection_id.0,
                });
                id_to_handle.remove(&conn.connection_id.0);
                conn.parse_buf.clear();
                return;
            }
        };

        let payload = &conn.parse_buf[cursor + 4..cursor + 4 + frame_len];

        match codec::decode_request(payload) {
            Ok((seq, request)) => {
                if !shared_request::should_filter(&request) {
                    #[allow(clippy::let_unit_value)]
                    // mono_trace_ns() returns () without latency-trace
                    let recv_ts = mono_trace_ns();
                    let event = shared_request::to_event(&request);
                    // Sequence is allocated by the journal stage in
                    // disruptor cursor order — see `InputSlot::sequence`.
                    let ts = if matches!(
                        event,
                        JournalEvent::App(melin_trading::trading_event::TradingEvent::QueryStats)
                            | JournalEvent::App(
                                melin_trading::trading_event::TradingEvent::QueryPosition { .. }
                            )
                    ) {
                        0
                    } else {
                        batch_wall_ns
                    };
                    let connection_id = conn.connection_id.0;
                    let key_hash = conn.key_hash;
                    #[allow(clippy::let_unit_value)]
                    let publish_ts = mono_trace_ns();
                    batch.push_with(|slot| {
                        slot.connection_id = connection_id;
                        slot.key_hash = key_hash;
                        slot.request_seq = seq;
                        slot.sequence = 0;
                        slot.timestamp_ns = ts;
                        slot.event = event;
                        slot.publish_ts = publish_ts;
                        slot.recv_ts = recv_ts;
                    });
                    #[cfg(feature = "tick-to-trade")]
                    ingest_rec.record_elapsed(recv_ts, mono_trace_ns());
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

        cursor += 4 + frame_len;
    }

    // Compact: shift remaining bytes to front. Single memmove for the
    // entire batch instead of one per frame.
    if cursor > 0 {
        let remaining = conn.parse_buf.len() - cursor;
        conn.parse_buf.copy_within(cursor.., 0);
        conn.parse_buf.truncate(remaining);
    }
}

/// Result of trying to extract a length-prefixed frame from a parse buffer.
#[derive(Debug, PartialEq)]
enum FrameResult<'a> {
    /// Complete frame extracted. Payload is the frame data (without length prefix).
    Complete(&'a [u8]),
    /// Not enough data yet — need more bytes.
    Incomplete,
    /// Frame length exceeds MAX_FRAME_SIZE — connection should be dropped.
    Oversized(usize),
}

/// Try to extract a length-prefixed frame from a parse buffer.
///
/// Wire format: `[u32 little-endian length][payload]`.
/// Returns the payload slice on success, or Incomplete/Oversized.
fn try_extract_frame(buf: &[u8]) -> FrameResult<'_> {
    if buf.len() < 4 {
        return FrameResult::Incomplete;
    }

    let frame_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;

    if frame_len > MAX_FRAME_SIZE {
        return FrameResult::Oversized(frame_len);
    }

    if buf.len() < 4 + frame_len {
        return FrameResult::Incomplete;
    }

    FrameResult::Complete(&buf[4..4 + frame_len])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    use crate::JournalEvent;
    use melin_types::types::*;

    // --- try_extract_frame tests ---

    #[test]
    fn extract_frame_empty_buffer() {
        assert_eq!(try_extract_frame(&[]), FrameResult::Incomplete);
    }

    #[test]
    fn extract_frame_partial_length() {
        assert_eq!(try_extract_frame(&[0x05, 0x00]), FrameResult::Incomplete);
    }

    #[test]
    fn extract_frame_length_only_no_payload() {
        // Length says 5 bytes, but no payload present.
        assert_eq!(
            try_extract_frame(&[0x05, 0x00, 0x00, 0x00]),
            FrameResult::Incomplete
        );
    }

    #[test]
    fn extract_frame_partial_payload() {
        // Length says 5 bytes, only 3 payload bytes present.
        assert_eq!(
            try_extract_frame(&[0x05, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC]),
            FrameResult::Incomplete
        );
    }

    #[test]
    fn extract_frame_complete() {
        let buf = [0x03, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC];
        assert_eq!(
            try_extract_frame(&buf),
            FrameResult::Complete(&[0xAA, 0xBB, 0xCC])
        );
    }

    #[test]
    fn extract_frame_complete_with_trailing_data() {
        // 3-byte payload + extra bytes (next frame).
        let buf = [0x03, 0x00, 0x00, 0x00, 0xAA, 0xBB, 0xCC, 0xFF, 0xFF];
        assert_eq!(
            try_extract_frame(&buf),
            FrameResult::Complete(&[0xAA, 0xBB, 0xCC])
        );
    }

    #[test]
    fn extract_frame_zero_length() {
        // Zero-length frame is valid (empty payload).
        let buf = [0x00, 0x00, 0x00, 0x00];
        assert_eq!(try_extract_frame(&buf), FrameResult::Complete(&[]));
    }

    #[test]
    fn extract_frame_oversized() {
        // Frame length exceeds MAX_FRAME_SIZE (1024).
        let len = (MAX_FRAME_SIZE + 1) as u32;
        let buf = len.to_le_bytes();
        assert_eq!(
            try_extract_frame(&buf),
            FrameResult::Oversized(MAX_FRAME_SIZE + 1)
        );
    }

    #[test]
    fn extract_frame_exactly_max_size() {
        // Frame length exactly at MAX_FRAME_SIZE should succeed (not oversized).
        let len = MAX_FRAME_SIZE as u32;
        let mut buf = Vec::from(len.to_le_bytes().as_slice());
        buf.extend(vec![0u8; MAX_FRAME_SIZE]);
        assert!(matches!(try_extract_frame(&buf), FrameResult::Complete(_)));
    }

    // --- request_to_event tests ---

    fn make_order(id: u64, account: u32, side: Side) -> Order {
        Order {
            id: OrderId(id),
            account: AccountId(account),
            side,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(100).unwrap()),
                post_only: false,
            },
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            time_in_force: TimeInForce::GTC,
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        }
    }

    #[test]
    fn request_to_event_submit_order() {
        let order = make_order(1, 1, Side::Buy);
        let req = Request::SubmitOrder {
            symbol: Symbol(1),
            order,
        };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::SubmitOrder { symbol, .. }) if symbol == Symbol(1))
        );
    }

    #[test]
    fn request_to_event_cancel_order() {
        let req = Request::CancelOrder {
            symbol: Symbol(2),
            account: AccountId(5),
            order_id: OrderId(42),
        };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelOrder { symbol, account, order_id })
                if symbol == Symbol(2) && account == AccountId(5) && order_id == OrderId(42))
        );
    }

    #[test]
    fn request_to_event_cancel_all() {
        let req = Request::CancelAll {
            account: AccountId(7),
        };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelAll { account }) if account == AccountId(7))
        );
    }

    #[test]
    fn request_to_event_deposit() {
        let req = Request::Deposit {
            account: AccountId(1),
            currency: CurrencyId(2),
            amount: 1000,
        };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::Deposit { account, currency, amount })
                if account == AccountId(1) && currency == CurrencyId(2) && amount == 1000)
        );
    }

    #[test]
    fn request_to_event_add_instrument() {
        let spec = InstrumentSpec {
            symbol: Symbol(10),
            base: CurrencyId(1),
            quote: CurrencyId(2),
        };
        let req = Request::AddInstrument { spec };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::AddInstrument { spec: s }) if s.symbol == Symbol(10))
        );
    }

    #[test]
    fn request_to_event_cancel_replace() {
        let req = Request::CancelReplace {
            symbol: Symbol(1),
            account: AccountId(1),
            order_id: OrderId(5),
            new_price: Price(NonZeroU64::new(200).unwrap()),
            new_quantity: Quantity(NonZeroU64::new(50).unwrap()),
        };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelReplace { order_id, .. }) if order_id == OrderId(5))
        );
    }

    #[test]
    fn request_to_event_set_risk_limits() {
        let req = Request::SetRiskLimits {
            symbol: Symbol(1),
            limits: RiskLimits::default(),
        };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::SetRiskLimits { symbol, .. }) if symbol == Symbol(1))
        );
    }

    #[test]
    fn request_to_event_set_circuit_breaker() {
        let req = Request::SetCircuitBreaker {
            symbol: Symbol(1),
            config: CircuitBreakerConfig::default(),
        };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::SetCircuitBreaker { symbol, .. }) if symbol == Symbol(1))
        );
    }

    #[test]
    fn request_to_event_set_fee_schedule() {
        let req = Request::SetFeeSchedule {
            symbol: Symbol(3),
            schedule: FeeSchedule::default(),
        };
        let event = shared_request::to_event(&req);
        assert!(
            matches!(event, JournalEvent::App(melin_trading::trading_event::TradingEvent::SetFeeSchedule { symbol, .. }) if symbol == Symbol(3))
        );
    }

    #[test]
    fn request_to_event_query_stats() {
        let req = Request::QueryStats;
        let event = shared_request::to_event(&req);
        assert!(matches!(
            event,
            JournalEvent::App(melin_trading::trading_event::TradingEvent::QueryStats)
        ));
    }

    #[test]
    #[should_panic(expected = "must be filtered before to_event")]
    fn request_to_event_heartbeat_panics() {
        shared_request::to_event(&Request::Heartbeat);
    }

    #[test]
    #[should_panic(expected = "must be filtered before to_event")]
    fn request_to_event_challenge_response_panics() {
        shared_request::to_event(&Request::ChallengeResponse {
            signature: [0u8; 64],
            public_key: [0u8; 32],
        });
    }

    // --- Wire-level round-trip: encode request → extract frame → decode ---

    #[test]
    fn wire_round_trip_submit_order() {
        let order = make_order(99, 1, Side::Sell);
        let req = Request::SubmitOrder {
            symbol: Symbol(5),
            order,
        };
        let mut buf = [0u8; 256];
        let written = codec::encode_request(&req, 0, &mut buf).unwrap();

        // encode_request writes [u32 length][payload].
        let frame = try_extract_frame(&buf[..written]);
        match frame {
            FrameResult::Complete(payload) => {
                let (_, decoded) = codec::decode_request(payload).unwrap();
                assert!(
                    matches!(decoded, Request::SubmitOrder { symbol, .. } if symbol == Symbol(5))
                );
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn wire_round_trip_cancel() {
        let req = Request::CancelOrder {
            symbol: Symbol(1),
            account: AccountId(2),
            order_id: OrderId(3),
        };
        let mut buf = [0u8; 256];
        let written = codec::encode_request(&req, 0, &mut buf).unwrap();

        let frame = try_extract_frame(&buf[..written]);
        match frame {
            FrameResult::Complete(payload) => {
                let (_, decoded) = codec::decode_request(payload).unwrap();
                assert!(
                    matches!(decoded, Request::CancelOrder { order_id, .. } if order_id == OrderId(3))
                );
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    // --- Incremental accumulation (simulates TCP byte-at-a-time arrival) ---

    #[test]
    fn incremental_frame_accumulation() {
        let req = Request::Heartbeat;
        let mut wire = [0u8; 64];
        let written = codec::encode_request(&req, 0, &mut wire).unwrap();
        let wire = &wire[..written];

        // Feed bytes one at a time into a parse buffer.
        let mut parse_buf = Vec::new();
        for (i, &byte) in wire.iter().enumerate() {
            parse_buf.push(byte);
            let result = try_extract_frame(&parse_buf);
            if i < written - 1 {
                assert_eq!(result, FrameResult::Incomplete, "byte {i}");
            } else {
                assert!(
                    matches!(result, FrameResult::Complete(_)),
                    "expected Complete at final byte"
                );
            }
        }
    }

    // --- Multiple frames back-to-back ---

    #[test]
    fn multiple_frames_in_buffer() {
        let req1 = Request::Heartbeat;
        let req2 = Request::QueryStats;
        let mut buf1 = [0u8; 64];
        let mut buf2 = [0u8; 64];
        let w1 = codec::encode_request(&req1, 0, &mut buf1).unwrap();
        let w2 = codec::encode_request(&req2, 0, &mut buf2).unwrap();

        // Concatenate two frames.
        let mut combined = Vec::new();
        combined.extend_from_slice(&buf1[..w1]);
        combined.extend_from_slice(&buf2[..w2]);

        // First extraction should get frame 1.
        let result1 = try_extract_frame(&combined);
        let payload1_len = match result1 {
            FrameResult::Complete(p) => {
                let (_, decoded) = codec::decode_request(p).unwrap();
                assert!(matches!(decoded, Request::Heartbeat));
                p.len()
            }
            other => panic!("expected Complete, got {other:?}"),
        };

        // Advance past first frame (4 bytes length + payload).
        let remaining = &combined[4 + payload1_len..];

        // Second extraction should get frame 2.
        let result2 = try_extract_frame(remaining);
        match result2 {
            FrameResult::Complete(p) => {
                let (_, decoded) = codec::decode_request(p).unwrap();
                assert!(matches!(decoded, Request::QueryStats));
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }
}
