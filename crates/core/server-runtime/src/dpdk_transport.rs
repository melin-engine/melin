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
//! New connections start in `AuthState::WaitingForResponse` — the poll loop
//! sends a Challenge frame and waits for the ChallengeResponse. Auth is
//! non-blocking: bytes accumulate in `parse_buf` across poll iterations
//! until a complete frame arrives. Connections that don't complete auth
//! within `AUTH_TIMEOUT` are dropped. A failed attempt is answered with
//! `AuthFailed` and the connection is closed once the peer has it.
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

use melin_app::Application;
use melin_app::auth::{AuthorizedKeys, ClientRole, KeyRole, RoleId};
use melin_app::unix_epoch_nanos;
use melin_dpdk::transport::DpdkTransport;
use melin_pipeline::ring;
use melin_transport_core::health::IngressMetrics;
use melin_transport_core::pipeline::InputSlot;
use melin_transport_core::tick::TickSchedule;
#[cfg(feature = "latency-trace")]
use melin_transport_core::trace::mono_trace_ns;
use melin_wire_protocol::control::ConnectionId;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;
use melin_wire_protocol::framing::split_frame_limited;
use rand::RngExt;

use melin_dpdk::SocketHandle;
use tracing::{debug, warn};

use crate::dpdk_response::{ControlEvent, TxFrame, lane_connection_id};
use crate::early_reply::EarlyReplySender;
use crate::halt::HaltGate;
use crate::policy_control::AckPolicyPublisher;

use crate::client_auth::MAX_AUTH_FRAME;
use crate::client_frames::{ClientIngress, LongParkReport, MAX_FRAME_SIZE};

/// Auth handshake timeout. Connections that don't complete auth within
/// this window are dropped.
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

/// A connection whose buffer holds this many bytes before a receive is
/// dropped: a bound on what one client can make the node hold. Not a
/// parked connection, though: its buffer is the node's backlog, which a
/// single receive can take past this bound before the park (the socket's
/// receive buffer is sized apart, `--dpdk-client-rx-buf-kib`).
///
/// A connection that is read leaves less than one frame buffered (its
/// frames are parsed until one is incomplete, or it parks or closes), so
/// the one producer left is the receive that completes the auth frame:
/// what a client pipelined behind it in that receive waits a poll
/// iteration unparsed, and this bound drops it at that point.
const MAX_PARSE_BUF: usize = 65536;

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
    /// Auth completed successfully. Connection is ready for requests.
    Authenticated {
        role: ClientRole<RoleId>,
        /// Since when the connection has been parked (see `client_frames`):
        /// an early reply had nowhere to go, so its buffer is retried every
        /// `PARK_RETRY_INTERVAL` poll iterations and nothing more is read
        /// from it until the retry gets through. `None` while it is read
        /// as usual. Here rather than beside `auth`, so only an
        /// authenticated connection can be parked.
        parked: Option<Instant>,
    },
    /// Auth failed and `AuthFailed` has been queued. Terminal: nothing
    /// the client sends is read any more, so it gets one attempt per
    /// connection, as on the io_uring path.
    ///
    /// The connection lingers only until the peer has acknowledged the
    /// `AuthFailed` frame, because `DpdkTransport::close` aborts and
    /// removes the socket without another dispatch — closing at once
    /// would discard the reason unsent. `AUTH_TIMEOUT`, still counted
    /// from accept, bounds the linger if the peer never acknowledges.
    Rejected {
        /// When the connection was accepted. Used for timeout.
        accepted_at: Instant,
    },
}

/// Per-connection state in the DPDK poll thread.
struct ConnectionState {
    connection_id: ConnectionId,
    addr: SocketAddr,
    handle: SocketHandle,
    auth: AuthState,
    /// FxHash of the client's Ed25519 public key. Set after auth,
    /// copied into every InputSlot as the submitting key's identity.
    key_hash: u64,
    /// Incremental frame parsing state: accumulates bytes until a
    /// complete length-prefixed frame is available, and what is left of a
    /// shed tail (see `client_frames`). The auth handshake reads its frame
    /// from the same buffer, before any request.
    ingress: ClientIngress,
    /// Last time data was received from this connection. Used for
    /// idle timeout (drops connections that stop sending).
    last_activity: Instant,
}

/// Run the DPDK poll loop.
///
/// This replaces the io_uring reader. It accepts connections, drives
/// auth handshakes, parses frames, publishes events to the disruptor,
/// and drains the TX channel from the response stage into smoltcp sockets.
/// When `tick_cadence` is `Some`, this thread also generates the application's
/// clock ticks via a wall-clock comparison between NIC bursts.
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
pub fn run_dpdk_poll<A: Application>(
    mut transport: DpdkTransport,
    mut producer: ring::Producer<InputSlot<A::Event>>,
    decoder: crate::reader::RequestDecoderArc<A>,
    // Every request not published — filtered or refused by the decoder,
    // refused while `halt` refuses writes, shed on a full input ring — is
    // answered through `early_replies` — see `crate::halt`.
    halt: HaltGate,
    mut early_replies: EarlyReplySender<A::Report>,
    // What this thread sheds, filters and refuses, and the connections it
    // parks, counted for the health endpoint.
    metrics: Arc<IngressMetrics>,
    // The admin `ACK-POLICY` command's requests, journaled by this thread
    // as the ring's one producer — see `crate::policy_control`.
    mut ack_policy: AckPolicyPublisher,
    control_tx: mpsc::Sender<ControlEvent>,
    mut tx_rx: melin_pipeline::spsc::Consumer<TxFrame>,
    shutdown: &AtomicBool,
    authorized_keys: Arc<AuthorizedKeys>,
    connection_timeout: Option<Duration>,
    tick_cadence: Option<Duration>,
    max_connections: u64,
    active_connections: Arc<std::sync::atomic::AtomicU64>,
    thread_id: u8,
    // Optional replication driver. When `Some`, this thread owns BOTH
    // client traffic on the client port and replication traffic on the
    // configured replication listen port. Connections accepted on the
    // replication port are handed to the driver; per-iteration the
    // driver's `tick()` advances each replica slot's state machine
    // (handshake, journal catch-up, snapshot transfer, live streaming,
    // ack processing). Replaces the separate replication-sender thread —
    // see `feat/dpdk-single-queue` for the rationale (RSS routing on
    // iavf made the previous multi-queue split unworkable).
    repl_driver: Option<crate::replication::DpdkReplicationDriver<A>>,
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
    // the surrounding decode, which is what `ingest` measures.
    #[cfg(feature = "latency-trace")]
    let mut publish_rec =
        melin_transport_core::trace::register_stage("dpdk: publish (decode → disruptor publish)");
    #[cfg(feature = "tick-to-trade")]
    let mut ingest_rec =
        melin_transport_core::trace::register_stage("reader: ingest (recv_ts → publish complete)");

    // Pre-allocated parse buffer pool. Avoids heap allocation on accept
    // by recycling buffers from disconnected connections.
    let mut parse_buf_pool: Vec<Vec<u8>> = (0..256)
        .map(|_| Vec::with_capacity(MAX_FRAME_SIZE + 4))
        .collect();

    // RNG for auth nonces. `rand::rng()` is a CSPRNG (ChaCha-based,
    // seeded and periodically reseeded from OS entropy), as
    // `client_auth::verify_client` requires: a predictable nonce would let
    // a response be obtained in advance — signed by the client ahead of
    // the challenge — and presented when that nonce is issued. It
    // generates in user space, so the poll thread never blocks on a
    // syscall per nonce.
    let mut rng = rand::rng();

    // Tick generator state. The DPDK poll thread is a tight busy-spin
    // loop, so unlike the io_uring reader it does not need a timeout
    // primitive — a wall-clock comparison between bursts is enough.
    // `tick_check_interval` amortises `Instant::now()` over many poll
    // iterations: at typical ~5M iterations/sec, checking every 4096
    // iterations stays well within microseconds of the deadline while
    // making the per-iteration overhead unmeasurable.
    //
    // The same counter paces the check for an `ACK-POLICY` request: a
    // request waits at most a few thousand polls, microseconds, and the
    // poll thread never parks, so it needs no wakeup.
    let tick_enabled = tick_cadence.is_some();
    let cadence = tick_cadence.unwrap_or(Duration::ZERO);
    let mut tick_schedule = TickSchedule::new(cadence, Instant::now());
    let mut tick_check_counter: u32 = 0;
    const TICK_CHECK_INTERVAL: u32 = 4096;

    // Auth / idle-timeout checks call `Instant::now()` via `elapsed()`,
    // which showed up as ~13% of the poll core in perf. The timeouts are
    // ~seconds, so firing the check once per ~1M polls (~100ms at 10M
    // polls/sec) is still orders of magnitude tighter than the deadline.
    let mut slow_check_counter: u32 = 0;
    const SLOW_CHECK_INTERVAL: u32 = 1024 * 1024;
    // A parked connection's buffer is retried on one poll iteration in
    // this many, not on each: a retry decodes the frame it parked on
    // again, and while the response stage is stalled every parked
    // connection would otherwise cost the poll thread a decode per
    // iteration. A resume waits at most this many iterations, the unit
    // the loop's other timers are counted in, after a stall long enough
    // to have filled the early-reply queue. A divisor of the slow
    // checks' interval, so every slow-check pass is a retry pass and finds
    // each parked connection's park time for the long-park report.
    const PARK_RETRY_INTERVAL: u32 = 256;
    const _: () = assert!(SLOW_CHECK_INTERVAL.is_multiple_of(PARK_RETRY_INTERVAL));
    // Connections parked for longer than `LONG_PARK` are reported on the
    // slow checks' cadence, at most once per `LONG_PARK`.
    let mut long_park_report = LongParkReport::default();
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
    // request experiences. Registered with the global stats registry; the
    // /stats-dump endpoint snapshots it alongside the other stages.
    #[cfg(feature = "latency-trace")]
    let mut poll_iter_rec = melin_transport_core::trace::register_stage(
        "dpdk poll: outer iteration (work-iterations only)",
    );
    #[cfg(feature = "latency-trace")]
    let mut poll_iter_start = mono_trace_ns();
    // Paces the recorder flush. The poll thread never idles — it
    // busy-spins — but it stops *recording* as soon as traffic stops,
    // which is enough to strand its samples in the thread-local buffer.
    // Nanoseconds rather than `Instant` so the gate reuses the
    // `mono_trace_ns` read the poll-iteration histogram already takes.
    #[cfg(feature = "latency-trace")]
    let stats_flush_interval_ns =
        melin_transport_core::trace::IDLE_FLUSH_INTERVAL.as_nanos() as u64;
    #[cfg(feature = "latency-trace")]
    let mut last_stats_flush_ns = poll_iter_start;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            // Tell the replicas this primary has gone, as its kernel
            // would on kernel TCP; a DPDK node that just stops says
            // nothing, leaving them to their liveness deadline.
            if let Some(driver) = repl_driver.take() {
                driver.close_all_links(&mut transport);
            }
            break;
        }

        // Set on any bytes received this iteration; gates poll_iter_hist
        // recording so idle iterations don't drown the percentiles.
        #[cfg(feature = "latency-trace")]
        let mut work_done_this_iter = false;

        // Once every TICK_CHECK_INTERVAL poll iterations: journal a
        // waiting `ACK-POLICY` request, and compare the wall clock to the
        // tick deadline, emitting a tick if due.
        tick_check_counter = tick_check_counter.wrapping_add(1);
        if tick_check_counter >= TICK_CHECK_INTERVAL {
            tick_check_counter = 0;
            ack_policy.poll(&mut producer, &halt);
            if tick_enabled {
                // Nothing to re-arm on a published tick: the poll loop
                // never sleeps, so the counter above is its only timer.
                tick_schedule.publish_if_due(Instant::now(), unix_epoch_nanos, &mut producer);
            }
        }

        // TX drain macro — used at multiple points in the loop to move
        // response frames from the SPSC queue into smoltcp socket buffers
        // so the next transport.poll() flushes them to the wire.
        macro_rules! drain_tx {
            () => {
                while let Some((_seq, frame)) = tx_rx.try_consume() {
                    if let Some(&handle) = id_to_handle.get(&frame.connection_id)
                        && !transport.queue_send(handle, frame.as_bytes())
                    {
                        debug!(
                            connection_id = frame.connection_id,
                            "DPDK: TX queue overflow, dropping connection"
                        );
                        transport.close(handle);
                        let _ = control_tx.send(ControlEvent::Disconnected {
                            connection_id: frame.connection_id,
                        });
                        id_to_handle.remove(&frame.connection_id);
                        release_slot(
                            &mut connections,
                            handle.index(),
                            &mut parse_buf_pool,
                            &mut connection_count,
                        );
                    }
                }
            };
        }

        // 1. Drain TX frames from the response stage BEFORE polling.
        // queue_send() stores data in TxQueue; the subsequent poll()
        // calls flush_tx_queues → iface.poll → flush_tx to push it
        // onto the wire in the same iteration. Previously the drain
        // happened after poll, adding a full loop iteration of latency.
        drain_tx!();

        // 2. Poll NIC + smoltcp (flushes TX we just drained + receives RX).
        transport.poll();

        // 3. Accept new connections — dispatch by listen port so the
        //    replication driver gets its own connections, client logic
        //    only sees client-port connections.
        for accepted in transport.take_accepted() {
            if let Some(ref mut driver) = repl_driver
                && accepted.listen_port == repl_listen_port
            {
                driver.accept_connection(accepted.peer, accepted.handle, &mut transport);
                continue;
            }

            // Enforce max_connections limit.
            if crate::connection_limit::connection_cap_reached(
                connection_count as u64,
                max_connections,
            ) {
                warn!(
                    peer = %accepted.peer,
                    "DPDK: connection rejected: max_connections reached"
                );
                transport.close(accepted.handle);
                continue;
            }

            let conn_id = ConnectionId(lane_connection_id(thread_id, next_connection_id));
            next_connection_id += 1;

            debug!(
                connection_id = conn_id.0,
                peer = %accepted.peer,
                "DPDK: new connection, starting auth"
            );

            // Fresh random nonce for the challenge, from the CSPRNG above.
            let nonce: [u8; 32] = rng.random();

            // Send the Challenge frame immediately.
            let mut challenge_buf = [0u8; 128];
            let written = control_codec::encode_transport_response(
                &TransportResponse::Challenge { nonce },
                &mut challenge_buf,
            )
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
                ingress: ClientIngress::new(
                    parse_buf_pool
                        .pop()
                        .unwrap_or_else(|| Vec::with_capacity(MAX_FRAME_SIZE + 4)),
                ),
                last_activity: Instant::now(),
            });
            connection_count += 1;
        }

        // 4. Read data from all connections and process.
        // Mid-iteration: drain TX + poll every N connections to keep
        // responses flowing and the NIC busy without waiting for the
        // full connection iteration to complete.
        const POLL_EVERY_N_CONNS: usize = 4;

        // One wall-clock read per outer poll iteration, reused for
        // every request stamped in this pass. Sub-microsecond precision
        // loss at DPDK poll rates; the timestamp drives the application's
        // clock but orders nothing (the pipeline orders by sequence).
        // Deferred until we actually stamp a frame — `clock_gettime`
        // dominates the profile on idle polls with no traffic.
        let mut batch_wall_ns: Option<u64> = None;

        slow_check_counter = slow_check_counter.wrapping_add(1);
        let do_slow_checks = slow_check_counter.is_multiple_of(SLOW_CHECK_INTERVAL);
        // The slow checks' one clock read for the long-park report, and
        // the connections found parked past `LONG_PARK` on this pass.
        let slow_check_now = do_slow_checks.then(Instant::now);
        let mut long_parked = 0usize;
        // Whether this pass retries parked connections' buffers.
        let retry_parked = slow_check_counter.is_multiple_of(PARK_RETRY_INTERVAL);
        // Connections parked after this pass, for the gauge: every parked
        // connection is counted on every pass, retried or not, so the
        // count is whole.
        let mut parked_now = 0u64;

        // Per-connection batching with a visibility-delay cap happens
        // inside `process_trading_frames`. The previous design opened a
        // single cross-connection batch here and committed it once at
        // the end of every outer poll iteration — fewer cursor stores
        // but unbounded visibility delay for the first frame on any
        // given connection. The kernel-TCP reader hit the same trade-off;
        // see `reader.rs::process_frames` for the measured numbers
        // motivating the cap. Per-connection scope keeps a 10×+ cursor
        // store reduction vs per-event publish, with bounded tail-delay.
        //
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
                // Drain fresh responses before polling so they're flushed
                // to the wire in this poll cycle instead of the next.
                drain_tx!();
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
                // drain_tx may have closed the connection at this idx
                if connections[idx].is_none() {
                    continue;
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

            // Check auth timeout for pending connections, and for rejected
            // ones whose peer never acknowledged `AuthFailed`. Throttled to
            // avoid a per-poll `Instant::now()` via `elapsed()`.
            if do_slow_checks
                && let AuthState::WaitingForResponse { accepted_at, .. }
                | AuthState::Rejected { accepted_at } = &conn.auth
                && accepted_at.elapsed() > AUTH_TIMEOUT
            {
                debug!(
                    connection_id = conn.connection_id.0,
                    addr = %conn.addr,
                    "DPDK: auth timeout, dropping connection"
                );
                transport.close(conn.handle);
                release_slot(
                    &mut connections,
                    idx,
                    &mut parse_buf_pool,
                    &mut connection_count,
                );
                continue;
            }

            // A rejected connection is closed as soon as the peer has
            // acknowledged `AuthFailed` (see `AuthState::Rejected`).
            if matches!(conn.auth, AuthState::Rejected { .. }) && transport.tx_drained(conn.handle)
            {
                debug!(
                    connection_id = conn.connection_id.0,
                    addr = %conn.addr,
                    "DPDK: AuthFailed delivered, dropping connection"
                );
                transport.close(conn.handle);
                release_slot(
                    &mut connections,
                    idx,
                    &mut parse_buf_pool,
                    &mut connection_count,
                );
                continue;
            }

            // A parked connection is not read: what it has buffered is
            // retried below, with no new bytes, on a retry pass
            // (`PARK_RETRY_INTERVAL`), and only counted on the others; the
            // client's bytes stay in the socket, whose receive window
            // closes on it. This comes before the buffer cap: one receive
            // can take the buffer past the cap before the connection
            // parks, and those bytes are the node's backlog, not a
            // client's abuse. And before the idle check: the client is not
            // idle, the node is not reading it.
            let parked = matches!(
                conn.auth,
                AuthState::Authenticated {
                    parked: Some(_),
                    ..
                }
            );
            if parked {
                if !retry_parked {
                    parked_now += 1;
                    continue;
                }
            } else {
                // Guard against unbounded buffer growth — check before recv.
                if conn.ingress.len() >= MAX_PARSE_BUF {
                    debug!(
                        connection_id = conn.connection_id.0,
                        buf_len = conn.ingress.len(),
                        "parse buffer exceeded limit, dropping connection"
                    );
                    transport.close(conn.handle);
                    let _ = control_tx.send(ControlEvent::Disconnected {
                        connection_id: conn.connection_id.0,
                    });
                    id_to_handle.remove(&conn.connection_id.0);
                    release_slot(
                        &mut connections,
                        idx,
                        &mut parse_buf_pool,
                        &mut connection_count,
                    );
                    continue;
                }

                // Read directly into parse_buf, skipping the intermediate
                // read_buf stack copy.
                let n = transport.recv_into_vec(conn.handle, conn.ingress.buf_mut());
                if n == 0 {
                    // Gone, or half-closed: the client's FIN leaves the
                    // socket in close-wait, which the stack still counts
                    // as active, and with its data drained the connection
                    // is over, as it is on kernel TCP at EOF. Without this
                    // the slot was held until the next heartbeat's RST, or
                    // for good with heartbeats off.
                    if !transport.is_active(conn.handle) || transport.peer_closed(conn.handle) {
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
                        release_slot(
                            &mut connections,
                            idx,
                            &mut parse_buf_pool,
                            &mut connection_count,
                        );
                    }
                    // Check idle timeout when no data was received.
                    // Throttled to avoid a per-poll `Instant::now()` via
                    // `elapsed()`.
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
                        release_slot(
                            &mut connections,
                            idx,
                            &mut parse_buf_pool,
                            &mut connection_count,
                        );
                    }
                    continue;
                }
                conn.last_activity = Instant::now();
                #[cfg(feature = "latency-trace")]
                {
                    work_done_this_iter = true;
                }
            }

            // Trace timestamp: the moment `recv_into_vec` handed us these
            // bytes, or that a parked connection's buffer is retried.
            // Captured once per recv — not per frame — and stamped onto
            // every InputSlot parsed below, so the reader-ingest and
            // server-e2e stages measure from true wire receipt (frame decode
            // included) rather than re-sampling after each decode.
            #[allow(clippy::let_unit_value)] // ZST when latency-trace is off
            let recv_ts = melin_transport_core::trace::mono_trace_ns();

            // Process frames based on auth state.
            let was_waiting = matches!(conn.auth, AuthState::WaitingForResponse { .. });
            match &mut conn.auth {
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
                // Terminal: whatever the client sends after a failed
                // attempt is discarded unread.
                AuthState::Rejected { .. } => conn.ingress.buf_mut().clear(),
                AuthState::Authenticated { role, parked } => {
                    use crate::client_frames::{FrameAction, process_client_frames};
                    let role = *role;
                    let action = process_client_frames::<A>(
                        &mut conn.ingress,
                        conn.connection_id.0,
                        conn.key_hash,
                        role,
                        &mut producer,
                        &*decoder,
                        &halt,
                        &mut early_replies,
                        &metrics,
                        *batch_wall_ns.get_or_insert_with(unix_epoch_nanos),
                        recv_ts,
                        #[cfg(feature = "latency-trace")]
                        &mut publish_rec,
                        #[cfg(feature = "tick-to-trade")]
                        &mut ingest_rec,
                    );
                    // A full input ring is answered with a busy through
                    // the early-reply queue, as is every frame buffered
                    // behind the shed request, and the connection is kept
                    // (`Continue`).
                    let close = match action {
                        FrameAction::Continue => {
                            // A parked connection whose retry got through
                            // is read again from the next iteration. Not
                            // read while parked, its idle clock restarts
                            // now, not at its last bytes.
                            if parked.take().is_some() {
                                conn.last_activity = Instant::now();
                                debug!(
                                    connection_id = conn.connection_id.0,
                                    "DPDK: connection resumed"
                                );
                            }
                            false
                        }
                        FrameAction::Disconnect => true,
                        // An answer with nowhere to go: stop reading the
                        // connection, and retry its buffer on the retry
                        // passes from here on (see `parked` above). One parked
                        // since it first parked, past `LONG_PARK`, is
                        // counted for the report below.
                        FrameAction::Park => {
                            parked_now += 1;
                            match *parked {
                                None => {
                                    *parked = Some(Instant::now());
                                    metrics.record_park();
                                    debug!(
                                        connection_id = conn.connection_id.0,
                                        "DPDK: connection parked"
                                    );
                                }
                                Some(since) => {
                                    if let Some(now) = slow_check_now
                                        && LongParkReport::parked_long(since, now)
                                    {
                                        long_parked += 1;
                                    }
                                }
                            }
                            false
                        }
                    };
                    if close {
                        transport.close(conn.handle);
                        let _ = control_tx.send(ControlEvent::Disconnected {
                            connection_id: conn.connection_id.0,
                        });
                        id_to_handle.remove(&conn.connection_id.0);
                        release_slot(
                            &mut connections,
                            idx,
                            &mut parse_buf_pool,
                            &mut connection_count,
                        );
                        continue;
                    }
                }
            }

            // Track auth → authenticated transition for the connection counter.
            if was_waiting && matches!(conn.auth, AuthState::Authenticated { .. }) {
                active_connections.fetch_add(1, Ordering::Relaxed);
            }
        }
        // A load and no store while the count is unchanged.
        metrics.set_parked(parked_now);
        if let Some(now) = slow_check_now {
            long_park_report.report(long_parked, now);
        }

        // Final drain + poll before the replication tick so fresh ACK
        // data from replicas is available (arrived since the last mid-
        // iteration poll) and any remaining client TX is flushed.
        drain_tx!();
        transport.poll();

        // Drive the replication driver's per-iteration work — handshake
        // progression, a joining replica's catch-up (read off the disk by
        // the slot's join worker, sent from here a tick at a time), ack
        // processing, and live data-batch sends. With a single
        // queue + single thread, this is what replaces the previous
        // dedicated replication sender thread; the poll above flushed any TX
        // the driver queued on the prior iteration and received any
        // pending ACKs from replicas.
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

            // Hand buffered samples to the stats registry so a
            // /stats-dump taken after traffic stops still sees them.
            if now.saturating_sub(last_stats_flush_ns) >= stats_flush_interval_ns {
                last_stats_flush_ns = now;
                poll_iter_rec.flush();
                publish_rec.flush();
                #[cfg(feature = "tick-to-trade")]
                ingest_rec.flush();
            }
        }
    }
}

/// What a pending connection's parse buffer amounts to, auth-wise.
#[derive(Debug)]
enum AuthVerdict {
    /// No complete auth frame yet — wait for more bytes.
    Incomplete,
    /// The frame is oversized, undecodable, or carries a signature that
    /// does not admit the client. Answered with `AuthFailed`.
    Rejected,
    /// The client signed the nonce with a key listed for `role`.
    Admitted {
        role: ClientRole<RoleId>,
        public_key: [u8; 32],
    },
}

/// Judge the auth frame at the head of `parse_buf` against `nonce`.
///
/// A complete frame is consumed from the buffer, so on admission the bytes
/// the client pipelined behind it stay in place for the data path. On
/// rejection the caller drops the whole buffer, and an oversized frame is
/// rejected on its prefix without being consumed.
fn evaluate_auth_frame(
    parse_buf: &mut Vec<u8>,
    nonce: &[u8; 32],
    authorized_keys: &AuthorizedKeys,
    connection_id: u64,
) -> AuthVerdict {
    let (decoded, consumed) = match split_frame_limited(parse_buf, MAX_AUTH_FRAME) {
        // Borrow the payload directly — no heap allocation.
        Ok(Some(frame)) => (
            control_codec::decode_challenge_response(frame.payload),
            frame.consumed(),
        ),
        Ok(None) => return AuthVerdict::Incomplete,
        Err(too_large) => {
            debug!(
                connection_id,
                frame_len = too_large.declared,
                "DPDK: auth frame too large"
            );
            return AuthVerdict::Rejected;
        }
    };

    // Compact the parse buffer now that the borrow is released.
    // Single memmove instead of drain()'s per-byte shift.
    let remaining = parse_buf.len() - consumed;
    parse_buf.copy_within(consumed.., 0);
    parse_buf.truncate(remaining);

    let cr = match decoded {
        Ok(cr) => cr,
        Err(e) => {
            debug!(connection_id, error = %e, "DPDK: auth decode error");
            return AuthVerdict::Rejected;
        }
    };

    match crate::client_auth::verify_client(authorized_keys, nonce, &cr.public_key, &cr.signature) {
        Ok(role) => AuthVerdict::Admitted {
            role,
            public_key: cr.public_key,
        },
        Err(e) => {
            debug!(connection_id, error = %e, "DPDK: auth failed");
            AuthVerdict::Rejected
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
    // The nonce captured at Challenge-send time, which the client signed.
    let (nonce, accepted_at) = match &conn.auth {
        AuthState::WaitingForResponse { nonce, accepted_at } => (*nonce, *accepted_at),
        _ => unreachable!("process_auth_frame called in wrong state"),
    };

    let (role, public_key_bytes) = match evaluate_auth_frame(
        conn.ingress.buf_mut(),
        &nonce,
        authorized_keys,
        conn.connection_id.0,
    ) {
        AuthVerdict::Incomplete => return,
        AuthVerdict::Rejected => {
            send_auth_failed(conn, transport);
            // One attempt per connection: the poll loop closes it once
            // the peer has the AuthFailed frame.
            conn.ingress.buf_mut().clear();
            conn.auth = AuthState::Rejected { accepted_at };
            return;
        }
        AuthVerdict::Admitted { role, public_key } => (role, public_key),
    };

    // Auth succeeded — send ServerReady.
    let mut buf = [0u8; 16];
    let written =
        control_codec::encode_transport_response(&TransportResponse::ServerReady, &mut buf)
            .expect("ServerReady encodes");
    transport.queue_send(conn.handle, &buf[..written]);

    debug!(
        connection_id = conn.connection_id.0,
        addr = %conn.addr,
        role = authorized_keys.token(KeyRole::Client(role)),
        "DPDK: authenticated"
    );

    // Transition to authenticated state, under the identity the
    // application sees as `ApplyCtx::key_hash`.
    conn.key_hash = melin_app::key_hash(&public_key_bytes);
    conn.auth = AuthState::Authenticated { role, parked: None };

    // Register with the response stage and ID map — before this thread
    // parses a single request of the connection, and keep it that way:
    // the response stage reads its output ring before it drains this
    // channel, and relies on `Connected` being queued first. See the
    // drain in `dpdk_response::run`.
    id_to_handle.insert(conn.connection_id.0, handle);
    let _ = control_tx.send(ControlEvent::Connected {
        connection_id: conn.connection_id.0,
    });
}

/// Free a closed connection's slot: return its parse buffer to the pool
/// (cleared, capacity kept) and release its `max_connections` count. A
/// no-op on an empty slot, so a path that may already have released it
/// stays balanced.
///
/// Only the slot: closing the socket, telling the response stage, and
/// dropping the `id_to_handle` route stay with the caller, because which
/// of those apply depends on how far the connection got.
#[inline]
fn release_slot(
    connections: &mut [Option<ConnectionState>],
    idx: usize,
    parse_buf_pool: &mut Vec<Vec<u8>>,
    connection_count: &mut usize,
) {
    if let Some(removed) = connections[idx].take() {
        parse_buf_pool.push(removed.ingress.into_buf());
        *connection_count -= 1;
    }
}

/// Queue an AuthFailed response, best effort. The caller moves the
/// connection to `AuthState::Rejected`, which closes it once the frame
/// has been delivered.
fn send_auth_failed(conn: &ConnectionState, transport: &mut DpdkTransport) {
    let mut buf = [0u8; 16];
    if let Ok(written) =
        control_codec::encode_transport_response(&TransportResponse::AuthFailed, &mut buf)
    {
        // The result is not checked: the queue refuses only past its
        // per-connection limit, and before auth it holds at most the
        // Challenge. Were it refused, only the reason would be lost; the
        // connection is closed on the same terms either way.
        transport.queue_send(conn.handle, &buf[..written]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The client data path splits frames in `process_client_frames`, shared
    // with the io_uring reader and tested there (`reader.rs`), on the wire
    // protocol's splitter, whose own tests cover the partial, empty,
    // exact-limit and oversized cases. What is left to check here is that
    // an application's encoded events come back out of that splitter, under
    // the cap the DPDK path applies, byte for byte.
    //
    // The in-tree counter example stands in for a production application's
    // event, keeping these tests free of any application crate but the
    // example.
    use counter_server::CounterEvent;
    use melin_app::AppEvent;
    use melin_wire_protocol::framing::Frame;

    /// The first frame of `buf` under the client data path's cap.
    fn split(buf: &[u8]) -> Option<Frame<'_>> {
        split_frame_limited(buf, MAX_FRAME_SIZE).expect("frame within MAX_FRAME_SIZE")
    }

    /// Encode a `CounterEvent` into a length-prefixed wire frame:
    /// `[len:u32][event bytes]` — the framing the DPDK transport extracts.
    fn counter_frame(event: CounterEvent) -> Vec<u8> {
        let mut body = vec![0u8; event.encoded_size()];
        let n = event.encode(&mut body);
        let mut frame = (n as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&body[..n]);
        frame
    }

    // --- Wire-level round-trip: encode event → extract frame → decode ---

    #[test]
    fn wire_round_trip_increment() {
        let wire = counter_frame(CounterEvent::Increment { amount: 42 });
        let frame = split(&wire).expect("complete frame");
        assert_eq!(frame.consumed(), wire.len());
        let decoded = CounterEvent::decode(frame.payload).unwrap();
        assert!(matches!(decoded, CounterEvent::Increment { amount: 42 }));
    }

    #[test]
    fn wire_round_trip_get_value() {
        let wire = counter_frame(CounterEvent::GetValue);
        let frame = split(&wire).expect("complete frame");
        let decoded = CounterEvent::decode(frame.payload).unwrap();
        assert!(matches!(decoded, CounterEvent::GetValue));
    }

    // --- Incremental accumulation (simulates TCP byte-at-a-time arrival) ---

    #[test]
    fn incremental_frame_accumulation() {
        let wire = counter_frame(CounterEvent::Increment { amount: 7 });
        let written = wire.len();

        // Feed bytes one at a time into a parse buffer.
        let mut parse_buf = Vec::new();
        for (i, &byte) in wire.iter().enumerate() {
            parse_buf.push(byte);
            let result = split(&parse_buf);
            if i < written - 1 {
                assert_eq!(result, None, "byte {i}");
            } else {
                assert!(result.is_some(), "expected a frame at the final byte");
            }
        }
    }

    // --- Multiple frames back-to-back ---

    #[test]
    fn multiple_frames_in_buffer() {
        let frame1 = counter_frame(CounterEvent::Increment { amount: 1 });
        let frame2 = counter_frame(CounterEvent::GetValue);

        // Concatenate two frames.
        let mut combined = Vec::new();
        combined.extend_from_slice(&frame1);
        combined.extend_from_slice(&frame2);

        // First extraction should get frame 1.
        let first = split(&combined).expect("first frame");
        let decoded = CounterEvent::decode(first.payload).unwrap();
        assert!(matches!(decoded, CounterEvent::Increment { amount: 1 }));

        // Second extraction, past the first frame, should get frame 2.
        let second = split(&combined[first.consumed()..]).expect("second frame");
        let decoded = CounterEvent::decode(second.payload).unwrap();
        assert!(matches!(decoded, CounterEvent::GetValue));
        assert_eq!(first.consumed() + second.consumed(), combined.len());
    }

    mod auth_verdict {
        use super::*;

        use base64::Engine;
        use ed25519_dalek::{Signer, SigningKey};
        use melin_wire_protocol::control::ChallengeResponse;

        use crate::test_roles::desk_keys;

        const NONCE: [u8; 32] = [0x5A; 32];

        fn client() -> SigningKey {
            SigningKey::from_bytes(&[0x11; 32])
        }

        fn keys() -> AuthorizedKeys {
            let public = base64::engine::general_purpose::STANDARD
                .encode(client().verifying_key().to_bytes());
            desk_keys("trader", &public)
        }

        /// A length-prefixed ChallengeResponse frame signing `nonce` with `signer`.
        fn auth_frame(signer: &SigningKey, nonce: &[u8; 32]) -> Vec<u8> {
            let response = ChallengeResponse {
                signature: signer.sign(nonce).to_bytes(),
                public_key: client().verifying_key().to_bytes(),
            };
            let mut payload = [0u8; 128];
            let len = control_codec::encode_challenge_response(&response, &mut payload).unwrap();
            let mut frame = (len as u32).to_le_bytes().to_vec();
            frame.extend_from_slice(&payload[..len]);
            frame
        }

        fn evaluate(buf: &mut Vec<u8>) -> AuthVerdict {
            evaluate_auth_frame(buf, &NONCE, &keys(), 1)
        }

        #[test]
        fn a_valid_response_is_admitted_and_pipelined_bytes_are_kept() {
            let mut buf = auth_frame(&client(), &NONCE);
            buf.extend_from_slice(b"next");
            match evaluate(&mut buf) {
                AuthVerdict::Admitted { public_key, .. } => {
                    assert_eq!(public_key, client().verifying_key().to_bytes());
                }
                other => panic!("expected Admitted, got {other:?}"),
            }
            assert_eq!(buf, b"next");
        }

        /// Every prefix of a valid frame waits for more bytes rather than
        /// being judged.
        #[test]
        fn a_partial_frame_is_incomplete_and_left_in_place() {
            let frame = auth_frame(&client(), &NONCE);
            for cut in 0..frame.len() {
                let mut buf = frame[..cut].to_vec();
                assert!(
                    matches!(evaluate(&mut buf), AuthVerdict::Incomplete),
                    "cut {cut}"
                );
                assert_eq!(buf, &frame[..cut]);
            }
        }

        #[test]
        fn a_signature_by_another_key_is_rejected() {
            let impostor = SigningKey::from_bytes(&[0x33; 32]);
            let mut buf = auth_frame(&impostor, &NONCE);
            assert!(matches!(evaluate(&mut buf), AuthVerdict::Rejected));
        }

        /// A signature over another nonce — a replay from an earlier
        /// connection — does not authenticate this one.
        #[test]
        fn a_signature_over_another_nonce_is_rejected() {
            let mut buf = auth_frame(&client(), &[0xA5; 32]);
            assert!(matches!(evaluate(&mut buf), AuthVerdict::Rejected));
        }

        #[test]
        fn an_undecodable_frame_is_rejected() {
            let mut buf = auth_frame(&client(), &NONCE);
            buf[4] ^= 0xFF; // corrupt the tag
            assert!(matches!(evaluate(&mut buf), AuthVerdict::Rejected));
        }

        #[test]
        fn a_zero_length_frame_is_rejected() {
            let mut buf = 0u32.to_le_bytes().to_vec();
            assert!(matches!(evaluate(&mut buf), AuthVerdict::Rejected));
        }

        /// Rejected on the length prefix alone, before the body arrives.
        #[test]
        fn an_oversized_frame_is_rejected_on_its_prefix() {
            let mut buf = ((MAX_AUTH_FRAME + 1) as u32).to_le_bytes().to_vec();
            assert!(matches!(evaluate(&mut buf), AuthVerdict::Rejected));

            let mut at_cap = (MAX_AUTH_FRAME as u32).to_le_bytes().to_vec();
            assert!(matches!(evaluate(&mut at_cap), AuthVerdict::Incomplete));
        }
    }

    /// Parking on the poll loop: a connection whose early reply has
    /// nowhere to go is not read, and its buffer is retried on every
    /// `PARK_RETRY_INTERVAL`-th iteration until the response stage takes
    /// early replies again.
    ///
    /// A running node never gets there on demand: its response stage takes
    /// early replies between any two events. So these run the poll loop
    /// on a node slot of the netns runner, with the DPDK response stage
    /// held at its test seam (`pause_after_control_drain`) and a small
    /// early-reply queue, and the test plays the matching stage, which
    /// acknowledges every write. The client is a kernel one on the
    /// runner's bridge, as a node test's is, whose send buffer is kept
    /// small so that it stalls soon after the node stops reading.
    ///
    /// The shared shedding and parking logic (`client_frames`) is tested
    /// on every host; what is left here is the poll loop's part: not
    /// reading, the retry, the idle check and the buffer cap.
    mod parking {
        use super::*;

        use std::net::TcpStream;
        use std::os::fd::AsRawFd;
        use std::sync::atomic::AtomicU64;
        use std::sync::{Condvar, Mutex};
        use std::thread::JoinHandle;

        use counter_server::{
            Counter, CounterQuery, CounterReport, CounterRole, KIND_RESP_ACK, RequestDecoder,
            ResponseEncoder, increment_request,
        };
        use melin_client::{
            Frame, NonBlockingTcp, PolledConnection, REQUEST_HEADER_LEN, RefusalKind, RequestId,
            SigningKey, StreamConnection, key,
        };
        use melin_dpdk::{DpdkConfig, DpdkShared, SocketBuffers};
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_pipeline::spsc;
        use melin_pipeline::wait::WaitStrategy;
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::pipeline::{OutputPayload, OutputSlot, StageUtilization};
        use melin_transport_core::{DurableWireSeqCursor, WireSeq};

        use crate::ack_policy::AckPolicy;
        use crate::client_frames::MAX_REQUEST_BODY;
        use crate::dpdk_response::TxFrame;
        use crate::durability_gate::{GatePolicy, GateStart};
        use crate::early_reply::early_reply_channel;
        use melin_transport_core::lineage::forced_policy_cell;

        /// The client's key, listed as a counter writer.
        const WRITER: [u8; 32] = [0xA1; 32];

        /// A request the counter's decoder refuses (an unknown kind), as
        /// wide as a request goes, so that a few of them fill a buffer:
        /// each is answered early, with a `Refused`.
        const MALFORMED: [u8; MAX_REQUEST_BODY] = [0x55; MAX_REQUEST_BODY];

        /// How long the client's backlog must stay put for the test to
        /// take the node to have stopped reading: the poll loop reads a
        /// connection on every iteration, microseconds apart.
        const QUIET: Duration = Duration::from_millis(500);

        /// Bounds every wait for something that must happen.
        const PATIENCE: Duration = Duration::from_secs(30);

        /// The client's socket send buffer, asked small so that the client
        /// stalls soon after the node's receive buffer is full (the kernel
        /// doubles what is asked).
        const CLIENT_SNDBUF: libc::c_int = 32 * 1024;

        /// The DPDK response stage's seam: held, or let through a number
        /// of iterations, or let go for good. A `Mutex` and `Condvar`
        /// rather than atomics: the stage sleeps while held, and the test
        /// wakes it.
        #[derive(Clone, Default)]
        struct Hold(Arc<(Mutex<HoldState>, Condvar)>);

        #[derive(Default)]
        struct HoldState {
            released: bool,
            /// Iterations the stage may still take while held. `u64`, a
            /// count no test approaches.
            permits: u64,
        }

        impl Hold {
            /// The seam's closure: wait while held with no permit.
            fn pause(&self) {
                let (lock, wake) = &*self.0;
                let mut state = lock.lock().expect("hold lock");
                while !state.released && state.permits == 0 {
                    state = wake.wait(state).expect("hold lock");
                }
                state.permits = state.permits.saturating_sub(1);
            }

            /// Let the stage take one more iteration.
            fn step(&self) {
                let (lock, wake) = &*self.0;
                lock.lock().expect("hold lock").permits += 1;
                wake.notify_all();
            }

            /// Let the stage run on, as production does.
            fn release(&self) {
                let (lock, wake) = &*self.0;
                lock.lock().expect("hold lock").released = true;
                wake.notify_all();
            }
        }

        /// The poll loop on a node slot, the DPDK response stage behind
        /// it (held at first), and the test's matching stage between them.
        /// Stopped and its slot freed when dropped, also on a failed
        /// test's unwind.
        struct Rig {
            addr: SocketAddr,
            hold: Hold,
            /// The poll thread's halt gate's fence: fenced, it closes
            /// every connection it reads.
            fence: Arc<FenceState>,
            /// Authenticated connections the response stage holds: the
            /// poll thread counts each up, the stage down when it drains
            /// its `Disconnected`.
            active: Arc<AtomicU64>,
            /// What the poll thread counts for the health endpoint.
            metrics: Arc<IngressMetrics>,
            shutdown: Arc<AtomicBool>,
            threads: Vec<JoinHandle<()>>,
            /// Dropped after the threads have returned, the transport with
            /// them, and before `nic`: the port closes with the last
            /// holder, and the slot's device can only be detached then.
            shared: Option<Arc<DpdkShared>>,
            _nic: melin_test_node::DpdkNodeNic,
        }

        impl Rig {
            /// A rig with an early-reply queue of `early_capacity`, the
            /// node's sockets receiving into `rx_buf_size` bytes, and
            /// `connection_timeout` for the idle check.
            fn start(
                early_capacity: usize,
                rx_buf_size: usize,
                connection_timeout: Option<Duration>,
            ) -> Self {
                // Opt-in diagnostics: with RUST_LOG set, the poll loop says
                // when it parks and resumes a connection, and why it closes
                // one. No-op when RUST_LOG is unset.
                melin_test_node::logs::capture_if_requested();
                let nic = melin_test_node::dpdk_node_nic();
                let addr = nic.addr();
                let dpdk_config = DpdkConfig {
                    nic: nic.nic().clone(),
                    listen_port: addr.port(),
                    client_buffers: SocketBuffers {
                        rx_buf_size,
                        ..SocketBuffers::DEFAULT
                    },
                    num_queues: 1,
                };
                let shared = DpdkShared::init(&dpdk_config).expect("bring the slot's NIC up");

                // Rings sized so that nothing in these tests fills them:
                // only the early-reply queue may.
                let (mut producer, mut input) =
                    DisruptorBuilder::<InputSlot<CounterEvent>>::new(1 << 12)
                        .add_consumer()
                        .build(WaitStrategy::SpinThenYield);
                let mut input = input.pop().expect("one consumer");
                let (mut output, mut consumers) =
                    DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(1 << 12)
                        .add_consumer()
                        .build(WaitStrategy::SpinThenYield);
                let consumer = consumers.pop().expect("one consumer");
                let (early_tx, early_rx) = early_reply_channel::<CounterReport>(early_capacity);
                let (control_tx, control_rx) = mpsc::channel();
                let (tx_out, tx_rx) = spsc::channel::<TxFrame>(4096, WaitStrategy::SpinThenYield);
                let ack_policy = crate::policy_control::test_support::attach(
                    &crate::policy_control::test_support::replicated(AckPolicy::Disk),
                    &mut producer,
                );
                let public = key::authorized_keys_line(
                    "writer",
                    &SigningKey::from_bytes(&WRITER).verifying_key(),
                    "writer",
                );
                let keys = Arc::new(
                    AuthorizedKeys::parse::<CounterRole>(&format!("{public}\n"))
                        .expect("parse authorized_keys"),
                );

                let hold = Hold::default();
                let fence = Arc::new(FenceState::new(0));
                let halt = HaltGate::new(None, Arc::clone(&fence), Arc::new(AtomicU64::new(0)));
                let active = Arc::new(AtomicU64::new(0));
                let metrics = Arc::new(IngressMetrics::default());
                let shutdown = Arc::new(AtomicBool::new(false));
                let mut threads = Vec::new();

                // The matching stage: every write acknowledged, in order,
                // nothing held for durability.
                let sd = Arc::clone(&shutdown);
                threads.push(std::thread::spawn(move || {
                    while !sd.load(Ordering::Relaxed) {
                        let Some((seq, slot)) = input.try_consume() else {
                            std::thread::yield_now();
                            continue;
                        };
                        if slot.connection_id == 0 {
                            continue;
                        }
                        output.publish(OutputSlot {
                            connection_id: slot.connection_id,
                            request_id: slot.request_id,
                            input_seq: seq,
                            wire_seq: seq + 1,
                            payload: OutputPayload::Report(CounterReport::Ack {
                                new_value: seq + 1,
                            }),
                            is_last_in_request: true,
                            ..Default::default()
                        });
                    }
                }));

                let (sd, stage_hold, stage_active) =
                    (Arc::clone(&shutdown), hold.clone(), Arc::clone(&active));
                threads.push(std::thread::spawn(move || {
                    crate::dpdk_response::run::<Counter>(
                        consumer,
                        control_rx,
                        // Everything is on disk: the gate holds nothing.
                        // Far past any wire seq the matching stage above
                        // stamps, and short of `u64::MAX`, which the
                        // policy's evaluation reads as "no cursor".
                        DurableWireSeqCursor::detached(WireSeq::new(1 << 62)),
                        GatePolicy {
                            start: GateStart::settled(AckPolicy::Disk),
                            forced: forced_policy_cell().1,
                        },
                        None,
                        None,
                        &sd,
                        None,
                        stage_active,
                        vec![tx_out],
                        Arc::new(StageUtilization::default()),
                        WaitStrategy::SpinThenYield,
                        Arc::new(ResponseEncoder),
                        early_rx,
                        None,
                        Some(Box::new(move || stage_hold.pause())),
                    );
                }));

                let (sd, poll_shared, poll_active, poll_metrics) = (
                    Arc::clone(&shutdown),
                    Arc::clone(&shared),
                    Arc::clone(&active),
                    Arc::clone(&metrics),
                );
                threads.push(std::thread::spawn(move || {
                    // Built on the thread that polls it, as a node's is.
                    let transport = DpdkTransport::from_shared(&poll_shared, &dpdk_config, 0)
                        .expect("the transport listens");
                    run_dpdk_poll::<Counter>(
                        transport,
                        producer,
                        Arc::new(RequestDecoder),
                        halt,
                        early_tx,
                        poll_metrics,
                        ack_policy,
                        control_tx,
                        tx_rx,
                        &sd,
                        keys,
                        connection_timeout,
                        None,
                        melin_test_node::MAX_CONNECTIONS,
                        poll_active,
                        0,
                        None,
                        0,
                    );
                }));

                Rig {
                    addr,
                    hold,
                    fence,
                    active,
                    metrics,
                    shutdown,
                    threads,
                    shared: Some(shared),
                    _nic: nic,
                }
            }

            /// A client of the rig's node, authenticated, its send buffer
            /// small. The node serves before this dials: the launcher's
            /// client waits for it first, as the node tests' do.
            fn client(&self) -> Client {
                let key = SigningKey::from_bytes(&WRITER);
                drop(melin_test_node::connect(self.addr, &key));
                let stream = TcpStream::connect_timeout(&self.addr, PATIENCE).expect("dial");
                // SAFETY: a live socket, owned by `stream`, and an option
                // value of the size passed.
                let rc = unsafe {
                    libc::setsockopt(
                        stream.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_SNDBUF,
                        (&CLIENT_SNDBUF as *const libc::c_int).cast(),
                        size_of::<libc::c_int>() as libc::socklen_t,
                    )
                };
                assert_eq!(rc, 0, "SO_SNDBUF: {}", std::io::Error::last_os_error());
                let stream = NonBlockingTcp::from_stream(stream).expect("non-blocking");
                let (stream, mut conn) = StreamConnection::handshake(stream, &key, PATIENCE)
                    .expect("the handshake")
                    .into_parts();
                // Replies are owed for as long as the stage is held.
                conn.set_read_timeout(PATIENCE * 10);
                Client {
                    stream,
                    conn,
                    clock: Instant::now(),
                    sent: Vec::new(),
                    framed: 0,
                    answers: Vec::new(),
                    acked: false,
                }
            }
        }

        impl Drop for Rig {
            fn drop(&mut self) {
                self.hold.release();
                self.shutdown.store(true, Ordering::Relaxed);
                for thread in self.threads.drain(..) {
                    // Dropped result: a thread that panicked has failed
                    // the test already, whose own panic is the report.
                    let _ = thread.join();
                }
                drop(self.shared.take());
            }
        }

        /// What a request was answered with.
        #[derive(Debug, PartialEq, Eq)]
        enum Answer {
            /// A write's batch: its acknowledgement, then `BatchEnd`.
            Acked,
            /// The decoder's refusal of [`MALFORMED`].
            Malformed,
        }

        /// A pipelining client, on the polled connection so that a request
        /// the stream does not take is queued rather than waited on: what
        /// it has sent, by ID, and what has come back.
        struct Client {
            stream: NonBlockingTcp,
            conn: PolledConnection,
            /// The connection's clock starts here.
            clock: Instant,
            sent: Vec<(RequestId, Answer)>,
            /// Bytes framed so far, the stream's or still the client's.
            framed: usize,
            answers: Vec<(RequestId, Answer)>,
            /// Whether a write's acknowledgement has come since the last
            /// final frame.
            acked: bool,
        }

        impl Client {
            /// Now, on the connection's clock.
            fn now_ns(&self) -> u64 {
                u64::try_from(self.clock.elapsed().as_nanos()).expect("within a test's span")
            }

            /// Send the next request of the burst: every fourth a write,
            /// the rest refused.
            fn send_next(&mut self) {
                if self.sent.len() % 4 == 3 {
                    self.send(&increment_request(1), Answer::Acked);
                } else {
                    self.send(&MALFORMED, Answer::Malformed);
                }
            }

            /// Send `body`, to be answered as `answer` says.
            fn send(&mut self, body: &[u8], answer: Answer) {
                let now = self.now_ns();
                let id = self.conn.send(&mut self.stream, now, body).expect("send");
                self.framed += REQUEST_HEADER_LEN + body.len();
                self.sent.push((id, answer));
            }

            /// Bytes the stream has taken: the node's, or on their way.
            fn taken(&self) -> usize {
                self.framed - self.conn.unsent()
            }

            /// Hand the stream what it takes, and take every answer that
            /// has arrived, without waiting.
            fn poll(&mut self) {
                let now = self.now_ns();
                while let Some(frame) = self
                    .conn
                    .poll(&mut self.stream, now)
                    .expect("the connection stays up")
                {
                    match frame {
                        Frame::Response(body) => {
                            assert_eq!(body[0], KIND_RESP_ACK, "a write's acknowledgement");
                            self.acked = true;
                        }
                        Frame::BatchEnd { id, .. } => {
                            assert!(std::mem::take(&mut self.acked), "{id:?}: an empty batch");
                            self.answers.push((id, Answer::Acked));
                        }
                        Frame::Refused { id, kind, reason } => {
                            assert_eq!(
                                (kind, reason),
                                (RefusalKind::Malformed, "unknown kind"),
                                "{id:?}"
                            );
                            self.answers.push((id, Answer::Malformed));
                        }
                        Frame::ServerBusy { id } => panic!("{id:?} shed: no ring fills here"),
                    }
                }
            }

            /// Whether the node has closed the connection, as far as the
            /// client can tell now: its close sends nothing, so it shows
            /// only as the reset a request since has drawn. Panics on an
            /// answer: the stage is held while this is asked.
            fn dropped_by_node(&mut self) -> bool {
                let now = self.now_ns();
                match self.conn.poll(&mut self.stream, now) {
                    Ok(None) => false,
                    Ok(Some(frame)) => panic!("answered while the stage is held: {frame:?}"),
                    Err(melin_client::Error::Disconnected | melin_client::Error::Io(_)) => true,
                    Err(e) => panic!("not a close: {e:?}"),
                }
            }

            /// Pipeline requests until the node stops taking bytes: a
            /// backlog the stream does not take, which stays put for
            /// [`QUIET`]. Nothing is answered meanwhile: the response
            /// stage is held.
            fn send_until_stalled(&mut self) {
                let deadline = Instant::now() + PATIENCE;
                loop {
                    self.top_up();
                    let before = self.taken();
                    let quiet_until = Instant::now() + QUIET;
                    while Instant::now() < quiet_until {
                        self.poll();
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    assert!(self.answers.is_empty(), "answered while the stage is held");
                    if self.taken() == before {
                        return;
                    }
                    assert!(Instant::now() < deadline, "the node never stopped reading");
                }
            }

            /// Keep a backlog of a request frame's width or more that the
            /// stream has not taken: the client always has more to send.
            fn top_up(&mut self) {
                /// Far past what the node and the client can buffer
                /// between them, so a node still reading fails the test
                /// rather than looping it.
                const NEVER_STALLED: usize = 64 << 20;
                while self.conn.unsent() < REQUEST_HEADER_LEN + MAX_REQUEST_BODY {
                    assert!(
                        self.framed < NEVER_STALLED,
                        "the node never stopped reading"
                    );
                    self.send_next();
                }
            }

            /// Take answers until every request sent has one, then check
            /// that each was answered once, as its kind says, and the
            /// writes in request order.
            fn finish(&mut self) {
                let deadline = Instant::now() + PATIENCE;
                while self.answers.len() < self.sent.len() {
                    assert!(
                        Instant::now() < deadline,
                        "{} of {} requests answered",
                        self.answers.len(),
                        self.sent.len()
                    );
                    self.poll();
                    std::thread::yield_now();
                }
                let acked = |list: &[(RequestId, Answer)]| -> Vec<RequestId> {
                    list.iter()
                        .filter(|(_, answer)| *answer == Answer::Acked)
                        .map(|(id, _)| *id)
                        .collect()
                };
                assert_eq!(acked(&self.answers), acked(&self.sent), "writes in order");
                self.answers.sort_by_key(|(id, _)| *id);
                assert_eq!(self.answers, self.sent, "every request answered once");
            }
        }

        /// Keep polling `client` for `for_how_long`: panics if the node
        /// closes the connection meanwhile and the client finds out. The
        /// node's close sends nothing: a client with bytes still to send
        /// finds out from the reset they draw, one with none only from its
        /// next request's.
        fn still_up(client: &mut Client, for_how_long: Duration) {
            let until = Instant::now() + for_how_long;
            while Instant::now() < until {
                client.poll();
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        /// A parked connection is not read: its client stalls once the
        /// node's receive buffer and its own send buffer are full. Let go,
        /// the node answers every request once, the writes in order, and
        /// the connection serves the next request.
        #[test]
        fn a_parked_connection_is_not_read_and_completes_once_let_go() {
            let rig = Rig::start(4, SocketBuffers::DEFAULT.rx_buf_size, None);
            let mut client = rig.client();
            client.send_until_stalled();
            still_up(&mut client, QUIET);

            rig.hold.release();
            client.finish();
            client.send(&increment_request(1), Answer::Acked);
            client.finish();
        }

        /// The idle check skips a parked connection, however long it stays
        /// parked: its client is not idle, the node is not reading it.
        ///
        /// The idle check is throttled, so the client surviving the park
        /// proves nothing unless the check has run meanwhile. A witness
        /// connection, idle the while, proves it: the client is let go only
        /// once the node has dropped the witness as idle.
        #[test]
        fn a_parked_connection_is_not_dropped_as_idle() {
            /// Short, so that a wait of a few of it outlasts the witness's
            /// clock and the poll loop's throttled check alike.
            const TIMEOUT: Duration = Duration::from_secs(1);
            let rig = Rig::start(4, SocketBuffers::DEFAULT.rx_buf_size, Some(TIMEOUT));
            let mut client = rig.client();
            let mut witness = rig.client();
            client.send_until_stalled();

            // Probe the witness every two timeouts: a node that still holds
            // it takes the write (and its clock restarts), one that has
            // dropped it as idle resets the connection. The client, parked
            // longer than the timeout, must be up throughout.
            let deadline = Instant::now() + PATIENCE;
            while !witness.dropped_by_node() {
                assert!(Instant::now() < deadline, "the witness was never dropped");
                still_up(&mut client, TIMEOUT * 2);
                witness.send(&increment_request(1), Answer::Acked);
                still_up(&mut client, QUIET);
            }

            rig.hold.release();
            client.finish();
        }

        /// A connection resumed after a park longer than the idle timeout
        /// is not dropped as idle at once, though nothing is read from it
        /// after it resumes: its idle clock restarts when it resumes.
        ///
        /// The client sends one more refused request than the queue holds,
        /// all of it in the node's buffer before the last one parks the
        /// connection, and then nothing: no read after the resume restarts
        /// the clock in its place.
        ///
        /// The idle check is throttled (it runs on the slow checks'
        /// cadence), so the connection outliving the resume proves nothing
        /// unless the check has run since. A witness connection proves it:
        /// idle since a write it sends while the client is parked, it falls
        /// due shortly after the resume, and the client is checked only
        /// once the witness has been dropped.
        #[test]
        fn a_resumed_connection_restarts_its_idle_clock() {
            /// The idle timeout. The slow check that drops the witness must
            /// come round within `TIMEOUT - WITNESS_LEAD` of the witness
            /// falling due, or it finds the client's restarted clock run
            /// out too; asserted below, so a cadence too slow for it fails
            /// with its cause named.
            const TIMEOUT: Duration = Duration::from_secs(4);
            /// How long the client stays parked: longer than the timeout,
            /// so the last bytes it read are older than that when it
            /// resumes.
            const PARKED_FOR: Duration = Duration::from_secs(6);
            /// How long after the resume the witness falls due: well
            /// inside the timeout, which the resumed client's restarted
            /// clock has from the resume on.
            const WITNESS_LEAD: Duration = Duration::from_millis(500);
            const EARLY_CAPACITY: usize = 4;
            let rig = Rig::start(
                EARLY_CAPACITY,
                SocketBuffers::DEFAULT.rx_buf_size,
                Some(TIMEOUT),
            );
            let mut client = rig.client();
            let mut witness = rig.client();
            for _ in 0..=EARLY_CAPACITY {
                client.send(&MALFORMED, Answer::Malformed);
            }
            still_up(&mut client, PARKED_FOR - TIMEOUT + WITNESS_LEAD);
            // A write: published at once (only the client is parked), its
            // reply held with the stage. The witness's last bytes.
            witness.send(&increment_request(1), Answer::Acked);
            still_up(&mut client, TIMEOUT - WITNESS_LEAD);
            assert!(
                client.answers.is_empty(),
                "answered while the stage is held"
            );
            assert_eq!(client.conn.unsent(), 0, "the burst is the node's");
            // Parked once, and counted parked; the refusals queued before
            // the park counted, the one it parked on not yet.
            assert_eq!((rig.metrics.parked(), rig.metrics.parks()), (1, 1));
            assert_eq!(
                rig.metrics.counts().refused_as(RefusalKind::Malformed),
                EARLY_CAPACITY as u64
            );

            let released_at = Instant::now();
            rig.hold.release();
            client.finish();
            // Resumed: no longer parked (the gauge is set at the end of the
            // pass that resumed it, which may trail the answers), parked
            // only the once, and every refusal counted once.
            let deadline = Instant::now() + PATIENCE;
            while rig.metrics.parked() != 0 {
                assert!(Instant::now() < deadline, "still counted as parked");
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(rig.metrics.parks(), 1);
            assert_eq!(
                rig.metrics.counts().refused_as(RefusalKind::Malformed),
                EARLY_CAPACITY as u64 + 1
            );
            // The stage, let go, drains the two probes' closes (one per
            // `rig.client()`), leaving the client and the witness; the
            // witness's idle drop takes the count to one. An idle check
            // has run since the resume, then, and found the client inside
            // its restarted clock. With a stale one, it would have been
            // dropped by that check, or an earlier one.
            let deadline = Instant::now() + PATIENCE;
            while rig.active.load(Ordering::Relaxed) > 1 {
                assert!(Instant::now() < deadline, "the witness was never dropped");
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                released_at.elapsed() < TIMEOUT,
                "the witness was dropped {:?} after the resume: the slow checks' \
                 cadence is too slow for this test's TIMEOUT",
                released_at.elapsed()
            );
            // The idle drop closes without a word to the client: a request
            // is what finds the connection gone, reset by the node's stack.
            client.send(&increment_request(1), Answer::Acked);
            client.finish();
            drop(witness);
        }

        /// A parked connection whose retry closes it (the node superseded
        /// while it waits) is closed: its client sees the close at once,
        /// with the response stage still held, and the stage, let go,
        /// forgets it as it does any closed connection.
        #[test]
        fn a_parked_connection_closed_by_its_retry_is_closed() {
            let rig = Rig::start(4, SocketBuffers::DEFAULT.rx_buf_size, None);
            let mut client = rig.client();
            client.send_until_stalled();
            rig.fence.fence();

            let deadline = Instant::now() + PATIENCE;
            loop {
                let now = client.now_ns();
                match client.conn.poll(&mut client.stream, now) {
                    Ok(None) => {}
                    Ok(Some(frame)) => panic!("answered while the stage is held: {frame:?}"),
                    Err(melin_client::Error::Disconnected | melin_client::Error::Io(_)) => break,
                    Err(e) => panic!("not a close: {e:?}"),
                }
                assert!(
                    Instant::now() < deadline,
                    "the parked connection was not closed"
                );
                std::thread::sleep(Duration::from_millis(10));
            }

            // Both connections (the launcher's probe, then the client's)
            // counted in, and out once the stage drains their closes.
            rig.hold.release();
            while rig.active.load(Ordering::Relaxed) != 0 {
                assert!(
                    Instant::now() < deadline,
                    "the stage still holds a connection"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        /// A connection that parks holding more than the buffer cap
        /// (`MAX_PARSE_BUF`) is kept: those bytes are the node's backlog.
        ///
        /// Two answers fill the queue (the smallest it is built with), so
        /// each step of the stage lets the parked connection through two
        /// requests at most. Stepped until it resumes, its next receive
        /// takes the contiguous chunk of the node's receive ring (sized
        /// well past the cap), and it parks again at once, holding all of
        /// it. That chunk is nearly the whole buffer only because the ring
        /// has not wrapped yet: the connection first parks a few KiB into
        /// a fresh socket. The client sees that receive as room for as
        /// much again: the node's window had been closed since the stall,
        /// and only a receive opens it, and a connection resumed is read
        /// once before it parks. So the pin is inferred from the client's
        /// window, not observed on the node; if the traffic before the
        /// first park ever grows to near the ring's size, the first chunk
        /// could fall under the cap and this test pass without pinning it.
        #[test]
        fn a_connection_parked_past_the_buffer_cap_is_kept() {
            const RX_BUF: usize = 4 * MAX_PARSE_BUF;
            let rig = Rig::start(2, RX_BUF, None);
            let mut client = rig.client();
            client.send_until_stalled();

            let stalled_at = client.taken();
            let deadline = Instant::now() + PATIENCE;
            while client.taken() - stalled_at <= 2 * MAX_PARSE_BUF {
                assert!(
                    Instant::now() < deadline,
                    "the node never took its receive buffer: {} bytes since the stall",
                    client.taken() - stalled_at
                );
                rig.hold.step();
                std::thread::sleep(Duration::from_millis(2));
                client.top_up();
                client.poll();
            }
            // Parked past the cap now, which the buffer check, run on
            // every iteration, would have dropped at once.
            still_up(&mut client, QUIET);

            rig.hold.release();
            client.finish();
        }
    }
}
