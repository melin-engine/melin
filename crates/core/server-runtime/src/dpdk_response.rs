//! DPDK response stage — encodes responses and queues them for the DPDK
//! poll thread instead of writing to kernel sockets.
//!
//! The response stage still runs on its own pinned thread for cursor
//! gating and response encoding. Instead of calling `write_all` on kernel
//! sockets, it pushes `(connection_id, encoded_bytes)` into a shared
//! lock-free queue. The DPDK poll thread drains this queue into smoltcp
//! TCP sockets during each poll iteration.
//!
//! This decoupling is necessary because smoltcp is single-threaded — only
//! the DPDK poll thread can call `socket.send_slice()`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use melin_disruptor::ring;
use melin_disruptor::spsc;

use melin_app::Application;
use melin_app::amortized_timer::AmortizedTimer;
use melin_transport_core::pipeline::{OutputPayload, OutputSlot, StageUtilization};

use melin_protocol::codec;
use melin_protocol::message::ResponseKind;

#[cfg(feature = "latency-trace")]
use melin_transport_core::trace;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded response size. PositionSnapshot is the largest variant
/// at up to 330 bytes.
const MAX_RESPONSE_BUF: usize = 512;

/// Maximum wire frame size: 4-byte length prefix + MAX_RESPONSE_BUF payload.
const MAX_TX_FRAME: usize = 4 + MAX_RESPONSE_BUF;

/// An encoded frame destined for a specific connection.
/// Sent from the response stage to the DPDK poll thread via lock-free SPSC.
///
/// Fixed-size and `Copy` to fit the SPSC queue's requirements (no heap
/// allocation per frame). Trading responses are small (~20-80 bytes),
/// well within the 132-byte slot.
#[derive(Clone, Copy)]
pub struct TxFrame {
    pub connection_id: u64,
    /// Number of valid bytes in `data`.
    pub len: u16,
    /// Wire frame: [u32 length prefix][payload]. Only `data[..len]` is valid.
    pub data: [u8; MAX_TX_FRAME],
}

impl Default for TxFrame {
    fn default() -> Self {
        TxFrame {
            connection_id: 0,
            len: 0,
            data: [0u8; MAX_TX_FRAME],
        }
    }
}

impl TxFrame {
    /// The valid wire frame bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
}

/// Control plane events for connection registration (DPDK variant).
///
/// Unlike the TCP variant, this doesn't carry a socket writer —
/// the DPDK poll thread owns all socket state.
pub enum ControlEvent {
    /// A new connection was accepted by the DPDK poll thread.
    Connected { connection_id: u64 },
    /// A connection was closed.
    Disconnected { connection_id: u64 },
}

/// Run the DPDK response stage loop. Blocks the calling thread until shutdown.
///
/// Identical to the TCP response stage except:
/// - No socket writers — encoded frames are sent via `tx_out` channel
/// - No flush syscalls — the DPDK poll thread handles transmission
/// - Heartbeats are sent via the same `tx_out` channel
///
/// Top-level thread entry point — the wide arg list mirrors stage state
/// owned elsewhere; bundling into a config struct adds indirection
/// without simplifying.
#[allow(clippy::too_many_arguments)]
// `journal_persisted_wire_seq`: highest wire seq durably persisted on
// this node's journal — same semantics as
// `response::Response::journal_persisted_wire_seq`. See that field's docs
// for why the gate must use wire-seq space rather than the
// journal-consumer cursor.
pub fn run<A: Application>(
    mut consumer: ring::Consumer<OutputSlot<A::Report, A::QueryResponse>>,
    control_rx: mpsc::Receiver<ControlEvent>,
    journal_persisted_wire_seq: Arc<AtomicU64>,
    durability_mode: Arc<std::sync::atomic::AtomicU8>,
    replication_metrics: Option<Arc<crate::replication::ReplicationMetrics>>,
    replica_active: Option<[Arc<AtomicBool>; 2]>,
    shutdown: &AtomicBool,
    heartbeat_interval: Option<Duration>,
    active_connections: Arc<AtomicU64>,
    mut tx_producers: Vec<spsc::Producer<TxFrame>>,
    utilization: Arc<StageUtilization>,
    busy_spin: bool,
    encoder: crate::response::ResponseEncoderArc<A>,
) {
    // Mirrors `response::run`: derive the local Policy from the shared
    // mode atomic and observe runtime swaps from the admin
    // `DURABILITY` command.
    use crate::durability_policy::DurabilityMode;
    let mut active_mode =
        DurabilityMode::from_u8(durability_mode.load(Ordering::Relaxed)).unwrap_or_else(|| {
            tracing::error!(
                "durability_mode atomic held a corrupted byte at startup; defaulting to hybrid (DPDK)"
            );
            DurabilityMode::Hybrid
        });
    let mut policy = active_mode.to_policy();
    // Track known connections (for heartbeat scheduling).
    let mut connections: HashMap<u64, ConnectionHeartbeat> = HashMap::with_capacity(256);

    let mut batch = [OutputSlot::<A::Report, A::QueryResponse>::default(); MAX_BATCH];
    let mut encode_buf = [0u8; MAX_RESPONSE_BUF];

    // Cached durability position (see response.rs for full explanation).
    // Initialised below from the policy's startup evaluation.
    let mut cached_durable_pos: u64;

    // Degradation logger — same scheme as the TCP response stage
    // (see `response::run`). Initialised below from an explicit
    // policy evaluation so a degraded startup state shows up on
    // `/healthz` and in the journal even before the first batch.
    let startup_now = Instant::now();
    let mut last_policy_check = startup_now;
    const DEGRADED_LOG_INTERVAL: Duration = Duration::from_secs(5);
    const POLICY_CHECK_INTERVAL: Duration = Duration::from_secs(1);

    let mut degraded_logger;
    {
        let journal_pos = journal_persisted_wire_seq.load(Ordering::Acquire);
        let metrics_ref = replication_metrics.as_deref();
        let active_ref = replica_active.as_ref();
        let status =
            crate::response::evaluate_durability(&policy, journal_pos, metrics_ref, active_ref);
        cached_durable_pos = status.durable_pos;
        utilization
            .policy_degraded
            .store(status.degraded, Ordering::Relaxed);
        degraded_logger = if status.degraded {
            crate::response::DegradationLogger::new_starting_degraded(startup_now, &policy)
        } else {
            crate::response::DegradationLogger::new(startup_now)
        };
    }

    // Pre-encode heartbeat frame (fixed-size, no heap allocation).
    let mut heartbeat_frame = [0u8; 8];
    let heartbeat_len = codec::encode_response(&ResponseKind::Heartbeat, &mut heartbeat_frame)
        .expect("heartbeat encodes");

    let mut last_heartbeat_scan = Instant::now();
    // Gate the heartbeat scan's clock read so the count==0 spin doesn't
    // spend the response thread's CPU on `__vdso_clock_gettime`. Reads
    // the clock every ~1 M idle iterations under busy_spin; heartbeat
    // interval is seconds, so this is plenty.
    let mut heartbeat_timer = AmortizedTimer::new();
    let mut idle_spins: u32 = 0;
    let mut busy_count: u64 = 0;
    let mut idle_count: u64 = 0;

    // Stage histograms — mirror the TCP response stage but without
    // an `egress` histogram. DPDK egress lives in the poll thread
    // (`dpdk_transport.rs`), which is where the actual TX happens;
    // sampling here would only capture SPSC-publish time and
    // mislead the bench's tick-to-trade breakdown.
    #[cfg(feature = "latency-trace")]
    let mut spsc_rec =
        trace::register_stage("response: SPSC wakeup (matching publish → response consume)");
    #[cfg(feature = "latency-trace")]
    let mut dispatch_rec = trace::register_stage("response: dispatch (consume → SPSC publish)");
    #[cfg(feature = "latency-trace")]
    let mut server_e2e_rec =
        trace::register_stage("server e2e (reader recv → response SPSC publish)");
    #[cfg(feature = "tick-to-trade")]
    let mut journal_wait_rec =
        trace::register_stage("response: journal-wait (match_complete → journal cursor crossed)");
    #[cfg(feature = "tick-to-trade")]
    let mut replica_wait_rec = trace::register_stage(
        "response: replica-wait (match_complete → replication cursor crossed)",
    );
    #[cfg(feature = "tick-to-trade")]
    let mut encode_rec = trace::register_stage("response: encode (per-kind wire encoding)");

    loop {
        // Observe runtime mode swaps from the admin `DURABILITY`
        // command. See `response::run` for the design rationale.
        let observed_byte = durability_mode.load(Ordering::Relaxed);
        if observed_byte != active_mode.as_u8() {
            match DurabilityMode::from_u8(observed_byte) {
                Some(next) => {
                    tracing::info!(
                        prev = active_mode.as_str(),
                        next = next.as_str(),
                        "durability mode swapped at runtime (DPDK)"
                    );
                    active_mode = next;
                    policy = active_mode.to_policy();
                    cached_durable_pos = 0;
                    degraded_logger = crate::response::DegradationLogger::new(Instant::now());
                }
                None => {
                    tracing::error!(
                        byte = observed_byte,
                        "durability_mode atomic held a corrupted byte; retaining prior mode (DPDK)"
                    );
                }
            }
        }

        if shutdown.load(Ordering::Relaxed) {
            utilization.busy.store(busy_count, Ordering::Relaxed);
            utilization.idle.store(idle_count, Ordering::Relaxed);
            return;
        }

        // Poll control channel for connect/disconnect.
        // Counter accounting: the response stage is the sole owner of
        // active_connections decrements. The poll thread increments on
        // auth success and sends ControlEvent::Disconnected on close.
        process_control_events(
            &control_rx,
            &mut connections,
            &active_connections,
            last_heartbeat_scan,
        );

        // Consume output slots from matching stage.
        let count = consumer.consume_batch(&mut batch, MAX_BATCH);
        if count == 0 {
            idle_count += 1;
            if idle_count.is_multiple_of(1024) {
                utilization.busy.store(busy_count, Ordering::Relaxed);
                utilization.idle.store(idle_count, Ordering::Relaxed);
            }

            // Heartbeat scan, gated by AmortizedTimer to keep the busy-
            // spin path off `clock_gettime`. Without this, perf-annotate
            // showed ~22 % of the response thread's CPU on the vDSO.
            if let Some(interval) = heartbeat_interval
                && heartbeat_timer
                    .tick(Duration::from_secs(1), busy_spin || idle_spins < 1000)
                    .is_some()
            {
                let now = Instant::now();
                last_heartbeat_scan = now;
                let mut failed: Vec<u64> = Vec::new();
                for (&conn_id, state) in connections.iter_mut() {
                    if now.duration_since(state.last_send) >= interval {
                        let mut frame = TxFrame {
                            connection_id: conn_id,
                            len: heartbeat_len as u16,
                            ..Default::default()
                        };
                        frame.data[..heartbeat_len]
                            .copy_from_slice(&heartbeat_frame[..heartbeat_len]);
                        let tid = (conn_id >> 56) as usize % tx_producers.len();
                        if tx_producers[tid].try_publish(frame).is_err() {
                            // SPSC full — DPDK poll thread fell behind.
                            failed.push(conn_id);
                            continue;
                        }
                        state.last_send = now;
                    }
                }
                for conn_id in failed {
                    connections.remove(&conn_id);
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                }
            }

            // Re-evaluate the policy on a slow timer so the
            // `policy_degraded` flag and warn-log track the cluster
            // state even when no batches are flowing. See response.rs
            // for the rationale.
            {
                let now_ts = Instant::now();
                if now_ts.duration_since(last_policy_check) >= POLICY_CHECK_INTERVAL {
                    last_policy_check = now_ts;
                    let journal_pos = journal_persisted_wire_seq.load(Ordering::Acquire);
                    let metrics_ref = replication_metrics.as_deref();
                    let active_ref = replica_active.as_ref();
                    let status = crate::response::evaluate_durability(
                        &policy,
                        journal_pos,
                        metrics_ref,
                        active_ref,
                    );
                    degraded_logger.tick(
                        &policy,
                        &utilization,
                        status.degraded,
                        now_ts,
                        DEGRADED_LOG_INTERVAL,
                    );
                    cached_durable_pos = status.durable_pos;
                }
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
        busy_count += 1;

        #[cfg(feature = "latency-trace")]
        let consume_ts = trace::mono_trace_ns();

        // Per-slot journal-wait / replica-wait tracker. Same shape as
        // the TCP response — see `crate::response::GateCrossTracker`
        // for the rationale and edge cases.
        #[cfg(feature = "tick-to-trade")]
        let mut gate_tracker;

        // Wait for durability (see response.rs for full explanation).
        {
            // Gate on `wire_seq` (matches `response::run`) — see that
            // module's notes on the input-seq vs wire-seq space mismatch
            // that motivated the field. With the wire-seq stamp, `needed`
            // is the exact wire seq the gate must observe to consider
            // the batch's latest event durable; no `+1` (the old `+1`
            // compensated for the input-seq off-by-(starting-1), which
            // is gone now).
            let needed = batch[..count]
                .iter()
                .map(|s| s.wire_seq)
                .max()
                .expect("non-empty batch");
            #[cfg(feature = "tick-to-trade")]
            {
                gate_tracker = crate::response::GateCrossTracker::new(needed);
            }
            if cached_durable_pos < needed {
                loop {
                    // Observe a mode swap mid-gate-wait so a stuck
                    // batch can be unblocked by an operator
                    // `DURABILITY <mode>` command. See `response.rs`
                    // for the rationale and ordering choice.
                    let observed_byte = durability_mode.load(Ordering::Relaxed);
                    if observed_byte != active_mode.as_u8()
                        && let Some(next) = DurabilityMode::from_u8(observed_byte)
                    {
                        tracing::info!(
                            prev = active_mode.as_str(),
                            next = next.as_str(),
                            "durability mode swapped during gate wait (DPDK)"
                        );
                        active_mode = next;
                        policy = active_mode.to_policy();
                        degraded_logger = crate::response::DegradationLogger::new(Instant::now());
                    }

                    let journal_pos = journal_persisted_wire_seq.load(Ordering::Acquire);
                    let metrics_ref = replication_metrics.as_deref();
                    let active_ref = replica_active.as_ref();
                    let repl_min =
                        crate::response::connected_persisted_min(metrics_ref, active_ref);

                    #[cfg(feature = "tick-to-trade")]
                    gate_tracker.observe(journal_pos, repl_min, trace::mono_trace_ns());

                    let status = crate::response::evaluate_durability(
                        &policy,
                        journal_pos,
                        metrics_ref,
                        active_ref,
                    );
                    cached_durable_pos = status.durable_pos;
                    utilization
                        .policy_degraded
                        .store(status.degraded, Ordering::Relaxed);
                    if cached_durable_pos >= needed {
                        // Attribution: which subsystem was slowest. See
                        // response.rs for the rationale.
                        if journal_pos <= repl_min {
                            utilization.gate_journal.fetch_add(1, Ordering::Relaxed);
                        } else {
                            utilization.gate_replication.fetch_add(1, Ordering::Relaxed);
                        }
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }

        // One Instant::now() per batch for heartbeat tracking instead of
        // per response — heartbeat interval is 10s, sub-ms precision is plenty.
        let batch_now = Instant::now();

        // Log degradation transitions / heartbeat after the gate
        // opens. Same scheme as the TCP response stage.
        let degraded_now = utilization.policy_degraded.load(Ordering::Relaxed);
        degraded_logger.tick(
            &policy,
            &utilization,
            degraded_now,
            batch_now,
            DEGRADED_LOG_INTERVAL,
        );
        last_policy_check = batch_now;

        // Encode and queue responses. Each slot expands to at most two
        // wire frames: the payload (Report / QueryResponse / EngineError)
        // and an optional trailing `BatchEnd` when `is_last_in_request`
        // is set. `OutputPayload::BatchEnd` carries no payload of its
        // own — the wire BatchEnd is emitted purely from the flag.
        for slot in &batch[..count] {
            #[cfg(feature = "latency-trace")]
            spsc_rec.record_elapsed(slot.match_complete_ts, consume_ts);

            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.journal_crossed() {
                journal_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }
            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.replica_crossed() {
                replica_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }

            if !connections.contains_key(&slot.connection_id) {
                continue;
            }

            // All frames for this slot share the same destination tid
            // (single connection_id), so we can compute it once and
            // bundle the slot's 1–2 frames under one Release at the end
            // of the slot. Per-slot flush — rather than once per outer
            // batch — keeps individual orders' RTT short under
            // saturation (each request's response ships as soon as
            // encoded) while still letting the DPDK poll thread drain
            // Report + BatchEnd in a single consume cycle for low-rate
            // workloads (consumer-side win for single-order p99).
            let tid = (slot.connection_id >> 56) as usize % tx_producers.len();
            let conn_id = slot.connection_id;

            // Frame 1: application payload (Report / Query via encoder;
            // EngineError via codec). BatchEnd payloads carry no body —
            // the terminator below handles them via is_last_in_request.
            let payload_result: Option<Result<usize, &'static str>> = match slot.payload {
                OutputPayload::Report(ref report) => {
                    Some(encoder.encode_report(report, &mut encode_buf))
                }
                OutputPayload::QueryResponse(ref q) => {
                    Some(encoder.encode_query(q, &mut encode_buf))
                }
                OutputPayload::EngineError => Some(
                    codec::encode_response(&ResponseKind::EngineError, &mut encode_buf)
                        .map_err(|_| "encode error"),
                ),
                OutputPayload::BatchEnd => None,
            };

            if let Some(result) = payload_result {
                #[cfg(feature = "tick-to-trade")]
                let encode_start = trace::mono_trace_ns();
                push_frame(result, conn_id, &mut tx_producers[tid], &encode_buf);
                #[cfg(feature = "tick-to-trade")]
                encode_rec.record_elapsed(encode_start, trace::mono_trace_ns());
            }

            // Frame 2: BatchEnd terminator. Transport-shaped, encoded
            // directly via codec — never reaches the application
            // encoder trait.
            if slot.is_last_in_request {
                let result = codec::encode_response(&ResponseKind::BatchEnd, &mut encode_buf)
                    .map_err(|_| "encode error");
                push_frame(result, conn_id, &mut tx_producers[tid], &encode_buf);
            }

            // Release this slot's frames as a unit. `flush` is a no-op
            // when nothing was pushed (e.g. every kind hit an encode
            // error), so the call is safe regardless.
            tx_producers[tid].flush();
            #[cfg(feature = "latency-trace")]
            if slot.is_last_in_request {
                // Record server-e2e relative to post-flush wall clock —
                // i.e. the moment the DPDK poll thread can see the
                // response.
                server_e2e_rec.record_elapsed(slot.recv_ts, trace::mono_trace_ns());
            }

            if let Some(state) = connections.get_mut(&slot.connection_id) {
                state.last_send = batch_now;
            }
        }

        #[cfg(feature = "latency-trace")]
        dispatch_rec.record_elapsed(consume_ts, trace::mono_trace_ns());
    }
}

/// Push one encoded wire frame onto the DPDK tx ring. Logs encode
/// failures at error level and silently drops the frame — same
/// behaviour as the pre-refactor inline loop. Splitting the
/// responsibility into a helper lets the slot-processing code call
/// it uniformly for application payloads (via the `ResponseEncoder`
/// trait) and transport-shaped frames (via `codec::encode_response`).
#[inline]
fn push_frame(
    result: Result<usize, &'static str>,
    conn_id: u64,
    tx: &mut spsc::Producer<TxFrame>,
    encode_buf: &[u8],
) {
    let written = match result {
        Ok(n) => n,
        Err(reason) => {
            tracing::error!(connection_id = conn_id, reason, "encode error");
            return;
        }
    };
    let len = written as u16;
    tx.push_with(|frame| {
        frame.connection_id = conn_id;
        frame.len = len;
        frame.data[..written].copy_from_slice(&encode_buf[..written]);
    });
}

/// Per-connection heartbeat state. No socket writer — the DPDK poll
/// thread owns socket state.
struct ConnectionHeartbeat {
    last_send: Instant,
}

/// Process a batch of control events, updating the connection map and
/// active_connections counter.
///
/// Extracted from the `run()` loop so the counter accounting invariant
/// can be unit-tested: the response stage is the **sole owner** of
/// `active_connections` decrements. The poll thread increments on auth
/// success and sends `Disconnected`; this function handles the decrement.
fn process_control_events(
    control_rx: &mpsc::Receiver<ControlEvent>,
    connections: &mut HashMap<u64, ConnectionHeartbeat>,
    active_connections: &AtomicU64,
    now: Instant,
) {
    while let Ok(event) = control_rx.try_recv() {
        match event {
            ControlEvent::Connected { connection_id } => {
                connections.insert(connection_id, ConnectionHeartbeat { last_send: now });
            }
            ControlEvent::Disconnected { connection_id } => {
                if connections.remove(&connection_id).is_some() {
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc;
    use std::time::Instant;

    /// Simulate the poll thread's side: increment counter on auth, send
    /// Disconnected on close. The response stage (process_control_events)
    /// owns the decrement.
    #[test]
    fn active_connections_single_lifecycle() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = HashMap::new();
        let now = Instant::now();

        // Poll thread: auth succeeds → increment.
        counter.fetch_add(1, Ordering::Relaxed);
        tx.send(ControlEvent::Connected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(connections.len(), 1);

        // Poll thread: connection closes → send Disconnected (no decrement).
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(connections.len(), 0);
    }

    /// Disconnected for an unknown connection (e.g., pre-auth drop or
    /// duplicate event) must not decrement the counter.
    #[test]
    fn disconnect_unknown_connection_no_decrement() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = HashMap::new();
        let now = Instant::now();

        tx.send(ControlEvent::Disconnected { connection_id: 999 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        // Counter must stay at 0 — not wrap to u64::MAX.
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    /// Multiple connections with interleaved connect/disconnect.
    #[test]
    fn active_connections_multiple_lifecycle() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = HashMap::new();
        let now = Instant::now();

        // Three connections authenticate.
        for id in 1..=3 {
            counter.fetch_add(1, Ordering::Relaxed);
            tx.send(ControlEvent::Connected { connection_id: id })
                .unwrap();
        }
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert_eq!(connections.len(), 3);

        // Connection 2 disconnects.
        tx.send(ControlEvent::Disconnected { connection_id: 2 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 2);
        assert_eq!(connections.len(), 2);

        // Remaining two disconnect.
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        tx.send(ControlEvent::Disconnected { connection_id: 3 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(connections.len(), 0);
    }

    /// Duplicate Disconnected for the same connection must only decrement
    /// once (the second remove returns None).
    #[test]
    fn duplicate_disconnect_single_decrement() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = HashMap::new();
        let now = Instant::now();

        counter.fetch_add(1, Ordering::Relaxed);
        tx.send(ControlEvent::Connected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);

        // Two Disconnected events for the same connection.
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
