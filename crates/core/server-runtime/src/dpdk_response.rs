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

use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use melin_pipeline::ring;
use melin_pipeline::spsc;
use melin_pipeline::wait::WaitStrategy;

use crate::durability_gate::{
    Admission, Backing, BatchEnds, DurabilityGate, GateInputs, GatePolicy,
};
use crate::halt::EarlyReplyQueue;
use melin_app::Application;
use melin_app::amortized_timer::AmortizedTimer;
use melin_transport_core::DurableWireSeqCursor;
use melin_transport_core::pipeline::{OutputSlot, StageUtilization};

use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec::{self, BATCH_END_FRAME_LEN};

use crate::response_frame::{
    EarlyReplyBuf, EncodeBuf, MAX_APP_FRAME, MAX_EARLY_REPLY, encode_early_reply,
    encode_slot_payload,
};

#[cfg(feature = "latency-trace")]
use melin_transport_core::trace;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum wire frame size: the largest framed application response, plus
/// room for the batch terminator (`BatchEnd` or `BatchEndDegraded`, the
/// wire protocol's `BATCH_END_FRAME_LEN`, its request ID included) carried
/// in the same frame when the slot closes a request.
const MAX_TX_FRAME: usize = MAX_APP_FRAME + BATCH_END_FRAME_LEN;

// An early reply (a rejection and its terminator, or a refusal with the
// longest reason) goes out as one frame too.
const _: () = assert!(MAX_EARLY_REPLY <= MAX_TX_FRAME);

/// An encoded frame destined for a specific connection.
/// Sent from the response stage to the DPDK poll thread via lock-free SPSC.
///
/// Fixed-size and `Copy` to fit the SPSC queue's requirements (no heap
/// allocation per frame). The slot is sized for the largest framed
/// response an application's encoder may produce (`MAX_APP_FRAME`), plus
/// the `BatchEnd` terminator.
///
/// A frame carries the response *and* the `BatchEnd` terminator when the slot
/// closes a request. The struct is copied by value out of the SPSC on the poll
/// thread, which is also the ingress thread, so one frame per request rather
/// than two halves both that copy and the per-frame `queue_send` work.
#[derive(Clone, Copy)]
pub struct TxFrame {
    pub connection_id: u64,
    /// Number of valid bytes in `data`.
    pub len: u16,
    /// Wire frame: `[u32 length prefix][payload]`. Only `data[..len]` is valid.
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
    journal_persisted_wire_seq: DurableWireSeqCursor,
    policy: GatePolicy,
    replication_metrics: Option<Arc<crate::replication::ReplicationMetrics>>,
    replica_active: Option<[Arc<AtomicBool>; 2]>,
    shutdown: &AtomicBool,
    heartbeat_interval: Option<Duration>,
    active_connections: Arc<AtomicU64>,
    mut tx_producers: Vec<spsc::Producer<TxFrame>>,
    utilization: Arc<StageUtilization>,
    wait: WaitStrategy,
    encoder: crate::response::ResponseEncoderArc<A>,
    // The answers the poll thread decided on without publishing the
    // request (halt rejections, filtered, refused and shed requests),
    // each sent once everything published before it is answered — see
    // `crate::halt`.
    mut early_replies: EarlyReplyQueue<A::Report>,
    // Release of held replies on this node's own fsync while halted;
    // `None` when off. See `crate::halt::DegradedRelease`.
    degraded_release: Option<crate::halt::DegradedRelease>,
) {
    // The ack policy in force and the durability gate — the same state
    // machine the kernel response stage runs. See `DurabilityGate`.
    let mut gate = DurabilityGate::new(
        GateInputs {
            journal_persisted_wire_seq,
            replication_metrics,
            replica_active,
            utilization: Arc::clone(&utilization),
            wait,
            degraded_release,
        },
        policy,
        "dpdk response",
    );
    // Track known connections (for heartbeat scheduling).
    // FxHash over SipHash: keys are server-generated connection ids, never
    // attacker-chosen, so HashDoS resistance buys nothing and the default
    // hasher costs more per lookup than the lookup itself.
    let mut connections: FxHashMap<u64, ConnectionHeartbeat> =
        FxHashMap::with_capacity_and_hasher(256, Default::default());

    let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
    let mut early_buf: EarlyReplyBuf = [0u8; MAX_EARLY_REPLY];

    // The two request terminators are the same bytes on every request but
    // for the request's ID, so they are encoded once here and the ID
    // written in per request — see `response::run`. Each is
    // `BATCH_END_FRAME_LEN` bytes, the room `MAX_TX_FRAME` keeps for it.
    let mut batch_ends = BatchEnds::new();

    // Pre-encode heartbeat frame (fixed-size, no heap allocation).
    let mut heartbeat_frame = [0u8; 8];
    let heartbeat_len = control_codec::encode_transport_response(
        &TransportResponse::Heartbeat,
        &mut heartbeat_frame,
    )
    .expect("heartbeat encodes");

    // Gate the heartbeat scan's clock read so the count==0 spin doesn't
    // spend the response thread's CPU on `__vdso_clock_gettime`. Reads
    // the clock every ~1 M idle iterations while spinning; heartbeat
    // interval is seconds, so this is plenty.
    let mut heartbeat_timer = AmortizedTimer::new();
    let mut waiter = wait.waiter();
    let mut busy_count: u64 = 0;
    let mut idle_count: u64 = 0;
    // Input sequence of the last output slot handled — early replies are
    // released only where it changes. See `response::run`.
    let mut last_input_seq = u64::MAX;

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
    // Paces the idle-path recorder flush — see `response::run`.
    #[cfg(feature = "latency-trace")]
    let mut last_stats_flush = Instant::now();

    'run: loop {
        // Fencing note: a superseded ex-primary exits through this same
        // shutdown branch (`FenceState::fence_if_superseded` co-sets
        // `shutdown`). Unlike the kernel response stage there is no
        // best-effort flush here to suppress, so the fence needs no
        // dedicated check — exiting stops releasing response frames onto
        // the TX rings, which is the whole ack gate.
        if shutdown.load(Ordering::Relaxed) {
            utilization.busy.store(busy_count, Ordering::Relaxed);
            utilization.idle.store(idle_count, Ordering::Relaxed);
            return;
        }

        // Borrow output slots from the matching stage in place — see
        // `response::run` for why this is a borrow and not a copy, and
        // for the consequence of publishing progress after the batch
        // instead of before it. Early replies follow the order
        // `EarlyReplyQueue` documents: the idle bound before the read, the
        // sync between the read and the control drain.
        let idle_early_bound = early_replies.idle_bound();
        let slots = consumer.read_contiguous(MAX_BATCH);
        let early_waiting = early_replies.sync();

        // Poll control channel for connect/disconnect — after the ring
        // is read, never before: the poll thread queues `Connected` on
        // authentication and parses that connection's requests only
        // afterwards, so every slot read above has its `Connected`
        // already queued, and this drain sees it. See `response::run`
        // for the reply this order protects.
        // Counter accounting: the response stage is the sole owner of
        // active_connections decrements. The poll thread increments on
        // auth success and sends ControlEvent::Disconnected on close.
        process_control_events(
            &control_rx,
            &mut connections,
            &active_connections,
            Instant::now,
        );

        if slots.is_empty() {
            if let Some(bound) = idle_early_bound {
                let now = Instant::now();
                early_replies.release(bound, |reply| {
                    push_early_reply(
                        reply,
                        &*encoder,
                        &mut connections,
                        &mut tx_producers,
                        &mut early_buf,
                        now,
                    );
                });
            }

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
                    .tick(Duration::from_secs(1), waiter.spinning())
                    .is_some()
            {
                let now = Instant::now();
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
                gate.idle_recheck(now_ts);

                // Hand buffered latency samples to the stats registry
                // while idle. Reuses the policy check's clock read —
                // see `response::run` for why this must happen at all.
                #[cfg(feature = "latency-trace")]
                if now_ts.duration_since(last_stats_flush) >= trace::IDLE_FLUSH_INTERVAL {
                    last_stats_flush = now_ts;
                    spsc_rec.flush();
                    dispatch_rec.flush();
                    server_e2e_rec.flush();
                    #[cfg(feature = "tick-to-trade")]
                    {
                        journal_wait_rec.flush();
                        replica_wait_rec.flush();
                        encode_rec.flush();
                    }
                }
            }

            waiter.idle();
            continue;
        }
        waiter.reset();
        busy_count += 1;

        #[cfg(feature = "latency-trace")]
        let consume_ts = trace::mono_trace_ns();

        // One Instant::now() per batch for heartbeat tracking instead of
        // per response — heartbeat interval is 10s, sub-ms precision is plenty.
        let batch_now = Instant::now();

        // Encode and queue responses. Each slot expands to at most two
        // wire frames: the payload (Report / QueryResponse) and an
        // optional trailing `BatchEnd`, naming the slot's request, when
        // `is_last_in_request` is set. `OutputPayload::BatchEnd` carries
        // no payload of its own — the wire BatchEnd is emitted purely
        // from the flag.
        //
        // The durability gate is evaluated per slot, inline — see
        // `response::run` for why batch-max gating was a head-of-line
        // block and why the extra waits are self-limiting.
        for slot in slots {
            #[cfg(feature = "latency-trace")]
            spsc_rec.record_elapsed(slot.match_complete_ts, consume_ts);

            // First slot of an event: early replies stamped up to its
            // sequence go first, ahead of the gate. See `response::run`.
            let first_of_event = slot.input_seq != last_input_seq;
            last_input_seq = slot.input_seq;
            if early_waiting && first_of_event {
                early_replies.release(slot.input_seq, |reply| {
                    push_early_reply(
                        reply,
                        &*encoder,
                        &mut connections,
                        &mut tx_producers,
                        &mut early_buf,
                        batch_now,
                    );
                });
            }

            // Per-slot journal-wait / replica-wait tracker. Same shape as
            // the TCP response — see `crate::response::GateCrossTracker`
            // for the rationale and edge cases.
            #[cfg(feature = "tick-to-trade")]
            let mut gate_tracker = crate::response::GateCrossTracker::new(slot.wire_seq);

            // Gate on `wire_seq` (matches `response::run`) — see that
            // module's notes on the input-seq vs wire-seq space mismatch
            // that motivated the field. With the wire-seq stamp, the
            // slot's own `wire_seq` is the exact value the gate must
            // observe for that slot's event to count as durable; no `+1`
            // (the old `+1` compensated for the input-seq
            // off-by-(starting-1), which is gone now).
            //
            // A policy change slot is the switch point (see
            // `DurabilityGate::admit`). Nothing to flush before a wait:
            // every slot's frames are released as soon as it is encoded.
            //
            // A gate that cannot open does not hold the shutdown sequence;
            // the reply the policy never confirmed is not queued. A policy
            // change slot is the gate's own: its frames go to no
            // connection, whatever it is addressed to. What backs a reply
            // decides its terminator: the policy, unless the gate released
            // it on this node's own fsync while halted.
            let (destination, backing) = match gate.admit(
                slot,
                shutdown,
                #[cfg(feature = "tick-to-trade")]
                &mut gate_tracker,
                || {},
            ) {
                Admission::Reply {
                    connection_id,
                    backing,
                } => (Some(connection_id), backing),
                Admission::GateOnly => (None, Backing::Policy),
                Admission::Shutdown => continue 'run,
            };

            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.journal_crossed() {
                journal_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }
            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.replica_crossed() {
                replica_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }

            // One lookup, not two: the entry decides whether the
            // connection is still known, and the same borrow stamps its
            // heartbeat clock once the frame is queued.
            let Some((conn_id, conn_state)) =
                destination.and_then(|id| connections.get_mut(&id).map(|state| (id, state)))
            else {
                // A reply for a connection this stage does not hold: it
                // is gone. The drain runs after the ring is read, so a
                // connection that is merely new is never a miss. Logged
                // so a lost reply leaves a trace. Connection 0 is a
                // server-originated event — a seed, a tick — whose
                // reports have no client to go to, and is skipped
                // without a word.
                if let Some(connection_id) = destination.filter(|&id| id != 0) {
                    tracing::debug!(
                        connection_id,
                        wire_seq = slot.wire_seq,
                        "reply dropped: connection not registered"
                    );
                }
                continue;
            };

            // All frames for this slot share the same destination tid
            // (single connection_id), so we can compute it once and
            // bundle the slot's 1–2 frames under one Release at the end
            // of the slot. Per-slot flush — rather than once per outer
            // batch — keeps individual requests' RTT short under
            // saturation (each request's response ships as soon as
            // encoded) while still letting the DPDK poll thread drain
            // Report + BatchEnd in a single consume cycle for low-rate
            // workloads (consumer-side win for single-request p99).
            let tid = (conn_id >> 56) as usize % tx_producers.len();

            // Frame 1: application payload (Report / Query via encoder).
            // BatchEnd payloads carry no body — the terminator below
            // handles them via is_last_in_request.
            let payload_result = encode_slot_payload(&slot.payload, &*encoder, &mut encode_buf);

            // The terminator rides in the same frame: `BatchEnd`, or
            // `BatchEndDegraded` as the slot is backed. Both are
            // transport-shaped and the same bytes every time but for the
            // request's ID, so they are encoded once at startup and the
            // ID written in per request.
            let trailer: &[u8] = if slot.is_last_in_request {
                gate.batch_end(backing, &mut batch_ends, slot.request_id)
            } else {
                &[]
            };

            #[cfg(feature = "tick-to-trade")]
            let encode_start = trace::mono_trace_ns();
            push_frame(
                payload_result,
                conn_id,
                &mut tx_producers[tid],
                &encode_buf,
                trailer,
            );
            #[cfg(feature = "tick-to-trade")]
            encode_rec.record_elapsed(encode_start, trace::mono_trace_ns());

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

            conn_state.last_send = batch_now;
        }

        // Borrowed slots are dead here — hand the batch back to the
        // producer now that every one of them has cleared its gate.
        //
        // Invariant the event publisher depends on: this consumer's
        // progress never passes a slot that has not cleared the gate
        // (the publisher is chained behind it, so this commit releases
        // events to it). See the same commit in `response::run`.
        consumer.commit();

        // Log degradation transitions / re-emit the reminder — after
        // dispatch, not before it. See `DurabilityGate::after_batch`.
        gate.after_batch();

        #[cfg(feature = "latency-trace")]
        dispatch_rec.record_elapsed(consume_ts, trace::mono_trace_ns());
    }
}

/// Push one SPSC frame carrying an encoded payload and/or a pre-encoded
/// trailer onto the DPDK tx ring.
///
/// `payload` is `None` for slot kinds with no body (`BatchEnd`), and `trailer`
/// is empty for slots that do not close a request; a call with neither pushes
/// nothing. Encode failures are logged at error level and drop the payload,
/// but still ship the trailer, so a request that fails to encode one response
/// is still terminated rather than leaving the client waiting.
///
/// # Panics
///
/// If the payload and the trailer together exceed [`MAX_TX_FRAME`]: a bug
/// in what reaches here (`MAX_TX_FRAME` holds the widest application frame
/// and its terminator), caught with its sizes rather than as a slice index
/// panic, in release builds too, for one compare per frame.
#[inline]
fn push_frame(
    payload: Option<Result<usize, &'static str>>,
    conn_id: u64,
    tx: &mut spsc::Producer<TxFrame>,
    encode_buf: &[u8],
    trailer: &[u8],
) {
    let written = match payload {
        Some(Ok(n)) => n,
        Some(Err(reason)) => {
            tracing::error!(connection_id = conn_id, reason, "encode error");
            0
        }
        None => 0,
    };
    if written == 0 && trailer.is_empty() {
        return;
    }
    let total = written + trailer.len();
    assert!(
        total <= MAX_TX_FRAME,
        "a {written}-byte frame and a {}-byte trailer overrun a {MAX_TX_FRAME}-byte TX slot",
        trailer.len()
    );
    // Lossless: at most `MAX_TX_FRAME`, asserted above.
    let len = total as u16;
    tx.push_with(|frame| {
        frame.connection_id = conn_id;
        frame.len = len;
        frame.data[..written].copy_from_slice(&encode_buf[..written]);
        frame.data[written..total].copy_from_slice(trailer);
    });
}

/// Queue an answer the poll thread decided on — a halt rejection, a
/// filtered or refused request, a shed one — for the poll thread, every
/// frame of it in one TX frame, encoded by [`encode_early_reply`]. No
/// durability gate: nothing was published. A connection that has gone
/// away is skipped.
fn push_early_reply<R: Copy, Q: Copy>(
    reply: crate::halt::EarlyReply<R>,
    encoder: &dyn melin_app::encoder::ResponseEncoder<Report = R, Query = Q>,
    connections: &mut FxHashMap<u64, ConnectionHeartbeat>,
    tx_producers: &mut [spsc::Producer<TxFrame>],
    early_buf: &mut EarlyReplyBuf,
    now: Instant,
) {
    let conn_id = reply.connection_id;
    let Some(conn_state) = connections.get_mut(&conn_id) else {
        tracing::debug!(
            connection_id = conn_id,
            "early reply dropped: connection not registered"
        );
        return;
    };
    let tid = (conn_id >> 56) as usize % tx_producers.len();
    let written = encode_early_reply(&reply, encoder, early_buf);
    push_frame(
        Some(Ok(written)),
        conn_id,
        &mut tx_producers[tid],
        early_buf,
        &[],
    );
    tx_producers[tid].flush();
    conn_state.last_send = now;
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
///
/// A new connection's heartbeat clock starts at `clock()`, read at most once
/// per call and only when a `Connected` is drained: this runs every loop
/// iteration, and almost every drain is empty.
fn process_control_events(
    control_rx: &mpsc::Receiver<ControlEvent>,
    connections: &mut FxHashMap<u64, ConnectionHeartbeat>,
    active_connections: &AtomicU64,
    clock: impl Fn() -> Instant,
) {
    let mut now = None;
    while let Ok(event) = control_rx.try_recv() {
        match event {
            ControlEvent::Connected { connection_id } => {
                let last_send = *now.get_or_insert_with(&clock);
                connections.insert(connection_id, ConnectionHeartbeat { last_send });
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
        let mut connections = FxHashMap::default();
        let now = Instant::now();

        // Poll thread: auth succeeds → increment.
        counter.fetch_add(1, Ordering::Relaxed);
        tx.send(ControlEvent::Connected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, || now);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(connections.len(), 1);

        // Poll thread: connection closes → send Disconnected (no decrement).
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, || now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(connections.len(), 0);
    }

    /// Disconnected for an unknown connection (e.g., pre-auth drop or
    /// duplicate event) must not decrement the counter.
    #[test]
    fn disconnect_unknown_connection_no_decrement() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = FxHashMap::default();
        let now = Instant::now();

        tx.send(ControlEvent::Disconnected { connection_id: 999 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, || now);
        // Counter must stay at 0 — not wrap to u64::MAX.
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    /// Multiple connections with interleaved connect/disconnect.
    #[test]
    fn active_connections_multiple_lifecycle() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = FxHashMap::default();
        let now = Instant::now();

        // Three connections authenticate.
        for id in 1..=3 {
            counter.fetch_add(1, Ordering::Relaxed);
            tx.send(ControlEvent::Connected { connection_id: id })
                .unwrap();
        }
        process_control_events(&rx, &mut connections, &counter, || now);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert_eq!(connections.len(), 3);

        // Connection 2 disconnects.
        tx.send(ControlEvent::Disconnected { connection_id: 2 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, || now);
        assert_eq!(counter.load(Ordering::Relaxed), 2);
        assert_eq!(connections.len(), 2);

        // Remaining two disconnect.
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        tx.send(ControlEvent::Disconnected { connection_id: 3 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, || now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(connections.len(), 0);
    }

    /// Duplicate Disconnected for the same connection must only decrement
    /// once (the second remove returns None).
    #[test]
    fn duplicate_disconnect_single_decrement() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = FxHashMap::default();
        let now = Instant::now();

        counter.fetch_add(1, Ordering::Relaxed);
        tx.send(ControlEvent::Connected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, || now);

        // Two Disconnected events for the same connection.
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, || now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    /// A new connection's heartbeat clock starts when its `Connected` is
    /// drained, not at some earlier stored instant; and a drain with no
    /// `Connected` reads no clock at all, since it runs every iteration.
    #[test]
    fn connected_stamps_the_clock_read_lazily_at_drain() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = FxHashMap::default();
        let reads = std::cell::Cell::new(0u32);
        let at_drain = Instant::now() + std::time::Duration::from_secs(60);
        let clock = || {
            reads.set(reads.get() + 1);
            at_drain
        };

        // Empty drain, and a drain of a Disconnected only: no clock read.
        process_control_events(&rx, &mut connections, &counter, clock);
        tx.send(ControlEvent::Disconnected { connection_id: 9 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, clock);
        assert_eq!(reads.get(), 0);

        // Two Connected in one drain: one read, both stamped with it.
        tx.send(ControlEvent::Connected { connection_id: 1 })
            .unwrap();
        tx.send(ControlEvent::Connected { connection_id: 2 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, clock);
        assert_eq!(reads.get(), 1);
        assert_eq!(connections[&1].last_send, at_drain);
        assert_eq!(connections[&2].last_send, at_drain);
    }

    /// A journaled policy change addressed to a registered client's own
    /// connection queues no frame for it, not even a batch end: the
    /// replies on either side of it are the only frames on the TX ring.
    /// The kernel stage's counterpart is `response.rs`'s
    /// `a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry`.
    #[test]
    fn a_change_slot_queues_no_frame_for_its_connection() {
        use counter_server::{Counter, CounterQuery, CounterReport, ResponseEncoder};
        use melin_journal::AckPolicyChange;
        use melin_pipeline::padding::CachePadded;
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_transport_core::WireSeq;
        use melin_transport_core::lineage::forced_policy_cell;
        use melin_transport_core::pipeline::OutputPayload;

        use crate::ack_policy::AckPolicy;
        use crate::durability_gate::GateStart;
        use crate::halt::early_reply_channel;

        /// Bounds the wait for a frame that must arrive.
        const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

        fn reply(wire_seq: u64) -> OutputSlot<CounterReport, CounterQuery> {
            OutputSlot {
                connection_id: 1,
                input_seq: wire_seq - 1,
                wire_seq,
                payload: OutputPayload::Report(CounterReport::Ack {
                    new_value: wire_seq,
                }),
                is_last_in_request: true,
                ..Default::default()
            }
        }

        let (mut producer, mut consumers) =
            DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(64)
                .add_consumer()
                .build(WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().expect("one consumer was requested");
        let (control_tx, control_rx) = mpsc::channel();
        control_tx
            .send(ControlEvent::Connected { connection_id: 1 })
            .expect("channel open");
        let (tx, mut rx) = spsc::channel::<TxFrame>(8, WaitStrategy::SpinThenYield);
        let early_replies = early_reply_channel(Arc::new(CachePadded::new(AtomicU64::new(0)))).1;

        producer.publish(reply(1));
        producer.publish(OutputSlot {
            // Addressed to the client's own connection, which the matching
            // stage never does: the change still queues nothing for it.
            connection_id: 1,
            input_seq: 1,
            wire_seq: 2,
            payload: OutputPayload::AckPolicyChange(AckPolicyChange {
                policy: AckPolicy::Disk.to_byte(),
                unwitnessed: false,
            }),
            is_last_in_request: true,
            ..Default::default()
        });
        producer.publish(reply(3));

        let shutdown = AtomicBool::new(false);
        std::thread::scope(|scope| {
            let stage = scope.spawn(|| {
                run::<Counter>(
                    consumer,
                    control_rx,
                    // Everything published is on disk: nothing is held.
                    DurableWireSeqCursor::detached(WireSeq::new(3)),
                    GatePolicy {
                        start: GateStart::settled(AckPolicy::Disk),
                        forced: forced_policy_cell().1,
                    },
                    None,
                    None,
                    &shutdown,
                    None,
                    Arc::new(AtomicU64::new(0)),
                    vec![tx],
                    Arc::new(StageUtilization::default()),
                    WaitStrategy::SpinThenYield,
                    Arc::new(ResponseEncoder),
                    early_replies,
                    None,
                )
            });

            // The replies on either side of the change: once the second
            // is queued, the stage has taken the change.
            let mut frames = Vec::new();
            let deadline = Instant::now() + SETTLE_TIMEOUT;
            while frames.len() < 2 && Instant::now() < deadline {
                match rx.try_consume() {
                    Some((_, frame)) => frames.push(frame.connection_id),
                    None => std::thread::yield_now(),
                }
            }
            shutdown.store(true, Ordering::Relaxed);
            stage.join().expect("stage panicked");
            // Anything the stage queued before it stopped.
            while let Some((_, frame)) = rx.try_consume() {
                frames.push(frame.connection_id);
            }

            assert_eq!(
                frames,
                [1, 1],
                "one frame per reply, and none for the policy change"
            );
        });
    }

    /// A halted node past the grace period answers a held write once its
    /// own journal holds it, ending in `BatchEndDegraded`, and the
    /// refusals around it keep their place and end in a plain `BatchEnd`.
    /// With the switch off the held write, and the refusal behind it, wait.
    /// The kernel stage's counterpart is `response.rs`'s
    /// `tests::degraded_release`.
    mod degraded_release {
        use super::*;

        use counter_server::{
            Counter, CounterQuery, CounterReport, KIND_RESP_ACK, KIND_RESP_REJECTED,
            ResponseEncoder,
        };
        use melin_pipeline::padding::CachePadded;
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_transport_core::WireSeq;
        use melin_transport_core::lineage::forced_policy_cell;
        use melin_transport_core::pipeline::{OutputPayload, RequestId};
        use melin_transport_core::replica_presence::ReplicaPresence;
        use melin_wire_protocol::control::RefusalKind;
        use melin_wire_protocol::control_codec::{TAG_BATCH_END, TAG_BATCH_END_DEGRADED};

        use crate::ack_policy::AckPolicy;
        use crate::durability_gate::GateStart;
        use crate::halt::{DegradedRelease, EarlyKind, early_reply_channel};
        use crate::replication::ReplicationMetrics;
        use crate::response::tests::refusal_order::{Seen, early, early_id, frames_of};

        /// The held write's ID.
        const HELD: RequestId = RequestId(7);

        /// A halt refusal, the kind these cases hold the write between.
        const REJECTED: EarlyKind<CounterReport> = EarlyKind::Rejected(CounterReport::Rejected);

        /// Every wire frame a TX frame carries — a response, then its
        /// terminator when it closes a request — in order.
        fn wire_frames(bytes: &[u8], seen: &mut Vec<Seen>) {
            let mut rest = bytes;
            while !rest.is_empty() {
                let (len, tail) = rest.split_first_chunk::<4>().expect("a length prefix");
                let (payload, tail) = tail.split_at(u32::from_le_bytes(*len) as usize);
                seen.push(Seen::of(payload));
                rest = tail;
            }
        }

        /// Run a stage under `disk+ram` on a primary whose one replica is
        /// gone, with degraded release `on` (no grace) or off, holding one
        /// write the primary's journal already holds between two early
        /// replies of `kind`. Returns what the poll thread was handed
        /// before `quiet` passed with nothing more.
        fn run_halted(on: bool, quiet: Duration, kind: EarlyKind<CounterReport>) -> Vec<Seen> {
            let (mut producer, mut consumers) =
                DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(64)
                    .add_consumer()
                    .build(WaitStrategy::SpinThenYield);
            let consumer = consumers.pop().expect("one consumer was requested");
            let (control_tx, control_rx) = mpsc::channel();
            control_tx
                .send(ControlEvent::Connected { connection_id: 1 })
                .expect("channel open");
            let (tx, mut rx) = spsc::channel::<TxFrame>(8, WaitStrategy::SpinThenYield);
            let matching_progress = Arc::new(CachePadded::new(AtomicU64::new(0)));
            let (mut early_tx, early_rx) = early_reply_channel(Arc::clone(&matching_progress));

            early_tx
                .try_send(early(1, 0, kind))
                .expect("queue has room");
            early_tx
                .try_send(early(1, 1, kind))
                .expect("queue has room");
            early_tx.flush();
            producer.publish(OutputSlot {
                connection_id: 1,
                request_id: HELD,
                input_seq: 0,
                wire_seq: 1,
                payload: OutputPayload::Report(CounterReport::Ack { new_value: 7 }),
                is_last_in_request: true,
                ..Default::default()
            });
            matching_progress.get().store(1, Ordering::Release);

            let shutdown = AtomicBool::new(false);
            let mut seen = Vec::new();
            std::thread::scope(|scope| {
                let stage = scope.spawn(|| {
                    run::<Counter>(
                        consumer,
                        control_rx,
                        // The primary's journal holds the write.
                        DurableWireSeqCursor::detached(WireSeq::new(1)),
                        GatePolicy {
                            start: GateStart::settled(AckPolicy::DiskAndRam),
                            forced: forced_policy_cell().1,
                        },
                        Some(Arc::new(ReplicationMetrics::default())),
                        Some([
                            Arc::new(AtomicBool::new(false)),
                            Arc::new(AtomicBool::new(false)),
                        ]),
                        &shutdown,
                        None,
                        Arc::new(AtomicU64::new(0)),
                        vec![tx],
                        Arc::new(StageUtilization::default()),
                        WaitStrategy::SpinThenYield,
                        Arc::new(ResponseEncoder),
                        early_rx,
                        // No replica ever streaming: halted from the start.
                        on.then(|| DegradedRelease {
                            presence: Arc::new(ReplicaPresence::new()),
                            grace: Duration::ZERO,
                        }),
                    )
                });

                let mut last_frame = Instant::now();
                while last_frame.elapsed() < quiet {
                    match rx.try_consume() {
                        Some((_, frame)) => {
                            assert_eq!(frame.connection_id, 1);
                            wire_frames(frame.as_bytes(), &mut seen);
                            last_frame = Instant::now();
                        }
                        None => std::thread::yield_now(),
                    }
                }
                shutdown.store(true, Ordering::Relaxed);
                stage.join().expect("stage panicked");
                // Anything the stage queued before it stopped.
                while let Some((_, frame)) = rx.try_consume() {
                    wire_frames(frame.as_bytes(), &mut seen);
                }
            });
            seen
        }

        #[test]
        fn a_held_write_is_answered_degraded_and_refusals_end_in_a_plain_batch_end() {
            let seen = run_halted(true, Duration::from_millis(500), REJECTED);
            assert_eq!(
                seen,
                [
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(0)),
                    Seen::App(KIND_RESP_ACK),
                    Seen::Final(TAG_BATCH_END_DEGRADED, HELD),
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(1)),
                ],
                "in request order, only the held write marked"
            );
        }

        #[test]
        fn with_the_switch_off_the_held_write_and_the_refusal_behind_it_wait() {
            let seen = run_halted(false, Duration::from_millis(300), REJECTED);
            assert_eq!(
                seen,
                [
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(0))
                ],
                "only the refusal ahead of the held write goes out"
            );
        }

        /// Every kind of early reply goes to the poll thread as its own
        /// frames, naming its request, in its place around the held write:
        /// a filtered request's empty batch, a decoder's refusal, a busy.
        #[test]
        fn every_kind_of_early_reply_keeps_its_place_and_names_its_request() {
            for kind in [
                EarlyKind::Filtered,
                EarlyKind::Refused {
                    kind: RefusalKind::PermissionDenied,
                    reason: "auditor role may not increment",
                },
                EarlyKind::Busy,
            ] {
                let seen = run_halted(true, Duration::from_millis(500), kind);
                let expected: Vec<Seen> = frames_of(kind, early_id(0))
                    .into_iter()
                    .chain([
                        Seen::App(KIND_RESP_ACK),
                        Seen::Final(TAG_BATCH_END_DEGRADED, HELD),
                    ])
                    .chain(frames_of(kind, early_id(1)))
                    .collect();
                assert_eq!(seen, expected, "{kind:?}");
            }
        }
    }

    mod push_frame {
        use super::*;

        const TRAILER: &[u8] = b"END";

        /// Drive `push_frame` and return every frame the poll thread would
        /// see, as `(connection_id, bytes)`.
        fn pushed(
            payload: Option<Result<usize, &'static str>>,
            trailer: &[u8],
        ) -> Vec<(u64, Vec<u8>)> {
            let (mut tx, mut rx) = spsc::channel::<TxFrame>(8, WaitStrategy::SpinThenYield);
            let encode_buf = b"response-body";
            super::super::push_frame(payload, 42, &mut tx, encode_buf, trailer);
            tx.flush();

            let mut out = Vec::new();
            while let Some((_, frame)) = rx.try_consume() {
                out.push((frame.connection_id, frame.as_bytes().to_vec()));
            }
            out
        }

        #[test]
        fn payload_and_trailer_ship_as_one_frame() {
            let frames = pushed(Some(Ok(8)), TRAILER);

            assert_eq!(frames.len(), 1, "two SPSC slots where one suffices");
            assert_eq!(frames[0].0, 42);
            assert_eq!(frames[0].1, b"responseEND".to_vec());
        }

        #[test]
        fn payload_without_trailer_ships_alone() {
            let frames = pushed(Some(Ok(8)), &[]);

            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].1, b"response".to_vec());
        }

        #[test]
        fn trailer_without_payload_ships_alone() {
            // A `BatchEnd` slot: no body of its own, but it closes the
            // request.
            let frames = pushed(None, TRAILER);

            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].1, TRAILER.to_vec());
        }

        #[test]
        fn nothing_to_send_pushes_no_frame() {
            assert!(pushed(None, &[]).is_empty());
        }

        #[test]
        fn encode_failure_still_terminates_the_request() {
            // The response is lost, but dropping the terminator too would
            // leave the client waiting on a batch that never ends.
            let frames = pushed(Some(Err("encode error")), TRAILER);

            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].1, TRAILER.to_vec());
        }

        #[test]
        fn encode_failure_without_trailer_pushes_no_frame() {
            assert!(pushed(Some(Err("encode error")), &[]).is_empty());
        }

        #[test]
        fn the_encoded_terminator_fits_its_reserved_room() {
            let id = melin_transport_core::pipeline::RequestId(u64::MAX);
            for terminator in [
                TransportResponse::BatchEnd { id },
                TransportResponse::BatchEndDegraded { id },
            ] {
                let mut buf = [0u8; BATCH_END_FRAME_LEN];
                let written = control_codec::encode_transport_response(&terminator, &mut buf)
                    .expect("a terminator encodes into its reserved room");
                assert_eq!(written, BATCH_END_FRAME_LEN);
            }
            assert_eq!(MAX_TX_FRAME, MAX_APP_FRAME + BATCH_END_FRAME_LEN);
        }

        /// The widest response with a full terminator behind it fills a TX
        /// frame exactly, and goes out whole: the overrun a terminator
        /// grown by its ID would cause in a slot sized for the old one.
        #[test]
        fn the_widest_response_and_its_terminator_fit_one_frame() {
            let response = [0xAB; MAX_APP_FRAME];
            let mut trailer = [0u8; BATCH_END_FRAME_LEN];
            let written = control_codec::encode_transport_response(
                &TransportResponse::BatchEnd {
                    id: melin_transport_core::pipeline::RequestId(9),
                },
                &mut trailer,
            )
            .expect("a terminator encodes");
            assert_eq!(written, BATCH_END_FRAME_LEN);
            let (mut tx, mut rx) = spsc::channel::<TxFrame>(8, WaitStrategy::SpinThenYield);
            super::super::push_frame(Some(Ok(MAX_APP_FRAME)), 42, &mut tx, &response, &trailer);
            tx.flush();
            let (_, frame) = rx.try_consume().expect("one frame");
            assert_eq!(frame.len as usize, MAX_TX_FRAME);
            assert_eq!(frame.as_bytes(), [&response[..], &trailer].concat());
            assert!(rx.try_consume().is_none());
        }

        /// A frame and trailer past a TX slot are a bug caught by name, in
        /// release builds too, not a slice-index panic or a torn frame.
        #[test]
        #[should_panic(expected = "overrun")]
        fn a_frame_past_a_tx_slot_panics_by_name() {
            let response = [0xAB; MAX_APP_FRAME];
            let trailer = [0u8; BATCH_END_FRAME_LEN + 1];
            let (mut tx, _rx) = spsc::channel::<TxFrame>(8, WaitStrategy::SpinThenYield);
            super::super::push_frame(Some(Ok(MAX_APP_FRAME)), 42, &mut tx, &response, &trailer);
        }
    }
}
