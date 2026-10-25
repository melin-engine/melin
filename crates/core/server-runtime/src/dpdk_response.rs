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
//!
//! The stage's loop is the one both transports share
//! (`response_loop::run`); what is DPDK's own is where its frames go,
//! [`DpdkEgress`].

use rustc_hash::FxHashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use melin_pipeline::ring;
use melin_pipeline::spsc;
use melin_pipeline::wait::WaitStrategy;

use crate::batch_boundary::AtBoundary;
use crate::durability_gate::{GateInputs, GatePolicy};
use crate::early_reply::{EarlyReplyQueue, Registered};
use crate::idle_housekeeping::heartbeat_due;
use melin_app::Application;
use melin_transport_core::DurableWireSeqCursor;
use melin_transport_core::pipeline::{OutputSlot, StageUtilization};

use melin_wire_protocol::control_codec::BATCH_END_FRAME_LEN;

use crate::response_frame::{EarlyReplyBuf, MAX_APP_FRAME, MAX_EARLY_REPLY, encode_early_reply};

use crate::response_loop::{
    self, Egress, EgressRecorders, LoopInputs, SlotOutcome, SlotReply, StageNames,
};

#[cfg(feature = "latency-trace")]
use melin_transport_core::trace;

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

/// Bit offset of the owning poll thread's index in a connection ID: the
/// thread sits in bits 56..63, the per-thread sequence in bits 0..55, so a
/// reply is routed to its TX ring without a lookup. `u32` because that is
/// what the shift operators take.
const LANE_SHIFT: u32 = 56;

// The thread's index fills the bits above the shift exactly, so the doc
// above is checked rather than asserted.
const _: () = assert!(LANE_SHIFT + u8::BITS == u64::BITS);

/// The ID of the `seq`th connection accepted by poll thread `thread_id`.
/// `thread_id` is a `u8` so that no index can spill past the top byte.
pub(crate) fn lane_connection_id(thread_id: u8, seq: u64) -> u64 {
    u64::from(thread_id) << LANE_SHIFT | seq
}

/// The TX ring, out of `lanes`, owned by the poll thread that accepted
/// `connection_id`.
fn tx_lane(connection_id: u64, lanes: usize) -> usize {
    (connection_id >> LANE_SHIFT) as usize % lanes
}

/// Run the DPDK response stage. Blocks the calling thread until shutdown.
///
/// The shared response loop ([`response_loop::run`]) over this stage's
/// [`DpdkEgress`]: every reply, early reply and heartbeat goes to the TX
/// ring of the poll thread that owns its connection, which alone touches
/// the socket, rather than to a kernel socket.
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
    consumer: ring::Consumer<OutputSlot<A::Report, A::QueryResponse>>,
    control_rx: mpsc::Receiver<ControlEvent>,
    journal_persisted_wire_seq: DurableWireSeqCursor,
    policy: GatePolicy,
    replication_metrics: Option<Arc<crate::replication::ReplicationMetrics>>,
    replica_active: Option<[Arc<AtomicBool>; 2]>,
    shutdown: &AtomicBool,
    heartbeat_interval: Option<Duration>,
    active_connections: Arc<AtomicU64>,
    tx_producers: Vec<spsc::Producer<TxFrame>>,
    utilization: Arc<StageUtilization>,
    wait: WaitStrategy,
    encoder: crate::response::ResponseEncoderArc<A>,
    // The answers the poll thread decided on without publishing the
    // request (halt rejections, filtered, refused and shed requests),
    // each sent at the first point between two events' replies — see
    // `crate::halt`.
    early_replies: EarlyReplyQueue<A::Report>,
    // Release of held replies on this node's own fsync while halted;
    // `None` when off. See `crate::halt::DegradedRelease`.
    degraded_release: Option<crate::halt::DegradedRelease>,
    // Test seam, the kernel stage's `Response::pause_after_control_drain`:
    // called once per iteration right after the control channel has been
    // drained, so a test can hold the stage there, and with it every early
    // reply, as the poll thread's parking tests do. `None` runs the stage
    // as production does; compiled out of non-test builds.
    #[cfg(test)] pause_after_control_drain: Option<Box<dyn FnMut() + Send>>,
) {
    let (loop_recorders, egress_recorders) = response_loop::register_recorders(&DpdkEgress::NAMES);
    let egress = DpdkEgress::new(tx_producers, active_connections, egress_recorders);

    response_loop::run(
        egress,
        LoopInputs {
            consumer,
            control_rx,
            gate: GateInputs {
                journal_persisted_wire_seq,
                replication_metrics,
                replica_active,
                utilization,
                wait,
                degraded_release,
            },
            policy,
            heartbeat_interval,
            encoder,
            early_replies,
            #[cfg(test)]
            pause_after_control_drain,
        },
        loop_recorders,
        shutdown,
    );
}

/// The DPDK stage's connections and the TX rings every frame goes to: the
/// stage's [`Egress`], which the shared loop ([`response_loop::run`])
/// drives. How it differs from the kernel's, on purpose: see the
/// `impl Egress for DpdkEgress` block.
///
/// No `egress` histogram, unlike the kernel's: DPDK egress lives in the
/// poll thread (`dpdk_transport.rs`), which is where the actual TX
/// happens; sampling here would only capture the SPSC publish and mislead
/// the bench's tick-to-trade breakdown.
struct DpdkEgress {
    /// Known connections, for the reply's lookup and heartbeat
    /// scheduling. FxHash over SipHash: keys are server-generated
    /// connection ids, never attacker-chosen, so HashDoS resistance buys
    /// nothing and the default hasher costs more per lookup than the
    /// lookup itself.
    connections: FxHashMap<u64, ConnectionHeartbeat>,
    /// One TX ring per poll thread, indexed by [`tx_lane`]. A `Vec`
    /// because the thread count is set at startup.
    tx_producers: Vec<spsc::Producer<TxFrame>>,
    /// Which lanes' heartbeats are held back by a full ring, for the log.
    heartbeat_stalls: HeartbeatStalls,
    /// The node's connection count. The response stage is the sole owner
    /// of its decrements: the poll thread increments on authentication
    /// and sends `ControlEvent::Disconnected` on close
    /// ([`process_control_events`]).
    active_connections: Arc<AtomicU64>,
    /// The lane the slot in hand pushed to, held from
    /// [`Egress::send_slot`] to [`Egress::after_slot`], which flushes it.
    /// An `Option` that `after_slot` *takes*: it also runs after a miss
    /// and after a change slot, which must neither flush again nor record
    /// the last reply's end-to-end sample a second time.
    pushed: Option<PushedSlot>,
    #[cfg(feature = "latency-trace")]
    server_e2e_rec: trace::StageRecorder,
    /// Wall time around a slot's push (see [`EgressRecorders::encode`]).
    #[cfg(feature = "tick-to-trade")]
    encode_rec: trace::StageRecorder,
}

/// A slot's push, until its lane is flushed.
struct PushedSlot {
    /// The slot's TX ring ([`tx_lane`]). `usize`: it indexes
    /// `tx_producers`.
    lane: usize,
    /// When the reader received the request, if the slot ends it: only
    /// such a slot closes an end-to-end sample, at the flush.
    #[cfg(feature = "latency-trace")]
    e2e_from: Option<trace::MonoTraceInstant>,
}

impl DpdkEgress {
    /// No connections yet, over one TX ring per poll thread.
    fn new(
        tx_producers: Vec<spsc::Producer<TxFrame>>,
        active_connections: Arc<AtomicU64>,
        // Empty in untraced builds.
        #[cfg_attr(not(feature = "latency-trace"), allow(unused_variables))]
        recorders: EgressRecorders,
    ) -> Self {
        Self {
            connections: FxHashMap::with_capacity_and_hasher(256, Default::default()),
            heartbeat_stalls: HeartbeatStalls::new(tx_producers.len()),
            tx_producers,
            active_connections,
            pushed: None,
            #[cfg(feature = "latency-trace")]
            server_e2e_rec: recorders.server_e2e,
            #[cfg(feature = "tick-to-trade")]
            encode_rec: recorders.encode,
        }
    }
}

/// Simpler than the kernel's (`response::UringEgress`), and deliberately
/// so: the poll thread owns every socket, so a frame pushed is a frame
/// handed over, and there is no per-connection send buffer on this side.
/// What follows from that, each a hook below rather than a comment:
///
/// - No flush cadence and no flush before a gate wait: each slot's lane
///   is flushed as soon as the slot is pushed ([`Self::after_slot`]), so
///   [`Self::before_wait`], [`Self::after_wait_release`] and
///   [`Self::on_idle`] are no-ops.
/// - No send buffer to overflow, so nothing to drop after a release or a
///   commit ([`Self::after_idle_release`], [`Self::after_commit`]).
/// - No shutdown flush to suppress when fenced ([`Self::on_shutdown`]):
///   leaving the loop stops releasing reply frames, which is the whole ack
///   gate.
/// - Heartbeats skip the kernel's blocked-peer and send-buffer-cap checks
///   ([`Self::send_heartbeats`]): with no send buffer there is nothing for
///   them to guard; a full TX ring keeps the heartbeat due instead.
impl Egress for DpdkEgress {
    type Control = ControlEvent;

    const NAMES: StageNames = StageNames {
        gate: "dpdk response",
        dispatch: "response: dispatch (consume → SPSC publish)",
        server_e2e: "server e2e (reader recv → response SPSC publish)",
    };

    /// See [`process_control_events`]. The poll thread queues `Connected`
    /// on authentication and parses that connection's requests only
    /// afterwards, which is what the loop's drain-after-read relies on.
    fn drain_control(&mut self, control_rx: &mpsc::Receiver<ControlEvent>) {
        process_control_events(
            control_rx,
            &mut self.connections,
            &self.active_connections,
            Instant::now,
        );
    }

    /// Push the slot's frames, payload and terminator in one TX frame
    /// ([`push_frame`]), onto its connection's lane, and keep the lane for
    /// [`Self::after_slot`] to flush. One lookup, not two: the entry
    /// decides whether the connection is still known, and the same borrow
    /// stamps its heartbeat clock, even when nothing was pushed (an encode
    /// error on a slot that does not end its request).
    ///
    /// [`SlotOutcome::Stopped`] if the push waited for room on a full
    /// lane and `shutdown` ended the wait: nothing was pushed, the clock
    /// is not stamped, and no lane is kept, since the loop leaves without
    /// this slot's `after_slot`.
    #[inline]
    fn send_slot<R: Copy, Q: Copy>(
        &mut self,
        connection_id: u64,
        reply: SlotReply<'_, R, Q>,
        now: Instant,
        shutdown: &AtomicBool,
    ) -> SlotOutcome {
        // The loop pairs every send_slot with an after_slot, which takes
        // the lane: one still held here would go unflushed and lose its
        // e2e sample when overwritten below.
        debug_assert!(
            self.pushed.is_none(),
            "a pushed lane was not released by after_slot"
        );
        let Some(state) = self.connections.get_mut(&connection_id) else {
            return SlotOutcome::Unknown;
        };
        let lane = tx_lane(connection_id, self.tx_producers.len());
        let frames = reply.into_frames();

        #[cfg(feature = "tick-to-trade")]
        let encode_start = trace::mono_trace_ns();
        let pushed = push_frame(
            frames.payload,
            connection_id,
            &mut self.tx_producers[lane],
            frames.encode_buf,
            frames.trailer,
            shutdown,
        );
        if let Err(spsc::Stopped) = pushed {
            return SlotOutcome::Stopped;
        }
        #[cfg(feature = "tick-to-trade")]
        self.encode_rec
            .record_elapsed(encode_start, trace::mono_trace_ns());

        state.last_send = now;
        self.pushed = Some(PushedSlot {
            lane,
            #[cfg(feature = "latency-trace")]
            e2e_from: frames.is_last_in_request.then_some(frames.recv_ts),
        });
        SlotOutcome::Known
    }

    /// Release the slot's frames as a unit, if it pushed to a known
    /// connection: its 1–2 frames share one lane, so they go under one
    /// release. Per slot rather than once per batch, so each request's
    /// reply ships as soon as it is pushed under saturation, while the
    /// poll thread still drains a reply and its terminator in one consume
    /// for low-rate workloads. `flush` is a no-op when nothing was pushed
    /// (an encode error with no terminator), so the call is safe
    /// regardless.
    #[inline]
    fn after_slot(&mut self) {
        let Some(pushed) = self.pushed.take() else {
            return;
        };
        self.tx_producers[pushed.lane].flush();
        // Server end-to-end, closed after the flush: the moment the poll
        // thread can see the reply.
        #[cfg(feature = "latency-trace")]
        if let Some(recv_ts) = pushed.e2e_from {
            self.server_e2e_rec
                .record_elapsed(recv_ts, trace::mono_trace_ns());
        }
    }

    /// Send every early reply `registered` holds, between two events'
    /// replies (`at`), each as its own TX frame ([`push_early_reply`]).
    /// Returns how many; `now` is read once, and only if there is one. No
    /// send buffer to overflow: each reply is handed to the poll thread as
    /// it is encoded. Once `shutdown` is set, a reply whose lane is full
    /// is dropped rather than waited for ([`push_early_reply`]), so the
    /// drain finishes without waiting.
    fn release_early_replies<R: Copy, Q: Copy>(
        &mut self,
        registered: &mut Registered<'_, R>,
        at: &AtBoundary<'_>,
        encoder: &dyn melin_app::encoder::ResponseEncoder<Report = R, Query = Q>,
        early_buf: &mut EarlyReplyBuf,
        now: impl Fn() -> Instant,
        shutdown: &AtomicBool,
    ) -> usize {
        let mut stamp = None;
        registered.drain(at, |reply| {
            let now = *stamp.get_or_insert_with(&now);
            push_early_reply(
                reply,
                encoder,
                &mut self.connections,
                &mut self.tx_producers,
                early_buf,
                now,
                shutdown,
            );
        })
    }

    /// Nothing to flush: every slot's frames went to the poll thread as
    /// soon as they were pushed.
    fn before_wait(&mut self) {}

    /// Nothing to ship: each early reply was flushed as it was pushed.
    fn after_wait_release(&mut self) {}

    /// Nothing to drop: a push cannot overflow.
    fn after_idle_release(&mut self) {}

    /// No steady-state flush: nothing is buffered here.
    fn on_idle(&mut self) {}

    /// Queue a heartbeat for every connection due one
    /// ([`send_due_heartbeats`]), then log the lanes whose stall began,
    /// ended or lasts ([`HeartbeatStalls::finish_scan`]).
    fn send_heartbeats(&mut self, frame: &[u8], now: Instant, interval: Duration) {
        send_due_heartbeats(
            &mut self.connections,
            &mut self.tx_producers,
            frame,
            now,
            interval,
            &mut self.heartbeat_stalls,
        );
        self.heartbeat_stalls
            .finish_scan(now, interval, log_stall_change);
    }

    /// Nothing to drop: a push cannot overflow.
    fn after_commit(&mut self) {}

    #[cfg(feature = "latency-trace")]
    fn flush_idle_stats(&mut self) {
        self.server_e2e_rec.flush();
        #[cfg(feature = "tick-to-trade")]
        self.encode_rec.flush();
    }

    /// Nothing to do on the way out, fenced or not. A superseded
    /// ex-primary leaves the loop through the same exits
    /// (`FenceState::fence_if_superseded` co-sets `shutdown`), and with no
    /// buffered bytes here there is no best-effort flush to suppress:
    /// leaving the loop stops releasing reply frames onto the TX rings,
    /// which is the whole ack gate.
    fn on_shutdown(&mut self) {}
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
/// The one push of a reply or an early reply, and so the one place either
/// waits for room: on a full ring it waits for the poll thread to drain it,
/// unless `shutdown` is set meanwhile, and then returns `Err(Stopped)`
/// having pushed nothing. A wedged poll thread cannot hold the stage past a
/// shutdown. The flag is read only while waiting: a push with room goes
/// through whatever it says.
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
    shutdown: &AtomicBool,
) -> Result<(), spsc::Stopped> {
    let written = match payload {
        Some(Ok(n)) => n,
        Some(Err(reason)) => {
            tracing::error!(connection_id = conn_id, reason, "encode error");
            0
        }
        None => 0,
    };
    if written == 0 && trailer.is_empty() {
        return Ok(());
    }
    let total = written + trailer.len();
    assert!(
        total <= MAX_TX_FRAME,
        "a {written}-byte frame and a {}-byte trailer overrun a {MAX_TX_FRAME}-byte TX slot",
        trailer.len()
    );
    // Lossless: at most `MAX_TX_FRAME`, asserted above.
    let len = total as u16;
    tx.push_with_unless(
        |frame| {
            frame.connection_id = conn_id;
            frame.len = len;
            frame.data[..written].copy_from_slice(&encode_buf[..written]);
            frame.data[written..total].copy_from_slice(trailer);
        },
        shutdown,
    )?;
    Ok(())
}

/// Queue an answer the poll thread decided on — a halt rejection, a
/// filtered or refused request, a shed one — for the poll thread, every
/// frame of it in one TX frame, encoded by [`encode_early_reply`]. No
/// durability gate: nothing was published. A connection that has gone
/// away is skipped, and so is a reply whose push was stopped by
/// `shutdown` while it waited for room ([`push_frame`]): a shutdown drops
/// every unsent reply.
fn push_early_reply<R: Copy, Q: Copy>(
    reply: crate::early_reply::EarlyReply<R>,
    encoder: &dyn melin_app::encoder::ResponseEncoder<Report = R, Query = Q>,
    connections: &mut FxHashMap<u64, ConnectionHeartbeat>,
    tx_producers: &mut [spsc::Producer<TxFrame>],
    early_buf: &mut EarlyReplyBuf,
    now: Instant,
    shutdown: &AtomicBool,
) {
    let conn_id = reply.connection_id;
    let Some(conn_state) = connections.get_mut(&conn_id) else {
        tracing::debug!(
            connection_id = conn_id,
            "early reply dropped: connection not registered"
        );
        return;
    };
    let tid = tx_lane(conn_id, tx_producers.len());
    let written = encode_early_reply(&reply, encoder, early_buf);
    if let Err(spsc::Stopped) = push_frame(
        Some(Ok(written)),
        conn_id,
        &mut tx_producers[tid],
        early_buf,
        &[],
        shutdown,
    ) {
        tracing::debug!(
            connection_id = conn_id,
            "early reply dropped: shutting down with the poll thread's ring full"
        );
        return;
    }
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

/// Queue a heartbeat for every connection that has had nothing sent for
/// `interval`, recording in `stalls` which lanes' rings took them.
///
/// A connection whose TX ring is full (its poll thread fell behind) keeps
/// its entry and its `last_send`, so the next scan retries it. Dropping
/// the entry instead would leave the socket open with every later reply
/// discarded as unregistered, since only the poll thread can close it,
/// and would release its `max_connections` permit while it is still
/// connected.
fn send_due_heartbeats(
    connections: &mut FxHashMap<u64, ConnectionHeartbeat>,
    tx_producers: &mut [spsc::Producer<TxFrame>],
    heartbeat: &[u8],
    now: Instant,
    interval: Duration,
    stalls: &mut HeartbeatStalls,
) {
    for (&conn_id, state) in connections.iter_mut() {
        let tid = tx_lane(conn_id, tx_producers.len());
        if !heartbeat_due(state.last_send, now, interval) {
            stalls.record(tid, Heartbeat::NotDue);
            continue;
        }
        let mut frame = TxFrame {
            connection_id: conn_id,
            // Lossless: a heartbeat is a few bytes.
            len: heartbeat.len() as u16,
            ..Default::default()
        };
        frame.data[..heartbeat.len()].copy_from_slice(heartbeat);
        if tx_producers[tid].try_publish(frame).is_ok() {
            state.last_send = now;
            stalls.record(tid, Heartbeat::Sent);
        } else {
            stalls.record(tid, Heartbeat::Skipped);
        }
    }
}

/// Per TX lane, whether its heartbeats are being skipped on a full ring,
/// so that a stall is logged when it starts, once per heartbeat interval
/// while it lasts, and when it ends, rather than on every scan. The scan
/// runs only while the stage is idle, so a ring found full there is one
/// the lane's poll thread is behind on: every idle client on that thread
/// stops hearing from the node, and nothing else in the log says so.
struct HeartbeatStalls {
    /// Indexed by `tx_lane`. A `Vec` because the lane count is set at
    /// startup; it holds one entry per poll thread, touched once per
    /// connection per heartbeat scan, off the reply path.
    lanes: Vec<LaneHeartbeats>,
}

#[derive(Clone, Copy, Default)]
struct LaneHeartbeats {
    /// The stall in progress, if any.
    stall: Option<Stall>,
    /// This scan's connections on the lane, and of them the heartbeats
    /// queued and skipped. `usize`: bounded by the connection count, like
    /// the map they come from.
    connections: usize,
    sent: usize,
    skipped: usize,
}

#[derive(Clone, Copy)]
struct Stall {
    since: Instant,
    last_report: Instant,
    /// Heartbeats skipped since `since`, retries included.
    skipped: usize,
}

/// What one connection's heartbeat did in a scan.
#[derive(Clone, Copy)]
enum Heartbeat {
    NotDue,
    Sent,
    Skipped,
}

/// A change in a lane's stall, for the log.
#[derive(Debug, PartialEq, Eq)]
enum StallChange {
    /// The scan skipped `skipped` heartbeats on a lane that was not
    /// stalled.
    Started { skipped: usize },
    /// The scan still skipped heartbeats on a lane stalled for
    /// `stalled_for`, a heartbeat interval or more after the last report.
    Continuing {
        skipped: usize,
        stalled_for: Duration,
    },
    /// The scan queued every heartbeat due on a stalled lane.
    Ended {
        skipped: usize,
        stalled_for: Duration,
    },
    /// A stalled lane has no connections left, so nothing shows whether
    /// its ring drained.
    Emptied {
        skipped: usize,
        stalled_for: Duration,
    },
}

impl HeartbeatStalls {
    fn new(lanes: usize) -> Self {
        HeartbeatStalls {
            lanes: vec![LaneHeartbeats::default(); lanes],
        }
    }

    fn record(&mut self, lane: usize, heartbeat: Heartbeat) {
        let lane = &mut self.lanes[lane];
        lane.connections += 1;
        match heartbeat {
            Heartbeat::NotDue => {}
            Heartbeat::Sent => lane.sent += 1,
            Heartbeat::Skipped => lane.skipped += 1,
        }
    }

    /// Close the scan made at `now`: hand `report` each lane whose stall
    /// began, ended, or has gone unreported for `report_every` in it, and
    /// reset the counts for the next. A lane with connections but nothing
    /// due keeps its state, having shown neither.
    fn finish_scan(
        &mut self,
        now: Instant,
        report_every: Duration,
        mut report: impl FnMut(usize, StallChange),
    ) {
        for (index, lane) in self.lanes.iter_mut().enumerate() {
            match lane.stall.as_mut() {
                None if lane.skipped > 0 => {
                    lane.stall = Some(Stall {
                        since: now,
                        last_report: now,
                        skipped: lane.skipped,
                    });
                    report(
                        index,
                        StallChange::Started {
                            skipped: lane.skipped,
                        },
                    );
                }
                None => {}
                Some(stall) => {
                    stall.skipped += lane.skipped;
                    let skipped = stall.skipped;
                    let stalled_for = now.duration_since(stall.since);
                    if lane.connections == 0 {
                        lane.stall = None;
                        report(
                            index,
                            StallChange::Emptied {
                                skipped,
                                stalled_for,
                            },
                        );
                    } else if lane.skipped == 0 && lane.sent > 0 {
                        lane.stall = None;
                        report(
                            index,
                            StallChange::Ended {
                                skipped,
                                stalled_for,
                            },
                        );
                    } else if lane.skipped > 0
                        && now.duration_since(stall.last_report) >= report_every
                    {
                        stall.last_report = now;
                        report(
                            index,
                            StallChange::Continuing {
                                skipped,
                                stalled_for,
                            },
                        );
                    }
                }
            }
            lane.connections = 0;
            lane.sent = 0;
            lane.skipped = 0;
        }
    }
}

/// Log a lane's stall change at `warn`: degraded operation on the node's
/// side, since the poll thread drains its ring whatever its clients do.
/// The lane is the poll thread's `thread_id`, under which it logs.
fn log_stall_change(thread_id: usize, change: StallChange) {
    match change {
        StallChange::Started { skipped } => tracing::warn!(
            thread_id,
            skipped,
            "DPDK: a poll thread's TX ring was full at the heartbeat scan: heartbeats to its \
             idle clients are held back and retried each scan until it catches up"
        ),
        StallChange::Continuing {
            skipped,
            stalled_for,
        } => tracing::warn!(
            thread_id,
            skipped,
            stalled_ms = stalled_for.as_millis() as u64,
            "DPDK: a poll thread's TX ring is still full: heartbeats to its idle clients are \
             still held back"
        ),
        StallChange::Ended {
            skipped,
            stalled_for,
        } => tracing::warn!(
            thread_id,
            skipped,
            stalled_ms = stalled_for.as_millis() as u64,
            "DPDK: a poll thread's TX ring has room again: heartbeats to its idle clients resumed"
        ),
        StallChange::Emptied {
            skipped,
            stalled_for,
        } => tracing::warn!(
            thread_id,
            skipped,
            stalled_ms = stalled_for.as_millis() as u64,
            "DPDK: a poll thread whose TX ring was full has no clients left: heartbeats no \
             longer show whether it caught up"
        ),
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

    /// A connection ID routes back to the poll thread that minted it,
    /// whatever its sequence, so a reply reaches the TX ring of the thread
    /// that owns its socket.
    #[test]
    fn a_connection_id_routes_to_the_lane_that_minted_it() {
        for lanes in [1usize, 2, 4] {
            for thread_id in 0..lanes as u8 {
                for seq in [1, 2, (1 << LANE_SHIFT) - 1] {
                    let id = lane_connection_id(thread_id, seq);
                    assert_eq!(tx_lane(id, lanes), usize::from(thread_id));
                }
            }
        }
    }

    /// How often the stall tests re-report a lasting stall.
    const STALL_REPORT_EVERY: Duration = Duration::from_secs(5);

    /// The stall changes the heartbeat scan made at `now` reports.
    fn finish_scan(stalls: &mut HeartbeatStalls, now: Instant) -> Vec<(usize, StallChange)> {
        let mut changes = Vec::new();
        stalls.finish_scan(now, STALL_REPORT_EVERY, |lane, change| {
            changes.push((lane, change))
        });
        changes
    }

    /// A lane's stall is reported when it starts, once per report interval
    /// while scans keep skipping, and when a scan gets every due heartbeat
    /// through; not on every scan. A scan that still skips some heartbeats
    /// keeps it stalled even if others got through, and a scan with
    /// nothing due on a lane says nothing about it either way. Each lane is
    /// tracked on its own.
    #[test]
    fn a_heartbeat_stall_is_reported_at_its_edges_and_while_it_lasts() {
        let start = Instant::now();
        let at = |secs| start + Duration::from_secs(secs);
        let mut stalls = HeartbeatStalls::new(2);

        stalls.record(1, Heartbeat::Skipped);
        stalls.record(1, Heartbeat::Skipped);
        stalls.record(0, Heartbeat::Sent);
        assert_eq!(
            finish_scan(&mut stalls, at(0)),
            [(1, StallChange::Started { skipped: 2 })]
        );

        // Still skipping some, within the report interval: no report.
        stalls.record(1, Heartbeat::Sent);
        stalls.record(1, Heartbeat::Skipped);
        assert_eq!(finish_scan(&mut stalls, at(1)), []);

        // Nothing due on the stalled lane: it stays stalled.
        stalls.record(1, Heartbeat::NotDue);
        stalls.record(0, Heartbeat::Sent);
        assert_eq!(finish_scan(&mut stalls, at(2)), []);

        // A report interval on, still skipping: reported again, with the
        // total so far, and not again until the next interval.
        stalls.record(1, Heartbeat::Skipped);
        assert_eq!(
            finish_scan(&mut stalls, at(5)),
            [(
                1,
                StallChange::Continuing {
                    skipped: 4,
                    stalled_for: Duration::from_secs(5),
                }
            )]
        );
        stalls.record(1, Heartbeat::Skipped);
        assert_eq!(finish_scan(&mut stalls, at(6)), []);

        stalls.record(1, Heartbeat::Sent);
        assert_eq!(
            finish_scan(&mut stalls, at(7)),
            [(
                1,
                StallChange::Ended {
                    skipped: 5,
                    stalled_for: Duration::from_secs(7),
                }
            )]
        );

        // Healthy again: sending is not news.
        stalls.record(1, Heartbeat::Sent);
        assert_eq!(finish_scan(&mut stalls, at(8)), []);
    }

    /// A stalled lane whose connections have all gone closes its stall
    /// with a report of its own, rather than leaving it open until some
    /// later connection on that lane first idles; a lane with no
    /// connections that was never stalled says nothing.
    #[test]
    fn a_stalled_lane_left_without_connections_closes_its_stall() {
        let start = Instant::now();
        let mut stalls = HeartbeatStalls::new(2);

        stalls.record(0, Heartbeat::Skipped);
        assert_eq!(
            finish_scan(&mut stalls, start),
            [(0, StallChange::Started { skipped: 1 })]
        );

        let later = start + Duration::from_secs(1);
        assert_eq!(
            finish_scan(&mut stalls, later),
            [(
                0,
                StallChange::Emptied {
                    skipped: 1,
                    stalled_for: Duration::from_secs(1),
                }
            )]
        );
        assert_eq!(finish_scan(&mut stalls, later), []);
    }

    /// A heartbeat that finds its TX ring full is retried on the next
    /// scan, and every connection on that ring stays registered: the stage
    /// cannot close the socket, so dropping one would leave the client
    /// connected with every later reply discarded, and a full ring says
    /// nothing about the other connections sharing it. Each connection's
    /// `max_connections` permit is released only by the poll thread's
    /// `Disconnected`.
    #[test]
    fn a_heartbeat_on_a_full_tx_ring_is_retried_and_keeps_the_connections() {
        let counter = AtomicU64::new(0);
        let (control_tx, control_rx) = mpsc::channel();
        let mut connections = FxHashMap::default();
        let interval = std::time::Duration::from_secs(1);
        let start = Instant::now();
        let heartbeat = [0xAB; 5];

        for connection_id in [7, 8] {
            counter.fetch_add(1, Ordering::Relaxed);
            control_tx
                .send(ControlEvent::Connected { connection_id })
                .unwrap();
        }
        process_control_events(&control_rx, &mut connections, &counter, || start);

        let (tx, mut rx) = spsc::channel::<TxFrame>(8, WaitStrategy::SpinThenYield);
        let mut producers = vec![tx];
        let mut filler = 0;
        while producers[0].try_publish(TxFrame::default()).is_ok() {
            filler += 1;
        }

        let mut stalls = HeartbeatStalls::new(producers.len());

        // Due, but the ring is full: both fail, both stay due and registered,
        // and the lane's stall is reported once.
        let due = start + interval;
        send_due_heartbeats(
            &mut connections,
            &mut producers,
            &heartbeat,
            due,
            interval,
            &mut stalls,
        );
        assert_eq!(connections[&7].last_send, start, "still due");
        assert_eq!(connections[&8].last_send, start, "still due");
        assert_eq!(counter.load(Ordering::Relaxed), 2);
        assert_eq!(
            finish_scan(&mut stalls, due),
            [(0, StallChange::Started { skipped: 2 })]
        );

        // The poll thread drains the ring; the next scan sends one heartbeat
        // to each connection, and nothing more, and the stall is over.
        for _ in 0..filler {
            rx.try_consume().expect("filler frame");
        }
        let later = due + std::time::Duration::from_millis(1);
        send_due_heartbeats(
            &mut connections,
            &mut producers,
            &heartbeat,
            later,
            interval,
            &mut stalls,
        );
        assert_eq!(
            finish_scan(&mut stalls, later),
            [(
                0,
                StallChange::Ended {
                    skipped: 2,
                    stalled_for: later - due,
                }
            )]
        );
        let mut sent = Vec::new();
        while let Some((_, frame)) = rx.try_consume() {
            assert_eq!(frame.as_bytes(), heartbeat);
            sent.push(frame.connection_id);
        }
        sent.sort_unstable();
        assert_eq!(sent, [7, 8], "one heartbeat per connection, no duplicates");
        assert_eq!(connections[&7].last_send, later);
        assert_eq!(connections[&8].last_send, later);

        // Sent just now, so not due again until a full interval has passed.
        let next = later + std::time::Duration::from_millis(1);
        send_due_heartbeats(
            &mut connections,
            &mut producers,
            &heartbeat,
            next,
            interval,
            &mut stalls,
        );
        assert!(rx.try_consume().is_none(), "not yet due: nothing queued");
        assert_eq!(finish_scan(&mut stalls, next), []);

        // Only the poll thread's close releases a permit.
        control_tx
            .send(ControlEvent::Disconnected { connection_id: 7 })
            .unwrap();
        process_control_events(&control_rx, &mut connections, &counter, || later);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert!(connections.contains_key(&8));
        control_tx
            .send(ControlEvent::Disconnected { connection_id: 8 })
            .unwrap();
        process_control_events(&control_rx, &mut connections, &counter, || later);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    /// A halted node past the grace period answers a held write once its
    /// own journal holds it, ending in `BatchEndDegraded`, and the
    /// refusals around it, which wait for nothing, end in a plain
    /// `BatchEnd`. With the switch off the held write waits; the refusals
    /// still do not. The kernel stage's counterpart is `response.rs`'s
    /// `tests::degraded_release`.
    mod degraded_release {
        use super::*;

        use counter_server::{
            Counter, CounterQuery, CounterReport, KIND_RESP_ACK, KIND_RESP_REJECTED,
            ResponseEncoder,
        };
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_transport_core::WireSeq;
        use melin_transport_core::lineage::forced_policy_cell;
        use melin_transport_core::pipeline::{OutputPayload, RequestId};
        use melin_transport_core::replica_presence::ReplicaPresence;
        use melin_wire_protocol::control::RefusalKind;
        use melin_wire_protocol::control_codec::{TAG_BATCH_END, TAG_BATCH_END_DEGRADED};

        use crate::ack_policy::AckPolicy;
        use crate::durability_gate::GateStart;
        use crate::early_reply::{EarlyKind, early_reply_channel};
        use crate::halt::DegradedRelease;
        use crate::replication::ReplicationMetrics;
        use crate::response::tests::fixtures::{Seen, early, early_id, frames_of};

        /// The held write's ID.
        const HELD: RequestId = RequestId(7);

        /// A halt refusal, the kind these cases hold the write between.
        const REJECTED: EarlyKind<CounterReport> = EarlyKind::Rejected(CounterReport::Rejected);

        /// Every wire frame a TX frame carries — a response, then its
        /// terminator when it closes a request — in order.
        pub(super) fn wire_frames(bytes: &[u8], seen: &mut Vec<Seen>) {
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
        /// write the primary's journal already holds, with two early
        /// replies of `kind` queued. Returns what the poll thread was
        /// handed before `quiet` passed with nothing more.
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
            let (mut early_tx, early_rx) =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY);

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
                        None,
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
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(1)),
                    Seen::App(KIND_RESP_ACK),
                    Seen::Final(TAG_BATCH_END_DEGRADED, HELD),
                ],
                "the refusals first, waiting for nothing; only the held write marked"
            );
        }

        #[test]
        fn with_the_switch_off_the_held_write_waits_and_the_refusals_do_not() {
            let seen = run_halted(false, Duration::from_millis(300), REJECTED);
            assert_eq!(
                seen,
                [
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(0)),
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(1)),
                ],
                "both refusals go out; the held write does not"
            );
        }

        /// Every kind of early reply goes to the poll thread as its own
        /// frames, naming its request, ahead of the held write: a filtered
        /// request's empty batch, a decoder's refusal, a busy.
        #[test]
        fn every_kind_of_early_reply_goes_ahead_of_the_held_write_and_names_its_request() {
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
                    .chain(frames_of(kind, early_id(1)))
                    .chain([
                        Seen::App(KIND_RESP_ACK),
                        Seen::Final(TAG_BATCH_END_DEGRADED, HELD),
                    ])
                    .collect();
                assert_eq!(seen, expected, "{kind:?}");
            }
        }
    }

    /// The early-release scenarios of `response.rs`'s
    /// `tests::early_release`, run against this stage: early replies go
    /// out between events, ahead of held writes, never inside an event.
    mod early_release {
        use super::*;

        use std::collections::{HashMap, HashSet, VecDeque};
        use std::thread::JoinHandle;

        use counter_server::{Counter, ResponseEncoder};
        use proptest::prelude::*;

        use super::degraded_release::wire_frames;
        use crate::ack_policy::AckPolicy;
        use crate::durability_gate::GateStart;
        use crate::early_reply::EarlyKind;
        use crate::response::tests::early_release::{
            EXIT_TIMEOUT, Rig, StageHarness, StageInputs,
            a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry,
            a_gate_held_inside_an_event_sends_no_early_reply,
            a_refusal_is_not_released_ahead_of_its_connection,
            a_reply_that_lands_with_its_connection_is_delivered,
            a_stopped_stage_leaves_the_gate_and_drops_the_held_reply,
            an_early_reply_waits_out_an_event_split_across_reads,
            an_idle_connection_is_sent_heartbeats, connection_first_seen_inside_a_gate_wait,
            early_replies_overtake_a_held_write, every_kind,
            every_request_is_answered_once_and_whole, rig, script, stopped_within,
        };
        use crate::response::tests::fixtures::{Seen, early_id, frames_of};

        /// The DPDK stage under test, read off its TX ring the way the
        /// poll thread reads it, each frame filed under its connection.
        struct DpdkStage {
            control: mpsc::Sender<ControlEvent>,
            tx_ring: spsc::Consumer<TxFrame>,
            /// Frames read off the TX ring and not yet asked for, per
            /// connection: the ring interleaves connections, a test reads
            /// one at a time.
            pending: HashMap<u64, VecDeque<Seen>>,
            /// Every connection the test registered. A TX frame for any
            /// other is a stray (the kernel stage, with no socket for it,
            /// cannot even send one), so it fails the test however that
            /// test reads. A `HashSet`: a handful of ids, looked up once
            /// per frame, in no order.
            connected: HashSet<u64>,
            shutdown: Arc<AtomicBool>,
            handle: Option<JoinHandle<()>>,
        }

        /// The harness's TX ring, in frames. A test that fills it, with
        /// nothing reading it, takes the count from here
        /// ([`blocked_on_a_full_tx_ring`]).
        const TX_RING_SIZE: usize = 1024;

        fn start_dpdk(inputs: StageInputs) -> DpdkStage {
            let (control, control_rx) = mpsc::channel();
            let (tx, tx_ring) = spsc::channel::<TxFrame>(TX_RING_SIZE, WaitStrategy::SpinThenYield);
            let shutdown = Arc::clone(&inputs.shutdown);
            let StageInputs {
                consumer,
                early_replies,
                journal,
                replication_metrics,
                replica_active,
                utilization,
                forced,
                shutdown: stage_shutdown,
                pause_after_control_drain,
                heartbeat_interval,
            } = inputs;
            let handle = std::thread::spawn(move || {
                run::<Counter>(
                    consumer,
                    control_rx,
                    journal,
                    GatePolicy {
                        start: GateStart::settled(AckPolicy::DiskAndRam),
                        forced,
                    },
                    Some(replication_metrics),
                    Some(replica_active),
                    &stage_shutdown,
                    heartbeat_interval,
                    Arc::new(AtomicU64::new(0)),
                    vec![tx],
                    utilization,
                    WaitStrategy::SpinThenYield,
                    Arc::new(ResponseEncoder),
                    early_replies,
                    None,
                    pause_after_control_drain,
                )
            });
            DpdkStage {
                control,
                tx_ring,
                pending: HashMap::new(),
                connected: HashSet::new(),
                shutdown,
                handle: Some(handle),
            }
        }

        impl StageHarness for DpdkStage {
            fn connect(&mut self, connection_id: u64) {
                self.control
                    .send(ControlEvent::Connected { connection_id })
                    .expect("the stage is running");
                self.connected.insert(connection_id);
            }

            fn next_frame(&mut self, connection_id: u64, within: Duration) -> Option<Seen> {
                let deadline = Instant::now() + within;
                loop {
                    if let Some(frame) = self
                        .pending
                        .get_mut(&connection_id)
                        .and_then(VecDeque::pop_front)
                    {
                        return Some(frame);
                    }
                    match self.tx_ring.try_consume() {
                        Some((_, frame)) => {
                            // The stage never queues a frame with no
                            // bytes, which would carry no wire frame for
                            // a test to see.
                            assert!(
                                !frame.as_bytes().is_empty(),
                                "an empty TX frame for connection {}",
                                frame.connection_id
                            );
                            assert!(
                                self.connected.contains(&frame.connection_id),
                                "a TX frame for connection {}, which the test never registered",
                                frame.connection_id
                            );
                            let mut seen = Vec::new();
                            wire_frames(frame.as_bytes(), &mut seen);
                            self.pending
                                .entry(frame.connection_id)
                                .or_default()
                                .extend(seen);
                        }
                        None if Instant::now() >= deadline => return None,
                        None => std::thread::yield_now(),
                    }
                }
            }

            fn stopped_within(&mut self, within: Duration) -> bool {
                stopped_within(&self.shutdown, &mut self.handle, within)
            }
        }

        impl Drop for DpdkStage {
            fn drop(&mut self) {
                self.shutdown.store(true, Ordering::Relaxed);
                if let Some(handle) = self.handle.take() {
                    // Joined only to surface a panic; while unwinding
                    // from a failed assertion, another panic would abort.
                    if handle.join().is_err() && !std::thread::panicking() {
                        panic!("the stage panicked");
                    }
                }
            }
        }

        #[test]
        fn an_idle_connection_is_sent_heartbeats_dpdk() {
            an_idle_connection_is_sent_heartbeats(start_dpdk);
        }

        #[test]
        fn an_early_reply_waits_out_an_event_split_across_reads_dpdk() {
            an_early_reply_waits_out_an_event_split_across_reads(start_dpdk);
        }

        #[test]
        fn early_replies_overtake_a_held_write_dpdk() {
            for kind in every_kind() {
                early_replies_overtake_a_held_write(start_dpdk, kind);
            }
        }

        #[test]
        fn a_gate_held_inside_an_event_sends_no_early_reply_dpdk() {
            a_gate_held_inside_an_event_sends_no_early_reply(start_dpdk);
        }

        #[test]
        fn connection_first_seen_inside_a_gate_wait_dpdk() {
            connection_first_seen_inside_a_gate_wait(start_dpdk);
        }

        #[test]
        fn a_reply_that_lands_with_its_connection_is_delivered_dpdk() {
            a_reply_that_lands_with_its_connection_is_delivered(start_dpdk);
        }

        #[test]
        fn a_refusal_is_not_released_ahead_of_its_connection_dpdk() {
            a_refusal_is_not_released_ahead_of_its_connection(start_dpdk);
        }

        /// The stopped case only: the fenced one has no DPDK meaning (the
        /// stage has no flush to suppress).
        #[test]
        fn a_stopped_stage_leaves_the_gate_and_drops_the_held_reply_dpdk() {
            a_stopped_stage_leaves_the_gate_and_drops_the_held_reply(start_dpdk);
        }

        #[test]
        fn a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry_dpdk() {
            a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry(start_dpdk);
        }

        /// Busies `0..count` for connection 1, as the poll thread reads
        /// them off its TX ring.
        fn busies(count: usize) -> Vec<Seen> {
            (0..count as u64)
                .flat_map(|n| frames_of(EarlyKind::Busy, early_id(n)))
                .collect()
        }

        /// Start a stage, held at its drain seam on its first two
        /// iterations, with connection 1 registered, `early` busies for it
        /// queued and, if `reply`, a write of request 7 whose event the
        /// journal already holds; and let it go once its second iteration
        /// has read them all. Past that seam nothing reads the shutdown
        /// flag before the push that finds the TX ring full: the gate's
        /// cached position already covers the write, so its admit does not
        /// wait. Nothing ever drains the TX ring.
        fn blocked_on_a_full_tx_ring(early: usize, reply: bool) -> Rig<DpdkStage> {
            let mut rig = rig(start_dpdk, 1, &[1], 2);
            rig.seam().parked();
            for n in 0..early as u64 {
                rig.answer_early(1, n, EarlyKind::Busy);
            }
            if reply {
                rig.publish(1, 7, 1, true);
            }
            rig.seam().resume();
            rig.seam().parked();
            rig.seam().resume();
            rig
        }

        /// A reply whose push finds the poll thread's TX ring full, the
        /// early replies released ahead of it having just filled it, does
        /// not hold the stage past a shutdown: the push gives up once the
        /// flag is set, and the reply is not sent.
        #[test]
        fn a_reply_blocked_on_a_full_tx_ring_does_not_hold_the_stage_past_shutdown() {
            let mut rig = blocked_on_a_full_tx_ring(TX_RING_SIZE, true);

            assert!(
                rig.stage.stopped_within(EXIT_TIMEOUT),
                "the stage stayed blocked on its reply's push"
            );
            let filled = busies(TX_RING_SIZE);
            assert_eq!(rig.frames(1, filled.len()), filled);
            assert_eq!(rig.quiet(1), None, "the stopped reply was pushed");
        }

        /// An early reply whose push finds the TX ring full (one more than
        /// it holds were released together) does not hold the stage past
        /// a shutdown either: it is dropped, and the stage leaves.
        #[test]
        fn an_early_reply_blocked_on_a_full_tx_ring_does_not_hold_the_stage_past_shutdown() {
            let mut rig = blocked_on_a_full_tx_ring(TX_RING_SIZE + 1, false);

            assert!(
                rig.stage.stopped_within(EXIT_TIMEOUT),
                "the stage stayed blocked on an early reply's push"
            );
            let filled = busies(TX_RING_SIZE);
            assert_eq!(rig.frames(1, filled.len()), filled);
            assert_eq!(rig.quiet(1), None, "the stopped early reply was pushed");
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(24))]
            #[test]
            fn every_request_is_answered_once_and_whole_dpdk(script in script()) {
                every_request_is_answered_once_and_whole(start_dpdk, &script)?;
            }
        }

        /// The gate-wait hook called directly, as the io_uring stage's
        /// `gate_wait_hook` tests call theirs: register nothing, then —
        /// as if during a wait — queue connection 1's `Connected` and a
        /// busy for it, and call `while_waiting` once, between events or
        /// not. Returns the connections after the call and every frame
        /// handed to the poll thread.
        fn hook_turn(between_events: bool) -> (FxHashMap<u64, ConnectionHeartbeat>, Vec<Seen>) {
            use crate::batch_boundary::BatchBoundary;
            use crate::durability_gate::GateWait;
            use crate::early_reply::{EarlyKind, early_reply_channel};
            use crate::response::tests::fixtures::early;
            use crate::response_loop::GateWaitHook;

            let (control_tx, control_rx) = mpsc::channel();
            let (mut tx, mut rx) =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY);
            let mut registered = rx.register(|| {});
            let (producer, mut tx_ring) =
                spsc::channel::<TxFrame>(1024, WaitStrategy::SpinThenYield);
            let (_, recorders) = response_loop::register_recorders(&DpdkEgress::NAMES);
            let mut egress =
                DpdkEgress::new(vec![producer], Arc::new(AtomicU64::new(1)), recorders);

            control_tx
                .send(ControlEvent::Connected { connection_id: 1 })
                .expect("the receiver is held");
            tx.try_send(early(1, 0, EarlyKind::Busy))
                .expect("queue has room");
            tx.flush();

            let boundary = BatchBoundary::new();
            let mut early_buf: EarlyReplyBuf = [0u8; MAX_EARLY_REPLY];
            GateWaitHook {
                egress: &mut egress,
                at_boundary: if between_events {
                    Some(boundary.at_boundary().expect("between events"))
                } else {
                    None
                },
                registered: &mut registered,
                control_rx: &control_rx,
                encoder: &ResponseEncoder,
                early_buf: &mut early_buf,
                shutdown: &AtomicBool::new(false),
            }
            .while_waiting();

            let mut seen = Vec::new();
            while let Some((_, frame)) = tx_ring.try_consume() {
                assert_eq!(frame.connection_id, 1);
                wire_frames(frame.as_bytes(), &mut seen);
            }
            (egress.connections, seen)
        }

        #[test]
        fn gate_wait_hook_between_events_registers_and_sends() {
            use crate::early_reply::EarlyKind;
            use crate::response::tests::fixtures::{early_id, frames_of};

            let (connections, seen) = hook_turn(true);

            assert!(connections.contains_key(&1), "not registered");
            assert_eq!(seen, frames_of(EarlyKind::Busy, early_id(0)));
        }

        #[test]
        fn gate_wait_hook_inside_an_event_does_nothing() {
            let (connections, seen) = hook_turn(false);

            assert!(connections.is_empty(), "control drained");
            assert!(seen.is_empty(), "a reply went out: {seen:?}");
        }
    }

    /// The DPDK egress's slot hooks, called directly: a slot's frames are
    /// pushed onto its connection's lane by `send_slot` and released by
    /// the `after_slot` that follows it, and only that one. What the loop
    /// does with the outcome is pinned by the recording egress in
    /// `response_loop`'s tests.
    mod dpdk_egress {
        use super::*;

        use counter_server::{CounterQuery, CounterReport, KIND_RESP_ACK, ResponseEncoder};
        use melin_transport_core::pipeline::{OutputPayload, RequestId};
        use melin_wire_protocol::control_codec::TAG_BATCH_END;

        use super::degraded_release::wire_frames;
        use crate::durability_gate::{Backing, BatchEnds};
        use crate::response::tests::fixtures::Seen;
        use crate::response_frame::EncodeBuf;

        /// The request every slot here belongs to.
        const REQUEST: RequestId = RequestId(7);

        /// An egress over `lanes` TX rings holding `connected`, and the
        /// poll threads' ends of the rings.
        fn egress_holding(
            lanes: usize,
            connected: &[u64],
        ) -> (DpdkEgress, Vec<spsc::Consumer<TxFrame>>) {
            let (producers, consumers) = (0..lanes)
                .map(|_| spsc::channel::<TxFrame>(8, WaitStrategy::SpinThenYield))
                .unzip();
            let (_, recorders) = response_loop::register_recorders(&DpdkEgress::NAMES);
            let mut egress = DpdkEgress::new(
                producers,
                Arc::new(AtomicU64::new(connected.len() as u64)),
                recorders,
            );
            let (control_tx, control_rx) = mpsc::channel();
            for &connection_id in connected {
                control_tx
                    .send(ControlEvent::Connected { connection_id })
                    .expect("the receiver is held");
            }
            egress.drain_control(&control_rx);
            (egress, consumers)
        }

        /// Hand `egress` one slot of `REQUEST` for `connection_id`, with
        /// `payload`, ending the request or not, sent at `now`.
        fn send_slot(
            egress: &mut DpdkEgress,
            connection_id: u64,
            payload: OutputPayload<CounterReport, CounterQuery>,
            is_last_in_request: bool,
            now: Instant,
            encode_buf: &mut EncodeBuf,
        ) -> SlotOutcome {
            send_slot_unless(
                egress,
                connection_id,
                payload,
                is_last_in_request,
                now,
                encode_buf,
                &AtomicBool::new(false),
            )
        }

        /// [`send_slot`], with the stage's shutdown flag at `shutdown`.
        fn send_slot_unless(
            egress: &mut DpdkEgress,
            connection_id: u64,
            payload: OutputPayload<CounterReport, CounterQuery>,
            is_last_in_request: bool,
            now: Instant,
            encode_buf: &mut EncodeBuf,
            shutdown: &AtomicBool,
        ) -> SlotOutcome {
            let slot = OutputSlot {
                connection_id,
                request_id: REQUEST,
                wire_seq: 1,
                payload,
                is_last_in_request,
                ..Default::default()
            };
            let mut batch_ends = BatchEnds::new();
            egress.send_slot(
                connection_id,
                SlotReply::new(
                    &slot,
                    Backing::Policy,
                    &ResponseEncoder,
                    encode_buf,
                    &mut batch_ends,
                ),
                now,
                shutdown,
            )
        }

        /// Fill lane `lane` of `egress` to the brim, published, as a poll
        /// thread that has stopped draining leaves it.
        fn fill_lane(egress: &mut DpdkEgress, lane: usize) {
            while egress.tx_producers[lane]
                .try_publish(TxFrame::default())
                .is_ok()
            {}
        }

        const ACK: OutputPayload<CounterReport, CounterQuery> =
            OutputPayload::Report(CounterReport::Ack { new_value: 7 });

        /// Every frame on `ring` the poll thread can see now.
        fn visible(ring: &mut spsc::Consumer<TxFrame>) -> Vec<(u64, Vec<Seen>)> {
            let mut frames = Vec::new();
            while let Some((_, frame)) = ring.try_consume() {
                let mut seen = Vec::new();
                wire_frames(frame.as_bytes(), &mut seen);
                frames.push((frame.connection_id, seen));
            }
            frames
        }

        /// A slot's reply and its terminator go onto its connection's
        /// lane as one frame, which the poll thread sees only once
        /// `after_slot` flushed it; the connection's heartbeat clock is
        /// stamped with the batch's time. A second `after_slot`, as after
        /// a later miss or change slot, has nothing left to flush.
        #[test]
        fn a_slot_is_pushed_to_its_lane_and_released_by_the_next_after_slot() {
            let connection_id = lane_connection_id(1, 5);
            let (mut egress, mut rings) = egress_holding(2, &[connection_id]);
            let now = Instant::now() + Duration::from_secs(60);

            let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
            let outcome = send_slot(&mut egress, connection_id, ACK, true, now, &mut encode_buf);

            assert_eq!(outcome, SlotOutcome::Known);
            assert_eq!(egress.connections[&connection_id].last_send, now);
            assert!(
                visible(&mut rings[1]).is_empty(),
                "released before after_slot"
            );

            egress.after_slot();
            assert_eq!(
                visible(&mut rings[1]),
                [(
                    connection_id,
                    vec![
                        Seen::App(KIND_RESP_ACK),
                        Seen::Final(TAG_BATCH_END, REQUEST)
                    ]
                )]
            );
            assert!(
                visible(&mut rings[0]).is_empty(),
                "pushed to the wrong lane"
            );
            assert!(egress.pushed.is_none(), "the lane was read, not taken");

            egress.after_slot();
            assert!(egress.pushed.is_none());
            assert!(visible(&mut rings[1]).is_empty());
        }

        /// A slot that leaves nothing to push (a `BatchEnd` payload that
        /// does not end its request) still stamps the connection's
        /// heartbeat clock, and its `after_slot` releases nothing.
        #[test]
        fn a_slot_with_nothing_to_push_still_stamps_its_connection() {
            let (mut egress, mut rings) = egress_holding(1, &[1]);
            let now = Instant::now() + Duration::from_secs(60);

            let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
            let outcome = send_slot(
                &mut egress,
                1,
                OutputPayload::BatchEnd,
                false,
                now,
                &mut encode_buf,
            );
            egress.after_slot();

            assert_eq!(outcome, SlotOutcome::Known);
            assert_eq!(egress.connections[&1].last_send, now);
            assert!(visible(&mut rings[0]).is_empty());
        }

        /// A slot for a connection the egress does not hold is `Unknown`:
        /// nothing is encoded or pushed, and the `after_slot` after it
        /// leaves untouched what an earlier slot left (here, nothing).
        #[test]
        fn a_slot_for_an_unknown_connection_is_unknown_and_pushes_nothing() {
            let (mut egress, mut rings) = egress_holding(1, &[1]);
            let before = egress.connections[&1].last_send;

            let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
            let outcome = send_slot(&mut egress, 9, ACK, true, Instant::now(), &mut encode_buf);

            assert_eq!(outcome, SlotOutcome::Unknown);
            assert!(egress.pushed.is_none());
            assert!(encode_buf.iter().all(|&b| b == 0), "the reply was encoded");
            egress.after_slot();
            assert!(visible(&mut rings[0]).is_empty());
            assert_eq!(egress.connections[&1].last_send, before);
        }

        /// A slot whose lane is full, with shutdown set, is `Stopped`:
        /// nothing pushed, no lane kept for `after_slot`, and the
        /// connection's heartbeat clock left as it was.
        #[test]
        fn a_slot_on_a_full_lane_is_stopped_by_shutdown() {
            let (mut egress, mut rings) = egress_holding(1, &[1]);
            fill_lane(&mut egress, 0);
            let before = egress.connections[&1].last_send;

            let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
            let outcome = send_slot_unless(
                &mut egress,
                1,
                ACK,
                true,
                Instant::now() + Duration::from_secs(60),
                &mut encode_buf,
                &AtomicBool::new(true),
            );

            assert_eq!(outcome, SlotOutcome::Stopped);
            assert!(egress.pushed.is_none(), "a lane was kept");
            assert_eq!(egress.connections[&1].last_send, before);
            let frames = visible(&mut rings[0]);
            assert_eq!(frames.len(), 8, "only the frames that filled the lane");
            assert!(
                frames.iter().all(|(connection_id, _)| *connection_id == 0),
                "the stopped reply was pushed"
            );
        }

        /// Once shutdown is set, an early-reply release still drains the
        /// whole snapshot without waiting: a reply whose lane is full is
        /// dropped, its connection's clock left alone, and one whose lane
        /// has room still goes.
        #[test]
        fn a_stopped_release_drops_what_would_wait_and_sends_what_has_room() {
            use crate::batch_boundary::BatchBoundary;
            use crate::early_reply::{EARLY_REPLY_QUEUE_CAPACITY, EarlyKind, early_reply_channel};
            use crate::response::tests::fixtures::{early, early_id, frames_of};

            let full = lane_connection_id(1, 1);
            let open = lane_connection_id(0, 1);
            let (mut egress, mut rings) = egress_holding(2, &[full, open]);
            fill_lane(&mut egress, 1);
            let before = egress.connections[&full].last_send;

            let (mut tx, mut rx) = early_reply_channel(EARLY_REPLY_QUEUE_CAPACITY);
            for (n, connection_id) in [full, open].into_iter().enumerate() {
                tx.try_send(early(connection_id, n as u64, EarlyKind::Busy))
                    .expect("queue has room");
            }
            tx.flush();
            let boundary = BatchBoundary::new();
            let mut early_buf: EarlyReplyBuf = [0u8; MAX_EARLY_REPLY];
            let now = Instant::now() + Duration::from_secs(60);
            let released = egress.release_early_replies(
                &mut rx.register(|| {}),
                &boundary.at_boundary().expect("between events"),
                &ResponseEncoder,
                &mut early_buf,
                || now,
                &AtomicBool::new(true),
            );

            assert_eq!(released, 2, "the drain stopped short");
            assert_eq!(egress.connections[&full].last_send, before);
            assert_eq!(egress.connections[&open].last_send, now);
            assert!(
                visible(&mut rings[1])
                    .iter()
                    .all(|(connection_id, _)| *connection_id == 0),
                "the reply on the full lane was pushed"
            );
            assert_eq!(
                visible(&mut rings[0]),
                [(open, frames_of(EarlyKind::Busy, early_id(1)))]
            );
        }
    }

    mod push_frame {
        use super::*;

        use melin_wire_protocol::control::TransportResponse;
        use melin_wire_protocol::control_codec;

        const TRAILER: &[u8] = b"END";

        /// Drive `push_frame` and return every frame the poll thread would
        /// see, as `(connection_id, bytes)`.
        fn pushed(
            payload: Option<Result<usize, &'static str>>,
            trailer: &[u8],
        ) -> Vec<(u64, Vec<u8>)> {
            let (mut tx, mut rx) = spsc::channel::<TxFrame>(8, WaitStrategy::SpinThenYield);
            let encode_buf = b"response-body";
            super::super::push_frame(
                payload,
                42,
                &mut tx,
                encode_buf,
                trailer,
                &AtomicBool::new(false),
            )
            .expect("never stopped");
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
            super::super::push_frame(
                Some(Ok(MAX_APP_FRAME)),
                42,
                &mut tx,
                &response,
                &trailer,
                &AtomicBool::new(false),
            )
            .expect("never stopped");
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
            super::super::push_frame(
                Some(Ok(MAX_APP_FRAME)),
                42,
                &mut tx,
                &response,
                &trailer,
                &AtomicBool::new(false),
            )
            .expect("never stopped");
        }

        /// A TX ring of 2, filled and published, with nothing draining it.
        fn full_ring() -> (spsc::Producer<TxFrame>, spsc::Consumer<TxFrame>) {
            let (mut tx, rx) = spsc::channel::<TxFrame>(2, WaitStrategy::SpinThenYield);
            for _ in 0..2 {
                tx.try_publish(TxFrame::default())
                    .expect("the ring has room");
            }
            (tx, rx)
        }

        /// On a full ring with shutdown set, the push is stopped and
        /// pushes nothing: the stage is not held by a poll thread that
        /// has stopped draining.
        #[test]
        fn a_push_on_a_full_ring_is_stopped_by_shutdown() {
            let (mut tx, mut rx) = full_ring();
            let pushed = super::super::push_frame(
                Some(Ok(8)),
                42,
                &mut tx,
                b"response",
                TRAILER,
                &AtomicBool::new(true),
            );
            tx.flush();

            assert_eq!(pushed, Err(spsc::Stopped));
            assert_eq!(rx.try_consume().map(|(seq, _)| seq), Some(0));
            assert_eq!(rx.try_consume().map(|(seq, _)| seq), Some(1));
            assert!(rx.try_consume().is_none(), "a stopped push published");
        }

        /// Nothing to send never touches the ring, so it is not stopped
        /// even on a full one.
        #[test]
        fn nothing_to_send_is_never_stopped() {
            let (mut tx, _rx) = full_ring();
            assert_eq!(
                super::super::push_frame(None, 42, &mut tx, &[], &[], &AtomicBool::new(true)),
                Ok(())
            );
        }
    }
}
