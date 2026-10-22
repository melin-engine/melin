//! The response stage's loop, shared by every transport: read the output
//! ring, hold each slot at the durability gate, hand its reply to the
//! transport, release early replies between two events' replies, commit
//! the batch.
//!
//! What differs by transport is only where the bytes go, behind
//! [`Egress`]: the kernel TCP stage's io_uring sends
//! (`response::UringEgress`), the DPDK stage's TX rings
//! (`dpdk_response::DpdkEgress`). Everything the
//! stage's correctness depends on, the order of its steps, is here, once:
//!
//! - the control channel is drained after the output ring is read, never
//!   before, so every slot's and early reply's `Connected` is seen;
//! - early replies go out only between two events' replies;
//! - per slot: `gate.admit` first, then `boundary.handled`, then the send;
//!   a slot's reply is sent only once the admit has released it;
//! - [`GateWait::before_wait`] runs at most once per admit, before its
//!   first wait, and `while_waiting` sends early replies only for a slot
//!   that is the first of its event;
//! - the batch's early-reply snapshot is released at each event's first
//!   slot *before* that slot's `gate.admit`, which is why a refusal never
//!   waits behind a held write;
//! - the batch's clock is read once per batch (`batch_now`), while early
//!   replies read it lazily, only when there is one to send;
//! - every exit (the top's shutdown check, the admit's `Shutdown`, a send
//!   stopped while it waited for room) leaves the loop to one epilogue,
//!   without committing and without reading the ring again;
//! - the consumer commits only after every slot of the batch cleared the
//!   gate, which is what the event publisher, chained behind it, relies on;
//! - after the commit, [`Egress::after_commit`], then `gate.after_batch`;
//!   `gate.idle_recheck` on the idle path.
//!
//! The trait is generic, never `dyn`: [`run`] is monomorphized per
//! transport, so every hook compiles to the code it replaces. The tests at
//! the bottom pin those rules against a recording egress, independent of
//! either transport.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use tracing::debug;

use melin_app::encoder::ResponseEncoder;
use melin_pipeline::ring;
use melin_transport_core::pipeline::{OutputPayload, OutputSlot, RequestId};
#[cfg(feature = "latency-trace")]
use melin_transport_core::trace;
use melin_transport_core::trace::MonoTraceInstant;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;

use crate::batch_boundary::{AtBoundary, BatchBoundary};
use crate::durability_gate::{
    Admission, Backing, BatchEnds, DurabilityGate, GateInputs, GatePolicy, GateWait,
};
use crate::early_reply::{EarlyReplyQueue, Registered};
use crate::idle_housekeeping::IdleHousekeeping;
#[cfg(feature = "tick-to-trade")]
use crate::response::GateCrossTracker;
#[cfg(feature = "pipeline-stats")]
use crate::response::print_utilization;
use crate::response_frame::{
    EarlyReplyBuf, EncodeBuf, MAX_APP_FRAME, MAX_EARLY_REPLY, encode_slot_payload,
};

/// Maximum number of output slots consumed per batch. `usize` because it
/// bounds a slice read (`read_contiguous`).
pub(crate) const MAX_BATCH: usize = 1024;

/// What a stage is called in its gate's log lines and its trace stages,
/// which differ by transport: the shared loop logs what each stage logged
/// before it was shared. `&'static str` because the gate and the stats
/// registry both keep the name for the process's lifetime.
pub(crate) struct StageNames {
    /// The durability gate's label in its log lines.
    pub(crate) gate: &'static str,
    /// The dispatch trace stage: consume → the stage's last step.
    /// Registered in traced builds only, as every trace stage is.
    #[cfg_attr(not(feature = "latency-trace"), allow(dead_code))]
    pub(crate) dispatch: &'static str,
    /// The server end-to-end trace stage: reader recv → the stage's
    /// hand-off, which the egress records.
    #[cfg_attr(not(feature = "latency-trace"), allow(dead_code))]
    pub(crate) server_e2e: &'static str,
}

/// Where a response stage's frames go: the one part of the stage that
/// differs by transport. Generic, never `dyn`: the loop is monomorphized
/// per transport, so every hook inlines as the code it replaces.
pub(crate) trait Egress {
    /// The control channel's event: a connection registered or gone.
    type Control;

    /// The stage's name in the gate's log lines and the trace stages.
    const NAMES: StageNames;

    /// Drain the control channel: register each new connection, forget
    /// each closed one. Called only through [`Registered`], after its
    /// snapshot.
    fn drain_control(&mut self, rx: &mpsc::Receiver<Self::Control>);

    /// One slot's reply to its connection, if this egress holds it: look
    /// it up, turn the reply into frames ([`SlotReply::into_frames`],
    /// which encodes the payload and builds the trailer), append or push
    /// them under the encode sample, stamp its own `last_send`.
    /// [`SlotOutcome::Unknown`] makes the loop log the miss; nothing is
    /// encoded for it and no trailer built. Called only for
    /// `Admission::Reply`: a change slot (`Admission::GateOnly`) has no
    /// destination and is not offered to the egress, so it is never
    /// logged as a miss.
    ///
    /// An egress whose send can wait for room reads `shutdown` while it
    /// waits, and only then, and returns `SlotOutcome::Stopped` (compiled
    /// only where such an egress is, so not linked here) if the
    /// flag ended the wait; one whose send never waits ignores it. A send
    /// with room goes through whatever the flag says: the loop finishes
    /// the batch in hand unless something has to wait.
    fn send_slot<R: Copy, Q: Copy>(
        &mut self,
        connection_id: u64,
        reply: SlotReply<'_, R, Q>,
        now: Instant,
        shutdown: &AtomicBool,
    ) -> SlotOutcome;

    /// After every slot the loop finishes, sent or not (a reply, a miss, a
    /// change slot); not after an exit.
    fn after_slot(&mut self);

    /// Send every early reply `registered` holds, between two events'
    /// replies (`at`), and return how many. `now` is read once, and only
    /// if there is one.
    ///
    /// The drain always runs to the end of the snapshot. An egress whose
    /// send can wait for room reads `shutdown` while it waits: once the
    /// flag is set, a reply that would have to wait is dropped, as a
    /// shutdown drops every unsent reply, and one with room still goes.
    /// The loop then leaves at its next shutdown check (the top of the
    /// loop on the idle path, the gate's wait inside it).
    fn release_early_replies<R: Copy, Q: Copy>(
        &mut self,
        registered: &mut Registered<'_, R>,
        at: &AtBoundary<'_>,
        encoder: &dyn ResponseEncoder<Report = R, Query = Q>,
        early_buf: &mut EarlyReplyBuf,
        now: impl Fn() -> Instant,
        shutdown: &AtomicBool,
    ) -> usize;

    /// Before the gate's first wait on a slot (see
    /// [`GateWait::before_wait`]).
    fn before_wait(&mut self);

    /// After a release during a gate wait, called only if it sent any.
    fn after_wait_release(&mut self);

    /// After a release on the idle path, at a boundary, whether it sent
    /// any or not.
    fn after_idle_release(&mut self);

    /// Every idle iteration, after any release: the kernel's steady-state
    /// flush.
    fn on_idle(&mut self);

    /// Send `frame`, the pre-encoded heartbeat, to every connection that
    /// has had nothing sent for `interval`. The idle path's heartbeat
    /// scan, on its housekeeping clock (`now`).
    fn send_heartbeats(&mut self, frame: &[u8], now: Instant, interval: Duration);

    /// After the consumer committed a batch, before `gate.after_batch`.
    fn after_commit(&mut self);

    /// Hand the recorders this egress owns, the encode sample's among
    /// them, to the stats registry; the loop flushes its own. Traced
    /// builds only, as the housekeeping's stats flush is.
    #[cfg(feature = "latency-trace")]
    fn flush_idle_stats(&mut self);

    /// The epilogue, once, on the way out of the loop, whichever exit
    /// took it there. The kernel egress decides from its own fence state.
    fn on_shutdown(&mut self);
}

/// What [`Egress::send_slot`] found, not whether bytes left: the loop
/// counts degraded acks and logs misses from it.
#[must_use]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SlotOutcome {
    /// The egress holds the connection, and the reply was turned into
    /// frames. The bytes may still have been dropped: an append that
    /// overflows the kernel's send buffer cap queues the connection for
    /// removal and is still `Known`, since the reply was answered as far
    /// as this node is concerned.
    Known,
    /// No such connection: nothing was encoded or built, and the loop
    /// logs the miss.
    Unknown,
    /// The send had to wait for room and was stopped by shutdown while it
    /// waited: nothing of the reply was handed on. The loop leaves to its
    /// epilogue, without the slot's `after_slot`, without committing and
    /// without reading the ring again. A way for an egress to report a
    /// stop, not an obligation to check: one whose send never waits never
    /// returns it. Only the DPDK egress's push waits, so the variant, and
    /// the loop's arm for it, exist only under its feature (and for the
    /// tests' recording egress).
    #[cfg(any(feature = "dpdk", test))]
    Stopped,
}

/// What [`Egress::send_slot`] hands over: what encoding the payload needs
/// (the payload, the encoder, `&mut` the encode buffer), what building the
/// trailer needs (`&mut` the [`BatchEnds`], the slot's backing and request
/// ID, `is_last_in_request`), and what the end-to-end sample needs
/// (`recv_ts`). No reference to the gate, and fields private to this
/// module: an egress gets frames out of it only through
/// [`Self::into_frames`].
pub(crate) struct SlotReply<'a, R: Copy, Q: Copy> {
    payload: &'a OutputPayload<R, Q>,
    encoder: &'a dyn ResponseEncoder<Report = R, Query = Q>,
    encode_buf: &'a mut EncodeBuf,
    batch_ends: &'a mut BatchEnds,
    backing: Backing,
    request_id: RequestId,
    is_last_in_request: bool,
    recv_ts: MonoTraceInstant,
}

impl<'a, R: Copy, Q: Copy> SlotReply<'a, R, Q> {
    /// `slot`'s reply, backed as the gate released it.
    pub(crate) fn new(
        slot: &'a OutputSlot<R, Q>,
        backing: Backing,
        encoder: &'a dyn ResponseEncoder<Report = R, Query = Q>,
        encode_buf: &'a mut EncodeBuf,
        batch_ends: &'a mut BatchEnds,
    ) -> Self {
        Self {
            payload: &slot.payload,
            encoder,
            encode_buf,
            batch_ends,
            backing,
            request_id: slot.request_id,
            is_last_in_request: slot.is_last_in_request,
            recv_ts: slot.recv_ts,
        }
    }

    /// Encode the payload into the buffer and build the trailer from
    /// [`BatchEnds`], once: consuming, so a reply becomes frames at most
    /// once, and only where an egress calls it, after its lookup. The
    /// buffer and `BatchEnds` are disjoint borrows, so both leave with
    /// lifetime `'a` side by side.
    #[inline]
    pub(crate) fn into_frames(self) -> SlotFrames<'a> {
        // Frame 1: the application payload, if any. A `BatchEnd` payload
        // carries no body: the terminator below handles it, via
        // `is_last_in_request`.
        let encode_buf: &'a mut EncodeBuf = self.encode_buf;
        let payload = encode_slot_payload(self.payload, self.encoder, encode_buf);
        let encode_buf: &'a EncodeBuf = encode_buf;
        // Frame 2: the pre-encoded terminator, `BatchEnd` or
        // `BatchEndDegraded` as the slot is backed, with the request's ID
        // written in. Only a request's last slot carries one.
        let trailer: &'a [u8] = if self.is_last_in_request {
            self.batch_ends.terminator(self.backing, self.request_id)
        } else {
            &[]
        };
        SlotFrames {
            payload,
            encode_buf,
            trailer,
            is_last_in_request: self.is_last_in_request,
            recv_ts: self.recv_ts,
        }
    }
}

/// A slot's reply as frames: the encode result and the buffer it wrote,
/// the trailer, and the end-to-end sample's inputs.
pub(crate) struct SlotFrames<'a> {
    /// What encoding the payload gave: `None` for a payload with no body,
    /// the frame's length in [`Self::encode_buf`], or the encoder's error,
    /// which the egress logs, still sending the trailer.
    pub(crate) payload: Option<Result<usize, &'static str>>,
    /// The encode buffer; its first bytes are the payload's frame.
    pub(crate) encode_buf: &'a [u8],
    /// The request's terminator, or empty if the slot does not end its
    /// request.
    pub(crate) trailer: &'a [u8],
    /// Whether the slot ends its request: only such a slot closes an
    /// end-to-end sample, which traced builds alone record.
    #[cfg_attr(not(feature = "latency-trace"), allow(dead_code))]
    pub(crate) is_last_in_request: bool,
    /// When the reader received the request (a unit in untraced builds).
    #[cfg_attr(not(feature = "latency-trace"), allow(dead_code))]
    pub(crate) recv_ts: MonoTraceInstant,
}

/// The stage's [`GateWait`] hook, built for one slot's admit: hand the
/// egress its turn before the wait, and, if the held slot is the first of
/// its event (`at_boundary`), send early replies during it, registering
/// new connections first ([`Registered::reregister`]). A slot held in the
/// middle of an event sends nothing: an early reply there would split that
/// event's reply.
pub(crate) struct GateWaitHook<'a, 'q, E: Egress, R: Copy, Q: Copy> {
    pub(crate) egress: &'a mut E,
    pub(crate) at_boundary: Option<AtBoundary<'a>>,
    pub(crate) registered: &'a mut Registered<'q, R>,
    pub(crate) control_rx: &'a mpsc::Receiver<E::Control>,
    pub(crate) encoder: &'a dyn ResponseEncoder<Report = R, Query = Q>,
    pub(crate) early_buf: &'a mut EarlyReplyBuf,
    /// The stage's shutdown flag, for an early reply's send that has to
    /// wait for room (see [`Egress::release_early_replies`]). The gate's
    /// own wait reads it on its next turn.
    pub(crate) shutdown: &'a AtomicBool,
}

impl<E: Egress, R: Copy, Q: Copy> GateWait for GateWaitHook<'_, '_, E, R, Q> {
    #[inline]
    fn before_wait(&mut self) {
        self.egress.before_wait();
    }

    /// Early replies decided while the gate holds a write (a refusal, a
    /// busy) go out now rather than behind it. Nothing to do, and no
    /// clock read, on a turn with none; one cursor load and a control
    /// channel poll is the cost of a turn.
    fn while_waiting(&mut self) {
        let Some(at) = &self.at_boundary else {
            return;
        };
        let egress = &mut *self.egress;
        let control_rx = self.control_rx;
        self.registered
            .reregister(|| egress.drain_control(control_rx));
        if egress.release_early_replies(
            self.registered,
            at,
            self.encoder,
            self.early_buf,
            Instant::now,
            self.shutdown,
        ) > 0
        {
            egress.after_wait_release();
        }
    }
}

/// The recorders the loop owns. The egress owns the rest
/// ([`EgressRecorders`]); see [`register_recorders`].
pub(crate) struct LoopRecorders {
    #[cfg(feature = "latency-trace")]
    spsc: trace::StageRecorder,
    #[cfg(feature = "latency-trace")]
    dispatch: trace::StageRecorder,
    // Tick-to-trade breakdown: per-slot wait observed for each durability
    // path, recorded only when the gate actually held the slot up
    // (cache-hit paths skip, so the metric is not inflated with crossings
    // that happened before the stage noticed).
    #[cfg(feature = "tick-to-trade")]
    journal_wait: trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")]
    replica_wait: trace::StageRecorder,
}

/// The recorders every egress owns, because it records them around its
/// own steps: the server end-to-end sample, closed when the egress hands
/// the bytes on, and the encode sample, around its append or push.
pub(crate) struct EgressRecorders {
    #[cfg(feature = "latency-trace")]
    pub(crate) server_e2e: trace::StageRecorder,
    /// Wall time around the egress's append or push, not the encode,
    /// which comes before it. Gated on `tick-to-trade`, not
    /// `latency-trace`, because the breakdown's stages roughly double the
    /// hot-path mutex traffic of the lighter mode.
    #[cfg(feature = "tick-to-trade")]
    pub(crate) encode: trace::StageRecorder,
}

/// Register a stage's histograms with the global registry (see
/// `melin_transport_core::trace`), in the order the stats report lists
/// them: the SPSC wakeup, dispatch and server end-to-end samples, then the
/// tick-to-trade breakdown's journal-wait, replica-wait and encode. A
/// transport's own recorders are registered after these. The breakdown
/// feeds the bench's tick-to-trade decomposition; the others are kept
/// alongside as overall sanity checks.
pub(crate) fn register_recorders(
    #[cfg_attr(not(feature = "latency-trace"), allow(unused_variables))] names: &StageNames,
) -> (LoopRecorders, EgressRecorders) {
    #[cfg(feature = "latency-trace")]
    let spsc = trace::register_stage("response: SPSC wakeup (matching publish → response consume)");
    #[cfg(feature = "latency-trace")]
    let dispatch = trace::register_stage(names.dispatch);
    #[cfg(feature = "latency-trace")]
    let server_e2e = trace::register_stage(names.server_e2e);
    #[cfg(feature = "tick-to-trade")]
    let journal_wait =
        trace::register_stage("response: journal-wait (match_complete → journal cursor crossed)");
    #[cfg(feature = "tick-to-trade")]
    let replica_wait = trace::register_stage(
        "response: replica-wait (match_complete → replication cursor crossed)",
    );
    #[cfg(feature = "tick-to-trade")]
    let encode = trace::register_stage("response: encode (per-kind wire encoding)");
    (
        LoopRecorders {
            #[cfg(feature = "latency-trace")]
            spsc,
            #[cfg(feature = "latency-trace")]
            dispatch,
            #[cfg(feature = "tick-to-trade")]
            journal_wait,
            #[cfg(feature = "tick-to-trade")]
            replica_wait,
        },
        EgressRecorders {
            #[cfg(feature = "latency-trace")]
            server_e2e,
            #[cfg(feature = "tick-to-trade")]
            encode,
        },
    )
}

/// What the loop runs on, besides its egress: the ring it reads, the
/// channels it drains, and the gate's inputs.
pub(crate) struct LoopInputs<R: Copy, Q: Copy, C> {
    pub(crate) consumer: ring::Consumer<OutputSlot<R, Q>>,
    pub(crate) control_rx: mpsc::Receiver<C>,
    /// The durability gate's inputs; the loop waits with the same
    /// strategy (`gate.wait`) and counts into the same gauges
    /// (`gate.utilization`).
    pub(crate) gate: GateInputs,
    /// The ack policy the gate starts under and the sources of its
    /// changes.
    pub(crate) policy: GatePolicy,
    pub(crate) heartbeat_interval: Option<Duration>,
    pub(crate) encoder: Arc<dyn ResponseEncoder<Report = R, Query = Q>>,
    pub(crate) early_replies: EarlyReplyQueue<R>,
    /// Test seam: see `response::Response::pause_after_control_drain`.
    #[cfg(test)]
    pub(crate) pause_after_control_drain: Option<Box<dyn FnMut() + Send>>,
}

/// Run the response stage's loop over `egress`. Blocks the calling thread
/// until shutdown.
///
/// Consumes from the output ring, holds each slot until the ack policy in
/// force confirms its event (see `response::evaluate_durability`), and
/// hands its reply to `egress`.
pub(crate) fn run<E: Egress, R: Copy, Q: Copy>(
    mut egress: E,
    inputs: LoopInputs<R, Q, E::Control>,
    recorders: LoopRecorders,
    shutdown: &AtomicBool,
) {
    let LoopInputs {
        mut consumer,
        control_rx,
        gate: gate_inputs,
        policy,
        heartbeat_interval,
        encoder,
        mut early_replies,
        #[cfg(test)]
        mut pause_after_control_drain,
    } = inputs;
    let LoopRecorders {
        #[cfg(feature = "latency-trace")]
            spsc: mut spsc_rec,
        #[cfg(feature = "latency-trace")]
            dispatch: mut dispatch_rec,
        #[cfg(feature = "tick-to-trade")]
            journal_wait: mut journal_wait_rec,
        #[cfg(feature = "tick-to-trade")]
            replica_wait: mut replica_wait_rec,
    } = recorders;
    let utilization = Arc::clone(&gate_inputs.utilization);
    let wait = gate_inputs.wait;

    // The ack policy in force and the durability gate. The gate owns the
    // policy for the stage's lifetime and switches it at the slots of
    // journaled changes. Built here, before the first batch, so a
    // degraded startup shows on `/healthz` and in the journal
    // immediately. See `DurabilityGate`.
    let mut gate = DurabilityGate::new(gate_inputs, policy, E::NAMES.gate);

    let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
    // An early reply's frames, encoded together: off the hot path, but on
    // the stack all the same.
    let mut early_buf: EarlyReplyBuf = [0u8; MAX_EARLY_REPLY];

    // Pre-encode the heartbeat frame once: the full wire frame (length
    // prefix and tag), fixed-size, no heap allocation.
    let mut heartbeat_buf = [0u8; 8];
    let heartbeat_len =
        control_codec::encode_transport_response(&TransportResponse::Heartbeat, &mut heartbeat_buf)
            .expect("heartbeat encodes");
    let heartbeat_frame = &heartbeat_buf[..heartbeat_len];

    // Pre-encode the two request terminators once. Unlike the heartbeat
    // these are on the hot path (every request ends with one) and only
    // their request ID changes, so the stage writes the ID into one rather
    // than encode a frame per request. An early reply's batch end is the
    // plain one: it is a definite answer, with nothing on any disk to
    // qualify. A busy, as every other early reply, is encoded per reply
    // with its request's ID (`encode_early_reply`).
    let mut batch_ends = BatchEnds::new();

    // Paces the idle path's clock read, and on it the heartbeat scan, the
    // policy re-check and the recorders' flush. See `IdleHousekeeping`.
    let mut housekeeping = IdleHousekeeping::new();

    let mut waiter = wait.waiter();

    let mut busy_count: u64 = 0;
    let mut idle_count: u64 = 0;

    // Whether the stage stands between two events' replies, the only
    // place early replies may go: told about every slot handled.
    let mut boundary = BatchBoundary::new();

    'run: loop {
        // Every exit goes to the epilogue after the loop: this check, the
        // admit's `Shutdown` and a stopped send below. A fenced node takes
        // it too: fencing always co-sets `shutdown`
        // (`FenceState::fence_if_superseded` owns that invariant), so the
        // steady-state loop pays nothing for the fence, and the egress
        // decides on the way out what a fenced stage may still send.
        if shutdown.load(Ordering::Relaxed) {
            break 'run;
        }

        // Borrow output slots from the matching stage in place.
        //
        // `read_contiguous` rather than `consume_batch`: the latter
        // memcpy'd every ready slot into a stack array before the first
        // one was touched, and `OutputSlot` embeds the application's
        // largest query response (often hundreds of bytes), so the head
        // slot of a deep batch paid for the whole copy before it could
        // be encoded. Borrowing costs nothing and the loop below reads
        // each slot exactly once anyway.
        //
        // The progress counter now moves *after* the batch instead of
        // before it (`consume_batch` committed up front), so the
        // matching stage cannot reclaim these slots until the loop —
        // durability gate waits included — is done with them. That is
        // at most `MAX_BATCH` of a 1 M-slot ring, and the ring fills at
        // the same rate either way.
        let slots = consumer.read_contiguous(MAX_BATCH);

        // Take a snapshot of the early replies, then poll the control
        // channel (non-blocking) for connect and disconnect — after the
        // ring is read, never before. A reply always trails its
        // connection's `Connected`: the transport queues that event
        // before it reads any request on the connection, so no request
        // can be read, let alone answered, until the event is in the
        // channel. Every slot in `slots` was published before the read
        // above, and every early reply in the snapshot before the drain,
        // so their `Connected` was queued before that, and this drain sees
        // it. Drained first, the stage could read the ring after a
        // `Connected` it had not seen and drop the reply — which it did,
        // now and then, on a fresh node under load.
        // `a_reply_that_lands_with_its_connection_is_delivered` in the
        // tests holds the stage right after this drain to pin the order.
        let mut registered = early_replies.register(|| egress.drain_control(&control_rx));

        // Test seam: hold the stage right after the drain — see
        // `Response::pause_after_control_drain`.
        #[cfg(test)]
        if let Some(pause) = pause_after_control_drain.as_mut() {
            pause();
        }

        if slots.is_empty() {
            // Between two events, the snapshot's early replies go now. An
            // empty read is not a boundary in itself: the matching stage
            // may have published only part of an event.
            if let Some(at) = boundary.at_boundary() {
                egress.release_early_replies(
                    &mut registered,
                    &at,
                    &*encoder,
                    &mut early_buf,
                    Instant::now,
                    shutdown,
                );
                egress.after_idle_release();
            }

            // The output ring is empty: the egress's steady-state turn.
            egress.on_idle();

            // Everything below is periodic housekeeping on
            // sub-second-to-second cadences, read off one amortized
            // clock read (see `IdleHousekeeping`).
            if let Some(due) = housekeeping.tick(&waiter) {
                let now = due.now;

                // Send heartbeats to idle connections. Only checked
                // during idle periods (ring empty) to avoid overhead on
                // the hot path.
                if let Some(interval) = heartbeat_interval
                    && due.heartbeat_scan
                {
                    egress.send_heartbeats(heartbeat_frame, now, interval);
                }

                // Re-evaluate the ack policy on a slow timer so the
                // `policy_degraded` flag and the periodic warn track the
                // cluster's real state even on an idle / quiet node —
                // the no-batch counterpart of `gate.after_batch()`.
                gate.idle_recheck(now);

                // Hand buffered latency samples to the stats registry
                // while we have nothing better to do. Reuses the
                // housekeeping clock read, so the spin path picks up no
                // extra `clock_gettime`.
                #[cfg(feature = "latency-trace")]
                if due.stats_flush {
                    spsc_rec.flush();
                    dispatch_rec.flush();
                    #[cfg(feature = "tick-to-trade")]
                    {
                        journal_wait_rec.flush();
                        replica_wait_rec.flush();
                    }
                    egress.flush_idle_stats();
                }
            }

            idle_count += 1;
            if idle_count.is_multiple_of(1024) {
                utilization.busy.store(busy_count, Ordering::Relaxed);
                utilization.idle.store(idle_count, Ordering::Relaxed);
            }
            waiter.idle();
            continue;
        }
        waiter.reset();
        busy_count += 1;

        #[cfg(feature = "latency-trace")]
        let consume_ts = trace::mono_trace_ns();

        // Wait for durability confirmation before sending responses.
        //
        // Gate on `wire_seq`, not `input_seq`. `input_seq` is in
        // local-consumer space (the matching cursor on the input ring,
        // starts at 0 in this process) while replica metrics and the
        // primary's `journal_persisted_wire_seq` live in wire-seq space
        // (allocated by the journal stage starting at
        // `starting_sequence`). A `needed` derived from `input_seq` and
        // compared against wire-seq cursors only works when
        // `starting_sequence == 1`; a recovered primary (or any process
        // whose journal already has prior content pushing
        // `starting_sequence` above 1) would silently open the gate
        // ahead of the replica's actual replicated state.
        //
        // Every cursor in the policy view (`journal_persisted_wire_seq`,
        // `metrics.in_memory_sequence`, `metrics.acked_sequence`)
        // carries "highest wire seq known to be in that state on node
        // X". A slot's `wire_seq` is therefore the *exact* wire seq the
        // gate must see — not `+1` — for that slot's event to be
        // considered durable. The legacy `+1` was load-bearing only
        // because `input_seq` was off by `starting_sequence - 1` from
        // wire seq; with the wire-seq stamp it would over-shoot by one
        // event and make the gate stall an extra round-trip per
        // response.
        //
        // The gate is evaluated **per slot**, not once per batch against
        // the batch's maximum `wire_seq`. Batch-max gating made every
        // response in a batch wait for the newest event in it to become
        // durable, so the oldest slot paid the durability latency of an
        // event sequenced up to `MAX_BATCH` positions after it — a
        // head-of-line block bounded only by the batch size, and one
        // that bites hardest exactly when the stage has caught up to the
        // durability frontier under load. Per-slot gating walks that
        // frontier instead: each response is released as soon as its own
        // event is durable.
        //
        // The extra waits are self-limiting. Slots arrive FIFO with
        // monotonic `wire_seq`, and the journal publishes its cursor per
        // fsync batch, so one wait typically advances the cursor past a
        // whole run of following slots and they fall through without
        // spinning. The number of waits tracks real durability
        // boundaries, not the slot count. Correctness does not depend on
        // that ordering — an unsorted batch would simply wait more often.
        //
        // One clock read per batch, for the connections' `last_send`
        // (heartbeat scheduling, on a seconds-scale interval) and the
        // batch's early replies.
        let batch_now = Instant::now();

        for slot in slots {
            #[cfg(feature = "latency-trace")]
            spsc_rec.record_elapsed(slot.match_complete_ts, consume_ts);

            // The first slot of an event: the last one handled ended its
            // event, so the snapshot's early replies go here, ahead of the
            // gate, which holds this event, not them. The iteration's
            // snapshot decides for the whole batch; the gate's hook takes
            // fresh ones if this slot waits.
            let at_boundary = boundary.at_boundary();
            if let Some(at) = &at_boundary {
                egress.release_early_replies(
                    &mut registered,
                    at,
                    &*encoder,
                    &mut early_buf,
                    || batch_now,
                    shutdown,
                );
            }

            // Per-slot journal-wait / replica-wait tracker. See
            // `GateCrossTracker` for the rationale (only records cursors
            // that were actually on the critical path). Both the
            // tracker's replica cursor and the attribution counters
            // below are derived from the policy in force rather than a
            // fixed level, so "which subsystem to optimize" answers for
            // the deployment the operator actually configured. A slot
            // that never waits leaves the tracker unobserved, and an
            // unobserved tracker reports no crossing — so the fast path
            // records nothing rather than a zero-length sample.
            #[cfg(feature = "tick-to-trade")]
            let mut gate_tracker = GateCrossTracker::new(slot.wire_seq);

            // Admit the slot: held until its event is durable under the
            // policy in force, and for a policy change, the switch point.
            // The hook gives the egress its turn if the slot has to wait,
            // and sends early replies during the wait if the slot is the
            // first of its event (`GateWaitHook`). What backs a reply
            // decides its terminator: the policy, unless the gate released
            // it on this node's own fsync while halted.
            //
            // Shutdown while the gate is closed: out of the loop, to the
            // epilogue, which decides what to flush. This slot's reply,
            // which the policy never confirmed, is not sent, and the rest
            // of the batch goes with it, uncommitted. Those clients see a
            // reset, as after a crash, and reconcile on reconnect. A break
            // rather than a trip back to the top: the top would read the
            // same uncommitted batch again, and resend every reply already
            // sent, if the gate ever returned `Shutdown` with the flag
            // clear.
            //
            // A policy change slot is the gate's own: its frames go to no
            // connection, whatever it is addressed to.
            let admission = gate.admit(
                slot,
                shutdown,
                #[cfg(feature = "tick-to-trade")]
                &mut gate_tracker,
                &mut GateWaitHook {
                    egress: &mut egress,
                    at_boundary,
                    registered: &mut registered,
                    control_rx: &control_rx,
                    encoder: &*encoder,
                    early_buf: &mut early_buf,
                    shutdown,
                },
            );
            let reply = match admission {
                Admission::Reply {
                    connection_id,
                    backing,
                } => Some((connection_id, backing)),
                Admission::GateOnly => None,
                Admission::Shutdown => break 'run,
            };
            // Handled from here on, whether or not its frames go
            // anywhere: nothing between here and the next slot sends an
            // early reply.
            boundary.handled(slot.is_last_in_request);

            // Per-slot durability-gate breakdown. Recorded only when the
            // gate actually held us up (the tracker captured a cross).
            // The cross timestamp is this slot's own `wire_seq`, so the
            // sample measures the wait that slot really paid — under
            // batch-max gating it was the batch maximum's crossing,
            // which overstated every slot but the last by up to the
            // batch's matching span.
            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.journal_crossed() {
                journal_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }
            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.replica_crossed() {
                replica_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }

            // Each slot expands to at most two wire frames: the
            // application payload (Report / Query) and an optional
            // trailing `BatchEnd`, naming the slot's request, when
            // `is_last_in_request` is set. The egress encodes them after
            // its own lookup, so a reply to a connection it does not hold
            // is never encoded.
            if let Some((connection_id, backing)) = reply {
                let reply =
                    SlotReply::new(slot, backing, &*encoder, &mut encode_buf, &mut batch_ends);
                match egress.send_slot(connection_id, reply, batch_now, shutdown) {
                    SlotOutcome::Known => {
                        // A degraded terminator counts for
                        // `melin_degraded_acks_total`: once per request,
                        // and only for a connection the egress holds, so
                        // a reply to a connection that is gone (a tick's
                        // among them) never counts.
                        if slot.is_last_in_request {
                            match backing {
                                Backing::Policy => {}
                                Backing::PrimaryOnly => {
                                    utilization.degraded_acks.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                    SlotOutcome::Unknown => {
                        // A reply for a connection this stage does not
                        // hold: it was dropped earlier in this batch, or
                        // went away before its reply was encoded. The
                        // drain above runs after the ring is read, so a
                        // connection that is merely new is never a miss.
                        // Logged so a lost reply leaves a trace.
                        // Connection 0 is a server-originated event — a
                        // seed, a tick — whose reports have no client to
                        // go to, and is skipped without a word.
                        if connection_id != 0 {
                            debug!(
                                connection_id,
                                wire_seq = slot.wire_seq,
                                "reply dropped: connection not registered"
                            );
                        }
                    }
                    // Shutdown while the send waited for room: out of the
                    // loop, as from the admit's `Shutdown` above, and for
                    // the same reasons. This slot's reply was not handed
                    // on, and the rest of the batch goes with it,
                    // uncommitted.
                    #[cfg(any(feature = "dpdk", test))]
                    SlotOutcome::Stopped => break 'run,
                }
            }

            // The egress's turn between slots (the kernel's consumed-path
            // flush), so a batch never holds every reply it sent until it
            // ends.
            egress.after_slot();
        }

        // The borrowed slots are dead here, so the batch can be handed
        // back to the producer. Publishing progress is deliberately the
        // last thing the iteration does with the ring: every slot has
        // cleared its durability gate and been handed to the egress by
        // now.
        //
        // Invariant the event publisher depends on: this consumer's
        // progress never passes a slot that has not cleared the gate.
        // The publisher's consumer is chained behind this one (see
        // `build_pipeline_with_replication`), so this commit is what
        // releases events to it; committing earlier, or on the way out
        // of a gate wait on shutdown (which leaves the loop without
        // committing), would broadcast events the ack policy has not
        // confirmed.
        consumer.commit();

        egress.after_commit();

        // Log degradation transitions / re-emit the reminder — after
        // dispatch, not before it. See `DurabilityGate::after_batch`.
        gate.after_batch();

        #[cfg(feature = "latency-trace")]
        dispatch_rec.record_elapsed(consume_ts, trace::mono_trace_ns());
    }

    // The one epilogue, whichever exit led here.
    egress.on_shutdown();
    utilization.busy.store(busy_count, Ordering::Relaxed);
    utilization.idle.store(idle_count, Ordering::Relaxed);
    // Named "response" on every transport: the print says which pipeline
    // stage it measures, not which transport carries it.
    #[cfg(feature = "pipeline-stats")]
    print_utilization("response", busy_count, idle_count);
}

/// The loop's ordering rules, pinned against a recording egress that logs
/// each hook call, so they are stated once, independent of either
/// transport, and run in every build. The stage is driven one iteration at
/// a time through the drain seam, which holds it right after each control
/// drain, so every log below is deterministic.
#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::AtomicU64;
    use std::sync::{Mutex, MutexGuard};
    use std::thread::{self, JoinHandle};

    use counter_server::{CounterQuery, CounterReport, ResponseEncoder as CounterEncoder};
    use melin_journal::AckPolicyChange;
    use melin_pipeline::padding::Sequence;
    use melin_pipeline::ring::DisruptorBuilder;
    use melin_pipeline::wait::WaitStrategy;
    use melin_transport_core::lineage::forced_policy_cell;
    use melin_transport_core::pipeline::StageUtilization;
    use melin_transport_core::replica_presence::ReplicaPresence;
    use melin_transport_core::{DurableWireSeqCursor, WireSeq};

    use super::*;
    use crate::ack_policy::AckPolicy;
    use crate::durability_gate::GateStart;
    use crate::early_reply::{
        EARLY_REPLY_QUEUE_CAPACITY, EarlyKind, EarlyReplySender, early_reply_channel,
    };
    use crate::halt::DegradedRelease;
    use crate::replication::ReplicationMetrics;
    use crate::response::tests::early_release::{Seam, seam};
    use crate::response::tests::fixtures::early;

    type Slot = OutputSlot<CounterReport, CounterQuery>;

    /// One hook call, with what the loop handed it. `committed` is the
    /// consumer's published progress when the hook ran.
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Call {
        /// The control drain, when it registered any connection.
        Drained(Vec<u64>),
        /// An early-reply release that sent any. An empty one is not
        /// logged: the gate's hook runs one on every turn of a wait.
        Released(usize),
        SendSlot {
            connection_id: u64,
            request: u64,
            known: bool,
            committed: u64,
        },
        AfterSlot {
            committed: u64,
        },
        BeforeWait,
        AfterWaitRelease,
        AfterIdleRelease,
        OnIdle,
        AfterCommit {
            committed: u64,
        },
        OnShutdown,
    }

    /// The connection whose sends never find room: see
    /// [`Recording::send_slot`].
    const FULL: u64 = 99;

    /// An egress that holds no bytes and logs every hook call. A
    /// connection is known once the control channel registered it.
    struct Recording {
        calls: Arc<Mutex<Vec<Call>>>,
        /// `BTreeSet`: a handful of IDs, looked up by value.
        known: BTreeSet<u64>,
        committed: Arc<Sequence>,
        utilization: Arc<StageUtilization>,
        /// The degraded-time counter as `after_commit` saw it, to tell
        /// whether `gate.after_batch` ran before or after it.
        degraded_nanos_at_commit: Arc<AtomicU64>,
    }

    impl Recording {
        fn log(&self, call: Call) {
            lock(&self.calls).push(call);
        }

        fn committed(&self) -> u64 {
            self.committed.get().load(Ordering::Acquire)
        }
    }

    fn lock(calls: &Mutex<Vec<Call>>) -> MutexGuard<'_, Vec<Call>> {
        calls.lock().expect("the log's lock")
    }

    impl Egress for Recording {
        /// A connection registered.
        type Control = u64;

        const NAMES: StageNames = StageNames {
            gate: "recording response",
            dispatch: "test: recording dispatch",
            server_e2e: "test: recording server e2e",
        };

        fn drain_control(&mut self, rx: &mpsc::Receiver<u64>) {
            let drained: Vec<u64> = rx.try_iter().collect();
            if !drained.is_empty() {
                self.known.extend(&drained);
                self.log(Call::Drained(drained));
            }
        }

        /// A send to [`FULL`] waits for room that never comes, until
        /// `shutdown` stops it, as a push on a ring nobody drains does.
        fn send_slot<R: Copy, Q: Copy>(
            &mut self,
            connection_id: u64,
            reply: SlotReply<'_, R, Q>,
            _now: Instant,
            shutdown: &AtomicBool,
        ) -> SlotOutcome {
            let known = self.known.contains(&connection_id);
            self.log(Call::SendSlot {
                connection_id,
                request: reply.request_id.0,
                known,
                committed: self.committed(),
            });
            if known && connection_id == FULL {
                while !shutdown.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(1));
                }
                return SlotOutcome::Stopped;
            }
            if known {
                let _frames = reply.into_frames();
                SlotOutcome::Known
            } else {
                SlotOutcome::Unknown
            }
        }

        fn after_slot(&mut self) {
            self.log(Call::AfterSlot {
                committed: self.committed(),
            });
        }

        fn release_early_replies<R: Copy, Q: Copy>(
            &mut self,
            registered: &mut Registered<'_, R>,
            at: &AtBoundary<'_>,
            _encoder: &dyn ResponseEncoder<Report = R, Query = Q>,
            _early_buf: &mut EarlyReplyBuf,
            _now: impl Fn() -> Instant,
            _shutdown: &AtomicBool,
        ) -> usize {
            let released = registered.drain(at, |_| {});
            if released > 0 {
                self.log(Call::Released(released));
            }
            released
        }

        fn before_wait(&mut self) {
            self.log(Call::BeforeWait);
        }

        fn after_wait_release(&mut self) {
            self.log(Call::AfterWaitRelease);
        }

        fn after_idle_release(&mut self) {
            self.log(Call::AfterIdleRelease);
        }

        fn on_idle(&mut self) {
            self.log(Call::OnIdle);
        }

        fn send_heartbeats(&mut self, _frame: &[u8], _now: Instant, _interval: Duration) {
            unreachable!("the stage under test sends no heartbeats");
        }

        fn after_commit(&mut self) {
            self.degraded_nanos_at_commit.store(
                self.utilization
                    .policy_degraded_nanos
                    .load(Ordering::Relaxed),
                Ordering::Relaxed,
            );
            self.log(Call::AfterCommit {
                committed: self.committed(),
            });
        }

        // Not logged: it runs on the housekeeping timer, not at a point
        // in the loop's order.
        #[cfg(feature = "latency-trace")]
        fn flush_idle_stats(&mut self) {}

        fn on_shutdown(&mut self) {
            self.log(Call::OnShutdown);
        }
    }

    /// The gate the stage runs behind.
    enum GateKind {
        /// `disk` on a standalone node: the journal cursor decides.
        Disk,
        /// `disk+ram` with no replica, and degraded release with no grace:
        /// every reply the journal holds goes out backed by the primary
        /// alone.
        Degraded,
    }

    /// How long a test waits for the stage to reach a hook.
    const TIMEOUT: Duration = Duration::from_secs(10);

    /// How long a test watches for a call that must not come.
    const QUIET: Duration = Duration::from_millis(100);

    /// A running loop over a recording egress, held at its drain seam.
    struct Stage {
        /// Declared first so it drops first: a failing test lets the
        /// stage carry on to its shutdown rather than wait at the seam.
        seam: Option<Seam>,
        producer: ring::Producer<Slot>,
        control: mpsc::Sender<u64>,
        early: EarlyReplySender<CounterReport>,
        journal: DurableWireSeqCursor,
        calls: Arc<Mutex<Vec<Call>>>,
        committed: Arc<Sequence>,
        utilization: Arc<StageUtilization>,
        degraded_nanos_at_commit: Arc<AtomicU64>,
        shutdown: Arc<AtomicBool>,
        handle: Option<JoinHandle<()>>,
    }

    /// Start the loop behind `gate`, the journal at `journal_at`, and
    /// wait for it to park at the seam of its first iteration.
    fn start(gate: GateKind, journal_at: u64) -> Stage {
        let (pause, seam) = seam(usize::MAX);
        let (producer, mut consumers) = DisruptorBuilder::<Slot>::new(64)
            .add_consumer()
            .build(WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().expect("one consumer was requested");
        let committed = consumer.progress_counter();
        let (control, control_rx) = mpsc::channel();
        let (early, early_replies) = early_reply_channel(EARLY_REPLY_QUEUE_CAPACITY);
        let journal = DurableWireSeqCursor::detached(WireSeq::new(journal_at));
        let utilization = Arc::new(StageUtilization::default());
        let (policy, replication_metrics, replica_active, degraded_release) = match gate {
            GateKind::Disk => (AckPolicy::Disk, None, None, None),
            GateKind::Degraded => (
                AckPolicy::DiskAndRam,
                Some(Arc::new(ReplicationMetrics::default())),
                Some([
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                ]),
                Some(DegradedRelease {
                    presence: Arc::new(ReplicaPresence::new()),
                    grace: Duration::ZERO,
                }),
            ),
        };
        let inputs = LoopInputs {
            consumer,
            control_rx,
            gate: GateInputs {
                journal_persisted_wire_seq: journal.clone(),
                replication_metrics,
                replica_active,
                utilization: Arc::clone(&utilization),
                wait: WaitStrategy::SpinThenYield,
                degraded_release,
            },
            policy: GatePolicy {
                start: GateStart::settled(policy),
                forced: forced_policy_cell().1,
            },
            heartbeat_interval: None,
            encoder: Arc::new(CounterEncoder),
            early_replies,
            pause_after_control_drain: Some(pause),
        };
        let calls = Arc::new(Mutex::new(Vec::new()));
        let degraded_nanos_at_commit = Arc::new(AtomicU64::new(0));
        let egress = Recording {
            calls: Arc::clone(&calls),
            known: BTreeSet::new(),
            committed: Arc::clone(&committed),
            utilization: Arc::clone(&utilization),
            degraded_nanos_at_commit: Arc::clone(&degraded_nanos_at_commit),
        };
        let (recorders, _) = register_recorders(&Recording::NAMES);
        let shutdown = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&shutdown);
        let handle = thread::spawn(move || run(egress, inputs, recorders, &flag));
        seam.parked();
        Stage {
            seam: Some(seam),
            producer,
            control,
            early,
            journal,
            calls,
            committed,
            utilization,
            degraded_nanos_at_commit,
            shutdown,
            handle: Some(handle),
        }
    }

    impl Stage {
        fn seam(&self) -> &Seam {
            self.seam.as_ref().expect("the stage is running")
        }

        /// Let the parked stage finish its iteration and park at the
        /// next one's seam; the calls it made meanwhile.
        fn step(&self) -> Vec<Call> {
            self.seam().resume();
            self.parked()
        }

        /// Wait for the stage to park at its seam; the calls it made
        /// since the last look.
        fn parked(&self) -> Vec<Call> {
            self.seam().parked();
            self.take()
        }

        fn take(&self) -> Vec<Call> {
            std::mem::take(&mut *lock(&self.calls))
        }

        /// The calls made so far, left in the log.
        fn so_far(&self) -> Vec<Call> {
            lock(&self.calls).clone()
        }

        /// Wait until the stage has made `call`.
        fn wait_for(&self, call: &Call) {
            let deadline = Instant::now() + TIMEOUT;
            while !lock(&self.calls).contains(call) {
                assert!(Instant::now() < deadline, "never called: {call:?}");
                thread::sleep(Duration::from_millis(1));
            }
        }

        fn committed(&self) -> u64 {
            self.committed.get().load(Ordering::Acquire)
        }

        fn connect(&self, connection_id: u64) {
            self.control
                .send(connection_id)
                .expect("the stage is running");
        }

        /// Queue a busy for `connection_id`, flushed, as a reader does.
        fn answer_early(&mut self, connection_id: u64, n: u64) {
            self.early
                .try_send(early(connection_id, n, EarlyKind::Busy))
                .expect("queue has room");
            self.early.flush();
        }

        /// Publish one slot of request `request` on `connection_id`, its
        /// event at `wire_seq`.
        fn publish(&mut self, connection_id: u64, request: u64, wire_seq: u64, last: bool) {
            self.producer.publish(Slot {
                connection_id,
                request_id: RequestId(request),
                input_seq: wire_seq - 1,
                wire_seq,
                payload: OutputPayload::Report(CounterReport::Ack {
                    new_value: wire_seq,
                }),
                is_last_in_request: last,
                ..Default::default()
            });
        }

        /// Stop the stage and wait for it to return; the calls it made
        /// since the last look.
        fn stop(&mut self) -> Vec<Call> {
            self.shutdown.store(true, Ordering::Relaxed);
            // Disconnected, the seam lets a parked stage carry on.
            self.seam = None;
            self.handle
                .take()
                .expect("stopped once")
                .join()
                .expect("the stage panicked");
            self.take()
        }
    }

    impl Drop for Stage {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Relaxed);
            self.seam = None;
            if let Some(handle) = self.handle.take() {
                // Joined only to surface a panic; while unwinding from a
                // failed assertion, another panic would abort.
                if handle.join().is_err() && !thread::panicking() {
                    panic!("the stage panicked");
                }
            }
        }
    }

    /// The idle path at a boundary, with nothing to release.
    const IDLE: [Call; 2] = [Call::AfterIdleRelease, Call::OnIdle];

    /// The rest of an idle iteration, then the next one's read and drain,
    /// which registered `connections`.
    fn idle_then_drained(connections: &[u64]) -> Vec<Call> {
        let mut calls = IDLE.to_vec();
        if !connections.is_empty() {
            calls.push(Call::Drained(connections.to_vec()));
        }
        calls
    }

    /// The control channel is drained after the ring is read: a reply
    /// published with its connection's `Connected` while the stage is
    /// parked right after a drain finds its connection. Drained before
    /// the read, the stage would read the slot past a drain that had not
    /// seen the `Connected`, and miss it. Every slot finishes with
    /// `after_slot`, a change slot too, which is not offered to the
    /// egress; the commit comes after the last slot, then `after_commit`.
    /// A reply the policy backs counts no degraded ack.
    #[test]
    fn the_drain_follows_the_read_and_every_slot_ends_before_the_commit() {
        let mut stage = start(GateKind::Disk, 10);
        let base = stage.committed();
        stage.connect(1);
        stage.publish(1, 1, 1, true);
        stage.producer.publish(Slot {
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

        assert_eq!(stage.step(), idle_then_drained(&[1]));
        assert_eq!(
            stage.step(),
            [
                Call::SendSlot {
                    connection_id: 1,
                    request: 1,
                    known: true,
                    committed: base,
                },
                Call::AfterSlot { committed: base },
                // The change slot: no send, and still its turn.
                Call::AfterSlot { committed: base },
                Call::AfterCommit {
                    committed: base + 2,
                },
            ]
        );
        assert_eq!(stage.utilization.degraded_acks.load(Ordering::Relaxed), 0);
    }

    /// The batch's early replies go at an event's first slot, before its
    /// admit; the admit's `before_wait` runs once however long it waits;
    /// during the wait, the hook registers new connections and releases
    /// what the reader decided meanwhile, and `after_wait_release`
    /// follows a release that sent any; the slot is sent only once the
    /// gate released it.
    #[test]
    fn early_replies_go_before_the_first_slots_admit_and_during_its_wait() {
        let mut stage = start(GateKind::Disk, 0);
        let base = stage.committed();
        stage.connect(1);
        assert_eq!(stage.step(), idle_then_drained(&[1]));

        stage.answer_early(1, 0);
        stage.publish(1, 1, 1, false);
        stage.publish(1, 1, 1, true);
        // The parked iteration's snapshot predates the early reply.
        assert_eq!(stage.step(), IDLE);

        stage.seam().resume();
        stage.wait_for(&Call::BeforeWait);
        stage.connect(2);
        stage.answer_early(2, 1);
        stage.wait_for(&Call::AfterWaitRelease);
        thread::sleep(QUIET);
        let waiting = [
            Call::Released(1),
            Call::BeforeWait,
            Call::Drained(vec![2]),
            Call::Released(1),
            Call::AfterWaitRelease,
        ];
        assert_eq!(stage.so_far(), waiting, "sent before the gate released it");

        stage.journal.store(WireSeq::new(1));
        let mut expected = waiting.to_vec();
        expected.extend([
            Call::SendSlot {
                connection_id: 1,
                request: 1,
                known: true,
                committed: base,
            },
            Call::AfterSlot { committed: base },
            Call::SendSlot {
                connection_id: 1,
                request: 1,
                known: true,
                committed: base,
            },
            Call::AfterSlot { committed: base },
            Call::AfterCommit {
                committed: base + 2,
            },
        ]);
        assert_eq!(stage.parked(), expected);
    }

    /// Inside an event, nothing releases early replies: not the slot
    /// that continues it, not a wait on that slot, not an idle iteration
    /// between its two reads. They go at the next boundary.
    #[test]
    fn no_early_reply_goes_out_inside_an_event() {
        let mut stage = start(GateKind::Disk, 1);
        let base = stage.committed();
        stage.connect(1);
        assert_eq!(stage.step(), idle_then_drained(&[1]));

        // The event's first slot, alone in its read.
        stage.publish(1, 1, 1, false);
        assert_eq!(stage.step(), IDLE);
        let first = Call::SendSlot {
            connection_id: 1,
            request: 1,
            known: true,
            committed: base,
        };
        assert_eq!(
            stage.step(),
            [
                first.clone(),
                Call::AfterSlot { committed: base },
                Call::AfterCommit {
                    committed: base + 1,
                },
            ]
        );

        // Mid-event: an idle iteration releases nothing.
        stage.answer_early(1, 0);
        assert_eq!(stage.step(), [Call::OnIdle]);
        stage.publish(1, 1, 2, true);
        assert_eq!(stage.step(), [Call::OnIdle]);

        // The event's last slot waits; a reply decided during the wait
        // stays queued.
        stage.seam().resume();
        stage.wait_for(&Call::BeforeWait);
        stage.answer_early(1, 1);
        thread::sleep(QUIET);
        assert_eq!(stage.so_far(), [Call::BeforeWait], "released mid-event");

        stage.journal.store(WireSeq::new(2));
        assert_eq!(
            stage.parked(),
            [
                Call::BeforeWait,
                Call::SendSlot {
                    connection_id: 1,
                    request: 1,
                    known: true,
                    committed: base + 1,
                },
                Call::AfterSlot {
                    committed: base + 1,
                },
                Call::AfterCommit {
                    committed: base + 2,
                },
            ]
        );
        // The boundary: both go, then the idle flush.
        assert_eq!(
            stage.step(),
            [Call::Released(2), Call::AfterIdleRelease, Call::OnIdle]
        );
    }

    /// A degraded ack is counted once per request whose last slot the
    /// egress holds (`Known`) and the primary alone backs: not for a
    /// slot that does not end its request, not for a miss (`Unknown`),
    /// connection 0's included. And `after_commit` runs before
    /// `gate.after_batch`, which charges the degraded time: the counter
    /// moved after `after_commit` saw it.
    #[test]
    fn degraded_acks_count_known_last_slots_and_after_commit_precedes_after_batch() {
        let mut stage = start(GateKind::Degraded, 10);
        let base = stage.committed();
        stage.connect(1);
        assert_eq!(stage.step(), idle_then_drained(&[1]));

        stage.publish(1, 1, 1, true);
        stage.publish(1, 2, 2, false);
        stage.publish(1, 2, 2, true);
        stage.publish(9, 3, 3, true);
        stage.publish(0, 4, 4, true);
        assert_eq!(stage.step(), IDLE);

        // Every slot waits, the policy never being met, until the gate
        // releases it on the journal alone.
        let mut expected: Vec<Call> = [
            (1, 1, true),
            (1, 2, true),
            (1, 2, true),
            (9, 3, false),
            (0, 4, false),
        ]
        .into_iter()
        .flat_map(|(connection_id, request, known)| {
            [
                Call::BeforeWait,
                Call::SendSlot {
                    connection_id,
                    request,
                    known,
                    committed: base,
                },
                Call::AfterSlot { committed: base },
            ]
        })
        .collect();
        expected.push(Call::AfterCommit {
            committed: base + 5,
        });
        assert_eq!(stage.step(), expected);
        assert_eq!(stage.utilization.degraded_acks.load(Ordering::Relaxed), 2);
        assert!(
            stage
                .utilization
                .policy_degraded_nanos
                .load(Ordering::Relaxed)
                > stage.degraded_nanos_at_commit.load(Ordering::Relaxed),
            "the batch's degraded time was charged before `after_commit`"
        );
    }

    /// Shutdown during a gate wait leaves the loop: the held slot is not
    /// sent, nor the rest of its batch, nothing is committed, and no hook
    /// runs but the epilogue's.
    #[test]
    fn an_exit_from_a_gate_wait_commits_nothing_and_runs_only_the_epilogue() {
        let mut stage = start(GateKind::Disk, 1);
        let base = stage.committed();
        stage.connect(1);
        assert_eq!(stage.step(), idle_then_drained(&[1]));
        stage.publish(1, 1, 1, true);
        stage.publish(1, 2, 2, true);
        stage.publish(1, 3, 3, true);
        assert_eq!(stage.step(), IDLE);

        stage.seam().resume();
        stage.wait_for(&Call::BeforeWait);
        assert_eq!(
            stage.stop(),
            [
                Call::SendSlot {
                    connection_id: 1,
                    request: 1,
                    known: true,
                    committed: base,
                },
                Call::AfterSlot { committed: base },
                Call::BeforeWait,
                Call::OnShutdown,
            ]
        );
        assert_eq!(stage.committed(), base, "a held batch was committed");
    }

    /// A send stopped by shutdown while it waited for room leaves the
    /// loop: no `after_slot` for it, the rest of its batch not sent,
    /// nothing committed, the ring not read again, and no hook but the
    /// epilogue's.
    #[test]
    fn a_stopped_send_commits_nothing_and_runs_only_the_epilogue() {
        let mut stage = start(GateKind::Disk, 10);
        let base = stage.committed();
        stage.connect(1);
        stage.connect(FULL);
        assert_eq!(stage.step(), idle_then_drained(&[1, FULL]));
        stage.publish(1, 1, 1, true);
        stage.publish(FULL, 2, 2, true);
        stage.publish(1, 3, 3, true);
        assert_eq!(stage.step(), IDLE);

        stage.seam().resume();
        let stopped = Call::SendSlot {
            connection_id: FULL,
            request: 2,
            known: true,
            committed: base,
        };
        stage.wait_for(&stopped);
        assert_eq!(
            stage.stop(),
            [
                Call::SendSlot {
                    connection_id: 1,
                    request: 1,
                    known: true,
                    committed: base,
                },
                Call::AfterSlot { committed: base },
                stopped,
                Call::OnShutdown,
            ]
        );
        assert_eq!(stage.committed(), base, "a stopped batch was committed");
    }

    /// Shutdown seen at the top of the loop: the epilogue, once, and the
    /// stage's utilization stored on the way out.
    #[test]
    fn an_exit_at_the_top_runs_only_the_epilogue() {
        let mut stage = start(GateKind::Disk, 0);
        let calls = stage.stop();
        assert_eq!(
            calls,
            [Call::AfterIdleRelease, Call::OnIdle, Call::OnShutdown]
        );
        assert!(stage.utilization.idle.load(Ordering::Relaxed) > 0);
    }
}
