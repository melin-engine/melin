//! Transport-agnostic replication receiver loop.
//!
//! Both the kernel (io_uring) and DPDK receiver paths share identical
//! business logic: parse length-prefixed frames from a receive buffer,
//! decode `InputBatch` frames into pipeline slots, manage dual-track ack
//! flushing, and handle shutdown/promote signals. The only difference is
//! how bytes arrive and how acks are sent.
//!
//! [`ReceiverTransport`] captures that difference as a trait;
//! [`streaming_loop`] is the generic receiver loop that both backends
//! drive.

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{debug, info};

use melin_app::AppEvent;
use melin_transport_core::pipeline::InputSlot;
use melin_transport_core::replication::protocol::{
    Ack, MAX_DATA_FRAME, PrimaryMessage, decode_primary_message, try_decode_input_batch_into,
};

use super::{PendingAckQueue, try_flush_dual_track};

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Backend-agnostic receiver transport.
///
/// Implemented by `UringTransport` (kernel TCP + io_uring) and
/// `DpdkReceiverTransport` (DPDK + smoltcp). The trait is used as a
/// generic bound (monomorphised) so there is zero vtable overhead.
pub(super) trait ReceiverTransport {
    /// Poll for incoming data. Appends received bytes to `recv_buf`.
    ///
    /// Also processes backend-specific completions (e.g. io_uring SEND
    /// CQEs that clear the ack-in-flight flag).
    ///
    /// Returns `true` if any new data arrived, `false` if idle.
    /// Returns `Err` on fatal I/O or connection loss.
    fn poll_recv(&mut self, recv_buf: &mut Vec<u8>) -> io::Result<bool>;

    /// Queue an ack for sending to the primary.
    ///
    /// Returns `true` if the ack was accepted (sent or queued —
    /// implementations may coalesce a queued ack with a newer one,
    /// since cursors are cumulative and the newest pair subsumes
    /// everything before it; an accepted ack's *progress* is always
    /// eventually delivered while the connection lives). Returns
    /// `false` if the ack was not accepted (caller retries next
    /// iteration). Returns `Err` on fatal send error.
    fn send_ack(&mut self, ack: &Ack) -> io::Result<bool>;

    /// Whether any accepted ack has not yet fully reached the wire.
    /// The flush path skips composing new acks while true; the drain
    /// paths poll on it to flush final acks before session exit.
    fn ack_in_flight(&self) -> bool;

    /// Whether the underlying connection is still active.
    fn is_connected(&mut self) -> bool;
}

// ---------------------------------------------------------------------------
// Shared frame-extraction helpers
// ---------------------------------------------------------------------------

pub(super) enum FrameResult {
    /// Complete frame: payload `[start..end)`, total frame `[0..end)`.
    Complete(usize, usize),
    /// Not enough data for a complete frame.
    Incomplete,
    /// Frame exceeds max_size or is malformed.
    Oversized,
}

/// Try to extract one length-prefixed frame from a receive buffer.
pub(super) fn try_extract_frame(buf: &[u8], max_size: usize) -> FrameResult {
    if buf.len() < 4 {
        return FrameResult::Incomplete;
    }
    let len = u32::from_le_bytes(
        buf[0..4]
            .try_into()
            .expect("bounds checked: buf has at least 4 bytes"),
    ) as usize;
    if len == 0 || len > max_size {
        return FrameResult::Oversized;
    }
    if buf.len() < 4 + len {
        return FrameResult::Incomplete;
    }
    FrameResult::Complete(4, 4 + len)
}

/// Remove `consumed` leading bytes from a receive buffer.
pub(super) fn compact_recv_buf(buf: &mut Vec<u8>, consumed: usize) {
    if consumed > 0 {
        buf.copy_within(consumed.., 0);
        buf.truncate(buf.len() - consumed);
    }
}

// ---------------------------------------------------------------------------
// Streaming frame processing
// ---------------------------------------------------------------------------

/// Outcome of [`process_streaming_frames`] for one recv-cycle.
pub(super) struct StreamingFrameOutcome {
    /// Bytes consumed from the recv buffer.
    pub consumed: usize,
    /// Sequence of the last slot pushed (for `pending_acks.push`).
    pub last_target: u64,
    /// Whether any slot was pushed this cycle.
    pub any_published: bool,
    /// Updated `accum_end_sequence` — only names slots that were
    /// committed to the input ring.
    pub accum_end_sequence: u64,
    /// Whether at least one non-empty `InputBatch` arrived.
    pub received_data: bool,
    /// Fatal frame error — caller should break with `SessionExit::Fatal`.
    pub frame_err: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Process every complete frame in `recv_buf`, publishing decoded slots
/// onto `input_producer` under a single `Producer::batch`.
///
/// Uses `try_decode_input_batch_into` to decode into a reusable
/// `slot_buf`, avoiding per-batch heap allocation on the hot path.
///
/// # Sequence contiguity
///
/// The wire stream is the replica's only source of truth for journal
/// sequences — the journal stage stamps `slot.sequence` verbatim, so
/// anything published here lands in the replica's journal at face
/// value. This function is the gate. Cumulative-delivery semantics,
/// anchored at `accum_end_sequence` (the session's resume point, then
/// the last accepted slot):
///
/// - `seq <= accum` — skipped: idempotent re-delivery. The catch-up →
///   live handoff drains ring chunks whole, and a chunk straddling the
///   catch-up end legitimately re-carries covered slots.
/// - `seq == accum + 1` — accepted.
/// - `seq > accum + 1` — fatal. A gap can never be repaired
///   downstream: acking past it overstates durability to the
///   primary's response gate, and the hole surfaces only at lineage
///   audit (the 2026-06-07 bench failure). The contiguous prefix is
///   committed (progress preserved — mirrors the oversize-frame
///   semantics); the session tears down and re-handshakes from its
///   true position.
pub(super) fn process_streaming_frames<E: AppEvent>(
    recv_buf: &[u8],
    input_producer: &mut melin_pipeline::ring::Producer<InputSlot<E>>,
    accum_end_sequence: u64,
    slot_buf: &mut Vec<InputSlot<E>>,
) -> StreamingFrameOutcome {
    let mut consumed = 0;
    let mut last_target = 0u64;
    let mut any_published = false;
    let mut received_data = false;
    let mut frame_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
    let mut batch = input_producer.batch();
    let mut pending_accum = accum_end_sequence;

    loop {
        let remaining = &recv_buf[consumed..];
        match try_extract_frame(remaining, MAX_DATA_FRAME) {
            FrameResult::Complete(payload_start, frame_end) => {
                let payload = &remaining[payload_start..frame_end];
                match try_decode_input_batch_into(payload, slot_buf) {
                    Ok(()) => {
                        if !slot_buf.is_empty() {
                            received_data = true;
                            for slot in slot_buf.drain(..) {
                                let primary_seq = slot.sequence;
                                if primary_seq <= pending_accum {
                                    // Duplicate from handoff chunk overlap —
                                    // already applied; never re-publish (a
                                    // re-applied slot rewinds the journal
                                    // stage's sequence counter).
                                    continue;
                                }
                                if primary_seq != pending_accum + 1 {
                                    frame_err = Some(
                                        format!(
                                            "sequence gap in replication stream: \
                                             expected {}, got {primary_seq}",
                                            pending_accum + 1
                                        )
                                        .into(),
                                    );
                                    break;
                                }
                                last_target = batch.push_with(|s| *s = slot);
                                pending_accum = primary_seq;
                                any_published = true;
                            }
                            if frame_err.is_some() {
                                break;
                            }
                        }
                    }
                    Err(_) => match decode_primary_message(payload) {
                        Ok(PrimaryMessage::Heartbeat { sequence }) => {
                            debug!(sequence, "heartbeat from primary");
                        }
                        Ok(PrimaryMessage::NeedSnapshot) => {
                            frame_err =
                                Some("primary says we need a snapshot transfer mid-stream".into());
                            break;
                        }
                        Ok(PrimaryMessage::HashMismatch) => {
                            frame_err = Some("chain hash mismatch from primary".into());
                            break;
                        }
                        Ok(_) => {
                            debug!("unexpected message during streaming");
                        }
                        Err(e) => {
                            frame_err =
                                Some(format!("failed to decode primary message: {e}").into());
                            break;
                        }
                    },
                }
                consumed += frame_end;
            }
            FrameResult::Oversized => {
                frame_err = Some("oversized frame from primary during streaming".into());
                break;
            }
            FrameResult::Incomplete => break,
        }
    }

    batch.commit();
    StreamingFrameOutcome {
        consumed,
        last_target,
        any_published,
        accum_end_sequence: pending_accum,
        received_data,
        frame_err,
    }
}

/// Outcome of [`process_drain_frames`] for one drain recv-cycle.
pub(super) struct DrainFrameOutcome {
    pub consumed: usize,
    pub last_target: u64,
    pub any_published: bool,
    pub accum_end_sequence: u64,
}

/// Drain pass: extract every `InputBatch` frame from `recv_buf` and
/// publish slots under a single batch. Non-input frames are silently
/// skipped — the promotion sequence only cares about flushing pending
/// data, not validating the wire.
///
/// Sequence contiguity is enforced exactly as in
/// [`process_streaming_frames`] — these slots feed the journal the
/// about-to-be-primary replays from, so a gap accepted here becomes a
/// gapped journal on the new primary. With no error channel on the
/// drain path, the drain simply stops at the gap: everything before it
/// is flushed, everything after is unreachable anyway (it could never
/// be applied without the missing entries).
pub(super) fn process_drain_frames<E: AppEvent>(
    recv_buf: &[u8],
    input_producer: &mut melin_pipeline::ring::Producer<InputSlot<E>>,
    accum_end_sequence: u64,
    slot_buf: &mut Vec<InputSlot<E>>,
) -> DrainFrameOutcome {
    let mut consumed = 0;
    let mut last_target = 0u64;
    let mut any_published = false;
    let mut batch = input_producer.batch();
    let mut pending_accum = accum_end_sequence;

    'frames: loop {
        let remaining = &recv_buf[consumed..];
        match try_extract_frame(remaining, MAX_DATA_FRAME) {
            FrameResult::Complete(ps, fe) => {
                let payload = &remaining[ps..fe];
                if let Ok(()) = try_decode_input_batch_into(payload, slot_buf) {
                    for slot in slot_buf.drain(..) {
                        let primary_seq = slot.sequence;
                        if primary_seq <= pending_accum {
                            // Duplicate from handoff chunk overlap.
                            continue;
                        }
                        if primary_seq != pending_accum + 1 {
                            tracing::warn!(
                                expected = pending_accum + 1,
                                got = primary_seq,
                                "sequence gap in promotion drain — stopping at the \
                                 last contiguous slot"
                            );
                            break 'frames;
                        }
                        last_target = batch.push_with(|s| *s = slot);
                        pending_accum = primary_seq;
                        any_published = true;
                    }
                }
                consumed += fe;
            }
            _ => break,
        }
    }
    batch.commit();
    DrainFrameOutcome {
        consumed,
        last_target,
        any_published,
        accum_end_sequence: pending_accum,
    }
}

// ---------------------------------------------------------------------------
// Session exit + streaming result
// ---------------------------------------------------------------------------

/// Outcome of the inner streaming receive loop.
pub(super) enum SessionExit {
    Shutdown,
    Promote,
    Disconnected,
    Fatal(Box<dyn std::error::Error + Send + Sync>),
}

/// What the streaming loop returns to the caller.
pub(super) struct StreamingResult {
    pub exit: SessionExit,
    pub received_data: bool,
}

// ---------------------------------------------------------------------------
// Generic streaming loop
// ---------------------------------------------------------------------------

/// Transport-agnostic inner streaming loop for the replication receiver.
///
/// Receives `InputBatch` frames from the primary, publishes decoded slots
/// into the local pipeline's input ring, and acks durable batches back
/// via the dual-track model (persisted + in-memory). Handles shutdown,
/// promotion, and backpressure.
///
/// Parameterised over `T: ReceiverTransport` so the exact same loop
/// drives both the io_uring (kernel TCP) and DPDK (smoltcp) backends.
///
/// `initial_sequence` is the session's resume point — the highest
/// primary sequence already applied locally (handshake `last_sequence`,
/// or the snapshot sequence after a transfer). It anchors the
/// contiguity gate in [`process_streaming_frames`]: the first slot on
/// the wire must be `initial_sequence + 1`.
#[allow(clippy::too_many_arguments)]
pub(super) fn streaming_loop<T: ReceiverTransport, E: AppEvent>(
    transport: &mut T,
    input_producer: &mut melin_pipeline::ring::Producer<InputSlot<E>>,
    journal_cursor: &melin_pipeline::padding::Sequence,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    pipeline_depth: usize,
    busy_spin: bool,
    initial_sequence: u64,
    // Caller-owned receive buffer. May contain leftover bytes from the
    // handshake phase (DPDK path: smoltcp can deliver the StreamStart
    // response and the first InputBatch in a single recv, so the bytes
    // beyond the handshake frame must survive into the streaming loop).
    // The TCP path passes an empty buffer — kernel-buffered data is
    // picked up by the io_uring multishot RECV.
    mut recv_buf: Vec<u8>,
    utilization: Option<&melin_transport_core::pipeline::StageUtilization>,
) -> StreamingResult {
    let mut slot_buf: Vec<InputSlot<E>> = Vec::new();
    let mut pending_acks = PendingAckQueue::new(pipeline_depth);

    // All four cursors seed from the resume point: `accum` anchors the
    // contiguity gate, `last_committed` keeps the in-memory-ack
    // debug_assert honest, and the `last_sent_*` pair suppresses a
    // session-start ack that would otherwise fire before any data
    // arrives (in-memory coverage up to `initial_sequence` is implied
    // by the handshake itself).
    let mut accum_end_sequence: u64 = initial_sequence;
    let mut last_sent_acked_seq: u64 = initial_sequence;
    let mut last_sent_in_memory_seq: u64 = initial_sequence;
    let mut last_committed_primary_seq: u64 = initial_sequence;

    let mut received_data = false;
    let mut idle_spins: u32 = 0;
    let mut busy_count: u64 = 0;
    let mut idle_count: u64 = 0;

    let exit = loop {
        // --- Check flags ---
        if shutdown.load(Ordering::Relaxed) {
            info!("replica shutting down");
            drain_pending_acks(
                transport,
                &mut pending_acks,
                journal_cursor,
                accum_end_sequence,
                busy_spin,
                &mut recv_buf,
            );
            break SessionExit::Shutdown;
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered — stopping replication, transitioning to primary");
            // Drain remaining data from the transport.
            loop {
                let got_more = transport.poll_recv(&mut recv_buf).unwrap_or(false);
                let outcome = process_drain_frames(
                    &recv_buf,
                    input_producer,
                    accum_end_sequence,
                    &mut slot_buf,
                );
                accum_end_sequence = outcome.accum_end_sequence;
                compact_recv_buf(&mut recv_buf, outcome.consumed);
                if outcome.any_published && !pending_acks.is_full() {
                    pending_acks.push(outcome.last_target, accum_end_sequence);
                }
                if !got_more {
                    break;
                }
            }
            drain_pending_acks(
                transport,
                &mut pending_acks,
                journal_cursor,
                accum_end_sequence,
                busy_spin,
                &mut recv_buf,
            );
            break SessionExit::Promote;
        }

        // --- Flush acks (dual-track) ---
        if !transport.ack_in_flight()
            && let Some(ack) = try_flush_dual_track(
                &mut pending_acks,
                journal_cursor,
                accum_end_sequence,
                last_sent_acked_seq,
                last_sent_in_memory_seq,
            )
        {
            debug_assert!(
                ack.in_memory_sequence <= last_committed_primary_seq,
                "in_memory ack ahead of committed cursor: in_memory={}, last_committed={}",
                ack.in_memory_sequence,
                last_committed_primary_seq,
            );
            match transport.send_ack(&ack) {
                Ok(true) => {
                    last_sent_acked_seq = ack.acked_sequence;
                    last_sent_in_memory_seq = ack.in_memory_sequence;
                }
                Ok(false) => {} // in flight, try next iteration
                Err(_) => break SessionExit::Disconnected,
            }
        }

        // --- Backpressure: pending_acks full ---
        if pending_acks.is_full() {
            // Wait for any in-flight ack to complete first.
            let mut bp_idle_spins: u32 = 0;
            while transport.ack_in_flight() {
                // poll_recv also processes SEND CQEs (io_uring) to clear
                // the in-flight flag.
                if transport.poll_recv(&mut recv_buf).is_err() {
                    break;
                }
                if busy_spin || bp_idle_spins < 1000 {
                    bp_idle_spins = bp_idle_spins.wrapping_add(1);
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
            }

            let seq = pending_acks.pop_oldest_blocking(journal_cursor, busy_spin);
            let in_mem_now = accum_end_sequence;
            debug_assert!(
                in_mem_now <= last_committed_primary_seq,
                "backpressure in_memory ack ahead of committed: in_memory={}, last_committed={}",
                in_mem_now,
                last_committed_primary_seq,
            );
            let ack = Ack {
                acked_sequence: seq,
                in_memory_sequence: in_mem_now,
            };
            match transport.send_ack(&ack) {
                Ok(_) => {
                    last_sent_acked_seq = seq;
                    last_sent_in_memory_seq = in_mem_now;
                }
                Err(_) => break SessionExit::Disconnected,
            }
        }

        // --- Receive data ---
        let any_data = match transport.poll_recv(&mut recv_buf) {
            Ok(d) => d,
            Err(_) => break SessionExit::Disconnected,
        };

        // Check connection after recv — if disconnected and recv_buf is
        // empty there's nothing left to process.
        if !transport.is_connected() && recv_buf.is_empty() {
            drain_pending_acks(
                transport,
                &mut pending_acks,
                journal_cursor,
                accum_end_sequence,
                busy_spin,
                &mut recv_buf,
            );
            break SessionExit::Disconnected;
        }

        // --- Parse frames ---
        let outcome =
            process_streaming_frames(&recv_buf, input_producer, accum_end_sequence, &mut slot_buf);
        accum_end_sequence = outcome.accum_end_sequence;
        last_committed_primary_seq = accum_end_sequence;
        received_data |= outcome.received_data;
        compact_recv_buf(&mut recv_buf, outcome.consumed);

        if let Some(e) = outcome.frame_err {
            break SessionExit::Fatal(e);
        }

        if outcome.any_published && !pending_acks.is_full() {
            pending_acks.push(outcome.last_target, accum_end_sequence);
        }

        // --- Idle wait ---
        if !any_data && !outcome.any_published {
            idle_count += 1;
            if busy_spin || idle_spins < 1000 {
                idle_spins = idle_spins.wrapping_add(1);
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        } else {
            busy_count += 1;
            idle_spins = 0;
        }
    };

    if let Some(u) = utilization {
        u.busy.store(busy_count, Ordering::Relaxed);
        u.idle.store(idle_count, Ordering::Relaxed);
    }

    StreamingResult {
        exit,
        received_data,
    }
}

/// Best-effort: wait for all pending batches to become durable, then
/// send a final cumulative ack. Used on shutdown, promote, and
/// disconnect exits.
fn drain_pending_acks<T: ReceiverTransport>(
    transport: &mut T,
    pending_acks: &mut PendingAckQueue,
    journal_cursor: &melin_pipeline::padding::Sequence,
    accum_end_sequence: u64,
    busy_spin: bool,
    recv_buf: &mut Vec<u8>,
) {
    if let Some(seq) = pending_acks.pop_all_blocking(journal_cursor, busy_spin) {
        let ack = Ack {
            acked_sequence: seq,
            in_memory_sequence: accum_end_sequence,
        };
        // Best-effort: session is ending; failure just means the primary won't advance its cursor.
        let _ = transport.send_ack(&ack);
        let mut attempts = 0u32;
        while transport.ack_in_flight() && attempts < 64 {
            // Best-effort drain; we're already on the exit path.
            let _ = transport.poll_recv(recv_buf);
            attempts += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use melin_app::{AppEvent, CodecError};
    use melin_journal::JournalEvent;
    use melin_pipeline::ring::DisruptorBuilder;
    use melin_transport_core::pipeline::InputSlot;
    use melin_transport_core::replication::protocol::{encode_heartbeat, encode_input_batch};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u8);

    impl AppEvent for TestEvent {
        fn encoded_size(&self) -> usize {
            1
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[0] = self.0;
            1
        }
        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            Ok(TestEvent(buf[0]))
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    fn slot(primary_seq: u64, tag: u64) -> InputSlot<TestEvent> {
        InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: tag,
            sequence: primary_seq,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent(tag as u8)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        }
    }

    fn append_input_batch_frame(out: &mut Vec<u8>, slots: &[InputSlot<TestEvent>]) {
        encode_input_batch(slots, out);
    }

    fn drain(
        consumer: &mut melin_pipeline::ring::Consumer<InputSlot<TestEvent>>,
    ) -> Vec<InputSlot<TestEvent>> {
        let mut out = Vec::new();
        while let Some((_seq, slot)) = consumer.try_consume() {
            out.push(slot);
        }
        out
    }

    fn ring(
        capacity: usize,
    ) -> (
        melin_pipeline::ring::Producer<InputSlot<TestEvent>>,
        melin_pipeline::ring::Consumer<InputSlot<TestEvent>>,
    ) {
        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(capacity)
            .add_consumer()
            .build();
        (producer, consumers.pop().expect("consumer present"))
    }

    // ---------------------------------------------------------------
    // MockTransport for streaming_loop tests
    // ---------------------------------------------------------------

    use std::collections::VecDeque;
    use std::sync::atomic::AtomicU64;

    struct MockTransport {
        // Chunks of data to deliver on successive poll_recv calls.
        recv_queue: VecDeque<Vec<u8>>,
        // Acks sent via send_ack (sequence pairs).
        sent_acks: Vec<Ack>,
        connected: bool,
        // Simulate async ack: when true, send_ack sets in_flight and
        // the next poll_recv clears it.
        simulate_in_flight: bool,
        in_flight: bool,
    }

    impl MockTransport {
        fn new() -> Self {
            Self {
                recv_queue: VecDeque::new(),
                sent_acks: Vec::new(),
                connected: true,
                simulate_in_flight: false,
                in_flight: false,
            }
        }

        fn push_data(&mut self, data: Vec<u8>) {
            self.recv_queue.push_back(data);
        }

        fn disconnect_after_data(&mut self) {
            self.connected = false;
        }
    }

    impl ReceiverTransport for MockTransport {
        fn poll_recv(&mut self, recv_buf: &mut Vec<u8>) -> io::Result<bool> {
            if self.in_flight {
                self.in_flight = false;
            }
            if let Some(chunk) = self.recv_queue.pop_front() {
                recv_buf.extend_from_slice(&chunk);
                Ok(true)
            } else {
                Ok(false)
            }
        }

        fn send_ack(&mut self, ack: &Ack) -> io::Result<bool> {
            if self.in_flight {
                return Ok(false);
            }
            self.sent_acks.push(Ack {
                acked_sequence: ack.acked_sequence,
                in_memory_sequence: ack.in_memory_sequence,
            });
            if self.simulate_in_flight {
                self.in_flight = true;
            }
            Ok(true)
        }

        fn ack_in_flight(&self) -> bool {
            self.in_flight
        }

        fn is_connected(&mut self) -> bool {
            self.connected || !self.recv_queue.is_empty()
        }
    }

    /// Build a journal cursor (CachePadded<AtomicU64>) at the given value.
    fn journal_cursor(val: u64) -> melin_pipeline::padding::Sequence {
        melin_pipeline::padding::CachePadded::new(AtomicU64::new(val))
    }

    // ---------------------------------------------------------------
    // streaming_loop tests
    // ---------------------------------------------------------------

    #[test]
    fn loop_shutdown_exits_immediately() {
        let (mut producer, _consumer) = ring(16);
        let cursor = journal_cursor(0);
        let shutdown = AtomicBool::new(true);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            4,
            false,
            0,
            Vec::new(),
            None,
        );

        assert!(matches!(result.exit, SessionExit::Shutdown));
        assert!(!result.received_data);
    }

    #[test]
    fn loop_promote_drains_data_then_exits() {
        let (mut producer, mut consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(true);
        let mut transport = MockTransport::new();

        // Queue one InputBatch that the promote drain should flush.
        let mut data = Vec::new();
        append_input_batch_frame(&mut data, &[slot(1, 0x01)]);
        transport.push_data(data);

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            4,
            false,
            0,
            Vec::new(),
            None,
        );

        assert!(matches!(result.exit, SessionExit::Promote));
        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 1, "promote drain should publish pending data");
        assert_eq!(slots[0].request_seq, 0x01);
    }

    #[test]
    fn loop_disconnect_on_poll_error() {
        let (mut producer, _consumer) = ring(16);
        let cursor = journal_cursor(0);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();
        transport.disconnect_after_data();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            4,
            false,
            0,
            Vec::new(),
            None,
        );

        assert!(matches!(result.exit, SessionExit::Disconnected));
    }

    #[test]
    fn loop_receives_data_and_acks() {
        let (mut producer, mut consumer) = ring(16);
        // Journal cursor at u64::MAX so pending acks are immediately durable.
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();

        let mut data = Vec::new();
        append_input_batch_frame(&mut data, &[slot(10, 0xA0), slot(11, 0xA1)]);
        transport.push_data(data);
        transport.disconnect_after_data();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            4,
            false,
            9,
            Vec::new(),
            None,
        );

        assert!(matches!(result.exit, SessionExit::Disconnected));
        assert!(result.received_data);

        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 2);
        assert_eq!(slots[0].sequence, 10);
        assert_eq!(slots[1].sequence, 11);

        assert!(
            !transport.sent_acks.is_empty(),
            "should have sent at least one ack"
        );
        let last_ack = transport.sent_acks.last().unwrap();
        assert_eq!(last_ack.in_memory_sequence, 11);
    }

    #[test]
    fn loop_handles_initial_recv_buf_data() {
        let (mut producer, mut consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();
        transport.disconnect_after_data();

        // Simulate leftover handshake data in recv_buf.
        let mut initial = Vec::new();
        append_input_batch_frame(&mut initial, &[slot(1, 0x42)]);

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            4,
            false,
            0,
            initial,
            None,
        );

        assert!(result.received_data);
        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].request_seq, 0x42);
    }

    #[test]
    fn loop_backpressure_waits_for_in_flight_ack() {
        let (mut producer, mut consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();
        transport.simulate_in_flight = true;

        // Pipeline depth of 1 means the queue fills after a single push,
        // triggering the backpressure path on the second batch.
        // With simulate_in_flight=true, the first ack sets in_flight;
        // the backpressure loop calls poll_recv which clears it.
        let mut data1 = Vec::new();
        append_input_batch_frame(&mut data1, &[slot(1, 0x01)]);
        let mut data2 = Vec::new();
        append_input_batch_frame(&mut data2, &[slot(2, 0x02)]);
        transport.push_data(data1);
        transport.push_data(data2);
        transport.disconnect_after_data();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            1, // pipeline_depth=1 → PendingAckQueue cap=1
            false,
            0,
            Vec::new(),
            None,
        );

        assert!(matches!(result.exit, SessionExit::Disconnected));
        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 2, "both batches should be published");

        assert!(
            transport.sent_acks.len() >= 2,
            "should have sent acks for both batches (got {})",
            transport.sent_acks.len()
        );
    }

    #[test]
    fn loop_fatal_on_oversize_frame() {
        let (mut producer, _consumer) = ring(16);
        let cursor = journal_cursor(0);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();

        let mut data = Vec::new();
        let oversize_len = (MAX_DATA_FRAME as u32) + 1;
        data.extend_from_slice(&oversize_len.to_le_bytes());
        transport.push_data(data);

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            4,
            false,
            0,
            Vec::new(),
            None,
        );

        assert!(matches!(result.exit, SessionExit::Fatal(_)));
    }

    #[test]
    fn loop_final_ack_on_shutdown_includes_durable_sequence() {
        let (mut producer, _consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();

        let mut data = Vec::new();
        append_input_batch_frame(&mut data, &[slot(42, 0xFF)]);
        transport.push_data(data);

        // Push data first, then signal shutdown on the next poll.
        // The mock delivers data on the first poll_recv, then
        // returns false. The loop processes the data, then on the
        // next iteration checks the shutdown flag.
        let mut data2 = Vec::new();
        append_input_batch_frame(&mut data2, &[slot(43, 0xFE)]);
        transport.push_data(data2);

        // We need the loop to process at least one batch before
        // shutdown. Use a thread to set shutdown after a short delay.
        let shutdown_ref = &shutdown;
        std::thread::scope(|s| {
            s.spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(10));
                shutdown_ref.store(true, Ordering::Release);
            });

            let result = streaming_loop::<MockTransport, TestEvent>(
                &mut transport,
                &mut producer,
                &cursor,
                shutdown_ref,
                &promote,
                4,
                false,
                41,
                Vec::new(),
                None,
            );

            assert!(matches!(result.exit, SessionExit::Shutdown));
        });

        // The final drain_pending_acks should have sent an ack.
        assert!(
            !transport.sent_acks.is_empty(),
            "shutdown should send a final ack for durable data"
        );
    }

    #[test]
    fn loop_tracks_utilization_when_provided() {
        let (mut producer, _consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();

        let mut data = Vec::new();
        append_input_batch_frame(&mut data, &[slot(1, 0x01)]);
        transport.push_data(data);
        transport.disconnect_after_data();

        let utilization = melin_transport_core::pipeline::StageUtilization::new();

        let _result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            4,
            false,
            0,
            Vec::new(),
            Some(&utilization),
        );

        let busy = utilization.busy.load(Ordering::Relaxed);
        let idle = utilization.idle.load(Ordering::Relaxed);
        assert!(busy > 0, "should have recorded busy iterations");
        assert!(busy + idle > 0, "total iterations should be non-zero");
    }

    // ---------------------------------------------------------------
    // Frame processing tests (existing)
    // ---------------------------------------------------------------

    #[test]
    fn streaming_publishes_all_slots_and_advances_accum_end_sequence() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(6, 0xA0), slot(7, 0xA1), slot(8, 0xA2)]);
        encode_heartbeat(99, &mut buf);
        append_input_batch_frame(&mut buf, &[slot(9, 0xA3)]);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 5, &mut slot_buf);

        assert!(outcome.frame_err.is_none(), "no fatal exit");
        assert_eq!(outcome.consumed, buf.len(), "every byte processed");
        assert!(outcome.any_published);
        assert!(outcome.received_data);
        assert_eq!(outcome.accum_end_sequence, 9);

        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 4);
        let ids: Vec<u64> = slots.iter().map(|s| s.request_seq).collect();
        assert_eq!(ids, vec![0xA0, 0xA1, 0xA2, 0xA3]);
    }

    #[test]
    fn streaming_oversize_frame_commits_prior_slots_then_fatal() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(7, 0xB0)]);
        append_input_batch_frame(&mut buf, &[slot(8, 0xB1)]);
        let oversize_len = (MAX_DATA_FRAME as u32) + 1;
        buf.extend_from_slice(&oversize_len.to_le_bytes());

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 6, &mut slot_buf);

        assert!(outcome.frame_err.is_some(), "oversize => fatal");
        assert_eq!(outcome.accum_end_sequence, 8);
        assert!(outcome.any_published);
        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 2);
    }

    #[test]
    fn streaming_unknown_message_after_valid_input_commits_then_fatal() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(3, 0xC0), slot(4, 0xC1)]);
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0xFF);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 2, &mut slot_buf);

        assert!(outcome.frame_err.is_some(), "unknown primary msg => fatal");
        assert_eq!(outcome.accum_end_sequence, 4);
        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 2);
    }

    #[test]
    fn streaming_partial_trailing_frame_is_incomplete_not_fatal() {
        let (mut producer, mut consumer) = ring(8);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(1, 0xD0)]);
        let complete_len = buf.len();
        buf.extend_from_slice(&[0xDE, 0xAD, 0xBE]);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 0, &mut slot_buf);

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.consumed, complete_len);
        assert_eq!(outcome.accum_end_sequence, 1);
        assert_eq!(drain(&mut consumer).len(), 1);
    }

    #[test]
    fn streaming_heartbeat_only_does_not_advance_accum_end_sequence() {
        let (mut producer, mut consumer) = ring(8);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        encode_heartbeat(42, &mut buf);
        encode_heartbeat(43, &mut buf);

        let outcome =
            process_streaming_frames::<TestEvent>(&buf, &mut producer, 100, &mut slot_buf);

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.consumed, buf.len());
        assert!(!outcome.any_published);
        assert!(!outcome.received_data);
        assert_eq!(outcome.accum_end_sequence, 100);
        assert!(drain(&mut consumer).is_empty());
    }

    #[test]
    fn streaming_empty_buffer_is_a_noop() {
        let (mut producer, mut consumer) = ring(4);
        let mut slot_buf = Vec::new();

        let outcome = process_streaming_frames::<TestEvent>(&[], &mut producer, 77, &mut slot_buf);

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.consumed, 0);
        assert!(!outcome.any_published);
        assert!(!outcome.received_data);
        assert_eq!(outcome.accum_end_sequence, 77);
        assert!(drain(&mut consumer).is_empty());
    }

    // ---------------------------------------------------------------
    // Sequence-contiguity tests
    //
    // The wire stream is the replica's only source of truth for journal
    // sequences — the journal stage stamps `slot.sequence` verbatim
    // (`set_next_sequence(slot.sequence + 1)`), so anything the receiver
    // publishes lands in the replica's journal at face value. The
    // receiver is therefore the gate: a slot whose sequence skips ahead
    // of the last accepted one must be a fatal protocol violation, never
    // silently applied. Regression: the 2026-06-07 LAN bench shipped a
    // reconnecting replica a stream with a 212-entry hole (catch-up →
    // live handoff race on the primary); the replica accepted it, acked
    // past the hole, and its journal failed lineage verification only
    // at post-run audit.
    //
    // Pinned semantics, mirroring TCP-style cumulative delivery:
    //   seq <= accum      → skip (idempotent re-delivery: the first
    //                       live chunk after catch-up may straddle the
    //                       catch-up end and re-carry covered slots)
    //   seq == accum + 1  → accept
    //   seq >  accum + 1  → fatal — a gap can never be repaired
    //                       downstream; acking past it overstates
    //                       durability and corrupts the journal lineage.
    // ---------------------------------------------------------------

    #[test]
    fn streaming_rejects_sequence_gap_across_frames() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(10, 0xA0), slot(11, 0xA1)]);
        // 11 → 14: entries 12..=13 are missing from the wire.
        append_input_batch_frame(&mut buf, &[slot(14, 0xA2), slot(15, 0xA3)]);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 9, &mut slot_buf);

        assert!(
            outcome.frame_err.is_some(),
            "a sequence gap (11 → 14) must be a fatal protocol violation, \
             not silently accepted"
        );
        let published: Vec<u64> = drain(&mut consumer).iter().map(|s| s.sequence).collect();
        assert_eq!(
            published,
            vec![10, 11],
            "nothing at or beyond the gap may reach the input ring"
        );
        assert_eq!(
            outcome.accum_end_sequence, 11,
            "accum must stop at the last contiguous slot"
        );
    }

    #[test]
    fn streaming_rejects_sequence_gap_within_a_frame() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        // 11 → 13 inside a single InputBatch: entry 12 is missing.
        append_input_batch_frame(&mut buf, &[slot(10, 0xB0), slot(11, 0xB1), slot(13, 0xB2)]);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 9, &mut slot_buf);

        assert!(
            outcome.frame_err.is_some(),
            "an intra-frame sequence gap (11 → 13) must be fatal"
        );
        let published: Vec<u64> = drain(&mut consumer).iter().map(|s| s.sequence).collect();
        assert_eq!(
            published,
            vec![10, 11],
            "the contiguous prefix is committed; the slot past the gap is not \
             (mirrors the oversize-frame semantics: commit prior progress, then fatal)"
        );
        assert_eq!(outcome.accum_end_sequence, 11);
    }

    #[test]
    fn streaming_skips_already_applied_slots_instead_of_reapplying() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(10, 0xC0), slot(11, 0xC1)]);
        // Overlapping re-delivery: the first live chunk after catch-up
        // may straddle the catch-up end and re-carry slot 11. The
        // duplicate must be dropped, the new slot accepted.
        append_input_batch_frame(&mut buf, &[slot(11, 0xC1), slot(12, 0xC2)]);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 9, &mut slot_buf);

        assert!(
            outcome.frame_err.is_none(),
            "covered-slot re-delivery is benign overlap, not a violation: {:?}",
            outcome.frame_err
        );
        let published: Vec<u64> = drain(&mut consumer).iter().map(|s| s.sequence).collect();
        assert_eq!(
            published,
            vec![10, 11, 12],
            "slot 11 must be applied exactly once — re-publishing rewinds the \
             replica journal's sequence counter and corrupts its lineage"
        );
        assert_eq!(outcome.accum_end_sequence, 12);
    }

    #[test]
    fn drain_does_not_publish_past_a_sequence_gap() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(10, 0xD0), slot(11, 0xD1)]);
        append_input_batch_frame(&mut buf, &[slot(14, 0xD2)]);

        // The promote drain flushes buffered frames straight into the
        // pipeline that the about-to-be-primary replays from — a gap
        // accepted here becomes a gapped journal on the new primary, at
        // the worst possible moment.
        let outcome = process_drain_frames::<TestEvent>(&buf, &mut producer, 9, &mut slot_buf);

        let published: Vec<u64> = drain(&mut consumer).iter().map(|s| s.sequence).collect();
        assert_eq!(
            published,
            vec![10, 11],
            "promotion drain must not publish slots past a sequence gap"
        );
        assert_eq!(outcome.accum_end_sequence, 11);
    }

    /// Loop-level pin of the durability contract: after a gapped wire
    /// stream, the session must end fatally and no ack — persisted or
    /// in-memory — may ever name a sequence past the last contiguous
    /// slot. In the bench failure the replica kept acking for the rest
    /// of the 60s run with a 212-entry hole behind its cursors,
    /// overstating durability to the primary's response gate.
    #[test]
    fn streaming_loop_sequence_gap_is_fatal_and_never_acked_past() {
        let (mut producer, mut consumer) = ring(16);
        // Journal cursor at u64::MAX so pending acks are immediately
        // durable — ack content is what's under test, not flush timing.
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let promote = AtomicBool::new(false);
        let mut transport = MockTransport::new();

        let mut data1 = Vec::new();
        append_input_batch_frame(&mut data1, &[slot(1, 0x01), slot(2, 0x02)]);
        transport.push_data(data1);
        let mut data2 = Vec::new();
        // 2 → 5: entries 3..=4 never arrive.
        append_input_batch_frame(&mut data2, &[slot(5, 0x05), slot(6, 0x06)]);
        transport.push_data(data2);
        transport.disconnect_after_data();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &promote,
            4,
            false,
            0,
            Vec::new(),
            None,
        );

        assert!(
            matches!(result.exit, SessionExit::Fatal(_)),
            "a gapped stream must end the session fatally (got a clean exit)"
        );
        let published: Vec<u64> = drain(&mut consumer).iter().map(|s| s.sequence).collect();
        assert_eq!(
            published,
            vec![1, 2],
            "slots past the gap must not enter the pipeline"
        );
        for ack in &transport.sent_acks {
            assert!(
                ack.acked_sequence <= 2 && ack.in_memory_sequence <= 2,
                "ack ({}, {}) names sequences past the gap at 2 — durability \
                 overstated for entries the replica never received",
                ack.acked_sequence,
                ack.in_memory_sequence,
            );
        }
    }

    #[test]
    fn drain_skips_non_input_frames_and_publishes_every_input_batch() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(20, 0xE0), slot(21, 0xE1)]);
        encode_heartbeat(999, &mut buf);
        append_input_batch_frame(&mut buf, &[slot(22, 0xE2)]);

        let outcome = process_drain_frames::<TestEvent>(&buf, &mut producer, 19, &mut slot_buf);

        assert!(outcome.any_published);
        assert_eq!(outcome.consumed, buf.len());
        assert_eq!(outcome.accum_end_sequence, 22);
        let slots = drain(&mut consumer);
        let ids: Vec<u64> = slots.iter().map(|s| s.request_seq).collect();
        assert_eq!(ids, vec![0xE0, 0xE1, 0xE2]);
    }

    #[test]
    fn drain_stops_at_incomplete_trailing_frame() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(50, 0xF0)]);
        let complete_len = buf.len();
        buf.extend_from_slice(&[0xDE, 0xAD]);

        let outcome = process_drain_frames::<TestEvent>(&buf, &mut producer, 49, &mut slot_buf);

        assert_eq!(outcome.consumed, complete_len);
        assert_eq!(outcome.accum_end_sequence, 50);
        assert_eq!(drain(&mut consumer).len(), 1);
    }

    #[test]
    fn drain_empty_buffer_is_a_noop() {
        let (mut producer, mut consumer) = ring(4);
        let mut slot_buf = Vec::new();

        let outcome = process_drain_frames::<TestEvent>(&[], &mut producer, 55, &mut slot_buf);

        assert_eq!(outcome.consumed, 0);
        assert!(!outcome.any_published);
        assert_eq!(outcome.accum_end_sequence, 55);
        assert!(drain(&mut consumer).is_empty());
    }
}
