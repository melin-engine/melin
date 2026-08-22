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
use melin_transport_core::pipeline::{AdoptedRotation, InputSlot, StreamMark, StreamMarkQueue};
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
// Chunked-body transfer (snapshot / segment seed)
// ---------------------------------------------------------------------------

/// A source of length-prefixed control-frame payloads from the primary
/// during the handshake / resync phase.
///
/// Abstracts the two transports' framing — the kernel-TCP blocking
/// reader and the DPDK poll loop — so the snapshot / segment-seed
/// transfer (and its tests) are transport-generic. Returns the decoded
/// frame *payload* (what [`decode_primary_message`] consumes), not the
/// length prefix. Cold path: only the one-time resync transfer drives
/// it.
pub(super) trait ControlFrameSource {
    /// Block until the next complete frame arrives; return its payload
    /// bytes. Errors on disconnect, an oversize / malformed frame, or a
    /// shutdown request.
    fn next_frame(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Receive a chunked body (`SnapshotChunk*` → `SnapshotEnd`) into
/// `tmp_path`, verifying the byte length and CRC32C trailer — the
/// framing shared by the snapshot payload and the segment seed. The tmp
/// file is removed on any failure (including transport errors), so
/// callers never see a partial file. Shared by both receivers via
/// [`ControlFrameSource`].
pub(super) fn receive_chunked_body<S: ControlFrameSource>(
    source: &mut S,
    tmp_path: &std::path::Path,
    expected_len: u64,
    what: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let result = (|| -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut tmp_file = std::fs::File::create(tmp_path)?;
        let mut received: u64 = 0;
        let mut running_crc: u32 = 0;
        loop {
            let frame = source.next_frame(MAX_DATA_FRAME)?;
            match decode_primary_message(&frame)? {
                PrimaryMessage::SnapshotChunk(data) => {
                    std::io::Write::write_all(&mut tmp_file, &data)?;
                    received += data.len() as u64;
                    running_crc = crc32c::crc32c_append(running_crc, &data);
                }
                PrimaryMessage::SnapshotEnd {
                    crc32c: expected_crc,
                } => {
                    tmp_file.sync_all()?;
                    if received != expected_len {
                        return Err(format!(
                            "{what} length mismatch: expected {expected_len} bytes, got {received}"
                        )
                        .into());
                    }
                    if running_crc != expected_crc {
                        return Err(format!(
                            "{what} CRC mismatch: expected {expected_crc:#x}, got {running_crc:#x}"
                        )
                        .into());
                    }
                    return Ok(());
                }
                other => {
                    return Err(format!("expected {what} SnapshotChunk/End, got {other:?}").into());
                }
            }
        }
    })();
    if result.is_err() {
        // Best-effort: a partial tmp file must not survive the failed
        // transfer (it would shadow the next attempt's write).
        let _ = std::fs::remove_file(tmp_path);
    }
    result
}

// ---------------------------------------------------------------------------
// Streaming frame processing
// ---------------------------------------------------------------------------

/// Exact-position rule shared by every stream-mark frame (`Rotate`,
/// `ChainCheck`): queue the mark only when the stream position sits
/// exactly on it — the queue push happens before any slot past the
/// mark is committed to the input ring, which is the ordering the
/// journal stage's split logic relies on. A mark strictly behind the
/// position is redundant re-delivery (handoff overlap) and is dropped
/// like a duplicate slot; one ahead implies missing entries — the same
/// contiguity-break contract as a slot-sequence gap
/// ([`FrameError::SequenceGap`]: end the session, reconnect).
fn queue_stream_mark(
    stream_marks: &StreamMarkQueue,
    pending_accum: u64,
    kind: &'static str,
    mark: StreamMark,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let position = mark.sequence();
    if position == pending_accum {
        stream_marks
            .lock()
            .expect("stream-mark queue poisoned")
            .push_back(mark);
        Ok(())
    } else if position < pending_accum {
        debug!(
            position,
            accum = pending_accum,
            kind,
            "stale stream mark skipped"
        );
        Ok(())
    } else {
        Err(
            format!("{kind} at {position} ahead of stream position {pending_accum} — sequence gap")
                .into(),
        )
    }
}

/// Why [`process_streaming_frames`] stopped consuming frames.
///
/// Two outcomes that both end the session but must not be handled the
/// same way: a contiguity break is a *stream* problem the replica
/// recovers from by re-handshaking at its durable position, whereas a
/// protocol violation or journal death is unrecoverable in-process.
#[derive(Debug)]
pub(super) enum FrameError {
    /// The wire stream skipped past the contiguous prefix — a slot at
    /// `seq > accum + 1`, or a stream mark announced ahead of the
    /// stream position. Everything before the gap is committed; the
    /// local journal is intact. Reconnect and re-handshake.
    SequenceGap(Box<dyn std::error::Error + Send + Sync>),
    /// Anything else — malformed/oversized frames, an unexpected
    /// primary message, or the journal stage dying mid-publish.
    Fatal(Box<dyn std::error::Error + Send + Sync>),
}

/// Slots published under one `Producer::batch` before it is committed
/// and reopened. Caps how long a decoded slot stays invisible to the
/// replica's journal stage; mirrors `COMMIT_EVERY` on client ingress.
const COMMIT_EVERY: u64 = 16;

/// Slots published per [`process_streaming_frames`] call, checked at
/// frame boundaries.
///
/// The cap is what lets the receive loop breathe during a catch-up
/// burst: after a stall the primary can hand over hundreds of kilobytes
/// at once, and without a bound the loop decodes all of it — thousands
/// of slots — before returning to the ack flush at the top of the loop.
/// Both ack tracks (in-memory, which the primary's `disk+ram` gate runs
/// on, and persisted) would then sit still for the whole drain. At ~50 ns
/// a slot this bounds that blind spot to a few tens of microseconds; the
/// remainder stays in the recv buffer and is picked up on the next
/// iteration, one ack later.
const MAX_SLOTS_PER_CYCLE: usize = 512;

/// Outcome of [`process_streaming_frames`] for one recv-cycle.
pub(super) struct StreamingFrameOutcome {
    /// Bytes consumed from the recv buffer.
    pub consumed: usize,
    /// Whether any slot was pushed this cycle.
    pub any_published: bool,
    /// Updated `accum_end_sequence` — only names slots that were
    /// committed to the input ring.
    pub accum_end_sequence: u64,
    /// Whether the primary spoke this cycle — a non-empty `InputBatch`
    /// or a `Heartbeat`. Session-level liveness evidence; see
    /// [`StreamingResult::heard_from_primary`].
    pub heard_from_primary: bool,
    /// Why frame processing stopped, if it did — the caller maps a
    /// `SequenceGap` to `SessionExit::StreamGap` (reconnect) and a
    /// `Fatal` to `SessionExit::Fatal`.
    pub frame_err: Option<FrameError>,
    /// The primary's ack policy as advertised by the last `Heartbeat`
    /// in this cycle, if any — the caller folds it into the
    /// control-plane gauge (`ReplicaControlPlane::primary_ack_policy`).
    pub observed_ack_policy: Option<u8>,
}

/// Process complete frames from `recv_buf`, publishing decoded slots
/// onto `input_producer` and recording one pending-ack entry per
/// `InputBatch` frame.
///
/// Uses `try_decode_input_batch_into` to decode into a reusable
/// `slot_buf`, avoiding per-batch heap allocation on the hot path.
///
/// # Publication granularity
///
/// A frame's slots are committed to the input ring and their ack target
/// recorded before the next frame is decoded (and mid-frame every
/// [`COMMIT_EVERY`] slots). One commit and one ack entry per *cycle*
/// meant the replica's journal stage saw nothing until every byte in the
/// recv buffer had been decoded, and the first frame's persisted ack
/// waited on the last frame's fsync. At most [`MAX_SLOTS_PER_CYCLE`]
/// slots are published per call so the caller gets back to its ack flush.
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
/// - `seq > accum + 1` — a [`FrameError::SequenceGap`]. A gap can never
///   be repaired downstream: acking past it overstates durability to
///   the primary's response gate, and the hole surfaces only at
///   lineage audit (the 2026-06-07 bench failure). The contiguous
///   prefix is committed (progress preserved — mirrors the
///   oversize-frame semantics); the session ends with
///   `SessionExit::StreamGap`, and the receiver reconnects and
///   re-handshakes from its durable position — the local journal is
///   intact, so this is a reconnect, not a restart.
pub(super) fn process_streaming_frames<E: AppEvent>(
    recv_buf: &[u8],
    input_producer: &mut melin_pipeline::ring::Producer<InputSlot<E>>,
    accum_end_sequence: u64,
    slot_buf: &mut Vec<InputSlot<E>>,
    stream_marks: &StreamMarkQueue,
    journal_failed: &AtomicBool,
    pending_acks: &mut PendingAckQueue,
) -> StreamingFrameOutcome {
    let mut consumed = 0;
    let mut last_target = 0u64;
    let mut any_published = false;
    let mut published_this_cycle = 0usize;
    let mut heard_from_primary = false;
    let mut frame_err: Option<FrameError> = None;
    let mut observed_ack_policy: Option<u8> = None;
    let mut batch = input_producer.batch();
    let mut pending_accum = accum_end_sequence;

    loop {
        // A dead journal stage freezes the ring's consumer cursor; with
        // enough frames in one recv cycle the publishes below would
        // fill the ring and spin forever inside the batch push. Bail
        // per-frame so at most one frame's slots are published after
        // the failure latch flips.
        if journal_failed.load(Ordering::Relaxed) {
            frame_err = Some(FrameError::Fatal("replica journal stage failed".into()));
            break;
        }
        let remaining = &recv_buf[consumed..];
        match try_extract_frame(remaining, MAX_DATA_FRAME) {
            FrameResult::Complete(payload_start, frame_end) => {
                let payload = &remaining[payload_start..frame_end];
                match try_decode_input_batch_into(payload, slot_buf) {
                    Ok(()) => {
                        if !slot_buf.is_empty() {
                            heard_from_primary = true;
                            let mut frame_published = false;
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
                                    frame_err = Some(FrameError::SequenceGap(
                                        format!(
                                            "sequence gap in replication stream: \
                                             expected {}, got {primary_seq}",
                                            pending_accum + 1
                                        )
                                        .into(),
                                    ));
                                    break;
                                }
                                // Abortable push: the loop-top latch check
                                // only runs between frames — if the journal
                                // stage dies while this push is blocked on a
                                // full ring, its gate cursor never advances
                                // again and an unconditional spin would wedge
                                // the receiver forever (no Fatal exit, no
                                // teardown, no divergence repair).
                                match batch.push_with_or_abort(
                                    |s| *s = slot,
                                    || journal_failed.load(Ordering::Relaxed),
                                ) {
                                    // `push` returns the slot index; the
                                    // journal cursor is next-to-consume,
                                    // so the slot is durable once the
                                    // cursor reaches index + 1.
                                    Ok(index) => last_target = index + 1,
                                    Err(_full) => {
                                        frame_err = Some(FrameError::Fatal(
                                            "replica journal stage failed \
                                             (ring full mid-publish)"
                                                .into(),
                                        ));
                                        break;
                                    }
                                }
                                pending_accum = primary_seq;
                                any_published = true;
                                frame_published = true;
                                published_this_cycle += 1;
                                if batch.len() >= COMMIT_EVERY {
                                    batch.commit();
                                    batch = input_producer.batch();
                                }
                            }
                            if frame_published {
                                // Publish this frame's slots and record
                                // their ack target before decoding the
                                // next one: the journal stage can start
                                // on them now, and their persisted ack
                                // no longer waits on the rest of the
                                // cycle's fsync. Committed first so the
                                // queued target names slots the ring has
                                // actually published.
                                batch.commit();
                                batch = input_producer.batch();
                                pending_acks.push(last_target, pending_accum);
                            }
                            if frame_err.is_some() {
                                break;
                            }
                        }
                    }
                    Err(_) => match decode_primary_message(payload) {
                        Ok(PrimaryMessage::Heartbeat {
                            sequence,
                            ack_policy,
                        }) => {
                            debug!(sequence, ack_policy, "heartbeat from primary");
                            observed_ack_policy = Some(ack_policy);
                            // A heartbeat is the primary speaking — on a
                            // quiet system it is the only liveness
                            // evidence a session produces.
                            heard_from_primary = true;
                        }
                        Ok(PrimaryMessage::Rotate {
                            boundary_seq,
                            tail_hash,
                        }) => {
                            if let Err(e) = queue_stream_mark(
                                stream_marks,
                                pending_accum,
                                "Rotate",
                                StreamMark::Rotate(AdoptedRotation {
                                    boundary_seq,
                                    tail_hash,
                                }),
                            ) {
                                // A mark ahead of the stream position is
                                // the same contiguity break as a skipped
                                // slot: something before it never arrived.
                                frame_err = Some(FrameError::SequenceGap(e));
                                break;
                            }
                        }
                        Ok(PrimaryMessage::ChainCheck {
                            sequence,
                            chain_hash,
                        }) => {
                            if let Err(e) = queue_stream_mark(
                                stream_marks,
                                pending_accum,
                                "ChainCheck",
                                StreamMark::ChainCheck {
                                    sequence,
                                    chain_hash,
                                },
                            ) {
                                frame_err = Some(FrameError::SequenceGap(e));
                                break;
                            }
                        }
                        Ok(PrimaryMessage::NeedSnapshot) => {
                            frame_err = Some(FrameError::Fatal(
                                "primary says we need a snapshot transfer mid-stream".into(),
                            ));
                            break;
                        }
                        Ok(PrimaryMessage::HashMismatch) => {
                            frame_err =
                                Some(FrameError::Fatal("chain hash mismatch from primary".into()));
                            break;
                        }
                        Ok(_) => {
                            debug!("unexpected message during streaming");
                        }
                        Err(e) => {
                            frame_err = Some(FrameError::Fatal(
                                format!("failed to decode primary message: {e}").into(),
                            ));
                            break;
                        }
                    },
                }
                consumed += frame_end;
                if published_this_cycle >= MAX_SLOTS_PER_CYCLE {
                    // Hand control back so an ack can go out; the rest of
                    // the buffer is still there next iteration. Checked
                    // at a frame boundary, so a single oversized frame
                    // can overshoot by its own slot count.
                    break;
                }
            }
            FrameResult::Oversized => {
                frame_err = Some(FrameError::Fatal(
                    "oversized frame from primary during streaming".into(),
                ));
                break;
            }
            FrameResult::Incomplete => break,
        }
    }

    batch.commit();
    StreamingFrameOutcome {
        consumed,
        any_published,
        accum_end_sequence: pending_accum,
        heard_from_primary,
        frame_err,
        observed_ack_policy,
    }
}

/// Outcome of [`process_drain_frames`] for one drain recv-cycle.
pub(super) struct DrainFrameOutcome {
    pub consumed: usize,
    /// Journal-cursor position one past the last slot pushed — see
    /// [`StreamingFrameOutcome::last_target`].
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
    journal_failed: &AtomicBool,
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
                        // Abortable for the same reason as the streaming
                        // path: a journal stage that dies mid-promotion
                        // freezes its gate cursor, and a blocked push
                        // would wedge the drain forever. Stopping here
                        // matches the gap semantics — everything after
                        // the stall is unreachable anyway.
                        match batch.push_with_or_abort(
                            |s| *s = slot,
                            || journal_failed.load(Ordering::Relaxed),
                        ) {
                            // Slot index → journal-cursor space (index + 1),
                            // as in `process_streaming_frames`.
                            Ok(index) => last_target = index + 1,
                            Err(_full) => {
                                tracing::warn!(
                                    "journal stage failed during promotion drain — \
                                     stopping at the last contiguous slot"
                                );
                                break 'frames;
                            }
                        }
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
    /// The transport dropped. Reconnect with backoff.
    Disconnected,
    /// The stream broke contiguity (see [`FrameError::SequenceGap`]).
    /// The contiguous prefix is committed and the local journal is
    /// intact, so this is handled like a disconnect — reconnect and
    /// re-handshake from the durable position — but logged with the
    /// reason, since it is the primary's stream, not the network, that
    /// misbehaved.
    StreamGap(Box<dyn std::error::Error + Send + Sync>),
    /// Unrecoverable in this process (protocol violation, journal
    /// death); mid-stream chain divergence is the one fatal the exit
    /// handler repairs by in-process resync.
    Fatal(Box<dyn std::error::Error + Send + Sync>),
}

/// What the streaming loop returns to the caller.
pub(super) struct StreamingResult {
    pub exit: SessionExit,
    /// Whether the primary spoke during the session — a non-empty
    /// `InputBatch` or a `Heartbeat` (which flow even on a quiet
    /// system). This is what the disconnect handler keys the backoff
    /// reset on: it is in-session liveness evidence, so a synthetic
    /// result from a session that never started streaming (flag false)
    /// keeps its escalated backoff.
    pub heard_from_primary: bool,
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
    // The replica's control-plane bundle: the promotion request this
    // loop polls, the advertised journal tip it advances, and the
    // primary-ack-policy gauge heartbeats refresh — see
    // [`super::ReplicaControlPlane`].
    control: &super::ReplicaControlPlane,
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
    stream_marks: &StreamMarkQueue,
    // Latched when the journal stage exits with an error (e.g. chain
    // divergence). Checked at the loop top and inside every blocking
    // wait on the journal cursor — a dead journal stage means the
    // cursor never advances, so waiting on it would wedge this thread
    // forever instead of tearing down for the reconnect/resync path.
    journal_failed: &AtomicBool,
) -> StreamingResult {
    let super::ReplicaControlPlane {
        promote,
        // Advanced to the in-memory accepted position as frames are
        // published, so raft vote filtering sees everything a promotion
        // would carry into the journal — not just the fsynced position.
        journal_tip,
        primary_ack_policy,
        // Recovery seeded it long before any streaming session.
        tip_ready: _,
        primary_link_up: _,
        // Latched by the journal thread's failure wrapper, not here.
        pipeline_healthy: _,
    } = control;
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

    // The session resume point covers this node's holdings (post-recovery
    // journal, or the just-installed snapshot); `advance` (not a store) so
    // a reconnect whose handshake read the journal before the ring settled
    // cannot regress the tip below data already accepted.
    journal_tip.advance(melin_transport_core::WireSeq::new(initial_sequence));

    let mut heard_from_primary = false;
    let mut idle_spins: u32 = 0;
    let mut busy_count: u64 = 0;
    let mut idle_count: u64 = 0;

    // Bytes of `recv_buf` already parsed. The parser reads from here and
    // `poll_recv` appends at the end, so a cycle that stops early
    // (`MAX_SLOTS_PER_CYCLE`) leaves its remainder in place instead of
    // memmoving it to the front on every iteration — which is exactly
    // the catch-up burst the cap exists for. Compaction happens when the
    // buffer drains (the steady-state case: a single `clear`) or when
    // the dead prefix grows past `RECV_COMPACT_THRESHOLD`.
    let mut recv_offset: usize = 0;
    const RECV_COMPACT_THRESHOLD: usize = 64 * 1024;

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
                journal_failed,
            );
            break SessionExit::Shutdown;
        }
        if journal_failed.load(Ordering::Acquire) {
            // The journal stage died (chain divergence, journal I/O
            // failure). Its cursor is frozen, so no further slot can
            // ever be journaled or acked — stop publishing and exit
            // fatally; teardown + restart routes the node through the
            // reconnect handshake, where divergence is repaired by
            // snapshot resync.
            break SessionExit::Fatal(
                "replica journal stage failed — tearing down for reconnect/resync".into(),
            );
        }
        if promote.is_requested() {
            info!("promotion triggered — stopping replication, transitioning to primary");
            // Normalise the parse position: the drain loop below owns the
            // buffer outright and compacts from the front.
            compact_recv_buf(&mut recv_buf, std::mem::take(&mut recv_offset));
            // Drain remaining data from the transport.
            loop {
                let got_more = transport.poll_recv(&mut recv_buf).unwrap_or(false);
                let outcome = process_drain_frames(
                    &recv_buf,
                    input_producer,
                    accum_end_sequence,
                    &mut slot_buf,
                    journal_failed,
                );
                accum_end_sequence = outcome.accum_end_sequence;
                journal_tip.advance(melin_transport_core::WireSeq::new(accum_end_sequence));
                compact_recv_buf(&mut recv_buf, outcome.consumed);
                if outcome.any_published {
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
                journal_failed,
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

        // --- Receive data ---
        let any_data = match transport.poll_recv(&mut recv_buf) {
            Ok(d) => d,
            Err(_) => break SessionExit::Disconnected,
        };

        // Check connection after recv — if disconnected and nothing is
        // left unparsed there's nothing more to process.
        if !transport.is_connected() && recv_offset == recv_buf.len() {
            drain_pending_acks(
                transport,
                &mut pending_acks,
                journal_cursor,
                accum_end_sequence,
                busy_spin,
                &mut recv_buf,
                journal_failed,
            );
            break SessionExit::Disconnected;
        }

        // --- Parse frames ---
        //
        // Ack targets are recorded per frame inside the call (never
        // gated on queue occupancy: a full queue merges into its newest
        // entry, so receiving continues at wire rate however far behind
        // the local fsync is).
        let outcome = process_streaming_frames(
            &recv_buf[recv_offset..],
            input_producer,
            accum_end_sequence,
            &mut slot_buf,
            stream_marks,
            journal_failed,
            &mut pending_acks,
        );
        accum_end_sequence = outcome.accum_end_sequence;
        last_committed_primary_seq = accum_end_sequence;
        journal_tip.advance(melin_transport_core::WireSeq::new(accum_end_sequence));
        if let Some(policy) = outcome.observed_ack_policy {
            // Heartbeats refresh the replica's view of the policy the
            // primary acks under (runtime `ACK-POLICY` retunes).
            primary_ack_policy.store(policy, Ordering::Release);
        }
        heard_from_primary |= outcome.heard_from_primary;

        recv_offset += outcome.consumed;
        if recv_offset == recv_buf.len() {
            recv_buf.clear();
            recv_offset = 0;
        } else if recv_offset >= RECV_COMPACT_THRESHOLD {
            compact_recv_buf(&mut recv_buf, recv_offset);
            recv_offset = 0;
        }

        match outcome.frame_err {
            Some(FrameError::SequenceGap(e)) => break SessionExit::StreamGap(e),
            Some(FrameError::Fatal(e)) => break SessionExit::Fatal(e),
            None => {}
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

    if pending_acks.merged() > 0 {
        // Not an error: the session kept receiving and acking, only the
        // persisted-ack granularity was coarsened. Worth a line because
        // it means the local journal stage spent time more than a full
        // queue behind the wire.
        debug!(
            merged = pending_acks.merged(),
            depth = pipeline_depth,
            "replica ack queue coarsened — journal fsync ran behind the wire"
        );
    }

    if let Some(u) = utilization {
        u.busy.store(busy_count, Ordering::Relaxed);
        u.idle.store(idle_count, Ordering::Relaxed);
    }

    StreamingResult {
        exit,
        heard_from_primary,
    }
}

/// Best-effort: wait for all pending batches to become durable, then
/// send a final cumulative ack. Used on shutdown, promote, and
/// disconnect exits. `journal_failed` aborts the durability wait — a
/// dead journal stage would otherwise hang the exit path forever.
fn drain_pending_acks<T: ReceiverTransport>(
    transport: &mut T,
    pending_acks: &mut PendingAckQueue,
    journal_cursor: &melin_pipeline::padding::Sequence,
    accum_end_sequence: u64,
    busy_spin: bool,
    recv_buf: &mut Vec<u8>,
    journal_failed: &AtomicBool,
) {
    if let Some(seq) = pending_acks.pop_all_blocking(journal_cursor, busy_spin, journal_failed) {
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

    /// `process_streaming_frames` against a scratch ack queue, handing
    /// both back. Most tests only assert on what was published; the ones
    /// that care about ack targets read the returned queue.
    fn stream_frames(
        buf: &[u8],
        producer: &mut melin_pipeline::ring::Producer<InputSlot<TestEvent>>,
        accum_end_sequence: u64,
        slot_buf: &mut Vec<InputSlot<TestEvent>>,
        marks: &StreamMarkQueue,
        journal_failed: &AtomicBool,
    ) -> (StreamingFrameOutcome, PendingAckQueue) {
        let mut acks = PendingAckQueue::new(16);
        let outcome = process_streaming_frames::<TestEvent>(
            buf,
            producer,
            accum_end_sequence,
            slot_buf,
            marks,
            journal_failed,
            &mut acks,
        );
        (outcome, acks)
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

    /// Empty adopted-rotation queue for tests that don't exercise
    /// rotation adoption.
    fn no_marks() -> StreamMarkQueue {
        std::sync::Arc::new(std::sync::Mutex::new(VecDeque::new()))
    }

    /// Throwaway control-plane bundle — these tests don't exercise
    /// raft; `streaming_loop` just needs somewhere to write.
    fn control() -> crate::replication::ReplicaControlPlane {
        crate::replication::ReplicaControlPlane::new()
    }

    /// A bundle whose promotion request is already filed (manually) —
    /// for the promote drain tests.
    fn promoted_control() -> crate::replication::ReplicaControlPlane {
        let c = control();
        c.promote
            .request(crate::promotion::PromotionRequest::MANUAL);
        c
    }

    // ---------------------------------------------------------------
    // streaming_loop tests
    // ---------------------------------------------------------------

    #[test]
    fn loop_shutdown_exits_immediately() {
        let (mut producer, _consumer) = ring(16);
        let cursor = journal_cursor(0);
        let shutdown = AtomicBool::new(true);
        let control = control();
        let mut transport = MockTransport::new();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &control,
            4,
            false,
            0,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(matches!(result.exit, SessionExit::Shutdown));
        assert!(!result.heard_from_primary);
    }

    #[test]
    fn loop_promote_drains_data_then_exits() {
        let (mut producer, mut consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let control = promoted_control();
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
            &control,
            4,
            false,
            0,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
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
        let control = control();
        let mut transport = MockTransport::new();
        transport.disconnect_after_data();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &control,
            4,
            false,
            0,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(matches!(result.exit, SessionExit::Disconnected));
    }

    #[test]
    fn loop_receives_data_and_acks() {
        let (mut producer, mut consumer) = ring(16);
        // Journal cursor at u64::MAX so pending acks are immediately durable.
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let control = control();
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
            &control,
            4,
            false,
            9,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(matches!(result.exit, SessionExit::Disconnected));
        assert!(result.heard_from_primary);

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
        let control = control();
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
            &control,
            4,
            false,
            0,
            initial,
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(result.heard_from_primary);
        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].request_seq, 0x42);
    }

    #[test]
    fn loop_full_ack_queue_still_acks_every_batch() {
        let (mut producer, mut consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let control = control();
        let mut transport = MockTransport::new();
        transport.simulate_in_flight = true;

        // Pipeline depth of 1 means the queue is full after a single
        // push, so the second batch takes the merge path. With
        // simulate_in_flight=true the first ack is still on the wire
        // when the second batch lands — the ack for it must not be
        // lost, only deferred to the next iteration.
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
            &control,
            1, // pipeline_depth=1 → PendingAckQueue cap=1
            false,
            0,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
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

    /// A burst larger than `MAX_SLOTS_PER_CYCLE` spans several parse
    /// calls, so the loop's read offset — not a memmove per cycle — is
    /// what tracks the parse position. Every slot must be published
    /// exactly once, in order, and the buffer must end up drained.
    #[test]
    fn loop_drains_a_burst_larger_than_one_cycle_without_losing_bytes() {
        let total = (MAX_SLOTS_PER_CYCLE * 2 + 37) as u64;
        let (mut producer, mut consumer) = ring(4096);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let control = control();
        let mut transport = MockTransport::new();

        // One chunk: the whole burst lands in recv_buf at once, which is
        // what a primary catching up after a stall looks like.
        let mut data = Vec::new();
        for i in 1..=total {
            append_input_batch_frame(&mut data, &[slot(i, i)]);
        }
        transport.push_data(data);
        transport.disconnect_after_data();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &control,
            16,
            false,
            0,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(matches!(result.exit, SessionExit::Disconnected));
        let seqs: Vec<u64> = drain(&mut consumer)
            .into_iter()
            .map(|s| s.sequence)
            .collect();
        assert_eq!(seqs.len(), total as usize, "every slot published once");
        assert_eq!(seqs, (1..=total).collect::<Vec<_>>(), "and in order");
        assert_eq!(
            transport.sent_acks.last().map(|a| a.in_memory_sequence),
            Some(total)
        );
    }

    /// A stalled local fsync must never stop the receiver. With
    /// `pipeline_depth = 1` the pending-ack queue is full after the
    /// first batch and the journal cursor never moves, which is exactly
    /// the state that used to park the loop in `pop_oldest_blocking` —
    /// freezing the *in-memory* ack track the primary's `disk+ram` gate
    /// runs on, i.e. putting the replica's disk tail on the client
    /// critical path.
    #[test]
    fn loop_keeps_receiving_while_the_journal_cursor_is_frozen() {
        let (mut producer, mut consumer) = ring(16);
        // Frozen at 0: nothing this session publishes ever becomes durable.
        let cursor = journal_cursor(0);
        let shutdown = AtomicBool::new(false);
        let control = control();
        let mut transport = MockTransport::new();
        for i in 1..=4u64 {
            let mut data = Vec::new();
            append_input_batch_frame(&mut data, &[slot(i, i)]);
            transport.push_data(data);
        }
        transport.disconnect_after_data();

        std::thread::scope(|s| {
            let reader = s.spawn(|| {
                // Collect what the receiver publishes, then release the
                // cursor: the *exit* drain does legitimately wait for
                // durability before its final ack.
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                let mut seen: Vec<u64> = Vec::new();
                while seen.len() < 4 && std::time::Instant::now() < deadline {
                    seen.extend(drain(&mut consumer).into_iter().map(|sl| sl.sequence));
                    std::hint::spin_loop();
                }
                cursor.get().store(u64::MAX, Ordering::Release);
                seen
            });

            let result = streaming_loop::<MockTransport, TestEvent>(
                &mut transport,
                &mut producer,
                &cursor,
                &shutdown,
                &control,
                1, // cap 1 → full after the first batch
                false,
                0,
                Vec::new(),
                None,
                &no_marks(),
                &AtomicBool::new(false),
            );

            assert!(matches!(result.exit, SessionExit::Disconnected));
            assert_eq!(
                reader.join().expect("reader thread"),
                vec![1, 2, 3, 4],
                "every batch must be published while the cursor is frozen"
            );
        });

        assert!(
            transport
                .sent_acks
                .iter()
                .any(|a| a.acked_sequence == 0 && a.in_memory_sequence > 0),
            "in-memory acks must keep flowing while nothing is durable: {:?}",
            transport.sent_acks
        );
    }

    #[test]
    fn loop_fatal_on_oversize_frame() {
        let (mut producer, _consumer) = ring(16);
        let cursor = journal_cursor(0);
        let shutdown = AtomicBool::new(false);
        let control = control();
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
            &control,
            4,
            false,
            0,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(matches!(result.exit, SessionExit::Fatal(_)));
    }

    #[test]
    fn loop_final_ack_on_shutdown_includes_durable_sequence() {
        let (mut producer, _consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let control = control();
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
                &control,
                4,
                false,
                41,
                Vec::new(),
                None,
                &no_marks(),
                &AtomicBool::new(false),
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
        let control = control();
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
            &control,
            4,
            false,
            0,
            Vec::new(),
            Some(&utilization),
            &no_marks(),
            &AtomicBool::new(false),
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
        encode_heartbeat(99, 1, &mut buf);
        append_input_batch_frame(&mut buf, &[slot(9, 0xA3)]);

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            5,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(outcome.frame_err.is_none(), "no fatal exit");
        assert_eq!(outcome.consumed, buf.len(), "every byte processed");
        assert!(outcome.any_published);
        assert!(outcome.heard_from_primary);
        assert_eq!(outcome.accum_end_sequence, 9);
        assert_eq!(
            outcome.observed_ack_policy,
            Some(1),
            "the heartbeat's ack policy must surface for the control-plane gauge"
        );

        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 4);
        let ids: Vec<u64> = slots.iter().map(|s| s.request_seq).collect();
        assert_eq!(ids, vec![0xA0, 0xA1, 0xA2, 0xA3]);
    }

    /// Each `InputBatch` frame gets its own pending-ack entry, so the
    /// first frame's persisted ack is released by the first frame's
    /// fsync instead of waiting on the last frame in the cycle.
    #[test]
    fn streaming_records_one_ack_target_per_frame() {
        let (mut producer, _consumer) = ring(16);
        let mut slot_buf = Vec::new();
        let mut buf = Vec::new();
        // Three frames of two slots — fresh ring, so slot indices 0..5
        // and per-frame targets 2, 4, 6.
        for f in 0..3u64 {
            append_input_batch_frame(
                &mut buf,
                &[slot(f * 2 + 1, f * 2 + 1), slot(f * 2 + 2, f * 2 + 2)],
            );
        }

        let (outcome, mut q) = stream_frames(
            &buf,
            &mut producer,
            0,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );
        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.accum_end_sequence, 6);

        // A single whole-cycle entry would hold everything back to
        // cursor 6; per-frame entries release as the journal walks.
        assert_eq!(q.pop_ready(&journal_cursor(2)), Some(2));
        assert_eq!(q.pop_ready(&journal_cursor(4)), Some(4));
        assert_eq!(q.pop_ready(&journal_cursor(6)), Some(6));
    }

    /// A long drain must not lock the receiver out of its ack flush: the
    /// call stops at `MAX_SLOTS_PER_CYCLE` and leaves the rest in the
    /// buffer for the next iteration.
    #[test]
    fn streaming_stops_at_the_slot_cap_and_leaves_the_remainder() {
        let total = MAX_SLOTS_PER_CYCLE + 100;
        let (mut producer, _consumer) = ring(2048);
        let mut slot_buf = Vec::new();
        let mut buf = Vec::new();
        for i in 1..=total as u64 {
            append_input_batch_frame(&mut buf, &[slot(i, i)]);
        }

        let (first, _acks) = stream_frames(
            &buf,
            &mut producer,
            0,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );
        assert!(first.frame_err.is_none());
        assert_eq!(
            first.accum_end_sequence, MAX_SLOTS_PER_CYCLE as u64,
            "stops at the cap"
        );
        assert!(first.consumed < buf.len(), "remainder stays buffered");

        // The caller resumes from where it stopped — no byte parsed
        // twice, none skipped.
        let (second, _acks) = stream_frames(
            &buf[first.consumed..],
            &mut producer,
            first.accum_end_sequence,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );
        assert!(second.frame_err.is_none());
        assert_eq!(second.accum_end_sequence, total as u64);
        assert_eq!(first.consumed + second.consumed, buf.len());
    }

    /// The ack target must live in the journal cursor's space. The
    /// journal cursor is next-to-consume: `cursor == i` means ring slots
    /// `< i` are durable and slot `i` is NOT. `push` returns the slot
    /// index, so a frame whose last slot lands at index `i` is durable
    /// only once the cursor reaches `i + 1`. Releasing at `i` acked the
    /// primary for a sequence still sitting un-fsynced in the input ring
    /// — one event of overstated durability, reachable whenever a
    /// journal batch ends exactly at `i` (ring wrap, batch-size
    /// truncation, mid-batch mark barrier).
    #[test]
    fn streaming_ack_target_is_one_past_the_last_slot_index() {
        let (mut producer, _consumer) = ring(16);
        let mut slot_buf = Vec::new();
        let mut buf = Vec::new();
        // Fresh ring: these land at slot indices 0 and 1.
        append_input_batch_frame(&mut buf, &[slot(10, 0xA0), slot(11, 0xA1)]);

        let (outcome, mut q) = stream_frames(
            &buf,
            &mut producer,
            9,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );
        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.accum_end_sequence, 11);

        // Cursor at 1: slot 0 durable, slot 1 (seq 11) is not.
        assert_eq!(
            q.pop_ready(&journal_cursor(1)),
            None,
            "seq 11 sits at slot index 1, which cursor 1 has not covered"
        );
        // Cursor at 2: both slots durable — ack through 11.
        assert_eq!(q.pop_ready(&journal_cursor(2)), Some(11));
    }

    /// Same contract on the promotion drain path.
    #[test]
    fn drain_ack_target_is_one_past_the_last_slot_index() {
        let (mut producer, _consumer) = ring(16);
        let mut slot_buf = Vec::new();
        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(1, 0x01)]);

        let outcome = process_drain_frames::<TestEvent>(
            &buf,
            &mut producer,
            0,
            &mut slot_buf,
            &AtomicBool::new(false),
        );
        assert!(outcome.any_published);

        let mut q = crate::replication::PendingAckQueue::new(4);
        q.push(outcome.last_target, outcome.accum_end_sequence);
        assert_eq!(
            q.pop_ready(&journal_cursor(0)),
            None,
            "slot index 0 is not durable at cursor 0"
        );
        assert_eq!(q.pop_ready(&journal_cursor(1)), Some(1));
    }

    /// A Rotate frame arriving exactly at the stream position is queued
    /// for the journal stage; the slots around it flow through
    /// untouched.
    #[test]
    fn streaming_rotate_at_exact_boundary_is_queued() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();
        let rotations = no_marks();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(6, 0xE0)]);
        melin_transport_core::replication::protocol::encode_rotate(6, &[0x66; 32], &mut buf);
        append_input_batch_frame(&mut buf, &[slot(7, 0xE1)]);

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            5,
            &mut slot_buf,
            &rotations,
            &AtomicBool::new(false),
        );

        assert!(outcome.frame_err.is_none(), "rotate at boundary is valid");
        assert_eq!(outcome.accum_end_sequence, 7);
        assert_eq!(drain(&mut consumer).len(), 2);

        let queued: Vec<_> = rotations.lock().unwrap().iter().copied().collect();
        assert_eq!(queued.len(), 1);
        match queued[0] {
            StreamMark::Rotate(r) => {
                assert_eq!(r.boundary_seq, 6);
                assert_eq!(r.tail_hash, [0x66; 32]);
            }
            other => panic!("expected a Rotate mark, got {other:?}"),
        }
    }

    /// A ChainCheck at the exact stream position is queued as a stream
    /// mark, with the same position rules as Rotate.
    #[test]
    fn streaming_chain_check_at_exact_position_is_queued() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();
        let marks = no_marks();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(6, 0xE0)]);
        melin_transport_core::replication::protocol::encode_chain_check(6, &[0x77; 32], &mut buf);

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            5,
            &mut slot_buf,
            &marks,
            &AtomicBool::new(false),
        );

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.accum_end_sequence, 6);
        assert_eq!(drain(&mut consumer).len(), 1);

        let queued: Vec<_> = marks.lock().unwrap().iter().copied().collect();
        assert_eq!(queued.len(), 1);
        match queued[0] {
            StreamMark::ChainCheck {
                sequence,
                chain_hash,
            } => {
                assert_eq!(sequence, 6);
                assert_eq!(chain_hash, [0x77; 32]);
            }
            other => panic!("expected a ChainCheck mark, got {other:?}"),
        }
    }

    /// A Rotate announcing an already-covered boundary (handoff overlap
    /// re-delivery) is dropped — queuing it after later slots would trip
    /// the journal stage's ordering invariant.
    #[test]
    fn streaming_stale_rotate_is_dropped() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();
        let rotations = no_marks();

        let mut buf = Vec::new();
        melin_transport_core::replication::protocol::encode_rotate(3, &[0x33; 32], &mut buf);
        append_input_batch_frame(&mut buf, &[slot(6, 0xE0)]);

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            5,
            &mut slot_buf,
            &rotations,
            &AtomicBool::new(false),
        );

        assert!(outcome.frame_err.is_none(), "stale rotate is not fatal");
        assert_eq!(outcome.accum_end_sequence, 6);
        assert_eq!(drain(&mut consumer).len(), 1);
        assert!(rotations.lock().unwrap().is_empty(), "stale rotate dropped");
    }

    /// A Rotate ahead of the stream position implies missing entries —
    /// same contiguity-break contract as a slot-sequence gap: the
    /// session ends as a `SequenceGap` (reconnect), not a `Fatal`.
    #[test]
    fn streaming_rotate_ahead_of_stream_is_a_sequence_gap() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();
        let rotations = no_marks();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(6, 0xE0)]);
        melin_transport_core::replication::protocol::encode_rotate(9, &[0x99; 32], &mut buf);

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            5,
            &mut slot_buf,
            &rotations,
            &AtomicBool::new(false),
        );

        assert!(
            matches!(outcome.frame_err, Some(FrameError::SequenceGap(_))),
            "rotate past stream position => sequence gap"
        );
        assert_eq!(outcome.accum_end_sequence, 6, "prefix still committed");
        assert_eq!(drain(&mut consumer).len(), 1);
        assert!(rotations.lock().unwrap().is_empty());
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

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            6,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

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

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            2,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

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

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            0,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

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
        encode_heartbeat(42, 1, &mut buf);
        encode_heartbeat(43, 1, &mut buf);

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            100,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.consumed, buf.len());
        assert!(!outcome.any_published);
        // Heartbeats don't advance the stream, but they ARE the primary
        // speaking — the liveness evidence the backoff reset keys on.
        assert!(outcome.heard_from_primary);
        assert_eq!(outcome.accum_end_sequence, 100);
        assert!(drain(&mut consumer).is_empty());
    }

    #[test]
    fn streaming_empty_buffer_is_a_noop() {
        let (mut producer, mut consumer) = ring(4);
        let mut slot_buf = Vec::new();

        let (outcome, _acks) = stream_frames(
            &[],
            &mut producer,
            77,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.consumed, 0);
        assert!(!outcome.any_published);
        assert!(!outcome.heard_from_primary);
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
    //   seq >  accum + 1  → SequenceGap — a gap can never be repaired
    //                       downstream; acking past it overstates
    //                       durability and corrupts the journal lineage.
    //                       The session ends and reconnects from the
    //                       durable position.
    // ---------------------------------------------------------------

    #[test]
    fn streaming_rejects_sequence_gap_across_frames() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(10, 0xA0), slot(11, 0xA1)]);
        // 11 → 14: entries 12..=13 are missing from the wire.
        append_input_batch_frame(&mut buf, &[slot(14, 0xA2), slot(15, 0xA3)]);

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            9,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(
            matches!(outcome.frame_err, Some(FrameError::SequenceGap(_))),
            "a sequence gap (11 → 14) must end the session as a SequenceGap, \
             not be silently accepted"
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

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            9,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(
            matches!(outcome.frame_err, Some(FrameError::SequenceGap(_))),
            "an intra-frame sequence gap (11 → 13) must end the session as a SequenceGap"
        );
        let published: Vec<u64> = drain(&mut consumer).iter().map(|s| s.sequence).collect();
        assert_eq!(
            published,
            vec![10, 11],
            "the contiguous prefix is committed; the slot past the gap is not \
             (mirrors the oversize-frame semantics: commit prior progress, then stop)"
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

        let (outcome, _acks) = stream_frames(
            &buf,
            &mut producer,
            9,
            &mut slot_buf,
            &no_marks(),
            &AtomicBool::new(false),
        );

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
        let outcome = process_drain_frames::<TestEvent>(
            &buf,
            &mut producer,
            9,
            &mut slot_buf,
            &AtomicBool::new(false),
        );

        let published: Vec<u64> = drain(&mut consumer).iter().map(|s| s.sequence).collect();
        assert_eq!(
            published,
            vec![10, 11],
            "promotion drain must not publish slots past a sequence gap"
        );
        assert_eq!(outcome.accum_end_sequence, 11);
    }

    /// Loop-level pin of the durability contract: after a gapped wire
    /// stream, the session must end with `StreamGap` (reconnect from
    /// the durable position — the local journal is intact) and no ack
    /// — persisted or in-memory — may ever name a sequence past the
    /// last contiguous slot. In the bench failure the replica kept
    /// acking for the rest of the 60s run with a 212-entry hole behind
    /// its cursors, overstating durability to the primary's response
    /// gate.
    #[test]
    fn streaming_loop_sequence_gap_ends_session_and_never_acks_past() {
        let (mut producer, mut consumer) = ring(16);
        // Journal cursor at u64::MAX so pending acks are immediately
        // durable — ack content is what's under test, not flush timing.
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let control = control();
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
            &control,
            4,
            false,
            0,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(
            matches!(result.exit, SessionExit::StreamGap(_)),
            "a gapped stream must end the session as a StreamGap (reconnect), \
             not a clean exit and not a process-fatal error"
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

    /// The contiguity gate must anchor at the session's resume point, not
    /// at zero. After a snapshot transfer the receiver passes
    /// `initial_sequence = snap_sequence` (and on journal catch-up, the
    /// handshake `last_sequence`); `streaming_loop` must seed
    /// `accum_end_sequence` from it so the *first* live slot is required
    /// to be `initial_sequence + 1`. Without the seed the gate would
    /// start at 0, silently accept a first slot thousands past the
    /// snapshot, and journal a hole exactly where the snapshot→stream
    /// boundary lives.
    ///
    /// Here the replica resumes at 100 (e.g. a snapshot at sequence 100)
    /// and the first frame jumps to 102 — sequence 101 is missing. The
    /// session must end as a `StreamGap` with nothing published and no
    /// ack past 100.
    #[test]
    fn streaming_loop_anchors_contiguity_at_the_resume_point() {
        let (mut producer, mut consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let control = control();
        let mut transport = MockTransport::new();

        let mut data = Vec::new();
        // Resume point is 100; first slot is 102 → 101 missing.
        append_input_batch_frame(&mut data, &[slot(102, 0x66), slot(103, 0x67)]);
        transport.push_data(data);
        transport.disconnect_after_data();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &control,
            4,
            false,
            100, // initial_sequence — the post-snapshot resume point
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(
            matches!(result.exit, SessionExit::StreamGap(_)),
            "a first slot past resume_point+1 must end the session as a gap — \
             the gate is not anchored at the resume point"
        );
        assert!(
            drain(&mut consumer).is_empty(),
            "nothing may be published when the first slot is already a gap"
        );
        for ack in &transport.sent_acks {
            assert!(
                ack.acked_sequence <= 100 && ack.in_memory_sequence <= 100,
                "ack ({}, {}) names a sequence past the resume point 100",
                ack.acked_sequence,
                ack.in_memory_sequence,
            );
        }
    }

    /// Complement of the above: a stream that resumes exactly one past
    /// the resume point is the contiguous happy path — accepted and
    /// acked. Proves the seeding doesn't reject a legitimate
    /// post-snapshot stream (off-by-one in the anchor would make every
    /// snapshot resume fail).
    #[test]
    fn streaming_loop_accepts_contiguous_stream_from_resume_point() {
        let (mut producer, mut consumer) = ring(16);
        let cursor = journal_cursor(u64::MAX);
        let shutdown = AtomicBool::new(false);
        let control = control();
        let mut transport = MockTransport::new();

        let mut data = Vec::new();
        // Resume point 100; stream continues at 101, 102.
        append_input_batch_frame(&mut data, &[slot(101, 0x70), slot(102, 0x71)]);
        transport.push_data(data);
        transport.disconnect_after_data();

        let result = streaming_loop::<MockTransport, TestEvent>(
            &mut transport,
            &mut producer,
            &cursor,
            &shutdown,
            &control,
            4,
            false,
            100,
            Vec::new(),
            None,
            &no_marks(),
            &AtomicBool::new(false),
        );

        assert!(
            matches!(result.exit, SessionExit::Disconnected),
            "a contiguous post-resume stream must not be fatal: {:?}",
            matches!(result.exit, SessionExit::Fatal(_))
        );
        let published: Vec<u64> = drain(&mut consumer).iter().map(|s| s.sequence).collect();
        assert_eq!(published, vec![101, 102]);
        let last = transport
            .sent_acks
            .last()
            .expect("a contiguous stream must produce an ack");
        assert_eq!(
            last.in_memory_sequence, 102,
            "ack must advance to the last contiguous slot from the resume point"
        );
    }

    #[test]
    fn drain_skips_non_input_frames_and_publishes_every_input_batch() {
        let (mut producer, mut consumer) = ring(16);
        let mut slot_buf = Vec::new();

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(20, 0xE0), slot(21, 0xE1)]);
        encode_heartbeat(999, 1, &mut buf);
        append_input_batch_frame(&mut buf, &[slot(22, 0xE2)]);

        let outcome = process_drain_frames::<TestEvent>(
            &buf,
            &mut producer,
            19,
            &mut slot_buf,
            &AtomicBool::new(false),
        );

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

        let outcome = process_drain_frames::<TestEvent>(
            &buf,
            &mut producer,
            49,
            &mut slot_buf,
            &AtomicBool::new(false),
        );

        assert_eq!(outcome.consumed, complete_len);
        assert_eq!(outcome.accum_end_sequence, 50);
        assert_eq!(drain(&mut consumer).len(), 1);
    }

    #[test]
    fn drain_empty_buffer_is_a_noop() {
        let (mut producer, mut consumer) = ring(4);
        let mut slot_buf = Vec::new();

        let outcome = process_drain_frames::<TestEvent>(
            &[],
            &mut producer,
            55,
            &mut slot_buf,
            &AtomicBool::new(false),
        );

        assert_eq!(outcome.consumed, 0);
        assert!(!outcome.any_published);
        assert_eq!(outcome.accum_end_sequence, 55);
        assert!(drain(&mut consumer).is_empty());
    }

    // -----------------------------------------------------------------
    // Chunked-body transfer (snapshot / segment seed), driven by a
    // scripted ControlFrameSource — covers the body-receive logic (chunk
    // write, length + CRC verification, tmp cleanup) that BOTH receivers
    // now share, without a live transport.
    // -----------------------------------------------------------------
    mod chunked_body {
        use super::super::{ControlFrameSource, receive_chunked_body};
        use melin_transport_core::replication::protocol::{
            encode_snapshot_chunk, encode_snapshot_end, encode_stream_start,
        };
        use std::collections::VecDeque;

        /// Yields pre-built frame payloads in order, then errors
        /// (modelling a disconnect) once drained.
        struct Scripted {
            frames: VecDeque<Vec<u8>>,
        }

        impl ControlFrameSource for Scripted {
            fn next_frame(
                &mut self,
                _max_size: usize,
            ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
                self.frames
                    .pop_front()
                    .ok_or_else(|| "disconnected during transfer".into())
            }
        }

        /// `next_frame` yields payloads, not framed bytes — strip the
        /// 4-byte length prefix an `encode_*` helper writes.
        fn payload(mut encode: impl FnMut(&mut Vec<u8>)) -> Vec<u8> {
            let mut buf = Vec::new();
            encode(&mut buf);
            buf[4..].to_vec()
        }

        fn chunk(data: &[u8]) -> Vec<u8> {
            payload(|b| encode_snapshot_chunk(data, b))
        }

        fn end(crc: u32) -> Vec<u8> {
            payload(|b| encode_snapshot_end(crc, b))
        }

        fn source(frames: Vec<Vec<u8>>) -> Scripted {
            Scripted {
                frames: frames.into(),
            }
        }

        #[test]
        fn happy_path_writes_body_and_verifies() {
            let dir = tempfile::tempdir().unwrap();
            let tmp = dir.path().join("body.tmp");
            let body = b"hello, replica seed bytes";
            let crc = crc32c::crc32c(body);
            let mut src = source(vec![chunk(&body[..10]), chunk(&body[10..]), end(crc)]);

            receive_chunked_body(&mut src, &tmp, body.len() as u64, "snapshot").unwrap();
            assert_eq!(std::fs::read(&tmp).unwrap(), body);
        }

        #[test]
        fn length_mismatch_errs_and_removes_tmp() {
            let dir = tempfile::tempdir().unwrap();
            let tmp = dir.path().join("body.tmp");
            let body = b"twelve bytes";
            let crc = crc32c::crc32c(body);
            let mut src = source(vec![chunk(body), end(crc)]);

            // Claim one more byte than we sent.
            let err = receive_chunked_body(&mut src, &tmp, body.len() as u64 + 1, "snapshot")
                .unwrap_err();
            assert!(err.to_string().contains("length mismatch"), "{err}");
            assert!(!tmp.exists(), "partial tmp must be removed");
        }

        #[test]
        fn crc_mismatch_errs_and_removes_tmp() {
            let dir = tempfile::tempdir().unwrap();
            let tmp = dir.path().join("body.tmp");
            let body = b"twelve bytes";
            let mut src = source(vec![chunk(body), end(0xDEAD_BEEF)]);

            let err =
                receive_chunked_body(&mut src, &tmp, body.len() as u64, "snapshot").unwrap_err();
            assert!(err.to_string().contains("CRC mismatch"), "{err}");
            assert!(!tmp.exists());
        }

        #[test]
        fn disconnect_before_end_errs_and_removes_tmp() {
            let dir = tempfile::tempdir().unwrap();
            let tmp = dir.path().join("body.tmp");
            // A chunk arrives, then the source drains (disconnect) before
            // SnapshotEnd.
            let mut src = source(vec![chunk(b"partial")]);

            let err = receive_chunked_body(&mut src, &tmp, 7, "segment seed").unwrap_err();
            assert!(err.to_string().contains("disconnected"), "{err}");
            assert!(!tmp.exists());
        }

        #[test]
        fn unexpected_frame_errs_and_removes_tmp() {
            let dir = tempfile::tempdir().unwrap();
            let tmp = dir.path().join("body.tmp");
            // A StreamStart where a chunk/end belongs.
            let stray = payload(|b| encode_stream_start(0, 1, [0u8; 32], 0, 1, b));
            let mut src = source(vec![stray]);

            let err = receive_chunked_body(&mut src, &tmp, 4, "snapshot").unwrap_err();
            assert!(err.to_string().contains("SnapshotChunk/End"), "{err}");
            assert!(!tmp.exists());
        }
    }
}
