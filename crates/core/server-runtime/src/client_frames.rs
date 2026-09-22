//! Transport-agnostic client frame processing.
//!
//! Both the kernel (io_uring) and DPDK client readers parse the same
//! length-prefixed wire format, decode through the same
//! [`RequestDecoder`], and publish [`InputSlot`]s to the same disruptor
//! ring with identical batching semantics. This module extracts that
//! shared logic into [`process_client_frames`] so both backends call a
//! single implementation.

use tracing::debug;

use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder};
use melin_app::{AppEvent, Application};
use melin_journal::JournalEvent;
use melin_pipeline::ring;
use melin_transport_core::pipeline::InputSlot;
use melin_transport_core::trace::{MonoTraceInstant, mono_trace_ns};

use crate::halt::{HaltGate, Refusal, RefusalSender, Verdict};

/// Bound on one client request frame, after the 4-byte length prefix:
/// the wire protocol's, so a client library and a node agree on it by
/// construction.
///
/// A frame declaring more is not read: both readers treat it as a
/// protocol violation and drop the connection. What an application's
/// `RequestDecoder` can be handed is therefore at most this many bytes,
/// request-sequence header and tag included. Public so an application
/// can check its widest request against it at compile time; see the
/// re-export in the crate root.
pub const MAX_FRAME_SIZE: usize = melin_wire_protocol::blocking::MAX_FRAME_SIZE;

/// Outcome of [`process_client_frames`].
pub(crate) enum FrameAction {
    /// All complete frames processed. Any partial trailing bytes remain
    /// in `parse_buf` for the next recv cycle.
    Continue,
    /// An oversized frame was encountered (prior frames were committed),
    /// or the node is superseded and takes nothing more from anyone.
    /// Caller should drop the connection.
    Disconnect,
    /// The pipeline ring — or, while halted, the refusal queue — is full.
    /// Prior frames were committed. The frame that triggered full was
    /// consumed from `parse_buf` (bytes dropped). Caller should signal
    /// backpressure (e.g. ServerBusy).
    PipelineFull,
}

/// Extract, decode, and publish client request frames from `parse_buf`.
///
/// Processes every complete length-prefixed frame, decodes each through
/// `decoder`, and publishes permitted events to the input ring under
/// batched commits (cap: 16 events per commit to bound consumer
/// visibility delay). Compacts `parse_buf` on return.
///
/// While `halt` refuses writes, a permitted write is not published: its
/// rejection goes to `refusals`, stamped with the input sequence it would
/// have taken (see [`crate::halt`]). Queries are published either way.
/// The halt is sampled once per call, so one receive is judged as a whole,
/// and the writes it refused are counted into `halt` once, at the end. On
/// a superseded node nothing is read: the call returns
/// [`FrameAction::Disconnect`] before the first frame.
///
/// Returns [`FrameAction`] so the caller can handle transport-specific
/// side effects (ServerBusy write, transport close, control events).
///
/// `recv_ts` is the trace timestamp the caller captured once, at the
/// moment the kernel handed it this recv's bytes (the io_uring CQE /
/// DPDK `recv_into_vec` site). Every slot published from `parse_buf`
/// is stamped with it, so the `reader: ingest` and `server e2e` stages
/// measure from true wire receipt — frame decode included — rather than
/// re-sampling per frame after decode (which excluded decode and drifted
/// forward for later frames in a multi-frame recv). `()` (zero-sized)
/// when `latency-trace` is disabled.
#[allow(clippy::too_many_arguments)]
pub(crate) fn process_client_frames<A: Application>(
    parse_buf: &mut Vec<u8>,
    connection_id: u64,
    key_hash: u64,
    permission: Permission,
    producer: &mut ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn RequestDecoder<Event = A::Event>,
    halt: &HaltGate,
    refusals: &mut RefusalSender<A::Report>,
    batch_wall_ns: u64,
    recv_ts: MonoTraceInstant,
    #[cfg(feature = "latency-trace")] publish_rec: &mut melin_transport_core::trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) -> FrameAction {
    let mut cursor = 0;
    let mut result = FrameAction::Continue;

    // Batch publishes into a single Release store on the input ring's
    // producer cursor. Bounded at COMMIT_EVERY to cap consumer-
    // visibility delay (see reader.rs for the measured rationale).
    const COMMIT_EVERY: u64 = 16;
    let refusal_reason = match halt.verdict() {
        Verdict::Take => None,
        Verdict::Refuse(reason) => Some(reason),
        Verdict::Close => {
            debug!(connection_id, "node superseded, closing connection");
            return FrameAction::Disconnect;
        }
    };
    let mut batch = producer.batch();
    // Writes refused in this call, counted into the gate once at the end
    // rather than with an atomic add each. Includes one shed for a full
    // refusal queue: the halt is what turned it away.
    let mut refused: u64 = 0;

    while cursor + 4 <= parse_buf.len() {
        let len_bytes: [u8; 4] = parse_buf[cursor..cursor + 4]
            .try_into()
            .expect("slice is exactly 4 bytes");
        let frame_len = u32::from_le_bytes(len_bytes) as usize;

        if frame_len > MAX_FRAME_SIZE {
            debug!(
                connection_id,
                frame_len, "frame too large, dropping connection"
            );
            result = FrameAction::Disconnect;
            break;
        }

        if cursor + 4 + frame_len > parse_buf.len() {
            break;
        }

        let frame = &parse_buf[cursor + 4..cursor + 4 + frame_len];
        cursor += 4 + frame_len;

        let event = match decoder.decode(frame, permission) {
            Decoded::Filter => continue,
            Decoded::PermissionDenied(reason) => {
                debug!(connection_id, reason, "permission denied, dropping request");
                continue;
            }
            Decoded::DecodeError(reason) => {
                debug!(connection_id, reason, "decode error");
                continue;
            }
            Decoded::Permitted(event) => event,
        };

        if let Some(reason) = refusal_reason
            && !event.is_query()
        {
            refused += 1;
            let refusal = Refusal {
                connection_id,
                input_seq: batch.next_sequence(),
                report: A::build_reject(&event, reason),
            };
            if refusals.try_send(refusal).is_err() {
                result = FrameAction::PipelineFull;
                break;
            }
            continue;
        }

        let ts = if event.is_query() { 0 } else { batch_wall_ns };
        let event = JournalEvent::App(event);

        #[cfg(feature = "latency-trace")]
        let pre_publish = mono_trace_ns();
        #[allow(clippy::let_unit_value)]
        let publish_ts = mono_trace_ns();

        let push_result = batch.try_push_with(|slot| {
            slot.connection_id = connection_id;
            slot.key_hash = key_hash;
            slot.sequence = 0;
            slot.timestamp_ns = ts;
            slot.event = event;
            slot.publish_ts = publish_ts;
            slot.recv_ts = recv_ts;
        });

        if push_result.is_err() {
            result = FrameAction::PipelineFull;
            break;
        }

        #[cfg(feature = "latency-trace")]
        {
            let publish_done = mono_trace_ns();
            publish_rec.record_elapsed(pre_publish, publish_done);
        }
        #[cfg(feature = "tick-to-trade")]
        ingest_rec.record_elapsed(recv_ts, mono_trace_ns());

        if batch.len() >= COMMIT_EVERY {
            refusals.flush();
            batch.commit();
            batch = producer.batch();
        }
    }

    // Refusals become visible before the events published after them, at
    // every commit. The other way round, the response stage could answer
    // such an event before it sees the refusal that came first, and send
    // the two replies out of order.
    refusals.flush();
    batch.commit();

    if refused > 0 {
        halt.record_refused(refused);
    }

    // Compact: shift remaining bytes to the front.
    if cursor > 0 {
        let remaining = parse_buf.len() - cursor;
        parse_buf.copy_within(cursor.., 0);
        parse_buf.truncate(remaining);
    }

    result
}
