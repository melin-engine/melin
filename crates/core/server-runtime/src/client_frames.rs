//! Transport-agnostic client frame processing.
//!
//! Both the kernel (io_uring) and DPDK client readers parse the same
//! length-prefixed wire format, decode through the same
//! [`RequestDecoder`], and publish [`InputSlot`]s to the same disruptor
//! ring with identical batching semantics. This module extracts that
//! shared logic into [`process_client_frames`] so both backends call a
//! single implementation.

use tracing::debug;

use melin_app::AppEvent;
use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder};
use melin_journal::JournalEvent;
use melin_pipeline::ring;
use melin_transport_core::pipeline::InputSlot;
use melin_transport_core::trace::{MonoTraceInstant, mono_trace_ns};

/// Maximum frame payload size (matches `BlockingFrameReader`).
pub(crate) const MAX_FRAME_SIZE: usize = 1024;

/// Outcome of [`process_client_frames`].
pub(crate) enum FrameAction {
    /// All complete frames processed. Any partial trailing bytes remain
    /// in `parse_buf` for the next recv cycle.
    Continue,
    /// An oversized frame was encountered. Prior frames were committed.
    /// Caller should drop the connection.
    Disconnect,
    /// The pipeline ring is full. Prior frames were committed. The frame
    /// that triggered full was consumed from `parse_buf` (bytes dropped).
    /// Caller should signal backpressure (e.g. ServerBusy).
    PipelineFull,
}

/// Extract, decode, and publish client request frames from `parse_buf`.
///
/// Processes every complete length-prefixed frame, decodes each through
/// `decoder`, and publishes permitted events to the input ring under
/// batched commits (cap: 16 events per commit to bound consumer
/// visibility delay). Compacts `parse_buf` on return.
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
pub(crate) fn process_client_frames<E: AppEvent>(
    parse_buf: &mut Vec<u8>,
    connection_id: u64,
    key_hash: u64,
    permission: Permission,
    producer: &mut ring::Producer<InputSlot<E>>,
    decoder: &dyn RequestDecoder<Event = E>,
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
    let mut batch = producer.batch();

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

        let (seq, event) = match decoder.decode(frame, permission) {
            Decoded::Filter => continue,
            Decoded::PermissionDenied(reason) => {
                debug!(connection_id, reason, "permission denied, dropping request");
                continue;
            }
            Decoded::DecodeError(reason) => {
                debug!(connection_id, reason, "decode error");
                continue;
            }
            Decoded::Permitted { request_seq, event } => (request_seq, event),
        };

        let ts = if event.is_query() { 0 } else { batch_wall_ns };
        let event = JournalEvent::App(event);

        #[cfg(feature = "latency-trace")]
        let pre_publish = mono_trace_ns();
        #[allow(clippy::let_unit_value)]
        let publish_ts = mono_trace_ns();

        let push_result = batch.try_push_with(|slot| {
            slot.connection_id = connection_id;
            slot.key_hash = key_hash;
            slot.request_seq = seq;
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
            batch.commit();
            batch = producer.batch();
        }
    }

    batch.commit();

    // Compact: shift remaining bytes to the front.
    if cursor > 0 {
        let remaining = parse_buf.len() - cursor;
        parse_buf.copy_within(cursor.., 0);
        parse_buf.truncate(remaining);
    }

    result
}
