//! Transport-agnostic client frame processing.
//!
//! Both the kernel (io_uring) and DPDK client readers parse the same
//! length-prefixed wire format (split by the wire protocol's zero-copy
//! splitter, the one `melin-client` frames with too), decode through the
//! same [`ErasedDecoder`], and publish [`InputSlot`]s to the same
//! disruptor ring with identical batching semantics. This module extracts that
//! shared logic into [`process_client_frames`] so both backends call a
//! single implementation.

use tracing::debug;

use melin_app::auth::{ClientRole, RoleId};
use melin_app::decoder::{Decoded, ErasedDecoder};
use melin_app::{AppEvent, Application};
use melin_pipeline::ring;
use melin_transport_core::pipeline::{ClientOrigin, InputSlot, RequestId};
use melin_transport_core::trace::MonoTraceInstant;
#[cfg(any(feature = "latency-trace", feature = "tick-to-trade"))]
use melin_transport_core::trace::mono_trace_ns;
use melin_wire_protocol::control::RefusalKind;
use melin_wire_protocol::framing::{split_frame_limited, split_request};

use crate::halt::{EarlyKind, EarlyReply, EarlyReplySender, HaltGate, Verdict};

/// Bound on one client request frame, after the 4-byte length prefix:
/// the wire protocol's, so a client library and a node agree on it by
/// construction.
///
/// A frame declaring more is not read: both readers treat it as a
/// protocol violation and drop the connection. Public for a program that
/// reads client frames itself; an application sizes its requests against
/// [`MAX_REQUEST_BODY`].
pub const MAX_FRAME_SIZE: usize = melin_wire_protocol::blocking::MAX_FRAME_SIZE;

/// Bound on one request body — what an application's `RequestDecoder`
/// is handed after the runtime has read the tag and the request's ID.
/// Public so an application can check its widest request against it at
/// compile time; see the re-export in the crate root. The wire
/// protocol's, the bound a client's request framing enforces, so the two
/// sides agree on it by construction.
pub const MAX_REQUEST_BODY: usize = melin_wire_protocol::framing::MAX_REQUEST_BODY;

/// Outcome of [`process_client_frames`].
pub(crate) enum FrameAction {
    /// Every complete frame was processed, or processing stopped at a full
    /// input ring after the request that met it was answered `ServerBusy`.
    /// Whatever was not processed (a partial trailing frame, the frames
    /// behind a shed request) remains in `parse_buf` for the next receive.
    Continue,
    /// The connection is to be closed (the frames before the cause were
    /// committed): an oversized frame, a frame that is not a request (an
    /// empty frame, another tag, a request too short for its ID), a full
    /// early-reply queue, or a superseded node that takes nothing more
    /// from anyone.
    Disconnect,
}

/// Extract, decode, and publish client request frames from `parse_buf`.
///
/// Processes every complete length-prefixed frame: each must be a request
/// (`TAG_REQUEST` and the client's ID), decoded through `decoder`, and
/// published to the input ring, the ID in its slot, under batched commits
/// (cap: 16 events per commit to bound consumer visibility delay).
/// Compacts `parse_buf` on return.
///
/// Every request the reader does not publish is answered through
/// `early_replies`, under its ID (see [`crate::halt`]): a filtered request
/// with an empty batch, a refused one with the decoder's reason, a write
/// refused while `halt` refuses writes with the application's rejection
/// (queries are published either way), and the request that finds the
/// input ring full with `ServerBusy`, after which processing stops for
/// this call. The halt is sampled once per call, so one receive is judged
/// as a whole, and the writes it refused are counted into `halt` once, at
/// the end. On a superseded node nothing is read: the call returns
/// [`FrameAction::Disconnect`] before the first frame.
///
/// Every early reply the call queues is visible to the response stage by
/// the time it returns, whatever it returns: the queue is flushed once on
/// the way out, around every return path. An early reply waits for
/// nothing published, so where that flush falls relative to the input
/// ring's commits does not matter.
///
/// A full early-reply queue closes the connection whose answer has
/// nowhere to go ([`FrameAction::Disconnect`]).
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
    role: ClientRole<RoleId>,
    producer: &mut ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn ErasedDecoder<A::Event>,
    halt: &HaltGate,
    early_replies: &mut EarlyReplySender<A::Report>,
    batch_wall_ns: u64,
    recv_ts: MonoTraceInstant,
    #[cfg(feature = "latency-trace")] publish_rec: &mut melin_transport_core::trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) -> FrameAction {
    let action = process_frames_unflushed::<A>(
        parse_buf,
        connection_id,
        key_hash,
        role,
        producer,
        decoder,
        halt,
        early_replies,
        batch_wall_ns,
        recv_ts,
        #[cfg(feature = "latency-trace")]
        publish_rec,
        #[cfg(feature = "tick-to-trade")]
        ingest_rec,
    );
    // The one flush of the early replies the call staged, whatever the
    // outcome: here, outside the body, so no return path inside it can
    // leave a reply staged and unseen.
    early_replies.flush();
    action
}

/// [`process_client_frames`] without its closing flush of the early-reply
/// queue: every reply it queues is only staged.
#[allow(clippy::too_many_arguments)]
#[inline]
fn process_frames_unflushed<A: Application>(
    parse_buf: &mut Vec<u8>,
    connection_id: u64,
    key_hash: u64,
    role: ClientRole<RoleId>,
    producer: &mut ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn ErasedDecoder<A::Event>,
    halt: &HaltGate,
    early_replies: &mut EarlyReplySender<A::Report>,
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
    // rather than with an atomic add each.
    let mut refused: u64 = 0;

    loop {
        // An oversized prefix is refused as soon as its 4 bytes are in,
        // without waiting for a payload that would never be read.
        let frame = match split_frame_limited(&parse_buf[cursor..], MAX_FRAME_SIZE) {
            Ok(Some(frame)) => frame,
            // A partial frame stays in `parse_buf` for the next recv.
            Ok(None) => break,
            Err(too_large) => {
                debug!(
                    connection_id,
                    frame_len = too_large.declared,
                    "frame too large, dropping connection"
                );
                result = FrameAction::Disconnect;
                break;
            }
        };
        cursor += frame.consumed();

        // Only a request may come after the handshake: a client sends none
        // of the protocol's own frames, and the tag and the ID are read
        // here, so no decoder ever sees them. Anything else is a client
        // speaking another protocol, told at once by the close.
        let (request_id, body) = match split_request(frame.payload) {
            Ok(request) => request,
            Err(not_a_request) => {
                debug!(
                    connection_id,
                    %not_a_request,
                    "not a request, dropping connection"
                );
                result = FrameAction::Disconnect;
                break;
            }
        };

        // An answer decided here, or the event to publish.
        let early = match decoder.decode_erased(body, role) {
            Decoded::Filter => EarlyKind::Filtered,
            Decoded::PermissionDenied(reason) => {
                debug!(connection_id, reason, "permission denied, refusing request");
                EarlyKind::Refused {
                    kind: RefusalKind::PermissionDenied,
                    reason,
                }
            }
            Decoded::DecodeError(reason) => {
                debug!(connection_id, reason, "decode error, refusing request");
                EarlyKind::Refused {
                    kind: RefusalKind::Malformed,
                    reason,
                }
            }
            Decoded::Permitted(event) => match refusal_reason {
                Some(reason) if !event.is_query() => {
                    EarlyKind::Rejected(A::build_reject(&event, reason))
                }
                _ => {
                    if publish::<A>(
                        &mut batch,
                        ClientOrigin {
                            connection_id,
                            request_id,
                            key_hash,
                        },
                        event,
                        batch_wall_ns,
                        recv_ts,
                        #[cfg(feature = "latency-trace")]
                        publish_rec,
                        #[cfg(feature = "tick-to-trade")]
                        ingest_rec,
                    ) {
                        if batch.len() >= COMMIT_EVERY {
                            batch.commit();
                            batch = producer.batch();
                        }
                        continue;
                    }
                    // The input ring is full: this request is shed, and
                    // parsing stops; the frames behind it wait in
                    // `parse_buf` for the next receive.
                    debug!(connection_id, "input ring full, shedding request");
                    if !queue_early(early_replies, connection_id, request_id, EarlyKind::Busy) {
                        result = FrameAction::Disconnect;
                    }
                    break;
                }
            },
        };
        if !queue_early(early_replies, connection_id, request_id, early) {
            result = FrameAction::Disconnect;
            break;
        }
        // A halt refusal is counted once its answer is queued: a write the
        // connection was closed on instead was not refused, it was never
        // read.
        if matches!(early, EarlyKind::Rejected(_)) {
            refused += 1;
        }
    }

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

/// Stage the early reply `kind` for request `request_id`. `false` when the
/// queue is full: the caller closes the connection, since the answer has
/// nowhere to go.
fn queue_early<R: Copy>(
    early_replies: &mut EarlyReplySender<R>,
    connection_id: u64,
    request_id: RequestId,
    kind: EarlyKind<R>,
) -> bool {
    let queued = early_replies
        .try_send(EarlyReply {
            connection_id,
            request_id,
            kind,
        })
        .is_ok();
    if !queued {
        debug!(connection_id, "early-reply queue full, dropping connection");
    }
    queued
}

/// Publish one client event into `batch`. `false` when the input ring is
/// full and nothing was published.
#[allow(clippy::too_many_arguments)]
#[inline]
fn publish<A: Application>(
    batch: &mut ring::Batch<'_, InputSlot<A::Event>>,
    origin: ClientOrigin,
    event: A::Event,
    batch_wall_ns: u64,
    recv_ts: MonoTraceInstant,
    #[cfg(feature = "latency-trace")] publish_rec: &mut melin_transport_core::trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) -> bool {
    let ts = if event.is_query() { 0 } else { batch_wall_ns };

    #[cfg(feature = "latency-trace")]
    let pre_publish = mono_trace_ns();

    // The whole slot is rebuilt in place, through the one constructor
    // for a client slot, so no field keeps the previous occupant's
    // value.
    if batch
        .try_push_with(|slot| {
            *slot = InputSlot::client(origin, event, ts, recv_ts);
        })
        .is_err()
    {
        return false;
    }

    #[cfg(feature = "latency-trace")]
    {
        let publish_done = mono_trace_ns();
        publish_rec.record_elapsed(pre_publish, publish_done);
    }
    #[cfg(feature = "tick-to-trade")]
    ingest_rec.record_elapsed(recv_ts, mono_trace_ns());
    true
}
