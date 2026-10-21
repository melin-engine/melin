//! Transport-agnostic client frame processing.
//!
//! Both the kernel (io_uring) and DPDK client readers parse the same
//! length-prefixed wire format (split by the wire protocol's zero-copy
//! splitter, the one `melin-client` frames with too), decode through the
//! same [`ErasedDecoder`], and publish [`InputSlot`]s to the same
//! disruptor ring with identical batching semantics. This module extracts that
//! shared logic into [`process_client_frames`] so both backends call a
//! single implementation, over the per-connection input state both hold
//! ([`ClientIngress`]).
//!
//! # Shedding and parking
//!
//! No request is left unanswered, and none waits for bytes that may never
//! come. A request that finds the input ring full is answered
//! `ServerBusy`, and so is every complete frame already buffered behind
//! it, read only as far as its ID: the *shed tail*. Nothing in that tail
//! is published, so nothing a client sent behind a shed request in the
//! same burst is applied ahead of its resend. A trailing partial frame is
//! not part of the tail: it is judged once it is whole.
//!
//! An answer with nowhere to go (a full early-reply queue, which fills
//! only while the response stage is not draining it) *parks* the
//! connection instead ([`FrameAction::Park`]): processing stops before the
//! frame, which stays buffered with everything behind it, and the reader
//! stops reading the connection and retries the buffer later. A park in
//! the middle of a shed tail resumes shedding where it stopped, since the
//! tail's extent is kept with the buffer.

use std::time::{Duration, Instant};

use tracing::debug;

use melin_app::auth::{ClientRole, RoleId};
use melin_app::decoder::{Decoded, ErasedDecoder};
use melin_app::{AppEvent, Application};
use melin_pipeline::ring;
use melin_transport_core::health::{IngressMetrics, IngressTally};
use melin_transport_core::pipeline::{ClientOrigin, InputSlot, RequestId};
use melin_transport_core::trace::MonoTraceInstant;
#[cfg(any(feature = "latency-trace", feature = "tick-to-trade"))]
use melin_transport_core::trace::mono_trace_ns;
use melin_wire_protocol::control::RefusalKind;
use melin_wire_protocol::framing::{split_frame_limited, split_request};

use crate::early_reply::{EarlyKind, EarlyReply, EarlyReplySender};
use crate::halt::{HaltGate, Verdict};

/// Bound on one client request frame, after the 4-byte length prefix:
/// the wire protocol's, so a client library and a node agree on it by
/// construction.
///
/// A frame declaring more is not read: both readers treat it as a
/// protocol violation and drop the connection. A program that reads
/// client frames itself takes the same bound from `melin-client`; an
/// application sizes its requests against [`MAX_REQUEST_BODY`].
pub(crate) const MAX_FRAME_SIZE: usize = melin_wire_protocol::blocking::MAX_FRAME_SIZE;

/// Bound on one request body — what an application's `RequestDecoder`
/// is handed after the runtime has read the tag and the request's ID.
/// Public so an application can check its widest request against it at
/// compile time; see the re-export in the crate root. The wire
/// protocol's, the bound a client's request framing enforces, so the two
/// sides agree on it by construction.
pub const MAX_REQUEST_BODY: usize = melin_wire_protocol::framing::MAX_REQUEST_BODY;

/// Outcome of [`process_client_frames`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrameAction {
    /// Every complete frame was processed: published, or answered early (a
    /// shed tail included). A partial trailing frame remains buffered for
    /// the next receive.
    Continue,
    /// The connection is to be closed (the frames before the cause were
    /// committed): an oversized frame, a frame that is not a request (an
    /// empty frame, another tag, a request too short for its ID, in a shed
    /// tail too), or a superseded node that takes nothing more from anyone.
    Disconnect,
    /// The early-reply queue is full: processing stopped before the frame
    /// whose answer had nowhere to go, which stays buffered with everything
    /// behind it. The reader stops reading the connection and calls again
    /// later with no new bytes, until the call returns something else.
    Park,
}

/// How long a connection may stay parked before the reader reports it:
/// past this the response stage has stopped draining early replies, which
/// is degraded operation rather than load. Also the shortest interval
/// between two reports ([`LongParkReport`]).
pub(crate) const LONG_PARK: Duration = Duration::from_secs(1);

/// Paces the reader's report of connections parked for longer than
/// [`LONG_PARK`]: at most one per [`LONG_PARK`], while any is.
#[derive(Debug, Default)]
pub(crate) struct LongParkReport {
    last: Option<Instant>,
}

impl LongParkReport {
    /// Whether a connection parked `since` has been parked past
    /// [`LONG_PARK`] at `now`, and so counts towards the report.
    pub(crate) fn parked_long(since: Instant, now: Instant) -> bool {
        now.saturating_duration_since(since) > LONG_PARK
    }

    /// Whether to report `long_parked` connections parked past
    /// [`LONG_PARK`] at `now`: never for none, and at most once per
    /// [`LONG_PARK`] otherwise. A `true` starts the next interval.
    fn due(&mut self, long_parked: usize, now: Instant) -> bool {
        if long_parked == 0 {
            return false;
        }
        if self
            .last
            .is_some_and(|last| now.duration_since(last) < LONG_PARK)
        {
            return false;
        }
        self.last = Some(now);
        true
    }

    /// Report, at `warn`, `long_parked` connections parked past
    /// [`LONG_PARK`] at `now`, when [`due`](Self::due): the one wording
    /// both readers log.
    pub(crate) fn report(&mut self, long_parked: usize, now: Instant) {
        if self.due(long_parked, now) {
            tracing::warn!(
                connections = long_parked,
                "client connections parked for over a second: the response stage is not taking \
                 early replies, and these clients are not read"
            );
        }
    }
}

/// One client connection's input, as both readers hold it: the bytes
/// received and not yet processed, and how far into them a shed tail
/// still runs (see the module docs). Only [`process_client_frames`] reads
/// or consumes it; a reader appends what it receives.
#[derive(Debug)]
pub(crate) struct ClientIngress {
    /// Bytes received and not yet processed: complete frames still to be
    /// answered after a park, then at most one partial frame. A `Vec`
    /// compacted to the front after each call: frames are split where they
    /// lie, as contiguous slices.
    parse_buf: Vec<u8>,
    /// How many bytes at the front of `parse_buf` are the rest of a shed
    /// tail, each frame in them still to be answered `ServerBusy`; zero
    /// outside one. Never more than `parse_buf.len()`, and always ending on
    /// a frame boundary: only `process_client_frames` sets it, and nothing
    /// else consumes bytes once the connection sends requests. `usize`, as
    /// it counts bytes of a buffer.
    shed_remaining: usize,
}

impl ClientIngress {
    /// Input over `parse_buf`, an empty buffer (one recycled from a pool,
    /// so its capacity is kept).
    pub(crate) fn new(parse_buf: Vec<u8>) -> Self {
        debug_assert!(parse_buf.is_empty(), "a connection starts with no input");
        Self {
            parse_buf,
            shed_remaining: 0,
        }
    }

    /// Bytes buffered and not yet processed.
    #[cfg(feature = "dpdk")]
    pub(crate) fn len(&self) -> usize {
        self.parse_buf.len()
    }

    /// Append received bytes.
    pub(crate) fn extend_from_slice(&mut self, bytes: &[u8]) {
        self.parse_buf.extend_from_slice(bytes);
    }

    /// The buffer itself, for a receive that appends into it in place, and
    /// for an authentication handshake, which consumes its own frame before
    /// any request is read (and so before any tail can be shed). Nothing
    /// may remove bytes through it once requests flow.
    #[cfg(feature = "dpdk")]
    pub(crate) fn buf_mut(&mut self) -> &mut Vec<u8> {
        &mut self.parse_buf
    }

    /// The buffer, emptied, for a pool to hand the next connection.
    #[cfg(feature = "dpdk")]
    pub(crate) fn into_buf(mut self) -> Vec<u8> {
        self.parse_buf.clear();
        self.parse_buf
    }

    /// The bytes buffered, for the crate's tests.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.parse_buf
    }

    /// How many buffered bytes are the rest of a shed tail, for the
    /// crate's tests.
    #[cfg(test)]
    pub(crate) fn shed_remaining(&self) -> usize {
        self.shed_remaining
    }
}

/// Extract, decode, and publish client request frames from `ingress`.
///
/// Processes every complete length-prefixed frame: each must be a request
/// (`TAG_REQUEST` and the client's ID), decoded through `decoder`, and
/// published to the input ring, the ID in its slot, under batched commits
/// (cap: 16 events per commit to bound consumer visibility delay).
/// Compacts the buffer on return.
///
/// Every request the reader does not publish is answered through
/// `early_replies`, under its ID (see [`crate::halt`]): a filtered request
/// with an empty batch, a refused one with the decoder's reason, a write
/// refused while `halt` refuses writes with the application's rejection
/// (queries are published either way), and the request that finds the
/// input ring full with `ServerBusy`, as is every complete frame buffered
/// behind it (the shed tail, see the module docs). The halt is sampled
/// once per call, so one receive is judged as a whole, and the writes it
/// refused are counted into `halt` once, at the end, as the requests it
/// shed, filtered and refused are into `ingress`. On a superseded node
/// nothing is read: the call returns [`FrameAction::Disconnect`] before
/// the first frame.
///
/// Every early reply the call queues is visible to the response stage by
/// the time it returns, whatever it returns: the queue is flushed once on
/// the way out, around every return path. An early reply waits for
/// nothing published, so where that flush falls relative to the input
/// ring's commits does not matter.
///
/// A full early-reply queue parks the connection ([`FrameAction::Park`]):
/// nothing is dropped and nothing is answered out of turn, and the next
/// call, with or without new bytes, starts at the frame that met it.
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
    ingress: &mut ClientIngress,
    connection_id: u64,
    key_hash: u64,
    role: ClientRole<RoleId>,
    producer: &mut ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn ErasedDecoder<A::Event>,
    halt: &HaltGate,
    early_replies: &mut EarlyReplySender<A::Report>,
    metrics: &IngressMetrics,
    batch_wall_ns: u64,
    recv_ts: MonoTraceInstant,
    #[cfg(feature = "latency-trace")] publish_rec: &mut melin_transport_core::trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) -> FrameAction {
    let action = process_frames_unflushed::<A>(
        ingress,
        connection_id,
        key_hash,
        role,
        producer,
        decoder,
        halt,
        early_replies,
        metrics,
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
    ingress: &mut ClientIngress,
    connection_id: u64,
    key_hash: u64,
    role: ClientRole<RoleId>,
    producer: &mut ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn ErasedDecoder<A::Event>,
    halt: &HaltGate,
    early_replies: &mut EarlyReplySender<A::Report>,
    metrics: &IngressMetrics,
    batch_wall_ns: u64,
    recv_ts: MonoTraceInstant,
    #[cfg(feature = "latency-trace")] publish_rec: &mut melin_transport_core::trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) -> FrameAction {
    let ClientIngress {
        parse_buf,
        shed_remaining,
    } = ingress;
    debug_assert!(*shed_remaining <= parse_buf.len());
    let mut cursor = 0;
    // Where the shed tail ends, as an offset into `parse_buf`: every frame
    // that starts before it is answered `ServerBusy`. What a park left of
    // a tail, then set when a request finds the ring full.
    let mut shed_end = *shed_remaining;
    let mut result = FrameAction::Continue;
    // Requests answered early in this call, counted into `metrics` once at
    // the end rather than with an atomic add each; the shed count is also
    // the call's one log line.
    let mut tally = IngressTally::default();

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
        let frame_start = cursor;
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

        // In a shed tail: answered busy, read only as far as its ID, never
        // decoded.
        if frame_start < shed_end {
            if !queue_early(early_replies, connection_id, request_id, EarlyKind::Busy) {
                cursor = frame_start;
                result = FrameAction::Park;
                break;
            }
            tally.shed += 1;
            continue;
        }

        // An answer decided here, or the event to publish.
        let early = match decoder.decode_erased(body, role) {
            Decoded::Filter => EarlyKind::Filtered,
            Decoded::PermissionDenied(reason) => EarlyKind::Refused {
                kind: RefusalKind::PermissionDenied,
                reason,
            },
            Decoded::DecodeError(reason) => EarlyKind::Refused {
                kind: RefusalKind::Malformed,
                reason,
            },
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
                    // so is every complete frame buffered behind it. The
                    // tail starts here, so a park on this very answer
                    // sheds this request on the next call rather than
                    // publishing it.
                    shed_end = frame_start + complete_frames_len(&parse_buf[frame_start..]);
                    if !queue_early(early_replies, connection_id, request_id, EarlyKind::Busy) {
                        cursor = frame_start;
                        result = FrameAction::Park;
                        break;
                    }
                    tally.shed += 1;
                    continue;
                }
            },
        };
        if !queue_early(early_replies, connection_id, request_id, early) {
            // Re-read on the next call: a decoder is a pure function of
            // the body and the role, so it decides the same again.
            cursor = frame_start;
            result = FrameAction::Park;
            break;
        }
        // Counted and logged once its answer is queued: a request parked
        // on is decoded again by every retry until then.
        match early {
            EarlyKind::Rejected(_) => refused += 1,
            EarlyKind::Refused { kind, reason } => {
                tally.refused(kind);
                match kind {
                    RefusalKind::PermissionDenied => {
                        debug!(connection_id, reason, "permission denied, refusing request")
                    }
                    RefusalKind::Malformed => {
                        debug!(connection_id, reason, "decode error, refusing request")
                    }
                }
            }
            EarlyKind::Filtered => tally.filtered += 1,
            // A busy is queued and counted where the ring is found full,
            // never through here.
            EarlyKind::Busy => {}
        }
    }

    batch.commit();

    if refused > 0 {
        halt.record_refused(refused);
    }
    metrics.record(&tally);
    if tally.shed > 0 {
        debug!(
            connection_id,
            shed = tally.shed,
            "input ring full, requests shed"
        );
    }

    // What is left of the tail: nothing once the call has passed its end.
    *shed_remaining = shed_end.saturating_sub(cursor);

    // Compact: shift remaining bytes to the front.
    if cursor > 0 {
        let remaining = parse_buf.len() - cursor;
        parse_buf.copy_within(cursor.., 0);
        parse_buf.truncate(remaining);
    }

    result
}

/// How many bytes at the front of `bytes` are complete frames: up to the
/// first partial frame, or an oversized prefix, or the end. What a shed
/// tail covers.
fn complete_frames_len(bytes: &[u8]) -> usize {
    let mut len = 0;
    // The scan stops at an oversized prefix without reporting it: the
    // frame is outside the tail, and the frame loop closes the connection
    // when it reaches it.
    while let Ok(Some(frame)) = split_frame_limited(&bytes[len..], MAX_FRAME_SIZE) {
        len += frame.consumed();
    }
    len
}

/// Stage the early reply `kind` for request `request_id`. `false` when the
/// queue is full: the caller parks the connection, since the answer has
/// nowhere to go yet.
fn queue_early<R: Copy>(
    early_replies: &mut EarlyReplySender<R>,
    connection_id: u64,
    request_id: RequestId,
    kind: EarlyKind<R>,
) -> bool {
    // A full queue is not logged here: the reader logs the park it
    // causes, once, where a line here would repeat on every retry of the
    // parked connection.
    early_replies
        .try_send(EarlyReply {
            connection_id,
            request_id,
            kind,
        })
        .is_ok()
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

// `process_client_frames` is tested in `reader.rs`, beside the reader that
// calls it, on its fixtures.
#[cfg(test)]
mod tests {
    use super::*;

    /// The long-park report: never for no connection, then at most once
    /// per `LONG_PARK` while any stays parked.
    #[test]
    fn a_long_park_is_reported_at_most_once_per_interval() {
        let mut report = LongParkReport::default();
        let start = Instant::now();
        assert!(!report.due(0, start), "nothing parked long");
        assert!(report.due(2, start), "the first report");
        assert!(!report.due(2, start + LONG_PARK / 2), "within the interval");
        assert!(report.due(1, start + LONG_PARK), "the next interval");
        assert!(!report.due(0, start + LONG_PARK * 3), "none any more");
        assert!(report.due(1, start + LONG_PARK * 3), "a new one, later");
    }

    /// A connection counts towards the report only once parked past
    /// `LONG_PARK`, and a clock read before the park (the slow checks'
    /// read, taken ahead of the pass that parks it) counts as no time.
    #[test]
    fn a_park_is_long_past_the_interval_only() {
        let since = Instant::now();
        assert!(!LongParkReport::parked_long(since, since));
        assert!(!LongParkReport::parked_long(since, since + LONG_PARK));
        assert!(LongParkReport::parked_long(
            since,
            since + LONG_PARK + Duration::from_millis(1)
        ));
        assert!(!LongParkReport::parked_long(
            since + Duration::from_secs(5),
            since
        ));
    }

    /// The tail covers whole frames only: up to a partial frame, an
    /// oversized prefix, or the end.
    #[test]
    fn a_shed_tail_covers_complete_frames_only() {
        let frame = |len: u8| {
            let mut f = u32::from(len).to_le_bytes().to_vec();
            f.extend(std::iter::repeat_n(0xAB, usize::from(len)));
            f
        };
        let two = [frame(3), frame(5)].concat();
        assert_eq!(complete_frames_len(&[]), 0);
        assert_eq!(complete_frames_len(&two), two.len());
        assert_eq!(
            complete_frames_len(&[two.as_slice(), &[9, 0]].concat()),
            two.len()
        );
        let oversized = (MAX_FRAME_SIZE as u32 + 1).to_le_bytes();
        assert_eq!(
            complete_frames_len(&[two.as_slice(), &oversized, &frame(1)].concat()),
            two.len()
        );
    }
}
