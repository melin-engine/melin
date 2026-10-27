//! Framing of application responses, shared by the kernel-TCP and DPDK
//! response stages: the application's encoder writes a body, and the
//! runtime writes the `[length: u32 LE][TAG_APP]` header in front of it,
//! so no application carries the protocol's framing.

use melin_app::encoder::ResponseEncoder;
use melin_transport_core::pipeline::OutputPayload;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec::{
    self, BATCH_END_FRAME_LEN, MAX_CONTROL_FRAME_LEN, TAG_APP, TAG_LEN,
};

use crate::early_reply::{EarlyKind, EarlyReply};

/// Bound on one application response body — what a `ResponseEncoder`
/// may write for a single report or query response.
///
/// The response stages encode into a stack buffer of this size behind
/// the frame header. An application whose encoder needs more does not get
/// a larger buffer: the encode fails, the reply is dropped, and the
/// failure is logged at `error!` — so an application should check its
/// widest body against this bound at compile time rather than discover
/// it under load. Public for that reason; see the re-export in the crate
/// root.
pub const MAX_RESPONSE_BODY: usize = 512;

/// Bytes the runtime writes ahead of a response body: the length prefix
/// and the tag.
const HEADER_LEN: usize = 4 + TAG_LEN;

/// Largest response frame an application's encoder can produce.
pub(crate) const MAX_APP_FRAME: usize = HEADER_LEN + MAX_RESPONSE_BODY;

/// Scratch buffer one response frame is encoded into. An array rather
/// than a `Vec`: fixed size, on the stack, no allocation on the hot path.
pub(crate) type EncodeBuf = [u8; MAX_APP_FRAME];

/// Frame an application response in `buf`: zero the body region, run
/// `encode` on it, then write the header in front of what it wrote.
/// Returns the frame's length, header included.
///
/// Every application response is framed here, whichever path sends it,
/// so the zeroing and, in debug builds, the purity check cover them all
/// and no caller can forget them.
///
/// A length past the body region is this server's bug and comes back as
/// `Err` — the caller logs it and drops the response, rather than send a
/// frame that would desync the client's framing.
///
/// # Panics
///
/// In debug builds only, if `encode` is impure: see
/// [`check_encoder_purity`].
#[inline]
pub(crate) fn frame_app_response(
    buf: &mut EncodeBuf,
    encode: impl Fn(&mut [u8]) -> Result<usize, &'static str>,
) -> Result<usize, &'static str> {
    // Triples the encoder calls, in debug builds only; release builds pay
    // nothing for it.
    #[cfg(debug_assertions)]
    check_encoder_purity(buf, &encode);
    frame_zeroed(buf, encode)
}

/// [`frame_app_response`] without the purity check: the zeroing, the
/// encode and the header, which is everything a release build does.
///
/// Writes the same header as the wire protocol's `seal_request` but does
/// not call it: the body cap here is `MAX_RESPONSE_BODY`, not the request
/// cap, the `&'static str` error cannot take a `From` conversion, and
/// `EncodeBuf`'s fixed size already bounds the frame, so the extra frame
/// limit check would be a wasted branch on the response hot path.
#[inline]
fn frame_zeroed(
    buf: &mut EncodeBuf,
    encode: impl FnOnce(&mut [u8]) -> Result<usize, &'static str>,
) -> Result<usize, &'static str> {
    // A release build cannot tell whether an encoder wrote every byte
    // below the length it returns, and an encoder may use the bytes past
    // it as scratch. The buffer is reused for every response, so a byte
    // it skips would otherwise carry an earlier body, possibly another
    // client's. Only the whole region guarantees no byte of an earlier
    // body survives: zeroing up to the previous length, or a high-water
    // mark of returned lengths, misses the scratch bytes, which resurface
    // when a later encode skips one. The buffer is reused, so it stays in
    // L1 and this is one short memset per response.
    buf[HEADER_LEN..].fill(0);
    let len = encode(&mut buf[HEADER_LEN..])?;
    if len > MAX_RESPONSE_BODY {
        return Err("encoder reported a body longer than its buffer");
    }
    // Lossless: the tag plus at most `MAX_RESPONSE_BODY` bytes.
    let payload_len = (TAG_LEN + len) as u32;
    buf[..4].copy_from_slice(&payload_len.to_le_bytes());
    buf[4] = TAG_APP;
    Ok(HEADER_LEN + len)
}

/// What [`check_encoder_purity`]'s panic message starts with.
#[cfg(debug_assertions)]
const IMPURE_ENCODER: &str = "impure response encoder";

/// The two patterns [`check_encoder_purity`] fills the body region with
/// before its two encodes: different at every bit, so a byte an encoder
/// skips differs between the two whatever its position.
#[cfg(debug_assertions)]
const POISON: [u8; 2] = [0xA5, 0x5A];

/// Debug builds' check that `encode` is pure: encode twice, the body
/// region filled with a different [`POISON`] pattern each time, and panic
/// unless both return the same `Ok` length with the same bytes below it,
/// or the same `Err`. That catches an encoder that skips a byte below the
/// length it returns (it shows the poison) and one whose output depends
/// on something other than its input, such as its own state.
///
/// A panic, not an error: an impure encoder is a bug in the application.
/// In production the zeroing makes a skipped byte harmless, but not a
/// body that depends on the encoder's state: copies of one response
/// then differ from node to node, which a digest of the responses
/// (planned, to detect diverging nodes) would turn into a stopped node.
/// And the skipped byte is still a bug, in a field the application meant
/// to write. Panicking in the tests that encode it finds the bug where it
/// is cheap to fix. Bytes past the returned length are not compared: an
/// encoder may use them as scratch.
#[cfg(debug_assertions)]
fn check_encoder_purity(
    buf: &mut EncodeBuf,
    encode: &impl Fn(&mut [u8]) -> Result<usize, &'static str>,
) {
    const CONTRACT: &str = "an encoder must write every byte below the length it returns, \
                            and its output must depend only on the value it encodes";
    let body = &mut buf[HEADER_LEN..];
    body.fill(POISON[0]);
    let first = encode(body);
    // An array, not a `Vec`: one body, its size fixed, no allocation.
    let mut first_body = [0u8; MAX_RESPONSE_BODY];
    first_body.copy_from_slice(body);
    body.fill(POISON[1]);
    let second = encode(body);
    if first != second {
        panic!(
            "{IMPURE_ENCODER}: two encodes of one value returned {first:?}, then {second:?}; \
             {CONTRACT}"
        );
    }
    // A length past the region is refused by the caller; compare what
    // lies within it.
    let Ok(len) = first.map(|len| len.min(MAX_RESPONSE_BODY)) else {
        return;
    };
    if let Some(at) = (0..len).find(|&i| first_body[i] != body[i]) {
        panic!(
            "{IMPURE_ENCODER}: body byte {at} of {len} differs between two encodes of one \
             value ({:#04x} over {:#04x}, then {:#04x} over {:#04x}); {CONTRACT}",
            first_body[at], POISON[0], body[at], POISON[1],
        );
    }
}

/// Encode one output slot's payload frame into `buf`.
///
/// Application-shaped payloads (`Report`, `QueryResponse`) go through the
/// application's encoder and [`frame_app_response`]. `BatchEnd` carries
/// no body and returns `None` — the wire terminator is emitted from the
/// slot's `is_last_in_request` flag, not from the payload. So does
/// `AckPolicyChange`, which the durability gate consumes and no client
/// receives.
#[inline]
pub(crate) fn encode_slot_payload<R: Copy, Q: Copy>(
    payload: &OutputPayload<R, Q>,
    encoder: &dyn ResponseEncoder<Report = R, Query = Q>,
    buf: &mut EncodeBuf,
) -> Option<Result<usize, &'static str>> {
    match payload {
        OutputPayload::Report(report) => Some(frame_app_response(buf, |body| {
            encoder.encode_report(report, body)
        })),
        OutputPayload::QueryResponse(q) => Some(frame_app_response(buf, |body| {
            encoder.encode_query(q, body)
        })),
        // A policy change is the gate's, not a reply: it has no client.
        OutputPayload::BatchEnd | OutputPayload::AckPolicyChange(_) => None,
    }
}

/// The widest early reply, every frame of it, length prefixes included:
/// a halt rejection (the application's report and a `BatchEnd`) or a
/// refusal with the longest reason, whichever is wider.
pub(crate) const MAX_EARLY_REPLY: usize = {
    let rejected = MAX_APP_FRAME + BATCH_END_FRAME_LEN;
    if rejected > MAX_CONTROL_FRAME_LEN {
        rejected
    } else {
        MAX_CONTROL_FRAME_LEN
    }
};

/// Scratch buffer one early reply's frames are encoded into: on the stack,
/// fixed size, no allocation.
pub(crate) type EarlyReplyBuf = [u8; MAX_EARLY_REPLY];

/// Encode every frame of an early reply into `buf`, back to back, and
/// return their length: the one encoding of an early reply, for both
/// response stages. A rejection is the application's report then
/// `BatchEnd`, a filtered request `BatchEnd` alone, a refusal its
/// `Refused` frame, a shed request `ServerBusy`; each final frame names
/// the request's ID.
///
/// A report the application's encoder fails on is this server's bug: it
/// is logged at `error!` and dropped, and the `BatchEnd` still goes out,
/// so the request is answered rather than leaving the client waiting.
pub(crate) fn encode_early_reply<R: Copy, Q: Copy>(
    reply: &EarlyReply<R>,
    encoder: &dyn ResponseEncoder<Report = R, Query = Q>,
    buf: &mut EarlyReplyBuf,
) -> usize {
    let id = reply.request_id;
    let (written, last) = match reply.kind {
        EarlyKind::Rejected(report) => {
            let (app, _) = buf
                .split_first_chunk_mut::<MAX_APP_FRAME>()
                .expect("an early reply's buffer holds an application frame (`MAX_EARLY_REPLY`)");
            let written = match frame_app_response(app, |body| encoder.encode_report(&report, body))
            {
                Ok(n) => n,
                Err(reason) => {
                    tracing::error!(connection_id = reply.connection_id, reason, "encode error");
                    0
                }
            };
            (written, TransportResponse::BatchEnd { id })
        }
        EarlyKind::Filtered => (0, TransportResponse::BatchEnd { id }),
        EarlyKind::Refused { kind, reason } => (0, TransportResponse::Refused { id, kind, reason }),
        EarlyKind::Busy => (0, TransportResponse::ServerBusy { id }),
    };
    let tail = control_codec::encode_transport_response(&last, &mut buf[written..])
        .expect("an early reply's final frame fits behind its report (`MAX_EARLY_REPLY`)");
    written + tail
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_transport_core::pipeline::RequestId;
    use melin_wire_protocol::control::RefusalKind;

    /// Writes the report / query value as one byte, so a test can see
    /// which encoder method ran.
    struct ByteEncoder;

    impl ResponseEncoder for ByteEncoder {
        type Report = u8;
        type Query = u16;

        fn encode_report(&self, report: &u8, buf: &mut [u8]) -> Result<usize, &'static str> {
            buf[0] = *report;
            Ok(1)
        }

        fn encode_query(&self, query: &u16, buf: &mut [u8]) -> Result<usize, &'static str> {
            buf[..2].copy_from_slice(&query.to_le_bytes());
            Ok(2)
        }
    }

    #[test]
    fn a_report_goes_through_the_report_encoder() {
        let mut buf = [0u8; MAX_APP_FRAME];
        let n = encode_slot_payload(&OutputPayload::Report(7), &ByteEncoder, &mut buf);
        assert_eq!(n, Some(Ok(6)));
        assert_eq!(&buf[..6], &[2, 0, 0, 0, TAG_APP, 7]);
    }

    #[test]
    fn a_query_response_goes_through_the_query_encoder() {
        let mut buf = [0u8; MAX_APP_FRAME];
        let n = encode_slot_payload(
            &OutputPayload::<u8, u16>::QueryResponse(0x0201),
            &ByteEncoder,
            &mut buf,
        );
        assert_eq!(n, Some(Ok(7)));
        assert_eq!(&buf[..7], &[3, 0, 0, 0, TAG_APP, 1, 2]);
    }

    /// `response`, encoded on its own.
    fn control(response: TransportResponse) -> Vec<u8> {
        let mut buf = [0u8; MAX_CONTROL_FRAME_LEN];
        let n = control_codec::encode_transport_response(&response, &mut buf).unwrap();
        buf[..n].to_vec()
    }

    /// An early reply of `kind` for request 42 on connection 7, encoded.
    fn early(kind: EarlyKind<u8>) -> Vec<u8> {
        early_with(&ByteEncoder, kind)
    }

    fn early_with(
        encoder: &dyn ResponseEncoder<Report = u8, Query = u16>,
        kind: EarlyKind<u8>,
    ) -> Vec<u8> {
        let mut buf = [0u8; MAX_EARLY_REPLY];
        let reply = EarlyReply {
            connection_id: 7,
            request_id: RequestId(42),
            kind,
        };
        let n = encode_early_reply(&reply, encoder, &mut buf);
        buf[..n].to_vec()
    }

    const ID: RequestId = RequestId(42);

    #[test]
    fn each_early_reply_is_its_frames_naming_the_request() {
        // A rejection: the report, then the batch end.
        assert_eq!(
            early(EarlyKind::Rejected(9)),
            [
                &[2, 0, 0, 0, TAG_APP, 9][..],
                &control(TransportResponse::BatchEnd { id: ID })
            ]
            .concat()
        );
        // A filtered request: an empty batch.
        assert_eq!(
            early(EarlyKind::Filtered),
            control(TransportResponse::BatchEnd { id: ID })
        );
        // A refusal and a busy: one frame each.
        let refused = EarlyKind::Refused {
            kind: RefusalKind::PermissionDenied,
            reason: "no",
        };
        assert_eq!(
            early(refused),
            control(TransportResponse::Refused {
                id: ID,
                kind: RefusalKind::PermissionDenied,
                reason: "no"
            })
        );
        assert_eq!(
            early(EarlyKind::Busy),
            control(TransportResponse::ServerBusy { id: ID })
        );
    }

    /// The widest of each fits the buffer: a report of the widest body
    /// with its batch end, and a refusal whose reason is cut to the limit.
    #[test]
    fn the_widest_early_replies_fit() {
        let rejected = early_with(&FillingEncoder, EarlyKind::Rejected(0xAB));
        assert_eq!(rejected.len(), MAX_APP_FRAME + BATCH_END_FRAME_LEN);
        assert!(rejected.len() <= MAX_EARLY_REPLY);
        let long: &'static str = "r".repeat(1000).leak();
        let refused = early(EarlyKind::Refused {
            kind: RefusalKind::Malformed,
            reason: long,
        });
        assert_eq!(refused.len(), MAX_CONTROL_FRAME_LEN);
        assert!(refused.len() <= MAX_EARLY_REPLY);
    }

    /// A report the encoder fails on is dropped, and the request is still
    /// answered with its batch end.
    #[test]
    fn a_rejection_that_fails_to_encode_still_ends_its_batch() {
        struct Failing;
        impl ResponseEncoder for Failing {
            type Report = u8;
            type Query = u16;
            fn encode_report(&self, _: &u8, _: &mut [u8]) -> Result<usize, &'static str> {
                Err("no")
            }
            fn encode_query(&self, _: &u16, _: &mut [u8]) -> Result<usize, &'static str> {
                Err("no")
            }
        }
        assert_eq!(
            early_with(&Failing, EarlyKind::Rejected(1)),
            control(TransportResponse::BatchEnd { id: ID })
        );
    }

    #[test]
    fn a_batch_end_has_no_payload_frame() {
        let mut buf = [0u8; MAX_APP_FRAME];
        let n = encode_slot_payload(&OutputPayload::<u8, u16>::BatchEnd, &ByteEncoder, &mut buf);
        assert_eq!(n, None);
    }

    #[test]
    fn a_policy_change_has_no_payload_frame() {
        let mut buf = [0u8; MAX_APP_FRAME];
        let change = OutputPayload::<u8, u16>::AckPolicyChange(melin_journal::AckPolicyChange {
            policy: melin_journal::AckPolicyByte(0),
            unwitnessed: false,
        });
        assert_eq!(encode_slot_payload(&change, &ByteEncoder, &mut buf), None);
    }

    fn frame(
        encode: impl Fn(&mut [u8]) -> Result<usize, &'static str>,
    ) -> (Result<usize, &'static str>, EncodeBuf) {
        let mut buf = [0u8; MAX_APP_FRAME];
        let result = frame_app_response(&mut buf, encode);
        (result, buf)
    }

    #[test]
    fn the_header_goes_in_front_of_the_body() {
        let (n, buf) = frame(|body| {
            body[..3].copy_from_slice(b"abc");
            Ok(3)
        });
        assert_eq!(n, Ok(8));
        assert_eq!(&buf[..8], &[4, 0, 0, 0, TAG_APP, b'a', b'b', b'c']);
    }

    #[test]
    fn an_empty_body_is_a_tag_only_frame() {
        let (n, buf) = frame(|_| Ok(0));
        assert_eq!(n, Ok(5));
        assert_eq!(&buf[..5], &[1, 0, 0, 0, TAG_APP]);
    }

    /// The body is the application's from its first byte: one that looks
    /// like a protocol tag is carried as it is, behind the application
    /// tag, and a client cannot read it as a protocol frame.
    #[test]
    fn a_body_may_start_with_any_byte() {
        for first in [0x00, 0x01, 0x02, TAG_APP, 0xFF] {
            let (n, buf) = frame(|body| {
                body[0] = first;
                Ok(1)
            });
            assert_eq!(n, Ok(6));
            assert_eq!(&buf[..6], &[2, 0, 0, 0, TAG_APP, first]);
        }
    }

    #[test]
    fn the_encoder_is_given_the_whole_body_bound() {
        let (n, _) = frame(|body| {
            assert_eq!(body.len(), MAX_RESPONSE_BODY);
            body.fill(1);
            Ok(MAX_RESPONSE_BODY)
        });
        assert_eq!(n, Ok(MAX_APP_FRAME));
    }

    #[test]
    fn a_length_past_the_body_is_refused() {
        let (n, _) = frame(|body| {
            body.fill(1);
            Ok(MAX_RESPONSE_BODY + 1)
        });
        assert!(n.is_err());
    }

    #[test]
    fn an_encode_error_passes_through() {
        let (n, _) = frame(|_| Err("no"));
        assert_eq!(n, Err("no"));
    }

    /// Body length both of [`SkippingEncoder`]'s methods return.
    const SKIPPING_LEN: usize = 16;
    /// The body byte [`SkippingEncoder`]'s report leaves unwritten.
    const SKIPPED: usize = 1;

    /// An encoder that leaves a byte of its report unwritten, as one with
    /// a reserved or padding field might: its query fills the whole body
    /// with `0xFF`, its report writes the value at every body byte but
    /// [`SKIPPED`].
    struct SkippingEncoder;

    impl ResponseEncoder for SkippingEncoder {
        type Report = u8;
        type Query = u16;

        fn encode_report(&self, report: &u8, buf: &mut [u8]) -> Result<usize, &'static str> {
            buf[..SKIPPED].fill(*report);
            buf[SKIPPED + 1..SKIPPING_LEN].fill(*report);
            Ok(SKIPPING_LEN)
        }

        fn encode_query(&self, _: &u16, buf: &mut [u8]) -> Result<usize, &'static str> {
            buf[..SKIPPING_LEN].fill(0xFF);
            Ok(SKIPPING_LEN)
        }
    }

    /// [`SkippingEncoder`]'s report of 7, framed: the skipped byte zero,
    /// every other body byte 7.
    fn skipped_report_frame() -> Vec<u8> {
        let mut frame = vec![(TAG_LEN + SKIPPING_LEN) as u8, 0, 0, 0, TAG_APP];
        frame.extend((0..SKIPPING_LEN).map(|i| if i == SKIPPED { 0 } else { 7 }));
        frame
    }

    // The zeroing test below frames through `frame_zeroed`, which skips
    // the purity check: its encoder is impure by design, so through
    // `frame_app_response` a debug build panics before the zeroing is
    // seen. That keeps the release guarantee tested in every build.

    /// The previous body was another response, possibly to another
    /// client: none of it may reach this one through a byte the encoder
    /// skipped.
    #[test]
    fn a_body_byte_the_encoder_skips_is_zero_not_the_previous_bodys() {
        let mut buf = [0u8; MAX_APP_FRAME];
        let query = frame_zeroed(&mut buf, |body| SkippingEncoder.encode_query(&0, body));
        assert_eq!(query, Ok(HEADER_LEN + SKIPPING_LEN));
        let report = frame_zeroed(&mut buf, |body| SkippingEncoder.encode_report(&7, body));
        assert_eq!(report, Ok(HEADER_LEN + SKIPPING_LEN));
        assert_eq!(
            &buf[..HEADER_LEN + SKIPPING_LEN],
            &skipped_report_frame()[..],
            "the skipped byte carries the query's"
        );
    }

    /// The early-reply path zeroes too: a short early rejection encoded
    /// where a widest one was leaves the rest of the application frame's
    /// region zero, not the previous body's. Both encoders are pure, so
    /// this goes through `encode_early_reply` itself in every build.
    #[test]
    fn an_early_rejection_carries_no_byte_of_the_previous_early_reply() {
        let mut buf = [0u8; MAX_EARLY_REPLY];
        let reply = |report| EarlyReply {
            connection_id: 7,
            request_id: ID,
            kind: EarlyKind::Rejected(report),
        };
        // A widest body, every byte 0xFF: what the next reply must not show.
        let full = encode_early_reply(&reply(0xFF), &FillingEncoder, &mut buf);
        assert_eq!(full, MAX_APP_FRAME + BATCH_END_FRAME_LEN);
        let n = encode_early_reply(&reply(7), &ByteEncoder, &mut buf);
        let report = [2, 0, 0, 0, TAG_APP, 7];
        let batch_end = control(TransportResponse::BatchEnd { id: ID });
        assert_eq!(&buf[..n], &[&report[..], &batch_end].concat()[..]);
        assert!(
            buf[n..MAX_APP_FRAME].iter().all(|&b| b == 0),
            "the region past the reply carries the previous body's bytes"
        );
    }

    /// Whatever the purity check does to the region, what is sent is
    /// what a release build sends: the body over zeroes, not over poison.
    #[test]
    fn a_frame_is_the_same_in_debug_and_release_builds() {
        let mut checked = [0u8; MAX_APP_FRAME];
        let mut zeroed = [0u8; MAX_APP_FRAME];
        // Writes less than it returns, but the same bytes every time: pure.
        let encode = |body: &mut [u8]| {
            body[0] = 7;
            Ok(1)
        };
        assert_eq!(frame_app_response(&mut checked, encode), Ok(6));
        assert_eq!(frame_zeroed(&mut zeroed, encode), Ok(6));
        assert_eq!(checked, zeroed, "the whole buffer, body region included");
        assert!(
            checked[6..].iter().all(|&b| b == 0),
            "no poison left behind"
        );
    }

    /// Writes a call count into its one-byte body: output that depends on
    /// the encoder's state, not on the value it encodes.
    #[cfg(debug_assertions)]
    #[derive(Default)]
    struct CountingEncoder {
        // An atomic, not a `Cell`: a `ResponseEncoder` is `Sync`.
        calls: std::sync::atomic::AtomicU8,
    }

    #[cfg(debug_assertions)]
    impl ResponseEncoder for CountingEncoder {
        type Report = u8;
        type Query = u16;
        fn encode_report(&self, _: &u8, buf: &mut [u8]) -> Result<usize, &'static str> {
            buf[0] = self
                .calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(1)
        }
        fn encode_query(&self, _: &u16, _: &mut [u8]) -> Result<usize, &'static str> {
            Ok(0)
        }
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "impure response encoder: body byte 1 of 16 differs")]
    fn a_skipped_body_byte_fails_the_purity_check() {
        let mut buf = [0u8; MAX_APP_FRAME];
        // Dropped result: the call panics before it returns one.
        let _ = encode_slot_payload(&OutputPayload::Report(7), &SkippingEncoder, &mut buf);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "impure response encoder: body byte 0 of 1 differs")]
    fn a_body_that_depends_on_the_encoders_state_fails_the_purity_check() {
        let mut buf = [0u8; MAX_APP_FRAME];
        // Dropped result: the call panics before it returns one.
        let _ = encode_slot_payload(
            &OutputPayload::Report(7),
            &CountingEncoder::default(),
            &mut buf,
        );
    }

    /// A result that depends on the encoder's state: a length that
    /// changes, or an error one time and not the next.
    #[cfg(debug_assertions)]
    #[test]
    fn a_result_that_depends_on_the_encoders_state_fails_the_purity_check() {
        let results: [[Result<usize, &'static str>; 2]; 3] = [
            [Ok(1), Ok(2)],
            [Ok(1), Err("no")],
            [Err("no"), Err("other")],
        ];
        for pair in results {
            let calls = std::sync::atomic::AtomicUsize::new(0);
            let encode = |body: &mut [u8]| {
                body[..2].fill(0);
                pair[calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % 2]
            };
            let panic = std::panic::catch_unwind(|| {
                let mut buf = [0u8; MAX_APP_FRAME];
                frame_app_response(&mut buf, encode)
            })
            .expect_err("an impure result must fail the check");
            let message = panic
                .downcast_ref::<String>()
                .expect("a formatted panic message");
            assert!(
                message.starts_with(&format!(
                    "{IMPURE_ENCODER}: two encodes of one value returned {:?}, then {:?}",
                    pair[0], pair[1]
                )),
                "{message}"
            );
        }
    }

    /// The early-reply path is checked too: it frames through
    /// `frame_app_response`.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "impure response encoder: body byte 1 of 16 differs")]
    fn an_early_rejection_is_checked_for_purity() {
        early_with(&SkippingEncoder, EarlyKind::Rejected(7));
    }

    /// Scratch writes past the returned length are not compared, however
    /// they vary: an encoder may use those bytes as it likes.
    #[test]
    fn scratch_past_the_returned_length_passes_the_purity_check() {
        let calls = std::sync::atomic::AtomicU8::new(0);
        let (n, buf) = frame(|body| {
            body[1] = calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            body[0] = 7;
            Ok(1)
        });
        assert_eq!(n, Ok(6));
        assert_eq!(&buf[..6], &[2, 0, 0, 0, TAG_APP, 7]);
    }

    /// Fills the whole body with the report's value, so a test can see
    /// every byte of it.
    struct FillingEncoder;

    impl ResponseEncoder for FillingEncoder {
        type Report = u8;
        type Query = u16;
        fn encode_report(&self, report: &u8, buf: &mut [u8]) -> Result<usize, &'static str> {
            buf.fill(*report);
            Ok(buf.len())
        }
        fn encode_query(&self, _: &u16, _: &mut [u8]) -> Result<usize, &'static str> {
            Ok(0)
        }
    }
}
