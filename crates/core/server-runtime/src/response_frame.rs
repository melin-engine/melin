//! Framing of application responses, shared by the kernel-TCP and DPDK
//! response stages: the application's encoder writes a body, and the
//! runtime writes the `[length: u32 LE][TAG_APP]` header in front of it,
//! so no application carries the protocol's framing.

use melin_app::encoder::ResponseEncoder;
use melin_transport_core::output_digest::{
    BodyBuf, MAX_RESPONSE_BODY, ReportBody, encode_app_body,
};
use melin_transport_core::pipeline::OutputPayload;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec::{
    self, BATCH_END_FRAME_LEN, MAX_CONTROL_FRAME_LEN, TAG_APP, TAG_LEN,
};

use crate::early_reply::{EarlyKind, EarlyReply};

/// Bytes the runtime writes ahead of a response body: the length prefix
/// and the tag.
const HEADER_LEN: usize = 4 + TAG_LEN;

/// Largest response frame an application's encoder can produce.
pub(crate) const MAX_APP_FRAME: usize = HEADER_LEN + MAX_RESPONSE_BODY;

/// Scratch buffer one response frame is encoded into. An array rather
/// than a `Vec`: fixed size, on the stack, no allocation on the hot path.
pub(crate) type EncodeBuf = [u8; MAX_APP_FRAME];

/// Frame an application response in `buf`: encode its body behind the
/// header's room ([`encode_app_body`], which zeroes the region and, in
/// debug builds, checks the encoder's purity), then write the header in
/// front of what it wrote. Returns the frame's length, header included.
///
/// Every application response sent is framed here, so the body encode
/// every report and query response goes through is the same one a copy
/// that only digests a report uses.
///
/// A length past the body region is this server's bug and comes back as
/// `Err`: the caller logs it and drops the response, rather than send a
/// frame that would desync the client's framing.
///
/// Writes the same header as the wire protocol's `seal_request` but does
/// not call it: the body cap here is `MAX_RESPONSE_BODY`, not the request
/// cap, the `&'static str` error cannot take a `From` conversion, and
/// `EncodeBuf`'s fixed size already bounds the frame, so the extra frame
/// limit check would be a wasted branch on the response hot path.
///
/// # Panics
///
/// In debug builds only, if `encode` is impure (see [`encode_app_body`]).
#[inline]
pub(crate) fn frame_app_response(
    buf: &mut EncodeBuf,
    encode: impl Fn(&mut [u8]) -> Result<usize, &'static str>,
) -> Result<usize, &'static str> {
    let (header, body) = buf
        .split_first_chunk_mut::<HEADER_LEN>()
        .expect("a response frame holds its header (`MAX_APP_FRAME`)");
    let body: &mut BodyBuf = body
        .try_into()
        .expect("a response frame's body region is `MAX_RESPONSE_BODY` bytes (`MAX_APP_FRAME`)");
    let len = encode_app_body(body, encode)?;
    // Lossless: the tag plus at most `MAX_RESPONSE_BODY` bytes.
    let payload_len = (TAG_LEN + len) as u32;
    header[..4].copy_from_slice(&payload_len.to_le_bytes());
    header[4] = TAG_APP;
    Ok(HEADER_LEN + len)
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

/// A report's body as the output digest folds it, from what
/// [`encode_slot_payload`] gave for it in `buf`: the body behind the
/// header, or `Failed` for an encode error.
///
/// # Panics
///
/// If `encoded` is `None`: a report always has a payload frame, so a
/// `None` here is a slot that was not a report, the caller's bug.
#[inline]
pub(crate) fn report_body<'a>(
    buf: &'a EncodeBuf,
    encoded: Option<Result<usize, &'static str>>,
) -> ReportBody<'a> {
    // `encoded` is the frame's length, header included; the body is what
    // follows the header.
    let body_len = encoded
        .expect("a report always has a payload frame")
        .map(|frame_len| frame_len - HEADER_LEN);
    ReportBody::of(&buf[HEADER_LEN..], body_len)
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

    // The zeroing of a byte an encoder skips, the purity check's verdicts
    // and the release build's equivalence are pinned with
    // `encode_app_body`, in `melin_transport_core::output_digest`. The
    // tests here pin that both framing paths go through it.

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

    /// A slot's report is checked for purity: it frames through
    /// `encode_app_body`.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "impure response encoder: body byte 1 of 16 differs")]
    fn a_skipped_body_byte_fails_the_purity_check() {
        let mut buf = [0u8; MAX_APP_FRAME];
        // Dropped result: the call panics before it returns one.
        let _ = encode_slot_payload(&OutputPayload::Report(7), &SkippingEncoder, &mut buf);
    }

    /// The early-reply path is checked too: it frames through
    /// `frame_app_response`.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "impure response encoder: body byte 1 of 16 differs")]
    fn an_early_rejection_is_checked_for_purity() {
        early_with(&SkippingEncoder, EarlyKind::Rejected(7));
    }

    /// A frame's region past its body is zero, poison-free, in every
    /// build: the body went through the zeroing encode.
    #[test]
    fn a_frame_leaves_no_poison_behind() {
        let (n, buf) = frame(|body| {
            body[0] = 7;
            Ok(1)
        });
        assert_eq!(n, Ok(6));
        assert_eq!(&buf[..6], &[2, 0, 0, 0, TAG_APP, 7]);
        assert!(buf[6..].iter().all(|&b| b == 0), "no poison left behind");
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
