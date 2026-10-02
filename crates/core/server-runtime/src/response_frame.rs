//! Framing of application responses, shared by the kernel-TCP and DPDK
//! response stages: the application's encoder writes a body, and the
//! runtime writes the `[length: u32 LE][TAG_APP]` header in front of it,
//! so no application carries the protocol's framing.

use melin_app::encoder::ResponseEncoder;
use melin_transport_core::pipeline::OutputPayload;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec::{self, TAG_APP, TAG_LEN};

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

/// Frame an application response in `buf`: run `encode` on the body
/// region, then write the header in front of what it wrote. Returns the
/// frame's length, header included.
///
/// A length past the body region is this server's bug and comes back as
/// `Err` — the caller logs it and drops the response, rather than send a
/// frame that would desync the client's framing.
#[inline]
pub(crate) fn frame_app_response(
    buf: &mut EncodeBuf,
    encode: impl FnOnce(&mut [u8]) -> Result<usize, &'static str>,
) -> Result<usize, &'static str> {
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

/// Encode one output slot's payload frame into `buf`.
///
/// Application-shaped payloads (`Report`, `QueryResponse`) go through the
/// application's encoder and [`frame_app_response`]; `EngineError` is
/// transport-shaped and encoded by the runtime directly. `BatchEnd` carries
/// no body and returns `None` — the wire terminator is emitted from the
/// slot's `is_last_in_request` flag, not from the payload.
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
        OutputPayload::EngineError => Some(
            control_codec::encode_transport_response(&TransportResponse::EngineError, buf)
                .map_err(|_| "encode error"),
        ),
        OutputPayload::BatchEnd => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn an_engine_error_is_the_transport_frame() {
        let mut buf = [0u8; MAX_APP_FRAME];
        let n = encode_slot_payload(
            &OutputPayload::<u8, u16>::EngineError,
            &ByteEncoder,
            &mut buf,
        )
        .expect("EngineError has a body")
        .expect("EngineError encodes");
        let mut expected = [0u8; 16];
        let m = control_codec::encode_transport_response(
            &TransportResponse::EngineError,
            &mut expected,
        )
        .expect("EngineError encodes");
        assert_eq!(&buf[..n], &expected[..m]);
    }

    #[test]
    fn a_batch_end_has_no_payload_frame() {
        let mut buf = [0u8; MAX_APP_FRAME];
        let n = encode_slot_payload(&OutputPayload::<u8, u16>::BatchEnd, &ByteEncoder, &mut buf);
        assert_eq!(n, None);
    }

    fn frame(
        encode: impl FnOnce(&mut [u8]) -> Result<usize, &'static str>,
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
            Ok(MAX_RESPONSE_BODY)
        });
        assert_eq!(n, Ok(MAX_APP_FRAME));
    }

    #[test]
    fn a_length_past_the_body_is_refused() {
        let (n, _) = frame(|_| Ok(MAX_RESPONSE_BODY + 1));
        assert!(n.is_err());
    }

    #[test]
    fn an_encode_error_passes_through() {
        let (n, _) = frame(|_| Err("no"));
        assert_eq!(n, Err("no"));
    }
}
