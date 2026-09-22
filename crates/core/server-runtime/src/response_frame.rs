//! Framing of application responses, shared by the kernel-TCP and DPDK
//! response stages: the application's encoder writes a body and names its
//! tag, and the runtime writes the `[length: u32 LE][tag]` header around
//! it, so no application carries the protocol's framing.

use melin_app::encoder::Encoded;
use melin_wire_protocol::control_codec::FIRST_APP_TAG;

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
const HEADER_LEN: usize = 4 + 1;

/// Largest response frame an application's encoder can produce.
pub(crate) const MAX_APP_FRAME: usize = HEADER_LEN + MAX_RESPONSE_BODY;

/// Scratch buffer one response frame is encoded into. An array rather
/// than a `Vec`: fixed size, on the stack, no allocation on the hot path.
pub(crate) type EncodeBuf = [u8; MAX_APP_FRAME];

/// Frame an application response in `buf`: run `encode` on the body
/// region, then write the header in front of what it wrote. Returns the
/// frame's length, header included.
///
/// A tag in the protocol's reserved range, or a length past the body
/// region, is this server's bug and comes back as `Err` — the caller
/// logs it and drops the response, rather than send a frame the client
/// would read as a protocol frame or that would desync its framing.
#[inline]
pub(crate) fn frame_app_response(
    buf: &mut EncodeBuf,
    encode: impl FnOnce(&mut [u8]) -> Result<Encoded, &'static str>,
) -> Result<usize, &'static str> {
    let Encoded { tag, len } = encode(&mut buf[HEADER_LEN..])?;
    if tag < FIRST_APP_TAG {
        return Err("encoder returned a tag in the protocol's reserved range");
    }
    if len > MAX_RESPONSE_BODY {
        return Err("encoder reported a body longer than its buffer");
    }
    // Lossless: the tag plus at most `MAX_RESPONSE_BODY` bytes.
    let payload_len = (1 + len) as u32;
    buf[..4].copy_from_slice(&payload_len.to_le_bytes());
    buf[4] = tag;
    Ok(HEADER_LEN + len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(
        encode: impl FnOnce(&mut [u8]) -> Result<Encoded, &'static str>,
    ) -> (Result<usize, &'static str>, EncodeBuf) {
        let mut buf = [0u8; MAX_APP_FRAME];
        let result = frame_app_response(&mut buf, encode);
        (result, buf)
    }

    #[test]
    fn the_header_goes_in_front_of_the_body() {
        let (n, buf) = frame(|body| {
            body[..3].copy_from_slice(b"abc");
            Ok(Encoded { tag: 0x30, len: 3 })
        });
        assert_eq!(n, Ok(8));
        assert_eq!(&buf[..8], &[4, 0, 0, 0, 0x30, b'a', b'b', b'c']);
    }

    #[test]
    fn an_empty_body_is_a_tag_only_frame() {
        let (n, buf) = frame(|_| Ok(Encoded { tag: 0x31, len: 0 }));
        assert_eq!(n, Ok(5));
        assert_eq!(&buf[..5], &[1, 0, 0, 0, 0x31]);
    }

    #[test]
    fn the_encoder_is_given_the_whole_body_bound() {
        let (n, _) = frame(|body| {
            assert_eq!(body.len(), MAX_RESPONSE_BODY);
            Ok(Encoded {
                tag: FIRST_APP_TAG,
                len: MAX_RESPONSE_BODY,
            })
        });
        assert_eq!(n, Ok(MAX_APP_FRAME));
    }

    /// A reserved tag would reach the client as a heartbeat, a batch end
    /// or a protocol error — never as the response it is.
    #[test]
    fn a_reserved_tag_is_refused() {
        for tag in [0x00, 0x01, 0x02, FIRST_APP_TAG - 1] {
            let (n, _) = frame(|_| Ok(Encoded { tag, len: 0 }));
            assert!(n.is_err(), "tag {tag:#04x}");
        }
    }

    #[test]
    fn a_length_past_the_body_is_refused() {
        let (n, _) = frame(|_| {
            Ok(Encoded {
                tag: 0x30,
                len: MAX_RESPONSE_BODY + 1,
            })
        });
        assert!(n.is_err());
    }

    #[test]
    fn an_encode_error_passes_through() {
        let (n, _) = frame(|_| Err("no"));
        assert_eq!(n, Err("no"));
    }
}
