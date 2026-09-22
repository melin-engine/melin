//! Codec for transport-level control frames.
//!
//! Encodes/decodes [`TransportResponse`] and [`ChallengeResponse`].
//! Tag constants are public so every program that reads or writes frames
//! — the node, the client library, a gateway on its own I/O loop — shares
//! one source of truth for the wire values.
//!
//! Every frame, in either direction, is `[length: u32 LE][tag: u8][body]`.
//! The tag is the protocol's alone: [`TAG_APP`] marks a frame whose body
//! belongs to the application, and every other tag is one of the
//! protocol's own frames. The protocol never reads an application body,
//! and an application never sees the tag, so an application body may
//! start with any byte.

use crate::control::{ChallengeResponse, TransportResponse};
use crate::error::ProtocolError;

/// Length of the tag that follows every frame's length prefix, in either
/// direction.
pub const TAG_LEN: usize = 1;

/// An application frame: the body is the application's request or
/// response, opaque to the protocol.
///
/// Not `0x00`, so a zeroed buffer on the wire is an unknown tag rather
/// than an empty application frame. Not in `0x10..`, the range the
/// protocol once left to application tags in this position, so a client
/// still framing requests that way has them dropped as unknown tags
/// rather than misread as application frames.
pub const TAG_APP: u8 = 0x09;

pub const TAG_RESPONSE_HEARTBEAT: u8 = 0x01;
pub const TAG_BATCH_END: u8 = 0x02;
pub const TAG_ENGINE_ERROR: u8 = 0x03;
pub const TAG_SERVER_BUSY: u8 = 0x04;
pub const TAG_CHALLENGE: u8 = 0x05;
pub const TAG_CHALLENGE_RESPONSE: u8 = 0x06;
pub const TAG_AUTH_FAILED: u8 = 0x07;
pub const TAG_SERVER_READY: u8 = 0x08;

/// Encode a transport-level response into `buf`.
///
/// Returns the total bytes written including the 4-byte LE length
/// prefix. Returns `Err(Truncated)` if `buf` is too small for the
/// variant (5 bytes for tag-only variants, 37 for Challenge).
pub fn encode_transport_response(
    response: &TransportResponse,
    buf: &mut [u8],
) -> Result<usize, ProtocolError> {
    // 4-byte length prefix + 1-byte tag; Challenge adds 32 nonce bytes.
    let needed = match response {
        TransportResponse::Challenge { .. } => 4 + TAG_LEN + 32,
        _ => 4 + TAG_LEN,
    };
    if buf.len() < needed {
        return Err(ProtocolError::Truncated);
    }

    let mut pos = 4;

    match response {
        TransportResponse::Heartbeat => {
            buf[pos] = TAG_RESPONSE_HEARTBEAT;
            pos += 1;
        }
        TransportResponse::BatchEnd => {
            buf[pos] = TAG_BATCH_END;
            pos += 1;
        }
        TransportResponse::EngineError => {
            buf[pos] = TAG_ENGINE_ERROR;
            pos += 1;
        }
        TransportResponse::ServerBusy => {
            buf[pos] = TAG_SERVER_BUSY;
            pos += 1;
        }
        TransportResponse::Challenge { nonce } => {
            buf[pos] = TAG_CHALLENGE;
            pos += 1;
            buf[pos..pos + 32].copy_from_slice(nonce);
            pos += 32;
        }
        TransportResponse::AuthFailed => {
            buf[pos] = TAG_AUTH_FAILED;
            pos += 1;
        }
        TransportResponse::ServerReady => {
            buf[pos] = TAG_SERVER_READY;
            pos += 1;
        }
    }

    // Write the length prefix (payload length, excluding the prefix itself).
    let payload_len = (pos - 4) as u32;
    buf[..4].copy_from_slice(&payload_len.to_le_bytes());

    Ok(pos)
}

/// Wire size of a challenge-response frame payload:
/// tag(1) + signature(64) + public_key(32).
pub const CHALLENGE_RESPONSE_LEN: usize = TAG_LEN + 64 + 32;

/// Encode a client's auth challenge-response into `buf`, as the frame
/// payload *without* the 4-byte length prefix:
/// `[tag:u8][signature:64][pubkey:32]`.
///
/// The inverse of [`decode_challenge_response`], kept beside it so the
/// layout has one home. Returns `Err(Truncated)` if `buf` is shorter
/// than [`CHALLENGE_RESPONSE_LEN`].
pub fn encode_challenge_response(
    response: &ChallengeResponse,
    buf: &mut [u8],
) -> Result<usize, ProtocolError> {
    if buf.len() < CHALLENGE_RESPONSE_LEN {
        return Err(ProtocolError::Truncated);
    }
    buf[0] = TAG_CHALLENGE_RESPONSE;
    buf[1..65].copy_from_slice(&response.signature);
    buf[65..97].copy_from_slice(&response.public_key);
    Ok(CHALLENGE_RESPONSE_LEN)
}

/// Decode a client's auth challenge-response from a wire frame.
///
/// `buf` must contain the frame payload *after* the 4-byte length
/// prefix has been stripped: `[tag:u8][signature:64][pubkey:32]`.
///
/// Returns `Err(UnknownTag)` if the tag is not `TAG_CHALLENGE_RESPONSE`.
pub fn decode_challenge_response(buf: &[u8]) -> Result<ChallengeResponse, ProtocolError> {
    if buf.len() < CHALLENGE_RESPONSE_LEN {
        return Err(ProtocolError::Truncated);
    }

    let tag = buf[0];
    if tag != TAG_CHALLENGE_RESPONSE {
        return Err(ProtocolError::UnknownTag(tag));
    }

    let mut signature = [0u8; 64];
    signature.copy_from_slice(&buf[1..65]);
    let mut public_key = [0u8; 32];
    public_key.copy_from_slice(&buf[65..97]);

    Ok(ChallengeResponse {
        signature,
        public_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_tag_only_variants() {
        let variants = [
            TransportResponse::Heartbeat,
            TransportResponse::BatchEnd,
            TransportResponse::EngineError,
            TransportResponse::ServerBusy,
            TransportResponse::AuthFailed,
            TransportResponse::ServerReady,
        ];

        for variant in &variants {
            let mut buf = [0u8; 8];
            let written = encode_transport_response(variant, &mut buf).unwrap();
            // 4-byte length prefix + 1-byte tag = 5 bytes
            assert_eq!(written, 5, "variant {variant:?}");
            // Length prefix should be 1 (just the tag byte)
            assert_eq!(u32::from_le_bytes(buf[..4].try_into().unwrap()), 1);
        }
    }

    /// The application's tag is distinct from every control frame's, so
    /// a client can never read one as the other.
    #[test]
    fn the_app_tag_is_no_control_frame() {
        let variants = [
            TransportResponse::Heartbeat,
            TransportResponse::BatchEnd,
            TransportResponse::EngineError,
            TransportResponse::ServerBusy,
            TransportResponse::Challenge { nonce: [0; 32] },
            TransportResponse::AuthFailed,
            TransportResponse::ServerReady,
        ];
        for variant in &variants {
            let mut buf = [0u8; 64];
            encode_transport_response(variant, &mut buf).unwrap();
            assert_ne!(buf[4], TAG_APP, "variant {variant:?}");
        }
        assert_ne!(TAG_APP, TAG_CHALLENGE_RESPONSE);
        assert_ne!(TAG_APP, 0x00);
    }

    #[test]
    fn encode_truncated_tag_only() {
        let mut buf = [0u8; 4]; // too small (needs 5)
        assert!(matches!(
            encode_transport_response(&TransportResponse::Heartbeat, &mut buf),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn encode_truncated_challenge() {
        let mut buf = [0u8; 36]; // too small (needs 37)
        assert!(matches!(
            encode_transport_response(&TransportResponse::Challenge { nonce: [0; 32] }, &mut buf),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn round_trip_challenge() {
        let nonce = [0xAB; 32];
        let mut buf = [0u8; 64];
        let written =
            encode_transport_response(&TransportResponse::Challenge { nonce }, &mut buf).unwrap();
        // 4 prefix + 1 tag + 32 nonce = 37
        assert_eq!(written, 37);
        assert_eq!(
            u32::from_le_bytes(buf[..4].try_into().unwrap()),
            33 // tag + nonce
        );
        assert_eq!(buf[4], TAG_CHALLENGE);
        assert_eq!(&buf[5..37], &nonce);
    }

    #[test]
    fn decode_valid_challenge_response() {
        let sig = [0x11; 64];
        let pubkey = [0x22; 32];

        // Build wire frame: [tag:u8][sig:64][pubkey:32]
        let mut buf = [0u8; 97];
        buf[0] = TAG_CHALLENGE_RESPONSE;
        buf[1..65].copy_from_slice(&sig);
        buf[65..97].copy_from_slice(&pubkey);

        let cr = decode_challenge_response(&buf).unwrap();
        assert_eq!(cr.signature, sig);
        assert_eq!(cr.public_key, pubkey);
    }

    #[test]
    fn challenge_response_round_trips() {
        let response = ChallengeResponse {
            signature: [0x33; 64],
            public_key: [0x44; 32],
        };
        let mut buf = [0u8; CHALLENGE_RESPONSE_LEN];
        let written = encode_challenge_response(&response, &mut buf).unwrap();
        assert_eq!(written, CHALLENGE_RESPONSE_LEN);
        assert_eq!(decode_challenge_response(&buf).unwrap(), response);

        let mut short = [0u8; CHALLENGE_RESPONSE_LEN - 1];
        assert!(matches!(
            encode_challenge_response(&response, &mut short),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn decode_truncated() {
        let buf = [0u8; 50]; // too short
        assert!(matches!(
            decode_challenge_response(&buf),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn decode_wrong_tag() {
        let mut buf = [0u8; CHALLENGE_RESPONSE_LEN];
        buf[0] = 99; // not TAG_CHALLENGE_RESPONSE
        assert!(matches!(
            decode_challenge_response(&buf),
            Err(ProtocolError::UnknownTag(99))
        ));
    }
}
