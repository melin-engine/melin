//! Codec for transport-level control frames.
//!
//! Encodes/decodes [`TransportResponse`] and [`ChallengeResponse`]
//! using the same wire format and tag values as `melin-protocol`'s
//! full codec, so the two are interchangeable on the wire.

use crate::control::{ChallengeResponse, TransportResponse};
use crate::error::ProtocolError;

// Wire tags — must stay in sync with the values in melin-protocol's
// codec. The transport-level subset lives here; domain-level tags
// (SubmitOrder, Placed, etc.) stay in the exchange crate.
const TAG_CHALLENGE_RESPONSE: u8 = 5;
const TAG_ENGINE_ERROR: u8 = 16;
const TAG_BATCH_END: u8 = 17;
const TAG_SERVER_READY: u8 = 18;
const TAG_RESPONSE_HEARTBEAT: u8 = 19;
const TAG_CHALLENGE: u8 = 20;
const TAG_AUTH_FAILED: u8 = 21;
const TAG_SERVER_BUSY: u8 = 24;

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
        TransportResponse::Challenge { .. } => 4 + 1 + 32,
        _ => 4 + 1,
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

/// Decode a client's auth challenge-response from a wire frame.
///
/// `buf` must contain the frame payload *after* the 4-byte length
/// prefix has been stripped: `[seq:u64][tag:u8][signature:64][pubkey:32]`.
///
/// Returns `(request_seq, ChallengeResponse)`. Returns
/// `Err(UnknownTag)` if the tag is not `TAG_CHALLENGE_RESPONSE`.
pub fn decode_challenge_response(buf: &[u8]) -> Result<(u64, ChallengeResponse), ProtocolError> {
    // seq(8) + tag(1) + signature(64) + public_key(32) = 105
    if buf.len() < 105 {
        return Err(ProtocolError::Truncated);
    }

    let seq = u64::from_le_bytes(buf[..8].try_into().unwrap());
    let tag = buf[8];

    if tag != TAG_CHALLENGE_RESPONSE {
        return Err(ProtocolError::UnknownTag(tag));
    }

    let payload = &buf[9..];
    let mut signature = [0u8; 64];
    signature.copy_from_slice(&payload[..64]);
    let mut public_key = [0u8; 32];
    public_key.copy_from_slice(&payload[64..96]);

    Ok((
        seq,
        ChallengeResponse {
            signature,
            public_key,
        },
    ))
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

        // Build wire frame: [seq:u64][tag:u8][sig:64][pubkey:32]
        let mut buf = [0u8; 105];
        buf[..8].copy_from_slice(&42u64.to_le_bytes());
        buf[8] = TAG_CHALLENGE_RESPONSE;
        buf[9..73].copy_from_slice(&sig);
        buf[73..105].copy_from_slice(&pubkey);

        let (seq, cr) = decode_challenge_response(&buf).unwrap();
        assert_eq!(seq, 42);
        assert_eq!(cr.signature, sig);
        assert_eq!(cr.public_key, pubkey);
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
        let mut buf = [0u8; 105];
        buf[..8].copy_from_slice(&1u64.to_le_bytes());
        buf[8] = 99; // not TAG_CHALLENGE_RESPONSE
        assert!(matches!(
            decode_challenge_response(&buf),
            Err(ProtocolError::UnknownTag(99))
        ));
    }
}
