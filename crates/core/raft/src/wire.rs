//! Wire format for control-plane raft peer traffic.
//!
//! One frame per raft [`Message`]:
//!
//! ```text
//! [payload_len u32 LE][crc32c u32 LE][payload]
//! payload:
//!   [tip_epoch u64][tip_last_sequence u64][chain_hash 32 bytes]
//!   [prost-encoded eraftpb.Message]
//! ```
//!
//! The envelope prefixes every message with the sender's **journal
//! tip** (epoch + last sequence + chain hash) so the receiver can apply
//! the recency vote filter ([`crate::recency`]) without touching the
//! raft payload. The chain hash rides along for step-3 divergence
//! diagnostics; it is not compared here.
//!
//! prost stays an implementation detail of this crate: peers exchange
//! opaque payload bytes, and the CRC (same crc32c as the journal)
//! rejects corruption before decoding.

use std::io;

use prost::Message as _;
use raft::eraftpb::Message;

use crate::recency::JournalTip;

/// Hard cap on a frame payload. Control-plane messages are small
/// (votes, heartbeats, config entries); snapshots are metadata-only in
/// step 1 and config-sized in step 2. Far above both, small enough
/// that a corrupt length prefix cannot balloon a buffer.
pub const MAX_FRAME: usize = 4 << 20;

/// Fixed envelope prefix: epoch u64 + last_sequence u64 + 32-byte
/// chain hash.
const ENVELOPE_PREFIX: usize = 8 + 8 + 32;
/// Frame header: payload length u32 + crc32c u32.
const FRAME_HEADER: usize = 4 + 4;

/// A decoded peer frame.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    /// Sender's journal tip at send time.
    pub tip: JournalTip,
    /// Sender's journal chain hash at the tip (diagnostics only).
    pub chain_hash: [u8; 32],
    /// The raft message itself.
    pub message: Message,
}

/// Append one framed envelope to `buf`.
pub fn encode_frame(tip: JournalTip, chain_hash: &[u8; 32], message: &Message, buf: &mut Vec<u8>) {
    let payload_len = ENVELOPE_PREFIX + message.encoded_len();
    buf.reserve(FRAME_HEADER + payload_len);
    buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
    let crc_pos = buf.len();
    buf.extend_from_slice(&[0u8; 4]); // crc placeholder
    let payload_start = buf.len();
    buf.extend_from_slice(&tip.epoch.to_le_bytes());
    buf.extend_from_slice(&tip.last_sequence.to_le_bytes());
    buf.extend_from_slice(chain_hash);
    // Infallible: encoding into a `Vec` cannot hit a capacity error.
    message
        .encode(buf)
        .expect("prost encode into Vec is infallible");
    let crc = crc32c::crc32c(&buf[payload_start..]);
    buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Outcome of scanning a receive buffer for one frame.
#[derive(Debug)]
pub enum FrameScan {
    /// A complete, CRC-valid frame: the decoded envelope plus the
    /// total number of buffer bytes it consumed (drain this many).
    Complete(Box<Envelope>, usize),
    /// More bytes needed.
    Incomplete,
}

/// Try to extract and decode one frame from the front of `buf`.
///
/// Errors are terminal for the connection (oversized length, CRC
/// mismatch, undecodable payload) — the caller drops the peer link and
/// lets it re-establish, the same policy as the replication receiver.
pub fn scan_frame(buf: &[u8]) -> io::Result<FrameScan> {
    if buf.len() < FRAME_HEADER {
        return Ok(FrameScan::Incomplete);
    }
    let payload_len = u32::from_le_bytes(buf[..4].try_into().expect("len 4 slice")) as usize;
    if payload_len > MAX_FRAME {
        return Err(io::Error::other(format!(
            "raft frame of {payload_len} bytes exceeds the {MAX_FRAME} cap"
        )));
    }
    if payload_len < ENVELOPE_PREFIX {
        return Err(io::Error::other(format!(
            "raft frame of {payload_len} bytes is shorter than the envelope prefix"
        )));
    }
    let total = FRAME_HEADER + payload_len;
    if buf.len() < total {
        return Ok(FrameScan::Incomplete);
    }
    let stored_crc = u32::from_le_bytes(buf[4..8].try_into().expect("len 4 slice"));
    let payload = &buf[FRAME_HEADER..total];
    let actual_crc = crc32c::crc32c(payload);
    if stored_crc != actual_crc {
        return Err(io::Error::other(format!(
            "raft frame CRC mismatch (stored {stored_crc:#010x}, computed {actual_crc:#010x})"
        )));
    }

    let epoch = u64::from_le_bytes(payload[..8].try_into().expect("len 8 slice"));
    let last_sequence = u64::from_le_bytes(payload[8..16].try_into().expect("len 8 slice"));
    let mut chain_hash = [0u8; 32];
    chain_hash.copy_from_slice(&payload[16..48]);
    let message = Message::decode(&payload[ENVELOPE_PREFIX..])
        .map_err(|e| io::Error::other(format!("undecodable raft message: {e}")))?;

    Ok(FrameScan::Complete(
        Box::new(Envelope {
            tip: JournalTip {
                epoch,
                last_sequence,
            },
            chain_hash,
            message,
        }),
        total,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::MessageType;

    fn sample_message() -> Message {
        let mut m = Message::default();
        m.set_msg_type(MessageType::MsgRequestVote);
        m.from = 2;
        m.to = 3;
        m.term = 7;
        m
    }

    fn sample_tip() -> JournalTip {
        JournalTip {
            epoch: 4,
            last_sequence: 12_345,
        }
    }

    #[test]
    fn roundtrip() {
        let mut buf = Vec::new();
        encode_frame(sample_tip(), &[0xAB; 32], &sample_message(), &mut buf);
        match scan_frame(&buf).unwrap() {
            FrameScan::Complete(env, consumed) => {
                assert_eq!(consumed, buf.len());
                assert_eq!(env.tip, sample_tip());
                assert_eq!(env.chain_hash, [0xAB; 32]);
                assert_eq!(env.message, sample_message());
            }
            FrameScan::Incomplete => panic!("expected a complete frame"),
        }
    }

    #[test]
    fn partial_frames_wait_for_more_bytes() {
        let mut buf = Vec::new();
        encode_frame(sample_tip(), &[0; 32], &sample_message(), &mut buf);
        for cut in [0, 3, FRAME_HEADER, buf.len() - 1] {
            assert!(
                matches!(scan_frame(&buf[..cut]).unwrap(), FrameScan::Incomplete),
                "cut at {cut}"
            );
        }
    }

    #[test]
    fn two_frames_extract_in_order() {
        let mut second = sample_message();
        second.term = 8;
        let mut buf = Vec::new();
        encode_frame(sample_tip(), &[0; 32], &sample_message(), &mut buf);
        encode_frame(sample_tip(), &[0; 32], &second, &mut buf);

        let FrameScan::Complete(first_env, consumed) = scan_frame(&buf).unwrap() else {
            panic!("first frame incomplete");
        };
        assert_eq!(first_env.message.term, 7);
        let FrameScan::Complete(second_env, rest) = scan_frame(&buf[consumed..]).unwrap() else {
            panic!("second frame incomplete");
        };
        assert_eq!(second_env.message.term, 8);
        assert_eq!(consumed + rest, buf.len());
    }

    #[test]
    fn corrupted_payload_is_rejected() {
        let mut buf = Vec::new();
        encode_frame(sample_tip(), &[0; 32], &sample_message(), &mut buf);
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        assert!(scan_frame(&buf).is_err());
    }

    #[test]
    fn oversized_length_is_rejected() {
        let mut buf = ((MAX_FRAME + 1) as u32).to_le_bytes().to_vec();
        buf.extend_from_slice(&[0u8; 64]);
        assert!(scan_frame(&buf).is_err());
    }

    #[test]
    fn undersized_length_is_rejected() {
        // Payload length below the envelope prefix can't be a frame.
        let mut buf = 4u32.to_le_bytes().to_vec();
        buf.extend_from_slice(&crc32c::crc32c(&[0u8; 4]).to_le_bytes());
        buf.extend_from_slice(&[0u8; 4]);
        assert!(scan_frame(&buf).is_err());
    }
}
