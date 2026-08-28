//! What goes on the wire, and what comes back.
//!
//! A request is `[len: u32 LE][request_seq: u64 LE][TAG_ECHO][body]`, and
//! the body -- which the server echoes back untouched -- is what carries
//! the measurement:
//!
//! ```text
//! [scheduled tick: u64 LE][request_seq: u64 LE][zero fill][checksum: u64 LE]
//! ```
//!
//! The tick is when the schedule said the request should go, so the
//! latency counted from it includes any delay in getting it out. The
//! sequence lets a reply be matched to its request and lets a lost or
//! reordered reply be caught rather than counted. The checksum, a fixed
//! value in the last eight bytes, is what the Aeron benchmark harness
//! puts there too; it catches a reply whose body is not the one sent.
//!
//! The minimum body is three words, where the Aeron harness accepts two:
//! it recovers the sequence from ordering alone, and this client would
//! rather carry it and check.

use echo_server::{MAX_PAYLOAD, TAG_ECHO, TAG_RESP_ECHO, TAG_RESP_REJECTED};
use melin_wire_protocol::control_codec::{
    TAG_AUTH_FAILED, TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_ENGINE_ERROR,
    TAG_RESPONSE_HEARTBEAT, TAG_SERVER_BUSY, TAG_SERVER_READY,
};

/// Bytes of length prefix in front of every frame, both directions.
pub const LENGTH_PREFIX: usize = 4;
/// Bytes of request header after the prefix: the sequence and the tag.
pub const REQUEST_HEADER: usize = 8 + 1;
/// Smallest body that carries the tick, the sequence and the checksum.
pub const MIN_BODY: usize = 3 * 8;
/// Largest body the server takes.
pub const MAX_BODY: usize = MAX_PAYLOAD;
/// The value in the last eight bytes of every body. Arbitrary, fixed.
pub const CHECKSUM: u64 = 0xA5C3_5EED_0BAD_F00D;
/// A frame longer than this is not one of ours and its length is refused
/// before it drives any buffering.
pub const MAX_FRAME_PAYLOAD: usize = 4096;

const SEQ_OFFSET: usize = LENGTH_PREFIX;
const TAG_OFFSET: usize = SEQ_OFFSET + 8;
const BODY_OFFSET: usize = TAG_OFFSET + 1;
const TICK_OFFSET: usize = BODY_OFFSET;
const INDEX_OFFSET: usize = TICK_OFFSET + 8;

/// One request frame, built once and re-stamped per send. Only the
/// sequence (in the header and the body) and the tick change between
/// sends; the rest of the bytes are written at construction.
pub struct RequestFrame {
    bytes: Vec<u8>,
}

impl RequestFrame {
    pub fn new(body: usize) -> Result<Self, String> {
        if !(MIN_BODY..=MAX_BODY).contains(&body) {
            return Err(format!(
                "message length {body} is outside {MIN_BODY}..={MAX_BODY} bytes"
            ));
        }
        let mut bytes = vec![0u8; LENGTH_PREFIX + REQUEST_HEADER + body];
        // The prefix is four bytes on the wire and the value is bounded
        // by MAX_BODY, so the narrowing is exact.
        let payload_len = (REQUEST_HEADER + body) as u32;
        bytes[..LENGTH_PREFIX].copy_from_slice(&payload_len.to_le_bytes());
        bytes[TAG_OFFSET] = TAG_ECHO;
        let end = bytes.len();
        bytes[end - 8..].copy_from_slice(&CHECKSUM.to_le_bytes());
        Ok(Self { bytes })
    }

    /// Set the sequence -- in the header, where the server reads it, and in
    /// the body, where it comes back -- and the scheduled tick.
    #[inline(always)]
    pub fn stamp(&mut self, seq: u64, tick: u64) {
        self.bytes[SEQ_OFFSET..SEQ_OFFSET + 8].copy_from_slice(&seq.to_le_bytes());
        self.bytes[TICK_OFFSET..TICK_OFFSET + 8].copy_from_slice(&tick.to_le_bytes());
        self.bytes[INDEX_OFFSET..INDEX_OFFSET + 8].copy_from_slice(&seq.to_le_bytes());
    }

    #[inline(always)]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// The frame `[seq: 0][TAG_CHALLENGE_RESPONSE][signature][public key]`,
/// length prefix included.
pub fn auth_response(signature: &[u8; 64], public_key: &[u8; 32]) -> Vec<u8> {
    let payload_len = 8 + 1 + 64 + 32;
    let mut frame = Vec::with_capacity(LENGTH_PREFIX + payload_len);
    frame.extend_from_slice(&(payload_len as u32).to_le_bytes());
    frame.extend_from_slice(&0u64.to_le_bytes());
    frame.push(TAG_CHALLENGE_RESPONSE);
    frame.extend_from_slice(signature);
    frame.extend_from_slice(public_key);
    frame
}

/// A decoded inbound frame payload (length prefix already stripped).
pub enum Frame<'a> {
    Echo {
        tick: u64,
        seq: u64,
        checksum: u64,
        body_len: usize,
    },
    BatchEnd,
    Heartbeat,
    Challenge(&'a [u8]),
    ServerReady,
    AuthFailed,
    Rejected,
    EngineError,
    Busy,
    /// An echo too short to carry a measurement: not a reply to anything
    /// this client sent.
    Malformed,
    Empty,
    Unknown(u8),
}

pub fn decode(payload: &[u8]) -> Frame<'_> {
    let Some((&tag, body)) = payload.split_first() else {
        return Frame::Empty;
    };
    match tag {
        TAG_RESP_ECHO => {
            if body.len() < MIN_BODY {
                return Frame::Malformed;
            }
            Frame::Echo {
                tick: u64_at(body, 0),
                seq: u64_at(body, 8),
                checksum: u64_at(body, body.len() - 8),
                body_len: body.len(),
            }
        }
        TAG_BATCH_END => Frame::BatchEnd,
        TAG_RESPONSE_HEARTBEAT => Frame::Heartbeat,
        TAG_CHALLENGE => Frame::Challenge(body),
        TAG_SERVER_READY => Frame::ServerReady,
        TAG_AUTH_FAILED => Frame::AuthFailed,
        TAG_RESP_REJECTED => Frame::Rejected,
        TAG_ENGINE_ERROR => Frame::EngineError,
        TAG_SERVER_BUSY => Frame::Busy,
        other => Frame::Unknown(other),
    }
}

#[inline(always)]
fn u64_at(bytes: &[u8], at: usize) -> u64 {
    let mut word = [0u8; 8];
    word.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(word)
}

/// Bytes received and not yet consumed, with the frames cut out of them
/// in place. One fixed allocation: the transport reads into `space()`,
/// `filled()` accounts for what it read, and `pop()` hands out complete
/// frames until only a partial one is left. Compaction is a copy of that
/// partial tail to the front, and happens only when the space would
/// otherwise run out.
pub struct Inbound {
    buf: Vec<u8>,
    /// Bytes of `buf` that hold received data.
    len: usize,
    /// Start of the data not yet handed out by `pop`.
    cursor: usize,
}

impl Inbound {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buf: vec![0u8; capacity],
            len: 0,
            cursor: 0,
        }
    }

    /// Where the transport may write next. Empty only if a single frame
    /// larger than the whole buffer is pending, which `MAX_FRAME_PAYLOAD`
    /// rules out for any buffer larger than it.
    pub fn space(&mut self) -> &mut [u8] {
        if self.cursor == self.len {
            self.cursor = 0;
            self.len = 0;
        } else if self.len == self.buf.len() && self.cursor > 0 {
            self.buf.copy_within(self.cursor..self.len, 0);
            self.len -= self.cursor;
            self.cursor = 0;
        }
        &mut self.buf[self.len..]
    }

    /// `n` bytes were written at the start of the slice `space` returned.
    pub fn filled(&mut self, n: usize) {
        debug_assert!(self.len + n <= self.buf.len());
        self.len += n;
    }

    /// The next complete frame's payload, or `None` if the data on hand
    /// ends mid-frame. A length that no frame of ours can have is an
    /// error: the stream is not one we can parse.
    pub fn pop(&mut self) -> Result<Option<&[u8]>, String> {
        let pending = &self.buf[self.cursor..self.len];
        if pending.len() < LENGTH_PREFIX {
            return Ok(None);
        }
        let payload_len =
            u32::from_le_bytes([pending[0], pending[1], pending[2], pending[3]]) as usize;
        if payload_len == 0 || payload_len > MAX_FRAME_PAYLOAD {
            return Err(format!("frame of {payload_len} bytes is not plausible"));
        }
        if pending.len() < LENGTH_PREFIX + payload_len {
            return Ok(None);
        }
        let start = self.cursor + LENGTH_PREFIX;
        let end = start + payload_len;
        self.cursor = end;
        Ok(Some(&self.buf[start..end]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_round_trips_through_the_decoder() {
        let mut frame = RequestFrame::new(MIN_BODY).unwrap();
        frame.stamp(42, 0xDEAD_BEEF);
        let bytes = frame.bytes();
        assert_eq!(bytes.len(), LENGTH_PREFIX + REQUEST_HEADER + MIN_BODY);
        assert_eq!(u64_at(bytes, SEQ_OFFSET), 42, "sequence in the header");
        assert_eq!(bytes[TAG_OFFSET], TAG_ECHO);

        // What the server sends back: the body, behind its own tag.
        let mut reply = vec![TAG_RESP_ECHO];
        reply.extend_from_slice(&bytes[BODY_OFFSET..]);
        match decode(&reply) {
            Frame::Echo {
                tick,
                seq,
                checksum,
                body_len,
            } => {
                assert_eq!(tick, 0xDEAD_BEEF);
                assert_eq!(seq, 42);
                assert_eq!(checksum, CHECKSUM);
                assert_eq!(body_len, MIN_BODY);
            }
            _ => panic!("not an echo"),
        }
    }

    #[test]
    fn restamping_changes_only_the_sequence_and_the_tick() {
        let mut a = RequestFrame::new(64).unwrap();
        let mut b = RequestFrame::new(64).unwrap();
        a.stamp(1, 100);
        b.stamp(2, 200);
        let differ: Vec<usize> = a
            .bytes()
            .iter()
            .zip(b.bytes())
            .enumerate()
            .filter(|(_, (x, y))| x != y)
            .map(|(i, _)| i)
            .collect();
        assert!(differ.iter().all(|&i| {
            (SEQ_OFFSET..SEQ_OFFSET + 8).contains(&i)
                || (TICK_OFFSET..TICK_OFFSET + 8).contains(&i)
                || (INDEX_OFFSET..INDEX_OFFSET + 8).contains(&i)
        }));
    }

    #[test]
    fn body_lengths_outside_the_bounds_are_refused() {
        assert!(RequestFrame::new(MIN_BODY - 1).is_err());
        assert!(RequestFrame::new(MAX_BODY + 1).is_err());
        assert!(RequestFrame::new(MAX_BODY).is_ok());
    }

    #[test]
    fn control_tags_decode_and_a_short_echo_is_malformed() {
        assert!(matches!(decode(&[TAG_BATCH_END]), Frame::BatchEnd));
        assert!(matches!(
            decode(&[TAG_RESPONSE_HEARTBEAT]),
            Frame::Heartbeat
        ));
        assert!(matches!(decode(&[TAG_SERVER_READY]), Frame::ServerReady));
        assert!(matches!(decode(&[TAG_AUTH_FAILED]), Frame::AuthFailed));
        assert!(matches!(decode(&[TAG_RESP_REJECTED]), Frame::Rejected));
        assert!(matches!(decode(&[TAG_ENGINE_ERROR]), Frame::EngineError));
        assert!(matches!(decode(&[TAG_SERVER_BUSY]), Frame::Busy));
        assert!(matches!(decode(&[]), Frame::Empty));
        assert!(matches!(decode(&[0x7F]), Frame::Unknown(0x7F)));
        let short = [TAG_RESP_ECHO, 1, 2, 3];
        assert!(matches!(decode(&short), Frame::Malformed));
        let mut challenge = vec![TAG_CHALLENGE];
        challenge.extend_from_slice(&[9u8; 32]);
        assert!(matches!(decode(&challenge), Frame::Challenge(nonce) if nonce == [9u8; 32]));
    }

    #[test]
    fn the_auth_response_is_the_documented_frame() {
        let frame = auth_response(&[7u8; 64], &[8u8; 32]);
        assert_eq!(frame.len(), LENGTH_PREFIX + 8 + 1 + 64 + 32);
        assert_eq!(
            u32::from_le_bytes([frame[0], frame[1], frame[2], frame[3]]),
            105
        );
        assert_eq!(u64_at(&frame, 4), 0, "the auth frame has sequence zero");
        assert_eq!(frame[12], TAG_CHALLENGE_RESPONSE);
        assert_eq!(&frame[13..77], &[7u8; 64]);
        assert_eq!(&frame[77..], &[8u8; 32]);
    }

    /// Two frames delivered in three pieces, the second cut mid-prefix.
    #[test]
    fn inbound_reassembles_frames_across_partial_reads() {
        let mut inbound = Inbound::with_capacity(64);
        let one = [3u8, 0, 0, 0, TAG_BATCH_END, 0xAA, 0xBB];
        let two = [1u8, 0, 0, 0, TAG_SERVER_READY];
        let stream: Vec<u8> = one.iter().chain(&two).copied().collect();

        let pieces = [&stream[..5], &stream[5..9], &stream[9..]];
        let mut popped = Vec::new();
        for piece in pieces {
            inbound.space()[..piece.len()].copy_from_slice(piece);
            inbound.filled(piece.len());
            while let Some(payload) = inbound.pop().unwrap() {
                popped.push(payload.to_vec());
            }
        }
        assert_eq!(popped, vec![one[4..].to_vec(), two[4..].to_vec()]);
    }

    #[test]
    fn inbound_compacts_a_partial_tail_only_when_it_must() {
        let mut inbound = Inbound::with_capacity(8);
        // A complete 1-byte frame followed by the first two bytes of the next.
        let bytes = [1u8, 0, 0, 0, TAG_BATCH_END, 2, 0];
        inbound.space()[..bytes.len()].copy_from_slice(&bytes);
        inbound.filled(bytes.len());
        assert!(inbound.pop().unwrap().is_some());
        assert!(inbound.pop().unwrap().is_none(), "the tail is incomplete");
        // One byte of space left, no compaction yet.
        assert_eq!(inbound.space().len(), 1);
        inbound.filled(1);
        // Now full: the tail moves to the front and the space reopens.
        assert_eq!(inbound.space().len(), 8 - 3);
        assert_eq!(inbound.cursor, 0);
        assert_eq!(&inbound.buf[..3], &[2, 0, 0]);
    }

    #[test]
    fn an_implausible_length_is_an_error_not_a_wait() {
        let mut inbound = Inbound::with_capacity(16);
        let too_long = ((MAX_FRAME_PAYLOAD + 1) as u32).to_le_bytes();
        inbound.space()[..4].copy_from_slice(&too_long);
        inbound.filled(4);
        assert!(inbound.pop().is_err());
        let mut inbound = Inbound::with_capacity(16);
        inbound.space()[..4].copy_from_slice(&[0, 0, 0, 0]);
        inbound.filled(4);
        assert!(
            inbound.pop().is_err(),
            "a zero-length frame is not one of ours"
        );
    }
}
