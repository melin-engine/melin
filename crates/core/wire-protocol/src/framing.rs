//! I/O-free framing: split length-prefixed frames out of bytes the caller
//! already holds.
//!
//! The wire format is a 4-byte little-endian payload length followed by
//! that many payload bytes. [`BlockingFrameReader`] reads it straight off
//! a `std::io::Read`; this module is for programs that own their I/O loop
//! instead (io_uring completions, DPDK receive bursts, a non-blocking
//! socket polled by hand) and only need to be told where the frames are
//! in the bytes they have received so far.
//!
//! Two layers:
//!
//! - [`split_frame`] / [`split_frame_limited`]: stateless and zero-copy.
//!   They look at the front of a caller-owned buffer and return the first
//!   complete frame as a borrowed slice, `None` if more bytes are needed,
//!   or an error if the length prefix declares more than the limit. The
//!   caller keeps its own buffer, cursor and compaction policy, so a
//!   receive loop on the hot path pays nothing beyond the bounds checks
//!   it would write by hand.
//! - [`FrameDecoder`]: an owning buffer built on the splitter, for a
//!   caller that would rather push received chunks and pull frames out.
//!
//! A receive loop over its own buffer:
//!
//! ```
//! use melin_wire_protocol::framing::{split_frame, FrameTooLarge};
//!
//! fn on_receive(buf: &mut Vec<u8>, frames: &mut Vec<Vec<u8>>) -> Result<(), FrameTooLarge> {
//!     let mut cursor = 0;
//!     while let Some(frame) = split_frame(&buf[cursor..])? {
//!         frames.push(frame.payload.to_vec());
//!         cursor += frame.consumed();
//!     }
//!     // Keep the partial frame, if any, for the next receive.
//!     buf.drain(..cursor);
//!     Ok(())
//! }
//!
//! let mut buf = vec![2, 0, 0, 0, b'h', b'i', 3, 0];
//! let mut frames = Vec::new();
//! on_receive(&mut buf, &mut frames)?;
//! assert_eq!(frames, [b"hi".to_vec()]);
//! assert_eq!(buf, [3, 0]);
//!
//! // A prefix declaring more than the limit fails before its payload arrives.
//! buf.extend_from_slice(&[0xFF, 0xFF]);
//! assert!(on_receive(&mut buf, &mut frames).is_err());
//! # Ok::<(), FrameTooLarge>(())
//! ```
//!
//! # Oversized frames
//!
//! A length above the limit is rejected as soon as the 4 prefix bytes are
//! readable, before any payload arrives. A peer that sends a corrupt or
//! hostile prefix is therefore refused immediately instead of leaving the
//! receiver buffering towards a length it will never accept. Past such a
//! prefix the byte stream has no trustworthy frame boundary left: the
//! connection should be closed, not resynchronised.
//!
//! # Empty frames
//!
//! A zero length is a valid frame with an empty payload: it consumes the
//! 4 prefix bytes and nothing else. Framing does not judge content; a
//! protocol that has no use for an empty frame rejects it one layer up,
//! where it already inspects the payload.
//!
//! [`BlockingFrameReader`]: crate::blocking::BlockingFrameReader

use std::fmt;

use crate::blocking::MAX_FRAME_SIZE;

/// Size of the length prefix in front of every frame's payload.
pub const PREFIX_LEN: usize = 4;

/// A complete frame found at the front of a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Frame<'a> {
    /// The payload, borrowed from the buffer, without its length prefix.
    pub payload: &'a [u8],
}

impl Frame<'_> {
    /// Bytes this frame occupies at the front of the buffer, prefix
    /// included: advance the read cursor by this much to reach the next
    /// frame.
    #[inline]
    pub fn consumed(&self) -> usize {
        PREFIX_LEN + self.payload.len()
    }
}

/// A length prefix declared more payload than the limit allows.
///
/// The framing of the stream it came from is no longer trustworthy; the
/// connection should be dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameTooLarge {
    /// The payload length the prefix declared. `u32` because that is the
    /// prefix's width on the wire: the value is reported exactly as
    /// received, whatever the platform's `usize`.
    pub declared: u32,
    /// The limit it exceeded. `usize` because it echoes the `max_payload`
    /// the caller passed, which bounds a buffer length.
    pub max: usize,
}

impl fmt::Display for FrameTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "frame too large: {} bytes (max {})",
            self.declared, self.max
        )
    }
}

impl std::error::Error for FrameTooLarge {}

impl From<FrameTooLarge> for std::io::Error {
    /// An oversized frame is malformed input, the same kind
    /// [`BlockingFrameReader`](crate::blocking::BlockingFrameReader)
    /// reports for it.
    fn from(e: FrameTooLarge) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e)
    }
}

/// Split the first frame off the front of `buf`, with the protocol's
/// [`MAX_FRAME_SIZE`] as the payload limit.
///
/// See [`split_frame_limited`].
#[inline]
pub fn split_frame(buf: &[u8]) -> Result<Option<Frame<'_>>, FrameTooLarge> {
    split_frame_limited(buf, MAX_FRAME_SIZE)
}

/// Split the first frame off the front of `buf`, refusing a payload
/// longer than `max_payload` bytes.
///
/// - `Ok(Some(frame))`: a complete frame starts at `buf[0]`; the next one
///   starts at `buf[frame.consumed()]`.
/// - `Ok(None)`: `buf` holds less than one frame; keep its bytes and call
///   again once more have arrived.
/// - `Err(_)`: the prefix declares more than `max_payload`. Returned as
///   soon as the 4 prefix bytes are present, however few payload bytes
///   follow them.
///
/// Bytes after the first frame are not looked at. The function holds no
/// state, so the caller can resume with any suffix of its buffer that
/// starts on a frame boundary.
#[inline]
pub fn split_frame_limited(
    buf: &[u8],
    max_payload: usize,
) -> Result<Option<Frame<'_>>, FrameTooLarge> {
    let Some((prefix, rest)) = buf.split_first_chunk::<PREFIX_LEN>() else {
        return Ok(None);
    };
    let declared = u32::from_le_bytes(*prefix);
    // A length that does not fit `usize` exceeds any limit expressible in
    // one; on every supported target the conversion is lossless and this
    // folds into the plain comparison.
    let len = match usize::try_from(declared) {
        Ok(len) if len <= max_payload => len,
        _ => {
            return Err(FrameTooLarge {
                declared,
                max: max_payload,
            });
        }
    };
    Ok(rest.get(..len).map(|payload| Frame { payload }))
}

/// An owning frame buffer: push received bytes in, take complete frames
/// out.
///
/// Built on [`split_frame_limited`]. Frames come out as raw payloads, so
/// a caller can route them as it likes: handshake frames to the
/// handshake, reply frames to whatever classifies them afterwards.
///
/// Consumed bytes are reclaimed lazily: [`next`](Self::next) only
/// advances a cursor, and a later [`push`](Self::push) moves the
/// unconsumed tail to the front before appending once the consumed prefix
/// is at least as large as that tail. Draining a burst of frames then
/// costs at most one move of the leftover partial frame, and a caller that
/// interleaves pushes with single `next` calls still pays amortised
/// constant work per byte.
///
/// # After an error
///
/// Once [`next`](Self::next) has reported [`FrameTooLarge`] the decoder is
/// poisoned: the stream has no trustworthy frame boundary left. Every
/// later `next` returns the same error, and `push` discards its input
/// rather than buffer bytes that can never be framed. Drop the connection
/// and the decoder with it.
#[derive(Debug)]
pub struct FrameDecoder {
    /// Received bytes; `buf[start..]` is the part not yet returned as a
    /// frame. A `Vec` because the bytes in flight are unbounded in
    /// principle (one push may carry many frames) but steady in practice:
    /// after the first few pushes its capacity settles and no further
    /// allocation happens. A ring buffer would avoid the compaction move,
    /// but could hand out a frame split across its wrap point, which a
    /// borrowed `&[u8]` cannot represent.
    buf: Vec<u8>,
    /// Read cursor into `buf`: everything before it has been returned.
    /// `usize` because it indexes `buf`.
    start: usize,
    /// Largest payload a frame may declare.
    max_payload: usize,
    /// The error that poisoned the decoder, if any. Stored so every later
    /// call reports the original declared length.
    poisoned: Option<FrameTooLarge>,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameDecoder {
    /// A decoder enforcing the protocol's [`MAX_FRAME_SIZE`].
    pub fn new() -> Self {
        Self::with_max_payload(MAX_FRAME_SIZE)
    }

    /// A decoder refusing any frame whose payload exceeds `max_payload`.
    ///
    /// Preallocates room for one maximal frame (capped at one
    /// [`MAX_FRAME_SIZE`] frame, so a generous limit does not reserve
    /// memory up front), sparing the first pushes a reallocation.
    pub fn with_max_payload(max_payload: usize) -> Self {
        Self {
            buf: Vec::with_capacity(PREFIX_LEN + max_payload.min(MAX_FRAME_SIZE)),
            start: 0,
            max_payload,
            poisoned: None,
        }
    }

    /// The payload limit this decoder enforces.
    pub fn max_payload(&self) -> usize {
        self.max_payload
    }

    /// Whether a [`FrameTooLarge`] has poisoned the decoder.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    /// Bytes received but not yet returned as a frame: a partial frame,
    /// or complete frames still waiting for [`next`](Self::next).
    pub fn pending(&self) -> &[u8] {
        &self.buf[self.start..]
    }

    /// Append received bytes.
    ///
    /// Discarded once the decoder is poisoned.
    pub fn push(&mut self, bytes: &[u8]) {
        if self.poisoned.is_some() {
            return;
        }
        let remaining = self.buf.len() - self.start;
        if self.start > 0 && self.start >= remaining {
            // Compact only once the consumed prefix is at least as large as
            // the tail: every byte moved is matched by at least one byte
            // reclaimed, so the cost stays amortised O(1) per byte even for
            // a caller that interleaves single `next` calls with pushes,
            // and the buffer never grows past twice its live bytes plus
            // the push. A fully drained buffer moves nothing.
            // `copy_within` + `truncate` is a single memmove of the tail;
            // `drain(..start)` would do the same with more bookkeeping.
            self.buf.copy_within(self.start.., 0);
            self.buf.truncate(remaining);
            self.start = 0;
        }
        self.buf.extend_from_slice(bytes);
    }

    /// The next complete frame's payload, or `None` until more bytes are
    /// pushed. The slice stays valid until the next call on the decoder.
    ///
    /// Not an `Iterator`: the payload borrows the decoder, and `None`
    /// means "not yet", not "never".
    #[expect(clippy::should_implement_trait)] // cannot be `Iterator::next`, see above
    pub fn next(&mut self) -> Result<Option<&[u8]>, FrameTooLarge> {
        if let Some(e) = self.poisoned {
            return Err(e);
        }
        match split_frame_limited(&self.buf[self.start..], self.max_payload) {
            Ok(Some(frame)) => {
                let payload_start = self.start + PREFIX_LEN;
                self.start += frame.consumed();
                Ok(Some(&self.buf[payload_start..self.start]))
            }
            Ok(None) => Ok(None),
            Err(e) => {
                self.poisoned = Some(e);
                // The bytes can never be framed and `push` discards from
                // here on, so free the allocation now rather than hold it
                // until the decoder is dropped (`clear` would keep it).
                self.buf = Vec::new();
                self.start = 0;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Encode payloads as one length-prefixed stream.
    fn encode(frames: &[Vec<u8>]) -> Vec<u8> {
        let mut out = Vec::new();
        for f in frames {
            out.extend_from_slice(&(f.len() as u32).to_le_bytes());
            out.extend_from_slice(f);
        }
        out
    }

    /// Every frame `split_frame_limited` finds in `buf`, walking a cursor.
    fn split_all(buf: &[u8], max: usize) -> (Vec<Vec<u8>>, usize) {
        let mut cursor = 0;
        let mut out = Vec::new();
        while let Some(frame) = split_frame_limited(&buf[cursor..], max).unwrap() {
            out.push(frame.payload.to_vec());
            cursor += frame.consumed();
        }
        (out, cursor)
    }

    /// Feed `stream` to a decoder in chunks ending at each of `cuts`,
    /// draining after every push.
    fn decode_chunked(stream: &[u8], cuts: &[usize], max: usize) -> Vec<Vec<u8>> {
        let mut dec = FrameDecoder::with_max_payload(max);
        let mut out = Vec::new();
        let mut prev = 0;
        for &cut in cuts.iter().chain(std::iter::once(&stream.len())) {
            dec.push(&stream[prev..cut]);
            prev = cut;
            while let Some(p) = dec.next().unwrap() {
                out.push(p.to_vec());
            }
        }
        assert!(dec.pending().is_empty());
        out
    }

    // --- split_frame ---

    #[test]
    fn empty_and_partial_prefix_need_more() {
        assert_eq!(split_frame(&[]), Ok(None));
        assert_eq!(split_frame(&[3]), Ok(None));
        assert_eq!(split_frame(&[3, 0, 0]), Ok(None));
    }

    #[test]
    fn prefix_without_payload_needs_more() {
        assert_eq!(split_frame(&[3, 0, 0, 0]), Ok(None));
        assert_eq!(split_frame(&[3, 0, 0, 0, 0xAA, 0xBB]), Ok(None));
    }

    #[test]
    fn complete_frame_ignores_trailing_bytes() {
        let buf = [3, 0, 0, 0, 0xAA, 0xBB, 0xCC, 0xFF, 0xFF];
        let frame = split_frame(&buf).unwrap().unwrap();
        assert_eq!(frame.payload, &[0xAA, 0xBB, 0xCC]);
        assert_eq!(frame.consumed(), 7);
    }

    #[test]
    fn zero_length_frame_is_an_empty_payload() {
        let buf = [0, 0, 0, 0, 9];
        let frame = split_frame(&buf).unwrap().unwrap();
        assert!(frame.payload.is_empty());
        assert_eq!(frame.consumed(), PREFIX_LEN);
    }

    #[test]
    fn exact_limit_succeeds() {
        let stream = encode(&[vec![7u8; MAX_FRAME_SIZE]]);
        let frame = split_frame(&stream).unwrap().unwrap();
        assert_eq!(frame.payload.len(), MAX_FRAME_SIZE);
        assert_eq!(frame.consumed(), stream.len());
    }

    #[test]
    fn oversized_fails_on_the_prefix_alone() {
        let declared = MAX_FRAME_SIZE as u32 + 1;
        assert_eq!(
            split_frame(&declared.to_le_bytes()),
            Err(FrameTooLarge {
                declared,
                max: MAX_FRAME_SIZE
            })
        );
    }

    #[test]
    fn max_u32_prefix_reports_the_declared_length() {
        assert_eq!(
            split_frame(&[0xFF; 4]),
            Err(FrameTooLarge {
                declared: u32::MAX,
                max: MAX_FRAME_SIZE
            })
        );
    }

    #[test]
    fn custom_limit_is_respected() {
        let at = encode(&[vec![1u8; 256]]);
        assert_eq!(
            split_frame_limited(&at, 256)
                .unwrap()
                .unwrap()
                .payload
                .len(),
            256
        );
        let over = encode(&[vec![1u8; 257]]);
        assert_eq!(
            split_frame_limited(&over[..PREFIX_LEN], 256),
            Err(FrameTooLarge {
                declared: 257,
                max: 256
            })
        );
        // The same frame is fine under the default limit.
        assert!(split_frame(&over).unwrap().is_some());
    }

    #[test]
    fn zero_limit_admits_only_empty_frames() {
        assert!(split_frame_limited(&[0, 0, 0, 0], 0).unwrap().is_some());
        assert_eq!(
            split_frame_limited(&[1, 0, 0, 0], 0),
            Err(FrameTooLarge {
                declared: 1,
                max: 0
            })
        );
    }

    #[test]
    fn limit_above_u32_admits_every_prefix() {
        assert_eq!(split_frame_limited(&[0xFF; 4], usize::MAX), Ok(None));
    }

    #[test]
    fn error_converts_to_invalid_data() {
        let e: std::io::Error = FrameTooLarge {
            declared: 2000,
            max: 1024,
        }
        .into();
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(e.to_string(), "frame too large: 2000 bytes (max 1024)");
    }

    /// Every prefix of a valid stream splits into a prefix of its frames
    /// and stops exactly at the last frame boundary it contains.
    #[test]
    fn every_truncation_yields_whole_frames_only() {
        let frames = vec![vec![], vec![1], vec![2, 3, 4], vec![], vec![5; 40]];
        let stream = encode(&frames);
        for end in 0..=stream.len() {
            let (got, consumed) = split_all(&stream[..end], MAX_FRAME_SIZE);
            assert_eq!(got, frames[..got.len()], "end {end}");
            assert_eq!(consumed, encode(&got).len(), "end {end}");
            // Whatever is left is less than the next frame.
            if got.len() < frames.len() {
                assert!(end - consumed < PREFIX_LEN + frames[got.len()].len());
            }
        }
    }

    // --- FrameDecoder ---

    #[test]
    fn decoder_splits_at_every_boundary() {
        let frames = vec![vec![], vec![1], vec![2, 3, 4], vec![], vec![5; 40]];
        let stream = encode(&frames);
        for cut in 0..=stream.len() {
            assert_eq!(decode_chunked(&stream, &[cut], MAX_FRAME_SIZE), frames);
        }
        for a in 0..=stream.len() {
            for b in a..=stream.len() {
                assert_eq!(decode_chunked(&stream, &[a, b], MAX_FRAME_SIZE), frames);
            }
        }
    }

    #[test]
    fn decoder_byte_at_a_time() {
        let frames = vec![vec![9; 3], vec![], vec![8; MAX_FRAME_SIZE]];
        let stream = encode(&frames);
        let cuts: Vec<usize> = (1..stream.len()).collect();
        assert_eq!(decode_chunked(&stream, &cuts, MAX_FRAME_SIZE), frames);
    }

    #[test]
    fn decoder_keeps_pipelined_bytes_pending() {
        let mut dec = FrameDecoder::new();
        dec.push(&encode(&[vec![1, 2]]));
        dec.push(&[5, 0, 0, 0, 7]);
        assert_eq!(dec.next().unwrap(), Some(&[1u8, 2][..]));
        assert_eq!(dec.next().unwrap(), None);
        assert_eq!(dec.pending(), &[5, 0, 0, 0, 7]);
        // Compaction on push keeps the partial frame intact.
        dec.push(&[8, 9, 10, 11]);
        assert_eq!(dec.next().unwrap(), Some(&[7u8, 8, 9, 10, 11][..]));
        assert!(dec.pending().is_empty());
    }

    #[test]
    fn decoder_interleaved_push_and_single_next() {
        // One `next` per push leaves complete frames pending across pushes:
        // the deferred compaction must neither lose nor reorder them, and
        // must keep the buffer bounded by twice its live bytes plus a push.
        let frames: Vec<Vec<u8>> = (0..64u8).map(|i| vec![i; usize::from(i % 7)]).collect();
        let stream = encode(&frames);
        let mut dec = FrameDecoder::new();
        let mut out = Vec::new();
        for chunk in stream.chunks(5) {
            let live_before = dec.pending().len();
            dec.push(chunk);
            assert!(dec.buf.len() <= 2 * live_before + chunk.len());
            if let Some(p) = dec.next().unwrap() {
                out.push(p.to_vec());
            }
        }
        while let Some(p) = dec.next().unwrap() {
            out.push(p.to_vec());
        }
        assert_eq!(out, frames);
        assert!(dec.pending().is_empty());
    }

    #[test]
    fn decoder_poisons_on_oversized_prefix() {
        let mut dec = FrameDecoder::with_max_payload(256);
        assert_eq!(dec.max_payload(), 256);
        dec.push(&encode(&[vec![1]]));
        dec.push(&257u32.to_le_bytes());
        // The good frame ahead of it still comes out.
        assert_eq!(dec.next().unwrap(), Some(&[1u8][..]));
        let err = FrameTooLarge {
            declared: 257,
            max: 256,
        };
        assert_eq!(dec.next(), Err(err));
        assert!(dec.is_poisoned());
        assert!(dec.pending().is_empty());
        // Later input is discarded and the original error repeats.
        dec.push(&encode(&[vec![2]]));
        assert!(dec.pending().is_empty());
        assert_eq!(dec.next(), Err(err));
    }

    #[test]
    fn decoder_starts_empty() {
        let mut dec = FrameDecoder::default();
        assert_eq!(dec.max_payload(), MAX_FRAME_SIZE);
        assert!(!dec.is_poisoned());
        assert_eq!(dec.next(), Ok(None));
        dec.push(&[]);
        assert_eq!(dec.next(), Ok(None));
    }

    // --- properties ---

    fn arb_frames(max: usize) -> impl Strategy<Value = Vec<Vec<u8>>> {
        prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=max), 0..12)
    }

    proptest! {
        /// Any chunking of a valid stream yields the same frames, in order,
        /// with nothing left over.
        #[test]
        fn any_chunking_yields_the_same_frames(
            frames in arb_frames(64),
            raw_cuts in prop::collection::vec(any::<prop::sample::Index>(), 0..16),
        ) {
            let stream = encode(&frames);
            let mut cuts: Vec<usize> =
                raw_cuts.iter().map(|i| i.index(stream.len() + 1)).collect();
            cuts.sort_unstable();
            prop_assert_eq!(decode_chunked(&stream, &cuts, 64), frames.clone());
            prop_assert_eq!(split_all(&stream, 64), (frames, stream.len()));
        }

        /// An oversized prefix is refused with only its 4 bytes present,
        /// wherever it falls in the stream and however the stream is cut.
        #[test]
        fn oversized_prefix_fails_on_arrival(
            frames in arb_frames(32),
            max in 0usize..64,
            excess in 1u32..1_000_000,
            cut in any::<prop::sample::Index>(),
        ) {
            let good: Vec<Vec<u8>> =
                frames.into_iter().map(|mut f| { f.truncate(max); f }).collect();
            let declared = max as u32 + excess;
            let mut stream = encode(&good);
            stream.extend_from_slice(&declared.to_le_bytes());
            let cut = cut.index(stream.len() + 1);

            let mut dec = FrameDecoder::with_max_payload(max);
            let mut got = Vec::new();
            let mut result = Ok(());
            for chunk in [&stream[..cut], &stream[cut..]] {
                dec.push(chunk);
                loop {
                    match dec.next() {
                        Ok(Some(p)) => got.push(p.to_vec()),
                        Ok(None) => break,
                        Err(e) => { result = Err(e); break; }
                    }
                }
            }
            prop_assert_eq!(got, good);
            prop_assert_eq!(result, Err(FrameTooLarge { declared, max }));
        }
    }
}
