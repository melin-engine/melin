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
//! The sending side has its counterpart for application requests: the
//! body is encoded in place in the caller's send buffer, behind room for
//! a header of [`REQUEST_HEADER_LEN`] bytes, and [`seal_request`] writes
//! the length prefix and the protocol's tag in front of it once its
//! length is known ([`request_body`] hands out the region to encode
//! into, and [`frame_request`] does both around a closure). Frames built
//! at successive offsets lie back to back, ready for one write. A codec
//! crate that needs only framing depends on this crate alone, and a
//! codec whose error is [`ProtocolError`] frames with `?`.
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
use crate::control_codec::{TAG_APP, TAG_LEN};
use crate::error::ProtocolError;

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

// ---------------------------------------------------------------------------
// Request frames
// ---------------------------------------------------------------------------

/// Bytes written ahead of a request body: the length prefix and the
/// protocol's one-byte [`TAG_APP`].
pub const REQUEST_HEADER_LEN: usize = PREFIX_LEN + TAG_LEN;

/// The widest request body one frame carries: the tag counts toward
/// [`MAX_FRAME_SIZE`], so a body may take the rest of it.
pub const MAX_REQUEST_BODY: usize = MAX_FRAME_SIZE - TAG_LEN;

// The prefix is written as a `u32`; every request payload is bounded by
// `MAX_FRAME_SIZE`, so the conversion in `request_payload_len` is lossless.
const _: () = assert!(MAX_FRAME_SIZE <= u32::MAX as usize);

/// A request could not be framed. Nothing was written to the header, so a
/// caller that sends only what a successful call reported sends nothing
/// that could desync the node's framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestFrameError {
    /// The body, with the tag in front of it, would not fit in one frame
    /// of [`MAX_FRAME_SIZE`] bytes. `len` is that payload length (tag and
    /// body), saturated at `usize::MAX` for an absurd body length.
    RequestTooLarge { len: usize },
    /// The buffer cannot hold the frame: `needed` bytes, header included,
    /// against the `available` length of the buffer. Either the buffer is
    /// shorter than [`REQUEST_HEADER_LEN`], or the body runs past its end
    /// (from an encoder, a bug: it reported more than it was handed).
    BufferTooSmall { needed: usize, available: usize },
}

impl fmt::Display for RequestFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequestTooLarge { len } => write!(
                f,
                "request too large: {len} bytes with its tag, the frame limit is {MAX_FRAME_SIZE}"
            ),
            Self::BufferTooSmall { needed, available } => write!(
                f,
                "request buffer too small: the frame needs {needed} bytes, the buffer holds {available}"
            ),
        }
    }
}

impl std::error::Error for RequestFrameError {}

impl From<RequestFrameError> for ProtocolError {
    /// For a codec whose error is [`ProtocolError`], so its request
    /// encoder frames with `?`. An oversized request is
    /// [`MessageTooLarge`](ProtocolError::MessageTooLarge) with the
    /// payload length; a buffer too small is
    /// [`Truncated`](ProtocolError::Truncated), what this crate's own
    /// encoders report for a buffer that cannot hold their output.
    fn from(e: RequestFrameError) -> Self {
        match e {
            RequestFrameError::RequestTooLarge { len } => ProtocolError::MessageTooLarge(len),
            RequestFrameError::BufferTooSmall { .. } => ProtocolError::Truncated,
        }
    }
}

/// The payload length of a request frame carrying `body_len` bytes (the
/// tag and the body), or [`RequestFrameError::RequestTooLarge`] if that
/// exceeds [`MAX_FRAME_SIZE`]. The one place the request limit is
/// decided, for a sender that writes the header apart from the body as
/// much as for [`seal_request`].
///
/// `u32` because that is the length prefix's width on the wire: the value
/// goes into it as it is.
#[inline]
pub fn request_payload_len(body_len: usize) -> Result<u32, RequestFrameError> {
    // Saturating: a length near `usize::MAX` (an encoder's bug) is still
    // reported as too large rather than wrapping into one that fits.
    let len = TAG_LEN.saturating_add(body_len);
    if len > MAX_FRAME_SIZE {
        return Err(RequestFrameError::RequestTooLarge { len });
    }
    // Lossless: bounded by `MAX_FRAME_SIZE`, which fits a `u32` (asserted
    // above).
    Ok(len as u32)
}

/// The region of `buf` a request body is encoded into, ahead of
/// [`seal_request`]: everything after the first [`REQUEST_HEADER_LEN`]
/// bytes, capped at [`MAX_REQUEST_BODY`] bytes, so a codec that sizes its
/// output by the slice it is given never writes a body the frame limit
/// would refuse, even in a send buffer wider than one frame.
///
/// A `buf` shorter than the header is
/// [`RequestFrameError::BufferTooSmall`].
#[inline]
pub fn request_body(buf: &mut [u8]) -> Result<&mut [u8], RequestFrameError> {
    let available = buf.len();
    match buf.get_mut(REQUEST_HEADER_LEN..) {
        Some(body) => {
            let region = body.len().min(MAX_REQUEST_BODY);
            Ok(&mut body[..region])
        }
        None => Err(RequestFrameError::BufferTooSmall {
            needed: REQUEST_HEADER_LEN,
            available,
        }),
    }
}

/// Seal a request whose `body_len`-byte body already sits at
/// `buf[REQUEST_HEADER_LEN..]`: write the header (the length prefix and
/// the protocol's tag) in front of it. Returns the frame's length, header
/// included: the bytes to send are `buf[..n]`. The caller never needs the
/// protocol's tag.
///
/// - a `body_len` wider than [`MAX_REQUEST_BODY`] (one that takes the
///   frame over [`MAX_FRAME_SIZE`]) is
///   [`RequestTooLarge`](RequestFrameError::RequestTooLarge);
/// - otherwise, a frame longer than `buf` (a `buf` shorter than the
///   header, or a body past its end) is
///   [`BufferTooSmall`](RequestFrameError::BufferTooSmall).
///
/// The frame limit is checked before the buffer, so a length that is both
/// is reported as `RequestTooLarge`. On either error nothing is written.
///
/// Several requests batch into one buffer by framing each at the current
/// end, in `buf[end..]`, and advancing `end` by what this returns; the
/// frames lie back to back, ready for one write.
///
/// ```
/// use melin_wire_protocol::framing::{FrameDecoder, request_body, seal_request};
///
/// let mut buf = [0u8; 64];
/// let mut end = 0;
/// for body in [&b"first"[..], b"second"] {
///     let region = request_body(&mut buf[end..])?;
///     region[..body.len()].copy_from_slice(body);
///     end += seal_request(&mut buf[end..], body.len())?;
/// }
/// // `buf[..end]` is two request frames, back to back.
/// let mut decoder = FrameDecoder::new();
/// decoder.push(&buf[..end]);
/// assert!(decoder.next()?.is_some_and(|p| p.ends_with(b"first")));
/// assert!(decoder.next()?.is_some_and(|p| p.ends_with(b"second")));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[inline]
pub fn seal_request(buf: &mut [u8], body_len: usize) -> Result<usize, RequestFrameError> {
    let payload_len = request_payload_len(body_len)?;
    // Cannot overflow: `body_len` is at most `MAX_REQUEST_BODY` here.
    let frame_len = REQUEST_HEADER_LEN + body_len;
    let available = buf.len();
    let Some(header) = buf
        .get_mut(..frame_len)
        .and_then(|frame| frame.first_chunk_mut::<REQUEST_HEADER_LEN>())
    else {
        return Err(RequestFrameError::BufferTooSmall {
            needed: frame_len,
            available,
        });
    };
    header[..PREFIX_LEN].copy_from_slice(&payload_len.to_le_bytes());
    header[PREFIX_LEN] = TAG_APP;
    Ok(frame_len)
}

/// Frame one request in place at the front of `buf`, the closure form of
/// [`request_body`] and [`seal_request`]: `encode` writes the
/// application's body into the region `request_body` returns and reports
/// how many bytes it wrote, then the header goes in front of them.
/// Returns the frame's length, header included.
///
/// `encode` may fail with its own error type, which needs a conversion
/// from [`RequestFrameError`] for the failures decided here (a codec
/// using [`ProtocolError`] has one):
///
/// - a `buf` shorter than [`REQUEST_HEADER_LEN`] is
///   [`BufferTooSmall`](RequestFrameError::BufferTooSmall), and `encode`
///   is not called;
/// - a length from `encode` wider than [`MAX_REQUEST_BODY`] is
///   [`RequestTooLarge`](RequestFrameError::RequestTooLarge);
/// - otherwise, a length from `encode` past the region it was handed is
///   `BufferTooSmall`.
///
/// The frame limit is checked before the region, so a length that is
/// both is reported as `RequestTooLarge`. On any error the header is not
/// written: `encode` may have written into the body region, but a caller
/// that sends only what `frame_request` reported, and does not advance
/// its cursor on an error, sends nothing that could desync the node's
/// framing.
///
/// ```
/// use melin_wire_protocol::error::ProtocolError;
/// use melin_wire_protocol::framing::{REQUEST_HEADER_LEN, frame_request};
///
/// /// A codec's request encoder: the body, then the frame around it.
/// fn encode_request(body: &[u8], buf: &mut [u8]) -> Result<usize, ProtocolError> {
///     frame_request(buf, |out| {
///         let dst = out.get_mut(..body.len()).ok_or(ProtocolError::Truncated)?;
///         dst.copy_from_slice(body);
///         Ok(body.len())
///     })
/// }
///
/// let mut buf = [0u8; 16];
/// assert_eq!(encode_request(b"abc", &mut buf)?, REQUEST_HEADER_LEN + 3);
/// assert!(matches!(encode_request(&[0; 64], &mut buf), Err(ProtocolError::Truncated)));
/// # Ok::<(), ProtocolError>(())
/// ```
#[inline]
pub fn frame_request<E: From<RequestFrameError>>(
    buf: &mut [u8],
    encode: impl FnOnce(&mut [u8]) -> Result<usize, E>,
) -> Result<usize, E> {
    let body_len = encode(request_body(buf)?)?;
    Ok(seal_request(buf, body_len)?)
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

    // --- request frames ---

    /// An application frame carrying `body`, length prefix included, built
    /// by hand.
    fn app_frame(body: &[u8]) -> Vec<u8> {
        let mut frame = ((TAG_LEN + body.len()) as u32).to_le_bytes().to_vec();
        frame.push(TAG_APP);
        frame.extend_from_slice(body);
        frame
    }

    /// `frame_request` with an encoder that copies `body`, under the
    /// module's own error type.
    fn frame_body(buf: &mut [u8], body: &[u8]) -> Result<usize, RequestFrameError> {
        frame_request(buf, |out| {
            let Some(dst) = out.get_mut(..body.len()) else {
                return Err(RequestFrameError::BufferTooSmall {
                    needed: body.len(),
                    available: out.len(),
                });
            };
            dst.copy_from_slice(body);
            Ok(body.len())
        })
    }

    #[test]
    fn request_constants_follow_the_frame_limit() {
        assert_eq!(REQUEST_HEADER_LEN, PREFIX_LEN + TAG_LEN);
        assert_eq!(MAX_REQUEST_BODY, MAX_FRAME_SIZE - TAG_LEN);
        assert_eq!(
            request_payload_len(MAX_REQUEST_BODY),
            Ok(MAX_FRAME_SIZE as u32)
        );
        assert_eq!(
            request_payload_len(MAX_REQUEST_BODY + 1),
            Err(RequestFrameError::RequestTooLarge {
                len: MAX_FRAME_SIZE + 1
            })
        );
        assert_eq!(
            request_payload_len(usize::MAX),
            Err(RequestFrameError::RequestTooLarge { len: usize::MAX })
        );
    }

    #[test]
    fn request_body_is_the_rest_capped_at_the_widest_body() {
        let mut buf = [0u8; 32];
        assert_eq!(
            request_body(&mut buf).unwrap().len(),
            32 - REQUEST_HEADER_LEN
        );
        let mut wide = vec![0u8; REQUEST_HEADER_LEN + MAX_FRAME_SIZE + 16];
        assert_eq!(request_body(&mut wide).unwrap().len(), MAX_REQUEST_BODY);
        let mut header_only = [0u8; REQUEST_HEADER_LEN];
        assert!(request_body(&mut header_only).unwrap().is_empty());
        for len in 0..REQUEST_HEADER_LEN {
            assert_eq!(
                request_body(&mut vec![0u8; len]),
                Err(RequestFrameError::BufferTooSmall {
                    needed: REQUEST_HEADER_LEN,
                    available: len
                })
            );
        }
    }

    #[test]
    fn seal_request_writes_the_header_and_nothing_else() {
        let mut buf = [0xEE; 32];
        buf[REQUEST_HEADER_LEN..REQUEST_HEADER_LEN + 3].copy_from_slice(b"abc");
        let n = seal_request(&mut buf, 3).unwrap();
        assert_eq!(n, REQUEST_HEADER_LEN + 3);
        assert_eq!(&buf[..n], app_frame(b"abc").as_slice());
        assert!(buf[n..].iter().all(|&b| b == 0xEE));

        // An empty body is a tag-only frame, even in a header-sized buffer.
        let mut buf = [0u8; REQUEST_HEADER_LEN];
        assert_eq!(seal_request(&mut buf, 0), Ok(REQUEST_HEADER_LEN));
        assert_eq!(buf.as_slice(), app_frame(b"").as_slice());
    }

    #[test]
    fn seal_request_refuses_without_writing() {
        // A body past the buffer's end.
        let mut buf = [0x77u8; REQUEST_HEADER_LEN + 4];
        assert_eq!(
            seal_request(&mut buf, 5),
            Err(RequestFrameError::BufferTooSmall {
                needed: REQUEST_HEADER_LEN + 5,
                available: REQUEST_HEADER_LEN + 4
            })
        );
        // No room for the header at all.
        for len in 0..REQUEST_HEADER_LEN {
            let mut short = vec![0x77u8; len];
            assert_eq!(
                seal_request(&mut short, 0),
                Err(RequestFrameError::BufferTooSmall {
                    needed: REQUEST_HEADER_LEN,
                    available: len
                })
            );
            assert!(short.iter().all(|&b| b == 0x77));
        }
        // Over the frame limit, in a buffer wide enough to hold it: the
        // limit is checked first, and before the buffer when both fail.
        let mut wide = vec![0x77u8; REQUEST_HEADER_LEN + MAX_FRAME_SIZE + 16];
        assert_eq!(
            seal_request(&mut wide, MAX_REQUEST_BODY + 1),
            Err(RequestFrameError::RequestTooLarge {
                len: MAX_FRAME_SIZE + 1
            })
        );
        assert_eq!(
            seal_request(&mut buf, usize::MAX),
            Err(RequestFrameError::RequestTooLarge { len: usize::MAX })
        );
        assert!(wide.iter().all(|&b| b == 0x77));
        assert!(buf.iter().all(|&b| b == 0x77));
    }

    #[test]
    fn frame_request_writes_the_header_in_front_of_the_body() {
        let mut buf = [0xEE; 32];
        let n = frame_body(&mut buf, b"abc").unwrap();
        assert_eq!(n, REQUEST_HEADER_LEN + 3);
        assert_eq!(&buf[..n], app_frame(b"abc").as_slice());
        // Nothing past the frame is touched.
        assert!(buf[n..].iter().all(|&b| b == 0xEE));

        // An empty body is a tag-only frame, and the encoder is handed
        // everything after the header.
        let n = frame_request(&mut buf, |out| {
            assert_eq!(out.len(), 32 - REQUEST_HEADER_LEN);
            Ok::<_, RequestFrameError>(0)
        })
        .unwrap();
        assert_eq!(&buf[..n], app_frame(b"").as_slice());
    }

    #[test]
    fn frame_request_takes_the_widest_body_and_refuses_one_more() {
        let mut buf = vec![0u8; REQUEST_HEADER_LEN + MAX_FRAME_SIZE + 16];

        let widest = vec![0xAB; MAX_REQUEST_BODY];
        let n = frame_body(&mut buf, &widest).unwrap();
        assert_eq!(n, PREFIX_LEN + MAX_FRAME_SIZE);
        let frame = split_frame(&buf[..n]).unwrap().unwrap();
        assert_eq!(frame.consumed(), n);
        assert_eq!(frame.payload, [&[TAG_APP][..], &widest].concat());

        // One byte over: refused, and no header is written. The encoder is
        // handed exactly the widest body, though the buffer is wider, so a
        // codec sizing itself by its slice stays in bounds.
        let mut buf = vec![0x5C; REQUEST_HEADER_LEN + MAX_FRAME_SIZE + 16];
        let err = frame_request(&mut buf, |out| {
            assert_eq!(out.len(), MAX_REQUEST_BODY);
            Ok::<_, RequestFrameError>(MAX_REQUEST_BODY + 1)
        })
        .unwrap_err();
        assert_eq!(
            err,
            RequestFrameError::RequestTooLarge {
                len: MAX_FRAME_SIZE + 1
            }
        );
        assert!(buf[..REQUEST_HEADER_LEN].iter().all(|&b| b == 0x5C));

        // An encoder reporting an absurd length is refused, not wrapped;
        // the frame limit is checked before the region, even when the
        // buffer is too short as well.
        let err = frame_request(&mut buf, |_| Ok::<_, RequestFrameError>(usize::MAX)).unwrap_err();
        assert_eq!(err, RequestFrameError::RequestTooLarge { len: usize::MAX });
        let mut small = [0u8; 16];
        let err = frame_request(&mut small, |_| {
            Ok::<_, RequestFrameError>(MAX_REQUEST_BODY + 1)
        })
        .unwrap_err();
        assert!(
            matches!(err, RequestFrameError::RequestTooLarge { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn frame_request_refuses_a_buffer_too_small() {
        // No room for the header: the encoder is never called.
        for len in 0..REQUEST_HEADER_LEN {
            let mut buf = vec![0u8; len];
            let err = frame_request(&mut buf, |_| -> Result<usize, RequestFrameError> {
                panic!("the encoder must not run without room for the header")
            })
            .unwrap_err();
            assert_eq!(
                err,
                RequestFrameError::BufferTooSmall {
                    needed: REQUEST_HEADER_LEN,
                    available: len
                }
            );
            assert!(err.to_string().contains("too small"), "{err}");
        }

        // Exactly the header: room for an empty body only.
        let mut buf = [0u8; REQUEST_HEADER_LEN];
        assert_eq!(frame_body(&mut buf, b"").unwrap(), REQUEST_HEADER_LEN);

        // An encoder claiming more than it was handed: refused, header
        // left unwritten.
        let mut buf = [0x77u8; REQUEST_HEADER_LEN + 4];
        let err = frame_request(&mut buf, |_| Ok::<_, RequestFrameError>(5)).unwrap_err();
        assert_eq!(
            err,
            RequestFrameError::BufferTooSmall {
                needed: REQUEST_HEADER_LEN + 5,
                available: REQUEST_HEADER_LEN + 4
            }
        );
        assert!(buf[..REQUEST_HEADER_LEN].iter().all(|&b| b == 0x77));
    }

    #[test]
    fn frame_request_passes_the_encoders_error_through() {
        #[derive(Debug, PartialEq)]
        enum CodecError {
            Mine,
            Framing(RequestFrameError),
        }
        impl From<RequestFrameError> for CodecError {
            fn from(e: RequestFrameError) -> Self {
                CodecError::Framing(e)
            }
        }
        let mut buf = [0x33u8; 16];
        assert_eq!(
            frame_request(&mut buf, |_| Err(CodecError::Mine)),
            Err(CodecError::Mine)
        );
        assert!(buf.iter().all(|&b| b == 0x33));
        // The failures decided by `frame_request` arrive converted.
        assert_eq!(
            frame_request(&mut buf[..2], |_| Ok::<_, CodecError>(0)),
            Err(CodecError::Framing(RequestFrameError::BufferTooSmall {
                needed: REQUEST_HEADER_LEN,
                available: 2
            }))
        );
    }

    /// A codec's request encoder whose error is `ProtocolError`: framing
    /// failures convert with `?`, with no conversion of the codec's own.
    fn encode_request_as_codec(body: &[u8], buf: &mut [u8]) -> Result<usize, ProtocolError> {
        let region = request_body(buf)?;
        let dst = region
            .get_mut(..body.len())
            .ok_or(ProtocolError::InvalidField("body"))?;
        dst.copy_from_slice(body);
        Ok(seal_request(buf, body.len())?)
    }

    #[test]
    fn a_protocol_error_codec_frames_with_question_mark() {
        let mut buf = [0u8; 16];
        assert_eq!(
            encode_request_as_codec(b"abc", &mut buf).unwrap(),
            REQUEST_HEADER_LEN + 3
        );
        assert_eq!(&buf[..REQUEST_HEADER_LEN + 3], app_frame(b"abc").as_slice());
        assert!(matches!(
            encode_request_as_codec(b"", &mut buf[..2]),
            Err(ProtocolError::Truncated)
        ));
        // The closure form under the same error type.
        let err = frame_request(&mut [0u8; 8], |_| {
            Ok::<_, ProtocolError>(MAX_REQUEST_BODY + 1)
        })
        .unwrap_err();
        assert!(
            matches!(err, ProtocolError::MessageTooLarge(len) if len == MAX_FRAME_SIZE + 1),
            "{err:?}"
        );
        assert!(matches!(
            ProtocolError::from(RequestFrameError::BufferTooSmall {
                needed: 9,
                available: 8
            }),
            ProtocolError::Truncated
        ));
    }

    #[test]
    fn request_frame_errors_say_what_failed() {
        assert_eq!(
            RequestFrameError::RequestTooLarge { len: 2000 }.to_string(),
            format!(
                "request too large: 2000 bytes with its tag, the frame limit is {MAX_FRAME_SIZE}"
            )
        );
        assert_eq!(
            RequestFrameError::BufferTooSmall {
                needed: 9,
                available: 8
            }
            .to_string(),
            "request buffer too small: the frame needs 9 bytes, the buffer holds 8"
        );
    }

    #[test]
    fn frame_request_frames_at_an_offset_and_batches_back_to_back() {
        let bodies: [&[u8]; 4] = [b"one", b"", &[TAG_APP, 0x00], b"four!"];
        let mut buf = [0xEE; 128];
        // Start part-way in, as after bytes already queued for sending.
        let start = 7;
        let mut end = start;
        for body in bodies {
            end += frame_body(&mut buf[end..], body).unwrap();
        }
        assert!(buf[..start].iter().all(|&b| b == 0xEE));
        assert!(buf[end..].iter().all(|&b| b == 0xEE));
        assert_eq!(&buf[start..end], bodies.map(app_frame).concat().as_slice());
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

        /// Any run of bodies that fit, framed back to back from any
        /// offset, decodes to the same bodies however the bytes arrive.
        #[test]
        fn framed_requests_round_trip_through_the_decoder(
            bodies in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=64), 0..8),
            offset in 0usize..16,
            chunk in 1usize..32,
        ) {
            let mut buf = vec![0u8; offset + bodies.len() * (REQUEST_HEADER_LEN + 64)];
            let mut end = offset;
            for body in &bodies {
                end += frame_body(&mut buf[end..], body).unwrap();
            }
            let mut decoder = FrameDecoder::new();
            let mut got = Vec::new();
            for piece in buf[offset..end].chunks(chunk) {
                decoder.push(piece);
                while let Some(payload) = decoder.next().unwrap() {
                    let Some((&TAG_APP, body)) = payload.split_first() else {
                        panic!("a request frame without TAG_APP: {payload:?}");
                    };
                    got.push(body.to_vec());
                }
            }
            prop_assert_eq!(got, bodies);
            prop_assert!(decoder.pending().is_empty());
        }
    }
}
