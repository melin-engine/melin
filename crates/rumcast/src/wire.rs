//! On-the-wire frame definitions for melin-rumcast.
//!
//! All frames are little-endian. Headers are POD with explicit field-level
//! padding and natural alignment, so a `&[u8]` aligned to 8 bytes can be
//! viewed as a `&Header` via [`bytemuck::from_bytes`] with zero copy.
//!
//! # Position encoding
//!
//! Every byte in a stream has a unique 64-bit `position`:
//!
//! ```text
//!     position = (term_id << term_length_bits) | term_offset
//! ```
//!
//! `term_length_bits = log2(term_length)`. With a 16 MiB term length
//! (`term_length_bits = 24`), the term-id space is 40 bits wide — enough
//! for ~140 years at one term rotation per millisecond.
//!
//! # Layout differences from Aeron
//!
//! Aeron's wire format relies on unaligned access (legal in C). To stay
//! `bytemuck::Pod` we add an explicit `_padding` field in [`StatusMessage`]
//! so its `receiver_id: u64` lands on its natural 8-byte alignment. All
//! other frames pack naturally with no padding. The on-the-wire field set
//! is otherwise the same; pcap dumps remain readable.

use bytemuck::{Pod, Zeroable};

/// Protocol version stamped in every frame header. Bumping this rejects all
/// frames from older peers.
pub const PROTOCOL_VERSION: u8 = 1;

/// Frame type discriminator carried in [`HeaderCommon::frame_type`].
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Data = 0x01,
    Nak = 0x02,
    StatusMessage = 0x03,
    Setup = 0x04,
    Heartbeat = 0x05,
}

impl FrameType {
    /// Convert from the raw u16 found in a received header.
    #[inline]
    pub fn from_u16(value: u16) -> Option<Self> {
        match value {
            0x01 => Some(Self::Data),
            0x02 => Some(Self::Nak),
            0x03 => Some(Self::StatusMessage),
            0x04 => Some(Self::Setup),
            0x05 => Some(Self::Heartbeat),
            _ => None,
        }
    }
}

/// Bit flags carried in the 8-bit `flags` field of a [`DataFrame`].
pub mod data_flags {
    /// Set on the first fragment of a multi-fragment message.
    pub const BEGIN_FRAGMENT: u8 = 0x80;
    /// Set on the last fragment of a multi-fragment message.
    pub const END_FRAGMENT: u8 = 0x40;
    /// An unfragmented message has both `BEGIN_FRAGMENT` and `END_FRAGMENT`.
    pub const UNFRAGMENTED: u8 = BEGIN_FRAGMENT | END_FRAGMENT;
    /// Term-end padding fragment. The publisher emits this when the active
    /// term cannot fit the next message and a rotation is required; the
    /// payload bytes are not part of the message stream and the receiver
    /// MUST skip them, advancing its position to `term_offset + payload_len`
    /// without delivering anything to the application.
    pub const PADDING: u8 = 0x10;
}

/// Common 8-byte prefix shared by every frame. Lets the receiver dispatch
/// on `frame_type` after a single 8-byte read.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct HeaderCommon {
    /// Total frame length in bytes, including this header and any payload.
    pub frame_length: u32,
    /// Protocol version. Receivers reject mismatched frames.
    pub version: u8,
    /// Type-specific flags. See [`data_flags`] for [`DataFrame`].
    pub flags: u8,
    /// Frame type discriminator. See [`FrameType`].
    pub frame_type: u16,
}

const _: () = assert!(core::mem::size_of::<HeaderCommon>() == 8);
const _: () = assert!(core::mem::align_of::<HeaderCommon>() == 4);

/// Data fragment header. Fixed 32 bytes, followed by `frame_length - 32`
/// payload bytes in the same UDP packet.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct DataFrame {
    pub common: HeaderCommon,
    pub session_id: u32,
    pub stream_id: u32,
    pub term_id: u32,
    pub term_offset: u32,
    /// Reserved for future protocol extensions. Senders MUST set to zero in
    /// v1; receivers MUST ignore. Gives us 8 bytes of forever-future runway
    /// without re-versioning the wire format.
    pub reserved_value: u64,
}

const _: () = assert!(core::mem::size_of::<DataFrame>() == 32);
const _: () = assert!(core::mem::align_of::<DataFrame>() == 8);

impl DataFrame {
    pub const HEADER_LEN: usize = core::mem::size_of::<Self>();

    /// Build a fragment header for a payload of `payload_len` bytes.
    #[inline]
    pub const fn new(
        session_id: u32,
        stream_id: u32,
        term_id: u32,
        term_offset: u32,
        flags: u8,
        payload_len: u32,
    ) -> Self {
        Self {
            common: HeaderCommon {
                frame_length: Self::HEADER_LEN as u32 + payload_len,
                version: PROTOCOL_VERSION,
                flags,
                frame_type: FrameType::Data as u16,
            },
            session_id,
            stream_id,
            term_id,
            term_offset,
            reserved_value: 0,
        }
    }
}

/// NAK (negative acknowledgement). Receiver requests retransmission of a
/// contiguous gap `[term_offset, term_offset + gap_length)` within `term_id`.
///
/// 28 bytes on the wire — all `u32` fields, no padding needed.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct NakFrame {
    pub common: HeaderCommon,
    pub session_id: u32,
    pub stream_id: u32,
    pub term_id: u32,
    pub term_offset: u32,
    pub gap_length: u32,
}

const _: () = assert!(core::mem::size_of::<NakFrame>() == 28);
const _: () = assert!(core::mem::align_of::<NakFrame>() == 4);

impl NakFrame {
    pub const HEADER_LEN: usize = core::mem::size_of::<Self>();

    #[inline]
    pub const fn new(
        session_id: u32,
        stream_id: u32,
        term_id: u32,
        term_offset: u32,
        gap_length: u32,
    ) -> Self {
        Self {
            common: HeaderCommon {
                frame_length: Self::HEADER_LEN as u32,
                version: PROTOCOL_VERSION,
                flags: 0,
                frame_type: FrameType::Nak as u16,
            },
            session_id,
            stream_id,
            term_id,
            term_offset,
            gap_length,
        }
    }
}

/// Status message. Receiver advertises the highest contiguously-consumed
/// position and remaining receive-buffer window so the sender can drive
/// flow control and reclaim log space.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct StatusMessage {
    pub common: HeaderCommon,
    pub session_id: u32,
    pub stream_id: u32,
    pub consumption_term_id: u32,
    pub consumption_term_offset: u32,
    pub receiver_window: u32,
    /// Padding so `receiver_id` lands on its natural 8-byte alignment.
    /// Wire value MUST be zero.
    pub _padding: u32,
    /// Unique per subscriber within a stream. Lets the sender disambiguate
    /// status messages from N receivers in a multicast fan-out.
    pub receiver_id: u64,
}

const _: () = assert!(core::mem::size_of::<StatusMessage>() == 40);
const _: () = assert!(core::mem::align_of::<StatusMessage>() == 8);

impl StatusMessage {
    pub const HEADER_LEN: usize = core::mem::size_of::<Self>();

    #[inline]
    pub const fn new(
        session_id: u32,
        stream_id: u32,
        consumption_term_id: u32,
        consumption_term_offset: u32,
        receiver_window: u32,
        receiver_id: u64,
    ) -> Self {
        Self {
            common: HeaderCommon {
                frame_length: Self::HEADER_LEN as u32,
                version: PROTOCOL_VERSION,
                flags: 0,
                frame_type: FrameType::StatusMessage as u16,
            },
            session_id,
            stream_id,
            consumption_term_id,
            consumption_term_offset,
            receiver_window,
            _padding: 0,
            receiver_id,
        }
    }
}

/// Setup frame. Sent by the publisher at session start and periodically
/// thereafter so a fresh subscriber joining mid-stream learns the stream
/// parameters (initial term, active term/offset, term length).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct SetupFrame {
    pub common: HeaderCommon,
    pub session_id: u32,
    pub stream_id: u32,
    pub initial_term_id: u32,
    pub active_term_id: u32,
    pub term_offset: u32,
    pub term_length: u32,
}

const _: () = assert!(core::mem::size_of::<SetupFrame>() == 32);
const _: () = assert!(core::mem::align_of::<SetupFrame>() == 4);

impl SetupFrame {
    pub const HEADER_LEN: usize = core::mem::size_of::<Self>();

    #[inline]
    pub const fn new(
        session_id: u32,
        stream_id: u32,
        initial_term_id: u32,
        active_term_id: u32,
        term_offset: u32,
        term_length: u32,
    ) -> Self {
        Self {
            common: HeaderCommon {
                frame_length: Self::HEADER_LEN as u32,
                version: PROTOCOL_VERSION,
                flags: 0,
                frame_type: FrameType::Setup as u16,
            },
            session_id,
            stream_id,
            initial_term_id,
            active_term_id,
            term_offset,
            term_length,
        }
    }
}

/// Heartbeat. Sent when the publisher has no data to publish so the
/// receiver can distinguish silence from a dead peer (and not mistakenly
/// declare a gap that doesn't exist).
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable)]
pub struct HeartbeatFrame {
    pub common: HeaderCommon,
    pub session_id: u32,
    pub stream_id: u32,
}

const _: () = assert!(core::mem::size_of::<HeartbeatFrame>() == 16);
const _: () = assert!(core::mem::align_of::<HeartbeatFrame>() == 4);

impl HeartbeatFrame {
    pub const HEADER_LEN: usize = core::mem::size_of::<Self>();

    #[inline]
    pub const fn new(session_id: u32, stream_id: u32) -> Self {
        Self {
            common: HeaderCommon {
                frame_length: Self::HEADER_LEN as u32,
                version: PROTOCOL_VERSION,
                flags: 0,
                frame_type: FrameType::Heartbeat as u16,
            },
            session_id,
            stream_id,
        }
    }
}

/// Decoded view of a frame, borrowing from the input buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameView<'a> {
    Data {
        header: &'a DataFrame,
        payload: &'a [u8],
    },
    Nak(&'a NakFrame),
    StatusMessage(&'a StatusMessage),
    Setup(&'a SetupFrame),
    Heartbeat(&'a HeartbeatFrame),
}

/// Errors returned by [`parse_frame`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    /// The supplied buffer is shorter than required. Covers both "smaller
    /// than the common header" and "smaller than the announced
    /// `frame_length`". `needed` is the number of bytes we'd have wanted.
    BufferTooSmall { needed: usize, got: usize },
    /// `frame_length` field doesn't match the expected length for this
    /// frame type. For data frames this means smaller than the data
    /// header; for fixed-size frames (NAK / SM / Setup / Heartbeat) it
    /// means anything other than the exact expected size.
    LengthMismatch { expected: usize, got: usize },
    /// The buffer's start address isn't aligned to the header's required
    /// alignment. Recv buffers must guarantee 8-byte alignment.
    Misaligned,
    /// `version` field doesn't match [`PROTOCOL_VERSION`].
    BadVersion(u8),
    /// `frame_type` field is not a known [`FrameType`].
    UnknownFrameType(u16),
}

/// Parse one frame from the start of `buf`. Returns a borrowed view; no
/// allocation, no copying.
///
/// `buf` must be aligned to at least 8 bytes (the strictest header
/// alignment, [`DataFrame`]). The recv layer is responsible for delivering
/// buffers with that guarantee. Trailing bytes past `frame_length` are
/// silently ignored — Ethernet sometimes pads small UDP packets up to the
/// 60-byte minimum frame size, and the trailing bytes are not part of the
/// rumcast frame.
///
/// # Example
///
/// ```
/// use melin_rumcast::wire::{
///     parse_frame, DataFrame, FrameView, data_flags, PROTOCOL_VERSION,
/// };
///
/// // Build a packet: a 4-byte unfragmented data payload.
/// #[repr(C, align(8))]
/// struct Buf([u8; 36]);
/// let mut buf = Buf([0u8; 36]);
/// let frame = DataFrame::new(1, 2, 3, 4, data_flags::UNFRAGMENTED, 4);
/// buf.0[..32].copy_from_slice(bytemuck::bytes_of(&frame));
/// buf.0[32..36].copy_from_slice(b"hi!\n");
///
/// match parse_frame(&buf.0).unwrap() {
///     FrameView::Data { header, payload } => {
///         assert_eq!(header.common.version, PROTOCOL_VERSION);
///         assert_eq!(payload, b"hi!\n");
///     }
///     _ => unreachable!(),
/// }
/// ```
pub fn parse_frame(buf: &[u8]) -> Result<FrameView<'_>, ParseError> {
    let common_size = core::mem::size_of::<HeaderCommon>();
    if buf.len() < common_size {
        return Err(ParseError::BufferTooSmall {
            needed: common_size,
            got: buf.len(),
        });
    }

    let common: &HeaderCommon =
        bytemuck::try_from_bytes(&buf[..common_size]).map_err(|_| ParseError::Misaligned)?;

    if common.version != PROTOCOL_VERSION {
        return Err(ParseError::BadVersion(common.version));
    }

    let frame_length = common.frame_length as usize;
    if frame_length < common_size {
        // The announced length can't even fit the common header — packet
        // is malformed regardless of buffer size.
        return Err(ParseError::LengthMismatch {
            expected: common_size,
            got: frame_length,
        });
    }
    if frame_length > buf.len() {
        return Err(ParseError::BufferTooSmall {
            needed: frame_length,
            got: buf.len(),
        });
    }

    let frame_type = FrameType::from_u16(common.frame_type)
        .ok_or(ParseError::UnknownFrameType(common.frame_type))?;

    match frame_type {
        FrameType::Data => {
            if frame_length < DataFrame::HEADER_LEN {
                return Err(ParseError::LengthMismatch {
                    expected: DataFrame::HEADER_LEN,
                    got: frame_length,
                });
            }
            let header: &DataFrame = bytemuck::try_from_bytes(&buf[..DataFrame::HEADER_LEN])
                .map_err(|_| ParseError::Misaligned)?;
            let payload = &buf[DataFrame::HEADER_LEN..frame_length];
            Ok(FrameView::Data { header, payload })
        }
        FrameType::Nak => parse_fixed::<NakFrame>(buf, frame_length).map(FrameView::Nak),
        FrameType::StatusMessage => {
            parse_fixed::<StatusMessage>(buf, frame_length).map(FrameView::StatusMessage)
        }
        FrameType::Setup => parse_fixed::<SetupFrame>(buf, frame_length).map(FrameView::Setup),
        FrameType::Heartbeat => {
            parse_fixed::<HeartbeatFrame>(buf, frame_length).map(FrameView::Heartbeat)
        }
    }
}

#[inline]
fn parse_fixed<T: Pod>(buf: &[u8], frame_length: usize) -> Result<&T, ParseError> {
    let header_len = core::mem::size_of::<T>();
    if frame_length != header_len {
        return Err(ParseError::LengthMismatch {
            expected: header_len,
            got: frame_length,
        });
    }
    bytemuck::try_from_bytes::<T>(&buf[..header_len]).map_err(|_| ParseError::Misaligned)
}

/// Compose a 64-bit position from a (term_id, term_offset) pair.
///
/// `term_length_bits` is `log2(term_length)` and MUST be in the range
/// returned by [`term_length_bits`] (currently 16..=30). Outside that
/// range the shift can overflow and the result is undefined in release
/// builds. Callers MUST validate any peer-supplied term length via
/// [`term_length_bits`] before passing it here — never trust raw values
/// from a [`SetupFrame`].
#[inline]
pub const fn position(term_id: u32, term_offset: u32, term_length_bits: u32) -> u64 {
    debug_assert!(term_length_bits < 32);
    debug_assert!((term_offset as u64) < (1u64 << term_length_bits));
    ((term_id as u64) << term_length_bits) | (term_offset as u64)
}

/// Inverse of [`position`]: extract the term-id portion. Same precondition
/// on `term_length_bits` as [`position`].
#[inline]
pub const fn term_id_from_position(position: u64, term_length_bits: u32) -> u32 {
    debug_assert!(term_length_bits < 32);
    (position >> term_length_bits) as u32
}

/// Inverse of [`position`]: extract the term-offset portion. Same
/// precondition on `term_length_bits` as [`position`].
#[inline]
pub const fn term_offset_from_position(position: u64, term_length_bits: u32) -> u32 {
    debug_assert!(term_length_bits < 32);
    (position & ((1u64 << term_length_bits) - 1)) as u32
}

/// Smallest term length we accept. Below this the per-rotation overhead
/// dominates throughput; above 1 GiB you're consuming RAM you'd rather
/// spend on receive buffers.
pub const MIN_TERM_LENGTH: u32 = 64 * 1024;
pub const MAX_TERM_LENGTH: u32 = 1024 * 1024 * 1024;

/// Validate a configured term length and return its `log2` if valid.
pub fn term_length_bits(term_length: u32) -> Option<u32> {
    if !term_length.is_power_of_two() || !(MIN_TERM_LENGTH..=MAX_TERM_LENGTH).contains(&term_length)
    {
        return None;
    }
    Some(term_length.trailing_zeros())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 8-byte-aligned scratch buffer for parser tests. `Vec<u8>` is
    /// allocated with align-of u8 = 1, so we can't rely on it for parsing
    /// frames whose headers require 8-byte alignment.
    #[repr(C, align(8))]
    struct AlignedBuf<const N: usize>([u8; N]);

    impl<const N: usize> AlignedBuf<N> {
        fn new() -> Self {
            Self([0u8; N])
        }
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.0
        }
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    #[test]
    fn frame_type_round_trip() {
        for variant in [
            FrameType::Data,
            FrameType::Nak,
            FrameType::StatusMessage,
            FrameType::Setup,
            FrameType::Heartbeat,
        ] {
            assert_eq!(FrameType::from_u16(variant as u16), Some(variant));
        }
    }

    #[test]
    fn frame_type_unknown_returns_none() {
        for v in [0u16, 0x06, 0x10, 0xFFFF] {
            assert_eq!(FrameType::from_u16(v), None);
        }
    }

    #[test]
    fn data_frame_round_trip() {
        let frame = DataFrame::new(
            0xCAFEBABE,
            0xDEADBEEF,
            0x12345678,
            0x00ABCDEF,
            data_flags::UNFRAGMENTED,
            100,
        );
        let bytes = bytemuck::bytes_of(&frame);
        let decoded: &DataFrame = bytemuck::from_bytes(bytes);
        assert_eq!(decoded, &frame);
        assert_eq!(decoded.common.frame_length, 32 + 100);
        assert_eq!(decoded.common.version, PROTOCOL_VERSION);
        assert_eq!(decoded.common.frame_type, FrameType::Data as u16);
        assert_eq!(decoded.reserved_value, 0);
    }

    #[test]
    fn data_frame_layout_golden() {
        let frame = DataFrame::new(
            0x11223344,
            0x55667788,
            0x99AABBCC,
            0xDDEEFF00,
            data_flags::BEGIN_FRAGMENT,
            64,
        );
        let bytes = bytemuck::bytes_of(&frame);
        // frame_length = 32 + 64 = 96
        assert_eq!(&bytes[0..4], &96u32.to_le_bytes());
        assert_eq!(bytes[4], PROTOCOL_VERSION);
        assert_eq!(bytes[5], data_flags::BEGIN_FRAGMENT);
        assert_eq!(&bytes[6..8], &(FrameType::Data as u16).to_le_bytes());
        assert_eq!(&bytes[8..12], &0x11223344u32.to_le_bytes());
        assert_eq!(&bytes[12..16], &0x55667788u32.to_le_bytes());
        assert_eq!(&bytes[16..20], &0x99AABBCCu32.to_le_bytes());
        assert_eq!(&bytes[20..24], &0xDDEEFF00u32.to_le_bytes());
        // reserved_value must be zero
        assert_eq!(&bytes[24..32], &0u64.to_le_bytes());
    }

    #[test]
    fn nak_frame_round_trip() {
        let frame = NakFrame::new(1, 2, 3, 4, 5);
        let bytes = bytemuck::bytes_of(&frame);
        assert_eq!(bytes.len(), 28, "NAK is exactly 28 bytes on the wire");
        let decoded: &NakFrame = bytemuck::from_bytes(bytes);
        assert_eq!(decoded, &frame);
        assert_eq!(decoded.common.frame_type, FrameType::Nak as u16);
        assert_eq!(decoded.common.frame_length, 28);
    }

    #[test]
    fn status_message_round_trip_and_padding_zero() {
        let frame = StatusMessage::new(1, 2, 3, 4, 1024, 0xFEDCBA9876543210);
        let bytes = bytemuck::bytes_of(&frame);
        let decoded: &StatusMessage = bytemuck::from_bytes(bytes);
        assert_eq!(decoded, &frame);
        assert_eq!(decoded._padding, 0);
        assert_eq!(decoded.receiver_id, 0xFEDCBA9876543210);
    }

    #[test]
    fn setup_frame_round_trip() {
        let frame = SetupFrame::new(1, 2, 100, 100, 0, 16 * 1024 * 1024);
        let bytes = bytemuck::bytes_of(&frame);
        let decoded: &SetupFrame = bytemuck::from_bytes(bytes);
        assert_eq!(decoded, &frame);
        assert_eq!(decoded.term_length, 16 * 1024 * 1024);
    }

    #[test]
    fn heartbeat_frame_round_trip() {
        let frame = HeartbeatFrame::new(7, 9);
        let bytes = bytemuck::bytes_of(&frame);
        let decoded: &HeartbeatFrame = bytemuck::from_bytes(bytes);
        assert_eq!(decoded, &frame);
    }

    #[test]
    fn position_round_trip() {
        let bits = 24u32;
        for &(t, o) in &[
            (0u32, 0u32),
            (1, 0),
            (0, 1),
            (12345, 67890),
            (u32::MAX >> 8, (1u32 << bits) - 1),
        ] {
            let p = position(t, o, bits);
            assert_eq!(
                term_id_from_position(p, bits),
                t,
                "term_id failed for ({t}, {o})"
            );
            assert_eq!(
                term_offset_from_position(p, bits),
                o,
                "term_offset failed for ({t}, {o})"
            );
        }
    }

    #[test]
    fn position_advances_monotonically() {
        let bits = 24u32;
        let term_length = 1u32 << bits;
        // Last byte of term 0 → first byte of term 1.
        let p0 = position(0, term_length - 1, bits);
        let p1 = position(1, 0, bits);
        assert_eq!(p1, p0 + 1);
    }

    #[test]
    fn term_length_bits_validates_input() {
        assert_eq!(term_length_bits(64 * 1024), Some(16));
        assert_eq!(term_length_bits(16 * 1024 * 1024), Some(24));
        assert_eq!(term_length_bits(1024 * 1024 * 1024), Some(30));
        // Not power of two
        assert_eq!(term_length_bits(1000), None);
        assert_eq!(term_length_bits(96 * 1024), None);
        // Below MIN
        assert_eq!(term_length_bits(32 * 1024), None);
        // Above MAX (next power of two above 1 GiB)
        assert_eq!(term_length_bits(2u32 * 1024 * 1024 * 1024), None);
        // Zero
        assert_eq!(term_length_bits(0), None);
    }

    #[test]
    fn parse_data_frame_returns_view_with_payload() {
        let mut buf = AlignedBuf::<48>::new();
        let frame = DataFrame::new(11, 22, 33, 44, data_flags::UNFRAGMENTED, 8);
        buf.as_mut()[..32].copy_from_slice(bytemuck::bytes_of(&frame));
        buf.as_mut()[32..40].copy_from_slice(b"hello!\x01\x02");

        match parse_frame(buf.as_ref()) {
            Ok(FrameView::Data { header, payload }) => {
                assert_eq!(header, &frame);
                assert_eq!(payload, b"hello!\x01\x02");
            }
            other => panic!("expected Data view, got {other:?}"),
        }
    }

    #[test]
    fn parse_nak_frame() {
        let mut buf = AlignedBuf::<28>::new();
        let frame = NakFrame::new(1, 2, 3, 4, 5);
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        match parse_frame(buf.as_ref()) {
            Ok(FrameView::Nak(n)) => assert_eq!(n, &frame),
            other => panic!("expected Nak view, got {other:?}"),
        }
    }

    #[test]
    fn parse_status_message() {
        let mut buf = AlignedBuf::<40>::new();
        let frame = StatusMessage::new(1, 2, 3, 4, 8192, 999);
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        match parse_frame(buf.as_ref()) {
            Ok(FrameView::StatusMessage(s)) => assert_eq!(s, &frame),
            other => panic!("expected StatusMessage view, got {other:?}"),
        }
    }

    #[test]
    fn parse_setup_frame() {
        let mut buf = AlignedBuf::<32>::new();
        let frame = SetupFrame::new(1, 2, 0, 0, 0, 16 * 1024 * 1024);
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        match parse_frame(buf.as_ref()) {
            Ok(FrameView::Setup(s)) => assert_eq!(s, &frame),
            other => panic!("expected Setup view, got {other:?}"),
        }
    }

    #[test]
    fn parse_heartbeat() {
        let mut buf = AlignedBuf::<16>::new();
        let frame = HeartbeatFrame::new(1, 2);
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        match parse_frame(buf.as_ref()) {
            Ok(FrameView::Heartbeat(h)) => assert_eq!(h, &frame),
            other => panic!("expected Heartbeat view, got {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_buffer_smaller_than_common_header() {
        let buf = AlignedBuf::<4>::new();
        assert_eq!(
            parse_frame(buf.as_ref()),
            Err(ParseError::BufferTooSmall { needed: 8, got: 4 })
        );
    }

    #[test]
    fn parse_rejects_bad_version() {
        let mut buf = AlignedBuf::<32>::new();
        let mut frame = DataFrame::new(1, 2, 3, 4, data_flags::UNFRAGMENTED, 0);
        frame.common.version = 99;
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        assert_eq!(parse_frame(buf.as_ref()), Err(ParseError::BadVersion(99)));
    }

    #[test]
    fn parse_rejects_unknown_frame_type() {
        let mut buf = AlignedBuf::<32>::new();
        let mut frame = DataFrame::new(1, 2, 3, 4, data_flags::UNFRAGMENTED, 0);
        frame.common.frame_type = 0xABCD;
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        assert_eq!(
            parse_frame(buf.as_ref()),
            Err(ParseError::UnknownFrameType(0xABCD))
        );
    }

    #[test]
    fn parse_rejects_frame_length_exceeding_buffer() {
        let mut buf = AlignedBuf::<32>::new();
        let mut frame = DataFrame::new(1, 2, 3, 4, data_flags::UNFRAGMENTED, 0);
        // Claim a 64-byte frame but only supply 32 bytes of buffer.
        frame.common.frame_length = 64;
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        assert_eq!(
            parse_frame(buf.as_ref()),
            Err(ParseError::BufferTooSmall {
                needed: 64,
                got: 32,
            })
        );
    }

    #[test]
    fn parse_rejects_frame_length_below_common_header() {
        let mut buf = AlignedBuf::<32>::new();
        let mut frame = DataFrame::new(1, 2, 3, 4, data_flags::UNFRAGMENTED, 0);
        frame.common.frame_length = 4; // shorter than HeaderCommon
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        assert_eq!(
            parse_frame(buf.as_ref()),
            Err(ParseError::LengthMismatch {
                expected: 8,
                got: 4,
            })
        );
    }

    #[test]
    fn parse_rejects_nak_with_oversized_length() {
        let mut buf = AlignedBuf::<40>::new();
        let mut frame = NakFrame::new(1, 2, 3, 4, 5);
        // Inflate the frame_length above NAK's fixed 28-byte size so the
        // length-mismatch path fires (the buffer is big enough).
        frame.common.frame_length = 40;
        buf.as_mut()[..28].copy_from_slice(bytemuck::bytes_of(&frame));
        assert_eq!(
            parse_frame(buf.as_ref()),
            Err(ParseError::LengthMismatch {
                expected: NakFrame::HEADER_LEN,
                got: 40,
            })
        );
    }

    #[test]
    fn parse_data_with_zero_payload() {
        // A zero-payload data frame is a valid term-end-padding pattern:
        // sender writes a 32-byte header to mark the rest of a term as
        // skipped before rotation.
        let mut buf = AlignedBuf::<32>::new();
        let frame = DataFrame::new(1, 2, 3, 4, data_flags::UNFRAGMENTED, 0);
        buf.as_mut().copy_from_slice(bytemuck::bytes_of(&frame));
        match parse_frame(buf.as_ref()) {
            Ok(FrameView::Data { header, payload }) => {
                assert_eq!(header, &frame);
                assert!(payload.is_empty());
                assert_eq!(header.common.frame_length, 32);
            }
            other => panic!("expected Data view, got {other:?}"),
        }
    }

    #[test]
    fn parse_data_ignores_trailing_bytes_past_frame_length() {
        // Ethernet pads UDP packets up to the 60-byte minimum frame size.
        // The receiver gets those padding bytes appended; parse_frame must
        // ignore them rather than treat them as payload or fail.
        let mut buf = AlignedBuf::<64>::new();
        let frame = DataFrame::new(1, 2, 3, 4, data_flags::UNFRAGMENTED, 8);
        buf.as_mut()[..32].copy_from_slice(bytemuck::bytes_of(&frame));
        buf.as_mut()[32..40].copy_from_slice(b"payload!");
        // bytes 40..64 are zero (Ethernet pad).
        match parse_frame(buf.as_ref()) {
            Ok(FrameView::Data { header, payload }) => {
                assert_eq!(header, &frame);
                assert_eq!(payload, b"payload!");
                assert_eq!(payload.len(), 8);
            }
            other => panic!("expected Data view, got {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_misaligned_buffer() {
        // HeaderCommon requires 4-byte alignment. An AlignedBuf is 8-byte
        // aligned, so &buf[1..] is at base+1 — guaranteed misaligned.
        let mut aligned = AlignedBuf::<33>::new();
        let frame = HeartbeatFrame::new(1, 2);
        aligned.as_mut()[1..17].copy_from_slice(bytemuck::bytes_of(&frame));
        let misaligned = &aligned.as_ref()[1..17];
        assert_eq!(parse_frame(misaligned), Err(ParseError::Misaligned));
    }

    #[test]
    fn position_helpers_usable_in_const_context() {
        // Compile-time check that the position helpers can be used in
        // const initializers — useful for hard-coding stream parameters.
        const BITS: u32 = 24;
        const POS: u64 = position(7, 1024, BITS);
        const TID: u32 = term_id_from_position(POS, BITS);
        const OFF: u32 = term_offset_from_position(POS, BITS);
        assert_eq!(TID, 7);
        assert_eq!(OFF, 1024);
    }
}
