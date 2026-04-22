//! Binary codec for journal entries.
//!
//! Manual serialization (no serde) for zero allocation, predictable
//! layout, and no format-stability concerns across dependency versions.
//!
//! ## File header (8 bytes, written once at creation)
//!
//! | Field          | Type | Bytes | Purpose                                |
//! |----------------|------|-------|----------------------------------------|
//! | file_magic     | u32  | 4     | `0x4A4F5552` ("JOUR")                  |
//! | format_version | u16  | 2     | Current version = 12                   |
//! | reserved       | u16  | 2     | Padding for alignment, zeroed          |
//!
//! ## Entry layout (little-endian, repeats after file header)
//!
//! | Field        | Type   | Bytes | Purpose                               |
//! |--------------|--------|-------|---------------------------------------|
//! | magic        | u16    | 2     | `0x4A45` — misalignment detection     |
//! | length       | u16    | 2     | Byte count after header, before CRC   |
//! | sequence     | u64    | 8     | Monotonically increasing, starts at 1 |
//! | timestamp_ns | u64    | 8     | Wall-clock nanos since epoch           |
//! | key_hash     | u64    | 8     | FxHash of client Ed25519 pubkey       |
//! | request_seq  | u64    | 8     | Per-key request sequence               |
//! | event_tag    | u8     | 1     | Transport variant discriminant        |
//! | payload      | varies | ≤64K  | Transport-variant fields, or `E::encode` bytes for `App(e)` |
//! | crc32c       | u32    | 4     | CRC32C of all preceding bytes         |
//!
//! `length` = size of (key_hash + request_seq + event_tag + payload).
//! Total entry size = 20 + length + 4.
//!
//! ## Event tag space
//!
//! The journal reserves four tags for transport-intrinsic events. Tags
//! ≥ `TAG_APP` are opaque to the journal and carry `E::encode` payloads:
//! app codecs may use any internal tag layout they like inside that
//! payload.
//!
//! | Tag  | Variant              |
//! |------|----------------------|
//! | 0x01 | `GenesisHash`        |
//! | 0x02 | `Checkpoint`         |
//! | 0x03 | `Tick`               |
//! | 0x80 | `App(E)` (dispatches to [`AppEvent::encode`]) |

use melin_app::AppEvent;

use super::error::JournalError;
use super::event::JournalEvent;
use crate::le;

/// File magic bytes: "JOUR" in ASCII (little-endian u32).
pub const FILE_MAGIC: u32 = 0x4A4F_5552;

/// Current format version. Bumped on any layout change.
///
/// v11 → v12: `JournalEvent` made generic over `AppEvent`; transport
/// variants renumbered to the `TAG_*` constants below, app payloads
/// delegated to `AppEvent::encode` under `TAG_APP = 0x80`.
pub const FORMAT_VERSION: u16 = 12;

/// File header size in bytes.
pub const FILE_HEADER_SIZE: usize = 8;

/// Entry header size: magic(2) + length(2) + sequence(8) + timestamp(8) = 20.
pub(crate) const ENTRY_HEADER_SIZE: usize = 20;

/// Entry magic bytes for corruption/misalignment detection.
const ENTRY_MAGIC: u16 = 0x4A45;

/// CRC32C checksum size in bytes.
pub(crate) const CRC_SIZE: usize = 4;

/// Event tag space — 0x01..0x7F reserved for transport-intrinsic
/// variants, 0x80 and above for `App(E)` payloads.
const TAG_GENESIS_HASH: u8 = 0x01;
const TAG_CHECKPOINT: u8 = 0x02;
const TAG_TICK: u8 = 0x03;
const TAG_APP: u8 = 0x80;

/// Bytes after the header + key_hash + request_seq reserved for the
/// event payload, excluding the CRC. The `length` field is a `u16` and
/// covers `key_hash(8) + request_seq(8) + tag(1) + payload`, so the
/// payload itself can grow to `u16::MAX - 17 ≈ 65 518` bytes before the
/// frame overflows. App codecs may assume their `encoded_size` fits.
pub const MAX_PAYLOAD_SIZE: usize = u16::MAX as usize - 17;

/// Encode the file header into `buf`.
pub fn encode_file_header(buf: &mut [u8]) {
    buf[0..4].copy_from_slice(&FILE_MAGIC.to_le_bytes());
    buf[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes());
}

/// Validate a file header. Returns `Ok(version)` on success.
pub fn decode_file_header(buf: &[u8]) -> Result<u16, JournalError> {
    if buf.len() < FILE_HEADER_SIZE {
        return Err(JournalError::TruncatedEntry);
    }
    let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != FILE_MAGIC {
        return Err(JournalError::InvalidFile);
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    // Pre-production: only the current version is accepted. Older
    // formats can be revived later as the on-disk format stabilises.
    if version != FORMAT_VERSION {
        return Err(JournalError::UnsupportedVersion { version });
    }
    Ok(version)
}

/// Encode a journal entry into `buf`.
///
/// Returns the total number of bytes written (header + tag + payload + CRC).
/// The caller must ensure `buf` is large enough:
/// `ENTRY_HEADER_SIZE + 16 + 1 + max(transport variant size, E::encoded_size()) + CRC_SIZE`
/// always suffices. A 128-byte buffer covers every transport variant plus
/// a generously-sized app payload.
pub fn encode<E: AppEvent>(
    sequence: u64,
    timestamp_ns: u64,
    key_hash: u64,
    request_seq: u64,
    event: &JournalEvent<E>,
    buf: &mut [u8],
) -> Result<usize, JournalError> {
    // Leave room for header + key_hash(8) + request_seq(8) + event_tag(1).
    let payload_start = ENTRY_HEADER_SIZE + 16 + 1;
    let mut pos = payload_start;

    let event_tag = match event {
        JournalEvent::GenesisHash { hash } => {
            buf[pos..pos + 32].copy_from_slice(hash);
            pos += 32;
            TAG_GENESIS_HASH
        }
        JournalEvent::Checkpoint {
            chain_hash,
            events_since_checkpoint,
        } => {
            buf[pos..pos + 32].copy_from_slice(chain_hash);
            pos += 32;
            le::put_u64(&mut buf[pos..], *events_since_checkpoint);
            pos += 8;
            TAG_CHECKPOINT
        }
        JournalEvent::Tick { now_ns } => {
            le::put_u64(&mut buf[pos..], *now_ns);
            pos += 8;
            TAG_TICK
        }
        JournalEvent::App(e) => {
            let n = e.encoded_size();
            if n > MAX_PAYLOAD_SIZE {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "app event exceeds u16 length field",
                });
            }
            // Subslice passed to encode so bugs in `encoded_size` produce
            // an out-of-bounds panic at the callsite we can fix, not a
            // silent over-write of the CRC region.
            let written = e.encode(&mut buf[pos..pos + n]);
            debug_assert_eq!(written, n, "AppEvent::encode disagrees with encoded_size");
            pos += written;
            TAG_APP
        }
    };

    // `length` covers key_hash(8) + request_seq(8) + event_tag(1) + payload.
    let length = pos - ENTRY_HEADER_SIZE;
    let length_u16 = u16::try_from(length).map_err(|_| JournalError::CorruptEntry {
        sequence,
        reason: "encoded payload exceeds u16 max",
    })?;

    // Write entry header.
    let mut h = 0;
    le::put_u16(&mut buf[h..], ENTRY_MAGIC);
    h += 2;
    le::put_u16(&mut buf[h..], length_u16);
    h += 2;
    le::put_u64(&mut buf[h..], sequence);
    h += 8;
    le::put_u64(&mut buf[h..], timestamp_ns);
    h += 8;
    debug_assert_eq!(h, ENTRY_HEADER_SIZE);

    // Write key_hash, request_seq, tag.
    le::put_u64(&mut buf[ENTRY_HEADER_SIZE..], key_hash);
    le::put_u64(&mut buf[ENTRY_HEADER_SIZE + 8..], request_seq);
    buf[ENTRY_HEADER_SIZE + 16] = event_tag;

    // CRC32C over everything before the checksum.
    let crc = crc32c::crc32c(&buf[..pos]);
    le::put_u32(&mut buf[pos..], crc);
    pos += CRC_SIZE;

    Ok(pos)
}

/// Tuple returned by [`decode`]: bytes consumed, the four per-entry
/// metadata fields, and the decoded event.
pub type DecodedEntry<E> = (usize, u64, u64, u64, u64, JournalEvent<E>);

/// Decode a journal entry from `buf`.
///
/// Returns `(bytes_consumed, sequence, timestamp_ns, key_hash, request_seq, event)`.
/// The `version` parameter is reserved for future per-version layout
/// branches; the current codec accepts only [`FORMAT_VERSION`].
pub fn decode<E: AppEvent>(buf: &[u8], _version: u16) -> Result<DecodedEntry<E>, JournalError> {
    if buf.len() < ENTRY_HEADER_SIZE + 1 + CRC_SIZE {
        return Err(JournalError::TruncatedEntry);
    }

    let magic = le::get_u16(&buf[0..]);
    if magic != ENTRY_MAGIC {
        return Err(JournalError::CorruptEntry {
            sequence: 0,
            reason: "bad entry magic",
        });
    }

    let payload_len = le::get_u16(&buf[2..]) as usize;
    let total_len = ENTRY_HEADER_SIZE + payload_len + CRC_SIZE;
    if buf.len() < total_len {
        return Err(JournalError::TruncatedEntry);
    }

    let sequence = le::get_u64(&buf[4..]);
    let timestamp_ns = le::get_u64(&buf[12..]);

    let data_end = ENTRY_HEADER_SIZE + payload_len;
    let expected_crc = le::get_u32(&buf[data_end..]);
    let actual_crc = crc32c::crc32c(&buf[..data_end]);
    if expected_crc != actual_crc {
        return Err(JournalError::ChecksumMismatch {
            sequence,
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    if payload_len < 17 {
        return Err(JournalError::CorruptEntry {
            sequence,
            reason: "entry too short for key_hash + request_seq + tag",
        });
    }
    let key_hash = le::get_u64(&buf[ENTRY_HEADER_SIZE..]);
    let request_seq = le::get_u64(&buf[ENTRY_HEADER_SIZE + 8..]);
    let event_tag_offset = ENTRY_HEADER_SIZE + 16;

    let event_tag = buf[event_tag_offset];
    let event_payload = &buf[event_tag_offset + 1..data_end];

    let event = match event_tag {
        TAG_GENESIS_HASH => {
            if event_payload.len() < 32 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "GenesisHash payload too short",
                });
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&event_payload[..32]);
            JournalEvent::GenesisHash { hash }
        }
        TAG_CHECKPOINT => {
            if event_payload.len() < 40 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "Checkpoint payload too short",
                });
            }
            let mut chain_hash = [0u8; 32];
            chain_hash.copy_from_slice(&event_payload[..32]);
            let events_since_checkpoint = le::get_u64(&event_payload[32..]);
            JournalEvent::Checkpoint {
                chain_hash,
                events_since_checkpoint,
            }
        }
        TAG_TICK => {
            if event_payload.len() < 8 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "Tick payload too short",
                });
            }
            JournalEvent::Tick {
                now_ns: le::get_u64(event_payload),
            }
        }
        TAG_APP => {
            let e = E::decode(event_payload).map_err(|codec_err| JournalError::CorruptEntry {
                sequence,
                reason: codec_err_reason(codec_err),
            })?;
            JournalEvent::App(e)
        }
        _ => {
            return Err(JournalError::CorruptEntry {
                sequence,
                reason: "unknown event tag",
            });
        }
    };

    Ok((
        total_len,
        sequence,
        timestamp_ns,
        key_hash,
        request_seq,
        event,
    ))
}

/// Flatten a [`melin_app::CodecError`] into a static reason string so it
/// can live inside [`JournalError::CorruptEntry`] without forcing the
/// journal error enum to carry a generic payload.
fn codec_err_reason(e: melin_app::CodecError) -> &'static str {
    match e {
        melin_app::CodecError::UnknownTag(_) => "app codec: unknown event tag",
        melin_app::CodecError::Truncated => "app codec: truncated event",
        melin_app::CodecError::InvalidField => "app codec: invalid field",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_app::CodecError;

    /// Minimal `AppEvent` for codec round-trip coverage — two variants,
    /// one zero-payload, one carrying a `u64`. Real apps ship their own.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestEvent {
        Ping,
        Payload(u64),
    }

    impl AppEvent for TestEvent {
        fn encoded_size(&self) -> usize {
            match self {
                TestEvent::Ping => 1,
                TestEvent::Payload(_) => 9, // tag + u64
            }
        }

        fn encode(&self, buf: &mut [u8]) -> usize {
            match self {
                TestEvent::Ping => {
                    buf[0] = 0;
                    1
                }
                TestEvent::Payload(v) => {
                    buf[0] = 1;
                    le::put_u64(&mut buf[1..], *v);
                    9
                }
            }
        }

        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            if buf.is_empty() {
                return Err(CodecError::Truncated);
            }
            match buf[0] {
                0 => Ok(TestEvent::Ping),
                1 => {
                    if buf.len() < 9 {
                        return Err(CodecError::Truncated);
                    }
                    Ok(TestEvent::Payload(le::get_u64(&buf[1..])))
                }
                other => Err(CodecError::UnknownTag(other)),
            }
        }

        fn is_query(&self) -> bool {
            false
        }
    }

    fn round_trip(event: JournalEvent<TestEvent>) {
        let mut buf = [0u8; 256];
        let n = encode(42, 123_456, 0xabcd, 7, &event, &mut buf).expect("encode");
        let (consumed, seq, ts, kh, rs, decoded) =
            decode::<TestEvent>(&buf[..n], FORMAT_VERSION).expect("decode");
        assert_eq!(consumed, n);
        assert_eq!(seq, 42);
        assert_eq!(ts, 123_456);
        assert_eq!(kh, 0xabcd);
        assert_eq!(rs, 7);
        assert_eq!(decoded, event);
    }

    #[test]
    fn round_trip_genesis() {
        let hash = [0x5a; 32];
        round_trip(JournalEvent::GenesisHash { hash });
    }

    #[test]
    fn round_trip_checkpoint() {
        let chain_hash = [0xff; 32];
        round_trip(JournalEvent::Checkpoint {
            chain_hash,
            events_since_checkpoint: 100_000,
        });
    }

    #[test]
    fn round_trip_tick() {
        round_trip(JournalEvent::Tick {
            now_ns: 1_700_000_000_000_000_000,
        });
    }

    #[test]
    fn round_trip_app_ping() {
        round_trip(JournalEvent::App(TestEvent::Ping));
    }

    #[test]
    fn round_trip_app_payload() {
        round_trip(JournalEvent::App(TestEvent::Payload(u64::MAX)));
    }

    #[test]
    fn bad_entry_magic_rejected() {
        let ev = JournalEvent::App::<TestEvent>(TestEvent::Ping);
        let mut buf = [0u8; 256];
        let n = encode(1, 0, 0, 0, &ev, &mut buf).unwrap();
        // Corrupt the entry magic.
        buf[0] = 0;
        buf[1] = 0;
        let err = decode::<TestEvent>(&buf[..n], FORMAT_VERSION).unwrap_err();
        assert!(matches!(err, JournalError::CorruptEntry { .. }));
    }

    #[test]
    fn crc_mismatch_rejected() {
        let ev = JournalEvent::App::<TestEvent>(TestEvent::Payload(123));
        let mut buf = [0u8; 256];
        let n = encode(1, 0, 0, 0, &ev, &mut buf).unwrap();
        // Flip a byte inside the payload (post-header, pre-CRC).
        buf[ENTRY_HEADER_SIZE + 16 + 1] ^= 0xff;
        let err = decode::<TestEvent>(&buf[..n], FORMAT_VERSION).unwrap_err();
        assert!(matches!(err, JournalError::ChecksumMismatch { .. }));
    }

    #[test]
    fn truncated_input_rejected() {
        let err = decode::<TestEvent>(&[0u8; 10], FORMAT_VERSION).unwrap_err();
        assert!(matches!(err, JournalError::TruncatedEntry));
    }

    #[test]
    fn unknown_tag_rejected() {
        let ev = JournalEvent::App::<TestEvent>(TestEvent::Ping);
        let mut buf = [0u8; 256];
        let n = encode(1, 0, 0, 0, &ev, &mut buf).unwrap();
        // Overwrite the event tag with an unknown value and recompute
        // the CRC so the frame parses past the CRC check.
        let tag_offset = ENTRY_HEADER_SIZE + 16;
        buf[tag_offset] = 0x7f;
        let data_end = n - CRC_SIZE;
        let new_crc = crc32c::crc32c(&buf[..data_end]);
        le::put_u32(&mut buf[data_end..], new_crc);
        let err = decode::<TestEvent>(&buf[..n], FORMAT_VERSION).unwrap_err();
        assert!(matches!(err, JournalError::CorruptEntry { .. }));
    }

    #[test]
    fn file_header_round_trip() {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        encode_file_header(&mut buf);
        assert_eq!(decode_file_header(&buf).unwrap(), FORMAT_VERSION);
    }

    #[test]
    fn file_header_rejects_wrong_version() {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        encode_file_header(&mut buf);
        // Bump version.
        buf[4] = buf[4].wrapping_add(1);
        assert!(matches!(
            decode_file_header(&buf),
            Err(JournalError::UnsupportedVersion { .. })
        ));
    }
}
