//! Binary codec for journal entries.
//!
//! Manual serialization (no serde) for zero allocation, predictable
//! layout, and no format-stability concerns across dependency versions.
//!
//! ## File header (one sector, written once at creation)
//!
//! The header occupies the first [`ENTRY_OFFSET`] bytes on disk. The
//! meaningful fields fit in the first 52 bytes; the remainder is
//! zero-padded.
//!
//! | Field             | Type     | Bytes | Purpose                             |
//! |-------------------|----------|-------|-------------------------------------|
//! | file_magic        | u32      | 4     | `0x4A4F5552` ("JOUR")               |
//! | format_version    | u16      | 2     | Current version = 14                |
//! | sector_size       | u16      | 2     | Always [`MAX_SECTOR_SIZE`] (4096)   |
//! | starting_sequence | u64      | 8     | Sequence of this segment's first entry |
//! | anchor_hash       | [u8; 32] | 32    | Chain anchor: random salt (fresh journal) or previous segment's tail hash (rotation) |
//! | header_crc        | u32      | 4     | CRC32C of the preceding 48 bytes    |
//!
//! The anchor seeds the segment's BLAKE3 hash chain (see
//! [`crate::chain`]); chain metadata lives *only* here — the entry
//! stream contains application events exclusively, so sequence numbers
//! are dense over user-visible entries.
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
//! The journal reserves the low tag range for transport-intrinsic
//! events. Tags ≥ `TAG_APP` are opaque to the journal and carry
//! `E::encode` payloads: app codecs may use any internal tag layout they
//! like inside that payload.
//!
//! | Tag  | Variant              |
//! |------|----------------------|
//! | 0x01 | retired (`GenesisHash`, ≤ v13 — anchor now lives in the file header) |
//! | 0x02 | retired (`Checkpoint`, ≤ v13 — chain is schedule-free, no in-stream seals) |
//! | 0x03 | `Tick`               |
//! | 0x80 | `App(E)` (dispatches to [`AppEvent::encode`]) |

use melin_app::AppEvent;
use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

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
///
/// v12 → v13: entry offset fixed at [`ENTRY_OFFSET`] (= [`MAX_SECTOR_SIZE`]
/// = 4096) regardless of the device's logical sector size, so journals
/// can be opened under either [`crate::SectorWriter`] (O_DIRECT) or
/// [`crate::BufferedWriter`] (page cache + fdatasync) without
/// recreation. The header's `sector_size` field is now always 4096 in
/// newly-written files; SectorWriter derives its O_DIRECT alignment
/// from the device (`detect_sector_size`) rather than the header.
///
/// v13 → v14: hash-chain metadata moved out of the entry stream. The
/// file header gained `starting_sequence`, `anchor_hash`, and its own
/// CRC; the `GenesisHash` (0x01) and `Checkpoint` (0x02) entry tags were
/// retired. The chain is anchored per segment and schedule-free —
/// `chain(S) = BLAKE3(entry bytes ≤ S || anchor)` (see [`crate::chain`]).
pub const FORMAT_VERSION: u16 = 14;

/// Entry magic bytes for corruption/misalignment detection.
const ENTRY_MAGIC: u16 = 0x4A45;

// --- Wire structs ---
//
// `little_endian::U{16,32,64}` are 1-byte-aligned LE wrappers, so a
// `repr(C)` struct of them is byte-packed (no padding) and serialises
// bit-for-bit identically to the previous hand-rolled `to_le_bytes`
// chains. The on-disk layout is authoritative — `const _: () = assert!`
// below pins it. Reordering or extending these structs would silently
// break compatibility with journals on disk written by older builds, so
// we fail the compile instead.

/// File header (52 bytes of meaningful fields; on disk, padded to
/// [`ENTRY_OFFSET`]).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct FileHeader {
    file_magic: U32,
    format_version: U16,
    /// Physical sector size used when the journal was created, in bytes.
    /// Always [`MAX_SECTOR_SIZE`] in v13+ journals; readers treat 0 as
    /// 512 for backward compatibility with pre-v13 layouts.
    sector_size: U16,
    /// Sequence number carried by this segment's first entry. 1 for a
    /// fresh journal; the rotation boundary's next sequence for archived
    /// and rotated-in segments.
    starting_sequence: U64,
    /// BLAKE3 chain anchor for this segment: random salt for a fresh
    /// journal, the previous segment's tail chain hash after rotation.
    /// All-zeros only when a build without the `hash-chain` feature
    /// rotated this segment in (it has no tail hash to anchor to).
    anchor_hash: [u8; 32],
    /// CRC32C over all preceding header bytes. The header is written
    /// once and never modified, so a mismatch means storage corruption —
    /// in particular it protects the anchor, which every chain
    /// verification depends on.
    header_crc: U32,
}

/// Decoded file-header fields returned by [`decode_file_header`].
///
/// No `version` field: [`decode_file_header`] accepts only
/// [`FORMAT_VERSION`], so the gate inside it is the single source of
/// truth and nothing downstream branches on a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeaderInfo {
    /// Byte offset where entries begin (one header reservation).
    pub sector_size: usize,
    /// Sequence number of the segment's first entry.
    pub starting_sequence: u64,
    /// Chain anchor for this segment (zeros only for segments rotated
    /// in by a build without `hash-chain`).
    pub anchor_hash: [u8; 32],
}

/// Per-entry fixed prefix (20 bytes). `length` covers everything after
/// this header up to (but not including) the CRC trailer. `pub(crate)`
/// because `reader.rs` peeks at `length` and `sequence` to advance past
/// entries without doing a full decode.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(crate) struct EntryHeader {
    pub(crate) magic: U16,
    pub(crate) length: U16,
    pub(crate) sequence: U64,
    pub(crate) timestamp_ns: U64,
}

/// Per-entry metadata (17 bytes) sitting inside the length-protected
/// region. The variable-length event payload follows.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct EntryMetadata {
    key_hash: U64,
    request_seq: U64,
    event_tag: u8,
}

/// Minimum on-disk reservation for the file header — the size of the
/// meaningful `FileHeader` fields rounded up to the minimum sector size.
///
/// The actual on-disk reservation is one full sector (512 or 4096 bytes
/// depending on the device). `FILE_HEADER_SIZE` is a floor used for
/// backward-compatible header reads — always reading at least this many
/// bytes is sufficient to decode all header fields regardless of the
/// device sector size when the journal was created.
pub const FILE_HEADER_SIZE: usize = 512;

/// Maximum physical sector size supported. Journal buffers are aligned to
/// this value so O_DIRECT writes are valid on both 512-byte and 4096-byte
/// (4Kn) NVMe drives without re-opening the file.
pub const MAX_SECTOR_SIZE: usize = 4096;

/// Fixed on-disk offset of the first journal entry. Both writers reserve
/// this many bytes for the file header (most of which is zero padding)
/// so journals are interchangeable between writer modes regardless of
/// the device's logical sector size. Equal to [`MAX_SECTOR_SIZE`] so
/// O_DIRECT writes on a 4Kn drive start at exactly one sector.
pub const ENTRY_OFFSET: u64 = MAX_SECTOR_SIZE as u64;

/// Size of the meaningful `FileHeader` fields (the rest of the on-disk
/// reservation is zero-padded).
const FILE_HEADER_FIELDS_SIZE: usize = core::mem::size_of::<FileHeader>();

/// Entry header size: magic(2) + length(2) + sequence(8) + timestamp(8) = 20.
pub(crate) const ENTRY_HEADER_SIZE: usize = core::mem::size_of::<EntryHeader>();

/// Entry metadata size: key_hash(8) + request_seq(8) + tag(1) = 17.
/// The journal's `length` field covers `ENTRY_META_SIZE + payload_len`,
/// so consumers that derive payload size from `length` (replication
/// wire) need this constant.
pub const ENTRY_META_SIZE: usize = core::mem::size_of::<EntryMetadata>();

/// CRC32C checksum size in bytes.
pub(crate) const CRC_SIZE: usize = 4;

const _: () = assert!(FILE_HEADER_FIELDS_SIZE == 52);
const _: () = assert!(FILE_HEADER_SIZE >= FILE_HEADER_FIELDS_SIZE);
const _: () = assert!(ENTRY_HEADER_SIZE == 20);
const _: () = assert!(ENTRY_META_SIZE == 17);

/// Event tag space — 0x01..0x7F reserved for transport-intrinsic
/// variants, 0x80 and above for `App(E)` payloads. Tags 0x01
/// (`GenesisHash`) and 0x02 (`Checkpoint`) were retired in v14 — chain
/// metadata lives in the file header now. Do not reuse them: a v14
/// reader pointed at a ≤ v13 file fails fast at the header version
/// check, but distinct tags keep any forensic byte-level inspection
/// unambiguous.
const TAG_TICK: u8 = 0x03;
/// Replication fencing epoch bump (see [`JournalEvent::EpochBump`]).
/// Payload is the 8-byte little-endian epoch.
///
/// Added *without* a `FORMAT_VERSION` bump — a deliberate trade-off. The
/// tag is additive (this binary reads pre-fencing v14 journals
/// unchanged), and bumping the version would orphan every existing v14
/// journal behind the strict equality gate. The cost: a binary *older*
/// than this tag replaying a post-promotion journal fails with
/// `CorruptEntry("unknown event tag")` rather than `UnsupportedVersion`.
/// Rollback across a promotion therefore needs the operator note in
/// `docs/replication.md` (roll forward, or restart from a snapshot) —
/// the journal itself is healthy.
const TAG_EPOCH_BUMP: u8 = 0x04;
const TAG_APP: u8 = 0x80;

/// Bytes after the header + key_hash + request_seq reserved for the
/// event payload, excluding the CRC. The `length` field is a `u16` and
/// covers `key_hash(8) + request_seq(8) + tag(1) + payload`, so the
/// payload itself can grow to `u16::MAX - 17 ≈ 65 518` bytes before the
/// frame overflows. App codecs may assume their `encoded_size` fits.
pub const MAX_PAYLOAD_SIZE: usize = u16::MAX as usize - 17;

/// Encode the file header into `buf`.
///
/// `buf` must be exactly `sector_size` bytes long. Writes the meaningful
/// fields into the first `FILE_HEADER_FIELDS_SIZE` bytes and zero-fills
/// the rest, so the buffer can be written directly as one sector-aligned
/// O_DIRECT pwrite. `sector_size` must be 512 or 4096.
///
/// `starting_sequence` is the sequence the segment's first entry will
/// carry; `anchor_hash` seeds the segment's hash chain (zeros when the
/// `hash-chain` feature is off).
pub fn encode_file_header(
    buf: &mut [u8],
    sector_size: usize,
    starting_sequence: u64,
    anchor_hash: [u8; 32],
) {
    debug_assert!(
        sector_size == 512 || sector_size == 4096,
        "sector_size must be 512 or 4096, got {sector_size}"
    );
    debug_assert_eq!(
        buf.len(),
        sector_size,
        "buf must be exactly sector_size bytes"
    );
    let header = FileHeader::mut_from_bytes(&mut buf[..FILE_HEADER_FIELDS_SIZE])
        .expect("FILE_HEADER_FIELDS_SIZE slice matches struct size");
    header.file_magic = U32::new(FILE_MAGIC);
    header.format_version = U16::new(FORMAT_VERSION);
    header.sector_size = U16::new(sector_size as u16);
    header.starting_sequence = U64::new(starting_sequence);
    header.anchor_hash = anchor_hash;
    let crc_offset = FILE_HEADER_FIELDS_SIZE - CRC_SIZE;
    let crc = crc32c::crc32c(&buf[..crc_offset]);
    let header = FileHeader::mut_from_bytes(&mut buf[..FILE_HEADER_FIELDS_SIZE])
        .expect("FILE_HEADER_FIELDS_SIZE slice matches struct size");
    header.header_crc = U32::new(crc);
    buf[FILE_HEADER_FIELDS_SIZE..].fill(0);
}

/// Validate a file header. Returns the decoded fields on success.
pub fn decode_file_header(buf: &[u8]) -> Result<FileHeaderInfo, JournalError> {
    let header = FileHeader::ref_from_prefix(buf)
        .map_err(|_| JournalError::TruncatedEntry)?
        .0;
    if header.file_magic.get() != FILE_MAGIC {
        return Err(JournalError::InvalidFile);
    }
    // Pre-production: only the current version is accepted. Older
    // formats can be revived later as the on-disk format stabilises.
    let version = header.format_version.get();
    if version != FORMAT_VERSION {
        return Err(JournalError::UnsupportedVersion { version });
    }
    // CRC over everything before the trailer. Validated before any field
    // is trusted — the anchor in particular is the root of all chain
    // verification, so a corrupted header must fail loudly rather than
    // cascade into a bogus `SegmentChainBreak` later.
    let crc_offset = FILE_HEADER_FIELDS_SIZE - CRC_SIZE;
    let actual_crc = crc32c::crc32c(&buf[..crc_offset]);
    if header.header_crc.get() != actual_crc {
        return Err(JournalError::ChecksumMismatch {
            sequence: 0,
            expected: header.header_crc.get(),
            actual: actual_crc,
        });
    }
    let sector_size = match header.sector_size.get() {
        // Legacy journals written before dynamic sector detection: assume 512.
        0 => 512,
        512 | 4096 => header.sector_size.get() as usize,
        // Unknown sector size: reject rather than silently falling back to 512,
        // which would cause EINVAL on the first O_DIRECT write to a 4Kn journal
        // that the caller incorrectly decoded as 512-byte.
        _ => return Err(JournalError::InvalidFile),
    };
    Ok(FileHeaderInfo {
        sector_size,
        starting_sequence: header.starting_sequence.get(),
        anchor_hash: header.anchor_hash,
    })
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
    // Layout: [EntryHeader: 20][EntryMetadata: 17][payload: var][CRC: 4].
    // Header back-filled at the end (length depends on payload size);
    // metadata back-filled in one block once the tag is known.
    let payload_start = ENTRY_HEADER_SIZE + ENTRY_META_SIZE;
    let mut pos = payload_start;

    let event_tag = match event {
        JournalEvent::Tick { now_ns } => {
            le::put_u64(&mut buf[pos..], *now_ns);
            pos += 8;
            TAG_TICK
        }
        JournalEvent::EpochBump { epoch } => {
            le::put_u64(&mut buf[pos..], *epoch);
            pos += 8;
            TAG_EPOCH_BUMP
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
        JournalEvent::Shutdown => {
            // Shutdown is a transient pipeline sentinel — never persisted.
            // The journal stage must filter it before reaching the codec;
            // this arm is a safety net that surfaces a clear error if
            // anyone bypasses that filter.
            return Err(JournalError::CorruptEntry {
                sequence,
                reason: "JournalEvent::Shutdown must not reach the codec",
            });
        }
    };

    // `length` covers key_hash(8) + request_seq(8) + event_tag(1) + payload.
    let length = pos - ENTRY_HEADER_SIZE;
    let length_u16 = u16::try_from(length).map_err(|_| JournalError::CorruptEntry {
        sequence,
        reason: "encoded payload exceeds u16 max",
    })?;

    let meta = EntryMetadata::mut_from_bytes(
        &mut buf[ENTRY_HEADER_SIZE..ENTRY_HEADER_SIZE + ENTRY_META_SIZE],
    )
    .expect("ENTRY_META_SIZE slice matches struct size");
    meta.key_hash = U64::new(key_hash);
    meta.request_seq = U64::new(request_seq);
    meta.event_tag = event_tag;

    let header = EntryHeader::mut_from_bytes(&mut buf[..ENTRY_HEADER_SIZE])
        .expect("ENTRY_HEADER_SIZE slice matches struct size");
    header.magic = U16::new(ENTRY_MAGIC);
    header.length = U16::new(length_u16);
    header.sequence = U64::new(sequence);
    header.timestamp_ns = U64::new(timestamp_ns);

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
/// Entry layout is versioned by the file header alone —
/// [`decode_file_header`] rejects anything but [`FORMAT_VERSION`], so by
/// the time entries are decoded the layout is known.
pub fn decode<E: AppEvent>(buf: &[u8]) -> Result<DecodedEntry<E>, JournalError> {
    if buf.len() < ENTRY_HEADER_SIZE + 1 + CRC_SIZE {
        return Err(JournalError::TruncatedEntry);
    }

    let header = EntryHeader::ref_from_prefix(buf)
        .map_err(|_| JournalError::TruncatedEntry)?
        .0;
    if header.magic.get() != ENTRY_MAGIC {
        return Err(JournalError::CorruptEntry {
            sequence: 0,
            reason: "bad entry magic",
        });
    }

    let payload_len = header.length.get() as usize;
    let total_len = ENTRY_HEADER_SIZE + payload_len + CRC_SIZE;
    if buf.len() < total_len {
        return Err(JournalError::TruncatedEntry);
    }

    let sequence = header.sequence.get();
    let timestamp_ns = header.timestamp_ns.get();

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

    if payload_len < ENTRY_META_SIZE {
        return Err(JournalError::CorruptEntry {
            sequence,
            reason: "entry too short for key_hash + request_seq + tag",
        });
    }
    let meta =
        EntryMetadata::ref_from_bytes(&buf[ENTRY_HEADER_SIZE..ENTRY_HEADER_SIZE + ENTRY_META_SIZE])
            .expect("ENTRY_META_SIZE slice matches struct size");
    let key_hash = meta.key_hash.get();
    let request_seq = meta.request_seq.get();
    let event_tag = meta.event_tag;

    let event_payload = &buf[ENTRY_HEADER_SIZE + ENTRY_META_SIZE..data_end];

    let event = match event_tag {
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
        TAG_EPOCH_BUMP => {
            if event_payload.len() < 8 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "EpochBump payload too short",
                });
            }
            JournalEvent::EpochBump {
                epoch: le::get_u64(event_payload),
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
        let (consumed, seq, ts, kh, rs, decoded) = decode::<TestEvent>(&buf[..n]).expect("decode");
        assert_eq!(consumed, n);
        assert_eq!(seq, 42);
        assert_eq!(ts, 123_456);
        assert_eq!(kh, 0xabcd);
        assert_eq!(rs, 7);
        assert_eq!(decoded, event);
    }

    #[test]
    fn round_trip_tick() {
        round_trip(JournalEvent::Tick {
            now_ns: 1_700_000_000_000_000_000,
        });
    }

    #[test]
    fn round_trip_epoch_bump() {
        round_trip(JournalEvent::EpochBump { epoch: 0 });
        round_trip(JournalEvent::EpochBump { epoch: 1 });
        round_trip(JournalEvent::EpochBump { epoch: u64::MAX });
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
        let err = decode::<TestEvent>(&buf[..n]).unwrap_err();
        assert!(matches!(err, JournalError::CorruptEntry { .. }));
    }

    #[test]
    fn crc_mismatch_rejected() {
        let ev = JournalEvent::App::<TestEvent>(TestEvent::Payload(123));
        let mut buf = [0u8; 256];
        let n = encode(1, 0, 0, 0, &ev, &mut buf).unwrap();
        // Flip a byte inside the payload (post-header, pre-CRC).
        buf[ENTRY_HEADER_SIZE + 16 + 1] ^= 0xff;
        let err = decode::<TestEvent>(&buf[..n]).unwrap_err();
        assert!(matches!(err, JournalError::ChecksumMismatch { .. }));
    }

    #[test]
    fn truncated_input_rejected() {
        let err = decode::<TestEvent>(&[0u8; 10]).unwrap_err();
        assert!(matches!(err, JournalError::TruncatedEntry));
    }

    #[test]
    fn shutdown_sentinel_rejected_by_codec() {
        // The Shutdown variant is a pipeline-only sentinel; the journal
        // stage must filter it before encode. If anyone bypasses that
        // filter, encode must surface a clear error rather than silently
        // writing a corrupt entry.
        let mut buf = [0u8; 256];
        let err = encode(42, 0, 0, 0, &JournalEvent::Shutdown::<TestEvent>, &mut buf).unwrap_err();
        assert!(
            matches!(err, JournalError::CorruptEntry { sequence: 42, .. }),
            "expected CorruptEntry, got {err:?}"
        );
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
        let err = decode::<TestEvent>(&buf[..n]).unwrap_err();
        assert!(matches!(err, JournalError::CorruptEntry { .. }));
    }

    #[test]
    fn file_header_round_trip() {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        let anchor = [0xab; 32];
        encode_file_header(&mut buf, 512, 42, anchor);
        assert_eq!(
            decode_file_header(&buf).unwrap(),
            FileHeaderInfo {
                sector_size: 512,
                starting_sequence: 42,
                anchor_hash: anchor,
            }
        );
    }

    #[test]
    fn file_header_round_trip_4096() {
        let mut buf = [0u8; MAX_SECTOR_SIZE];
        encode_file_header(&mut buf, 4096, 1, [0u8; 32]);
        let info = decode_file_header(&buf).unwrap();
        assert_eq!(info.sector_size, 4096);
        assert_eq!(info.starting_sequence, 1);
    }

    #[test]
    fn file_header_rejects_corrupted_anchor() {
        // The anchor is the root of all chain verification — a flipped
        // bit in it must surface at header decode, not as a downstream
        // chain mismatch.
        let mut buf = [0u8; FILE_HEADER_SIZE];
        encode_file_header(&mut buf, 512, 7, [0x11; 32]);
        buf[20] ^= 0xff; // inside anchor_hash (offset 16..48)
        assert!(matches!(
            decode_file_header(&buf),
            Err(JournalError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn file_header_rejects_wrong_version() {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        encode_file_header(&mut buf, 512, 1, [0u8; 32]);
        // Bump version.
        buf[4] = buf[4].wrapping_add(1);
        assert!(matches!(
            decode_file_header(&buf),
            Err(JournalError::UnsupportedVersion { .. })
        ));
    }

    /// Pins the on-disk byte layout of a Tick entry. Sentinel u64s are
    /// chosen so each LE byte sequence is human-readable. Any future
    /// field reorder, padding insertion, or endianness flip — including
    /// "harmless" struct edits that pass roundtrip — fails this test
    /// before it can break compatibility with journals on disk.
    #[test]
    fn entry_layout_is_byte_pinned() {
        let event = JournalEvent::Tick::<TestEvent> {
            now_ns: 0x4847_4645_4443_4241,
        };
        let mut buf = [0u8; 256];
        let n = encode(
            0x2827_2625_2423_2221, // sequence
            0x3837_3635_3433_3231, // timestamp_ns
            0x0807_0605_0403_0201, // key_hash
            0x1817_1615_1413_1211, // request_seq
            &event,
            &mut buf,
        )
        .expect("encode");

        // Body: EntryHeader(20) + EntryMetadata(17) + Tick payload(8) = 45.
        // Total = 45 + CRC(4) = 49. length field = 17 + 8 = 25 = 0x19.
        let mut expected: Vec<u8> = vec![
            // EntryHeader: magic(u16) + length(u16) + sequence(u64) + timestamp_ns(u64)
            0x45, 0x4A, // ENTRY_MAGIC = 0x4A45
            0x19, 0x00, // length = 25
            0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, // sequence
            0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, // timestamp_ns
            // EntryMetadata: key_hash(u64) + request_seq(u64) + event_tag(u8)
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_hash
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // request_seq
            0x03, // TAG_TICK
            // Tick payload: now_ns(u64)
            0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
        ];
        let crc = crc32c::crc32c(&expected);
        expected.extend_from_slice(&crc.to_le_bytes());

        assert_eq!(n, 49);
        assert_eq!(
            &buf[..n],
            expected.as_slice(),
            "on-disk layout must not change"
        );
    }
}
