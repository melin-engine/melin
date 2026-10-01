//! Wire format for input replication (`InputBatch` frames).
//!
//! A slot is a journal entry minus its 2-byte magic, CRC trailer
//! included. The per-slot `length` field has the journal codec's
//! semantics (covers `ENTRY_META_SIZE + payload`), so the journal stage
//! ships the bytes it just encoded verbatim — the slice from after the
//! entry's magic to the end of its CRC is the slot, byte for byte — and
//! journal catch-up ships entries straight out of the journal files.
//!
//! Wire layout (after a `[length:u32]` frame prefix):
//! ```text
//! [type:0x21] [count:u16]
//! for each slot:
//!   [length:u16]   ← ENTRY_META_SIZE + payload bytes (matches journal)
//!   [sequence:u64]
//!   [timestamp_ns:u64]
//!   [key_hash:u64]
//!   [event_tag:u8]
//!   [event_payload: length - ENTRY_META_SIZE bytes]
//!   [crc32c:u32]   ← the primary's journal-entry CRC (magic included)
//! ```
//!
//! # End-to-end integrity
//!
//! The transport's own checksums are not enough to carry a journal: the
//! TCP checksum is 16 bits and misses a fraction of errors, and nothing
//! covers a frame damaged in a NIC, a switch buffer, or host memory
//! before or after it. The slot CRC closes that end to end. The receiver
//! decodes each slot, re-encodes the result with the journal codec —
//! exactly what its journal stage will write — and refuses the frame
//! unless the re-encoded entry's CRC equals the one the primary shipped.
//! Equality means the entry the replica is about to apply, acknowledge
//! and journal is the one the primary journaled, which covers damage in
//! transit and an application codec whose decode→encode does not
//! reproduce its input alike.
//!
//! The check runs while decoding, before a slot can reach the input
//! ring: the matching stage consumes that ring in parallel with the
//! journal stage, and the in-memory ack is issued at publish, so a check
//! any later would run after the event was applied and acknowledged.
//!
//! On a mismatch the decoder recomputes the CRC over the bytes as
//! received to name the cause — [`InputBatchError::Corrupted`] if they no
//! longer match it, [`InputBatchError::NotRoundTrip`] if they do. That
//! second CRC is computed on the failure path only.
//!
//! `connection_id`, `publish_ts`, `recv_ts` from `InputSlot` are not on
//! the wire (primary-internal bookkeeping); the receiver reconstructs
//! them with `Default::default()`.

use std::fmt;
use std::io;

use melin_app::AppEvent;
use melin_journal::JournalEvent;
use melin_journal::codec::{self, CRC_SIZE, ENTRY_MAGIC, ENTRY_MAGIC_SIZE, ENTRY_META_SIZE};
use melin_journal::encoder::{MAX_ENTRY_SIZE, entry_size};
use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::pipeline::InputSlot;

// --- Constants ---

pub const MSG_INPUT_BATCH: u8 = 0x21;

// Slot tags 0x01 (`GenesisHash`) and 0x02 (`Checkpoint`) are retired —
// chain metadata never rides in the entry stream; divergence checks use
// dedicated chain frames instead. Do not reuse the values.
pub const SLOT_TAG_TICK: u8 = 0x03;
/// Replication fencing epoch bump (see [`JournalEvent::EpochBump`]).
/// Payload is the 8-byte little-endian epoch — kept in lockstep with the
/// journal codec's `TAG_EPOCH_BUMP` so the replica re-journals it
/// identically to the primary.
pub const SLOT_TAG_EPOCH_BUMP: u8 = 0x04;
pub const SLOT_TAG_APP: u8 = 0x80;

// --- Wire structs ---
//
// `little_endian::U{16,32,64}` are 1-byte-aligned LE wrappers, so a `repr(C)`
// struct of them is byte-packed (no padding), can be safely viewed over any
// `&[u8]` regardless of alignment, and serialises bit-for-bit identically to
// the previous hand-rolled `to_le_bytes` chains. The wire layout is
// authoritative — `const _: () = assert!(...)` below pins it.

/// `[length:u32] [type:u8] [count:u16]` — full frame preamble (length-prefixed).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct FrameHeader {
    length: U32,
    msg_type: u8,
    count: U16,
}

/// `[type:u8] [count:u16]` — bytes after the length prefix. The decoder is
/// handed the post-length payload by the framing layer, so it only sees this.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct BatchPreamble {
    msg_type: u8,
    count: U16,
}

/// Per-slot fixed prefix; the variable-length event payload and the
/// CRC trailer follow. `length` matches the journal's `length` field
/// (covers `ENTRY_META_SIZE + payload_len`); the payload size is
/// therefore `length - ENTRY_META_SIZE`. The shared semantics let the
/// journal stage hand a slice of the just-encoded journal entry directly
/// to replication, without re-encoding.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SlotHeader {
    length: U16,
    sequence: U64,
    timestamp_ns: U64,
    key_hash: U64,
    event_tag: u8,
}

/// Visible to the crate because the journal batch has to be sized so a
/// finished `InputBatch` frame fits one replication chunk, and the header
/// is part of that frame.
pub(crate) const FRAME_HEADER_LEN: usize = core::mem::size_of::<FrameHeader>();
const SLOT_HEADER_LEN: usize = core::mem::size_of::<SlotHeader>();

// Pin the wire layout. Reordering or extending these structs would silently
// break compatibility with peers running the previous build, so we fail the
// compile instead.
const _: () = assert!(FRAME_HEADER_LEN == 7);
const _: () = assert!(SLOT_HEADER_LEN == 27);
const _: () = assert!(core::mem::size_of::<BatchPreamble>() == 3);
// A slot is a journal entry minus its magic, so the slot header is the
// entry header and metadata block minus the same two bytes.
const _: () = assert!(SLOT_HEADER_LEN + ENTRY_MAGIC_SIZE == 20 + ENTRY_META_SIZE);

// --- Errors ---

/// Why [`try_decode_input_batch_into`] refused a frame.
///
/// The variants are distinct because the receiver must react to them
/// differently: damage in transit is cured by fetching the entries again,
/// an application codec that does not round-trip fails the same way on
/// every attempt, and a frame that is not an `InputBatch` is simply some
/// other message.
#[derive(Debug)]
pub enum InputBatchError {
    /// The frame's type byte is not [`MSG_INPUT_BATCH`]. Not a fault: the
    /// receive loop takes it as its cue to decode a control message.
    NotInputBatch(u8),
    /// An `InputBatch` that does not parse, with nothing marking its
    /// bytes as damaged: truncated, a slot tag this build does not know,
    /// or an event the application codec refuses although its bytes are
    /// the ones the primary journaled.
    Malformed(String),
    /// A slot's bytes are not the ones the primary journaled: the CRC
    /// over them as received disagrees with the CRC the primary shipped.
    /// Damage between the primary's journal stage and this decoder.
    Corrupted {
        sequence: u64,
        shipped_crc: u32,
        received_crc: u32,
    },
    /// A slot arrived intact, but decoding it and encoding the result
    /// again does not reproduce it — the application codec does not
    /// round-trip, and the replica would apply and journal an event other
    /// than the primary's.
    NotRoundTrip {
        sequence: u64,
        shipped_crc: u32,
        reencoded_crc: u32,
    },
}

impl fmt::Display for InputBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InputBatchError::NotInputBatch(tag) => write!(
                f,
                "expected InputBatch (0x{MSG_INPUT_BATCH:02x}), got 0x{tag:02x}"
            ),
            InputBatchError::Malformed(reason) => write!(f, "malformed InputBatch: {reason}"),
            InputBatchError::Corrupted {
                sequence,
                shipped_crc,
                received_crc,
            } => write!(
                f,
                "replicated entry {sequence} was damaged in transit: the primary journaled \
                 CRC32C {shipped_crc:#010x}, the bytes received hash to {received_crc:#010x}"
            ),
            InputBatchError::NotRoundTrip {
                sequence,
                shipped_crc,
                reencoded_crc,
            } => write!(
                f,
                "replicated entry {sequence} arrived intact but does not survive the \
                 application codec: the primary journaled CRC32C {shipped_crc:#010x}, \
                 decoding and re-encoding it gives {reencoded_crc:#010x} — AppEvent::decode \
                 followed by AppEvent::encode must reproduce the original bytes exactly"
            ),
        }
    }
}

impl std::error::Error for InputBatchError {}

impl From<InputBatchError> for io::Error {
    fn from(e: InputBatchError) -> Self {
        io::Error::other(e)
    }
}

// --- Streaming encode (used by the journal stage on the hot path) ---

/// Reset `buf` and reserve placeholder bytes for the frame header.
/// The journal stage then appends each entry's replication slice (see
/// `JournalEncoder::last_user_entry_replication_slice`) and back-fills
/// the header with [`finalize_input_batch`] before publishing.
pub fn init_input_batch(buf: &mut Vec<u8>) {
    buf.clear();
    buf.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
}

/// Append one slot's wire bytes to a buffer initialized with
/// [`init_input_batch`]. `seq` is the sequence to encode — `slot.sequence`
/// may be zero on the primary (the journal stage allocates at encode
/// time), so callers pass the allocated value explicitly.
///
/// The slot is produced by the journal codec itself, so it is exactly
/// the entry a journal would hold for this event, minus its magic — the
/// CRC trailer is the entry's own. A `Shutdown` sentinel is
/// pipeline-only and is skipped (`Ok(false)`); `Ok(true)` means a slot
/// was appended. An event the journal codec refuses (see
/// [`codec::encode`]) leaves `buf` as it was and is returned as an error.
///
/// **Caller contract**: `buf` must already contain a frame header (i.e.
/// `init_input_batch` was called, or this is being called inside
/// `encode_input_batch`). The debug assertion catches the bare-empty
/// `Vec` misuse in tests.
pub fn append_input_slot<E: AppEvent>(
    buf: &mut Vec<u8>,
    slot: &InputSlot<E>,
    seq: u64,
) -> io::Result<bool> {
    debug_assert!(
        buf.len() >= FRAME_HEADER_LEN,
        "append_input_slot requires init_input_batch first (buf.len() = {})",
        buf.len()
    );
    if slot.event.is_shutdown() {
        // Pipeline-only sentinel — never written to the wire (or the
        // journal, whose codec refuses it).
        return Ok(false);
    }

    let start = buf.len();
    buf.resize(start + entry_size::<E>(), 0);
    let written = match codec::encode(
        seq,
        slot.timestamp_ns,
        slot.key_hash,
        &slot.event,
        &mut buf[start..],
    ) {
        Ok(n) => n,
        Err(e) => {
            buf.truncate(start);
            return Err(io::Error::other(format!(
                "encode replication slot {seq}: {e}"
            )));
        }
    };
    // Drop the magic; the rest of the entry, CRC included, is the slot.
    buf.copy_within(start + ENTRY_MAGIC_SIZE..start + written, start);
    buf.truncate(start + written - ENTRY_MAGIC_SIZE);
    Ok(true)
}

/// Back-fill the frame header so `buf` is wire-ready (length-prefixed,
/// type-tagged, count populated). `slot_count` is the number of slots
/// appended since the last [`init_input_batch`].
pub fn finalize_input_batch(buf: &mut [u8], slot_count: u16) {
    debug_assert!(buf.len() >= FRAME_HEADER_LEN);
    let payload_len = u32::try_from(buf.len() - 4).expect("InputBatch payload exceeds u32");
    let header = FrameHeader::mut_from_bytes(&mut buf[..FRAME_HEADER_LEN])
        .expect("FRAME_HEADER_LEN slice matches struct size");
    header.length = U32::new(payload_len);
    header.msg_type = MSG_INPUT_BATCH;
    header.count = U16::new(slot_count);
}

// --- One-shot encode (tests and tooling that already have a slot vec) ---

/// Encode a complete length-prefixed `InputBatch` frame into `buf`,
/// appending after whatever `buf` already holds. Equivalent to
/// `init_input_batch` + `append_input_slot` per slot (with
/// `slot.sequence`) + `finalize_input_batch`.
///
/// Builds each slot by encoding the event, so it is for slots that
/// exist only in memory. Entries that already exist as journal bytes
/// ship with [`encode_input_batch_from_journal`], which does not
/// re-encode them. On error `buf` is left as it was.
pub fn encode_input_batch<E: AppEvent>(
    slots: &[InputSlot<E>],
    buf: &mut Vec<u8>,
) -> io::Result<()> {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
    // `u16` because that is the wire's count field; overflow is refused
    // rather than wrapped.
    let mut count: u16 = 0;
    for slot in slots {
        match append_input_slot(buf, slot, slot.sequence) {
            Ok(true) => {
                count = match count.checked_add(1) {
                    Some(c) => c,
                    None => {
                        buf.truncate(start);
                        return Err(io::Error::other("InputBatch slot count exceeds u16"));
                    }
                };
            }
            Ok(false) => {}
            Err(e) => {
                buf.truncate(start);
                return Err(e);
            }
        }
    }
    finalize_input_batch(&mut buf[start..], count);
    Ok(())
}

/// Encode whole journal entries — journal-codec bytes as a journal file
/// holds them, the catch-up path's input — into one length-prefixed
/// `InputBatch` frame appended to `buf`. Returns the number of slots.
///
/// Each entry ships verbatim minus its magic, so its CRC trailer is the
/// one on disk: a replica checks its own re-encode against what the
/// primary actually journaled, not against a second encode on the
/// primary, which a codec that does not round-trip would agree with.
/// Every entry is decoded first, so an entry whose on-disk CRC fails, or
/// whose event the codec refuses, stops the catch-up here instead of
/// being shipped. On error `buf` is left as it was.
pub fn encode_input_batch_from_journal<E: AppEvent>(
    journal_bytes: &[u8],
    buf: &mut Vec<u8>,
) -> io::Result<u16> {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
    // `u16`: the wire's count field — see `encode_input_batch`.
    let mut count: u16 = 0;
    let mut offset = 0;
    while offset < journal_bytes.len() {
        let entry = &journal_bytes[offset..];
        let consumed = match codec::decode::<E>(entry) {
            Ok((consumed, ..)) => consumed,
            Err(e) => {
                buf.truncate(start);
                return Err(io::Error::other(format!(
                    "journal entry at offset {offset}: {e}"
                )));
            }
        };
        count = match count.checked_add(1) {
            Some(c) => c,
            None => {
                buf.truncate(start);
                return Err(io::Error::other("InputBatch slot count exceeds u16"));
            }
        };
        buf.extend_from_slice(&entry[ENTRY_MAGIC_SIZE..consumed]);
        offset += consumed;
    }
    finalize_input_batch(&mut buf[start..], count);
    Ok(count)
}

// --- Decode ---

/// Decode an `InputBatch` frame payload (the bytes after the length
/// prefix, starting with the type byte) into a caller-supplied buffer,
/// verifying every slot against the CRC the primary journaled it with
/// (see the module docs). `connection_id`, `publish_ts`, `recv_ts` are
/// reset to defaults.
///
/// The buffer is cleared then filled; capacity is grown on demand but
/// never shrunk, so the allocator is hit at most once per batch size seen
/// so far. Prefer this over `try_decode_input_batch` on hot paths to
/// avoid per-call heap allocation.
///
/// All or nothing: on error `slots` may hold a prefix of the frame,
/// which the caller must not publish — the frame is refused whole.
pub fn try_decode_input_batch_into<E: AppEvent>(
    payload: &[u8],
    slots: &mut Vec<InputSlot<E>>,
) -> Result<(), InputBatchError> {
    match payload.first() {
        Some(&MSG_INPUT_BATCH) => {}
        Some(&other) => return Err(InputBatchError::NotInputBatch(other)),
        None => return Err(InputBatchError::Malformed("empty frame".into())),
    }
    let (preamble, mut rest) = BatchPreamble::ref_from_prefix(payload)
        .map_err(|_| InputBatchError::Malformed("header truncated".into()))?;
    let count = preamble.count.get() as usize;
    slots.clear();
    if slots.capacity() < count {
        slots.reserve(count - slots.capacity());
    }

    // The re-encode lands here. One entry wide (`MAX_ENTRY_SIZE` bounds
    // every application's entry) and on the stack, so verifying a frame
    // allocates nothing; reused for every slot of the frame.
    let mut scratch = [0u8; MAX_ENTRY_SIZE];

    for _ in 0..count {
        let (header, after_header) = SlotHeader::ref_from_prefix(rest)
            .map_err(|_| InputBatchError::Malformed("slot header truncated".into()))?;

        let length = header.length.get() as usize;
        if length < ENTRY_META_SIZE {
            return Err(InputBatchError::Malformed(
                "slot length below ENTRY_META_SIZE".into(),
            ));
        }
        let payload_size = length - ENTRY_META_SIZE;
        if after_header.len() < payload_size + CRC_SIZE {
            return Err(InputBatchError::Malformed("slot truncated".into()));
        }
        // Everything the CRC covers bar the magic: header and payload.
        let slot_body = &rest[..SLOT_HEADER_LEN + payload_size];
        let event_payload = &after_header[..payload_size];
        let shipped_crc = u32::from_le_bytes(
            after_header[payload_size..payload_size + CRC_SIZE]
                .try_into()
                .expect("CRC_SIZE-byte slice into [u8; 4]"),
        );
        rest = &after_header[payload_size + CRC_SIZE..];

        let sequence = header.sequence.get();
        let timestamp_ns = header.timestamp_ns.get();
        let key_hash = header.key_hash.get();

        let event = match decode_slot_event::<E>(header.event_tag, event_payload) {
            Ok(event) => event,
            Err(reason) => return Err(refused(sequence, slot_body, shipped_crc, reason)),
        };

        // Re-encode exactly as the journal stage will, and compare CRCs.
        let reencoded_crc =
            match codec::encode(sequence, timestamp_ns, key_hash, &event, &mut scratch) {
                Ok(n) => u32::from_le_bytes(
                    scratch[n - CRC_SIZE..n]
                        .try_into()
                        .expect("CRC_SIZE-byte slice into [u8; 4]"),
                ),
                Err(e) => {
                    return Err(refused(
                        sequence,
                        slot_body,
                        shipped_crc,
                        format!("the journal codec refuses the decoded event: {e}"),
                    ));
                }
            };
        if reencoded_crc != shipped_crc {
            let received_crc = received_crc(slot_body);
            return Err(if received_crc != shipped_crc {
                InputBatchError::Corrupted {
                    sequence,
                    shipped_crc,
                    received_crc,
                }
            } else {
                InputBatchError::NotRoundTrip {
                    sequence,
                    shipped_crc,
                    reencoded_crc,
                }
            });
        }

        slots.push(InputSlot {
            connection_id: 0,
            key_hash,
            sequence,
            timestamp_ns,
            event,
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
    }

    Ok(())
}

/// The event a slot carries, or why it has none.
fn decode_slot_event<E: AppEvent>(tag: u8, payload: &[u8]) -> Result<JournalEvent<E>, String> {
    match tag {
        SLOT_TAG_TICK => {
            let now_ns = payload
                .get(..8)
                .ok_or("Tick payload too short")?
                .try_into()
                .map(u64::from_le_bytes)
                .expect("8-byte slice into [u8; 8]");
            Ok(JournalEvent::Tick { now_ns })
        }
        SLOT_TAG_EPOCH_BUMP => {
            let epoch = payload
                .get(..8)
                .ok_or("EpochBump payload too short")?
                .try_into()
                .map(u64::from_le_bytes)
                .expect("8-byte slice into [u8; 8]");
            Ok(JournalEvent::EpochBump { epoch })
        }
        SLOT_TAG_APP => E::decode(payload)
            .map(JournalEvent::App)
            .map_err(|e| format!("app event decode failed: {e:?}")),
        other => Err(format!("unknown slot tag: 0x{other:02x}")),
    }
}

/// CRC32C of a slot's bytes as received, magic restored in front — what
/// the primary's CRC trailer was computed over if nothing changed them.
fn received_crc(slot_body: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&ENTRY_MAGIC.to_le_bytes()), slot_body)
}

/// Classify a slot that could not be turned back into an entry: damage
/// in transit if its bytes no longer match the shipped CRC, otherwise a
/// genuinely malformed (or codec-refused) entry.
fn refused(sequence: u64, slot_body: &[u8], shipped_crc: u32, reason: String) -> InputBatchError {
    let received_crc = received_crc(slot_body);
    if received_crc != shipped_crc {
        InputBatchError::Corrupted {
            sequence,
            shipped_crc,
            received_crc,
        }
    } else {
        InputBatchError::Malformed(format!("slot {sequence}: {reason}"))
    }
}

/// Decode an `InputBatch` frame payload (the bytes after the length prefix,
/// starting with the type byte), with the same verification as
/// [`try_decode_input_batch_into`]. Returns the reconstructed `InputSlot`
/// vector with `connection_id`, `publish_ts`, `recv_ts` reset to defaults.
pub fn try_decode_input_batch<E: AppEvent>(
    payload: &[u8],
) -> Result<Vec<InputSlot<E>>, InputBatchError> {
    let mut slots = Vec::new();
    try_decode_input_batch_into(payload, &mut slots)?;
    Ok(slots)
}

/// Read the journal sequence of the *first* slot in a length-prefixed
/// `InputBatch` frame — the full wire bytes as carried in a replication
/// ring chunk (`[length:u32][type:u8][count:u16][slots…]`) — without
/// decoding the rest of the batch.
///
/// Used by the catch-up→live handoff to decide, in O(1), whether the
/// ring's first uncovered chunk is contiguous with what the sender has
/// already streamed: a chunk whose first slot is more than one past the
/// high-water means the journal stage skipped entries from the ring that
/// must be back-filled from disk first. Reads only the frame header and
/// the first slot header — no allocation, no per-slot walk.
///
/// Errors if the frame is too short, isn't an `InputBatch`, or carries
/// no slots.
pub fn peek_first_sequence(frame: &[u8]) -> io::Result<u64> {
    let (header, rest) = FrameHeader::ref_from_prefix(frame)
        .map_err(|_| io::Error::other("InputBatch frame too short for header"))?;
    if header.msg_type != MSG_INPUT_BATCH {
        return Err(io::Error::other(format!(
            "expected InputBatch (0x{MSG_INPUT_BATCH:02x}), got 0x{:02x}",
            header.msg_type
        )));
    }
    if header.count.get() == 0 {
        return Err(io::Error::other("InputBatch frame carries no slots"));
    }
    let (slot, _) = SlotHeader::ref_from_prefix(rest)
        .map_err(|_| io::Error::other("InputBatch frame too short for first slot"))?;
    Ok(slot.sequence.get())
}

/// Message tag of a length-prefixed frame (the byte right after the
/// 4-byte length). Lets ring consumers distinguish `InputBatch` chunks
/// from in-stream control frames (e.g. `Rotate`) before committing to
/// an `InputBatch`-shaped parse.
pub fn peek_frame_tag(frame: &[u8]) -> io::Result<u8> {
    frame
        .get(4)
        .copied()
        .ok_or_else(|| io::Error::other("frame too short for a message tag"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_app::CodecError;

    /// Minimal AppEvent for round-trip tests. Encodes a single u32 payload.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u32);

    impl AppEvent for TestEvent {
        const MAX_ENCODED_SIZE: usize = 4;

        fn encoded_size(&self) -> usize {
            4
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[..4].copy_from_slice(&self.0.to_le_bytes());
            4
        }
        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            if buf.len() < 4 {
                return Err(CodecError::Truncated);
            }
            Ok(TestEvent(u32::from_le_bytes(
                buf[..4].try_into().expect("4-byte slice into [u8; 4]"),
            )))
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    /// An application codec that does not round-trip: `decode` drops the
    /// top bit, so any byte above 0x7F re-encodes to something else.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct LossyEvent(u8);

    impl AppEvent for LossyEvent {
        const MAX_ENCODED_SIZE: usize = 1;

        fn encoded_size(&self) -> usize {
            1
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[0] = self.0;
            1
        }
        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            buf.first()
                .map(|b| LossyEvent(b & 0x7F))
                .ok_or(CodecError::Truncated)
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    /// An application codec that cannot decode its own output.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct OneWayEvent;

    impl AppEvent for OneWayEvent {
        const MAX_ENCODED_SIZE: usize = 1;

        fn encoded_size(&self) -> usize {
            1
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[0] = 0xEE;
            1
        }
        fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
            Err(CodecError::InvalidField)
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    fn sample_slot<E: AppEvent>(sequence: u64, event: JournalEvent<E>) -> InputSlot<E> {
        InputSlot {
            connection_id: 0,
            key_hash: 0xabcd_ef00_1234_5678,
            sequence,
            timestamp_ns: 1_700_000_000_000_000_000,
            event,
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        }
    }

    fn frame<E: AppEvent>(slots: &[InputSlot<E>]) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_input_batch(slots, &mut buf).expect("encode");
        buf
    }

    /// Offset, within a whole frame, of the first slot's first byte.
    const FIRST_SLOT: usize = FRAME_HEADER_LEN;

    #[test]
    fn roundtrip_transport_variants() {
        let slots = vec![sample_slot::<TestEvent>(
            10,
            JournalEvent::Tick { now_ns: 12_345_678 },
        )];

        let buf = frame(&slots);

        let payload_len =
            u32::from_le_bytes(buf[..4].try_into().expect("4-byte slice into [u8; 4]")) as usize;
        assert_eq!(buf.len(), 4 + payload_len);
        let payload = &buf[4..];

        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(payload).expect("decode succeeds");
        assert_eq!(decoded.len(), 1);

        for (orig, dec) in slots.iter().zip(decoded.iter()) {
            assert_eq!(dec.sequence, orig.sequence);
            assert_eq!(dec.timestamp_ns, orig.timestamp_ns);
            assert_eq!(dec.key_hash, orig.key_hash);
            assert_eq!(dec.connection_id, 0);
        }

        match decoded[0].event {
            JournalEvent::Tick { now_ns } => assert_eq!(now_ns, 12_345_678),
            ref other => panic!("expected Tick, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_epoch_bump() {
        // The fencing epoch bump streams to replicas like any other entry;
        // the replica must re-journal it byte-identically to the primary.
        let buf = frame(&[sample_slot::<TestEvent>(
            11,
            JournalEvent::EpochBump { epoch: 42 },
        )]);
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(&buf[4..]).expect("decode succeeds");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].sequence, 11);
        match decoded[0].event {
            JournalEvent::EpochBump { epoch } => assert_eq!(epoch, 42),
            ref other => panic!("expected EpochBump, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_app_variant() {
        let buf = frame(&[sample_slot(7, JournalEvent::App(TestEvent(0xdead_beef)))]);
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(&buf[4..]).expect("decode succeeds");
        assert_eq!(decoded.len(), 1);
        match decoded[0].event {
            JournalEvent::App(TestEvent(v)) => assert_eq!(v, 0xdead_beef),
            ref other => panic!("expected App, got {other:?}"),
        }
    }

    #[test]
    fn empty_batch_roundtrips() {
        let buf = frame::<TestEvent>(&[]);
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(&buf[4..]).expect("decode succeeds");
        assert!(decoded.is_empty());
    }

    /// A slot is the journal entry minus its magic — byte for byte, CRC
    /// trailer included — which is what lets the journal stage ship its
    /// own bytes and the replica compare against the primary's CRC.
    #[test]
    fn a_slot_is_the_journal_entry_minus_its_magic() {
        let slot = sample_slot(9, JournalEvent::App(TestEvent(0x0102_0304)));
        let mut entry = [0u8; MAX_ENTRY_SIZE];
        let n = codec::encode(
            slot.sequence,
            slot.timestamp_ns,
            slot.key_hash,
            &slot.event,
            &mut entry,
        )
        .expect("encode entry");

        let buf = frame(std::slice::from_ref(&slot));
        assert_eq!(&buf[FIRST_SLOT..], &entry[ENTRY_MAGIC_SIZE..n]);
    }

    #[test]
    fn peek_first_sequence_reads_the_lead_slot() {
        // peek operates on the FULL length-prefixed frame (the ring-chunk
        // shape), not the post-length payload — verify it skips the frame
        // header and reads the first slot, ignoring later slots.
        let buf = frame(&[
            sample_slot(6_932_801, JournalEvent::Tick { now_ns: 1 }),
            sample_slot(6_932_802, JournalEvent::App(TestEvent(2))),
            sample_slot(6_932_803, JournalEvent::App(TestEvent(3))),
        ]);
        assert_eq!(peek_first_sequence(&buf).unwrap(), 6_932_801);
    }

    #[test]
    fn peek_first_sequence_single_slot() {
        let buf = frame(&[sample_slot(1, JournalEvent::App(TestEvent(0)))]);
        assert_eq!(peek_first_sequence(&buf).unwrap(), 1);
    }

    #[test]
    fn peek_first_sequence_rejects_malformed_frames() {
        // Too short for the frame header.
        assert!(peek_first_sequence(&[]).is_err());
        assert!(peek_first_sequence(&[0, 0, 0]).is_err());

        // Well-formed 7-byte header but wrong message type.
        let mut wrong_type = Vec::new();
        wrong_type.extend_from_slice(&28u32.to_le_bytes()); // length
        wrong_type.push(0xFF); // not MSG_INPUT_BATCH
        wrong_type.extend_from_slice(&1u16.to_le_bytes()); // count
        wrong_type.extend_from_slice(&[0u8; SLOT_HEADER_LEN]); // slot header
        assert!(peek_first_sequence(&wrong_type).is_err());

        // count=0 frame carries no first slot to peek.
        let empty = frame::<TestEvent>(&[]);
        assert!(peek_first_sequence(&empty).is_err());

        // Header claims a slot but the bytes are truncated before it.
        let mut truncated = Vec::new();
        truncated.extend_from_slice(&28u32.to_le_bytes());
        truncated.push(MSG_INPUT_BATCH);
        truncated.extend_from_slice(&1u16.to_le_bytes());
        // no slot header bytes
        assert!(peek_first_sequence(&truncated).is_err());
    }

    #[test]
    fn shutdown_sentinel_is_truncated_from_wire() {
        // The Shutdown variant is a pipeline-only sentinel. If it ever
        // reaches the wire encoder, nothing may be written for it.
        let mut buf = Vec::new();
        init_input_batch(&mut buf);
        let pre_len = buf.len();
        let sentinel = sample_slot::<TestEvent>(99, JournalEvent::Shutdown);
        assert!(!append_input_slot(&mut buf, &sentinel, sentinel.sequence).unwrap());
        assert_eq!(buf.len(), pre_len);
    }

    #[test]
    fn shutdown_sentinel_does_not_break_surrounding_slots() {
        // Real-world bug case: a sentinel slot interleaved with valid
        // slots in the same batch. The valid slots before and after must
        // round-trip, and the sentinel must be silently dropped.
        let buf = frame(&[
            sample_slot(1, JournalEvent::Tick { now_ns: 111 }),
            sample_slot(2, JournalEvent::Shutdown),
            sample_slot(3, JournalEvent::App(TestEvent(0xcafe))),
        ]);
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(&buf[4..]).expect("decode succeeds");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].sequence, 1);
        assert_eq!(decoded[1].sequence, 3);
    }

    #[test]
    fn a_frame_of_another_type_is_named_as_such() {
        let payload = [0xFF, 0x00, 0x00];
        assert!(matches!(
            try_decode_input_batch::<TestEvent>(&payload),
            Err(InputBatchError::NotInputBatch(0xFF))
        ));
        // A one-byte control frame too short for an InputBatch preamble
        // must still be recognised as "not an InputBatch".
        assert!(matches!(
            try_decode_input_batch::<TestEvent>(&[0x11]),
            Err(InputBatchError::NotInputBatch(0x11))
        ));
    }

    #[test]
    fn rejects_truncated_header() {
        let payload = [MSG_INPUT_BATCH];
        assert!(matches!(
            try_decode_input_batch::<TestEvent>(&payload),
            Err(InputBatchError::Malformed(_))
        ));
    }

    #[test]
    fn rejects_truncated_slot() {
        let buf = frame(&[sample_slot::<TestEvent>(
            1,
            JournalEvent::Tick { now_ns: 0 },
        )]);
        let payload = &buf[4..buf.len() - 1];
        assert!(matches!(
            try_decode_input_batch::<TestEvent>(payload),
            Err(InputBatchError::Malformed(_))
        ));
    }

    #[test]
    fn streaming_api_matches_one_shot() {
        let slots = vec![
            sample_slot(20, JournalEvent::Tick { now_ns: 100 }),
            sample_slot(21, JournalEvent::App(TestEvent(42))),
        ];

        let one_shot = frame(&slots);

        let mut streaming = Vec::new();
        init_input_batch(&mut streaming);
        for slot in &slots {
            assert!(append_input_slot(&mut streaming, slot, slot.sequence).unwrap());
        }
        finalize_input_batch(&mut streaming, slots.len() as u16);

        assert_eq!(one_shot, streaming);
    }

    // --- Integrity ---

    /// Flip `mask` into byte `at` of a whole frame and decode it.
    fn decode_flipped<E: AppEvent>(
        buf: &[u8],
        at: usize,
        mask: u8,
    ) -> Result<Vec<InputSlot<E>>, InputBatchError> {
        let mut damaged = buf.to_vec();
        damaged[at] ^= mask;
        try_decode_input_batch::<E>(&damaged[4..])
    }

    /// The scenario the audit names: a flipped payload bit that still
    /// decodes to a valid — different — event. Refused as corruption,
    /// with no slot returned.
    #[test]
    fn a_damaged_payload_that_still_decodes_is_refused() {
        let buf = frame(&[sample_slot(5, JournalEvent::App(TestEvent(1000)))]);
        let payload_at = FIRST_SLOT + SLOT_HEADER_LEN;
        let err = decode_flipped::<TestEvent>(&buf, payload_at, 0x01).unwrap_err();
        assert!(
            matches!(err, InputBatchError::Corrupted { sequence: 5, .. }),
            "{err:?}"
        );
    }

    /// Every field of the slot is covered, not just the payload: the
    /// sequence, timestamp and key hash are journaled too.
    #[test]
    fn damage_anywhere_in_a_slot_is_refused() {
        let buf = frame(&[sample_slot(5, JournalEvent::App(TestEvent(1000)))]);
        let slot_len = buf.len() - FIRST_SLOT;
        for offset in 0..slot_len {
            for mask in [0x01u8, 0x80] {
                let at = FIRST_SLOT + offset;
                match decode_flipped::<TestEvent>(&buf, at, mask) {
                    Err(InputBatchError::Corrupted { .. }) => {}
                    // A damaged length field can leave the slot running
                    // off the end of the frame, which is caught before
                    // there is a CRC to read.
                    Err(InputBatchError::Malformed(_)) if offset < 2 => {}
                    other => panic!("flip {mask:#04x} at slot byte {offset}: {other:?}"),
                }
            }
        }
    }

    /// A slot damaged into an unknown tag, or into an event the codec
    /// refuses, is still named as damage — the CRC tells it apart from a
    /// primary that sent something this build cannot read.
    #[test]
    fn an_undecodable_damaged_slot_is_named_as_damage() {
        let buf = frame(&[sample_slot(5, JournalEvent::App(TestEvent(1000)))]);
        let tag_at = FIRST_SLOT + SLOT_HEADER_LEN - 1;
        let err = decode_flipped::<TestEvent>(&buf, tag_at, 0x40).unwrap_err();
        assert!(matches!(err, InputBatchError::Corrupted { .. }), "{err:?}");
    }

    /// A good slot ahead of a damaged one in the same frame is not handed
    /// out either: the frame is refused whole.
    #[test]
    fn a_damaged_slot_refuses_the_whole_frame() {
        let buf = frame(&[
            sample_slot(5, JournalEvent::App(TestEvent(1))),
            sample_slot(6, JournalEvent::App(TestEvent(2))),
        ]);
        let last = buf.len() - CRC_SIZE - 1;
        let err = decode_flipped::<TestEvent>(&buf, last, 0x01).unwrap_err();
        assert!(
            matches!(err, InputBatchError::Corrupted { sequence: 6, .. }),
            "{err:?}"
        );
    }

    /// Finding 26's case: the bytes arrive exactly as the primary
    /// journaled them, but the application's decode→encode does not
    /// reproduce them. Named as a codec fault, not as damage.
    #[test]
    fn a_codec_that_does_not_round_trip_is_refused() {
        let buf = frame(&[sample_slot(8, JournalEvent::App(LossyEvent(0xC1)))]);
        let err = try_decode_input_batch::<LossyEvent>(&buf[4..]).unwrap_err();
        assert!(
            matches!(err, InputBatchError::NotRoundTrip { sequence: 8, .. }),
            "{err:?}"
        );

        // Values the lossy codec does preserve pass — the check compares
        // bytes, not types.
        let ok = frame(&[sample_slot(8, JournalEvent::App(LossyEvent(0x41)))]);
        let decoded = try_decode_input_batch::<LossyEvent>(&ok[4..]).expect("round-trips");
        assert_eq!(decoded.len(), 1);
    }

    /// Intact bytes the codec cannot decode are a codec fault, not damage.
    #[test]
    fn intact_bytes_the_codec_refuses_are_malformed() {
        let buf = frame(&[sample_slot(3, JournalEvent::App(OneWayEvent))]);
        let err = try_decode_input_batch::<OneWayEvent>(&buf[4..]).unwrap_err();
        assert!(matches!(err, InputBatchError::Malformed(_)), "{err:?}");
    }

    // --- Catch-up: journal bytes → frame ---

    fn journal_bytes<E: AppEvent>(slots: &[InputSlot<E>]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut entry = [0u8; MAX_ENTRY_SIZE];
        for s in slots {
            let n = codec::encode(s.sequence, s.timestamp_ns, s.key_hash, &s.event, &mut entry)
                .expect("encode entry");
            out.extend_from_slice(&entry[..n]);
        }
        out
    }

    #[test]
    fn journal_entries_ship_verbatim() {
        let slots = vec![
            sample_slot(1, JournalEvent::Tick { now_ns: 5 }),
            sample_slot(2, JournalEvent::App(TestEvent(77))),
            sample_slot(3, JournalEvent::EpochBump { epoch: 4 }),
        ];
        let mut from_journal = Vec::new();
        let count =
            encode_input_batch_from_journal::<TestEvent>(&journal_bytes(&slots), &mut from_journal)
                .expect("encode");
        assert_eq!(count, 3);
        // Same bytes as the in-memory path, and they decode.
        assert_eq!(from_journal, frame(&slots));
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(&from_journal[4..]).expect("decode");
        assert_eq!(
            decoded.iter().map(|s| s.sequence).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    /// Catch-up ships the CRC that is on disk, so a codec that does not
    /// round-trip is caught on catch-up too — a re-encode on the primary
    /// would have agreed with the replica's and hidden it.
    #[test]
    fn catch_up_exposes_a_codec_that_does_not_round_trip() {
        let journal = journal_bytes(&[sample_slot(4, JournalEvent::App(LossyEvent(0xF0)))]);
        let mut buf = Vec::new();
        encode_input_batch_from_journal::<LossyEvent>(&journal, &mut buf).expect("encode");
        assert!(matches!(
            try_decode_input_batch::<LossyEvent>(&buf[4..]),
            Err(InputBatchError::NotRoundTrip { sequence: 4, .. })
        ));
    }

    #[test]
    fn catch_up_refuses_an_entry_whose_disk_crc_fails() {
        let mut journal = journal_bytes(&[sample_slot(4, JournalEvent::App(TestEvent(9)))]);
        let mid = journal.len() / 2;
        journal[mid] ^= 0x10;
        let mut buf = vec![0xAB];
        assert!(encode_input_batch_from_journal::<TestEvent>(&journal, &mut buf).is_err());
        assert_eq!(
            buf,
            vec![0xAB],
            "a refused batch leaves the buffer as it was"
        );
    }

    /// Pins the on-wire byte layout of a 1-slot Tick batch. Sentinel u64
    /// values are chosen so each LE byte sequence is human-readable
    /// (0x0807_0605_0403_0201 → `[01,02,03,04,05,06,07,08]`). Any future
    /// field reorder, padding insertion, or endianness flip — including
    /// "harmless" struct edits that pass roundtrip — fails this test
    /// before it can break compatibility with peers running older builds.
    #[test]
    fn wire_format_is_byte_pinned() {
        let slot = InputSlot::<TestEvent> {
            connection_id: 0,
            key_hash: 0x0807_0605_0403_0201,
            sequence: 0x2827_2625_2423_2221,
            timestamp_ns: 0x3837_3635_3433_3231,
            event: JournalEvent::Tick {
                now_ns: 0x4847_4645_4443_4241,
            },
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        };

        let buf = frame(&[slot]);

        // Total = FrameHeader(7) + SlotHeader(27) + Tick payload(8) + CRC(4) = 46.
        // FrameHeader.length = total - 4 (the length field itself) = 42 = 0x2a.
        // SlotHeader.length = ENTRY_META_SIZE(9) + payload(8) = 17 = 0x11.
        let mut expected: Vec<u8> = vec![
            // FrameHeader: length(u32) + type(u8) + count(u16)
            0x2a, 0x00, 0x00, 0x00, // length = 42
            0x21, // MSG_INPUT_BATCH
            0x01, 0x00, // count = 1
        ];
        let slot_body: &[u8] = &[
            // SlotHeader: length(u16) + sequence(u64) + timestamp_ns(u64)
            //           + key_hash(u64) + event_tag(u8)
            0x11, 0x00, // length = 17 (matches journal's length: 9 + 8)
            0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, // sequence
            0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, // timestamp_ns
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_hash
            0x03, // SLOT_TAG_TICK
            // Tick payload: now_ns(u64)
            0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
        ];
        expected.extend_from_slice(slot_body);
        // CRC32C trailer, LE: over the entry magic (0x4A45, LE) and the
        // slot body — the journal entry's own CRC.
        let mut covered = vec![0x45, 0x4A];
        covered.extend_from_slice(slot_body);
        expected.extend_from_slice(&crc32c::crc32c(&covered).to_le_bytes());

        assert_eq!(buf, expected, "wire format byte layout must not change");
        assert_eq!(buf.len(), 46);
    }
}
