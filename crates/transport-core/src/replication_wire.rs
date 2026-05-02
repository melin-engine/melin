//! Wire format for input replication (`InputBatch` frames).
//!
//! Replicas accept `InputSlot` records directly — no journal-codec
//! round-trip on the wire. The per-slot length-field semantics match
//! the journal codec's `length` (covers `ENTRY_META_SIZE + payload`),
//! so the journal stage can ship the just-encoded journal bytes
//! verbatim — the slice from after the journal entry's magic to before
//! its CRC trailer is the replication slot, byte-for-byte.
//!
//! Wire layout (after a `[length:u32]` frame prefix):
//! ```text
//! [type:0x21] [count:u16]
//! for each slot:
//!   [length:u16]   ← ENTRY_META_SIZE + payload bytes (matches journal)
//!   [sequence:u64]
//!   [timestamp_ns:u64]
//!   [key_hash:u64]
//!   [request_seq:u64]
//!   [event_tag:u8]
//!   [event_payload: length - ENTRY_META_SIZE bytes]
//! ```
//!
//! No per-entry magic or CRC32C — TCP/DPDK handle framing and integrity.
//! `connection_id`, `publish_ts`, `recv_ts` from `InputSlot` are not on
//! the wire (primary-internal bookkeeping); the receiver reconstructs
//! them with `Default::default()`.

use std::io;

use melin_app::AppEvent;
use melin_journal::JournalEvent;
use melin_journal::codec::ENTRY_META_SIZE;
use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::pipeline::InputSlot;

// --- Constants ---

pub const MSG_INPUT_BATCH: u8 = 0x21;

pub const SLOT_TAG_GENESIS_HASH: u8 = 0x01;
pub const SLOT_TAG_CHECKPOINT: u8 = 0x02;
pub const SLOT_TAG_TICK: u8 = 0x03;
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

/// Per-slot fixed prefix; variable-length event payload follows.
/// `length` matches the journal's `length` field (covers
/// `ENTRY_META_SIZE + payload_len`); the payload size is therefore
/// `length - ENTRY_META_SIZE`. The shared semantics let the journal
/// stage hand a slice of the just-encoded journal entry directly to
/// replication, without re-encoding.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SlotHeader {
    length: U16,
    sequence: U64,
    timestamp_ns: U64,
    key_hash: U64,
    request_seq: U64,
    event_tag: u8,
}

const FRAME_HEADER_LEN: usize = core::mem::size_of::<FrameHeader>();
const SLOT_HEADER_LEN: usize = core::mem::size_of::<SlotHeader>();

// Pin the wire layout. Reordering or extending these structs would silently
// break compatibility with peers running the previous build, so we fail the
// compile instead.
const _: () = assert!(FRAME_HEADER_LEN == 7);
const _: () = assert!(SLOT_HEADER_LEN == 35);
const _: () = assert!(core::mem::size_of::<BatchPreamble>() == 3);

// --- Streaming encode (used by the journal stage on the hot path) ---

/// Reset `buf` and reserve placeholder bytes for the frame header.
/// Caller appends slots via [`append_input_slot`] and back-fills the
/// header with [`finalize_input_batch`] before publishing.
pub fn init_input_batch(buf: &mut Vec<u8>) {
    buf.clear();
    buf.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
}

/// Append one slot's wire bytes to a buffer initialized with
/// [`init_input_batch`]. `seq` is the sequence to encode — `slot.sequence`
/// may be zero on the primary (the journal stage allocates at encode
/// time), so callers pass the allocated value explicitly.
///
/// **Caller contract**: `buf` must already contain a frame header (i.e.
/// `init_input_batch` was called, or this is being called inside
/// `encode_input_batch`). The debug assertion catches the bare-empty
/// `Vec` misuse in tests.
pub fn append_input_slot<E: AppEvent>(buf: &mut Vec<u8>, slot: &InputSlot<E>, seq: u64) {
    debug_assert!(
        buf.len() >= FRAME_HEADER_LEN,
        "append_input_slot requires init_input_batch first (buf.len() = {})",
        buf.len()
    );

    // Reserve a zeroed slot header; back-fill the typed view in one block
    // once the payload is written and length is known. The payload writes
    // may grow the Vec and reallocate, so we cannot hold a borrow into
    // `buf` across them — the typed view is taken at the end.
    let header_start = buf.len();
    buf.resize(header_start + SLOT_HEADER_LEN, 0);

    let tag = match &slot.event {
        JournalEvent::GenesisHash { hash } => {
            buf.extend_from_slice(hash);
            SLOT_TAG_GENESIS_HASH
        }
        JournalEvent::Checkpoint {
            chain_hash,
            events_since_checkpoint,
        } => {
            buf.extend_from_slice(chain_hash);
            buf.extend_from_slice(&events_since_checkpoint.to_le_bytes());
            SLOT_TAG_CHECKPOINT
        }
        JournalEvent::Tick { now_ns } => {
            buf.extend_from_slice(&now_ns.to_le_bytes());
            SLOT_TAG_TICK
        }
        JournalEvent::App(e) => {
            let n = e.encoded_size();
            let start = buf.len();
            buf.resize(start + n, 0);
            let written = e.encode(&mut buf[start..start + n]);
            debug_assert_eq!(written, n, "AppEvent::encode disagrees with encoded_size");
            SLOT_TAG_APP
        }
        JournalEvent::Shutdown => {
            // Pipeline-only sentinel — never written to the wire. The
            // journal stage filters it before reaching this encoder; if
            // it arrives here, that's a logic bug. Truncate the buffer
            // back to the pre-header position so the partially-written
            // header isn't appended to the wire batch.
            buf.truncate(header_start);
            return;
        }
    };

    let payload_bytes = buf.len() - (header_start + SLOT_HEADER_LEN);
    let length =
        u16::try_from(ENTRY_META_SIZE + payload_bytes).expect("event payload exceeds u16 max");

    let header = SlotHeader::mut_from_bytes(&mut buf[header_start..header_start + SLOT_HEADER_LEN])
        .expect("SLOT_HEADER_LEN slice matches struct size");
    header.length = U16::new(length);
    header.sequence = U64::new(seq);
    header.timestamp_ns = U64::new(slot.timestamp_ns);
    header.key_hash = U64::new(slot.key_hash);
    header.request_seq = U64::new(slot.request_seq);
    header.event_tag = tag;
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

// --- One-shot encode (used by catch-up paths that already have a slot vec) ---

/// Encode a complete length-prefixed `InputBatch` frame into `buf`.
/// Equivalent to `init_input_batch` + `append_input_slot` per slot
/// (with `slot.sequence`) + `finalize_input_batch`. Use the streaming
/// API on the journal stage hot path; this helper is for catch-up that
/// already materialised a slot vector.
pub fn encode_input_batch<E: AppEvent>(slots: &[InputSlot<E>], buf: &mut Vec<u8>) {
    let start = buf.len();
    buf.extend_from_slice(&[0u8; FRAME_HEADER_LEN]);
    for slot in slots {
        append_input_slot(buf, slot, slot.sequence);
    }
    let count = u16::try_from(slots.len()).expect("InputBatch slot count exceeds u16");
    finalize_input_batch(&mut buf[start..], count);
}

// --- Decode ---

/// Decode an `InputBatch` frame payload (the bytes after the length prefix,
/// starting with the type byte). Returns the reconstructed `InputSlot`
/// vector with `connection_id`, `publish_ts`, `recv_ts` reset to defaults.
/// Decode an `InputBatch` frame payload into a caller-supplied buffer.
/// The buffer is cleared then filled; capacity is grown on demand but never
/// shrunk, so the allocator is hit at most once per batch size seen so far.
/// Prefer this over `try_decode_input_batch` on hot paths to avoid per-call
/// heap allocation.
pub fn try_decode_input_batch_into<E: AppEvent>(
    payload: &[u8],
    slots: &mut Vec<InputSlot<E>>,
) -> io::Result<()> {
    let (preamble, mut rest) = BatchPreamble::ref_from_prefix(payload)
        .map_err(|_| io::Error::other("InputBatch header truncated"))?;
    if preamble.msg_type != MSG_INPUT_BATCH {
        return Err(io::Error::other(format!(
            "expected InputBatch (0x{:02x}), got 0x{:02x}",
            MSG_INPUT_BATCH, preamble.msg_type
        )));
    }
    let count = preamble.count.get() as usize;
    slots.clear();
    if slots.capacity() < count {
        slots.reserve(count - slots.capacity());
    }

    for _ in 0..count {
        let (header, after_header) = SlotHeader::ref_from_prefix(rest)
            .map_err(|_| io::Error::other("InputBatch slot header truncated"))?;

        let length = header.length.get() as usize;
        if length < ENTRY_META_SIZE {
            return Err(io::Error::other(
                "InputBatch slot length below ENTRY_META_SIZE",
            ));
        }
        let payload_size = length - ENTRY_META_SIZE;
        if after_header.len() < payload_size {
            return Err(io::Error::other("InputBatch slot payload truncated"));
        }
        let event_payload = &after_header[..payload_size];
        rest = &after_header[payload_size..];

        let event = match header.event_tag {
            SLOT_TAG_GENESIS_HASH => {
                if event_payload.len() < 32 {
                    return Err(io::Error::other("GenesisHash payload too short"));
                }
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&event_payload[..32]);
                JournalEvent::GenesisHash { hash }
            }
            SLOT_TAG_CHECKPOINT => {
                if event_payload.len() < 40 {
                    return Err(io::Error::other("Checkpoint payload too short"));
                }
                let mut chain_hash = [0u8; 32];
                chain_hash.copy_from_slice(&event_payload[..32]);
                let events_since_checkpoint = u64::from_le_bytes(
                    event_payload[32..40]
                        .try_into()
                        .expect("8-byte slice into [u8; 8]"),
                );
                JournalEvent::Checkpoint {
                    chain_hash,
                    events_since_checkpoint,
                }
            }
            SLOT_TAG_TICK => {
                if event_payload.len() < 8 {
                    return Err(io::Error::other("Tick payload too short"));
                }
                let now_ns = u64::from_le_bytes(
                    event_payload[..8]
                        .try_into()
                        .expect("8-byte slice into [u8; 8]"),
                );
                JournalEvent::Tick { now_ns }
            }
            SLOT_TAG_APP => {
                let app = E::decode(event_payload)
                    .map_err(|e| io::Error::other(format!("App event decode failed: {e:?}")))?;
                JournalEvent::App(app)
            }
            other => {
                return Err(io::Error::other(format!("unknown slot tag: 0x{other:02x}")));
            }
        };

        slots.push(InputSlot {
            connection_id: 0,
            key_hash: header.key_hash.get(),
            request_seq: header.request_seq.get(),
            sequence: header.sequence.get(),
            timestamp_ns: header.timestamp_ns.get(),
            event,
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
    }

    Ok(())
}

/// Decode an `InputBatch` frame payload (the bytes after the length prefix,
/// starting with the type byte). Returns the reconstructed `InputSlot`
/// vector with `connection_id`, `publish_ts`, `recv_ts` reset to defaults.
pub fn try_decode_input_batch<E: AppEvent>(payload: &[u8]) -> io::Result<Vec<InputSlot<E>>> {
    let mut slots = Vec::new();
    try_decode_input_batch_into(payload, &mut slots)?;
    Ok(slots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_app::CodecError;

    /// Minimal AppEvent for round-trip tests. Encodes a single u32 payload.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u32);

    impl AppEvent for TestEvent {
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

    fn sample_slot(sequence: u64, event: JournalEvent<TestEvent>) -> InputSlot<TestEvent> {
        InputSlot {
            connection_id: 0,
            key_hash: 0xabcd_ef00_1234_5678,
            request_seq: 9_999,
            sequence,
            timestamp_ns: 1_700_000_000_000_000_000,
            event,
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        }
    }

    #[test]
    fn roundtrip_transport_variants() {
        let slots = vec![
            sample_slot(10, JournalEvent::Tick { now_ns: 12_345_678 }),
            sample_slot(
                11,
                JournalEvent::Checkpoint {
                    chain_hash: [0x42; 32],
                    events_since_checkpoint: 1_000_000,
                },
            ),
            sample_slot(12, JournalEvent::GenesisHash { hash: [0x77; 32] }),
        ];

        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);

        let payload_len =
            u32::from_le_bytes(buf[..4].try_into().expect("4-byte slice into [u8; 4]")) as usize;
        assert_eq!(buf.len(), 4 + payload_len);
        let payload = &buf[4..];

        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(payload).expect("decode succeeds");
        assert_eq!(decoded.len(), 3);

        for (orig, dec) in slots.iter().zip(decoded.iter()) {
            assert_eq!(dec.sequence, orig.sequence);
            assert_eq!(dec.timestamp_ns, orig.timestamp_ns);
            assert_eq!(dec.key_hash, orig.key_hash);
            assert_eq!(dec.request_seq, orig.request_seq);
            assert_eq!(dec.connection_id, 0);
        }

        match decoded[0].event {
            JournalEvent::Tick { now_ns } => assert_eq!(now_ns, 12_345_678),
            ref other => panic!("expected Tick, got {other:?}"),
        }
        match decoded[1].event {
            JournalEvent::Checkpoint {
                chain_hash,
                events_since_checkpoint,
            } => {
                assert_eq!(chain_hash, [0x42; 32]);
                assert_eq!(events_since_checkpoint, 1_000_000);
            }
            ref other => panic!("expected Checkpoint, got {other:?}"),
        }
        match decoded[2].event {
            JournalEvent::GenesisHash { hash } => assert_eq!(hash, [0x77; 32]),
            ref other => panic!("expected GenesisHash, got {other:?}"),
        }
    }

    #[test]
    fn roundtrip_app_variant() {
        let slots = vec![sample_slot(7, JournalEvent::App(TestEvent(0xdead_beef)))];
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        let payload = &buf[4..];
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(payload).expect("decode succeeds");
        assert_eq!(decoded.len(), 1);
        match decoded[0].event {
            JournalEvent::App(TestEvent(v)) => assert_eq!(v, 0xdead_beef),
            ref other => panic!("expected App, got {other:?}"),
        }
    }

    #[test]
    fn empty_batch_roundtrips() {
        let slots: Vec<InputSlot<TestEvent>> = Vec::new();
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        let payload = &buf[4..];
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(payload).expect("decode succeeds");
        assert!(decoded.is_empty());
    }

    #[test]
    fn shutdown_sentinel_is_truncated_from_wire() {
        // The Shutdown variant is a pipeline-only sentinel. If it ever
        // reaches the wire encoder, the partially-written slot header
        // must be truncated so the wire batch contains only valid slots.
        let mut buf = Vec::new();
        init_input_batch(&mut buf);
        let pre_len = buf.len();
        let sentinel = sample_slot(99, JournalEvent::Shutdown);
        append_input_slot(&mut buf, &sentinel, sentinel.sequence);
        // Buffer must be back at the pre-header position — no slot bytes,
        // no half-written slot header.
        assert_eq!(buf.len(), pre_len);
    }

    #[test]
    fn shutdown_sentinel_does_not_break_surrounding_slots() {
        // Real-world bug case: a sentinel slot interleaved with valid
        // slots in the same batch. The valid slots before and after must
        // round-trip, and the sentinel must be silently dropped.
        let mut buf = Vec::new();
        init_input_batch(&mut buf);
        let s1 = sample_slot(1, JournalEvent::Tick { now_ns: 111 });
        append_input_slot(&mut buf, &s1, s1.sequence);
        let sentinel = sample_slot(2, JournalEvent::Shutdown);
        append_input_slot(&mut buf, &sentinel, sentinel.sequence);
        let s3 = sample_slot(3, JournalEvent::App(TestEvent(0xcafe)));
        append_input_slot(&mut buf, &s3, s3.sequence);
        finalize_input_batch(&mut buf, 2); // only 2 valid slots written

        let payload = &buf[4..];
        let decoded: Vec<InputSlot<TestEvent>> =
            try_decode_input_batch(payload).expect("decode succeeds");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].sequence, 1);
        assert_eq!(decoded[1].sequence, 3);
    }

    #[test]
    fn rejects_wrong_type_tag() {
        let payload = [0xFF, 0x00, 0x00];
        assert!(try_decode_input_batch::<TestEvent>(&payload).is_err());
    }

    #[test]
    fn rejects_truncated_header() {
        let payload = [MSG_INPUT_BATCH];
        assert!(try_decode_input_batch::<TestEvent>(&payload).is_err());
    }

    #[test]
    fn rejects_truncated_slot_payload() {
        let slots = vec![sample_slot(1, JournalEvent::Tick { now_ns: 0 })];
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        let payload = &buf[4..buf.len() - 1];
        assert!(try_decode_input_batch::<TestEvent>(payload).is_err());
    }

    #[test]
    fn streaming_api_matches_one_shot() {
        let slots = vec![
            sample_slot(20, JournalEvent::Tick { now_ns: 100 }),
            sample_slot(21, JournalEvent::App(TestEvent(42))),
        ];

        let mut one_shot = Vec::new();
        encode_input_batch(&slots, &mut one_shot);

        let mut streaming = Vec::new();
        init_input_batch(&mut streaming);
        for slot in &slots {
            append_input_slot(&mut streaming, slot, slot.sequence);
        }
        finalize_input_batch(&mut streaming, slots.len() as u16);

        assert_eq!(one_shot, streaming);
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
            request_seq: 0x1817_1615_1413_1211,
            sequence: 0x2827_2625_2423_2221,
            timestamp_ns: 0x3837_3635_3433_3231,
            event: JournalEvent::Tick {
                now_ns: 0x4847_4645_4443_4241,
            },
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        };

        let mut buf = Vec::new();
        encode_input_batch(&[slot], &mut buf);

        // Total = FrameHeader(7) + SlotHeader(35) + Tick payload(8) = 50.
        // FrameHeader.length = total - 4 (the length field itself) = 46 = 0x2E.
        // SlotHeader.length = ENTRY_META_SIZE(17) + payload(8) = 25 = 0x19.
        let expected: &[u8] = &[
            // FrameHeader: length(u32) + type(u8) + count(u16)
            0x2E, 0x00, 0x00, 0x00, // length = 46
            0x21, // MSG_INPUT_BATCH
            0x01, 0x00, // count = 1
            // SlotHeader: length(u16) + sequence(u64) + timestamp_ns(u64)
            //           + key_hash(u64) + request_seq(u64) + event_tag(u8)
            0x19, 0x00, // length = 25 (matches journal's length: 17 + 8)
            0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, // sequence
            0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, // timestamp_ns
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // key_hash
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // request_seq
            0x03, // SLOT_TAG_TICK
            // Tick payload: now_ns(u64)
            0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48,
        ];
        assert_eq!(buf, expected, "wire format byte layout must not change");
        assert_eq!(buf.len(), 50);
    }
}
