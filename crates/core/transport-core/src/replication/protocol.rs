//! Replication wire protocol — message types, framing, encode/decode.
//!
//! Length-prefixed frames, little-endian, over a dedicated TCP connection
//! (or DPDK pipe). See the parent module for the message catalogue.

use std::io::{self, Read};

use melin_app::AppEvent;
use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::pipeline::InputSlot;

// Wire format for `MSG_INPUT_BATCH` lives alongside in
// `crate::replication_wire` so the journal stage can encode directly
// into the replication ring without depending on this submodule.
// Re-export the helpers at replication scope so existing
// `replication::protocol::{...}` imports keep working.
pub use crate::replication_wire::{
    encode_input_batch, peek_first_sequence, try_decode_input_batch, try_decode_input_batch_into,
};

// --- Wire protocol message tags ---

pub const MSG_HANDSHAKE: u8 = 0x01;
pub const MSG_ACK: u8 = 0x02;
// Auth messages (exchanged before the handshake).
pub const MSG_CHALLENGE: u8 = 0x03;
pub const MSG_CHALLENGE_RESPONSE: u8 = 0x04;
pub const MSG_AUTH_OK: u8 = 0x05;
pub const MSG_AUTH_FAILED: u8 = 0x06;
pub const MSG_STREAM_START: u8 = 0x10;
pub const MSG_NEED_SNAPSHOT: u8 = 0x11;
pub const MSG_HASH_MISMATCH: u8 = 0x12;
pub const MSG_SNAPSHOT_BEGIN: u8 = 0x13;
pub const MSG_SNAPSHOT_CHUNK: u8 = 0x14;
pub const MSG_SNAPSHOT_END: u8 = 0x15;
// `MSG_INPUT_BATCH` (0x21) — re-exported above; carries `InputSlot`
// records on the wire. Replaces the old `MSG_DATA_BATCH = 0x20` (removed
// in phase 3 of feat/unified-pipeline).
pub const MSG_HEARTBEAT: u8 = 0x30;

/// Replication protocol version, advertised in the replica's handshake
/// and validated by the primary. Bumped whenever a frame layout changes
/// so mixed-version pairs fail with a *diagnosable* error instead of an
/// opaque short-frame decode failure (or, worse, a silently-ignored
/// trailing field — zerocopy prefix parsing accepts longer frames).
/// History: 1 = pre-fencing (41-byte handshake, no epoch);
/// 2 = fencing epochs (epoch on handshake/StreamStart) + this field.
pub const REPL_PROTOCOL_VERSION: u16 = 2;

/// Maximum frame size for control messages (handshake, ack, etc.).
/// `InputBatch` frames can be much larger (up to a full 512 KiB ring chunk).
pub const MAX_CONTROL_FRAME: usize = 256;

/// Maximum `InputBatch` frame size. Must be >= the replication ring's
/// `CHUNK_SIZE` (512 KiB) — the journal stage's `InputBatch` buffer can
/// fill an entire chunk before sync. The 256 KiB headroom covers
/// length-prefix + per-slot framing overhead inside the chunk plus a
/// safety margin.
pub const MAX_DATA_FRAME: usize = 768 * 1024;

// --- Message structs / enums ---

/// Handshake message sent by the replica on connection.
#[derive(Debug, Clone)]
pub struct Handshake {
    pub last_sequence: u64,
    pub chain_hash: [u8; 32],
    /// The replica's current fencing epoch. The primary compares it against
    /// its own: a replica advertising a *higher* epoch has seen a promotion
    /// the primary hasn't, so the primary is stale and self-demotes (see
    /// `crate::fence`).
    pub epoch: u64,
}

/// Ack message sent by the replica.
///
/// Carries two cursors so the primary's response gate can evaluate
/// multi-level durability policies (see
/// `crate::durability_policy`):
///
/// - `acked_sequence` — highest sequence persisted on the replica
///   (`O_DIRECT` `pwrite` returned, durable behind PLP).
/// - `in_memory_sequence` — highest sequence the replica has accepted
///   into its pipeline pre-journal. Always `>= acked_sequence`.
#[derive(Debug, Clone, Copy)]
pub struct Ack {
    pub acked_sequence: u64,
    pub in_memory_sequence: u64,
}

/// Messages from primary to replica.
#[derive(Debug)]
pub enum PrimaryMessage {
    StreamStart {
        start_sequence: u64,
        /// Header identity for the journal segment a *fresh* replica
        /// should create before consuming the stream: the segment's
        /// `starting_sequence` and chain `anchor_hash`. For full
        /// catch-up this is the primary's oldest segment (lineage
        /// origin); after a snapshot transfer it is `snap_sequence + 1`
        /// anchored to the snapshot's chain hash. Replicas with
        /// existing local state ignore it.
        segment_start_sequence: u64,
        anchor_hash: [u8; 32],
        /// The primary's current fencing epoch. A replica that already
        /// observed a *higher* epoch refuses to follow this (stale) primary;
        /// a replica behind it adopts the epoch as the stream's `EpochBump`s
        /// replay. See `crate::fence`.
        epoch: u64,
    },
    NeedSnapshot,
    HashMismatch,
    /// Start of a snapshot transfer. Sent after NeedSnapshot.
    SnapshotBegin {
        /// Total snapshot file size in bytes.
        snapshot_len: u64,
        /// Journal sequence at which the snapshot was taken.
        snap_sequence: u64,
        /// BLAKE3 chain hash at the snapshot point.
        snap_chain_hash: [u8; 32],
    },
    /// A chunk of snapshot data. Sent repeatedly after SnapshotBegin.
    SnapshotChunk(Vec<u8>),
    /// End of snapshot transfer. Contains CRC32C of the entire snapshot file.
    SnapshotEnd {
        crc32c: u32,
    },
    Heartbeat {
        sequence: u64,
    },
}

/// Messages from replica to primary.
#[derive(Debug)]
pub enum ReplicaMessage {
    Handshake(Handshake),
    Ack(Ack),
}

// --- Wire structs ---
//
// Post-length form (the byte stripped by `read_frame` is the 4-byte
// length prefix; what remains starts with the tag). Each frame type
// has its own struct so encoders write all fields in one block and
// decoders peel a typed prefix instead of computing offsets.
//
// `little_endian::U{32,64}` are 1-byte-aligned LE wrappers, so a
// `repr(C)` struct of them is byte-packed (no padding) and serialises
// bit-for-bit identically to the previous hand-rolled `to_le_bytes`
// chains. Wire layout is authoritative — assertions below pin it.

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct HandshakeFrame {
    tag: u8,
    last_sequence: U64,
    chain_hash: [u8; 32],
    epoch: U64,
    /// [`REPL_PROTOCOL_VERSION`] — appended *last* deliberately: prefix
    /// parsing on an older peer still reads the fields it knows, and the
    /// new decoder rejects a mismatch with an explicit version error.
    protocol_version: U16,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AckFrame {
    tag: u8,
    acked_sequence: U64,
    in_memory_sequence: U64,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ChallengeFrame {
    tag: u8,
    nonce: [u8; 32],
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ChallengeResponseFrame {
    tag: u8,
    signature: [u8; 64],
    pubkey: [u8; 32],
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SnapshotBeginFrame {
    tag: u8,
    snapshot_len: U64,
    snap_sequence: U64,
    snap_chain_hash: [u8; 32],
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SnapshotEndFrame {
    tag: u8,
    crc32c: U32,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct HeartbeatFrame {
    tag: u8,
    sequence: U64,
}

/// Fixed-size StreamStart frame: stream resume point plus the segment
/// header identity a fresh replica should create its journal with.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct StreamStartFrame {
    tag: u8,
    start_sequence: U64,
    segment_start_sequence: U64,
    anchor_hash: [u8; 32],
    epoch: U64,
}

const _: () = assert!(core::mem::size_of::<HandshakeFrame>() == 51);
const _: () = assert!(core::mem::size_of::<AckFrame>() == 17);
const _: () = assert!(core::mem::size_of::<ChallengeFrame>() == 33);
const _: () = assert!(core::mem::size_of::<ChallengeResponseFrame>() == 97);
const _: () = assert!(core::mem::size_of::<SnapshotBeginFrame>() == 49);
const _: () = assert!(core::mem::size_of::<SnapshotEndFrame>() == 5);
const _: () = assert!(core::mem::size_of::<HeartbeatFrame>() == 9);
const _: () = assert!(core::mem::size_of::<StreamStartFrame>() == 57);

/// Helper: `length_prefix(buf, payload_len)` writes the 4-byte LE
/// frame length prefix for a payload of `payload_len` bytes.
#[inline]
fn write_length_prefix(buf: &mut Vec<u8>, payload_len: u32) {
    buf.extend_from_slice(&payload_len.to_le_bytes());
}

// --- Encoders ---

/// Encode a handshake message into a frame (length-prefixed).
pub fn encode_handshake(h: &Handshake, buf: &mut Vec<u8>) {
    let frame = HandshakeFrame {
        tag: MSG_HANDSHAKE,
        last_sequence: U64::new(h.last_sequence),
        chain_hash: h.chain_hash,
        epoch: U64::new(h.epoch),
        protocol_version: U16::new(REPL_PROTOCOL_VERSION),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode an ack message into a frame.
pub fn encode_ack(ack: &Ack, buf: &mut Vec<u8>) {
    let frame = AckFrame {
        tag: MSG_ACK,
        acked_sequence: U64::new(ack.acked_sequence),
        in_memory_sequence: U64::new(ack.in_memory_sequence),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a Challenge message (primary → replica).
pub fn encode_challenge(nonce: &[u8; 32], buf: &mut Vec<u8>) {
    let frame = ChallengeFrame {
        tag: MSG_CHALLENGE,
        nonce: *nonce,
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a ChallengeResponse message (replica → primary).
pub fn encode_challenge_response(signature: &[u8; 64], pubkey: &[u8; 32], buf: &mut Vec<u8>) {
    let frame = ChallengeResponseFrame {
        tag: MSG_CHALLENGE_RESPONSE,
        signature: *signature,
        pubkey: *pubkey,
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode an AuthOk message (primary → replica).
pub fn encode_auth_ok(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_OK);
}

/// Encode an AuthFailed message (primary → replica).
pub fn encode_auth_failed(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_FAILED);
}

/// Encode a StreamStart message into a frame.
///
/// `segment_start_sequence` and `anchor_hash` identify the journal
/// segment a fresh replica should create before consuming the stream —
/// with the same header identity and the same entry bytes, the
/// replica's segment is byte-identical to the primary's **until the
/// first rotation on either node**: rotations are local, so segment
/// boundaries (and with them per-segment anchors and chain values)
/// diverge after that, even though the entry stream stays identical.
/// Boundary alignment lands with primary-driven rotation (roadmap).
pub fn encode_stream_start(
    start_sequence: u64,
    segment_start_sequence: u64,
    anchor_hash: [u8; 32],
    epoch: u64,
    buf: &mut Vec<u8>,
) {
    let frame = StreamStartFrame {
        tag: MSG_STREAM_START,
        start_sequence: U64::new(start_sequence),
        segment_start_sequence: U64::new(segment_start_sequence),
        anchor_hash,
        epoch: U64::new(epoch),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a NeedSnapshot message.
pub fn encode_need_snapshot(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_NEED_SNAPSHOT);
}

/// Encode a SnapshotBegin message.
pub fn encode_snapshot_begin(
    snapshot_len: u64,
    snap_sequence: u64,
    snap_chain_hash: &[u8; 32],
    buf: &mut Vec<u8>,
) {
    let frame = SnapshotBeginFrame {
        tag: MSG_SNAPSHOT_BEGIN,
        snapshot_len: U64::new(snapshot_len),
        snap_sequence: U64::new(snap_sequence),
        snap_chain_hash: *snap_chain_hash,
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a SnapshotChunk message.
pub fn encode_snapshot_chunk(data: &[u8], buf: &mut Vec<u8>) {
    // type(1) + data
    let payload_len: u32 = (1 + data.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_CHUNK);
    buf.extend_from_slice(data);
}

/// Encode a SnapshotEnd message.
pub fn encode_snapshot_end(crc32c: u32, buf: &mut Vec<u8>) {
    let frame = SnapshotEndFrame {
        tag: MSG_SNAPSHOT_END,
        crc32c: U32::new(crc32c),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a HashMismatch message.
pub fn encode_hash_mismatch(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HASH_MISMATCH);
}

/// Encode a Heartbeat message. Carries only the last-sent sequence.
pub fn encode_heartbeat(sequence: u64, buf: &mut Vec<u8>) {
    let frame = HeartbeatFrame {
        tag: MSG_HEARTBEAT,
        sequence: U64::new(sequence),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

// --- Decoders / framing ---

/// Read a length-prefixed frame from a stream. Returns the payload (without length prefix).
pub fn read_frame(reader: &mut impl Read, max_size: usize) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > max_size {
        return Err(io::Error::other(format!(
            "frame too large: {len} > {max_size}"
        )));
    }
    if len == 0 {
        return Err(io::Error::other("empty frame"));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Decode a Challenge payload → 32-byte nonce.
pub fn decode_challenge(payload: &[u8]) -> io::Result<[u8; 32]> {
    let (frame, _) = ChallengeFrame::ref_from_prefix(payload)
        .map_err(|_| io::Error::other("challenge too short"))?;
    if frame.tag != MSG_CHALLENGE {
        return Err(io::Error::other(format!(
            "expected Challenge (0x{:02x}), got 0x{:02x}",
            MSG_CHALLENGE, frame.tag
        )));
    }
    Ok(frame.nonce)
}

/// Decode a ChallengeResponse payload → (signature, pubkey).
pub fn decode_challenge_response(payload: &[u8]) -> io::Result<([u8; 64], [u8; 32])> {
    let (frame, _) = ChallengeResponseFrame::ref_from_prefix(payload)
        .map_err(|_| io::Error::other("challenge response too short"))?;
    if frame.tag != MSG_CHALLENGE_RESPONSE {
        return Err(io::Error::other(format!(
            "expected ChallengeResponse (0x{:02x}), got 0x{:02x}",
            MSG_CHALLENGE_RESPONSE, frame.tag
        )));
    }
    Ok((frame.signature, frame.pubkey))
}

/// Decode an auth result payload → true if AuthOk, false if AuthFailed.
pub fn decode_auth_result(payload: &[u8]) -> io::Result<bool> {
    if payload.is_empty() {
        return Err(io::Error::other("empty auth result"));
    }
    match payload[0] {
        MSG_AUTH_OK => Ok(true),
        MSG_AUTH_FAILED => Ok(false),
        other => Err(io::Error::other(format!(
            "expected AuthOk/AuthFailed, got 0x{other:02x}"
        ))),
    }
}

/// Decode a replica message from a frame payload.
pub fn decode_replica_message(payload: &[u8]) -> io::Result<ReplicaMessage> {
    if payload.is_empty() {
        return Err(io::Error::other("empty payload"));
    }
    match payload[0] {
        MSG_HANDSHAKE => {
            let (frame, _) = HandshakeFrame::ref_from_prefix(payload).map_err(|_| {
                io::Error::other(format!(
                    "handshake frame too short ({} bytes, expected {}) — the replica is \
                     likely running an older, incompatible replication protocol version; \
                     upgrade it to this binary's version",
                    payload.len(),
                    core::mem::size_of::<HandshakeFrame>()
                ))
            })?;
            let peer_version = frame.protocol_version.get();
            if peer_version != REPL_PROTOCOL_VERSION {
                return Err(io::Error::other(format!(
                    "replica speaks replication protocol v{peer_version}, this primary \
                     speaks v{REPL_PROTOCOL_VERSION} — upgrade both nodes to the same version"
                )));
            }
            Ok(ReplicaMessage::Handshake(Handshake {
                last_sequence: frame.last_sequence.get(),
                chain_hash: frame.chain_hash,
                epoch: frame.epoch.get(),
            }))
        }
        MSG_ACK => {
            let (frame, _) = AckFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("ack too short"))?;
            Ok(ReplicaMessage::Ack(Ack {
                acked_sequence: frame.acked_sequence.get(),
                in_memory_sequence: frame.in_memory_sequence.get(),
            }))
        }
        other => Err(io::Error::other(format!(
            "unknown replica message type: 0x{other:02x}"
        ))),
    }
}

/// Decode a primary message from a frame payload.
pub fn decode_primary_message(payload: &[u8]) -> io::Result<PrimaryMessage> {
    if payload.is_empty() {
        return Err(io::Error::other("empty payload"));
    }
    match payload[0] {
        MSG_STREAM_START => {
            let (frame, _) = StreamStartFrame::ref_from_prefix(payload).map_err(|_| {
                io::Error::other(format!(
                    "StreamStart frame too short ({} bytes, expected {}) — the primary is \
                     likely running an older, incompatible replication protocol version; \
                     upgrade it to this binary's version",
                    payload.len(),
                    core::mem::size_of::<StreamStartFrame>()
                ))
            })?;
            Ok(PrimaryMessage::StreamStart {
                start_sequence: frame.start_sequence.get(),
                segment_start_sequence: frame.segment_start_sequence.get(),
                anchor_hash: frame.anchor_hash,
                epoch: frame.epoch.get(),
            })
        }
        MSG_NEED_SNAPSHOT => Ok(PrimaryMessage::NeedSnapshot),
        MSG_HASH_MISMATCH => Ok(PrimaryMessage::HashMismatch),
        MSG_SNAPSHOT_BEGIN => {
            let (frame, _) = SnapshotBeginFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("SnapshotBegin too short"))?;
            Ok(PrimaryMessage::SnapshotBegin {
                snapshot_len: frame.snapshot_len.get(),
                snap_sequence: frame.snap_sequence.get(),
                snap_chain_hash: frame.snap_chain_hash,
            })
        }
        MSG_SNAPSHOT_CHUNK => Ok(PrimaryMessage::SnapshotChunk(payload[1..].to_vec())),
        MSG_SNAPSHOT_END => {
            let (frame, _) = SnapshotEndFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("SnapshotEnd too short"))?;
            Ok(PrimaryMessage::SnapshotEnd {
                crc32c: frame.crc32c.get(),
            })
        }
        MSG_HEARTBEAT => {
            let (frame, _) = HeartbeatFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("Heartbeat too short"))?;
            Ok(PrimaryMessage::Heartbeat {
                sequence: frame.sequence.get(),
            })
        }
        other => Err(io::Error::other(format!(
            "unknown primary message type: 0x{other:02x}"
        ))),
    }
}

// --- Catch-up helper: journal-codec bytes → InputSlot records ---
//
// The replication ring no longer carries journal-codec bytes (Phase 3
// switched it to wire-ready `InputBatch` frames produced by the journal
// stage). Catch-up still reads journal *files* — which are journal-codec
// — and decodes them into `InputSlot` records before re-encoding as
// `InputBatch` for the wire.

/// Decode a journal-codec byte stream into `InputSlot` records. Used by
/// the catch-up paths (`catchup.rs` for TCP, the DPDK catch-up loop) to
/// turn journal-file bytes into wire-ready `InputBatch` frames.
///
/// Generic over `E: AppEvent` — the journal codec decodes into the
/// application's event type, and the resulting `InputSlot<E>` records
/// are what the receiver's input ring expects.
pub fn decode_journal_to_input_slots<E: AppEvent>(
    journal_bytes: &[u8],
) -> io::Result<Vec<InputSlot<E>>> {
    let mut slots = Vec::with_capacity(64);
    let mut offset = 0;
    while offset < journal_bytes.len() {
        let (consumed, sequence, timestamp_ns, key_hash, request_seq, event) =
            melin_journal::codec::decode::<E>(&journal_bytes[offset..]).map_err(|e| {
                io::Error::other(format!("journal decode at offset {offset}: {e:?}"))
            })?;
        offset += consumed;
        slots.push(InputSlot {
            connection_id: 0,
            key_hash,
            request_seq,
            sequence,
            timestamp_ns,
            event,
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
    }
    Ok(slots)
}
