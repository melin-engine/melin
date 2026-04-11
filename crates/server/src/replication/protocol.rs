//! Replication wire protocol — message types, framing, encode/decode.
//!
//! Length-prefixed frames, little-endian, over a dedicated TCP connection
//! (or DPDK pipe). See `mod.rs` for the message catalogue.
//!
//! All items are `pub(super)` — the protocol is an internal contract
//! between the sender and receiver paths in the parent module.

use std::io::{self, Read};

// --- Wire protocol message tags ---

pub(super) const MSG_HANDSHAKE: u8 = 0x01;
pub(super) const MSG_ACK: u8 = 0x02;
// Auth messages (exchanged before the handshake).
pub(super) const MSG_CHALLENGE: u8 = 0x03;
pub(super) const MSG_CHALLENGE_RESPONSE: u8 = 0x04;
pub(super) const MSG_AUTH_OK: u8 = 0x05;
pub(super) const MSG_AUTH_FAILED: u8 = 0x06;
pub(super) const MSG_STREAM_START: u8 = 0x10;
pub(super) const MSG_NEED_SNAPSHOT: u8 = 0x11;
pub(super) const MSG_HASH_MISMATCH: u8 = 0x12;
pub(super) const MSG_SNAPSHOT_BEGIN: u8 = 0x13;
pub(super) const MSG_SNAPSHOT_CHUNK: u8 = 0x14;
pub(super) const MSG_SNAPSHOT_END: u8 = 0x15;
pub(super) const MSG_DATA_BATCH: u8 = 0x20;
pub(super) const MSG_HEARTBEAT: u8 = 0x30;

/// Maximum frame size for control messages (handshake, ack, etc.).
/// Data batches can be much larger (up to 128 KiB of journal data).
pub(super) const MAX_CONTROL_FRAME: usize = 256;

/// Maximum data batch frame size. Must be >= CHUNK_SIZE (512 KiB) in the
/// replication ring, plus header overhead (45 bytes). Ring batches can use
/// the full 512 KiB chunk, so the frame limit must accommodate that.
pub(super) const MAX_DATA_FRAME: usize = 768 * 1024;

// --- Message structs / enums ---

/// Handshake message sent by the replica on connection.
#[derive(Debug, Clone)]
pub struct Handshake {
    pub last_sequence: u64,
    pub chain_hash: [u8; 32],
}

/// Ack message sent by the replica after durable write.
#[derive(Debug, Clone, Copy)]
pub struct Ack {
    pub acked_sequence: u64,
}

/// Messages from primary to replica.
#[derive(Debug)]
pub enum PrimaryMessage {
    StreamStart {
        start_sequence: u64,
        /// Primary's raw genesis entry bytes — the replica writes these
        /// directly to its journal for a byte-identical hash chain start.
        genesis_entry: Vec<u8>,
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
    DataBatch {
        end_sequence: u64,
        chain_hash: [u8; 32],
        entry_count: u32,
        journal_bytes: Vec<u8>,
    },
    Heartbeat {
        sequence: u64,
        chain_hash: [u8; 32],
    },
}

/// Messages from replica to primary.
#[derive(Debug)]
pub enum ReplicaMessage {
    Handshake(Handshake),
    Ack(Ack),
}

// --- Encoders ---

/// Encode a handshake message into a frame (length-prefixed).
pub(super) fn encode_handshake(h: &Handshake, buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8 + 32; // type + sequence + hash
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HANDSHAKE);
    buf.extend_from_slice(&h.last_sequence.to_le_bytes());
    buf.extend_from_slice(&h.chain_hash);
}

/// Encode an ack message into a frame.
pub(super) fn encode_ack(ack: &Ack, buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8; // type + sequence
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_ACK);
    buf.extend_from_slice(&ack.acked_sequence.to_le_bytes());
}

/// Encode a Challenge message (primary → replica).
pub(super) fn encode_challenge(nonce: &[u8; 32], buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 32; // type + nonce
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_CHALLENGE);
    buf.extend_from_slice(nonce);
}

/// Encode a ChallengeResponse message (replica → primary).
pub(super) fn encode_challenge_response(
    signature: &[u8; 64],
    pubkey: &[u8; 32],
    buf: &mut Vec<u8>,
) {
    let payload_len: u32 = 1 + 64 + 32; // type + signature + pubkey
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_CHALLENGE_RESPONSE);
    buf.extend_from_slice(signature);
    buf.extend_from_slice(pubkey);
}

/// Encode an AuthOk message (primary → replica).
pub(super) fn encode_auth_ok(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_OK);
}

/// Encode an AuthFailed message (primary → replica).
pub(super) fn encode_auth_failed(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_FAILED);
}

/// Encode a StreamStart message into a frame.
///
/// Includes the primary's raw genesis entry bytes so the replica can
/// write a byte-identical genesis to its journal. This ensures the
/// BLAKE3 hash chain starts from the exact same encoded bytes (including
/// the timestamp), so checkpoint verification works on the replica.
pub(super) fn encode_stream_start(
    start_sequence: u64,
    genesis_entry_bytes: &[u8],
    buf: &mut Vec<u8>,
) {
    // type(1) + sequence(8) + genesis_len(4) + genesis_bytes
    let payload_len: u32 = (1 + 8 + 4 + genesis_entry_bytes.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_STREAM_START);
    buf.extend_from_slice(&start_sequence.to_le_bytes());
    buf.extend_from_slice(&(genesis_entry_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(genesis_entry_bytes);
}

/// Encode a NeedSnapshot message.
pub(super) fn encode_need_snapshot(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_NEED_SNAPSHOT);
}

/// Encode a SnapshotBegin message.
pub(super) fn encode_snapshot_begin(
    snapshot_len: u64,
    snap_sequence: u64,
    snap_chain_hash: &[u8; 32],
    buf: &mut Vec<u8>,
) {
    // type(1) + snapshot_len(8) + snap_sequence(8) + snap_chain_hash(32)
    let payload_len: u32 = 1 + 8 + 8 + 32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_BEGIN);
    buf.extend_from_slice(&snapshot_len.to_le_bytes());
    buf.extend_from_slice(&snap_sequence.to_le_bytes());
    buf.extend_from_slice(snap_chain_hash);
}

/// Encode a SnapshotChunk message.
pub(super) fn encode_snapshot_chunk(data: &[u8], buf: &mut Vec<u8>) {
    // type(1) + data
    let payload_len: u32 = (1 + data.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_CHUNK);
    buf.extend_from_slice(data);
}

/// Encode a SnapshotEnd message.
pub(super) fn encode_snapshot_end(crc32c: u32, buf: &mut Vec<u8>) {
    // type(1) + crc32c(4)
    let payload_len: u32 = 1 + 4;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_END);
    buf.extend_from_slice(&crc32c.to_le_bytes());
}

/// Encode a HashMismatch message.
#[allow(dead_code)] // Used in future catch-up implementation.
pub(super) fn encode_hash_mismatch(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HASH_MISMATCH);
}

/// Encode a DataBatch message.
pub(super) fn encode_data_batch(
    end_sequence: u64,
    chain_hash: &[u8; 32],
    entry_count: u32,
    journal_bytes: &[u8],
    buf: &mut Vec<u8>,
) {
    // type(1) + end_sequence(8) + chain_hash(32) + entry_count(4) + journal_bytes
    let payload_len: u32 = (1 + 8 + 32 + 4 + journal_bytes.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_DATA_BATCH);
    buf.extend_from_slice(&end_sequence.to_le_bytes());
    buf.extend_from_slice(chain_hash);
    buf.extend_from_slice(&entry_count.to_le_bytes());
    buf.extend_from_slice(journal_bytes);
}

/// Encode a Heartbeat message.
pub(super) fn encode_heartbeat(sequence: u64, chain_hash: &[u8; 32], buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8 + 32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HEARTBEAT);
    buf.extend_from_slice(&sequence.to_le_bytes());
    buf.extend_from_slice(chain_hash);
}

// --- Decoders / framing ---

/// Read a length-prefixed frame from a stream. Returns the payload (without length prefix).
pub(super) fn read_frame(reader: &mut impl Read, max_size: usize) -> io::Result<Vec<u8>> {
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
pub(super) fn decode_challenge(payload: &[u8]) -> io::Result<[u8; 32]> {
    if payload.len() < 1 + 32 {
        return Err(io::Error::other("challenge too short"));
    }
    if payload[0] != MSG_CHALLENGE {
        return Err(io::Error::other(format!(
            "expected Challenge (0x{:02x}), got 0x{:02x}",
            MSG_CHALLENGE, payload[0]
        )));
    }
    let mut nonce = [0u8; 32];
    nonce.copy_from_slice(&payload[1..33]);
    Ok(nonce)
}

/// Decode a ChallengeResponse payload → (signature, pubkey).
pub(super) fn decode_challenge_response(payload: &[u8]) -> io::Result<([u8; 64], [u8; 32])> {
    if payload.len() < 1 + 64 + 32 {
        return Err(io::Error::other("challenge response too short"));
    }
    if payload[0] != MSG_CHALLENGE_RESPONSE {
        return Err(io::Error::other(format!(
            "expected ChallengeResponse (0x{:02x}), got 0x{:02x}",
            MSG_CHALLENGE_RESPONSE, payload[0]
        )));
    }
    let mut signature = [0u8; 64];
    signature.copy_from_slice(&payload[1..65]);
    let mut pubkey = [0u8; 32];
    pubkey.copy_from_slice(&payload[65..97]);
    Ok((signature, pubkey))
}

/// Decode an auth result payload → true if AuthOk, false if AuthFailed.
pub(super) fn decode_auth_result(payload: &[u8]) -> io::Result<bool> {
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
pub(super) fn decode_replica_message(payload: &[u8]) -> io::Result<ReplicaMessage> {
    if payload.is_empty() {
        return Err(io::Error::other("empty payload"));
    }
    match payload[0] {
        MSG_HANDSHAKE => {
            if payload.len() < 1 + 8 + 32 {
                return Err(io::Error::other("handshake too short"));
            }
            let last_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let mut chain_hash = [0u8; 32];
            chain_hash.copy_from_slice(&payload[9..41]);
            Ok(ReplicaMessage::Handshake(Handshake {
                last_sequence,
                chain_hash,
            }))
        }
        MSG_ACK => {
            if payload.len() < 1 + 8 {
                return Err(io::Error::other("ack too short"));
            }
            let acked_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            Ok(ReplicaMessage::Ack(Ack { acked_sequence }))
        }
        other => Err(io::Error::other(format!(
            "unknown replica message type: 0x{other:02x}"
        ))),
    }
}

/// Fast-path decoder for `DataBatch` frames. Returns a *borrowed* slice
/// into `payload` so the receiver hot path can copy journal bytes directly
/// into its accumulator without the `Vec<u8>` allocation that the general
/// [`decode_primary_message`] performs on the `MSG_DATA_BATCH` arm.
///
/// Returns `None` in two cases:
/// - the payload is not a `DataBatch` (different type tag) — the caller
///   should fall back to [`decode_primary_message`] to handle control
///   messages (heartbeats, hash-mismatch, etc.).
/// - the payload *is* tagged as a `DataBatch` but is shorter than the
///   fixed header — indistinguishable from the non-data case here, so the
///   caller's general-decoder fallback will surface the truncation as a
///   protocol error.
pub(super) fn try_decode_data_batch(payload: &[u8]) -> Option<(u64, [u8; 32], u32, &[u8])> {
    // Layout: type(1) + end_sequence(8) + chain_hash(32) + entry_count(4) + journal_bytes
    const HEADER: usize = 1 + 8 + 32 + 4;
    if payload.len() < HEADER || payload[0] != MSG_DATA_BATCH {
        return None;
    }
    let end_sequence = u64::from_le_bytes(payload[1..9].try_into().ok()?);
    // Fixed-size array copy — explicit so the borrow checker can reason
    // about `chain_hash` independently from the returned `journal_bytes`
    // slice, which still borrows from `payload`.
    let mut chain_hash = [0u8; 32];
    chain_hash.copy_from_slice(&payload[9..41]);
    let entry_count = u32::from_le_bytes(payload[41..45].try_into().ok()?);
    let journal_bytes = &payload[HEADER..];
    Some((end_sequence, chain_hash, entry_count, journal_bytes))
}

/// Decode a primary message from a frame payload.
pub(super) fn decode_primary_message(payload: &[u8]) -> io::Result<PrimaryMessage> {
    if payload.is_empty() {
        return Err(io::Error::other("empty payload"));
    }
    match payload[0] {
        MSG_STREAM_START => {
            if payload.len() < 1 + 8 + 4 {
                return Err(io::Error::other("StreamStart too short"));
            }
            let start_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let genesis_len = u32::from_le_bytes(payload[9..13].try_into().unwrap()) as usize;
            if payload.len() < 13 + genesis_len {
                return Err(io::Error::other("StreamStart genesis truncated"));
            }
            let genesis_entry = payload[13..13 + genesis_len].to_vec();
            Ok(PrimaryMessage::StreamStart {
                start_sequence,
                genesis_entry,
            })
        }
        MSG_NEED_SNAPSHOT => Ok(PrimaryMessage::NeedSnapshot),
        MSG_HASH_MISMATCH => Ok(PrimaryMessage::HashMismatch),
        MSG_SNAPSHOT_BEGIN => {
            if payload.len() < 1 + 8 + 8 + 32 {
                return Err(io::Error::other("SnapshotBegin too short"));
            }
            let snapshot_len = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let snap_sequence = u64::from_le_bytes(payload[9..17].try_into().unwrap());
            let mut snap_chain_hash = [0u8; 32];
            snap_chain_hash.copy_from_slice(&payload[17..49]);
            Ok(PrimaryMessage::SnapshotBegin {
                snapshot_len,
                snap_sequence,
                snap_chain_hash,
            })
        }
        MSG_SNAPSHOT_CHUNK => {
            let data = payload[1..].to_vec();
            Ok(PrimaryMessage::SnapshotChunk(data))
        }
        MSG_SNAPSHOT_END => {
            if payload.len() < 1 + 4 {
                return Err(io::Error::other("SnapshotEnd too short"));
            }
            let crc32c = u32::from_le_bytes(payload[1..5].try_into().unwrap());
            Ok(PrimaryMessage::SnapshotEnd { crc32c })
        }
        MSG_DATA_BATCH => {
            if payload.len() < 1 + 8 + 32 + 4 {
                return Err(io::Error::other("DataBatch too short"));
            }
            let end_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let mut chain_hash = [0u8; 32];
            chain_hash.copy_from_slice(&payload[9..41]);
            let entry_count = u32::from_le_bytes(payload[41..45].try_into().unwrap());
            let journal_bytes = payload[45..].to_vec();
            Ok(PrimaryMessage::DataBatch {
                end_sequence,
                chain_hash,
                entry_count,
                journal_bytes,
            })
        }
        MSG_HEARTBEAT => {
            if payload.len() < 1 + 8 + 32 {
                return Err(io::Error::other("Heartbeat too short"));
            }
            let sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
            let mut chain_hash = [0u8; 32];
            chain_hash.copy_from_slice(&payload[9..41]);
            Ok(PrimaryMessage::Heartbeat {
                sequence,
                chain_hash,
            })
        }
        other => Err(io::Error::other(format!(
            "unknown primary message type: 0x{other:02x}"
        ))),
    }
}
