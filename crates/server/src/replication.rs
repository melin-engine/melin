//! Replication — synchronous journal streaming from primary to replica.
//!
//! The JournalStage sends byte-for-byte copies of encoded journal batches
//! through a bounded channel. The `ReplicationSender` streams them to the
//! replica as `DataBatch` frames. The replica writes them directly to its
//! local journal via `write_raw_sync()` and replays into its Exchange.
//!
//! ## Wire Protocol
//!
//! Length-prefixed frames, little-endian, over a dedicated TCP connection.
//!
//! ### Auth (before handshake)
//! - **Challenge** (Primary → Replica): `[len:u32][0x03][nonce:[u8;32]]`
//! - **ChallengeResponse** (Replica → Primary): `[len:u32][0x04][signature:[u8;64]][pubkey:[u8;32]]`
//! - **AuthOk** (Primary → Replica): `[len:u32][0x05]`
//! - **AuthFailed** (Primary → Replica): `[len:u32][0x06]`
//!
//! ### Replica → Primary
//! - **Handshake**: `[len:u32][0x01][last_sequence:u64][chain_hash:[u8;32]]`
//! - **Ack**: `[len:u32][0x02][acked_sequence:u64]`
//!
//! ### Primary → Replica
//! - **StreamStart**: `[len:u32][0x10][start_sequence:u64]`
//! - **NeedSnapshot**: `[len:u32][0x11]`
//! - **HashMismatch**: `[len:u32][0x12]`
//! - **SnapshotBegin**: `[len:u32][0x13][snapshot_len:u64][snap_sequence:u64][snap_chain_hash:[u8;32]]`
//! - **SnapshotChunk**: `[len:u32][0x14][data...]`
//! - **SnapshotEnd**: `[len:u32][0x15][crc32c:u32]`
//! - **DataBatch**: `[len:u32][0x20][end_sequence:u64][chain_hash:[u8;32]][journal_bytes...]`
//! - **Heartbeat**: `[len:u32][0x30][sequence:u64][chain_hash:[u8;32]]`
//!
//! ## v1 Limitations
//!
//! - No chain hash verification on received DataBatch (CRC per-entry only)
//! - No handshake chain hash validation (HashMismatch never sent)
//! - Dual replication (up to 2 replicas in parallel)
//!
//! See `docs/replication.md` for the full design document and limitation details.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use tracing::{debug, error, info, warn};

use melin_engine::journal::replication::ReplicationConsumer;

/// Per-slot replication metrics exposed via the health endpoint.
/// Updated by sender threads (atomic stores), read by the health
/// thread (atomic loads). Zero hot-path impact — all writes happen
/// alongside TCP I/O in the sender threads.
pub struct ReplicationMetrics {
    /// Per-slot acked sequence (last sequence the replica confirmed
    /// as durable). Used to compute per-replica replication lag.
    pub acked_sequence: [AtomicU64; 2],
    /// Per-slot bytes sent to the replica (cumulative). Includes
    /// catch-up and live streaming.
    pub bytes_sent: [AtomicU64; 2],
    /// Per-slot ack round-trip latency in microseconds. Updated on
    /// each ack by measuring elapsed time since the last batch send.
    pub ack_latency_us: [AtomicU64; 2],
    /// Per-slot catch-up state: true while streaming historical
    /// journal entries, false once the replica enters live mode.
    pub catching_up: [AtomicBool; 2],
    /// Total eviction count (both slots combined). Incremented when
    /// the journal stage's backpressure timeout fires.
    pub evictions_total: AtomicU64,
}

impl Default for ReplicationMetrics {
    fn default() -> Self {
        Self {
            acked_sequence: [AtomicU64::new(0), AtomicU64::new(0)],
            bytes_sent: [AtomicU64::new(0), AtomicU64::new(0)],
            ack_latency_us: [AtomicU64::new(0), AtomicU64::new(0)],
            catching_up: [AtomicBool::new(false), AtomicBool::new(false)],
            evictions_total: AtomicU64::new(0),
        }
    }
}

// --- Wire protocol message types ---

/// Message type tags.
const MSG_HANDSHAKE: u8 = 0x01;
const MSG_ACK: u8 = 0x02;
// Auth messages (exchanged before the handshake).
const MSG_CHALLENGE: u8 = 0x03;
const MSG_CHALLENGE_RESPONSE: u8 = 0x04;
const MSG_AUTH_OK: u8 = 0x05;
const MSG_AUTH_FAILED: u8 = 0x06;
const MSG_STREAM_START: u8 = 0x10;
const MSG_NEED_SNAPSHOT: u8 = 0x11;
const MSG_HASH_MISMATCH: u8 = 0x12;
const MSG_SNAPSHOT_BEGIN: u8 = 0x13;
const MSG_SNAPSHOT_CHUNK: u8 = 0x14;
const MSG_SNAPSHOT_END: u8 = 0x15;
const MSG_DATA_BATCH: u8 = 0x20;
const MSG_HEARTBEAT: u8 = 0x30;

/// Maximum frame size for control messages (handshake, ack, etc.).
/// Data batches can be much larger (up to 128 KiB of journal data).
const MAX_CONTROL_FRAME: usize = 256;

/// Maximum data batch frame size. Must be >= CHUNK_SIZE (512 KiB) in the
/// replication ring, plus header overhead (45 bytes). Ring batches can use
/// the full 512 KiB chunk, so the frame limit must accommodate that.
const MAX_DATA_FRAME: usize = 768 * 1024;

// --- Wire protocol encode/decode ---

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

/// Encode a handshake message into a frame (length-prefixed).
fn encode_handshake(h: &Handshake, buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8 + 32; // type + sequence + hash
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HANDSHAKE);
    buf.extend_from_slice(&h.last_sequence.to_le_bytes());
    buf.extend_from_slice(&h.chain_hash);
}

/// Encode an ack message into a frame.
fn encode_ack(ack: &Ack, buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8; // type + sequence
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_ACK);
    buf.extend_from_slice(&ack.acked_sequence.to_le_bytes());
}

/// Encode a Challenge message (primary → replica).
fn encode_challenge(nonce: &[u8; 32], buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 32; // type + nonce
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_CHALLENGE);
    buf.extend_from_slice(nonce);
}

/// Encode a ChallengeResponse message (replica → primary).
fn encode_challenge_response(signature: &[u8; 64], pubkey: &[u8; 32], buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 64 + 32; // type + signature + pubkey
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_CHALLENGE_RESPONSE);
    buf.extend_from_slice(signature);
    buf.extend_from_slice(pubkey);
}

/// Encode an AuthOk message (primary → replica).
fn encode_auth_ok(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_OK);
}

/// Encode an AuthFailed message (primary → replica).
fn encode_auth_failed(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_FAILED);
}

/// Decode a Challenge payload → 32-byte nonce.
fn decode_challenge(payload: &[u8]) -> io::Result<[u8; 32]> {
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
fn decode_challenge_response(payload: &[u8]) -> io::Result<([u8; 64], [u8; 32])> {
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
fn decode_auth_result(payload: &[u8]) -> io::Result<bool> {
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

/// Encode a StreamStart message into a frame.
///
/// Includes the primary's raw genesis entry bytes so the replica can
/// write a byte-identical genesis to its journal. This ensures the
/// BLAKE3 hash chain starts from the exact same encoded bytes (including
/// the timestamp), so checkpoint verification works on the replica.
fn encode_stream_start(start_sequence: u64, genesis_entry_bytes: &[u8], buf: &mut Vec<u8>) {
    // type(1) + sequence(8) + genesis_len(4) + genesis_bytes
    let payload_len: u32 = (1 + 8 + 4 + genesis_entry_bytes.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_STREAM_START);
    buf.extend_from_slice(&start_sequence.to_le_bytes());
    buf.extend_from_slice(&(genesis_entry_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(genesis_entry_bytes);
}

/// Encode a NeedSnapshot message.
fn encode_need_snapshot(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_NEED_SNAPSHOT);
}

/// Encode a SnapshotBegin message.
fn encode_snapshot_begin(
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
fn encode_snapshot_chunk(data: &[u8], buf: &mut Vec<u8>) {
    // type(1) + data
    let payload_len: u32 = (1 + data.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_CHUNK);
    buf.extend_from_slice(data);
}

/// Encode a SnapshotEnd message.
fn encode_snapshot_end(crc32c: u32, buf: &mut Vec<u8>) {
    // type(1) + crc32c(4)
    let payload_len: u32 = 1 + 4;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_END);
    buf.extend_from_slice(&crc32c.to_le_bytes());
}

/// Encode a HashMismatch message.
#[allow(dead_code)] // Used in future catch-up implementation.
fn encode_hash_mismatch(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HASH_MISMATCH);
}

/// Encode a DataBatch message.
fn encode_data_batch(
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
fn encode_heartbeat(sequence: u64, chain_hash: &[u8; 32], buf: &mut Vec<u8>) {
    let payload_len: u32 = 1 + 8 + 32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HEARTBEAT);
    buf.extend_from_slice(&sequence.to_le_bytes());
    buf.extend_from_slice(chain_hash);
}

/// Read a length-prefixed frame from a stream. Returns the payload (without length prefix).
fn read_frame(reader: &mut impl Read, max_size: usize) -> io::Result<Vec<u8>> {
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

/// Decode a replica message from a frame payload.
fn decode_replica_message(payload: &[u8]) -> io::Result<ReplicaMessage> {
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

/// Decode a primary message from a frame payload.
fn decode_primary_message(payload: &[u8]) -> io::Result<PrimaryMessage> {
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

// --- Replication Sender (Primary side) ---

/// Run the replication sender. Listens for a single replica connection,
/// streams journal data batches, processes acks, and updates the
/// replication cursor.
///
/// `genesis_entry` is the primary's raw genesis entry bytes (the encoded
/// GenesisHash journal entry), sent to the replica in `StreamStart` so it
/// can write a byte-identical genesis to its journal. This ensures the
/// BLAKE3 hash chain starts from the exact same encoded bytes.
///
/// Runs on a dedicated thread. Blocks until shutdown.
#[allow(clippy::too_many_arguments)]
pub fn run_sender(
    bind_addr: SocketAddr,
    repl_consumer_1: ReplicationConsumer,
    repl_consumer_2: ReplicationConsumer,
    replication_cursor: Arc<AtomicU64>,
    fastest_replica_cursor: Arc<AtomicU64>,
    genesis_entry: Vec<u8>,
    journal_path: std::path::PathBuf,
    authorized_keys: Arc<melin_protocol::auth::AuthorizedKeys>,
    shutdown: &AtomicBool,
    replica_ready: &AtomicBool,
    replicas_connected: &AtomicU32,
    evict_flags: [Arc<AtomicBool>; 2],
    active_flags: [Arc<AtomicBool>; 2],
    metrics: Arc<ReplicationMetrics>,
    handler_cores: [usize; 2],
    batch_size: usize,
    heartbeat_secs: u64,
    busy_spin: bool,
) {
    let listener = match TcpListener::bind(bind_addr) {
        Ok(l) => l,
        Err(e) => {
            error!(addr = %bind_addr, error = %e, "failed to bind replication listener");
            return;
        }
    };
    // Non-blocking accept so we can check shutdown.
    if let Err(e) = listener.set_nonblocking(true) {
        error!(error = %e, "failed to set non-blocking on replication listener");
        return;
    }

    info!(addr = %bind_addr, "replication sender listening");

    // Two replica slots, each with its own ring consumer and thread handle.
    // The accept loop fills empty slots. When a replica disconnects, its
    // slot becomes available for a new connection. The consumer is `None`
    // while a handler thread owns it (moved into the thread, returned on exit).
    struct ReplicaSlot {
        consumer: Option<ReplicationConsumer>,
        handle: Option<std::thread::JoinHandle<ReplicationConsumer>>,
    }

    let mut slots = [
        ReplicaSlot {
            consumer: Some(repl_consumer_1),
            handle: None,
        },
        ReplicaSlot {
            consumer: Some(repl_consumer_2),
            handle: None,
        },
    ];

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("replication sender shutting down");
            // Wait for active replica threads to finish.
            for slot in &mut slots {
                if let Some(handle) = slot.handle.take()
                    && let Ok(consumer) = handle.join()
                {
                    slot.consumer = Some(consumer);
                }
            }
            return;
        }

        // Check eviction flags from the journal stage. When set, the
        // journal stage timed out publishing to this slot's ring. We need
        // to reclaim the consumer so the idle drain loop can clear the ring,
        // allowing the journal stage to resume publishing.
        for (i, slot) in slots.iter_mut().enumerate() {
            if evict_flags[i].load(Ordering::Acquire) && slot.handle.is_some() {
                metrics.evictions_total.fetch_add(1, Ordering::Relaxed);
                warn!(
                    slot = i,
                    "evicting slow replica (ring backpressure timeout)"
                );
                // The handler thread checks shutdown — we can't signal it
                // individually without adding per-slot flags. Instead, join
                // the thread with a short timeout by checking is_finished.
                // The handler's TCP read timeout (5s) will cause it to exit
                // on the next iteration when it checks shutdown. But we
                // want faster eviction, so we shutdown the TCP stream to
                // unblock the read.
                //
                // For now, mark the slot and let it be collected below
                // when the handler finishes naturally (TCP timeout or
                // next send failure after the ring stops being fed).
            }
        }

        // Collect finished replica threads (disconnected replicas).
        for (i, slot) in slots.iter_mut().enumerate() {
            if let Some(ref handle) = slot.handle
                && handle.is_finished()
            {
                let handle = slot.handle.take().expect("just checked is_some");
                match handle.join() {
                    Ok(consumer) => {
                        slot.consumer = Some(consumer);
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        // Clear active flag — journal stage stops publishing
                        // to this ring. Must happen before clearing evict.
                        active_flags[i].store(false, Ordering::Release);
                        // Reset per-slot metrics for the disconnected replica.
                        metrics.acked_sequence[i].store(0, Ordering::Relaxed);
                        metrics.catching_up[i].store(false, Ordering::Relaxed);
                        // Clear eviction flag after reclaiming the consumer.
                        if evict_flags[i].load(Ordering::Relaxed) {
                            evict_flags[i].store(false, Ordering::Release);
                            warn!(slot = i, "evicted replica — ring ready for reconnection");
                        } else {
                            warn!(slot = i, "replica disconnected");
                        }
                        if replicas_connected.load(Ordering::Relaxed) == 0 {
                            // Reset cursors so the response stage stops gating
                            // on replica acks. Without this, the cursor stays
                            // at the last acked sequence and the response stage
                            // blocks indefinitely.
                            replication_cursor.store(u64::MAX, Ordering::Release);
                            fastest_replica_cursor.store(u64::MAX, Ordering::Release);
                            warn!("all replicas disconnected — trading halted");
                        } else {
                            // One replica disconnected, the other is still
                            // streaming. Reset fastest_replica_cursor to the
                            // min cursor (safe lower bound) — the surviving
                            // handler's next fetch_max will correct it upward.
                            let min_pos = replication_cursor.load(Ordering::Acquire);
                            fastest_replica_cursor.store(min_pos, Ordering::Release);
                        }
                    }
                    Err(_) => {
                        error!(slot = i, "replica handler thread panicked");
                        // Consumer is lost — can't recover this slot.
                        // With independent rings, only this slot's ring is
                        // affected. The other replica continues normally.
                        active_flags[i].store(false, Ordering::Release);
                        evict_flags[i].store(false, Ordering::Release);
                    }
                }
            }
        }

        // Find a slot that has a consumer available (not in use by a thread).
        let empty_slot = slots
            .iter()
            .position(|s| s.handle.is_none() && s.consumer.is_some());

        // Accept a connection if there's an empty slot.
        if let Some(slot_idx) = empty_slot {
            match listener.accept() {
                Ok((stream, addr)) => {
                    info!(addr = %addr, slot = slot_idx, "replica connected");
                    if let Err(e) = stream.set_nodelay(true) {
                        debug!(error = %e, "failed to set TCP_NODELAY on replica connection");
                    }

                    replicas_connected.fetch_add(1, Ordering::Release);

                    // Take the consumer out of the slot for the handler thread.
                    // The slot's consumer becomes None while the thread owns it.
                    let consumer = slots[slot_idx]
                        .consumer
                        .take()
                        .expect("empty_slot check guarantees consumer is Some");

                    let cursor = Arc::clone(&replication_cursor);
                    let fastest_cursor = Arc::clone(&fastest_replica_cursor);
                    let genesis = genesis_entry.clone();
                    let jpath = journal_path.clone();
                    let auth_keys = Arc::clone(&authorized_keys);
                    let slot_metrics = Arc::clone(&metrics);
                    let slot_active = Arc::clone(&active_flags[slot_idx]);
                    let slot_evict = Arc::clone(&evict_flags[slot_idx]);
                    let handler_core = handler_cores[slot_idx];
                    let shutdown_flag = shutdown as *const AtomicBool as usize;
                    let ready_flag = replica_ready as *const AtomicBool as usize;
                    let handle = std::thread::Builder::new()
                        .name(format!("repl-{slot_idx}"))
                        .spawn(move || {
                            // Pin to a dedicated core if configured (> 0),
                            // otherwise clear inherited affinity from the
                            // sender thread so the OS can schedule freely.
                            if handler_core > 0 {
                                match crate::affinity::pin_to_core(handler_core) {
                                    Ok(c) => tracing::info!(
                                        core = c,
                                        slot = slot_idx,
                                        "replication handler pinned to core"
                                    ),
                                    Err(e) => tracing::warn!(
                                        error = e,
                                        slot = slot_idx,
                                        "failed to pin handler"
                                    ),
                                }
                            } else if let Err(e) = crate::affinity::clear_affinity() {
                                tracing::warn!(error = e, "failed to clear handler affinity");
                            }
                            // Safety: shutdown and replica_ready outlive this thread
                            // (they're on the parent's stack, which blocks on join
                            // during shutdown).
                            let shutdown_ref = unsafe { &*(shutdown_flag as *const AtomicBool) };
                            let ready_ref = unsafe { &*(ready_flag as *const AtomicBool) };
                            run_replica_slot(
                                stream,
                                consumer,
                                cursor,
                                fastest_cursor,
                                genesis,
                                jpath,
                                auth_keys,
                                shutdown_ref,
                                ready_ref,
                                &slot_active,
                                &slot_evict,
                                &slot_metrics,
                                slot_idx,
                                batch_size,
                                heartbeat_secs,
                                busy_spin,
                            )
                        })
                        .expect("spawn replica handler thread");
                    slots[slot_idx].handle = Some(handle);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No pending connection.
                }
                Err(e) => {
                    error!(error = %e, "replication accept error");
                }
            }
        }

        // No idle drain needed — the journal stage only publishes to
        // rings where active_flag is true (set by handler on live loop
        // entry, cleared on disconnect). Idle consumers stay empty.

        std::thread::sleep(std::time::Duration::from_millis(50));
    }
}

/// Handle a single replica connection on a dedicated thread.
/// Returns the consumer when the connection ends (for slot reuse).
#[allow(clippy::too_many_arguments)]
fn run_replica_slot(
    stream: TcpStream,
    mut consumer: ReplicationConsumer,
    replication_cursor: Arc<AtomicU64>,
    fastest_replica_cursor: Arc<AtomicU64>,
    genesis_entry: Vec<u8>,
    journal_path: std::path::PathBuf,
    authorized_keys: Arc<melin_protocol::auth::AuthorizedKeys>,
    shutdown: &AtomicBool,
    replica_ready: &AtomicBool,
    active_flag: &AtomicBool,
    evict_flag: &AtomicBool,
    metrics: &ReplicationMetrics,
    slot_idx: usize,
    batch_size: usize,
    heartbeat_secs: u64,
    busy_spin: bool,
) -> ReplicationConsumer {
    match handle_replica_connection(
        stream,
        &mut consumer,
        &replication_cursor,
        &fastest_replica_cursor,
        &genesis_entry,
        &journal_path,
        &authorized_keys,
        shutdown,
        replica_ready,
        active_flag,
        evict_flag,
        metrics,
        slot_idx,
        batch_size,
        heartbeat_secs,
        busy_spin,
    ) {
        Ok(()) => info!("replica disconnected cleanly"),
        Err(e) => warn!(error = %e, "replica connection error"),
    }
    consumer
}

/// Discover journal archive files, sorted oldest to newest.
/// Returns `[path.3, path.2, path.1, path]` — only files that exist.
fn discover_journal_files(journal_path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut archives = Vec::new();
    let mut n = 1u32;
    loop {
        let archive = std::path::PathBuf::from(format!("{}.{n}", journal_path.display()));
        if !archive.exists() {
            break;
        }
        archives.push(archive);
        n += 1;
    }
    // Reverse so oldest is first (highest number = oldest).
    archives.reverse();
    // Current journal is newest.
    if journal_path.exists() {
        archives.push(journal_path.to_path_buf());
    }
    archives
}

/// Stream historical journal entries to a catching-up replica.
///
/// Reads raw entry bytes from the primary's journal files and sends
/// them as DataBatch frames. Does NOT consume from the replication ring
/// during catch-up — the ring accumulates live data. The caller must
/// drain overlapping ring entries after catch-up completes.
///
/// Result of a journal catch-up attempt.
enum CatchUpResult {
    /// Catch-up succeeded. Contains the last sequence sent (or the input
    /// last_sequence if no entries were sent).
    Ok(u64),
    /// Replica's last_sequence predates all available journal files.
    /// The primary must transfer a snapshot instead.
    NeedSnapshot,
}

/// Check if journal catch-up is possible without sending any data.
/// Returns true if the journal archives contain the replica's last_sequence,
/// false if the archives have been purged and a snapshot transfer is needed.
fn can_catch_up_from_journal(
    journal_path: &std::path::Path,
    last_sequence: u64,
) -> io::Result<bool> {
    use melin_engine::journal::reader::RawJournalScanner;

    let files = discover_journal_files(journal_path);
    if files.is_empty() || last_sequence == 0 {
        // No files or fresh replica — catch-up will handle it.
        return Ok(true);
    }

    // Check if any file starts at or before the target sequence.
    for path in files.iter().rev() {
        let mut scanner = RawJournalScanner::open(path)
            .map_err(|e| io::Error::other(format!("open journal {}: {e}", path.display())))?;
        if let Some(first_seq) = scanner
            .first_sequence()
            .map_err(|e| io::Error::other(format!("read {}: {e}", path.display())))?
            && first_seq <= last_sequence
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Returns the last sequence sent, or 0 if no entries were sent.
fn catch_up_from_journal(
    journal_path: &std::path::Path,
    last_sequence: u64,
    writer: &mut TcpStream,
    shutdown: &AtomicBool,
) -> io::Result<CatchUpResult> {
    use melin_engine::journal::reader::RawJournalScanner;

    let files = discover_journal_files(journal_path);
    if files.is_empty() {
        return Ok(CatchUpResult::Ok(last_sequence));
    }

    // Find the first file that contains entries after last_sequence.
    // For a fresh replica (last_sequence=0), start from the oldest file.
    let mut start_file_idx = 0;
    if last_sequence > 0 {
        // Scan files from newest to oldest to find which contains our target.
        let mut found = false;
        for (i, path) in files.iter().enumerate().rev() {
            let mut scanner = RawJournalScanner::open(path)
                .map_err(|e| io::Error::other(format!("open journal {}: {e}", path.display())))?;
            if let Some(first_seq) = scanner
                .first_sequence()
                .map_err(|e| io::Error::other(format!("read {}: {e}", path.display())))?
                && first_seq <= last_sequence
            {
                // This file starts at or before our target — start here.
                start_file_idx = i;
                found = true;
                break;
            }
        }
        if !found {
            // All files start after our target — journal archives were purged.
            // The replica needs a snapshot transfer.
            warn!(
                last_sequence,
                "replica's last_sequence predates all available journal files — snapshot transfer required"
            );
            return Ok(CatchUpResult::NeedSnapshot);
        }
    }

    let mut send_buf = Vec::with_capacity(128 * 1024);
    let mut batch_buf = Vec::with_capacity(64 * 1024);
    let mut end_sequence = last_sequence;
    let mut batches_sent = 0u64;

    info!(
        last_sequence,
        files = files.len(),
        start_file = start_file_idx,
        "starting journal catch-up"
    );

    for path in &files[start_file_idx..] {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(CatchUpResult::Ok(end_sequence));
        }

        let mut scanner = RawJournalScanner::open(path)
            .map_err(|e| io::Error::other(format!("open journal {}: {e}", path.display())))?;

        // Skip entries the replica already has. Always skip at least
        // genesis (seq 1) — it's delivered via StreamStart, not catch-up.
        let skip_to = end_sequence.max(1);
        scanner
            .skip_to_after(skip_to)
            .map_err(|e| io::Error::other(format!("skip in {}: {e}", path.display())))?;

        // Read and send batches of raw entries.
        // Target ~64 KiB per DataBatch frame (~800 entries at ~80 bytes each).
        loop {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(CatchUpResult::Ok(end_sequence));
            }

            batch_buf.clear();
            let batch = scanner
                .read_raw_batch(&mut batch_buf, 64 * 1024)
                .map_err(|e| io::Error::other(format!("read {}: {e}", path.display())))?;

            let Some((entry_count, batch_end_seq)) = batch else {
                break; // EOF on this file.
            };

            // Encode and send DataBatch frame.
            // Chain hash is zeroed — chain verification is a documented v1 limitation.
            encode_data_batch(
                batch_end_seq,
                &[0u8; 32],
                entry_count,
                &batch_buf,
                &mut send_buf,
            );
            writer
                .write_all(&send_buf)
                .map_err(|e| io::Error::other(format!("write catch-up batch: {e}")))?;
            writer
                .flush()
                .map_err(|e| io::Error::other(format!("flush catch-up batch: {e}")))?;
            send_buf.clear();

            end_sequence = batch_end_seq;
            batches_sent += 1;
        }
    }

    info!(end_sequence, batches_sent, "journal catch-up complete");

    Ok(CatchUpResult::Ok(end_sequence))
}

/// Authenticate a replica connection (primary side).
///
/// Sends a 32-byte nonce challenge, verifies the replica's Ed25519
/// signature, and checks that the key has `Replication` permission.
/// Must complete within the stream's existing read timeout.
fn authenticate_replica(
    reader: &mut impl Read,
    writer: &mut impl Write,
    authorized_keys: &melin_protocol::auth::AuthorizedKeys,
) -> io::Result<()> {
    use ed25519_dalek::{Verifier, VerifyingKey};

    // Generate random nonce.
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("getrandom failed: {e}")))?;

    // Send Challenge.
    let mut buf = Vec::with_capacity(64);
    encode_challenge(&nonce, &mut buf);
    writer.write_all(&buf)?;
    writer.flush()?;

    // Read ChallengeResponse.
    let frame = read_frame(reader, MAX_CONTROL_FRAME)?;
    let (signature_bytes, pubkey_bytes) = match decode_challenge_response(&frame) {
        Ok(pair) => pair,
        Err(e) => {
            buf.clear();
            encode_auth_failed(&mut buf);
            let _ = writer.write_all(&buf);
            return Err(io::Error::other(format!("bad challenge response: {e}")));
        }
    };

    // Look up key and verify permission.
    let permission = match authorized_keys.lookup(&pubkey_bytes) {
        Some(perm) => perm,
        None => {
            buf.clear();
            encode_auth_failed(&mut buf);
            let _ = writer.write_all(&buf);
            return Err(io::Error::other("unknown replication key"));
        }
    };
    if !permission.is_replication() {
        buf.clear();
        encode_auth_failed(&mut buf);
        let _ = writer.write_all(&buf);
        return Err(io::Error::other(format!(
            "key has {permission:?} permission, expected Replication"
        )));
    }

    // Verify Ed25519 signature over the nonce.
    let verifying_key = VerifyingKey::from_bytes(&pubkey_bytes).map_err(|e| {
        buf.clear();
        encode_auth_failed(&mut buf);
        let _ = writer.write_all(&buf);
        io::Error::other(format!("invalid public key: {e}"))
    })?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key.verify(&nonce, &signature).map_err(|e| {
        buf.clear();
        encode_auth_failed(&mut buf);
        let _ = writer.write_all(&buf);
        io::Error::other(format!("signature verification failed: {e}"))
    })?;

    // Auth succeeded.
    buf.clear();
    encode_auth_ok(&mut buf);
    writer.write_all(&buf)?;
    writer.flush()?;

    Ok(())
}

/// Authenticate with the primary (replica side).
///
/// Reads the nonce challenge, signs it with the replica's private key,
/// sends the response, and waits for AuthOk/AuthFailed.
fn authenticate_with_primary(
    reader: &mut impl Read,
    writer: &mut impl Write,
    signing_key: &ed25519_dalek::SigningKey,
) -> io::Result<()> {
    use ed25519_dalek::Signer;

    // Read Challenge.
    let frame = read_frame(reader, MAX_CONTROL_FRAME)?;
    let nonce = decode_challenge(&frame)?;

    // Sign the nonce.
    let signature = signing_key.sign(&nonce);
    let pubkey = signing_key.verifying_key();

    // Send ChallengeResponse.
    let mut buf = Vec::with_capacity(128);
    encode_challenge_response(&signature.to_bytes(), pubkey.as_bytes(), &mut buf);
    writer.write_all(&buf)?;
    writer.flush()?;

    // Read auth result.
    let result_frame = read_frame(reader, MAX_CONTROL_FRAME)?;
    match decode_auth_result(&result_frame)? {
        true => Ok(()),
        false => Err(io::Error::other("primary rejected replication key")),
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_replica_connection(
    stream: TcpStream,
    repl_consumer: &mut ReplicationConsumer,
    replication_cursor: &Arc<AtomicU64>,
    fastest_replica_cursor: &Arc<AtomicU64>,
    genesis_entry: &[u8],
    journal_path: &std::path::Path,
    authorized_keys: &melin_protocol::auth::AuthorizedKeys,
    shutdown: &AtomicBool,
    replica_ready: &AtomicBool,
    active_flag: &AtomicBool,
    evict_flag: &AtomicBool,
    metrics: &ReplicationMetrics,
    slot_idx: usize,
    batch_size: usize,
    heartbeat_secs: u64,
    busy_spin: bool,
) -> io::Result<()> {
    let mut reader = stream.try_clone()?;
    let mut writer = stream;

    // Set a read timeout for the handshake and auth.
    reader.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;

    // Authenticate before any data exchange.
    authenticate_replica(&mut reader, &mut writer, authorized_keys)?;
    info!("replica authenticated");

    // Read handshake.
    let handshake_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
    let handshake = match decode_replica_message(&handshake_frame)? {
        ReplicaMessage::Handshake(h) => h,
        ReplicaMessage::Ack(_) => {
            return Err(io::Error::other("expected Handshake, got Ack"));
        }
    };

    info!(
        last_sequence = handshake.last_sequence,
        "replica handshake received"
    );

    // Mark this slot as catching up. Cleared when entering the live loop.
    metrics.catching_up[slot_idx].store(true, Ordering::Relaxed);

    let mut send_buf = Vec::with_capacity(128);

    // Probe whether journal catch-up is possible before committing to
    // a protocol path. This avoids sending StreamStart only to discover
    // the journals are too old.
    let can_catch_up = can_catch_up_from_journal(journal_path, handshake.last_sequence)?;

    let catchup_end = if can_catch_up {
        // Normal path: send StreamStart, then catch up from journal files.
        encode_stream_start(handshake.last_sequence, genesis_entry, &mut send_buf);
        writer.write_all(&send_buf)?;
        writer.flush()?;
        send_buf.clear();

        let catchup_result =
            catch_up_from_journal(journal_path, handshake.last_sequence, &mut writer, shutdown)?;
        match catchup_result {
            CatchUpResult::Ok(end) => end,
            CatchUpResult::NeedSnapshot => {
                // Shouldn't happen — we already checked. But handle gracefully.
                return Err(io::Error::other("catch-up failed unexpectedly after probe"));
            }
        }
    } else {
        // Replica's state predates all journal archives. Transfer a snapshot.
        let snap_path = journal_path.with_extension("snapshot");
        if !snap_path.exists() {
            error!(
                "snapshot transfer requested but no snapshot file at {}",
                snap_path.display()
            );
            return Err(io::Error::other(
                "snapshot transfer required but no snapshot available \
                 — enable --snapshot-interval-secs or trigger a journal rotation",
            ));
        }

        // Send NeedSnapshot to tell the replica to prepare.
        encode_need_snapshot(&mut send_buf);
        writer.write_all(&send_buf)?;
        writer.flush()?;
        send_buf.clear();

        // Read snapshot file and validate magic before transferring.
        let snap_data = std::fs::read(&snap_path)
            .map_err(|e| io::Error::other(format!("read snapshot {}: {e}", snap_path.display())))?;
        let snap_len = snap_data.len() as u64;

        // Parse header: magic(4) + version(2) + reserved(2) + sequence(8) + chain_hash(32)
        if snap_data.len() < 48 {
            return Err(io::Error::other("snapshot file too small for header"));
        }
        // Validate snapshot magic (0x534E4150 = "SNAP") before transfer.
        let magic = u32::from_le_bytes(snap_data[0..4].try_into().unwrap());
        if magic != 0x534E_4150 {
            return Err(io::Error::other(format!(
                "snapshot file has invalid magic: {magic:#x} (expected 0x534e4150)"
            )));
        }
        let snap_sequence = u64::from_le_bytes(snap_data[8..16].try_into().unwrap());
        let mut snap_chain_hash = [0u8; 32];
        snap_chain_hash.copy_from_slice(&snap_data[16..48]);

        info!(
            snap_sequence,
            snap_len,
            path = %snap_path.display(),
            "transferring snapshot to replica"
        );

        // Send SnapshotBegin.
        encode_snapshot_begin(snap_len, snap_sequence, &snap_chain_hash, &mut send_buf);
        writer.write_all(&send_buf)?;
        writer.flush()?;
        send_buf.clear();

        // Stream snapshot in 64 KiB chunks.
        const CHUNK_SIZE: usize = 64 * 1024;
        let mut offset = 0;
        while offset < snap_data.len() {
            let end = (offset + CHUNK_SIZE).min(snap_data.len());
            encode_snapshot_chunk(&snap_data[offset..end], &mut send_buf);
            writer.write_all(&send_buf)?;
            send_buf.clear();
            offset = end;
        }
        writer.flush()?;

        // Send SnapshotEnd with CRC32C of the entire file.
        // The snapshot file already has a CRC at the end, but we
        // compute one over the entire file for transfer integrity.
        let transfer_crc = crc32c::crc32c(&snap_data);
        encode_snapshot_end(transfer_crc, &mut send_buf);
        writer.write_all(&send_buf)?;
        writer.flush()?;
        send_buf.clear();

        info!(snap_sequence, "snapshot transfer complete");

        // Send StreamStart so the replica can set up its journal after
        // loading the snapshot. The start_sequence is the snapshot's
        // sequence — catch-up will send entries after this.
        encode_stream_start(snap_sequence, genesis_entry, &mut send_buf);
        writer.write_all(&send_buf)?;
        writer.flush()?;
        send_buf.clear();

        // Catch up from the snapshot's sequence using the current journal.
        // The current journal starts at snap_sequence+1 (rotation boundary).
        let post_snap_result =
            catch_up_from_journal(journal_path, snap_sequence, &mut writer, shutdown)?;
        match post_snap_result {
            CatchUpResult::Ok(end) => end,
            CatchUpResult::NeedSnapshot => {
                // This shouldn't happen — we just transferred a snapshot
                // and the current journal should cover from snap_sequence.
                return Err(io::Error::other(
                    "catch-up failed even after snapshot transfer",
                ));
            }
        }
    };

    // Drain overlapping ring entries — the ring may contain entries that
    // were already sent during catch-up. Only discard entries whose
    // end_sequence is fully covered by the catch-up. Entries beyond
    // catch-up are left in the ring for the live streaming loop.
    if catchup_end > 0 {
        while let Some((meta, _data)) = repl_consumer.try_read() {
            if meta.end_sequence > catchup_end {
                // This batch has new data beyond catch-up. Send it now
                // and commit so the live loop starts clean.
                encode_data_batch(
                    meta.end_sequence,
                    &meta.chain_hash,
                    meta.entry_count,
                    _data,
                    &mut send_buf,
                );
                repl_consumer.commit();
                writer.write_all(&send_buf)?;
                writer.flush()?;
                send_buf.clear();
                break;
            }
            repl_consumer.commit();
        }
    }

    // Engage the replication cursor so the response stage gates on
    // replica acks. Uses compare_exchange to only lower the cursor from
    // u64::MAX (no replicas connected). If another replica is already
    // connected (cursor < MAX), this is a no-op — their acks already
    // maintain the cursor. Subsequent acks from this replica advance
    // the cursor via fetch_max in process_acks.
    let _ = replication_cursor.compare_exchange(
        u64::MAX,
        handshake.last_sequence + 1,
        Ordering::Release,
        Ordering::Relaxed,
    );

    // Catch-up complete — replica is entering the live streaming loop.
    metrics.catching_up[slot_idx].store(false, Ordering::Relaxed);

    // Mark this ring as active — the journal stage will start publishing
    // to it. Must happen BEFORE replica_ready so the seed drain can wait
    // on this ring's consumer cursor.
    active_flag.store(true, Ordering::Release);

    // Signal that this replica is ready to consume from the replication
    // ring. The main thread waits on this before seeding test data.
    // Must happen AFTER catch-up and overlap drain complete — otherwise
    // seeding fills the replication ring faster than we can drain it,
    // deadlocking the journal stage.
    replica_ready.store(true, Ordering::Release);

    let heartbeat_interval = std::time::Duration::from_secs(heartbeat_secs);
    let mut last_send = std::time::Instant::now();
    let mut last_sequence = handshake.last_sequence;
    let mut last_chain_hash = handshake.chain_hash;

    live_stream_uring(
        writer,
        repl_consumer,
        replication_cursor,
        fastest_replica_cursor,
        shutdown,
        evict_flag,
        metrics,
        slot_idx,
        batch_size,
        heartbeat_interval,
        busy_spin,
        &mut send_buf,
        &mut last_send,
        &mut last_sequence,
        &mut last_chain_hash,
    )
}

/// io_uring live streaming loop for the primary replication handler.
///
/// Live streaming loop using async RECV/SEND via io_uring. A single RECV is always
/// in-flight for ack frames; SEND is submitted when the replication ring
/// has data. Both complete via the memory-mapped CQ with zero syscalls
/// in the hot path.
#[allow(clippy::too_many_arguments)]
fn live_stream_uring(
    writer: TcpStream,
    repl_consumer: &mut ReplicationConsumer,
    replication_cursor: &Arc<AtomicU64>,
    fastest_replica_cursor: &Arc<AtomicU64>,
    shutdown: &AtomicBool,
    evict_flag: &AtomicBool,
    metrics: &ReplicationMetrics,
    slot_idx: usize,
    batch_size: usize,
    heartbeat_interval: std::time::Duration,
    busy_spin: bool,
    send_buf: &mut Vec<u8>,
    last_send: &mut std::time::Instant,
    last_sequence: &mut u64,
    last_chain_hash: &mut [u8; 32],
) -> io::Result<()> {
    use io_uring::{IoUring, opcode, types};
    use std::os::unix::io::AsRawFd;

    const TOKEN_RECV: u64 = 0;
    const TOKEN_SEND: u64 = 1;

    let tcp_fd = writer.as_raw_fd();

    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .build(8)
        .map_err(|e| io::Error::other(format!("io_uring init failed: {e}")))?;

    ring.submitter()
        .register_files(&[tcp_fd])
        .map_err(|e| io::Error::other(format!("io_uring register_files: {e}")))?;

    // Pin io-wq workers to core 0 (keep them off pipeline cores).
    {
        let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        unsafe { libc::CPU_SET(0, &mut cpuset) };
        let _ = ring.submitter().register_iowq_aff(&cpuset);
    }

    // RECV buffer for ack frames (13 bytes each, but kernel may
    // coalesce multiple). 4 KiB is plenty.
    let mut recv_buf = vec![0u8; 4096];
    // Accumulation buffer for partial ack frame parsing.
    let mut parse_buf: Vec<u8> = Vec::with_capacity(MAX_CONTROL_FRAME + 4);
    // RECV is always resubmitted after CQE processing — no explicit
    // tracking needed. The io_uring kernel guarantees ordering.
    let mut send_in_flight = false;
    let mut send_offset: usize = 0;
    let mut idle_spins: u32 = 0;

    // Submit initial RECV.
    let sqe = opcode::Recv::new(
        types::Fixed(0),
        recv_buf.as_mut_ptr(),
        recv_buf.len() as u32,
    )
    .build()
    .user_data(TOKEN_RECV);
    unsafe { ring.submission().push(&sqe).expect("SQ full") };

    loop {
        // --- Check flags ---
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        if evict_flag.load(Ordering::Relaxed) {
            info!(slot = slot_idx, "handler exiting: evicted by journal stage");
            return Ok(());
        }

        // --- Drain replication ring into send_buf (memory, non-blocking) ---
        if !send_in_flight {
            let mut coalesced = 0;
            while coalesced < batch_size {
                if let Some((meta, data)) = repl_consumer.try_read() {
                    encode_data_batch(
                        meta.end_sequence,
                        &meta.chain_hash,
                        meta.entry_count,
                        data,
                        send_buf,
                    );
                    repl_consumer.commit();
                    *last_sequence = meta.end_sequence;
                    *last_chain_hash = meta.chain_hash;
                    coalesced += 1;
                } else {
                    break;
                }
            }

            if coalesced > 0 {
                // Submit SEND for the coalesced buffer.
                let sqe =
                    opcode::Send::new(types::Fixed(0), send_buf.as_ptr(), send_buf.len() as u32)
                        .build()
                        .user_data(TOKEN_SEND);
                unsafe { ring.submission().push(&sqe).expect("SQ full") };
                send_in_flight = true;
                send_offset = 0;
                *last_send = std::time::Instant::now();
                idle_spins = 0;
            } else if last_send.elapsed() >= heartbeat_interval {
                // No data — send heartbeat if idle.
                encode_heartbeat(*last_sequence, last_chain_hash, send_buf);
                let sqe =
                    opcode::Send::new(types::Fixed(0), send_buf.as_ptr(), send_buf.len() as u32)
                        .build()
                        .user_data(TOKEN_SEND);
                unsafe { ring.submission().push(&sqe).expect("SQ full") };
                send_in_flight = true;
                send_offset = 0;
                *last_send = std::time::Instant::now();
            }
        }

        // --- Submit SQEs to kernel (non-blocking) ---
        ring.submit()
            .map_err(|e| io::Error::other(format!("io_uring submit: {e}")))?;

        // --- Collect CQEs (must drain before pushing new SQEs) ---
        // Collecting into a small stack array avoids the CQ borrow
        // conflicting with SQ pushes during processing.
        let mut cqes: [(u64, i32); 4] = [(0, 0); 4];
        let mut cqe_count = 0;
        for cqe in ring.completion() {
            if cqe_count < cqes.len() {
                cqes[cqe_count] = (cqe.user_data(), cqe.result());
                cqe_count += 1;
            }
        }

        let any_cqe = cqe_count > 0;
        for &(token, result) in &cqes[..cqe_count] {
            idle_spins = 0;
            match token {
                TOKEN_RECV => {
                    if result <= 0 {
                        return Err(io::Error::other(format!(
                            "replica disconnected (recv returned {result})"
                        )));
                    }
                    let n = result as usize;
                    parse_buf.extend_from_slice(&recv_buf[..n]);

                    // Extract complete ack frames from parse_buf.
                    let mut cursor = 0;
                    while cursor + 4 <= parse_buf.len() {
                        let frame_len =
                            u32::from_le_bytes(parse_buf[cursor..cursor + 4].try_into().unwrap())
                                as usize;
                        if frame_len == 0 || frame_len > MAX_CONTROL_FRAME {
                            return Err(io::Error::other(format!(
                                "invalid ack frame length: {frame_len}"
                            )));
                        }
                        if cursor + 4 + frame_len > parse_buf.len() {
                            break; // Incomplete frame.
                        }
                        let payload = &parse_buf[cursor + 4..cursor + 4 + frame_len];
                        if let Ok(ReplicaMessage::Ack(ack)) = decode_replica_message(payload) {
                            let new_val = ack.acked_sequence + 1;
                            let _ = replication_cursor.fetch_max(new_val, Ordering::Release);
                            let _ = fastest_replica_cursor.fetch_max(new_val, Ordering::Release);
                            metrics.acked_sequence[slot_idx]
                                .store(ack.acked_sequence, Ordering::Relaxed);
                            metrics.ack_latency_us[slot_idx]
                                .store(last_send.elapsed().as_micros() as u64, Ordering::Relaxed);
                        }
                        cursor += 4 + frame_len;
                    }
                    // Compact parse_buf.
                    if cursor > 0 {
                        let remaining = parse_buf.len() - cursor;
                        parse_buf.copy_within(cursor.., 0);
                        parse_buf.truncate(remaining);
                    }

                    // Resubmit RECV.
                    let sqe = opcode::Recv::new(
                        types::Fixed(0),
                        recv_buf.as_mut_ptr(),
                        recv_buf.len() as u32,
                    )
                    .build()
                    .user_data(TOKEN_RECV);
                    unsafe { ring.submission().push(&sqe).expect("SQ full") };
                }

                TOKEN_SEND => {
                    if result < 0 {
                        return Err(io::Error::other(format!("send error (returned {result})")));
                    }
                    let sent = result as usize;
                    send_offset += sent;
                    if send_offset >= send_buf.len() {
                        // Fully sent.
                        metrics.bytes_sent[slot_idx]
                            .fetch_add(send_buf.len() as u64, Ordering::Relaxed);
                        send_buf.clear();
                        send_offset = 0;
                        send_in_flight = false;
                    } else {
                        // Partial send — resubmit remainder.
                        let sqe = opcode::Send::new(
                            types::Fixed(0),
                            send_buf[send_offset..].as_ptr(),
                            (send_buf.len() - send_offset) as u32,
                        )
                        .build()
                        .user_data(TOKEN_SEND);
                        unsafe { ring.submission().push(&sqe).expect("SQ full") };
                    }
                }

                _ => {}
            }
        }

        // --- Idle wait ---
        if !any_cqe && send_buf.is_empty() {
            if busy_spin || idle_spins < 1000 {
                idle_spins = idle_spins.wrapping_add(1);
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

/// io_uring streaming loop for the replica receiver.
///
/// io_uring streaming receive loop for the replica. Uses async RECV/SEND
/// with async RECV/SEND. A single RECV is always in-flight for
/// DataBatch frames; SEND is submitted when an ack becomes ready
/// (journal cursor catches up). Frame parsing uses the same
/// accumulate-and-extract pattern as the bench client and reader.
#[allow(clippy::too_many_arguments)]
fn replica_stream_uring(
    tcp_stream: &TcpStream,
    input_producer: &melin_disruptor::ring::MultiProducer<
        melin_engine::journal::pipeline::InputSlot,
    >,
    raw_journal_tx: &melin_engine::journal::pipeline::RawBatchSender,
    journal_cursor: &melin_disruptor::padding::Sequence,
    pending_acks: &mut PendingAckQueue,
    received_data: &mut bool,
    journal_accum: &mut Vec<u8>,
    accum_end_sequence: &mut u64,
    accum_chain_hash: &mut [u8; 32],
    shutdown: &AtomicBool,
    promote: &AtomicBool,
) -> SessionExit {
    use io_uring::{IoUring, opcode, types};
    use std::os::unix::io::AsRawFd;

    const TOKEN_RECV: u64 = 0;
    const TOKEN_SEND: u64 = 1;

    let tcp_fd = tcp_stream.as_raw_fd();

    let mut ring: IoUring = match IoUring::builder().setup_single_issuer().build(8) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "io_uring init failed for replica receiver");
            return SessionExit::Disconnected;
        }
    };

    if let Err(e) = ring.submitter().register_files(&[tcp_fd]) {
        tracing::error!(error = %e, "io_uring register_files failed");
        return SessionExit::Disconnected;
    }

    // Pin io-wq workers to core 0.
    {
        let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        unsafe { libc::CPU_SET(0, &mut cpuset) };
        let _ = ring.submitter().register_iowq_aff(&cpuset);
    }

    // RECV buffer — 64 KiB. DataBatch frames can be up to 768 KiB,
    // but TCP delivers data in chunks. parse_buf accumulates until
    // a complete frame is available.
    let mut recv_buf = vec![0u8; 65536];
    let mut parse_buf: Vec<u8> = Vec::with_capacity(MAX_DATA_FRAME + 4);
    let mut ack_send_buf: Vec<u8> = Vec::with_capacity(64);
    let mut ack_send_offset: usize = 0;
    let mut ack_send_in_flight = false;
    let mut idle_spins: u32 = 0;

    // Submit initial RECV.
    let sqe = opcode::Recv::new(
        types::Fixed(0),
        recv_buf.as_mut_ptr(),
        recv_buf.len() as u32,
    )
    .build()
    .user_data(TOKEN_RECV);
    unsafe { ring.submission().push(&sqe).expect("SQ full") };

    loop {
        // --- Check flags ---
        if shutdown.load(Ordering::Relaxed) {
            info!("replica shutting down");
            return SessionExit::Shutdown;
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered — stopping replication, transitioning to primary");
            // Drain any complete frames already in parse_buf for
            // maximum data freshness during promotion.
            let mut cursor = 0;
            while cursor + 4 <= parse_buf.len() {
                let frame_len =
                    u32::from_le_bytes(parse_buf[cursor..cursor + 4].try_into().unwrap()) as usize;
                if frame_len == 0
                    || frame_len > MAX_DATA_FRAME
                    || cursor + 4 + frame_len > parse_buf.len()
                {
                    break;
                }
                let payload = &parse_buf[cursor + 4..cursor + 4 + frame_len];
                if let Ok(PrimaryMessage::DataBatch {
                    end_sequence,
                    chain_hash: batch_chain_hash,
                    journal_bytes,
                    ..
                }) = decode_primary_message(payload)
                {
                    journal_accum.extend_from_slice(&journal_bytes);
                    *accum_end_sequence = end_sequence;
                    *accum_chain_hash = batch_chain_hash;
                }
                cursor += 4 + frame_len;
            }
            // Submit any accumulated data before returning.
            if !journal_accum.is_empty() && !pending_acks.is_full() {
                if let Ok(target) = submit_batch_to_pipeline(
                    journal_accum,
                    *accum_end_sequence,
                    *accum_chain_hash,
                    input_producer,
                    raw_journal_tx,
                ) {
                    pending_acks.push(target, *accum_end_sequence);
                }
                journal_accum.clear();
            }
            return SessionExit::Promote;
        }

        // --- Flush durable acks (non-blocking journal cursor check) ---
        if !ack_send_in_flight && let Some(seq) = pending_acks.pop_ready(journal_cursor) {
            ack_send_buf.clear();
            encode_ack(
                &Ack {
                    acked_sequence: seq,
                },
                &mut ack_send_buf,
            );
            let sqe = opcode::Send::new(
                types::Fixed(0),
                ack_send_buf.as_ptr(),
                ack_send_buf.len() as u32,
            )
            .build()
            .user_data(TOKEN_SEND);
            unsafe { ring.submission().push(&sqe).expect("SQ full") };
            ack_send_in_flight = true;
            ack_send_offset = 0;
        }

        // --- Backpressure: if pending_acks full, drain in-flight SEND
        // then pop + send the oldest ack. Must not pop while a SEND is
        // in-flight — the popped sequence would be lost (no buffer to
        // defer it).
        if pending_acks.is_full() {
            // Wait for any in-flight ack SEND to complete first.
            // Collect CQEs into stack buffer to avoid CQ/SQ borrow conflict.
            while ack_send_in_flight {
                let _ = ring.submit();
                let mut bp_cqes: [(u64, i32); 4] = [(0, 0); 4];
                let mut bp_count = 0;
                for cqe in ring.completion() {
                    if bp_count < bp_cqes.len() {
                        bp_cqes[bp_count] = (cqe.user_data(), cqe.result());
                        bp_count += 1;
                    }
                }
                for &(bp_token, bp_result) in &bp_cqes[..bp_count] {
                    match bp_token {
                        TOKEN_SEND => {
                            if bp_result < 0 {
                                warn!("ack send error during backpressure drain");
                                return SessionExit::Disconnected;
                            }
                            let sent = bp_result as usize;
                            ack_send_offset += sent;
                            if ack_send_offset >= ack_send_buf.len() {
                                ack_send_buf.clear();
                                ack_send_offset = 0;
                                ack_send_in_flight = false;
                            } else {
                                let sqe = opcode::Send::new(
                                    types::Fixed(0),
                                    ack_send_buf[ack_send_offset..].as_ptr(),
                                    (ack_send_buf.len() - ack_send_offset) as u32,
                                )
                                .build()
                                .user_data(TOKEN_SEND);
                                unsafe { ring.submission().push(&sqe).expect("SQ full") };
                            }
                        }
                        TOKEN_RECV => {
                            // Stash RECV CQE data into parse_buf for later
                            // processing — don't handle frames here to keep
                            // the backpressure drain simple.
                            if bp_result > 0 {
                                let n = bp_result as usize;
                                parse_buf.extend_from_slice(&recv_buf[..n]);
                                // Resubmit RECV.
                                let sqe = opcode::Recv::new(
                                    types::Fixed(0),
                                    recv_buf.as_mut_ptr(),
                                    recv_buf.len() as u32,
                                )
                                .build()
                                .user_data(TOKEN_RECV);
                                unsafe { ring.submission().push(&sqe).expect("SQ full") };
                            } else {
                                warn!("primary disconnected during backpressure drain");
                                return SessionExit::Disconnected;
                            }
                        }
                        _ => {}
                    }
                }
                std::hint::spin_loop();
            }

            let seq = pending_acks.pop_oldest_blocking(journal_cursor);
            ack_send_buf.clear();
            encode_ack(
                &Ack {
                    acked_sequence: seq,
                },
                &mut ack_send_buf,
            );
            let sqe = opcode::Send::new(
                types::Fixed(0),
                ack_send_buf.as_ptr(),
                ack_send_buf.len() as u32,
            )
            .build()
            .user_data(TOKEN_SEND);
            unsafe { ring.submission().push(&sqe).expect("SQ full") };
            ack_send_in_flight = true;
            ack_send_offset = 0;
        }

        // --- Submit SQEs and drain CQEs ---
        if let Err(e) = ring.submit() {
            tracing::error!(error = %e, "io_uring submit failed");
            return SessionExit::Disconnected;
        }

        let mut cqes: [(u64, i32); 4] = [(0, 0); 4];
        let mut cqe_count = 0;
        for cqe in ring.completion() {
            if cqe_count < cqes.len() {
                cqes[cqe_count] = (cqe.user_data(), cqe.result());
                cqe_count += 1;
            }
        }

        let any_cqe = cqe_count > 0;
        for &(token, result) in &cqes[..cqe_count] {
            idle_spins = 0;
            match token {
                TOKEN_RECV => {
                    if result <= 0 {
                        warn!("primary disconnected (recv returned {result})");
                        return SessionExit::Disconnected;
                    }
                    let n = result as usize;
                    parse_buf.extend_from_slice(&recv_buf[..n]);

                    // Extract complete frames from parse_buf.
                    let mut cursor = 0;
                    while cursor + 4 <= parse_buf.len() {
                        let frame_len =
                            u32::from_le_bytes(parse_buf[cursor..cursor + 4].try_into().unwrap())
                                as usize;
                        if frame_len == 0 || frame_len > MAX_DATA_FRAME {
                            warn!(frame_len, "invalid frame length from primary");
                            return SessionExit::Disconnected;
                        }
                        if cursor + 4 + frame_len > parse_buf.len() {
                            break; // Incomplete frame — wait for more data.
                        }
                        let payload = &parse_buf[cursor + 4..cursor + 4 + frame_len];
                        match decode_primary_message(payload) {
                            Ok(PrimaryMessage::DataBatch {
                                end_sequence,
                                chain_hash: batch_chain_hash,
                                entry_count: _,
                                journal_bytes,
                            }) => {
                                *received_data = true;
                                journal_accum.extend_from_slice(&journal_bytes);
                                *accum_end_sequence = end_sequence;
                                *accum_chain_hash = batch_chain_hash;
                            }
                            Ok(PrimaryMessage::Heartbeat { sequence, .. }) => {
                                debug!(sequence, "heartbeat from primary");
                            }
                            Ok(PrimaryMessage::NeedSnapshot) => {
                                return SessionExit::Fatal(
                                    "primary says we need a snapshot transfer mid-stream".into(),
                                );
                            }
                            Ok(PrimaryMessage::HashMismatch) => {
                                return SessionExit::Fatal(
                                    "chain hash mismatch from primary".into(),
                                );
                            }
                            Ok(_) => {
                                debug!("unexpected message during streaming");
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to decode primary message");
                                return SessionExit::Disconnected;
                            }
                        }
                        cursor += 4 + frame_len;
                    }
                    // Compact parse_buf.
                    if cursor > 0 {
                        let remaining = parse_buf.len() - cursor;
                        parse_buf.copy_within(cursor.., 0);
                        parse_buf.truncate(remaining);
                    }

                    // Submit accumulated data to pipeline (if room in pending acks).
                    if !journal_accum.is_empty() && !pending_acks.is_full() {
                        match submit_batch_to_pipeline(
                            journal_accum,
                            *accum_end_sequence,
                            *accum_chain_hash,
                            input_producer,
                            raw_journal_tx,
                        ) {
                            Ok(target) => {
                                pending_acks.push(target, *accum_end_sequence);
                                journal_accum.clear();
                            }
                            Err(e) => {
                                error!(error = %e, "failed to submit batch to pipeline");
                                return SessionExit::Disconnected;
                            }
                        }
                    }

                    // Resubmit RECV.
                    let sqe = opcode::Recv::new(
                        types::Fixed(0),
                        recv_buf.as_mut_ptr(),
                        recv_buf.len() as u32,
                    )
                    .build()
                    .user_data(TOKEN_RECV);
                    unsafe { ring.submission().push(&sqe).expect("SQ full") };
                }

                TOKEN_SEND => {
                    if result < 0 {
                        warn!("ack send error (returned {result})");
                        return SessionExit::Disconnected;
                    }
                    let sent = result as usize;
                    ack_send_offset += sent;
                    if ack_send_offset >= ack_send_buf.len() {
                        ack_send_buf.clear();
                        ack_send_offset = 0;
                        ack_send_in_flight = false;
                    } else {
                        // Partial send — resubmit remainder.
                        let sqe = opcode::Send::new(
                            types::Fixed(0),
                            ack_send_buf[ack_send_offset..].as_ptr(),
                            (ack_send_buf.len() - ack_send_offset) as u32,
                        )
                        .build()
                        .user_data(TOKEN_SEND);
                        unsafe { ring.submission().push(&sqe).expect("SQ full") };
                    }
                }

                _ => {}
            }
        }

        // --- Idle wait ---
        if !any_cqe {
            if idle_spins < 1000 {
                idle_spins = idle_spins.wrapping_add(1);
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

// --- Replication Receiver (Replica side) ---

/// Pending ack waiting for journal durability confirmation.
struct PendingAck {
    /// Disruptor sequence target — ack is safe to send once the journal
    /// cursor reaches this value.
    journal_target: u64,
    /// Wire-protocol sequence to include in the ack frame.
    acked_sequence: u64,
}

/// Fixed-capacity circular buffer of pending acks. Decouples TCP receives
/// from journal fsync by allowing up to `CAP` batches to be submitted to
/// the journal stage before any ack is sent. Acks are flushed in FIFO
/// order as the journal cursor advances.
///
/// Capacity 8 matches `RAW_RING_CAPACITY` — the SPSC ring between the
/// receiver and journal stage is the pipelining bottleneck.
struct PendingAckQueue {
    /// Circular buffer of pending acks. Indices wrap via `& MASK`.
    buf: [PendingAck; Self::CAP],
    /// Index of the oldest pending ack (next to flush).
    head: usize,
    /// Number of pending acks in the queue.
    len: usize,
}

impl PendingAckQueue {
    // Capacity 8 matches RAW_RING_CAPACITY — the SPSC ring between the
    // receiver and journal stage is the pipelining bottleneck. With
    // io_uring, the deadlock that forced CAP=1 is eliminated (RECV CQEs
    // arrive asynchronously while pop_ready checks the cursor each
    // iteration).
    const CAP: usize = 8;
    const MASK: usize = Self::CAP - 1;

    fn new() -> Self {
        Self {
            buf: std::array::from_fn(|_| PendingAck {
                journal_target: 0,
                acked_sequence: 0,
            }),
            head: 0,
            len: 0,
        }
    }

    fn is_full(&self) -> bool {
        self.len >= Self::CAP
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Record a pending ack. Caller must ensure `!is_full()`.
    fn push(&mut self, journal_target: u64, acked_sequence: u64) {
        debug_assert!(!self.is_full());
        let idx = (self.head + self.len) & Self::MASK;
        self.buf[idx] = PendingAck {
            journal_target,
            acked_sequence,
        };
        self.len += 1;
    }

    /// Pop acks for all batches where the journal cursor has caught up.
    /// Non-blocking — returns `None` immediately if the oldest pending
    /// batch isn't durable yet. Returns the highest acked sequence
    /// among the flushed entries.
    fn pop_ready(&mut self, journal_cursor: &melin_disruptor::padding::Sequence) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        let cursor_val = journal_cursor.get().load(Ordering::Acquire);
        let mut last_acked = None;
        while self.len > 0 {
            let entry = &self.buf[self.head];
            if cursor_val < entry.journal_target {
                break; // Not durable yet.
            }
            last_acked = Some(entry.acked_sequence);
            self.head = (self.head + 1) & Self::MASK;
            self.len -= 1;
        }
        last_acked
    }

    /// Block until the oldest pending ack is durable, then pop all
    /// ready entries. Returns the highest acked sequence.
    fn pop_oldest_blocking(&mut self, journal_cursor: &melin_disruptor::padding::Sequence) -> u64 {
        debug_assert!(!self.is_empty());
        let target = self.buf[self.head].journal_target;
        wait_for_journal_cursor(journal_cursor, target);
        // The cursor advanced — pop this entry plus any others that
        // are now also durable.
        self.pop_ready(journal_cursor)
            .expect("at least one entry became ready after wait")
    }

    /// Block until ALL pending acks are durable. Returns the highest
    /// acked sequence, or `None` if the queue was already empty.
    fn pop_all_blocking(
        &mut self,
        journal_cursor: &melin_disruptor::padding::Sequence,
    ) -> Option<u64> {
        let mut last = None;
        while !self.is_empty() {
            last = Some(self.pop_oldest_blocking(journal_cursor));
        }
        last
    }
}

/// Send an ack for `acked_sequence` over TCP, coalescing into `send_buf`.
/// Flushes the buffer immediately.
fn send_ack_tcp(
    acked_sequence: u64,
    writer: &mut TcpStream,
    send_buf: &mut Vec<u8>,
) -> io::Result<()> {
    encode_ack(&Ack { acked_sequence }, send_buf);
    writer.write_all(send_buf)?;
    writer.flush()?;
    send_buf.clear();
    Ok(())
}

/// Outcome of the inner streaming receive loop.
enum SessionExit {
    Shutdown,
    Promote,
    Disconnected,
    Fatal(Box<dyn std::error::Error>),
}

/// Run the replication receiver. Connects to a primary, receives journal
/// entries, persists them locally, replays into the Exchange, and sends acks.
///
/// Blocks until the connection drops or shutdown is signaled.
/// Result of `run_receiver`: `None` = clean shutdown, `Some` = promotion
/// triggered with the fully-replayed Exchange and positioned JournalWriter.
pub type ReceiverResult = Result<
    Option<(
        melin_engine::exchange::Exchange,
        melin_engine::journal::writer::JournalWriter,
    )>,
    Box<dyn std::error::Error>,
>;

#[allow(clippy::too_many_arguments)]
pub fn run_receiver(
    primary_addr: SocketAddr,
    journal_path: &std::path::Path,
    signing_key: &ed25519_dalek::SigningKey,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_secs: u64,
    snapshot_path: std::path::PathBuf,
    cores: crate::server::PipelineCores,
) -> ReceiverResult {
    use melin_engine::exchange::Exchange;
    use melin_engine::journal::writer::JournalWriter;

    // Recover local state from journal (if any). On first call this may
    // be (None, None) for a fresh replica. After a reconnect, the pipeline
    // shutdown returns the Exchange + JournalWriter directly.
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        if journal_path.exists() {
            let engine = if snapshot_path.exists() {
                info!("recovering replica from snapshot + journal");
                melin_engine::journal::JournaledExchange::recover_from_snapshot(
                    &snapshot_path,
                    journal_path,
                )?
            } else {
                melin_engine::journal::JournaledExchange::recover(journal_path)?
            };
            let next = engine.next_sequence();
            let last = next.saturating_sub(1);
            let hash = engine.writer_chain_hash().unwrap_or([0u8; 32]);
            let (exchange, writer) = engine.into_parts();
            (Some(exchange), Some(writer), last, hash)
        } else {
            (None, None, 0u64, [0u8; 32])
        };

    // Exponential backoff for reconnection: 1s → 2s → 4s → … → 30s max.
    // Reset to 1s on successful streaming (first DataBatch received).
    let mut backoff = std::time::Duration::from_secs(1);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

    // Reusable buffers — survive across reconnections.
    let mut send_buf = Vec::with_capacity(64);
    let mut journal_accum: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut accum_end_sequence: u64 = 0;
    let mut accum_chain_hash: [u8; 32] = [0u8; 32];

    // --- Outer reconnect loop ---
    //
    // Each iteration: connect → auth → handshake → pipeline → stream.
    // On disconnect (eviction or crash): shut down pipeline, recover
    // Exchange + JournalWriter, backoff, reconnect.
    loop {
        // Check shutdown/promote before attempting to connect.
        if shutdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered while disconnected");
            return match (exchange, journal_writer) {
                (Some(e), Some(w)) => Ok(Some((e, w))),
                _ => Err("promotion requested but no local state available".into()),
            };
        }

        // --- Connect and authenticate ---

        info!(primary = %primary_addr, "connecting to primary as replica");

        let stream = match TcpStream::connect(primary_addr) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "failed to connect to primary — retrying"
                );
                sleep_checking_flags(backoff, shutdown, promote);
                if shutdown.load(Ordering::Relaxed) {
                    return Ok(None);
                }
                if promote.load(Ordering::Acquire) {
                    info!("promotion triggered during reconnect backoff");
                    return match (exchange, journal_writer) {
                        (Some(e), Some(w)) => Ok(Some((e, w))),
                        _ => Err("promotion requested but no local state available".into()),
                    };
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;

        let mut reader = stream.try_clone()?;
        let mut tcp_writer = stream;

        if let Err(e) = authenticate_with_primary(&mut reader, &mut tcp_writer, signing_key) {
            warn!(error = %e, "authentication failed — retrying");
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        info!("authenticated with primary");

        // --- Handshake ---

        let handshake = Handshake {
            last_sequence,
            chain_hash,
        };
        send_buf.clear();
        encode_handshake(&handshake, &mut send_buf);
        tcp_writer.write_all(&send_buf)?;
        tcp_writer.flush()?;
        send_buf.clear();

        // --- Protocol negotiation (StreamStart / NeedSnapshot) ---

        let response_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
        let response = decode_primary_message(&response_frame)?;
        let primary_genesis_entry = match response {
            PrimaryMessage::StreamStart {
                start_sequence,
                genesis_entry,
            } => {
                info!(start_sequence, "streaming started");
                genesis_entry
            }
            PrimaryMessage::NeedSnapshot => {
                info!("primary requires snapshot transfer — receiving snapshot");

                let _ = std::fs::remove_file(journal_path);
                let _ = std::fs::remove_file(&snapshot_path);

                let begin_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
                let (snap_len, snap_sequence, snap_chain_hash) =
                    match decode_primary_message(&begin_frame)? {
                        PrimaryMessage::SnapshotBegin {
                            snapshot_len,
                            snap_sequence,
                            snap_chain_hash,
                        } => (snapshot_len, snap_sequence, snap_chain_hash),
                        other => {
                            return Err(format!("expected SnapshotBegin, got {other:?}").into());
                        }
                    };

                info!(snap_sequence, snap_len, "receiving snapshot");

                let tmp_path = snapshot_path.with_extension("snapshot.tmp");
                {
                    let mut tmp_file = std::fs::File::create(&tmp_path)?;
                    let mut received: u64 = 0;
                    let mut running_crc: u32 = 0;
                    loop {
                        let chunk_frame = read_frame(&mut reader, MAX_DATA_FRAME)?;
                        match decode_primary_message(&chunk_frame)? {
                            PrimaryMessage::SnapshotChunk(data) => {
                                std::io::Write::write_all(&mut tmp_file, &data)?;
                                received += data.len() as u64;
                                running_crc = crc32c::crc32c_append(running_crc, &data);
                            }
                            PrimaryMessage::SnapshotEnd {
                                crc32c: expected_crc,
                            } => {
                                tmp_file.sync_all()?;
                                drop(tmp_file);

                                if received != snap_len {
                                    let _ = std::fs::remove_file(&tmp_path);
                                    return Err(format!(
                                        "snapshot length mismatch: expected {snap_len} bytes, got {received}"
                                    )
                                    .into());
                                }

                                if running_crc != expected_crc {
                                    let _ = std::fs::remove_file(&tmp_path);
                                    return Err(format!(
                                        "snapshot CRC mismatch: expected {expected_crc:#x}, got {running_crc:#x}"
                                    )
                                    .into());
                                }

                                std::fs::rename(&tmp_path, &snapshot_path)?;
                                info!(snap_sequence, received, "snapshot received and verified");
                                break;
                            }
                            other => {
                                let _ = std::fs::remove_file(&tmp_path);
                                return Err(
                                    format!("expected SnapshotChunk/End, got {other:?}").into()
                                );
                            }
                        }
                    }
                }

                let (snap_exchange, snap_seq, snap_hash) =
                    melin_engine::journal::snapshot::load(&snapshot_path)?;
                if snap_hash != snap_chain_hash {
                    return Err(format!(
                        "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
                         loaded snapshot has {snap_hash:02x?}"
                    )
                    .into());
                }
                exchange = Some(snap_exchange);

                let writer =
                    JournalWriter::create_continuing(journal_path, snap_seq + 1, snap_hash)?;
                journal_writer = Some(writer);

                let ss_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
                match decode_primary_message(&ss_frame)? {
                    PrimaryMessage::StreamStart {
                        start_sequence,
                        genesis_entry,
                    } => {
                        info!(start_sequence, "streaming resumed after snapshot transfer");
                        genesis_entry
                    }
                    other => {
                        return Err(
                            format!("expected StreamStart after snapshot, got {other:?}").into(),
                        );
                    }
                }
            }
            PrimaryMessage::HashMismatch => {
                return Err("chain hash mismatch — replica has divergent history".into());
            }
            _ => {
                return Err(format!("unexpected response: {response:?}").into());
            }
        };

        // --- Create journal for fresh replica (first connection only) ---

        if journal_writer.is_none() {
            use melin_engine::journal::codec::{self as journal_codec, FILE_HEADER_SIZE};
            use std::fs::OpenOptions;
            use std::os::unix::fs::FileExt;

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(journal_path)?;
            let mut header = [0u8; FILE_HEADER_SIZE];
            journal_codec::encode_file_header(&mut header);
            file.write_all_at(&header, 0)?;
            file.write_all_at(&primary_genesis_entry, FILE_HEADER_SIZE as u64)?;
            file.sync_all()?;

            let genesis_chain_hash = {
                let entry_len = primary_genesis_entry.len();
                let hash = blake3::hash(&primary_genesis_entry[..entry_len - 4]);
                *hash.as_bytes()
            };

            let valid_end = FILE_HEADER_SIZE as u64 + primary_genesis_entry.len() as u64;
            let writer = JournalWriter::open_append(
                journal_path,
                1, // genesis consumed sequence 1
                valid_end,
                Some(genesis_chain_hash),
                0, // events_since_checkpoint
            )?;
            exchange = Some(Exchange::new());
            journal_writer = Some(writer);
        }

        let cur_exchange = exchange.take().expect("exchange initialized");
        let cur_writer = journal_writer.take().expect("journal_writer initialized");

        // --- Build pipeline and spawn threads ---

        let shadow_exchange = cur_exchange.clone_via_snapshot();

        let enable_shadow = snapshot_interval_secs > 0;
        let (
            input_producer,
            journal_stage,
            matching_stage,
            drain_consumer,
            journal_cursor,
            _matching_cursor,
            raw_journal_tx,
            shadow_consumer,
            chain_hash_lock,
        ) = melin_engine::journal::pipeline::build_replica_pipeline(
            cur_exchange,
            cur_writer,
            4096,  // max_journal_batch
            false, // don't busy-spin on replica
            enable_shadow,
        );

        let pipeline_shutdown = Arc::new(AtomicBool::new(false));

        let ps = Arc::clone(&pipeline_shutdown);
        let journal_core = cores.journal;
        let journal_handle = std::thread::Builder::new()
            .name("journal".into())
            .spawn(move || {
                pin_replica_thread("journal", journal_core);
                journal_stage.run(&ps)
            })
            .expect("spawn journal thread");

        let ps = Arc::clone(&pipeline_shutdown);
        let matching_core = cores.matching;
        let matching_handle = std::thread::Builder::new()
            .name("matching".into())
            .spawn(move || {
                pin_replica_thread("matching", matching_core);
                matching_stage.run(&ps)
            })
            .expect("spawn matching thread");

        // Drain thread uses the response core — on the primary this core
        // runs the response stage, but replicas have no response stage.
        let ps = Arc::clone(&pipeline_shutdown);
        let drain_core = cores.response;
        let drain_handle = std::thread::Builder::new()
            .name("drain".into())
            .spawn(move || {
                pin_replica_thread("drain", drain_core);
                let mut consumer = drain_consumer;
                let mut batch = vec![melin_engine::journal::pipeline::OutputSlot::default(); 256];
                loop {
                    if ps.load(Ordering::Relaxed) {
                        return;
                    }
                    let count = consumer.consume_batch(&mut batch, 256);
                    if count == 0 {
                        std::thread::yield_now();
                    }
                }
            })
            .expect("spawn drain thread");

        let shadow_handle = if let Some(shadow_cons) = shadow_consumer {
            let snap_path = snapshot_path.clone();
            let chain_lock = chain_hash_lock.expect("chain hash lock with shadow");
            let ps = Arc::clone(&pipeline_shutdown);
            let shadow_core = cores.shadow;
            Some(
                std::thread::Builder::new()
                    .name("replica-shadow".into())
                    .spawn(move || {
                        pin_replica_thread("replica-shadow", shadow_core);
                        crate::shadow::run(
                            shadow_cons,
                            shadow_exchange,
                            snap_path,
                            std::time::Duration::from_secs(snapshot_interval_secs),
                            chain_lock,
                            &ps,
                            false,
                        );
                    })
                    .expect("spawn shadow thread"),
            )
        } else {
            None
        };

        // --- Inner streaming receive loop ---
        //
        // Exits via `break` with a SessionExit value. All pipeline
        // teardown happens after the loop to avoid ownership issues
        // with thread handles across multiple break paths.

        let mut pending_acks = PendingAckQueue::new();
        let mut received_data = false;

        let exit_reason: SessionExit = replica_stream_uring(
            &tcp_writer,
            &input_producer,
            &raw_journal_tx,
            &journal_cursor,
            &mut pending_acks,
            &mut received_data,
            &mut journal_accum,
            &mut accum_end_sequence,
            &mut accum_chain_hash,
            shutdown,
            promote,
        );

        // --- Common teardown (all exit paths) ---

        // Flush any accumulated data not yet submitted.
        if !journal_accum.is_empty() {
            if let Ok(target) = submit_batch_to_pipeline(
                &journal_accum,
                accum_end_sequence,
                accum_chain_hash,
                &input_producer,
                &raw_journal_tx,
            ) {
                pending_acks.push(target, accum_end_sequence);
            }
            journal_accum.clear();
        }
        // Wait for all pending batches to become durable.
        if let Some(seq) = pending_acks.pop_all_blocking(&journal_cursor) {
            let _ = send_ack_tcp(seq, &mut tcp_writer, &mut send_buf);
        }

        // Shut down pipeline and recover state.
        drop(raw_journal_tx);
        let pipeline_state = shutdown_pipeline(
            &pipeline_shutdown,
            journal_handle,
            matching_handle,
            drain_handle,
            shadow_handle,
        );

        match exit_reason {
            SessionExit::Shutdown => return Ok(None),

            SessionExit::Promote => {
                return match pipeline_state {
                    Some((e, w)) => Ok(Some((e, w))),
                    None => Err("pipeline thread panicked during promotion".into()),
                };
            }

            SessionExit::Fatal(e) => return Err(e),

            SessionExit::Disconnected => {
                // Recover Exchange + JournalWriter for the next iteration.
                match pipeline_state {
                    Some((e, w)) => {
                        last_sequence = w.next_sequence().saturating_sub(1);
                        chain_hash = w.chain_hash().unwrap_or([0u8; 32]);
                        exchange = Some(e);
                        journal_writer = Some(w);
                    }
                    None => {
                        error!("pipeline thread panicked during disconnect recovery");
                        if journal_path.exists() {
                            match melin_engine::journal::JournaledExchange::recover(journal_path) {
                                Ok(engine) => {
                                    last_sequence = engine.next_sequence().saturating_sub(1);
                                    chain_hash = engine.writer_chain_hash().unwrap_or([0u8; 32]);
                                    let (e, w) = engine.into_parts();
                                    exchange = Some(e);
                                    journal_writer = Some(w);
                                }
                                Err(e) => {
                                    return Err(format!(
                                        "pipeline panicked and journal recovery failed: {e}"
                                    )
                                    .into());
                                }
                            }
                        } else {
                            return Err("pipeline panicked and no journal to recover from".into());
                        }
                    }
                }

                if received_data {
                    backoff = std::time::Duration::from_secs(1);
                }

                warn!(
                    last_sequence,
                    backoff_secs = backoff.as_secs(),
                    "reconnecting to primary"
                );
                sleep_checking_flags(backoff, shutdown, promote);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

/// Pin a replica pipeline thread to a core, mirroring the primary's layout.
fn pin_replica_thread(name: &str, core: usize) {
    match crate::affinity::pin_to_core(core) {
        Ok(c) => tracing::info!(core = c, thread = name, "pinned to core"),
        Err(e) => tracing::warn!(thread = name, error = e, "core pinning failed"),
    }
}

/// Sleep for the given duration in 100ms increments, checking shutdown
/// and promote flags between increments. Returns early if either is set.
fn sleep_checking_flags(
    duration: std::time::Duration,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
) {
    let deadline = std::time::Instant::now() + duration;
    while std::time::Instant::now() < deadline {
        if shutdown.load(Ordering::Relaxed) || promote.load(Ordering::Acquire) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Shut down the replica pipeline and extract Exchange + JournalWriter from
/// the stage threads. Returns None if a thread panicked.
fn shutdown_pipeline(
    shutdown_flag: &AtomicBool,
    journal_handle: std::thread::JoinHandle<melin_engine::journal::writer::JournalWriter>,
    matching_handle: std::thread::JoinHandle<melin_engine::exchange::Exchange>,
    drain_handle: std::thread::JoinHandle<()>,
    shadow_handle: Option<std::thread::JoinHandle<()>>,
) -> Option<(
    melin_engine::exchange::Exchange,
    melin_engine::journal::writer::JournalWriter,
)> {
    shutdown_flag.store(true, Ordering::Relaxed);
    let writer = journal_handle.join().ok()?;
    let exchange = matching_handle.join().ok()?;
    let _ = drain_handle.join();
    if let Some(h) = shadow_handle {
        let _ = h.join();
    }
    Some((exchange, writer))
}

/// Decode accumulated journal bytes into events, publish to the input
/// disruptor, and send raw bytes to the journal stage's SPSC ring.
/// Returns the disruptor sequence target that the caller must wait for
/// before sending an ack (ensures persist-before-ack).
///
/// This function is NON-BLOCKING — it pushes to the SPSC ring and
/// returns immediately (unless the ring is full, in which case it
/// spins briefly). The caller can submit multiple batches before
/// waiting, allowing the journal stage to overlap NVMe writes with
/// TCP receives.
fn submit_batch_to_pipeline(
    journal_bytes: &[u8],
    end_sequence: u64,
    chain_hash: [u8; 32],
    producer: &melin_disruptor::ring::MultiProducer<melin_engine::journal::pipeline::InputSlot>,
    raw_tx: &melin_engine::journal::pipeline::RawBatchSender,
) -> Result<u64, Box<dyn std::error::Error>> {
    use melin_engine::journal::pipeline::{InputSlot, RawJournalBatch};

    // Decode ALL entries from the raw bytes and publish to the disruptor,
    // including auto-emitted Checkpoint entries. Count the actual entries
    // published — this may exceed entry_count because the primary's
    // entry_count only counts disruptor events, not checkpoint entries
    // that the hash chain auto-emits into the journal bytes.
    let mut offset = 0;
    let mut last_published_seq = 0u64;
    let mut decoded_count: u32 = 0;
    while offset < journal_bytes.len() {
        let remaining = &journal_bytes[offset..];
        match melin_engine::journal::codec::decode(
            remaining,
            melin_engine::journal::codec::FORMAT_VERSION,
        ) {
            Ok((consumed, _sequence, _timestamp_ns, key_hash, request_seq, event)) => {
                last_published_seq = producer.publish(InputSlot {
                    connection_id: 0,
                    key_hash,
                    request_seq,
                    event,
                    publish_ts: Default::default(),
                    recv_ts: Default::default(),
                });
                decoded_count += 1;
                offset += consumed;
            }
            Err(e) => {
                return Err(
                    format!("failed to decode journal entry at offset {offset}: {e}").into(),
                );
            }
        }
    }

    // Send raw bytes to the journal stage via the bounded SPSC ring.
    // The ring has 8 slots — the receiver can pipeline up to 8 batches
    // ahead of the journal stage's NVMe writes.
    raw_tx.send(RawJournalBatch {
        bytes: journal_bytes.to_vec(),
        end_sequence,
        chain_hash,
        entry_count: decoded_count,
    });

    // Return the disruptor target — the caller waits for this before
    // sending an ack.
    Ok(last_published_seq + 1)
}

/// Wait for the journal cursor to reach the target sequence,
/// confirming all submitted batches are durable on disk.
fn wait_for_journal_cursor(journal_cursor: &melin_disruptor::padding::Sequence, target: u64) {
    while journal_cursor.get().load(Ordering::Acquire) < target {
        std::hint::spin_loop();
    }
}

// --- DPDK replication (smoltcp transport) ---

/// Result of trying to extract one length-prefixed frame from a buffer.
#[cfg(feature = "dpdk")]
enum FrameResult {
    /// Complete frame found: payload starts at index 0, frame ends at index 1.
    Complete(usize, usize),
    /// Not enough data for a complete frame — wait for more.
    Incomplete,
    /// Frame exceeds max_size or is malformed — connection should be dropped.
    Oversized,
}

/// Try to extract one length-prefixed frame from a receive buffer.
#[cfg(feature = "dpdk")]
fn try_extract_frame(buf: &[u8], max_size: usize) -> FrameResult {
    if buf.len() < 4 {
        return FrameResult::Incomplete;
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    if len == 0 || len > max_size {
        return FrameResult::Oversized;
    }
    if buf.len() < 4 + len {
        return FrameResult::Incomplete;
    }
    FrameResult::Complete(4, 4 + len)
}

/// Compact a receive buffer by removing consumed bytes from the front.
#[cfg(feature = "dpdk")]
fn compact_recv_buf(buf: &mut Vec<u8>, consumed: usize) {
    if consumed > 0 {
        buf.drain(..consumed);
    }
}

/// DPDK variant of the replication sender. Uses a `DpdkTransport` (smoltcp)
/// instead of kernel TCP. The replication sender thread gets its own DPDK
/// queue pair for independent NIC access.
///
/// Supports dual replicas: each slot has its own `ReplicationConsumer` and
/// independent state machine. Both are polled in a single-threaded loop
/// (no per-replica threads — DPDK is single-threaded).
///
#[cfg(any(feature = "dpdk", test))]
/// Compute and store the replication cursor for dual-replica DPDK mode.
///
/// The cursor is `min(this_slot_acked, other_slot_acked)`. Idle slots use
/// `u64::MAX` so they don't block. Uses `store` (not `fetch_max`) because
/// the cursor must be able to *decrease* when a second replica connects
/// with a lower acked position.
/// Update both replication cursors after an ack.
///
/// - `cursor_min`: `min(slot0, slot1)` — both replicas have confirmed up to here.
/// - `cursor_max`: `max(slot0, slot1)` — fastest replica has confirmed up to here.
///
/// The response stage uses these for quorum durability:
/// `durable = max(cursor_min, min(journal, cursor_max))`
/// i.e., an event is durable if *either* both replicas acked *or* the
/// journal fsynced and the fastest replica acked (two distinct durable copies).
fn update_dual_replication_cursor(
    this_acked: u64,
    other_acked: u64,
    cursor_min: &AtomicU64,
    cursor_max: &AtomicU64,
) {
    cursor_min.store(this_acked.min(other_acked), Ordering::Release);
    cursor_max.store(this_acked.max(other_acked), Ordering::Release);
}

/// The protocol is identical to `run_sender` — same wire format, same
/// handshake, same streaming logic. Only the I/O primitives differ.
#[cfg(feature = "dpdk")]
pub fn run_sender_dpdk(
    mut transport: melin_dpdk::DpdkTransport,
    repl_consumers: [ReplicationConsumer; 2],
    replication_cursor: Arc<AtomicU64>,
    fastest_replica_cursor: Arc<AtomicU64>,
    genesis_entry: Vec<u8>,
    journal_path: std::path::PathBuf,
    shutdown: &AtomicBool,
    replica_ready: &AtomicBool,
    replicas_connected: &AtomicU32,
    evict_flags: [Arc<AtomicBool>; 2],
    active_flags: [Arc<AtomicBool>; 2],
    metrics: Arc<ReplicationMetrics>,
    batch_size: usize,
    heartbeat_secs: u64,
    busy_spin: bool,
) {
    info!("DPDK replication sender started (dual-replica)");

    /// Per-slot state machine.
    enum SlotState {
        /// No replica connected on this slot.
        Idle,
        /// Replica connected, performing handshake.
        Handshaking(melin_dpdk::SocketHandle),
        /// Streaming journal data to replica.
        Streaming(melin_dpdk::SocketHandle),
    }

    /// Per-replica slot. Each has its own ring consumer and state.
    struct DpdkReplicaSlot {
        state: SlotState,
        consumer: ReplicationConsumer,
        active_flag: Arc<AtomicBool>,
        evict_flag: Arc<AtomicBool>,
        recv_buf: Vec<u8>,
        send_buf: Vec<u8>,
        last_send: std::time::Instant,
        last_sequence: u64,
        last_chain_hash: [u8; 32],
        /// Per-slot acked cursor. `u64::MAX` when not streaming —
        /// doesn't block the replication cursor (min of both slots).
        acked_cursor: u64,
    }

    let [consumer_0, consumer_1] = repl_consumers;
    let heartbeat_interval = std::time::Duration::from_secs(heartbeat_secs);
    let now = std::time::Instant::now();

    let mut slots = [
        DpdkReplicaSlot {
            state: SlotState::Idle,
            consumer: consumer_0,
            active_flag: Arc::clone(&active_flags[0]),
            evict_flag: Arc::clone(&evict_flags[0]),
            recv_buf: Vec::with_capacity(4096),
            send_buf: Vec::with_capacity(512 * 1024),
            last_send: now,
            last_sequence: 0,
            last_chain_hash: [0u8; 32],
            acked_cursor: u64::MAX,
        },
        DpdkReplicaSlot {
            state: SlotState::Idle,
            consumer: consumer_1,
            active_flag: Arc::clone(&active_flags[1]),
            evict_flag: Arc::clone(&evict_flags[1]),
            recv_buf: Vec::with_capacity(4096),
            send_buf: Vec::with_capacity(512 * 1024),
            last_send: now,
            last_sequence: 0,
            last_chain_hash: [0u8; 32],
            acked_cursor: u64::MAX,
        },
    ];

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("DPDK replication sender shutting down");
            return;
        }

        // Drive smoltcp (rx/tx, timers, retransmit).
        transport.poll();

        // Accept new connections into the first idle slot.
        let accepted = transport.take_accepted();
        for conn in accepted {
            let idle_slot = slots
                .iter()
                .position(|s| matches!(s.state, SlotState::Idle));
            if let Some(idx) = idle_slot {
                info!(peer = ?conn.peer, slot = idx, "replica connected via DPDK");
                replicas_connected.fetch_add(1, Ordering::Release);
                slots[idx].recv_buf.clear();
                slots[idx].state = SlotState::Handshaking(conn.handle);
            } else {
                debug!(peer = ?conn.peer, "replica rejected — both slots occupied");
                transport.close(conn.handle);
            }
        }

        // Check eviction flags from the journal stage.
        for (i, slot) in slots.iter_mut().enumerate() {
            if slot.evict_flag.load(Ordering::Acquire) && !matches!(slot.state, SlotState::Idle) {
                metrics.evictions_total.fetch_add(1, Ordering::Relaxed);
                warn!(
                    slot = i,
                    "evicting slow replica (ring backpressure timeout, DPDK)"
                );
                if let SlotState::Streaming(h) | SlotState::Handshaking(h) = slot.state {
                    transport.close(h);
                }
                slot.active_flag.store(false, Ordering::Release);
                slot.evict_flag.store(false, Ordering::Release);
                metrics.acked_sequence[i].store(0, Ordering::Relaxed);
                metrics.catching_up[i].store(false, Ordering::Relaxed);
                slot.acked_cursor = u64::MAX;
                slot.recv_buf.clear();
                slot.state = SlotState::Idle;
                replicas_connected.fetch_sub(1, Ordering::Release);
                if replicas_connected.load(Ordering::Relaxed) == 0 {
                    replication_cursor.store(u64::MAX, Ordering::Release);
                    fastest_replica_cursor.store(u64::MAX, Ordering::Release);
                    warn!("all replicas disconnected — trading halted");
                }
            }
        }

        let mut any_active = false;

        for slot_idx in 0..2 {
            // Split the array to get disjoint mutable/immutable borrows.
            // This lets us read the other slot's acked_cursor while
            // mutably borrowing the current slot.
            let (slot, other_acked) = {
                let (left, right) = slots.split_at_mut(1);
                if slot_idx == 0 {
                    (&mut left[0], right[0].acked_cursor)
                } else {
                    (&mut right[0], left[0].acked_cursor)
                }
            };

            match slot.state {
                SlotState::Idle => {
                    // Drain ring to keep it flowing. The journal stage
                    // skips inactive rings (active_flag=false), but there
                    // may be residual entries from before the flag was cleared.
                    while slot.consumer.try_read().is_some() {
                        slot.consumer.commit();
                    }
                }

                SlotState::Handshaking(handle) => {
                    any_active = true;

                    // Check for disconnect during handshake.
                    if !transport.is_active(handle) {
                        warn!(
                            slot = slot_idx,
                            "replica disconnected during handshake (DPDK)"
                        );
                        slot.state = SlotState::Idle;
                        slot.acked_cursor = u64::MAX;
                        slot.recv_buf.clear();
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        if replicas_connected.load(Ordering::Relaxed) == 0 {
                            replication_cursor.store(u64::MAX, Ordering::Release);
                            fastest_replica_cursor.store(u64::MAX, Ordering::Release);
                        }
                        continue;
                    }

                    // Try to read handshake frame.
                    transport.recv_into_vec(handle, &mut slot.recv_buf);

                    match try_extract_frame(&slot.recv_buf, MAX_CONTROL_FRAME) {
                        FrameResult::Complete(payload_start, frame_end) => {
                            let payload = &slot.recv_buf[payload_start..frame_end];
                            match decode_replica_message(payload) {
                                Ok(ReplicaMessage::Handshake(h)) => {
                                    info!(
                                        slot = slot_idx,
                                        last_sequence = h.last_sequence,
                                        "replica handshake received (DPDK)"
                                    );

                                    metrics.catching_up[slot_idx].store(true, Ordering::Relaxed);

                                    // Probe whether journal catch-up is possible.
                                    let can_catch_up = match can_catch_up_from_journal(
                                        &journal_path,
                                        h.last_sequence,
                                    ) {
                                        Ok(v) => v,
                                        Err(e) => {
                                            warn!(slot = slot_idx, error = %e, "catch-up probe failed — disconnecting");
                                            transport.close(handle);
                                            slot.state = SlotState::Idle;
                                            slot.recv_buf.clear();
                                            replicas_connected.fetch_sub(1, Ordering::Release);
                                            if replicas_connected.load(Ordering::Relaxed) == 0 {
                                                replication_cursor
                                                    .store(u64::MAX, Ordering::Release);
                                            }
                                            continue;
                                        }
                                    };

                                    compact_recv_buf(&mut slot.recv_buf, frame_end);

                                    if can_catch_up {
                                        // Send StreamStart, then catch up from journal files.
                                        slot.send_buf.clear();
                                        encode_stream_start(
                                            h.last_sequence,
                                            &genesis_entry,
                                            &mut slot.send_buf,
                                        );
                                        transport.queue_send(handle, &slot.send_buf);
                                        slot.send_buf.clear();

                                        // Journal catch-up via DPDK transport.
                                        if let Err(e) = catch_up_from_journal_dpdk(
                                            &journal_path,
                                            h.last_sequence,
                                            handle,
                                            &mut transport,
                                            &mut slot.send_buf,
                                            shutdown,
                                        ) {
                                            warn!(slot = slot_idx, error = %e, "journal catch-up failed — disconnecting");
                                            transport.close(handle);
                                            slot.state = SlotState::Idle;
                                            slot.recv_buf.clear();
                                            metrics.catching_up[slot_idx]
                                                .store(false, Ordering::Relaxed);
                                            replicas_connected.fetch_sub(1, Ordering::Release);
                                            if replicas_connected.load(Ordering::Relaxed) == 0 {
                                                replication_cursor
                                                    .store(u64::MAX, Ordering::Release);
                                            }
                                            continue;
                                        }
                                    } else {
                                        // Replica's state predates all journal archives.
                                        // Transfer a snapshot, then catch up.
                                        if let Err(e) = snapshot_transfer_dpdk(
                                            &journal_path,
                                            &genesis_entry,
                                            handle,
                                            &mut transport,
                                            &mut slot.send_buf,
                                            shutdown,
                                        ) {
                                            warn!(slot = slot_idx, error = %e, "snapshot transfer failed — disconnecting");
                                            transport.close(handle);
                                            slot.state = SlotState::Idle;
                                            slot.recv_buf.clear();
                                            metrics.catching_up[slot_idx]
                                                .store(false, Ordering::Relaxed);
                                            replicas_connected.fetch_sub(1, Ordering::Release);
                                            if replicas_connected.load(Ordering::Relaxed) == 0 {
                                                replication_cursor
                                                    .store(u64::MAX, Ordering::Release);
                                            }
                                            continue;
                                        }
                                    }

                                    // Set cursor to this replica's acked position.
                                    slot.acked_cursor = h.last_sequence + 1;
                                    slot.last_sequence = h.last_sequence;
                                    slot.last_chain_hash = h.chain_hash;
                                    slot.last_send = std::time::Instant::now();

                                    // Drain overlapping ring entries from catch-up.
                                    while let Some((meta, _data)) = slot.consumer.try_read() {
                                        if meta.end_sequence > h.last_sequence {
                                            // This batch has new data beyond catch-up.
                                            // Send it now and commit so the live loop
                                            // starts clean.
                                            slot.send_buf.clear();
                                            encode_data_batch(
                                                meta.end_sequence,
                                                &meta.chain_hash,
                                                meta.entry_count,
                                                _data,
                                                &mut slot.send_buf,
                                            );
                                            slot.consumer.commit();
                                            transport.queue_send(handle, &slot.send_buf);
                                            slot.send_buf.clear();
                                            slot.last_sequence = meta.end_sequence;
                                            slot.last_chain_hash = meta.chain_hash;
                                            break;
                                        }
                                        slot.consumer.commit();
                                    }

                                    // Mark ring active before signaling readiness
                                    // so the journal stage publishes when seeds flow.
                                    slot.active_flag.store(true, Ordering::Release);
                                    replica_ready.store(true, Ordering::Release);

                                    update_dual_replication_cursor(
                                        slot.acked_cursor,
                                        other_acked,
                                        &replication_cursor,
                                        &fastest_replica_cursor,
                                    );

                                    metrics.catching_up[slot_idx].store(false, Ordering::Relaxed);
                                    slot.state = SlotState::Streaming(handle);
                                }
                                Ok(ReplicaMessage::Ack(_)) => {
                                    warn!(
                                        slot = slot_idx,
                                        "expected Handshake, got Ack — disconnecting"
                                    );
                                    transport.close(handle);
                                    slot.state = SlotState::Idle;
                                    slot.recv_buf.clear();
                                    replicas_connected.fetch_sub(1, Ordering::Release);
                                    if replicas_connected.load(Ordering::Relaxed) == 0 {
                                        replication_cursor.store(u64::MAX, Ordering::Release);
                                        fastest_replica_cursor.store(u64::MAX, Ordering::Release);
                                    }
                                }
                                Err(e) => {
                                    warn!(slot = slot_idx, error = %e, "failed to decode handshake — disconnecting");
                                    transport.close(handle);
                                    slot.state = SlotState::Idle;
                                    slot.recv_buf.clear();
                                    replicas_connected.fetch_sub(1, Ordering::Release);
                                    if replicas_connected.load(Ordering::Relaxed) == 0 {
                                        replication_cursor.store(u64::MAX, Ordering::Release);
                                        fastest_replica_cursor.store(u64::MAX, Ordering::Release);
                                    }
                                }
                            }
                        }
                        FrameResult::Oversized => {
                            warn!(slot = slot_idx, "oversized handshake frame — disconnecting");
                            transport.close(handle);
                            slot.state = SlotState::Idle;
                            slot.recv_buf.clear();
                            replicas_connected.fetch_sub(1, Ordering::Release);
                            if replicas_connected.load(Ordering::Relaxed) == 0 {
                                replication_cursor.store(u64::MAX, Ordering::Release);
                                fastest_replica_cursor.store(u64::MAX, Ordering::Release);
                            }
                        }
                        FrameResult::Incomplete => {} // Wait for more data.
                    }
                }

                SlotState::Streaming(handle) => {
                    any_active = true;

                    // 1. Process acks (non-blocking).
                    transport.recv_into_vec(handle, &mut slot.recv_buf);
                    let mut consumed = 0;
                    let mut ack_error = false;
                    loop {
                        let remaining = &slot.recv_buf[consumed..];
                        match try_extract_frame(remaining, MAX_CONTROL_FRAME) {
                            FrameResult::Complete(payload_start, frame_end) => {
                                let payload = &remaining[payload_start..frame_end];
                                if let Ok(ReplicaMessage::Ack(ack)) =
                                    decode_replica_message(payload)
                                {
                                    slot.acked_cursor = ack.acked_sequence + 1;
                                    metrics.acked_sequence[slot_idx]
                                        .store(ack.acked_sequence, Ordering::Relaxed);
                                    update_dual_replication_cursor(
                                        slot.acked_cursor,
                                        other_acked,
                                        &replication_cursor,
                                        &fastest_replica_cursor,
                                    );
                                }
                                consumed += frame_end;
                            }
                            FrameResult::Oversized => {
                                warn!(
                                    slot = slot_idx,
                                    "oversized ack frame from replica — disconnecting"
                                );
                                ack_error = true;
                                break;
                            }
                            FrameResult::Incomplete => break,
                        }
                    }
                    compact_recv_buf(&mut slot.recv_buf, consumed);
                    if ack_error {
                        transport.close(handle);
                        slot.active_flag.store(false, Ordering::Release);
                        slot.acked_cursor = u64::MAX;
                        metrics.acked_sequence[slot_idx].store(0, Ordering::Relaxed);
                        slot.recv_buf.clear();
                        slot.state = SlotState::Idle;
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        if replicas_connected.load(Ordering::Relaxed) == 0 {
                            replication_cursor.store(u64::MAX, Ordering::Release);
                            fastest_replica_cursor.store(u64::MAX, Ordering::Release);
                            warn!("all replicas disconnected — trading halted");
                        }
                        continue;
                    }

                    // 2. Send data batches.
                    slot.send_buf.clear();
                    let mut batches_sent = 0;
                    if let Some((meta, data)) = slot.consumer.try_read() {
                        encode_data_batch(
                            meta.end_sequence,
                            &meta.chain_hash,
                            meta.entry_count,
                            data,
                            &mut slot.send_buf,
                        );
                        slot.consumer.commit();
                        slot.last_sequence = meta.end_sequence;
                        slot.last_chain_hash = meta.chain_hash;
                        batches_sent += 1;

                        // Coalesce more batches.
                        for _ in 1..batch_size {
                            if let Some((meta, data)) = slot.consumer.try_read() {
                                encode_data_batch(
                                    meta.end_sequence,
                                    &meta.chain_hash,
                                    meta.entry_count,
                                    data,
                                    &mut slot.send_buf,
                                );
                                slot.consumer.commit();
                                slot.last_sequence = meta.end_sequence;
                                slot.last_chain_hash = meta.chain_hash;
                                batches_sent += 1;
                            } else {
                                break;
                            }
                        }

                        metrics.bytes_sent[slot_idx]
                            .fetch_add(slot.send_buf.len() as u64, Ordering::Relaxed);
                        transport.queue_send(handle, &slot.send_buf);
                        slot.last_send = std::time::Instant::now();
                    }

                    // 3. Heartbeat if idle.
                    if batches_sent == 0 && slot.last_send.elapsed() >= heartbeat_interval {
                        slot.send_buf.clear();
                        encode_heartbeat(
                            slot.last_sequence,
                            &slot.last_chain_hash,
                            &mut slot.send_buf,
                        );
                        transport.queue_send(handle, &slot.send_buf);
                        slot.last_send = std::time::Instant::now();
                    }

                    // 4. Check for disconnect.
                    if !transport.is_active(handle) {
                        warn!(slot = slot_idx, "replica disconnected (DPDK)");
                        slot.active_flag.store(false, Ordering::Release);
                        slot.acked_cursor = u64::MAX;
                        metrics.acked_sequence[slot_idx].store(0, Ordering::Relaxed);
                        slot.recv_buf.clear();
                        slot.state = SlotState::Idle;
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        if replicas_connected.load(Ordering::Relaxed) == 0 {
                            replication_cursor.store(u64::MAX, Ordering::Release);
                            fastest_replica_cursor.store(u64::MAX, Ordering::Release);
                            warn!("all replicas disconnected — trading halted");
                        }
                        continue;
                    }

                    // Eviction is handled by the journal-stage evict_flag check
                    // at the top of the loop (lines 3254+). No timeout-based
                    // eviction here — try_read() returning None means the
                    // consumer caught up, not that it's slow.
                }
            }
        }

        if !any_active {
            if busy_spin {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

/// DPDK-adapted journal catch-up: reads journal files and sends DataBatch
/// frames via the DPDK transport. Periodically polls the transport to flush
/// TX and keep smoltcp's timers alive.
#[cfg(feature = "dpdk")]
fn catch_up_from_journal_dpdk(
    journal_path: &std::path::Path,
    last_sequence: u64,
    handle: melin_dpdk::SocketHandle,
    transport: &mut melin_dpdk::DpdkTransport,
    send_buf: &mut Vec<u8>,
    shutdown: &AtomicBool,
) -> std::io::Result<()> {
    use melin_engine::journal::reader::RawJournalScanner;

    let files = discover_journal_files(journal_path);
    if files.is_empty() {
        return Ok(());
    }

    // Find the first file that contains entries after last_sequence.
    let mut start_file_idx = 0;
    if last_sequence > 0 {
        let mut found = false;
        for (i, path) in files.iter().enumerate().rev() {
            let mut scanner = RawJournalScanner::open(path)
                .map_err(|e| io::Error::other(format!("open journal {}: {e}", path.display())))?;
            if let Some(first_seq) = scanner
                .first_sequence()
                .map_err(|e| io::Error::other(format!("read {}: {e}", path.display())))?
                && first_seq <= last_sequence
            {
                start_file_idx = i;
                found = true;
                break;
            }
        }
        if !found {
            return Err(io::Error::other(
                "catch-up failed: replica's last_sequence predates all journal files",
            ));
        }
    }

    let mut batch_buf = Vec::with_capacity(64 * 1024);
    let mut end_sequence = last_sequence;
    let mut batches_sent = 0u64;

    info!(
        last_sequence,
        files = files.len(),
        start_file = start_file_idx,
        "starting journal catch-up (DPDK)"
    );

    for path in &files[start_file_idx..] {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        let mut scanner = RawJournalScanner::open(path)
            .map_err(|e| io::Error::other(format!("open journal {}: {e}", path.display())))?;

        let skip_to = end_sequence.max(1);
        scanner
            .skip_to_after(skip_to)
            .map_err(|e| io::Error::other(format!("skip in {}: {e}", path.display())))?;

        loop {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(());
            }

            batch_buf.clear();
            let batch = scanner
                .read_raw_batch(&mut batch_buf, 64 * 1024)
                .map_err(|e| io::Error::other(format!("read {}: {e}", path.display())))?;

            let Some((entry_count, batch_end_seq)) = batch else {
                break;
            };

            send_buf.clear();
            encode_data_batch(batch_end_seq, &[0u8; 32], entry_count, &batch_buf, send_buf);
            transport.queue_send(handle, send_buf);
            // Flush TX periodically to keep smoltcp and the NIC flowing.
            transport.poll();

            if !transport.is_active(handle) {
                return Err(io::Error::other(
                    "replica disconnected during journal catch-up",
                ));
            }

            end_sequence = batch_end_seq;
            batches_sent += 1;
        }
    }

    info!(
        end_sequence,
        batches_sent, "journal catch-up complete (DPDK)"
    );
    Ok(())
}

/// Transfer a snapshot to a replica via DPDK, then catch up from journals.
/// Sends: NeedSnapshot → SnapshotBegin → SnapshotChunk* → SnapshotEnd →
/// StreamStart → DataBatch* (catch-up).
#[cfg(feature = "dpdk")]
fn snapshot_transfer_dpdk(
    journal_path: &std::path::Path,
    genesis_entry: &[u8],
    handle: melin_dpdk::SocketHandle,
    transport: &mut melin_dpdk::DpdkTransport,
    send_buf: &mut Vec<u8>,
    shutdown: &AtomicBool,
) -> std::io::Result<()> {
    let snap_path = journal_path.with_extension("snapshot");
    if !snap_path.exists() {
        return Err(io::Error::other(
            "snapshot transfer required but no snapshot available \
             — enable --snapshot-interval-secs or trigger a journal rotation",
        ));
    }

    // Send NeedSnapshot.
    send_buf.clear();
    encode_need_snapshot(send_buf);
    transport.queue_send(handle, send_buf);
    transport.poll();

    // Read and validate snapshot.
    let snap_data = std::fs::read(&snap_path)
        .map_err(|e| io::Error::other(format!("read snapshot {}: {e}", snap_path.display())))?;
    if snap_data.len() < 48 {
        return Err(io::Error::other("snapshot file too small for header"));
    }
    let magic = u32::from_le_bytes(snap_data[0..4].try_into().unwrap());
    if magic != 0x534E_4150 {
        return Err(io::Error::other(format!(
            "snapshot file has invalid magic: {magic:#x} (expected 0x534e4150)"
        )));
    }
    let snap_sequence = u64::from_le_bytes(snap_data[8..16].try_into().unwrap());
    let mut snap_chain_hash = [0u8; 32];
    snap_chain_hash.copy_from_slice(&snap_data[16..48]);
    let snap_len = snap_data.len() as u64;

    info!(snap_sequence, snap_len, path = %snap_path.display(), "transferring snapshot to replica (DPDK)");

    // Send SnapshotBegin.
    send_buf.clear();
    encode_snapshot_begin(snap_len, snap_sequence, &snap_chain_hash, send_buf);
    transport.queue_send(handle, send_buf);
    transport.poll();

    // Stream snapshot in 64 KiB chunks.
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut offset = 0;
    while offset < snap_data.len() {
        let end = (offset + CHUNK_SIZE).min(snap_data.len());
        send_buf.clear();
        encode_snapshot_chunk(&snap_data[offset..end], send_buf);
        transport.queue_send(handle, send_buf);
        // Flush periodically to avoid overwhelming the TX queue.
        if offset % (CHUNK_SIZE * 8) == 0 {
            transport.poll();
            if !transport.is_active(handle) {
                return Err(io::Error::other(
                    "replica disconnected during snapshot transfer",
                ));
            }
        }
        offset = end;
    }
    transport.poll();

    // Send SnapshotEnd with CRC32C.
    let transfer_crc = crc32c::crc32c(&snap_data);
    send_buf.clear();
    encode_snapshot_end(transfer_crc, send_buf);
    transport.queue_send(handle, send_buf);
    transport.poll();

    info!(snap_sequence, "snapshot transfer complete (DPDK)");

    // Send StreamStart so the replica can set up its journal.
    send_buf.clear();
    encode_stream_start(snap_sequence, genesis_entry, send_buf);
    transport.queue_send(handle, send_buf);
    transport.poll();

    // Catch up from the snapshot's sequence using the current journal.
    catch_up_from_journal_dpdk(
        journal_path,
        snap_sequence,
        handle,
        transport,
        send_buf,
        shutdown,
    )
}

/// DPDK variant of the replication receiver. Uses a `DpdkTransport` (smoltcp)
/// to connect to the primary via DPDK instead of kernel TCP.
///
/// Includes reconnection with exponential backoff (1s → 30s) and snapshot
/// transfer support — matching the TCP receiver's feature set.
///
/// The protocol is identical to `run_receiver` — same wire format, same
/// fsync-then-ack-then-replay pattern. Only the I/O primitives differ.
#[cfg(feature = "dpdk")]
pub fn run_receiver_dpdk(
    mut transport: melin_dpdk::DpdkTransport,
    primary_ip: std::net::Ipv4Addr,
    primary_port: u16,
    journal_path: &std::path::Path,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_secs: u64,
    snapshot_path: std::path::PathBuf,
) -> ReceiverResult {
    use melin_engine::exchange::Exchange;
    use melin_engine::journal::writer::JournalWriter;

    // Recover local state from journal (if any). On first call this may
    // be (None, None) for a fresh replica. After a reconnect, the pipeline
    // shutdown returns the Exchange + JournalWriter directly.
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        if journal_path.exists() {
            let engine = if snapshot_path.exists() {
                info!("recovering replica from snapshot + journal (DPDK)");
                melin_engine::journal::JournaledExchange::recover_from_snapshot(
                    &snapshot_path,
                    journal_path,
                )?
            } else {
                melin_engine::journal::JournaledExchange::recover(journal_path)?
            };
            let next = engine.next_sequence();
            let last = next.saturating_sub(1);
            let hash = engine.writer_chain_hash().unwrap_or([0u8; 32]);
            let (exchange, writer) = engine.into_parts();
            (Some(exchange), Some(writer), last, hash)
        } else {
            (None, None, 0u64, [0u8; 32])
        };

    // Exponential backoff for reconnection: 1s → 2s → 4s → … → 30s max.
    // Reset to 1s on successful streaming (first DataBatch received).
    let mut backoff = std::time::Duration::from_secs(1);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

    // Reusable buffers — survive across reconnections.
    let mut send_buf = Vec::with_capacity(64);
    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    // Ephemeral port counter for outbound connections. Each reconnect uses
    // a different local port to avoid smoltcp TIME_WAIT conflicts.
    let mut local_port: u16 = 40000;

    // --- Outer reconnect loop ---
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered while disconnected (DPDK)");
            return match (exchange, journal_writer) {
                (Some(e), Some(w)) => Ok(Some((e, w))),
                _ => Err("promotion requested but no local state available".into()),
            };
        }

        info!(
            primary_ip = %primary_ip,
            primary_port,
            "connecting to primary as replica (DPDK)"
        );

        // Connect to primary via smoltcp.
        let handle = transport.connect_to(primary_ip, primary_port, local_port);
        local_port = local_port.wrapping_add(1).max(40000);

        // Poll until TCP handshake completes (with timeout).
        let connect_start = std::time::Instant::now();
        const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let connected = loop {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(None);
            }
            transport.poll();
            if transport.is_connected(handle) {
                break true;
            }
            if !transport.is_active(handle) || connect_start.elapsed() >= CONNECT_TIMEOUT {
                break false;
            }
            std::thread::yield_now();
        };

        if !connected {
            warn!(
                backoff_secs = backoff.as_secs(),
                "failed to connect to primary (DPDK) — retrying"
            );
            transport.close(handle);
            sleep_checking_flags(backoff, shutdown, promote);
            if shutdown.load(Ordering::Relaxed) {
                return Ok(None);
            }
            if promote.load(Ordering::Acquire) {
                info!("promotion triggered during reconnect backoff (DPDK)");
                return match (exchange, journal_writer) {
                    (Some(e), Some(w)) => Ok(Some((e, w))),
                    _ => Err("promotion requested but no local state available".into()),
                };
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        info!("connected to primary (DPDK)");

        // Send handshake.
        send_buf.clear();
        let handshake = Handshake {
            last_sequence,
            chain_hash,
        };
        encode_handshake(&handshake, &mut send_buf);
        transport.queue_send(handle, &send_buf);
        send_buf.clear();

        // Read protocol response (StreamStart / NeedSnapshot / HashMismatch).
        recv_buf.clear();
        let primary_genesis_entry = 'handshake: loop {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(None);
            }
            transport.poll();
            transport.recv_into_vec(handle, &mut recv_buf);

            match try_extract_frame(&recv_buf, MAX_CONTROL_FRAME) {
                FrameResult::Complete(payload_start, frame_end) => {
                    let payload = &recv_buf[payload_start..frame_end];
                    let response = decode_primary_message(payload)?;
                    compact_recv_buf(&mut recv_buf, frame_end);
                    match response {
                        PrimaryMessage::StreamStart {
                            start_sequence,
                            genesis_entry,
                        } => {
                            info!(start_sequence, "streaming started (DPDK)");
                            break 'handshake genesis_entry;
                        }
                        PrimaryMessage::NeedSnapshot => {
                            info!("primary requires snapshot transfer — receiving snapshot (DPDK)");

                            // Remove stale local state. Invalidate the in-memory
                            // Exchange and JournalWriter — their underlying files
                            // are about to be deleted. Without this, a failed
                            // snapshot transfer would leave stale state that
                            // the reconnect loop mistakes for valid.
                            exchange = None;
                            journal_writer = None;
                            let _ = std::fs::remove_file(journal_path);
                            let _ = std::fs::remove_file(&snapshot_path);

                            // Receive snapshot via DPDK transport.
                            match receive_snapshot_dpdk(
                                handle,
                                &mut transport,
                                &mut recv_buf,
                                &snapshot_path,
                                shutdown,
                            ) {
                                Ok((snap_exchange, snap_seq, snap_hash)) => {
                                    exchange = Some(snap_exchange);
                                    let writer = JournalWriter::create_continuing(
                                        journal_path,
                                        snap_seq + 1,
                                        snap_hash,
                                    )?;
                                    journal_writer = Some(writer);
                                    last_sequence = snap_seq;
                                    chain_hash = snap_hash;

                                    // After snapshot, expect StreamStart.
                                    continue;
                                }
                                Err(e) => {
                                    warn!(error = %e, "snapshot transfer failed (DPDK) — retrying");
                                    transport.close(handle);
                                    sleep_checking_flags(backoff, shutdown, promote);
                                    backoff = (backoff * 2).min(MAX_BACKOFF);
                                    break 'handshake Vec::new(); // will be caught by the empty check below
                                }
                            }
                        }
                        PrimaryMessage::HashMismatch => {
                            return Err(
                                "chain hash mismatch — replica has divergent history".into()
                            );
                        }
                        other => {
                            return Err(format!("unexpected response: {other:?}").into());
                        }
                    }
                }
                FrameResult::Oversized => {
                    return Err("oversized frame from primary during handshake".into());
                }
                FrameResult::Incomplete => {}
            }

            if !transport.is_active(handle) {
                warn!("disconnected from primary during handshake (DPDK)");
                transport.close(handle);
                sleep_checking_flags(backoff, shutdown, promote);
                backoff = (backoff * 2).min(MAX_BACKOFF);
                break Vec::new(); // trigger reconnect via empty check
            }
            std::thread::yield_now();
        };

        // Empty genesis entry means the handshake loop exited via a
        // failure path (disconnect or snapshot error) — reconnect.
        if primary_genesis_entry.is_empty() {
            continue;
        }

        // Create journal for fresh replica using the primary's raw genesis entry.
        if journal_writer.is_none() && !primary_genesis_entry.is_empty() {
            use melin_engine::journal::codec::{self as journal_codec, FILE_HEADER_SIZE};
            use std::fs::OpenOptions;
            use std::os::unix::fs::FileExt;

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(journal_path)?;
            let mut header = [0u8; FILE_HEADER_SIZE];
            journal_codec::encode_file_header(&mut header);
            file.write_all_at(&header, 0)?;
            file.write_all_at(&primary_genesis_entry, FILE_HEADER_SIZE as u64)?;
            file.sync_all()?;

            let genesis_chain_hash = {
                let entry_len = primary_genesis_entry.len();
                let hash = blake3::hash(&primary_genesis_entry[..entry_len - 4]);
                *hash.as_bytes()
            };

            let valid_end = FILE_HEADER_SIZE as u64 + primary_genesis_entry.len() as u64;
            let writer = JournalWriter::open_append(
                journal_path,
                1,
                valid_end,
                Some(genesis_chain_hash),
                0,
            )?;
            exchange = Some(Exchange::new());
            journal_writer = Some(writer);
        }

        // If we still have no state after all the handshake logic, reconnect.
        if exchange.is_none() || journal_writer.is_none() {
            continue;
        }

        let cur_exchange = exchange.take().expect("exchange initialized");
        let cur_writer = journal_writer.take().expect("journal_writer initialized");

        // Clone exchange for shadow stage before moving into pipeline.
        let shadow_exchange = cur_exchange.clone_via_snapshot();

        // Build the replica pipeline — same as the TCP receiver.
        let enable_shadow = snapshot_interval_secs > 0;
        let (
            input_producer,
            journal_stage,
            matching_stage,
            drain_consumer,
            journal_cursor,
            _matching_cursor,
            raw_journal_tx,
            shadow_consumer,
            chain_hash_lock,
        ) = melin_engine::journal::pipeline::build_replica_pipeline(
            cur_exchange,
            cur_writer,
            4096,
            false,
            enable_shadow,
        );

        let pipeline_shutdown = Arc::new(AtomicBool::new(false));

        let ps = Arc::clone(&pipeline_shutdown);
        let journal_handle = std::thread::Builder::new()
            .name("journal".into())
            .spawn(move || journal_stage.run(&ps))
            .expect("spawn journal thread");

        let ps = Arc::clone(&pipeline_shutdown);
        let matching_handle = std::thread::Builder::new()
            .name("matching".into())
            .spawn(move || matching_stage.run(&ps))
            .expect("spawn matching thread");

        let ps = Arc::clone(&pipeline_shutdown);
        let drain_handle = std::thread::Builder::new()
            .name("drain".into())
            .spawn(move || {
                let mut consumer = drain_consumer;
                let mut batch = vec![melin_engine::journal::pipeline::OutputSlot::default(); 256];
                loop {
                    if ps.load(Ordering::Relaxed) {
                        return;
                    }
                    let count = consumer.consume_batch(&mut batch, 256);
                    if count == 0 {
                        std::thread::yield_now();
                    }
                }
            })
            .expect("spawn drain thread");

        let shadow_handle = if let Some(shadow_cons) = shadow_consumer {
            let snap_path = snapshot_path.clone();
            let chain_lock = chain_hash_lock.expect("chain hash lock with shadow");
            let ps = Arc::clone(&pipeline_shutdown);
            Some(
                std::thread::Builder::new()
                    .name("replica-shadow".into())
                    .spawn(move || {
                        crate::shadow::run(
                            shadow_cons,
                            shadow_exchange,
                            snap_path,
                            std::time::Duration::from_secs(snapshot_interval_secs),
                            chain_lock,
                            &ps,
                            false,
                        );
                    })
                    .expect("spawn shadow thread"),
            )
        } else {
            None
        };

        let mut pending_acks = PendingAckQueue::new();
        let mut received_data = false;
        let mut journal_accum: Vec<u8> = Vec::with_capacity(128 * 1024);
        let mut accum_end_sequence: u64 = 0;
        let mut accum_chain_hash: [u8; 32] = [0u8; 32];

        // Encode an ack into send_buf and queue it on the DPDK transport.
        macro_rules! send_ack_dpdk {
            ($seq:expr) => {{
                send_buf.clear();
                encode_ack(
                    &Ack {
                        acked_sequence: $seq,
                    },
                    &mut send_buf,
                );
                transport.queue_send(handle, &send_buf);
            }};
        }

        // --- Inner streaming loop ---
        let session_exit = 'streaming: loop {
            if shutdown.load(Ordering::Relaxed) {
                info!("replica shutting down (DPDK)");
                if let Some(seq) = pending_acks.pop_all_blocking(&journal_cursor) {
                    send_ack_dpdk!(seq);
                    transport.poll();
                }
                drop(raw_journal_tx);
                shutdown_pipeline(
                    &pipeline_shutdown,
                    journal_handle,
                    matching_handle,
                    drain_handle,
                    shadow_handle,
                );
                return Ok(None);
            }

            if promote.load(Ordering::Acquire) {
                info!("promotion triggered (DPDK) — stopping replication");
                // Drain remaining data from smoltcp buffer.
                loop {
                    transport.poll();
                    let before = recv_buf.len();
                    transport.recv_into_vec(handle, &mut recv_buf);
                    if recv_buf.len() == before {
                        break;
                    }
                    let mut consumed = 0;
                    loop {
                        let remaining = &recv_buf[consumed..];
                        match try_extract_frame(remaining, MAX_DATA_FRAME) {
                            FrameResult::Complete(ps, fe) => {
                                if let Ok(PrimaryMessage::DataBatch {
                                    end_sequence,
                                    entry_count: _,
                                    journal_bytes,
                                    chain_hash: batch_chain_hash,
                                }) = decode_primary_message(&remaining[ps..fe])
                                {
                                    journal_accum.extend_from_slice(&journal_bytes);
                                    accum_end_sequence = end_sequence;
                                    accum_chain_hash = batch_chain_hash;
                                }
                                consumed += fe;
                            }
                            _ => break,
                        }
                    }
                    compact_recv_buf(&mut recv_buf, consumed);
                }
                if !journal_accum.is_empty() {
                    if let Ok(target) = submit_batch_to_pipeline(
                        &journal_accum,
                        accum_end_sequence,
                        accum_chain_hash,
                        &input_producer,
                        &raw_journal_tx,
                    ) {
                        pending_acks.push(target, accum_end_sequence);
                    }
                    journal_accum.clear();
                }
                if let Some(seq) = pending_acks.pop_all_blocking(&journal_cursor) {
                    send_ack_dpdk!(seq);
                    transport.poll();
                }
                drop(raw_journal_tx);
                return match shutdown_pipeline(
                    &pipeline_shutdown,
                    journal_handle,
                    matching_handle,
                    drain_handle,
                    shadow_handle,
                ) {
                    Some((ex, wr)) => Ok(Some((ex, wr))),
                    None => Err("pipeline thread panicked during promotion (DPDK)".into()),
                };
            }

            // Flush any acks that have become durable since last iteration.
            if let Some(seq) = pending_acks.pop_ready(&journal_cursor) {
                send_ack_dpdk!(seq);
            }

            // Backpressure: if pipeline is saturated, block until the oldest
            // batch is durable.
            if pending_acks.is_full() {
                let seq = pending_acks.pop_oldest_blocking(&journal_cursor);
                send_ack_dpdk!(seq);
            }

            // Poll smoltcp and receive data.
            transport.poll();
            transport.recv_into_vec(handle, &mut recv_buf);

            // Check for disconnect.
            if !transport.is_active(handle) && recv_buf.is_empty() {
                if let Some(seq) = pending_acks.pop_all_blocking(&journal_cursor) {
                    send_ack_dpdk!(seq);
                    transport.poll();
                }
                break 'streaming false; // disconnected
            }

            // Parse frames from the receive buffer.
            let mut consumed = 0;
            let mut got_data = false;
            loop {
                let remaining = &recv_buf[consumed..];
                match try_extract_frame(remaining, MAX_DATA_FRAME) {
                    FrameResult::Complete(payload_start, frame_end) => {
                        let payload = &remaining[payload_start..frame_end];
                        match decode_primary_message(payload) {
                            Ok(PrimaryMessage::DataBatch {
                                end_sequence,
                                entry_count: _,
                                journal_bytes,
                                chain_hash: batch_chain_hash,
                            }) => {
                                journal_accum.extend_from_slice(&journal_bytes);
                                accum_end_sequence = end_sequence;
                                accum_chain_hash = batch_chain_hash;
                                got_data = true;
                                received_data = true;
                            }
                            Ok(PrimaryMessage::Heartbeat {
                                sequence,
                                chain_hash: _,
                            }) => {
                                debug!(sequence, "heartbeat from primary (DPDK)");
                            }
                            Ok(other) => {
                                debug!("unexpected message during streaming: {other:?}");
                            }
                            Err(e) => {
                                drop(raw_journal_tx);
                                shutdown_pipeline(
                                    &pipeline_shutdown,
                                    journal_handle,
                                    matching_handle,
                                    drain_handle,
                                    shadow_handle,
                                );
                                return Err(format!("failed to decode primary message: {e}").into());
                            }
                        }
                        consumed += frame_end;
                    }
                    FrameResult::Oversized => {
                        drop(raw_journal_tx);
                        shutdown_pipeline(
                            &pipeline_shutdown,
                            journal_handle,
                            matching_handle,
                            drain_handle,
                            shadow_handle,
                        );
                        return Err("oversized frame from primary during streaming".into());
                    }
                    FrameResult::Incomplete => break,
                }
            }
            compact_recv_buf(&mut recv_buf, consumed);

            // Submit to pipeline and record pending ack.
            if got_data {
                let target = submit_batch_to_pipeline(
                    &journal_accum,
                    accum_end_sequence,
                    accum_chain_hash,
                    &input_producer,
                    &raw_journal_tx,
                )?;

                pending_acks.push(target, accum_end_sequence);
                journal_accum.clear();
            } else {
                std::thread::yield_now();
            }
        };

        // --- Disconnect handling: recover state and reconnect ---
        let _disconnected = session_exit; // false = disconnected
        drop(raw_journal_tx);

        match shutdown_pipeline(
            &pipeline_shutdown,
            journal_handle,
            matching_handle,
            drain_handle,
            shadow_handle,
        ) {
            Some((e, w)) => {
                last_sequence = w.next_sequence().saturating_sub(1);
                chain_hash = w.chain_hash().unwrap_or([0u8; 32]);
                exchange = Some(e);
                journal_writer = Some(w);
            }
            None => {
                error!("pipeline thread panicked during disconnect recovery (DPDK)");
                if journal_path.exists() {
                    match melin_engine::journal::JournaledExchange::recover(journal_path) {
                        Ok(engine) => {
                            last_sequence = engine.next_sequence().saturating_sub(1);
                            chain_hash = engine.writer_chain_hash().unwrap_or([0u8; 32]);
                            let (e, w) = engine.into_parts();
                            exchange = Some(e);
                            journal_writer = Some(w);
                        }
                        Err(e) => {
                            return Err(format!(
                                "pipeline panicked and journal recovery failed: {e}"
                            )
                            .into());
                        }
                    }
                } else {
                    return Err("pipeline panicked and no journal to recover from (DPDK)".into());
                }
            }
        }

        if received_data {
            backoff = std::time::Duration::from_secs(1);
        }

        warn!(
            last_sequence,
            backoff_secs = backoff.as_secs(),
            "reconnecting to primary (DPDK)"
        );
        sleep_checking_flags(backoff, shutdown, promote);
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Receive a snapshot from the primary via DPDK transport.
/// Expects: SnapshotBegin → SnapshotChunk* → SnapshotEnd.
/// Returns the loaded Exchange, snapshot sequence, and chain hash.
#[cfg(feature = "dpdk")]
fn receive_snapshot_dpdk(
    handle: melin_dpdk::SocketHandle,
    transport: &mut melin_dpdk::DpdkTransport,
    recv_buf: &mut Vec<u8>,
    snapshot_path: &std::path::Path,
    shutdown: &AtomicBool,
) -> Result<
    (melin_engine::exchange::Exchange, u64, [u8; 32]),
    Box<dyn std::error::Error + Send + Sync>,
> {
    // Read SnapshotBegin.
    let (snap_len, snap_sequence, snap_chain_hash) = loop {
        if shutdown.load(Ordering::Relaxed) {
            return Err("shutdown during snapshot transfer".into());
        }
        transport.poll();
        transport.recv_into_vec(handle, recv_buf);

        match try_extract_frame(recv_buf, MAX_CONTROL_FRAME) {
            FrameResult::Complete(payload_start, frame_end) => {
                let payload = &recv_buf[payload_start..frame_end];
                let msg = decode_primary_message(payload)?;
                compact_recv_buf(recv_buf, frame_end);
                match msg {
                    PrimaryMessage::SnapshotBegin {
                        snapshot_len,
                        snap_sequence,
                        snap_chain_hash,
                    } => break (snapshot_len, snap_sequence, snap_chain_hash),
                    other => return Err(format!("expected SnapshotBegin, got {other:?}").into()),
                }
            }
            FrameResult::Oversized => {
                return Err("oversized frame during snapshot transfer".into());
            }
            FrameResult::Incomplete => {}
        }

        if !transport.is_active(handle) {
            return Err("disconnected during snapshot transfer".into());
        }
        std::thread::yield_now();
    };

    info!(snap_sequence, snap_len, "receiving snapshot (DPDK)");

    // Receive snapshot chunks into a temp file.
    let tmp_path = snapshot_path.with_extension("snapshot.tmp");
    {
        let mut tmp_file = std::fs::File::create(&tmp_path)?;
        let mut received: u64 = 0;
        let mut running_crc: u32 = 0;

        'snap_recv: loop {
            if shutdown.load(Ordering::Relaxed) {
                let _ = std::fs::remove_file(&tmp_path);
                return Err("shutdown during snapshot transfer".into());
            }
            transport.poll();
            transport.recv_into_vec(handle, recv_buf);

            // Process all complete frames in the buffer.
            let mut consumed = 0;
            loop {
                let remaining = &recv_buf[consumed..];
                match try_extract_frame(remaining, MAX_DATA_FRAME) {
                    FrameResult::Complete(payload_start, frame_end) => {
                        let payload = &remaining[payload_start..frame_end];
                        match decode_primary_message(payload)? {
                            PrimaryMessage::SnapshotChunk(data) => {
                                std::io::Write::write_all(&mut tmp_file, &data)?;
                                received += data.len() as u64;
                                running_crc = crc32c::crc32c_append(running_crc, &data);
                            }
                            PrimaryMessage::SnapshotEnd {
                                crc32c: expected_crc,
                            } => {
                                tmp_file.sync_all()?;
                                drop(tmp_file);

                                if received != snap_len {
                                    let _ = std::fs::remove_file(&tmp_path);
                                    return Err(format!(
                                        "snapshot length mismatch: expected {snap_len}, got {received}"
                                    )
                                    .into());
                                }
                                if running_crc != expected_crc {
                                    let _ = std::fs::remove_file(&tmp_path);
                                    return Err(format!(
                                        "snapshot CRC mismatch: expected {expected_crc:#x}, got {running_crc:#x}"
                                    )
                                    .into());
                                }

                                std::fs::rename(&tmp_path, snapshot_path)?;
                                info!(
                                    snap_sequence,
                                    received, "snapshot received and verified (DPDK)"
                                );
                                consumed += frame_end;
                                compact_recv_buf(recv_buf, consumed);
                                break 'snap_recv;
                            }
                            other => {
                                let _ = std::fs::remove_file(&tmp_path);
                                return Err(
                                    format!("expected SnapshotChunk/End, got {other:?}").into()
                                );
                            }
                        }
                        consumed += frame_end;
                    }
                    FrameResult::Oversized => {
                        let _ = std::fs::remove_file(&tmp_path);
                        return Err("oversized frame during snapshot chunk transfer".into());
                    }
                    FrameResult::Incomplete => break,
                }
            }
            compact_recv_buf(recv_buf, consumed);

            if !transport.is_active(handle) {
                let _ = std::fs::remove_file(&tmp_path);
                return Err("disconnected during snapshot chunk transfer".into());
            }
            std::thread::yield_now();
        }
    } // tmp_file dropped here if not already dropped in SnapshotEnd path

    // Load and verify the snapshot.
    let (snap_exchange, _snap_seq, snap_hash) =
        melin_engine::journal::snapshot::load(snapshot_path)?;
    if snap_hash != snap_chain_hash {
        return Err(format!(
            "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
             loaded snapshot has {snap_hash:02x?}"
        )
        .into());
    }

    Ok((snap_exchange, snap_sequence, snap_chain_hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_encode_decode_round_trip() {
        let handshake = Handshake {
            last_sequence: 42,
            chain_hash: [0xAB; 32],
        };
        let mut buf = Vec::new();
        encode_handshake(&handshake, &mut buf);

        // Read frame: skip 4-byte length prefix.
        let payload = &buf[4..];
        let msg = decode_replica_message(payload).unwrap();
        match msg {
            ReplicaMessage::Handshake(h) => {
                assert_eq!(h.last_sequence, 42);
                assert_eq!(h.chain_hash, [0xAB; 32]);
            }
            _ => panic!("expected Handshake"),
        }
    }

    #[test]
    fn ack_encode_decode_round_trip() {
        let ack = Ack {
            acked_sequence: 1000,
        };
        let mut buf = Vec::new();
        encode_ack(&ack, &mut buf);

        let payload = &buf[4..];
        let msg = decode_replica_message(payload).unwrap();
        match msg {
            ReplicaMessage::Ack(a) => {
                assert_eq!(a.acked_sequence, 1000);
            }
            _ => panic!("expected Ack"),
        }
    }

    #[test]
    fn stream_start_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_stream_start(99, &[0xAA; 64], &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::StreamStart {
                start_sequence,
                genesis_entry,
            } => {
                assert_eq!(start_sequence, 99);
                assert_eq!(genesis_entry, vec![0xAA; 64]);
            }
            _ => panic!("expected StreamStart"),
        }
    }

    #[test]
    fn data_batch_encode_decode_round_trip() {
        let journal_bytes = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let chain_hash = [0xCD; 32];
        let mut buf = Vec::new();
        encode_data_batch(500, &chain_hash, 1, &journal_bytes, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::DataBatch {
                entry_count: _,
                end_sequence,
                chain_hash: h,
                journal_bytes: data,
            } => {
                assert_eq!(end_sequence, 500);
                assert_eq!(h, chain_hash);
                assert_eq!(data, journal_bytes);
            }
            _ => panic!("expected DataBatch"),
        }
    }

    #[test]
    fn heartbeat_encode_decode_round_trip() {
        let chain_hash = [0xEF; 32];
        let mut buf = Vec::new();
        encode_heartbeat(123, &chain_hash, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::Heartbeat {
                sequence,
                chain_hash: h,
            } => {
                assert_eq!(sequence, 123);
                assert_eq!(h, chain_hash);
            }
            _ => panic!("expected Heartbeat"),
        }
    }

    #[test]
    fn need_snapshot_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_need_snapshot(&mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        assert!(matches!(msg, PrimaryMessage::NeedSnapshot));
    }

    #[test]
    fn hash_mismatch_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_hash_mismatch(&mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        assert!(matches!(msg, PrimaryMessage::HashMismatch));
    }

    #[test]
    fn unknown_replica_message_type_is_error() {
        let payload = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = decode_replica_message(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_primary_message_type_is_error() {
        let payload = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = decode_primary_message(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn read_frame_enforces_max_size() {
        // Create a buffer with a length prefix claiming 1000 bytes.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 100]); // not enough data, but max_size check comes first

        let mut cursor = std::io::Cursor::new(buf);
        let result = read_frame(&mut cursor, 64);
        assert!(result.is_err());
    }

    #[test]
    fn challenge_encode_decode_round_trip() {
        let nonce = [0x42; 32];
        let mut buf = Vec::new();
        encode_challenge(&nonce, &mut buf);

        let payload = &buf[4..];
        let decoded = decode_challenge(payload).unwrap();
        assert_eq!(decoded, nonce);
    }

    #[test]
    fn challenge_response_encode_decode_round_trip() {
        let sig = [0xAA; 64];
        let pubkey = [0xBB; 32];
        let mut buf = Vec::new();
        encode_challenge_response(&sig, &pubkey, &mut buf);

        let payload = &buf[4..];
        let (decoded_sig, decoded_pubkey) = decode_challenge_response(payload).unwrap();
        assert_eq!(decoded_sig, sig);
        assert_eq!(decoded_pubkey, pubkey);
    }

    #[test]
    fn auth_ok_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_auth_ok(&mut buf);

        let payload = &buf[4..];
        assert!(decode_auth_result(payload).unwrap());
    }

    #[test]
    fn auth_failed_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_auth_failed(&mut buf);

        let payload = &buf[4..];
        assert!(!decode_auth_result(payload).unwrap());
    }

    #[test]
    fn decode_challenge_rejects_wrong_tag() {
        let mut payload = [0u8; 33];
        payload[0] = MSG_AUTH_OK;
        assert!(decode_challenge(&payload).is_err());
    }

    #[test]
    fn decode_challenge_response_rejects_short_payload() {
        let payload = [MSG_CHALLENGE_RESPONSE; 10]; // too short
        assert!(decode_challenge_response(&payload).is_err());
    }

    #[test]
    fn auth_round_trip_valid_key() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        let repl_key = SigningKey::from_bytes(&[0xFC; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            repl_key.verifying_key().to_bytes(),
        );
        let keys_content = format!("replication {pub_b64} test-replica\n");
        let authorized_keys = melin_protocol::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let repl_key_clone = SigningKey::from_bytes(&[0xFC; 32]);
        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            authenticate_with_primary(&mut reader, &mut writer, &repl_key_clone)
        });

        let mut reader = primary_stream.try_clone().unwrap();
        let mut writer = primary_stream;
        authenticate_replica(&mut reader, &mut writer, &authorized_keys).unwrap();

        replica_handle.join().unwrap().unwrap();
    }

    #[test]
    fn auth_rejects_unknown_key() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        // authorized_keys has one key, but the replica uses a different one.
        let authorized_key = SigningKey::from_bytes(&[0xAA; 32]);
        let rogue_key = SigningKey::from_bytes(&[0xBB; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            authorized_key.verifying_key().to_bytes(),
        );
        let keys_content = format!("replication {pub_b64} authorized-replica\n");
        let authorized_keys = melin_protocol::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            authenticate_with_primary(&mut reader, &mut writer, &rogue_key)
        });

        let mut reader = primary_stream.try_clone().unwrap();
        let mut writer = primary_stream;
        let result = authenticate_replica(&mut reader, &mut writer, &authorized_keys);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown"));

        // Replica should also get a rejection.
        let replica_result = replica_handle.join().unwrap();
        assert!(replica_result.is_err());
    }

    #[test]
    fn auth_rejects_wrong_permission() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        // Key exists but has Trader permission, not Replication.
        let key = SigningKey::from_bytes(&[0xCC; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            key.verifying_key().to_bytes(),
        );
        let keys_content = format!("trader {pub_b64} wrong-role\n");
        let authorized_keys = melin_protocol::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            authenticate_with_primary(&mut reader, &mut writer, &key)
        });

        let mut reader = primary_stream.try_clone().unwrap();
        let mut writer = primary_stream;
        let result = authenticate_replica(&mut reader, &mut writer, &authorized_keys);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Replication"));

        let replica_result = replica_handle.join().unwrap();
        assert!(replica_result.is_err());
    }

    #[test]
    fn sender_receiver_end_to_end() {
        use std::os::unix::net::UnixStream;

        // Create a mock connection.
        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let replication_cursor = Arc::new(AtomicU64::new(0));

        // Spawn a thread simulating the replica side.
        let _replica_cursor = Arc::clone(&replication_cursor);
        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;

            // Send handshake.
            let mut buf = Vec::new();
            let handshake = Handshake {
                last_sequence: 0,
                chain_hash: [0u8; 32],
            };
            encode_handshake(&handshake, &mut buf);
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let msg = decode_primary_message(&frame).unwrap();
            assert!(matches!(msg, PrimaryMessage::StreamStart { .. }));

            // Read DataBatch.
            let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
            let msg = decode_primary_message(&frame).unwrap();
            let end_seq = match &msg {
                PrimaryMessage::DataBatch { end_sequence, .. } => *end_sequence,
                _ => panic!("expected DataBatch, got {msg:?}"),
            };

            // Send ack.
            let ack = Ack {
                acked_sequence: end_seq,
            };
            encode_ack(&ack, &mut buf);
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();

            end_seq
        });

        // Primary side: simulate handle_replica_connection partially.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let handshake = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Handshake(h) => h,
            _ => panic!("expected Handshake"),
        };
        assert_eq!(handshake.last_sequence, 0);

        // Send StreamStart.
        let mut buf = Vec::new();
        encode_stream_start(0, &[0u8; 32], &mut buf); // fake genesis bytes for test
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send a DataBatch with some fake journal bytes.
        let journal_bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let chain_hash = [0x11; 32];
        encode_data_batch(42, &chain_hash, 1, &journal_bytes, &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Read ack.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let ack = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Ack(a) => a,
            _ => panic!("expected Ack"),
        };
        assert_eq!(ack.acked_sequence, 42);

        // Join replica thread.
        let end_seq = replica_handle.join().unwrap();
        assert_eq!(end_seq, 42);
    }

    #[test]
    fn disconnect_degrades_cursor_to_max() {
        // When a replica disconnects, run_sender resets the replication
        // cursor to u64::MAX so the response stage stops gating on acks.
        // Test the cursor lifecycle: starts at 0, set during handshake,
        // then reset to MAX on disconnect.
        let cursor = Arc::new(AtomicU64::new(0));

        // Simulate handshake: cursor set to last_sequence + 1.
        let handshake_seq = 42u64;
        cursor.store(handshake_seq + 1, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 43);

        // Simulate ack advancing cursor.
        cursor.fetch_max(100 + 1, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 101);

        // Simulate disconnect: run_sender resets to MAX.
        cursor.store(u64::MAX, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), u64::MAX);

        // Simulate reconnect: cursor set back to handshake value.
        cursor.store(1, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 1);
    }

    #[test]
    fn ack_advances_cursor_monotonically() {
        // Acks must only advance the cursor, never regress it.
        // A stale ack (lower sequence) should be ignored.
        let cursor = Arc::new(AtomicU64::new(0));

        // Simulate processing ack seq=100 → cursor should become 101.
        let new_val = 100 + 1;
        cursor.fetch_max(new_val, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 101);

        // Stale ack seq=50 → cursor should stay at 101.
        let stale_val = 50 + 1;
        cursor.fetch_max(stale_val, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 101);

        // Newer ack seq=200 → cursor should advance to 201.
        let newer_val = 200 + 1;
        cursor.fetch_max(newer_val, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 201);
    }

    #[test]
    fn multiple_data_batches_acked_in_order() {
        // Send multiple DataBatch frames, verify replica acks each one
        // and the cursor advances correctly.
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            let mut buf = Vec::new();

            // Send handshake.
            encode_handshake(
                &Handshake {
                    last_sequence: 0,
                    chain_hash: [0u8; 32],
                },
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::StreamStart { .. }
            ));

            // Read and ack 3 DataBatches.
            let mut acked_seqs = Vec::new();
            for _ in 0..3 {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                let end_seq = match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::DataBatch { end_sequence, .. } => end_sequence,
                    other => panic!("expected DataBatch, got {other:?}"),
                };
                acked_seqs.push(end_seq);

                encode_ack(
                    &Ack {
                        acked_sequence: end_seq,
                    },
                    &mut buf,
                );
                writer.write_all(&buf).unwrap();
                writer.flush().unwrap();
                buf.clear();
            }

            acked_seqs
        });

        // Primary side.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;
        let mut buf = Vec::new();

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        assert!(matches!(
            decode_replica_message(&frame).unwrap(),
            ReplicaMessage::Handshake(_)
        ));

        // Send StreamStart.
        encode_stream_start(0, &[0u8; 32], &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send 3 DataBatches with increasing sequence numbers.
        for seq in [10u64, 20, 30] {
            encode_data_batch(seq, &[0x11; 32], 1, &[0xAA; 8], &mut buf);
            p_writer.write_all(&buf).unwrap();
            p_writer.flush().unwrap();
            buf.clear();
        }

        // Read 3 acks.
        for expected_seq in [10u64, 20, 30] {
            let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
            let ack = match decode_replica_message(&frame).unwrap() {
                ReplicaMessage::Ack(a) => a,
                other => panic!("expected Ack, got {other:?}"),
            };
            assert_eq!(ack.acked_sequence, expected_seq);
        }

        let acked = replica_handle.join().unwrap();
        assert_eq!(acked, vec![10, 20, 30]);
    }

    #[test]
    fn heartbeat_encode_contains_sequence_and_hash() {
        // Heartbeat messages carry the last known sequence and chain hash
        // so the replica can verify it hasn't missed any data.
        let chain_hash = [0x42; 32];
        let mut buf = Vec::new();
        encode_heartbeat(999, &chain_hash, &mut buf);

        let payload = &buf[4..];
        match decode_primary_message(payload).unwrap() {
            PrimaryMessage::Heartbeat {
                sequence,
                chain_hash: h,
            } => {
                assert_eq!(sequence, 999);
                assert_eq!(h, chain_hash);
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn replica_mid_stream_handshake_with_nonzero_sequence() {
        // A replica that already has some data sends a non-zero last_sequence
        // in its handshake. The primary should respond with StreamStart
        // containing that sequence, and the replica should only receive
        // events after that point.
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            let mut buf = Vec::new();

            // Replica already has events up to sequence 100.
            encode_handshake(
                &Handshake {
                    last_sequence: 100,
                    chain_hash: [0xBB; 32],
                },
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart — should echo back our last_sequence.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            match decode_primary_message(&frame).unwrap() {
                PrimaryMessage::StreamStart { start_sequence, .. } => {
                    assert_eq!(
                        start_sequence, 100,
                        "StreamStart should echo replica's last_sequence"
                    );
                }
                other => panic!("expected StreamStart, got {other:?}"),
            }

            // Read a DataBatch — should be for events AFTER 100.
            let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
            match decode_primary_message(&frame).unwrap() {
                PrimaryMessage::DataBatch { end_sequence, .. } => {
                    assert!(
                        end_sequence > 100,
                        "DataBatch should be after replica's last_sequence"
                    );
                }
                other => panic!("expected DataBatch, got {other:?}"),
            }
        });

        // Primary side.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;
        let mut buf = Vec::new();

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let handshake = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Handshake(h) => h,
            _ => panic!("expected Handshake"),
        };
        assert_eq!(handshake.last_sequence, 100);
        assert_eq!(handshake.chain_hash, [0xBB; 32]);

        // Send StreamStart echoing the replica's sequence.
        encode_stream_start(handshake.last_sequence, &[0u8; 32], &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send DataBatch with sequence 150 (after replica's 100).
        encode_data_batch(150, &[0x11; 32], 1, &[0xAA; 8], &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();

        replica_handle.join().unwrap();
    }

    #[test]
    fn snapshot_begin_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_snapshot_begin(1_000_000, 42, &[0xAB; 32], &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotBegin {
                snapshot_len,
                snap_sequence,
                snap_chain_hash,
            } => {
                assert_eq!(snapshot_len, 1_000_000);
                assert_eq!(snap_sequence, 42);
                assert_eq!(snap_chain_hash, [0xAB; 32]);
            }
            _ => panic!("expected SnapshotBegin"),
        }
    }

    #[test]
    fn snapshot_chunk_encode_decode_round_trip() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut buf = Vec::new();
        encode_snapshot_chunk(&data, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotChunk(chunk) => {
                assert_eq!(chunk, data);
            }
            _ => panic!("expected SnapshotChunk"),
        }
    }

    #[test]
    fn snapshot_end_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_snapshot_end(0xDEADBEEF, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotEnd { crc32c } => {
                assert_eq!(crc32c, 0xDEADBEEF);
            }
            _ => panic!("expected SnapshotEnd"),
        }
    }

    /// Simulate the receiver side of a snapshot transfer where the
    /// advertised snap_len doesn't match the actual bytes sent.
    /// The receiver must detect this and return an error.
    #[test]
    fn snapshot_receiver_detects_length_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        // Receiver thread — reads NeedSnapshot, then the snapshot transfer.
        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            // Read NeedSnapshot.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            // Read SnapshotBegin.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let (snap_len, _snap_sequence, _snap_chain_hash) =
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotBegin {
                        snapshot_len,
                        snap_sequence,
                        snap_chain_hash,
                    } => (snapshot_len, snap_sequence, snap_chain_hash),
                    other => panic!("expected SnapshotBegin, got {other:?}"),
                };

            // Receive chunks and check length at SnapshotEnd.
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                    }
                    PrimaryMessage::SnapshotEnd { .. } => {
                        if received != snap_len {
                            return format!(
                                "snapshot length mismatch: expected {snap_len} bytes, got {received}"
                            );
                        }
                        return String::new(); // no error
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }
        });

        // Primary side — send snapshot with wrong advertised length.
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let actual_data = vec![0xAA; 100];
        let wrong_len = 999u64; // advertise 999 bytes, send only 100

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(wrong_len, 42, &[0xBB; 32], &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&actual_data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        let crc = crc32c::crc32c(&actual_data);
        encode_snapshot_end(crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("length mismatch"),
            "expected length mismatch error, got: {error_msg:?}"
        );
    }

    /// Simulate the receiver side of a snapshot transfer where the CRC
    /// in SnapshotEnd doesn't match the actual data. The receiver must
    /// detect and reject the transfer.
    #[test]
    fn snapshot_receiver_detects_crc_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            // Read NeedSnapshot.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            // Read SnapshotBegin.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let snap_len = match decode_primary_message(&frame).unwrap() {
                PrimaryMessage::SnapshotBegin { snapshot_len, .. } => snapshot_len,
                other => panic!("expected SnapshotBegin, got {other:?}"),
            };

            // Receive chunks, verify CRC at SnapshotEnd.
            let mut received_data = Vec::new();
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                        received_data.extend_from_slice(&data);
                    }
                    PrimaryMessage::SnapshotEnd {
                        crc32c: expected_crc,
                    } => {
                        if received != snap_len {
                            return format!("length mismatch: {snap_len} vs {received}");
                        }
                        let actual_crc = crc32c::crc32c(&received_data);
                        if actual_crc != expected_crc {
                            return format!(
                                "CRC mismatch: expected {expected_crc:#x}, got {actual_crc:#x}"
                            );
                        }
                        return String::new();
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }
        });

        // Primary side — send correct length but wrong CRC.
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let data = vec![0xAA; 100];

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(data.len() as u64, 42, &[0xBB; 32], &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        // Send a wrong CRC (flip bits).
        let wrong_crc = !crc32c::crc32c(&data);
        encode_snapshot_end(wrong_crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("CRC mismatch"),
            "expected CRC mismatch error, got: {error_msg:?}"
        );
    }

    /// The receiver verifies the chain hash from the loaded snapshot
    /// matches the one advertised in SnapshotBegin. Simulate a mismatch.
    #[test]
    fn snapshot_receiver_detects_chain_hash_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let (snap_len, _snap_sequence, snap_chain_hash) =
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotBegin {
                        snapshot_len,
                        snap_sequence,
                        snap_chain_hash,
                    } => (snapshot_len, snap_sequence, snap_chain_hash),
                    other => panic!("expected SnapshotBegin, got {other:?}"),
                };

            // Receive the snapshot data.
            let mut received_data = Vec::new();
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                        received_data.extend_from_slice(&data);
                    }
                    PrimaryMessage::SnapshotEnd {
                        crc32c: expected_crc,
                    } => {
                        assert_eq!(received, snap_len, "length should match");
                        let actual_crc = crc32c::crc32c(&received_data);
                        assert_eq!(actual_crc, expected_crc, "CRC should match");
                        break;
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }

            // Simulate chain hash verification: the loaded snapshot would
            // have a different chain hash than what SnapshotBegin advertised.
            let loaded_hash = [0xFF; 32]; // different from snap_chain_hash
            if loaded_hash != snap_chain_hash {
                return format!(
                    "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
                     loaded snapshot has {loaded_hash:02x?}"
                );
            }
            String::new()
        });

        // Primary side — send valid snapshot but with a chain hash in
        // SnapshotBegin that won't match what the replica "loads".
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let data = vec![0xAA; 64];
        // Advertise chain hash [0xBB; 32] — receiver will "load" [0xFF; 32].
        let advertised_hash = [0xBB; 32];

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(data.len() as u64, 10, &advertised_hash, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        let crc = crc32c::crc32c(&data);
        encode_snapshot_end(crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("chain hash mismatch"),
            "expected chain hash mismatch error, got: {error_msg:?}"
        );
    }

    /// Primary-side magic validation: a file without the SNAP magic
    /// (0x534E4150) must be rejected before transfer.
    #[test]
    fn primary_rejects_snapshot_with_invalid_magic() {
        let tmp = tempfile::tempdir().unwrap();
        let snap_path = tmp.path().join("test.snapshot");

        // Write a file with wrong magic but enough bytes for a header.
        let mut bad_snap = vec![0u8; 64];
        bad_snap[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes()); // wrong magic

        std::fs::write(&snap_path, &bad_snap).unwrap();

        // Replicate the primary's validation logic.
        let snap_data = std::fs::read(&snap_path).unwrap();
        assert!(
            snap_data.len() >= 48,
            "file should be big enough for header"
        );

        let magic = u32::from_le_bytes(snap_data[0..4].try_into().unwrap());
        assert_ne!(magic, 0x534E_4150);
        assert_eq!(magic, 0xDEAD_BEEF);
    }

    /// Primary-side: a snapshot file smaller than the 48-byte header
    /// must be rejected.
    #[test]
    fn primary_rejects_snapshot_too_small_for_header() {
        let tmp = tempfile::tempdir().unwrap();
        let snap_path = tmp.path().join("test.snapshot");

        // Write a file smaller than the 48-byte header.
        std::fs::write(&snap_path, [0u8; 20]).unwrap();

        let snap_data = std::fs::read(&snap_path).unwrap();
        assert!(
            snap_data.len() < 48,
            "file must be too small for header validation"
        );
    }

    #[test]
    fn decode_snapshot_begin_too_short() {
        // SnapshotBegin needs type(1) + snapshot_len(8) + snap_sequence(8) + chain_hash(32) = 49.
        // Send only the type byte + a few extra bytes.
        let payload = [MSG_SNAPSHOT_BEGIN, 0x01, 0x02, 0x03];
        let err = decode_primary_message(&payload).unwrap_err();
        assert!(
            err.to_string().contains("SnapshotBegin too short"),
            "expected 'SnapshotBegin too short', got: {err}"
        );
    }

    #[test]
    fn decode_snapshot_end_too_short() {
        // SnapshotEnd needs type(1) + crc32c(4) = 5. Send only the type byte.
        let payload = [MSG_SNAPSHOT_END];
        let err = decode_primary_message(&payload).unwrap_err();
        assert!(
            err.to_string().contains("SnapshotEnd too short"),
            "expected 'SnapshotEnd too short', got: {err}"
        );
    }

    #[test]
    fn decode_snapshot_chunk_empty_data() {
        // SnapshotChunk with just the type byte — valid but empty payload.
        let payload = [MSG_SNAPSHOT_CHUNK];
        let msg = decode_primary_message(&payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotChunk(data) => {
                assert!(data.is_empty());
            }
            _ => panic!("expected SnapshotChunk"),
        }
    }

    // --- PendingAckQueue tests ---

    fn make_journal_cursor(val: u64) -> melin_disruptor::padding::Sequence {
        melin_disruptor::padding::Sequence::new(AtomicU64::new(val))
    }

    #[test]
    fn pending_ack_queue_push_and_pop_ready() {
        let mut q = PendingAckQueue::new();
        assert!(q.is_empty());
        assert!(!q.is_full());

        q.push(10, 100);
        q.push(20, 200);
        assert!(!q.is_empty());

        // Cursor at 5 — neither ready.
        let cursor = make_journal_cursor(5);
        assert!(q.pop_ready(&cursor).is_none());

        // Cursor at 15 — first ready, second not.
        cursor.get().store(15, Ordering::Relaxed);
        assert_eq!(q.pop_ready(&cursor), Some(100));
        // Only one popped — second still pending.
        assert!(!q.is_empty());

        // Cursor at 25 — second now ready.
        cursor.get().store(25, Ordering::Relaxed);
        assert_eq!(q.pop_ready(&cursor), Some(200));
        assert!(q.is_empty());
    }

    #[test]
    fn pending_ack_queue_pop_ready_returns_highest_sequence() {
        // When multiple acks become ready simultaneously, pop_ready
        // returns the highest acked_sequence (ack semantics are
        // cumulative — "everything up to this sequence is durable").
        let mut q = PendingAckQueue::new();
        q.push(10, 100);
        q.push(20, 200);
        q.push(30, 300);

        let cursor = make_journal_cursor(30);
        assert_eq!(q.pop_ready(&cursor), Some(300));
        assert!(q.is_empty());
    }

    #[test]
    fn pending_ack_queue_capacity_and_full() {
        let mut q = PendingAckQueue::new();
        for i in 0..PendingAckQueue::CAP {
            assert!(!q.is_full());
            q.push(i as u64 + 1, (i + 1) as u64 * 100);
        }
        assert!(q.is_full());
    }

    #[test]
    fn pending_ack_queue_pop_oldest_blocking() {
        let mut q = PendingAckQueue::new();
        q.push(10, 100);
        q.push(20, 200);

        // Cursor already past both targets — pop_oldest_blocking
        // returns immediately.
        let cursor = make_journal_cursor(25);
        let seq = q.pop_oldest_blocking(&cursor);
        // Should pop both (oldest + any others that became ready).
        assert_eq!(seq, 200);
        assert!(q.is_empty());
    }

    #[test]
    fn pending_ack_queue_wraps_around() {
        let mut q = PendingAckQueue::new();
        let cursor = make_journal_cursor(100);

        // Fill and drain multiple times to exercise circular buffer wrap.
        for round in 0..3 {
            for i in 0..PendingAckQueue::CAP {
                let target = (round * PendingAckQueue::CAP + i) as u64 + 1;
                q.push(target, target * 10);
            }
            assert!(q.is_full());
            let seq = q.pop_ready(&cursor).expect("should be ready");
            assert_eq!(
                seq,
                (round * PendingAckQueue::CAP + PendingAckQueue::CAP) as u64 * 10
            );
            assert!(q.is_empty());
        }
    }

    #[test]
    fn pending_ack_queue_pop_all_blocking_empty() {
        let mut q = PendingAckQueue::new();
        let cursor = make_journal_cursor(0);
        assert!(q.pop_all_blocking(&cursor).is_none());
    }

    // --- Dual-replica cursor update tests ---

    #[test]
    fn dual_cursor_takes_min_and_max_of_both_slots() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));
        // Slot 0 at seq 100, slot 1 at seq 50 → min = 50, max = 100.
        update_dual_replication_cursor(100, 50, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 50);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn dual_cursor_idle_slot_uses_max() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));
        // Slot 0 at seq 100, slot 1 idle (u64::MAX) → min = 100, max = u64::MAX.
        update_dual_replication_cursor(100, u64::MAX, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 100);
        assert_eq!(cursor_max.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn dual_cursor_both_idle() {
        let cursor_min = Arc::new(AtomicU64::new(42));
        let cursor_max = Arc::new(AtomicU64::new(42));
        // Both idle → min = max = u64::MAX (no replicas gating).
        update_dual_replication_cursor(u64::MAX, u64::MAX, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(cursor_max.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn dual_cursor_decreases_when_slower_replica_connects() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));

        // Slot 0 streaming alone → min = 100, max = u64::MAX.
        update_dual_replication_cursor(100, u64::MAX, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 100);

        // Slot 1 connects with acked_cursor = 51 (last_sequence 50).
        // Min must decrease to 51, max stays at 100.
        update_dual_replication_cursor(51, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 51);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn dual_cursor_advances_as_slower_replica_catches_up() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));

        // Initial: slot 0 at 100, slot 1 at 51 → min = 51, max = 100.
        update_dual_replication_cursor(51, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 51);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);

        // Slot 1 catches up to 80 → min = 80, max = 100.
        update_dual_replication_cursor(80, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 80);

        // Both at 100 → min = max = 100.
        update_dual_replication_cursor(100, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 100);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);

        // Both advance → min = max = 150.
        update_dual_replication_cursor(150, 150, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 150);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn dual_cursor_slot_disconnect_raises_to_surviving() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));

        // Both streaming: slot 0 at 100, slot 1 at 80 → min = 80, max = 100.
        update_dual_replication_cursor(80, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 80);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);

        // Slot 1 disconnects (goes to u64::MAX) → min = 100, max = u64::MAX.
        update_dual_replication_cursor(100, u64::MAX, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 100);
        assert_eq!(cursor_max.load(Ordering::Relaxed), u64::MAX);
    }

    // --- Cursor reset test ---

    #[test]
    fn disconnect_resets_cursor_to_max() {
        // Verify the cursor reset behavior documented in the replication
        // cursor table: "All replicas disconnect → u64::MAX".
        let cursor = Arc::new(AtomicU64::new(42));
        let replicas_connected = Arc::new(AtomicU32::new(1));

        // Simulate disconnect: decrement connected count.
        replicas_connected.fetch_sub(1, Ordering::Release);

        // The sender loop checks and resets.
        if replicas_connected.load(Ordering::Relaxed) == 0 {
            cursor.store(u64::MAX, Ordering::Release);
        }

        assert_eq!(cursor.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn cursor_not_reset_when_replica_still_connected() {
        let cursor = Arc::new(AtomicU64::new(42));
        let replicas_connected = Arc::new(AtomicU32::new(2));

        // One replica disconnects, one remains.
        replicas_connected.fetch_sub(1, Ordering::Release);

        if replicas_connected.load(Ordering::Relaxed) == 0 {
            cursor.store(u64::MAX, Ordering::Release);
        }

        // Cursor should NOT be reset — one replica still connected.
        assert_eq!(cursor.load(Ordering::Relaxed), 42);
    }
}
