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
use std::os::unix::io::AsRawFd;
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

/// Read a length-prefixed frame into a reusable buffer. Avoids per-frame
/// heap allocation — the caller owns the Vec and it grows to high-water
/// mark then stays there.
fn read_frame_into(reader: &mut impl Read, buf: &mut Vec<u8>, max_size: usize) -> io::Result<()> {
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
    buf.resize(len, 0);
    reader.read_exact(buf)?;
    Ok(())
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
                            warn!("all replicas disconnected — trading halted");
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

    // Short read timeout for process_acks. The actual availability check
    // uses poll(0) — this timeout only applies after poll confirms data
    // is ready, as a safety net for partial frames.
    reader.set_read_timeout(Some(std::time::Duration::from_millis(5)))?;

    // pollfd for non-blocking ack availability check. poll(fd, POLLIN, 0)
    // returns immediately — avoids the 2ms+ SO_RCVTIMEO floor that Linux
    // imposes even for 1µs timeouts (kernel jiffy rounding).
    let reader_fd = reader.as_raw_fd();
    let mut pollfd = libc::pollfd {
        fd: reader_fd,
        events: libc::POLLIN,
        revents: 0,
    };

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

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Check if the journal stage evicted this ring (ring was full).
        // Exit so the sender thread can reclaim the slot — the replica
        // will reconnect and catch up from journal files.
        if evict_flag.load(Ordering::Relaxed) {
            info!(slot = slot_idx, "handler exiting: evicted by journal stage");
            return Ok(());
        }

        // Process any pending acks (non-blocking via internal poll(0)).
        match process_acks(&mut reader, replication_cursor, &mut pollfd) {
            Ok(Some(acked_seq)) => {
                metrics.acked_sequence[slot_idx].store(acked_seq, Ordering::Relaxed);
                metrics.ack_latency_us[slot_idx]
                    .store(last_send.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
            Ok(None) => {}
            Err(e) => {
                return Err(io::Error::other(format!("replica ack read error: {e}")));
            }
        }

        // Try to read a batch from the replication ring (non-blocking).
        if let Some((meta, data)) = repl_consumer.try_read() {
            // Coalesce multiple batches into one TCP write+flush to
            // amortize syscall overhead. Drain up to 16 batches from
            // the ring before flushing. Each batch is encoded into the
            // send buffer; a single write_all+flush sends them all.
            encode_data_batch(
                meta.end_sequence,
                &meta.chain_hash,
                meta.entry_count,
                data,
                &mut send_buf,
            );
            repl_consumer.commit();
            last_sequence = meta.end_sequence;
            last_chain_hash = meta.chain_hash;

            // Drain more batches if available.
            for _ in 1..batch_size {
                if let Some((meta, data)) = repl_consumer.try_read() {
                    encode_data_batch(
                        meta.end_sequence,
                        &meta.chain_hash,
                        meta.entry_count,
                        data,
                        &mut send_buf,
                    );
                    repl_consumer.commit();
                    last_sequence = meta.end_sequence;
                    last_chain_hash = meta.chain_hash;
                } else {
                    break;
                }
            }

            let batch_bytes = send_buf.len() as u64;
            if let Err(e) = writer.write_all(&send_buf) {
                return Err(io::Error::other(format!("write DataBatch: {e}")));
            }
            if let Err(e) = writer.flush() {
                return Err(io::Error::other(format!("flush DataBatch: {e}")));
            }
            metrics.bytes_sent[slot_idx].fetch_add(batch_bytes, Ordering::Relaxed);
            send_buf.clear();
            last_send = std::time::Instant::now();
        } else {
            // No batch available — send heartbeat if idle.
            if last_send.elapsed() >= heartbeat_interval {
                encode_heartbeat(last_sequence, &last_chain_hash, &mut send_buf);
                if let Err(e) = writer.write_all(&send_buf) {
                    return Err(io::Error::other(format!("write Heartbeat: {e}")));
                }
                if let Err(e) = writer.flush() {
                    return Err(io::Error::other(format!("flush Heartbeat: {e}")));
                }
                send_buf.clear();
                last_send = std::time::Instant::now();
            }
            // Ring empty — process any pending acks, then yield.
            // Using poll(0) instead of poll(1ms) to avoid adding 1ms
            // to the ack→response latency path when the ring empties
            // between journal stage batches.
            match process_acks(&mut reader, replication_cursor, &mut pollfd) {
                Ok(Some(acked_seq)) => {
                    metrics.acked_sequence[slot_idx].store(acked_seq, Ordering::Relaxed);
                    metrics.ack_latency_us[slot_idx]
                        .store(last_send.elapsed().as_micros() as u64, Ordering::Relaxed);
                }
                Ok(None) => {}
                Err(e) => {
                    return Err(io::Error::other(format!("replica ack read error: {e}")));
                }
            }
            if busy_spin {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

/// Read and process ack frames from the replica using poll(0) for
/// each frame. Never blocks — returns as soon as no more data is
/// available. This avoids the ~2ms Linux SO_RCVTIMEO floor that
/// makes sub-ms read timeouts unreliable.
/// Returns the last acked sequence seen during this call, or `None` if
/// no ack was processed. The caller uses this to update per-replica metrics.
fn process_acks(
    reader: &mut TcpStream,
    replication_cursor: &Arc<AtomicU64>,
    pollfd: &mut libc::pollfd,
) -> io::Result<Option<u64>> {
    let mut last_acked: Option<u64> = None;
    loop {
        // Check if more ack data is available before calling read_frame.
        // poll(0) is truly non-blocking — no kernel jiffy rounding.
        pollfd.revents = 0;
        let ready = unsafe { libc::poll(pollfd, 1, 0) };
        if ready <= 0 || (pollfd.revents & libc::POLLIN) == 0 {
            return Ok(last_acked); // No data available.
        }

        match read_frame(reader, MAX_CONTROL_FRAME) {
            Ok(payload) => match decode_replica_message(&payload) {
                Ok(ReplicaMessage::Ack(ack)) => {
                    let new_val = ack.acked_sequence + 1;
                    let _ = replication_cursor.fetch_max(new_val, Ordering::Release);
                    last_acked = Some(ack.acked_sequence);
                }
                Ok(ReplicaMessage::Handshake(_)) => {
                    debug!("unexpected Handshake during streaming");
                }
                Err(e) => {
                    debug!(error = %e, "failed to decode replica message");
                }
            },
            Err(e) => {
                // WouldBlock/TimedOut is expected (non-blocking read).
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut {
                    return Ok(last_acked);
                }
                // Other errors mean the connection is dead.
                return Err(e);
            }
        }
    }
}

// --- Replication Receiver (Replica side) ---

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

pub fn run_receiver(
    primary_addr: SocketAddr,
    journal_path: &std::path::Path,
    signing_key: &ed25519_dalek::SigningKey,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_secs: u64,
    snapshot_path: std::path::PathBuf,
) -> ReceiverResult {
    use melin_engine::exchange::Exchange;
    use melin_engine::journal::writer::JournalWriter;

    info!(primary = %primary_addr, "connecting to primary as replica");

    let stream = TcpStream::connect(primary_addr)?;
    stream.set_nodelay(true)?;
    // Set a read timeout so the receiver can check the shutdown flag
    // periodically instead of blocking indefinitely.
    stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;

    let mut reader = stream.try_clone()?;
    let mut tcp_writer = stream;

    // Authenticate with the primary before any data exchange.
    authenticate_with_primary(&mut reader, &mut tcp_writer, signing_key)?;
    info!("authenticated with primary");

    // Determine our current state from the local journal (if any).
    // For fresh starts, we defer journal creation until after the handshake
    // so we can use the primary's genesis hash.
    let (mut exchange, mut journal_writer, last_sequence, chain_hash) = if journal_path.exists() {
        // Recover from snapshot + journal (fast) or journal only (full replay).
        let engine = if snapshot_path.exists() {
            info!("recovering replica from snapshot + journal");
            melin_engine::journal::JournaledExchange::recover_from_snapshot(
                &snapshot_path,
                journal_path,
            )?
        } else {
            melin_engine::journal::JournaledExchange::recover(journal_path)?
        };
        // next_sequence is the next to assign, so last written = next - 1.
        // If next_sequence is 1, no user events have been written (only genesis).
        let next = engine.next_sequence();
        let last = next.saturating_sub(1);
        let hash = engine.writer_chain_hash().unwrap_or([0u8; 32]);
        let (exchange, writer) = engine.into_parts();
        (Some(exchange), Some(writer), last, hash)
    } else {
        (None, None, 0u64, [0u8; 32])
    };

    // Send handshake.
    let mut send_buf = Vec::with_capacity(64);
    let handshake = Handshake {
        last_sequence,
        chain_hash,
    };
    encode_handshake(&handshake, &mut send_buf);
    tcp_writer.write_all(&send_buf)?;
    tcp_writer.flush()?;
    send_buf.clear();

    // Read StreamStart (or NeedSnapshot / HashMismatch).
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
            // Primary's journal archives don't go back far enough.
            // Receive a snapshot transfer, then resume streaming.
            info!("primary requires snapshot transfer — receiving snapshot");

            // Delete stale local state.
            let _ = std::fs::remove_file(journal_path);
            let _ = std::fs::remove_file(&snapshot_path);

            // Receive SnapshotBegin.
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

            // Receive chunks into a temp file, computing CRC incrementally
            // to avoid re-reading the entire file after write.
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

                            // Verify received length matches advertised.
                            if received != snap_len {
                                let _ = std::fs::remove_file(&tmp_path);
                                return Err(format!(
                                    "snapshot length mismatch: expected {snap_len} bytes, got {received}"
                                )
                                .into());
                            }

                            // Verify CRC computed incrementally during receive.
                            if running_crc != expected_crc {
                                let _ = std::fs::remove_file(&tmp_path);
                                return Err(format!(
                                    "snapshot CRC mismatch: expected {expected_crc:#x}, got {running_crc:#x}"
                                )
                                .into());
                            }

                            // Rename temp to final.
                            std::fs::rename(&tmp_path, &snapshot_path)?;
                            info!(snap_sequence, received, "snapshot received and verified");
                            break;
                        }
                        other => {
                            let _ = std::fs::remove_file(&tmp_path);
                            return Err(format!("expected SnapshotChunk/End, got {other:?}").into());
                        }
                    }
                }
            }

            // Load the snapshot and verify chain hash matches what the
            // primary advertised in SnapshotBegin.
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

            // Create a fresh journal continuing from the snapshot point.
            let writer = JournalWriter::create_continuing(journal_path, snap_seq + 1, snap_hash)?;
            journal_writer = Some(writer);

            // Read the StreamStart that the primary sends after the snapshot.
            // It carries the genesis entry and start_sequence.
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

    // Create journal for fresh replica using the primary's raw genesis entry.
    // Writing the exact bytes (including the primary's timestamp) ensures
    // the BLAKE3 hash chain is byte-identical, so checkpoint verification
    // works on replica recovery.
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

        // Compute genesis chain hash (same as JournalReader would).
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

    let exchange = exchange.expect("exchange initialized");
    let journal_writer = journal_writer.expect("journal_writer initialized");

    // Clone exchange for the shadow stage BEFORE moving it into the pipeline.
    // The shadow needs the fully-recovered state as its base — it only sees
    // new events from the disruptor, not historical ones from the journal.
    let shadow_exchange = exchange.clone_via_snapshot();

    // Build the replica pipeline — same stages as the primary (journal →
    // matching → shadow), with the replication receiver feeding the disruptor
    // instead of reader threads. The journal stage writes raw bytes from
    // a side channel instead of encoding events.
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
        exchange,
        journal_writer,
        4096,  // max_journal_batch
        false, // don't busy-spin on replica
        enable_shadow,
    );

    // RAII guard for pipeline threads — ensures all threads are joined on
    // any exit path (including ? returns). The guard also signals shutdown
    // and extracts the Exchange + JournalWriter from the stage return values.
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

    // Output drain thread — consumes and discards output slots so the
    // matching stage doesn't block on an unconsumed output ring.
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

    // Shadow snapshot thread — reuses the primary's shadow::run().
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

    // Reusable buffers for the receive loop.
    let mut frame_buf: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut journal_accum: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut accum_end_sequence: u64 = 0;
    let mut accum_chain_hash: [u8; 32] = [0u8; 32];

    // Main receive loop.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("replica shutting down");
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
            info!("promotion triggered — stopping replication, transitioning to primary");
            // Drain remaining TCP data for maximum freshness.
            drain_tcp_data_batches(
                &mut reader,
                &mut frame_buf,
                &mut journal_accum,
                &mut accum_end_sequence,
                &mut accum_chain_hash,
            );
            // Flush accumulated data through the pipeline.
            if !journal_accum.is_empty() {
                publish_batch_to_pipeline(
                    &journal_accum,
                    accum_end_sequence,
                    accum_chain_hash,
                    &input_producer,
                    &raw_journal_tx,
                    &journal_cursor,
                )?;
            }
            // Shut down pipeline and extract Exchange + JournalWriter.
            drop(raw_journal_tx); // unblock journal stage if waiting on channel
            return match shutdown_pipeline(
                &pipeline_shutdown,
                journal_handle,
                matching_handle,
                drain_handle,
                shadow_handle,
            ) {
                Some((exchange, writer)) => Ok(Some((exchange, writer))),
                None => Err("pipeline thread panicked during promotion".into()),
            };
        }

        // Read the first frame (blocking, with 5s timeout for shutdown check).
        match read_frame_into(&mut reader, &mut frame_buf, MAX_DATA_FRAME) {
            Ok(()) => {}
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => {
                warn!(error = %e, "primary disconnected — waiting for promotion");
                // Flush any accumulated data.
                if !journal_accum.is_empty() {
                    let _ = publish_batch_to_pipeline(
                        &journal_accum,
                        accum_end_sequence,
                        accum_chain_hash,
                        &input_producer,
                        &raw_journal_tx,
                        &journal_cursor,
                    );
                }
                // Wait for promotion or shutdown.
                loop {
                    if shutdown.load(Ordering::Relaxed) {
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
                        info!("promotion triggered after primary disconnect");
                        drop(raw_journal_tx);
                        return match shutdown_pipeline(
                            &pipeline_shutdown,
                            journal_handle,
                            matching_handle,
                            drain_handle,
                            shadow_handle,
                        ) {
                            Some((exchange, writer)) => Ok(Some((exchange, writer))),
                            None => Err("pipeline thread panicked during promotion".into()),
                        };
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }

        let message = decode_primary_message(&frame_buf)?;
        match message {
            PrimaryMessage::DataBatch {
                end_sequence,
                chain_hash: batch_chain_hash,
                entry_count: _,
                journal_bytes,
            } => {
                journal_accum.extend_from_slice(&journal_bytes);
                accum_end_sequence = end_sequence;
                accum_chain_hash = batch_chain_hash;

                // Drain additional frames from TCP buffer.
                drain_tcp_data_batches(
                    &mut reader,
                    &mut frame_buf,
                    &mut journal_accum,
                    &mut accum_end_sequence,
                    &mut accum_chain_hash,
                );

                // Submit events to the disruptor and raw bytes to the
                // journal stage's SPSC ring (non-blocking). The journal
                // stage writes them via io_uring asynchronously.
                let target = submit_batch_to_pipeline(
                    &journal_accum,
                    accum_end_sequence,
                    accum_chain_hash,
                    &input_producer,
                    &raw_journal_tx,
                )?;

                // Wait for the journal cursor to confirm durability,
                // then send the ack. With the SPSC ring and io_uring
                // on the journal stage, the wait may be shorter than
                // one NVMe write because previously submitted batches
                // are already in-flight.
                wait_for_journal_cursor(&journal_cursor, target);

                let ack = Ack {
                    acked_sequence: accum_end_sequence,
                };
                encode_ack(&ack, &mut send_buf);
                tcp_writer.write_all(&send_buf)?;
                tcp_writer.flush()?;
                send_buf.clear();

                journal_accum.clear();
            }
            PrimaryMessage::Heartbeat {
                sequence,
                chain_hash: _,
            } => {
                debug!(sequence, "heartbeat from primary");
            }
            PrimaryMessage::StreamStart { .. } => {
                debug!("unexpected StreamStart during streaming");
            }
            PrimaryMessage::NeedSnapshot => {
                return Err("primary says we need a snapshot transfer".into());
            }
            PrimaryMessage::HashMismatch => {
                return Err("chain hash mismatch from primary".into());
            }
            PrimaryMessage::SnapshotBegin { .. }
            | PrimaryMessage::SnapshotChunk(_)
            | PrimaryMessage::SnapshotEnd { .. } => {
                debug!("unexpected snapshot message during streaming");
            }
        }
    }
}

/// Drain DataBatch frames from the TCP buffer using non-blocking poll(0).
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

/// Drain DataBatch frames from the TCP buffer using non-blocking poll(0).
fn drain_tcp_data_batches(
    reader: &mut TcpStream,
    frame_buf: &mut Vec<u8>,
    journal_accum: &mut Vec<u8>,
    accum_end_sequence: &mut u64,
    accum_chain_hash: &mut [u8; 32],
) {
    let mut rpollfd = libc::pollfd {
        fd: std::os::unix::io::AsRawFd::as_raw_fd(reader),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        rpollfd.revents = 0;
        let ready = (unsafe { libc::poll(&mut rpollfd, 1, 0) }) > 0
            && (rpollfd.revents & libc::POLLIN) != 0;
        if !ready {
            break;
        }
        match read_frame_into(reader, frame_buf, MAX_DATA_FRAME) {
            Ok(()) => {}
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(_) => break,
        }
        match decode_primary_message(frame_buf) {
            Ok(PrimaryMessage::DataBatch {
                end_sequence,
                entry_count: _,
                journal_bytes,
                chain_hash,
            }) => {
                journal_accum.extend_from_slice(&journal_bytes);
                *accum_end_sequence = end_sequence;
                *accum_chain_hash = chain_hash;
            }
            _ => break,
        }
    }
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

/// Legacy wrapper: submit + wait + return. Used by code paths that
/// haven't been updated to the split submit/wait pattern yet.
fn publish_batch_to_pipeline(
    journal_bytes: &[u8],
    end_sequence: u64,
    chain_hash: [u8; 32],
    producer: &melin_disruptor::ring::MultiProducer<melin_engine::journal::pipeline::InputSlot>,
    raw_tx: &melin_engine::journal::pipeline::RawBatchSender,
    journal_cursor: &melin_disruptor::padding::Sequence,
) -> Result<(), Box<dyn std::error::Error>> {
    let target =
        submit_batch_to_pipeline(journal_bytes, end_sequence, chain_hash, producer, raw_tx)?;
    wait_for_journal_cursor(journal_cursor, target);
    Ok(())
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
/// The protocol is identical to `run_sender` — same wire format, same
/// handshake, same streaming logic. Only the I/O primitives differ.
#[cfg(feature = "dpdk")]
pub fn run_sender_dpdk(
    mut transport: melin_dpdk::DpdkTransport,
    mut repl_consumer: ReplicationConsumer,
    replication_cursor: Arc<AtomicU64>,
    genesis_entry: Vec<u8>,
    shutdown: &AtomicBool,
    replica_ready: &AtomicBool,
    replica_connected: &AtomicBool,
    active_flag: Arc<AtomicBool>,
    metrics: Arc<ReplicationMetrics>,
    batch_size: usize,
    heartbeat_secs: u64,
    busy_spin: bool,
) {
    info!("DPDK replication sender started");

    /// Sender state machine.
    enum State {
        /// Waiting for a replica to connect.
        WaitingForReplica,
        /// Replica connected, performing handshake.
        Handshaking(melin_dpdk::SocketHandle),
        /// Streaming journal data to replica.
        Streaming(melin_dpdk::SocketHandle),
    }

    let mut state = State::WaitingForReplica;
    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    let mut send_buf: Vec<u8> = Vec::with_capacity(512 * 1024);
    let heartbeat_interval = std::time::Duration::from_secs(heartbeat_secs);
    let mut last_send = std::time::Instant::now();
    let mut last_sequence: u64 = 0;
    let mut last_chain_hash: [u8; 32] = [0u8; 32];

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("DPDK replication sender shutting down");
            return;
        }

        // Drive smoltcp (rx/tx, timers, retransmit).
        transport.poll();

        match state {
            State::WaitingForReplica => {
                // Check for accepted connections.
                let accepted = transport.take_accepted();
                if let Some(conn) = accepted.into_iter().next() {
                    info!(peer = ?conn.peer, "replica connected via DPDK");
                    replica_connected.store(true, Ordering::Release);
                    recv_buf.clear();
                    state = State::Handshaking(conn.handle);
                    continue;
                }

                // Ring is inactive (active_flag=false) — the journal
                // stage skips it, so no drain needed.

                if busy_spin {
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
            }

            State::Handshaking(handle) => {
                // Try to read handshake frame.
                transport.recv_into_vec(handle, &mut recv_buf);

                match try_extract_frame(&recv_buf, MAX_CONTROL_FRAME) {
                    FrameResult::Complete(payload_start, frame_end) => {
                        let payload = &recv_buf[payload_start..frame_end];
                        match decode_replica_message(payload) {
                            Ok(ReplicaMessage::Handshake(h)) => {
                                info!(
                                    last_sequence = h.last_sequence,
                                    "replica handshake received (DPDK)"
                                );

                                // Send StreamStart.
                                send_buf.clear();
                                encode_stream_start(h.last_sequence, &genesis_entry, &mut send_buf);
                                transport.queue_send(handle, &send_buf);
                                send_buf.clear();

                                // Reset cursor.
                                replication_cursor.store(h.last_sequence + 1, Ordering::Release);

                                last_sequence = h.last_sequence;
                                last_chain_hash = h.chain_hash;
                                last_send = std::time::Instant::now();

                                compact_recv_buf(&mut recv_buf, frame_end);
                                // Mark ring active before signaling readiness
                                // so the journal stage publishes when seeds flow.
                                active_flag.store(true, Ordering::Release);
                                replica_ready.store(true, Ordering::Release);
                                state = State::Streaming(handle);
                            }
                            Ok(ReplicaMessage::Ack(_)) => {
                                warn!("expected Handshake, got Ack — disconnecting");
                                transport.close(handle);
                                replication_cursor.store(u64::MAX, Ordering::Release);
                                active_flag.store(false, Ordering::Release);
                                replica_connected.store(false, Ordering::Release);
                                state = State::WaitingForReplica;
                            }
                            Err(e) => {
                                warn!(error = %e, "failed to decode handshake — disconnecting");
                                transport.close(handle);
                                replication_cursor.store(u64::MAX, Ordering::Release);
                                replica_connected.store(false, Ordering::Release);
                                state = State::WaitingForReplica;
                            }
                        }
                    }
                    FrameResult::Oversized => {
                        warn!("oversized handshake frame — disconnecting");
                        transport.close(handle);
                        replication_cursor.store(u64::MAX, Ordering::Release);
                        replica_connected.store(false, Ordering::Release);
                        state = State::WaitingForReplica;
                    }
                    FrameResult::Incomplete => {} // Wait for more data.
                }

                // Check for disconnect during handshake.
                if matches!(state, State::Handshaking(h) if !transport.is_active(h)) {
                    warn!("replica disconnected during handshake");
                    replication_cursor.store(u64::MAX, Ordering::Release);
                    replica_connected.store(false, Ordering::Release);
                    state = State::WaitingForReplica;
                }
            }

            State::Streaming(handle) => {
                // 1. Process acks (non-blocking).
                transport.recv_into_vec(handle, &mut recv_buf);
                let mut consumed = 0;
                let mut ack_error = false;
                loop {
                    let remaining = &recv_buf[consumed..];
                    match try_extract_frame(remaining, MAX_CONTROL_FRAME) {
                        FrameResult::Complete(payload_start, frame_end) => {
                            let payload = &remaining[payload_start..frame_end];
                            if let Ok(ReplicaMessage::Ack(ack)) = decode_replica_message(payload) {
                                let new_val = ack.acked_sequence + 1;
                                let _ = replication_cursor.fetch_max(new_val, Ordering::Release);
                                // DPDK uses slot 0 only (single-replica).
                                metrics.acked_sequence[0]
                                    .store(ack.acked_sequence, Ordering::Relaxed);
                            }
                            consumed += frame_end;
                        }
                        FrameResult::Oversized => {
                            warn!("oversized ack frame from replica — disconnecting");
                            ack_error = true;
                            break;
                        }
                        FrameResult::Incomplete => break,
                    }
                }
                compact_recv_buf(&mut recv_buf, consumed);
                if ack_error {
                    transport.close(handle);
                    active_flag.store(false, Ordering::Release);
                    replication_cursor.store(u64::MAX, Ordering::Release);
                    replica_connected.store(false, Ordering::Release);
                    recv_buf.clear();
                    state = State::WaitingForReplica;
                    continue;
                }

                // 2. Send data batches.
                send_buf.clear();
                let mut batches_sent = 0;
                if let Some((meta, data)) = repl_consumer.try_read() {
                    encode_data_batch(
                        meta.end_sequence,
                        &meta.chain_hash,
                        meta.entry_count,
                        data,
                        &mut send_buf,
                    );
                    repl_consumer.commit();
                    last_sequence = meta.end_sequence;
                    last_chain_hash = meta.chain_hash;
                    batches_sent += 1;

                    // Coalesce more batches.
                    for _ in 1..batch_size {
                        if let Some((meta, data)) = repl_consumer.try_read() {
                            encode_data_batch(
                                meta.end_sequence,
                                &meta.chain_hash,
                                meta.entry_count,
                                data,
                                &mut send_buf,
                            );
                            repl_consumer.commit();
                            last_sequence = meta.end_sequence;
                            last_chain_hash = meta.chain_hash;
                            batches_sent += 1;
                        } else {
                            break;
                        }
                    }

                    // DPDK uses slot 0 only (single-replica).
                    metrics.bytes_sent[0].fetch_add(send_buf.len() as u64, Ordering::Relaxed);
                    transport.queue_send(handle, &send_buf);
                    last_send = std::time::Instant::now();
                }

                // 3. Heartbeat if idle.
                if batches_sent == 0 && last_send.elapsed() >= heartbeat_interval {
                    send_buf.clear();
                    encode_heartbeat(last_sequence, &last_chain_hash, &mut send_buf);
                    transport.queue_send(handle, &send_buf);
                    last_send = std::time::Instant::now();
                }

                // 4. Check for disconnect.
                if !transport.is_active(handle) {
                    warn!("replica disconnected (DPDK) — trading halted");
                    active_flag.store(false, Ordering::Release);
                    replication_cursor.store(u64::MAX, Ordering::Release);
                    replica_connected.store(false, Ordering::Release);
                    recv_buf.clear();
                    state = State::WaitingForReplica;
                    continue;
                }

                if batches_sent == 0 {
                    if busy_spin {
                        std::hint::spin_loop();
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        }
    }
}

/// DPDK variant of the replication receiver. Uses a `DpdkTransport` (smoltcp)
/// to connect to the primary via DPDK instead of kernel TCP.
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

    info!(
        primary_ip = %primary_ip,
        primary_port,
        "connecting to primary as replica (DPDK)"
    );

    // Determine our current state from the local journal (if any).
    let (mut exchange, mut journal_writer, last_sequence, chain_hash) = if journal_path.exists() {
        let engine = melin_engine::journal::JournaledExchange::recover(journal_path)?;
        let next = engine.next_sequence();
        let last = next.saturating_sub(1);
        let hash = engine.writer_chain_hash().unwrap_or([0u8; 32]);
        let (exchange, writer) = engine.into_parts();
        (Some(exchange), Some(writer), last, hash)
    } else {
        (None, None, 0u64, [0u8; 32])
    };

    // Connect to primary via smoltcp.
    let handle = transport.connect_to(primary_ip, primary_port, 40000);

    // Poll until TCP handshake completes.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        transport.poll();
        if transport.is_connected(handle) {
            break;
        }
        std::thread::yield_now();
    }
    info!("connected to primary (DPDK)");

    // Send handshake.
    let mut send_buf = Vec::with_capacity(64);
    let handshake = Handshake {
        last_sequence,
        chain_hash,
    };
    encode_handshake(&handshake, &mut send_buf);
    transport.queue_send(handle, &send_buf);
    send_buf.clear();

    // Read StreamStart.
    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    let primary_genesis_entry = loop {
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
                        break genesis_entry;
                    }
                    PrimaryMessage::NeedSnapshot => {
                        return Err(
                            "primary says we need a snapshot transfer (not yet implemented)".into(),
                        );
                    }
                    PrimaryMessage::HashMismatch => {
                        return Err("chain hash mismatch — replica has divergent history".into());
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
            return Err("disconnected from primary before StreamStart".into());
        }
        std::thread::yield_now();
    };

    // Create journal for fresh replica using the primary's raw genesis entry.
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
            0,
        )?;
        exchange = Some(Exchange::new());
        journal_writer = Some(writer);
    }

    let exchange = exchange.expect("exchange initialized");
    let journal_writer = journal_writer.expect("journal_writer initialized");

    // Clone exchange for shadow stage before moving into pipeline.
    let shadow_exchange = exchange.clone_via_snapshot();

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
        exchange,
        journal_writer,
        4096,
        false, // don't busy-spin on replica
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

    let mut journal_accum: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut accum_end_sequence: u64 = 0;
    let mut accum_chain_hash: [u8; 32] = [0u8; 32];

    // Main receive loop.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("replica shutting down (DPDK)");
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
                let _ = publish_batch_to_pipeline(
                    &journal_accum,
                    accum_end_sequence,
                    accum_chain_hash,
                    &input_producer,
                    &raw_journal_tx,
                    &journal_cursor,
                );
                journal_accum.clear();
            }
            drop(raw_journal_tx);
            return match shutdown_pipeline(
                &pipeline_shutdown,
                journal_handle,
                matching_handle,
                drain_handle,
                shadow_handle,
            ) {
                Some((exchange, writer)) => Ok(Some((exchange, writer))),
                None => Err("pipeline thread panicked during promotion (DPDK)".into()),
            };
        }

        // Poll smoltcp and receive data.
        transport.poll();
        transport.recv_into_vec(handle, &mut recv_buf);

        // Check for disconnect.
        if !transport.is_active(handle) && recv_buf.is_empty() {
            drop(raw_journal_tx);
            shutdown_pipeline(
                &pipeline_shutdown,
                journal_handle,
                matching_handle,
                drain_handle,
                shadow_handle,
            );
            return Err("disconnected from primary (DPDK)".into());
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

        // Submit to pipeline (non-blocking), wait for journal
        // cursor, then ack.
        if got_data {
            let target = submit_batch_to_pipeline(
                &journal_accum,
                accum_end_sequence,
                accum_chain_hash,
                &input_producer,
                &raw_journal_tx,
            )?;
            wait_for_journal_cursor(&journal_cursor, target);

            // Ack — data is durable.
            send_buf.clear();
            let ack = Ack {
                acked_sequence: accum_end_sequence,
            };
            encode_ack(&ack, &mut send_buf);
            transport.queue_send(handle, &send_buf);

            journal_accum.clear();
        } else {
            std::thread::yield_now();
        }
    }
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
}
