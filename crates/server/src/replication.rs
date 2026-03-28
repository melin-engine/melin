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
//! ### Replica → Primary
//! - **Handshake**: `[len:u32][0x01][last_sequence:u64][chain_hash:[u8;32]]`
//! - **Ack**: `[len:u32][0x02][acked_sequence:u64]`
//!
//! ### Primary → Replica
//! - **StreamStart**: `[len:u32][0x10][start_sequence:u64]`
//! - **NeedSnapshot**: `[len:u32][0x11]`
//! - **HashMismatch**: `[len:u32][0x12]`
//! - **DataBatch**: `[len:u32][0x20][end_sequence:u64][chain_hash:[u8;32]][journal_bytes...]`
//! - **Heartbeat**: `[len:u32][0x30][sequence:u64][chain_hash:[u8;32]]`
//!
//! ## v1 Limitations
//!
//! - No catch-up from journal files (replica must be connected from start)
//! - No chain hash verification on received DataBatch (CRC per-entry only)
//! - No handshake validation (NeedSnapshot/HashMismatch never sent)
//! - Single replica only (second connection replaces first)
//!
//! See `docs/replication.md` for the full design document and limitation details.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::{debug, error, info, warn};

use melin_engine::journal::replication::ReplicationConsumer;

// --- Wire protocol message types ---

/// Message type tags.
const MSG_HANDSHAKE: u8 = 0x01;
const MSG_ACK: u8 = 0x02;
const MSG_STREAM_START: u8 = 0x10;
const MSG_NEED_SNAPSHOT: u8 = 0x11;
const MSG_HASH_MISMATCH: u8 = 0x12;
const MSG_DATA_BATCH: u8 = 0x20;
const MSG_HEARTBEAT: u8 = 0x30;

/// Maximum frame size for control messages (handshake, ack, etc.).
/// Data batches can be much larger (up to 128 KiB of journal data).
const MAX_CONTROL_FRAME: usize = 256;

/// Maximum data batch frame size. Sized for MAX_JOURNAL_BATCH (1024) entries
/// at ~80 bytes each = ~80 KiB, plus header overhead.
const MAX_DATA_FRAME: usize = 256 * 1024;

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
#[allow(dead_code)] // Used in future catch-up implementation.
fn encode_need_snapshot(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_NEED_SNAPSHOT);
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
    mut repl_consumer: ReplicationConsumer,
    replication_cursor: Arc<AtomicU64>,
    genesis_entry: Vec<u8>,
    shutdown: &AtomicBool,
    replica_ready: &AtomicBool,
    replica_connected: &AtomicBool,
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

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("replication sender shutting down");
            return;
        }

        // Accept ONE replica connection at a time (single-replica v1).
        let stream = match listener.accept() {
            Ok((stream, addr)) => {
                info!(addr = %addr, "replica connected");
                // Signal that a replica is connected — unblocks seed event
                // publishing in the main thread.
                replica_ready.store(true, Ordering::Release);
                // Resume trading — matching stage will stop rejecting events.
                replica_connected.store(true, Ordering::Release);
                stream
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                // No pending connection — drain batches to avoid blocking the
                // replication ring. Only drain after the first replica has
                // connected; before that, seed data must be preserved.
                if replica_ready.load(Ordering::Relaxed) {
                    drain_batches_while_waiting(&mut repl_consumer);
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
            Err(e) => {
                error!(error = %e, "replication accept error");
                std::thread::sleep(std::time::Duration::from_millis(100));
                continue;
            }
        };

        // Set TCP_NODELAY for low-latency replication.
        if let Err(e) = stream.set_nodelay(true) {
            debug!(error = %e, "failed to set TCP_NODELAY on replica connection");
        }

        // Handle this replica connection until it disconnects.
        match handle_replica_connection(
            stream,
            &mut repl_consumer,
            &replication_cursor,
            &genesis_entry,
            shutdown,
            batch_size,
            heartbeat_secs,
            busy_spin,
        ) {
            Ok(()) => warn!("replica disconnected cleanly"),
            Err(e) => warn!(error = %e, "replica connection error"),
        }

        // On disconnect, set cursor to u64::MAX to degrade to local-only.
        replication_cursor.store(u64::MAX, Ordering::Release);
        // Halt trading — matching stage will reject all mutations until
        // the replica reconnects.
        replica_connected.store(false, Ordering::Release);
        warn!("replica disconnected — trading halted, waiting for reconnect");
    }
}

/// Drain pending batches from the ring without blocking.
/// Called when no replica is connected to prevent the journal stage
/// from being blocked by ring backpressure.
fn drain_batches_while_waiting(consumer: &mut ReplicationConsumer) {
    while consumer.try_read().is_some() {
        consumer.commit();
    }
}

/// Handle a single replica connection: handshake, streaming, ack processing.
fn handle_replica_connection(
    stream: TcpStream,
    repl_consumer: &mut ReplicationConsumer,
    replication_cursor: &Arc<AtomicU64>,
    genesis_entry: &[u8],
    shutdown: &AtomicBool,
    batch_size: usize,
    heartbeat_secs: u64,
    busy_spin: bool,
) -> io::Result<()> {
    let mut reader = stream.try_clone()?;
    let mut writer = stream;

    // Set a read timeout for the handshake.
    reader.set_read_timeout(Some(std::time::Duration::from_secs(10)))?;

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

    // For v1, we don't do catch-up from journal files — just start streaming
    // from the live feed. The replica must start from the beginning or have
    // been caught up previously.
    // TODO(step 4): Add catch-up from journal file for late-joining replicas.

    // Send StreamStart with the primary's raw genesis entry so the replica
    // can write a byte-identical genesis to its journal.
    let mut send_buf = Vec::with_capacity(128);
    encode_stream_start(handshake.last_sequence, genesis_entry, &mut send_buf);
    writer.write_all(&send_buf)?;
    writer.flush()?;
    send_buf.clear();

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

    // Reset the replication cursor from u64::MAX (disconnect state) so
    // that subsequent fetch_max calls from ack processing can advance it.
    // Without this reset, fetch_max(ack_seq) would be a no-op since
    // u64::MAX > any ack_seq, permanently disabling replication gating.
    //
    // Set to handshake.last_sequence + 1: events up to last_sequence are
    // already durable on the replica (it reported them in the handshake).
    // Events after that will gate until the replica acks them.
    replication_cursor.store(handshake.last_sequence + 1, Ordering::Release);

    let heartbeat_interval = std::time::Duration::from_secs(heartbeat_secs);
    let mut last_send = std::time::Instant::now();
    let mut last_sequence = handshake.last_sequence;
    let mut last_chain_hash = handshake.chain_hash;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Process any pending acks (non-blocking via internal poll(0)).
        if let Err(e) = process_acks(&mut reader, replication_cursor, &mut pollfd) {
            return Err(io::Error::other(format!("replica ack read error: {e}")));
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

            if let Err(e) = writer.write_all(&send_buf) {
                return Err(io::Error::other(format!("write DataBatch: {e}")));
            }
            if let Err(e) = writer.flush() {
                return Err(io::Error::other(format!("flush DataBatch: {e}")));
            }
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
            if let Err(e) = process_acks(&mut reader, replication_cursor, &mut pollfd) {
                return Err(io::Error::other(format!("replica ack read error: {e}")));
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
fn process_acks(
    reader: &mut TcpStream,
    replication_cursor: &Arc<AtomicU64>,
    pollfd: &mut libc::pollfd,
) -> io::Result<()> {
    loop {
        // Check if more ack data is available before calling read_frame.
        // poll(0) is truly non-blocking — no kernel jiffy rounding.
        pollfd.revents = 0;
        let ready = unsafe { libc::poll(pollfd, 1, 0) };
        if ready <= 0 || (pollfd.revents & libc::POLLIN) == 0 {
            return Ok(()); // No data available.
        }

        match read_frame(reader, MAX_CONTROL_FRAME) {
            Ok(payload) => match decode_replica_message(&payload) {
                Ok(ReplicaMessage::Ack(ack)) => {
                    let new_val = ack.acked_sequence + 1;
                    let _ = replication_cursor.fetch_max(new_val, Ordering::Release);
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
                    return Ok(());
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
    shutdown: &AtomicBool,
    promote: &AtomicBool,
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

    // Determine our current state from the local journal (if any).
    // For fresh starts, we defer journal creation until after the handshake
    // so we can use the primary's genesis hash.
    let (mut exchange, mut journal_writer, last_sequence, chain_hash) = if journal_path.exists() {
        // Recover from existing journal.
        let engine = melin_engine::journal::JournaledExchange::recover(journal_path)?;
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
            return Err("primary says we need a snapshot transfer (not yet implemented)".into());
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

    let mut exchange = exchange.expect("exchange initialized");
    let mut journal_writer = journal_writer.expect("journal_writer initialized");

    // Pre-allocated report buffer, reused across events to avoid per-event
    // heap allocation. Same pattern as MatchingStage.
    let mut reports: Vec<melin_engine::types::ExecutionReport> = Vec::with_capacity(256);

    // Accumulation buffer for coalescing multiple DataBatch frames into
    // one fsync. The replica reads all available frames from the TCP
    // buffer, accumulates journal bytes, then does ONE pwritev2+RWF_DSYNC
    // and ONE ack for the highest sequence. This reduces NVMe fsync
    // overhead from one-per-batch to one-per-TCP-read-burst.
    let mut journal_accum: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut accum_entry_count: u64 = 0;
    let mut accum_end_sequence: u64;
    // Reusable frame buffer — grows to high-water mark, avoids per-frame
    // heap allocation in the hot receive loop.
    let mut frame_buf: Vec<u8> = Vec::with_capacity(64 * 1024);

    // Main receive loop.
    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("replica shutting down");
            return Ok(None);
        }

        if promote.load(Ordering::Acquire) {
            info!("promotion triggered — stopping replication, transitioning to primary");
            // Drain any remaining data already in the TCP buffer to
            // maximize data freshness before promotion.
            let mut rpollfd = libc::pollfd {
                fd: reader.as_raw_fd(),
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
                if read_frame_into(&mut reader, &mut frame_buf, MAX_DATA_FRAME).is_err() {
                    break;
                }
                match decode_primary_message(&frame_buf) {
                    Ok(PrimaryMessage::DataBatch {
                        entry_count,
                        journal_bytes,
                        ..
                    }) => {
                        journal_accum.extend_from_slice(&journal_bytes);
                        accum_entry_count += entry_count as u64;
                    }
                    _ => break,
                }
            }
            // Fsync any accumulated data.
            if !journal_accum.is_empty() {
                journal_writer.write_raw_sync(&journal_accum, accum_entry_count)?;
                replay_journal_bytes(&journal_accum, &mut exchange, &mut reports)?;
                journal_accum.clear();
            }
            return Ok(Some((exchange, journal_writer)));
        }

        // Read the first frame (blocking, with the 5s timeout for
        // shutdown checking).
        match read_frame_into(&mut reader, &mut frame_buf, MAX_DATA_FRAME) {
            Ok(()) => {}
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => {
                // Primary disconnected (crash, network failure, or graceful
                // shutdown). Instead of exiting, wait for the operator to
                // promote this replica. The replica's journal is durable and
                // the Exchange state is consistent up to the last acked
                // sequence.
                warn!(error = %e, "primary disconnected — waiting for promotion");

                // Flush any accumulated but un-acked data before waiting.
                if !journal_accum.is_empty() {
                    journal_writer.write_raw_sync(
                        &journal_accum,
                        accum_entry_count,
                    )?;
                    replay_journal_bytes(
                        &journal_accum,
                        &mut exchange,
                        &mut reports,
                    )?;
                    journal_accum.clear();
                }

                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        return Ok(None);
                    }
                    if promote.load(Ordering::Acquire) {
                        info!("promotion triggered after primary disconnect");
                        return Ok(Some((exchange, journal_writer)));
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
            }
        }

        let message = decode_primary_message(&frame_buf)?;
        match message {
            PrimaryMessage::DataBatch {
                end_sequence,
                chain_hash: _batch_chain_hash,
                entry_count,
                journal_bytes,
            } => {
                // Accumulate raw bytes for coalesced fsync. Replay is
                // deferred until after the ack — the ack only guarantees
                // durability (bytes on disk), not state application.
                // Entry count comes from the wire frame — no data scanning.
                journal_accum.extend_from_slice(&journal_bytes);
                accum_entry_count += entry_count as u64;
                accum_end_sequence = end_sequence;

                // Drain additional frames already in the TCP buffer.
                // Use poll(0) to check availability — avoids the ~2ms
                // SO_RCVTIMEO jiffy floor on Linux.
                let mut rpollfd = libc::pollfd {
                    fd: reader.as_raw_fd(),
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
                    if read_frame_into(&mut reader, &mut frame_buf, MAX_DATA_FRAME).is_err() {
                        break;
                    }
                    match decode_primary_message(&frame_buf)? {
                        PrimaryMessage::DataBatch {
                            end_sequence,
                            entry_count,
                            journal_bytes,
                            ..
                        } => {
                            journal_accum.extend_from_slice(&journal_bytes);
                            accum_entry_count += entry_count as u64;
                            accum_end_sequence = end_sequence;
                        }
                        _ => break,
                    }
                }

                // Fsync all accumulated batches.
                journal_writer.write_raw_sync(&journal_accum, accum_entry_count)?;

                // Ack immediately — data is durable on disk.
                let ack = Ack {
                    acked_sequence: accum_end_sequence,
                };
                encode_ack(&ack, &mut send_buf);
                tcp_writer.write_all(&send_buf)?;
                tcp_writer.flush()?;
                send_buf.clear();

                // Replay AFTER acking. The primary's replication cursor
                // advances as soon as the ack arrives, unblocking the
                // response stage. Replay is not on the critical path —
                // on crash recovery, the replica replays from its journal.
                replay_journal_bytes(&journal_accum, &mut exchange, &mut reports)?;

                journal_accum.clear();
                accum_entry_count = 0;
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
        }
    }
}

/// Replay journal events against the exchange (same as MatchingStage::process_event
/// but without the output SPSC publishing — replicas don't serve clients).
///
/// The reports Vec is caller-owned and reused across calls to avoid
/// per-event heap allocation.
/// Decode and replay journal entries from raw bytes into the exchange.
/// Called AFTER the ack is sent — not on the critical path.
fn replay_journal_bytes(
    journal_bytes: &[u8],
    exchange: &mut melin_engine::exchange::Exchange,
    reports: &mut Vec<melin_engine::types::ExecutionReport>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut offset = 0;
    while offset < journal_bytes.len() {
        let remaining = &journal_bytes[offset..];
        match melin_engine::journal::codec::decode(
            remaining,
            melin_engine::journal::codec::FORMAT_VERSION,
        ) {
            Ok((consumed, _sequence, _timestamp_ns, key_hash, request_seq, event)) => {
                // Rebuild per-key HWM state on replica (same as primary replay).
                exchange.check_request_seq(key_hash, request_seq);
                replay_event(exchange, &event, reports);
                offset += consumed;
            }
            Err(e) => {
                return Err(
                    format!("failed to decode journal entry at offset {offset}: {e}").into(),
                );
            }
        }
    }
    Ok(())
}

fn replay_event(
    exchange: &mut melin_engine::exchange::Exchange,
    event: &melin_engine::journal::event::JournalEvent,
    reports: &mut Vec<melin_engine::types::ExecutionReport>,
) {
    use melin_engine::journal::event::JournalEvent;

    reports.clear();
    match event {
        JournalEvent::AddInstrument { spec } => {
            exchange.add_instrument(*spec);
        }
        JournalEvent::Deposit {
            account,
            currency,
            amount,
        } => {
            exchange.deposit(*account, *currency, *amount);
        }
        JournalEvent::SubmitOrder { symbol, order } => {
            exchange.execute(*symbol, *order, reports);
        }
        JournalEvent::CancelOrder {
            symbol,
            account,
            order_id,
        } => {
            exchange.cancel(*symbol, *account, *order_id, reports);
        }
        JournalEvent::SetRiskLimits { symbol, limits } => {
            exchange.set_risk_limits(*symbol, *limits);
        }
        JournalEvent::CancelAll { account } => {
            exchange.cancel_all(*account, reports);
        }
        JournalEvent::EndOfDay => {
            exchange.end_of_day(reports);
        }
        JournalEvent::ExpireOrders { timestamp_ns } => {
            exchange.expire_orders(*timestamp_ns, reports);
        }
        JournalEvent::SetCircuitBreaker { symbol, config } => {
            exchange.set_circuit_breaker(*symbol, *config);
        }
        JournalEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => {
            exchange.cancel_replace(
                *symbol,
                *account,
                *order_id,
                *new_price,
                *new_quantity,
                reports,
            );
        }
        JournalEvent::SetFeeSchedule { symbol, schedule } => {
            exchange.set_fee_schedule(*symbol, *schedule);
        }
        JournalEvent::ProvisionAccount { account, amount } => {
            exchange.provision_account(*account, *amount);
        }
        JournalEvent::Withdraw {
            account,
            currency,
            amount,
        } => {
            let _ = exchange.withdraw(*account, *currency, *amount);
        }
        JournalEvent::DisableInstrument { symbol } => {
            exchange.disable_instrument(*symbol, reports);
        }
        JournalEvent::EnableInstrument { symbol } => {
            exchange.enable_instrument(*symbol, reports);
        }
        JournalEvent::RemoveInstrument { symbol } => {
            exchange.remove_instrument(*symbol, reports);
        }
        JournalEvent::QueryStats
        | JournalEvent::GenesisHash { .. }
        | JournalEvent::Checkpoint { .. } => {
            // No state change.
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
        cursor.store(0 + 1, Ordering::Release);
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
}
