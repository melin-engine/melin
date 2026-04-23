//! DPDK replication transport — smoltcp-based sender and receiver paths.
//!
//! Mirrors the kernel-TCP variants in `mod.rs` but uses `DpdkTransport`
//! (a `smoltcp` socket over DPDK queue pairs) instead of `TcpStream`.
//! The wire protocol is identical — see `protocol.rs` for the message catalogue.

#![cfg(feature = "dpdk")]

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use tracing::{debug, error, info, warn};

use melin_journal::replication::ReplicationConsumer;

use super::catchup::{can_catch_up_from_journal, discover_journal_files};
use super::protocol::{
    Ack, Handshake, MAX_CONTROL_FRAME, MAX_DATA_FRAME, PrimaryMessage, ReplicaMessage,
    decode_primary_message, decode_replica_message, encode_ack, encode_data_batch,
    encode_handshake, encode_heartbeat, encode_need_snapshot, encode_snapshot_begin,
    encode_snapshot_chunk, encode_snapshot_end, encode_stream_start, try_decode_data_batch,
};
use super::{
    PendingAckQueue, ReceiverResult, ReplicationMetrics, shutdown_pipeline, sleep_checking_flags,
    submit_batch_to_pipeline, update_dual_replication_cursor,
};

enum FrameResult {
    /// Complete frame found: payload starts at index 0, frame ends at index 1.
    Complete(usize, usize),
    /// Not enough data for a complete frame — wait for more.
    Incomplete,
    /// Frame exceeds max_size or is malformed — connection should be dropped.
    Oversized,
}

/// Try to extract one length-prefixed frame from a receive buffer.
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
/// The protocol is identical to `run_sender` — same wire format, same
/// handshake, same streaming logic. Only the I/O primitives differ.
///
/// Top-level thread entry point — the wide arg list mirrors what the
/// shared replication state owns and would re-export through any wrapper
/// struct, so a config struct adds indirection without simplifying.
#[allow(clippy::too_many_arguments)]
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
                // Drop any unread ring entries so a reconnecting replica
                // on this slot doesn't replay pre-eviction data and stall
                // the primary's replication cursor. See kernel-TCP path
                // in tcp_sender.rs for the detailed rationale.
                slot.consumer.skip_to_producer();
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
                                                _data,
                                                &mut slot.send_buf,
                                            );
                                            slot.consumer.commit();
                                            transport.queue_send(handle, &slot.send_buf);
                                            slot.send_buf.clear();
                                            slot.last_sequence = meta.end_sequence;
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
                        encode_data_batch(meta.end_sequence, data, &mut slot.send_buf);
                        slot.consumer.commit();
                        slot.last_sequence = meta.end_sequence;
                        batches_sent += 1;

                        // Coalesce more batches.
                        for _ in 1..batch_size {
                            if let Some((meta, data)) = slot.consumer.try_read() {
                                encode_data_batch(meta.end_sequence, data, &mut slot.send_buf);
                                slot.consumer.commit();
                                slot.last_sequence = meta.end_sequence;
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
                        encode_heartbeat(slot.last_sequence, &mut slot.send_buf);
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
fn catch_up_from_journal_dpdk(
    journal_path: &std::path::Path,
    last_sequence: u64,
    handle: melin_dpdk::SocketHandle,
    transport: &mut melin_dpdk::DpdkTransport,
    send_buf: &mut Vec<u8>,
    shutdown: &AtomicBool,
) -> std::io::Result<()> {
    use melin_journal::RawJournalScanner;

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

            let Some(batch_end_seq) = batch else {
                break;
            };

            send_buf.clear();
            encode_data_batch(batch_end_seq, &batch_buf, send_buf);
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
    use crate::App;
    use crate::JournalWriter;

    // Recover local state from journal (if any). On first call this may
    // be (None, None) for a fresh replica. After a reconnect, the pipeline
    // shutdown returns the App + JournalWriter directly.
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        if journal_path.exists() {
            let engine = if snapshot_path.exists() {
                info!("recovering replica from snapshot + journal (DPDK)");
                melin_transport_core::JournaledApp::<App>::recover_from_snapshot(
                    &snapshot_path,
                    journal_path,
                )?
            } else {
                melin_transport_core::JournaledApp::<App>::recover(
                    crate::server::empty_app(),
                    journal_path,
                )?
            };
            let next = engine.next_sequence();
            let last = next.saturating_sub(1);
            let hash = engine.chain_hash().unwrap_or([0u8; 32]);
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

        // Seed the primary's MAC into smoltcp's neighbor cache. Without
        // this, smoltcp emits a broadcast ARP on connect which the SR-IOV
        // PF silently drops, and the SYN never goes out — the replica
        // spins on "failed to connect (DPDK)" forever. VF MACs follow the
        // 02:00:<IP-bytes> convention set by dpdk-setup-sriov.sh, matching
        // what the bench client does on its outbound connect.
        let primary_mac = [
            0x02,
            0x00,
            primary_ip.octets()[0],
            primary_ip.octets()[1],
            primary_ip.octets()[2],
            primary_ip.octets()[3],
        ];
        transport.seed_neighbor(primary_ip, primary_mac);
        // Drain the injected ARP reply through smoltcp so the neighbor
        // cache is populated BEFORE connect_to() runs. Without this poll
        // smoltcp's connect tries to resolve ARP from an empty cache,
        // queues a broadcast request that the PF drops, and the SYN
        // never ships.
        transport.poll();

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
                            // App and JournalWriter — their underlying files
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
            use melin_journal::codec::{self as journal_codec, FILE_HEADER_SIZE};
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
            exchange = Some(crate::server::empty_app());
            journal_writer = Some(writer);
        }

        // If we still have no state after all the handshake logic, reconnect.
        if exchange.is_none() || journal_writer.is_none() {
            continue;
        }

        let cur_exchange = exchange.take().expect("exchange initialized");
        let cur_writer = journal_writer.take().expect("journal_writer initialized");

        // Clone exchange for shadow stage before moving into pipeline.
        let shadow_exchange = <App as melin_app::Application>::clone_via_snapshot(&cur_exchange)?;

        // Build the replica pipeline — same as the TCP receiver.
        let enable_shadow = snapshot_interval_secs > 0;
        let pipeline = melin_transport_core::pipeline::build_replica_pipeline(
            cur_exchange,
            cur_writer,
            4096,
            false,
            enable_shadow,
        );
        let mut input_producer = pipeline.input_producer;
        let journal_stage = pipeline.journal_stage;
        let matching_stage = pipeline.matching_stage;
        let drain_consumer = pipeline.drain_consumer;
        let journal_cursor = pipeline.journal_cursor;
        let shadow_consumer = pipeline.shadow_consumer;
        let chain_hash_lock = pipeline.chain_hash_lock;

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
                let mut batch = vec![crate::OutputSlot::default(); 256];
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
                                // Fast path: borrowed decoder avoids the Vec
                                // allocation on steady-state DataBatch frames.
                                // Mirrors the io_uring receiver path.
                                if let Some((end_sequence, journal_bytes)) =
                                    try_decode_data_batch(&remaining[ps..fe])
                                {
                                    journal_accum.extend_from_slice(journal_bytes);
                                    accum_end_sequence = end_sequence;
                                }
                                consumed += fe;
                            }
                            _ => break,
                        }
                    }
                    compact_recv_buf(&mut recv_buf, consumed);
                }
                if !journal_accum.is_empty() {
                    if let Ok(target) =
                        submit_batch_to_pipeline(&journal_accum, &mut input_producer)
                    {
                        pending_acks.push(target, accum_end_sequence);
                    }
                    journal_accum.clear();
                }
                if let Some(seq) = pending_acks.pop_all_blocking(&journal_cursor) {
                    send_ack_dpdk!(seq);
                    transport.poll();
                }
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
                        // Fast path: borrowed DataBatch decoder avoids the
                        // per-batch Vec allocation that used to dominate the
                        // DPDK replica's CPU profile under load.
                        if let Some((end_sequence, journal_bytes)) = try_decode_data_batch(payload)
                        {
                            journal_accum.extend_from_slice(journal_bytes);
                            accum_end_sequence = end_sequence;
                            got_data = true;
                            received_data = true;
                        } else {
                            match decode_primary_message(payload) {
                                Ok(PrimaryMessage::Heartbeat { sequence }) => {
                                    debug!(sequence, "heartbeat from primary (DPDK)");
                                }
                                Ok(PrimaryMessage::DataBatch { .. }) => {
                                    // try_decode_data_batch rejected this frame
                                    // as too short; the general decoder should
                                    // have surfaced it as Err. Treat as a
                                    // protocol violation.
                                    warn!("malformed DataBatch slipped past fast path (DPDK)");
                                    shutdown_pipeline(
                                        &pipeline_shutdown,
                                        journal_handle,
                                        matching_handle,
                                        drain_handle,
                                        shadow_handle,
                                    );
                                    return Err("malformed DataBatch".into());
                                }
                                Ok(other) => {
                                    debug!("unexpected message during streaming: {other:?}");
                                }
                                Err(e) => {
                                    shutdown_pipeline(
                                        &pipeline_shutdown,
                                        journal_handle,
                                        matching_handle,
                                        drain_handle,
                                        shadow_handle,
                                    );
                                    return Err(
                                        format!("failed to decode primary message: {e}").into()
                                    );
                                }
                            }
                        }
                        consumed += frame_end;
                    }
                    FrameResult::Oversized => {
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
                let target = submit_batch_to_pipeline(&journal_accum, &mut input_producer)?;

                pending_acks.push(target, accum_end_sequence);
                journal_accum.clear();
            } else {
                std::thread::yield_now();
            }
        };

        // --- Disconnect handling: recover state and reconnect ---
        let _disconnected = session_exit; // false = disconnected

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
                    match melin_transport_core::JournaledApp::<App>::recover(
                        crate::server::empty_app(),
                        journal_path,
                    ) {
                        Ok(engine) => {
                            last_sequence = engine.next_sequence().saturating_sub(1);
                            chain_hash = engine.chain_hash().unwrap_or([0u8; 32]);
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
/// Returns the loaded App, snapshot sequence, and chain hash.
fn receive_snapshot_dpdk(
    handle: melin_dpdk::SocketHandle,
    transport: &mut melin_dpdk::DpdkTransport,
    recv_buf: &mut Vec<u8>,
    snapshot_path: &std::path::Path,
    shutdown: &AtomicBool,
) -> Result<(crate::App, u64, [u8; 32]), Box<dyn std::error::Error + Send + Sync>> {
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
        melin_transport_core::snapshot::load::<crate::App>(snapshot_path)?;
    if snap_hash != snap_chain_hash {
        return Err(format!(
            "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
             loaded snapshot has {snap_hash:02x?}"
        )
        .into());
    }

    Ok((snap_exchange, snap_sequence, snap_chain_hash))
}
