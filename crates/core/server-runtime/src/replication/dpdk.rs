//! DPDK replication transport — smoltcp-based sender and receiver paths.
//!
//! Mirrors the kernel-TCP variants in `mod.rs` but uses `DpdkTransport`
//! (a `smoltcp` socket over DPDK queue pairs) instead of `TcpStream`.
//! The wire protocol is identical — see `protocol.rs` for the message catalogue.

#![cfg(feature = "dpdk")]

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use tracing::{debug, info, warn};

use melin_app::Application;
use melin_journal::JournalWrite;
use melin_journal::replication::ReplicationConsumer;
use melin_transport_core::pipeline::{JournalStage, JournalStageRun};

use super::{
    PendingAckQueue, ReceiverResult, ReplicaPipelineHandles, ReplicationMetrics,
    build_replica_pipeline_with_threads, sleep_checking_flags, teardown_replica_pipeline,
    try_flush_dual_track, update_dual_replication_cursor,
};
use melin_transport_core::replication::catchup::{
    can_catch_up_from_journal, discover_journal_files,
};
use melin_transport_core::replication::protocol::{
    Ack, Handshake, MAX_CONTROL_FRAME, MAX_DATA_FRAME, PrimaryMessage, ReplicaMessage,
    decode_primary_message, decode_replica_message, encode_ack, encode_handshake, encode_heartbeat,
    encode_need_snapshot, encode_snapshot_begin, encode_snapshot_chunk, encode_snapshot_end,
    encode_stream_start, try_decode_input_batch,
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
    let len = u32::from_le_bytes(
        buf[0..4]
            .try_into()
            .expect("bounds checked: buf has at least 4 bytes"),
    ) as usize;
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

// ---------------------------------------------------------------------------
// Recv-cycle frame processing
//
// The streaming and promotion-drain inner loops both extract length-prefixed
// frames from a recv buffer, decode each as either an `InputBatch` or a
// primary control message, and publish the resulting `InputSlot`s into the
// replica's input ring under a single per-recv `Producer::batch`. Pulling
// that logic into pure helpers lets the receiver code stay focused on
// session lifecycle (handshake, acks, exit) and — more importantly — lets
// the `pending_accum`-shadow invariant be exercised by unit tests without
// real DPDK hardware. See the `tests` module at the bottom of this file.
// ---------------------------------------------------------------------------

/// Outcome of `process_streaming_frames` for one recv-cycle.
struct StreamingFrameOutcome {
    /// Bytes consumed from `recv_buf` — caller passes to `compact_recv_buf`.
    consumed: usize,
    /// Sequence of the last slot pushed, for `pending_acks.push`.
    last_target: u64,
    /// Whether any slot was pushed this cycle. Drives the caller's
    /// `pending_acks.push` vs `yield_now` choice.
    any_published: bool,
    /// Updated `accum_end_sequence` — reflects only slots that were
    /// actually committed to the input ring. Equal to the input value
    /// when no slots were pushed.
    accum_end_sequence: u64,
    /// Whether at least one non-empty `InputBatch` arrived. Folded into
    /// the session-wide `received_data` flag by the caller.
    received_data: bool,
    /// `Some(e)` ⇒ caller breaks `'streaming SessionExit::Fatal(e)` —
    /// oversize frame or unrecognised primary message. Slots pushed
    /// before the error are still committed.
    frame_err: Option<Box<dyn std::error::Error>>,
}

/// Process every frame visible in `recv_buf`, publishing decoded slots
/// onto `input_producer` under a single `Producer::batch`. The cursor
/// advances exactly once per call regardless of how many frames or slots
/// were processed.
///
/// `pending_accum` shadows `accum_end_sequence` until `batch.commit()`
/// runs so the returned `accum_end_sequence` only ever names slots that
/// are visible to consumers. The commit runs in every exit path
/// (clean end, fatal break) so the caller can safely use the returned
/// `accum_end_sequence` for `pending_acks.push` or for a subsequent ack.
fn process_streaming_frames<E: melin_app::AppEvent>(
    recv_buf: &[u8],
    input_producer: &mut melin_pipeline::ring::Producer<
        melin_transport_core::pipeline::InputSlot<E>,
    >,
    accum_end_sequence: u64,
) -> StreamingFrameOutcome {
    let mut consumed = 0;
    let mut last_target = 0u64;
    let mut any_published = false;
    let mut received_data = false;
    let mut frame_err: Option<Box<dyn std::error::Error>> = None;
    let mut batch = input_producer.batch();
    let mut pending_accum = accum_end_sequence;
    loop {
        let remaining = &recv_buf[consumed..];
        match try_extract_frame(remaining, MAX_DATA_FRAME) {
            FrameResult::Complete(payload_start, frame_end) => {
                let payload = &remaining[payload_start..frame_end];
                match try_decode_input_batch::<E>(payload) {
                    Ok(slots) => {
                        if !slots.is_empty() {
                            received_data = true;
                            for slot in slots {
                                let primary_seq = slot.sequence;
                                last_target = batch.push_with(|s| *s = slot);
                                pending_accum = primary_seq;
                                any_published = true;
                            }
                        }
                    }
                    Err(_) => match decode_primary_message(payload) {
                        Ok(PrimaryMessage::Heartbeat { sequence }) => {
                            debug!(sequence, "heartbeat from primary (DPDK)");
                        }
                        Ok(other) => {
                            debug!("unexpected message during streaming: {other:?}");
                        }
                        Err(e) => {
                            frame_err =
                                Some(format!("failed to decode primary message: {e}").into());
                            break;
                        }
                    },
                }
                consumed += frame_end;
            }
            FrameResult::Oversized => {
                frame_err = Some("oversized frame from primary during streaming".into());
                break;
            }
            FrameResult::Incomplete => break,
        }
    }
    // Commit before returning so the slots pushed above become visible to
    // the apply consumer in every exit path — including the fatal ones,
    // which tear the session down right after this returns.
    batch.commit();
    StreamingFrameOutcome {
        consumed,
        last_target,
        any_published,
        accum_end_sequence: pending_accum,
        received_data,
        frame_err,
    }
}

/// Outcome of `process_drain_frames` for one drain recv-cycle. Drain is
/// the post-promotion flush: a lenient pass that publishes whatever
/// `InputBatch` frames are visible and stops at the first non-input
/// frame (no heartbeat handling, no fatal-on-decode-error — we're
/// already on the way out).
struct DrainFrameOutcome {
    consumed: usize,
    last_target: u64,
    any_published: bool,
    accum_end_sequence: u64,
}

/// Drain pass: extract every `InputBatch` frame from `recv_buf` and
/// publish slots under a single batch. Anything that isn't a decodable
/// `InputBatch` (heartbeat, partial frame, error) terminates the inner
/// loop without raising — the promotion sequence is about flushing
/// pending data, not validating the wire.
fn process_drain_frames<E: melin_app::AppEvent>(
    recv_buf: &[u8],
    input_producer: &mut melin_pipeline::ring::Producer<
        melin_transport_core::pipeline::InputSlot<E>,
    >,
    accum_end_sequence: u64,
) -> DrainFrameOutcome {
    let mut consumed = 0;
    let mut last_target = 0u64;
    let mut any_published = false;
    let mut batch = input_producer.batch();
    let mut pending_accum = accum_end_sequence;
    loop {
        let remaining = &recv_buf[consumed..];
        match try_extract_frame(remaining, MAX_DATA_FRAME) {
            FrameResult::Complete(ps, fe) => {
                let payload = &remaining[ps..fe];
                if let Ok(slots) = try_decode_input_batch::<E>(payload) {
                    for slot in slots {
                        let primary_seq = slot.sequence;
                        last_target = batch.push_with(|s| *s = slot);
                        pending_accum = primary_seq;
                        any_published = true;
                    }
                }
                consumed += fe;
            }
            _ => break,
        }
    }
    batch.commit();
    DrainFrameOutcome {
        consumed,
        last_target,
        any_published,
        accum_end_sequence: pending_accum,
    }
}

/// Per-slot state for the DPDK replication sender.
enum SlotState {
    /// No replica connected on this slot.
    Idle,
    /// Replica connected, performing handshake.
    Handshaking(melin_dpdk::SocketHandle),
    /// Streaming journal data to replica.
    Streaming(melin_dpdk::SocketHandle),
}

/// Per-replica slot — owns its ring consumer and state machine.
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

/// Step-able DPDK replication state. Owns both slot state machines and the
/// shared cursors / metrics, but does NOT own the `DpdkTransport` — it
/// reaches into one supplied by the caller per call. This shape lets the
/// primary's single DPDK poll thread drive replication alongside client
/// traffic by calling `tick()` once per poll iteration and dispatching
/// `accept_connection()` for any `AcceptedConnection` that arrives on the
/// replication listen port.
///
/// Parameterised over `A: Application` so successive `tick` calls cannot
/// disagree on the application type — the journal catch-up and snapshot
/// transfer paths both decode events through `A`, so a mid-flight switch
/// would silently corrupt the wire. The driver carries no `A`-typed
/// data, hence the `PhantomData`.
pub struct DpdkReplicationDriver<A: Application> {
    slots: [DpdkReplicaSlot; 2],
    replication_cursor: Arc<AtomicU64>,
    fastest_replica_cursor: Arc<AtomicU64>,
    genesis_entry: Vec<u8>,
    journal_path: std::path::PathBuf,
    replica_ready: Arc<AtomicBool>,
    replicas_connected: Arc<AtomicU32>,
    metrics: Arc<ReplicationMetrics>,
    batch_size: usize,
    heartbeat_interval: std::time::Duration,
    // Anchors the `A` type parameter — the struct holds no app-typed
    // state, but `tick`'s journal-catchup and snapshot-transfer paths
    // do, and we want the type system to enforce that the same `A`
    // flows through every call rather than relying on every call site
    // to spell `::<A>` consistently.
    _app: PhantomData<fn(A)>,
}

impl<A: Application> DpdkReplicationDriver<A> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repl_consumers: [ReplicationConsumer; 2],
        replication_cursor: Arc<AtomicU64>,
        fastest_replica_cursor: Arc<AtomicU64>,
        genesis_entry: Vec<u8>,
        journal_path: std::path::PathBuf,
        replica_ready: Arc<AtomicBool>,
        replicas_connected: Arc<AtomicU32>,
        evict_flags: [Arc<AtomicBool>; 2],
        active_flags: [Arc<AtomicBool>; 2],
        metrics: Arc<ReplicationMetrics>,
        batch_size: usize,
        heartbeat_secs: u64,
    ) -> Self {
        let [consumer_0, consumer_1] = repl_consumers;
        let now = std::time::Instant::now();
        DpdkReplicationDriver {
            slots: [
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
            ],
            replication_cursor,
            fastest_replica_cursor,
            genesis_entry,
            journal_path,
            replica_ready,
            replicas_connected,
            metrics,
            batch_size,
            heartbeat_interval: std::time::Duration::from_secs(heartbeat_secs),
            _app: PhantomData,
        }
    }

    /// Take ownership of a freshly-accepted connection on the replication
    /// port. Assigns it to the first idle slot, or closes it if both slots
    /// are occupied (dual-repl cap). Caller is responsible for filtering by
    /// `AcceptedConnection::listen_port`.
    pub fn accept_connection(
        &mut self,
        peer: std::net::SocketAddr,
        handle: melin_dpdk::SocketHandle,
        transport: &mut melin_dpdk::DpdkTransport,
    ) {
        let idle_slot = self
            .slots
            .iter()
            .position(|s| matches!(s.state, SlotState::Idle));
        if let Some(idx) = idle_slot {
            info!(peer = ?peer, slot = idx, "replica connected via DPDK");
            self.replicas_connected.fetch_add(1, Ordering::Release);
            self.slots[idx].recv_buf.clear();
            self.slots[idx].state = SlotState::Handshaking(handle);
        } else {
            debug!(peer = ?peer, "replica rejected — both slots occupied");
            transport.close(handle);
        }
    }

    /// Drive both slots' state machines for one poll iteration. Returns
    /// `true` if at least one slot is currently active (Handshaking or
    /// Streaming) — caller can use this to decide whether to busy-spin
    /// on idle.
    pub fn tick(
        &mut self,
        transport: &mut melin_dpdk::DpdkTransport,
        shutdown: &AtomicBool,
    ) -> bool {
        // Local rebinds for readability — the body below was lifted from
        // the previous run_sender_dpdk thread, mostly verbatim, so keep
        // the variable names matching.
        let slots = &mut self.slots;
        let replication_cursor = &self.replication_cursor;
        let fastest_replica_cursor = &self.fastest_replica_cursor;
        let genesis_entry = &self.genesis_entry;
        let journal_path = &self.journal_path;
        let replica_ready = &self.replica_ready;
        let replicas_connected = &self.replicas_connected;
        let metrics = &self.metrics;
        let batch_size = self.batch_size;
        let heartbeat_interval = self.heartbeat_interval;

        // Check eviction flags from the journal stage.
        for i in 0..2 {
            let evicting = slots[i].evict_flag.load(Ordering::Acquire)
                && !matches!(slots[i].state, SlotState::Idle);
            if !evicting {
                continue;
            }
            metrics.evictions_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                slot = i,
                "evicting slow replica (ring backpressure timeout, DPDK)"
            );
            let slot = &mut slots[i];
            if let SlotState::Streaming(h) | SlotState::Handshaking(h) = slot.state {
                transport.close(h);
            }
            // Zero per-slot metrics BEFORE the active_flag Release so
            // a reader cannot observe `active=true` paired with `cursor=0`
            // on weak-memory architectures (see B2 in the follow-ups doc).
            metrics.acked_sequence[i].store(0, Ordering::Relaxed);
            metrics.in_memory_sequence[i].store(0, Ordering::Relaxed);
            metrics.catching_up[i].store(false, Ordering::Relaxed);
            slot.active_flag.store(false, Ordering::Release);
            slot.evict_flag.store(false, Ordering::Release);
            slot.acked_cursor = u64::MAX;
            slot.recv_buf.clear();
            // Drop any unread ring entries so a reconnecting replica
            // on this slot doesn't replay pre-eviction data and stall
            // the primary's replication cursor. See kernel-TCP path
            // in tcp_sender.rs for the detailed rationale.
            slot.consumer.skip_to_producer();
            slot.state = SlotState::Idle;
            replicas_connected.fetch_sub(1, Ordering::Release);

            // Recompute the shared replication cursor from the *surviving*
            // slot's progress so the response stage's gate can advance.
            // Without this, `replication_cursor` stays frozen at its pre-
            // eviction value (the min that included this slot's last
            // ack), and the primary stops acking client requests even
            // though the surviving replica is healthy. The kernel-TCP
            // path does the equivalent in tcp_sender.rs.
            let other_acked = slots[1 - i].acked_cursor;
            update_dual_replication_cursor(
                u64::MAX,
                other_acked,
                replication_cursor,
                fastest_replica_cursor,
            );
            if replicas_connected.load(Ordering::Relaxed) == 0 {
                warn!("all replicas disconnected — trading halted");
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
                        update_dual_replication_cursor(
                            u64::MAX,
                            other_acked,
                            replication_cursor,
                            fastest_replica_cursor,
                        );
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
                                        journal_path,
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
                                            genesis_entry,
                                            &mut slot.send_buf,
                                        );
                                        transport.queue_send(handle, &slot.send_buf);
                                        slot.send_buf.clear();

                                        // Journal catch-up via DPDK transport.
                                        if let Err(e) = catch_up_from_journal_dpdk::<A>(
                                            journal_path,
                                            h.last_sequence,
                                            handle,
                                            transport,
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
                                        if let Err(e) = snapshot_transfer_dpdk::<A>(
                                            journal_path,
                                            genesis_entry,
                                            handle,
                                            transport,
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
                                    // Ring chunks are wire-ready InputBatch frames;
                                    // forward as-is.
                                    while let Some((meta, data)) = slot.consumer.try_read() {
                                        if meta.end_sequence > h.last_sequence {
                                            slot.send_buf.clear();
                                            slot.send_buf.extend_from_slice(data);
                                            slot.consumer.commit();
                                            transport.queue_send(handle, &slot.send_buf);
                                            slot.send_buf.clear();
                                            slot.last_sequence = meta.end_sequence;
                                            break;
                                        }
                                        slot.consumer.commit();
                                    }

                                    // Seed the per-slot metrics cursors before flipping
                                    // active so a reader that observes active=true also
                                    // observes a non-zero cursor pair. Without this, a
                                    // degraded gate freezes after a replica rejoins —
                                    // see tcp_sender for the full rationale. Relaxed is
                                    // fine because the active_flag Release below
                                    // publishes these stores in program order.
                                    metrics.acked_sequence[slot_idx]
                                        .store(h.last_sequence, Ordering::Relaxed);
                                    metrics.in_memory_sequence[slot_idx]
                                        .store(h.last_sequence, Ordering::Relaxed);

                                    // Mark ring active before signaling readiness
                                    // so the journal stage publishes when seeds flow.
                                    slot.active_flag.store(true, Ordering::Release);
                                    replica_ready.store(true, Ordering::Release);

                                    update_dual_replication_cursor(
                                        slot.acked_cursor,
                                        other_acked,
                                        replication_cursor,
                                        fastest_replica_cursor,
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
                                    metrics.in_memory_sequence[slot_idx]
                                        .store(ack.in_memory_sequence, Ordering::Relaxed);
                                    update_dual_replication_cursor(
                                        slot.acked_cursor,
                                        other_acked,
                                        replication_cursor,
                                        fastest_replica_cursor,
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
                        // Zero metrics before active_flag Release — see B2.
                        metrics.acked_sequence[slot_idx].store(0, Ordering::Relaxed);
                        metrics.in_memory_sequence[slot_idx].store(0, Ordering::Relaxed);
                        slot.active_flag.store(false, Ordering::Release);
                        slot.acked_cursor = u64::MAX;
                        slot.recv_buf.clear();
                        slot.state = SlotState::Idle;
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        update_dual_replication_cursor(
                            u64::MAX,
                            other_acked,
                            replication_cursor,
                            fastest_replica_cursor,
                        );
                        if replicas_connected.load(Ordering::Relaxed) == 0 {
                            warn!("all replicas disconnected — trading halted");
                        }
                        continue;
                    }

                    // 2. Send data batches. Ring chunks are wire-ready
                    //    `InputBatch` frames produced by the journal stage
                    //    — the sender is a passthrough. Pre-check the
                    //    per-socket TX queue: if we commit a batch from
                    //    the ring but `queue_send` rejects it (TX full),
                    //    the data is gone from the ring without ever
                    //    reaching the replica — replica never acks,
                    //    replication_cursor stalls, and the response
                    //    gate freezes the whole exchange. We saw this
                    //    exact symptom on dpdk-dual-repl.
                    let max_tx = melin_dpdk::DpdkTransport::max_tx_queue_size();
                    let used = transport.tx_queue_bytes(handle);
                    let mut available = max_tx.saturating_sub(used);

                    slot.send_buf.clear();
                    let mut batches_sent = 0;
                    // Read-and-peek loop: only commit a batch once we've
                    // confirmed it fits. If it doesn't fit, leave the
                    // ring cursor in place so the next iteration retries
                    // after transport.poll() drains the wire.
                    while batches_sent < batch_size {
                        let Some((meta, data)) = slot.consumer.try_read() else {
                            break;
                        };
                        let data_len = data.len();
                        if data_len > available {
                            // Don't commit; retry next iteration.
                            break;
                        }
                        slot.send_buf.extend_from_slice(data);
                        slot.consumer.commit();
                        slot.last_sequence = meta.end_sequence;
                        batches_sent += 1;
                        available = available.saturating_sub(data_len);
                    }

                    if !slot.send_buf.is_empty() {
                        metrics.bytes_sent[slot_idx]
                            .fetch_add(slot.send_buf.len() as u64, Ordering::Relaxed);
                        if !transport.queue_send(handle, &slot.send_buf) {
                            // Pre-check should have prevented this; this
                            // branch is defense-in-depth. Replica catches
                            // up via journal on reconnect, so committed
                            // (now-unsent) batches aren't lost.
                            warn!(
                                slot = slot_idx,
                                used,
                                send_len = slot.send_buf.len(),
                                "TX overflow on replica socket — disconnecting"
                            );
                            transport.close(handle);
                            // Zero metrics before active_flag Release — see B2.
                            metrics.acked_sequence[slot_idx].store(0, Ordering::Relaxed);
                            metrics.in_memory_sequence[slot_idx].store(0, Ordering::Relaxed);
                            slot.active_flag.store(false, Ordering::Release);
                            slot.acked_cursor = u64::MAX;
                            slot.recv_buf.clear();
                            slot.state = SlotState::Idle;
                            replicas_connected.fetch_sub(1, Ordering::Release);
                            update_dual_replication_cursor(
                                u64::MAX,
                                other_acked,
                                replication_cursor,
                                fastest_replica_cursor,
                            );
                            if replicas_connected.load(Ordering::Relaxed) == 0 {
                                warn!("all replicas disconnected — trading halted");
                            }
                            continue;
                        }
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
                        // Zero metrics before active_flag Release — see B2.
                        metrics.acked_sequence[slot_idx].store(0, Ordering::Relaxed);
                        metrics.in_memory_sequence[slot_idx].store(0, Ordering::Relaxed);
                        slot.active_flag.store(false, Ordering::Release);
                        slot.acked_cursor = u64::MAX;
                        slot.recv_buf.clear();
                        slot.state = SlotState::Idle;
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        update_dual_replication_cursor(
                            u64::MAX,
                            other_acked,
                            replication_cursor,
                            fastest_replica_cursor,
                        );
                        if replicas_connected.load(Ordering::Relaxed) == 0 {
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

        any_active
    }
}

/// DPDK-adapted journal catch-up: reads journal files (journal-codec
/// bytes), decodes them into `InputSlot` records, and sends them as
/// `InputBatch` frames via the DPDK transport. Periodically polls the
/// transport to flush TX and keep smoltcp's timers alive.
fn catch_up_from_journal_dpdk<A: Application>(
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

            // Decode the journal-batch bytes into InputSlots and re-encode
            // as an InputBatch for the wire — same wire format the live
            // streaming path uses.
            let slots =
                melin_transport_core::replication::protocol::decode_journal_to_input_slots::<
                    A::Event,
                >(&batch_buf)
                .map_err(|e| {
                    io::Error::other(format!(
                        "catch-up journal decode at seq {batch_end_seq}: {e}"
                    ))
                })?;
            send_buf.clear();
            melin_transport_core::replication::protocol::encode_input_batch(&slots, send_buf);
            // Retry-with-poll: a 64 KiB batch can fill the TX queue even
            // after a previous poll. Spin-poll until queue_send accepts
            // the batch (or the replica drops). This is bounded — TX
            // drains as fast as smoltcp can dispatch segments.
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    return Ok(());
                }
                if transport.queue_send(handle, send_buf) {
                    break;
                }
                transport.poll();
                if !transport.is_active(handle) {
                    return Err(io::Error::other(
                        "replica disconnected during journal catch-up (TX backpressure)",
                    ));
                }
            }
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
/// StreamStart → InputBatch* (catch-up).
fn snapshot_transfer_dpdk<A: Application>(
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
             — set --snapshot-interval-ms to a non-zero value so the shadow exchange writes snapshots",
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
    let magic = u32::from_le_bytes(
        snap_data[0..4]
            .try_into()
            .expect("bounds checked: snap_data has at least 48 bytes"),
    );
    if magic != 0x534E_4150 {
        return Err(io::Error::other(format!(
            "snapshot file has invalid magic: {magic:#x} (expected 0x534e4150)"
        )));
    }
    let snap_sequence = u64::from_le_bytes(
        snap_data[8..16]
            .try_into()
            .expect("bounds checked: snap_data has at least 48 bytes"),
    );
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
    catch_up_from_journal_dpdk::<A>(
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
#[allow(clippy::too_many_arguments)]
pub fn run_receiver_dpdk<A, W>(
    mut transport: melin_dpdk::DpdkTransport,
    primary_ip: std::net::Ipv4Addr,
    primary_port: u16,
    journal_path: &std::path::Path,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_ms: u64,
    snapshot_path: std::path::PathBuf,
    cores: crate::server::PipelineCores,
    receiver_core: usize,
    group_commit_delay: std::time::Duration,
    pipeline_depth: usize,
    busy_spin: bool,
    rotation: Option<(u64, std::sync::Arc<AtomicBool>)>,
    // Application factory: see the kernel-TCP `run_receiver` for the
    // shape and rationale. Carries operator policy (rate limits, caps,
    // ...) alongside the empty-app constructor.
    factory: std::sync::Arc<dyn melin_app::app_factory::AppFactory<App = A>>,
) -> ReceiverResult<A, W>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
    JournalStage<A::Event, W>: JournalStageRun<A::Event, Writer = W>,
{
    // Recover local state from journal (if any). On first call this may
    // be (None, None) for a fresh replica. After a reconnect, the pipeline
    // shutdown returns the App + writer directly.
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        if journal_path.exists() {
            let engine = if snapshot_path.exists() {
                info!("recovering replica from snapshot + journal (DPDK)");
                melin_transport_core::JournaledApp::<A, W>::recover_from_snapshot(
                    &snapshot_path,
                    journal_path,
                )?
            } else {
                melin_transport_core::JournaledApp::<A, W>::recover(factory.empty(), journal_path)?
            };
            let next = engine.next_sequence();
            let last = next.saturating_sub(1);
            let hash = engine.chain_hash().unwrap_or([0u8; 32]);
            let (mut exchange, writer) = engine.into_parts();
            factory.apply_operator_policy(&mut exchange);
            (Some(exchange), Some(writer), last, hash)
        } else {
            (None, None, 0u64, [0u8; 32])
        };

    // Exponential backoff for reconnection: 1s → 2s → 4s → … → 30s max.
    // Reset to 1s on successful streaming (first InputBatch received).
    let mut backoff = std::time::Duration::from_secs(1);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

    // Reusable buffers — survive across reconnections.
    let mut send_buf = Vec::with_capacity(64);
    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    // Ephemeral port counter for outbound connections. Each reconnect uses
    // a different local port to avoid smoltcp TIME_WAIT conflicts.
    let mut local_port: u16 = 40000;

    // Live pipeline state — built once on first connect (or after a snapshot
    // transfer), persists across `Disconnected` reconnects so we don't pay
    // the journal-recover + thread-spawn + warm-up cost on every drop.
    // None = no pipeline yet (first iteration, or just torn down for
    // snapshot transfer); Some = running pipeline with threads + atomics
    // we can read for the next reconnect handshake.
    let mut pipeline: Option<ReplicaPipelineHandles<A, W>> = None;

    // --- Outer reconnect loop ---
    //
    // Each iteration: connect → handshake → (snapshot rebuild?) →
    // (build pipeline if absent) → stream. On disconnect the pipeline
    // stays live — we just refresh handshake state from its atomics and
    // reconnect. Only `Promote` / `Shutdown` / snapshot-transfer / fatal
    // error tear it down.
    loop {
        // Refresh handshake state from the running pipeline, if any.
        if let Some(p) = pipeline.as_ref() {
            last_sequence = p.last_seq.load(Ordering::Acquire);
            if let Some(ref lock) = p.chain_hash_lock {
                chain_hash = lock.load().chain_hash;
            }
        }

        if shutdown.load(Ordering::Relaxed) {
            if let Some(p) = pipeline.take() {
                let _ = teardown_replica_pipeline::<A, W>(p);
            }
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered while disconnected (DPDK)");
            if let Some(p) = pipeline.take()
                && let Some((e, w)) = teardown_replica_pipeline::<A, W>(p)
            {
                exchange = Some(e);
                journal_writer = Some(w);
            }
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
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Ok(None);
            }
            if promote.load(Ordering::Acquire) {
                info!("promotion triggered during reconnect backoff (DPDK)");
                if let Some(p) = pipeline.take()
                    && let Some((e, w)) = teardown_replica_pipeline::<A, W>(p)
                {
                    exchange = Some(e);
                    journal_writer = Some(w);
                }
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
        // Helper macro: shut the pipeline down before bubbling up a fatal
        // error from the handshake. Borrows `pipeline` directly so we don't
        // leak the threads on the way out.
        macro_rules! fatal_err_dpdk {
            ($msg:expr) => {{
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Err($msg);
            }};
        }
        recv_buf.clear();
        let primary_genesis_entry = 'handshake: loop {
            if shutdown.load(Ordering::Relaxed) {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
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

                            // Tear down the live pipeline before wiping its
                            // backing journal — the journal stage holds the
                            // file open and the App lives on the matching
                            // thread; both must exit cleanly first.
                            if let Some(p) = pipeline.take() {
                                let _ = teardown_replica_pipeline::<A, W>(p);
                            }

                            // Remove stale local state. Invalidate the in-memory
                            // App and SectorWriter — their underlying files
                            // are about to be deleted. Without this, a failed
                            // snapshot transfer would leave stale state that
                            // the reconnect loop mistakes for valid.
                            exchange = None;
                            journal_writer = None;
                            let _ = std::fs::remove_file(journal_path);
                            let _ = std::fs::remove_file(&snapshot_path);

                            // Receive snapshot via DPDK transport.
                            match receive_snapshot_dpdk::<A>(
                                handle,
                                &mut transport,
                                &mut recv_buf,
                                &snapshot_path,
                                shutdown,
                            ) {
                                Ok((snap_exchange, snap_seq, snap_hash)) => {
                                    exchange = Some(snap_exchange);
                                    let writer = W::create_continuing(
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
                            fatal_err_dpdk!(
                                "chain hash mismatch — replica has divergent history".into()
                            );
                        }
                        other => {
                            fatal_err_dpdk!(format!("unexpected response: {other:?}").into());
                        }
                    }
                }
                FrameResult::Oversized => {
                    fatal_err_dpdk!("oversized frame from primary during handshake".into());
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

        // Create journal for fresh replica (first connection only).
        //
        // Gate on `pipeline.is_none()` rather than `journal_writer.is_none()`:
        // the writer is moved into the pipeline on first connect and never
        // returned, so on every subsequent reconnect `journal_writer` is
        // `None` even though a live writer is still mid-stream inside the
        // pipeline. `pipeline.is_none()` distinguishes "true first connect or
        // post-snapshot rebuild" from "reconnect against an existing
        // pipeline" — the latter must not recreate the journal file.
        if pipeline.is_none() && journal_writer.is_none() {
            let writer =
                melin_journal::create_fresh_replica::<_, W>(journal_path, &primary_genesis_entry)?;
            let mut fresh = factory.empty();
            factory.apply_operator_policy(&mut fresh);
            exchange = Some(fresh);
            journal_writer = Some(writer);
        }

        // --- Build pipeline if absent ---
        //
        // Built once on first connect, or after a snapshot transfer tore
        // the previous one down. On disconnect the pipeline lives, so
        // this branch is skipped.
        if pipeline.is_none() {
            // If we still have no state after all the handshake logic, reconnect.
            if exchange.is_none() || journal_writer.is_none() {
                continue;
            }
            let cur_exchange = exchange.take().expect("exchange initialized");
            let cur_writer = journal_writer.take().expect("journal_writer initialized");

            // Unpin before spawning the pipeline. Same rationale as the
            // kernel-TCP receiver: `pin_to_core` sets `SCHED_FIFO` and on
            // post-snapshot rebuilds this thread is already pinned —
            // children would inherit `{receiver_core}` + FIFO and never
            // preempt the busy-spinning receiver to reach their own
            // self-pin.
            if let Err(e) = melin_app::affinity::clear_affinity() {
                tracing::warn!(error = e, "failed to clear receiver affinity before spawn");
            }

            pipeline = Some(build_replica_pipeline_with_threads::<A, W>(
                cur_exchange,
                cur_writer,
                cores,
                snapshot_interval_ms,
                snapshot_path.clone(),
                group_commit_delay,
                busy_spin,
                rotation.clone(),
            )?);

            // Pipeline children are spawned and self-pinned. Now safe to
            // pin the receive thread — mirrors the primary's reader pin
            // so the thread producing input-ring entries from the network
            // isn't migrated across L3s mid-batch.
            melin_app::affinity::pin_thread("receiver", receiver_core);
        }

        let mut pending_acks = PendingAckQueue::new(pipeline_depth);
        let mut received_data = false;
        let mut accum_end_sequence: u64 = 0;
        // Last cursor pair sent on the wire. Coalesces dual-track ack
        // triggers (see the `// --- Flush acks ---` block below for the
        // full rationale; mirrors `tcp_receiver`'s scheme).
        let mut last_sent_acked_seq: u64 = 0;
        let mut last_sent_in_memory_seq: u64 = 0;

        // Encode an ack into send_buf and queue it on the DPDK transport.
        //
        // Bounded retry-with-poll: under sustained load the DPDK TX
        // queue can fill up before this ack is queued. Silently
        // dropping it permanently would freeze the primary's
        // `replication_cursor`. But blocking forever while waiting
        // for TX to drain deadlocks against the primary, which is
        // also waiting on its own TX (single-queue path shares one
        // TX between client traffic, replication data, and acks).
        //
        // Cap the retry at `ACK_RETRY_CAP` poll cycles. If we still
        // can't queue the ack, drop it: acks carry the cumulative
        // sequence, so the next ack we send (with a higher sequence)
        // subsumes anything dropped here. The streaming loop's
        // outer `transport.poll()` will continue draining TX, and
        // the next data-batch processing pass will retry the ack
        // with an updated cursor.
        const ACK_RETRY_CAP: u32 = 32;
        macro_rules! send_ack_dpdk {
            ($ack:expr) => {{
                send_buf.clear();
                encode_ack(&$ack, &mut send_buf);
                let mut attempts: u32 = 0;
                loop {
                    if transport.queue_send(handle, &send_buf) {
                        break;
                    }
                    attempts += 1;
                    if attempts >= ACK_RETRY_CAP {
                        break;
                    }
                    transport.poll();
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    if !transport.is_active(handle) {
                        break;
                    }
                }
            }};
        }

        // Borrow input_producer + journal_cursor from the live pipeline
        // for the streaming session. On disconnect we drop these locals
        // and retake them next iteration.
        let p = pipeline.as_mut().expect("pipeline must exist by here");
        let input_producer = &mut p.input_producer;
        let journal_cursor = p.journal_cursor.as_ref();

        // --- Inner streaming loop ---
        //
        // Returns one of: Disconnected → reconnect with pipeline alive;
        // Shutdown → tear down + Ok(None); Promote → tear down + Ok(Some);
        // Fatal(err) → tear down + Err.
        enum SessionExit {
            Disconnected,
            Shutdown,
            Promote,
            Fatal(Box<dyn std::error::Error>),
        }
        let session_exit = 'streaming: loop {
            if shutdown.load(Ordering::Relaxed) {
                info!("replica shutting down (DPDK)");
                if let Some(seq) = pending_acks.pop_all_blocking(journal_cursor, busy_spin) {
                    send_ack_dpdk!(Ack {
                        acked_sequence: seq,
                        in_memory_sequence: accum_end_sequence,
                    });
                    transport.poll();
                }
                break 'streaming SessionExit::Shutdown;
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
                    let outcome = process_drain_frames::<A::Event>(
                        &recv_buf,
                        input_producer,
                        accum_end_sequence,
                    );
                    accum_end_sequence = outcome.accum_end_sequence;
                    compact_recv_buf(&mut recv_buf, outcome.consumed);
                    if outcome.any_published && !pending_acks.is_full() {
                        pending_acks.push(outcome.last_target, accum_end_sequence);
                    }
                }
                if let Some(seq) = pending_acks.pop_all_blocking(journal_cursor, busy_spin) {
                    send_ack_dpdk!(Ack {
                        acked_sequence: seq,
                        in_memory_sequence: accum_end_sequence,
                    });
                    transport.poll();
                }
                break 'streaming SessionExit::Promote;
            }

            // --- Flush acks (dual-track) ---
            //
            // See `try_flush_dual_track` in `replication/mod.rs` for the
            // model. The helper centralises the persisted-vs-in-memory
            // logic and the namespace translation between local-ring
            // positions and primary sequences across all three receivers.
            if let Some(ack) = try_flush_dual_track(
                &mut pending_acks,
                journal_cursor,
                accum_end_sequence,
                last_sent_acked_seq,
                last_sent_in_memory_seq,
            ) {
                send_ack_dpdk!(ack);
                last_sent_acked_seq = ack.acked_sequence;
                last_sent_in_memory_seq = ack.in_memory_sequence;
            }

            // Backpressure: if pipeline is saturated, block until the oldest
            // batch is durable.
            if pending_acks.is_full() {
                let seq = pending_acks.pop_oldest_blocking(journal_cursor, busy_spin);
                let in_mem_now = accum_end_sequence;
                send_ack_dpdk!(Ack {
                    acked_sequence: seq,
                    in_memory_sequence: in_mem_now,
                });
                // Sync trackers so the flush block doesn't refire — see
                // tcp_receiver for the full rationale.
                last_sent_acked_seq = seq;
                last_sent_in_memory_seq = in_mem_now;
            }

            // Poll smoltcp and receive data.
            transport.poll();
            transport.recv_into_vec(handle, &mut recv_buf);

            // Check for disconnect.
            if !transport.is_active(handle) && recv_buf.is_empty() {
                if let Some(seq) = pending_acks.pop_all_blocking(journal_cursor, busy_spin) {
                    send_ack_dpdk!(Ack {
                        acked_sequence: seq,
                        in_memory_sequence: accum_end_sequence,
                    });
                    transport.poll();
                }
                break 'streaming SessionExit::Disconnected;
            }

            // Parse frames from the receive buffer and publish slots
            // straight into the input ring (mirrors the io_uring TCP
            // receiver — no journal-codec round-trip on the wire). The
            // helper opens one `Producer::batch` across every frame so
            // the input-ring cursor advances with a single Release
            // store, not one per slot. See `process_streaming_frames`
            // for the `pending_accum`-shadow correctness invariant.
            let outcome =
                process_streaming_frames::<A::Event>(&recv_buf, input_producer, accum_end_sequence);
            accum_end_sequence = outcome.accum_end_sequence;
            received_data |= outcome.received_data;
            let burst_last_target = outcome.last_target;
            let burst_any_published = outcome.any_published;
            compact_recv_buf(&mut recv_buf, outcome.consumed);
            if let Some(e) = outcome.frame_err {
                break 'streaming SessionExit::Fatal(e);
            }

            // One pending_acks entry per recv burst — covers all slots
            // published from this RECV's buffer.
            if burst_any_published && !pending_acks.is_full() {
                pending_acks.push(burst_last_target, accum_end_sequence);
            } else if !burst_any_published {
                std::thread::yield_now();
            }
        };

        match session_exit {
            SessionExit::Shutdown => {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Ok(None);
            }
            SessionExit::Promote => {
                return match pipeline.take() {
                    Some(p) => match teardown_replica_pipeline::<A, W>(p) {
                        Some((ex, wr)) => Ok(Some((ex, wr))),
                        None => Err("pipeline thread panicked during promotion (DPDK)".into()),
                    },
                    None => Err("pipeline missing on promote (DPDK)".into()),
                };
            }
            SessionExit::Fatal(e) => {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Err(e);
            }
            SessionExit::Disconnected => {
                // Pipeline stays live — `last_sequence` and `chain_hash`
                // refresh from its atomics at the top of the next iteration.
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
fn receive_snapshot_dpdk<A: Application>(
    handle: melin_dpdk::SocketHandle,
    transport: &mut melin_dpdk::DpdkTransport,
    recv_buf: &mut Vec<u8>,
    snapshot_path: &std::path::Path,
    shutdown: &AtomicBool,
) -> Result<(A, u64, [u8; 32]), Box<dyn std::error::Error + Send + Sync>> {
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
        melin_transport_core::snapshot::load::<A>(snapshot_path)?;
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
    //! Unit tests for the recv-cycle frame helpers
    //! ([`process_streaming_frames`] / [`process_drain_frames`]).
    //!
    //! Real DPDK transport needs hardware, but the helpers are pure
    //! functions over a `&[u8]` recv buffer and a `Producer<InputSlot<E>>`
    //! — every exit path is exercisable in-process. The tests pin the
    //! batch-commit ordering invariants introduced by the perf refactor:
    //! the returned `accum_end_sequence` must only name slots that were
    //! actually committed, in every path including the fatal ones.
    use super::*;
    use melin_app::{AppEvent, CodecError};
    use melin_journal::JournalEvent;
    use melin_pipeline::ring::DisruptorBuilder;
    use melin_transport_core::pipeline::InputSlot;
    use melin_transport_core::replication::protocol::{encode_heartbeat, encode_input_batch};

    /// Minimal `AppEvent` for these tests. Encoded as a single tag byte
    /// (the value of the variant); we never round-trip the bytes — the
    /// helpers under test only depend on `is_query` (false here) and the
    /// fact that the type is `Copy`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u8);

    impl AppEvent for TestEvent {
        fn encoded_size(&self) -> usize {
            1
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[0] = self.0;
            1
        }
        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            Ok(TestEvent(buf[0]))
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    /// Build an `InputSlot<TestEvent>` carrying the given primary sequence.
    /// `tag` is stored in `request_seq` because `try_decode_input_batch`
    /// resets `connection_id` / `publish_ts` / `recv_ts` to defaults (the
    /// wire format documents this — replication frames don't carry
    /// per-connection metadata, see `try_decode_input_batch`'s doc).
    /// `request_seq` round-trips, so it doubles as an identity tag the
    /// tests can assert on after the consumer drain.
    fn slot(primary_seq: u64, tag: u64) -> InputSlot<TestEvent> {
        InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: tag,
            sequence: primary_seq,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent(tag as u8)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        }
    }

    /// Wire-encode an `InputBatch` frame for the given slots and append
    /// it to `out`. `encode_input_batch` produces a length-prefixed
    /// frame, so multiple calls chain into a valid recv buffer.
    fn append_input_batch_frame(out: &mut Vec<u8>, slots: &[InputSlot<TestEvent>]) {
        encode_input_batch(slots, out);
    }

    /// Drain `consumer` until it yields `None`, returning every slot it saw.
    fn drain(
        consumer: &mut melin_pipeline::ring::Consumer<InputSlot<TestEvent>>,
    ) -> Vec<InputSlot<TestEvent>> {
        let mut out = Vec::new();
        while let Some((_seq, slot)) = consumer.try_consume() {
            out.push(slot);
        }
        out
    }

    /// Build a single-consumer disruptor of the requested capacity.
    fn ring(
        capacity: usize,
    ) -> (
        melin_pipeline::ring::Producer<InputSlot<TestEvent>>,
        melin_pipeline::ring::Consumer<InputSlot<TestEvent>>,
    ) {
        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(capacity)
            .add_consumer()
            .build();
        (producer, consumers.pop().expect("consumer present"))
    }

    #[test]
    fn streaming_publishes_all_slots_and_advances_accum_end_sequence() {
        // One InputBatch frame with three slots, plus a heartbeat between
        // two more single-slot frames. Verifies:
        //   * every slot lands on the consumer (single-commit visibility)
        //   * accum_end_sequence reflects the last primary_seq
        //   * heartbeats don't push spurious slots
        //   * `received_data` reflects whether non-empty InputBatch frames arrived
        let (mut producer, mut consumer) = ring(16);

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(10, 0xA0), slot(11, 0xA1), slot(12, 0xA2)]);
        encode_heartbeat(99, &mut buf);
        append_input_batch_frame(&mut buf, &[slot(13, 0xA3)]);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 5);

        assert!(outcome.frame_err.is_none(), "no fatal exit");
        assert_eq!(outcome.consumed, buf.len(), "every byte processed");
        assert!(outcome.any_published);
        assert!(outcome.received_data);
        assert_eq!(
            outcome.accum_end_sequence, 13,
            "tracks the last primary_seq, not the pre-call value"
        );

        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 4);
        let ids: Vec<u64> = slots.iter().map(|s| s.request_seq).collect();
        assert_eq!(ids, vec![0xA0, 0xA1, 0xA2, 0xA3]);
    }

    #[test]
    fn streaming_oversize_frame_commits_prior_slots_then_fatal() {
        // **Core invariant**: a fatal exit (oversize) must not leave
        // `accum_end_sequence` referencing a slot the consumer can't
        // observe. Two valid frames followed by an oversize length
        // prefix → both prior slots must be visible AND
        // `accum_end_sequence` must equal their max primary_seq.
        let (mut producer, mut consumer) = ring(16);

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(7, 0xB0)]);
        append_input_batch_frame(&mut buf, &[slot(8, 0xB1)]);
        // Length prefix > MAX_DATA_FRAME, no body — `try_extract_frame`
        // returns `Oversized` on the prefix alone.
        let oversize_len = (MAX_DATA_FRAME as u32) + 1;
        buf.extend_from_slice(&oversize_len.to_le_bytes());

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 0);

        assert!(outcome.frame_err.is_some(), "oversize ⇒ fatal");
        assert_eq!(
            outcome.accum_end_sequence, 8,
            "only committed slots count toward accum_end_sequence"
        );
        assert!(outcome.any_published);
        let slots = drain(&mut consumer);
        assert_eq!(
            slots.len(),
            2,
            "prior frames are visible to the consumer despite the fatal exit"
        );
    }

    #[test]
    fn streaming_unknown_message_after_valid_input_commits_then_fatal() {
        // A non-InputBatch frame whose payload `decode_primary_message`
        // also rejects must terminate the session, but only after
        // committing the valid slots that came before.
        let (mut producer, mut consumer) = ring(16);

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(3, 0xC0), slot(4, 0xC1)]);
        // A 1-byte garbage payload (msg_type 0xFF doesn't exist on the
        // primary control vocabulary, so decode_primary_message errors).
        // Wire format: [u32 LE length=1][0xFF].
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(0xFF);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 0);

        assert!(outcome.frame_err.is_some(), "unknown primary msg ⇒ fatal");
        assert_eq!(outcome.accum_end_sequence, 4);
        let slots = drain(&mut consumer);
        assert_eq!(slots.len(), 2);
    }

    #[test]
    fn streaming_partial_trailing_frame_is_incomplete_not_fatal() {
        // A complete frame followed by a truncated length prefix:
        //   * the complete frame publishes,
        //   * `consumed` stops before the partial bytes,
        //   * no fatal exit (the caller will recv more next cycle).
        let (mut producer, mut consumer) = ring(8);

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(1, 0xD0)]);
        let complete_len = buf.len();
        buf.extend_from_slice(&[0xDE, 0xAD, 0xBE]); // 3 of 4 prefix bytes

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 0);

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.consumed, complete_len);
        assert_eq!(outcome.accum_end_sequence, 1);
        assert_eq!(drain(&mut consumer).len(), 1);
    }

    #[test]
    fn streaming_heartbeat_only_does_not_advance_accum_end_sequence() {
        // No InputBatch frames in this recv. accum_end_sequence is
        // unchanged from the input value (the consumer can't see slots
        // that don't exist).
        let (mut producer, mut consumer) = ring(8);

        let mut buf = Vec::new();
        encode_heartbeat(42, &mut buf);
        encode_heartbeat(43, &mut buf);

        let outcome = process_streaming_frames::<TestEvent>(&buf, &mut producer, 100);

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.consumed, buf.len());
        assert!(!outcome.any_published);
        assert!(!outcome.received_data);
        assert_eq!(
            outcome.accum_end_sequence, 100,
            "pre-call value preserved when nothing committed"
        );
        assert!(drain(&mut consumer).is_empty());
    }

    #[test]
    fn streaming_empty_buffer_is_a_noop() {
        // Nothing in the recv buffer ⇒ loop never enters ⇒ batch.commit
        // is a no-op ⇒ accum_end_sequence is returned untouched.
        let (mut producer, mut consumer) = ring(4);

        let outcome = process_streaming_frames::<TestEvent>(&[], &mut producer, 77);

        assert!(outcome.frame_err.is_none());
        assert_eq!(outcome.consumed, 0);
        assert!(!outcome.any_published);
        assert!(!outcome.received_data);
        assert_eq!(outcome.accum_end_sequence, 77);
        assert!(drain(&mut consumer).is_empty());
    }

    #[test]
    fn drain_skips_non_input_frames_and_publishes_every_input_batch() {
        // The promotion-drain helper is lenient: it silently skips any
        // frame whose payload isn't an `InputBatch` (heartbeats,
        // anything else `try_decode_input_batch` rejects) and keeps
        // going. The inner `_ => break` arm only fires on `Incomplete`
        // or `Oversized` from `try_extract_frame` — a structurally
        // valid but non-input frame is consumed and skipped, not
        // treated as terminal.
        //
        // This matters because drain runs at promotion to flush every
        // pending slot before the replica becomes primary. Heartbeats
        // interleaved with data must not strand the slots behind them.
        let (mut producer, mut consumer) = ring(16);

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(20, 0xE0), slot(21, 0xE1)]);
        encode_heartbeat(999, &mut buf);
        // A trailing InputBatch *after* the heartbeat — drain MUST
        // reach it, because the heartbeat is silently skipped.
        append_input_batch_frame(&mut buf, &[slot(22, 0xE2)]);

        let outcome = process_drain_frames::<TestEvent>(&buf, &mut producer, 0);

        assert!(outcome.any_published);
        assert_eq!(
            outcome.consumed,
            buf.len(),
            "drain advances past every well-formed frame"
        );
        assert_eq!(outcome.accum_end_sequence, 22);
        let slots = drain(&mut consumer);
        let ids: Vec<u64> = slots.iter().map(|s| s.request_seq).collect();
        assert_eq!(
            ids,
            vec![0xE0, 0xE1, 0xE2],
            "every InputBatch slot lands, including those after the heartbeat"
        );
    }

    #[test]
    fn drain_stops_at_incomplete_trailing_frame() {
        // Drain *does* stop on `Incomplete` (truncated length prefix)
        // and on `Oversized` — the only two `_ => break` cases. Verify
        // the incomplete-prefix path leaves `consumed` at the boundary
        // of the last fully-extracted frame so the caller's
        // `compact_recv_buf` preserves the partial bytes for the next
        // recv.
        let (mut producer, mut consumer) = ring(16);

        let mut buf = Vec::new();
        append_input_batch_frame(&mut buf, &[slot(50, 0xF0)]);
        let complete_len = buf.len();
        buf.extend_from_slice(&[0xDE, 0xAD]); // 2 of 4 prefix bytes

        let outcome = process_drain_frames::<TestEvent>(&buf, &mut producer, 0);

        assert_eq!(outcome.consumed, complete_len);
        assert_eq!(outcome.accum_end_sequence, 50);
        assert_eq!(drain(&mut consumer).len(), 1);
    }

    #[test]
    fn drain_empty_buffer_is_a_noop() {
        let (mut producer, mut consumer) = ring(4);

        let outcome = process_drain_frames::<TestEvent>(&[], &mut producer, 55);

        assert_eq!(outcome.consumed, 0);
        assert!(!outcome.any_published);
        assert_eq!(outcome.accum_end_sequence, 55);
        assert!(drain(&mut consumer).is_empty());
    }
}
