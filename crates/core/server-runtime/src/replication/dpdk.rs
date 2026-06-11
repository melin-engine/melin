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

use super::receiver_transport::{
    FrameResult, ReceiverTransport, SessionExit, compact_recv_buf, streaming_loop,
    try_extract_frame,
};
use super::{
    ReceiverResult, ReplicaCursors, ReplicaPipelineHandles, ReplicationMetrics, SentHighWater,
    build_replica_pipeline_with_threads, sleep_checking_flags, teardown_replica_pipeline,
};
use melin_transport_core::replication::catchup::{
    CatchUpResult, bridge_catchup_to_live, can_catch_up_from_journal, catch_up_from_journal_with,
    snapshot_transfer_with,
};
use melin_transport_core::replication::protocol::{
    Ack, Handshake, MAX_CONTROL_FRAME, MAX_DATA_FRAME, PrimaryMessage, ReplicaMessage,
    decode_primary_message, decode_replica_message, encode_ack, encode_handshake, encode_heartbeat,
    encode_stream_start,
};

// ---------------------------------------------------------------------------
// DPDK ReceiverTransport implementation
// ---------------------------------------------------------------------------

/// DPDK/smoltcp-backed receiver transport for the replica side.
///
/// Wraps a `DpdkTransport` + `SocketHandle` and implements
/// [`ReceiverTransport`] so the generic [`streaming_loop`] can drive it
/// identically to the kernel io_uring path.
struct DpdkReceiverTransport<'a> {
    transport: &'a mut melin_dpdk::DpdkTransport,
    handle: melin_dpdk::SocketHandle,
    send_buf: Vec<u8>,
}

const ACK_RETRY_CAP: u32 = 32;

impl ReceiverTransport for DpdkReceiverTransport<'_> {
    fn poll_recv(&mut self, recv_buf: &mut Vec<u8>) -> io::Result<bool> {
        self.transport.poll();
        let before = recv_buf.len();
        self.transport.recv_into_vec(self.handle, recv_buf);
        Ok(recv_buf.len() > before)
    }

    fn send_ack(&mut self, ack: &Ack) -> io::Result<bool> {
        self.send_buf.clear();
        encode_ack(ack, &mut self.send_buf);
        let mut attempts: u32 = 0;
        loop {
            if self.transport.queue_send(self.handle, &self.send_buf) {
                // Flush immediately so the ACK reaches the primary
                // without waiting for the next poll_recv iteration.
                self.transport.poll();
                return Ok(true);
            }
            attempts += 1;
            if attempts >= ACK_RETRY_CAP {
                tracing::warn!("DPDK ack send failed after {ACK_RETRY_CAP} retries");
                return Ok(false);
            }
            self.transport.poll();
            if !self.transport.is_active(self.handle) {
                return Err(io::Error::other("replica disconnected during ack send"));
            }
        }
    }

    fn ack_in_flight(&self) -> bool {
        false
    }

    fn is_connected(&mut self) -> bool {
        self.transport.is_active(self.handle)
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
    /// Sent high-water for the current connection — the ack-sanity
    /// bound and the heartbeat sequence. Meaningless while `Idle`;
    /// re-seeded on every handshake. See `SentHighWater`.
    sent: SentHighWater,
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
    /// Single owner of the per-replica progress cursors (per-slot
    /// acked positions, shared min/max, and the gate's gauge pair).
    cursors: ReplicaCursors,
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
                    // Placeholder until the slot engages — re-seeded on
                    // every handshake.
                    sent: SentHighWater::seed(0, 0),
                },
                DpdkReplicaSlot {
                    state: SlotState::Idle,
                    consumer: consumer_1,
                    active_flag: Arc::clone(&active_flags[1]),
                    evict_flag: Arc::clone(&evict_flags[1]),
                    recv_buf: Vec::with_capacity(4096),
                    send_buf: Vec::with_capacity(512 * 1024),
                    last_send: now,
                    // Placeholder until the slot engages — re-seeded on
                    // every handshake.
                    sent: SentHighWater::seed(0, 0),
                },
            ],
            cursors: ReplicaCursors::new(
                replication_cursor,
                fastest_replica_cursor,
                metrics.clone(),
            ),
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
        let cursors = &self.cursors;
        let journal_path = &self.journal_path;
        let replica_ready = &self.replica_ready;
        let replicas_connected = &self.replicas_connected;
        let metrics = &self.metrics;
        let batch_size = self.batch_size;
        let heartbeat_interval = self.heartbeat_interval;

        // Check eviction flags from the journal stage.
        for (i, slot) in slots.iter_mut().enumerate() {
            let evicting =
                slot.evict_flag.load(Ordering::Acquire) && !matches!(slot.state, SlotState::Idle);
            if !evicting {
                continue;
            }
            metrics.evictions_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                slot = i,
                "evicting slow replica (ring backpressure timeout, DPDK)"
            );
            if let SlotState::Streaming(h) | SlotState::Handshaking(h) = slot.state {
                transport.close(h);
            }
            // Disengage the slot's cursors BEFORE the active_flag
            // Release — see `ReplicaCursors` for the ordering contract
            // (B2) and why the shared min must be recomputed from the
            // surviving slot (a frozen min stops the primary from
            // acking client requests even though the surviving replica
            // is healthy).
            cursors.clear_on_disconnect(i);
            metrics.catching_up[i].store(false, Ordering::Relaxed);
            slot.active_flag.store(false, Ordering::Release);
            slot.evict_flag.store(false, Ordering::Release);
            slot.recv_buf.clear();
            // Drop any unread ring entries so a reconnecting replica
            // on this slot doesn't replay pre-eviction data and stall
            // the primary's replication cursor. See kernel-TCP path
            // in tcp_sender.rs for the detailed rationale.
            slot.consumer.skip_to_producer();
            slot.state = SlotState::Idle;
            replicas_connected.fetch_sub(1, Ordering::Release);
            if replicas_connected.load(Ordering::Relaxed) == 0 {
                warn!("all replicas disconnected — trading halted");
            }
        }

        let mut any_active = false;

        for (slot_idx, slot) in slots.iter_mut().enumerate() {
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
                        slot.recv_buf.clear();
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        cursors.clear_on_disconnect(slot_idx);
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
                                            cursors.clear_on_disconnect(slot_idx);
                                            continue;
                                        }
                                    };

                                    compact_recv_buf(&mut slot.recv_buf, frame_end);

                                    // DPDK publisher: queue_send + poll to keep
                                    // smoltcp timers alive during bulk transfer.
                                    let mut dpdk_publish = |buf: &[u8]| -> std::io::Result<()> {
                                        loop {
                                            if transport.queue_send(handle, buf) {
                                                break;
                                            }
                                            transport.poll();
                                            if !transport.is_active(handle) {
                                                return Err(std::io::Error::other(
                                                    "replica disconnected during send (TX backpressure)",
                                                ));
                                            }
                                        }
                                        transport.poll();
                                        Ok(())
                                    };

                                    // Highest sequence streamed during catch-up /
                                    // snapshot transfer — monotonic from the
                                    // handshake value. Seeds the slot's sent
                                    // high-water mark (heartbeats + ack-sanity
                                    // bound) below.
                                    let mut catchup_end = h.last_sequence;
                                    let catchup_err = if can_catch_up {
                                        slot.send_buf.clear();
                                        melin_transport_core::replication::catchup::lineage_origin(
                                            journal_path,
                                        )
                                        .and_then(|(lineage_start, lineage_anchor)| {
                                            encode_stream_start(
                                                h.last_sequence,
                                                lineage_start,
                                                lineage_anchor,
                                                &mut slot.send_buf,
                                            );
                                            dpdk_publish(&slot.send_buf)
                                        })
                                        .and_then(|()| {
                                            match catch_up_from_journal_with::<A::Event>(
                                                journal_path,
                                                h.last_sequence,
                                                &mut dpdk_publish,
                                                shutdown,
                                            )? {
                                                CatchUpResult::Ok(end) => {
                                                    catchup_end = end;
                                                    Ok(())
                                                }
                                                CatchUpResult::NeedSnapshot => {
                                                    Err(io::Error::other(
                                                        "catch-up failed unexpectedly after probe",
                                                    ))
                                                }
                                            }
                                        })
                                        .err()
                                    } else {
                                        match snapshot_transfer_with::<A::Event>(
                                            journal_path,
                                            &mut dpdk_publish,
                                            shutdown,
                                        ) {
                                            Ok(CatchUpResult::Ok(end)) => {
                                                catchup_end = end;
                                                None
                                            }
                                            Ok(CatchUpResult::NeedSnapshot) => {
                                                Some(io::Error::other(
                                                    "catch-up failed even after snapshot transfer",
                                                ))
                                            }
                                            Err(e) => Some(e),
                                        }
                                    };

                                    if let Some(e) = catchup_err {
                                        warn!(slot = slot_idx, error = %e, "catch-up/snapshot failed — disconnecting");
                                        transport.close(handle);
                                        slot.state = SlotState::Idle;
                                        slot.recv_buf.clear();
                                        metrics.catching_up[slot_idx]
                                            .store(false, Ordering::Relaxed);
                                        replicas_connected.fetch_sub(1, Ordering::Release);
                                        cursors.clear_on_disconnect(slot_idx);
                                        continue;
                                    }

                                    // Engage this slot's cursors and seed the gauge
                                    // pair BEFORE the bridge flips active so a reader
                                    // that observes active=true also observes a
                                    // non-zero cursor pair — see `ReplicaCursors` for
                                    // the ordering contract.
                                    cursors.seed_on_handshake(slot_idx, h.last_sequence);

                                    // Bridge into live streaming: activates the
                                    // ring, re-reads from the journal the entries
                                    // that fell into the activation window, then
                                    // drains the ring into sequence-contiguity
                                    // (back-filling from disk if a skipped entry
                                    // hasn't flushed yet) before going live. The
                                    // bridge closes the catch-up→live gap under load
                                    // (the receiver's contiguity gate backstops only
                                    // the rare quiescent corner) — see
                                    // `bridge_catchup_to_live`.
                                    // Forwards via the retrying DPDK publisher — the
                                    // drain may leave bytes in the TX queue, so the
                                    // previous fire-and-forget `queue_send` would
                                    // silently drop chunks here. Returns the slot's
                                    // sent high-water (heartbeats + ack-sanity bound).
                                    match bridge_catchup_to_live::<A::Event>(
                                        journal_path,
                                        h.last_sequence,
                                        catchup_end,
                                        &slot.active_flag,
                                        &mut slot.consumer,
                                        &mut dpdk_publish,
                                        shutdown,
                                    ) {
                                        Ok(sent) => slot.sent = sent,
                                        Err(e) => {
                                            warn!(slot = slot_idx, error = %e, "catch-up handoff failed — disconnecting");
                                            transport.close(handle);
                                            slot.state = SlotState::Idle;
                                            slot.recv_buf.clear();
                                            metrics.catching_up[slot_idx]
                                                .store(false, Ordering::Relaxed);
                                            replicas_connected.fetch_sub(1, Ordering::Release);
                                            // Disengage cursors before clearing
                                            // active — ordering contract B2.
                                            cursors.clear_on_disconnect(slot_idx);
                                            slot.active_flag.store(false, Ordering::Release);
                                            continue;
                                        }
                                    }
                                    slot.last_send = std::time::Instant::now();

                                    replica_ready.store(true, Ordering::Release);
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
                                    cursors.clear_on_disconnect(slot_idx);
                                }
                                Err(e) => {
                                    warn!(slot = slot_idx, error = %e, "failed to decode handshake — disconnecting");
                                    transport.close(handle);
                                    slot.state = SlotState::Idle;
                                    slot.recv_buf.clear();
                                    replicas_connected.fetch_sub(1, Ordering::Release);
                                    cursors.clear_on_disconnect(slot_idx);
                                }
                            }
                        }
                        FrameResult::Oversized => {
                            warn!(slot = slot_idx, "oversized handshake frame — disconnecting");
                            transport.close(handle);
                            slot.state = SlotState::Idle;
                            slot.recv_buf.clear();
                            replicas_connected.fetch_sub(1, Ordering::Release);
                            cursors.clear_on_disconnect(slot_idx);
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
                                    && cursors.record_ack(slot_idx, &ack, slot.sent.get()).is_err()
                                {
                                    // Eviction on violation: reuse the ack-error
                                    // teardown below (the store already logged
                                    // the violation at error level).
                                    ack_error = true;
                                    break;
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
                        // Disengage cursors before the active_flag Release — see B2.
                        cursors.clear_on_disconnect(slot_idx);
                        slot.active_flag.store(false, Ordering::Release);
                        slot.recv_buf.clear();
                        slot.state = SlotState::Idle;
                        replicas_connected.fetch_sub(1, Ordering::Release);
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
                    let max_tx = transport.max_tx_queue_size(handle);
                    let used = transport.tx_queue_bytes(handle);
                    let mut available = max_tx.saturating_sub(used);

                    slot.send_buf.clear();
                    let mut batches_sent = 0;
                    let mut tx_overflow = false;
                    while batches_sent < batch_size {
                        let Some((meta, data)) = slot.consumer.try_read() else {
                            break;
                        };
                        let data_len = data.len();
                        if data_len > available {
                            // Batch doesn't fit — flush what we have so
                            // far, poll to drain the wire, then re-check
                            // with a clean slate.
                            if !slot.send_buf.is_empty() {
                                metrics.bytes_sent[slot_idx]
                                    .fetch_add(slot.send_buf.len() as u64, Ordering::Relaxed);
                                if !transport.queue_send(handle, &slot.send_buf) {
                                    slot.send_buf.clear();
                                    tx_overflow = true;
                                    break;
                                }
                                slot.send_buf.clear();
                            }
                            transport.poll();
                            let used = transport.tx_queue_bytes(handle);
                            available = max_tx.saturating_sub(used);
                            if data_len > available {
                                break;
                            }
                        }
                        slot.send_buf.extend_from_slice(data);
                        slot.consumer.commit();
                        slot.sent.advance(meta.end_sequence);
                        batches_sent += 1;
                        available = available.saturating_sub(data_len);
                    }

                    if !tx_overflow && !slot.send_buf.is_empty() {
                        metrics.bytes_sent[slot_idx]
                            .fetch_add(slot.send_buf.len() as u64, Ordering::Relaxed);
                        if !transport.queue_send(handle, &slot.send_buf) {
                            tx_overflow = true;
                        }
                    }
                    if tx_overflow {
                        slot.send_buf.clear();
                        warn!(
                            slot = slot_idx,
                            "TX overflow on replica socket — disconnecting"
                        );
                        transport.close(handle);
                        // Disengage cursors before the active_flag Release — see B2.
                        cursors.clear_on_disconnect(slot_idx);
                        slot.active_flag.store(false, Ordering::Release);
                        slot.recv_buf.clear();
                        slot.state = SlotState::Idle;
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        if replicas_connected.load(Ordering::Relaxed) == 0 {
                            warn!("all replicas disconnected — trading halted");
                        }
                        continue;
                    }
                    if !slot.send_buf.is_empty() {
                        // Flush immediately so replication data hits the
                        // wire without waiting for the next outer-loop
                        // poll. Without this, a client-traffic burst
                        // starves the replication TX path: the TxQueue
                        // fills, the driver backs off, the ring overflows,
                        // and the response gate freezes the exchange.
                        transport.poll();
                        slot.last_send = std::time::Instant::now();
                    }

                    // 3. Heartbeat if idle.
                    if batches_sent == 0 && slot.last_send.elapsed() >= heartbeat_interval {
                        slot.send_buf.clear();
                        encode_heartbeat(slot.sent.get(), &mut slot.send_buf);
                        transport.queue_send(handle, &slot.send_buf);
                        slot.last_send = std::time::Instant::now();
                    }

                    // 4. Check for disconnect.
                    if !transport.is_active(handle) {
                        warn!(slot = slot_idx, "replica disconnected (DPDK)");
                        // Disengage cursors before the active_flag Release — see B2.
                        cursors.clear_on_disconnect(slot_idx);
                        slot.active_flag.store(false, Ordering::Release);
                        slot.recv_buf.clear();
                        slot.state = SlotState::Idle;
                        replicas_connected.fetch_sub(1, Ordering::Release);
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
    //
    // Recover whenever any journal segment survives — live OR archived;
    // a post-rotation crash leaves archives with no live segment, and
    // recovery handles that layout (see the kernel-TCP receiver).
    let lineage_exists =
        journal_path.exists() || !melin_journal::segment::list_archives(journal_path)?.is_empty();
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) = if lineage_exists {
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
            last_sequence = p.last_seq.load().get();
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
        // 02:00:<IP-bytes> convention set by dpdk-setup.sh, matching
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
        // Replication streams large journal batches; use bigger RX buffer
        // so smoltcp can advertise a window large enough to sustain throughput.
        const REPL_RX_BUF: usize = 512 * 1024;
        const REPL_TX_BUF: usize = 64 * 1024;
        const REPL_TX_QUEUE: usize = 64 * 1024;
        let handle = transport.connect_to_with_buffers(
            primary_ip,
            primary_port,
            local_port,
            REPL_RX_BUF,
            REPL_TX_BUF,
            REPL_TX_QUEUE,
        );
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
        // `None` from the loop = failure path (disconnect or snapshot
        // error) → reconnect. `Some(lineage)` = StreamStart received.
        // After a snapshot transfer, the next StreamStart's lineage must
        // agree with the snapshot the primary just sent (see the
        // kernel-TCP receiver for the rationale).
        let mut expected_post_snapshot: Option<(u64, [u8; 32])> = None;
        let stream_lineage: Option<(u64, [u8; 32])> = 'handshake: loop {
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
                            segment_start_sequence,
                            anchor_hash,
                        } => {
                            if let Some((expected_start, expected_anchor)) = expected_post_snapshot
                                && (segment_start_sequence != expected_start
                                    || anchor_hash != expected_anchor)
                            {
                                fatal_err_dpdk!(
                                    format!(
                                        "post-snapshot StreamStart lineage (start \
                                     {segment_start_sequence}) disagrees with the \
                                     transferred snapshot (expected start \
                                     {expected_start}) — inconsistent primary"
                                    )
                                    .into()
                                );
                            }
                            info!(start_sequence, "streaming started (DPDK)");
                            break 'handshake Some((segment_start_sequence, anchor_hash));
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

                                    // After snapshot, expect a StreamStart
                                    // whose lineage matches what was just
                                    // transferred.
                                    expected_post_snapshot = Some((snap_seq + 1, snap_hash));
                                    continue;
                                }
                                Err(e) => {
                                    warn!(error = %e, "snapshot transfer failed (DPDK) — retrying");
                                    transport.close(handle);
                                    sleep_checking_flags(backoff, shutdown, promote);
                                    backoff = (backoff * 2).min(MAX_BACKOFF);
                                    break 'handshake None; // caught by the None check below
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
                break None; // trigger reconnect via the None check below
            }
            std::thread::yield_now();
        };

        // `None` means the handshake loop exited via a failure path
        // (disconnect or snapshot error) — reconnect.
        let Some((lineage_start, lineage_anchor)) = stream_lineage else {
            continue;
        };

        // Create journal for fresh replica (first connection only).
        //
        // Gate on `pipeline.is_none()` rather than `journal_writer.is_none()`:
        // the writer is moved into the pipeline on first connect and never
        // returned, so on every subsequent reconnect `journal_writer` is
        // `None` even though a live writer is still mid-stream inside the
        // pipeline. `pipeline.is_none()` distinguishes "true first connect or
        // post-snapshot rebuild" from "reconnect against an existing
        // pipeline" — the latter must not recreate the journal file.
        //
        // The StreamStart lineage carries the segment header identity
        // (starting sequence + chain anchor); creating the local segment
        // from the same identity makes the replica's segment
        // byte-identical to the primary's until the first rotation on
        // either node (rotations are local, so segment boundaries
        // diverge after that even though the entry stream stays
        // identical).
        if pipeline.is_none() && journal_writer.is_none() {
            let writer = W::create_continuing(journal_path, lineage_start, lineage_anchor)?;
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
            // children would inherit `cores.reader` + FIFO and never
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
            melin_app::affinity::pin_thread("receiver", cores.reader);
        }

        // --- Streaming session (transport-agnostic) ---
        let result = {
            let p = pipeline.as_mut().expect("pipeline must exist by here");
            let input_producer = &mut p.input_producer;
            let journal_cursor = p.journal_cursor.as_ref();
            let mut dpdk_transport = DpdkReceiverTransport {
                transport: &mut transport,
                handle,
                send_buf: std::mem::take(&mut send_buf),
            };
            // `last_sequence` is the session's resume point — updated to
            // the snapshot sequence after a transfer, so it anchors the
            // contiguity gate on both negotiation paths.
            let r = streaming_loop::<DpdkReceiverTransport<'_>, A::Event>(
                &mut dpdk_transport,
                input_producer,
                journal_cursor,
                shutdown,
                promote,
                pipeline_depth,
                busy_spin,
                last_sequence,
                std::mem::take(&mut recv_buf),
                None,
            );
            send_buf = dpdk_transport.send_buf;
            r
        };

        // Publish sentinel for terminal exits.
        if !matches!(result.exit, SessionExit::Disconnected)
            && let Some(p) = pipeline.as_mut()
        {
            p.input_producer.publish(
                melin_transport_core::pipeline::InputSlot::<A::Event>::shutdown_sentinel(),
            );
        }

        match result.exit {
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

        if result.received_data {
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
