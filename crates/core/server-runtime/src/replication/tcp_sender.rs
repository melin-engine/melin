//! TCP replication sender (primary side).
//!
//! Listens for replica connections, handles authentication, journal
//! catch-up, snapshot transfer, and live streaming. The wire protocol
//! is defined in `super::protocol`.

use std::io::{self, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use tracing::{debug, error, info, warn};

use melin_journal::replication::ReplicationConsumer;

use super::auth::authenticate_replica;
use super::{ReplicaCursors, ReplicationMetrics, SentHighWater};
use melin_app::Application;
use melin_transport_core::replication::catchup::{
    CatchUpResult, bridge_catchup_to_live, can_catch_up_from_journal, catch_up_from_journal,
    snapshot_transfer_with,
};
use melin_transport_core::replication::protocol::{
    MAX_CONTROL_FRAME, ReplicaMessage, decode_replica_message, encode_heartbeat,
    encode_stream_start, read_frame,
};

// --- Replication Sender (Primary side) ---

/// Owned state for the replication sender thread.
pub struct Sender {
    pub bind_addr: SocketAddr,
    pub repl_consumer_1: ReplicationConsumer,
    pub repl_consumer_2: ReplicationConsumer,
    pub replication_cursor: Arc<AtomicU64>,
    pub fastest_replica_cursor: Arc<AtomicU64>,
    pub journal_path: std::path::PathBuf,
    pub authorized_keys: Arc<melin_app::auth::AuthorizedKeys>,
    pub evict_flags: [Arc<AtomicBool>; 2],
    pub active_flags: [Arc<AtomicBool>; 2],
    pub metrics: Arc<ReplicationMetrics>,
    pub handler_cores: [usize; 2],
    pub batch_size: usize,
    pub heartbeat_secs: u64,
    pub busy_spin: bool,
    /// Node fencing state. Read to stamp the primary's epoch onto each
    /// `StreamStart`, and to self-demote when a replica handshakes with a
    /// higher epoch (this primary has been superseded). See `crate::fence`.
    pub fence_state: Arc<melin_transport_core::fence::FenceState>,
}

/// Run the replication sender. Listens for replica connections,
/// streams journal data batches, processes acks, and updates the
/// replication cursor.
///
/// Runs on a dedicated thread. Blocks until shutdown.
pub fn run_sender<A: Application>(
    config: Sender,
    shutdown: &AtomicBool,
    replica_ready: &AtomicBool,
    replicas_connected: &AtomicU32,
) {
    let Sender {
        bind_addr,
        repl_consumer_1,
        repl_consumer_2,
        replication_cursor,
        fastest_replica_cursor,
        journal_path,
        authorized_keys,
        evict_flags,
        active_flags,
        metrics,
        handler_cores,
        batch_size,
        heartbeat_secs,
        busy_spin,
        fence_state,
    } = config;
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

    // Single owner of the per-replica progress cursors (per-slot acked
    // positions, shared min/max, and the gate's gauge pair). Shared by
    // both handler threads — see `ReplicaCursors` for the ordering
    // contract relative to the active flags.
    let cursors = Arc::new(ReplicaCursors::new(
        Arc::clone(&replication_cursor),
        Arc::clone(&fastest_replica_cursor),
        Arc::clone(&metrics),
    ));

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
                    Ok(mut consumer) => {
                        // Drop any unread entries before the consumer is
                        // stashed back in the slot. The journal stage may
                        // have published batches into the ring that the
                        // evicted handler never got to forward — if we
                        // left them in place, the NEXT handler on this
                        // slot would drain them to its replica and
                        // acknowledge with pre-eviction sequences. Those
                        // acks would stall `replication_cursor` at the
                        // old position and gate the primary's response
                        // stage at the slow-replica rate.
                        //
                        // Fast-forward to the producer cursor so the
                        // live-streaming loop starts with a clean ring.
                        consumer.skip_to_producer();
                        slot.consumer = Some(consumer);
                        replicas_connected.fetch_sub(1, Ordering::Release);
                        // Disengage the slot's cursors BEFORE clearing the
                        // active flag — see `ReplicaCursors` for the
                        // ordering contract (B2).
                        cursors.clear_on_disconnect(i);
                        metrics.catching_up[i].store(false, Ordering::Relaxed);
                        // Clear active flag — journal stage stops publishing
                        // to this ring. Must happen before clearing evict.
                        active_flags[i].store(false, Ordering::Release);
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
                    // SO_BUSY_POLL on the sender side: the per-replica thread
                    // spins on `recv` for ack frames, so kernel busy-polling
                    // removes the softirq->wakeup handoff from the ack path.
                    if let Err(e) =
                        crate::server::set_busy_poll(&stream, crate::server::BUSY_POLL_US)
                    {
                        debug!(error = %e, "failed to set SO_BUSY_POLL on replica connection");
                    }

                    replicas_connected.fetch_add(1, Ordering::Release);

                    // Take the consumer out of the slot for the handler thread.
                    // The slot's consumer becomes None while the thread owns it.
                    let consumer = slots[slot_idx]
                        .consumer
                        .take()
                        .expect("empty_slot check guarantees consumer is Some");

                    let slot_cursors = Arc::clone(&cursors);
                    let jpath = journal_path.clone();
                    let auth_keys = Arc::clone(&authorized_keys);
                    let slot_metrics = Arc::clone(&metrics);
                    let slot_active = Arc::clone(&active_flags[slot_idx]);
                    let slot_evict = Arc::clone(&evict_flags[slot_idx]);
                    let slot_fence = Arc::clone(&fence_state);
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
                                match melin_app::affinity::pin_to_core(handler_core) {
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
                            } else if let Err(e) = melin_app::affinity::clear_affinity() {
                                tracing::warn!(error = e, "failed to clear handler affinity");
                            }
                            // Safety: shutdown and replica_ready outlive this thread
                            // (they're on the parent's stack, which blocks on join
                            // during shutdown).
                            let shutdown_ref = unsafe { &*(shutdown_flag as *const AtomicBool) };
                            let ready_ref = unsafe { &*(ready_flag as *const AtomicBool) };
                            let ctx = SlotContext {
                                cursors: &slot_cursors,
                                journal_path: &jpath,
                                authorized_keys: &auth_keys,
                                shutdown: shutdown_ref,
                                replica_ready: ready_ref,
                                active_flag: &slot_active,
                                evict_flag: &slot_evict,
                                metrics: &slot_metrics,
                                fence_state: &slot_fence,
                                slot_idx,
                                batch_size,
                                heartbeat_secs,
                                busy_spin,
                            };
                            run_replica_slot::<A>(stream, consumer, &ctx)
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

/// Per-slot state shared across the replica handler call chain
/// (`run_replica_slot` → `handle_replica_connection` → `live_stream_uring`).
struct SlotContext<'a> {
    cursors: &'a Arc<ReplicaCursors>,
    journal_path: &'a std::path::Path,
    authorized_keys: &'a melin_app::auth::AuthorizedKeys,
    shutdown: &'a AtomicBool,
    replica_ready: &'a AtomicBool,
    active_flag: &'a AtomicBool,
    evict_flag: &'a AtomicBool,
    metrics: &'a ReplicationMetrics,
    fence_state: &'a melin_transport_core::fence::FenceState,
    slot_idx: usize,
    batch_size: usize,
    heartbeat_secs: u64,
    busy_spin: bool,
}

/// Handle a single replica connection on a dedicated thread.
/// Returns the consumer when the connection ends (for slot reuse).
fn run_replica_slot<A: Application>(
    stream: TcpStream,
    mut consumer: ReplicationConsumer,
    ctx: &SlotContext<'_>,
) -> ReplicationConsumer {
    match handle_replica_connection::<A>(stream, &mut consumer, ctx) {
        Ok(()) => info!("replica disconnected cleanly"),
        Err(e) => warn!(error = %e, "replica connection error"),
    }
    consumer
}

fn handle_replica_connection<A: Application>(
    stream: TcpStream,
    repl_consumer: &mut ReplicationConsumer,
    ctx: &SlotContext<'_>,
) -> io::Result<()> {
    let SlotContext {
        cursors,
        journal_path,
        authorized_keys,
        shutdown,
        replica_ready,
        active_flag,
        evict_flag: _,
        metrics,
        fence_state,
        slot_idx,
        batch_size: _,
        heartbeat_secs,
        busy_spin: _,
    } = ctx;
    let slot_idx = *slot_idx;
    let heartbeat_secs = *heartbeat_secs;

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
        replica_epoch = handshake.epoch,
        "replica handshake received"
    );

    // Fence: a replica advertising an epoch higher than ours means a
    // promotion happened that this node missed — we are a stale ex-primary.
    // The policy (latch fenced + trigger shutdown) lives on `FenceState` so
    // the kernel-TCP and DPDK senders cannot drift; the matching stage
    // halts new client writes, the response gate stops acking, and the node
    // winds down for the operator to restart it as a replica. Refuse this
    // connection so we send no stale stream.
    let our_epoch = fence_state.epoch();
    if let Some(first_latch) = fence_state.fence_if_superseded(handshake.epoch, shutdown) {
        if first_latch {
            error!(
                replica_epoch = handshake.epoch,
                our_epoch,
                "fenced: a replica advertises a higher epoch — this primary has been \
                 superseded; self-demoting and shutting down"
            );
        }
        return Err(io::Error::other("fenced by higher-epoch replica"));
    }

    // Mark this slot as catching up. Cleared when entering the live loop.
    metrics.catching_up[slot_idx].store(true, Ordering::Relaxed);

    let mut send_buf = Vec::with_capacity(128);

    // Probe whether journal catch-up is possible before committing to
    // a protocol path. This avoids sending StreamStart only to discover
    // the journals are too old.
    let can_catch_up = can_catch_up_from_journal(journal_path, handshake.last_sequence)?;

    let mut publish = |buf: &[u8]| -> io::Result<()> {
        writer.write_all(buf)?;
        writer.flush()
    };

    let catchup_end = if can_catch_up {
        // The lineage origin (oldest segment's header identity) lets a
        // fresh replica create a byte-identical journal before
        // consuming the stream. Replicas with local state ignore it.
        let (lineage_start, lineage_anchor) =
            melin_transport_core::replication::catchup::lineage_origin(journal_path)?;
        encode_stream_start(
            handshake.last_sequence,
            lineage_start,
            lineage_anchor,
            fence_state.epoch(),
            &mut send_buf,
        );
        publish(&send_buf)?;
        send_buf.clear();

        match catch_up_from_journal::<A::Event>(
            journal_path,
            handshake.last_sequence,
            &mut writer,
            shutdown,
        )? {
            CatchUpResult::Ok(end) => end,
            CatchUpResult::NeedSnapshot => {
                return Err(io::Error::other("catch-up failed unexpectedly after probe"));
            }
        }
    } else {
        match snapshot_transfer_with::<A::Event>(journal_path, &mut publish, shutdown)? {
            CatchUpResult::Ok(end) => end,
            CatchUpResult::NeedSnapshot => {
                return Err(io::Error::other(
                    "catch-up failed even after snapshot transfer",
                ));
            }
        }
    };

    // Engage this slot's cursors and seed the gauge pair the response
    // gate's `evaluate_durability` reads. Must happen BEFORE the
    // bridge's active_flag Release — see `ReplicaCursors` for the
    // ordering contract.
    cursors.seed_on_handshake(slot_idx, handshake.last_sequence);

    // Bridge into live streaming: activates the ring, re-reads from the
    // journal the entries that fell into the activation window, then
    // drains the ring into sequence-contiguity (back-filling from disk
    // if a skipped entry hasn't flushed yet) before going live. Returns
    // the slot's sent high-water — the bound the ack-sanity invariant
    // checks acks against, advanced by every chunk the live loop
    // forwards. The bridge closes the catch-up→live gap under load (the
    // receiver's contiguity gate backstops only the rare quiescent
    // corner). See `bridge_catchup_to_live`.
    let mut sent = {
        let mut publish = |buf: &[u8]| -> io::Result<()> {
            writer.write_all(buf)?;
            writer.flush()
        };
        bridge_catchup_to_live::<A::Event>(
            journal_path,
            handshake.last_sequence,
            catchup_end,
            active_flag,
            repl_consumer,
            &mut publish,
            shutdown,
        )?
    };

    // Catch-up complete — replica is entering the live streaming loop.
    metrics.catching_up[slot_idx].store(false, Ordering::Relaxed);

    // Signal that this replica is ready to consume from the replication
    // ring. The main thread waits on this before seeding test data.
    // Must happen AFTER catch-up and overlap drain complete — otherwise
    // seeding fills the replication ring faster than we can drain it,
    // deadlocking the journal stage.
    replica_ready.store(true, Ordering::Release);

    let heartbeat_interval = std::time::Duration::from_secs(heartbeat_secs);
    let mut last_send = std::time::Instant::now();

    live_stream_uring(
        writer,
        repl_consumer,
        ctx,
        heartbeat_interval,
        &mut send_buf,
        &mut last_send,
        &mut sent,
    )
}

/// io_uring live streaming loop for the primary replication handler.
///
/// Live streaming loop using async RECV/SEND via io_uring. A single RECV is always
/// in-flight for ack frames; SEND is submitted when the replication ring
/// has data. Both complete via the memory-mapped CQ with zero syscalls
/// in the hot path.
fn live_stream_uring(
    writer: TcpStream,
    repl_consumer: &mut ReplicationConsumer,
    ctx: &SlotContext<'_>,
    heartbeat_interval: std::time::Duration,
    send_buf: &mut Vec<u8>,
    last_send: &mut std::time::Instant,
    sent: &mut SentHighWater,
) -> io::Result<()> {
    let SlotContext {
        cursors,
        shutdown,
        evict_flag,
        metrics,
        slot_idx,
        batch_size,
        busy_spin,
        // Only used during handshake/catch-up (handle_replica_connection).
        journal_path: _,
        authorized_keys: _,
        replica_ready: _,
        active_flag: _,
        heartbeat_secs: _,
        fence_state: _,
    } = ctx;
    let slot_idx = *slot_idx;
    let batch_size = *batch_size;
    let busy_spin = *busy_spin;

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
    let mut heartbeat_timer = melin_app::amortized_timer::AmortizedTimer::new();

    // Diagnostic (RUST_LOG=debug): per-slot TCP_INFO snapshot once a
    // second, slow-SEND detection (CQE elapsed >= threshold), and a
    // TCP_INFO capture at the evict-exit point. Amortized so the
    // per-iteration cost is a single `AND` + predictable branch.
    let mut info_log_timer = melin_app::amortized_timer::AmortizedTimer::new();
    let mut send_submit_ts: Option<std::time::Instant> = None;
    const SLOW_SEND_THRESHOLD_MS: u128 = 5;

    // Submit initial RECV.
    let sqe = opcode::Recv::new(
        types::Fixed(0),
        recv_buf.as_mut_ptr(),
        recv_buf.len() as u32,
    )
    .build()
    .user_data(TOKEN_RECV);
    // SAFETY: `recv_buf` is owned by this task and lives across the
    // event loop until the corresponding CQE is drained. The ring is
    // single-threaded — only this task pushes/reaps on it.
    unsafe { ring.submission().push(&sqe).expect("SQ full") };

    loop {
        // --- Check flags ---
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        if evict_flag.load(Ordering::Relaxed) {
            // Capture the TCP state at the moment of eviction — the
            // critical frame for comparing an evicted slot's teardown
            // against the still-live slot's socket state when
            // diagnosing post-eviction regressions.
            super::log_tcp_info(tcp_fd, "evict_exit", slot_idx);
            info!(slot = slot_idx, "handler exiting: evicted by journal stage");
            return Ok(());
        }

        // --- Drain replication ring into send_buf (memory, non-blocking) ---
        //
        // Ring chunks are wire-ready `InputBatch` frames produced by the
        // journal stage (phase 3 of the unified-pipeline plan), so the
        // sender is a passthrough — no decode + re-encode here.
        if !send_in_flight {
            let mut coalesced = 0;
            while coalesced < batch_size {
                if let Some((meta, data)) = repl_consumer.try_read() {
                    send_buf.extend_from_slice(data);
                    repl_consumer.commit();
                    sent.advance(meta.end_sequence);
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
                // SAFETY: `send_buf` is owned by this task and pinned
                // by `send_in_flight = true` below — it isn't mutated
                // until the matching TOKEN_SEND CQE clears the flag.
                // The ring is single-threaded.
                unsafe { ring.submission().push(&sqe).expect("SQ full") };
                send_in_flight = true;
                send_offset = 0;
                *last_send = std::time::Instant::now();
                send_submit_ts = Some(*last_send);
                idle_spins = 0;
                heartbeat_timer = melin_app::amortized_timer::AmortizedTimer::new();
            } else {
                // Heartbeat check: amortized when spinning (mask keeps the
                // clock read at ~10/s at 10M iter/s). In yield mode the loop
                // already pays a syscall per iteration, so the clock read is
                // free and must not be skipped — see AmortizedTimer docs.
                let spinning = busy_spin || idle_spins < 1000;
                if heartbeat_timer.tick(heartbeat_interval, spinning).is_some() {
                    encode_heartbeat(sent.get(), send_buf);
                    let sqe = opcode::Send::new(
                        types::Fixed(0),
                        send_buf.as_ptr(),
                        send_buf.len() as u32,
                    )
                    .build()
                    .user_data(TOKEN_SEND);
                    // SAFETY: same as the coalesced send above —
                    // `send_buf` is owned and pinned by `send_in_flight`
                    // until the TOKEN_SEND CQE clears it.
                    unsafe { ring.submission().push(&sqe).expect("SQ full") };
                    send_in_flight = true;
                    send_offset = 0;
                    *last_send = std::time::Instant::now();
                    send_submit_ts = Some(*last_send);
                }
            }
        }

        // Periodic TCP_INFO dump — debug level. Amortized so the
        // per-iteration cost is a single `AND` + predictable branch.
        if info_log_timer
            .tick(
                std::time::Duration::from_secs(1),
                busy_spin || idle_spins < 1000,
            )
            .is_some()
        {
            super::log_tcp_info(tcp_fd, "live_stream", slot_idx);
        }

        // --- Submit SQEs to kernel (non-blocking) ---
        // Skip the syscall when no new SQEs were pushed — an empty
        // io_uring_enter still costs ~200 ns of mode-switch overhead and
        // showed up as 6 % of total CPU in profiles of the sender loop.
        let pending = ring.submission().len();
        if pending > 0 {
            ring.submit()
                .map_err(|e| io::Error::other(format!("io_uring submit: {e}")))?;
        }

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
                        let frame_len = u32::from_le_bytes(
                            parse_buf[cursor..cursor + 4]
                                .try_into()
                                .expect("bounds checked: 4-byte slice"),
                        ) as usize;
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
                            // Eviction on violation: returning Err tears the
                            // connection down; the accept loop's cleanup
                            // disengages the cursors and frees the slot.
                            cursors
                                .record_ack(slot_idx, &ack, sent.get())
                                .map_err(io::Error::other)?;
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
                    // SAFETY: `recv_buf` is owned by this task; only
                    // one RECV is ever in flight (we just consumed the
                    // previous CQE). Ring is single-threaded.
                    unsafe { ring.submission().push(&sqe).expect("SQ full") };
                }

                TOKEN_SEND => {
                    if result < 0 {
                        return Err(io::Error::other(format!("send error (returned {result})")));
                    }
                    let sent = result as usize;
                    send_offset += sent;
                    if send_offset >= send_buf.len() {
                        // Fully sent. Measure end-to-end SEND latency: a
                        // healthy io_uring TCP SEND completes in
                        // microseconds; > threshold implies the kernel
                        // waited on cwnd / peer window / retransmit.
                        if let Some(ts) = send_submit_ts.take() {
                            let elapsed = ts.elapsed();
                            if elapsed.as_millis() >= SLOW_SEND_THRESHOLD_MS {
                                tracing::debug!(
                                    slot = slot_idx,
                                    elapsed_us = elapsed.as_micros() as u64,
                                    bytes = send_buf.len(),
                                    "slow SEND completion"
                                );
                                super::log_tcp_info(tcp_fd, "slow_send", slot_idx);
                            }
                        }
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
                        // SAFETY: `send_buf` is owned and still pinned
                        // (`send_in_flight` stays true across partial
                        // sends); ring is single-threaded.
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
