//! io_uring-based response stage — routes matching output to connections via
//! `IORING_OP_SEND`.
//!
//! Replaces the blocking `write(2)` + `BufWriter` flush path with batched
//! io_uring sends. Instead of N `write(2)` syscalls (one per dirty connection
//! on flush), we submit N SEND SQEs in a single `io_uring_enter` call.
//!
//! Same SPSC consumption and journal cursor gating as `response.rs`.
//! Runs on a dedicated OS thread.

use std::collections::{HashMap, HashSet};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error};

use melin_pipeline::ring;

use crate::durability_policy::{
    Blocker, CursorView, DurabilityMode, EvalStatus, MAX_CLUSTER_SIZE, Policy,
};
use crate::replication::ReplicationMetrics;
use melin_app::Application;
use melin_app::amortized_timer::AmortizedTimer;
use melin_transport_core::pipeline::{OutputPayload, OutputSlot, StageUtilization};
#[cfg(feature = "latency-trace")]
use melin_transport_core::trace;
use melin_transport_core::{DurableWireSeqCursor, WireSeq};

use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded response size. PositionSnapshot is the largest variant
/// at up to 330 bytes (length(4) + tag(1) + account(4) + count(1) +
/// 16*(currency(4)+free(8)+reserved(8))). 512 bytes covers all variants.
const MAX_RESPONSE_BUF: usize = 512;

/// io_uring submission queue depth for sends. Must be ≥ max concurrent
/// connections to avoid SQ overflow when all connections are dirty.
/// Power of 2 for io_uring alignment. 4096 supports 1024+ client
/// benchmarks where all connections flush simultaneously.
const RING_SIZE: u32 = 4096;

/// Maximum accumulated send buffer per connection (64 KiB). If a client
/// falls behind and the buffer exceeds this, the connection is dropped.
/// 64 KiB holds ~500 response frames — well beyond any reasonable lag.
const MAX_SEND_BUF: usize = 64 * 1024;

pub use crate::ControlEvent;

/// Encoder type alias: response encoder bound to the application's
/// `Report` / `QueryResponse` types. Hides the long
/// `dyn ResponseEncoder<Report = ..., Query = ...>` at call sites.
pub type ResponseEncoderArc<A> = Arc<
    dyn melin_app::encoder::ResponseEncoder<
            Report = <A as Application>::Report,
            Query = <A as Application>::QueryResponse,
        >,
>;

/// Configuration and shared state for the response stage.
pub struct Response<A: Application> {
    /// Highest wire seq durably persisted on the primary's journal.
    /// In the same sequence space as `OutputSlot.wire_seq` and the
    /// replica metrics (`metrics.in_memory_sequence` /
    /// `metrics.acked_sequence`), so the durability gate can compare
    /// these values numerically and the comparison is meaningful
    /// regardless of `starting_sequence` (fresh vs recovered primary).
    /// Typed [`DurableWireSeqCursor`] so the gate cannot be wired to a
    /// ring-space counter — the pre-v14 bug class. Updated by the
    /// journal stage after every fsync batch via `set_last_seq_publisher`.
    pub journal_persisted_wire_seq: DurableWireSeqCursor,
    /// Operator-selected durability mode, published through a shared
    /// [`AtomicU8`] so the admin `DURABILITY` command can swap it at
    /// runtime without restarting the node. The response stage reads
    /// this once per gate iteration with a relaxed load (cheaper than a
    /// `Mutex` or refcounted `Arc<Policy>` snapshot) and rebuilds its
    /// local [`Policy`] when the byte changes. See
    /// [`crate::durability_policy::DurabilityMode::as_u8`] for the
    /// encoding.
    pub durability_mode: Arc<std::sync::atomic::AtomicU8>,
    /// Per-slot replica cursors. `None` for standalone deployments
    /// (no replication wiring) — the policy then evaluates against the
    /// primary alone.
    pub replication_metrics: Option<Arc<ReplicationMetrics>>,
    /// Per-slot replica active flags. Only "true" slots are included in
    /// the cursor view fed to `Policy::evaluate`, so disconnected slots
    /// don't pollute the view with stale zero cursors. When the
    /// resulting view is too small to satisfy a clause, the policy
    /// reports degraded and the gate stalls.
    /// Mirrors `replication_metrics` — `None` in standalone.
    pub replica_active: Option<[Arc<AtomicBool>; 2]>,
    pub heartbeat_interval: Option<Duration>,
    pub busy_spin: bool,
    pub utilization: Arc<StageUtilization>,
    /// Wire encoder for application-shaped payloads. Constructed
    /// once at boot (`Arc::new(ExchangeResponseEncoder)`) and shared
    /// with the DPDK response stage.
    pub encoder: ResponseEncoderArc<A>,
    /// Node fencing state. When latched (a higher epoch was observed), the
    /// stage exits *without* the best-effort flush — in-flight responses
    /// for orders on a superseded epoch must not be acknowledged. Fencing
    /// co-sets `shutdown`, so the latch is only consulted on the shutdown
    /// path (zero steady-state cost). See `crate::fence`.
    pub fence_state: Arc<melin_transport_core::fence::FenceState>,
}

/// Per-connection state for batched io_uring sends.
struct ConnectionEntry {
    fd: RawFd,
    /// Owns the write half of the socket to keep the fd alive.
    _owner: Box<dyn Send>,
    /// Accumulates encoded response frames between flushes.
    /// The full wire frame (length prefix + payload) is appended here.
    /// Vec's internal data pointer is heap-stable, so io_uring SEND SQEs
    /// referencing `as_ptr()` remain valid even if the HashMap relocates
    /// this struct — as long as we don't reallocate the Vec during in-flight sends.
    send_buf: Vec<u8>,
    /// Last time data was sent to this connection. Used for heartbeat scheduling.
    last_send: Instant,
}

/// Run the io_uring response stage loop. Blocks the calling thread until shutdown.
///
/// Consumes from the output SPSC, waits for durability confirmation, and
/// sends responses via io_uring SEND.
///
/// Durability gating: every gate iteration reads the journal cursor
/// (primary persisted) plus per-slot replica cursors (in-memory and
/// persisted) from `replication_metrics` and feeds them through the
/// configured [`Policy`]. See [`evaluate_durability`].
pub fn run<A: Application>(
    mut consumer: ring::Consumer<OutputSlot<A::Report, A::QueryResponse>>,
    control_rx: mpsc::Receiver<ControlEvent>,
    config: Response<A>,
    shutdown: &AtomicBool,
) {
    let Response {
        journal_persisted_wire_seq,
        durability_mode,
        replication_metrics,
        replica_active,
        heartbeat_interval,
        busy_spin,
        utilization,
        encoder,
        fence_state,
    } = config;
    // Resolve the starting mode from the shared atomic and derive the
    // local Policy. The atomic is the single source of truth across the
    // process lifetime; the response thread keeps a thread-local copy
    // for cheap per-iteration use and rebuilds it when an admin
    // `DURABILITY` command swaps the atomic. Initialise as Hybrid (the
    // default mode) if the atomic ever holds a corrupted byte — better
    // than panicking on a degraded process and matches the default
    // operators see at boot.
    let mut active_mode =
        DurabilityMode::from_u8(durability_mode.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or_else(|| {
                tracing::error!(
                    "durability_mode atomic held a corrupted byte at startup; defaulting to hybrid"
                );
                DurabilityMode::Hybrid
            });
    let mut policy = active_mode.to_policy();
    let mut ring =
        IoUring::new(RING_SIZE).expect("failed to create io_uring instance for response stage");

    // Connection table: maps connection IDs to their state.
    // HashMap for O(1) lookup. Pre-sized for a reasonable number of concurrent clients.
    let mut connections: HashMap<u64, ConnectionEntry> = HashMap::with_capacity(256);

    let mut batch = [OutputSlot::<A::Report, A::QueryResponse>::default(); MAX_BATCH];
    let mut encode_buf = [0u8; MAX_RESPONSE_BUF];

    // Cached durability position to avoid atomic reads on every slot.
    // Initialised below from the policy's startup evaluation; updated
    // via `evaluate_durability` on every gate iteration.
    let mut cached_durable_pos: u64;

    // Degradation logger. Tracks transitions, suppresses sub-second
    // flap noise, and drives the `/healthz` `policy_degraded` gauge.
    // See `DegradationLogger` for the full state machine. Initialised
    // below from the policy's startup evaluation so an unsatisfiable
    // policy (e.g. a primary that just lost both replicas while
    // running `hybrid` or `durably-replicated`) is visible immediately
    // on `/healthz` and in the journal.
    let startup_now = Instant::now();
    let mut last_policy_check = startup_now;
    /// Re-emit interval for the "still degraded" reminder.
    const DEGRADED_LOG_INTERVAL: Duration = Duration::from_secs(5);
    /// Cadence at which the idle path re-evaluates the policy. Bounds
    /// the lag between a connection-state change and the `/healthz`
    /// gauge / warn-log reflecting it. Cheap (a handful of atomic
    /// loads + the policy evaluator) at this rate.
    const POLICY_CHECK_INTERVAL: Duration = Duration::from_secs(1);
    /// Cadence at which the gate-wait spin folds elapsed time into the
    /// degraded-duration counter while the durability gate is stalled.
    /// Tighter than the idle cadence — it bounds the boundary error when
    /// a degradation begins or flips mid-wedge, which matters most for
    /// short stalls. The accrual tick is gated by this period, but the
    /// clock read behind it is gated by the `AmortizedTimer` mask, so the
    /// effective resolution is `max(this, the mask's clock-read cadence)`.
    /// The mask reads the clock ~every 6.5 ms at this spin's rate
    /// (`AmortizedTimer::CHECK_MASK` is `2^16`), finer than the 10 ms
    /// here, so this cadence is the binding one and is realized in full.
    const GATE_ACCRUAL_INTERVAL: Duration = Duration::from_millis(10);

    // Initial evaluation so the cached durable position and the
    // `/healthz` gauge reflect the cluster's startup shape before
    // the first batch arrives.
    let mut degraded_logger;
    {
        let journal_pos = journal_persisted_wire_seq.load();
        let metrics_ref = replication_metrics.as_deref();
        let active_ref = replica_active.as_ref();
        let status = evaluate_durability(&policy, journal_pos, metrics_ref, active_ref);
        cached_durable_pos = status.durable_pos;
        utilization
            .policy_degraded
            .store(status.degraded, Ordering::Relaxed);
        degraded_logger = if status.degraded {
            DegradationLogger::new_starting_degraded(startup_now, &policy)
        } else {
            DegradationLogger::new(startup_now)
        };
    }

    // Stage histograms registered with the global registry — see
    // `melin_transport_core::trace`. The four breakdown stages
    // (journal-wait, replica-wait, encode, egress) feed the bench's
    // tick-to-trade decomposition; spsc/dispatch/server-e2e are kept
    // alongside as overall sanity checks.
    #[cfg(feature = "latency-trace")]
    let mut spsc_rec =
        trace::register_stage("response: SPSC wakeup (matching publish → response consume)");
    #[cfg(feature = "latency-trace")]
    let mut dispatch_rec = trace::register_stage("response: dispatch (consume → socket write)");
    #[cfg(feature = "latency-trace")]
    let mut server_e2e_rec = trace::register_stage("server e2e (reader recv → response flush)");
    // Tick-to-trade breakdown: per-slot wait observed for each
    // durability path (recorded only when the gate actually held us
    // up — cache-hit paths skip to avoid inflating the metric with
    // crossings that happened before we noticed). Encode is wall-time
    // around `encode_transport_response`. Egress wraps a `flush_sends`
    // call (one sample per io_uring flush, batching many slots).
    // Gated on `tick-to-trade`, not `latency-trace`, because these
    // stages roughly double the hot-path mutex traffic vs the lighter
    // 4-stage mode.
    #[cfg(feature = "tick-to-trade")]
    let mut journal_wait_rec =
        trace::register_stage("response: journal-wait (match_complete → journal cursor crossed)");
    #[cfg(feature = "tick-to-trade")]
    let mut replica_wait_rec = trace::register_stage(
        "response: replica-wait (match_complete → replication cursor crossed)",
    );
    #[cfg(feature = "tick-to-trade")]
    let mut encode_rec = trace::register_stage("response: encode (per-kind wire encoding)");
    #[cfg(feature = "tick-to-trade")]
    let mut egress_rec = trace::register_stage("response: egress (flush_sends elapsed)");
    // Paces the idle-path recorder flush. Without it, every sample this
    // thread recorded stays in its thread-local buffer once traffic
    // stops — which is exactly when the bench scrapes /stats-dump.
    #[cfg(feature = "latency-trace")]
    let mut last_stats_flush = Instant::now();

    // Server-side end-to-end samples awaiting a flush, tagged with the
    // connection whose bytes they measure.
    //
    // The stage encodes into `send_buf` and hands the bytes to the
    // kernel later, so closing the sample at encode time would omit
    // every microsecond a response spends buffered — the interval this
    // stage most needs to be honest about, since that is exactly where a
    // deferred flush hides. Holding each frame's `recv_ts` until the
    // flush completes measures reader-recv → kernel, which is what the
    // stage name claims and what the DPDK path already records (it
    // samples after `tx_producers.flush()`).
    //
    // The connection id is what lets a connection dropped without a
    // flush have its samples thrown away rather than closed — see
    // `discard_e2e_samples`. Two rules keep the queue honest, and every
    // mutation of `dirty_connections` obeys one of them:
    //
    // - a clear-by-flush closes the queue (`close_e2e_samples`),
    // - a removal-by-drop discards that connection's entries
    //   (`discard_e2e_samples`).
    //
    // Together they give the invariant "queue non-empty implies some
    // connection is dirty", which is what makes the flush sites
    // sufficient drain points.
    //
    // `Vec` rather than a per-connection map: entries are appended and
    // then walked wholesale, order is irrelevant, and the dropped-
    // connection lists they are matched against are almost always empty.
    // A flat append is the cheapest thing that does that. Capacity is a
    // starting point, not a bound — under saturation the output ring
    // never empties, so the queue grows until something forces a flush.
    //
    // Only compiled under `latency-trace`; production builds carry
    // neither the buffer nor the pushes.
    #[cfg(feature = "latency-trace")]
    let mut pending_e2e: Vec<(u64, trace::MonoTraceInstant)> = Vec::with_capacity(MAX_BATCH);

    // Track connections with buffered (unflushed) writes across batches.
    let mut dirty_connections: HashSet<u64> = HashSet::new();

    // Connections to remove after flush (send errors).
    let mut to_remove: Vec<u64> = Vec::new();

    // Pre-allocated CQE collection buffer. Must collect CQEs before
    // processing because the CQ borrow must end before mutating connections.
    // Pre-sized to RING_SIZE to avoid per-iteration heap allocation.
    let mut cqes: Vec<(u64, i32)> = Vec::with_capacity(RING_SIZE as usize);

    // Pre-encode the heartbeat response frame once. Full wire frame
    // (length prefix + tag) for direct append to send_buf.
    let heartbeat_wire_frame = {
        let mut buf = [0u8; 8];
        let written =
            control_codec::encode_transport_response(&TransportResponse::Heartbeat, &mut buf)
                .expect("heartbeat encodes");
        buf[..written].to_vec()
    };

    // Coarse timestamp for heartbeat scan — avoids Instant::now() on every spin.
    let mut last_heartbeat_scan = Instant::now();

    // Adaptive spin: spin first (fast wakeup), yield after threshold.
    let mut idle_spins: u32 = 0;

    let mut busy_count: u64 = 0;
    let mut idle_count: u64 = 0;

    // Paces accrual ticks inside the durability gate-wait spin so the
    // degraded-duration counter keeps advancing during a hard stall.
    // Declared once at function scope (not per gate entry) so the normal
    // gated path — entered briefly whenever durability lags by a few µs —
    // pays no extra `Instant::now()`; the amortized mask only reads the
    // clock once per ~1 M cumulative spin iterations.
    let mut gate_accrual_timer = AmortizedTimer::new();

    loop {
        // Observe runtime mode swaps from the admin `DURABILITY`
        // command. Relaxed load (single writer is the admin handler,
        // single reader is this thread). When the byte changes,
        // rebuild the local Policy and reset the cached durable
        // position so the next gate evaluation starts from a clean
        // slate under the new shape; log the transition for the audit
        // trail. An unknown byte is treated as memory corruption: we
        // log and keep the prior mode rather than silently downgrading.
        let observed_byte = durability_mode.load(Ordering::Relaxed);
        if observed_byte != active_mode.as_u8() {
            match DurabilityMode::from_u8(observed_byte) {
                Some(next) => {
                    tracing::info!(
                        prev = active_mode.as_str(),
                        next = next.as_str(),
                        "durability mode swapped at runtime"
                    );
                    active_mode = next;
                    policy = active_mode.to_policy();
                    // The fresh policy may evaluate degraded/undegraded
                    // differently against the same cluster shape; let
                    // the next gate evaluation re-derive.
                    cached_durable_pos = 0;
                    // Re-seed the degradation logger so a transition
                    // out of (or into) degraded under the new policy
                    // surfaces immediately rather than waiting for the
                    // sustained-state hold to roll over. Flushes accrual
                    // first so pre-swap degraded time isn't dropped.
                    degraded_logger.reseed(&utilization, Instant::now());
                }
                None => {
                    tracing::error!(
                        byte = observed_byte,
                        "durability_mode atomic held a corrupted byte; retaining prior mode"
                    );
                }
            }
        }

        if shutdown.load(Ordering::Relaxed) {
            // Fence: a superseded ex-primary must not acknowledge any
            // further in-flight work — skip the best-effort flush so
            // responses buffered for orders on the old epoch are dropped
            // (the client sees a connection reset and reconciles on
            // reconnect). Checked only here, not per iteration: fencing
            // always co-sets `shutdown` (`FenceState::fence_if_superseded`
            // owns that invariant), so this branch is the first one a
            // fenced node reaches and the steady-state loop pays nothing.
            let flush = !fence_state.is_fenced();
            // Best-effort flush before shutdown.
            if flush && !dirty_connections.is_empty() {
                flush_sends(
                    &mut ring,
                    &mut connections,
                    &dirty_connections,
                    &mut to_remove,
                    &mut cqes,
                );
                #[cfg(feature = "latency-trace")]
                close_e2e_samples(&mut pending_e2e, &mut server_e2e_rec);
                dirty_connections.clear();
            }
            utilization.busy.store(busy_count, Ordering::Relaxed);
            utilization.idle.store(idle_count, Ordering::Relaxed);
            #[cfg(feature = "pipeline-stats")]
            print_utilization("response", busy_count, idle_count);
            return;
        }

        // Poll control channel (non-blocking) for connect/disconnect.
        while let Ok(event) = control_rx.try_recv() {
            match event {
                ControlEvent::Connected {
                    connection_id,
                    fd,
                    writer,
                } => {
                    // The writer keeps the fd alive — store it as the owner.
                    let owner: Box<dyn Send> = Box::new(writer);
                    connections.insert(
                        connection_id,
                        ConnectionEntry {
                            fd,
                            _owner: owner,
                            send_buf: Vec::with_capacity(4096),
                            last_send: Instant::now(),
                        },
                    );
                }
                ControlEvent::Disconnected { connection_id } => {
                    connections.remove(&connection_id);
                    dirty_connections.remove(&connection_id);
                    // Anything this connection had buffered goes with
                    // it, so its queued samples measure bytes that will
                    // never be sent.
                    #[cfg(feature = "latency-trace")]
                    discard_e2e_samples(&mut pending_e2e, &[connection_id]);
                }
            }
        }

        // Consume output slots from matching stage.
        let count = consumer.consume_batch(&mut batch, MAX_BATCH);
        if count == 0 {
            // SPSC is empty — flush all dirty connections via io_uring.
            // This is the response-data egress path; heartbeat flushes
            // below aren't sampled because they're admin traffic, not
            // on the client RTT path.
            if !dirty_connections.is_empty() {
                #[cfg(feature = "tick-to-trade")]
                let egress_start = trace::mono_trace_ns();
                flush_sends(
                    &mut ring,
                    &mut connections,
                    &dirty_connections,
                    &mut to_remove,
                    &mut cqes,
                );
                #[cfg(feature = "tick-to-trade")]
                egress_rec.record_elapsed(egress_start, trace::mono_trace_ns());
                #[cfg(feature = "latency-trace")]
                close_e2e_samples(&mut pending_e2e, &mut server_e2e_rec);
                for conn_id in to_remove.drain(..) {
                    connections.remove(&conn_id);
                }
                dirty_connections.clear();
            }

            // Send heartbeats to idle connections. Only checked during
            // idle periods (SPSC empty) to avoid overhead on the hot path.
            //
            // No end-to-end samples to close here: the sample queue's
            // invariant is that a queued entry implies a dirty
            // connection, so the flush above — which closes the queue
            // whenever anything was dirty — leaves it empty. Anything
            // dirtied from here on is heartbeat frames, which carry no
            // samples.
            if let Some(interval) = heartbeat_interval {
                let now = Instant::now();
                // Coarse gate: only scan at most once per second.
                if now.duration_since(last_heartbeat_scan) >= Duration::from_secs(1) {
                    last_heartbeat_scan = now;
                    for (&conn_id, entry) in connections.iter_mut() {
                        if now.duration_since(entry.last_send) >= interval {
                            entry.send_buf.extend_from_slice(&heartbeat_wire_frame);
                            dirty_connections.insert(conn_id);
                            entry.last_send = now;
                        }
                    }
                    // Flush the heartbeat sends immediately.
                    if !dirty_connections.is_empty() {
                        flush_sends(
                            &mut ring,
                            &mut connections,
                            &dirty_connections,
                            &mut to_remove,
                            &mut cqes,
                        );
                        for conn_id in to_remove.drain(..) {
                            connections.remove(&conn_id);
                        }
                        dirty_connections.clear();
                    }
                }
            }

            // Re-evaluate the durability policy on a slow timer so the
            // `policy_degraded` flag and the periodic warn track the
            // cluster's real state even on idle / quiet venues. The
            // gate-open block also calls `update_degraded_state` after
            // each consumed batch; this is the equivalent for the
            // no-batch path.
            {
                let now_ts = Instant::now();
                if now_ts.duration_since(last_policy_check) >= POLICY_CHECK_INTERVAL {
                    last_policy_check = now_ts;
                    let journal_pos = journal_persisted_wire_seq.load();
                    let metrics_ref = replication_metrics.as_deref();
                    let active_ref = replica_active.as_ref();
                    let status = evaluate_durability(&policy, journal_pos, metrics_ref, active_ref);
                    degraded_logger.tick(
                        &policy,
                        &utilization,
                        status.degraded,
                        now_ts,
                        DEGRADED_LOG_INTERVAL,
                    );
                    // Cache the position so the next batch's gate sees a
                    // fresh value rather than spinning from a stale cache.
                    cached_durable_pos = status.durable_pos;
                }

                // Hand buffered latency samples to the stats registry
                // while we have nothing better to do. Reuses the policy
                // check's clock read, so the spin path picks up no
                // extra `clock_gettime`.
                #[cfg(feature = "latency-trace")]
                if now_ts.duration_since(last_stats_flush) >= trace::IDLE_FLUSH_INTERVAL {
                    last_stats_flush = now_ts;
                    spsc_rec.flush();
                    dispatch_rec.flush();
                    server_e2e_rec.flush();
                    #[cfg(feature = "tick-to-trade")]
                    {
                        journal_wait_rec.flush();
                        replica_wait_rec.flush();
                        encode_rec.flush();
                        egress_rec.flush();
                    }
                }
            }

            idle_count += 1;
            if idle_count.is_multiple_of(1024) {
                utilization.busy.store(busy_count, Ordering::Relaxed);
                utilization.idle.store(idle_count, Ordering::Relaxed);
            }
            if busy_spin || idle_spins < 1000 {
                idle_spins = idle_spins.wrapping_add(1);
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            continue;
        }
        idle_spins = 0;
        busy_count += 1;

        #[cfg(feature = "latency-trace")]
        let consume_ts = trace::mono_trace_ns();

        // Wait for durability confirmation before sending responses.
        //
        // Gate on `wire_seq`, not `input_seq`. `input_seq` is in
        // local-consumer space (the matching cursor on the input ring,
        // starts at 0 in this process) while replica metrics and the
        // primary's `journal_persisted_wire_seq` live in wire-seq space
        // (allocated by the journal stage starting at
        // `starting_sequence`). A `needed` derived from `input_seq` and
        // compared against wire-seq cursors only works when
        // `starting_sequence == 1`; a recovered primary (or any process
        // whose journal already has prior content pushing
        // `starting_sequence` above 1) would silently open the gate
        // ahead of the replica's actual replicated state.
        //
        // Every cursor in the policy view (`journal_persisted_wire_seq`,
        // `metrics.in_memory_sequence`, `metrics.acked_sequence`)
        // carries "highest wire seq known to be in that state on node
        // X". A slot's `wire_seq` is therefore the *exact* wire seq the
        // gate must see — not `+1` — for that slot's event to be
        // considered durable. The legacy `+1` was load-bearing only
        // because `input_seq` was off by `starting_sequence - 1` from
        // wire seq; with the wire-seq stamp it would over-shoot by one
        // event and make the gate stall an extra round-trip per
        // response.
        //
        // The gate is evaluated **per slot**, not once per batch against
        // the batch's maximum `wire_seq`. Batch-max gating made every
        // response in a batch wait for the newest event in it to become
        // durable, so the oldest slot paid the durability latency of an
        // event sequenced up to `MAX_BATCH` positions after it — a
        // head-of-line block bounded only by the batch size, and one
        // that bites hardest exactly when the stage has caught up to the
        // durability frontier under load. Per-slot gating walks that
        // frontier instead: each response is released as soon as its own
        // event is durable.
        //
        // The extra waits are self-limiting. Slots arrive FIFO with
        // monotonic `wire_seq`, and the journal publishes its cursor per
        // fsync batch, so one wait typically advances the cursor past a
        // whole run of following slots and they fall through without
        // spinning. The number of waits (and so of flushes) tracks real
        // durability boundaries, not the slot count. Correctness does
        // not depend on that ordering — an unsorted batch would simply
        // wait more often.
        let batch_now = Instant::now();

        for slot in &batch[..count] {
            #[cfg(feature = "latency-trace")]
            spsc_rec.record_elapsed(slot.match_complete_ts, consume_ts);

            // Per-slot journal-wait / replica-wait tracker. See
            // `GateCrossTracker` for the rationale (only records cursors
            // that were actually on the critical path). Both the
            // tracker's replica cursor and the attribution counters
            // below are derived from the policy in force rather than a
            // fixed level, so "which subsystem to optimize" answers for
            // the deployment the operator actually configured. A slot
            // that never waits leaves the tracker unobserved, and an
            // unobserved tracker reports no crossing — so the fast path
            // records nothing rather than a zero-length sample.
            #[cfg(feature = "tick-to-trade")]
            let mut gate_tracker = GateCrossTracker::new(slot.wire_seq);

            if slot_needs_gate(slot, cached_durable_pos) {
                let needed = slot.wire_seq;

                // Drain buffered sends before blocking, never after.
                //
                // The steady-state flush happens on the `count == 0`
                // path — once the output ring is empty. That batches
                // many responses behind one `io_uring_enter`, which is
                // the right trade on the fast path. But it means a
                // response that has already cleared its own durability
                // gate stays in `send_buf` while the loop blocks on a
                // later event's gate, so a client waits out an fsync +
                // replica round-trip for an event whose durability was
                // confirmed before that event was even sequenced.
                //
                // Flushing here costs nothing in latency terms: it only
                // fires when we are about to spin on the gate anyway, so
                // the syscall overlaps time that would otherwise be
                // dead. Slots whose gate is already satisfied fall
                // straight through and keep their batching.
                //
                // Traced builds only: this delays the tracker's first
                // `observe` by the flush duration, so a cursor that
                // crosses mid-flush is attributed late (or, if it
                // crosses past `needed`, not attributed at all). Bounded
                // by one flush and confined to the tick-to-trade
                // breakdown — the gate's own behaviour is unchanged.
                if !dirty_connections.is_empty() {
                    #[cfg(feature = "tick-to-trade")]
                    let egress_start = trace::mono_trace_ns();
                    flush_sends(
                        &mut ring,
                        &mut connections,
                        &dirty_connections,
                        &mut to_remove,
                        &mut cqes,
                    );
                    #[cfg(feature = "tick-to-trade")]
                    egress_rec.record_elapsed(egress_start, trace::mono_trace_ns());
                    #[cfg(feature = "latency-trace")]
                    close_e2e_samples(&mut pending_e2e, &mut server_e2e_rec);
                    // This slot and later ones addressed to a dropped
                    // connection are skipped by the
                    // `connections.get_mut` lookup below, so removing
                    // here is safe.
                    for conn_id in to_remove.drain(..) {
                        connections.remove(&conn_id);
                    }
                    dirty_connections.clear();
                }

                loop {
                    // Inside the gate-wait spin loop, also observe a
                    // mode swap. Without this, a batch whose gate
                    // becomes structurally unsatisfiable (e.g. all
                    // replicas die while a non-bypass slot is in
                    // flight under `Hybrid`) would wedge the response
                    // stage forever, even if an operator sends the
                    // remediating `DURABILITY local` — the outer loop
                    // observation never gets a chance to run. The
                    // relaxed load is ~1 cycle on x86; cheaper than
                    // the `spin_loop` hint below.
                    let observed_byte = durability_mode.load(Ordering::Relaxed);
                    if observed_byte != active_mode.as_u8()
                        && let Some(next) = DurabilityMode::from_u8(observed_byte)
                    {
                        tracing::info!(
                            prev = active_mode.as_str(),
                            next = next.as_str(),
                            "durability mode swapped during gate wait"
                        );
                        active_mode = next;
                        policy = active_mode.to_policy();
                        // Flush accrual before re-seeding so the wedged-
                        // degraded interval up to the swap isn't dropped.
                        degraded_logger.reseed(&utilization, Instant::now());
                    }

                    let journal_pos = journal_persisted_wire_seq.load();
                    let metrics_ref = replication_metrics.as_deref();
                    let active_ref = replica_active.as_ref();

                    // The cross-tracker (traced builds only) samples the
                    // replica cursor itself rather than sharing the
                    // evaluation's read below: computing a standalone
                    // replica cursor unconditionally spent four Acquire
                    // loads per spin iteration on `ReplicationMetrics`,
                    // the same cache line the replication sender writes
                    // on every ack and every completed SEND. Gate
                    // attribution does not re-read at all — it comes out
                    // of `evaluate_gate`, from the same snapshot that
                    // opens the gate.
                    #[cfg(feature = "tick-to-trade")]
                    gate_tracker.observe(
                        journal_pos.get(),
                        // The level the *active policy* gates replicas
                        // on — in-memory under `hybrid`, persisted under
                        // `durably-replicated`. `None` when no clause is
                        // replica-supplied (`local`) and, transiently,
                        // when the binding replica drops out of the
                        // cursor view mid-wait. Passed through as-is so
                        // the tracker can tell "no replica wait to
                        // measure" from "the replica caught up".
                        policy_replica_cursor(&policy, journal_pos, metrics_ref, active_ref),
                        trace::mono_trace_ns(),
                    );

                    let (status, blocker) =
                        evaluate_gate(&policy, needed, journal_pos, metrics_ref, active_ref);
                    cached_durable_pos = status.durable_pos;
                    utilization
                        .policy_degraded
                        .store(status.degraded, Ordering::Relaxed);

                    // Accrue degraded time while wedged. The post-gate
                    // tick attributes the whole wait to a single state, so
                    // without this a healthy→degraded flip during the
                    // wedge would be mis-charged. `spinning = true`: this
                    // loop always spins (never yields), so the clock read
                    // behind the tick is mask-gated, landing only every
                    // ~65 k iterations (`CHECK_MASK = 2^16`) regardless of
                    // the period below.
                    if gate_accrual_timer
                        .tick(GATE_ACCRUAL_INTERVAL, true)
                        .is_some()
                    {
                        degraded_logger.tick(
                            &policy,
                            &utilization,
                            status.degraded,
                            Instant::now(),
                            DEGRADED_LOG_INTERVAL,
                        );
                    }

                    if cached_durable_pos >= needed {
                        // Attribution: which subsystem supplied the
                        // binding cursor, from the same snapshot that
                        // opened the gate and against the policy
                        // actually in force. Relaxed is fine — health
                        // reads are infrequent.
                        //
                        // `None` is unreachable here: `needed >= 1`
                        // inside this loop, and a degraded evaluation
                        // pins `durable_pos` to 0, so an open gate
                        // implies the policy was satisfiable and
                        // attribution has a verdict. The no-op arm
                        // keeps a metrics-only path from ever
                        // panicking regardless.
                        match blocker {
                            Some(Blocker::Journal) => {
                                utilization.gate_journal.fetch_add(1, Ordering::Relaxed);
                            }
                            Some(Blocker::Replication) => {
                                utilization.gate_replication.fetch_add(1, Ordering::Relaxed);
                            }
                            None => {}
                        }
                        break;
                    }
                    std::hint::spin_loop();
                }
            }

            // Per-slot durability-gate breakdown. Recorded only when the
            // gate actually held us up (the tracker captured a cross).
            // The cross timestamp is this slot's own `wire_seq`, so the
            // sample measures the wait that slot really paid — under
            // batch-max gating it was the batch maximum's crossing,
            // which overstated every slot but the last by up to the
            // batch's matching span.
            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.journal_crossed() {
                journal_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }
            #[cfg(feature = "tick-to-trade")]
            if let Some(ts) = gate_tracker.replica_crossed() {
                replica_wait_rec.record_elapsed(slot.match_complete_ts, ts);
            }

            // Each slot expands to at most two wire frames: the
            // application payload (Report / Query / EngineError) and
            // an optional trailing `BatchEnd` when
            // `is_last_in_request` is set. Application-shaped
            // payloads go through the encoder; transport-shaped
            // frames (EngineError, BatchEnd) are encoded by the
            // runtime directly.
            if let Some(entry) = connections.get_mut(&slot.connection_id) {
                // Frame 1: application payload (if any). BatchEnd
                // payloads carry no body — the terminator below
                // handles them via `is_last_in_request`.
                let payload_result: Option<Result<usize, &'static str>> = match slot.payload {
                    OutputPayload::Report(ref report) => {
                        Some(encoder.encode_report(report, &mut encode_buf))
                    }
                    OutputPayload::QueryResponse(ref q) => {
                        Some(encoder.encode_query(q, &mut encode_buf))
                    }
                    OutputPayload::EngineError => Some(
                        control_codec::encode_transport_response(
                            &TransportResponse::EngineError,
                            &mut encode_buf,
                        )
                        .map_err(|_| "encode error"),
                    ),
                    OutputPayload::BatchEnd => None,
                };

                let payload_handled = if let Some(result) = payload_result {
                    #[cfg(feature = "tick-to-trade")]
                    let encode_start = trace::mono_trace_ns();
                    let outcome = append_frame(
                        result,
                        slot.connection_id,
                        entry,
                        &encode_buf,
                        batch_now,
                        &mut dirty_connections,
                        &mut to_remove,
                    );
                    #[cfg(feature = "tick-to-trade")]
                    encode_rec.record_elapsed(encode_start, trace::mono_trace_ns());
                    outcome
                } else {
                    AppendOutcome::Continue
                };

                // Frame 2: BatchEnd terminator. Skipped if the payload
                // append dropped the connection.
                if matches!(payload_handled, AppendOutcome::Continue) && slot.is_last_in_request {
                    let result = control_codec::encode_transport_response(
                        &TransportResponse::BatchEnd,
                        &mut encode_buf,
                    )
                    .map_err(|_| "encode error");
                    let outcome = append_frame(
                        result,
                        slot.connection_id,
                        entry,
                        &encode_buf,
                        batch_now,
                        &mut dirty_connections,
                        &mut to_remove,
                    );
                    // Queue the server-side end-to-end sample: reader
                    // recv -> response flush. Only the BatchEnd frame
                    // carries this measurement; queued here after the
                    // append so a dropped connection doesn't skew the
                    // metric, and closed by whichever flush ships the
                    // bytes.
                    #[cfg(feature = "latency-trace")]
                    if matches!(outcome, AppendOutcome::Continue) {
                        pending_e2e.push((slot.connection_id, slot.recv_ts));
                    }
                    let _ = outcome;
                }
            }
        }

        // Remove connections that exceeded the send buffer limit. Like
        // the disconnect handler, this un-dirties a connection without
        // flushing it, so it has to discard that connection's queued
        // samples too — otherwise they outlive the bytes they measure
        // and land on some later, unrelated flush.
        #[cfg(feature = "latency-trace")]
        discard_e2e_samples(&mut pending_e2e, &to_remove);
        for conn_id in to_remove.drain(..) {
            connections.remove(&conn_id);
            dirty_connections.remove(&conn_id);
        }

        // Log degradation transitions / re-emit the reminder. Same
        // logger the idle path uses; transitions are gated on a
        // sustained-state hold so sub-second flap doesn't spam.
        //
        // Ticked after dispatch rather than before it, and off a fresh
        // clock read: with the gate now evaluated per slot, a batch can
        // span several waits, and `batch_now` was taken before the first
        // of them. The accrual inside each wait already charges degraded
        // time as it elapses; this tick decides transitions, so it wants
        // the state and the timestamp as of the end of the batch.
        let ticked_at = Instant::now();
        let degraded_now = utilization.policy_degraded.load(Ordering::Relaxed);
        degraded_logger.tick(
            &policy,
            &utilization,
            degraded_now,
            ticked_at,
            DEGRADED_LOG_INTERVAL,
        );
        // Bump the idle-path's check timestamp so we don't double-
        // tick the logger when traffic stops.
        last_policy_check = ticked_at;

        #[cfg(feature = "latency-trace")]
        dispatch_rec.record_elapsed(consume_ts, trace::mono_trace_ns());
    }
}

/// Close every queued server-side end-to-end sample against the flush
/// that just completed. Call immediately after `flush_sends`.
///
/// Closes the whole queue unconditionally, including entries for a
/// connection the flush then dropped on a send error. That is
/// deliberate: the stage measures "reader recv → response flush", which
/// ends when the bytes are handed to the kernel, not when the peer
/// acknowledges them. A failed SEND still produces a sample whose
/// duration is the real one for everything this stage controls.
/// Contrast [`discard_e2e_samples`], where no flush happens at all.
///
/// One clock read serves the whole queue: the samples all end at the
/// same flush, so reading per entry would only measure the drain loop.
#[cfg(feature = "latency-trace")]
fn close_e2e_samples(
    pending: &mut Vec<(u64, trace::MonoTraceInstant)>,
    rec: &mut trace::StageRecorder,
) {
    let flushed_at = trace::mono_trace_ns();
    for (_, recv_ts) in pending.drain(..) {
        rec.record_elapsed(recv_ts, flushed_at);
    }
}

/// Drop queued samples belonging to connections going away *without* a
/// flush — the disconnect handler and the send-buffer-overflow drain.
///
/// Their buffered bytes are discarded along with the connection, so
/// there is no flush to close the samples against and nothing to
/// measure. Left queued, they would instead be timed against some later,
/// unrelated connection's flush: a millisecond-scale phantom in a
/// microsecond histogram, landing squarely in the tail this stage exists
/// to report.
///
/// Not to be used after `flush_sends`. `dropped` there can hold a
/// connection queued by an earlier append overflow whose buffered bytes
/// the flush nonetheless shipped — discarding would lose a legitimate
/// sample.
///
/// Linear scan per entry, but `dropped` is empty on every iteration that
/// loses no connection, which is essentially all of them, and the early
/// return keeps that case free.
#[cfg(feature = "latency-trace")]
fn discard_e2e_samples(pending: &mut Vec<(u64, trace::MonoTraceInstant)>, dropped: &[u64]) {
    if dropped.is_empty() {
        return;
    }
    pending.retain(|(conn_id, _)| !dropped.contains(conn_id));
}

/// Whether this slot must wait on the durability gate before its
/// response is encoded.
///
/// Two reasons to skip the wait. First, the durability-gate carve-out
/// for halt-state output: slots tagged `durability_bypass = true` at
/// emission carry no engine state worth replicating before delivery —
/// see [`OutputSlot::durability_bypass`] for the correctness argument —
/// so clients receive the halt reason immediately rather than blocking
/// on a structurally unsatisfiable policy (e.g. `Hybrid` with all
/// replicas disconnected, which would otherwise stall the gate until
/// peers return). Second, the slot's own event is already durable.
///
/// Applied per slot rather than per batch, so a bypass slot is released
/// as soon as the dispatch loop reaches it instead of riding behind a
/// gated slot earlier in the same batch. Ordering is unaffected: the
/// loop walks slots in ring order either way, so a bypass slot still
/// leaves after everything sequenced before it.
///
/// Shared by both response stages. The matching stage that sets the flag
/// is transport-agnostic, so the same slots reach the io_uring and DPDK
/// paths and the two must agree; keeping the predicate in one place is
/// what stops them drifting apart again.
#[inline]
pub(crate) fn slot_needs_gate<R: Copy, Q: Copy>(
    slot: &OutputSlot<R, Q>,
    cached_durable_pos: u64,
) -> bool {
    !slot.durability_bypass && cached_durable_pos < slot.wire_seq
}

/// Submit io_uring SEND SQEs for all dirty connections and wait for completions.
///
/// Outcome of a single per-frame append. `Continue` means the
/// caller may proceed to the next frame for this slot;
/// `ConnectionDropped` means the connection's send buffer overflowed
/// or the encode failed, and the connection has been queued for
/// removal — no further frames should be appended for this slot.
#[derive(Clone, Copy)]
enum AppendOutcome {
    Continue,
    ConnectionDropped,
}

/// Copy an encoded frame into the connection's send buffer with
/// overflow checking. Splits the responsibilities the inline encode
/// loop used to have: the caller passes in the encode result (so it
/// can come from the `ResponseEncoder` trait for application
/// payloads, or `encode_transport_response` for transport-shaped
/// frames), and this helper handles size accounting + dirty
/// tracking uniformly.
#[allow(clippy::too_many_arguments)]
fn append_frame(
    result: Result<usize, &'static str>,
    connection_id: u64,
    entry: &mut ConnectionEntry,
    encode_buf: &[u8],
    batch_now: Instant,
    dirty_connections: &mut HashSet<u64>,
    to_remove: &mut Vec<u64>,
) -> AppendOutcome {
    let written = match result {
        Ok(n) => n,
        Err(reason) => {
            tracing::error!(connection_id, reason, "encode error");
            return AppendOutcome::Continue;
        }
    };

    // Drop slow clients whose send buffer has grown too large. This
    // prevents unbounded memory growth from a single laggy connection
    // causing allocator pressure and tail latency spikes.
    if entry.send_buf.len() + written > MAX_SEND_BUF {
        debug!(
            connection_id,
            send_buf_len = entry.send_buf.len(),
            "send buffer exceeded limit, dropping connection"
        );
        to_remove.push(connection_id);
        return AppendOutcome::ConnectionDropped;
    }

    // Append the full wire frame to the connection's send buffer.
    // The encoder writes [length(4) | payload], which is the complete
    // wire format — no extra framing needed.
    entry.send_buf.extend_from_slice(&encode_buf[..written]);
    entry.last_send = batch_now;
    dirty_connections.insert(connection_id);
    AppendOutcome::Continue
}

/// Each dirty connection's accumulated send buffer is sent in a single SEND
/// operation. Partial sends are retried until all bytes are delivered.
/// Failed connections are collected in `to_remove` for the caller to clean up.
fn flush_sends(
    ring: &mut IoUring,
    connections: &mut HashMap<u64, ConnectionEntry>,
    dirty: &HashSet<u64>,
    to_remove: &mut Vec<u64>,
    cqes: &mut Vec<(u64, i32)>,
) {
    // Submit SEND SQEs for all dirty connections.
    let mut pending: usize = 0;
    for &conn_id in dirty {
        if let Some(entry) = connections.get(&conn_id) {
            if entry.send_buf.is_empty() {
                continue;
            }
            let sqe = opcode::Send::new(
                types::Fd(entry.fd),
                entry.send_buf.as_ptr(),
                entry.send_buf.len() as u32,
            )
            .build()
            .user_data(conn_id);

            unsafe {
                ring.submission()
                    .push(&sqe)
                    .expect("io_uring SQ full — increase RING_SIZE");
            }
            pending += 1;
        }
    }

    if pending == 0 {
        return;
    }

    // Submit and wait for all completions.
    if let Err(e) = ring.submit_and_wait(pending) {
        error!(error = %e, "io_uring submit_and_wait failed in response stage");
        return;
    }

    // Drain completions into pre-allocated buffer. Must collect to
    // release CQ borrow before mutating connections.
    cqes.clear();
    cqes.extend(ring.completion().map(|cqe| (cqe.user_data(), cqe.result())));

    for &(conn_id, result) in cqes.iter() {
        if result < 0 {
            debug!(
                connection_id = conn_id,
                error = result,
                "send error, dropping connection"
            );
            to_remove.push(conn_id);
            continue;
        }

        let sent = result as usize;
        if let Some(entry) = connections.get_mut(&conn_id) {
            if sent >= entry.send_buf.len() {
                entry.send_buf.clear();
            } else {
                // Partial send — drain sent bytes, retry remainder.
                // Rare for small response frames over TCP/UDS but must
                // be handled for correctness (e.g., send buffer pressure).
                entry.send_buf.drain(..sent);
                retry_send(ring, entry, conn_id, to_remove);
            }
        }
    }
}

/// Retry sending remaining bytes after a partial send. Loops until the
/// entire buffer is delivered or an error occurs.
fn retry_send(
    ring: &mut IoUring,
    entry: &mut ConnectionEntry,
    conn_id: u64,
    to_remove: &mut Vec<u64>,
) {
    while !entry.send_buf.is_empty() {
        let sqe = opcode::Send::new(
            types::Fd(entry.fd),
            entry.send_buf.as_ptr(),
            entry.send_buf.len() as u32,
        )
        .build()
        .user_data(conn_id);

        unsafe {
            ring.submission()
                .push(&sqe)
                .expect("io_uring SQ full during send retry");
        }

        if let Err(e) = ring.submit_and_wait(1) {
            debug!(connection_id = conn_id, error = %e, "send retry failed");
            to_remove.push(conn_id);
            return;
        }

        if let Some(cqe) = ring.completion().next() {
            let result = cqe.result();
            if result <= 0 {
                debug!(
                    connection_id = conn_id,
                    error = result,
                    "send retry error, dropping connection"
                );
                to_remove.push(conn_id);
                return;
            }
            let sent = result as usize;
            if sent >= entry.send_buf.len() {
                entry.send_buf.clear();
            } else {
                entry.send_buf.drain(..sent);
            }
        }
    }
}

/// Read the live gate cursors and lend them to `f` as a [`CursorView`].
///
/// The view contains the primary followed by every *currently connected*
/// replica slot. Disconnected slots are omitted rather than entered with
/// zero cursors, so `CursorView::len` reflects how many nodes are
/// actually available to satisfy a clause — too few and the policy
/// reports degraded and the gate stalls. Node 0 is always the primary;
/// [`Policy::attribute_blocker`] relies on that index convention to tell
/// journal from replication.
///
/// The primary's in-memory cursor is modeled as `u64::MAX` because the
/// response stage only gates events the matching engine has already
/// processed — those are trivially in-memory on the primary.
///
/// Scoped-borrow shape rather than a returned view: `CursorView` holds a
/// slice, so it cannot outlive the array behind it. Lending it to a
/// closure keeps that array on this frame and off every caller's
/// signature.
#[inline]
fn with_cursor_view<R>(
    journal_pos: WireSeq,
    metrics: Option<&ReplicationMetrics>,
    replica_active: Option<&[Arc<AtomicBool>; 2]>,
    f: impl FnOnce(&CursorView<'_>) -> R,
) -> R {
    // Fixed-size array rather than a `Vec`: the cluster is capped at
    // MAX_CLUSTER_SIZE and this runs on the gate path, so a heap
    // allocation would be pure overhead. Raw `u64` in wire-seq space —
    // `journal_pos` leaves the type system here, alongside the replica
    // metrics gauges (the `Ack` frame's wire-seq fields verbatim).
    let mut nodes = [[0u64; 2]; MAX_CLUSTER_SIZE as usize];
    nodes[0] = [u64::MAX, journal_pos.get()];
    let mut len = 1;
    if let (Some(m), Some(active)) = (metrics, replica_active) {
        for (i, slot_active) in active.iter().enumerate() {
            // Skip inactive slots up-front.
            if !slot_active.load(Ordering::Acquire) {
                continue;
            }
            let in_mem = m.in_memory_sequence[i].load(Ordering::Acquire);
            let persisted = m.acked_sequence[i].load(Ordering::Acquire);
            nodes[len] = [in_mem, persisted];
            len += 1;
        }
    }
    f(&CursorView::new(&nodes[..len]))
}

/// Highest sequence at which the policy is satisfied by the live
/// cursors, plus whether the cluster shape can satisfy it at all.
#[inline]
pub(crate) fn evaluate_durability(
    policy: &Policy,
    journal_pos: WireSeq,
    metrics: Option<&ReplicationMetrics>,
    replica_active: Option<&[Arc<AtomicBool>; 2]>,
) -> EvalStatus {
    with_cursor_view(journal_pos, metrics, replica_active, |view| {
        policy.evaluate_with_status(view)
    })
}

/// One-snapshot gate evaluation: the policy's durable position plus,
/// when that position opens the gate (`>= needed`), which subsystem
/// supplied the binding cursor.
///
/// Both answers come from the *same* [`CursorView`]. Re-reading the
/// replica cursors for attribution after the evaluation that opened the
/// gate would rank a later snapshot — the replica side can advance
/// between the two reads and flip the verdict to a subsystem that was
/// not the one the gate actually opened on.
///
/// The blocker is `None` while the gate stays closed (attribution is
/// only read at the open, and ranking the clauses for it on every spin
/// iteration would be wasted work). At an open with `needed >= 1` it is
/// always `Some`: a policy left unsatisfiable by the cluster shape pins
/// `durable_pos` to 0, which cannot reach `needed`.
///
/// The attribution itself replaces the old "compare the journal cursor
/// against the minimum replica *persisted* cursor" heuristic, which
/// reported replication as the blocker under `local` — where replicas
/// cannot bind the gate at all — and read the persisted level under
/// `hybrid`, which gates on in-memory.
#[inline]
pub(crate) fn evaluate_gate(
    policy: &Policy,
    needed: u64,
    journal_pos: WireSeq,
    metrics: Option<&ReplicationMetrics>,
    replica_active: Option<&[Arc<AtomicBool>; 2]>,
) -> (EvalStatus, Option<Blocker>) {
    with_cursor_view(journal_pos, metrics, replica_active, |view| {
        let status = policy.evaluate_with_status(view);
        let blocker = if status.durable_pos >= needed {
            policy.attribute_blocker(view)
        } else {
            None
        };
        (status, blocker)
    })
}

/// The replica-side cursor the active policy is waiting on, for the
/// `tick-to-trade` replica-wait histogram. `None` when no clause is
/// replica-supplied (e.g. `local`).
#[cfg(feature = "tick-to-trade")]
#[inline]
pub(crate) fn policy_replica_cursor(
    policy: &Policy,
    journal_pos: WireSeq,
    metrics: Option<&ReplicationMetrics>,
    replica_active: Option<&[Arc<AtomicBool>; 2]>,
) -> Option<u64> {
    with_cursor_view(journal_pos, metrics, replica_active, |view| {
        policy.replica_gate_cursor(view)
    })
}

/// Hold-time before a state transition is committed to the log.
/// Suppresses log spam when a replica flaps faster than this — only
/// transitions that hold for at least this long emit warn/info
/// entries. The `/healthz` gauge updates immediately regardless,
/// so dashboards and alerts still see real-time state.
const DEGRADED_FLAP_HOLD: Duration = Duration::from_secs(1);

/// Tracks degradation state across calls and emits warn/info logs
/// with sustained-state gating + a periodic heartbeat re-emit.
///
/// The hot path calls [`Self::tick`] every gate iteration / idle
/// poll with the current `degraded` value and the wall clock. The
/// logger handles:
///
/// - Updating the `policy_degraded` health gauge immediately.
/// - Suppressing log lines for transitions that don't hold for at
///   least [`DEGRADED_FLAP_HOLD`] — a replica flapping at sub-second
///   cadence produces no log noise, only a quietly-updating gauge.
/// - Emitting a warn at the moment a sustained degraded state
///   crosses the hold threshold, plus a periodic re-emit every
///   `heartbeat_interval` while it persists.
/// - Emitting an info when a sustained healthy state crosses the
///   hold threshold (the cluster is back to its target shape and
///   stayed there long enough that we trust the recovery).
pub(crate) struct DegradationLogger {
    /// Last value passed to `tick`; what we'd log about if it stayed
    /// at this value past the hold threshold.
    pending_state: bool,
    /// When `pending_state` first appeared. Reset on every flip.
    pending_since: Instant,
    /// Whether the current pending state has been logged yet. Only
    /// the *first* log per sustained streak crosses; subsequent
    /// re-emits while degraded are heartbeat warns.
    pending_logged: bool,
    /// When the last warn fired. Drives the periodic re-emit while
    /// degraded.
    last_log: Option<Instant>,
    /// Wall clock of the previous accrual. The interval since this
    /// instant is attributed to the state observed then (`pending_state`)
    /// and accumulated into the `policy_degraded_nanos` counter. Advanced
    /// by [`Self::accrue`] — reached on every `tick` (post-gate, idle, and
    /// the gate-wait pacing tick) and on `reseed`.
    last_tick: Instant,
}

impl DegradationLogger {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            pending_state: false,
            pending_since: now,
            pending_logged: true, // healthy is the assumed initial state; nothing to log
            last_log: None,
            last_tick: now,
        }
    }

    /// Use when the policy is known to start in a degraded state
    /// (e.g. a primary in `hybrid` mode with no replica yet
    /// connected). Logs a startup warn immediately and treats the
    /// state as already-logged so the next tick doesn't re-emit.
    pub(crate) fn new_starting_degraded(now: Instant, policy: &Policy) -> Self {
        tracing::warn!(
            policy = %policy,
            "durability policy starts in degraded mode — fewer connected nodes than the target count"
        );
        Self {
            pending_state: true,
            pending_since: now,
            pending_logged: true,
            last_log: Some(now),
            last_tick: now,
        }
    }

    /// Charge the interval `[last_tick, now]` to the degraded-duration
    /// counter when `prev_degraded` (the state observed at the previous
    /// tick) was degraded, then advance `last_tick`. Drives the
    /// `_seconds_total` counter so `rate()` reflects time-in-degraded
    /// continuously, even mid-incident, rather than only stepping on
    /// recovery. Shared by [`Self::tick`] and [`Self::reseed`] so accrual
    /// is never skipped on a logger lifecycle boundary.
    #[inline]
    fn accrue(&mut self, utilization: &StageUtilization, prev_degraded: bool, now: Instant) {
        if prev_degraded {
            // `duration_since` saturates to zero when `now < last_tick`;
            // `now` is always >= `last_tick` here (single-thread monotonic
            // clock), matching the plain `duration_since` used elsewhere
            // in this file.
            let elapsed = now.duration_since(self.last_tick);
            // `as_nanos()` is u128; one inter-tick interval is sub-second
            // to a few seconds, far below the u64 nanos ceiling (~584
            // years), so the cast can't truncate. See the field doc on
            // `StageUtilization::policy_degraded_nanos`.
            utilization
                .policy_degraded_nanos
                .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        }
        self.last_tick = now;
    }

    /// Flush in-flight degraded accrual up to `now`, then reset to a
    /// fresh healthy-start logger. Called on a runtime durability-mode
    /// swap, which is a logger lifecycle boundary: without flushing
    /// first, the pre-swap degraded interval — on the gate-wait path the
    /// entire wedge — would be silently dropped. The new policy's actual
    /// degraded state is re-derived by the next `tick`, so the brief
    /// swap-to-next-tick window is attributed healthy; that residual is
    /// bounded by one tick interval, the counter's inherent granularity.
    pub(crate) fn reseed(&mut self, utilization: &StageUtilization, now: Instant) {
        let prev_state = self.pending_state;
        self.accrue(utilization, prev_state, now);
        *self = Self::new(now);
    }

    /// Update the gauge + emit transition/heartbeat logs as needed.
    /// Cheap on the hot path: one atomic store, a few branches, one
    /// `Instant::duration_since`.
    pub(crate) fn tick(
        &mut self,
        policy: &Policy,
        utilization: &StageUtilization,
        degraded_now: bool,
        now: Instant,
        heartbeat_interval: Duration,
    ) {
        // Charge the just-elapsed interval to the state observed at the
        // *previous* tick. Snapshot it up front, before the flap-hold
        // bookkeeping below mutates `pending_state`, so accrual stays
        // correct even if that block is later reordered.
        let prev_state = self.pending_state;
        self.accrue(utilization, prev_state, now);

        utilization
            .policy_degraded
            .store(degraded_now, Ordering::Relaxed);

        if degraded_now != self.pending_state {
            // State changed — start a new hold window. Don't log
            // until / unless this new state stays long enough.
            self.pending_state = degraded_now;
            self.pending_since = now;
            self.pending_logged = false;
            return;
        }

        // State held. If we haven't yet logged this streak's onset,
        // and it's been pending for at least the flap-hold time,
        // emit the transition message and mark logged.
        if !self.pending_logged && now.duration_since(self.pending_since) >= DEGRADED_FLAP_HOLD {
            if degraded_now {
                tracing::warn!(
                    policy = %policy,
                    "durability policy operating in degraded mode — fewer connected nodes than the target count, response gate stalled until the cluster recovers or the mode is swapped"
                );
            } else {
                tracing::info!(
                    policy = %policy,
                    "durability policy returned to target shape"
                );
            }
            self.pending_logged = true;
            self.last_log = Some(now);
            return;
        }

        // Heartbeat re-emit while a degraded state persists.
        if degraded_now
            && self.pending_logged
            && self
                .last_log
                .is_none_or(|t| now.duration_since(t) >= heartbeat_interval)
        {
            tracing::warn!(
                policy = %policy,
                "durability policy still degraded — fewer connected nodes than the target count"
            );
            self.last_log = Some(now);
        }
    }
}

/// Tracks per-cursor "first observed transition from below to >= needed"
/// inside the durability gate loop, to drive the journal-wait /
/// replica-wait histograms in the bench's tick-to-trade decomposition.
///
/// A sample is recorded only for cursors that were strictly below
/// `needed` at the loop's first observation. Cursors already past at
/// entry were not on the critical path for this batch, so attributing
/// "wait time" to them would inflate the metric with cursor-poll
/// observation timestamps that have nothing to do with how long the
/// stage actually held us up.
///
/// `now_ns` is taken as a parameter rather than read internally so
/// tests can supply deterministic timestamps. The caller's hot path
/// reads `trace::mono_trace_ns()` once per gate iteration and feeds it in.
#[cfg(feature = "tick-to-trade")]
pub(crate) struct GateCrossTracker {
    needed: u64,
    journal_crossed_ts: Option<trace::MonoTraceInstant>,
    replica_crossed_ts: Option<trace::MonoTraceInstant>,
    journal_was_below: bool,
    replica_was_below: bool,
    first: bool,
}

#[cfg(feature = "tick-to-trade")]
impl GateCrossTracker {
    pub(crate) fn new(needed: u64) -> Self {
        Self {
            needed,
            journal_crossed_ts: None,
            replica_crossed_ts: None,
            journal_was_below: false,
            replica_was_below: false,
            first: true,
        }
    }

    /// `replica_pos` is the replica-side cursor the *active policy*
    /// gates on — see [`policy_replica_cursor`]. `None` means no replica
    /// currently supplies a binding cursor, which happens permanently
    /// under `local` and transiently when the binding replica drops out
    /// of the cursor view part-way through a wait.
    ///
    /// Both cases must leave `replica_crossed_ts` alone. Treating
    /// `None` as an infinite cursor would satisfy the crossing test and
    /// latch the timestamp at the *disconnect*, reporting a replica
    /// wait that ended when the link died rather than when the replica
    /// caught up — understating exactly the failover runs the histogram
    /// exists to measure. On the first observe it additionally leaves
    /// `replica_was_below` false, so a policy that never waits on a
    /// replica records no sample at all.
    pub(crate) fn observe(
        &mut self,
        journal_pos: u64,
        replica_pos: Option<u64>,
        now_ns: trace::MonoTraceInstant,
    ) {
        if self.first {
            self.journal_was_below = journal_pos < self.needed;
            self.replica_was_below = replica_pos.is_some_and(|p| p < self.needed);
            self.first = false;
        }
        if self.journal_was_below && self.journal_crossed_ts.is_none() && journal_pos >= self.needed
        {
            self.journal_crossed_ts = Some(now_ns);
        }
        if self.replica_was_below
            && self.replica_crossed_ts.is_none()
            && replica_pos.is_some_and(|p| p >= self.needed)
        {
            self.replica_crossed_ts = Some(now_ns);
        }
    }

    pub(crate) fn journal_crossed(&self) -> Option<trace::MonoTraceInstant> {
        self.journal_crossed_ts
    }

    pub(crate) fn replica_crossed(&self) -> Option<trace::MonoTraceInstant> {
        self.replica_crossed_ts
    }
}

/// Print busy/idle utilization for a pipeline stage on shutdown.
#[cfg(feature = "pipeline-stats")]
fn print_utilization(stage: &str, busy: u64, idle: u64) {
    let total = busy + idle;
    if total == 0 {
        tracing::info!(stage, "no iterations recorded");
        return;
    }
    let pct = (busy as f64 / total as f64) * 100.0;
    tracing::info!(
        stage,
        pct_busy = format_args!("{pct:.2}%"),
        busy,
        idle,
        total,
        "pipeline utilization",
    );
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "tick-to-trade")]
    use super::GateCrossTracker;
    use super::{
        Blocker, DegradationLogger, OutputSlot, WireSeq, evaluate_durability, evaluate_gate,
        slot_needs_gate,
    };
    use crate::durability_policy::{Clause, Level, Policy};
    use crate::replication::ReplicationMetrics;

    /// Queued end-to-end samples survive a flush that dropped nothing,
    /// and are closed (drained) by it.
    #[cfg(feature = "latency-trace")]
    #[test]
    fn e2e_samples_survive_until_a_flush_closes_them() {
        let mut pending = vec![(1u64, 10u64), (2, 20), (1, 30)];
        super::discard_e2e_samples(&mut pending, &[]);
        assert_eq!(pending.len(), 3, "an empty drop list must lose nothing");

        let mut rec = melin_transport_core::trace::register_stage("test::response_e2e_close");
        super::close_e2e_samples(&mut pending, &mut rec);
        assert!(pending.is_empty(), "a flush closes the whole queue");
    }

    /// The defect this pairing exists to prevent: a dropped connection's
    /// buffered bytes are discarded with it, so its samples must not
    /// survive to be timed against some later connection's flush.
    #[cfg(feature = "latency-trace")]
    #[test]
    fn e2e_samples_for_a_dropped_connection_are_discarded() {
        let mut pending = vec![(1u64, 10u64), (2, 20), (1, 30), (3, 40)];
        super::discard_e2e_samples(&mut pending, &[1, 3]);
        assert_eq!(
            pending,
            vec![(2, 20)],
            "only the surviving connection's samples remain"
        );

        // Dropping the last live connection empties the queue, which is
        // what restores "queue non-empty implies something is dirty" —
        // the invariant the flush sites rely on to be sufficient drain
        // points.
        super::discard_e2e_samples(&mut pending, &[2]);
        assert!(pending.is_empty());
    }

    /// Output slot carrying only the fields the gate decision reads.
    /// `()` for the report/query types keeps the fixture independent of
    /// any concrete application.
    fn slot(wire_seq: u64, durability_bypass: bool) -> OutputSlot<(), ()> {
        OutputSlot {
            wire_seq,
            durability_bypass,
            ..Default::default()
        }
    }

    #[test]
    fn bypass_slot_never_gates() {
        // Halt-state output must not block on a policy that may be
        // structurally unsatisfiable, however far behind the cursor is.
        assert!(!slot_needs_gate(&slot(10, true), 0));
        assert!(!slot_needs_gate(&slot(10, true), 9));
    }

    #[test]
    fn normal_slot_gates_until_its_own_sequence_is_durable() {
        assert!(slot_needs_gate(&slot(10, false), 0));
        assert!(slot_needs_gate(&slot(10, false), 9));
    }

    #[test]
    fn normal_slot_clears_at_its_own_sequence_not_the_batch_maximum() {
        // The cursor reaching exactly this slot's `wire_seq` is enough —
        // an off-by-one here would stall every response an extra
        // durability round-trip. A later slot in the same batch needing
        // more must not hold this one back.
        assert!(!slot_needs_gate(&slot(10, false), 10));
        assert!(!slot_needs_gate(&slot(10, false), 11));
    }

    /// Build a [`Policy`] from a mini DSL: one or more
    /// `"<level>>=<count>"` clauses joined with `&&`. Test-only
    /// ergonomics — production builds policies via
    /// [`DurabilityMode::to_policy`].
    fn parse(s: &str) -> Result<Policy, String> {
        let mut clauses = Vec::new();
        for raw in s.split("&&") {
            let token = raw.trim();
            let (lvl, rhs) = token
                .split_once(">=")
                .ok_or_else(|| format!("clause `{token}` missing `>=`"))?;
            let level = match lvl.trim() {
                "persisted" => Level::Persisted,
                "in_memory" => Level::InMemory,
                other => return Err(format!("unknown level `{other}`")),
            };
            let count: u8 = rhs.trim().parse().map_err(|e| format!("bad count: {e}"))?;
            clauses.push(Clause { count, level });
        }
        Policy::new(clauses).map_err(|e| e.to_string())
    }
    use melin_transport_core::pipeline::StageUtilization;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    /// Build a `ReplicationMetrics` with both slots populated. Tests
    /// that need to simulate a disconnected slot use [`flags`] to mark
    /// it inactive — its cursors are then ignored regardless of value.
    fn metrics(slot0: (u64, u64), slot1: (u64, u64)) -> Arc<ReplicationMetrics> {
        let m = Arc::new(ReplicationMetrics::default());
        m.in_memory_sequence[0].store(slot0.0, Ordering::Relaxed);
        m.acked_sequence[0].store(slot0.1, Ordering::Relaxed);
        m.in_memory_sequence[1].store(slot1.0, Ordering::Relaxed);
        m.acked_sequence[1].store(slot1.1, Ordering::Relaxed);
        m
    }

    /// Build a `[active; 2]` flags array.
    fn flags(slot0_active: bool, slot1_active: bool) -> [Arc<AtomicBool>; 2] {
        [
            Arc::new(AtomicBool::new(slot0_active)),
            Arc::new(AtomicBool::new(slot1_active)),
        ]
    }

    /// Both replicas active — the common healthy-cluster case.
    fn both_active() -> [Arc<AtomicBool>; 2] {
        flags(true, true)
    }

    // --- Standalone (no replicas wired) ---

    #[test]
    fn standalone_persisted_one_gates_on_journal() {
        // No metrics → only the primary is in the view. `persisted>=1`
        // is satisfied by the primary alone at journal_pos.
        let p = parse("persisted>=1").unwrap();
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(500), None, None).durable_pos,
            500
        );
    }

    #[test]
    fn standalone_strict_persisted_two_never_opens() {
        // `persisted>=2` on a standalone primary stays at 0: the
        // operator asked for two copies and there is only one. The
        // policy surfaces as degraded so the operator sees the gate
        // is stalled because the cluster can't meet the policy.
        let p = parse("persisted>=2").unwrap();
        let r = evaluate_durability(&p, WireSeq::new(500), None, None);
        assert_eq!(r.durable_pos, 0);
        assert!(
            r.degraded,
            "policy structurally unsatisfiable on this shape → degraded",
        );
    }

    // --- 2 replicas connected ---

    #[test]
    fn quorum_both_replicas_ahead_of_journal() {
        // Both replicas persisted past journal. `persisted>=2` returns
        // the 2nd-largest persisted across {primary, slot0, slot1}.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((100, 100), (120, 120));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(50), Some(&m), Some(&a)).durable_pos,
            100
        );
    }

    #[test]
    fn quorum_journal_ahead_of_both_replicas() {
        // Journal at 500, replicas at 100/120. 2nd-largest persisted = 120.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((100, 100), (120, 120));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(500), Some(&m), Some(&a)).durable_pos,
            120
        );
    }

    #[test]
    fn quorum_journal_between_slow_and_fast_replica() {
        // {primary=150, slot0_persisted=50, slot1_persisted=200}.
        // 2nd-largest = 150 (primary itself).
        let p = parse("persisted>=2").unwrap();
        let m = metrics((50, 50), (200, 200));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(150), Some(&m), Some(&a)).durable_pos,
            150
        );
    }

    // --- Single replica connected ---

    #[test]
    fn single_replica_strict_persisted_two_requires_both_survivors() {
        // Slot 0 connected, slot 1 disconnected. View = {primary, slot0}.
        // Strict `persisted>=2`: 2nd-largest of the 2-row view =
        // min(primary, slot0). Strictly stronger than legacy auto-
        // degrade-to-1-node in the same shape.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((100, 100), (999, 999)); // slot 1 cursors ignored
        let a = flags(true, false);
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(50), Some(&m), Some(&a)).durable_pos,
            50
        );
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(200), Some(&m), Some(&a)).durable_pos,
            100
        );
    }

    #[test]
    fn single_replica_persisted_two_requires_both_survivors() {
        // 2-node view (primary + surviving replica). `persisted>=2` is
        // satisfiable; the gate opens at the slower of the two and the
        // policy is not degraded.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((100, 100), (999, 999));
        let a = flags(true, false);
        let r = evaluate_durability(&p, WireSeq::new(50), Some(&m), Some(&a));
        assert_eq!(r.durable_pos, 50);
        assert!(!r.degraded);
    }

    #[test]
    fn both_replicas_disconnected_strict_stalls() {
        // View has only the primary. Strict `persisted>=2` cannot be
        // satisfied — operator opted out of degrade.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((999, 999), (999, 999));
        let a = flags(false, false);
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(500), Some(&m), Some(&a)).durable_pos,
            0
        );
    }

    #[test]
    fn both_replicas_disconnected_strict_stalls_and_flags_degraded() {
        // With `persisted>=2` and both replicas down, the cursor view
        // collapses to {primary}: the clause's count (=2) exceeds the
        // view size, so the gate stays at 0 and the policy flags
        // degraded. Note the matching stage's separate halt at
        // `replicas_connected==0` rejects new orders before they reach
        // the gate; this verifies the gate semantics in isolation.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((999, 999), (999, 999));
        let a = flags(false, false);
        let r = evaluate_durability(&p, WireSeq::new(500), Some(&m), Some(&a));
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded);
    }

    // --- Mixed-level policies ---

    #[test]
    fn persisted_one_and_in_memory_two() {
        // "Leader persists, plus one other node has it in memory" —
        // the cheap-but-non-zero durability target. Slot 0 has it in
        // memory, slot 1 disconnected.
        let p = parse("persisted>=1 && in_memory>=2").unwrap();
        // primary persisted=50, slot0 in_mem=80 / persisted=20.
        // persisted>=1: max(50, 20, 0) = 50.
        // in_memory>=2: primary in_mem=u64::MAX (always), slot0_eff=max(80, 20)=80,
        //               slot1=0. 2nd-largest = 80.
        // min(50, 80) = 50.
        let m = metrics((80, 20), (999, 999));
        let a = flags(true, false);
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(50), Some(&m), Some(&a)).durable_pos,
            50
        );
    }

    // --- Edge: journal at 0 ---

    #[test]
    fn journal_at_zero_with_replicas_persisted_one() {
        // Journal hasn't fsynced anything; both replicas have. With
        // `persisted>=1` the gate opens at the fastest replica.
        let p = parse("persisted>=1").unwrap();
        let m = metrics((100, 100), (200, 200));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(0), Some(&m), Some(&a)).durable_pos,
            200
        );
    }

    // --- gate-blocker attribution ---
    //
    // The attribution must follow the policy actually in force. The
    // previous heuristic compared the journal cursor against the
    // minimum replica *persisted* cursor unconditionally, which
    // reported replication as the blocker under `local` (where
    // replicas cannot bind the gate) and read the wrong level under
    // `hybrid` (which gates replicas on in-memory).
    //
    // Attribution comes out of `evaluate_gate` alongside the durable
    // position, computed from the same cursor snapshot, and is `None`
    // while the gate stays closed — so each test passes a `needed` at
    // or below the scenario's durable position to model the iteration
    // on which the gate opens.

    #[test]
    fn local_mode_never_blames_replication() {
        // `persisted>=1` is satisfied by the highest persisted cursor
        // in the cluster — the primary's. A connected replica lagging
        // far behind is irrelevant to the gate, so it must not be
        // credited as the blocker. This is the case the old heuristic
        // got flatly wrong: journal_pos (500) > repl_min (10) sent it
        // down the `else` branch every single time.
        let p = parse("persisted>=1").unwrap();
        let m = metrics((10, 10), (10, 10));
        let a = both_active();
        assert_eq!(
            evaluate_gate(&p, 500, WireSeq::new(500), Some(&m), Some(&a)).1,
            Some(Blocker::Journal)
        );
    }

    #[test]
    fn hybrid_reads_replica_in_memory_not_persisted() {
        // `persisted>=1 && in_memory>=2`. The replica has the event in
        // memory (400) well ahead of its own fsync (100), and the
        // primary's journal is behind that in-memory cursor (300).
        // The binding clause is therefore the primary's persisted
        // cursor → journal. Comparing against the replica's *persisted*
        // cursor (100) would have said replication.
        let p = parse("persisted>=1 && in_memory>=2").unwrap();
        let m = metrics((400, 100), (0, 0));
        let a = flags(true, false);
        assert_eq!(
            evaluate_gate(&p, 300, WireSeq::new(300), Some(&m), Some(&a)).1,
            Some(Blocker::Journal)
        );

        // Same policy, replica in-memory now the laggard → replication.
        // The gate opens at the replica's in-memory cursor (200).
        let m = metrics((200, 100), (0, 0));
        assert_eq!(
            evaluate_gate(&p, 200, WireSeq::new(300), Some(&m), Some(&a)).1,
            Some(Blocker::Replication)
        );
    }

    #[test]
    fn hybrid_is_satisfied_by_the_faster_replica() {
        // `in_memory>=2` needs the primary plus *one* replica, so the
        // best replica binds, not the worst. Replica 0 holds the event
        // in memory at 400 with its own fsync trailing at 250 (behind
        // the primary, as the physical ordering requires); replica 1
        // lags badly at 10. The gate is not held up by replication —
        // the primary's persisted cursor (300) is the binding term.
        // Taking the min across replicas would have blamed the slow
        // slot.
        let p = parse("persisted>=1 && in_memory>=2").unwrap();
        let m = metrics((400, 250), (10, 10));
        let a = both_active();
        assert_eq!(
            evaluate_gate(&p, 300, WireSeq::new(300), Some(&m), Some(&a)).1,
            Some(Blocker::Journal)
        );
    }

    #[test]
    fn durably_replicated_uses_second_largest_persisted() {
        // `persisted>=2` is met by the primary plus the *best* replica's
        // persisted cursor. With the primary ahead at 900 and replicas
        // at 400 and 10, the clause resolves at 400 — the fast replica —
        // not at the minimum of 10.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((400, 400), (10, 10));
        let a = both_active();
        assert_eq!(
            evaluate_durability(&p, WireSeq::new(900), Some(&m), Some(&a)).durable_pos,
            400,
            "clause takes the second-largest persisted cursor, not the minimum"
        );
        // Replication binds, which is the norm for this mode: the
        // primary persists before it ships, so it is normally ahead of
        // every replica.
        assert_eq!(
            evaluate_gate(&p, 400, WireSeq::new(900), Some(&m), Some(&a)).1,
            Some(Blocker::Replication)
        );
    }

    #[test]
    fn standalone_credits_the_journal() {
        // No replication wired: the primary is the only node, so the
        // journal is the only thing the gate can be waiting on.
        let p = parse("persisted>=1").unwrap();
        assert_eq!(
            evaluate_gate(&p, 500, WireSeq::new(500), None, None).1,
            Some(Blocker::Journal)
        );
    }

    #[test]
    fn unsatisfiable_shape_never_opens_so_never_attributes() {
        // `persisted>=2` with no replica connected: the shape pins
        // `durable_pos` to 0, so the gate cannot open and no blocker is
        // ever produced — the stall is a missing node, not either
        // subsystem's progress. `policy_degraded` is the metric for it.
        let p = parse("persisted>=2").unwrap();
        let (status, blocker) = evaluate_gate(&p, 1, WireSeq::new(500), None, None);
        assert!(status.degraded);
        assert_eq!(status.durable_pos, 0);
        assert_eq!(blocker, None);
    }

    #[test]
    fn closed_gate_yields_no_attribution() {
        // The policy is satisfiable and healthy, but the batch needs a
        // sequence the cursors haven't reached — the gate stays closed
        // and `evaluate_gate` must not spend a ranking pass (nor name a
        // blocker) on an iteration that keeps spinning.
        let p = parse("persisted>=1").unwrap();
        let (status, blocker) = evaluate_gate(&p, 501, WireSeq::new(500), None, None);
        assert!(!status.degraded);
        assert_eq!(status.durable_pos, 500);
        assert_eq!(blocker, None);
    }

    #[test]
    fn attribution_skips_disconnected_slots() {
        // Slot 1 is disconnected, so its (very advanced) cursors must
        // not satisfy `in_memory>=2`; only slot 0 counts, and it lags.
        // The gate opens at slot 0's in-memory cursor (100).
        let p = parse("persisted>=1 && in_memory>=2").unwrap();
        let m = metrics((100, 100), (999, 999));
        let a = flags(true, false);
        assert_eq!(
            evaluate_gate(&p, 100, WireSeq::new(500), Some(&m), Some(&a)).1,
            Some(Blocker::Replication)
        );
    }

    /// Fresh-cluster catch-up: a replica that handshakes at sequence
    /// 0 (the legitimate genesis case, not a stale-flag race) must be
    /// included in the cursor view with its zero cursors so the policy
    /// behaves the same way it would for a 1-replica deployment that
    /// has just produced its first batch. The disconnect-race
    /// mitigations (B1 seed-on-connect + B2 reorder) keep this from
    /// being conflated with the stale-flag-paired-with-zero-cursor
    /// case under normal cluster lifecycles.
    #[test]
    fn fresh_cluster_zero_cursors_included_in_view() {
        let p = parse("persisted>=2").unwrap();
        // Both replicas just handshook at seq 0, primary also at 0
        // (fresh cluster, no events yet). View = 3 nodes; the clause's
        // count (=2) is met by the view size, so the policy is not
        // degraded and the gate sits at the 2nd-largest persisted = 0.
        let m = metrics((0, 0), (0, 0));
        let a = both_active();
        let r = evaluate_durability(&p, WireSeq::new(0), Some(&m), Some(&a));
        assert_eq!(r.durable_pos, 0);
        assert!(
            !r.degraded,
            "all 3 nodes present, view meets clause target — should not flag degraded"
        );
    }

    // -- Race-window regression tests --
    //
    // The replication senders fix two memory-ordering issues at the
    // active-flag transition points:
    //
    //   B1 (`a84540a`): seed `metrics.{acked,in_memory}_sequence[i]`
    //   to `handshake.last_sequence` BEFORE setting active_flag=true
    //   on reconnect. Without this, the gate would observe (active=
    //   true, cursor=0) for ~1 RTT after a replica catch-up completed,
    //   pinning any multi-node clause — and thus the gate — to 0.
    //
    //   B2 (`8888732`): zero `metrics.{acked,in_memory}_sequence[i]`
    //   BEFORE setting active_flag=false on disconnect. Without this,
    //   a weak-memory reader could observe (active=true, cursor=0)
    //   for one iteration during the disconnect window.
    //
    // Both fixes are in the senders, but the gate's *behaviour* under
    // the race-window inputs is tested here. The intent is to lock in
    // the invariant: even under a hypothetical (active=true,cursor=0)
    // observation, the gate must not produce a spuriously-open answer
    // that would cause a client to be told "your event is durable"
    // when it isn't. Stalling-briefly is safe; opening-spuriously is
    // not.

    #[test]
    fn race_b1_post_seed_gate_doesnt_freeze_on_reconnect() {
        // Post-B1-fix state: replica reconnected, cursors seeded to
        // `handshake.last_sequence` (480) before active flipped to
        // true. Primary kept moving and is at 500. The gate's view
        // is now [primary=500, slot=480]; the durable position dips
        // from 500 (primary alone, degraded) to 480 (both nodes).
        //
        // The dip is correct, not a bug: once a 2nd node is
        // connected, durability is bounded by the slower of the two.
        // Events 481-500 were already served as durable on primary
        // alone — they aren't unsent. New responses for seq>500 wait
        // until slot acks; we just don't freeze at 0.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((480, 480), (999, 999));
        let a = flags(true, false);
        let r = evaluate_durability(&p, WireSeq::new(500), Some(&m), Some(&a));
        assert_eq!(
            r.durable_pos, 480,
            "post-seed reconnect should produce a coherent gate position equal to the slower node, not freeze at 0"
        );
    }

    #[test]
    fn race_b1_pre_seed_freeze_is_what_the_fix_avoids() {
        // Pre-B1-fix state: cursors at 0, active=true. The gate sees
        // [primary=500, slot=[0,0]] and 2nd-largest persisted = 0.
        // The gate WOULD freeze at 0. This test documents the bug
        // the seeding fix is designed to avoid; the senders ensure
        // this state is never observed in production.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((0, 0), (999, 999));
        let a = flags(true, false);
        let r = evaluate_durability(&p, WireSeq::new(500), Some(&m), Some(&a));
        assert_eq!(
            r.durable_pos, 0,
            "the gate behaviour under (active=true, cursor=0) — if the senders ever fail to seed before flipping active, this is the freeze the operator would see"
        );
    }

    #[test]
    fn race_b2_disconnect_window_doesnt_open_gate_spuriously() {
        // Simulates the B2 race window: a weak-memory reader observes
        // (active=true, cursor=0) for one iteration during the
        // disconnect transition. The slot legitimately has cursor=0
        // because the disconnect handler just zeroed the metrics.
        //
        // Critical invariant: the gate must NOT produce a higher
        // durable_pos than it would with the slot correctly excluded.
        // Specifically: with primary at 500, slot stale-zero-included,
        // the gate must not "see" the primary alone and open at 500
        // — that would let a client be told a seq is durable when
        // only the primary has it under a `persisted>=2` policy that
        // demands 2 nodes.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((0, 0), (999, 999));
        let a = flags(true, false);
        let r = evaluate_durability(&p, WireSeq::new(500), Some(&m), Some(&a));
        // 2nd-largest persisted across {primary=500, slot=0} = 0.
        // Gate stalls. ✓
        assert_eq!(r.durable_pos, 0);

        // Post-disconnect (active=false): view shrinks to {primary}.
        // `persisted>=2` is structurally unsatisfiable on a 1-node
        // view, so the gate stays at 0 AND surfaces degraded. The
        // matching stage's `replicas_connected==0` halt is what stops
        // accepting new orders; the gate side's job is just to keep
        // the existing in-flight orders stalled and the alert lit.
        let a_disconnected = flags(false, false);
        let r_after = evaluate_durability(&p, WireSeq::new(500), Some(&m), Some(&a_disconnected));
        assert_eq!(r_after.durable_pos, 0);
        assert!(
            r_after.degraded,
            "post-disconnect view of size 1 cannot meet persisted>=2 → degraded"
        );
    }

    #[test]
    fn race_invariant_zero_cursor_never_opens_gate_above_slower_node() {
        // Property under both B1 and B2 race windows: for any slot
        // observed at cursor=0 with active=true, the gate cannot
        // produce a durable_pos that exceeds what an honest 2-node
        // evaluation would give. Spot-check a handful of primary
        // positions to lock the invariant.
        let p = parse("persisted>=2").unwrap();
        let m = metrics((0, 0), (999, 999));
        let a = flags(true, false);
        for primary_pos in [0, 1, 100, 500, 1_000_000_000_u64] {
            let r = evaluate_durability(&p, WireSeq::new(primary_pos), Some(&m), Some(&a));
            // 2nd-largest of {primary_pos, 0} = 0 for any primary > 0.
            // For primary_pos = 0, also 0. So always 0.
            assert_eq!(
                r.durable_pos, 0,
                "race-window observation must not open the gate above 0 for any primary position (got {} for primary_pos={primary_pos})",
                r.durable_pos
            );
        }
    }

    // ------------------------------------------------------------------
    // GateCrossTracker — per-cursor "first transition from below to
    // crossed" inside the gate loop, used by the journal-wait /
    // replica-wait histograms.
    // ------------------------------------------------------------------

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_records_journal_when_strictly_below() {
        // Journal starts at 5 (< 10), repl_min already at 100.
        // Journal crosses on the second observation. Replica was already
        // past at entry, so no replica sample.
        let mut t = GateCrossTracker::new(10);
        t.observe(5, Some(100), 1_000);
        t.observe(15, Some(100), 2_000);
        assert_eq!(t.journal_crossed(), Some(2_000));
        assert_eq!(t.replica_crossed(), None);
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_records_replica_when_strictly_below() {
        // Mirror image: journal already past, replica below at entry.
        let mut t = GateCrossTracker::new(10);
        t.observe(50, Some(5), 1_000);
        t.observe(50, Some(12), 2_000);
        assert_eq!(t.journal_crossed(), None);
        assert_eq!(t.replica_crossed(), Some(2_000));
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_records_both_when_both_below() {
        // Both below at entry, both cross independently.
        let mut t = GateCrossTracker::new(100);
        t.observe(50, Some(60), 1_000); // both below
        t.observe(105, Some(60), 2_000); // journal crosses
        t.observe(105, Some(110), 3_000); // replica crosses
        assert_eq!(t.journal_crossed(), Some(2_000));
        assert_eq!(t.replica_crossed(), Some(3_000));
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_skips_cursor_already_past_at_entry() {
        // Both cursors already >= needed at first observation —
        // neither was on the critical path. No samples.
        let mut t = GateCrossTracker::new(10);
        t.observe(50, Some(100), 1_000);
        // Even later observations don't backfill: was_below is sticky.
        t.observe(60, Some(110), 2_000);
        assert_eq!(t.journal_crossed(), None);
        assert_eq!(t.replica_crossed(), None);
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_first_observation_only_for_cross_decision() {
        // A cursor that goes back below `needed` after first iteration
        // (impossible in practice — cursors are monotonic — but we
        // verify the first-iteration snapshot is what gates the
        // sample). Journal: 50 < 10 false → was_below=false → no sample.
        let mut t = GateCrossTracker::new(10);
        t.observe(50, Some(5), 1_000); // journal already past, replica below
        t.observe(20, Some(12), 2_000); // both >= needed now
        // Journal: was_below=false at entry → still no sample.
        assert_eq!(t.journal_crossed(), None);
        // Replica: was_below=true at entry, crosses on iter 2 → sample.
        assert_eq!(t.replica_crossed(), Some(2_000));
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_holds_first_cross_only() {
        // Once a cross is recorded, later observations don't
        // overwrite — the metric is "when did it first cross", not
        // "when was it last below".
        let mut t = GateCrossTracker::new(10);
        t.observe(5, Some(100), 1_000);
        t.observe(15, Some(100), 2_000); // first cross
        t.observe(25, Some(100), 3_000); // would otherwise re-record
        assert_eq!(t.journal_crossed(), Some(2_000));
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_ignores_replica_dropping_out_mid_wait() {
        // The regression this signature exists for. The replica is
        // behind at entry (was_below latches true), then disconnects
        // part-way through the wait so no replica supplies a binding
        // cursor. That must not be read as "caught up" — the histogram
        // would otherwise report a wait that ended at the disconnect.
        let mut t = GateCrossTracker::new(100);
        t.observe(50, Some(60), 1_000); // replica behind
        t.observe(105, None, 2_000); // replica drops out mid-wait
        t.observe(105, None, 3_000); // still gone
        assert_eq!(t.journal_crossed(), Some(2_000));
        assert_eq!(t.replica_crossed(), None);
        // Reconnecting past `needed` still records the real crossing.
        t.observe(105, Some(150), 4_000);
        assert_eq!(t.replica_crossed(), Some(4_000));
    }

    #[cfg(feature = "tick-to-trade")]
    #[test]
    fn gate_cross_tracker_records_nothing_when_no_replica_clause() {
        // `local` never binds on a replica, so `policy_replica_cursor`
        // returns `None` for the whole wait: was_below stays false and
        // no replica sample is produced.
        let mut t = GateCrossTracker::new(100);
        t.observe(50, None, 1_000);
        t.observe(105, None, 2_000);
        assert_eq!(t.journal_crossed(), Some(2_000));
        assert_eq!(t.replica_crossed(), None);
    }

    // -- DegradationLogger flap-suppression --
    //
    // The logger gates transition logs on a sustained-state hold so a
    // replica flapping at sub-second cadence doesn't spam the journal.
    // These tests don't observe the logs themselves (tracing is
    // process-global and brittle to capture in unit tests); they
    // assert the underlying state machine via the `policy_degraded`
    // gauge, which the logger updates on every tick regardless of
    // log emission.

    fn logger_test_policy() -> crate::durability_policy::Policy {
        parse("persisted>=2").unwrap()
    }

    /// Tick the logger N times at `step` intervals, alternating
    /// `degraded` per call. Returns the gauge value after the last
    /// tick — useful for asserting that flap cycles don't leak the
    /// AtomicBool into a wrong terminal state.
    fn drive_logger(
        logger: &mut DegradationLogger,
        utilization: &StageUtilization,
        policy: &crate::durability_policy::Policy,
        start: Instant,
        states: &[bool],
        step: Duration,
    ) -> bool {
        for (i, &state) in states.iter().enumerate() {
            let now = start + step.checked_mul(i as u32).unwrap_or(Duration::ZERO);
            logger.tick(policy, utilization, state, now, Duration::from_secs(5));
        }
        utilization.policy_degraded.load(Ordering::Relaxed)
    }

    #[test]
    fn logger_gauge_tracks_state_immediately() {
        // The /healthz gauge reflects the *latest* state on every
        // tick — this is what dashboards / alerts read. Sustained-
        // state gating only affects the warn/info log emission.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new(now);

        logger.tick(&p, &utilization, true, now, Duration::from_secs(5));
        assert!(utilization.policy_degraded.load(Ordering::Relaxed));

        logger.tick(
            &p,
            &utilization,
            false,
            now + Duration::from_millis(50),
            Duration::from_secs(5),
        );
        assert!(!utilization.policy_degraded.load(Ordering::Relaxed));
    }

    #[test]
    fn logger_starting_degraded_marks_initial_state_logged() {
        // `new_starting_degraded` emits the startup warn and treats
        // the state as already-logged so the next tick at the same
        // cluster shape doesn't re-emit instantly. The gauge starts
        // at 1.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new_starting_degraded(now, &p);
        // The logger doesn't write the gauge from the constructor —
        // first tick does. Tick at the same state to settle the
        // gauge. No new log line should fire (state hasn't changed).
        logger.tick(
            &p,
            &utilization,
            true,
            now + Duration::from_millis(10),
            Duration::from_secs(5),
        );
        assert!(utilization.policy_degraded.load(Ordering::Relaxed));
    }

    #[test]
    fn logger_accumulates_degraded_seconds_only_while_degraded() {
        // The `policy_degraded_nanos` counter must advance by exactly
        // the wall-clock time spent in the degraded state and stay flat
        // while healthy. Each tick attributes the interval since the
        // previous tick to the state observed at that previous tick.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let start = Instant::now();
        let mut logger = DegradationLogger::new(start);
        let sec = Duration::from_secs(1);

        // t0: healthy -> degraded. The zero-length interval before the
        // transition is attributed to the healthy start: no accrual.
        logger.tick(&p, &utilization, true, start, Duration::from_secs(5));
        assert_eq!(utilization.policy_degraded_nanos.load(Ordering::Relaxed), 0);

        // t0..t1 spent degraded -> +1s.
        logger.tick(&p, &utilization, true, start + sec, Duration::from_secs(5));
        // t1..t2 still degraded, then flips healthy at the tick -> +1s.
        logger.tick(
            &p,
            &utilization,
            false,
            start + 2 * sec,
            Duration::from_secs(5),
        );
        assert_eq!(
            utilization.policy_degraded_nanos.load(Ordering::Relaxed),
            2 * sec.as_nanos() as u64
        );

        // t2..t3 spent healthy -> counter holds flat.
        logger.tick(
            &p,
            &utilization,
            false,
            start + 3 * sec,
            Duration::from_secs(5),
        );
        assert_eq!(
            utilization.policy_degraded_nanos.load(Ordering::Relaxed),
            2 * sec.as_nanos() as u64
        );
    }

    #[test]
    fn logger_starting_degraded_accrues_from_construction() {
        // A primary that boots already-degraded (e.g. `hybrid` with no
        // replica yet) must accrue from construction, not from the first
        // observed transition — `new_starting_degraded` seeds the
        // degraded state so the first tick's interval counts.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let start = Instant::now();
        let mut logger = DegradationLogger::new_starting_degraded(start, &p);

        logger.tick(
            &p,
            &utilization,
            true,
            start + Duration::from_secs(1),
            Duration::from_secs(5),
        );
        assert_eq!(
            utilization.policy_degraded_nanos.load(Ordering::Relaxed),
            Duration::from_secs(1).as_nanos() as u64
        );
    }

    #[test]
    fn logger_reseed_flushes_in_flight_degraded_accrual() {
        // A runtime mode swap re-seeds the logger. On the gate-wait path
        // the swap can land after many seconds of wedged-degraded time
        // with no intervening tick, so `reseed` must flush that interval
        // before resetting — otherwise the whole wedge is silently lost,
        // which is exactly the incident the metric exists to measure.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let start = Instant::now();
        let sec = Duration::from_secs(1);

        let mut logger = DegradationLogger::new_starting_degraded(start, &p);
        logger.tick(&p, &utilization, true, start, Duration::from_secs(5));
        assert_eq!(utilization.policy_degraded_nanos.load(Ordering::Relaxed), 0);

        // 30s wedged-degraded with no tick (frozen spin), then swap.
        logger.reseed(&utilization, start + 30 * sec);
        assert_eq!(
            utilization.policy_degraded_nanos.load(Ordering::Relaxed),
            30 * sec.as_nanos() as u64
        );

        // Post-reseed the logger is healthy-start; a healthy interval
        // does not accrue, and `last_tick` was rebased to the swap so
        // the next interval isn't double-counted.
        logger.tick(
            &p,
            &utilization,
            false,
            start + 31 * sec,
            Duration::from_secs(5),
        );
        assert_eq!(
            utilization.policy_degraded_nanos.load(Ordering::Relaxed),
            30 * sec.as_nanos() as u64
        );
    }

    #[test]
    fn logger_starting_degraded_then_first_tick_healthy_counts_construction_interval() {
        // `new_starting_degraded` seeds the degraded state, so the
        // construction→first-tick interval is attributed degraded even
        // when the first observed state is already healthy (the cluster
        // recovered before the first tick). Locks this intentional edge.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let start = Instant::now();
        let mut logger = DegradationLogger::new_starting_degraded(start, &p);

        logger.tick(
            &p,
            &utilization,
            false,
            start + Duration::from_secs(2),
            Duration::from_secs(5),
        );
        assert_eq!(
            utilization.policy_degraded_nanos.load(Ordering::Relaxed),
            Duration::from_secs(2).as_nanos() as u64
        );
    }

    #[test]
    fn logger_counter_matches_total_degraded_time_across_flaps() {
        // The cumulative counter must equal exactly the wall time spent
        // in degraded states across a flap sequence — the guarantee
        // `rate(...degraded_seconds_total)` dashboards depend on.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let start = Instant::now();
        let step = Duration::from_millis(500);
        let mut logger = DegradationLogger::new(start);

        // State observed at tick i (fired at start + i*step). The state
        // set at tick i holds over [tick i, tick i+1], so degraded wall
        // time = the spans where the *prior* tick's state was true:
        // [t1,t2], [t2,t3], [t4,t5] = 3 * step.
        let states = [false, true, true, false, true, false];
        for (i, &s) in states.iter().enumerate() {
            logger.tick(
                &p,
                &utilization,
                s,
                start + step * i as u32,
                Duration::from_secs(5),
            );
        }

        assert_eq!(
            utilization.policy_degraded_nanos.load(Ordering::Relaxed),
            (3 * step).as_nanos() as u64
        );
    }

    #[test]
    fn logger_handles_rapid_flap_without_panic() {
        // Drive the logger through 100 alternating flips at 100ms
        // each (faster than the 1s flap-hold). The state machine
        // must remain coherent — no panics, gauge tracks final
        // state, and `pending_logged` doesn't get stuck.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new(now);
        let states: Vec<bool> = (0..100u32).map(|i| i.is_multiple_of(2)).collect();
        let final_state = drive_logger(
            &mut logger,
            &utilization,
            &p,
            now,
            &states,
            Duration::from_millis(100),
        );
        // 100 states starting at i=0 → final state is i=99 → odd → false.
        assert!(!final_state);
    }

    #[test]
    fn logger_sustained_degraded_eventually_settles() {
        // After a sustained-true state, the logger should be in the
        // "logged the onset" mode. Drive 5 ticks of degraded=true at
        // 500ms intervals — total 2s, well past the 1s flap-hold.
        // Last tick should leave gauge=1 and the heartbeat re-emit
        // window primed (last_log set).
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new(now);
        let final_state = drive_logger(
            &mut logger,
            &utilization,
            &p,
            now,
            &[true; 5],
            Duration::from_millis(500),
        );
        assert!(final_state);
    }

    #[test]
    fn logger_recovery_to_healthy_settles_gauge() {
        // Sustained degraded → sustained healthy. Gauge should end at 0.
        let p = logger_test_policy();
        let utilization = StageUtilization::new();
        let now = Instant::now();
        let mut logger = DegradationLogger::new_starting_degraded(now, &p);
        let mut states = vec![true; 5]; // 2.5s degraded
        states.extend(vec![false; 5]); // 2.5s healthy
        let final_state = drive_logger(
            &mut logger,
            &utilization,
            &p,
            now,
            &states,
            Duration::from_millis(500),
        );
        assert!(!final_state);
    }
}
