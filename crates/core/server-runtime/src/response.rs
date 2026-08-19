//! io_uring-based response stage — routes matching output to connections via
//! `IORING_OP_SEND`.
//!
//! Replaces the blocking `write(2)` + `BufWriter` flush path with batched
//! io_uring sends. Instead of N `write(2)` syscalls (one per dirty connection
//! on flush), we submit N SEND SQEs in a single `io_uring_enter` call.
//!
//! Same SPSC consumption and journal cursor gating as `response.rs`.
//! Runs on a dedicated OS thread.

use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use rustc_hash::FxHashMap;
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

/// Minimum interval between SEND retries to a connection whose socket
/// buffer is full (`MSG_DONTWAIT` completed with `EAGAIN`). The stage
/// busy-spins, so without pacing a blocked connection would cost one
/// futile `io_uring_enter` per loop iteration. 100 µs adds no meaningful
/// delivery delay: a full socket buffer drains at the client's read
/// cadence (millisecond scale), and healthy connections are never paced.
const BLOCKED_RETRY_INTERVAL: Duration = Duration::from_micros(100);

/// Byte-threshold flush trigger for the consumed (busy) path — the
/// still-open half of finding 4 in
/// `docs/internal/latency-audit-2026-07.md`. Under sustained load with
/// an open durability gate, neither the idle-path flush (SPSC never
/// empties) nor the gate-path flush (no wait) runs, so without this
/// trigger responses sit in `send_buf` until `MAX_SEND_BUF` *drops* the
/// connection instead of serving it. Roughly one TCP MSS (1460 for
/// standard Ethernet, minus margin): once a connection has a full
/// segment buffered, flushing early costs nothing in wire efficiency,
/// and the bound keeps `send_buf` ~45 flushes away from the disconnect
/// cap instead of zero.
const FLUSH_BYTES_THRESHOLD: usize = 1400;

/// Slot-count flush trigger: how many appended slots may pass between
/// flushes before one is forced, regardless of how few bytes any single
/// connection has buffered.
///
/// [`FLUSH_BYTES_THRESHOLD`] bounds a *connection's* buffered bytes but
/// not their *age*: the interval to the next flush is the time the
/// busiest connection needs to accumulate an MSS, which grows with
/// client count. At 4 clients that is microseconds; at 100 clients each
/// sending ~40 B responses at a modest rate it is milliseconds, and every
/// connection's already-encoded responses wait that long. Counting slots
/// instead makes the bound depend on the stage's own throughput: one
/// extra `io_uring_enter` per 256 responses is well amortized (~0.1 % of
/// the per-slot cost at 1 M/s) and caps the wait at the time to encode
/// 256 responses — tens of microseconds — however the load is spread.
const FLUSH_SLOT_INTERVAL: usize = 256;

/// Consecutive idle iterations before the loop falls back to
/// `yield_now` (only reached when `--yield-idle` is set; production
/// busy-spins). Doubles as the "sustained idle" threshold past which the
/// idle housekeeping timer may mask its clock read — see the call site.
const IDLE_SPIN_LIMIT: u32 = 1000;

/// The consumed path's flush cadence: both triggers plus the counter
/// they share.
///
/// A struct rather than loose locals because the age counter has to be
/// restarted by *every* flush site (consumed path, pre-gate-wait, idle),
/// and a missed reset silently shortens the bound. One `on_flush` call
/// per site is checkable by eye.
#[derive(Default)]
struct FlushCadence {
    /// Slots appended since the last flush — the age bound.
    slots_since_flush: usize,
    /// Latched by the byte trigger, cleared by [`Self::on_flush`].
    due: bool,
}

impl FlushCadence {
    /// Record one appended slot on a connection that now holds
    /// `buffered_bytes` unflushed.
    #[inline]
    fn on_append(&mut self, buffered_bytes: usize) {
        self.slots_since_flush += 1;
        self.due |= buffered_bytes >= FLUSH_BYTES_THRESHOLD;
    }

    /// Whether a flush must run before the next slot is encoded.
    #[inline]
    fn is_due(&self) -> bool {
        self.due || self.slots_since_flush >= FLUSH_SLOT_INTERVAL
    }

    /// Restart both triggers. Call immediately after any `flush_sends`.
    #[inline]
    fn on_flush(&mut self) {
        self.due = false;
        self.slots_since_flush = 0;
    }
}

/// How long a connection's socket may accept *zero* bytes while it has
/// undelivered data before it is dropped. Partial progress restarts the
/// clock — a slow-but-draining client is `MAX_SEND_BUF`'s problem, not
/// this one's. Complements `MAX_SEND_BUF`, which only catches clients
/// that lag while new responses keep *arriving* — a client that stops
/// reading during a quiet period accumulates almost nothing (heartbeats
/// only) and would otherwise pin its buffered bytes and its slot
/// indefinitely.
const BLOCKED_SEND_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// Live-connection count shared with the accept loop's
    /// `max_connections` gate. Incremented there after auth; decremented
    /// here — the response stage owns the connection map, so "an entry
    /// left the map" is the one place a connection verifiably dies
    /// exactly once, whichever side (reader disconnect or a drop
    /// decision here) initiated it.
    pub active_connections: Arc<AtomicU64>,
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
    /// this struct. No SEND outlives its `flush_sends` call (every SQE
    /// carries `MSG_DONTWAIT`, and the flush reaps all completions before
    /// returning), so the Vec is never mutated while the kernel holds its
    /// pointer.
    send_buf: Vec<u8>,
    /// Last time data was sent to this connection. Used for heartbeat scheduling.
    last_send: Instant,
    /// When this connection's socket last made zero forward progress:
    /// set on `EAGAIN`, reset to the flush timestamp on a partial send
    /// (bytes moved), cleared when the buffer fully drains. `None` while
    /// the socket is accepting everything. Drives retry pacing and the
    /// [`BLOCKED_SEND_TIMEOUT`] drop.
    blocked_since: Option<Instant>,
    /// Last SEND attempt. Read only while blocked, to pace retries at
    /// [`BLOCKED_RETRY_INTERVAL`].
    last_send_attempt: Instant,
    /// Whether this connection is on the dirty list. A flag on the entry
    /// rather than set membership: the append path already holds the
    /// entry (and just wrote its `send_buf`), so the dedup test is a
    /// byte in a cache line that is hot, instead of a hash + probe on
    /// every one of the ~2 frames per request. See `dirty_connections`.
    dirty: bool,
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
        active_connections,
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
    // SINGLE_ISSUER: created and submitted from this thread only — the
    // kernel skips SQ locking and rejects cross-thread submission with
    // EEXIST instead of racing. Matches the journal/replication rings.
    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .build(RING_SIZE)
        .expect("failed to create io_uring instance for response stage");

    // Connection table: maps connection IDs to their state.
    //
    // `FxHashMap` rather than std's SipHash-1-3 map: this is looked up
    // once per output slot on the hot path, and the keys are
    // server-generated monotonic connection ids — a client cannot choose
    // them, so the HashDoS resistance SipHash buys is worth nothing here
    // while its ~20 ns/lookup is not. Pre-sized for a reasonable number
    // of concurrent clients.
    let mut connections: FxHashMap<u64, ConnectionEntry> =
        FxHashMap::with_capacity_and_hasher(256, Default::default());

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
    /// Cadence at which the idle spin does its housekeeping *clock read*.
    /// The heartbeat scan, the policy re-check and the trace-stats flush
    /// each keep their own (coarser) interval; this only bounds how
    /// often the loop asks the clock what time it is, so it must stay at
    /// or below the finest of them (`trace::IDLE_FLUSH_INTERVAL`, 100 ms).
    const IDLE_HOUSEKEEPING_INTERVAL: Duration = Duration::from_millis(10);

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

    // Connections with buffered (unflushed) writes, carried across
    // batches.
    //
    // `Vec<u64>` paired with `ConnectionEntry::dirty` rather than a
    // `HashSet`: membership is never *queried*, only iterated and
    // rebuilt, so the set's only contribution on the hot path was
    // deduplicating repeat appends — which the entry flag does with a
    // branch instead of a hash. Pre-sized to the connection table.
    let mut dirty_connections: Vec<u64> = Vec::with_capacity(256);

    // Flush triggers for the consumed path. Lives across batches because
    // the regime the age bound exists for is a ring that never empties:
    // the byte threshold is per connection, so with many low-rate
    // clients no single `send_buf` reaches an MSS for milliseconds while
    // the stage encodes continuously.
    let mut flush = FlushCadence::default();

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

    // Pre-encode the ServerBusy frame the same way. Sent on behalf of
    // the reader (`ControlEvent::PipelineBusy`) — this stage owns all
    // egress on a client socket, so the busy notice appends to
    // `send_buf` like any other frame instead of racing our sends from
    // the reader thread.
    let server_busy_wire_frame = {
        let mut buf = [0u8; 8];
        let written =
            control_codec::encode_transport_response(&TransportResponse::ServerBusy, &mut buf)
                .expect("ServerBusy encodes");
        buf[..written].to_vec()
    };

    // Pre-encode the BatchEnd terminator once. Unlike the two above this
    // one is on the hot path — every request ends with it — and its
    // bytes are a constant, so re-encoding it per request was pure
    // overhead.
    let batch_end_wire_frame = {
        let mut buf = [0u8; 8];
        let written =
            control_codec::encode_transport_response(&TransportResponse::BatchEnd, &mut buf)
                .expect("BatchEnd encodes");
        buf[..written].to_vec()
    };

    // Coarse timestamp for heartbeat scan — avoids Instant::now() on every spin.
    let mut last_heartbeat_scan = Instant::now();

    // Gates the idle path's housekeeping clock read. The idle spin used
    // to call `Instant::now()` twice per iteration (heartbeat scan gate,
    // policy re-check) to decide it had nothing to do — the same vDSO
    // tax the DPDK stage removed with this timer.
    let mut idle_housekeeping_timer = AmortizedTimer::new();

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
                    &mut dirty_connections,
                    &mut to_remove,
                    &mut cqes,
                );
                #[cfg(feature = "latency-trace")]
                close_e2e_samples(&mut pending_e2e, &mut server_e2e_rec);
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
                            blocked_since: None,
                            last_send_attempt: Instant::now(),
                            dirty: false,
                        },
                    );
                }
                ControlEvent::Disconnected { connection_id } => {
                    // Reader-initiated teardown: the reader already
                    // closed its half. Decrement only if the entry was
                    // actually present — a response-initiated drop
                    // already removed it (and paid the decrement), and
                    // this event is its echo.
                    if connections.remove(&connection_id).is_some() {
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                    unmark_dirty(connection_id, &mut dirty_connections);
                    // Anything this connection had buffered goes with
                    // it, so its queued samples measure bytes that will
                    // never be sent.
                    #[cfg(feature = "latency-trace")]
                    discard_e2e_samples(&mut pending_e2e, &[connection_id]);
                }
                ControlEvent::PipelineBusy { connection_id } => {
                    if let Some(entry) = connections.get_mut(&connection_id) {
                        // Overflow discipline: a busy notice to a peer
                        // already at its buffer cap is not worth more
                        // memory — skip it; the cap (or the blocked
                        // timeout) is about to drop the connection
                        // anyway. Missing entry likewise: best-effort.
                        if entry.send_buf.len() + server_busy_wire_frame.len() <= MAX_SEND_BUF {
                            entry.send_buf.extend_from_slice(&server_busy_wire_frame);
                            entry.last_send = Instant::now();
                            mark_dirty(entry, connection_id, &mut dirty_connections);
                        }
                    }
                }
            }
        }

        // Borrow output slots from the matching stage in place.
        //
        // `read_contiguous` rather than `consume_batch`: the latter
        // memcpy'd every ready slot into a stack array before the first
        // one was touched, and `OutputSlot` embeds the application's
        // largest query response (~330 B for the exchange), so the head
        // slot of a deep batch paid for the whole copy before it could
        // be encoded. Borrowing costs nothing and the loop below reads
        // each slot exactly once anyway.
        //
        // The progress counter now moves *after* the batch instead of
        // before it (`consume_batch` committed up front), so the
        // matching stage cannot reclaim these slots until the loop —
        // durability gate waits included — is done with them. That is
        // at most `MAX_BATCH` of a 1 M-slot ring, and the ring fills at
        // the same rate either way.
        let slots = consumer.read_contiguous(MAX_BATCH);
        if slots.is_empty() {
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
                    &mut dirty_connections,
                    &mut to_remove,
                    &mut cqes,
                );
                flush.on_flush();
                #[cfg(feature = "tick-to-trade")]
                egress_rec.record_elapsed(egress_start, trace::mono_trace_ns());
                #[cfg(feature = "latency-trace")]
                close_e2e_samples(&mut pending_e2e, &mut server_e2e_rec);
                for conn_id in to_remove.drain(..) {
                    if let Some(entry) = connections.remove(&conn_id) {
                        teardown_dropped(entry, &active_connections);
                    }
                }
            }

            // Everything below is periodic housekeeping on
            // sub-second-to-second cadences, and each check needs the
            // current time only to discover it has nothing to do. One
            // amortized tick per idle iteration replaces the two
            // unconditional `Instant::now()` calls that decision used to
            // cost; the inner intervals are unchanged, so cadences are
            // the same (see `IDLE_HOUSEKEEPING_INTERVAL`).
            //
            // The timer's iteration mask is only engaged once the stage
            // has been idle for a sustained stretch (`idle_spins` resets
            // on every consumed batch). That is deliberate: the mask
            // trades cadence for clock reads, and it should only do so
            // when clock reads are the loop's whole cost. A stage that
            // is nearly saturated reaches this path a handful of times
            // between batches — there the read is already rare, and
            // masking it would stretch the one-second heartbeat scan
            // into minutes. `--yield-idle` never masks either: the yield
            // syscall dwarfs a vDSO read.
            if idle_housekeeping_timer
                .tick(
                    IDLE_HOUSEKEEPING_INTERVAL,
                    busy_spin && idle_spins >= IDLE_SPIN_LIMIT,
                )
                .is_some()
            {
                let now = Instant::now();

                // Send heartbeats to idle connections. Only checked
                // during idle periods (SPSC empty) to avoid overhead on
                // the hot path.
                //
                // No end-to-end samples to close here: the sample
                // queue's invariant is that a queued entry implies a
                // dirty connection, so the flush above — which closes
                // the queue whenever anything was dirty — leaves it
                // empty. Anything dirtied from here on is heartbeat
                // frames, which carry no samples.
                if let Some(interval) = heartbeat_interval
                    // Coarse gate: only scan at most once per second.
                    && now.duration_since(last_heartbeat_scan) >= Duration::from_secs(1)
                {
                    last_heartbeat_scan = now;
                    for (&conn_id, entry) in connections.iter_mut() {
                        if heartbeat_due(entry, now, interval, heartbeat_wire_frame.len()) {
                            entry.send_buf.extend_from_slice(&heartbeat_wire_frame);
                            entry.last_send = now;
                            mark_dirty(entry, conn_id, &mut dirty_connections);
                        }
                    }
                    // Flush the heartbeat sends immediately.
                    if !dirty_connections.is_empty() {
                        flush_sends(
                            &mut ring,
                            &mut connections,
                            &mut dirty_connections,
                            &mut to_remove,
                            &mut cqes,
                        );
                        flush.on_flush();
                        for conn_id in to_remove.drain(..) {
                            if let Some(entry) = connections.remove(&conn_id) {
                                teardown_dropped(entry, &active_connections);
                            }
                        }
                    }
                }

                // Re-evaluate the durability policy on a slow timer so
                // the `policy_degraded` flag and the periodic warn track
                // the cluster's real state even on idle / quiet venues.
                // The gate-open block also calls `update_degraded_state`
                // after each consumed batch; this is the equivalent for
                // the no-batch path.
                if now.duration_since(last_policy_check) >= POLICY_CHECK_INTERVAL {
                    last_policy_check = now;
                    let journal_pos = journal_persisted_wire_seq.load();
                    let metrics_ref = replication_metrics.as_deref();
                    let active_ref = replica_active.as_ref();
                    let status = evaluate_durability(&policy, journal_pos, metrics_ref, active_ref);
                    degraded_logger.tick(
                        &policy,
                        &utilization,
                        status.degraded,
                        now,
                        DEGRADED_LOG_INTERVAL,
                    );
                    // Cache the position so the next batch's gate sees a
                    // fresh value rather than spinning from a stale cache.
                    cached_durable_pos = status.durable_pos;
                }

                // Hand buffered latency samples to the stats registry
                // while we have nothing better to do. Reuses the
                // housekeeping clock read, so the spin path picks up no
                // extra `clock_gettime`.
                #[cfg(feature = "latency-trace")]
                if now.duration_since(last_stats_flush) >= trace::IDLE_FLUSH_INTERVAL {
                    last_stats_flush = now;
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
            if busy_spin || idle_spins < IDLE_SPIN_LIMIT {
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

        // `flush` (see `FlushCadence`) is fed at append time and checked
        // after every slot, so the flush runs *within* the batch.
        // Deferring it to the end of the batch would reopen the defect
        // the byte threshold exists to close: one MAX_BATCH batch of
        // large frames (anything over ~64 bytes average) can push a
        // healthy connection from empty past `MAX_SEND_BUF` before a
        // batch-end flush ever ran, tearing it down with nothing
        // written. A flag written at append time rather than a per-slot
        // scan of the dirty set: the scan would cost a hash lookup per
        // dirty connection on the hot path.
        for slot in slots {
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
                        &mut dirty_connections,
                        &mut to_remove,
                        &mut cqes,
                    );
                    flush.on_flush();
                    #[cfg(feature = "tick-to-trade")]
                    egress_rec.record_elapsed(egress_start, trace::mono_trace_ns());
                    #[cfg(feature = "latency-trace")]
                    close_e2e_samples(&mut pending_e2e, &mut server_e2e_rec);
                    // This slot and later ones addressed to a dropped
                    // connection are skipped by the
                    // `connections.get_mut` lookup below, so removing
                    // here is safe.
                    for conn_id in to_remove.drain(..) {
                        if let Some(entry) = connections.remove(&conn_id) {
                            teardown_dropped(entry, &active_connections);
                        }
                    }
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

                // Frame 2: the pre-encoded BatchEnd terminator, appended
                // with the payload in one call.
                let trailer: &[u8] = if slot.is_last_in_request {
                    &batch_end_wire_frame
                } else {
                    &[]
                };

                #[cfg(feature = "tick-to-trade")]
                let encode_start = trace::mono_trace_ns();
                // Underscored: with both frames in one call there is no
                // second append to skip, so the outcome is purely
                // informational — `append_frames` has already queued an
                // overflowing connection for removal. Only the traced
                // build reads it, to avoid measuring bytes that were
                // never buffered.
                let _outcome = append_frames(
                    payload_result,
                    trailer,
                    slot.connection_id,
                    entry,
                    &encode_buf,
                    batch_now,
                    &mut dirty_connections,
                    &mut to_remove,
                );
                #[cfg(feature = "tick-to-trade")]
                encode_rec.record_elapsed(encode_start, trace::mono_trace_ns());

                // Queue the server-side end-to-end sample: reader recv ->
                // response flush. Only a request's last slot carries this
                // measurement; queued after the append so a dropped
                // connection doesn't skew the metric, and closed by
                // whichever flush ships the bytes.
                #[cfg(feature = "latency-trace")]
                if slot.is_last_in_request && matches!(_outcome, AppendOutcome::Continue) {
                    pending_e2e.push((slot.connection_id, slot.recv_ts));
                }

                flush.on_append(entry.send_buf.len());
            }

            // Consumed-path flush — the still-open half of July-audit
            // finding 4, plus its age bound (August audit finding 3).
            // Under sustained load with an open gate this is the ONLY
            // flush that runs: the idle path needs an empty SPSC, the
            // gate path needs a durability wait, and heartbeats need
            // idleness. Without it, delivery on a busy stretch
            // degenerates to `MAX_SEND_BUF` evicting the very clients
            // being served. Trigger-gated so light traffic keeps full
            // batching; once a connection holds ~an MSS — or the stage
            // has encoded `FLUSH_SLOT_INTERVAL` responses since the last
            // flush, whichever comes first — the extra submit no longer
            // costs wire efficiency. MSG_DONTWAIT flushes cannot block,
            // so this adds no head-of-line exposure (the hazard that
            // deferred this trigger in July). Runs between slots, not
            // after the batch, so `MAX_SEND_BUF` can never trip on a
            // healthy connection within a single batch — a blocked peer
            // re-arms the flag on its later slots, but its paced retry
            // inside `flush_sends` keeps that cheap.
            if flush.is_due() {
                flush.on_flush();
                // Overflow-dropped connections leave first so the flush
                // skips them — same order as the batch-end cleanup, and
                // their queued samples go with them (they measure bytes
                // that will never be flushed). Later slots addressed to
                // them are skipped by the `connections.get_mut` miss
                // above.
                #[cfg(feature = "latency-trace")]
                discard_e2e_samples(&mut pending_e2e, &to_remove);
                for conn_id in to_remove.drain(..) {
                    if let Some(entry) = connections.remove(&conn_id) {
                        teardown_dropped(entry, &active_connections);
                    }
                    unmark_dirty(conn_id, &mut dirty_connections);
                }
                #[cfg(feature = "tick-to-trade")]
                let egress_start = trace::mono_trace_ns();
                flush_sends(
                    &mut ring,
                    &mut connections,
                    &mut dirty_connections,
                    &mut to_remove,
                    &mut cqes,
                );
                #[cfg(feature = "tick-to-trade")]
                egress_rec.record_elapsed(egress_start, trace::mono_trace_ns());
                #[cfg(feature = "latency-trace")]
                close_e2e_samples(&mut pending_e2e, &mut server_e2e_rec);
                for conn_id in to_remove.drain(..) {
                    if let Some(entry) = connections.remove(&conn_id) {
                        teardown_dropped(entry, &active_connections);
                    }
                }
            }
        }

        // The borrowed slots are dead here, so the batch can be handed
        // back to the producer. Publishing progress is deliberately the
        // last thing the iteration does with the ring: every slot has
        // cleared its durability gate and been encoded by now.
        consumer.commit();

        // Remove connections that exceeded the send buffer limit. Like
        // the disconnect handler, this un-dirties a connection without
        // flushing it, so it has to discard that connection's queued
        // samples too — otherwise they outlive the bytes they measure
        // and land on some later, unrelated flush.
        #[cfg(feature = "latency-trace")]
        discard_e2e_samples(&mut pending_e2e, &to_remove);
        for conn_id in to_remove.drain(..) {
            if let Some(entry) = connections.remove(&conn_id) {
                teardown_dropped(entry, &active_connections);
            }
            unmark_dirty(conn_id, &mut dirty_connections);
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

/// Tear down a connection the *response stage* decided to drop (send
/// error, `MAX_SEND_BUF` overflow, blocked-send timeout).
///
/// Removing the map entry alone only drops this stage's dup of the
/// socket — the reader still holds its own dup, so the socket would
/// stay fully open: the dropped client's requests keep flowing through
/// the reader (whose idle timeout never fires while the client keeps
/// sending) with no response ever delivered, and the accept loop's
/// `max_connections` permit stays pinned forever. `shutdown(2)` reaches
/// every dup: the reader's multishot RECV completes with 0, it tears
/// down its slab entry and emits `Disconnected` — which finds this
/// entry already gone, so the permit is released exactly once.
fn teardown_dropped(entry: ConnectionEntry, active_connections: &AtomicU64) {
    // Best-effort: ENOTCONN just means the peer tore the socket down
    // first — dropping `entry` below still reclaims our dup.
    unsafe {
        libc::shutdown(entry.fd, libc::SHUT_RDWR);
    }
    active_connections.fetch_sub(1, Ordering::Relaxed);
}

/// Outcome of a slot's append. `Continue` means the slot's bytes are
/// buffered (or there were none); `ConnectionDropped` means the
/// connection's send buffer overflowed and the connection has been
/// queued for removal — nothing further should be appended for it.
#[derive(Clone, Copy)]
enum AppendOutcome {
    Continue,
    ConnectionDropped,
}

/// Whether the idle-path heartbeat scan should append a frame to this
/// connection: idle for a full interval — and able to receive it.
///
/// The two guards close the one append path that used to bypass
/// `MAX_SEND_BUF`. A blocked peer's socket is full, so a heartbeat
/// would only sit in `send_buf` — and an idle client that trickle-read
/// a few bytes per interval kept resetting the blocked clock while
/// unchecked heartbeat appends grew its buffer without bound, pinning
/// its connection permit on ever-growing memory. Skipping blocked
/// peers (they detect liveness by draining, not by new frames) and
/// cap-checking the append bounds the buffer; a peer that never drains
/// is then `BLOCKED_SEND_TIMEOUT`'s or `MAX_SEND_BUF`'s problem, as
/// designed. Residual: a trickling client still holds its permit while
/// it drains — bounded memory, and the reader's idle timeout covers
/// clients that stop sending entirely.
/// Put a connection on the dirty list if it isn't already there.
/// Idempotent — the entry flag is the dedup that the old `HashSet`
/// membership provided.
#[inline]
fn mark_dirty(entry: &mut ConnectionEntry, connection_id: u64, dirty: &mut Vec<u64>) {
    if !entry.dirty {
        entry.dirty = true;
        dirty.push(connection_id);
    }
}

/// Take a connection off the dirty list — teardown paths only. `retain`
/// rather than a swap-remove scan because the ordering is cheap to keep
/// and this runs once per dropped connection, never per frame.
fn unmark_dirty(connection_id: u64, dirty: &mut Vec<u64>) {
    dirty.retain(|&id| id != connection_id);
}

fn heartbeat_due(
    entry: &ConnectionEntry,
    now: Instant,
    interval: Duration,
    frame_len: usize,
) -> bool {
    now.duration_since(entry.last_send) >= interval
        && entry.blocked_since.is_none()
        && entry.send_buf.len() + frame_len <= MAX_SEND_BUF
}

/// Copy a slot's wire bytes into the connection's send buffer with
/// overflow checking and dirty tracking: the encoded payload (from the
/// `ResponseEncoder` for application shapes, or
/// `encode_transport_response` for transport ones) followed by an
/// optional pre-encoded `trailer`.
///
/// One call per slot rather than one per frame. A request's last slot
/// carries a `BatchEnd` terminator behind its payload, and appending
/// them separately paid the cap check, the `last_send` stamp and the
/// dirty marking twice for every request — plus a re-encode of a frame
/// whose bytes never change.
#[allow(clippy::too_many_arguments)]
fn append_frames(
    payload: Option<Result<usize, &'static str>>,
    trailer: &[u8],
    connection_id: u64,
    entry: &mut ConnectionEntry,
    encode_buf: &[u8],
    batch_now: Instant,
    dirty_connections: &mut Vec<u64>,
    to_remove: &mut Vec<u64>,
) -> AppendOutcome {
    // An encode failure is this server's bug, not the client's — log it
    // and carry on with the trailer, exactly as when the two frames were
    // appended by separate calls.
    let written = match payload {
        Some(Ok(n)) => n,
        Some(Err(reason)) => {
            tracing::error!(connection_id, reason, "encode error");
            0
        }
        None => 0,
    };
    let total = written + trailer.len();
    if total == 0 {
        return AppendOutcome::Continue;
    }

    // Drop slow clients whose send buffer has grown too large. This
    // prevents unbounded memory growth from a single laggy connection
    // causing allocator pressure and tail latency spikes. Checked
    // against both frames together, so a request is never delivered
    // without its terminator.
    if entry.send_buf.len() + total > MAX_SEND_BUF {
        debug!(
            connection_id,
            send_buf_len = entry.send_buf.len(),
            "send buffer exceeded limit, dropping connection"
        );
        to_remove.push(connection_id);
        return AppendOutcome::ConnectionDropped;
    }

    // Append the full wire frames to the connection's send buffer.
    // The encoder writes [length(4) | payload], which is the complete
    // wire format — no extra framing needed.
    entry.send_buf.extend_from_slice(&encode_buf[..written]);
    entry.send_buf.extend_from_slice(trailer);
    entry.last_send = batch_now;
    mark_dirty(entry, connection_id, dirty_connections);
    AppendOutcome::Continue
}

/// Each dirty connection's accumulated send buffer is sent in a single
/// SEND operation, and the flush never blocks on a slow peer.
///
/// Every SEND carries `MSG_DONTWAIT`, so a full socket buffer completes
/// immediately with `EAGAIN` instead of parking the operation until the
/// peer reads. That matters because io_uring never surfaces `EAGAIN` for
/// a plain SEND — it arms an internal poll and withholds the CQE, which
/// is what let one zero-window client wedge this stage (and with it every
/// client's acks) behind the `submit_and_wait` below.
///
/// Undelivered bytes (EAGAIN or a partial send) stay in the connection's
/// `send_buf` and the connection stays in `dirty`; retries are paced by
/// [`BLOCKED_RETRY_INTERVAL`] and bounded by [`BLOCKED_SEND_TIMEOUT`].
/// `MAX_SEND_BUF` remains the growth cap while new responses accumulate.
///
/// On return, `dirty` holds exactly the connections that still have
/// undelivered bytes and were not queued in `to_remove` — callers must
/// not clear it. Failed connections are collected in `to_remove` for the
/// caller to purge from `connections`.
fn flush_sends(
    ring: &mut IoUring,
    connections: &mut FxHashMap<u64, ConnectionEntry>,
    dirty: &mut Vec<u64>,
    to_remove: &mut Vec<u64>,
    cqes: &mut Vec<(u64, i32)>,
) {
    // One clock read per flush, shared by the retry-pacing and
    // blocked-timeout decisions. Cheap next to the submit syscall below,
    // and flushes are already batched (one per drained SPSC batch or
    // idle-loop pass, not one per response).
    let now = Instant::now();

    // Submit SEND SQEs for all dirty connections not in retry backoff.
    let mut pending: usize = 0;
    for &conn_id in dirty.iter() {
        let Some(entry) = connections.get_mut(&conn_id) else {
            continue;
        };
        if entry.send_buf.is_empty() {
            continue;
        }
        if let Some(blocked_at) = entry.blocked_since {
            // A peer that has refused bytes for the full timeout is not
            // coming back for them — reclaim the slot. Client-caused,
            // hence debug (see the log-level convention).
            if now.duration_since(blocked_at) >= BLOCKED_SEND_TIMEOUT {
                debug!(
                    connection_id = conn_id,
                    pending_bytes = entry.send_buf.len(),
                    blocked_ms = now.duration_since(blocked_at).as_millis() as u64,
                    "socket blocked past timeout, dropping connection"
                );
                to_remove.push(conn_id);
                continue;
            }
            if now.duration_since(entry.last_send_attempt) < BLOCKED_RETRY_INTERVAL {
                continue;
            }
        }
        entry.last_send_attempt = now;

        let sqe = opcode::Send::new(
            types::Fd(entry.fd),
            entry.send_buf.as_ptr(),
            entry.send_buf.len() as u32,
        )
        .flags(libc::MSG_DONTWAIT)
        .build()
        .user_data(conn_id);

        unsafe {
            ring.submission()
                .push(&sqe)
                .expect("io_uring SQ full — increase RING_SIZE");
        }
        pending += 1;
    }

    if pending > 0 {
        // With MSG_DONTWAIT every SEND completes during submission
        // (success, partial, or EAGAIN — the flag sets the kernel's
        // no-wait path, so nothing is deferred to a poll retry). The
        // wait is bounded by op execution, never by a peer.
        //
        // EINTR must retry, not return: a signal (profilers use them
        // routinely) can interrupt the CQE wait *after* the submit phase
        // consumed the SQEs — the inline completions are already posted
        // by then. Returning without reaping would leave stale CQEs that
        // the next flush drains against *updated* buffer contents:
        // a delivered prefix gets re-sent and `drain(..sent)` double-
        // drains. The retry submits an empty SQ and finds the CQ already
        // holding `pending` entries, so it cannot stall.
        loop {
            match ring.submit_and_wait(pending) {
                Ok(_) => break,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => {
                    // Submit-phase failure (ENOMEM class — EBUSY cannot
                    // happen: the CQ is RING_SIZE deep and fully reaped
                    // every flush, so at most `pending` ≤ RING_SIZE
                    // entries are ever outstanding). An error here means
                    // the SQEs were NOT consumed: continuing would leave
                    // them queued for the next flush's submit, by which
                    // time their addr fields can point at drained or
                    // reallocated send_bufs — garbage on client sockets,
                    // on the ack path. The kernel is out of resources
                    // and the stage cannot proceed without corruption
                    // risk: fail loudly and let the accept loop's
                    // pipeline-death detection take the server down.
                    error!(error = %e, "io_uring submit_and_wait failed in response stage");
                    panic!("response stage io_uring submit failed: {e}");
                }
            }
        }

        // Drain completions into pre-allocated buffer. Must collect to
        // release CQ borrow before mutating connections.
        cqes.clear();
        cqes.extend(ring.completion().map(|cqe| (cqe.user_data(), cqe.result())));

        for &(conn_id, result) in cqes.iter() {
            let Some(entry) = connections.get_mut(&conn_id) else {
                continue;
            };
            if result == -libc::EAGAIN {
                // Socket buffer full. Keep the bytes; the connection
                // stays dirty and retries at the paced cadence.
                if entry.blocked_since.is_none() {
                    entry.blocked_since = Some(now);
                }
                continue;
            }
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
            if sent == 0 {
                // Unreachable for SOCK_STREAM sends of len > 0 (the
                // kernel reports -EAGAIN instead), but keep the
                // zero-progress invariant literal: 0 bytes must not
                // restart the blocked clock via the partial branch.
                if entry.blocked_since.is_none() {
                    entry.blocked_since = Some(now);
                }
            } else if sent >= entry.send_buf.len() {
                entry.send_buf.clear();
                entry.blocked_since = None;
            } else {
                // Partial send — the socket buffer filled mid-copy. An
                // immediate retry would only report EAGAIN, so keep the
                // remainder for the paced retry. Bytes moved, though:
                // restart the blocked clock from this progress, so only
                // a peer accepting *zero* bytes for the full timeout is
                // dropped — a slow-but-draining client keeps its
                // connection (until `MAX_SEND_BUF` says otherwise).
                entry.send_buf.drain(..sent);
                entry.blocked_since = Some(now);
            }
        }
    }

    // Keep exactly the connections that still hold undelivered bytes and
    // survived this flush. Runs even when nothing was submitted so a
    // blocked-timeout drop above still leaves the dirty list consistent.
    // The entry flag is cleared in lockstep — it is what keeps a
    // connection off the list until its next append.
    dirty.retain(|conn_id| {
        let keep = !to_remove.contains(conn_id)
            && connections
                .get(conn_id)
                .is_some_and(|e| !e.send_buf.is_empty());
        if !keep && let Some(entry) = connections.get_mut(conn_id) {
            entry.dirty = false;
        }
        keep
    });
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
    use crate::durability_policy::{Clause, DurabilityMode, Level, Policy};
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

    #[test]
    fn ram_quorum_gates_on_replica_memory_not_on_any_disk() {
        // `replicated` (`in_memory>=2`): the journal position must not
        // bind. Journal at 0 — nothing persisted anywhere on the
        // primary — while the replicas hold 100/120 in memory. The
        // 2nd-largest in-memory across {primary=MAX, 100, 120} is 120:
        // the gate is exactly the fastest replica's RAM receipt.
        let p = DurabilityMode::Replicated.to_policy();
        let m = metrics((100, 0), (120, 0));
        let a = both_active();
        let r = evaluate_durability(&p, WireSeq::new(0), Some(&m), Some(&a));
        assert_eq!(r.durable_pos, 120);
        assert!(!r.degraded);
    }

    #[test]
    fn ram_quorum_stalls_with_no_replica_connected() {
        // Fail-closed: with only the primary in the view, `in_memory>=2`
        // is structurally unsatisfiable however far the journal is.
        let p = DurabilityMode::Replicated.to_policy();
        let r = evaluate_durability(&p, WireSeq::new(500), None, None);
        assert_eq!(r.durable_pos, 0);
        assert!(r.degraded);
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

    /// Dirty tracking: an entry flag for dedup plus a flat list for
    /// iteration, replacing the per-append `HashSet` insert.
    mod dirty_tracking {
        use super::super::{mark_dirty, unmark_dirty};
        use super::flush_sends::entry_for;
        use std::os::unix::net::UnixStream;

        fn entry() -> super::super::ConnectionEntry {
            let (tx, _rx) = UnixStream::pair().expect("socketpair");
            let mut e = entry_for(tx, b"payload");
            e.dirty = false;
            e
        }

        #[test]
        fn repeat_appends_enqueue_a_connection_once() {
            let mut e = entry();
            let mut dirty = Vec::new();
            for _ in 0..8 {
                mark_dirty(&mut e, 42, &mut dirty);
            }
            assert_eq!(dirty, vec![42], "the flag is the dedup");
            assert!(e.dirty);
        }

        #[test]
        fn teardown_takes_a_connection_off_the_list() {
            let mut a = entry();
            let mut b = entry();
            let mut dirty = Vec::new();
            mark_dirty(&mut a, 1, &mut dirty);
            mark_dirty(&mut b, 2, &mut dirty);
            unmark_dirty(1, &mut dirty);
            assert_eq!(dirty, vec![2], "order of the survivors is preserved");
        }
    }

    /// One append per slot: the encoded payload plus the request's
    /// pre-encoded `BatchEnd` terminator, sharing a single cap check and
    /// a single dirty marking.
    mod append_frames {
        use super::super::{AppendOutcome, ConnectionEntry, MAX_SEND_BUF, append_frames};
        use super::flush_sends::entry_for;
        use std::os::unix::net::UnixStream;
        use std::time::Instant;

        const TRAILER: &[u8] = &[0xEE, 0xEE];

        fn entry(prefill: usize) -> ConnectionEntry {
            let (tx, _rx) = UnixStream::pair().expect("socketpair");
            let mut e = entry_for(tx, &vec![0u8; prefill]);
            e.dirty = false;
            e
        }

        #[test]
        fn payload_and_terminator_land_in_one_append() {
            let mut e = entry(0);
            let mut dirty = Vec::new();
            let mut to_remove = Vec::new();
            let encoded = [1u8, 2, 3, 4, 0xFF];

            let outcome = append_frames(
                Some(Ok(4)),
                TRAILER,
                7,
                &mut e,
                &encoded,
                Instant::now(),
                &mut dirty,
                &mut to_remove,
            );

            assert!(matches!(outcome, AppendOutcome::Continue));
            assert_eq!(
                e.send_buf,
                vec![1, 2, 3, 4, 0xEE, 0xEE],
                "payload then terminator, nothing past the encoder's length"
            );
            assert_eq!(dirty, vec![7], "one dirty marking for the pair");
            assert!(to_remove.is_empty());
        }

        #[test]
        fn a_slot_with_no_bytes_does_not_dirty_the_connection() {
            // `OutputPayload::BatchEnd` mid-request: no payload, no
            // terminator. Marking it dirty would queue an empty send.
            let mut e = entry(0);
            let mut dirty = Vec::new();
            let mut to_remove = Vec::new();

            let outcome = append_frames(
                None,
                &[],
                7,
                &mut e,
                &[],
                Instant::now(),
                &mut dirty,
                &mut to_remove,
            );

            assert!(matches!(outcome, AppendOutcome::Continue));
            assert!(e.send_buf.is_empty());
            assert!(dirty.is_empty());
            assert!(!e.dirty);
        }

        #[test]
        fn the_cap_is_checked_against_both_frames_together() {
            // Payload alone fits; payload + terminator does not. The
            // connection must be dropped *before* the payload lands —
            // never delivered without its terminator.
            let mut e = entry(MAX_SEND_BUF - 5);
            let mut dirty = Vec::new();
            let mut to_remove = Vec::new();
            let encoded = [1u8, 2, 3, 4];

            let outcome = append_frames(
                Some(Ok(4)),
                TRAILER,
                7,
                &mut e,
                &encoded,
                Instant::now(),
                &mut dirty,
                &mut to_remove,
            );

            assert!(matches!(outcome, AppendOutcome::ConnectionDropped));
            assert_eq!(to_remove, vec![7]);
            assert_eq!(e.send_buf.len(), MAX_SEND_BUF - 5, "no partial append");
            assert!(dirty.is_empty(), "a dropped connection is not queued");
        }

        #[test]
        fn an_encode_failure_still_terminates_the_request() {
            // The payload is this server's bug; the client still needs
            // the terminator or it waits forever for the batch to end.
            let mut e = entry(0);
            let mut dirty = Vec::new();
            let mut to_remove = Vec::new();

            let outcome = append_frames(
                Some(Err("encode error")),
                TRAILER,
                7,
                &mut e,
                &[],
                Instant::now(),
                &mut dirty,
                &mut to_remove,
            );

            assert!(matches!(outcome, AppendOutcome::Continue));
            assert_eq!(e.send_buf, TRAILER);
            assert_eq!(dirty, vec![7]);
        }
    }

    /// The consumed path's two flush triggers. The byte threshold bounds
    /// a connection's buffered *bytes*; the slot interval bounds their
    /// *age*, which is what many-low-rate-client deployments need (2026-08
    /// network audit, finding 3).
    mod flush_cadence {
        use super::super::{FLUSH_BYTES_THRESHOLD, FLUSH_SLOT_INTERVAL, FlushCadence};

        #[test]
        fn byte_threshold_fires_on_a_single_fat_connection() {
            let mut c = FlushCadence::default();
            c.on_append(FLUSH_BYTES_THRESHOLD - 1);
            assert!(!c.is_due());
            c.on_append(FLUSH_BYTES_THRESHOLD);
            assert!(c.is_due());
        }

        #[test]
        fn slot_interval_bounds_the_age_of_tiny_buffers() {
            // Every connection stays far below an MSS — the byte
            // trigger never fires, so before the age bound these
            // responses sat in `send_buf` until the ring went idle.
            let mut c = FlushCadence::default();
            for _ in 0..FLUSH_SLOT_INTERVAL - 1 {
                c.on_append(40);
                assert!(!c.is_due());
            }
            c.on_append(40);
            assert!(
                c.is_due(),
                "age bound must fire on the {FLUSH_SLOT_INTERVAL}th slot"
            );
        }

        #[test]
        fn flushing_restarts_both_triggers() {
            let mut c = FlushCadence::default();
            for _ in 0..FLUSH_SLOT_INTERVAL {
                c.on_append(FLUSH_BYTES_THRESHOLD);
            }
            assert!(c.is_due());
            c.on_flush();
            assert!(
                !c.is_due(),
                "a flush restarts the byte flag and the counter"
            );
            // And the age bound counts from the flush, not from start.
            for _ in 0..FLUSH_SLOT_INTERVAL - 1 {
                c.on_append(1);
            }
            assert!(!c.is_due());
            c.on_append(1);
            assert!(c.is_due());
        }
    }

    /// Regression tests for the slow-client head-of-line block fixed in
    /// the 2026-08 io_uring audit (docs/internal/io-uring-audit-2026-08.md,
    /// finding 1): `flush_sends` used to `submit_and_wait` on SENDs
    /// without `MSG_DONTWAIT`, and io_uring holds such a SEND's CQE back
    /// until the peer reads — so one zero-window client stalled every
    /// client's acks. These use real sockets and a real ring: the defect
    /// was kernel-level behaviour a mock would not reproduce.
    mod heartbeat {
        use super::super::{MAX_SEND_BUF, heartbeat_due};
        use super::flush_sends::entry_for;
        use std::os::unix::net::UnixStream;
        use std::time::{Duration, Instant};

        const INTERVAL: Duration = Duration::from_secs(1);
        const FRAME_LEN: usize = 13;

        /// Backdate `last_send` a full interval so only the receive-
        /// ability guards decide. `checked_sub` because a young
        /// monotonic clock can't be backdated (same pattern as the
        /// flush_sends tests).
        fn idle_entry(payload: &[u8]) -> Option<super::super::ConnectionEntry> {
            let (tx, _rx) = UnixStream::pair().unwrap();
            let mut entry = entry_for(tx, payload);
            entry.last_send = Instant::now().checked_sub(INTERVAL * 2)?;
            Some(entry)
        }

        #[test]
        fn idle_unblocked_connection_gets_a_heartbeat() {
            let Some(entry) = idle_entry(&[]) else {
                eprintln!("monotonic clock too young to backdate; skipping");
                return;
            };
            assert!(heartbeat_due(&entry, Instant::now(), INTERVAL, FRAME_LEN));
        }

        #[test]
        fn recently_active_connection_is_skipped() {
            let (tx, _rx) = UnixStream::pair().unwrap();
            let entry = entry_for(tx, &[]);
            assert!(
                !heartbeat_due(&entry, Instant::now(), INTERVAL, FRAME_LEN),
                "a connection inside the interval must not be pinged"
            );
        }

        /// A blocked peer's socket is full — the frame would only grow
        /// `send_buf`. This is one half of the fix for the unbounded
        /// heartbeat growth on trickle-reading clients.
        #[test]
        fn blocked_connection_is_skipped() {
            let Some(mut entry) = idle_entry(&[]) else {
                eprintln!("monotonic clock too young to backdate; skipping");
                return;
            };
            entry.blocked_since = Some(Instant::now());
            assert!(!heartbeat_due(&entry, Instant::now(), INTERVAL, FRAME_LEN));
        }

        /// The other half: heartbeats respect `MAX_SEND_BUF` like every
        /// other append — this was the one path that bypassed the cap.
        #[test]
        fn append_never_exceeds_the_send_buffer_cap() {
            let payload = vec![0u8; MAX_SEND_BUF - FRAME_LEN + 1];
            let Some(entry) = idle_entry(&payload) else {
                eprintln!("monotonic clock too young to backdate; skipping");
                return;
            };
            assert!(!heartbeat_due(&entry, Instant::now(), INTERVAL, FRAME_LEN));

            let payload = vec![0u8; MAX_SEND_BUF - FRAME_LEN];
            let Some(entry) = idle_entry(&payload) else {
                eprintln!("monotonic clock too young to backdate; skipping");
                return;
            };
            assert!(
                heartbeat_due(&entry, Instant::now(), INTERVAL, FRAME_LEN),
                "a frame that exactly fits is allowed"
            );
        }
    }

    mod flush_sends {
        use super::super::{
            BLOCKED_RETRY_INTERVAL, BLOCKED_SEND_TIMEOUT, ConnectionEntry, flush_sends,
        };
        use io_uring::IoUring;
        use rustc_hash::FxHashMap;
        use std::io::Read as _;
        use std::os::unix::io::{AsRawFd, RawFd};
        use std::os::unix::net::UnixStream;
        use std::time::{Duration, Instant};

        pub(super) fn entry_for(stream: UnixStream, payload: &[u8]) -> ConnectionEntry {
            ConnectionEntry {
                fd: stream.as_raw_fd(),
                _owner: Box::new(stream),
                send_buf: payload.to_vec(),
                last_send: Instant::now(),
                blocked_since: None,
                last_send_attempt: Instant::now(),
                // Every entry these tests build is handed to `flush_sends`
                // on the dirty list, so the flag has to agree with it.
                dirty: true,
            }
        }

        /// Fill `fd`'s socket send buffer with non-blocking writes until
        /// the kernel refuses more — the state a zero-window / non-reading
        /// client leaves a connection in.
        fn fill_socket(fd: RawFd) {
            let junk = [0u8; 4096];
            loop {
                let n =
                    unsafe { libc::send(fd, junk.as_ptr().cast(), junk.len(), libc::MSG_DONTWAIT) };
                if n < 0 {
                    let err = std::io::Error::last_os_error();
                    assert_eq!(
                        err.raw_os_error(),
                        Some(libc::EAGAIN),
                        "unexpected errno while filling socket"
                    );
                    break;
                }
            }
        }

        /// The defect this module exists to prevent: a peer that stops
        /// reading must cost the flush nothing, and healthy peers on the
        /// same flush must still be delivered. Against the pre-fix code
        /// this test never returns.
        #[test]
        fn slow_peer_does_not_block_flush_and_healthy_peer_is_delivered() {
            let mut ring = IoUring::new(64).unwrap();
            let (slow_tx, _slow_rx) = UnixStream::pair().unwrap();
            let (fast_tx, mut fast_rx) = UnixStream::pair().unwrap();
            fill_socket(slow_tx.as_raw_fd());

            let mut connections = FxHashMap::default();
            connections.insert(1u64, entry_for(slow_tx, &[0xAA; 1024]));
            connections.insert(2u64, entry_for(fast_tx, b"hello"));
            let mut dirty: Vec<u64> = vec![1, 2];
            let mut to_remove = Vec::new();
            let mut cqes = Vec::new();

            let start = Instant::now();
            flush_sends(
                &mut ring,
                &mut connections,
                &mut dirty,
                &mut to_remove,
                &mut cqes,
            );
            // Generous bound to stay unflaky under CI load — the fixed
            // path completes in microseconds, the broken one only when
            // the slow peer reads (here: never).
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "flush blocked on a slow peer"
            );

            assert!(to_remove.is_empty(), "nobody dropped on first refusal");
            let slow = &connections[&1];
            assert!(
                !slow.send_buf.is_empty(),
                "slow peer keeps its undelivered bytes"
            );
            assert!(slow.blocked_since.is_some(), "slow peer marked blocked");
            assert!(dirty.contains(&1), "slow peer stays dirty for retry");
            assert!(slow.dirty, "and its flag agrees with the list");

            assert!(connections[&2].send_buf.is_empty(), "healthy peer drained");
            assert!(!dirty.contains(&2), "healthy peer leaves the dirty set");
            // Flag and list must clear together: a stale `true` would
            // keep the connection off the list forever, since `mark_dirty`
            // only pushes when the flag is down.
            assert!(!connections[&2].dirty, "and its flag clears with it");
            fast_rx
                .set_read_timeout(Some(Duration::from_secs(1)))
                .unwrap();
            let mut buf = [0u8; 5];
            fast_rx.read_exact(&mut buf).unwrap();
            assert_eq!(&buf, b"hello", "healthy peer's bytes reached the socket");
        }

        /// A payload larger than the socket buffer must be partially
        /// delivered, the remainder kept, and the connection treated as
        /// blocked (an immediate retry would only report EAGAIN).
        #[test]
        fn partial_send_keeps_remainder_and_marks_blocked() {
            let mut ring = IoUring::new(64).unwrap();
            let (tx, _rx) = UnixStream::pair().unwrap();
            // Shrink the send buffer so the payload cannot fit in one go.
            let sz: libc::c_int = 8192;
            let rc = unsafe {
                libc::setsockopt(
                    tx.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_SNDBUF,
                    (&raw const sz).cast(),
                    size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            assert_eq!(rc, 0, "setsockopt(SO_SNDBUF) failed");
            let payload = vec![0x55u8; 512 * 1024];

            // Enter the flush as an already-blocked peer with a stale
            // (but unexpired) block timestamp and an expired pacing
            // window, so the partial delivery below must *restart* the
            // blocked clock — the behaviour that keeps a slow-but-
            // draining client alive past BLOCKED_SEND_TIMEOUT.
            let Some(stale_block) = Instant::now().checked_sub(BLOCKED_SEND_TIMEOUT / 2) else {
                eprintln!("monotonic clock too young to backdate; skipping");
                return;
            };
            let mut entry = entry_for(tx, &payload);
            entry.blocked_since = Some(stale_block);
            entry.last_send_attempt = stale_block;

            let mut connections = FxHashMap::default();
            connections.insert(1u64, entry);
            let mut dirty: Vec<u64> = vec![1];
            let mut to_remove = Vec::new();
            let mut cqes = Vec::new();

            flush_sends(
                &mut ring,
                &mut connections,
                &mut dirty,
                &mut to_remove,
                &mut cqes,
            );

            let entry = &connections[&1];
            assert!(entry.send_buf.len() < payload.len(), "some bytes were sent");
            assert!(!entry.send_buf.is_empty(), "remainder kept for retry");
            let refreshed = entry.blocked_since.expect("partial send marks blocked");
            assert!(
                refreshed > stale_block,
                "forward progress restarts the blocked clock"
            );
            assert!(dirty.contains(&1));
            assert!(to_remove.is_empty());
        }

        /// A connection blocked past `BLOCKED_SEND_TIMEOUT` is dropped —
        /// the guard for clients that stop reading during quiet periods,
        /// where `MAX_SEND_BUF` never trips because nothing accumulates.
        #[test]
        fn blocked_past_timeout_is_dropped() {
            let mut ring = IoUring::new(64).unwrap();
            let (tx, _rx) = UnixStream::pair().unwrap();
            fill_socket(tx.as_raw_fd());

            let mut entry = entry_for(tx, &[1u8; 64]);
            // `checked_sub`: a plain `-` panics when the monotonic clock
            // is younger than the timeout (fresh-boot CI microVMs).
            let Some(expired) = Instant::now().checked_sub(BLOCKED_SEND_TIMEOUT) else {
                eprintln!("monotonic clock too young to backdate; skipping");
                return;
            };
            entry.blocked_since = Some(expired);
            let mut connections = FxHashMap::default();
            connections.insert(7u64, entry);
            let mut dirty: Vec<u64> = vec![7];
            let mut to_remove = Vec::new();
            let mut cqes = Vec::new();

            flush_sends(
                &mut ring,
                &mut connections,
                &mut dirty,
                &mut to_remove,
                &mut cqes,
            );

            assert_eq!(to_remove, vec![7], "timed-out peer queued for removal");
            assert!(!dirty.contains(&7), "dropped peer leaves the dirty set");
        }

        /// A blocked connection retried a moment ago is skipped — the
        /// pacing that keeps a busy-spinning stage from probing a full
        /// socket once per loop iteration.
        #[test]
        fn blocked_retry_is_paced() {
            let mut ring = IoUring::new(64).unwrap();
            let (tx, _rx) = UnixStream::pair().unwrap();
            fill_socket(tx.as_raw_fd());

            let mut entry = entry_for(tx, &[1u8; 64]);
            entry.blocked_since = Some(Instant::now());
            // A future attempt timestamp makes the pacing skip
            // deterministic: `duration_since` saturates to zero, so no
            // CI-load preemption between here and the flush's own clock
            // read can open the 100 µs window and flake the assertion.
            entry.last_send_attempt = Instant::now() + Duration::from_secs(1);
            let attempted_at = entry.last_send_attempt;
            let mut connections = FxHashMap::default();
            connections.insert(3u64, entry);
            let mut dirty: Vec<u64> = vec![3];
            let mut to_remove = Vec::new();
            let mut cqes = Vec::new();

            flush_sends(
                &mut ring,
                &mut connections,
                &mut dirty,
                &mut to_remove,
                &mut cqes,
            );

            assert_eq!(
                connections[&3].last_send_attempt, attempted_at,
                "no SEND submitted inside the pacing window"
            );
            assert!(dirty.contains(&3), "paced peer stays dirty");
            assert!(to_remove.is_empty());

            // Outside the pacing window the retry happens: backdate the
            // last attempt and verify the attempt timestamp advances.
            let Some(backdated) = Instant::now().checked_sub(BLOCKED_RETRY_INTERVAL * 2) else {
                eprintln!("monotonic clock too young to backdate; skipping");
                return;
            };
            connections.get_mut(&3).unwrap().last_send_attempt = backdated;
            flush_sends(
                &mut ring,
                &mut connections,
                &mut dirty,
                &mut to_remove,
                &mut cqes,
            );
            assert!(
                connections[&3].last_send_attempt > backdated,
                "retry submitted once the pacing window elapsed"
            );
        }

        /// Review finding F1 (io-uring-audit-2026-08.md): a
        /// response-initiated drop must tear down the *whole* socket and
        /// release the `max_connections` permit. The reader holds its
        /// own dup of the socket, so merely dropping this stage's entry
        /// (closing our dup) leaves the socket open — the EOF below must
        /// arrive *despite* a live second dup, which only shutdown(2)
        /// achieves.
        #[test]
        fn teardown_shuts_the_socket_despite_other_dups_and_releases_the_permit() {
            use std::sync::atomic::{AtomicU64, Ordering};

            let (tx, mut peer) = UnixStream::pair().unwrap();
            // Simulates the reader's half: still open across the drop.
            let reader_dup = tx.try_clone().unwrap();
            let entry = entry_for(tx, &[]);
            let active = AtomicU64::new(1);

            super::super::teardown_dropped(entry, &active);

            assert_eq!(active.load(Ordering::Relaxed), 0, "permit released");
            peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            let mut buf = [0u8; 1];
            assert_eq!(
                peer.read(&mut buf).expect("EOF, not a timeout"),
                0,
                "peer observes EOF even though another dup is still open"
            );
            drop(reader_dup);
        }
    }
}
