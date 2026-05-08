//! Pipeline stages for the LMAX disruptor architecture.
//!
//! Two hot-path stages consume from an input disruptor in **parallel**:
//! 1. **Journal stage**: batch-encodes events, then writes in a single `pwrite` + `O_DIRECT`.
//!    Advances its cursor only after the write completes. When replication is enabled,
//!    sends a copy of each encoded batch to the replication sender thread via a bounded
//!    channel. The bytes are identical to what was written to disk — same sequences,
//!    timestamps, CRC checksums, and checkpoint entries.
//! 2. **Matching stage**: executes commands on the `Exchange`, publishes responses
//!    to the output SPSC. Runs concurrently with the journal — no waiting for sync.
//!
//! The **response stage** (in the server crate) consumes the output SPSC but
//! gates on `min(journal_cursor, replication_cursor)` before sending: a response
//! is only sent after the event is durable on disk **and** acknowledged by the
//! replica (when replication is active).
//!
//! This gives maximum pipeline parallelism (matching overlaps journal I/O)
//! while preserving persist-before-ack at the response boundary.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use melin_app::{AppEvent, Application, ApplyCtx, RejectReason};
use melin_journal::JournalError;
use melin_journal::replication::{ReplicationConsumer, ReplicationProducer};
use melin_journal::trace::{TraceTimestamp, trace_ts};

use melin_disruptor::padding::Sequence;
use melin_disruptor::ring;
use melin_disruptor::seqlock::SeqLock;

use crate::replication_wire::{finalize_input_batch, init_input_batch};

/// Per-stage busy/idle iteration counters for pipeline utilization monitoring.
///
/// Each pipeline stage (journal, matching, response) owns one instance.
/// The stage thread increments local `u64` counters and periodically flushes
/// to these shared atomics. The health endpoint reads with `Relaxed` ordering
/// — no hot-path contention since the stage thread is the only writer.
///
/// Prometheus exposes these as monotonic counters; `rate(busy) / rate(busy+idle)`
/// gives utilization over any window.
pub struct StageUtilization {
    /// Cumulative iterations where the stage had work to do.
    pub busy: AtomicU64,
    /// Cumulative iterations where the stage was idle (no input available).
    pub idle: AtomicU64,
    /// Cumulative gate-wait events where the journal cursor was the last
    /// to reach the needed position (journal fsync was the bottleneck).
    /// Only used by the response stage; always 0 for journal/matching.
    pub gate_journal: AtomicU64,
    /// Cumulative gate-wait events where the replication cursor was the
    /// last to reach the needed position (replica ack was the bottleneck).
    /// Only used by the response stage; always 0 for journal/matching.
    pub gate_replication: AtomicU64,
}

impl StageUtilization {
    pub fn new() -> Self {
        Self {
            busy: AtomicU64::new(0),
            idle: AtomicU64::new(0),
            gate_journal: AtomicU64::new(0),
            gate_replication: AtomicU64::new(0),
        }
    }
}

impl Default for StageUtilization {
    fn default() -> Self {
        Self::new()
    }
}

/// Ring buffer capacity for the input disruptor (journal + matching consumers).
/// 2^20 = 1,048,576 slots. At ~72 bytes per slot, this is ~72 MiB — fits in
/// L3 cache on modern server CPUs. Provides ~100 ms of buffering at 10M
/// orders/sec, enough headroom for fsync stalls without backpressure.
pub const INPUT_RING_CAPACITY: usize = 1 << 20;

/// SPSC queue capacity for the output path (matching → response).
/// Matches the input ring size since one input event can produce multiple
/// output messages (e.g., Fill + BatchEnd).
pub const OUTPUT_RING_CAPACITY: usize = 1 << 20;

/// Maximum number of events processed in one journal batch.
/// Larger batches amortize the fixed cost of each NVMe write over more
/// events. A single NVMe command covers up to ~128 KiB, so 4096 events ×
/// ~104 bytes ≈ 416 KiB still fits in one write. Under low load, batches are
/// naturally small (drain what's available); the cap only matters at
/// sustained high throughput.
pub const MAX_JOURNAL_BATCH: usize = 4096;

/// Spin-wait idle hint. When `busy_spin` is false (default), falls back to
/// `sched_yield` after 1000 spins — courteous on shared cores but expensive
/// on EPYC (~1-5µs per yield). When true, spins indefinitely with PAUSE —
/// the thread owns the core (requires `isolcpus`).
#[inline(always)]
fn idle_wait(idle_spins: &mut u32, busy_spin: bool) {
    if busy_spin || *idle_spins < 1000 {
        *idle_spins = idle_spins.wrapping_add(1);
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
}

/// Maximum events consumed per disruptor batch in the matching stage.
/// Amortizes one atomic Release store over N events. Keep small to avoid
/// burstiness that causes the response stage to wait on the journal cursor.
/// 16 events × ~100 ns/event = ~1.6 µs worst-case batch queuing jitter.
/// Halved from 32 to reduce tail latency at the cost of 2x more atomic
/// stores per second (~5-8ns each, negligible at this batch size).
const MAX_MATCHING_BATCH: usize = 16;

/// Slot in the input disruptor ring buffer.
///
/// Carries a connection ID alongside the event so the response stage
/// knows where to route execution reports. `Copy` for zero-cost ring
/// buffer ops. Generic over `E: AppEvent` — the concrete engine crate
/// aliases this to `InputSlot<TradingEvent>`.
#[derive(Debug, Clone, Copy)]
pub struct InputSlot<E: AppEvent> {
    /// Which client connection submitted this command.
    pub connection_id: u64,
    /// FxHash of the client's Ed25519 public key. Used with `request_seq`
    /// for per-key idempotency dedup. 0 for seed/internal events.
    pub key_hash: u64,
    /// Per-key monotonic request sequence number from the wire protocol.
    /// Used with `key_hash` for idempotency dedup. 0 for seed/internal events.
    pub request_seq: u64,
    /// Journal sequence number. **Always zero on primary-side input** —
    /// the journal stage allocates the sequence at encode time, in
    /// disruptor cursor order, so producers never have to coordinate
    /// across an external counter. On replicas the replication receiver
    /// stamps the primary's sequence here before publishing, and the
    /// journal stage uses that value verbatim. Also zero for non-journaled
    /// events (queries) which the journal stage skips.
    pub sequence: u64,
    /// Wall-clock timestamp (nanoseconds since epoch), assigned at
    /// publish time alongside the sequence. Zero only for non-journaled
    /// events (queries).
    pub timestamp_ns: u64,
    /// The journaled event (order submit, cancel, etc.).
    pub event: melin_journal::JournalEvent<E>,
    /// Timestamp when the publisher wrote this slot to the disruptor.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub publish_ts: TraceTimestamp,
    /// Timestamp when the reader task received this request from the wire.
    /// Flows through the entire pipeline to measure server-side end-to-end latency.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub recv_ts: TraceTimestamp,
}

impl<E: AppEvent> Default for InputSlot<E> {
    fn default() -> Self {
        // Default uses a transport-intrinsic `Tick` as placeholder — it
        // works for any `E` without requiring `E: Default`. Ring buffer
        // slots are always overwritten before being read, so the default
        // value is never observed in steady state.
        Self {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: melin_journal::JournalEvent::Tick { now_ns: 0 },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        }
    }
}

impl<E: AppEvent> InputSlot<E> {
    /// Build the pipeline-shutdown sentinel slot. The producer (receiver
    /// or reader thread) publishes this as its last action before exit;
    /// each downstream stage stops at this slot, completing any pending
    /// work and then returning. The `event` is `JournalEvent::Shutdown`,
    /// which the journal stage drops without writing — it's a transient
    /// pipeline signal, never persisted.
    pub fn shutdown_sentinel() -> Self {
        Self {
            event: melin_journal::JournalEvent::Shutdown,
            ..Self::default()
        }
    }
}

/// Slot in the output SPSC queue (matching → response).
///
/// Each slot carries an execution report, a query response, or a
/// terminator marker for a specific connection, plus the input
/// sequence it originated from so the response stage can gate on
/// journal completion.
///
/// The matching stage sets `is_last_in_request` on the final slot
/// it emits for one input event; the response stage uses that to
/// emit a wire `ResponseKind::BatchEnd` after the payload, so
/// matching no longer needs to publish a separate `BatchEnd` slot
/// when the event already produced at least one report or query
/// response. Events with no reports still emit a single
/// `BatchEnd`-payload slot with `is_last_in_request=true`.
#[derive(Debug, Clone, Copy)]
pub struct OutputSlot<R: Copy, Q: Copy> {
    /// Which client connection receives this response.
    pub connection_id: u64,
    /// Input disruptor sequence this output originated from.
    /// The response stage must not send this until the journal cursor
    /// has advanced past this value (i.e., the event is durable).
    pub input_seq: u64,
    /// The response payload.
    pub payload: OutputPayload<R, Q>,
    /// Timestamp when the matching stage finished processing this event.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub match_complete_ts: TraceTimestamp,
    /// Timestamp when the reader task received this request from the wire.
    /// Carried through the pipeline to measure server-side end-to-end latency.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub recv_ts: TraceTimestamp,
    /// True when this is the final slot the matching stage emits for
    /// the originating input event. The response stage emits a wire
    /// `ResponseKind::BatchEnd` after the payload (skipped when the
    /// payload itself is `BatchEnd` — which is its own terminator).
    pub is_last_in_request: bool,
}

/// Payload within an output slot.
///
/// Generic over the application's report type `R` and query response
/// type `Q`. Must remain `Copy` for zero-allocation ring buffer
/// transport. Large query-response variants (e.g. the trading engine's
/// balance snapshot) dominate the enum size; they are rare enough that
/// the per-slot overhead is acceptable while the hot-path scratch
/// `Vec<R>` stays small.
///
/// `Report(R)` carries fan-out reports (fills, acks, cancels) that
/// flow through the matching stage's scratch vec. `QueryResponse(Q)`
/// carries 1:1 query responses returned directly from
/// `Application::apply`, bypassing the scratch vec entirely.
#[derive(Debug, Clone, Copy)]
#[allow(clippy::large_enum_variant)]
pub enum OutputPayload<R: Copy, Q: Copy> {
    /// An application report from matching.
    Report(R),
    /// A 1:1 query response returned directly from `Application::apply`.
    QueryResponse(Q),
    /// Signals the end of reports for one request.
    BatchEnd,
    /// Internal error during matching.
    EngineError,
}

impl<R: Copy, Q: Copy> Default for OutputSlot<R, Q> {
    fn default() -> Self {
        Self {
            connection_id: 0,
            input_seq: 0,
            payload: OutputPayload::BatchEnd,
            match_complete_ts: trace_ts(),
            recv_ts: trace_ts(),
            is_last_in_request: true,
        }
    }
}

/// Journal stage: consumes from the input disruptor, batch-encodes events,
/// and writes in a single `pwrite` + `O_DIRECT`.
///
/// Runs on a dedicated OS thread. Uses `read_batch` + `commit` so its
/// cursor only advances **after** the durable write. The response stage
/// reads this cursor to know when events are durable.
///
/// When replication is enabled, the journal stage also sends a copy of
/// each encoded batch to the replication sender thread via a bounded
/// channel. The bytes are identical to what was written to disk — same
/// sequences, timestamps, CRC checksums, and checkpoint entries.
pub struct JournalStage<E: AppEvent> {
    writer: melin_journal::JournalWriter<E>,
    consumer: ring::Consumer<InputSlot<E>>,
    /// Group commit coalescing window. The journal stage waits up to this
    /// duration after the first unsynced write before issuing the durable
    /// write, allowing more events to accumulate in the batch. At high
    /// event rates, the batch fills naturally and the delay rarely fires.
    /// Zero means sync immediately (no delay).
    group_commit_delay: Duration,
    /// Maximum events per journal fsync batch. Capped at MAX_JOURNAL_BATCH
    /// (the stack array size). Smaller values reduce tail latency.
    max_batch: usize,
    /// Replication state, boxed to keep the struct small on the hot path.
    /// In standalone mode this is a null-like default (no producers, no
    /// flags). The Box indirection keeps the JournalStage struct the same
    /// cache layout as on main, avoiding tail latency regression.
    repl: Box<ReplicationState>,
    /// Optional SeqLock for publishing the BLAKE3 chain hash to the shadow
    /// snapshot stage. Updated once per fsync batch (cold path). `None` when
    /// shadow snapshots are disabled — no allocation or write overhead.
    chain_hash: Option<Arc<SeqLock<[u8; 32]>>>,
    /// Optional atomic for publishing the writer's `next_sequence - 1`
    /// (the highest journal sequence durably persisted) to readers outside
    /// the pipeline thread. Replicas use this so the orchestrator can read
    /// last-journal-seq for reconnect handshakes without owning the writer.
    /// Updated once per fsync batch alongside `chain_hash`.
    last_seq: Option<Arc<AtomicU64>>,
    /// When true, never yield to the OS scheduler — spin indefinitely with
    /// PAUSE. Requires isolated cores (`isolcpus`). See [`idle_wait`].
    busy_spin: bool,
    /// Shared busy/idle counters for health endpoint monitoring.
    utilization: Arc<StageUtilization>,
}

/// Replication state for the journal stage. Boxed in JournalStage to
/// avoid inflating the struct size on the hot path (standalone mode has
/// no replication but the struct layout affects cache behavior).
struct ReplicationState {
    /// Independent replication ring producers (one per replica slot).
    producers: [Option<ReplicationProducer>; 2],
    /// Per-ring eviction flags.
    evict: [Arc<AtomicBool>; 2],
    /// Per-ring active flags.
    active: [Arc<AtomicBool>; 2],
    /// Wire-ready `InputBatch` frame accumulating between fsync points.
    /// Initialized lazily on the first slot of each batch via
    /// `init_input_batch`; finalized + published at sync time. Empty (and
    /// unused) in standalone mode where both producers are `None`.
    input_batch_buf: Vec<u8>,
    /// Number of slots appended to `input_batch_buf` since the last
    /// publish/reset. Stays in sync with `input_batch_buf`'s contents.
    input_batch_count: u16,
}

impl Default for ReplicationState {
    fn default() -> Self {
        Self {
            producers: [None, None],
            evict: [
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
            ],
            active: [
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
            ],
            input_batch_buf: Vec::new(),
            input_batch_count: 0,
        }
    }
}

impl ReplicationState {
    #[inline]
    fn any_producer(&self) -> bool {
        self.producers[0].is_some() || self.producers[1].is_some()
    }
}

impl<E: AppEvent> JournalStage<E> {
    /// Create a new journal stage.
    ///
    /// `group_commit_delay`: coalescing window for sync batching. The
    /// journal waits up to this duration for more events to arrive before
    /// issuing the durable write. Zero means sync immediately after each
    /// batch read.
    pub fn new(
        writer: melin_journal::JournalWriter<E>,
        consumer: ring::Consumer<InputSlot<E>>,
        group_commit_delay: Duration,
        max_batch: usize,
        busy_spin: bool,
    ) -> Self {
        Self {
            writer,
            consumer,
            group_commit_delay,
            max_batch: max_batch.min(MAX_JOURNAL_BATCH),
            repl: Box::default(),
            chain_hash: None,
            last_seq: None,
            busy_spin,
            utilization: Arc::new(StageUtilization::new()),
        }
    }

    /// Shared utilization counters for health endpoint monitoring.
    pub fn utilization(&self) -> Arc<StageUtilization> {
        Arc::clone(&self.utilization)
    }

    /// Set independent replication ring producers (one per replica slot)
    /// and their shared eviction/active flags. The journal stage only
    /// publishes to rings where `active` is true, and sets the eviction
    /// flag on backpressure timeout.
    pub fn set_replication_producers(
        &mut self,
        producers: [ReplicationProducer; 2],
        evict_flags: [Arc<AtomicBool>; 2],
        active_flags: [Arc<AtomicBool>; 2],
    ) {
        let [p0, p1] = producers;
        self.repl.producers = [Some(p0), Some(p1)];
        self.repl.evict = evict_flags;
        self.repl.active = active_flags;
    }

    /// Set the SeqLock for publishing the BLAKE3 chain hash to the shadow
    /// snapshot stage. Called once during pipeline construction when shadow
    /// snapshots are enabled.
    pub fn set_chain_hash_lock(&mut self, lock: Arc<SeqLock<[u8; 32]>>) {
        self.chain_hash = Some(lock);
    }

    /// Set the atomic for publishing the highest journal sequence durably
    /// persisted. Called once during pipeline construction when readers
    /// outside the pipeline thread (e.g., the replication-receive
    /// orchestrator) need to read the writer's progress without owning it.
    pub fn set_last_seq_publisher(&mut self, last_seq: Arc<AtomicU64>) {
        self.last_seq = Some(last_seq);
    }

    /// Run the journal stage loop.
    ///
    /// Dispatches to the io_uring overlapped path for journal writes.
    /// With the `no-persist` feature, falls back to synchronous writes
    /// (io_uring overlapping is only useful with actual disk I/O).
    ///
    /// Returns the `JournalWriter` on shutdown for clean resource release.
    pub fn run(
        self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<melin_journal::JournalWriter<E>, JournalError> {
        let use_uring = !cfg!(feature = "no-persist");

        if use_uring {
            self.run_uring(shutdown)
        } else {
            self.run_sync(shutdown)
        }
    }

    /// Synchronous journal loop: `pwrite` + `O_DIRECT` blocks until the write completes.
    ///
    /// Uses `read_batch` + `commit` (not `consume_batch`) to ensure the
    /// journal cursor is only advanced **after** the write is durable.
    /// The response stage checks this cursor before sending — this is
    /// the persist-before-ack boundary.
    fn run_sync(
        mut self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<melin_journal::JournalWriter<E>, JournalError> {
        use std::time::Instant;

        let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];
        let delay = self.group_commit_delay;
        let mut idle_spins: u32 = 0;

        // Total events encoded since last sync/commit.
        let mut pending: usize = 0;
        // Timestamp of first unsynced write (for group commit delay).
        let mut first_write_ts: Option<Instant> = None;

        let mut busy_count: u64 = 0;
        let mut idle_count: u64 = 0;

        // Stage histograms registered with the process-global stats
        // registry. Lock cost is irrelevant — `latency-trace` builds
        // are dev/bench only. The registry owns the histogram via Arc;
        // the server's shutdown path calls `trace::print_report_all`
        // after all stage threads join, so dev runs still see the
        // stderr breakdown.
        #[cfg(feature = "latency-trace")]
        let mut wakeup_rec = melin_journal::trace::register_stage(
            "journal: disruptor wakeup (publish → journal consume)",
        );
        #[cfg(feature = "latency-trace")]
        let mut batch_rec =
            melin_journal::trace::register_stage("journal: batch processing (write + sync)");

        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                // Flush any pending data before shutdown.
                if pending > 0 {
                    #[cfg(not(feature = "no-persist"))]
                    if let Err(e) = self.writer.flush_batch_sync() {
                        tracing::error!(error = %e, "journal sync error on shutdown");
                    }
                    self.consumer.commit(pending);
                }
                self.drain_remaining(&mut batch);
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("journal", busy_count, idle_count);
                return Ok(self.writer);
            }

            // Read entries WITHOUT advancing the cursor.
            let remaining = MAX_JOURNAL_BATCH.saturating_sub(pending);
            let count = if remaining > 0 {
                self.consumer.read_batch(&mut batch, remaining)
            } else {
                0
            };

            // Sentinel observed in the inner loop. Set to true the moment
            // we see a `JournalEvent::Shutdown` slot; checked after the
            // batch is sync'd so we exit on the same persist-before-ack
            // boundary as steady-state writes.
            let mut saw_shutdown = false;
            // Number of slots actually consumed from this batch — equals
            // `count` unless we broke out early on the sentinel.
            let mut consumed = 0usize;

            if count > 0 {
                idle_spins = 0;
                busy_count += 1;

                #[cfg(feature = "latency-trace")]
                let batch_start = trace_ts();

                #[cfg(feature = "latency-trace")]
                for slot in &batch[..count] {
                    wakeup_rec.record_elapsed(slot.publish_ts, batch_start);
                }

                // Batch-encode all events into the writer's internal buffer.
                // Data stays in the buffer until the write point — one
                // O_DIRECT pwrite covers the entire batch.
                // QueryStats/QueryPosition are not journaled (no state change).
                // Checkpoint events are not encoded (each node auto-emits
                // its own), but their chain hash is verified for divergence
                // detection when received from a primary.
                //
                // The journal stage is the authoritative sequence allocator
                // on the primary: when `slot.sequence == 0` (every primary-
                // side input) we allocate at encode time in disruptor cursor
                // order. On replicas the replication receiver stamps the
                // primary's sequence onto `slot.sequence` before publish, and
                // we use it verbatim (also syncing the writer's counter so
                // its own checkpoint auto-emission stays aligned).
                // Encoding always runs — under no-persist the bytes still
                // populate `batch_buf` so replication can publish them, and
                // sequence allocation must happen so downstream stages see
                // the same numbering they would in durable mode. The
                // discard happens at the sync point below in place of the
                // fsync, keeping `batch_buf` bounded.
                for slot in &batch[..count] {
                    if slot.event.is_shutdown() {
                        saw_shutdown = true;
                        break;
                    }
                    consumed += 1;
                    if slot.event.is_query() {
                        continue;
                    }
                    if let melin_journal::JournalEvent::Checkpoint {
                        #[cfg(feature = "hash-chain")]
                        chain_hash,
                        ..
                    } = &slot.event
                    {
                        #[cfg(feature = "hash-chain")]
                        if slot.sequence != 0 {
                            self.verify_primary_checkpoint(chain_hash, slot.sequence)?;
                        }
                        continue;
                    }
                    let seq = if slot.sequence != 0 {
                        self.writer.set_next_sequence(slot.sequence + 1);
                        slot.sequence
                    } else {
                        self.writer.allocate_sequence()
                    };
                    self.writer
                        .encode_event(
                            seq,
                            slot.timestamp_ns,
                            &slot.event,
                            slot.key_hash,
                            slot.request_seq,
                        )
                        .map_err(|e| {
                            JournalError::Io(std::io::Error::other(format!(
                                "journal encode (run_sync, seq {seq}): {e}"
                            )))
                        })?;
                    let journal_slice = self.writer.last_user_entry_replication_slice();
                    Self::record_slot_for_replication(&mut self.repl, journal_slice);
                }
                pending += consumed;
                if first_write_ts.is_none() && consumed > 0 {
                    first_write_ts = Some(Instant::now());
                }

                #[cfg(feature = "latency-trace")]
                batch_rec.record_elapsed(batch_start, trace_ts());
            }

            // Sync when: we have data AND (batch full OR delay expired OR no delay).
            if pending > 0 {
                let should_sync = pending >= self.max_batch
                    || delay.is_zero()
                    || first_write_ts.is_some_and(|ts| ts.elapsed() >= delay);

                if should_sync {
                    // Publish the accumulated `InputBatch` frame to the
                    // replication rings BEFORE the flush or discard below
                    // clears the buffer. The frame was built up alongside
                    // the journal-codec writes via
                    // `record_slot_for_replication` in the encode loop;
                    // it's already wire-ready except for the back-fill
                    // inside `publish_input_batch_to_rings`. No-op in
                    // standalone mode (no producers) or when no slots
                    // were appended (e.g., a sync that only flushed
                    // checkpoint metadata).
                    //
                    // Runs unconditionally: under `no-persist` the
                    // replication path must still run, otherwise the
                    // response stage's replication-cursor gate deadlocks.
                    if self.repl.any_producer() {
                        let end_seq = self.writer.next_sequence() - 1;
                        Self::publish_input_batch_to_rings(&mut self.repl, end_seq);
                    }

                    // Persist mode: pwrite+O_DIRECT; no-persist mode:
                    // drop the buffer. `no-persist` means "skip the write
                    // syscall," not "skip everything that follows."
                    #[cfg(not(feature = "no-persist"))]
                    {
                        // Fatal: journal I/O failure means we can't
                        // guarantee durability. Surface the error so the
                        // pipeline shuts down rather than spinning forever
                        // on a broken disk (e.g., ENOSPC).
                        self.writer.flush_batch_sync().map_err(|e| {
                            JournalError::Io(std::io::Error::other(format!(
                                "journal flush_batch_sync: {e}"
                            )))
                        })?;
                    }
                    #[cfg(feature = "no-persist")]
                    self.writer.discard_batch_buf();

                    self.consumer.commit(pending);
                    self.publish_chain_hash();

                    pending = 0;
                    first_write_ts = None;
                }
            } else {
                idle_count += 1;
                // Periodically flush utilization counters so the health
                // endpoint has a reasonably fresh view without adding
                // atomic stores on the busy path.
                if idle_count.is_multiple_of(1024) {
                    self.utilization.busy.store(busy_count, Ordering::Relaxed);
                    self.utilization.idle.store(idle_count, Ordering::Relaxed);
                }
                idle_wait(&mut idle_spins, self.busy_spin);
            }

            if saw_shutdown {
                // Sentinel — by FIFO, every slot the receiver published
                // before it has now been consumed. Sync any encoded-but-
                // not-yet-flushed events so the persist-before-ack
                // boundary holds for the final batch, then exit.
                if pending > 0 {
                    if self.repl.any_producer() {
                        let end_seq = self.writer.next_sequence() - 1;
                        Self::publish_input_batch_to_rings(&mut self.repl, end_seq);
                    }
                    #[cfg(not(feature = "no-persist"))]
                    if let Err(e) = self.writer.flush_batch_sync() {
                        tracing::error!(error = %e, "journal sync error on sentinel exit");
                    }
                    #[cfg(feature = "no-persist")]
                    self.writer.discard_batch_buf();
                    self.consumer.commit(pending);
                    self.publish_chain_hash();
                }
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("journal", busy_count, idle_count);
                return Ok(self.writer);
            }
        }
    }

    /// Append a slot to the in-progress `InputBatch` buffer for replication.
    /// Lazily initializes the buffer header on the first slot of each batch.
    /// Append the just-encoded journal entry's bytes to the InputBatch
    /// buffer. `journal_slice` comes from
    /// [`JournalWriter::last_user_entry_replication_slice`] and is laid
    /// out exactly as the on-the-wire slot — the journal codec's frame
    /// minus its 2-byte magic and 4-byte CRC. No re-encode on the
    /// hot path; the hand-off is a single `extend_from_slice`.
    ///
    /// No-op when no replication producers are active (standalone mode).
    #[inline]
    fn record_slot_for_replication(repl: &mut ReplicationState, journal_slice: &[u8]) {
        if !repl.any_producer() {
            return;
        }
        if repl.input_batch_count == 0 {
            init_input_batch(&mut repl.input_batch_buf);
        }
        repl.input_batch_buf.extend_from_slice(journal_slice);
        repl.input_batch_count = repl
            .input_batch_count
            .checked_add(1)
            .expect("InputBatch slot count overflowed u16 in a single fsync batch");
    }

    /// Finalize the accumulated `InputBatch` buffer (back-fill length, type,
    /// count) and publish it to all active replication rings, then reset
    /// for the next fsync batch. No-op when no slots were appended this
    /// batch (e.g., a fsync that only flushed checkpoint metadata).
    fn publish_input_batch_to_rings(repl: &mut ReplicationState, end_seq: u64) {
        if repl.input_batch_count == 0 {
            return;
        }
        finalize_input_batch(&mut repl.input_batch_buf, repl.input_batch_count);
        Self::publish_to_replication_rings(
            &mut repl.producers,
            &repl.evict,
            &repl.active,
            &repl.input_batch_buf,
            end_seq,
        );
        // Reset for the next batch. Drop content but keep capacity so
        // subsequent batches don't reallocate.
        repl.input_batch_buf.clear();
        repl.input_batch_count = 0;
    }

    /// Publish a batch to all active replication rings. Fully non-blocking:
    /// a single `try_publish` per ring, no spinning. If a ring is full,
    /// the replica is evicted immediately — a skipped batch would create
    /// a sequence gap in the replica's journal that can only be repaired
    /// by reconnection and catch-up from journal files.
    ///
    /// This ensures a slow replica NEVER stalls the pipeline. The healthy
    /// replica's ring gets data at full speed.
    ///
    /// Free function to avoid borrow conflicts with `self.writer`.
    fn publish_to_replication_rings(
        producers: &mut [Option<ReplicationProducer>; 2],
        evict_flags: &[Arc<AtomicBool>; 2],
        active_flags: &[Arc<AtomicBool>; 2],
        bytes: &[u8],
        end_seq: u64,
    ) {
        for i in 0..2 {
            if let Some(ref mut producer) = producers[i] {
                if !active_flags[i].load(Ordering::Relaxed) {
                    continue;
                }
                if evict_flags[i].load(Ordering::Relaxed) {
                    continue;
                }
                if producer.try_publish(bytes, end_seq).is_err() {
                    // Ring full — evict immediately. A skipped batch creates
                    // a sequence gap in the replica's journal that can only
                    // be repaired by reconnection + catch-up from journal
                    // files. Continuing to publish would deliver subsequent
                    // batches with a hole, corrupting the replica's state.
                    evict_flags[i].store(true, Ordering::Release);
                    tracing::warn!(
                        ring = i,
                        end_seq,
                        "replication ring full — evicting replica (would create sequence gap)"
                    );
                }
            }
        }
    }

    /// Publish the current BLAKE3 chain hash to the shadow snapshot stage
    /// via the SeqLock. Called once per fsync batch (cold path). No-op when
    /// shadow snapshots are disabled or hash-chain is not active.
    /// Publish the post-fsync writer state to optional readers:
    /// `chain_hash` (for shadow snapshots and replica handshakes) and
    /// `last_seq` (highest journal sequence durably persisted, used by the
    /// replica orchestrator on reconnect handshakes). Both `Option`s are
    /// independent; either can be set or unset. Called once per fsync
    /// batch (cold path); each `if let Some` is a single branch on a small
    /// struct field.
    #[inline]
    fn publish_chain_hash(&self) {
        if let Some(ref lock) = self.chain_hash
            && let Some(hash) = self.writer.chain_hash()
        {
            lock.store(hash);
        }
        if let Some(ref atom) = self.last_seq {
            // `next_sequence` is the sequence about to be allocated; the
            // highest written is one less. Saturating prevents underflow for
            // a fresh writer at sequence 0.
            atom.store(
                self.writer.next_sequence().saturating_sub(1),
                Ordering::Release,
            );
        }
    }

    /// Verify the replica's chain hash against a checkpoint from the
    /// primary. Called when the JournalStage encounters a Checkpoint
    /// event with a pre-assigned sequence (replica mode). The replica's
    /// writer has just auto-emitted its own checkpoint at the same
    /// position, so the chain hashes must match.
    ///
    /// Returns `Err` on mismatch — the caller should shut down the
    /// pipeline to prevent silent divergence.
    ///
    /// No-op when the `hash-chain` feature is disabled (checkpoints
    /// don't exist without it).
    #[cfg(feature = "hash-chain")]
    fn verify_primary_checkpoint(
        &self,
        primary_hash: &[u8; 32],
        sequence: u64,
    ) -> Result<(), JournalError> {
        if let Some(local_hash) = self.writer.chain_hash()
            && local_hash != *primary_hash
        {
            return Err(JournalError::Io(std::io::Error::other(format!(
                "divergence detected at checkpoint seq {sequence}: \
                 replica hash {local_hash:02x?} != primary hash {primary_hash:02x?}"
            ))));
        }
        Ok(())
    }

    /// Drain any remaining entries from the ring buffer on shutdown.
    fn drain_remaining(&mut self, batch: &mut [InputSlot<E>]) {
        loop {
            let count = self.consumer.read_batch(batch, MAX_JOURNAL_BATCH);
            if count == 0 {
                break;
            }
            #[cfg(not(feature = "no-persist"))]
            {
                for slot in &batch[..count] {
                    if slot.event.is_query() || slot.event.is_shutdown() {
                        // Sentinel is never persisted (codec rejects it).
                        // Reaching this path means the shutdown flag fired
                        // before the sentinel was consumed — skip it.
                        continue;
                    }
                    if let melin_journal::JournalEvent::Checkpoint {
                        #[cfg(feature = "hash-chain")]
                        chain_hash,
                        ..
                    } = &slot.event
                    {
                        #[cfg(feature = "hash-chain")]
                        if slot.sequence != 0
                            && let Err(e) =
                                self.verify_primary_checkpoint(chain_hash, slot.sequence)
                        {
                            tracing::error!(error = %e, "divergence on drain");
                        }
                        continue;
                    }
                    let seq = if slot.sequence != 0 {
                        self.writer.set_next_sequence(slot.sequence + 1);
                        slot.sequence
                    } else {
                        self.writer.allocate_sequence()
                    };
                    if let Err(e) = self.writer.encode_event(
                        seq,
                        slot.timestamp_ns,
                        &slot.event,
                        slot.key_hash,
                        slot.request_seq,
                    ) {
                        tracing::error!(error = %e, "journal encode error on drain");
                        continue;
                    }
                    let journal_slice = self.writer.last_user_entry_replication_slice();
                    Self::record_slot_for_replication(&mut self.repl, journal_slice);
                }

                // Publish accumulated InputBatch frame to replication rings
                // before flush, mirroring the steady-state sync path.
                if self.repl.any_producer() {
                    let end_seq = self.writer.next_sequence() - 1;
                    Self::publish_input_batch_to_rings(&mut self.repl, end_seq);
                }

                if let Err(e) = self.writer.flush_batch_sync() {
                    tracing::error!(error = %e, "journal sync error on drain");
                }
            }
            self.consumer.commit(count);
        }
    }

    /// Overlapped io_uring journal loop: submits `Write` asynchronously and
    /// accumulates the next batch in a second buffer while the NVMe write is
    /// in flight. Doubles effective throughput when journal I/O is the bottleneck.
    ///
    /// Cursor only advances after the CQE confirms durability — the
    /// persist-before-ack guarantee is preserved.
    fn run_uring(
        mut self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<melin_journal::JournalWriter<E>, JournalError> {
        use io_uring::{IoUring, opcode, types};
        use std::time::Instant;

        // SINGLE_ISSUER: only one thread submits SQEs — lets the kernel skip
        // internal locking on the SQ.
        //
        // COOP_TASKRUN is deliberately NOT used: it defers CQE delivery to
        // io_uring_enter() calls, requiring an extra syscall (~200ns) at
        // every reap point. Without it, CQEs are posted directly to the
        // shared CQ ring in interrupt context (on core 0 per IRQ affinity),
        // and the journal thread reads them via the memory-mapped CQ with
        // zero syscall overhead.
        let mut ring: IoUring = IoUring::builder()
            .setup_single_issuer()
            .build(4)
            .map_err(|e| JournalError::Io(std::io::Error::other(format!("io_uring init: {e}"))))?;

        // Register the journal fd so the kernel skips fget/fput (fd table
        // lookup + atomic refcount) on every SQE. Use types::Fixed(0) in
        // SQEs instead of types::Fd(raw_fd).
        let raw_fd = self.writer.fd();
        let rw_flags = self.writer.io_uring_rw_flags();
        ring.submitter().register_files(&[raw_fd]).map_err(|e| {
            JournalError::Io(std::io::Error::other(format!(
                "io_uring register_files: {e}"
            )))
        })?;

        // Pin io-wq worker threads to core 0 (OS/IRQ core). Without this,
        // io-wq workers inherit the journal thread's CPU affinity (core 1)
        // and contend with the busy-spinning journal thread. With nohz_full,
        // timer ticks are suppressed on core 1, so the worker can be starved
        // for up to 4ms (HZ=250) waiting for preemption — causing ~6ms p99.9
        // tail latency spikes. Core 0 is non-isolated and always has ticks.
        {
            let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
            unsafe { libc::CPU_SET(0, &mut cpuset) };
            ring.submitter().register_iowq_aff(&cpuset).map_err(|e| {
                JournalError::Io(std::io::Error::other(format!(
                    "io_uring register_iowq_aff: {e}"
                )))
            })?;
        }

        let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];
        let delay = self.group_commit_delay;
        let mut idle_spins: u32 = 0;
        let mut pending: usize = 0;
        let mut first_write_ts: Option<Instant> = None;

        // In-flight state: the batch being written and the sequence to commit
        // when the CQE arrives. `inflight_seq` is the consumer's `next_read`
        // at the time of submission — committing it advances the cursor to
        // exactly the events covered by the durable write.
        let mut inflight: Option<(melin_journal::AsyncWriteBatch, u64)> = None;

        let mut busy_count: u64 = 0;
        let mut idle_count: u64 = 0;

        loop {
            // --- Check shutdown ---
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                self.reap_inflight_on_shutdown(&mut ring, &mut inflight)?;
                // Flush any pending buffered data through the same async path
                // as steady-state — submit + reap one CQE — instead of falling
                // back to a synchronous pwrite. Keeps the production write path
                // symmetric and removes the last sync-flush call from the
                // hot/critical lifecycle.
                if pending > 0 {
                    if let Some(async_batch) = self.writer.take_batch_for_async_write()? {
                        let seq = self.consumer.next_read();
                        let len = async_batch.len;
                        let sqe = opcode::Write::new(
                            types::Fixed(0),
                            async_batch.buf.as_ptr(),
                            len as u32,
                        )
                        .offset(async_batch.offset)
                        .rw_flags(rw_flags)
                        .build()
                        .user_data(1);
                        unsafe {
                            ring.submission().push(&sqe).expect("SQ full");
                        }
                        ring.submit().expect("io_uring submit failed");
                        self.wait_for_cqe(&mut ring, len)?;
                        self.consumer.set_progress(seq);
                        self.publish_chain_hash();
                        self.writer.confirm_async_write(async_batch);
                    } else {
                        // Buffer was empty (read-only queries only) — just commit.
                        self.consumer.commit(pending);
                    }
                }
                self.drain_remaining(&mut batch);
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("journal", busy_count, idle_count);
                return Ok(self.writer);
            }

            // --- Reap CQE from previous in-flight write (non-blocking) ---
            // CQEs are posted directly to the shared CQ ring in interrupt
            // context — no syscall needed to make them visible.
            if let Some((ref batch_data, seq)) = inflight
                && let Some(cqe) = ring.completion().next()
            {
                let result = cqe.result();
                if result < 0 {
                    return Err(JournalError::Io(std::io::Error::other(format!(
                        "io_uring journal write failed (errno {})",
                        -result
                    ))));
                } else if (result as usize) != batch_data.len {
                    return Err(JournalError::Io(std::io::Error::other(format!(
                        "io_uring journal short write ({} of {} bytes)",
                        result, batch_data.len
                    ))));
                }
                // Advance cursor: these events are now durable.
                self.consumer.set_progress(seq);
                self.publish_chain_hash();
                let completed = inflight.take().expect("checked above");
                self.writer.confirm_async_write(completed.0);
            }

            // --- Read events from disruptor ---
            let remaining = MAX_JOURNAL_BATCH.saturating_sub(pending);
            let count = if remaining > 0 {
                self.consumer.read_batch(&mut batch, remaining)
            } else {
                0
            };

            // `saw_shutdown` becomes true the moment we observe a sentinel
            // slot in the input ring. Set on the inner loop, checked on the
            // outer loop to break out into the shutdown-flush path. We
            // process every slot up to (and excluding) the sentinel — the
            // disruptor's FIFO order guarantees we've consumed everything
            // the receiver published before the sentinel.
            let mut saw_shutdown = false;

            if count > 0 {
                idle_spins = 0;
                busy_count += 1;

                for slot in &batch[..count] {
                    if slot.event.is_shutdown() {
                        saw_shutdown = true;
                        break;
                    }
                    if slot.event.is_query() {
                        continue;
                    }
                    if let melin_journal::JournalEvent::Checkpoint {
                        #[cfg(feature = "hash-chain")]
                        chain_hash,
                        ..
                    } = &slot.event
                    {
                        #[cfg(feature = "hash-chain")]
                        if slot.sequence != 0 {
                            self.verify_primary_checkpoint(chain_hash, slot.sequence)?;
                        }
                        continue;
                    }
                    let seq = if slot.sequence != 0 {
                        self.writer.set_next_sequence(slot.sequence + 1);
                        slot.sequence
                    } else {
                        self.writer.allocate_sequence()
                    };
                    self.writer
                        .encode_event(
                            seq,
                            slot.timestamp_ns,
                            &slot.event,
                            slot.key_hash,
                            slot.request_seq,
                        )
                        .map_err(|e| {
                            JournalError::Io(std::io::Error::other(format!(
                                "journal encode (run_uring, seq {seq}): {e}"
                            )))
                        })?;
                    let journal_slice = self.writer.last_user_entry_replication_slice();
                    Self::record_slot_for_replication(&mut self.repl, journal_slice);
                }
                pending += count;
                if first_write_ts.is_none() {
                    first_write_ts = Some(Instant::now());
                }
            }

            if saw_shutdown {
                // Drain anything in flight + any pending batch, then exit.
                // Same shape as the shutdown-flag path above — we just got
                // here via the sentinel rather than the flag.
                self.reap_inflight_on_shutdown(&mut ring, &mut inflight)?;
                if pending > 0 {
                    self.writer.flush_batch_sync()?;
                    self.consumer.commit(pending);
                }
                self.drain_remaining(&mut batch);
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("journal", busy_count, idle_count);
                return Ok(self.writer);
            }

            // --- Eagerly reap CQE after encoding ---
            // The non-blocking check at the top of the loop may have missed
            // a CQE that arrived while we were encoding events. Reap it now
            // so the cursor advances sooner and the slot frees up for
            // immediate submission.
            if let Some((ref batch_data, seq)) = inflight
                && let Some(cqe) = ring.completion().next()
            {
                let result = cqe.result();
                if result < 0 {
                    return Err(JournalError::Io(std::io::Error::other(format!(
                        "io_uring journal write failed (errno {})",
                        -result
                    ))));
                } else if (result as usize) != batch_data.len {
                    return Err(JournalError::Io(std::io::Error::other(format!(
                        "io_uring journal short write ({} of {} bytes)",
                        result, batch_data.len
                    ))));
                }
                self.consumer.set_progress(seq);
                self.publish_chain_hash();
                let completed = inflight.take().expect("checked above");
                self.writer.confirm_async_write(completed.0);
            }

            // --- Decide whether to submit ---
            if pending > 0 {
                // With overlapping: only submit when either (a) there's no
                // in-flight write (slot is free), or (b) batch is full
                // (backpressure — must drain before accumulating more).
                // When a write IS in-flight and the batch isn't full, we
                // continue accumulating — the CQE reap at the top of the
                // loop will free the slot, and the NEXT iteration submits.
                let slot_free = inflight.is_none();
                let batch_full = pending >= self.max_batch;
                let delay_expired =
                    delay.is_zero() || first_write_ts.is_some_and(|ts| ts.elapsed() >= delay);

                let should_submit = (slot_free && delay_expired) || batch_full;

                if should_submit {
                    // If a write is still in-flight, block until it completes
                    // (backpressure — both buffers full).
                    if let Some((batch_data, seq)) = inflight.take() {
                        self.wait_for_cqe(&mut ring, batch_data.len)?;
                        self.consumer.set_progress(seq);
                        self.publish_chain_hash();
                        self.writer.confirm_async_write(batch_data);
                    }

                    // Publish the accumulated InputBatch frame to
                    // replication rings BEFORE take_batch_for_async_write
                    // (which swaps the journal-codec buffer). The InputBatch
                    // buffer is independent of that swap, but publish at the
                    // same boundary so the ring's `end_sequence` matches the
                    // batch about to be submitted.
                    if self.repl.any_producer() {
                        let end_seq = self.writer.next_sequence() - 1;
                        Self::publish_input_batch_to_rings(&mut self.repl, end_seq);
                    }

                    // Take the batch buffer and submit async write.
                    match self.writer.take_batch_for_async_write() {
                        Ok(Some(async_batch)) => {
                            let seq = self.consumer.next_read();
                            let sqe = opcode::Write::new(
                                types::Fixed(0),
                                async_batch.buf.as_ptr(),
                                async_batch.len as u32,
                            )
                            .offset(async_batch.offset)
                            .rw_flags(rw_flags)
                            .build()
                            .user_data(1);

                            unsafe {
                                ring.submission().push(&sqe).expect("SQ full");
                            }
                            ring.submit().expect("io_uring submit failed");

                            inflight = Some((async_batch, seq));
                        }
                        Ok(None) => {
                            // Buffer was empty (all read-only queries), just commit.
                            self.consumer.commit(pending);
                        }
                        Err(e) => {
                            return Err(JournalError::Io(std::io::Error::other(format!(
                                "journal take_batch_for_async_write: {e}"
                            ))));
                        }
                    }
                    pending = 0;
                    first_write_ts = None;
                }
            } else {
                idle_count += 1;
                if idle_count.is_multiple_of(1024) {
                    self.utilization.busy.store(busy_count, Ordering::Relaxed);
                    self.utilization.idle.store(idle_count, Ordering::Relaxed);
                }
                idle_wait(&mut idle_spins, self.busy_spin);
            }
        }
    }

    /// Busy-poll until the in-flight io_uring CQE arrives.
    ///
    /// Pure userspace spin on the memory-mapped CQ ring — no syscalls.
    /// CQEs are posted by the kernel in interrupt context (on core 0 per
    /// IRQ affinity) and become visible here via the shared memory mapping.
    /// The journal thread is pinned to a dedicated core, so busy-polling
    /// is appropriate and avoids kernel sleep/wake jitter entirely.
    /// Drain a single in-flight async write at shutdown: wait for its
    /// CQE, advance the consumer cursor past those events, publish the
    /// chain hash, and hand the buffer back to the writer. No-op if no
    /// write is in flight. Used by both shutdown paths in `run_uring`
    /// (the shutdown-flag check at the loop top, and the sentinel
    /// observed mid-batch).
    fn reap_inflight_on_shutdown(
        &mut self,
        ring: &mut io_uring::IoUring,
        inflight: &mut Option<(melin_journal::AsyncWriteBatch, u64)>,
    ) -> Result<(), JournalError> {
        if let Some((batch_data, seq)) = inflight.take() {
            self.wait_for_cqe(ring, batch_data.len)?;
            self.consumer.set_progress(seq);
            self.publish_chain_hash();
            self.writer.confirm_async_write(batch_data);
        }
        Ok(())
    }

    fn wait_for_cqe(
        &self,
        ring: &mut io_uring::IoUring,
        expected_len: usize,
    ) -> Result<(), JournalError> {
        loop {
            if let Some(cqe) = ring.completion().next() {
                let result = cqe.result();
                if result < 0 {
                    return Err(JournalError::Io(std::io::Error::other(format!(
                        "io_uring journal write failed (errno {})",
                        -result
                    ))));
                } else if (result as usize) != expected_len {
                    return Err(JournalError::Io(std::io::Error::other(format!(
                        "io_uring journal short write ({} of {expected_len} bytes)",
                        result,
                    ))));
                }
                return Ok(());
            }
            std::hint::spin_loop();
        }
    }
}

/// Matching stage: consumes from the input disruptor (in parallel with
/// the journal stage), executes commands on the Exchange, and publishes
/// responses to the output SPSC.
///
/// Runs on a dedicated OS thread. Does NOT wait for journal sync —
/// the persist-before-ack check happens in the response stage.
pub struct MatchingStage<A: Application> {
    app: A,
    consumer: ring::Consumer<InputSlot<A::Event>>,
    output: ring::Producer<OutputSlot<A::Report, A::QueryResponse>>,
    /// Monotonically increasing count of events processed. Relaxed ordering
    /// is sufficient — this is a diagnostic counter, not a synchronization
    /// primitive. One `fetch_add(1, Relaxed)` per event (~1ns).
    events_processed: Arc<AtomicU64>,
    /// Journal cursor for reading the current durable sequence. Used by
    /// `QueryStats` to report the journal position without adding any
    /// cross-thread synchronization on the hot path.
    journal_cursor: Arc<Sequence>,
    /// Active connection count, shared with the server accept loop.
    /// Read only when processing `QueryStats` (once per second at most).
    active_connections: Arc<AtomicU64>,
    /// When `Some`, replication is enabled. One Relaxed load per event
    /// (~1ns). `0` = no replicas connected → reject all mutations.
    /// `None` = standalone mode → no halt check.
    replicas_connected: Option<Arc<AtomicU32>>,
    /// When true, never yield — spin indefinitely. See [`idle_wait`].
    busy_spin: bool,
    /// Shared busy/idle counters for health endpoint monitoring.
    utilization: Arc<StageUtilization>,
    /// Highest event timestamp the scheduler has drained against. Each event
    /// (including non-Tick events) advances this whenever its `slot.timestamp_ns`
    /// is newer, so the scheduler fires due tasks at every-event resolution under
    /// load. Tick events become a quiet-period safety net rather than the only
    /// thing that moves time forward. Derived state — not snapshotted; recovery
    /// catches up at the first replayed event with a non-zero timestamp.
    last_drain_ns: u64,
}

impl<A: Application> MatchingStage<A> {
    /// Create a new matching stage.
    pub fn new(
        app: A,
        consumer: ring::Consumer<InputSlot<A::Event>>,
        output: ring::Producer<OutputSlot<A::Report, A::QueryResponse>>,
        events_processed: Arc<AtomicU64>,
        journal_cursor: Arc<Sequence>,
        active_connections: Arc<AtomicU64>,
        replicas_connected: Option<Arc<AtomicU32>>,
        busy_spin: bool,
    ) -> Self {
        Self {
            app,
            consumer,
            output,
            events_processed,
            journal_cursor,
            active_connections,
            replicas_connected,
            busy_spin,
            utilization: Arc::new(StageUtilization::new()),
            last_drain_ns: 0,
        }
    }

    /// Shared utilization counters for health endpoint monitoring.
    pub fn utilization(&self) -> Arc<StageUtilization> {
        Arc::clone(&self.utilization)
    }

    /// Returns true if trading is halted due to all replicas disconnected.
    /// Always false in standalone mode (replicas_connected is None).
    fn is_halted(&self) -> bool {
        self.replicas_connected
            .as_ref()
            .is_some_and(|count| count.load(Ordering::Relaxed) == 0)
    }

    /// Run the matching stage loop. Blocks until shutdown.
    ///
    /// Uses small-batch consumption from the disruptor to amortize the
    /// atomic progress-store: one `Release` store per batch instead of
    /// per event. Events are still processed sequentially — only the
    /// disruptor I/O is batched.
    ///
    /// Returns the application on shutdown for potential snapshot saving.
    pub fn run(mut self, shutdown: &std::sync::atomic::AtomicBool) -> A {
        // Pre-allocated report buffer, reused across commands.
        // Pre-allocate with generous capacity. A market order sweeping many
        // price levels can produce one Fill per level + Placed/Cancelled. 256
        // avoids mid-hot-path reallocation for all but extreme scenarios.
        let mut reports: Vec<A::Report> = Vec::with_capacity(256);
        // Spin count for adaptive wait: spin first (fast wakeup), then yield
        // to the OS scheduler (prevents the kernel from aggressively preempting
        // this thread during busy periods). 1000 spins ≈ 1µs at ~1ns/spin,
        // which is well under the inter-event arrival time at peak throughput.
        let mut idle_spins: u32 = 0;
        // Thread-local events counter — plain u64 increment (~0.3ns) instead
        // of atomic fetch_add (~5-8ns). Flushed to the shared Arc<AtomicU64>
        // once per batch and on shutdown.
        let mut local_events: u64 = 0;

        let mut busy_count: u64 = 0;
        let mut idle_count: u64 = 0;

        // Histograms via the global stats registry — see the journal
        // stage for the rationale. The registry owns the histograms;
        // the server prints them via `trace::print_report_all` once
        // all stage threads have joined.
        #[cfg(feature = "latency-trace")]
        let mut wakeup_rec = melin_journal::trace::register_stage(
            "matching: disruptor wakeup (publish → matching consume)",
        );
        #[cfg(feature = "latency-trace")]
        let mut execute_rec =
            melin_journal::trace::register_stage("matching: execute (process_event)");

        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                // Drain remaining entries so every journaled event gets a response.
                self.drain_remaining(&mut reports);
                // Flush the thread-local counter to the shared atomic.
                self.events_processed.store(local_events, Ordering::Relaxed);
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("matching", busy_count, idle_count);
                return self.app;
            }

            let batch_start = self.consumer.next_read();
            // Borrow up to MAX_MATCHING_BATCH ready slots in place from
            // the input ring instead of copying them into a 64×104 B
            // stack buffer first. The two slices together form the
            // logical batch (the second is non-empty only when the
            // batch crosses the ring's wrap point). `commit_consumed`
            // is called below once iteration finishes, advancing the
            // consumer cursor and releasing those slots back to the
            // producer for backpressure.
            let (slots_a, slots_b) = self.consumer.peek_batch(MAX_MATCHING_BATCH);
            let count = slots_a.len() + slots_b.len();
            if count == 0 {
                idle_count += 1;
                if idle_count.is_multiple_of(1024) {
                    self.utilization.busy.store(busy_count, Ordering::Relaxed);
                    self.utilization.idle.store(idle_count, Ordering::Relaxed);
                }
                idle_wait(&mut idle_spins, self.busy_spin);
                continue;
            }
            idle_spins = 0;

            // Build ApplyCtx once per batch — the counters are advisory
            // (stats queries, health endpoint) so batch-stale values are
            // fine. `now_ns` and `key_hash` are overwritten per-event
            // below (the latter from the slot's authenticated identity
            // so self-introspecting queries can read it from `ctx`).
            // Three Relaxed loads per batch instead of per event.
            let mut ctx = ApplyCtx {
                now_ns: 0,
                journal_sequence: self.journal_cursor.get().load(Ordering::Relaxed),
                active_connections: self.active_connections.load(Ordering::Relaxed),
                events_processed: local_events,
                key_hash: 0,
            };

            let mut saw_shutdown = false;

            // Halt status is constant for the duration of one disruptor
            // batch (replica counts only change between batches in
            // practice). One Relaxed load per batch, hoisted out of the
            // per-event branch.
            let halted = self
                .replicas_connected
                .as_ref()
                .is_some_and(|count| count.load(Ordering::Relaxed) == 0);

            // Open a single output batch for the entire disruptor batch:
            // all OutputSlots produced below share one Release store on
            // the output cursor at `out_batch.commit()`, instead of one
            // Release per slot. This is the LMAX disruptor batching
            // pattern — at saturation it cuts cache-line bouncing on the
            // matching → response cursor by up to MAX_MATCHING_BATCH×.
            let mut out_batch = self.output.batch();

            for (i, slot) in slots_a.iter().chain(slots_b.iter()).enumerate() {
                if slot.event.is_shutdown() {
                    // Sentinel — every event the producer published before
                    // this slot has already been consumed (FIFO). Exit
                    // cleanly without applying the sentinel itself.
                    saw_shutdown = true;
                    break;
                }
                let input_seq = batch_start + i as u64;
                busy_count += 1;

                #[cfg(feature = "latency-trace")]
                wakeup_rec.record_elapsed(slot.publish_ts, trace_ts());

                reports.clear();
                let mut query_report: Option<A::QueryResponse> = None;

                #[cfg(feature = "latency-trace")]
                let exec_start = trace_ts();

                ctx.events_processed = local_events;
                ctx.key_hash = slot.key_hash;
                local_events += 1;

                // Halt check first: reject before advancing any HWMs so
                // the client can safely retry the same seq after
                // reconnect. Read-only queries bypass both halt and
                // dedup — they never mutate durable state, so returning
                // the current snapshot during a halt is safe (and
                // actually useful for operators monitoring the outage).
                let is_query = slot.event.is_query();
                if !is_query && halted {
                    // Only app events produce client-facing rejections;
                    // transport variants (Tick, GenesisHash, Checkpoint)
                    // have no client to reject to, so they silently
                    // skip during halt.
                    if let melin_journal::JournalEvent::App(ref e) = slot.event {
                        reports.push(A::build_reject(e, RejectReason::ReplicaDisconnected));
                    }
                } else if !is_query && !self.app.check_request_seq(slot.key_hash, slot.request_seq)
                {
                    // Duplicate request — produce a Rejected report for
                    // the app event; transport variants don't go through
                    // dedup (they use `key_hash == 0` which the app
                    // exempts).
                    if let melin_journal::JournalEvent::App(ref e) = slot.event {
                        reports.push(A::build_reject(e, RejectReason::DuplicateRequest));
                    }
                } else {
                    // Inlined `process_event` so the run loop only borrows
                    // disjoint fields (`self.app`, `self.last_drain_ns`),
                    // freeing `self.output` for the in-flight `out_batch`.
                    if slot.timestamp_ns > self.last_drain_ns {
                        self.last_drain_ns = slot.timestamp_ns;
                        self.app.tick(slot.timestamp_ns, &mut reports);
                    }
                    match slot.event {
                        melin_journal::JournalEvent::App(event) => {
                            let event_ctx = ApplyCtx {
                                now_ns: slot.timestamp_ns,
                                ..ctx
                            };
                            query_report = self.app.apply(event, &event_ctx, &mut reports);
                        }
                        melin_journal::JournalEvent::Tick { now_ns } => {
                            self.app.tick(now_ns, &mut reports);
                        }
                        melin_journal::JournalEvent::GenesisHash { .. }
                        | melin_journal::JournalEvent::Checkpoint { .. } => {}
                        melin_journal::JournalEvent::Shutdown => {}
                    }
                }

                #[cfg(feature = "latency-trace")]
                {
                    let exec_end = trace_ts();
                    let elapsed_ns = melin_journal::trace::trace_elapsed_ns(exec_start, exec_end);
                    // Outlier log: any execute > 1 ms is well into pathological
                    // territory for a path whose p50 is ~200 ns. Capture the
                    // event variant + correlation IDs so we can pin down what
                    // class of work triggered the stall. `AppEvent` doesn't
                    // require `Debug`, so we discriminate the variant only —
                    // enough to tell tick from app event.
                    if elapsed_ns > 1_000_000 {
                        let event_kind: &'static str = match &slot.event {
                            melin_journal::JournalEvent::App(_) => "app",
                            melin_journal::JournalEvent::Tick { .. } => "tick",
                            melin_journal::JournalEvent::GenesisHash { .. } => "genesis_hash",
                            melin_journal::JournalEvent::Checkpoint { .. } => "checkpoint",
                            melin_journal::JournalEvent::Shutdown => "shutdown",
                        };
                        tracing::warn!(
                            elapsed_us = elapsed_ns / 1000,
                            event_kind,
                            connection_id = slot.connection_id,
                            request_seq = slot.request_seq,
                            input_seq,
                            "matching execute outlier"
                        );
                    }
                    execute_rec.record_ns(elapsed_ns);
                }

                #[allow(clippy::let_unit_value)] // ZST when latency-trace is disabled
                let match_complete_ts = trace_ts();

                // Push execution reports into the output batch.
                // All output slots for this request carry the same
                // input_seq so the response stage can gate on journal
                // completion. Fan-out reports (fills, acks) come from
                // the scratch vec; query responses (stats, position)
                // are returned directly by the app and pushed here
                // without ever entering the vec.
                //
                // The terminating wire `BatchEnd` is signalled via
                // `is_last_in_request` on the final slot — saving one
                // ring slot per event when there is at least one
                // report or query response to mark. When the event
                // produces no payload at all, fall back to a single
                // `BatchEnd`-payload slot.
                let report_count = reports.len();
                let last_is_query = query_report.is_some();
                if report_count == 0 && !last_is_query {
                    out_batch.push_with(|s| {
                        *s = OutputSlot {
                            connection_id: slot.connection_id,
                            input_seq,
                            payload: OutputPayload::BatchEnd,
                            match_complete_ts,
                            recv_ts: slot.recv_ts,
                            is_last_in_request: true,
                        };
                    });
                } else {
                    for (j, report) in reports.iter().enumerate() {
                        let is_last = j + 1 == report_count && !last_is_query;
                        out_batch.push_with(|s| {
                            *s = OutputSlot {
                                connection_id: slot.connection_id,
                                input_seq,
                                payload: OutputPayload::Report(*report),
                                match_complete_ts,
                                recv_ts: slot.recv_ts,
                                is_last_in_request: is_last,
                            };
                        });
                    }
                    if let Some(qr) = query_report {
                        out_batch.push_with(|s| {
                            *s = OutputSlot {
                                connection_id: slot.connection_id,
                                input_seq,
                                payload: OutputPayload::QueryResponse(qr),
                                match_complete_ts,
                                recv_ts: slot.recv_ts,
                                is_last_in_request: true,
                            };
                        });
                    }
                }
            }

            // Single Release store on the output cursor for everything
            // pushed during this disruptor batch. The response stage
            // sees all the slots become visible at once.
            out_batch.commit();

            // Release the input slots back to the producer for
            // backpressure. The borrowed slices `slots_a` / `slots_b`
            // dropped at the end of the for loop above, so this
            // re-borrow on `self.consumer` is unconflicted.
            self.consumer.commit_consumed(count);

            // Flush the thread-local counter once per batch so the health
            // endpoint can observe progress. One Relaxed store per batch
            // (~1ns) is negligible compared to per-event atomic increment.
            self.events_processed.store(local_events, Ordering::Relaxed);

            if saw_shutdown {
                // events_processed already flushed above. Flush the
                // utilization counters and exit.
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("matching", busy_count, idle_count);
                return self.app;
            }
        }
    }

    /// Drain any remaining entries from the ring buffer on shutdown,
    /// processing each and publishing responses. Ensures every journaled
    /// event gets a matching response sent to the client.
    fn drain_remaining(&mut self, reports: &mut Vec<A::Report>) {
        // Shutdown path — not performance-critical. Build a single ctx
        // with zeroed counters (no health endpoint cares at this point).
        let ctx = ApplyCtx {
            now_ns: 0,
            journal_sequence: 0,
            active_connections: 0,
            events_processed: 0,
            key_hash: 0,
        };
        loop {
            let entry = self.consumer.try_consume();
            let Some((input_seq, slot)) = entry else {
                break;
            };
            // Read-only queries are meaningless during shutdown — skip
            // to avoid emitting a bare BatchEnd without a preceding
            // response.
            if slot.event.is_query() {
                continue;
            }
            reports.clear();

            // Halt check first, then dedup (same order as the main run loop).
            if self.is_halted() {
                if let melin_journal::JournalEvent::App(ref e) = slot.event {
                    reports.push(A::build_reject(e, RejectReason::ReplicaDisconnected));
                }
            } else if !self.app.check_request_seq(slot.key_hash, slot.request_seq) {
                if let melin_journal::JournalEvent::App(ref e) = slot.event {
                    reports.push(A::build_reject(e, RejectReason::DuplicateRequest));
                }
            } else {
                // Queries are already skipped above, so process_event
                // will not return a query response here.
                let query_report = self.process_event(&slot, &ctx, reports);
                debug_assert!(query_report.is_none(), "drain_remaining skips queries");
            }

            #[allow(clippy::let_unit_value)]
            let match_complete_ts = trace_ts();

            // Same is_last_in_request convention as the run loop: mark
            // the final report (or a fallback BatchEnd-payload slot) as
            // the request terminator so the response stage emits the
            // wire BatchEnd.
            let report_count = reports.len();
            if report_count == 0 {
                self.output.publish(OutputSlot {
                    connection_id: slot.connection_id,
                    input_seq,
                    payload: OutputPayload::BatchEnd,
                    match_complete_ts,
                    recv_ts: slot.recv_ts,
                    is_last_in_request: true,
                });
            } else {
                for (j, report) in reports.iter().enumerate() {
                    let is_last = j + 1 == report_count;
                    self.output.publish(OutputSlot {
                        connection_id: slot.connection_id,
                        input_seq,
                        payload: OutputPayload::Report(*report),
                        match_complete_ts,
                        recv_ts: slot.recv_ts,
                        is_last_in_request: is_last,
                    });
                }
            }
        }
    }

    /// Dispatch a single event through the [`Application`] trait.
    ///
    /// The 17-arm trading match that used to live here now collapses
    /// into a single `Application::apply` call — the trait impl on
    /// `Exchange` (see `application_impl.rs`) owns the per-variant
    /// dispatch, freeing the pipeline from knowing anything about
    /// trading semantics. `#[inline]` on `Exchange::apply` + fat LTO
    /// keep the hot path zero-cost.
    fn process_event(
        &mut self,
        slot: &InputSlot<A::Event>,
        ctx: &ApplyCtx,
        reports: &mut Vec<A::Report>,
    ) -> Option<A::QueryResponse> {
        // Hybrid scheduler clock: every event with a non-zero, monotonic
        // timestamp drives the scheduler forward. Under load this fires
        // due tasks at every-event resolution (microseconds) without
        // waiting for the next Tick. The non-monotonic guard tolerates
        // the rare multi-producer ordering race in which a slot arrives
        // with an earlier timestamp than its predecessor.
        if slot.timestamp_ns > self.last_drain_ns {
            self.last_drain_ns = slot.timestamp_ns;
            self.app.tick(slot.timestamp_ns, reports);
        }

        match slot.event {
            melin_journal::JournalEvent::App(event) => {
                // `now_ns` is the only per-event field — stamp it from
                // the slot. The remaining ctx fields were loaded once per
                // batch by the caller.
                let ctx = ApplyCtx {
                    now_ns: slot.timestamp_ns,
                    ..*ctx
                };
                return self.app.apply(event, &ctx, reports);
            }
            melin_journal::JournalEvent::Tick { now_ns } => {
                // Defensive: the head-of-event drain has already advanced
                // the clock to `slot.timestamp_ns`, which equals `now_ns`
                // for tick-generator-published slots — so this call is
                // typically a no-op. Kept for paths where
                // `slot.timestamp_ns` is 0 (tests, manually constructed
                // Ticks) so time still advances as documented on
                // `JournalEvent::Tick`.
                self.app.tick(now_ns, reports);
            }
            melin_journal::JournalEvent::GenesisHash { .. }
            | melin_journal::JournalEvent::Checkpoint { .. } => {
                // Hash chain metadata — journal internal, never reaches
                // the application.
            }
            melin_journal::JournalEvent::Shutdown => {
                // Pipeline sentinel — handled at the run-loop level
                // (stage exits on observing it). Never reaches process_event
                // in practice; this arm is a safety net.
            }
        }
        None
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

/// Build the input disruptor and output SPSC, returning the stages and
/// the journal progress cursor for the response stage.
///
/// **Topology**: journal, matching, and (optionally) replication consumers
/// are all gated on the producer (parallel). The matching stage does NOT
/// wait for journal sync — the response stage gates on the journal cursor
/// (and replication cursor when active) instead.
///
/// Assembled pipeline stages and handles returned by [`build_pipeline_with_replication`].
pub struct Pipeline<A: Application> {
    pub input_producer: ring::Producer<InputSlot<A::Event>>,
    pub journal_stage: JournalStage<A::Event>,
    pub matching_stage: MatchingStage<A>,
    pub output_consumers: Vec<ring::Consumer<OutputSlot<A::Report, A::QueryResponse>>>,
    pub journal_cursor: Arc<Sequence>,
    pub matching_cursor: Arc<Sequence>,
    pub events_processed: Arc<AtomicU64>,
    pub input_cursor: Box<dyn ring::QueueCursor>,
    pub replication_consumers: Option<(ReplicationConsumer, ReplicationConsumer)>,
    pub replication_cursor: Arc<AtomicU64>,
    pub replicas_connected: Option<Arc<AtomicU32>>,
    pub shadow_consumer: Option<ring::Consumer<InputSlot<A::Event>>>,
    pub chain_hash_lock: Option<Arc<SeqLock<[u8; 32]>>>,
    pub replication_ring_progress: Option<ReplicationRingProgress>,
}

/// Assembled replica pipeline stages and handles returned by [`build_replica_pipeline`].
pub struct ReplicaPipeline<A: Application> {
    pub input_producer: ring::Producer<InputSlot<A::Event>>,
    pub journal_stage: JournalStage<A::Event>,
    pub matching_stage: MatchingStage<A>,
    pub drain_consumer: ring::Consumer<OutputSlot<A::Report, A::QueryResponse>>,
    pub journal_cursor: Arc<Sequence>,
    pub matching_cursor: Arc<Sequence>,
    pub shadow_consumer: Option<ring::Consumer<InputSlot<A::Event>>>,
    /// Always populated for replicas — the orchestrator reads it for
    /// reconnect handshakes (last journal sequence the replica has durably
    /// persisted). Updated by `JournalStage` after each fsync batch.
    pub last_seq: Arc<AtomicU64>,
    pub chain_hash_lock: Option<Arc<SeqLock<[u8; 32]>>>,
}

/// Build the pipeline with optional replication support.
///
/// When `enable_replication` is true, builds a replication ring (pre-allocated,
/// lock-free) and wires the producer into the `JournalStage`. After each
/// `flush_batch_sync()`, the journal stage copies the encoded bytes into a
/// pre-allocated ring slot (no heap allocation). The returned consumer(s)
/// are for replica sender threads.
///
/// Returns one `ReplicationConsumer` for the sender thread, and a
/// `replication_cursor` `Arc<AtomicU64>` for the response stage.
/// Handles for monitoring replication ring drain progress.
///
/// The server gates on ring drain after seeding: it waits for all ring
/// consumers (sender threads) to have read every published batch. This
/// is stronger than no gate (replicas might miss seeds) and faster than
/// waiting for replica TCP acks (no network round-trip). Deadlock-free
/// because the ring backpressures (spins) instead of dropping batches.
pub struct ReplicationRingProgress {
    /// Producer cursors (one per independent ring). `Arc<dyn QueueCursor>`
    /// so multiple readers (seed-drain gate, health snapshot) can share the
    /// same handle; `cursor_reader` only clones the inner `Arc<Shared>` so
    /// sharing has no hot-path cost.
    pub producer_cursors: Vec<Arc<dyn ring::QueueCursor>>,
    /// Consumer progress counters (one per independent ring, paired
    /// with the corresponding producer cursor by index).
    pub consumer_cursors: Vec<Arc<Sequence>>,
    /// Per-ring eviction flags. Set by the journal stage when a publish
    /// times out. Cleared by the sender thread after disconnecting the
    /// slow replica.
    pub evict_flags: [Arc<AtomicBool>; 2],
    /// Per-ring active flags. Set by the handler thread when the replica
    /// enters the live streaming loop. The journal stage only publishes
    /// to active rings.
    pub active_flags: [Arc<AtomicBool>; 2],
}

/// Pieces of the input disruptor that primary and replica builders both
/// need. Built by [`build_input_disruptor`] in one place so the two
/// builders don't drift on consumer order or cursor wiring.
struct InputDisruptorParts<E: AppEvent> {
    input_producer: ring::Producer<InputSlot<E>>,
    /// Type-erased cursor reader for queue-depth monitoring. Always
    /// extracted before the producer is moved to its owning thread —
    /// the replica builder discards it (no health probe today), the
    /// primary builder threads it into [`Pipeline::input_cursor`].
    input_cursor: Box<dyn ring::QueueCursor>,
    journal_consumer: ring::Consumer<InputSlot<E>>,
    matching_consumer: ring::Consumer<InputSlot<E>>,
    shadow_consumer: Option<ring::Consumer<InputSlot<E>>>,
    journal_cursor: Arc<Sequence>,
    matching_cursor: Arc<Sequence>,
}

/// Build the shared input disruptor topology: journal + matching gated on
/// the producer in parallel, plus an optional shadow consumer chained
/// after journal (only sees events that have been durably fsynced).
///
/// Single producer: the ingress thread on primaries (which also emits
/// ticks) or the replication receiver on replicas. The seed loop reuses
/// the same producer before handing it off to the ingress thread, so the
/// ring is single-producer at every moment of operation.
fn build_input_disruptor<E: AppEvent + Send + 'static>(
    enable_shadow: bool,
) -> InputDisruptorParts<E> {
    let mut builder = ring::DisruptorBuilder::<InputSlot<E>>::new(INPUT_RING_CAPACITY)
        .add_consumer() // consumer 0: journal, gated on producer
        .add_consumer(); // consumer 1: matching, gated on producer (parallel)
    if enable_shadow {
        builder = builder.add_consumer_after(0); // consumer 2: shadow, gated on journal
    }
    let (input_producer, mut consumers) = builder.build();

    let input_cursor = input_producer.cursor_reader();

    // Pop consumers in reverse order of addition: with shadow enabled the
    // build order is [journal(0), matching(1), shadow(2)], so pop yields
    // shadow → matching → journal.
    let shadow_consumer = if enable_shadow {
        Some(consumers.pop().expect("shadow consumer"))
    } else {
        None
    };
    let matching_consumer = consumers.pop().expect("matching consumer");
    let journal_consumer = consumers.pop().expect("journal consumer");

    let journal_cursor = journal_consumer.progress_counter();
    let matching_cursor = matching_consumer.progress_counter();

    InputDisruptorParts {
        input_producer,
        input_cursor,
        journal_consumer,
        matching_consumer,
        shadow_consumer,
        journal_cursor,
        matching_cursor,
    }
}

/// If shadow snapshots are enabled, allocate a SeqLock for publishing the
/// BLAKE3 chain hash to the shadow stage and wire it into `journal_stage`.
/// Returns the lock (so the caller can return it through its pipeline
/// handle struct) or `None` when shadow is disabled — zero overhead in
/// that case.
fn setup_chain_hash_publisher<E: AppEvent>(
    journal_stage: &mut JournalStage<E>,
    enable_shadow: bool,
) -> Option<Arc<SeqLock<[u8; 32]>>> {
    if enable_shadow {
        let lock = Arc::new(SeqLock::new([0u8; 32]));
        journal_stage.set_chain_hash_lock(Arc::clone(&lock));
        Some(lock)
    } else {
        None
    }
}

/// When replication is disabled, the cursor is `u64::MAX` (standalone mode).
#[allow(clippy::too_many_arguments)]
pub fn build_pipeline_with_replication<A>(
    app: A,
    writer: melin_journal::JournalWriter<A::Event>,
    group_commit_delay: Duration,
    active_connections: Arc<AtomicU64>,
    enable_replication: bool,
    max_journal_batch: usize,
    replication_ring_size: usize,
    busy_spin: bool,
    enable_event_publisher: bool,
    enable_shadow: bool,
) -> Pipeline<A>
where
    A: Application + Send + 'static,
    A::Event: Send + 'static,
    A::Report: Send + 'static,
{
    let InputDisruptorParts {
        input_producer,
        input_cursor,
        journal_consumer,
        matching_consumer,
        shadow_consumer,
        journal_cursor,
        matching_cursor,
    } = build_input_disruptor::<A::Event>(enable_shadow);

    // Output disruptor ring: matching → response (+ optional event publisher).
    // Single producer, N consumers (1 = response only, 2 = response + event publisher).
    // Uses the same ring::Producer/Consumer API as the input disruptor.
    let mut output_builder =
        ring::DisruptorBuilder::<OutputSlot<A::Report, A::QueryResponse>>::new(
            OUTPUT_RING_CAPACITY,
        )
        .add_consumer(); // consumer 0: response stage
    if enable_event_publisher {
        output_builder = output_builder.add_consumer(); // consumer 1: event publisher
    }
    let (output_producer, output_consumers) = output_builder.build();

    let events_processed = Arc::new(AtomicU64::new(0));

    let mut journal_stage = JournalStage::new(
        writer,
        journal_consumer,
        group_commit_delay,
        max_journal_batch,
        busy_spin,
    );

    // Build two independent SPSC replication rings (one per replica slot).
    // Each ring has its own producer and consumer, so a slow replica only
    // stalls its own ring — not the other replica's. The journal stage
    // publishes to both rings sequentially; on timeout, sets an eviction
    // flag and stops publishing to the stalled ring.
    let (replication_consumers, replication_ring_progress) = if enable_replication {
        let (producer_0, mut consumers_0) =
            melin_journal::replication::build_replication_ring(1, replication_ring_size);
        let (producer_1, mut consumers_1) =
            melin_journal::replication::build_replication_ring(1, replication_ring_size);

        let evict_flags = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        let active_flags = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];

        let ring_progress = ReplicationRingProgress {
            producer_cursors: vec![
                Arc::<dyn ring::QueueCursor>::from(producer_0.cursor_reader()),
                Arc::<dyn ring::QueueCursor>::from(producer_1.cursor_reader()),
            ],
            consumer_cursors: vec![
                consumers_0[0].progress_counter(),
                consumers_1[0].progress_counter(),
            ],
            evict_flags: [Arc::clone(&evict_flags[0]), Arc::clone(&evict_flags[1])],
            active_flags: [Arc::clone(&active_flags[0]), Arc::clone(&active_flags[1])],
        };

        journal_stage.set_replication_producers(
            [producer_0, producer_1],
            [Arc::clone(&evict_flags[0]), Arc::clone(&evict_flags[1])],
            [Arc::clone(&active_flags[0]), Arc::clone(&active_flags[1])],
        );

        let consumer_0 = consumers_0.pop().expect("ring 0 consumer");
        let consumer_1 = consumers_1.pop().expect("ring 1 consumer");
        (Some((consumer_0, consumer_1)), Some(ring_progress))
    } else {
        (None, None)
    };

    let chain_hash_lock = setup_chain_hash_publisher(&mut journal_stage, enable_shadow);

    // Connected replica count: when replication is enabled, starts at 0
    // (no replicas yet). The replication sender increments on connect,
    // decrements on disconnect. The matching stage checks it (one Relaxed
    // load per event) and rejects all mutations when 0. In standalone
    // mode, None — no halt check. u32 counter supports dual replication.
    let replicas_connected = if enable_replication {
        Some(Arc::new(AtomicU32::new(0)))
    } else {
        None
    };

    let matching_stage = MatchingStage::<A>::new(
        app,
        matching_consumer,
        output_producer,
        Arc::clone(&events_processed),
        Arc::clone(&journal_cursor),
        active_connections,
        replicas_connected.clone(),
        busy_spin,
    );

    // Replication cursor: shared atomic read by the response stage.
    // Always initialized to u64::MAX so the server works immediately
    // even before a replica connects. When a replica connects and starts
    // acking, the sender thread sets this to the acked sequence.
    // On disconnect, it's reset to u64::MAX (degrade to local-only).
    // This means: `min(journal_cursor, u64::MAX) = journal_cursor`.
    let replication_cursor = Arc::new(AtomicU64::new(u64::MAX));

    Pipeline {
        input_producer,
        journal_stage,
        matching_stage,
        output_consumers,
        journal_cursor,
        matching_cursor,
        events_processed,
        input_cursor,
        replication_consumers,
        replication_cursor,
        replicas_connected,
        shadow_consumer,
        chain_hash_lock,
        replication_ring_progress,
    }
}

/// Build a pipeline for replica mode. Same disruptor stages as the primary
/// (journal → matching → shadow), but:
/// - No replication ring (this IS the replica)
/// - No `replicas_connected` halt check
/// - Output disruptor has a single drain consumer (no response stage)
///
/// The replica's journal stage encodes events from the disruptor using
/// the pre-assigned sequences and timestamps carried in each `InputSlot`
/// (set by the replication receiver from the primary's batch metadata).
/// Journals are logically identical across nodes (same sequences, same
/// events) but not byte-identical (each node stamps its own wall-clock
/// on the batch when `slot.sequence == 0`, and checkpoint timing may
/// vary after journal rotation).
pub fn build_replica_pipeline<A>(
    app: A,
    writer: melin_journal::JournalWriter<A::Event>,
    max_journal_batch: usize,
    group_commit_delay: Duration,
    busy_spin: bool,
    enable_shadow: bool,
) -> ReplicaPipeline<A>
where
    A: Application + Send + 'static,
    A::Event: Send + 'static,
    A::Report: Send + 'static,
{
    let InputDisruptorParts {
        input_producer,
        input_cursor: _, // replica has no health-probe consumer of input depth
        journal_consumer,
        matching_consumer,
        shadow_consumer,
        journal_cursor,
        matching_cursor,
    } = build_input_disruptor::<A::Event>(enable_shadow);

    // Output disruptor: single drain consumer (no response stage on replica).
    let output_builder = ring::DisruptorBuilder::<OutputSlot<A::Report, A::QueryResponse>>::new(
        OUTPUT_RING_CAPACITY,
    )
    .add_consumer();
    let (output_producer, mut output_consumers) = output_builder.build();
    let drain_consumer = output_consumers.pop().expect("drain consumer");

    let events_processed = Arc::new(AtomicU64::new(0));

    // Journal stage: same as primary (encode mode). Pre-assigned sequences
    // in each InputSlot keep the replica's journal aligned with the primary.
    let mut journal_stage = JournalStage::new(
        writer,
        journal_consumer,
        group_commit_delay,
        max_journal_batch,
        busy_spin,
    );

    let chain_hash_lock = setup_chain_hash_publisher(&mut journal_stage, enable_shadow);

    // Last-journaled-sequence atomic — always populated for replicas. The
    // orchestrator reads it for reconnect handshakes without owning the
    // writer (the writer lives inside `journal_stage` for the lifetime of
    // the pipeline).
    let last_seq = Arc::new(AtomicU64::new(0));
    journal_stage.set_last_seq_publisher(Arc::clone(&last_seq));

    // Matching stage: same as primary but with no replicas_connected check
    // (None = standalone mode, never halts on replica disconnect).
    let active_connections = Arc::new(AtomicU64::new(0));
    let matching_stage = MatchingStage::<A>::new(
        app,
        matching_consumer,
        output_producer,
        Arc::clone(&events_processed),
        Arc::clone(&journal_cursor),
        active_connections,
        None, // no replicas_connected halt check on replica
        busy_spin,
    );

    ReplicaPipeline {
        input_producer,
        journal_stage,
        matching_stage,
        drain_consumer,
        journal_cursor,
        matching_cursor,
        shadow_consumer,
        last_seq,
        chain_hash_lock,
    }
}
