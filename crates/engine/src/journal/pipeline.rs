//! Pipeline stages for the LMAX disruptor architecture.
//!
//! Two hot-path stages consume from an input disruptor in **parallel**:
//! 1. **Journal stage**: batch-encodes events, then writes + syncs in a single
//!    `pwritev2` with `RWF_DSYNC` (FUA). Advances its cursor only after the
//!    durable write completes.
//! 2. **Matching stage**: executes commands on the `Exchange`, publishes responses
//!    to the output SPSC. Runs concurrently with the journal — no waiting for sync.
//!
//! The **response stage** (in the server crate) consumes the output SPSC but
//! gates on the journal cursor before sending: a response is only sent to
//! the client after the corresponding event is durable on disk.
//!
//! This gives maximum pipeline parallelism (matching overlaps journal I/O)
//! while preserving persist-before-ack at the response boundary.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::exchange::Exchange;
use crate::journal::event::JournalEvent;
use crate::journal::trace::{TraceTimestamp, trace_ts};
use crate::journal::writer::JournalWriter;
use crate::types::ExecutionReport;

use trading_disruptor::padding::Sequence;
use trading_disruptor::ring;
use trading_disruptor::spsc;

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
/// Limits the time spent encoding before sync, keeping tail latency bounded.
const MAX_JOURNAL_BATCH: usize = 1024;

/// Maximum events consumed per disruptor batch in the matching stage.
/// Amortizes one atomic Release store over N events. Keep small to avoid
/// burstiness that causes the response stage to wait on the journal cursor.
/// Benchmarked: 32 is the sweet spot — lower values underperform due to
/// consume_batch overhead, higher values (64+) add burstiness with no
/// throughput gain. At ~100 ns/event, 32 events = ~3.2 µs burst.
const MAX_MATCHING_BATCH: usize = 32;

/// Slot in the input disruptor ring buffer.
///
/// Carries a connection ID alongside the event so the response stage knows
/// where to route execution reports. `Copy` for zero-cost ring buffer ops.
/// ~72 bytes: connection_id(8) + JournalEvent(~60) + padding.
#[derive(Debug, Clone, Copy)]
pub struct InputSlot {
    /// Which client connection submitted this command.
    pub connection_id: u64,
    /// The journaled event (order submit, cancel, etc.).
    pub event: JournalEvent,
    /// Timestamp when the publisher wrote this slot to the disruptor.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub publish_ts: TraceTimestamp,
    /// Timestamp when the reader task received this request from the wire.
    /// Flows through the entire pipeline to measure server-side end-to-end latency.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub recv_ts: TraceTimestamp,
}

impl Default for InputSlot {
    fn default() -> Self {
        // Default uses a zero-cost Deposit event as placeholder.
        // Ring buffer slots are always overwritten before being read,
        // so the default value is never observed.
        Self {
            connection_id: 0,
            event: JournalEvent::Deposit {
                account: crate::types::AccountId(0),
                currency: crate::types::CurrencyId(0),
                amount: 0,
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        }
    }
}

/// Slot in the output SPSC queue (matching → response).
///
/// Each slot carries either an execution report or a batch-end marker
/// for a specific connection, plus the input sequence it originated from
/// so the response stage can gate on journal completion.
#[derive(Debug, Clone, Copy)]
pub struct OutputSlot {
    /// Which client connection receives this response.
    pub connection_id: u64,
    /// Input disruptor sequence this output originated from.
    /// The response stage must not send this until the journal cursor
    /// has advanced past this value (i.e., the event is durable).
    pub input_seq: u64,
    /// The response payload.
    pub payload: OutputPayload,
    /// Timestamp when the matching stage finished processing this event.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub match_complete_ts: TraceTimestamp,
    /// Timestamp when the reader task received this request from the wire.
    /// Carried through the pipeline to measure server-side end-to-end latency.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub recv_ts: TraceTimestamp,
}

/// Payload within an output slot.
#[derive(Debug, Clone, Copy)]
pub enum OutputPayload {
    /// An execution report from matching.
    Report(ExecutionReport),
    /// Signals the end of reports for one request.
    BatchEnd,
    /// Internal error during matching.
    EngineError,
    /// Server stats snapshot in response to `QueryStats`.
    StatsHeader {
        active_connections: u64,
        events_processed: u64,
        journal_sequence: u64,
    },
}

impl Default for OutputSlot {
    fn default() -> Self {
        Self {
            connection_id: 0,
            input_seq: 0,
            payload: OutputPayload::BatchEnd,
            match_complete_ts: trace_ts(),
            recv_ts: trace_ts(),
        }
    }
}

/// Journal stage: consumes from the input disruptor, batch-encodes events,
/// and writes + syncs in a single `pwritev2` with `RWF_DSYNC` (FUA).
///
/// Runs on a dedicated OS thread. Uses `read_batch` + `commit` so its
/// cursor only advances **after** the durable write. The response stage
/// reads this cursor to know when events are durable.
pub struct JournalStage {
    writer: JournalWriter,
    consumer: ring::Consumer<InputSlot>,
    /// Group commit coalescing window. The journal stage waits up to this
    /// duration after the first unsynced write before issuing the durable
    /// write, allowing more events to accumulate in the batch. At high
    /// event rates, the batch fills naturally and the delay rarely fires.
    /// Zero means sync immediately (no delay).
    /// Only read when fsync is enabled (not `no-fsync` feature).
    #[cfg_attr(feature = "no-fsync", allow(dead_code))]
    group_commit_delay: Duration,
}

impl JournalStage {
    /// Create a new journal stage.
    ///
    /// `group_commit_delay`: coalescing window for sync batching. The
    /// journal waits up to this duration for more events to arrive before
    /// issuing the durable write. Zero means sync immediately after each
    /// batch read.
    pub fn new(
        writer: JournalWriter,
        consumer: ring::Consumer<InputSlot>,
        group_commit_delay: Duration,
    ) -> Self {
        Self {
            writer,
            consumer,
            group_commit_delay,
        }
    }

    /// Run the journal stage loop.
    ///
    /// Dispatches to the io_uring overlapped path when the `io-uring`
    /// feature is enabled and fsync is active. Falls back to the
    /// synchronous `pwritev2+RWF_DSYNC` path otherwise.
    ///
    /// Returns the `JournalWriter` on shutdown for clean resource release.
    pub fn run(self, shutdown: &std::sync::atomic::AtomicBool) -> JournalWriter {
        #[cfg(all(
            feature = "io-uring",
            not(feature = "no-fsync"),
            not(feature = "no-persist")
        ))]
        {
            self.run_uring(shutdown)
        }
        #[cfg(not(all(
            feature = "io-uring",
            not(feature = "no-fsync"),
            not(feature = "no-persist")
        )))]
        {
            self.run_sync(shutdown)
        }
    }

    /// Synchronous journal loop: `pwritev2+RWF_DSYNC` blocks until durable.
    ///
    /// Uses `read_batch` + `commit` (not `consume_batch`) to ensure the
    /// journal cursor is only advanced **after** the write is durable.
    /// The response stage checks this cursor before sending — this is
    /// the persist-before-ack boundary.
    #[cfg_attr(
        all(
            feature = "io-uring",
            not(feature = "no-fsync"),
            not(feature = "no-persist")
        ),
        allow(dead_code)
    )]
    fn run_sync(mut self, shutdown: &std::sync::atomic::AtomicBool) -> JournalWriter {
        #[cfg(not(feature = "no-fsync"))]
        use std::time::Instant;

        let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];
        #[cfg(not(feature = "no-fsync"))]
        let delay = self.group_commit_delay;
        let mut idle_spins: u32 = 0;

        // Total events encoded since last sync/commit.
        let mut pending: usize = 0;
        // Timestamp of first unsynced write (for group commit delay).
        #[cfg(not(feature = "no-fsync"))]
        let mut first_write_ts: Option<Instant> = None;

        #[cfg(feature = "pipeline-stats")]
        let mut busy_count: u64 = 0;
        #[cfg(feature = "pipeline-stats")]
        let mut idle_count: u64 = 0;

        #[cfg(feature = "latency-trace")]
        let mut wakeup_hist = crate::journal::trace::StageHistogram::new(
            "journal: disruptor wakeup (publish → journal consume)",
        );
        #[cfg(feature = "latency-trace")]
        let mut batch_hist =
            crate::journal::trace::StageHistogram::new("journal: batch processing (write + sync)");

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
                #[cfg(feature = "latency-trace")]
                {
                    wakeup_hist.print_report();
                    batch_hist.print_report();
                }
                #[cfg(feature = "pipeline-stats")]
                print_utilization("journal", busy_count, idle_count);
                return self.writer;
            }

            // Read entries WITHOUT advancing the cursor.
            let remaining = MAX_JOURNAL_BATCH.saturating_sub(pending);
            let count = if remaining > 0 {
                self.consumer.read_batch(&mut batch, remaining)
            } else {
                0
            };

            if count > 0 {
                idle_spins = 0;
                #[cfg(feature = "pipeline-stats")]
                {
                    busy_count += 1;
                }

                #[cfg(feature = "latency-trace")]
                let batch_start = trace_ts();

                #[cfg(feature = "latency-trace")]
                for slot in &batch[..count] {
                    wakeup_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                        slot.publish_ts,
                        batch_start,
                    ));
                }

                // Batch-encode all events into the writer's internal buffer.
                // Data stays in the buffer until the sync point — one
                // pwritev2+RWF_DSYNC replaces multiple pwrites + fdatasync.
                // QueryStats is not journaled (no state change).
                #[cfg(not(feature = "no-persist"))]
                {
                    for slot in &batch[..count] {
                        if matches!(slot.event, JournalEvent::QueryStats) {
                            continue;
                        }
                        if let Err(e) = self.writer.batch_append(&slot.event) {
                            tracing::error!(error = %e, "journal encode error");
                        }
                    }
                }
                pending += count;
                #[cfg(not(feature = "no-fsync"))]
                if first_write_ts.is_none() {
                    first_write_ts = Some(Instant::now());
                }

                #[cfg(feature = "latency-trace")]
                batch_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                    batch_start,
                    trace_ts(),
                ));
            }

            // Sync when: we have data AND (batch full OR delay expired OR no delay).
            if pending > 0 {
                #[cfg(not(feature = "no-fsync"))]
                let should_sync = pending >= MAX_JOURNAL_BATCH
                    || delay.is_zero()
                    || first_write_ts.is_some_and(|ts| ts.elapsed() >= delay);
                #[cfg(feature = "no-fsync")]
                let should_sync = true;

                if should_sync {
                    #[cfg(not(feature = "no-persist"))]
                    if let Err(e) = self.writer.flush_batch_sync() {
                        tracing::error!(error = %e, "journal flush_batch_sync error");
                    }
                    self.consumer.commit(pending);
                    pending = 0;
                    #[cfg(not(feature = "no-fsync"))]
                    {
                        first_write_ts = None;
                    }
                }
            } else {
                #[cfg(feature = "pipeline-stats")]
                {
                    idle_count += 1;
                }
                if idle_spins < 1000 {
                    idle_spins += 1;
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Drain any remaining entries from the ring buffer on shutdown.
    fn drain_remaining(&mut self, batch: &mut [InputSlot]) {
        loop {
            let count = self.consumer.read_batch(batch, MAX_JOURNAL_BATCH);
            if count == 0 {
                break;
            }
            #[cfg(not(feature = "no-persist"))]
            {
                for slot in &batch[..count] {
                    if matches!(slot.event, JournalEvent::QueryStats) {
                        continue;
                    }
                    if let Err(e) = self.writer.batch_append(&slot.event) {
                        tracing::error!(error = %e, "journal encode error on drain");
                    }
                }
                if let Err(e) = self.writer.flush_batch_sync() {
                    tracing::error!(error = %e, "journal sync error on drain");
                }
            }
            self.consumer.commit(count);
        }
    }

    /// Overlapped io_uring journal loop: submits Write+RWF_DSYNC asynchronously
    /// and accumulates the next batch in a second buffer while the NVMe FUA
    /// write is in flight. Doubles effective throughput when journal I/O is
    /// the bottleneck.
    ///
    /// Cursor only advances after the CQE confirms durability — the
    /// persist-before-ack guarantee is preserved.
    #[cfg(all(
        feature = "io-uring",
        not(feature = "no-fsync"),
        not(feature = "no-persist")
    ))]
    fn run_uring(mut self, shutdown: &std::sync::atomic::AtomicBool) -> JournalWriter {
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
            .expect("io_uring init failed");

        // Register the journal fd so the kernel skips fget/fput (fd table
        // lookup + atomic refcount) on every SQE. Use types::Fixed(0) in
        // SQEs instead of types::Fd(raw_fd).
        let raw_fd = self.writer.fd();
        ring.submitter()
            .register_files(&[raw_fd])
            .expect("io_uring register_files failed");

        let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];
        let delay = self.group_commit_delay;
        let mut idle_spins: u32 = 0;
        let mut pending: usize = 0;
        let mut first_write_ts: Option<Instant> = None;

        // In-flight state: the batch being written and the sequence to commit
        // when the CQE arrives. `inflight_seq` is the consumer's `next_read`
        // at the time of submission — committing it advances the cursor to
        // exactly the events covered by the durable write.
        let mut inflight: Option<(super::writer::AsyncWriteBatch, u64)> = None;

        #[cfg(feature = "pipeline-stats")]
        let mut busy_count: u64 = 0;
        #[cfg(feature = "pipeline-stats")]
        let mut idle_count: u64 = 0;

        loop {
            // --- Check shutdown ---
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                // Wait for in-flight write to complete.
                if let Some((batch_data, seq)) = inflight.take() {
                    self.wait_for_cqe(&mut ring, &batch_data);
                    self.consumer.set_progress(seq);
                    self.writer.confirm_async_write(batch_data);
                }
                // Flush any pending buffered data synchronously.
                if pending > 0 {
                    if let Err(e) = self.writer.flush_batch_sync() {
                        tracing::error!(error = %e, "journal sync error on shutdown");
                    }
                    self.consumer.commit(pending);
                }
                self.drain_remaining(&mut batch);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("journal", busy_count, idle_count);
                return self.writer;
            }

            // --- Reap CQE from previous in-flight write (non-blocking) ---
            // CQEs are posted directly to the shared CQ ring in interrupt
            // context — no syscall needed to make them visible.
            if let Some((ref batch_data, seq)) = inflight
                && let Some(cqe) = ring.completion().next()
            {
                let result = cqe.result();
                if result < 0 {
                    tracing::error!(errno = -result, "io_uring journal write failed");
                } else if (result as usize) != batch_data.buf.len() {
                    tracing::error!(
                        written = result,
                        expected = batch_data.buf.len(),
                        "io_uring journal short write"
                    );
                }
                // Advance cursor: these events are now durable.
                self.consumer.set_progress(seq);
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

            if count > 0 {
                idle_spins = 0;
                #[cfg(feature = "pipeline-stats")]
                {
                    busy_count += 1;
                }

                for slot in &batch[..count] {
                    if matches!(slot.event, JournalEvent::QueryStats) {
                        continue;
                    }
                    if let Err(e) = self.writer.batch_append(&slot.event) {
                        tracing::error!(error = %e, "journal encode error");
                    }
                }
                pending += count;
                if first_write_ts.is_none() {
                    first_write_ts = Some(Instant::now());
                }
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
                    tracing::error!(errno = -result, "io_uring journal write failed");
                } else if (result as usize) != batch_data.buf.len() {
                    tracing::error!(
                        written = result,
                        expected = batch_data.buf.len(),
                        "io_uring journal short write"
                    );
                }
                self.consumer.set_progress(seq);
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
                let batch_full = pending >= MAX_JOURNAL_BATCH;
                let delay_expired =
                    delay.is_zero() || first_write_ts.is_some_and(|ts| ts.elapsed() >= delay);

                let should_submit = (slot_free && delay_expired) || batch_full;

                if should_submit {
                    // If a write is still in-flight, block until it completes
                    // (backpressure — both buffers full).
                    if let Some((batch_data, seq)) = inflight.take() {
                        self.wait_for_cqe(&mut ring, &batch_data);
                        self.consumer.set_progress(seq);
                        self.writer.confirm_async_write(batch_data);
                    }

                    // Take the batch buffer and submit async write.
                    match self.writer.take_batch_for_async_write() {
                        Ok(Some(async_batch)) => {
                            let seq = self.consumer.next_read();
                            let sqe = opcode::Write::new(
                                types::Fixed(0),
                                async_batch.buf.as_ptr(),
                                async_batch.buf.len() as u32,
                            )
                            .offset(async_batch.offset)
                            .rw_flags(libc::RWF_DSYNC)
                            .build()
                            .user_data(1);

                            unsafe {
                                ring.submission().push(&sqe).expect("SQ full");
                            }
                            ring.submit().expect("io_uring submit failed");

                            inflight = Some((async_batch, seq));
                        }
                        Ok(None) => {
                            // Buffer was empty (all QueryStats), just commit.
                            self.consumer.commit(pending);
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "journal take_batch error");
                        }
                    }
                    pending = 0;
                    first_write_ts = None;
                }
            } else {
                #[cfg(feature = "pipeline-stats")]
                {
                    idle_count += 1;
                }
                if idle_spins < 1000 {
                    idle_spins += 1;
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
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
    #[cfg(all(
        feature = "io-uring",
        not(feature = "no-fsync"),
        not(feature = "no-persist")
    ))]
    fn wait_for_cqe(
        &self,
        ring: &mut io_uring::IoUring,
        batch_data: &super::writer::AsyncWriteBatch,
    ) {
        loop {
            if let Some(cqe) = ring.completion().next() {
                let result = cqe.result();
                if result < 0 {
                    tracing::error!(errno = -result, "io_uring journal write failed (drain)");
                } else if (result as usize) != batch_data.buf.len() {
                    tracing::error!(
                        written = result,
                        expected = batch_data.buf.len(),
                        "io_uring journal short write (drain)"
                    );
                }
                return;
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
pub struct MatchingStage {
    exchange: Exchange,
    consumer: ring::Consumer<InputSlot>,
    output: spsc::Producer<OutputSlot>,
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
}

impl MatchingStage {
    /// Create a new matching stage.
    pub fn new(
        exchange: Exchange,
        consumer: ring::Consumer<InputSlot>,
        output: spsc::Producer<OutputSlot>,
        events_processed: Arc<AtomicU64>,
        journal_cursor: Arc<Sequence>,
        active_connections: Arc<AtomicU64>,
    ) -> Self {
        Self {
            exchange,
            consumer,
            output,
            events_processed,
            journal_cursor,
            active_connections,
        }
    }

    /// Run the matching stage loop. Blocks until shutdown.
    ///
    /// Uses small-batch consumption from the disruptor to amortize the
    /// atomic progress-store: one `Release` store per batch instead of
    /// per event. Events are still processed sequentially — only the
    /// disruptor I/O is batched.
    ///
    /// Returns the `Exchange` on shutdown for potential snapshot saving.
    pub fn run(mut self, shutdown: &std::sync::atomic::AtomicBool) -> Exchange {
        // Pre-allocated report buffer, reused across commands.
        // Pre-allocate with generous capacity. A market order sweeping many
        // price levels can produce one Fill per level + Placed/Cancelled. 256
        // avoids mid-hot-path reallocation for all but extreme scenarios.
        let mut reports: Vec<ExecutionReport> = Vec::with_capacity(256);
        // Spin count for adaptive wait: spin first (fast wakeup), then yield
        // to the OS scheduler (prevents the kernel from aggressively preempting
        // this thread during busy periods). 1000 spins ≈ 1µs at ~1ns/spin,
        // which is well under the inter-event arrival time at peak throughput.
        let mut idle_spins: u32 = 0;
        // Thread-local events counter — plain u64 increment (~0.3ns) instead
        // of atomic fetch_add (~5-8ns). Flushed to the shared Arc<AtomicU64>
        // only when QueryStats fires (~1/sec) or on shutdown.
        let mut local_events: u64 = 0;

        let mut batch = [InputSlot::default(); MAX_MATCHING_BATCH];

        #[cfg(feature = "pipeline-stats")]
        let mut busy_count: u64 = 0;
        #[cfg(feature = "pipeline-stats")]
        let mut idle_count: u64 = 0;

        #[cfg(feature = "latency-trace")]
        let mut wakeup_hist = crate::journal::trace::StageHistogram::new(
            "matching: disruptor wakeup (publish → matching consume)",
        );
        #[cfg(feature = "latency-trace")]
        let mut execute_hist =
            crate::journal::trace::StageHistogram::new("matching: execute (process_event)");

        loop {
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                // Drain remaining entries so every journaled event gets a response.
                self.drain_remaining(&mut reports);
                // Flush the thread-local counter to the shared atomic.
                self.events_processed.store(local_events, Ordering::Relaxed);
                #[cfg(feature = "latency-trace")]
                {
                    wakeup_hist.print_report();
                    execute_hist.print_report();
                }
                #[cfg(feature = "pipeline-stats")]
                print_utilization("matching", busy_count, idle_count);
                return self.exchange;
            }

            let batch_start = self.consumer.next_read();
            let count = self.consumer.consume_batch(&mut batch, MAX_MATCHING_BATCH);
            if count == 0 {
                #[cfg(feature = "pipeline-stats")]
                {
                    idle_count += 1;
                }
                if idle_spins < 1000 {
                    idle_spins += 1;
                    std::hint::spin_loop();
                } else {
                    std::thread::yield_now();
                }
                continue;
            }
            idle_spins = 0;

            for (i, slot) in batch[..count].iter().enumerate() {
                let input_seq = batch_start + i as u64;

                #[cfg(feature = "pipeline-stats")]
                {
                    busy_count += 1;
                }

                #[cfg(feature = "latency-trace")]
                {
                    let now = trace_ts();
                    wakeup_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                        slot.publish_ts,
                        now,
                    ));
                }

                reports.clear();

                #[cfg(feature = "latency-trace")]
                let exec_start = trace_ts();

                // QueryStats is handled inline — it reads matching-stage-owned
                // state and publishes directly without touching the Exchange.
                // Not counted in events_processed (it's not a trading event).
                if matches!(slot.event, JournalEvent::QueryStats) {
                    #[cfg(feature = "latency-trace")]
                    let exec_end = trace_ts();
                    #[cfg(feature = "latency-trace")]
                    execute_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                        exec_start, exec_end,
                    ));

                    #[allow(clippy::let_unit_value)]
                    let match_complete_ts = trace_ts();

                    // Flush the thread-local counter so the snapshot is current.
                    self.events_processed.store(local_events, Ordering::Relaxed);

                    let journal_sequence = self.journal_cursor.get().load(Ordering::Relaxed);
                    let active_connections = self.active_connections.load(Ordering::Relaxed);
                    self.output.publish(OutputSlot {
                        connection_id: slot.connection_id,
                        input_seq,
                        payload: OutputPayload::StatsHeader {
                            active_connections,
                            events_processed: local_events,
                            journal_sequence,
                        },
                        match_complete_ts,
                        recv_ts: slot.recv_ts,
                    });
                    self.output.publish(OutputSlot {
                        connection_id: slot.connection_id,
                        input_seq,
                        payload: OutputPayload::BatchEnd,
                        match_complete_ts,
                        recv_ts: slot.recv_ts,
                    });
                    continue;
                }

                local_events += 1;
                self.process_event(slot, &mut reports);

                #[cfg(feature = "latency-trace")]
                let exec_end = trace_ts();

                #[cfg(feature = "latency-trace")]
                execute_hist.record_ns(crate::journal::trace::trace_elapsed_ns(
                    exec_start, exec_end,
                ));

                #[allow(clippy::let_unit_value)] // ZST when latency-trace is disabled
                let match_complete_ts = trace_ts();

                // Publish execution reports to the output SPSC.
                // All output slots for this request carry the same input_seq
                // so the response stage can gate on journal completion.
                for report in &reports {
                    self.output.publish(OutputSlot {
                        connection_id: slot.connection_id,
                        input_seq,
                        payload: OutputPayload::Report(*report),
                        match_complete_ts,
                        recv_ts: slot.recv_ts,
                    });
                }

                // Signal end of batch for this request.
                self.output.publish(OutputSlot {
                    connection_id: slot.connection_id,
                    input_seq,
                    payload: OutputPayload::BatchEnd,
                    match_complete_ts,
                    recv_ts: slot.recv_ts,
                });
            }
        }
    }

    /// Drain any remaining entries from the ring buffer on shutdown,
    /// processing each and publishing responses. Ensures every journaled
    /// event gets a matching response sent to the client.
    fn drain_remaining(&mut self, reports: &mut Vec<ExecutionReport>) {
        loop {
            let entry = self.consumer.try_consume();
            let Some((input_seq, slot)) = entry else {
                break;
            };
            // Stats queries are meaningless during shutdown — skip to avoid
            // emitting a bare BatchEnd without a preceding StatsHeader.
            if matches!(slot.event, JournalEvent::QueryStats) {
                continue;
            }
            reports.clear();
            self.process_event(&slot, reports);

            #[allow(clippy::let_unit_value)]
            let match_complete_ts = trace_ts();

            for report in &*reports {
                self.output.publish(OutputSlot {
                    connection_id: slot.connection_id,
                    input_seq,
                    payload: OutputPayload::Report(*report),
                    match_complete_ts,
                    recv_ts: slot.recv_ts,
                });
            }
            self.output.publish(OutputSlot {
                connection_id: slot.connection_id,
                input_seq,
                payload: OutputPayload::BatchEnd,
                match_complete_ts,
                recv_ts: slot.recv_ts,
            });
        }
    }

    /// Execute a single event against the exchange.
    fn process_event(&mut self, slot: &InputSlot, reports: &mut Vec<ExecutionReport>) {
        match slot.event {
            JournalEvent::AddInstrument { spec } => {
                self.exchange.add_instrument(spec);
            }
            JournalEvent::Deposit {
                account,
                currency,
                amount,
            } => {
                self.exchange.deposit(account, currency, amount);
            }
            JournalEvent::SubmitOrder { symbol, order } => {
                self.exchange.execute(symbol, order, reports);
            }
            JournalEvent::CancelOrder {
                symbol,
                account,
                order_id,
            } => {
                self.exchange.cancel(symbol, account, order_id, reports);
            }
            JournalEvent::SetRiskLimits { symbol, limits } => {
                self.exchange.set_risk_limits(symbol, limits);
            }
            JournalEvent::CancelAll { account } => {
                self.exchange.cancel_all(account, reports);
            }
            JournalEvent::SetCircuitBreaker { symbol, config } => {
                self.exchange.set_circuit_breaker(symbol, config);
            }
            JournalEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                self.exchange.cancel_replace(
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                    reports,
                );
            }
            JournalEvent::SetFeeSchedule { symbol, schedule } => {
                self.exchange.set_fee_schedule(symbol, schedule);
            }
            JournalEvent::QueryStats => {
                // Handled inline in the run loop before process_event is called.
                // This arm exists only for exhaustiveness.
            }
            JournalEvent::GenesisHash { .. } | JournalEvent::Checkpoint { .. } => {
                // Hash chain metadata — no exchange state change.
            }
        }
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
/// **Topology**: both journal and matching consumers are gated on the
/// producer (parallel). The matching stage does NOT wait for journal
/// sync — the response stage gates on the journal cursor instead.
///
/// The caller (server) is responsible for building the response stage
/// and spawning all threads.
pub fn build_pipeline(
    exchange: Exchange,
    writer: JournalWriter,
    group_commit_delay: Duration,
    active_connections: Arc<AtomicU64>,
) -> (
    ring::MultiProducer<InputSlot>,
    JournalStage,
    MatchingStage,
    spsc::Consumer<OutputSlot>,
    Arc<Sequence>,
    Arc<AtomicU64>,
) {
    // Input disruptor: N producers (reader threads), 2 parallel consumers.
    // MultiProducer allows lock-free concurrent publishing from all reader
    // threads, eliminating the Mutex that previously serialized access.
    let (input_producer, mut consumers) =
        ring::DisruptorBuilder::<InputSlot>::new(INPUT_RING_CAPACITY)
            .add_consumer() // consumer 0: journal, gated on producer
            .add_consumer() // consumer 1: matching, gated on producer (parallel)
            .build_multi_producer();

    let matching_consumer = consumers.pop().expect("matching consumer");
    let journal_consumer = consumers.pop().expect("journal consumer");

    // Grab the journal's progress cursor before moving it into the stage.
    // The response stage will read this to gate on sync completion.
    let journal_cursor = journal_consumer.progress_counter();

    // Output SPSC: matching → response.
    let (output_producer, output_consumer) = spsc::channel::<OutputSlot>(OUTPUT_RING_CAPACITY);

    let events_processed = Arc::new(AtomicU64::new(0));

    let journal_stage = JournalStage::new(writer, journal_consumer, group_commit_delay);
    let matching_stage = MatchingStage::new(
        exchange,
        matching_consumer,
        output_producer,
        Arc::clone(&events_processed),
        Arc::clone(&journal_cursor),
        active_connections,
    );

    (
        input_producer,
        journal_stage,
        matching_stage,
        output_consumer,
        journal_cursor,
        events_processed,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    fn limit_order(id: u64, account: AccountId, side: Side, price: u64, qty: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(price).unwrap()),
            },
            time_in_force: TimeInForce::GTC,
            quantity: Quantity(NonZeroU64::new(qty).unwrap()),
            stp: SelfTradeProtection::Allow,
        }
    }

    #[test]
    fn journal_stage_batch_writes_and_syncs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline_journal.journal");

        let writer = JournalWriter::create(&path).unwrap();

        let (mut producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();

        let consumer = consumers.pop().unwrap();
        let stage = JournalStage::new(writer, consumer, Duration::ZERO);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        producer.publish(InputSlot {
            connection_id: 1,
            event: JournalEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(0),
                    quote: CurrencyId(1),
                },
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
        producer.publish(InputSlot {
            connection_id: 1,
            event: JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100_000,
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        std::thread::sleep(std::time::Duration::from_millis(50));
        shutdown.store(true, Ordering::Relaxed);
        let _writer = handle.join().unwrap();

        // Verify events were journaled (only when persistence is enabled).
        #[cfg(not(feature = "no-persist"))]
        {
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();
            let entry1 = reader.next_entry().unwrap().unwrap();
            assert!(matches!(entry1.event, JournalEvent::AddInstrument { .. }));
            let entry2 = reader.next_entry().unwrap().unwrap();
            assert!(matches!(entry2.event, JournalEvent::Deposit { .. }));
            assert!(reader.next_entry().unwrap().is_none());
        }
    }

    #[test]
    fn matching_stage_processes_events() {
        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);
        exchange.deposit(AccountId(2), CurrencyId(0), 1_000);

        let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let (output_producer, mut output_consumer) = spsc::channel::<OutputSlot>(64);

        // Journal cursor and counters not used in this test — create dummies.
        let dummy_cursor = Arc::new(Sequence::new(AtomicU64::new(0)));
        let events_counter = Arc::new(AtomicU64::new(0));
        let active_conns = Arc::new(AtomicU64::new(0));
        let stage = MatchingStage::new(
            exchange,
            consumer,
            output_producer,
            events_counter,
            dummy_cursor,
            active_conns,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        input_producer.publish(InputSlot {
            connection_id: 42,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        let mut attempts = 0;
        let output = loop {
            if let Some((_, slot)) = output_consumer.try_consume() {
                break slot;
            }
            attempts += 1;
            if attempts > 1_000_000 {
                panic!("timeout waiting for output");
            }
            std::hint::spin_loop();
        };

        assert_eq!(output.connection_id, 42);
        assert_eq!(output.input_seq, 0);
        assert!(matches!(
            output.payload,
            OutputPayload::Report(ExecutionReport::Placed { .. })
        ));

        let batch_end = loop {
            if let Some((_, slot)) = output_consumer.try_consume() {
                break slot;
            }
            std::hint::spin_loop();
        };
        assert!(matches!(batch_end.payload, OutputPayload::BatchEnd));

        shutdown.store(true, Ordering::Relaxed);
        let _exchange = handle.join().unwrap();
    }

    #[test]
    fn full_pipeline_journal_and_matching_parallel() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full_pipeline.journal");

        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);
        exchange.deposit(AccountId(2), CurrencyId(0), 1_000);

        let writer = JournalWriter::create(&path).unwrap();

        let active_conns = Arc::new(AtomicU64::new(0));
        let (
            input_producer,
            journal_stage,
            matching_stage,
            mut output_consumer,
            journal_cursor,
            _events_processed,
        ) = build_pipeline(exchange, writer, Duration::ZERO, active_conns);

        let shutdown = Arc::new(AtomicBool::new(false));
        let s1 = Arc::clone(&shutdown);
        let s2 = Arc::clone(&shutdown);

        let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
        let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

        // Submit an order through the pipeline.
        input_producer.publish(InputSlot {
            connection_id: 1,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        // Wait for the Placed report in the output SPSC.
        let output = loop {
            if let Some((_, slot)) = output_consumer.try_consume() {
                break slot;
            }
            std::hint::spin_loop();
        };

        assert!(matches!(
            output.payload,
            OutputPayload::Report(ExecutionReport::Placed { .. })
        ));
        assert_eq!(output.input_seq, 0);

        // Wait for journal to confirm durability (cursor > input_seq).
        loop {
            let cursor = journal_cursor.get().load(Ordering::Acquire);
            if cursor > output.input_seq {
                break;
            }
            std::hint::spin_loop();
        }

        // Now it's safe to send the response — event is durable.

        shutdown.store(true, Ordering::Relaxed);
        let _writer = t_journal.join().unwrap();
        let _exchange = t_matching.join().unwrap();

        // Verify the event was journaled (only when persistence is enabled).
        #[cfg(not(feature = "no-persist"))]
        {
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();
            let entry = reader.next_entry().unwrap().unwrap();
            assert!(matches!(entry.event, JournalEvent::SubmitOrder { .. }));
        }
    }
}
