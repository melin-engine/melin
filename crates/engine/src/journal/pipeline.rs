//! Pipeline stages for the LMAX disruptor architecture.
//!
//! Two hot-path stages consume from an input disruptor in **parallel**:
//! 1. **Journal stage**: batch-encodes events, then writes + syncs in a single
//!    `pwritev2` with `RWF_DSYNC` (FUA). Advances its cursor only after the
//!    durable write completes. When replication is enabled, sends a copy of
//!    each encoded batch to the replication sender thread via a bounded channel.
//!    The bytes are identical to what was written to disk — same sequences,
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

use crate::exchange::Exchange;
use crate::journal::error::JournalError;
use crate::journal::event::JournalEvent;
use crate::journal::replication::{ReplicationConsumer, ReplicationProducer};
use crate::journal::trace::{TraceTimestamp, trace_ts};
use crate::journal::writer::JournalWriter;
use crate::types::{AccountId, ExecutionReport, OrderId, RejectReason, Symbol};

use melin_disruptor::padding::Sequence;
use melin_disruptor::ring;
use melin_disruptor::seqlock::SeqLock;

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
/// Larger batches amortize the fixed cost of each NVMe FUA write over more
/// events. FUA cost is roughly constant up to ~128 KiB (one NVMe command),
/// so 4096 events × ~104 bytes ≈ 416 KiB still fits in a single write.
/// Under low load, batches are naturally small (drain what's available);
/// the cap only matters at sustained high throughput.
const MAX_JOURNAL_BATCH: usize = 4096;

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
/// Carries a connection ID alongside the event so the response stage knows
/// where to route execution reports. `Copy` for zero-cost ring buffer ops.
/// ~72 bytes: connection_id(8) + JournalEvent(~60) + padding.
#[derive(Debug, Clone, Copy)]
pub struct InputSlot {
    /// Which client connection submitted this command.
    pub connection_id: u64,
    /// FxHash of the client's Ed25519 public key. Used with `request_seq`
    /// for per-key idempotency dedup. 0 for seed/internal events.
    pub key_hash: u64,
    /// Per-key monotonic request sequence number from the wire protocol.
    /// Used with `key_hash` for idempotency dedup. 0 for seed/internal events.
    pub request_seq: u64,
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
            key_hash: 0,
            request_seq: 0,
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
///
/// When replication is enabled, the journal stage also sends a copy of
/// each encoded batch to the replication sender thread via a bounded
/// channel. The bytes are identical to what was written to disk — same
/// sequences, timestamps, CRC checksums, and checkpoint entries.
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
    /// Maximum events per journal fsync batch. Capped at MAX_JOURNAL_BATCH
    /// (the stack array size). Smaller values reduce tail latency.
    /// Only read when fsync is enabled (not `no-fsync` feature).
    #[cfg_attr(feature = "no-fsync", allow(dead_code))]
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
    /// When true, never yield to the OS scheduler — spin indefinitely with
    /// PAUSE. Requires isolated cores (`isolcpus`). See [`idle_wait`].
    busy_spin: bool,
    /// In replica mode, receives pre-encoded journal byte batches from the
    /// replication receiver thread via a lock-free hand-off. The journal
    /// stage writes these raw bytes instead of encoding events from the
    /// disruptor. This preserves byte-identical journals between primary
    /// and replica.
    raw_journal_rx: Option<RawBatchReceiver>,
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
        }
    }
}

/// Metadata for one raw batch in the SPSC ring. Small and `Copy` — the
/// journal bytes themselves live in the pre-allocated slot buffer at the
/// same ring index. The `len` field records how many bytes of the slot
/// are valid for this batch; the rest is leftover capacity from prior
/// batches and must not be read.
#[derive(Clone, Copy, Default)]
struct RawJournalMeta {
    len: u32,
    entry_count: u32,
    end_sequence: u64,
    chain_hash: [u8; 32],
}

/// Number of slots in the raw batch ring. Must be a power of two. At 8
/// slots the receiver can pipeline ~8 batches ahead of the journal
/// stage's NVMe writes.
const RAW_RING_CAPACITY: usize = 8;
const RAW_RING_MASK: u64 = (RAW_RING_CAPACITY as u64) - 1;

/// Initial per-slot buffer capacity. Sized for the common case of one
/// `journal_accum` gather per RECV pass on the replica receiver (a
/// coalesced batch from the primary is usually ~40 KB, worst observed
/// under stress is a few hundred KB). The slot buffer is a `Vec<u8>`
/// so it can grow amortized on the rare burst that exceeds this — the
/// per-slot capacity only affects allocator traffic during cold-start
/// and burst tails, never the steady-state hot path.
const RAW_SLOT_INITIAL_CAPACITY: usize = 1 << 20; // 1 MiB

/// Lock-free bounded SPSC ring for passing raw journal batches from the
/// replication receiver to the journal stage. Zero-allocation on the
/// hot path: the receiver writes directly into a pre-allocated slot
/// buffer, the journal stage reads the slot in place (borrowed) and
/// submits io_uring writes against the slot memory, then releases the
/// slot via [`RawBatchSlot::drop`] once the CQE confirms durability.
///
/// `head`/`tail` are cache-padded so false sharing can't bounce the
/// producer and consumer cache lines against each other.
///
/// `Vec<u8>` is chosen over a fixed `[u8; N]` array per slot so that
/// the rare burst batch larger than [`RAW_SLOT_INITIAL_CAPACITY`] grows
/// amortized instead of panicking; the amortized cost only fires in
/// the tail of a burst, not the steady state.
struct RawBatchRing {
    /// Per-slot byte buffers. `UnsafeCell` for interior mutability —
    /// mutual exclusion between producer and consumer is enforced by
    /// the head/tail ring protocol, not the type system.
    slots: Box<[std::cell::UnsafeCell<Vec<u8>>]>,
    /// Per-slot metadata. Published with the slot's bytes under the
    /// same Release store on `head`, so the consumer's Acquire load
    /// on `head` sees both consistently.
    metas: Box<[std::cell::UnsafeCell<RawJournalMeta>]>,
    /// Next slot to write (producer). Cache-padded to avoid false
    /// sharing with `tail` on the consumer's cache line.
    head: melin_disruptor::padding::CachePadded<AtomicU64>,
    /// Next slot to read (consumer).
    tail: melin_disruptor::padding::CachePadded<AtomicU64>,
}

unsafe impl Send for RawBatchRing {}
unsafe impl Sync for RawBatchRing {}

impl RawBatchRing {
    fn new() -> Self {
        let slots: Vec<std::cell::UnsafeCell<Vec<u8>>> = (0..RAW_RING_CAPACITY)
            .map(|_| std::cell::UnsafeCell::new(Vec::with_capacity(RAW_SLOT_INITIAL_CAPACITY)))
            .collect();
        let metas: Vec<std::cell::UnsafeCell<RawJournalMeta>> = (0..RAW_RING_CAPACITY)
            .map(|_| std::cell::UnsafeCell::new(RawJournalMeta::default()))
            .collect();
        Self {
            slots: slots.into_boxed_slice(),
            metas: metas.into_boxed_slice(),
            head: melin_disruptor::padding::CachePadded::new(AtomicU64::new(0)),
            tail: melin_disruptor::padding::CachePadded::new(AtomicU64::new(0)),
        }
    }
}

/// Sender half of the bounded SPSC ring. Owned by the replication
/// receiver thread.
pub struct RawBatchSender {
    ring: Arc<RawBatchRing>,
}

impl RawBatchSender {
    /// Publish a raw journal batch into the ring. Copies `bytes` into
    /// the next free slot buffer, writes the metadata, and releases the
    /// slot to the receiver. Spins if all slots are occupied — with 8
    /// slots and ~10–30 µs per NVMe write, the ring absorbs ~80–240 µs
    /// of receiver-ahead-of-journal pipelining.
    ///
    /// The slot buffer is a `Vec<u8>` with `RAW_SLOT_INITIAL_CAPACITY`
    /// headroom; larger batches grow the Vec amortized without altering
    /// the ring protocol.
    ///
    /// # Panics
    /// Panics if `bytes.len() > u32::MAX`. The wire format caps batches
    /// at a few hundred KB so this is unreachable in practice, but the
    /// explicit check prevents a silent truncation of `len` in the slot
    /// metadata (which would translate to a short/empty journal write).
    pub fn send(&self, bytes: &[u8], end_sequence: u64, chain_hash: [u8; 32], entry_count: u32) {
        assert!(
            bytes.len() <= u32::MAX as usize,
            "raw batch too large for u32 len field: {}",
            bytes.len()
        );
        loop {
            let head = self.ring.head.get().load(Ordering::Relaxed);
            let tail = self.ring.tail.get().load(Ordering::Acquire);
            if (head - tail) < RAW_RING_CAPACITY as u64 {
                let idx = (head & RAW_RING_MASK) as usize;
                // SAFETY: the ring protocol guarantees the consumer is
                // not touching slot `idx` — `tail <= head - CAP` means
                // the consumer is at least one full wrap behind. We are
                // the sole producer, so no other thread writes this slot.
                unsafe {
                    let slot = &mut *self.ring.slots[idx].get();
                    slot.clear();
                    slot.extend_from_slice(bytes);
                    *self.ring.metas[idx].get() = RawJournalMeta {
                        len: bytes.len() as u32,
                        entry_count,
                        end_sequence,
                        chain_hash,
                    };
                }
                // Release-store on `head` publishes both the slot bytes
                // and the metadata write above. Pairs with the Acquire
                // load in `RawBatchReceiver::try_recv`.
                self.ring.head.get().store(head + 1, Ordering::Release);
                return;
            }
            std::hint::spin_loop();
        }
    }
}

/// Handle to a raw batch slot held by the journal stage. The slot's
/// byte buffer is pinned in the ring for the lifetime of this handle —
/// the sender will not overwrite it until `Drop` advances `tail`.
///
/// This is the zero-copy hand-off: the journal stage submits an
/// io_uring Write with a pointer derived from this handle, carries the
/// handle in its in-flight state, and drops it after the CQE confirms
/// durability. Dropping releases the slot back to the sender.
///
/// The raw pointer + `Arc<RawBatchRing>` pair is used instead of a
/// borrowed slice so the handle is `'static` and can live in the
/// journal stage's `Option<InflightRaw>` across loop iterations without
/// tangling the borrow checker in self-referential lifetimes.
pub struct RawBatchSlot {
    ring: Arc<RawBatchRing>,
    ptr: *const u8,
    len: usize,
    pub end_sequence: u64,
    pub chain_hash: [u8; 32],
    pub entry_count: u32,
}

// SAFETY: `RawBatchSlot` is moved between the receiver thread and the
// journal-stage thread by value (no aliasing). The raw pointer is
// derived from `Arc<RawBatchRing>` memory that outlives the handle,
// and the ring protocol keeps the slot pinned for the handle's
// lifetime. `Sync` is deliberately NOT implemented — only one thread
// holds the handle at a time.
unsafe impl Send for RawBatchSlot {}

impl RawBatchSlot {
    /// Borrowed view of the journal bytes in the pinned slot.
    pub fn bytes(&self) -> &[u8] {
        // SAFETY: the slot memory is pinned by the ring protocol
        // between `try_recv` and `Drop`. The `Arc<RawBatchRing>` field
        // keeps the backing storage alive for the slice lifetime.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// Raw pointer into the slot buffer. Used by the journal stage's
    /// io_uring Write submission — the kernel reads directly from this
    /// address, so the slot must remain pinned until the CQE lands.
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Number of valid journal bytes in the slot.
    pub fn len(&self) -> usize {
        self.len
    }

    /// True when the slot contains no journal bytes. Included so
    /// `self.len()` can be paired with `self.is_empty()` without
    /// tripping clippy's `len_without_is_empty` lint.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for RawBatchSlot {
    fn drop(&mut self) {
        // Release the slot back to the sender. SPSC: we are the sole
        // consumer, so a plain load + store on `tail` is correct; the
        // Release ordering pairs with the sender's Acquire load on
        // `tail` in its backpressure check.
        let tail = self.ring.tail.get().load(Ordering::Relaxed);
        self.ring.tail.get().store(tail + 1, Ordering::Release);
    }
}

/// Receiver half of the bounded SPSC ring. Owned by the journal stage
/// thread.
pub struct RawBatchReceiver {
    ring: Arc<RawBatchRing>,
}

impl RawBatchReceiver {
    /// Try to receive a raw batch without blocking. Returns `None` if
    /// the ring is empty. The returned [`RawBatchSlot`] pins the slot
    /// until dropped.
    ///
    /// # Single-outstanding contract
    ///
    /// Callers must drop the previous slot before calling `try_recv`
    /// again. Violating this is **not** memory-unsafe — the sender's
    /// backpressure keeps the slot pinned as long as any handle exists
    /// — but it causes silent data loss: the second call returns the
    /// *same* slot (both reads see the pre-drop `tail`), and dropping
    /// both handles advances `tail` twice, skipping the next batch
    /// entirely. The journal stage enforces the invariant structurally
    /// via its `Option<InflightRaw>` state machine — there is only one
    /// legitimate caller and it cannot violate the contract.
    pub fn try_recv(&self) -> Option<RawBatchSlot> {
        let tail = self.ring.tail.get().load(Ordering::Relaxed);
        let head = self.ring.head.get().load(Ordering::Acquire);
        if tail >= head {
            return None;
        }
        let idx = (tail & RAW_RING_MASK) as usize;
        // SAFETY: the Acquire load on `head` pairs with the sender's
        // Release store, so the slot's byte buffer and metadata writes
        // are visible. The sender's backpressure check ensures slot
        // `idx` is not touched again until we advance `tail` via
        // `RawBatchSlot::drop`.
        let (ptr, meta) = unsafe {
            let slot = &*self.ring.slots[idx].get();
            let meta = *self.ring.metas[idx].get();
            (slot.as_ptr(), meta)
        };
        Some(RawBatchSlot {
            ring: Arc::clone(&self.ring),
            ptr,
            len: meta.len as usize,
            end_sequence: meta.end_sequence,
            chain_hash: meta.chain_hash,
            entry_count: meta.entry_count,
        })
    }
}

/// Create a bounded SPSC ring for raw journal batch transfer.
pub fn raw_batch_channel() -> (RawBatchSender, RawBatchReceiver) {
    let ring = Arc::new(RawBatchRing::new());
    (
        RawBatchSender {
            ring: Arc::clone(&ring),
        },
        RawBatchReceiver { ring },
    )
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
            busy_spin,
            raw_journal_rx: None,
            utilization: Arc::new(StageUtilization::new()),
        }
    }

    /// Shared utilization counters for health endpoint monitoring.
    pub fn utilization(&self) -> Arc<StageUtilization> {
        Arc::clone(&self.utilization)
    }

    /// Set the raw journal receiver for replica mode. When set, the journal
    /// stage writes pre-encoded bytes from the replication receiver instead
    /// of encoding events from the disruptor.
    pub fn set_raw_journal_receiver(&mut self, rx: RawBatchReceiver) {
        self.raw_journal_rx = Some(rx);
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

    /// Run the journal stage loop.
    ///
    /// Dispatches to the io_uring overlapped path for journal writes.
    /// With `no-fsync` or `no-persist` features, falls back to
    /// synchronous writes (io_uring overlapping is only useful with fsync).
    ///
    /// Returns the `JournalWriter` on shutdown for clean resource release.
    pub fn run(
        self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<JournalWriter, JournalError> {
        let use_uring = !cfg!(feature = "no-fsync") && !cfg!(feature = "no-persist");

        if self.raw_journal_rx.is_some() {
            // Replica mode: write raw bytes from the replication receiver.
            if use_uring {
                return self.run_replica_uring(shutdown);
            } else {
                return self.run_replica(shutdown);
            }
        }

        if use_uring {
            self.run_uring(shutdown)
        } else {
            self.run_sync(shutdown)
        }
    }

    /// Synchronous journal loop: `pwritev2+RWF_DSYNC` blocks until durable.
    ///
    /// Uses `read_batch` + `commit` (not `consume_batch`) to ensure the
    /// journal cursor is only advanced **after** the write is durable.
    /// The response stage checks this cursor before sending — this is
    /// the persist-before-ack boundary.
    fn run_sync(
        mut self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<JournalWriter, JournalError> {
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

        let mut busy_count: u64 = 0;
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

            if count > 0 {
                idle_spins = 0;
                busy_count += 1;

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
                // One clock_gettime per batch instead of per event. Events
                // within a batch share a timestamp — ordering is preserved by
                // sequence numbers. If per-event wall-clock timestamps become
                // a regulatory requirement (MiFID II, SEC CAT), revert to
                // batch_append() which calls clock_gettime per event.
                #[cfg(not(feature = "no-persist"))]
                {
                    let ts = crate::journal::writer::wall_clock_nanos();
                    for slot in &batch[..count] {
                        if matches!(slot.event, JournalEvent::QueryStats) {
                            continue;
                        }
                        self.writer
                            .batch_append_with_ts(&slot.event, ts, slot.key_hash, slot.request_seq)
                            .map_err(|e| {
                                JournalError::Io(std::io::Error::other(format!(
                                    "journal encode (run_sync, seq {}): {e}",
                                    slot.request_seq
                                )))
                            })?;
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
                let should_sync = pending >= self.max_batch
                    || delay.is_zero()
                    || first_write_ts.is_some_and(|ts| ts.elapsed() >= delay);
                #[cfg(feature = "no-fsync")]
                let should_sync = true;

                if should_sync {
                    #[cfg(not(feature = "no-persist"))]
                    {
                        // Snapshot batch bytes for replication BEFORE flush
                        // (flush clears the buffer). Copies into a pre-allocated
                        // ring slot — no heap allocation.
                        // Only when persistence is enabled — with no-persist,
                        // batch_buf is never cleared and would grow unbounded.
                        // Guard: skip entirely in standalone mode (both producers None).
                        // One field read — no atomics, no function call on the hot path.
                        if self.repl.producers[0].is_some() || self.repl.producers[1].is_some() {
                            let bytes = self.writer.pending_batch_bytes();
                            if !bytes.is_empty() {
                                let end_seq = self.writer.next_sequence() - 1;
                                Self::publish_to_replication_rings(
                                    &mut self.repl.producers,
                                    &self.repl.evict,
                                    &self.repl.active,
                                    bytes,
                                    end_seq,
                                    [0u8; 32],
                                    pending as u32,
                                );
                            }
                        }

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

                    self.consumer.commit(pending);
                    self.publish_chain_hash();

                    pending = 0;
                    #[cfg(not(feature = "no-fsync"))]
                    {
                        first_write_ts = None;
                    }
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
        }
    }

    /// Replica journal loop: writes pre-encoded bytes from the replication
    /// receiver while consuming (and discarding) events from the disruptor
    /// to advance the cursor. The cursor advance happens AFTER `write_raw_sync`
    /// completes, preserving the persist-before-ack guarantee.
    fn run_replica(
        mut self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<JournalWriter, JournalError> {
        let rx = self.raw_journal_rx.take().ok_or_else(|| {
            JournalError::Io(std::io::Error::other(
                "run_replica called without raw_journal_rx",
            ))
        })?;
        let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];
        let mut idle_spins: u32 = 0;
        let mut busy_count: u64 = 0;
        let mut idle_count: u64 = 0;

        loop {
            if shutdown.load(Ordering::Relaxed) {
                // Drain remaining disruptor events before shutdown.
                self.drain_remaining(&mut batch);
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                return Ok(self.writer);
            }

            // Try to receive a raw journal batch from the replication
            // receiver via the lock-free hand-off. The returned slot
            // borrows directly from the ring's pre-allocated buffer —
            // no Vec ownership transfer, no intermediate copy. Dropped
            // at the end of this arm to release the slot back.
            if let Some(raw_slot) = rx.try_recv() {
                idle_spins = 0;
                busy_count += 1;

                // Write raw bytes to journal (durable write) straight
                // from the slot memory. write_raw_sync takes `&[u8]`,
                // so this is genuinely zero-copy at the user-space
                // boundary — the kernel's pwritev2 reads from the slot.
                self.writer
                    .write_raw_sync(raw_slot.bytes(), raw_slot.entry_count as u64)
                    .map_err(|e| {
                        JournalError::Io(std::io::Error::other(format!(
                            "replica journal write_raw_sync (end_seq {}): {e}",
                            raw_slot.end_sequence
                        )))
                    })?;

                self.publish_chain_hash();

                // Consume the corresponding events from the disruptor to
                // advance the cursor. The receiver published exactly
                // `entry_count` events before sending this raw batch.
                let mut remaining = raw_slot.entry_count as usize;
                while remaining > 0 {
                    let count = self
                        .consumer
                        .read_batch(&mut batch, remaining.min(MAX_JOURNAL_BATCH));
                    if count > 0 {
                        // Discard events — they were already encoded in
                        // the slot bytes by the primary.
                        self.consumer.commit(count);
                        remaining -= count;
                    } else {
                        // Events not yet published to the disruptor by
                        // the receiver thread — spin briefly.
                        std::hint::spin_loop();
                    }
                }
                // `raw_slot` drops here, advancing the raw-batch ring's
                // `tail` and releasing the slot back to the sender.
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

    /// Replica journal loop with io_uring overlapped writes. Same
    /// double-buffering as the primary's `run_uring`: submits an async
    /// Write+RWF_DSYNC while receiving the next batch from the SPSC
    /// ring. Advances the cursor only after the CQE confirms durability.
    ///
    /// This overlaps NVMe writes with TCP receives, eliminating the
    /// serialized write_raw_sync + cursor wait bottleneck.
    /// Replica journal loop using io_uring overlapped writes.
    ///
    /// Receives pre-encoded journal batches from the replication handler
    /// via a bounded SPSC ring and submits Write+RWF_DSYNC SQEs
    /// asynchronously. The cursor advances AFTER the CQE confirms
    /// durability, preserving the persist-before-ack guarantee.
    ///
    /// The SPSC ring decouples the replication handler's TCP receive
    /// loop from the NVMe write latency: the handler can push up to
    /// 8 batches ahead while previous writes are in flight.
    fn run_replica_uring(
        mut self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<JournalWriter, JournalError> {
        use io_uring::{IoUring, opcode, types};

        let rx = self.raw_journal_rx.take().ok_or_else(|| {
            JournalError::Io(std::io::Error::other(
                "run_replica_uring called without raw_journal_rx",
            ))
        })?;
        let mut batch = [InputSlot::default(); MAX_JOURNAL_BATCH];
        let mut idle_spins: u32 = 0;
        let mut busy_count: u64 = 0;
        let mut idle_count: u64 = 0;

        // Set up io_uring — same as the primary's run_uring.
        let mut ring: IoUring = IoUring::builder()
            .setup_single_issuer()
            .build(8)
            .map_err(|e| JournalError::Io(std::io::Error::other(format!("io_uring init: {e}"))))?;

        let raw_fd = self.writer.fd();
        ring.submitter().register_files(&[raw_fd]).map_err(|e| {
            JournalError::Io(std::io::Error::other(format!(
                "io_uring register_files: {e}"
            )))
        })?;

        // Pin io-wq workers to core 0.
        {
            let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
            unsafe { libc::CPU_SET(0, &mut cpuset) };
            ring.submitter().register_iowq_aff(&cpuset).map_err(|e| {
                JournalError::Io(std::io::Error::other(format!(
                    "io_uring register_iowq_aff: {e}"
                )))
            })?;
        }

        // In-flight state. The slot stays pinned in the raw-batch ring
        // until it is dropped from this field; that drop releases the
        // slot's buffer back to the replication receiver thread.
        struct InflightRaw {
            slot: super::pipeline::RawBatchSlot,
            entry_count: usize,
        }
        let mut inflight: Option<InflightRaw> = None;

        loop {
            // --- Shutdown ---
            if shutdown.load(Ordering::Relaxed) {
                if let Some(inf) = inflight.take() {
                    self.wait_for_cqe(&mut ring, inf.slot.len());
                    self.consume_disruptor_events(&mut batch, inf.entry_count);
                    // `inf.slot` drops at end of scope, releasing it.
                }
                self.drain_remaining(&mut batch);
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                return Ok(self.writer);
            }

            // --- Reap CQE (non-blocking) ---
            if let Some(ref inf) = inflight
                && let Some(cqe) = ring.completion().next()
            {
                let result = cqe.result();
                if result < 0 {
                    return Err(JournalError::Io(std::io::Error::other(format!(
                        "replica journal io_uring write returned errno {}",
                        -result
                    ))));
                } else if (result as usize) != inf.slot.len() {
                    return Err(JournalError::Io(std::io::Error::other(format!(
                        "replica journal short write ({} of {} bytes)",
                        result,
                        inf.slot.len()
                    ))));
                }

                // Safe: the `if let Some(ref inf)` guard ensures
                // `inflight` is `Some` here.
                let completed = inflight.take().expect("checked by if-let");
                self.consume_disruptor_events(&mut batch, completed.entry_count);
                self.publish_chain_hash();
                // `completed.slot` drops here — releases the raw-batch
                // ring slot back to the replication receiver. The
                // receiver can now reuse that slot for the next batch.
                drop(completed);
            }

            // --- Submit next batch (if nothing in-flight) ---
            if inflight.is_none() {
                if let Some(raw_slot) = rx.try_recv() {
                    idle_spins = 0;
                    busy_count += 1;
                    let entry_count = raw_slot.entry_count as usize;
                    let len = raw_slot.len();

                    // Reserve the file offset and eagerly advance the
                    // writer's `write_pos` / `next_sequence`. The CQE
                    // will confirm durability; only then do we advance
                    // the journal cursor.
                    let offset = self
                        .writer
                        .reserve_raw_async_write(len as u64, raw_slot.entry_count as u64)
                        .map_err(|e| {
                            JournalError::Io(std::io::Error::other(format!(
                                "replica journal reserve_raw_async_write (end_seq {}): {e}",
                                raw_slot.end_sequence
                            )))
                        })?;

                    // Submit the Write SQE with a raw pointer straight
                    // into the raw-batch ring slot. The slot is pinned
                    // by `raw_slot` / `inflight` until the CQE lands,
                    // so the kernel reads stable memory.
                    let sqe = opcode::Write::new(types::Fixed(0), raw_slot.as_ptr(), len as u32)
                        .offset(offset)
                        .rw_flags(libc::RWF_DSYNC)
                        .build()
                        .user_data(1);

                    unsafe {
                        ring.submission().push(&sqe).expect("SQ full");
                    }
                    ring.submit().expect("io_uring submit failed");

                    inflight = Some(InflightRaw {
                        slot: raw_slot,
                        entry_count,
                    });
                } else {
                    idle_count += 1;
                    if idle_count.is_multiple_of(1024) {
                        self.utilization.busy.store(busy_count, Ordering::Relaxed);
                        self.utilization.idle.store(idle_count, Ordering::Relaxed);
                    }
                    idle_wait(&mut idle_spins, self.busy_spin);
                }
            } else {
                // Write in-flight — don't submit another, just idle-wait
                // for the CQE. The NVMe is busy; trying to submit more
                // would just queue in the kernel.
                idle_count += 1;
                if idle_count.is_multiple_of(1024) {
                    self.utilization.busy.store(busy_count, Ordering::Relaxed);
                    self.utilization.idle.store(idle_count, Ordering::Relaxed);
                }
                idle_wait(&mut idle_spins, self.busy_spin);
            }
        }
    }

    /// Consume exactly `count` events from the input disruptor,
    /// advancing the cursor. Used by the replica after an async raw
    /// write completes.
    fn consume_disruptor_events(&mut self, batch: &mut [InputSlot], mut remaining: usize) {
        while remaining > 0 {
            let count = self
                .consumer
                .read_batch(batch, remaining.min(MAX_JOURNAL_BATCH));
            if count > 0 {
                self.consumer.commit(count);
                remaining -= count;
            } else {
                std::hint::spin_loop();
            }
        }
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
        chain_hash: [u8; 32],
        entry_count: u32,
    ) {
        for i in 0..2 {
            if let Some(ref mut producer) = producers[i] {
                if !active_flags[i].load(Ordering::Relaxed) {
                    continue;
                }
                if evict_flags[i].load(Ordering::Relaxed) {
                    continue;
                }
                if producer
                    .try_publish(bytes, end_seq, chain_hash, entry_count)
                    .is_err()
                {
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
    #[inline]
    fn publish_chain_hash(&self) {
        if let Some(ref lock) = self.chain_hash
            && let Some(hash) = self.writer.chain_hash()
        {
            lock.store(hash);
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
                let ts = crate::journal::writer::wall_clock_nanos();
                for slot in &batch[..count] {
                    if matches!(slot.event, JournalEvent::QueryStats) {
                        continue;
                    }
                    if let Err(e) = self.writer.batch_append_with_ts(
                        &slot.event,
                        ts,
                        slot.key_hash,
                        slot.request_seq,
                    ) {
                        tracing::error!(error = %e, "journal encode error on drain");
                    }
                }

                // Snapshot for replication before flush.
                if self.repl.producers[0].is_some() || self.repl.producers[1].is_some() {
                    let bytes = self.writer.pending_batch_bytes();
                    if !bytes.is_empty() {
                        let end_seq = self.writer.next_sequence() - 1;
                        let chain = self.writer.chain_hash().unwrap_or([0u8; 32]);
                        Self::publish_to_replication_rings(
                            &mut self.repl.producers,
                            &self.repl.evict,
                            &self.repl.active,
                            bytes,
                            end_seq,
                            chain,
                            count as u32,
                        );
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
    fn run_uring(
        mut self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<JournalWriter, JournalError> {
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
        let mut inflight: Option<(super::writer::AsyncWriteBatch, u64)> = None;

        let mut busy_count: u64 = 0;
        let mut idle_count: u64 = 0;

        loop {
            // --- Check shutdown ---
            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                // Wait for in-flight write to complete.
                if let Some((batch_data, seq)) = inflight.take() {
                    self.wait_for_cqe(&mut ring, batch_data.buf.len());
                    self.consumer.set_progress(seq);
                    self.publish_chain_hash();
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

            if count > 0 {
                idle_spins = 0;
                busy_count += 1;

                let ts = crate::journal::writer::wall_clock_nanos();
                for slot in &batch[..count] {
                    if matches!(slot.event, JournalEvent::QueryStats) {
                        continue;
                    }
                    self.writer
                        .batch_append_with_ts(&slot.event, ts, slot.key_hash, slot.request_seq)
                        .map_err(|e| {
                            JournalError::Io(std::io::Error::other(format!(
                                "journal encode (run_uring, seq {}): {e}",
                                slot.request_seq
                            )))
                        })?;
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
                        self.wait_for_cqe(&mut ring, batch_data.buf.len());
                        self.consumer.set_progress(seq);
                        self.publish_chain_hash();
                        self.writer.confirm_async_write(batch_data);
                    }

                    // Snapshot batch bytes for replication BEFORE
                    // take_batch_for_async_write (which swaps the buffer).
                    if self.repl.producers[0].is_some() || self.repl.producers[1].is_some() {
                        let bytes = self.writer.pending_batch_bytes();
                        if !bytes.is_empty() {
                            let end_seq = self.writer.next_sequence() - 1;
                            let chain = self.writer.chain_hash().unwrap_or([0u8; 32]);
                            Self::publish_to_replication_rings(
                                &mut self.repl.producers,
                                &self.repl.evict,
                                &self.repl.active,
                                bytes,
                                end_seq,
                                chain,
                                pending as u32,
                            );
                        }
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
    fn wait_for_cqe(&self, ring: &mut io_uring::IoUring, expected_len: usize) {
        loop {
            if let Some(cqe) = ring.completion().next() {
                let result = cqe.result();
                if result < 0 {
                    tracing::error!(errno = -result, "io_uring journal write failed (drain)");
                } else if (result as usize) != expected_len {
                    tracing::error!(
                        written = result,
                        expected = expected_len,
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
    output: ring::Producer<OutputSlot>,
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
}

impl MatchingStage {
    /// Create a new matching stage.
    pub fn new(
        exchange: Exchange,
        consumer: ring::Consumer<InputSlot>,
        output: ring::Producer<OutputSlot>,
        events_processed: Arc<AtomicU64>,
        journal_cursor: Arc<Sequence>,
        active_connections: Arc<AtomicU64>,
        replicas_connected: Option<Arc<AtomicU32>>,
        busy_spin: bool,
    ) -> Self {
        Self {
            exchange,
            consumer,
            output,
            events_processed,
            journal_cursor,
            active_connections,
            replicas_connected,
            busy_spin,
            utilization: Arc::new(StageUtilization::new()),
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

    /// Extract the order ID from the event for reject reports, or OrderId(0) if N/A.
    fn extract_order_id(event: &JournalEvent) -> OrderId {
        match event {
            JournalEvent::SubmitOrder { order, .. } => order.id,
            JournalEvent::CancelOrder { order_id, .. }
            | JournalEvent::CancelReplace { order_id, .. } => *order_id,
            _ => OrderId(0),
        }
    }

    /// Extract the account ID from the event for reject reports, or AccountId(0) if N/A.
    fn extract_account_id(event: &JournalEvent) -> AccountId {
        match event {
            JournalEvent::SubmitOrder { order, .. } => order.account,
            JournalEvent::CancelOrder { account, .. }
            | JournalEvent::CancelAll { account }
            | JournalEvent::CancelReplace { account, .. }
            | JournalEvent::Deposit { account, .. }
            | JournalEvent::Withdraw { account, .. }
            | JournalEvent::ProvisionAccount { account, .. } => *account,
            _ => AccountId(0),
        }
    }

    /// Extract the symbol from the event for reject reports, or Symbol(0) if N/A.
    fn extract_symbol(event: &JournalEvent) -> Symbol {
        match event {
            JournalEvent::SubmitOrder { symbol, .. }
            | JournalEvent::CancelOrder { symbol, .. }
            | JournalEvent::CancelReplace { symbol, .. }
            | JournalEvent::SetRiskLimits { symbol, .. }
            | JournalEvent::SetCircuitBreaker { symbol, .. }
            | JournalEvent::SetFeeSchedule { symbol, .. } => *symbol,
            _ => Symbol(0),
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
        // once per batch, on QueryStats, and on shutdown.
        let mut local_events: u64 = 0;

        let mut batch = [InputSlot::default(); MAX_MATCHING_BATCH];

        let mut busy_count: u64 = 0;
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
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
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
                idle_count += 1;
                if idle_count.is_multiple_of(1024) {
                    self.utilization.busy.store(busy_count, Ordering::Relaxed);
                    self.utilization.idle.store(idle_count, Ordering::Relaxed);
                }
                idle_wait(&mut idle_spins, self.busy_spin);
                continue;
            }
            idle_spins = 0;

            for (i, slot) in batch[..count].iter().enumerate() {
                let input_seq = batch_start + i as u64;
                busy_count += 1;

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

                    // Flush thread-local counters so the snapshot is current.
                    self.events_processed.store(local_events, Ordering::Relaxed);
                    self.utilization.busy.store(busy_count, Ordering::Relaxed);
                    self.utilization.idle.store(idle_count, Ordering::Relaxed);

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

                // Halt check first: reject before advancing any HWMs so the
                // client can safely retry the same seq after reconnect.
                if self.is_halted() {
                    reports.push(ExecutionReport::Rejected {
                        order_id: Self::extract_order_id(&slot.event),
                        symbol: Self::extract_symbol(&slot.event),
                        account: Self::extract_account_id(&slot.event),
                        reason: RejectReason::ReplicaDisconnected,
                    });
                } else if !self
                    .exchange
                    .check_request_seq(slot.key_hash, slot.request_seq)
                {
                    // Duplicate request — produce Rejected response.
                    // Use OrderId(0) and AccountId(0) as placeholders since
                    // we don't parse the specific order/account from the slot.
                    reports.push(ExecutionReport::Rejected {
                        order_id: OrderId(0),
                        symbol: Symbol(0),
                        account: AccountId(0),
                        reason: RejectReason::DuplicateRequest,
                    });
                } else {
                    self.process_event(slot, &mut reports);
                }

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

            // Flush the thread-local counter once per batch so the health
            // endpoint can observe progress. One Relaxed store per batch
            // (~1ns) is negligible compared to per-event atomic increment.
            self.events_processed.store(local_events, Ordering::Relaxed);
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

            // Halt check first, then dedup (same order as the main run loop).
            if self.is_halted() {
                reports.push(ExecutionReport::Rejected {
                    order_id: Self::extract_order_id(&slot.event),
                    symbol: Self::extract_symbol(&slot.event),
                    account: Self::extract_account_id(&slot.event),
                    reason: RejectReason::ReplicaDisconnected,
                });
            } else if !self
                .exchange
                .check_request_seq(slot.key_hash, slot.request_seq)
            {
                reports.push(ExecutionReport::Rejected {
                    order_id: OrderId(0),
                    symbol: Symbol(0),
                    account: AccountId(0),
                    reason: RejectReason::DuplicateRequest,
                });
            } else {
                self.process_event(&slot, reports);
            }

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
            JournalEvent::EndOfDay => {
                self.exchange.end_of_day(reports);
            }
            JournalEvent::ExpireOrders { timestamp_ns } => {
                self.exchange.expire_orders(timestamp_ns, reports);
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
            JournalEvent::ProvisionAccount { account, amount } => {
                self.exchange.provision_account(account, amount);
            }
            JournalEvent::Withdraw {
                account,
                currency,
                amount,
            } => {
                // Replay path: rejections (insufficient balance, resting
                // orders, unknown account) are deterministic — they
                // reproduce the original live outcome and were already
                // surfaced to the client at the time. Discarding here is
                // intentional and safe.
                let _ = self.exchange.withdraw(account, currency, amount);
            }
            JournalEvent::DisableInstrument { symbol } => {
                self.exchange.disable_instrument(symbol, reports);
            }
            JournalEvent::EnableInstrument { symbol } => {
                self.exchange.enable_instrument(symbol, reports);
            }
            JournalEvent::RemoveInstrument { symbol } => {
                self.exchange.remove_instrument(symbol, reports);
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
/// **Topology**: journal, matching, and (optionally) replication consumers
/// are all gated on the producer (parallel). The matching stage does NOT
/// wait for journal sync — the response stage gates on the journal cursor
/// (and replication cursor when active) instead.
///
/// The caller (server) is responsible for building the response stage
/// and spawning all threads.
///
/// When `enable_replication` is true, a 3rd consumer is added for the
/// replication stage. The returned `Option<ReplicationStage>` and
/// `Arc<AtomicU64>` (replication cursor) are used by the server to spawn
/// the replication thread and gate the response stage.
#[allow(clippy::type_complexity)]
pub fn build_pipeline(
    exchange: Exchange,
    writer: JournalWriter,
    group_commit_delay: Duration,
    active_connections: Arc<AtomicU64>,
) -> (
    ring::MultiProducer<InputSlot>,
    JournalStage,
    MatchingStage,
    Vec<ring::Consumer<OutputSlot>>,
    Arc<Sequence>,
    Arc<AtomicU64>,
) {
    let (
        producer,
        journal_stage,
        matching_stage,
        output_consumers,
        journal_cursor,
        _matching_cursor,
        events_processed,
        _input_cursor,
        _,
        _,
        _,
        _,
        _,
        _,
    ) = build_pipeline_with_replication(
        exchange,
        writer,
        group_commit_delay,
        active_connections,
        false,
        MAX_JOURNAL_BATCH,
        crate::journal::replication::REPLICATION_RING_CAPACITY,
        false,
        false,
        false,
    );
    (
        producer,
        journal_stage,
        matching_stage,
        output_consumers,
        journal_cursor,
        events_processed,
    )
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
    /// Producer cursors (one per independent ring).
    pub producer_cursors: Vec<Box<dyn ring::QueueCursor>>,
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

/// When replication is disabled, the cursor is `u64::MAX` (standalone mode).
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub fn build_pipeline_with_replication(
    exchange: Exchange,
    writer: JournalWriter,
    group_commit_delay: Duration,
    active_connections: Arc<AtomicU64>,
    enable_replication: bool,
    max_journal_batch: usize,
    replication_ring_size: usize,
    busy_spin: bool,
    enable_event_publisher: bool,
    enable_shadow: bool,
) -> (
    ring::MultiProducer<InputSlot>,
    JournalStage,
    MatchingStage,
    Vec<ring::Consumer<OutputSlot>>,
    Arc<Sequence>,
    Arc<Sequence>,
    Arc<AtomicU64>,
    Box<dyn ring::QueueCursor>,
    Option<(ReplicationConsumer, ReplicationConsumer)>,
    Arc<AtomicU64>,
    Option<Arc<AtomicU32>>,
    Option<ring::Consumer<InputSlot>>,
    Option<Arc<SeqLock<[u8; 32]>>>,
    Option<ReplicationRingProgress>,
) {
    // Input disruptor: N producers (reader threads), 2+ parallel consumers.
    // MultiProducer allows lock-free concurrent publishing from all reader
    // threads, eliminating the Mutex that previously serialized access.
    // When shadow snapshots are enabled, a third consumer is chained after
    // journal (consumer 0) — it only sees events that have been durably fsynced.
    let mut builder = ring::DisruptorBuilder::<InputSlot>::new(INPUT_RING_CAPACITY)
        .add_consumer() // consumer 0: journal, gated on producer
        .add_consumer(); // consumer 1: matching, gated on producer (parallel)
    if enable_shadow {
        builder = builder.add_consumer_after(0); // consumer 2: shadow, gated on journal
    }
    let (input_producer, mut consumers) = builder.build_multi_producer();

    // Type-erased cursor reader for queue depth monitoring.
    // Extracted before the producer is cloned to reader threads.
    let input_cursor = input_producer.cursor_reader();

    // Pop consumers in reverse order of addition. With shadow enabled the
    // build order is [journal(0), matching(1), shadow(2)], so pop yields:
    // shadow(2), matching(1), journal(0).
    let shadow_consumer = if enable_shadow {
        Some(consumers.pop().expect("shadow consumer"))
    } else {
        None
    };
    let matching_consumer = consumers.pop().expect("matching consumer");
    let journal_consumer = consumers.pop().expect("journal consumer");

    // Grab the journal's progress cursor before moving it into the stage.
    // The response stage will read this to gate on sync completion.
    let journal_cursor = journal_consumer.progress_counter();
    // Grab the matching consumer's progress cursor for seed drain gating.
    // The server waits for both journal and matching to advance past the
    // last seed sequence before accepting clients.
    let matching_cursor = matching_consumer.progress_counter();

    // Output disruptor ring: matching → response (+ optional event publisher).
    // Single producer, N consumers (1 = response only, 2 = response + event publisher).
    // Uses the same ring::Producer/Consumer API as the input disruptor.
    let mut output_builder =
        ring::DisruptorBuilder::<OutputSlot>::new(OUTPUT_RING_CAPACITY).add_consumer(); // consumer 0: response stage
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
            crate::journal::replication::build_replication_ring(1, replication_ring_size);
        let (producer_1, mut consumers_1) =
            crate::journal::replication::build_replication_ring(1, replication_ring_size);

        let evict_flags = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        let active_flags = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];

        let ring_progress = ReplicationRingProgress {
            producer_cursors: vec![producer_0.cursor_reader(), producer_1.cursor_reader()],
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

    // SeqLock for publishing the BLAKE3 chain hash to the shadow snapshot
    // stage. Allocated only when shadow is enabled — zero overhead otherwise.
    // Initialized to all-zeros; the journal stage writes the real hash after
    // the first fsync batch.
    let chain_hash_lock = if enable_shadow {
        let lock = Arc::new(SeqLock::new([0u8; 32]));
        journal_stage.set_chain_hash_lock(Arc::clone(&lock));
        Some(lock)
    } else {
        None
    };

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

    let matching_stage = MatchingStage::new(
        exchange,
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

    (
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
    )
}

/// Build a pipeline for replica mode. Same disruptor stages as the primary
/// (journal → matching → shadow), but:
/// - No replication ring (this IS the replica)
/// - No `replicas_connected` halt check
/// - Journal stage uses raw-write mode (pre-encoded bytes from primary)
/// - Output disruptor has a single drain consumer (no response stage)
///
/// Returns the input producer (for the replication receiver), the pipeline
/// stages, the journal cursor (for ack gating), and a `RawBatchSender`
/// for sending raw journal batches to the journal stage.
#[allow(clippy::type_complexity)]
pub fn build_replica_pipeline(
    exchange: Exchange,
    writer: JournalWriter,
    max_journal_batch: usize,
    busy_spin: bool,
    enable_shadow: bool,
) -> (
    ring::MultiProducer<InputSlot>,
    JournalStage,
    MatchingStage,
    ring::Consumer<OutputSlot>,
    Arc<Sequence>,
    Arc<Sequence>,
    RawBatchSender,
    Option<ring::Consumer<InputSlot>>,
    Option<Arc<SeqLock<[u8; 32]>>>,
) {
    // Input disruptor: same topology as primary (journal + matching in parallel,
    // optional shadow gated on journal).
    let mut builder = ring::DisruptorBuilder::<InputSlot>::new(INPUT_RING_CAPACITY)
        .add_consumer() // consumer 0: journal
        .add_consumer(); // consumer 1: matching (parallel)
    if enable_shadow {
        builder = builder.add_consumer_after(0); // consumer 2: shadow, gated on journal
    }
    let (input_producer, mut consumers) = builder.build_multi_producer();

    let shadow_consumer = if enable_shadow {
        Some(consumers.pop().expect("shadow consumer"))
    } else {
        None
    };
    let matching_consumer = consumers.pop().expect("matching consumer");
    let journal_consumer = consumers.pop().expect("journal consumer");

    let journal_cursor = journal_consumer.progress_counter();
    let matching_cursor = matching_consumer.progress_counter();

    // Output disruptor: single drain consumer (no response stage on replica).
    let output_builder =
        ring::DisruptorBuilder::<OutputSlot>::new(OUTPUT_RING_CAPACITY).add_consumer();
    let (output_producer, mut output_consumers) = output_builder.build();
    let drain_consumer = output_consumers.pop().expect("drain consumer");

    let events_processed = Arc::new(AtomicU64::new(0));

    // Journal stage in replica mode: raw-write via lock-free hand-off.
    // Single-slot (one batch in flight). The receiver thread spins on
    // send if the journal stage hasn't consumed the previous batch yet
    // (backpressure). Lock-free to avoid starvation when the journal
    // stage busy-spins on try_recv.
    let (raw_tx, raw_rx) = raw_batch_channel();
    let mut journal_stage = JournalStage::new(
        writer,
        journal_consumer,
        Duration::ZERO, // no group commit delay in replica mode
        max_journal_batch,
        busy_spin,
    );
    journal_stage.set_raw_journal_receiver(raw_rx);

    // Chain hash SeqLock for shadow snapshots.
    let chain_hash_lock = if enable_shadow {
        let lock = Arc::new(SeqLock::new([0u8; 32]));
        journal_stage.set_chain_hash_lock(Arc::clone(&lock));
        Some(lock)
    } else {
        None
    };

    // Matching stage: same as primary but with no replicas_connected check
    // (None = standalone mode, never halts on replica disconnect).
    let active_connections = Arc::new(AtomicU64::new(0));
    let matching_stage = MatchingStage::new(
        exchange,
        matching_consumer,
        output_producer,
        Arc::clone(&events_processed),
        Arc::clone(&journal_cursor),
        active_connections,
        None, // no replicas_connected halt check on replica
        busy_spin,
    );

    (
        input_producer,
        journal_stage,
        matching_stage,
        drain_consumer,
        journal_cursor,
        matching_cursor,
        raw_tx,
        shadow_consumer,
        chain_hash_lock,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::replication::REPLICATION_RING_CAPACITY;
    use crate::types::*;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::time::Duration;

    /// Return type for `start_matching_with_halt`:
    /// (input_producer, output_consumer, connected_counter, shutdown, join_handle).
    type MatchingHaltResult = (
        ring::Producer<InputSlot>,
        ring::Consumer<OutputSlot>,
        Arc<AtomicU32>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<Exchange>,
    );

    /// First user-event sequence: 2 with hash-chain (genesis takes 1), 1 without.
    #[cfg(feature = "hash-chain")]
    const FIRST_SEQ: u64 = 2;
    #[cfg(not(feature = "hash-chain"))]
    const FIRST_SEQ: u64 = 1;

    fn limit_order(id: u64, account: AccountId, side: Side, price: u64, qty: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(price).unwrap()),
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: Quantity(NonZeroU64::new(qty).unwrap()),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
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
        let stage = JournalStage::new(writer, consumer, Duration::ZERO, MAX_JOURNAL_BATCH, false);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
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
            key_hash: 0,
            request_seq: 0,
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

        let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<OutputSlot>::new(64)
            .add_consumer()
            .build();
        let mut output_consumer = output_consumers.pop().unwrap();

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
            None, // standalone — no halt check
            false,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        input_producer.publish(InputSlot {
            connection_id: 42,
            key_hash: 0,
            request_seq: 0,
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
            mut output_consumers,
            journal_cursor,
            _events_processed,
        ) = build_pipeline(exchange, writer, Duration::ZERO, active_conns);
        let mut output_consumer = output_consumers.pop().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s1 = Arc::clone(&shutdown);
        let s2 = Arc::clone(&shutdown);

        let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
        let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

        // Submit an order through the pipeline.
        input_producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
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

    #[test]
    fn journal_stage_sends_replication_batches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("repl_pipeline.journal");

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
            mut output_consumers,
            journal_cursor,
            _matching_cursor,
            _events_processed,
            _input_cursor,
            replication_rx,
            replication_cursor,
            _replicas_connected,
            _shadow_consumer,
            _chain_hash_lock,
            _ring_progress,
        ) = build_pipeline_with_replication(
            exchange,
            writer,
            Duration::ZERO,
            active_conns,
            true,
            MAX_JOURNAL_BATCH,
            REPLICATION_RING_CAPACITY,
            false,
            false,
            false,
        );
        let mut output_consumer = output_consumers.pop().unwrap();

        let (mut repl_consumer, _repl_consumer_2) =
            replication_rx.expect("replication should be enabled");

        // Simulate a connected replica so the matching stage doesn't halt
        // and the journal stage publishes to replication rings.
        if let Some(ref count) = _replicas_connected {
            count.store(1, Ordering::Relaxed);
        }
        if let Some(ref rp) = _ring_progress {
            rp.active_flags[0].store(true, Ordering::Relaxed);
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let s1 = Arc::clone(&shutdown);
        let s2 = Arc::clone(&shutdown);

        let t_journal = std::thread::spawn(move || journal_stage.run(&s1));
        let t_matching = std::thread::spawn(move || matching_stage.run(&s2));

        // Submit an order through the pipeline.
        input_producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(2), Side::Sell, 100, 50),
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        // Wait for the Placed report in the output SPSC (matching stage).
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

        // Wait for journal to confirm durability.
        loop {
            let cursor = journal_cursor.get().load(Ordering::Acquire);
            if cursor > output.input_seq {
                break;
            }
            std::hint::spin_loop();
        }

        // The journal stage should have published a replication batch with the
        // exact same bytes it wrote to disk. Spin-wait for it.
        let (repl_meta, repl_data) = loop {
            if let Some((meta, data)) = repl_consumer.try_read() {
                // Copy data out before commit releases the slot.
                let data_copy = data.to_vec();
                repl_consumer.commit();
                break (meta, data_copy);
            }
            std::hint::spin_loop();
        };
        assert!(
            repl_meta.end_sequence > 0,
            "replication batch should have events"
        );
        assert!(!repl_data.is_empty(), "replication batch should have data");
        #[cfg(feature = "hash-chain")]
        assert_ne!(
            repl_meta.chain_hash, [0u8; 32],
            "chain hash should be initialized"
        );

        // Verify the replication batch contains valid journal entries with
        // the same sequence numbers as the on-disk journal.
        let (consumed, seq, _ts, _kh, _rs, event) =
            crate::journal::codec::decode(&repl_data, crate::journal::codec::FORMAT_VERSION)
                .unwrap();
        assert!(consumed > 0);
        assert_eq!(
            seq, FIRST_SEQ,
            "replication sequence should match journal first user event"
        );
        assert!(matches!(event, JournalEvent::SubmitOrder { .. }));

        // Verify the replicated bytes are byte-identical to what's on disk.
        #[cfg(not(feature = "no-persist"))]
        {
            use crate::journal::codec::FILE_HEADER_SIZE;
            let file_bytes = std::fs::read(&path).unwrap();

            // Find the start of user entries (after file header and genesis if present).
            let offset = {
                #[cfg(feature = "hash-chain")]
                {
                    // Skip past the genesis entry.
                    let genesis_len = u16::from_le_bytes([
                        file_bytes[FILE_HEADER_SIZE + 2],
                        file_bytes[FILE_HEADER_SIZE + 3],
                    ]) as usize;
                    FILE_HEADER_SIZE + 20 + genesis_len + 4
                }
                #[cfg(not(feature = "hash-chain"))]
                {
                    FILE_HEADER_SIZE
                }
            };

            // Find end of valid data via reader.
            let mut reader = crate::journal::JournalReader::open(&path).unwrap();
            while reader.next_entry().unwrap().is_some() {}
            let data_end = reader.valid_file_end() as usize;

            let disk_bytes = &file_bytes[offset..data_end];
            assert_eq!(
                repl_data, disk_bytes,
                "replicated bytes must be byte-identical to journal file"
            );
        }

        // Simulate replica acking — update the replication cursor.
        replication_cursor.store(repl_meta.end_sequence + 1, Ordering::Release);

        // Verify dual-cursor gating: both cursors advanced.
        let journal_pos = journal_cursor.get().load(Ordering::Acquire);
        let repl_pos = replication_cursor.load(Ordering::Acquire);
        let effective = journal_pos.min(repl_pos);
        assert!(
            effective > output.input_seq,
            "both cursors should have advanced"
        );

        shutdown.store(true, Ordering::Relaxed);
        let _writer = t_journal.join().unwrap();
        let _exchange = t_matching.join().unwrap();
    }

    #[test]
    fn replication_cursor_always_starts_at_max() {
        // Cursor should be u64::MAX regardless of replication mode.
        // When disabled: no replica, no gating.
        // When enabled: server works before a replica connects; cursor
        // only engages when the replica sends its first ack.
        let dir = tempfile::tempdir().unwrap();

        // Standalone mode.
        {
            let path = dir.path().join("standalone.journal");
            let exchange = Exchange::new();
            let writer = JournalWriter::create(&path).unwrap();
            let active_conns = Arc::new(AtomicU64::new(0));

            let (_, _, _, _, _, _, _, _, replication, replication_cursor, _, _, _, _) =
                build_pipeline_with_replication(
                    exchange,
                    writer,
                    Duration::ZERO,
                    active_conns,
                    false,
                    MAX_JOURNAL_BATCH,
                    REPLICATION_RING_CAPACITY,
                    false,
                    false,
                    false,
                );
            assert!(replication.is_none());
            assert_eq!(replication_cursor.load(Ordering::Relaxed), u64::MAX);
        }

        // Replication enabled — cursor still starts at u64::MAX.
        {
            let path = dir.path().join("repl_enabled.journal");
            let exchange = Exchange::new();
            let writer = JournalWriter::create(&path).unwrap();
            let active_conns = Arc::new(AtomicU64::new(0));

            let (_, _, _, _, _, _, _, _, replication, replication_cursor, _, _, _, _) =
                build_pipeline_with_replication(
                    exchange,
                    writer,
                    Duration::ZERO,
                    active_conns,
                    true,
                    MAX_JOURNAL_BATCH,
                    REPLICATION_RING_CAPACITY,
                    false,
                    false,
                    false,
                );
            assert!(replication.is_some());
            assert_eq!(
                replication_cursor.load(Ordering::Relaxed),
                u64::MAX,
                "replication cursor should start at MAX even when enabled"
            );
        }
    }

    /// Helper: build a minimal matching stage with a replicas_connected counter.
    /// Returns (input_producer, output_consumer, connected_counter, shutdown, join_handle).
    fn start_matching_with_halt(initial_connected: u32) -> MatchingHaltResult {
        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);

        let (input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();
        let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<OutputSlot>::new(64)
            .add_consumer()
            .build();
        let output_consumer = output_consumers.pop().unwrap();

        let dummy_cursor = Arc::new(Sequence::new(AtomicU64::new(0)));
        let events_counter = Arc::new(AtomicU64::new(0));
        let active_conns = Arc::new(AtomicU64::new(0));
        let counter = Arc::new(AtomicU32::new(initial_connected));

        let stage = MatchingStage::new(
            exchange,
            consumer,
            output_producer,
            events_counter,
            dummy_cursor,
            active_conns,
            Some(Arc::clone(&counter)),
            false,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        (input_producer, output_consumer, counter, shutdown, handle)
    }

    /// Consume outputs until we see a BatchEnd, returning all reports.
    fn collect_reports(output: &mut ring::Consumer<OutputSlot>) -> Vec<ExecutionReport> {
        let mut reports = Vec::new();
        loop {
            if let Some((_, slot)) = output.try_consume() {
                match slot.payload {
                    OutputPayload::Report(r) => reports.push(r),
                    OutputPayload::BatchEnd => return reports,
                    _ => {}
                }
            }
            std::hint::spin_loop();
        }
    }

    #[test]
    fn halt_rejects_submit_order() {
        let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0xAA,
            request_seq: 1,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(100, AccountId(1), Side::Buy, 50, 10),
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                order_id: OrderId(100),
                account: AccountId(1),
                reason: RejectReason::ReplicaDisconnected,
                ..
            }
        ));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn halt_rejects_deposit() {
        let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            event: JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100,
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ReplicaDisconnected,
                ..
            }
        ));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn halt_allows_query_stats() {
        let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            event: JournalEvent::QueryStats,
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        // QueryStats produces StatsHeader + BatchEnd, not a Rejected.
        let mut got_stats = false;
        let mut got_batch_end = false;
        for _ in 0..1_000_000 {
            if let Some((_, slot)) = output.try_consume() {
                match slot.payload {
                    OutputPayload::StatsHeader { .. } => got_stats = true,
                    OutputPayload::BatchEnd => {
                        got_batch_end = true;
                        break;
                    }
                    OutputPayload::Report(ExecutionReport::Rejected { reason, .. }) => {
                        panic!("QueryStats should not be rejected, got: {reason:?}");
                    }
                    _ => {}
                }
            }
            std::hint::spin_loop();
        }
        assert!(got_stats, "should have received StatsHeader");
        assert!(got_batch_end, "should have received BatchEnd");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn halt_then_reconnect_resumes_trading() {
        let (mut input, mut output, flag, shutdown, handle) = start_matching_with_halt(0);

        // Submit while halted — rejected.
        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0xBB,
            request_seq: 1,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(200, AccountId(1), Side::Buy, 50, 10),
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output);
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ReplicaDisconnected,
                ..
            }
        ));

        // Reconnect replica.
        flag.store(1, Ordering::Relaxed);

        // Retry the same seq — should succeed now (HWM was not advanced).
        input.publish(InputSlot {
            connection_id: 1,
            key_hash: 0xBB,
            request_seq: 1,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(200, AccountId(1), Side::Buy, 50, 10),
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output);
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Placed { .. })),
            "order should be placed after reconnect, got: {reports:?}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn standalone_mode_no_halt() {
        // replicas_connected = None → no halt check, events always processed.
        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);

        let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();
        let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<OutputSlot>::new(64)
            .add_consumer()
            .build();
        let mut output_consumer = output_consumers.pop().unwrap();

        let stage = MatchingStage::new(
            exchange,
            consumer,
            output_producer,
            Arc::new(AtomicU64::new(0)),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(AtomicU64::new(0)),
            None, // standalone
            false,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        input_producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            event: JournalEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(1), Side::Buy, 50, 10),
            },
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });

        let reports = collect_reports(&mut output_consumer);
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Placed { .. })),
            "standalone mode should process normally, got: {reports:?}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    /// Journal stage in replica mode: writes raw bytes from a channel and
    /// advances the disruptor cursor only after the durable write.
    #[test]
    fn replica_journal_stage_writes_raw_bytes_and_advances_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let primary_path = dir.path().join("primary.journal");
        let replica_path = dir.path().join("replica.journal");

        // Write events to a primary journal to get raw bytes.
        let events = vec![
            JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(0),
                amount: 500,
            },
            JournalEvent::Deposit {
                account: AccountId(2),
                currency: CurrencyId(0),
                amount: 300,
            },
        ];

        let raw_bytes;
        let entry_count;
        {
            let mut writer = crate::journal::writer::JournalWriter::create(&primary_path).unwrap();
            for event in &events {
                writer.batch_append(event).unwrap();
            }
            raw_bytes = writer.pending_batch_bytes().to_vec();
            entry_count = events.len() as u32;
            writer.flush_batch_sync().unwrap();
        }

        // Set up replica pipeline components: disruptor + journal stage with
        // raw_journal_rx.
        let replica_writer = crate::journal::writer::JournalWriter::create(&replica_path).unwrap();

        let (input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build_multi_producer();
        let consumer = consumers.pop().unwrap();
        let journal_cursor = consumer.progress_counter();

        let (raw_tx, raw_rx) = raw_batch_channel();

        let mut stage = JournalStage::new(
            replica_writer,
            consumer,
            Duration::ZERO,
            MAX_JOURNAL_BATCH,
            false,
        );
        stage.set_raw_journal_receiver(raw_rx);

        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&shutdown2));

        // Publish events to the disruptor (same as the replication receiver
        // would do) so the journal stage can advance the cursor.
        for event in &events {
            input_producer.publish(InputSlot {
                connection_id: 0,
                key_hash: 0,
                request_seq: 0,
                event: *event,
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        }

        // Send raw bytes via the pre-allocated channel.
        raw_tx.send(
            &raw_bytes,
            FIRST_SEQ + entry_count as u64 - 1,
            [0u8; 32],
            entry_count,
        );

        // Wait for the journal cursor to advance past our events.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while journal_cursor.get().load(Ordering::Acquire) < entry_count as u64 {
            assert!(
                std::time::Instant::now() < deadline,
                "timeout waiting for replica journal cursor"
            );
            std::hint::spin_loop();
        }

        // Shut down and verify the replica journal has the correct entries.
        shutdown.store(true, Ordering::Relaxed);
        let _writer = handle.join().unwrap();

        #[cfg(not(feature = "no-persist"))]
        {
            let mut reader = crate::journal::JournalReader::open(&replica_path).unwrap();
            let e1 = reader.next_entry().unwrap().unwrap();
            assert!(matches!(
                e1.event,
                JournalEvent::Deposit { amount: 500, .. }
            ));
            let e2 = reader.next_entry().unwrap().unwrap();
            assert!(matches!(
                e2.event,
                JournalEvent::Deposit { amount: 300, .. }
            ));
            assert!(reader.next_entry().unwrap().is_none());
        }
    }

    /// `build_replica_pipeline` produces a working pipeline: events published
    /// to the input disruptor are processed by the matching stage, and raw
    /// bytes sent to the journal stage are written to disk.
    #[test]
    fn build_replica_pipeline_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let primary_path = dir.path().join("primary.journal");
        let replica_path = dir.path().join("replica.journal");

        // Prepare a primary journal to get raw bytes.
        let deposit = JournalEvent::Deposit {
            account: AccountId(1),
            currency: CurrencyId(0),
            amount: 1_000,
        };
        let raw_bytes;
        {
            let mut writer = crate::journal::writer::JournalWriter::create(&primary_path).unwrap();
            writer.batch_append(&deposit).unwrap();
            raw_bytes = writer.pending_batch_bytes().to_vec();
            writer.flush_batch_sync().unwrap();
        }

        // Build the replica pipeline.
        let exchange = Exchange::new();
        let writer = crate::journal::writer::JournalWriter::create(&replica_path).unwrap();
        let (
            input_producer,
            journal_stage,
            matching_stage,
            drain_consumer,
            journal_cursor,
            _matching_cursor,
            raw_tx,
            _shadow_consumer,
            _chain_hash_lock,
        ) = build_replica_pipeline(exchange, writer, MAX_JOURNAL_BATCH, false, false);

        let shutdown = Arc::new(AtomicBool::new(false));

        let s = Arc::clone(&shutdown);
        let j_handle = std::thread::spawn(move || journal_stage.run(&s));

        let s = Arc::clone(&shutdown);
        let m_handle = std::thread::spawn(move || matching_stage.run(&s));

        // Drain output so matching stage doesn't block.
        let s = Arc::clone(&shutdown);
        let d_handle = std::thread::spawn(move || {
            let mut consumer = drain_consumer;
            let mut batch = vec![OutputSlot::default(); 64];
            loop {
                if s.load(Ordering::Relaxed) {
                    return;
                }
                let count = consumer.consume_batch(&mut batch, 64);
                if count == 0 {
                    std::thread::yield_now();
                }
            }
        });

        // Publish event to disruptor and raw bytes to journal stage.
        input_producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            event: deposit,
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
        raw_tx.send(&raw_bytes, FIRST_SEQ, [0u8; 32], 1);

        // Wait for journal cursor.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while journal_cursor.get().load(Ordering::Acquire) < 1 {
            assert!(
                std::time::Instant::now() < deadline,
                "timeout waiting for journal cursor"
            );
            std::hint::spin_loop();
        }

        // Shut down pipeline.
        drop(raw_tx); // unblock journal stage
        shutdown.store(true, Ordering::Relaxed);
        let _writer = j_handle.join().unwrap();
        let exchange = m_handle.join().unwrap();
        let _ = d_handle.join();

        // Verify matching stage applied the deposit.
        assert!(
            exchange.accounts().has_balances(AccountId(1)),
            "matching stage should have applied the deposit"
        );

        // Verify journal has the entry.
        #[cfg(not(feature = "no-persist"))]
        {
            let mut reader = crate::journal::JournalReader::open(&replica_path).unwrap();
            let entry = reader.next_entry().unwrap().unwrap();
            assert!(matches!(
                entry.event,
                JournalEvent::Deposit { amount: 1_000, .. }
            ));
        }
    }

    // --- SPSC ring unit tests ---

    #[test]
    fn raw_batch_ring_send_recv() {
        let (tx, rx) = raw_batch_channel();
        tx.send(&[1, 2, 3], 10, [0u8; 32], 1);
        let slot = rx.try_recv().unwrap();
        assert_eq!(slot.bytes(), &[1, 2, 3]);
        assert_eq!(slot.end_sequence, 10);
        assert_eq!(slot.entry_count, 1);
    }

    #[test]
    fn raw_batch_ring_empty_returns_none() {
        let (_tx, rx) = raw_batch_channel();
        assert!(rx.try_recv().is_none());
    }

    #[test]
    fn raw_batch_ring_fill_all_slots() {
        let (tx, rx) = raw_batch_channel();
        // Fill all 8 slots.
        for i in 0..RAW_RING_CAPACITY {
            tx.send(&[i as u8], i as u64, [0u8; 32], 1);
        }
        // Drain all — each slot handle drops at end-of-iteration,
        // advancing `tail` and releasing the slot back to the sender.
        for i in 0..RAW_RING_CAPACITY {
            let slot = rx.try_recv().unwrap();
            assert_eq!(slot.bytes(), &[i as u8]);
            assert_eq!(slot.end_sequence, i as u64);
        }
        assert!(rx.try_recv().is_none());
    }

    #[test]
    fn raw_batch_ring_wraps_around() {
        let (tx, rx) = raw_batch_channel();
        // Send and receive more than capacity to exercise wrap-around.
        for round in 0..3u64 {
            for slot in 0..RAW_RING_CAPACITY as u64 {
                let seq = round * RAW_RING_CAPACITY as u64 + slot;
                tx.send(&[seq as u8], seq, [0u8; 32], 1);
            }
            for slot in 0..RAW_RING_CAPACITY as u64 {
                let seq = round * RAW_RING_CAPACITY as u64 + slot;
                let handle = rx.try_recv().unwrap();
                assert_eq!(handle.end_sequence, seq);
            }
            assert!(rx.try_recv().is_none());
        }
    }

    #[test]
    fn raw_batch_ring_concurrent_send_recv() {
        use std::thread;

        let (tx, rx) = raw_batch_channel();
        let count = 10_000u64;

        let producer = thread::spawn(move || {
            for i in 0..count {
                tx.send(&[(i & 0xFF) as u8], i, [0u8; 32], 1);
            }
        });

        let consumer = thread::spawn(move || {
            let mut received = 0u64;
            while received < count {
                if let Some(handle) = rx.try_recv() {
                    assert_eq!(handle.end_sequence, received);
                    received += 1;
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    }

    #[test]
    fn raw_batch_ring_drop_releases_uncommitted_slots() {
        // The new ring holds its slot buffers inline (no boxed
        // payloads), so there's nothing to leak if the channel is
        // dropped with outstanding batches — the Box<[UnsafeCell<Vec>]>
        // drops cleanly. This test exists to catch future regressions
        // where a drop impl might start to matter.
        let (tx, rx) = raw_batch_channel();
        for i in 0..3u8 {
            tx.send(&[i], i as u64, [0u8; 32], 1);
        }
        let _ = rx.try_recv(); // Consume 1, dropping its handle here.
        drop(tx);
        drop(rx);
    }

    #[test]
    fn raw_batch_ring_sender_drops_before_receiver() {
        let (tx, rx) = raw_batch_channel();
        tx.send(&[42], 1, [0u8; 32], 1);
        // Drop sender first — the Arc<RawBatchRing> in the slot handle
        // keeps the backing storage alive, so the receiver still reads
        // the batch cleanly.
        drop(tx);
        let slot = rx.try_recv().unwrap();
        assert_eq!(slot.bytes(), &[42]);
        drop(slot);
        assert!(rx.try_recv().is_none());
    }

    #[test]
    fn raw_batch_ring_slot_grows_past_initial_capacity() {
        // The slot buffer starts at RAW_SLOT_INITIAL_CAPACITY (1 MiB) but
        // is a `Vec<u8>` so it grows amortized on larger bursts. Send a
        // batch that exceeds the initial capacity and verify the payload
        // round-trips correctly.
        let (tx, rx) = raw_batch_channel();
        let big: Vec<u8> = (0..(RAW_SLOT_INITIAL_CAPACITY + 4096))
            .map(|i| (i & 0xFF) as u8)
            .collect();
        tx.send(&big, 99, [0xCC; 32], 1);
        let slot = rx.try_recv().unwrap();
        assert_eq!(slot.len(), big.len());
        assert_eq!(slot.bytes(), big.as_slice());
        assert_eq!(slot.end_sequence, 99);
        assert_eq!(slot.chain_hash, [0xCC; 32]);

        // After the slot is released, the sender can reuse it for a
        // smaller batch. The Vec keeps its grown capacity; no realloc
        // on the next send.
        drop(slot);
        tx.send(&[1, 2, 3], 100, [0; 32], 1);
        let next = rx.try_recv().unwrap();
        assert_eq!(next.bytes(), &[1, 2, 3]);
    }

    #[test]
    fn raw_batch_ring_slot_pins_buffer_until_drop() {
        // The sender must not overwrite a slot while its handle is
        // held by the consumer. Verify by keeping a slot alive across
        // multiple sends that would otherwise wrap onto it.
        let (tx, rx) = raw_batch_channel();
        tx.send(&[0xAA, 0xBB, 0xCC], 1, [0u8; 32], 1);
        let slot = rx.try_recv().unwrap();

        // Fill the remaining 7 slots — sender is blocked from reusing
        // slot[0] because `slot` is still held by us (tail hasn't
        // advanced). This succeeds because CAP - 1 = 7 slots remain.
        for i in 1..RAW_RING_CAPACITY as u64 {
            tx.send(&[i as u8], i + 1, [0u8; 32], 1);
        }

        // The first slot's bytes must still be intact — the held handle
        // pinned them.
        assert_eq!(slot.bytes(), &[0xAA, 0xBB, 0xCC]);
        assert_eq!(slot.end_sequence, 1);
        drop(slot); // Release slot 0 back to the sender.

        // Now drain the remaining 7.
        for i in 1..RAW_RING_CAPACITY as u64 {
            let next = rx.try_recv().unwrap();
            assert_eq!(next.end_sequence, i + 1);
        }
    }
}
