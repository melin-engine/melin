//! Pipeline stages for the LMAX disruptor architecture.
//!
//! Two hot-path stages consume from an input disruptor in **parallel**:
//! 1. **Journal stage**: batch-encodes events and hands each batch to the
//!    disk thread, which writes and syncs it (`pwrite` + `fdatasync`) and
//!    then publishes the cursors that mean durable — so the sequencing
//!    thread never waits on the device. When replication is enabled, the
//!    sequencer also sends a copy of each encoded batch to the replication
//!    sender thread via a bounded channel, *before* handing it to the disk
//!    thread, so replicas never wait on local storage either. The bytes are
//!    identical to what is written to disk — same sequences, timestamps,
//!    CRC checksums, and checkpoint entries. See [`crate::journal_disk`].
//! 2. **Matching stage**: executes commands on the `Exchange`, publishes responses
//!    to the output SPSC. Runs concurrently with the journal — no waiting for sync.
//!
//! The **response stage** (in the server crate) consumes the output SPSC but
//! gates each event on the configured durability policy before sending: it
//! evaluates the durable wire-seq cursor (local fsync progress) together with
//! the per-replica ack metrics, so a response is only sent once the policy's
//! durability requirement is met (e.g. on disk **and** acknowledged by a
//! replica when replication is active).
//!
//! This gives maximum pipeline parallelism (matching overlaps journal I/O,
//! and encoding overlaps the device) while preserving persist-before-ack at
//! the response boundary: the cursors the gate reads are published by the
//! thread that performed the sync, after it returned.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::trace::{MonoTraceInstant, mono_trace_ns};
use melin_app::{AppEvent, Application, ApplyCtx, RejectReason};
use melin_journal::BufferedWriter;
use melin_journal::JournalError;
use melin_journal::encoder::JournalEncoder;
use melin_journal::preparer::SegmentPreparer;
use melin_journal::segment_file::SegmentFile;
use melin_journal::write_ring::{
    ClaimedChunk, JournalWriteMeta, JournalWriteProducer, build_journal_write_ring,
};

use crate::journal_disk::{DiskControl, DurabilityCursors, JournalDisk, RotateRequest};
use melin_journal::replication::{ReplicationConsumer, ReplicationProducer};

use melin_pipeline::padding::Sequence;
use melin_pipeline::ring;
use melin_pipeline::seqlock::{NoPadding, SeqLockReader, SeqLockWriter};

use crate::cursors::{
    AdvertisedJournalTip, DurableWireSeqCursor, PipelineCursors, RingPos, WireSeq,
};

use crate::replication_wire::{finalize_input_batch, init_input_batch};

/// Post-fsync state published by the disk thread after each durable
/// write. The seqlock guarantees all fields are read atomically —
/// no TOCTOU between `journal_seq` and `chain_hash`.
///
/// Read by the shadow snapshot stage (`journal_seq` + `chain_hash` for
/// the snapshot header, `input_ring_seq` for alignment) and by
/// replication receivers (`chain_hash` for handshake validation).
#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct FsyncState {
    /// Highest journal sequence in the batch that just became durable.
    /// Wire-seq space — the same value published through
    /// `PipelineCursors::durable_wire_seq` (both are written in the
    /// disk thread's single publication step, after the sync).
    pub journal_seq: WireSeq,
    /// BLAKE3 chain hash after the fsync. `[0u8; 32]` when hash-chain
    /// is disabled.
    pub chain_hash: [u8; 32],
    /// Input ring read cursor at the batch's sync point, captured by
    /// the sequencer when it handed the batch over.
    /// The shadow compares this against its own `next_read` to confirm
    /// it has caught up to the exact fsync boundary.
    pub input_ring_seq: RingPos,
}

// Safety: `repr(C)` over padding-free fields (`WireSeq` and `RingPos` are
// `repr(transparent)` over `u64`, plus a byte array), with the assertion
// below proving the size equals the sum of the field sizes — under
// `repr(C)`, that equality rules out padding.
unsafe impl NoPadding for FsyncState {}
// Compile-time proof for the impl above; fails the build if a future field
// introduces padding.
const _: () = assert!(
    size_of::<FsyncState>() == size_of::<WireSeq>() + size_of::<[u8; 32]>() + size_of::<RingPos>()
);

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
    /// Cumulative gate opens where the *primary itself* supplied the
    /// binding cursor — the local journal was the bottleneck.
    /// Only used by the response stage; always 0 for journal/matching.
    pub gate_journal: AtomicU64,
    /// Cumulative gate opens where a *replica* supplied the binding
    /// cursor — replication was the bottleneck. Which replica cursor
    /// counts depends on the configured durability policy: in-memory
    /// under `hybrid`, persisted under `durably-replicated`. Under
    /// `local` a replica can never bind, so this stays at 0.
    /// Only used by the response stage; always 0 for journal/matching.
    ///
    /// Exactly one of the two counters moves per gate open: attribution
    /// is computed from the same cursor snapshot as the evaluation that
    /// opened the gate, and an open gate implies the policy was
    /// satisfiable by the connected nodes. While the shape *cannot*
    /// satisfy the policy the gate does not open at all —
    /// `policy_degraded` covers that state.
    pub gate_replication: AtomicU64,
    /// Whether the most recent durability-gate evaluation found a
    /// clause requiring more nodes than are currently connected. The
    /// clause is not relaxed — the gate stalls until the cluster shape
    /// recovers or an operator swaps the mode.
    /// Surfaced on `/healthz` so dashboards and alerting can fire on
    /// it. Only used by the response stage.
    pub policy_degraded: AtomicBool,
    /// Cumulative nanoseconds the durability policy has spent in the
    /// degraded state above. Paired with the `policy_degraded` gauge so
    /// dashboards can compute time-in-degraded over a window with
    /// `rate(...degraded_seconds_total[5m])` instead of reconstructing
    /// intervals from high-frequency gauge samples. `u64` nanoseconds:
    /// the response stage accumulates sub-second tick intervals, and
    /// u64 holds ~584 years of nanos before overflow — `u128` would be
    /// wasteful and `AtomicU128` isn't available anyway. Only written
    /// by the response stage; read by the health endpoint.
    pub policy_degraded_nanos: AtomicU64,
    /// Cumulative journal segment rotations (local triggers and
    /// primary-announced adoptions) that consumed a pre-staged segment
    /// from the background preparer — the fast path: two renames + a
    /// directory fsync instead of the ~38 ms synchronous allocate.
    /// Only used by the journal stage; always 0 for matching/response.
    /// `AtomicU64` (not a plain field) so `/healthz` can read it live
    /// while the stage runs — increments happen only at rotation time,
    /// so the atomic costs nothing on the hot path.
    pub rotations_fast_path: AtomicU64,
    /// Cumulative rotations that fell back to the synchronous
    /// create-and-allocate path because no prepared segment was
    /// available. Steady state under recurring rotation should be zero
    /// for both writers — growth means the preparer isn't keeping up
    /// (or isn't armed) and the rotation stall is landing on the
    /// journal thread. For the buffered writer a fallback segment also
    /// loses the pre-zeroed-extents property until the next rotation
    /// (see `BufferedWriter::rotate_segment_with_prepared`). Only used
    /// by the journal stage.
    pub rotations_sync_fallback: AtomicU64,
    /// Cumulative rotation *attempts* that failed (ENOSPC, read-only
    /// filesystem, …) and left the current segment in place. The
    /// journal keeps growing past its threshold while this climbs, so
    /// any growth is alert-worthy — and without this counter the
    /// pathology is visible only in logs. Only used by the journal
    /// stage.
    pub rotations_failed: AtomicU64,
    /// Batches handed to the disk thread that are not yet durable.
    ///
    /// Zero in steady state: the disk thread keeps up, so a batch is
    /// written and synced before the next one arrives. A sustained
    /// non-zero value is the disk falling behind — which the pipeline
    /// now rides through instead of freezing on, up to the hand-off
    /// ring's depth. At the depth it becomes backpressure: the
    /// sequencer stalls, then the input ring fills, then producers
    /// stall. Alert on it approaching the ring depth, not on it being
    /// briefly non-zero. Only used by the journal stage.
    pub journal_disk_lag: AtomicU64,
}

impl StageUtilization {
    pub fn new() -> Self {
        Self {
            busy: AtomicU64::new(0),
            idle: AtomicU64::new(0),
            gate_journal: AtomicU64::new(0),
            gate_replication: AtomicU64::new(0),
            policy_degraded: AtomicBool::new(false),
            policy_degraded_nanos: AtomicU64::new(0),
            rotations_fast_path: AtomicU64::new(0),
            rotations_sync_fallback: AtomicU64::new(0),
            rotations_failed: AtomicU64::new(0),
            journal_disk_lag: AtomicU64::new(0),
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
pub(crate) fn idle_wait(idle_spins: &mut u32, busy_spin: bool) {
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
///
/// `#[repr(align(64))]` forces 64-byte alignment and rounds the struct
/// size up to a multiple of 64 — without padding the natural layout is
/// 104 bytes (or 120 with `latency-trace`), which makes adjacent slots
/// share cache lines and forces every slot access to touch 2–3 lines
/// instead of 2. With this attribute both configurations occupy exactly
/// 128 bytes (two cache lines), so the producer's writes to slot N never
/// share a line with slot N±1 and per-slot line traffic is minimised.
#[derive(Debug, Clone, Copy)]
#[repr(align(64))]
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
    pub publish_ts: MonoTraceInstant,
    /// Timestamp when the reader task received this request from the wire.
    /// Flows through the entire pipeline to measure server-side end-to-end latency.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub recv_ts: MonoTraceInstant,
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
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
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
    /// Retained for per-slot latency attribution and ordering invariants
    /// inside this process. The response stage's *durability* gate uses
    /// `wire_seq` instead — `input_seq` is in local-consumer space (this
    /// process's matching cursor on the input ring) while replica
    /// metrics live in wire-seq space, and a direct numeric comparison
    /// across those two spaces is unsound (see commit history).
    pub input_seq: u64,
    /// Primary-allocated wire sequence of the event that produced this
    /// output slot, in the same space as `metrics.in_memory_sequence` /
    /// `metrics.acked_sequence` and the journal stage's allocator. The
    /// response stage uses this — *not* `input_seq` — for `needed` when
    /// evaluating the durability policy, so the gate is sound regardless
    /// of `starting_sequence` (fresh start vs recovery from a journal
    /// with prior history).
    ///
    /// Set by the matching stage; the journal stage publishes a parallel
    /// `journal_wire_seq_cursor` on the persisted track. Both follow the
    /// journal-allocation rule (events the journal would `continue` past
    /// — `Query` — do not advance the counter; their output slots carry
    /// the prior wire seq, so the gate waits for preceding allocated
    /// events to be durable before releasing a query response).
    pub wire_seq: u64,
    /// The response payload.
    pub payload: OutputPayload<R, Q>,
    /// Timestamp when the matching stage finished processing this event.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub match_complete_ts: MonoTraceInstant,
    /// Timestamp when the reader task received this request from the wire.
    /// Carried through the pipeline to measure server-side end-to-end latency.
    /// `()` (zero-sized) when `latency-trace` is disabled.
    pub recv_ts: MonoTraceInstant,
    /// True when this is the final slot the matching stage emits for
    /// the originating input event. The response stage emits a wire
    /// `ResponseKind::BatchEnd` after the payload (skipped when the
    /// payload itself is `BatchEnd` — which is its own terminator).
    pub is_last_in_request: bool,
    /// Exempt this slot from the response stage's durability gate.
    ///
    /// Set on every slot the matching stage emits while `halted` (all
    /// replicas disconnected). Two kinds of slot reach the output ring
    /// under halt: the explicit `Rejected{ReplicaDisconnected}` reports
    /// produced for incoming client orders, and the empty `BatchEnd`
    /// terminators emitted for transport-internal events (Tick).
    /// Neither carries engine state worth
    /// replicating before delivery — the rejection records no mutation,
    /// and replicas deterministically reach the same halt decision when
    /// they replay the same inputs. Gating either under a structurally
    /// unsatisfiable policy (e.g. `Hybrid` with no replicas) would
    /// stall the response gate forever, including for the rejection
    /// itself, which is exactly what we want clients to see immediately.
    /// The carve-out is therefore correctness-preserving and improves
    /// operator visibility during outages.
    ///
    /// Every other output kind (Placed, Fill, Cancelled, non-halt
    /// reject reasons, query responses) keeps the gate, since each
    /// reflects engine state or a state-derived decision (rate-limiter
    /// consumption, dedup) that must be durable before reply.
    pub durability_bypass: bool,
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
            wire_seq: 0,
            payload: OutputPayload::BatchEnd,
            match_complete_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
            is_last_in_request: true,
            durability_bypass: false,
        }
    }
}

/// Journal stage: consumes from the input disruptor, batch-encodes events,
/// and hands the encoded bytes to the disk thread.
///
/// This type is the stage before it runs: configuration, plus the
/// composed writer. [`run`](Self::run) splits the writer, starts the
/// disk thread on the file half, and drives a [`Sequencer`] over the
/// stream half.
///
/// The sequencing thread never touches the device. It orders events,
/// allocates sequences, encodes, chains, and feeds replicas; the disk
/// thread writes, syncs, and publishes the cursors that mean durable.
/// The response stage reads those cursors to know when events are
/// durable, so persist-before-ack is preserved by *where* they are
/// published (see [`crate::journal_disk`]).
///
/// When replication is enabled, the stage also sends a copy of each
/// encoded batch to the replication sender thread via a bounded
/// channel. The bytes are identical to what is written to disk — same
/// sequences, timestamps, CRC checksums, and checkpoint entries.
pub struct JournalStage<E: AppEvent> {
    /// Composed writer, split by `run` into the stream half (kept by
    /// the sequencer) and the file half (moved to the disk thread).
    writer: BufferedWriter<E>,
    _marker: std::marker::PhantomData<fn() -> E>,
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
    /// Optional seqlock writer for publishing the BLAKE3 chain hash to the
    /// shadow snapshot stage. Updated once per fsync batch (cold path). `None`
    /// when shadow snapshots are disabled — no allocation or write overhead.
    /// The writer handle is unique, so the journal stage owning it is what
    /// makes the seqlock's single-writer requirement structural.
    chain_hash: Option<SeqLockWriter<FsyncState>>,
    /// Optional typed handle for publishing the writer's `next_sequence - 1`
    /// (the highest wire seq durably persisted) to readers outside the
    /// pipeline thread — the durability gate, health endpoint, and the
    /// replica orchestrator's reconnect handshake all read the same cursor.
    /// Updated once per fsync batch alongside `chain_hash`.
    last_seq: Option<DurableWireSeqCursor>,
    /// Optional handle to the control-plane advertised journal tip
    /// (raft vote filtering). Wired only on a primary's pipeline — on a
    /// replica the replication receiver owns the tip at its in-memory
    /// accepted position instead (see [`AdvertisedJournalTip`]). Advanced
    /// once per fsync batch alongside `last_seq`, with the same value.
    advertised_tip: Option<AdvertisedJournalTip>,
    /// When true, never yield to the OS scheduler — spin indefinitely with
    /// PAUSE. Requires isolated cores (`isolcpus`). See [`idle_wait`].
    busy_spin: bool,
    /// Shared busy/idle counters for health endpoint monitoring.
    utilization: Arc<StageUtilization>,
    /// Live-segment size threshold (bytes). When > 0, the journal stage
    /// rotates immediately after the fsync batch that pushes the live
    /// file past this threshold. `0` disables size-driven rotation —
    /// runtime rotation can still be requested manually via
    /// `rotate_requested`.
    max_journal_bytes: u64,
    /// Operator-driven rotation flag. When this flips to `true` (e.g.
    /// from a `ROTATE` admin command), the journal stage performs one
    /// rotation at the next fsync boundary and clears the flag. Cleared
    /// via `compare_exchange(true → false)` so concurrent triggers
    /// degrade to a single rotation rather than queueing.
    rotate_requested: Option<Arc<AtomicBool>>,
    /// Suppression window for size-driven rotation after a failure. Set
    /// to `now + ROTATION_FAILURE_BACKOFF` whenever `rotate_segment`
    /// returns Err so a permanent failure (ENOSPC, RO-FS) doesn't
    /// re-arm on every batch and flood the logs. Manual `ROTATE`
    /// requests bypass this window — operators get a fresh attempt and
    /// a fresh error log on each command.
    rotation_backoff_until: Option<Instant>,
    /// Background preparer that pre-stages the next segment off the
    /// rotation hot path. `Some` once `enable_preparer` arms it at run
    /// startup (size-driven rotation or replica adoption); `None` when
    /// rotation can't recur (no point spending disk + a thread on
    /// speculation that may never pay off). Survives every
    /// rotation — only the writer's file is swapped, the preparer
    /// keeps preparing the same live-path sidecar across the rotation
    /// boundary. Rotation outcome counters live in
    /// [`StageUtilization`] so `/healthz` can surface them.
    preparer: Option<SegmentPreparer>,
    /// Core the preparer worker pins itself to; `0` = unpinned
    /// (default). Set via [`set_preparer_core`](Self::set_preparer_core)
    /// before the stage runs — keeps staging I/O off the IRQ core and
    /// pipeline cores deterministically, same convention as the shadow
    /// and event-publisher threads.
    preparer_core: usize,
    /// Primary-announced stream marks to apply (replica mode only;
    /// `None` on primaries/standalone). Pushed by the replication
    /// receiver, popped here. See [`StreamMark`].
    stream_marks: Option<StreamMarkQueue>,
    /// Front of the stream-mark queue — the next position this stage
    /// must act at. Held locally so the steady-state cost is one
    /// `Option` check, not a mutex lock.
    pending_mark: Option<StreamMark>,
    /// Core the disk thread pins itself to; `0` = unpinned. Set via
    /// [`set_disk_core`](Self::set_disk_core) before the stage runs.
    disk_core: usize,
}

/// The running half of the journal stage: everything the sequencing
/// thread owns while the pipeline is live.
///
/// Built by [`JournalStage::run`] once the writer has been split. The
/// file half is gone by then — it lives on the disk thread — so every
/// field here is either in-memory stream state or a hand-off handle.
/// That is the point: a field this type cannot reach is a device
/// operation the sequencing thread cannot accidentally perform.
struct Sequencer<E: AppEvent> {
    /// Stream half of the writer: sequences, framing, chain.
    encoder: JournalEncoder<E>,
    /// Hand-off to the disk thread.
    batches: JournalWriteProducer,
    /// Slot currently being encoded into, claimed lazily at the first
    /// event of a batch and given up at the submit point.
    claim: Option<ClaimedChunk>,
    /// Shared control: poison, rotation rendezvous, stop, lag gauge.
    disk: Arc<DiskControl>,
    /// The disk thread, joined at shutdown to recover the file half.
    disk_thread: Option<std::thread::JoinHandle<SegmentFile>>,
    /// Whether anything downstream reads the per-batch chain value.
    ///
    /// `FsyncState.chain_hash` has exactly one consumer — the shadow
    /// stage's snapshot writer — and it is wired only when snapshots
    /// are enabled. Producing the value costs a BLAKE3 hasher clone and
    /// finalize, ~400 ns, on the hand-off path of *every* batch; with
    /// no publisher attached that is pure waste, which is what this
    /// flag removes. It does not gate the chain itself: rotation
    /// anchors, `Rotate` announces, and replication chain checks all
    /// still read the encoder directly.
    chain_hash_observed: bool,
    /// Bytes encoded into the live segment. The size-driven rotation
    /// trigger reads this instead of the file's end: the file is on
    /// another thread, and the boundary is a property of the stream
    /// anyway. Tracks `SegmentFile::valid_end` exactly once the disk
    /// thread has caught up.
    segment_bytes: u64,
    consumer: ring::Consumer<InputSlot<E>>,
    group_commit_delay: Duration,
    max_batch: usize,
    repl: Box<ReplicationState>,
    busy_spin: bool,
    utilization: Arc<StageUtilization>,
    max_journal_bytes: u64,
    rotate_requested: Option<Arc<AtomicBool>>,
    rotation_backoff_until: Option<Instant>,
    preparer: Option<SegmentPreparer>,
    stream_marks: Option<StreamMarkQueue>,
    pending_mark: Option<StreamMark>,
}

/// How long to suppress size-driven rotation attempts after a failure.
/// Picked to balance log-flood prevention against responsiveness once
/// the environmental issue (disk space, fs read-only) is resolved.
const ROTATION_FAILURE_BACKOFF: Duration = Duration::from_secs(30);

/// Sleep between encode-loop barrier polls while an adopted rotation is
/// stalled in its failure backoff. The replica cannot encode past the
/// boundary, so the stage is blocked on storage recovery — coarse
/// enough to keep the stall loop cold, fine enough that the retry fires
/// promptly once the backoff expires.
const ADOPTION_STALL_POLL: Duration = Duration::from_millis(10);

/// A segment rotation announced by the primary over the replication
/// stream (`Rotate { boundary_seq, tail_hash }`). The replica's journal
/// stage adopts it at exactly `boundary_seq`: flush everything up to
/// and including the boundary, verify the local chain tail equals
/// `tail_hash` (divergence check), then rotate so the new segment's
/// anchor — the local tail — matches the primary's. Replicas never
/// rotate on local triggers; shared boundaries are what make chain
/// values comparable across nodes and healthy replica journals bitwise
/// mirrors of the primary's.
#[derive(Debug, Clone, Copy)]
pub struct AdoptedRotation {
    /// Last sequence of the outgoing segment.
    pub boundary_seq: u64,
    /// The primary's chain value at `boundary_seq` (= the new segment's
    /// header anchor).
    pub tail_hash: [u8; 32],
}

/// A primary-announced action tied to an exact stream position, applied
/// by the replica's journal stage when its writer reaches that
/// sequence. Order matters relative to the entry stream AND between
/// marks, hence one queue for both kinds.
#[derive(Debug, Clone, Copy)]
pub enum StreamMark {
    /// Rotate at the boundary (see [`AdoptedRotation`]). Requires a
    /// quiesced writer (nothing in flight); the rotation itself flushes.
    Rotate(AdoptedRotation),
    /// Compare the local chain value at `sequence` against the
    /// primary's. Applies inline — the chain is a pure function of
    /// encoded bytes, so no flush or quiesce is needed.
    ChainCheck { sequence: u64, chain_hash: [u8; 32] },
}

impl StreamMark {
    /// Stream position this mark acts at.
    pub fn sequence(&self) -> u64 {
        match self {
            Self::Rotate(r) => r.boundary_seq,
            Self::ChainCheck { sequence, .. } => *sequence,
        }
    }
}

/// Cross-thread hand-off of primary-announced stream marks: the
/// replication receiver pushes (in stream order, before publishing any
/// slot past the mark's position), the journal stage pops.
/// `Mutex<VecDeque>` rather than a lock-free queue: marks are strictly
/// cold (rotations a few per gigabyte, chain checks one per
/// [`CHAIN_CHECK_INTERVAL_BATCHES`] fsync batches), and the journal
/// stage only locks at batch/sync boundaries — never per entry.
pub type StreamMarkQueue = Arc<Mutex<VecDeque<StreamMark>>>;

/// Emit a live-stream `ChainCheck` after every N published replication
/// batches. Count-based (not time-based) so emission is a pure function
/// of the event stream. At full load (~12.5K batches/s) this is ~200
/// checks/s — 45 wire bytes and one BLAKE3 finalize each, negligible
/// against the data volume; at low rate checks are sparse, which is
/// fine: handshake validation covers reconnects, and every rotation
/// adoption is itself a chain check.
const CHAIN_CHECK_INTERVAL_BATCHES: u32 = 64;

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
    /// Published batches since the last live-stream `ChainCheck` was
    /// emitted — see [`CHAIN_CHECK_INTERVAL_BATCHES`].
    batches_since_chain_check: u32,
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
            batches_since_chain_check: 0,
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
        writer: BufferedWriter<E>,
        consumer: ring::Consumer<InputSlot<E>>,
        group_commit_delay: Duration,
        max_batch: usize,
        busy_spin: bool,
    ) -> Self {
        Self {
            writer,
            _marker: std::marker::PhantomData,
            consumer,
            group_commit_delay,
            max_batch: max_batch.min(MAX_JOURNAL_BATCH),
            repl: Box::default(),
            chain_hash: None,
            last_seq: None,
            advertised_tip: None,
            busy_spin,
            utilization: Arc::new(StageUtilization::new()),
            max_journal_bytes: 0,
            rotate_requested: None,
            rotation_backoff_until: None,
            preparer: None,
            preparer_core: 0,
            stream_marks: None,
            pending_mark: None,
            disk_core: 0,
        }
    }

    /// Enable runtime rotation.
    ///
    /// `max_journal_bytes`: live-segment size threshold in bytes; `0`
    /// disables size-driven rotation. `rotate_flag`: optional shared
    /// AtomicBool flipped by the admin endpoint to force a rotation at
    /// the next fsync boundary. Either or both may be supplied — both
    /// disabled means rotation only happens at startup (legacy
    /// behaviour).
    pub fn set_rotation(&mut self, max_journal_bytes: u64, rotate_flag: Option<Arc<AtomicBool>>) {
        self.max_journal_bytes = max_journal_bytes;
        self.rotate_requested = rotate_flag;
        // The preparer fast path is wired by `enable_preparer`, called
        // from the stage's run path: it pre-writes physical zeros so
        // appends stop
        // generating extent-conversion metadata (the fdatasync
        // journal-force beat — see the preparer module docs).
    }

    /// Replica mode: act only on primary-announced stream marks pushed
    /// onto `queue` by the replication receiver — rotations at announced
    /// boundaries, chain checks at announced positions. Mutually
    /// exclusive with local triggers (`set_rotation`) — a replica that
    /// rotated on its own would desynchronize its segment boundaries
    /// from the primary's, making chain values incomparable across
    /// nodes.
    pub fn set_stream_marks(&mut self, queue: StreamMarkQueue) {
        debug_assert!(
            self.max_journal_bytes == 0 && self.rotate_requested.is_none(),
            "stream marks are mutually exclusive with local rotation triggers"
        );
        self.stream_marks = Some(queue);
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

    /// Install the seqlock's writer half for publishing the BLAKE3 chain
    /// hash to the shadow snapshot stage. Called once during pipeline
    /// construction when shadow snapshots are enabled. Takes the handle by
    /// value — the stage becomes the only thing that can publish.
    pub fn set_chain_hash_lock(&mut self, writer: SeqLockWriter<FsyncState>) {
        self.chain_hash = Some(writer);
    }

    /// Set the cursor handle for publishing the highest wire seq durably
    /// persisted. Called once during pipeline construction when readers
    /// outside the pipeline thread (e.g., the replication-receive
    /// orchestrator) need to read the writer's progress without owning it.
    pub fn set_last_seq_publisher(&mut self, last_seq: DurableWireSeqCursor) {
        self.last_seq = Some(last_seq);
    }

    /// Set the control-plane advertised-tip handle. Called by the server
    /// wiring on a *primary's* pipeline only — see the field docs for the
    /// replica-side ownership rule.
    pub fn set_advertised_tip_publisher(&mut self, tip: AdvertisedJournalTip) {
        self.advertised_tip = Some(tip);
    }

    /// Set the core the disk thread pins itself to (`0` = unpinned).
    /// Call before the stage runs; `run` reads it when it spawns.
    pub fn set_disk_core(&mut self, core: usize) {
        self.disk_core = core;
    }

    /// Split the writer, start the disk thread on the file half, and
    /// build the sequencer over the stream half.
    ///
    /// The split flushes anything the writer still holds, so the disk
    /// thread starts from a quiesced segment and no encoded bytes can
    /// be stranded on the wrong side of the boundary.
    fn into_sequencer(self) -> Result<Sequencer<E>, JournalError> {
        let (encoder, segment) = self.writer.into_halves()?;
        let (batches, batch_consumer) =
            build_journal_write_ring(melin_journal::write_ring::DEFAULT_CAPACITY);
        let control = Arc::new(DiskControl::new());
        let segment_bytes = segment.valid_end();
        // Decided once, here, because this is the only place that still
        // sees both halves: past this point the publisher lives on the
        // disk thread and the encoder on the sequencer.
        let chain_hash_observed = self.chain_hash.is_some();

        // The cursors that mean "durable" move with the file half —
        // only the disk thread knows when a batch has become durable.
        let cursors = DurabilityCursors {
            input_progress: self.consumer.progress_counter(),
            durable_wire_seq: self.last_seq,
            advertised_tip: self.advertised_tip,
            fsync_state: self.chain_hash,
        };
        let disk = JournalDisk::new(
            segment,
            batch_consumer,
            cursors,
            Arc::clone(&control),
            self.busy_spin,
        );
        let disk_core = self.disk_core;

        // The child's scheduling context must be set HERE, by the
        // parent, before the thread exists.
        //
        // This runs on the journal thread, which on a tuned deployment
        // is pinned to an isolated core at SCHED_FIFO and busy-spins
        // without ever yielding. A child inherits both the mask and the
        // policy at creation, so a child left to fix itself would have
        // to run first — on a core whose real-time occupant never
        // yields it. It never executes its first instruction, not even
        // the one that sets its name. Doing the reset child-side is the
        // bug this codebase already fixed once, in "configure spawned
        // threads from the parent, not the child".
        //
        // So: adopt the child's context, spawn, put ours back. The
        // window is a few microseconds at startup, before this thread
        // consumes anything.
        let saved = melin_app::affinity::take_context();
        if let Err(ref e) = saved {
            tracing::warn!(error = %e, "journal-disk: cannot snapshot scheduling context");
        }
        if let Err(e) = melin_app::affinity::prepare_child_context(disk_core) {
            tracing::warn!(error = %e, "journal-disk: cannot prepare child context");
        }
        let spawned = std::thread::Builder::new()
            .name("journal-disk".into())
            .spawn(move || {
                // Already on the right core under SCHED_OTHER, courtesy
                // of the parent. This only promotes to SCHED_FIFO when
                // the core is isolated.
                melin_app::affinity::pin_thread("journal-disk", disk_core);
                disk.run()
            });
        // Restore before handling the spawn result: a failed spawn must
        // not leave the journal thread on the disk core.
        if let Ok(ctx) = saved
            && let Err(e) = melin_app::affinity::restore_context(&ctx)
        {
            tracing::error!(error = %e, "journal thread could not restore its own affinity");
        }
        let disk_thread = spawned.map_err(|e| {
            JournalError::Io(std::io::Error::other(format!(
                "spawn journal-disk thread: {e}"
            )))
        })?;

        Ok(Sequencer {
            encoder,
            batches,
            claim: None,
            disk: control,
            disk_thread: Some(disk_thread),
            chain_hash_observed,
            segment_bytes,
            consumer: self.consumer,
            group_commit_delay: self.group_commit_delay,
            max_batch: self.max_batch,
            repl: self.repl,
            busy_spin: self.busy_spin,
            utilization: self.utilization,
            max_journal_bytes: self.max_journal_bytes,
            rotate_requested: self.rotate_requested,
            rotation_backoff_until: self.rotation_backoff_until,
            preparer: self.preparer,
            stream_marks: self.stream_marks,
            pending_mark: self.pending_mark,
        })
    }

    /// Drive the stage to completion on this (the sequencing) thread.
    ///
    /// Returns the writer, reassembled from both halves, on shutdown.
    pub fn run_sync(
        self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<BufferedWriter<E>, JournalError> {
        self.into_sequencer()?.run(shutdown)
    }
}

impl<E: AppEvent> Sequencer<E> {
    /// Drive the sequencing loop, then tear the disk thread down.
    ///
    /// The wrapper exists so the thread is stopped and joined on *every*
    /// exit, including the fatal ones: a sequencer that returned an
    /// error without joining would leak a spinning thread holding the
    /// live segment's descriptor.
    fn run(
        mut self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<BufferedWriter<E>, JournalError> {
        match self.sequence(shutdown) {
            Ok(()) => self.finish(),
            Err(e) => {
                self.halt_disk_thread();
                Err(e)
            }
        }
    }

    /// The sequencing loop: read, encode, publish to replicas, hand the
    /// batch to the disk thread. Never blocks on the device.
    ///
    /// Returns `Ok(())` when the stage should shut down cleanly.
    fn sequence(&mut self, shutdown: &std::sync::atomic::AtomicBool) -> Result<(), JournalError> {
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
        let mut wakeup_rec =
            crate::trace::register_stage("journal: disruptor wakeup (publish → journal consume)");
        #[cfg(feature = "latency-trace")]
        let mut batch_rec =
            crate::trace::register_stage("journal: batch processing (write + sync)");
        // Paces the idle-path recorder flush. See `trace::StageRecorder::flush`.
        #[cfg(feature = "latency-trace")]
        let mut stats_flush_timer = melin_app::amortized_timer::AmortizedTimer::new();

        loop {
            // A broken journal is fatal. The disk thread has stopped
            // publishing, so nothing downstream can ack past the
            // failure; surface the original cause and tear down.
            if self.disk.poisoned() {
                return Err(self.take_poison());
            }

            if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
                // Hand over anything encoded before shutting down.
                if pending > 0 {
                    self.submit_batch(self.consumer.next_read())?;
                }
                self.drain_remaining(&mut batch);
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("journal", busy_count, idle_count);
                return Ok(());
            }

            // Read entries WITHOUT advancing the cursor.
            // Ring position before this read — the mark barrier computes
            // its mid-batch commit target as `read_start + encoded slots`
            // (publishing `next_read` there would over-commit; see
            // `submit_batch`).
            let read_start = self.consumer.next_read();
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

            if count > 0 {
                idle_spins = 0;
                busy_count += 1;

                #[cfg(feature = "latency-trace")]
                let batch_start = mono_trace_ns();

                #[cfg(feature = "latency-trace")]
                for slot in &batch[..count] {
                    wakeup_rec.record_elapsed(slot.publish_ts, batch_start);
                }

                // Batch-encode all events into the writer's internal buffer.
                // Data stays in the buffer until the write point — one
                // O_DIRECT pwrite covers the entire batch.
                // QueryStats/QueryPosition are not journaled (no state change).
                //
                // The journal stage is the authoritative sequence allocator
                // on the primary: when `slot.sequence == 0` (every primary-
                // side input) we allocate at encode time in disruptor cursor
                // order. On replicas the replication receiver stamps the
                // primary's sequence onto `slot.sequence` before publish, and
                // we use it verbatim (also syncing the writer's counter).
                // Encoding always runs — under no-persist the bytes still
                // populate `batch_buf` so replication can publish them, and
                // sequence allocation must happen so downstream stages see
                // the same numbering they would in durable mode. The
                // discard happens at the sync point below in place of the
                // fsync, keeping `batch_buf` bounded.
                //
                // Replica mode: a primary-announced stream mark (rotation
                // boundary or chain check) may fall inside this batch.
                // `mark_split` bounds each encode span at the pending
                // mark; between spans the barrier below acts at exactly
                // the marked entry, then encoding resumes.
                self.refresh_pending_mark();
                let mut start = 0usize;
                loop {
                    let stop = self.mark_split(&batch, start, count);
                    let mut span_consumed = 0usize;
                    for slot in &batch[start..stop] {
                        if slot.event.is_shutdown() {
                            saw_shutdown = true;
                            break;
                        }
                        span_consumed += 1;
                        if slot.event.is_query() {
                            continue;
                        }
                        let seq = if slot.sequence != 0 {
                            self.encoder.set_next_sequence(slot.sequence + 1);
                            slot.sequence
                        } else {
                            self.encoder.allocate_sequence()
                        };
                        // Encode straight into the hand-off slot — the
                        // disk thread writes these exact bytes, so the
                        // split costs no copy.
                        self.claim_slot()?;
                        let chunk = self.claim.as_mut().expect("claimed above");
                        self.encoder
                            .encode_event(
                                chunk.bytes_mut(),
                                seq,
                                slot.timestamp_ns,
                                &slot.event,
                                slot.key_hash,
                                slot.request_seq,
                            )
                            .map_err(|e| {
                                JournalError::Io(std::io::Error::other(format!(
                                    "journal encode (sequencer, seq {seq}): {e}"
                                )))
                            })?;
                        let journal_slice = self
                            .encoder
                            .last_user_entry_replication_slice(chunk.bytes());
                        Self::record_slot_for_replication(&mut self.repl, journal_slice);
                    }
                    pending += span_consumed;
                    if first_write_ts.is_none() && span_consumed > 0 {
                        first_write_ts = Some(Instant::now());
                    }
                    if stop == count || saw_shutdown {
                        break;
                    }
                    // Mark barrier: the pending mark sits between
                    // batch[stop - 1] and batch[stop]. Chain checks
                    // resolve against the encoded chain — nothing has
                    // to reach the disk. A rotation must hand the batch
                    // over first, so the boundary the disk thread
                    // rotates at is exactly the marked entry. The
                    // progress target is the boundary slot's ring
                    // position — NOT the whole read batch, whose tail
                    // is not encoded yet.
                    self.apply_stream_marks(false)?;
                    if matches!(self.pending_mark, Some(StreamMark::Rotate(_))) {
                        if pending > 0 {
                            self.submit_batch(read_start + stop as u64)?;
                            pending = 0;
                            first_write_ts = None;
                        }
                        if !self.apply_stream_marks(true)?
                            && matches!(self.pending_mark, Some(StreamMark::Rotate(_)))
                        {
                            // The rotation failed and is backed off; the
                            // rest of this read batch sits past the
                            // boundary and cannot be encoded until the
                            // retry succeeds. Sleep instead of spinning
                            // hot on the barrier — the stage is stalled
                            // on storage either way.
                            std::thread::sleep(ADOPTION_STALL_POLL);
                        }
                    }
                    start = stop;
                }

                #[cfg(feature = "latency-trace")]
                batch_rec.record_elapsed(batch_start, mono_trace_ns());
            }

            // Sync when: we have data AND (batch full OR delay expired OR no delay).
            if pending > 0 {
                let should_sync = pending >= self.max_batch
                    || delay.is_zero()
                    || first_write_ts.is_some_and(|ts| ts.elapsed() >= delay);

                if should_sync {
                    // Everything read so far is encoded — publishing
                    // progress at `next_read` is exact here.
                    self.submit_batch(self.consumer.next_read())?;
                    self.maybe_publish_chain_check();
                    let _ = self.maybe_rotate();
                    // Replica mode: act on a mark that landed exactly at
                    // this batch's end — no later slot exists yet to
                    // trigger the mid-batch barrier, and waiting for one
                    // would leave it unapplied until traffic resumes.
                    // The batch is handed over, so the stream is
                    // quiesced at the boundary.
                    self.apply_stream_marks(true)?;

                    pending = 0;
                    first_write_ts = None;
                }
            } else {
                idle_count += 1;
                // Periodically flush utilization counters so the health
                // endpoint has a reasonably fresh view without adding
                // atomic stores on the busy path. Piggy-back the
                // stream-mark check on the same amortization: a trailing
                // mark (primary rotated or checked, then went quiet)
                // must be applied while idle, but not at the cost of a
                // mutex lock per idle spin. `pending == 0` here (this is
                // the no-pending branch), so the writer is quiesced.
                if idle_count.is_multiple_of(1024) {
                    self.utilization.busy.store(busy_count, Ordering::Relaxed);
                    self.utilization.idle.store(idle_count, Ordering::Relaxed);
                    // The lag gauge falls on this path, never on the
                    // submit path: the value stored at submit time is
                    // the batch just handed over, so it is at least 1
                    // by construction. Left to that store alone the
                    // gauge would freeze non-zero every time traffic
                    // stopped — a permanent phantom stall on a metric
                    // operators are told to alert on.
                    self.utilization
                        .journal_disk_lag
                        .store(self.batches.in_flight(), Ordering::Relaxed);
                    self.apply_stream_marks(true)?;
                }
                // Hand buffered latency samples to the stats registry
                // while quiesced — a recorder that stops recording keeps
                // its samples thread-local and they never reach a
                // snapshot. `AmortizedTimer` keeps the clock read off
                // the spin path.
                #[cfg(feature = "latency-trace")]
                if stats_flush_timer
                    .tick(
                        crate::trace::IDLE_FLUSH_INTERVAL,
                        self.busy_spin || idle_spins < 1000,
                    )
                    .is_some()
                {
                    wakeup_rec.flush();
                    batch_rec.flush();
                }
                idle_wait(&mut idle_spins, self.busy_spin);
            }

            if saw_shutdown {
                // Sentinel — by FIFO, every slot the receiver published
                // before it has now been consumed. Hand over any
                // encoded-but-unsubmitted events; `finish` waits for
                // them to land, so the persist-before-ack boundary
                // holds for the final batch too.
                if pending > 0 {
                    self.submit_batch(self.consumer.next_read())?;
                }
                self.utilization.busy.store(busy_count, Ordering::Relaxed);
                self.utilization.idle.store(idle_count, Ordering::Relaxed);
                #[cfg(feature = "pipeline-stats")]
                print_utilization("journal", busy_count, idle_count);
                return Ok(());
            }
        }
    }

    /// Append a slot to the in-progress `InputBatch` buffer for replication.
    /// Lazily initializes the buffer header on the first slot of each batch.
    /// Append the just-encoded journal entry's bytes to the InputBatch
    /// buffer. `journal_slice` comes from
    /// `BufferedWriter::last_user_entry_replication_slice` and is laid
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

    /// Emit a live-stream `ChainCheck` every
    /// [`CHAIN_CHECK_INTERVAL_BATCHES`] published batches: this node's
    /// chain value at its current position, for replicas to compare
    /// against their own. Called right after a batch publish, so the
    /// frame lands after the entries it covers. No-op on standalone
    /// nodes and with `hash-chain` disabled.
    fn maybe_publish_chain_check(&mut self) {
        if !self.repl.any_producer() {
            return;
        }
        self.repl.batches_since_chain_check += 1;
        if self.repl.batches_since_chain_check < CHAIN_CHECK_INTERVAL_BATCHES {
            return;
        }
        self.repl.batches_since_chain_check = 0;
        let Some(hash) = self.encoder.chain_hash() else {
            return;
        };
        let sequence = self.encoder.next_sequence() - 1;
        // Local buffer: the frame is 45 bytes and checks are sparse.
        let mut buf = Vec::with_capacity(64);
        crate::replication::protocol::encode_chain_check(sequence, &hash, &mut buf);
        Self::publish_to_replication_rings(
            &mut self.repl.producers,
            &self.repl.evict,
            &self.repl.active,
            &buf,
            sequence,
        );
    }

    /// Publish a `Rotate` frame to the replication rings, announcing
    /// the boundary the writer just rotated at. Called immediately
    /// after a successful rotation: `next_sequence - 1` is the outgoing
    /// segment's last sequence, and the fresh (empty) live segment's
    /// chain value is its header anchor — the outgoing segment's tail.
    /// An evicted ring is skipped exactly like a data batch would be:
    /// the replica re-learns the boundary from journal catch-up on
    /// reconnect.
    fn publish_rotate_to_rings(repl: &mut ReplicationState, encoder: &JournalEncoder<E>) {
        if !repl.any_producer() {
            return;
        }
        let boundary_seq = encoder.next_sequence() - 1;
        let tail_hash = encoder.chain_hash().unwrap_or([0u8; 32]);
        // Local buffer: the frame is 45 bytes and rotations are cold —
        // not worth a dedicated reusable buffer on ReplicationState.
        let mut buf = Vec::with_capacity(64);
        crate::replication::protocol::encode_rotate(boundary_seq, &tail_hash, &mut buf);
        Self::publish_to_replication_rings(
            &mut repl.producers,
            &repl.evict,
            &repl.active,
            &buf,
            boundary_seq,
        );
    }

    /// Publish post-fsync writer state to optional readers:
    /// [`FsyncState`] (for shadow snapshots and replica handshakes) and
    /// `last_seq` (highest journal sequence durably persisted, used by the
    /// replica orchestrator on reconnect handshakes). Both `Option`s are
    /// independent; either can be set or unset. Called once per fsync
    /// batch (cold path); each `if let Some` is a single branch on a small
    /// struct field.
    /// Claim a hand-off slot if the batch doesn't have one yet.
    ///
    /// A full ring means the disk thread is behind by the whole staging
    /// depth. Spinning here is the backpressure: the sequencer stops
    /// draining the input disruptor, which backpressures its producers
    /// — the same chain a slow journal has always had, with a deeper
    /// buffer in front of it.
    fn claim_slot(&mut self) -> Result<(), JournalError> {
        if self.claim.is_some() {
            return Ok(());
        }
        let mut idle_spins: u32 = 0;
        loop {
            match self.batches.try_claim() {
                Ok(claim) => {
                    self.claim = Some(claim);
                    return Ok(());
                }
                Err(_) => {
                    // A poisoned disk thread never drains again — the
                    // spin would be forever.
                    if self.disk.poisoned() {
                        return Err(self.take_poison());
                    }
                    idle_wait(&mut idle_spins, self.busy_spin);
                }
            }
        }
    }

    /// Hand the encoded batch to the disk thread: publish the
    /// accumulated `InputBatch` frame to the replication rings first
    /// (the frame was built alongside the journal-codec writes via
    /// `record_slot_for_replication`), then publish the slot with
    /// everything the disk thread must make true once the batch is
    /// durable.
    ///
    /// `progress` is the ring position of the last slot this batch
    /// covers — an explicit value, NOT `Consumer::commit`, because
    /// `commit` publishes `next_read`, which after a `read_batch` spans
    /// the WHOLE read batch. At the mid-batch mark barrier only a
    /// prefix of the batch has been encoded; publishing `next_read`
    /// there would let the replica ack entries past the boundary that
    /// are not yet journaled (the ack path gates on this cursor —
    /// persist-before-ack). The steady-state caller passes
    /// `consumer.next_read()`, which is then equivalent to `commit`.
    ///
    /// Returns once the batch is *submitted*, not once it is durable.
    /// That is the point of the split: the sequencer keeps encoding and
    /// feeding replicas while the device works.
    fn submit_batch(&mut self, progress: u64) -> Result<(), JournalError> {
        if self.repl.any_producer() {
            let end_seq = self.encoder.next_sequence() - 1;
            Self::publish_input_batch_to_rings(&mut self.repl, end_seq);
        }

        // A batch of nothing but queries encodes no bytes, yet its ring
        // slots still have to be released upstream — so it travels as a
        // zero-length batch carrying only cursors. `no-persist` uses
        // the same shape: "skip the write syscall", not "skip
        // everything that follows" (the replication publish above must
        // still run or the response stage's replication-cursor gate
        // deadlocks).
        #[cfg(not(feature = "no-persist"))]
        let len = self.encoder.batch_len() as u32;
        #[cfg(feature = "no-persist")]
        let len = 0u32;

        let meta = JournalWriteMeta {
            len,
            journal_seq: self.encoder.next_sequence().saturating_sub(1),
            // ~400 ns of BLAKE3 per batch, skipped outright when no
            // publisher is attached — see `chain_hash_observed`.
            chain_hash: if self.chain_hash_observed {
                self.encoder.chain_hash().unwrap_or([0u8; 32])
            } else {
                [0u8; 32]
            },
            ring_progress: progress,
            input_ring_seq: self.consumer.next_read(),
        };

        // Every batch needs a slot, even an empty one, because the slot
        // is what carries the cursors.
        self.claim_slot()?;
        let claim = self.claim.take().expect("claimed above");
        self.batches.publish(claim, meta);
        self.encoder.clear_batch();
        self.segment_bytes += u64::from(len);
        // One `Relaxed` store per batch, off the encode path — the only
        // cost of making a stalling disk visible to operators.
        self.utilization
            .journal_disk_lag
            .store(self.batches.in_flight(), Ordering::Relaxed);
        Ok(())
    }

    /// Block until every submitted batch is durable and its cursors are
    /// published.
    ///
    /// The rotation rendezvous and shutdown both need this: the disk
    /// thread must have nothing in flight before the segment is swapped
    /// or the thread is stopped.
    fn drain_disk(&mut self) -> Result<(), JournalError> {
        let mut idle_spins: u32 = 0;
        while !self.batches.drained() {
            if self.disk.poisoned() {
                return Err(self.take_poison());
            }
            idle_wait(&mut idle_spins, self.busy_spin);
        }
        Ok(())
    }

    /// The failure the disk thread latched, with a fallback for the
    /// (unreachable) case of a poison flag without a stored cause.
    fn take_poison(&self) -> JournalError {
        self.disk.take_error().unwrap_or_else(|| {
            JournalError::Io(std::io::Error::other(
                "journal disk thread failed without recording a cause",
            ))
        })
    }

    /// Stop the disk thread and discard the segment. The journal is
    /// already broken (or the stream hit a fatal condition), so there is
    /// nothing to hand back — but the thread must not be left running.
    fn halt_disk_thread(&mut self) {
        self.disk.stop();
        if let Some(handle) = self.disk_thread.take()
            && handle.join().is_err()
        {
            tracing::error!("journal-disk thread panicked");
        }
    }

    /// Wait for everything submitted to land, stop the disk thread, and
    /// reassemble the writer from both halves.
    ///
    /// Consumes the sequencer: the stream half it owns becomes half of
    /// the returned writer, which recovery and promotion then drive
    /// single-threaded.
    fn finish(mut self) -> Result<BufferedWriter<E>, JournalError> {
        // A drain failure still has to stop the thread — the journal is
        // broken, but leaking a spinning thread on top of that helps
        // nobody.
        if let Err(e) = self.drain_disk() {
            self.halt_disk_thread();
            return Err(e);
        }
        self.disk.stop();
        let segment = match self.disk_thread.take() {
            Some(handle) => handle.join().map_err(|_| {
                JournalError::Io(std::io::Error::other("journal-disk thread panicked"))
            })?,
            None => {
                return Err(JournalError::Io(std::io::Error::other(
                    "journal-disk thread already joined",
                )));
            }
        };
        // A poison latched during the final drain still has to surface:
        // the batches it covers were never made durable.
        if self.disk.poisoned() {
            return Err(self.take_poison());
        }
        Ok(BufferedWriter::from_halves(self.encoder, segment))
    }

    /// Rotate the live journal segment if a trigger has fired.
    ///
    /// Called at fsync boundaries. Two triggers: a manual
    /// `rotate_requested` flag (consumed via CAS so duplicate signals
    /// collapse into one rotation) and a size threshold against the
    /// live segment's on-disk size. After a successful rotation,
    /// re-publishes the chain hash so shadow observers pick up the new
    /// genesis-anchored value.
    ///
    /// Errors are logged but do not abort the pipeline: the live
    /// segment is restored by `BufferedWriter::rotate_segment` on
    /// failure, so the next batch can continue writing to it.
    ///
    /// Returns `true` when a rotation actually happened.
    #[inline]
    fn maybe_rotate(&mut self) -> bool {
        let Some(manual) = self.local_rotation_armed() else {
            return false;
        };
        let pre_size = self.segment_bytes;
        let (rotate_result, used_fast_path) = self.rotate_taking_prepared();
        self.finish_local_rotation(rotate_result, manual, used_fast_path, pre_size)
    }

    /// Rotate, adopting a pre-staged sidecar segment when the
    /// background preparer (spawned by `enable_preparer`) has one
    /// ready; synchronous `rotate_segment`
    /// otherwise. Returns the rotation result and whether the fast
    /// path was used — the single fast-path selection point shared by
    /// size/manual rotation and replica boundary adoption, so a policy
    /// change (e.g. rejecting a stale-sized prepared segment) cannot
    /// skew the two triggers apart.
    ///
    /// A rotation is the one place the sequencer waits for the disk
    /// thread. It has to: the outgoing segment must hold everything
    /// encoded before the boundary, the new segment's header carries
    /// values only the stream half knows (the boundary sequence and the
    /// chain tail), and the `Rotate` frame replicas receive must
    /// reflect what actually happened. The wait is one drain plus the
    /// rotation itself — strictly less than the inline rotation it
    /// replaces, and a few times per gigabyte.
    fn rotate_taking_prepared(&mut self) -> (Result<std::path::PathBuf, JournalError>, bool) {
        // Encoded-but-unsubmitted bytes belong to the OUTGOING segment.
        // Every caller reaches here right after a submit (steady state,
        // mark barrier) or with nothing encoded (idle), so this holds by
        // construction — but silently rotating over a pending batch
        // would write those bytes into the new segment and misframe
        // both journals, so pin it.
        debug_assert_eq!(
            self.encoder.batch_len(),
            0,
            "rotation with an unsubmitted batch would move it into the new segment"
        );
        if let Err(e) = self.drain_disk() {
            return (Err(e), false);
        }
        let prepared = self.preparer.as_ref().and_then(|p| p.take());
        let used_fast_path = prepared.is_some();
        // No sequence is consumed: the next event still gets
        // `next_sequence`, and the new segment's header anchor is the
        // outgoing segment's tail — which makes the chain value across
        // the boundary unchanged.
        let starting_sequence = self.encoder.next_sequence();
        let anchor_hash = self.encoder.chain_hash().unwrap_or([0u8; 32]);
        self.disk.request_rotation(RotateRequest {
            prepared,
            starting_sequence,
            anchor_hash,
        });

        let result = self.await_rotation();
        if result.is_ok() {
            self.encoder.begin_segment(starting_sequence, anchor_hash);
            self.segment_bytes = melin_journal::codec::ENTRY_OFFSET;
        }
        (result, used_fast_path)
    }

    /// Wait for the disk thread's rotation outcome.
    fn await_rotation(&mut self) -> Result<std::path::PathBuf, JournalError> {
        let mut idle_spins: u32 = 0;
        loop {
            if let Some(result) = self.disk.take_rotation_result() {
                return result;
            }
            // A poisoned thread has stopped and will never answer.
            if self.disk.poisoned() {
                return Err(self.take_poison());
            }
            idle_wait(&mut idle_spins, self.busy_spin);
        }
    }

    /// Trigger/guard half of [`JournalStage::maybe_rotate`]: consume the
    /// manual flag (CAS so duplicate signals collapse into one
    /// rotation), evaluate the size trigger and the failure backoff,
    /// skip empty-live rotations, and pre-publish pending replication
    /// bytes. Returns `Some(manual)` when the rotation should proceed.
    fn local_rotation_armed(&mut self) -> Option<bool> {
        let manual = self
            .rotate_requested
            .as_ref()
            .map(|f| {
                f.compare_exchange(true, false, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
            })
            .unwrap_or(false);
        let size_triggered =
            self.max_journal_bytes > 0 && self.segment_bytes >= self.max_journal_bytes;
        if !(manual || size_triggered) {
            return None;
        }
        // Suppress size-driven retries during the backoff window so a
        // permanent failure (ENOSPC, RO-FS) doesn't re-arm every batch
        // and flood the error log. Manual triggers always proceed —
        // operators expect a fresh attempt and a fresh error per
        // command.
        if !manual
            && let Some(until) = self.rotation_backoff_until
            && Instant::now() < until
        {
            return None;
        }
        // Never rotate an empty live segment: it would archive an
        // entry-less file the replicas have no reason to mirror, and a
        // boundary already exists at this exact sequence. Also keeps
        // "live starts at boundary+1" an unambiguous already-adopted
        // signal on replicas (no zero-length segment can sit between).
        if self.encoder.next_sequence() == self.encoder.segment_starting_sequence() {
            if manual {
                tracing::info!(
                    next_sequence = self.encoder.next_sequence(),
                    "manual rotation skipped: live segment is empty (already at a boundary)"
                );
            }
            return None;
        }
        // Entries encoded for replication but not yet published belong
        // to the outgoing segment (the rotation drains everything
        // submitted into it), so they must precede the Rotate frame on
        // the rings.
        if self.repl.any_producer() {
            let end_seq = self.encoder.next_sequence() - 1;
            Self::publish_input_batch_to_rings(&mut self.repl, end_seq);
        }
        Some(manual)
    }

    /// Record a completed rotation's path in the shared utilization
    /// counters (surfaced on `GET /metrics` so operators can verify
    /// the fallback count stays at zero in steady state).
    fn count_rotation(&self, used_fast_path: bool) {
        let counter = if used_fast_path {
            &self.utilization.rotations_fast_path
        } else {
            &self.utilization.rotations_sync_fallback
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Bookkeeping half of the local-rotation twins. On success:
    /// counters, log, backoff reset, preparer re-arm, fsync-state
    /// republish (rotation consumes no sequence and the chain value is
    /// unchanged — the new segment's anchor *is* the old tail — but
    /// observers need a state consistent with the new on-disk layout),
    /// and the `Rotate` announce so replicas rotate at exactly the same
    /// sequence. On failure: error log + retry backoff + preparer
    /// re-arm. Returns whether a rotation happened.
    fn finish_local_rotation(
        &mut self,
        rotate_result: Result<std::path::PathBuf, JournalError>,
        manual: bool,
        used_fast_path: bool,
        pre_size: u64,
    ) -> bool {
        match rotate_result {
            Ok(archived) => {
                self.count_rotation(used_fast_path);
                tracing::info!(
                    archive = %archived.display(),
                    pre_rotate_bytes = pre_size,
                    next_sequence = self.encoder.next_sequence(),
                    trigger = if manual { "manual" } else { "size" },
                    fast_path = used_fast_path,
                    rotations_fast_path =
                        self.utilization.rotations_fast_path.load(Ordering::Relaxed),
                    rotations_sync_fallback = self
                        .utilization
                        .rotations_sync_fallback
                        .load(Ordering::Relaxed),
                    "journal segment rotated"
                );
                self.rotation_backoff_until = None;
                // Kick the preparer to start staging the *next*
                // segment ahead of the next rotation, tuning the
                // zero-fill target to the segment size just observed
                // (a replica's only source of truth for the primary's
                // segment size).
                if let Some(p) = self.preparer.as_ref() {
                    p.arm_with_observed_len(pre_size);
                }
                // No cursor republish: a rotation consumes no sequence
                // and the new segment's anchor *is* the outgoing tail,
                // so `journal_seq` and the chain value are unchanged —
                // and the rendezvous drained the ring, so ring progress
                // is unchanged too. The state observers already hold is
                // the state after the rotation.
                Self::publish_rotate_to_rings(&mut self.repl, &self.encoder);
                true
            }
            Err(e) => {
                self.utilization
                    .rotations_failed
                    .fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    error = %e,
                    trigger = if manual { "manual" } else { "size" },
                    fast_path = used_fast_path,
                    backoff_secs = ROTATION_FAILURE_BACKOFF.as_secs(),
                    "journal segment rotation failed; continuing with current segment"
                );
                self.rotation_backoff_until = Some(Instant::now() + ROTATION_FAILURE_BACKOFF);
                // Re-arm the preparer so the next attempt also has a
                // chance at the fast path, even after a transient
                // failure of the writer's rotate path.
                if let Some(p) = self.preparer.as_ref() {
                    p.arm();
                }
                false
            }
        }
    }

    /// Pull the next primary-announced stream mark off the queue into
    /// `pending_mark`. One `Option` check in steady state; the mutex is
    /// only touched when a mark is actually outstanding or the local
    /// copy is empty at a batch/sync boundary.
    #[inline]
    fn refresh_pending_mark(&mut self) {
        if self.pending_mark.is_none()
            && let Some(q) = &self.stream_marks
        {
            self.pending_mark = q
                .lock()
                .expect("stream-mark queue poisoned (receiver thread panicked)")
                .pop_front();
        }
    }

    /// Index of the first slot in `batch[start..count]` past the
    /// pending mark's position, or `count` when none is (no mark
    /// pending, or every slot is at or below the position). Slots with
    /// `sequence == 0` (locally injected, never journaled) sort below
    /// any position and stay in the current span.
    #[inline]
    fn mark_split(&self, batch: &[InputSlot<E>], start: usize, count: usize) -> usize {
        let Some(m) = self.pending_mark else {
            return count;
        };
        let position = m.sequence();
        for (i, slot) in batch[start..count].iter().enumerate() {
            if slot.sequence > position {
                return start + i;
            }
        }
        count
    }

    /// Resolve pending stream marks against the writer position.
    /// Chain checks apply inline (the chain is a pure function of
    /// encoded bytes); consecutive resolved marks are drained in order.
    /// A rotation due at the current position is verified and returned
    /// for the caller to perform — but only when `quiesced` (nothing
    /// buffered or in flight on the writer); otherwise it stays pending
    /// for a later, quiesced call.
    ///
    /// `Err` means divergence (local chain disagrees with the
    /// primary's) or an ordering violation; either tears the pipeline
    /// down, and the reconnect handshake routes the replica to snapshot
    /// resync.
    fn resolve_stream_marks(
        &mut self,
        quiesced: bool,
    ) -> Result<Option<AdoptedRotation>, JournalError> {
        loop {
            self.refresh_pending_mark();
            let Some(mark) = self.pending_mark else {
                return Ok(None);
            };
            let position = mark.sequence();
            let at = self.encoder.next_sequence() - 1;
            if at < position {
                return Ok(None);
            }
            match mark {
                StreamMark::ChainCheck {
                    sequence,
                    chain_hash,
                } => {
                    if at > sequence {
                        // The receiver pushes marks before publishing any
                        // slot past their position, and the encode loops
                        // split batches at pending marks — the writer can
                        // never legitimately pass one. A bug, not
                        // divergence.
                        return Err(JournalError::Io(std::io::Error::other(format!(
                            "chain-check position {sequence} already passed (writer at \
                             {at}) — mark/stream ordering bug"
                        ))));
                    }
                    // `None` only with `hash-chain` disabled — nothing to
                    // compare, the check passes by construction.
                    let local = self.encoder.chain_hash().unwrap_or(chain_hash);
                    if local != chain_hash {
                        return Err(JournalError::ReplicaChainDivergence {
                            sequence,
                            expected: chain_hash,
                            actual: local,
                        });
                    }
                    self.pending_mark = None;
                    // Loop: the next mark may sit at this same position.
                }
                StreamMark::Rotate(r) => {
                    // Duplicate announce of a boundary this journal already
                    // rotated at (the live segment starts right past it).
                    // Possible only through redundant delivery — e.g. a
                    // catch-up walk re-emitting a boundary a held ring chunk
                    // also carries — never through a second real rotation:
                    // the primary skips empty-live rotations, so two
                    // distinct rotations at one sequence cannot exist.
                    if self.encoder.segment_starting_sequence() == r.boundary_seq + 1 {
                        self.pending_mark = None;
                        continue;
                    }
                    if at > r.boundary_seq {
                        return Err(JournalError::Io(std::io::Error::other(format!(
                            "adopted rotation boundary {} already passed (writer at {at}) \
                             — rotation/stream ordering bug",
                            r.boundary_seq
                        ))));
                    }
                    if !quiesced {
                        return Ok(None);
                    }
                    // Skip the comparison when either side cannot produce
                    // a chain value: locally `chain_hash()` is `None`
                    // without the `hash-chain` feature, and an all-zeros
                    // announce means the *primary* runs without it (a
                    // real BLAKE3 tail is never zeros — the boundary has
                    // absorbed at least one entry). Mirrors the
                    // ChainCheck arm and the handshake validator: a
                    // mixed-feature pair runs with reduced verification
                    // instead of falsely diverging on every rotation.
                    let local_tail = self.encoder.chain_hash().unwrap_or(r.tail_hash);
                    if r.tail_hash != [0u8; 32] && local_tail != r.tail_hash {
                        return Err(JournalError::ReplicaChainDivergence {
                            sequence: r.boundary_seq,
                            expected: r.tail_hash,
                            actual: local_tail,
                        });
                    }
                    return Ok(Some(r));
                }
            }
        }
    }

    /// Bookkeeping after a successful adopted rotation.
    fn finish_adoption(&mut self, boundary_seq: u64, used_fast_path: bool) {
        self.count_rotation(used_fast_path);
        tracing::info!(
            boundary_seq,
            fast_path = used_fast_path,
            "adopted primary-announced rotation (chain verified at boundary)"
        );
        self.pending_mark = None;
        // The queue may hold a successor mark that falls inside the
        // same already-read ring batch (a ChainCheck typically trails a
        // Rotate; catch-up can deliver two Rotates back-to-back). The
        // encode loops bound spans with `mark_split`, which only reads
        // `pending_mark` — left `None`, the rest of the batch would be
        // encoded straight past the successor, and the next resolve
        // would report it "already passed" and kill the stage.
        self.refresh_pending_mark();
        // Nothing to republish — see `finish_local_rotation`.
    }

    /// True while a failed rotation attempt's backoff window is still
    /// open; clears the window once it expires.
    fn rotation_backed_off(&mut self) -> bool {
        match self.rotation_backoff_until {
            Some(until) if Instant::now() < until => true,
            Some(_) => {
                self.rotation_backoff_until = None;
                false
            }
            None => false,
        }
    }

    /// Failure half of an adopted rotation — the replica twin of
    /// `finish_local_rotation`'s error arm. The mark stays pending
    /// (`mark_split` keeps every encode span bounded at the boundary,
    /// so no entry can pass it) and the rotation is retried after the
    /// backoff: a transient failure (ENOSPC clearing, a brief RO
    /// remount) must stall the stage — honest backpressure toward the
    /// primary — not kill the replica process. Unlike the primary, the
    /// replica cannot keep writing into the current segment: a
    /// post-boundary entry there would misframe the mirror and force a
    /// full resync.
    fn fail_adoption(&mut self, boundary_seq: u64, e: &JournalError) {
        self.utilization
            .rotations_failed
            .fetch_add(1, Ordering::Relaxed);
        tracing::error!(
            error = %e,
            boundary_seq,
            backoff_secs = ROTATION_FAILURE_BACKOFF.as_secs(),
            "adopted rotation failed; stalling at the boundary until retry"
        );
        self.rotation_backoff_until = Some(Instant::now() + ROTATION_FAILURE_BACKOFF);
        // Re-arm the preparer so the retry has a shot at the fast path.
        if let Some(p) = self.preparer.as_ref() {
            p.arm();
        }
    }

    /// Apply pending stream marks: chain checks inline, and — when
    /// `quiesced` and the writer sits exactly at a verified boundary —
    /// the rotation itself (adopting a pre-staged segment when the
    /// preparer has one ready, synchronous `rotate_segment` otherwise).
    ///
    /// Returns `Ok(true)` when a rotation happened, `Ok(false)` when
    /// there was nothing (left) to do or the rotation failed and is
    /// backed off for retry (the mark stays pending), and `Err` on
    /// divergence or an ordering violation.
    fn apply_stream_marks(&mut self, quiesced: bool) -> Result<bool, JournalError> {
        let Some(r) = self.resolve_stream_marks(quiesced)? else {
            return Ok(false);
        };
        if self.rotation_backed_off() {
            return Ok(false);
        }
        // Verified: the local tail equals the primary's, so rotating
        // here anchors the new segment identically on both nodes.
        // Adopt a pre-staged segment when one is ready — on a replica
        // the rotation stall sits on the ack path, so the fast path
        // matters *more* here than on the primary.
        let pre_size = self.segment_bytes;
        let (rotate_result, used_fast_path) = self.rotate_taking_prepared();
        // Re-arm regardless of outcome so the next boundary also has a
        // chance at the fast path, tuning the zero-fill target to the
        // observed segment size (the replica's only source of truth
        // for the primary's segment size).
        if let Some(p) = self.preparer.as_ref() {
            p.arm_with_observed_len(pre_size);
        }
        match rotate_result {
            Ok(_) => {
                self.finish_adoption(r.boundary_seq, used_fast_path);
                Ok(true)
            }
            Err(e) => {
                self.fail_adoption(r.boundary_seq, &e);
                Ok(false)
            }
        }
    }

    /// Drain any remaining entries from the ring buffer on shutdown.
    ///
    /// Replica-mode caveat (accepted race): pending stream marks are
    /// NOT applied here, so a shutdown racing a primary rotation can
    /// journal post-boundary entries into the pre-boundary segment.
    /// That misframes the local journal but loses nothing — the entries
    /// are intact and the reconnect handshake detects the framing
    /// mismatch (segment-scoped chains differ), archiving the journal
    /// and re-seeding from the primary. Honoring marks here would need
    /// the full barrier machinery on a path that must never fail;
    /// self-healing via resync is the safer trade.
    fn drain_remaining(&mut self, batch: &mut [InputSlot<E>]) {
        loop {
            let count = self.consumer.read_batch(batch, MAX_JOURNAL_BATCH);
            if count == 0 {
                break;
            }
            for slot in &batch[..count] {
                if slot.event.is_query() || slot.event.is_shutdown() {
                    // Sentinel is never persisted (codec rejects it).
                    // Reaching this path means the shutdown flag fired
                    // before the sentinel was consumed — skip it.
                    continue;
                }
                let seq = if slot.sequence != 0 {
                    self.encoder.set_next_sequence(slot.sequence + 1);
                    slot.sequence
                } else {
                    self.encoder.allocate_sequence()
                };
                if let Err(e) = self.claim_slot() {
                    tracing::error!(error = %e, "journal hand-off failed on drain");
                    return;
                }
                let chunk = self.claim.as_mut().expect("claimed above");
                if let Err(e) = self.encoder.encode_event(
                    chunk.bytes_mut(),
                    seq,
                    slot.timestamp_ns,
                    &slot.event,
                    slot.key_hash,
                    slot.request_seq,
                ) {
                    tracing::error!(error = %e, "journal encode error on drain");
                    continue;
                }
                let journal_slice = self
                    .encoder
                    .last_user_entry_replication_slice(chunk.bytes());
                Self::record_slot_for_replication(&mut self.repl, journal_slice);
            }

            // Hand the batch over exactly as the steady-state path
            // does — replication frame first, then the slot carrying
            // the cursors. `finish` waits for it to become durable.
            if let Err(e) = self.submit_batch(self.consumer.next_read()) {
                tracing::error!(error = %e, "journal hand-off failed on drain");
                return;
            }
        }
    }
}

impl<E: AppEvent> JournalStage<E> {
    /// Drive the journal stage to completion, returning the writer on
    /// clean shutdown.
    #[inline]
    pub fn run(
        self,
        shutdown: &std::sync::atomic::AtomicBool,
    ) -> Result<BufferedWriter<E>, JournalError> {
        // Under `no-persist` the sync point discards batches instead of
        // writing, `valid_end` never advances, and the size trigger can
        // never fire — arming the preparer would zero-write a full
        // segment at boot for a staging file nothing ever adopts.
        #[cfg(feature = "no-persist")]
        {
            self.run_sync(shutdown)
        }
        #[cfg(not(feature = "no-persist"))]
        {
            let mut stage = self;
            stage.enable_preparer();
            stage.run_sync(shutdown)
        }
    }

    /// Spawn the background segment preparer in zero-fill mode. Called
    /// from the stage's `run` before entering `run_sync`; no-op if
    /// already spawned. Arming policy lives in
    /// [`JournalStage::arm_preparer_if_recurring`].
    ///
    /// Zero-fill — physical zeros, not `FALLOC_FL_ZERO_RANGE` — is the
    /// load-bearing detail: appends into pre-written extents generate
    /// no extent-conversion metadata, so `flush_batch_sync`'s
    /// `fdatasync` stays on its data-only fast path instead of
    /// periodically forcing the filesystem journal on the order
    /// pipeline's critical path (see the preparer module docs and
    /// `docs/internal/journal-fsync-beat-2026-08.md`).
    pub fn enable_preparer(&mut self) {
        self.arm_preparer_if_recurring(|stage| {
            SegmentPreparer::spawn_zero_fill(
                stage.writer.path().to_path_buf(),
                stage.max_journal_bytes,
                stage.preparer_core,
            )
        });
    }

    /// Spawn a preparer via `spawn` iff rotation recurs on a
    /// predictable cadence — size-driven rotation, or replica mode
    /// (primary-announced boundaries arrive at the primary's cadence;
    /// a replica's rotation stall sits on the ack path, where under
    /// `hybrid`/`durably-replicated` gating it delays the primary's
    /// durability gate, so the fast path matters *more* there than on
    /// the primary itself). Deliberately NOT armed for manual-only
    /// rotation (`ROTATE` with `max_journal_bytes == 0`): the cadence
    /// is unpredictable and the staged segment's disk + thread cost may
    /// never pay off.
    fn arm_preparer_if_recurring(&mut self, spawn: impl FnOnce(&Self) -> SegmentPreparer) {
        let rotation_recurs = self.max_journal_bytes > 0 || self.stream_marks.is_some();
        if rotation_recurs && self.preparer.is_none() {
            self.preparer = Some(spawn(self));
        }
    }

    /// Set the core the preparer worker pins itself to (`0` =
    /// unpinned). Call before the stage runs — `enable_preparer` reads
    /// it at spawn time; changing it afterwards has no effect on an
    /// already-running worker.
    pub fn set_preparer_core(&mut self, core: usize) {
        self.preparer_core = core;
    }

    /// Test-only probe: whether `enable_preparer` armed the preparer.
    #[cfg(test)]
    pub(crate) fn preparer_enabled(&self) -> bool {
        self.preparer.is_some()
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
    /// Durable-wire-seq cursor for reading the highest durably-persisted
    /// sequence. Feeds `ApplyCtx.journal_sequence` (read by `QueryStats`),
    /// in the same wire-seq space as the health endpoint's `journal_seq`
    /// gauge so the two operator surfaces agree. One `Acquire` load per
    /// batch — no extra cross-thread synchronization on the hot path.
    durable_wire_seq: DurableWireSeqCursor,
    /// Active connection count, shared with the server accept loop.
    /// Read only when processing `QueryStats` (once per second at most).
    active_connections: Arc<AtomicU64>,
    /// When `Some`, replication is enabled. One Relaxed load per event
    /// (~1ns). `0` = no replicas connected → reject all mutations.
    /// `None` = standalone mode → no halt check.
    replicas_connected: Option<Arc<AtomicU32>>,
    /// Replication fencing state. Advanced when an `EpochBump` event is
    /// processed (recovery replay, live replication stream, or local
    /// promotion injection); the halt check folds in its `is_fenced()`
    /// latch so a fenced node stops accepting client writes. One Relaxed
    /// load per disruptor batch (hoisted alongside the replica-count
    /// load), shared with the response stage and replication threads.
    fence_state: Arc<crate::fence::FenceState>,
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
    /// Wire-seq counter shadowing the journal stage's allocator. Stamped
    /// into `OutputSlot.wire_seq` so the response stage's durability gate
    /// can compare against replica metrics in the same space. Initialised
    /// to the journal writer's `starting_sequence` and advanced for each
    /// event the journal would allocate (App non-query, Tick); held flat
    /// for events the journal skips (Query). The skip-rule mirrors
    /// `JournalStage::run` exactly — any drift would re-introduce the
    /// off-by-one the wire-seq field exists to eliminate. Tests pin this
    /// invariant.
    next_wire_seq: u64,
}

impl<A: Application> MatchingStage<A> {
    /// Create a new matching stage.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        app: A,
        consumer: ring::Consumer<InputSlot<A::Event>>,
        output: ring::Producer<OutputSlot<A::Report, A::QueryResponse>>,
        events_processed: Arc<AtomicU64>,
        durable_wire_seq: DurableWireSeqCursor,
        active_connections: Arc<AtomicU64>,
        replicas_connected: Option<Arc<AtomicU32>>,
        fence_state: Arc<crate::fence::FenceState>,
        busy_spin: bool,
        starting_wire_seq: u64,
    ) -> Self {
        Self {
            app,
            consumer,
            output,
            events_processed,
            durable_wire_seq,
            active_connections,
            replicas_connected,
            fence_state,
            busy_spin,
            utilization: Arc::new(StageUtilization::new()),
            last_drain_ns: 0,
            next_wire_seq: starting_wire_seq,
        }
    }

    /// Shared utilization counters for health endpoint monitoring.
    pub fn utilization(&self) -> Arc<StageUtilization> {
        Arc::clone(&self.utilization)
    }

    /// Returns true if trading is halted: either all replicas have
    /// disconnected (durability can't be honoured) or the node has been
    /// fenced by a higher epoch (superseded after a promotion). Always
    /// false for the replica-disconnect cause in standalone mode
    /// (`replicas_connected` is None); the fence latch is checked
    /// unconditionally but can only be set when replication is active.
    fn is_halted(&self) -> bool {
        self.fence_state.is_fenced()
            || self
                .replicas_connected
                .as_ref()
                .is_some_and(|count| count.load(Ordering::Relaxed) == 0)
    }

    /// Reject reason a halted node returns to clients: `Superseded` when
    /// the fence latched (a higher epoch demoted us), `ReplicaDisconnected`
    /// otherwise. Fence takes priority — a fenced node is shutting down
    /// regardless of replica count. The run loop inlines this same rule
    /// (it can't borrow `self` while the peeked batch is live); keep the two
    /// in sync.
    fn halt_reject_reason(&self) -> RejectReason {
        if self.fence_state.is_fenced() {
            RejectReason::Superseded
        } else {
            RejectReason::ReplicaDisconnected
        }
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
        let mut wakeup_rec =
            crate::trace::register_stage("matching: disruptor wakeup (publish → matching consume)");
        #[cfg(feature = "latency-trace")]
        let mut execute_rec = crate::trace::register_stage("matching: execute (process_event)");
        // Paces the idle-path recorder flush. See `trace::StageRecorder::flush`.
        #[cfg(feature = "latency-trace")]
        let mut stats_flush_timer = melin_app::amortized_timer::AmortizedTimer::new();

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
                // Hand buffered latency samples to the stats registry
                // while idle — see the journal stage for why.
                #[cfg(feature = "latency-trace")]
                if stats_flush_timer
                    .tick(
                        crate::trace::IDLE_FLUSH_INTERVAL,
                        self.busy_spin || idle_spins < 1000,
                    )
                    .is_some()
                {
                    wakeup_rec.flush();
                    execute_rec.flush();
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
            // Two Relaxed loads + one Acquire load per batch instead of
            // per event.
            let mut ctx = ApplyCtx {
                now_ns: 0,
                journal_sequence: self.durable_wire_seq.load(),
                active_connections: self.active_connections.load(Ordering::Relaxed),
                events_processed: local_events,
                key_hash: 0,
            };

            let mut saw_shutdown = false;

            // Halt status is constant for the duration of one disruptor
            // batch (replica counts and the fence latch only change
            // between batches in practice). Two Relaxed loads per batch,
            // hoisted out of the per-event branch. Spelled as field
            // accesses rather than `self.is_halted()` (the consumer's
            // peeked batch keeps `self` mutably borrowed) — must stay in
            // sync with that method: a fenced node must stop applying
            // client writes on the *live* path too, or it keeps extending
            // the superseded journal lineage until the shutdown sentinel
            // arrives (the response stage only drops the acks).
            let fenced = self.fence_state.is_fenced();
            let halted = fenced
                || self
                    .replicas_connected
                    .as_ref()
                    .is_some_and(|count| count.load(Ordering::Relaxed) == 0);
            // Reason a halted node hands back to clients: `Superseded` when
            // the fence latched (a higher epoch demoted us), else
            // `ReplicaDisconnected`. Fence wins — a fenced node is shutting
            // down regardless of replica count. A `Copy` enum captured here
            // so the per-event reject site needs no fresh `self` borrow.
            let halt_reason = if fenced {
                RejectReason::Superseded
            } else {
                RejectReason::ReplicaDisconnected
            };

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
                // Wire seq for this event, in the same space as
                // `metrics.in_memory_sequence` / the journal stage's
                // allocator. Skip the counter advance for the same event
                // kind the journal skips (Queries via `is_query()`). For
                // those skipped slots we stamp the *prior* allocated wire
                // seq so the response gate waits on already-allocated
                // events to be durable before releasing — a query
                // arriving before any allocation gets `0`, which the gate
                // is guaranteed to satisfy.
                let is_query_event = slot.event.is_query();
                let wire_seq = if is_query_event {
                    self.next_wire_seq.saturating_sub(1)
                } else {
                    let s = self.next_wire_seq;
                    self.next_wire_seq = self.next_wire_seq.saturating_add(1);
                    s
                };
                busy_count += 1;

                #[cfg(feature = "latency-trace")]
                wakeup_rec.record_elapsed(slot.publish_ts, mono_trace_ns());

                reports.clear();
                let mut query_report: Option<A::QueryResponse> = None;

                #[cfg(feature = "latency-trace")]
                let exec_start = mono_trace_ns();

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
                // Every output slot emitted while `halted` is exempt from
                // the response stage's durability gate. Two kinds reach
                // the output ring during halt: the explicit halt-state
                // rejection below (`Rejected{ReplicaDisconnected}` —
                // operator-visible refusal, no engine state changed) and
                // the empty `BatchEnd` terminator that the transport
                // variant (Tick) emits as its "I produced no client
                // payload" marker. Neither carries
                // engine state worth replicating before delivery; gating
                // them under a structurally unsatisfiable policy
                // (e.g. `Hybrid` with no replicas) would stall the gate
                // forever — including for the rejection itself, which is
                // exactly what we want clients to see immediately. See
                // `OutputSlot::durability_bypass` for the correctness
                // argument. Queries bypass halt entirely (they're
                // read-only), so they keep the gate as usual.
                // Transport-internal events carry `connection_id == 0`:
                // the runtime emits them for startup seeds (AddInstrument /
                // ProvisionAccount) and the journal-replay path on
                // recovery. They are server-originated, predate any
                // client traffic, and have no client to whom a
                // `ReplicaDisconnected` rejection would be addressed. The
                // halt check exists to refuse *client writes* while the
                // replication policy can't honour the persist-before-ack
                // invariant; applying transport-internal events to the
                // local engine during halt doesn't violate that invariant
                // (no ack to a client) and is required so a fresh primary
                // can seed its instruments before any replica connects.
                // Mirrors the existing dedup exemption a few lines below
                // (`key_hash == 0` — same provenance, same reasoning).
                let is_transport_internal = slot.connection_id == 0;
                let halt_bypass = halted && !is_query;
                if !is_query && halted && !is_transport_internal {
                    // Only app events produce client-facing rejections;
                    // transport variants (Tick)
                    // have no client to reject to, so they silently
                    // skip during halt.
                    if let melin_journal::JournalEvent::App(ref e) = slot.event {
                        reports.push(A::build_reject(e, halt_reason));
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
                        melin_journal::JournalEvent::EpochBump { epoch } => {
                            // Lineage metadata, not application state: advance
                            // the observed epoch and produce no report. Reaches
                            // here on a replica replaying the stream and on the
                            // new primary's own promotion injection.
                            self.fence_state.observe_epoch(epoch);
                        }
                        melin_journal::JournalEvent::Shutdown => {}
                    }
                }

                #[cfg(feature = "latency-trace")]
                {
                    let exec_end = mono_trace_ns();
                    let elapsed_ns = crate::trace::mono_trace_elapsed_ns(exec_start, exec_end);
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
                            melin_journal::JournalEvent::EpochBump { .. } => "epoch_bump",
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
                let match_complete_ts = mono_trace_ns();

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
                    // Transport-internal events (Tick published by the
                    // reader thread) carry
                    // `connection_id == 0` and have no client to reply
                    // to. They previously emitted a BatchEnd-payload
                    // slot as a terminator; downstream consumers
                    // (response, event publisher) just looked up
                    // connection 0, didn't find it, and dropped the
                    // slot. Skipping the publish is equivalent — and
                    // critical during halt onset, where such a slot
                    // would otherwise sit in the response ring with
                    // `durability_bypass=false` (set pre-halt, before
                    // the policy went unsatisfiable) and wedge the
                    // response gate forever waiting for a replication
                    // condition that can no longer be met.
                    if slot.connection_id != 0 {
                        out_batch.push_with(|s| {
                            *s = OutputSlot {
                                connection_id: slot.connection_id,
                                input_seq,
                                wire_seq,
                                payload: OutputPayload::BatchEnd,
                                match_complete_ts,
                                recv_ts: slot.recv_ts,
                                is_last_in_request: true,
                                durability_bypass: halt_bypass,
                            };
                        });
                    }
                } else {
                    for (j, report) in reports.iter().enumerate() {
                        let is_last = j + 1 == report_count && !last_is_query;
                        out_batch.push_with(|s| {
                            *s = OutputSlot {
                                connection_id: slot.connection_id,
                                input_seq,
                                wire_seq,
                                payload: OutputPayload::Report(*report),
                                match_complete_ts,
                                recv_ts: slot.recv_ts,
                                is_last_in_request: is_last,
                                durability_bypass: halt_bypass,
                            };
                        });
                    }
                    if let Some(qr) = query_report {
                        out_batch.push_with(|s| {
                            *s = OutputSlot {
                                connection_id: slot.connection_id,
                                input_seq,
                                wire_seq,
                                payload: OutputPayload::QueryResponse(qr),
                                match_complete_ts,
                                recv_ts: slot.recv_ts,
                                is_last_in_request: true,
                                // Query responses are read-only snapshots
                                // and already bypass the halt check (see
                                // `is_query` above), but they still carry
                                // state-derived data — keep the gate so
                                // clients only observe replicated state.
                                durability_bypass: false,
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
            journal_sequence: WireSeq::new(0),
            active_connections: 0,
            events_processed: 0,
            key_hash: 0,
        };
        loop {
            let entry = self.consumer.try_consume();
            let Some((input_seq, slot)) = entry else {
                break;
            };
            // Mirror the main loop's wire-seq stamping rule so output
            // slots emitted on shutdown carry a sound wire_seq for the
            // response stage's gate. Queries are skipped below (their
            // output slots are meaningless on shutdown) — for non-query
            // events the journal would still allocate, so advance the
            // counter.
            let is_query_event = slot.event.is_query();
            let wire_seq = if is_query_event {
                self.next_wire_seq.saturating_sub(1)
            } else {
                let s = self.next_wire_seq;
                self.next_wire_seq = self.next_wire_seq.saturating_add(1);
                s
            };
            // Read-only queries are meaningless during shutdown — skip
            // to avoid emitting a bare BatchEnd without a preceding
            // response.
            if is_query_event {
                continue;
            }
            reports.clear();

            // Halt check first, then dedup (same order as the main run loop).
            // `connection_id == 0` marks transport-internal events
            // (startup seeds, journal replay) — mirror the main loop's
            // exemption so a shutdown drain doesn't drop the very seed
            // events the next startup will recover.
            let is_transport_internal = slot.connection_id == 0;
            let halt_bypass = self.is_halted();
            if halt_bypass && !is_transport_internal {
                if let melin_journal::JournalEvent::App(ref e) = slot.event {
                    reports.push(A::build_reject(e, self.halt_reject_reason()));
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
            let match_complete_ts = mono_trace_ns();

            // Same is_last_in_request convention as the run loop: mark
            // the final report (or a fallback BatchEnd-payload slot) as
            // the request terminator so the response stage emits the
            // wire BatchEnd.
            let report_count = reports.len();
            if report_count == 0 {
                self.output.publish(OutputSlot {
                    connection_id: slot.connection_id,
                    input_seq,
                    wire_seq,
                    payload: OutputPayload::BatchEnd,
                    match_complete_ts,
                    recv_ts: slot.recv_ts,
                    is_last_in_request: true,
                    durability_bypass: halt_bypass,
                });
            } else {
                for (j, report) in reports.iter().enumerate() {
                    let is_last = j + 1 == report_count;
                    self.output.publish(OutputSlot {
                        connection_id: slot.connection_id,
                        input_seq,
                        wire_seq,
                        payload: OutputPayload::Report(*report),
                        match_complete_ts,
                        recv_ts: slot.recv_ts,
                        is_last_in_request: is_last,
                        durability_bypass: halt_bypass,
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
            melin_journal::JournalEvent::EpochBump { epoch } => {
                // Lineage metadata — advance the observed epoch, never
                // touch application state (see the main run loop).
                self.fence_state.observe_epoch(epoch);
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
    pub events_processed: Arc<AtomicU64>,
    pub input_cursor: Box<dyn ring::QueueCursor>,
    pub replication_consumers: Option<(ReplicationConsumer, ReplicationConsumer)>,
    pub replicas_connected: Option<Arc<AtomicU32>>,
    pub shadow_consumer: Option<ring::Consumer<InputSlot<A::Event>>>,
    pub chain_hash_lock: Option<SeqLockReader<FsyncState>>,
    pub replication_ring_progress: Option<ReplicationRingProgress>,
    /// Journal-progress cursors, space-typed. Bundles the durable wire seq
    /// (the response stage's `persisted` cursor and the replica handshake
    /// value), the journal/matching ring positions (queue-depth monitoring),
    /// and the replica quorum cursor (slowest engaged replica's ack, for
    /// replication-lag monitoring). See [`PipelineCursors`] for why the
    /// wire-seq vs ring-index split matters.
    pub cursors: PipelineCursors,
}

/// Assembled replica pipeline stages and handles returned by [`build_replica_pipeline`].
pub struct ReplicaPipeline<A: Application> {
    pub input_producer: ring::Producer<InputSlot<A::Event>>,
    pub journal_stage: JournalStage<A::Event>,
    pub matching_stage: MatchingStage<A>,
    pub drain_consumer: ring::Consumer<OutputSlot<A::Report, A::QueryResponse>>,
    pub shadow_consumer: Option<ring::Consumer<InputSlot<A::Event>>>,
    /// Journal-progress cursors, space-typed. On a replica the orchestrator
    /// reads `durable_wire_seq` for reconnect handshakes (last journal sequence
    /// durably persisted); the replica slot cursors stay disengaged
    /// (no downstream replica). Updated by `JournalStage` after each fsync.
    pub cursors: PipelineCursors,
    pub chain_hash_lock: Option<SeqLockReader<FsyncState>>,
}

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

/// If shadow snapshots are enabled, allocate a seqlock for publishing the
/// BLAKE3 chain hash to the shadow stage and move the writer half into
/// `journal_stage`. Returns the reader half (so the caller can return it
/// through its pipeline handle struct) or `None` when shadow is disabled —
/// zero overhead in that case.
///
/// The journal stage taking the only writer handle is what makes it
/// impossible for a second thread to publish fsync state.
fn setup_chain_hash_publisher<E: AppEvent>(
    journal_stage: &mut JournalStage<E>,
    enable_shadow: bool,
) -> Option<SeqLockReader<FsyncState>> {
    if enable_shadow {
        let (writer, reader) = melin_pipeline::seqlock::split(FsyncState::default());
        journal_stage.set_chain_hash_lock(writer);
        Some(reader)
    } else {
        None
    }
}

/// Build the pipeline with optional replication support.
///
/// When `enable_replication` is true, builds a replication ring (pre-allocated,
/// lock-free) and wires the producer into the `JournalStage`. At each
/// batch boundary the sequencer copies the encoded bytes into a
/// pre-allocated ring slot (no heap allocation) — before the batch is
/// handed to the disk thread, so replication never waits on the device.
/// The returned consumer(s) are for replica sender threads.
///
/// When replication is disabled, the replica slot cursors stay disengaged
/// (standalone mode) and the derived quorum view reads "no replica".
#[allow(clippy::too_many_arguments)]
pub fn build_pipeline_with_replication<A>(
    app: A,
    writer: BufferedWriter<A::Event>,
    group_commit_delay: Duration,
    active_connections: Arc<AtomicU64>,
    enable_replication: bool,
    max_journal_batch: usize,
    replication_ring_size: usize,
    busy_spin: bool,
    enable_event_publisher: bool,
    enable_shadow: bool,
    fence_state: Arc<crate::fence::FenceState>,
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

    // Snapshot the wire-seq allocator's starting value before handing the
    // writer to the journal stage. The matching stage shadows this counter
    // (incrementing in lockstep with what the journal would allocate) so
    // it can stamp `OutputSlot.wire_seq` in the same sequence space as
    // replica metrics — that's what makes the response gate sound under
    // recovery from a non-trivial journal (`starting_sequence > 1`).
    let starting_wire_seq = writer.next_sequence();

    // Bundle the journal-progress cursors behind space-typed accessors and
    // wire every stage from it. The durable cursor starts at
    // `starting_wire_seq - 1` so the response stage sees the post-recovery /
    // post-genesis durable position immediately rather than waiting for the
    // first user event's fsync to publish it; the journal stage
    // Release-stores into it after every fsync batch. The replica slot
    // cursors start disengaged so the server works immediately even before
    // a replica connects (the replication sender takes over the slots when
    // one does).
    let cursors = PipelineCursors::new(
        WireSeq::new(starting_wire_seq.saturating_sub(1)),
        journal_cursor,
        matching_cursor,
    );

    let mut journal_stage = JournalStage::new(
        writer,
        journal_consumer,
        group_commit_delay,
        max_journal_batch,
        busy_spin,
    );
    journal_stage.set_last_seq_publisher(cursors.durable_wire_seq());

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
        cursors.durable_wire_seq(),
        active_connections,
        replicas_connected.clone(),
        fence_state,
        busy_spin,
        starting_wire_seq,
    );

    Pipeline {
        input_producer,
        journal_stage,
        matching_stage,
        output_consumers,
        events_processed,
        input_cursor,
        replication_consumers,
        replicas_connected,
        shadow_consumer,
        chain_hash_lock,
        replication_ring_progress,
        cursors,
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
    writer: BufferedWriter<A::Event>,
    max_journal_batch: usize,
    group_commit_delay: Duration,
    busy_spin: bool,
    enable_shadow: bool,
    fence_state: Arc<crate::fence::FenceState>,
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

    // Snapshot the wire-seq allocator's starting value before handing the
    // writer to the journal stage. On the replica every incoming slot
    // already carries `slot.sequence`, so the matching stage's counter
    // only needs to be initialised here and from then on tracks 1:1 with
    // those primary-assigned sequences. After promotion (or any future
    // path where the replica's matching stamps wire seqs locally) the
    // counter is positioned correctly.
    let starting_wire_seq = writer.next_sequence();

    // Journal stage: same as primary (encode mode). Pre-assigned sequences
    // in each InputSlot keep the replica's journal aligned with the primary.
    let mut journal_stage = JournalStage::new(
        writer,
        journal_consumer,
        group_commit_delay,
        max_journal_batch,
        busy_spin,
    );

    // Unconditional on replicas (not gated on shadow snapshots): the
    // reconnect handshake reads (journal_seq, chain_hash) as one
    // consistent FsyncState snapshot, and the primary validates the
    // pair against its own chain — a replica without this lock would
    // present a stale hash next to a fresh sequence and be falsely
    // judged divergent.
    let chain_hash_lock = setup_chain_hash_publisher(&mut journal_stage, true);

    // Bundle the journal-progress cursors (mirrors the primary builder).
    // The durable cursor is always published on replicas: the orchestrator
    // reads it for reconnect handshakes without owning the writer (the
    // writer lives inside `journal_stage` for the lifetime of the
    // pipeline). Initialise from the writer's pre-pipeline state so the
    // handshake reflects what's already on disk even before the first
    // fsync nudges the cursor — a fresh writer here returns
    // `starting_wire_seq == 1` (no genesis yet) or `>= 2` (post-genesis),
    // so `saturating_sub(1)` yields the correct "highest wire seq durable"
    // reading at boot. The replica slot cursors stay disengaged for the
    // lifetime of the pipeline (no downstream replica).
    let cursors = PipelineCursors::new(
        WireSeq::new(starting_wire_seq.saturating_sub(1)),
        journal_cursor,
        matching_cursor,
    );
    journal_stage.set_last_seq_publisher(cursors.durable_wire_seq());

    // Matching stage: same as primary but with no replicas_connected check
    // (None = standalone mode, never halts on replica disconnect).
    let active_connections = Arc::new(AtomicU64::new(0));
    let matching_stage = MatchingStage::<A>::new(
        app,
        matching_consumer,
        output_producer,
        Arc::clone(&events_processed),
        cursors.durable_wire_seq(),
        active_connections,
        None, // no replicas_connected halt check on replica
        fence_state,
        busy_spin,
        starting_wire_seq,
    );

    ReplicaPipeline {
        input_producer,
        journal_stage,
        matching_stage,
        drain_consumer,
        shadow_consumer,
        cursors,
        chain_hash_lock,
    }
}
