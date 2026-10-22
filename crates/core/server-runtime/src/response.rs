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
use melin_pipeline::wait::WaitStrategy;

use crate::ack_policy::{Blocker, CursorView, EvalStatus, MAX_CLUSTER_SIZE, Policy};
use crate::batch_boundary::AtBoundary;
use crate::durability_gate::GateInputs;
pub(crate) use crate::durability_gate::GatePolicy;
use crate::early_reply::{EarlyReply, EarlyReplyQueue, Registered};
use crate::idle_housekeeping;
use crate::replication::ReplicationMetrics;
use melin_app::Application;
use melin_transport_core::pipeline::{OutputSlot, StageUtilization};
#[cfg(feature = "latency-trace")]
use melin_transport_core::trace;
use melin_transport_core::{DurableWireSeqCursor, WireSeq};

use crate::connection_limit::{RingSizing, ring_setup_error};
use crate::response_frame::{EarlyReplyBuf, encode_early_reply};
#[cfg(feature = "latency-trace")]
use crate::response_loop::MAX_BATCH;
use crate::response_loop::{
    self, Egress, EgressRecorders, LoopInputs, SlotOutcome, SlotReply, StageNames,
};

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

pub(crate) use crate::ControlEvent;

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
    /// Highest wire seq durably persisted on this node's journal.
    /// In the same sequence space as `OutputSlot.wire_seq` and the
    /// replica metrics (`metrics.in_memory_sequence` /
    /// `metrics.acked_sequence`), so the durability gate can compare
    /// these values numerically and the comparison is meaningful
    /// regardless of `starting_sequence` (fresh vs recovered primary).
    /// Typed [`DurableWireSeqCursor`] so the gate cannot be wired to a
    /// ring-space counter — the pre-v14 bug class. Updated by the
    /// journal stage after every fsync batch via `set_last_seq_publisher`.
    pub journal_persisted_wire_seq: DurableWireSeqCursor,
    /// The ack policy the gate starts under and the sources of its
    /// changes — see [`GatePolicy`].
    pub policy: GatePolicy,
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
    /// How this thread waits — on an empty output ring, and inside the
    /// durability gate on the journal-disk and replication threads.
    pub wait: WaitStrategy,
    pub utilization: Arc<StageUtilization>,
    /// Wire encoder for application-shaped payloads. The application's
    /// encoder, constructed once at boot and shared with the DPDK
    /// response stage.
    pub encoder: ResponseEncoderArc<A>,
    /// Node fencing state. When latched (a higher epoch was observed), the
    /// stage exits *without* the best-effort flush — in-flight responses
    /// for requests on a superseded epoch must not be acknowledged. Fencing
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
    /// The answers the reader decided on without publishing the request:
    /// writes refused while the node was halted, requests the decoder
    /// filtered or refused, requests shed on a full input ring. Each is
    /// sent at the first point between two events' replies, without
    /// waiting for replies to earlier requests — see [`crate::halt`].
    pub early_replies: EarlyReplyQueue<A::Report>,
    /// Sizes the stage's io_uring instance. A flush submits at most one
    /// SEND per dirty connection, and the connection table never holds
    /// more than `--max-connections` entries (the accept gate counts
    /// them), so a ring sized from that cap holds a whole flush — see
    /// [`RingSizing`].
    pub ring_sizing: RingSizing,
    /// Startup report: `Ok` once the stage holds its io_uring instance,
    /// the creation error otherwise (and the stage returns without
    /// running). The ring is created on the stage's own thread, which is
    /// the only one allowed to submit to it, so this is how the spawner
    /// learns the outcome before it carries on. `None` when nobody waits
    /// for it; a creation failure is then only logged.
    pub ready: Option<mpsc::SyncSender<std::io::Result<()>>>,
    /// Release of held replies on this node's own fsync while it is
    /// halted for want of a replica; `None` when off. See
    /// [`crate::halt::DegradedRelease`].
    pub degraded_release: Option<crate::halt::DegradedRelease>,
    /// Test seam: called once per iteration right after the control
    /// channel has been drained, so a test can hold the stage there and
    /// choose what lands while it waits. The drain runs after the
    /// output ring is read, and the seam guards that order: were the
    /// drain moved back in front of the read, the seam would sit
    /// between the two, and a `Connected` landing there would be
    /// missed by the read that follows — which `registration_race`
    /// checks. `None` runs the stage as production does; the field
    /// itself is compiled out of non-test builds, so the loop pays
    /// nothing.
    #[cfg(test)]
    pub pause_after_control_drain: Option<Box<dyn FnMut() + Send>>,
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
    /// every one of the ~2 frames per request. See `UringEgress::dirty`.
    dirty: bool,
}

/// The stage's connections and everything that sends to them: the
/// io_uring instance, the connection table, the dirty and drop lists, the
/// flush cadence, and (traced builds) the samples a flush closes.
///
/// One struct because every place that sends needs all of it: the slot
/// loop, the idle path, the flush before a gate wait, and the early
/// replies sent during one ([`response_loop::GateWaitHook`]), which runs inside the gate
/// while the loop's own locals are borrowed. The kernel TCP stage's
/// [`Egress`], which the shared loop ([`response_loop::run`]) drives.
struct UringEgress {
    ring: IoUring,
    /// Connection table: maps connection IDs to their state.
    ///
    /// `FxHashMap` rather than std's SipHash-1-3 map: this is looked up
    /// once per output slot on the hot path, and the keys are
    /// server-generated monotonic connection ids — a client cannot choose
    /// them, so the HashDoS resistance SipHash buys is worth nothing here
    /// while its ~20 ns/lookup is not. Pre-sized for a reasonable number
    /// of concurrent clients.
    connections: FxHashMap<u64, ConnectionEntry>,
    /// Connections with buffered (unflushed) writes, carried across
    /// batches.
    ///
    /// `Vec<u64>` paired with `ConnectionEntry::dirty` rather than a
    /// `HashSet`: membership is never *queried*, only iterated and
    /// rebuilt, so the set's only contribution on the hot path was
    /// deduplicating repeat appends — which the entry flag does with a
    /// branch instead of a hash. Pre-sized to the connection table.
    dirty: Vec<u64>,
    /// Connections to remove: an append that would overflow
    /// `MAX_SEND_BUF`, or a send error in a flush.
    to_remove: Vec<u64>,
    /// Pre-allocated CQE collection buffer. Must collect CQEs before
    /// processing because the CQ borrow must end before mutating
    /// connections. Pre-sized to the ring's depth (a flush's most SENDs)
    /// to avoid per-iteration heap allocation.
    cqes: Vec<(u64, i32)>,
    /// Flush triggers for the consumed path, and for a drain of early
    /// replies. Lives across batches because the regime the age bound
    /// exists for is a ring that never empties: the byte threshold is per
    /// connection, so with many low-rate clients no single `send_buf`
    /// reaches an MSS for milliseconds while the stage encodes
    /// continuously.
    cadence: FlushCadence,
    /// See [`Response::active_connections`]: decremented here, as each
    /// entry leaves the table.
    active_connections: Arc<AtomicU64>,
    /// See [`Response::fence_state`]: read once, on the way out
    /// ([`Egress::on_shutdown`]), to decide whether the buffered bytes may
    /// still go.
    fence_state: Arc<melin_transport_core::fence::FenceState>,
    /// Server-side end-to-end samples awaiting a flush, tagged with the
    /// connection whose bytes they measure.
    ///
    /// The stage encodes into `send_buf` and hands the bytes to the
    /// kernel later, so closing the sample at encode time would omit
    /// every microsecond a response spends buffered — the interval this
    /// stage most needs to be honest about, since that is exactly where a
    /// deferred flush hides. Holding each frame's `recv_ts` until the
    /// flush completes measures reader-recv → kernel, which is what the
    /// stage name claims and what the DPDK path already records (it
    /// samples after `tx_producers.flush()`).
    ///
    /// The connection id is what lets a connection dropped without a
    /// flush have its samples thrown away rather than closed — see
    /// `discard_e2e_samples`. Two rules keep the queue honest, and every
    /// mutation of `dirty` obeys one of them:
    ///
    /// - a clear-by-flush closes the queue (`close_e2e_samples`),
    /// - a removal-by-drop discards that connection's entries
    ///   (`discard_e2e_samples`).
    ///
    /// Together they give the invariant "queue non-empty implies some
    /// connection is dirty", which is what makes the flush sites
    /// sufficient drain points.
    ///
    /// `Vec` rather than a per-connection map: entries are appended and
    /// then walked wholesale, order is irrelevant, and the dropped-
    /// connection lists they are matched against are almost always empty.
    /// A flat append is the cheapest thing that does that. Capacity is a
    /// starting point, not a bound — under saturation the output ring
    /// never empties, so the queue grows until something forces a flush.
    ///
    /// Only compiled under `latency-trace`; production builds carry
    /// neither the buffer nor the pushes.
    #[cfg(feature = "latency-trace")]
    pending_e2e: Vec<(u64, trace::MonoTraceInstant)>,
    #[cfg(feature = "latency-trace")]
    server_e2e_rec: trace::StageRecorder,
    /// Wall time around a slot's append (see [`EgressRecorders::encode`]).
    #[cfg(feature = "tick-to-trade")]
    encode_rec: trace::StageRecorder,
    /// One sample per io_uring flush that ships responses, batching many
    /// slots.
    #[cfg(feature = "tick-to-trade")]
    egress_rec: trace::StageRecorder,
}

impl UringEgress {
    /// An empty table over `ring`, whose depth sizes the CQE buffer,
    /// fenced by `fence_state`.
    fn for_stage(
        ring: IoUring,
        ring_entries: u32,
        active_connections: Arc<AtomicU64>,
        fence_state: Arc<melin_transport_core::fence::FenceState>,
        // Empty in untraced builds.
        #[cfg_attr(not(feature = "latency-trace"), allow(unused_variables))]
        recorders: EgressRecorders,
        #[cfg(feature = "tick-to-trade")] egress_rec: trace::StageRecorder,
    ) -> Self {
        Self {
            ring,
            connections: FxHashMap::with_capacity_and_hasher(256, Default::default()),
            dirty: Vec::with_capacity(256),
            to_remove: Vec::new(),
            cqes: Vec::with_capacity(ring_entries as usize),
            cadence: FlushCadence::default(),
            active_connections,
            fence_state,
            #[cfg(feature = "latency-trace")]
            pending_e2e: Vec::with_capacity(MAX_BATCH),
            #[cfg(feature = "latency-trace")]
            server_e2e_rec: recorders.server_e2e,
            #[cfg(feature = "tick-to-trade")]
            encode_rec: recorders.encode,
            #[cfg(feature = "tick-to-trade")]
            egress_rec,
        }
    }

    /// The egress the unit tests build: as [`Self::for_stage`], on a node
    /// that is never fenced, with a test encode recorder of its own.
    #[cfg(test)]
    fn new(
        ring: IoUring,
        ring_entries: u32,
        active_connections: Arc<AtomicU64>,
        #[cfg(feature = "latency-trace")] server_e2e_rec: trace::StageRecorder,
        #[cfg(feature = "tick-to-trade")] egress_rec: trace::StageRecorder,
    ) -> Self {
        Self::for_stage(
            ring,
            ring_entries,
            active_connections,
            Arc::new(melin_transport_core::fence::FenceState::new(0)),
            EgressRecorders {
                #[cfg(feature = "latency-trace")]
                server_e2e: server_e2e_rec,
                #[cfg(feature = "tick-to-trade")]
                encode: trace::register_stage("test: encode"),
            },
            #[cfg(feature = "tick-to-trade")]
            egress_rec,
        )
    }

    /// Submit every dirty connection's buffer ([`flush_sends`]) and
    /// restart the flush cadence. Leaves the flush's failures in
    /// `to_remove`.
    fn send_dirty(&mut self) {
        flush_sends(
            &mut self.ring,
            &mut self.connections,
            &mut self.dirty,
            &mut self.to_remove,
            &mut self.cqes,
        );
        self.cadence.on_flush();
    }

    /// Flush every dirty connection now, if any is, closing the queued
    /// samples against it, and tear down the connections the flush
    /// failed. The idle path's flush, and the one before a gate wait.
    ///
    /// Restarts the cadence even with nothing dirty: the appends it
    /// counted may all have been to connections dropped since, and a
    /// cadence left due would make every later slot's
    /// [`Self::flush_if_due`] run again for nothing.
    fn flush(&mut self) {
        if self.dirty.is_empty() {
            self.cadence.on_flush();
            return;
        }
        #[cfg(feature = "tick-to-trade")]
        let egress_start = trace::mono_trace_ns();
        self.send_dirty();
        #[cfg(feature = "tick-to-trade")]
        self.egress_rec
            .record_elapsed(egress_start, trace::mono_trace_ns());
        #[cfg(feature = "latency-trace")]
        close_e2e_samples(&mut self.pending_e2e, &mut self.server_e2e_rec);
        self.teardown_flush_failures();
    }

    /// Tear down the connections a flush failed. They are off the dirty
    /// list already (see [`flush_sends`]).
    fn teardown_flush_failures(&mut self) {
        for conn_id in self.to_remove.drain(..) {
            if let Some(entry) = self.connections.remove(&conn_id) {
                teardown_dropped(entry, &self.active_connections);
            }
        }
    }

    /// Tear down the connections an append overflowed, before anything
    /// flushes them. This un-dirties a connection without flushing it, so
    /// its queued samples go too: they measure bytes that will never be
    /// sent, and would otherwise land on some later, unrelated flush.
    /// Later slots addressed to them are skipped by the table miss.
    fn drop_overflowed(&mut self) {
        #[cfg(feature = "latency-trace")]
        discard_e2e_samples(&mut self.pending_e2e, &self.to_remove);
        for conn_id in self.to_remove.drain(..) {
            if let Some(entry) = self.connections.remove(&conn_id) {
                teardown_dropped(entry, &self.active_connections);
            }
            unmark_dirty(conn_id, &mut self.dirty);
        }
    }

    /// The consumed path's flush, run between slots and between released
    /// early replies alike: when the cadence says one is due, drop the
    /// overflowed connections, so the flush skips them, and flush.
    ///
    /// The still-open half of July-audit finding 4, plus its age bound
    /// (August audit finding 3). Under sustained load with an open gate
    /// this is the ONLY flush that runs: the idle path needs an empty
    /// SPSC, the gate path needs a durability wait, and heartbeats need
    /// idleness. Without it, delivery on a busy stretch degenerates to
    /// `MAX_SEND_BUF` evicting the very clients being served — and so
    /// would a drain of early replies, which can release thousands to one
    /// connection at once. Trigger-gated so light traffic keeps full
    /// batching; once a connection holds ~an MSS — or the stage has
    /// appended `FLUSH_SLOT_INTERVAL` times since the last flush,
    /// whichever comes first — the extra submit no longer costs wire
    /// efficiency. MSG_DONTWAIT flushes cannot block, so this adds no
    /// head-of-line exposure (the hazard that deferred this trigger in
    /// July). A blocked peer re-arms the flag on its later appends, but
    /// its paced retry inside `flush_sends` keeps that cheap.
    #[inline]
    fn flush_if_due(&mut self) {
        if !self.cadence.is_due() {
            return;
        }
        self.drop_overflowed();
        self.flush();
    }
}

impl Egress for UringEgress {
    type Control = ControlEvent;

    const NAMES: StageNames = StageNames {
        gate: "response",
        dispatch: "response: dispatch (consume → socket write)",
        server_e2e: "server e2e (reader recv → response flush)",
    };

    fn drain_control(&mut self, control_rx: &mpsc::Receiver<ControlEvent>) {
        while let Ok(event) = control_rx.try_recv() {
            match event {
                ControlEvent::Connected {
                    connection_id,
                    fd,
                    writer,
                } => {
                    // The writer keeps the fd alive — store it as the owner.
                    let owner: Box<dyn Send> = Box::new(writer);
                    self.connections.insert(
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
                    if self.connections.remove(&connection_id).is_some() {
                        self.active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                    unmark_dirty(connection_id, &mut self.dirty);
                    // Anything this connection had buffered goes with
                    // it, so its queued samples measure bytes that will
                    // never be sent.
                    #[cfg(feature = "latency-trace")]
                    discard_e2e_samples(&mut self.pending_e2e, &[connection_id]);
                }
            }
        }
    }

    /// Send every early reply `registered` holds, between two events'
    /// replies (`at`), each appended to its connection's buffer
    /// ([`append_early_reply`]) with the consumed path's flush cadence
    /// applied after each one ([`Self::flush_if_due`]): a drain can
    /// release thousands of replies to one connection, and appending them
    /// all before a flush would take a client that reads its replies past
    /// `MAX_SEND_BUF`. Flushing here is allowed because the drain holds
    /// `at`: no event's reply is half-buffered. Returns how many were
    /// sent; `now` is read once, and only if there is one.
    fn release_early_replies<R: Copy, Q: Copy>(
        &mut self,
        registered: &mut Registered<'_, R>,
        at: &AtBoundary<'_>,
        encoder: &dyn melin_app::encoder::ResponseEncoder<Report = R, Query = Q>,
        early_buf: &mut EarlyReplyBuf,
        now: impl Fn() -> Instant,
    ) -> usize {
        let mut stamp = None;
        registered.drain(at, |reply| {
            let now = *stamp.get_or_insert_with(&now);
            append_early_reply(
                reply,
                encoder,
                &mut self.connections,
                early_buf,
                now,
                &mut self.dirty,
                &mut self.to_remove,
                &mut self.cadence,
            );
            self.flush_if_due();
        })
    }

    /// Append the slot's frames to its connection's send buffer
    /// ([`append_frames`]), payload and terminator in one call, and feed
    /// the flush cadence ([`FlushCadence`]), which [`Self::after_slot`]
    /// checks. An append that would overflow `MAX_SEND_BUF` queues the
    /// connection for removal, sends nothing, and is still
    /// [`SlotOutcome::Known`].
    #[inline]
    fn send_slot<R: Copy, Q: Copy>(
        &mut self,
        connection_id: u64,
        reply: SlotReply<'_, R, Q>,
        now: Instant,
    ) -> SlotOutcome {
        let Some(entry) = self.connections.get_mut(&connection_id) else {
            return SlotOutcome::Unknown;
        };
        let frames = reply.into_frames();

        #[cfg(feature = "tick-to-trade")]
        let encode_start = trace::mono_trace_ns();
        // Underscored: with both frames in one call there is no second
        // append to skip, so the outcome is purely informational —
        // `append_frames` has already queued an overflowing connection for
        // removal. Only the traced build reads it, to avoid measuring
        // bytes that were never buffered.
        let _outcome = append_frames(
            frames.payload,
            frames.trailer,
            connection_id,
            entry,
            frames.encode_buf,
            now,
            &mut self.dirty,
            &mut self.to_remove,
        );
        #[cfg(feature = "tick-to-trade")]
        self.encode_rec
            .record_elapsed(encode_start, trace::mono_trace_ns());

        // Queue the server-side end-to-end sample: reader recv -> response
        // flush. Only a request's last slot carries this measurement;
        // queued after the append so a dropped connection doesn't skew the
        // metric, and closed by whichever flush ships the bytes.
        #[cfg(feature = "latency-trace")]
        if frames.is_last_in_request && matches!(_outcome, AppendOutcome::Continue) {
            self.pending_e2e.push((connection_id, frames.recv_ts));
        }

        self.cadence.on_append(entry.send_buf.len());
        SlotOutcome::Known
    }

    /// The consumed-path flush ([`Self::flush_if_due`]), between slots,
    /// not after the batch, so `MAX_SEND_BUF` can never trip on a healthy
    /// connection within a single batch.
    ///
    /// The cadence is fed at append time and checked after every slot, so
    /// the flush runs *within* the batch. Deferring it to the end of the
    /// batch would reopen the defect the byte threshold exists to close:
    /// one `MAX_BATCH` batch of large frames (anything over ~64 bytes
    /// average) can push a healthy connection from empty past
    /// `MAX_SEND_BUF` before a batch-end flush ever ran, tearing it down
    /// with nothing written. A flag written at append time rather than a
    /// per-slot scan of the dirty set: the scan would cost a hash lookup
    /// per dirty connection on the hot path.
    #[inline]
    fn after_slot(&mut self) {
        self.flush_if_due();
    }

    /// Drain buffered sends before blocking, never after.
    ///
    /// The steady-state flush happens on the `count == 0` path — once the
    /// output ring is empty. That batches many responses behind one
    /// `io_uring_enter`, which is the right trade on the fast path. But it
    /// means a response that has already cleared its own durability gate
    /// stays in `send_buf` while the loop blocks on a later event's gate,
    /// so a client waits out an fsync + replica round-trip for an event
    /// whose durability was confirmed before that event was even
    /// sequenced.
    ///
    /// Flushing here costs nothing in latency terms: it only fires when we
    /// are about to spin on the gate anyway, so the syscall overlaps time
    /// that would otherwise be dead. Slots whose gate is already satisfied
    /// fall straight through and keep their batching.
    ///
    /// Traced builds only: this delays the tracker's first `observe` by
    /// the flush duration, so a cursor that crosses mid-flush is
    /// attributed late (or, if it crosses past `needed`, not attributed at
    /// all). Bounded by one flush and confined to the tick-to-trade
    /// breakdown — the gate's own behaviour is unchanged.
    ///
    /// The held slot and later ones addressed to a connection the flush
    /// drops are skipped by the table lookup after the admit, so removing
    /// here is safe.
    fn before_wait(&mut self) {
        self.flush();
    }

    /// Ship the early replies a gate wait released, dropping first the
    /// connections they overflowed, so the flush skips them.
    fn after_wait_release(&mut self) {
        self.drop_overflowed();
        self.flush();
    }

    /// An early reply can overflow its connection's buffer; drop those
    /// before the idle flush ([`Self::on_idle`]), as the batch end does.
    fn after_idle_release(&mut self) {
        self.drop_overflowed();
    }

    /// The output ring is empty: flush all dirty connections via
    /// io_uring. This is the response-data egress path; heartbeat flushes
    /// ([`Self::send_heartbeats`]) aren't sampled because they're admin
    /// traffic, not on the client RTT path.
    fn on_idle(&mut self) {
        self.flush();
    }

    /// Append the heartbeat to every connection it is due to
    /// ([`heartbeat_due`]) and flush them at once.
    ///
    /// No end-to-end samples to close here: the sample queue's invariant
    /// is that a queued entry implies a dirty connection, so the idle
    /// flush before this ([`Self::on_idle`]), which closes the queue
    /// whenever anything was dirty, leaves it empty. Anything dirtied from
    /// here on is heartbeat frames, which carry no samples.
    fn send_heartbeats(&mut self, frame: &[u8], now: Instant, interval: Duration) {
        for (&conn_id, entry) in self.connections.iter_mut() {
            if heartbeat_due(entry, now, interval, frame.len()) {
                entry.send_buf.extend_from_slice(frame);
                entry.last_send = now;
                mark_dirty(entry, conn_id, &mut self.dirty);
            }
        }
        // Flush the heartbeat sends immediately. Unsampled: admin
        // traffic, and no sample is queued (see above).
        if !self.dirty.is_empty() {
            self.send_dirty();
            self.teardown_flush_failures();
        }
    }

    /// Remove connections that exceeded the send buffer limit.
    fn after_commit(&mut self) {
        self.drop_overflowed();
    }

    #[cfg(feature = "latency-trace")]
    fn flush_idle_stats(&mut self) {
        self.server_e2e_rec.flush();
        #[cfg(feature = "tick-to-trade")]
        {
            self.encode_rec.flush();
            self.egress_rec.flush();
        }
    }

    /// The best-effort flush before shutdown, unless fenced: a superseded
    /// ex-primary must not acknowledge any further in-flight work, so the
    /// flush is skipped and responses buffered for requests on the old
    /// epoch are dropped (the client sees a connection reset and
    /// reconciles on reconnect). Checked only here, not per slot: fencing
    /// always co-sets `shutdown` (`FenceState::fence_if_superseded` owns
    /// that invariant), and every exit of the loop leads here, so this is
    /// the first place a fenced node reaches and the steady-state loop
    /// pays nothing.
    ///
    /// `send_dirty` and the close of the queued samples, not
    /// [`Self::flush`], which would also tear down the flush's failures:
    /// the stage is going away with every connection.
    fn on_shutdown(&mut self) {
        let flush = !self.fence_state.is_fenced();
        if flush && !self.dirty.is_empty() {
            self.send_dirty();
            #[cfg(feature = "latency-trace")]
            close_e2e_samples(&mut self.pending_e2e, &mut self.server_e2e_rec);
        }
    }
}

/// Run the io_uring response stage loop. Blocks the calling thread until shutdown.
///
/// Consumes from the output SPSC, waits for durability confirmation, and
/// sends responses via io_uring SEND: the shared loop
/// ([`response_loop::run`]) over this stage's [`UringEgress`].
///
/// Ack gating: every gate iteration reads the journal cursor (this
/// node's persisted) plus per-slot replica cursors (in-memory and
/// persisted) from `replication_metrics` and feeds them through the
/// configured [`Policy`]. See `evaluate_durability`.
///
/// Returns `true` once the stage has run and stopped on shutdown, `false`
/// when it never started: its io_uring instance could not be created
/// (reported through [`Response::ready`]), or nobody was left to report
/// to.
pub fn run<A: Application>(
    consumer: ring::Consumer<OutputSlot<A::Report, A::QueryResponse>>,
    control_rx: mpsc::Receiver<ControlEvent>,
    config: Response<A>,
    shutdown: &AtomicBool,
) -> bool {
    let Response {
        journal_persisted_wire_seq,
        policy,
        replication_metrics,
        replica_active,
        heartbeat_interval,
        wait,
        utilization,
        encoder,
        fence_state,
        active_connections,
        early_replies,
        ring_sizing,
        ready,
        degraded_release,
        #[cfg(test)]
        pause_after_control_drain,
    } = config;
    // SINGLE_ISSUER: created and submitted from this thread only — the
    // kernel skips SQ locking and rejects cross-thread submission with
    // EEXIST instead of racing. Matches the journal/replication rings.
    // Created first, before anything else the stage builds, so a node
    // that cannot have it learns so before the stage touches anything.
    let ring_entries = ring_sizing.ring_entries();
    let ring: IoUring = match IoUring::builder().setup_single_issuer().build(ring_entries) {
        Ok(ring) => ring,
        Err(e) => {
            let err = ring_setup_error("response", ring_entries, e);
            match ready {
                // Dropped send: a spawner that stopped waiting is
                // unwinding, and the stage returns either way.
                Some(ready) => {
                    let _ = ready.send(Err(err));
                }
                None => error!(error = %err, "response stage cannot start"),
            }
            return false;
        }
    };
    // Started. A failed send means the spawner is no longer waiting — it
    // is unwinding — so nothing would ever stop this stage: return.
    if let Some(ready) = ready
        && ready.send(Ok(())).is_err()
    {
        return false;
    }
    // The loop's recorders and the egress's, then the one only this
    // egress has: one sample per io_uring flush that ships responses,
    // batching many slots.
    let (loop_recorders, egress_recorders) = response_loop::register_recorders(&UringEgress::NAMES);
    #[cfg(feature = "tick-to-trade")]
    let egress_rec = trace::register_stage("response: egress (flush_sends elapsed)");

    let egress = UringEgress::for_stage(
        ring,
        ring_entries,
        active_connections,
        fence_state,
        egress_recorders,
        #[cfg(feature = "tick-to-trade")]
        egress_rec,
    );

    response_loop::run(
        egress,
        LoopInputs {
            consumer,
            control_rx,
            gate: GateInputs {
                journal_persisted_wire_seq,
                replication_metrics,
                replica_active,
                utilization,
                wait,
                degraded_release,
            },
            policy,
            heartbeat_interval,
            encoder,
            early_replies,
            #[cfg(test)]
            pause_after_control_drain,
        },
        loop_recorders,
        shutdown,
    );
    true
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
/// response is encoded: its own event is not yet known to be durable.
///
/// Every slot is subject to it. The one reply a halted node sends without
/// waiting — the refusal of a write — never enters the output ring (see
/// [`crate::halt`]).
///
/// Shared by both response stages, so the io_uring and DPDK paths cannot
/// drift apart on it.
#[inline]
pub(crate) fn slot_needs_gate<R: Copy, Q: Copy>(
    slot: &OutputSlot<R, Q>,
    cached_durable_pos: u64,
) -> bool {
    cached_durable_pos < slot.wire_seq
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

/// Whether the idle-path heartbeat scan should append a frame to this
/// connection: idle for a full interval (the due rule both stages share,
/// `idle_housekeeping::heartbeat_due`) — and able to receive it.
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
fn heartbeat_due(
    entry: &ConnectionEntry,
    now: Instant,
    interval: Duration,
    frame_len: usize,
) -> bool {
    idle_housekeeping::heartbeat_due(entry.last_send, now, interval)
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
    // `encode_buf` already holds a complete frame — header written by
    // `frame_app_response`, or by the control codec — so no extra
    // framing is needed.
    entry.send_buf.extend_from_slice(&encode_buf[..written]);
    entry.send_buf.extend_from_slice(trailer);
    entry.last_send = batch_now;
    mark_dirty(entry, connection_id, dirty_connections);
    AppendOutcome::Continue
}

/// Append an answer the reader decided on — a halt rejection, a filtered
/// or refused request, a shed one — to its connection's send buffer, its
/// frames encoded by [`encode_early_reply`]. A connection that has gone
/// away loses nothing it can still receive.
///
/// No durability gate: nothing was published, so there is nothing to wait
/// for. The caller stands between two events' replies, so it splits none
/// (see [`crate::halt`]).
#[allow(clippy::too_many_arguments)]
fn append_early_reply<R: Copy, Q: Copy>(
    reply: EarlyReply<R>,
    encoder: &dyn melin_app::encoder::ResponseEncoder<Report = R, Query = Q>,
    connections: &mut FxHashMap<u64, ConnectionEntry>,
    early_buf: &mut EarlyReplyBuf,
    now: Instant,
    dirty_connections: &mut Vec<u64>,
    to_remove: &mut Vec<u64>,
    flush: &mut FlushCadence,
) {
    let Some(entry) = connections.get_mut(&reply.connection_id) else {
        debug!(
            connection_id = reply.connection_id,
            "early reply dropped: connection not registered"
        );
        return;
    };
    let written = encode_early_reply(&reply, encoder, early_buf);
    append_frames(
        Some(Ok(written)),
        &[],
        reply.connection_id,
        entry,
        early_buf,
        now,
        dirty_connections,
        to_remove,
    );
    flush.on_append(entry.send_buf.len());
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
            ring.submission().push(&sqe).expect(
                "io_uring SQ full: a flush submits one SEND per connection, and the ring \
                 is sized above --max-connections",
            );
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
                    // happen: the CQ is twice the ring's depth and fully
                    // reaped every flush, and `pending` — one SEND per
                    // connection — is at most `--max-connections`, below
                    // the depth `RingSizing` derives from it, so the CQ
                    // never holds more than one flush). An error here means
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
/// response stage only gates events the matching stage has already
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
/// reported replication as the blocker under `disk` whenever a replica
/// merely lagged — a replica binds `persisted>=1` only when its fsync
/// leads the journal's — and read the persisted level under
/// `disk+ram`, which gates on in-memory.
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
/// replica-supplied (e.g. `disk`).
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
    /// (e.g. a primary under `disk+ram` with no replica yet
    /// connected). Logs a startup warn immediately and treats the
    /// state as already-logged so the next tick doesn't re-emit.
    pub(crate) fn new_starting_degraded(now: Instant, policy: &Policy) -> Self {
        tracing::warn!(
            policy = %policy,
            "ack policy starts degraded — fewer connected nodes than the target count"
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
    /// fresh healthy-start logger. Called on a runtime ack-policy
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
                    "ack policy operating degraded — fewer connected nodes than the target count, response gate stalled until the cluster recovers or the policy is swapped"
                );
            } else {
                tracing::info!(
                    policy = %policy,
                    "ack policy returned to target shape"
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
                "ack policy still degraded — fewer connected nodes than the target count"
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
    /// under `disk` and transiently when the binding replica drops out
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

/// Print busy/idle utilization for a pipeline stage on shutdown: the
/// response loop's epilogue, on both transports (`response_loop::run`).
#[cfg(feature = "pipeline-stats")]
pub(crate) fn print_utilization(stage: &str, busy: u64, idle: u64) {
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
pub(crate) mod tests {
    #[cfg(feature = "tick-to-trade")]
    use super::GateCrossTracker;
    use super::{
        Blocker, DegradationLogger, OutputSlot, WireSeq, evaluate_durability, evaluate_gate,
        slot_needs_gate,
    };
    use crate::ack_policy::{AckPolicy, Clause, Level, Policy};
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
    fn slot(wire_seq: u64) -> OutputSlot<(), ()> {
        OutputSlot {
            wire_seq,
            ..Default::default()
        }
    }

    #[test]
    fn slot_gates_until_its_own_sequence_is_durable() {
        assert!(slot_needs_gate(&slot(10), 0));
        assert!(slot_needs_gate(&slot(10), 9));
    }

    #[test]
    fn slot_clears_at_its_own_sequence_not_the_batch_maximum() {
        // The cursor reaching exactly this slot's `wire_seq` is enough —
        // an off-by-one here would stall every response an extra
        // durability round-trip. A later slot in the same batch needing
        // more must not hold this one back.
        assert!(!slot_needs_gate(&slot(10), 10));
        assert!(!slot_needs_gate(&slot(10), 11));
    }

    /// Build a [`Policy`] from a mini DSL: one or more
    /// `"<level>>=<count>"` clauses joined with `&&`. Test-only
    /// ergonomics — production builds policies via
    /// [`AckPolicy::to_policy`].
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
        // `ram` (`in_memory>=2`): the journal position must not
        // bind. Journal at 0 — nothing persisted anywhere on the
        // primary — while the replicas hold 100/120 in memory. The
        // 2nd-largest in-memory across {primary=MAX, 100, 120} is 120:
        // the gate is exactly the fastest replica's RAM receipt.
        let p = AckPolicy::Ram.to_policy();
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
        let p = AckPolicy::Ram.to_policy();
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
        // degraded. Note the readers' separate halt at
        // `replicas_connected==0` refuses new writes before they reach
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
    // reported replication as the blocker under `disk` whenever a
    // replica lagged (a replica binds `persisted>=1` only when its fsync
    // leads) and read the wrong level under `disk+ram` (which gates
    // replicas on in-memory).
    //
    // Attribution comes out of `evaluate_gate` alongside the durable
    // position, computed from the same cursor snapshot, and is `None`
    // while the gate stays closed — so each test passes a `needed` at
    // or below the scenario's durable position to model the iteration
    // on which the gate opens.

    #[test]
    fn disk_policy_credits_journal_when_replicas_lag() {
        // `persisted>=1` is satisfied by the highest persisted cursor
        // in the cluster — here the primary's. A connected replica lagging
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
    fn disk_policy_credits_replication_when_a_replica_fsync_leads() {
        // The mirror: policies count copies, not nodes. With the
        // primary's disk stalled at 10 and a replica persisted through
        // 500, `persisted>=1` opens on the replica's copy — and the
        // attribution must say so, or an operator reading the gate
        // counters would conclude the primary's disk was keeping up.
        let p = parse("persisted>=1").unwrap();
        let m = metrics((500, 500), (10, 10));
        let a = both_active();
        assert_eq!(
            evaluate_gate(&p, 500, WireSeq::new(10), Some(&m), Some(&a)).1,
            Some(Blocker::Replication)
        );
    }

    #[test]
    fn disk_and_ram_reads_replica_in_memory_not_persisted() {
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
    fn disk_and_ram_is_satisfied_by_the_faster_replica() {
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
    fn two_disks_uses_second_largest_persisted() {
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
        // Replication binds, which is the norm for this policy: the
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
        // readers' `replicas_connected==0` halt is what stops
        // accepting new writes; the gate side's job is just to keep
        // the existing in-flight writes stalled and the alert lit.
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

    fn logger_test_policy() -> crate::ack_policy::Policy {
        parse("persisted>=2").unwrap()
    }

    /// Tick the logger N times at `step` intervals, alternating
    /// `degraded` per call. Returns the gauge value after the last
    /// tick — useful for asserting that flap cycles don't leak the
    /// AtomicBool into a wrong terminal state.
    fn drive_logger(
        logger: &mut DegradationLogger,
        utilization: &StageUtilization,
        policy: &crate::ack_policy::Policy,
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
        // A primary that boots already-degraded (e.g. `disk+ram` with no
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
        // A runtime policy swap re-seeds the logger. On the gate-wait path
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
        pub(super) fn fill_socket(fd: RawFd) {
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

    /// One drain of early replies can release thousands to one connection
    /// (a burst of refused or shed requests). The drain applies the
    /// consumed path's flush cadence between replies, so a client that
    /// reads its replies keeps its connection however many there are,
    /// and `MAX_SEND_BUF` still drops one that does not read.
    mod early_drain {
        use std::os::unix::io::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::thread;
        use std::time::{Duration, Instant};

        use counter_server::{CounterReport, ResponseEncoder};
        use io_uring::IoUring;
        use melin_wire_protocol::control::RefusalKind;

        use super::early_release::widest_reason;
        use super::flush_sends::{entry_for, fill_socket};
        use super::refusal_order::{Seen, early, early_id, frames_of, read_frame};
        use crate::batch_boundary::BatchBoundary;
        use crate::early_reply::{EARLY_REPLY_QUEUE_CAPACITY, EarlyKind, early_reply_channel};
        use crate::response::{MAX_SEND_BUF, UringEgress};
        use crate::response_frame::{EarlyReplyBuf, MAX_EARLY_REPLY};
        use crate::response_loop::Egress as _;

        /// Replies in the burst: alternately a busy and a refusal with the
        /// widest reason, together well past `MAX_SEND_BUF`, within the
        /// queue's capacity.
        const BURST: u64 = 800;

        /// The kind of the burst's reply number `n`.
        fn kind(n: u64) -> EarlyKind<CounterReport> {
            if n.is_multiple_of(2) {
                EarlyKind::Busy
            } else {
                EarlyKind::Refused {
                    kind: RefusalKind::PermissionDenied,
                    reason: widest_reason(),
                }
            }
        }

        /// A stage's egress holding one connection, 1, on `stream`, and
        /// the count of live connections it decrements.
        pub(super) fn egress_over(stream: UnixStream) -> (UringEgress, Arc<AtomicU64>) {
            let active = Arc::new(AtomicU64::new(1));
            let mut egress = UringEgress::new(
                IoUring::new(64).expect("an io_uring"),
                64,
                Arc::clone(&active),
                #[cfg(feature = "latency-trace")]
                melin_transport_core::trace::register_stage("test: server e2e"),
                #[cfg(feature = "tick-to-trade")]
                melin_transport_core::trace::register_stage("test: egress"),
            );
            let mut entry = entry_for(stream, &[]);
            entry.dirty = false;
            egress.connections.insert(1, entry);
            (egress, active)
        }

        /// Queue the burst for connection 1 and release it in one drain,
        /// then flush as the idle path does.
        fn drain_burst(egress: &mut UringEgress) -> usize {
            assert!(BURST as usize <= EARLY_REPLY_QUEUE_CAPACITY);
            let (mut tx, mut rx) =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY);
            for n in 0..BURST {
                tx.try_send(early(1, n, kind(n))).expect("queue has room");
            }
            tx.flush();
            let boundary = BatchBoundary::new();
            let at = boundary.at_boundary().expect("between events");
            let mut early_buf: EarlyReplyBuf = [0u8; MAX_EARLY_REPLY];
            let released = egress.release_early_replies(
                &mut rx.register(|| {}),
                &at,
                &ResponseEncoder,
                &mut early_buf,
                Instant::now,
            );
            egress.drop_overflowed();
            egress.flush();
            released
        }

        #[test]
        fn a_drain_past_the_send_buffer_flushes_as_it_goes_and_keeps_a_reading_client() {
            let expected: Vec<Seen> = (0..BURST)
                .flat_map(|n| frames_of(kind(n), early_id(n)))
                .collect();
            // On the wire: a busy is its length prefix, tag and ID; a
            // refusal adds its kind, the reason's length, and the reason.
            let wire_bytes: usize = (0..BURST)
                .map(|n| match kind(n) {
                    EarlyKind::Busy => 13,
                    _ => 15 + widest_reason().len(),
                })
                .sum();
            assert!(wire_bytes > MAX_SEND_BUF, "the burst outgrows the buffer");

            let (server, mut client) = UnixStream::pair().expect("socketpair");
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let count = expected.len();
            let reader = thread::spawn(move || {
                (0..count)
                    .map_while(|_| read_frame(&mut client).ok())
                    .collect::<Vec<_>>()
            });

            let (mut egress, active) = egress_over(server);
            assert_eq!(drain_burst(&mut egress), BURST as usize);
            // What a flush left behind on a socket the reader had not
            // emptied yet goes out on the paced retries.
            let deadline = Instant::now() + Duration::from_secs(5);
            while !egress.dirty.is_empty() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(1));
                egress.flush();
            }

            assert!(
                egress.connections.contains_key(&1),
                "a client reading its replies was dropped"
            );
            assert_eq!(active.load(Ordering::Relaxed), 1);
            assert_eq!(reader.join().expect("reader"), expected, "in queue order");
        }

        #[test]
        fn a_drain_to_a_client_that_does_not_read_drops_it_at_the_cap() {
            let (server, _client) = UnixStream::pair().expect("socketpair");
            fill_socket(server.as_raw_fd());
            let (mut egress, active) = egress_over(server);

            assert_eq!(drain_burst(&mut egress), BURST as usize);

            assert!(
                !egress.connections.contains_key(&1),
                "a client that reads nothing kept its connection"
            );
            assert!(!egress.dirty.contains(&1));
            assert_eq!(
                active.load(Ordering::Relaxed),
                0,
                "its permit released once"
            );
        }

        /// A flush inside the drain that fails (the client has hung up)
        /// tears the connection down there, once, and the rest of the
        /// drain's replies to it are dropped as for any connection gone.
        #[test]
        fn a_drain_to_a_client_that_has_hung_up_tears_it_down_once() {
            let (server, client) = UnixStream::pair().expect("socketpair");
            drop(client);
            let (mut egress, active) = egress_over(server);

            assert_eq!(drain_burst(&mut egress), BURST as usize);

            assert!(!egress.connections.contains_key(&1));
            assert!(egress.to_remove.is_empty(), "nothing left to tear down");
            assert_eq!(
                active.load(Ordering::Relaxed),
                0,
                "its permit released once"
            );
        }

        /// A cadence made due by appends to a connection gone since (so
        /// nothing is dirty) is restarted by the flush that finds nothing
        /// to send, rather than left due for every later slot.
        #[test]
        fn a_due_cadence_with_nothing_dirty_is_restarted() {
            let (server, _client) = UnixStream::pair().expect("socketpair");
            let (mut egress, _active) = egress_over(server);
            for _ in 0..super::super::FLUSH_SLOT_INTERVAL {
                egress.cadence.on_append(1);
            }
            assert!(egress.cadence.is_due());
            assert!(egress.dirty.is_empty());

            egress.flush_if_due();

            assert!(!egress.cadence.is_due());
        }
    }

    /// The kernel egress's hooks, called directly: what a slot's send
    /// reports to the loop, which counts degraded acks and logs misses
    /// from it (pinned by the recording egress in `response_loop`'s
    /// tests), and what the way out sends.
    mod uring_egress {
        use std::os::unix::net::UnixStream;
        use std::sync::Arc;
        use std::sync::atomic::Ordering;
        use std::time::{Duration, Instant};

        use counter_server::{CounterQuery, CounterReport, KIND_RESP_ACK, ResponseEncoder};
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::pipeline::{OutputPayload, OutputSlot, RequestId};
        use melin_wire_protocol::control_codec::TAG_BATCH_END;

        use super::early_drain::egress_over;
        use super::refusal_order::{Seen, read_frame};
        use crate::durability_gate::{Backing, BatchEnds};
        use crate::response::{MAX_SEND_BUF, UringEgress};
        use crate::response_frame::{EncodeBuf, MAX_APP_FRAME};
        use crate::response_loop::{Egress as _, SlotOutcome, SlotReply};

        /// The request every slot here ends.
        const REQUEST: RequestId = RequestId(7);

        /// Hand `egress` a request's last slot for `connection_id`, an ack
        /// and its terminator, backed by `backing`, encoded into
        /// `encode_buf`.
        fn send_last_slot(
            egress: &mut UringEgress,
            connection_id: u64,
            backing: Backing,
            encode_buf: &mut EncodeBuf,
        ) -> SlotOutcome {
            let slot = OutputSlot::<CounterReport, CounterQuery> {
                connection_id,
                request_id: REQUEST,
                wire_seq: 1,
                payload: OutputPayload::Report(CounterReport::Ack { new_value: 7 }),
                is_last_in_request: true,
                ..Default::default()
            };
            let mut batch_ends = BatchEnds::new();
            egress.send_slot(
                connection_id,
                SlotReply::new(
                    &slot,
                    backing,
                    &ResponseEncoder,
                    encode_buf,
                    &mut batch_ends,
                ),
                Instant::now(),
            )
        }

        /// A last slot whose append would take its connection past
        /// `MAX_SEND_BUF` sends nothing and queues the connection for
        /// removal, and is still `Known`: the connection was found, which
        /// is what the loop's degraded-ack count and its miss log go by.
        #[test]
        fn an_overflowing_last_slot_is_known_and_queues_its_connection_for_removal() {
            let (server, _client) = UnixStream::pair().expect("socketpair");
            let (mut egress, active) = egress_over(server);
            // One byte short of the cap: no frame fits.
            egress
                .connections
                .get_mut(&1)
                .expect("connection 1")
                .send_buf
                .resize(MAX_SEND_BUF - 1, 0);

            let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
            let outcome = send_last_slot(&mut egress, 1, Backing::PrimaryOnly, &mut encode_buf);

            assert_eq!(outcome, SlotOutcome::Known);
            assert_eq!(egress.to_remove, [1], "queued for removal");
            let entry = egress.connections.get(&1).expect("removed only later");
            assert_eq!(entry.send_buf.len(), MAX_SEND_BUF - 1, "nothing appended");
            assert!(egress.dirty.is_empty(), "nothing to flush");
            assert_eq!(active.load(Ordering::Relaxed), 1, "not torn down yet");
        }

        /// A slot for a connection the egress does not hold is `Unknown`,
        /// and nothing is encoded for it.
        #[test]
        fn a_slot_for_an_unknown_connection_is_unknown_and_encodes_nothing() {
            let (server, _client) = UnixStream::pair().expect("socketpair");
            let (mut egress, _active) = egress_over(server);

            let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
            let outcome = send_last_slot(&mut egress, 9, Backing::Policy, &mut encode_buf);

            assert_eq!(outcome, SlotOutcome::Unknown);
            assert!(encode_buf.iter().all(|&b| b == 0), "the reply was encoded");
            assert!(egress.to_remove.is_empty());
            assert!(egress.dirty.is_empty());
        }

        /// An egress holding a reply buffered for connection 1, fenced or
        /// not, after its way out; and the client's end of the socket.
        fn shut_down_holding_a_reply(fenced: bool) -> UnixStream {
            let (server, client) = UnixStream::pair().expect("socketpair");
            let (mut egress, _active) = egress_over(server);
            if fenced {
                let fence = FenceState::new(0);
                fence.fence();
                egress.fence_state = Arc::new(fence);
            }
            let mut encode_buf: EncodeBuf = [0u8; MAX_APP_FRAME];
            assert_eq!(
                send_last_slot(&mut egress, 1, Backing::Policy, &mut encode_buf),
                SlotOutcome::Known
            );
            assert_eq!(egress.dirty, [1], "the reply is buffered");
            egress.on_shutdown();
            client
                .set_read_timeout(Some(Duration::from_millis(200)))
                .expect("set read timeout");
            client
        }

        /// A stage stopped unfenced flushes what it buffered on the way
        /// out.
        #[test]
        fn the_way_out_flushes_what_is_buffered() {
            let mut client = shut_down_holding_a_reply(false);
            let seen: Vec<Seen> = (0..2)
                .map(|_| read_frame(&mut client).expect("the buffered reply"))
                .collect();
            assert_eq!(
                seen,
                [
                    Seen::App(KIND_RESP_ACK),
                    Seen::Final(TAG_BATCH_END, REQUEST)
                ]
            );
        }

        /// A fenced stage sends nothing on the way out: a superseded
        /// ex-primary acknowledges no further work.
        #[test]
        fn a_fenced_way_out_sends_nothing() {
            let mut client = shut_down_holding_a_reply(true);
            assert!(read_frame(&mut client).is_err(), "a reply went out");
        }
    }

    /// The io_uring stage's gate-wait hook, called directly: the stage
    /// tests drive it through a running stage, where whether a reply
    /// queued during a wait goes out from the hook or from the boundary
    /// drain after the admit is up to the scheduler.
    mod gate_wait_hook {
        use std::os::unix::net::UnixStream;
        use std::sync::Arc;
        use std::sync::atomic::AtomicU64;
        use std::sync::mpsc;
        use std::time::Duration;

        use counter_server::{CounterReport, ResponseEncoder};
        use io_uring::IoUring;

        use super::refusal_order::{Seen, connection, early, early_id, frames_of, read_frame};
        use crate::batch_boundary::BatchBoundary;
        use crate::durability_gate::GateWait;
        use crate::early_reply::{EarlyKind, early_reply_channel};
        use crate::response::UringEgress;
        use crate::response_frame::{EarlyReplyBuf, MAX_EARLY_REPLY};
        use crate::response_loop::GateWaitHook;

        const BUSY: EarlyKind<CounterReport> = EarlyKind::Busy;

        /// Register nothing, then — as if during a wait — queue
        /// connection 1's `Connected` and a busy for it, and call the
        /// hook's `while_waiting` once, between events or not. Returns the
        /// egress after the call and the client's end of the socket.
        fn one_turn(between_events: bool) -> (UringEgress, UnixStream) {
            let mut egress = UringEgress::new(
                IoUring::new(64).expect("an io_uring"),
                64,
                Arc::new(AtomicU64::new(1)),
                #[cfg(feature = "latency-trace")]
                melin_transport_core::trace::register_stage("test: server e2e"),
                #[cfg(feature = "tick-to-trade")]
                melin_transport_core::trace::register_stage("test: egress"),
            );
            let (control_tx, control_rx) = mpsc::channel();
            let (mut tx, mut rx) =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY);
            let mut registered = rx.register(|| {});

            let (event, client) = connection(1);
            control_tx.send(event).expect("the receiver is held");
            tx.try_send(early(1, 0, BUSY)).expect("queue has room");
            tx.flush();

            let boundary = BatchBoundary::new();
            let mut early_buf: EarlyReplyBuf = [0u8; MAX_EARLY_REPLY];
            GateWaitHook {
                egress: &mut egress,
                at_boundary: if between_events {
                    Some(boundary.at_boundary().expect("between events"))
                } else {
                    None
                },
                registered: &mut registered,
                control_rx: &control_rx,
                encoder: &ResponseEncoder,
                early_buf: &mut early_buf,
            }
            .while_waiting();
            (egress, client)
        }

        #[test]
        fn between_events_it_registers_sends_and_flushes() {
            let (egress, mut client) = one_turn(true);

            assert!(egress.connections.contains_key(&1), "not registered");
            assert!(egress.dirty.is_empty(), "sent but left buffered");
            // A reply that never reached the socket fails the read
            // rather than hanging the test.
            client
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("set read timeout");
            let expected = frames_of(BUSY, early_id(0));
            let seen: Vec<Seen> = expected
                .iter()
                .map(|_| read_frame(&mut client).expect("the busy's frames"))
                .collect();
            assert_eq!(seen, expected);
        }

        #[test]
        fn inside_an_event_it_does_nothing() {
            let (egress, mut client) = one_turn(false);

            assert!(egress.connections.is_empty(), "control drained");
            assert!(egress.dirty.is_empty());
            client
                .set_read_timeout(Some(Duration::from_millis(50)))
                .expect("set read timeout");
            assert!(read_frame(&mut client).is_err(), "a reply went out");
        }
    }

    /// The stage learns of a connection from its control channel and of
    /// that connection's replies from the output ring. A reply always
    /// trails its `Connected`, so reading the ring before draining the
    /// channel is enough for every slot read to find its connection.
    /// Drained first, the stage could read the ring after a `Connected`
    /// it had not seen — the accept loop queues that event only after
    /// the client holds `ServerReady`, so a first request is often on
    /// the wire before the stage has heard of its connection — and drop
    /// the reply. Seen as a first reply lost on a fresh node under
    /// full-suite load. The seam sits right
    /// after the drain: with the drain after the read that is harmless,
    /// with the drain before it the seam is the gap, and a `Connected`
    /// landing there is missed by the read that follows. Holding the
    /// stage at the seam makes the interleaving the test's rather than
    /// the scheduler's.
    mod registration_race {
        use std::io::{Read, Write};
        use std::os::unix::io::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::sync::{Arc, mpsc};
        use std::thread;
        use std::time::Duration;

        use counter_server::{
            Counter, CounterQuery, CounterReport, KIND_RESP_ACK, ResponseEncoder,
        };
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_pipeline::wait::WaitStrategy;
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::pipeline::{OutputPayload, OutputSlot, StageUtilization};
        use melin_transport_core::{DurableWireSeqCursor, WireSeq};
        use melin_wire_protocol::blocking::BlockingFrameWriter;
        use melin_wire_protocol::control_codec::TAG_APP;

        use crate::ControlEvent;
        use crate::ack_policy::AckPolicy;
        use crate::response::{Response, run};

        /// `CounterReport::Ack` on the wire: len(4) + tag(1), then the
        /// counter's body, kind(1) + value(8).
        const FRAME_LEN: usize = 14;
        /// Long enough that a genuine loss is distinguishable from
        /// scheduler noise on a loaded box, short enough to fail in
        /// bounded time.
        const READ_TIMEOUT: Duration = Duration::from_secs(5);

        #[test]
        fn a_reply_that_lands_with_its_connection_is_delivered() {
            let (mut producer, mut consumers) =
                DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(64)
                    .add_consumer()
                    .build(WaitStrategy::SpinThenYield);
            let consumer = consumers.pop().expect("one consumer was requested");

            let (server_sock, mut client_sock) = UnixStream::pair().expect("socketpair");
            let server_fd = server_sock.as_raw_fd();
            let writer = BlockingFrameWriter::new(Box::new(server_sock) as Box<dyn Write + Send>);
            client_sock
                .set_read_timeout(Some(READ_TIMEOUT))
                .expect("set read timeout");

            // The stage reports reaching the gap on `parked` and waits on
            // `resume` — once, on its first iteration.
            let (parked_tx, parked_rx) = mpsc::channel::<()>();
            let (resume_tx, resume_rx) = mpsc::channel::<()>();
            let mut held = false;
            let pause = move || {
                if !held {
                    held = true;
                    parked_tx.send(()).expect("the test is waiting");
                    resume_rx.recv().expect("the test resumes the stage");
                }
            };

            let (control_tx, control_rx) = mpsc::channel();
            let shutdown = AtomicBool::new(false);
            let config = Response::<Counter> {
                // The journal is already past the one event, so the gate
                // is open and delivery hinges on registration alone.
                journal_persisted_wire_seq: DurableWireSeqCursor::detached(WireSeq::new(1)),
                policy: crate::response::GatePolicy {
                    start: crate::durability_gate::GateStart::settled(AckPolicy::Disk),
                    forced: melin_transport_core::lineage::forced_policy_cell().1,
                },
                replication_metrics: None,
                replica_active: None,
                heartbeat_interval: None,
                wait: WaitStrategy::SpinThenYield,
                utilization: Arc::new(StageUtilization::default()),
                encoder: Arc::new(ResponseEncoder),
                fence_state: Arc::new(FenceState::new(0)),
                active_connections: Arc::new(AtomicU64::new(0)),
                // No reader: nothing is ever answered early.
                early_replies: crate::early_reply::early_reply_channel(
                    crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY,
                )
                .1,
                ring_sizing: crate::connection_limit::RingSizing::for_max_connections(1)
                    .expect("a supported cap"),
                ready: None,
                degraded_release: None,
                pause_after_control_drain: Some(Box::new(pause)),
            };

            thread::scope(|scope| {
                let stage = scope.spawn(|| run::<Counter>(consumer, control_rx, config, &shutdown));

                // The stage has drained an empty control channel and is
                // held at the seam. Now both arrive: read after drain,
                // the next iteration finds the slot and then its
                // connection; drain after read, the read would find the
                // slot and the connection would still be in the channel.
                parked_rx.recv().expect("the stage reaches the seam");
                control_tx
                    .send(ControlEvent::Connected {
                        connection_id: 1,
                        fd: server_fd,
                        writer,
                    })
                    .expect("stage is running");
                producer.publish(OutputSlot {
                    connection_id: 1,
                    wire_seq: 1,
                    payload: OutputPayload::Report(CounterReport::Ack { new_value: 7 }),
                    // No trailing `BatchEnd`: one frame on the wire.
                    is_last_in_request: false,
                    ..Default::default()
                });
                resume_tx.send(()).expect("the stage is parked");

                let mut frame = [0u8; FRAME_LEN];
                let delivered = client_sock.read_exact(&mut frame);
                shutdown.store(true, Ordering::Relaxed);
                stage.join().expect("stage panicked");
                delivered.expect(
                    "the reply was dropped: the control channel was drained before the ring was read",
                );
                assert_eq!(
                    frame[4..6],
                    [TAG_APP, KIND_RESP_ACK],
                    "an application frame carrying the counter's ack"
                );
                assert_eq!(
                    u64::from_le_bytes(frame[6..].try_into().expect("8 bytes")),
                    7,
                    "the ack carries the counter's value"
                );
            });
        }
    }

    /// An early reply (a write refused while halted, a request filtered or
    /// refused by the decoder, one shed on a full ring) reaches the stage
    /// only with its connection registered.
    ///
    /// Its frame classifier and early-reply builders are shared with the
    /// other early-reply tests here and the DPDK stage's
    /// (`dpdk_response.rs`).
    pub(crate) mod refusal_order {
        use std::io::{Read, Write};
        use std::os::unix::io::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        use std::sync::{Arc, mpsc};
        use std::thread;
        use std::time::Duration;

        use counter_server::{
            Counter, CounterQuery, CounterReport, KIND_RESP_REJECTED, ResponseEncoder,
        };
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_pipeline::wait::WaitStrategy;
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::pipeline::{OutputSlot, RequestId, StageUtilization};
        use melin_transport_core::{DurableWireSeqCursor, WireSeq};
        use melin_wire_protocol::blocking::BlockingFrameWriter;
        use melin_wire_protocol::control_codec::{
            TAG_APP, TAG_BATCH_END, TAG_REFUSED, TAG_SERVER_BUSY, terminal_request_id,
        };

        use crate::ControlEvent;
        use crate::ack_policy::AckPolicy;
        use crate::early_reply::{EarlyKind, EarlyReply, EarlyReplyQueue, early_reply_channel};
        use crate::response::{Response, run};

        /// Bounds a read that must succeed; generous for a loaded box.
        pub(crate) const READ_TIMEOUT: Duration = Duration::from_secs(5);

        /// What one frame on the wire was: a counter response, by the
        /// kind its body starts with; a frame that ends a request's reply,
        /// by its tag and the request's ID; or another of the protocol's
        /// own frames, by its tag.
        #[derive(Debug, PartialEq, Eq)]
        pub(crate) enum Seen {
            App(u8),
            Final(u8, RequestId),
            Protocol(u8),
        }

        impl Seen {
            /// What a frame with this payload (its length prefix stripped)
            /// is.
            pub(crate) fn of(payload: &[u8]) -> Self {
                if let Some(id) = terminal_request_id(payload) {
                    return Seen::Final(payload[0], id);
                }
                match *payload {
                    [TAG_APP, kind, ..] => Seen::App(kind),
                    // Every counter response carries a kind.
                    [TAG_APP] => panic!("a counter response with an empty body"),
                    [tag, ..] => Seen::Protocol(tag),
                    [] => panic!("an empty frame on the wire"),
                }
            }
        }

        /// The ID early reply number `n` carries in these tests: distinct
        /// per reply, and from the events' own.
        pub(crate) fn early_id(n: u64) -> RequestId {
            RequestId(1000 + n)
        }

        /// Early reply number `n`, of `kind`, for `connection_id`: its
        /// request is [`early_id`]`(n)`.
        pub(crate) fn early(
            connection_id: u64,
            n: u64,
            kind: EarlyKind<CounterReport>,
        ) -> EarlyReply<CounterReport> {
            EarlyReply {
                connection_id,
                request_id: early_id(n),
                kind,
            }
        }

        /// The frames an early reply of `kind` for request `id` goes out
        /// as.
        pub(crate) fn frames_of(kind: EarlyKind<CounterReport>, id: RequestId) -> Vec<Seen> {
            match kind {
                EarlyKind::Rejected(_) => {
                    vec![
                        Seen::App(KIND_RESP_REJECTED),
                        Seen::Final(TAG_BATCH_END, id),
                    ]
                }
                EarlyKind::Filtered => vec![Seen::Final(TAG_BATCH_END, id)],
                EarlyKind::Refused { .. } => vec![Seen::Final(TAG_REFUSED, id)],
                EarlyKind::Busy => vec![Seen::Final(TAG_SERVER_BUSY, id)],
            }
        }

        /// Read one frame and say what it was.
        pub(super) fn read_frame(sock: &mut UnixStream) -> std::io::Result<Seen> {
            let mut len = [0u8; 4];
            sock.read_exact(&mut len)?;
            let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
            sock.read_exact(&mut payload)?;
            Ok(Seen::of(&payload))
        }

        /// Read one frame into `seen`; false if none arrived.
        fn read_into(sock: &mut UnixStream, seen: &mut Vec<Seen>) -> bool {
            match read_frame(sock) {
                Ok(frame) => {
                    seen.push(frame);
                    true
                }
                Err(_) => false,
            }
        }

        /// A refusal reaches the stage only with its connection registered.
        /// The reader refuses a new connection's first request as soon as
        /// that connection's `Connected` is queued, so a refusal made
        /// visible while the stage is past its control drain can belong to
        /// a connection the stage has not heard of. Released in that
        /// iteration it would be dropped, and the client left without a
        /// reply.
        ///
        /// The seam holds the stage after its drain twice. The first hold
        /// lands connection 1's refusal, which the next iteration's idle
        /// path is then due to release; the second hold, in that iteration,
        /// lands connection 2 and its refusal just ahead of the release.
        /// The same inside a gate wait is `early_release`'s
        /// `connection_first_seen_inside_a_gate_wait`.
        #[test]
        fn a_refusal_is_not_released_ahead_of_its_connection() {
            let (_producer, mut consumers) =
                DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(64)
                    .add_consumer()
                    .build(WaitStrategy::SpinThenYield);
            let consumer = consumers.pop().expect("one consumer was requested");
            let (connected_1, mut client_1) = connection(1);
            let (connected_2, mut client_2) = connection(2);

            let (mut refusals, refusal_rx) =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY);
            let (control_tx, control_rx) = mpsc::channel();
            control_tx.send(connected_1).expect("channel open");

            let (parked_tx, parked_rx) = mpsc::channel::<()>();
            let (resume_tx, resume_rx) = mpsc::channel::<()>();
            let mut holds = 0;
            let pause = move || {
                if holds < 2 {
                    holds += 1;
                    parked_tx.send(()).expect("the test is waiting");
                    resume_rx.recv().expect("the test resumes the stage");
                }
            };
            let shutdown = AtomicBool::new(false);
            let config = config(
                DurableWireSeqCursor::detached(WireSeq::new(0)),
                Arc::new(FenceState::new(0)),
                refusal_rx,
                Some(Box::new(pause)),
            );

            thread::scope(|scope| {
                let stage = scope.spawn(|| run::<Counter>(consumer, control_rx, config, &shutdown));

                parked_rx.recv().expect("the stage reaches the seam");
                refusals.try_send(refusal(1, 0)).expect("queue has room");
                refusals.flush();
                resume_tx.send(()).expect("the stage is parked");

                parked_rx.recv().expect("the stage reaches the seam again");
                control_tx.send(connected_2).expect("stage is running");
                refusals.try_send(refusal(2, 0)).expect("queue has room");
                refusals.flush();
                resume_tx.send(()).expect("the stage is parked");

                let mut seen_1 = Vec::new();
                let mut seen_2 = Vec::new();
                let got_1 = (0..2).all(|_| read_into(&mut client_1, &mut seen_1));
                let got_2 = (0..2).all(|_| read_into(&mut client_2, &mut seen_2));
                shutdown.store(true, Ordering::Relaxed);
                stage.join().expect("stage panicked");

                assert!(got_1, "connection 1's refusal never arrived: {seen_1:x?}");
                assert!(
                    got_2,
                    "connection 2's refusal was dropped: released before its connection registered"
                );
                for seen in [seen_1, seen_2] {
                    assert_eq!(
                        seen,
                        [
                            Seen::App(KIND_RESP_REJECTED),
                            Seen::Final(TAG_BATCH_END, early_id(0))
                        ]
                    );
                }
            });
        }

        /// A registered connection: its `Connected` event, and the client's
        /// end of the socket.
        pub(super) fn connection(connection_id: u64) -> (ControlEvent, UnixStream) {
            let (server_sock, client_sock) = UnixStream::pair().expect("socketpair");
            client_sock
                .set_read_timeout(Some(READ_TIMEOUT))
                .expect("set read timeout");
            let event = ControlEvent::Connected {
                connection_id,
                fd: server_sock.as_raw_fd(),
                writer: BlockingFrameWriter::new(Box::new(server_sock) as Box<dyn Write + Send>),
            };
            (event, client_sock)
        }

        /// Halt refusal number `n`, for `connection_id`.
        pub(crate) fn refusal(connection_id: u64, n: u64) -> EarlyReply<CounterReport> {
            early(
                connection_id,
                n,
                EarlyKind::Rejected(CounterReport::Rejected),
            )
        }

        /// A stage gating on `journal_cursor` under `disk`, fed
        /// `early_replies`.
        pub(super) fn config(
            journal_cursor: DurableWireSeqCursor,
            fence_state: Arc<FenceState>,
            early_replies: EarlyReplyQueue<CounterReport>,
            pause_after_control_drain: Option<Box<dyn FnMut() + Send>>,
        ) -> Response<Counter> {
            Response::<Counter> {
                journal_persisted_wire_seq: journal_cursor,
                policy: crate::response::GatePolicy {
                    start: crate::durability_gate::GateStart::settled(AckPolicy::Disk),
                    forced: melin_transport_core::lineage::forced_policy_cell().1,
                },
                replication_metrics: None,
                replica_active: None,
                heartbeat_interval: None,
                wait: WaitStrategy::SpinThenYield,
                utilization: Arc::new(StageUtilization::default()),
                encoder: Arc::new(ResponseEncoder),
                fence_state,
                active_connections: Arc::new(AtomicU64::new(0)),
                early_replies,
                ring_sizing: crate::connection_limit::RingSizing::for_max_connections(4)
                    .expect("a supported cap"),
                ready: None,
                degraded_release: None,
                pause_after_control_drain,
            }
        }
    }

    /// Early replies go out at the first point between two events'
    /// replies, without waiting for replies to earlier requests, and never
    /// inside an event's reply. The scenarios and their rig are shared
    /// with the DPDK stage, whose tests run each against its own harness
    /// (`dpdk_response.rs`); the kernel stage's harness is here. So are
    /// the ports of the kernel-only loop tests (`registration_race`,
    /// `refusal_order`, `stop_while_gated`'s stopped case, and
    /// `policy_change`), which pin both stages before their loops are
    /// shared (`docs/internal/shared-response-loop-2026-10.md`).
    pub(crate) mod early_release {
        use std::collections::{BTreeMap, HashMap};
        use std::os::unix::net::UnixStream;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, OnceLock, mpsc};
        use std::thread::{self, JoinHandle};
        use std::time::{Duration, Instant};

        use counter_server::{
            Counter, CounterQuery, CounterReport, KIND_RESP_ACK, KIND_RESP_REJECTED,
        };
        use melin_journal::AckPolicyChange;
        use melin_pipeline::ring::{self, DisruptorBuilder};
        use melin_pipeline::wait::WaitStrategy;
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::lineage::{
            ForcedPolicy, ForcedPolicyReader, ForcedPolicyWriter, forced_policy_cell,
        };
        use melin_transport_core::pipeline::{
            OutputPayload, OutputSlot, RequestId, StageUtilization,
        };
        use melin_transport_core::{DurableWireSeqCursor, WireSeq};
        use melin_wire_protocol::control::{MAX_REFUSAL_REASON, RefusalKind};
        use melin_wire_protocol::control_codec::{TAG_BATCH_END, TAG_RESPONSE_HEARTBEAT};
        use proptest::prelude::*;

        use super::refusal_order::{
            READ_TIMEOUT, Seen, config, connection, early, early_id, frames_of, read_frame,
        };
        use crate::ControlEvent;
        use crate::ack_policy::AckPolicy;
        use crate::early_reply::{
            EarlyKind, EarlyReplyQueue, EarlyReplySender, early_reply_channel,
        };
        use crate::replication::ReplicationMetrics;
        use crate::response::run;

        /// How long a test watches for a frame that must not arrive yet:
        /// long enough that a stage wrongly sending it would have.
        pub(crate) const QUIET: Duration = Duration::from_millis(100);

        /// An output slot of the counter.
        pub(crate) type Slot = OutputSlot<CounterReport, CounterQuery>;

        /// Every kind of early reply, a decoder refusal with the widest
        /// reason a frame carries.
        pub(crate) fn every_kind() -> [EarlyKind<CounterReport>; 4] {
            [
                EarlyKind::Rejected(CounterReport::Rejected),
                EarlyKind::Filtered,
                EarlyKind::Refused {
                    kind: RefusalKind::Malformed,
                    reason: widest_reason(),
                },
                EarlyKind::Busy,
            ]
        }

        /// A refusal reason as wide as a `Refused` frame carries.
        pub(crate) fn widest_reason() -> &'static str {
            static REASON: OnceLock<&'static str> = OnceLock::new();
            REASON.get_or_init(|| "r".repeat(MAX_REFUSAL_REASON).leak())
        }

        /// What a stage under test starts with: the output ring, the
        /// early-reply queue, and a gate under `disk+ram` over the journal
        /// cursor and one replica, whose in-memory cursor is far ahead,
        /// so the journal decides what is held until the replica is
        /// marked gone. The forced-policy cell's reader goes to the stage;
        /// the test keeps the writer ([`Rig::forced`]).
        pub(crate) struct StageInputs {
            pub(crate) consumer: ring::Consumer<Slot>,
            pub(crate) early_replies: EarlyReplyQueue<CounterReport>,
            pub(crate) journal: DurableWireSeqCursor,
            pub(crate) replication_metrics: Arc<ReplicationMetrics>,
            pub(crate) replica_active: [Arc<AtomicBool>; 2],
            pub(crate) utilization: Arc<StageUtilization>,
            pub(crate) forced: ForcedPolicyReader,
            pub(crate) shutdown: Arc<AtomicBool>,
            /// The stage's test seam, called right after each control
            /// drain; `None` runs it as production does.
            pub(crate) pause_after_control_drain: Option<Box<dyn FnMut() + Send>>,
            /// The stage's heartbeat interval; `None` sends none, so a
            /// test that does not ask for heartbeats sees only replies.
            pub(crate) heartbeat_interval: Option<Duration>,
        }

        /// A running response stage, seen from its clients. Stops the
        /// stage when dropped, so a failed assertion does not leave it
        /// spinning.
        pub(crate) trait StageHarness {
            /// Register `connection_id` with the stage: its `Connected`
            /// on the control channel.
            fn connect(&mut self, connection_id: u64);
            /// The next frame the stage sent `connection_id`, waiting up
            /// to `within`; `None` if none came.
            fn next_frame(&mut self, connection_id: u64, within: Duration) -> Option<Seen>;
            /// Tell the stage to stop, and whether its thread returned
            /// within `within`. See [`stopped_within`].
            fn stopped_within(&mut self, within: Duration) -> bool;
        }

        /// A harness's stop probe: set `shutdown`, take the stage's
        /// thread and poll it until `within` has passed. A thread that
        /// finished is joined, and a panic out of it is propagated, so a
        /// stage that panicked out of the gate fails the test rather than
        /// passing as stopped. A thread still running at the deadline is
        /// detached, and `false` returned: the harness's `Drop` then has
        /// nothing to join, and does not hang on the very wedge the probe
        /// exists to catch.
        pub(crate) fn stopped_within<T>(
            shutdown: &AtomicBool,
            handle: &mut Option<JoinHandle<T>>,
            within: Duration,
        ) -> bool {
            shutdown.store(true, Ordering::Relaxed);
            let handle = handle.take().expect("the stage is probed once");
            let deadline = Instant::now() + within;
            while !handle.is_finished() {
                if Instant::now() >= deadline {
                    // Detached: the thread may outlive the test, and dies
                    // with the process.
                    drop(handle);
                    return false;
                }
                thread::sleep(Duration::from_millis(5));
            }
            if let Err(panic) = handle.join() {
                std::panic::resume_unwind(panic);
            }
            true
        }

        /// The test's side of a stage under test: it plays the matching
        /// stage, the reader, the journal, the replica and the operator
        /// forcing a policy.
        pub(crate) struct Rig<H> {
            /// The test's end of the drain seam, if the stage has one.
            /// Declared first so it drops first: a test failing while the
            /// stage is parked, or before its next hold, disconnects the
            /// seam before `stage`'s `Drop` joins the stage, and the stage
            /// carries on instead of waiting out [`SEAM_TIMEOUT`].
            seam: Option<Seam>,
            pub(crate) producer: ring::Producer<Slot>,
            pub(crate) early: EarlyReplySender<CounterReport>,
            pub(crate) journal: DurableWireSeqCursor,
            pub(crate) replica_active: Arc<AtomicBool>,
            pub(crate) utilization: Arc<StageUtilization>,
            pub(crate) forced: ForcedPolicyWriter,
            pub(crate) stage: H,
        }

        /// Start a stage with `start`, the journal at `journal_at`, a
        /// drain seam holding it on its first `seam_holds` iterations
        /// (none if zero; see [`Rig::seam`]), and `connections` registered
        /// right after it starts (none if empty).
        pub(crate) fn rig<H: StageHarness>(
            start: fn(StageInputs) -> H,
            journal_at: u64,
            connections: &[u64],
            seam_holds: usize,
        ) -> Rig<H> {
            rig_with_heartbeats(start, journal_at, connections, seam_holds, None)
        }

        /// [`rig`], with the stage sending heartbeats every
        /// `heartbeat_interval` (none if `None`).
        fn rig_with_heartbeats<H: StageHarness>(
            start: fn(StageInputs) -> H,
            journal_at: u64,
            connections: &[u64],
            seam_holds: usize,
            heartbeat_interval: Option<Duration>,
        ) -> Rig<H> {
            let (pause, seam) = match seam_holds {
                0 => (None, None),
                holds => {
                    let (pause, seam) = seam(holds);
                    (Some(pause), Some(seam))
                }
            };
            let (producer, mut consumers) = DisruptorBuilder::<Slot>::new(128)
                .add_consumer()
                .build(WaitStrategy::SpinThenYield);
            let consumer = consumers.pop().expect("one consumer was requested");
            let (early, early_replies) =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY);
            let journal = DurableWireSeqCursor::detached(WireSeq::new(journal_at));
            let replication_metrics = Arc::new(ReplicationMetrics::default());
            replication_metrics.in_memory_sequence[0].store(1_000_000, Ordering::Relaxed);
            let replica_active = Arc::new(AtomicBool::new(true));
            let utilization = Arc::new(StageUtilization::default());
            let (forced, forced_reader) = forced_policy_cell();
            let mut stage = start(StageInputs {
                consumer,
                early_replies,
                journal: journal.clone(),
                replication_metrics,
                replica_active: [
                    Arc::clone(&replica_active),
                    Arc::new(AtomicBool::new(false)),
                ],
                utilization: Arc::clone(&utilization),
                forced: forced_reader,
                shutdown: Arc::new(AtomicBool::new(false)),
                pause_after_control_drain: pause,
                heartbeat_interval,
            });
            for &id in connections {
                stage.connect(id);
            }
            Rig {
                seam,
                producer,
                early,
                journal,
                replica_active,
                utilization,
                forced,
                stage,
            }
        }

        /// The test's end of a stage's drain seam: the stage parks at the
        /// seam on its first `holds` iterations, until the test resumes
        /// it.
        pub(crate) struct Seam {
            parked: mpsc::Receiver<()>,
            resume: mpsc::Sender<()>,
        }

        /// How long either side of a [`Seam`] waits for the other: a stage
        /// that never parks, or a test that never resumes it, fails rather
        /// than hangs (a stage parked forever would hang the harness's
        /// `Drop`).
        const SEAM_TIMEOUT: Duration = Duration::from_secs(10);

        /// A seam that holds the stage on its first `holds` iterations,
        /// and the pause to start the stage with.
        pub(crate) fn seam(holds: usize) -> (Box<dyn FnMut() + Send>, Seam) {
            let (parked_tx, parked) = mpsc::channel();
            let (resume, resume_rx) = mpsc::channel();
            let mut left = holds;
            let pause = move || {
                if left == 0 {
                    return;
                }
                left -= 1;
                // A test that has gone (failed and unwinding) is not
                // waiting: its `Rig` drops the seam before the stage, so
                // the stage carries on and is stopped by the harness's
                // `Drop`.
                if parked_tx.send(()).is_err() {
                    return;
                }
                match resume_rx.recv_timeout(SEAM_TIMEOUT) {
                    // Resumed, or the test has gone: carry on, as above.
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => {}
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        panic!("the test never resumed the stage")
                    }
                }
            };
            (Box::new(pause), Seam { parked, resume })
        }

        impl Seam {
            /// Wait for the stage to park at the seam.
            pub(crate) fn parked(&self) {
                self.parked
                    .recv_timeout(SEAM_TIMEOUT)
                    .expect("the stage reaches the seam");
            }

            /// Let the parked stage go on.
            pub(crate) fn resume(&self) {
                self.resume.send(()).expect("the stage is parked");
            }
        }

        impl<H: StageHarness> Rig<H> {
            /// The test's end of the drain seam.
            ///
            /// # Panics
            ///
            /// If the rig was started without one.
            pub(crate) fn seam(&self) -> &Seam {
                self.seam.as_ref().expect("the rig was started with a seam")
            }

            /// Queue an early reply, flushed, as a reader does.
            pub(crate) fn answer_early(
                &mut self,
                connection_id: u64,
                n: u64,
                kind: EarlyKind<CounterReport>,
            ) {
                self.early
                    .try_send(early(connection_id, n, kind))
                    .expect("queue has room");
                self.early.flush();
            }

            /// Publish one output slot: a counter ack for `request_id` on
            /// `connection_id`, its event at `wire_seq`.
            pub(crate) fn publish(
                &mut self,
                connection_id: u64,
                request_id: u64,
                wire_seq: u64,
                is_last_in_request: bool,
            ) {
                self.producer.publish(Slot {
                    connection_id,
                    request_id: RequestId(request_id),
                    input_seq: wire_seq - 1,
                    wire_seq,
                    payload: OutputPayload::Report(CounterReport::Ack {
                        new_value: wire_seq,
                    }),
                    is_last_in_request,
                    ..Default::default()
                });
            }

            /// The next `count` frames for `connection_id`, each waited
            /// for up to [`READ_TIMEOUT`]; fewer if they stop coming.
            pub(crate) fn frames(&mut self, connection_id: u64, count: usize) -> Vec<Seen> {
                (0..count)
                    .map_while(|_| self.stage.next_frame(connection_id, READ_TIMEOUT))
                    .collect()
            }

            /// Whether nothing reaches `connection_id` for [`QUIET`].
            pub(crate) fn quiet(&mut self, connection_id: u64) -> Option<Seen> {
                self.stage.next_frame(connection_id, QUIET)
            }
        }

        /// The frames of a one-slot write's reply: the ack, then its batch
        /// end.
        fn write_reply(id: u64) -> Vec<Seen> {
            vec![
                Seen::App(KIND_RESP_ACK),
                Seen::Final(TAG_BATCH_END, RequestId(id)),
            ]
        }

        /// An idle connection is sent a heartbeat on each of the idle
        /// path's heartbeat scans, which run off its housekeeping timer:
        /// the wiring from `IdleHousekeeping`'s due scan to the stage's
        /// heartbeat send. Two in a row, so a scan that fired once and
        /// never again fails.
        pub(crate) fn an_idle_connection_is_sent_heartbeats<H: StageHarness>(
            start: fn(StageInputs) -> H,
        ) {
            let mut rig = rig_with_heartbeats(start, 0, &[1], 0, Some(Duration::from_millis(10)));
            assert_eq!(
                rig.frames(1, 2),
                [
                    Seen::Protocol(TAG_RESPONSE_HEARTBEAT),
                    Seen::Protocol(TAG_RESPONSE_HEARTBEAT),
                ]
            );
        }

        /// An early reply for a connection whose event straddles two reads
        /// of the output ring (the matching stage has published only its
        /// first slot) waits for the event's terminator, and goes out
        /// right after it, never between its frames.
        pub(crate) fn an_early_reply_waits_out_an_event_split_across_reads<H: StageHarness>(
            start: fn(StageInputs) -> H,
        ) {
            let mut rig = rig(start, 10, &[1], 0);
            rig.publish(1, 7, 1, false);
            assert_eq!(
                rig.frames(1, 1),
                [Seen::App(KIND_RESP_ACK)],
                "the first slot"
            );

            rig.answer_early(1, 0, EarlyKind::Busy);
            assert_eq!(
                rig.quiet(1),
                None,
                "an early reply inside the event's reply"
            );

            rig.publish(1, 7, 1, true);
            let expected: Vec<Seen> = write_reply(7)
                .into_iter()
                .chain(frames_of(EarlyKind::Busy, early_id(0)))
                .collect();
            assert_eq!(rig.frames(1, 3), expected);
        }

        /// Finding 18's shape: early replies of `kind` go out while the
        /// gate holds an earlier write of the same connection — one queued
        /// before the stage reached the write, and one decided while it
        /// waits — and so does one for another connection; the write's
        /// reply follows once its event is durable.
        pub(crate) fn early_replies_overtake_a_held_write<H: StageHarness>(
            start: fn(StageInputs) -> H,
            kind: EarlyKind<CounterReport>,
        ) {
            let mut rig = rig(start, 0, &[1, 2], 0);
            rig.answer_early(1, 0, kind);
            rig.publish(1, 7, 1, true);
            let before = frames_of(kind, early_id(0));
            assert_eq!(
                rig.frames(1, before.len()),
                before,
                "{kind:?} queued before"
            );

            rig.answer_early(1, 1, kind);
            rig.answer_early(2, 2, kind);
            let during = frames_of(kind, early_id(1));
            assert_eq!(
                rig.frames(1, during.len()),
                during,
                "{kind:?} during the wait"
            );
            let other = frames_of(kind, early_id(2));
            assert_eq!(
                rig.frames(2, other.len()),
                other,
                "{kind:?} for another connection"
            );
            assert_eq!(rig.quiet(1), None, "the write is still held");

            rig.journal.store(WireSeq::new(1));
            assert_eq!(rig.frames(1, 2), write_reply(7), "{kind:?}");
        }

        /// A gate that blocks on a slot in the middle of an event — the
        /// cached durable position dropped to 0 by the idle re-check of a
        /// degraded policy, after the event's first slot went out — sends
        /// no early reply until the event's terminator has gone.
        pub(crate) fn a_gate_held_inside_an_event_sends_no_early_reply<H: StageHarness>(
            start: fn(StageInputs) -> H,
        ) {
            let mut rig = rig(start, 1, &[1], 0);
            rig.publish(1, 7, 1, false);
            assert_eq!(
                rig.frames(1, 1),
                [Seen::App(KIND_RESP_ACK)],
                "the first slot"
            );

            // The replica leaves: the idle re-check finds the policy
            // degraded and pins the cached position to 0.
            rig.replica_active.store(false, Ordering::Relaxed);
            let deadline = Instant::now() + READ_TIMEOUT;
            while !rig.utilization.policy_degraded.load(Ordering::Relaxed) {
                assert!(Instant::now() < deadline, "the idle re-check never ran");
                thread::sleep(Duration::from_millis(5));
            }

            // The event's last slot is held, and an early reply comes in.
            rig.publish(1, 7, 1, true);
            rig.answer_early(1, 0, EarlyKind::Busy);
            assert_eq!(rig.quiet(1), None, "an early reply inside the held event");

            rig.replica_active.store(true, Ordering::Relaxed);
            let expected: Vec<Seen> = write_reply(7)
                .into_iter()
                .chain(frames_of(EarlyKind::Busy, early_id(0)))
                .collect();
            assert_eq!(rig.frames(1, 3), expected);
        }

        /// A connection the stage first hears of while the gate holds a
        /// write — its `Connected` and an early reply for it both land
        /// during the wait — is registered before the reply is sent,
        /// which is therefore delivered, not dropped as unknown.
        pub(crate) fn connection_first_seen_inside_a_gate_wait<H: StageHarness>(
            start: fn(StageInputs) -> H,
        ) {
            let mut rig = rig(start, 0, &[1], 0);
            rig.publish(1, 7, 1, true);
            // Proof the stage has reached the held write: an early reply
            // for connection 1 queued after it is out.
            rig.answer_early(1, 0, EarlyKind::Busy);
            assert_eq!(rig.frames(1, 1), frames_of(EarlyKind::Busy, early_id(0)));

            rig.stage.connect(2);
            let kind = EarlyKind::Rejected(CounterReport::Rejected);
            rig.answer_early(2, 1, kind);
            assert_eq!(
                rig.frames(2, 2),
                [
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(1))
                ],
                "dropped: sent before its connection registered"
            );
            assert_eq!(rig.quiet(1), None, "the write is still held");

            rig.journal.store(WireSeq::new(1));
            assert_eq!(rig.frames(1, 2), write_reply(7));
        }

        /// How long a stage gets to exit once told to, or to reach a
        /// state the test waits for; generous for a loaded box.
        pub(crate) const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

        /// `registration_race` on any stage: a reply that lands with its
        /// connection's `Connected`, both made visible while the stage is
        /// held right after its control drain, is delivered. The drain
        /// comes after the ring's read: drained first, the read would find
        /// the slot with the connection still in the channel, and drop the
        /// reply. The harness sees an application frame, not the counter's
        /// value; what the race is about is that the reply arrives.
        pub(crate) fn a_reply_that_lands_with_its_connection_is_delivered<H: StageHarness>(
            start: fn(StageInputs) -> H,
        ) {
            // The journal is already past the one event, so the gate is
            // open and delivery hinges on registration alone.
            let mut rig = rig(start, 1, &[], 1);

            rig.seam().parked();
            rig.stage.connect(1);
            // Not the last of its request: one frame, no terminator.
            rig.publish(1, 7, 1, false);
            rig.seam().resume();

            assert_eq!(
                rig.frames(1, 1),
                [Seen::App(KIND_RESP_ACK)],
                "the reply was dropped: the control channel was drained before the ring was read"
            );
        }

        /// `refusal_order` on any stage: a refusal reaches the stage only
        /// with its connection registered. The seam holds the stage after
        /// its drain twice. The first hold lands connection 1's refusal,
        /// which the next iteration's idle path is then due to release;
        /// the second hold, in that iteration, lands connection 2 and its
        /// refusal just ahead of the release.
        pub(crate) fn a_refusal_is_not_released_ahead_of_its_connection<H: StageHarness>(
            start: fn(StageInputs) -> H,
        ) {
            // Connection 1 is registered by the second iteration, whether
            // or not the first drain saw it.
            let mut rig = rig(start, 0, &[1], 2);
            let refusal = EarlyKind::Rejected(CounterReport::Rejected);

            rig.seam().parked();
            rig.answer_early(1, 0, refusal);
            rig.seam().resume();

            rig.seam().parked();
            rig.stage.connect(2);
            rig.answer_early(2, 0, refusal);
            rig.seam().resume();

            for connection in [1, 2] {
                assert_eq!(
                    rig.frames(connection, 2),
                    frames_of(refusal, early_id(0)),
                    "connection {connection}'s refusal was dropped: \
                     released before its connection registered"
                );
            }
        }

        /// `stop_while_gated`'s stopped case on any stage: a stage told
        /// to stop while the gate holds a reply that may never be
        /// confirmed leaves the gate, without sending it. (Its fenced case
        /// stays kernel-only: the DPDK stage has no flush to suppress.)
        pub(crate) fn a_stopped_stage_leaves_the_gate_and_drops_the_held_reply<H: StageHarness>(
            start: fn(StageInputs) -> H,
        ) {
            // The journal confirms the first event and never the second.
            let mut rig = rig(start, 1, &[1], 0);
            rig.publish(1, 1, 1, true);
            rig.publish(1, 2, 2, true);
            assert_eq!(
                rig.frames(1, 2),
                write_reply(1),
                "the confirmed reply goes out before the stage blocks on the gate"
            );

            assert!(
                rig.stage.stopped_within(EXIT_TIMEOUT),
                "the stage did not leave the gate on shutdown"
            );
            assert_eq!(
                rig.quiet(1),
                None,
                "a reply the policy never confirmed was sent"
            );
        }

        /// The change-slot test on any stage: a journaled policy change
        /// sends nothing, and a forced change frees a held reply only once
        /// its own entry is on disk.
        pub(crate) fn a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry<
            H: StageHarness,
        >(
            start: fn(StageInputs) -> H,
        ) {
            // Everything up to the reply after the change is on disk; the
            // rig's replica holds nothing on disk, so `two-disks` cannot
            // be met.
            let mut rig = rig(start, 3, &[1], 0);
            rig.publish(1, 1, 1, true);
            rig.producer.publish(Slot {
                // Addressed to the client's own connection, which the
                // matching stage never does: the change still sends it
                // nothing, not even a batch end.
                connection_id: 1,
                input_seq: 1,
                wire_seq: 2,
                payload: OutputPayload::AckPolicyChange(AckPolicyChange {
                    policy: AckPolicy::TwoDisks.to_byte(),
                    unwitnessed: false,
                }),
                is_last_in_request: true,
                ..Default::default()
            });
            rig.publish(1, 3, 3, true);

            assert_eq!(
                rig.frames(1, 2),
                write_reply(1),
                "the reply before the change"
            );
            let deadline = Instant::now() + EXIT_TIMEOUT;
            while rig.utilization.ack_policy_in_force.load() != Some(AckPolicy::TwoDisks.to_byte())
            {
                assert!(Instant::now() < deadline, "the stage never took the change");
                thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                rig.quiet(1),
                None,
                "a frame went out for the change slot, or the reply it holds"
            );

            // A forced swap back to `disk`, not yet on disk itself.
            rig.forced.store(ForcedPolicy {
                wire_seq: WireSeq::new(4),
                policy: AckPolicy::Disk.to_byte(),
            });
            assert_eq!(
                rig.quiet(1),
                None,
                "the forced swap applied before its entry was on disk"
            );
            rig.journal.store(WireSeq::new(4));
            assert_eq!(
                rig.frames(1, 2),
                write_reply(3),
                "the held reply, once the forced entry is on disk"
            );
        }

        /// One step of a [`Script`].
        #[derive(Clone, Debug)]
        pub(crate) enum Step {
            /// An event on `connection` with `slots` reports, its slots
            /// published one by one. With `straddle`, and more than one
            /// slot, the test pauses after the first, so the stage may
            /// read the event in two parts, and queues a busy for the same
            /// connection in the pause: the reply a stage that sent early
            /// replies mid-event would split the event with.
            Event {
                connection: u64,
                slots: u8,
                straddle: bool,
            },
            /// An early reply of kind `kind` (an index into
            /// [`every_kind`]) for `connection`.
            Early { connection: u64, kind: u8 },
            /// The journal catches up with every event published so far,
            /// and the next one: they are released, and the next event's
            /// slots pass the gate as they come.
            Open,
        }

        /// The connections a script's steps are spread over.
        const SCRIPT_CONNECTIONS: [u64; 3] = [1, 2, 3];

        pub(crate) fn step() -> impl Strategy<Value = Step> {
            prop_oneof![
                (1..=3u64, 1..=3u8, any::<bool>()).prop_map(|(connection, slots, straddle)| {
                    Step::Event {
                        connection,
                        slots,
                        straddle,
                    }
                }),
                (1..=3u64, 0..4u8).prop_map(|(connection, kind)| Step::Early { connection, kind }),
                Just(Step::Open),
            ]
        }

        /// A script: events, early replies and gate releases interleaved
        /// across connections. Short enough that its slots fit the rig's
        /// ring while the gate holds all of them.
        pub(crate) fn script() -> impl Strategy<Value = Vec<Step>> {
            proptest::collection::vec(step(), 1..24)
        }

        /// What a request's reply must be, as the client sees it: how many
        /// `Response` frames come right before its final frame, and which
        /// final frame.
        #[derive(Debug, PartialEq, Eq)]
        struct Owed {
            responses: usize,
            final_tag: u8,
        }

        /// Run `script` against a stage, then release everything, and
        /// check what each connection received: every request has exactly
        /// one final frame naming it; its `Response` frames, and only
        /// them, come right before it (none of another request's frames
        /// between them); and the replies to published events come in
        /// the order the events were published.
        pub(crate) fn every_request_is_answered_once_and_whole<H: StageHarness>(
            start: fn(StageInputs) -> H,
            script: &[Step],
        ) -> Result<(), TestCaseError> {
            let mut rig = rig(start, 0, &SCRIPT_CONNECTIONS, 0);
            // Per connection: what each request is owed, and the order
            // of its published events.
            let mut owed: HashMap<RequestId, Owed> = HashMap::new();
            let mut events: BTreeMap<u64, Vec<RequestId>> = BTreeMap::new();
            let mut expected_frames: BTreeMap<u64, usize> = BTreeMap::new();
            let mut wire_seq = 0;
            let mut early_n = 0;
            // Queue early reply number `early_n` of `kind` for
            // `connection`, and record what it is owed.
            let mut answer = |rig: &mut Rig<H>,
                              owed: &mut HashMap<RequestId, Owed>,
                              expected_frames: &mut BTreeMap<u64, usize>,
                              connection: u64,
                              kind: EarlyKind<CounterReport>| {
                let frames = frames_of(kind, early_id(early_n));
                let Some(&Seen::Final(final_tag, _)) = frames.last() else {
                    unreachable!("every early reply ends in a final frame");
                };
                owed.insert(
                    early_id(early_n),
                    Owed {
                        responses: frames.len() - 1,
                        final_tag,
                    },
                );
                *expected_frames.entry(connection).or_default() += frames.len();
                rig.answer_early(connection, early_n, kind);
                early_n += 1;
            };
            for step in script {
                match *step {
                    Step::Event {
                        connection,
                        slots,
                        straddle,
                    } => {
                        wire_seq += 1;
                        let id = RequestId(wire_seq);
                        for n in 1..=slots {
                            rig.publish(connection, id.0, wire_seq, n == slots);
                            if straddle && n == 1 && slots > 1 {
                                thread::sleep(Duration::from_millis(2));
                                answer(
                                    &mut rig,
                                    &mut owed,
                                    &mut expected_frames,
                                    connection,
                                    EarlyKind::Busy,
                                );
                            }
                        }
                        owed.insert(
                            id,
                            Owed {
                                responses: usize::from(slots),
                                final_tag: TAG_BATCH_END,
                            },
                        );
                        events.entry(connection).or_default().push(id);
                        *expected_frames.entry(connection).or_default() += usize::from(slots) + 1;
                    }
                    Step::Early { connection, kind } => answer(
                        &mut rig,
                        &mut owed,
                        &mut expected_frames,
                        connection,
                        every_kind()[usize::from(kind)],
                    ),
                    Step::Open => rig.journal.store(WireSeq::new(wire_seq + 1)),
                }
            }
            rig.journal.store(WireSeq::new(wire_seq + 1));

            for connection in SCRIPT_CONNECTIONS {
                let count = expected_frames.get(&connection).copied().unwrap_or(0);
                let frames = rig.frames(connection, count);
                prop_assert_eq!(
                    frames.len(),
                    count,
                    "connection {} got {:?}",
                    connection,
                    frames
                );
                prop_assert_eq!(
                    rig.quiet(connection),
                    None,
                    "more than owed on {}",
                    connection
                );

                let mut responses = 0;
                let mut answered = Vec::new();
                for frame in &frames {
                    match *frame {
                        Seen::App(_) => responses += 1,
                        Seen::Final(tag, id) => {
                            let owed = owed.remove(&id);
                            prop_assert_eq!(
                                owed,
                                Some(Owed {
                                    responses,
                                    final_tag: tag
                                }),
                                "request {:?} on {}: {:?}",
                                id,
                                connection,
                                frames
                            );
                            answered.push(id);
                            responses = 0;
                        }
                        Seen::Protocol(tag) => {
                            prop_assert!(false, "an unexpected frame {tag:#x} on {connection}")
                        }
                    }
                }
                prop_assert_eq!(
                    responses,
                    0,
                    "responses with no final frame on {}",
                    connection
                );
                let published: Vec<RequestId> =
                    answered.into_iter().filter(|id| id.0 <= wire_seq).collect();
                prop_assert_eq!(
                    published,
                    events.remove(&connection).unwrap_or_default(),
                    "replies to published events out of order on {}",
                    connection
                );
            }
            prop_assert!(owed.is_empty(), "never answered: {owed:?}");
            Ok(())
        }

        /// The kernel stage under test, read through a socket per client.
        pub(crate) struct UringStage {
            control: mpsc::Sender<ControlEvent>,
            clients: HashMap<u64, UnixStream>,
            shutdown: Arc<AtomicBool>,
            handle: Option<JoinHandle<bool>>,
        }

        pub(crate) fn start_uring(inputs: StageInputs) -> UringStage {
            let (control, control_rx) = mpsc::channel();
            let mut config = config(
                inputs.journal,
                Arc::new(FenceState::new(0)),
                inputs.early_replies,
                inputs.pause_after_control_drain,
            );
            config.policy.start = crate::durability_gate::GateStart::settled(AckPolicy::DiskAndRam);
            config.policy.forced = inputs.forced;
            config.replication_metrics = Some(inputs.replication_metrics);
            config.replica_active = Some(inputs.replica_active);
            config.utilization = inputs.utilization;
            config.heartbeat_interval = inputs.heartbeat_interval;
            let shutdown = Arc::clone(&inputs.shutdown);
            let consumer = inputs.consumer;
            let handle =
                thread::spawn(move || run::<Counter>(consumer, control_rx, config, &shutdown));
            UringStage {
                control,
                clients: HashMap::new(),
                shutdown: inputs.shutdown,
                handle: Some(handle),
            }
        }

        impl StageHarness for UringStage {
            fn connect(&mut self, connection_id: u64) {
                let (event, client) = connection(connection_id);
                self.control.send(event).expect("the stage is running");
                self.clients.insert(connection_id, client);
            }

            fn next_frame(&mut self, connection_id: u64, within: Duration) -> Option<Seen> {
                let client = self.clients.get_mut(&connection_id)?;
                client
                    .set_read_timeout(Some(within))
                    .expect("set read timeout");
                read_frame(client).ok()
            }

            fn stopped_within(&mut self, within: Duration) -> bool {
                stopped_within(&self.shutdown, &mut self.handle, within)
            }
        }

        impl Drop for UringStage {
            fn drop(&mut self) {
                self.shutdown.store(true, Ordering::Relaxed);
                if let Some(handle) = self.handle.take() {
                    // Joined only to surface a panic; while unwinding
                    // from a failed assertion, another panic would abort.
                    if handle.join().is_err() && !thread::panicking() {
                        panic!("the stage panicked");
                    }
                }
            }
        }

        #[test]
        fn an_idle_connection_is_sent_heartbeats_uring() {
            an_idle_connection_is_sent_heartbeats(start_uring);
        }

        #[test]
        fn an_early_reply_waits_out_an_event_split_across_reads_uring() {
            an_early_reply_waits_out_an_event_split_across_reads(start_uring);
        }

        #[test]
        fn early_replies_overtake_a_held_write_uring() {
            for kind in every_kind() {
                early_replies_overtake_a_held_write(start_uring, kind);
            }
        }

        #[test]
        fn a_gate_held_inside_an_event_sends_no_early_reply_uring() {
            a_gate_held_inside_an_event_sends_no_early_reply(start_uring);
        }

        #[test]
        fn connection_first_seen_inside_a_gate_wait_uring() {
            connection_first_seen_inside_a_gate_wait(start_uring);
        }

        #[test]
        fn a_reply_that_lands_with_its_connection_is_delivered_uring() {
            a_reply_that_lands_with_its_connection_is_delivered(start_uring);
        }

        #[test]
        fn a_refusal_is_not_released_ahead_of_its_connection_uring() {
            a_refusal_is_not_released_ahead_of_its_connection(start_uring);
        }

        #[test]
        fn a_stopped_stage_leaves_the_gate_and_drops_the_held_reply_uring() {
            a_stopped_stage_leaves_the_gate_and_drops_the_held_reply(start_uring);
        }

        #[test]
        fn a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry_uring() {
            a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry(start_uring);
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(24))]
            #[test]
            fn every_request_is_answered_once_and_whole_uring(script in script()) {
                every_request_is_answered_once_and_whole(start_uring, &script)?;
            }
        }

        /// The stop probe on a thread that returns once told to: it is
        /// joined, and the handle is gone, so a `Drop` has nothing left
        /// to join.
        #[test]
        fn the_stop_probe_joins_a_thread_that_stops() {
            let shutdown = Arc::new(AtomicBool::new(false));
            let flag = Arc::clone(&shutdown);
            let mut handle = Some(thread::spawn(move || {
                while !flag.load(Ordering::Relaxed) {
                    thread::sleep(Duration::from_millis(1));
                }
            }));
            assert!(stopped_within(&shutdown, &mut handle, EXIT_TIMEOUT));
            assert!(handle.is_none());
        }

        /// The stop probe on a thread that panicked: the panic reaches
        /// the test, rather than passing as a stop.
        #[test]
        #[should_panic(expected = "the stage panicked out of the gate")]
        fn the_stop_probe_propagates_a_panic() {
            let shutdown = AtomicBool::new(false);
            let mut handle = Some(thread::spawn(|| {
                panic!("the stage panicked out of the gate");
            }));
            stopped_within(&shutdown, &mut handle, EXIT_TIMEOUT);
        }

        /// The stop probe on a thread that ignores the flag: it gives up
        /// at the deadline and detaches the thread, so a `Drop` that
        /// joins would not hang on it.
        #[test]
        fn the_stop_probe_detaches_a_thread_that_does_not_stop() {
            let shutdown = AtomicBool::new(false);
            let (release, wedged) = mpsc::channel::<()>();
            let mut handle = Some(thread::spawn(move || {
                // Wedged until the test lets it go, whatever the flag.
                wedged.recv().expect("the test releases the thread");
            }));
            assert!(!stopped_within(
                &shutdown,
                &mut handle,
                Duration::from_millis(20)
            ));
            assert!(handle.is_none(), "the probe kept the handle");
            assert!(shutdown.load(Ordering::Relaxed), "the probe set the flag");
            release.send(()).expect("the detached thread is waiting");
        }
    }

    /// A halted node past the grace period answers a held write once its
    /// own journal holds it, ending in `BatchEndDegraded`, and the
    /// refusals around it, which wait for nothing, end in a plain
    /// `BatchEnd`. With the switch off the held write waits; the refusals
    /// still do not.
    mod degraded_release {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, mpsc};
        use std::thread;
        use std::time::Duration;

        use counter_server::{
            Counter, CounterQuery, CounterReport, KIND_RESP_ACK, KIND_RESP_REJECTED,
        };
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_pipeline::wait::WaitStrategy;
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::pipeline::{OutputPayload, OutputSlot, RequestId};
        use melin_transport_core::replica_presence::ReplicaPresence;
        use melin_transport_core::{DurableWireSeqCursor, WireSeq};
        use melin_wire_protocol::control_codec::{TAG_BATCH_END, TAG_BATCH_END_DEGRADED};

        use super::refusal_order::{Seen, config, connection, early_id, read_frame, refusal};
        use crate::ack_policy::AckPolicy;
        use crate::early_reply::early_reply_channel;
        use crate::halt::DegradedRelease;
        use crate::replication::ReplicationMetrics;
        use crate::response::run;

        /// Run a stage under `disk+ram` on a primary whose one replica is
        /// gone, with degraded release `on` (no grace) or off, holding one
        /// write the primary's journal already holds, with two refusals
        /// queued. Returns what the client read before `quiet` passed with
        /// nothing more.
        fn run_halted(on: bool, quiet: Duration) -> Vec<Seen> {
            let (mut producer, mut consumers) =
                DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(64)
                    .add_consumer()
                    .build(WaitStrategy::SpinThenYield);
            let consumer = consumers.pop().expect("one consumer was requested");
            let (connected, mut client) = connection(1);
            let (mut refusals, refusal_rx) =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY);

            let journal_cursor = DurableWireSeqCursor::detached(WireSeq::new(1));
            let mut config = config(
                journal_cursor,
                Arc::new(FenceState::new(0)),
                refusal_rx,
                None,
            );
            config.policy = crate::response::GatePolicy {
                start: crate::durability_gate::GateStart::settled(AckPolicy::DiskAndRam),
                forced: melin_transport_core::lineage::forced_policy_cell().1,
            };
            config.replication_metrics = Some(Arc::new(ReplicationMetrics::default()));
            config.replica_active = Some([
                Arc::new(AtomicBool::new(false)),
                Arc::new(AtomicBool::new(false)),
            ]);
            // No replica ever streaming: halted from the start.
            config.degraded_release = on.then(|| DegradedRelease {
                presence: Arc::new(ReplicaPresence::new()),
                grace: Duration::ZERO,
            });

            let (control_tx, control_rx) = mpsc::channel();
            control_tx.send(connected).expect("channel open");
            refusals.try_send(refusal(1, 0)).expect("queue has room");
            refusals.try_send(refusal(1, 1)).expect("queue has room");
            refusals.flush();
            producer.publish(OutputSlot {
                connection_id: 1,
                request_id: HELD,
                input_seq: 0,
                wire_seq: 1,
                payload: OutputPayload::Report(CounterReport::Ack { new_value: 7 }),
                is_last_in_request: true,
                ..Default::default()
            });

            let shutdown = AtomicBool::new(false);
            let mut seen = Vec::new();
            thread::scope(|scope| {
                let stage = scope.spawn(|| run::<Counter>(consumer, control_rx, config, &shutdown));
                client
                    .set_read_timeout(Some(quiet))
                    .expect("set read timeout");
                while let Ok(frame) = read_frame(&mut client) {
                    seen.push(frame);
                }
                shutdown.store(true, Ordering::Relaxed);
                stage.join().expect("stage panicked");
            });
            seen
        }

        /// The held write's ID.
        const HELD: RequestId = RequestId(7);

        #[test]
        fn a_held_write_is_answered_degraded_and_refusals_end_in_a_plain_batch_end() {
            let seen = run_halted(true, Duration::from_millis(500));
            assert_eq!(
                seen,
                [
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(0)),
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(1)),
                    Seen::App(KIND_RESP_ACK),
                    Seen::Final(TAG_BATCH_END_DEGRADED, HELD),
                ],
                "the refusals first, waiting for nothing; only the held write marked"
            );
        }

        #[test]
        fn with_the_switch_off_the_held_write_waits_and_the_refusals_do_not() {
            let seen = run_halted(false, Duration::from_millis(300));
            assert_eq!(
                seen,
                [
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(0)),
                    Seen::App(KIND_RESP_REJECTED),
                    Seen::Final(TAG_BATCH_END, early_id(1)),
                ],
                "both refusals go out; the held write does not"
            );
        }
    }

    /// A node stopped while a reply is held by the ack policy. The gate
    /// may never open — every replica gone under a policy that needs one —
    /// and the shutdown sequence joins the stage without a timeout, so the
    /// stage must leave the gate on shutdown. It leaves without sending
    /// the held reply, which the policy never confirmed. A fence co-sets
    /// shutdown and takes the same exit.
    mod stop_while_gated {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, mpsc};
        use std::thread;
        use std::time::{Duration, Instant};

        use counter_server::{Counter, CounterQuery, CounterReport, KIND_RESP_ACK};
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_pipeline::wait::WaitStrategy;
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::pipeline::{OutputPayload, OutputSlot, RequestId};
        use melin_transport_core::{DurableWireSeqCursor, WireSeq};
        use melin_wire_protocol::control_codec::TAG_BATCH_END;

        use super::early_release::EXIT_TIMEOUT;
        use super::refusal_order::{Seen, config, connection, read_frame};
        use crate::early_reply::early_reply_channel;
        use crate::response::run;

        fn stop_while_gated(fenced: bool) {
            let (mut producer, mut consumers) =
                DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(64)
                    .add_consumer()
                    .build(WaitStrategy::SpinThenYield);
            let consumer = consumers.pop().expect("one consumer was requested");
            let (connected, mut client) = connection(1);
            let (control_tx, control_rx) = mpsc::channel();
            control_tx.send(connected).expect("channel open");

            // The journal confirms the first event and never the second.
            let journal_cursor = DurableWireSeqCursor::detached(WireSeq::new(1));
            let fence = Arc::new(FenceState::new(0));
            let early_replies =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY).1;
            let config = config(journal_cursor, Arc::clone(&fence), early_replies, None);
            for wire_seq in [1, 2] {
                producer.publish(OutputSlot {
                    connection_id: 1,
                    request_id: RequestId(wire_seq),
                    input_seq: wire_seq - 1,
                    wire_seq,
                    payload: OutputPayload::Report(CounterReport::Ack {
                        new_value: wire_seq,
                    }),
                    is_last_in_request: true,
                    ..Default::default()
                });
            }

            // Not a scoped thread: a stage that never leaves the gate
            // would hang a scoped join, and that hang is the failure this
            // test exists to catch. The flag is leaked for the same
            // reason — the thread may outlive the test, and dies with the
            // process.
            let shutdown: &'static AtomicBool = Box::leak(Box::new(AtomicBool::new(false)));
            let stage =
                thread::spawn(move || run::<Counter>(consumer, control_rx, config, shutdown));

            // The confirmed reply goes out before the stage blocks on the
            // gate.
            let mut seen = Vec::new();
            for _ in 0..2 {
                seen.push(read_frame(&mut client).expect("the confirmed reply arrives"));
            }
            assert_eq!(
                seen,
                [
                    Seen::App(KIND_RESP_ACK),
                    Seen::Final(TAG_BATCH_END, RequestId(1))
                ]
            );

            if fenced {
                assert!(fence.fence(), "first latch");
            }
            shutdown.store(true, Ordering::Relaxed);
            let deadline = Instant::now() + EXIT_TIMEOUT;
            while !stage.is_finished() {
                assert!(
                    Instant::now() < deadline,
                    "the stage did not leave the gate on shutdown"
                );
                thread::sleep(Duration::from_millis(10));
            }
            stage.join().expect("stage panicked");

            // The held reply never went out: the stage closed the
            // connection with nothing more on it.
            assert!(
                read_frame(&mut client).is_err(),
                "a reply the policy never confirmed was sent"
            );
        }

        #[test]
        fn a_stopped_stage_leaves_the_gate_and_drops_the_held_reply() {
            stop_while_gated(false);
        }

        #[test]
        fn a_fenced_stage_leaves_the_gate_and_drops_the_held_reply() {
            stop_while_gated(true);
        }
    }

    /// A journaled policy change reaches the running stage as a slot that
    /// sends nothing, and a forced change frees a held reply only once its
    /// own entry is on disk. The switch itself is pinned by the gate's
    /// tests (`durability_gate.rs`), the in-force gauge by
    /// `tests/ack_policy_changes.rs`.
    mod policy_change {
        use std::io::ErrorKind;
        use std::os::unix::io::AsRawFd;
        use std::os::unix::net::UnixStream;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, mpsc};
        use std::thread;
        use std::time::{Duration, Instant};

        use counter_server::{Counter, CounterQuery, CounterReport, KIND_RESP_ACK};
        use melin_journal::AckPolicyChange;
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_pipeline::wait::WaitStrategy;
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::lineage::{ForcedPolicy, forced_policy_cell};
        use melin_transport_core::pipeline::{OutputPayload, OutputSlot, RequestId};
        use melin_transport_core::{DurableWireSeqCursor, WireSeq};
        use melin_wire_protocol::control_codec::TAG_BATCH_END;

        use super::refusal_order::{Seen, config, connection, read_frame};
        use crate::ack_policy::AckPolicy;
        use crate::early_reply::early_reply_channel;
        use crate::response::run;

        /// How long the test waits for the stage to reach a state.
        const SETTLE_TIMEOUT: Duration = Duration::from_secs(10);

        /// How long a held reply is watched for: long enough that a stage
        /// wrongly releasing it would have sent it.
        const HOLD: Duration = Duration::from_millis(50);

        fn reply(wire_seq: u64) -> OutputSlot<CounterReport, CounterQuery> {
            OutputSlot {
                connection_id: 1,
                request_id: RequestId(wire_seq),
                input_seq: wire_seq - 1,
                wire_seq,
                payload: OutputPayload::Report(CounterReport::Ack {
                    new_value: wire_seq,
                }),
                is_last_in_request: true,
                ..Default::default()
            }
        }

        /// No frame waiting on the client's socket.
        fn nothing_sent(client: &UnixStream) -> bool {
            let mut byte = 0u8;
            // Safety: a live socket fd and a one-byte buffer; MSG_PEEK
            // leaves anything there for the reads that follow.
            let n = unsafe {
                libc::recv(
                    client.as_raw_fd(),
                    (&raw mut byte).cast(),
                    1,
                    libc::MSG_PEEK | libc::MSG_DONTWAIT,
                )
            };
            n < 0 && std::io::Error::last_os_error().kind() == ErrorKind::WouldBlock
        }

        #[test]
        fn a_change_slot_sends_nothing_and_a_forced_one_waits_for_its_entry() {
            let (mut producer, mut consumers) =
                DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(64)
                    .add_consumer()
                    .build(WaitStrategy::SpinThenYield);
            let consumer = consumers.pop().expect("one consumer was requested");
            let (connected, mut client) = connection(1);
            let (control_tx, control_rx) = mpsc::channel();
            control_tx.send(connected).expect("channel open");

            // Everything up to the reply after the change is on disk; a
            // standalone stage cannot satisfy `two-disks` at all.
            let journal_cursor = DurableWireSeqCursor::detached(WireSeq::new(3));
            let early_replies =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY).1;
            let mut config = config(
                journal_cursor.clone(),
                Arc::new(FenceState::new(0)),
                early_replies,
                None,
            );
            let (mut forced_writer, forced) = forced_policy_cell();
            config.policy.forced = forced;
            let utilization = Arc::clone(&config.utilization);

            producer.publish(reply(1));
            producer.publish(OutputSlot {
                // Addressed to the client's own connection, which the
                // matching stage never does: the change still sends it
                // nothing, not even a batch end.
                connection_id: 1,
                input_seq: 1,
                wire_seq: 2,
                payload: OutputPayload::AckPolicyChange(AckPolicyChange {
                    policy: AckPolicy::TwoDisks.to_byte(),
                    unwitnessed: false,
                }),
                is_last_in_request: true,
                ..Default::default()
            });
            producer.publish(reply(3));

            let shutdown = AtomicBool::new(false);
            thread::scope(|scope| {
                let stage = scope.spawn(|| run::<Counter>(consumer, control_rx, config, &shutdown));

                // The reply before the change goes out; once the stage has
                // taken the change, it has sent nothing for it (and the
                // reply after it is held by `two-disks`).
                let before = [read_frame(&mut client), read_frame(&mut client)];
                let deadline = Instant::now() + SETTLE_TIMEOUT;
                while utilization.ack_policy_in_force.load() != Some(AckPolicy::TwoDisks.to_byte())
                    && Instant::now() < deadline
                {
                    thread::sleep(Duration::from_millis(1));
                }
                thread::sleep(HOLD);
                let the_change_sent_nothing = nothing_sent(&client);

                // A forced swap back to `disk`, not yet on disk itself.
                forced_writer.store(ForcedPolicy {
                    wire_seq: WireSeq::new(4),
                    policy: AckPolicy::Disk.to_byte(),
                });
                thread::sleep(HOLD);
                let held_until_the_entry_is_on_disk = nothing_sent(&client);
                journal_cursor.store(WireSeq::new(4));
                let after = [read_frame(&mut client), read_frame(&mut client)];

                shutdown.store(true, Ordering::Relaxed);
                stage.join().expect("stage panicked");

                for (frames, id) in [(before, 1), (after, 3)] {
                    let frames = frames.map(|frame| frame.expect("a reply arrived"));
                    assert_eq!(
                        frames,
                        [
                            Seen::App(KIND_RESP_ACK),
                            Seen::Final(TAG_BATCH_END, RequestId(id))
                        ]
                    );
                }
                assert!(
                    the_change_sent_nothing,
                    "a frame went out for the change slot, or the reply it holds"
                );
                assert!(
                    held_until_the_entry_is_on_disk,
                    "the forced swap applied before its entry was on disk"
                );
            });
        }
    }

    /// The stage reports whether it holds its io_uring instance before it
    /// runs, so the node can refuse to start instead of losing the stage
    /// to a panic on a thread nobody is watching.
    mod startup_report {
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, mpsc};

        use counter_server::{Counter, CounterQuery, CounterReport};
        use melin_pipeline::ring::DisruptorBuilder;
        use melin_pipeline::wait::WaitStrategy;
        use melin_transport_core::fence::FenceState;
        use melin_transport_core::pipeline::OutputSlot;
        use melin_transport_core::{DurableWireSeqCursor, WireSeq};

        use super::refusal_order::config;
        use crate::connection_limit::RingSizing;
        use crate::early_reply::early_reply_channel;
        use crate::response::run;

        /// A zero-entry ring stands in for the locked-memory limit: both
        /// fail `io_uring_setup`. The stage reports the error and returns
        /// without running — `run` returning at all is the proof, since a
        /// running stage only stops on shutdown, which is never set.
        #[test]
        fn a_ring_that_cannot_be_created_is_reported_and_the_stage_returns() {
            let (_producer, mut consumers) =
                DisruptorBuilder::<OutputSlot<CounterReport, CounterQuery>>::new(64)
                    .add_consumer()
                    .build(WaitStrategy::SpinThenYield);
            let consumer = consumers.pop().expect("one consumer was requested");
            let (_control_tx, control_rx) = mpsc::channel();
            let early_replies =
                early_reply_channel(crate::early_reply::EARLY_REPLY_QUEUE_CAPACITY).1;
            let (ready_tx, ready_rx) = mpsc::sync_channel(1);
            let mut config = config(
                DurableWireSeqCursor::detached(WireSeq::new(0)),
                Arc::new(FenceState::new(0)),
                early_replies,
                None,
            );
            config.ring_sizing = RingSizing::unchecked(0, 32);
            config.ready = Some(ready_tx);

            let started = run::<Counter>(consumer, control_rx, config, &AtomicBool::new(false));
            assert!(!started, "the stage reports that it never ran");

            let err = ready_rx
                .recv()
                .expect("the stage reports")
                .expect_err("a zero-entry ring cannot be created");
            let msg = err.to_string();
            assert!(msg.contains("response"), "{msg}");
            assert!(msg.contains("io_uring"), "{msg}");
        }
    }
}
