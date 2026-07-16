//! Health/liveness endpoint — plain TCP listener on a dedicated port.
//!
//! Supports three response modes based on the incoming request:
//!
//! 1. **Plain TCP** (no data sent): writes a one-line status and closes.
//!    Backward-compatible with Kubernetes TCP probes and `nc`.
//! 2. **HTTP `GET /`**: wraps the one-line status in an HTTP 200 response.
//! 3. **HTTP `GET /metrics`**: returns Prometheus text exposition format with
//!    all pipeline and replication counters.
//!
//! ## Plain-text response format
//!
//! ```text
//! OK <active_connections> <journal_seq> <replication_lag> trading|halted\n
//! ```
//!
//! Returns `ERR` instead of `OK` when the pipeline is unhealthy (a thread
//! panicked or the server is shutting down).
//!
//! - `active_connections`: currently authenticated client connections
//! - `journal_seq`: latest durable journal sequence number (wire-seq space —
//!   the highest sequence fsynced to this node's journal)
//! - `replication_lag`: `journal_seq - replica_quorum_ack` in wire-seq space,
//!   where `replica_quorum_ack` is the *slowest engaged* replica's durably
//!   confirmed sequence — the number of durable events not yet confirmed by
//!   every engaged replica (0 in standalone, or until a replica engages; the
//!   fastest replica's position is reported by the `fastest_replica_cursor`
//!   gauge, derived from the same per-slot cursors)

use std::io::{Cursor, Read as _, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use melin_pipeline::padding::Sequence;
use melin_pipeline::ring::QueueCursor;
use tracing::{debug, error, info};

use crate::cursors::{PipelineCursors, RingPos, WireSeq};
use crate::pipeline::{INPUT_RING_CAPACITY, StageUtilization};

/// Shared monitoring state passed to the health loop.
/// Bundles all the atomics/cursors into one struct to avoid parameter explosion.
pub struct HealthState {
    pub active_connections: Arc<AtomicU64>,
    pub events_processed: Arc<AtomicU64>,
    /// Journal-progress cursors, space-typed. The `journal_seq` gauge reads
    /// `durable_wire_seq` (wire-seq); the ring positions drive queue-depth, and
    /// the replica quorum cursor drives replication lag. See [`PipelineCursors`].
    pub cursors: PipelineCursors,
    /// Input ring producer cursor (ring-index space). Paired with the matching
    /// consumer position to compute input queue depth.
    pub input_cursor: Box<dyn QueueCursor>,
    pub pipeline_healthy: Arc<AtomicBool>,
    pub replicas_connected: Option<Arc<AtomicU32>>,
    /// Node fencing state. Folded into the `trading` flag so a fenced
    /// (superseded) ex-primary reports `halted` to probes and monitoring
    /// for the short window before the process finishes winding down —
    /// it has already stopped acking, and load balancers must not keep
    /// routing to it. `None` in tests/binaries without fencing wired.
    pub fence_state: Option<Arc<crate::fence::FenceState>>,
    /// Per-replica replication metrics. None in standalone mode.
    pub replication_metrics: Option<Arc<crate::replication::ReplicationMetrics>>,
    /// Per-slot engaged flags from the replication sender (`Release`-flipped
    /// after the slot's gauge pair is seeded). Distinguishes "engaged at
    /// acked 0" (fresh replica — real lag) from "disconnected" (gauges
    /// zeroed — lag 0). `None` in standalone mode or for callers that don't
    /// wire it; per-slot lag then falls back to the `acked == 0` heuristic.
    pub replica_active: Option<[Arc<AtomicBool>; 2]>,
    /// Per-slot replication-ring producer cursors. Paired index-wise with
    /// `replication_ring_consumer_cursors` to compute per-slot ring depth
    /// (producer - consumer). `None` in standalone mode.
    pub replication_ring_producer_cursors: Option<[Arc<dyn QueueCursor>; 2]>,
    /// Per-slot replication-ring consumer progress counters. See above.
    pub replication_ring_consumer_cursors: Option<[Arc<Sequence>; 2]>,
    /// Per-stage busy/idle utilization counters.
    pub journal_utilization: Arc<StageUtilization>,
    pub matching_utilization: Arc<StageUtilization>,
    pub response_utilization: Arc<StageUtilization>,
    /// Control-plane raft election state, updated by the raft driver
    /// thread. `None` when the node runs without control-plane raft
    /// (no `--raft-bind`).
    pub raft: Option<Arc<RaftStatus>>,
}

/// A [`QueueCursor`] that always reads 0 — used for the pipeline-cursor
/// slots of a replica's minimal [`HealthState`], which has no client
/// input ring.
struct ZeroCursor;
impl QueueCursor for ZeroCursor {
    fn load(&self) -> u64 {
        0
    }
}

impl HealthState {
    /// Minimal health state for a **replica** node.
    ///
    /// A replica accepts no client connections and its detailed
    /// replication progress is reported by the primary's per-replica
    /// gauges, so its own `/metrics` exists mainly to expose
    /// control-plane raft election state (`raft`) and node liveness.
    /// The pipeline/journal/replication gauges are intentionally
    /// unpopulated (0); `replicas_connected: Some(0)` makes the node
    /// report `halted` (it is following, not trading), and the fence
    /// state still surfaces a superseded ex-primary. This reuses the one
    /// `/metrics` implementation and exposition format rather than
    /// standing up a second endpoint.
    pub fn for_replica(
        fence_state: Arc<crate::fence::FenceState>,
        raft: Option<Arc<RaftStatus>>,
        pipeline_healthy: Arc<AtomicBool>,
    ) -> Self {
        Self {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            cursors: PipelineCursors::new(
                WireSeq::new(0),
                Arc::new(Sequence::new(AtomicU64::new(0))),
                Arc::new(Sequence::new(AtomicU64::new(0))),
            ),
            input_cursor: Box::new(ZeroCursor),
            pipeline_healthy,
            // `Some(0)` (not `None`) so the trading flag reports
            // `halted` — a replica is not accepting client orders.
            replicas_connected: Some(Arc::new(AtomicU32::new(0))),
            fence_state: Some(fence_state),
            replication_metrics: None,
            replica_active: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft,
        }
    }
}

/// Control-plane raft election state exposed through `/metrics`.
///
/// Plain atomics (not a `Mutex`) so the raft driver publishes after
/// every metrics change and the health thread reads without any
/// coordination — each gauge is independently meaningful, so a torn
/// multi-field read is harmless. Defined here (not in `melin-raft`)
/// because this crate is observability plumbing and must not depend on
/// the consensus crate.
#[derive(Debug)]
pub struct RaftStatus {
    /// This node's raft id (static once configured).
    pub node_id: u64,
    /// Current raft term. Under `--raft-auto-promote` the term doubles as
    /// the fencing-epoch allocator: a promotion journals `epoch = term`,
    /// so an election win and the fencing epoch it mints stay aligned.
    pub term: AtomicU64,
    /// The leader this node currently believes in; 0 while unknown
    /// (mid-election).
    pub leader_id: AtomicU64,
    /// Role encoding: 0 = follower, 1 = learner, 2 = candidate,
    /// 3 = leader (openraft `ServerState` mapping). `u8` — four states,
    /// and the health thread only formats it.
    pub role: std::sync::atomic::AtomicU8,
    /// Whether the raft driver thread is still running. Flipped to
    /// `false` when the driver exits (clean shutdown, or an
    /// unrecoverable storage failure that stops raft while trading
    /// continues). Exposed as `melin_raft_driver_running` so a dead
    /// control plane is visible instead of its gauges freezing at the
    /// last-published (possibly leader) state.
    pub running: AtomicBool,
}

impl RaftStatus {
    /// Role gauge values (kept in sync with the raft driver's mapping
    /// from `openraft::ServerState`).
    pub const ROLE_FOLLOWER: u8 = 0;
    pub const ROLE_LEARNER: u8 = 1;
    pub const ROLE_CANDIDATE: u8 = 2;
    pub const ROLE_LEADER: u8 = 3;

    /// Fresh status for node `node_id`: follower, term 0, no leader,
    /// running.
    pub fn new(node_id: u64) -> Self {
        Self {
            node_id,
            term: AtomicU64::new(0),
            leader_id: AtomicU64::new(0),
            role: std::sync::atomic::AtomicU8::new(Self::ROLE_FOLLOWER),
            running: AtomicBool::new(true),
        }
    }

    /// Mark the driver stopped: clears leadership (role → follower,
    /// leader → none) so `melin_raft_is_leader` cannot stay stuck at 1
    /// on a node whose control plane has died, and drops
    /// `melin_raft_driver_running` to 0. The term is left as-is — its
    /// last value is still meaningful for correlating the outage.
    pub fn mark_stopped(&self) {
        self.role.store(Self::ROLE_FOLLOWER, Ordering::Relaxed);
        self.leader_id.store(0, Ordering::Relaxed);
        self.running.store(false, Ordering::Relaxed);
    }
}

/// Spawn the health endpoint thread. Returns the join handle.
///
/// Binds a TCP listener on `bind_addr` and accepts connections in a loop.
/// Each connection receives a one-line status response and is closed.
/// The thread exits when `shutdown` is set to true.
///
/// `pipeline_healthy` should be set to `true` at startup and flipped to
/// `false` by the accept loop when a pipeline thread dies or on shutdown.
pub fn spawn(
    bind_addr: SocketAddr,
    state: HealthState,
    shutdown: Arc<AtomicBool>,
) -> Result<std::thread::JoinHandle<()>, std::io::Error> {
    let listener = TcpListener::bind(bind_addr)?;
    // Non-blocking so we can check the shutdown flag periodically.
    listener.set_nonblocking(true)?;

    info!(addr = %bind_addr, "health endpoint listening");

    let handle = std::thread::Builder::new()
        .name("health".into())
        .spawn(move || {
            health_loop(&listener, &state, &shutdown);
        })
        .expect("failed to spawn health thread");

    Ok(handle)
}

/// Snapshot of all health metrics — collected once per connection to avoid
/// duplicate atomic reads between the plain-text and Prometheus formatters.
struct HealthSnapshot {
    healthy: bool,
    active_connections: u64,
    events_processed: u64,
    journal_seq: u64,
    replication_lag: u64,
    input_queue_depth: u64,
    trading: bool,
    /// Number of replicas currently connected. 0 in standalone mode.
    replicas_connected: u32,
    /// Per-replica lag: journal_seq - acked_sequence (0 if no ack yet).
    /// Fixed-size array for up to 2 replica slots.
    per_replica_lag: [u64; 2],
    /// Per-replica cumulative bytes sent.
    per_replica_bytes_sent: [u64; 2],
    /// Per-replica ack round-trip latency in microseconds.
    per_replica_ack_latency_us: [u64; 2],
    /// Per-replica cumulative valid-ack count (monotonic across
    /// reconnects). Δacked_sequence / Δacks_received between two
    /// samples gives the mean ack quantum — how many sequences each
    /// cursor advance covers.
    per_replica_acks_received: [u64; 2],
    /// Per-replica catch-up state.
    per_replica_catching_up: [bool; 2],
    /// Per-replica last acked sequence number.
    per_replica_acked_sequence: [u64; 2],
    /// Per-replica last in-memory sequence number (highest seq the
    /// replica has accepted into its input ring, pre-journal). Always
    /// `>= per_replica_acked_sequence` under correct operation —
    /// inversion or equality under sustained traffic indicates a
    /// namespace-translation bug between local-ring positions and
    /// primary sequences.
    per_replica_in_memory_sequence: [u64; 2],
    /// Per-slot replication-ring depth: producer_cursor - consumer.processed.
    /// 0 in standalone mode or when ring cursors aren't available.
    per_replica_ring_depth: [u64; 2],
    /// Fastest-replica cursor: the fastest engaged replica's acked wire
    /// seq, derived from the per-slot cursors at read time. 0 when no
    /// replica has engaged, so the plotted series stays on-scale.
    fastest_replica_cursor: u64,
    /// Total replica eviction count.
    evictions_total: u64,
    /// Total divergent-replica handshake verdicts. Any growth outside
    /// an expected failover rejoin warrants immediate investigation.
    divergence_total: u64,
    /// Per-stage busy/idle iteration counters for utilization monitoring.
    /// Monotonic counters — Prometheus `rate()` gives utilization over any window.
    journal_busy: u64,
    journal_idle: u64,
    matching_busy: u64,
    matching_idle: u64,
    response_busy: u64,
    response_idle: u64,
    /// Response gate-wait events where the journal cursor was the bottleneck.
    response_gate_journal: u64,
    /// Response gate-wait events where the replication cursor was the bottleneck.
    response_gate_replication: u64,
    /// Whether the durability policy was last evaluated as degraded —
    /// at least one clause requires more nodes than are currently
    /// connected, so the response gate stalls until the cluster shape
    /// recovers (or an operator swaps the mode). Trips when a replica
    /// disconnects from a two-node cluster running `persisted>=2`, etc.
    /// Operator alerting should fire on this transitioning to `true`.
    response_policy_degraded: bool,
    /// Cumulative nanoseconds the durability policy has spent degraded.
    /// Emitted as a `_seconds_total` counter (nanos / 1e9) so operators
    /// can `rate()` time-in-degraded over a window — see
    /// `StageUtilization::policy_degraded_nanos`.
    response_policy_degraded_nanos: u64,
    /// Journal rotations that adopted a pre-staged segment (fast path).
    journal_rotations_fast_path: u64,
    /// Journal rotations that fell back to the synchronous allocate
    /// path. Should stay at 0 in steady state on the io_uring path —
    /// growth means rotation stalls are landing on the journal thread.
    journal_rotations_sync_fallback: u64,
    /// Rotation attempts that failed and left the current segment in
    /// place (the journal keeps growing) — any growth is alert-worthy.
    journal_rotations_failed: u64,
    /// Control-plane raft election state; `None` when raft isn't
    /// configured on this node.
    raft: Option<RaftSnapshot>,
}

/// Point-in-time copy of [`RaftStatus`] for the formatters.
struct RaftSnapshot {
    node_id: u64,
    term: u64,
    leader_id: u64,
    role: u8,
    running: bool,
}

impl HealthSnapshot {
    /// Collect a snapshot from the shared atomics.
    fn collect(state: &HealthState) -> Self {
        let healthy = state.pipeline_healthy.load(Ordering::Relaxed);
        let conns = state.active_connections.load(Ordering::Relaxed);
        let evts = state.events_processed.load(Ordering::Relaxed);
        // Highest durably-persisted wire seq — the true "latest durable journal
        // sequence". Read in wire-seq space (not the journal ring cursor) so the
        // gauge survives recovery and queries, and so the lag computations below
        // stay in one space.
        let journal_seq = state.cursors.load_durable_wire_seq();
        let replica_quorum_acked = state.cursors.load_replica_quorum_acked();

        // Input queue depth: producer_cursor - matching_cursor (ring-index
        // space). Matching is the terminal consumer (gated on journal), so this
        // is the total pending items in the input disruptor.
        let producer_seq = RingPos::new(state.input_cursor.load());
        let matching_seq = state.cursors.load_matching_ring();
        let input_queue_depth = producer_seq.saturating_sub(matching_seq);

        // Replication lag in wire-seq space: durable events not yet confirmed
        // by every engaged replica (the slowest engaged replica's deficit).
        // 0 in standalone mode (no replica has engaged).
        let replication_lag = match replica_quorum_acked {
            Some(acked) => journal_seq.saturating_sub(acked),
            None => 0,
        };

        // Trading state: "trading" when standalone or at least one replica
        // connected, "halted" when replication is enabled but all replicas
        // are disconnected — or when the node has been fenced (superseded
        // by a higher-epoch primary). Mirrors the matching stage's
        // `is_halted()` so probes agree with what the engine enforces.
        let fenced = state.fence_state.as_ref().is_some_and(|f| f.is_fenced());
        let trading = !fenced
            && state
                .replicas_connected
                .as_ref()
                .is_none_or(|count| count.load(Ordering::Relaxed) > 0);

        // Per-replica metrics from the replication sender (if enabled).
        let replicas_connected_val = state
            .replicas_connected
            .as_ref()
            .map_or(0, |c| c.load(Ordering::Relaxed));

        type ReplMetricsTuple = (
            [u64; 2],
            [u64; 2],
            [u64; 2],
            [u64; 2],
            [u64; 2],
            [u64; 2],
            [bool; 2],
            u64,
        );
        let (
            per_replica_acked_sequence,
            per_replica_in_memory_sequence,
            per_replica_lag,
            per_replica_bytes_sent,
            per_replica_ack_latency_us,
            per_replica_acks_received,
            per_replica_catching_up,
            evictions_total,
        ): ReplMetricsTuple = if let Some(ref rm) = state.replication_metrics {
            let acked = [
                rm.acked_sequence[0].load(Ordering::Relaxed),
                rm.acked_sequence[1].load(Ordering::Relaxed),
            ];
            let in_memory = [
                rm.in_memory_sequence[0].load(Ordering::Relaxed),
                rm.in_memory_sequence[1].load(Ordering::Relaxed),
            ];
            // `acked` is wire-seq (replica ack metrics), same space as
            // `journal_seq`. Lag is reported only for engaged slots: a
            // replica legitimately engaged at acked 0 (fresh journal) must
            // show its real deficit, while a disengaged slot (whose gauge
            // pair is zeroed on disconnect) reports 0. The per-slot active
            // flags disambiguate; when they aren't wired (older callers,
            // tests), fall back to the historical `acked == 0` heuristic.
            let engaged = |i: usize| {
                state
                    .replica_active
                    .as_ref()
                    // `Acquire` pairs with the sender's `Release` flag flip,
                    // which happens after the gauge pair is seeded.
                    .map(|flags| flags[i].load(Ordering::Acquire))
                    .unwrap_or(acked[i] != 0)
            };
            let lag = [
                if engaged(0) {
                    journal_seq.saturating_sub(WireSeq::new(acked[0]))
                } else {
                    0
                },
                if engaged(1) {
                    journal_seq.saturating_sub(WireSeq::new(acked[1]))
                } else {
                    0
                },
            ];
            let bytes = [
                rm.bytes_sent[0].load(Ordering::Relaxed),
                rm.bytes_sent[1].load(Ordering::Relaxed),
            ];
            let latency = [
                rm.ack_latency_us[0].load(Ordering::Relaxed),
                rm.ack_latency_us[1].load(Ordering::Relaxed),
            ];
            let acks = [
                rm.acks_received[0].load(Ordering::Relaxed),
                rm.acks_received[1].load(Ordering::Relaxed),
            ];
            let catching = [
                rm.catching_up[0].load(Ordering::Relaxed),
                rm.catching_up[1].load(Ordering::Relaxed),
            ];
            let evictions = rm.evictions_total.load(Ordering::Relaxed);
            (
                acked, in_memory, lag, bytes, latency, acks, catching, evictions,
            )
        } else {
            (
                [0, 0],
                [0, 0],
                [0, 0],
                [0, 0],
                [0, 0],
                [0, 0],
                [false, false],
                0,
            )
        };

        // Per-slot replication ring depth: producer_cursor - consumer.processed.
        // Zero when cursors aren't wired (standalone mode). `saturating_sub`
        // tolerates the benign race where the consumer side is read a hair
        // after the producer — never produces underflow.
        let per_replica_ring_depth = match (
            state.replication_ring_producer_cursors.as_ref(),
            state.replication_ring_consumer_cursors.as_ref(),
        ) {
            (Some(prods), Some(cons)) => [
                prods[0]
                    .load()
                    .saturating_sub(cons[0].get().load(Ordering::Relaxed)),
                prods[1]
                    .load()
                    .saturating_sub(cons[1].get().load(Ordering::Relaxed)),
            ],
            _ => [0, 0],
        };

        // Fastest-replica gauge, derived from the per-slot cursors at read
        // time (same wire-seq space as `journal_seq` and the per-slot acked
        // gauges). "No replica engaged" maps to 0 so the plotted series
        // stays on-scale.
        let fastest_replica_cursor = state
            .cursors
            .load_fastest_replica_acked()
            .map_or(0, WireSeq::get);

        Self {
            healthy,
            active_connections: conns,
            events_processed: evts,
            journal_seq: journal_seq.get(),
            replication_lag,
            input_queue_depth,
            trading,
            replicas_connected: replicas_connected_val,
            per_replica_lag,
            per_replica_bytes_sent,
            per_replica_ack_latency_us,
            per_replica_acks_received,
            per_replica_catching_up,
            per_replica_acked_sequence,
            per_replica_in_memory_sequence,
            per_replica_ring_depth,
            fastest_replica_cursor,
            evictions_total,
            divergence_total: state
                .replication_metrics
                .as_ref()
                .map_or(0, |rm| rm.divergence_total.load(Ordering::Relaxed)),
            journal_busy: state.journal_utilization.busy.load(Ordering::Relaxed),
            journal_idle: state.journal_utilization.idle.load(Ordering::Relaxed),
            matching_busy: state.matching_utilization.busy.load(Ordering::Relaxed),
            matching_idle: state.matching_utilization.idle.load(Ordering::Relaxed),
            response_busy: state.response_utilization.busy.load(Ordering::Relaxed),
            response_idle: state.response_utilization.idle.load(Ordering::Relaxed),
            response_gate_journal: state
                .response_utilization
                .gate_journal
                .load(Ordering::Relaxed),
            response_gate_replication: state
                .response_utilization
                .gate_replication
                .load(Ordering::Relaxed),
            response_policy_degraded: state
                .response_utilization
                .policy_degraded
                .load(Ordering::Relaxed),
            response_policy_degraded_nanos: state
                .response_utilization
                .policy_degraded_nanos
                .load(Ordering::Relaxed),
            journal_rotations_fast_path: state
                .journal_utilization
                .rotations_fast_path
                .load(Ordering::Relaxed),
            journal_rotations_sync_fallback: state
                .journal_utilization
                .rotations_sync_fallback
                .load(Ordering::Relaxed),
            journal_rotations_failed: state
                .journal_utilization
                .rotations_failed
                .load(Ordering::Relaxed),
            raft: state.raft.as_ref().map(|r| RaftSnapshot {
                node_id: r.node_id,
                term: r.term.load(Ordering::Relaxed),
                leader_id: r.leader_id.load(Ordering::Relaxed),
                role: r.role.load(Ordering::Relaxed),
                running: r.running.load(Ordering::Relaxed),
            }),
        }
    }

    /// Write the one-line status into `buf`. Returns bytes written.
    fn write_status_line(&self, buf: &mut [u8]) -> usize {
        let status = if self.healthy { "OK" } else { "ERR" };
        let trading = if self.trading { "trading" } else { "halted" };
        let mut c = Cursor::new(buf);
        let _ = writeln!(
            c,
            "{status} {} {} {} {trading}",
            self.active_connections, self.journal_seq, self.replication_lag
        );
        c.position() as usize
    }

    /// Write the Prometheus text exposition body into `buf`. Returns bytes written.
    fn write_prometheus(&self, buf: &mut [u8]) -> usize {
        let healthy_val: u8 = if self.healthy { 1 } else { 0 };
        let trading_val: u8 = if self.trading { 1 } else { 0 };
        let catching_0: u8 = if self.per_replica_catching_up[0] {
            1
        } else {
            0
        };
        let catching_1: u8 = if self.per_replica_catching_up[1] {
            1
        } else {
            0
        };
        let mut c = Cursor::new(buf);
        let _ = write!(
            c,
            "# HELP melin_active_connections Current authenticated client connections.\n\
             # TYPE melin_active_connections gauge\n\
             melin_active_connections {}\n\
             # HELP melin_events_processed Total events processed by the matching engine.\n\
             # TYPE melin_events_processed counter\n\
             melin_events_processed {}\n\
             # HELP melin_journal_sequence Latest durable journal sequence number.\n\
             # TYPE melin_journal_sequence counter\n\
             melin_journal_sequence {}\n\
             # HELP melin_replication_lag Durable events not yet confirmed by every engaged replica (0 when none engaged).\n\
             # TYPE melin_replication_lag gauge\n\
             melin_replication_lag {}\n\
             # HELP melin_pipeline_healthy Whether the pipeline is healthy (1) or degraded (0).\n\
             # TYPE melin_pipeline_healthy gauge\n\
             melin_pipeline_healthy {}\n\
             # HELP melin_input_queue_depth Items pending in the input disruptor.\n\
             # TYPE melin_input_queue_depth gauge\n\
             melin_input_queue_depth {}\n\
             # HELP melin_input_queue_capacity Total input ring buffer capacity.\n\
             # TYPE melin_input_queue_capacity gauge\n\
             melin_input_queue_capacity {}\n\
             # HELP melin_trading_active Whether the engine is accepting orders (1) or halted (0).\n\
             # TYPE melin_trading_active gauge\n\
             melin_trading_active {}\n\
             # HELP melin_replicas_connected Number of replicas currently connected.\n\
             # TYPE melin_replicas_connected gauge\n\
             melin_replicas_connected {}\n\
             # HELP melin_replica_acked_sequence Last sequence acked by each replica slot (persisted to journal).\n\
             # TYPE melin_replica_acked_sequence gauge\n\
             melin_replica_acked_sequence{{slot=\"0\"}} {}\n\
             melin_replica_acked_sequence{{slot=\"1\"}} {}\n\
             # HELP melin_replica_in_memory_sequence Last sequence the replica has accepted into its input ring (pre-journal).\n\
             # TYPE melin_replica_in_memory_sequence gauge\n\
             melin_replica_in_memory_sequence{{slot=\"0\"}} {}\n\
             melin_replica_in_memory_sequence{{slot=\"1\"}} {}\n\
             # HELP melin_replica_lag Per-replica replication lag (journal_seq - acked_sequence).\n\
             # TYPE melin_replica_lag gauge\n\
             melin_replica_lag{{slot=\"0\"}} {}\n\
             melin_replica_lag{{slot=\"1\"}} {}\n\
             # HELP melin_replica_bytes_sent_total Cumulative bytes sent to each replica.\n\
             # TYPE melin_replica_bytes_sent_total counter\n\
             melin_replica_bytes_sent_total{{slot=\"0\"}} {}\n\
             melin_replica_bytes_sent_total{{slot=\"1\"}} {}\n\
             # HELP melin_replica_ack_latency_us Ack round-trip latency per replica in microseconds.\n\
             # TYPE melin_replica_ack_latency_us gauge\n\
             melin_replica_ack_latency_us{{slot=\"0\"}} {}\n\
             melin_replica_ack_latency_us{{slot=\"1\"}} {}\n\
             # HELP melin_replica_acks_received_total Cumulative valid ack frames recorded per replica slot.\n\
             # TYPE melin_replica_acks_received_total counter\n\
             melin_replica_acks_received_total{{slot=\"0\"}} {}\n\
             melin_replica_acks_received_total{{slot=\"1\"}} {}\n\
             # HELP melin_replica_catching_up Whether each replica is catching up from journal (1) or live (0).\n\
             # TYPE melin_replica_catching_up gauge\n\
             melin_replica_catching_up{{slot=\"0\"}} {}\n\
             melin_replica_catching_up{{slot=\"1\"}} {}\n\
             # HELP melin_replica_evictions_total Total replica evictions due to ring backpressure.\n\
             # TYPE melin_replica_evictions_total counter\n\
             melin_replica_evictions_total {}\n\
             # HELP melin_replica_divergence_total Divergent replica handshakes (journal chain failed validation; replica routed through archive + re-seed). Growth outside an expected failover rejoin warrants immediate investigation.\n\
             # TYPE melin_replica_divergence_total counter\n\
             melin_replica_divergence_total {}\n\
             # HELP melin_replication_ring_depth Per-slot replication-ring depth (producer_cursor - consumer.processed).\n\
             # TYPE melin_replication_ring_depth gauge\n\
             melin_replication_ring_depth{{slot=\"0\"}} {}\n\
             melin_replication_ring_depth{{slot=\"1\"}} {}\n\
             # HELP melin_fastest_replica_cursor Highest acked sequence across replica slots (0 when none engaged).\n\
             # TYPE melin_fastest_replica_cursor gauge\n\
             melin_fastest_replica_cursor {}\n\
             # HELP melin_stage_busy_total Cumulative busy iterations per pipeline stage (journal/response: batches, matching: events).\n\
             # TYPE melin_stage_busy_total counter\n\
             melin_stage_busy_total{{stage=\"journal\"}} {}\n\
             melin_stage_busy_total{{stage=\"matching\"}} {}\n\
             melin_stage_busy_total{{stage=\"response\"}} {}\n\
             # HELP melin_stage_idle_total Cumulative idle iterations per pipeline stage.\n\
             # TYPE melin_stage_idle_total counter\n\
             melin_stage_idle_total{{stage=\"journal\"}} {}\n\
             melin_stage_idle_total{{stage=\"matching\"}} {}\n\
             melin_stage_idle_total{{stage=\"response\"}} {}\n\
             # HELP melin_response_gate_total Gate opens by which node supplied the binding cursor of the configured durability policy (journal = the local primary, replication = a replica). While the cluster shape cannot satisfy the policy the gate does not open and neither label moves (see melin_durability_policy_degraded).\n\
             # TYPE melin_response_gate_total counter\n\
             melin_response_gate_total{{blocker=\"journal\"}} {}\n\
             melin_response_gate_total{{blocker=\"replication\"}} {}\n\
             # HELP melin_journal_rotations_total Journal segment rotation attempts by outcome (fast = adopted a pre-staged segment; sync_fallback = synchronous allocate on the journal thread; failed = rotation failed, current segment kept growing).\n\
             # TYPE melin_journal_rotations_total counter\n\
             melin_journal_rotations_total{{path=\"fast\"}} {}\n\
             melin_journal_rotations_total{{path=\"sync_fallback\"}} {}\n\
             melin_journal_rotations_total{{path=\"failed\"}} {}\n\
             # HELP melin_durability_policy_degraded Durability policy currently unsatisfiable by the connected cluster shape; the response gate stalls while set (1 = degraded, 0 = healthy).\n\
             # TYPE melin_durability_policy_degraded gauge\n\
             melin_durability_policy_degraded {}\n\
             # HELP melin_durability_policy_degraded_seconds_total Cumulative seconds the durability policy has spent unsatisfiable by the connected cluster shape.\n\
             # TYPE melin_durability_policy_degraded_seconds_total counter\n\
             melin_durability_policy_degraded_seconds_total {:.6}\n",
            self.active_connections,
            self.events_processed,
            self.journal_seq,
            self.replication_lag,
            healthy_val,
            self.input_queue_depth,
            INPUT_RING_CAPACITY,
            trading_val,
            self.replicas_connected,
            self.per_replica_acked_sequence[0],
            self.per_replica_acked_sequence[1],
            self.per_replica_in_memory_sequence[0],
            self.per_replica_in_memory_sequence[1],
            self.per_replica_lag[0],
            self.per_replica_lag[1],
            self.per_replica_bytes_sent[0],
            self.per_replica_bytes_sent[1],
            self.per_replica_ack_latency_us[0],
            self.per_replica_ack_latency_us[1],
            self.per_replica_acks_received[0],
            self.per_replica_acks_received[1],
            catching_0,
            catching_1,
            self.evictions_total,
            self.divergence_total,
            self.per_replica_ring_depth[0],
            self.per_replica_ring_depth[1],
            self.fastest_replica_cursor,
            self.journal_busy,
            self.matching_busy,
            self.response_busy,
            self.journal_idle,
            self.matching_idle,
            self.response_idle,
            self.response_gate_journal,
            self.response_gate_replication,
            self.journal_rotations_fast_path,
            self.journal_rotations_sync_fallback,
            self.journal_rotations_failed,
            if self.response_policy_degraded { 1 } else { 0 },
            self.response_policy_degraded_nanos as f64 / 1e9,
        );
        // Raft gauges only exist on raft-enabled nodes — omitting the
        // series entirely (rather than exporting zeros) keeps dashboards
        // from suggesting a one-node "cluster" on standalone deployments.
        if let Some(raft) = &self.raft {
            // Best-effort like the main block above: a write error only
            // means the fixed metrics buffer filled, which truncates the
            // exposition rather than being actionable here.
            let _ = write!(
                c,
                "# HELP melin_raft_node_id This node's control-plane raft id.\n\
                 # TYPE melin_raft_node_id gauge\n\
                 melin_raft_node_id {}\n\
                 # HELP melin_raft_term Current raft election term; under auto-promotion a promotion journals this as its fencing epoch.\n\
                 # TYPE melin_raft_term gauge\n\
                 melin_raft_term {}\n\
                 # HELP melin_raft_leader_id Node id of the current raft leader (0 while unknown).\n\
                 # TYPE melin_raft_leader_id gauge\n\
                 melin_raft_leader_id {}\n\
                 # HELP melin_raft_role This node's raft role (0 follower, 1 learner, 2 candidate, 3 leader).\n\
                 # TYPE melin_raft_role gauge\n\
                 melin_raft_role {}\n\
                 # HELP melin_raft_is_leader Whether this node currently leads the control plane (1) or not (0).\n\
                 # TYPE melin_raft_is_leader gauge\n\
                 melin_raft_is_leader {}\n\
                 # HELP melin_raft_driver_running Whether the raft driver thread is alive (1) or has stopped, e.g. on an unrecoverable state-file error while trading continues (0).\n\
                 # TYPE melin_raft_driver_running gauge\n\
                 melin_raft_driver_running {}\n",
                raft.node_id,
                raft.term,
                raft.leader_id,
                raft.role,
                // A stopped driver never leads, regardless of the last
                // role it published.
                u8::from(raft.running && raft.role == RaftStatus::ROLE_LEADER),
                u8::from(raft.running),
            );
        }
        c.position() as usize
    }
}

/// What kind of request the client sent.
enum RequestKind {
    /// No data within timeout — plain TCP probe (e.g., `nc`, Kubernetes TCP check).
    PlainTcp,
    /// HTTP GET / — serve the one-line status wrapped in HTTP.
    HttpHealth,
    /// HTTP GET /metrics — serve Prometheus text exposition format.
    Metrics,
    /// HTTP GET /stats-dump — serve the bench's tick-to-trade per-stage
    /// histogram dump from the latency-trace registry. Empty body when
    /// the server was built without `--features latency-trace`.
    StatsDump,
}

/// Peek at the first bytes to detect HTTP vs plain TCP.
///
/// Strategy: try a non-blocking read first. If data is already buffered
/// (HTTP client sent request before we accepted), we classify immediately
/// with zero delay. Only if the non-blocking read returns WouldBlock do
/// we fall back to a short blocking read — 5ms is enough for loopback
/// HTTP headers to arrive, and keeps plain TCP probes fast (~5ms worst
/// case instead of the old 50ms).
fn detect_request(stream: &mut TcpStream) -> RequestKind {
    // 16 bytes is enough to distinguish "GET /m" from "GET /" from nothing.
    let mut buf = [0u8; 16];

    // First try: non-blocking. Data is usually already in the kernel
    // buffer by the time we accept() the connection.
    let _ = stream.set_nonblocking(true);
    let n = match stream.read(&mut buf) {
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // No data yet — fall back to a short blocking wait.
            // 5ms is generous for loopback; plain TCP probes (nc, k8s)
            // never send data, so this is their worst-case delay.
            let _ = stream.set_nonblocking(false);
            let _ = stream.set_read_timeout(Some(Duration::from_millis(5)));
            match stream.read(&mut buf) {
                Ok(n) => n,
                Err(_) => return RequestKind::PlainTcp,
            }
        }
        Err(_) => return RequestKind::PlainTcp,
    };

    let data = &buf[..n];
    // Prefix matches use 6 bytes (`GET /` + 1 path byte) so that a
    // short non-blocking read still classifies correctly. `/m` and
    // `/s` are the only documented two paths beyond `/`; an
    // undocumented path beginning with `m` or `s` would be
    // misclassified, but no other paths are exposed.
    let kind = if data.starts_with(b"GET /m") {
        RequestKind::Metrics
    } else if data.starts_with(b"GET /s") {
        RequestKind::StatsDump
    } else if data.starts_with(b"GET /") {
        RequestKind::HttpHealth
    } else {
        return RequestKind::PlainTcp;
    };

    // Drain remaining HTTP request data so close() doesn't RST the connection.
    // HTTP clients send headers beyond our 16-byte peek; leaving unread data
    // in the recv buffer causes the kernel to send RST instead of FIN.
    // Cap at 4 KiB to prevent a malicious client from holding the health thread.
    let mut discard = [0u8; 512];
    let mut drained = 0usize;
    while drained < 4096 {
        match stream.read(&mut discard) {
            Ok(0) | Err(_) => break,
            Ok(n) => drained += n,
        }
    }

    kind
}

/// Write the latency-trace stage histograms into `buf` as one
/// tab-separated record per registered stage. Returns bytes written.
///
/// Stages that recorded nothing are emitted with `samples` = 0 rather
/// than omitted, so a stage that was quiet is distinguishable from one
/// that was never compiled in.
///
/// If the stage inventory ever outgrows `buf`, the body ends with
/// `# truncated\t<count>` rather than a half-written stage line — the
/// dump degrades visibly instead of silently.
///
/// Format (one line per stage, '\n'-terminated):
///
///   stage\t<name>\t<samples>\t<min_ns>\t<p50_ns>\t<p90_ns>\t<p99_ns>\t<p99_9_ns>\t<max_ns>
///
/// Tab as the field delimiter so stage names containing spaces / colons
/// / parens parse unambiguously. The bench (phase 3) parses this and
/// merges with its own RTT histograms for the tick-to-trade table.
///
/// When `latency-trace` is disabled the body is a single comment line
/// so the bench can detect the unsupported state without a different
/// HTTP status code.
fn write_stats_dump(buf: &mut [u8]) -> usize {
    let mut c = Cursor::new(buf);

    #[cfg(feature = "latency-trace")]
    {
        // Room held back so a truncation marker always fits. Every
        // stage is emitted now (including zero-sample ones), so the
        // body grows with the stage count rather than with how many
        // stages happened to record — worth saying so out loud instead
        // of handing the bench a half-written line.
        const TRUNCATION_RESERVE: usize = 64;
        let limit = c.get_ref().len().saturating_sub(TRUNCATION_RESERVE);

        let snaps = crate::trace::global_registry().snapshot_all();
        if snaps.is_empty() {
            // Feature on but no stage has registered yet — explicit
            // marker so the bench doesn't confuse it with a feature-off
            // server. Reached only before the pipeline threads start;
            // once they have, a quiet stage shows up as a zero-sample
            // row instead.
            let _ = writeln!(c, "# no samples");
        } else {
            let total = snaps.len();
            let mut written = 0usize;
            for s in snaps {
                // Format first so the length is known before committing
                // to the buffer; a `write!` straight to the cursor can
                // stop mid-line. Allocation is fine here — this runs
                // once per /stats-dump request, not per event.
                let line = format!(
                    "stage\t{name}\t{samples}\t{min}\t{p50}\t{p90}\t{p99}\t{p99_9}\t{max}\n",
                    name = s.name,
                    samples = s.samples,
                    min = s.min_ns,
                    p50 = s.p50_ns,
                    p90 = s.p90_ns,
                    p99 = s.p99_ns,
                    p99_9 = s.p99_9_ns,
                    max = s.max_ns,
                );
                if c.position() as usize + line.len() > limit {
                    break;
                }
                // Cannot fail: the bounds check above guarantees room.
                let _ = c.write_all(line.as_bytes());
                written += 1;
            }
            if written < total {
                // Best-effort diagnostic; the reserve guarantees room.
                let _ = writeln!(c, "# truncated\t{}", total - written);
            }
        }
    }
    #[cfg(not(feature = "latency-trace"))]
    {
        let _ = writeln!(c, "# latency-trace disabled");
    }

    c.position() as usize
}

/// Write HTTP header + body into `buf`. Returns total bytes written.
fn write_http(buf: &mut [u8], content_type: &str, body: &[u8]) -> usize {
    let mut c = Cursor::new(buf);
    let _ = write!(
        c,
        "HTTP/1.1 200 OK\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    let _ = c.write_all(body);
    c.position() as usize
}

/// Main health endpoint loop. Accepts connections and writes status.
fn health_loop(listener: &TcpListener, state: &HealthState, shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, addr)) => {
                debug!(addr = %addr, "health check");
                handle_health_connection(stream, state);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No pending connection — sleep briefly then retry.
                // 100ms is fine for health checks (they're infrequent).
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                error!(error = %e, "health accept error");
            }
        }
    }
}

/// Collect snapshot, detect request kind, write the appropriate response.
/// Best-effort — errors are debug-logged but don't affect the server.
///
/// Zero heap allocations — all formatting uses stack buffers.
fn handle_health_connection(mut stream: TcpStream, state: &HealthState) {
    // Short write timeout — health probes should not block the thread.
    let _ = stream.set_write_timeout(Some(Duration::from_secs(1)));

    let snapshot = HealthSnapshot::collect(state);

    let kind = detect_request(&mut stream);

    // Stack buffers — sized for the largest body we serve.
    // - Prometheus body is ~3.5 KiB with max-length u64 values
    //   (includes per-replica replication metrics, ring depth, and
    //   the fastest-replica cursor).
    // - StatsDump body is ~260 bytes per registered stage; current
    //   set is 9–13 stages (transport-dependent) for ~3.5 KiB tops.
    //   8 KiB gives headroom for future stages without resizing.
    // Response = body + HTTP headers (~200 bytes).
    let mut body_buf = [0u8; 8192];
    let mut resp_buf = [0u8; 8448];

    let resp_len = match kind {
        RequestKind::Metrics => {
            let body_len = snapshot.write_prometheus(&mut body_buf);
            write_http(
                &mut resp_buf,
                "text/plain; version=0.0.4; charset=utf-8",
                &body_buf[..body_len],
            )
        }
        RequestKind::HttpHealth => {
            let body_len = snapshot.write_status_line(&mut body_buf);
            write_http(
                &mut resp_buf,
                "text/plain; charset=utf-8",
                &body_buf[..body_len],
            )
        }
        RequestKind::StatsDump => {
            let body_len = write_stats_dump(&mut body_buf);
            write_http(
                &mut resp_buf,
                "text/tab-separated-values; charset=utf-8",
                &body_buf[..body_len],
            )
        }
        RequestKind::PlainTcp => snapshot.write_status_line(&mut resp_buf),
    };

    if let Err(e) = stream.write_all(&resp_buf[..resp_len]) {
        debug!(error = %e, "health write failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cursors::SlotAcked;
    use crate::replication::ReplicationMetrics;
    use std::io::Read;

    #[test]
    fn raft_status_mark_stopped_clears_leadership() {
        let status = RaftStatus::new(7);
        status
            .role
            .store(RaftStatus::ROLE_LEADER, Ordering::Relaxed);
        status.leader_id.store(7, Ordering::Relaxed);
        status.term.store(4, Ordering::Relaxed);

        status.mark_stopped();

        assert_eq!(
            status.role.load(Ordering::Relaxed),
            RaftStatus::ROLE_FOLLOWER
        );
        assert_eq!(status.leader_id.load(Ordering::Relaxed), 0);
        assert!(!status.running.load(Ordering::Relaxed));
        // Term is deliberately retained for outage correlation.
        assert_eq!(status.term.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn replica_health_endpoint_serves_raft_gauges_and_reports_halted() {
        // A replica's minimal endpoint must expose the election gauges
        // (the point of having it) and report `halted` (it serves no
        // client orders).
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let raft = Arc::new(RaftStatus::new(2));
        raft.role.store(RaftStatus::ROLE_LEADER, Ordering::Relaxed);
        raft.leader_id.store(2, Ordering::Relaxed);
        raft.term.store(7, Ordering::Relaxed);
        let fence = Arc::new(crate::fence::FenceState::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let handle = spawn(
            addr,
            HealthState::for_replica(fence, Some(raft), Arc::new(AtomicBool::new(true))),
            Arc::clone(&shutdown),
        )
        .unwrap();
        // Give the listener a moment to come up.
        std::thread::sleep(Duration::from_millis(100));

        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(body.contains("melin_raft_node_id 2\n"), "{body}");
        assert!(body.contains("melin_raft_term 7\n"), "{body}");
        assert!(body.contains("melin_raft_is_leader 1\n"), "{body}");
        assert!(body.contains("melin_raft_driver_running 1\n"), "{body}");
        // A replica is following, not accepting client orders.
        assert!(body.contains("melin_trading_active 0\n"), "{body}");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn stopped_driver_clears_is_leader_gauge() {
        // A dead control plane must not keep advertising leadership.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let raft = Arc::new(RaftStatus::new(3));
        raft.role.store(RaftStatus::ROLE_LEADER, Ordering::Relaxed);
        raft.leader_id.store(3, Ordering::Relaxed);
        raft.mark_stopped();
        let fence = Arc::new(crate::fence::FenceState::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));

        let handle = spawn(
            addr,
            HealthState::for_replica(fence, Some(raft), Arc::new(AtomicBool::new(true))),
            Arc::clone(&shutdown),
        )
        .unwrap();
        std::thread::sleep(Duration::from_millis(100));

        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(body.contains("melin_raft_is_leader 0\n"), "{body}");
        assert!(body.contains("melin_raft_driver_running 0\n"), "{body}");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn raft_gauges_absent_without_raft() {
        // Standalone/non-raft nodes must not export the series at all.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(!body.contains("melin_raft_"), "{body}");
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    /// Test-only QueueCursor backed by an AtomicU64.
    struct MockCursor(AtomicU64);
    impl QueueCursor for MockCursor {
        fn load(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    /// Build test cursors with explicit seeds for each space. `repl_acked`
    /// is the engaged replica's *acked wire seq*, stored into slot 0 the
    /// way `ReplicaCursors` does, or `u64::MAX` for "no replica engaged".
    fn test_cursors(
        durable: u64,
        journal_ring: u64,
        matching_ring: u64,
        repl_acked: u64,
    ) -> PipelineCursors {
        let cursors = PipelineCursors::new(
            WireSeq::new(durable),
            Arc::new(Sequence::new(AtomicU64::new(journal_ring))),
            Arc::new(Sequence::new(AtomicU64::new(matching_ring))),
        );
        // `u64::MAX` means "no replica engaged" — leave every slot parked.
        if repl_acked != u64::MAX {
            cursors
                .replica_slot_cursors()
                .store(0, SlotAcked::from_acked(WireSeq::new(repl_acked)));
        }
        cursors
    }

    /// Helper: create a non-blocking listener and spawn the health loop.
    /// Returns (addr, events_processed, pipeline_healthy, shutdown_flag, join_handle).
    /// `replicas_connected` is None (standalone mode) unless overridden.
    fn start_health(
        active: u64,
        journal_seq: u64,
        repl_acked: u64,
    ) -> (
        SocketAddr,
        Arc<AtomicU64>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<()>,
    ) {
        start_health_with_replica(active, journal_seq, repl_acked, None, None)
    }

    /// Like `start_health` but with explicit `replicas_connected` and
    /// `fence_state` wiring. `repl_acked` is the slowest engaged replica's
    /// acked wire seq, or `u64::MAX` for "no replica engaged" — see
    /// [`test_cursors`].
    fn start_health_with_replica(
        active: u64,
        journal_seq: u64,
        repl_acked: u64,
        replicas_connected: Option<Arc<AtomicU32>>,
        fence_state: Option<Arc<crate::fence::FenceState>>,
    ) -> (
        SocketAddr,
        Arc<AtomicU64>,
        Arc<AtomicBool>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let active = Arc::new(AtomicU64::new(active));
        let events = Arc::new(AtomicU64::new(0));
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(AtomicBool::new(false));

        let s = Arc::clone(&shutdown);
        let state = HealthState {
            active_connections: active,
            events_processed: Arc::clone(&events),
            // The gauge reads `durable_wire_seq`; the ring cursors only feed
            // queue depth. Seed all three from `journal_seq` so "fully caught
            // up, empty queue" holds for most tests (depth = input − matching
            // = 0).
            cursors: test_cursors(journal_seq, journal_seq, journal_seq, repl_acked),
            // Input cursor = journal_seq (empty queue) for most tests.
            input_cursor: Box::new(MockCursor(AtomicU64::new(journal_seq))),
            pipeline_healthy: Arc::clone(&healthy),
            replicas_connected,
            fence_state,
            replication_metrics: None,
            replica_active: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        (addr, events, healthy, shutdown, handle)
    }

    /// Read the full response from a health connection (plain TCP, no request sent).
    fn read_health(addr: SocketAddr) -> String {
        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut buf = String::new();
        client.read_to_string(&mut buf).unwrap();
        buf
    }

    /// Send an HTTP request and read the full response.
    fn http_request(addr: SocketAddr, request: &str) -> String {
        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client.write_all(request.as_bytes()).unwrap();
        // Shut down write side so the server's drain sees EOF immediately
        // instead of blocking until the 50ms read timeout expires.
        client.shutdown(std::net::Shutdown::Write).unwrap();
        let mut buf = String::new();
        client.read_to_string(&mut buf).unwrap();
        buf
    }

    #[test]
    fn plain_tcp_backward_compatible() {
        // Connect without sending any data → raw one-line status (no HTTP headers).
        let (addr, _events, _healthy, shutdown, handle) = start_health(5, 42, 40);

        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 42 2 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_standalone_replication_lag_is_zero() {
        // Standalone mode: replication cursor is u64::MAX → lag = 0.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 100, u64::MAX);

        let buf = read_health(addr);
        assert_eq!(buf, "OK 0 100 0 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_multiple_connections() {
        let (addr, _events, _healthy, shutdown, handle) = start_health(10, 0, u64::MAX);

        // Multiple sequential health checks should all succeed.
        for _ in 0..3 {
            let buf = read_health(addr);
            assert!(buf.starts_with("OK "), "unexpected response: {buf}");
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_returns_err_when_pipeline_unhealthy() {
        let (addr, _events, healthy, shutdown, handle) = start_health(3, 50, u64::MAX);

        // Healthy pipeline returns OK.
        let buf = read_health(addr);
        assert!(buf.starts_with("OK "), "expected OK, got: {buf}");

        // Mark pipeline unhealthy (simulates thread panic detection).
        healthy.store(false, Ordering::Relaxed);

        let buf = read_health(addr);
        assert_eq!(buf, "ERR 3 50 0 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_shutdown_stops_loop() {
        let (_addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        // Signal shutdown — thread should exit within ~200ms.
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn spawn_end_to_end() {
        // Test the public `spawn` API (bind + thread + accept + respond).
        let active = Arc::new(AtomicU64::new(7));
        let events = Arc::new(AtomicU64::new(0));
        let healthy = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::new(AtomicBool::new(false));

        let handle = spawn(
            "127.0.0.1:0".parse().unwrap(),
            HealthState {
                active_connections: Arc::clone(&active),
                events_processed: Arc::clone(&events),
                cursors: test_cursors(99, 99, 99, u64::MAX),
                input_cursor: Box::new(MockCursor(AtomicU64::new(99))),
                pipeline_healthy: Arc::clone(&healthy),
                replicas_connected: None,
                fence_state: None,
                replication_metrics: None,
                replica_active: None,
                replication_ring_producer_cursors: None,
                replication_ring_consumer_cursors: None,
                journal_utilization: Arc::new(StageUtilization::new()),
                matching_utilization: Arc::new(StageUtilization::new()),
                response_utilization: Arc::new(StageUtilization::new()),
                raft: None,
            },
            Arc::clone(&shutdown),
        );
        // spawn binds to port 0 which is auto-assigned — we can't know the
        // port, so this test just verifies it doesn't panic or error.
        // For a full round-trip, use start_health (which gives us the addr).
        assert!(handle.is_ok());
        shutdown.store(true, Ordering::Relaxed);
        handle.unwrap().join().unwrap();
    }

    #[test]
    fn spawn_bind_failure_returns_error() {
        // Bind to the same port twice — second should fail.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let result = spawn(
            addr,
            HealthState {
                active_connections: Arc::new(AtomicU64::new(0)),
                events_processed: Arc::new(AtomicU64::new(0)),
                cursors: test_cursors(0, 0, 0, u64::MAX),
                input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
                pipeline_healthy: Arc::new(AtomicBool::new(true)),
                replicas_connected: None,
                fence_state: None,
                replication_metrics: None,
                replica_active: None,
                replication_ring_producer_cursors: None,
                replication_ring_consumer_cursors: None,
                journal_utilization: Arc::new(StageUtilization::new()),
                matching_utilization: Arc::new(StageUtilization::new()),
                response_utilization: Arc::new(StageUtilization::new()),
                raft: None,
            },
            Arc::new(AtomicBool::new(false)),
        );
        assert!(result.is_err(), "expected bind failure on occupied port");
        drop(listener);
    }

    #[test]
    fn client_disconnect_before_reading() {
        // TCP connect-only probe: connect and immediately drop (no read).
        // The health loop should handle the broken pipe gracefully.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        for _ in 0..3 {
            let client = TcpStream::connect(addr).unwrap();
            drop(client); // immediate disconnect
        }

        // Health loop should still be alive and serving.
        let buf = read_health(addr);
        assert!(
            buf.starts_with("OK "),
            "expected OK after disconnects, got: {buf}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn concurrent_health_checks() {
        let (addr, _events, _healthy, shutdown, handle) = start_health(2, 77, u64::MAX);

        // Spawn 5 concurrent clients.
        let threads: Vec<_> = (0..5)
            .map(|_| {
                let a = addr;
                std::thread::spawn(move || read_health(a))
            })
            .collect();

        for t in threads {
            let buf = t.join().unwrap();
            assert!(buf.starts_with("OK "), "unexpected: {buf}");
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn health_shows_halted_when_replica_disconnected() {
        let replica_count = Arc::new(AtomicU32::new(0)); // no replicas connected
        let (addr, _events, _healthy, shutdown, handle) =
            start_health_with_replica(5, 100, u64::MAX, Some(Arc::clone(&replica_count)), None);

        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 100 0 halted\n");

        // Connect a replica — should switch to trading.
        replica_count.store(1, Ordering::Relaxed);
        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 100 0 trading\n");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    /// A fenced (superseded) ex-primary must report `halted` even while a
    /// replica is still connected — the replica count is healthy in the
    /// exact split-brain scenario fencing exists for, so probes must key
    /// off the fence latch, not just the count.
    #[test]
    fn health_shows_halted_when_fenced() {
        let replica_count = Arc::new(AtomicU32::new(1)); // replica still connected
        let fence = Arc::new(crate::fence::FenceState::new(0));
        let (addr, _events, _healthy, shutdown, handle) = start_health_with_replica(
            5,
            100,
            u64::MAX,
            Some(Arc::clone(&replica_count)),
            Some(Arc::clone(&fence)),
        );

        let buf = read_health(addr);
        assert_eq!(buf, "OK 5 100 0 trading\n", "healthy node trades");

        fence.fence();
        let buf = read_health(addr);
        assert_eq!(
            buf, "OK 5 100 0 halted\n",
            "fenced node must report halted despite a connected replica"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_response_format() {
        let (addr, events, _healthy, shutdown, handle) = start_health(5, 42, 40);
        events.store(1000, Ordering::Relaxed);

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");

        // Verify HTTP response structure.
        assert!(
            response.starts_with("HTTP/1.1 200 OK\r\n"),
            "expected HTTP 200, got: {response}"
        );
        assert!(
            response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"),
            "missing prometheus content type"
        );

        // Verify all 8 metric lines.
        assert!(response.contains("melin_active_connections 5\n"));
        assert!(response.contains("melin_events_processed 1000\n"));
        assert!(response.contains("melin_journal_sequence 42\n"));
        assert!(response.contains("melin_replication_lag 2\n"));
        assert!(response.contains("melin_pipeline_healthy 1\n"));
        assert!(response.contains("melin_input_queue_depth 0\n"));
        assert!(response.contains("melin_input_queue_capacity 1048576\n"));
        assert!(response.contains("melin_trading_active 1\n"));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    /// Regression: the `journal_sequence` gauge must report the durable
    /// wire-seq, not the journal ring cursor. These live in different spaces
    /// (the ring cursor resets to ~0 each process start and counts queries),
    /// so a recovered node would otherwise report a tiny value. We pin the
    /// distinction by giving the two cursors deliberately different values.
    #[test]
    fn journal_sequence_gauge_reads_durable_wire_seq_not_ring() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        // Durable wire seq = 1_000_000 (post-recovery high-water), but the
        // journal ring cursor is only 3 (fresh process, few slots drained).
        // Matching ring = 1 so input queue depth = input(5) − matching(1) = 4.
        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            cursors: test_cursors(1_000_000, 3, 1, u64::MAX),
            input_cursor: Box::new(MockCursor(AtomicU64::new(5))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            fence_state: None,
            replication_metrics: None,
            replica_active: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        // Gauge tracks the durable wire seq, not the ring cursor (3).
        assert!(
            response.contains("melin_journal_sequence 1000000\n"),
            "gauge should read durable wire seq, got: {response}"
        );
        // Queue depth still comes from the ring cursors (5 − 1 = 4).
        assert!(
            response.contains("melin_input_queue_depth 4\n"),
            "queue depth should use the ring cursors, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_boolean_encoding() {
        // Verify that unhealthy + halted → 0 values.
        let replica_count = Arc::new(AtomicU32::new(0)); // disconnected → halted
        let (addr, _events, healthy, shutdown, handle) =
            start_health_with_replica(0, 0, u64::MAX, Some(Arc::clone(&replica_count)), None);

        healthy.store(false, Ordering::Relaxed);

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(response.contains("melin_pipeline_healthy 0\n"));
        assert!(response.contains("melin_trading_active 0\n"));

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn http_health_response() {
        let (addr, _events, _healthy, shutdown, handle) = start_health(5, 42, 40);

        let response = http_request(addr, "GET / HTTP/1.1\r\n\r\n");

        assert!(
            response.starts_with("HTTP/1.1 200 OK\r\n"),
            "expected HTTP 200, got: {response}"
        );
        assert!(
            response.contains("Content-Type: text/plain; charset=utf-8"),
            "missing content type"
        );
        assert!(
            response.contains("OK 5 42 2 trading\n"),
            "missing status line in body: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn events_processed_in_metrics() {
        let (addr, events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        events.store(999_999, Ordering::Relaxed);

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_events_processed 999999\n"),
            "events_processed not found in: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn input_queue_depth_in_metrics() {
        // Set up with producer at 1000, matching at 900 → depth = 100.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            cursors: test_cursors(1000, 1000, 900, u64::MAX),
            input_cursor: Box::new(MockCursor(AtomicU64::new(1000))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            fence_state: None,
            replication_metrics: None,
            replica_active: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_input_queue_depth 100\n"),
            "expected depth 100, response: {response}"
        );
        assert!(
            response.contains("melin_input_queue_capacity 1048576\n"),
            "expected capacity metric, response: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn stage_utilization_in_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        let journal_util = Arc::new(StageUtilization::new());
        journal_util.busy.store(500, Ordering::Relaxed);
        journal_util.idle.store(9500, Ordering::Relaxed);
        // Distinct values per rotation outcome so a transposed pair in
        // the positional format args fails loudly.
        journal_util.rotations_fast_path.store(7, Ordering::Relaxed);
        journal_util
            .rotations_sync_fallback
            .store(3, Ordering::Relaxed);
        journal_util.rotations_failed.store(2, Ordering::Relaxed);

        let matching_util = Arc::new(StageUtilization::new());
        matching_util.busy.store(2000, Ordering::Relaxed);
        matching_util.idle.store(8000, Ordering::Relaxed);

        let response_util = Arc::new(StageUtilization::new());
        // Response left at 0/0 — verifies zero counters render correctly.

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            cursors: test_cursors(0, 0, 0, u64::MAX),
            input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            fence_state: None,
            replication_metrics: None,
            replica_active: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: journal_util,
            matching_utilization: matching_util,
            response_utilization: response_util,
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_stage_busy_total{stage=\"journal\"} 500\n"),
            "journal busy not found in: {response}"
        );
        assert!(
            response.contains("melin_stage_idle_total{stage=\"journal\"} 9500\n"),
            "journal idle not found in: {response}"
        );
        assert!(
            response.contains("melin_stage_busy_total{stage=\"matching\"} 2000\n"),
            "matching busy not found in: {response}"
        );
        assert!(
            response.contains("melin_stage_busy_total{stage=\"response\"} 0\n"),
            "response busy not found in: {response}"
        );
        assert!(
            response.contains("melin_journal_rotations_total{path=\"fast\"} 7\n"),
            "fast-path rotation count not found in: {response}"
        );
        assert!(
            response.contains("melin_journal_rotations_total{path=\"sync_fallback\"} 3\n"),
            "sync-fallback rotation count not found in: {response}"
        );
        assert!(
            response.contains("melin_journal_rotations_total{path=\"failed\"} 2\n"),
            "failed rotation count not found in: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn replication_ring_depth_and_fastest_cursor_in_metrics() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        // Slot 0: producer at 5000, consumer at 4950 → depth 50 (backpressured).
        // Slot 1: producer = consumer at 5000 → depth 0 (caught up).
        let prod_0: Arc<dyn QueueCursor> = Arc::new(MockCursor(AtomicU64::new(5000)));
        let prod_1: Arc<dyn QueueCursor> = Arc::new(MockCursor(AtomicU64::new(5000)));
        let cons_0 = Arc::new(Sequence::new(AtomicU64::new(4950)));
        let cons_1 = Arc::new(Sequence::new(AtomicU64::new(5000)));

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            // The engaged slot at acked 4990 drives the fastest-replica
            // gauge, which must decode back to the acked wire seq.
            cursors: test_cursors(5000, 5000, 5000, 4990),
            input_cursor: Box::new(MockCursor(AtomicU64::new(5000))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            fence_state: None,
            replication_metrics: None,
            replica_active: None,
            replication_ring_producer_cursors: Some([prod_0, prod_1]),
            replication_ring_consumer_cursors: Some([cons_0, cons_1]),
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_replication_ring_depth{slot=\"0\"} 50\n"),
            "slot 0 depth not found in: {response}"
        );
        assert!(
            response.contains("melin_replication_ring_depth{slot=\"1\"} 0\n"),
            "slot 1 depth not found in: {response}"
        );
        assert!(
            response.contains("melin_fastest_replica_cursor 4990\n"),
            "fastest cursor not found in: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn fastest_replica_cursor_renders_zero_with_no_replica() {
        // With every slot disengaged the derived view is None — it must
        // render as 0 so it doesn't dominate the plotted y-axis or skew
        // aggregates.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            cursors: test_cursors(0, 0, 0, u64::MAX),
            input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            fence_state: None,
            replication_metrics: None,
            replica_active: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_fastest_replica_cursor 0\n"),
            "expected sentinel mapped to 0, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    // ------------------------------------------------------------------
    // STATS-DUMP — bench tick-to-trade per-stage histogram dump.
    // ------------------------------------------------------------------

    #[test]
    fn stats_dump_returns_http_with_tsv_content_type() {
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        assert!(
            response.starts_with("HTTP/1.1 200 OK\r\n"),
            "expected HTTP 200, got: {response}"
        );
        assert!(
            response.contains("Content-Type: text/tab-separated-values"),
            "expected tab-separated-values content type, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[cfg(not(feature = "latency-trace"))]
    #[test]
    fn stats_dump_body_when_latency_trace_disabled() {
        // Without the feature, the body is a single comment line so
        // the bench can detect the unsupported state.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);

        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        assert!(
            response.contains("# latency-trace disabled"),
            "expected feature-disabled marker, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[cfg(feature = "latency-trace")]
    #[test]
    fn stats_dump_body_emits_registered_stages() {
        // Register a stage with deterministic samples and verify the
        // dump contains a tab-separated record for it.
        // The global registry is shared across tests; we use a unique
        // stage name to avoid collisions with concurrent test runs.
        // Flushed before the snapshot fetch — see the SyncHistogram
        // caveat in `crates/core/transport-core/src/trace.rs` tests.
        let mut rec = crate::trace::register_stage("test::stats_dump_emit_marker");
        rec.record_ns(1_500);
        rec.record_ns(2_500);
        rec.record_ns(3_500);
        rec.flush();

        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        // Body lines look like:
        //   stage\t<name>\t<samples>\t<min>\t<p50>\t<p90>\t<p99>\t<p99_9>\t<max>
        assert!(
            response.contains("stage\ttest::stats_dump_emit_marker\t3\t"),
            "expected stage record with 3 samples, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[cfg(feature = "latency-trace")]
    #[test]
    fn stats_dump_marks_truncation_instead_of_cutting_a_line() {
        // Every registered stage is emitted now, so the body scales with
        // the stage inventory. If it ever outgrows the buffer the bench
        // must see a marker, not a half-written record it would parse as
        // a real stage. Driven through a deliberately tiny buffer.
        let mut rec = crate::trace::register_stage("test::stats_dump_truncation");
        rec.record_ns(1_234);
        rec.flush();

        // 40 bytes is shorter than any single stage line, so an
        // unguarded `write!` straight to the cursor stops mid-record —
        // the exact failure this guards. It is also below the shortest
        // possible line regardless of how many stages other tests in
        // this process have registered, so the outcome is deterministic.
        let mut buf = [0u8; 40];
        let n = write_stats_dump(&mut buf);
        let body = std::str::from_utf8(&buf[..n]).expect("ascii body");

        assert!(
            n <= buf.len(),
            "wrote {n} bytes into a {}-byte buffer",
            buf.len()
        );
        assert!(
            body.contains("# truncated\t"),
            "expected a truncation marker, got: {body:?}"
        );
        assert!(
            body.ends_with('\n'),
            "body must not end mid-line, got: {body:?}"
        );
        for line in body.lines() {
            if let Some(rest) = line.strip_prefix("stage\t") {
                assert_eq!(
                    rest.split('\t').count(),
                    8,
                    "emitted stage line is incomplete: {line:?}"
                );
            }
        }
    }

    #[cfg(feature = "latency-trace")]
    #[test]
    fn stats_dump_body_emits_zero_sample_stages() {
        // A stage that registered but recorded nothing must still
        // appear, so the bench can tell "quiet" from "not compiled in".
        let _rec = crate::trace::register_stage("test::stats_dump_zero_sample");

        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        assert!(
            response.contains("stage\ttest::stats_dump_zero_sample\t0\t0\t0\t0\t0\t0\t0"),
            "expected zero-sample stage record, got: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[cfg(feature = "latency-trace")]
    #[test]
    fn stats_dump_body_line_format() {
        // Pin the wire contract that phase 3's bench parser will rely
        // on: every non-comment body line is exactly 9 tab-separated
        // fields — `stage`, name, then 7 numeric percentile fields.
        // Recorder dropped before the snapshot fetch — see the
        // SyncHistogram caveat in `crates/core/transport-core/src/trace.rs`
        // tests.
        {
            let mut rec = crate::trace::register_stage("test::stats_dump_line_format_marker");
            rec.record_ns(1_000);
            rec.record_ns(2_000);
            rec.record_ns(3_000);
        }

        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 0, u64::MAX);
        let response = http_request(addr, "GET /stats-dump HTTP/1.1\r\n\r\n");

        // Strip HTTP head, find our marker line.
        let body = response
            .split("\r\n\r\n")
            .nth(1)
            .expect("body separated by blank line");
        let line = body
            .lines()
            .find(|l| l.contains("test::stats_dump_line_format_marker"))
            .unwrap_or_else(|| panic!("marker line missing in body: {body}"));

        let fields: Vec<&str> = line.split('\t').collect();
        assert_eq!(
            fields.len(),
            9,
            "expected 9 tab-separated fields, got {}: {fields:?}",
            fields.len(),
        );
        assert_eq!(fields[0], "stage");
        assert_eq!(fields[1], "test::stats_dump_line_format_marker");
        assert_eq!(fields[2], "3");
        // Fields 3..9 are min/p50/p90/p99/p99_9/max — must parse as u64.
        for (i, f) in fields.iter().enumerate().skip(2) {
            f.parse::<u64>()
                .unwrap_or_else(|_| panic!("field {i} not a u64: {f:?}"));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    // ------------------------------------------------------------------
    // PER-REPLICA METRICS — Prometheus output for the per-slot replication
    // counters wasn't asserted by any test before, so a rename or label
    // typo in `write_prometheus` could ship silently. One test per family.
    // ------------------------------------------------------------------

    /// Spin up the health loop with a fully-populated `ReplicationMetrics`
    /// and gauge-style auxiliary cursors, then return the Prometheus body
    /// for the caller to make assertions on. Keeps each per-family test
    /// short.
    fn prometheus_with_full_replication_state()
    -> (String, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        // Populate every per-slot counter with a distinct value so a
        // label/index swap (e.g. printing slot 1's value under slot 0)
        // shows up as a failed assertion below.
        let metrics = Arc::new(ReplicationMetrics::default());
        metrics.acked_sequence[0].store(900, Ordering::Relaxed);
        metrics.acked_sequence[1].store(800, Ordering::Relaxed);
        metrics.in_memory_sequence[0].store(950, Ordering::Relaxed);
        metrics.in_memory_sequence[1].store(850, Ordering::Relaxed);
        metrics.bytes_sent[0].store(11_111, Ordering::Relaxed);
        metrics.bytes_sent[1].store(22_222, Ordering::Relaxed);
        metrics.ack_latency_us[0].store(33, Ordering::Relaxed);
        metrics.ack_latency_us[1].store(44, Ordering::Relaxed);
        metrics.catching_up[0].store(true, Ordering::Relaxed);
        metrics.catching_up[1].store(false, Ordering::Relaxed);
        metrics.evictions_total.store(7, Ordering::Relaxed);
        metrics.divergence_total.store(3, Ordering::Relaxed);

        // journal_seq=1000 so per_replica_lag = 1000 - acked.
        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            // Quorum cursor = slowest acked (slot 1's 800), consistent with
            // the per-slot metrics above.
            cursors: test_cursors(1000, 1000, 1000, 800),
            input_cursor: Box::new(MockCursor(AtomicU64::new(1000))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: Some(Arc::new(AtomicU32::new(2))),
            fence_state: None,
            replication_metrics: Some(metrics),
            replica_active: Some([
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(true)),
            ]),
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        (body, shutdown, handle)
    }

    #[test]
    fn metrics_emits_per_replica_acked_and_in_memory_sequence() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();
        assert!(
            body.contains("melin_replica_acked_sequence{slot=\"0\"} 900\n"),
            "slot 0 acked: {body}"
        );
        assert!(
            body.contains("melin_replica_acked_sequence{slot=\"1\"} 800\n"),
            "slot 1 acked: {body}"
        );
        assert!(
            body.contains("melin_replica_in_memory_sequence{slot=\"0\"} 950\n"),
            "slot 0 in_memory: {body}"
        );
        assert!(
            body.contains("melin_replica_in_memory_sequence{slot=\"1\"} 850\n"),
            "slot 1 in_memory: {body}"
        );
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_emits_per_replica_lag_relative_to_journal_seq() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();
        // journal_seq=1000, acked=[900, 800] → lag=[100, 200].
        assert!(
            body.contains("melin_replica_lag{slot=\"0\"} 100\n"),
            "slot 0 lag: {body}"
        );
        assert!(
            body.contains("melin_replica_lag{slot=\"1\"} 200\n"),
            "slot 1 lag: {body}"
        );
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    /// Regression: a replica engaged at acked 0 (fresh journal, mid
    /// catch-up) must report its real per-slot lag — `acked == 0` is also
    /// the cleared-on-disconnect gauge state, so only the per-slot active
    /// flag can distinguish the two. Slot 0 is engaged at 0 (full lag),
    /// slot 1 is disconnected (lag 0).
    #[test]
    fn engaged_replica_at_acked_zero_reports_real_lag() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        let metrics = Arc::new(ReplicationMetrics::default());
        // Both slots' gauges read 0 — indistinguishable without the flags.
        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            cursors: test_cursors(1_000, 1_000, 1_000, 0),
            input_cursor: Box::new(MockCursor(AtomicU64::new(1_000))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: Some(Arc::new(AtomicU32::new(1))),
            fence_state: None,
            replication_metrics: Some(metrics),
            replica_active: Some([
                Arc::new(AtomicBool::new(true)),
                Arc::new(AtomicBool::new(false)),
            ]),
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            body.contains("melin_replica_lag{slot=\"0\"} 1000\n"),
            "engaged-at-0 slot should report full lag: {body}"
        );
        assert!(
            body.contains("melin_replica_lag{slot=\"1\"} 0\n"),
            "disengaged slot should report zero lag: {body}"
        );
        // The aggregate agrees with the drill-down: quorum acked 0 → lag 1000.
        assert!(
            body.contains("melin_replication_lag 1000\n"),
            "aggregate lag should match the engaged slot: {body}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    /// Writer↔reader contract: drive the *production* cursor writer
    /// (`ReplicaCursors`) against the same atomics the health snapshot
    /// reads — no hand-seeded values — and assert the gauges decode to
    /// exact wire-seq numbers through every lifecycle step (two engaged,
    /// one disconnects, all disconnect). This is what pins the slot-acked
    /// encode (writer) and decode (reader) to the same `SlotAcked`
    /// convention; the other tests seed cursors by hand and would keep
    /// passing if one side drifted.
    #[test]
    fn replica_cursor_writer_drives_health_gauges_end_to_end() {
        use crate::replication::ReplicaCursors;
        use crate::replication::protocol::Ack;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        // Production wiring shape: the bundle owns the per-slot cursors
        // (the quorum and fastest gauges are derived from them at read
        // time), and ReplicaCursors is the single writer for the slots
        // plus the metrics gauge pair.
        let cursors = PipelineCursors::new(
            WireSeq::new(1_000),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(Sequence::new(AtomicU64::new(0))),
        );
        let metrics = Arc::new(ReplicationMetrics::default());
        let active = [
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        ];
        let writer = ReplicaCursors::new(cursors.replica_slot_cursors(), Arc::clone(&metrics));

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            cursors: cursors.clone(),
            input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: Some(Arc::new(AtomicU32::new(2))),
            fence_state: None,
            replication_metrics: Some(Arc::clone(&metrics)),
            replica_active: Some([Arc::clone(&active[0]), Arc::clone(&active[1])]),
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: Arc::new(StageUtilization::new()),
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        // Two replicas engage and ack: slot 0 at 900, slot 1 at 800.
        // Ordering mirrors the senders: seed/ack before the flag flip.
        writer.seed_on_handshake(0, 850);
        active[0].store(true, Ordering::Release);
        writer.seed_on_handshake(1, 800);
        active[1].store(true, Ordering::Release);
        writer
            .record_ack(
                0,
                &Ack {
                    acked_sequence: 900,
                    in_memory_sequence: 950,
                },
                1_000,
            )
            .expect("valid ack");

        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            body.contains("melin_replication_lag 200\n"),
            "quorum = slowest engaged (800): journal 1000 - 800: {body}"
        );
        assert!(
            body.contains("melin_fastest_replica_cursor 900\n"),
            "fastest = highest acked, decoded to wire seq: {body}"
        );
        assert!(
            body.contains("melin_replica_lag{slot=\"0\"} 100\n"),
            "slot 0 lag: {body}"
        );
        assert!(
            body.contains("melin_replica_lag{slot=\"1\"} 200\n"),
            "slot 1 lag: {body}"
        );

        // The slower replica disconnects: quorum and fastest both follow
        // the survivor; its per-slot lag drops to 0 (disengaged).
        writer.clear_on_disconnect(1);
        active[1].store(false, Ordering::Release);

        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            body.contains("melin_replication_lag 100\n"),
            "quorum follows the survivor (900): {body}"
        );
        assert!(
            body.contains("melin_fastest_replica_cursor 900\n"),
            "single engaged replica IS the fastest — not the sentinel: {body}"
        );
        assert!(
            body.contains("melin_replica_lag{slot=\"1\"} 0\n"),
            "disengaged slot lag: {body}"
        );

        // Last replica disconnects: everything returns to the no-replica shape.
        writer.clear_on_disconnect(0);
        active[0].store(false, Ordering::Release);

        let body = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            body.contains("melin_replication_lag 0\n"),
            "no engaged replica → lag 0: {body}"
        );
        assert!(
            body.contains("melin_fastest_replica_cursor 0\n"),
            "no engaged replica → fastest renders 0: {body}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_emits_per_replica_bytes_and_ack_latency() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();
        assert!(
            body.contains("melin_replica_bytes_sent_total{slot=\"0\"} 11111\n"),
            "slot 0 bytes: {body}"
        );
        assert!(
            body.contains("melin_replica_bytes_sent_total{slot=\"1\"} 22222\n"),
            "slot 1 bytes: {body}"
        );
        assert!(
            body.contains("melin_replica_ack_latency_us{slot=\"0\"} 33\n"),
            "slot 0 latency: {body}"
        );
        assert!(
            body.contains("melin_replica_ack_latency_us{slot=\"1\"} 44\n"),
            "slot 1 latency: {body}"
        );
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_emits_per_replica_catching_up_and_evictions() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();
        assert!(
            body.contains("melin_replica_catching_up{slot=\"0\"} 1\n"),
            "slot 0 catching_up: {body}"
        );
        assert!(
            body.contains("melin_replica_catching_up{slot=\"1\"} 0\n"),
            "slot 1 catching_up: {body}"
        );
        assert!(
            body.contains("melin_replica_evictions_total 7\n"),
            "evictions: {body}"
        );
        assert!(
            body.contains("melin_replica_divergence_total 3\n"),
            "divergence: {body}"
        );
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn metrics_emits_response_gate_counters_and_policy_degraded() {
        // The response-stage StageUtilization carries three signals not
        // exercised by `stage_utilization_in_metrics`: gate_journal,
        // gate_replication, and policy_degraded. All three are read
        // from the response stage's utilization counter on every
        // health snapshot.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        let response_util = Arc::new(StageUtilization::new());
        response_util.gate_journal.store(13, Ordering::Relaxed);
        response_util.gate_replication.store(17, Ordering::Relaxed);
        response_util.policy_degraded.store(true, Ordering::Relaxed);
        // 2.5s in nanos — exercises the nanos -> seconds conversion and
        // the sub-second precision of the `_seconds_total` formatting.
        response_util
            .policy_degraded_nanos
            .store(2_500_000_000, Ordering::Relaxed);

        let state = HealthState {
            active_connections: Arc::new(AtomicU64::new(0)),
            events_processed: Arc::new(AtomicU64::new(0)),
            cursors: test_cursors(0, 0, 0, u64::MAX),
            input_cursor: Box::new(MockCursor(AtomicU64::new(0))),
            pipeline_healthy: Arc::new(AtomicBool::new(true)),
            replicas_connected: None,
            fence_state: None,
            replication_metrics: None,
            replica_active: None,
            replication_ring_producer_cursors: None,
            replication_ring_consumer_cursors: None,
            journal_utilization: Arc::new(StageUtilization::new()),
            matching_utilization: Arc::new(StageUtilization::new()),
            response_utilization: response_util,
            raft: None,
        };

        let handle = std::thread::spawn(move || {
            health_loop(&listener, &state, &s);
        });

        let response = http_request(addr, "GET /metrics HTTP/1.1\r\n\r\n");
        assert!(
            response.contains("melin_response_gate_total{blocker=\"journal\"} 13\n"),
            "gate_journal: {response}"
        );
        assert!(
            response.contains("melin_response_gate_total{blocker=\"replication\"} 17\n"),
            "gate_replication: {response}"
        );
        assert!(
            response.contains("melin_durability_policy_degraded 1\n"),
            "policy_degraded: {response}"
        );
        assert!(
            response.contains("melin_durability_policy_degraded_seconds_total 2.500000\n"),
            "policy_degraded_seconds_total: {response}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    // ------------------------------------------------------------------
    // REQUEST CLASSIFICATION — guard against `detect_request` returning
    // `PlainTcp` for an HTTP request that doesn't start with `GET `.
    // A non-GET method would otherwise get a raw status-line response
    // (no HTTP framing), which most HTTP clients would treat as garbage.
    // ------------------------------------------------------------------

    #[test]
    fn non_get_http_method_is_classified_as_plain_tcp() {
        // POST is not a documented health-endpoint method — it falls
        // through the GET prefix guards and is treated as a plain TCP
        // probe. The server writes a raw status line and closes; the
        // unread request bytes still in the kernel buffer mean the close
        // may RST the connection, so the client can legitimately observe
        // either the status line + EOF or a truncated read + RST. The
        // load-bearing assertion is just "no HTTP framing comes back" —
        // a future regression that started serving an HTTP response to
        // POST would be a deliberate, reviewed change.
        let (addr, _events, _healthy, shutdown, handle) = start_health(0, 42, u64::MAX);

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client
            .write_all(b"POST /metrics HTTP/1.1\r\nContent-Length: 0\r\n\r\n")
            .unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();
        let mut buf = String::new();
        // RST from the server's close (unread bytes in recv buffer) is
        // expected on Linux — tolerate the read error and inspect what
        // bytes did arrive before the reset.
        let _ = client.read_to_string(&mut buf);

        assert!(
            !buf.starts_with("HTTP/"),
            "POST must not get an HTTP response (got: {buf:?})"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    // ------------------------------------------------------------------
    // BUFFER CAPACITY — `write_prometheus` writes into a fixed-size stack
    // buffer (8 KiB). A future addition of more per-slot metrics could
    // silently truncate output. Pin a lower bound on the current full
    // body length and assert the buffer still holds it with headroom.
    // ------------------------------------------------------------------

    #[test]
    fn prometheus_body_fits_with_headroom_under_full_replication_state() {
        let (body, shutdown, handle) = prometheus_with_full_replication_state();

        // Strip HTTP headers — keep only the metrics body.
        let metrics_body = body
            .split("\r\n\r\n")
            .nth(1)
            .expect("HTTP head separator present");

        // The body buffer in handle_health_connection is 8192 bytes.
        // Today's body is around 3 KiB; allocate 25 % headroom and fail
        // loudly if we ever drift past it. The point of this test is
        // to fire before silent truncation, not to track the exact size.
        const BODY_BUF: usize = 8192;
        const HEADROOM_LIMIT: usize = BODY_BUF * 3 / 4; // 6144

        assert!(
            metrics_body.len() < HEADROOM_LIMIT,
            "prometheus body ({} bytes) is past 75 % of the {BODY_BUF}-byte stack \
             buffer — adding more metrics will silently truncate the output. \
             Either trim the body or grow body_buf in handle_health_connection.",
            metrics_body.len()
        );

        // Sanity: confirm we're well past zero. If the body shrank
        // dramatically, something would have stopped rendering.
        assert!(
            metrics_body.len() > 1500,
            "prometheus body unexpectedly short ({} bytes) — a write! failure \
             would silently drop content. Body: {metrics_body}",
            metrics_body.len()
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
