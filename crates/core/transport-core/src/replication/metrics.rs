//! Per-replica observability counters.
//!
//! Updated by the primary's per-slot sender threads (atomic stores)
//! and read by the health endpoint (atomic loads). Zero hot-path
//! impact — all writes happen alongside TCP/DPDK I/O on the sender
//! threads, never on the matching critical path.

use std::sync::atomic::{AtomicBool, AtomicU64};

/// Per-slot replication metrics exposed via the health endpoint.
///
/// The two-element arrays mirror the topology cap of `1 primary + 2
/// replica slots`. Sized at compile time because the cluster shape is
/// fixed by the response gate's `MAX_CLUSTER_SIZE` (see
/// [`crate::durability_policy::MAX_CLUSTER_SIZE`]); a `Vec` would add
/// a layer of indirection on every health-endpoint read for zero
/// gain.
pub struct ReplicationMetrics {
    /// Per-slot acked sequence (last sequence the replica confirmed
    /// as durable). Used to compute per-replica replication lag.
    pub acked_sequence: [AtomicU64; 2],
    /// Per-slot in-memory sequence (last sequence the replica has
    /// accepted into its pipeline pre-journal). Always
    /// `>= acked_sequence`. Used by the multi-level durability gate
    /// (see [`crate::durability_policy`]).
    pub in_memory_sequence: [AtomicU64; 2],
    /// Per-slot bytes sent to the replica (cumulative). Includes
    /// catch-up and live streaming.
    pub bytes_sent: [AtomicU64; 2],
    /// Per-slot ack round-trip latency in microseconds. Updated on
    /// each ack by measuring elapsed time since the last batch send.
    pub ack_latency_us: [AtomicU64; 2],
    /// Per-slot catch-up state: true while streaming historical
    /// journal entries, false once the replica enters live mode.
    pub catching_up: [AtomicBool; 2],
    /// Total eviction count (both slots combined). Incremented when
    /// the journal stage's backpressure timeout fires.
    pub evictions_total: AtomicU64,
}

impl Default for ReplicationMetrics {
    fn default() -> Self {
        Self {
            acked_sequence: [AtomicU64::new(0), AtomicU64::new(0)],
            in_memory_sequence: [AtomicU64::new(0), AtomicU64::new(0)],
            bytes_sent: [AtomicU64::new(0), AtomicU64::new(0)],
            ack_latency_us: [AtomicU64::new(0), AtomicU64::new(0)],
            catching_up: [AtomicBool::new(false), AtomicBool::new(false)],
            evictions_total: AtomicU64::new(0),
        }
    }
}
