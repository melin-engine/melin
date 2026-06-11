//! Owning module for the primary's view of replica progress.
//!
//! Every store to the per-replica progress cursors — the values the
//! response gate's durability policy and the health endpoint read —
//! goes through [`ReplicaCursors`]. Before this module existed, the
//! same store group (per-slot acked position, the shared min/max
//! replication cursors, and the `ReplicationMetrics` gauge pair) was
//! repeated at ~10 call sites across the TCP and DPDK senders, each
//! re-stating the memory-ordering contract in comments. During the
//! pre-v14 vacuous-gate incident, monitoring reported `replica_lag = 0`
//! from these cursors the entire time the durability gate was being
//! satisfied by sequence-space drift — scattered stores are exactly
//! what made that class of bug invisible. One owning module means one
//! place to state the ordering contract and one store site to guard
//! with invariants.
//!
//! ## Cursor spaces
//!
//! - **Slot-acked space** (`slot_acked`, `cursor_min`, `cursor_max`):
//!   `acked_sequence + 1` — "the replica has durably confirmed every
//!   sequence below this value". `u64::MAX` marks a disengaged slot;
//!   because the shared cursors are recomputed as `min`/`max` over the
//!   per-slot values, a disengaged slot never gates the min and an
//!   all-disengaged store yields `u64::MAX` (= "not gating") on both.
//! - **Wire-ack space** (`ReplicationMetrics::acked_sequence` /
//!   `in_memory_sequence`): the `Ack` frame's fields verbatim — the
//!   highest primary sequence the replica has fsynced / accepted into
//!   its pipeline. This is the pair `evaluate_durability` compares
//!   against `OutputSlot.wire_seq`.
//!
//! ## Ordering contract
//!
//! The gauge pair is stored `Relaxed`; publication to the response
//! gate rides on the caller's per-slot `active_flag` `Release` store.
//! Callers MUST therefore order calls relative to their flag flips:
//!
//! - [`ReplicaCursors::seed_on_handshake`] **before** storing
//!   `active_flag = true` (`Release`), so a gate reader that observes
//!   `active = true` also observes a seeded, non-zero cursor pair —
//!   otherwise a 1-replica deployment running degraded freezes the
//!   gate at 0 for the first live-ack RTT after a reconnect.
//! - [`ReplicaCursors::clear_on_disconnect`] **before** storing
//!   `active_flag = false` (`Release`), so a reader that observes
//!   `active = false` also observes the zeroed pair. Reversing this
//!   leaves a window on weak-memory architectures (ARM/AArch64) where
//!   a reader sees `active = true` (stale) paired with `cursor = 0`
//!   (fresh) — see the B2 notes in
//!   `docs/durability-policy-followups.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::error;

use crate::cursors::{SlotAcked, WireSeq};

use super::metrics::ReplicationMetrics;
use super::protocol::Ack;

/// Number of replica slots. Fixed by the `1 primary + 2 replicas`
/// topology cap (see `ReplicationMetrics` for the same rationale).
const SLOTS: usize = 2;

/// A disengaged slot's parking value, from the shared [`SlotAcked`] space
/// type: `min` over all-disengaged slots propagates it into the shared
/// cursors, where `PipelineCursors::load_replica_quorum_acked` maps it to
/// `None` (and the max recompute below skips it explicitly).
const DISENGAGED: u64 = SlotAcked::DISENGAGED.raw();

/// A replica reported a cursor that cannot be true: ahead of what the
/// primary ever sent it, or with the persisted track ahead of the
/// in-memory track. Either is a protocol violation (a bug in the
/// cluster software or a rogue replica binary), never a load effect —
/// the caller must evict the replica. The violating ack is NOT applied:
/// advancing the gate's cursors from it would let the durability policy
/// release client acks against confirmation that never happened — the
/// exact failure shape of the pre-v14 vacuous-gate incident.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckViolation {
    pub slot: usize,
    pub acked_sequence: u64,
    pub in_memory_sequence: u64,
    /// Highest primary sequence actually streamed to this replica at
    /// the time the ack arrived.
    pub highest_sent_sequence: u64,
}

impl std::fmt::Display for AckViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "replica ack violation (slot {}): acked={} in_memory={} but highest sent={}",
            self.slot, self.acked_sequence, self.in_memory_sequence, self.highest_sent_sequence
        )
    }
}

impl std::error::Error for AckViolation {}

/// Single owner of the primary's per-replica progress cursors.
///
/// Shared across the per-slot sender threads (TCP path) or borrowed by
/// the single-threaded driver loop (DPDK path). All state is atomic;
/// per-slot writers never store to another slot's entry, and the
/// shared min/max recompute tolerates concurrent recomputes because
/// each per-slot cursor is monotonic within a connection.
pub struct ReplicaCursors {
    /// Per-slot acked position in slot-acked space (`acked_sequence + 1`),
    /// `u64::MAX` when disengaged. `[AtomicU64; 2]` rather than per-slot
    /// `Arc`s: both slots live on one cache line of a single shared
    /// allocation, and the recompute needs both values anyway.
    slot_acked: [AtomicU64; SLOTS],
    /// `min` over the per-slot cursors — every connected replica has
    /// durably confirmed up to here. Shared with the response stage and
    /// health endpoint (created at server startup), hence `Arc` rather
    /// than owned.
    cursor_min: Arc<AtomicU64>,
    /// `max` over the per-slot cursors — the fastest replica has
    /// confirmed up to here. Same sharing rationale as `cursor_min`.
    cursor_max: Arc<AtomicU64>,
    /// Per-slot wire-ack gauges read by the response gate and health.
    metrics: Arc<ReplicationMetrics>,
}

impl ReplicaCursors {
    /// Wrap the shared cursors created at server startup. Recomputes
    /// the min/max pair from the (disengaged) slot state so the
    /// invariant `cursor_min/max == min/max(slot_acked)` holds from
    /// construction.
    pub fn new(
        cursor_min: Arc<AtomicU64>,
        cursor_max: Arc<AtomicU64>,
        metrics: Arc<ReplicationMetrics>,
    ) -> Self {
        let cursors = Self {
            slot_acked: [AtomicU64::new(DISENGAGED), AtomicU64::new(DISENGAGED)],
            cursor_min,
            cursor_max,
            metrics,
        };
        cursors.recompute_shared();
        cursors
    }

    /// Engage a slot after handshake + catch-up: the replica has
    /// confirmed everything up to `handshake_last_sequence`, so the
    /// slot cursor starts at `handshake_last_sequence + 1` and the
    /// gauge pair is seeded with the handshake value.
    ///
    /// Ordering: call BEFORE storing `active_flag = true` (`Release`) —
    /// see the module docs.
    pub fn seed_on_handshake(&self, slot: usize, handshake_last_sequence: u64) {
        self.metrics.acked_sequence[slot].store(handshake_last_sequence, Ordering::Relaxed);
        self.metrics.in_memory_sequence[slot].store(handshake_last_sequence, Ordering::Relaxed);
        self.slot_acked[slot].store(
            SlotAcked::from_acked(WireSeq::new(handshake_last_sequence)).raw(),
            Ordering::Release,
        );
        self.recompute_shared();
    }

    /// Record a replica's `Ack` frame: advance the slot cursor and the
    /// wire-ack gauge pair, then recompute the shared min/max.
    ///
    /// `highest_sent_sequence` is the highest primary sequence the
    /// caller has actually streamed to this replica (handshake
    /// baseline, catch-up end, and live ring drains all count) —
    /// callers maintain it via [`super::sent::SentHighWater`], which
    /// keeps it monotonic within a connection by construction. The
    /// ack-sanity invariant is checked against it at this single store
    /// site: a replica cannot truthfully confirm sequences it was never
    /// sent, nor report its persisted track ahead of its in-memory
    /// track. On violation the ack is NOT applied and the caller must
    /// evict the replica — see [`AckViolation`]. This is the check that
    /// turns a v14-class cursor drift into a same-day `error!` log line
    /// instead of a months-later benchmark anomaly.
    pub fn record_ack(
        &self,
        slot: usize,
        ack: &Ack,
        highest_sent_sequence: u64,
    ) -> Result<(), AckViolation> {
        if ack.in_memory_sequence > highest_sent_sequence
            || ack.acked_sequence > ack.in_memory_sequence
        {
            let violation = AckViolation {
                slot,
                acked_sequence: ack.acked_sequence,
                in_memory_sequence: ack.in_memory_sequence,
                highest_sent_sequence,
            };
            // error! (not warn/debug): an authenticated cluster member
            // reporting an impossible cursor is a software bug on one
            // side of the connection, never client input or load.
            error!(
                slot,
                acked_sequence = ack.acked_sequence,
                in_memory_sequence = ack.in_memory_sequence,
                highest_sent_sequence,
                "replica ack violation — evicting replica"
            );
            return Err(violation);
        }
        self.metrics.acked_sequence[slot].store(ack.acked_sequence, Ordering::Relaxed);
        self.metrics.in_memory_sequence[slot].store(ack.in_memory_sequence, Ordering::Relaxed);
        self.metrics.acks_received[slot].fetch_add(1, Ordering::Relaxed);
        self.slot_acked[slot].store(
            SlotAcked::from_acked(WireSeq::new(ack.acked_sequence)).raw(),
            Ordering::Release,
        );
        self.recompute_shared();
        Ok(())
    }

    /// Disengage a slot on disconnect or eviction: zero the gauge pair,
    /// park the slot cursor at `u64::MAX` (not gating), and recompute
    /// the shared min/max from the surviving slot. Without the
    /// recompute, `cursor_min` stays frozen at its pre-disconnect value
    /// (the min that included this slot's last ack) and the primary
    /// stops acking client requests even though the surviving replica
    /// is healthy.
    ///
    /// Idempotent, and safe for slots that never engaged (handshake
    /// failures): the gauge pair is already zero and the slot cursor
    /// already `u64::MAX`.
    ///
    /// Ordering: call BEFORE storing `active_flag = false` (`Release`) —
    /// see the module docs.
    pub fn clear_on_disconnect(&self, slot: usize) {
        self.metrics.acked_sequence[slot].store(0, Ordering::Relaxed);
        self.metrics.in_memory_sequence[slot].store(0, Ordering::Relaxed);
        self.slot_acked[slot].store(DISENGAGED, Ordering::Release);
        self.recompute_shared();
    }

    /// Recompute the shared min/max pair from the per-slot cursors.
    ///
    /// Plain stores (not `fetch_min`/`fetch_max`) because the cursors
    /// must be able to *decrease*: a second replica connecting with a
    /// lower acked position lowers the min, and a disconnect can lower
    /// the max back to the survivor's position.
    ///
    /// The min needs no disengaged handling (`min(x, DISENGAGED) == x`,
    /// and all-disengaged yields the sentinel), but the max must skip
    /// disengaged slots explicitly — otherwise one parked slot's sentinel
    /// masquerades as the fastest replica whenever fewer than two replicas
    /// are connected.
    fn recompute_shared(&self) {
        let a = self.slot_acked[0].load(Ordering::Acquire);
        let b = self.slot_acked[1].load(Ordering::Acquire);
        self.cursor_min.store(a.min(b), Ordering::Release);
        let max = match (a == DISENGAGED, b == DISENGAGED) {
            (true, true) => DISENGAGED,
            (true, false) => b,
            (false, true) => a,
            (false, false) => a.max(b),
        };
        self.cursor_max.store(max, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (
        Arc<AtomicU64>,
        Arc<AtomicU64>,
        Arc<ReplicationMetrics>,
        ReplicaCursors,
    ) {
        let min = Arc::new(AtomicU64::new(u64::MAX));
        let max = Arc::new(AtomicU64::new(u64::MAX));
        let metrics = Arc::new(ReplicationMetrics::default());
        let cursors = ReplicaCursors::new(Arc::clone(&min), Arc::clone(&max), Arc::clone(&metrics));
        (min, max, metrics, cursors)
    }

    fn ack(acked: u64, in_memory: u64) -> Ack {
        Ack {
            acked_sequence: acked,
            in_memory_sequence: in_memory,
        }
    }

    #[test]
    fn fresh_store_is_disengaged_on_both_cursors() {
        let (min, max, _, _cursors) = store();
        assert_eq!(min.load(Ordering::Acquire), u64::MAX);
        assert_eq!(max.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn seed_engages_both_cursors_at_the_single_slot() {
        let (min, max, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 41);
        // Slot 0 is the only engaged replica, so it is simultaneously the
        // slowest (min) and the fastest (max) at 42 (= last + 1). The
        // disengaged slot 1 must not leak its parking sentinel into the max.
        assert_eq!(min.load(Ordering::Acquire), 42);
        assert_eq!(max.load(Ordering::Acquire), 42);
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 41);
        assert_eq!(metrics.in_memory_sequence[0].load(Ordering::Relaxed), 41);
    }

    #[test]
    fn record_ack_advances_gauges_and_shared_cursors() {
        let (min, max, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.seed_on_handshake(1, 0);
        cursors.record_ack(0, &ack(10, 15), 15).expect("valid ack");
        cursors.record_ack(1, &ack(7, 12), 12).expect("valid ack");
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 10);
        assert_eq!(metrics.in_memory_sequence[0].load(Ordering::Relaxed), 15);
        assert_eq!(metrics.acked_sequence[1].load(Ordering::Relaxed), 7);
        assert_eq!(metrics.in_memory_sequence[1].load(Ordering::Relaxed), 12);
        // Slot-acked space: 11 and 8.
        assert_eq!(min.load(Ordering::Acquire), 8);
        assert_eq!(max.load(Ordering::Acquire), 11);
    }

    #[test]
    fn second_replica_joining_behind_lowers_the_min_then_catches_up() {
        let (min, max, _, cursors) = store();
        cursors.seed_on_handshake(0, 100);
        cursors
            .record_ack(0, &ack(500, 500), 500)
            .expect("valid ack");
        assert_eq!(min.load(Ordering::Acquire), 501);
        // A fresh replica joins having only caught up to 200 — the min
        // must DECREASE (plain store, not fetch_max).
        cursors.seed_on_handshake(1, 200);
        assert_eq!(min.load(Ordering::Acquire), 201);
        assert_eq!(max.load(Ordering::Acquire), 501);
        // It catches up partially, then fully; the min tracks it until
        // the two slots converge.
        cursors
            .record_ack(1, &ack(350, 350), 500)
            .expect("valid ack");
        assert_eq!(min.load(Ordering::Acquire), 351);
        assert_eq!(max.load(Ordering::Acquire), 501);
        cursors
            .record_ack(1, &ack(500, 500), 500)
            .expect("valid ack");
        assert_eq!(min.load(Ordering::Acquire), 501);
        assert_eq!(max.load(Ordering::Acquire), 501);
    }

    #[test]
    fn disconnect_zeroes_gauges_and_releases_the_min_to_the_survivor() {
        let (min, max, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.seed_on_handshake(1, 0);
        cursors.record_ack(0, &ack(10, 15), 15).expect("valid ack");
        cursors.record_ack(1, &ack(7, 12), 12).expect("valid ack");
        cursors.clear_on_disconnect(1);
        assert_eq!(metrics.acked_sequence[1].load(Ordering::Relaxed), 0);
        assert_eq!(metrics.in_memory_sequence[1].load(Ordering::Relaxed), 0);
        // Survivor (slot 0, cursor 11) owns both cursors — the max must
        // DECREASE back to the survivor, not park at the sentinel.
        assert_eq!(min.load(Ordering::Acquire), 11);
        assert_eq!(max.load(Ordering::Acquire), 11);
    }

    #[test]
    fn disconnect_of_last_replica_parks_both_cursors() {
        let (min, max, _, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.record_ack(0, &ack(10, 15), 15).expect("valid ack");
        cursors.clear_on_disconnect(0);
        assert_eq!(min.load(Ordering::Acquire), u64::MAX);
        assert_eq!(max.load(Ordering::Acquire), u64::MAX);
    }

    #[test]
    fn disconnect_of_never_engaged_slot_is_a_safe_noop() {
        let (min, max, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.record_ack(0, &ack(10, 15), 15).expect("valid ack");
        // Slot 1 fails its handshake without ever engaging.
        cursors.clear_on_disconnect(1);
        assert_eq!(min.load(Ordering::Acquire), 11);
        assert_eq!(max.load(Ordering::Acquire), 11);
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 10);
    }

    #[test]
    fn valid_acks_count_toward_acks_received() {
        let (_, _, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.record_ack(0, &ack(10, 15), 20).expect("valid ack");
        cursors.record_ack(0, &ack(15, 20), 20).expect("valid ack");
        assert_eq!(metrics.acks_received[0].load(Ordering::Relaxed), 2);
        assert_eq!(metrics.acks_received[1].load(Ordering::Relaxed), 0);
        // Disconnect zeroes the gauges but not the cumulative counter
        // (it's a Prometheus-style total, monotonic across reconnects).
        cursors.clear_on_disconnect(0);
        assert_eq!(metrics.acks_received[0].load(Ordering::Relaxed), 2);
    }

    #[test]
    fn ack_ahead_of_highest_sent_is_rejected_and_not_applied() {
        let (min, max, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 100);
        // The replica claims an in-memory cursor past anything the
        // primary streamed to it — a v14-class impossible cursor.
        let violation = cursors
            .record_ack(0, &ack(150, 250), 200)
            .expect_err("ack beyond highest sent must be rejected");
        assert_eq!(violation.slot, 0);
        assert_eq!(violation.in_memory_sequence, 250);
        assert_eq!(violation.highest_sent_sequence, 200);
        // Nothing moved: the gate's view still shows the seeded state
        // (slot 0 is the only engaged replica, so it owns both cursors).
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 100);
        assert_eq!(metrics.in_memory_sequence[0].load(Ordering::Relaxed), 100);
        assert_eq!(metrics.acks_received[0].load(Ordering::Relaxed), 0);
        assert_eq!(min.load(Ordering::Acquire), 101);
        assert_eq!(max.load(Ordering::Acquire), 101);
    }

    #[test]
    fn persisted_track_ahead_of_in_memory_is_rejected() {
        let (_, _, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        // acked (fsynced) can never lead in-memory (received) — the
        // replica journals what it has accepted into its pipeline.
        let violation = cursors
            .record_ack(0, &ack(50, 40), 100)
            .expect_err("acked > in_memory must be rejected");
        assert_eq!(violation.acked_sequence, 50);
        assert_eq!(violation.in_memory_sequence, 40);
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 0);
    }

    #[test]
    fn ack_exactly_at_highest_sent_is_valid() {
        let (min, _, _, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        // Boundary: confirming precisely everything sent is legal.
        cursors
            .record_ack(0, &ack(200, 200), 200)
            .expect("boundary ack is valid");
        assert_eq!(min.load(Ordering::Acquire), 201);
    }
}
