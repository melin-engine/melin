//! Owning module for the primary's view of replica progress.
//!
//! Every store to the per-replica progress cursors — the values the
//! response gate's durability policy and the health endpoint read —
//! goes through [`ReplicaCursors`]. Before this module existed, the
//! same store group (per-slot acked position and the
//! `ReplicationMetrics` gauge pair) was repeated at ~10 call sites
//! across the TCP and DPDK senders, each re-stating the memory-ordering
//! contract in comments. During the pre-v14 vacuous-gate incident,
//! monitoring reported `replica_lag = 0` from these cursors the entire
//! time the durability gate was being satisfied by sequence-space
//! drift — scattered stores are exactly what made that class of bug
//! invisible. One owning module means one place to state the ordering
//! contract and one store site to guard with invariants.
//!
//! ## Cursor spaces
//!
//! - **Slot-acked space** ([`ReplicaSlotCursors`]): `acked_sequence + 1`
//!   — "the replica has durably confirmed every sequence below this
//!   value". `u64::MAX` marks a disengaged slot. The quorum (`min`) and
//!   fastest (`max`) views are *derived at read time* from the per-slot
//!   values — there is no published aggregate, so concurrent writers
//!   cannot race a recompute (see [`ReplicaSlotCursors`] for the full
//!   rationale). A disengaged slot never gates the quorum, and an
//!   all-disengaged store reads as "no replica" on both views.
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
use std::sync::atomic::Ordering;

use tracing::error;

use crate::cursors::{ReplicaSlotCursors, SlotAcked, WireSeq};

use super::metrics::ReplicationMetrics;
use super::protocol::Ack;

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
/// the single-threaded driver loop (DPDK path). All state is atomic and
/// per-slot: writers never store to another slot's entry, and there is
/// no cross-slot aggregate to maintain — readers derive the quorum and
/// fastest views from the slots at read time (see
/// [`ReplicaSlotCursors`]).
pub struct ReplicaCursors {
    /// Per-slot acked positions in slot-acked space, shared with the
    /// monitoring readers (created inside `PipelineCursors` at server
    /// startup), hence `Arc` rather than owned.
    slots: Arc<ReplicaSlotCursors>,
    /// Per-slot wire-ack gauges read by the response gate and health.
    metrics: Arc<ReplicationMetrics>,
}

impl ReplicaCursors {
    /// Wrap the shared per-slot cursors created at server startup,
    /// parking every slot at [`SlotAcked::DISENGAGED`] so a (re)started
    /// sender begins from a clean no-replica state.
    pub fn new(slots: Arc<ReplicaSlotCursors>, metrics: Arc<ReplicationMetrics>) -> Self {
        for slot in 0..ReplicaSlotCursors::SLOTS {
            slots.store(slot, SlotAcked::DISENGAGED);
        }
        Self { slots, metrics }
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
        self.slots.store(
            slot,
            SlotAcked::from_acked(WireSeq::new(handshake_last_sequence)),
        );
    }

    /// Record a replica's `Ack` frame: advance the slot cursor and the
    /// wire-ack gauge pair.
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
        self.slots.store(
            slot,
            SlotAcked::from_acked(WireSeq::new(ack.acked_sequence)),
        );
        Ok(())
    }

    /// Disengage a slot on disconnect or eviction: zero the gauge pair
    /// and park the slot cursor at [`SlotAcked::DISENGAGED`] (not
    /// gating). The derived quorum view immediately re-forms over the
    /// surviving slot — without the park, the quorum would stay frozen
    /// at the departed replica's last ack and the primary would stop
    /// acking client requests even though the surviving replica is
    /// healthy.
    ///
    /// Idempotent, and safe for slots that never engaged (handshake
    /// failures): the gauge pair is already zero and the slot cursor
    /// already parked.
    ///
    /// Ordering: call BEFORE storing `active_flag = false` (`Release`) —
    /// see the module docs.
    pub fn clear_on_disconnect(&self, slot: usize) {
        self.metrics.acked_sequence[slot].store(0, Ordering::Relaxed);
        self.metrics.in_memory_sequence[slot].store(0, Ordering::Relaxed);
        self.slots.store(slot, SlotAcked::DISENGAGED);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (
        Arc<ReplicaSlotCursors>,
        Arc<ReplicationMetrics>,
        ReplicaCursors,
    ) {
        let slots = Arc::new(ReplicaSlotCursors::new());
        let metrics = Arc::new(ReplicationMetrics::default());
        let cursors = ReplicaCursors::new(Arc::clone(&slots), Arc::clone(&metrics));
        (slots, metrics, cursors)
    }

    fn ack(acked: u64, in_memory: u64) -> Ack {
        Ack {
            acked_sequence: acked,
            in_memory_sequence: in_memory,
        }
    }

    #[test]
    fn fresh_store_reads_as_no_replica() {
        let (slots, _, _cursors) = store();
        assert_eq!(slots.quorum_acked(), None);
        assert_eq!(slots.fastest_acked(), None);
    }

    #[test]
    fn new_parks_previously_engaged_slots() {
        let (slots, _, cursors) = store();
        cursors.seed_on_handshake(0, 41);
        // A restarted sender wrapping the same shared cursors must not
        // inherit the previous incarnation's slot state.
        let _cursors =
            ReplicaCursors::new(Arc::clone(&slots), Arc::new(ReplicationMetrics::default()));
        assert_eq!(slots.quorum_acked(), None);
        assert_eq!(slots.fastest_acked(), None);
    }

    #[test]
    fn seed_engages_both_views_at_the_single_slot() {
        let (slots, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 41);
        // Slot 0 is the only engaged replica, so it is simultaneously the
        // slowest (quorum) and the fastest at 41. The disengaged slot 1
        // must not leak its parking sentinel into the fastest view.
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(41)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(41)));
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 41);
        assert_eq!(metrics.in_memory_sequence[0].load(Ordering::Relaxed), 41);
    }

    #[test]
    fn record_ack_advances_gauges_and_derived_views() {
        let (slots, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.seed_on_handshake(1, 0);
        cursors.record_ack(0, &ack(10, 15), 15).expect("valid ack");
        cursors.record_ack(1, &ack(7, 12), 12).expect("valid ack");
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 10);
        assert_eq!(metrics.in_memory_sequence[0].load(Ordering::Relaxed), 15);
        assert_eq!(metrics.acked_sequence[1].load(Ordering::Relaxed), 7);
        assert_eq!(metrics.in_memory_sequence[1].load(Ordering::Relaxed), 12);
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(7)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(10)));
    }

    #[test]
    fn second_replica_joining_behind_lowers_the_quorum_then_catches_up() {
        let (slots, _, cursors) = store();
        cursors.seed_on_handshake(0, 100);
        cursors
            .record_ack(0, &ack(500, 500), 500)
            .expect("valid ack");
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(500)));
        // A fresh replica joins having only caught up to 200 — the
        // quorum must DECREASE. (With a published aggregate this needed
        // plain stores, not fetch_max; derived views make it automatic.)
        cursors.seed_on_handshake(1, 200);
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(200)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(500)));
        // It catches up partially, then fully; the quorum tracks it
        // until the two slots converge.
        cursors
            .record_ack(1, &ack(350, 350), 500)
            .expect("valid ack");
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(350)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(500)));
        cursors
            .record_ack(1, &ack(500, 500), 500)
            .expect("valid ack");
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(500)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(500)));
    }

    #[test]
    fn disconnect_zeroes_gauges_and_releases_the_quorum_to_the_survivor() {
        let (slots, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.seed_on_handshake(1, 0);
        cursors.record_ack(0, &ack(10, 15), 15).expect("valid ack");
        cursors.record_ack(1, &ack(7, 12), 12).expect("valid ack");
        cursors.clear_on_disconnect(1);
        assert_eq!(metrics.acked_sequence[1].load(Ordering::Relaxed), 0);
        assert_eq!(metrics.in_memory_sequence[1].load(Ordering::Relaxed), 0);
        // Survivor (slot 0, acked 10) owns both views — the fastest must
        // DECREASE back to the survivor, not track the departed slot.
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(10)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(10)));
    }

    #[test]
    fn disconnect_of_last_replica_reads_as_no_replica() {
        let (slots, _, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.record_ack(0, &ack(10, 15), 15).expect("valid ack");
        cursors.clear_on_disconnect(0);
        assert_eq!(slots.quorum_acked(), None);
        assert_eq!(slots.fastest_acked(), None);
    }

    #[test]
    fn disconnect_of_never_engaged_slot_is_a_safe_noop() {
        let (slots, metrics, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        cursors.record_ack(0, &ack(10, 15), 15).expect("valid ack");
        // Slot 1 fails its handshake without ever engaging.
        cursors.clear_on_disconnect(1);
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(10)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(10)));
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 10);
    }

    #[test]
    fn valid_acks_count_toward_acks_received() {
        let (_, metrics, cursors) = store();
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
        let (slots, metrics, cursors) = store();
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
        // (slot 0 is the only engaged replica, so it owns both views).
        assert_eq!(metrics.acked_sequence[0].load(Ordering::Relaxed), 100);
        assert_eq!(metrics.in_memory_sequence[0].load(Ordering::Relaxed), 100);
        assert_eq!(metrics.acks_received[0].load(Ordering::Relaxed), 0);
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(100)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(100)));
    }

    #[test]
    fn persisted_track_ahead_of_in_memory_is_rejected() {
        let (_, metrics, cursors) = store();
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
        let (slots, _, cursors) = store();
        cursors.seed_on_handshake(0, 0);
        // Boundary: confirming precisely everything sent is legal.
        cursors
            .record_ack(0, &ack(200, 200), 200)
            .expect("boundary ack is valid");
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(200)));
    }

    /// Two `repl-{slot}` threads updating their slots at the same
    /// instant must never leave the derived views stale: after each
    /// concurrent round settles, the quorum/fastest views equal min/max
    /// over the two slots. This pins the derive-on-read design — a
    /// reintroduced published aggregate would race its recompute
    /// last-writer-wins and fail here within a few rounds.
    #[test]
    fn concurrent_slot_updates_never_leave_the_views_stale() {
        use std::sync::Barrier;

        let (slots, _metrics, cursors) = store();
        let cursors = Arc::new(cursors);
        // Engage both slots so every round has two live cursors.
        cursors.seed_on_handshake(0, 0);
        cursors.seed_on_handshake(1, 0);

        const ROUNDS: u64 = 2_000;
        // 2 workers + the checker (main): all three rendezvous each round
        // so the two record_ack calls overlap, then the checker reads only
        // once both stores have returned (quiescent, so no transient lag).
        let start = Arc::new(Barrier::new(3));
        let done = Arc::new(Barrier::new(3));

        let workers: Vec<_> = (0..2usize)
            .map(|slot| {
                let cursors = Arc::clone(&cursors);
                let start = Arc::clone(&start);
                let done = Arc::clone(&done);
                // Distinct per-round steps keep the quorum and fastest
                // views on different slots, so staleness on either view
                // is observable.
                let step = if slot == 0 { 2 } else { 1 };
                std::thread::spawn(move || {
                    for round in 1..=ROUNDS {
                        start.wait();
                        let v = round * step;
                        cursors
                            .record_ack(slot, &ack(v, v), v)
                            .expect("monotonic ack within highest_sent");
                        done.wait();
                    }
                })
            })
            .collect();

        for round in 1..=ROUNDS {
            start.wait();
            done.wait();
            // Slot 0 is at 2·round, slot 1 at round.
            assert_eq!(
                slots.quorum_acked(),
                Some(WireSeq::new(round)),
                "quorum view stale at round {round}"
            );
            assert_eq!(
                slots.fastest_acked(),
                Some(WireSeq::new(2 * round)),
                "fastest view stale at round {round}"
            );
        }

        for w in workers {
            w.join().expect("worker thread");
        }
    }

    mod props {
        use super::*;
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        enum Op {
            Seed { slot: usize, last: u64 },
            Ack { slot: usize, advance: u64 },
            Disconnect { slot: usize },
        }

        fn op_strategy() -> impl Strategy<Value = Op> {
            prop_oneof![
                (0usize..2, 0u64..1_000).prop_map(|(slot, last)| Op::Seed { slot, last }),
                (0usize..2, 0u64..100).prop_map(|(slot, advance)| Op::Ack { slot, advance }),
                (0usize..2usize).prop_map(|slot| Op::Disconnect { slot }),
            ]
        }

        proptest! {
            /// Model check: after every step of an arbitrary connect /
            /// ack / disconnect lifecycle, the derived views equal
            /// min/max over the *engaged* slots' acked positions, or
            /// `None` when no slot is engaged. Pins both the `SlotAcked`
            /// encoding at the store sites and the disengaged-slot
            /// exclusion in the fastest view.
            #[test]
            fn derived_views_track_engaged_min_max(
                ops in proptest::collection::vec(op_strategy(), 1..40)
            ) {
                let slots = Arc::new(ReplicaSlotCursors::new());
                let metrics = Arc::new(ReplicationMetrics::default());
                let cursors = ReplicaCursors::new(Arc::clone(&slots), metrics);

                // Model: each engaged slot's acked wire seq.
                let mut engaged: [Option<u64>; 2] = [None, None];

                for op in ops {
                    match op {
                        Op::Seed { slot, last } => {
                            cursors.seed_on_handshake(slot, last);
                            engaged[slot] = Some(last);
                        }
                        Op::Ack { slot, advance } => {
                            // Acks are only meaningful on an engaged slot;
                            // keep them monotonic (the senders' SentHighWater
                            // guarantees this) and pass a generous
                            // highest_sent so validity never trips.
                            if let Some(acked) = engaged[slot] {
                                let next = acked + advance;
                                cursors
                                    .record_ack(slot, &ack(next, next), u64::MAX - 1)
                                    .expect("monotonic ack within highest_sent");
                                engaged[slot] = Some(next);
                            }
                        }
                        Op::Disconnect { slot } => {
                            cursors.clear_on_disconnect(slot);
                            engaged[slot] = None;
                        }
                    }

                    let expect_quorum =
                        engaged.iter().flatten().min().map(|&a| WireSeq::new(a));
                    let expect_fastest =
                        engaged.iter().flatten().max().map(|&a| WireSeq::new(a));
                    prop_assert_eq!(slots.quorum_acked(), expect_quorum);
                    prop_assert_eq!(slots.fastest_acked(), expect_fastest);
                }
            }
        }
    }
}
