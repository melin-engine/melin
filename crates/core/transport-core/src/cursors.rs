//! Named sequence-space cursors for the durable pipeline.
//!
//! Several atomics answer "how far is the journal?" but they live in
//! **different sequence spaces**, and historically that ambiguity has been a
//! bug factory — the pre-v14 durability gate read an allocator-space value
//! through a variable *named* `journal_persisted_wire_seq`. This module gives
//! each space its own type so the compiler rejects the mix-up:
//!
//! - [`WireSeq`] — the monotonic sequence the journal allocates per durable
//!   event. Shared with replica metrics and `OutputSlot.wire_seq`; comparable
//!   across nodes and stable across `starting_sequence` (a fresh vs recovered
//!   primary). This is what the durability gate compares.
//! - [`RingPos`] — a disruptor consumer's progress counter (slots read). Starts
//!   at `0` every process start and counts *every* input slot (orders, queries,
//!   ticks), so it is **not** comparable to a [`WireSeq`].
//!
//! [`PipelineCursors`] bundles the journal-progress cursors behind accessors
//! that name the space. The two wire-seq atomics are constructed *inside* the
//! bundle (see [`PipelineCursors::new`]) so they can never be cross-wired; the
//! two ring cursors are `Arc<Sequence>` (cache-padded), a different type from
//! any wire-seq handle, so a ring cursor cannot be wired into a wire-seq slot
//! either.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use melin_pipeline::padding::Sequence;

/// Wire-sequence space — defined in the trait crate so `ApplyCtx` can carry
/// it across the application boundary; re-exported here as the canonical
/// home alongside the sibling space types.
pub use melin_app::WireSeq;

/// Ring-index space — see the module docs. A position, not a count.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, Debug)]
pub struct RingPos(u64);

impl RingPos {
    #[inline]
    pub const fn new(pos: u64) -> Self {
        Self(pos)
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Depth between two ring positions (e.g. producer − consumer), saturating
    /// at zero. Returns a raw `u64` because a depth is a count, not a position.
    #[inline]
    pub const fn saturating_sub(self, behind: RingPos) -> u64 {
        self.0.saturating_sub(behind.0)
    }
}

/// Slot-acked space: how the replication sender stores replica progress.
///
/// An engaged slot holds `acked + 1` — "the replica has durably confirmed
/// every sequence below this value" — and a disengaged slot is parked at
/// [`Self::DISENGAGED`]. The encode ([`Self::from_acked`], used by
/// `ReplicaCursors`' store sites) and the decode ([`Self::acked`], used by
/// the quorum and fastest-replica monitoring reads) live on this one type so
/// the `+1` convention cannot drift between the writer and reader modules.
///
/// `#[repr(transparent)]` over `u64`: the cursors themselves stay
/// `AtomicU64`s (shared, lock-free); this type wraps the value at the
/// store/load boundaries.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct SlotAcked(u64);

impl SlotAcked {
    /// Parking value for a slot with no engaged replica. `min`/`max` over
    /// all-disengaged slots yield it back, which is how the shared cursors
    /// signal "no replica" to their readers.
    pub const DISENGAGED: SlotAcked = SlotAcked(u64::MAX);

    /// Encode an acked wire seq into slot-acked space (`acked + 1`).
    /// Saturating: an acked seq of `u64::MAX` is unreachable (the journal
    /// allocator would have to exhaust `u64` first), and saturating keeps
    /// this `const` and panic-free rather than wrapping to a bogus `0`.
    #[inline]
    pub const fn from_acked(acked: WireSeq) -> Self {
        Self(acked.get().saturating_add(1))
    }

    /// Wrap a raw cursor value loaded from one of the shared atomics.
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Unwrap for storing into one of the shared atomics.
    #[inline]
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Decode back to the acked wire seq, or `None` for a disengaged slot.
    /// Engaged writers always store `acked + 1 >= 1`, so the saturation
    /// never engages — it only guards a hypothetical zero store from
    /// reporting a bogus huge value.
    #[inline]
    pub const fn acked(self) -> Option<WireSeq> {
        match self.0 {
            u64::MAX => None,
            cursor => Some(WireSeq::new(cursor.saturating_sub(1))),
        }
    }
}

/// Shared per-slot replica progress cursors, in [`SlotAcked`] space.
///
/// The single source of truth for replica-progress monitoring: the
/// replication sender (`ReplicaCursors`, the sole production writer)
/// stores each slot's acked position here, and readers derive the
/// quorum (`min`) and fastest (`max`) views **at read time**.
///
/// Derive-on-read replaces a previously *published* min/max aggregate
/// pair. Maintaining derived state from multiple writer threads (the two
/// per-slot sender threads plus the accept loop's disconnect path) was a
/// multi-atomic read-modify-write — snapshot both slots, store min, store
/// max — and two concurrent recomputes could interleave last-writer-wins,
/// leaving a stale aggregate published until the next ack. `fetch_min`/
/// `fetch_max` could not fix that because the aggregates must be able to
/// *decrease* (a reconnect lowers the min, a disconnect lowers the max).
/// With no derived state there is nothing to race on: each slot has
/// exactly one writer at a time, and every read observes the latest
/// committed slot stores. Both consumers (health endpoint, Prometheus
/// gauges) are cold read paths, so the two loads per read are free.
pub struct ReplicaSlotCursors {
    /// `[AtomicU64; 2]` rather than per-slot `Arc`s: both slots live on
    /// one cache line of a single shared allocation, and both derived
    /// views need both values anyway.
    slots: [AtomicU64; Self::SLOTS],
}

impl ReplicaSlotCursors {
    /// Number of replica slots. Fixed by the `1 primary + 2 replicas`
    /// topology cap (see `ReplicationMetrics` for the same rationale).
    pub const SLOTS: usize = 2;

    /// Fresh cursors with every slot parked at [`SlotAcked::DISENGAGED`].
    pub fn new() -> Self {
        Self {
            slots: [
                AtomicU64::new(SlotAcked::DISENGAGED.raw()),
                AtomicU64::new(SlotAcked::DISENGAGED.raw()),
            ],
        }
    }

    /// Store a slot's position. `Release` so a reader that observes the
    /// writer's subsequent `active_flag` flip also observes this position
    /// (see the ordering contract in the `replication::cursors` module).
    /// `pub(crate)`: every production store goes through `ReplicaCursors`,
    /// which owns the ack-sanity invariant; only in-crate tests store
    /// directly.
    pub(crate) fn store(&self, slot: usize, value: SlotAcked) {
        self.slots[slot].store(value.raw(), Ordering::Release);
    }

    /// Highest wire seq durably confirmed by *every* engaged replica (the
    /// slowest engaged replica's ack), or `None` while no replica is
    /// engaged. `min` over the slots needs no disengaged handling:
    /// `min(x, DISENGAGED) == x`, and all-disengaged yields the sentinel,
    /// which decodes to `None`.
    #[inline]
    pub fn quorum_acked(&self) -> Option<WireSeq> {
        // `Acquire` pairs with the writer's `Release` stores.
        let a = self.slots[0].load(Ordering::Acquire);
        let b = self.slots[1].load(Ordering::Acquire);
        SlotAcked::from_raw(a.min(b)).acked()
    }

    /// Highest wire seq confirmed by the *fastest* engaged replica, or
    /// `None` while no replica is engaged. Decoding each slot before
    /// taking the max skips disengaged slots (`None < Some(_)`) —
    /// otherwise one parked slot's sentinel would masquerade as the
    /// fastest replica whenever fewer than two replicas are connected.
    #[inline]
    pub fn fastest_acked(&self) -> Option<WireSeq> {
        let a = SlotAcked::from_raw(self.slots[0].load(Ordering::Acquire)).acked();
        let b = SlotAcked::from_raw(self.slots[1].load(Ordering::Acquire)).acked();
        a.max(b)
    }
}

impl Default for ReplicaSlotCursors {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared handle to the durable-wire-seq cursor: the highest wire seq durably
/// persisted on this node's journal. This is the durability gate's `persisted`
/// cursor, the replica reconnect-handshake value, and the health endpoint's
/// `journal_seq` gauge — every consumer reads it through this typed handle so
/// the value never travels as a bare `u64` atomic.
///
/// Single writer: the journal stage [`store`](Self::store)s after each fsync
/// batch (`Release`); readers [`load`](Self::load) with `Acquire`.
#[derive(Clone)]
pub struct DurableWireSeqCursor(Arc<AtomicU64>);

impl DurableWireSeqCursor {
    /// A cursor detached from any pipeline bundle, seeded at `start`. For
    /// stage-level tests and tools that need a handle without building a
    /// pipeline; production wiring obtains handles from
    /// [`PipelineCursors::durable_wire_seq`] so all readers share the journal
    /// stage's atomic.
    pub fn detached(start: WireSeq) -> Self {
        Self(Arc::new(AtomicU64::new(start.get())))
    }

    /// Highest wire seq durably persisted. `Acquire` to pair with the journal
    /// stage's `Release` publish.
    #[inline]
    pub fn load(&self) -> WireSeq {
        WireSeq::new(self.0.load(Ordering::Acquire))
    }

    /// Publish the highest durably-persisted wire seq. Single-writer (journal
    /// stage), `Release` to pair with the readers' `Acquire`.
    #[inline]
    pub fn store(&self, seq: WireSeq) {
        self.0.store(seq.get(), Ordering::Release);
    }
}

/// The journal tip a node advertises on the control plane: the highest
/// contiguous wire seq this node is guaranteed to carry into a promotion.
/// The raft driver pairs it with the fencing epoch to filter vote requests
/// (`melin_raft::recency`), which is what makes an election pick the
/// most-caught-up node.
///
/// The writer changes with the node's data-plane role, which is why this is
/// its own handle rather than an alias of [`DurableWireSeqCursor`]:
///
/// - **Replica** — the replication receiver owns it and advances it to its
///   in-memory *accepted* position (everything published into the input
///   ring), NOT its durable cursor. A promotion drains the ring into the
///   journal before serving, so accepted events survive; advertising only
///   the fsynced position would understate the tip and let a
///   less-caught-up candidate win an election and drop acked orders.
/// - **Primary** — the journal stage publishes its durable cursor into it
///   after each fsync batch (a primary's ring holds client requests that
///   are not yet acked, so the durable cursor is the honest tip).
///
/// `advance` is a monotonic max: phase handoffs (recovery → streaming →
/// promotion → primary tenure) may briefly re-derive a *lower* value (e.g.
/// a reconnect handshake reads the journal before the ring settles), and
/// regressing the advertised tip in that window would grant votes to
/// candidates behind data this node still holds. The one legitimate
/// regression — snapshot resync discarding a *divergent* suffix — goes
/// through the explicit [`reset`](Self::reset) instead: a divergent suffix
/// was never acked (divergence is detected precisely because it left the
/// acked lineage), so dropping it from the tip cannot lose acked data.
#[derive(Debug, Clone)]
pub struct AdvertisedJournalTip(Arc<AtomicU64>);

impl AdvertisedJournalTip {
    /// Fresh handle, seeded at `start` (0 for a node with no journal yet;
    /// the recovered high-water mark otherwise).
    pub fn new(start: WireSeq) -> Self {
        Self(Arc::new(AtomicU64::new(start.get())))
    }

    /// Current advertised tip. `Acquire` to pair with the writers'
    /// `Release` — the control-plane reader is off the hot path.
    #[inline]
    pub fn load(&self) -> WireSeq {
        WireSeq::new(self.0.load(Ordering::Acquire))
    }

    /// Raise the tip to `seq` if higher (monotonic max — see the type docs
    /// for why plain stores are not offered on the advance path).
    #[inline]
    pub fn advance(&self, seq: WireSeq) {
        self.0.fetch_max(seq.get(), Ordering::Release);
    }

    /// Deliberately regress the tip to `seq`. Only for the snapshot-resync
    /// path, where the previously-advertised suffix is known-divergent and
    /// the node's holdings genuinely shrank to the transferred position.
    #[inline]
    pub fn reset(&self, seq: WireSeq) {
        self.0.store(seq.get(), Ordering::Release);
    }
}

/// The journal-progress cursors, bundled with one space-typed accessor each.
///
/// All fields are `Arc`, so the struct is cheap to [`Clone`] for the readers
/// that need a handle (the response-stage gate, the health endpoint, the
/// replica orchestrator). Writers publish through handles cloned from the
/// same `Arc`s: the journal stage Release-stores the durable wire seq after
/// each fsync, the replication sender recomputes the replica quorum cursor on
/// each ack, and the ring counters advance inside `ring::Consumer::commit`.
#[derive(Clone)]
pub struct PipelineCursors {
    /// Highest wire seq durably persisted on this node's journal — held as
    /// the typed handle so the bundle's own reads go through the same single
    /// load path every other consumer uses.
    durable_wire_seq: DurableWireSeqCursor,
    /// Journal consumer's ring progress (slots read), for queue-depth monitoring.
    journal_ring: Arc<Sequence>,
    /// Matching consumer's ring progress (slots read), for queue-depth monitoring.
    matching_ring: Arc<Sequence>,
    /// Per-slot replica progress cursors, maintained by the replication
    /// sender (`ReplicaCursors`) in [`SlotAcked`] space. The quorum
    /// (slowest engaged replica) and fastest-replica views are derived at
    /// read time — see [`ReplicaSlotCursors`]. Every slot stays parked at
    /// [`SlotAcked::DISENGAGED`] until a replica engages, and for the
    /// lifetime of a replica node (no downstream replica to ack it).
    /// Monitoring-only: the durability gate evaluates replica progress
    /// from `ReplicationMetrics`, not from these cursors.
    replica_slots: Arc<ReplicaSlotCursors>,
}

impl PipelineCursors {
    /// Bundle the journal-progress cursors.
    ///
    /// The wire-seq cursors are constructed here — `durable_wire_seq`
    /// from `starting_durable` (the recovered/genesis high-water mark) and
    /// `replica_slots` with every slot disengaged — so no call site can
    /// cross-wire them; writers are handed handles afterwards
    /// ([`durable_wire_seq`](Self::durable_wire_seq) for the journal stage,
    /// [`replica_slot_cursors`](Self::replica_slot_cursors) for the
    /// replication sender). The two ring cursors share a type, so a swap
    /// there is still expressible — but it only skews queue-depth gauges,
    /// not durability.
    pub fn new(
        starting_durable: WireSeq,
        journal_ring: Arc<Sequence>,
        matching_ring: Arc<Sequence>,
    ) -> Self {
        Self {
            durable_wire_seq: DurableWireSeqCursor::detached(starting_durable),
            journal_ring,
            matching_ring,
            replica_slots: Arc::new(ReplicaSlotCursors::new()),
        }
    }

    // ── Typed reads (the safe interface) ───────────────────────────────

    /// Highest wire seq durably persisted. Delegates to
    /// [`DurableWireSeqCursor::load`] so there is a single load path.
    #[inline]
    pub fn load_durable_wire_seq(&self) -> WireSeq {
        self.durable_wire_seq.load()
    }

    /// Matching consumer's ring position. `Relaxed` — monitoring only.
    #[inline]
    pub fn load_matching_ring(&self) -> RingPos {
        RingPos(self.matching_ring.get().load(Ordering::Relaxed))
    }

    /// Highest wire seq durably confirmed by *every* engaged replica (i.e.
    /// the slowest engaged replica's ack), or `None` while no replica is
    /// engaged. Derived from the per-slot cursors at read time — see
    /// [`ReplicaSlotCursors::quorum_acked`].
    #[inline]
    pub fn load_replica_quorum_acked(&self) -> Option<WireSeq> {
        self.replica_slots.quorum_acked()
    }

    /// Highest wire seq confirmed by the *fastest* engaged replica, or
    /// `None` while no replica is engaged. Derived from the per-slot
    /// cursors at read time — see [`ReplicaSlotCursors::fastest_acked`].
    #[inline]
    pub fn load_fastest_replica_acked(&self) -> Option<WireSeq> {
        self.replica_slots.fastest_acked()
    }

    // ── Writer / wiring handles ────────────────────────────────────────

    /// Typed handle to the durable-wire-seq cursor, for the consumers that
    /// keep their own handle (the journal-stage publisher, the response
    /// stage's durability gate, the matching stage's stats reads, the replica
    /// orchestrator's reconnect handshake).
    #[inline]
    pub fn durable_wire_seq(&self) -> DurableWireSeqCursor {
        self.durable_wire_seq.clone()
    }

    // The ring getters below hand the underlying `Arc` to wiring that still
    // reads it directly (the server's seed-drain gate uses `Acquire` loads,
    // stronger than the `Relaxed` monitoring reads above; the replica
    // orchestrator's journal-wait does the same). `Arc<Sequence>` itself
    // names the ring-index space, so these cannot be wired into a wire-seq
    // slot.

    #[inline]
    pub fn journal_ring_arc(&self) -> Arc<Sequence> {
        Arc::clone(&self.journal_ring)
    }

    #[inline]
    pub fn matching_ring_arc(&self) -> Arc<Sequence> {
        Arc::clone(&self.matching_ring)
    }

    /// Shared handle to the per-slot replica cursors, for the replication
    /// sender that maintains them (`ReplicaCursors`, the sole production
    /// writer). Slot-acked space — see the field docs.
    #[inline]
    pub fn replica_slot_cursors(&self) -> Arc<ReplicaSlotCursors> {
        Arc::clone(&self.replica_slots)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursors() -> PipelineCursors {
        PipelineCursors::new(
            WireSeq::new(0),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(Sequence::new(AtomicU64::new(0))),
        )
    }

    #[test]
    fn advertised_tip_advance_is_monotonic_and_reset_is_not() {
        let tip = AdvertisedJournalTip::new(WireSeq::new(10));
        assert_eq!(tip.load(), WireSeq::new(10));
        tip.advance(WireSeq::new(50));
        assert_eq!(tip.load(), WireSeq::new(50));
        // A phase handoff re-deriving a lower value must not regress the
        // tip (see the type docs — a regression would grant votes to
        // candidates behind data this node still holds).
        tip.advance(WireSeq::new(30));
        assert_eq!(tip.load(), WireSeq::new(50));
        // Snapshot resync of a divergent suffix is the one legitimate
        // regression, and it must be explicit.
        tip.reset(WireSeq::new(20));
        assert_eq!(tip.load(), WireSeq::new(20));
    }

    #[test]
    fn advertised_tip_clones_share_the_cursor() {
        let tip = AdvertisedJournalTip::new(WireSeq::new(0));
        let writer = tip.clone();
        writer.advance(WireSeq::new(7));
        assert_eq!(tip.load(), WireSeq::new(7));
    }

    #[test]
    fn durable_wire_seq_round_trips() {
        let c = cursors();
        assert_eq!(c.load_durable_wire_seq(), WireSeq::new(0));
        let handle = c.durable_wire_seq();
        handle.store(WireSeq::new(42));
        assert_eq!(c.load_durable_wire_seq(), WireSeq::new(42));
        // The writer handle observes its own store.
        assert_eq!(handle.load(), WireSeq::new(42));
    }

    #[test]
    fn starting_durable_seeds_the_cursor() {
        let c = PipelineCursors::new(
            WireSeq::new(1_000_000),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(Sequence::new(AtomicU64::new(0))),
        );
        assert_eq!(c.load_durable_wire_seq(), WireSeq::new(1_000_000));
    }

    #[test]
    fn matching_ring_reads_through_the_shared_arc() {
        let c = cursors();
        c.matching_ring_arc().get().store(3, Ordering::Relaxed);
        assert_eq!(c.load_matching_ring(), RingPos::new(3));
    }

    #[test]
    fn replica_views_are_none_until_a_slot_engages() {
        let c = cursors();
        assert_eq!(c.load_replica_quorum_acked(), None);
        assert_eq!(c.load_fastest_replica_acked(), None);
        // Store the way the replication sender does: encoded slot-acked.
        c.replica_slot_cursors()
            .store(0, SlotAcked::from_acked(WireSeq::new(100)));
        assert_eq!(c.load_replica_quorum_acked(), Some(WireSeq::new(100)));
        assert_eq!(c.load_fastest_replica_acked(), Some(WireSeq::new(100)));
    }

    #[test]
    fn quorum_is_the_slowest_and_fastest_the_quickest_engaged_slot() {
        let slots = ReplicaSlotCursors::new();
        slots.store(0, SlotAcked::from_acked(WireSeq::new(500)));
        slots.store(1, SlotAcked::from_acked(WireSeq::new(200)));
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(200)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(500)));
        // One slot disengages: the survivor owns both views — the parked
        // slot's sentinel must not masquerade as the fastest replica.
        slots.store(1, SlotAcked::DISENGAGED);
        assert_eq!(slots.quorum_acked(), Some(WireSeq::new(500)));
        assert_eq!(slots.fastest_acked(), Some(WireSeq::new(500)));
        // Both disengaged: no replica, both views are None.
        slots.store(0, SlotAcked::DISENGAGED);
        assert_eq!(slots.quorum_acked(), None);
        assert_eq!(slots.fastest_acked(), None);
    }

    #[test]
    fn slot_acked_round_trips_and_marks_disengaged() {
        assert_eq!(
            SlotAcked::from_acked(WireSeq::new(0)).acked(),
            Some(WireSeq::new(0)),
            "a replica engaged at seq 0 is engaged, not disengaged"
        );
        assert_eq!(
            SlotAcked::from_acked(WireSeq::new(41)).acked(),
            Some(WireSeq::new(41))
        );
        assert_eq!(SlotAcked::DISENGAGED.acked(), None);
    }

    #[test]
    fn lag_and_depth_saturate() {
        // Lag is a count, never negative.
        assert_eq!(WireSeq::new(100).saturating_sub(WireSeq::new(40)), 60);
        assert_eq!(WireSeq::new(40).saturating_sub(WireSeq::new(100)), 0);
        assert_eq!(RingPos::new(10).saturating_sub(RingPos::new(4)), 6);
    }

    // The spaces deliberately do not inter-convert. The following would not
    // compile, which is the whole point of the newtypes:
    //   let _ = WireSeq::new(1).saturating_sub(RingPos::new(1)); // mismatched types
    //   let _: WireSeq = RingPos::new(1);                        // mismatched types

    mod props {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// Encode/decode round-trip over the engaged range. The upper
            /// bound excludes `u64::MAX - 1`, whose encoding saturates into
            /// the DISENGAGED sentinel — unreachable in practice (the
            /// journal allocator would have to exhaust `u64` first).
            #[test]
            fn slot_acked_round_trips(acked in 0u64..u64::MAX - 1) {
                prop_assert_eq!(
                    SlotAcked::from_acked(WireSeq::new(acked)).acked(),
                    Some(WireSeq::new(acked))
                );
            }

            /// Raw transport through the shared atomics is the identity —
            /// `from_raw`/`raw` add no transformation.
            #[test]
            fn slot_acked_raw_is_identity(raw in proptest::num::u64::ANY) {
                prop_assert_eq!(SlotAcked::from_raw(raw).raw(), raw);
            }
        }
    }
}
