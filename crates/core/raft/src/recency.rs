//! Journal-tip recency filter for vote requests.
//!
//! Melin replicates event data out-of-band (the replication data plane),
//! so Raft's own log-recency vote check says nothing about which node
//! holds the most event data. Left unmodified, Raft could elect a node
//! whose journal is behind, and auto-promotion would then lose acked
//! events. The fix — the same shape as MongoDB's election over its
//! out-of-band oplog, PacificA, and Viewstamped Replication — extends
//! the vote rule: candidates advertise their journal tip in the RPC
//! envelope, and a voter **drops** vote requests from candidates behind
//! its own tip.
//!
//! Filtering happens at the RPC boundary ([`crate::rpc_server`]), *before*
//! the request reaches `Raft::vote` — openraft exposes no vote-filter
//! hook, and none is needed: a dropped vote request is indistinguishable
//! from packet loss to Raft, so it can never violate Raft safety.
//!
//! **Liveness needs an escape hatch.** The journal-tip order and Raft's
//! own log-recency vote rule are *independent* orderings applied
//! conjunctively: a candidate must be delivered (tip order) *and*
//! granted (Raft log order). After a control-plane-only leadership
//! change, the old data-plane primary can hold the highest journal tip
//! but the older Raft log while a peer holds the newer log but a lower,
//! frozen tip — each vetoes the other and the cluster sits leaderless
//! indefinitely, quietly (dropped votes inflate no terms). So the
//! filter is a *stateful gate* ([`VoteFilter`]): after
//! [`LIVENESS_ESCAPE_DROPS`] consecutively dropped requests with no
//! leader observed, it stops filtering until a leader is next seen,
//! letting Raft's own rules elect whoever they can. The filter is
//! therefore best-effort steering, not a guarantee: the authoritative
//! journal-safety check belongs at *promotion* time, never at the
//! ballot box. That check exists — the auto-promotion policy in
//! `melin-server-runtime` refuses to promote while a recently-heard
//! peer advertises a higher tip, fed by [`PeerTips`] below — so a
//! behind node that wins here (via the escape, or by holding
//! leadership from before the failure) stalls loudly instead of
//! promoting, and nudges the caught-up peer to campaign.
//!
//! **Every envelope is stamped with its place in the sender's order**
//! ([`SenderStamp`]), so [`PeerTips`] can tell a delayed frame from a
//! current one. A peer may hold several connections to this node (openraft
//! opens a fresh client per voter to confirm leadership), so a frame sent
//! before another can arrive after it, and recording it would regress the
//! peer's tip while making it look freshly heard: the promotion check
//! would then see a faster replica as level with a slower one.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use melin_transport_core::boot_stamp::BootStamp;
use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;
use serde::Deserialize;
use serde::Serialize;

/// A node's journal tip as advertised in the control-plane RPC envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalTip {
    /// Fencing epoch in force at the tip (see `docs/replication.md`,
    /// "Fencing epochs").
    pub epoch: u64,
    /// Last journal sequence the node holds.
    pub last_sequence: u64,
}

impl JournalTip {
    /// Total order on tips: epoch first, then sequence.
    ///
    /// Epoch dominates because a higher epoch marks a newer primary
    /// tenure — a node still on an older epoch may hold a *longer* but
    /// divergent (never-acked) suffix from a deposed primary, and its
    /// raw sequence must not outrank the newer lineage. Within an epoch,
    /// sequences are totally ordered by the single-writer journal.
    fn key(self) -> (u64, u64) {
        (self.epoch, self.last_sequence)
    }

    /// Strictly ahead of `other` in the (epoch, sequence) tip order —
    /// the comparison the promotion-time journal-safety veto uses.
    pub fn is_ahead_of(self, other: JournalTip) -> bool {
        self.key() > other.key()
    }
}

/// Where a frame sits in its sender's order: which process of the sender
/// sent it (the host boot and the process start, from the sender's
/// [`BootStamp`]) and the sender's per-frame counter. Carried in every
/// envelope, request and reply, beside the tip it orders.
///
/// The counter is per process and shared by every frame the process sends,
/// on any connection, requests and replies alike, so frames from one
/// process are totally ordered by it; a later process start on the same
/// boot outranks every frame of an earlier one. `u64`: one value per frame
/// sent, which no node sends enough of to wrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SenderStamp {
    /// The sender's host boot id ([`BootStamp::boot_id`]).
    pub boot_id: [u8; 16],
    /// The sender's process start ([`BootStamp::started_ns`]).
    pub started_ns: u64,
    /// The sender's frame counter, taken with the tip it stamps.
    pub counter: u64,
}

impl SenderStamp {
    /// Whether a frame stamped `self` is newer than one stamped `prior`,
    /// both from the same peer.
    ///
    /// Another boot id always is: the host rebooted, so the kernel that
    /// sent `prior` is gone and nothing from it can still be in flight.
    /// (The order between two boots is unknowable, and not needed.) Within
    /// one boot, a later process start wins, then a higher counter. An
    /// equal stamp is not newer: it is the same frame.
    pub fn is_newer_than(&self, prior: &SenderStamp) -> bool {
        if self.boot_id != prior.boot_id {
            return true;
        }
        (self.started_ns, self.counter) > (prior.started_ns, prior.counter)
    }
}

/// A journal tip with the stamp of the frame that carried it: read
/// together by the sender (`TipSource::stamp`), recorded together by the
/// receiver ([`PeerTips::record`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StampedTip {
    pub tip: JournalTip,
    pub stamp: SenderStamp,
}

/// Live source of this node's advertised journal tip, read by the RPC
/// layer for every outgoing envelope and every vote-filter decision.
/// Both halves are owned by the data plane (fencing state; journal /
/// replication-receiver cursor) — the control plane only ever reads.
pub struct TipSource {
    pub fence: Arc<FenceState>,
    pub seq: AdvertisedJournalTip,
    /// `true` once journal recovery has seeded both halves. Until then
    /// the advertised tip (and any vote this node might grant) would be
    /// built on a default epoch/sequence — so the RPC server refuses to
    /// deliver vote requests while this is `false`.
    pub ready: Arc<AtomicBool>,
    /// This process's boot stamp, on every frame it sends.
    boot: BootStamp,
    /// The next frame's counter. A `Mutex` rather than an `AtomicU64`: the
    /// counter must be taken in the same critical section as the tip's two
    /// loads, or a frame stamped later could carry a tip read earlier and
    /// the receiver would keep the older tip as the newer. The lock is held
    /// for those two loads and an increment, on the raft send path (every
    /// request and reply, a few per heartbeat per peer), never on the event
    /// hot path, and never across an `.await`.
    next_counter: Mutex<u64>,
}

impl TipSource {
    /// A tip source over the data plane's fence and advertised sequence,
    /// stamping its frames with `boot`, this process's stamp
    /// ([`BootStamp::read`] at startup).
    pub fn new(
        fence: Arc<FenceState>,
        seq: AdvertisedJournalTip,
        ready: Arc<AtomicBool>,
        boot: BootStamp,
    ) -> Self {
        Self {
            fence,
            seq,
            ready,
            boot,
            next_counter: Mutex::new(0),
        }
    }

    /// This node's tip now, unstamped: the local side of the vote filter's
    /// comparison. Every outgoing envelope takes `TipSource::stamp`
    /// instead.
    pub fn local_tip(&self) -> JournalTip {
        JournalTip {
            epoch: self.fence.epoch(),
            last_sequence: self.seq.load().get(),
        }
    }

    /// This node's tip and the next frame counter, taken together: the one
    /// place an outgoing envelope's tip is read. Counter order is tip-read
    /// order, so a frame with a higher counter never carries an older tip
    /// (the tip only rises, but for a resync's explicit reset).
    pub(crate) fn stamp(&self) -> StampedTip {
        // Poisoning unreachable under panic=abort (no unwinding).
        let mut next = self
            .next_counter
            .lock()
            .expect("frame counter mutex poisoned");
        let tip = self.local_tip();
        let counter = *next;
        *next += 1;
        StampedTip {
            tip,
            stamp: SenderStamp {
                boot_id: self.boot.boot_id(),
                started_ns: self.boot.started_ns(),
                counter,
            },
        }
    }

    pub fn is_ready(&self) -> bool {
        // Acquire pairs with the recovery path's Release store, which
        // happens after both halves are seeded.
        self.ready.load(Ordering::Acquire)
    }
}

/// Last journal tip heard from each configured peer, with when it was
/// heard. Every control-plane RPC envelope — request and response —
/// carries the sender's tip, so this fills from both directions: the
/// RPC server records inbound frames per authenticated peer, and the
/// RPC client records every reply from its target. A leader therefore
/// learns its followers' tips within one heartbeat round, and a
/// follower learns its leader's tip from every append.
///
/// Read by the auto-promotion policy in `melin-server-runtime`: the
/// promotion-time journal-safety check the vote filter above defers to.
///
/// Only a frame newer in its sender's order than the one already recorded
/// ([`SenderStamp::is_newer_than`]) is recorded; an older one is dropped,
/// tip and arrival time alike, so a delayed frame neither regresses the
/// peer's tip nor keeps a dead peer looking freshly heard.
pub struct PeerTips {
    /// `Mutex<HashMap>` rather than anything lock-free: written a few
    /// times per heartbeat interval per peer and read once per 100 ms
    /// promotion poll, so contention is irrelevant; and the map holds
    /// 2–4 entries (the configured voter set), so `HashMap` vs anything
    /// smarter is noise. Never touched by the data plane.
    inner: std::sync::Mutex<std::collections::HashMap<crate::types::NodeId, TipSample>>,
}

/// One recorded observation: the advertised tip, the stamp of the frame
/// that carried it, and when it arrived.
#[derive(Debug, Clone, Copy)]
struct TipSample {
    tip: StampedTip,
    at: std::time::Instant,
}

impl PeerTips {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Record a tip heard from `peer` now, if its frame is newer than the
    /// last one recorded from `peer`. Returns whether it was recorded.
    ///
    /// The arrival time is read before the lock is taken. Every caller runs
    /// on the raft driver's current-thread runtime, with no `.await`
    /// between the two, so no other record falls in between.
    pub fn record(&self, peer: crate::types::NodeId, tip: StampedTip) -> bool {
        self.record_at(peer, tip, std::time::Instant::now())
    }

    /// Deterministic-time seam for `record` — tests inject the arrival
    /// instant so aging is assertable without sleeping.
    pub fn record_at(
        &self,
        peer: crate::types::NodeId,
        tip: StampedTip,
        at: std::time::Instant,
    ) -> bool {
        use std::collections::hash_map::Entry;
        // Poisoning unreachable under panic=abort (no unwinding).
        let mut map = self.inner.lock().expect("peer tips mutex poisoned");
        match map.entry(peer) {
            Entry::Occupied(mut held) => {
                let recorded = held.get().tip.stamp;
                if !tip.stamp.is_newer_than(&recorded) {
                    // `debug!`: a delayed frame is a network event, not a
                    // malfunction of either node. Both process starts are
                    // logged: a dropped frame always shares the recorded
                    // boot id (another one is always newer), so a peer whose
                    // every frame is dropped, its clock rewound by a restore,
                    // shows here as a lower `started_ns`.
                    tracing::debug!(
                        peer,
                        started_ns = tip.stamp.started_ns,
                        recorded_started_ns = recorded.started_ns,
                        counter = tip.stamp.counter,
                        recorded_counter = recorded.counter,
                        "raft frame older than one already heard from this peer — its tip is not recorded"
                    );
                    return false;
                }
                held.insert(TipSample { tip, at });
            }
            Entry::Vacant(slot) => {
                slot.insert(TipSample { tip, at });
            }
        }
        true
    }

    /// The last tip heard from `peer` and how long ago it arrived
    /// (relative to `now`), or `None` if the peer has never been heard.
    pub fn sample(
        &self,
        peer: crate::types::NodeId,
        now: std::time::Instant,
    ) -> Option<(JournalTip, std::time::Duration)> {
        // Poisoning unreachable under panic=abort (no unwinding).
        let map = self.inner.lock().expect("peer tips mutex poisoned");
        map.get(&peer)
            .map(|s| (s.tip.tip, now.saturating_duration_since(s.at)))
    }
}

impl Default for PeerTips {
    fn default() -> Self {
        Self::new()
    }
}

/// Voter-side recency rule: should a vote request from a candidate
/// advertising `candidate` be delivered to the local Raft node, given
/// our own `local` tip?
///
/// `true` when the candidate is at least as caught up as we are.
/// Callers apply this to vote requests only — appends/heartbeats must
/// never be filtered, or a legitimately elected leader could not lead.
#[inline]
pub fn candidate_is_current(candidate: JournalTip, local: JournalTip) -> bool {
    candidate.key() >= local.key()
}

/// Consecutively dropped vote requests after which a voter stops
/// filtering until it next observes a leader. A deadlocked candidate
/// re-campaigns roughly once per randomized election timeout (1–2 s
/// with the [`crate::driver`] defaults), so the escape opens after
/// ~10–15 s of provable leaderlessness — far longer than any healthy
/// election, far shorter than an operator page.
pub const LIVENESS_ESCAPE_DROPS: u32 = 8;

/// Voter-side stateful gate applying [`candidate_is_current`] with the
/// liveness escape described in the module docs. One per local node
/// (the state is *this voter's* view of election progress).
#[derive(Debug, Default)]
pub struct VoteFilter {
    /// Vote requests dropped since a leader was last observed. Saturates
    /// at [`LIVENESS_ESCAPE_DROPS`]; `u32` (not `u8`) only to make the
    /// constant's type unremarkable — the count never exceeds the limit.
    consecutive_drops: u32,
}

impl VoteFilter {
    /// Should a vote request advertising `candidate` be delivered to
    /// the local Raft node, given our own `local` tip?
    pub fn should_deliver(&mut self, candidate: JournalTip, local: JournalTip) -> bool {
        if candidate_is_current(candidate, local) {
            return true;
        }
        if self.consecutive_drops >= LIVENESS_ESCAPE_DROPS {
            // Escape open: the tip order has blocked every candidate
            // for several election timeouts — defer to Raft's own vote
            // rules rather than stay leaderless.
            return true;
        }
        self.consecutive_drops += 1;
        if self.consecutive_drops == LIVENESS_ESCAPE_DROPS {
            tracing::warn!(
                drops = self.consecutive_drops,
                "journal-tip vote filter blocked every election — \
                 disabling it until a leader emerges (a behind node may \
                 win; promotion-time checks remain authoritative)"
            );
        }
        false
    }

    /// A live leader was observed (an append/heartbeat arrived, or the
    /// local raft reports one) — elections are working, so re-arm the
    /// filter.
    pub fn leader_observed(&mut self) {
        self.consecutive_drops = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tip(epoch: u64, seq: u64) -> JournalTip {
        JournalTip {
            epoch,
            last_sequence: seq,
        }
    }

    #[test]
    fn equal_tips_pass() {
        assert!(candidate_is_current(tip(3, 100), tip(3, 100)));
    }

    /// The boot id of the sender's host in these tests.
    const BOOT: [u8; 16] = [7; 16];

    /// `tip` as stamped by the process of boot `boot_id` started at
    /// `started_ns`, on its frame `counter`.
    fn stamped_by(boot_id: [u8; 16], started_ns: u64, counter: u64, tip: JournalTip) -> StampedTip {
        StampedTip {
            tip,
            stamp: SenderStamp {
                boot_id,
                started_ns,
                counter,
            },
        }
    }

    /// `tip` on frame `counter` of one sender process.
    fn stamped(counter: u64, tip: JournalTip) -> StampedTip {
        stamped_by(BOOT, 1_000, counter, tip)
    }

    #[test]
    fn peer_tips_sample_returns_latest_with_age() {
        use std::time::{Duration, Instant};
        let tips = PeerTips::new();
        let t0 = Instant::now();
        assert!(tips.sample(2, t0).is_none(), "never-heard peer is None");

        assert!(tips.record_at(2, stamped(1, tip(1, 10)), t0));
        // A later observation replaces the earlier one entirely.
        assert!(tips.record_at(2, stamped(2, tip(1, 42)), t0 + Duration::from_millis(500)));

        let (t, age) = tips.sample(2, t0 + Duration::from_secs(2)).expect("sample");
        assert_eq!(t, tip(1, 42));
        assert_eq!(age, Duration::from_millis(1500));

        // Peers are independent.
        assert!(tips.sample(3, t0 + Duration::from_secs(2)).is_none());
    }

    /// A frame sent before the one already recorded, arriving after it (on
    /// another connection), is dropped: the tip does not regress, and the
    /// arrival does not refresh the peer's last-heard time.
    #[test]
    fn an_older_frame_is_dropped_and_does_not_refresh_the_peer() {
        use std::time::{Duration, Instant};
        let tips = PeerTips::new();
        let t0 = Instant::now();
        assert!(tips.record_at(2, stamped(5, tip(3, 100)), t0));

        let late = t0 + Duration::from_secs(10);
        assert!(!tips.record_at(2, stamped(4, tip(3, 90)), late));
        let (t, age) = tips.sample(2, late).expect("sample");
        assert_eq!(t, tip(3, 100), "the tip must not regress");
        assert_eq!(age, Duration::from_secs(10), "the peer must not look fresh");

        // A duplicate of the recorded frame is not newer either.
        assert!(!tips.record_at(2, stamped(5, tip(3, 100)), late));
        assert_eq!(
            tips.sample(2, late).expect("sample").1,
            Duration::from_secs(10)
        );
    }

    #[test]
    fn a_newer_frame_is_recorded() {
        use std::time::{Duration, Instant};
        let tips = PeerTips::new();
        let t0 = Instant::now();
        assert!(tips.record_at(2, stamped(5, tip(3, 100)), t0));
        // Newer even with an equal tip: an idle peer's heartbeat refreshes it.
        let t1 = t0 + Duration::from_secs(1);
        assert!(tips.record_at(2, stamped(6, tip(3, 100)), t1));
        assert_eq!(tips.sample(2, t1), Some((tip(3, 100), Duration::ZERO)));
        // Counters need not be consecutive (frames to other peers take some).
        let t2 = t0 + Duration::from_secs(2);
        assert!(tips.record_at(2, stamped(60, tip(3, 120)), t2));
        assert_eq!(tips.sample(2, t2), Some((tip(3, 120), Duration::ZERO)));
    }

    /// The peer's host rebooted: its new process starts its counter over,
    /// at a boot clock reading that may be lower than the old one's, and is
    /// recorded all the same, replacing the stored boot.
    #[test]
    fn a_new_boot_id_replaces_the_stored_one() {
        use std::time::{Duration, Instant};
        let tips = PeerTips::new();
        let t0 = Instant::now();
        let rebooted = [8; 16];
        assert!(tips.record_at(2, stamped_by(BOOT, 9_000, 500, tip(3, 100)), t0));

        let t1 = t0 + Duration::from_secs(1);
        assert!(tips.record_at(2, stamped_by(rebooted, 10, 0, tip(3, 100)), t1));
        assert_eq!(tips.sample(2, t1), Some((tip(3, 100), Duration::ZERO)));

        // From then on the new boot's own order applies.
        let t2 = t0 + Duration::from_secs(2);
        assert!(!tips.record_at(2, stamped_by(rebooted, 5, 900, tip(3, 50)), t2));
        assert!(tips.record_at(2, stamped_by(rebooted, 10, 1, tip(3, 101)), t2));
        assert_eq!(tips.sample(2, t2), Some((tip(3, 101), Duration::ZERO)));
    }

    /// The peer restarted on the same host boot: its new process wins over
    /// every frame of the old one, whatever their counters.
    #[test]
    fn a_later_process_start_on_the_same_boot_wins() {
        use std::time::{Duration, Instant};
        let tips = PeerTips::new();
        let t0 = Instant::now();
        assert!(tips.record_at(2, stamped_by(BOOT, 1_000, 500, tip(3, 100)), t0));

        // The restarted process: a lower counter, a later start.
        let t1 = t0 + Duration::from_secs(1);
        assert!(tips.record_at(2, stamped_by(BOOT, 2_000, 0, tip(3, 100)), t1));

        // A stale frame of the old process, however high its counter.
        let t2 = t0 + Duration::from_secs(2);
        assert!(!tips.record_at(2, stamped_by(BOOT, 1_000, 501, tip(3, 90)), t2));
        assert_eq!(
            tips.sample(2, t2),
            Some((tip(3, 100), Duration::from_secs(1)))
        );
    }

    fn source_at(epoch: u64, seq: u64) -> TipSource {
        let fence = Arc::new(FenceState::new(0));
        fence.observe_epoch(epoch);
        TipSource::new(
            fence,
            AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(seq)),
            Arc::new(AtomicBool::new(true)),
            BootStamp::from_parts(BOOT, 1_000),
        )
    }

    #[test]
    fn a_stamp_carries_the_boot_the_current_tip_and_the_next_counter() {
        let source = source_at(3, 10);
        assert_eq!(source.stamp(), stamped(0, tip(3, 10)));
        source.seq.advance(melin_transport_core::WireSeq::new(11));
        assert_eq!(source.stamp(), stamped(1, tip(3, 11)));
        source.fence.observe_epoch(4);
        assert_eq!(source.stamp(), stamped(2, tip(4, 11)));
    }

    /// The counter and the tip are taken together: with the tip advancing
    /// under several stamping threads, ordering every stamp by its counter
    /// orders the tips too. A counter taken apart from the tip read would
    /// let a later counter carry an earlier read.
    #[test]
    fn counter_order_is_tip_read_order_under_concurrent_stamping() {
        const STAMPERS: usize = 4;
        const PER_STAMPER: usize = 5_000;
        let source = Arc::new(source_at(0, 0));
        let advancing = Arc::new(AtomicBool::new(true));
        let advancer = {
            let source = Arc::clone(&source);
            let advancing = Arc::clone(&advancing);
            std::thread::spawn(move || {
                let mut seq = 0;
                while advancing.load(Ordering::Relaxed) {
                    seq += 1;
                    source.seq.advance(melin_transport_core::WireSeq::new(seq));
                    if seq % 64 == 0 {
                        source.fence.observe_epoch(seq / 64);
                    }
                }
            })
        };
        let stampers: Vec<_> = (0..STAMPERS)
            .map(|_| {
                let source = Arc::clone(&source);
                std::thread::spawn(move || {
                    (0..PER_STAMPER).map(|_| source.stamp()).collect::<Vec<_>>()
                })
            })
            .collect();
        let mut stamps: Vec<StampedTip> = stampers
            .into_iter()
            .flat_map(|s| s.join().unwrap())
            .collect();
        advancing.store(false, Ordering::Relaxed);
        advancer.join().unwrap();

        stamps.sort_by_key(|s| s.stamp.counter);
        // Every counter once: 0..n in order.
        assert!(
            stamps
                .iter()
                .enumerate()
                .all(|(i, s)| s.stamp.counter == i as u64)
        );
        for pair in stamps.windows(2) {
            assert!(
                !pair[0].tip.is_ahead_of(pair[1].tip),
                "frame {} carries {:?}, ahead of frame {}'s {:?}",
                pair[0].stamp.counter,
                pair[0].tip,
                pair[1].stamp.counter,
                pair[1].tip
            );
        }
    }

    #[test]
    fn peer_tips_age_saturates_for_out_of_order_clock_reads() {
        use std::time::{Duration, Instant};
        let tips = PeerTips::new();
        let t0 = Instant::now();
        tips.record_at(2, stamped(1, tip(1, 10)), t0 + Duration::from_secs(1));
        // `now` earlier than the record instant (racing clock reads
        // between threads) must yield age zero, not a panic.
        let (_, age) = tips.sample(2, t0).expect("sample");
        assert_eq!(age, Duration::ZERO);
    }

    #[test]
    fn candidate_ahead_passes() {
        assert!(candidate_is_current(tip(3, 101), tip(3, 100)));
        assert!(candidate_is_current(tip(4, 0), tip(3, 100)));
    }

    #[test]
    fn candidate_behind_is_rejected() {
        assert!(!candidate_is_current(tip(3, 99), tip(3, 100)));
    }

    #[test]
    fn epoch_dominates_sequence() {
        // A long suffix on an old epoch is a divergent lineage, not
        // recency — the newer-epoch node must win.
        assert!(!candidate_is_current(tip(2, 1_000_000), tip(3, 10)));
        assert!(candidate_is_current(tip(3, 10), tip(2, 1_000_000)));
    }

    #[test]
    fn filter_escape_opens_after_sustained_drops_and_rearms_on_leader() {
        let mut f = VoteFilter::default();
        let behind = tip(3, 99);
        let local = tip(3, 100);

        // Current candidates always pass, without disturbing the count.
        assert!(f.should_deliver(tip(3, 100), local));

        // The first LIVENESS_ESCAPE_DROPS behind-requests are dropped…
        for i in 0..LIVENESS_ESCAPE_DROPS {
            assert!(!f.should_deliver(behind, local), "drop {i}");
        }
        // …then the escape opens and stays open.
        assert!(f.should_deliver(behind, local));
        assert!(f.should_deliver(behind, local));

        // Observing a leader re-arms the filter.
        f.leader_observed();
        assert!(!f.should_deliver(behind, local));
    }

    proptest::proptest! {
        /// `candidate_is_current` is monotone: raising the candidate's
        /// tip (in tip order) never turns an admitted vote into a drop.
        #[test]
        fn admission_is_monotone_in_candidate_tip(
            ce in 0u64..8, cs in 0u64..1000, le in 0u64..8, ls in 0u64..1000
        ) {
            let local = tip(le, ls);
            if candidate_is_current(tip(ce, cs), local) {
                proptest::prop_assert!(candidate_is_current(tip(ce, cs + 1), local));
                proptest::prop_assert!(candidate_is_current(tip(ce + 1, 0), local));
            }
        }

        /// The escape hatch always opens: for any candidate/local pair,
        /// at most LIVENESS_ESCAPE_DROPS requests are dropped before
        /// delivery resumes.
        #[test]
        fn liveness_escape_always_unblocks(
            ce in 0u64..8, cs in 0u64..1000, le in 0u64..8, ls in 0u64..1000
        ) {
            let mut f = VoteFilter::default();
            let candidate = tip(ce, cs);
            let local = tip(le, ls);
            let mut delivered = false;
            for _ in 0..=LIVENESS_ESCAPE_DROPS {
                if f.should_deliver(candidate, local) {
                    delivered = true;
                    break;
                }
            }
            proptest::prop_assert!(
                delivered || f.should_deliver(candidate, local),
                "filter must deliver within LIVENESS_ESCAPE_DROPS + 1 requests"
            );
        }
    }
}
