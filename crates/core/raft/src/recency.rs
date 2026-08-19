//! Journal-tip recency filter for vote requests.
//!
//! Melin replicates order data out-of-band (the replication data plane),
//! so Raft's own log-recency vote check says nothing about which node
//! holds the most order data. Left unmodified, Raft could elect a node
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

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;

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
}

impl TipSource {
    pub fn local_tip(&self) -> JournalTip {
        JournalTip {
            epoch: self.fence.epoch(),
            last_sequence: self.seq.load().get(),
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
pub struct PeerTips {
    /// `Mutex<HashMap>` rather than anything lock-free: written a few
    /// times per heartbeat interval per peer and read once per 100 ms
    /// promotion poll, so contention is irrelevant; and the map holds
    /// 2–4 entries (the configured voter set), so `HashMap` vs anything
    /// smarter is noise. Never touched by the data plane.
    inner: std::sync::Mutex<std::collections::HashMap<crate::types::NodeId, TipSample>>,
}

/// One recorded observation: the advertised tip and when it arrived.
#[derive(Debug, Clone, Copy)]
struct TipSample {
    tip: JournalTip,
    at: std::time::Instant,
}

impl PeerTips {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Record a tip heard from `peer` now.
    pub fn record(&self, peer: crate::types::NodeId, tip: JournalTip) {
        self.record_at(peer, tip, std::time::Instant::now());
    }

    /// Deterministic-time seam for `record` — tests inject the arrival
    /// instant so aging is assertable without sleeping.
    pub fn record_at(&self, peer: crate::types::NodeId, tip: JournalTip, at: std::time::Instant) {
        // Poisoning unreachable under panic=abort (no unwinding).
        let mut map = self.inner.lock().expect("peer tips mutex poisoned");
        map.insert(peer, TipSample { tip, at });
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
            .map(|s| (s.tip, now.saturating_duration_since(s.at)))
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

    #[test]
    fn peer_tips_sample_returns_latest_with_age() {
        use std::time::{Duration, Instant};
        let tips = PeerTips::new();
        let t0 = Instant::now();
        assert!(tips.sample(2, t0).is_none(), "never-heard peer is None");

        tips.record_at(2, tip(1, 10), t0);
        // A later observation replaces the earlier one entirely.
        tips.record_at(2, tip(1, 42), t0 + Duration::from_millis(500));

        let (t, age) = tips.sample(2, t0 + Duration::from_secs(2)).expect("sample");
        assert_eq!(t, tip(1, 42));
        assert_eq!(age, Duration::from_millis(1500));

        // Peers are independent.
        assert!(tips.sample(3, t0 + Duration::from_secs(2)).is_none());
    }

    #[test]
    fn peer_tips_age_saturates_for_out_of_order_clock_reads() {
        use std::time::{Duration, Instant};
        let tips = PeerTips::new();
        let t0 = Instant::now();
        tips.record_at(2, tip(1, 10), t0 + Duration::from_secs(1));
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
