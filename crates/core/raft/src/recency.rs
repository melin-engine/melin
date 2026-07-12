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
//! Filtering happens at the RPC boundary, *before* the message reaches
//! the Raft state machine, because raft-rs's vote predicate is not
//! extensible. Dropping a vote request is indistinguishable from packet
//! loss to Raft, so it can never violate Raft safety.
//!
//! **Liveness needs an escape hatch.** The journal-tip order and Raft's
//! own log-recency vote rule are *independent* orderings applied
//! conjunctively: a candidate must be delivered (tip order) *and*
//! granted (Raft log order). After a control-plane-only leadership
//! change, the old data-plane primary can hold the highest journal tip
//! but the older Raft log while a peer holds the newer log but a lower,
//! frozen tip — each vetoes the other and the cluster sits leaderless
//! indefinitely, quietly (dropped pre-votes inflate no terms). So the
//! filter is a *stateful gate* ([`VoteFilter`]): after
//! [`LIVENESS_ESCAPE_DROPS`] consecutively dropped requests with no
//! leader observed, it stops filtering until a leader is next seen,
//! letting Raft's own rules elect whoever they can. The filter is
//! therefore best-effort steering, not a guarantee: the authoritative
//! journal-safety check belongs at *promotion* time (a Raft leader with
//! a stale journal must refuse to serve), never at the ballot box.

/// A node's journal tip as advertised in the control-plane RPC envelope.
///
/// `u64` fields (not the transport-core `WireSeq` newtype) because this
/// crate sits below transport-core in the dependency graph; the driver
/// converts at the boundary.
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
}

/// Voter-side recency rule: should a vote request from a candidate
/// advertising `candidate` be delivered to the local Raft node, given
/// our own `local` tip?
///
/// `true` when the candidate is at least as caught up as we are.
/// Callers apply this to vote-shaped messages only (see
/// [`is_vote_request`]) — regular heartbeats/appends must never be
/// filtered, or a legitimately elected leader would be unable to lead.
#[inline]
pub fn candidate_is_current(candidate: JournalTip, local: JournalTip) -> bool {
    candidate.key() >= local.key()
}

/// Consecutively dropped vote requests after which a voter stops
/// filtering until it next observes a leader. A deadlocked candidate
/// re-campaigns roughly once per randomized election timeout (1–2 s
/// with the [`crate::node`] defaults), so the escape opens after
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
    /// the local Raft node, given our own `local` tip? Callers apply
    /// this to vote-shaped messages only (see [`is_vote_request`]).
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

    /// The driver observed a live leader — elections are working, so
    /// re-arm the filter.
    pub fn leader_observed(&mut self) {
        self.consecutive_drops = 0;
    }
}

/// Whether a message type is subject to the recency filter: the vote
/// and pre-vote requests a candidate sends when campaigning. Everything
/// else — heartbeats, appends, vote *responses*, transfers — passes
/// unfiltered.
#[inline]
pub fn is_vote_request(msg_type: raft::eraftpb::MessageType) -> bool {
    use raft::eraftpb::MessageType;
    matches!(
        msg_type,
        MessageType::MsgRequestVote | MessageType::MsgRequestPreVote
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::MessageType;

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

    #[test]
    fn only_vote_requests_are_filtered() {
        assert!(is_vote_request(MessageType::MsgRequestVote));
        assert!(is_vote_request(MessageType::MsgRequestPreVote));
        for passthrough in [
            MessageType::MsgHeartbeat,
            MessageType::MsgAppend,
            MessageType::MsgRequestVoteResponse,
            MessageType::MsgRequestPreVoteResponse,
            MessageType::MsgTransferLeader,
        ] {
            assert!(!is_vote_request(passthrough), "{passthrough:?}");
        }
    }
}
