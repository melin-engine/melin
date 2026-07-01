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
//! loss to Raft, so it can only delay an election (liveness) — it can
//! never violate Raft safety. Liveness is preserved as long as the
//! most-caught-up live node can reach a quorum: that node's requests
//! pass every peer's filter.

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
