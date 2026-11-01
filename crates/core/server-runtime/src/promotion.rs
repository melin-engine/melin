//! The replica → primary promotion request channel.
//!
//! One shared handle connects the three parties of a promotion: the
//! **requesters** (the admin `PROMOTE` command, and the raft driver on
//! auto-promotion), the **consumer** (the replica's receive loop, which
//! observes the request, drains, and tears down), and the **epoch
//! allocator** (`run_as_primary`, or `run_as_primary_dpdk` on DPDK, which
//! journals the tenure's `EpochBump`).
//!
//! The request is a [`PromotionOrigin`], not a bool, because a promotion
//! must carry *what authorized it*: the raft driver files the term it was
//! elected at, and the new tenure's fencing epoch is
//! `max(current_epoch + 1, origin.epoch_floor())`. Capturing the term at
//! request time (rather than reading it again at bump time) matters — if
//! a newer election happens while this node is mid-promotion, the newer
//! winner allocates a strictly higher epoch and fences this node, which
//! is exactly the "promote exactly one replica" rule, enforced by machine
//! instead of playbook. A manual `PROMOTE` files
//! [`PromotionOrigin::Manual`], whose floor the `max` folds to the
//! classic `current_epoch + 1`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// What authorized a promotion.
///
/// An enum rather than a bare number: an election at term 1 and a manual
/// `PROMOTE` share an epoch floor of 1, so a number cannot tell them
/// apart, and the mint must know whether it is honouring an election.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PromotionOrigin {
    /// An operator's `PROMOTE`: no election backs it.
    Manual,
    /// A raft election won at `term`. `u64` because that is openraft's
    /// term type, carried unchanged.
    Election { term: u64 },
}

impl PromotionOrigin {
    /// The lowest fencing epoch the promoted tenure may take. An election
    /// floors it at its term, so tenure epochs align with raft terms; a
    /// manual promotion floors it at 1, which `max(epoch + 1, floor)`
    /// always exceeds or meets, so it resolves to the classic
    /// `epoch + 1`.
    pub fn epoch_floor(self) -> u64 {
        match self {
            Self::Manual => 1,
            Self::Election { term } => term,
        }
    }
}

/// The stored word for "no request".
const NONE: u64 = 0;
/// The stored word for [`PromotionOrigin::Manual`]. No election files it:
/// [`PromotionRequest::request`] refuses a term equal to it.
const MANUAL: u64 = u64::MAX;

/// Shared one-shot promotion request. First requester wins; the origin
/// never changes afterwards (the flag was already one-way as a bool — a
/// promotion cannot be un-asked once the receive loop may have observed
/// it).
///
/// `Arc<AtomicU64>` inside (not a `Mutex`, not an atomic per field):
/// every reader is a poll on a hot-ish loop (the receive loop checks it
/// per iteration), and first-request-wins is one compare-exchange, which
/// needs the whole request in one word. The word encodes the origin as
/// `0` = none, `u64::MAX` = manual, any other value = the election term.
/// An election term is never 0 (openraft's first election is term 1), and
/// the one term the encoding cannot carry, `u64::MAX`, is refused rather
/// than misread as manual.
#[derive(Clone, Debug)]
pub struct PromotionRequest(Arc<AtomicU64>);

impl Default for PromotionRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl PromotionRequest {
    /// Fresh, unrequested handle.
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(NONE)))
    }

    /// File a promotion request with its origin. Returns `true` if this
    /// call filed the request, `false` if one was already pending — the
    /// first request wins and later ones are ignored, so a manual
    /// `PROMOTE` racing an auto-promotion cannot retarget an in-flight
    /// transition — or if `origin` is an election at term `u64::MAX`,
    /// the manual sentinel's word, which this handle cannot carry.
    pub fn request(&self, origin: PromotionOrigin) -> bool {
        let word = match origin {
            PromotionOrigin::Manual => MANUAL,
            // `u64::MAX` is the manual sentinel: filing it would read
            // back as a manual promotion. No term gets there in
            // practice; refusing (not asserting) keeps debug and release
            // alike and the refusal testable.
            PromotionOrigin::Election { term: MANUAL } => return false,
            // Term 0 is never elected (openraft starts at term 1), and
            // `NONE` would read back as unrequested: refuse it alike.
            PromotionOrigin::Election { term: NONE } => return false,
            PromotionOrigin::Election { term } => term,
        };
        // AcqRel success ordering: pairs with the consumers'
        // `Acquire` polls, same convention as the old bool flag.
        self.0
            .compare_exchange(NONE, word, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Whether a promotion has been requested (the receive loop's poll).
    #[inline]
    pub fn is_requested(&self) -> bool {
        self.0.load(Ordering::Acquire) != NONE
    }

    /// The pending request's origin, or `None` when unrequested.
    pub fn pending(&self) -> Option<PromotionOrigin> {
        match self.0.load(Ordering::Acquire) {
            NONE => None,
            MANUAL => Some(PromotionOrigin::Manual),
            term => Some(PromotionOrigin::Election { term }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_unrequested() {
        let r = PromotionRequest::new();
        assert!(!r.is_requested());
        assert_eq!(r.pending(), None);
    }

    #[test]
    fn first_request_wins_and_later_ones_are_ignored() {
        let r = PromotionRequest::new();
        assert!(r.request(PromotionOrigin::Election { term: 7 }));
        assert_eq!(r.pending(), Some(PromotionOrigin::Election { term: 7 }));
        // A racing manual PROMOTE must not retarget the in-flight
        // transition to a different epoch.
        assert!(!r.request(PromotionOrigin::Manual));
        assert_eq!(r.pending(), Some(PromotionOrigin::Election { term: 7 }));
        // Nor a newer election.
        assert!(!r.request(PromotionOrigin::Election { term: 8 }));
        assert_eq!(r.pending(), Some(PromotionOrigin::Election { term: 7 }));
    }

    #[test]
    fn a_manual_request_reads_back_as_manual() {
        let r = PromotionRequest::new();
        assert!(r.request(PromotionOrigin::Manual));
        assert_eq!(r.pending(), Some(PromotionOrigin::Manual));
        assert!(r.is_requested());
        // An election racing it loses.
        assert!(!r.request(PromotionOrigin::Election { term: 3 }));
        assert_eq!(r.pending(), Some(PromotionOrigin::Manual));
    }

    #[test]
    fn clones_share_the_request() {
        let r = PromotionRequest::new();
        let requester = r.clone();
        assert!(requester.request(PromotionOrigin::Election { term: 42 }));
        assert_eq!(r.pending(), Some(PromotionOrigin::Election { term: 42 }));
    }

    /// The bug the enum closes: as a raw number, the first election after
    /// `initialize` (term 1) was the manual sentinel.
    #[test]
    fn an_election_at_term_1_is_not_a_manual_promotion() {
        let r = PromotionRequest::new();
        assert!(r.request(PromotionOrigin::Election { term: 1 }));
        assert_eq!(r.pending(), Some(PromotionOrigin::Election { term: 1 }));
    }

    #[test]
    fn a_term_equal_to_the_manual_sentinel_is_refused() {
        let r = PromotionRequest::new();
        assert!(!r.request(PromotionOrigin::Election { term: u64::MAX }));
        assert!(!r.is_requested());
        assert_eq!(r.pending(), None);
        // The refusal leaves the one-shot free for a valid request.
        assert!(r.request(PromotionOrigin::Election { term: u64::MAX - 1 }));
        assert_eq!(
            r.pending(),
            Some(PromotionOrigin::Election { term: u64::MAX - 1 })
        );
    }

    #[test]
    fn a_term_0_election_is_refused() {
        let r = PromotionRequest::new();
        assert!(!r.request(PromotionOrigin::Election { term: 0 }));
        assert!(!r.is_requested());
        assert_eq!(r.pending(), None);
    }

    #[test]
    fn epoch_floor_per_origin() {
        assert_eq!(PromotionOrigin::Manual.epoch_floor(), 1);
        assert_eq!(PromotionOrigin::Election { term: 1 }.epoch_floor(), 1);
        assert_eq!(PromotionOrigin::Election { term: 9 }.epoch_floor(), 9);
    }
}
