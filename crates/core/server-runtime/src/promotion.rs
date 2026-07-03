//! The replica → primary promotion request channel.
//!
//! One shared handle connects the three parties of a promotion: the
//! **requesters** (the admin `PROMOTE` command, and the raft driver on
//! auto-promotion), the **consumer** (the replica's receive loop, which
//! observes the request, drains, and tears down), and the **epoch
//! allocator** (`run_as_primary`, which journals the tenure's
//! `EpochBump`).
//!
//! The request is a `u64`, not a bool, because a promotion must carry
//! *which election authorized it*: the raft driver stores its leader
//! term, and the new tenure's fencing epoch is
//! `max(current_epoch + 1, requested)`. Capturing the term at request
//! time (rather than reading it again at bump time) matters — if a
//! newer election happens while this node is mid-promotion, the newer
//! winner allocates a strictly higher epoch and fences this node,
//! which is exactly the "promote exactly one replica" rule, enforced
//! by machine instead of playbook. A manual `PROMOTE` carries
//! [`PromotionRequest::MANUAL`] (= 1), which the `max` folds to the
//! classic `current_epoch + 1`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Shared one-shot promotion request. `0` = no request; any non-zero
/// value is a request whose new fencing epoch must be at least that
/// value. First requester wins; the value never changes afterwards
/// (the flag was already one-way as a bool — a promotion cannot be
/// un-asked once the receive loop may have observed it).
///
/// `Arc<AtomicU64>` inside (not a `Mutex`): every reader is a poll on
/// a hot-ish loop (the receive loop checks it per iteration), and a
/// single word carries the whole request.
#[derive(Clone, Debug)]
pub struct PromotionRequest(Arc<AtomicU64>);

impl Default for PromotionRequest {
    fn default() -> Self {
        Self::new()
    }
}

impl PromotionRequest {
    /// Request value for an operator-driven `PROMOTE`: no election term
    /// backs it, so the epoch allocator's `max` resolves to the classic
    /// `current_epoch + 1`.
    pub const MANUAL: u64 = 1;

    /// Fresh, unrequested handle.
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    /// File a promotion request whose new epoch must be at least
    /// `min_epoch` (≥ 1; [`Self::MANUAL`] for operator requests).
    /// Returns `true` if this call filed the request, `false` if one
    /// was already pending — the first request wins and later ones are
    /// ignored, so a manual `PROMOTE` racing an auto-promotion cannot
    /// retarget an in-flight transition.
    pub fn request(&self, min_epoch: u64) -> bool {
        debug_assert!(min_epoch >= 1, "a promotion request must be non-zero");
        // AcqRel success ordering: pairs with the consumers'
        // `Acquire` polls, same convention as the old bool flag.
        self.0
            .compare_exchange(0, min_epoch.max(1), Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Whether a promotion has been requested (the receive loop's poll).
    #[inline]
    pub fn is_requested(&self) -> bool {
        self.0.load(Ordering::Acquire) != 0
    }

    /// The pending request's minimum epoch, or `None` when unrequested.
    pub fn pending(&self) -> Option<u64> {
        match self.0.load(Ordering::Acquire) {
            0 => None,
            epoch => Some(epoch),
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
        assert!(r.request(7));
        assert_eq!(r.pending(), Some(7));
        // A racing manual PROMOTE must not retarget the in-flight
        // transition to a different epoch.
        assert!(!r.request(PromotionRequest::MANUAL));
        assert_eq!(r.pending(), Some(7));
    }

    #[test]
    fn manual_request_carries_the_manual_sentinel() {
        let r = PromotionRequest::new();
        assert!(r.request(PromotionRequest::MANUAL));
        assert_eq!(r.pending(), Some(1));
        assert!(r.is_requested());
    }

    #[test]
    fn clones_share_the_request() {
        let r = PromotionRequest::new();
        let requester = r.clone();
        assert!(requester.request(42));
        assert_eq!(r.pending(), Some(42));
    }
}
