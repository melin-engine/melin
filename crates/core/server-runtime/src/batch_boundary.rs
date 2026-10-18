//! Where a response stage stands between two events' replies.
//!
//! A client attributes `Response` frames to the next batch terminator on
//! its connection, so nothing but a heartbeat may fall between an event's
//! first frame and its terminator. Every frame a response stage sends for
//! a published event comes from the output ring, in order, so the only
//! frames that could land inside one are the early replies
//! ([`crate::halt`]), which come from a queue of their own. A stage sends
//! them only while it holds an [`AtBoundary`], which only its
//! [`BatchBoundary`] tracker hands out, and only between two events.
//!
//! An event's slots can straddle two reads of the output ring (the read
//! cap, the ring's wrap, a matching stage that has published only part of
//! the event), and the durability gate can hold a slot in the middle of an
//! event, so "between two reads" and "the gate is waiting" are not
//! boundaries in themselves: the tracker is told about every slot, and
//! judges by the last one.

use std::marker::PhantomData;

/// Whether the last output slot a response stage handled ended its event.
///
/// Told about every slot the stage handles, whether or not its frames
/// went anywhere (a reply to a connection that has gone, a policy change,
/// a server-originated event): each still ends, or does not end, its
/// event. Starts between events, before the first slot.
#[derive(Debug)]
pub(crate) struct BatchBoundary {
    /// Whether the last slot handled was `is_last_in_request`, or no slot
    /// has been. A `bool` rather than the last input sequence: the matching
    /// stage marks every event's last slot, the output ring's own record
    /// of the boundary, and a sequence compare would only tell a new event
    /// from an old one once its first slot was already in hand.
    between_events: bool,
}

impl Default for BatchBoundary {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchBoundary {
    /// A tracker for a stage that has handled no slot yet.
    pub(crate) fn new() -> Self {
        Self {
            between_events: true,
        }
    }

    /// The stage has handled a slot: one that ends its event
    /// (`is_last_in_request`) or not. Takes the tracker mutably, so no
    /// [`AtBoundary`] can be alive across it.
    #[inline]
    pub(crate) fn handled(&mut self, is_last_in_request: bool) {
        self.between_events = is_last_in_request;
    }

    /// The token that lets the stage send early replies, if it is between
    /// two events' replies now: no slot handled yet, or the last one
    /// ended its event. Borrows the tracker, so the stage cannot handle a
    /// slot while it holds the token.
    #[inline]
    pub(crate) fn at_boundary(&self) -> Option<AtBoundary<'_>> {
        self.between_events.then_some(AtBoundary {
            _tracker: PhantomData,
        })
    }
}

/// A response stage's proof that it stands between two events' replies,
/// from its [`BatchBoundary`]: what sending early replies takes
/// ([`crate::halt::Registered::drain`]).
#[derive(Debug)]
pub(crate) struct AtBoundary<'t> {
    /// Ties the token to the tracker's borrow; a private field, so only
    /// the tracker builds one.
    _tracker: PhantomData<&'t BatchBoundary>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn a_fresh_tracker_is_between_events() {
        assert!(BatchBoundary::new().at_boundary().is_some());
        assert!(BatchBoundary::default().at_boundary().is_some());
    }

    #[test]
    fn a_slot_that_does_not_end_its_event_closes_the_boundary_until_one_does() {
        let mut tracker = BatchBoundary::new();
        tracker.handled(false);
        assert!(tracker.at_boundary().is_none(), "inside an event");
        tracker.handled(false);
        assert!(tracker.at_boundary().is_none(), "still inside it");
        tracker.handled(true);
        assert!(tracker.at_boundary().is_some(), "the event ended");
        tracker.handled(true);
        assert!(tracker.at_boundary().is_some(), "a one-slot event");
    }

    proptest! {
        /// Across every mix of slots, the token is handed out exactly
        /// when the last slot handled ended its event (or none was).
        #[test]
        fn the_token_follows_the_last_slot(slots in proptest::collection::vec(any::<bool>(), 0..64)) {
            let mut tracker = BatchBoundary::new();
            prop_assert!(tracker.at_boundary().is_some());
            for is_last in slots {
                tracker.handled(is_last);
                prop_assert_eq!(tracker.at_boundary().is_some(), is_last);
            }
        }
    }
}
