//! The lineage metadata a node tracks next to its application state: the
//! fencing epoch and the ack policy in force.
//!
//! Both are journaled as sequenced events
//! ([`JournalEvent::EpochBump`](melin_journal::JournalEvent::EpochBump),
//! [`JournalEvent::AckPolicyChange`](melin_journal::JournalEvent::AckPolicyChange)),
//! replayed on recovery and replication, and recorded in snapshots, so the
//! value at any journal position is a function of the history up to it.
//! [`Lineage`] is that value, and its crate-private `observe` the one rule that
//! folds an event into it: every replay path (recovery, the shadow stage)
//! goes through it, so the two fields are never tracked in parallel by
//! hand.

use std::sync::atomic::{AtomicU16, Ordering};

use melin_journal::AckPolicyByte;

use crate::dispatch::LineageEvent;

/// The lineage metadata in force at one journal position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lineage {
    /// The highest fencing epoch journaled so far: a maximum, as the
    /// epoch only ever moves forward.
    pub epoch: u64,
    /// The policy the newest journaled `AckPolicyChange` put in force,
    /// or `None` when the lineage holds no such entry (a new history
    /// before its first, or a lineage written before the entry existed).
    /// Last writer wins: a change can weaken the policy as well as
    /// strengthen it, so there is no order to take a maximum in.
    pub ack_policy: Option<AckPolicyByte>,
}

impl Lineage {
    /// The lineage of a new history: no promotion, no policy journaled.
    pub const GENESIS: Self = Self {
        epoch: 0,
        ack_policy: None,
    };

    /// Fold one journaled lineage event into the value.
    #[inline]
    pub(crate) fn observe(&mut self, event: LineageEvent) {
        match event {
            LineageEvent::Epoch(epoch) => crate::fence::observe_into(&mut self.epoch, epoch),
            LineageEvent::AckPolicy(change) => self.ack_policy = Some(change.policy),
        }
    }
}

/// A shared `Option<AckPolicyByte>`: a replica's record of the lineage's
/// ack policy, read by threads other than the one that writes it.
///
/// Stored, never merged (see [`Lineage::ack_policy`]): after a recovery or
/// a snapshot resync the cell takes the recovered value whole, so a change
/// in a divergent tail disappears with the tail.
#[derive(Debug)]
pub struct LineagePolicyCell(AtomicU16);

/// The cell's encoding of `None`: outside the `u8` range, so every policy
/// byte stays representable.
const NO_POLICY: u16 = u16::MAX;

impl LineagePolicyCell {
    /// A cell holding `policy`.
    pub fn new(policy: Option<AckPolicyByte>) -> Self {
        Self(AtomicU16::new(encode(policy)))
    }

    /// The policy last stored. Acquire, pairing with [`Self::store`].
    #[inline]
    pub fn load(&self) -> Option<AckPolicyByte> {
        decode(self.0.load(Ordering::Acquire))
    }

    /// Replace the policy, whatever the cell held.
    #[inline]
    pub fn store(&self, policy: Option<AckPolicyByte>) {
        self.0.store(encode(policy), Ordering::Release);
    }
}

fn encode(policy: Option<AckPolicyByte>) -> u16 {
    policy.map_or(NO_POLICY, |p| u16::from(p.0))
}

fn decode(raw: u16) -> Option<AckPolicyByte> {
    u8::try_from(raw).ok().map(AckPolicyByte)
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_journal::AckPolicyChange;

    fn change(policy: u8) -> LineageEvent {
        LineageEvent::AckPolicy(AckPolicyChange {
            policy: AckPolicyByte(policy),
            unwitnessed: false,
        })
    }

    /// The epoch is a maximum; the policy is the newest change, whichever
    /// direction it goes, and an epoch bump leaves it alone.
    #[test]
    fn the_epoch_is_a_maximum_and_the_policy_the_newest_change() {
        let mut lineage = Lineage::GENESIS;
        lineage.observe(LineageEvent::Epoch(4));
        lineage.observe(change(1));
        lineage.observe(LineageEvent::Epoch(2));
        assert_eq!(
            lineage,
            Lineage {
                epoch: 4,
                ack_policy: Some(AckPolicyByte(1)),
            }
        );
        // A lower byte after a higher one still wins: no ranking.
        lineage.observe(change(0));
        assert_eq!(lineage.ack_policy, Some(AckPolicyByte(0)));
        lineage.observe(LineageEvent::Epoch(9));
        assert_eq!(
            lineage,
            Lineage {
                epoch: 9,
                ack_policy: Some(AckPolicyByte(0)),
            }
        );
    }

    /// Every byte, and its absence, round-trips through the cell, and a
    /// store replaces whatever it held, `None` included.
    #[test]
    fn the_cell_round_trips_every_value_and_stores_whole() {
        let cell = LineagePolicyCell::new(None);
        assert_eq!(cell.load(), None);
        for byte in 0..=u8::MAX {
            cell.store(Some(AckPolicyByte(byte)));
            assert_eq!(cell.load(), Some(AckPolicyByte(byte)));
        }
        cell.store(None);
        assert_eq!(cell.load(), None);
        assert_eq!(
            LineagePolicyCell::new(Some(AckPolicyByte(3))).load(),
            Some(AckPolicyByte(3))
        );
    }
}
