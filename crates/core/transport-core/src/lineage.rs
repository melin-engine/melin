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
//! hand. A replica's receive thread folds the changes it accepts with the
//! same rule ([`LineagePolicy::after`], through [`SharedLineagePolicy`]).

use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};

use melin_journal::{AckPolicyByte, AckPolicyChange};

use crate::cursors::WireSeq;
use crate::dispatch::LineageEvent;

/// The lineage metadata in force at one journal position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lineage {
    /// The highest fencing epoch journaled so far: a maximum, as the
    /// epoch only ever moves forward.
    pub epoch: u64,
    /// The ack policy the journaled `AckPolicyChange`s put in force, with
    /// what a node needs to apply the newest of them again ([`LineagePolicy`]);
    /// `None` when the lineage holds no such entry (a new history before
    /// its first, or a lineage written before the entry existed).
    pub ack_policy: Option<LineagePolicy>,
}

impl Lineage {
    /// The lineage of a new history: no promotion, no policy journaled.
    pub const GENESIS: Self = Self {
        epoch: 0,
        ack_policy: None,
    };

    /// Fold one journaled lineage event, the entry at `wire_seq`, into
    /// the value.
    #[inline]
    pub(crate) fn observe(&mut self, wire_seq: u64, event: LineageEvent) {
        match event {
            LineageEvent::Epoch(epoch) => crate::fence::observe_into(&mut self.epoch, epoch),
            LineageEvent::AckPolicy(change) => {
                self.ack_policy = Some(LineagePolicy::after(self.ack_policy, wire_seq, change));
            }
        }
    }
}

/// A lineage's ack policy: the policy its journaled changes put in force,
/// and the transition that put it there, so that a node starting a
/// primary tenure on the lineage (a restart, a promotion) applies that
/// transition again rather than assuming it took effect. Whether it had
/// is not on disk: a crash can come between the change being journaled
/// and the nodes the policy before it relied on holding it.
///
/// The transition is the newest change that *moved* the policy. A change
/// naming the policy already in force (a re-sent `ACK-POLICY`, every
/// command being journaled) moves nothing, and recording it as the
/// transition would make the policy it repeats look like the one before
/// it: `disk+ram`, then `disk` at `w1` (waiting for a replica), then
/// `disk` again at `w2`, must still be applied as `disk+ram` → `disk` at
/// `w1`. Last writer wins for the policy itself: a change can weaken the
/// policy as well as strengthen it, so there is no order to take a
/// maximum in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineagePolicy {
    /// The policy in force, and whether the transition to it was
    /// unwitnessed: set if any change naming it since it was last
    /// different was journaled unwitnessed, so a forced re-send of a
    /// waiting change (the documented way to force it) is applied, again,
    /// once on disk rather than waiting for a witness.
    pub change: AckPolicyChange,
    /// The wire sequence of the transition: the first change of the run
    /// naming `change.policy`. `u64` because it is compared against
    /// output slots' wire sequences.
    pub wire_seq: u64,
    /// The policy in force before the transition; `None` when it was the
    /// lineage's first change, which a primary journals as the policy its
    /// gate already starts under, so there is nothing to apply again.
    pub previous: Option<AckPolicyByte>,
}

impl LineagePolicy {
    /// The policy in force.
    #[inline]
    pub fn policy(&self) -> AckPolicyByte {
        self.change.policy
    }

    /// The lineage policy after the change at `wire_seq`, given the one
    /// before it: the one fold, for replay and for a replica's receive
    /// thread alike.
    #[inline]
    #[must_use]
    pub fn after(current: Option<Self>, wire_seq: u64, change: AckPolicyChange) -> Self {
        match current {
            Some(current) if current.change.policy == change.policy => Self {
                change: AckPolicyChange {
                    policy: change.policy,
                    unwitnessed: current.change.unwitnessed || change.unwitnessed,
                },
                ..current
            },
            current => Self {
                change,
                wire_seq,
                previous: current.map(|p| p.policy()),
            },
        }
    }
}

/// A shared [`LineagePolicy`]: a replica's record of its lineage's ack
/// policy, folded by its receive thread as changes are accepted and
/// stored whole after a recovery or a snapshot resync (so a change in a
/// divergent tail disappears with the tail); read by auto-promotion and
/// by a promotion, which seeds its gate from it.
///
/// A mutex rather than atomics: the record is wider than one word, and a
/// reader must never pair one change's sequence with another's policy.
/// It is taken only when a change is accepted (rare) or the record read
/// off the stream's path, never per slot.
#[derive(Debug, Default)]
pub struct SharedLineagePolicy(Mutex<Option<LineagePolicy>>);

impl SharedLineagePolicy {
    /// A record holding `policy`.
    pub fn new(policy: Option<LineagePolicy>) -> Self {
        Self(Mutex::new(policy))
    }

    /// The record as last stored or folded.
    pub fn load(&self) -> Option<LineagePolicy> {
        *self.lock()
    }

    /// Replace the record, whatever it held.
    pub fn store(&self, policy: Option<LineagePolicy>) {
        *self.lock() = policy;
    }

    /// Fold the change at `wire_seq` into the record
    /// ([`LineagePolicy::after`]).
    pub fn observe(&self, wire_seq: u64, change: AckPolicyChange) {
        let mut record = self.lock();
        *record = Some(LineagePolicy::after(*record, wire_seq, change));
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<LineagePolicy>> {
        // Poisoning cannot leave the record half-written: every write
        // stores a whole `Copy` value. The value is still the last one
        // stored, so it is used as is.
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// A shared `Option<AckPolicyByte>`, read by threads other than the one
/// that writes it: on a primary, the health gauges' view of the policy
/// journaled last and of the one the response gate enforces.
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

/// An unwitnessed ack-policy change the matching stage has dispatched:
/// the entry's wire sequence and the policy it puts in force.
///
/// The response gate applies it out of order, as soon as the entry itself
/// is durable on the primary's disk, rather than at the entry's slot: the
/// slot a halted gate is stuck on comes before the change in the output
/// ring, so an in-order switch would never be reached. A change is forced
/// only with the operator's explicit consent (a weakening sent while no
/// replica is connected) or on a node without replication; every other
/// change is applied in order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForcedPolicy {
    /// The change entry's wire sequence.
    pub wire_seq: WireSeq,
    /// The policy it puts in force.
    pub policy: AckPolicyByte,
}

/// The seqlock payload behind [`ForcedPolicyWriter`]: `[wire_seq,
/// FORCED_PRESENT | policy]`, or all zero before any forced change. Two
/// `u64` words rather than one packed word so every wire sequence stays
/// representable, and a seqlock rather than two atomics so a reader never
/// pairs one change's sequence with another's policy.
type ForcedWords = [u64; 2];

/// Set in the second word once a forced change has been stored, so a
/// policy byte of zero is told apart from "none".
const FORCED_PRESENT: u64 = 1 << 8;

/// Create the forced-policy cell: its single writer (the matching stage)
/// and a reader for the response gate, both empty.
pub fn forced_policy_cell() -> (ForcedPolicyWriter, ForcedPolicyReader) {
    let (writer, reader) = melin_pipeline::seqlock::split::<ForcedWords>([0, 0]);
    (ForcedPolicyWriter(writer), ForcedPolicyReader(reader))
}

/// The matching stage's half of the forced-policy cell. Not `Clone`, and
/// [`store`](Self::store) takes `&mut self`: one writer, by construction.
pub struct ForcedPolicyWriter(melin_pipeline::seqlock::SeqLockWriter<ForcedWords>);

impl ForcedPolicyWriter {
    /// Publish the newest forced change, replacing any earlier one: a
    /// later forced change supersedes an earlier one the gate has not
    /// applied yet.
    #[inline]
    pub fn store(&mut self, forced: ForcedPolicy) {
        self.0.store([
            forced.wire_seq.get(),
            FORCED_PRESENT | u64::from(forced.policy.0),
        ]);
    }
}

/// The response gate's half of the forced-policy cell.
#[derive(Clone)]
pub struct ForcedPolicyReader(melin_pipeline::seqlock::SeqLockReader<ForcedWords>);

impl ForcedPolicyReader {
    /// The newest forced change, `None` before the first.
    #[inline]
    pub fn load(&self) -> Option<ForcedPolicy> {
        let [wire_seq, policy] = self.0.load();
        (policy & FORCED_PRESENT != 0).then_some(ForcedPolicy {
            wire_seq: WireSeq::new(wire_seq),
            // Truncation intended: the low byte is the policy, the bit
            // above it the presence flag.
            policy: AckPolicyByte(policy as u8),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(policy: u8, unwitnessed: bool) -> AckPolicyChange {
        AckPolicyChange {
            policy: AckPolicyByte(policy),
            unwitnessed,
        }
    }

    fn change(policy: u8) -> LineageEvent {
        LineageEvent::AckPolicy(payload(policy, false))
    }

    fn record(
        policy: u8,
        unwitnessed: bool,
        wire_seq: u64,
        previous: Option<u8>,
    ) -> Option<LineagePolicy> {
        Some(LineagePolicy {
            change: payload(policy, unwitnessed),
            wire_seq,
            previous: previous.map(AckPolicyByte),
        })
    }

    /// The epoch is a maximum; the policy is the newest change, whichever
    /// direction it goes, with the policy before it and its sequence, and
    /// an epoch bump leaves it alone.
    #[test]
    fn the_epoch_is_a_maximum_and_the_policy_the_newest_change() {
        let mut lineage = Lineage::GENESIS;
        lineage.observe(1, LineageEvent::Epoch(4));
        lineage.observe(2, change(1));
        lineage.observe(3, LineageEvent::Epoch(2));
        assert_eq!(
            lineage,
            Lineage {
                epoch: 4,
                ack_policy: record(1, false, 2, None),
            },
            "the lineage's first change has nothing before it"
        );
        // A lower byte after a higher one still wins: no ranking.
        lineage.observe(4, change(0));
        assert_eq!(lineage.ack_policy, record(0, false, 4, Some(1)));
        lineage.observe(5, LineageEvent::Epoch(9));
        assert_eq!(
            lineage,
            Lineage {
                epoch: 9,
                ack_policy: record(0, false, 4, Some(1)),
            }
        );
    }

    /// A change naming the policy in force moves nothing: the transition
    /// stays the first change of the run, with the policy before the run,
    /// so a re-sent weakening still waits for what the policy before it
    /// relied on. An unwitnessed one in the run marks the transition
    /// unwitnessed (a forced re-send), and a later witnessed one does not
    /// clear it; a change that moves the policy starts afresh.
    #[test]
    fn a_repeated_change_keeps_the_transition_and_an_unwitnessed_one_marks_it() {
        let disk_and_ram = 2;
        let disk = 0;
        let mut policy = Some(LineagePolicy::after(None, 3, payload(disk_and_ram, false)));
        assert_eq!(policy, record(disk_and_ram, false, 3, None));
        for (wire_seq, change, expected) in [
            (
                7,
                payload(disk, false),
                record(disk, false, 7, Some(disk_and_ram)),
            ),
            (
                9,
                payload(disk, false),
                record(disk, false, 7, Some(disk_and_ram)),
            ),
            (
                11,
                payload(disk, true),
                record(disk, true, 7, Some(disk_and_ram)),
            ),
            (
                12,
                payload(disk, false),
                record(disk, true, 7, Some(disk_and_ram)),
            ),
            (
                15,
                payload(disk_and_ram, false),
                record(disk_and_ram, false, 15, Some(disk)),
            ),
        ] {
            policy = Some(LineagePolicy::after(policy, wire_seq, change));
            assert_eq!(policy, expected, "after the change at {wire_seq}");
        }
    }

    /// The shared record folds with the same rule, and a store replaces
    /// it whole, `None` included.
    #[test]
    fn the_shared_record_folds_and_stores_whole() {
        let shared = SharedLineagePolicy::new(None);
        assert_eq!(shared.load(), None);
        shared.observe(4, payload(2, false));
        shared.observe(6, payload(0, false));
        shared.observe(8, payload(0, true));
        assert_eq!(shared.load(), record(0, true, 6, Some(2)));
        shared.store(record(3, false, 1, None));
        assert_eq!(shared.load(), record(3, false, 1, None));
        shared.store(None);
        assert_eq!(shared.load(), None);
        assert_eq!(SharedLineagePolicy::default().load(), None);
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

    /// The forced cell starts empty, holds every policy byte (zero
    /// included, which the presence bit tells apart from "none") at any
    /// wire sequence, and a later store replaces an earlier one whole.
    #[test]
    fn the_forced_cell_starts_empty_and_holds_the_newest_change() {
        let (mut writer, reader) = forced_policy_cell();
        assert_eq!(reader.load(), None);
        for (wire_seq, byte) in [(1, 0), (7, u8::MAX), (u64::MAX, 3)] {
            let forced = ForcedPolicy {
                wire_seq: WireSeq::new(wire_seq),
                policy: AckPolicyByte(byte),
            };
            writer.store(forced);
            assert_eq!(reader.load(), Some(forced));
            assert_eq!(reader.clone().load(), Some(forced), "clones share the cell");
        }
    }
}
