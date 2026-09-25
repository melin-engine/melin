//! The time floor a writer opens with: the stamp its next entry must
//! exceed.
//!
//! Journaled time strictly increases across the whole journal, not just
//! within a segment, so a writer opened partway through history has to
//! know the stamp that precedes its write position. That stamp is threaded
//! in by whoever opens the writer, beside the sequence, from the same
//! source: the recovery walk's last entry, or a snapshot's anchor. The
//! segment header does not carry it.

use melin_app::SequencerTime;

/// What precedes a writer's first entry in time. Required by every writer
/// constructor that opens partway through history, with no default, so
/// each place the floor is waived can be found by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeFloor {
    /// Nothing precedes the write position: a brand-new journal.
    Genesis,
    /// The stamp of the last entry before the write position.
    After(SequencerTime),
    /// The stamp is not known: the writer continues from a snapshot
    /// written before snapshots recorded one. Happens once, on the boot
    /// that upgrades to a build that does.
    Unknown,
}

impl TimeFloor {
    /// The stamp the writer's first entry must exceed. Zero for
    /// [`Genesis`](Self::Genesis) and [`Unknown`](Self::Unknown).
    pub fn time(self) -> SequencerTime {
        match self {
            Self::After(time) => time,
            Self::Genesis | Self::Unknown => SequencerTime::default(),
        }
    }
}
