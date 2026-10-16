//! Journal event model.
//!
//! The journal speaks two kinds of events:
//!
//! 1. **Transport-intrinsic**: `Tick` for the scheduler clock (and the
//!    in-memory-only `Shutdown` sentinel). Hash-chain metadata is *not*
//!    an event — the chain anchor lives in the segment file header and
//!    the chain itself is recomputed from entry bytes (see the crate's
//!    `chain` module), so every journaled entry carries a dense,
//!    user-visible sequence number.
//! 2. **Application**: delivered to the `Application` for state mutation,
//!    wrapped in `App(E)` so the journal is agnostic to what the app does.
//!
//! Only input commands are journaled — not the application's reports. The
//! application is deterministic, so replaying inputs reproduces outputs
//! identically (halves journal size, simplifies the format).
//!
//! No bound on the in-memory size of `JournalEvent<E>` is enforced here:
//! it is meaningful only when `E`'s layout is known, so it is the
//! concrete-`E` consumer's to choose and to check.

use melin_app::AppEvent;

/// An input event to be journaled for replay and crash recovery.
///
/// `Copy` because all fields are fixed-size primitives/newtypes (no heap)
/// — the disruptor ring publishes by byte-copy, so hot-path operations
/// stay allocation-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalEvent<E: AppEvent> {
    /// Internal clock tick. Published by the ingress thread at the
    /// configured cadence and journaled like any other input event.
    /// Carries the wall-clock time the application uses to fire due
    /// scheduled tasks. Replay feeds the recorded `now_ns` back,
    /// preserving determinism.
    Tick { now_ns: u64 },
    /// Replication fencing epoch bump. Written as the first journaled
    /// entry of a node's primary tenure — genesis primaries never emit
    /// it (epoch stays 0); a promoted replica emits `EpochBump { prior
    /// epoch + 1 }` before accepting any client write. Carries no
    /// application state: it is lineage metadata, like the chain anchor,
    /// but unlike the anchor it must be totally ordered against the event
    /// flow (every event is unambiguously inside exactly one epoch), so it
    /// rides the journal as a sequenced event rather than living in the
    /// segment header. Replayed on recovery and replication to advance a
    /// node's observed epoch; never delivered to the `Application`.
    EpochBump { epoch: u64 },
    /// A change of the ack policy in force. Lineage metadata like
    /// [`EpochBump`](Self::EpochBump), and journaled for the same reason:
    /// the policy governs the events after it, so it must be totally
    /// ordered against them, replayed on recovery and replicated with
    /// them. Never delivered to the `Application`.
    AckPolicyChange(AckPolicyChange),
    /// Pipeline-shutdown sentinel. Published by the receiver/reader as
    /// the last slot before exit; downstream stages stop at this slot
    /// (after completing any pending work) and exit.
    ///
    /// Unlike the other variants, this is **never** journaled — the
    /// journal stage's encoder skips it. It's a transient in-memory
    /// signal that piggy-backs on the input ring's existing FIFO order
    /// to guarantee every event published before shutdown is processed
    /// by every stage before that stage exits. No drain-loop heuristics,
    /// no shutdown-flag synchronization with the producer cursor.
    Shutdown,
    /// Application-level event. Opaque to the journal; serialised via
    /// [`AppEvent::encode`] / [`AppEvent::decode`].
    App(E),
}

/// The ack policy's wire byte, carried opaquely: the named policies are
/// the server runtime's, which validates the byte when it applies the
/// entry. A newtype rather than a bare `u8` so it cannot be confused with
/// the entry's other bytes (its flags) or passed where a policy is not
/// meant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AckPolicyByte(pub u8);

/// The payload of [`JournalEvent::AckPolicyChange`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AckPolicyChange {
    /// The policy in force for the events after this entry.
    pub policy: AckPolicyByte,
    /// Applied without the nodes the previous policy relied on holding
    /// this entry: an operator's explicit weakening while no replica was
    /// connected, or a node running without replication. Recorded so the
    /// lineage says where nobody witnessed the change.
    pub unwitnessed: bool,
}

impl<E: AppEvent> JournalEvent<E> {
    /// Returns `true` for events the journal stage must skip (read-only
    /// queries). App events delegate to [`AppEvent::is_query`]; every
    /// transport event is journaled.
    ///
    /// The journal stage's sequence allocator and the matching stage's
    /// wire-sequence shadow both decide through this predicate, so it
    /// names every variant: a new one has to choose here, at compile
    /// time, rather than fall into a wildcard.
    #[inline]
    pub fn is_query(&self) -> bool {
        match self {
            JournalEvent::App(e) => e.is_query(),
            JournalEvent::Tick { .. }
            | JournalEvent::EpochBump { .. }
            | JournalEvent::AckPolicyChange(_)
            | JournalEvent::Shutdown => false,
        }
    }

    /// The ack-policy change this event carries, if it is one.
    ///
    /// The one decision point for the question, on the matching stage
    /// (where the change's output slot goes) and on a replica's receive
    /// thread (where its lineage cell follows it). Names every variant,
    /// like [`Self::is_query`]: a new lineage-metadata variant that also
    /// moves the policy must break the build here rather than fall into a
    /// wildcard's `None`.
    #[inline]
    pub fn ack_policy_change(&self) -> Option<AckPolicyChange> {
        match self {
            JournalEvent::AckPolicyChange(change) => Some(*change),
            JournalEvent::Tick { .. }
            | JournalEvent::EpochBump { .. }
            | JournalEvent::Shutdown
            | JournalEvent::App(_) => None,
        }
    }

    /// Returns `true` for the pipeline-shutdown sentinel. Each stage
    /// stops at this slot (after completing any pending work) and exits.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        matches!(self, JournalEvent::Shutdown)
    }
}
