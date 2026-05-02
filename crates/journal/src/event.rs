//! Journal event model.
//!
//! The journal speaks two kinds of events:
//!
//! 1. **Transport-intrinsic**: `GenesisHash`, `Checkpoint` for the hash
//!    chain, `Tick` for the scheduler clock. The journal emits and
//!    consumes these itself; applications do not see them.
//! 2. **Application**: delivered to the `Application` for state mutation,
//!    wrapped in `App(E)` so the journal is agnostic to what the app does.
//!
//! Only input commands are journaled — not execution reports. The
//! application is deterministic, so replaying inputs reproduces outputs
//! identically (halves journal size, simplifies the format).
//!
//! The ≤ 64-byte size bound is enforced by the concrete-`E` consumer
//! (e.g. `melin-engine` asserts on `JournalEvent<TradingEvent>`), not
//! here: the bound is meaningful only when `E`'s layout is known.

use melin_app::AppEvent;

/// An input event to be journaled for replay and crash recovery.
///
/// `Copy` because all fields are fixed-size primitives/newtypes (no heap)
/// — the disruptor ring publishes by byte-copy, so hot-path operations
/// stay allocation-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalEvent<E: AppEvent> {
    /// First entry in every v12+ journal. Contains random bytes (fresh
    /// journal) or the chain hash at the rotation boundary (rotated
    /// journal). Seeds the BLAKE3 hash chain for tamper evidence and
    /// replica consistency.
    GenesisHash { hash: [u8; 32] },
    /// Periodic hash chain checkpoint. Contains the running BLAKE3 chain
    /// hash so readers can verify integrity without recomputing from
    /// genesis. Written to the journal like any other entry and itself
    /// hashed into the chain for continuity.
    Checkpoint {
        chain_hash: [u8; 32],
        events_since_checkpoint: u64,
    },
    /// Internal clock tick. Published by the ingress thread at the
    /// configured cadence and journaled like any other input event.
    /// Carries the wall-clock time the application uses to fire due
    /// scheduled tasks. Replay feeds the recorded `now_ns` back,
    /// preserving determinism.
    Tick { now_ns: u64 },
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

impl<E: AppEvent> JournalEvent<E> {
    /// Returns `true` for events the journal stage must skip (read-only
    /// queries). `GenesisHash` / `Checkpoint` / `Tick` are always
    /// journaled; app events delegate to [`AppEvent::is_query`].
    #[inline]
    pub fn is_query(&self) -> bool {
        match self {
            JournalEvent::App(e) => e.is_query(),
            _ => false,
        }
    }

    /// Returns `true` for the pipeline-shutdown sentinel. Each stage
    /// stops at this slot (after completing any pending work) and exits.
    #[inline]
    pub fn is_shutdown(&self) -> bool {
        matches!(self, JournalEvent::Shutdown)
    }
}
