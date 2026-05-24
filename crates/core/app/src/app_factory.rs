//! Application construction, pre-allocation, and operator policy.
//!
//! The server runtime constructs application instances in several
//! contexts: a fresh primary at startup, a replica preparing to
//! receive a snapshot transfer, a replica catching up from genesis
//! via journal replay, and the shadow stage (which maintains a
//! parallel copy for snapshotting). All share [`AppFactory::empty`].
//! The primary startup path additionally calls
//! [`AppFactory::prefault`] to pre-size collections before the
//! bulk-seed phase, then [`AppFactory::apply_operator_policy`] for
//! non-journaled config.
//!
//! Operator-controlled policy (rate limits, caps, ...) is kept
//! separate from journaled state. [`AppFactory::apply_operator_policy`]
//! reapplies these knobs after snapshot restore so primary and
//! replica converge on matching values even though the journal
//! carries no record of them.

use crate::Application;

/// Build and configure application instances on behalf of the
/// runtime.
///
/// Implementors are typically construction-config holders (sizing
/// hints, operator knobs) rather than zero-sized — they capture the
/// CLI-level values needed to produce `A` instances. Stored as
/// `Arc<dyn AppFactory<App = ConcreteA>>` on the runtime config so
/// replication paths can construct fresh apps after their snapshot
/// transfers or catch-up scans.
pub trait AppFactory: Send + Sync {
    /// The concrete application this factory produces.
    type App: Application;

    /// Construct an empty application. Used by all paths that need
    /// a clean state: primary startup, replication snapshot receive,
    /// journal replay from genesis.
    fn empty(&self) -> Self::App;

    /// Pre-allocate internal collections for a known bulk-seed
    /// workload. Called once on the primary startup path before
    /// seeding begins, so the seed phase doesn't hit allocation
    /// stalls as collections grow.
    fn prefault(&self, app: &mut Self::App);

    /// Reapply operator-controlled policy (rate limits, caps, ...)
    /// to an existing app. The policy is NOT journaled — primary
    /// and replica must apply matching values independently — so
    /// this is called after every snapshot restore (which
    /// reconstructs state but not policy) and after every replica
    /// reconnect that reuses an existing pipeline. Default impl is
    /// a no-op for applications that have no operator policy.
    fn apply_operator_policy(&self, _app: &mut Self::App) {}

    /// Yield the bulk-seed events the runtime should journal at
    /// startup. Called once on a fresh primary (empty journal, no
    /// snapshot); replicas receive the same events through standard
    /// journal replay and never call this themselves. Default impl
    /// returns an empty `Vec` for applications that don't
    /// bulk-seed.
    ///
    /// Returning a `Vec` rather than streaming an iterator is a
    /// deliberate trade-off: seed sets are bounded by operator
    /// config (counts of accounts / instruments / similar) and run
    /// once at startup, so the allocation is not on any hot path
    /// and the simpler signature keeps the trait object-safe.
    fn seed_events(&self) -> Vec<<Self::App as Application>::Event> {
        Vec::new()
    }
}
