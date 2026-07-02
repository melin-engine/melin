//! Application construction, pre-allocation, and operator policy.
//!
//! The server runtime constructs application instances in several
//! contexts: a fresh primary at startup, a replica preparing to
//! receive a snapshot transfer, a replica catching up from genesis
//! via journal replay, and the shadow stage (which maintains a
//! parallel copy for snapshotting). All share [`AppFactory::empty`].
//! The primary startup path additionally calls
//! [`AppFactory::prefault`] to pre-size collections before the
//! bulk-seed phase.
//!
//! Operator-controlled policy (rate limits, caps, ...) is now part of
//! journaled state: the factory yields it as
//! [`AppFactory::operator_policy_event`], which the runtime seeds into a
//! fresh journal (or injects once when migrating a pre-feature lineage)
//! so primary and replica converge by replay rather than by each node
//! reapplying matching CLI flags.

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

    /// The operator-policy event (rate limits, caps, ...) built from this
    /// factory's CLI-level config, if the application has one. Journaled so
    /// primary and replica converge by replay rather than by each node
    /// independently reapplying matching CLI flags. The runtime uses it in
    /// two places: prepended to [`seed_events`](AppFactory::seed_events) on
    /// a fresh primary, and injected once as a migration event on a primary
    /// whose recovered journal predates journaled operator policy (see
    /// [`Application::operator_policy_present`]). Default `None` for
    /// applications that carry no operator policy.
    fn operator_policy_event(&self) -> Option<<Self::App as Application>::Event> {
        None
    }

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
