//! Trading / transport-only server library — exposes server startup
//! for embedding (benchmarks, tests). The mode is selected at compile
//! time through the `trading` and `skip-order-exec` cargo features,
//! exactly one of which must be enabled.

#[cfg(all(feature = "trading", feature = "skip-order-exec"))]
compile_error!(
    "melin-server must be built with exactly one of the `trading` or \
     `skip-order-exec` features enabled"
);
#[cfg(not(any(feature = "trading", feature = "skip-order-exec")))]
compile_error!(
    "melin-server must be built with exactly one of the `trading` or \
     `skip-order-exec` features enabled"
);

/// The concrete [`melin_app::Application`] this server is built against
/// — re-export of `melin_trading_server::exchange_app::ServerApp`.
pub type App = domain::exchange_app::ServerApp;

// Re-export the writer-selection enum + the generic pipeline / trace /
// codec / replication modules at the server crate root. Bench and any
// other downstream consumer now reach the LMAX-pipeline plumbing
// through `melin-server` instead of through `melin-exchange-core` — engine
// is the matching domain library, server is the wiring layer.
/// Re-export of the journal replication module — namespaced under
/// `journal_replication` to avoid colliding with the runtime's
/// `replication` module (the orchestrator that wraps it).
pub use melin_journal::replication as journal_replication;
pub use melin_journal::{
    AsyncWriteBatch, JournalError, JournalWrite, JournalWriterMode, RawJournalScanner,
    checkpoint_interval, codec, create_fresh_replica,
};
pub use melin_transport_core::{pipeline, trace};

/// Re-export of the control-plane event so existing call sites
/// (`melin_server::ControlEvent`) keep resolving after the runtime
/// crate split.
pub use melin_server_runtime::ControlEvent;

/// Application-agnostic server runtime — re-exported from
/// `melin-server-runtime`. Kept under the same `runtime` path so
/// internal callers (`melin_server::runtime::server::*`) keep
/// resolving without churn.
pub use melin_server_runtime as runtime;
/// Trading-specific server wiring — re-exported from
/// `melin-trading-server`. Kept under the same `domain` path so
/// internal callers (`melin_server::domain::exchange_app::*`,
/// `melin_server::domain::request::*`, etc.) keep resolving without
/// churn.
pub use melin_trading_server as domain;
