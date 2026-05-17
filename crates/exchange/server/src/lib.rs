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

/// The concrete [`melin_app::Application`] this server is built against.
///
/// `ServerApp` is a transparent newtype around `melin_engine::exchange::Exchange`
/// that carries the `Application` impl. Wrapping is required by the
/// orphan rule — the trait lives in `melin-app` and `Exchange` lives in
/// `melin-engine`, so the impl can only attach via a type that's local
/// here. Under `--features skip-order-exec` the engine short-circuits
/// `Exchange::execute` to a single rejection per `SubmitOrder` so the
/// matching hot path is bypassed, but the type stays uniform for
/// downstream modules.
pub type App = domain::exchange_app::ServerApp;

/// Trading-bound ring-slot aliases. The server operates on the trading
/// wire format regardless of which mode it's built in (that's the
/// whole point of skip-order-exec — same protocol, no matching).
pub type JournalEvent = melin_journal::JournalEvent<melin_trading::trading_event::TradingEvent>;
pub type InputSlot =
    melin_transport_core::pipeline::InputSlot<melin_trading::trading_event::TradingEvent>;
pub type OutputSlot = melin_transport_core::pipeline::OutputSlot<
    melin_types::types::ExecutionReport,
    melin_types::types::QueryResponse,
>;
pub type OutputPayload = melin_transport_core::pipeline::OutputPayload<
    melin_types::types::ExecutionReport,
    melin_types::types::QueryResponse,
>;
pub type SectorWriter = melin_journal::SectorWriter<melin_trading::trading_event::TradingEvent>;
pub type BufferedWriter = melin_journal::BufferedWriter<melin_trading::trading_event::TradingEvent>;
pub type JournalReader = melin_journal::JournalReader<melin_trading::trading_event::TradingEvent>;
/// Crate-wide shorthand for the wire-event type. Keeps the
/// `JournalWrite<TradingEvent>` / `JournalStageRun<TradingEvent, ...>`
/// bounds at every generic boot-path call site short.
pub type TradingEvent = melin_trading::trading_event::TradingEvent;

/// `TradingEvent`-bound alias for the generic journal stage in
/// transport-core. `W` is the concrete writer the caller picked at
/// boot (sector vs buffered).
pub type JournalStage<W> =
    melin_transport_core::pipeline::JournalStage<melin_trading::trading_event::TradingEvent, W>;

// Re-export the writer-selection enum + the generic pipeline / trace /
// codec / replication modules at the server crate root. Bench and any
// other downstream consumer now reach the LMAX-pipeline plumbing
// through `melin-server` instead of through `melin-engine` — engine
// is the matching domain library, server is the wiring layer.
pub use melin_app::unix_epoch_nanos;
/// Re-export of the journal replication module — namespaced under
/// `journal_replication` to avoid colliding with the server's own
/// `replication` module (the orchestrator that wraps it).
pub use melin_journal::replication as journal_replication;
pub use melin_journal::{
    AsyncWriteBatch, JournalError, JournalWrite, JournalWriterMode, RawJournalScanner,
    checkpoint_interval, codec, create_fresh_replica,
};
pub use melin_transport_core::{pipeline, trace};

/// Control plane event the accept loop and response stage exchange.
/// Defined at the crate root so both build modes refer to the same
/// type (it's transport-agnostic — the payload is a socket fd +
/// writer, not an app event).
pub enum ControlEvent {
    Connected {
        connection_id: u64,
        fd: std::os::unix::io::RawFd,
        writer: melin_protocol::blocking::BlockingFrameWriter<Box<dyn std::io::Write + Send>>,
    },
    Disconnected {
        connection_id: u64,
    },
}

/// Trading-specific server wiring — wire-`Request` decode,
/// `OutputPayload` response encoding, the `ServerApp` newtype that
/// carries the `Application` impl, and the market-data firehose.
pub mod domain;
/// Application-agnostic server runtime — accept loop, frame reader,
/// durability policy, admin endpoint, replication, DPDK transport.
/// Generic in shape; the long-term plan is to make it parametric
/// over `A: Application` and move it into `crates/core/`.
pub mod runtime;
