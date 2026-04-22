//! Trading / no-op server library — exposes server startup for embedding
//! (benchmarks, tests). The concrete [`Application`] that plugs into the
//! generic pipeline is selected at compile time through the `trading`
//! and `noop` cargo features, exactly one of which must be enabled.

#[cfg(all(feature = "trading", feature = "noop"))]
compile_error!(
    "melin-server must be built with exactly one of the `trading` or `noop` features enabled"
);
#[cfg(not(any(feature = "trading", feature = "noop")))]
compile_error!(
    "melin-server must be built with exactly one of the `trading` or `noop` features enabled"
);

/// The concrete [`Application`] this server is built against.
///
/// With `--features trading` (default): the full matching engine.
/// With `--features noop --no-default-features`: the transport-only
/// benchmark app. Downstream modules refer to it as [`App`] so there is
/// a single place to swap.
#[cfg(all(feature = "trading", not(feature = "noop")))]
pub type App = melin_engine::exchange::Exchange;

#[cfg(all(feature = "noop", not(feature = "trading")))]
pub type App = melin_noop::NoopApp;

/// Trading-bound ring-slot aliases. The server operates on the trading
/// wire format regardless of which application is plugged in (that's
/// the whole point of noop — same protocol, different matcher).
pub type JournalEvent = melin_journal::JournalEvent<melin_trading::trading_event::TradingEvent>;
pub type InputSlot =
    melin_transport_core::pipeline::InputSlot<melin_trading::trading_event::TradingEvent>;
pub type OutputSlot =
    melin_transport_core::pipeline::OutputSlot<melin_trading::types::ExecutionReport>;
pub type OutputPayload =
    melin_transport_core::pipeline::OutputPayload<melin_trading::types::ExecutionReport>;
pub type JournalWriter = melin_journal::JournalWriter<melin_trading::trading_event::TradingEvent>;
pub type JournalReader = melin_journal::JournalReader<melin_trading::trading_event::TradingEvent>;

/// Control plane event the accept loop and response stage exchange.
/// Defined at the crate root so both the trading `server` and the noop
/// `server_noop` can refer to the same type (it's transport-agnostic —
/// the payload is a socket fd + writer, not an app event).
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

pub mod affinity;
pub(crate) mod amortized_timer;
/// Firehose event publisher — trading-only because it depends on
/// `melin-market-data` for book-mirror snapshots.
#[cfg(all(feature = "trading", not(feature = "noop")))]
pub mod event_publisher;
pub mod health;
pub mod promote;
mod reader;
pub mod request;
mod response;
pub mod tick;

// --- Trading-only modules ---
// The full matching-engine server assumes `Exchange`-based recovery,
// shadow snapshotting, and replica failover — all currently coupled to
// `JournaledExchange` / `Exchange` internals. When building the noop
// binary for transport-only benchmarks, a much smaller server entry
// point ships instead.
#[cfg(all(feature = "trading", not(feature = "noop")))]
pub mod replication;
#[cfg(all(feature = "trading", not(feature = "noop")))]
pub mod server;
#[cfg(all(feature = "trading", not(feature = "noop")))]
pub mod shadow;

// --- No-op server ---
// Minimal primary-only server (no replication, no shadow, no snapshot
// rotation) used for transport-level benchmarking against the lan-bench
// suite. Exposes the same `run` / `run_with_shutdown` surface as
// `server::*` so `main.rs` doesn't branch on the feature.
#[cfg(all(feature = "noop", not(feature = "trading")))]
#[path = "server_noop.rs"]
pub mod server;

#[cfg(feature = "dpdk")]
pub mod dpdk_response;
#[cfg(feature = "dpdk")]
pub mod dpdk_transport;
