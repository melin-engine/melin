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

pub mod admin;
pub mod affinity;
/// AmortizedTimer is used by the shadow stage's clock-check amortization
/// and by the replication sender. Always compiled.
pub(crate) mod amortized_timer;
/// Configurable durability ack policy. Pure logic — defines the
/// `Level` / `Clause` / `Policy` types and a string parser used by
/// the response stage's gate. No threading or I/O concerns.
pub mod durability_policy;
/// Firehose event publisher — trading-only because it depends on
/// `melin-market-data` for book-mirror snapshots.
#[cfg(all(feature = "trading", not(feature = "noop")))]
pub mod event_publisher;
pub mod health;
mod reader;
pub mod request;
mod response;
pub mod tick;

/// Replica failover and shadow snapshotting. Both are transport-level
/// concerns and work for any `A: Application`, so they compile into the
/// noop build too — that is precisely the point of the noop binary
/// (stress the full durable transport without the matching engine).
pub mod replication;
pub mod shadow;

/// Server runtime (TCP accept loop, pipeline bootstrap, auth handshake).
/// Both the trading and no-op builds share the same entry points — the
/// feature-gated `App` alias plus the optional `replication` / `shadow`
/// modules are what actually differ. Cfg branches inside `server.rs`
/// select the right recovery/seed/shadow path per feature.
pub mod server;

#[cfg(feature = "dpdk")]
pub mod dpdk_response;
#[cfg(feature = "dpdk")]
pub mod dpdk_transport;
