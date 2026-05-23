//! Trading-specific server wiring.
//!
//! Holds the trading adapter for the generic
//! `melin-server-runtime` pipeline:
//!
//! - [`exchange_app::ServerApp`] — the `Application`-impl newtype
//!   wrapping `melin_exchange_core::exchange::Exchange` (orphan-rule
//!   workaround: the trait lives in `melin-app`, the engine in
//!   `melin-exchange-core`, so the impl can only attach here).
//! - [`app_factory::Factory`] — `AppFactory` impl that
//!   builds empty / seed-ready exchanges and yields the bulk-seed
//!   events the runtime journals on first start.
//! - [`request_decoder::RequestDecoder`] — wire-`Request` →
//!   `TradingEvent` decoder.
//! - [`response_encoder::ResponseEncoder`] —
//!   `ExecutionReport` / `QueryResponse` → wire encoder.
//! - [`event_publisher`] — market-data firehose.

pub mod app_factory;
pub mod exchange_app;
pub mod request_decoder;
pub mod response_encoder;

pub mod event_publisher;

// Crate-root re-exports for the three trading adapters most often
// referenced from outside this crate — the `melin-server` binary, the
// `melin-server-runtime` doc comments, and bench code all reach them by
// short path. Keeps doc-links like `melin_server::Factory`
// resolving without requiring callers to know the internal module layout.
pub use app_factory::Factory;
pub use exchange_app::ServerApp;
pub use request_decoder::RequestDecoder;
pub use response_encoder::ResponseEncoder;
