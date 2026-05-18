//! Trading-specific server wiring.
//!
//! This module holds the parts of `melin-server` that bind the
//! generic accept/journal/matching pipeline to the trading domain:
//! the `ServerApp` newtype that carries the `Application` impl on
//! `melin_engine::exchange::Exchange`, the wire-`Request` decoder,
//! and the market-data firehose publisher. When [`runtime`] becomes
//! fully generic these stay behind in this crate (or move to a
//! sibling `trading-server` crate) as the trading adapter.
//!
//! [`runtime`]: crate::runtime

pub mod exchange_app;
pub mod request;
pub mod response_encoder;

#[cfg(all(feature = "trading", not(feature = "skip-order-exec")))]
pub mod event_publisher;
