//! Application-agnostic server runtime.
//!
//! This module groups the parts of `melin-server` that are generic in
//! shape — the accept loop, frame reader, durability-policy wiring,
//! admin endpoint, replication, and DPDK transport. They still
//! reference the crate's concrete `App` / `TradingEvent` aliases
//! defined at the crate root, but the long-term plan is to make this
//! subtree fully generic over `A: Application` and move it into
//! `crates/core/server-runtime/`. The accompanying [`domain`] module
//! holds the trading-specific wiring (request decode, response
//! encode, the `ServerApp` newtype, the firehose publisher).
//!
//! [`domain`]: crate::domain

pub mod admin;
pub mod durability_policy;
pub mod reader;
pub mod replication;
pub mod server;

#[cfg(feature = "dpdk")]
pub mod dpdk_transport;
