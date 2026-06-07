//! Application-agnostic replication protocol and helpers.
//!
//! Wire framing, message types, journal-file catch-up, ack queueing,
//! and per-replica metrics. Generic over `E: AppEvent` so the same
//! transport works for any application built on the Melin pipeline.
//!
//! The consuming server owns its own connection orchestration (TCP
//! listener, replica connect loop, pipeline factory, app cloning) and
//! key authorization — those still live in the application's server
//! crate because they depend on concrete `Application` types and the
//! application's permission model.

pub mod ack_queue;
pub mod catchup;
pub mod cursors;
pub mod metrics;
pub mod protocol;
pub mod sent;

#[cfg(test)]
mod handoff_test;

pub use cursors::ReplicaCursors;
pub use metrics::ReplicationMetrics;
pub use sent::SentHighWater;
