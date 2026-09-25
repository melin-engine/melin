//! Application-agnostic replication protocol and helpers.
//!
//! Wire framing, message types, journal-file catch-up, ack queueing,
//! and per-replica metrics. Generic over `E: AppEvent` so the same
//! transport works for any application built on the Melin pipeline.
//!
//! The consuming server owns its own connection orchestration (TCP
//! listener, replica connect loop, pipeline factory, app cloning) and
//! key authorization against the node's `authorized_keys` — those live
//! in the server runtime, which builds the pipeline for the concrete
//! `Application` and holds the keys table.

pub mod ack_queue;
pub mod archive;
pub mod catchup;
pub mod cursors;
pub mod metrics;
pub mod protocol;
pub mod sent;
pub mod validate;

#[cfg(test)]
mod handoff_test;

pub use cursors::ReplicaCursors;
pub use metrics::ReplicationMetrics;
pub use sent::SentHighWater;
