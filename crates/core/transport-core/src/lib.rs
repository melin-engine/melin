//! Application-agnostic transport core for the Melin durable pipeline.
//!
//! Owns the disruptor wiring (journal stage + matching stage + response-stage
//! output ring), the `InputSlot<E>` / `OutputSlot<R, Q>` ring slot types, the
//! `OutputPayload<R, Q>` envelope, and the `Pipeline<A>` / `ReplicaPipeline<A>`
//! builders. Everything here is generic over an `A: Application` — the
//! matching engine (`melin-engine`) is the canonical implementation.
//!
//! Also owns the application-generic snapshot framing (`snapshot::{save,
//! load}`) and the `JournaledApp<A>` lifecycle wrapper (create / recover /
//! recover_from_snapshot / rotate) that composes a journal writer with an
//! application state machine. The application supplies only the payload
//! bytes via `Application::{snapshot, restore}`; the framing (magic,
//! versions, sequence, chain hash, CRC) lives here.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

/// Cluster-wide durability ack policy: the `Level`/`Clause`/`Policy`
/// cursor-evaluation core used by the response stage's ack gate.
/// Application-agnostic — the operator-facing CLI mode that picks
/// between named policies lives with the consuming application.
pub mod durability_policy;
pub mod journaled_app;
pub mod pipeline;
/// Replication wire protocol, journal-file catch-up, ack queueing,
/// and per-replica observability metrics. Generic over
/// `E: AppEvent`; connection orchestration and key authorization live
/// with the consuming application.
pub mod replication;
pub mod replication_wire;
/// Shadow snapshot stage — replays journal events on a cloned
/// application off the hot path and writes periodic snapshots gated on
/// journal fsync. Generic over `A: Application`.
pub mod shadow;
pub mod snapshot;
pub mod tick;
pub mod trace;

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod pipeline_tests;

pub use journaled_app::{JournaledApp, JournaledAppError};
