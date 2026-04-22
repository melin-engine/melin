//! Application-agnostic transport core for the Melin durable pipeline.
//!
//! Owns the disruptor wiring (journal stage + matching stage + response-stage
//! output ring), the `InputSlot<E>` / `OutputSlot<R, Q>` ring slot types, the
//! `OutputPayload<R, Q>` envelope, and the `Pipeline<A>` / `ReplicaPipeline<A>`
//! builders. Everything here is generic over an `A: Application` — the
//! matching engine (`melin-engine`) is one such application, the no-op
//! demonstration (`melin-noop`) is another.
//!
//! Also owns the application-generic snapshot framing (`snapshot::{save,
//! load}`) and the `JournaledApp<A>` lifecycle wrapper (create / recover /
//! recover_from_snapshot / rotate) that composes a journal writer with an
//! application state machine. The application supplies only the payload
//! bytes via `Application::{snapshot, restore}`; the framing (magic,
//! versions, sequence, chain hash, CRC) lives here.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod journaled_app;
pub mod pipeline;
pub mod snapshot;

#[cfg(test)]
mod test_support;

pub use journaled_app::{JournaledApp, JournaledAppError};
