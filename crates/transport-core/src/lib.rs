//! Application-agnostic transport core for the Melin durable pipeline.
//!
//! Owns the disruptor wiring (journal stage + matching stage + response-stage
//! output ring), the `InputSlot<E>` / `OutputSlot<R>` ring slot types, the
//! `OutputPayload<R>` envelope, and the `Pipeline<A>` / `ReplicaPipeline<A>`
//! builders. Everything here is generic over an `A: Application` — the
//! matching engine (`melin-engine`) is one such application, the no-op
//! demonstration (`melin-noop`) is another.
//!
//! Snapshot framing and the `JournaledExchange`-style synchronous
//! wrappers still live with the concrete application (in `melin-engine`)
//! because they're entangled with Exchange internals; this crate stays
//! focused on the hot-path disruptor + journal fan-out that any
//! application plugs into.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

pub mod journaled_app;
pub mod pipeline;
pub mod snapshot;

pub use journaled_app::{JournaledApp, JournaledAppError};
