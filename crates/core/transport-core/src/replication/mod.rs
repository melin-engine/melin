//! Application-agnostic replication protocol and helpers.
//!
//! Wire framing, message types, and journal-file catch-up. Generic
//! over `E: AppEvent` so the same transport works for any application
//! built on the Melin pipeline.
//!
//! The consuming server owns its own connection orchestration (TCP
//! listener, replica connect loop, pipeline factory, app cloning) and
//! key authorization — those still live in the application's server
//! crate because they depend on concrete `Application` types and the
//! application's permission model.

pub mod catchup;
pub mod protocol;
