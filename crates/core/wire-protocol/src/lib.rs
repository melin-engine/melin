//! Domain-free wire layer: length-prefixed framing, blocking frame
//! reader/writer, and a transport-listener abstraction with TCP and
//! Unix-domain-socket implementations. Trading-shaped messages and
//! their codec live in the `melin-protocol` crate, which builds on
//! this one.

pub mod blocking;
pub mod control;
pub mod control_codec;
pub mod error;
pub mod tcp;
pub mod transport;
pub mod uds;
