//! Domain-free wire layer: length-prefixed framing, blocking frame
//! reader/writer, an I/O-free frame splitter for programs that run their
//! own I/O loop, and a transport-listener abstraction with TCP and
//! Unix-domain-socket implementations. Application messages and their
//! codec live with each application, behind `melin-app`'s request
//! decoder and response encoder.

pub mod blocking;
pub mod control;
pub mod control_codec;
pub mod error;
pub mod framing;
pub mod tcp;
pub mod transport;
pub mod uds;
