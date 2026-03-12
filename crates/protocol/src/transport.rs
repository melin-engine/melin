//! Transport abstraction layer.
//!
//! Defines the blocking transport listener trait used by the server
//! accept loop. Transport-specific implementations (TCP, UDS) provide
//! blocking read/write halves directly — no async runtime needed.
//!
//! Frame-level I/O is handled by `BlockingFrameReader` / `BlockingFrameWriter`
//! in the `blocking` module, which are generic over any `Read`/`Write` type.

use std::io;
use std::net::SocketAddr;

/// Blocking transport listener for the server accept loop.
///
/// Accepts new connections and returns blocking read/write halves that
/// can be handed directly to reader threads and the response thread.
/// No async-to-blocking conversion needed.
pub trait BlockingTransportListener: Send + 'static {
    type Read: io::Read + Send + 'static;
    type Write: io::Write + Send + 'static;

    /// Accept a new connection, returning blocking read/write halves
    /// and the peer address.
    fn accept(&mut self) -> io::Result<(Self::Read, Self::Write, SocketAddr)>;
}
