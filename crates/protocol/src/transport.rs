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
use std::os::unix::io::AsRawFd;

/// Blocking transport listener for the server accept loop.
///
/// Accepts new connections and returns blocking read/write halves that
/// can be handed directly to the reader thread and the response thread.
/// No async-to-blocking conversion needed.
///
/// `Read` requires `AsRawFd` so the io_uring reader can register
/// connection fds for multiplexed I/O. `Write` requires `AsRawFd` so
/// the response stage can submit SEND SQEs with the raw fd.
pub trait BlockingTransportListener: Send + 'static {
    type Read: io::Read + AsRawFd + Send + 'static;
    type Write: io::Write + AsRawFd + Send + 'static;

    /// Accept a new connection, returning blocking read/write halves
    /// and the peer address.
    fn accept(&mut self) -> io::Result<(Self::Read, Self::Write, SocketAddr)>;

    /// Set the listener to non-blocking mode for shutdown-aware accept loops.
    /// Accepted connections are still set to blocking mode individually.
    fn set_nonblocking(&mut self, _nonblocking: bool) {}
}
