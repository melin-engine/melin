//! TCP transport implementation.
//!
//! Blocking TCP listener for the server accept loop. Sets `TCP_NODELAY = true`
//! on accepted connections to avoid Nagle's algorithm adding latency.

use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};

use crate::transport::BlockingTransportListener;

/// Blocking TCP listener backed by `std::net::TcpListener`.
///
/// Used by the server accept loop. Accepted connections are already in
/// blocking mode with `TCP_NODELAY` set — no async runtime needed.
pub struct BlockingTcpListener {
    listener: std::net::TcpListener,
}

impl BlockingTcpListener {
    /// Bind to the given address and start listening.
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = std::net::TcpListener::bind(addr)?;
        Ok(Self { listener })
    }

    /// Returns the local address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl AsRawFd for BlockingTcpListener {
    fn as_raw_fd(&self) -> RawFd {
        self.listener.as_raw_fd()
    }
}

impl BlockingTransportListener for BlockingTcpListener {
    type Read = std::net::TcpStream;
    type Write = std::net::TcpStream;

    fn accept(&mut self) -> io::Result<(std::net::TcpStream, std::net::TcpStream, SocketAddr)> {
        let (stream, addr) = self.listener.accept()?;
        stream.set_nodelay(true)?;
        // Ensure accepted connections are in blocking mode even if the
        // listener is non-blocking (for shutdown support).
        stream.set_nonblocking(false)?;
        let read_half = stream.try_clone()?;
        Ok((read_half, stream, addr))
    }

    fn set_nonblocking(&mut self, nonblocking: bool) {
        let _ = self.listener.set_nonblocking(nonblocking);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocking::{BlockingFrameReader, BlockingFrameWriter};

    #[test]
    fn accept_and_exchange_frame() {
        let listener = BlockingTcpListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let mut listener = listener;
            let (read, write, _addr) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(read);
            let mut writer = BlockingFrameWriter::new(write);

            let frame = reader.read_frame().unwrap().unwrap();
            writer.write_frame(frame).unwrap();
            writer.flush().unwrap();
        });

        let stream = std::net::TcpStream::connect(addr).unwrap();
        let mut writer = BlockingFrameWriter::new(stream.try_clone().unwrap());
        let mut reader = BlockingFrameReader::new(stream);

        let data = b"hello trading";
        writer.write_frame(data).unwrap();
        writer.flush().unwrap();

        let received = reader.read_frame().unwrap().unwrap();
        assert_eq!(received, data);

        handle.join().unwrap();
    }
}
