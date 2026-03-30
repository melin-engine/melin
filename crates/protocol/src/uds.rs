//! Unix domain socket transport implementation.
//!
//! Avoids the TCP/IP stack entirely — no checksums, congestion control,
//! or connection tracking. Used as a benchmarking comparison point to
//! isolate TCP stack overhead from application-level latency.
//! Production deployments use TCP (required for remote clients).

use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::{Path, PathBuf};

use crate::transport::BlockingTransportListener;

/// Blocking Unix domain socket listener.
///
/// Used by the server accept loop. Accepted connections are in blocking
/// mode — no async runtime needed.
pub struct BlockingUdsListener {
    listener: std::os::unix::net::UnixListener,
    /// Store the path so we can report it and for cleanup.
    path: PathBuf,
}

impl BlockingUdsListener {
    /// Bind to the given filesystem path.
    ///
    /// Removes any stale socket file at `path` before binding.
    pub fn bind(path: &Path) -> io::Result<Self> {
        // Remove stale socket file if it exists (previous unclean shutdown).
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        Ok(Self {
            listener,
            path: path.to_owned(),
        })
    }

    /// Returns a synthetic `SocketAddr` (127.0.0.1:0) since UDS doesn't
    /// have IP addresses. The server logs this, so we need something valid.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok("127.0.0.1:0".parse().expect("valid addr"))
    }

    /// Returns the filesystem path this listener is bound to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl AsRawFd for BlockingUdsListener {
    fn as_raw_fd(&self) -> RawFd {
        self.listener.as_raw_fd()
    }
}

impl BlockingTransportListener for BlockingUdsListener {
    type Read = std::os::unix::net::UnixStream;
    type Write = std::os::unix::net::UnixStream;

    fn accept(
        &mut self,
    ) -> io::Result<(
        std::os::unix::net::UnixStream,
        std::os::unix::net::UnixStream,
        SocketAddr,
    )> {
        let (stream, _unix_addr) = self.listener.accept()?;
        // UDS doesn't have IP addresses — return a synthetic loopback address.
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("valid addr");
        let read_half = stream.try_clone()?;
        Ok((read_half, stream, addr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blocking::{BlockingFrameReader, BlockingFrameWriter};

    #[test]
    fn accept_and_exchange_frame() {
        let dir = tempfile::tempdir().unwrap();
        let sock_path = dir.path().join("test.sock");

        let listener = BlockingUdsListener::bind(&sock_path).unwrap();

        let sock = sock_path.clone();
        let handle = std::thread::spawn(move || {
            let mut listener = listener;
            let (read, write, _addr) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(read);
            let mut writer = BlockingFrameWriter::new(write);

            let frame = reader.read_frame().unwrap().unwrap();
            writer.write_frame(frame).unwrap();
            writer.flush().unwrap();
        });

        let stream = std::os::unix::net::UnixStream::connect(&sock).unwrap();
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
