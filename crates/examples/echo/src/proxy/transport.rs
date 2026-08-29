//! The seam between the loop and the wire, and the kernel side of it.
//!
//! Both transports are non-blocking and polled from the one thread: a
//! `send` may take part of the data or none, `recv` may return nothing,
//! and `service` is where a transport that has work to do between the
//! two -- a userspace TCP stack -- does it. The loop is generic over the
//! trait, so there is no indirection on the hot path.
//!
//! The proxy bridges either transport to another process's rings and
//! never looks past the trait.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

pub trait Transport {
    /// Queue as much of `data` as the transport will take right now.
    /// `Ok(0)` means none of it: try again after `service`.
    fn send(&mut self, data: &[u8]) -> io::Result<usize>;

    /// Move queued bytes toward the wire and inbound bytes toward `recv`.
    /// Called after every send and on every idle spin.
    fn service(&mut self, now_unix_ns: u64);

    /// Read what has arrived. `Ok(0)` means nothing yet; a closed
    /// connection is an error, never a zero.
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    fn name(&self) -> &'static str;
}

/// Kernel TCP, non-blocking, `TCP_NODELAY`. The baseline the DPDK path is
/// measured against, and what makes the rest of this binary testable on
/// a machine with no DPDK.
pub struct KernelTcp {
    stream: TcpStream,
}

impl KernelTcp {
    pub fn connect(addr: SocketAddr, timeout: Duration) -> io::Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, timeout)?;
        stream.set_nodelay(true)?;
        stream.set_nonblocking(true)?;
        Ok(Self { stream })
    }
}

fn would_block(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

impl Transport for KernelTcp {
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        match self.stream.write(data) {
            Ok(n) => Ok(n),
            Err(e) if would_block(&e) => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn service(&mut self, _now_unix_ns: u64) {}

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.stream.read(buf) {
            Ok(0) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the server closed the connection",
            )),
            Ok(n) => Ok(n),
            Err(e) if would_block(&e) => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn name(&self) -> &'static str {
        "kernel TCP"
    }
}
