//! Pluggable UDP transport substrate.
//!
//! The protocol logic (sender loop in Task #6, receiver loop in Task #7)
//! is generic over the [`UdpTransport`] trait so the same code runs over
//! the kernel network stack today and over DPDK / user-space UDP / RDMA
//! tomorrow without recompiling the protocol.
//!
//! Generic — not `dyn` — so the compiler monomorphizes the hot path and
//! inlines straight through the trait method to the underlying syscall
//! (or DPDK PMD call).
//!
//! # Backends
//!
//! - [`KernelUdp`] — `std::net::UdpSocket` in non-blocking mode. The
//!   default for unit tests, integration tests, and any deployment
//!   without a kernel-bypass NIC.
//! - DPDK backend — deferred (Task DEFER #A); the existing `melin-dpdk`
//!   crate's PMD will plug in here.
//!
//! [`KernelUdp`]: KernelUdp

use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};

/// Pluggable UDP datagram transport. Implementations represent one bound
/// endpoint (a `local_addr`) that can send to and receive from arbitrary
/// `SocketAddr`s.
///
/// All methods are non-blocking. `recv_from` returns `Ok(None)` when no
/// datagram is immediately available, so callers (sender / receiver
/// loops) can poll without dedicated reactors.
pub trait UdpTransport: Send + Sync {
    /// Send `bytes` as a single UDP datagram to `dst`. Returns the number
    /// of bytes accepted by the kernel (always `bytes.len()` in
    /// practice; UDP is all-or-nothing per datagram).
    ///
    /// Returns `WouldBlock` if the send buffer is full (rare for UDP;
    /// typically only happens with very small `SO_SNDBUF` and very high
    /// send rates).
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize>;

    /// Try to receive one datagram into `buf`. Returns `Ok(None)` when
    /// no datagram is ready. On `Ok(Some((addr, len)))`, the first
    /// `len` bytes of `buf` are valid and `addr` is the sender.
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>>;

    /// Local socket address (after binding).
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Join an IPv4 multicast group on the given local interface.
    /// `iface` of `0.0.0.0` lets the kernel pick the default interface.
    fn join_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()>;

    /// Leave a previously-joined multicast group.
    fn leave_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()>;
}

/// Kernel UDP socket backend (`std::net::UdpSocket`, non-blocking).
pub struct KernelUdp {
    socket: UdpSocket,
}

impl KernelUdp {
    /// Bind to `local`. The socket is set to non-blocking mode.
    pub fn bind(local: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(local)?;
        socket.set_nonblocking(true)?;
        Ok(Self { socket })
    }

    /// Set the multicast TTL for outbound packets.
    pub fn set_multicast_ttl_v4(&self, ttl: u32) -> io::Result<()> {
        self.socket.set_multicast_ttl_v4(ttl)
    }

    /// Toggle whether multicast packets the local host sends are looped
    /// back to local subscribers. On for tests and for "self-tail"
    /// scenarios; off for production fan-out where the publisher is on
    /// a different host than its subscribers.
    pub fn set_multicast_loop_v4(&self, on: bool) -> io::Result<()> {
        self.socket.set_multicast_loop_v4(on)
    }
}

impl UdpTransport for KernelUdp {
    #[inline]
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send_to(bytes, dst)
    }

    #[inline]
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        match self.socket.recv_from(buf) {
            Ok((len, addr)) => Ok(Some((addr, len))),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    fn join_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
        self.socket.join_multicast_v4(&group, &iface)
    }

    fn leave_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
        self.socket.leave_multicast_v4(&group, &iface)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::{Duration, Instant};

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    /// Spin briefly waiting for one datagram to arrive — UDP loopback is
    /// fast but recv may need a couple of polls in some kernels.
    fn recv_one<T: UdpTransport>(t: &T, buf: &mut [u8]) -> (SocketAddr, usize) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(got) = t.recv_from(buf).expect("recv failed") {
                return got;
            }
            if Instant::now() > deadline {
                panic!("no datagram within deadline");
            }
            std::thread::sleep(Duration::from_micros(100));
        }
    }

    #[test]
    fn bind_returns_local_addr_with_chosen_port() {
        let t = KernelUdp::bind(loopback(0)).unwrap();
        let addr = t.local_addr().unwrap();
        assert_eq!(addr.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_ne!(addr.port(), 0, "kernel must assign a non-zero port");
    }

    #[test]
    fn send_recv_round_trip_unicast() {
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        let send = KernelUdp::bind(loopback(0)).unwrap();
        let send_addr = send.local_addr().unwrap();

        let payload = b"hello, rumcast";
        let n = send.send_to(recv_addr, payload).unwrap();
        assert_eq!(n, payload.len());

        let mut buf = [0u8; 64];
        let (from, len) = recv_one(&recv, &mut buf);
        assert_eq!(from, send_addr);
        assert_eq!(&buf[..len], payload);
    }

    #[test]
    fn recv_from_returns_none_when_no_datagram_ready() {
        let t = KernelUdp::bind(loopback(0)).unwrap();
        let mut buf = [0u8; 64];
        let result = t.recv_from(&mut buf).unwrap();
        assert!(result.is_none(), "expected None on idle socket");
    }

    #[test]
    fn send_to_unbound_destination_does_not_block() {
        // Sending to an address with no listener is fine for UDP — the
        // datagram is silently dropped on the receiver side. The send
        // call must still succeed (or at most return WouldBlock, never
        // hang).
        let t = KernelUdp::bind(loopback(0)).unwrap();
        let dst = loopback(1); // port 1 typically unbound
        let result = t.send_to(dst, b"into the void");
        // Either succeeds or returns ConnectionRefused/WouldBlock
        // depending on kernel behavior — but never hangs and never
        // panics.
        match result {
            Ok(_) | Err(_) => (),
        }
    }

    #[test]
    fn multiple_datagrams_received_in_order() {
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        let send = KernelUdp::bind(loopback(0)).unwrap();

        for i in 0..5u8 {
            send.send_to(recv_addr, &[i; 16]).unwrap();
        }

        // UDP doesn't guarantee in-order delivery in general, but on
        // localhost it's reliable enough for this test.
        let mut buf = [0u8; 16];
        for i in 0..5u8 {
            let (_, len) = recv_one(&recv, &mut buf);
            assert_eq!(len, 16);
            assert!(
                buf[..len].iter().all(|&b| b == i),
                "expected fill of {i}, got {:?}",
                &buf[..len]
            );
        }
    }

    #[test]
    fn multicast_join_send_recv() {
        // 239.x.x.x is admin-scoped multicast — link-local only, won't
        // leak past the host. We use a single socket that joins the
        // group AND sends to it: with multicast loop enabled, the
        // kernel delivers the packet right back to us. This avoids the
        // cross-interface routing pitfalls of two-socket multicast on
        // localhost (the kernel can pick any multicast-enabled
        // interface for sending, which may not be the one the receiver
        // bound to).
        let group = Ipv4Addr::new(239, 1, 2, 3);
        let recv_port = {
            let scratch = UdpSocket::bind("127.0.0.1:0").unwrap();
            scratch.local_addr().unwrap().port()
        };
        let socket = KernelUdp::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            recv_port,
        ))
        .unwrap();
        socket.set_multicast_loop_v4(true).unwrap();
        socket.set_multicast_ttl_v4(1).unwrap();
        // join_multicast_v4 with UNSPECIFIED iface lets the kernel
        // choose; on hosts without multicast-enabled interfaces the
        // join itself fails — skip the test in that case to keep CI
        // green on minimal containers.
        if socket
            .join_multicast_v4(group, Ipv4Addr::UNSPECIFIED)
            .is_err()
        {
            eprintln!("skipping: no multicast-capable interface available");
            return;
        }

        let group_dst = SocketAddr::new(IpAddr::V4(group), recv_port);
        let payload = b"multicast hello";
        socket.send_to(group_dst, payload).unwrap();

        // Multicast loopback may take a touch longer than unicast on
        // some kernels; the recv_one helper has a 2-second deadline.
        let mut buf = [0u8; 64];
        let (_from, len) = recv_one(&socket, &mut buf);
        assert_eq!(&buf[..len], payload);

        socket
            .leave_multicast_v4(group, Ipv4Addr::UNSPECIFIED)
            .unwrap();
    }

    /// Smoke test that the trait is callable through a generic
    /// (monomorphized) function — verifying that callers like the
    /// future sender / receiver loops can be parameterized over `T:
    /// UdpTransport`.
    #[test]
    fn trait_usable_through_generic() {
        fn echo_one<T: UdpTransport>(t: &T, dst: SocketAddr, msg: &[u8]) -> usize {
            t.send_to(dst, msg).unwrap()
        }
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let send = KernelUdp::bind(loopback(0)).unwrap();
        let n = echo_one(&send, recv.local_addr().unwrap(), b"hi");
        assert_eq!(n, 2);
    }
}
