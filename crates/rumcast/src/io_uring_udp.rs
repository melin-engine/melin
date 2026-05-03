//! io_uring-backed UDP transport.
//!
//! Submits a pool of `RecvMsg` SQEs upfront so the kernel can fill recv
//! buffers without userspace issuing a syscall per packet. On `recv_from`,
//! we harvest any completed CQEs — zero syscalls when the pool is in-flight
//! and completions land in the CQ ring. On `park`, we call
//! `submit_and_wait` so the thread sleeps until a packet arrives or the
//! timeout elapses, replacing busy-spin or `sleep(10µs)`.
//!
//! `send_to` uses the standard kernel path (`sendto`). UDP sends are
//! already fast (one CQE per send would add overhead, not reduce it).

use std::collections::VecDeque;
use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::os::unix::io::AsRawFd;
use std::sync::Mutex;
use std::time::Duration;

use io_uring::types::Fd;
use io_uring::{IoUring, opcode};

use crate::transport::UdpTransport;

/// Number of pre-submitted RecvMsg SQEs. Larger pool = more in-flight
/// receives before the first `submit` call; 64 matches SharedUdp's
/// PER_DIRECTION_QUEUE_CAP and is well within a 256-entry ring.
const RECV_POOL: usize = 64;

/// Must match the rumcast frame size limit used elsewhere.
const BUF_SIZE: usize = 2048;

/// Number of resubmitted RecvMsg SQEs that may sit in the userspace
/// SQ before `recv_from` issues an `io_uring_enter` to hand them to
/// the kernel. Bounds the worst-case shrinkage of the in-flight pool
/// at `RECV_POOL - SUBMIT_THRESHOLD` while keeping syscalls amortized
/// (one `submit` per `SUBMIT_THRESHOLD` packets, not per packet).
const SUBMIT_THRESHOLD: usize = 16;

/// One pinned receive slot. Heap-allocated via `Box` so the raw
/// pointers in `iov` and `msg` remain stable even when the owning
/// `Vec<Box<RecvSlot>>` is pushed into or reallocated.
struct RecvSlot {
    buf: [u8; BUF_SIZE],
    iov: libc::iovec,
    name: libc::sockaddr_storage,
    msg: libc::msghdr,
}

impl RecvSlot {
    fn new() -> Box<Self> {
        // Build with zeroed fields first, then patch self-references
        // after the heap allocation (address is stable from here on).
        let mut s = Box::new(Self {
            buf: [0u8; BUF_SIZE],
            iov: unsafe { std::mem::zeroed() },
            name: unsafe { std::mem::zeroed() },
            msg: unsafe { std::mem::zeroed() },
        });
        s.iov.iov_base = s.buf.as_mut_ptr() as *mut libc::c_void;
        s.iov.iov_len = BUF_SIZE;
        s.msg.msg_iov = &mut s.iov as *mut _;
        s.msg.msg_iovlen = 1;
        s.msg.msg_name = &mut s.name as *mut _ as *mut libc::c_void;
        s.msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        s
    }
}

struct ReadyPacket {
    slot_idx: usize,
    len: usize,
    from: SocketAddr,
}

struct IoUringInner {
    ring: IoUring,
    fd: Fd,
    slots: Vec<Box<RecvSlot>>,
    /// Indices of slots that have been reaped from the CQ and are
    /// waiting for resubmission after the caller copies their data.
    pending_resubmit: Vec<usize>,
    ready: VecDeque<ReadyPacket>,
    /// Count of RecvMsg SQEs pushed to the SQ but not yet handed to
    /// the kernel via `submit`. `recv_from` calls `submit` when this
    /// reaches `SUBMIT_THRESHOLD`; `park` always submits via
    /// `submit_with_args`.
    unsubmitted: usize,
}

// RecvSlot contains raw pointers (iov_base, msg_iov, msg_name) that are
// self-referential within the same Box allocation. All access is
// serialized through Mutex<IoUringInner>, so cross-thread sharing is safe.
unsafe impl Send for IoUringInner {}

impl IoUringInner {
    /// Harvest all available CQEs without blocking.
    fn harvest(&mut self) {
        // Safety: we harvest entries while holding exclusive access
        // (&mut self via Mutex). The kernel has written to slot.buf /
        // slot.name for every CQE we pop here — they are safe to read.
        let cq = self.ring.completion();
        for cqe in cq {
            let slot_idx = cqe.user_data() as usize;
            let res = cqe.result();
            if res < 0 {
                // Receive error (e.g. ENOBUFS) — resubmit slot
                // without delivering a packet.
                self.pending_resubmit.push(slot_idx);
                continue;
            }
            let len = res as usize;
            let slot = &self.slots[slot_idx];
            let from = sockaddr_to_socket_addr(&slot.name);
            self.ready.push_back(ReadyPacket {
                slot_idx,
                len,
                from,
            });
        }
        self.resubmit_pending();
    }

    /// Re-submit all slots that have been reaped but not yet
    /// returned to the kernel. Called after harvesting CQEs and
    /// after the caller has finished copying data from ready packets.
    fn resubmit_pending(&mut self) {
        let fd = self.fd;
        let pushed = self.pending_resubmit.len();
        let mut sq = self.ring.submission();
        for &idx in &self.pending_resubmit {
            // Reset msg_namelen — the kernel mutates it to the actual
            // address length on completion (16 for IPv4, 28 for IPv6),
            // so without this reset a subsequent IPv6 recv would see a
            // truncated sockaddr buffer.
            let slot = &mut self.slots[idx];
            slot.msg.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
            // Safety: slot at `idx` is not in-flight (we just reaped
            // it). The msghdr pointer is heap-stable (Box allocation).
            let msg_ptr = &mut slot.msg as *mut libc::msghdr;
            let entry = opcode::RecvMsg::new(fd, msg_ptr)
                .build()
                .user_data(idx as u64);
            // SQ full would mean we sized the ring wrong relative to
            // RECV_POOL — a build-time invariant, not a runtime
            // condition (the ring is 256, the pool is 64).
            unsafe {
                sq.push(&entry)
                    .expect("SQ full — ring undersized for RECV_POOL")
            };
        }
        self.pending_resubmit.clear();
        self.unsubmitted += pushed;
        // Submit happens in recv_from (above threshold) or in park
        // (always) — this fn just stages SQEs.
    }
}

/// io_uring UDP transport. Implements [`UdpTransport`] with batched
/// recvmsg via a pre-submitted SQE pool. Sends via the kernel fast
/// path (no io_uring overhead for sends).
pub struct IoUringUdp {
    socket: UdpSocket,
    inner: Mutex<IoUringInner>,
}

impl IoUringUdp {
    /// Bind to `local`. Creates a 256-entry ring and pre-submits
    /// `RECV_POOL` RecvMsg SQEs.
    pub fn bind(local: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(local)?;
        // Non-blocking so send_to doesn't stall; recv_from uses the
        // ring, which is event-driven and never blocks inline.
        socket.set_nonblocking(true)?;
        let fd = Fd(socket.as_raw_fd());

        let mut ring: IoUring = IoUring::builder().build(256)?;

        // Pre-allocate recv slots and submit RecvMsg for each.
        let mut slots: Vec<Box<RecvSlot>> = (0..RECV_POOL).map(|_| RecvSlot::new()).collect();
        {
            let mut sq = ring.submission();
            for (idx, slot) in slots.iter_mut().enumerate() {
                // Safety: msghdr points into the Box's heap allocation,
                // which remains valid as long as `slots` lives.
                let msg_ptr = &mut slot.msg as *mut libc::msghdr;
                let entry = opcode::RecvMsg::new(fd, msg_ptr)
                    .build()
                    .user_data(idx as u64);
                unsafe { sq.push(&entry).expect("SQ full on init") };
            }
        }
        // Submit all initial SQEs to the kernel.
        ring.submitter().submit()?;

        Ok(Self {
            socket,
            inner: Mutex::new(IoUringInner {
                ring,
                fd,
                slots,
                pending_resubmit: Vec::with_capacity(RECV_POOL),
                ready: VecDeque::new(),
                unsubmitted: 0,
            }),
        })
    }
}

impl UdpTransport for IoUringUdp {
    #[inline]
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send_to(bytes, dst)
    }

    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        let mut inner = self.inner.lock().expect("io_uring mutex poisoned");
        inner.harvest();
        let result = match inner.ready.pop_front() {
            None => Ok(None),
            Some(pkt) => {
                let n = pkt.len.min(buf.len());
                buf[..n].copy_from_slice(&inner.slots[pkt.slot_idx].buf[..n]);
                inner.pending_resubmit.push(pkt.slot_idx);
                inner.resubmit_pending();
                Ok(Some((pkt.from, n)))
            }
        };
        // Hand staged SQEs to the kernel in batches so a steady-state
        // recv loop (no `park` between calls) keeps the in-flight pool
        // primed. Without this, the pool drains to zero after RECV_POOL
        // packets and the kernel silently drops every subsequent
        // datagram until the next park.
        if inner.unsubmitted >= SUBMIT_THRESHOLD {
            // Errors here would indicate an io_uring setup bug (EBADF,
            // EINVAL); WouldBlock isn't possible for plain submit. If
            // submit fails, the next call will retry — surface via the
            // counter staying non-zero rather than panicking on a
            // transient kernel state.
            if inner.ring.submitter().submit().is_ok() {
                inner.unsubmitted = 0;
            }
        }
        result
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

    fn park(&self, timeout: Duration) {
        use io_uring::types::{SubmitArgs, Timespec};

        let mut inner = self.inner.lock().expect("io_uring mutex poisoned");

        // If there's already data in the ready queue (harvested but
        // not yet consumed by recv_from), don't block.
        if !inner.ready.is_empty() {
            return;
        }

        let ts = Timespec::from(timeout);
        let args = SubmitArgs::new().timespec(&ts);
        // submit_with_args(1, …): submit pending SQEs and wait until
        // at least 1 CQE arrives or the timeout fires. Only ETIME
        // (timer expired) and EINTR (signal) are expected wakeups —
        // anything else indicates a real io_uring error worth
        // surfacing. We don't propagate from `park` (the trait method
        // returns ()) but we at least avoid silently masking bugs by
        // matching explicitly.
        match inner.ring.submitter().submit_with_args(1, &args) {
            Ok(_) => inner.unsubmitted = 0,
            Err(e) => {
                let raw = e.raw_os_error();
                if raw == Some(libc::ETIME) || raw == Some(libc::EINTR) {
                    // Expected wakeups — submission still happened
                    // before the wait timed out / was interrupted, so
                    // staged SQEs are with the kernel.
                    inner.unsubmitted = 0;
                } else {
                    // Real error. Submission may not have happened;
                    // leave `unsubmitted` so the next recv_from retries.
                    tracing::warn!(error = %e, "io_uring submit_with_args failed");
                }
            }
        }

        // Harvest whatever arrived.
        inner.harvest();
    }
}

/// Convert a `sockaddr_storage` filled by the kernel into a `SocketAddr`.
/// Only IPv4 and IPv6 are supported — anything else is mapped to
/// `0.0.0.0:0` (will be filtered by the caller's frame parser).
fn sockaddr_to_socket_addr(storage: &libc::sockaddr_storage) -> SocketAddr {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            // Safety: ss_family == AF_INET guarantees the storage
            // holds a valid sockaddr_in.
            let sin = unsafe { &*(storage as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
            let port = u16::from_be(sin.sin_port);
            SocketAddr::new(ip.into(), port)
        }
        libc::AF_INET6 => {
            let sin6 = unsafe { &*(storage as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            let port = u16::from_be(sin6.sin6_port);
            SocketAddr::new(ip.into(), port)
        }
        _ => SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
    }
}
