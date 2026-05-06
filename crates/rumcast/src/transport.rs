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
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::os::unix::io::RawFd;

/// One reusable receive buffer + metadata, used by the batched
/// `recv_batch` API. Owners pre-allocate a pool sized to the per-tick
/// receive cap and reuse them across ticks — no per-frame allocation.
pub struct DatagramBuf {
    buf: Box<[u8]>,
    /// Sender address. Filled by `recv_batch`; unspecified before
    /// the slot has been written to.
    pub from: SocketAddr,
    /// Number of valid bytes in `buf` after `recv_batch`. Reads past
    /// `len` are stale.
    pub len: usize,
}

impl DatagramBuf {
    /// Allocate a new buffer of `capacity` bytes. Typical capacity is
    /// `2048` (rumcast's frame cap).
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0u8; capacity].into_boxed_slice(),
            // Placeholder address; never read before being written.
            from: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            len: 0,
        }
    }

    /// Valid bytes received in the most recent batch fill.
    #[inline]
    pub fn payload(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    /// Mutable view of the full backing buffer. Used by transports
    /// to write incoming bytes.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf
    }
}

/// Cap on the number of datagrams pushed in one `sendmmsg` call.
/// Bounded so the per-call stack arrays stay small (~16 KB at this
/// cap) and one transient send error doesn't waste a huge batch.
/// Larger batches are split across multiple syscalls automatically.
const SENDMMSG_BATCH_CAP: usize = 64;

/// Issue `sendmmsg(2)` against `fd`, sending each entry in `payloads`
/// as one UDP datagram to `dst`. Returns the count successfully
/// queued; partial-success swallows the trailing error (matching the
/// `UdpTransport::send_batch_to` contract).
///
/// Shared between [`KernelUdp`] and the io_uring endpoint halves —
/// both hold a `RawFd` for an unconnected `UdpSocket`, and sendmmsg
/// is the kernel-fast-path send regardless of which transport drives
/// recv. Stays in this module since `KernelUdp` is its primary user.
pub(crate) fn sendmmsg_to(fd: RawFd, dst: SocketAddr, payloads: &[&[u8]]) -> io::Result<usize> {
    if payloads.is_empty() {
        return Ok(0);
    }

    // One destination shared across all mmsghdrs — encode once.
    let (sa_storage, sa_len) = sockaddr_from_socket_addr(dst);

    let mut total: usize = 0;
    let mut start = 0;
    while start < payloads.len() {
        let chunk_len = (payloads.len() - start).min(SENDMMSG_BATCH_CAP);
        // Stack-allocated arrays sized to the cap; only the first
        // chunk_len entries are populated.
        // mmsghdr layout: { msg_hdr: msghdr, msg_len: u32 }.
        let mut iovs: [libc::iovec; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
        let mut msgs: [libc::mmsghdr; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };

        for i in 0..chunk_len {
            let bytes = payloads[start + i];
            iovs[i].iov_base = bytes.as_ptr() as *mut libc::c_void;
            iovs[i].iov_len = bytes.len();

            let hdr = &mut msgs[i].msg_hdr;
            hdr.msg_name = &sa_storage as *const _ as *mut libc::c_void;
            hdr.msg_namelen = sa_len;
            hdr.msg_iov = &mut iovs[i] as *mut _;
            hdr.msg_iovlen = 1;
            // msg_control / msg_controllen / msg_flags zeroed above.
        }

        // Safety: msgs[..chunk_len] is fully initialized; the sockaddr
        // pointer outlives the syscall (sa_storage is on this stack
        // frame); iov pointers point at caller-supplied slices that
        // outlive the call.
        let ret = unsafe {
            libc::sendmmsg(
                fd,
                msgs.as_mut_ptr(),
                chunk_len as libc::c_uint,
                0, // no MSG_DONTWAIT — socket is already non-blocking
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            // Partial success on prior chunks: return the count
            // already accepted by the kernel.
            if total > 0 {
                return Ok(total);
            }
            // Map WouldBlock the same way std::net does so callers
            // can retry on the next tick uniformly.
            return Err(err);
        }
        let sent = ret as usize;
        total += sent;
        if sent < chunk_len {
            // Kernel accepted fewer than we asked — usually the send
            // buffer is full. Stop here and let the caller retry.
            return Ok(total);
        }
        start += chunk_len;
    }
    Ok(total)
}

/// Issue `sendmmsg(2)` against `fd` with per-message destinations.
/// Each entry is `(dst, payload)`; messages may go to different
/// addresses in one syscall. Returns the count accepted; on partial
/// success the trailing error is swallowed so the caller retries.
///
/// Used by [`MuxedSender::tick`] to batch all sessions' outbound
/// fragments in a single syscall rather than one per session.
pub(crate) fn sendmmsg_multi_to(fd: RawFd, entries: &[(SocketAddr, &[u8])]) -> io::Result<usize> {
    sendmmsg_staged_impl(fd, entries.len(), |i| {
        let (dst, payload) = entries[i];
        (dst, payload.as_ptr(), payload.len())
    })
}

/// Issue `sendmmsg(2)` against `fd` where each message's payload is a
/// `(offset, len)` slice into a shared `data` buffer. Avoids the
/// per-tick `Vec<(SocketAddr, &[u8])>` collect in [`MuxedSender::tick`]
/// by referencing payload bytes by offset rather than by pointer slice.
pub(crate) fn sendmmsg_staged(
    fd: RawFd,
    data: &[u8],
    entries: &[(SocketAddr, usize, usize)],
) -> io::Result<usize> {
    sendmmsg_staged_impl(fd, entries.len(), |i| {
        let (dst, offset, len) = entries[i];
        (dst, data[offset..].as_ptr(), len)
    })
}

/// Common `sendmmsg` loop used by both [`sendmmsg_multi_to`] and
/// [`sendmmsg_staged`]. `get_entry(i)` returns `(dst, payload_ptr, len)`
/// for message index `i`. Extracted so the two callers share chunking
/// and error-handling without an intermediate allocation.
fn sendmmsg_staged_impl(
    fd: RawFd,
    count: usize,
    get_entry: impl Fn(usize) -> (SocketAddr, *const u8, usize),
) -> io::Result<usize> {
    if count == 0 {
        return Ok(0);
    }
    let mut total = 0usize;
    let mut start = 0;
    while start < count {
        let chunk_end = count.min(start + SENDMMSG_BATCH_CAP);
        let chunk_len = chunk_end - start;

        // Per-message sockaddr storage — each message has its own dst.
        let mut addrs: [libc::sockaddr_storage; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
        let mut iovs: [libc::iovec; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
        let mut msgs: [libc::mmsghdr; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };

        for i in 0..chunk_len {
            let (dst, ptr, len) = get_entry(start + i);
            let (sa, sa_len) = sockaddr_from_socket_addr(dst);
            addrs[i] = sa;
            iovs[i].iov_base = ptr as *mut libc::c_void;
            iovs[i].iov_len = len;
            let hdr = &mut msgs[i].msg_hdr;
            hdr.msg_name = &addrs[i] as *const _ as *mut libc::c_void;
            hdr.msg_namelen = sa_len;
            hdr.msg_iov = &mut iovs[i];
            hdr.msg_iovlen = 1;
        }

        // Safety: msgs[..chunk_len] is fully initialised; addrs and iovs
        // outlive the syscall; socket is non-blocking.
        let ret = unsafe { libc::sendmmsg(fd, msgs.as_mut_ptr(), chunk_len as libc::c_uint, 0) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if total > 0 {
                return Ok(total);
            }
            return Err(err);
        }
        let sent = ret as usize;
        total += sent;
        if sent < chunk_len {
            return Ok(total);
        }
        start = chunk_end;
    }
    Ok(total)
}

/// `UDP_SEGMENT` cmsg type — present in `linux/udp.h` since 4.18 but
/// not exposed by the glibc variant of the `libc` crate (only the
/// uclibc fork). Hardcoded here; the value is part of the kernel ABI.
#[cfg(target_os = "linux")]
const UDP_SEGMENT_CMSG: libc::c_int = 103;

/// Issue `sendmmsg(2)` with per-message `UDP_SEGMENT` cmsg (UDP-GSO).
/// Each entry `(dst, offset, total_len, segment_size)` describes one
/// contiguous run in `data` that the kernel will split into
/// `ceil(total_len / segment_size)` UDP datagrams of `segment_size`
/// bytes each (the last may be shorter). Reduces per-packet kernel
/// UDP-send cost: N logical packets traverse the IP/UDP stack as one
/// skb. On NICs with hardware UDP segmentation offload the splitting
/// happens on the wire, not in the kernel.
///
/// Returns the number of mmsghdrs the kernel accepted (= number of
/// session-runs sent, NOT total UDP datagrams). Caller knows
/// segments-per-msghdr from its own bookkeeping.
///
/// On systems where the kernel rejects `UDP_SEGMENT` (pre-4.18, or
/// some virt environments), the syscall returns `EINVAL`. The caller
/// is expected to detect this once at startup and fall back to plain
/// `sendmmsg_staged`.
#[cfg(target_os = "linux")]
pub(crate) fn sendmmsg_staged_segmented(
    fd: RawFd,
    data: &[u8],
    entries: &[(SocketAddr, usize, usize, u16)],
) -> io::Result<usize> {
    if entries.is_empty() {
        return Ok(0);
    }

    // CMSG_SPACE(sizeof(u16)) — alignment-padded cmsg buffer per
    // mmsghdr. Using the libc macro keeps us correct across libc/arch
    // combos (it returns 24 on x86_64 glibc).
    // Safety: CMSG_SPACE is a pure arithmetic macro.
    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) } as usize;

    let mut total = 0usize;
    let mut start = 0;
    while start < entries.len() {
        let chunk_end = entries.len().min(start + SENDMMSG_BATCH_CAP);
        let chunk_len = chunk_end - start;

        // Stack-resident parallel arrays sized to the cap. cmsg_bufs
        // is backed by `u64` so the storage is 8-byte aligned — the
        // alignment cmsghdr requires on every Linux arch (it contains
        // a size_t). 4 × u64 = 32 bytes, ample for CMSG_SPACE(u16) on
        // every libc we care about.
        const CMSG_BUF_U64S: usize = 4;
        const CMSG_BUF_LEN: usize = CMSG_BUF_U64S * std::mem::size_of::<u64>();
        let mut addrs: [libc::sockaddr_storage; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
        let mut iovs: [libc::iovec; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
        let mut msgs: [libc::mmsghdr; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
        let mut cmsg_bufs: [[u64; CMSG_BUF_U64S]; SENDMMSG_BATCH_CAP] =
            [[0u64; CMSG_BUF_U64S]; SENDMMSG_BATCH_CAP];
        debug_assert!(cmsg_space <= CMSG_BUF_LEN);

        for i in 0..chunk_len {
            let (dst, offset, total_len, seg_size) = entries[start + i];
            debug_assert!(seg_size > 0, "segment_size must be > 0");
            debug_assert!(total_len > 0, "total_len must be > 0");
            let (sa, sa_len) = sockaddr_from_socket_addr(dst);
            addrs[i] = sa;

            iovs[i].iov_base = data[offset..].as_ptr() as *mut libc::c_void;
            iovs[i].iov_len = total_len;

            // Build the cmsg in cmsg_bufs[i]. Manual layout: write
            // cmsghdr fields then the u16 segment size at CMSG_DATA.
            // Safety: cmsg_bufs[i] is 8-byte aligned (u64-backed) and
            // at least cmsg_space bytes long; the pointer arithmetic
            // via CMSG_DATA is the kernel-blessed way to locate the
            // cmsg payload regardless of arch alignment rules.
            unsafe {
                let hdr_ptr = cmsg_bufs[i].as_mut_ptr() as *mut libc::cmsghdr;
                (*hdr_ptr).cmsg_len =
                    libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as libc::size_t;
                (*hdr_ptr).cmsg_level = libc::SOL_UDP;
                (*hdr_ptr).cmsg_type = UDP_SEGMENT_CMSG;
                let data_ptr = libc::CMSG_DATA(hdr_ptr) as *mut u16;
                std::ptr::write_unaligned(data_ptr, seg_size);
            }

            let hdr = &mut msgs[i].msg_hdr;
            hdr.msg_name = &addrs[i] as *const _ as *mut libc::c_void;
            hdr.msg_namelen = sa_len;
            hdr.msg_iov = &mut iovs[i] as *mut _;
            hdr.msg_iovlen = 1;
            hdr.msg_control = cmsg_bufs[i].as_mut_ptr() as *mut libc::c_void;
            hdr.msg_controllen = cmsg_space as _;
        }

        // Safety: msgs[..chunk_len] is fully initialized; addrs, iovs,
        // and cmsg_bufs all live for this loop body which contains the
        // syscall; data outlives the call.
        let ret = unsafe { libc::sendmmsg(fd, msgs.as_mut_ptr(), chunk_len as libc::c_uint, 0) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if total > 0 {
                return Ok(total);
            }
            return Err(err);
        }
        let sent = ret as usize;
        total += sent;
        if sent < chunk_len {
            return Ok(total);
        }
        start = chunk_end;
    }
    Ok(total)
}

/// Issue `recvmmsg(2)` against `fd`, filling up to `slots.len()`
/// `DatagramBuf`s. Returns the number written. Non-blocking: returns
/// `Ok(0)` when nothing is ready (matching the `recv_batch` contract).
///
/// Shared between [`KernelUdp::recv_batch`] and any future caller
/// that wants to drain a non-connected `UdpSocket` fd in batches.
pub(crate) fn recvmmsg_into(fd: RawFd, slots: &mut [DatagramBuf]) -> io::Result<usize> {
    if slots.is_empty() {
        return Ok(0);
    }
    let chunk_len = slots.len().min(SENDMMSG_BATCH_CAP);

    // Stack-resident parallel arrays sized to the cap. Only the first
    // `chunk_len` entries are populated.
    let mut iovs: [libc::iovec; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
    let mut names: [libc::sockaddr_storage; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
    let mut msgs: [libc::mmsghdr; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };

    for i in 0..chunk_len {
        let buf = slots[i].as_mut_slice();
        iovs[i].iov_base = buf.as_mut_ptr() as *mut libc::c_void;
        iovs[i].iov_len = buf.len();

        let hdr = &mut msgs[i].msg_hdr;
        hdr.msg_name = &mut names[i] as *mut _ as *mut libc::c_void;
        hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        hdr.msg_iov = &mut iovs[i] as *mut _;
        hdr.msg_iovlen = 1;
    }

    // Safety: msgs[..chunk_len] fully initialized; iovs/names/slots
    // all outlive the syscall. MSG_DONTWAIT mirrors `recv_from`'s
    // non-blocking semantics — return `Ok(0)` instead of blocking
    // when nothing is ready.
    let ret = unsafe {
        libc::recvmmsg(
            fd,
            msgs.as_mut_ptr(),
            chunk_len as libc::c_uint,
            libc::MSG_DONTWAIT,
            std::ptr::null_mut(), // timeout
        )
    };
    if ret < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Ok(0);
        }
        return Err(err);
    }
    let n = ret as usize;
    for i in 0..n {
        slots[i].len = msgs[i].msg_len as usize;
        slots[i].from = sockaddr_storage_to_socket_addr(&names[i]);
    }
    Ok(n)
}

/// Linux `UDP_MAX_SEGMENTS` — the kernel-imposed cap on segments the
/// receive-side GRO path will coalesce into a single skb. We use it
/// to bound the recvmmsg msghdr count so a single coalesced msghdr
/// can never overflow the caller's slot array (which would silently
/// drop the unfanned tail). Mirrors the kernel constant in
/// `include/net/udp.h`; conservative — newer kernels haven't raised it.
#[cfg(target_os = "linux")]
const GRO_MAX_SEGS_PER_MSGHDR: usize = 64;

/// `recvmmsg(2)` variant for `UDP_GRO`-enabled sockets.
///
/// The kernel may have coalesced multiple consecutive UDP datagrams
/// (each originally sent via `UDP_SEGMENT`) into one large skb. Per
/// msghdr we receive:
/// * a `payload_len` that may exceed one wire MTU,
/// * a `UDP_GRO` cmsg whose `u16` payload is the original `seg_size`.
///
/// We recvmmsg into the caller's input pool (large buffers + per-
/// msghdr cmsg storage), then walk each filled msghdr and fan out
/// segments — one segment per output `slot` — copying `seg_size`
/// bytes per segment. The final segment of each msghdr may be
/// shorter (kernel preserves the unsegmented tail). `from` and `len`
/// are written on each output slot.
///
/// Returns the number of output slots filled. The msghdr count is
/// capped to `slots.len() / GRO_MAX_SEGS_PER_MSGHDR` so a single
/// coalesced msghdr can never overflow the caller's slot array.
/// Callers with `slots.len() < GRO_MAX_SEGS_PER_MSGHDR` fall through
/// to the non-GRO recv path automatically.
///
/// `MSG_DONTWAIT` semantics match `recvmmsg_into` — `Ok(0)` on no
/// data, `Err(WouldBlock)` mapped to `Ok(0)`.
#[cfg(target_os = "linux")]
fn recvmmsg_into_gro(
    fd: RawFd,
    pool: &mut GroInputPool,
    slots: &mut [DatagramBuf],
) -> io::Result<usize> {
    if slots.is_empty() || pool.buffers.is_empty() {
        return Ok(0);
    }
    // Cap msghdr count so the worst-case fan-out (every msghdr maxed
    // at GRO_MAX_SEGS_PER_MSGHDR) still fits in `slots`. With slots
    // smaller than one msghdr's worst case, fall back to the non-GRO
    // recv path (one wire packet per slot) — that's correct, just
    // doesn't get the GRO syscall amortization.
    let max_chunk_for_slots = slots.len() / GRO_MAX_SEGS_PER_MSGHDR;
    if max_chunk_for_slots == 0 {
        return recvmmsg_into(fd, slots);
    }
    let chunk_len = pool
        .buffers
        .len()
        .min(SENDMMSG_BATCH_CAP)
        .min(max_chunk_for_slots);

    let mut iovs: [libc::iovec; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
    let mut names: [libc::sockaddr_storage; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };
    let mut msgs: [libc::mmsghdr; SENDMMSG_BATCH_CAP] = unsafe { std::mem::zeroed() };

    for i in 0..chunk_len {
        let buf = &mut pool.buffers[i];
        iovs[i].iov_base = buf.as_mut_ptr() as *mut libc::c_void;
        iovs[i].iov_len = buf.len();

        let hdr = &mut msgs[i].msg_hdr;
        hdr.msg_name = &mut names[i] as *mut _ as *mut libc::c_void;
        hdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        hdr.msg_iov = &mut iovs[i] as *mut _;
        hdr.msg_iovlen = 1;
        hdr.msg_control = pool.cmsg_bufs[i].as_mut_ptr() as *mut libc::c_void;
        hdr.msg_controllen = std::mem::size_of::<[u64; 4]>();
    }

    let ret = unsafe {
        libc::recvmmsg(
            fd,
            msgs.as_mut_ptr(),
            chunk_len as libc::c_uint,
            libc::MSG_DONTWAIT,
            std::ptr::null_mut(),
        )
    };
    if ret < 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Ok(0);
        }
        return Err(err);
    }
    let n_msgs = ret as usize;

    // Fan out via the pure-logic helper so the segment-splitting can
    // be unit-tested without a real recvmmsg.
    let mut out = 0usize;
    for i in 0..n_msgs {
        if out >= slots.len() {
            break;
        }
        let payload_len = msgs[i].msg_len as usize;
        let from = sockaddr_storage_to_socket_addr(&names[i]);
        let buf = &pool.buffers[i][..payload_len];
        let seg_size = parse_udp_gro_cmsg(&msgs[i].msg_hdr);
        out += fan_out_msghdr(buf, seg_size, from, &mut slots[out..]);
    }
    Ok(out)
}

/// Pure fan-out helper — split one recvmmsg msghdr's payload into
/// segments and write each into the next output slot. Extracted out
/// of [`recvmmsg_into_gro`] so it can be unit-tested without
/// fabricating a kernel `msghdr`. Returns the number of slots written.
///
/// `seg_size` is `None` when the kernel returned no `UDP_GRO` cmsg
/// (no coalescing happened) — in that case we treat the whole
/// payload as one segment. When `seg_size` is `Some(s)` with `0 < s <
/// payload_len`, we stride through the payload in chunks of `s`; the
/// final segment may be shorter.
fn fan_out_msghdr(
    payload: &[u8],
    seg_size: Option<u16>,
    from: SocketAddr,
    slots: &mut [DatagramBuf],
) -> usize {
    if payload.is_empty() || slots.is_empty() {
        return 0;
    }
    let stride = match seg_size {
        // Coalesced: seg_size from the cmsg dictates segment length.
        Some(s) if (s as usize) > 0 && (s as usize) < payload.len() => s as usize,
        // Either no cmsg or seg_size >= payload_len — one segment.
        _ => payload.len(),
    };
    let mut out = 0;
    let mut offset = 0;
    while offset < payload.len() && out < slots.len() {
        let end = (offset + stride).min(payload.len());
        let seg = &payload[offset..end];
        let dst = slots[out].as_mut_slice();
        let n = seg.len().min(dst.len());
        dst[..n].copy_from_slice(&seg[..n]);
        slots[out].len = n;
        slots[out].from = from;
        out += 1;
        offset = end;
    }
    out
}

/// Walk the `msg_control` block on a kernel-filled `msghdr` and
/// return the `UDP_GRO` segment size if present, else `None`.
#[cfg(target_os = "linux")]
fn parse_udp_gro_cmsg(hdr: &libc::msghdr) -> Option<u16> {
    if hdr.msg_control.is_null() || hdr.msg_controllen == 0 {
        return None;
    }
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(hdr);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_UDP && (*cmsg).cmsg_type == UDP_GRO_CMSG {
                let data = libc::CMSG_DATA(cmsg) as *const u16;
                return Some(*data);
            }
            cmsg = libc::CMSG_NXTHDR(hdr, cmsg);
        }
    }
    None
}

/// Decode a kernel-filled `sockaddr_storage` back into a
/// `SocketAddr`. Anything that isn't AF_INET / AF_INET6 falls back to
/// `0.0.0.0:0` — the protocol parser drops it later.
fn sockaddr_storage_to_socket_addr(storage: &libc::sockaddr_storage) -> SocketAddr {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
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
        _ => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
    }
}

/// Render a `SocketAddr` into a `sockaddr_storage` + length pair
/// suitable for `msg_name` / `msg_namelen` in a `msghdr`. IPv4 →
/// `sockaddr_in`, IPv6 → `sockaddr_in6`.
fn sockaddr_from_socket_addr(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    match addr {
        SocketAddr::V4(v4) => {
            // Safety: SocketAddr::V4 → sockaddr_in fits in storage.
            let sin = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in) };
            sin.sin_family = libc::AF_INET as libc::sa_family_t;
            sin.sin_port = v4.port().to_be();
            sin.sin_addr.s_addr = u32::from(*v4.ip()).to_be();
            (
                storage,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(v6) => {
            let sin6 = unsafe { &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6) };
            sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_flowinfo = v6.flowinfo();
            sin6.sin6_addr.s6_addr = v6.ip().octets();
            sin6.sin6_scope_id = v6.scope_id();
            (
                storage,
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
            )
        }
    }
}

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

    /// Send up to `payloads.len()` datagrams to `dst` in one batched
    /// call when the transport supports it. Returns the number of
    /// datagrams accepted by the kernel; remaining payloads can be
    /// retried on the next tick.
    ///
    /// Default impl loops over [`send_to`]. Backends like
    /// [`KernelUdp`] override with `sendmmsg(2)` to amortize the
    /// syscall cost across the batch — at high fragment rates this
    /// is the difference between ~150 ns/fragment and ~1.5 ns/fragment
    /// of syscall overhead.
    ///
    /// On partial success (some datagrams sent, then an error), the
    /// fn returns the count of successful sends and swallows the
    /// error so the caller can re-attempt the unsent tail. An error
    /// at index 0 propagates, since no progress was made.
    ///
    /// [`send_to`]: Self::send_to
    fn send_batch_to(&self, dst: SocketAddr, payloads: &[&[u8]]) -> io::Result<usize> {
        for (i, p) in payloads.iter().enumerate() {
            match self.send_to(dst, p) {
                Ok(_) => continue,
                Err(e) if i == 0 => return Err(e),
                Err(_) => return Ok(i),
            }
        }
        Ok(payloads.len())
    }

    /// Send multiple datagrams each to a distinct destination in one
    /// batched syscall. Each entry is `(dst, payload)`. Used by
    /// [`MuxedSender::tick`] to collapse all sessions' outbound
    /// fragments into one `sendmmsg(2)` call instead of one per session.
    ///
    /// Default impl loops over [`send_to`]. [`KernelUdp`] and the
    /// io_uring endpoints override with [`sendmmsg_multi_to`].
    ///
    /// [`send_to`]: Self::send_to
    fn send_multi_to(&self, entries: &[(SocketAddr, &[u8])]) -> io::Result<usize> {
        for (i, (dst, payload)) in entries.iter().enumerate() {
            match self.send_to(*dst, payload) {
                Ok(_) => {}
                Err(e) if i == 0 => return Err(e),
                Err(_) => return Ok(i),
            }
        }
        Ok(entries.len())
    }

    /// Send multiple datagrams each to a distinct destination, where
    /// each entry is `(dst, offset, len)` into a shared `data` buffer.
    /// Avoids the `Vec<(SocketAddr, &[u8])>` collect that
    /// [`send_multi_to`] would require — the caller pre-allocates
    /// `entries` once and reuses it across ticks.
    ///
    /// Default impl loops over [`send_to`]. [`KernelUdp`] and the
    /// io_uring endpoints override with [`sendmmsg_staged`].
    ///
    /// [`send_to`]: Self::send_to
    /// [`send_multi_to`]: Self::send_multi_to
    fn send_staged(
        &self,
        data: &[u8],
        entries: &[(SocketAddr, usize, usize)],
    ) -> io::Result<usize> {
        for (i, &(dst, offset, len)) in entries.iter().enumerate() {
            match self.send_to(dst, &data[offset..offset + len]) {
                Ok(_) => {}
                Err(e) if i == 0 => return Err(e),
                Err(_) => return Ok(i),
            }
        }
        Ok(entries.len())
    }

    /// Send batched UDP datagrams using kernel UDP-GSO (`UDP_SEGMENT`
    /// cmsg). Each entry is `(dst, offset, total_len, segment_size)`:
    /// the kernel splits the contiguous `total_len` bytes starting at
    /// `data[offset..]` into `ceil(total_len / segment_size)` UDP
    /// datagrams of `segment_size` bytes each (the last may be
    /// shorter). Returns the number of *mmsghdrs* (not segments)
    /// accepted; the caller knows segments-per-msghdr from its own
    /// bookkeeping.
    ///
    /// All segments within a single msghdr share one dst and one
    /// segment_size — the caller must group same-size runs per dst.
    /// Mixed-size sends should fall back to [`send_staged`].
    ///
    /// Default impl loops over [`send_to`] one segment at a time so
    /// transports without GSO support stay correct, just slow. The
    /// `KernelUdp` override calls [`sendmmsg_staged_segmented`].
    ///
    /// On kernels that reject `UDP_SEGMENT` (pre-4.18 or some virt
    /// environments) the syscall returns `EINVAL`; callers should
    /// detect this once at startup and fall back permanently.
    ///
    /// [`send_to`]: Self::send_to
    /// [`send_staged`]: Self::send_staged
    /// [`sendmmsg_staged_segmented`]: crate::transport::sendmmsg_staged_segmented
    fn send_segmented_staged(
        &self,
        data: &[u8],
        entries: &[(SocketAddr, usize, usize, u16)],
    ) -> io::Result<usize> {
        // Default: per-segment send_to, preserving the partial-success
        // contract at *msghdr* granularity (caller's unit of work).
        for (i, &(dst, offset, total_len, seg_size)) in entries.iter().enumerate() {
            debug_assert!(seg_size > 0, "segment_size must be > 0");
            let seg = seg_size as usize;
            let mut sent_in_msg = 0usize;
            while sent_in_msg < total_len {
                let end = (sent_in_msg + seg).min(total_len);
                match self.send_to(dst, &data[offset + sent_in_msg..offset + end]) {
                    Ok(_) => sent_in_msg = end,
                    Err(e) if i == 0 && sent_in_msg == 0 => return Err(e),
                    Err(_) => return Ok(i),
                }
            }
        }
        Ok(entries.len())
    }

    /// Try to receive one datagram into `buf`. Returns `Ok(None)` when
    /// no datagram is ready. On `Ok(Some((addr, len)))`, the first
    /// `len` bytes of `buf` are valid and `addr` is the sender.
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>>;

    /// Receive up to `slots.len()` datagrams in one batched call.
    /// Each filled slot has its `from` and `len` fields written; the
    /// payload sits in `slot.buf[..slot.len]`. Returns the number of
    /// slots filled. `Ok(0)` means no datagram is ready (or all the
    /// kernel has is one that returned WouldBlock first time).
    ///
    /// Default impl loops over [`recv_from`]. Backends like
    /// [`KernelUdp`] override with `recvmmsg(2)` to amortize the
    /// syscall cost; the io_uring endpoint specializes by draining N
    /// frames under one mutex acquire on its SPSC consumer.
    ///
    /// [`recv_from`]: Self::recv_from
    fn recv_batch(&self, slots: &mut [DatagramBuf]) -> io::Result<usize> {
        for (i, slot) in slots.iter_mut().enumerate() {
            match self.recv_from(slot.as_mut_slice()) {
                Ok(Some((from, len))) => {
                    slot.from = from;
                    slot.len = len;
                }
                Ok(None) => return Ok(i),
                Err(e) if i == 0 => return Err(e),
                Err(_) => return Ok(i),
            }
        }
        Ok(slots.len())
    }

    /// Local socket address (after binding).
    fn local_addr(&self) -> io::Result<SocketAddr>;

    /// Join an IPv4 multicast group on the given local interface.
    /// `iface` of `0.0.0.0` lets the kernel pick the default interface.
    fn join_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()>;

    /// Leave a previously-joined multicast group.
    fn leave_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()>;

    /// Block until a datagram is available or `timeout` elapses. Used
    /// by the bench idle path to replace `sleep(10µs)` with an
    /// event-driven wakeup, eliminating the scheduling gap between
    /// response arrival and poll. Default: sleep for the full timeout.
    fn park(&self, timeout: std::time::Duration) {
        std::thread::sleep(timeout);
    }
}

/// Kernel UDP socket backend (`std::net::UdpSocket`, non-blocking).
pub struct KernelUdp {
    socket: UdpSocket,
    /// Whether the receive path should request `UDP_GRO` cmsg fan-out.
    /// Set via [`KernelUdp::set_udp_gro`]; checked once per recv_batch.
    /// `AtomicBool` so callers don't need `&mut self` to enable.
    gro_enabled: std::sync::atomic::AtomicBool,
    /// Lazily-allocated pool of large input buffers used only on the
    /// GRO recv path. Sized for the worst-case coalesced UDP packet
    /// (~64 KiB). `Mutex` covers pool creation + per-call borrow; the
    /// uncontested cost is ~30 cycles, negligible vs the syscall.
    /// `None` until first GRO recv.
    gro_input_pool: std::sync::Mutex<Option<GroInputPool>>,
}

/// Input buffers used by the `UDP_GRO` recv path. Pre-allocated once
/// the first time `recv_batch` runs with GRO enabled. Each buffer
/// holds one coalesced GRO skb; userspace then fans out the segments
/// into the caller's small `DatagramBuf` slots.
struct GroInputPool {
    /// One large buffer per mmsghdr.
    buffers: Vec<Box<[u8]>>,
    /// Per-mmsghdr msg_control storage. Sized for one `UDP_GRO` cmsg
    /// (`CMSG_SPACE(sizeof(u16))` = 24 bytes on x86_64; round up to
    /// `[u64; 4]` = 32 bytes for 8-byte alignment with headroom).
    cmsg_bufs: Vec<[u64; 4]>,
}

/// Per-mmsghdr slot count. With GRO active, one msghdr can yield
/// many segments; we fan out into the caller's slots, so the input
/// pool only needs enough buffers to absorb a syscall's worth at the
/// cap below. Larger than `SENDMMSG_BATCH_CAP / 2` is wasteful since
/// recvmmsg returns N msghdrs ≤ pool size in one call.
const GRO_INPUT_BUF_BYTES: usize = 65536;
const GRO_INPUT_POOL_CAP: usize = 32;

/// `UDP_GRO` cmsg type — Linux `<linux/udp.h>` defines this as `104`.
/// `libc` doesn't currently re-export it.
#[cfg(target_os = "linux")]
const UDP_GRO_CMSG: libc::c_int = 104;

impl KernelUdp {
    /// Bind to `local`. The socket is set to non-blocking mode.
    pub fn bind(local: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(local)?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            gro_enabled: std::sync::atomic::AtomicBool::new(false),
            gro_input_pool: std::sync::Mutex::new(None),
        })
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

    /// Request a larger SO_RCVBUF on this socket. The kernel may cap
    /// the effective size at `net.core.rmem_max`; the caller should
    /// verify via `getsockopt` if the exact size matters. Used on the
    /// server's response socket to absorb bursts of SMs/NAKs from
    /// many concurrent subscribers without kernel-dropping them
    /// (which would stall rumcast's flow control).
    pub fn set_recv_buffer_bytes(&self, bytes: usize) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        // i32 size matches the SO_RCVBUF socket option ABI on Linux;
        // value is doubled by the kernel internally and capped by
        // rmem_max, so we don't try to be precise about the cap here.
        let val: libc::c_int = bytes.min(i32::MAX as usize) as libc::c_int;
        let ret = unsafe {
            libc::setsockopt(
                self.socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            )
        };
        if ret == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    /// Enable NAPI busy polling on the receive path.
    ///
    /// `microseconds` bounds the time the kernel will busy-poll the
    /// driver's NIC ring on each blocking recv. With
    /// `SO_PREFER_BUSY_POLL` set the kernel skips the interrupt+wakeup
    /// path entirely while busy-polling — at the cost of CPU spent
    /// looping inside the syscall instead of sleeping. Pays off on
    /// real NICs at low-jitter, low-latency rumcast workloads;
    /// no-op on the loopback device (no NAPI ring).
    ///
    /// Requires either `CAP_NET_ADMIN` on the calling process or a
    /// non-zero `sysctl net.core.busy_read` floor — the kernel
    /// rejects values above the sysctl floor for unprivileged
    /// callers. Operators bench-running this should typically:
    ///
    /// ```text
    /// sudo sysctl -w net.core.busy_read=50
    /// ```
    ///
    /// then pass `microseconds = 50` here. Larger values trade more
    /// CPU for tighter recv latency.
    ///
    /// Pass `0` to disable.
    pub fn set_busy_poll(&self, microseconds: u32) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        let fd = self.socket.as_raw_fd();
        // SO_BUSY_POLL takes microseconds as i32 — the kernel ABI
        // matches the same layout as SO_RCVBUF.
        let us: libc::c_int = microseconds.min(i32::MAX as u32) as libc::c_int;
        let ret = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_BUSY_POLL,
                &us as *const _ as *const libc::c_void,
                std::mem::size_of_val(&us) as libc::socklen_t,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        // SO_PREFER_BUSY_POLL is the boolean toggle that tells the
        // kernel to prefer busy poll over the regular interrupt path
        // when both are available. Only meaningful when SO_BUSY_POLL
        // is non-zero, so we skip the toggle when disabling.
        if microseconds > 0 {
            let prefer: libc::c_int = 1;
            let ret = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_PREFER_BUSY_POLL,
                    &prefer as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&prefer) as libc::socklen_t,
                )
            };
            if ret != 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    /// Enable kernel `UDP_GRO` on this socket. With GRO active the
    /// kernel coalesces consecutive datagrams from the same flow into
    /// one large skb on receive, and `recv_batch` fans the segments
    /// out into the caller's slots based on the `seg_size` carried in
    /// the per-msghdr `UDP_GRO` cmsg.
    ///
    /// The peer must be sending with `UDP_SEGMENT` (UDP-GSO) for this
    /// to fire — that's how the kernel knows the original segment
    /// boundaries. Without UDP-GSO on the wire, GRO is a no-op and
    /// `recv_batch` returns one segment per recvmsg as before.
    ///
    /// Pass `false` to disable. `setsockopt` returns `ENOPROTOOPT` on
    /// pre-5.0 kernels — the caller can treat that as "GRO not
    /// available" and continue without it.
    #[cfg(target_os = "linux")]
    pub fn set_udp_gro(&self, on: bool) -> io::Result<()> {
        use std::os::unix::io::AsRawFd;
        let val: libc::c_int = if on { 1 } else { 0 };
        let ret = unsafe {
            libc::setsockopt(
                self.socket.as_raw_fd(),
                libc::SOL_UDP,
                UDP_GRO_CMSG,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        self.gro_enabled
            .store(on, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    pub fn set_udp_gro(&self, _on: bool) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "UDP_GRO is Linux-only",
        ))
    }
}

impl UdpTransport for KernelUdp {
    #[inline]
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send_to(bytes, dst)
    }

    fn send_batch_to(&self, dst: SocketAddr, payloads: &[&[u8]]) -> io::Result<usize> {
        use std::os::unix::io::AsRawFd;
        sendmmsg_to(self.socket.as_raw_fd(), dst, payloads)
    }

    fn send_multi_to(&self, entries: &[(SocketAddr, &[u8])]) -> io::Result<usize> {
        use std::os::unix::io::AsRawFd;
        sendmmsg_multi_to(self.socket.as_raw_fd(), entries)
    }

    fn send_staged(
        &self,
        data: &[u8],
        entries: &[(SocketAddr, usize, usize)],
    ) -> io::Result<usize> {
        use std::os::unix::io::AsRawFd;
        sendmmsg_staged(self.socket.as_raw_fd(), data, entries)
    }

    fn send_segmented_staged(
        &self,
        data: &[u8],
        entries: &[(SocketAddr, usize, usize, u16)],
    ) -> io::Result<usize> {
        use std::os::unix::io::AsRawFd;
        sendmmsg_staged_segmented(self.socket.as_raw_fd(), data, entries)
    }

    fn recv_batch(&self, slots: &mut [DatagramBuf]) -> io::Result<usize> {
        use std::os::unix::io::AsRawFd;
        let fd = self.socket.as_raw_fd();
        // Fast path: GRO disabled — straight-line recvmmsg, no
        // per-msghdr cmsg parsing and no input pool.
        if !self.gro_enabled.load(std::sync::atomic::Ordering::Acquire) {
            return recvmmsg_into(fd, slots);
        }
        // GRO path: lazily allocate the input pool on first call,
        // then recvmmsg into it and fan out segments.
        #[cfg(target_os = "linux")]
        {
            let mut guard = self
                .gro_input_pool
                .lock()
                .expect("gro_input_pool mutex poisoned");
            if guard.is_none() {
                let mut buffers: Vec<Box<[u8]>> = Vec::with_capacity(GRO_INPUT_POOL_CAP);
                let mut cmsg_bufs: Vec<[u64; 4]> = Vec::with_capacity(GRO_INPUT_POOL_CAP);
                for _ in 0..GRO_INPUT_POOL_CAP {
                    buffers.push(vec![0u8; GRO_INPUT_BUF_BYTES].into_boxed_slice());
                    cmsg_bufs.push([0u64; 4]);
                }
                *guard = Some(GroInputPool { buffers, cmsg_bufs });
            }
            let pool = guard.as_mut().expect("just initialized");
            recvmmsg_into_gro(fd, pool, slots)
        }
        #[cfg(not(target_os = "linux"))]
        {
            recvmmsg_into(fd, slots)
        }
    }

    fn park(&self, timeout: std::time::Duration) {
        use std::os::unix::io::AsRawFd;
        let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as libc::c_int;
        let mut pfd = libc::pollfd {
            fd: self.socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // Errors (EINTR, ENOMEM) are ignored — the caller will retry
        // recv_from on the next iteration and detect the real issue there.
        unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
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
    fn set_udp_gro_off_succeeds() {
        // Disabling UDP_GRO is harmless on every kernel and never
        // requires privileges — exercises the setsockopt ABI.
        let t = KernelUdp::bind(loopback(0)).unwrap();
        t.set_udp_gro(false).expect("disabling UDP_GRO");
    }

    #[test]
    fn set_udp_gro_on_succeeds_or_unsupported() {
        // Enabling UDP_GRO requires Linux >= 5.0. Accept Ok or
        // ENOPROTOOPT so the test runs on older kernels too.
        let t = KernelUdp::bind(loopback(0)).unwrap();
        match t.set_udp_gro(true) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::ENOPROTOOPT) => {}
            Err(e) => panic!("unexpected set_udp_gro error: {e}"),
        }
    }

    #[test]
    fn fan_out_msghdr_no_cmsg_emits_one_slot() {
        // No UDP_GRO cmsg → kernel didn't coalesce → emit the whole
        // payload as one segment.
        let payload = vec![0xABu8; 96];
        let from = loopback(1234);
        let mut slots: Vec<DatagramBuf> = (0..4).map(|_| DatagramBuf::new(2048)).collect();
        let n = fan_out_msghdr(&payload, None, from, &mut slots);
        assert_eq!(n, 1);
        assert_eq!(slots[0].len, 96);
        assert_eq!(slots[0].from, from);
        assert!(slots[0].payload().iter().all(|&b| b == 0xAB));
        assert_eq!(slots[1].len, 0);
    }

    #[test]
    fn fan_out_msghdr_coalesced_splits_by_seg_size() {
        // Kernel coalesced 5×64 = 320 bytes into one msghdr with a
        // UDP_GRO cmsg seg_size=64. Fan out should emit 5 slots of
        // 64 bytes each.
        let mut payload = Vec::with_capacity(320);
        for i in 0..5u8 {
            payload.extend(std::iter::repeat_n(i, 64));
        }
        let from = loopback(5678);
        let mut slots: Vec<DatagramBuf> = (0..8).map(|_| DatagramBuf::new(2048)).collect();
        let n = fan_out_msghdr(&payload, Some(64), from, &mut slots);
        assert_eq!(n, 5);
        for (i, slot) in slots.iter().enumerate().take(5) {
            assert_eq!(slot.len, 64);
            assert_eq!(slot.from, from);
            assert!(slot.payload().iter().all(|&b| b == i as u8));
        }
        assert_eq!(slots[5].len, 0);
    }

    #[test]
    fn fan_out_msghdr_short_trailing_segment() {
        // 47 segments of 64B + one trailing 32B short segment = 3040
        // bytes. seg_size in the cmsg is the FULL segment size; the
        // tail is shorter (kernel preserves it). Verify the last slot
        // gets 32 bytes, not 64.
        let mut payload = Vec::with_capacity(64 * 47 + 32);
        for i in 0..47u8 {
            payload.extend(std::iter::repeat_n(i, 64));
        }
        payload.extend(std::iter::repeat_n(0xFFu8, 32));
        let from = loopback(7777);
        let mut slots: Vec<DatagramBuf> = (0..64).map(|_| DatagramBuf::new(2048)).collect();
        let n = fan_out_msghdr(&payload, Some(64), from, &mut slots);
        assert_eq!(n, 48);
        assert_eq!(slots[47].len, 32);
        assert!(slots[47].payload().iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn fan_out_msghdr_caps_at_slot_count() {
        // 10 segments of 64B but caller has only 4 slots — emit 4,
        // drop the rest. The caller must size `slots` appropriately;
        // recvmmsg_into_gro caps msghdr count to prevent this in
        // practice.
        let payload = vec![0xCDu8; 640];
        let from = loopback(9999);
        let mut slots: Vec<DatagramBuf> = (0..4).map(|_| DatagramBuf::new(2048)).collect();
        let n = fan_out_msghdr(&payload, Some(64), from, &mut slots);
        assert_eq!(n, 4);
        for slot in &slots {
            assert_eq!(slot.len, 64);
        }
    }

    #[test]
    fn fan_out_msghdr_seg_size_ge_payload_treated_as_single() {
        // seg_size=128 but payload is only 64 bytes → no actual
        // coalescing happened (or only one segment fit). Emit one
        // slot with the full 64 bytes.
        let payload = vec![0x77u8; 64];
        let from = loopback(2222);
        let mut slots: Vec<DatagramBuf> = (0..2).map(|_| DatagramBuf::new(2048)).collect();
        let n = fan_out_msghdr(&payload, Some(128), from, &mut slots);
        assert_eq!(n, 1);
        assert_eq!(slots[0].len, 64);
    }

    #[test]
    fn fan_out_msghdr_empty_payload_noop() {
        let mut slots: Vec<DatagramBuf> = (0..2).map(|_| DatagramBuf::new(2048)).collect();
        let n = fan_out_msghdr(&[], Some(64), loopback(1), &mut slots);
        assert_eq!(n, 0);
        assert_eq!(slots[0].len, 0);
    }

    #[test]
    fn fan_out_msghdr_no_slots_noop() {
        let payload = vec![0u8; 64];
        let mut slots: Vec<DatagramBuf> = Vec::new();
        let n = fan_out_msghdr(&payload, Some(64), loopback(1), &mut slots);
        assert_eq!(n, 0);
    }

    #[test]
    fn recv_batch_with_gro_falls_back_to_single_segment_on_loopback() {
        // Loopback never triggers GRO coalescing — every datagram
        // arrives separately. Verify that with GRO enabled, the recv
        // path still delivers each datagram as one slot (the
        // single-segment fallback inside `recvmmsg_into_gro`).
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let send = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        // Skip the test on hosts without UDP_GRO support.
        if let Err(e) = recv.set_udp_gro(true) {
            if e.raw_os_error() == Some(libc::ENOPROTOOPT) {
                return;
            }
            panic!("set_udp_gro failed: {e}");
        }

        for i in 0..4u8 {
            send.send_to(recv_addr, &[i; 32]).unwrap();
        }

        let mut slots: Vec<DatagramBuf> = (0..8).map(|_| DatagramBuf::new(2048)).collect();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut total = 0;
        while total < 4 && Instant::now() < deadline {
            match recv.recv_batch(&mut slots[total..]).unwrap() {
                0 => std::thread::sleep(Duration::from_micros(100)),
                n => total += n,
            }
        }
        assert_eq!(total, 4, "expected 4 datagrams, got {total}");
        for (i, slot) in slots.iter().enumerate().take(4) {
            assert_eq!(slot.len, 32);
            assert!(slot.payload().iter().all(|&b| b == i as u8));
        }
    }

    #[test]
    fn set_busy_poll_zero_succeeds_unprivileged() {
        // Disabling busy poll is always allowed regardless of
        // CAP_NET_ADMIN or sysctl floors — exercises the setsockopt
        // ABI plumbing (correct level/option/payload) without
        // depending on test-host privileges.
        let t = KernelUdp::bind(loopback(0)).unwrap();
        t.set_busy_poll(0)
            .expect("disabling busy poll should always succeed");
    }

    #[test]
    fn set_busy_poll_nonzero_succeeds_or_eperm() {
        // Enabling busy poll requires CAP_NET_ADMIN or a non-zero
        // `net.core.busy_read` sysctl floor. Accept either outcome
        // so the test is portable across operator environments;
        // the goal is to verify we don't pass garbage to the kernel
        // (EINVAL would be a real bug).
        let t = KernelUdp::bind(loopback(0)).unwrap();
        match t.set_busy_poll(50) {
            Ok(()) => {}
            Err(e) if e.raw_os_error() == Some(libc::EPERM) => {}
            Err(e) => panic!("unexpected error from set_busy_poll(50): {e}"),
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

    #[test]
    fn send_batch_to_delivers_each_payload_as_separate_datagram() {
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        let send = KernelUdp::bind(loopback(0)).unwrap();

        let payloads: Vec<Vec<u8>> = (0..5).map(|i| vec![i as u8; 8]).collect();
        let refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();
        let sent = send.send_batch_to(recv_addr, &refs).unwrap();
        assert_eq!(sent, 5);

        let mut buf = [0u8; 32];
        for i in 0..5u8 {
            let (_, len) = recv_one(&recv, &mut buf);
            assert_eq!(len, 8);
            assert!(buf[..len].iter().all(|&b| b == i));
        }
    }

    #[test]
    fn send_batch_to_handles_chunks_larger_than_cap() {
        // Drive past SENDMMSG_BATCH_CAP to exercise the chunked loop.
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        // Bump SO_RCVBUF so the kernel doesn't drop our localhost
        // burst before the test reads them.
        let _ = recv.set_recv_buffer_bytes(4 * 1024 * 1024);
        let send = KernelUdp::bind(loopback(0)).unwrap();

        const N: usize = SENDMMSG_BATCH_CAP + 16;
        let payloads: Vec<Vec<u8>> = (0..N).map(|i| vec![(i & 0xff) as u8; 16]).collect();
        let refs: Vec<&[u8]> = payloads.iter().map(|p| p.as_slice()).collect();

        let sent = send.send_batch_to(recv_addr, &refs).unwrap();
        // Kernel may report fewer if SO_SNDBUF / receiver-side queue
        // pressure kicks in mid-batch; we only require >= 1 chunk
        // round-trip plus some, proving the chunk loop runs.
        assert!(sent > SENDMMSG_BATCH_CAP, "sent={}", sent);
    }

    /// Helper: drain up to `n` datagrams off `recv` into a Vec of
    /// `(len, payload)` tuples. Spins briefly to absorb loopback delay.
    fn drain_n<T: UdpTransport>(recv: &T, n: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(n);
        let mut buf = [0u8; 4096];
        let deadline = Instant::now() + Duration::from_secs(2);
        while out.len() < n && Instant::now() < deadline {
            match recv.recv_from(&mut buf).expect("recv_from") {
                Some((_, len)) => out.push(buf[..len].to_vec()),
                None => std::thread::sleep(Duration::from_micros(100)),
            }
        }
        out
    }

    /// Returns true if the kernel rejects UDP_SEGMENT (e.g. some virt
    /// environments). Tests skip rather than fail in that case.
    fn segmented_send_supported(send: &KernelUdp, dst: SocketAddr) -> bool {
        use std::os::unix::io::AsRawFd;
        let data = [0u8; 16];
        let entries = [(dst, 0usize, data.len(), 8u16)];
        match sendmmsg_staged_segmented(send.socket.as_raw_fd(), &data, &entries) {
            Ok(_) => true,
            Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                eprintln!("kernel rejects UDP_SEGMENT — skipping segmented test");
                false
            }
            Err(e) => panic!("unexpected error probing UDP_SEGMENT: {e}"),
        }
    }

    #[test]
    fn sendmmsg_staged_segmented_splits_one_buffer_into_n_datagrams() {
        // GSO smoke test: hand the kernel one contiguous buffer with a
        // segment-size cmsg, expect to receive N separate datagrams of
        // segment_size bytes each.
        use std::os::unix::io::AsRawFd;
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        let _ = recv.set_recv_buffer_bytes(4 * 1024 * 1024);
        let send = KernelUdp::bind(loopback(0)).unwrap();
        if !segmented_send_supported(&send, recv_addr) {
            return;
        }
        // Drain the probe datagrams the support check just sent.
        let _ = drain_n(&recv, 2);

        // 4 segments of 32 bytes each, distinct payloads so we can
        // verify split alignment.
        const SEG: usize = 32;
        const N: usize = 4;
        let mut data = Vec::with_capacity(SEG * N);
        for i in 0..N {
            data.extend(std::iter::repeat_n(i as u8, SEG));
        }
        let entries = [(recv_addr, 0usize, data.len(), SEG as u16)];

        let sent = sendmmsg_staged_segmented(send.socket.as_raw_fd(), &data, &entries)
            .expect("segmented send");
        assert_eq!(sent, 1, "kernel accepted exactly one mmsghdr");

        let dgrams = drain_n(&recv, N);
        assert_eq!(dgrams.len(), N, "all {N} segments arrived");
        for (i, payload) in dgrams.iter().enumerate() {
            assert_eq!(payload.len(), SEG, "segment {i} full size");
            assert!(
                payload.iter().all(|&b| b == i as u8),
                "segment {i} payload mismatch"
            );
        }
    }

    #[test]
    fn sendmmsg_staged_segmented_short_trailing_segment_arrives_smaller() {
        // GSO contract: every segment except the last must equal
        // segment_size; the last may be shorter. Prove it works.
        use std::os::unix::io::AsRawFd;
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        let _ = recv.set_recv_buffer_bytes(4 * 1024 * 1024);
        let send = KernelUdp::bind(loopback(0)).unwrap();
        if !segmented_send_supported(&send, recv_addr) {
            return;
        }
        let _ = drain_n(&recv, 2);

        const SEG: usize = 16;
        // 2 full segments + a 5-byte tail = 37 bytes total.
        let data: Vec<u8> = (0..37u8).collect();
        let entries = [(recv_addr, 0usize, data.len(), SEG as u16)];

        let sent = sendmmsg_staged_segmented(send.socket.as_raw_fd(), &data, &entries)
            .expect("segmented send");
        assert_eq!(sent, 1);

        let dgrams = drain_n(&recv, 3);
        assert_eq!(dgrams.len(), 3, "two full + one short segment");
        assert_eq!(dgrams[0], (0..16u8).collect::<Vec<_>>());
        assert_eq!(dgrams[1], (16..32u8).collect::<Vec<_>>());
        assert_eq!(dgrams[2], (32..37u8).collect::<Vec<_>>());
    }

    #[test]
    fn sendmmsg_staged_segmented_multi_entry_routes_to_distinct_dsts() {
        // Two destinations, two msghdrs, in one sendmmsg call.
        use std::os::unix::io::AsRawFd;
        let recv_a = KernelUdp::bind(loopback(0)).unwrap();
        let recv_b = KernelUdp::bind(loopback(0)).unwrap();
        let _ = recv_a.set_recv_buffer_bytes(4 * 1024 * 1024);
        let _ = recv_b.set_recv_buffer_bytes(4 * 1024 * 1024);
        let addr_a = recv_a.local_addr().unwrap();
        let addr_b = recv_b.local_addr().unwrap();
        let send = KernelUdp::bind(loopback(0)).unwrap();
        if !segmented_send_supported(&send, addr_a) {
            return;
        }
        let _ = drain_n(&recv_a, 2);

        const SEG: usize = 8;
        // Lay out: 3×SEG of 0xAA for dst A, then 2×SEG of 0xBB for dst B.
        let mut data = Vec::new();
        data.extend(std::iter::repeat_n(0xAAu8, 3 * SEG));
        data.extend(std::iter::repeat_n(0xBBu8, 2 * SEG));
        let entries = [
            (addr_a, 0usize, 3 * SEG, SEG as u16),
            (addr_b, 3 * SEG, 2 * SEG, SEG as u16),
        ];

        let sent = sendmmsg_staged_segmented(send.socket.as_raw_fd(), &data, &entries)
            .expect("segmented send");
        assert_eq!(sent, 2);

        let on_a = drain_n(&recv_a, 3);
        assert_eq!(on_a.len(), 3);
        assert!(on_a.iter().all(|p| p.iter().all(|&b| b == 0xAA)));
        let on_b = drain_n(&recv_b, 2);
        assert_eq!(on_b.len(), 2);
        assert!(on_b.iter().all(|p| p.iter().all(|&b| b == 0xBB)));
    }

    #[test]
    fn sendmmsg_staged_segmented_empty_entries_noop() {
        use std::os::unix::io::AsRawFd;
        let send = KernelUdp::bind(loopback(0)).unwrap();
        let n = sendmmsg_staged_segmented(send.socket.as_raw_fd(), &[], &[]).unwrap();
        assert_eq!(n, 0);
    }

    /// Default trait impl correctness: drives the per-segment fallback
    /// path that runs on transports without a kernel override.
    struct LoopbackTransport(KernelUdp);
    impl UdpTransport for LoopbackTransport {
        fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
            self.0.send_to(dst, bytes)
        }
        fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
            self.0.recv_from(buf)
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            self.0.local_addr()
        }
        fn join_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
            self.0.join_multicast_v4(group, iface)
        }
        fn leave_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
            self.0.leave_multicast_v4(group, iface)
        }
        // send_segmented_staged left as the default impl on purpose —
        // this is what we're testing.
    }

    #[test]
    fn send_segmented_staged_default_impl_emits_per_segment_send_to() {
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        let _ = recv.set_recv_buffer_bytes(4 * 1024 * 1024);
        let send = LoopbackTransport(KernelUdp::bind(loopback(0)).unwrap());

        const SEG: usize = 16;
        // 2 full + 1 short.
        let data: Vec<u8> = (0..37u8).collect();
        let entries = [(recv_addr, 0usize, data.len(), SEG as u16)];

        let sent = send.send_segmented_staged(&data, &entries).unwrap();
        assert_eq!(sent, 1, "one msghdr fully sent");

        let dgrams = drain_n(&recv, 3);
        assert_eq!(dgrams.len(), 3);
        assert_eq!(dgrams[0], (0..16u8).collect::<Vec<_>>());
        assert_eq!(dgrams[1], (16..32u8).collect::<Vec<_>>());
        assert_eq!(dgrams[2], (32..37u8).collect::<Vec<_>>());
    }

    #[test]
    fn send_batch_to_empty_is_noop() {
        let send = KernelUdp::bind(loopback(0)).unwrap();
        let dst = loopback(1);
        let sent = send.send_batch_to(dst, &[]).unwrap();
        assert_eq!(sent, 0);
    }

    #[test]
    fn recv_batch_drains_pending_datagrams_in_one_call() {
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let recv_addr = recv.local_addr().unwrap();
        let _ = recv.set_recv_buffer_bytes(4 * 1024 * 1024);
        let send = KernelUdp::bind(loopback(0)).unwrap();
        let send_addr = send.local_addr().unwrap();

        // Fire 8 datagrams with distinct payloads.
        for i in 0..8u8 {
            send.send_to(recv_addr, &[i; 16]).unwrap();
        }

        // Spin briefly until at least one shows up — UDP loopback can
        // race with the test thread on slow CI.
        let mut slots: Vec<DatagramBuf> = (0..16).map(|_| DatagramBuf::new(2048)).collect();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let mut total = 0usize;
        while total < 8 && std::time::Instant::now() < deadline {
            let n = recv.recv_batch(&mut slots[total..]).unwrap();
            for slot in &slots[total..total + n] {
                assert_eq!(slot.len, 16);
                assert_eq!(slot.from, send_addr);
            }
            total += n;
            if n == 0 {
                std::thread::sleep(Duration::from_micros(100));
            }
        }
        assert_eq!(total, 8, "expected all 8 datagrams within deadline");

        // Bytes preserved in order (loopback).
        for (i, slot) in slots.iter().enumerate().take(8) {
            assert!(slot.payload().iter().all(|&b| b == i as u8));
        }
    }

    #[test]
    fn recv_batch_returns_zero_when_idle() {
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let mut slots: Vec<DatagramBuf> = (0..4).map(|_| DatagramBuf::new(2048)).collect();
        let n = recv.recv_batch(&mut slots).unwrap();
        assert_eq!(n, 0);
    }
}
