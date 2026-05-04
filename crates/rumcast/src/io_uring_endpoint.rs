//! LMAX-shaped io_uring UDP endpoint.
//!
//! Threading model:
//!
//! ```text
//!                       ┌─────────────────────────┐
//!                       │   poller thread (own)   │
//!                       │   ─ pinned to one core  │
//!                       │   ─ owns the IoUring    │
//!                       │   ─ owns RecvSlot pool  │
//!                       └─┬───────────┬───────────┘
//!                         │ classify  │ classify
//!                  ┌──────┘           └──────┐
//!                  ▼                         ▼
//!          ┌──────────────┐          ┌──────────────┐
//!          │  send-bound  │          │  recv-bound  │
//!          │  SPSC ring   │          │  SPSC ring   │
//!          └──────┬───────┘          └──────┬───────┘
//!                 │                         │
//!                 ▼                         ▼
//!         publisher half             subscriber half
//!        (recv_from = pop)         (recv_from = pop)
//! ```
//!
//! Exactly one thread (the poller) touches the io_uring. The two
//! consumer halves see only their respective SPSC ring — no shared
//! mutex, no cross-core lock contention. This is the architecture
//! that survives the eventual swap to a DPDK PMD: replace the harvest
//! body with `rte_eth_rx_burst`, everything else stays the same.
//!
//! `send_to` on either half goes straight to the underlying
//! `UdpSocket` (UDP sends are kernel-thread-safe, no benefit from
//! routing through io_uring).
//!
//! # Idle behavior
//!
//! Configurable. For production (single-purpose pinned poller core)
//! the loop busy-spins with `PAUSE` between iterations. For
//! tests / dev / low-load scenarios the poller falls back to
//! `submit_with_args(1, park_timeout)` after `idle_iterations_before_park`
//! consecutive iterations with no work, so it doesn't burn a core
//! at idle.
//!
//! # Lifecycle
//!
//! [`IoUringEndpoint::bind`] starts the poller thread. [`split`]
//! consumes the endpoint and returns the two halves; both halves
//! plus a `PollerHandle` keep the poller alive via shared `Arc`s.
//! When both halves are dropped, the `PollerHandle`'s `Drop` flips
//! the shutdown flag and joins the poller thread.
//!
//! [`split`]: IoUringEndpoint::split

use std::io;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use io_uring::cqueue::Entry as Cqe;
use io_uring::types::Fd;
use io_uring::{IoUring, opcode};

use crate::shared_udp::{Direction, classify};
use crate::spsc::{self, Consumer, Producer};
use crate::transport::UdpTransport;

/// Pre-submitted RecvMsg SQE pool size. The kernel keeps up to this
/// many recv buffers in flight at any moment.
const RECV_POOL: usize = 64;

/// Frame buffer size — must match rumcast's wire frame cap.
const BUF_SIZE: usize = 2048;

/// Submit staged RecvMsg SQEs once this many are pending. Bounded
/// pool shrinkage = `RECV_POOL - SUBMIT_THRESHOLD`; bounded
/// per-packet syscall amortization = `1 / SUBMIT_THRESHOLD`.
const SUBMIT_THRESHOLD: usize = 16;

/// io_uring SQ/CQ size. Power-of-two; must be ≥ `RECV_POOL` plus
/// some headroom for resubmits buffered while pool drains.
const RING_ENTRIES: u32 = 256;

/// Default SPSC capacity per direction. 128 slots × ~2 KB = ~256 KB
/// per ring; two rings ≈ 512 KB. Large enough that the consumer can
/// fall a few hundred frames behind without the producer blocking.
const DEFAULT_SPSC_CAPACITY: usize = 128;

/// Default idle-iterations-before-park. ~10 µs of busy-spin at
/// modern x86 PAUSE rates. Long enough to absorb micro-bursts,
/// short enough that an idle bench/test doesn't burn a core.
const DEFAULT_IDLE_BEFORE_PARK: u32 = 1024;

/// One pinned receive slot. Heap-allocated; `iov` and `msg` hold
/// raw pointers into the same `Box` allocation, which makes the
/// allocation address load-bearing and means we must never move the
/// `RecvSlot` after construction.
struct RecvSlot {
    buf: [u8; BUF_SIZE],
    iov: libc::iovec,
    name: libc::sockaddr_storage,
    msg: libc::msghdr,
}

impl RecvSlot {
    fn new() -> Box<Self> {
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

/// One frame as it travels through the SPSC fan-out: payload bytes,
/// length, and origin address. Fixed-size so SPSC slots stay
/// allocation-free on the hot path.
pub struct Frame {
    /// Sender's socket address.
    pub from: SocketAddr,
    /// Valid bytes in `buf`.
    pub len: u16,
    /// Frame payload. Bytes past `len` are stale.
    pub buf: [u8; BUF_SIZE],
}

impl Frame {
    fn empty() -> Self {
        Self {
            // Placeholder; never read before being overwritten.
            from: SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            len: 0,
            buf: [0u8; BUF_SIZE],
        }
    }
}

/// Endpoint configuration. Defaults are reasonable for the bench;
/// production should set `idle_iterations_before_park = 0` (always
/// busy-spin) and pin the poller to a dedicated core.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// Pin the poller thread to this core. `None` leaves scheduling
    /// to the kernel.
    pub poller_core: Option<usize>,
    /// SPSC capacity per direction. Must be a power of two.
    pub spsc_capacity: usize,
    /// Fall back to `submit_with_args(1, park_timeout)` after this
    /// many consecutive idle iterations. `0` disables the fallback —
    /// the poller busy-spins forever (production default).
    pub idle_iterations_before_park: u32,
    /// Sleep timeout when the poller falls back to a kernel wait.
    /// Ignored if `idle_iterations_before_park == 0`.
    pub park_timeout: Duration,
}

impl Default for EndpointConfig {
    fn default() -> Self {
        Self {
            poller_core: None,
            spsc_capacity: DEFAULT_SPSC_CAPACITY,
            idle_iterations_before_park: DEFAULT_IDLE_BEFORE_PARK,
            park_timeout: Duration::from_millis(1),
        }
    }
}

/// Counters exposed for observability. All values are monotonic.
#[derive(Default, Debug)]
struct EndpointCounters {
    /// Frames classified as recv-bound and pushed onto the recv ring.
    recv_pushed: AtomicU64,
    /// Frames classified as send-bound and pushed onto the send ring.
    send_pushed: AtomicU64,
    /// Frames dropped because the recv ring was full.
    recv_dropped: AtomicU64,
    /// Frames dropped because the send ring was full.
    send_dropped: AtomicU64,
    /// Frames the wire parser rejected.
    parse_dropped: AtomicU64,
    /// CQEs that came back with a kernel error (e.g. ENOBUFS).
    cqe_errors: AtomicU64,
}

/// Owns the io_uring lifecycle. Held by the halves via `Arc`; when
/// both halves drop, this `PollerHandle`'s `Drop` flips `shutdown`
/// and joins the poller thread.
struct PollerHandle {
    shutdown: Arc<AtomicBool>,
    join: Mutex<Option<JoinHandle<()>>>,
    counters: Arc<EndpointCounters>,
}

impl Drop for PollerHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.join.lock().expect("join mutex poisoned").take() {
            // Best-effort: if the poller panicked we just log. Park
            // / shutdown semantics don't depend on join success.
            if let Err(e) = handle.join() {
                tracing::warn!(?e, "io_uring poller thread panicked");
            }
        }
    }
}

/// Endpoint factory. Construct with [`bind`], then [`split`] to get
/// the two halves.
///
/// [`bind`]: IoUringEndpoint::bind
/// [`split`]: IoUringEndpoint::split
pub struct IoUringEndpoint {
    socket: Arc<UdpSocket>,
    poller: Arc<PollerHandle>,
    send_consumer: Consumer<Frame>,
    recv_consumer: Consumer<Frame>,
}

impl IoUringEndpoint {
    /// Bind a fresh `UdpSocket` to `local`, pre-submit `RECV_POOL`
    /// RecvMsg SQEs, and spawn the poller thread.
    pub fn bind(local: SocketAddr, cfg: EndpointConfig) -> io::Result<Self> {
        let socket = Arc::new(UdpSocket::bind(local)?);
        // Non-blocking so the poller's send_to from the halves
        // doesn't stall and so that a misuse of the socket
        // outside io_uring fails loudly.
        socket.set_nonblocking(true)?;

        let (send_producer, send_consumer) = spsc::channel::<Frame>(cfg.spsc_capacity);
        let (recv_producer, recv_consumer) = spsc::channel::<Frame>(cfg.spsc_capacity);

        let shutdown = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(EndpointCounters::default());

        let poller_state = PollerState {
            socket_fd: Fd(socket.as_raw_fd()),
            send_producer,
            recv_producer,
            shutdown: Arc::clone(&shutdown),
            counters: Arc::clone(&counters),
            cfg: cfg.clone(),
        };

        let join = std::thread::Builder::new()
            .name("rumcast-io-uring-poller".to_string())
            .spawn(move || run_poller(poller_state))?;

        let poller = Arc::new(PollerHandle {
            shutdown,
            join: Mutex::new(Some(join)),
            counters,
        });

        Ok(Self {
            socket,
            poller,
            send_consumer,
            recv_consumer,
        })
    }

    /// Bound local address — useful when `local.port() == 0`.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Split into the two halves. The endpoint is consumed; the
    /// poller stays alive until both halves drop.
    pub fn split(self) -> (EndpointSend, EndpointRecv) {
        let send = EndpointSend {
            socket: Arc::clone(&self.socket),
            poller: Arc::clone(&self.poller),
            consumer: Mutex::new(self.send_consumer),
        };
        let recv = EndpointRecv {
            socket: self.socket,
            poller: self.poller,
            consumer: Mutex::new(self.recv_consumer),
        };
        (send, recv)
    }
}

/// State owned exclusively by the poller thread.
struct PollerState {
    socket_fd: Fd,
    send_producer: Producer<Frame>,
    recv_producer: Producer<Frame>,
    shutdown: Arc<AtomicBool>,
    counters: Arc<EndpointCounters>,
    cfg: EndpointConfig,
}

fn run_poller(mut state: PollerState) {
    if let Some(core) = state.cfg.poller_core
        && let Err(e) = pin_current_thread_to_core(core)
    {
        tracing::warn!(core, %e, "io_uring poller failed to pin to core");
    }

    let mut ring = match IoUring::builder().build(RING_ENTRIES) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(%e, "io_uring init failed; poller exiting");
            return;
        }
    };
    let fd = state.socket_fd;

    let mut slots: Vec<Box<RecvSlot>> = (0..RECV_POOL).map(|_| RecvSlot::new()).collect();
    {
        let mut sq = ring.submission();
        for (idx, slot) in slots.iter_mut().enumerate() {
            // Safety: msghdr points into the Box allocation, stable
            // for the lifetime of `slots`. Index-as-user-data lets
            // CQE handling find the slot in O(1).
            let msg_ptr = &mut slot.msg as *mut libc::msghdr;
            let entry = opcode::RecvMsg::new(fd, msg_ptr)
                .build()
                .user_data(idx as u64);
            unsafe {
                sq.push(&entry)
                    .expect("SQ full on init — RING_ENTRIES too small")
            };
        }
    }
    if let Err(e) = ring.submitter().submit() {
        tracing::error!(%e, "io_uring initial submit failed; poller exiting");
        return;
    }

    let mut pending_resubmit: Vec<usize> = Vec::with_capacity(RECV_POOL);
    let mut unsubmitted: usize = 0;
    let mut idle_iterations: u32 = 0;

    while !state.shutdown.load(Ordering::Acquire) {
        let mut work_done = false;

        // Harvest CQEs.
        {
            let cq: io_uring::cqueue::CompletionQueue<'_, Cqe> = ring.completion();
            for cqe in cq {
                work_done = true;
                let slot_idx = cqe.user_data() as usize;
                let res = cqe.result();
                if res < 0 {
                    state.counters.cqe_errors.fetch_add(1, Ordering::Relaxed);
                    pending_resubmit.push(slot_idx);
                    continue;
                }
                let len = res as usize;
                let slot = &slots[slot_idx];
                let from = sockaddr_to_socket_addr(&slot.name);
                let bytes = &slot.buf[..len];
                dispatch_frame(&mut state, from, len, bytes);
                pending_resubmit.push(slot_idx);
            }
        }

        // Stage resubmits.
        if !pending_resubmit.is_empty() {
            let pushed = pending_resubmit.len();
            let mut sq = ring.submission();
            for &idx in &pending_resubmit {
                // Reset msg_namelen — kernel mutates it on each
                // completion, so without this an IPv4-after-IPv6
                // recv would see a truncated sockaddr buffer.
                let slot = &mut slots[idx];
                slot.msg.msg_namelen =
                    std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                // Safety: slot is reaped (not in flight); msghdr is
                // heap-stable.
                let msg_ptr = &mut slot.msg as *mut libc::msghdr;
                let entry = opcode::RecvMsg::new(fd, msg_ptr)
                    .build()
                    .user_data(idx as u64);
                unsafe {
                    sq.push(&entry)
                        .expect("SQ full — RING_ENTRIES undersized for RECV_POOL")
                };
            }
            pending_resubmit.clear();
            unsubmitted += pushed;
        }

        if unsubmitted >= SUBMIT_THRESHOLD {
            // submit() may EINTR / EBUSY transiently; on success the
            // SQEs are with the kernel and we can reset. On failure
            // we leave `unsubmitted` so the next iteration retries.
            if ring.submitter().submit().is_ok() {
                unsubmitted = 0;
            }
        }

        if work_done {
            idle_iterations = 0;
            continue;
        }

        idle_iterations = idle_iterations.saturating_add(1);
        let park_threshold = state.cfg.idle_iterations_before_park;
        if park_threshold != 0 && idle_iterations >= park_threshold {
            // Soft-park: submit any staged SQEs and wait for one
            // CQE or the timeout, whichever first.
            soft_park(&mut ring, &mut unsubmitted, state.cfg.park_timeout);
            idle_iterations = 0;
        } else {
            std::hint::spin_loop();
        }
    }
}

/// Classify a frame and push to the matching SPSC, or count the drop.
#[inline]
fn dispatch_frame(state: &mut PollerState, from: SocketAddr, len: usize, bytes: &[u8]) {
    match classify(bytes) {
        Direction::Recv => {
            let mut frame = Frame::empty();
            frame.from = from;
            frame.len = len as u16;
            frame.buf[..len].copy_from_slice(bytes);
            if state.recv_producer.try_push(frame).is_err() {
                state.counters.recv_dropped.fetch_add(1, Ordering::Relaxed);
            } else {
                state.counters.recv_pushed.fetch_add(1, Ordering::Relaxed);
            }
        }
        Direction::Send => {
            let mut frame = Frame::empty();
            frame.from = from;
            frame.len = len as u16;
            frame.buf[..len].copy_from_slice(bytes);
            if state.send_producer.try_push(frame).is_err() {
                state.counters.send_dropped.fetch_add(1, Ordering::Relaxed);
            } else {
                state.counters.send_pushed.fetch_add(1, Ordering::Relaxed);
            }
        }
        Direction::Drop => {
            state.counters.parse_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn soft_park(ring: &mut IoUring, unsubmitted: &mut usize, timeout: Duration) {
    use io_uring::types::{SubmitArgs, Timespec};
    let ts = Timespec::from(timeout);
    let args = SubmitArgs::new().timespec(&ts);
    match ring.submitter().submit_with_args(1, &args) {
        Ok(_) => *unsubmitted = 0,
        Err(e) => {
            let raw = e.raw_os_error();
            if raw == Some(libc::ETIME) || raw == Some(libc::EINTR) {
                // Submission still happened before the wait timed
                // out / was interrupted.
                *unsubmitted = 0;
            } else {
                tracing::warn!(error = %e, "io_uring submit_with_args failed");
            }
        }
    }
}

/// Publisher half. `send_to` goes direct to the kernel socket; the
/// poller-fed SPSC ring delivers NAK / StatusMessage frames via
/// `recv_from`.
pub struct EndpointSend {
    socket: Arc<UdpSocket>,
    poller: Arc<PollerHandle>,
    /// Mutex is uncontended in normal use (one thread per half) and
    /// gives the trait-required `&self` recv API on top of the SPSC
    /// `&mut self` consumer.
    consumer: Mutex<Consumer<Frame>>,
}

/// Subscriber half. `send_to` goes direct to the kernel socket; the
/// poller-fed SPSC ring delivers Data / Setup / Heartbeat frames via
/// `recv_from`.
pub struct EndpointRecv {
    socket: Arc<UdpSocket>,
    poller: Arc<PollerHandle>,
    consumer: Mutex<Consumer<Frame>>,
}

impl UdpTransport for EndpointSend {
    #[inline]
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send_to(bytes, dst)
    }

    #[inline]
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        consume_one(&self.consumer, buf)
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

impl UdpTransport for EndpointRecv {
    #[inline]
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send_to(bytes, dst)
    }

    #[inline]
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        consume_one(&self.consumer, buf)
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

/// Counters for diagnostics: `(recv_pushed, send_pushed, recv_dropped,
/// send_dropped, parse_dropped, cqe_errors)`.
impl EndpointSend {
    pub fn counters(&self) -> (u64, u64, u64, u64, u64, u64) {
        snapshot_counters(&self.poller.counters)
    }
}

impl EndpointRecv {
    pub fn counters(&self) -> (u64, u64, u64, u64, u64, u64) {
        snapshot_counters(&self.poller.counters)
    }
}

fn snapshot_counters(c: &EndpointCounters) -> (u64, u64, u64, u64, u64, u64) {
    (
        c.recv_pushed.load(Ordering::Relaxed),
        c.send_pushed.load(Ordering::Relaxed),
        c.recv_dropped.load(Ordering::Relaxed),
        c.send_dropped.load(Ordering::Relaxed),
        c.parse_dropped.load(Ordering::Relaxed),
        c.cqe_errors.load(Ordering::Relaxed),
    )
}

#[inline]
fn consume_one(
    consumer: &Mutex<Consumer<Frame>>,
    buf: &mut [u8],
) -> io::Result<Option<(SocketAddr, usize)>> {
    let mut guard = consumer.lock().expect("consumer mutex poisoned");
    match guard.try_pop() {
        None => Ok(None),
        Some(frame) => {
            let len = (frame.len as usize).min(buf.len());
            buf[..len].copy_from_slice(&frame.buf[..len]);
            Ok(Some((frame.from, len)))
        }
    }
}

/// Convert kernel-filled sockaddr_storage to SocketAddr. Anything
/// that isn't AF_INET / AF_INET6 maps to 0.0.0.0:0 — the wire
/// parser will reject the frame regardless.
fn sockaddr_to_socket_addr(storage: &libc::sockaddr_storage) -> SocketAddr {
    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            // Safety: ss_family == AF_INET → storage is sockaddr_in.
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

/// Pin the calling thread to one core via `sched_setaffinity`.
/// Errors carry the OS error string — caller logs and continues
/// (best-effort).
fn pin_current_thread_to_core(core: usize) -> io::Result<()> {
    // CPU_SET on a single-core mask. We don't need the full
    // affinity dance from `melin-server` because the poller is a
    // single thread that we just spawned; the parent's affinity
    // mask is irrelevant.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_SET(core, &mut set) };
    let ret = unsafe {
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set as *const _)
    };
    if ret == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{DataFrame, HeartbeatFrame, NakFrame, SetupFrame, StatusMessage, data_flags};
    use std::net::{IpAddr, UdpSocket};
    use std::time::Instant;

    const SESSION: u32 = 7;
    const STREAM: u32 = 11;

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn data_frame(payload: &[u8]) -> Vec<u8> {
        let header = DataFrame::new(
            SESSION,
            STREAM,
            100,
            0,
            data_flags::UNFRAGMENTED,
            payload.len() as u32,
        );
        let mut buf = Vec::with_capacity(DataFrame::HEADER_LEN + payload.len());
        buf.extend_from_slice(bytemuck::bytes_of(&header));
        buf.extend_from_slice(payload);
        buf
    }

    fn nak_frame() -> Vec<u8> {
        let nak = NakFrame::new(SESSION, STREAM, 100, 0, 96);
        bytemuck::bytes_of(&nak).to_vec()
    }

    fn sm_frame() -> Vec<u8> {
        let sm = StatusMessage::new(SESSION, STREAM, 100, 0, 64 * 1024, 1);
        bytemuck::bytes_of(&sm).to_vec()
    }

    fn setup_frame() -> Vec<u8> {
        let s = SetupFrame::new(SESSION, STREAM, 100, 100, 0, 64 * 1024);
        bytemuck::bytes_of(&s).to_vec()
    }

    fn heartbeat_frame() -> Vec<u8> {
        let h = HeartbeatFrame::new(SESSION, STREAM);
        bytemuck::bytes_of(&h).to_vec()
    }

    fn recv_one<T: UdpTransport>(t: &T, deadline: Instant) -> Vec<u8> {
        let mut buf = [0u8; BUF_SIZE];
        while Instant::now() < deadline {
            if let Some((_, len)) = t.recv_from(&mut buf).expect("recv_from") {
                return buf[..len].to_vec();
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        panic!("no datagram within deadline");
    }

    /// Default test config: short park timeout so idle tests don't
    /// burn a core, but small idle threshold so the poller picks up
    /// work promptly.
    fn test_cfg() -> EndpointConfig {
        EndpointConfig {
            idle_iterations_before_park: 4,
            park_timeout: Duration::from_millis(1),
            ..Default::default()
        }
    }

    #[test]
    fn data_frame_routes_to_recv_half() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (send_half, recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        let frame = data_frame(b"hello");
        peer.send_to(&frame, bound).unwrap();

        let bytes = recv_one(&recv_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(bytes, frame);

        let mut buf = [0u8; BUF_SIZE];
        assert!(send_half.recv_from(&mut buf).unwrap().is_none());
    }

    #[test]
    fn nak_routes_to_send_half() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (send_half, recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        let frame = nak_frame();
        peer.send_to(&frame, bound).unwrap();

        let bytes = recv_one(&send_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(bytes, frame);

        let mut buf = [0u8; BUF_SIZE];
        assert!(recv_half.recv_from(&mut buf).unwrap().is_none());
    }

    #[test]
    fn sm_routes_to_send_half() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (send_half, _recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        let frame = sm_frame();
        peer.send_to(&frame, bound).unwrap();

        let bytes = recv_one(&send_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(bytes, frame);
    }

    #[test]
    fn setup_routes_to_recv_half() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (_send_half, recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        let frame = setup_frame();
        peer.send_to(&frame, bound).unwrap();

        let bytes = recv_one(&recv_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(bytes, frame);
    }

    #[test]
    fn heartbeat_routes_to_recv_half() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (_send_half, recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        let frame = heartbeat_frame();
        peer.send_to(&frame, bound).unwrap();

        let bytes = recv_one(&recv_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(bytes, frame);
    }

    #[test]
    fn unparseable_frames_are_counted_and_dropped() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (send_half, recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        peer.send_to(b"not a rumcast frame", bound).unwrap();

        // Wait for parse_dropped to tick.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let (_, _, _, _, parse_dropped, _) = send_half.counters();
            if parse_dropped >= 1 {
                break;
            }
            assert!(Instant::now() < deadline, "parse_dropped never incremented");
            std::thread::sleep(Duration::from_micros(100));
        }

        // Neither half ever sees the bytes.
        let mut buf = [0u8; BUF_SIZE];
        assert!(send_half.recv_from(&mut buf).unwrap().is_none());
        assert!(recv_half.recv_from(&mut buf).unwrap().is_none());
    }

    #[test]
    fn send_to_passes_through_socket() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let (send_half, _recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let peer_addr = peer.local_addr().unwrap();

        send_half.send_to(peer_addr, b"hello").unwrap();

        let mut buf = [0u8; 64];
        let (n, _) = peer.recv_from(&mut buf).expect("peer recv");
        assert_eq!(&buf[..n], b"hello");
    }

    #[test]
    fn dropping_both_halves_shuts_poller_down() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let (send_half, recv_half) = endpoint.split();
        let _ = send_half.counters();

        let start = Instant::now();
        drop(send_half);
        drop(recv_half);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "shutdown should be prompt"
        );
    }

    #[test]
    fn local_addr_is_reported_consistently_across_halves() {
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (send_half, recv_half) = endpoint.split();
        assert_eq!(send_half.local_addr().unwrap(), bound);
        assert_eq!(recv_half.local_addr().unwrap(), bound);
    }
}
