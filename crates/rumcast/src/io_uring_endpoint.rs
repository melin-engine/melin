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
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use io_uring::cqueue::Entry as Cqe;
use io_uring::types::Fd;
use io_uring::{IoUring, opcode};

use crate::shared_udp::{Direction, classify};
use crate::spsc::{self, Consumer, Producer};
use crate::transport::{DatagramBuf, UdpTransport};

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

// RecvSlot's `iov` and `msg` fields hold raw pointers to other
// fields of the same `Box<RecvSlot>` — sending the box across threads
// keeps those pointers valid (the Box payload doesn't move). The
// poller thread is the sole accessor after construction.
unsafe impl Send for RecvSlot {}

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
/// allocation-free on the hot path. `buf` is `MaybeUninit` so the
/// in-place initializer skips zeroing the unused tail — at typical
/// frame sizes (~1 KB on a 2 KB cap), that's a 1 KB memset saved per
/// packet.
pub(crate) struct Frame {
    /// Sender's socket address.
    from: SocketAddr,
    /// Valid bytes in `buf`. Reads past this are forbidden — the
    /// underlying memory is `MaybeUninit`.
    len: u16,
    /// Frame payload. Only `buf[..len]` is initialized.
    buf: [MaybeUninit<u8>; BUF_SIZE],
}

impl Frame {
    /// Initialize a Frame at `slot` from header fields and a payload
    /// slice. Writes `from`, `len`, and `buf[..bytes.len()]`; leaves
    /// `buf[bytes.len()..]` uninitialized (and unreadable through the
    /// public API, which always bounds reads by `len`).
    ///
    /// # Safety
    ///
    /// - `slot` must point to writable memory sized and aligned for
    ///   `Frame` (i.e. produced by `Producer::try_claim`).
    /// - `bytes.len()` must be `<= BUF_SIZE`.
    /// - After this call the slot is fully valid (reads through
    ///   `payload()` only touch initialized bytes).
    #[inline]
    unsafe fn init_in_place(slot: *mut Frame, from: SocketAddr, bytes: &[u8]) {
        debug_assert!(bytes.len() <= BUF_SIZE);
        // `addr_of_mut!` avoids creating a `&mut Frame` to a
        // half-initialized struct (which would be UB).
        unsafe {
            std::ptr::addr_of_mut!((*slot).from).write(from);
            std::ptr::addr_of_mut!((*slot).len).write(bytes.len() as u16);
            let buf_ptr = std::ptr::addr_of_mut!((*slot).buf) as *mut u8;
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf_ptr, bytes.len());
        }
    }

    /// Initialized payload bytes. Bounded by `len`, so never reads
    /// the uninitialized tail of `buf`.
    #[inline]
    fn payload(&self) -> &[u8] {
        // Safety: producer's `init_in_place` initialized exactly the
        // first `len` bytes; `MaybeUninit<u8>` and `u8` have identical
        // layout so the cast is sound.
        unsafe { std::slice::from_raw_parts(self.buf.as_ptr() as *const u8, self.len as usize) }
    }

    /// Sender address recorded by the kernel.
    #[inline]
    fn from(&self) -> SocketAddr {
        self.from
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
    /// `eventfd(2)` used to wake a consumer half blocked in
    /// [`UdpTransport::park`]. The poller writes to it after pushing
    /// frames into the SPSC; halves poll it during park. This bridges
    /// the userspace SPSC handoff that io_uring's CQ-readiness signal
    /// alone doesn't cover (io_uring's submit_with_args wakes the
    /// poller, not the consumer thread).
    wake_fd: RawFd,
}

impl Drop for PollerHandle {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        // Wake the poller in case it's parked in submit_with_args, so
        // it observes the shutdown flag promptly. (No-op if it's busy.)
        wake_eventfd(self.wake_fd);
        if let Some(handle) = self.join.lock().expect("join mutex poisoned").take() {
            // Best-effort: if the poller panicked we just log. Park
            // / shutdown semantics don't depend on join success.
            if let Err(e) = handle.join() {
                tracing::warn!(?e, "io_uring poller thread panicked");
            }
        }
        // Close the eventfd. Best-effort — process exit would close
        // it anyway.
        unsafe {
            libc::close(self.wake_fd);
        }
    }
}

/// Write `1` to a non-blocking eventfd. Errors (EAGAIN if the counter
/// is at u64::MAX, which never happens in practice) are swallowed —
/// the only consequence is one missed wakeup, which is bounded by the
/// half's park_timeout.
#[inline]
fn wake_eventfd(fd: RawFd) {
    let val: u64 = 1;
    // Safety: writing 8 bytes to an eventfd is the documented API.
    unsafe {
        let _ = libc::write(fd, &val as *const _ as *const libc::c_void, 8);
    }
}

/// Drain pending wakeups from the eventfd by reading its accumulated
/// counter (single 8-byte read resets to 0). Non-blocking: returns
/// silently if the counter is already 0.
#[inline]
fn drain_eventfd(fd: RawFd) {
    let mut val: u64 = 0;
    // Safety: reading 8 bytes from an eventfd is the documented API.
    // EAGAIN (counter == 0) is the expected idle case.
    unsafe {
        let _ = libc::read(fd, &mut val as *mut _ as *mut libc::c_void, 8);
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
    /// Bind a fresh `UdpSocket` to `local`, build the `IoUring` and
    /// pre-submit `RECV_POOL` RecvMsg SQEs, then spawn the poller
    /// thread. Any failure in socket binding, ring construction, or
    /// initial submit surfaces as `io::Error` here — the endpoint
    /// is never returned with a dead poller.
    pub fn bind(local: SocketAddr, cfg: EndpointConfig) -> io::Result<Self> {
        Self::bind_paused(local, cfg)?.start()
    }

    /// Two-phase construction: build the socket, ring, and SPSC rings
    /// without spawning the poller. Useful when the caller needs to
    /// configure the socket (e.g. `set_recv_buffer_bytes`) and finish
    /// other init (e.g. seeding an engine pipeline) before traffic
    /// starts being harvested into the SPSC ring. Without the pause,
    /// the kernel's rcvbuf can fill faster than the consumer drains
    /// and the SPSC ring overflows during slow init.
    ///
    /// Call [`PausedEndpoint::start`] when ready to begin harvesting.
    pub fn bind_paused(local: SocketAddr, cfg: EndpointConfig) -> io::Result<PausedEndpoint> {
        let socket = Arc::new(UdpSocket::bind(local)?);
        // Non-blocking so the halves' direct `send_to` paths never
        // stall, and any misuse outside io_uring fails loudly rather
        // than silently blocking.
        socket.set_nonblocking(true)?;

        // Eventfd used by the poller to wake any consumer half that's
        // currently parked. NONBLOCK so the drain path never blocks;
        // CLOEXEC for hygiene.
        // Safety: eventfd(2) returns a fresh fd or -1 on error.
        let wake_fd: RawFd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if wake_fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // Build the ring on this thread so init failures surface as
        // a normal `io::Result` to the caller.
        let mut ring: IoUring = IoUring::builder().build(RING_ENTRIES)?;
        let fd = Fd(socket.as_raw_fd());

        // Pre-allocate slots and pre-submit RecvMsg SQEs. The kernel
        // will start completing these as packets arrive even though
        // the poller hasn't yet been spawned — completions accumulate
        // in the CQ until `start()` runs and begins harvesting.
        let mut slots: Vec<Box<RecvSlot>> = (0..RECV_POOL).map(|_| RecvSlot::new()).collect();
        {
            let mut sq = ring.submission();
            for (idx, slot) in slots.iter_mut().enumerate() {
                // Safety: msghdr points into the Box's heap allocation,
                // stable for the lifetime of `slots` (slots is moved
                // into the poller closure below, so it outlives the
                // ring's view of the SQEs).
                let msg_ptr = &mut slot.msg as *mut libc::msghdr;
                let entry = opcode::RecvMsg::new(fd, msg_ptr)
                    .build()
                    .user_data(idx as u64);
                unsafe {
                    sq.push(&entry)
                        .expect("SQ full on init — RING_ENTRIES too small for RECV_POOL")
                };
            }
        }
        ring.submitter().submit()?;

        let (send_producer, send_consumer) = spsc::channel::<Frame>(cfg.spsc_capacity);
        let (recv_producer, recv_consumer) = spsc::channel::<Frame>(cfg.spsc_capacity);

        Ok(PausedEndpoint {
            socket,
            ring,
            slots,
            socket_fd: fd,
            send_producer,
            recv_producer,
            send_consumer,
            recv_consumer,
            cfg,
            wake_fd,
        })
    }

    /// Bound local address — useful when `local.port() == 0`.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Request a larger SO_RCVBUF on the underlying socket. Mirrors
    /// `KernelUdp::set_recv_buffer_bytes`; the kernel may cap the
    /// effective size at `net.core.rmem_max`. Useful on the server's
    /// orders socket to absorb client bursts during init.
    pub fn set_recv_buffer_bytes(&self, bytes: usize) -> io::Result<()> {
        let val: libc::c_int = bytes.min(i32::MAX as usize) as libc::c_int;
        // Safety: setsockopt with a valid fd, level, name, and a
        // pointer + length to a stack `c_int` matches the SO_RCVBUF
        // ABI on Linux.
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

/// Paused endpoint returned by [`IoUringEndpoint::bind_paused`]. The
/// socket is bound, the io_uring ring is built, RecvMsg SQEs are
/// submitted, and SPSC rings are allocated — but no poller thread
/// has been spawned yet. Inbound traffic accumulates in the kernel's
/// rcvbuf until [`PausedEndpoint::start`] is called.
pub struct PausedEndpoint {
    socket: Arc<UdpSocket>,
    ring: IoUring,
    slots: Vec<Box<RecvSlot>>,
    socket_fd: Fd,
    send_producer: Producer<Frame>,
    recv_producer: Producer<Frame>,
    send_consumer: Consumer<Frame>,
    recv_consumer: Consumer<Frame>,
    cfg: EndpointConfig,
    wake_fd: RawFd,
}

impl PausedEndpoint {
    /// Bound local address — useful when `local.port() == 0`.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Configure SO_RCVBUF on the underlying socket. Mirrors
    /// [`IoUringEndpoint::set_recv_buffer_bytes`]; called pre-start so
    /// the kernel's rcvbuf is sized to absorb traffic during the
    /// pause window.
    pub fn set_recv_buffer_bytes(&self, bytes: usize) -> io::Result<()> {
        let val: libc::c_int = bytes.min(i32::MAX as usize) as libc::c_int;
        // Safety: setsockopt on a valid socket fd with the SO_RCVBUF
        // ABI shape. Same as `IoUringEndpoint::set_recv_buffer_bytes`.
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

    /// Spawn the poller thread and transition into a live
    /// [`IoUringEndpoint`]. Frames already queued in the kernel
    /// rcvbuf and CQEs already completed start being harvested
    /// immediately.
    pub fn start(self) -> io::Result<IoUringEndpoint> {
        let PausedEndpoint {
            socket,
            ring,
            slots,
            socket_fd,
            send_producer,
            recv_producer,
            send_consumer,
            recv_consumer,
            cfg,
            wake_fd,
        } = self;

        let shutdown = Arc::new(AtomicBool::new(false));
        let counters = Arc::new(EndpointCounters::default());

        let poller_state = PollerState {
            ring,
            slots,
            socket_fd,
            send_producer,
            recv_producer,
            shutdown: Arc::clone(&shutdown),
            counters: Arc::clone(&counters),
            cfg,
            wake_fd,
        };

        let join = std::thread::Builder::new()
            .name("rumcast-io-uring-poller".to_string())
            .spawn(move || run_poller(poller_state))?;

        let poller = Arc::new(PollerHandle {
            shutdown,
            wake_fd,
            join: Mutex::new(Some(join)),
            counters,
        });

        Ok(IoUringEndpoint {
            socket,
            poller,
            send_consumer,
            recv_consumer,
        })
    }
}

/// State owned exclusively by the poller thread. Built (and the
/// initial RecvMsg SQEs submitted) on the calling thread inside
/// `bind` so I/O errors surface synchronously.
struct PollerState {
    ring: IoUring,
    slots: Vec<Box<RecvSlot>>,
    socket_fd: Fd,
    send_producer: Producer<Frame>,
    recv_producer: Producer<Frame>,
    shutdown: Arc<AtomicBool>,
    counters: Arc<EndpointCounters>,
    cfg: EndpointConfig,
    wake_fd: RawFd,
}

fn run_poller(state: PollerState) {
    let PollerState {
        mut ring,
        mut slots,
        socket_fd: fd,
        mut send_producer,
        mut recv_producer,
        shutdown,
        counters,
        cfg,
        wake_fd,
    } = state;

    if let Some(core) = cfg.poller_core
        && let Err(e) = pin_current_thread_to_core(core)
    {
        tracing::warn!(core, %e, "io_uring poller failed to pin to core");
    }

    let mut pending_resubmit: Vec<usize> = Vec::with_capacity(RECV_POOL);
    let mut unsubmitted: usize = 0;
    let mut idle_iterations: u32 = 0;

    // Relaxed is sufficient: shutdown is a one-way flag with no
    // associated data being published — we only need eventual
    // visibility of the store.
    while !shutdown.load(Ordering::Relaxed) {
        let mut work_done = false;

        // Harvest CQEs.
        let mut frames_pushed_this_iter = 0u32;
        {
            let cq: io_uring::cqueue::CompletionQueue<'_, Cqe> = ring.completion();
            for cqe in cq {
                work_done = true;
                let slot_idx = cqe.user_data() as usize;
                let res = cqe.result();
                if res < 0 {
                    counters.cqe_errors.fetch_add(1, Ordering::Relaxed);
                    pending_resubmit.push(slot_idx);
                    continue;
                }
                let len = res as usize;
                let slot = &slots[slot_idx];
                let from = sockaddr_to_socket_addr(&slot.name);
                let bytes = &slot.buf[..len];
                let pushed = dispatch_frame(
                    &mut send_producer,
                    &mut recv_producer,
                    &counters,
                    from,
                    bytes,
                );
                if pushed {
                    frames_pushed_this_iter += 1;
                }
                pending_resubmit.push(slot_idx);
            }
        }
        // Wake any consumer parked in `EndpointSend::park` /
        // `EndpointRecv::park`. One eventfd write per harvest pass
        // (not per frame) — eventfd accumulates, a single read drains.
        // This is the userspace bridge io_uring's submit_with_args
        // doesn't provide on its own.
        if frames_pushed_this_iter > 0 {
            wake_eventfd(wake_fd);
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
        let park_threshold = cfg.idle_iterations_before_park;
        if park_threshold != 0 && idle_iterations >= park_threshold {
            // Soft-park: submit any staged SQEs and wait for one
            // CQE or the timeout, whichever first. Note: shutdown
            // observation is bounded by `park_timeout` — the poller
            // may sleep for up to that long before noticing the
            // shutdown flag was set.
            soft_park(&mut ring, &mut unsubmitted, cfg.park_timeout);
            idle_iterations = 0;
        } else {
            std::hint::spin_loop();
        }
    }
}

/// Classify a frame and push to the matching SPSC, or count the drop.
/// Returns `true` if the frame was successfully published to a
/// consumer ring (so the caller can issue one eventfd wake per
/// harvest batch instead of per frame).
#[inline]
fn dispatch_frame(
    send_producer: &mut Producer<Frame>,
    recv_producer: &mut Producer<Frame>,
    counters: &EndpointCounters,
    from: SocketAddr,
    bytes: &[u8],
) -> bool {
    let (producer, pushed_counter, dropped_counter) = match classify(bytes) {
        Direction::Recv => (recv_producer, &counters.recv_pushed, &counters.recv_dropped),
        Direction::Send => (send_producer, &counters.send_pushed, &counters.send_dropped),
        Direction::Drop => {
            counters.parse_dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
    };

    match producer.try_claim() {
        None => {
            dropped_counter.fetch_add(1, Ordering::Relaxed);
            false
        }
        Some(claim) => {
            // Safety: claim hands us writable, properly-aligned memory
            // for a `Frame`; init_in_place fully initializes it; commit
            // publishes after init.
            unsafe {
                Frame::init_in_place(claim.as_mut_ptr(), from, bytes);
                claim.commit();
            }
            pushed_counter.fetch_add(1, Ordering::Relaxed);
            true
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

    fn send_batch_to(&self, dst: SocketAddr, payloads: &[&[u8]]) -> io::Result<usize> {
        // Same kernel-fast-path sendmmsg as KernelUdp — io_uring's
        // SendMsg SQEs aren't a meaningful win for UDP sends and
        // would add ring contention with the recv poller.
        crate::transport::sendmmsg_to(self.socket.as_raw_fd(), dst, payloads)
    }

    fn send_multi_to(&self, entries: &[(SocketAddr, &[u8])]) -> io::Result<usize> {
        crate::transport::sendmmsg_multi_to(self.socket.as_raw_fd(), entries)
    }

    #[inline]
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        consume_one(&self.consumer, buf)
    }

    fn recv_batch(&self, slots: &mut [DatagramBuf]) -> io::Result<usize> {
        // Drain N frames from the SPSC under ONE Mutex acquire —
        // turns N lock/unlock cmpxchgs into one. The kernel-side
        // batching already happened in the poller's CQE harvest;
        // this batches the consumer hop too.
        consume_batch(&self.consumer, slots)
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
        park_on_eventfd(&self.consumer, self.poller.wake_fd, timeout);
    }
}

impl UdpTransport for EndpointRecv {
    #[inline]
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send_to(bytes, dst)
    }

    fn send_batch_to(&self, dst: SocketAddr, payloads: &[&[u8]]) -> io::Result<usize> {
        crate::transport::sendmmsg_to(self.socket.as_raw_fd(), dst, payloads)
    }

    fn send_multi_to(&self, entries: &[(SocketAddr, &[u8])]) -> io::Result<usize> {
        crate::transport::sendmmsg_multi_to(self.socket.as_raw_fd(), entries)
    }

    #[inline]
    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        consume_one(&self.consumer, buf)
    }

    fn recv_batch(&self, slots: &mut [DatagramBuf]) -> io::Result<usize> {
        consume_batch(&self.consumer, slots)
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
        park_on_eventfd(&self.consumer, self.poller.wake_fd, timeout);
    }
}

/// Event-driven park: returns immediately if the SPSC consumer
/// already has frames; otherwise waits on the poller's eventfd
/// up to `timeout`. The poller writes one byte to the eventfd
/// per harvest pass that pushed at least one frame, so the
/// caller wakes within microseconds of frame arrival rather
/// than sleeping the full timeout.
///
/// io_uring already wakes its own poller via `submit_with_args`,
/// but that's the kernel→poller signal. The poller→consumer
/// handoff is a userspace SPSC ring, which io_uring doesn't see;
/// the eventfd plugs that gap.
fn park_on_eventfd(consumer: &Mutex<Consumer<Frame>>, wake_fd: RawFd, timeout: Duration) {
    // Fast path: SPSC already non-empty (poller pushed before we
    // got here, or another caller's wakeup is still pending).
    {
        let g = consumer.lock().expect("consumer mutex poisoned");
        if !g.is_empty() {
            return;
        }
    }
    // Slow path: poll(2) on the eventfd. Capped at i32::MAX ms
    // (~25 days) to fit poll's signed-int timeout.
    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as libc::c_int;
    let mut pfd = libc::pollfd {
        fd: wake_fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // Safety: pfd is a valid pointer to one initialized pollfd.
    // Errors (EINTR, EFAULT) are ignored — the caller will recheck
    // the SPSC on the next iteration regardless.
    unsafe {
        libc::poll(&mut pfd, 1, timeout_ms);
    }
    // Drain whatever the poller wrote so the next park doesn't
    // wake immediately on a stale signal. Non-blocking; EAGAIN is
    // the expected idle case.
    drain_eventfd(wake_fd);
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
            let payload = frame.payload();
            let len = payload.len().min(buf.len());
            buf[..len].copy_from_slice(&payload[..len]);
            Ok(Some((frame.from(), len)))
        }
    }
}

/// Drain up to `slots.len()` frames from the SPSC consumer under one
/// `Mutex` acquire. Each filled slot has its `from` and `len` updated
/// in place. Returns the number filled.
fn consume_batch(
    consumer: &Mutex<Consumer<Frame>>,
    slots: &mut [DatagramBuf],
) -> io::Result<usize> {
    let mut guard = consumer.lock().expect("consumer mutex poisoned");
    let mut filled = 0;
    for slot in slots.iter_mut() {
        match guard.try_pop() {
            None => break,
            Some(frame) => {
                let payload = frame.payload();
                let buf = slot.as_mut_slice();
                let len = payload.len().min(buf.len());
                buf[..len].copy_from_slice(&payload[..len]);
                slot.from = frame.from();
                slot.len = len;
                filled += 1;
            }
        }
    }
    Ok(filled)
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

    /// Build a Data frame whose payload encodes `seq` as little-endian
    /// u32 — used by the ordering test to verify FIFO across the SPSC.
    fn data_frame_with_seq(seq: u32) -> Vec<u8> {
        let payload = seq.to_le_bytes();
        data_frame(&payload)
    }

    #[test]
    fn frames_arrive_in_fifo_order_through_spsc() {
        // SPSC promises FIFO. A peer fires N data frames; the recv
        // half must observe them in the same order. Loopback UDP is
        // already in-order at the kernel; this test pins down the
        // guarantee that the endpoint adds nothing that could
        // reorder.
        const N: u32 = 64;
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (_send_half, recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        for seq in 0..N {
            peer.send_to(&data_frame_with_seq(seq), bound).unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        for expected in 0..N {
            let bytes = recv_one(&recv_half, deadline);
            // Payload starts at DataFrame::HEADER_LEN.
            let payload = &bytes[DataFrame::HEADER_LEN..];
            assert_eq!(payload.len(), 4, "unexpected payload length");
            let got = u32::from_le_bytes(payload.try_into().unwrap());
            assert_eq!(got, expected, "frames out of order");
        }
    }

    #[test]
    fn slow_consumer_overflow_increments_recv_dropped() {
        // SPSC has finite capacity. If the consumer never pops while
        // the producer fans out frames, the ring fills and subsequent
        // frames are counted as dropped.
        let cfg = EndpointConfig {
            // Tiny ring so we don't have to fire many frames.
            spsc_capacity: 4,
            idle_iterations_before_park: 4,
            park_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let endpoint = IoUringEndpoint::bind(loopback(0), cfg).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (send_half, _recv_half) = endpoint.split();

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        // Fire well more than spsc_capacity so the ring backs up.
        const FIRE: u32 = 64;
        for seq in 0..FIRE {
            peer.send_to(&data_frame_with_seq(seq), bound).unwrap();
        }

        // Wait for recv_dropped to be non-zero (poller has caught up
        // and observed the ring-full condition).
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let (recv_pushed, _, recv_dropped, _, _, _) = send_half.counters();
            // Once the poller has classified all frames, pushed +
            // dropped should equal FIRE; recv_dropped must be > 0
            // because spsc_capacity=4 < FIRE=64.
            if recv_dropped > 0 && recv_pushed + recv_dropped >= FIRE as u64 {
                assert!(
                    recv_dropped >= (FIRE as u64) - 4,
                    "expected ~{} drops, got recv_pushed={}, recv_dropped={}",
                    FIRE - 4,
                    recv_pushed,
                    recv_dropped,
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "drop counter never accumulated: pushed={}, dropped={}",
                recv_pushed,
                recv_dropped,
            );
            std::thread::sleep(Duration::from_micros(200));
        }
    }

    #[test]
    fn concurrent_recv_from_serializes_safely() {
        // The recv_from path on a half is `&self`-safe because the
        // Consumer<Frame> sits behind a Mutex. Two threads racing on
        // the same half must collectively observe each frame exactly
        // once: no double-delivery, no panic, no deadlock.
        //
        // N must fit within the SPSC ring (default 128) since we
        // fire all frames synchronously before the consumers start
        // draining — otherwise the ring fills and frames are
        // dropped, which is correct backpressure but not what this
        // test is checking.
        const N: u32 = 100;
        let endpoint = IoUringEndpoint::bind(loopback(0), test_cfg()).expect("bind");
        let bound = endpoint.local_addr().unwrap();
        let (_send_half, recv_half) = endpoint.split();
        let recv_half = Arc::new(recv_half);

        let peer = UdpSocket::bind(loopback(0)).unwrap();
        for seq in 0..N {
            peer.send_to(&data_frame_with_seq(seq), bound).unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        let collected: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::with_capacity(N as usize)));
        // Shared exit flag so once one thread observes the Nth frame,
        // the other thread doesn't keep spinning to its own deadline.
        let done = Arc::new(AtomicBool::new(false));

        let mut threads = Vec::new();
        for _ in 0..2 {
            let half = Arc::clone(&recv_half);
            let collected = Arc::clone(&collected);
            let done = Arc::clone(&done);
            threads.push(std::thread::spawn(move || {
                let mut buf = [0u8; BUF_SIZE];
                while !done.load(Ordering::Relaxed) && Instant::now() < deadline {
                    if let Some((_, n)) = half.recv_from(&mut buf).expect("recv_from") {
                        let payload = &buf[DataFrame::HEADER_LEN..n];
                        let seq = u32::from_le_bytes(payload.try_into().unwrap());
                        let mut g = collected.lock().unwrap();
                        g.push(seq);
                        if g.len() == N as usize {
                            done.store(true, Ordering::Relaxed);
                            return;
                        }
                    } else {
                        std::thread::sleep(Duration::from_micros(50));
                    }
                }
            }));
        }
        for t in threads {
            t.join().expect("recv thread panicked");
        }

        let mut got = collected.lock().unwrap().clone();
        got.sort_unstable();
        let expected: Vec<u32> = (0..N).collect();
        assert_eq!(got, expected, "every frame seen exactly once");
    }
}
