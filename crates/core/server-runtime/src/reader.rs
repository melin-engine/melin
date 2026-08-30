//! io_uring-based multiplexed reader with multishot RECV.
//!
//! Uses `IORING_OP_RECV` with `IORING_RECV_MULTISHOT` — a single SQE per
//! connection produces multiple CQEs as data arrives, eliminating the
//! resubmission overhead of standard RECV. Combined with a ring-mapped
//! provided-buffer ring (`IOSQE_BUFFER_SELECT` + `buf_ring`), the kernel
//! selects a buffer from a shared pool for each recv, and consumed
//! buffers are recycled with a shared-memory store — no per-recv
//! ProvideBuffers SQE/CQE round trip. Requires kernel ≥ 5.19.
//!
//! Uses a single reader thread — io_uring is efficient enough for hundreds
//! of connections. New connections are registered via eventfd wakeup.
//!
//! Connection state is stored in a slab (index-stable Vec) so that io_uring
//! user_data carries a slab index, not an fd. This avoids fd-reuse races
//! where a recycled fd number could match a stale CQE.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error};

use crate::ControlEvent;
use crate::buf_ring::BufRing;
use melin_app::Application;
use melin_app::auth::Permission;
use melin_app::decoder::RequestDecoder;

/// Decoder type alias: request decoder bound to the application's `Event`
/// type. Mirrors [`crate::response::ResponseEncoderArc`]; hides
/// the `dyn RequestDecoder<Event = …>` spelling at call sites that thread
/// the decoder through several functions.
pub type RequestDecoderArc<A> = Arc<dyn RequestDecoder<Event = <A as Application>::Event>>;
use melin_app::unix_epoch_nanos;
use melin_pipeline::ring;
use melin_transport_core::pipeline::InputSlot;

/// Size of each provided buffer. 4 KiB accommodates multiple frames per
/// recv (frames are typically <100 bytes).
const BUF_SIZE: usize = 4096;

/// Number of provided buffers in the shared pool. Must be a power of two
/// (buf_ring ABI) and large enough for concurrent in-flight recvs across
/// all connections. On exhaustion the kernel completes the multishot
/// with `ENOBUFS` (no data consumed) and the loop re-arms it after the
/// drain's recycles refill the ring. 2048 supports up to ~1024
/// connections per reader thread; recycling is now a shared-memory store
/// (no SQE), so raising this no longer interacts with `RING_SIZE`.
const NUM_BUFFERS: u16 = 2048;

/// Buffer group ID for the provided recv buffer pool.
const BUF_GROUP_ID: u16 = 0;

use crate::client_frames::MAX_FRAME_SIZE;

/// io_uring submission queue depth. Power of 2, sized for up to ~1024
/// connections per reader thread (multishot RECVs + eventfd read; buffer
/// recycling goes through the buf_ring, not the SQ).
const RING_SIZE: u32 = 4096;

/// User data sentinel for the eventfd read SQE.
const EVENTFD_TOKEN: u64 = u64::MAX;

/// User data sentinel for legacy ProvideBuffers CQEs (fallback recycle
/// path only). Best-effort re-provisions: we log errors but don't act
/// on success.
const PROVIDE_BUFS_TOKEN: u64 = u64::MAX - 1;

/// User data sentinel for AsyncCancel SQEs (connection teardown and the
/// shutdown quiesce). The completion carries no actionable information:
/// `ENOENT`/`EALREADY` just mean the target op already finished.
const CANCEL_TOKEN: u64 = u64::MAX - 3;

/// User data sentinel for the tick timeout SQE. The reader arms a single
/// `IORING_OP_TIMEOUT` per cadence so `submit_and_wait` returns at the tick
/// deadline even when no client traffic is flowing. The CQE itself carries
/// no information; the loop body checks `Instant::now()` against the next
/// deadline and emits the actual `JournalEvent::Tick`.
const TICK_TIMEOUT_TOKEN: u64 = u64::MAX - 2;

/// CQE flag: buffer ID is valid in upper 16 bits of flags.
const IORING_CQE_F_BUFFER: u32 = 1 << 0;

/// CQE flag: more completions coming from this multishot operation.
const IORING_CQE_F_MORE: u32 = 1 << 1;

/// Bit shift to extract buffer ID from CQE flags.
const IORING_CQE_BUFFER_SHIFT: u32 = 16;

use melin_wire_protocol::control::ConnectionId;

/// Command sent from the accept loop to a reader thread.
pub struct ReaderRegistration<R> {
    pub connection_id: ConnectionId,
    pub reader: R,
    pub addr: SocketAddr,
    /// Permission level established during the auth handshake.
    pub permission: Permission,
    /// FxHash of the client's Ed25519 public key. Stored per-connection
    /// and copied into every InputSlot for per-key idempotency dedup.
    pub key_hash: u64,
}

/// Handle for the accept loop to register connections with the io_uring reader.
pub struct UringReaderHandle<R> {
    tx: mpsc::Sender<ReaderRegistration<R>>,
    /// Wakeup eventfd, shared with the reader thread.
    ///
    /// `Arc<OwnedFd>` rather than a `RawFd` so the descriptor cannot be
    /// closed while the other side still holds it: the close happens in
    /// `OwnedFd`'s `Drop`, which runs when the last holder goes away, and
    /// no code here is able to call `close` early because none of it owns
    /// a raw descriptor to close.
    ///
    /// The reader thread used to close this itself while this handle was
    /// still writing wakeups to it — a use-after-close whose real danger
    /// was descriptor reuse, not `EBADF`: once the number is free, a
    /// journal segment or an accepted connection can be assigned it, and
    /// the next wakeup writes eight bytes into that instead. Found by
    /// ThreadSanitizer.
    event_fd: Arc<OwnedFd>,
    join_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl<R> UringReaderHandle<R> {
    /// Register a new connection with the reader thread.
    ///
    /// If the reader thread's channel is dead (thread panicked), logs an
    /// error and signals shutdown so the server can restart cleanly.
    pub fn register(&mut self, registration: ReaderRegistration<R>) {
        if self.tx.send(registration).is_ok() {
            // Signal the eventfd to wake the reader from io_uring_enter.
            let val: u64 = 1;
            unsafe {
                libc::write(
                    self.event_fd.as_raw_fd(),
                    &val as *const u64 as *const libc::c_void,
                    8,
                );
            }
        } else {
            error!("reader thread dead, cannot register connection");
            self.shutdown.store(true, Ordering::Relaxed);
        }
    }

    /// Signal the reader thread to shut down and wake it from io_uring_enter.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let val: u64 = 1;
        unsafe {
            libc::write(
                self.event_fd.as_raw_fd(),
                &val as *const u64 as *const libc::c_void,
                8,
            );
        }
    }

    /// Join the reader thread. Call after `shutdown()`.
    pub fn join(mut self) {
        if let Some(handle) = self.join_handle.take()
            && let Err(panic) = handle.join()
        {
            let msg = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
                .unwrap_or("<non-string panic>");
            error!(message = msg, "reader thread panicked");
        }
    }
}

/// Spawn the io_uring reader thread. Returns a handle for registering
/// connections.
///
/// One reader thread serves every TCP connection on the server. io_uring
/// with multishot RECV multiplexes thousands of sockets efficiently and the
/// matching stage is the throughput limit, so adding more reader threads
/// would not raise throughput — it would only re-introduce contention on
/// the input ring's multi-producer cursor.
///
/// `tick_cadence: Some(d)` makes the reader the engine's tick generator: it
/// arms an `IORING_OP_TIMEOUT` so `submit_and_wait` returns at the tick
/// deadline even when no client traffic is flowing, then publishes a
/// `JournalEvent::Tick { now_ns }` onto the same input ring it uses for
/// client requests. Pass `None` to disable the tick (useful for benchmarks
/// that don't exercise time-driven features).
pub fn spawn_reader<A: Application, R: AsRawFd + Send + 'static>(
    producer: ring::Producer<InputSlot<A::Event>>,
    decoder: Arc<dyn RequestDecoder<Event = A::Event>>,
    control_tx: mpsc::Sender<ControlEvent>,
    core: usize,
    connection_timeout: Option<Duration>,
    tick_cadence: Option<Duration>,
    shutdown: Arc<AtomicBool>,
) -> UringReaderHandle<R>
where
    A::Event: Send + Sync + 'static,
{
    let (tx, rx) = mpsc::channel();

    let raw_event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    assert!(raw_event_fd >= 0, "eventfd creation failed");

    // SAFETY: `eventfd` returned a fresh descriptor (asserted non-negative
    // above) that nothing else owns, so transferring ownership to `OwnedFd`
    // is sound. From here the raw number is never closed by hand — the
    // descriptor lives exactly as long as the last `Arc` holder.
    let event_fd = Arc::new(unsafe { OwnedFd::from_raw_fd(raw_event_fd) });
    let wakeup_fd = Arc::clone(&event_fd);
    let shutdown_clone = Arc::clone(&shutdown);

    let handle = std::thread::Builder::new()
        .name("uring-reader".into())
        .spawn(move || {
            // `core == 0` is the "do not pin" sentinel — see
            // `crate::affinity` module docs.
            if core == 0 {
                tracing::info!(thread = "uring-reader", "thread left unpinned (core 0 sentinel)");
            } else {
                match melin_app::affinity::pin_to_core(core) {
                    Ok(c) => {
                        tracing::info!(thread = "uring-reader", core = c, "pinned to core")
                    }
                    Err(e) => tracing::warn!(thread = "uring-reader", core = core, error = %e, "failed to pin"),
                }
            }
            reader_loop::<A, R>(
                rx,
                wakeup_fd,
                producer,
                &*decoder,
                &control_tx,
                connection_timeout,
                tick_cadence,
                &shutdown_clone,
            );
        })
        .expect("failed to spawn uring reader thread");

    UringReaderHandle {
        tx,
        event_fd,
        join_handle: Some(handle),
        shutdown,
    }
}

// ---------------------------------------------------------------------------
// Slab-based connection storage
// ---------------------------------------------------------------------------

/// Per-connection state for multishot io_uring recv + incremental frame parsing.
struct ConnectionEntry<R> {
    connection_id: u64,
    addr: SocketAddr,
    /// Permission level from auth handshake. Checked per-request on
    /// the reader thread (cold path), zero cost on the matching engine.
    permission: Permission,
    /// FxHash of the client's Ed25519 public key. Copied into every
    /// InputSlot for per-key idempotency dedup.
    key_hash: u64,
    /// Owned reader — keeps the fd alive. Dropping closes the fd.
    _reader: R,
    fd: RawFd,
    /// Accumulated bytes not yet parsed into complete frames.
    /// Grows when partial frames arrive, shrinks when frames are consumed.
    parse_buf: Vec<u8>,
    /// True if a multishot RecvMulti is currently active for this connection.
    /// Multishot stays active until the kernel clears IORING_CQE_F_MORE
    /// (e.g., buffer pool exhaustion, socket error, or EOF).
    multishot_active: bool,
    /// Last time any data was received on this connection. Used for
    /// idle timeout detection.
    last_activity: Instant,
    /// Teardown has begun (malformed frame or idle timeout) while the
    /// multishot RECV was still armed. The slab index must stay
    /// allocated until the armed op's terminal CQE arrives — freeing it
    /// early lets the LIFO free list hand the index to a new
    /// registration while the kernel can still post CQEs carrying it,
    /// and the old peer's bytes would be parsed under the new
    /// connection's identity and permissions (audit review F1). CQEs
    /// for a dying entry recycle their buffers and are otherwise
    /// discarded.
    dying: bool,
}

/// Index-stable allocator for connection state. Slab indices are used as
/// io_uring user_data, avoiding fd-reuse races.
struct ConnectionSlab<R> {
    entries: Vec<Option<ConnectionEntry<R>>>,
    /// Recycled indices for O(1) allocation.
    free: Vec<usize>,
}

impl<R> ConnectionSlab<R> {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(256),
            free: Vec::new(),
        }
    }

    /// Insert a connection, returning its stable slab index.
    fn insert(&mut self, entry: ConnectionEntry<R>) -> usize {
        if let Some(idx) = self.free.pop() {
            self.entries[idx] = Some(entry);
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(entry));
            idx
        }
    }

    fn get_mut(&mut self, idx: usize) -> Option<&mut ConnectionEntry<R>> {
        self.entries.get_mut(idx).and_then(|e| e.as_mut())
    }

    /// Remove and return a connection entry, recycling its index.
    fn remove(&mut self, idx: usize) -> Option<ConnectionEntry<R>> {
        if let Some(slot) = self.entries.get_mut(idx) {
            let removed = slot.take();
            if removed.is_some() {
                self.free.push(idx);
            }
            removed
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

/// How the reader should approach the kernel before draining the CQ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RingEntry {
    /// Completions are visible and there is nothing to hand over — drain
    /// from the mmap'd ring without a syscall.
    Skip,
    /// SQEs to submit, but completions are already visible: hand them
    /// over and come straight back rather than asking to wait.
    Submit,
    /// Nothing to process yet — submit whatever is queued and park.
    SubmitAndWait,
}

/// The reader's enter policy. Two rules: never park while completions
/// are already visible, and never skip the syscall while SQEs are
/// waiting to be handed over.
#[inline]
fn ring_entry(sq_pending: usize, cq_ready: bool) -> RingEntry {
    match (sq_pending > 0, cq_ready) {
        (false, true) => RingEntry::Skip,
        (true, true) => RingEntry::Submit,
        (_, false) => RingEntry::SubmitAndWait,
    }
}

/// Main io_uring reader loop. Runs until channel disconnection.
///
/// When `tick_cadence` is `Some`, the loop also generates the engine's
/// scheduler ticks — see [`spawn_reader`] for the rationale.
fn reader_loop<A: Application, R: AsRawFd>(
    command_rx: mpsc::Receiver<ReaderRegistration<R>>,
    // Shared with `UringReaderHandle`. Taken by value so this thread keeps
    // the descriptor alive for as long as it is armed in the ring, and
    // releases it by dropping the `Arc` rather than by closing the fd.
    wakeup_fd: Arc<OwnedFd>,
    mut producer: ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn RequestDecoder<Event = A::Event>,
    control_tx: &mpsc::Sender<ControlEvent>,
    connection_timeout: Option<Duration>,
    tick_cadence: Option<Duration>,
    shutdown: &AtomicBool,
) {
    // Kernel-referenced memory is declared BEFORE the io_uring so it
    // drops AFTER the ring on every exit path, including panic unwind
    // (locals drop in reverse declaration order). The kernel holds live
    // references into all three for as long as the ring fd is open:
    // armed multishot RECVs select entries from the buf_ring and write
    // into the pool, and the armed eventfd READ writes its buffer.

    // Eventfd read buffer — boxed for pointer stability across SQE lifetimes.
    let mut eventfd_buf: Box<[u8; 8]> = Box::new([0u8; 8]);

    // Shared buffer pool for provided buffers. Contiguous allocation of
    // NUM_BUFFERS × BUF_SIZE bytes. The kernel selects a buffer from this
    // pool for each recv completion, identified by buffer ID in the CQE.
    let mut buffer_pool = vec![0u8; NUM_BUFFERS as usize * BUF_SIZE].into_boxed_slice();

    // Ring-mapped provided-buffer ring: recycling a consumed buffer is a
    // shared-memory store, not a ProvideBuffers SQE — see `buf_ring`.
    // Allocated unconditionally (32 KiB) so its declaration precedes the
    // io_uring's even when the legacy fallback below ends up in use.
    let mut buf_ring = BufRing::new(NUM_BUFFERS, buffer_pool.as_mut_ptr(), BUF_SIZE);

    // SINGLE_ISSUER: this thread creates the ring and is the only one
    // that ever submits — lets the kernel skip SQ locking, and turns any
    // future cross-thread submission bug into an immediate EEXIST
    // instead of a silent race. Same rationale as the journal and
    // replication rings. (COOP_TASKRUN/DEFER_TASKRUN deliberately not
    // set — see the journal ring's measured rationale in
    // melin-transport-core::pipeline.)
    let mut ring: IoUring = IoUring::builder()
        .setup_single_issuer()
        .build(RING_SIZE)
        .expect("failed to create io_uring instance");

    // Prefer the buf_ring; fall back to legacy ProvideBuffers SQEs if
    // the kernel rejects the registration. Not theoretical: PBUF_RING
    // needs kernel ≥ 5.19, and some virtualized hosts filter newer
    // io_uring register opcodes (EINVAL) while reporting a modern
    // uname. The fallback costs one SQE + one CQE per received chunk —
    // degraded but fully functional, hence warn (see log conventions).
    let use_buf_ring = match buf_ring.register(&ring, BUF_GROUP_ID) {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "buf_ring registration rejected — falling back to legacy \
                 ProvideBuffers recycling (kernel < 5.19, or a hypervisor \
                 filtering io_uring register opcodes)"
            );
            register_buffer_pool(&mut ring, buffer_pool.as_mut_ptr());
            false
        }
    };

    let mut slab = ConnectionSlab::<R>::new();
    // Reverse map for cleanup when a connection's fd needs removal.
    // HashMap for O(1) lookup by fd. Sized for typical connection counts.
    let mut fd_to_slab: HashMap<RawFd, usize> = HashMap::with_capacity(256);

    // Pre-allocated CQE collection buffer. We must collect CQEs before
    // processing because the CQ borrow must end before pushing new SQEs.
    // Stores (user_data, result, flags) — flags needed for buffer ID and
    // multishot continuation. Sized to the CQ depth (2× the SQ) so even
    // a maximal drain never reallocates mid-loop.
    let mut cqes: Vec<(u64, i32, u32)> = Vec::with_capacity(RING_SIZE as usize * 2);

    // Submit the initial eventfd read so we wake on first connection.
    // `eventfd_armed` mirrors whether that READ is pushed/armed — the
    // kernel owes its CQE and writes `eventfd_buf` when it lands — and
    // is maintained at every push/consume site so the teardown quiesce
    // below can prove when the buffer is free of kernel references.
    push_eventfd_read(&mut ring, wakeup_fd.as_raw_fd(), eventfd_buf.as_mut_ptr());
    let mut eventfd_armed = true;

    // Stage histograms via the global registry. `publish` is the
    // narrow ring-publish call cost (lightweight, gated on
    // `latency-trace`); `ingest` is the full per-frame reader cost
    // and feeds the bench's tick-to-trade decomposition (heavier,
    // gated on `tick-to-trade`).
    #[cfg(feature = "latency-trace")]
    let mut publish_rec =
        melin_transport_core::trace::register_stage("reader: publish (decode → disruptor publish)");
    #[cfg(feature = "tick-to-trade")]
    let mut ingest_rec =
        melin_transport_core::trace::register_stage("reader: ingest (recv_ts → publish complete)");
    // Paces the recorder flush at the tail of each loop iteration. This
    // thread parks in `submit_and_wait` when there is no traffic, so
    // without an explicit flush its samples never reach the registry.
    #[cfg(feature = "latency-trace")]
    let mut last_stats_flush = Instant::now();

    // Coarse gate for timeout scanning — avoids scanning on every
    // submit_and_wait return during high throughput.
    let mut last_timeout_scan = Instant::now();
    // Pre-allocated buffer for stale connection indices to avoid
    // heap allocation inside the hot loop.
    let mut stale: Vec<usize> = Vec::new();

    // Tick generator state. `next_tick_deadline` is the monotonic instant the
    // next `JournalEvent::Tick` should fire. `last_tick_ns` enforces strict
    // monotonicity on the wall-clock timestamps published in those events
    // (NTP can step the wall clock backwards). `tick_armed` tracks whether
    // an `IORING_OP_TIMEOUT` SQE is currently pending; we keep at most one.
    //
    // `tick_ts` lives across loop iterations because the kernel reads its
    // bytes via the SQE's addr field at submit time, not at push time. If
    // we declared it inside the `if !tick_armed` arm-timeout block, the
    // value would be dropped before the `submit_and_wait` below — the
    // kernel would then read freed stack memory. (See `md-gateway` for the
    // same pattern: it stores Timespec as a long-lived struct field.)
    let tick_enabled = tick_cadence.is_some();
    let cadence = tick_cadence.unwrap_or(Duration::ZERO);
    let mut next_tick_deadline = Instant::now() + cadence;
    let mut last_tick_ns: u64 = 0;
    let mut tick_armed = false;
    // Arm the very first timeout here, before entering the loop. This both
    // (a) makes the initial `tick_ts` value actually read by the kernel
    // (silencing the unused-assignment lint, since rustc cannot see kernel
    // pointer reads) and (b) ensures the first `submit_and_wait` returns at
    // the cadence even if no client traffic ever arrives.
    let mut tick_ts = types::Timespec::new()
        .sec(cadence.as_secs())
        .nsec(cadence.subsec_nanos());
    if tick_enabled {
        let sqe = opcode::Timeout::new(&tick_ts)
            .build()
            .user_data(TICK_TIMEOUT_TOKEN);
        unsafe {
            ring.submission()
                .push(&sqe)
                .expect("io_uring SQ full while arming initial tick timeout");
        }
        tick_armed = true;
        tracing::info!(
            cadence_ms = cadence.as_millis() as u64,
            "tick generator integrated into reader thread"
        );
    }

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Tick generator: emit any due tick before sleeping again. Done before
        // the timeout-arm so that a freshly-emitted tick re-arms a timeout for
        // the *new* deadline.
        if tick_enabled {
            let now = Instant::now();
            if now >= next_tick_deadline {
                let raw_now_ns = unix_epoch_nanos();
                let now_ns = melin_transport_core::tick::clamp_monotonic(raw_now_ns, last_tick_ns);
                last_tick_ns = now_ns;
                melin_transport_core::tick::publish_tick(&mut producer, now_ns);
                // Catch up rather than burst-emit if we fell badly behind.
                let elapsed = Instant::now().saturating_duration_since(next_tick_deadline);
                next_tick_deadline = if elapsed > cadence {
                    Instant::now() + cadence
                } else {
                    next_tick_deadline + cadence
                };
                // The previous timeout (if any) is now stale; let it fire and
                // be ignored, then arm a new one below.
                tick_armed = false;
            }

            if !tick_armed {
                let remaining = next_tick_deadline.saturating_duration_since(Instant::now());
                // Update the loop-scoped Timespec in place. The kernel reads
                // it via the SQE's addr pointer on submit_and_wait below, so
                // the binding must outlive that call (it does — outer scope).
                tick_ts = types::Timespec::new()
                    .sec(remaining.as_secs())
                    .nsec(remaining.subsec_nanos());
                let sqe = opcode::Timeout::new(&tick_ts)
                    .build()
                    .user_data(TICK_TIMEOUT_TOKEN);
                unsafe {
                    ring.submission()
                        .push(&sqe)
                        .expect("io_uring SQ full while arming tick timeout");
                }
                tick_armed = true;
            }
        }

        // Enter the kernel only when there is a reason to.
        //
        // Under load the CQ usually already holds completions by the
        // time we get here: in the ring's default task-work mode the
        // kernel posts them while this thread is in userspace parsing.
        // Reading that is pure userspace — the CQ is mmap'd and
        // `completion()` loads the tail with `Acquire` — so when there
        // is also nothing to hand over, the `io_uring_enter` is a
        // ~200 ns mode switch that buys nothing. The replication sender
        // measured the same skip at ~6 % of its thread's cycles
        // (`tcp_sender.rs`); this loop makes one enter per drain.
        //
        // When there ARE SQEs to submit we still enter, but we do not
        // ask the kernel to *wait* if completions are already visible.
        // Only a genuinely empty CQ parks the thread.
        //
        // Note this is the half of the io_uring-audit item that pairs
        // *against* `DEFER_TASKRUN`: deferred task work only runs on an
        // enter with GETEVENTS, so under that flag the CQ would be empty
        // here and the skip would simply never fire.
        let sq_pending = ring.submission().len();
        let cq_ready = !ring.completion().is_empty();
        let entered = match ring_entry(sq_pending, cq_ready) {
            RingEntry::Skip => Ok(0),
            RingEntry::Submit => ring.submit(),
            RingEntry::SubmitAndWait => ring.submit_and_wait(1),
        };
        match entered {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
                error!(error = %e, "io_uring submit/wait error");
                break;
            }
        }

        // Drain all available CQEs into the pre-allocated buffer.
        // Must collect before processing because the CQ borrow must end
        // before we can push new SQEs to the SQ.
        cqes.clear();
        cqes.extend(
            ring.completion()
                .map(|cqe| (cqe.user_data(), cqe.result(), cqe.flags())),
        );

        let batch_now = Instant::now();
        // One wall-clock read per CQE batch instead of per request. The
        // reader can see 4–6 M requests/s at peak; a per-request
        // `unix_epoch_nanos()` was ~2.8 % of the primary's cycles
        // (vDSO `clock_gettime(CLOCK_REALTIME)`). All requests in the
        // same batch share the timestamp — precision loss is bounded
        // by the CQE-drain cadence (tens of µs under load) and order
        // timestamps are used for reporting, not matching (the engine
        // orders by sequence, not time).
        let batch_wall_ns = unix_epoch_nanos();

        for &(token, result, flags) in &cqes {
            // ── Tick timeout ──
            // The CQE is just a wakeup signal — the actual tick emission
            // happens at the top of the next loop iteration via the
            // deadline check, so the time the tick is stamped with reflects
            // unix_epoch_nanos at fire time, not at submit time.
            if token == TICK_TIMEOUT_TOKEN {
                tick_armed = false;
                continue;
            }

            // ── Legacy ProvideBuffers completion (fallback mode only) ──
            if token == PROVIDE_BUFS_TOKEN {
                if result < 0 {
                    error!(error = result, "ProvideBuffers failed");
                }
                continue;
            }

            // ── AsyncCancel completion ──
            if token == CANCEL_TOKEN {
                // Nothing to do: the cancelled op's own terminal CQE
                // drives the state machine; ENOENT/EALREADY just mean
                // it already finished on its own.
                continue;
            }

            // ── Eventfd wakeup ──
            if token == EVENTFD_TOKEN {
                // This CQE is the armed READ completing; the re-arm at
                // the end of the branch replaces it before anything can
                // observe the gap, so `eventfd_armed` stays `true`
                // through the pair. The teardown drain clears it for
                // the one case where a completion is never processed
                // here (a CQE still queued when the loop breaks).
                if result >= 0 {
                    // Process all pending registrations.
                    while let Ok(reg) = command_rx.try_recv() {
                        let fd = reg.reader.as_raw_fd();
                        let entry = ConnectionEntry {
                            connection_id: reg.connection_id.0,
                            addr: reg.addr,
                            permission: reg.permission,
                            key_hash: reg.key_hash,
                            fd,
                            _reader: reg.reader,
                            parse_buf: Vec::with_capacity(MAX_FRAME_SIZE + 4),
                            multishot_active: false,
                            last_activity: Instant::now(),
                            dying: false,
                        };
                        let idx = slab.insert(entry);
                        fd_to_slab.insert(fd, idx);

                        // Submit multishot RECV for this connection.
                        push_recv_multi(&mut ring, &mut slab, idx);
                    }
                } else {
                    error!(error = result, "eventfd read error");
                }

                // Re-submit eventfd read for the next wakeup
                // (`eventfd_armed` stays `true` — see above).
                push_eventfd_read(&mut ring, wakeup_fd.as_raw_fd(), eventfd_buf.as_mut_ptr());
                continue;
            }

            // ── Connection multishot RECV completion ──

            let slab_idx = token as usize;
            let has_more = (flags & IORING_CQE_F_MORE) != 0;

            // A cleared F_MORE means THIS armed op posts no further
            // CQEs — record it before any branch, so no early `continue`
            // below can wedge the connection with a stale `true`.
            if !has_more && let Some(entry) = slab.get_mut(slab_idx) {
                entry.multishot_active = false;
            }

            // Extract the buffer (if any) before acting on `result`: a
            // buffer can ride ANY recv CQE, and one that is not recycled
            // is leaked from the pool forever. On 6.8 the kernel
            // recycles internally before posting error/EOF CQEs, but the
            // supported floor is 5.19, where that guarantee could not be
            // established — the defensive recycle is one branch.
            let buf_id = if (flags & IORING_CQE_F_BUFFER) != 0 {
                Some((flags >> IORING_CQE_BUFFER_SHIFT) as usize)
            } else {
                None
            };

            // Dying connection: teardown began while its multishot was
            // still armed (see `begin_teardown`). Consume its CQEs
            // without parsing — the bytes belong to a repudiated peer —
            // but keep recycling their buffers; free the slab index only
            // at the terminal CQE, so it cannot be handed to a new
            // registration while the kernel can still post CQEs carrying
            // it. Checked before the disconnect branch so the cancel's
            // -ECANCELED completion lands here, not there (which would
            // emit a second Disconnected event).
            if slab.get_mut(slab_idx).is_some_and(|e| e.dying) {
                if let Some(bid) = buf_id {
                    recycle_buffer(&mut ring, &mut buf_ring, use_buf_ring, &buffer_pool, bid);
                }
                if !has_more && let Some(dead) = slab.remove(slab_idx) {
                    debug!(
                        connection_id = dead.connection_id,
                        "teardown complete, slab index released"
                    );
                }
                continue;
            }

            if result == -libc::ENOBUFS {
                // Pool exhausted at buffer-selection time: the multishot
                // terminated WITHOUT reading — no data was lost, it sits
                // in the socket buffer. This is not a client error and
                // must not disconnect (a bare `result <= 0 ⇒ remove`
                // here dropped innocent clients under burst). Re-arm:
                // recycles from data CQEs earlier in this drain have
                // already refilled the buf_ring (publication is
                // immediate), and the re-arm SQE submits after the whole
                // drain, so the retry finds buffers.
                if let Some(bid) = buf_id {
                    // No buffer accompanies a selection failure by
                    // definition; recycle defensively if one appears.
                    recycle_buffer(&mut ring, &mut buf_ring, use_buf_ring, &buffer_pool, bid);
                }
                if has_more {
                    // Future-kernel guard: should the kernel ever keep
                    // the multishot alive across ENOBUFS, arming a second
                    // one would interleave two delivery streams into the
                    // same parse buffer.
                    continue;
                }
                push_recv_multi(&mut ring, &mut slab, slab_idx);
                continue;
            }

            if result <= 0 {
                // Disconnect (0) or error (negative errno) — terminal
                // for the armed op, so the index is immediately safe to
                // free. Recycle any buffer riding the CQE first.
                if let Some(bid) = buf_id {
                    recycle_buffer(&mut ring, &mut buf_ring, use_buf_ring, &buffer_pool, bid);
                }
                if let Some(removed) = slab.remove(slab_idx) {
                    if result == 0 {
                        debug!(
                            connection_id = removed.connection_id,
                            addr = %removed.addr,
                            "client disconnected"
                        );
                    } else {
                        debug!(
                            connection_id = removed.connection_id,
                            addr = %removed.addr,
                            error = result,
                            "recv error"
                        );
                    }
                    fd_to_slab.remove(&removed.fd);
                    let _ = control_tx.send(ControlEvent::Disconnected {
                        connection_id: removed.connection_id,
                    });
                }
                continue;
            }

            let n = result as usize;

            // Trace timestamp: the moment the kernel handed us this recv's
            // bytes. Captured once per CQE — not per frame — and stamped
            // onto every InputSlot parsed from this buffer below, so the
            // reader-ingest and server-e2e stages measure from true wire
            // receipt (frame decode included) rather than re-sampling after
            // each decode (which excluded decode and drifted forward for
            // later frames in a multi-frame recv).
            #[allow(clippy::let_unit_value)] // ZST when latency-trace is off
            let recv_ts = melin_transport_core::trace::mono_trace_ns();

            // Data CQEs always carry a buffer with buffer-select recvs.
            // Defensive: nothing to copy or recycle without one, but the
            // multishot bookkeeping already ran above, so a terminal
            // no-buffer CQE re-arms instead of wedging the connection.
            let Some(buf_id) = buf_id else {
                debug!(slab_idx, "recv CQE without buffer flag");
                if !has_more {
                    push_recv_multi(&mut ring, &mut slab, slab_idx);
                }
                continue;
            };

            // Feed received bytes into the frame parser from the shared pool.
            let action = if let Some(entry) = slab.get_mut(slab_idx) {
                // Any successful recv resets the idle timeout.
                entry.last_activity = batch_now;

                // Copy received data from the shared buffer pool into the
                // connection's parse buffer.
                let buf_start = buf_id * BUF_SIZE;
                entry
                    .parse_buf
                    .extend_from_slice(&buffer_pool[buf_start..buf_start + n]);

                // Extract and publish complete frames.
                let drop_conn = process_frames::<A, R>(
                    entry,
                    &mut producer,
                    decoder,
                    control_tx,
                    batch_wall_ns,
                    recv_ts,
                    #[cfg(feature = "latency-trace")]
                    &mut publish_rec,
                    #[cfg(feature = "tick-to-trade")]
                    &mut ingest_rec,
                );
                if drop_conn {
                    Action::Remove
                } else if !has_more {
                    // Multishot terminated (buffer pool exhaustion or kernel
                    // decision) but connection is healthy — resubmit.
                    Action::Resubmit
                } else {
                    Action::None
                }
            } else {
                // Stale CQE for a removed connection — ignore.
                Action::None
            };

            // Recycle the consumed buffer. Must happen after the copy-out
            // above — from this line on the kernel may write fresh recv
            // data into the slot.
            recycle_buffer(&mut ring, &mut buf_ring, use_buf_ring, &buffer_pool, buf_id);

            match action {
                Action::Remove => {
                    // Deferred teardown, NOT an immediate slab free — the
                    // multishot may still be armed and its index must not
                    // be reused until the terminal CQE (audit review F1).
                    begin_teardown(&mut ring, &mut slab, &mut fd_to_slab, control_tx, slab_idx);
                }
                Action::Resubmit => {
                    push_recv_multi(&mut ring, &mut slab, slab_idx);
                }
                Action::None => {}
            }
        }

        // Scan for idle connections that have exceeded the timeout.
        // Coarse gate: only scan once per second to avoid unnecessary
        // iteration during high-throughput phases when submit_and_wait
        // returns immediately with CQEs.
        if let Some(timeout) = connection_timeout {
            let now = Instant::now();
            if now.duration_since(last_timeout_scan) >= Duration::from_secs(1) {
                last_timeout_scan = now;
                stale.clear();
                for (idx, slot) in slab.entries.iter().enumerate() {
                    // Dying entries are already mid-teardown: their
                    // `last_activity` stays stale by design, and a second
                    // teardown would emit a duplicate Disconnected.
                    if let Some(entry) = slot
                        && !entry.dying
                        && now.duration_since(entry.last_activity) > timeout
                    {
                        debug!(
                            connection_id = entry.connection_id,
                            addr = %entry.addr,
                            "connection timed out"
                        );
                        stale.push(idx);
                    }
                }
                for &idx in &stale {
                    begin_teardown(&mut ring, &mut slab, &mut fd_to_slab, control_tx, idx);
                }
            }
        }

        // Hand buffered latency samples to the stats registry before
        // parking in `submit_and_wait` at the top of the next
        // iteration — once traffic stops this thread can sleep in the
        // kernel indefinitely, which is precisely when the bench
        // scrapes /stats-dump. Reuses the per-batch clock read.
        #[cfg(feature = "latency-trace")]
        if batch_now.duration_since(last_stats_flush)
            >= melin_transport_core::trace::IDLE_FLUSH_INTERVAL
        {
            last_stats_flush = batch_now;
            publish_rec.flush();
            #[cfg(feature = "tick-to-trade")]
            ingest_rec.flush();
        }
    }

    // ── Teardown quiesce ────────────────────────────────────────────
    // Armed operations hold kernel pointers into the buffer pool, the
    // buf_ring memory, and the eventfd buffer. Closing the ring fd only
    // triggers ASYNCHRONOUS cancellation (the kernel's ring-exit work),
    // which can still be touching those allocations after this function
    // returns and frees them — the same corruption class fixed on both
    // replication transports (see `crate::uring_teardown`). Quiesce
    // *provably*: wake every armed operation, then reap CQEs until none
    // is owed, and leak the buffers if that cannot be shown within the
    // deadline. "CQ went quiet" is not proof — a wake that silently
    // failed leaves armed ops and a quiet CQ.
    //
    // The wake cannot rely on the cancel opcode alone: some hosts
    // filter io_uring opcodes (the reason the buf_ring path has a
    // legacy fallback), and a filtered cancel completes with EINVAL
    // while the armed operations live on. So every operation that has
    // to be proven down gets a wake it must answer: `shutdown(2)` on
    // each connection socket completes its multishot RECV with EOF,
    // and a wakeup write completes the armed eventfd READ. The cancel
    // is still pushed as an accelerator where the host honours it. The
    // tick timeout is not in the proof at all — see `owed` below.
    // Panic unwind skips all of this and accepts the (tiny,
    // process-is-dying) exit-work window; declaration order still
    // closes the ring fd before the frees.
    let proven = {
        let cancel_all = opcode::AsyncCancel2::new(types::CancelBuilder::any())
            .build()
            .user_data(CANCEL_TOKEN);
        unsafe {
            // Ignore a full SQ: the wakes below do not depend on it.
            let _ = ring.submission().push(&cancel_all);
        }
        for entry in slab.entries.iter().flatten() {
            crate::uring_teardown::wake_pending_ops(entry.fd);
        }
        // Completes the armed eventfd READ into `eventfd_buf` (still
        // alive). Best-effort: on failure the cancel is the fallback.
        let wake: u64 = 1;
        unsafe {
            libc::write(
                wakeup_fd.as_raw_fd(),
                &wake as *const u64 as *const libc::c_void,
                8,
            );
        }
        // Flush SQEs pushed but never submitted (re-arms from the last
        // batch, the cancel above): until they reach the kernel no CQE
        // is coming for them, but their armed-state flags are already
        // set, so abandoning them in the SQ would stall the proof
        // below. On a submit error the flags stay set and the deadline
        // routes to the leak path — the conservative outcome.
        loop {
            match ring.submit() {
                Ok(_) => break,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(_) => break,
            }
        }
        let mut backoff = crate::uring_teardown::DrainBackoff::new();
        loop {
            for cqe in ring.completion() {
                match cqe.user_data() {
                    EVENTFD_TOKEN => eventfd_armed = false,
                    // None of these touch the allocations freed below,
                    // so reaping them settles nothing: cancel results
                    // carry no information, legacy ProvideBuffers
                    // completions only registered addresses, and the
                    // tick timeout reads nothing after submit (see
                    // `owed`).
                    PROVIDE_BUFS_TOKEN | CANCEL_TOKEN | TICK_TIMEOUT_TOKEN => {}
                    // Connection tokens are slab indices.
                    idx => {
                        // A terminal CQE retires that connection's arm.
                        // Data CQEs landing mid-teardown are discarded
                        // and their buffers deliberately not recycled —
                        // nothing re-arms, so the pool cannot be
                        // selected from again once every arm is down.
                        if (cqe.flags() & IORING_CQE_F_MORE) == 0
                            && let Some(entry) = slab.get_mut(idx as usize)
                        {
                            entry.multishot_active = false;
                        }
                    }
                }
            }
            // `tick_armed` is deliberately absent: an armed
            // `IORING_OP_TIMEOUT` holds no pointer into any of the
            // allocations below. The kernel copies its `Timespec` out
            // of `tick_ts` when the SQE is *submitted* (which is why
            // that binding only has to outlive the submit — see its
            // declaration), and the ring's own exit work retires the
            // timer. Waiting for it would prove nothing and cost
            // plenty: nothing here can wake a timeout, so the only
            // things that retire one are it firing at its own cadence
            // — `--tick-interval-ms`, operator-set and unbounded — or
            // the cancel above, which is exactly what a host that
            // filters opcodes refuses. Gating the proof on it would
            // hand those hosts a full-deadline stall and a spurious
            // "leaking" warning on every clean shutdown.
            let owed = eventfd_armed || slab.entries.iter().flatten().any(|e| e.multishot_active);
            if !owed {
                break true;
            }
            if !backoff.wait() {
                break false;
            }
        }
    };
    if !proven {
        // The kernel may still write the pool or the eventfd buffer, or
        // read buf_ring entries. Leak all three rather than hand the
        // allocator memory the kernel still touches — a bounded one-off
        // cost on a thread that is exiting anyway. `mem::forget` on the
        // `BufRing` also skips its ring-memory dealloc, which is the
        // point.
        // Report what was still owed, not `tick_armed` — naming a
        // timer that was never part of the proof would point the next
        // reader at the wrong operation.
        tracing::warn!(
            eventfd_armed,
            armed_connections = slab
                .entries
                .iter()
                .flatten()
                .filter(|e| e.multishot_active)
                .count(),
            "io_uring teardown drain did not complete; leaking reader buffers"
        );
        std::mem::forget(buffer_pool);
        std::mem::forget(buf_ring);
        std::mem::forget(eventfd_buf);
    }

    // No `close` here. This thread shares the eventfd with
    // `UringReaderHandle`, which writes wakeups to it, and closing it from
    // this side left that write racing a reused descriptor number. Dropping
    // the `Arc` at the end of this scope releases our share; the descriptor
    // itself closes when the handle drops too.
}

/// What to do after processing a RECV CQE.
enum Action {
    /// Multishot terminated but connection healthy — resubmit RecvMulti.
    Resubmit,
    /// Connection should be torn down (malformed frame). Handled via
    /// `begin_teardown`, which reads what it needs from the slab entry.
    Remove,
    /// Multishot still active — nothing to do.
    None,
}

// ---------------------------------------------------------------------------
// SQE helpers
// ---------------------------------------------------------------------------

/// Register the provided buffer pool via a legacy ProvideBuffers op —
/// the fallback when buf_ring registration is rejected. Submits
/// synchronously and panics on failure — called once at startup, and
/// only after the preferred path already failed.
fn register_buffer_pool(ring: &mut IoUring, pool_ptr: *mut u8) {
    let sqe = opcode::ProvideBuffers::new(pool_ptr, BUF_SIZE as i32, NUM_BUFFERS, BUF_GROUP_ID, 0)
        .build()
        .user_data(PROVIDE_BUFS_TOKEN);

    unsafe {
        ring.submission()
            .push(&sqe)
            .expect("io_uring SQ full during buffer pool registration");
    }

    ring.submit_and_wait(1)
        .expect("io_uring submit failed during buffer pool registration");

    // Check the completion result.
    let cqe = ring
        .completion()
        .next()
        .expect("no CQE after ProvideBuffers");
    assert!(cqe.result() >= 0, "ProvideBuffers failed: {}", cqe.result());
}

/// Re-provide a single consumed buffer back to the pool (legacy fallback
/// mode). Pushed to SQ without immediate submission — batched with the
/// next submit_and_wait. Safe against SQ overflow only because at most
/// `NUM_BUFFERS` (< `RING_SIZE`) recycles can accumulate per drain.
fn re_provide_buffer(ring: &mut IoUring, pool_ptr: *mut u8, buf_id: usize) {
    let buf_ptr = unsafe { pool_ptr.add(buf_id * BUF_SIZE) };
    let sqe = opcode::ProvideBuffers::new(buf_ptr, BUF_SIZE as i32, 1, BUF_GROUP_ID, buf_id as u16)
        .build()
        .user_data(PROVIDE_BUFS_TOKEN);

    unsafe {
        ring.submission()
            .push(&sqe)
            .expect("io_uring SQ full — increase RING_SIZE");
    }
}

/// Return a consumed buffer to the shared pool, in whichever recycle
/// mode registration selected.
///
/// Hard assert, not debug: the bid comes from a kernel CQE, this path
/// runs per CQE (off the per-frame budget), and an out-of-range bid
/// handed back to the kernel would point a future recv's DMA at
/// arbitrary heap — the one failure mode worth an unconditional branch.
fn recycle_buffer(
    ring: &mut IoUring,
    buf_ring: &mut BufRing,
    use_buf_ring: bool,
    buffer_pool: &[u8],
    buf_id: usize,
) {
    assert!(
        buf_id < NUM_BUFFERS as usize,
        "kernel returned out-of-pool buffer id {buf_id}"
    );
    if use_buf_ring {
        buf_ring.push(buf_id as u16);
    } else {
        re_provide_buffer(ring, buffer_pool.as_ptr() as *mut u8, buf_id);
    }
}

/// Begin tearing down a connection the reader decided to drop while its
/// multishot RECV may still be armed (malformed frame, idle timeout).
///
/// The slab index must NOT be freed yet: the kernel can still post CQEs
/// carrying it, and the slab's LIFO free list would hand the index to
/// the next registration — the old peer's bytes would then be parsed
/// under the new connection's identity, key hash, and permissions
/// (order-flow injection; audit review F1). Instead: sever the peer,
/// mark the entry dying, cancel the armed op, and let the terminal CQE
/// free the index (the `dying` branch of the CQE loop). When no op is
/// armed there is nothing that can post — free immediately.
fn begin_teardown<R>(
    ring: &mut IoUring,
    slab: &mut ConnectionSlab<R>,
    fd_to_slab: &mut HashMap<RawFd, usize>,
    control_tx: &mpsc::Sender<ControlEvent>,
    idx: usize,
) {
    let Some(entry) = slab.get_mut(idx) else {
        return;
    };
    if entry.dying {
        return;
    }
    let connection_id = entry.connection_id;
    let fd = entry.fd;
    // Sever the peer now. The fd itself must stay open until the armed
    // op completes (the kernel holds a file reference for it anyway);
    // shutdown(2) stops both directions immediately. Best-effort:
    // ENOTCONN just means the peer already went away.
    unsafe {
        libc::shutdown(fd, libc::SHUT_RDWR);
    }
    fd_to_slab.remove(&fd);
    // Dropped error: a dead control channel means the response stage is
    // gone and the server is shutting down.
    let _ = control_tx.send(ControlEvent::Disconnected { connection_id });

    if entry.multishot_active {
        entry.dying = true;
        let sqe = opcode::AsyncCancel::new(idx as u64)
            .build()
            .user_data(CANCEL_TOKEN);
        unsafe {
            ring.submission()
                .push(&sqe)
                .expect("io_uring SQ full — increase RING_SIZE");
        }
    } else {
        // No armed op ⇒ no future CQEs can carry this index.
        slab.remove(idx);
    }
}

/// Push a multishot RECV SQE for a connection. The kernel will produce
/// CQEs continuously until EOF, error, or buffer pool exhaustion —
/// no resubmission needed unless multishot terminates.
fn push_recv_multi<R>(ring: &mut IoUring, slab: &mut ConnectionSlab<R>, idx: usize) {
    let entry = match slab.get_mut(idx) {
        Some(e) => e,
        None => return,
    };

    if entry.multishot_active {
        return;
    }

    let sqe = opcode::RecvMulti::new(types::Fd(entry.fd), BUF_GROUP_ID)
        .build()
        .user_data(idx as u64);

    unsafe {
        ring.submission()
            .push(&sqe)
            .expect("io_uring SQ full — increase RING_SIZE");
    }
    entry.multishot_active = true;
}

/// Push a READ SQE for the eventfd (wakeup notification).
fn push_eventfd_read(ring: &mut IoUring, wakeup_fd: RawFd, buf: *mut u8) {
    let sqe = opcode::Read::new(types::Fd(wakeup_fd), buf, 8)
        .build()
        .user_data(EVENTFD_TOKEN);

    unsafe {
        ring.submission()
            .push(&sqe)
            .expect("io_uring SQ full — increase RING_SIZE");
    }
}

// ---------------------------------------------------------------------------
// Frame parsing
// ---------------------------------------------------------------------------

/// Extract complete frames from the connection's parse buffer, decode them,
/// and publish to the disruptor. Returns `true` if the connection should be
/// dropped (e.g., oversized frame).
/// Extract complete frames from `conn.parse_buf` and publish them as
/// `InputSlot`s. `batch_wall_ns` is the wall-clock timestamp captured
/// once per CQE batch by the caller (see `reader_loop`); all non-query
/// requests published in this call share it, sparing the reader a
/// per-request `clock_gettime(CLOCK_REALTIME)` on the hot path. Returns
/// `true` if the connection should be dropped.
fn process_frames<A: Application, R>(
    conn: &mut ConnectionEntry<R>,
    producer: &mut ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn RequestDecoder<Event = A::Event>,
    control_tx: &mpsc::Sender<ControlEvent>,
    batch_wall_ns: u64,
    recv_ts: melin_transport_core::trace::MonoTraceInstant,
    #[cfg(feature = "latency-trace")] publish_rec: &mut melin_transport_core::trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) -> bool {
    use crate::client_frames::{FrameAction, process_client_frames};

    let action = process_client_frames(
        &mut conn.parse_buf,
        conn.connection_id,
        conn.key_hash,
        conn.permission,
        producer,
        decoder,
        batch_wall_ns,
        recv_ts,
        usize::MAX,
        #[cfg(feature = "latency-trace")]
        publish_rec,
        #[cfg(feature = "tick-to-trade")]
        ingest_rec,
    );

    match action {
        FrameAction::Continue => false,
        FrameAction::Disconnect => true,
        FrameAction::PipelineFull => {
            debug!(
                connection_id = conn.connection_id,
                "pipeline full, routing ServerBusy via response stage"
            );
            // The response stage owns ALL egress on a client socket. A
            // reader-side send here — however non-blocking — races the
            // response stage's own writes and can land between the two
            // halves of a partially-flushed response frame, permanently
            // desyncing the client's length-prefix framing (audit review
            // F3). It also kept a client-socket syscall on the ingress
            // thread. Routing through the control channel makes the
            // notice an ordinary send_buf append over there.
            //
            // Error deliberately dropped: a dead control channel means
            // the response stage is gone and the server is shutting
            // down — same reasoning as the Disconnected sends below.
            let _ = control_tx.send(ControlEvent::PipelineBusy {
                connection_id: conn.connection_id,
            });
            false
        }
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for [`process_frames`]. The function has four exit paths
    //! (normal end, partial parse-buf, pipeline-full, oversize-frame), each
    //! with subtle batch-commit ordering requirements. These tests pin that
    //! behaviour against a synthetic decoder so refactors of the batch path
    //! (e.g. moving the batch up to span the whole CQE drain) can't silently
    //! regress the "earlier frames must be visible before ServerBusy /
    //! disconnect" guarantees.
    use super::*;
    use melin_app::auth::Permission;
    use melin_app::decoder::{Decoded, RequestDecoder};
    use melin_app::{AppEvent, Application, ApplyCtx, CodecError, RejectReason};
    use melin_journal::JournalEvent;
    use melin_pipeline::ring::DisruptorBuilder;
    use std::io::{ErrorKind, Read};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    /// The reader's enter policy. Each case is a syscall-per-drain
    /// decision on the hot path, and getting one wrong is either a lost
    /// wakeup (skipping with an empty CQ) or unsubmitted SQEs (skipping
    /// with a non-empty SQ) — both silent.
    mod ring_entry {
        use super::super::{RingEntry, ring_entry};

        #[test]
        fn visible_completions_and_nothing_to_submit_need_no_syscall() {
            assert_eq!(ring_entry(0, true), RingEntry::Skip);
        }

        #[test]
        fn queued_sqes_are_always_handed_over() {
            // Never skip with work in the SQ: those entries would sit
            // there until some later iteration happened to enter.
            assert_eq!(ring_entry(1, true), RingEntry::Submit);
            assert_eq!(ring_entry(8, false), RingEntry::SubmitAndWait);
        }

        #[test]
        fn an_empty_cq_is_the_only_thing_that_parks_the_thread() {
            assert_eq!(ring_entry(0, false), RingEntry::SubmitAndWait);
            // ...and visible work never does, whatever the SQ holds.
            for sq in [0, 1, 64] {
                assert_ne!(ring_entry(sq, true), RingEntry::SubmitAndWait);
            }
        }
    }

    /// Minimal `AppEvent` for these tests. `Copy` is required by `AppEvent`;
    /// the on-wire codec is unused because [`TagDecoder`] never invokes it
    /// (frames are interpreted directly from their tag byte).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestEvent {
        Cmd(u8),
        Query,
    }

    impl AppEvent for TestEvent {
        const MAX_ENCODED_SIZE: usize = 2;

        fn encoded_size(&self) -> usize {
            // Unused — the tests never round-trip through encode/decode.
            2
        }
        fn encode(&self, _buf: &mut [u8]) -> usize {
            unreachable!("process_frames does not encode app events")
        }
        fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
            unreachable!("process_frames does not decode app events directly")
        }
        fn is_query(&self) -> bool {
            matches!(self, TestEvent::Query)
        }
    }

    /// Placeholder `Application` impl. `process_frames` is generic over `A`
    /// only to constrain `A::Event` — none of the trait methods are called
    /// from the function under test, so they all `unreachable!`.
    struct TestApp;

    impl Application for TestApp {
        type Event = TestEvent;
        type Report = ();
        type QueryResponse = ();
        const APP_VERSION: u16 = 0;
        fn apply(&mut self, _event: TestEvent, _ctx: &ApplyCtx, _out: &mut Vec<()>) -> Option<()> {
            unreachable!()
        }
        fn tick(&mut self, _now_ns: u64, _out: &mut Vec<()>) {
            unreachable!()
        }
        fn check_request_seq(&mut self, _key_hash: u64, _seq: u64) -> bool {
            unreachable!()
        }
        fn build_reject(_event: &TestEvent, _reason: RejectReason) -> () {
            unreachable!()
        }
        fn snapshot<W: std::io::Write>(&self, _w: &mut W) -> std::io::Result<()> {
            unreachable!()
        }
        fn restore<R: Read>(_r: &mut R) -> std::io::Result<Self> {
            unreachable!()
        }
    }

    /// Stateless decoder that maps a frame's single payload byte to a
    /// [`Decoded`] outcome. Lets each test feed a precise mix of permitted,
    /// filtered, denied, and decode-error frames without standing up the
    /// real wire codec.
    ///
    /// Tag mapping (`0x00..=0xFB` map 1:1 to a Permitted seq, reserving the
    /// top four byte values for the non-Permitted outcomes):
    ///   * `0xFC` -> `Filter`
    ///   * `0xFD` -> `PermissionDenied`
    ///   * `0xFE` -> `DecodeError`
    ///   * `0xFF` -> `Permitted` with `is_query == true`
    ///   * `0x00..=0xFB` -> `Permitted` with `request_seq == byte`
    struct TagDecoder;

    impl RequestDecoder for TagDecoder {
        type Event = TestEvent;
        fn decode(&self, bytes: &[u8], _permission: Permission) -> Decoded<TestEvent> {
            match bytes.first().copied() {
                None => Decoded::DecodeError("empty payload"),
                Some(0xFC) => Decoded::Filter,
                Some(0xFD) => Decoded::PermissionDenied("denied"),
                Some(0xFE) => Decoded::DecodeError("bad"),
                Some(0xFF) => Decoded::Permitted {
                    request_seq: 0xFF,
                    event: TestEvent::Query,
                },
                Some(b) => Decoded::Permitted {
                    request_seq: b as u64,
                    event: TestEvent::Cmd(b),
                },
            }
        }
    }

    /// One-byte payload framed as `[u32 LE length=1][byte]`.
    fn frame(byte: u8) -> [u8; 5] {
        let mut f = [0u8; 5];
        f[..4].copy_from_slice(&1u32.to_le_bytes());
        f[4] = byte;
        f
    }

    /// Length prefix announcing an oversize frame. No payload bytes follow —
    /// `process_frames` decides on the prefix alone, before waiting for the
    /// body.
    fn oversize_prefix() -> [u8; 4] {
        ((MAX_FRAME_SIZE as u32) + 1).to_le_bytes()
    }

    /// Test fixture bundle. Grouped into a struct rather than returned as a
    /// 4-tuple to keep clippy happy (`type_complexity`) and to give each
    /// field a name at call sites.
    struct Fixture {
        conn: ConnectionEntry<UnixStream>,
        producer: ring::Producer<InputSlot<TestEvent>>,
        consumer: ring::Consumer<InputSlot<TestEvent>>,
        /// Client-side end of the socket pair — read from this to inspect
        /// any `ServerBusy` bytes the function under test writes.
        peer: UnixStream,
    }

    /// Build a fresh fixture: a `ConnectionEntry` backed by a `UnixStream`
    /// pair plus a single-consumer disruptor of the requested capacity.
    fn make_fixture(ring_capacity: usize) -> Fixture {
        let (server_side, peer) = UnixStream::pair().expect("UnixStream::pair");
        // Short read timeout on the peer so assertions of "no ServerBusy
        // written" return promptly instead of hanging the test.
        peer.set_read_timeout(Some(Duration::from_millis(50)))
            .expect("set_read_timeout");

        let entry = ConnectionEntry::<UnixStream> {
            connection_id: 7,
            addr: "127.0.0.1:1".parse().expect("addr parses"),
            permission: Permission::Trader,
            key_hash: 0xC0FFEE_u64,
            fd: server_side.as_raw_fd(),
            _reader: server_side,
            parse_buf: Vec::with_capacity(64),
            multishot_active: false,
            last_activity: Instant::now(),
            dying: false,
        };

        let (producer, mut consumers) =
            DisruptorBuilder::<InputSlot<TestEvent>>::new(ring_capacity)
                .add_consumer()
                .build();
        let consumer = consumers.pop().expect("consumer present");

        Fixture {
            conn: entry,
            producer,
            consumer,
            peer,
        }
    }

    /// Invoke `process_frames::<TestApp, UnixStream>`, threading the
    /// feature-gated histogram args when the relevant features are on so
    /// the call compiles in every `cargo test` configuration.
    ///
    /// Returns the disconnect flag plus the control-channel receiver —
    /// pipeline-full now surfaces as a `PipelineBusy` event for the
    /// response stage rather than reader-side socket bytes, so busy
    /// assertions read the channel, not the peer.
    fn run_process_frames(
        conn: &mut ConnectionEntry<UnixStream>,
        producer: &mut ring::Producer<InputSlot<TestEvent>>,
    ) -> (bool, mpsc::Receiver<ControlEvent>) {
        #[cfg(feature = "latency-trace")]
        let mut publish_rec = melin_transport_core::trace::register_stage("test: publish");
        #[cfg(feature = "tick-to-trade")]
        let mut ingest_rec = melin_transport_core::trace::register_stage("test: ingest");

        #[allow(clippy::let_unit_value)] // ZST when latency-trace is off
        let recv_ts = melin_transport_core::trace::mono_trace_ns();

        let (control_tx, control_rx) = mpsc::channel();
        let disconnect = process_frames::<TestApp, UnixStream>(
            conn,
            producer,
            &TagDecoder,
            &control_tx,
            0xDEAD_BEEF,
            recv_ts,
            #[cfg(feature = "latency-trace")]
            &mut publish_rec,
            #[cfg(feature = "tick-to-trade")]
            &mut ingest_rec,
        );
        (disconnect, control_rx)
    }

    /// Count `PipelineBusy` events for the fixture connection (id 7)
    /// queued on the control channel.
    fn busy_events(rx: &mpsc::Receiver<ControlEvent>) -> usize {
        let mut n = 0;
        while let Ok(event) = rx.try_recv() {
            if matches!(event, ControlEvent::PipelineBusy { connection_id } if connection_id == 7) {
                n += 1;
            }
        }
        n
    }

    /// Drain `consumer` into a Vec of `(seq, slot)` until it yields `None`.
    /// Used to assert exact event sequences after `process_frames` returns.
    fn drain(
        consumer: &mut ring::Consumer<InputSlot<TestEvent>>,
    ) -> Vec<(u64, InputSlot<TestEvent>)> {
        let mut out = Vec::new();
        while let Some(pair) = consumer.try_consume() {
            out.push(pair);
        }
        out
    }

    /// Try to read exactly 5 bytes (the ServerBusy frame size) from the
    /// peer. Returns `Some(bytes)` if the read completes within the peer's
    /// configured timeout, `None` if it times out (i.e. nothing was sent).
    fn read_server_busy(peer: &mut UnixStream) -> Option<[u8; 5]> {
        let mut buf = [0u8; 5];
        match peer.read_exact(&mut buf) {
            Ok(()) => Some(buf),
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => None,
            Err(e) => panic!("unexpected peer read error: {e}"),
        }
    }

    #[test]
    fn process_frames_publishes_all_frames_after_single_commit() {
        // Capacity > number of frames — every frame must succeed and become
        // visible to the consumer after the trailing `batch.commit()`.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(16);
        for byte in [0x01, 0x02, 0x03, 0x04, 0x05] {
            conn.parse_buf.extend_from_slice(&frame(byte));
        }

        let (disconnect, control_rx) = run_process_frames(&mut conn, &mut producer);
        assert!(!disconnect, "no oversize frame ⇒ no disconnect");

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 5, "all 5 frames must be visible");
        for (i, (seq, slot)) in events.iter().enumerate() {
            assert_eq!(*seq, i as u64, "seq monotonic from 0");
            assert_eq!(slot.connection_id, 7);
            assert_eq!(slot.key_hash, 0xC0FFEE_u64);
            let byte = (i + 1) as u8;
            assert_eq!(slot.request_seq, byte as u64);
            assert_eq!(slot.event, JournalEvent::App(TestEvent::Cmd(byte)));
            // Non-query event ⇒ inherits the caller-supplied wall-clock.
            assert_eq!(slot.timestamp_ns, 0xDEAD_BEEF);
        }
        // Parse buffer fully consumed.
        assert!(conn.parse_buf.is_empty());
        // No Full happened: no busy event, and the reader never writes
        // the client socket (egress belongs to the response stage).
        assert_eq!(
            busy_events(&control_rx),
            0,
            "no PipelineBusy on the happy path"
        );
        assert!(
            read_server_busy(&mut peer).is_none(),
            "reader must never write the client socket"
        );
    }

    /// The caller stamps `recv_ts` once per recv (at the kernel-return
    /// site) and every frame parsed from that buffer must carry that exact
    /// value. Guards against a regression to per-frame `recv_ts` capture,
    /// which excluded decode from the reader-ingest / server-e2e windows
    /// and drifted forward for later frames in a multi-frame recv. Gated on
    /// `latency-trace` because `recv_ts` is a real `u64` only then (`()`
    /// otherwise, leaving nothing to assert).
    #[cfg(feature = "latency-trace")]
    #[test]
    fn process_frames_stamps_every_slot_with_caller_recv_ts() {
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        for byte in [0x01, 0x02, 0x03, 0x04] {
            conn.parse_buf.extend_from_slice(&frame(byte));
        }

        // A recognizable sentinel the caller would have captured at the
        // recv site; distinct from the wall-clock stamp (0xDEAD_BEEF).
        const RECV_TS: u64 = 0x5EED_5EED;

        let mut publish_rec = melin_transport_core::trace::register_stage("test: publish recv_ts");
        #[cfg(feature = "tick-to-trade")]
        let mut ingest_rec = melin_transport_core::trace::register_stage("test: ingest recv_ts");

        let (control_tx, _control_rx) = mpsc::channel();
        let disconnect = process_frames::<TestApp, UnixStream>(
            &mut conn,
            &mut producer,
            &TagDecoder,
            &control_tx,
            0xDEAD_BEEF,
            RECV_TS,
            &mut publish_rec,
            #[cfg(feature = "tick-to-trade")]
            &mut ingest_rec,
        );
        assert!(!disconnect);

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 4, "all frames published");
        for (_seq, slot) in &events {
            assert_eq!(
                slot.recv_ts, RECV_TS,
                "every frame from one recv shares the single caller-supplied recv_ts"
            );
        }
    }

    #[test]
    fn process_frames_rotates_batch_at_commit_every_cap() {
        // Push more events than `COMMIT_EVERY` (= 16) into a recv-cycle.
        // The cap must trigger at least one mid-loop commit so the
        // consumer sees the first capacity-many events before the
        // remainder lands. Validates the visibility-delay cap from the
        // perf branch — without it, all 32 events would commit together
        // and the first frame would wait for the 32nd to decode.
        //
        // Capacity 64 leaves room for the entire input (no Full); ring
        // backpressure is exercised separately in
        // `process_frames_partial_commit_then_server_busy_when_pipeline_full`.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(64);
        const EVENT_COUNT: usize = 32;
        for i in 0..EVENT_COUNT {
            // Use bytes 1..=32 (each ≤ 0xFB so TagDecoder yields
            // `Permitted` with `request_seq == byte`).
            conn.parse_buf.extend_from_slice(&frame((i + 1) as u8));
        }

        let (disconnect, _control_rx) = run_process_frames(&mut conn, &mut producer);
        assert!(!disconnect, "no oversize / no Full ⇒ no disconnect");

        let events = drain(&mut consumer);
        assert_eq!(events.len(), EVENT_COUNT, "every event visible");
        for (i, (seq, slot)) in events.iter().enumerate() {
            assert_eq!(*seq, i as u64, "seq contiguous across batch rotations");
            let byte = (i + 1) as u8;
            assert_eq!(slot.event, JournalEvent::App(TestEvent::Cmd(byte)));
        }
        assert!(conn.parse_buf.is_empty());
    }

    #[test]
    fn process_frames_query_event_skips_wall_clock_stamp() {
        // `AppEvent::is_query` events bypass the journal stamp — verify
        // the timestamp is zeroed even when a non-zero batch_wall_ns was
        // supplied.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(8);
        conn.parse_buf.extend_from_slice(&frame(0xFF)); // tag → Query

        let (disconnect, _control_rx) = run_process_frames(&mut conn, &mut producer);
        assert!(!disconnect);

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 1);
        let (_, slot) = &events[0];
        assert_eq!(slot.event, JournalEvent::App(TestEvent::Query));
        assert_eq!(
            slot.timestamp_ns, 0,
            "query events must skip the wall-clock stamp"
        );
    }

    #[test]
    fn process_frames_partial_commit_then_server_busy_when_pipeline_full() {
        // Ring capacity 4 + 6 frames ⇒ first 4 commit, 5th triggers Full,
        // 6th is never reached because the loop breaks on Full. Validates:
        //   * `Err(Full)` does not roll back the prior 4 (single commit
        //     happens before the ServerBusy write).
        //   * The frame that triggered Full is silently dropped — its bytes
        //     are compacted out of `parse_buf` along with every earlier
        //     frame, mirroring pre-batch behaviour.
        //   * Exactly one PipelineBusy event is routed to the response
        //     stage, and nothing is written to the socket from here.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(4);
        for byte in [0x01, 0x02, 0x03, 0x04, 0x05, 0x06] {
            conn.parse_buf.extend_from_slice(&frame(byte));
        }

        let (disconnect, control_rx) = run_process_frames(&mut conn, &mut producer);
        assert!(!disconnect, "Full does not drop the connection");

        let events = drain(&mut consumer);
        assert_eq!(
            events.len(),
            4,
            "only the first capacity-many frames are visible"
        );
        for (i, (_, slot)) in events.iter().enumerate() {
            let byte = (i + 1) as u8;
            assert_eq!(slot.event, JournalEvent::App(TestEvent::Cmd(byte)));
        }

        // The busy notice goes to the response stage — never directly
        // onto the socket, where it could tear a partially-flushed
        // response frame (audit review F3).
        assert_eq!(
            busy_events(&control_rx),
            1,
            "exactly one PipelineBusy routed"
        );
        assert!(
            read_server_busy(&mut peer).is_none(),
            "reader must never write the client socket"
        );

        // The frame that triggered Full (0x05) had its bytes consumed by
        // the loop's `cursor +=` before `try_push_with` ran; the 6th frame
        // is never inspected because the loop broke. Compaction shifts
        // the unprocessed tail (the 6th frame's bytes) to the front.
        assert_eq!(
            conn.parse_buf,
            frame(0x06).to_vec(),
            "the 6th frame remains in parse_buf for the next recv-cycle"
        );
    }

    /// Regression for the 2026-08 io_uring audit, finding 2 and review
    /// finding F3 (docs/internal/io-uring-audit-2026-08.md): the
    /// pipeline-full path must not touch the client socket at all. The
    /// original defect was a blocking `write(2)` of ServerBusy that
    /// parked the reader thread whenever the offender's socket buffer
    /// was also full — stalling ingress for *every* connection exactly
    /// on the overload path (against that code this test never
    /// returns). The busy notice now routes to the response stage,
    /// which also closes the frame-tearing race of any reader-side
    /// send. The socket-buffer fill stays: it proves the path is
    /// insensitive to socket state.
    #[test]
    fn pipeline_full_never_touches_the_client_socket() {
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(2);

        // Fill the server→peer direction until the kernel refuses more —
        // the state a non-reading client leaves the connection in.
        let junk = [0u8; 4096];
        loop {
            let n = unsafe {
                libc::send(
                    conn.fd,
                    junk.as_ptr().cast(),
                    junk.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            if n < 0 {
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EAGAIN),
                    "unexpected errno while filling socket"
                );
                break;
            }
        }

        // Capacity 2 + 3 frames ⇒ the third triggers Full ⇒ ServerBusy.
        conn.parse_buf.extend_from_slice(&frame(0x01));
        conn.parse_buf.extend_from_slice(&frame(0x02));
        conn.parse_buf.extend_from_slice(&frame(0x03));

        let start = std::time::Instant::now();
        let (disconnect, control_rx) = run_process_frames(&mut conn, &mut producer);
        // Generous bound to stay unflaky under CI load — the fixed path
        // returns in microseconds, the broken one only when the peer
        // reads (here: never).
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "pipeline-full path blocked on a full socket"
        );
        assert!(!disconnect, "Full does not drop the connection");
        assert_eq!(
            drain(&mut consumer).len(),
            2,
            "the frames that fit are still published"
        );
        assert_eq!(
            busy_events(&control_rx),
            1,
            "busy notice routed to response stage"
        );
        // Nothing beyond the pre-fill junk arrives: drain it, then the
        // read must time out rather than yield a reader-written frame.
        let mut sink = [0u8; 4096];
        loop {
            match peer.read(&mut sink) {
                Ok(0) => panic!("unexpected EOF"),
                Ok(_) => continue,
                Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                    break;
                }
                Err(e) => panic!("unexpected peer read error: {e}"),
            }
        }
    }

    #[test]
    fn process_frames_oversize_commits_prior_frames_then_signals_disconnect() {
        // Two valid frames followed by an oversize length prefix must:
        //   * publish the two valid frames (commit-before-break) so the
        //     pipeline observes them even though we're about to tear the
        //     connection down,
        //   * return `true` so the caller drops the connection,
        //   * NOT emit PipelineBusy (that is reserved for pipeline-full).
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(16);
        conn.parse_buf.extend_from_slice(&frame(0x01));
        conn.parse_buf.extend_from_slice(&frame(0x02));
        conn.parse_buf.extend_from_slice(&oversize_prefix());

        let (disconnect, control_rx) = run_process_frames(&mut conn, &mut producer);
        assert!(disconnect, "oversize frame must request disconnect");

        let events = drain(&mut consumer);
        assert_eq!(
            events.len(),
            2,
            "prior frames are committed before the break"
        );
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x01)));
        assert_eq!(events[1].1.event, JournalEvent::App(TestEvent::Cmd(0x02)));

        assert_eq!(
            busy_events(&control_rx),
            0,
            "PipelineBusy is emitted on Full, not on oversize"
        );
        assert!(
            read_server_busy(&mut peer).is_none(),
            "reader must never write the client socket"
        );
    }

    #[test]
    fn process_frames_filters_denied_and_decode_errors_advance_cursor() {
        // Mixed batch: Permitted, Filter, PermissionDenied, DecodeError,
        // Permitted. Only the two Permitted frames must reach the
        // consumer; all bytes are consumed (parse_buf fully drains).
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        for byte in [0x01, 0xFC, 0xFD, 0xFE, 0x02] {
            conn.parse_buf.extend_from_slice(&frame(byte));
        }

        let (disconnect, _control_rx) = run_process_frames(&mut conn, &mut producer);
        assert!(!disconnect);

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x01)));
        assert_eq!(events[1].1.event, JournalEvent::App(TestEvent::Cmd(0x02)));
        assert!(
            conn.parse_buf.is_empty(),
            "all bytes advanced past compaction"
        );
    }

    #[test]
    fn process_frames_preserves_partial_trailing_frame() {
        // One complete frame followed by a truncated length prefix. The
        // complete frame must publish; the partial bytes must survive
        // compaction at the front of `parse_buf` for the next recv-cycle.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        conn.parse_buf.extend_from_slice(&frame(0x42));
        // Three of four length-prefix bytes — `cursor + 4 <= len()` is
        // false, so the loop breaks before consuming anything from the
        // partial.
        conn.parse_buf.extend_from_slice(&[0xDE, 0xAD, 0xBE]);

        let (disconnect, _control_rx) = run_process_frames(&mut conn, &mut producer);
        assert!(!disconnect);

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x42)));
        assert_eq!(
            conn.parse_buf,
            vec![0xDE, 0xAD, 0xBE],
            "partial length prefix preserved for next recv-cycle"
        );
    }

    #[test]
    fn process_frames_empty_buffer_is_noop() {
        // No bytes in parse_buf ⇒ loop never enters; commit is the
        // documented zero-slot no-op; no busy event; no disconnect.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(4);

        let (disconnect, control_rx) = run_process_frames(&mut conn, &mut producer);
        assert!(!disconnect);
        assert_eq!(drain(&mut consumer).len(), 0);
        assert!(conn.parse_buf.is_empty());
        assert_eq!(busy_events(&control_rx), 0);
        assert!(read_server_busy(&mut peer).is_none());
    }

    /// End-to-end soak through the real reader loop: 4 connections
    /// concurrently write 10 000 frames each, one `write(2)` per frame,
    /// so the receive path churns through the shared buffer pool and
    /// its recycle machinery under genuine concurrency (buf_ring where
    /// the kernel supports it, legacy ProvideBuffers otherwise — this
    /// test is the coverage for whichever mode the host engages).
    ///
    /// The failure signatures of a recycle bug are exactly what is
    /// asserted: a torn or misordered frame (a buffer reused while the
    /// kernel still owned it), a lost frame, or a spurious disconnect
    /// (`ENOBUFS` mishandled as a client error).
    #[test]
    fn reader_loop_soak_delivers_every_frame_in_order() {
        use std::io::Write as _;

        const CONNS: u64 = 4;
        const FRAMES: u32 = 10_000;

        // Capacity ≥ total frames: even if this thread is descheduled
        // and stops draining, the ring cannot fill, so the reader never
        // sheds load (which would legitimately drop frames and turn a
        // CI hiccup into a false failure).
        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(65536)
            .add_consumer()
            .build();
        let mut consumer = consumers.pop().expect("consumer");
        let (control_tx, control_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));

        let mut handle = spawn_reader::<TestApp, UnixStream>(
            producer,
            Arc::new(TagDecoder),
            control_tx,
            0,    // "do not pin" sentinel
            None, // no idle timeout — a CI stall must not disconnect
            None, // no tick generator
            Arc::clone(&shutdown),
        );

        let mut writers = Vec::new();
        for id in 0..CONNS {
            let (client, server_side) = UnixStream::pair().expect("socketpair");
            handle.register(ReaderRegistration {
                connection_id: ConnectionId(id),
                reader: server_side,
                addr: "127.0.0.1:1".parse().expect("addr"),
                permission: Permission::Trader,
                key_hash: id,
            });
            writers.push(std::thread::spawn(move || {
                let mut client = client;
                for i in 0..FRAMES {
                    let byte = (i % 200 + 1) as u8;
                    client.write_all(&frame(byte)).expect("client write");
                    // Brief pauses fragment the stream so the reader
                    // sees many small recvs (heavy buffer churn) rather
                    // than a few large coalesced ones.
                    if i % 512 == 0 {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                }
                client // keep the socket alive until after the drain
            }));
        }

        // Drain until every frame arrived or the deadline passes. The
        // deadline is generous — the run takes well under a second on
        // an idle machine — because a genuine recycle deadlock must
        // fail loudly, not hang CI.
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut per_conn_count = [0u32; CONNS as usize];
        let mut total: u64 = 0;
        while total < CONNS * FRAMES as u64 {
            assert!(
                Instant::now() < deadline,
                "soak stalled: {total} of {} frames after 60s ({per_conn_count:?})",
                CONNS * FRAMES as u64
            );
            while let Some((_seq, slot)) = consumer.try_consume() {
                let conn = slot.connection_id as usize;
                assert!(conn < CONNS as usize, "unknown connection id");
                let expected = (per_conn_count[conn] % 200 + 1) as u8;
                assert_eq!(
                    slot.event,
                    JournalEvent::App(TestEvent::Cmd(expected)),
                    "conn {conn} frame {} out of order or corrupted",
                    per_conn_count[conn]
                );
                per_conn_count[conn] += 1;
                total += 1;
            }
            std::hint::spin_loop();
        }

        // Any control event at this point is a defect: sockets are
        // still alive (held by the writer join handles) and the ring
        // never filled. Every arm diverges, so inspecting the first
        // queued event suffices.
        if let Ok(event) = control_rx.try_recv() {
            match event {
                ControlEvent::Disconnected { connection_id } => {
                    panic!("spurious disconnect of connection {connection_id}")
                }
                ControlEvent::PipelineBusy { connection_id } => {
                    panic!("spurious pipeline-full for connection {connection_id}")
                }
                ControlEvent::Connected { .. } => {
                    unreachable!("reader never sends Connected")
                }
            }
        }

        let _clients: Vec<UnixStream> = writers
            .into_iter()
            .map(|w| w.join().expect("writer thread"))
            .collect();
        handle.shutdown();
        handle.join();
    }

    /// Shutdown with live, armed connections must quiesce the ring
    /// provably and promptly. The armed multishot RECVs sit on sockets
    /// whose peers never disconnect, so nothing completes them on its
    /// own — the teardown's wakes (per-connection `shutdown(2)`, the
    /// eventfd write) are what retire them, including on hosts that
    /// filter the AsyncCancel opcode, where the old cancel-then-quiet
    /// teardown could conclude "done" with arms still live against the
    /// soon-freed pool. A drain that cannot prove quiescence only
    /// returns at the full deadline (and leaks the buffers), so
    /// returning well inside it is the proof this test can observe.
    #[test]
    fn shutdown_quiesces_armed_connections_promptly() {
        use std::io::Write as _;

        const CONNS: u64 = 3;
        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(1024)
            .add_consumer()
            .build();
        let mut consumer = consumers.pop().expect("consumer");
        let (control_tx, _control_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut handle = spawn_reader::<TestApp, UnixStream>(
            producer,
            Arc::new(TagDecoder),
            control_tx,
            0,    // "do not pin" sentinel
            None, // no idle timeout
            None, // no tick generator
            Arc::clone(&shutdown),
        );

        let mut clients = Vec::new();
        for id in 0..CONNS {
            let (mut client, server_side) = UnixStream::pair().expect("socketpair");
            handle.register(ReaderRegistration {
                connection_id: ConnectionId(id),
                reader: server_side,
                addr: "127.0.0.1:1".parse().expect("addr"),
                permission: Permission::Trader,
                key_hash: id,
            });
            // One delivered frame per connection proves its multishot
            // is armed and active before the shutdown fires.
            client.write_all(&frame(7)).expect("client write");
            clients.push(client);
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut seen = 0u64;
        while seen < CONNS {
            assert!(
                Instant::now() < deadline,
                "frames never arrived ({seen}/{CONNS})"
            );
            while consumer.try_consume().is_some() {
                seen += 1;
            }
            std::hint::spin_loop();
        }

        // The clients stay alive across the shutdown: no peer
        // disconnect can complete the armed RECVs — only the teardown's
        // own wakes can.
        let started = Instant::now();
        handle.shutdown();
        handle.join();
        let elapsed = started.elapsed();
        assert!(
            elapsed < crate::uring_teardown::DRAIN_TIMEOUT / 4,
            "reader teardown took {elapsed:?}, close to the drain deadline — \
             armed operations were not woken"
        );
        drop(clients);
    }

    /// Regression for audit review F1
    /// (docs/internal/io-uring-audit-2026-08.md): a connection torn down
    /// while its multishot RECV is still armed must not have its slab
    /// index reused while the kernel can still post CQEs carrying it.
    /// Pre-fix, the LIFO free list handed client A's index straight to
    /// client B; A's socket stayed open (the armed op holds a file
    /// reference past the fd close), and A's continued bytes were parsed
    /// under B's connection id, key hash, and permissions — order-flow
    /// injection under someone else's identity. Post-fix, A's entry dies
    /// in place until the cancelled op's terminal CQE, so B gets a fresh
    /// index and A's post-teardown bytes are discarded.
    #[test]
    fn torn_down_connection_cannot_inject_frames_into_a_reused_slot() {
        use std::io::Write as _;

        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(65536)
            .add_consumer()
            .build();
        let mut consumer = consumers.pop().expect("consumer");
        let (control_tx, control_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut handle = spawn_reader::<TestApp, UnixStream>(
            producer,
            Arc::new(TagDecoder),
            control_tx,
            0,
            None,
            None,
            Arc::clone(&shutdown),
        );

        const A_ID: u64 = 100;
        const B_ID: u64 = 200;

        // Client A: valid frames (0x01..=0x08), an oversize prefix (the
        // teardown trigger), then a sustained stream of would-be
        // injection frames from A's byte range.
        let (a_client, a_server) = UnixStream::pair().expect("socketpair");
        handle.register(ReaderRegistration {
            connection_id: ConnectionId(A_ID),
            reader: a_server,
            addr: "127.0.0.1:1".parse().expect("addr"),
            permission: Permission::Trader,
            key_hash: A_ID,
        });
        let a_writer = std::thread::spawn(move || {
            let mut a = a_client;
            for byte in 0x01..=0x08u8 {
                let _ = a.write_all(&frame(byte));
            }
            let _ = a.write_all(&oversize_prefix());
            // Keep writing after the reader repudiates us. Write errors
            // (EPIPE once teardown's shutdown(2) lands) are the fix
            // doing its job — ignore them and keep trying.
            let stop = Instant::now() + Duration::from_millis(300);
            while Instant::now() < stop {
                let _ = a.write_all(&frame(0x05));
                std::thread::sleep(Duration::from_micros(50));
            }
            a
        });

        // Wait for A's teardown announcement, then register B — the
        // instant a buggy free list would hand B the still-armed index.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match control_rx.try_recv() {
                Ok(ControlEvent::Disconnected { connection_id }) if connection_id == A_ID => break,
                Ok(_) => {}
                Err(_) => {
                    assert!(Instant::now() < deadline, "no Disconnected for A");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }

        const B_FRAMES: u32 = 2_000;
        let (b_client, b_server) = UnixStream::pair().expect("socketpair");
        handle.register(ReaderRegistration {
            connection_id: ConnectionId(B_ID),
            reader: b_server,
            addr: "127.0.0.1:2".parse().expect("addr"),
            permission: Permission::Trader,
            key_hash: B_ID,
        });
        let b_writer = std::thread::spawn(move || {
            let mut b = b_client;
            for i in 0..B_FRAMES {
                let byte = 0x21 + (i % 0x10) as u8; // 0x21..=0x30, disjoint from A
                b.write_all(&frame(byte)).expect("B write");
                if i % 128 == 0 {
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
            b
        });

        // Drain until all of B's frames arrive. Every B-attributed slot
        // must carry B's key hash and a B-range byte; anything else is
        // the injection this test exists to prevent.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut b_seen = 0u32;
        while b_seen < B_FRAMES {
            assert!(
                Instant::now() < deadline,
                "stalled: {b_seen}/{B_FRAMES} B frames"
            );
            while let Some((_seq, slot)) = consumer.try_consume() {
                match slot.event {
                    JournalEvent::App(TestEvent::Cmd(byte)) => {
                        if slot.connection_id == B_ID {
                            assert_eq!(slot.key_hash, B_ID, "B slot with foreign key hash");
                            assert!(
                                (0x21..=0x30).contains(&byte),
                                "byte {byte:#x} injected into B's stream"
                            );
                            b_seen += 1;
                        } else {
                            assert_eq!(slot.connection_id, A_ID, "unknown connection id");
                            assert!(
                                (0x01..=0x08).contains(&byte),
                                "unexpected byte {byte:#x} on A"
                            );
                        }
                    }
                    ref other => panic!("unexpected event {other:?}"),
                }
            }
            std::hint::spin_loop();
        }

        let _a = a_writer.join().expect("A writer");
        let _b = b_writer.join().expect("B writer");
        handle.shutdown();
        handle.join();
    }
}
