//! io_uring-based multiplexed reader with multishot RECV.
//!
//! Uses `IORING_OP_RECV` with `IORING_RECV_MULTISHOT` — a single SQE per
//! connection produces multiple CQEs as data arrives, eliminating the
//! resubmission overhead of standard RECV. Combined with provided buffer
//! groups (`IOSQE_BUFFER_SELECT`), the kernel selects a buffer from a
//! shared pool for each recv, replacing per-connection buffer allocations.
//!
//! Uses a single reader thread — io_uring is efficient enough for hundreds
//! of connections. New connections are registered via eventfd wakeup.
//!
//! Connection state is stored in a slab (index-stable Vec) so that io_uring
//! user_data carries a slab index, not an fd. This avoids fd-reuse races
//! where a recycled fd number could match a stale CQE.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error};

use crate::ControlEvent;
use melin_app::AppEvent;
use melin_app::Application;
use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder};

/// Decoder type alias: request decoder bound to the application's `Event`
/// type. Mirrors [`crate::response::ResponseEncoderArc`]; hides
/// the `dyn RequestDecoder<Event = …>` spelling at call sites that thread
/// the decoder through several functions.
pub type RequestDecoderArc<A> = Arc<dyn RequestDecoder<Event = <A as Application>::Event>>;
use melin_app::unix_epoch_nanos;
use melin_journal::JournalEvent;
use melin_pipeline::ring;
use melin_transport_core::pipeline::InputSlot;
use melin_transport_core::trace::mono_trace_ns;
use melin_wire_protocol::control::TransportResponse;
use melin_wire_protocol::control_codec;

/// Size of each provided buffer. 4 KiB accommodates multiple frames per
/// recv (frames are typically <100 bytes).
const BUF_SIZE: usize = 4096;

/// Number of provided buffers in the shared pool. Must be large enough
/// to handle concurrent in-flight recvs across all connections. When the
/// pool is exhausted, multishot terminates and is resubmitted after buffers
/// are re-provided. 2048 supports up to ~1024 connections per reader
/// thread with headroom for burst re-provision lag.
const NUM_BUFFERS: u16 = 2048;

/// Buffer group ID for the provided recv buffer pool.
const BUF_GROUP_ID: u16 = 0;

/// Maximum frame payload size (matches `BlockingFrameReader`).
const MAX_FRAME_SIZE: usize = 1024;

/// io_uring submission queue depth. Power of 2, sized for up to ~1024
/// connections per reader thread (multishot RECVs + eventfd read +
/// buffer re-provisions).
const RING_SIZE: u32 = 4096;

/// User data sentinel for the eventfd read SQE.
const EVENTFD_TOKEN: u64 = u64::MAX;

/// User data sentinel for ProvideBuffers CQEs. These are best-effort
/// re-provisions — we log errors but don't act on success.
const PROVIDE_BUFS_TOKEN: u64 = u64::MAX - 1;

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
    event_fd: RawFd,
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
                libc::write(self.event_fd, &val as *const u64 as *const libc::c_void, 8);
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
            libc::write(self.event_fd, &val as *const u64 as *const libc::c_void, 8);
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

    let event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    assert!(event_fd >= 0, "eventfd creation failed");

    let wakeup_fd = event_fd;
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

/// Main io_uring reader loop. Runs until channel disconnection.
///
/// When `tick_cadence` is `Some`, the loop also generates the engine's
/// scheduler ticks — see [`spawn_reader`] for the rationale.
fn reader_loop<A: Application, R: AsRawFd>(
    command_rx: mpsc::Receiver<ReaderRegistration<R>>,
    wakeup_fd: RawFd,
    mut producer: ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn RequestDecoder<Event = A::Event>,
    control_tx: &mpsc::Sender<ControlEvent>,
    connection_timeout: Option<Duration>,
    tick_cadence: Option<Duration>,
    shutdown: &AtomicBool,
) {
    let mut ring = IoUring::new(RING_SIZE).expect("failed to create io_uring instance");

    // Pre-encode the ServerBusy response frame (length prefix + tag = 5 bytes).
    let server_busy_frame = {
        let mut buf = [0u8; 8];
        let n = control_codec::encode_transport_response(&TransportResponse::ServerBusy, &mut buf)
            .expect("ServerBusy encodes");
        let mut frame = [0u8; 5];
        frame.copy_from_slice(&buf[..n]);
        frame
    };

    let mut slab = ConnectionSlab::<R>::new();
    // Reverse map for cleanup when a connection's fd needs removal.
    // HashMap for O(1) lookup by fd. Sized for typical connection counts.
    let mut fd_to_slab: HashMap<RawFd, usize> = HashMap::with_capacity(256);

    // Eventfd read buffer — boxed for pointer stability across SQE lifetimes.
    let mut eventfd_buf: Box<[u8; 8]> = Box::new([0u8; 8]);

    // Shared buffer pool for provided buffers. Contiguous allocation of
    // NUM_BUFFERS × BUF_SIZE bytes. The kernel selects a buffer from this
    // pool for each recv completion, identified by buffer ID in the CQE.
    let mut buffer_pool = vec![0u8; NUM_BUFFERS as usize * BUF_SIZE].into_boxed_slice();

    // Pre-allocated CQE collection buffer. We must collect CQEs before
    // processing because the CQ borrow must end before pushing new SQEs.
    // Stores (user_data, result, flags) — flags needed for buffer ID and
    // multishot continuation.
    let mut cqes: Vec<(u64, i32, u32)> = Vec::with_capacity(RING_SIZE as usize);

    // Register the provided buffer pool with io_uring.
    register_buffer_pool(&mut ring, buffer_pool.as_mut_ptr());

    // Submit the initial eventfd read so we wake on first connection.
    push_eventfd_read(&mut ring, wakeup_fd, eventfd_buf.as_mut_ptr());

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

    // Coarse gate for timeout scanning — avoids scanning on every
    // submit_and_wait return during high throughput.
    let mut last_timeout_scan = Instant::now();
    // Pre-allocated buffer for stale connection indices to avoid
    // heap allocation inside the hot loop.
    let mut stale: Vec<(usize, u64, RawFd)> = Vec::new();

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

        // Submit any pending SQEs and block until at least 1 CQE is ready.
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
                error!(error = %e, "io_uring submit_and_wait error");
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

            // ── ProvideBuffers completion ──
            if token == PROVIDE_BUFS_TOKEN {
                if result < 0 {
                    error!(error = result, "ProvideBuffers failed");
                }
                continue;
            }

            // ── Eventfd wakeup ──
            if token == EVENTFD_TOKEN {
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
                        };
                        let idx = slab.insert(entry);
                        fd_to_slab.insert(fd, idx);

                        // Submit multishot RECV for this connection.
                        push_recv_multi(&mut ring, &mut slab, idx);
                    }
                } else {
                    error!(error = result, "eventfd read error");
                }

                // Re-submit eventfd read for the next wakeup.
                push_eventfd_read(&mut ring, wakeup_fd, eventfd_buf.as_mut_ptr());
                continue;
            }

            // ── Connection multishot RECV completion ──

            let slab_idx = token as usize;
            let has_more = (flags & IORING_CQE_F_MORE) != 0;

            if result <= 0 {
                // Disconnect (0) or error (negative errno).
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

            // Extract the buffer ID from the CQE flags. The kernel sets
            // IORING_CQE_F_BUFFER and encodes the buffer ID in bits 16-31.
            let buf_id = if (flags & IORING_CQE_F_BUFFER) != 0 {
                (flags >> IORING_CQE_BUFFER_SHIFT) as usize
            } else {
                // Should not happen with provided buffers — defensive skip.
                debug!(slab_idx, "recv CQE without buffer flag");
                continue;
            };

            // Feed received bytes into the frame parser from the shared pool.
            let action = if let Some(entry) = slab.get_mut(slab_idx) {
                if !has_more {
                    entry.multishot_active = false;
                }

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
                    &server_busy_frame,
                    batch_wall_ns,
                    #[cfg(feature = "latency-trace")]
                    &mut publish_rec,
                    #[cfg(feature = "tick-to-trade")]
                    &mut ingest_rec,
                );
                if drop_conn {
                    Action::Remove {
                        connection_id: entry.connection_id,
                        fd: entry.fd,
                    }
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

            // Re-provide the consumed buffer back to the pool. Must happen
            // after we've copied the data out. Pushed to SQ and submitted
            // on the next submit_and_wait.
            re_provide_buffer(&mut ring, buffer_pool.as_mut_ptr(), buf_id);

            match action {
                Action::Remove { connection_id, fd } => {
                    slab.remove(slab_idx);
                    fd_to_slab.remove(&fd);
                    let _ = control_tx.send(ControlEvent::Disconnected { connection_id });
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
                    if let Some(entry) = slot
                        && now.duration_since(entry.last_activity) > timeout
                    {
                        debug!(
                            connection_id = entry.connection_id,
                            addr = %entry.addr,
                            "connection timed out"
                        );
                        stale.push((idx, entry.connection_id, entry.fd));
                    }
                }
                for &(idx, connection_id, fd) in &stale {
                    slab.remove(idx);
                    fd_to_slab.remove(&fd);
                    let _ = control_tx.send(ControlEvent::Disconnected { connection_id });
                }
            }
        }
    }

    unsafe {
        libc::close(wakeup_fd);
    }
}

/// What to do after processing a RECV CQE.
enum Action {
    /// Multishot terminated but connection healthy — resubmit RecvMulti.
    Resubmit,
    /// Connection should be removed (malformed frame).
    Remove { connection_id: u64, fd: RawFd },
    /// Multishot still active — nothing to do.
    None,
}

// ---------------------------------------------------------------------------
// SQE helpers
// ---------------------------------------------------------------------------

/// Register the provided buffer pool with io_uring via ProvideBuffers.
/// Submits synchronously and panics on failure — called once at startup.
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

/// Re-provide a single consumed buffer back to the pool. Pushed to SQ
/// without immediate submission — batched with the next submit_and_wait.
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
    server_busy_frame: &[u8; 5],
    batch_wall_ns: u64,
    #[cfg(feature = "latency-trace")] publish_rec: &mut melin_transport_core::trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) -> bool {
    let mut cursor = 0;
    // Disconnect signal returned to caller (set on oversize frame).
    let mut disconnect = false;
    // Set when `try_push_with` reports the input ring is full. We commit
    // whatever made it into the batch before sending ServerBusy and
    // breaking out of the loop, so the consumer sees this connection's
    // earlier frames promptly.
    let mut pipeline_full = false;

    // Batch every publish from this recv-cycle into a single Release
    // store on the input ring's producer cursor. Per-event `try_publish`
    // emits one cursor-store per frame; under steady-state TCP load the
    // matching/journal consumers see that store cross the cache line on
    // every event. Coalescing into one store per recv-loop iteration
    // brings the TCP path to parity with the DPDK ingress (which
    // already uses the batch API — see `dpdk_transport.rs`). The batch
    // is committed at function exit regardless of how the loop ends; on
    // an early return we explicitly commit before propagating.
    //
    // Bounded at `COMMIT_EVERY` events to cap consumer-visibility delay.
    // Unbounded batching let tcp-dual-repl p99.9 drift +~80 µs vs the
    // pre-batch baseline: tail recv-cycles up to ~100 events left their
    // first frame waiting ~tens of µs before becoming visible to the
    // journal stage, which then interacted with replica ack pacing.
    // A diagnostic histogram (now removed) measured the recv-cycle
    // distribution under steady-state load — p50 ≈ 10, p99 ≈ 30,
    // p99.9 ≈ 40, max ≈ 100 events per batch. Capping at 16 leaves the
    // median path unsplit (≤1% extra cursor stores) while bounding
    // worst-case visibility delay to ~16 decode iterations. The cap
    // turned out to *also* improve median throughput on tcp single-node
    // (+4.75% vs main vs +3.15% unbounded) — pipeline backpressure
    // visibly eased once consumers stopped waiting on long batches.
    const COMMIT_EVERY: u64 = 16;
    let mut batch = producer.batch();

    while cursor + 4 <= conn.parse_buf.len() {
        // Read 4-byte little-endian length prefix.
        let len_bytes: [u8; 4] = conn.parse_buf[cursor..cursor + 4]
            .try_into()
            .expect("slice is exactly 4 bytes");
        let frame_len = u32::from_le_bytes(len_bytes) as usize;

        if frame_len > MAX_FRAME_SIZE {
            debug!(
                connection_id = conn.connection_id,
                addr = %conn.addr,
                frame_len,
                "frame too large, dropping connection"
            );
            disconnect = true;
            break;
        }

        // Wait for the complete frame before parsing.
        if cursor + 4 + frame_len > conn.parse_buf.len() {
            break;
        }

        let frame = &conn.parse_buf[cursor + 4..cursor + 4 + frame_len];
        cursor += 4 + frame_len;

        let (seq, event) = match decoder.decode(frame, conn.permission) {
            Decoded::Filter => continue,
            Decoded::PermissionDenied(reason) => {
                debug!(
                    connection_id = conn.connection_id,
                    reason, "permission denied, dropping request"
                );
                continue;
            }
            Decoded::DecodeError(reason) => {
                debug!(
                    connection_id = conn.connection_id,
                    addr = %conn.addr,
                    reason, "decode error"
                );
                continue;
            }
            Decoded::Permitted { request_seq, event } => (request_seq, event),
        };

        #[allow(clippy::let_unit_value)]
        let recv_ts = mono_trace_ns();

        // Sequence is allocated by the journal stage in disruptor cursor
        // order — see `InputSlot::sequence`. Read-only query events
        // (`AppEvent::is_query`) bypass the journal and skip the
        // wall-clock stamp; everything else inherits this batch's
        // shared `batch_wall_ns`.
        let ts = if event.is_query() { 0 } else { batch_wall_ns };
        let event = JournalEvent::App(event);

        #[cfg(feature = "latency-trace")]
        let pre_publish = mono_trace_ns();
        #[allow(clippy::let_unit_value)]
        let publish_ts = mono_trace_ns();
        let connection_id = conn.connection_id;
        let key_hash = conn.key_hash;

        // Slot fields are captured by-value into the closure so the slot
        // can be filled in place inside the ring buffer — same idiom as
        // the DPDK ingress path.
        let push_result = batch.try_push_with(|slot| {
            slot.connection_id = connection_id;
            slot.key_hash = key_hash;
            slot.request_seq = seq;
            slot.sequence = 0;
            slot.timestamp_ns = ts;
            slot.event = event;
            slot.publish_ts = publish_ts;
            slot.recv_ts = recv_ts;
        });

        if push_result.is_err() {
            // Pipeline full. The frame's bytes have already been
            // consumed from `parse_buf` via the `cursor +=` above and
            // will be dropped at the compaction step — the client
            // receives a ServerBusy in lieu of a response for this
            // frame. We send ServerBusy *after* committing the batch
            // below so the events that did fit become visible to the
            // pipeline before the client is told we're busy.
            pipeline_full = true;
            break;
        }

        #[cfg(feature = "latency-trace")]
        let publish_done = mono_trace_ns();
        #[cfg(feature = "latency-trace")]
        publish_rec.record_elapsed(pre_publish, publish_done);
        // Ingest covers the entire reader cost for this frame:
        // decode + auth/dedup + slot construction + publish.
        // `recv_ts` is the frame-extraction timestamp (a software
        // approximation of NIC ingress — true HW timestamping is
        // a follow-up; see `docs/benchmarking.md`). Measured up to the
        // slot-fill completion; the cursor-advance cost is amortised
        // across the whole batch and not attributed per-frame.
        #[cfg(feature = "tick-to-trade")]
        ingest_rec.record_elapsed(recv_ts, mono_trace_ns());

        // Rotate the batch once it reaches the visibility-delay cap. The
        // commit produces a single Release store on the input cursor; a
        // fresh `producer.batch()` starts the next group from the new
        // cursor with `count = 0`. Recv-cycles smaller than the cap (the
        // common case — p50 ≈ 10 events) commit exactly once at function
        // exit and never enter this branch.
        if batch.len() >= COMMIT_EVERY {
            batch.commit();
            batch = producer.batch();
        }
    }

    // Single Release store on the input cursor for every slot pushed in
    // this call. Safe to invoke with zero slots — `Batch::commit` is a
    // no-op when nothing was written.
    batch.commit();

    if pipeline_full {
        debug!(
            connection_id = conn.connection_id,
            "pipeline full, sending ServerBusy"
        );
        // Best-effort: if the write fails, the client will timeout.
        let n = unsafe {
            libc::write(
                conn.fd,
                server_busy_frame.as_ptr().cast(),
                server_busy_frame.len(),
            )
        };
        if n != server_busy_frame.len() as isize {
            debug!(
                connection_id = conn.connection_id,
                written = n,
                "ServerBusy write incomplete"
            );
        }
    }

    // Compact: shift remaining bytes to the front of the parse buffer.
    // Uses copy_within + truncate instead of drain() to avoid the
    // Drain iterator overhead.
    if cursor > 0 {
        let remaining = conn.parse_buf.len() - cursor;
        conn.parse_buf.copy_within(cursor.., 0);
        conn.parse_buf.truncate(remaining);
    }

    disconnect
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
    use melin_pipeline::ring::DisruptorBuilder;
    use std::io::{ErrorKind, Read};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    /// Minimal `AppEvent` for these tests. `Copy` is required by `AppEvent`;
    /// the on-wire codec is unused because [`TagDecoder`] never invokes it
    /// (frames are interpreted directly from their tag byte).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestEvent {
        Cmd(u8),
        Query,
    }

    impl AppEvent for TestEvent {
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

    /// 5-byte ServerBusy placeholder. The real reader passes an encoded
    /// `ResponseKind::ServerBusy`, but `process_frames` writes the bytes
    /// verbatim — distinct sentinel bytes make peer-side assertions
    /// unambiguous.
    const TEST_SERVER_BUSY: [u8; 5] = [0xAA, 0xBB, 0xCC, 0xDD, 0xEE];

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
    fn run_process_frames(
        conn: &mut ConnectionEntry<UnixStream>,
        producer: &mut ring::Producer<InputSlot<TestEvent>>,
    ) -> bool {
        #[cfg(feature = "latency-trace")]
        let mut publish_rec = melin_transport_core::trace::register_stage("test: publish");
        #[cfg(feature = "tick-to-trade")]
        let mut ingest_rec = melin_transport_core::trace::register_stage("test: ingest");

        process_frames::<TestApp, UnixStream>(
            conn,
            producer,
            &TagDecoder,
            &TEST_SERVER_BUSY,
            0xDEAD_BEEF,
            #[cfg(feature = "latency-trace")]
            &mut publish_rec,
            #[cfg(feature = "tick-to-trade")]
            &mut ingest_rec,
        )
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

        let disconnect = run_process_frames(&mut conn, &mut producer);
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
        // No ServerBusy on the wire — no Full happened.
        assert!(
            read_server_busy(&mut peer).is_none(),
            "ServerBusy must not be sent on the happy path"
        );
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

        let disconnect = run_process_frames(&mut conn, &mut producer);
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

        let disconnect = run_process_frames(&mut conn, &mut producer);
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
        //   * ServerBusy is written exactly once to the peer.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(4);
        for byte in [0x01, 0x02, 0x03, 0x04, 0x05, 0x06] {
            conn.parse_buf.extend_from_slice(&frame(byte));
        }

        let disconnect = run_process_frames(&mut conn, &mut producer);
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

        // ServerBusy is delivered to the peer.
        let busy = read_server_busy(&mut peer).expect("ServerBusy frame written");
        assert_eq!(busy, TEST_SERVER_BUSY);

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

    #[test]
    fn process_frames_oversize_commits_prior_frames_then_signals_disconnect() {
        // Two valid frames followed by an oversize length prefix must:
        //   * publish the two valid frames (commit-before-break) so the
        //     pipeline observes them even though we're about to tear the
        //     connection down,
        //   * return `true` so the caller drops the connection,
        //   * NOT write ServerBusy (that is reserved for pipeline-full).
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(16);
        conn.parse_buf.extend_from_slice(&frame(0x01));
        conn.parse_buf.extend_from_slice(&frame(0x02));
        conn.parse_buf.extend_from_slice(&oversize_prefix());

        let disconnect = run_process_frames(&mut conn, &mut producer);
        assert!(disconnect, "oversize frame must request disconnect");

        let events = drain(&mut consumer);
        assert_eq!(
            events.len(),
            2,
            "prior frames are committed before the break"
        );
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x01)));
        assert_eq!(events[1].1.event, JournalEvent::App(TestEvent::Cmd(0x02)));

        assert!(
            read_server_busy(&mut peer).is_none(),
            "ServerBusy is sent on Full, not on oversize"
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

        let disconnect = run_process_frames(&mut conn, &mut producer);
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

        let disconnect = run_process_frames(&mut conn, &mut producer);
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
        // documented zero-slot no-op; no ServerBusy; no disconnect.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(4);

        let disconnect = run_process_frames(&mut conn, &mut producer);
        assert!(!disconnect);
        assert_eq!(drain(&mut consumer).len(), 0);
        assert!(conn.parse_buf.is_empty());
        assert!(read_server_busy(&mut peer).is_none());
    }
}
