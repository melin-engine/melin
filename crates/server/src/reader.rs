//! Epoll-based multiplexed reader pool.
//!
//! Replaces the per-connection reader thread model (N threads for N connections)
//! with a small pool of reader threads (default 2), each using `epoll` to
//! multiplex a subset of connections. This eliminates thread oversubscription
//! (32 clients → 2 reader threads + 3 pipeline = 5 pinned threads) while
//! maintaining parallel I/O throughput. Reader threads are pinned to
//! dedicated cores (default 4-5) to avoid cache contention with the pipeline.
//!
//! Each connection's fd is set to `O_NONBLOCK` and registered with epoll in
//! edge-triggered mode. When data arrives, the reader performs incremental
//! (non-blocking) frame parsing: length prefix first, then payload. Complete
//! frames are decoded and published to the disruptor via `MultiProducer`.
//!
//! New connections are assigned round-robin across reader threads.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing::{debug, error};

use melin_disruptor::ring;
use melin_engine::journal::event::JournalEvent;
use melin_engine::journal::pipeline::InputSlot;
use melin_engine::journal::trace::trace_ts;
use melin_protocol::auth::Permission;
use melin_protocol::codec;
use melin_protocol::message::{ConnectionId, Request};

use crate::response::ControlEvent;

/// Maximum frame payload size (matches `BlockingFrameReader`).
const MAX_FRAME_SIZE: usize = 1024;

/// Maximum epoll events returned per `epoll_wait` call.
const MAX_EPOLL_EVENTS: usize = 64;

/// Sentinel token for the eventfd in epoll event data.
const EVENTFD_TOKEN: u64 = u64::MAX;

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

/// One reader thread's channel + wakeup fd.
struct ReaderThread<R> {
    tx: mpsc::Sender<ReaderRegistration<R>>,
    event_fd: RawFd,
}

/// Handle for the accept loop to register connections with the reader pool.
///
/// Distributes connections round-robin across reader threads.
pub struct EpollReaderHandle<R> {
    threads: Vec<ReaderThread<R>>,
    join_handles: Vec<JoinHandle<()>>,
    next: usize,
    shutdown: Arc<AtomicBool>,
}

impl<R> EpollReaderHandle<R> {
    /// Register a new connection with the next reader thread (round-robin).
    ///
    /// If the reader thread's channel is dead (thread panicked), logs an
    /// error and signals shutdown so the server can restart cleanly.
    pub fn register(&mut self, registration: ReaderRegistration<R>) {
        let idx = self.next % self.threads.len();
        self.next += 1;

        let thread = &self.threads[idx];
        if thread.tx.send(registration).is_ok() {
            // Signal the eventfd to wake the reader thread from epoll_wait.
            let val: u64 = 1;
            unsafe {
                libc::write(
                    thread.event_fd,
                    &val as *const u64 as *const libc::c_void,
                    8,
                );
            }
        } else {
            error!(
                thread = idx,
                "reader thread dead, cannot register connection"
            );
            self.shutdown.store(true, Ordering::Relaxed);
        }
    }

    /// Signal all reader threads to shut down and wake them from epoll_wait.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Wake each reader thread so it sees the shutdown flag.
        for thread in &self.threads {
            let val: u64 = 1;
            unsafe {
                libc::write(
                    thread.event_fd,
                    &val as *const u64 as *const libc::c_void,
                    8,
                );
            }
        }
    }

    /// Join all reader threads. Call after `shutdown()`.
    pub fn join(self) {
        for (i, handle) in self.join_handles.into_iter().enumerate() {
            if let Err(panic) = handle.join() {
                let msg = panic
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
                    .unwrap_or("<non-string panic>");
                error!(thread = i, message = msg, "reader thread panicked");
            }
        }
    }
}

/// Spawn a pool of epoll reader threads. Returns a handle for registering
/// connections via round-robin assignment.
///
/// Each reader thread has its own epoll instance and manages its own subset
/// of connections independently. `MultiProducer` is cloned per thread for
/// lock-free concurrent publishing.
pub fn spawn_reader_pool<R: AsRawFd + Send + 'static>(
    num_threads: usize,
    producer: ring::MultiProducer<InputSlot>,
    control_tx: mpsc::Sender<ControlEvent>,
    core_start: usize,
    connection_timeout: Option<Duration>,
    shutdown: Arc<AtomicBool>,
) -> EpollReaderHandle<R> {
    assert!(num_threads > 0, "need at least 1 reader thread");

    let mut threads = Vec::with_capacity(num_threads);
    let mut join_handles = Vec::with_capacity(num_threads);

    for i in 0..num_threads {
        let (tx, rx) = mpsc::channel();

        // Create eventfd for wakeup signaling (non-blocking).
        let event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        assert!(event_fd >= 0, "eventfd creation failed");

        let producer_clone = producer.clone();
        let control_tx_clone = control_tx.clone();
        let wakeup_fd = event_fd;
        let core_id = core_start + i;

        let timeout = connection_timeout;
        let shutdown_clone = Arc::clone(&shutdown);
        let handle = std::thread::Builder::new()
            .name(format!("reader-{i}"))
            .spawn(move || {
                match crate::affinity::pin_to_core(core_id) {
                    Ok(c) => tracing::info!(thread = "reader-{i}", core = c, "pinned to core"),
                    Err(e) => tracing::warn!(thread = "reader-{i}", core = core_id, error = %e, "failed to pin"),
                }
                epoll_reader_loop(rx, wakeup_fd, producer_clone, &control_tx_clone, timeout, &shutdown_clone);
            })
            .expect("failed to spawn reader thread");

        threads.push(ReaderThread { tx, event_fd });
        join_handles.push(handle);
    }

    EpollReaderHandle {
        threads,
        join_handles,
        next: 0,
        shutdown,
    }
}

/// Per-connection state for incremental (non-blocking) frame parsing.
struct ConnectionState<R> {
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
    /// 4-byte length prefix buffer.
    len_buf: [u8; 4],
    /// Bytes filled in `len_buf`.
    len_filled: usize,
    /// Frame payload buffer (fixed-size, avoids per-frame allocation).
    payload_buf: [u8; MAX_FRAME_SIZE],
    /// Expected payload length (parsed from length prefix).
    payload_len: usize,
    /// Bytes filled in `payload_buf`.
    payload_filled: usize,
    /// True when we've parsed the length prefix and are reading payload.
    reading_payload: bool,
    /// Last time any data was received on this connection. Used for
    /// idle timeout detection.
    last_activity: Instant,
}

/// Main epoll reader loop. Runs until the channel is disconnected.
fn epoll_reader_loop<R: AsRawFd>(
    command_rx: mpsc::Receiver<ReaderRegistration<R>>,
    wakeup_fd: RawFd,
    producer: ring::MultiProducer<InputSlot>,
    control_tx: &mpsc::Sender<ControlEvent>,
    connection_timeout: Option<Duration>,
    shutdown: &AtomicBool,
) {
    let epoll_fd = unsafe { libc::epoll_create1(0) };
    assert!(epoll_fd >= 0, "epoll_create1 failed");

    // Register the wakeup eventfd with epoll (edge-triggered).
    let mut ev = libc::epoll_event {
        events: (libc::EPOLLIN | libc::EPOLLET) as u32,
        u64: EVENTFD_TOKEN,
    };
    let ret = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, wakeup_fd, &mut ev) };
    assert!(ret == 0, "epoll_ctl add eventfd failed");

    // Pre-size for a reasonable number of concurrent connections per reader thread.
    let mut connections: HashMap<RawFd, ConnectionState<R>> = HashMap::with_capacity(256);
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; MAX_EPOLL_EVENTS];

    #[cfg(feature = "latency-trace")]
    let mut publish_hist = melin_engine::journal::trace::StageHistogram::new(
        "reader: publish (decode → disruptor publish)",
    );

    // epoll_wait timeout: 1000ms to periodically check the shutdown flag
    // and scan for stale connections. The eventfd wakeup provides immediate
    // responsiveness for new connections and shutdown signals.
    let epoll_timeout_ms: i32 = 1000;

    // Coarse gate for timeout scanning — avoids scanning on every
    // epoll_wait return during high throughput. Only scans when >=1 second
    // has elapsed since the last scan.
    let mut last_timeout_scan = Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let nfds = unsafe {
            libc::epoll_wait(
                epoll_fd,
                events.as_mut_ptr(),
                MAX_EPOLL_EVENTS as i32,
                epoll_timeout_ms,
            )
        };

        if nfds < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            error!(error = %err, "epoll_wait error");
            break;
        }

        // One Instant::now() per epoll batch for connection timeout tracking
        // instead of per frame — timeout is 30s, sub-ms precision is plenty.
        let batch_now = Instant::now();

        for event in &events[..nfds as usize] {
            let token = event.u64;

            if token == EVENTFD_TOKEN {
                // Drain the eventfd.
                let mut buf: u64 = 0;
                unsafe {
                    libc::read(wakeup_fd, &mut buf as *mut u64 as *mut libc::c_void, 8);
                }
                // Process pending registrations.
                while let Ok(reg) = command_rx.try_recv() {
                    register_connection(epoll_fd, reg, &mut connections);
                }
                continue;
            }

            // Connection fd is ready.
            let fd = token as RawFd;
            let disconnected = if let Some(conn) = connections.get_mut(&fd) {
                process_connection(
                    conn,
                    &producer,
                    batch_now,
                    #[cfg(feature = "latency-trace")]
                    &mut publish_hist,
                )
            } else {
                false
            };

            if disconnected {
                remove_connection(epoll_fd, fd, &mut connections, control_tx);
            }
        }

        // Scan for idle connections that have exceeded the timeout.
        // Coarse gate: only scan once per second to avoid unnecessary
        // iteration during high-throughput phases when epoll_wait returns
        // immediately with events.
        if let Some(timeout) = connection_timeout {
            let now = Instant::now();
            if now.duration_since(last_timeout_scan) >= Duration::from_secs(1) {
                last_timeout_scan = now;
                let stale_fds: Vec<RawFd> = connections
                    .iter()
                    .filter(|(_, conn)| now.duration_since(conn.last_activity) > timeout)
                    .map(|(&fd, _)| fd)
                    .collect();
                for fd in stale_fds {
                    if let Some(conn) = connections.get(&fd) {
                        debug!(
                            connection_id = conn.connection_id,
                            addr = %conn.addr,
                            "connection timed out"
                        );
                    }
                    remove_connection(epoll_fd, fd, &mut connections, control_tx);
                }
            }
        }
    }

    unsafe {
        libc::close(epoll_fd);
        libc::close(wakeup_fd);
    }

    #[cfg(feature = "latency-trace")]
    publish_hist.print_report();
}

/// Register a new connection: set non-blocking, add to epoll, store state.
fn register_connection<R: AsRawFd>(
    epoll_fd: RawFd,
    reg: ReaderRegistration<R>,
    connections: &mut HashMap<RawFd, ConnectionState<R>>,
) {
    let fd = reg.reader.as_raw_fd();

    // Set non-blocking.
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL);
        libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
    }

    // Register with epoll (edge-triggered).
    let mut conn_ev = libc::epoll_event {
        events: (libc::EPOLLIN | libc::EPOLLET) as u32,
        u64: fd as u64,
    };
    let ret = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut conn_ev) };
    if ret < 0 {
        debug!(
            connection_id = reg.connection_id.0,
            error = %io::Error::last_os_error(),
            "epoll_ctl add failed"
        );
        return;
    }

    connections.insert(
        fd,
        ConnectionState {
            connection_id: reg.connection_id.0,
            addr: reg.addr,
            permission: reg.permission,
            key_hash: reg.key_hash,
            _reader: reg.reader,
            fd,
            len_buf: [0u8; 4],
            len_filled: 0,
            payload_buf: [0u8; MAX_FRAME_SIZE],
            payload_len: 0,
            payload_filled: 0,
            reading_payload: false,
            last_activity: Instant::now(),
        },
    );
}

/// Remove a disconnected connection: deregister from epoll, notify response stage.
fn remove_connection<R>(
    epoll_fd: RawFd,
    fd: RawFd,
    connections: &mut HashMap<RawFd, ConnectionState<R>>,
    control_tx: &mpsc::Sender<ControlEvent>,
) {
    if let Some(conn) = connections.remove(&fd) {
        debug!(
            connection_id = conn.connection_id,
            addr = %conn.addr,
            "client disconnected"
        );
        unsafe {
            libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut());
        }
        // Best-effort: receiver may have shut down during server teardown.
        let _ = control_tx.send(ControlEvent::Disconnected {
            connection_id: conn.connection_id,
        });
    }
}

/// Process available data on a connection. Returns `true` if disconnected.
///
/// With edge-triggered epoll, we drain all available data (loop until EAGAIN).
fn process_connection<R>(
    conn: &mut ConnectionState<R>,
    producer: &ring::MultiProducer<InputSlot>,
    now: Instant,
    #[cfg(feature = "latency-trace")]
    publish_hist: &mut melin_engine::journal::trace::StageHistogram,
) -> bool {
    loop {
        // Step 1: Read 4-byte length prefix.
        if !conn.reading_payload {
            match nonblocking_read(conn.fd, &mut conn.len_buf, conn.len_filled, 4) {
                ReadResult::Complete(filled) => {
                    conn.len_filled = filled;
                    if filled < 4 {
                        return false; // EAGAIN
                    }
                    // Any successful read resets the idle timeout.
                    conn.last_activity = now;
                    let len = u32::from_le_bytes(conn.len_buf) as usize;
                    if len > MAX_FRAME_SIZE {
                        debug!(
                            connection_id = conn.connection_id,
                            addr = %conn.addr,
                            frame_len = len,
                            "frame too large, dropping connection"
                        );
                        return true;
                    }
                    conn.payload_len = len;
                    conn.payload_filled = 0;
                    conn.reading_payload = true;
                    conn.len_filled = 0;
                }
                ReadResult::Disconnected => return true,
                ReadResult::Error => return true,
            }
        }

        // Step 2: Read payload.
        if conn.reading_payload {
            match nonblocking_read(
                conn.fd,
                &mut conn.payload_buf,
                conn.payload_filled,
                conn.payload_len,
            ) {
                ReadResult::Complete(filled) => {
                    conn.payload_filled = filled;
                    if filled < conn.payload_len {
                        return false; // EAGAIN
                    }
                    conn.reading_payload = false;

                    let frame = &conn.payload_buf[..conn.payload_len];
                    let (seq, request) = match codec::decode_request(frame) {
                        Ok(pair) => pair,
                        Err(e) => {
                            debug!(
                                connection_id = conn.connection_id,
                                addr = %conn.addr,
                                error = %e,
                                "decode error"
                            );
                            continue;
                        }
                    };

                    // Heartbeat requests are keepalives — they reset
                    // last_activity (above) but must not enter the pipeline.
                    if matches!(request, Request::Heartbeat) {
                        continue;
                    }

                    // ChallengeResponse after auth is invalid — ignore.
                    if matches!(request, Request::ChallengeResponse { .. }) {
                        debug!(
                            connection_id = conn.connection_id,
                            "ChallengeResponse after auth, ignoring"
                        );
                        continue;
                    }

                    // Enforce permissions.
                    if request.requires_admin() && !conn.permission.is_admin() {
                        debug!(
                            connection_id = conn.connection_id,
                            "non-admin attempted admin command, dropping request"
                        );
                        continue;
                    }
                    if !request.requires_admin() && !conn.permission.can_trade() {
                        debug!(
                            connection_id = conn.connection_id,
                            "read-only connection attempted trade, dropping request"
                        );
                        continue;
                    }

                    #[allow(clippy::let_unit_value)]
                    let recv_ts = trace_ts();

                    let event = request_to_event(&request);

                    #[cfg(feature = "latency-trace")]
                    let pre_publish = trace_ts();

                    producer.publish(InputSlot {
                        connection_id: conn.connection_id,
                        key_hash: conn.key_hash,
                        request_seq: seq,
                        event,
                        publish_ts: trace_ts(),
                        recv_ts,
                    });

                    #[cfg(feature = "latency-trace")]
                    publish_hist.record_ns(melin_engine::journal::trace::trace_elapsed_ns(
                        pre_publish,
                        trace_ts(),
                    ));

                    // Edge-triggered: keep draining.
                    continue;
                }
                ReadResult::Disconnected => return true,
                ReadResult::Error => return true,
            }
        }
    }
}

/// Result of a non-blocking read attempt.
enum ReadResult {
    /// Read progressed to `filled` bytes. If `filled < target`, EAGAIN.
    Complete(usize),
    /// Peer disconnected (read returned 0).
    Disconnected,
    /// I/O error (not EAGAIN/EWOULDBLOCK).
    Error,
}

/// Non-blocking read into `buf[filled..target]`.
fn nonblocking_read(fd: RawFd, buf: &mut [u8], mut filled: usize, target: usize) -> ReadResult {
    while filled < target {
        let n = unsafe {
            libc::read(
                fd,
                buf[filled..target].as_mut_ptr() as *mut libc::c_void,
                target - filled,
            )
        };
        if n > 0 {
            filled += n as usize;
        } else if n == 0 {
            return ReadResult::Disconnected;
        } else {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::WouldBlock {
                return ReadResult::Complete(filled);
            }
            return ReadResult::Error;
        }
    }
    ReadResult::Complete(filled)
}

/// Convert a wire `Request` to a `JournalEvent` for the pipeline.
///
/// `Request::Heartbeat` is filtered out before reaching this function.
fn request_to_event(request: &Request) -> JournalEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => JournalEvent::SubmitOrder { symbol, order },
        Request::CancelOrder {
            symbol,
            account,
            order_id,
        } => JournalEvent::CancelOrder {
            symbol,
            account,
            order_id,
        },
        Request::CancelAll { account } => JournalEvent::CancelAll { account },
        Request::AddInstrument { spec } => JournalEvent::AddInstrument { spec },
        Request::Deposit {
            account,
            currency,
            amount,
        } => JournalEvent::Deposit {
            account,
            currency,
            amount,
        },
        Request::Withdraw {
            account,
            currency,
            amount,
        } => JournalEvent::Withdraw {
            account,
            currency,
            amount,
        },
        Request::SetRiskLimits { symbol, limits } => JournalEvent::SetRiskLimits { symbol, limits },
        Request::SetCircuitBreaker { symbol, config } => {
            JournalEvent::SetCircuitBreaker { symbol, config }
        }
        Request::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => JournalEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        },
        Request::SetFeeSchedule { symbol, schedule } => {
            JournalEvent::SetFeeSchedule { symbol, schedule }
        }
        Request::QueryStats => JournalEvent::QueryStats,
        Request::Heartbeat | Request::ChallengeResponse { .. } => {
            unreachable!("heartbeats and auth messages filtered before request_to_event")
        }
    }
}
