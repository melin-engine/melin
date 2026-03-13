//! Epoll-based multiplexed reader pool.
//!
//! Replaces the per-connection reader thread model (N threads for N connections)
//! with a small pool of reader threads (default 2), each using `epoll` to
//! multiplex a subset of connections. This eliminates thread oversubscription
//! (32 clients → 2 reader threads + 3 pipeline = 5 total) while maintaining
//! parallel I/O throughput.
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
use std::sync::mpsc;

use tracing::debug;

use trading_disruptor::ring;
use trading_engine::journal::event::JournalEvent;
use trading_engine::journal::pipeline::InputSlot;
use trading_engine::journal::trace::trace_ts;
use trading_protocol::codec;
use trading_protocol::message::{ConnectionId, Request};

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
    next: usize,
}

impl<R> EpollReaderHandle<R> {
    /// Register a new connection with the next reader thread (round-robin).
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
) -> EpollReaderHandle<R> {
    assert!(num_threads > 0, "need at least 1 reader thread");

    let mut threads = Vec::with_capacity(num_threads);

    for i in 0..num_threads {
        let (tx, rx) = mpsc::channel();

        // Create eventfd for wakeup signaling (non-blocking).
        let event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
        assert!(event_fd >= 0, "eventfd creation failed");

        let producer_clone = producer.clone();
        let control_tx_clone = control_tx.clone();
        let wakeup_fd = event_fd;

        std::thread::Builder::new()
            .name(format!("reader-{i}"))
            .spawn(move || {
                epoll_reader_loop(rx, wakeup_fd, producer_clone, &control_tx_clone);
            })
            .expect("failed to spawn reader thread");

        threads.push(ReaderThread { tx, event_fd });
    }

    EpollReaderHandle { threads, next: 0 }
}

/// Per-connection state for incremental (non-blocking) frame parsing.
struct ConnectionState<R> {
    connection_id: u64,
    addr: SocketAddr,
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
}

/// Main epoll reader loop. Runs until the channel is disconnected.
fn epoll_reader_loop<R: AsRawFd>(
    command_rx: mpsc::Receiver<ReaderRegistration<R>>,
    wakeup_fd: RawFd,
    producer: ring::MultiProducer<InputSlot>,
    control_tx: &mpsc::Sender<ControlEvent>,
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

    let mut connections: HashMap<RawFd, ConnectionState<R>> = HashMap::new();
    let mut events = vec![libc::epoll_event { events: 0, u64: 0 }; MAX_EPOLL_EVENTS];

    #[cfg(feature = "latency-trace")]
    let mut publish_hist = trading_engine::journal::trace::StageHistogram::new(
        "reader: publish (decode → disruptor publish)",
    );

    loop {
        let nfds =
            unsafe { libc::epoll_wait(epoll_fd, events.as_mut_ptr(), MAX_EPOLL_EVENTS as i32, -1) };

        if nfds < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            debug!(error = %err, "epoll_wait error");
            break;
        }

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
            _reader: reg.reader,
            fd,
            len_buf: [0u8; 4],
            len_filled: 0,
            payload_buf: [0u8; MAX_FRAME_SIZE],
            payload_len: 0,
            payload_filled: 0,
            reading_payload: false,
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
    #[cfg(feature = "latency-trace")]
    publish_hist: &mut trading_engine::journal::trace::StageHistogram,
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
                    let request = match codec::decode_request(frame) {
                        Ok(req) => req,
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

                    #[allow(clippy::let_unit_value)]
                    let recv_ts = trace_ts();

                    let event = request_to_event(&request);

                    #[cfg(feature = "latency-trace")]
                    let pre_publish = trace_ts();

                    producer.publish(InputSlot {
                        connection_id: conn.connection_id,
                        event,
                        publish_ts: trace_ts(),
                        recv_ts,
                    });

                    #[cfg(feature = "latency-trace")]
                    publish_hist.record_ns(trading_engine::journal::trace::trace_elapsed_ns(
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
fn request_to_event(request: &Request) -> JournalEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => JournalEvent::SubmitOrder { symbol, order },
        Request::CancelOrder { symbol, order_id } => JournalEvent::CancelOrder { symbol, order_id },
    }
}
