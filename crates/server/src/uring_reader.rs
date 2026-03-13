//! io_uring-based multiplexed reader.
//!
//! Replaces the epoll reader pool with io_uring RECV operations. Instead of
//! `epoll_wait` + non-blocking `read(2)` syscalls per connection, we submit
//! `IORING_OP_RECV` SQEs and get completions in batches via a single
//! `io_uring_enter` syscall. This eliminates the epoll→read double-syscall
//! overhead and lets the kernel batch completions.
//!
//! Uses a single reader thread — io_uring is efficient enough for hundreds
//! of connections. New connections are registered via eventfd wakeup, same
//! pattern as the epoll reader.
//!
//! Connection state is stored in a slab (index-stable Vec) so that io_uring
//! user_data carries a slab index, not an fd. This avoids fd-reuse races
//! where a recycled fd number could match a stale CQE.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::mpsc;

use io_uring::{IoUring, opcode, types};
use tracing::debug;

use trading_disruptor::ring;
use trading_engine::journal::event::JournalEvent;
use trading_engine::journal::pipeline::InputSlot;
use trading_engine::journal::trace::trace_ts;
use trading_protocol::codec;
use trading_protocol::message::{ConnectionId, Request};

use crate::uring_response::ControlEvent;

/// Size of the per-connection recv buffer. 4 KiB accommodates multiple
/// frames per recv (frames are typically <100 bytes), reducing the
/// number of RECV resubmissions.
const RECV_BUF_SIZE: usize = 4096;

/// Maximum frame payload size (matches `BlockingFrameReader`).
const MAX_FRAME_SIZE: usize = 1024;

/// io_uring submission queue depth. Power of 2, sized for hundreds of
/// connections (one RECV per connection + eventfd read).
const RING_SIZE: u32 = 512;

/// User data sentinel for the eventfd read SQE.
const EVENTFD_TOKEN: u64 = u64::MAX;

/// Command sent from the accept loop to the reader thread.
pub struct ReaderRegistration<R> {
    pub connection_id: ConnectionId,
    pub reader: R,
    pub addr: SocketAddr,
}

/// Handle for the accept loop to register connections with the io_uring reader.
pub struct UringReaderHandle<R> {
    tx: mpsc::Sender<ReaderRegistration<R>>,
    event_fd: RawFd,
}

impl<R> UringReaderHandle<R> {
    /// Register a new connection with the reader thread.
    pub fn register(&mut self, registration: ReaderRegistration<R>) {
        if self.tx.send(registration).is_ok() {
            // Signal the eventfd to wake the reader from io_uring_enter.
            let val: u64 = 1;
            unsafe {
                libc::write(self.event_fd, &val as *const u64 as *const libc::c_void, 8);
            }
        }
    }
}

/// Spawn the io_uring reader thread. Returns a handle for registering
/// connections.
///
/// Unlike the epoll reader pool, io_uring uses a single thread since
/// the ring efficiently batches I/O for hundreds of connections.
/// The `num_threads` parameter is accepted for API compatibility but
/// only one thread is spawned.
pub fn spawn_reader_pool<R: AsRawFd + Send + 'static>(
    _num_threads: usize,
    producer: ring::MultiProducer<InputSlot>,
    control_tx: mpsc::Sender<ControlEvent>,
) -> UringReaderHandle<R> {
    let (tx, rx) = mpsc::channel();

    let event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    assert!(event_fd >= 0, "eventfd creation failed");

    let wakeup_fd = event_fd;

    std::thread::Builder::new()
        .name("uring-reader".into())
        .spawn(move || {
            uring_reader_loop(rx, wakeup_fd, producer, &control_tx);
        })
        .expect("failed to spawn uring reader thread");

    UringReaderHandle { tx, event_fd }
}

// ---------------------------------------------------------------------------
// Slab-based connection storage
// ---------------------------------------------------------------------------

/// Per-connection state for io_uring recv + incremental frame parsing.
struct ConnectionEntry<R> {
    connection_id: u64,
    addr: SocketAddr,
    /// Owned reader — keeps the fd alive. Dropping closes the fd.
    _reader: R,
    fd: RawFd,
    /// Buffer for io_uring RECV. Kernel writes received bytes here.
    /// Boxed for pointer stability — the slab Vec may relocate entries,
    /// but the Box heap allocation stays fixed while a RECV SQE is in-flight.
    recv_buf: Box<[u8; RECV_BUF_SIZE]>,
    /// Accumulated bytes not yet parsed into complete frames.
    /// Grows when partial frames arrive, shrinks when frames are consumed.
    parse_buf: Vec<u8>,
    /// True if a RECV SQE is currently in-flight for this connection.
    recv_pending: bool,
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
fn uring_reader_loop<R: AsRawFd>(
    command_rx: mpsc::Receiver<ReaderRegistration<R>>,
    wakeup_fd: RawFd,
    producer: ring::MultiProducer<InputSlot>,
    control_tx: &mpsc::Sender<ControlEvent>,
) {
    let mut ring = IoUring::new(RING_SIZE).expect("failed to create io_uring instance");
    let mut slab = ConnectionSlab::<R>::new();
    // Reverse map for cleanup when a connection's fd needs removal.
    // HashMap for O(1) lookup by fd. Sized for typical connection counts.
    let mut fd_to_slab: HashMap<RawFd, usize> = HashMap::with_capacity(256);

    // Eventfd read buffer — boxed for pointer stability across SQE lifetimes.
    let mut eventfd_buf: Box<[u8; 8]> = Box::new([0u8; 8]);

    // Submit the initial eventfd read so we wake on first connection.
    push_eventfd_read(&mut ring, wakeup_fd, eventfd_buf.as_mut_ptr());

    #[cfg(feature = "latency-trace")]
    let mut publish_hist = trading_engine::journal::trace::StageHistogram::new(
        "reader: publish (decode → disruptor publish)",
    );

    loop {
        // Submit any pending SQEs and block until at least 1 CQE is ready.
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
                debug!(error = %e, "io_uring submit_and_wait error");
                break;
            }
        }

        // Drain all available CQEs. Must collect into a Vec because the CQ
        // borrow must end before we can push new SQEs to the SQ.
        let cqes: Vec<(u64, i32)> = ring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect();

        for (token, result) in cqes {
            if token == EVENTFD_TOKEN {
                if result >= 0 {
                    // Process all pending registrations.
                    while let Ok(reg) = command_rx.try_recv() {
                        let fd = reg.reader.as_raw_fd();
                        let entry = ConnectionEntry {
                            connection_id: reg.connection_id.0,
                            addr: reg.addr,
                            fd,
                            _reader: reg.reader,
                            recv_buf: Box::new([0u8; RECV_BUF_SIZE]),
                            parse_buf: Vec::with_capacity(MAX_FRAME_SIZE + 4),
                            recv_pending: false,
                        };
                        let idx = slab.insert(entry);
                        fd_to_slab.insert(fd, idx);

                        // Submit initial RECV for this connection.
                        push_recv(&mut ring, &mut slab, idx);
                    }
                } else {
                    debug!(error = result, "eventfd read error");
                }

                // Re-submit eventfd read for the next wakeup.
                push_eventfd_read(&mut ring, wakeup_fd, eventfd_buf.as_mut_ptr());
                continue;
            }

            // ── Connection RECV completion ──

            let slab_idx = token as usize;

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

            // Feed received bytes into the frame parser. We extract the
            // decision (remove or resubmit) and then act on it after
            // releasing the mutable slab borrow.
            let action = if let Some(entry) = slab.get_mut(slab_idx) {
                entry.recv_pending = false;

                // Append received bytes to the parse buffer.
                entry.parse_buf.extend_from_slice(&entry.recv_buf[..n]);

                // Extract and publish complete frames.
                let drop_conn = process_frames(
                    entry,
                    &producer,
                    #[cfg(feature = "latency-trace")]
                    &mut publish_hist,
                );
                if drop_conn {
                    Action::Remove {
                        connection_id: entry.connection_id,
                        fd: entry.fd,
                    }
                } else {
                    Action::Resubmit
                }
            } else {
                // Stale CQE for a removed connection — ignore.
                Action::None
            };

            match action {
                Action::Remove { connection_id, fd } => {
                    slab.remove(slab_idx);
                    fd_to_slab.remove(&fd);
                    let _ = control_tx.send(ControlEvent::Disconnected { connection_id });
                }
                Action::Resubmit => {
                    push_recv(&mut ring, &mut slab, slab_idx);
                }
                Action::None => {}
            }
        }
    }

    unsafe {
        libc::close(wakeup_fd);
    }

    #[cfg(feature = "latency-trace")]
    publish_hist.print_report();
}

/// What to do after processing a RECV CQE.
enum Action {
    /// Connection is healthy — resubmit RECV.
    Resubmit,
    /// Connection should be removed (malformed frame).
    Remove { connection_id: u64, fd: RawFd },
    /// Stale CQE — do nothing.
    None,
}

// ---------------------------------------------------------------------------
// SQE helpers
// ---------------------------------------------------------------------------

/// Push a RECV SQE for a connection. Does not submit — the caller batches
/// submissions via `submit_and_wait` at the top of the loop.
fn push_recv<R>(ring: &mut IoUring, slab: &mut ConnectionSlab<R>, idx: usize) {
    let entry = match slab.get_mut(idx) {
        Some(e) => e,
        None => return,
    };

    if entry.recv_pending {
        return;
    }

    let sqe = opcode::Recv::new(
        types::Fd(entry.fd),
        entry.recv_buf.as_mut_ptr(),
        RECV_BUF_SIZE as u32,
    )
    .build()
    .user_data(idx as u64);

    unsafe {
        ring.submission()
            .push(&sqe)
            .expect("io_uring SQ full — increase RING_SIZE");
    }
    entry.recv_pending = true;
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
fn process_frames<R>(
    conn: &mut ConnectionEntry<R>,
    producer: &ring::MultiProducer<InputSlot>,
    #[cfg(feature = "latency-trace")]
    publish_hist: &mut trading_engine::journal::trace::StageHistogram,
) -> bool {
    let mut cursor = 0;

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
            return true;
        }

        // Wait for the complete frame before parsing.
        if cursor + 4 + frame_len > conn.parse_buf.len() {
            break;
        }

        let frame = &conn.parse_buf[cursor + 4..cursor + 4 + frame_len];
        cursor += 4 + frame_len;

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
    }

    // Compact: remove consumed bytes from parse buffer.
    if cursor > 0 {
        conn.parse_buf.drain(..cursor);
    }

    false
}

/// Convert a wire `Request` to a `JournalEvent` for the pipeline.
fn request_to_event(request: &Request) -> JournalEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => JournalEvent::SubmitOrder { symbol, order },
        Request::CancelOrder { symbol, order_id } => JournalEvent::CancelOrder { symbol, order_id },
    }
}
