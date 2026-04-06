//! Single-threaded io_uring event loop for the FIX gateway.
//!
//! Multiplexes all FIX client connections and their corresponding Melin
//! server connections on a single io_uring ring. No threads, no mutexes,
//! no shared state — all session state is owned by the event loop thread.
//!
//! Uses multishot RECV with provided buffer groups (same pattern as
//! `melin-server`'s reader.rs) for efficient I/O multiplexing.

use std::collections::HashMap;
use std::net::TcpListener;
use std::os::unix::io::{IntoRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error, info};

use crate::config::GatewayConfig;
use crate::session::{Session, SessionState};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Size of each provided buffer. 4 KiB accommodates multiple FIX messages
/// per recv (FIX messages are typically ~200 bytes).
const BUF_SIZE: usize = 4096;

/// Number of provided buffers in the shared pool. 256 is ample for a
/// gateway handling ~100 FIX sessions (each with a Melin connection).
const NUM_BUFFERS: u16 = 256;

/// Buffer group ID for the provided recv buffer pool.
const BUF_GROUP_ID: u16 = 0;

/// io_uring submission queue depth. Sized for accept + multishot RECVs +
/// SENDs + connects + buffer re-provisions across ~100 sessions.
const RING_SIZE: u32 = 1024;

/// Heartbeat check interval. The event loop scans sessions for heartbeat
/// timeouts at this cadence.
const HEARTBEAT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// User data encoding
// ---------------------------------------------------------------------------

/// Operation types encoded in the upper byte of io_uring user_data.
const OP_ACCEPT: u64 = 0x00 << 56;
const OP_FIX_RECV: u64 = 0x01 << 56;
const OP_MELIN_RECV: u64 = 0x02 << 56;
const OP_SEND_FIX: u64 = 0x03 << 56;
const OP_SEND_MELIN: u64 = 0x04 << 56;
const OP_CONNECT: u64 = 0x05 << 56;
const OP_MASK: u64 = 0xFF << 56;
const IDX_MASK: u64 = 0x00FF_FFFF_FFFF_FFFF;

/// User data sentinel for ProvideBuffers CQEs.
const PROVIDE_BUFS_TOKEN: u64 = u64::MAX;

/// User data sentinel for the timeout SQE.
const TIMEOUT_TOKEN: u64 = u64::MAX - 1;

/// CQE flag: buffer ID is valid in upper 16 bits of flags.
const IORING_CQE_F_BUFFER: u32 = 1 << 0;

/// CQE flag: more completions coming from this multishot operation.
const IORING_CQE_F_MORE: u32 = 1 << 1;

/// Bit shift to extract buffer ID from CQE flags.
const IORING_CQE_BUFFER_SHIFT: u32 = 16;

#[inline(always)]
fn op_type(token: u64) -> u64 {
    token & OP_MASK
}

#[inline(always)]
fn slab_idx(token: u64) -> usize {
    (token & IDX_MASK) as usize
}

// ---------------------------------------------------------------------------
// Slab — index-stable session storage
// ---------------------------------------------------------------------------

/// Index-stable slab for session storage. io_uring user_data carries the
/// slab index, so entries must not move. Free indices are recycled via a
/// free list for O(1) insert/remove.
pub struct Slab {
    entries: Vec<Option<Session>>,
    /// Recycled indices for O(1) allocation.
    free: Vec<usize>,
}

impl Slab {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(64),
            free: Vec::new(),
        }
    }

    fn insert(&mut self, session: Session) -> usize {
        if let Some(idx) = self.free.pop() {
            self.entries[idx] = Some(session);
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(session));
            idx
        }
    }

    fn remove(&mut self, idx: usize) -> Option<Session> {
        if idx < self.entries.len() {
            let entry = self.entries[idx].take();
            if entry.is_some() {
                self.free.push(idx);
            }
            entry
        } else {
            None
        }
    }

    fn get(&self, idx: usize) -> Option<&Session> {
        self.entries.get(idx).and_then(|e| e.as_ref())
    }

    fn get_mut(&mut self, idx: usize) -> Option<&mut Session> {
        self.entries.get_mut(idx).and_then(|e| e.as_mut())
    }

    /// Iterate over all active sessions (index, session).
    fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut Session)> {
        self.entries
            .iter_mut()
            .enumerate()
            .filter_map(|(i, slot)| slot.as_mut().map(|s| (i, s)))
    }
}

// ---------------------------------------------------------------------------
// Gateway — main event loop state
// ---------------------------------------------------------------------------

pub struct Gateway {
    ring: IoUring,
    config: &'static GatewayConfig,
    listener_fd: RawFd,
    sessions: Slab,
    /// Contiguous buffer pool for io_uring provided buffers.
    buffer_pool: Box<[u8]>,
    /// Pre-allocated CQE drain buffer: (user_data, result, flags).
    cqes: Vec<(u64, i32, u32)>,
    /// Session indices to remove after CQE processing.
    to_remove: Vec<usize>,
    /// Sessions with pending outbound data to flush.
    dirty_fix: Vec<usize>,
    dirty_melin: Vec<usize>,
    /// Pre-built symbol lookup map.
    symbol_map: HashMap<String, crate::config::SymbolConfig>,
    /// Pre-built session lookup map: SenderCompID → session config index.
    session_map: HashMap<String, usize>,
    /// Coarse timer for heartbeat scanning.
    last_heartbeat_check: Instant,
}

impl Gateway {
    /// Create the gateway and register the listener + buffer pool with io_uring.
    pub fn new(
        listener: TcpListener,
        config: &'static GatewayConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut ring = IoUring::new(RING_SIZE)?;
        // Take ownership of the fd so it stays open for the program's
        // lifetime without leaking the TcpListener wrapper.
        let listener_fd = listener.into_raw_fd();

        // Register the provided buffer pool.
        let mut buffer_pool = vec![0u8; NUM_BUFFERS as usize * BUF_SIZE].into_boxed_slice();
        register_buffer_pool(&mut ring, buffer_pool.as_mut_ptr());

        // Build lookup maps.
        let symbol_map: HashMap<String, crate::config::SymbolConfig> = config
            .symbols
            .iter()
            .cloned()
            .map(|s| (s.fix_symbol.clone(), s))
            .collect();

        let session_map: HashMap<String, usize> = config
            .sessions
            .iter()
            .enumerate()
            .map(|(i, s)| (s.sender_comp_id.clone(), i))
            .collect();

        Ok(Self {
            ring,
            config,
            listener_fd,
            sessions: Slab::new(),
            buffer_pool,
            cqes: Vec::with_capacity(RING_SIZE as usize),
            to_remove: Vec::new(),
            dirty_fix: Vec::new(),
            dirty_melin: Vec::new(),
            symbol_map,
            session_map,
            last_heartbeat_check: Instant::now(),
        })
    }

    /// Run the event loop. Blocks until shutdown.
    pub fn run(&mut self, shutdown: &AtomicBool) -> Result<(), Box<dyn std::error::Error>> {
        // Submit the first ACCEPT.
        self.push_accept();

        info!("io_uring event loop started");

        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            // Submit pending SQEs and wait for at least 1 CQE.
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => {
                    error!(error = %e, "io_uring submit_and_wait error");
                    break;
                }
            }

            // Drain all CQEs into pre-allocated buffer. Must collect before
            // processing because the CQ borrow must end before pushing SQEs.
            self.cqes.clear();
            self.cqes.extend(
                self.ring
                    .completion()
                    .map(|cqe| (cqe.user_data(), cqe.result(), cqe.flags())),
            );

            let now = Instant::now();

            for i in 0..self.cqes.len() {
                let (token, result, flags) = self.cqes[i];

                // Sentinel tokens.
                if token == PROVIDE_BUFS_TOKEN {
                    if result < 0 {
                        error!(error = result, "ProvideBuffers failed");
                    }
                    continue;
                }
                if token == TIMEOUT_TOKEN {
                    // Timeout expired — handled below in heartbeat check.
                    continue;
                }

                match op_type(token) {
                    OP_ACCEPT => self.handle_accept(result, now),
                    OP_FIX_RECV => self.handle_fix_recv(slab_idx(token), result, flags, now),
                    OP_MELIN_RECV => {
                        self.handle_melin_recv(slab_idx(token), result, flags, now);
                    }
                    OP_SEND_FIX => self.handle_fix_send_complete(slab_idx(token), result),
                    OP_SEND_MELIN => self.handle_melin_send_complete(slab_idx(token), result),
                    OP_CONNECT => self.handle_melin_connected(slab_idx(token), result, now),
                    _ => {
                        debug!(token, "unknown op type in CQE");
                    }
                }
            }

            // Flush pending outbound data before removing sessions,
            // so closing sessions can send their final messages
            // (e.g., FIX Logout).
            self.flush_dirty_sends();

            // Remove sessions that are safe to remove (no in-flight
            // SEND SQEs referencing session buffers). Sessions with
            // pending sends are deferred until the send completes.
            self.drain_removals();

            // Periodic heartbeat check.
            if now.duration_since(self.last_heartbeat_check) >= HEARTBEAT_CHECK_INTERVAL {
                self.check_heartbeats(now);
                self.last_heartbeat_check = now;
            }
        }

        info!("io_uring event loop stopped");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ACCEPT
    // -----------------------------------------------------------------------

    fn push_accept(&mut self) {
        let sqe = opcode::Accept::new(
            types::Fd(self.listener_fd),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
        .build()
        .user_data(OP_ACCEPT);

        unsafe {
            self.ring
                .submission()
                .push(&sqe)
                .expect("io_uring SQ full during accept");
        }
    }

    fn handle_accept(&mut self, result: i32, now: Instant) {
        // Always resubmit ACCEPT for the next connection.
        self.push_accept();

        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            debug!(error = %err, "accept failed");
            return;
        }

        let fd = result;

        // Set TCP_NODELAY on the new socket.
        set_tcp_nodelay(fd);

        let peer = get_peer_addr(fd);
        info!(peer = %peer, fd, "FIX client connected");

        // Create a new session in AwaitingLogon state.
        let session = Session::new(fd, now);
        let idx = self.sessions.insert(session);

        // Submit multishot RECV on the FIX client socket.
        self.push_fix_recv_multi(idx);
    }

    // -----------------------------------------------------------------------
    // FIX RECV
    // -----------------------------------------------------------------------

    fn push_fix_recv_multi(&mut self, idx: usize) {
        let session = match self.sessions.get_mut(idx) {
            Some(s) => s,
            None => return,
        };
        if session.fix_multishot_active {
            return;
        }

        let sqe = opcode::RecvMulti::new(types::Fd(session.fix_fd), BUF_GROUP_ID)
            .build()
            .user_data(OP_FIX_RECV | idx as u64);

        unsafe {
            self.ring.submission().push(&sqe).expect("io_uring SQ full");
        }
        session.fix_multishot_active = true;
    }

    fn handle_fix_recv(&mut self, idx: usize, result: i32, flags: u32, now: Instant) {
        let has_more = (flags & IORING_CQE_F_MORE) != 0;

        if result <= 0 {
            // Disconnect or error.
            if let Some(session) = self.sessions.get(idx) {
                if result == 0 {
                    debug!(sender = %session.sender_comp_id, "FIX client disconnected");
                } else {
                    debug!(sender = %session.sender_comp_id, error = result, "FIX recv error");
                }
            }
            self.to_remove.push(idx);
            return;
        }

        let n = result as usize;
        let buf_id = if (flags & IORING_CQE_F_BUFFER) != 0 {
            (flags >> IORING_CQE_BUFFER_SHIFT) as usize
        } else {
            debug!(idx, "FIX recv CQE without buffer flag");
            return;
        };

        // Copy received bytes from pool into session's parse buffer.
        let buf_start = buf_id * BUF_SIZE;
        let data = &self.buffer_pool[buf_start..buf_start + n];

        if let Some(session) = self.sessions.get_mut(idx) {
            if !has_more {
                session.fix_multishot_active = false;
            }
            session.last_fix_recv = now;
            session.fix_parse_buf.extend_from_slice(data);
        }

        // Re-provide the consumed buffer back to the pool.
        self.re_provide_buffer(buf_id);

        // Process complete FIX messages.
        self.process_fix_messages(idx);

        // Restart multishot if it was terminated (buffer pool exhaustion).
        if !has_more {
            self.push_fix_recv_multi(idx);
        }
    }

    fn process_fix_messages(&mut self, idx: usize) {
        // We need to extract messages in a loop. Each message may trigger
        // state transitions that produce outbound data.
        loop {
            let session = match self.sessions.get_mut(idx) {
                Some(s) => s,
                None => return,
            };

            let raw = match crate::fix::parse::try_extract_message(&mut session.fix_parse_buf) {
                Some(raw) => raw,
                None => return, // No complete message yet.
            };

            // Dispatch based on session state.
            let action =
                session.handle_fix_message(&raw, self.config, &self.session_map, &self.symbol_map);

            match action {
                SessionAction::None => {}
                SessionAction::ConnectMelin => {
                    self.start_melin_connect(idx);
                }
                SessionAction::SendFix => {
                    self.dirty_fix.push(idx);
                }
                SessionAction::SendMelin => {
                    self.dirty_melin.push(idx);
                }
                SessionAction::SendBoth => {
                    self.dirty_fix.push(idx);
                    self.dirty_melin.push(idx);
                }
                SessionAction::Close => {
                    // Send any pending data, then remove.
                    self.dirty_fix.push(idx);
                    self.to_remove.push(idx);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // MELIN CONNECT
    // -----------------------------------------------------------------------

    fn start_melin_connect(&mut self, idx: usize) {
        let server_addr = self.config.server_addr;

        // Create a non-blocking TCP socket.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0) };
        if fd < 0 {
            error!(error = fd, "socket() failed for Melin connection");
            self.to_remove.push(idx);
            return;
        }

        set_tcp_nodelay(fd);

        if let Some(session) = self.sessions.get_mut(idx) {
            session.melin_fd = Some(fd);
        }

        // Build sockaddr for the connect SQE.
        let sockaddr = socket_addr_to_sockaddr(server_addr);
        let sockaddr_len = std::mem::size_of::<libc::sockaddr_in>() as u32;

        // Store the sockaddr in the session so it lives long enough for
        // io_uring to read it.
        if let Some(session) = self.sessions.get_mut(idx) {
            session.connect_addr = Some(sockaddr);
        }

        let session = self.sessions.get(idx).unwrap();
        let addr_ptr = session.connect_addr.as_ref().unwrap() as *const libc::sockaddr_in
            as *const libc::sockaddr;

        let sqe = opcode::Connect::new(types::Fd(fd), addr_ptr, sockaddr_len)
            .build()
            .user_data(OP_CONNECT | idx as u64);

        unsafe {
            self.ring.submission().push(&sqe).expect("io_uring SQ full");
        }
    }

    fn handle_melin_connected(&mut self, idx: usize, result: i32, now: Instant) {
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            if let Some(session) = self.sessions.get(idx) {
                error!(
                    sender = %session.sender_comp_id,
                    error = %err,
                    "Melin connect failed"
                );
            }
            self.to_remove.push(idx);
            return;
        }

        if let Some(session) = self.sessions.get_mut(idx) {
            info!(
                sender = %session.sender_comp_id,
                "connected to Melin server"
            );
            session.on_melin_connected(now);
        }

        // Start multishot RECV on the Melin socket to receive the Challenge.
        self.push_melin_recv_multi(idx);
    }

    // -----------------------------------------------------------------------
    // MELIN RECV
    // -----------------------------------------------------------------------

    fn push_melin_recv_multi(&mut self, idx: usize) {
        let session = match self.sessions.get_mut(idx) {
            Some(s) => s,
            None => return,
        };
        let melin_fd = match session.melin_fd {
            Some(fd) => fd,
            None => return,
        };
        if session.melin_multishot_active {
            return;
        }

        let sqe = opcode::RecvMulti::new(types::Fd(melin_fd), BUF_GROUP_ID)
            .build()
            .user_data(OP_MELIN_RECV | idx as u64);

        unsafe {
            self.ring.submission().push(&sqe).expect("io_uring SQ full");
        }
        session.melin_multishot_active = true;
    }

    fn handle_melin_recv(&mut self, idx: usize, result: i32, flags: u32, now: Instant) {
        let has_more = (flags & IORING_CQE_F_MORE) != 0;

        if result <= 0 {
            if let Some(session) = self.sessions.get(idx) {
                if result == 0 {
                    info!(sender = %session.sender_comp_id, "Melin server disconnected");
                } else {
                    debug!(sender = %session.sender_comp_id, error = result, "Melin recv error");
                }
            }
            self.to_remove.push(idx);
            return;
        }

        let n = result as usize;
        let buf_id = if (flags & IORING_CQE_F_BUFFER) != 0 {
            (flags >> IORING_CQE_BUFFER_SHIFT) as usize
        } else {
            debug!(idx, "Melin recv CQE without buffer flag");
            return;
        };

        // Copy from pool into session's Melin parse buffer.
        let buf_start = buf_id * BUF_SIZE;
        let data = &self.buffer_pool[buf_start..buf_start + n];

        if let Some(session) = self.sessions.get_mut(idx) {
            if !has_more {
                session.melin_multishot_active = false;
            }
            session.melin_parse_buf.extend_from_slice(data);
        }

        // Re-provide the consumed buffer.
        self.re_provide_buffer(buf_id);

        // Process complete Melin frames.
        self.process_melin_frames(idx, now);

        // Restart multishot if terminated.
        if !has_more {
            self.push_melin_recv_multi(idx);
        }
    }

    fn process_melin_frames(&mut self, idx: usize, now: Instant) {
        loop {
            let session = match self.sessions.get_mut(idx) {
                Some(s) => s,
                None => return,
            };

            let action = session.try_process_melin_frame(self.config, &self.symbol_map, now);

            match action {
                SessionAction::None => return, // No complete frame or nothing to do.
                SessionAction::SendFix => {
                    self.dirty_fix.push(idx);
                }
                SessionAction::SendMelin => {
                    self.dirty_melin.push(idx);
                }
                SessionAction::SendBoth => {
                    self.dirty_fix.push(idx);
                    self.dirty_melin.push(idx);
                }
                SessionAction::Close => {
                    self.dirty_fix.push(idx);
                    self.to_remove.push(idx);
                    return;
                }
                SessionAction::ConnectMelin => {
                    // Should not happen from Melin recv path.
                    debug!(idx, "unexpected ConnectMelin from Melin recv");
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // SEND
    // -----------------------------------------------------------------------

    fn flush_dirty_sends(&mut self) {
        // Dedup: multiple messages processed in one CQE batch can push
        // the same session index, causing redundant send SQEs.
        self.dirty_fix.sort_unstable();
        self.dirty_fix.dedup();
        self.dirty_melin.sort_unstable();
        self.dirty_melin.dedup();

        // Flush FIX outbound.
        let fix_dirty: Vec<usize> = self.dirty_fix.drain(..).collect();
        for idx in fix_dirty {
            let session = match self.sessions.get_mut(idx) {
                Some(s) => s,
                None => continue,
            };

            if !session.fix_inflight.is_empty() {
                // Previous send partially completed — resubmit the
                // remaining bytes. The buffer is stable (untouched
                // since the partial CQE).
            } else if !session.fix_send_buf.is_empty() {
                // New data: swap send_buf into inflight so the buffer
                // is stable while the kernel reads it. New messages
                // that arrive while the send is in flight will append
                // to send_buf (now empty) without disturbing inflight.
                std::mem::swap(&mut session.fix_send_buf, &mut session.fix_inflight);
            } else {
                continue;
            }

            let sqe = opcode::Send::new(
                types::Fd(session.fix_fd),
                session.fix_inflight.as_ptr(),
                session.fix_inflight.len() as u32,
            )
            .build()
            .user_data(OP_SEND_FIX | idx as u64);

            unsafe {
                self.ring.submission().push(&sqe).expect("io_uring SQ full");
            }
        }

        // Flush Melin outbound.
        let melin_dirty: Vec<usize> = self.dirty_melin.drain(..).collect();
        for idx in melin_dirty {
            let session = match self.sessions.get_mut(idx) {
                Some(s) => s,
                None => continue,
            };
            let melin_fd = match session.melin_fd {
                Some(fd) => fd,
                None => continue,
            };

            if !session.melin_inflight.is_empty() {
                // Partial send remainder — resubmit.
            } else if !session.melin_send_buf.is_empty() {
                std::mem::swap(&mut session.melin_send_buf, &mut session.melin_inflight);
            } else {
                continue;
            }

            let sqe = opcode::Send::new(
                types::Fd(melin_fd),
                session.melin_inflight.as_ptr(),
                session.melin_inflight.len() as u32,
            )
            .build()
            .user_data(OP_SEND_MELIN | idx as u64);

            unsafe {
                self.ring.submission().push(&sqe).expect("io_uring SQ full");
            }
        }
    }

    fn handle_fix_send_complete(&mut self, idx: usize, result: i32) {
        if result < 0 {
            if let Some(session) = self.sessions.get(idx) {
                debug!(sender = %session.sender_comp_id, error = result, "FIX send error");
            }
            self.to_remove.push(idx);
            return;
        }

        let sent = result as usize;
        let (needs_requeue, needs_remove) = match self.sessions.get_mut(idx) {
            Some(session) => {
                if sent >= session.fix_inflight.len() {
                    session.fix_inflight.clear();
                } else {
                    session.fix_inflight.drain(..sent);
                }
                let requeue = !session.fix_inflight.is_empty() || !session.fix_send_buf.is_empty();
                // If this Closing session has no more data to send on
                // either side, it can be removed.
                let remove = !requeue
                    && matches!(session.state, SessionState::Closing)
                    && session.melin_inflight.is_empty();
                (requeue, remove)
            }
            None => (false, false),
        };
        if needs_requeue {
            self.dirty_fix.push(idx);
        }
        if needs_remove {
            self.to_remove.push(idx);
        }
    }

    fn handle_melin_send_complete(&mut self, idx: usize, result: i32) {
        if result < 0 {
            if let Some(session) = self.sessions.get(idx) {
                debug!(sender = %session.sender_comp_id, error = result, "Melin send error");
            }
            self.to_remove.push(idx);
            return;
        }

        let sent = result as usize;
        let (needs_requeue, needs_remove) = match self.sessions.get_mut(idx) {
            Some(session) => {
                if sent >= session.melin_inflight.len() {
                    session.melin_inflight.clear();
                } else {
                    session.melin_inflight.drain(..sent);
                }
                let requeue =
                    !session.melin_inflight.is_empty() || !session.melin_send_buf.is_empty();
                let remove = !requeue
                    && matches!(session.state, SessionState::Closing)
                    && session.fix_inflight.is_empty();
                (requeue, remove)
            }
            None => (false, false),
        };
        if needs_requeue {
            self.dirty_melin.push(idx);
        }
        if needs_remove {
            self.to_remove.push(idx);
        }
    }

    /// Remove sessions from the slab, deferring any that still have
    /// io_uring SEND SQEs in flight (their inflight buffers are
    /// non-empty and the kernel may still be reading from them).
    fn drain_removals(&mut self) {
        let pending: Vec<usize> = self.to_remove.drain(..).collect();
        for idx in pending {
            let can_remove = self
                .sessions
                .get(idx)
                .is_none_or(|s| s.fix_inflight.is_empty() && s.melin_inflight.is_empty());
            if can_remove {
                if let Some(session) = self.sessions.remove(idx) {
                    debug!(sender = %session.sender_comp_id, "session removed");
                }
            } else {
                // Sends still in flight — mark as Closing so the send
                // completion handler will schedule removal once the
                // kernel is done with the buffers.
                if let Some(session) = self.sessions.get_mut(idx)
                    && !matches!(session.state, SessionState::Closing)
                {
                    session.state = SessionState::Closing;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Heartbeat
    // -----------------------------------------------------------------------

    fn check_heartbeats(&mut self, now: Instant) {
        // Collect actions first — can't borrow self.sessions and push
        // to self.dirty_fix/to_remove at the same time.
        let mut actions: Vec<(usize, SessionAction)> = Vec::new();

        for (idx, session) in self.sessions.iter_mut() {
            let action = session.check_heartbeat(now, self.config);
            if !matches!(action, SessionAction::None) {
                actions.push((idx, action));
            }
        }

        for (idx, action) in actions {
            match action {
                SessionAction::SendFix => self.dirty_fix.push(idx),
                SessionAction::Close => {
                    self.dirty_fix.push(idx);
                    self.to_remove.push(idx);
                }
                _ => {}
            }
        }
    }

    // -----------------------------------------------------------------------
    // Buffer pool
    // -----------------------------------------------------------------------

    fn re_provide_buffer(&mut self, buf_id: usize) {
        let buf_ptr = unsafe { self.buffer_pool.as_mut_ptr().add(buf_id * BUF_SIZE) };
        let sqe =
            opcode::ProvideBuffers::new(buf_ptr, BUF_SIZE as i32, 1, BUF_GROUP_ID, buf_id as u16)
                .build()
                .user_data(PROVIDE_BUFS_TOKEN);

        unsafe {
            self.ring.submission().push(&sqe).expect("io_uring SQ full");
        }
    }
}

// ---------------------------------------------------------------------------
// Actions returned by session message handlers
// ---------------------------------------------------------------------------

/// Actions the event loop should take after a session processes a message.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub enum SessionAction {
    /// No I/O needed.
    None,
    /// Initiate Melin TCP connect.
    ConnectMelin,
    /// Flush FIX send buffer.
    SendFix,
    /// Flush Melin send buffer.
    SendMelin,
    /// Flush both send buffers.
    SendBoth,
    /// Send pending data and close the session.
    Close,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

    let cqe = ring
        .completion()
        .next()
        .expect("no CQE after ProvideBuffers");
    assert!(cqe.result() >= 0, "ProvideBuffers failed: {}", cqe.result());
}

fn set_tcp_nodelay(fd: RawFd) {
    let val: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of_val(&val) as libc::socklen_t,
        );
    }
}

fn get_peer_addr(fd: RawFd) -> String {
    let mut addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
    let mut len: libc::socklen_t = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let rc = unsafe { libc::getpeername(fd, &mut addr as *mut _ as *mut libc::sockaddr, &mut len) };
    if rc != 0 {
        return "unknown".to_string();
    }

    match addr.ss_family as libc::c_int {
        libc::AF_INET => {
            let sa = unsafe { &*(&addr as *const _ as *const libc::sockaddr_in) };
            let ip = std::net::Ipv4Addr::from(u32::from_be(sa.sin_addr.s_addr));
            let port = u16::from_be(sa.sin_port);
            format!("{ip}:{port}")
        }
        libc::AF_INET6 => {
            let sa = unsafe { &*(&addr as *const _ as *const libc::sockaddr_in6) };
            let ip = std::net::Ipv6Addr::from(sa.sin6_addr.s6_addr);
            let port = u16::from_be(sa.sin6_port);
            format!("[{ip}]:{port}")
        }
        _ => "unknown".to_string(),
    }
}

fn socket_addr_to_sockaddr(addr: std::net::SocketAddr) -> libc::sockaddr_in {
    match addr {
        std::net::SocketAddr::V4(v4) => {
            let mut sa: libc::sockaddr_in = unsafe { std::mem::zeroed() };
            sa.sin_family = libc::AF_INET as libc::sa_family_t;
            sa.sin_port = v4.port().to_be();
            sa.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            sa
        }
        std::net::SocketAddr::V6(_) => {
            // The gateway config uses SocketAddr which can be v4 or v6.
            // For now, only v4 Melin server addresses are supported.
            panic!("IPv6 Melin server addresses not yet supported");
        }
    }
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fix::parse::FixMessage;
    use crate::fix::serialize::FixMessageBuilder;
    use crate::fix::tags;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::thread::JoinHandle;

    // -----------------------------------------------------------------------
    // Scaffolding
    // -----------------------------------------------------------------------

    /// Write a deterministic 32-byte Ed25519 seed to a unique temp path
    /// and return the path. Leaks at process exit.
    fn make_key_file() -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("melin-fix-it-key-{pid}-{n}.bin"));
        std::fs::write(&path, [0xABu8; 32]).unwrap();
        path
    }

    /// Build and leak a `GatewayConfig` for the lifetime of the test
    /// process. The listen_addr is a placeholder — `Gateway::new` takes
    /// the `TcpListener` directly and never reads this field.
    fn make_config(sender: &str, target: &str) -> &'static GatewayConfig {
        let key_path = make_key_file();
        let toml = format!(
            r#"
server_addr = "127.0.0.1:1"
listen_addr = "127.0.0.1:1"
target_comp_id = "{target}"

[[session]]
sender_comp_id = "{sender}"
account_id = 7
key_path = "{}"

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 1
tick_size_inverse = 100
lot_size_inverse = 1
"#,
            key_path.display()
        );
        let config: GatewayConfig = toml::from_str(&toml).unwrap();
        Box::leak(Box::new(config))
    }

    fn logon_bytes(sender: &str, target: &str, seq: u64) -> Vec<u8> {
        FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .str_tag(tags::HEART_BT_INT, "30")
            .build(sender, target, seq)
    }

    /// Handle wrapping a running gateway thread. Shutting down requires
    /// waking the event loop from `submit_and_wait(1)` — we do that by
    /// opening a short-lived dummy connection that fires an Accept CQE.
    struct GwHandle {
        port: u16,
        shutdown: Arc<AtomicBool>,
        join: Option<JoinHandle<()>>,
    }
    impl GwHandle {
        fn shutdown(mut self) {
            self.shutdown.store(true, Ordering::Relaxed);
            // Wake the blocked submit_and_wait.
            let _ = TcpStream::connect(("127.0.0.1", self.port));
            if let Some(j) = self.join.take() {
                j.join().expect("gateway thread panicked");
            }
        }
    }
    impl Drop for GwHandle {
        fn drop(&mut self) {
            if let Some(j) = self.join.take() {
                self.shutdown.store(true, Ordering::Relaxed);
                let _ = TcpStream::connect(("127.0.0.1", self.port));
                let _ = j.join();
            }
        }
    }

    fn init_tracing() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(
                    tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug")),
                )
                .with_test_writer()
                .try_init();
        });
    }

    fn spawn_gateway(config: &'static GatewayConfig) -> GwHandle {
        init_tracing();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let join = std::thread::spawn(move || {
            let mut gw = Gateway::new(listener, config).expect("gateway new");
            gw.run(&shutdown_clone).expect("gateway run");
        });
        GwHandle {
            port,
            shutdown,
            join: Some(join),
        }
    }

    /// Read one complete FIX message from a TCP stream with a timeout.
    /// Uses `try_extract_message` to frame.
    fn read_fix_message(stream: &mut TcpStream) -> Vec<u8> {
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut buf = Vec::with_capacity(256);
        let mut tmp = [0u8; 256];
        loop {
            if let Some(msg) = crate::fix::parse::try_extract_message(&mut buf) {
                return msg;
            }
            match stream.read(&mut tmp) {
                Ok(0) => panic!(
                    "unexpected EOF before complete FIX message (got {} bytes so far)",
                    buf.len()
                ),
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(e) => panic!("read error waiting for FIX message: {e}"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    #[test]
    fn unknown_sender_gets_logout_and_disconnects() {
        let config = make_config("FIRM_A", "MELIN");
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();

        let logon = logon_bytes("UNKNOWN_FIRM", "MELIN", 1);
        client.write_all(&logon).unwrap();

        // Server should send a Logout and then close the connection.
        let raw = read_fix_message(&mut client);
        let msg = FixMessage::parse(&raw).expect("valid FIX Logout");
        assert_eq!(msg.msg_type(), tags::MSG_LOGOUT);

        // Next read should return EOF (server closed).
        let mut tail = [0u8; 64];
        let n = client.read(&mut tail).expect("final read");
        assert_eq!(n, 0, "expected EOF after Logout");

        drop(client);
        gw.shutdown();
    }

    #[test]
    fn non_logon_first_message_is_rejected() {
        let config = make_config("FIRM_A", "MELIN");
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();

        // Send a Heartbeat as the first message — must be Logon.
        let hb = FixMessageBuilder::new(tags::MSG_HEARTBEAT).build("FIRM_A", "MELIN", 1);
        client.write_all(&hb).unwrap();

        let raw = read_fix_message(&mut client);
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_LOGOUT);

        let mut tail = [0u8; 64];
        assert_eq!(client.read(&mut tail).unwrap(), 0);

        drop(client);
        gw.shutdown();
    }

    #[test]
    fn garbage_first_bytes_close_connection() {
        let config = make_config("FIRM_A", "MELIN");
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();

        // Bytes that look like a complete (but invalid) FIX message so
        // try_extract_message frames them — checksum validation then
        // rejects them and the gateway closes the socket.
        // Minimal shape: 8=FIX.4.2\x019=5\x0135=0\x0110=000\x01
        client
            .write_all(b"8=FIX.4.2\x019=5\x0135=0\x0110=000\x01")
            .unwrap();

        // Gateway should close without sending anything (malformed
        // Logon never produces a Logout — see handle_logon).
        let mut buf = [0u8; 64];
        let n = client.read(&mut buf).expect("read after garbage");
        assert_eq!(n, 0, "expected EOF for malformed Logon");

        drop(client);
        gw.shutdown();
    }

    #[test]
    fn two_concurrent_clients_each_get_logout() {
        let config = make_config("FIRM_A", "MELIN");
        let gw = spawn_gateway(config);

        let mut c1 = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        let mut c2 = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        c1.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        c2.set_read_timeout(Some(Duration::from_secs(3))).unwrap();

        c1.write_all(&logon_bytes("UNKNOWN_A", "MELIN", 1)).unwrap();
        c2.write_all(&logon_bytes("UNKNOWN_B", "MELIN", 1)).unwrap();

        let raw1 = read_fix_message(&mut c1);
        let raw2 = read_fix_message(&mut c2);
        let m1 = FixMessage::parse(&raw1).unwrap();
        let m2 = FixMessage::parse(&raw2).unwrap();
        assert_eq!(m1.msg_type(), tags::MSG_LOGOUT);
        assert_eq!(m2.msg_type(), tags::MSG_LOGOUT);

        // Both should EOF independently.
        let mut tail = [0u8; 16];
        assert_eq!(c1.read(&mut tail).unwrap(), 0);
        assert_eq!(c2.read(&mut tail).unwrap(), 0);

        drop(c1);
        drop(c2);
        gw.shutdown();
    }
}
