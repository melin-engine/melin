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
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error, info, warn};

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
const OP_SEND: u64 = 0x03 << 56;
const OP_CONNECT: u64 = 0x04 << 56;
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
        let listener_fd = listener.as_raw_fd();

        // Leak the listener to keep the fd alive for the program's lifetime.
        std::mem::forget(listener);

        // Register the provided buffer pool.
        let mut buffer_pool =
            vec![0u8; NUM_BUFFERS as usize * BUF_SIZE].into_boxed_slice();
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
                    OP_SEND => self.handle_send_complete(slab_idx(token), result),
                    OP_CONNECT => self.handle_melin_connected(slab_idx(token), result, now),
                    _ => {
                        debug!(token, "unknown op type in CQE");
                    }
                }
            }

            // Remove sessions marked for cleanup.
            for idx in self.to_remove.drain(..) {
                if let Some(session) = self.sessions.remove(idx) {
                    debug!(
                        sender = %session.sender_comp_id,
                        "session removed"
                    );
                    // Fds are closed when the OwnedFd fields in Session drop.
                }
            }

            // Flush pending outbound data.
            self.flush_dirty_sends();

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
        let sqe = opcode::Accept::new(types::Fd(self.listener_fd), std::ptr::null_mut(), std::ptr::null_mut())
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
            self.ring
                .submission()
                .push(&sqe)
                .expect("io_uring SQ full");
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
            let action = session.handle_fix_message(
                &raw,
                self.config,
                &self.session_map,
                &self.symbol_map,
            );

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
        let fd = unsafe {
            libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0)
        };
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
        let addr_ptr = session.connect_addr.as_ref().unwrap()
            as *const libc::sockaddr_in as *const libc::sockaddr;

        let sqe = opcode::Connect::new(types::Fd(fd), addr_ptr, sockaddr_len)
            .build()
            .user_data(OP_CONNECT | idx as u64);

        unsafe {
            self.ring
                .submission()
                .push(&sqe)
                .expect("io_uring SQ full");
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
            self.ring
                .submission()
                .push(&sqe)
                .expect("io_uring SQ full");
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

            let action = session.try_process_melin_frame(
                self.config,
                &self.symbol_map,
                now,
            );

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
        // Flush FIX outbound.
        let fix_dirty: Vec<usize> = self.dirty_fix.drain(..).collect();
        for idx in fix_dirty {
            let session = match self.sessions.get_mut(idx) {
                Some(s) => s,
                None => continue,
            };
            if session.fix_send_buf.is_empty() {
                continue;
            }

            let sqe = opcode::Send::new(
                types::Fd(session.fix_fd),
                session.fix_send_buf.as_ptr(),
                session.fix_send_buf.len() as u32,
            )
            .build()
            .user_data(OP_SEND | idx as u64);

            unsafe {
                self.ring
                    .submission()
                    .push(&sqe)
                    .expect("io_uring SQ full");
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
            if session.melin_send_buf.is_empty() {
                continue;
            }

            let sqe = opcode::Send::new(
                types::Fd(melin_fd),
                session.melin_send_buf.as_ptr(),
                session.melin_send_buf.len() as u32,
            )
            .build()
            .user_data(OP_SEND | idx as u64);

            unsafe {
                self.ring
                    .submission()
                    .push(&sqe)
                    .expect("io_uring SQ full");
            }
        }
    }

    fn handle_send_complete(&mut self, idx: usize, result: i32) {
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            if let Some(session) = self.sessions.get(idx) {
                debug!(sender = %session.sender_comp_id, error = %err, "send error");
            }
            self.to_remove.push(idx);
            return;
        }

        let sent = result as usize;

        // Drain sent bytes from both send buffers (only the non-empty one
        // was submitted, but checking both is simpler and correct).
        if let Some(session) = self.sessions.get_mut(idx) {
            if !session.fix_send_buf.is_empty() {
                if sent >= session.fix_send_buf.len() {
                    session.fix_send_buf.clear();
                } else {
                    session.fix_send_buf.drain(..sent);
                    // Partial send — requeue.
                    self.dirty_fix.push(idx);
                }
            } else if !session.melin_send_buf.is_empty() {
                if sent >= session.melin_send_buf.len() {
                    session.melin_send_buf.clear();
                } else {
                    session.melin_send_buf.drain(..sent);
                    self.dirty_melin.push(idx);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Heartbeat
    // -----------------------------------------------------------------------

    fn check_heartbeats(&mut self, now: Instant) {
        let mut to_disconnect: Vec<usize> = Vec::new();

        for (idx, session) in self.sessions.iter_mut() {
            if !matches!(session.state, SessionState::Active) {
                continue;
            }

            let elapsed = now.duration_since(session.last_fix_recv);

            if elapsed > session.heartbeat_interval * 2 {
                // Client unresponsive — disconnect.
                warn!(sender = %session.sender_comp_id, "FIX heartbeat timeout");
                to_disconnect.push(idx);
            }
        }

        for idx in to_disconnect {
            self.to_remove.push(idx);
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
            self.ring
                .submission()
                .push(&sqe)
                .expect("io_uring SQ full");
        }
    }
}

// ---------------------------------------------------------------------------
// Actions returned by session message handlers
// ---------------------------------------------------------------------------

/// Actions the event loop should take after a session processes a message.
#[allow(dead_code)]
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
    let sqe =
        opcode::ProvideBuffers::new(pool_ptr, BUF_SIZE as i32, NUM_BUFFERS, BUF_GROUP_ID, 0)
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
    let rc = unsafe {
        libc::getpeername(fd, &mut addr as *mut _ as *mut libc::sockaddr, &mut len)
    };
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
