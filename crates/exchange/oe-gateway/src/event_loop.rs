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
const OP_SEND_FIX: u64 = 0x03 << 56;
const OP_SEND_MELIN: u64 = 0x04 << 56;
const OP_CONNECT: u64 = 0x05 << 56;
const OP_MASK: u64 = 0xFF << 56;
/// Session/connection generation, encoded in bits 40..56 of user_data
/// for every per-session op. Minted by the slab's per-slot counter: a
/// new occupant of a recycled index gets a fresh generation (guards
/// FIX- and Melin-side ops against stale CQEs from the previous
/// occupant), and a redirect re-mints the Melin connection generation
/// (guards against CQEs from the replaced socket). Handlers drop any
/// CQE whose generation does not match the session's current one.
const GEN_SHIFT: u32 = 40;
const GEN_MASK: u64 = 0xFFFF << GEN_SHIFT;
/// 40 bits of session index — far beyond any realistic session count.
const IDX_MASK: u64 = (1 << GEN_SHIFT) - 1;

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

#[inline(always)]
fn token_gen(token: u64) -> u16 {
    ((token & GEN_MASK) >> GEN_SHIFT) as u16
}

/// Build a per-session user_data token: op | generation | session index.
#[inline(always)]
fn op_token(op: u64, generation: u16, idx: usize) -> u64 {
    op | ((generation as u64) << GEN_SHIFT) | idx as u64
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
    /// Per-slot monotonic generation counter, parallel to `entries`.
    /// Bumped on every insert into the slot and on every Melin
    /// reconnect within a session, and stamped into each SQE's
    /// user_data — so a stale CQE from a previous occupant of a
    /// recycled index (or a previous Melin connection) can never be
    /// misattributed to the current session. Survives removal on
    /// purpose: that is what makes cross-occupancy values distinct.
    gens: Vec<u16>,
}

impl Slab {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(64),
            free: Vec::new(),
            gens: Vec::with_capacity(64),
        }
    }

    /// Store a session, minting its slot generation from the slot's
    /// counter (seeding both the FIX-side `slot_gen` and the initial
    /// Melin connection generation).
    fn insert(&mut self, mut session: Session) -> usize {
        let idx = match self.free.pop() {
            Some(idx) => idx,
            None => {
                self.entries.push(None);
                self.gens.push(0);
                self.entries.len() - 1
            }
        };
        self.gens[idx] = self.gens[idx].wrapping_add(1);
        session.slot_gen = self.gens[idx];
        session.melin_gen = self.gens[idx];
        self.entries[idx] = Some(session);
        idx
    }

    /// Mint the next generation for `idx` (a Melin reconnect within
    /// the current session). Shares the slot counter with `insert` so
    /// a generation value can never repeat across occupancies.
    fn bump_gen(&mut self, idx: usize) -> u16 {
        self.gens[idx] = self.gens[idx].wrapping_add(1);
        self.gens[idx]
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
    metrics: &'static crate::metrics::GatewayMetrics,
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
    /// Last primary address learned from a Melin redirect. New Melin
    /// connects prefer it over the configured seed address so every
    /// session after the first skips the redirect hop; cleared when a
    /// connect to it fails so the gateway falls back to the seed
    /// (mirrors the replica receiver's leader/static alternation —
    /// a stale hint costs one dial, never a wedged loop).
    /// Gateway-global, last-writer-wins on purpose: the cluster has one
    /// serving primary, so per-session state would only re-pay the
    /// redirect hop per session. During a contested election two nodes
    /// can briefly name different primaries and concurrent sessions
    /// flip this back and forth — bounded behavior (each session's hop
    /// budget converts the flapping into a Logout-and-retry), not a
    /// reason to make the hint per-session.
    melin_target: Option<std::net::SocketAddr>,
    /// Round-robin index into `config.seeds`, used only when no primary
    /// has been learned (`melin_target` is `None`). Gateway-global, not
    /// per-session: upstream reachability is a cluster-wide fact, so N
    /// sessions must not each re-probe the same dead seed N times.
    /// Advanced when a connect to the current seed fails; wraps at the
    /// end of the list.
    seed_cursor: usize,
}

impl Gateway {
    /// Create the gateway and register the listener + buffer pool with io_uring.
    pub fn new(
        listener: TcpListener,
        config: &'static GatewayConfig,
        metrics: &'static crate::metrics::GatewayMetrics,
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
            metrics,
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
            melin_target: None,
            seed_cursor: 0,
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
                    OP_FIX_RECV => self.handle_fix_recv(token, result, flags, now),
                    OP_MELIN_RECV => {
                        self.handle_melin_recv(token, result, flags, now);
                    }
                    OP_SEND_FIX => self.handle_fix_send_complete(token, result),
                    OP_SEND_MELIN => self.handle_melin_send_complete(token, result),
                    OP_CONNECT => self.handle_melin_connected(token, result, now),
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
        // Enable SO_BUSY_POLL — gateway loop spins on the io_uring CQ.
        set_busy_poll(fd);

        let peer = get_peer_addr(fd);
        info!(peer = %peer, fd, "FIX client connected");

        // Create a new session in AwaitingLogon state.
        let session = Session::new(fd, now, self.metrics);
        let idx = self.sessions.insert(session);
        self.metrics
            .sessions_accepted_total
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.sessions_active.fetch_add(1, Ordering::Relaxed);

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
            .user_data(op_token(OP_FIX_RECV, session.slot_gen, idx));

        unsafe {
            self.ring.submission().push(&sqe).expect("io_uring SQ full");
        }
        session.fix_multishot_active = true;
    }

    fn handle_fix_recv(&mut self, token: u64, result: i32, flags: u32, now: Instant) {
        let idx = slab_idx(token);
        // Stale generation: a CQE from a removed session whose slot was
        // recycled. Return any provided buffer; the current occupant
        // must not be touched (a stale EOF would tear it down).
        if !self.slot_gen_current(token) {
            if result > 0 && (flags & IORING_CQE_F_BUFFER) != 0 {
                self.re_provide_buffer((flags >> IORING_CQE_BUFFER_SHIFT) as usize);
            }
            return;
        }
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

            let raw = match melin_gateway_core::fix::parse::try_extract_message(
                &mut session.fix_parse_buf,
            ) {
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
                SessionAction::RedirectMelin(addr) => {
                    // Redirects arrive on the Melin side only; honoring
                    // one from here would tear down a live Melin
                    // connection in an arbitrary session state. Same
                    // convention as ConnectMelin on the Melin path:
                    // log loudly enough to surface the bug, act on
                    // nothing.
                    debug!(idx, target = %addr, "unexpected RedirectMelin from FIX recv");
                }
                SessionAction::SendFix => {
                    self.dirty_fix.push(idx);
                }
                SessionAction::SendMelin => {
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

    /// Pick the address the next Melin connect should dial: the learned
    /// primary if we have one (best information — skips the redirect hop),
    /// else the seed the cursor currently points at. `config.seeds` is
    /// guaranteed non-empty by config validation, so the index is sound.
    fn connect_target(&self) -> std::net::SocketAddr {
        self.melin_target
            .unwrap_or_else(|| self.config.seeds[self.seed_cursor])
    }

    fn start_melin_connect(&mut self, idx: usize) {
        // Prefer the last redirect-learned primary over the configured
        // seeds: after a failover, the first session pays the redirect
        // hop and every later one dials the primary directly.
        let server_addr = self.connect_target();
        self.start_melin_connect_to(idx, server_addr);
    }

    /// Rotate the upstream candidate after a failed Melin connect. A
    /// dead learned primary is cleared (a redirect from any live node
    /// re-teaches it); a dead seed advances the round-robin cursor so
    /// the next session tries the next node. Either way the *next*
    /// connect picks a fresh candidate — see [`Gateway::connect_target`].
    fn on_connect_failed(&mut self, failed: Option<std::net::SocketAddr>) {
        if failed.is_some() && failed == self.melin_target {
            self.melin_target = None;
        } else {
            // `seeds` is non-empty (config validation), so the modulo is
            // well-defined and the cursor stays in range.
            self.seed_cursor = (self.seed_cursor + 1) % self.config.seeds.len();
        }
    }

    fn start_melin_connect_to(&mut self, idx: usize, server_addr: std::net::SocketAddr) {
        // Create a non-blocking TCP socket.
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0) };
        if fd < 0 {
            error!(error = fd, "socket() failed for Melin connection");
            self.to_remove.push(idx);
            return;
        }

        set_tcp_nodelay(fd);
        set_busy_poll(fd);

        // Build sockaddr for the connect SQE.
        let sockaddr = socket_addr_to_sockaddr(server_addr);
        let sockaddr_len = std::mem::size_of::<libc::sockaddr_in>() as u32;

        // Store fd + sockaddr on the session so it lives long enough for
        // io_uring to read it. Bail if the session slot has gone away.
        let Some(session) = self.sessions.get_mut(idx) else {
            debug!(idx, "session missing in start_melin_connect");
            self.to_remove.push(idx);
            return;
        };
        session.melin_fd = Some(fd);
        // Remember where this attempt is dialing so the failure path can
        // tell a dead learned-primary from a dead seed.
        session.melin_connect_target = Some(server_addr);
        // The kernel reads this address asynchronously after we submit the
        // connect SQE, so it must outlive the call — the session owns it
        // until the connection completes.
        let addr_ptr = session.connect_addr.insert(sockaddr) // &mut sockaddr_in
            as *const libc::sockaddr_in                      // coerce to raw pointer
            as *const libc::sockaddr; // reinterpret as generic sockaddr

        let sqe = opcode::Connect::new(types::Fd(fd), addr_ptr, sockaddr_len)
            .build()
            .user_data(op_token(OP_CONNECT, session.melin_gen, idx));

        unsafe {
            self.ring.submission().push(&sqe).expect("io_uring SQ full");
        }
    }

    /// Replace a session's Melin connection with one to the redirect
    /// target: close the old socket (its in-flight CQEs are dropped by
    /// the generation guard), remember the target as the cluster's
    /// serving primary, and reconnect. The session re-enters the
    /// handshake from `ConnectingMelin`.
    fn redirect_melin(&mut self, idx: usize, addr: std::net::SocketAddr) {
        // Backstop for the session-level IPv4 policy check (the
        // sockaddr conversion below would panic on v6): should be
        // unreachable, so no Logout niceties — just drop the session.
        if !addr.is_ipv4() {
            debug!(idx, target = %addr, "unfollowable redirect reached the event loop");
            self.to_remove.push(idx);
            return;
        }

        self.melin_target = Some(addr);

        if self.sessions.get(idx).is_none() {
            return;
        }
        // Mint the replacement connection's generation from the slot
        // counter, so it can never collide with any generation a
        // previous occupant or connection used.
        let new_gen = self.sessions.bump_gen(idx);
        let Some(session) = self.sessions.get_mut(idx) else {
            return;
        };
        if let Some(old_fd) = session.begin_melin_redirect(new_gen) {
            // shutdown() before close(): the pending multishot recv
            // holds a kernel reference to the socket, so close() alone
            // would neither send FIN nor complete the recv — the old
            // node would keep a zombie connection. shutdown() tears the
            // TCP stream down immediately and flushes the pending recv
            // out as a terminal CQE, which carries the old generation
            // and is dropped by the guards.
            // Return codes discarded: this is best-effort teardown of a
            // socket we are abandoning — the only failure mode for a
            // valid fd is "already shut down", which is the goal.
            unsafe {
                libc::shutdown(old_fd, libc::SHUT_RDWR);
                libc::close(old_fd);
            }
        }
        self.start_melin_connect_to(idx, addr);
    }

    fn handle_melin_connected(&mut self, token: u64, result: i32, now: Instant) {
        let idx = slab_idx(token);
        // Stale generation: a redirect already replaced this connection
        // attempt's socket. Nothing to clean up — the fd was closed by
        // the redirect.
        if !self.melin_gen_current(token) {
            return;
        }
        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            let failed = self.sessions.get(idx).and_then(|s| s.melin_connect_target);
            if let Some(session) = self.sessions.get(idx) {
                // warn, not error: an unreachable upstream is a network
                // condition the rotation below handles (clear the dead
                // primary or step to the next seed), not a gateway bug.
                warn!(
                    sender = %session.sender_comp_id,
                    error = %err,
                    "Melin connect failed"
                );
            }
            self.on_connect_failed(failed);
            self.to_remove.push(idx);
            return;
        }

        if let Some(session) = self.sessions.get_mut(idx) {
            // A connect completing on a dying session must not
            // resurrect it: Closing is what lets the pending-send
            // completion handlers finish the removal, and overwriting
            // it would leave a zombie that completes the Melin auth
            // handshake under a disconnected client's key. The fd is
            // released by Session::Drop when the removal lands.
            if matches!(session.state, SessionState::Closing) {
                return;
            }
            info!(
                sender = %session.sender_comp_id,
                "connected to Melin server"
            );
            session.on_melin_connected(now);
        }

        // Start multishot RECV on the Melin socket to receive the Challenge.
        self.push_melin_recv_multi(idx);
    }

    /// `true` when the token's Melin connection generation matches the
    /// session's current one. Stale CQEs (from a socket a redirect
    /// closed, or from a previous occupant of a recycled slot) must be
    /// dropped, not attributed to the current connection. A missing
    /// session also reports stale.
    fn melin_gen_current(&self, token: u64) -> bool {
        self.sessions
            .get(slab_idx(token))
            .is_some_and(|s| s.melin_gen == token_gen(token))
    }

    /// FIX-side twin of [`Gateway::melin_gen_current`]: the FIX socket
    /// never reconnects, so only slot recycling can produce a stale
    /// FIX-op CQE.
    fn slot_gen_current(&self, token: u64) -> bool {
        self.sessions
            .get(slab_idx(token))
            .is_some_and(|s| s.slot_gen == token_gen(token))
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
            .user_data(op_token(OP_MELIN_RECV, session.melin_gen, idx));

        unsafe {
            self.ring.submission().push(&sqe).expect("io_uring SQ full");
        }
        session.melin_multishot_active = true;
    }

    fn handle_melin_recv(&mut self, token: u64, result: i32, flags: u32, now: Instant) {
        let idx = slab_idx(token);
        // Stale generation: this CQE belongs to a socket a redirect
        // closed. Return any provided buffer to the pool, but do not
        // touch the session — a stale error/EOF here must not tear down
        // the replacement connection, and a stale terminal CQE must not
        // clear the new multishot's active flag.
        if !self.melin_gen_current(token) {
            if result > 0 && (flags & IORING_CQE_F_BUFFER) != 0 {
                self.re_provide_buffer((flags >> IORING_CQE_BUFFER_SHIFT) as usize);
            }
            return;
        }
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
                SessionAction::Close => {
                    self.dirty_fix.push(idx);
                    self.to_remove.push(idx);
                    return;
                }
                SessionAction::RedirectMelin(addr) => {
                    // The Melin connection is being replaced: any frames
                    // still buffered came from the node we are leaving,
                    // so stop processing and let the new connection's
                    // handshake produce fresh ones.
                    self.redirect_melin(idx, addr);
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

            // A send SQE is already in flight: submitting another over
            // the same inflight buffer would transmit the bytes twice
            // and let the two completions desync the buffer swap. New
            // data waits in send_buf; the completion handler re-queues
            // this session as dirty.
            if session.fix_send_pending {
                continue;
            }
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
            .user_data(op_token(OP_SEND_FIX, session.slot_gen, idx));

            unsafe {
                self.ring.submission().push(&sqe).expect("io_uring SQ full");
            }
            session.fix_send_pending = true;
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

            // Same single-SQE-in-flight rule as the FIX side above.
            if session.melin_send_pending {
                continue;
            }
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
            .user_data(op_token(OP_SEND_MELIN, session.melin_gen, idx));

            unsafe {
                self.ring.submission().push(&sqe).expect("io_uring SQ full");
            }
            session.melin_send_pending = true;
        }
    }

    fn handle_fix_send_complete(&mut self, token: u64, result: i32) {
        let idx = slab_idx(token);
        // Stale generation: the send belonged to a previous occupant of
        // this slot. Its buffers were freed with that session (the
        // removal gate guarantees no send SQE was pending), so there is
        // nothing to release — just keep the CQE away from the current
        // occupant.
        if !self.slot_gen_current(token) {
            return;
        }
        // The SQE this CQE answers is done — the slot is free for the
        // next submission whatever the outcome below.
        if let Some(session) = self.sessions.get_mut(idx) {
            session.fix_send_pending = false;
        }
        if result < 0 {
            // Clear the inflight buffer before queueing removal.
            // drain_removals refuses to remove sessions whose inflight
            // buffers are non-empty (the kernel may still be reading
            // them) — but on a SEND error the operation is *complete*,
            // the kernel is no longer touching the bytes, so the buffer
            // is safe to drop. Without this, every send error leaks
            // the session permanently into the slab.
            if let Some(session) = self.sessions.get_mut(idx) {
                debug!(sender = %session.sender_comp_id, error = result, "FIX send error");
                session.fix_inflight.clear();
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
                    && session.melin_inflight.is_empty()
                    && session.stale_melin_inflight.is_empty();
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

    fn handle_melin_send_complete(&mut self, token: u64, result: i32) {
        let idx = slab_idx(token);
        // Stale generation: the send belonged to a socket a redirect
        // closed. The kernel is done with the buffer parked under that
        // generation — release exactly that one (a cross-occupancy
        // stale send CQE cannot exist: the removal gate holds sessions
        // until every send SQE has completed, so a generation found
        // here always belongs to the current session's graveyard). The
        // (likely error) result must not remove the session serving
        // the replacement connection.
        if !self.melin_gen_current(token) {
            if let Some(session) = self.sessions.get_mut(idx) {
                let stale_gen = token_gen(token);
                session
                    .stale_melin_inflight
                    .retain(|(parked_gen, _)| *parked_gen != stale_gen);
                // A Closing session may have been waiting on this very
                // buffer — re-check removability now that it is gone.
                if matches!(session.state, SessionState::Closing)
                    && session.stale_melin_inflight.is_empty()
                    && session.fix_inflight.is_empty()
                    && session.melin_inflight.is_empty()
                {
                    self.to_remove.push(idx);
                }
            }
            return;
        }
        // The SQE this CQE answers is done — the slot is free for the
        // next submission whatever the outcome below.
        if let Some(session) = self.sessions.get_mut(idx) {
            session.melin_send_pending = false;
        }
        if result < 0 {
            // See comment in handle_fix_send_complete: clear the
            // inflight buffer so drain_removals can actually remove
            // this session. Otherwise it leaks permanently.
            if let Some(session) = self.sessions.get_mut(idx) {
                debug!(sender = %session.sender_comp_id, error = result, "Melin send error");
                session.melin_inflight.clear();
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
                    && session.fix_inflight.is_empty()
                    && session.stale_melin_inflight.is_empty();
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
    /// io_uring SEND SQEs in flight — live inflight buffers or
    /// redirect-parked ones — since the kernel may still be reading
    /// from those allocations.
    fn drain_removals(&mut self) {
        let pending: Vec<usize> = self.to_remove.drain(..).collect();
        for idx in pending {
            let can_remove = self.sessions.get(idx).is_none_or(|s| {
                s.fix_inflight.is_empty()
                    && s.melin_inflight.is_empty()
                    && s.stale_melin_inflight.is_empty()
            });
            if can_remove {
                if let Some(session) = self.sessions.remove(idx) {
                    debug!(sender = %session.sender_comp_id, "session removed");
                    self.metrics.sessions_active.fetch_sub(1, Ordering::Relaxed);
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
#[derive(Debug, PartialEq, Eq)]
pub enum SessionAction {
    /// No I/O needed.
    None,
    /// Initiate Melin TCP connect.
    ConnectMelin,
    /// Replace the Melin connection: the dialed node is a replica and
    /// named the serving primary. Close the current Melin socket and
    /// reconnect to the carried address.
    RedirectMelin(std::net::SocketAddr),
    /// Flush FIX send buffer.
    SendFix,
    /// Flush Melin send buffer.
    SendMelin,
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

/// Enable kernel busy-polling on the FIX client socket. Removes the
/// softirq->wakeup handoff for incoming bytes; safe to apply because the
/// gateway's io_uring loop already busy-spins on the CQ. 50us window
/// matches the Melin server-side value.
fn set_busy_poll(fd: RawFd) {
    let val: libc::c_int = 50;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of_val(&val) as libc::socklen_t,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        debug!(fd, error = %err, "failed to set SO_BUSY_POLL on FIX client socket");
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
    use melin_gateway_core::fix::parse::FixMessage;
    use melin_gateway_core::fix::serialize::FixMessageBuilder;
    use melin_gateway_core::fix::tags;
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
    /// `server_port` is the port the Melin stub (or a bogus unused
    /// port) is listening on.
    fn make_config_with_port(
        sender: &str,
        target: &str,
        server_port: u16,
    ) -> &'static GatewayConfig {
        let key_path = make_key_file();
        let toml = format!(
            r#"
server_addr = "127.0.0.1:{server_port}"
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
        let mut config: GatewayConfig = toml::from_str(&toml).unwrap();
        // Normalize `seeds` exactly as production load does — the event
        // loop indexes `config.seeds`, which `toml::from_str` leaves empty.
        config.validate().unwrap();
        Box::leak(Box::new(config))
    }

    /// Like `make_config_with_port` but seeds a whole `server_addrs`
    /// list (all on 127.0.0.1), for the multi-seed rotation tests.
    fn make_config_with_ports(sender: &str, target: &str, ports: &[u16]) -> &'static GatewayConfig {
        let key_path = make_key_file();
        let addrs: Vec<String> = ports.iter().map(|p| format!("\"127.0.0.1:{p}\"")).collect();
        let toml = format!(
            r#"
server_addrs = [{}]
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
            addrs.join(", "),
            key_path.display()
        );
        let mut config: GatewayConfig = toml::from_str(&toml).unwrap();
        config.validate().unwrap();
        Box::leak(Box::new(config))
    }

    /// Reserve a 127.0.0.1 port and immediately release it, so a connect
    /// to it refuses fast. A theoretical race (something else grabbing
    /// the port) is acceptable for a test.
    fn dead_port() -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        drop(l);
        port
    }

    /// Default config for tests that never reach the Melin connect
    /// path. Points `server_addr` at a bogus port (1) that will never
    /// be dialed.
    fn make_config(sender: &str, target: &str) -> &'static GatewayConfig {
        make_config_with_port(sender, target, 1)
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
            let metrics = crate::metrics::GatewayMetrics::leak_default();
            let mut gw = Gateway::new(listener, config, metrics).expect("gateway new");
            gw.run(&shutdown_clone).expect("gateway run");
        });
        GwHandle {
            port,
            shutdown,
            join: Some(join),
        }
    }

    /// A TCP client paired with a persistent accumulator buffer so
    /// reads can frame multiple FIX messages without losing leftover
    /// bytes between calls. Required for any test that expects more
    /// than one message in sequence — without it, when the gateway
    /// flushes several messages in one io_uring SEND the kernel can
    /// deliver them in a single read and the second message's bytes
    /// would be dropped on the floor.
    struct FramedClient {
        stream: TcpStream,
        accum: Vec<u8>,
    }
    impl FramedClient {
        fn connect(port: u16) -> Self {
            let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            Self {
                stream,
                accum: Vec::with_capacity(512),
            }
        }
        fn write_all(&mut self, bytes: &[u8]) {
            self.stream.write_all(bytes).unwrap();
        }
        fn read_message(&mut self) -> Vec<u8> {
            let mut tmp = [0u8; 256];
            loop {
                if let Some(msg) =
                    melin_gateway_core::fix::parse::try_extract_message(&mut self.accum)
                {
                    return msg;
                }
                match self.stream.read(&mut tmp) {
                    Ok(0) => panic!(
                        "unexpected EOF before complete FIX message (accum has {} bytes)",
                        self.accum.len()
                    ),
                    Ok(n) => self.accum.extend_from_slice(&tmp[..n]),
                    Err(e) => panic!("read error: {e}"),
                }
            }
        }
    }

    /// Read one complete FIX message from a TCP stream with a timeout.
    /// One-shot variant for tests that expect exactly one inbound
    /// message; do NOT use when more than one message may follow on
    /// the same connection — leftover bytes after the first frame are
    /// dropped. Use `FramedClient` instead in that case.
    fn read_fix_message(stream: &mut TcpStream) -> Vec<u8> {
        stream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut buf = Vec::with_capacity(256);
        let mut tmp = [0u8; 256];
        loop {
            if let Some(msg) = melin_gateway_core::fix::parse::try_extract_message(&mut buf) {
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

    // -----------------------------------------------------------------------
    // Direct unit tests for handler-internal invariants
    // -----------------------------------------------------------------------

    /// Construct a Gateway, plant `bytes` in `fix_inflight`, simulate a
    /// SEND error CQE, and assert the buffer was cleared and the session
    /// is queued for removal. Without the fix, drain_removals would
    /// refuse to remove the session because inflight is non-empty.
    #[test]
    fn fix_send_error_clears_inflight_so_session_can_be_removed() {
        use crate::session::Session;
        use std::os::unix::io::IntoRawFd;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let config = make_config("FIRM_A", "MELIN");
        let metrics = crate::metrics::GatewayMetrics::leak_default();
        let mut gw = Gateway::new(listener, config, metrics).expect("gateway new");

        // Insert a fake session with non-empty inflight.
        let dummy_fd = std::fs::File::open("/dev/null").unwrap().into_raw_fd();
        let mut session = Session::new(dummy_fd, Instant::now(), metrics);
        session.fix_inflight = b"PENDING SEND".to_vec();
        let idx = gw.sessions.insert(session);

        let slot_gen = gw.sessions.get(idx).unwrap().slot_gen;
        gw.handle_fix_send_complete(op_token(OP_SEND_FIX, slot_gen, idx), -32); // EPIPE

        let session = gw.sessions.get(idx).expect("session still in slab");
        assert!(
            session.fix_inflight.is_empty(),
            "inflight must be cleared on send error"
        );
        assert!(
            gw.to_remove.contains(&idx),
            "session must be queued for removal"
        );
    }

    #[test]
    fn melin_send_error_clears_inflight_so_session_can_be_removed() {
        use crate::session::Session;
        use std::os::unix::io::IntoRawFd;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let config = make_config("FIRM_A", "MELIN");
        let metrics = crate::metrics::GatewayMetrics::leak_default();
        let mut gw = Gateway::new(listener, config, metrics).expect("gateway new");

        let dummy_fd = std::fs::File::open("/dev/null").unwrap().into_raw_fd();
        let mut session = Session::new(dummy_fd, Instant::now(), metrics);
        session.melin_inflight = b"PENDING SEND".to_vec();
        let idx = gw.sessions.insert(session);

        let melin_gen = gw.sessions.get(idx).unwrap().melin_gen;
        gw.handle_melin_send_complete(op_token(OP_SEND_MELIN, melin_gen, idx), -104); // ECONNRESET

        let session = gw.sessions.get(idx).expect("session still in slab");
        assert!(
            session.melin_inflight.is_empty(),
            "melin inflight must be cleared on send error"
        );
        assert!(gw.to_remove.contains(&idx));
    }

    #[test]
    fn stale_generation_melin_cqes_do_not_touch_the_session() {
        // After a redirect bumps the connection generation, CQEs from
        // the closed socket (recv EOF/errors, send failures) carry the
        // old generation and must be dropped — a stale error must not
        // remove the session now serving the replacement connection.
        use crate::session::Session;
        use std::os::unix::io::IntoRawFd;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let config = make_config("FIRM_A", "MELIN");
        let metrics = crate::metrics::GatewayMetrics::leak_default();
        let mut gw = Gateway::new(listener, config, metrics).expect("gateway new");

        let dummy_fd = std::fs::File::open("/dev/null").unwrap().into_raw_fd();
        let mut session = Session::new(dummy_fd, Instant::now(), metrics);
        session.melin_inflight = b"OLD NODE HANDSHAKE".to_vec();
        session.melin_send_pending = true; // SQE still in flight
        let idx = gw.sessions.insert(session);
        let old_gen = gw.sessions.get(idx).unwrap().melin_gen;

        // Redirect: parks the pending buffer under the old generation.
        let new_gen = gw.sessions.bump_gen(idx);
        let session = gw.sessions.get_mut(idx).unwrap();
        let _ = session.begin_melin_redirect(new_gen);
        assert_eq!(
            session.stale_melin_inflight,
            vec![(old_gen, b"OLD NODE HANDSHAKE".to_vec())],
            "pending inflight must be parked under the submitting generation"
        );
        assert!(!session.melin_send_pending, "new connection starts idle");
        session.melin_multishot_active = true; // as if the new recv is armed

        // Stale recv error (old socket closed): session must survive
        // and the new multishot flag must stay armed.
        gw.handle_melin_recv(
            op_token(OP_MELIN_RECV, old_gen, idx),
            -104,
            0,
            Instant::now(),
        );
        assert!(gw.sessions.get(idx).is_some(), "session must survive");
        assert!(!gw.to_remove.contains(&idx));
        assert!(gw.sessions.get(idx).unwrap().melin_multishot_active);

        // Stale send completion: releases the parked buffer, nothing else.
        gw.handle_melin_send_complete(op_token(OP_SEND_MELIN, old_gen, idx), -32);
        let session = gw.sessions.get(idx).expect("session still in slab");
        assert!(
            session.stale_melin_inflight.is_empty(),
            "parked buffer must be released once its CQE fires"
        );
        assert!(!gw.to_remove.contains(&idx));

        // Current-generation error still removes as before.
        let cur_gen = gw.sessions.get(idx).unwrap().melin_gen;
        gw.handle_melin_recv(
            op_token(OP_MELIN_RECV, cur_gen, idx),
            -104,
            0,
            Instant::now(),
        );
        assert!(gw.to_remove.contains(&idx));
    }

    /// Shared boilerplate for the lifecycle tests below: a Gateway plus
    /// one inserted session backed by a dummy fd.
    fn gateway_with_session() -> (Gateway, usize) {
        use crate::session::Session;
        use std::os::unix::io::IntoRawFd;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let config = make_config("FIRM_A", "MELIN");
        let metrics = crate::metrics::GatewayMetrics::leak_default();
        let mut gw = Gateway::new(listener, config, metrics).expect("gateway new");
        let dummy_fd = std::fs::File::open("/dev/null").unwrap().into_raw_fd();
        let session = Session::new(dummy_fd, Instant::now(), metrics);
        let idx = gw.sessions.insert(session);
        (gw, idx)
    }

    /// Build a gateway whose config lists `ports` as seeds (all on
    /// 127.0.0.1). Nothing is dialed — these tests exercise only the
    /// pure target-selection / rotation helpers.
    fn gateway_with_seeds(ports: &[u16]) -> Gateway {
        let config = make_config_with_ports("FIRM_A", "MELIN", ports);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let metrics = crate::metrics::GatewayMetrics::leak_default();
        Gateway::new(listener, config, metrics).expect("gateway new")
    }

    fn addr(port: u16) -> std::net::SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn connect_target_prefers_learned_primary_over_seeds() {
        let mut gw = gateway_with_seeds(&[1, 2, 3]);
        // Cursor starts at 0 → the first seed.
        assert_eq!(gw.connect_target(), addr(1));
        // A learned primary wins over any seed.
        gw.melin_target = Some(addr(99));
        assert_eq!(gw.connect_target(), addr(99));
    }

    #[test]
    fn connect_failure_on_seed_advances_cursor() {
        let mut gw = gateway_with_seeds(&[1, 2, 3]);
        assert_eq!(gw.connect_target(), addr(1));
        gw.on_connect_failed(Some(addr(1)));
        assert_eq!(gw.connect_target(), addr(2));
        gw.on_connect_failed(Some(addr(2)));
        assert_eq!(gw.connect_target(), addr(3));
    }

    #[test]
    fn connect_failure_on_learned_target_clears_it_not_the_cursor() {
        let mut gw = gateway_with_seeds(&[1, 2]);
        gw.melin_target = Some(addr(99));
        // A dead learned primary is cleared; the seed cursor is untouched
        // (still 0), so the next dial falls back to the first seed.
        gw.on_connect_failed(Some(addr(99)));
        assert_eq!(gw.melin_target, None);
        assert_eq!(gw.seed_cursor, 0);
        assert_eq!(gw.connect_target(), addr(1));
    }

    #[test]
    fn seed_cursor_wraps_past_end_of_list() {
        let mut gw = gateway_with_seeds(&[1, 2]);
        gw.on_connect_failed(Some(addr(1))); // cursor → 1
        gw.on_connect_failed(Some(addr(2))); // cursor wraps → 0
        assert_eq!(gw.seed_cursor, 0);
        assert_eq!(gw.connect_target(), addr(1));
    }

    #[test]
    fn recycled_slot_gets_a_fresh_generation() {
        // A stale CQE from a removed session must not be attributed to
        // the new occupant of the recycled slab index — both FIX- and
        // Melin-side.
        use crate::session::Session;
        use std::os::unix::io::IntoRawFd;

        let (mut gw, idx) = gateway_with_session();
        let old_slot_gen = gw.sessions.get(idx).unwrap().slot_gen;
        let old_melin_gen = gw.sessions.get(idx).unwrap().melin_gen;
        gw.sessions.remove(idx).expect("occupied");

        let dummy_fd = std::fs::File::open("/dev/null").unwrap().into_raw_fd();
        let session = Session::new(dummy_fd, Instant::now(), gw.metrics);
        let idx2 = gw.sessions.insert(session);
        assert_eq!(idx2, idx, "slab must recycle the index (LIFO free list)");
        assert_ne!(
            gw.sessions.get(idx).unwrap().slot_gen,
            old_slot_gen,
            "new occupant must not share the old generation"
        );

        // Stale terminal CQEs from the previous occupant: dropped.
        gw.handle_fix_recv(
            op_token(OP_FIX_RECV, old_slot_gen, idx),
            0,
            0,
            Instant::now(),
        );
        gw.handle_melin_recv(
            op_token(OP_MELIN_RECV, old_melin_gen, idx),
            -104,
            0,
            Instant::now(),
        );
        assert!(
            !gw.to_remove.contains(&idx),
            "stale CQEs from the previous occupant must not tear down the new session"
        );
    }

    #[test]
    fn no_second_send_while_one_is_pending() {
        // New outbound data queued while a send SQE is in flight must
        // wait in send_buf — a second submission over the same inflight
        // buffer would duplicate bytes on the wire.
        let (mut gw, idx) = gateway_with_session();
        {
            let session = gw.sessions.get_mut(idx).unwrap();
            session.melin_fd = Some(session.fix_fd); // any valid fd
            session.melin_inflight = b"IN FLIGHT".to_vec();
            session.melin_send_pending = true;
            session.melin_send_buf = b"QUEUED".to_vec();
            session.fix_send_pending = true;
            session.fix_inflight = b"FIX IN FLIGHT".to_vec();
            session.fix_send_buf = b"FIX QUEUED".to_vec();
        }
        gw.dirty_melin.push(idx);
        gw.dirty_fix.push(idx);
        gw.flush_dirty_sends();

        let session = gw.sessions.get(idx).unwrap();
        assert_eq!(
            session.melin_send_buf, b"QUEUED",
            "queued data must stay in send_buf while a send is pending"
        );
        assert_eq!(session.melin_inflight, b"IN FLIGHT");
        assert_eq!(session.fix_send_buf, b"FIX QUEUED");

        // Completion frees the slot; the requeue path then swaps.
        let melin_gen = session.melin_gen;
        gw.handle_melin_send_complete(op_token(OP_SEND_MELIN, melin_gen, idx), 9);
        let session = gw.sessions.get(idx).unwrap();
        assert!(!session.melin_send_pending, "completion must free the slot");
        assert!(session.melin_inflight.is_empty());
        assert!(
            gw.dirty_melin.contains(&idx),
            "queued data must be re-flushed after completion"
        );
    }

    #[test]
    fn late_connect_cqe_does_not_resurrect_a_closing_session() {
        let (mut gw, idx) = gateway_with_session();
        {
            let session = gw.sessions.get_mut(idx).unwrap();
            session.state = SessionState::Closing;
        }
        let melin_gen = gw.sessions.get(idx).unwrap().melin_gen;
        gw.handle_melin_connected(op_token(OP_CONNECT, melin_gen, idx), 0, Instant::now());
        assert!(
            matches!(gw.sessions.get(idx).unwrap().state, SessionState::Closing),
            "a late connect must not flip Closing back to AwaitingChallenge"
        );
    }

    #[test]
    fn removal_waits_for_parked_send_buffers() {
        // A session queued for removal while a redirect-parked send is
        // still un-reaped must survive until the stale CQE releases the
        // buffer the kernel may be reading.
        let (mut gw, idx) = gateway_with_session();
        let old_gen = gw.sessions.get(idx).unwrap().melin_gen;
        {
            let session = gw.sessions.get_mut(idx).unwrap();
            session.melin_inflight = b"KERNEL MAY READ".to_vec();
            session.melin_send_pending = true;
        }
        let new_gen = gw.sessions.bump_gen(idx);
        let _ = gw
            .sessions
            .get_mut(idx)
            .unwrap()
            .begin_melin_redirect(new_gen);

        gw.to_remove.push(idx);
        gw.drain_removals();
        assert!(
            gw.sessions.get(idx).is_some(),
            "removal must defer while a parked buffer awaits its CQE"
        );
        assert!(matches!(
            gw.sessions.get(idx).unwrap().state,
            SessionState::Closing
        ));

        // The stale CQE releases the buffer and completes the removal.
        gw.handle_melin_send_complete(op_token(OP_SEND_MELIN, old_gen, idx), -32);
        assert!(gw.to_remove.contains(&idx), "now removable");
        gw.drain_removals();
        assert!(gw.sessions.get(idx).is_none(), "session removed");
    }

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
        // Minimal shape: 8=FIX.4.4\x019=5\x0135=0\x0110=000\x01
        client
            .write_all(b"8=FIX.4.4\x019=5\x0135=0\x0110=000\x01")
            .unwrap();

        // Gateway should close without sending anything (malformed
        // Logon never produces a Logout — see handle_logon).
        let mut buf = [0u8; 64];
        let n = client.read(&mut buf).expect("read after garbage");
        assert_eq!(n, 0, "expected EOF for malformed Logon");

        drop(client);
        gw.shutdown();
    }

    // -----------------------------------------------------------------------
    // End-to-end tests with a loopback Melin stub
    // -----------------------------------------------------------------------

    #[test]
    fn authenticated_logon_flow() {
        use crate::test_stub::MelinStub;

        // Boot the stub FIRST so the gateway's config points at a live
        // port. The stub is idle until the gateway dials it.
        let stub = MelinStub::start();
        let config = make_config_with_port("FIRM_A", "MELIN", stub.port());
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // Send a valid Logon. The gateway should:
        //   1. Parse and validate Logon
        //   2. io_uring CONNECT to the stub
        //   3. Receive Challenge, sign the nonce, send ChallengeResponse
        //   4. Receive ServerReady
        //   5. Send a FIX Logon ack back to the client
        client
            .write_all(&logon_bytes("FIRM_A", "MELIN", 1))
            .unwrap();

        let raw = read_fix_message(&mut client);
        let msg = FixMessage::parse(&raw).expect("valid FIX Logon ack");
        assert_eq!(
            msg.msg_type(),
            tags::MSG_LOGON,
            "expected Logon ack, got {:?}",
            std::str::from_utf8(msg.msg_type())
        );
        assert_eq!(msg.sender_comp_id(), Some("MELIN"));

        drop(client);
        gw.shutdown();
        drop(stub);
    }

    #[test]
    fn gateway_follows_melin_redirect_to_the_primary() {
        use crate::test_stub::MelinStub;
        use melin_protocol::message::Request;

        // "Primary" stub serves normally; "replica" stub authenticates
        // and answers Redirect naming the primary. The gateway is
        // configured at the replica — exactly the post-failover
        // topology where ops haven't repointed the gateway yet.
        let primary = MelinStub::start();
        let primary_addr: std::net::SocketAddr =
            format!("127.0.0.1:{}", primary.port()).parse().unwrap();
        let replica = MelinStub::start_redirecting_to(primary_addr);
        let config = make_config_with_port("FIRM_A", "MELIN", replica.port());
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // The Logon must complete even though the configured upstream
        // is a replica: gateway dials replica → authenticates → gets
        // Redirect → reconnects to primary → full handshake → Logon ack.
        client
            .write_all(&logon_bytes("FIRM_A", "MELIN", 1))
            .unwrap();
        let raw = read_fix_message(&mut client);
        let msg = FixMessage::parse(&raw).expect("valid FIX Logon ack");
        assert_eq!(msg.msg_type(), tags::MSG_LOGON);

        // The replica saw the gateway hang up after the redirect.
        assert!(
            replica.wait_for_disconnect(Duration::from_secs(5)),
            "gateway must close the replica connection it was redirected away from"
        );

        // Traffic flows to the primary: a NewOrderSingle reaches it.
        let nos = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build("FIRM_A", "MELIN", 2);
        client.write_all(&nos).unwrap();
        let (_seq, req) = primary.next_request(Duration::from_secs(5));
        assert!(
            matches!(req, Request::SubmitOrder { .. }),
            "order must reach the redirect target, got {req:?}"
        );

        drop(client);
        gw.shutdown();
        drop(replica);
        drop(primary);
    }

    #[test]
    fn gateway_walks_past_dead_seed_to_live_seed_redirect() {
        use crate::test_stub::MelinStub;
        use melin_protocol::message::Request;

        // seed 0 = dead (nothing listening); seed 1 = a replica that
        // redirects to the primary. The dead seed is the only hint at
        // boot, so the gateway must walk to seed 1 and follow its
        // redirect — the failure this whole task exists to fix.
        let primary = MelinStub::start();
        let primary_addr: std::net::SocketAddr =
            format!("127.0.0.1:{}", primary.port()).parse().unwrap();
        let replica = MelinStub::start_redirecting_to(primary_addr);
        let config = make_config_with_ports("FIRM_A", "MELIN", &[dead_port(), replica.port()]);
        let gw = spawn_gateway(config);

        // First attempt lands on the dead seed: the upstream connect is
        // refused, so the gateway tears the FIX session down. That
        // failure advances the seed cursor to seed 1.
        let mut c1 = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        c1.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c1.write_all(&logon_bytes("FIRM_A", "MELIN", 1)).unwrap();
        let mut buf = [0u8; 64];
        let n = c1.read(&mut buf).unwrap_or(0);
        assert_eq!(
            n, 0,
            "gateway must drop the FIX session when its only seed is dead"
        );
        drop(c1);

        // Retry (a FIX client always reconnects): the cursor now points
        // at the live replica, which redirects to the primary.
        let mut c2 = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        c2.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c2.write_all(&logon_bytes("FIRM_A", "MELIN", 1)).unwrap();
        let raw = read_fix_message(&mut c2);
        assert_eq!(
            FixMessage::parse(&raw).unwrap().msg_type(),
            tags::MSG_LOGON,
            "second attempt must complete Logon via the live seed's redirect"
        );

        // Orders reach the primary the replica pointed at.
        let nos = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build("FIRM_A", "MELIN", 2);
        c2.write_all(&nos).unwrap();
        let (_seq, req) = primary.next_request(Duration::from_secs(5));
        assert!(
            matches!(req, Request::SubmitOrder { .. }),
            "order must reach the primary, got {req:?}"
        );

        drop(c2);
        gw.shutdown();
        drop(replica);
        drop(primary);
    }

    #[test]
    fn new_order_single_round_trip_to_execution_report() {
        use crate::test_stub::MelinStub;
        use melin_protocol::message::{Request, ResponseKind};
        use melin_types::types::{AccountId, ExecutionReport, Price, Quantity, Side, Symbol};
        use std::num::NonZeroU64;

        let stub = MelinStub::start();
        let config = make_config_with_port("FIRM_A", "MELIN", stub.port());
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // Logon handshake.
        client
            .write_all(&logon_bytes("FIRM_A", "MELIN", 1))
            .unwrap();
        let raw = read_fix_message(&mut client);
        let ack = FixMessage::parse(&raw).unwrap();
        assert_eq!(ack.msg_type(), tags::MSG_LOGON);

        // Send a NewOrderSingle.
        let nos = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build("FIRM_A", "MELIN", 2);
        client.write_all(&nos).unwrap();

        // The stub should receive the translated SubmitOrder.
        let (_seq, req) = stub.next_request(Duration::from_secs(3));
        let order_id = match req {
            Request::SubmitOrder { symbol, order } => {
                assert_eq!(symbol.0, 1);
                assert_eq!(order.side, Side::Buy);
                order.id
            }
            other => panic!("expected SubmitOrder, got {other:?}"),
        };

        // Push a Placed execution report back. The gateway should
        // translate it into a FIX ExecutionReport and forward it.
        stub.send_response(ResponseKind::Report(ExecutionReport::Placed {
            order_id,
            symbol: Symbol(1),
            account: AccountId(7),
            side: Side::Buy,
            price: Price(NonZeroU64::new(5_000_000).unwrap()),
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
        }));

        let raw = read_fix_message(&mut client);
        let er = FixMessage::parse(&raw).unwrap();
        assert_eq!(er.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(er.get_str(tags::CL_ORD_ID), Some("ORD1"));
        assert_eq!(er.get_str(tags::EXEC_TYPE), Some("0")); // New
        assert_eq!(er.get_str(tags::SYMBOL), Some("BTC/USD"));
        assert_eq!(er.get_str(tags::PRICE), Some("50000.00"));

        drop(client);
        gw.shutdown();
        drop(stub);
    }

    #[test]
    fn resend_request_replays_through_real_io_uring() {
        use crate::test_stub::MelinStub;
        use melin_protocol::message::{Request, ResponseKind};
        use melin_types::types::{AccountId, ExecutionReport, Price, Quantity, Side, Symbol};
        use std::num::NonZeroU64;

        let stub = MelinStub::start();
        let config = make_config_with_port("FIRM_A", "MELIN", stub.port());
        let gw = spawn_gateway(config);

        // Use FramedClient — this test reads multiple messages from
        // a single ResendRequest reply, so we need a persistent
        // accumulator across reads.
        let mut client = FramedClient::connect(gw.port);

        // Logon (seq 1) → Logon ack lands as outbound seq 1.
        client.write_all(&logon_bytes("FIRM_A", "MELIN", 1));
        let raw = client.read_message();
        assert_eq!(FixMessage::parse(&raw).unwrap().msg_type(), tags::MSG_LOGON);

        // NewOrderSingle (seq 2) → SubmitOrder → Placed → ER as
        // outbound seq 2.
        let nos = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build("FIRM_A", "MELIN", 2);
        client.write_all(&nos);
        let (_, req) = stub.next_request(Duration::from_secs(3));
        let order_id = match req {
            Request::SubmitOrder { order, .. } => order.id,
            other => panic!("expected SubmitOrder, got {other:?}"),
        };
        stub.send_response(ResponseKind::Report(ExecutionReport::Placed {
            order_id,
            symbol: Symbol(1),
            account: AccountId(7),
            side: Side::Buy,
            price: Price(NonZeroU64::new(5_000_000).unwrap()),
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
        }));
        let raw = client.read_message();
        assert_eq!(
            FixMessage::parse(&raw).unwrap().msg_type(),
            tags::MSG_EXECUTION_REPORT
        );

        // ResendRequest [1, 0] = "everything from seq 1". Store has:
        //   seq 1 = Logon ack (admin) → collapses to GapFill
        //   seq 2 = ER (application) → replays with PossDup=Y
        let rr = FixMessageBuilder::new(tags::MSG_RESEND_REQUEST)
            .u64_tag(tags::BEGIN_SEQ_NO, 1)
            .u64_tag(tags::END_SEQ_NO, 0)
            .build("FIRM_A", "MELIN", 3);
        client.write_all(&rr);

        let frame1 = client.read_message();
        let frame2 = client.read_message();

        let m1 = FixMessage::parse(&frame1).unwrap();
        assert_eq!(m1.msg_type(), tags::MSG_SEQUENCE_RESET);
        assert_eq!(m1.get_str(tags::GAP_FILL_FLAG), Some("Y"));
        assert_eq!(m1.get_str(tags::POSS_DUP_FLAG), Some("Y"));
        assert_eq!(m1.msg_seq_num(), Some(1));
        assert_eq!(m1.get_str(tags::NEW_SEQ_NO), Some("2"));

        let m2 = FixMessage::parse(&frame2).unwrap();
        assert_eq!(m2.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(m2.get_str(tags::POSS_DUP_FLAG), Some("Y"));
        assert!(m2.get_str(tags::ORIG_SENDING_TIME).is_some());
        assert_eq!(
            m2.msg_seq_num(),
            Some(2),
            "replay must preserve original seq"
        );
        assert_eq!(m2.get_str(tags::CL_ORD_ID), Some("ORD1"));

        drop(client);
        gw.shutdown();
        drop(stub);
    }

    #[test]
    fn cancel_rejected_by_engine_yields_order_cancel_reject() {
        use crate::test_stub::MelinStub;
        use melin_protocol::message::{Request, ResponseKind};
        use melin_types::types::{AccountId, ExecutionReport, RejectReason, Symbol};

        let stub = MelinStub::start();
        let config = make_config_with_port("FIRM_A", "MELIN", stub.port());
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // Logon.
        client
            .write_all(&logon_bytes("FIRM_A", "MELIN", 1))
            .unwrap();
        let raw = read_fix_message(&mut client);
        assert_eq!(FixMessage::parse(&raw).unwrap().msg_type(), tags::MSG_LOGON);

        // Submit an order so the session has a ClOrdID → OrderId mapping.
        let nos = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build("FIRM_A", "MELIN", 2);
        client.write_all(&nos).unwrap();
        let (_, req) = stub.next_request(Duration::from_secs(3));
        let order_id = match req {
            Request::SubmitOrder { order, .. } => order.id,
            other => panic!("expected SubmitOrder, got {other:?}"),
        };

        // Now send a cancel for it.
        let cxl = FixMessageBuilder::new(tags::MSG_ORDER_CANCEL_REQUEST)
            .str_tag(tags::CL_ORD_ID, "CXL1")
            .str_tag(tags::ORIG_CL_ORD_ID, "ORD1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORDER_QTY, "10")
            .build("FIRM_A", "MELIN", 3);
        client.write_all(&cxl).unwrap();

        let (_, req) = stub.next_request(Duration::from_secs(3));
        match req {
            Request::CancelOrder {
                order_id: cancel_target,
                ..
            } => assert_eq!(cancel_target, order_id),
            other => panic!("expected CancelOrder, got {other:?}"),
        }

        // Engine rejects the cancel (e.g. already filled).
        stub.send_response(ResponseKind::Report(ExecutionReport::Rejected {
            order_id,
            symbol: Symbol(1),
            account: AccountId(7),
            reason: RejectReason::UnknownOrder,
        }));

        // Gateway should emit an OrderCancelReject (35=9), not an ER.
        let raw = read_fix_message(&mut client);
        let reject = FixMessage::parse(&raw).unwrap();
        assert_eq!(reject.msg_type(), tags::MSG_ORDER_CANCEL_REJECT);
        assert_eq!(reject.get_str(tags::CL_ORD_ID), Some("CXL1"));
        assert_eq!(reject.get_str(tags::ORIG_CL_ORD_ID), Some("ORD1"));

        drop(client);
        gw.shutdown();
        drop(stub);
    }

    #[test]
    fn melin_server_disconnect_mid_session_closes_client() {
        use crate::test_stub::MelinStub;

        let stub = MelinStub::start();
        let config = make_config_with_port("FIRM_A", "MELIN", stub.port());
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client
            .write_all(&logon_bytes("FIRM_A", "MELIN", 1))
            .unwrap();
        let raw = read_fix_message(&mut client);
        assert_eq!(FixMessage::parse(&raw).unwrap().msg_type(), tags::MSG_LOGON);

        // Stub drops the connection mid-session. This is done by
        // dropping the stub handle entirely — which joins the thread
        // after flipping shutdown. The accepted stream is dropped too,
        // sending FIN to the gateway.
        drop(stub);

        // The gateway's multishot RECV on the Melin fd should fire
        // with result=0, trigger session cleanup, and FIN the client.
        let mut tail = [0u8; 64];
        let n = client.read(&mut tail).expect("final client read");
        assert_eq!(n, 0, "client should see EOF after Melin disconnect");

        drop(client);
        gw.shutdown();
    }

    #[test]
    fn melin_connect_refused_disconnects_client_promptly() {
        // No stub — point the gateway at a port nothing is listening
        // on. The kernel should immediately refuse the connect.
        //
        // Pick a port by binding a throwaway listener, reading back
        // the assigned port, then dropping the listener so that port
        // is free again. Small race window but in practice reliable
        // on loopback.
        let port = {
            let throwaway = TcpListener::bind("127.0.0.1:0").unwrap();
            throwaway.local_addr().unwrap().port()
        };
        let config = make_config_with_port("FIRM_A", "MELIN", port);
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client
            .write_all(&logon_bytes("FIRM_A", "MELIN", 1))
            .unwrap();

        // The gateway's io_uring CONNECT to the bogus port returns
        // -ECONNREFUSED and the session is torn down. The client
        // should see EOF without ever receiving a Logon ack.
        let mut tail = [0u8; 64];
        let n = client.read(&mut tail).expect("client read");
        assert_eq!(n, 0, "expected EOF after connect refused");

        drop(client);
        gw.shutdown();
    }

    #[test]
    fn client_disconnect_mid_session_closes_melin_connection() {
        use crate::test_stub::MelinStub;

        let stub = MelinStub::start();
        let config = make_config_with_port("FIRM_A", "MELIN", stub.port());
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client
            .write_all(&logon_bytes("FIRM_A", "MELIN", 1))
            .unwrap();
        let raw = read_fix_message(&mut client);
        assert_eq!(FixMessage::parse(&raw).unwrap().msg_type(), tags::MSG_LOGON);

        // Symmetric to the Melin-disconnect test: client closes,
        // gateway should close its Melin socket. The stub's read
        // loop observes EOF and sets `disconnected`.
        drop(client);

        assert!(
            stub.wait_for_disconnect(Duration::from_secs(3)),
            "gateway did not close Melin socket after client disconnect"
        );

        gw.shutdown();
        drop(stub);
    }

    #[test]
    fn auth_failed_from_melin_closes_client_session() {
        // This variant of the stub answers the handshake with AuthFailed
        // instead of ServerReady. The gateway should Logout the client
        // and close the connection.
        //
        // The existing `MelinStub` auto-sends ServerReady, so we bypass
        // it and use a bespoke one-shot listener here.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server_port = listener.local_addr().unwrap().port();
        let stub_thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Send Challenge.
            let mut buf = [0u8; 128];
            let n = melin_protocol::codec::encode_response(
                &melin_protocol::message::ResponseKind::Challenge { nonce: [0u8; 32] },
                &mut buf,
            )
            .unwrap();
            stream.write_all(&buf[..n]).unwrap();

            // Read and discard the ChallengeResponse frame.
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).unwrap();

            // Send AuthFailed instead of ServerReady.
            let n = melin_protocol::codec::encode_response(
                &melin_protocol::message::ResponseKind::AuthFailed,
                &mut buf,
            )
            .unwrap();
            stream.write_all(&buf[..n]).unwrap();
            // Let the gateway see the AuthFailed and react.
            let _ = stream.read(&mut buf);
        });

        let config = make_config_with_port("FIRM_A", "MELIN", server_port);
        let gw = spawn_gateway(config);

        let mut client = TcpStream::connect(("127.0.0.1", gw.port)).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        client
            .write_all(&logon_bytes("FIRM_A", "MELIN", 1))
            .unwrap();

        // Expect a Logout (not a Logon ack).
        let raw = read_fix_message(&mut client);
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_LOGOUT);

        // Connection should close.
        let mut tail = [0u8; 16];
        assert_eq!(client.read(&mut tail).unwrap(), 0);

        drop(client);
        gw.shutdown();
        let _ = stub_thread.join();
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
