//! Single-threaded io_uring event loop for the market data FIX gateway.
//!
//! Multiplexes all FIX client connections on a single io_uring ring.
//! Unlike the order-entry gateway, the md-gateway does NOT maintain
//! per-session connections to the melin server — the MarketDataCore
//! thread handles event publisher subscriptions separately.
//!
//! Uses multishot RECV with provided buffer groups for efficient I/O
//! multiplexing (same pattern as `melin-server`'s reader.rs and the
//! order-entry gateway).

use std::net::TcpListener;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error, info, warn};

use melin_gateway_core::fix::parse::FixMessage;
use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;
use melin_market_data::core::MdState;

use crate::config::GatewayConfig;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Size of each provided buffer. 4 KiB accommodates multiple FIX messages
/// per recv (FIX messages are typically ~200 bytes).
const BUF_SIZE: usize = 4096;

/// Number of provided buffers in the shared pool. 256 is ample for a
/// market data gateway handling ~100 FIX sessions.
const NUM_BUFFERS: u16 = 256;

/// Buffer group ID for the provided recv buffer pool.
const BUF_GROUP_ID: u16 = 0;

/// io_uring submission queue depth. Sized for accept + multishot RECVs +
/// SENDs + buffer re-provisions across ~100 sessions.
const RING_SIZE: u32 = 1024;

/// Heartbeat check interval. Used as a timeout so the event loop does
/// not block indefinitely on an empty ring.
const HEARTBEAT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

// ---------------------------------------------------------------------------
// User data encoding
// ---------------------------------------------------------------------------

/// Operation types encoded in the upper byte of io_uring user_data.
const OP_ACCEPT: u64 = 0x00 << 56;
const OP_FIX_RECV: u64 = 0x01 << 56;
const OP_SEND_FIX: u64 = 0x02 << 56;
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
struct Slab {
    entries: Vec<Option<MdSession>>,
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

    fn insert(&mut self, session: MdSession) -> usize {
        if let Some(idx) = self.free.pop() {
            self.entries[idx] = Some(session);
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(session));
            idx
        }
    }

    fn remove(&mut self, idx: usize) -> Option<MdSession> {
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

    fn get(&self, idx: usize) -> Option<&MdSession> {
        self.entries.get(idx).and_then(|e| e.as_ref())
    }

    fn get_mut(&mut self, idx: usize) -> Option<&mut MdSession> {
        self.entries.get_mut(idx).and_then(|e| e.as_mut())
    }
}

// ---------------------------------------------------------------------------
// MdSession
// ---------------------------------------------------------------------------

/// Session state for a connected FIX market data client.
#[derive(Debug, PartialEq, Eq)]
enum SessionState {
    /// Waiting for the client's Logon message.
    AwaitingLogon,
    /// Logon exchanged, session is active.
    Active,
    /// Draining outbound data before close.
    Closing,
}

/// A single FIX client session on the market data gateway.
struct MdSession {
    state: SessionState,
    // `OwnedFd` closes the socket on drop — no manual `libc::close`.
    fix_fd: OwnedFd,
    /// Accumulates partial FIX data from multishot recv until a
    /// complete message can be extracted.
    fix_parse_buf: Vec<u8>,
    /// Outbound FIX data staged by message handlers. Swapped into
    /// `fix_inflight` when the event loop submits a SEND SQE.
    fix_send_buf: Vec<u8>,
    /// Bytes currently being written by an in-flight io_uring SEND.
    /// Must not be touched until the corresponding CQE arrives.
    fix_inflight: Vec<u8>,
    /// Whether the multishot recv is still active on this fd.
    fix_multishot_active: bool,
    /// Client's SenderCompID (set on successful Logon).
    sender_comp_id: String,
    /// Next expected inbound MsgSeqNum.
    fix_inbound_seq: u64,
    /// Next outbound MsgSeqNum.
    fix_outbound_seq: u64,
    /// Negotiated heartbeat interval.
    heartbeat_interval: Duration,
}

impl MdSession {
    fn new(fix_fd: OwnedFd) -> Self {
        Self {
            state: SessionState::AwaitingLogon,
            fix_fd,
            fix_parse_buf: Vec::with_capacity(512),
            fix_send_buf: Vec::with_capacity(512),
            fix_inflight: Vec::with_capacity(512),
            fix_multishot_active: false,
            sender_comp_id: String::new(),
            fix_inbound_seq: 1,
            fix_outbound_seq: 1,
            heartbeat_interval: Duration::from_secs(30),
        }
    }

    /// Queue a serialized FIX message into the outbound send buffer
    /// and advance the outbound sequence number.
    fn queue_fix(&mut self, msg: &[u8]) {
        self.fix_send_buf.extend_from_slice(msg);
        self.fix_outbound_seq += 1;
    }

    /// Build and queue a Logout message with the given reason text,
    /// then transition to Closing.
    fn queue_fix_logout(&mut self, config: &GatewayConfig, text: &str) {
        let target = if self.sender_comp_id.is_empty() {
            "UNKNOWN"
        } else {
            &self.sender_comp_id
        };
        let msg = FixMessageBuilder::new(tags::MSG_LOGOUT)
            .str_tag(tags::TEXT, text)
            .build(&config.sender_comp_id, target, self.fix_outbound_seq);
        self.queue_fix(&msg);
        self.state = SessionState::Closing;
    }

    /// Build and queue a Reject (MsgType=3) for a session-level error.
    fn queue_fix_reject(&mut self, config: &GatewayConfig, text: &str) {
        let target = if self.sender_comp_id.is_empty() {
            "UNKNOWN"
        } else {
            &self.sender_comp_id
        };
        let msg = FixMessageBuilder::new(tags::MSG_REJECT)
            .str_tag(tags::TEXT, text)
            .build(&config.sender_comp_id, target, self.fix_outbound_seq);
        self.queue_fix(&msg);
    }

    /// Handle one complete FIX message. Returns the action the event
    /// loop should take.
    fn handle_fix_message(
        &mut self,
        raw: &[u8],
        config: &GatewayConfig,
        md_state: &Arc<RwLock<MdState>>,
    ) -> SessionAction {
        match self.state {
            SessionState::AwaitingLogon => self.handle_logon(raw, config),
            SessionState::Active => self.handle_active_fix(raw, config, md_state),
            SessionState::Closing => {
                // Draining — ignore further inbound messages.
                SessionAction::None
            }
        }
    }

    /// Process a Logon message. Validates SenderCompID and
    /// TargetCompID, then sends back a Logon response.
    fn handle_logon(&mut self, raw: &[u8], config: &GatewayConfig) -> SessionAction {
        let msg = match FixMessage::parse(raw) {
            Ok(m) => m,
            Err(e) => {
                debug!(error = %e, "malformed FIX Logon");
                return SessionAction::Close;
            }
        };

        if msg.msg_type() != tags::MSG_LOGON {
            self.queue_fix_logout(config, "first message must be Logon");
            return SessionAction::Close;
        }

        // FIX 4.4 section 4.5: TargetCompID must match our SenderCompID.
        if msg.target_comp_id() != Some(config.sender_comp_id.as_str()) {
            warn!(
                target = msg.target_comp_id().unwrap_or("?"),
                expected = %config.sender_comp_id,
                "Logon TargetCompID mismatch"
            );
            self.queue_fix_logout(config, "invalid TargetCompID");
            return SessionAction::Close;
        }

        let sender_comp_id = match msg.sender_comp_id() {
            Some(s) => s,
            None => {
                self.queue_fix_logout(config, "Logon missing SenderCompID");
                return SessionAction::Close;
            }
        };

        // The md-gateway authenticates by SenderCompID presence in the
        // symbols config — any connected client with a valid CompID can
        // subscribe. More granular auth (e.g., per-symbol ACLs) is a
        // follow-up.

        // Validate MsgSeqNum — Logon must be sequence 1.
        if let Some(seq) = msg.msg_seq_num()
            && seq != 1
        {
            warn!(sender = sender_comp_id, seq, "Logon MsgSeqNum must be 1");
            self.queue_fix_logout(config, "MsgSeqNum must be 1 on Logon");
            return SessionAction::Close;
        }

        // Extract HeartBtInt. Malformed or missing falls back to 30s.
        let heartbeat_secs: u64 = msg
            .get_str(tags::HEART_BT_INT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        info!(sender = sender_comp_id, "FIX Logon received");

        // Store session info.
        self.sender_comp_id = sender_comp_id.to_owned();
        self.heartbeat_interval = Duration::from_secs(heartbeat_secs);
        self.fix_inbound_seq = 2; // Logon was seq 1.

        // Build Logon response.
        let logon_response = FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .u64_tag(tags::HEART_BT_INT, heartbeat_secs)
            .build(
                &config.sender_comp_id,
                &self.sender_comp_id,
                self.fix_outbound_seq,
            );
        self.queue_fix(&logon_response);

        self.state = SessionState::Active;
        SessionAction::SendFix
    }

    /// Handle a FIX message on an active session.
    fn handle_active_fix(
        &mut self,
        raw: &[u8],
        config: &GatewayConfig,
        md_state: &Arc<RwLock<MdState>>,
    ) -> SessionAction {
        let msg = match FixMessage::parse(raw) {
            Ok(m) => m,
            Err(e) => {
                debug!(error = %e, "malformed FIX message");
                self.queue_fix_reject(config, "malformed message");
                self.fix_inbound_seq += 1;
                return SessionAction::SendFix;
            }
        };

        // Validate MsgSeqNum if present.
        if let Some(seq) = msg.msg_seq_num()
            && seq != self.fix_inbound_seq
        {
            debug!(
                expected = self.fix_inbound_seq,
                got = seq,
                "FIX sequence gap"
            );
            // For the md-gateway we take the simple approach: reject
            // and close. Full gap-fill is overkill for market data.
            self.queue_fix_logout(config, "MsgSeqNum gap detected");
            return SessionAction::Close;
        }
        self.fix_inbound_seq += 1;

        let msg_type = msg.msg_type();

        if msg_type == tags::MSG_HEARTBEAT {
            // Client heartbeat — no response needed.
            SessionAction::None
        } else if msg_type == tags::MSG_TEST_REQUEST {
            // Respond with Heartbeat carrying the TestReqID.
            let test_req_id = msg.get_str(tags::TEST_REQ_ID).unwrap_or("");
            let hb = FixMessageBuilder::new(tags::MSG_HEARTBEAT)
                .str_tag(tags::TEST_REQ_ID, test_req_id)
                .build(
                    &config.sender_comp_id,
                    &self.sender_comp_id,
                    self.fix_outbound_seq,
                );
            self.queue_fix(&hb);
            SessionAction::SendFix
        } else if msg_type == tags::MSG_LOGOUT {
            info!(sender = %self.sender_comp_id, "FIX Logout received");
            // Respond with Logout.
            let logout = FixMessageBuilder::new(tags::MSG_LOGOUT)
                .str_tag(tags::TEXT, "goodbye")
                .build(
                    &config.sender_comp_id,
                    &self.sender_comp_id,
                    self.fix_outbound_seq,
                );
            self.queue_fix(&logout);
            SessionAction::Close
        } else if msg_type == tags::MSG_MARKET_DATA_REQUEST {
            self.handle_market_data_request(&msg, config, md_state)
        } else if msg_type == tags::MSG_SECURITY_LIST_REQUEST {
            self.handle_security_list_request(&msg, config)
        } else {
            // Unknown or unsupported message type — send Reject.
            debug!(
                sender = %self.sender_comp_id,
                msg_type = ?msg_type,
                "unsupported FIX message type"
            );
            self.queue_fix_reject(config, "unsupported MsgType");
            SessionAction::SendFix
        }
    }

    /// Handle SecurityListRequest (35=x). Responds with a SecurityList (35=y)
    /// Handle MarketDataRequest (35=V). Reads the current book state
    /// from the shared mirrors and sends a MarketDataSnapshotFullRefresh (W).
    fn handle_market_data_request(
        &mut self,
        msg: &FixMessage<'_>,
        config: &GatewayConfig,
        md_state: &Arc<RwLock<MdState>>,
    ) -> SessionAction {
        let md_req_id = msg.get_str(tags::MD_REQ_ID).unwrap_or("");

        // Collect requested symbols from the NoRelatedSym group.
        let requested_symbols: Vec<&str> = msg
            .fields_iter()
            .filter(|f| f.tag == tags::SYMBOL)
            .filter_map(|f| std::str::from_utf8(f.value).ok())
            .collect();

        if requested_symbols.is_empty() {
            let reject =
                crate::translate::md_request_reject(md_req_id, "0", "no symbols specified").build(
                    &config.sender_comp_id,
                    &self.sender_comp_id,
                    self.fix_outbound_seq,
                );
            self.queue_fix(&reject);
            return SessionAction::SendFix;
        }

        // Read the shared mirror state.
        let state = match md_state.read() {
            Ok(s) => s,
            Err(_) => {
                let reject = crate::translate::md_request_reject(md_req_id, "0", "internal error")
                    .build(
                        &config.sender_comp_id,
                        &self.sender_comp_id,
                        self.fix_outbound_seq,
                    );
                self.queue_fix(&reject);
                return SessionAction::SendFix;
            }
        };

        if !state.ready {
            let reject =
                crate::translate::md_request_reject(md_req_id, "0", "market data not ready").build(
                    &config.sender_comp_id,
                    &self.sender_comp_id,
                    self.fix_outbound_seq,
                );
            self.queue_fix(&reject);
            return SessionAction::SendFix;
        }

        // Send one W per requested symbol.
        for sym_str in &requested_symbols {
            // Look up symbol ID from config.
            let sym_cfg = config.symbols.get(*sym_str);
            let sym_id = sym_cfg.map(|c| melin_trading::types::Symbol(c.id));
            let tick_inverse = sym_cfg.map_or(1, |c| c.tick_inverse);

            let (bids, asks) = if let Some(id) = sym_id
                && let Some(mirror) = state.mirrors.get(&id)
            {
                // Collect levels: bids descending, asks ascending.
                let bids: Vec<_> = mirror.bids().iter().rev().map(|(&p, &l)| (p, l)).collect();
                let asks: Vec<_> = mirror.asks().iter().map(|(&p, &l)| (p, l)).collect();
                (bids, asks)
            } else {
                (Vec::new(), Vec::new())
            };

            let snapshot = crate::translate::md_snapshot_to_fix(
                md_req_id,
                sym_str,
                &bids,
                &asks,
                tick_inverse,
            )
            .build(
                &config.sender_comp_id,
                &self.sender_comp_id,
                self.fix_outbound_seq,
            );
            self.queue_fix(&snapshot);
        }

        SessionAction::SendFix
    }

    /// containing all configured symbols.
    fn handle_security_list_request(
        &mut self,
        msg: &FixMessage<'_>,
        config: &GatewayConfig,
    ) -> SessionAction {
        let req_id = msg.get_str(tags::SECURITY_REQ_ID).unwrap_or("");
        info!(sender = %self.sender_comp_id, req_id, "SecurityListRequest");

        let symbols: Vec<crate::translate::SecurityInfo> = config
            .symbols
            .iter()
            .map(|(name, sym_cfg)| crate::translate::SecurityInfo {
                symbol: name.clone(),
                base_ccy: sym_cfg.base_ccy.clone(),
                quote_ccy: sym_cfg.quote_ccy.clone(),
                min_price_increment: if sym_cfg.tick_inverse > 1 {
                    crate::translate::ticks_to_decimal(1, sym_cfg.tick_inverse)
                } else {
                    "1".to_string()
                },
                round_lot: if sym_cfg.lot_inverse > 1 {
                    crate::translate::ticks_to_decimal(1, sym_cfg.lot_inverse)
                } else {
                    "1".to_string()
                },
            })
            .collect();

        let response = crate::translate::security_list_to_fix(req_id, &symbols).build(
            &config.sender_comp_id,
            &self.sender_comp_id,
            self.fix_outbound_seq,
        );
        self.queue_fix(&response);
        SessionAction::SendFix
    }
}

// ---------------------------------------------------------------------------
// SessionAction
// ---------------------------------------------------------------------------

/// Actions the event loop should take after a session processes a message.
#[derive(Debug, PartialEq, Eq)]
pub enum SessionAction {
    /// No I/O needed.
    None,
    /// Flush FIX send buffer.
    SendFix,
    /// Send pending data and close the session.
    Close,
}

// ---------------------------------------------------------------------------
// MdGateway — main event loop state
// ---------------------------------------------------------------------------

pub struct MdGateway {
    ring: IoUring,
    config: &'static GatewayConfig,
    // `OwnedFd` closes the listener on drop. Session fds are closed
    // the same way when `MdSession` is dropped out of the slab.
    listener_fd: OwnedFd,
    sessions: Slab,
    /// Shared book mirror state from the MarketDataCore thread.
    md_state: Arc<RwLock<MdState>>,
    /// Contiguous buffer pool for io_uring provided buffers.
    buffer_pool: Box<[u8]>,
    /// Pre-allocated CQE drain buffer: (user_data, result, flags).
    cqes: Vec<(u64, i32, u32)>,
    /// Session indices to remove after CQE processing.
    to_remove: Vec<usize>,
    /// Sessions with pending outbound data to flush.
    dirty_fix: Vec<usize>,
    /// Stable storage for the timeout timespec — io_uring reads the
    /// pointer at submit time, so it must outlive the SQE.
    timeout_ts: types::Timespec,
}

impl MdGateway {
    /// Create the gateway and register the listener + buffer pool with
    /// io_uring.
    pub fn new(
        listener: TcpListener,
        config: &'static GatewayConfig,
        md_state: Arc<RwLock<MdState>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut ring = IoUring::new(RING_SIZE)?;
        // `TcpListener` owns the fd; converting to `OwnedFd` keeps
        // ownership without leaking the wrapper.
        let listener_fd: OwnedFd = listener.into();

        // Register the provided buffer pool.
        let mut buffer_pool = vec![0u8; NUM_BUFFERS as usize * BUF_SIZE].into_boxed_slice();
        register_buffer_pool(&mut ring, buffer_pool.as_mut_ptr());

        Ok(Self {
            ring,
            config,
            listener_fd,
            sessions: Slab::new(),
            md_state,
            buffer_pool,
            cqes: Vec::with_capacity(RING_SIZE as usize),
            to_remove: Vec::new(),
            dirty_fix: Vec::new(),
            timeout_ts: types::Timespec::new()
                .sec(HEARTBEAT_CHECK_INTERVAL.as_secs())
                .nsec(HEARTBEAT_CHECK_INTERVAL.subsec_nanos()),
        })
    }

    /// Run the event loop. Blocks until shutdown.
    pub fn run(&mut self, shutdown: &AtomicBool) -> Result<(), Box<dyn std::error::Error>> {
        // Submit the first ACCEPT and a timeout so submit_and_wait
        // does not block indefinitely when no clients are connected.
        self.push_accept();
        self.push_timeout();

        info!("md-gateway io_uring event loop started");

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
                    // Timeout expired — resubmit for the next interval.
                    self.push_timeout();
                    continue;
                }

                match op_type(token) {
                    OP_ACCEPT => self.handle_accept(result),
                    OP_FIX_RECV => self.handle_fix_recv(slab_idx(token), result, flags),
                    OP_SEND_FIX => self.handle_fix_send_complete(slab_idx(token), result),
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
        }

        info!("md-gateway io_uring event loop stopped");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // ACCEPT
    // -----------------------------------------------------------------------

    fn push_accept(&mut self) {
        let sqe = opcode::Accept::new(
            types::Fd(self.listener_fd.as_raw_fd()),
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

    fn handle_accept(&mut self, result: i32) {
        // Always resubmit ACCEPT for the next connection.
        self.push_accept();

        if result < 0 {
            let err = std::io::Error::from_raw_os_error(-result);
            debug!(error = %err, "accept failed");
            return;
        }

        let fd = result;

        set_tcp_nodelay(fd);

        let peer = get_peer_addr(fd);
        info!(peer = %peer, fd, "FIX client connected");

        // SAFETY: `fd` was just returned by accept(2); no other owner
        // exists until we wrap it here, and the kernel guarantees the
        // descriptor is fresh and unique.
        let owned_fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let session = MdSession::new(owned_fd);
        let idx = self.sessions.insert(session);

        // Submit multishot RECV on the FIX client socket.
        self.push_fix_recv_multi(idx);
    }

    // -----------------------------------------------------------------------
    // TIMEOUT
    // -----------------------------------------------------------------------

    fn push_timeout(&mut self) {
        let sqe = opcode::Timeout::new(&self.timeout_ts)
            .build()
            .user_data(TIMEOUT_TOKEN);

        unsafe {
            self.ring
                .submission()
                .push(&sqe)
                .expect("io_uring SQ full during timeout");
        }
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

        let sqe = opcode::RecvMulti::new(types::Fd(session.fix_fd.as_raw_fd()), BUF_GROUP_ID)
            .build()
            .user_data(OP_FIX_RECV | idx as u64);

        unsafe {
            self.ring.submission().push(&sqe).expect("io_uring SQ full");
        }
        session.fix_multishot_active = true;
    }

    fn handle_fix_recv(&mut self, idx: usize, result: i32, flags: u32) {
        let has_more = (flags & IORING_CQE_F_MORE) != 0;

        if result <= 0 {
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

            let action = session.handle_fix_message(&raw, self.config, &self.md_state);

            match action {
                SessionAction::None => {}
                SessionAction::SendFix => {
                    self.dirty_fix.push(idx);
                }
                SessionAction::Close => {
                    self.dirty_fix.push(idx);
                    self.to_remove.push(idx);
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
                types::Fd(session.fix_fd.as_raw_fd()),
                session.fix_inflight.as_ptr(),
                session.fix_inflight.len() as u32,
            )
            .build()
            .user_data(OP_SEND_FIX | idx as u64);

            unsafe {
                self.ring.submission().push(&sqe).expect("io_uring SQ full");
            }
        }
    }

    fn handle_fix_send_complete(&mut self, idx: usize, result: i32) {
        if result < 0 {
            // SEND error — the kernel is no longer reading the buffer,
            // so clear inflight to allow drain_removals to proceed.
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
                let remove = !requeue && matches!(session.state, SessionState::Closing);
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

    // -----------------------------------------------------------------------
    // Session removal
    // -----------------------------------------------------------------------

    /// Remove sessions from the slab, deferring any that still have
    /// io_uring SEND SQEs in flight (their inflight buffers are
    /// non-empty and the kernel may still be reading from them).
    fn drain_removals(&mut self) {
        let pending: Vec<usize> = self.to_remove.drain(..).collect();
        for idx in pending {
            let can_remove = self
                .sessions
                .get(idx)
                .is_none_or(|s| s.fix_inflight.is_empty());
            if can_remove {
                if let Some(session) = self.sessions.remove(idx) {
                    debug!(sender = %session.sender_comp_id, "session removed");
                    // Drop closes the fd. The kernel cancels outstanding
                    // io_uring ops on close.
                    drop(session);
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

#[cfg(test)]
mod tests {
    use super::*;
    use melin_gateway_core::fix::parse::FixMessage;
    use melin_gateway_core::fix::serialize::FixMessageBuilder;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::Arc;

    fn make_config(sender: &str) -> &'static GatewayConfig {
        let config = GatewayConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            event_publisher: "127.0.0.1:1".parse().unwrap(),
            authorized_keys: std::path::PathBuf::new(),
            subscriber_key: std::path::PathBuf::new(),
            core: 0,
            sender_comp_id: sender.to_string(),
            symbols: std::collections::HashMap::new(),
        };
        Box::leak(Box::new(config))
    }

    fn logon_bytes(sender: &str, target: &str, seq: u64) -> Vec<u8> {
        FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .u64_tag(tags::HEART_BT_INT, 30)
            .build(sender, target, seq)
    }

    struct GwHandle {
        port: u16,
        shutdown: Arc<AtomicBool>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl GwHandle {
        fn shutdown(mut self) {
            self.shutdown.store(true, Ordering::Relaxed);
            if let Some(h) = self.handle.take() {
                // Connect to unblock the accept.
                let _ = TcpStream::connect(format!("127.0.0.1:{}", self.port));
                let _ = h.join();
            }
        }
    }

    impl Drop for GwHandle {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Relaxed);
            if let Some(h) = self.handle.take() {
                let _ = TcpStream::connect(format!("127.0.0.1:{}", self.port));
                let _ = h.join();
            }
        }
    }

    fn spawn_gateway(config: &'static GatewayConfig) -> GwHandle {
        spawn_gateway_with_state(config, Arc::new(RwLock::new(MdState::new())))
    }

    fn spawn_gateway_with_state(
        config: &'static GatewayConfig,
        state: Arc<RwLock<MdState>>,
    ) -> GwHandle {
        let listener = TcpListener::bind(config.listen).unwrap();
        let port = listener.local_addr().unwrap().port();
        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);

        let handle = std::thread::Builder::new()
            .name("test-md-gw".into())
            .spawn(move || {
                let mut gw = MdGateway::new(listener, config, state).unwrap();
                let _ = gw.run(&s);
            })
            .unwrap();

        // Give the thread time to bind and start accepting.
        std::thread::sleep(Duration::from_millis(50));

        GwHandle {
            port,
            shutdown,
            handle: Some(handle),
        }
    }

    fn read_fix_message(stream: &mut TcpStream) -> Vec<u8> {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut buf = Vec::new();
        let mut tmp = [0u8; 256];
        loop {
            match stream.read(&mut tmp) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&tmp[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
                Err(e) => panic!("read error: {e}"),
            }
            if let Some(msg) = melin_gateway_core::fix::parse::try_extract_message(&mut buf.clone())
            {
                return msg;
            }
        }
        buf
    }

    #[test]
    fn logon_and_logout_flow() {
        let config = make_config("MELIN-MD");
        let gw = spawn_gateway(config);

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", gw.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        // Send Logon.
        let logon = logon_bytes("CLIENT", "MELIN-MD", 1);
        stream.write_all(&logon).unwrap();
        stream.flush().unwrap();

        // Read Logon response.
        let resp = read_fix_message(&mut stream);
        let msg = FixMessage::parse(&resp).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_LOGON);
        assert_eq!(msg.sender_comp_id(), Some("MELIN-MD"));
        assert_eq!(msg.target_comp_id(), Some("CLIENT"));

        // Send Logout.
        let logout = FixMessageBuilder::new(tags::MSG_LOGOUT)
            .str_tag(tags::TEXT, "done")
            .build("CLIENT", "MELIN-MD", 2);
        stream.write_all(&logout).unwrap();
        stream.flush().unwrap();

        // Read Logout response.
        let resp = read_fix_message(&mut stream);
        let msg = FixMessage::parse(&resp).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_LOGOUT);

        gw.shutdown();
    }

    #[test]
    fn wrong_target_comp_id_gets_logout() {
        let config = make_config("MELIN-MD");
        let gw = spawn_gateway(config);

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", gw.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        // Send Logon with wrong TargetCompID.
        let logon = logon_bytes("CLIENT", "WRONG-TARGET", 1);
        stream.write_all(&logon).unwrap();
        stream.flush().unwrap();

        // Should get a Logout with error text.
        let resp = read_fix_message(&mut stream);
        let msg = FixMessage::parse(&resp).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_LOGOUT);
        assert!(
            msg.get_str(tags::TEXT)
                .unwrap_or("")
                .contains("TargetCompID")
        );

        gw.shutdown();
    }

    /// Build a config with a single BTCUSD symbol (id=1, tick_inverse=1, lot_inverse=1).
    fn make_config_with_btcusd() -> &'static GatewayConfig {
        let mut config = GatewayConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            event_publisher: "127.0.0.1:1".parse().unwrap(),
            authorized_keys: std::path::PathBuf::new(),
            subscriber_key: std::path::PathBuf::new(),
            core: 0,
            sender_comp_id: "MELIN-MD".to_string(),
            symbols: std::collections::HashMap::new(),
        };
        config.symbols.insert(
            "BTCUSD".to_string(),
            crate::config::SymbolConfig {
                id: 1,
                tick_inverse: 1,
                lot_inverse: 1,
                base_ccy: "BTC".to_string(),
                quote_ccy: "USD".to_string(),
            },
        );
        Box::leak(Box::new(config))
    }

    /// Build a MarketDataRequest (35=V) FIX message.
    fn market_data_request_bytes(
        sender: &str,
        target: &str,
        seq: u64,
        md_req_id: &str,
        symbols: &[&str],
    ) -> Vec<u8> {
        let mut builder = FixMessageBuilder::new(tags::MSG_MARKET_DATA_REQUEST)
            .str_tag(tags::MD_REQ_ID, md_req_id)
            .str_tag(tags::SUBSCRIPTION_REQUEST_TYPE, "1")
            .str_tag(tags::NO_RELATED_SYM, &symbols.len().to_string());
        for sym in symbols {
            builder = builder.str_tag(tags::SYMBOL, sym);
        }
        builder.build(sender, target, seq)
    }

    #[test]
    fn market_data_request_returns_snapshot() {
        use melin_market_data::mirror::BookMirror;
        use melin_trading::types::{
            AccountId, ExecutionReport, OrderId, Price, Quantity, Side, Symbol,
        };
        use std::num::NonZeroU64;

        let config = make_config_with_btcusd();

        // Seed MdState with a book for Symbol(1).
        let state = Arc::new(RwLock::new(MdState::new()));
        {
            let mut s = state.write().unwrap();
            let mut mirror = BookMirror::new(Symbol(1));
            // Place a bid at price 100 qty 10.
            mirror.apply(&ExecutionReport::Placed {
                order_id: OrderId(1),
                symbol: Symbol(1),
                account: AccountId(1),
                side: Side::Buy,
                price: Price(NonZeroU64::new(100).unwrap()),
                quantity: Quantity(NonZeroU64::new(10).unwrap()),
            });
            // Place an ask at price 200 qty 5.
            mirror.apply(&ExecutionReport::Placed {
                order_id: OrderId(2),
                symbol: Symbol(1),
                account: AccountId(1),
                side: Side::Sell,
                price: Price(NonZeroU64::new(200).unwrap()),
                quantity: Quantity(NonZeroU64::new(5).unwrap()),
            });
            s.mirrors.insert(Symbol(1), mirror);
            s.ready = true;
        }

        let gw = spawn_gateway_with_state(config, state);

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", gw.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        // Logon.
        let logon = logon_bytes("CLIENT", "MELIN-MD", 1);
        stream.write_all(&logon).unwrap();
        stream.flush().unwrap();
        let _ = read_fix_message(&mut stream); // consume Logon response

        // Send MarketDataRequest.
        let req = market_data_request_bytes("CLIENT", "MELIN-MD", 2, "REQ1", &["BTCUSD"]);
        stream.write_all(&req).unwrap();
        stream.flush().unwrap();

        // Read MarketDataSnapshotFullRefresh (35=W).
        let resp = read_fix_message(&mut stream);
        let msg = FixMessage::parse(&resp).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_MD_SNAPSHOT);
        assert_eq!(msg.get_str(tags::MD_REQ_ID), Some("REQ1"));
        assert_eq!(msg.get_str(tags::SYMBOL), Some("BTCUSD"));
        assert_eq!(msg.get_str(tags::NO_MD_ENTRIES), Some("2"));

        // Verify entry types: first entry is bid (0), second is ask (1).
        let entry_types: Vec<_> = msg
            .fields_iter()
            .filter(|f| f.tag == tags::MD_ENTRY_TYPE)
            .map(|f| std::str::from_utf8(f.value).unwrap().to_string())
            .collect();
        assert_eq!(entry_types, vec!["0", "1"]);

        // Verify prices.
        let prices: Vec<_> = msg
            .fields_iter()
            .filter(|f| f.tag == tags::MD_ENTRY_PX)
            .map(|f| std::str::from_utf8(f.value).unwrap().to_string())
            .collect();
        assert_eq!(prices, vec!["100", "200"]);

        // Verify sizes.
        let sizes: Vec<_> = msg
            .fields_iter()
            .filter(|f| f.tag == tags::MD_ENTRY_SIZE)
            .map(|f| std::str::from_utf8(f.value).unwrap().to_string())
            .collect();
        assert_eq!(sizes, vec!["10", "5"]);

        gw.shutdown();
    }

    #[test]
    fn market_data_request_rejects_when_not_ready() {
        let config = make_config_with_btcusd();

        // MdState with ready = false (the default).
        let state = Arc::new(RwLock::new(MdState::new()));

        let gw = spawn_gateway_with_state(config, state);

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", gw.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        // Logon.
        let logon = logon_bytes("CLIENT", "MELIN-MD", 1);
        stream.write_all(&logon).unwrap();
        stream.flush().unwrap();
        let _ = read_fix_message(&mut stream); // consume Logon response

        // Send MarketDataRequest.
        let req = market_data_request_bytes("CLIENT", "MELIN-MD", 2, "REQ1", &["BTCUSD"]);
        stream.write_all(&req).unwrap();
        stream.flush().unwrap();

        // Should get a MarketDataRequestReject (35=Y).
        let resp = read_fix_message(&mut stream);
        let msg = FixMessage::parse(&resp).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_MD_REQUEST_REJECT);
        assert_eq!(msg.get_str(tags::MD_REQ_ID), Some("REQ1"));
        assert!(
            msg.get_str(tags::TEXT).unwrap_or("").contains("not ready"),
            "expected reject text to mention 'not ready', got: {:?}",
            msg.get_str(tags::TEXT)
        );

        gw.shutdown();
    }

    #[test]
    fn market_data_request_rejects_empty_symbols() {
        let config = make_config_with_btcusd();

        // Ready state, but we send a V with no Symbol tags.
        let state = Arc::new(RwLock::new(MdState::new()));
        {
            let mut s = state.write().unwrap();
            s.ready = true;
        }

        let gw = spawn_gateway_with_state(config, state);

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", gw.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        // Logon.
        let logon = logon_bytes("CLIENT", "MELIN-MD", 1);
        stream.write_all(&logon).unwrap();
        stream.flush().unwrap();
        let _ = read_fix_message(&mut stream); // consume Logon response

        // Send MarketDataRequest with no Symbol tags.
        let req = FixMessageBuilder::new(tags::MSG_MARKET_DATA_REQUEST)
            .str_tag(tags::MD_REQ_ID, "REQ1")
            .str_tag(tags::SUBSCRIPTION_REQUEST_TYPE, "1")
            .str_tag(tags::NO_RELATED_SYM, "0")
            .build("CLIENT", "MELIN-MD", 2);
        stream.write_all(&req).unwrap();
        stream.flush().unwrap();

        // Should get a MarketDataRequestReject (35=Y).
        let resp = read_fix_message(&mut stream);
        let msg = FixMessage::parse(&resp).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_MD_REQUEST_REJECT);
        assert_eq!(msg.get_str(tags::MD_REQ_ID), Some("REQ1"));
        assert!(
            msg.get_str(tags::TEXT).unwrap_or("").contains("no symbols"),
            "expected reject text to mention 'no symbols', got: {:?}",
            msg.get_str(tags::TEXT)
        );

        gw.shutdown();
    }

    #[test]
    fn security_list_request_response() {
        let mut config = GatewayConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            event_publisher: "127.0.0.1:1".parse().unwrap(),
            authorized_keys: std::path::PathBuf::new(),
            subscriber_key: std::path::PathBuf::new(),
            core: 0,
            sender_comp_id: "MELIN-MD".to_string(),
            symbols: std::collections::HashMap::new(),
        };
        config.symbols.insert(
            "BTCUSD".to_string(),
            crate::config::SymbolConfig {
                id: 1,
                tick_inverse: 100,
                lot_inverse: 1,
                base_ccy: "BTC".to_string(),
                quote_ccy: "USD".to_string(),
            },
        );
        let config: &'static GatewayConfig = Box::leak(Box::new(config));
        let gw = spawn_gateway(config);

        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", gw.port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        // Logon first.
        let logon = logon_bytes("CLIENT", "MELIN-MD", 1);
        stream.write_all(&logon).unwrap();
        stream.flush().unwrap();
        let _ = read_fix_message(&mut stream); // consume Logon response

        // Send SecurityListRequest.
        let req = FixMessageBuilder::new(tags::MSG_SECURITY_LIST_REQUEST)
            .str_tag(tags::SECURITY_REQ_ID, "SLR1")
            .str_tag(tags::SECURITY_LIST_REQUEST_TYPE, "0")
            .build("CLIENT", "MELIN-MD", 2);
        stream.write_all(&req).unwrap();
        stream.flush().unwrap();

        // Read SecurityList response.
        let resp = read_fix_message(&mut stream);
        let msg = FixMessage::parse(&resp).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_SECURITY_LIST);
        assert_eq!(msg.get_str(tags::SECURITY_REQ_ID), Some("SLR1"));
        assert_eq!(msg.get_str(tags::NO_RELATED_SYM), Some("1"));

        let sym_fields: Vec<_> = msg
            .fields_iter()
            .filter(|f| f.tag == tags::SYMBOL)
            .collect();
        assert_eq!(sym_fields.len(), 1);
        assert_eq!(std::str::from_utf8(sym_fields[0].value).unwrap(), "BTCUSD");

        gw.shutdown();
    }
}
