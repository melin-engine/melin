//! FIX session state machine driven by io_uring CQE events.
//!
//! Each `Session` owns all its state (no Arc, no Mutex). The event loop
//! calls `handle_fix_message` and `try_process_melin_frame` as data
//! arrives, and the session responds with a `SessionAction` indicating
//! what I/O the event loop should perform.

use std::collections::HashMap;
use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};

use ed25519_dalek::{Signer, SigningKey};
use tracing::{debug, error, info, warn};

use melin_engine::types::{AccountId, OrderId, Side};
use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};

use crate::config::{GatewayConfig, SymbolConfig};
use crate::event_loop::SessionAction;
use crate::fix::parse::FixMessage;
use crate::fix::serialize::FixMessageBuilder;
use crate::fix::tags;
use crate::id_map::ClOrdIdMap;
use crate::translate::{self, TranslateContext};

// ---------------------------------------------------------------------------
// Order → symbol mapping for exec report translation
// ---------------------------------------------------------------------------

/// Per-order metadata needed to translate Melin execution reports back
/// to FIX with correct symbol names, price/quantity scaling, and side.
struct OrderSymbolInfo {
    fix_symbol: String,
    tick_inverse: u64,
    lot_inverse: u64,
    side: Side,
}

/// Tracks a pending cancel or cancel-replace request so we can emit
/// the correct FIX message type on rejection (OrderCancelReject 35=9
/// instead of ExecutionReport 35=8).
struct PendingCancel {
    /// ClOrdID of the cancel/replace request itself (not the original order).
    cancel_clord_id: String,
    /// True for cancel-replace (35=G), false for cancel (35=F).
    is_replace: bool,
}

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

/// States a FIX session progresses through.
#[derive(Debug)]
pub enum SessionState {
    /// Waiting for the FIX Logon message from the client.
    AwaitingLogon,
    /// Melin TCP connect in progress (io_uring CONNECT submitted).
    ConnectingMelin,
    /// Waiting for the Melin Challenge frame after TCP connect.
    AwaitingChallenge,
    /// ChallengeResponse sent, waiting for ServerReady/AuthFailed.
    AwaitingAuthResult,
    /// Fully active — bidirectional FIX ↔ Melin forwarding.
    Active,
    /// Logout initiated, pending cleanup.
    Closing,
}

/// Per-FIX-session state. Owned entirely by the event loop thread.
pub struct Session {
    pub state: SessionState,

    // ── FIX client side ──
    pub fix_fd: RawFd,
    pub fix_parse_buf: Vec<u8>,
    pub fix_send_buf: Vec<u8>,
    /// Buffer currently being sent by io_uring — must not be mutated
    /// until the corresponding SEND CQE arrives.
    pub fix_inflight: Vec<u8>,
    /// Expected next inbound MsgSeqNum from the FIX client.
    fix_inbound_seq: u64,
    /// Next outbound MsgSeqNum to the FIX client.
    fix_outbound_seq: u64,
    pub sender_comp_id: String,
    pub heartbeat_interval: Duration,
    pub last_fix_recv: Instant,
    /// Last time we sent any FIX message (for outbound heartbeat timing).
    pub last_fix_sent: Instant,
    /// When we sent a TestRequest (tag 112) to probe a silent client.
    /// If the client doesn't respond within HeartBtInt, we disconnect.
    pub test_request_sent_at: Option<Instant>,
    pub fix_multishot_active: bool,

    // ── Melin server side ──
    pub melin_fd: Option<RawFd>,
    pub melin_parse_buf: Vec<u8>,
    pub melin_send_buf: Vec<u8>,
    /// Buffer currently being sent by io_uring — must not be mutated
    /// until the corresponding SEND CQE arrives.
    pub melin_inflight: Vec<u8>,
    /// Melin request sequence number (per-key monotonic).
    melin_seq: u64,
    /// Reusable encode buffer for Melin requests.
    melin_encode_buf: [u8; 136],
    pub melin_multishot_active: bool,

    // ── Session-owned data ──
    id_map: ClOrdIdMap,
    /// Maps Melin OrderId → symbol/scaling info for exec report translation.
    /// Populated when orders are submitted, consulted when exec reports arrive.
    order_symbols: HashMap<OrderId, OrderSymbolInfo>,
    /// Tracks pending cancel/replace requests by original order ID.
    /// Used to distinguish OrderCancelReject from ExecutionReport on rejection.
    pending_cancels: HashMap<OrderId, PendingCancel>,
    account_id: AccountId,
    signing_key: Option<SigningKey>,
    /// Index into config.sessions for this FIX session.
    session_config_idx: Option<usize>,
    /// Monotonic ExecID counter for FIX execution reports (tag 17).
    exec_id: u64,

    // ── Rate limiting ──
    /// Maximum inbound messages per second (0 = unlimited).
    max_msgs_per_sec: u32,
    /// Messages received in the current one-second window.
    rate_msg_count: u32,
    /// Start of the current rate-limit window.
    rate_window_start: Instant,

    // ── Auth state ──
    /// Nonce from the Melin Challenge, kept until auth completes.
    auth_nonce: Option<[u8; 32]>,

    // ── Connect state ──
    /// Stored sockaddr for the io_uring CONNECT SQE lifetime.
    pub connect_addr: Option<libc::sockaddr_in>,
}

impl Session {
    /// Create a new session for a just-accepted FIX client socket.
    pub fn new(fix_fd: RawFd, now: Instant) -> Self {
        Self {
            state: SessionState::AwaitingLogon,
            fix_fd,
            fix_parse_buf: Vec::with_capacity(512),
            fix_send_buf: Vec::with_capacity(512),
            fix_inflight: Vec::with_capacity(512),
            fix_inbound_seq: 1,
            fix_outbound_seq: 1,
            sender_comp_id: String::new(),
            heartbeat_interval: Duration::from_secs(30),
            last_fix_recv: now,
            last_fix_sent: now,
            test_request_sent_at: None,
            fix_multishot_active: false,

            melin_fd: None,
            melin_parse_buf: Vec::with_capacity(256),
            melin_send_buf: Vec::with_capacity(256),
            melin_inflight: Vec::with_capacity(256),
            melin_seq: 0,
            melin_encode_buf: [0u8; 136],
            melin_multishot_active: false,

            id_map: ClOrdIdMap::new(),
            order_symbols: HashMap::new(),
            pending_cancels: HashMap::new(),
            account_id: AccountId(0),
            signing_key: None,
            session_config_idx: None,
            exec_id: 1,

            max_msgs_per_sec: 0,
            rate_msg_count: 0,
            rate_window_start: now,

            auth_nonce: None,
            connect_addr: None,
        }
    }

    // -----------------------------------------------------------------------
    // FIX message dispatch
    // -----------------------------------------------------------------------

    /// Handle a complete FIX message received from the client.
    /// Returns an action for the event loop.
    pub fn handle_fix_message(
        &mut self,
        raw: &[u8],
        config: &GatewayConfig,
        session_map: &HashMap<String, usize>,
        symbol_map: &HashMap<String, SymbolConfig>,
    ) -> SessionAction {
        match self.state {
            SessionState::AwaitingLogon => self.handle_logon(raw, config, session_map),
            SessionState::Active => self.handle_active_fix(raw, config, symbol_map),
            _ => {
                // Received FIX data in a non-ready state — ignore.
                debug!(state = ?self.state, "FIX message received in non-ready state");
                SessionAction::None
            }
        }
    }

    // -----------------------------------------------------------------------
    // Logon
    // -----------------------------------------------------------------------

    fn handle_logon(
        &mut self,
        raw: &[u8],
        config: &GatewayConfig,
        session_map: &HashMap<String, usize>,
    ) -> SessionAction {
        let msg = match FixMessage::parse(raw) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "malformed FIX Logon");
                return SessionAction::Close;
            }
        };

        if msg.msg_type() != tags::MSG_LOGON {
            self.queue_fix_logout(config, "first message must be Logon");
            return SessionAction::Close;
        }

        let sender_comp_id = match msg.sender_comp_id() {
            Some(s) => s,
            None => {
                self.queue_fix_logout(config, "Logon missing SenderCompID");
                return SessionAction::Close;
            }
        };

        // Look up session config.
        let cfg_idx = match session_map.get(sender_comp_id) {
            Some(&idx) => idx,
            None => {
                warn!(sender = sender_comp_id, "unknown SenderCompID");
                self.queue_fix_logout(config, "unknown SenderCompID");
                return SessionAction::Close;
            }
        };

        let session_config = &config.sessions[cfg_idx];

        info!(
            sender = sender_comp_id,
            account = session_config.account_id,
            "FIX Logon received"
        );

        // Validate MsgSeqNum — Logon must be sequence 1.
        if let Some(seq) = msg.msg_seq_num()
            && seq != 1
        {
            warn!(sender = sender_comp_id, seq, "Logon MsgSeqNum must be 1");
            self.queue_fix_logout(config, "MsgSeqNum must be 1 on Logon");
            return SessionAction::Close;
        }

        // Extract HeartBtInt (default 30s).
        let heartbeat_secs: u64 = msg
            .get_str(tags::HEART_BT_INT)
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);

        // Load the signing key for Melin authentication.
        let signing_key = match load_signing_key(&session_config.key_path) {
            Ok(k) => k,
            Err(e) => {
                error!(error = %e, "failed to load signing key");
                self.queue_fix_logout(config, "internal error");
                return SessionAction::Close;
            }
        };

        // Store session info.
        self.sender_comp_id = sender_comp_id.to_owned();
        self.account_id = AccountId(session_config.account_id);
        self.heartbeat_interval = Duration::from_secs(heartbeat_secs);
        self.signing_key = Some(signing_key);
        self.session_config_idx = Some(cfg_idx);
        self.max_msgs_per_sec = session_config.max_msgs_per_sec;
        self.fix_inbound_seq = 2; // Logon was seq 1.

        // Transition: start Melin TCP connect.
        self.state = SessionState::ConnectingMelin;
        SessionAction::ConnectMelin
    }

    // -----------------------------------------------------------------------
    // Melin auth state machine (driven by Melin RECV)
    // -----------------------------------------------------------------------

    /// Called by the event loop when the Melin TCP connect completes.
    pub fn on_melin_connected(&mut self, _now: Instant) {
        self.state = SessionState::AwaitingChallenge;
    }

    /// Try to process one complete Melin frame from `melin_parse_buf`.
    /// Returns an action for the event loop.
    pub fn try_process_melin_frame(
        &mut self,
        config: &GatewayConfig,
        symbol_map: &HashMap<String, SymbolConfig>,
        _now: Instant,
    ) -> SessionAction {
        // Melin uses length-prefixed framing: [u32 LE length][payload].
        let buf = &self.melin_parse_buf;
        if buf.len() < 4 {
            return SessionAction::None;
        }
        let frame_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.len() < 4 + frame_len {
            return SessionAction::None; // Incomplete frame.
        }

        // Extract the frame payload.
        let payload = self.melin_parse_buf[4..4 + frame_len].to_vec();
        self.melin_parse_buf.drain(..4 + frame_len);

        match self.state {
            SessionState::AwaitingChallenge => self.handle_challenge(&payload, config),
            SessionState::AwaitingAuthResult => self.handle_auth_result(&payload, config),
            SessionState::Active => self.handle_active_melin(&payload, config, symbol_map),
            _ => {
                debug!(state = ?self.state, "Melin frame in unexpected state");
                SessionAction::None
            }
        }
    }

    fn handle_challenge(&mut self, payload: &[u8], config: &GatewayConfig) -> SessionAction {
        let response = match codec::decode_response(payload) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "failed to decode Melin Challenge");
                self.queue_fix_logout(config, "internal error");
                return SessionAction::Close;
            }
        };

        let nonce = match response {
            ResponseKind::Challenge { nonce } => nonce,
            other => {
                error!(response = ?other, "expected Challenge from Melin server");
                self.queue_fix_logout(config, "internal error");
                return SessionAction::Close;
            }
        };

        // Sign the nonce with the session's Ed25519 key.
        let signing_key = match &self.signing_key {
            Some(k) => k,
            None => {
                error!("no signing key loaded");
                return SessionAction::Close;
            }
        };

        let signature = signing_key.sign(&nonce);
        let request = Request::ChallengeResponse {
            signature: signature.to_bytes(),
            public_key: signing_key.verifying_key().to_bytes(),
        };

        // Encode ChallengeResponse into Melin send buffer.
        let written = match codec::encode_request(&request, 0, &mut self.melin_encode_buf) {
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "failed to encode ChallengeResponse");
                return SessionAction::Close;
            }
        };
        self.melin_send_buf
            .extend_from_slice(&self.melin_encode_buf[..written]);

        self.auth_nonce = Some(nonce);
        self.state = SessionState::AwaitingAuthResult;
        SessionAction::SendMelin
    }

    fn handle_auth_result(&mut self, payload: &[u8], config: &GatewayConfig) -> SessionAction {
        let response = match codec::decode_response(payload) {
            Ok(r) => r,
            Err(e) => {
                error!(error = %e, "failed to decode Melin auth result");
                self.queue_fix_logout(config, "internal error");
                return SessionAction::Close;
            }
        };

        match response {
            ResponseKind::ServerReady => {
                info!(
                    sender = %self.sender_comp_id,
                    "Melin authentication succeeded"
                );

                // Send FIX Logon response to the client.
                let logon_response = FixMessageBuilder::new(tags::MSG_LOGON)
                    .str_tag(tags::ENCRYPT_METHOD, "0")
                    .u64_tag(tags::HEART_BT_INT, self.heartbeat_interval.as_secs())
                    .build(
                        &config.target_comp_id,
                        &self.sender_comp_id,
                        self.fix_outbound_seq,
                    );
                self.queue_fix_raw(&logon_response);

                // Clean up auth state.
                self.auth_nonce = None;
                self.signing_key = None;

                self.state = SessionState::Active;
                SessionAction::SendFix
            }
            ResponseKind::AuthFailed => {
                warn!(sender = %self.sender_comp_id, "Melin authentication failed");
                self.queue_fix_logout(config, "authentication failed");
                SessionAction::Close
            }
            other => {
                error!(response = ?other, "unexpected Melin auth response");
                self.queue_fix_logout(config, "internal error");
                SessionAction::Close
            }
        }
    }

    // -----------------------------------------------------------------------
    // Active state — FIX message handling
    // -----------------------------------------------------------------------

    fn handle_active_fix(
        &mut self,
        raw: &[u8],
        config: &GatewayConfig,
        symbol_map: &HashMap<String, SymbolConfig>,
    ) -> SessionAction {
        let msg = match FixMessage::parse(raw) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "malformed FIX message");
                self.queue_fix_reject(config, &e.to_string());
                return SessionAction::SendFix;
            }
        };

        // Any valid message from the client proves it's alive —
        // cancel any pending TestRequest probe.
        self.test_request_sent_at = None;

        // Validate MsgSeqNum.
        if let Some(seq) = msg.msg_seq_num() {
            if seq < self.fix_inbound_seq {
                // Duplicate — ignore.
                return SessionAction::None;
            }
            if seq > self.fix_inbound_seq {
                // Gap — disconnect (v1: no gap fill).
                warn!(expected = self.fix_inbound_seq, got = seq, "MsgSeqNum gap");
                self.queue_fix_logout(config, "MsgSeqNum too high, expected sequence reset");
                return SessionAction::Close;
            }
            self.fix_inbound_seq += 1;
        }

        let msg_type = msg.msg_type();
        match msg_type {
            tags::MSG_HEARTBEAT => SessionAction::None,
            tags::MSG_TEST_REQUEST => {
                let test_req_id = msg.get_str(tags::TEST_REQ_ID).unwrap_or("");
                let hb = FixMessageBuilder::new(tags::MSG_HEARTBEAT)
                    .str_tag(tags::TEST_REQ_ID, test_req_id)
                    .build(
                        &config.target_comp_id,
                        &self.sender_comp_id,
                        self.fix_outbound_seq,
                    );
                self.queue_fix_raw(&hb);
                SessionAction::SendFix
            }
            tags::MSG_LOGOUT => {
                info!(sender = %self.sender_comp_id, "FIX Logout received");
                self.queue_fix_logout(config, "Logout acknowledged");
                SessionAction::Close
            }
            tags::MSG_NEW_ORDER_SINGLE
            | tags::MSG_ORDER_CANCEL_REQUEST
            | tags::MSG_ORDER_CANCEL_REPLACE => {
                if self.check_rate_limit() {
                    self.translate_and_send_order(msg_type, &msg, config, symbol_map)
                } else {
                    warn!(sender = %self.sender_comp_id, "message rate limit exceeded");
                    self.queue_fix_reject(config, "message rate limit exceeded");
                    SessionAction::SendFix
                }
            }
            _ => {
                warn!(
                    msg_type = ?std::str::from_utf8(msg_type),
                    "unsupported FIX message type"
                );
                self.queue_fix_reject(config, "unsupported message type");
                SessionAction::SendFix
            }
        }
    }

    fn translate_and_send_order(
        &mut self,
        msg_type: &[u8],
        msg: &FixMessage<'_>,
        config: &GatewayConfig,
        symbol_map: &HashMap<String, SymbolConfig>,
    ) -> SessionAction {
        let mut ctx = TranslateContext {
            account_id: self.account_id,
            symbols: symbol_map,
            id_map: &mut self.id_map,
        };

        let request = match msg_type {
            b if b == tags::MSG_NEW_ORDER_SINGLE => translate::new_order_single(msg, &mut ctx),
            b if b == tags::MSG_ORDER_CANCEL_REQUEST => translate::cancel_order(msg, &mut ctx),
            b if b == tags::MSG_ORDER_CANCEL_REPLACE => translate::cancel_replace(msg, &mut ctx),
            _ => unreachable!(),
        };

        match request {
            Ok(req) => {
                // Record order → symbol/side mapping for exec report translation.
                if let Some(fix_sym) = msg.get_str(tags::SYMBOL)
                    && let Some(sym_cfg) = symbol_map.get(fix_sym)
                {
                    match &req {
                        Request::SubmitOrder { order, .. } => {
                            self.order_symbols
                                .entry(order.id)
                                .or_insert_with(|| OrderSymbolInfo {
                                    fix_symbol: fix_sym.to_owned(),
                                    tick_inverse: sym_cfg.tick_size_inverse,
                                    lot_inverse: sym_cfg.lot_size_inverse,
                                    side: order.side,
                                });
                        }
                        Request::CancelOrder { order_id, .. } => {
                            // Track the cancel request's ClOrdID so we can
                            // emit OrderCancelReject if the engine rejects it.
                            if let Some(clord) = msg.get_str(tags::CL_ORD_ID) {
                                self.pending_cancels.insert(
                                    *order_id,
                                    PendingCancel {
                                        cancel_clord_id: clord.to_owned(),
                                        is_replace: false,
                                    },
                                );
                            }
                        }
                        Request::CancelReplace { order_id, .. } => {
                            if let Some(clord) = msg.get_str(tags::CL_ORD_ID) {
                                self.pending_cancels.insert(
                                    *order_id,
                                    PendingCancel {
                                        cancel_clord_id: clord.to_owned(),
                                        is_replace: true,
                                    },
                                );
                            }
                        }
                        _ => {}
                    }
                }

                self.melin_seq += 1;
                match codec::encode_request(&req, self.melin_seq, &mut self.melin_encode_buf) {
                    Ok(written) => {
                        self.melin_send_buf
                            .extend_from_slice(&self.melin_encode_buf[..written]);
                        SessionAction::SendMelin
                    }
                    Err(e) => {
                        error!(error = %e, "failed to encode Melin request");
                        self.queue_fix_reject(config, "internal error");
                        SessionAction::SendFix
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "FIX translation error");
                self.queue_fix_reject(config, &e.to_string());
                SessionAction::SendFix
            }
        }
    }

    // -----------------------------------------------------------------------
    // Active state — Melin response handling
    // -----------------------------------------------------------------------

    fn handle_active_melin(
        &mut self,
        payload: &[u8],
        config: &GatewayConfig,
        _symbol_map: &HashMap<String, SymbolConfig>,
    ) -> SessionAction {
        let response = match codec::decode_response(payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to decode Melin response");
                return SessionAction::None;
            }
        };

        match response {
            ResponseKind::Report(ref report) => {
                match report {
                    ExecutionReport::Fill {
                        maker_order_id,
                        taker_order_id,
                        price: fill_price,
                        quantity,
                        maker_fee,
                        taker_fee,
                        ..
                    } => {
                        // Emit separate fill reports for each side that
                        // belongs to this session (identified by id_map).
                        let mut sent = false;

                        // Maker side.
                        if self.id_map.get_clord_id(*maker_order_id).is_some() {
                            let info = self.order_symbols.get(maker_order_id);
                            let (sym, ti, li, side) = sym_info_or_default(info);
                            let ctx = translate::FixCtx {
                                id_map: &self.id_map,
                                symbol_str: sym,
                                tick_inverse: ti,
                                lot_inverse: li,
                                sender: &config.target_comp_id,
                                target: &self.sender_comp_id,
                            };
                            let msg = translate::fill_report_for_order(
                                *maker_order_id,
                                side,
                                *fill_price,
                                *quantity,
                                *maker_fee,
                                &ctx,
                                self.fix_outbound_seq,
                                self.exec_id,
                            );
                            self.queue_fix_raw(&msg);
                            self.exec_id += 1;
                            sent = true;
                        }

                        // Taker side.
                        if self.id_map.get_clord_id(*taker_order_id).is_some() {
                            let info = self.order_symbols.get(taker_order_id);
                            let (sym, ti, li, side) = sym_info_or_default(info);
                            let ctx = translate::FixCtx {
                                id_map: &self.id_map,
                                symbol_str: sym,
                                tick_inverse: ti,
                                lot_inverse: li,
                                sender: &config.target_comp_id,
                                target: &self.sender_comp_id,
                            };
                            let msg = translate::fill_report_for_order(
                                *taker_order_id,
                                side,
                                *fill_price,
                                *quantity,
                                *taker_fee,
                                &ctx,
                                self.fix_outbound_seq,
                                self.exec_id,
                            );
                            self.queue_fix_raw(&msg);
                            self.exec_id += 1;
                            sent = true;
                        }

                        if sent {
                            SessionAction::SendFix
                        } else {
                            SessionAction::None
                        }
                    }

                    ExecutionReport::Rejected {
                        order_id, reason, ..
                    } => {
                        // Check if this was a rejected cancel/replace → OrderCancelReject.
                        if let Some(pending) = self.pending_cancels.remove(order_id) {
                            let orig_clord =
                                self.id_map.get_clord_id(*order_id).unwrap_or("UNKNOWN");
                            let msg = translate::cancel_reject_to_fix(
                                *order_id,
                                &pending.cancel_clord_id,
                                orig_clord,
                                reason,
                                pending.is_replace,
                                &config.target_comp_id,
                                &self.sender_comp_id,
                                self.fix_outbound_seq,
                            );
                            self.queue_fix_raw(&msg);
                            SessionAction::SendFix
                        } else {
                            // Regular order rejection.
                            let info = self.order_symbols.get(order_id);
                            let (sym, ti, li, side) = sym_info_or_default(info);
                            let ctx = translate::FixCtx {
                                id_map: &self.id_map,
                                symbol_str: sym,
                                tick_inverse: ti,
                                lot_inverse: li,
                                sender: &config.target_comp_id,
                                target: &self.sender_comp_id,
                            };
                            let msg = translate::execution_report_to_fix(
                                report,
                                &ctx,
                                Some(side),
                                self.fix_outbound_seq,
                                self.exec_id,
                            );
                            self.queue_fix_raw(&msg);
                            self.exec_id += 1;
                            SessionAction::SendFix
                        }
                    }

                    _ => {
                        // Placed, Cancelled, Replaced, Triggered, InstrumentStatusChanged.
                        let order_id = report_order_id(report);

                        // Clean up pending cancel tracking on success.
                        if let Some(oid) = order_id {
                            self.pending_cancels.remove(&oid);
                        }

                        let info = order_id.and_then(|id| self.order_symbols.get(&id));
                        let (sym, ti, li, side) = sym_info_or_default(info);
                        let ctx = translate::FixCtx {
                            id_map: &self.id_map,
                            symbol_str: sym,
                            tick_inverse: ti,
                            lot_inverse: li,
                            sender: &config.target_comp_id,
                            target: &self.sender_comp_id,
                        };

                        let fix_msg = translate::execution_report_to_fix(
                            report,
                            &ctx,
                            Some(side),
                            self.fix_outbound_seq,
                            self.exec_id,
                        );

                        if !fix_msg.is_empty() {
                            self.queue_fix_raw(&fix_msg);
                            self.exec_id += 1;
                            SessionAction::SendFix
                        } else {
                            SessionAction::None
                        }
                    }
                }
            }
            ResponseKind::BatchEnd | ResponseKind::Heartbeat | ResponseKind::ServerReady => {
                SessionAction::None
            }
            ResponseKind::ServerBusy => {
                warn!(sender = %self.sender_comp_id, "Melin server busy");
                SessionAction::None
            }
            ResponseKind::EngineError => {
                error!(sender = %self.sender_comp_id, "Melin engine error");
                SessionAction::None
            }
            _ => SessionAction::None,
        }
    }

    // -----------------------------------------------------------------------
    // FIX message builders
    // -----------------------------------------------------------------------

    fn queue_fix_logout(&mut self, config: &GatewayConfig, text: &str) {
        let target = if self.sender_comp_id.is_empty() {
            "UNKNOWN"
        } else {
            &self.sender_comp_id
        };
        let msg = FixMessageBuilder::new(tags::MSG_LOGOUT)
            .str_tag(tags::TEXT, text)
            .build(&config.target_comp_id, target, self.fix_outbound_seq);
        self.queue_fix_raw(&msg);
        self.state = SessionState::Closing;
    }

    fn queue_fix_reject(&mut self, config: &GatewayConfig, text: &str) {
        let msg = FixMessageBuilder::new(tags::MSG_REJECT)
            .str_tag(tags::TEXT, text)
            .build(
                &config.target_comp_id,
                &self.sender_comp_id,
                self.fix_outbound_seq,
            );
        self.queue_fix_raw(&msg);
    }

    /// Append a serialized FIX message to the send buffer and bump
    /// the outbound sequence counter + last-sent timestamp.
    fn queue_fix_raw(&mut self, msg: &[u8]) {
        self.fix_send_buf.extend_from_slice(msg);
        self.fix_outbound_seq += 1;
        self.last_fix_sent = Instant::now();
    }

    // -----------------------------------------------------------------------
    // Rate limiting
    // -----------------------------------------------------------------------

    /// Returns true if the message is allowed, false if rate-limited.
    /// Uses a simple per-second sliding window: counts messages in the
    /// current one-second window and rejects when the limit is exceeded.
    fn check_rate_limit(&mut self) -> bool {
        if self.max_msgs_per_sec == 0 {
            return true; // Unlimited.
        }
        let now = Instant::now();
        if now.duration_since(self.rate_window_start) >= Duration::from_secs(1) {
            // New window.
            self.rate_window_start = now;
            self.rate_msg_count = 1;
            true
        } else if self.rate_msg_count < self.max_msgs_per_sec {
            self.rate_msg_count += 1;
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // Heartbeat management
    // -----------------------------------------------------------------------

    /// Check heartbeat timers and return an action if the event loop
    /// needs to send or close.
    ///
    /// FIX heartbeat protocol:
    /// 1. If we haven't sent anything in HeartBtInt, send a Heartbeat.
    /// 2. If we haven't received anything in HeartBtInt, send a
    ///    TestRequest to probe the client.
    /// 3. If the TestRequest goes unanswered for HeartBtInt, disconnect.
    pub fn check_heartbeat(&mut self, now: Instant, config: &GatewayConfig) -> SessionAction {
        if !matches!(self.state, SessionState::Active) {
            return SessionAction::None;
        }

        let hb = self.heartbeat_interval;
        let since_recv = now.duration_since(self.last_fix_recv);
        let since_sent = now.duration_since(self.last_fix_sent);

        // Step 3: TestRequest was sent and timed out → disconnect.
        if let Some(sent_at) = self.test_request_sent_at
            && now.duration_since(sent_at) > hb
        {
            warn!(sender = %self.sender_comp_id, "FIX heartbeat timeout (TestRequest unanswered)");
            self.queue_fix_logout(config, "heartbeat timeout");
            return SessionAction::Close;
        }

        // Step 2: Haven't heard from client in HeartBtInt → send TestRequest.
        if since_recv > hb && self.test_request_sent_at.is_none() {
            let test_req_id = format!("TR{}", self.fix_outbound_seq);
            let msg = FixMessageBuilder::new(tags::MSG_TEST_REQUEST)
                .str_tag(tags::TEST_REQ_ID, &test_req_id)
                .build(
                    &config.target_comp_id,
                    &self.sender_comp_id,
                    self.fix_outbound_seq,
                );
            self.queue_fix_raw(&msg);
            self.test_request_sent_at = Some(now);
            return SessionAction::SendFix;
        }

        // Step 1: Haven't sent anything in HeartBtInt → send Heartbeat.
        if since_sent > hb {
            let msg = FixMessageBuilder::new(tags::MSG_HEARTBEAT).build(
                &config.target_comp_id,
                &self.sender_comp_id,
                self.fix_outbound_seq,
            );
            self.queue_fix_raw(&msg);
            return SessionAction::SendFix;
        }

        SessionAction::None
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // `shutdown(SHUT_RDWR)` initiates TCP FIN immediately. `close`
        // alone is not enough here because io_uring may still hold an
        // internal reference to the socket via an armed multishot RECV
        // — in that case `close` decrements the user refcount but the
        // kernel keeps the socket alive (no FIN to the peer) until the
        // multishot completes. `shutdown` sidesteps that by forcing
        // the half-close at the protocol level so the client observes
        // EOF promptly.
        unsafe {
            libc::shutdown(self.fix_fd, libc::SHUT_RDWR);
            libc::close(self.fix_fd);
        }
        if let Some(fd) = self.melin_fd {
            unsafe {
                libc::shutdown(fd, libc::SHUT_RDWR);
                libc::close(fd);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

use melin_engine::types::ExecutionReport;

/// Extract symbol info from an OrderSymbolInfo, or return defaults.
fn sym_info_or_default(info: Option<&OrderSymbolInfo>) -> (&str, u64, u64, Side) {
    match info {
        Some(i) => (i.fix_symbol.as_str(), i.tick_inverse, i.lot_inverse, i.side),
        None => ("UNKNOWN", 1, 1, Side::Buy),
    }
}

/// Extract the primary order ID from an execution report for
/// order→symbol lookups.
fn report_order_id(report: &ExecutionReport) -> Option<OrderId> {
    match report {
        ExecutionReport::Placed { order_id, .. }
        | ExecutionReport::Cancelled { order_id, .. }
        | ExecutionReport::Rejected { order_id, .. }
        | ExecutionReport::Replaced { order_id, .. }
        | ExecutionReport::Triggered { order_id, .. } => Some(*order_id),
        ExecutionReport::Fill { taker_order_id, .. } => Some(*taker_order_id),
        ExecutionReport::InstrumentStatusChanged { .. } => None,
    }
}

/// Load a 32-byte Ed25519 private key seed from a file.
fn load_signing_key(path: &std::path::Path) -> Result<SigningKey, Box<dyn std::error::Error>> {
    let seed = std::fs::read(path)?;
    if seed.len() != 32 {
        return Err(format!(
            "key file must be 32 bytes, got {} ({})",
            seed.len(),
            path.display()
        )
        .into());
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&seed);
    Ok(SigningKey::from_bytes(&bytes))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fix::serialize::FixMessageBuilder;
    use melin_engine::types::{
        ExecutionReport, InstrumentStatus, Price, Quantity, RejectReason, Side, Symbol,
    };
    use melin_protocol::message::ResponseKind;
    use std::num::NonZeroU64;
    use std::sync::atomic::{AtomicU64, Ordering};

    // -----------------------------------------------------------------------
    // Test scaffolding
    // -----------------------------------------------------------------------

    /// Open `/dev/null` to get a real, closeable file descriptor that
    /// `Session::drop` can `close()` without affecting the test process.
    fn fake_fd() -> RawFd {
        use std::ffi::CString;
        let path = CString::new("/dev/null").unwrap();
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDWR) };
        assert!(fd >= 0, "failed to open /dev/null");
        fd
    }

    /// Write a fresh 32-byte signing key seed to a unique temp file and
    /// return the path. The file leaks at process exit; that's fine for
    /// a test process.
    fn make_key_file() -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let path = std::env::temp_dir().join(format!("melin-fix-test-key-{pid}-{n}.bin"));
        // Deterministic 32-byte seed; the value doesn't matter for these tests.
        let seed = [0xABu8; 32];
        std::fs::write(&path, seed).unwrap();
        path
    }

    fn make_config(sender: &str, target: &str) -> GatewayConfig {
        let key_path = make_key_file();
        let toml = format!(
            r#"
server_addr = "127.0.0.1:9876"
listen_addr = "127.0.0.1:9100"
target_comp_id = "{target}"

[[session]]
sender_comp_id = "{sender}"
account_id = 7
key_path = "{}"
max_msgs_per_sec = 0

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 1
tick_size_inverse = 100
lot_size_inverse = 1
"#,
            key_path.display()
        );
        toml::from_str(&toml).unwrap()
    }

    fn session_map(config: &GatewayConfig) -> HashMap<String, usize> {
        config
            .sessions
            .iter()
            .enumerate()
            .map(|(i, s)| (s.sender_comp_id.clone(), i))
            .collect()
    }

    fn symbol_map(config: &GatewayConfig) -> HashMap<String, SymbolConfig> {
        config
            .symbols
            .iter()
            .cloned()
            .map(|s| (s.fix_symbol.clone(), s))
            .collect()
    }

    /// Build a fresh session in `AwaitingLogon`. `now` is fixed so
    /// heartbeat-timer tests can advance it deterministically.
    fn new_session(now: Instant) -> Session {
        Session::new(fake_fd(), now)
    }

    /// Construct a session already in `Active` state, bypassing the
    /// real auth flow. Mirrors what `handle_logon` + `handle_auth_result`
    /// would have done on success.
    fn active_session(config: &GatewayConfig, now: Instant) -> Session {
        let mut s = new_session(now);
        s.state = SessionState::Active;
        s.sender_comp_id = config.sessions[0].sender_comp_id.clone();
        s.account_id = AccountId(config.sessions[0].account_id);
        s.session_config_idx = Some(0);
        s.fix_inbound_seq = 2; // Logon was seq 1.
        s.fix_outbound_seq = 2; // Logon ack was seq 1.
        s.heartbeat_interval = Duration::from_secs(30);
        s.max_msgs_per_sec = config.sessions[0].max_msgs_per_sec;
        s
    }

    /// Build a Logon message from a FIX client (sender → target).
    fn logon_msg(sender: &str, target: &str, seq: u64, hb_secs: u32) -> Vec<u8> {
        FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .str_tag(tags::HEART_BT_INT, &hb_secs.to_string())
            .build(sender, target, seq)
    }

    /// Build a NewOrderSingle limit-buy.
    fn new_order_msg(sender: &str, target: &str, seq: u64, clord: &str) -> Vec<u8> {
        FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, clord)
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build(sender, target, seq)
    }

    /// Build an OrderCancelRequest referencing `clord` (the cancel
    /// request's own ClOrdID) and `orig` (the original order's ClOrdID).
    fn cancel_msg(sender: &str, target: &str, seq: u64, clord: &str, orig: &str) -> Vec<u8> {
        FixMessageBuilder::new(tags::MSG_ORDER_CANCEL_REQUEST)
            .str_tag(tags::CL_ORD_ID, clord)
            .str_tag(tags::ORIG_CL_ORD_ID, orig)
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORDER_QTY, "10")
            .build(sender, target, seq)
    }

    /// Push a Melin response onto the session's parse buffer in the
    /// length-prefixed wire format.
    fn push_melin_response(session: &mut Session, response: &ResponseKind) {
        let mut buf = [0u8; 256];
        let n = melin_protocol::codec::encode_response(response, &mut buf).unwrap();
        session.melin_parse_buf.extend_from_slice(&buf[..n]);
    }

    fn px(v: u64) -> Price {
        Price(NonZeroU64::new(v).unwrap())
    }
    fn qty(v: u64) -> Quantity {
        Quantity(NonZeroU64::new(v).unwrap())
    }

    // -----------------------------------------------------------------------
    // Logon tests
    // -----------------------------------------------------------------------

    #[test]
    fn logon_happy_path() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = new_session(Instant::now());

        let raw = logon_msg("FIRM_A", "MELIN", 1, 30);
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);

        assert_eq!(action, SessionAction::ConnectMelin);
        assert!(matches!(s.state, SessionState::ConnectingMelin));
        assert_eq!(s.sender_comp_id, "FIRM_A");
        assert_eq!(s.account_id, AccountId(7));
        assert_eq!(s.heartbeat_interval, Duration::from_secs(30));
        assert!(s.signing_key.is_some());
        assert_eq!(s.fix_inbound_seq, 2);
    }

    #[test]
    fn logon_unknown_sender_closes() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = new_session(Instant::now());

        let raw = logon_msg("UNKNOWN_FIRM", "MELIN", 1, 30);
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);

        assert_eq!(action, SessionAction::Close);
        assert!(matches!(s.state, SessionState::Closing));
        // Logout was queued.
        assert!(!s.fix_send_buf.is_empty());
    }

    #[test]
    fn logon_bad_seq_closes() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = new_session(Instant::now());

        let raw = logon_msg("FIRM_A", "MELIN", 5, 30); // Seq must be 1.
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);

        assert_eq!(action, SessionAction::Close);
        assert!(matches!(s.state, SessionState::Closing));
    }

    #[test]
    fn logon_first_message_must_be_logon() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = new_session(Instant::now());

        let raw = new_order_msg("FIRM_A", "MELIN", 1, "ORD1");
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);

        assert_eq!(action, SessionAction::Close);
        assert!(matches!(s.state, SessionState::Closing));
    }

    #[test]
    fn logon_malformed_message_closes() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = new_session(Instant::now());

        let action = s.handle_fix_message(b"garbage", &config, &smap, &sym);
        assert_eq!(action, SessionAction::Close);
    }

    // -----------------------------------------------------------------------
    // Active state — inbound FIX
    // -----------------------------------------------------------------------

    #[test]
    fn active_heartbeat_message_is_noop() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let smap = session_map(&config);
        let mut s = active_session(&config, Instant::now());

        let raw = FixMessageBuilder::new(tags::MSG_HEARTBEAT).build("FIRM_A", "MELIN", 2);
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);
        assert_eq!(action, SessionAction::None);
    }

    #[test]
    fn active_test_request_replies_heartbeat() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());
        let outbound_before = s.fix_outbound_seq;

        let raw = FixMessageBuilder::new(tags::MSG_TEST_REQUEST)
            .str_tag(tags::TEST_REQ_ID, "ABC")
            .build("FIRM_A", "MELIN", 2);
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);

        assert_eq!(action, SessionAction::SendFix);
        assert_eq!(s.fix_outbound_seq, outbound_before + 1);
        // The reply contains the TestReqID echoed back.
        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_HEARTBEAT);
        assert_eq!(parsed.get_str(tags::TEST_REQ_ID), Some("ABC"));
    }

    #[test]
    fn active_logout_closes() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let raw = FixMessageBuilder::new(tags::MSG_LOGOUT).build("FIRM_A", "MELIN", 2);
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);

        assert_eq!(action, SessionAction::Close);
        assert!(matches!(s.state, SessionState::Closing));
    }

    #[test]
    fn active_seq_gap_disconnects() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let raw = new_order_msg("FIRM_A", "MELIN", 99, "ORD1"); // Expected 2, got 99.
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);
        assert_eq!(action, SessionAction::Close);
    }

    #[test]
    fn active_duplicate_seq_silently_dropped() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());
        s.fix_inbound_seq = 5;

        let raw = new_order_msg("FIRM_A", "MELIN", 3, "ORD1"); // Old seq.
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);
        assert_eq!(action, SessionAction::None);
        // Inbound seq unchanged, no Melin request sent.
        assert_eq!(s.fix_inbound_seq, 5);
        assert!(s.melin_send_buf.is_empty());
    }

    #[test]
    fn active_unsupported_msg_type_rejects() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        // 35=B (News) — not supported.
        let raw = FixMessageBuilder::new(b"B").build("FIRM_A", "MELIN", 2);
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);

        assert_eq!(action, SessionAction::SendFix);
        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_REJECT);
    }

    #[test]
    fn active_new_order_encodes_melin_request_and_tracks_symbol() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let raw = new_order_msg("FIRM_A", "MELIN", 2, "ORD1");
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);

        assert_eq!(action, SessionAction::SendMelin);
        assert!(!s.melin_send_buf.is_empty());
        assert_eq!(s.melin_seq, 1);
        // Order was registered in id_map.
        let order_id = s
            .id_map
            .get_order_id("ORD1")
            .expect("ORD1 should be mapped");
        // And in order_symbols with side Buy.
        let info = s.order_symbols.get(&order_id).expect("symbol info missing");
        assert_eq!(info.fix_symbol, "BTC/USD");
        assert_eq!(info.tick_inverse, 100);
        assert_eq!(info.side, Side::Buy);
        // Inbound seq advanced.
        assert_eq!(s.fix_inbound_seq, 3);
    }

    #[test]
    fn active_cancel_tracks_pending_cancel_for_reject_routing() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        // Submit an order so the ClOrdID maps to an OrderId.
        let order = new_order_msg("FIRM_A", "MELIN", 2, "ORD1");
        s.handle_fix_message(&order, &config, &smap, &sym);
        let order_id = s.id_map.get_order_id("ORD1").unwrap();

        // Now send a cancel for it.
        let cancel = cancel_msg("FIRM_A", "MELIN", 3, "CXL1", "ORD1");
        let action = s.handle_fix_message(&cancel, &config, &smap, &sym);
        assert_eq!(action, SessionAction::SendMelin);

        let pending = s
            .pending_cancels
            .get(&order_id)
            .expect("pending cancel not tracked");
        assert_eq!(pending.cancel_clord_id, "CXL1");
        assert!(!pending.is_replace);
    }

    #[test]
    fn active_rate_limit_blocks_excess_messages() {
        let mut config = make_config("FIRM_A", "MELIN");
        config.sessions[0].max_msgs_per_sec = 2;
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());
        s.max_msgs_per_sec = 2;

        // 1st and 2nd allowed.
        for (i, clord) in ["ORD1", "ORD2"].iter().enumerate() {
            let raw = new_order_msg("FIRM_A", "MELIN", 2 + i as u64, clord);
            let action = s.handle_fix_message(&raw, &config, &smap, &sym);
            assert_eq!(action, SessionAction::SendMelin, "msg {i}");
        }

        // 3rd is rejected.
        let raw = new_order_msg("FIRM_A", "MELIN", 4, "ORD3");
        let action = s.handle_fix_message(&raw, &config, &smap, &sym);
        assert_eq!(action, SessionAction::SendFix);
        // Should be a Reject (35=3) at the *end* of the send buffer.
        // (Just check the last message type by parsing the tail.)
        let last_msg_start = s
            .fix_send_buf
            .windows(11)
            .rposition(|w| w.starts_with(b"8=FIX.4.2\x01"))
            .unwrap();
        let parsed = FixMessage::parse(&s.fix_send_buf[last_msg_start..]).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_REJECT);
    }

    // -----------------------------------------------------------------------
    // Heartbeat timer
    // -----------------------------------------------------------------------

    #[test]
    fn heartbeat_idle_outbound_sends_heartbeat() {
        let config = make_config("FIRM_A", "MELIN");
        let t0 = Instant::now();
        let mut s = active_session(&config, t0);

        // 31s later: client is alive (last_recv just bumped) but
        // we haven't sent anything. Should emit a Heartbeat.
        let later = t0 + Duration::from_secs(31);
        s.last_fix_recv = later; // Suppress TestRequest path.
        let action = s.check_heartbeat(later, &config);

        assert_eq!(action, SessionAction::SendFix);
        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_HEARTBEAT);
    }

    #[test]
    fn heartbeat_silent_client_sends_test_request() {
        let config = make_config("FIRM_A", "MELIN");
        let t0 = Instant::now();
        let mut s = active_session(&config, t0);

        // 31s later: client hasn't sent anything → TestRequest.
        let later = t0 + Duration::from_secs(31);
        let action = s.check_heartbeat(later, &config);

        assert_eq!(action, SessionAction::SendFix);
        assert!(s.test_request_sent_at.is_some());
        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_TEST_REQUEST);
    }

    #[test]
    fn heartbeat_test_request_unanswered_disconnects() {
        let config = make_config("FIRM_A", "MELIN");
        let t0 = Instant::now();
        let mut s = active_session(&config, t0);

        // First tick: client silent → TestRequest goes out at t+31s.
        let t1 = t0 + Duration::from_secs(31);
        s.check_heartbeat(t1, &config);
        assert!(s.test_request_sent_at.is_some());

        // Second tick: another HeartBtInt later, still no client reply.
        let t2 = t1 + Duration::from_secs(31);
        let action = s.check_heartbeat(t2, &config);
        assert_eq!(action, SessionAction::Close);
        assert!(matches!(s.state, SessionState::Closing));
    }

    #[test]
    fn heartbeat_inbound_message_clears_test_request_probe() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());
        s.test_request_sent_at = Some(Instant::now());

        // Any valid inbound message should clear the probe.
        let raw = FixMessageBuilder::new(tags::MSG_HEARTBEAT).build("FIRM_A", "MELIN", 2);
        s.handle_fix_message(&raw, &config, &smap, &sym);
        assert!(s.test_request_sent_at.is_none());
    }

    #[test]
    fn heartbeat_check_in_non_active_state_is_noop() {
        let config = make_config("FIRM_A", "MELIN");
        let mut s = new_session(Instant::now()); // AwaitingLogon
        let action = s.check_heartbeat(Instant::now() + Duration::from_secs(60), &config);
        assert_eq!(action, SessionAction::None);
    }

    // -----------------------------------------------------------------------
    // Inbound Melin frames (Active)
    // -----------------------------------------------------------------------

    #[test]
    fn melin_placed_emits_execution_report() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        // Pretend an order was submitted: register the ClOrdID and side.
        let order_id = s.id_map.insert("ORD1");
        s.order_symbols.insert(
            order_id,
            OrderSymbolInfo {
                fix_symbol: "BTC/USD".to_owned(),
                tick_inverse: 100,
                lot_inverse: 1,
                side: Side::Buy,
            },
        );

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Placed {
                order_id,
                side: Side::Buy,
                price: px(5_000_000),
                quantity: qty(10),
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::SendFix);

        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(parsed.get_str(tags::EXEC_TYPE), Some("0")); // New
        assert_eq!(parsed.get_str(tags::CL_ORD_ID), Some("ORD1"));
        assert_eq!(parsed.get_str(tags::PRICE), Some("50000.00"));
    }

    #[test]
    fn melin_self_trade_emits_two_fill_reports() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        // Register both sides of the fill in this session's id_map
        // (i.e., a self-trade by the same firm).
        let maker = s.id_map.insert("MAKER");
        let taker = s.id_map.insert("TAKER");
        for (id, side) in [(maker, Side::Sell), (taker, Side::Buy)] {
            s.order_symbols.insert(
                id,
                OrderSymbolInfo {
                    fix_symbol: "BTC/USD".to_owned(),
                    tick_inverse: 100,
                    lot_inverse: 1,
                    side,
                },
            );
        }

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Fill {
                maker_order_id: maker,
                taker_order_id: taker,
                maker_account: AccountId(7),
                taker_account: AccountId(7),
                price: px(5_000_000),
                quantity: qty(5),
                maker_fee: -10,
                taker_fee: 25,
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::SendFix);

        // Two ExecutionReports should be in the buffer back-to-back.
        // Drain them one at a time using the same framing helper the
        // event loop uses.
        let mut buf = s.fix_send_buf.clone();
        let raw1 = crate::fix::parse::try_extract_message(&mut buf).expect("first msg");
        let raw2 = crate::fix::parse::try_extract_message(&mut buf).expect("second msg");
        assert!(buf.is_empty(), "exactly two messages expected");

        let first = FixMessage::parse(&raw1).unwrap();
        let second = FixMessage::parse(&raw2).unwrap();
        assert_eq!(first.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(first.get_str(tags::EXEC_TYPE), Some("F"));
        assert_eq!(second.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(second.get_str(tags::EXEC_TYPE), Some("F"));
        // The two reports should carry opposite Side fields.
        assert_ne!(
            first.get_str(tags::SIDE),
            second.get_str(tags::SIDE),
            "maker and taker sides should differ"
        );
        // exec_id was bumped twice.
        assert_eq!(s.exec_id, 3);
    }

    #[test]
    fn melin_fill_with_no_session_orders_emits_nothing() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        // Neither order_id is in this session's id_map → no reports.
        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Fill {
                maker_order_id: OrderId(999),
                taker_order_id: OrderId(1000),
                maker_account: AccountId(99),
                taker_account: AccountId(100),
                price: px(5_000_000),
                quantity: qty(5),
                maker_fee: 0,
                taker_fee: 0,
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::None);
        assert!(s.fix_send_buf.is_empty());
    }

    #[test]
    fn melin_rejected_pending_cancel_emits_cancel_reject() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let order_id = s.id_map.insert("ORD1");
        s.pending_cancels.insert(
            order_id,
            PendingCancel {
                cancel_clord_id: "CXL1".to_owned(),
                is_replace: false,
            },
        );

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Rejected {
                order_id,
                account: AccountId(7),
                reason: RejectReason::UnknownOrder,
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::SendFix);

        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_ORDER_CANCEL_REJECT);
        // pending_cancels entry was consumed.
        assert!(!s.pending_cancels.contains_key(&order_id));
    }

    #[test]
    fn melin_rejected_no_pending_cancel_emits_execution_report() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let order_id = s.id_map.insert("ORD1");

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Rejected {
                order_id,
                account: AccountId(7),
                reason: RejectReason::InsufficientBalance,
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::SendFix);

        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(parsed.get_str(tags::EXEC_TYPE), Some("8")); // Rejected
    }

    #[test]
    fn melin_cancelled_clears_pending_cancel() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let order_id = s.id_map.insert("ORD1");
        s.order_symbols.insert(
            order_id,
            OrderSymbolInfo {
                fix_symbol: "BTC/USD".to_owned(),
                tick_inverse: 100,
                lot_inverse: 1,
                side: Side::Buy,
            },
        );
        s.pending_cancels.insert(
            order_id,
            PendingCancel {
                cancel_clord_id: "CXL1".to_owned(),
                is_replace: false,
            },
        );

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Cancelled {
                order_id,
                account: AccountId(7),
                remaining_quantity: qty(5),
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::SendFix);
        assert!(!s.pending_cancels.contains_key(&order_id));

        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(parsed.get_str(tags::EXEC_TYPE), Some("4")); // Canceled
    }

    #[test]
    fn melin_instrument_status_change_is_dropped() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::InstrumentStatusChanged {
                symbol: Symbol(1),
                status: InstrumentStatus::Enabled,
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::None);
        assert!(s.fix_send_buf.is_empty());
    }

    // -----------------------------------------------------------------------
    // Remaining branches
    // -----------------------------------------------------------------------

    /// Build a CancelReplace request referencing an existing ClOrdID.
    fn cancel_replace_msg(
        sender: &str,
        target: &str,
        seq: u64,
        clord: &str,
        orig: &str,
    ) -> Vec<u8> {
        FixMessageBuilder::new(tags::MSG_ORDER_CANCEL_REPLACE)
            .str_tag(tags::CL_ORD_ID, clord)
            .str_tag(tags::ORIG_CL_ORD_ID, orig)
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "51000.00")
            .str_tag(tags::ORDER_QTY, "15")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build(sender, target, seq)
    }

    #[test]
    fn active_cancel_replace_tracks_pending_with_is_replace_true() {
        let config = make_config("FIRM_A", "MELIN");
        let smap = session_map(&config);
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        // Seed the id_map with an order.
        let order = new_order_msg("FIRM_A", "MELIN", 2, "ORD1");
        s.handle_fix_message(&order, &config, &smap, &sym);
        let order_id = s.id_map.get_order_id("ORD1").unwrap();

        // Cancel-replace it.
        let rpl = cancel_replace_msg("FIRM_A", "MELIN", 3, "RPL1", "ORD1");
        let action = s.handle_fix_message(&rpl, &config, &smap, &sym);
        assert_eq!(action, SessionAction::SendMelin);

        let pending = s
            .pending_cancels
            .get(&order_id)
            .expect("pending cancel-replace not tracked");
        assert_eq!(pending.cancel_clord_id, "RPL1");
        assert!(pending.is_replace, "is_replace should be true for 35=G");
    }

    #[test]
    fn melin_rejected_pending_replace_emits_cancel_reject_for_replace() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let order_id = s.id_map.insert("ORD1");
        s.pending_cancels.insert(
            order_id,
            PendingCancel {
                cancel_clord_id: "RPL1".to_owned(),
                is_replace: true,
            },
        );

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Rejected {
                order_id,
                account: AccountId(7),
                reason: RejectReason::UnknownOrder,
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::SendFix);

        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_ORDER_CANCEL_REJECT);
        // CxlRejResponseTo=2 means the reject is for a cancel-replace
        // (per FIX 4.2: 1=cancel, 2=cancel/replace).
        assert_eq!(parsed.get_str(tags::CXL_REJ_RESPONSE_TO), Some("2"));
        assert!(!s.pending_cancels.contains_key(&order_id));
    }

    #[test]
    fn melin_triggered_emits_execution_report() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let order_id = s.id_map.insert("STOP1");
        s.order_symbols.insert(
            order_id,
            OrderSymbolInfo {
                fix_symbol: "BTC/USD".to_owned(),
                tick_inverse: 100,
                lot_inverse: 1,
                side: Side::Sell,
            },
        );

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Triggered {
                order_id,
                trigger_price: px(4_800_000),
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::SendFix);

        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(parsed.get_str(tags::EXEC_TYPE), Some("L")); // Triggered
        assert_eq!(parsed.get_str(tags::STOP_PX), Some("48000.00"));
        assert_eq!(parsed.get_str(tags::SIDE), Some("2")); // Sell via tracked info
    }

    #[test]
    fn melin_replaced_emits_execution_report_and_clears_pending() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        let order_id = s.id_map.insert("ORD1");
        s.order_symbols.insert(
            order_id,
            OrderSymbolInfo {
                fix_symbol: "BTC/USD".to_owned(),
                tick_inverse: 100,
                lot_inverse: 1,
                side: Side::Buy,
            },
        );
        // Replace-in-flight: pending_cancels populated.
        s.pending_cancels.insert(
            order_id,
            PendingCancel {
                cancel_clord_id: "RPL1".to_owned(),
                is_replace: true,
            },
        );

        push_melin_response(
            &mut s,
            &ResponseKind::Report(ExecutionReport::Replaced {
                order_id,
                side: Side::Buy,
                old_price: px(5_000_000),
                new_price: px(5_100_000),
                old_remaining: qty(10),
                new_remaining: qty(15),
            }),
        );

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::SendFix);
        assert!(
            !s.pending_cancels.contains_key(&order_id),
            "successful Replaced should clear pending_cancels"
        );

        let parsed = FixMessage::parse(&s.fix_send_buf).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(parsed.get_str(tags::EXEC_TYPE), Some("5")); // Replace
        assert_eq!(parsed.get_str(tags::PRICE), Some("51000.00"));
        assert_eq!(parsed.get_str(tags::LEAVES_QTY), Some("15"));
    }

    #[test]
    fn rate_limit_resets_after_window_rolls_over() {
        let mut config = make_config("FIRM_A", "MELIN");
        config.sessions[0].max_msgs_per_sec = 1;
        let smap = session_map(&config);
        let sym = symbol_map(&config);

        let t0 = Instant::now();
        let mut s = active_session(&config, t0);
        s.max_msgs_per_sec = 1;
        s.rate_window_start = t0;

        // First message of window: allowed.
        let m1 = new_order_msg("FIRM_A", "MELIN", 2, "ORD1");
        assert_eq!(
            s.handle_fix_message(&m1, &config, &smap, &sym),
            SessionAction::SendMelin
        );

        // Second within the same window: rejected.
        let m2 = new_order_msg("FIRM_A", "MELIN", 3, "ORD2");
        assert_eq!(
            s.handle_fix_message(&m2, &config, &smap, &sym),
            SessionAction::SendFix
        );

        // Rewind the window start so check_rate_limit sees >1s elapsed
        // and starts a fresh window. (check_rate_limit uses Instant::now
        // internally, so rewinding the stored start is the cleanest way
        // to simulate elapsed time without a clock abstraction.)
        s.rate_window_start = Instant::now() - Duration::from_secs(2);

        let m3 = new_order_msg("FIRM_A", "MELIN", 4, "ORD3");
        assert_eq!(
            s.handle_fix_message(&m3, &config, &smap, &sym),
            SessionAction::SendMelin,
            "window should have rolled over"
        );
        assert_eq!(s.rate_msg_count, 1, "counter should have reset to 1");
    }

    #[test]
    fn melin_undecodable_response_is_silently_dropped() {
        let config = make_config("FIRM_A", "MELIN");
        let sym = symbol_map(&config);
        let mut s = active_session(&config, Instant::now());

        // Inject a length-prefixed frame with a bogus tag byte that
        // codec::decode_response will reject. `handle_active_melin`
        // should log a warning and return SessionAction::None — NOT
        // close the session (decode errors from the engine must not
        // take the session down).
        let payload = [0xFFu8]; // Invalid tag.
        let len = (payload.len() as u32).to_le_bytes();
        s.melin_parse_buf.extend_from_slice(&len);
        s.melin_parse_buf.extend_from_slice(&payload);

        let action = s.try_process_melin_frame(&config, &sym, Instant::now());
        assert_eq!(action, SessionAction::None);
        // Session stays Active.
        assert!(matches!(s.state, SessionState::Active));
        // No FIX bytes queued.
        assert!(s.fix_send_buf.is_empty());
    }
}
