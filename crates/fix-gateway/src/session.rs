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

use melin_engine::types::AccountId;
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
    /// Expected next inbound MsgSeqNum from the FIX client.
    fix_inbound_seq: u64,
    /// Next outbound MsgSeqNum to the FIX client.
    fix_outbound_seq: u64,
    pub sender_comp_id: String,
    pub heartbeat_interval: Duration,
    pub last_fix_recv: Instant,
    pub fix_multishot_active: bool,

    // ── Melin server side ──
    pub melin_fd: Option<RawFd>,
    pub melin_parse_buf: Vec<u8>,
    pub melin_send_buf: Vec<u8>,
    /// Melin request sequence number (per-key monotonic).
    melin_seq: u64,
    /// Reusable encode buffer for Melin requests.
    melin_encode_buf: [u8; 136],
    pub melin_multishot_active: bool,

    // ── Session-owned data ──
    id_map: ClOrdIdMap,
    account_id: AccountId,
    signing_key: Option<SigningKey>,
    /// Index into config.sessions for this FIX session.
    session_config_idx: Option<usize>,
    /// Monotonic ExecID counter for FIX execution reports (tag 17).
    exec_id: u64,

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
            fix_inbound_seq: 1,
            fix_outbound_seq: 1,
            sender_comp_id: String::new(),
            heartbeat_interval: Duration::from_secs(30),
            last_fix_recv: now,
            fix_multishot_active: false,

            melin_fd: None,
            melin_parse_buf: Vec::with_capacity(256),
            melin_send_buf: Vec::with_capacity(256),
            melin_seq: 0,
            melin_encode_buf: [0u8; 136],
            melin_multishot_active: false,

            id_map: ClOrdIdMap::new(),
            account_id: AccountId(0),
            signing_key: None,
            session_config_idx: None,
            exec_id: 1,

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
                self.fix_send_buf.extend_from_slice(&logon_response);
                self.fix_outbound_seq += 1;

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

        // Validate MsgSeqNum.
        if let Some(seq) = msg.msg_seq_num() {
            if seq < self.fix_inbound_seq {
                // Duplicate — ignore.
                return SessionAction::None;
            }
            if seq > self.fix_inbound_seq {
                // Gap — disconnect (v1: no gap fill).
                warn!(
                    expected = self.fix_inbound_seq,
                    got = seq,
                    "MsgSeqNum gap"
                );
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
                self.fix_send_buf.extend_from_slice(&hb);
                self.fix_outbound_seq += 1;
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
                self.translate_and_send_order(msg_type, &msg, config, symbol_map)
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
                // TODO: track order→symbol mapping for proper symbol/tick
                // resolution. For v1, use defaults.
                let default_symbol = "UNKNOWN";
                let default_tick = 1u64;
                let default_lot = 1u64;

                let fix_msg = translate::execution_report_to_fix(
                    report,
                    &self.id_map,
                    default_symbol,
                    default_tick,
                    default_lot,
                    &config.target_comp_id,
                    &self.sender_comp_id,
                    self.fix_outbound_seq,
                    self.exec_id,
                );

                if !fix_msg.is_empty() {
                    self.fix_send_buf.extend_from_slice(&fix_msg);
                    self.fix_outbound_seq += 1;
                    self.exec_id += 1;
                    SessionAction::SendFix
                } else {
                    SessionAction::None
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
        self.fix_send_buf.extend_from_slice(&msg);
        self.fix_outbound_seq += 1;
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
        self.fix_send_buf.extend_from_slice(&msg);
        self.fix_outbound_seq += 1;
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Close the FIX client socket.
        unsafe { libc::close(self.fix_fd) };
        // Close the Melin socket if open.
        if let Some(fd) = self.melin_fd {
            unsafe { libc::close(fd) };
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
