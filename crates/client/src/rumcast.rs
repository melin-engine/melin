//! Rumcast-backed client. Same public API as the TCP client, but the
//! wire is reliable UDP via `melin-rumcast`. Built when the crate is
//! compiled with `--features rumcast`. Mirrors the server's
//! `rumcast_transport` constants exactly so tests can spawn a rumcast
//! server and connect with this client without configuration drift.
//!
//! Single-session: each `Client` owns one ephemeral local UDP port,
//! one `MuxedSender`/`MuxedReceiver` pair, and one rumcast session.
//! The bench's multi-client `crates/bench/src/rumcast.rs` is the
//! multi-session generalization of this file.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;

use melin_protocol::codec;
use melin_protocol::error::ProtocolError;
use melin_protocol::message::{Request, ResponseKind};
use melin_protocol::session::{ClientHandshake, encode_envelope, verify_and_decode_envelope};
use melin_rumcast::flow_control::FlowControl;
use melin_rumcast::muxed_receiver::{MuxedReceiver, MuxedReceiverConfig};
use melin_rumcast::muxed_sender::{MuxedSender, MuxedSenderConfig};
use melin_rumcast::pub_log::PublicationLog;
use melin_rumcast::shared_udp::{SharedUdp, SharedUdpRecv, SharedUdpSend};
use melin_rumcast::wire::{FrameView, data_flags};

use crate::{ClientError, StatsSnapshot};

// MUST match `melin-server/src/rumcast_transport.rs`.
const RUMCAST_ORDERS_STREAM: u32 = 1;
const RUMCAST_RESP_STREAM: u32 = 2;
const TERM_LENGTH: u32 = 1024 * 1024;
const MTU: u32 = 1408;
const INITIAL_TERM_ID: u32 = 1;

/// Local receiver_id sent in our SMs back to the server. Single-
/// session, single-receiver — value is arbitrary.
const CLIENT_RECEIVER_ID: u64 = 1;

/// Reusable envelope-encode buffer size. Worst-case inner payload is
/// ChallengeResponse (≤168 bytes). Add the 24-byte envelope header
/// and round up — 512 leaves room for any future request growth
/// without ever reallocating on the hot path.
const ENVELOPE_BUF_SIZE: usize = 512;

/// Upper bound on a single handshake step / response-batch wait.
/// Generous enough that loopback / LAN never trips it; if it does
/// fire, something is genuinely wrong (server crashed, network black-
/// holed). Tests run on loopback in milliseconds.
const REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// Sleep granularity when no inbound activity. Short enough that
/// tests don't observe artificial latency, long enough that an idle
/// client doesn't burn a core.
const IDLE_SLEEP: Duration = Duration::from_micros(50);

/// Rumcast Client. Single session, single ephemeral local UDP port.
pub struct Client {
    muxed_sender: MuxedSender<SharedUdpSend>,
    muxed_receiver: MuxedReceiver<SharedUdpRecv>,
    pub_log: Arc<PublicationLog>,
    session_id: u32,
    session_token: [u8; 32],
    /// Outbound envelope sequence — increments on every published
    /// frame (including handshake). Distinct from `next_seq`, which
    /// is the application-layer request_seq used for engine dedup.
    envelope_outbound_seq: u64,
    /// Last accepted inbound envelope seq for replay-protection in
    /// `verify_and_decode_envelope`.
    envelope_inbound_seq: u64,
    /// Per-connection monotonic request_seq. Same semantics as the
    /// TCP client: starts at 0, increments before each send,
    /// Heartbeats use seq=0 (exempt from dedup).
    next_seq: u64,
    /// Reused envelope-encode buffer; see [`ENVELOPE_BUF_SIZE`].
    envelope_buf: Vec<u8>,
    /// Reused request-encode buffer. Same 168-byte sizing rationale
    /// as the TCP client.
    encode_buf: [u8; 168],
    /// FIFO of decoded inner-payload frames received but not yet
    /// consumed by [`Self::send_request`]. Required because
    /// `MuxedReceiver::poll` advances the per-session cursor for
    /// every frame the callback observes — frames not stashed here
    /// are lost. The server can emit multiple frames per request
    /// (e.g. `Report` + `BatchEnd`), and they may surface in a
    /// single `poll`, so this queue is non-optional.
    response_queue: VecDeque<Vec<u8>>,
}

impl Client {
    /// Connect to a rumcast trading server with Ed25519 challenge-
    /// response auth. Same four-message flow as TCP, but the
    /// transport is rumcast-over-UDP. Picks an ephemeral local UDP
    /// port and a random `session_id` per connect — reconnects from
    /// the same process never collide on the server's session table.
    pub fn connect(addr: SocketAddr, key: &SigningKey) -> Result<Self, ClientError> {
        // Bind ephemeral on the same address family as the target.
        // Loopback target → loopback bind (kernel route is symmetric);
        // any other target → unspecified bind so the kernel picks an
        // appropriate source IP.
        let bind: SocketAddr = if addr.ip().is_loopback() {
            match addr {
                SocketAddr::V4(_) => "127.0.0.1:0".parse().unwrap(),
                SocketAddr::V6(_) => "[::1]:0".parse().unwrap(),
            }
        } else {
            match addr {
                SocketAddr::V4(_) => "0.0.0.0:0".parse().unwrap(),
                SocketAddr::V6(_) => "[::]:0".parse().unwrap(),
            }
        };
        let shared = SharedUdp::bind(bind)?;
        let (send_half, recv_half) = shared.split();

        // Random session_id; collision with concurrent client of same
        // server is astronomically unlikely (2^32 space).
        let mut sid_bytes = [0u8; 4];
        getrandom::fill(&mut sid_bytes)
            .map_err(|e| ClientError::Io(std::io::Error::other(format!("getrandom: {e}"))))?;
        let session_id = u32::from_le_bytes(sid_bytes);

        // max_sessions = 2 leaves slack should the server ever bounce
        // a NAK/SM tagged with a stale session_id during a reconnect
        // burst — single live session, one reserve slot.
        let max_sessions: u32 = 2;

        let mut muxed_sender = MuxedSender::new(
            send_half,
            MuxedSenderConfig {
                stream_id: RUMCAST_ORDERS_STREAM,
                initial_term_id: INITIAL_TERM_ID,
                term_length: TERM_LENGTH,
                mtu: MTU,
                setup_interval: Duration::from_millis(100),
                heartbeat_interval: Duration::from_millis(50),
                max_drain_per_tick: 64 * 1024,
                max_control_per_tick: 32,
                // Single receiver — Min and Max degenerate. Pick Min
                // to match the server-side default.
                flow_control: FlowControl::Min,
                max_sessions,
            },
        );
        let mut muxed_receiver = MuxedReceiver::new(
            recv_half,
            MuxedReceiverConfig {
                stream_id: RUMCAST_RESP_STREAM,
                receiver_id: CLIENT_RECEIVER_ID,
                initial_term_id: INITIAL_TERM_ID,
                term_length: TERM_LENGTH,
                sm_interval: Duration::from_millis(2),
                nak_backoff_min: Duration::from_micros(50),
                nak_backoff_jitter: Duration::from_micros(50),
                max_recv_per_tick: 1024,
                max_sessions,
            },
        );

        let pub_log = muxed_sender.create_session(session_id, addr).map_err(|e| {
            ClientError::Io(std::io::Error::other(format!("create_session: {e:?}")))
        })?;
        // Single producer, no peer-pacing required: we trust ourselves
        // to keep our publog ahead of the server's subscriber. Avoids
        // the wait-for-first-SM stall during handshake.
        pub_log.set_publisher_limit(u64::MAX);
        muxed_sender.send_setup_now(session_id);

        // ---- Handshake ----
        // Step 1: kick the server with an in-stream Heartbeat so it
        // allocates the per-session matcher state and emits Challenge.
        let envelope_outbound_seq: u64 = 0;
        let mut encode_buf = [0u8; 168];
        let envelope_buf = vec![0u8; ENVELOPE_BUF_SIZE];

        let written = codec::encode_request(&Request::Heartbeat, 0, &mut encode_buf)?;
        publish_blocking(&pub_log, &encode_buf[4..written]);

        // Step 2: receive Challenge. Pre-auth: the server sends
        // Challenge as a bare codec response (no envelope) — there's
        // no shared session_token to MAC it with yet.
        let challenge_bytes = wait_for_response(
            &mut muxed_sender,
            &mut muxed_receiver,
            session_id,
            REQUEST_DEADLINE,
            |bytes| {
                matches!(
                    codec::decode_response(bytes),
                    Ok(ResponseKind::Challenge { .. })
                )
            },
        )?;
        let (nonce, server_eph) = match codec::decode_response(&challenge_bytes)? {
            ResponseKind::Challenge {
                nonce,
                server_x25519_eph,
            } => (nonce, server_x25519_eph),
            _ => unreachable!("predicate filtered to Challenge"),
        };

        // Step 3: produce ChallengeResponse + derive session_token.
        let mut x25519_secret_bytes = [0u8; 32];
        getrandom::fill(&mut x25519_secret_bytes)
            .map_err(|e| ClientError::Io(std::io::Error::other(format!("getrandom: {e}"))))?;
        let handshake = ClientHandshake::new(key, x25519_secret_bytes);
        let completed = handshake.finish(&nonce, &server_eph);

        let written = codec::encode_request(&completed.challenge_response, 0, &mut encode_buf)?;
        publish_blocking(&pub_log, &encode_buf[4..written]);

        // Step 4: wait for ServerReady (or AuthFailed). Still bare
        // codec — server only switches to envelope mode after auth
        // succeeds.
        let ready_bytes = wait_for_response(
            &mut muxed_sender,
            &mut muxed_receiver,
            session_id,
            REQUEST_DEADLINE,
            |bytes| {
                matches!(
                    codec::decode_response(bytes),
                    Ok(ResponseKind::ServerReady) | Ok(ResponseKind::AuthFailed)
                )
            },
        )?;
        match codec::decode_response(&ready_bytes)? {
            ResponseKind::ServerReady => {}
            ResponseKind::AuthFailed => return Err(ClientError::AuthFailed),
            _ => {
                return Err(ClientError::Protocol(ProtocolError::InvalidField(
                    "expected ServerReady or AuthFailed",
                )));
            }
        }

        Ok(Self {
            muxed_sender,
            muxed_receiver,
            pub_log,
            session_id,
            session_token: completed.session_token,
            envelope_outbound_seq,
            envelope_inbound_seq: 0,
            next_seq: 0,
            envelope_buf,
            encode_buf: [0u8; 168],
            response_queue: VecDeque::new(),
        })
    }

    /// Send a request and collect all responses until BatchEnd. Same
    /// contract as the TCP client.
    pub fn send_request(&mut self, request: &Request) -> Result<Vec<ResponseKind>, ClientError> {
        self.next_seq += 1;
        let written = codec::encode_request(request, self.next_seq, &mut self.encode_buf)?;
        let inner = &self.encode_buf[4..written];

        self.envelope_outbound_seq += 1;
        let env_len = encode_envelope(
            &self.session_token,
            self.session_id,
            self.envelope_outbound_seq,
            inner,
            &mut self.envelope_buf,
        )
        .map_err(|e| ClientError::Io(std::io::Error::other(format!("encode_envelope: {e:?}"))))?;
        publish_blocking(&self.pub_log, &self.envelope_buf[..env_len]);

        let mut responses = Vec::new();
        let deadline = Instant::now() + REQUEST_DEADLINE;
        loop {
            let inner_bytes = self.next_inner_frame(deadline)?;
            let kind = codec::decode_response(&inner_bytes)?;
            match kind {
                ResponseKind::BatchEnd => return Ok(responses),
                ResponseKind::Heartbeat | ResponseKind::ServerReady => continue,
                ResponseKind::ServerBusy => return Err(ClientError::ServerBusy),
                other => responses.push(other),
            }
        }
    }

    /// Pop the next decoded inner-payload from the response queue,
    /// refilling it via tick + poll if empty. Blocks (with periodic
    /// idle-sleep) until a frame arrives or `deadline` expires.
    fn next_inner_frame(&mut self, deadline: Instant) -> Result<Vec<u8>, ClientError> {
        loop {
            if let Some(bytes) = self.response_queue.pop_front() {
                return Ok(bytes);
            }
            self.muxed_sender.tick();
            self.muxed_receiver.tick();

            let session_id = self.session_id;
            let token = &self.session_token;
            let last_seq = &mut self.envelope_inbound_seq;
            let queue = &mut self.response_queue;
            let mut decode_err: Option<ClientError> = None;

            self.muxed_receiver.poll(64 * 1024, |sid, _src, view| {
                if decode_err.is_some() || sid != session_id {
                    return;
                }
                let FrameView::Data { header, payload } = view else {
                    return;
                };
                if header.common.flags & data_flags::PADDING != 0 {
                    return;
                }
                match verify_and_decode_envelope(token, sid, *last_seq, payload) {
                    Ok((seq, inner)) => {
                        *last_seq = seq;
                        // Stash a copy — the borrow into the
                        // SubscriptionLog dies when the callback
                        // returns, so we can't keep the &[u8].
                        queue.push_back(inner.to_vec());
                    }
                    Err(e) => {
                        decode_err = Some(ClientError::Io(std::io::Error::other(format!(
                            "envelope verify: {e:?}"
                        ))));
                    }
                }
            });

            if let Some(err) = decode_err {
                return Err(err);
            }
            if !self.response_queue.is_empty() {
                continue;
            }
            if Instant::now() >= deadline {
                return Err(ClientError::Timeout);
            }
            std::thread::sleep(IDLE_SLEEP);
        }
    }

    /// Adopt the engine's current request_seq HWM for this session's
    /// authenticated key. Same semantics as the TCP path.
    pub fn synchronize_request_seq(&mut self) -> Result<u64, ClientError> {
        let responses = self.send_request(&Request::QueryRequestSeq)?;
        for resp in &responses {
            if let ResponseKind::RequestSeqHwm { hwm } = resp {
                self.next_seq = *hwm;
                return Ok(*hwm);
            }
        }
        Err(ClientError::Protocol(ProtocolError::InvalidField(
            "no RequestSeqHwm in response",
        )))
    }

    /// Query server stats. Same contract as the TCP path.
    pub fn query_stats(&mut self) -> Result<StatsSnapshot, ClientError> {
        let responses = self.send_request(&Request::QueryStats)?;
        for resp in &responses {
            if let ResponseKind::StatsHeader {
                active_connections,
                events_processed,
                journal_sequence,
            } = resp
            {
                return Ok(StatsSnapshot {
                    active_connections: *active_connections,
                    events_processed: *events_processed,
                    journal_sequence: *journal_sequence,
                });
            }
        }
        Err(ClientError::Protocol(ProtocolError::InvalidField(
            "no StatsHeader in response",
        )))
    }
}

/// Spin-claim and publish a single-fragment payload. Used for both
/// the four handshake frames and post-auth requests — payloads are
/// well under MTU so single-fragment publish is always fine.
fn publish_blocking(pub_log: &PublicationLog, payload: &[u8]) {
    loop {
        match pub_log.try_claim(payload.len() as u32) {
            Ok(mut claim) => {
                claim.payload_mut().copy_from_slice(payload);
                claim.publish(data_flags::UNFRAGMENTED);
                return;
            }
            // Backpressure here would mean the publog is full
            // (server hasn't acked enough to advance publisher_limit).
            // We set publisher_limit = u64::MAX, so this branch only
            // fires under genuine ring-buffer exhaustion (>1 MiB
            // unacked). Spin-yield and retry.
            Err(_) => std::hint::spin_loop(),
        }
    }
}

/// Drive both muxers and poll for an inbound bare-codec frame matching
/// `predicate`, returning its payload. Pre-auth only — handshake-stage
/// responses (Challenge, ServerReady, AuthFailed) are unwrapped, no
/// envelope. Post-auth, [`Client::next_inner_frame`] handles the
/// envelope path because it has to buffer multi-frame batches.
fn wait_for_response(
    muxed_sender: &mut MuxedSender<SharedUdpSend>,
    muxed_receiver: &mut MuxedReceiver<SharedUdpRecv>,
    target_sid: u32,
    timeout: Duration,
    predicate: impl Fn(&[u8]) -> bool,
) -> Result<Vec<u8>, ClientError> {
    let deadline = Instant::now() + timeout;
    loop {
        muxed_sender.tick();
        muxed_receiver.tick();

        let mut found: Option<Vec<u8>> = None;
        muxed_receiver.poll(64 * 1024, |sid, _src, view| {
            if found.is_some() || sid != target_sid {
                return;
            }
            let FrameView::Data { header, payload } = view else {
                return;
            };
            if header.common.flags & data_flags::PADDING != 0 {
                return;
            }
            if predicate(payload) {
                found = Some(payload.to_vec());
            }
        });

        if let Some(bytes) = found {
            return Ok(bytes);
        }
        if Instant::now() >= deadline {
            return Err(ClientError::Timeout);
        }
        std::thread::sleep(IDLE_SLEEP);
    }
}
