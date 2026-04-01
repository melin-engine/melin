//! FIX session management: Logon handshake, heartbeat, message routing,
//! and two-thread forwarding between FIX client and Melin server.

use std::collections::HashMap;
use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::{Signer, SigningKey};
use tracing::{debug, error, info, warn};

use melin_engine::types::AccountId;
use melin_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
use melin_protocol::codec;
use melin_protocol::message::ResponseKind;

use crate::config::{GatewayConfig, SymbolConfig};
use crate::fix::parse::{self, FixMessage};
use crate::fix::serialize::FixMessageBuilder;
use crate::fix::tags;
use crate::id_map::ClOrdIdMap;
use crate::translate::{self, TranslateContext};

/// Run a FIX session for one client connection.
///
/// Handles the full lifecycle: Logon, message forwarding, Logout.
/// Blocks until the session ends (client disconnect or error).
pub fn run_session(
    client_stream: TcpStream,
    config: &GatewayConfig,
    shutdown: &AtomicBool,
) {
    let peer = client_stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".into());
    info!(peer = %peer, "FIX client connected");

    if let Err(e) = run_session_inner(client_stream, config, shutdown) {
        debug!(peer = %peer, error = %e, "FIX session ended");
    } else {
        info!(peer = %peer, "FIX session ended cleanly");
    }
}

fn run_session_inner(
    mut client_stream: TcpStream,
    config: &GatewayConfig,
    shutdown: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set a read timeout so we can check for shutdown periodically.
    client_stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    client_stream.set_nodelay(true)?;

    // ── Phase 1: Await Logon ───────────────────────────────────────

    let logon_raw = loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        match parse::read_message(&mut client_stream)? {
            Some(raw) => break raw,
            None => return Ok(()), // Client disconnected before Logon.
        }
    };
    let logon_msg = FixMessage::parse(&logon_raw)?;

    if logon_msg.msg_type() != tags::MSG_LOGON {
        send_logout(&mut client_stream, config, "UNKNOWN", 1, "first message must be Logon")?;
        return Err("first message was not Logon".into());
    }

    let sender_comp_id = logon_msg
        .sender_comp_id()
        .ok_or("Logon missing SenderCompID")?;

    // Look up session config.
    let session_map = config.session_map();
    let session_idx = session_map
        .get(sender_comp_id)
        .ok_or_else(|| format!("unknown SenderCompID: {sender_comp_id}"))?;
    let session_config = &config.sessions[*session_idx];

    info!(
        sender = sender_comp_id,
        account = session_config.account_id,
        "FIX Logon received"
    );

    // HeartBtInt from Logon (default 30s).
    let heartbeat_secs: u64 = logon_msg
        .get_str(tags::HEART_BT_INT)
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    // ── Phase 2: Connect to Melin server ───────────────────────────

    let melin_stream = TcpStream::connect_timeout(
        &config.server_addr,
        Duration::from_secs(10),
    )?;
    melin_stream.set_nodelay(true)?;

    // Authenticate with Ed25519.
    let signing_key = load_signing_key(&session_config.key_path)?;
    let (melin_reader, melin_writer) = authenticate_melin(melin_stream, &signing_key)?;

    info!(
        sender = sender_comp_id,
        server = %config.server_addr,
        "authenticated with Melin server"
    );

    // ── Phase 3: Send Logon response ───────────────────────────────

    let mut outbound_seq: u64 = 1;
    let logon_response = FixMessageBuilder::new(tags::MSG_LOGON)
        .str_tag(tags::ENCRYPT_METHOD, "0")
        .u64_tag(tags::HEART_BT_INT, heartbeat_secs)
        .build(&config.target_comp_id, sender_comp_id, outbound_seq);
    client_stream.write_all(&logon_response)?;
    client_stream.flush()?;
    outbound_seq += 1;

    // ── Phase 4: Two-thread message forwarding ─────────────────────

    let session_done = Arc::new(AtomicBool::new(false));

    // Build symbol lookup map.
    let symbol_map: HashMap<String, SymbolConfig> = config
        .symbols
        .iter()
        .cloned()
        .map(|s| (s.fix_symbol.clone(), s))
        .collect();

    // Shared state for the outbound thread.
    let sender_id = sender_comp_id.to_owned();
    let target_id = config.target_comp_id.clone();

    // Clone stream for outbound thread.
    let mut fix_writer = client_stream.try_clone()?;

    let done_flag = Arc::clone(&session_done);
    let outbound_symbols: HashMap<String, SymbolConfig> = symbol_map
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    // The outbound thread reads from Melin and sends FIX execution reports.
    // It needs its own id_map reference — we use a shared mutex since
    // both threads need to access it (inbound inserts, outbound reads).
    let id_map = Arc::new(std::sync::Mutex::new(ClOrdIdMap::new()));
    let outbound_id_map = Arc::clone(&id_map);

    let outbound_handle = std::thread::Builder::new()
        .name(format!("fix-out-{sender_id}"))
        .spawn(move || {
            run_outbound(
                melin_reader,
                &mut fix_writer,
                &outbound_id_map,
                &outbound_symbols,
                &sender_id,
                &target_id,
                &mut outbound_seq,
                &done_flag,
            );
        })?;

    // The inbound thread reads FIX messages and sends Melin requests.
    let mut inbound_seq: u64 = 1; // Expected next MsgSeqNum from client.
    let mut melin_seq: u64 = 1; // Melin request sequence.
    let mut encode_buf = [0u8; 136];
    let heartbeat_interval = Duration::from_secs(heartbeat_secs);
    let mut last_recv = Instant::now();

    loop {
        if shutdown.load(Ordering::Relaxed) || session_done.load(Ordering::Relaxed) {
            break;
        }

        // Read next FIX message (with timeout for shutdown checking).
        let raw = match parse::read_message(&mut client_stream) {
            Ok(Some(raw)) => raw,
            Ok(None) => {
                info!("FIX client disconnected");
                break;
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
                // Check if we should send a heartbeat.
                if last_recv.elapsed() > heartbeat_interval * 2 {
                    // Client is unresponsive — disconnect.
                    warn!("FIX client heartbeat timeout");
                    break;
                }
                continue;
            }
            Err(e) => {
                debug!(error = %e, "FIX read error");
                break;
            }
        };

        last_recv = Instant::now();

        let msg = match FixMessage::parse(&raw) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "malformed FIX message, sending Reject");
                // Send session-level Reject.
                let reject = FixMessageBuilder::new(tags::MSG_REJECT)
                    .str_tag(tags::TEXT, &e.to_string())
                    .build(
                        &config.target_comp_id,
                        sender_comp_id,
                        outbound_seq,
                    );
                let _ = client_stream.write_all(&reject);
                let _ = client_stream.flush();
                outbound_seq += 1;
                continue;
            }
        };

        // Validate MsgSeqNum.
        if let Some(seq) = msg.msg_seq_num() {
            if seq < inbound_seq {
                // Duplicate — ignore.
                continue;
            }
            if seq > inbound_seq {
                // Gap — disconnect (v1: no gap fill).
                warn!(expected = inbound_seq, got = seq, "MsgSeqNum gap, disconnecting");
                let _ = send_logout(
                    &mut client_stream,
                    config,
                    sender_comp_id,
                    outbound_seq,
                    "MsgSeqNum too high, expected sequence reset",
                );
                break;
            }
            inbound_seq += 1;
        }

        // Route by MsgType.
        let msg_type = msg.msg_type();
        match msg_type {
            tags::MSG_HEARTBEAT => {
                // Client heartbeat — no action needed.
            }
            tags::MSG_TEST_REQUEST => {
                // Respond with Heartbeat containing TestReqID.
                let test_req_id = msg.get_str(tags::TEST_REQ_ID).unwrap_or("");
                let hb = FixMessageBuilder::new(tags::MSG_HEARTBEAT)
                    .str_tag(tags::TEST_REQ_ID, test_req_id)
                    .build(
                        &config.target_comp_id,
                        sender_comp_id,
                        outbound_seq,
                    );
                client_stream.write_all(&hb)?;
                client_stream.flush()?;
                outbound_seq += 1;
            }
            tags::MSG_LOGOUT => {
                info!("FIX Logout received");
                let _ = send_logout(
                    &mut client_stream,
                    config,
                    sender_comp_id,
                    outbound_seq,
                    "Logout acknowledged",
                );
                break;
            }
            tags::MSG_NEW_ORDER_SINGLE | tags::MSG_ORDER_CANCEL_REQUEST | tags::MSG_ORDER_CANCEL_REPLACE => {
                let mut map = id_map.lock().unwrap();
                let mut ctx = TranslateContext {
                    account_id: AccountId(session_config.account_id),
                    symbols: &symbol_map,
                    id_map: &mut map,
                };

                let request = match msg_type {
                    tags::MSG_NEW_ORDER_SINGLE => translate::new_order_single(&msg, &mut ctx),
                    tags::MSG_ORDER_CANCEL_REQUEST => translate::cancel_order(&msg, &mut ctx),
                    tags::MSG_ORDER_CANCEL_REPLACE => translate::cancel_replace(&msg, &mut ctx),
                    _ => unreachable!(),
                };

                match request {
                    Ok(req) => {
                        melin_seq += 1;
                        let written = codec::encode_request(&req, melin_seq, &mut encode_buf)?;
                        melin_writer.lock().unwrap().write_frame(&encode_buf[4..written])?;
                        melin_writer.lock().unwrap().flush()?;
                    }
                    Err(e) => {
                        warn!(error = %e, "FIX translation error");
                        // Send BusinessReject or Reject.
                        let reject = FixMessageBuilder::new(tags::MSG_REJECT)
                            .str_tag(tags::TEXT, &e.to_string())
                            .build(
                                &config.target_comp_id,
                                sender_comp_id,
                                outbound_seq,
                            );
                        client_stream.write_all(&reject)?;
                        client_stream.flush()?;
                        outbound_seq += 1;
                    }
                }
            }
            _ => {
                warn!(msg_type = ?std::str::from_utf8(msg_type), "unsupported FIX message type");
                let reject = FixMessageBuilder::new(tags::MSG_REJECT)
                    .str_tag(tags::TEXT, "unsupported message type")
                    .build(
                        &config.target_comp_id,
                        sender_comp_id,
                        outbound_seq,
                    );
                client_stream.write_all(&reject)?;
                client_stream.flush()?;
                outbound_seq += 1;
            }
        }
    }

    // Signal outbound thread to stop and wait.
    session_done.store(true, Ordering::Relaxed);
    let _ = outbound_handle.join();

    Ok(())
}

/// Outbound thread: reads Melin responses and sends FIX execution reports.
fn run_outbound(
    mut melin_reader: BlockingFrameReader<TcpStream>,
    fix_writer: &mut TcpStream,
    id_map: &std::sync::Mutex<ClOrdIdMap>,
    symbols: &HashMap<String, SymbolConfig>,
    sender: &str,
    target: &str,
    seq: &mut u64,
    done: &AtomicBool,
) {
    let mut exec_id: u64 = 1;

    // Build reverse symbol lookup: Melin symbol ID → (fix_symbol, config).
    let _reverse_symbols: HashMap<u32, (&str, &SymbolConfig)> = symbols
        .iter()
        .map(|(name, cfg)| (cfg.melin_symbol, (name.as_str(), cfg)))
        .collect();

    // Default symbol info for unknown symbols.
    let default_symbol = "UNKNOWN";
    let default_tick = 1u64;
    let default_lot = 1u64;

    loop {
        if done.load(Ordering::Relaxed) {
            return;
        }

        let frame = match melin_reader.read_frame() {
            Ok(Some(f)) => f.to_vec(), // Copy out of reader's internal buffer.
            Ok(None) => {
                info!("Melin server disconnected");
                done.store(true, Ordering::Relaxed);
                return;
            }
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => {
                debug!(error = %e, "Melin read error");
                done.store(true, Ordering::Relaxed);
                return;
            }
        };

        let response = match codec::decode_response(&frame) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to decode Melin response");
                continue;
            }
        };

        match response {
            ResponseKind::Report(ref report) => {
                // Determine symbol for this report (best-effort).
                let (sym_str, tick_inv, lot_inv) = match report {
                    // Reports don't carry the symbol — we'd need to track
                    // order→symbol mapping. For v1, use a default.
                    _ => (default_symbol, default_tick, default_lot),
                };

                let map = id_map.lock().unwrap();
                let fix_msg = translate::execution_report_to_fix(
                    report, &map, sym_str, tick_inv, lot_inv, target, sender, *seq, exec_id,
                );
                drop(map);

                if !fix_msg.is_empty() {
                    if let Err(e) = fix_writer.write_all(&fix_msg) {
                        debug!(error = %e, "FIX write error");
                        done.store(true, Ordering::Relaxed);
                        return;
                    }
                    let _ = fix_writer.flush();
                    *seq += 1;
                    exec_id += 1;
                }
            }
            ResponseKind::BatchEnd | ResponseKind::Heartbeat | ResponseKind::ServerReady => {
                // Ignore session-level Melin messages.
            }
            ResponseKind::ServerBusy => {
                warn!("Melin server busy — pipeline full");
            }
            ResponseKind::EngineError => {
                error!("Melin engine error received");
            }
            _ => {}
        }
    }
}

fn send_logout(
    stream: &mut TcpStream,
    config: &GatewayConfig,
    target: &str,
    seq: u64,
    text: &str,
) -> io::Result<()> {
    let msg = FixMessageBuilder::new(tags::MSG_LOGOUT)
        .str_tag(tags::TEXT, text)
        .build(&config.target_comp_id, target, seq);
    stream.write_all(&msg)?;
    stream.flush()
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

/// Authenticate with melin-server using Ed25519 challenge-response.
/// Returns (reader, writer) for the authenticated connection.
///
/// Same handshake as `melin-client::Client::connect()`, but returns
/// the raw reader/writer instead of a `Client` so we can use them
/// from separate threads.
fn authenticate_melin(
    stream: TcpStream,
    signing_key: &SigningKey,
) -> Result<
    (
        BlockingFrameReader<TcpStream>,
        Arc<std::sync::Mutex<BlockingFrameWriter<TcpStream>>>,
    ),
    Box<dyn std::error::Error>,
> {
    let reader_stream = stream.try_clone()?;
    // Set read timeout for the outbound thread's blocking reads.
    reader_stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut reader = BlockingFrameReader::new(reader_stream);
    let mut writer = BlockingFrameWriter::new(stream);

    // Read Challenge.
    let frame = reader
        .read_frame()?
        .ok_or("server closed before Challenge")?;
    let challenge = codec::decode_response(frame)?;
    let nonce = match challenge {
        ResponseKind::Challenge { nonce } => nonce,
        _ => return Err("expected Challenge from server".into()),
    };

    // Sign and send ChallengeResponse.
    let signature = signing_key.sign(&nonce);
    let request = melin_protocol::message::Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: signing_key.verifying_key().to_bytes(),
    };
    let mut buf = [0u8; 136];
    let written = codec::encode_request(&request, 0, &mut buf)?;
    writer.write_frame(&buf[4..written])?;
    writer.flush()?;

    // Read auth result.
    let frame = reader
        .read_frame()?
        .ok_or("server closed before auth result")?;
    match codec::decode_response(frame)? {
        ResponseKind::ServerReady => {}
        ResponseKind::AuthFailed => return Err("authentication failed".into()),
        other => return Err(format!("unexpected auth response: {other:?}").into()),
    }

    Ok((reader, Arc::new(std::sync::Mutex::new(writer))))
}
