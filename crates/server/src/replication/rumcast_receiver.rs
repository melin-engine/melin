//! Rumcast replication receiver (replica side).
//!
//! Mirrors `tcp_receiver.rs` but uses `SenderLoop` + `ReceiverLoop`
//! over a [`SharedUdp`] socket. Single-session (one peer = the primary)
//! so we don't need the muxed primitives. The main thread drives both
//! rumcast ticks inline; pipeline threads (journal, matching, drain)
//! spawn only after handshake completes and tear down on every
//! disconnect.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ed25519_dalek::Signer;
use tracing::{debug, error, info, warn};

use melin_rumcast::pub_log::{ClaimError, PublicationConfig, PublicationLog};
use melin_rumcast::receiver::{ReceiverConfig, ReceiverLoop};
use melin_rumcast::sender::{SenderConfig, SenderLoop};
use melin_rumcast::shared_udp::{SharedUdp, SharedUdpRecv, SharedUdpSend};
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::transport::KernelUdp;
use melin_rumcast::wire::{FrameView, data_flags};

use super::protocol::{
    Ack, Handshake, MAX_DATA_FRAME, PrimaryMessage, decode_auth_result, decode_challenge,
    decode_primary_message, encode_ack, encode_challenge_response, encode_handshake,
    try_decode_input_batch,
};
use super::{PendingAckQueue, shutdown_pipeline, sleep_checking_flags};

// ---------------------------------------------------------------------------
// Wire-format constants — must match `replication/rumcast_sender.rs`.
// ---------------------------------------------------------------------------

const REPL_PRIMARY_STREAM: u32 = 11; // primary → replica
const REPL_REPLICA_STREAM: u32 = 12; // replica → primary
const REPL_TERM_LENGTH: u32 = 4 * 1024 * 1024;
const REPL_MTU: u32 = 1408;
const REPL_INITIAL_TERM_ID: u32 = 1;
/// Per-replica receiver_id stamped into Status Messages we emit toward
/// the primary. Different from the primary's receiver_id; just needs
/// to be unique across the deployment so primary-side flow control can
/// disambiguate slow replicas.
const REPL_REPLICA_RECEIVER_ID: u64 = 102;

const MAX_FRAGMENT_PAYLOAD: usize = (REPL_MTU as usize) - 32;

/// Upper bound on a single auth or handshake step. Far longer than any
/// realistic LAN RTT — bails out if the primary isn't responding rather
/// than hanging the receiver.
const HANDSHAKE_DEADLINE: Duration = Duration::from_secs(10);

/// Cap on a single snapshot transfer. A large snapshot legitimately
/// takes seconds; bigger than that on a healthy LAN means the primary
/// is broken and we should reconnect.
const SNAPSHOT_DEADLINE: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Run the rumcast replication receiver. Connects to the primary's
/// rumcast replication endpoint, authenticates via Ed25519 challenge-
/// response, performs catch-up / snapshot recovery, and runs the
/// streaming receive loop. Builds the replica's local pipeline
/// (journal + matching + drain) to apply incoming events and ack
/// durable batches back to the primary.
///
/// Blocks until the connection drops or shutdown is signaled.
/// `Some` return value = promotion triggered with the fully-replayed
/// `App` and positioned `JournalWriter`; `None` = clean shutdown.
#[allow(clippy::too_many_arguments)]
pub fn run_receiver_rumcast(
    primary_addr: SocketAddr,
    bind_addr: SocketAddr,
    journal_path: &Path,
    signing_key: &ed25519_dalek::SigningKey,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_secs: u64,
    snapshot_path: PathBuf,
    cores: crate::server::PipelineCores,
    async_ack: bool,
    busy_spin: bool,
    rotation: Option<(u64, Arc<AtomicBool>)>,
    // SEC-03: must equal the primary's --max-orders-per-account.
    max_orders_per_account: u32,
    // SEC-04: must equal the primary's --max-orders-per-second / --max-orders-burst.
    max_orders_per_second: u32,
    max_orders_burst: u32,
) -> super::ReceiverResult {
    use crate::App;
    use crate::JournalWriter;
    use melin_transport_core::JournaledApp;

    // ---- Recover local state from journal (if any) ----
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        if journal_path.exists() {
            let engine = if snapshot_path.exists() {
                info!("recovering replica from snapshot + journal");
                JournaledApp::<App>::recover_from_snapshot(&snapshot_path, journal_path)?
            } else {
                JournaledApp::<App>::recover(crate::server::empty_app(), journal_path)?
            };
            let next = engine.next_sequence();
            let last = next.saturating_sub(1);
            let hash = engine.chain_hash().unwrap_or([0u8; 32]);
            let (mut e, w) = engine.into_parts();
            crate::server::apply_max_orders(
                &mut e,
                max_orders_per_account,
                max_orders_per_second,
                max_orders_burst,
            );
            (Some(e), Some(w), last, hash)
        } else {
            (None, None, 0u64, [0u8; 32])
        };

    let mut backoff = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    // ---- Reusable state — survives across reconnections ----
    let mut accum_end_sequence: u64 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered while disconnected");
            return match (exchange, journal_writer) {
                (Some(e), Some(w)) => Ok(Some((e, w))),
                _ => Err("promotion requested but no local state available".into()),
            };
        }

        info!(
            primary = %primary_addr,
            bind = %bind_addr,
            "connecting to primary as rumcast replica"
        );

        // ---- Bind a fresh rumcast endpoint per attempt ----
        //
        // SharedUdp gives us one bound port and two halves — the
        // SenderLoop owns the send half (primary-bound traffic), the
        // ReceiverLoop owns the recv half (primary-sourced traffic).
        // A new bind on each reconnect drops any half-open state from
        // a prior session; the primary will re-allocate its session
        // on first contact via the new source addr.
        let shared = match SharedUdp::bind(bind_addr) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, backoff_secs = backoff.as_secs(), "shared bind failed — retrying");
                sleep_checking_flags(backoff, shutdown, promote);
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        let (send_half, recv_half) = shared.split();
        let session_id = generate_session_id();

        let session_state = SessionState::new(session_id, primary_addr, send_half, recv_half);

        // ---- Auth + handshake (drives ticks inline) ----
        let outcome = match auth_and_handshake(
            &session_state,
            signing_key,
            last_sequence,
            chain_hash,
            shutdown,
            promote,
            busy_spin,
        ) {
            Ok(Some(StreamStartInfo {
                primary_genesis,
                start_sequence,
                snapshot_bytes,
            })) => {
                // ---- Persist + load snapshot if one was transferred ----
                if let Some(snap_bytes) = snapshot_bytes {
                    // Drop any prior journal/snapshot — we're starting
                    // from this snapshot's sequence.
                    let _ = std::fs::remove_file(journal_path);
                    let _ = std::fs::remove_file(&snapshot_path);
                    write_snapshot_to_disk(&snapshot_path, &snap_bytes)?;

                    let (snap_exchange, snap_seq, snap_hash) =
                        melin_transport_core::snapshot::load::<App>(&snapshot_path)?;
                    exchange = Some(snap_exchange);
                    let writer =
                        JournalWriter::create_continuing(journal_path, snap_seq + 1, snap_hash)?;
                    journal_writer = Some(writer);
                    last_sequence = snap_seq;
                    chain_hash = snap_hash;
                    info!(start_sequence, "rumcast replica: snapshot loaded");
                }

                // ---- Create journal for fresh replica (no snapshot, no prior journal) ----
                if journal_writer.is_none() {
                    let writer = create_fresh_replica_journal(journal_path, &primary_genesis)?;
                    let mut fresh = crate::server::empty_app();
                    crate::server::apply_max_orders(
                        &mut fresh,
                        max_orders_per_account,
                        max_orders_per_second,
                        max_orders_burst,
                    );
                    exchange = Some(fresh);
                    journal_writer = Some(writer);
                }
                Some((primary_genesis, start_sequence))
            }
            Ok(None) => {
                // Clean shutdown / promote during handshake.
                if shutdown.load(Ordering::Relaxed) {
                    return Ok(None);
                }
                if promote.load(Ordering::Acquire) {
                    return match (exchange, journal_writer) {
                        (Some(e), Some(w)) => Ok(Some((e, w))),
                        _ => Err("promotion requested but no local state available".into()),
                    };
                }
                None
            }
            Err(e) => {
                warn!(error = %e, backoff_secs = backoff.as_secs(), "handshake failed — retrying");
                sleep_checking_flags(backoff, shutdown, promote);
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        let Some((_genesis, _start_sequence)) = outcome else {
            continue;
        };

        let cur_exchange = exchange.take().expect("exchange initialized");
        let cur_writer = journal_writer.take().expect("journal_writer initialized");

        // ---- Build pipeline + spawn pipeline threads ----
        let shadow_exchange = <App as melin_app::Application>::clone_via_snapshot(&cur_exchange)?;

        let enable_shadow = snapshot_interval_secs > 0;
        let pipeline = melin_transport_core::pipeline::build_replica_pipeline(
            cur_exchange,
            cur_writer,
            4096,
            busy_spin,
            enable_shadow,
        );
        let mut input_producer = pipeline.input_producer;
        let mut journal_stage = pipeline.journal_stage;
        if let Some((max_bytes, ref flag)) = rotation {
            journal_stage.set_rotation(max_bytes, Some(Arc::clone(flag)));
        }
        let matching_stage = pipeline.matching_stage;
        let drain_consumer = pipeline.drain_consumer;
        let journal_cursor = pipeline.journal_cursor;
        let shadow_consumer = pipeline.shadow_consumer;
        let chain_hash_lock = pipeline.chain_hash_lock;

        let pipeline_shutdown = Arc::new(AtomicBool::new(false));

        let ps = Arc::clone(&pipeline_shutdown);
        let journal_core = cores.journal;
        let journal_handle = std::thread::Builder::new()
            .name("journal".into())
            .spawn(move || {
                crate::affinity::pin_thread("journal", journal_core);
                journal_stage.run(&ps)
            })
            .expect("spawn journal thread");

        let ps = Arc::clone(&pipeline_shutdown);
        let matching_core = cores.matching;
        let matching_handle = std::thread::Builder::new()
            .name("matching".into())
            .spawn(move || {
                crate::affinity::pin_thread("matching", matching_core);
                matching_stage.run(&ps)
            })
            .expect("spawn matching thread");

        let ps = Arc::clone(&pipeline_shutdown);
        let drain_core = cores.response;
        let drain_handle = std::thread::Builder::new()
            .name("drain".into())
            .spawn(move || {
                crate::affinity::pin_thread("drain", drain_core);
                let mut consumer = drain_consumer;
                let mut batch = vec![crate::OutputSlot::default(); 256];
                loop {
                    if ps.load(Ordering::Relaxed) {
                        return;
                    }
                    let count = consumer.consume_batch(&mut batch, 256);
                    if count == 0 {
                        if busy_spin {
                            std::hint::spin_loop();
                        } else {
                            std::thread::yield_now();
                        }
                    }
                }
            })
            .expect("spawn drain thread");

        let shadow_handle = if let Some(shadow_cons) = shadow_consumer {
            let snap_path = snapshot_path.clone();
            let chain_lock = chain_hash_lock.expect("chain hash lock with shadow");
            let ps = Arc::clone(&pipeline_shutdown);
            let shadow_core = cores.shadow;
            Some(
                std::thread::Builder::new()
                    .name("replica-shadow".into())
                    .spawn(move || {
                        crate::affinity::pin_thread("replica-shadow", shadow_core);
                        crate::shadow::run(
                            shadow_cons,
                            shadow_exchange,
                            snap_path,
                            Duration::from_secs(snapshot_interval_secs),
                            chain_lock,
                            &ps,
                            false,
                        );
                    })
                    .expect("spawn shadow thread"),
            )
        } else {
            None
        };

        // ---- Inner streaming loop ----
        let mut pending_acks = PendingAckQueue::new();
        let mut received_data = false;

        let exit_reason = streaming_loop(
            &session_state,
            &mut input_producer,
            &journal_cursor,
            &mut pending_acks,
            &mut received_data,
            &mut accum_end_sequence,
            shutdown,
            promote,
            async_ack,
            busy_spin,
        );

        // ---- Common teardown ----
        if let Some(seq) = pending_acks.pop_all_blocking(&journal_cursor) {
            let _ = session_state.send_ack(seq);
        }

        let pipeline_state = shutdown_pipeline(
            &pipeline_shutdown,
            journal_handle,
            matching_handle,
            drain_handle,
            shadow_handle,
        );

        match exit_reason {
            SessionExit::Shutdown => return Ok(None),
            SessionExit::Promote => {
                return match pipeline_state {
                    Some((e, w)) => Ok(Some((e, w))),
                    None => Err("pipeline failed during promotion".into()),
                };
            }
            SessionExit::Fatal(e) => return Err(e),
            SessionExit::Disconnected => {
                match pipeline_state {
                    Some((e, w)) => {
                        last_sequence = w.next_sequence().saturating_sub(1);
                        chain_hash = w.chain_hash().unwrap_or([0u8; 32]);
                        exchange = Some(e);
                        journal_writer = Some(w);
                    }
                    None => {
                        error!("pipeline thread panicked during disconnect recovery");
                        if journal_path.exists() {
                            let engine = JournaledApp::<App>::recover(
                                crate::server::empty_app(),
                                journal_path,
                            )?;
                            last_sequence = engine.next_sequence().saturating_sub(1);
                            chain_hash = engine.chain_hash().unwrap_or([0u8; 32]);
                            let (mut e, w) = engine.into_parts();
                            crate::server::apply_max_orders(
                                &mut e,
                                max_orders_per_account,
                                max_orders_per_second,
                                max_orders_burst,
                            );
                            exchange = Some(e);
                            journal_writer = Some(w);
                        } else {
                            return Err("pipeline panicked and no journal to recover from".into());
                        }
                    }
                }

                if received_data {
                    backoff = Duration::from_secs(1);
                }

                warn!(
                    last_sequence,
                    backoff_secs = backoff.as_secs(),
                    "reconnecting to primary"
                );
                sleep_checking_flags(backoff, shutdown, promote);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-attempt session state
// ---------------------------------------------------------------------------

/// Owns the rumcast endpoints for one connection attempt. Wraps the
/// [`SenderLoop`] / [`ReceiverLoop`] state so the auth + streaming
/// helpers can borrow them mutably from the same call site.
struct SessionState {
    session_id: u32,
    sender: std::cell::RefCell<SenderLoop<SharedUdpSend<KernelUdp>>>,
    receiver: std::cell::RefCell<ReceiverLoop<SharedUdpRecv<KernelUdp>>>,
    pub_log: Arc<PublicationLog>,
    sub_log: Arc<SubscriptionLog>,
    /// Frames that arrived in the same poll pass as the target of a
    /// `wait_for_message` call, AFTER the target was found. Because
    /// `sub_log.poll()` advances the cursor for every frame it visits
    /// — including frames the callback skips — dropping them would
    /// lose events that must reach the pipeline (e.g. the initial
    /// catch-up data that arrives right after `StreamStart`).
    /// `streaming_loop` drains this queue before each `sub_log.poll`.
    stashed: std::cell::RefCell<VecDeque<Vec<u8>>>,
    /// Accumulation buffer for multi-fragment messages. `sub_log.poll()`
    /// delivers raw rumcast fragments (BEGIN/middle/END) individually;
    /// large replication frames (e.g. catch-up InputBatch > MTU) are
    /// split across multiple fragments and must be reassembled before
    /// decoding. Persists across poll calls because a BEGIN_FRAGMENT may
    /// arrive in one tick and the END_FRAGMENT in the next.
    reassembly_buf: std::cell::RefCell<Option<Vec<u8>>>,
}

impl SessionState {
    fn new(
        session_id: u32,
        primary_addr: SocketAddr,
        send_half: SharedUdpSend<KernelUdp>,
        recv_half: SharedUdpRecv<KernelUdp>,
    ) -> Self {
        let pub_log = Arc::new(
            PublicationLog::new(PublicationConfig {
                session_id,
                stream_id: REPL_REPLICA_STREAM,
                initial_term_id: REPL_INITIAL_TERM_ID,
                term_length: REPL_TERM_LENGTH,
                mtu: REPL_MTU,
            })
            .expect("publication config"),
        );
        // Single-publisher trust ourselves; the primary will gate via
        // its own SMs. Removes a wait-for-first-SM stall during auth.
        pub_log.set_publisher_limit(u64::MAX);

        let sub_log = Arc::new(
            SubscriptionLog::new(SubscriptionConfig {
                session_id,
                stream_id: REPL_PRIMARY_STREAM,
                initial_term_id: REPL_INITIAL_TERM_ID,
                term_length: REPL_TERM_LENGTH,
            })
            .expect("subscription config"),
        );

        let mut sender_config = SenderConfig::defaults(primary_addr);
        sender_config.setup_interval = Duration::from_millis(100);
        sender_config.heartbeat_interval = Duration::from_millis(50);
        sender_config.max_drain_per_tick = 1024 * 1024;
        let sender = SenderLoop::new(Arc::clone(&pub_log), send_half, sender_config);

        let mut receiver_config = ReceiverConfig::defaults(primary_addr, REPL_REPLICA_RECEIVER_ID);
        receiver_config.sm_interval = Duration::from_millis(2);
        receiver_config.nak_backoff_min = Duration::from_micros(50);
        receiver_config.nak_backoff_jitter = Duration::from_micros(50);
        receiver_config.max_recv_per_tick = 1024;
        let receiver = ReceiverLoop::new(Arc::clone(&sub_log), recv_half, receiver_config);

        Self {
            session_id,
            sender: std::cell::RefCell::new(sender),
            receiver: std::cell::RefCell::new(receiver),
            pub_log,
            sub_log,
            stashed: std::cell::RefCell::new(VecDeque::new()),
            reassembly_buf: std::cell::RefCell::new(None),
        }
    }

    /// Drive both rumcast ticks once. Cheap when nothing is pending —
    /// this gets called from every busy-wait loop in the receiver.
    fn tick(&self) {
        self.sender.borrow_mut().tick();
        self.receiver.borrow_mut().tick();
    }

    /// Force the Setup frame out immediately; the primary's
    /// MuxedReceiver allocates a session when it sees the Setup, which
    /// triggers the Challenge — without this the replica would wait
    /// `setup_interval` (~100ms) before the first Setup goes out.
    fn kick(&self) {
        self.sender.borrow_mut().send_setup_now();
        self.receiver.borrow_mut().send_sm_now();
    }

    /// Spin-publish a complete (possibly multi-fragment) message via
    /// the publication log. Returns `Err` only on shutdown. Mirrors
    /// the sender-side `spin_publish`.
    fn publish(&self, payload: &[u8], shutdown: &AtomicBool) -> io::Result<()> {
        if payload.is_empty() {
            return Err(io::Error::other("empty payload"));
        }
        if payload.len() <= MAX_FRAGMENT_PAYLOAD {
            return self.publish_one(payload, data_flags::UNFRAGMENTED, shutdown);
        }
        let mut offset = 0;
        let total = payload.len();
        while offset < total {
            let end = (offset + MAX_FRAGMENT_PAYLOAD).min(total);
            let flags = if offset == 0 {
                data_flags::BEGIN_FRAGMENT
            } else if end == total {
                data_flags::END_FRAGMENT
            } else {
                0
            };
            self.publish_one(&payload[offset..end], flags, shutdown)?;
            offset = end;
        }
        Ok(())
    }

    fn publish_one(&self, fragment: &[u8], flags: u8, shutdown: &AtomicBool) -> io::Result<()> {
        loop {
            if shutdown.load(Ordering::Relaxed) {
                return Err(io::Error::other("shutdown"));
            }
            match self.pub_log.try_claim(fragment.len() as u32) {
                Ok(mut claim) => {
                    claim.payload_mut().copy_from_slice(fragment);
                    claim.publish(flags);
                    self.tick();
                    return Ok(());
                }
                Err(ClaimError::PayloadTooLarge { .. }) => {
                    return Err(io::Error::other(format!(
                        "fragment {} bytes exceeds MTU",
                        fragment.len()
                    )));
                }
                Err(ClaimError::BackPressure { .. }) => {
                    self.tick();
                    std::hint::spin_loop();
                }
            }
        }
    }

    /// Send an Ack frame to the primary. Used by the streaming loop on
    /// every durable batch. Wire format: `protocol::encode_ack` already
    /// emits the `[len:u32][type][payload]` framing — we publish the
    /// whole encoded buffer so the primary's `strip_length_prefix`
    /// finds what it expects.
    fn send_ack(&self, acked_sequence: u64) -> io::Result<()> {
        let mut buf = Vec::with_capacity(16);
        encode_ack(&Ack { acked_sequence }, &mut buf);
        let dummy_shutdown = AtomicBool::new(false);
        self.publish(&buf, &dummy_shutdown)
    }

    /// Same as `send_ack` but observes the supplied shutdown flag —
    /// the streaming loop uses this so a shutdown during a backpressure
    /// spin-wait short-circuits.
    fn send_ack_with(&self, acked_sequence: u64, shutdown: &AtomicBool) -> io::Result<()> {
        let mut buf = Vec::with_capacity(16);
        encode_ack(&Ack { acked_sequence }, &mut buf);
        self.publish(&buf, shutdown)
    }
}

// ---------------------------------------------------------------------------
// Auth + handshake helpers
// ---------------------------------------------------------------------------

/// What the primary returned in `StreamStart`, plus optional snapshot
/// bytes when the snapshot transfer path ran. The caller writes the
/// snapshot to disk because it owns the configured `snapshot_path`.
struct StreamStartInfo {
    primary_genesis: Vec<u8>,
    start_sequence: u64,
    /// Some(bytes) when a snapshot was received and verified;
    /// None when the journal-catch-up path was taken.
    snapshot_bytes: Option<Vec<u8>>,
}

/// Run the four-message Ed25519 challenge/response, then the
/// replication-protocol handshake (Handshake → StreamStart /
/// NeedSnapshot). Returns `Ok(Some)` with the StreamStart info on
/// success, `Ok(None)` if shutdown / promote interrupted, `Err` for
/// protocol or transport errors (caller backoff-retries).
fn auth_and_handshake(
    session: &SessionState,
    signing_key: &ed25519_dalek::SigningKey,
    last_sequence: u64,
    chain_hash: [u8; 32],
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    busy_spin: bool,
) -> io::Result<Option<StreamStartInfo>> {
    // Force the Setup out so the primary allocates our session ASAP.
    session.kick();

    // ---- 1. Receive Challenge ----
    let challenge_payload = match wait_for_message(
        session,
        |inner| decode_challenge(inner).is_ok(),
        HANDSHAKE_DEADLINE,
        shutdown,
        promote,
        busy_spin,
    )? {
        Some(p) => p,
        None => return Ok(None),
    };
    let nonce = decode_challenge(&challenge_payload)
        .map_err(|e| io::Error::other(format!("decode challenge: {e}")))?;
    debug!(
        session_id = session.session_id,
        "rumcast replica: challenge received"
    );

    // ---- 2. Sign and send ChallengeResponse ----
    let signature = signing_key.sign(&nonce);
    let pubkey = signing_key.verifying_key();
    let mut buf = Vec::with_capacity(128);
    encode_challenge_response(&signature.to_bytes(), pubkey.as_bytes(), &mut buf);
    session.publish(&buf, shutdown)?;

    // ---- 3. Receive AuthOk / AuthFailed ----
    let auth_payload = match wait_for_message(
        session,
        |inner| decode_auth_result(inner).is_ok(),
        HANDSHAKE_DEADLINE,
        shutdown,
        promote,
        busy_spin,
    )? {
        Some(p) => p,
        None => return Ok(None),
    };
    match decode_auth_result(&auth_payload) {
        Ok(true) => info!("rumcast replica: authenticated"),
        Ok(false) => {
            return Err(io::Error::other(
                "primary rejected replication key (AuthFailed)",
            ));
        }
        Err(e) => return Err(io::Error::other(format!("decode auth result: {e}"))),
    }

    // ---- 4. Send Handshake ----
    let handshake = Handshake {
        last_sequence,
        chain_hash,
    };
    buf.clear();
    encode_handshake(&handshake, &mut buf);
    session.publish(&buf, shutdown)?;

    // ---- 5. Receive StreamStart / NeedSnapshot ----
    let response_payload = match wait_for_message(
        session,
        |inner| decode_primary_message(inner).is_ok(),
        HANDSHAKE_DEADLINE,
        shutdown,
        promote,
        busy_spin,
    )? {
        Some(p) => p,
        None => return Ok(None),
    };
    match decode_primary_message(&response_payload)
        .map_err(|e| io::Error::other(format!("decode primary message: {e}")))?
    {
        PrimaryMessage::StreamStart {
            start_sequence,
            genesis_entry,
        } => Ok(Some(StreamStartInfo {
            primary_genesis: genesis_entry,
            start_sequence,
            snapshot_bytes: None,
        })),
        PrimaryMessage::NeedSnapshot => {
            // Snapshot path — handled by recv_snapshot below.
            recv_snapshot(session, shutdown, promote, busy_spin)
        }
        PrimaryMessage::HashMismatch => Err(io::Error::other(
            "chain hash mismatch — replica has divergent history",
        )),
        other => Err(io::Error::other(format!("unexpected response: {other:?}"))),
    }
}

/// Receive a complete snapshot transfer, write it to a tmp file, and
/// return the StreamStart that follows. Returns `Ok(None)` on shutdown
/// / promote.
fn recv_snapshot(
    session: &SessionState,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    busy_spin: bool,
) -> io::Result<Option<StreamStartInfo>> {
    // ---- Receive SnapshotBegin ----
    let begin_payload = match wait_for_message(
        session,
        |inner| {
            matches!(
                decode_primary_message(inner),
                Ok(PrimaryMessage::SnapshotBegin { .. })
            )
        },
        SNAPSHOT_DEADLINE,
        shutdown,
        promote,
        busy_spin,
    )? {
        Some(p) => p,
        None => return Ok(None),
    };
    let (snap_len, _snap_sequence, _snap_chain_hash) = match decode_primary_message(&begin_payload)?
    {
        PrimaryMessage::SnapshotBegin {
            snapshot_len,
            snap_sequence,
            snap_chain_hash,
        } => (snapshot_len, snap_sequence, snap_chain_hash),
        other => {
            return Err(io::Error::other(format!(
                "expected SnapshotBegin, got {other:?}"
            )));
        }
    };
    info!(snap_len, "rumcast replica: receiving snapshot");

    // Snapshot is buffered in memory; the deadline + 60s cap prevents
    // unbounded growth. For very large snapshots we'd want streaming
    // to a tmp file, but the existing TCP path also reads whole files
    // into memory in `tcp_sender.rs::handle_replica_connection`, so
    // we match its pattern.
    let mut snap_buf: Vec<u8> = Vec::with_capacity(snap_len as usize);
    let deadline = Instant::now() + SNAPSHOT_DEADLINE;
    loop {
        if Instant::now() > deadline {
            return Err(io::Error::other("snapshot transfer timed out"));
        }
        if shutdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            return Ok(None);
        }
        let mut done = false;
        let mut error_msg: Option<String> = None;
        session.tick();
        poll_reassembled(
            &session.sub_log,
            &session.reassembly_buf,
            MAX_DATA_FRAME as u32,
            |inner| {
                if done || error_msg.is_some() {
                    return;
                }
                match decode_primary_message(inner) {
                    Ok(PrimaryMessage::SnapshotChunk(data)) => {
                        snap_buf.extend_from_slice(&data);
                    }
                    Ok(PrimaryMessage::SnapshotEnd { crc32c: expected }) => {
                        let actual = crc32c::crc32c(&snap_buf);
                        if actual != expected {
                            error_msg = Some(format!(
                                "snapshot CRC mismatch: expected {expected:#x}, got {actual:#x}"
                            ));
                        } else if snap_buf.len() as u64 != snap_len {
                            error_msg = Some(format!(
                                "snapshot length mismatch: expected {snap_len}, got {}",
                                snap_buf.len()
                            ));
                        } else {
                            done = true;
                        }
                    }
                    Ok(other) => {
                        error_msg = Some(format!(
                            "expected SnapshotChunk/End during transfer, got {other:?}"
                        ));
                    }
                    Err(e) => {
                        error_msg = Some(format!("decode error during snapshot: {e}"));
                    }
                }
            },
        );
        if let Some(msg) = error_msg {
            return Err(io::Error::other(msg));
        }
        if done {
            break;
        }
        if busy_spin {
            std::hint::spin_loop();
        } else {
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    // ---- Receive StreamStart ----
    let ss_payload = match wait_for_message(
        session,
        |inner| {
            matches!(
                decode_primary_message(inner),
                Ok(PrimaryMessage::StreamStart { .. })
            )
        },
        HANDSHAKE_DEADLINE,
        shutdown,
        promote,
        busy_spin,
    )? {
        Some(p) => p,
        None => return Ok(None),
    };
    match decode_primary_message(&ss_payload)? {
        PrimaryMessage::StreamStart {
            start_sequence,
            genesis_entry,
        } => Ok(Some(StreamStartInfo {
            primary_genesis: genesis_entry,
            start_sequence,
            snapshot_bytes: Some(snap_buf),
        })),
        other => Err(io::Error::other(format!(
            "expected StreamStart after snapshot, got {other:?}"
        ))),
    }
}

/// Persist a received snapshot to disk via the same atomic-rename
/// pattern the TCP receiver uses (`tcp_receiver.rs::run_receiver`).
fn write_snapshot_to_disk(snapshot_path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp_path = snapshot_path.with_extension("snapshot.tmp");
    {
        let mut tmp_file = std::fs::File::create(&tmp_path)?;
        tmp_file.write_all(bytes)?;
        tmp_file.sync_all()?;
    }
    std::fs::rename(&tmp_path, snapshot_path)?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Streaming loop
// ---------------------------------------------------------------------------

enum SessionExit {
    Shutdown,
    Promote,
    Disconnected,
    Fatal(Box<dyn std::error::Error>),
}

#[allow(clippy::too_many_arguments)]
fn streaming_loop(
    session: &SessionState,
    input_producer: &mut melin_disruptor::ring::Producer<crate::InputSlot>,
    journal_cursor: &melin_disruptor::padding::Sequence,
    pending_acks: &mut PendingAckQueue,
    received_data: &mut bool,
    accum_end_sequence: &mut u64,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    async_ack: bool,
    busy_spin: bool,
) -> SessionExit {
    let mut last_publisher_seen = Instant::now();
    // Keepalive: periodically re-send the last acked sequence so the
    // primary's ack-timeout can distinguish a healthy-but-idle replica
    // from a dead one. Without this, a quiescent stream produces no acks
    // and the primary would incorrectly evict the slot.
    let mut last_sent_ack_seq: u64 = 0;
    let mut last_keepalive = Instant::now();
    const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return SessionExit::Shutdown;
        }
        if promote.load(Ordering::Acquire) {
            return SessionExit::Promote;
        }

        // ---- Drive ticks ----
        session.tick();

        // ---- Liveness probe: track last data from primary ----
        if let Some(t) = session.receiver.borrow().last_publisher_seen_at() {
            last_publisher_seen = t;
        }
        if last_publisher_seen.elapsed() > Duration::from_secs(30) {
            warn!("no traffic from primary for 30s — declaring disconnect");
            return SessionExit::Disconnected;
        }

        // ---- Drain inbound InputBatch / Heartbeat / control ----
        //
        // Two sources: (1) frames stashed by wait_for_message when they
        // arrived in the same poll pass as the handshake target, and
        // (2) freshly polled frames. Both are decoded identically.
        // Processing stashed frames first ensures catch-up data that
        // arrived alongside StreamStart isn't dropped.
        let mut fatal: Option<Box<dyn std::error::Error>> = None;
        let mut burst_any_published = false;
        let mut burst_last_target: u64 = 0;

        // Inline frame handler shared by the stash-drain and poll paths.
        macro_rules! handle_inner {
            ($inner:expr) => {{
                if let Ok(slots) = try_decode_input_batch($inner) {
                    *received_data = true;
                    for slot in slots {
                        let primary_seq = slot.sequence;
                        burst_last_target = input_producer.publish(slot);
                        *accum_end_sequence = primary_seq;
                        burst_any_published = true;
                    }
                } else {
                    match decode_primary_message($inner) {
                        Ok(PrimaryMessage::Heartbeat { sequence }) => {
                            debug!(sequence, "rumcast replica: heartbeat from primary");
                        }
                        Ok(PrimaryMessage::NeedSnapshot) => {
                            fatal = Some(
                                "primary requested snapshot mid-stream; reconnect required"
                                    .into(),
                            );
                        }
                        Ok(PrimaryMessage::HashMismatch) => {
                            fatal = Some("chain hash mismatch from primary".into());
                        }
                        Ok(_) => {
                            debug!("unexpected message during streaming");
                        }
                        Err(e) => {
                            warn!(error = %e, "failed to decode primary message");
                        }
                    }
                }
            }};
        }

        // Drain stashed frames before polling for new ones.
        {
            let mut stash = session.stashed.borrow_mut();
            while let Some(inner) = stash.pop_front() {
                if fatal.is_none() {
                    handle_inner!(&inner);
                }
            }
        }

        if fatal.is_none() {
            poll_reassembled(
                &session.sub_log,
                &session.reassembly_buf,
                MAX_DATA_FRAME as u32,
                |inner| {
                    if fatal.is_none() {
                        handle_inner!(inner);
                    }
                },
            );
        }

        if let Some(e) = fatal {
            return SessionExit::Fatal(e);
        }

        // ---- Record one ack covering the whole burst ----
        if burst_any_published && !pending_acks.is_full() {
            pending_acks.push(burst_last_target, *accum_end_sequence);
        }

        // ---- Flush acks ----
        let ready_seq = if async_ack {
            pending_acks.pop_all_async()
        } else {
            pending_acks.pop_ready(journal_cursor)
        };
        if let Some(seq) = ready_seq {
            if let Err(e) = session.send_ack_with(seq, shutdown) {
                if shutdown.load(Ordering::Relaxed) {
                    return SessionExit::Shutdown;
                }
                warn!(error = %e, "ack send failed");
                return SessionExit::Disconnected;
            }
            last_sent_ack_seq = seq;
            last_keepalive = Instant::now();
        }

        // ---- Backpressure: drain blocking acks if pending is full ----
        if pending_acks.is_full() {
            let seq = if async_ack {
                pending_acks
                    .pop_all_async()
                    .expect("non-empty queue after full check")
            } else {
                pending_acks.pop_oldest_blocking(journal_cursor)
            };
            if let Err(e) = session.send_ack_with(seq, shutdown) {
                if shutdown.load(Ordering::Relaxed) {
                    return SessionExit::Shutdown;
                }
                warn!(error = %e, "ack send failed during backpressure drain");
                return SessionExit::Disconnected;
            }
            last_sent_ack_seq = seq;
            last_keepalive = Instant::now();
        }

        // ---- Keepalive ack (idle liveness signal) ----
        //
        // Re-send the last acked sequence on a fixed interval so the
        // primary can distinguish a healthy-but-idle replica from a dead
        // one. Without this, no new orders → no new acks → the primary's
        // ack-timeout evicts a perfectly healthy replica.
        if last_sent_ack_seq > 0 && last_keepalive.elapsed() >= KEEPALIVE_INTERVAL {
            if let Err(e) = session.send_ack_with(last_sent_ack_seq, shutdown) {
                if shutdown.load(Ordering::Relaxed) {
                    return SessionExit::Shutdown;
                }
                warn!(error = %e, "keepalive ack send failed");
                return SessionExit::Disconnected;
            }
            last_keepalive = Instant::now();
        }

        // ---- Idle wait ----
        if busy_spin {
            std::hint::spin_loop();
        } else {
            std::thread::sleep(Duration::from_micros(50));
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Wait for a message that satisfies `predicate` (operating on the
/// payload after the 4-byte length prefix is stripped). Drives ticks
/// inline. Returns `Ok(Some(payload))` on match, `Ok(None)` on
/// shutdown / promote, `Err` on deadline.
fn wait_for_message(
    session: &SessionState,
    predicate: impl Fn(&[u8]) -> bool,
    deadline: Duration,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    busy_spin: bool,
) -> io::Result<Option<Vec<u8>>> {
    let until = Instant::now() + deadline;
    loop {
        if Instant::now() > until {
            return Err(io::Error::other("deadline expired waiting for primary"));
        }
        if shutdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            return Ok(None);
        }
        session.tick();
        let mut found: Option<Vec<u8>> = None;
        poll_reassembled(
            &session.sub_log,
            &session.reassembly_buf,
            MAX_DATA_FRAME as u32,
            |inner| {
                if found.is_none() && predicate(inner) {
                    found = Some(inner.to_vec());
                } else if found.is_some() {
                    // A frame arrived in the same poll pass as the
                    // target. sub_log.poll advances the cursor for
                    // every visited frame, so we must stash it rather
                    // than drop it — otherwise catch-up data that
                    // arrives immediately after StreamStart is lost.
                    session.stashed.borrow_mut().push_back(inner.to_vec());
                }
                // Note: a frame that arrives BEFORE the target and
                // doesn't satisfy the predicate is silently dropped.
                // Safe because rumcast preserves publication order from
                // a single publisher: every handshake-step predicate
                // (Challenge → AuthOk → StreamStart) is the next thing
                // the primary sends, so an earlier non-matching frame
                // can only be a stale message from a prior handshake
                // step we already consumed (and would re-decode here as
                // a no-op). If a future protocol change interleaves
                // unrelated control frames during handshake, this
                // branch must stash them too.
            },
        );
        if let Some(p) = found {
            return Ok(Some(p));
        }
        if busy_spin {
            std::hint::spin_loop();
        } else {
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

/// Poll the subscription log with multi-fragment reassembly.
///
/// `sub_log.poll()` delivers raw rumcast data frames — one per callback
/// invocation — including fragments of messages that span multiple MTU-
/// sized UDP datagrams. Large replication frames (e.g. a catch-up
/// `InputBatch` with 30+ events) exceed `MAX_FRAGMENT_PAYLOAD` and are
/// split by the sender's `spin_publish` into BEGIN_FRAGMENT / middle /
/// END_FRAGMENT pieces. This helper reassembles them transparently.
///
/// `on_complete` is called with the complete inner payload (4-byte
/// length prefix already stripped) for each fully reassembled message.
/// Fragments are accumulated in `reassembly_buf` across calls so a
/// BEGIN_FRAGMENT received in one tick and an END_FRAGMENT in the next
/// are correctly stitched together.
fn poll_reassembled<F>(
    sub_log: &SubscriptionLog,
    reassembly_buf: &std::cell::RefCell<Option<Vec<u8>>>,
    max_bytes: u32,
    mut on_complete: F,
) where
    F: FnMut(&[u8]),
{
    sub_log.poll(max_bytes, |view| {
        let FrameView::Data {
            header, payload, ..
        } = view
        else {
            return;
        };
        let flags = header.common.flags;
        let is_begin = flags & data_flags::BEGIN_FRAGMENT != 0;
        let is_end = flags & data_flags::END_FRAGMENT != 0;

        if is_begin && is_end {
            // Unfragmented: process directly.
            if let Some(inner) = strip_length_prefix(payload) {
                on_complete(inner);
            }
            return;
        }

        if is_begin {
            // First fragment: start a new reassembly buffer.
            *reassembly_buf.borrow_mut() = Some(payload.to_vec());
            return;
        }

        // Middle or final fragment: append to the in-progress buffer.
        let complete = {
            let mut guard = reassembly_buf.borrow_mut();
            if let Some(ref mut buf) = *guard {
                buf.extend_from_slice(payload);
                if is_end {
                    guard.take() // clears *guard = None, returns the Vec
                } else {
                    None
                }
            } else {
                // No BEGIN_FRAGMENT seen — orphaned fragment; discard.
                warn!("reassembly: orphaned fragment (no BEGIN_FRAGMENT), discarding");
                None
            }
        };

        if let Some(complete) = complete
            && let Some(inner) = strip_length_prefix(&complete)
        {
            on_complete(inner);
        }
    });
}

/// Strip the 4-byte little-endian length prefix that `protocol.rs`
/// encoders emit. Returns `None` if the prefix is malformed.
fn strip_length_prefix(payload: &[u8]) -> Option<&[u8]> {
    if payload.len() < 4 {
        return None;
    }
    let declared = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
    if declared == 0 || declared > MAX_DATA_FRAME {
        return None;
    }
    let inner = &payload[4..];
    if inner.len() != declared {
        return None;
    }
    Some(inner)
}

/// Pick a fresh 32-bit `session_id`. Each connection attempt picks a
/// new one — reusing across reconnect would land on a stale session
/// state on the primary side and produce confusing auth failures.
fn generate_session_id() -> u32 {
    let mut bytes = [0u8; 4];
    getrandom::fill(&mut bytes).expect("getrandom for session_id");
    u32::from_le_bytes(bytes)
}

/// Create a fresh replica journal seeded with the primary's genesis
/// entry. Same logic as `tcp_receiver.rs::run_receiver`'s "create
/// journal for fresh replica" block, factored out so the rumcast
/// receiver doesn't duplicate it inline.
fn create_fresh_replica_journal(
    journal_path: &Path,
    primary_genesis_entry: &[u8],
) -> io::Result<crate::JournalWriter> {
    use melin_journal::codec as journal_codec;
    use melin_journal::detect_sector_size;
    use std::fs::OpenOptions;
    use std::os::fd::AsFd;
    use std::os::unix::fs::FileExt;

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(journal_path)?;
    let sector_size = detect_sector_size(file.as_fd());
    let mut header = vec![0u8; sector_size];
    journal_codec::encode_file_header(&mut header, sector_size);
    file.write_all_at(&header, 0)?;
    file.write_all_at(primary_genesis_entry, sector_size as u64)?;
    file.sync_all()?;

    let genesis_chain_hash = {
        let entry_len = primary_genesis_entry.len();
        let hash = blake3::hash(&primary_genesis_entry[..entry_len - 4]);
        *hash.as_bytes()
    };

    let valid_end = sector_size as u64 + primary_genesis_entry.len() as u64;
    crate::JournalWriter::open_append(journal_path, 1, valid_end, Some(genesis_chain_hash), 0)
        .map_err(|e| io::Error::other(format!("open_append: {e}")))
}
