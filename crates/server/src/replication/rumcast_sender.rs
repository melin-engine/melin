//! Rumcast replication sender (primary side).
//!
//! Mirrors `tcp_sender.rs` but uses `MuxedReceiver` + `MuxedSender` over
//! kernel UDP for the data plane. Threading model is single-thread (DPDK-
//! style) — the rumcast muxers require single-producer access on each
//! per-session log, so all per-slot state machines run on one thread.
//!
//! # Auth
//!
//! Replication on rumcast keeps the same Ed25519 challenge-response that
//! the TCP path uses. No envelope wrapping: replication runs on a
//! separate UDP port operators are expected to firewall to internal /
//! VLAN-only, the same threat model the TCP path already accepts. The
//! rumcast handshake auth used for client-facing traffic in
//! `rumcast_transport.rs` is not reused here — see the Phase 4 design
//! note in `docs/roadmap.md` for context.
//!
//! # Wire format
//!
//! Every replication message is encoded by `replication::protocol` with
//! its standard `[len:u32][type:u8][payload]` framing and published as
//! one rumcast Data message. The receiver side strips the 4-byte length
//! prefix and decodes via `decode_replica_message` /
//! `decode_primary_message`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ed25519_dalek::Verifier;
use tracing::{debug, error, info, warn};

use melin_journal::replication::ReplicationConsumer;
use melin_protocol::auth::AuthorizedKeys;
use melin_rumcast::counters::Counters;
use melin_rumcast::muxed_receiver::{MuxedReceiver, MuxedReceiverConfig};
use melin_rumcast::muxed_sender::{MuxedSender, MuxedSenderConfig};
use melin_rumcast::pub_log::PublicationLog;
use melin_rumcast::transport::KernelUdp;
use melin_rumcast::wire::{FrameView, data_flags};

use super::catchup::{CatchUpResult, can_catch_up_from_journal, catch_up_from_journal_with};
use super::protocol::{
    MAX_DATA_FRAME, ReplicaMessage, decode_challenge_response, decode_replica_message,
    encode_auth_failed, encode_auth_ok, encode_challenge, encode_heartbeat, encode_need_snapshot,
    encode_snapshot_begin, encode_snapshot_chunk, encode_snapshot_end, encode_stream_start,
};
use super::{ReplicationMetrics, update_dual_replication_cursor};

// ---------------------------------------------------------------------------
// Wire-format constants for replication-over-rumcast
// ---------------------------------------------------------------------------

/// Stream IDs for the replication channels. Distinct from the
/// order-entry stream IDs (1, 2) used by `rumcast_transport.rs` so that
/// a misconfiguration that points clients at the replication port (or
/// vice-versa) silently drops their frames at the rumcast stream-id
/// filter rather than mixing traffic.
const REPL_PRIMARY_STREAM: u32 = 11; // primary → replica
const REPL_REPLICA_STREAM: u32 = 12; // replica → primary

/// Per-session term length for the replication mux. Sized for catch-up
/// bursts (the journal stage can publish a 768 KiB InputBatch worst case)
/// and BDP headroom on a 10 GbE LAN. Memory cost is `3 × term_length ×
/// max_sessions` per direction, but `max_sessions = 4` here (we cap
/// well below the muxer's default 1024) so worst-case is `4 × 4 MiB ×
/// 3 × 2 = 96 MiB` per replication endpoint — comfortable.
const REPL_TERM_LENGTH: u32 = 4 * 1024 * 1024;
/// Same conservative MTU as the order-entry path.
const REPL_MTU: u32 = 1408;
/// Both sides start at term_id = 1.
const REPL_INITIAL_TERM_ID: u32 = 1;
/// Server-side receiver_id stamped into every Status Message.
const REPL_PRIMARY_RECEIVER_ID: u64 = 101;
/// Cap on concurrent replica sessions. Higher than the actual slot
/// count (2) so an out-of-order reconnect from an old session_id
/// doesn't immediately starve a fresh one — they coexist briefly until
/// the stale Challenged entry times out.
const REPL_MAX_SESSIONS: u32 = 4;

/// Drop a session that hasn't completed the Ed25519 handshake within
/// this. Bounds memory pinned by a peer that opens a rumcast session
/// but never sends a valid `ChallengeResponse`.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Release a Live slot if no Ack has been received within this window.
/// Under TCP the OS signals a dead peer immediately; rumcast has no
/// equivalent, so we infer death from silence. The replica sends a
/// keepalive ack every ~1s even on an idle stream, so this window need
/// only exceed that interval by a comfortable margin. 3s gives a ×3
/// buffer while still freeing dead slots quickly enough for tests and
/// for replacement replicas to connect.
const LIVE_ACK_TIMEOUT: Duration = Duration::from_secs(3);

/// Minimum gap between auth-state sweeps. Mirrors the order-entry
/// session translator's amortization.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Snapshot chunk size for SnapshotChunk frames. Same value the TCP
/// path uses (`tcp_sender.rs::CHUNK_SIZE`) so observability and bench
/// behavior match across transports.
const SNAPSHOT_CHUNK_SIZE: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// Sender configuration
// ---------------------------------------------------------------------------

/// Owned state for the rumcast replication sender thread. Mirrors
/// `tcp_sender::Sender` field-for-field so the call site in
/// `rumcast_transport.rs` looks identical to the TCP wiring in
/// `server.rs`. The `bind_addr` here is a UDP bind for replication
/// traffic — distinct from the order-entry bind.
pub struct Sender {
    pub bind_addr: SocketAddr,
    pub repl_consumer_1: ReplicationConsumer,
    pub repl_consumer_2: ReplicationConsumer,
    pub replication_cursor: Arc<AtomicU64>,
    pub fastest_replica_cursor: Arc<AtomicU64>,
    /// Raw genesis entry bytes — sent in `StreamStart` so the replica
    /// writes a byte-identical genesis to its journal.
    pub genesis_entry: Vec<u8>,
    pub journal_path: PathBuf,
    pub authorized_keys: Arc<AuthorizedKeys>,
    pub evict_flags: [Arc<AtomicBool>; 2],
    pub active_flags: [Arc<AtomicBool>; 2],
    pub metrics: Arc<ReplicationMetrics>,
    /// Heartbeat cadence in seconds. Replication-protocol heartbeats
    /// are application-level pings (carrying the last-acked sequence)
    /// in addition to the rumcast transport heartbeats.
    pub heartbeat_secs: u64,
    /// Whether the per-tick loop should busy-spin on idle. Same flag
    /// as the order-entry path, derived from `--yield-idle`.
    pub busy_spin: bool,
    /// Optional rumcast counters (for observability). The order-entry
    /// path passes its own; replication shares the same struct so the
    /// health endpoint can aggregate.
    pub counters: Option<Arc<Counters>>,
}

/// Run the rumcast replication sender. Binds a UDP socket on
/// `config.bind_addr`, drives the muxers' per-tick loops, runs the
/// per-replica auth + replication state machines inline, and updates
/// the replication cursors on every Ack.
///
/// Blocks until shutdown.
pub fn run_sender_rumcast(
    config: Sender,
    shutdown: &AtomicBool,
    replica_ready: &AtomicBool,
    replicas_connected: &AtomicU32,
) {
    let Sender {
        bind_addr,
        repl_consumer_1,
        repl_consumer_2,
        replication_cursor,
        fastest_replica_cursor,
        genesis_entry,
        journal_path,
        authorized_keys,
        evict_flags,
        active_flags,
        metrics,
        heartbeat_secs,
        busy_spin,
        counters,
    } = config;

    // ---- Bind rumcast endpoints ----
    //
    // Replication uses two distinct UDP sockets: one bound for inbound
    // replica → primary frames (Setup, Heartbeat, Handshake, Ack), and
    // one ephemeral for primary → replica responses. Same split the
    // order-entry path uses (one MuxedReceiver, one MuxedSender).
    let inbound_socket = match KernelUdp::bind(bind_addr) {
        Ok(s) => s,
        Err(e) => {
            error!(addr = %bind_addr, error = %e, "failed to bind rumcast replication listener");
            return;
        }
    };
    let outbound_socket = match KernelUdp::bind("0.0.0.0:0".parse::<SocketAddr>().unwrap()) {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to bind rumcast replication outbound socket");
            return;
        }
    };

    let receiver_config = MuxedReceiverConfig {
        stream_id: REPL_REPLICA_STREAM,
        receiver_id: REPL_PRIMARY_RECEIVER_ID,
        initial_term_id: REPL_INITIAL_TERM_ID,
        term_length: REPL_TERM_LENGTH,
        sm_interval: Duration::from_millis(2),
        nak_backoff_min: Duration::from_micros(50),
        nak_backoff_jitter: Duration::from_micros(50),
        max_recv_per_tick: 1024,
        max_sessions: REPL_MAX_SESSIONS,
    };
    let mut muxed_receiver = MuxedReceiver::new(inbound_socket, receiver_config);

    let sender_config = MuxedSenderConfig {
        stream_id: REPL_PRIMARY_STREAM,
        initial_term_id: REPL_INITIAL_TERM_ID,
        term_length: REPL_TERM_LENGTH,
        mtu: REPL_MTU,
        setup_interval: Duration::from_millis(100),
        heartbeat_interval: Duration::from_millis(50),
        // Replication catch-up bursts are large; keep the per-tick
        // drain budget high so a single tick can flush a 768 KiB batch
        // without splitting work across many ticks.
        max_drain_per_tick: 1024 * 1024,
        max_control_per_tick: 32,
        flow_control: melin_rumcast::flow_control::FlowControl::Min,
        max_sessions: REPL_MAX_SESSIONS,
    };
    let mut muxed_sender = MuxedSender::new(outbound_socket, sender_config);

    if let Some(c) = counters.as_ref() {
        muxed_receiver.set_counters(Some(Arc::clone(c)));
        muxed_sender.set_counters(Some(Arc::clone(c)));
    }

    info!(addr = %bind_addr, "rumcast replication sender listening");

    // ---- Per-replica state ----
    //
    // `auth_states` keys by rumcast `session_id`. Sessions enter
    // `Challenged` when we see them for the first time and we send a
    // Challenge, advance to `Authenticated` on a valid
    // ChallengeResponse, then either get assigned to a slot (running
    // catch-up + live streaming) or rejected if both slots are full.
    let mut auth_states: HashMap<u32, AuthState> = HashMap::new();

    // Two slots, mirroring the TCP sender's two-replica cap. Each slot
    // owns one ReplicationConsumer; assignment happens lazily when an
    // authenticated replica is ready to enter the live phase.
    let mut slots: [SlotState; 2] = [
        SlotState::new(
            repl_consumer_1,
            Arc::clone(&evict_flags[0]),
            Arc::clone(&active_flags[0]),
            0,
        ),
        SlotState::new(
            repl_consumer_2,
            Arc::clone(&evict_flags[1]),
            Arc::clone(&active_flags[1]),
            1,
        ),
    ];
    let slot_acked: [Arc<AtomicU64>; 2] = [
        Arc::new(AtomicU64::new(u64::MAX)),
        Arc::new(AtomicU64::new(u64::MAX)),
    ];

    let mut last_sweep = Instant::now();
    let mut idle_spins: u32 = 0;
    let heartbeat_interval = Duration::from_secs(heartbeat_secs.max(1));

    // Reusable scratch buffers for the per-iteration two-phase poll.
    // Allocating once and clearing each iteration avoids a per-frame
    // `payload.to_vec()` heap allocation on the replication hot path
    // (was up to one alloc per inbound rumcast Data frame). Sized to
    // the typical batch we see; grows lazily if a tick observes more.
    let mut inbound_index: Vec<(u32, std::ops::Range<usize>)> = Vec::with_capacity(16);
    let mut inbound_bytes: Vec<u8> = Vec::with_capacity(16 * MAX_DATA_FRAME);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("rumcast replication sender shutting down");
            return;
        }

        muxed_receiver.tick();
        muxed_sender.tick();

        // ---- Detect new sessions and emit Challenge frames ----
        //
        // A new rumcast session shows up in `muxed_receiver.sessions()`
        // before any application-layer frame arrives (rumcast Setup is
        // enough). We allocate auth state lazily on first sight, send
        // a Challenge, and let the muxer's reliable transport carry it.
        {
            let mut to_challenge: Vec<u32> = Vec::new();
            for (session_id, _sublog) in muxed_receiver.sessions() {
                if !auth_states.contains_key(&session_id) {
                    to_challenge.push(session_id);
                }
            }
            for session_id in to_challenge {
                let dst = match muxed_receiver.effective_dst(session_id) {
                    Some(d) => d,
                    None => {
                        // Session exists in the receiver but no Setup
                        // frame has carried a source addr yet — this is
                        // usually a transient race; we'll get it on a
                        // later tick.
                        continue;
                    }
                };
                let pub_log = match muxed_sender.create_session(session_id, dst) {
                    Ok(log) => log,
                    Err(e) => {
                        warn!(%session_id, error = ?e, "rumcast replication: create_session failed");
                        // Session refused — drop the rumcast session so
                        // the peer must re-handshake with a fresh ID
                        // (and might land while a slot is open).
                        muxed_receiver.evict(session_id);
                        continue;
                    }
                };
                let mut nonce = [0u8; 32];
                if let Err(e) = getrandom::fill(&mut nonce) {
                    error!(error = %e, "getrandom failed during challenge generation");
                    return;
                }
                let mut buf = Vec::with_capacity(64);
                encode_challenge(&nonce, &mut buf);
                if !spin_publish(&pub_log, &buf, shutdown) {
                    return;
                }
                // Force the Setup frame out immediately so the replica
                // doesn't have to wait one full setup_interval for
                // stream parameters.
                muxed_sender.send_setup_now(session_id);
                debug!(%session_id, %dst, "rumcast replication: sent Challenge");
                auth_states.insert(
                    session_id,
                    AuthState::Challenged {
                        nonce,
                        accepted_at: Instant::now(),
                        pub_log,
                    },
                );
            }
        }

        // ---- Process inbound frames per session ----
        //
        // Two-phase pattern (same trick as `rumcast_transport.rs`): the
        // poll callback only borrows the muxed_receiver, so we record
        // routed events and process them after the poll returns where
        // we can mutate auth_states / muxed_sender.
        //
        // We append payload bytes into a single reusable `inbound_bytes`
        // buffer and remember each frame's `(session_id, range)` —
        // avoids a per-frame heap allocation on the replication hot
        // path. The buffers are cleared (length-only) at the end of
        // each iteration; capacity is retained.
        inbound_index.clear();
        inbound_bytes.clear();
        muxed_receiver.poll(MAX_DATA_FRAME as u32, |session_id, _src, view| {
            if let FrameView::Data { payload, .. } = view {
                let start = inbound_bytes.len();
                inbound_bytes.extend_from_slice(payload);
                let end = inbound_bytes.len();
                inbound_index.push((session_id, start..end));
            }
        });

        for (session_id, range) in inbound_index.drain(..) {
            // Borrow the slice afresh per iteration — we've already
            // finished extending `inbound_bytes` so the slice is stable
            // for the lifetime of this loop body.
            let payload = &inbound_bytes[range];
            handle_inbound_frame(
                session_id,
                payload,
                &mut auth_states,
                &mut muxed_sender,
                &mut muxed_receiver,
                &authorized_keys,
                &mut slots,
                &slot_acked,
                &replication_cursor,
                &fastest_replica_cursor,
                &metrics,
                replica_ready,
                replicas_connected,
                &genesis_entry,
                &journal_path,
                shutdown,
            );
        }

        // ---- Per-slot live streaming + heartbeat ----
        let mut any_work = false;
        for slot in slots.iter_mut() {
            any_work |= slot.tick(
                &mut muxed_sender,
                &mut muxed_receiver,
                &mut auth_states,
                &slot_acked,
                &replication_cursor,
                &fastest_replica_cursor,
                replicas_connected,
                &metrics,
                heartbeat_interval,
                shutdown,
            );
        }

        // ---- Sweep stale auth states ----
        if last_sweep.elapsed() >= SWEEP_INTERVAL {
            sweep_auth_states(
                &mut auth_states,
                &mut muxed_sender,
                &mut muxed_receiver,
                &mut slots,
            );
            // Slot-side AwaitingHandshake timeout — we run it here
            // (not inside `sweep_auth_states`) because the slot's
            // release path needs the cursor arcs.
            for slot in slots.iter_mut() {
                if slot_handshake_timed_out(slot) {
                    slot.release(
                        "handshake timeout",
                        &mut muxed_sender,
                        &mut muxed_receiver,
                        &mut auth_states,
                        &slot_acked,
                        &replication_cursor,
                        &fastest_replica_cursor,
                        replicas_connected,
                        &metrics,
                    );
                }
            }
            last_sweep = Instant::now();
        }

        // ---- Idle wait ----
        if any_work {
            idle_spins = 0;
        } else if busy_spin || idle_spins < 1000 {
            idle_spins = idle_spins.wrapping_add(1);
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }
}

// ---------------------------------------------------------------------------
// Auth state machine
// ---------------------------------------------------------------------------

/// Per-session auth lifecycle. Mirrors the TCP path's
/// `authenticate_replica` flow but spread across multiple ticks: the
/// rumcast muxer is single-threaded so we can't block waiting for the
/// `ChallengeResponse` like a TCP read does.
enum AuthState {
    /// Challenge sent, awaiting `ChallengeResponse`. Holds the nonce
    /// for verification and the per-session pub_log we use for
    /// `AuthOk`/`AuthFailed`.
    Challenged {
        nonce: [u8; 32],
        accepted_at: Instant,
        pub_log: Arc<PublicationLog>,
    },
    /// Auth complete; the session is bound to a slot and the slot's
    /// state machine handles everything from `Handshake` onward.
    AuthenticatedAndAssigned { slot_idx: usize },
}

#[allow(clippy::too_many_arguments)]
fn handle_inbound_frame(
    session_id: u32,
    payload: &[u8],
    auth_states: &mut HashMap<u32, AuthState>,
    muxed_sender: &mut MuxedSender<KernelUdp>,
    muxed_receiver: &mut MuxedReceiver<KernelUdp>,
    authorized_keys: &AuthorizedKeys,
    slots: &mut [SlotState; 2],
    slot_acked: &[Arc<AtomicU64>; 2],
    replication_cursor: &Arc<AtomicU64>,
    fastest_replica_cursor: &Arc<AtomicU64>,
    metrics: &ReplicationMetrics,
    replica_ready: &AtomicBool,
    replicas_connected: &AtomicU32,
    genesis_entry: &[u8],
    journal_path: &std::path::Path,
    shutdown: &AtomicBool,
) {
    // Strip the 4-byte length prefix that `protocol.rs` encoders emit.
    // Rumcast already bounds the message; the prefix is redundant but
    // we keep it so the same encoders work on both transports.
    let inner = match strip_length_prefix(payload) {
        Some(s) => s,
        None => {
            debug!(%session_id, "malformed replication frame (no length prefix)");
            return;
        }
    };

    match auth_states.get(&session_id) {
        Some(AuthState::Challenged { nonce, pub_log, .. }) => {
            let nonce_copy = *nonce;
            let pub_log_clone = Arc::clone(pub_log);
            let auth_result = verify_challenge_response(inner, &nonce_copy, authorized_keys);
            match auth_result {
                Ok(()) => {
                    let mut buf = Vec::with_capacity(8);
                    encode_auth_ok(&mut buf);
                    if !spin_publish(&pub_log_clone, &buf, shutdown) {
                        return;
                    }
                    info!(%session_id, "rumcast replication: replica authenticated");
                    // Try to assign a slot. If both are full, refuse.
                    let assigned_slot = slots
                        .iter()
                        .position(|s| matches!(s.phase, SlotPhase::Empty));
                    match assigned_slot {
                        Some(idx) => {
                            slots[idx].assign(session_id, pub_log_clone);
                            auth_states.insert(
                                session_id,
                                AuthState::AuthenticatedAndAssigned { slot_idx: idx },
                            );
                        }
                        None => {
                            warn!(%session_id, "rumcast replication: both slots full, rejecting replica");
                            let mut buf2 = Vec::with_capacity(8);
                            encode_auth_failed(&mut buf2);
                            let _ = spin_publish(&pub_log_clone, &buf2, shutdown);
                            // Drop auth state — sweep will evict the
                            // rumcast session. Actually evict now so
                            // the peer sees a clean rejection.
                            auth_states.remove(&session_id);
                            muxed_sender.evict(session_id);
                            muxed_receiver.evict(session_id);
                        }
                    }
                }
                Err(e) => {
                    let mut buf = Vec::with_capacity(8);
                    encode_auth_failed(&mut buf);
                    let _ = spin_publish(&pub_log_clone, &buf, shutdown);
                    debug!(%session_id, error = %e, "rumcast replication: auth failed");
                    auth_states.remove(&session_id);
                    muxed_sender.evict(session_id);
                    muxed_receiver.evict(session_id);
                }
            }
        }
        Some(AuthState::AuthenticatedAndAssigned { slot_idx }) => {
            let idx = *slot_idx;
            // Borrow checker: we already hold an immutable read of
            // auth_states above. Drop it before calling on_inbound,
            // which needs &mut auth_states for failure cleanup.
            let (left, right) = slots.split_at_mut(idx);
            let _ = left;
            // SAFETY: idx is 0 or 1 (REPL_MAX_SLOTS = 2); split_at_mut
            // guarantees `right[0]` is the slot at idx.
            let slot = &mut right[0];
            // We need to release the auth_states borrow before
            // mutating it inside on_inbound — split the call.
            slot.on_inbound(
                session_id,
                inner,
                muxed_sender,
                muxed_receiver,
                auth_states,
                slot_acked,
                replication_cursor,
                fastest_replica_cursor,
                replicas_connected,
                metrics,
                replica_ready,
                genesis_entry,
                journal_path,
                shutdown,
            );
        }
        None => {
            // No auth state — frame is from a session we already
            // evicted (or never handshook). Silently drop.
            debug!(%session_id, "rumcast replication: frame for unknown session");
        }
    }
}

/// Verify a `ChallengeResponse` payload against the nonce we sent.
/// On success the replica is authenticated; on error returns a human-
/// readable description for the debug log. Mirrors `auth.rs`'s logic
/// but operates on a payload slice rather than a stream.
fn verify_challenge_response(
    payload: &[u8],
    nonce: &[u8; 32],
    authorized_keys: &AuthorizedKeys,
) -> Result<(), String> {
    let (signature_bytes, pubkey_bytes) =
        decode_challenge_response(payload).map_err(|e| format!("decode error: {e}"))?;
    let permission = authorized_keys
        .lookup(&pubkey_bytes)
        .ok_or_else(|| "unknown replication key".to_string())?;
    if !permission.is_replication() {
        return Err(format!(
            "key has {permission:?} permission, expected Replication"
        ));
    }
    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pubkey_bytes)
        .map_err(|e| format!("invalid public key: {e}"))?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key
        .verify(nonce, &signature)
        .map_err(|e| format!("signature verification failed: {e}"))?;
    Ok(())
}

fn sweep_auth_states(
    auth_states: &mut HashMap<u32, AuthState>,
    muxed_sender: &mut MuxedSender<KernelUdp>,
    muxed_receiver: &mut MuxedReceiver<KernelUdp>,
    slots: &mut [SlotState; 2],
) {
    let now = Instant::now();

    // Drop Challenged sessions that never sent a valid response.
    let mut to_drop: Vec<u32> = Vec::new();
    for (session_id, state) in auth_states.iter() {
        if let AuthState::Challenged { accepted_at, .. } = state
            && now.duration_since(*accepted_at) >= HANDSHAKE_TIMEOUT
        {
            to_drop.push(*session_id);
        }
    }
    for session_id in to_drop {
        debug!(%session_id, "rumcast replication: handshake timeout, dropping session");
        auth_states.remove(&session_id);
        muxed_sender.evict(session_id);
        muxed_receiver.evict(session_id);
    }

    let _ = slots;
}

/// Returns true for any slot stuck in `AwaitingHandshake` past
/// [`HANDSHAKE_TIMEOUT`]. The main loop handles release because it has
/// the cursor arcs the slot's release path needs.
fn slot_handshake_timed_out(slot: &SlotState) -> bool {
    matches!(
        &slot.phase,
        SlotPhase::AwaitingHandshake { accepted_at, .. }
            if accepted_at.elapsed() >= HANDSHAKE_TIMEOUT
    )
}

// ---------------------------------------------------------------------------
// Slot state machine
// ---------------------------------------------------------------------------

/// Lifecycle phases per slot.
enum SlotPhase {
    /// Slot has its `ReplicationConsumer` parked but no replica is
    /// connected. Waiting for an `AuthenticatedAndAssigned` session
    /// to drive `assign`.
    Empty,
    /// Replica authenticated; waiting for the application-layer
    /// `Handshake` (last_sequence + chain_hash).
    AwaitingHandshake {
        session_id: u32,
        pub_log: Arc<PublicationLog>,
        accepted_at: Instant,
    },
    /// Replica has handshaken and is consuming live data. Catch-up
    /// completes synchronously inside `on_inbound` before transitioning
    /// here — the catch-up phase doesn't need its own variant.
    Live {
        session_id: u32,
        pub_log: Arc<PublicationLog>,
        last_sequence: u64,
        last_send: Instant,
        /// Timestamp of the most recent Ack received from this replica.
        /// Reset to `Instant::now()` on transition to Live and on every
        /// inbound Ack. Used by `tick()` to detect dead replicas via
        /// `LIVE_ACK_TIMEOUT`; under TCP the OS signals death immediately
        /// but rumcast has no equivalent.
        last_ack: Instant,
    },
}

struct SlotState {
    consumer: Option<ReplicationConsumer>,
    /// Set by the journal stage when this slot times out publishing
    /// (slow replica). The slot tears down its replica session.
    evict_flag: Arc<AtomicBool>,
    /// Set by the slot when it enters Live so the journal stage starts
    /// publishing here. Cleared on disconnect.
    active_flag: Arc<AtomicBool>,
    slot_idx: usize,
    phase: SlotPhase,
}

impl SlotState {
    fn new(
        consumer: ReplicationConsumer,
        evict_flag: Arc<AtomicBool>,
        active_flag: Arc<AtomicBool>,
        slot_idx: usize,
    ) -> Self {
        Self {
            consumer: Some(consumer),
            evict_flag,
            active_flag,
            slot_idx,
            phase: SlotPhase::Empty,
        }
    }

    fn assign(&mut self, session_id: u32, pub_log: Arc<PublicationLog>) {
        debug_assert!(matches!(self.phase, SlotPhase::Empty));
        self.phase = SlotPhase::AwaitingHandshake {
            session_id,
            pub_log,
            accepted_at: Instant::now(),
        };
    }

    /// Tear down whatever replica session is bound to this slot and
    /// return the slot to `Empty`. Idempotent — calling on an already-
    /// empty slot is a no-op. `reason` only feeds the log line.
    #[allow(clippy::too_many_arguments)]
    fn release(
        &mut self,
        reason: &'static str,
        muxed_sender: &mut MuxedSender<KernelUdp>,
        muxed_receiver: &mut MuxedReceiver<KernelUdp>,
        auth_states: &mut HashMap<u32, AuthState>,
        slot_acked: &[Arc<AtomicU64>; 2],
        replication_cursor: &Arc<AtomicU64>,
        fastest_replica_cursor: &Arc<AtomicU64>,
        replicas_connected: &AtomicU32,
        metrics: &ReplicationMetrics,
    ) {
        let (sid_opt, was_live) = match &self.phase {
            SlotPhase::Empty => (None, false),
            SlotPhase::AwaitingHandshake { session_id, .. } => (Some(*session_id), false),
            SlotPhase::Live { session_id, .. } => (Some(*session_id), true),
        };
        if let Some(sid) = sid_opt {
            warn!(
                slot = self.slot_idx,
                session_id = sid,
                reason,
                "rumcast replica released"
            );
            muxed_sender.evict(sid);
            muxed_receiver.evict(sid);
            auth_states.remove(&sid);
        }
        // Zero per-slot metrics BEFORE clearing the active flag. The
        // active_flag Release publishes these Relaxed stores together
        // — without this ordering a reader could observe `active=true`
        // (stale) paired with a freshly-zeroed cursor on weak-memory
        // architectures. See B2 in the follow-ups doc.
        metrics.acked_sequence[self.slot_idx].store(0, Ordering::Relaxed);
        metrics.in_memory_sequence[self.slot_idx].store(0, Ordering::Relaxed);
        // Clear the active flag so the journal stage stops publishing
        // into this slot's ring before we fast-forward the consumer.
        self.active_flag.store(false, Ordering::Release);
        if let Some(c) = self.consumer.as_mut() {
            c.skip_to_producer();
        }
        // Reset this slot's acked cursor and recompute the shared
        // min/max from the surviving slot.
        slot_acked[self.slot_idx].store(u64::MAX, Ordering::Release);
        let other = slot_acked[1 - self.slot_idx].load(Ordering::Acquire);
        update_dual_replication_cursor(u64::MAX, other, replication_cursor, fastest_replica_cursor);
        if was_live {
            replicas_connected.fetch_sub(1, Ordering::Release);
            if replicas_connected.load(Ordering::Relaxed) == 0 {
                warn!("all replicas disconnected — trading halted");
            }
        }
        self.phase = SlotPhase::Empty;
    }

    /// Per-tick maintenance: drains the ring consumer into the per-
    /// session pub_log when in Live, fires application-layer
    /// heartbeats, and processes ring-backpressure evictions. Returns
    /// `true` if any work happened (used to drive the busy-spin/yield
    /// decision in the parent loop).
    #[allow(clippy::too_many_arguments)]
    fn tick(
        &mut self,
        muxed_sender: &mut MuxedSender<KernelUdp>,
        muxed_receiver: &mut MuxedReceiver<KernelUdp>,
        auth_states: &mut HashMap<u32, AuthState>,
        slot_acked: &[Arc<AtomicU64>; 2],
        replication_cursor: &Arc<AtomicU64>,
        fastest_replica_cursor: &Arc<AtomicU64>,
        replicas_connected: &AtomicU32,
        metrics: &ReplicationMetrics,
        heartbeat_interval: Duration,
        shutdown: &AtomicBool,
    ) -> bool {
        let mut did_work = false;

        // Eviction: journal stage couldn't publish to this slot's ring
        // (slow replica). Tear down the session.
        if self.evict_flag.load(Ordering::Acquire) {
            self.release(
                "ring backpressure",
                muxed_sender,
                muxed_receiver,
                auth_states,
                slot_acked,
                replication_cursor,
                fastest_replica_cursor,
                replicas_connected,
                metrics,
            );
            self.evict_flag.store(false, Ordering::Release);
            return false;
        }

        // Ack timeout: release a live slot that has gone silent. Under
        // TCP the OS delivers a RST/EOF when the peer dies; rumcast has
        // no equivalent, so we detect dead replicas by the absence of
        // acks. A live, healthy replica acks every durable batch — the
        // window is sized well above any realistic ack latency.
        if matches!(&self.phase, SlotPhase::Live { last_ack, .. }
            if last_ack.elapsed() >= LIVE_ACK_TIMEOUT)
        {
            self.release(
                "ack timeout (replica silent)",
                muxed_sender,
                muxed_receiver,
                auth_states,
                slot_acked,
                replication_cursor,
                fastest_replica_cursor,
                replicas_connected,
                metrics,
            );
            return false;
        }

        if let SlotPhase::Live {
            pub_log,
            last_sequence,
            last_send,
            ..
        } = &mut self.phase
        {
            // Drain the ring consumer into the pub_log. Ring chunks are
            // wire-ready `InputBatch` frames (the journal stage dual-encodes
            // them at the pre-fsync boundary), so the sender is a passthrough
            // — no decode + re-encode here. The pub_log handles backpressure
            // via `try_claim` returning ClaimError; we spin-wait so we don't
            // drop journal data.
            let mut send_buf = Vec::with_capacity(MAX_DATA_FRAME);
            let consumer = self
                .consumer
                .as_mut()
                .expect("consumer present in Live phase");
            while let Some((meta, data)) = consumer.try_read() {
                if !spin_publish(pub_log, data, shutdown) {
                    return did_work;
                }
                consumer.commit();
                *last_sequence = meta.end_sequence;
                *last_send = Instant::now();
                did_work = true;
            }
            // Heartbeat if idle.
            if last_send.elapsed() >= heartbeat_interval {
                send_buf.clear();
                encode_heartbeat(*last_sequence, &mut send_buf);
                if !spin_publish(pub_log, &send_buf, shutdown) {
                    return did_work;
                }
                *last_send = Instant::now();
                did_work = true;
            }
        }

        did_work
    }

    #[allow(clippy::too_many_arguments)]
    fn on_inbound(
        &mut self,
        session_id: u32,
        payload: &[u8],
        muxed_sender: &mut MuxedSender<KernelUdp>,
        muxed_receiver: &mut MuxedReceiver<KernelUdp>,
        auth_states: &mut HashMap<u32, AuthState>,
        slot_acked: &[Arc<AtomicU64>; 2],
        replication_cursor: &Arc<AtomicU64>,
        fastest_replica_cursor: &Arc<AtomicU64>,
        replicas_connected: &AtomicU32,
        metrics: &ReplicationMetrics,
        replica_ready: &AtomicBool,
        genesis_entry: &[u8],
        journal_path: &std::path::Path,
        shutdown: &AtomicBool,
    ) {
        match &mut self.phase {
            SlotPhase::Empty => {
                debug!(slot = self.slot_idx, %session_id, "frame for unassigned slot");
            }
            SlotPhase::AwaitingHandshake {
                session_id: slot_sid,
                pub_log,
                ..
            } => {
                debug_assert_eq!(*slot_sid, session_id);
                let pub_log_clone = Arc::clone(pub_log);
                match decode_replica_message(payload) {
                    Ok(ReplicaMessage::Handshake(h)) => {
                        info!(
                            slot = self.slot_idx,
                            %session_id,
                            last_sequence = h.last_sequence,
                            "rumcast replication: replica handshake"
                        );
                        metrics.catching_up[self.slot_idx].store(true, Ordering::Relaxed);
                        if !run_catchup_or_snapshot(
                            self.slot_idx,
                            &pub_log_clone,
                            h.last_sequence,
                            genesis_entry,
                            journal_path,
                            shutdown,
                        ) {
                            // Catch-up failed; drop the replica.
                            metrics.catching_up[self.slot_idx].store(false, Ordering::Relaxed);
                            self.release(
                                "catch-up failed",
                                muxed_sender,
                                muxed_receiver,
                                auth_states,
                                slot_acked,
                                replication_cursor,
                                fastest_replica_cursor,
                                replicas_connected,
                                metrics,
                            );
                            return;
                        }

                        // Drain any ring entries that overlap with what
                        // catch-up already shipped. Mirrors the TCP
                        // sender's "skip overlapping ring entries"
                        // pattern.
                        if let Some(consumer) = self.consumer.as_mut() {
                            while let Some((meta, data)) = consumer.try_read() {
                                if meta.end_sequence > h.last_sequence {
                                    // Ring chunks are wire-ready InputBatch frames; passthrough.
                                    if !spin_publish(&pub_log_clone, data, shutdown) {
                                        return;
                                    }
                                    consumer.commit();
                                    break;
                                }
                                consumer.commit();
                            }
                        }

                        // Engage cursors.
                        let initial = h.last_sequence + 1;
                        slot_acked[self.slot_idx].store(initial, Ordering::Release);
                        let other = slot_acked[1 - self.slot_idx].load(Ordering::Acquire);
                        update_dual_replication_cursor(
                            initial,
                            other,
                            replication_cursor,
                            fastest_replica_cursor,
                        );

                        replicas_connected.fetch_add(1, Ordering::Release);
                        metrics.catching_up[self.slot_idx].store(false, Ordering::Relaxed);
                        // Seed metrics cursors before active_flag Release —
                        // see tcp_sender for the rationale. The active_flag
                        // Release below publishes these Relaxed stores.
                        metrics.acked_sequence[self.slot_idx]
                            .store(h.last_sequence, Ordering::Relaxed);
                        metrics.in_memory_sequence[self.slot_idx]
                            .store(h.last_sequence, Ordering::Relaxed);
                        self.active_flag.store(true, Ordering::Release);
                        replica_ready.store(true, Ordering::Release);

                        self.phase = SlotPhase::Live {
                            session_id,
                            pub_log: pub_log_clone,
                            last_sequence: h.last_sequence,
                            last_send: Instant::now(),
                            last_ack: Instant::now(),
                        };
                    }
                    Ok(ReplicaMessage::Ack(_)) => {
                        warn!(
                            slot = self.slot_idx,
                            %session_id,
                            "rumcast replication: unexpected Ack before Handshake"
                        );
                    }
                    Err(e) => {
                        warn!(slot = self.slot_idx, %session_id, error = %e, "decode error");
                    }
                }
            }
            SlotPhase::Live {
                last_sequence: _,
                last_ack,
                ..
            } => match decode_replica_message(payload) {
                Ok(ReplicaMessage::Ack(ack)) => {
                    *last_ack = Instant::now();
                    let new_val = ack.acked_sequence + 1;
                    slot_acked[self.slot_idx].store(new_val, Ordering::Release);
                    let other = slot_acked[1 - self.slot_idx].load(Ordering::Acquire);
                    update_dual_replication_cursor(
                        new_val,
                        other,
                        replication_cursor,
                        fastest_replica_cursor,
                    );
                    metrics.acked_sequence[self.slot_idx]
                        .store(ack.acked_sequence, Ordering::Relaxed);
                    metrics.in_memory_sequence[self.slot_idx]
                        .store(ack.in_memory_sequence, Ordering::Relaxed);
                }
                Ok(ReplicaMessage::Handshake(_)) => {
                    warn!(
                        slot = self.slot_idx,
                        %session_id,
                        "rumcast replication: unexpected Handshake during Live"
                    );
                }
                Err(e) => {
                    warn!(slot = self.slot_idx, %session_id, error = %e, "decode error");
                }
            },
        }

        let _ = (muxed_sender, replication_cursor, fastest_replica_cursor);
    }
}

/// Run the catch-up (or snapshot transfer) flow synchronously and
/// publish all frames via `pub_log`. Returns true on success, false on
/// any error (caller drops the replica).
///
/// Mirrors the TCP sender's `handle_replica_connection` body up to the
/// live-streaming entry point, but everything is published via the
/// rumcast pub_log instead of a `TcpStream`.
fn run_catchup_or_snapshot(
    slot_idx: usize,
    pub_log: &Arc<PublicationLog>,
    last_sequence: u64,
    genesis_entry: &[u8],
    journal_path: &std::path::Path,
    shutdown: &AtomicBool,
) -> bool {
    // Probe whether journal catch-up is possible.
    let can_catch_up = match can_catch_up_from_journal(journal_path, last_sequence) {
        Ok(v) => v,
        Err(e) => {
            error!(slot = slot_idx, error = %e, "rumcast replication: journal probe failed");
            return false;
        }
    };

    let mut send_buf = Vec::with_capacity(128);
    if can_catch_up {
        // Send StreamStart and stream journal entries.
        encode_stream_start(last_sequence, genesis_entry, &mut send_buf);
        if !spin_publish(pub_log, &send_buf, shutdown) {
            return false;
        }
        let mut publish = |buf: &[u8]| -> std::io::Result<()> {
            if spin_publish(pub_log, buf, shutdown) {
                Ok(())
            } else {
                Err(std::io::Error::other("publish aborted by shutdown"))
            }
        };
        match catch_up_from_journal_with(journal_path, last_sequence, &mut publish, shutdown) {
            Ok(CatchUpResult::Ok(_end)) => true,
            Ok(CatchUpResult::NeedSnapshot) => {
                error!(slot = slot_idx, "catch-up failed unexpectedly after probe");
                false
            }
            Err(e) => {
                error!(slot = slot_idx, error = %e, "catch-up error");
                false
            }
        }
    } else {
        // Snapshot transfer path.
        let snap_path = journal_path.with_extension("snapshot");
        if !snap_path.exists() {
            error!(
                slot = slot_idx,
                path = %snap_path.display(),
                "snapshot transfer required but no snapshot file present"
            );
            return false;
        }
        send_buf.clear();
        encode_need_snapshot(&mut send_buf);
        if !spin_publish(pub_log, &send_buf, shutdown) {
            return false;
        }
        let snap_data = match std::fs::read(&snap_path) {
            Ok(d) => d,
            Err(e) => {
                error!(slot = slot_idx, error = %e, "read snapshot failed");
                return false;
            }
        };
        if snap_data.len() < 48 {
            error!(slot = slot_idx, "snapshot too small for header");
            return false;
        }
        let magic = u32::from_le_bytes(snap_data[0..4].try_into().unwrap());
        if magic != 0x534E_4150 {
            error!(slot = slot_idx, magic, "snapshot has invalid magic");
            return false;
        }
        let snap_sequence = u64::from_le_bytes(snap_data[8..16].try_into().unwrap());
        let mut snap_chain_hash = [0u8; 32];
        snap_chain_hash.copy_from_slice(&snap_data[16..48]);

        send_buf.clear();
        encode_snapshot_begin(
            snap_data.len() as u64,
            snap_sequence,
            &snap_chain_hash,
            &mut send_buf,
        );
        if !spin_publish(pub_log, &send_buf, shutdown) {
            return false;
        }

        let mut offset = 0;
        while offset < snap_data.len() {
            let end = (offset + SNAPSHOT_CHUNK_SIZE).min(snap_data.len());
            send_buf.clear();
            encode_snapshot_chunk(&snap_data[offset..end], &mut send_buf);
            if !spin_publish(pub_log, &send_buf, shutdown) {
                return false;
            }
            offset = end;
        }

        let crc = crc32c::crc32c(&snap_data);
        send_buf.clear();
        encode_snapshot_end(crc, &mut send_buf);
        if !spin_publish(pub_log, &send_buf, shutdown) {
            return false;
        }

        // Send StreamStart and continue with journal catch-up from
        // `snap_sequence`.
        send_buf.clear();
        encode_stream_start(snap_sequence, genesis_entry, &mut send_buf);
        if !spin_publish(pub_log, &send_buf, shutdown) {
            return false;
        }
        let mut publish = |buf: &[u8]| -> std::io::Result<()> {
            if spin_publish(pub_log, buf, shutdown) {
                Ok(())
            } else {
                Err(std::io::Error::other("publish aborted by shutdown"))
            }
        };
        match catch_up_from_journal_with(journal_path, snap_sequence, &mut publish, shutdown) {
            Ok(_) => true,
            Err(e) => {
                error!(slot = slot_idx, error = %e, "post-snapshot catch-up failed");
                false
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Strip the 4-byte little-endian length prefix the `protocol.rs`
/// encoders prepend. Returns `None` if the prefix is missing or the
/// declared length doesn't match the payload size.
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

/// Maximum per-fragment payload bytes derived from the rumcast MTU
/// minus the 32-byte fragment header. Must remain a multiple of the
/// crate's `FRAGMENT_ALIGNMENT` (32) — both `1408 - 32 = 1376` and the
/// alignment constraint hold for our fixed `REPL_MTU`.
const MAX_FRAGMENT_PAYLOAD: usize = (REPL_MTU as usize) - 32;

/// Spin-wait publish a complete (possibly multi-fragment) message via
/// `pub_log`. Handles fragmentation for payloads larger than the per-
/// fragment MTU. Returns false only on shutdown — backpressure from a
/// slow replica is the common case during catch-up and resolves once
/// the receiver advances its publisher_limit.
///
/// Important: every fragment of a multi-fragment message must be
/// claimed and published in order without yielding to other publishers
/// of the same `pub_log`. Rumcast's `PublicationLog` is single-producer
/// by contract, so the only contention is between this function and
/// itself across loop iterations — which is fine because each call
/// publishes one full message before returning.
fn spin_publish(pub_log: &PublicationLog, payload: &[u8], shutdown: &AtomicBool) -> bool {
    if payload.is_empty() {
        warn!("spin_publish: empty payload, dropping");
        return false;
    }
    if payload.len() <= MAX_FRAGMENT_PAYLOAD {
        return spin_publish_one(pub_log, payload, data_flags::UNFRAGMENTED, shutdown);
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
        if !spin_publish_one(pub_log, &payload[offset..end], flags, shutdown) {
            return false;
        }
        offset = end;
    }
    true
}

fn spin_publish_one(
    pub_log: &PublicationLog,
    fragment: &[u8],
    flags: u8,
    shutdown: &AtomicBool,
) -> bool {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return false;
        }
        match pub_log.try_claim(fragment.len() as u32) {
            Ok(mut claim) => {
                claim.payload_mut().copy_from_slice(fragment);
                claim.publish(flags);
                return true;
            }
            Err(melin_rumcast::pub_log::ClaimError::PayloadTooLarge { .. }) => {
                // Caller picked a bad fragment size — programmer error,
                // not a runtime condition. Drop the message rather than
                // spinning forever.
                error!(
                    len = fragment.len(),
                    "rumcast publish: PayloadTooLarge, fragment exceeds MTU"
                );
                return false;
            }
            Err(_) => {
                std::hint::spin_loop();
            }
        }
    }
}
