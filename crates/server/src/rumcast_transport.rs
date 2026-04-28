//! Standalone server with rumcast (reliable UDP) as the order-entry
//! transport. Mutually exclusive with the `dpdk` feature at build time.
//!
//! # What this is for
//!
//! Lets the LAN bench suite (`melin-bench`) compare TCP versus rumcast
//! on the same engine pipeline. Phase 3 scope:
//!
//! - Standalone primary only (no replica, no promotion).
//! - **Pure-UDP authentication** via Ed25519 challenge-response +
//!   X25519 ECDH, with per-message BLAKE3 keyed-MAC envelopes on the
//!   data plane. Same Ed25519 identities as the TCP path
//!   (`authorized_keys`).
//! - **Multi-client demux.** Each client picks its own random
//!   `session_id`; the muxed receiver allocates a per-session
//!   `SubscriptionLog` lazily on first contact, the muxed sender
//!   allocates a per-session `PublicationLog` at the handshake-
//!   completion event. Bounded by `MAX_SESSIONS`. Each session's
//!   response dst is auto-discovered from the source addr of the
//!   client's first inbound frame — this requires the client to use
//!   `melin_rumcast::shared_udp::SharedUdp` so its publisher source
//!   addr equals its subscriber addr (single socket per peer).
//! - Kernel UDP only (rumcast's `KernelUdp`). DPDK rumcast backend is
//!   a separate effort tracked under the rumcast crate's deferred list.
//!
//! # Wiring (at a glance)
//!
//! ```text
//! [bench client A]            [melin-server (this)]            [bench client B]
//!   PublicationLog A ──orders (UDP)──▶ MuxedReceiver ◀── orders (UDP)── PublicationLog B
//!                                              │
//!                                              ▼
//!                                       session-translator
//!                                       (auth state machine,
//!                                        envelope verify/wrap,
//!                                        drives muxed ticks)
//!                                              │
//!                                  input ring ▲▼ output ring
//!                                              │
//!                                       engine pipeline
//!                                              │
//!   SubscriptionLog A ◀── responses (UDP)── MuxedSender ── responses (UDP)──▶ SubscriptionLog B
//! ```
//!
//! The session-translator is a single thread that owns the per-session
//! auth + replay state AND drives the muxed receiver/sender ticks
//! inline. Combining everything into one thread keeps each muxed
//! primitive's session table lock-free and preserves the single-
//! producer contract on every per-session PublicationLog.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tracing::{debug, error, info, warn};
use x25519_dalek::{PublicKey as X25519Public, StaticSecret as X25519Secret};

use melin_protocol::auth::{AuthorizedKeys, Permission};
use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};
use melin_protocol::session::{
    EnvelopeError, encode_envelope, verify_and_decode_envelope, verify_client_handshake,
};
use melin_rumcast::counters::Counters;
use melin_rumcast::muxed_receiver::{MuxedReceiver, MuxedReceiverConfig};
use melin_rumcast::muxed_sender::{MuxedSender, MuxedSenderConfig};
use melin_rumcast::pub_log::PublicationLog;
use melin_rumcast::transport::KernelUdp;
use melin_rumcast::wire::{FrameView, data_flags};
use melin_trading::types::QueryResponse;
use melin_transport_core::pipeline::{OutputPayload, Pipeline, build_pipeline_with_replication};

use crate::server::{ServerConfig, init_engine};
use crate::{InputSlot, JournalEvent, OutputSlot};

// ---------------------------------------------------------------------------
// Per-session auth state
// ---------------------------------------------------------------------------

/// State per rumcast session, keyed by the wire `session_id`.
///
/// Pre-handshake there's no entry — the first inbound `Heartbeat`
/// from a fresh `session_id` creates a `Challenged` entry. Receipt
/// of a valid `ChallengeResponse` advances it to `Authenticated`.
/// Any other inbound state transition (wrong message in wrong stage,
/// unknown key, bad signature) drops the entry and silently rejects
/// further traffic from that session_id until the client retries.
///
/// **Client restart**: a client that restarts MUST pick a fresh
/// `session_id`. Reusing the previous one finds either a stale
/// `Challenged` entry (which expects a `ChallengeResponse`, not a
/// new `Heartbeat`) or a stale `Authenticated` entry (which expects
/// envelope-wrapped traffic, not an unwrapped `Heartbeat`). Either
/// way the new client's first message is dropped and the client
/// hangs until [`HANDSHAKE_TIMEOUT`] expires (Challenged) or
/// indefinitely (Authenticated). Clients should generate a random
/// 32-bit `session_id` per fresh connect — same convention Aeron
/// uses for publication identity.
enum AuthStage {
    /// Server has sent a Challenge frame and is awaiting the client's
    /// ChallengeResponse. The nonce + ephemeral keypair are kept here
    /// so the verify side can rebuild the signing payload and
    /// complete the X25519 ECDH. The per-session `pub_log` was
    /// allocated by `MuxedSender::create_session` at first-contact
    /// time and is used to send Challenge / ServerReady / AuthFailed
    /// (all unwrapped, since the client doesn't have the token yet).
    Challenged {
        nonce: [u8; 32],
        server_x25519_secret: X25519Secret,
        server_x25519_public: [u8; 32],
        accepted_at: Instant,
        pub_log: Arc<PublicationLog>,
    },
    /// Handshake complete. All subsequent inbound payloads must be
    /// envelope-wrapped under `token`. Outbound responses are wrapped
    /// using the same token with a separately-tracked `outbound_seq`.
    /// The same `pub_log` Arc is carried over from `Challenged`.
    Authenticated {
        token: [u8; 32],
        key_hash: u64,
        permission: Permission,
        last_inbound_seq: u64,
        outbound_seq: u64,
        pub_log: Arc<PublicationLog>,
    },
}

/// Drop a Challenged entry if the client takes longer than this to
/// reply with a ChallengeResponse. Bounds the memory an unauthenticated
/// peer can pin by spamming Heartbeats from new session_ids.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Minimum gap between handshake-timeout sweeps. The sweep itself
/// is `O(n)` over the session table, so on a busy-spinning idle loop
/// we'd otherwise burn cycles iterating it millions of times per
/// second for no useful work. 1s is well below `HANDSHAKE_TIMEOUT`,
/// so a stale Challenged entry lives at most `HANDSHAKE_TIMEOUT +
/// SWEEP_INTERVAL` ≈ 6s before getting reaped.
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Reusable buffers for the session translator. Sized for the
/// largest auth control frame (Challenge: ~70B encoded) and the
/// largest data-plane request (~100B order + 24B envelope). 1 KiB
/// gives generous headroom on both, fits in a single cache page,
/// and gets allocated once at thread startup.
const RESPONSE_ENCODE_BUF_SIZE: usize = 1024;
const ENVELOPE_BUF_SIZE: usize = 2048;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration specific to the rumcast standalone path. Built from
/// `ServerConfig` by `main.rs::rumcast_config_from`.
#[derive(Debug, Clone, Copy)]
pub struct RumcastConfig {
    /// Local address the server binds for incoming order datagrams.
    /// Reuses the existing `--bind` ServerConfig flag so users don't
    /// have to learn a new knob.
    pub bind: SocketAddr,
}

// ---------------------------------------------------------------------------
// Wire-format constants
// ---------------------------------------------------------------------------

/// Stream IDs for the order-entry channels. session_id is now per-
/// client (random 32-bit, picked by the client at connect time —
/// see Aeron's publication-identity convention).
const RUMCAST_ORDERS_STREAM: u32 = 1; // client → server
const RUMCAST_RESP_STREAM: u32 = 2; // server → client

/// Per-session term length. Each session allocates `3 * term_length`
/// for its receive sublog plus the same for its send publog. This
/// stays at 16 MiB for #29 because the bench/smoke clients are
/// hardcoded to that value — reducing it would require a coordinated
/// wire-format-compatible change on both sides. Phase 3 task #31
/// drops it (likely to 1 MiB or 256 KiB) once bench/smoke get
/// updated together.
const TERM_LENGTH: u32 = 16 * 1024 * 1024;
/// Conservative MTU for kernel UDP — leaves ~92 bytes of headroom
/// below the typical 1500-byte Ethernet payload to absorb any IP+UDP
/// header growth (no VLAN/IPv6 surprises).
const MTU: u32 = 1408;
/// Both sides start at term_id = 1 by convention.
const INITIAL_TERM_ID: u32 = 1;
/// Per-server receiver_id stamped into every Status Message. With
/// the muxed receiver each session has its own `subscriber_position`
/// but the receiver_id itself is a server-wide constant — every
/// response publog the server sends to is from this same logical
/// "subscriber" identity.
const SERVER_RECEIVER_ID: u64 = 1;
/// Cap on concurrent clients. With `TERM_LENGTH = 16 MiB` and 3
/// partitions per sublog + 3 per publog, the worst case is
/// `MAX_SESSIONS × 6 × 16 MiB`. At the current 16 — chosen
/// deliberately small until #31 reduces `TERM_LENGTH` — that's
/// 1.5 GiB. Phase 3 task #31 raises this to ~1024 once term_length
/// drops.
const MAX_SESSIONS: u32 = 16;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Entry point for the rumcast standalone server.
pub fn run_rumcast(
    config: ServerConfig,
    rumcast_config: RumcastConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        bind = %rumcast_config.bind,
        "starting rumcast standalone server"
    );

    // ---- Authorized keys ----
    //
    // Same on-disk format and Permission model as the TCP path —
    // there's exactly one source of identity in the system. The
    // session translator looks clients up here when verifying
    // ChallengeResponse frames.
    let authorized_keys = Arc::new(AuthorizedKeys::load(&config.authorized_keys)?);
    info!(
        keys = authorized_keys.len(),
        path = %config.authorized_keys.display(),
        "loaded authorized_keys for rumcast auth"
    );

    // ---- Engine pipeline ----
    let (app, writer, needs_seeding) = init_engine(&config)?;

    let active_connections = Arc::new(AtomicU64::new(1));
    let pipeline: Pipeline<crate::App> = build_pipeline_with_replication(
        app,
        writer,
        Duration::from_micros(config.group_commit_us),
        Arc::clone(&active_connections),
        false, // enable_replication
        config.max_journal_batch,
        config.replication_ring_size,
        !config.yield_idle, // busy_spin
        false,              // enable_event_publisher
        false,              // enable_shadow
    );

    let Pipeline {
        mut input_producer,
        journal_stage,
        matching_stage,
        mut output_consumers,
        journal_cursor,
        matching_cursor,
        ..
    } = pipeline;

    let response_consumer = output_consumers
        .pop()
        .expect("response consumer (output_consumers must have at least one)");

    // ---- Rumcast endpoints (multi-session muxed) ----
    //
    // Each client is identified by a random 32-bit `session_id`
    // it picks at connect time. The server owns one socket per
    // direction and a per-session SubscriptionLog / PublicationLog
    // map; sessions are allocated lazily on first contact (inbound)
    // or when authentication completes (outbound).
    let orders_socket = KernelUdp::bind(rumcast_config.bind)?;
    let muxed_receiver_config = MuxedReceiverConfig {
        stream_id: RUMCAST_ORDERS_STREAM,
        receiver_id: SERVER_RECEIVER_ID,
        initial_term_id: INITIAL_TERM_ID,
        term_length: TERM_LENGTH,
        sm_interval: Duration::from_millis(2),
        nak_backoff_min: Duration::from_micros(50),
        nak_backoff_jitter: Duration::from_micros(50),
        max_recv_per_tick: 1024,
        max_sessions: MAX_SESSIONS,
    };
    let muxed_receiver = MuxedReceiver::new(orders_socket, muxed_receiver_config);

    let resp_socket = KernelUdp::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())?;
    let muxed_sender_config = MuxedSenderConfig {
        stream_id: RUMCAST_RESP_STREAM,
        initial_term_id: INITIAL_TERM_ID,
        term_length: TERM_LENGTH,
        mtu: MTU,
        setup_interval: Duration::from_millis(100),
        heartbeat_interval: Duration::from_millis(50),
        max_drain_per_tick: 1024 * 1024,
        max_control_per_tick: 32,
        // Phase 3: leave flow control as `Min` (the rumcast default).
        // We don't expect to be flow-control-bound on a healthy LAN
        // — backpressure here would stem from a slow subscriber, and
        // we'd rather the client see backpressure than the server
        // accumulate unbounded buffers.
        flow_control: melin_rumcast::flow_control::FlowControl::Min,
        max_sessions: MAX_SESSIONS,
    };
    let muxed_sender = MuxedSender::new(resp_socket, muxed_sender_config);

    // Shared counters (helpful for bench observability; cheap when nobody reads).
    let counters = Arc::new(Counters::new());

    // ---- Thread plumbing ----

    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();

    // Pipeline: journal stage.
    let journal_shutdown = Arc::clone(&shutdown);
    handles.push(
        thread::Builder::new()
            .name("journal".into())
            .spawn(move || {
                if let Err(e) = journal_stage.run(&journal_shutdown) {
                    error!(error = ?e, "journal stage exited with error");
                }
            })?,
    );

    // Pipeline: matching stage.
    let matching_shutdown = Arc::clone(&shutdown);
    handles.push(
        thread::Builder::new()
            .name("matching".into())
            .spawn(move || {
                let _final_app = matching_stage.run(&matching_shutdown);
            })?,
    );

    // ---- Seed accounts and instruments on first startup ----
    //
    // The bench publishes orders against a fixed set of (instrument,
    // account) IDs. Without seeding, the matching engine rejects every
    // request as "unknown instrument" / "unknown account". Mirrors the
    // TCP path's `if needs_seeding` block.
    if needs_seeding {
        seed_and_drain(
            &mut input_producer,
            &journal_cursor,
            &matching_cursor,
            config.instruments,
            config.accounts,
            &shutdown,
        );
    }

    // Idle strategy: default (no flag) = busy-spin (lowest latency on
    // isolated cores). `--yield-idle` switches the session translator
    // (which now drives the muxed receiver / sender ticks inline) to
    // sleep-tick. Matches the convention used by JournalStage /
    // MatchingStage (which take the same flag inverted as `busy_spin`).
    let yield_idle = config.yield_idle;

    // Session translator: drives the muxed receiver + sender ticks
    // inline AND runs the auth state machine + envelope wrap/verify.
    // One thread for everything keeps the per-session state lock-free
    // and preserves the single-producer contract on every per-session
    // PublicationLog (the sender's tick is the sole reader of each
    // log's publisher_position).
    {
        let shutdown = Arc::clone(&shutdown);
        let cursor = Arc::clone(&journal_cursor);
        let authorized_keys = Arc::clone(&authorized_keys);
        let mut muxed_receiver = muxed_receiver;
        let mut muxed_sender = muxed_sender;
        muxed_receiver.set_counters(Some(Arc::clone(&counters)));
        muxed_sender.set_counters(Some(Arc::clone(&counters)));
        handles.push(
            thread::Builder::new()
                .name("rumcast-session".into())
                .spawn(move || {
                    session_translator(
                        muxed_receiver,
                        muxed_sender,
                        &mut input_producer,
                        response_consumer,
                        cursor,
                        authorized_keys,
                        &shutdown,
                        yield_idle,
                    );
                })?,
        );
    }

    info!("rumcast standalone server up; awaiting shutdown");

    // Wait for shutdown.
    while !shutdown.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(100));
    }

    info!("shutdown signalled; joining threads");
    for h in handles {
        if let Err(e) = h.join() {
            warn!(?e, "thread join error");
        }
    }
    info!("rumcast standalone server stopped");
    Ok(())
}

// ---------------------------------------------------------------------------
// Session translator
// ---------------------------------------------------------------------------

/// Combined translator + rumcast tick driver for the multi-session
/// path.
///
/// One thread:
/// 1. Drives `MuxedReceiver::tick` (drain UDP into per-session
///    sublogs, run NAK/SM bookkeeping per session).
/// 2. Polls every per-session SubscriptionLog and feeds frames into
///    the auth state machine.
/// 3. Drives `MuxedSender::tick` (drain each per-session
///    PublicationLog → UDP, route incoming NAK/SM by session_id,
///    fire periodic Setup/Heartbeat per session).
/// 4. Wraps engine responses in envelopes addressed to the right
///    per-session PublicationLog.
///
/// Why combined: each `MuxedSender` per-session PublicationLog
/// requires single-producer access. If inbound (handshake control
/// replies) and outbound (engine responses) lived on separate
/// threads, two threads could `try_claim` the same publog. One
/// thread sidesteps the race and keeps every per-session
/// HashMap entry lock-free.
///
/// **Single-pending outbound design**: at most one OutputSlot is
/// held while we wait for the journal cursor to catch up
/// (persist-before-ack). Inbound traffic continues to drain during
/// that wait, which prevents per-session SubscriptionLogs from
/// filling during a long fsync.
#[allow(clippy::too_many_arguments)]
fn session_translator(
    mut muxed_receiver: MuxedReceiver<KernelUdp>,
    mut muxed_sender: MuxedSender<KernelUdp>,
    input_producer: &mut melin_disruptor::ring::Producer<InputSlot>,
    mut output_consumer: melin_disruptor::ring::Consumer<OutputSlot>,
    journal_cursor: Arc<melin_disruptor::padding::Sequence>,
    authorized_keys: Arc<AuthorizedKeys>,
    shutdown: &AtomicBool,
    yield_idle: bool,
) {
    let mut sessions: HashMap<u32, AuthStage> = HashMap::new();
    let mut pending_outbound: Option<OutputSlot> = None;
    let mut response_buf = vec![0u8; RESPONSE_ENCODE_BUF_SIZE];
    let mut envelope_buf = vec![0u8; ENVELOPE_BUF_SIZE];
    // Wall-clock checkpoint for handshake-timeout sweeps. Throttled
    // because the sweep is O(n) over `sessions` and would otherwise
    // run millions of times per second under busy-spin idle.
    let mut last_sweep_at = Instant::now();

    while !shutdown.load(Ordering::Acquire) {
        let mut did_work = false;

        // ---- Drive the rumcast wire-layer ticks ----
        //
        // These run inline (no separate threads). `tick()` drains
        // UDP into per-session sublogs / out of per-session publogs
        // and processes NAK/SM control frames. They're cheap when
        // idle and proportional to per-session work otherwise.
        let recv_stats = muxed_receiver.tick();
        let send_stats = muxed_sender.tick();
        if recv_stats.fragments_accepted > 0
            || recv_stats.bytes_received > 0
            || send_stats.fragments_sent > 0
        {
            did_work = true;
        }

        // ---- Inbound: per-session sublogs → handshake / engine input ----
        //
        // `to_evict` collects session_ids that the auth machine
        // wants to drop (auth failure, unrecoverable error). We
        // can't evict from `muxed_receiver` while inside its `poll`
        // (would need &mut while holding &), so apply post-poll.
        let mut to_evict: Vec<u32> = Vec::new();
        let drained = muxed_receiver.poll(64 * 1024, |session_id, src_addr, view| {
            let FrameView::Data { header, payload } = view else {
                return;
            };
            if header.common.flags & data_flags::PADDING != 0 {
                return;
            }
            handle_inbound(
                session_id,
                src_addr,
                payload,
                &mut sessions,
                &authorized_keys,
                input_producer,
                &mut muxed_sender,
                &mut response_buf,
                &mut to_evict,
                shutdown,
            );
        });
        if drained > 0 {
            did_work = true;
        }
        for sid in to_evict.drain(..) {
            sessions.remove(&sid);
            muxed_receiver.evict(sid);
            muxed_sender.evict(sid);
        }

        // ---- Drop stale Challenged sessions ----
        //
        // Throttled to once per `SWEEP_INTERVAL`. When a
        // Challenged entry expires we evict from the auth table AND
        // both muxers — otherwise a half-handshaked client would
        // pin per-session sublog/publog memory until process
        // restart.
        let now = Instant::now();
        if now.duration_since(last_sweep_at) >= SWEEP_INTERVAL {
            let mut expired: Vec<u32> = Vec::new();
            sessions.retain(|session_id, stage| match stage {
                AuthStage::Challenged { accepted_at, .. } => {
                    if now.duration_since(*accepted_at) >= HANDSHAKE_TIMEOUT {
                        debug!(%session_id, "handshake timed out; dropping session");
                        expired.push(*session_id);
                        false
                    } else {
                        true
                    }
                }
                AuthStage::Authenticated { .. } => true,
            });
            for sid in expired {
                muxed_receiver.evict(sid);
                muxed_sender.evict(sid);
            }
            last_sweep_at = now;
        }

        // ---- Outbound: engine output ring → envelope → PublicationLog ----
        //
        // Single-pending design: try to consume one slot, hold it
        // until the journal cursor catches up. This is the
        // persist-before-ack boundary — we never publish a response
        // for an event that hasn't been durably journaled.
        if pending_outbound.is_none()
            && let Some((_, slot)) = output_consumer.try_consume()
        {
            pending_outbound = Some(slot);
            did_work = true;
        }
        if let Some(slot) = pending_outbound.as_ref() {
            // Seed events (connection_id=0) come from `seed_and_drain`.
            // No client to route them to — drop.
            if slot.connection_id == 0 {
                pending_outbound = None;
                // Mark progress so the loop immediately tries to
                // consume the next slot — otherwise yield-idle mode
                // would sleep 10µs per dropped seed event, turning
                // ~hundred-event seed_and_drain into ~ms latency.
                did_work = true;
            } else if journal_cursor.get().load(Ordering::Acquire) > slot.input_seq {
                let slot = pending_outbound.take().expect("checked is_some above");
                handle_outbound(
                    &slot,
                    &mut sessions,
                    &mut response_buf,
                    &mut envelope_buf,
                    shutdown,
                );
                did_work = true;
            }
            // else: not durable yet, leave pending and re-check next loop.
        }

        // ---- Idle ----
        if !did_work {
            if yield_idle {
                thread::sleep(Duration::from_micros(10));
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

/// Process one inbound payload for a given `session_id`. Drives the
/// handshake state machine pre-auth and verifies envelopes post-auth.
///
/// All failure paths drop silently with a debug log — there's no
/// authenticated channel to send an error back on for an
/// unauthenticated client, and post-auth a malformed/replayed
/// envelope is indistinguishable from network noise.
///
/// On unrecoverable auth failure (unknown key, bad signature) the
/// caller wants to evict from BOTH the auth table AND the muxed
/// receiver / sender. Since we can't evict the receiver while
/// iterating its `poll`, we append the session_id to `to_evict`
/// and let the caller apply it after the poll returns.
#[allow(clippy::too_many_arguments)]
fn handle_inbound(
    session_id: u32,
    src_addr: SocketAddr,
    payload: &[u8],
    sessions: &mut HashMap<u32, AuthStage>,
    authorized_keys: &AuthorizedKeys,
    input_producer: &mut melin_disruptor::ring::Producer<InputSlot>,
    muxed_sender: &mut MuxedSender<KernelUdp>,
    response_buf: &mut [u8],
    to_evict: &mut Vec<u32>,
    shutdown: &AtomicBool,
) {
    match sessions.entry(session_id) {
        Entry::Vacant(slot) => {
            // Pre-handshake. The protocol says the client kicks off
            // with a Heartbeat (UDP has no `accept` event for the
            // server to react to, so the client has to speak first).
            let request = match codec::decode_request(payload) {
                Ok((_, r)) => r,
                Err(_) => return,
            };
            if !matches!(request, Request::Heartbeat) {
                debug!(%session_id, "pre-auth: expected Heartbeat, dropping");
                return;
            }

            let (nonce, server_secret, server_public) = match generate_challenge_material() {
                Some(t) => t,
                None => {
                    error!("getrandom failed during Challenge generation; dropping session");
                    return;
                }
            };

            // Allocate the per-session response PublicationLog now —
            // we need it to send Challenge. If `MuxedSender` refuses
            // (max_sessions hit), drop the kickoff and bail.
            //
            // `src_addr` is the auto-discovered source addr from the
            // client's first inbound frame. With a `SharedUdp`-based
            // client, that's also the client's subscriber address —
            // so responses route correctly back to the same socket.
            // Two-socket clients would land here with the publisher
            // port, NOT the subscriber port; #30 / docs require
            // SharedUdp on the client side.
            let pub_log = match muxed_sender.create_session(session_id, src_addr) {
                Ok(log) => log,
                Err(e) => {
                    debug!(
                        %session_id,
                        ?e,
                        "pre-auth: MuxedSender refused session, dropping kickoff"
                    );
                    // The MuxedReceiver already allocated a sublog
                    // for this session_id. Mark it for eviction so we
                    // don't pin its memory.
                    to_evict.push(session_id);
                    return;
                }
            };

            if encode_and_publish_unwrapped(
                &ResponseKind::Challenge {
                    nonce,
                    server_x25519_eph: server_public,
                },
                &pub_log,
                response_buf,
                shutdown,
            )
            .is_none()
            {
                // Shutdown — drop without inserting; clean exit on
                // next loop. Caller will evict via the muxers when
                // shutdown propagates.
                muxed_sender.evict(session_id);
                return;
            }

            slot.insert(AuthStage::Challenged {
                nonce,
                server_x25519_secret: server_secret,
                server_x25519_public: server_public,
                accepted_at: Instant::now(),
                pub_log,
            });
        }
        Entry::Occupied(mut entry) => {
            // Borrow once; branch by stage.
            let stage = entry.get_mut();
            match stage {
                AuthStage::Challenged {
                    nonce,
                    server_x25519_secret,
                    server_x25519_public,
                    pub_log,
                    ..
                } => {
                    let request = match codec::decode_request(payload) {
                        Ok((_, r)) => r,
                        Err(_) => return,
                    };
                    let (signature, public_key, client_eph) = match request {
                        Request::ChallengeResponse {
                            signature,
                            public_key,
                            client_x25519_eph,
                        } => (signature, public_key, client_x25519_eph),
                        _ => {
                            debug!(
                                %session_id,
                                "challenged: expected ChallengeResponse, dropping"
                            );
                            return;
                        }
                    };

                    // Look up the client's identity. Unknown key →
                    // AuthFailed + drop the session.
                    let permission = match authorized_keys.lookup(&public_key) {
                        Some(p) => p,
                        None => {
                            debug!(%session_id, "auth: unknown public key");
                            let _ = encode_and_publish_unwrapped(
                                &ResponseKind::AuthFailed,
                                pub_log,
                                response_buf,
                                shutdown,
                            );
                            to_evict.push(session_id);
                            return;
                        }
                    };

                    // Verify Ed25519 signature + derive the session
                    // token via X25519 ECDH + BLAKE3 KDF. Single
                    // helper shared with the bench-side
                    // `ClientHandshake::finish` so the byte assembly
                    // can't drift between peers.
                    let token = match verify_client_handshake(
                        nonce,
                        server_x25519_public,
                        server_x25519_secret,
                        &public_key,
                        &client_eph,
                        &signature,
                    ) {
                        Ok(t) => t,
                        Err(e) => {
                            debug!(%session_id, ?e, "auth: handshake verify failed");
                            let _ = encode_and_publish_unwrapped(
                                &ResponseKind::AuthFailed,
                                pub_log,
                                response_buf,
                                shutdown,
                            );
                            to_evict.push(session_id);
                            return;
                        }
                    };
                    let key_hash = compute_key_hash(&public_key);

                    if encode_and_publish_unwrapped(
                        &ResponseKind::ServerReady,
                        pub_log,
                        response_buf,
                        shutdown,
                    )
                    .is_none()
                    {
                        // Shutdown — drop the entry rather than leave
                        // a partially-completed handshake.
                        to_evict.push(session_id);
                        return;
                    }

                    debug!(%session_id, ?permission, "rumcast auth complete");

                    // Carry the existing pub_log Arc through to
                    // Authenticated — it points at the same per-
                    // session PublicationLog the MuxedSender owns.
                    let pub_log_clone = Arc::clone(pub_log);
                    *stage = AuthStage::Authenticated {
                        token,
                        key_hash,
                        permission,
                        last_inbound_seq: 0,
                        outbound_seq: 0,
                        pub_log: pub_log_clone,
                    };
                }
                AuthStage::Authenticated {
                    token,
                    key_hash,
                    permission,
                    last_inbound_seq,
                    ..
                } => {
                    // Verify the envelope. Replay first (cheap), then
                    // MAC. Any failure: drop silently.
                    let (seq, inner) = match verify_and_decode_envelope(
                        token,
                        session_id,
                        *last_inbound_seq,
                        payload,
                    ) {
                        Ok(x) => x,
                        Err(EnvelopeError::Replay { .. }) => {
                            debug!(%session_id, "envelope replay");
                            return;
                        }
                        Err(e) => {
                            debug!(%session_id, ?e, "envelope verify failed");
                            return;
                        }
                    };
                    *last_inbound_seq = seq;

                    let (request_seq, request) = match codec::decode_request(inner) {
                        Ok(x) => x,
                        Err(e) => {
                            debug!(?e, "post-auth inner decode failed");
                            return;
                        }
                    };

                    // Heartbeats / Subscribes / control messages are
                    // filtered before the engine sees them. Same
                    // filter the TCP path uses.
                    if crate::request::should_filter(&request) {
                        return;
                    }

                    if let Err(reason) = crate::request::check_permission(&request, *permission) {
                        debug!(%session_id, reason, "permission denied");
                        return;
                    }

                    let event = crate::request::to_event(&request);
                    let timestamp_ns = wall_clock_nanos();
                    let slot = InputSlot {
                        connection_id: session_id as u64,
                        key_hash: *key_hash,
                        request_seq,
                        sequence: 0, // assigned by journal stage
                        timestamp_ns,
                        event,
                        ..Default::default()
                    };
                    input_producer.publish(slot);
                }
            }
        }
    }
}

/// Wrap an engine response in an envelope and publish it to the
/// session's per-session PublicationLog. No-op if the session
/// disappeared between request and response (handshake timed out,
/// client disconnected, etc.).
fn handle_outbound(
    slot: &OutputSlot,
    sessions: &mut HashMap<u32, AuthStage>,
    response_buf: &mut [u8],
    envelope_buf: &mut [u8],
    shutdown: &AtomicBool,
) {
    let session_id = slot.connection_id as u32;

    let Some(AuthStage::Authenticated {
        token,
        outbound_seq,
        pub_log,
        ..
    }) = sessions.get_mut(&session_id)
    else {
        // Session unknown or still in handshake — drop the response.
        // Should be rare: the engine doesn't produce responses for
        // pre-auth traffic, since pre-auth requests never reach the
        // engine in the first place.
        debug!(%session_id, "outbound: no authenticated session, dropping");
        return;
    };

    let kind = match slot.payload {
        OutputPayload::Report(report) => ResponseKind::Report(report),
        OutputPayload::QueryResponse(QueryResponse::Position {
            account,
            balances,
            count,
        }) => ResponseKind::PositionSnapshot {
            account,
            balances,
            count,
        },
        OutputPayload::QueryResponse(QueryResponse::Stats {
            active_connections,
            events_processed,
            journal_sequence,
        }) => ResponseKind::StatsHeader {
            active_connections,
            events_processed,
            journal_sequence,
        },
        OutputPayload::BatchEnd => ResponseKind::BatchEnd,
        OutputPayload::EngineError => ResponseKind::EngineError,
    };

    let written = match codec::encode_response(&kind, response_buf) {
        Ok(n) => n,
        Err(e) => {
            error!(error = ?e, "failed to encode response");
            return;
        }
    };
    // Strip the 4-byte length prefix the codec writes for TCP
    // byte-stream framing — rumcast frames per-message and the
    // envelope wraps the codec body directly.
    let inner = &response_buf[4..written];

    *outbound_seq += 1;
    let env_len = match encode_envelope(token, session_id, *outbound_seq, inner, envelope_buf) {
        Ok(n) => n,
        Err(e) => {
            error!(error = ?e, "failed to encode envelope");
            return;
        }
    };
    let envelope = &envelope_buf[..env_len];

    spin_publish(pub_log, envelope, shutdown);
}

/// Encode + publish a handshake-stage response (Challenge,
/// ServerReady, AuthFailed) **without** envelope wrapping — the
/// client hasn't yet completed the handshake when these arrive, so
/// no shared token exists to MAC them with.
///
/// Returns `Some(())` on success, `None` if shutdown was signalled
/// while spinning on `try_claim`.
fn encode_and_publish_unwrapped(
    response: &ResponseKind,
    pub_log: &PublicationLog,
    encode_buf: &mut [u8],
    shutdown: &AtomicBool,
) -> Option<()> {
    let written = codec::encode_response(response, encode_buf)
        .map_err(|e| error!(?e, "encode handshake response"))
        .ok()?;
    // Strip the 4-byte length prefix — rumcast frames per-message.
    let payload = &encode_buf[4..written];
    spin_publish(pub_log, payload, shutdown)
}

/// Spin on `try_claim` until the publisher accepts the fragment or
/// shutdown is signalled. Single-producer log so backpressure is
/// rare; when it does happen it's brief.
fn spin_publish(pub_log: &PublicationLog, payload: &[u8], shutdown: &AtomicBool) -> Option<()> {
    loop {
        match pub_log.try_claim(payload.len() as u32) {
            Ok(mut claim) => {
                claim.payload_mut().copy_from_slice(payload);
                claim.publish(data_flags::UNFRAGMENTED);
                return Some(());
            }
            Err(_) => {
                if shutdown.load(Ordering::Acquire) {
                    return None;
                }
                std::hint::spin_loop();
            }
        }
    }
}

/// Generate a fresh nonce + ephemeral X25519 keypair for a
/// Challenge frame. Returns `None` if the OS RNG (`getrandom`) is
/// unavailable — should never happen on Linux but we surface it
/// rather than panic.
fn generate_challenge_material() -> Option<([u8; 32], X25519Secret, [u8; 32])> {
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).ok()?;
    let mut secret_bytes = [0u8; 32];
    getrandom::fill(&mut secret_bytes).ok()?;
    let secret = X25519Secret::from(secret_bytes);
    let public = X25519Public::from(&secret).to_bytes();
    Some((nonce, secret, public))
}

/// FxHash of the client's Ed25519 public key — non-cryptographic but
/// fast, used for the engine's per-key idempotency dedup table.
/// Same scheme as the TCP path so a key authenticated over either
/// transport hashes to the same bucket.
fn compute_key_hash(public_key: &[u8; 32]) -> u64 {
    let mut hasher = rustc_hash::FxHasher::default();
    public_key.hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Seed instruments and accounts on first startup, then wait for the
/// pipeline's journal + matching cursors to drain past the last seed
/// event. Inlined from `run_as_primary`'s seeding block — Phase 1 only
/// supports a subset (no replication ring drain wait, no event
/// publisher) so the inlined version stays small.
fn seed_and_drain(
    input_producer: &mut melin_disruptor::ring::Producer<InputSlot>,
    journal_cursor: &Arc<melin_disruptor::padding::Sequence>,
    matching_cursor: &Arc<melin_disruptor::padding::Sequence>,
    instruments: u32,
    accounts: u32,
    shutdown: &AtomicBool,
) {
    use melin_journal::trace::trace_ts;
    use melin_journal::wall_clock_nanos as journal_wall_clock_nanos;
    use melin_trading::trading_event::TradingEvent;
    use melin_trading::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};

    let seed_start = std::time::Instant::now();

    // Instruments first — accounts may need them present.
    for i in 0..instruments {
        input_producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: journal_wall_clock_nanos(),
            event: JournalEvent::App(TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(i),
                    base: CurrencyId(i * 2),
                    quote: CurrencyId(i * 2 + 1),
                },
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
    }

    let mut last_published_seq = 0u64;
    for acct in 1..=accounts {
        last_published_seq = input_producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: journal_wall_clock_nanos(),
            event: JournalEvent::App(TradingEvent::ProvisionAccount {
                account: AccountId(acct),
                amount: u64::MAX / 4,
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
    }

    // Wait for both stages to drain past the last seed event.
    let target = last_published_seq + 1;
    info!(
        instruments,
        accounts, target, "seeding: waiting for pipeline to drain"
    );
    while !shutdown.load(Ordering::Relaxed)
        && (journal_cursor.get().load(Ordering::Acquire) < target
            || matching_cursor.get().load(Ordering::Acquire) < target)
    {
        std::hint::spin_loop();
    }
    info!(elapsed = ?seed_start.elapsed(), "seeding complete");
}

fn wall_clock_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
