//! Server with rumcast (reliable UDP) as the order-entry transport.
//! Mutually exclusive with the `dpdk` feature at build time.
//!
//! # What this is for
//!
//! Lets the LAN bench suite (`melin-bench`) run end-to-end against a
//! full UDP cluster — primary, replica(s), and bench all over rumcast.
//!
//! Scope (post-Phase 4):
//!
//! - **Primary or replica** mode (selected by `--replica-of`). Primary
//!   side spawns a separate rumcast replication endpoint on
//!   `--replication-bind` when set, parallel to the order-entry
//!   endpoint on `--bind`. Replica side connects out to the primary's
//!   replication endpoint; promotion handoff is not yet wired (an
//!   operator-triggered promotion errors out and asks for a process
//!   restart in primary mode).
//! - **Pure-UDP order-entry authentication** via Ed25519 challenge-
//!   response + X25519 ECDH, with per-message BLAKE3 keyed-MAC
//!   envelopes on the data plane. Same Ed25519 identities as the TCP
//!   path (`authorized_keys`).
//! - **Multi-client demux.** Each client picks its own random
//!   `session_id`; the muxed receiver allocates a per-session
//!   `SubscriptionLog` lazily on first contact, the muxed sender
//!   allocates a per-session `PublicationLog` at the handshake-
//!   completion event. Bounded by `MAX_SESSIONS`. Each session's
//!   response dst is auto-discovered from the source addr of the
//!   client's first inbound frame — this requires the client to use
//!   `melin_rumcast::shared_udp::SharedUdp` so its publisher source
//!   addr equals its subscriber addr (single socket per peer).
//! - **Replication** uses the same Ed25519 challenge-response that
//!   `replication/auth.rs` runs on the TCP path, but inlined in
//!   `replication/rumcast_sender.rs` to operate on rumcast message
//!   payloads. No envelope wrapping for replication: it runs on a
//!   separate UDP port operators firewall to internal/VLAN, the same
//!   threat model the TCP path already accepts.
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
use melin_rumcast::shared_udp::SharedUdp;
use melin_rumcast::transport::{KernelUdp, UdpTransport};
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
    /// `last_activity_at` is refreshed on every successful envelope
    /// verify (whether or not the inner request reaches the engine —
    /// post-auth Heartbeats count as activity); the idle-GC sweep
    /// reaps entries whose `last_activity_at` is older than
    /// [`IDLE_TIMEOUT`].
    Authenticated {
        token: [u8; 32],
        key_hash: u64,
        permission: Permission,
        last_inbound_seq: u64,
        outbound_seq: u64,
        pub_log: Arc<PublicationLog>,
        last_activity_at: Instant,
    },
}

/// Drop a Challenged entry if the client takes longer than this to
/// reply with a ChallengeResponse. Bounds the memory an unauthenticated
/// peer can pin by spamming Heartbeats from new session_ids.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Drop an Authenticated entry if no valid envelope has been
/// received in this long. Bounds memory for clients that
/// silently disappear (process killed, network partition,
/// laptop closed) without going through any clean shutdown.
/// Long enough that a heartbeat-only client (no order traffic)
/// keeps the session alive — post-auth Heartbeats refresh the
/// timer because envelope-verification happens before
/// `should_filter`.
///
/// TODO(config): trading deployments may want longer (clients
/// quietly tracking market state during off-hours); embedded
/// deployments may want shorter. Promote to `ServerConfig`.
const IDLE_TIMEOUT: Duration = Duration::from_secs(300);

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
/// for its receive sublog plus the same for its send publog, so this
/// directly controls the per-session memory budget.
///
/// 1 MiB is comfortable for the bench's typical workloads (a few
/// hundred KiB in flight). At fully-saturated 10 Gbps with 1ms NAK
/// reaction (`bandwidth × delay = 1.25 GB/s × 1ms = 1.25 MiB`), it's
/// borderline — a fully-saturating workload would benefit from 4 MiB.
/// At `MAX_SESSIONS = 1024`, the worst-case fully-allocated memory
/// is `1 MiB × 3 × 2 × 1024 = 6 GiB`; lazy allocation keeps real-
/// world usage at hundreds of MiB.
///
/// Wire-format constraint: must match every client's
/// `PublicationConfig::term_length` exactly. If you change this,
/// update bench/src/rumcast.rs and tests/rumcast_smoke.rs in the
/// same commit.
const TERM_LENGTH: u32 = 1024 * 1024;
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
/// Cap on concurrent clients. With `TERM_LENGTH = 1 MiB` and 3
/// partitions per sublog + 3 per publog, the worst case is
/// `MAX_SESSIONS × 6 × 1 MiB = 6 GiB` at full saturation. Sessions
/// are allocated lazily, so a server with a few dozen real clients
/// uses a few hundred MiB in practice. Past this cap, a fresh
/// inbound `session_id` is rejected at the muxer level
/// (`MuxedReceiver` bumps `sessions_rejected` and drops the frame;
/// `MuxedSender::create_session` returns `SessionsExhausted`).
///
/// TODO(config): production deployments may want this lower (small
/// boxes) or higher (busy gateways). Promote to `ServerConfig`.
const MAX_SESSIONS: u32 = 1024;

/// Requested `SO_RCVBUF` for the orders (inbound) socket. With 16 clients at
/// window 128, up to 2048 datagrams can be in-flight simultaneously; the
/// kernel default (208 KiB) overflows in <100 ms under load. Requires
/// `net.core.rmem_max` >= this value on the host.
const ORDERS_RCVBUF_BYTES: usize = 32 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Entry point for the rumcast server. Dispatches to replica mode if
/// `--replica-of` is set, otherwise runs the primary path.
pub fn run_rumcast(
    config: ServerConfig,
    rumcast_config: RumcastConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(primary_addr) = config.replica_of {
        return run_rumcast_replica(config, rumcast_config, primary_addr, shutdown);
    }
    run_rumcast_primary(config, rumcast_config, shutdown)
}

/// Primary path. With `--replication-bind` set, also spawns the
/// rumcast replication sender on a separate UDP port.
fn run_rumcast_primary(
    config: ServerConfig,
    rumcast_config: RumcastConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        bind = %rumcast_config.bind,
        replication_bind = ?config.replication_bind,
        "starting rumcast primary"
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

    // ---- Bind order-entry socket BEFORE engine init ----
    //
    // `init_engine` can take hundreds of ms (journal create + first
    // fsync). Binding the kernel UDP socket first lets the kernel
    // queue any client packets that arrive during init instead of
    // dropping them with ICMP Port Unreachable. Without this, a
    // client connecting at server startup loses its first Hello/
    // Heartbeat to the unbound-port race. Kernel rcvbuf absorbs
    // the burst; frames are drained once the session translator
    // thread starts.
    let orders = SharedUdp::bind(rumcast_config.bind)?;
    if let Err(e) = orders.set_recv_buffer_bytes(ORDERS_RCVBUF_BYTES) {
        warn!(error = ?e, requested = ORDERS_RCVBUF_BYTES, "failed to bump pre-bound orders socket SO_RCVBUF");
    }

    // ---- Engine pipeline ----
    let (app, writer, needs_seeding) = init_engine(&config)?;

    // Spawn the admin listener once if configured. PROMOTE is rejected
    // on a primary (no flag wired); ROTATE shares the flag the journal
    // stage will observe inside `run_rumcast_primary_with_state`.
    let rotate_flag = config.admin_bind.map(|_| Arc::new(AtomicBool::new(false)));
    let _admin_handle = config.admin_bind.map(|addr| {
        crate::admin::spawn(
            addr,
            None,
            rotate_flag.clone(),
            Arc::clone(&shutdown),
            Arc::clone(&authorized_keys),
        )
    });

    run_rumcast_primary_with_state(
        config,
        rumcast_config,
        shutdown,
        authorized_keys,
        app,
        writer,
        needs_seeding,
        Some(orders),
        rotate_flag,
    )
}

/// Same as [`run_rumcast_primary`] but takes an existing
/// `(App, JournalWriter)` instead of calling `init_engine`. Used by the
/// promotion path: when `run_receiver_rumcast` returns `Some(state)` on
/// promote, the replica calls into this with its already-replayed
/// state rather than re-recovering from the journal.
#[allow(clippy::too_many_arguments)]
fn run_rumcast_primary_with_state(
    config: ServerConfig,
    rumcast_config: RumcastConfig,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
    app: crate::App,
    writer: crate::JournalWriter,
    needs_seeding: bool,
    // Pre-bound order-entry socket. The startup path binds before
    // `init_engine` so clients connecting during journal creation
    // don't lose packets to an unbound port. The promotion path
    // (replica → primary) passes `None` and binds here — promotion
    // happens after a failover where clients are already retrying.
    pre_bound_orders_socket: Option<SharedUdp<KernelUdp>>,
    // Shared admin rotation flag (None when no admin endpoint is
    // configured). On the promotion path this is the same Arc the
    // replica's journal stage observed; the new primary's stage picks
    // up where it left off.
    rotate_flag: Option<Arc<AtomicBool>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let enable_replication = config.replication_bind.is_some();
    if enable_replication && config.standalone {
        return Err("--replication-bind and --standalone are mutually exclusive".into());
    }

    // Read raw genesis entry bytes before the writer is consumed by
    // the pipeline. Sent to the replica via StreamStart so the BLAKE3
    // hash chain starts from byte-identical bytes on both sides.
    let genesis_entry = if enable_replication {
        extract_genesis_entry(writer.path())?
    } else {
        Vec::new()
    };

    // Shadow snapshot stage: enabled when --snapshot-interval-ms > 0.
    // Mirrors the TCP path in `server::run_as_primary`. Required so a
    // long-running primary periodically snapshots its state to disk;
    // a fresh replica that connects after journal archives are purged
    // can recover via snapshot transfer.
    //
    // Clones the App via snapshot round-trip BEFORE the pipeline
    // consumes it — App doesn't implement Clone (per-symbol books +
    // dedup table are too complex to derive).
    let enable_shadow = config.snapshot_interval_ms > 0;
    let shadow_exchange = if enable_shadow {
        Some(<crate::App as melin_app::Application>::clone_via_snapshot(
            &app,
        )?)
    } else {
        None
    };

    let active_connections = Arc::new(AtomicU64::new(1));
    let pipeline: Pipeline<crate::App> = build_pipeline_with_replication(
        app,
        writer,
        Duration::from_micros(config.group_commit_us),
        Arc::clone(&active_connections),
        enable_replication,
        config.max_journal_batch,
        config.replication_ring_size,
        !config.yield_idle, // busy_spin
        false,              // enable_event_publisher
        enable_shadow,
    );

    let Pipeline {
        mut input_producer,
        mut journal_stage,
        matching_stage,
        mut output_consumers,
        events_processed,
        input_cursor,
        journal_cursor,
        matching_cursor,
        replication_consumers,
        replication_cursor,
        replicas_connected,
        shadow_consumer,
        chain_hash_lock,
        replication_ring_progress,
        ..
    } = pipeline;

    // Fastest-replica cursor: max(slot0_acked, slot1_acked). Mirrors the
    // TCP path's allocation in `server::run_as_primary`. Read by the
    // response-gate `durable_pos` call in `session_translator`; written
    // by the rumcast replication sender when present (None in the
    // standalone case keeps it at u64::MAX so the gate degrades to the
    // journal-only path).
    let fastest_replica_cursor = Arc::new(AtomicU64::new(u64::MAX));
    let quorum_durability = !config.no_quorum_durability;

    // Wire runtime journal rotation: size threshold + the shared
    // admin flag so `ROTATE` keeps working across a replica → primary
    // transition.
    let max_journal_bytes = config.max_journal_mib.saturating_mul(1024 * 1024);
    journal_stage.set_rotation(max_journal_bytes, rotate_flag);
    if config.max_journal_mib > 0 {
        info!(
            max_journal_mib = config.max_journal_mib,
            "runtime journal rotation enabled (size threshold, rumcast)"
        );
    }

    // Snapshot per-stage utilization handles before the stages move into
    // their threads. The TCP path threads response_utilization through
    // its dedicated response stage; rumcast has no equivalent (the
    // session translator handles inbound + outbound on one thread), so
    // we hand the health endpoint a fresh `StageUtilization` for it —
    // it stays at zero, which is honest: there is no separate response
    // stage to measure.
    let journal_utilization = journal_stage.utilization();
    let matching_utilization = matching_stage.utilization();
    let response_utilization = Arc::new(melin_transport_core::pipeline::StageUtilization::new());
    let pipeline_healthy = Arc::new(AtomicBool::new(true));

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
    let orders = match pre_bound_orders_socket {
        Some(s) => s,
        None => SharedUdp::bind(rumcast_config.bind)?,
    };
    if let Err(e) = orders.set_recv_buffer_bytes(ORDERS_RCVBUF_BYTES) {
        warn!(error = ?e, requested = ORDERS_RCVBUF_BYTES, "failed to bump orders socket SO_RCVBUF");
    }
    // EPERM means CAP_NET_ADMIN missing or net.core.busy_read floor
    // too low — fall back to the regular recv path with a warning
    // rather than refusing to start. Anything else is an ABI bug we
    // want to see immediately.
    if config.rumcast_busy_poll_us > 0
        && let Err(e) = orders.set_busy_poll(config.rumcast_busy_poll_us)
    {
        warn!(
            error = ?e,
            requested_us = config.rumcast_busy_poll_us,
            "failed to enable SO_BUSY_POLL on orders socket; continuing without busy poll"
        );
    }
    // UDP_GRO on the orders socket enables receive-side segment fan-out
    // when peers send via UDP_SEGMENT. ENOPROTOOPT on pre-5.0 kernels
    // is non-fatal — fall back to per-datagram recv with a warning.
    if config.rumcast_udp_gro
        && let Err(e) = orders.set_udp_gro(true)
    {
        warn!(
            error = ?e,
            "failed to enable UDP_GRO on orders socket; continuing without GRO fan-out"
        );
    }
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

    // Bind the response socket to the same IP as the order-entry
    // socket (with an ephemeral port). Responses then carry a source
    // IP the client recognises — the same one it connected to —
    // matching the TCP/DPDK paths where replies naturally come from
    // the listener's bound IP. The previous `127.0.0.1:0` worked
    // for in-process tests but broke on a LAN: responses carried a
    // loopback source addr that couldn't reach a remote client.
    // `0.0.0.0:0` would also work but on multi-NIC hosts the kernel
    // could pick a source IP the client doesn't expect.
    let resp_bind = SocketAddr::new(rumcast_config.bind.ip(), 0);
    let resp = SharedUdp::bind(resp_bind)?;
    // Bump the response socket's kernel recv buffer to absorb bursts
    // of SMs/NAKs from many concurrent subscribers. With 16 clients
    // sending 1 SM per 2 ms, baseline is ~8 k SMs/sec; under load
    // this can spike much higher. The default 200 KB recv buffer
    // overflows in <400 ms during any tick gap, kernel-drops NAKs,
    // and stalls rumcast's flow control. 64 MB gives headroom for
    // multi-second drain hiccups. Kernel caps at `net.core.rmem_max`
    // — operators must raise that sysctl for the full request to
    // take effect; we log a warning if the kernel returns a smaller
    // effective size than requested.
    const RESP_RCVBUF_BYTES: usize = 64 * 1024 * 1024;
    if let Err(e) = resp.set_recv_buffer_bytes(RESP_RCVBUF_BYTES) {
        warn!(error = ?e, requested = RESP_RCVBUF_BYTES, "failed to bump response socket SO_RCVBUF");
    }
    if config.rumcast_busy_poll_us > 0
        && let Err(e) = resp.set_busy_poll(config.rumcast_busy_poll_us)
    {
        warn!(
            error = ?e,
            requested_us = config.rumcast_busy_poll_us,
            "failed to enable SO_BUSY_POLL on response socket; continuing without busy poll"
        );
    }

    let muxed_sender_config = MuxedSenderConfig {
        stream_id: RUMCAST_RESP_STREAM,
        initial_term_id: INITIAL_TERM_ID,
        term_length: TERM_LENGTH,
        mtu: MTU,
        setup_interval: Duration::from_millis(100),
        heartbeat_interval: Duration::from_millis(50),
        max_drain_per_tick: 1024 * 1024,
        // Symmetric with the receiver side's `max_recv_per_tick`. SMs
        // and NAKs from N clients arrive on the response socket at
        // `N / sm_interval` rate; with N=16 clients and a 2 ms
        // interval that's ~8 k SMs/sec. A single recv-side tick can
        // drain up to 1024 inbound order packets and take ~ms, during
        // which SMs accumulate in the kernel rmem buffer. Capping
        // drain at 32 here means the server falls behind, the rmem
        // cap (~212 KB) fills, the kernel drops further SMs, and
        // MuxedSender's flow control stops advancing the publisher
        // limit — the response pub_log fills and the whole pipeline
        // deadlocks. `drain_control` exits early when there's nothing
        // to read, so a high cap costs nothing on idle.
        max_control_per_tick: 1024,
        // Phase 3: leave flow control as `Min` (the rumcast default).
        // We don't expect to be flow-control-bound on a healthy LAN
        // — backpressure here would stem from a slow subscriber, and
        // we'd rather the client see backpressure than the server
        // accumulate unbounded buffers.
        flow_control: melin_rumcast::flow_control::FlowControl::Min,
        max_sessions: MAX_SESSIONS,
    };

    // Shared counters (helpful for bench observability; cheap when nobody reads).
    let counters = Arc::new(Counters::new());

    // ---- Thread plumbing ----

    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();

    // Pipeline: journal stage.
    let journal_shutdown = Arc::clone(&shutdown);
    let journal_core = config.cores.journal;
    handles.push(
        thread::Builder::new()
            .name("journal".into())
            .spawn(move || {
                crate::affinity::pin_thread("journal", journal_core);
                if let Err(e) = journal_stage.run(&journal_shutdown) {
                    error!(error = ?e, "journal stage exited with error");
                }
            })?,
    );

    // Pipeline: matching stage.
    let matching_shutdown = Arc::clone(&shutdown);
    let matching_core = config.cores.matching;
    handles.push(
        thread::Builder::new()
            .name("matching".into())
            .spawn(move || {
                crate::affinity::pin_thread("matching", matching_core);
                let _final_app = matching_stage.run(&matching_shutdown);
            })?,
    );

    // Pipeline: shadow snapshot stage. Mirrors the TCP path's wiring
    // in `server.rs::run_as_primary`. Periodically writes a snapshot
    // of the matching engine's state to disk so a fresh replica can
    // recover via snapshot transfer when journal archives have been
    // purged.
    if let Some(shadow_cons) = shadow_consumer {
        let snap_path = config.shadow_snapshot_path();
        let interval = Duration::from_millis(config.snapshot_interval_ms);
        let chain_hash =
            chain_hash_lock.ok_or("chain hash lock must be Some when shadow is enabled")?;
        let shadow_ex =
            shadow_exchange.ok_or("shadow exchange must be Some when shadow is enabled")?;
        let shadow_shutdown = Arc::clone(&shutdown);
        let shadow_core = config.cores.shadow;
        let busy_spin = !config.yield_idle;
        handles.push(
            thread::Builder::new()
                .name("shadow".into())
                .spawn(move || {
                    crate::affinity::pin_thread("shadow", shadow_core);
                    crate::shadow::run(
                        shadow_cons,
                        shadow_ex,
                        snap_path,
                        interval,
                        chain_hash,
                        &shadow_shutdown,
                        busy_spin,
                    );
                })?,
        );
        info!(
            interval_ms = config.snapshot_interval_ms,
            path = %config.shadow_snapshot_path().display(),
            "shadow snapshot stage started"
        );
    }

    // ---- Replication sender (rumcast) ----
    //
    // Spawned before seeding because the journal stage starts
    // publishing into the replication ring as soon as a replica
    // enters its Live phase. We don't gate seeding on `replica_ready`:
    // a replica that connects after seeding still catches up via
    // journal-file scan on its handshake — same model as TCP.
    let replica_ready = Arc::new(AtomicBool::new(false));
    // Health-snapshot handles populated below when replication is
    // enabled; left None in standalone. Mirrors the TCP path's
    // shape so the `/healthz` endpoint reports the same fields
    // regardless of transport.
    let mut replication_metrics_for_health: Option<Arc<crate::replication::ReplicationMetrics>> =
        None;
    let mut replication_ring_producer_cursors: Option<
        [Arc<dyn melin_disruptor::ring::QueueCursor>; 2],
    > = None;
    let mut replication_ring_consumer_cursors: Option<
        [Arc<melin_disruptor::padding::Sequence>; 2],
    > = None;
    let mut fastest_replica_cursor_for_health: Option<Arc<AtomicU64>> = None;
    if let Some((repl_consumer_1, repl_consumer_2)) = replication_consumers {
        let repl_bind = config
            .replication_bind
            .ok_or("replication_bind must be set when replication is enabled")?;
        let progress = replication_ring_progress
            .ok_or("replication_ring_progress must be present when replication is enabled")?;
        let connected = replicas_connected
            .clone()
            .ok_or("replicas_connected must be Some when replication is enabled")?;
        let metrics = Arc::new(crate::replication::ReplicationMetrics::default());
        replication_metrics_for_health = Some(Arc::clone(&metrics));
        replication_ring_producer_cursors = Some([
            Arc::clone(&progress.producer_cursors[0]),
            Arc::clone(&progress.producer_cursors[1]),
        ]);
        replication_ring_consumer_cursors = Some([
            Arc::clone(&progress.consumer_cursors[0]),
            Arc::clone(&progress.consumer_cursors[1]),
        ]);

        let s_repl = Arc::clone(&shutdown);
        let ready_flag = Arc::clone(&replica_ready);
        let busy_spin = !config.yield_idle;
        let heartbeat_secs = config.replication_heartbeat_secs;
        let journal_path = config.journal.clone();
        let repl_auth_keys = Arc::clone(&authorized_keys);
        let repl_counters = Arc::clone(&counters);
        let evict_flags = [
            Arc::clone(&progress.evict_flags[0]),
            Arc::clone(&progress.evict_flags[1]),
        ];
        let active_flags = [
            Arc::clone(&progress.active_flags[0]),
            Arc::clone(&progress.active_flags[1]),
        ];
        let cursor = Arc::clone(&replication_cursor);
        // Fastest-replica cursor: max(slot0_acked, slot1_acked). Used by
        // the response-gate path in `session_translator` for quorum
        // durability — an event is durable when both replicas acked OR
        // when journal fsync + the fastest replica acked. Init to
        // u64::MAX so `min(journal, u64::MAX) = journal` when no
        // replicas are connected.
        let fastest_for_sender = Arc::clone(&fastest_replica_cursor);
        fastest_replica_cursor_for_health = Some(Arc::clone(&fastest_for_sender));
        let connected_for_thread = Arc::clone(&connected);
        let ready_for_thread = Arc::clone(&ready_flag);
        let s_for_thread = Arc::clone(&s_repl);
        let repl_sender_core = config.cores.repl_sender;

        handles.push(
            thread::Builder::new()
                .name("repl-rumcast-sender".into())
                .spawn(move || {
                    crate::affinity::pin_thread("repl-rumcast-sender", repl_sender_core);
                    crate::replication::run_sender_rumcast(
                        crate::replication::RumcastSender {
                            bind_addr: repl_bind,
                            repl_consumer_1,
                            repl_consumer_2,
                            replication_cursor: cursor,
                            fastest_replica_cursor: fastest_for_sender,
                            genesis_entry,
                            journal_path,
                            authorized_keys: repl_auth_keys,
                            evict_flags,
                            active_flags,
                            metrics,
                            heartbeat_secs,
                            busy_spin,
                            counters: Some(repl_counters),
                        },
                        &s_for_thread,
                        &ready_for_thread,
                        &connected_for_thread,
                    );
                })?,
        );
        info!(addr = %repl_bind, "rumcast replication sender thread started");
    } else {
        // Standalone — no replication. Mirror server.rs behavior.
        if !config.standalone && config.replica_of.is_none() {
            info!("running rumcast in standalone mode (no replication)");
        }
    }

    // ---- Health endpoint ----
    //
    // Spawned before seeding so operators (and the failover test
    // harness) can probe `/healthz` to confirm the server has bound
    // its sockets and is past replication setup. Mirrors the TCP
    // path's behaviour — `--health-bind` is no longer silently
    // ignored under `--features rumcast`.
    let health_handle = if let Some(health_addr) = config.health_bind {
        Some(crate::health::spawn(
            health_addr,
            crate::health::HealthState {
                active_connections: Arc::clone(&active_connections),
                events_processed: Arc::clone(&events_processed),
                journal_cursor: Arc::clone(&journal_cursor),
                matching_cursor: Arc::clone(&matching_cursor),
                input_cursor,
                replication_cursor: Arc::clone(&replication_cursor),
                pipeline_healthy: Arc::clone(&pipeline_healthy),
                replicas_connected: replicas_connected.clone(),
                replication_metrics: replication_metrics_for_health,
                replication_ring_producer_cursors,
                replication_ring_consumer_cursors,
                fastest_replica_cursor: fastest_replica_cursor_for_health,
                journal_utilization: Arc::clone(&journal_utilization),
                matching_utilization: Arc::clone(&matching_utilization),
                response_utilization: Arc::clone(&response_utilization),
            },
            Arc::clone(&shutdown),
        )?)
    } else {
        None
    };

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

    // Split both sockets after seeding. SharedUdp demuxes inline —
    // no poller thread to start. Orders socket: recv half handles
    // inbound Data/Setup/HB; unused send half is dropped. Response
    // socket: send half handles outbound Data + inbound NAK/SM;
    // unused recv half is dropped.
    let (_orders_send_unused, orders_recv_half) = orders.split();
    let (resp_send_half, _resp_recv_unused) = resp.split();
    let muxed_receiver = MuxedReceiver::new(orders_recv_half, muxed_receiver_config);
    let muxed_sender = MuxedSender::new(resp_send_half, muxed_sender_config);

    // Session translator: drives the muxed receiver + sender ticks
    // inline AND runs the auth state machine + envelope wrap/verify.
    // One thread for everything keeps the per-session state lock-free
    // and preserves the single-producer contract on every per-session
    // PublicationLog (the sender's tick is the sole reader of each
    // log's publisher_position).
    {
        let shutdown = Arc::clone(&shutdown);
        let cursor = Arc::clone(&journal_cursor);
        let repl_cursor = Arc::clone(&replication_cursor);
        let fastest_cursor = Arc::clone(&fastest_replica_cursor);
        let authorized_keys = Arc::clone(&authorized_keys);
        let mut muxed_receiver = muxed_receiver;
        let mut muxed_sender = muxed_sender;
        muxed_receiver.set_counters(Some(Arc::clone(&counters)));
        muxed_sender.set_counters(Some(Arc::clone(&counters)));
        // Pin to the response core: the session_translator does the
        // response-equivalent work in the rumcast path (TCP/DPDK
        // builds run a separate `response` stage on this core; rumcast
        // collapses recv+match-output+send into one thread).
        let session_core = config.cores.response;
        handles.push(
            thread::Builder::new()
                .name("rumcast-session".into())
                .spawn(move || {
                    crate::affinity::pin_thread("rumcast-session", session_core);
                    session_translator(
                        muxed_receiver,
                        muxed_sender,
                        &mut input_producer,
                        response_consumer,
                        cursor,
                        repl_cursor,
                        fastest_cursor,
                        quorum_durability,
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
    if let Some(h) = health_handle
        && let Err(e) = h.join()
    {
        warn!(?e, "health thread join error");
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
/// Per-stage counters for diagnosing where the rumcast-session loop
/// is starving when the bench hangs. Gated by `RUMCAST_DIAG=1`; printed
/// to stderr every ~1s. Single-threaded — plain `u64` is sufficient
/// (no atomics needed).
#[derive(Default)]
struct DiagCounters {
    iters: u64,
    idle_iters: u64,
    recv_frags: u64,
    recv_bytes: u64,
    recv_dropped: u64,
    recv_errors: u64,
    setups_recv: u64,
    naks_sent: u64,
    sms_sent: u64,
    send_frags: u64,
    inbound_drained: u64,
    outputs_consumed: u64,
    outputs_seed_dropped: u64,
    outputs_journal_blocked: u64,
    encode_attempts: u64,
    encode_returned_none: u64,
    publish_inline_ok: u64,
    publish_inline_backpressured: u64,
    publish_inline_no_session: u64,
    publish_pending_ok: u64,
    publish_pending_backpressured: u64,
    publish_pending_session_evicted: u64,
    pending_publish_held: u64,
    pending_outbound_held: u64,
    // Per-stage cumulative wall time, ns. Lets us see which of the
    // four serialised stages owns the rumcast-session thread. Each
    // measurement adds two `Instant::now()` calls (~20ns on x86 vDSO),
    // tolerable while RUMCAST_DIAG=1 is set.
    recv_tick_ns: u64,
    send_tick_ns: u64,
    poll_ns: u64,
    outbound_ns: u64,
    idle_ns: u64,
}

#[allow(clippy::too_many_arguments)]
fn session_translator<S, R>(
    mut muxed_receiver: MuxedReceiver<R>,
    mut muxed_sender: MuxedSender<S>,
    input_producer: &mut melin_disruptor::ring::Producer<InputSlot>,
    mut output_consumer: melin_disruptor::ring::Consumer<OutputSlot>,
    journal_cursor: Arc<melin_disruptor::padding::Sequence>,
    // Replication-cursor inputs for the response gate. `replication_cursor`
    // = min(slot0_acked, slot1_acked) — both replicas have confirmed up to
    // here. `fastest_replica_cursor` = max(slot0, slot1). Both stay at
    // u64::MAX in standalone mode, which makes `durable_pos` collapse to
    // the journal-only path. See `crate::response::durable_pos` for the
    // formula.
    replication_cursor: Arc<AtomicU64>,
    fastest_replica_cursor: Arc<AtomicU64>,
    quorum_durability: bool,
    authorized_keys: Arc<AuthorizedKeys>,
    shutdown: &AtomicBool,
    yield_idle: bool,
) where
    S: UdpTransport,
    R: UdpTransport,
{
    let mut sessions: HashMap<u32, AuthStage> = HashMap::new();
    // In-flight per-slot progress: a slot expands to up to two wire
    // kinds (payload + trailing BatchEnd via `is_last_in_request`),
    // and we publish them sequentially. The progress survives across
    // outer-loop iterations so a backpressured first publish doesn't
    // lose the trailing BatchEnd. See `PendingOutbound`.
    let mut pending_outbound: Option<PendingOutbound> = None;
    let mut response_buf = vec![0u8; RESPONSE_ENCODE_BUF_SIZE];
    let mut envelope_buf = vec![0u8; ENVELOPE_BUF_SIZE];

    let diag_enabled = std::env::var("RUMCAST_DIAG")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let mut diag = DiagCounters::default();
    let mut diag_last_dump = Instant::now();
    // Encoded envelope awaiting publish on its session's PublicationLog.
    // Set when an outbound slot has been encoded but the rumcast pub_log
    // can't accept it yet (publisher_limit not advanced — typically the
    // subscriber hasn't drained recent fragments). Stored as
    // (session_id, envelope_len_in_envelope_buf) so we don't have to
    // hold an Arc<PublicationLog> across iterations and re-look-up the
    // session each retry. The rumcast-session loop must not block on
    // try_claim — if it did, drain_control on the response socket
    // wouldn't run, SMs would never be processed, publisher_limit would
    // never advance, and the loop would deadlock.
    let mut pending_publish: Option<(u32, usize)> = None;
    // Cached durability position. Mirrors the TCP/DPDK response stages
    // (see `crate::response::run` and `crate::dpdk_response::run`):
    // matching is a *parallel* consumer of the input ring (not gated on
    // journal), so an output slot can land in the output ring before
    // its input is journaled. The gate enforces persist-before-ack.
    // Caching avoids three atomic loads per slot and lets one journal
    // observation cover every slot below the cached position. Persists
    // across outer iterations: when journal advances during tick/poll,
    // the next outbound pass sees the new value on first use.
    let mut cached_durable_pos: u64 = 0;
    // Wall-clock checkpoint for handshake-timeout sweeps. Throttled
    // because the sweep is O(n) over `sessions` and would otherwise
    // run millions of times per second under busy-spin idle.
    let mut last_sweep_at = Instant::now();

    while !shutdown.load(Ordering::Acquire) {
        let mut did_work = false;
        diag.iters += 1;

        // ---- Drive the rumcast wire-layer ticks ----
        //
        // These run inline (no separate threads). `tick()` drains
        // UDP into per-session sublogs / out of per-session publogs
        // and processes NAK/SM control frames. They're cheap when
        // idle and proportional to per-session work otherwise.
        //
        // Per-stage attribution timestamps are taken only when
        // RUMCAST_DIAG=1. Skipping them in production saves 5
        // `Instant::now()` calls per iter — at the ~1.87 M iters/sec
        // busy-spin rate seen at clients=1 window=1 this is ~280 ms/sec
        // of clock_gettime() calls that perf was attributing to the
        // session_translator thread.
        let recv_stats;
        let send_stats;
        // Carries the `Instant` after `muxed_sender.tick()` so the
        // post-poll timing block below can attribute `poll_ns`
        // correctly. `None` when diag is off (no timestamps taken).
        let after_send: Option<Instant>;
        if diag_enabled {
            let stage_start = Instant::now();
            recv_stats = muxed_receiver.tick();
            let after_recv = Instant::now();
            send_stats = muxed_sender.tick();
            let t = Instant::now();
            diag.recv_tick_ns += after_recv.duration_since(stage_start).as_nanos() as u64;
            diag.send_tick_ns += t.duration_since(after_recv).as_nanos() as u64;
            after_send = Some(t);
        } else {
            recv_stats = muxed_receiver.tick();
            send_stats = muxed_sender.tick();
            after_send = None;
        }
        diag.recv_frags += recv_stats.fragments_accepted as u64;
        diag.recv_bytes += recv_stats.bytes_received;
        diag.recv_dropped += recv_stats.fragments_dropped as u64;
        diag.recv_errors += recv_stats.recv_errors as u64;
        diag.setups_recv += recv_stats.setups_received as u64;
        diag.naks_sent += recv_stats.naks_sent as u64;
        diag.sms_sent += recv_stats.sms_sent as u64;
        diag.send_frags += send_stats.fragments_sent as u64;
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
        diag.inbound_drained += drained as u64;
        if drained > 0 {
            did_work = true;
        }
        for sid in to_evict.drain(..) {
            sessions.remove(&sid);
            muxed_receiver.evict(sid);
            muxed_sender.evict(sid);
        }

        // ---- Drop stale sessions (Challenged + idle Authenticated) ----
        //
        // Throttled to once per `SWEEP_INTERVAL`. Two cases:
        // 1. `Challenged` entries older than `HANDSHAKE_TIMEOUT`:
        //    a half-handshaked client failed to complete auth; reap
        //    so spamming Heartbeats from random session_ids can't
        //    pin per-session memory.
        // 2. `Authenticated` entries idle longer than `IDLE_TIMEOUT`:
        //    a client silently disappeared (process killed, network
        //    partition, laptop closed) without clean shutdown.
        //    Heartbeat traffic refreshes the timer, so a passive
        //    client only sending keepalives stays alive.
        //
        // Both cases evict from the auth table AND both muxers
        // — otherwise the session's sublog/publog memory would pin
        // until process restart.
        // `now` is the wall-clock reference for the sweep below; the
        // sweep is throttled by `SWEEP_INTERVAL` so we always need a
        // real `Instant`. When diag is on it doubles as the post-poll
        // boundary for `poll_ns` attribution.
        let now = Instant::now();
        if let Some(t) = after_send {
            diag.poll_ns += now.duration_since(t).as_nanos() as u64;
        }
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
                AuthStage::Authenticated {
                    last_activity_at, ..
                } => {
                    if now.duration_since(*last_activity_at) >= IDLE_TIMEOUT {
                        debug!(
                            %session_id,
                            idle_for = ?now.duration_since(*last_activity_at),
                            "session idle past IDLE_TIMEOUT; dropping"
                        );
                        expired.push(*session_id);
                        false
                    } else {
                        true
                    }
                }
            });
            for sid in expired {
                muxed_receiver.evict(sid);
                muxed_sender.evict(sid);
            }
            last_sweep_at = now;
        }

        // ---- Outbound: engine output ring → envelope → PublicationLog ----
        //
        // Two-stage pending: first wait for the journal cursor to catch
        // up to the slot (persist-before-ack), then encode the envelope
        // and try to publish it. If the rumcast pub_log can't accept it
        // (publisher_limit not advanced because the subscriber hasn't
        // drained yet), keep the encoded envelope in `pending_publish`
        // and retry next iteration — never block, since this thread
        // also drains the SMs that advance publisher_limit.

        // Outbound coalescing: drain all available OutputSlots into
        // per-session publogs in one iter so the next `tick` ships them
        // in a single sendmmsg. The loop exits naturally when the output
        // ring is empty (try_consume returns None) or publisher_limit is
        // exhausted (try_claim returns Err). No artificial cap — a fixed
        // batch size throttles throughput proportionally to RTT (e.g. a
        // cap of 32 at 1.7K iters/sec yields only 55K responses/sec ÷ 2
        // responses/order = ~28K orders/sec regardless of window size).
        loop {
            // Stage 2: drain a previously-encoded envelope first. The
            // envelope corresponds to `pending_outbound`'s
            // `next_kind` — on success we advance that cursor so the
            // next iteration encodes the *following* kind (e.g. the
            // trailing BatchEnd after a Report).
            if let Some((session_id, env_len)) = pending_publish {
                diag.pending_publish_held += 1;
                match sessions.get(&session_id) {
                    Some(AuthStage::Authenticated { pub_log, .. }) => {
                        match pub_log.try_claim(env_len as u32) {
                            Ok(mut claim) => {
                                claim
                                    .payload_mut()
                                    .copy_from_slice(&envelope_buf[..env_len]);
                                claim.publish(data_flags::UNFRAGMENTED);
                                pending_publish = None;
                                diag.publish_pending_ok += 1;
                                did_work = true;
                                if let Some(prog) = pending_outbound.as_mut() {
                                    prog.next_kind += 1;
                                    if prog.next_kind >= prog.total {
                                        pending_outbound = None;
                                    }
                                }
                            }
                            Err(_) => {
                                // Publog still full — `tick` needs to drain
                                // it before we can publish anything else.
                                diag.publish_pending_backpressured += 1;
                                break;
                            }
                        }
                    }
                    _ => {
                        // Session vanished — drop the encoded envelope
                        // *and* the in-flight slot it belongs to,
                        // since the trailing kinds (if any) target the
                        // same gone session.
                        pending_publish = None;
                        pending_outbound = None;
                        diag.publish_pending_session_evicted += 1;
                    }
                }
            }

            // Stage 1: encode + publish the next kind. Skipped when a
            // pending envelope still occupies envelope_buf.
            if pending_publish.is_some() {
                break;
            }

            // Acquire a fresh slot if none is in flight, expanding it
            // into its (up to two) wire kinds up front.
            if pending_outbound.is_none() {
                let Some((_, slot)) = output_consumer.try_consume() else {
                    // Nothing to consume — coalescing is done for this iter.
                    break;
                };
                diag.outputs_consumed += 1;
                did_work = true;
                // Seed events (connection_id=0) come from `seed_and_drain`.
                // No client to route them to — drop.
                if slot.connection_id == 0 {
                    diag.outputs_seed_dropped += 1;
                    continue;
                }
                let (kinds, total) = slot_to_kinds(&slot);
                if total == 0 {
                    // Defensive: a slot with neither a payload nor
                    // `is_last_in_request` produces no wire output.
                    // The matching stage never emits these today.
                    continue;
                }
                pending_outbound = Some(PendingOutbound {
                    slot,
                    kinds,
                    total,
                    next_kind: 0,
                    durable: false,
                });
            }
            let prog = pending_outbound
                .as_mut()
                .expect("pending_outbound just set above");

            // Durability gate. Once a slot is durable, all of its
            // kinds inherit the gate — recheck only on the first kind.
            // `needed` is the cursor value at which `slot.input_seq`
            // is confirmed durable (journal_cursor is "next sequence
            // to be read" — it must reach input_seq+1).
            if !prog.durable {
                let needed = prog.slot.input_seq + 1;
                if cached_durable_pos < needed {
                    cached_durable_pos = crate::response::durable_pos(
                        journal_cursor.get().load(Ordering::Acquire),
                        replication_cursor.load(Ordering::Acquire),
                        fastest_replica_cursor.load(Ordering::Acquire),
                        quorum_durability,
                    );
                    if cached_durable_pos < needed {
                        // Bounded spin. Matching publishes outputs in
                        // parallel with the journal, so the journal often
                        // catches up within tens of µs. Spinning here
                        // avoids the ~200µs round-trip back through
                        // tick()+poll() before we could re-check.
                        // Cap is generous enough to cover one journal
                        // fsync (~35µs) but well under one outer iter.
                        const SPIN_RECHECKS: u32 = 64;
                        const PAUSES_PER_RECHECK: u32 = 1024;
                        let mut still_blocked = true;
                        'spin: for _ in 0..SPIN_RECHECKS {
                            for _ in 0..PAUSES_PER_RECHECK {
                                std::hint::spin_loop();
                            }
                            cached_durable_pos = crate::response::durable_pos(
                                journal_cursor.get().load(Ordering::Acquire),
                                replication_cursor.load(Ordering::Acquire),
                                fastest_replica_cursor.load(Ordering::Acquire),
                                quorum_durability,
                            );
                            if cached_durable_pos >= needed {
                                still_blocked = false;
                                break 'spin;
                            }
                        }
                        if still_blocked {
                            // Journal still behind after the spin. Defer
                            // to the next outer iter so tick()+poll() get
                            // CPU and the journal thread isn't starved by
                            // an unbounded spin on the same socket.
                            diag.outputs_journal_blocked += 1;
                            diag.pending_outbound_held += 1;
                            break;
                        }
                    }
                }
                prog.durable = true;
            }

            // Encode the next kind.
            diag.encode_attempts += 1;
            let kind = prog.kinds[prog.next_kind as usize];
            let session_id = prog.slot.connection_id as u32;
            let Some(env_len) = encode_outbound(
                session_id,
                &kind,
                &mut sessions,
                &mut response_buf,
                &mut envelope_buf,
            ) else {
                // Encoding failed (session unknown / codec error) —
                // drop the whole slot; subsequent kinds for this
                // session would fail the same way.
                diag.encode_returned_none += 1;
                pending_outbound = None;
                did_work = true;
                continue;
            };
            // Try the first publish inline so the common (uncongested)
            // case avoids a loop iteration.
            match sessions.get(&session_id) {
                Some(AuthStage::Authenticated { pub_log, .. }) => {
                    match pub_log.try_claim(env_len as u32) {
                        Ok(mut claim) => {
                            claim
                                .payload_mut()
                                .copy_from_slice(&envelope_buf[..env_len]);
                            claim.publish(data_flags::UNFRAGMENTED);
                            diag.publish_inline_ok += 1;
                            prog.next_kind += 1;
                            if prog.next_kind >= prog.total {
                                pending_outbound = None;
                            }
                        }
                        Err(_) => {
                            // Pub_log full — defer; outer loop will
                            // retry stage 2 next iter once tick drains.
                            // `prog.next_kind` stays put: stage 2 will
                            // advance it on successful publish.
                            pending_publish = Some((session_id, env_len));
                            diag.publish_inline_backpressured += 1;
                        }
                    }
                }
                _ => {
                    // Race: session evicted between encode and publish —
                    // drop the slot, no point encoding subsequent kinds
                    // for the same gone session.
                    diag.publish_inline_no_session += 1;
                    pending_outbound = None;
                }
            }
            did_work = true;
        }

        // Outbound + idle attribution — only sampled when diag is on.
        // `now` is the post-poll boundary captured above for the
        // sweep; reusing it avoids an extra `Instant::now()` here.
        let after_outbound = if diag_enabled {
            let t = Instant::now();
            diag.outbound_ns += t.duration_since(now).as_nanos() as u64;
            Some(t)
        } else {
            None
        };

        // ---- Idle ----
        if !did_work {
            diag.idle_iters += 1;
            if yield_idle {
                thread::sleep(Duration::from_micros(10));
            } else {
                std::hint::spin_loop();
            }
            if let Some(t) = after_outbound {
                diag.idle_ns += t.elapsed().as_nanos() as u64;
            }
        }

        // ---- Diagnostic dump ----
        if diag_enabled {
            let now = Instant::now();
            if now.duration_since(diag_last_dump) >= Duration::from_secs(1) {
                // ms per stage = ns / 1_000_000. Sum across stages
                // approximates per-second wall time on this thread; gap
                // vs 1000ms is `Instant::now()` overhead + uninstrumented
                // bookkeeping (counter accumulation, did_work, sweep).
                let to_ms = |ns: u64| ns as f64 / 1_000_000.0;
                eprintln!(
                    "[rumcast-diag] iters={} idle={} \
                     recv_ms={:.1} send_ms={:.1} poll_ms={:.1} out_ms={:.1} idle_ms={:.1} \
                     recv_frags={} recv_bytes={} \
                     recv_dropped={} recv_errors={} setups_recv={} naks_sent={} \
                     sms_sent={} send_frags={} inbound_drained={} outputs_consumed={} \
                     seed_dropped={} journal_blocked={} encode_attempts={} encode_none={} \
                     pub_inline_ok={} pub_inline_bp={} pub_inline_nosess={} \
                     pub_pending_ok={} pub_pending_bp={} pub_pending_evict={} \
                     pending_publish_held={} pending_outbound_held={} sessions={}",
                    diag.iters,
                    diag.idle_iters,
                    to_ms(diag.recv_tick_ns),
                    to_ms(diag.send_tick_ns),
                    to_ms(diag.poll_ns),
                    to_ms(diag.outbound_ns),
                    to_ms(diag.idle_ns),
                    diag.recv_frags,
                    diag.recv_bytes,
                    diag.recv_dropped,
                    diag.recv_errors,
                    diag.setups_recv,
                    diag.naks_sent,
                    diag.sms_sent,
                    diag.send_frags,
                    diag.inbound_drained,
                    diag.outputs_consumed,
                    diag.outputs_seed_dropped,
                    diag.outputs_journal_blocked,
                    diag.encode_attempts,
                    diag.encode_returned_none,
                    diag.publish_inline_ok,
                    diag.publish_inline_backpressured,
                    diag.publish_inline_no_session,
                    diag.publish_pending_ok,
                    diag.publish_pending_backpressured,
                    diag.publish_pending_session_evicted,
                    diag.pending_publish_held,
                    diag.pending_outbound_held,
                    sessions.len(),
                );
                diag = DiagCounters::default();
                diag_last_dump = now;
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
fn handle_inbound<S: UdpTransport>(
    session_id: u32,
    src_addr: SocketAddr,
    payload: &[u8],
    sessions: &mut HashMap<u32, AuthStage>,
    authorized_keys: &AuthorizedKeys,
    input_producer: &mut melin_disruptor::ring::Producer<InputSlot>,
    muxed_sender: &mut MuxedSender<S>,
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
                        last_activity_at: Instant::now(),
                    };
                }
                AuthStage::Authenticated {
                    token,
                    key_hash,
                    permission,
                    last_inbound_seq,
                    last_activity_at,
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
                    // Refresh idle-GC timer on every successful
                    // verify, before the should_filter / engine-
                    // dispatch path. Heartbeats and other filtered
                    // requests count as activity — a heartbeat-only
                    // client keeps its session alive.
                    *last_activity_at = Instant::now();

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
/// Encode an outbound slot into an envelope inside `envelope_buf`.
/// Expand an `OutputSlot` into the up-to-two wire `ResponseKind`s
/// the rumcast subscriber should see: the payload (if any) and a
/// trailing `BatchEnd` when `is_last_in_request` is set. Returns the
/// populated count (`0..=2`); the array is filled from index 0.
///
/// `OutputPayload::BatchEnd` carries no payload of its own — the wire
/// `BatchEnd` is emitted from the `is_last_in_request` flag, mirroring
/// the TCP/DPDK response stages.
fn slot_to_kinds(slot: &OutputSlot) -> ([ResponseKind; 2], u8) {
    let mut kinds: [ResponseKind; 2] = [ResponseKind::BatchEnd; 2];
    let mut len: u8 = 0;
    match slot.payload {
        OutputPayload::Report(report) => {
            kinds[len as usize] = ResponseKind::Report(report);
            len += 1;
        }
        OutputPayload::QueryResponse(QueryResponse::Position {
            account,
            balances,
            count,
        }) => {
            kinds[len as usize] = ResponseKind::PositionSnapshot {
                account,
                balances,
                count,
            };
            len += 1;
        }
        OutputPayload::QueryResponse(QueryResponse::Stats {
            active_connections,
            events_processed,
            journal_sequence,
        }) => {
            kinds[len as usize] = ResponseKind::StatsHeader {
                active_connections,
                events_processed,
                journal_sequence,
            };
            len += 1;
        }
        OutputPayload::QueryResponse(QueryResponse::RequestSeqHwm { hwm }) => {
            kinds[len as usize] = ResponseKind::RequestSeqHwm { hwm };
            len += 1;
        }
        OutputPayload::BatchEnd => {
            // No payload — terminator is emitted via is_last_in_request below.
        }
        OutputPayload::EngineError => {
            kinds[len as usize] = ResponseKind::EngineError;
            len += 1;
        }
    }
    if slot.is_last_in_request {
        kinds[len as usize] = ResponseKind::BatchEnd;
        len += 1;
    }
    (kinds, len)
}

/// Per-slot outbound progress. Carries the originating slot, the
/// pre-computed wire kinds it expands to, and a cursor for which kind
/// to encode next. Set by stage 1 the first time it touches a slot,
/// advanced after each successful publish, cleared when all kinds
/// have been published. Persists across loop iterations so a
/// backpressured publish (`pending_publish`) doesn't lose track of
/// the trailing `BatchEnd` for that slot.
struct PendingOutbound {
    slot: OutputSlot,
    kinds: [ResponseKind; 2],
    total: u8,
    next_kind: u8,
    /// `true` once the durability gate has been satisfied for this
    /// slot. Recomputing it for every kind would issue redundant
    /// atomic loads (durability only progresses forward).
    durable: bool,
}

/// Encode one wire `ResponseKind` into an envelope for `session_id`.
///
/// Returns the envelope length on success, or `None` if the session
/// isn't authenticated or encoding fails. The caller is responsible
/// for publishing the bytes to the session's PublicationLog — kept
/// separate from encoding so the publish can be retried across loop
/// iterations without re-encoding (which would re-bump `outbound_seq`
/// and create a sequence gap on the receiver).
fn encode_outbound(
    session_id: u32,
    kind: &ResponseKind,
    sessions: &mut HashMap<u32, AuthStage>,
    response_buf: &mut [u8],
    envelope_buf: &mut [u8],
) -> Option<usize> {
    let Some(AuthStage::Authenticated {
        token,
        outbound_seq,
        ..
    }) = sessions.get_mut(&session_id)
    else {
        // Session unknown or still in handshake — drop the response.
        // Should be rare: the engine doesn't produce responses for
        // pre-auth traffic, since pre-auth requests never reach the
        // engine in the first place.
        debug!(%session_id, "outbound: no authenticated session, dropping");
        return None;
    };

    let written = match codec::encode_response(kind, response_buf) {
        Ok(n) => n,
        Err(e) => {
            error!(error = ?e, "failed to encode response");
            return None;
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
            return None;
        }
    };
    Some(env_len)
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

// ---------------------------------------------------------------------------
// Replica path
// ---------------------------------------------------------------------------

/// Replica-mode entry point. Dispatched from `run_rumcast` when
/// `--replica-of` is set. Connects to the primary via rumcast,
/// authenticates via Ed25519 challenge-response, and runs the streaming
/// receive loop. On promotion, transitions back into the primary path.
fn run_rumcast_replica(
    config: ServerConfig,
    rumcast_config: RumcastConfig,
    primary_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        primary = %primary_addr,
        bind = %rumcast_config.bind,
        "starting in rumcast replica mode"
    );

    // Load the replication signing key — same `--replication-key` flag
    // the TCP path uses; replication identity is transport-independent.
    let replication_key_path = config.replication_key.as_ref().ok_or_else(|| {
        std::io::Error::other("--replication-key is required in replica mode (--replica-of)")
    })?;
    let signing_key = {
        let seed = std::fs::read(replication_key_path).map_err(|e| {
            std::io::Error::other(format!(
                "failed to read replication key {}: {e}",
                replication_key_path.display()
            ))
        })?;
        if seed.len() != 32 {
            return Err(format!(
                "replication key must be 32 bytes, got {} ({})",
                seed.len(),
                replication_key_path.display()
            )
            .into());
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&seed);
        ed25519_dalek::SigningKey::from_bytes(&bytes)
    };

    // Authorized keys are needed both by the receiver-side promotion
    // listener (TCP path uses these for operator-key challenge-
    // response) and by the post-promotion primary mode. We load once
    // and pass through.
    let authorized_keys = Arc::new(AuthorizedKeys::load(&config.authorized_keys)?);
    info!(
        keys = authorized_keys.len(),
        path = %config.authorized_keys.display(),
        "loaded authorized_keys (replica mode)"
    );

    // Admin listener — same TCP-based operator interface the kernel TCP
    // replica path uses. The replication data plane runs over UDP, but
    // PROMOTE/ROTATE stay on TCP so existing tooling doesn't need to
    // learn the rumcast wire format.
    let promote_flag = Arc::new(AtomicBool::new(false));
    let rotate_flag = config.admin_bind.map(|_| Arc::new(AtomicBool::new(false)));
    let _admin_handle = config.admin_bind.map(|addr| {
        crate::admin::spawn(
            addr,
            Some(Arc::clone(&promote_flag)),
            rotate_flag.clone(),
            Arc::clone(&shutdown),
            Arc::clone(&authorized_keys),
        )
    });

    let max_journal_bytes = config.max_journal_mib.saturating_mul(1024 * 1024);
    let rotation = match (max_journal_bytes, rotate_flag.as_ref()) {
        (0, None) => None,
        (b, f) => Some((
            b,
            f.cloned()
                .unwrap_or_else(|| Arc::new(AtomicBool::new(false))),
        )),
    };

    // The replica's local UDP bind for rumcast. We reuse the `--bind`
    // flag (= `rumcast_config.bind`) — operators get one knob to
    // configure the replica's local UDP address rather than two.
    match crate::replication::run_receiver_rumcast(
        primary_addr,
        rumcast_config.bind,
        &config.journal,
        &signing_key,
        &shutdown,
        &promote_flag,
        config.snapshot_interval_ms,
        config.shadow_snapshot_path(),
        config.cores,
        config.async_replica_ack,
        !config.yield_idle,
        rotation,
        config.max_orders_per_account,
        config.max_orders_per_second,
        config.max_orders_burst,
    )? {
        None => Ok(()), // clean shutdown
        Some((mut exchange, writer)) => {
            // Promotion! Transition to primary mode using the already-
            // replayed `(exchange, writer)` rather than calling
            // `init_engine`. Mirrors the TCP path's
            // `run_with_shutdown` → `run_as_primary` handoff.
            info!("rumcast replica promoted — transitioning to primary");
            <crate::App as melin_app::Application>::prefault(&mut exchange);
            run_rumcast_primary_with_state(
                config,
                rumcast_config,
                shutdown,
                authorized_keys,
                exchange,
                writer,
                false, // no seeding — state already replayed from primary
                None,  // bind inside — promotion has no startup-race risk
                rotate_flag,
            )
        }
    }
}

/// Read the raw genesis entry bytes from a journal file. Used to seed
/// the `StreamStart` message so the replica writes a byte-identical
/// genesis entry. Mirrors the inline logic in `server::run_as_primary`.
fn extract_genesis_entry(
    journal_path: &std::path::Path,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    use melin_journal::codec::FILE_HEADER_SIZE;
    let file_bytes = std::fs::read(journal_path)?;
    let offset = FILE_HEADER_SIZE;
    if file_bytes.len() < offset + 4 {
        return Err("journal file too short to contain genesis entry".into());
    }
    let entry_len = u16::from_le_bytes([file_bytes[offset + 2], file_bytes[offset + 3]]) as usize;
    let total = 20 + entry_len + 4; // header(20) + payload + crc(4)
    if file_bytes.len() < offset + total {
        return Err("journal file truncated at genesis entry".into());
    }
    Ok(file_bytes[offset..offset + total].to_vec())
}
