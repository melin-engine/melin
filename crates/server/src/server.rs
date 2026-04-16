//! Server orchestrator — binds the accept loop, pipeline threads, and reader.
//!
//! On startup:
//! 1. Recovers or creates the `JournaledExchange`.
//! 2. Decomposes it into `(Exchange, JournalWriter)` via `into_parts()`.
//! 3. Builds the disruptor pipeline (input ring + output ring).
//! 4. Spawns 3-5 OS threads: journal, matching, response, [repl-sender], [event-publisher].
//! 5. Runs the accept loop, registering connections with the io_uring reader.
//!
//! Fully synchronous — no async runtime needed. Reader threads use io_uring
//! with multishot RECV to multiplex connections, eliminating thread
//! oversubscription. The response thread writes via io_uring SEND.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use std::hash::{Hash, Hasher};

use tracing::{debug, error, info, warn};

use melin_engine::exchange::Exchange;
use melin_engine::journal::JournaledExchange;
use melin_engine::journal::error::JournalError;
use melin_engine::journal::pipeline::{Pipeline, build_pipeline_with_replication};
use melin_engine::journal::writer::JournalWriter;

use melin_protocol::auth::{AuthorizedKeys, Permission};
use melin_protocol::blocking::BlockingFrameWriter;
use melin_protocol::message::ConnectionId;
use melin_protocol::transport::BlockingTransportListener;

/// Server configuration, parsed from CLI arguments via clap.
#[derive(clap::Parser)]
#[command(name = "melin-server", about = "Low-latency matching engine server")]
pub struct ServerConfig {
    /// Address to bind the TCP listener.
    #[arg(long, default_value = "127.0.0.1:9876")]
    pub bind: SocketAddr,
    /// Path to the journal file for durable event sourcing.
    #[arg(long, default_value = "melin.journal")]
    pub journal: PathBuf,
    /// Path to a snapshot file for faster recovery.
    #[arg(long)]
    pub snapshot: Option<PathBuf>,
    /// Pipeline core IDs: journal,matching,response,repl-sender,event-publisher,shadow,repl-handler-0,repl-handler-1
    /// (comma-separated). Core 0 is reserved for OS/IRQ handling.
    /// repl-sender is used when replication is enabled, event-publisher when
    /// `--event-bind` is set, shadow when `--snapshot-interval-secs` > 0.
    /// repl-handler-0/1 are for the per-replica TCP handler threads (0 = unpinned).
    #[arg(long, default_value = "1,2,3,6,7,8,9,10", value_parser = parse_cores)]
    pub cores: PipelineCores,
    /// Number of io_uring reader threads (kernel TCP only). Ignored in
    /// DPDK mode, which uses a single poll thread on core `reader-cores`.
    #[arg(long, default_value_t = 2)]
    pub readers: usize,
    /// First CPU core for reader/poll thread pinning. In TCP mode, reader
    /// thread i is pinned to core reader_cores + i. In DPDK mode, the
    /// single poll thread is pinned to this core.
    #[arg(long, default_value_t = 4)]
    pub reader_cores: usize,
    /// Group commit coalescing delay in microseconds. Keep at 0 for TCP.
    #[arg(long, default_value_t = 0)]
    pub group_commit_us: u64,
    /// Heartbeat interval in seconds. The server sends a heartbeat to idle
    /// connections after this many seconds of silence. Set to 0 to disable.
    #[arg(long, default_value_t = 10)]
    pub heartbeat_interval_secs: u64,
    /// Connection timeout in seconds. The server disconnects clients that
    /// have not sent any data within this window. Set to 0 to disable.
    #[arg(long, default_value_t = 30)]
    pub connection_timeout_secs: u64,
    /// Maximum number of concurrent authenticated connections. New
    /// connections are rejected (closed immediately) when this limit is
    /// reached. 0 means unlimited. Prevents fd/memory exhaustion (SEC-02).
    #[arg(long, default_value_t = 1024)]
    pub max_connections: u64,
    /// Number of accounts to seed on first startup. Uses the
    /// ProvisionAccount event for O(accounts) seeding (~0.5s for 1M).
    #[arg(long, default_value_t = 100_000)]
    pub accounts: u32,
    /// Number of instruments to seed on first startup.
    #[arg(long, default_value_t = 100)]
    pub instruments: u32,
    /// Path to the authorized keys file for Ed25519 challenge-response
    /// authentication. Every connection must authenticate before trading.
    /// Required for primary mode; ignored in replica mode (--replica-of).
    /// See `AuthorizedKeys` for file format.
    #[arg(long, default_value = "authorized_keys")]
    pub authorized_keys: PathBuf,
    /// Maximum journal size in MiB before automatic rotation at startup.
    /// When the journal exceeds this threshold, the server saves a snapshot
    /// and starts a fresh journal. Set to 0 to disable. Default: 256 MiB.
    #[arg(long, default_value_t = 256)]
    pub max_journal_mib: u64,

    /// Address to listen for replica connections (enables synchronous replication).
    /// Mutually exclusive with `--standalone` and `--replica-of`.
    #[arg(long)]
    pub replication_bind: Option<std::net::SocketAddr>,

    /// Disable replication (dev/test mode). Sets the replication cursor to
    /// `u64::MAX` so `min(journal_cursor, MAX) = journal_cursor`.
    /// Mutually exclusive with `--replication-bind` and `--replica-of`.
    #[arg(long, default_value_t = false)]
    pub standalone: bool,

    /// Run as a replica connected to the given primary address.
    /// In replica mode, the server does not accept client connections.
    /// Mutually exclusive with `--replication-bind` and `--standalone`.
    #[arg(long)]
    pub replica_of: Option<std::net::SocketAddr>,

    /// Path to the Ed25519 private key for replication authentication.
    /// Required in replica mode (`--replica-of`). The corresponding
    /// public key must be listed in the primary's authorized_keys file
    /// with permission `replication`.
    #[arg(long)]
    pub replication_key: Option<std::path::PathBuf>,

    /// Maximum number of replication ring batches to coalesce into a
    /// single TCP write+flush. Higher values reduce syscall overhead
    /// but increase per-write latency. Default: 32.
    #[arg(long, default_value_t = 32)]
    pub replication_batch_size: usize,

    /// Maximum events per journal fsync batch. Smaller values reduce
    /// tail latency (less work per sync), larger values improve throughput
    /// (fewer fsyncs). Default: 4096.
    #[arg(long, default_value_t = 4096)]
    pub max_journal_batch: usize,

    /// Replication heartbeat interval in seconds. The primary sends a
    /// heartbeat to the replica after this many seconds of idle. Used
    /// for disconnect detection. Default: 5.
    #[arg(long, default_value_t = 5)]
    pub replication_heartbeat_secs: u64,

    /// Acknowledge replicated batches as soon as they are received and
    /// queued for the local journal stage, instead of waiting for the
    /// local fsync to complete. Removes ~50–80µs of fsync latency from
    /// the replication round-trip, lifting steady-state throughput.
    ///
    /// Durability tradeoff: a replica crash before its local fsync can
    /// lose recently-acked batches from that replica's journal. The
    /// primary still has them on disk and re-syncs the replica via the
    /// catch-up protocol on reconnect, so end-to-end no data is lost
    /// unless the primary's disk also fails simultaneously. Acceptable
    /// for venues where dual-disk-failure is mitigated by separate means
    /// (RAID, three-way replication, off-site journaling).
    #[arg(long, default_value_t = false)]
    pub async_replica_ack: bool,

    /// Number of slots in each replication ring buffer. Must be a power
    /// of two. Each slot holds up to 512 KiB. More slots = more buffering
    /// before eviction. Default: 256 (128 MiB per ring, 256 MiB dual-repl).
    #[arg(long, default_value_t = 256)]
    pub replication_ring_size: usize,

    /// Disable quorum-based durability. By default, when 2 replicas have
    /// acked an event the response stage sends without waiting for the local
    /// journal fsync — removing NVMe tail latency from the critical path.
    /// The journal still writes (for local crash recovery) but does not gate
    /// client responses. Falls back to fsync-gated mode automatically when
    /// fewer than 2 replicas are connected. This flag forces fsync-gated
    /// mode unconditionally (useful for debugging).
    #[arg(long, default_value_t = false)]
    pub no_quorum_durability: bool,

    /// Yield to the OS scheduler when pipeline threads are idle instead
    /// of busy-spinning. Use on shared machines without isolated cores to
    /// avoid starving other processes. Default (no flag) is busy-spin,
    /// which gives lowest latency on isolated cores (isolcpus).
    #[arg(long, default_value_t = false)]
    pub yield_idle: bool,

    // --- DPDK configuration (only used with --features dpdk) ---
    /// DPDK EAL arguments (space-separated). Example: "-l 0-7 --huge-dir /dev/hugepages".
    /// Passed directly to rte_eal_init. Only used when compiled with --features dpdk.
    #[arg(long, default_value = "", allow_hyphen_values = true)]
    pub dpdk_eal_args: String,

    /// DPDK port IDs (comma-separated). For LACP bonds, pass both VF ports
    /// (e.g., "0,1") so traffic arriving on either bond member is received.
    #[arg(long, default_value = "0", value_delimiter = ',')]
    pub dpdk_ports: Vec<u16>,

    /// IPv4 address for the DPDK interface (e.g., "10.0.0.1").
    #[arg(long, default_value = "10.0.0.1")]
    pub dpdk_ip: String,

    /// IPv4 prefix length for the DPDK interface. Default: 24.
    #[arg(long, default_value_t = 24)]
    pub dpdk_prefix_len: u8,

    /// IPv4 gateway for the DPDK interface (optional, needed for cross-subnet traffic).
    #[arg(long)]
    pub dpdk_gateway: Option<String>,

    /// MTU for the DPDK interface. Use 9000 for jumbo frames (6x fewer TCP
    /// segments). Requires switch and PF MTU to be set accordingly.
    #[arg(long, default_value_t = 1500)]
    pub dpdk_mtu: usize,

    /// VLAN ID for hardware VLAN strip/insert. Required in dedicated NIC
    /// mode (dpdk-setup-dedicated.sh) where the kernel doesn't handle VLAN
    /// tags. Not needed for SR-IOV mode (the PF handles VLAN tagging).
    #[arg(long)]
    pub dpdk_vlan: Option<u16>,

    /// Address for the output event publisher. Subscribers connect here
    /// to receive a real-time stream of all execution events (market data,
    /// fills, cancellations). Ed25519 auth required (ReadOnly or above).
    /// Omit to disable (ring has 1 consumer — identical to before).
    #[arg(long)]
    pub event_bind: Option<SocketAddr>,

    /// Address for the health/liveness TCP endpoint. On connect, returns
    /// a one-line status (`OK <conns> <journal_seq> <repl_lag>\n`) and
    /// closes. No auth required. Set to empty string to disable.
    #[arg(long, default_value = "127.0.0.1:9878")]
    pub health_bind: Option<SocketAddr>,

    /// TCP address for the promotion trigger endpoint (replica only).
    /// An operator connects and sends `PROMOTE\n` to promote the replica
    /// to primary. Ignored in primary mode.
    #[arg(long)]
    pub promote_bind: Option<SocketAddr>,

    /// Interval in seconds between automatic shadow snapshots. The shadow
    /// stage replays journaled events on a cloned Exchange and saves a
    /// consistent snapshot at this cadence — no hot-path stall. Set to 0
    /// to disable shadow snapshots entirely.
    #[arg(long, default_value_t = 3000)]
    pub snapshot_interval_secs: u64,

    /// Path for shadow snapshots. Defaults to the journal path with a
    /// `.snapshot` extension (same as the startup snapshot path).
    #[arg(long)]
    pub snapshot_path: Option<PathBuf>,
}

/// Delegates to clap so `#[arg(default_value...)]` is the single source of
/// truth for every default.  Used by the bench crate for struct-literal
/// construction with `..ServerConfig::default()`.
impl Default for ServerConfig {
    fn default() -> Self {
        // On the dpdk branch we spell out every field so that dpdk-specific
        // fields (not known to clap on plain builds) get their defaults.
        // Main uses `Self::parse_from(["melin-server"])` — the values below
        // mirror those clap defaults plus the dpdk extras.
        Self {
            bind: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9876),
            journal: PathBuf::from("melin.journal"),
            snapshot: None,
            cores: PipelineCores {
                journal: 1,
                matching: 2,
                response: 3,
                repl_sender: 6,
                event_publisher: 7,
                shadow: 8,
                repl_handler_0: 9,
                repl_handler_1: 10,
            },
            readers: 2,
            reader_cores: 4,
            group_commit_us: 0,
            heartbeat_interval_secs: 10,
            connection_timeout_secs: 30,
            max_connections: 1024,
            accounts: 2,
            instruments: 2,
            authorized_keys: PathBuf::from("authorized_keys"),
            max_journal_mib: 256,
            replication_bind: None,
            standalone: false,
            replica_of: None,
            replication_key: None,
            replication_batch_size: 32,
            max_journal_batch: 1024,
            replication_heartbeat_secs: 5,
            async_replica_ack: false,
            replication_ring_size: 256,
            no_quorum_durability: false,
            yield_idle: false,
            dpdk_eal_args: String::new(),
            dpdk_ports: vec![0],
            dpdk_ip: "10.0.0.1".into(),
            dpdk_prefix_len: 24,
            dpdk_gateway: None,
            dpdk_mtu: 1500,
            dpdk_vlan: None,
            event_bind: None,
            health_bind: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9878)),
            promote_bind: None,
            snapshot_interval_secs: 3000,
            snapshot_path: None,
        }
    }
}

impl ServerConfig {
    /// Group commit delay as a Duration.
    pub fn group_commit_delay(&self) -> std::time::Duration {
        std::time::Duration::from_micros(self.group_commit_us)
    }

    /// Heartbeat interval as a Duration. Returns `None` if disabled (0).
    pub fn heartbeat_interval(&self) -> Option<std::time::Duration> {
        if self.heartbeat_interval_secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(self.heartbeat_interval_secs))
        }
    }

    /// Connection timeout as a Duration. Returns `None` if disabled (0).
    pub fn connection_timeout(&self) -> Option<std::time::Duration> {
        if self.connection_timeout_secs == 0 {
            None
        } else {
            Some(std::time::Duration::from_secs(self.connection_timeout_secs))
        }
    }

    /// Snapshot path for the shadow stage. Uses the explicit `--snapshot-path`
    /// if set, otherwise derives from the journal path with `.snapshot` extension.
    pub fn shadow_snapshot_path(&self) -> PathBuf {
        self.snapshot_path
            .clone()
            .unwrap_or_else(|| self.journal.with_extension("snapshot"))
    }
}

/// Core assignments for pipeline threads.
///
/// Six fields: journal, matching, response, repl-sender, event-publisher,
/// and shadow. All are always stored; repl-sender is only used when
/// replication is enabled, event-publisher only when `--event-bind` is set,
/// and shadow only when `--snapshot-interval-secs` > 0.
#[derive(Debug, Clone, Copy)]
pub struct PipelineCores {
    pub journal: usize,
    pub matching: usize,
    pub response: usize,
    pub repl_sender: usize,
    pub event_publisher: usize,
    pub shadow: usize,
    /// Core for replication handler thread 0. 0 = unpinned (OS scheduled).
    pub repl_handler_0: usize,
    /// Core for replication handler thread 1. 0 = unpinned (OS scheduled).
    pub repl_handler_1: usize,
}

/// Parse "j,m,r,s,e,sh,h0,h1" into `PipelineCores` for pipeline core affinity.
fn parse_cores(s: &str) -> Result<PipelineCores, String> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 8 {
        return Err(format!(
            "expected 8 comma-separated core IDs (journal,matching,response,repl-sender,event-publisher,shadow,repl-handler-0,repl-handler-1), got {}",
            parts.len()
        ));
    }
    let parse = |p: &str| {
        p.parse::<usize>()
            .map_err(|_| format!("invalid core ID: {p}"))
    };
    Ok(PipelineCores {
        journal: parse(parts[0])?,
        matching: parse(parts[1])?,
        response: parse(parts[2])?,
        repl_sender: parse(parts[3])?,
        event_publisher: parse(parts[4])?,
        shadow: parse(parts[5])?,
        repl_handler_0: parse(parts[6])?,
        repl_handler_1: parse(parts[7])?,
    })
}

/// Run the trading server.
///
/// 1. Initializes (or recovers) the `JournaledExchange`, then decomposes
///    it into `Exchange` and `JournalWriter` for the pipeline.
/// 2. Builds the disruptor pipeline (input ring + output ring + stages).
/// 3. Spawns 3 OS threads: journal, matching, response.
/// 4. Runs the accept loop, spawning a reader OS thread per connection.
///
/// Returns when the listener encounters a fatal error.
pub fn run<L: BlockingTransportListener>(
    listener: L,
    config: ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    run_with_shutdown(listener, config, Arc::new(AtomicBool::new(false)))
}

/// Run the trading server with an externally controlled shutdown flag.
///
/// Same as [`run`], but the caller can set `shutdown` to `true` to trigger
/// a clean shutdown of all pipeline threads (useful for benchmarks that need
/// to collect latency trace reports).
pub fn run_with_shutdown<L: BlockingTransportListener>(
    listener: L,
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Replica mode: connect to primary, receive journal stream, replay.
    // Must run before init_engine — the replica's journal is created from
    // the primary's genesis during the replication handshake.
    //
    // TODO: this is a minimal receiver that writes to journal only. For
    // production, the replica should build the full pipeline (Exchange,
    // matching stage, accept loop in dormant state) so it can:
    //   - Be promoted to primary (switch input from replication to clients)
    //   - Serve read-only queries (L2 book snapshots, trade feed)
    //   - Verify state via BLAKE3 hash chain
    if let Some(primary_addr) = config.replica_of {
        info!(primary = %primary_addr, "starting in replica mode");

        // Load replication signing key.
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

        // Load authorized keys early — the promotion listener needs them
        // for Ed25519 challenge-response auth (operator keys only).
        let authorized_keys = Arc::new(AuthorizedKeys::load(&config.authorized_keys)?);
        info!(
            keys = authorized_keys.len(),
            path = %config.authorized_keys.display(),
            "loaded authorized keys (replica mode, for promotion auth)"
        );

        // Spawn promotion listener if configured.
        let promote_flag = Arc::new(AtomicBool::new(false));
        let _promote_handle = config.promote_bind.map(|addr| {
            crate::promote::spawn(
                addr,
                Arc::clone(&promote_flag),
                Arc::clone(&shutdown),
                Arc::clone(&authorized_keys),
            )
        });

        match crate::replication::run_receiver(
            primary_addr,
            &config.journal,
            &signing_key,
            &shutdown,
            &promote_flag,
            config.snapshot_interval_secs,
            config.shadow_snapshot_path(),
            config.cores,
            config.async_replica_ack,
        )? {
            None => return Ok(()), // clean shutdown
            Some((mut exchange, writer)) => {
                // Promotion! Transition to primary mode.
                info!("replica promoted — transitioning to primary");
                exchange.prefault();

                return run_as_primary(
                    exchange,
                    writer,
                    listener,
                    &config,
                    shutdown,
                    authorized_keys,
                    false, // no seeding needed — state comes from replication
                );
            }
        }
    }

    // Load authorized keys for challenge-response authentication.
    let authorized_keys = Arc::new(AuthorizedKeys::load(&config.authorized_keys)?);
    info!(
        keys = authorized_keys.len(),
        path = %config.authorized_keys.display(),
        "loaded authorized keys"
    );

    // Initialize or recover the exchange. `needs_seeding` is true on
    // first startup — seed events will flow through the pipeline later.
    let (engine, needs_seeding) = init_engine(&config)?;

    // Decompose into parts for the pipeline.
    let (mut exchange, writer) = engine.into_parts();

    // Pre-fault all HashMap pages so page faults happen now, not on the hot path.
    exchange.prefault();

    run_as_primary(
        exchange,
        writer,
        listener,
        &config,
        shutdown,
        authorized_keys,
        needs_seeding,
    )
}

/// Run the server as a primary: build the disruptor pipeline, spawn
/// pipeline threads, optionally seed instruments/accounts, then accept
/// client connections.
///
/// Control event for the response stage. The io_uring response path reads
/// `fd` for I/O; the `writer` keeps the fd alive via ownership.
pub enum ControlEvent {
    Connected {
        connection_id: u64,
        fd: std::os::unix::io::RawFd,
        writer: BlockingFrameWriter<Box<dyn std::io::Write + Send>>,
    },
    Disconnected {
        connection_id: u64,
    },
}

/// Joinable handles for every long-lived thread spawned by a primary.
/// Optional handles are `None` when their feature is disabled (e.g.,
/// no replication, no health endpoint) or unsupported on a transport
/// (e.g., DPDK runs without an event publisher or shadow snapshotter).
struct PipelineHandles {
    journal: std::thread::JoinHandle<Result<JournalWriter, JournalError>>,
    matching: std::thread::JoinHandle<Exchange>,
    response: std::thread::JoinHandle<()>,
    replication: Option<std::thread::JoinHandle<()>>,
    event_publisher: Option<std::thread::JoinHandle<()>>,
    shadow: Option<std::thread::JoinHandle<()>>,
    health: Option<std::thread::JoinHandle<()>>,
}

/// Drain the pipeline and join every worker thread, surfacing panics
/// and journal-stage errors as a single `pipeline failure` return.
///
/// `extras` is a list of pre-joined results (used by the DPDK path,
/// which joins its poll threads before draining the pipeline).
fn shutdown_pipeline_stages(
    handles: PipelineHandles,
    extras: Vec<(String, std::thread::Result<()>)>,
    pipeline_healthy: &AtomicBool,
    shutdown: &AtomicBool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("shutdown: draining pipeline");
    pipeline_healthy.store(false, Ordering::Relaxed);
    shutdown.store(true, Ordering::Relaxed);

    let mut thread_panicked = false;
    let mut check_join = |name: &str, result: std::thread::Result<()>| {
        if let Err(panic) = result {
            let msg = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
                .unwrap_or("<non-string panic>");
            error!(thread = name, message = msg, "pipeline thread panicked");
            thread_panicked = true;
        }
    };

    let journal_result = handles.journal.join();
    let journal_failed = matches!(&journal_result, Ok(Err(_)));
    if let Ok(Err(ref e)) = journal_result {
        error!(thread = "journal", error = %e, "journal stage returned error");
    }
    check_join("journal", journal_result.map(|_| ()));
    check_join("matching", handles.matching.join().map(|_| ()));
    check_join("response", handles.response.join());
    for (name, r) in extras {
        check_join(&name, r);
    }
    if let Some(h) = handles.replication {
        check_join("replication-sender", h.join());
    }
    if let Some(h) = handles.event_publisher {
        check_join("event-publisher", h.join());
    }
    if let Some(h) = handles.shadow {
        check_join("shadow", h.join());
    }
    if let Some(h) = handles.health {
        check_join("health", h.join());
    }

    if thread_panicked || journal_failed {
        error!("shutdown complete (with pipeline failure)");
        return Err("pipeline failure".into());
    }

    info!("shutdown complete");
    Ok(())
}

/// Used by both the normal primary startup path and the promotion path
/// (replica → primary transition).
fn run_as_primary<L: BlockingTransportListener>(
    exchange: Exchange,
    writer: JournalWriter,
    mut listener: L,
    config: &ServerConfig,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
    needs_seeding: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Active connection counter shared between accept loop, response
    // stage, and matching stage (for stats queries).
    // Incremented on successful auth, decremented on disconnect.
    // Used to enforce max_connections (SEC-02).
    let active_connections = Arc::new(AtomicU64::new(0));

    // Determine replication mode.
    let enable_replication = config.replication_bind.is_some();
    if enable_replication && config.standalone {
        return Err("--replication-bind and --standalone are mutually exclusive".into());
    }
    // Read the raw genesis entry bytes from the journal file before
    // moving the writer into the pipeline. Sent to the replica during
    // handshake so it can write a byte-identical genesis, ensuring the
    // BLAKE3 hash chain starts from the exact same encoded bytes.
    let genesis_entry = if enable_replication {
        use melin_engine::journal::codec::FILE_HEADER_SIZE;
        let file_bytes = std::fs::read(writer.path())?;
        // Genesis entry starts right after the 8-byte file header.
        // Read entry length from bytes [offset+2..offset+4].
        let offset = FILE_HEADER_SIZE;
        if file_bytes.len() < offset + 4 {
            return Err("journal file too short to contain genesis entry".into());
        }
        let entry_len =
            u16::from_le_bytes([file_bytes[offset + 2], file_bytes[offset + 3]]) as usize;
        let total = 20 + entry_len + 4; // header(20) + payload + crc(4)
        if file_bytes.len() < offset + total {
            return Err("journal file truncated at genesis entry".into());
        }
        file_bytes[offset..offset + total].to_vec()
    } else {
        Vec::new()
    };

    // Clone the exchange for the shadow snapshot stage before the pipeline
    // consumes it. Uses snapshot_state() + restore_state() round-trip since
    // Exchange doesn't implement Clone (internal data structures are complex).
    let enable_shadow = config.snapshot_interval_secs > 0;
    let shadow_exchange = if enable_shadow {
        Some(exchange.clone_via_snapshot())
    } else {
        None
    };

    // Build the disruptor pipeline with optional replication consumer.
    let enable_event_publisher = config.event_bind.is_some();
    let Pipeline {
        input_producer,
        sequencer,
        journal_stage,
        matching_stage,
        mut output_consumers,
        journal_cursor,
        matching_cursor,
        events_processed,
        input_cursor,
        replication_consumers,
        replication_cursor,
        replicas_connected,
        shadow_consumer,
        chain_hash_lock,
        replication_ring_progress,
    } = build_pipeline_with_replication(
        exchange,
        writer,
        config.group_commit_delay(),
        Arc::clone(&active_connections),
        enable_replication,
        config.max_journal_batch,
        config.replication_ring_size,
        !config.yield_idle,
        enable_event_publisher,
        enable_shadow,
    );
    // Fastest-replica cursor: `max(slot0_acked, slot1_acked)`. Used by the
    // response stage for quorum durability — an event is durable if either
    // both replicas acked (replication_cursor) or the journal fsynced and
    // the fastest replica acked (journal_cursor.min(fastest_replica_cursor)).
    // Initialized to u64::MAX so `min(journal, u64::MAX) = journal` when
    // no replicas are connected.
    let fastest_replica_cursor = Arc::new(AtomicU64::new(u64::MAX));

    // Consumer 0 is always the response stage. Consumer 1 (if present)
    // is the event publisher — only created when --event-bind is set.
    let output_consumer = output_consumers.remove(0);
    let event_publisher_consumer = if enable_event_publisher {
        Some(output_consumers.remove(0))
    } else {
        None
    };

    // Control channel for connect/disconnect events → response stage.
    let (control_tx, control_rx) = std::sync::mpsc::channel();

    // Spawn the io_uring reader thread pool. Connections are distributed
    // round-robin across reader threads. Each thread uses io_uring with
    // multishot RECV to multiplex its connections and MultiProducer to
    // publish to the disruptor. With 2 readers (cores 4-5) + 3 pipeline
    // (cores 1-3) = 5 pinned OS threads, no oversubscription.
    let connection_timeout = config.connection_timeout();
    let heartbeat_interval = config.heartbeat_interval();

    // Clone the input producer for seeding. Seed events flow through the
    // disruptor like regular events so they're journaled, replicated, and
    // processed by the matching engine via the normal pipeline.
    let seed_producer = if needs_seeding {
        Some(input_producer.clone())
    } else {
        None
    };

    let reader_shutdown = Arc::new(AtomicBool::new(false));
    let mut reader_handle = crate::reader::spawn_reader_pool(
        config.readers,
        input_producer,
        control_tx.clone(),
        config.reader_cores,
        connection_timeout,
        Arc::clone(&reader_shutdown),
        Arc::clone(&sequencer),
    );

    // Spawn pipeline OS threads.
    let cores = config.cores;

    // Extract utilization handles before stages are moved into threads.
    let journal_utilization = journal_stage.utilization();
    let matching_utilization = matching_stage.utilization();
    let response_utilization = Arc::new(melin_engine::journal::pipeline::StageUtilization::new());

    let s1 = Arc::clone(&shutdown);
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || {
            apply_affinity("journal", cores.journal);
            journal_stage.run(&s1)
        })
        .map_err(|e| format!("spawn journal thread: {e}"))?;

    let s2 = Arc::clone(&shutdown);
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            apply_affinity("matching", cores.matching);
            matching_stage.run(&s2)
        })
        .map_err(|e| format!("spawn matching thread: {e}"))?;

    // Clone cursors for the response thread — the originals are needed
    // later for seed drain gating.
    let journal_cursor_response = Arc::clone(&journal_cursor);
    let replication_cursor_response = Arc::clone(&replication_cursor);
    let fastest_replica_cursor_response = Arc::clone(&fastest_replica_cursor);
    let quorum_durability = !config.no_quorum_durability;
    let s3 = Arc::clone(&shutdown);
    let busy_spin = !config.yield_idle;
    let response_utilization_thread = Arc::clone(&response_utilization);
    let response_handle = std::thread::Builder::new()
        .name("response".into())
        .spawn(move || {
            apply_affinity("response", cores.response);
            crate::response::run(
                output_consumer,
                control_rx,
                crate::response::Response {
                    journal_cursor: journal_cursor_response,
                    replication_cursor: replication_cursor_response,
                    fastest_replica_cursor: fastest_replica_cursor_response,
                    quorum_durability,
                    heartbeat_interval,
                    busy_spin,
                    utilization: response_utilization_thread,
                },
                &s3,
            );
        })
        .map_err(|e| format!("spawn response thread: {e}"))?;

    // Spawn replication sender thread if enabled. The journal stage publishes
    // encoded batches to a pre-allocated ring; the sender thread consumes them.
    // `replica_ready` is set when the first replica connects — seeding waits
    // on this to ensure seed events aren't drained before the replica arrives.
    let replica_ready = Arc::new(AtomicBool::new(false));
    let replication_metrics: Option<Arc<crate::replication::ReplicationMetrics>> =
        if replication_consumers.is_some() {
            Some(Arc::new(crate::replication::ReplicationMetrics::default()))
        } else {
            None
        };
    // Ring depth monitoring: the producer cursors are in ReplicationRingProgress
    // (owned by this function), so we compute depth via ring_progress rather
    // than storing Box<dyn QueueCursor> in ReplicationMetrics. The health
    // snapshot reads consumer cursors from ReplicationMetrics and producer
    // cursors are not needed — ring depth is a secondary metric.

    let replication_handle = if let Some((repl_consumer_1, repl_consumer_2)) = replication_consumers
    {
        let repl_bind = config
            .replication_bind
            .ok_or("replication_bind must be set when replication is enabled")?;
        let s_repl = Arc::clone(&shutdown);
        let repl_cursor = Arc::clone(&replication_cursor);
        let fastest_repl_cursor = Arc::clone(&fastest_replica_cursor);
        let ready_flag = Arc::clone(&replica_ready);
        let connected_counter = replicas_connected
            .clone()
            .ok_or("replicas_connected must be Some when replication is enabled")?;

        let batch_size = config.replication_batch_size;
        let heartbeat_secs = config.replication_heartbeat_secs;
        let journal_path = config.journal.clone();
        let repl_auth_keys = Arc::clone(&authorized_keys);
        let evict_flags = replication_ring_progress
            .as_ref()
            .map(|rp| {
                [
                    Arc::clone(&rp.evict_flags[0]),
                    Arc::clone(&rp.evict_flags[1]),
                ]
            })
            .unwrap_or_else(|| {
                [
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                ]
            });
        let active_flags = replication_ring_progress
            .as_ref()
            .map(|rp| {
                [
                    Arc::clone(&rp.active_flags[0]),
                    Arc::clone(&rp.active_flags[1]),
                ]
            })
            .unwrap_or_else(|| {
                [
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                ]
            });
        let repl_metrics = replication_metrics
            .clone()
            .ok_or("replication_metrics must be Some when replication is enabled")?;
        let handler_cores = [cores.repl_handler_0, cores.repl_handler_1];
        let repl_sender_handle = std::thread::Builder::new()
            .name("repl-sender".into())
            .spawn(move || {
                apply_affinity("repl-sender", cores.repl_sender);
                crate::replication::run_sender(
                    crate::replication::Sender {
                        bind_addr: repl_bind,
                        repl_consumer_1,
                        repl_consumer_2,
                        replication_cursor: repl_cursor,
                        fastest_replica_cursor: fastest_repl_cursor,
                        genesis_entry,
                        journal_path,
                        authorized_keys: repl_auth_keys,
                        evict_flags,
                        active_flags,
                        metrics: repl_metrics,
                        handler_cores,
                        batch_size,
                        heartbeat_secs,
                        busy_spin,
                    },
                    &s_repl,
                    &ready_flag,
                    &connected_counter,
                );
            })
            .map_err(|e| format!("spawn replication sender thread: {e}"))?;

        info!(addr = %repl_bind, "replication listener started");
        Some(repl_sender_handle)
    } else {
        if !config.standalone && config.replica_of.is_none() {
            info!("running in standalone mode (no replication)");
        }
        None
    };

    // Spawn event publisher thread if enabled. Consumes from output ring
    // consumer 1 and broadcasts all execution events to TCP subscribers.
    let event_publisher_handle = if let Some(event_consumer) = event_publisher_consumer {
        let event_bind = config
            .event_bind
            .ok_or("event_bind must be set when event publisher is enabled")?;
        let s_event = Arc::clone(&shutdown);
        let event_keys = Arc::clone(&authorized_keys);
        let event_handle = std::thread::Builder::new()
            .name("event-publisher".into())
            .spawn(move || {
                apply_affinity("event-publisher", cores.event_publisher);
                crate::event_publisher::run(
                    event_consumer,
                    event_bind,
                    event_keys,
                    &s_event,
                    busy_spin,
                );
            })
            .map_err(|e| format!("spawn event publisher thread: {e}"))?;

        info!(addr = %event_bind, "event publisher started");
        Some(event_handle)
    } else {
        None
    };

    // Spawn shadow snapshot thread if enabled. The shadow stage replays
    // journal-fsynced events on a cloned Exchange and saves periodic
    // snapshots — fully off the hot path.
    let shadow_handle = if let Some(shadow_cons) = shadow_consumer {
        let snap_path = config.shadow_snapshot_path();
        let interval = std::time::Duration::from_secs(config.snapshot_interval_secs);
        let chain_hash =
            chain_hash_lock.ok_or("chain hash lock must be Some when shadow is enabled")?;
        let shadow_ex =
            shadow_exchange.ok_or("shadow exchange must be Some when shadow is enabled")?;
        let s_shadow = Arc::clone(&shutdown);
        let handle = std::thread::Builder::new()
            .name("shadow".into())
            .spawn(move || {
                apply_affinity("shadow", cores.shadow);
                crate::shadow::run(
                    shadow_cons,
                    shadow_ex,
                    snap_path,
                    interval,
                    chain_hash,
                    &s_shadow,
                    busy_spin,
                );
            })
            .map_err(|e| format!("spawn shadow thread: {e}"))?;

        info!(
            interval_secs = config.snapshot_interval_secs,
            path = %config.shadow_snapshot_path().display(),
            "shadow snapshot stage started"
        );
        Some(handle)
    } else {
        None
    };

    // Seed instruments and accounts through the pipeline on first startup.
    // Events flow through journal + matching + replication like regular
    // trading events. Must happen after pipeline threads start (they
    // consume from the disruptor) but before accepting client connections.
    //
    // When replication is enabled, wait for the first replica to connect
    // before publishing. replica_ready is set by the replica handler
    // thread after catch-up completes and it enters the live streaming
    // loop — this ensures the ring consumer is actively draining before
    // seeds start flowing.
    if enable_replication && needs_seeding {
        info!("waiting for replica to connect before seeding...");
        while !replica_ready.load(Ordering::Acquire) {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    if let Some(producer) = seed_producer {
        use melin_engine::journal::event::JournalEvent;
        use melin_engine::journal::pipeline::InputSlot;
        use melin_engine::journal::trace::trace_ts;
        use melin_engine::journal::writer::wall_clock_nanos;
        use melin_engine::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};

        let seed_start = std::time::Instant::now();

        for i in 0..config.instruments {
            producer.publish(InputSlot {
                connection_id: 0,
                key_hash: 0,
                request_seq: 0,
                sequence: sequencer.next(),
                timestamp_ns: wall_clock_nanos(),
                event: JournalEvent::AddInstrument {
                    spec: InstrumentSpec {
                        symbol: Symbol(i),
                        base: CurrencyId(i * 2),
                        quote: CurrencyId(i * 2 + 1),
                    },
                },
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        }

        let instrument_elapsed = seed_start.elapsed();

        let account_start = std::time::Instant::now();
        let mut last_published_seq = 0u64;
        for acct in 1..=config.accounts {
            last_published_seq = producer.publish(InputSlot {
                connection_id: 0,
                key_hash: 0,
                request_seq: 0,
                sequence: sequencer.next(),
                timestamp_ns: wall_clock_nanos(),
                event: JournalEvent::ProvisionAccount {
                    account: AccountId(acct),
                    amount: u64::MAX / 4,
                },
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        }
        let publish_elapsed = account_start.elapsed();

        // Wait for all seed events to be fully processed by the pipeline
        // before accepting clients. Without this, early client orders
        // compete with seed events for pipeline time, contaminating
        // benchmark results.
        //
        // Gates on journal + matching cursors (disruptor sequence space),
        // then waits for the replication ring to be fully consumed. This
        // confirms sender threads have read all seed batches from the ring
        // (sent or being sent to replicas). Stronger than no gate, faster
        // than waiting for replica TCP acks, and deadlock-free because the
        // ring backpressures instead of dropping batches.
        let drain_start = std::time::Instant::now();
        let last_seed_seq = last_published_seq + 1; // cursor = next-to-consume

        info!(
            last_seed_seq,
            journal = journal_cursor
                .get()
                .load(std::sync::atomic::Ordering::Relaxed),
            matching = matching_cursor
                .get()
                .load(std::sync::atomic::Ordering::Relaxed),
            "seed drain: waiting for pipeline cursors"
        );

        while journal_cursor
            .get()
            .load(std::sync::atomic::Ordering::Acquire)
            < last_seed_seq
            || matching_cursor
                .get()
                .load(std::sync::atomic::Ordering::Acquire)
                < last_seed_seq
        {
            std::hint::spin_loop();
        }

        info!("seed drain: pipeline cursors reached target");

        // After journal + matching are done, wait for each ACTIVE
        // replication ring's consumer to have read all published batches.
        // Inactive rings (no connected replica) were never published to,
        // so their producer cursor is 0 — no wait needed.
        if let Some(ref ring_progress) = replication_ring_progress {
            for i in 0..ring_progress.producer_cursors.len() {
                if !ring_progress.active_flags[i].load(std::sync::atomic::Ordering::Relaxed) {
                    continue;
                }
                let target = ring_progress.producer_cursors[i].load();
                while ring_progress.consumer_cursors[i]
                    .get()
                    .load(std::sync::atomic::Ordering::Acquire)
                    < target
                {
                    std::hint::spin_loop();
                }
            }
        }

        info!("seed drain: replication rings drained");
        let drain_elapsed = drain_start.elapsed();

        info!(
            accounts = config.accounts,
            instruments = config.instruments,
            instrument_ms = instrument_elapsed.as_millis(),
            publish_ms = publish_elapsed.as_millis(),
            drain_ms = drain_elapsed.as_millis(),
            total_ms = seed_start.elapsed().as_millis(),
            "seeded test data through pipeline"
        );
    }

    // Pipeline health flag: true while all pipeline threads are alive.
    // Flipped to false when a thread dies or on shutdown. Read by the
    // health endpoint to distinguish OK from ERR.
    let pipeline_healthy = Arc::new(AtomicBool::new(true));

    // Spawn health/liveness endpoint before the accept loop so it's
    // reachable as soon as the server is ready to accept connections.
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
                replication_metrics: replication_metrics.clone(),
                journal_utilization: Arc::clone(&journal_utilization),
                matching_utilization: Arc::clone(&matching_utilization),
                response_utilization: Arc::clone(&response_utilization),
            },
            Arc::clone(&shutdown),
        )?)
    } else {
        None
    };

    // Set the listener to non-blocking so accept() returns immediately
    // with WouldBlock when no connection is pending. This lets the accept
    // loop check the shutdown flag without blocking indefinitely.
    // Rust's std TcpListener retries on EINTR, so signals alone can't
    // interrupt a blocking accept().
    listener.set_nonblocking(true);

    info!(addr = %config.bind, "listening");

    // Monotonically increasing connection ID counter. AtomicU64 because
    // the accept loop is the only writer, but using atomic for future
    // flexibility (e.g., multiple listeners).
    let next_connection_id = AtomicU64::new(1);

    // Accept loop — non-blocking with 100ms sleep on WouldBlock. Each
    // accepted connection is registered with the reader thread (no
    // per-connection threads).
    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("shutdown signal received");
            break;
        }

        // Detect pipeline thread death early so we don't keep accepting
        // connections into a broken pipeline. These threads only exit on
        // shutdown or panic — if one is finished while shutdown is false,
        // it panicked. Re-check shutdown to avoid a TOCTOU race where a
        // clean shutdown signal arrives between the two checks.
        let event_pub_died = event_publisher_handle
            .as_ref()
            .is_some_and(|h| h.is_finished());
        let shadow_died = shadow_handle.as_ref().is_some_and(|h| h.is_finished());
        if (journal_handle.is_finished()
            || matching_handle.is_finished()
            || response_handle.is_finished()
            || event_pub_died
            || shadow_died)
            && !shutdown.load(Ordering::Relaxed)
        {
            error!("pipeline thread died, initiating shutdown");
            pipeline_healthy.store(false, Ordering::Relaxed);
            break;
        }

        let (mut std_read, mut std_write, addr) = match listener.accept() {
            Ok(conn) => conn,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    // No pending connection — sleep briefly then retry.
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    continue;
                }
                error!(error = %e, "accept error");
                continue;
            }
        };

        // Enforce max_connections limit (SEC-02). Reject early before
        // spending time on auth. The counter is decremented by the response
        // stage on disconnect or write error.
        if config.max_connections > 0
            && active_connections.load(Ordering::Relaxed) >= config.max_connections
        {
            warn!(addr = %addr, "connection rejected: max_connections reached");
            drop(std_read);
            drop(std_write);
            continue;
        }

        let connection_id = ConnectionId(next_connection_id.fetch_add(1, Ordering::Relaxed));

        debug!(connection_id = connection_id.0, addr = %addr, "new connection");

        // Set a 5-second read timeout for the auth handshake to prevent
        // slow/malicious clients from blocking the accept loop.
        if let Err(e) = set_read_timeout(&std_read, Some(std::time::Duration::from_secs(5))) {
            debug!(connection_id = connection_id.0, error = %e, "failed to set auth timeout");
        }

        // Challenge-response authentication handshake (cold path).
        // 1. Send Challenge with random nonce
        // 2. Read ChallengeResponse (signature + public key)
        // 3. Verify signature and look up key in authorized_keys
        // 4. Send ServerReady on success, AuthFailed on failure
        let (permission, public_key_bytes) = match authenticate_connection(
            connection_id,
            addr,
            &mut std_read,
            &mut std_write,
            &authorized_keys,
        ) {
            Ok(pair) => pair,
            Err(e) => {
                debug!(connection_id = connection_id.0, addr = %addr, error = %e, "auth failed, dropping");
                continue;
            }
        };

        // Hash the client's public key for per-key idempotency dedup.
        // FxHash is fast and non-cryptographic — sufficient for dedup
        // table keying (the public key itself is already authenticated).
        let key_hash = {
            let mut hasher = rustc_hash::FxHasher::default();
            public_key_bytes.hash(&mut hasher);
            hasher.finish()
        };

        active_connections.fetch_add(1, Ordering::Relaxed);

        // Clear the read timeout before handing to the io_uring reader.
        // io_uring uses kernel-managed I/O, so the timeout is irrelevant,
        // but clearing it avoids surprising behavior if the fd is ever
        // used in blocking mode again.
        if let Err(e) = set_read_timeout(&std_read, None) {
            debug!(connection_id = connection_id.0, error = %e, "failed to clear auth timeout");
        }

        // Enable SO_BUSY_POLL on the client data socket. The reader
        // thread already busy-spins on io_uring CQEs, so the kernel's
        // NIC busy-poll happens during cycles that would have been
        // spent spinning anyway — net cost is zero, and we avoid the
        // softirq → wakeup handoff for every recv on this connection.
        if let Err(e) = set_busy_poll(&std_read, BUSY_POLL_US) {
            // Best-effort: a kernel without CAP_NET_ADMIN or with
            // SO_BUSY_POLL disabled by sysctl will reject this. Logged
            // as debug — it's a per-connection event and only affects
            // latency, not correctness.
            debug!(
                connection_id = connection_id.0,
                error = %e,
                "failed to set SO_BUSY_POLL on client socket"
            );
        }

        // Set a write timeout on the response socket so a slow/stalled
        // client cannot block the response thread (SEC-01). If a write
        // takes longer than this, it returns EAGAIN and the response
        // stage drops the connection.
        if let Err(e) = set_write_timeout(&std_write, Some(std::time::Duration::from_secs(5))) {
            debug!(connection_id = connection_id.0, error = %e, "failed to set write timeout");
        }

        // Register the writer with the response thread before the reader.
        // This ensures the response stage has the writer before any
        // requests arrive from this connection.
        let fd = std_write.as_raw_fd();
        let boxed_writer: Box<dyn std::io::Write + Send> = Box::new(std_write);
        let control_event = ControlEvent::Connected {
            connection_id: connection_id.0,
            fd,
            writer: BlockingFrameWriter::new(boxed_writer),
        };
        if control_tx.send(control_event).is_err() {
            info!("response thread gone, shutting down");
            break;
        }

        // Register the reader fd with the io_uring reader thread.
        reader_handle.register(crate::reader::ReaderRegistration {
            connection_id,
            reader: std_read,
            addr,
            permission,
            key_hash,
        });
    }

    // --- Ordered shutdown sequence ---
    // 1. Stop readers first so no new events enter the disruptor.
    info!("shutdown: stopping reader threads");
    reader_handle.shutdown();
    reader_handle.join();

    // 2. Now drain the pipeline and join every worker thread.
    shutdown_pipeline_stages(
        PipelineHandles {
            journal: journal_handle,
            matching: matching_handle,
            response: response_handle,
            replication: replication_handle,
            event_publisher: event_publisher_handle,
            shadow: shadow_handle,
            health: health_handle,
        },
        Vec::new(),
        &pipeline_healthy,
        &shutdown,
    )
}

/// Run the trading server with DPDK kernel-bypass networking.
///
/// Replaces the kernel TCP stack entirely. The DPDK poll thread handles
/// all NIC I/O and TCP processing via smoltcp. The response stage encodes
/// frames and pushes them through an mpsc channel to the poll thread.
///
/// Thread layout:
/// - Core N:   DPDK poll thread (rx_burst, smoltcp, frame decode, tx_burst)
/// - Core 1:   Journal stage
/// - Core 2:   Matching stage
/// - Core 3:   Response stage (encodes to TX channel)
#[cfg(feature = "dpdk")]
pub fn run_dpdk(
    config: ServerConfig,
    dpdk_config: melin_dpdk::DpdkConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize shared DPDK resources (EAL, mempool, ports with N queues).
    let shared = melin_dpdk::DpdkShared::init(&dpdk_config)?;
    // Actual queue count may be less than requested (TAP only supports 1).
    let num_dpdk_threads = shared.num_queues as usize;

    // --- Replica mode (DPDK) ---
    // If --replica-of is set, run the DPDK replication receiver instead
    // of the primary path. The receiver uses one queue pair for the
    // outbound connection to the primary.
    if let Some(primary_addr) = config.replica_of {
        info!(primary = %primary_addr, "starting in replica mode (DPDK)");

        // Load authorized keys early — the promotion listener needs them
        // for Ed25519 challenge-response auth (operator keys only).
        let authorized_keys = Arc::new(AuthorizedKeys::load(&config.authorized_keys)?);
        info!(
            keys = authorized_keys.len(),
            path = %config.authorized_keys.display(),
            "loaded authorized keys (DPDK replica mode, for promotion auth)"
        );

        let promote_flag = Arc::new(AtomicBool::new(false));
        let _promote_handle = config.promote_bind.map(|addr| {
            crate::promote::spawn(
                addr,
                Arc::clone(&promote_flag),
                Arc::clone(&shutdown),
                Arc::clone(&authorized_keys),
            )
        });

        // Use queue 0 for the replication receiver's smoltcp connection.
        // Listen port is unused (receiver connects outbound, doesn't listen),
        // but DpdkTransport requires one — use an ephemeral port.
        let mut repl_transport = melin_dpdk::DpdkTransport::from_shared_with_port(
            &shared,
            &dpdk_config,
            0,
            39999, // Ephemeral — receiver connects outbound, doesn't accept.
        )?;
        repl_transport.send_gratuitous_arp();

        let primary_ipv4 = match primary_addr.ip() {
            std::net::IpAddr::V4(ip) => ip,
            std::net::IpAddr::V6(_) => {
                return Err("DPDK replication requires IPv4".into());
            }
        };

        match crate::replication::run_receiver_dpdk(
            repl_transport,
            primary_ipv4,
            primary_addr.port(),
            &config.journal,
            &shutdown,
            &promote_flag,
            config.snapshot_interval_secs,
            config.shadow_snapshot_path(),
        )? {
            None => return Ok(()), // clean shutdown
            Some((mut exchange, writer)) => {
                // Promotion! Transition to primary mode (DPDK).
                info!("replica promoted (DPDK) — transitioning to primary");
                exchange.prefault();

                // TODO: run_as_primary_dpdk — for now, fall back to
                // kernel TCP primary after promotion.
                warn!("DPDK primary promotion not yet implemented — falling back to kernel TCP");
                let listener = melin_protocol::tcp::BlockingTcpListener::bind(config.bind)?;
                return run_as_primary(
                    exchange,
                    writer,
                    listener,
                    &config,
                    shutdown,
                    authorized_keys,
                    false,
                );
            }
        }
    }

    // Create per-thread transports (each gets its own queue pair + smoltcp stack).
    // In primary mode with replication, the last queue is reserved for the
    // replication sender — only create client transports for queues 0..N-1.
    let num_client_queues = if config.replication_bind.is_some() {
        num_dpdk_threads.saturating_sub(1)
    } else {
        num_dpdk_threads
    };
    let mut transports = Vec::with_capacity(num_client_queues);
    for q in 0..num_client_queues {
        let mut transport =
            melin_dpdk::DpdkTransport::from_shared(&shared, &dpdk_config, q as u16)?;
        // Send a gratuitous ARP from the first queue so the switch learns
        // our VF's MAC address. Without this, SR-IOV VFs that can't enable
        // promiscuous mode are unreachable — the switch has no forwarding
        // entry and drops unicast frames to our MAC.
        if q == 0 {
            transport.send_gratuitous_arp();
        }
        transports.push(transport);
    }

    // Load authorized keys for challenge-response authentication.
    let authorized_keys = Arc::new(AuthorizedKeys::load(&config.authorized_keys)?);
    info!(
        keys = authorized_keys.len(),
        path = %config.authorized_keys.display(),
        "loaded authorized keys"
    );

    // Initialize or recover the exchange.
    let (engine, needs_seeding) = init_engine(&config)?;
    let (mut exchange, writer) = engine.into_parts();
    exchange.prefault();

    // Clone exchange state for the shadow snapshot stage before moving
    // exchange into the pipeline (same as the kernel TCP path).
    let enable_shadow = config.snapshot_interval_secs > 0;
    let shadow_exchange = if enable_shadow {
        Some(exchange.clone_via_snapshot())
    } else {
        None
    };

    let active_connections = Arc::new(AtomicU64::new(0));

    // Replication setup (same as TCP path).
    let enable_replication = config.replication_bind.is_some();
    if enable_replication && config.standalone {
        return Err("--replication-bind and --standalone are mutually exclusive".into());
    }

    let genesis_entry = if enable_replication {
        use melin_engine::journal::codec::FILE_HEADER_SIZE;
        let file_bytes = std::fs::read(writer.path())?;
        let offset = FILE_HEADER_SIZE;
        if file_bytes.len() < offset + 4 {
            return Err("journal file too short to contain genesis entry".into());
        }
        let entry_len =
            u16::from_le_bytes([file_bytes[offset + 2], file_bytes[offset + 3]]) as usize;
        let total = 20 + entry_len + 4;
        if file_bytes.len() < offset + total {
            return Err("journal file truncated at genesis entry".into());
        }
        file_bytes[offset..offset + total].to_vec()
    } else {
        Vec::new()
    };

    // Build disruptor pipeline (same flags as the kernel TCP path).
    let enable_event_publisher = config.event_bind.is_some();
    let enable_shadow = config.snapshot_interval_secs > 0;
    let Pipeline {
        input_producer,
        sequencer,
        journal_stage,
        matching_stage,
        mut output_consumers,
        journal_cursor,
        matching_cursor,
        events_processed,
        input_cursor,
        replication_consumers,
        replication_cursor,
        replicas_connected,
        shadow_consumer,
        chain_hash_lock,
        replication_ring_progress,
    } = build_pipeline_with_replication(
        exchange,
        writer,
        config.group_commit_delay(),
        Arc::clone(&active_connections),
        enable_replication,
        config.max_journal_batch,
        config.replication_ring_size,
        !config.yield_idle,
        enable_event_publisher,
        enable_shadow,
    );

    let heartbeat_interval = config.heartbeat_interval();

    // Clone producer for seeding before moving it to the DPDK thread.
    let seed_producer = if needs_seeding {
        Some(input_producer.clone())
    } else {
        None
    };

    // Fastest-replica cursor (see TCP path for explanation).
    let fastest_replica_cursor = Arc::new(AtomicU64::new(u64::MAX));

    // Control channel: DPDK poll thread → response stage (connect/disconnect).
    let (control_tx, control_rx) = std::sync::mpsc::channel();

    // TX SPSC: response stage → DPDK poll thread (encoded frames).
    // Lock-free, fixed-size slots — no heap allocation per frame.
    // 4096 slots × ~140 bytes = ~560 KiB. Enough to buffer a burst
    // without backpressuring the response stage.
    // One SPSC channel per DPDK poll thread. The response stage routes
    // frames to the correct thread based on thread_id encoded in
    // connection_id bits 56..63.
    let mut tx_producers = Vec::with_capacity(num_dpdk_threads);
    let mut tx_consumers = Vec::with_capacity(num_dpdk_threads);
    for _ in 0..num_dpdk_threads {
        let (tx_out, tx_rx) = melin_disruptor::spsc::channel::<crate::dpdk_response::TxFrame>(4096);
        tx_producers.push(tx_out);
        tx_consumers.push(tx_rx);
    }

    // Spawn pipeline threads (journal, matching — identical to TCP path).
    let cores = config.cores;

    // Extract utilization handles before stages are moved into threads.
    let journal_utilization = journal_stage.utilization();
    let matching_utilization = matching_stage.utilization();
    let response_utilization = Arc::new(melin_engine::journal::pipeline::StageUtilization::new());

    let s1 = Arc::clone(&shutdown);
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || {
            apply_affinity("journal", cores.journal);
            journal_stage.run(&s1)
        })
        .map_err(|e| format!("spawn journal thread: {e}"))?;

    let s2 = Arc::clone(&shutdown);
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            apply_affinity("matching", cores.matching);
            matching_stage.run(&s2)
        })
        .map_err(|e| format!("spawn matching thread: {e}"))?;

    // Spawn DPDK response stage (encodes to TX channel instead of kernel sockets).
    let output_consumer = output_consumers.remove(0);
    let journal_cursor_response = Arc::clone(&journal_cursor);
    let replication_cursor_response = Arc::clone(&replication_cursor);
    let fastest_replica_cursor_response = Arc::clone(&fastest_replica_cursor);
    let quorum_durability = !config.no_quorum_durability;
    let active_connections_response = Arc::clone(&active_connections);
    let s3 = Arc::clone(&shutdown);
    let response_utilization_thread = Arc::clone(&response_utilization);
    let response_handle = std::thread::Builder::new()
        .name("response".into())
        .spawn(move || {
            apply_affinity("response", cores.response);
            crate::dpdk_response::run(
                output_consumer,
                control_rx,
                journal_cursor_response,
                replication_cursor_response,
                fastest_replica_cursor_response,
                quorum_durability,
                &s3,
                heartbeat_interval,
                active_connections_response,
                tx_producers,
                response_utilization_thread,
            );
        })
        .map_err(|e| format!("spawn response thread: {e}"))?;

    // Spawn shadow snapshot thread if enabled (same as kernel TCP path).
    let busy_spin = !config.yield_idle;
    let shadow_handle = if let Some(shadow_cons) = shadow_consumer {
        let snap_path = config.shadow_snapshot_path();
        let interval = std::time::Duration::from_secs(config.snapshot_interval_secs);
        let chain_hash =
            chain_hash_lock.ok_or("chain hash lock must be Some when shadow is enabled")?;
        let shadow_ex =
            shadow_exchange.ok_or("shadow exchange must be Some when shadow is enabled")?;
        let s_shadow = Arc::clone(&shutdown);
        let handle = std::thread::Builder::new()
            .name("shadow".into())
            .spawn(move || {
                apply_affinity("shadow", cores.shadow);
                crate::shadow::run(
                    shadow_cons,
                    shadow_ex,
                    snap_path,
                    interval,
                    chain_hash,
                    &s_shadow,
                    busy_spin,
                );
            })
            .map_err(|e| format!("spawn shadow thread: {e}"))?;

        info!(
            interval_secs = config.snapshot_interval_secs,
            path = %config.shadow_snapshot_path().display(),
            "shadow snapshot stage started"
        );
        Some(handle)
    } else {
        None
    };

    // Spawn DPDK replication sender if enabled. Uses its own DPDK queue pair
    // and smoltcp stack so the replication channel goes through kernel bypass.
    let replica_ready = Arc::new(AtomicBool::new(false));
    let replication_metrics: Option<Arc<crate::replication::ReplicationMetrics>> =
        if replication_consumers.is_some() {
            Some(Arc::new(crate::replication::ReplicationMetrics::default()))
        } else {
            None
        };
    let replication_handle = if let Some((repl_consumer_1, repl_consumer_2)) = replication_consumers
    {
        let repl_bind = config
            .replication_bind
            .ok_or("replication_bind must be set when replication is enabled")?;
        let repl_port = repl_bind.port();

        // Create a DpdkTransport for the replication sender with its own
        // queue pair. Client transports use queues 0..num_client_queues-1;
        // the replication sender gets the next one (num_client_queues).
        let repl_transport = melin_dpdk::DpdkTransport::from_shared_with_port(
            &shared,
            &dpdk_config,
            num_client_queues as u16,
            repl_port,
        )
        .map_err(|e| format!("create DPDK transport for replication: {e}"))?;

        let s_repl = Arc::clone(&shutdown);
        let repl_cursor = Arc::clone(&replication_cursor);
        let fastest_repl_cursor = Arc::clone(&fastest_replica_cursor);
        let ready_flag = Arc::clone(&replica_ready);
        let batch_size = config.replication_batch_size;
        let heartbeat_secs = config.replication_heartbeat_secs;
        let busy_spin = !config.yield_idle;
        let repl_metrics = replication_metrics
            .clone()
            .ok_or("replication_metrics must be Some when replication is enabled")?;
        let connected_counter = replicas_connected
            .clone()
            .ok_or("replicas_connected must be Some when replication is enabled")?;
        let dpdk_active_flags: [Arc<AtomicBool>; 2] = replication_ring_progress
            .as_ref()
            .map(|rp| {
                [
                    Arc::clone(&rp.active_flags[0]),
                    Arc::clone(&rp.active_flags[1]),
                ]
            })
            .unwrap_or_else(|| {
                [
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                ]
            });
        let dpdk_evict_flags: [Arc<AtomicBool>; 2] = replication_ring_progress
            .as_ref()
            .map(|rp| {
                [
                    Arc::clone(&rp.evict_flags[0]),
                    Arc::clone(&rp.evict_flags[1]),
                ]
            })
            .unwrap_or_else(|| {
                [
                    Arc::new(AtomicBool::new(false)),
                    Arc::new(AtomicBool::new(false)),
                ]
            });
        let journal_path = config.journal.clone();
        let repl_sender_handle = std::thread::Builder::new()
            .name("repl-sender".into())
            .spawn(move || {
                apply_affinity("repl-sender", cores.repl_sender);
                crate::replication::run_sender_dpdk(
                    repl_transport,
                    [repl_consumer_1, repl_consumer_2],
                    repl_cursor,
                    fastest_repl_cursor,
                    genesis_entry,
                    journal_path,
                    &s_repl,
                    &ready_flag,
                    &connected_counter,
                    dpdk_evict_flags,
                    dpdk_active_flags,
                    repl_metrics,
                    batch_size,
                    heartbeat_secs,
                    busy_spin,
                );
            })
            .map_err(|e| format!("spawn replication sender thread: {e}"))?;
        info!(addr = %repl_bind, "DPDK replication sender started (dual-replica)");
        Some(repl_sender_handle)
    } else {
        if !config.standalone && config.replica_of.is_none() {
            info!("running in standalone mode (no replication)");
        }
        None
    };

    // Seed through the pipeline if needed.
    if enable_replication && needs_seeding {
        info!("waiting for replica to connect before seeding...");
        while !replica_ready.load(Ordering::Acquire) {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    if let Some(producer) = seed_producer {
        use melin_engine::journal::event::JournalEvent;
        use melin_engine::journal::pipeline::InputSlot;
        use melin_engine::journal::trace::trace_ts;
        use melin_engine::journal::writer::wall_clock_nanos;
        use melin_engine::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};

        for i in 0..config.instruments {
            producer.publish(InputSlot {
                connection_id: 0,
                key_hash: 0,
                request_seq: 0,
                sequence: sequencer.next(),
                timestamp_ns: wall_clock_nanos(),
                event: JournalEvent::AddInstrument {
                    spec: InstrumentSpec {
                        symbol: Symbol(i),
                        base: CurrencyId(i * 2),
                        quote: CurrencyId(i * 2 + 1),
                    },
                },
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        }

        let mut last_published_seq = 0u64;
        for acct in 1..=config.accounts {
            last_published_seq = producer.publish(InputSlot {
                connection_id: 0,
                key_hash: 0,
                request_seq: 0,
                sequence: sequencer.next(),
                timestamp_ns: wall_clock_nanos(),
                event: JournalEvent::ProvisionAccount {
                    account: AccountId(acct),
                    amount: u64::MAX / 4,
                },
                publish_ts: trace_ts(),
                recv_ts: trace_ts(),
            });
        }

        // Wait for seeding to complete through journal + matching stages,
        // then wait for the replication ring to drain. See TCP path comment.
        let last_seed_seq = last_published_seq + 1;
        while journal_cursor
            .get()
            .load(std::sync::atomic::Ordering::Acquire)
            < last_seed_seq
            || matching_cursor
                .get()
                .load(std::sync::atomic::Ordering::Acquire)
                < last_seed_seq
        {
            std::hint::spin_loop();
        }
        if let Some(ref ring_progress) = replication_ring_progress {
            for i in 0..ring_progress.producer_cursors.len() {
                if !ring_progress.active_flags[i].load(std::sync::atomic::Ordering::Relaxed) {
                    continue;
                }
                let target = ring_progress.producer_cursors[i].load();
                while ring_progress.consumer_cursors[i]
                    .get()
                    .load(std::sync::atomic::Ordering::Acquire)
                    < target
                {
                    std::hint::spin_loop();
                }
            }
        }

        info!(
            accounts = config.accounts,
            instruments = config.instruments,
            "seeded test data through pipeline"
        );
    }

    // Pipeline health flag: true while all pipeline threads are alive.
    let pipeline_healthy = Arc::new(AtomicBool::new(true));

    // Spawn health/liveness endpoint (same as TCP path).
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
                replication_metrics: replication_metrics.clone(),
                journal_utilization: Arc::clone(&journal_utilization),
                matching_utilization: Arc::clone(&matching_utilization),
                response_utilization: Arc::clone(&response_utilization),
            },
            Arc::clone(&shutdown),
        )?)
    } else {
        None
    };

    info!(
        ip = %dpdk_config.ip_addr,
        port = dpdk_config.listen_port,
        num_dpdk_threads,
        "DPDK transport listening"
    );

    // Spawn N-1 DPDK poll threads (queues 1..N). Queue 0 runs on the
    // main thread below. Each thread gets its own transport, SPSC
    // consumer, and a clone of the MultiProducer.
    let connection_timeout = config.connection_timeout();
    let max_conns = config.max_connections;
    let reader_cores = config.reader_cores;
    let mut dpdk_handles = Vec::with_capacity(num_dpdk_threads.saturating_sub(1));

    for i in (1..num_dpdk_threads).rev() {
        let transport_i = transports.pop().expect("transport for thread");
        let tx_rx_i = tx_consumers.pop().expect("SPSC consumer for thread");
        let producer_i = input_producer.clone();
        let control_i = control_tx.clone();
        let shutdown_i = Arc::clone(&shutdown);
        let active_i = Arc::clone(&active_connections);
        let keys_i = Arc::clone(&authorized_keys);

        let sequencer_i = Arc::clone(&sequencer);
        let handle = std::thread::Builder::new()
            .name(format!("dpdk-poll-{i}"))
            .spawn(move || {
                apply_affinity(&format!("dpdk-poll-{i}"), reader_cores + i);
                crate::dpdk_transport::run_dpdk_poll(
                    transport_i,
                    producer_i,
                    control_i,
                    tx_rx_i,
                    &shutdown_i,
                    keys_i,
                    connection_timeout,
                    max_conns,
                    active_i,
                    i as u8,
                    sequencer_i,
                );
            })
            .map_err(|e| format!("spawn DPDK poll thread: {e}"))?;
        dpdk_handles.push(handle);
    }

    if !transports.is_empty() {
        // Queue 0 runs on the main thread.
        apply_affinity("dpdk-poll-0", reader_cores);
        let transport_0 = transports.remove(0);
        let tx_rx_0 = tx_consumers.remove(0);
        crate::dpdk_transport::run_dpdk_poll(
            transport_0,
            input_producer,
            control_tx,
            tx_rx_0,
            &shutdown,
            authorized_keys,
            connection_timeout,
            max_conns,
            Arc::clone(&active_connections),
            0,
            sequencer,
        );
    } else {
        // No client queues (e.g., single-queue NIC with replication taking
        // the only queue). The main thread just waits for shutdown while
        // the replication sender runs on its own thread.
        info!("no client queues available — waiting for shutdown");
        while !shutdown.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    // Join DPDK poll threads before draining the pipeline — they're the
    // ingress producers, so they must stop pushing before journal/matching
    // can drain.
    let dpdk_extras: Vec<(String, std::thread::Result<()>)> = dpdk_handles
        .into_iter()
        .enumerate()
        .map(|(i, h)| (format!("dpdk-poll-{i}"), h.join()))
        .collect();

    shutdown_pipeline_stages(
        PipelineHandles {
            journal: journal_handle,
            matching: matching_handle,
            response: response_handle,
            replication: replication_handle,
            event_publisher: None,
            shadow: shadow_handle,
            health: health_handle,
        },
        dpdk_extras,
        &pipeline_healthy,
        &shutdown,
    )
}

/// Initialize or recover the JournaledExchange from disk.
/// Returns (engine, needs_seeding). `needs_seeding` is true on first startup
/// (fresh journal) — the caller must publish seed events through the pipeline.
fn init_engine(
    config: &ServerConfig,
) -> Result<(JournaledExchange, bool), Box<dyn std::error::Error>> {
    // Check for a snapshot: either the explicit --snapshot path, or the
    // default derived path (used by auto-rotation when --snapshot is not set).
    let derived_snap = config.journal.with_extension("snapshot");
    let snap_path = config.snapshot.as_deref().or_else(|| {
        if derived_snap.exists() {
            Some(derived_snap.as_path())
        } else {
            None
        }
    });

    let journal_exists = config.journal.exists();
    let mut engine = if let Some(snap_path) = snap_path
        && snap_path.exists()
        && journal_exists
    {
        info!(snapshot = %snap_path.display(), "recovering from snapshot + journal");
        JournaledExchange::recover_from_snapshot(snap_path, &config.journal)?
    } else if let Some(snap_path) = snap_path
        && snap_path.exists()
        && !journal_exists
    {
        // Snapshot exists but journal doesn't — likely a crash between
        // rotate_file() and create_continuing(). Recover from snapshot
        // alone and create a fresh journal.
        info!(
            snapshot = %snap_path.display(),
            "recovering from snapshot only (journal missing, post-rotation crash?)"
        );
        let (exchange, snap_sequence, snap_chain_hash) =
            melin_engine::journal::snapshot::load(snap_path)?;
        let writer =
            JournalWriter::create_continuing(&config.journal, snap_sequence + 1, snap_chain_hash)?;
        JournaledExchange::from_parts(exchange, writer)
    } else if journal_exists {
        info!("recovering from journal");
        JournaledExchange::recover(&config.journal)?
    } else {
        info!("creating new journal");
        JournaledExchange::create(&config.journal)?
    };

    let needs_seeding = !journal_exists;

    // Rotate journal if it exceeds the configured size threshold.
    // This saves a snapshot, archives the old journal, and starts
    // a fresh one — preventing unbounded disk growth across restarts.
    if config.max_journal_mib > 0 {
        let threshold = config.max_journal_mib * 1024 * 1024;
        let current_size = engine.journal_size();
        if current_size > threshold {
            let snap_path = config
                .snapshot
                .clone()
                .unwrap_or_else(|| config.journal.with_extension("snapshot"));
            info!(
                current_mib = current_size / (1024 * 1024),
                threshold_mib = config.max_journal_mib,
                snapshot = %snap_path.display(),
                "journal exceeds threshold, rotating"
            );
            engine.rotate(&snap_path)?;
            info!("journal rotated successfully");
        }
    }

    Ok((engine, needs_seeding))
}

/// Apply CPU core affinity for a pipeline thread, logging the result.
fn apply_affinity(thread_name: &str, core_id: usize) {
    match crate::affinity::pin_to_core(core_id) {
        Ok(c) => info!(core = c, thread = thread_name, "pinned to core"),
        Err(e) => tracing::warn!(thread = thread_name, error = e, "core pinning failed"),
    }
}

/// Perform challenge-response authentication on a new connection.
///
/// Runs on the accept thread (cold path, blocking). The caller must set
/// a read timeout on the stream before calling to prevent slow clients
/// from stalling the accept loop.
///
/// Uses raw `read_exact` instead of `BufReader` to avoid over-reading
/// bytes that belong to the first post-auth request.
///
/// Returns `(Permission, public_key_bytes)` on success.
fn authenticate_connection<R: std::io::Read, W: std::io::Write>(
    connection_id: ConnectionId,
    addr: SocketAddr,
    reader: &mut R,
    writer: &mut W,
    authorized_keys: &AuthorizedKeys,
) -> Result<(Permission, [u8; 32]), Box<dyn std::error::Error>> {
    use std::io;

    use ed25519_dalek::{Verifier, VerifyingKey};
    use melin_protocol::codec;
    use melin_protocol::message::{Request, ResponseKind};

    // Generate a 32-byte random nonce for this connection.
    // Explicit OsRng for cryptographic material (SEC-10).
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("getrandom failed: {e}")))?;

    // Send Challenge.
    let mut buf = [0u8; 64];
    let written = codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf)
        .map_err(|e| io::Error::other(format!("encode Challenge: {e}")))?;
    writer.write_all(&buf[..written])?;
    writer.flush()?;

    // Read ChallengeResponse frame directly (no BufReader). Using raw
    // read_exact avoids BufReader over-reading bytes that belong to the
    // first post-auth request — those bytes would be lost when the
    // BufReader is dropped and the fd moves to the io_uring reader.
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .map_err(|e| io::Error::other(format!("read auth frame length: {e}")))?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    // ChallengeResponse is 1 (tag) + 64 (signature) + 32 (public key) = 97 bytes.
    if frame_len > 256 {
        send_auth_failed(writer);
        return Err(io::Error::other(format!("auth frame too large: {frame_len}")).into());
    }
    let mut frame_buf = [0u8; 256];
    reader
        .read_exact(&mut frame_buf[..frame_len])
        .map_err(|e| io::Error::other(format!("read auth frame payload: {e}")))?;

    let (_seq, request) = match codec::decode_request(&frame_buf[..frame_len]) {
        Ok(pair) => pair,
        Err(e) => {
            send_auth_failed(writer);
            return Err(io::Error::other(format!("decode ChallengeResponse: {e}")).into());
        }
    };

    let (signature_bytes, public_key_bytes) = match request {
        Request::ChallengeResponse {
            signature,
            public_key,
        } => (signature, public_key),
        other => {
            send_auth_failed(writer);
            return Err(format!(
                "expected ChallengeResponse, got {:?}",
                std::mem::discriminant(&other)
            )
            .into());
        }
    };

    // Look up the public key in authorized_keys.
    let permission = match authorized_keys.lookup(&public_key_bytes) {
        Some(perm) => perm,
        None => {
            send_auth_failed(writer);
            return Err("unknown public key".into());
        }
    };

    // Verify the Ed25519 signature over the nonce.
    let verifying_key = VerifyingKey::from_bytes(&public_key_bytes).map_err(|e| {
        send_auth_failed(writer);
        io::Error::other(format!("invalid public key: {e}"))
    })?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key.verify(&nonce, &signature).map_err(|e| {
        send_auth_failed(writer);
        io::Error::other(format!("signature verification failed: {e}"))
    })?;

    // Auth succeeded — send ServerReady.
    let written = codec::encode_response(&ResponseKind::ServerReady, &mut buf)
        .map_err(|e| io::Error::other(format!("encode ServerReady: {e}")))?;
    writer.write_all(&buf[..written])?;
    writer.flush()?;

    debug!(
        connection_id = connection_id.0,
        addr = %addr,
        permission = ?permission,
        "authenticated"
    );

    Ok((permission, public_key_bytes))
}

/// Set a read timeout on a raw fd via `setsockopt(SO_RCVTIMEO)`.
///
/// Works for both TCP and UDS since both are sockets. Uses `AsRawFd`
/// to avoid requiring a concrete stream type.
fn set_read_timeout<F: std::os::unix::io::AsRawFd>(
    fd: &F,
    timeout: Option<std::time::Duration>,
) -> std::io::Result<()> {
    let tv = match timeout {
        Some(d) => libc::timeval {
            tv_sec: d.as_secs() as libc::time_t,
            tv_usec: d.subsec_micros() as libc::suseconds_t,
        },
        None => libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
    };
    let ret = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            &tv as *const libc::timeval as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Enable `SO_BUSY_POLL` on a TCP data socket so the kernel busy-polls
/// the NIC for incoming data instead of going to sleep on the softirq
/// → wakeup handoff. Removes scheduler-wakeup latency from the recv
/// path on hardware where IRQ delivery is the dominant per-packet cost
/// (e.g. ixgbe-class NICs). 50 µs covers a typical LAN ack RTT.
///
/// Best-effort: requires `CAP_NET_ADMIN` (or unprivileged operation
/// permitted via sysctl). Failures are surfaced as warnings by the
/// caller — they only cost latency, not correctness, and we don't want
/// a misconfigured kernel to halt connection acceptance.
///
/// Only beneficial when the receiving thread is already busy-spinning
/// (otherwise the spin cost is wasted on idle connections). All
/// callers in Melin meet that condition: client reader threads spin on
/// io_uring CQEs, the replication sender's ack-recv thread spins, and
/// the bench client thread spins.
pub(crate) fn set_busy_poll<F: std::os::unix::io::AsRawFd>(
    fd: &F,
    micros: i32,
) -> std::io::Result<()> {
    // SAFETY: fd is a live socket fd owned by the caller for the
    // duration of the call; the option pointer is to a stack-local i32
    // with the right size for SO_BUSY_POLL.
    let val: libc::c_int = micros;
    let ret = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            &val as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Default `SO_BUSY_POLL` window in microseconds. Matches the value
/// already used on the replica receive socket; chosen to cover a
/// typical LAN round-trip without burning excessive CPU on quiet
/// connections.
pub(crate) const BUSY_POLL_US: i32 = 50;

/// Set `SO_SNDTIMEO` on a socket. Prevents blocking writes from stalling
/// the response thread when a client stops reading (SEC-01).
fn set_write_timeout<F: std::os::unix::io::AsRawFd>(
    fd: &F,
    timeout: Option<std::time::Duration>,
) -> std::io::Result<()> {
    let tv = match timeout {
        Some(d) => libc::timeval {
            tv_sec: d.as_secs() as libc::time_t,
            tv_usec: d.subsec_micros() as libc::suseconds_t,
        },
        None => libc::timeval {
            tv_sec: 0,
            tv_usec: 0,
        },
    };
    let ret = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDTIMEO,
            &tv as *const libc::timeval as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        )
    };
    if ret < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// Best-effort send of AuthFailed before dropping a connection.
fn send_auth_failed(writer: &mut impl std::io::Write) {
    let mut buf = [0u8; 8];
    if let Ok(written) = melin_protocol::codec::encode_response(
        &melin_protocol::message::ResponseKind::AuthFailed,
        &mut buf,
    ) {
        let _ = writer.write_all(&buf[..written]);
        let _ = writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    use ed25519_dalek::{Signer, SigningKey};
    use melin_protocol::auth::{AuthorizedKeys, Permission};
    use melin_protocol::codec;
    use melin_protocol::message::{ConnectionId, Request, ResponseKind};

    use super::authenticate_connection;

    /// Deterministic test key.
    fn test_key() -> SigningKey {
        SigningKey::from_bytes(&[0xAA; 32])
    }

    /// Build an `AuthorizedKeys` containing the test key with the given permission.
    fn keys_with_test_key(perm: &str) -> AuthorizedKeys {
        // Use melin_protocol's base64 re-export via AuthorizedKeys::parse.
        // Encode the public key bytes as base64 manually using the simple
        // alphabet (all test keys produce valid base64).
        let pub_bytes = test_key().verifying_key().to_bytes();
        let pub_b64 = base64_encode(&pub_bytes);
        AuthorizedKeys::parse(&format!("{perm} {pub_b64} test\n")).unwrap()
    }

    /// Minimal base64 encoder for test use only. Avoids adding base64
    /// as a dev-dependency to the server crate.
    fn base64_encode(bytes: &[u8]) -> String {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = String::new();
        for chunk in bytes.chunks(3) {
            let b0 = chunk[0] as u32;
            let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
            let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
            let n = (b0 << 16) | (b1 << 8) | b2;
            out.push(ALPHABET[(n >> 18 & 0x3F) as usize] as char);
            out.push(ALPHABET[(n >> 12 & 0x3F) as usize] as char);
            if chunk.len() > 1 {
                out.push(ALPHABET[(n >> 6 & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
            if chunk.len() > 2 {
                out.push(ALPHABET[(n & 0x3F) as usize] as char);
            } else {
                out.push('=');
            }
        }
        out
    }

    /// Run `authenticate_connection` on one end of a `UnixStream::pair()`,
    /// returning the result. Maps the error to `String` so it's `Send`.
    fn run_server_auth(
        mut stream: UnixStream,
        keys: AuthorizedKeys,
    ) -> std::thread::JoinHandle<Result<Permission, String>> {
        std::thread::spawn(move || {
            // Clone the stream so we have independent read/write halves.
            let mut writer = stream.try_clone().unwrap();
            authenticate_connection(
                ConnectionId(1),
                "127.0.0.1:0".parse().unwrap(),
                &mut stream,
                &mut writer,
                &keys,
            )
            .map(|(perm, _pk)| perm)
            .map_err(|e| e.to_string())
        })
    }

    /// Read a Challenge frame from the client end, sign it, and write
    /// a ChallengeResponse back.
    fn client_sign_challenge(stream: &mut UnixStream, key: &SigningKey) {
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        stream.read_exact(&mut payload[..len]).unwrap();

        let resp = codec::decode_response(&payload[..len]).unwrap();
        let nonce = match resp {
            ResponseKind::Challenge { nonce } => nonce,
            other => panic!("expected Challenge, got {other:?}"),
        };

        let sig = key.sign(&nonce);
        let request = Request::ChallengeResponse {
            signature: sig.to_bytes(),
            public_key: key.verifying_key().to_bytes(),
        };
        let mut buf = [0u8; 256];
        let written = codec::encode_request(&request, 0, &mut buf).unwrap();
        stream.write_all(&buf[..written]).unwrap();
        stream.flush().unwrap();
    }

    /// Like `client_sign_challenge` but corrupts the signature.
    fn client_sign_challenge_bad(stream: &mut UnixStream, key: &SigningKey) {
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        stream.read_exact(&mut payload[..len]).unwrap();

        let resp = codec::decode_response(&payload[..len]).unwrap();
        let nonce = match resp {
            ResponseKind::Challenge { nonce } => nonce,
            other => panic!("expected Challenge, got {other:?}"),
        };

        let mut sig_bytes = key.sign(&nonce).to_bytes();
        sig_bytes[0] ^= 0xFF;

        let request = Request::ChallengeResponse {
            signature: sig_bytes,
            public_key: key.verifying_key().to_bytes(),
        };
        let mut buf = [0u8; 256];
        let written = codec::encode_request(&request, 0, &mut buf).unwrap();
        stream.write_all(&buf[..written]).unwrap();
        stream.flush().unwrap();
    }

    /// Read one length-prefixed frame and decode as a response.
    fn read_response(stream: &mut UnixStream) -> ResponseKind {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = [0u8; 64];
        stream.read_exact(&mut buf[..len]).unwrap();
        codec::decode_response(&buf[..len]).unwrap()
    }

    #[test]
    fn auth_success_returns_permission() {
        let keys = keys_with_test_key("trader");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::ServerReady));

        let result = handle.join().unwrap();
        assert_eq!(result.unwrap(), Permission::Trader);
    }

    #[test]
    fn auth_admin_permission() {
        let keys = keys_with_test_key("operator");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::ServerReady));

        assert_eq!(handle.join().unwrap().unwrap(), Permission::Operator);
    }

    #[test]
    fn auth_unknown_key_sends_auth_failed() {
        let keys = AuthorizedKeys::parse("").unwrap();
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_bad_signature_sends_auth_failed() {
        let keys = keys_with_test_key("operator");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge_bad(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_wrong_message_type_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard the Challenge.
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        s2.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        s2.read_exact(&mut payload[..len]).unwrap();

        // Send a Heartbeat instead of ChallengeResponse.
        let mut buf = [0u8; 16];
        let written = codec::encode_request(&Request::Heartbeat, 0, &mut buf).unwrap();
        s2.write_all(&buf[..written]).unwrap();
        s2.flush().unwrap();

        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_client_disconnects_is_error() {
        let keys = keys_with_test_key("trader");
        let (s1, s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Drop immediately — server fails reading the ChallengeResponse.
        drop(s2);

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_different_key_than_authorized_is_rejected() {
        // Authorize the test key, but connect with a different key.
        let keys = keys_with_test_key("trader");
        let wrong_key = SigningKey::from_bytes(&[0xCC; 32]);
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &wrong_key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_oversized_frame_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard Challenge.
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        s2.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        s2.read_exact(&mut payload[..len]).unwrap();

        // Send a frame claiming to be 1000 bytes (way over the 256 limit).
        let fake_len: u32 = 1000;
        s2.write_all(&fake_len.to_le_bytes()).unwrap();
        s2.flush().unwrap();

        // Server should send AuthFailed before dropping.
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_zero_length_frame_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard Challenge.
        let mut len_buf = [0u8; 4];
        let mut payload = [0u8; 64];
        s2.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        s2.read_exact(&mut payload[..len]).unwrap();

        // Send a zero-length frame — decode_request will fail on empty input.
        let zero_len: u32 = 0;
        s2.write_all(&zero_len.to_le_bytes()).unwrap();
        s2.flush().unwrap();

        // Server should send AuthFailed before dropping.
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::AuthFailed));

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_readonly_permission() {
        let keys = keys_with_test_key("readonly");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response(&mut s2);
        assert!(matches!(resp, ResponseKind::ServerReady));

        let perm = handle.join().unwrap().unwrap();
        assert_eq!(perm, Permission::ReadOnly);
        assert!(!perm.can_trade());
    }
}
