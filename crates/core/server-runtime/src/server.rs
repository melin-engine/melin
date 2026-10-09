//! Server orchestrator — binds the accept loop, pipeline threads, and reader.
//!
//! On startup:
//! 1. Recovers or creates the `JournaledApp<A>`.
//! 2. Decomposes it into `(A, W)` via `into_parts()`, where `W` is the journal writer.
//! 3. Builds the disruptor pipeline (input ring + output ring).
//! 4. Spawns 3-5 OS threads: journal, matching, response, [repl-accept], [event-publisher].
//! 5. Runs the accept loop, registering connections with the io_uring reader.
//!
//! Fully synchronous — no async runtime needed. Reader threads use io_uring
//! with multishot RECV to multiplex connections, eliminating thread
//! oversubscription. The response thread writes via io_uring SEND.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};

use tracing::{debug, error, info, warn};

use melin_journal::BufferedWriter;
use melin_journal::JournalError;
use melin_journal::JournalWrite;
use melin_transport_core::journaled_app::JournaledApp;
use melin_transport_core::pipeline::{
    InputSlot, OutputSlot as GenericOutputSlot, Pipeline as GenericPipeline,
    build_pipeline_with_replication,
};
/// Internal alias for the disruptor-built pipeline, used only by
/// destructuring `let Pipeline { … }` patterns inside the boot path.
/// Not part of the public API — callers reach the underlying type
/// through `melin_transport_core::pipeline`.
type Pipeline<A> = GenericPipeline<A>;

use crate::StartupEvents;
use crate::client_auth::MAX_AUTH_FRAME;
use crate::reader::RequestDecoderArc;
use crate::response::ResponseEncoderArc;
use melin_app::auth::{AuthorizedKeys, ClientRole, KeyRole, RoleId};
use melin_app::decoder::{ErasedDecoder, RequestDecoder};
use melin_app::encoder::ResponseEncoder;
use melin_app::{AppEvent, Application};
use melin_pipeline::ring::Consumer;
use melin_pipeline::wait::WaitStrategy;

use crate::layout::{DEFAULT_CORES, parse_cores};
/// The layout types live in [`crate::layout`]; re-exported here because
/// `ServerConfig::cores` is one of them and callers reach the config
/// through this module.
pub use crate::layout::{PipelineCores, Placement, ReaderThread};

/// How the orchestrator (main) thread waits on pipeline threads during
/// startup — the on_primary drain and the promotion epoch bump. It is unpinned
/// and on no latency path, so it yields whatever the layout says: a
/// spinner without a core of its own is the co-location bug with the
/// victim chosen by the scheduler.
const ORCHESTRATOR_WAIT: WaitStrategy = WaitStrategy::SpinThenYield;

/// Output-slot sugar parameterised on the application — saves spelling
/// `<A::Report, A::QueryResponse>` at every pipeline-facing signature
/// that doesn't already destructure them.
type OutputSlot<A> =
    GenericOutputSlot<<A as Application>::Report, <A as Application>::QueryResponse>;

/// Body of the event-publisher thread: a free function with this exact
/// signature (an application with an output feed supplies its
/// publisher; one without supplies nothing). Passing it as a function
/// pointer keeps the runtime decoupled from the application — no
/// application reference inside server.rs — without paying for a
/// boxed closure.
///
/// Threaded into [`run`] (and `run_dpdk` under `feature = "dpdk"`) as `Option<EventPublisherFn>`:
/// `None` disables the publisher unconditionally, `Some(_)` wires it up
/// when `--event-bind` is also set.
pub type EventPublisherFn<A> = fn(
    consumer: Consumer<OutputSlot<A>>,
    bind_addr: SocketAddr,
    authorized_keys: Arc<AuthorizedKeys>,
    shutdown: &AtomicBool,
    wait: WaitStrategy,
);

use melin_wire_protocol::blocking::BlockingFrameWriter;
use melin_wire_protocol::control::ConnectionId;
use melin_wire_protocol::transport::BlockingTransportListener;

/// Default replica pipeline depth (pending ack queue capacity).
///
/// Only bounds persisted-ack *granularity*, never the receive rate: the
/// queue merges instead of blocking when it fills. 256 entries × 16 B is
/// 4 KiB, so the depth is chosen to keep granularity through an fsync
/// hiccup rather than to save memory.
const DEFAULT_REPLICATION_PIPELINE_DEPTH: usize = 256;

/// CLI spelling of [`melin_journal::StagingMode`].
///
/// A separate enum rather than a `clap::ValueEnum` derive on the journal
/// type: `melin-journal` has no clap dependency and should not acquire
/// one for a flag: the crate is a library first, and its callers are not
/// all command-line servers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum JournalStagingMode {
    /// Pre-write every staged segment so appends generate no
    /// extent-conversion metadata. Costs one extra pass over each
    /// segment in device bandwidth.
    #[default]
    ZeroFill,
    /// Allocate staged extents without materialising them. No staging
    /// bandwidth; appends convert extents and `fdatasync` periodically
    /// forces the filesystem log.
    Allocate,
}

impl From<JournalStagingMode> for melin_journal::StagingMode {
    fn from(m: JournalStagingMode) -> Self {
        match m {
            JournalStagingMode::ZeroFill => melin_journal::StagingMode::ZeroFill,
            JournalStagingMode::Allocate => melin_journal::StagingMode::Allocate,
        }
    }
}

/// Server configuration, parsed from CLI arguments via clap.
#[derive(clap::Parser)]
#[command(
    name = "melin-server",
    about = "A node of the Melin replicated sequencer"
)]
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
    /// Where each pipeline thread runs, as comma-separated `thread=core`
    /// entries in any order. Threads: journal-seq, matching, response, reader,
    /// event-publisher, shadow, repl-handler-0, repl-handler-1, journal-prep,
    /// journal-disk. Every thread must be named: `0` leaves one unpinned,
    /// and `none` unpins every thread. The node warns at boot when
    /// journal-seq, matching, response, reader or journal-disk has no core:
    /// every request passes through them. Core 0 is reserved for OS/IRQ
    /// handling.
    /// reader pins the io_uring reader (TCP) or DPDK poll thread.
    /// event-publisher applies when `--event-bind` is set, shadow when
    /// `--snapshot-interval-ms` > 0. repl-handler-0/1 are for the
    /// per-replica TCP handler threads. journal-disk pins the thread that
    /// writes and syncs the journal; give it a core on the same CCD as
    /// journal-seq, since the two exchange a cache line per batch.
    ///
    /// Each core may carry a suffix saying how that thread waits: `7`
    /// (or `7s`) busy-spins and needs the core to itself, `7y` spins
    /// briefly then yields and may share the core with other `y`
    /// threads. `0` (unpinned) always yields. Two threads on one core
    /// where either busy-spins is refused at startup — they would starve
    /// each other. journal-prep never busy-waits and takes no suffix.
    #[arg(long, default_value = DEFAULT_CORES, value_parser = parse_cores)]
    pub cores: PipelineCores,
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
    /// Maximum number of concurrent authenticated connections, from 1 to
    /// 8192. New connections are rejected (closed immediately) when this
    /// limit is reached. Prevents fd/memory exhaustion (SEC-02). The
    /// io_uring rings are sized from it, and their memory counts against
    /// the locked-memory limit, so a cap well above the clients the node
    /// serves costs memory for nothing. There is no "unlimited".
    #[arg(
        long,
        default_value_t = 1024,
        value_parser = clap::value_parser!(u64).range(1..=crate::connection_limit::MAX_SUPPORTED_CONNECTIONS)
    )]
    pub max_connections: u64,
    /// Path to the authorized keys file for Ed25519 challenge-response
    /// authentication. Every connection must authenticate before sending
    /// requests.
    /// Required for primary mode; ignored in replica mode (--replica-of).
    /// See `AuthorizedKeys` for file format.
    #[arg(long, default_value = "authorized_keys")]
    pub authorized_keys: PathBuf,
    /// Maximum journal size in MiB before automatic rotation at startup.
    /// When the journal exceeds this threshold, the server saves a snapshot
    /// and starts a fresh journal. Set to 0 to disable. Default: 256 MiB.
    #[arg(long, default_value_t = 256)]
    pub max_journal_mib: u64,
    /// How the background preparer materialises the next journal
    /// segment's extents.
    ///
    /// - `zero-fill` (default) physically pre-writes the segment, so
    ///   appends carry no extent-conversion metadata and each flush's
    ///   `fdatasync` never forces the filesystem log. It costs one extra
    ///   pass over every segment in device bandwidth.
    /// - `allocate` reserves the extents and stops. Staging becomes
    ///   free, and the periodic log force returns to the flush path.
    ///
    /// Keep the default on local NVMe, where sequential bandwidth is
    /// cheap and the log forces are what hurts. Consider `allocate` on
    /// network-attached storage (EBS and similar), where staging
    /// bandwidth is metered and drawn from the same budget as the hot
    /// path — measure both on the volume in question rather than
    /// assuming, since which one wins is a property of the device.
    #[arg(long, value_enum, default_value_t = JournalStagingMode::ZeroFill)]
    pub journal_staging_mode: JournalStagingMode,

    /// Address to listen for replica connections (enables synchronous replication).
    /// Mutually exclusive with `--standalone` and `--replica-of`.
    #[arg(long)]
    pub replication_bind: Option<std::net::SocketAddr>,

    /// Disable replication (dev/test mode). The replica quorum cursor stays
    /// at its no-replica sentinel (health reports zero replication lag) and
    /// the ack policy evaluates against the primary's journal alone.
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
    /// under the `replication` role.
    #[arg(long)]
    pub replication_key: Option<std::path::PathBuf>,

    /// Maximum number of replication ring batches to coalesce into a
    /// single TCP write+flush. Higher values reduce syscall overhead
    /// but increase per-write latency. Default: 128.
    #[arg(long, default_value_t = 128)]
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

    /// Number of receive batches the replica tracks individually while
    /// they await local journal fsync. Must be a power of two. Beyond
    /// this depth the receiver keeps receiving and coalesces pending
    /// acks (a later ack, never an early one) — it never applies
    /// backpressure to the primary. Default: 256.
    #[arg(long, default_value_t = DEFAULT_REPLICATION_PIPELINE_DEPTH)]
    pub replication_pipeline_depth: usize,

    /// Number of slots in each replication ring buffer. Must be a power
    /// of two. Each slot holds up to 512 KiB. More slots = more buffering
    /// before eviction. Default: 256 (128 MiB per ring, 256 MiB dual-repl).
    #[arg(long, default_value_t = 256)]
    pub replication_ring_size: usize,

    /// Ack policy: which copies of an event must exist before its
    /// response is released. One of:
    ///
    /// - `disk`               `persisted>=1`. One fsynced copy, on
    ///   whichever node's disk confirms first; required with
    ///   `--standalone`. Dev/staging deployments.
    /// - `ram`                `in_memory>=2`. Two nodes hold the event
    ///   in memory before the ack; every fsync trails off the ack path
    ///   (the journal still syncs every batch). Lowest ack latency;
    ///   survives any single node failure via failover; loses only the
    ///   un-fsynced tail on a whole-cluster power loss. For slow-fsync
    ///   storage (cloud volumes) or RPO-tolerant apps.
    /// - `disk+ram` (default) `persisted>=1 && in_memory>=2`. One
    ///   fsynced copy plus a second copy in another node's memory.
    ///   Single-failure-safe with a brief RAM-only window for the
    ///   second copy. Typical live deployments. Faster than `two-disks`:
    ///   an acknowledgement waits for the second node to receive the
    ///   event, not to fsync it.
    /// - `two-disks`          `persisted>=2`. Two fsynced copies before
    ///   the client ack. Zero RAM-only window; the gate stalls when no
    ///   replica is connected. Compliance-driven deployments.
    ///
    /// `--standalone` requires `disk`. Under every other policy the
    /// gate stalls while no replica is connected — the correct
    /// behaviour for a serious deployment that has lost its replicas.
    /// See `docs/replication.md` for the operational menu.
    #[arg(long, value_enum, default_value_t = crate::ack_policy::AckPolicy::DiskAndRam)]
    pub ack_policy: crate::ack_policy::AckPolicy,

    // --- DPDK configuration (only used with --features dpdk) ---
    /// DPDK EAL arguments (space-separated). Example: --dpdk-eal-args="-l 0-7 --huge-dir /dev/hugepages".
    /// Passed directly to rte_eal_init. Only used when compiled with --features dpdk.
    /// The `=` is required: the value itself starts with a dash, and requiring
    /// the joined form keeps a forgotten value from silently swallowing the
    /// next flag as the EAL string.
    #[arg(long, default_value = "", require_equals = true)]
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

    /// Peer IPv4 used for the bifurcated `rte_flow` steering rule. When
    /// set, the DPDK port opens in isolated mode and only IPv4 packets
    /// sourced from this address are delivered into DPDK queue 0 —
    /// everything else stays with the kernel netdev. Required for L3
    /// setups that share the public NIC with the kernel (SSH, etc.).
    #[arg(long)]
    pub dpdk_peer_ip: Option<String>,

    /// Gateway MAC (aa:bb:cc:dd:ee:ff) seeded into smoltcp for the
    /// `--dpdk-gateway` IP. Required in L3 bifurcated mode because the
    /// gateway's ARP reply would not match the steering rule and would
    /// be eaten by the kernel. Source from `ip neigh` on the host.
    #[arg(long)]
    pub dpdk_gateway_mac: Option<String>,

    /// MAC (aa:bb:cc:dd:ee:ff) of the primary named by `--replica-of`,
    /// seeded into smoltcp so the replica's first frame is addressed
    /// before any ARP. Required on ports that keep a real hardware MAC —
    /// an mlx5 in bifurcated mode shares the kernel netdev's — where the
    /// SR-IOV `02:00:<ip>` fallback is wrong and the replica would spin
    /// on connect with no error. Read it from
    /// `/sys/class/net/<iface>/address` on the primary, or from
    /// `DPDK_MAC` in `/etc/melin-dpdk.conf`. Ignored on a primary, which
    /// learns peer MACs from inbound frames.
    #[arg(long)]
    pub dpdk_peer_mac: Option<String>,

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
    /// to receive a real-time stream of the application's reports, as the
    /// application's publisher encodes them. Ed25519 auth against the
    /// node's authorized_keys; which keys may subscribe, and what each may
    /// see, is the application's publisher's decision.
    /// Omit to disable (ring has 1 consumer — identical to before).
    #[arg(long)]
    pub event_bind: Option<SocketAddr>,

    /// Address for the health/liveness TCP endpoint. On connect, returns
    /// a one-line status (`OK <conns> <journal_seq> <repl_lag>\n`) and
    /// closes. No auth required. Set to empty string to disable.
    #[arg(long, default_value = "127.0.0.1:9878")]
    pub health_bind: Option<SocketAddr>,

    /// TCP address for the operator admin endpoint. Authenticated with
    /// operator keys (Ed25519 challenge-response, same as all other
    /// admin handshakes). Accepts:
    ///
    /// - `PROMOTE\n` — replica → primary leadership transition (replica
    ///   nodes only; rejected with ERR on a primary).
    /// - `ROTATE\n` — archive the current journal segment at the next
    ///   fsync boundary (any node where runtime rotation is enabled).
    ///
    /// Unset means no admin endpoint is listening; operators can still
    /// rely on the size-driven rotation trigger (`--max-journal-mib`)
    /// without exposing a port.
    #[arg(long)]
    pub admin_bind: Option<SocketAddr>,

    /// Interval in milliseconds between automatic shadow snapshots. The
    /// shadow stage replays journaled events on a cloned `A` and saves a
    /// consistent snapshot at this cadence — no hot-path stall. Set to 0
    /// to disable shadow snapshots entirely. Default: 3 000 000 ms (50 min).
    #[arg(long, default_value_t = 3_000_000)]
    pub snapshot_interval_ms: u64,

    /// Path for shadow snapshots. Defaults to the journal path with a
    /// `.snapshot` extension (same as the startup snapshot path).
    #[arg(long)]
    pub snapshot_path: Option<PathBuf>,

    /// Cadence in milliseconds for the application's clock tick.
    /// The ingress thread (io_uring reader or DPDK poll thread) publishes
    /// a `JournalEvent::Tick { now_ns }` at this interval so the
    /// application's time-driven work (expiries, timeouts, scheduled
    /// transitions) fires in deterministic, journaled lockstep. There is
    /// no separate tick thread on either transport. Set to 0 to disable
    /// tick generation entirely (useful for benchmarks that don't
    /// exercise time-driven features).
    ///
    /// Defaults to 250 ms. Under load the matching stage advances the
    /// application's clock at every-event resolution from `slot.timestamp_ns`
    /// (microsecond precision), so the tick is only the safety net for
    /// quiet periods. 250 ms keeps time-driven work in quiet periods within
    /// a quarter-second of their deadline at a cost of ~4 events/sec of
    /// journal traffic.
    #[arg(long, default_value_t = 250)]
    pub tick_interval_ms: u64,

    /// Disable `mlockall(MCL_CURRENT | MCL_FUTURE)` at startup.
    ///
    /// By default the server locks all current and future pages into
    /// RAM to prevent rare multi-millisecond stalls from page faults
    /// or swap activity on the matching hot path. Locking requires
    /// `CAP_IPC_LOCK` (or running as root) and `RLIMIT_MEMLOCK` raised
    /// to a value larger than the process's RSS — the server raises
    /// the rlimit itself, but only succeeds with the capability.
    ///
    /// Use `--no-mlock` for development / containerised runs where
    /// the privilege isn't available; on a bare-metal production host
    /// leave it on.
    #[arg(long, default_value_t = false)]
    pub no_mlock: bool,

    /// Bind address for the control-plane raft RPC listener. Setting this
    /// enables the control plane on this node: leader election runs and is
    /// observable via the `melin_raft_*` gauges. Requires `--raft-node-id`
    /// and a `--raft-peer` entry for this node; requires
    /// `--replication-key` (peer links authenticate with the replication
    /// key, same trust domain as the data plane). Raft always runs on
    /// kernel TCP, including on DPDK nodes.
    #[arg(long)]
    pub raft_bind: Option<SocketAddr>,

    /// This node's control-plane raft id (non-zero, unique per cluster).
    #[arg(long)]
    pub raft_node_id: Option<u64>,

    /// A control-plane cluster member as `id@host:port#base64-pubkey`
    /// (repeatable). Give **every node the same list, including an entry
    /// for the node itself** — the self entry supplies the dialable
    /// address peers use to reach it, and identical lists keep the
    /// first-boot membership consistent. The pubkey pins the peer's
    /// identity: a connection authenticated with a different key cannot
    /// speak for that id.
    #[arg(long)]
    pub raft_peer: Vec<String>,

    /// Directory for durable raft state (vote, log, membership).
    /// Defaults to the journal path with a `.raft` extension. Must live
    /// on the same durability class as the journal — losing it can
    /// double-grant a vote (see docs/replication.md).
    #[arg(long)]
    pub raft_dir: Option<PathBuf>,

    /// Act on control-plane election wins: a replica elected leader
    /// promotes itself (journaling the election term as its fencing
    /// epoch), and a serving node self-fences when the raft mesh shows
    /// it superseded. Off by default — automatic failover is an explicit
    /// operator policy decision. Requires `--raft-bind` and at least
    /// three configured voters (a two-node cluster cannot elect after
    /// losing either node, so automation would be dead weight with real
    /// misconfiguration risk; see docs/replication.md).
    #[arg(long, default_value_t = false)]
    pub raft_auto_promote: bool,
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
                journal_seq: Placement::spinning(1),
                matching: Placement::spinning(2),
                response: Placement::spinning(3),
                reader: Placement::spinning(4),
                event_publisher: Placement::spinning(6),
                shadow: Placement::spinning(7),
                repl_handler_0: Placement::spinning(8),
                repl_handler_1: Placement::spinning(9),
                journal_prep: Placement::yielding(10),
                journal_disk: Placement::spinning(11),
            },
            group_commit_us: 0,
            heartbeat_interval_secs: 10,
            connection_timeout_secs: 30,
            max_connections: 1024,
            authorized_keys: PathBuf::from("authorized_keys"),
            max_journal_mib: 256,
            journal_staging_mode: JournalStagingMode::ZeroFill,

            replication_bind: None,
            standalone: false,
            replica_of: None,
            replication_key: None,
            replication_batch_size: 128,
            max_journal_batch: 1024,
            replication_heartbeat_secs: 5,
            replication_pipeline_depth: DEFAULT_REPLICATION_PIPELINE_DEPTH,
            replication_ring_size: 256,
            ack_policy: crate::ack_policy::AckPolicy::DiskAndRam,
            dpdk_eal_args: String::new(),
            dpdk_peer_ip: None,
            dpdk_gateway_mac: None,
            dpdk_peer_mac: None,
            dpdk_ports: vec![0],
            dpdk_ip: "10.0.0.1".into(),
            dpdk_prefix_len: 24,
            dpdk_gateway: None,
            dpdk_mtu: 1500,
            dpdk_vlan: None,
            event_bind: None,
            health_bind: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9878)),
            admin_bind: None,
            snapshot_interval_ms: 3_000_000,
            snapshot_path: None,
            tick_interval_ms: 250,
            no_mlock: false,
            raft_bind: None,
            raft_node_id: None,
            raft_peer: Vec::new(),
            raft_dir: None,
            raft_auto_promote: false,
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

    /// Tick generator cadence as a `Duration`. Returns `None` when the tick
    /// thread is disabled (`--tick-interval-ms 0`).
    pub fn tick_interval(&self) -> Option<std::time::Duration> {
        if self.tick_interval_ms == 0 {
            None
        } else {
            Some(std::time::Duration::from_millis(self.tick_interval_ms))
        }
    }

    /// The per-thread layout this node actually runs — see
    /// [`PipelineCores::resolve`]. `reader` says which thread the reader
    /// entry pins on this transport, since one of the two cannot yield.
    ///
    /// An `Err` is a configuration the node refuses to start with.
    pub fn resolved_cores(&self, reader: ReaderThread) -> Result<PipelineCores, String> {
        self.cores.resolve(reader)
    }
}

/// Run the server with automatic transport selection.
///
/// Installs SIGINT/SIGTERM handlers, optionally locks memory, selects
/// the transport (DPDK under the `dpdk` feature, kernel TCP otherwise),
/// and enters the pipeline loop. This is the main entry point for
/// application binaries.
///
/// The application starts from `A::default()` on every node; `startup`
/// is what the node journals on top of that as it becomes primary — see
/// [`StartupEvents`]. `sizing` is what this node reserves memory for,
/// handed to [`Application::prefault`] on every instance the node
/// builds; it is local to the node and never journaled.
///
/// For callers that need a pre-bound listener or an externally
/// controlled shutdown flag (e.g. benchmarks), use
/// [`run_with_listener`] instead; for an externally controlled flag on
/// the build's own transport, [`run_with_shutdown`].
pub fn run<A>(
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: impl RequestDecoder<Event = A::Event> + 'static,
    encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
    event_publisher: Option<EventPublisherFn<A>>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    // Before anything pins a thread or initialises DPDK, whose EAL can
    // narrow this thread to one lcore: the CPU set captured here is what
    // every unpinned thread runs on.
    melin_app::affinity::capture_home_mask();
    let authorized_keys = load_authorized_keys(&decoder, &config.authorized_keys)?;
    let shutdown = Arc::new(AtomicBool::new(false));
    crate::process::install_shutdown_handler(&shutdown);
    run_selected::<A>(
        config,
        startup,
        sizing,
        decoder,
        encoder,
        event_publisher,
        authorized_keys,
        shutdown,
    )
}

/// [`run`], stopped through `shutdown` rather than by a signal: the same
/// transport selection and the same startup, but no SIGINT/SIGTERM
/// handler is installed. Set `shutdown` to `true` for a clean shutdown;
/// the node may also set it itself (a fenced node stops this way).
///
/// For a host that owns its process's signals, or runs several nodes in
/// one process — the signal handler `run` installs is process-wide and
/// stops one node only. Several DPDK nodes in one process also need a
/// process-wide EAL (`melin_dpdk::Eal::init_process_wide`) and a port
/// each. Unlike [`run_with_listener`], the client listener is the
/// transport's own: on DPDK there is no kernel socket to hand in.
pub fn run_with_shutdown<A>(
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: impl RequestDecoder<Event = A::Event> + 'static,
    encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
    event_publisher: Option<EventPublisherFn<A>>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    // As in `run`.
    melin_app::affinity::capture_home_mask();
    let authorized_keys = load_authorized_keys(&decoder, &config.authorized_keys)?;
    run_selected::<A>(
        config,
        startup,
        sizing,
        decoder,
        encoder,
        event_publisher,
        authorized_keys,
        shutdown,
    )
}

/// The part of [`run`] and [`run_with_shutdown`] after the shutdown flag
/// exists: memory locking, then the build's transport.
#[allow(clippy::too_many_arguments)] // entry-chain hop, same arguments as run_impl
fn run_selected<A>(
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: impl RequestDecoder<Event = A::Event> + 'static,
    encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
    event_publisher: Option<EventPublisherFn<A>>,
    authorized_keys: Arc<AuthorizedKeys>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    if !config.no_mlock {
        crate::process::try_lock_memory();
    }

    #[cfg(feature = "dpdk")]
    {
        run_dpdk::<A>(
            config,
            startup,
            sizing,
            Arc::new(decoder),
            Arc::new(encoder),
            event_publisher,
            authorized_keys,
            shutdown,
        )
    }

    #[cfg(not(feature = "dpdk"))]
    {
        let listener = melin_wire_protocol::tcp::BlockingTcpListener::bind(config.bind)?;
        run_tcp::<A, _>(
            listener,
            config,
            startup,
            sizing,
            Arc::new(decoder),
            Arc::new(encoder),
            event_publisher,
            authorized_keys,
            shutdown,
        )
    }
}

/// Load the `authorized_keys` file for the decoder's roles: the one place
/// the file meets the application's role type. Generic over the decoder so
/// its role type is named while it is still in scope; past the entry
/// points the runtime holds the table and an erased decoder, and never
/// the type.
fn load_authorized_keys<D: RequestDecoder>(
    _decoder: &D,
    path: &std::path::Path,
) -> Result<Arc<AuthorizedKeys>, Box<dyn std::error::Error>> {
    let keys = AuthorizedKeys::load::<D::Role>(path)?;
    info!(keys = keys.len(), path = %path.display(), "loaded authorized keys");
    Ok(Arc::new(keys))
}

/// Refuse to start on a keys table parsed for a role type other than the
/// decoder's: its role indices would name the decoder's roles wrongly,
/// granting one role another's rights. Correct by construction today, as
/// both come from the same decoder in the entry point; checked where the
/// two arrive as separate parameters, which is the seam a later change
/// could break.
fn check_keys_match_decoder<E: AppEvent>(
    decoder: &dyn ErasedDecoder<E>,
    authorized_keys: &AuthorizedKeys,
) -> Result<(), Box<dyn std::error::Error>> {
    if decoder.matches_keys(authorized_keys) {
        Ok(())
    } else {
        Err("authorized keys were parsed for a role type other than the decoder's".into())
    }
}

/// Run the server with a caller-supplied listener.
///
/// Use this when the listener must be pre-bound before the server
/// starts (e.g. benchmarks that need the kernel backlog to queue
/// connections immediately).
///
/// Set `shutdown` to `true` to trigger a clean shutdown of all
/// pipeline threads.
pub fn run_with_listener<A>(
    listener: impl BlockingTransportListener,
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: impl RequestDecoder<Event = A::Event> + 'static,
    encoder: impl ResponseEncoder<Report = A::Report, Query = A::QueryResponse> + 'static,
    event_publisher: Option<EventPublisherFn<A>>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    // As in `run`: the CPU set unpinned threads run on, captured before
    // anything pins a thread.
    melin_app::affinity::capture_home_mask();
    let authorized_keys = load_authorized_keys(&decoder, &config.authorized_keys)?;
    run_tcp::<A, _>(
        listener,
        config,
        startup,
        sizing,
        Arc::new(decoder),
        Arc::new(encoder),
        event_publisher,
        authorized_keys,
        shutdown,
    )
}

#[allow(clippy::too_many_arguments)] // entry-chain hop, same arguments as run_impl
fn run_tcp<A, L>(
    listener: L,
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: RequestDecoderArc<A>,
    encoder: ResponseEncoderArc<A>,
    event_publisher: Option<EventPublisherFn<A>>,
    authorized_keys: Arc<AuthorizedKeys>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    L: BlockingTransportListener,
{
    check_keys_match_decoder(&*decoder, &authorized_keys)?;
    run_impl::<A, L>(
        listener,
        config,
        startup,
        sizing,
        decoder,
        encoder,
        event_publisher,
        authorized_keys,
        shutdown,
    )
}

/// Announce at boot that this binary was built without the
/// tamper-evident journal chain.
///
/// A chain-less build is otherwise silent: the journal still writes and
/// replication still streams, but segment-continuity verification at
/// recovery, the snapshot/journal cross-check, and cross-node divergence
/// detection are all absent — a rejoining ex-primary carrying a
/// journaled-but-unreplicated suffix gets streamed to rather than
/// resynced. A deployment can arrive here without meaning to, because
/// `--no-default-features` anywhere in the dependency graph turns the
/// chain off wholesale, so boot says so out loud rather than leaving the
/// absence to be discovered during an incident.
///
/// `warn!` rather than `error!`: it is a deliberate build configuration,
/// not a malfunction.
fn warn_if_chain_disabled() {
    #[cfg(not(feature = "hash-chain"))]
    warn!(
        "built without the `hash-chain` feature: journal tamper evidence, \
         segment-continuity and snapshot cross-checks at recovery, and \
         cross-node divergence detection are all disabled"
    );
}

/// Log the layout the node runs, and warn when it leaves a mandatory
/// thread without a core. Every request passes through those threads, so
/// a core shared with whatever else the scheduler puts there is paid for
/// on every acknowledgement; the auxiliary threads' placement is the
/// operator's documented trade and draws no warning. `none` is called
/// out on its own: nothing pinned is the development layout, and figures
/// measured under it say nothing about the node. One function for both
/// transports, so the two paths cannot drift on what they warn about.
///
/// `warn!` rather than `error!`: a layout the operator chose, not a
/// malfunction.
fn log_layout(cores: &PipelineCores) {
    info!(cores = %cores, "pipeline layout");
    if cores.pins_nothing() {
        warn!(
            "--cores none: no pipeline thread is pinned. Every thread runs wherever the \
             scheduler puts it and shares that core with everything else on the host. A \
             development layout: latency measured under it is not representative. Name a \
             core per thread in --cores"
        );
        return;
    }
    let unpinned = cores.unpinned_mandatory();
    if !unpinned.is_empty() {
        warn!(
            threads = %unpinned.join(", "),
            "--cores: mandatory pipeline threads have no core of their own. Every request \
             passes through them, and each runs wherever the scheduler puts it, sharing \
             that core with whatever else is there. Give them cores"
        );
    }
}

#[allow(clippy::too_many_arguments)] // boot assembly point: each argument is one piece the node is built from
fn run_impl<A, L>(
    listener: L,
    config: ServerConfig,
    mut startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: RequestDecoderArc<A>,
    encoder: ResponseEncoderArc<A>,
    event_publisher: Option<EventPublisherFn<A>>,
    authorized_keys: Arc<AuthorizedKeys>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    L: BlockingTransportListener,
{
    warn_if_chain_disabled();

    // The layout every spawn site below reads, refused outright if two
    // threads would starve each other.
    let config = ServerConfig {
        cores: config.resolved_cores(ReaderThread::IoUring)?,
        ..config
    };
    log_layout(&config.cores);

    // Shared ack-policy atomic, constructed once per process and
    // threaded through both roles. Wiring it on the replica path
    // (where the live node has no response stage) lets an operator
    // pre-stage the post-promotion policy with `ACK-POLICY <policy>`
    // before issuing `PROMOTE`; the same `Arc` becomes the response
    // stage's source of truth after the replica → primary transition.
    let ack_policy_atomic = Arc::new(AtomicU8::new(config.ack_policy.as_u8()));
    // The operator's override of the replica-loss halt, beside the policy
    // it is set through. Created as early, for the same reason: the admin
    // endpoint holds it from boot, and it survives a promotion. The
    // replica count it is judged against is attached once the node serves
    // as a primary (`run_as_primary`).
    let halt_state = Arc::new(melin_transport_core::halt_state::HaltState::new());
    let ack_policy_control = crate::admin::AckPolicyControl {
        policy: Arc::clone(&ack_policy_atomic),
        halt_state: Arc::clone(&halt_state),
    };

    // Validate before the bind below and before anything touches the
    // journal directory, so a refused boot has no side effects and leaves
    // the disk as it found it. Checked in every role: a replica's
    // configuration is also the one it serves under once promoted, and a
    // configuration that cannot serve is better refused now than in the
    // middle of a failover. `run_as_primary` re-checks (a promotion
    // reaches it without passing through boot).
    validate_primary_config(&config)?;

    // Bind the replication listener up front, before any pipeline thread
    // exists and regardless of role: a bad or stolen --replication-bind
    // fails the boot cleanly (nothing spawned yet to leak), and a
    // replica holds its configured port from boot, so a later promotion
    // can neither lose the port to another process nor die on a
    // mid-failover bind. Pending connections just sit in the backlog
    // until a promotion starts the sender's accept loop — replicas dial
    // the configured primary, not each other, so nothing queues there
    // in normal operation.
    let repl_listener = config
        .replication_bind
        .map(bind_replication_listener)
        .transpose()?;

    // Replica mode: connect to primary, receive journal stream, replay.
    // Must run before init_engine — the replica's journal is created from
    // the primary's segment lineage during the replication handshake.
    if let Some(primary_addr) = config.replica_of {
        info!(primary = %primary_addr, "starting in replica mode");
        // A replica receives the genesis events in the primary's history
        // and never journals its own, promoted or not — nothing to hold
        // them for, and a large genesis is worth freeing. Not even their
        // count: the promotion check reads the genesis length the
        // primary recorded in the lineage, which this replica's journal
        // header holds, never this node's configuration.
        startup.genesis = Vec::new();

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

        // The replica's control-plane bundle: promotion request (admin
        // PROMOTE / raft auto-promotion), tip readiness + advertised
        // journal tip (vote recency), primary link state, and the
        // primary's advertised ack policy. Constructed before
        // mode-detection so it survives a replica → primary transition.
        // The rotate flag is re-wired into the new primary's journal
        // stage by `run_as_primary`.
        let control = crate::replication::ReplicaControlPlane::new();
        let promotion_request = control.promote.clone();
        let rotate_flag = config.admin_bind.map(|_| Arc::new(AtomicBool::new(false)));
        let _admin_handle = config
            .admin_bind
            .map(|addr| {
                crate::admin::spawn(
                    addr,
                    Some(promotion_request.clone()),
                    rotate_flag.clone(),
                    Some(ack_policy_control.clone()),
                    Arc::clone(&shutdown),
                    Arc::clone(&authorized_keys),
                )
            })
            .transpose()?;

        // Shared fencing state, created before mode detection so it
        // survives a replica → primary transition. Seeded at epoch 0; the
        // receiver raises it from the replica's recovered journal and the
        // replication stream, and a promotion bumps it by injecting an
        // `EpochBump`. Carried into `run_as_primary` post-promotion.
        let fence_state = Arc::new(melin_transport_core::fence::FenceState::new(0));

        // Control-plane raft (observational election). Spawned before the
        // receive loop so the driver — like the fence state and admin
        // flags — survives a replica → primary promotion untouched: it
        // shares the process shutdown flag and runs on its own thread.
        // The advertised tip isn't trustworthy until journal recovery
        // seeds the fence epoch and sequence, so `control.tip_ready`
        // starts false and the receiver flips it — until then the driver
        // refuses to grant votes. The receiver owns the sequence half
        // until a promotion hands it to the new primary's journal stage.
        let journal_tip = control.journal_tip.clone();
        let mut raft = match crate::raft::build_raft_config(&config)? {
            None => crate::raft::RaftDriverGuard::disabled(&shutdown),
            Some(cfg) => crate::raft::spawn_raft_driver(
                cfg,
                config.raft_auto_promote,
                &signing_key,
                &authorized_keys,
                &fence_state,
                journal_tip.clone(),
                Arc::clone(&control.tip_ready),
                // A replica claims to be serving once a promotion is in
                // flight (see SupersessionPolicy).
                {
                    let promote = control.promote.clone();
                    Arc::new(move || promote.is_requested())
                },
                &shutdown,
            )?,
        };
        // Act on election wins when the operator opted in. Genesis
        // primaries have nothing to promote — replica paths only. The
        // guard joins the thread on every exit path.
        if config.raft_auto_promote
            && let Some(wiring) = raft.promotion_wiring()
        {
            raft.arm_promotion(crate::raft_promotion::spawn_auto_promotion(
                wiring.status,
                control.clone(),
                Arc::clone(&fence_state),
                Arc::clone(&ack_policy_atomic),
                wiring.peer_tips,
                wiring.peer_ids,
                wiring.elect_requested,
                wiring.elect_enabled,
                Arc::clone(&shutdown),
            ));
        }
        let raft_status = raft.status();
        // A raft-enabled replica exposes election gauges on --health-bind;
        // stopped explicitly before promotion so run_as_primary can rebind
        // the port, and by its guard on every other exit path.
        let mut replica_health = crate::raft::spawn_replica_health(
            &config,
            &fence_state,
            raft_status.as_ref(),
            Arc::clone(&control.pipeline_healthy),
        )?;

        // No local rotation triggers on the replica side: segment
        // rotation is primary-driven (the replica adopts the boundaries
        // announced over the replication stream), so replica journals
        // stay bitwise mirrors of the primary's. `--max-journal-mib`
        // and the admin `ROTATE` command only act on primaries.

        match crate::replication::run_receiver::<A>(
            primary_addr,
            &config.journal,
            &signing_key,
            &shutdown,
            &control,
            config.snapshot_interval_ms,
            config.shadow_snapshot_path(),
            config.cores,
            config.journal_staging_mode.into(),
            config.group_commit_delay(),
            config.replication_pipeline_depth,
            Arc::clone(&fence_state),
            &sizing,
        )? {
            // Clean shutdown — the raft and health guards tear down on drop.
            None => return Ok(()),
            Some((mut app, writer)) => {
                // Promotion! Transition to primary mode. Bump the epoch so a
                // paused/partitioned ex-primary is fenced when it reconnects.
                info!("replica promoted — transitioning to primary");
                check_promotable(&writer)?;
                // Release --health-bind before run_as_primary rebinds it
                // with the full primary health state.
                replica_health.stop();
                // A replica that ran a pipeline sized this instance
                // already; one promoted before its first session did
                // not (recovered from disk, never streamed). Sizing is
                // idempotent, so size here either way.
                <A as Application>::prefault(&mut app, &sizing);

                // A ROTATE received while this node was a replica latched
                // the flag but rotated nothing (rotation is primary-driven
                // and replicas follow the primary's boundaries). Clear it
                // so the stale latch can't fire a surprise rotation on the
                // first post-promotion fsync.
                if let Some(ref flag) = rotate_flag {
                    flag.store(false, Ordering::Release);
                }

                // The raft guard drops — stopping the driver and joining
                // the (already exited) promotion thread — after this
                // returns, so the driver serves elections and the fencing
                // channel for the whole primary tenure.
                return run_as_primary::<A, L>(
                    app,
                    writer,
                    listener,
                    repl_listener,
                    &config,
                    startup.on_primary,
                    decoder,
                    encoder,
                    event_publisher,
                    Arc::clone(&shutdown),
                    authorized_keys,
                    rotate_flag,
                    ack_policy_atomic,
                    halt_state,
                    fence_state,
                    promotion_request.pending(), // promoted — EpochBump with the request's epoch floor
                    false, // a promoted node continues the history it streamed
                    raft_status,
                    journal_tip,
                );
            }
        }
    }

    // Spawn the admin listener once if configured. PROMOTE is rejected
    // (with ERR) on a primary because no promote flag is wired here —
    // promotion is meaningful only on a replica. ROTATE is wired
    // whenever the admin endpoint is configured.
    let rotate_flag = config.admin_bind.map(|_| Arc::new(AtomicBool::new(false)));
    let _admin_handle = config
        .admin_bind
        .map(|addr| {
            crate::admin::spawn(
                addr,
                None,
                rotate_flag.clone(),
                Some(ack_policy_control.clone()),
                Arc::clone(&shutdown),
                Arc::clone(&authorized_keys),
            )
        })
        .transpose()?;

    // The configuration was validated before the replication bind above,
    // so the journal is never created by a boot that is then refused.

    // Initialize or recover the app. On a new journal, genesis is
    // journaled here, as the journal is created.
    let InitializedEngine {
        mut app,
        writer,
        recovered_epoch,
        began_history,
    } = init_engine::<A, BufferedWriter<A::Event>>(
        &config,
        &sizing,
        std::mem::take(&mut startup.genesis),
    )?;

    // Size and pre-fault application-owned memory (slabs, indices) so
    // growth and page faults happen now, not on the hot path. Runs on the
    // recovered state too: a snapshot restores contents, not capacity,
    // and `init_engine` sizes a genesis instance before replay only.
    <A as Application>::prefault(&mut app, &sizing);

    // A primary booting directly (not via promotion) keeps whatever epoch
    // its journal recovered; no bump.
    let fence_state = Arc::new(melin_transport_core::fence::FenceState::new(
        recovered_epoch,
    ));

    // Control-plane raft (observational election). The primary needs the
    // replication signing key only when raft is on — peer links
    // authenticate with it (build_raft_config enforces the flag).
    // A primary's fence epoch is already recovered here, so its tip is
    // trustworthy from the moment the driver starts. Seed the advertised
    // sequence from the recovered journal for the same reason — the
    // pipeline's journal stage takes the handle over once it runs.
    let journal_tip = melin_transport_core::AdvertisedJournalTip::new(
        melin_transport_core::WireSeq::new(writer.next_sequence().saturating_sub(1)),
    );
    let raft = match crate::raft::build_raft_config(&config)? {
        None => crate::raft::RaftDriverGuard::disabled(&shutdown),
        Some(cfg) => {
            let signing_key = load_replication_key(&config)?;
            crate::raft::spawn_raft_driver(
                cfg,
                config.raft_auto_promote,
                &signing_key,
                &authorized_keys,
                &fence_state,
                journal_tip.clone(),
                Arc::new(AtomicBool::new(true)),
                // A primary always claims to be serving.
                Arc::new(|| true),
                &shutdown,
            )?
        }
    };
    let raft_status = raft.status();

    // The raft guard drops — stopping the driver — after this returns.
    run_as_primary::<A, L>(
        app,
        writer,
        listener,
        repl_listener,
        &config,
        startup.on_primary,
        decoder,
        encoder,
        event_publisher,
        Arc::clone(&shutdown),
        authorized_keys,
        rotate_flag,
        ack_policy_atomic,
        halt_state,
        fence_state,
        None, // not promoted — no EpochBump injection
        began_history,
        raft_status,
        journal_tip,
    )
}

/// Bind the kernel-TCP replication listener, non-blocking (the sender's
/// accept loop polls the shutdown flag between accepts). Bound at boot
/// (`run_impl`) so a failure cannot leak pipeline threads and a promotion
/// cannot fail on it. Kernel TCP only: a DPDK node's replication listener
/// is a socket of its own stack, which no other process can take.
fn bind_replication_listener(
    addr: std::net::SocketAddr,
) -> Result<crate::replication::ReplicationListener, Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind(addr)
        .map_err(|e| format!("failed to bind replication listener on {addr}: {e}"))?;
    // The newtype sets non-blocking — the invariant the sender's accept
    // loop needs is enforced by construction, not by this call site.
    let listener = crate::replication::ReplicationListener::new(listener)
        .map_err(|e| format!("failed to set non-blocking on replication listener: {e}"))?;
    info!(%addr, "replication listener bound");
    Ok(listener)
}

/// Refuse a configuration no primary can run under. Called at boot,
/// before the journal is opened or created, and again by
/// `run_as_primary`, which a promotion reaches without passing through
/// boot.
fn validate_primary_config(config: &ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    // The cap sizes the io_uring rings; one they cannot be sized for
    // (0, once "unlimited", or above the ceiling) is refused here, on
    // either transport, rather than at the first ring.
    crate::connection_limit::RingSizing::for_max_connections(config.max_connections)?;
    if config.replication_bind.is_some() && config.standalone {
        return Err("--replication-bind and --standalone are mutually exclusive".into());
    }
    // `--standalone` declares "no replicas ever" — only `disk` can be
    // satisfied. Every other policy requires a second node and would
    // stall the gate forever; reject loudly at startup with the fix in
    // the message.
    if config.standalone && config.ack_policy != crate::ack_policy::AckPolicy::Disk {
        return Err(format!(
            "--standalone requires --ack-policy disk; got `{}` (this policy needs at least one connected replica)",
            config.ack_policy,
        )
        .into());
    }
    Ok(())
}

/// Refuse to promote a replica whose history does not yet hold the
/// whole genesis.
///
/// A journal begins with its genesis — sequences `1..=genesis_entries`
/// — and a replica copies it entry by entry, by catch-up, like the rest
/// of the history. A primary that dies before the replica has copied
/// all of it leaves the replica with an empty journal or a genesis
/// prefix. Promoting it would serve a state the application never
/// configured, silently and for good: a later boot recovers the history
/// as it is, and replicas follow it. (Under an ack policy that waits
/// for a replica, no client was acknowledged past such a history, since
/// every acknowledged request follows the genesis in the stream.)
/// Refuse instead, and leave the decision to the operator.
///
/// `next_sequence` is the promoted writer's; the history holds entries
/// `1..next_sequence`. A history that recovered from a snapshot counts
/// the snapshot's entries too, which is right: the snapshot's state
/// includes them.
///
/// `genesis_entries` is the lineage's genesis length from the promoted
/// writer's journal header — recorded by the primary that began the
/// history and learned by this replica when it created its journal, so
/// it is there with the primary gone, whatever genesis this node is
/// configured with. `None` (a lineage begun before the length was
/// recorded) skips the check.
fn check_promoted_history_holds_genesis(
    next_sequence: u64,
    genesis_entries: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some((held, genesis_entries)) = genesis_shortfall(next_sequence, genesis_entries) {
        return Err(format!(
            "refusing promotion: this replica's history holds {held} entries but the \
             lineage's genesis takes {genesis_entries}, so it copied only part of the genesis \
             from a primary that stopped before finishing it; restart the original primary, or \
             start the cluster again from a new journal"
        )
        .into());
    }
    Ok(())
}

/// [`check_promoted_history_holds_genesis`] on the writer a promotion
/// hands over: its next sequence, and the genesis length its journal
/// header records. What both promotion paths (kernel TCP and DPDK) run
/// before the node serves as primary.
pub(crate) fn check_promotable<E, W>(writer: &W) -> Result<(), Box<dyn std::error::Error>>
where
    E: melin_app::AppEvent,
    W: JournalWrite<E>,
{
    check_promoted_history_holds_genesis(
        writer.next_sequence(),
        writer.read_header_info()?.genesis_entries,
    )
}

/// Refuse to serve, as a primary booting from disk, a recovered history
/// that does not hold the whole genesis.
///
/// The boot-time twin of [`check_promoted_history_holds_genesis`], on
/// the same lineage-recorded length. A journal this release creates
/// always begins with its complete genesis, so a shorter history is a
/// replica's partial copy restarted on a primary's flags instead of
/// being promoted. Recovering it would serve a state the application
/// never configured, and replicas would follow it. The node's own
/// configured genesis plays no part: a configuration changed since the
/// history began changes nothing.
///
/// `next_sequence` is the recovered writer's, after `init_engine` has
/// given an empty journal its genesis (the one shortfall it can make
/// good, since nothing was served from it); `genesis_entries` its
/// journal header's. `None` — a journal written by a release that did
/// not record the length — skips the check, as those releases did.
fn check_recovered_history_holds_genesis(
    next_sequence: u64,
    genesis_entries: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some((held, genesis_entries)) = genesis_shortfall(next_sequence, genesis_entries) {
        return Err(format!(
            "refusing to start: the recovered history holds {held} entries but the lineage's \
             genesis takes {genesis_entries}, so it holds only part of the genesis (a replica's \
             partial copy started as a primary); start again from a new history (remove the \
             journal, its archives and its snapshots), or restart this node as a replica of a \
             primary holding the whole history"
        )
        .into());
    }
    Ok(())
}

/// `(held, genesis_entries)` when a history ending before
/// `next_sequence` holds fewer entries than the genesis takes; `None`
/// when it holds them all, or when the genesis length is unknown.
fn genesis_shortfall(next_sequence: u64, genesis_entries: Option<u64>) -> Option<(u64, u64)> {
    let genesis_entries = genesis_entries?;
    let held = next_sequence.saturating_sub(1);
    (held < genesis_entries).then_some((held, genesis_entries))
}

/// Load the Ed25519 replication signing key from `--replication-key` —
/// shared by the replica connect path and raft-enabled primaries.
fn load_replication_key(
    config: &ServerConfig,
) -> Result<ed25519_dalek::SigningKey, Box<dyn std::error::Error>> {
    let path = config.replication_key.as_ref().ok_or_else(|| {
        std::io::Error::other("--replication-key is required (replica mode or --raft-bind)")
    })?;
    let seed = std::fs::read(path).map_err(|e| {
        std::io::Error::other(format!(
            "failed to read replication key {}: {e}",
            path.display()
        ))
    })?;
    if seed.len() != 32 {
        return Err(format!(
            "replication key must be 32 bytes, got {} ({})",
            seed.len(),
            path.display()
        )
        .into());
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&seed);
    Ok(ed25519_dalek::SigningKey::from_bytes(&bytes))
}

/// Run the server as a primary: build the disruptor pipeline, spawn
/// pipeline threads, journal the application's startup events, then
/// accept client connections.
///
/// Control event for the response stage. The io_uring response path reads
/// `fd` for I/O; the `writer` keeps the fd alive via ownership.
pub use crate::ControlEvent;

/// Joinable handles for every long-lived thread spawned by a primary.
/// Optional handles are `None` when their feature is disabled (e.g.,
/// no replication, no health endpoint) or unsupported on a transport
/// (e.g., DPDK runs without an event publisher or shadow snapshotter).
struct PipelineHandles<A: Send + 'static, W: Send + 'static> {
    journal: std::thread::JoinHandle<Result<W, JournalError>>,
    matching: std::thread::JoinHandle<A>,
    response: std::thread::JoinHandle<()>,
    replication: Option<std::thread::JoinHandle<()>>,
    event_publisher: Option<std::thread::JoinHandle<()>>,
    shadow: Option<std::thread::JoinHandle<()>>,
    health: Option<std::thread::JoinHandle<()>>,
    // No standalone tick thread on either transport: io_uring emits ticks
    // from inside the reader (via IORING_OP_TIMEOUT) and DPDK emits them
    // from inside the poll thread (via a wall-clock check between bursts).
}

/// Drain the pipeline and join every worker thread, surfacing panics
/// and journal-stage errors as a single `pipeline failure` return.
///
/// `extras` is a list of pre-joined results (used by the DPDK path,
/// which joins its poll threads before draining the pipeline).
fn shutdown_pipeline_stages<A: Send + 'static, W: Send + 'static>(
    handles: PipelineHandles<A, W>,
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
        error!(thread = "journal-seq", error = %e, "journal stage returned error");
    }
    check_join("journal-seq", journal_result.map(|_| ()));
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

    // After every stage thread has joined, dump the per-stage latency
    // histograms to stderr. No-op when `latency-trace` is disabled.
    melin_transport_core::trace::print_report_all();

    if thread_panicked || journal_failed {
        error!("shutdown complete (with pipeline failure)");
        return Err("pipeline failure".into());
    }

    info!("shutdown complete");
    Ok(())
}

/// Stop the stages a primary already spawned when a later one cannot
/// start, so the startup error goes back to the caller with no thread
/// left running behind it.
fn abort_startup<A: Send + 'static, W: Send + 'static>(
    handles: PipelineHandles<A, W>,
    pipeline_healthy: &AtomicBool,
    shutdown: &AtomicBool,
) {
    // Dropped result: the failures it can carry (a panicked thread, a
    // journal error) are logged by `shutdown_pipeline_stages` itself, and
    // the stage that could not start is the error the caller returns.
    let _ = shutdown_pipeline_stages(handles, Vec::new(), pipeline_healthy, shutdown);
}

/// Used by both the normal primary startup path and the promotion path
/// (replica → primary transition).
///
/// `rotate_flag` is the shared `AtomicBool` toggled by the admin
/// endpoint (or `None` if no admin endpoint was configured). On the
/// promotion path the replica's journal stage is torn down and a new
/// primary stage is built here; passing the flag through means the
/// admin endpoint, which was spawned once at process start, keeps
/// driving the new stage's rotation.
#[allow(clippy::too_many_arguments)]
fn run_as_primary<A, L>(
    app: A,
    writer: BufferedWriter<A::Event>,
    mut listener: L,
    // Pre-bound replication listener (non-blocking by construction),
    // `Some` iff `--replication-bind` is set. Bound in `run_impl` before
    // any pipeline thread exists so a bind failure cannot leak threads,
    // and held from boot on replicas so a promotion cannot fail on it.
    repl_listener: Option<crate::replication::ReplicationListener>,
    config: &ServerConfig,
    // The `on_primary` half of the application's `StartupEvents`. Genesis
    // never reaches here: it is journaled as the journal is created, in
    // `init_engine`, and a promoted node has it from the stream.
    on_primary: Vec<A::Event>,
    decoder: RequestDecoderArc<A>,
    encoder: ResponseEncoderArc<A>,
    event_publisher: Option<EventPublisherFn<A>>,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
    rotate_flag: Option<Arc<AtomicBool>>,
    ack_policy_atomic: Arc<AtomicU8>,
    // The operator's override of the replica-loss halt, shared with the
    // admin endpoint. This function attaches the pipeline's replica count.
    halt_state: Arc<melin_transport_core::halt_state::HaltState>,
    fence_state: Arc<melin_transport_core::fence::FenceState>,
    promotion: Option<u64>,
    // This boot began the history (`InitializedEngine::began_history`):
    // with replication on, client service waits for the first replica.
    began_history: bool,
    raft_status: Option<Arc<melin_transport_core::health::RaftStatus>>,
    // Control-plane advertised tip. This primary's journal stage becomes
    // its writer (the raft driver keeps reading the same handle across a
    // promotion) — see `AdvertisedJournalTip`.
    journal_tip: melin_transport_core::AdvertisedJournalTip,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    L: BlockingTransportListener,
{
    // Active connection counter shared between accept loop, response
    // stage, and matching stage (for stats queries).
    // Incremented on successful auth, decremented on disconnect.
    // Used to enforce max_connections (SEC-02).
    let active_connections = Arc::new(AtomicU64::new(0));

    // Determine replication mode. Every application's binary ships the
    // full durable transport (journal + replication + shadow), so the
    // same config knob drives any of them.
    let enable_replication = config.replication_bind.is_some();
    // Re-checked: a promotion reaches here without the boot-time check.
    validate_primary_config(config)?;
    // The reader's and the response stage's io_uring sizes, from the
    // connection cap the accept loop below enforces.
    let ring_sizing =
        crate::connection_limit::RingSizing::for_max_connections(config.max_connections)?;
    // The lineage's genesis length, read before the writer moves into
    // the pipeline: the shadow stage stamps it into every snapshot.
    let genesis_entries = writer.read_header_info()?.genesis_entries;
    // Clone the application for the shadow snapshot stage before the pipeline
    // consumes it. `Application` does not require `Clone`, so this goes
    // through `clone_via_snapshot` (a snapshot round-trip unless the
    // application overrides it with something cheaper).
    let enable_shadow = config.snapshot_interval_ms > 0;
    let shadow_app = if enable_shadow {
        Some(<A as Application>::clone_via_snapshot(&app)?)
    } else {
        None
    };

    // Build the disruptor pipeline with optional replication consumer.
    // An application without an event publisher silently ignores
    // `--event-bind`, so the same invocation works against any binary.
    // The caller (binary) decides whether an event-publisher fn is
    // available; we only allocate the consumer slot when both the fn is
    // wired AND `--event-bind` is set.
    let enable_event_publisher = event_publisher.is_some() && config.event_bind.is_some();
    let Pipeline {
        input_producer,
        journal_stage,
        matching_stage,
        mut output_consumers,
        events_processed,
        input_cursor,
        replication_consumers,
        replicas_connected,
        shadow_consumer,
        chain_hash_lock,
        replication_ring_progress,
        cursors,
    } = build_pipeline_with_replication(
        app,
        writer,
        config.group_commit_delay(),
        Arc::clone(&active_connections),
        enable_replication,
        config.max_journal_batch,
        config.replication_ring_size,
        config.cores.stage_waits(),
        enable_event_publisher,
        enable_shadow,
        Arc::clone(&fence_state),
    );
    // Ring-position cursors for the on_primary drain gate below (Acquire loads,
    // stronger than the bundle's monitoring reads). The ack gate and
    // the replication sender pull their typed handles straight from
    // `cursors`; the health endpoint takes `cursors` itself — the quorum
    // and fastest-replica gauges are derived from the bundle's per-slot
    // cursors at read time.
    let journal_cursor = cursors.journal_ring_arc();
    let matching_cursor = cursors.matching_ring_arc();

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

    // Client writes a halted node refuses at ingress, reader → response
    // stage, counted for the health endpoint. See `crate::halt`.
    let refused_writes = Arc::new(AtomicU64::new(0));
    attach_halt_state(&halt_state, replicas_connected.as_ref())?;
    let halt_gate = crate::halt::HaltGate::new(
        replicas_connected.clone(),
        Arc::clone(&halt_state),
        Arc::clone(&fence_state),
        Arc::clone(&refused_writes),
    );
    let (refusal_tx, refusal_rx) =
        crate::halt::refusal_channel::<A::Report>(Arc::clone(&matching_cursor));

    // Spawn the io_uring reader thread. A single reader uses multishot RECV
    // to multiplex every TCP connection on the server. Pinned to
    // cores.reader. The matching stage is the throughput limit, so a
    // second reader would not raise throughput — only re-introduce the
    // multi-producer ordering race on the input ring.
    let connection_timeout = config.connection_timeout();
    let heartbeat_interval = config.heartbeat_interval();

    // Startup events flow through the disruptor like regular events so
    // they're journaled, replicated, and processed by the matching stage via
    // the normal pipeline. The input ring is single-producer: main publishes
    // them through `input_producer`, then moves it into the reader thread
    // which becomes the sole steady-state producer. No cloning required.
    let mut input_producer = input_producer;

    // Spawn pipeline OS threads.
    let cores = config.cores;

    // Wire runtime journal rotation into the journal stage. The flag is
    // shared with the admin endpoint (spawned once at process start in
    // `run`) — passing it through here means PROMOTE-then-
    // ROTATE on a freshly-promoted node still drives rotation against
    // the new primary's stage. The size threshold uses the same
    // `max_journal_mib` knob that drives startup rotation.
    let mut journal_stage = journal_stage;
    let max_journal_bytes = config.max_journal_mib.saturating_mul(1024 * 1024);
    journal_stage.set_rotation(max_journal_bytes, rotate_flag.clone());
    config.cores.place_journal_children(&mut journal_stage);
    journal_stage.set_staging_mode(config.journal_staging_mode.into());
    // On a primary the journal stage owns the control-plane advertised
    // tip (durable cursor after each fsync batch). On the promotion path
    // this takes over the handle the receiver was advancing.
    journal_stage.set_advertised_tip_publisher(journal_tip.clone());
    if config.max_journal_mib > 0 {
        info!(
            max_journal_mib = config.max_journal_mib,
            "runtime journal rotation enabled (size threshold)"
        );
    }

    // Extract utilization handles before stages are moved into threads.
    let journal_utilization = journal_stage.utilization();
    let matching_utilization = matching_stage.utilization();
    let response_utilization = Arc::new(melin_transport_core::pipeline::StageUtilization::new());

    // Wrap each spawn closure with a tail log so a thread that returns
    // (cleanly OR via `Err`) is visible in the trace stream — the
    // accept loop's `is_finished` check fires error!("pipeline thread
    // died") only on its next tick, which can lag the actual exit by
    // up to 100ms and races with the listener teardown.
    let s1 = Arc::clone(&shutdown);
    let shutdown_for_journal = Arc::clone(&shutdown);
    // Start the journal's disk and preparer threads here rather than on
    // journal-seq, which is pinned by the time it runs: a thread inherits
    // its creator's placement. See `JournalStage::start`.
    let sequencer = journal_stage
        .start()
        .map_err(|e| format!("start the journal stage: {e}"))?;
    let journal_handle = std::thread::Builder::new()
        .name("journal-seq".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("journal-seq", cores.journal_seq.core);
            let result = sequencer.run(&s1);
            let was_shutdown = shutdown_for_journal.load(Ordering::Relaxed);
            match &result {
                Ok(_) if was_shutdown => info!("journal-seq thread exited cleanly on shutdown"),
                Ok(_) => error!("journal-seq thread returned without shutdown signal"),
                Err(e) => error!(error = %e, "journal-seq thread returned Err"),
            }
            result
        })
        .map_err(|e| format!("spawn journal-seq thread: {e}"))?;

    let s2 = Arc::clone(&shutdown);
    let shutdown_for_matching = Arc::clone(&shutdown);
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("matching", cores.matching.core);
            let app = matching_stage.run(&s2);
            let was_shutdown = shutdown_for_matching.load(Ordering::Relaxed);
            if was_shutdown {
                info!("matching thread exited cleanly on shutdown");
            } else {
                error!("matching thread returned without shutdown signal");
            }
            app
        })
        .map_err(|e| format!("spawn matching thread: {e}"))?;

    let replication_metrics = build_replication_metrics(
        replication_consumers.is_some(),
        &ack_policy_atomic,
        config.ack_policy,
    );

    // Per-slot active flags exposed by the journal stage's replication
    // ring; the response gate filters disconnected slots out of the
    // policy's cursor view via these.
    let replica_active: Option<[Arc<AtomicBool>; 2]> =
        replication_ring_progress.as_ref().map(|rp| {
            [
                Arc::clone(&rp.active_flags[0]),
                Arc::clone(&rp.active_flags[1]),
            ]
        });

    // Typed durable-cursor handle for the response thread's gate.
    let journal_persisted_wire_seq_response = cursors.durable_wire_seq();
    let replication_metrics_response = replication_metrics.as_ref().map(Arc::clone);
    let replica_active_response = replica_active.clone();
    let ack_policy_response = Arc::clone(&ack_policy_atomic);
    let s3 = Arc::clone(&shutdown);
    let shutdown_for_response = Arc::clone(&shutdown);
    let response_utilization_thread = Arc::clone(&response_utilization);
    let response_fence = Arc::clone(&fence_state);
    let active_connections_response = Arc::clone(&active_connections);
    // The stage's startup report — see `Response::ready`. Capacity one so
    // the report never blocks the stage on this thread reaching `recv`.
    let (response_ready_tx, response_ready_rx) = std::sync::mpsc::sync_channel(1);
    let response_handle = std::thread::Builder::new()
        .name("response".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("response", cores.response.core);
            let started = crate::response::run::<A>(
                output_consumer,
                control_rx,
                crate::response::Response::<A> {
                    journal_persisted_wire_seq: journal_persisted_wire_seq_response,
                    ack_policy: ack_policy_response,
                    replication_metrics: replication_metrics_response,
                    replica_active: replica_active_response,
                    heartbeat_interval,
                    wait: cores.response.wait,
                    utilization: response_utilization_thread,
                    encoder,
                    fence_state: response_fence,
                    active_connections: active_connections_response,
                    refusals: refusal_rx,
                    ring_sizing,
                    ready: Some(response_ready_tx),
                    #[cfg(test)]
                    pause_after_control_drain: None,
                },
                &s3,
            );
            if !started {
                // Never ran: the reason went back through the startup
                // report, and the spawner turns it into the node's error.
                return;
            }
            let was_shutdown = shutdown_for_response.load(Ordering::Relaxed);
            if was_shutdown {
                info!("response thread exited cleanly on shutdown");
            } else {
                error!("response thread returned without shutdown signal");
            }
        })
        .map_err(|e| format!("spawn response thread: {e}"))?;

    // A stage without its ring is a node that cannot answer anyone:
    // refuse to start, after stopping the stages already running.
    if let Err(e) = crate::connection_limit::await_startup("response", &response_ready_rx) {
        abort_startup(
            PipelineHandles {
                journal: journal_handle,
                matching: matching_handle,
                response: response_handle,
                replication: None,
                event_publisher: None,
                shadow: None,
                health: None,
            },
            &AtomicBool::new(false),
            &shutdown,
        );
        return Err(e.into());
    }

    // Spawn replication sender thread if enabled. The journal stage publishes
    // encoded batches to a pre-allocated ring; the sender thread consumes them.
    // `replica_ready` is set when the first replica enters live streaming;
    // the bring-up gate below waits on it.
    let replica_ready = Arc::new(AtomicBool::new(false));
    // Ring depth monitoring: the producer cursors are in ReplicationRingProgress
    // (owned by this function), so we compute depth via ring_progress rather
    // than storing Box<dyn QueueCursor> in ReplicationMetrics. The health
    // snapshot reads consumer cursors from ReplicationMetrics and producer
    // cursors are not needed — ring depth is a secondary metric.

    let replication_handle = if let Some((repl_consumer_1, repl_consumer_2)) = replication_consumers
    {
        let s_repl = Arc::clone(&shutdown);
        let repl_slots = cursors.replica_slot_cursors();
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
        let handlers = [cores.repl_handler_0, cores.repl_handler_1];
        let sender_fence = Arc::clone(&fence_state);
        let sender_ack_policy = Arc::clone(&ack_policy_atomic);
        let sender_halt_state = Arc::clone(&halt_state);
        // Bound in `run_impl` before any pipeline thread was spawned —
        // see the `repl_listener` parameter doc.
        let repl_listener = repl_listener
            .ok_or("replication listener must be pre-bound when replication is enabled")?;
        let repl_accept_handle = std::thread::Builder::new()
            // `repl-accept`, not `repl-sender`: this thread sends nothing.
            // The ring consumers are moved into the per-replica handler
            // threads, which do all the streaming; this one accepts
            // connections, reaps finished handlers, and sleeps 50 ms.
            .name("repl-accept".into())
            .spawn(move || {
                // Deliberately NOT pinned. Reserving a core — at
                // SCHED_FIFO, on an isolated one — for a thread that idles
                // 99.9% of the time bought nothing.
                //
                // Staying unpinned is now load-bearing, not just thrifty.
                // The handler threads are spawned from here and inherit
                // this thread's affinity and scheduling policy at
                // creation; a child of a pinned real-time parent cannot
                // move itself off that core, because moving itself
                // requires running. That is the DPDK handshake hang, and
                // this path escaped it only because the 50 ms sleep
                // happened to yield the core. See
                // `replication::validation_worker`.
                //
                // `PipelineCores` consequently has no field for it, and
                // `--cores` no name.
                crate::replication::run_sender::<A>(
                    crate::replication::Sender {
                        listener: repl_listener,
                        repl_consumer_1,
                        repl_consumer_2,
                        replica_slots: repl_slots,
                        journal_path,
                        authorized_keys: repl_auth_keys,
                        evict_flags,
                        active_flags,
                        metrics: repl_metrics,
                        handlers,
                        batch_size,
                        heartbeat_secs,
                        fence_state: sender_fence,
                        ack_policy: sender_ack_policy,
                        halt_state: sender_halt_state,
                    },
                    &s_repl,
                    &ready_flag,
                    &connected_counter,
                );
            })
            .map_err(|e| format!("spawn replication sender thread: {e}"))?;

        Some(repl_accept_handle)
    } else {
        if !config.standalone && config.replica_of.is_none() {
            info!("running in standalone mode (no replication)");
        }
        None
    };

    // Spawn event publisher thread if enabled. Consumes from output ring
    // consumer 1 and broadcasts the application's reports to TCP
    // subscribers. Caller-supplied (an application with an output feed
    // wires its publisher; one without passes `None`), so the runtime
    // carries no application-specific reference.
    let event_publisher_handle = spawn_event_publisher::<A>(
        event_publisher_consumer,
        event_publisher,
        config,
        &cores,
        &authorized_keys,
        &shutdown,
    )?;

    let shadow_handle = spawn_shadow_stage::<A>(
        shadow_consumer,
        shadow_app,
        chain_hash_lock,
        config,
        &cores,
        &shutdown,
        fence_state.epoch(),
        genesis_entries,
    )?;

    // Spawn the health endpoint BEFORE `journal_on_primary_events` and
    // the accept loop so operators (and the failover test harness) can
    // probe `/healthz` to confirm the server has bound its sockets and
    // is ready to accept replica connections. Replicas connect to the
    // *replication* endpoint, not the client port, so spawning health
    // here doesn't change accept semantics.
    let pipeline_healthy = Arc::new(AtomicBool::new(true));
    let health_handle = spawn_health_endpoint(
        config,
        &active_connections,
        &events_processed,
        &refused_writes,
        &cursors,
        input_cursor,
        &pipeline_healthy,
        &replicas_connected,
        &halt_state,
        &fence_state,
        &replication_metrics,
        &replica_active,
        &replication_ring_progress,
        &journal_utilization,
        &matching_utilization,
        &response_utilization,
        &shutdown,
        &raft_status,
    )?;

    // Promotion fencing: the bump is the first entry of a promoted node's
    // tenure. See `journal_promotion_epoch_bump`.
    if let Some(requested_epoch) = promotion {
        journal_promotion_epoch_bump(
            requested_epoch,
            &fence_state,
            &mut input_producer,
            &shutdown,
        );
    }

    // Bring-up gate: a primary that began the history with replication
    // on serves no client until its first replica is streaming live. A
    // write before then would be refused (no replica connected) under
    // every ack policy that requires one, so a new cluster's first
    // clients are held instead of refused while it assembles. Genesis is
    // already durable in the journal (`init_engine` creates the journal
    // with it), so a shutdown during the wait loses nothing; the wait
    // yields to it, and the unified shutdown below joins every thread.
    // A restarted or promoted primary continues a history and does not
    // wait: losing its replicas then is a halt, not a bring-up.
    if enable_replication && began_history {
        info!("new history: waiting for the first replica before serving clients");
        while !replica_ready.load(Ordering::Acquire) && !shutdown.load(Ordering::Relaxed) {
            // A sleep, not the orchestrator's spin: the wait lasts as
            // long as the operator takes to start a replica.
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    // `on_primary` — after the epoch bump on promotion, so the bump stays
    // the first entry of the tenure.
    journal_on_primary_events(
        on_primary,
        &mut input_producer,
        &journal_cursor,
        &matching_cursor,
        &replication_ring_progress,
        &shutdown,
    );

    // Now that the on_primary events are fully drained, spawn the reader
    // thread. From here on the reader is the sole producer on the input ring
    // (the on_primary loop has finished and its clone of `input_producer`
    // was dropped at the end of the block above). Any subsequent ticks the
    // reader emits cannot race with on_primary events: there are none in
    // flight.
    //
    // If shutdown was requested while they were draining we still spawn the
    // reader so the unified shutdown sequence below joins every thread.
    let reader_shutdown = Arc::new(AtomicBool::new(false));
    let mut reader_handle = match crate::reader::spawn_reader::<A, _>(
        input_producer,
        decoder,
        halt_gate,
        refusal_tx,
        control_tx.clone(),
        config.cores.reader.core,
        connection_timeout,
        config.tick_interval(),
        ring_sizing,
        Arc::clone(&reader_shutdown),
    ) {
        Ok(handle) => handle,
        Err(e) => {
            // No reader, no client is ever served: refuse to start, after
            // stopping every stage already running.
            abort_startup(
                PipelineHandles {
                    journal: journal_handle,
                    matching: matching_handle,
                    response: response_handle,
                    replication: replication_handle,
                    event_publisher: event_publisher_handle,
                    shadow: shadow_handle,
                    health: health_handle,
                },
                &pipeline_healthy,
                &shutdown,
            );
            return Err(e.into());
        }
    };

    // Health endpoint and `pipeline_healthy` were already spawned/created
    // earlier (before `journal_on_primary_events`) so probes can succeed
    // before the node accepts clients.

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
        if crate::connection_limit::connection_cap_reached(
            active_connections.load(Ordering::Relaxed),
            config.max_connections,
        ) {
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
        let (role, public_key_bytes) = match authenticate_connection(
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

        // The identity the application sees as `ApplyCtx::key_hash`.
        let key_hash = melin_app::key_hash(&public_key_bytes);

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

        // Belt-and-braces write timeout (SEC-01). The real slow-client
        // protection is in the response stage — MSG_DONTWAIT sends,
        // paced retries, and the blocked-send timeout — since io_uring
        // SEND does not honor SO_SNDTIMEO. This bounds any residual
        // blocking write(2) on this fd (none exist today; the response
        // stage is the sole egress writer).
        if let Err(e) = set_write_timeout(&std_write, Some(std::time::Duration::from_secs(5))) {
            debug!(connection_id = connection_id.0, error = %e, "failed to set write timeout");
        }

        // Register the writer with the response thread before the
        // reader, and keep it that way: the response stage reads its
        // output ring before it drains this channel, and relies on
        // `Connected` being queued before any request of the connection
        // can be read — see the drain in `response::run`. Queued, not
        // yet applied: the stage picks the event up on its next
        // iteration, which that order makes early enough.
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
            role,
            key_hash,
        });
    }

    // --- Ordered shutdown sequence ---
    // 1. Stop readers first so no new events enter the disruptor.
    info!("shutdown: stopping reader thread");
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

/// Run the server with DPDK kernel-bypass networking.
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
///
/// See [`run`] for the role of `startup`.
#[cfg(feature = "dpdk")]
fn run_dpdk<A>(
    config: ServerConfig,
    startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: RequestDecoderArc<A>,
    encoder: ResponseEncoderArc<A>,
    event_publisher: Option<EventPublisherFn<A>>,
    authorized_keys: Arc<AuthorizedKeys>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    check_keys_match_decoder(&*decoder, &authorized_keys)?;
    let dpdk_config = dpdk_config_from(&config)?;

    run_dpdk_impl::<A>(
        config,
        startup,
        sizing,
        decoder,
        encoder,
        event_publisher,
        authorized_keys,
        dpdk_config,
        shutdown,
    )
}

/// Translate the CLI surface into a [`melin_dpdk::DpdkConfig`].
///
/// Every operator-supplied string is validated here, before EAL init and
/// before any thread is spawned, and a bad value is returned as an error
/// rather than a panic. That ordering is the point: the peer MAC is only
/// consumed inside the replica's reconnect loop, so parsing it there
/// would turn a typo into a crash of an already-running node.
#[cfg(feature = "dpdk")]
fn dpdk_config_from(cfg: &ServerConfig) -> Result<melin_dpdk::DpdkConfig, String> {
    let parse_ip = |value: &str, flag: &str| -> Result<std::net::Ipv4Addr, String> {
        value
            .parse()
            .map_err(|e| format!("invalid {flag} address '{value}': {e}"))
    };
    let parse_mac = |value: &str, flag: &str| -> Result<[u8; 6], String> {
        melin_dpdk::try_parse_mac(value).map_err(|e| format!("invalid {flag}: {e}"))
    };

    Ok(melin_dpdk::DpdkConfig {
        eal_args: cfg
            .dpdk_eal_args
            .split_whitespace()
            .map(String::from)
            .collect(),
        port_ids: cfg.dpdk_ports.clone(),
        ip_addr: parse_ip(&cfg.dpdk_ip, "--dpdk-ip")?,
        prefix_len: cfg.dpdk_prefix_len,
        gateway: cfg
            .dpdk_gateway
            .as_deref()
            .map(|s| parse_ip(s, "--dpdk-gateway"))
            .transpose()?,
        listen_port: cfg.bind.port(),
        mtu: cfg.dpdk_mtu,
        vlan_id: cfg.dpdk_vlan,
        peer_ip: cfg
            .dpdk_peer_ip
            .as_deref()
            .map(|s| parse_ip(s, "--dpdk-peer-ip"))
            .transpose()?,
        gateway_mac: cfg
            .dpdk_gateway_mac
            .as_deref()
            .map(|s| parse_mac(s, "--dpdk-gateway-mac"))
            .transpose()?,
        peer_mac: cfg
            .dpdk_peer_mac
            .as_deref()
            .map(|s| parse_mac(s, "--dpdk-peer-mac"))
            .transpose()?,
        num_queues: 1,
    })
}

#[cfg(feature = "dpdk")]
#[allow(clippy::too_many_arguments)] // boot assembly point, as run_impl
fn run_dpdk_impl<A>(
    config: ServerConfig,
    mut startup: StartupEvents<A::Event>,
    sizing: A::Sizing,
    decoder: RequestDecoderArc<A>,
    encoder: ResponseEncoderArc<A>,
    event_publisher: Option<EventPublisherFn<A>>,
    authorized_keys: Arc<AuthorizedKeys>,
    dpdk_config: melin_dpdk::DpdkConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    warn_if_chain_disabled();

    // As on the kernel-TCP path: resolve the layout once, up front. The
    // reader entry pins the NIC poll thread here, which never yields.
    let config = ServerConfig {
        cores: config.resolved_cores(ReaderThread::DpdkPoll)?,
        ..config
    };
    log_layout(&config.cores);
    // The one placement the layout check cannot make honest: the poll
    // thread polls flat out whatever its entry says, and unpinned it
    // does so wherever the scheduler puts it — next to whatever else is
    // there. Accepted (test hosts run this way), but said out loud.
    if !config.cores.reader.is_pinned() {
        warn!(
            "--cores: the reader is unpinned, but on DPDK it is the NIC poll thread and \
             busy-polls regardless; it will take a full core wherever the scheduler places \
             it. Give it a core of its own"
        );
    }

    // Mirrors the kernel-TCP `run` path: one atomic per
    // process, threaded into both replica (pre-staging for promotion)
    // and primary admin listeners.
    let ack_policy_atomic = Arc::new(AtomicU8::new(config.ack_policy.as_u8()));
    // The halt override beside it, as on the kernel-TCP path.
    let halt_state = Arc::new(melin_transport_core::halt_state::HaltState::new());
    let ack_policy_control = crate::admin::AckPolicyControl {
        policy: Arc::clone(&ack_policy_atomic),
        halt_state: Arc::clone(&halt_state),
    };
    // As on the kernel-TCP path: refuse a configuration no primary can run
    // under before EAL init and before anything touches the journal, in
    // every role — a replica's configuration is the one it serves under
    // once promoted.
    validate_primary_config(&config)?;
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
        // As on the kernel-TCP path: a replica never journals genesis,
        // and its promotion check reads the lineage's genesis length from
        // its journal header, not its configuration.
        startup.genesis = Vec::new();

        // Load the replication signing key — the replica signs the primary's
        // challenge with it. Mirrors the kernel-TCP replica path.
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

        // The replica's control-plane bundle, same shape as the kernel
        // TCP replica path — see `run` for the rationale on lifetime.
        let control = crate::replication::ReplicaControlPlane::new();
        let promotion_request = control.promote.clone();
        let rotate_flag = config.admin_bind.map(|_| Arc::new(AtomicBool::new(false)));
        let _admin_handle = config
            .admin_bind
            .map(|addr| {
                crate::admin::spawn(
                    addr,
                    Some(promotion_request.clone()),
                    rotate_flag.clone(),
                    Some(ack_policy_control.clone()),
                    Arc::clone(&shutdown),
                    Arc::clone(&authorized_keys),
                )
            })
            .transpose()?;

        // Shared fencing state, created before mode detection so it
        // survives a replica → primary transition. Seeded at epoch 0; the
        // receiver raises it from the replica's recovered journal and the
        // replication stream, and a promotion bumps it by injecting an
        // `EpochBump`. Carried into `run_as_primary_dpdk` post-promotion.
        let fence_state = Arc::new(melin_transport_core::fence::FenceState::new(0));

        // Control-plane raft runs on kernel TCP even on DPDK nodes — the
        // control plane is off the hot path and must not consume a DPDK
        // queue. Same promotion-surviving placement and tip ownership as
        // the kernel replica path.
        let journal_tip = control.journal_tip.clone();
        let mut raft = match crate::raft::build_raft_config(&config)? {
            None => crate::raft::RaftDriverGuard::disabled(&shutdown),
            Some(cfg) => crate::raft::spawn_raft_driver(
                cfg,
                config.raft_auto_promote,
                &signing_key,
                &authorized_keys,
                &fence_state,
                journal_tip.clone(),
                Arc::clone(&control.tip_ready),
                // A replica claims to be serving once a promotion is in
                // flight (see SupersessionPolicy).
                {
                    let promote = control.promote.clone();
                    Arc::new(move || promote.is_requested())
                },
                &shutdown,
            )?,
        };
        // Act on election wins when the operator opted in. Genesis
        // primaries have nothing to promote — replica paths only. The
        // guard joins the thread on every exit path.
        if config.raft_auto_promote
            && let Some(wiring) = raft.promotion_wiring()
        {
            raft.arm_promotion(crate::raft_promotion::spawn_auto_promotion(
                wiring.status,
                control.clone(),
                Arc::clone(&fence_state),
                Arc::clone(&ack_policy_atomic),
                wiring.peer_tips,
                wiring.peer_ids,
                wiring.elect_requested,
                wiring.elect_enabled,
                Arc::clone(&shutdown),
            ));
        }
        let raft_status = raft.status();
        let mut replica_health = crate::raft::spawn_replica_health(
            &config,
            &fence_state,
            raft_status.as_ref(),
            Arc::clone(&control.pipeline_healthy),
        )?;

        // No local rotation triggers on the replica side — rotation is
        // primary-driven (see the kernel-TCP receiver path).

        // Queue 0's transport carries the replica's link to its primary
        // and, should this node be promoted, everything it serves after:
        // a promoted replica is a DPDK primary on this same transport (see
        // the promotion arm below). It listens on nothing while the node
        // is a replica — a replica serves nothing inbound, and the stack
        // refuses a connection attempt — and gains its listeners at
        // promotion.
        let mut transport =
            melin_dpdk::DpdkTransport::from_shared_unlistening(&shared, &dpdk_config, 0)?;
        transport.send_gratuitous_arp();

        let primary_ipv4 = match primary_addr.ip() {
            std::net::IpAddr::V4(ip) => ip,
            std::net::IpAddr::V6(_) => {
                return Err("DPDK replication requires IPv4".into());
            }
        };

        match crate::replication::run_receiver_dpdk::<A>(
            &mut transport,
            primary_ipv4,
            primary_addr.port(),
            &signing_key,
            &config.journal,
            &shutdown,
            &control,
            config.snapshot_interval_ms,
            config.shadow_snapshot_path(),
            config.cores,
            config.journal_staging_mode.into(),
            config.group_commit_delay(),
            config.replication_pipeline_depth,
            Arc::clone(&fence_state),
            &sizing,
        )? {
            // Clean shutdown — the raft and health guards tear down on drop.
            None => return Ok(()),
            Some((mut app, writer)) => {
                // Promotion! Transition to primary mode, on DPDK: the node
                // serves on the transport it ran on, as a promoted
                // kernel-TCP replica does. The receiver tore its pipeline
                // down and handed over its application and journal writer;
                // every link it opened is reset.
                info!("replica promoted (DPDK) — transitioning to primary");
                // See the kernel-TCP promotion path.
                check_promotable(&writer)?;
                // Release --health-bind before run_as_primary_dpdk rebinds it.
                replica_health.stop();
                // Idempotent; see the kernel-TCP promotion path.
                <A as Application>::prefault(&mut app, &sizing);

                // Clear a ROTATE latched while this node was a replica —
                // see the kernel-TCP promotion path.
                if let Some(ref flag) = rotate_flag {
                    flag.store(false, Ordering::Release);
                }

                // This thread ran the receiver, pinned to the reader's core
                // (on an isolated core, at real-time priority) once it
                // streamed, and it spawns the primary's threads next: a
                // child inheriting that placement could never run to pin
                // itself. Unpinned first, as the receiver does before it
                // builds a pipeline; it is pinned again as the poll thread.
                if let Err(e) = melin_app::affinity::clear_affinity() {
                    warn!(
                        error = e,
                        "failed to clear the receiver's affinity before promotion"
                    );
                }

                // What `DpdkTransport::from_shared` gives a primary at
                // boot: the client listener, with the client buffers. The
                // replication listener is added with the primary's other
                // replication wiring, as at boot.
                transport
                    .add_listener(dpdk_config.listen_port)
                    .map_err(|e| format!("add the client listener on promotion: {e}"))?;
                // As at boot: the switch may have aged out this port's MAC
                // while the node only dialled out.
                transport.send_gratuitous_arp();

                // The raft guard drops — stopping the driver and joining
                // the (already exited) promotion thread — after this
                // returns; see the kernel-TCP promotion path.
                return run_as_primary_dpdk::<A>(
                    app,
                    writer,
                    transport,
                    &dpdk_config,
                    &config,
                    startup.on_primary,
                    decoder,
                    encoder,
                    event_publisher,
                    Arc::clone(&shutdown),
                    authorized_keys,
                    rotate_flag,
                    ack_policy_atomic,
                    halt_state,
                    fence_state,
                    promotion_request.pending(), // promoted — EpochBump with the request's epoch floor
                    raft_status,
                    journal_tip,
                );
            }
        }
    }

    // Create per-thread transports (each gets its own queue pair + smoltcp stack).
    // The single-queue refactor (`feat/dpdk-single-queue`) collapsed
    // replication onto the same queue/thread as client traffic — no
    // separate queue is reserved any more, so `num_client_queues`
    // simply tracks `num_dpdk_threads`.
    let num_client_queues = num_dpdk_threads;
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
    // Exactly one client poll queue (LMAX: single reader → single
    // matcher); `dpdk_config_from` asks for one queue. Checked before
    // anything touches the journal or spawns a thread.
    let transport = match <[_; 1]>::try_from(transports) {
        Ok([transport]) => transport,
        Err(transports) => {
            return Err(format!(
                "expected exactly one client DPDK transport, the port gave {}",
                transports.len()
            )
            .into());
        }
    };

    // The configuration was validated at the top of this function. Now
    // initialize or recover the application (journaling genesis on a new
    // journal) and size it — see the kernel-TCP primary path.
    // `began_history` is not read: the DPDK primary has no bring-up gate
    // (its main thread is the poll thread that accepts replicas, so it
    // cannot wait for one — see the replication note further down).
    let InitializedEngine {
        mut app,
        writer,
        recovered_epoch,
        began_history: _,
    } = init_engine::<A, BufferedWriter<A::Event>>(
        &config,
        &sizing,
        std::mem::take(&mut startup.genesis),
    )?;
    <A as Application>::prefault(&mut app, &sizing);

    // Fencing state for this DPDK primary, seeded with the recovered epoch.
    let fence_state = Arc::new(melin_transport_core::fence::FenceState::new(
        recovered_epoch,
    ));

    // Control-plane raft (kernel TCP even on DPDK — off the hot path, no
    // DPDK queue consumed). Signing key loaded only when raft is on.
    // A DPDK primary's fence epoch is recovered here too; seed the
    // advertised sequence from the recovered journal (see the kernel
    // primary path). The pipeline's journal stage owns the handle below.
    let journal_tip = melin_transport_core::AdvertisedJournalTip::new(
        melin_transport_core::WireSeq::new(writer.next_sequence().saturating_sub(1)),
    );
    let raft = match crate::raft::build_raft_config(&config)? {
        None => crate::raft::RaftDriverGuard::disabled(&shutdown),
        Some(cfg) => {
            let signing_key = load_replication_key(&config)?;
            crate::raft::spawn_raft_driver(
                cfg,
                config.raft_auto_promote,
                &signing_key,
                &authorized_keys,
                &fence_state,
                journal_tip.clone(),
                Arc::new(AtomicBool::new(true)),
                // A primary always claims to be serving.
                Arc::new(|| true),
                &shutdown,
            )?
        }
    };
    let raft_status = raft.status();

    // The admin endpoint, as on the kernel-TCP primary path: spawned by
    // the caller of the primary function, since a promoted replica keeps
    // the one it booted with. PROMOTE is rejected on a primary (no flag
    // wired); ROTATE shares the flag the journal stage observes.
    let rotate_flag = config.admin_bind.map(|_| Arc::new(AtomicBool::new(false)));
    let _admin_handle = config
        .admin_bind
        .map(|addr| {
            crate::admin::spawn(
                addr,
                None,
                rotate_flag.clone(),
                Some(ack_policy_control.clone()),
                Arc::clone(&shutdown),
                Arc::clone(&authorized_keys),
            )
        })
        .transpose()?;

    // The raft guard and the admin endpoint drop after this returns.
    run_as_primary_dpdk::<A>(
        app,
        writer,
        transport,
        &dpdk_config,
        &config,
        startup.on_primary,
        decoder,
        encoder,
        event_publisher,
        Arc::clone(&shutdown),
        authorized_keys,
        rotate_flag,
        ack_policy_atomic,
        halt_state,
        fence_state,
        None, // not promoted — no EpochBump injection
        raft_status,
        journal_tip,
    )
}

/// Run the server as a DPDK primary: the DPDK twin of [`run_as_primary`],
/// used by both the normal DPDK primary startup and the promotion of a
/// DPDK replica. Builds the pipeline, spawns its threads, wires the
/// response stage, the ack gate, the halt gate and the replication driver
/// onto `transport`, journals the promotion's epoch bump (if any) and the
/// application's `on_primary` events, then runs the poll loop on this
/// thread until shutdown.
///
/// `transport` is queue 0's, listening on the client port: built at boot
/// by a primary, or the transport a promoted replica ran on, with its
/// client listener added. The replication listener is added here. The
/// EAL, ports and pool behind the transport are left as they are, never
/// re-initialised: EAL cannot be initialised twice in a process.
///
/// The other parameters are [`run_as_primary`]'s, less the bring-up gate's
/// `began_history` (a DPDK primary has none; see below).
#[cfg(feature = "dpdk")]
#[allow(clippy::too_many_arguments)] // boot assembly point, as run_as_primary
fn run_as_primary_dpdk<A>(
    app: A,
    writer: BufferedWriter<A::Event>,
    mut transport: melin_dpdk::DpdkTransport,
    dpdk_config: &melin_dpdk::DpdkConfig,
    config: &ServerConfig,
    on_primary: Vec<A::Event>,
    decoder: RequestDecoderArc<A>,
    encoder: ResponseEncoderArc<A>,
    event_publisher: Option<EventPublisherFn<A>>,
    shutdown: Arc<AtomicBool>,
    authorized_keys: Arc<AuthorizedKeys>,
    rotate_flag: Option<Arc<AtomicBool>>,
    ack_policy_atomic: Arc<AtomicU8>,
    halt_state: Arc<melin_transport_core::halt_state::HaltState>,
    fence_state: Arc<melin_transport_core::fence::FenceState>,
    promotion: Option<u64>,
    raft_status: Option<Arc<melin_transport_core::health::RaftStatus>>,
    journal_tip: melin_transport_core::AdvertisedJournalTip,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    // Re-checked, as in `run_as_primary`: a promotion reaches here without
    // the boot-time check.
    validate_primary_config(config)?;
    // As on the kernel-TCP path: read before the writer moves into the
    // pipeline, for the shadow stage's snapshots.
    let genesis_entries = writer.read_header_info()?.genesis_entries;

    // Clone the application's state for the shadow snapshot stage before
    // moving it into the pipeline (same as the kernel TCP path).
    let enable_shadow = config.snapshot_interval_ms > 0;
    let shadow_app = if enable_shadow {
        Some(<A as Application>::clone_via_snapshot(&app)?)
    } else {
        None
    };

    let active_connections = Arc::new(AtomicU64::new(0));

    // Replication setup (same as TCP path). The configuration was
    // validated before `init_engine`.
    let enable_replication = config.replication_bind.is_some();

    // Build disruptor pipeline (same flags as the kernel TCP path).
    // DPDK doesn't currently spawn the publisher thread (see
    // `event_publisher: None` in the handles bundle further down).
    // Consumer allocation is gated on the same `Some`/`event_bind` pair
    // the kernel-TCP path uses, so passing `None` wires up no consumer;
    // but with `Some` and `--event-bind` a consumer slot is wired up and
    // never drained — see "DPDK never runs the event publisher" in
    // docs/internal/transport-divergences-2026-10.md.
    let enable_event_publisher = event_publisher.is_some() && config.event_bind.is_some();
    let enable_shadow = config.snapshot_interval_ms > 0;
    let Pipeline {
        input_producer,
        journal_stage,
        matching_stage,
        mut output_consumers,
        events_processed,
        input_cursor,
        replication_consumers,
        replicas_connected,
        shadow_consumer,
        chain_hash_lock,
        replication_ring_progress,
        cursors,
    } = build_pipeline_with_replication(
        app,
        writer,
        config.group_commit_delay(),
        Arc::clone(&active_connections),
        enable_replication,
        config.max_journal_batch,
        config.replication_ring_size,
        config.cores.stage_waits(),
        enable_event_publisher,
        enable_shadow,
        Arc::clone(&fence_state),
    );
    // Ring-position cursors for the on_primary drain gate below (Acquire loads,
    // stronger than the bundle's monitoring reads). The ack gate and
    // the replication sender pull their typed handles straight from
    // `cursors`; the health endpoint takes `cursors` itself.
    let journal_cursor = cursors.journal_ring_arc();
    let matching_cursor = cursors.matching_ring_arc();

    let heartbeat_interval = config.heartbeat_interval();

    // on_primary events flow through the disruptor like regular events. The
    // input ring is single-producer: main publishes them, then moves the
    // producer into the DPDK poll thread. No cloning required.
    let mut input_producer = input_producer;

    // The DPDK poll thread also generates the application's clock ticks via
    // a wall-clock comparison between NIC bursts (see `run_dpdk_poll`). The
    // input ring is therefore single-producer in steady state alongside the
    // one-shot on_primary loop — same property as the io_uring transport.
    let tick_cadence = config.tick_interval();

    // Control channel: DPDK poll thread → response stage (connect/disconnect).
    let (control_tx, control_rx) = std::sync::mpsc::channel();

    // Client writes a halted node refuses at ingress, poll thread →
    // response stage, counted for the health endpoint. See `crate::halt`.
    let refused_writes = Arc::new(AtomicU64::new(0));
    attach_halt_state(&halt_state, replicas_connected.as_ref())?;
    let halt_gate = crate::halt::HaltGate::new(
        replicas_connected.clone(),
        Arc::clone(&halt_state),
        Arc::clone(&fence_state),
        Arc::clone(&refused_writes),
    );
    let (refusal_tx, refusal_rx) =
        crate::halt::refusal_channel::<A::Report>(Arc::clone(&matching_cursor));

    // TX SPSC: response stage → DPDK poll thread (encoded frames).
    // Lock-free, fixed-size slots — no heap allocation per frame.
    // 4096 slots × ~140 bytes = ~560 KiB. Enough to buffer a burst
    // without backpressuring the response stage.
    // One SPSC channel per DPDK poll thread (one: see `transport`). The
    // response stage routes frames to the correct thread based on
    // thread_id encoded in connection_id bits 56..63. A `Vec` because
    // that is the response stage's interface.
    // The response thread produces into it, so it waits its way.
    let wait = config.cores.response.wait;
    let (tx_out, tx_rx_0) =
        melin_pipeline::spsc::channel::<crate::dpdk_response::TxFrame>(4096, wait);
    let tx_producers = vec![tx_out];

    // Spawn pipeline threads (journal, matching — identical to TCP path).
    let cores = config.cores;

    // Wire runtime rotation into the journal stage (DPDK primary path).
    // ROTATE shares `rotate_flag` with the admin endpoint the caller
    // spawned.
    let mut journal_stage = journal_stage;
    let max_journal_bytes = config.max_journal_mib.saturating_mul(1024 * 1024);
    journal_stage.set_rotation(max_journal_bytes, rotate_flag.clone());
    config.cores.place_journal_children(&mut journal_stage);
    journal_stage.set_staging_mode(config.journal_staging_mode.into());
    // On a primary the journal stage owns the control-plane advertised
    // tip (durable cursor after each fsync batch). On the promotion path
    // this takes over the handle the receiver was advancing.
    journal_stage.set_advertised_tip_publisher(journal_tip.clone());
    if config.max_journal_mib > 0 {
        info!(
            max_journal_mib = config.max_journal_mib,
            "runtime journal rotation enabled (size threshold, DPDK)"
        );
    }

    // Extract utilization handles before stages are moved into threads.
    let journal_utilization = journal_stage.utilization();
    let matching_utilization = matching_stage.utilization();
    let response_utilization = Arc::new(melin_transport_core::pipeline::StageUtilization::new());

    let s1 = Arc::clone(&shutdown);
    // As on kernel TCP: start the journal's helper threads here, not on
    // journal-seq. See `JournalStage::start`.
    let sequencer = journal_stage
        .start()
        .map_err(|e| format!("start the journal stage: {e}"))?;
    let journal_handle = std::thread::Builder::new()
        .name("journal-seq".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("journal-seq", cores.journal_seq.core);
            sequencer.run(&s1)
        })
        .map_err(|e| format!("spawn journal-seq thread: {e}"))?;

    let s2 = Arc::clone(&shutdown);
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("matching", cores.matching.core);
            matching_stage.run(&s2)
        })
        .map_err(|e| format!("spawn matching thread: {e}"))?;

    let replication_metrics = build_replication_metrics(
        replication_consumers.is_some(),
        &ack_policy_atomic,
        config.ack_policy,
    );

    let replica_active: Option<[Arc<AtomicBool>; 2]> =
        replication_ring_progress.as_ref().map(|rp| {
            [
                Arc::clone(&rp.active_flags[0]),
                Arc::clone(&rp.active_flags[1]),
            ]
        });

    // Spawn DPDK response stage (encodes to TX channel instead of kernel sockets).
    let output_consumer = output_consumers.remove(0);
    // Typed durable-cursor handle for the response thread's gate.
    let journal_persisted_wire_seq_response = cursors.durable_wire_seq();
    let replication_metrics_response = replication_metrics.as_ref().map(Arc::clone);
    let replica_active_response = replica_active.clone();
    let ack_policy_response = Arc::clone(&ack_policy_atomic);
    let active_connections_response = Arc::clone(&active_connections);
    let s3 = Arc::clone(&shutdown);
    let response_utilization_thread = Arc::clone(&response_utilization);
    let response_handle = std::thread::Builder::new()
        .name("response".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("response", cores.response.core);
            crate::dpdk_response::run::<A>(
                output_consumer,
                control_rx,
                journal_persisted_wire_seq_response,
                ack_policy_response,
                replication_metrics_response,
                replica_active_response,
                &s3,
                heartbeat_interval,
                active_connections_response,
                tx_producers,
                response_utilization_thread,
                wait,
                encoder,
                refusal_rx,
            );
        })
        .map_err(|e| format!("spawn response thread: {e}"))?;

    let shadow_handle = spawn_shadow_stage::<A>(
        shadow_consumer,
        shadow_app,
        chain_hash_lock,
        config,
        &cores,
        &shutdown,
        fence_state.epoch(),
        genesis_entries,
    )?;

    // `replication_metrics` was constructed above so the response gate can
    // read per-slot cursors; the replication driver shares the same
    // instance.
    let replica_ready = Arc::new(AtomicBool::new(false));
    // Replication, if enabled: build a `DpdkReplicationDriver` for the
    // single client poll thread to drive. The driver's accept dispatch
    // hangs off a second listener on the client transport, added below
    // (port == repl_bind.port()).
    let (repl_driver, repl_listen_port) = if let Some((repl_consumer_1, repl_consumer_2)) =
        replication_consumers
    {
        let repl_bind = config
            .replication_bind
            .ok_or("replication_bind must be set when replication is enabled")?;
        let repl_port = repl_bind.port();

        let repl_slots = cursors.replica_slot_cursors();
        let ready_flag = Arc::clone(&replica_ready);
        let batch_size = config.replication_batch_size;
        let heartbeat_secs = config.replication_heartbeat_secs;
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

        // Add the replication listener to the client transport so the
        // poll thread accepts both client and replication connections
        // off the same queue.
        // Replication needs larger TX buffers than clients: journal batches
        // can be 100-200 KiB, far exceeding the 16 KiB client TX buffer.
        const REPL_TX_BUF: usize = 512 * 1024;
        const REPL_TX_QUEUE: usize = 512 * 1024;
        const REPL_RX_BUF: usize = 64 * 1024;
        transport
            .add_listener_with_buffers(
                repl_port,
                REPL_RX_BUF,
                REPL_TX_BUF,
                REPL_TX_QUEUE,
                crate::replication::REPL_DISPATCH_BURST,
            )
            .map_err(|e| format!("add replication listener: {e}"))?;

        let driver = crate::replication::DpdkReplicationDriver::new(
            [repl_consumer_1, repl_consumer_2],
            repl_slots,
            journal_path,
            ready_flag,
            connected_counter,
            dpdk_evict_flags,
            dpdk_active_flags,
            repl_metrics,
            batch_size,
            heartbeat_secs,
            Arc::clone(&fence_state),
            Arc::clone(&ack_policy_atomic),
            Arc::clone(&halt_state),
            Arc::clone(&authorized_keys),
        )?;
        // Legacy text match — `lan-bench-suite.sh` `wait_for_log` keys
        // off "DPDK replication sender started" to know the primary
        // is ready to accept replicas. Kept verbatim for backward
        // compatibility with existing bench tooling.
        info!(addr = %repl_bind, "DPDK replication sender started (single-queue, in client poll thread)");
        (Some(driver), repl_port)
    } else {
        if !config.standalone && config.replica_of.is_none() {
            info!("running in standalone mode (no replication)");
        }
        (None, 0)
    };
    // The DPDK replication-sender thread used to be spawned here; with
    // the single-queue / single-thread refactor the driver lives inside
    // the client poll thread instead. Nothing to join.
    let replication_handle: Option<std::thread::JoinHandle<()>> = None;

    // Promotion fencing, as on kernel TCP: the bump is the first entry of
    // a promoted node's tenure, ahead of `on_primary`, and is applied
    // before the poll loop below serves any client or replica. See
    // `journal_promotion_epoch_bump`.
    if let Some(requested_epoch) = promotion {
        journal_promotion_epoch_bump(
            requested_epoch,
            &fence_state,
            &mut input_producer,
            &shutdown,
        );
    }

    // Journal `on_primary` through the pipeline. Genesis is already in
    // the journal (`init_engine` creates the journal with it); replicas
    // copy it by catch-up like the rest of the history.
    //
    // Unlike the kernel-TCP path, a DPDK primary that began the history
    // has no bring-up gate holding clients until its first replica
    // attaches: the main thread IS the poll thread that accepts replica
    // connections, so blocking it here for one would deadlock until
    // shutdown. Until a replica attaches, writes under a policy that
    // requires one are refused (no replica connected). A promoted primary
    // continues a history, which the gate never holds on either transport.
    journal_on_primary_events(
        on_primary,
        &mut input_producer,
        &journal_cursor,
        &matching_cursor,
        &replication_ring_progress,
        &shutdown,
    );

    // Note: the DPDK poll threads are spawned BELOW, after this on_primary
    // drain. That ordering is what keeps the input ring single-producer
    // while on_primary events are published — the same property the TCP
    // path achieves by deferring its `spawn_reader` call to after the drain.

    let pipeline_healthy = Arc::new(AtomicBool::new(true));
    let health_handle = spawn_health_endpoint(
        config,
        &active_connections,
        &events_processed,
        &refused_writes,
        &cursors,
        input_cursor,
        &pipeline_healthy,
        &replicas_connected,
        &halt_state,
        &fence_state,
        &replication_metrics,
        &replica_active,
        &replication_ring_progress,
        &journal_utilization,
        &matching_utilization,
        &response_utilization,
        &shutdown,
        &raft_status,
    )?;

    info!(
        ip = %dpdk_config.ip_addr,
        port = dpdk_config.listen_port,
        num_dpdk_threads = 1,
        "DPDK transport listening"
    );

    let connection_timeout = config.connection_timeout();
    let max_conns = config.max_connections;
    let reader_core = config.cores.reader.core;

    // Exactly one client poll queue (LMAX: single reader → single
    // matcher), on this thread.
    melin_app::affinity::pin_thread("dpdk-poll-0", reader_core);
    crate::dpdk_transport::run_dpdk_poll::<A>(
        transport,
        input_producer,
        decoder,
        halt_gate,
        refusal_tx,
        control_tx,
        tx_rx_0,
        &shutdown,
        authorized_keys,
        connection_timeout,
        tick_cadence,
        max_conns,
        Arc::clone(&active_connections),
        0,
        repl_driver,
        repl_listen_port,
    );

    // The client poll runs on the main thread, so no extra DPDK threads
    // to join here — replication sender (if enabled) is joined below.
    let dpdk_extras: Vec<(String, std::thread::Result<()>)> = Vec::new();

    // The caller's raft guard drops — stopping the driver — after this
    // returns.
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

/// Promotion fencing: journal the `EpochBump` that opens a promoted
/// node's tenure, and return once it is applied. Both transports' primary
/// paths call it, before `on_primary` and before any client or replica is
/// served; a primary that booted as one (not promoted) does not, and keeps
/// the epoch its journal recovered.
///
/// The bump raises the cluster epoch so a paused/partitioned ex-primary
/// self-demotes when a handshake crosses (see `melin_transport_core::fence`).
/// It rides the input ring like a seed event (connection_id == 0), so it
/// flows through journal + replication to every replica. The wait for the
/// matching stage to apply it (epoch advanced) means the first handshake
/// already advertises the new epoch. The caller must be the input ring's
/// only producer.
///
/// The new epoch honours the promotion request's floor: a manual
/// `PROMOTE` carries `MANUAL` (= 1) and resolves to the classic
/// `epoch + 1`; a raft auto-promotion carries its election term
/// (strictly above the old epoch by the driver's request rule), so
/// tenure epochs align with raft terms and two overlapping
/// promotions from different elections always allocate distinct
/// epochs — the newer one fences the older.
fn journal_promotion_epoch_bump<E: melin_app::AppEvent>(
    requested_epoch: u64,
    fence_state: &melin_transport_core::fence::FenceState,
    input_producer: &mut melin_pipeline::ring::Producer<InputSlot<E>>,
    shutdown: &AtomicBool,
) {
    use melin_app::unix_epoch_nanos;
    use melin_journal::JournalEvent;
    use melin_transport_core::trace::mono_trace_ns;

    let new_epoch = fence_state.epoch().saturating_add(1).max(requested_epoch);
    // Re-validate the term↔epoch alignment at the moment the epoch is
    // minted, not just at the driver's request-time check: a streamed
    // `EpochBump` from a concurrent promotion elsewhere can raise the
    // fence during the drain, in which case `max` allocates `epoch+1`
    // instead of the election term. Fencing still converges (the
    // epochs stay distinct and the higher one wins), but the skew
    // makes later "epochs outran raft terms" refusals — so say what
    // actually happened while the evidence exists. Manual promotions
    // (requested == MANUAL) are exempt: they never claimed alignment.
    if requested_epoch > crate::promotion::PromotionRequest::MANUAL && new_epoch != requested_epoch
    {
        warn!(
            new_epoch,
            requested_epoch,
            "promotion epoch does not match its election term — a concurrent \
             promotion advanced the fencing epoch mid-drain; auto-promotion \
             refusals may report term/epoch misalignment until a newer election"
        );
    }
    info!(
        new_epoch,
        requested_epoch, "promotion: injecting epoch bump"
    );
    input_producer.publish(InputSlot {
        connection_id: 0,
        key_hash: 0,
        sequence: 0,
        timestamp_ns: unix_epoch_nanos(),
        event: JournalEvent::EpochBump { epoch: new_epoch },
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });
    // Wait until the matching stage observes the bump (epoch raised) so
    // the node advertises `new_epoch` on the very first handshake. Bounded
    // by the shutdown flag so a stuck pipeline can't wedge startup.
    ORCHESTRATOR_WAIT
        .wait_until(|| fence_state.epoch() >= new_epoch || shutdown.load(Ordering::Relaxed));
}

/// Journal a primary's `on_primary` [`StartupEvents`] and return once
/// they are applied. (Genesis is not journaled here: `init_engine`
/// creates a new journal with it already in it.)
///
/// The events ride the input ring as the node's own (connection 0, key
/// hash 0: no client identity), so the normal pipeline
/// journals, replicates and applies them. The caller must still be the
/// ring's only producer, and must not serve a client until this returns:
/// a client request would otherwise run before the configuration in
/// `on_primary` is in force.
///
/// Gates on the journal + matching cursors (disruptor sequence space),
/// then on every active replication ring being consumed — the sender
/// threads have read every batch (sent or being sent to replicas).
/// Stronger than no gate, faster than waiting for replica acks, and
/// deadlock-free because the ring backpressures instead of dropping
/// batches. Every wait yields to the shutdown flag, and goes through the
/// orchestrator wait strategy, never a bare spin: the stage threads may
/// share this thread's core on a small box.
fn journal_on_primary_events<E: melin_app::AppEvent>(
    on_primary: Vec<E>,
    input_producer: &mut melin_pipeline::ring::Producer<InputSlot<E>>,
    journal_cursor: &melin_pipeline::padding::Sequence,
    matching_cursor: &melin_pipeline::padding::Sequence,
    replication_ring_progress: &Option<melin_transport_core::pipeline::ReplicationRingProgress>,
    shutdown: &AtomicBool,
) {
    use melin_app::unix_epoch_nanos;
    use melin_journal::JournalEvent;
    use melin_transport_core::trace::mono_trace_ns;

    let on_primary_count = on_primary.len();
    // Nothing published: the cursors never advance past a target, so
    // waiting for one would hang the boot.
    if on_primary_count == 0 {
        return;
    }
    let start = std::time::Instant::now();

    // `sequence: 0` — the journal stage allocates sequences in disruptor
    // cursor order at encode time. The runtime wraps each application
    // event as `JournalEvent::App` and stamps transport-level metadata.
    let mut last_published_seq = 0u64;
    for event in on_primary {
        last_published_seq = input_producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence: 0,
            timestamp_ns: unix_epoch_nanos(),
            event: JournalEvent::App(event),
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });
    }
    let publish_elapsed = start.elapsed();

    let target = last_published_seq + 1; // cursor = next-to-consume
    info!(
        target,
        journal = journal_cursor.get().load(Ordering::Relaxed),
        matching = matching_cursor.get().load(Ordering::Relaxed),
        "startup events: waiting for pipeline cursors"
    );
    ORCHESTRATOR_WAIT.wait_until(|| {
        shutdown.load(Ordering::Relaxed)
            || (journal_cursor.get().load(Ordering::Acquire) >= target
                && matching_cursor.get().load(Ordering::Acquire) >= target)
    });

    // Inactive rings (no connected replica) were never published to, so
    // their producer cursor is 0 — no wait needed.
    if let Some(ring_progress) = replication_ring_progress {
        for i in 0..ring_progress.producer_cursors.len() {
            if !ring_progress.active_flags[i].load(Ordering::Relaxed) {
                continue;
            }
            let ring_target = ring_progress.producer_cursors[i].load();
            ORCHESTRATOR_WAIT.wait_until(|| {
                shutdown.load(Ordering::Relaxed)
                    || ring_progress.consumer_cursors[i]
                        .get()
                        .load(Ordering::Acquire)
                        >= ring_target
            });
        }
    }

    info!(
        on_primary_events = on_primary_count,
        publish_ms = publish_elapsed.as_millis(),
        total_ms = start.elapsed().as_millis(),
        "journaled startup events through pipeline"
    );
}

/// Bootstrap source chosen by [`init_engine`] from the on-disk layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BootstrapSource {
    /// Snapshot plus at least one journal segment (live or archived):
    /// restore the snapshot, replay the post-snapshot delta.
    SnapshotAndJournal,
    /// Snapshot with no journal segment at all: restore the snapshot and
    /// open a fresh segment continuing its sequence + chain.
    SnapshotOnly,
    /// Journal segments but no snapshot: full replay from genesis.
    JournalOnly,
    /// Nothing on disk: first-ever startup.
    Fresh,
}

/// Pure bootstrap decision, extracted so the matrix is unit-testable.
///
/// The critical cell: `(snapshot, no live, archives)` must route through
/// snapshot+journal recovery — the archives may hold acked events past
/// the snapshot (post-rotation crash window), and only the recovery walk
/// replays them, verifies the lineage, and synthesizes the missing live
/// segment. Likewise `(no snapshot, no live, archives)` is journal
/// recovery, not a fresh start.
fn choose_bootstrap(
    snapshot_exists: bool,
    live_exists: bool,
    archives_exist: bool,
) -> BootstrapSource {
    let lineage_exists = live_exists || archives_exist;
    match (snapshot_exists, lineage_exists) {
        (true, true) => BootstrapSource::SnapshotAndJournal,
        (true, false) => BootstrapSource::SnapshotOnly,
        (false, true) => BootstrapSource::JournalOnly,
        (false, false) => BootstrapSource::Fresh,
    }
}

/// Whether a recovered journal still needs its genesis, decided by
/// [`genesis_target`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GenesisTarget {
    /// The history has begun: genesis is in it, or the history holds no
    /// genesis to add. Journal nothing.
    None,
    /// The journal holds no entry: replace it with one that starts with
    /// the genesis.
    EmptyJournal,
}

/// Decide whether a journal recovered without a snapshot still needs
/// its genesis.
///
/// A journal is created with its genesis in it, so any entry at all
/// means the history has begun — a node that recovers a history never
/// journals genesis into it, whatever its configured genesis says (one
/// holding only part of it is refused by `init_engine` instead). The
/// one exception is a history with no entry whose header records no
/// genesis length (`recorded` is `None`): a live segment starting at
/// sequence 1 with nothing after its header, no archive, written by an
/// earlier release that created the journal before its genesis — or a
/// replica's copy of one. Nothing was ever journaled, served or
/// replicated from it, so beginning the history now is safe, and is
/// what the first boot that created it would have done.
///
/// An empty journal whose header records a length is never rewritten:
/// the length is lineage metadata fixed when the history began. `Some(0)`
/// is a history begun with no genesis, which a genesis configured since
/// changes nothing for; `Some(n)` with `n > 0` is a replica's copy that
/// received none of its primary's genesis, which `init_engine` refuses as
/// the partial copy it is rather than fork the lineage with this node's
/// own genesis.
///
/// A genesis with nothing to journal (empty, or only queries, which are
/// never journaled) leaves the journal as it is: rewriting it would
/// change nothing but its file.
///
/// `next_sequence` is the recovered writer's; it is 1 exactly when the
/// live segment starts at sequence 1 and holds no entry. `recorded` is
/// the genesis length its header records.
fn genesis_target<E: melin_app::AppEvent>(
    archives_exist: bool,
    next_sequence: u64,
    recorded: Option<u64>,
    genesis: &[E],
) -> GenesisTarget {
    if !archives_exist
        && next_sequence == 1
        && recorded.is_none()
        && genesis.iter().any(|e| !e.is_query())
    {
        GenesisTarget::EmptyJournal
    } else {
        GenesisTarget::None
    }
}

/// What [`init_engine`] hands the boot path.
pub(crate) struct InitializedEngine<A, W> {
    pub(crate) app: A,
    pub(crate) writer: W,
    /// The fencing epoch recovered from the snapshot + journal (0 for a
    /// genesis node); the caller seeds the node's `FenceState` with it.
    pub(crate) recovered_epoch: u64,
    /// This boot began the history: it created the journal (with its
    /// genesis), or journaled the genesis into a journal that held no
    /// entry. A primary uses it to hold client service until its first
    /// replica attaches — the bring-up of a new cluster.
    pub(crate) began_history: bool,
}

/// Initialize or recover the journaled application from disk.
///
/// Returns the application, its journal writer, the recovered fencing
/// epoch and whether this boot began the history ([`InitializedEngine`]).
/// The recovery paths (snapshot+journal, snapshot only, journal only,
/// fresh) are transport-level concerns and work uniformly for any
/// `A: Application` via `JournaledApp<A>`. Same engine initialization the
/// TCP / DPDK paths use.
///
/// `genesis` is journaled here, and only when this node starts a new
/// history: nothing on disk, or a journal holding no entry at all whose
/// header records no genesis length (see [`genesis_target`]). The
/// journal is created with the genesis already in it
/// ([`write_genesis_journal`]), so a journal at the configured path
/// always begins with the complete genesis, and every other layout —
/// any surviving segment, or a snapshot, whose state already includes
/// genesis — journals nothing. Genesis therefore reaches the returned
/// state through replay, like the rest of the history on every later
/// boot. The journal's header records the genesis length, and a
/// recovered history shorter than the length its header records is
/// refused ([`check_recovered_history_holds_genesis`]): it holds only
/// part of the genesis.
///
/// `sizing` is applied to a genesis instance before a journal is replayed
/// into it, so the history lands in reserved collections; the caller
/// sizes the result again afterwards, which covers the snapshot paths.
///
/// [`write_genesis_journal`]: melin_transport_core::journaled_app::write_genesis_journal
pub(crate) fn init_engine<A, W>(
    config: &ServerConfig,
    sizing: &A::Sizing,
    genesis: Vec<A::Event>,
) -> Result<InitializedEngine<A, W>, Box<dyn std::error::Error>>
where
    A: Application,
    W: JournalWrite<A::Event>,
{
    use melin_transport_core::journaled_app::write_genesis_journal;

    // Replay the journal at the configured path into a fresh instance,
    // sized before the history is applied to it, as a replica is before
    // it applies the stream: replay must not grow the collections the
    // sizing would have reserved.
    let recover_journal = || -> Result<JournaledApp<A, W>, Box<dyn std::error::Error>> {
        let mut app = A::default();
        <A as Application>::prefault(&mut app, sizing);
        Ok(JournaledApp::<A, W>::recover(app, &config.journal)?)
    };

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

    // The journal *lineage* exists if either the live segment or any
    // archived segment is on disk. Deciding on the live file alone is
    // how acked events used to vanish: a crash between `archive_live`
    // and `create_continuing` leaves archives (holding events past the
    // snapshot) with no live file — bootstrapping "snapshot only" from
    // that layout would silently rewind every event in the archives.
    // `JournaledApp::recover*` handles the missing-live case itself
    // (replays archives, synthesizes a fresh live continuing the chain).
    let journal_exists = config.journal.exists();
    let archives_exist = !melin_journal::segment::list_archives(&config.journal)?.is_empty();
    let snapshot_exists = snap_path.is_some_and(|p| p.exists());
    // An interrupted first boot's staging file is never history: sweep
    // it on every boot, not only on one that journals a genesis, so it
    // does not linger next to a history begun some other way.
    melin_transport_core::journaled_app::discard_genesis_staging(&config.journal)?;
    // Set by the two arms that begin the history.
    let mut began_history = false;
    let mut engine: JournaledApp<A, W> =
        match choose_bootstrap(snapshot_exists, journal_exists, archives_exist) {
            BootstrapSource::SnapshotAndJournal => {
                let snap_path = snap_path.expect("snapshot_exists implies snap_path");
                info!(snapshot = %snap_path.display(), "recovering from snapshot + journal");
                JournaledApp::<A, W>::recover_from_snapshot(snap_path, &config.journal)?
            }
            BootstrapSource::SnapshotOnly => {
                // Snapshot exists but no journal segment survives at all —
                // recover from the snapshot alone and start a fresh segment
                // continuing its sequence and chain. Not a new history:
                // the snapshot's state already includes genesis, so the
                // segment starts empty (the standard upgrade, which
                // starts a new binary on a fresh journal next to a
                // snapshot, lands here).
                let snap_path = snap_path.expect("snapshot_exists implies snap_path");
                info!(
                    snapshot = %snap_path.display(),
                    "recovering from snapshot only (no journal segments on disk)"
                );
                // The lineage's genesis length comes from the snapshot,
                // the only record of the lineage left on disk.
                let (app, snap) = melin_transport_core::snapshot::load_with_header::<A>(snap_path)?;
                let writer = W::create_continuing(
                    &config.journal,
                    snap.sequence + 1,
                    snap.chain_hash,
                    snap.genesis_entries,
                )?;
                JournaledApp::<A, W>::from_parts(app, writer, snap.epoch)
            }
            BootstrapSource::JournalOnly => {
                info!("recovering from journal");
                let engine = recover_journal()?;
                match genesis_target(
                    archives_exist,
                    engine.next_sequence(),
                    engine.genesis_entries()?,
                    &genesis,
                ) {
                    GenesisTarget::None => engine,
                    GenesisTarget::EmptyJournal => {
                        // An empty history with no recorded genesis
                        // length is a first boot that stopped before
                        // journaling anything under an earlier release,
                        // which created the journal before its genesis,
                        // or a replica's copy of one, restarted as a
                        // primary. Nothing was served or acknowledged
                        // from it either way, so starting the history now
                        // is safe. The anchor is kept, so a replica that
                        // copied the empty journal still chains to this
                        // one.
                        drop(engine);
                        let anchor =
                            melin_journal::segment::read_header_info(&config.journal)?.anchor_hash;
                        warn!(
                            journal = %config.journal.display(),
                            "the journal holds no entry and records no genesis \
                             length: journaling genesis (an earlier release's \
                             first boot stopped before journaling it)"
                        );
                        write_genesis_journal::<A::Event, W>(
                            &config.journal,
                            Some(anchor),
                            genesis,
                        )?;
                        began_history = true;
                        recover_journal()?
                    }
                }
            }
            BootstrapSource::Fresh => {
                info!("creating new journal");
                write_genesis_journal::<A::Event, W>(&config.journal, None, genesis)?;
                began_history = true;
                recover_journal()?
            }
        };

    // Every arm now holds the whole genesis, or a layout a primary never
    // writes (a replica's partial copy of it): refuse the latter rather
    // than serve it, against the genesis length the lineage records —
    // not `genesis`, this node's configuration. Checked before rotation,
    // so a refused boot rotates nothing and no recorded entry is
    // modified. Not quite side-effect free: a snapshot-only arm has
    // already left an empty live segment continuing the snapshot, which
    // the next boot recovers as snapshot-and-journal and refuses the
    // same way.
    check_recovered_history_holds_genesis(engine.next_sequence(), engine.genesis_entries()?)?;

    // Archive the live journal segment if it exceeds the configured
    // size threshold. The shadow stage owns snapshot writes; here we
    // only rotate the segment so disk usage stays bounded across
    // restarts. Recovery walks the archive chain forward from the
    // latest shadow snapshot.
    if config.max_journal_mib > 0 {
        let threshold = config.max_journal_mib * 1024 * 1024;
        let current_size = engine.journal_size();
        if current_size > threshold {
            info!(
                current_mib = current_size / (1024 * 1024),
                threshold_mib = config.max_journal_mib,
                "journal exceeds threshold, rotating segment"
            );
            engine.rotate_segment()?;
            info!("journal segment rotated successfully");
        }
    }

    let recovered_epoch = engine.recovered_epoch();
    let (app, writer) = engine.into_parts();
    Ok(InitializedEngine {
        app,
        writer,
        recovered_epoch,
        began_history,
    })
}

// ---------------------------------------------------------------------------
// Shared helpers — used identically by both TCP and DPDK boot paths.
// ---------------------------------------------------------------------------

fn build_replication_metrics(
    has_replication: bool,
    ack_policy_atomic: &AtomicU8,
    config_policy: crate::ack_policy::AckPolicy,
) -> Option<Arc<crate::replication::ReplicationMetrics>> {
    let metrics = if has_replication {
        Some(Arc::new(crate::replication::ReplicationMetrics::default()))
    } else {
        None
    };

    let active_policy =
        crate::ack_policy::AckPolicy::from_u8(ack_policy_atomic.load(Ordering::Relaxed))
            .unwrap_or(config_policy);
    info!(
        ack_policy = %active_policy,
        clauses = %active_policy.to_policy(),
        "ack policy active"
    );

    metrics
}

fn spawn_shadow_stage<A: Application + Send + 'static>(
    shadow_consumer: Option<Consumer<InputSlot<A::Event>>>,
    shadow_app: Option<A>,
    chain_hash_lock: Option<
        melin_pipeline::seqlock::SeqLockReader<melin_transport_core::pipeline::FsyncState>,
    >,
    config: &ServerConfig,
    cores: &PipelineCores,
    shutdown: &Arc<AtomicBool>,
    initial_epoch: u64,
    // The lineage's genesis length, from the journal header — stamped
    // into every snapshot (see `shadow::run`).
    genesis_entries: Option<u64>,
) -> Result<Option<std::thread::JoinHandle<()>>, Box<dyn std::error::Error>>
where
    A::Event: Send + Sync + 'static,
{
    let Some(shadow_cons) = shadow_consumer else {
        return Ok(None);
    };
    let snap_path = config.shadow_snapshot_path();
    let interval = std::time::Duration::from_millis(config.snapshot_interval_ms);
    let chain_hash =
        chain_hash_lock.ok_or("chain hash lock must be Some when shadow is enabled")?;
    let shadow_ex = shadow_app.ok_or("shadow application must be Some when shadow is enabled")?;
    let s_shadow = Arc::clone(shutdown);
    let shadow = cores.shadow;
    let shadow_initial_epoch = initial_epoch;
    let handle = std::thread::Builder::new()
        .name("shadow".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("shadow", shadow.core);
            melin_transport_core::shadow::run(
                shadow_cons,
                shadow_ex,
                snap_path,
                interval,
                chain_hash,
                &s_shadow,
                shadow.wait,
                shadow_initial_epoch,
                genesis_entries,
            );
        })
        .map_err(|e| format!("spawn shadow thread: {e}"))?;

    info!(
        interval_ms = config.snapshot_interval_ms,
        path = %config.shadow_snapshot_path().display(),
        "shadow snapshot stage started"
    );
    Ok(Some(handle))
}

/// Attach the pipeline's replica count to the halt override, on a node
/// that now serves as a replicated primary. A standalone node (no count)
/// never halts for want of a replica, and attaches nothing.
fn attach_halt_state(
    halt_state: &melin_transport_core::halt_state::HaltState,
    replicas_connected: Option<&Arc<std::sync::atomic::AtomicU32>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(count) = replicas_connected {
        halt_state.attach(Arc::clone(count))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_health_endpoint(
    config: &ServerConfig,
    active_connections: &Arc<AtomicU64>,
    events_processed: &Arc<AtomicU64>,
    refused_writes: &Arc<AtomicU64>,
    cursors: &melin_transport_core::PipelineCursors,
    input_cursor: Box<dyn melin_pipeline::ring::QueueCursor>,
    pipeline_healthy: &Arc<AtomicBool>,
    replicas_connected: &Option<Arc<std::sync::atomic::AtomicU32>>,
    halt_state: &Arc<melin_transport_core::halt_state::HaltState>,
    fence_state: &Arc<melin_transport_core::fence::FenceState>,
    replication_metrics: &Option<Arc<crate::replication::ReplicationMetrics>>,
    replica_active: &Option<[Arc<AtomicBool>; 2]>,
    replication_ring_progress: &Option<melin_transport_core::pipeline::ReplicationRingProgress>,
    journal_utilization: &Arc<melin_transport_core::pipeline::StageUtilization>,
    matching_utilization: &Arc<melin_transport_core::pipeline::StageUtilization>,
    response_utilization: &Arc<melin_transport_core::pipeline::StageUtilization>,
    shutdown: &Arc<AtomicBool>,
    raft_status: &Option<Arc<melin_transport_core::health::RaftStatus>>,
) -> Result<Option<std::thread::JoinHandle<()>>, Box<dyn std::error::Error>> {
    let Some(health_addr) = config.health_bind else {
        return Ok(None);
    };

    let (repl_ring_producers, repl_ring_consumers) = replication_ring_progress
        .as_ref()
        .map(|rp| {
            (
                Some([
                    Arc::clone(&rp.producer_cursors[0]),
                    Arc::clone(&rp.producer_cursors[1]),
                ]),
                Some([
                    Arc::clone(&rp.consumer_cursors[0]),
                    Arc::clone(&rp.consumer_cursors[1]),
                ]),
            )
        })
        .unwrap_or((None, None));
    Ok(Some(melin_transport_core::health::spawn(
        health_addr,
        melin_transport_core::health::HealthState {
            active_connections: Arc::clone(active_connections),
            events_processed: Arc::clone(events_processed),
            refused_writes: Arc::clone(refused_writes),
            cursors: cursors.clone(),
            input_cursor,
            pipeline_healthy: Arc::clone(pipeline_healthy),
            replicas_connected: replicas_connected.clone(),
            fence_state: Some(Arc::clone(fence_state)),
            halt_state: Some(Arc::clone(halt_state)),
            replication_metrics: replication_metrics.clone(),
            replica_active: replica_active.clone(),
            replication_ring_producer_cursors: repl_ring_producers,
            replication_ring_consumer_cursors: repl_ring_consumers,
            journal_utilization: Arc::clone(journal_utilization),
            matching_utilization: Arc::clone(matching_utilization),
            response_utilization: Arc::clone(response_utilization),
            raft: raft_status.clone(),
        },
        Arc::clone(shutdown),
    )?))
}

/// Spawn the event-publisher thread on the affinity core configured by
/// `cores.event_publisher`, delegating the loop body to the
/// caller-supplied [`EventPublisherFn`]. Returns `Ok(None)` when either
/// the consumer slot wasn't wired (the pipeline build saw
/// `enable_event_publisher == false`) or the binary passed no
/// publisher fn. The runtime owns thread lifecycle and CPU pinning so
/// the caller's fn stays domain-only.
fn spawn_event_publisher<A: Application>(
    consumer: Option<Consumer<OutputSlot<A>>>,
    run_fn: Option<EventPublisherFn<A>>,
    config: &ServerConfig,
    cores: &PipelineCores,
    authorized_keys: &Arc<AuthorizedKeys>,
    shutdown: &Arc<AtomicBool>,
) -> Result<Option<std::thread::JoinHandle<()>>, Box<dyn std::error::Error>>
where
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
{
    let (Some(event_consumer), Some(run_fn)) = (consumer, run_fn) else {
        return Ok(None);
    };
    let event_bind = config
        .event_bind
        .ok_or("event_bind must be set when event publisher is enabled")?;
    let s_event = Arc::clone(shutdown);
    let event_keys = Arc::clone(authorized_keys);
    let publisher = cores.event_publisher;
    let event_handle = std::thread::Builder::new()
        .name("event-publisher".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("event-publisher", publisher.core);
            run_fn(
                event_consumer,
                event_bind,
                event_keys,
                &s_event,
                publisher.wait,
            );
        })
        .map_err(|e| format!("spawn event publisher thread: {e}"))?;
    info!(addr = %event_bind, "event publisher started");
    Ok(Some(event_handle))
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
/// Returns `(role, public_key_bytes)` on success.
fn authenticate_connection<R: std::io::Read, W: std::io::Write>(
    connection_id: ConnectionId,
    addr: SocketAddr,
    reader: &mut R,
    writer: &mut W,
    authorized_keys: &AuthorizedKeys,
) -> Result<(ClientRole<RoleId>, [u8; 32]), Box<dyn std::error::Error>> {
    use std::io;

    use melin_wire_protocol::control::TransportResponse;
    use melin_wire_protocol::control_codec;

    // Generate a 32-byte random nonce for this connection.
    // Explicit OsRng for cryptographic material (SEC-10).
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("getrandom failed: {e}")))?;

    let mut buf = [0u8; 128];
    let written =
        control_codec::encode_transport_response(&TransportResponse::Challenge { nonce }, &mut buf)
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
    if frame_len > MAX_AUTH_FRAME {
        send_auth_failed(writer);
        return Err(io::Error::other(format!("auth frame too large: {frame_len}")).into());
    }
    let mut frame_buf = [0u8; MAX_AUTH_FRAME];
    reader
        .read_exact(&mut frame_buf[..frame_len])
        .map_err(|e| io::Error::other(format!("read auth frame payload: {e}")))?;

    let cr = match control_codec::decode_challenge_response(&frame_buf[..frame_len]) {
        Ok(cr) => cr,
        Err(e) => {
            send_auth_failed(writer);
            return Err(io::Error::other(format!("decode ChallengeResponse: {e}")).into());
        }
    };

    let public_key_bytes = cr.public_key;
    let role = crate::client_auth::verify_client(
        authorized_keys,
        &nonce,
        &public_key_bytes,
        &cr.signature,
    )
    .inspect_err(|_| send_auth_failed(writer))?;

    // Auth succeeded — send ServerReady.
    let written =
        control_codec::encode_transport_response(&TransportResponse::ServerReady, &mut buf)
            .map_err(|e| io::Error::other(format!("encode ServerReady: {e}")))?;
    writer.write_all(&buf[..written])?;
    writer.flush()?;

    debug!(
        connection_id = connection_id.0,
        addr = %addr,
        role = authorized_keys.token(KeyRole::Client(role)),
        "authenticated"
    );

    Ok((role, public_key_bytes))
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
    use melin_wire_protocol::control::TransportResponse;
    use melin_wire_protocol::control_codec;

    let mut buf = [0u8; 8];
    if let Ok(written) =
        control_codec::encode_transport_response(&TransportResponse::AuthFailed, &mut buf)
    {
        let _ = writer.write_all(&buf[..written]);
        let _ = writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::unix::net::UnixStream;

    use ed25519_dalek::{Signer, SigningKey};
    use melin_app::auth::{AuthorizedKeys, ClientRole, KeyRole, NoRoles};
    use melin_app::decoder::{Decoded, RequestDecoder};
    use melin_wire_protocol::control::ConnectionId;
    use melin_wire_protocol::control_codec::{
        TAG_AUTH_FAILED, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_RESPONSE_HEARTBEAT,
        TAG_SERVER_READY,
    };

    use super::authenticate_connection;
    use super::{BootstrapSource, choose_bootstrap};

    /// `--dpdk-eal-args` takes its value only in the joined form. The
    /// value itself starts with a dash, and the space form let a
    /// forgotten value silently swallow the next flag as the EAL
    /// string; both mistakes must be startup errors instead.
    #[test]
    fn dpdk_eal_args_requires_the_joined_form() {
        use clap::Parser;
        let cfg = super::ServerConfig::try_parse_from([
            "melin-server",
            "--dpdk-eal-args=-l 0-7 --huge-dir /dev/hugepages",
        ])
        .expect("joined form must parse");
        assert_eq!(cfg.dpdk_eal_args, "-l 0-7 --huge-dir /dev/hugepages");

        let space_form =
            super::ServerConfig::try_parse_from(["melin-server", "--dpdk-eal-args", "-l 0-7"]);
        assert!(space_form.is_err(), "space-separated form must be rejected");

        let forgotten_value =
            super::ServerConfig::try_parse_from(["melin-server", "--dpdk-eal-args", "--no-mlock"]);
        assert!(
            forgotten_value.is_err(),
            "a forgotten value must error, not swallow the next flag"
        );
    }

    /// Full bootstrap decision matrix. The two archive-only cells are
    /// the regression guard: a post-rotation crash leaves archives with
    /// no live segment, and bootstrapping "snapshot only" (or "fresh")
    /// from that layout silently rewinds every acked event held in the
    /// archives. Recovery must own any layout where a lineage survives.
    #[test]
    fn bootstrap_decision_routes_archives_through_recovery() {
        use BootstrapSource::*;
        // (snapshot, live, archives) → source
        let matrix = [
            ((false, false, false), Fresh),
            ((false, false, true), JournalOnly), // post-rotation crash, no snapshot
            ((false, true, false), JournalOnly),
            ((false, true, true), JournalOnly),
            ((true, false, false), SnapshotOnly),
            ((true, false, true), SnapshotAndJournal), // post-rotation crash
            ((true, true, false), SnapshotAndJournal),
            ((true, true, true), SnapshotAndJournal),
        ];
        for ((snap, live, arch), expected) in matrix {
            assert_eq!(
                choose_bootstrap(snap, live, arch),
                expected,
                "snapshot={snap} live={live} archives={arch}"
            );
        }
    }

    /// Deterministic test key.
    fn test_key() -> SigningKey {
        SigningKey::from_bytes(&[0xAA; 32])
    }

    /// Build an `AuthorizedKeys` listing the test key under `role`.
    fn keys_with_test_key(role: &str) -> AuthorizedKeys {
        // Encode the public key bytes as base64 with the local helper so
        // the test has no external codec dependency (all test keys produce
        // valid base64), then feed it through AuthorizedKeys::parse.
        let pub_bytes = test_key().verifying_key().to_bytes();
        let pub_b64 = base64_encode(&pub_bytes);
        crate::test_roles::desk_keys(role, &pub_b64)
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
    /// returning the admitted role's token in the keys file. Maps the
    /// error to `String` so it's `Send`.
    fn run_server_auth(
        mut stream: UnixStream,
        keys: AuthorizedKeys,
    ) -> std::thread::JoinHandle<Result<&'static str, String>> {
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
            .map(|(role, _pk)| keys.token(KeyRole::Client(role)))
            .map_err(|e| e.to_string())
        })
    }

    /// Read a length-prefixed Challenge frame and return its 32-byte
    /// nonce. Frame layout: `[len:u32][TAG_CHALLENGE][nonce:32]`.
    fn read_challenge(stream: &mut UnixStream) -> [u8; 32] {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).unwrap();
        assert_eq!(payload[0], TAG_CHALLENGE, "expected Challenge");
        payload[1..33].try_into().unwrap()
    }

    /// Write a length-prefixed ChallengeResponse frame, matching the
    /// layout the runtime's `control_codec::decode_challenge_response`
    /// expects: `[len:u32][TAG_CHALLENGE_RESPONSE][sig:64][pubkey:32]`.
    fn write_challenge_response(
        stream: &mut UnixStream,
        signature: [u8; 64],
        public_key: [u8; 32],
    ) {
        let mut frame = Vec::with_capacity(97);
        frame.push(TAG_CHALLENGE_RESPONSE);
        frame.extend_from_slice(&signature);
        frame.extend_from_slice(&public_key);
        stream
            .write_all(&(frame.len() as u32).to_le_bytes())
            .unwrap();
        stream.write_all(&frame).unwrap();
        stream.flush().unwrap();
    }

    /// Read a Challenge frame from the client end, sign the nonce, and
    /// write a valid ChallengeResponse back.
    fn client_sign_challenge(stream: &mut UnixStream, key: &SigningKey) {
        let nonce = read_challenge(stream);
        let sig = key.sign(&nonce);
        write_challenge_response(stream, sig.to_bytes(), key.verifying_key().to_bytes());
    }

    /// Like `client_sign_challenge` but corrupts the signature.
    fn client_sign_challenge_bad(stream: &mut UnixStream, key: &SigningKey) {
        let nonce = read_challenge(stream);
        let mut sig_bytes = key.sign(&nonce).to_bytes();
        sig_bytes[0] ^= 0xFF;
        write_challenge_response(stream, sig_bytes, key.verifying_key().to_bytes());
    }

    /// Read one length-prefixed control frame and return its tag byte.
    fn read_response_tag(stream: &mut UnixStream) -> u8 {
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).unwrap();
        buf[0]
    }

    #[test]
    fn auth_success_returns_the_application_role() {
        let keys = keys_with_test_key("trader");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_SERVER_READY);

        let result = handle.join().unwrap();
        assert_eq!(result.unwrap(), "trader");
    }

    #[test]
    fn auth_returns_the_operator_role() {
        let keys = keys_with_test_key("operator");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_SERVER_READY);

        assert_eq!(handle.join().unwrap().unwrap(), "operator");
    }

    #[test]
    fn auth_unknown_key_sends_auth_failed() {
        let keys = AuthorizedKeys::parse::<NoRoles>("").unwrap();
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_AUTH_FAILED);

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_bad_signature_sends_auth_failed() {
        let keys = keys_with_test_key("operator");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge_bad(&mut s2, &key);
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_AUTH_FAILED);

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_wrong_message_type_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard the Challenge.
        read_challenge(&mut s2);

        // Send a frame carrying a transport-heartbeat tag where a
        // ChallengeResponse is expected. It is the right length (97) so
        // the auth decoder reaches the tag check and rejects the
        // unexpected tag, replying AuthFailed.
        let mut frame = vec![0u8; 97];
        frame[0] = TAG_RESPONSE_HEARTBEAT;
        s2.write_all(&(frame.len() as u32).to_le_bytes()).unwrap();
        s2.write_all(&frame).unwrap();
        s2.flush().unwrap();

        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_AUTH_FAILED);

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
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_AUTH_FAILED);

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_oversized_frame_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard Challenge.
        read_challenge(&mut s2);

        // Send a frame claiming to be 1000 bytes (way over the 256 limit).
        let fake_len: u32 = 1000;
        s2.write_all(&fake_len.to_le_bytes()).unwrap();
        s2.flush().unwrap();

        // Server should send AuthFailed before dropping.
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_AUTH_FAILED);

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_zero_length_frame_sends_auth_failed() {
        let keys = keys_with_test_key("trader");
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        // Read and discard Challenge.
        read_challenge(&mut s2);

        // Send a zero-length frame — decode_request will fail on empty input.
        let zero_len: u32 = 0;
        s2.write_all(&zero_len.to_le_bytes()).unwrap();
        s2.flush().unwrap();

        // Server should send AuthFailed before dropping.
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_AUTH_FAILED);

        assert!(handle.join().unwrap().is_err());
    }

    #[test]
    fn auth_readonly_role() {
        let keys = keys_with_test_key("readonly");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_SERVER_READY);

        assert_eq!(handle.join().unwrap().unwrap(), "readonly");
    }

    /// A decoder with no application roles, for the pairing check below.
    struct OperatorOnlyDecoder;

    impl RequestDecoder for OperatorOnlyDecoder {
        type Event = counter_server::CounterEvent;
        type Role = NoRoles;

        fn decode(&self, _body: &[u8], _role: ClientRole<NoRoles>) -> Decoded<Self::Event> {
            Decoded::Filter
        }
    }

    /// A keys table parsed for another role type than the decoder's is
    /// refused before the node starts: its role indices would name the
    /// decoder's roles wrongly.
    #[test]
    fn keys_parsed_for_another_role_type_are_refused() {
        let decoder = OperatorOnlyDecoder;
        let other_roles = keys_with_test_key("operator");
        let err = super::check_keys_match_decoder(&decoder, &other_roles).unwrap_err();
        assert_eq!(
            err.to_string(),
            "authorized keys were parsed for a role type other than the decoder's"
        );
        let own_roles = AuthorizedKeys::parse::<NoRoles>("").unwrap();
        super::check_keys_match_decoder(&decoder, &own_roles).unwrap();
    }

    /// A replication key signs correctly and is in the keys file, but it
    /// authorizes node-to-node streaming only: the client listener refuses
    /// it at the handshake, before any request could reach the decoder.
    #[test]
    fn auth_replication_key_refused_on_client_listener() {
        let keys = keys_with_test_key("replication");
        let key = test_key();
        let (s1, mut s2) = UnixStream::pair().unwrap();

        let handle = run_server_auth(s1, keys);

        client_sign_challenge(&mut s2, &key);
        let resp = read_response_tag(&mut s2);
        assert_eq!(resp, TAG_AUTH_FAILED);

        let err = handle.join().unwrap().unwrap_err();
        assert!(
            err.contains("may not connect as a client"),
            "unexpected error: {err}"
        );
    }
}

/// Genesis is journaled once per lineage (audit findings 3 and 4): a new
/// journal is created with its genesis in it, and no other layout
/// journals genesis — a snapshot-only boot, a recovered history, a crash
/// in the middle of a first boot. Each test drives `init_engine` on a
/// directory laid out as the case leaves it.
#[cfg(test)]
mod genesis_tests {
    use std::path::Path;

    use counter_server::{Counter, CounterEvent};
    use melin_journal::{BufferedWriter, JournalEvent, JournalReader, JournalWrite};
    use melin_transport_core::journaled_app::genesis_staging_path;

    use super::{
        GenesisTarget, ServerConfig, check_promotable, check_recovered_history_holds_genesis,
        genesis_target, init_engine, validate_primary_config,
    };

    type Writer = BufferedWriter<CounterEvent>;

    fn config(dir: &Path) -> ServerConfig {
        ServerConfig {
            journal: dir.join("counter.journal"),
            ..ServerConfig::default()
        }
    }

    fn increments(amounts: &[u64]) -> Vec<CounterEvent> {
        amounts
            .iter()
            .map(|&amount| CounterEvent::Increment { amount })
            .collect()
    }

    /// The counter keeps its value private; its snapshot is the value in
    /// little-endian.
    fn value(app: &Counter) -> u64 {
        use melin_app::Application;
        let mut buf = Vec::new();
        app.snapshot(&mut buf).expect("snapshot to a vec");
        u64::from_le_bytes(buf[..8].try_into().expect("8-byte value"))
    }

    /// Boot `init_engine` on `cfg` with `genesis`: the recovered value and
    /// the writer's next sequence.
    fn boot(cfg: &ServerConfig, genesis: &[u64]) -> (u64, u64) {
        let engine =
            init_engine::<Counter, Writer>(cfg, &(), increments(genesis)).expect("init_engine");
        (value(&engine.app), engine.writer.next_sequence())
    }

    /// Whether a boot of `init_engine` on `cfg` with `genesis` began the
    /// history — what holds a new primary's clients until its first
    /// replica attaches.
    fn began_history(cfg: &ServerConfig, genesis: &[u64]) -> bool {
        init_engine::<Counter, Writer>(cfg, &(), increments(genesis))
            .expect("init_engine")
            .began_history
    }

    #[test]
    fn only_a_boot_that_begins_the_history_reports_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());

        // Nothing on disk: a new history, with or without a genesis.
        assert!(began_history(&cfg, &[]));
        // The journal now exists: every later boot continues it.
        assert!(!began_history(&cfg, &[]));

        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        assert!(began_history(&cfg, &[1_000]));
        assert!(!began_history(&cfg, &[1_000]));

        // An empty journal left by an earlier release's interrupted
        // first boot (a v15 header, no recorded genesis length):
        // journaling its genesis now begins the history.
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        drop(Writer::create_continuing(&cfg.journal, 1, [0x15; 32], None).expect("v15 journal"));
        assert!(began_history(&cfg, &[1_000]));
        assert!(!began_history(&cfg, &[1_000]));
    }

    /// Every application amount in the journal file at `path`, in order —
    /// what a replica's catch-up would copy.
    fn journaled_amounts(path: &Path) -> Vec<u64> {
        let mut reader = JournalReader::<CounterEvent>::open(path).expect("open journal");
        let mut out = Vec::new();
        while let Some(entry) = reader.next_entry().expect("read entry") {
            if let JournalEvent::App(CounterEvent::Increment { amount }) = entry.event {
                out.push(amount);
            }
        }
        out
    }

    fn anchor(path: &Path) -> [u8; 32] {
        melin_journal::segment::read_header_info(path)
            .expect("journal header")
            .anchor_hash
    }

    #[test]
    fn a_new_journal_is_created_with_its_genesis_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());

        assert_eq!(boot(&cfg, &[1_000, 2_000]), (3_000, 3));
        // On disk before `init_engine` returns, so before any replica
        // can be served: a replica only ever copies the whole genesis.
        assert_eq!(journaled_amounts(&cfg.journal), vec![1_000, 2_000]);
        assert!(!genesis_staging_path(&cfg.journal).exists());

        // Recovered on every later boot, never journaled again.
        assert_eq!(boot(&cfg, &[1_000, 2_000]), (3_000, 3));
        assert_eq!(journaled_amounts(&cfg.journal), vec![1_000, 2_000]);
    }

    /// Finding 3: the standard upgrade (snapshot, new binary, fresh
    /// journal) boots from the snapshot alone, whose state already holds
    /// the genesis.
    #[test]
    fn a_snapshot_only_boot_journals_no_genesis() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        let engine =
            init_engine::<Counter, Writer>(&cfg, &(), increments(&[1_000_000])).expect("boot");
        save_snapshot(&engine, &cfg.journal.with_extension("snapshot"));
        drop(engine);
        std::fs::remove_file(&cfg.journal).expect("move the journal aside");

        let (value, next_sequence) = boot(&cfg, &[1_000_000]);
        assert_eq!(value, 1_000_000, "genesis applied once, from the snapshot");
        assert_eq!(
            next_sequence, 2,
            "the new segment continues the snapshot, empty"
        );
        assert!(journaled_amounts(&cfg.journal).is_empty());
        // The new segment's header carries the lineage's genesis length
        // on from the snapshot, the only record of it left on disk.
        assert_eq!(genesis_entries(&cfg.journal), Some(1));
    }

    /// Save a snapshot of a booted engine as the shadow stage would:
    /// its state, position, chain, epoch and lineage genesis length.
    fn save_snapshot(engine: &super::InitializedEngine<Counter, Writer>, path: &Path) {
        melin_transport_core::snapshot::save::<Counter>(
            &engine.app,
            melin_transport_core::WireSeq::new(engine.writer.next_sequence() - 1),
            engine.writer.chain_hash().unwrap_or([0u8; 32]),
            engine.recovered_epoch,
            engine
                .writer
                .read_header_info()
                .expect("journal header")
                .genesis_entries,
            path,
        )
        .expect("save snapshot");
    }

    /// The genesis length the live segment's header at `path` records.
    fn genesis_entries(path: &Path) -> Option<u64> {
        melin_journal::segment::read_header_info(path)
            .expect("journal header")
            .genesis_entries
    }

    /// Finding 4, crash variant: a first boot that dies part-way through
    /// its genesis leaves the partial journal in the staging file, not at
    /// the journal's path, so the next boot is still a first boot.
    #[test]
    fn a_crash_mid_genesis_leaves_no_journal_and_the_next_boot_journals_it_whole() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        let staging = genesis_staging_path(&cfg.journal);
        let mut partial = Writer::create(&staging).expect("staging journal");
        partial
            .append(&JournalEvent::App(CounterEvent::Increment {
                amount: 1_000,
            }))
            .expect("first genesis event");
        drop(partial);
        assert!(!cfg.journal.exists());

        assert_eq!(boot(&cfg, &[1_000, 2_000, 4_000]), (7_000, 4));
        assert_eq!(journaled_amounts(&cfg.journal), vec![1_000, 2_000, 4_000]);
        assert!(!staging.exists(), "the partial journal is discarded");
        assert_eq!(genesis_entries(&cfg.journal), Some(3));
    }

    /// An empty journal whose header records a genesis length is never
    /// re-genesised: the length is fixed when the history began. A
    /// history begun with no genesis (`Some(0)`) stays one, whatever
    /// genesis is configured since; a replica's copy that received none
    /// of its primary's genesis (`Some(n)`, `n > 0`) is refused as the
    /// partial copy it is, rather than forked with this node's genesis.
    #[test]
    fn an_empty_journal_with_a_recorded_length_is_not_rewritten() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        drop(Writer::create(&cfg.journal).expect("empty journal"));
        assert_eq!(genesis_entries(&cfg.journal), Some(0));
        let before = anchor(&cfg.journal);

        assert_eq!(boot(&cfg, &[1_000_000]), (0, 1));
        assert!(journaled_amounts(&cfg.journal).is_empty());
        assert_eq!(anchor(&cfg.journal), before);
        assert_eq!(genesis_entries(&cfg.journal), Some(0));

        std::fs::remove_file(&cfg.journal).expect("move the journal aside");
        drop(
            Writer::create_continuing(&cfg.journal, 1, [0x2C; 32], Some(2))
                .expect("a replica's empty copy"),
        );
        for configured in [&[][..], &[1_000, 2_000], &[1_000, 2_000, 4_000]] {
            let refused = init_engine::<Counter, Writer>(&cfg, &(), increments(configured))
                .err()
                .expect("an empty copy of a genesis is refused");
            assert!(
                refused.to_string().contains("refusing to start"),
                "{configured:?}: {refused}"
            );
        }
        assert!(journaled_amounts(&cfg.journal).is_empty());
        assert_eq!(anchor(&cfg.journal), [0x2C; 32]);
        assert_eq!(genesis_entries(&cfg.journal), Some(2));
    }

    /// Finding 4, as an earlier release left it: the journal was created
    /// before a refused boot journaled anything — a v15 header, genesis
    /// length unknown. Nothing was served from it, so it gets its
    /// genesis, under the same anchor (a replica that copied the empty
    /// journal still chains to it), and the replacement records the
    /// genesis it journals.
    #[test]
    fn an_empty_old_format_journal_gets_its_genesis_and_its_length() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        drop(Writer::create_continuing(&cfg.journal, 1, [0x15; 32], None).expect("v15 journal"));
        assert_eq!(genesis_entries(&cfg.journal), None);

        assert_eq!(boot(&cfg, &[1_000, 2_000]), (3_000, 3));
        assert_eq!(anchor(&cfg.journal), [0x15; 32]);
        assert_eq!(genesis_entries(&cfg.journal), Some(2));
    }

    /// A stale staging file is swept by a boot that journals no genesis
    /// too, and the history beside it is untouched.
    #[test]
    fn a_stale_staging_file_is_swept_on_any_boot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        assert_eq!(boot(&cfg, &[1_000]), (1_000, 2));
        let staging = genesis_staging_path(&cfg.journal);
        drop(Writer::create(&staging).expect("orphaned staging file"));

        assert_eq!(boot(&cfg, &[1_000]), (1_000, 2));
        assert!(!staging.exists());
        assert_eq!(journaled_amounts(&cfg.journal), vec![1_000]);
    }

    /// With nothing to journal, an empty journal stays the file it was.
    #[test]
    fn an_empty_journal_without_genesis_is_left_alone() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        drop(Writer::create(&cfg.journal).expect("empty journal"));
        let before = anchor(&cfg.journal);

        assert_eq!(boot(&cfg, &[]), (0, 1));
        assert_eq!(anchor(&cfg.journal), before);
        assert!(!genesis_staging_path(&cfg.journal).exists());
    }

    /// A journal with history is recovered as it is, whatever the
    /// configured genesis: its header records the genesis it began with
    /// (none, here), and the configuration plays no part.
    #[test]
    fn a_journal_with_history_never_gets_genesis() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        let mut writer = Writer::create(&cfg.journal).expect("journal");
        writer
            .append(&JournalEvent::App(CounterEvent::Increment { amount: 5 }))
            .expect("one entry");
        drop(writer);

        assert_eq!(boot(&cfg, &[1_000_000]), (5, 2));
        assert_eq!(journaled_amounts(&cfg.journal), vec![5]);
    }

    /// A replica's journal as its primary's death left it: the header
    /// records the lineage's genesis of three entries (learned from the
    /// primary), and only the first two arrived.
    fn partial_replica_copy(path: &Path) {
        let mut writer =
            Writer::create_continuing(path, 1, [0x2B; 32], Some(3)).expect("replica journal");
        for amount in [1_000, 2_000] {
            writer
                .append(&JournalEvent::App(CounterEvent::Increment { amount }))
                .expect("genesis prefix");
        }
    }

    /// A history holding part of the lineage's genesis — a replica's
    /// partial copy started on a primary's flags — is refused, from the
    /// journal alone or from a snapshot, and left as it was. Whatever
    /// genesis the node is configured with: none, the same, or more.
    #[test]
    fn a_genesis_prefix_is_refused_at_boot() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        partial_replica_copy(&cfg.journal);

        for configured in [&[][..], &[1_000, 2_000, 4_000], &[1, 2, 3, 4, 5]] {
            let refused = init_engine::<Counter, Writer>(&cfg, &(), increments(configured))
                .err()
                .expect("a genesis prefix is refused");
            assert!(
                refused.to_string().contains("refusing to start"),
                "{configured:?}: {refused}"
            );
        }
        assert_eq!(journaled_amounts(&cfg.journal), vec![1_000, 2_000]);

        // The same history behind a snapshot: still a prefix, whose
        // snapshot records the genesis length too.
        let snapshot = cfg.journal.with_extension("snapshot");
        melin_transport_core::JournaledApp::<Counter, Writer>::recover(
            Counter::default(),
            &cfg.journal,
        )
        .expect("recover the partial copy")
        .save_snapshot(&snapshot)
        .expect("save snapshot");
        init_engine::<Counter, Writer>(&cfg, &(), Vec::new())
            .err()
            .expect("snapshot + journal holding a prefix is refused");
        std::fs::remove_file(&cfg.journal).expect("move the journal aside");
        let refused = init_engine::<Counter, Writer>(&cfg, &(), Vec::new())
            .err()
            .expect("a snapshot holding a prefix is refused");
        assert!(
            refused.to_string().contains("refusing to start"),
            "{refused}"
        );
    }

    /// A genesis configuration grown since the history began changes
    /// nothing: the history's own header says how long its genesis was,
    /// and a short history holding all of it boots.
    #[test]
    fn a_grown_genesis_configuration_does_not_refuse_a_short_history() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        assert_eq!(boot(&cfg, &[1_000]), (1_000, 2));
        assert_eq!(genesis_entries(&cfg.journal), Some(1));

        assert_eq!(boot(&cfg, &[1_000, 2_000, 4_000, 8_000]), (1_000, 2));
        assert_eq!(journaled_amounts(&cfg.journal), vec![1_000]);
        assert_eq!(genesis_entries(&cfg.journal), Some(1));
    }

    /// A journal written by an earlier release records no genesis
    /// length: it boots as it did there, with no check — even when it is
    /// shorter than the configured genesis — stays a v15 lineage, and a
    /// promotion from it is not checked either. The same from a v2
    /// snapshot alone, which records no length either.
    #[test]
    fn an_old_format_journal_boots_without_the_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = config(dir.path());
        let mut writer =
            Writer::create_continuing(&cfg.journal, 1, [0x15; 32], None).expect("v15 journal");
        writer
            .append(&JournalEvent::App(CounterEvent::Increment { amount: 7 }))
            .expect("one entry");
        drop(writer);

        let engine = init_engine::<Counter, Writer>(&cfg, &(), increments(&[1, 2, 3]))
            .expect("an unknown genesis length is not checked");
        assert_eq!(value(&engine.app), 7);
        assert_eq!(engine.writer.next_sequence(), 2);
        assert_eq!(genesis_entries(&cfg.journal), None, "still a v15 lineage");
        check_promotable(&engine.writer).expect("promotion is not checked either");

        // Snapshot-only: the v2 snapshot (unknown length) starts a v15
        // segment, and the boot is not checked.
        save_snapshot(&engine, &cfg.journal.with_extension("snapshot"));
        drop(engine);
        std::fs::remove_file(&cfg.journal).expect("move the journal aside");
        assert_eq!(boot(&cfg, &[1, 2, 3]), (7, 2));
        assert_eq!(genesis_entries(&cfg.journal), None);
    }

    #[test]
    fn genesis_target_starts_only_an_empty_history() {
        let genesis = increments(&[1]);
        let none: Vec<CounterEvent> = Vec::new();
        let queries = vec![CounterEvent::GetValue];
        // (archives, next_sequence, recorded length, genesis) → target
        type Case<'a> = (bool, u64, Option<u64>, &'a [CounterEvent], GenesisTarget);
        let cases: [Case<'_>; 8] = [
            (false, 1, None, &genesis, GenesisTarget::EmptyJournal),
            (false, 2, None, &genesis, GenesisTarget::None),
            // An empty live segment after a rotation still has history.
            (true, 1, None, &genesis, GenesisTarget::None),
            (true, 5, None, &genesis, GenesisTarget::None),
            (false, 1, None, &none, GenesisTarget::None),
            (false, 1, None, &queries, GenesisTarget::None),
            // A recorded length is fixed: a history begun with no
            // genesis, or a replica's empty copy of one.
            (false, 1, Some(0), &genesis, GenesisTarget::None),
            (false, 1, Some(3), &genesis, GenesisTarget::None),
        ];
        for (archives, next_sequence, recorded, genesis, expected) in cases {
            assert_eq!(
                genesis_target(archives, next_sequence, recorded, genesis),
                expected,
                "archives={archives} next_sequence={next_sequence} recorded={recorded:?} \
                 genesis={genesis:?}"
            );
        }
    }

    /// Finding 4, configuration variant: a refused configuration is
    /// refused by the check `run_impl` runs before the journal is
    /// touched.
    #[test]
    fn standalone_needs_the_disk_policy() {
        let refused = ServerConfig {
            standalone: true,
            ..ServerConfig::default()
        };
        let err = validate_primary_config(&refused).expect_err("default policy with --standalone");
        assert!(err.to_string().contains("--ack-policy disk"), "{err}");
        let accepted = ServerConfig {
            standalone: true,
            ack_policy: crate::ack_policy::AckPolicy::Disk,
            ..ServerConfig::default()
        };
        validate_primary_config(&accepted).expect("standalone under disk");
    }

    /// A connection cap the rings cannot be sized for is a startup error
    /// on either transport — `0`, which once meant "unlimited", included —
    /// and the CLI refuses it before the config exists.
    #[test]
    fn max_connections_must_be_a_supported_cap() {
        use crate::connection_limit::MAX_SUPPORTED_CONNECTIONS;
        use clap::Parser as _;

        for refused in [0, MAX_SUPPORTED_CONNECTIONS + 1] {
            let config = ServerConfig {
                max_connections: refused,
                ..ServerConfig::default()
            };
            let err = validate_primary_config(&config).expect_err("an unsupported cap");
            assert!(err.to_string().contains("--max-connections"), "{err}");
            assert!(
                ServerConfig::try_parse_from([
                    "melin-server",
                    "--max-connections",
                    &refused.to_string()
                ])
                .is_err(),
                "the CLI accepted --max-connections {refused}"
            );
        }
        for accepted in [1, 1024, MAX_SUPPORTED_CONNECTIONS] {
            let config = ServerConfig {
                max_connections: accepted,
                ..ServerConfig::default()
            };
            validate_primary_config(&config).expect("a supported cap");
            let parsed = ServerConfig::try_parse_from([
                "melin-server",
                "--max-connections",
                &accepted.to_string(),
            ])
            .expect("the CLI accepts a supported cap");
            assert_eq!(parsed.max_connections, accepted);
        }
        assert_eq!(
            ServerConfig::try_parse_from(["melin-server"])
                .expect("defaults parse")
                .max_connections,
            ServerConfig::default().max_connections,
            "the CLI default and `Default` agree"
        );
    }

    /// A replica that copied only part of the lineage's genesis — an
    /// empty journal (its primary died right after the handshake) or a
    /// prefix (died mid catch-up) — is refused promotion; one holding all
    /// of it, with or without history after it, is promoted. The length
    /// comes from the replica's journal header alone: no configuration
    /// reaches the check.
    #[test]
    fn promotion_needs_the_whole_genesis() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("replica.journal");
        let genesis = increments(&[1_000, 2_000, 4_000]);

        let mut writer =
            Writer::create_continuing(&path, 1, [0x2B; 32], Some(3)).expect("replica journal");
        let refused = check_promotable(&writer).expect_err("an empty history");
        assert!(
            refused.to_string().contains("refusing promotion"),
            "{refused}"
        );
        for event in &genesis[..2] {
            writer
                .append(&JournalEvent::App(*event))
                .expect("genesis prefix");
        }
        check_promotable(&writer).expect_err("a genesis prefix");

        writer
            .append(&JournalEvent::App(genesis[2]))
            .expect("last genesis event");
        check_promotable(&writer).expect("the whole genesis");
        writer
            .append(&JournalEvent::App(CounterEvent::Increment { amount: 5 }))
            .expect("client history");
        check_promotable(&writer).expect("genesis and history after it");

        // No genesis: any history, even an empty one, is promotable.
        let none = dir.path().join("none.journal");
        check_promotable(&Writer::create(&none).expect("journal")).expect("nothing to hold");
        // Unknown (a lineage begun before the length was recorded): not
        // checked, as before.
        let unknown = dir.path().join("unknown.journal");
        check_promotable(
            &Writer::create_continuing(&unknown, 1, [0x15; 32], None).expect("journal"),
        )
        .expect("unknown is not checked");
        // A history continuing a snapshot counts the snapshot's entries.
        let continued = dir.path().join("continued.journal");
        check_promotable(
            &Writer::create_continuing(&continued, 10, [0x2B; 32], Some(3)).expect("journal"),
        )
        .expect("snapshot covers genesis");
    }

    /// The boot check draws the same line as the promotion check, and
    /// skips an unknown length.
    #[test]
    fn the_boot_check_needs_the_whole_genesis() {
        check_recovered_history_holds_genesis(1, Some(3)).expect_err("empty");
        check_recovered_history_holds_genesis(3, Some(3)).expect_err("a prefix");
        check_recovered_history_holds_genesis(4, Some(3)).expect("the whole genesis");
        check_recovered_history_holds_genesis(100, Some(3)).expect("and history after it");
        check_recovered_history_holds_genesis(1, Some(0)).expect("no genesis");
        check_recovered_history_holds_genesis(1, None).expect("unknown: not checked");
        check_recovered_history_holds_genesis(3, None).expect("unknown: not checked");
    }
}

/// Startup-time validation of the DPDK CLI surface.
///
/// The value these pin is *when* a bad flag is caught. `--dpdk-peer-mac`
/// is consumed inside the replica's reconnect loop, so the failure has to
/// surface here — during config translation, before EAL init and before
/// any thread exists — rather than as a panic in a node that is already
/// serving.
#[cfg(all(test, feature = "dpdk"))]
mod dpdk_config_tests {
    use super::{ServerConfig, dpdk_config_from};

    #[test]
    fn a_supplied_peer_mac_reaches_the_transport_config() {
        let cfg = ServerConfig {
            dpdk_peer_mac: Some("0c:42:a1:5b:2e:80".into()),
            ..Default::default()
        };
        let dpdk = dpdk_config_from(&cfg).expect("valid config");
        assert_eq!(dpdk.peer_mac, Some([0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80]));
    }

    #[test]
    fn an_absent_peer_mac_leaves_the_transport_to_derive_one() {
        let cfg = ServerConfig::default();
        let dpdk = dpdk_config_from(&cfg).expect("valid config");
        assert_eq!(dpdk.peer_mac, None);
    }

    #[test]
    fn a_malformed_peer_mac_fails_config_translation() {
        let cfg = ServerConfig {
            dpdk_peer_mac: Some("0c:42:a1:5b:2e".into()),
            ..Default::default()
        };
        let err = dpdk_config_from(&cfg).expect_err("a 5-octet MAC must be rejected");
        assert!(err.contains("--dpdk-peer-mac"), "{err}");
    }

    #[test]
    fn a_malformed_gateway_mac_fails_config_translation() {
        // The same guarantee for the pre-existing flag, which used to
        // panic out of this function.
        let cfg = ServerConfig {
            dpdk_gateway_mac: Some("not-a-mac".into()),
            ..Default::default()
        };
        let err = dpdk_config_from(&cfg).expect_err("a non-MAC must be rejected");
        assert!(err.contains("--dpdk-gateway-mac"), "{err}");
    }

    #[test]
    fn a_malformed_ip_names_the_flag_it_came_from() {
        let cfg = ServerConfig {
            dpdk_peer_ip: Some("10.0.0".into()),
            ..Default::default()
        };
        let err = dpdk_config_from(&cfg).expect_err("a truncated IPv4 must be rejected");
        assert!(err.contains("--dpdk-peer-ip"), "{err}");
    }

    #[test]
    fn a_valid_configuration_translates() {
        let cfg = ServerConfig {
            dpdk_gateway: Some("10.0.0.254".into()),
            dpdk_gateway_mac: Some("0c:42:a1:5b:2e:80".into()),
            ..Default::default()
        };
        let dpdk = dpdk_config_from(&cfg).expect("valid config");
        assert_eq!(dpdk.gateway_mac, Some([0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80]));
    }
}
