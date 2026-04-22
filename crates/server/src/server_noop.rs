//! Minimal no-op server entry point.
//!
//! Compiled in place of the full `server` module when
//! `--features noop --no-default-features` is selected. Shares the
//! protocol, journal, and pipeline crates with the trading server —
//! what's stripped is Exchange-specific recovery, shadow snapshotting,
//! replication, and the multi-queue fan-out. The surface (`ServerConfig`,
//! `run`, `run_with_shutdown`) matches `server::*` so `main.rs` does not
//! need to branch on the cargo feature.
//!
//! Current status: the infrastructure (Cargo features, `App` alias,
//! feature-gated `init_engine`, shared trading-bound ring-slot aliases)
//! is landed and compiles. The TCP accept loop + pipeline wiring for a
//! noop-driven primary will follow in a separate commit — it mostly
//! mirrors `server::run_as_primary` minus shadow/replication.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use clap::Parser;

/// Mirror of `crate::server::ServerConfig` for the noop build. Kept in
/// sync field-for-field so the lan-bench-suite can drive either binary
/// with identical command-line flags — the noop server silently ignores
/// fields it doesn't act on (seeding counts, replication targets,
/// shadow intervals, …).
#[derive(Parser, Debug, Clone)]
#[command(name = "melin-server-noop", about = "No-op transport benchmark server")]
pub struct ServerConfig {
    #[arg(long, default_value = "127.0.0.1:9876")]
    pub bind: SocketAddr,
    #[arg(long, default_value = "melin.journal")]
    pub journal: PathBuf,
    #[arg(long)]
    pub snapshot: Option<PathBuf>,
    #[arg(long, default_value = "1,2,3,6,7,8,9,10")]
    pub cores: String,
    #[arg(long, default_value_t = 4)]
    pub reader_cores: usize,
    #[arg(long, default_value_t = 0)]
    pub group_commit_us: u64,
    #[arg(long, default_value_t = 10)]
    pub heartbeat_interval_secs: u64,
    #[arg(long, default_value_t = 30)]
    pub connection_timeout_secs: u64,
    #[arg(long, default_value_t = 1024)]
    pub max_connections: u64,
    #[arg(long, default_value_t = 0)]
    pub accounts: u32,
    #[arg(long, default_value_t = 0)]
    pub instruments: u32,
    #[arg(long, default_value = "authorized_keys")]
    pub authorized_keys: PathBuf,
    #[arg(long)]
    pub health_bind: Option<SocketAddr>,
    #[arg(long)]
    pub event_bind: Option<SocketAddr>,
    #[arg(long)]
    pub event_auth: Option<PathBuf>,
    #[arg(long)]
    pub promote_bind: Option<SocketAddr>,
    #[arg(long)]
    pub promote_key: Option<PathBuf>,
    #[arg(long)]
    pub replication_bind: Option<SocketAddr>,
    #[arg(long)]
    pub replication_auth: Option<PathBuf>,
    #[arg(long)]
    pub replica_of: Option<SocketAddr>,
    #[arg(long)]
    pub replication_key: Option<PathBuf>,
    #[arg(long, default_value_t = 0)]
    pub snapshot_interval_secs: u64,
    #[arg(long, default_value_t = 0)]
    pub max_journal_mib: u64,
    #[arg(long, default_value_t = false)]
    pub busy_spin: bool,
    #[arg(long, default_value_t = 0)]
    pub tick_interval_us: u64,
    #[arg(long, default_value_t = false)]
    pub no_quorum_durability: bool,
    #[cfg(feature = "dpdk")]
    #[arg(long)]
    pub dpdk: bool,
    #[cfg(feature = "dpdk")]
    #[arg(long, default_value = "")]
    pub dpdk_eal_args: String,
    #[cfg(feature = "dpdk")]
    #[arg(long, default_value = "")]
    pub dpdk_ports: String,
    #[cfg(feature = "dpdk")]
    #[arg(long, default_value = "127.0.0.1")]
    pub dpdk_ip: String,
    #[cfg(feature = "dpdk")]
    #[arg(long, default_value_t = 24)]
    pub dpdk_prefix_len: u8,
    #[cfg(feature = "dpdk")]
    #[arg(long)]
    pub dpdk_gateway: Option<String>,
    #[cfg(feature = "dpdk")]
    #[arg(long, default_value_t = 1500)]
    pub dpdk_mtu: u16,
    #[cfg(feature = "dpdk")]
    #[arg(long)]
    pub dpdk_vlan: Option<u16>,
}

/// Placeholder. The full noop primary loop (TCP accept, pipeline build
/// with `NoopApp`, response stage) is deferred to a follow-up commit —
/// what's in this crate already proves the architectural separation
/// (`cargo tree -p melin-server --no-default-features --features noop`
/// shows no `melin-engine`; `melin-noop::tests::pipeline_with_noop_app_runs_events_to_output`
/// exercises `Pipeline<NoopApp>` end-to-end).
pub fn run<L>(_listener: L, _config: ServerConfig) -> Result<(), Box<dyn std::error::Error>> {
    unimplemented!(
        "noop server entry point: primary TCP accept loop not yet wired. \
         See Phase 4d note in crates/server/src/server_noop.rs."
    )
}

/// Placeholder — see [`run`].
pub fn run_with_shutdown<L>(
    _listener: L,
    _config: ServerConfig,
    _shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    unimplemented!(
        "noop server entry point: primary TCP accept loop not yet wired. \
         See Phase 4d note in crates/server/src/server_noop.rs."
    )
}

#[cfg(feature = "dpdk")]
pub fn run_dpdk(
    _config: ServerConfig,
    _dpdk_config: melin_dpdk::DpdkConfig,
    _shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    unimplemented!(
        "noop server + DPDK combination is not yet supported; \
         use the default TCP transport."
    )
}
