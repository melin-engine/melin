//! Standalone server with rumcast (reliable UDP) as the order-entry
//! transport. Mutually exclusive with the `dpdk` feature at build time.
//!
//! # What this is for
//!
//! Lets the LAN bench suite (`melin-bench`) compare TCP versus rumcast
//! on the same engine pipeline. Phase 1 deliberately keeps scope tight:
//!
//! - Standalone primary only (no replica, no promotion).
//! - **No authentication.** Bench/server are on a trusted LAN. Auth is
//!   the Phase 2 effort (option-2 session-token MAC over fragments).
//! - Single client / single bench thread. Multi-client routing is
//!   Phase 3.
//! - Kernel UDP only (rumcast's `KernelUdp`). DPDK rumcast backend is
//!   a separate effort tracked under the rumcast crate's deferred list.
//!
//! # Wiring (at a glance)
//!
//! ```text
//! [bench client]                                   [melin-server (this)]
//!   PublicationLog ──orders (UDP)──▶ SubscriptionLog ─▶ in-translator
//!                                                        │
//!                                                  input disruptor
//!                                                        │
//!                                                  engine pipeline
//!                                                        │
//!                                                  output ring
//!                                                        │
//!   SubscriptionLog ◀──responses (UDP)── PublicationLog ◀ out-translator
//! ```
//!
//! Body of `run_rumcast` (engine pipeline + I/O loops) lands in a
//! follow-up commit — this commit ships the module skeleton, the
//! feature-flag plumbing, and the main.rs cfg dispatch so the
//! interface and build matrix are locked in before the loops are
//! filled in.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::server::ServerConfig;

/// Configuration specific to the rumcast standalone path. Built from
/// `ServerConfig` by `main.rs::rumcast_config_from`.
#[derive(Debug, Clone)]
pub struct RumcastConfig {
    /// Local address the server binds for incoming order datagrams
    /// (and outgoing responses from the same socket). Reuses the
    /// existing `--bind` ServerConfig flag so users don't have to
    /// learn a new knob.
    pub bind: SocketAddr,
}

/// Entry point for the rumcast standalone server. Called from
/// `main.rs` when the binary is built with `--features rumcast`.
///
/// **Phase 1 — not yet implemented.** Returns an explicit error so
/// the build matrix is exercisable now (cargo check / clippy on the
/// `rumcast` feature both pass) while the engine + I/O wiring is
/// fleshed out in a follow-up commit.
pub fn run_rumcast(
    _config: ServerConfig,
    rumcast_config: RumcastConfig,
    _shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    Err(format!(
        "rumcast standalone server: skeleton committed, run_rumcast not yet implemented \
         (would bind {bind})",
        bind = rumcast_config.bind,
    )
    .into())
}
