//! Configuration for the market data gateway.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

/// Top-level gateway configuration (parsed from TOML).
#[derive(Debug, Deserialize)]
#[allow(dead_code)] // Fields used by the event loop (not yet implemented).
pub struct GatewayConfig {
    /// Address to listen for FIX 4.4 client connections.
    pub listen: SocketAddr,
    /// Address of the melin event publisher to subscribe to.
    pub event_publisher: SocketAddr,
    /// Path to the Ed25519 authorized keys file for FIX client auth.
    pub authorized_keys: PathBuf,
    /// Path to the Ed25519 private key for authenticating to the
    /// event publisher as a subscriber.
    pub subscriber_key: PathBuf,
    /// CPU core to pin the event loop to (0 = no pinning).
    #[serde(default)]
    pub core: usize,
    /// Sender CompID for outbound FIX messages.
    pub sender_comp_id: String,
    /// Per-symbol configuration.
    #[serde(default)]
    pub symbols: HashMap<String, SymbolConfig>,
}

/// Per-symbol configuration.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)] // Fields used by translate and event loop.
pub struct SymbolConfig {
    /// Internal symbol ID (matches the engine's Symbol(u32)).
    pub id: u32,
    /// Price tick size inverse (e.g. 100 for 2 decimal places).
    #[serde(default = "default_tick_inverse")]
    pub tick_inverse: u64,
    /// Lot size inverse (e.g. 1 for integer lots).
    #[serde(default = "default_lot_inverse")]
    pub lot_inverse: u64,
    /// Base currency (e.g. "BTC"). Used in SecurityList responses.
    #[serde(default)]
    pub base_ccy: String,
    /// Quote currency (e.g. "USD"). Used in SecurityList responses.
    #[serde(default)]
    pub quote_ccy: String,
}

fn default_tick_inverse() -> u64 {
    1
}
fn default_lot_inverse() -> u64 {
    1
}
