//! TOML configuration for the FIX gateway.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Top-level gateway configuration.
#[derive(Debug, Deserialize)]
pub struct GatewayConfig {
    /// Address of the Melin server to connect to.
    pub server_addr: SocketAddr,
    /// Address to listen for FIX client connections.
    pub listen_addr: SocketAddr,
    /// TargetCompID that this gateway presents to FIX clients.
    pub target_comp_id: String,
    /// FIX session definitions.
    #[serde(rename = "session")]
    pub sessions: Vec<SessionConfig>,
    /// Symbol mapping table.
    #[serde(rename = "symbol")]
    pub symbols: Vec<SymbolConfig>,
}

/// Per-FIX-session configuration. Each SenderCompID maps to one Melin
/// account and Ed25519 key.
#[derive(Debug, Deserialize)]
pub struct SessionConfig {
    /// FIX SenderCompID that identifies this client.
    pub sender_comp_id: String,
    /// Melin account ID for orders from this session.
    pub account_id: u32,
    /// Path to the Ed25519 private key (32-byte raw seed) for
    /// authenticating to melin-server on behalf of this session.
    pub key_path: PathBuf,
}

/// Symbol mapping: FIX symbol string → Melin Symbol ID + price/qty scaling.
#[derive(Debug, Clone, Deserialize)]
pub struct SymbolConfig {
    /// FIX symbol name (e.g., "BTC/USD").
    pub fix_symbol: String,
    /// Melin Symbol(u32) ID.
    pub melin_symbol: u32,
    /// Inverse of tick size for price conversion.
    /// tick_size = 0.01 → tick_size_inverse = 100.
    /// FIX price × inverse = Melin ticks.
    pub tick_size_inverse: u64,
    /// Inverse of lot size for quantity conversion.
    /// lot_size = 1.0 → lot_size_inverse = 1.
    /// FIX quantity × inverse = Melin lots.
    #[serde(default = "default_lot_inverse")]
    pub lot_size_inverse: u64,
}

fn default_lot_inverse() -> u64 {
    1
}

impl GatewayConfig {
    /// Load configuration from a TOML file.
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.sessions.is_empty() {
            return Err("at least one [[session]] is required".into());
        }
        if self.symbols.is_empty() {
            return Err("at least one [[symbol]] is required".into());
        }
        // Check for duplicate SenderCompIDs.
        let mut seen = std::collections::HashSet::new();
        for s in &self.sessions {
            if !seen.insert(&s.sender_comp_id) {
                return Err(
                    format!("duplicate sender_comp_id: {}", s.sender_comp_id).into()
                );
            }
        }
        // Check for duplicate FIX symbols.
        seen.clear();
        for s in &self.symbols {
            if !seen.insert(&s.fix_symbol) {
                return Err(format!("duplicate fix_symbol: {}", s.fix_symbol).into());
            }
        }
        Ok(())
    }

    /// Build a lookup map from SenderCompID → SessionConfig index.
    pub fn session_map(&self) -> HashMap<&str, usize> {
        self.sessions
            .iter()
            .enumerate()
            .map(|(i, s)| (s.sender_comp_id.as_str(), i))
            .collect()
    }

    /// Build a lookup map from FIX symbol string → SymbolConfig index.
    pub fn symbol_map(&self) -> HashMap<&str, usize> {
        self.symbols
            .iter()
            .enumerate()
            .map(|(i, s)| (s.fix_symbol.as_str(), i))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_TOML: &str = r#"
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9100"
target_comp_id = "MELIN"

[[session]]
sender_comp_id = "FIRM_A"
account_id = 1
key_path = "keys/firm_a.key"

[[session]]
sender_comp_id = "FIRM_B"
account_id = 2
key_path = "keys/firm_b.key"

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 1
tick_size_inverse = 100

[[symbol]]
fix_symbol = "ETH/USD"
melin_symbol = 2
tick_size_inverse = 100
lot_size_inverse = 1
"#;

    #[test]
    fn parse_sample_config() {
        let config: GatewayConfig = toml::from_str(SAMPLE_TOML).unwrap();
        assert_eq!(config.target_comp_id, "MELIN");
        assert_eq!(config.sessions.len(), 2);
        assert_eq!(config.sessions[0].sender_comp_id, "FIRM_A");
        assert_eq!(config.sessions[0].account_id, 1);
        assert_eq!(config.symbols.len(), 2);
        assert_eq!(config.symbols[0].fix_symbol, "BTC/USD");
        assert_eq!(config.symbols[0].melin_symbol, 1);
        assert_eq!(config.symbols[0].tick_size_inverse, 100);
        // Default lot_size_inverse.
        assert_eq!(config.symbols[0].lot_size_inverse, 1);
    }

    #[test]
    fn session_map_lookup() {
        let config: GatewayConfig = toml::from_str(SAMPLE_TOML).unwrap();
        let map = config.session_map();
        assert_eq!(map.get("FIRM_A"), Some(&0));
        assert_eq!(map.get("FIRM_B"), Some(&1));
        assert_eq!(map.get("UNKNOWN"), None);
    }

    #[test]
    fn symbol_map_lookup() {
        let config: GatewayConfig = toml::from_str(SAMPLE_TOML).unwrap();
        let map = config.symbol_map();
        assert_eq!(map.get("BTC/USD"), Some(&0));
        assert_eq!(map.get("ETH/USD"), Some(&1));
    }
}
