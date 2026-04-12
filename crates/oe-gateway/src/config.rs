//! TOML configuration for the FIX gateway.

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
    /// Optional address for the Prometheus `/metrics` endpoint.
    /// When unset, metrics are still collected on the hot path but
    /// no scrape endpoint is exposed.
    #[serde(default)]
    pub metrics_addr: Option<SocketAddr>,
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
    /// Maximum inbound FIX messages per second (order + cancel + replace).
    /// 0 or absent = unlimited.
    #[serde(default)]
    pub max_msgs_per_sec: u32,
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
        // IPv6 Melin server addresses are not yet supported.
        if self.server_addr.is_ipv6() {
            return Err("server_addr must be IPv4 (IPv6 not yet supported)".into());
        }
        // Check for duplicate SenderCompIDs.
        let mut seen = std::collections::HashSet::new();
        for s in &self.sessions {
            if !seen.insert(&s.sender_comp_id) {
                return Err(format!("duplicate sender_comp_id: {}", s.sender_comp_id).into());
            }
        }
        // Check for duplicate FIX symbols and validate scaling factors.
        // Zero inverses would make decimal_to_ticks divide by zero (or
        // its overflow-checked equivalent) and silently reject every
        // price for the symbol.
        seen.clear();
        for s in &self.symbols {
            if !seen.insert(&s.fix_symbol) {
                return Err(format!("duplicate fix_symbol: {}", s.fix_symbol).into());
            }
            if s.tick_size_inverse == 0 {
                return Err(
                    format!("tick_size_inverse must be > 0 for symbol {}", s.fix_symbol).into(),
                );
            }
            if s.lot_size_inverse == 0 {
                return Err(
                    format!("lot_size_inverse must be > 0 for symbol {}", s.fix_symbol).into(),
                );
            }
        }
        Ok(())
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
    fn deserialize_missing_sessions_is_error() {
        let toml = r#"
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9100"
target_comp_id = "MELIN"

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 1
tick_size_inverse = 100
"#;
        let result: Result<GatewayConfig, _> = toml::from_str(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("session"));
    }

    #[test]
    fn deserialize_missing_symbols_is_error() {
        let toml = r#"
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9100"
target_comp_id = "MELIN"

[[session]]
sender_comp_id = "FIRM_A"
account_id = 1
key_path = "keys/firm_a.key"
"#;
        let result: Result<GatewayConfig, _> = toml::from_str(toml);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("symbol"));
    }

    #[test]
    fn validate_duplicate_sender_comp_id() {
        let toml = r#"
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9100"
target_comp_id = "MELIN"

[[session]]
sender_comp_id = "FIRM_A"
account_id = 1
key_path = "keys/firm_a.key"

[[session]]
sender_comp_id = "FIRM_A"
account_id = 2
key_path = "keys/firm_b.key"

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 1
tick_size_inverse = 100
"#;
        let config: GatewayConfig = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("duplicate sender_comp_id"));
    }

    #[test]
    fn validate_duplicate_fix_symbol() {
        let toml = r#"
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9100"
target_comp_id = "MELIN"

[[session]]
sender_comp_id = "FIRM_A"
account_id = 1
key_path = "keys/firm_a.key"

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 1
tick_size_inverse = 100

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 2
tick_size_inverse = 100
"#;
        let config: GatewayConfig = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("duplicate fix_symbol"));
    }

    #[test]
    fn validate_zero_tick_size_inverse_rejected() {
        let toml = r#"
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9100"
target_comp_id = "MELIN"

[[session]]
sender_comp_id = "FIRM_A"
account_id = 1
key_path = "keys/firm_a.key"

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 1
tick_size_inverse = 0
"#;
        let config: GatewayConfig = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("tick_size_inverse"));
    }

    #[test]
    fn lot_size_inverse_defaults_to_one() {
        let toml = r#"
server_addr = "127.0.0.1:9876"
listen_addr = "0.0.0.0:9100"
target_comp_id = "MELIN"

[[session]]
sender_comp_id = "FIRM_A"
account_id = 1
key_path = "keys/firm_a.key"

[[symbol]]
fix_symbol = "BTC/USD"
melin_symbol = 1
tick_size_inverse = 100
"#;
        let config: GatewayConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.symbols[0].lot_size_inverse, 1);
    }
}
