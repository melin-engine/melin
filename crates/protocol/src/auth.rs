//! Authentication types for challenge-response handshake.
//!
//! Provides the `Permission` model and `AuthorizedKeys` file loader.
//! The server requires an authorized keys file at startup — every
//! connection must authenticate via Ed25519 challenge-response.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

/// Permission level assigned to an authenticated connection.
///
/// Four specialized roles with no overlap — separation of duties:
///   Operator: exchange configuration (instruments, risk, circuit breakers)
///   Trader: order submission and cancellation
///   Custodian: fund management (deposit/withdraw)
///   ReadOnly: observation only (heartbeats, future market data)
///
/// No single role has full access. An organization needing both trading
/// and admin uses separate keys for each role.
///
/// Checked on the reader thread (cold per-request check) with zero
/// cost on the matching engine hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// Exchange configuration: instrument management, circuit breakers,
    /// risk limits, fee schedules, end-of-day, stats. Cannot trade or
    /// manage funds.
    Operator,
    /// Submit/cancel orders and heartbeats. Cannot perform admin ops
    /// or fund management (deposit/withdraw).
    Trader,
    /// Deposit and withdraw only. Cannot trade or perform admin ops.
    /// Separates fund management from trading and exchange administration.
    Custodian,
    /// Heartbeats only. Future: market data subscriptions.
    ReadOnly,
}

impl Permission {
    /// Whether this permission level allows trading operations
    /// (submit order, cancel order, cancel all, cancel-replace).
    pub fn can_trade(self) -> bool {
        matches!(self, Permission::Trader)
    }

    /// Whether this permission level allows administrative operations
    /// (add instrument, set risk limits, circuit breakers, fee schedules,
    /// end-of-day, query stats).
    pub fn is_operator(self) -> bool {
        matches!(self, Permission::Operator)
    }

    /// Whether this permission level allows fund management operations
    /// (deposit, withdraw).
    pub fn can_manage_funds(self) -> bool {
        matches!(self, Permission::Custodian)
    }
}

/// Maps Ed25519 public keys to permission levels.
///
/// HashMap for O(1) lookup by public key bytes. Loaded once at server
/// startup and shared (immutably) across threads via `Arc`.
#[derive(Debug)]
pub struct AuthorizedKeys {
    /// Public key bytes (32 bytes) → permission level.
    keys: HashMap<[u8; 32], Permission>,
}

impl AuthorizedKeys {
    /// Load authorized keys from a file.
    ///
    /// File format (one entry per line):
    /// ```text
    /// # <permission> <base64-encoded-public-key> <optional-comment>
    /// admin AAAA...base64... ops-team
    /// trader BBBB...base64... market-maker-1
    /// readonly DDDD...base64... monitoring
    /// ```
    ///
    /// Lines starting with `#` and empty lines are ignored.
    pub fn load(path: &Path) -> io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::parse(&content).map_err(|e| io::Error::other(format!("{path:?}: {e}")))
    }

    /// Parse authorized keys from a string. Separated from `load` for testing.
    pub fn parse(content: &str) -> Result<Self, String> {
        let mut keys = HashMap::new();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let mut parts = line.split_whitespace();
            let perm_str = parts
                .next()
                .ok_or_else(|| format!("line {}: missing permission", line_num + 1))?;
            let key_b64 = parts
                .next()
                .ok_or_else(|| format!("line {}: missing public key", line_num + 1))?;

            let permission = match perm_str {
                "operator" => Permission::Operator,
                "trader" => Permission::Trader,
                "custodian" => Permission::Custodian,
                "readonly" => Permission::ReadOnly,
                other => {
                    return Err(format!(
                        "line {}: unknown permission '{}' (expected operator/trader/custodian/readonly)",
                        line_num + 1,
                        other
                    ));
                }
            };

            let key_bytes = BASE64
                .decode(key_b64)
                .map_err(|e| format!("line {}: invalid base64: {e}", line_num + 1))?;

            if key_bytes.len() != 32 {
                return Err(format!(
                    "line {}: public key must be 32 bytes, got {}",
                    line_num + 1,
                    key_bytes.len()
                ));
            }

            let mut key = [0u8; 32];
            key.copy_from_slice(&key_bytes);
            keys.insert(key, permission);
        }

        Ok(Self { keys })
    }

    /// Look up the permission for a public key. Returns `None` if the
    /// key is not authorized.
    pub fn lookup(&self, public_key: &[u8; 32]) -> Option<Permission> {
        self.keys.get(public_key).copied()
    }

    /// Number of authorized keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Whether the keys file is empty (no authorized keys).
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_keys_file() {
        let content = "\
# Auth keys file
operator AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= ops-team
trader AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE= market-maker-1
readonly AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI= monitoring
";
        let keys = AuthorizedKeys::parse(content).unwrap();
        assert_eq!(keys.len(), 3);

        let admin_key = BASE64
            .decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .unwrap();
        let mut k = [0u8; 32];
        k.copy_from_slice(&admin_key);
        assert_eq!(keys.lookup(&k), Some(Permission::Operator));
    }

    #[test]
    fn parse_skips_comments_and_blanks() {
        let content = "\
# comment
   # indented comment

operator AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= test
";
        let keys = AuthorizedKeys::parse(content).unwrap();
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn parse_rejects_unknown_permission() {
        let content = "superuser AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= test\n";
        let result = AuthorizedKeys::parse(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown permission"));
    }

    #[test]
    fn parse_rejects_wrong_key_length() {
        let content = "operator AQID test\n"; // 3 bytes, not 32
        let result = AuthorizedKeys::parse(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("32 bytes"));
    }

    #[test]
    fn lookup_missing_key_returns_none() {
        let keys = AuthorizedKeys::parse("").unwrap();
        assert!(keys.lookup(&[0u8; 32]).is_none());
    }

    #[test]
    fn permission_can_trade() {
        assert!(!Permission::Operator.can_trade());
        assert!(Permission::Trader.can_trade());
        assert!(!Permission::Custodian.can_trade());
        assert!(!Permission::ReadOnly.can_trade());
    }

    #[test]
    fn permission_is_operator() {
        assert!(Permission::Operator.is_operator());
        assert!(!Permission::Trader.is_operator());
        assert!(!Permission::Custodian.is_operator());
        assert!(!Permission::ReadOnly.is_operator());
    }

    #[test]
    fn permission_can_manage_funds() {
        assert!(!Permission::Operator.can_manage_funds());
        assert!(!Permission::Trader.can_manage_funds());
        assert!(Permission::Custodian.can_manage_funds());
        assert!(!Permission::ReadOnly.can_manage_funds());
    }

    #[test]
    fn custodian_key_parsed_from_file() {
        let content = "custodian AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= treasury\n";
        let keys = AuthorizedKeys::parse(content).unwrap();
        let pub_key = [0u8; 32];
        assert_eq!(keys.lookup(&pub_key), Some(Permission::Custodian));
    }

    #[test]
    fn duplicate_key_last_permission_wins() {
        let content = "\
operator AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= first
readonly AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= second
";
        let keys = AuthorizedKeys::parse(content).unwrap();
        // HashMap insert overwrites, so the last entry wins.
        assert_eq!(keys.len(), 1);
        let key = BASE64
            .decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .unwrap();
        let mut k = [0u8; 32];
        k.copy_from_slice(&key);
        assert_eq!(keys.lookup(&k), Some(Permission::ReadOnly));
    }

    #[test]
    fn empty_file_produces_empty_keys() {
        let keys = AuthorizedKeys::parse("").unwrap();
        assert!(keys.is_empty());
        assert_eq!(keys.len(), 0);
        // Any key lookup returns None.
        assert!(keys.lookup(&[0u8; 32]).is_none());
    }

    #[test]
    fn parse_rejects_invalid_base64() {
        let content = "operator not-valid-base64!!! test\n";
        let result = AuthorizedKeys::parse(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid base64"));
    }

    #[test]
    fn parse_rejects_missing_key_field() {
        let content = "admin\n";
        let result = AuthorizedKeys::parse(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("missing public key"));
    }

    #[test]
    fn comments_only_file_produces_empty_keys() {
        let content = "\
# only comments
# nothing else
  # indented
";
        let keys = AuthorizedKeys::parse(content).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn load_nonexistent_file_is_error() {
        let result = AuthorizedKeys::load(std::path::Path::new("/nonexistent/path/keys.txt"));
        assert!(result.is_err());
    }

    #[test]
    fn load_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keys.txt");
        std::fs::write(
            &path,
            "trader AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= test\n",
        )
        .unwrap();
        let keys = AuthorizedKeys::load(&path).unwrap();
        assert_eq!(keys.len(), 1);
    }
}
