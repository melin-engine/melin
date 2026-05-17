//! Wire-side authentication for the challenge-response handshake.
//!
//! Owns the `AuthorizedKeys` file loader and the [`auth_signing_payload`]
//! helper. The application-shaped [`Permission`] enum lives in
//! [`melin_app::auth`] and is re-exported here for the existing call
//! sites that reach it through this module.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

pub use melin_app::auth::Permission;

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
                "replication" => Permission::Replication,
                other => {
                    return Err(format!(
                        "line {}: unknown permission '{}' (expected operator/trader/custodian/readonly/replication)",
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

/// Build the byte payload that the client signs (and the server
/// verifies) during the Ed25519 challenge-response handshake. The
/// payload is just the 32-byte nonce; the TCP/DPDK transports get
/// stream-level integrity for free so no per-message MAC binding is
/// needed in the signature.
#[inline]
pub fn auth_signing_payload(nonce: &[u8; 32]) -> [u8; 32] {
    *nonce
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
    fn replication_key_parsed_from_file() {
        let content = "replication AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= replica-1\n";
        let keys = AuthorizedKeys::parse(content).unwrap();
        let pub_key = [0u8; 32];
        assert_eq!(keys.lookup(&pub_key), Some(Permission::Replication));
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

    // --- auth_signing_payload ---

    #[test]
    fn auth_signing_payload_is_the_nonce() {
        let nonce = [0xA1u8; 32];
        assert_eq!(auth_signing_payload(&nonce), nonce);
    }

    #[test]
    fn auth_signing_payload_round_trip_with_ed25519() {
        // End-to-end: sign with one party's key, verify on the other
        // side using the same helper. This is the property that makes
        // the cross-transport (TCP / DPDK) signing scheme actually work.
        use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};

        // Deterministic key from a fixed seed — the rest of the
        // codebase uses the same `from_bytes` pattern (e.g., the
        // failover tests in melin-server). Avoids the rand_core
        // version skew between rand 0.9 and ed25519-dalek 2.2.
        let key = SigningKey::from_bytes(&[0x42u8; 32]);
        let vk: VerifyingKey = key.verifying_key();

        let nonce = [0x11u8; 32];
        let payload = auth_signing_payload(&nonce);
        let sig = key.sign(&payload);

        // Same nonce verifies.
        assert!(vk.verify(&payload, &sig).is_ok());

        // Tampering with the nonce fails verification.
        let tampered = auth_signing_payload(&[0xFFu8; 32]);
        assert!(vk.verify(&tampered, &sig).is_err());
    }
}
