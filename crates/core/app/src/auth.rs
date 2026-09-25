//! Connection-level permission model for application access control,
//! plus the `authorized_keys` file loader that maps Ed25519 public
//! keys to roles.
//!
//! Both live in `melin-app` (next to [`Application`](crate::Application))
//! because the role taxonomy ("who can do what to my app") and the
//! deployment-time mapping of operator-managed keys to roles are
//! application-shaped concerns, not wire-format concerns. The
//! wire-shaped helper for the challenge-response signing payload
//! lives in `melin-protocol::auth`.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

/// Permission level assigned to a connection on the client listener, and
/// handed to the application's decoder with every request.
///
/// Specialized roles with no overlap — separation of duties:
///   Operator: exchange configuration (instruments, risk, circuit breakers)
///   Trader: order submission and cancellation
///   Custodian: fund management (deposit/withdraw)
///   ReadOnly: observation only (heartbeats, future market data)
///
/// No single role has full access. An organization needing both trading
/// and admin uses separate keys for each role.
///
/// A key listed as `replication` has no `Permission`: it authorizes
/// streaming between nodes, and the client listener refuses it during the
/// handshake (see [`KeyRole`]), so no decoder ever sees one.
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
    /// The token naming this role in the `authorized_keys` file.
    pub fn token(self) -> &'static str {
        match self {
            Permission::Operator => "operator",
            Permission::Trader => "trader",
            Permission::Custodian => "custodian",
            Permission::ReadOnly => "readonly",
        }
    }

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

impl fmt::Display for Permission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.token())
    }
}

/// What the `authorized_keys` file grants a key: streaming between nodes,
/// or a connection to the client listener under a [`Permission`].
///
/// Every handshake decides on this — the client listener, the admin
/// endpoint, the replication and control-plane handshakes, and any
/// listener of the application's own that admits the same keys — and
/// only the client listener's hands the [`Permission`] on to the decoder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRole {
    /// Journal streaming between primary and replica, and the
    /// control-plane mesh. Refused on the client listener.
    Replication,
    /// A client connection with this permission.
    Client(Permission),
}

impl KeyRole {
    /// Every role a keys file can name, in the order an unknown-role error
    /// lists them: the runtime's own first. An array rather than a map:
    /// the set is fixed and small, and it is walked only while a keys file
    /// is parsed, once at startup.
    const ALL: [KeyRole; 5] = [
        KeyRole::Client(Permission::Operator),
        KeyRole::Replication,
        KeyRole::Client(Permission::Trader),
        KeyRole::Client(Permission::Custodian),
        KeyRole::Client(Permission::ReadOnly),
    ];

    /// The token naming this role in the `authorized_keys` file.
    pub fn token(self) -> &'static str {
        match self {
            KeyRole::Replication => "replication",
            KeyRole::Client(permission) => permission.token(),
        }
    }

    /// The role named by `token`, if the keys file format has one.
    fn from_token(token: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|role| role.token() == token)
    }

    /// Whether this key authorizes replication connections (journal
    /// streaming between primary and replica, and the control-plane mesh).
    pub fn is_replication(self) -> bool {
        matches!(self, KeyRole::Replication)
    }

    /// The permission a connection with this key gets on the client
    /// listener, or `None` if the key may not connect there at all. A
    /// replication key authorizes streaming between nodes and nothing
    /// else, so the runtime refuses it during the handshake: no request of
    /// its reaches the application's decoder, and no application has to
    /// remember to refuse it. A listener of the application's own that
    /// admits the same keys applies the same rule through this.
    pub fn client(self) -> Option<Permission> {
        match self {
            KeyRole::Replication => None,
            KeyRole::Client(permission) => Some(permission),
        }
    }
}

impl fmt::Display for KeyRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.token())
    }
}

/// Maps Ed25519 public keys to the role each is listed under.
///
/// HashMap for O(1) lookup by public key bytes. Loaded once at server
/// startup and shared (immutably) across threads via `Arc`.
#[derive(Debug)]
pub struct AuthorizedKeys {
    /// Public key bytes (32 bytes) → the role the file lists it under.
    keys: HashMap<[u8; 32], KeyRole>,
}

impl AuthorizedKeys {
    /// Load authorized keys from a file.
    ///
    /// File format (one entry per line):
    /// ```text
    /// # <role> <base64-encoded-public-key> <optional-comment>
    /// operator AAAA...base64... ops-team
    /// trader BBBB...base64... desk-1
    /// readonly DDDD...base64... monitoring
    /// ```
    ///
    /// Lines starting with `#` and empty lines are ignored. A key may be
    /// listed once: a second line for the same key is refused rather than
    /// resolved, since which role an operator meant is not the loader's
    /// to guess.
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
            let role_token = parts
                .next()
                .ok_or_else(|| format!("line {}: missing role", line_num + 1))?;
            let key_b64 = parts
                .next()
                .ok_or_else(|| format!("line {}: missing public key", line_num + 1))?;

            let role = KeyRole::from_token(role_token).ok_or_else(|| {
                let expected: Vec<&str> = KeyRole::ALL.iter().map(|role| role.token()).collect();
                format!(
                    "line {}: unknown role '{role_token}' (expected {})",
                    line_num + 1,
                    expected.join(", ")
                )
            })?;

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
            if keys.insert(key, role).is_some() {
                return Err(format!(
                    "line {}: public key already listed on an earlier line",
                    line_num + 1
                ));
            }
        }

        Ok(Self { keys })
    }

    /// Look up the role a public key is listed under. Returns `None` if
    /// the key is not authorized.
    pub fn lookup(&self, public_key: &[u8; 32]) -> Option<KeyRole> {
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

    /// The key used by the single-line fixtures below: 32 zero bytes.
    const ZERO_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

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
    fn only_replication_has_no_client_permission() {
        assert_eq!(KeyRole::Replication.client(), None);
        assert!(KeyRole::Replication.is_replication());
        for permission in [
            Permission::Operator,
            Permission::Trader,
            Permission::Custodian,
            Permission::ReadOnly,
        ] {
            let role = KeyRole::Client(permission);
            assert_eq!(role.client(), Some(permission));
            assert!(!role.is_replication());
        }
    }

    /// Every role parses from the token it displays as, so a log line or
    /// an error naming a role names what the operator wrote in the file.
    #[test]
    fn every_role_round_trips_through_its_token() {
        for role in KeyRole::ALL {
            let keys = AuthorizedKeys::parse(&format!("{role} {ZERO_KEY} test\n")).unwrap();
            assert_eq!(keys.lookup(&[0u8; 32]), Some(role));
            assert_eq!(role.to_string(), role.token());
        }
        assert_eq!(
            KeyRole::Client(Permission::ReadOnly).to_string(),
            "readonly"
        );
        assert_eq!(Permission::ReadOnly.to_string(), "readonly");
        assert_eq!(KeyRole::Replication.to_string(), "replication");
    }

    /// Matching is exact: the file's tokens are lowercase.
    #[test]
    fn a_token_in_another_case_is_unknown() {
        let err = AuthorizedKeys::parse(&format!("Trader {ZERO_KEY} test\n")).unwrap_err();
        assert!(err.contains("unknown role 'Trader'"), "{err}");
    }

    // --- AuthorizedKeys ---

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
        assert_eq!(keys.lookup(&k), Some(KeyRole::Client(Permission::Operator)));
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

    /// The error names every role the file may use, the runtime's own
    /// first, so the operator can see what was meant.
    #[test]
    fn parse_rejects_unknown_role_naming_every_valid_one() {
        let err = AuthorizedKeys::parse(&format!("superuser {ZERO_KEY} test\n")).unwrap_err();
        assert_eq!(
            err,
            "line 1: unknown role 'superuser' \
             (expected operator, replication, trader, custodian, readonly)"
        );
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

    /// Two roles for one key is an operator mistake with no safe reading
    /// (the loader used to keep the last line silently). Refused, and a
    /// repeat of the same role with it: a key has one line.
    #[test]
    fn duplicate_key_is_refused() {
        for content in [
            "\
operator AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= first
readonly AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= second
",
            "\
trader AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= first
trader AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= again
",
        ] {
            let err = AuthorizedKeys::parse(content).unwrap_err();
            assert!(
                err.starts_with("line 2:") && err.contains("already listed"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn empty_file_produces_empty_keys() {
        let keys = AuthorizedKeys::parse("").unwrap();
        assert!(keys.is_empty());
        assert_eq!(keys.len(), 0);
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
