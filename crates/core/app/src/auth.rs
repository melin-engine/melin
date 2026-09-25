//! Connection-level role model for application access control, plus the
//! `authorized_keys` file loader that maps Ed25519 public keys to roles.
//!
//! Both live in `melin-app` (next to [`Application`](crate::Application))
//! because the role taxonomy ("who can do what to my app") and the
//! deployment-time mapping of operator-managed keys to roles are
//! application-shaped concerns, not wire-format concerns. The
//! wire-shaped helper for the challenge-response signing payload
//! lives in `melin-protocol::auth`.
//!
//! The runtime owns the two roles it acts on, `operator` and
//! `replication`. Every other role is the application's: it declares them
//! as a type implementing [`Role`](crate::auth::Role), and its decoder
//! receives a [`ClientRole`](crate::auth::ClientRole) of that type with
//! every request.

use std::any::TypeId;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

/// The token of the runtime's operator role in the `authorized_keys` file.
const OPERATOR_TOKEN: &str = "operator";
/// The token of the runtime's replication role in the `authorized_keys`
/// file.
const REPLICATION_TOKEN: &str = "replication";

/// The roles an application declares for its clients, each named by a
/// token in the `authorized_keys` file.
///
/// Usually a fieldless enum:
///
/// ```
/// use melin_app::auth::Role;
///
/// #[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// enum PaymentsRole {
///     Payer,
///     Auditor,
/// }
///
/// impl Role for PaymentsRole {
///     const ROLES: &'static [(&'static str, Self)] =
///         &[("payer", PaymentsRole::Payer), ("auditor", PaymentsRole::Auditor)];
/// }
///
/// melin_app::auth::validate_roles::<PaymentsRole>().expect("a valid role table");
/// ```
///
/// The table is checked by [`validate_roles`] whenever a keys file is
/// parsed, so a node refuses a bad table before it serves. The order of
/// the table is free: inside the runtime a role travels as its index in
/// the table, but that index is never persisted or sent, so adding or
/// reordering roles between two builds changes nothing a node reads back.
pub trait Role: Copy + Eq + fmt::Debug + Send + Sync + 'static {
    /// Every role, each with the token that names it in `authorized_keys`.
    /// One entry per role, and one role per token.
    const ROLES: &'static [(&'static str, Self)];
}

/// The role type of an application with no client roles of its own: its
/// keys file lists `operator` and `replication` keys only. Also what a
/// keys table for replication keys alone is parsed with.
///
/// Not a default access model: an application on `NoRoles` admits
/// operator keys and nothing else on its client listener.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoRoles {}

impl Role for NoRoles {
    const ROLES: &'static [(&'static str, Self)] = &[];
}

/// Check an application's role table: every token well-formed and its
/// own, no token or role listed twice, and no more roles than a
/// [`RoleId`] indexes.
///
/// Every parse of a keys file calls this first. An application pins its
/// own table with a one-line test calling it.
///
/// A token is lowercase ASCII letters, digits, `-` and `_`, starting with
/// a letter; matching a token in the file is exact. The tokens `operator`
/// and `replication` are the runtime's.
pub fn validate_roles<R: Role>() -> Result<(), String> {
    let roles = R::ROLES;
    if roles.len() > RoleId::CAPACITY {
        return Err(format!(
            "role table: {} roles, more than the {} a role index holds",
            roles.len(),
            RoleId::CAPACITY
        ));
    }
    // Pairwise comparison, quadratic in the table's length: the table holds
    // at most `RoleId::CAPACITY` entries and is checked once per keys-file
    // parse, and `Role` asks for `Eq` only, not `Hash` or `Ord`.
    for (i, &(token, role)) in roles.iter().enumerate() {
        if !is_valid_token(token) {
            return Err(format!(
                "role table: '{token}' is not a valid role token (lowercase ASCII \
                 letters, digits, '-' and '_', starting with a letter)"
            ));
        }
        if token == OPERATOR_TOKEN || token == REPLICATION_TOKEN {
            return Err(format!(
                "role table: '{token}' is the runtime's own role token"
            ));
        }
        for &(earlier_token, earlier_role) in &roles[..i] {
            if earlier_token == token {
                return Err(format!("role table: token '{token}' is listed twice"));
            }
            if earlier_role == role {
                return Err(format!(
                    "role table: {role:?} is listed twice, as '{earlier_token}' and '{token}'"
                ));
            }
        }
    }
    Ok(())
}

/// Whether `token` fits `[a-z][a-z0-9_-]*`.
fn is_valid_token(token: &str) -> bool {
    let mut bytes = token.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z'))
        && bytes.all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_'))
}

/// An application role as the runtime carries it: its index in the
/// application's [`Role::ROLES`] table.
///
/// The runtime never names the application's role type. It stores this
/// per connection, and the decoder seam turns it back into the typed role
/// before the application sees it. Only parsing a keys file makes one.
/// Never persisted or sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoleId(
    /// `u8`: a connection's role sits beside other per-connection state on
    /// the reader and in the DPDK connection table, and no access model
    /// needs more than 256 roles.
    u8,
);

impl RoleId {
    /// How many roles a table may hold: one per `u8` value.
    const CAPACITY: usize = u8::MAX as usize + 1;

    /// The role's index in its table.
    pub(crate) fn index(self) -> usize {
        usize::from(self.0)
    }
}

/// Who a connection on the client listener is: the runtime's operator, or
/// one of the application's own roles. The application's decoder receives
/// one with every request and decides what it may do.
///
/// There is no replication variant: the client listener refuses a
/// replication key during the handshake, so no decoder ever sees one.
///
/// Exhaustive on purpose, not `#[non_exhaustive]`: that would force every
/// decoder to write a wildcard arm, and a wildcard in an access check is a
/// default grant that a role added later would inherit silently. A new
/// runtime role is a compile error in every decoder instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientRole<R> {
    /// The node's administrator. Also opens the admin endpoint.
    Operator,
    /// One of the application's own roles.
    App(R),
}

impl<R> ClientRole<R> {
    /// Whether this is the runtime's operator role.
    pub fn is_operator(&self) -> bool {
        matches!(self, ClientRole::Operator)
    }
}

/// What the `authorized_keys` file grants a key: streaming between nodes,
/// or a connection to the client listener under a [`ClientRole`].
///
/// Every handshake decides on this — the client listener, the admin
/// endpoint, the replication and control-plane handshakes, and any
/// listener of the application's own that admits the same keys — and none
/// of them needs the application's role type: an application role is its
/// [`RoleId`] here. [`AuthorizedKeys::token`] names one for a log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRole {
    /// Journal streaming between primary and replica, and the
    /// control-plane mesh. Refused on the client listener.
    Replication,
    /// A client connection with this role.
    Client(ClientRole<RoleId>),
}

impl KeyRole {
    /// Whether this key authorizes replication connections (journal
    /// streaming between primary and replica, and the control-plane mesh).
    pub fn is_replication(self) -> bool {
        matches!(self, KeyRole::Replication)
    }

    /// The role a connection with this key has on the client listener, or
    /// `None` if the key may not connect there at all. A replication key
    /// authorizes streaming between nodes and nothing else, so the runtime
    /// refuses it during the handshake: no request of its reaches the
    /// application's decoder, and no application has to remember to refuse
    /// it. A listener of the application's own that admits the same keys
    /// applies the same rule through this.
    pub fn client(self) -> Option<ClientRole<RoleId>> {
        match self {
            KeyRole::Replication => None,
            KeyRole::Client(role) => Some(role),
        }
    }
}

/// Maps Ed25519 public keys to the role each is listed under.
///
/// HashMap for O(1) lookup by public key bytes. Loaded once at server
/// startup and shared (immutably) across threads via `Arc`.
///
/// One type whatever the application's roles: parsing is generic over the
/// [`Role`] type, the table is not, so the runtime components that only
/// act on `operator` and `replication` never name the application's type.
#[derive(Debug)]
pub struct AuthorizedKeys {
    /// Public key bytes (32 bytes) → the role the file lists it under.
    keys: HashMap<[u8; 32], KeyRole>,
    /// The application's role tokens, indexed by [`RoleId`], so logs and
    /// errors name a role as the operator wrote it. A boxed slice: fixed
    /// once parsed.
    app_tokens: Box<[&'static str]>,
    /// The [`Role`] type the file was parsed for. Checked where the table
    /// is paired with a decoder, so a table parsed for another role type
    /// can never turn an index into the wrong role.
    role_type: TypeId,
}

impl AuthorizedKeys {
    /// Load authorized keys from a file, for the application roles `R`.
    ///
    /// File format (one entry per line):
    /// ```text
    /// # <role> <base64-encoded-public-key> <optional-comment>
    /// operator AAAA...base64... ops-team
    /// trader BBBB...base64... desk-1
    /// readonly DDDD...base64... monitoring
    /// ```
    ///
    /// A role is `operator`, `replication`, or a token of `R`. Lines
    /// starting with `#` and empty lines are ignored. A key may be listed
    /// once: a second line for the same key is refused rather than
    /// resolved, since which role an operator meant is not the loader's to
    /// guess.
    pub fn load<R: Role>(path: &Path) -> io::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Self::parse::<R>(&content).map_err(|e| io::Error::other(format!("{path:?}: {e}")))
    }

    /// Parse authorized keys from a string, for the application roles `R`.
    /// Separated from `load` for testing. Validates `R`'s table first (see
    /// [`validate_roles`]), even for an empty file.
    pub fn parse<R: Role>(content: &str) -> Result<Self, String> {
        validate_roles::<R>()?;
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

            let role = role_for_token::<R>(role_token).ok_or_else(|| {
                let expected: Vec<&str> = [OPERATOR_TOKEN, REPLICATION_TOKEN]
                    .into_iter()
                    .chain(R::ROLES.iter().map(|&(token, _)| token))
                    .collect();
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

        Ok(Self {
            keys,
            app_tokens: R::ROLES.iter().map(|&(token, _)| token).collect(),
            role_type: TypeId::of::<R>(),
        })
    }

    /// Look up the role a public key is listed under. Returns `None` if
    /// the key is not authorized.
    pub fn lookup(&self, public_key: &[u8; 32]) -> Option<KeyRole> {
        self.keys.get(public_key).copied()
    }

    /// The token naming `role` in the keys file, for a log line or an
    /// error. `role` is expected to come from this table; an application
    /// role this table has no token for is named `?`.
    pub fn token(&self, role: KeyRole) -> &'static str {
        match role {
            KeyRole::Replication => REPLICATION_TOKEN,
            KeyRole::Client(ClientRole::Operator) => OPERATOR_TOKEN,
            KeyRole::Client(ClientRole::App(id)) => {
                self.app_tokens.get(id.index()).copied().unwrap_or("?")
            }
        }
    }

    /// Whether this table was parsed for the application roles `R`.
    pub(crate) fn is_for<R: Role>(&self) -> bool {
        self.role_type == TypeId::of::<R>()
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

/// The role `token` names for an application with roles `R`: the
/// runtime's two first, then `R`'s table. `validate_roles` keeps the two
/// sets apart.
fn role_for_token<R: Role>(token: &str) -> Option<KeyRole> {
    match token {
        OPERATOR_TOKEN => Some(KeyRole::Client(ClientRole::Operator)),
        REPLICATION_TOKEN => Some(KeyRole::Replication),
        _ => R::ROLES
            .iter()
            .position(|&(app_token, _)| app_token == token)
            .map(|index| {
                // `validate_roles` caps the table at `RoleId::CAPACITY`
                // entries, so every index fits a `u8`.
                let id = u8::try_from(index).expect("role table validated to fit a u8 index");
                KeyRole::Client(ClientRole::App(RoleId(id)))
            }),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// The key used by the single-line fixtures below: 32 zero bytes.
    const ZERO_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";

    /// A role type shaped like a real application's, for the tests here
    /// and in `decoder`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum DeskRole {
        Trader,
        Custodian,
        ReadOnly,
    }

    impl Role for DeskRole {
        const ROLES: &'static [(&'static str, Self)] = &[
            ("trader", DeskRole::Trader),
            ("custodian", DeskRole::Custodian),
            ("readonly", DeskRole::ReadOnly),
        ];
    }

    /// A one-line keys table listing `ZERO_KEY` under `token`.
    fn listing<R: Role>(token: &str) -> Result<AuthorizedKeys, String> {
        AuthorizedKeys::parse::<R>(&format!("{token} {ZERO_KEY} test\n"))
    }

    /// Declares a role type named `$name` whose table is `$table`, for the
    /// validation tests: each needs a type of its own, since the table is
    /// an associated constant.
    macro_rules! role_table {
        ($name:ident, $table:expr) => {
            // Not every table lists both variants.
            #[allow(dead_code)]
            #[derive(Clone, Copy, Debug, PartialEq, Eq)]
            enum $name {
                A,
                B,
            }
            impl Role for $name {
                const ROLES: &'static [(&'static str, Self)] = $table;
            }
        };
    }

    #[test]
    fn a_valid_role_table_is_accepted() {
        validate_roles::<DeskRole>().unwrap();
        validate_roles::<NoRoles>().unwrap();
        role_table!(Mixed, &[("desk-2", Mixed::A), ("read_only9", Mixed::B)]);
        validate_roles::<Mixed>().unwrap();
    }

    #[test]
    fn a_runtime_token_in_the_role_table_is_refused() {
        role_table!(ClaimsOperator, &[("operator", ClaimsOperator::A)]);
        role_table!(ClaimsReplication, &[("replication", ClaimsReplication::A)]);
        for err in [
            validate_roles::<ClaimsOperator>().unwrap_err(),
            validate_roles::<ClaimsReplication>().unwrap_err(),
        ] {
            assert!(err.contains("is the runtime's own role token"), "{err}");
        }
    }

    #[test]
    fn a_token_listed_twice_is_refused() {
        role_table!(Twice, &[("desk", Twice::A), ("desk", Twice::B)]);
        let err = validate_roles::<Twice>().unwrap_err();
        assert_eq!(err, "role table: token 'desk' is listed twice");
    }

    /// One token per role: an alias is refused, and can be allowed later
    /// without breaking anything.
    #[test]
    fn a_role_listed_twice_is_refused() {
        role_table!(Alias, &[("desk", Alias::A), ("trading-desk", Alias::A)]);
        let err = validate_roles::<Alias>().unwrap_err();
        assert_eq!(
            err,
            "role table: A is listed twice, as 'desk' and 'trading-desk'"
        );
    }

    #[test]
    fn a_token_outside_the_charset_is_refused() {
        role_table!(Capital, &[("Trader", Capital::A)]);
        role_table!(Empty, &[("", Empty::A)]);
        role_table!(Space, &[("read only", Space::A)]);
        role_table!(Comment, &[("#desk", Comment::A)]);
        role_table!(LeadingDigit, &[("9desk", LeadingDigit::A)]);
        role_table!(Colon, &[("desk:1", Colon::A)]);
        for err in [
            validate_roles::<Capital>().unwrap_err(),
            validate_roles::<Empty>().unwrap_err(),
            validate_roles::<Space>().unwrap_err(),
            validate_roles::<Comment>().unwrap_err(),
            validate_roles::<LeadingDigit>().unwrap_err(),
            validate_roles::<Colon>().unwrap_err(),
        ] {
            assert!(err.contains("is not a valid role token"), "{err}");
        }
    }

    /// A table longer than a `RoleId` indexes is refused. Built from a
    /// role type whose values are the index itself, since a fieldless enum
    /// of that size is not worth writing out.
    #[test]
    fn a_table_longer_than_a_role_id_indexes_is_refused() {
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        struct Numbered(u16);

        // The tokens need not differ: the length is checked first.
        const TABLE: [(&str, Numbered); RoleId::CAPACITY + 1] = {
            let mut table = [("desk", Numbered(0)); RoleId::CAPACITY + 1];
            let mut i = 0;
            while i < table.len() {
                table[i].1 = Numbered(i as u16);
                i += 1;
            }
            table
        };
        impl Role for Numbered {
            const ROLES: &'static [(&'static str, Self)] = &TABLE;
        }

        let err = validate_roles::<Numbered>().unwrap_err();
        assert_eq!(
            err,
            "role table: 257 roles, more than the 256 a role index holds"
        );
    }

    /// A keys file is refused on a bad role table even when it lists no
    /// key at all.
    #[test]
    fn parsing_validates_the_role_table_even_for_an_empty_file() {
        role_table!(ClaimsOperator, &[("operator", ClaimsOperator::A)]);
        let err = AuthorizedKeys::parse::<ClaimsOperator>("").unwrap_err();
        assert!(err.contains("is the runtime's own role token"), "{err}");
    }

    #[test]
    fn client_role_is_operator() {
        assert!(ClientRole::<DeskRole>::Operator.is_operator());
        assert!(!ClientRole::App(DeskRole::Trader).is_operator());
    }

    #[test]
    fn only_replication_has_no_client_role() {
        assert_eq!(KeyRole::Replication.client(), None);
        assert!(KeyRole::Replication.is_replication());
        let operator = KeyRole::Client(ClientRole::Operator);
        assert_eq!(operator.client(), Some(ClientRole::Operator));
        assert!(!operator.is_replication());
    }

    /// The runtime's tokens parse to the runtime's roles whatever the
    /// application declares.
    #[test]
    fn runtime_tokens_parse_to_the_runtime_roles() {
        for keys in [
            [
                listing::<DeskRole>("operator"),
                listing::<DeskRole>("replication"),
            ],
            [
                listing::<NoRoles>("operator"),
                listing::<NoRoles>("replication"),
            ],
        ] {
            let [operator, replication] = keys.map(Result::unwrap);
            assert_eq!(
                operator.lookup(&[0u8; 32]),
                Some(KeyRole::Client(ClientRole::Operator))
            );
            assert_eq!(replication.lookup(&[0u8; 32]), Some(KeyRole::Replication));
        }
    }

    /// Every application role parses from its token to an index that
    /// names the same token back.
    #[test]
    fn every_app_role_round_trips_through_its_token() {
        for (index, &(token, _)) in DeskRole::ROLES.iter().enumerate() {
            let keys = listing::<DeskRole>(token).unwrap();
            let role = keys.lookup(&[0u8; 32]).unwrap();
            let Some(ClientRole::App(id)) = role.client() else {
                panic!("{token} must parse to an application role, got {role:?}");
            };
            assert_eq!(id.index(), index);
            assert_eq!(keys.token(role), token);
        }
    }

    #[test]
    fn the_runtime_roles_are_named_by_their_tokens() {
        let keys = AuthorizedKeys::parse::<NoRoles>("").unwrap();
        assert_eq!(keys.token(KeyRole::Replication), "replication");
        assert_eq!(
            keys.token(KeyRole::Client(ClientRole::Operator)),
            "operator"
        );
    }

    /// An application role an application without roles cannot parse.
    #[test]
    fn no_roles_admits_only_the_runtime_tokens() {
        let err = listing::<NoRoles>("trader").unwrap_err();
        assert_eq!(
            err,
            "line 1: unknown role 'trader' (expected operator, replication)"
        );
    }

    /// Matching is exact: the file's tokens are lowercase.
    #[test]
    fn a_token_in_another_case_is_unknown() {
        let err = listing::<DeskRole>("Trader").unwrap_err();
        assert!(err.contains("unknown role 'Trader'"), "{err}");
    }

    #[test]
    fn a_table_knows_the_role_type_it_was_parsed_for() {
        let keys = AuthorizedKeys::parse::<DeskRole>("").unwrap();
        assert!(keys.is_for::<DeskRole>());
        assert!(!keys.is_for::<NoRoles>());
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
        let keys = AuthorizedKeys::parse::<DeskRole>(content).unwrap();
        assert_eq!(keys.len(), 3);

        let admin_key = BASE64
            .decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .unwrap();
        let mut k = [0u8; 32];
        k.copy_from_slice(&admin_key);
        assert_eq!(keys.lookup(&k), Some(KeyRole::Client(ClientRole::Operator)));
    }

    #[test]
    fn parse_skips_comments_and_blanks() {
        let content = "\
# comment
   # indented comment

operator AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA= test
";
        let keys = AuthorizedKeys::parse::<NoRoles>(content).unwrap();
        assert_eq!(keys.len(), 1);
    }

    /// The error names every role the file may use, the runtime's own
    /// first, so the operator can see what was meant.
    #[test]
    fn parse_rejects_unknown_role_naming_every_valid_one() {
        let err = listing::<DeskRole>("superuser").unwrap_err();
        assert_eq!(
            err,
            "line 1: unknown role 'superuser' \
             (expected operator, replication, trader, custodian, readonly)"
        );
    }

    #[test]
    fn parse_rejects_wrong_key_length() {
        let content = "operator AQID test\n"; // 3 bytes, not 32
        let result = AuthorizedKeys::parse::<NoRoles>(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("32 bytes"));
    }

    #[test]
    fn lookup_missing_key_returns_none() {
        let keys = AuthorizedKeys::parse::<NoRoles>("").unwrap();
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
            let err = AuthorizedKeys::parse::<DeskRole>(content).unwrap_err();
            assert!(
                err.starts_with("line 2:") && err.contains("already listed"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn empty_file_produces_empty_keys() {
        let keys = AuthorizedKeys::parse::<NoRoles>("").unwrap();
        assert!(keys.is_empty());
        assert_eq!(keys.len(), 0);
        assert!(keys.lookup(&[0u8; 32]).is_none());
    }

    #[test]
    fn parse_rejects_invalid_base64() {
        let content = "operator not-valid-base64!!! test\n";
        let result = AuthorizedKeys::parse::<NoRoles>(content);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid base64"));
    }

    #[test]
    fn parse_rejects_missing_key_field() {
        let content = "admin\n";
        let result = AuthorizedKeys::parse::<NoRoles>(content);
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
        let keys = AuthorizedKeys::parse::<NoRoles>(content).unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn load_nonexistent_file_is_error() {
        let result =
            AuthorizedKeys::load::<NoRoles>(std::path::Path::new("/nonexistent/path/keys.txt"));
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
        let keys = AuthorizedKeys::load::<DeskRole>(&path).unwrap();
        assert_eq!(keys.len(), 1);
    }
}
