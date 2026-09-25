//! An application role type for the runtime's own tests, which otherwise
//! have no application to declare one: the handshake tests need a key
//! listed under an application role, not only the runtime's `operator`
//! and `replication`.

use melin_app::auth::{AuthorizedKeys, Role};

/// Two application roles: one that writes and one that only reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeskRole {
    Trader,
    ReadOnly,
}

impl Role for DeskRole {
    const ROLES: &'static [(&'static str, Self)] = &[
        ("trader", DeskRole::Trader),
        ("readonly", DeskRole::ReadOnly),
    ];
}

/// A keys table listing `public_key_b64` under `role`, for an application
/// with [`DeskRole`]s.
pub(crate) fn desk_keys(role: &str, public_key_b64: &str) -> AuthorizedKeys {
    AuthorizedKeys::parse::<DeskRole>(&format!("{role} {public_key_b64} test\n"))
        .expect("parse authorized_keys")
}
