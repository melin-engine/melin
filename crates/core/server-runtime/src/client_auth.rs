//! The client listener's verdict on a challenge response.
//!
//! The kernel TCP and DPDK transports each run the handshake over their
//! own sockets, but decide it here, so the two cannot disagree about which
//! keys may connect as a client. The DPDK path cannot be exercised without
//! the hardware; this is how its decision is tested all the same.

use std::fmt;

use ed25519_dalek::{Signature, SignatureError, Verifier, VerifyingKey};
use melin_app::auth::{AuthorizedKeys, KeyRole, Permission};

/// Why the client listener refused a challenge response.
#[derive(Debug)]
pub(crate) enum ClientAuthError {
    /// The key is not in the authorized keys file.
    UnknownKey,
    /// The key is listed, under a role that may not open a client
    /// connection (see [`KeyRole::client`]).
    RoleRefused(KeyRole),
    /// The listed bytes are not a valid Ed25519 public key.
    InvalidKey(SignatureError),
    /// The signature over the nonce does not verify.
    BadSignature(SignatureError),
}

impl fmt::Display for ClientAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownKey => f.write_str("unknown public key"),
            Self::RoleRefused(role) => write!(f, "{role} key refused on the client listener"),
            Self::InvalidKey(e) => write!(f, "invalid public key: {e}"),
            Self::BadSignature(e) => write!(f, "signature verification failed: {e}"),
        }
    }
}

impl std::error::Error for ClientAuthError {}

/// Decide a client's challenge response: the key must be listed, under a
/// role that may connect as a client, and must have signed `nonce`.
///
/// The role is checked before the signature, so a refused key costs no
/// verification; the answer the client sees is the same failure either
/// way.
pub(crate) fn verify_client(
    authorized_keys: &AuthorizedKeys,
    nonce: &[u8; 32],
    public_key: &[u8; 32],
    signature: &[u8; 64],
) -> Result<Permission, ClientAuthError> {
    let role = authorized_keys
        .lookup(public_key)
        .ok_or(ClientAuthError::UnknownKey)?;
    let permission = role.client().ok_or(ClientAuthError::RoleRefused(role))?;
    let verifying_key =
        VerifyingKey::from_bytes(public_key).map_err(ClientAuthError::InvalidKey)?;
    verifying_key
        .verify(nonce, &Signature::from_bytes(signature))
        .map_err(ClientAuthError::BadSignature)?;
    Ok(permission)
}

#[cfg(test)]
mod tests {
    use super::*;

    use base64::Engine;
    use ed25519_dalek::{Signer, SigningKey};

    const NONCE: [u8; 32] = [0x5A; 32];

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[0x11; 32])
    }

    fn keys_listing(role: &str, key: &SigningKey) -> AuthorizedKeys {
        let public =
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
        AuthorizedKeys::parse(&format!("{role} {public} test\n")).unwrap()
    }

    fn verify(keys: &AuthorizedKeys, signer: &SigningKey) -> Result<Permission, ClientAuthError> {
        let public_key = key().verifying_key().to_bytes();
        let signature = signer.sign(&NONCE).to_bytes();
        verify_client(keys, &NONCE, &public_key, &signature)
    }

    #[test]
    fn a_listed_client_key_that_signed_the_nonce_is_admitted() {
        for (role, permission) in [
            ("operator", Permission::Operator),
            ("trader", Permission::Trader),
            ("custodian", Permission::Custodian),
            ("readonly", Permission::ReadOnly),
        ] {
            let admitted = verify(&keys_listing(role, &key()), &key()).unwrap();
            assert_eq!(admitted, permission);
        }
    }

    #[test]
    fn an_unlisted_key_is_refused() {
        let other = SigningKey::from_bytes(&[0x22; 32]);
        let err = verify(&keys_listing("operator", &other), &key()).unwrap_err();
        assert!(matches!(err, ClientAuthError::UnknownKey), "{err}");
    }

    /// A replication key authorizes node-to-node streaming only: refused
    /// even though it is listed and signed correctly.
    #[test]
    fn a_replication_key_is_refused() {
        let err = verify(&keys_listing("replication", &key()), &key()).unwrap_err();
        assert!(
            matches!(err, ClientAuthError::RoleRefused(KeyRole::Replication)),
            "{err}"
        );
        // Named by its token, as the operator wrote it in the keys file.
        assert_eq!(
            err.to_string(),
            "replication key refused on the client listener"
        );
    }

    /// A keys file can list any 32 bytes; ones that are not a point on
    /// the curve cannot verify anything and are refused as such.
    #[test]
    fn a_listed_key_that_is_not_a_curve_point_is_refused() {
        let not_a_point = [0x02; 32];
        assert!(
            VerifyingKey::from_bytes(&not_a_point).is_err(),
            "the fixture must not decompress to a point"
        );
        let listed = base64::engine::general_purpose::STANDARD.encode(not_a_point);
        let keys = AuthorizedKeys::parse(&format!("operator {listed} test\n")).unwrap();
        let signature = key().sign(&NONCE).to_bytes();
        let err = verify_client(&keys, &NONCE, &not_a_point, &signature).unwrap_err();
        assert!(matches!(err, ClientAuthError::InvalidKey(_)), "{err}");
    }

    #[test]
    fn a_signature_by_another_key_is_refused() {
        let impostor = SigningKey::from_bytes(&[0x33; 32]);
        let err = verify(&keys_listing("operator", &key()), &impostor).unwrap_err();
        assert!(matches!(err, ClientAuthError::BadSignature(_)), "{err}");
    }
}
