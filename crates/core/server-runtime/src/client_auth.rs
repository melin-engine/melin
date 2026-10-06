//! The client listener's verdict on a challenge response.
//!
//! The kernel TCP and DPDK transports each run the handshake over their
//! own sockets, but decide it here, so the two cannot disagree about which
//! keys may connect as a client. The DPDK path cannot be exercised without
//! the hardware; this is how its decision is tested all the same.
//!
//! Public for a listener of the application's own that admits the node's
//! client keys, an event publisher's subscribers say: calling
//! [`verify_client`] applies the client listener's rule exactly, rather
//! than a copy of it that has to be kept in step by hand.

use std::fmt;

use ed25519_dalek::{Signature, SignatureError, Verifier, VerifyingKey};
use melin_app::auth::{AuthorizedKeys, ClientRole, RoleId};

/// Largest ChallengeResponse frame payload a client listener accepts. The
/// frame is 1 (tag) + 64 (signature) + 32 (public key) = 97 bytes; a longer
/// length prefix is refused before its body is read.
pub(crate) const MAX_AUTH_FRAME: usize = 256;

/// Why the client listener refused a challenge response.
///
/// Exhaustive, so a caller matching on it is told by the compiler when a
/// refusal reason is added (a breaking change, called out in the
/// changelog).
#[derive(Debug)]
pub enum ClientAuthError {
    /// The key is listed as `replication`, which authorizes streaming
    /// between nodes and may not open a client connection (see
    /// [`KeyRole::client`](melin_app::auth::KeyRole::client)).
    ReplicationKeyRefused,
    /// The key is not in the authorized keys file.
    UnknownKey,
    /// The listed bytes are not a valid Ed25519 public key.
    InvalidKey(SignatureError),
    /// The signature over the nonce does not verify.
    BadSignature(SignatureError),
}

impl fmt::Display for ClientAuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplicationKeyRefused => {
                f.write_str("replication key may not connect as a client")
            }
            Self::UnknownKey => f.write_str("unknown public key"),
            Self::InvalidKey(e) => write!(f, "invalid public key: {e}"),
            Self::BadSignature(e) => write!(f, "signature verification failed: {e}"),
        }
    }
}

impl std::error::Error for ClientAuthError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidKey(e) | Self::BadSignature(e) => Some(e),
            Self::ReplicationKeyRefused | Self::UnknownKey => None,
        }
    }
}

/// Decide a client's challenge response: the key must be listed, under a
/// role that may connect as a client, and must have signed `nonce`.
/// Returns the connection's role, as the runtime carries it: the operator,
/// or an application role as its index in the role table.
///
/// `nonce` must be the one this listener sent the client for this
/// connection, fresh from a cryptographic random source: a nonce reused
/// across connections lets a recorded response be replayed.
///
/// The role is checked before the signature, so a refused key costs no
/// verification; the answer the client sees is the same failure either
/// way.
///
/// ```
/// use base64::Engine;
/// use ed25519_dalek::{Signer, SigningKey};
/// use melin_app::auth::{AuthorizedKeys, NoRoles};
/// use melin_server_runtime::client_auth::{ClientAuthError, verify_client};
///
/// let client = SigningKey::from_bytes(&[7; 32]);
/// let public_key = client.verifying_key().to_bytes();
/// let listed = base64::engine::general_purpose::STANDARD.encode(public_key);
/// let keys = AuthorizedKeys::parse::<NoRoles>(&format!("operator {listed} ops\n")).unwrap();
///
/// // The listener sent this nonce; the client signed it.
/// let nonce = [0x5A; 32];
/// let signature = client.sign(&nonce).to_bytes();
///
/// let role = verify_client(&keys, &nonce, &public_key, &signature).unwrap();
/// assert!(role.is_operator());
///
/// let impostor = SigningKey::from_bytes(&[8; 32]).sign(&nonce).to_bytes();
/// assert!(matches!(
///     verify_client(&keys, &nonce, &public_key, &impostor),
///     Err(ClientAuthError::BadSignature(_))
/// ));
/// ```
pub fn verify_client(
    authorized_keys: &AuthorizedKeys,
    nonce: &[u8; 32],
    public_key: &[u8; 32],
    signature: &[u8; 64],
) -> Result<ClientRole<RoleId>, ClientAuthError> {
    let role = authorized_keys
        .lookup(public_key)
        .ok_or(ClientAuthError::UnknownKey)?
        .client()
        .ok_or(ClientAuthError::ReplicationKeyRefused)?;
    let verifying_key =
        VerifyingKey::from_bytes(public_key).map_err(ClientAuthError::InvalidKey)?;
    verifying_key
        .verify(nonce, &Signature::from_bytes(signature))
        .map_err(ClientAuthError::BadSignature)?;
    Ok(role)
}

#[cfg(test)]
mod tests {
    use super::*;

    use base64::Engine;
    use ed25519_dalek::{Signer, SigningKey};
    use melin_app::auth::KeyRole;

    use crate::test_roles::desk_keys;

    const NONCE: [u8; 32] = [0x5A; 32];

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[0x11; 32])
    }

    fn keys_listing(role: &str, key: &SigningKey) -> AuthorizedKeys {
        let public =
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
        desk_keys(role, &public)
    }

    fn verify(
        keys: &AuthorizedKeys,
        signer: &SigningKey,
    ) -> Result<ClientRole<RoleId>, ClientAuthError> {
        let public_key = key().verifying_key().to_bytes();
        let signature = signer.sign(&NONCE).to_bytes();
        verify_client(keys, &NONCE, &public_key, &signature)
    }

    /// The operator and every application role are admitted, each as the
    /// role it is listed under.
    #[test]
    fn a_listed_client_key_that_signed_the_nonce_is_admitted() {
        for role in ["operator", "trader", "readonly"] {
            let keys = keys_listing(role, &key());
            let admitted = verify(&keys, &key()).unwrap();
            assert_eq!(keys.token(KeyRole::Client(admitted)), role);
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
            matches!(err, ClientAuthError::ReplicationKeyRefused),
            "{err}"
        );
        // Named by its token, as the operator wrote it in the keys file.
        assert_eq!(
            err.to_string(),
            "replication key may not connect as a client"
        );
        assert!(std::error::Error::source(&err).is_none());
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
        let keys = desk_keys("operator", &listed);
        let signature = key().sign(&NONCE).to_bytes();
        let err = verify_client(&keys, &NONCE, &not_a_point, &signature).unwrap_err();
        assert!(matches!(err, ClientAuthError::InvalidKey(_)), "{err}");
    }

    #[test]
    fn a_signature_by_another_key_is_refused() {
        let impostor = SigningKey::from_bytes(&[0x33; 32]);
        let err = verify(&keys_listing("operator", &key()), &impostor).unwrap_err();
        assert!(matches!(err, ClientAuthError::BadSignature(_)), "{err}");
        // The verifier's own error stays reachable for a caller's report.
        assert!(std::error::Error::source(&err).is_some());
    }
}
