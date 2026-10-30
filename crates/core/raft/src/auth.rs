//! Ed25519 challenge-response for control-plane peer links — the async
//! counterpart of the replication handshake in
//! `melin-server-runtime/src/replication/auth.rs`, sharing the same pure
//! frame codecs from `melin_transport_core::replication::protocol` so the
//! two paths cannot diverge on the wire.
//!
//! Trust model: authentication is **mutual**, one challenge each way over
//! the same codecs. The responder challenges first ([`challenge_peer`]),
//! as the replication handshake does, and pins the dialer's verified key
//! to its configured node id; after the version exchange the dialer
//! challenges the responder ([`challenge_responder`]) and checks that the
//! key is the one configured for the node id it dialled, so a reply (which
//! the dialer fences on and records as the peer's tip) comes from that
//! peer and not from whatever answered at its address. Either side
//! verifies against the operator's `authorized_keys` table and requires
//! the `replication` role: control-plane peers live in the same trust
//! domain as the data-plane replication links, distinct from operator
//! admin keys.

use std::collections::HashMap;
use std::fmt;
use std::io;

use ed25519_dalek::Signer;
use ed25519_dalek::Verifier;
use melin_app::auth::AuthorizedKeys;
use melin_transport_core::replication::protocol::MAX_CONTROL_FRAME;
use melin_transport_core::replication::protocol::decode_auth_result;
use melin_transport_core::replication::protocol::decode_challenge;
use melin_transport_core::replication::protocol::decode_challenge_response;
use melin_transport_core::replication::protocol::encode_auth_failed;
use melin_transport_core::replication::protocol::encode_auth_ok;
use melin_transport_core::replication::protocol::encode_challenge;
use melin_transport_core::replication::protocol::encode_challenge_response;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::types::NodeId;
use crate::wire::read_bounded_payload;

/// Async read of one control frame (`[len: u32 LE][payload]`), bounded by
/// [`MAX_CONTROL_FRAME`] — the async analog of `protocol::read_frame`.
async fn read_control_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Vec<u8>> {
    read_bounded_payload(r, MAX_CONTROL_FRAME, "control frame").await
}

/// Why one end of a raft link refused the key the other end proved. A type
/// of its own, carried inside an `io::Error` on the dialing side, so
/// `network::to_rpc_error` can tell a refusal, which no retry cures until
/// the operator acts, from a transient failure. Every variant is the other
/// end's doing (or the operator's configuration), never this node's
/// malfunction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum KeyRefusal {
    /// The challenge response does not decode. The codec's message, as
    /// text: its error is an `io::Error`, which is not `Clone`.
    Malformed(String),
    /// The key is not in `authorized_keys`.
    Unknown,
    /// The key is listed under another role than `replication`.
    WrongRole { listed_as: &'static str },
    /// The key bytes are not a valid Ed25519 public key.
    InvalidKey(String),
    /// The signature does not verify over the nonce this end issued.
    BadSignature(String),
    /// The key verifies and holds the `replication` role, but it is not
    /// the one configured for the node this end dialled. `belongs_to` is
    /// the configured peer whose key it is, if any: a swapped address
    /// names both nodes.
    NotThePeer {
        dialled: NodeId,
        belongs_to: Option<NodeId>,
    },
    /// No `--raft-peer` entry configures a key for the node this end is
    /// about to dial (a node of the stored membership missing from the
    /// list), so nothing could check who answers at its address. Refused
    /// before connecting.
    NoKeyConfigured { dialled: NodeId },
}

impl fmt::Display for KeyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyRefusal::Malformed(e) => write!(f, "bad challenge response: {e}"),
            KeyRefusal::Unknown => f.write_str("unknown control-plane key"),
            KeyRefusal::WrongRole { listed_as } => {
                write!(f, "key listed as {listed_as}, expected replication")
            }
            KeyRefusal::InvalidKey(e) => write!(f, "invalid public key: {e}"),
            KeyRefusal::BadSignature(e) => write!(f, "signature verification failed: {e}"),
            KeyRefusal::NotThePeer {
                dialled,
                belongs_to: Some(other),
            } => write!(
                f,
                "the raft peer dialled as node {dialled} proved the key configured for node \
                 {other}; check the --raft-peer addresses"
            ),
            KeyRefusal::NotThePeer {
                dialled,
                belongs_to: None,
            } => write!(
                f,
                "the raft peer dialled as node {dialled} proved a replication key that no \
                 --raft-peer entry names"
            ),
            KeyRefusal::NoKeyConfigured { dialled } => write!(
                f,
                "node {dialled} is in the stored raft membership but no --raft-peer entry names \
                 its key, so this node cannot check who answers at its address"
            ),
        }
    }
}

impl std::error::Error for KeyRefusal {}

impl From<KeyRefusal> for io::Error {
    fn from(refusal: KeyRefusal) -> Self {
        io::Error::new(io::ErrorKind::PermissionDenied, refusal)
    }
}

/// How a challenge failed: the other end proved a key this end refuses (a
/// refusal, misconfiguration or impersonation, which needs the operator),
/// or the link failed under it (an I/O error, like any other on the link),
/// or this node could not draw the nonce (its own malfunction, which no
/// peer caused). An enum rather than an `io::Error` so each side matches
/// every case exhaustively and logs it at its own level.
#[derive(Debug)]
pub(crate) enum AuthError {
    Refused(KeyRefusal),
    Io(io::Error),
    Entropy(getrandom::Error),
}

impl From<io::Error> for AuthError {
    fn from(e: io::Error) -> Self {
        AuthError::Io(e)
    }
}

impl From<KeyRefusal> for AuthError {
    fn from(refusal: KeyRefusal) -> Self {
        AuthError::Refused(refusal)
    }
}

impl From<AuthError> for io::Error {
    fn from(e: AuthError) -> Self {
        match e {
            AuthError::Refused(refusal) => refusal.into(),
            AuthError::Io(e) => e,
            AuthError::Entropy(e) => io::Error::other(format!("getrandom failed: {e}")),
        }
    }
}

/// Verify a `ChallengeResponse` payload against the nonce we issued,
/// returning the verified public key on success. The one verification of
/// both challenges on a raft link, the responder's of the dialer and the
/// dialer's of the responder.
///
/// Mirrors `verify_challenge_response` in the server-runtime replication
/// auth (which is crate-private there — melin-raft sits *below*
/// server-runtime in the dependency graph, so it cannot be imported without
/// inverting the graph). The security-critical primitives — the frame codec,
/// the `AuthorizedKeys` lookup, and the dalek verify — are all shared; only
/// these ~20 lines of glue are restated. Keep the two in sync.
fn verify_challenge_response_identified(
    nonce: &[u8; 32],
    response_payload: &[u8],
    authorized_keys: &AuthorizedKeys,
) -> Result<[u8; 32], KeyRefusal> {
    let (signature_bytes, pubkey_bytes) = decode_challenge_response(response_payload)
        .map_err(|e| KeyRefusal::Malformed(e.to_string()))?;

    let role = authorized_keys
        .lookup(&pubkey_bytes)
        .ok_or(KeyRefusal::Unknown)?;
    if !role.is_replication() {
        return Err(KeyRefusal::WrongRole {
            listed_as: authorized_keys.token(role),
        });
    }

    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pubkey_bytes)
        .map_err(|e| KeyRefusal::InvalidKey(e.to_string()))?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key
        .verify(nonce, &signature)
        .map_err(|e| KeyRefusal::BadSignature(e.to_string()))?;

    Ok(pubkey_bytes)
}

/// Challenge the other end of `stream` to prove its key: send a fresh
/// nonce, verify the signed response ([`verify_challenge_response_identified`]),
/// hand the verified key to `accept`, and tell the other end the verdict.
/// On a refusal, by the verification or by `accept`, a best-effort
/// `AuthFailed` notice is sent before the error.
async fn challenge<S, T>(
    stream: &mut S,
    authorized_keys: &AuthorizedKeys,
    accept: impl FnOnce([u8; 32]) -> Result<T, KeyRefusal>,
) -> Result<T, AuthError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(AuthError::Entropy)?;

    let mut buf = Vec::with_capacity(64);
    encode_challenge(&nonce, &mut buf);
    stream.write_all(&buf).await?;
    stream.flush().await?;

    let response = read_control_frame(stream).await?;
    match verify_challenge_response_identified(&nonce, &response, authorized_keys).and_then(accept)
    {
        Ok(accepted) => {
            buf.clear();
            encode_auth_ok(&mut buf);
            stream.write_all(&buf).await?;
            stream.flush().await?;
            Ok(accepted)
        }
        Err(refusal) => {
            // Best-effort AuthFailed before dropping — the connection is
            // about to close, so a failed write here is not actionable.
            buf.clear();
            encode_auth_failed(&mut buf);
            let _ = stream.write_all(&buf).await;
            Err(refusal.into())
        }
    }
}

/// Responder side, first step: challenge the connecting peer, verify its
/// signature, and return the verified public key for identity pinning.
pub(crate) async fn challenge_peer<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    authorized_keys: &AuthorizedKeys,
) -> Result<[u8; 32], AuthError> {
    challenge(stream, authorized_keys, Ok).await
}

/// Dialer side, after the version exchange: challenge the responder and
/// accept only the key `peer_ids` (the `--raft-peer` table, key → node id,
/// the one the responder pins dialers with) maps to `dialled`.
pub(crate) async fn challenge_responder<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    authorized_keys: &AuthorizedKeys,
    peer_ids: &HashMap<[u8; 32], NodeId>,
    dialled: NodeId,
) -> Result<(), AuthError> {
    challenge(stream, authorized_keys, |key| {
        pin_to_dialled(peer_ids, dialled, &key)
    })
    .await
}

/// The dialer's pin: `key` is the one `peer_ids` maps to `dialled`.
fn pin_to_dialled(
    peer_ids: &HashMap<[u8; 32], NodeId>,
    dialled: NodeId,
    key: &[u8; 32],
) -> Result<(), KeyRefusal> {
    match peer_ids.get(key) {
        Some(&id) if id == dialled => Ok(()),
        other => Err(KeyRefusal::NotThePeer {
            dialled,
            belongs_to: other.copied(),
        }),
    }
}

/// Whether `peer_ids` configures a key for `dialled` at all, checked before
/// connecting: without one, [`challenge_responder`] could only refuse.
/// A scan of the values: the table holds the handful of configured
/// voters, and one table keyed by public key serves both ends' pins.
pub(crate) fn key_configured_for(
    peer_ids: &HashMap<[u8; 32], NodeId>,
    dialled: NodeId,
) -> Result<(), KeyRefusal> {
    if peer_ids.values().any(|&id| id == dialled) {
        Ok(())
    } else {
        Err(KeyRefusal::NoKeyConfigured { dialled })
    }
}

/// Answer the other end's challenge: read its nonce, sign it with
/// `signing_key`, await the verdict. The dialer's first step, and the
/// responder's after the version exchange.
pub(crate) async fn answer_challenge<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    signing_key: &ed25519_dalek::SigningKey,
) -> io::Result<()> {
    let frame = read_control_frame(stream).await?;
    let nonce = decode_challenge(&frame)?;

    let signature = signing_key.sign(&nonce);
    let mut buf = Vec::with_capacity(128);
    encode_challenge_response(
        &signature.to_bytes(),
        signing_key.verifying_key().as_bytes(),
        &mut buf,
    );
    stream.write_all(&buf).await?;
    stream.flush().await?;

    let result = read_control_frame(stream).await?;
    if decode_auth_result(&result)? {
        Ok(())
    } else {
        Err(io::Error::other("peer rejected control-plane key"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::authorized_keys_table;
    use ed25519_dalek::SigningKey;

    /// A table listing `key` under `role`.
    fn keys_for(key: &SigningKey, role: &str) -> AuthorizedKeys {
        authorized_keys_table(&[(key, role, "test")])
    }

    /// Drive both halves over an in-memory duplex pipe.
    async fn run_handshake(
        client_key: SigningKey,
        table: AuthorizedKeys,
    ) -> (Result<[u8; 32], AuthError>, io::Result<()>) {
        let (mut client_end, mut server_end) = tokio::io::duplex(1024);
        tokio::join!(
            async { challenge_peer(&mut server_end, &table).await },
            async { answer_challenge(&mut client_end, &client_key).await },
        )
    }

    fn refusal<T: fmt::Debug>(outcome: Result<T, AuthError>) -> KeyRefusal {
        match outcome {
            Err(AuthError::Refused(refusal)) => refusal,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn valid_replication_key_authenticates_and_is_identified() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let table = keys_for(&key, "replication");
        let expected = key.verifying_key().to_bytes();
        let (server, client) = run_handshake(key, table).await;
        assert_eq!(server.unwrap(), expected);
        client.unwrap();
    }

    /// An end that hangs up instead of answering proved nothing, right or
    /// wrong: an I/O error, not a refusal.
    #[tokio::test]
    async fn a_peer_hanging_up_mid_challenge_is_an_io_error_not_a_refusal() {
        let (mut challenger_end, other_end) = tokio::io::duplex(1024);
        drop(other_end);
        let table = keys_for(&SigningKey::from_bytes(&[0x11; 32]), "replication");
        match challenge_peer(&mut challenger_end, &table).await {
            Err(AuthError::Io(_)) => {}
            other => panic!("expected an I/O error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_key_is_rejected() {
        let signer = SigningKey::from_bytes(&[0x22; 32]);
        let listed = SigningKey::from_bytes(&[0x33; 32]);
        let (server, client) = run_handshake(signer, keys_for(&listed, "replication")).await;
        assert_eq!(refusal(server), KeyRefusal::Unknown);
        assert!(client.unwrap_err().to_string().contains("rejected"));
    }

    #[tokio::test]
    async fn operator_key_is_rejected() {
        let key = SigningKey::from_bytes(&[0x44; 32]);
        let table = keys_for(&key, "operator");
        let (server, client) = run_handshake(key, table).await;
        assert_eq!(
            refusal(server).to_string(),
            "key listed as operator, expected replication"
        );
        assert!(client.is_err());
    }

    /// Node 1 (`[0x51; 32]`) and node 2 (`[0x52; 32]`), both listed under
    /// `replication`, and the `--raft-peer` table mapping each to its id.
    fn two_peers() -> (AuthorizedKeys, HashMap<[u8; 32], NodeId>) {
        let (one, two) = (
            SigningKey::from_bytes(&[0x51; 32]),
            SigningKey::from_bytes(&[0x52; 32]),
        );
        let table = authorized_keys_table(&[
            (&one, "replication", "node-1"),
            (&two, "replication", "node-2"),
        ]);
        let peer_ids = HashMap::from([
            (one.verifying_key().to_bytes(), 1),
            (two.verifying_key().to_bytes(), 2),
        ]);
        (table, peer_ids)
    }

    /// A dialer that dialled `dialled` challenges a responder signing with
    /// `responder_key`.
    async fn dialer_challenges(
        dialled: NodeId,
        responder_key: SigningKey,
    ) -> (Result<(), AuthError>, io::Result<()>) {
        let (table, peer_ids) = two_peers();
        let (mut dialer_end, mut responder_end) = tokio::io::duplex(1024);
        tokio::join!(
            challenge_responder(&mut dialer_end, &table, &peer_ids, dialled),
            answer_challenge(&mut responder_end, &responder_key),
        )
    }

    #[tokio::test]
    async fn the_dialer_accepts_the_key_configured_for_the_node_it_dialled() {
        let (dialer, responder) = dialer_challenges(2, SigningKey::from_bytes(&[0x52; 32])).await;
        dialer.unwrap();
        responder.unwrap();
    }

    /// Another configured peer's key is refused, naming both nodes, and
    /// the responder is told so (`AuthFailed`), not left waiting.
    #[tokio::test]
    async fn the_dialer_refuses_another_configured_peers_key_and_says_so() {
        let (dialer, responder) = dialer_challenges(2, SigningKey::from_bytes(&[0x51; 32])).await;
        let refusal = refusal(dialer);
        assert_eq!(
            refusal,
            KeyRefusal::NotThePeer {
                dialled: 2,
                belongs_to: Some(1)
            }
        );
        let message = refusal.to_string();
        assert!(
            message.contains("node 2") && message.contains("node 1"),
            "{message}"
        );
        assert!(responder.unwrap_err().to_string().contains("rejected"));
    }

    /// A signature over another nonce (a replayed response) is refused,
    /// though the key is the right one.
    #[tokio::test]
    async fn the_dialer_refuses_a_signature_over_another_nonce() {
        let (table, peer_ids) = two_peers();
        let key = SigningKey::from_bytes(&[0x52; 32]);
        let (mut dialer_end, mut responder_end) = tokio::io::duplex(1024);
        let (dialer, ()) = tokio::join!(
            challenge_responder(&mut dialer_end, &table, &peer_ids, 2),
            async {
                read_control_frame(&mut responder_end).await.unwrap();
                let mut buf = Vec::new();
                encode_challenge_response(
                    &key.sign(&[0u8; 32]).to_bytes(),
                    key.verifying_key().as_bytes(),
                    &mut buf,
                );
                responder_end.write_all(&buf).await.unwrap();
            },
        );
        assert!(matches!(refusal(dialer), KeyRefusal::BadSignature(_)));
    }

    /// Whatever answers with a frame that is not a challenge response is
    /// refused, not trusted.
    #[tokio::test]
    async fn the_dialer_refuses_a_malformed_response() {
        let (table, peer_ids) = two_peers();
        let (mut dialer_end, mut responder_end) = tokio::io::duplex(1024);
        let (dialer, ()) = tokio::join!(
            challenge_responder(&mut dialer_end, &table, &peer_ids, 2),
            async {
                read_control_frame(&mut responder_end).await.unwrap();
                let mut buf = Vec::new();
                encode_auth_ok(&mut buf);
                responder_end.write_all(&buf).await.unwrap();
            },
        );
        let refusal = refusal(dialer);
        assert!(matches!(refusal, KeyRefusal::Malformed(_)), "{refusal}");
    }

    #[test]
    fn the_dialers_pin_refuses_a_listed_key_no_peer_is_configured_with() {
        let (_, peer_ids) = two_peers();
        let stray = SigningKey::from_bytes(&[0x53; 32])
            .verifying_key()
            .to_bytes();
        let refusal = pin_to_dialled(&peer_ids, 1, &stray).unwrap_err();
        assert_eq!(
            refusal,
            KeyRefusal::NotThePeer {
                dialled: 1,
                belongs_to: None
            }
        );
        assert!(refusal.to_string().contains("no --raft-peer entry"));
    }

    #[test]
    fn a_node_with_no_configured_key_is_refused_before_dialling() {
        let (_, peer_ids) = two_peers();
        assert_eq!(key_configured_for(&peer_ids, 2), Ok(()));
        let refusal = key_configured_for(&peer_ids, 3).unwrap_err();
        assert_eq!(refusal, KeyRefusal::NoKeyConfigured { dialled: 3 });
        assert!(refusal.to_string().contains("node 3"), "{refusal}");
    }

    #[test]
    fn a_refusal_rides_an_io_error_and_stays_typed() {
        let e = io::Error::from(AuthError::Refused(KeyRefusal::Unknown));
        assert_eq!(e.kind(), io::ErrorKind::PermissionDenied);
        assert!(e.get_ref().is_some_and(|inner| inner.is::<KeyRefusal>()));
        // An I/O failure stays the error it was.
        let e = io::Error::from(AuthError::Io(io::Error::from(io::ErrorKind::UnexpectedEof)));
        assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
    }
}
