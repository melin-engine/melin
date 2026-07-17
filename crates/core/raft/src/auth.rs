//! Ed25519 challenge-response for control-plane peer links — the async
//! counterpart of the replication handshake in
//! `melin-server-runtime/src/replication/auth.rs`, sharing the same pure
//! frame codecs from `melin_transport_core::replication::protocol` so the
//! two paths cannot diverge on the wire.
//!
//! Trust model (parity with replication): the **initiator proves identity**;
//! the responder verifies against the operator's `authorized_keys` table and
//! requires `Replication` permission — control-plane peers live in the same
//! trust domain as the data-plane replication links, distinct from operator
//! admin keys. The responder additionally returns the verified public key so
//! the caller can pin the connection to a configured peer id.

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
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

/// Async read of one control frame (`[len: u32 LE][payload]`), bounded by
/// [`MAX_CONTROL_FRAME`] — the async analog of `protocol::read_frame`.
async fn read_control_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_CONTROL_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("control frame length {len} exceeds cap {MAX_CONTROL_FRAME}"),
        ));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok(payload)
}

/// Verify a `ChallengeResponse` payload against the nonce we issued,
/// returning the verified public key on success.
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
) -> io::Result<[u8; 32]> {
    let (signature_bytes, pubkey_bytes) = decode_challenge_response(response_payload)
        .map_err(|e| io::Error::other(format!("bad challenge response: {e}")))?;

    let permission = authorized_keys
        .lookup(&pubkey_bytes)
        .ok_or_else(|| io::Error::other("unknown control-plane key"))?;
    if !permission.is_replication() {
        return Err(io::Error::other(format!(
            "key has {permission:?} permission, expected Replication"
        )));
    }

    let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pubkey_bytes)
        .map_err(|e| io::Error::other(format!("invalid public key: {e}")))?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key
        .verify(nonce, &signature)
        .map_err(|e| io::Error::other(format!("signature verification failed: {e}")))?;

    Ok(pubkey_bytes)
}

/// Responder side: challenge the connecting peer, verify its signature, and
/// return the verified public key for identity pinning. On verification
/// failure a best-effort `AuthFailed` notice is sent before the error.
pub async fn authenticate_inbound<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    authorized_keys: &AuthorizedKeys,
) -> io::Result<[u8; 32]> {
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("getrandom failed: {e}")))?;

    let mut buf = Vec::with_capacity(64);
    encode_challenge(&nonce, &mut buf);
    stream.write_all(&buf).await?;
    stream.flush().await?;

    let response = read_control_frame(stream).await?;
    match verify_challenge_response_identified(&nonce, &response, authorized_keys) {
        Ok(pubkey) => {
            buf.clear();
            encode_auth_ok(&mut buf);
            stream.write_all(&buf).await?;
            stream.flush().await?;
            Ok(pubkey)
        }
        Err(e) => {
            // Best-effort AuthFailed before dropping — the connection is
            // about to close, so a failed write here is not actionable.
            buf.clear();
            encode_auth_failed(&mut buf);
            let _ = stream.write_all(&buf).await;
            Err(e)
        }
    }
}

/// Initiator side: read the challenge, sign it, await the verdict.
pub async fn authenticate_outbound<S: AsyncRead + AsyncWrite + Unpin>(
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
    use base64::Engine;
    use ed25519_dalek::SigningKey;

    fn keys_for(key: &SigningKey, permission: &str) -> AuthorizedKeys {
        let pub_b64 =
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
        AuthorizedKeys::parse(&format!("{permission} {pub_b64} test\n")).unwrap()
    }

    /// Drive both halves over an in-memory duplex pipe.
    async fn run_handshake(
        client_key: SigningKey,
        table: AuthorizedKeys,
    ) -> (io::Result<[u8; 32]>, io::Result<()>) {
        let (mut client_end, mut server_end) = tokio::io::duplex(1024);
        tokio::join!(
            async { authenticate_inbound(&mut server_end, &table).await },
            async { authenticate_outbound(&mut client_end, &client_key).await },
        )
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

    #[tokio::test]
    async fn unknown_key_is_rejected() {
        let signer = SigningKey::from_bytes(&[0x22; 32]);
        let listed = SigningKey::from_bytes(&[0x33; 32]);
        let (server, client) = run_handshake(signer, keys_for(&listed, "replication")).await;
        assert!(server.unwrap_err().to_string().contains("unknown"));
        assert!(client.unwrap_err().to_string().contains("rejected"));
    }

    #[tokio::test]
    async fn operator_key_is_rejected() {
        let key = SigningKey::from_bytes(&[0x44; 32]);
        let table = keys_for(&key, "operator");
        let (server, client) = run_handshake(key, table).await;
        assert!(server.unwrap_err().to_string().contains("Replication"));
        assert!(client.is_err());
    }
}
