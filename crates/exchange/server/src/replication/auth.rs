//! Replication authentication — Ed25519 challenge/response.
//!
//! Both halves of the handshake live here: `authenticate_replica` runs on
//! the primary side and verifies the replica's signature; `authenticate_with_primary`
//! runs on the replica side and signs the challenge.
//!
//! The wire framing and message encoders/decoders live in
//! `melin_transport_core::replication::protocol`; this module is the
//! exchange-side glue that pairs the generic auth flow with the
//! operator-managed `AuthorizedKeys` permission table.

use std::io::{self, Read, Write};

use melin_transport_core::replication::protocol::{
    MAX_CONTROL_FRAME, decode_auth_result, decode_challenge, decode_challenge_response,
    encode_auth_failed, encode_auth_ok, encode_challenge, encode_challenge_response, read_frame,
};

/// Authenticate a replica connection (primary side).
///
/// Sends a 32-byte nonce challenge, verifies the replica's Ed25519
/// signature, and checks that the key has `Replication` permission.
/// Must complete within the stream's existing read timeout.
pub(super) fn authenticate_replica(
    reader: &mut impl Read,
    writer: &mut impl Write,
    authorized_keys: &melin_protocol::auth::AuthorizedKeys,
) -> io::Result<()> {
    use ed25519_dalek::{Verifier, VerifyingKey};

    // Generate random nonce.
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("getrandom failed: {e}")))?;

    // Send Challenge.
    let mut buf = Vec::with_capacity(64);
    encode_challenge(&nonce, &mut buf);
    writer.write_all(&buf)?;
    writer.flush()?;

    // Read ChallengeResponse.
    let frame = read_frame(reader, MAX_CONTROL_FRAME)?;
    let (signature_bytes, pubkey_bytes) = match decode_challenge_response(&frame) {
        Ok(pair) => pair,
        Err(e) => {
            buf.clear();
            encode_auth_failed(&mut buf);
            let _ = writer.write_all(&buf);
            return Err(io::Error::other(format!("bad challenge response: {e}")));
        }
    };

    // Look up key and verify permission.
    let permission = match authorized_keys.lookup(&pubkey_bytes) {
        Some(perm) => perm,
        None => {
            buf.clear();
            encode_auth_failed(&mut buf);
            let _ = writer.write_all(&buf);
            return Err(io::Error::other("unknown replication key"));
        }
    };
    if !permission.is_replication() {
        buf.clear();
        encode_auth_failed(&mut buf);
        let _ = writer.write_all(&buf);
        return Err(io::Error::other(format!(
            "key has {permission:?} permission, expected Replication"
        )));
    }

    // Verify Ed25519 signature over the nonce.
    let verifying_key = VerifyingKey::from_bytes(&pubkey_bytes).map_err(|e| {
        buf.clear();
        encode_auth_failed(&mut buf);
        let _ = writer.write_all(&buf);
        io::Error::other(format!("invalid public key: {e}"))
    })?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key.verify(&nonce, &signature).map_err(|e| {
        buf.clear();
        encode_auth_failed(&mut buf);
        let _ = writer.write_all(&buf);
        io::Error::other(format!("signature verification failed: {e}"))
    })?;

    // Auth succeeded.
    buf.clear();
    encode_auth_ok(&mut buf);
    writer.write_all(&buf)?;
    writer.flush()?;

    Ok(())
}

/// Authenticate with the primary (replica side).
///
/// Reads the nonce challenge, signs it with the replica's private key,
/// sends the response, and waits for AuthOk/AuthFailed.
pub(super) fn authenticate_with_primary(
    reader: &mut impl Read,
    writer: &mut impl Write,
    signing_key: &ed25519_dalek::SigningKey,
) -> io::Result<()> {
    use ed25519_dalek::Signer;

    // Read Challenge.
    let frame = read_frame(reader, MAX_CONTROL_FRAME)?;
    let nonce = decode_challenge(&frame)?;

    // Sign the nonce.
    let signature = signing_key.sign(&nonce);
    let pubkey = signing_key.verifying_key();

    // Send ChallengeResponse.
    let mut buf = Vec::with_capacity(128);
    encode_challenge_response(&signature.to_bytes(), pubkey.as_bytes(), &mut buf);
    writer.write_all(&buf)?;
    writer.flush()?;

    // Read auth result.
    let result_frame = read_frame(reader, MAX_CONTROL_FRAME)?;
    match decode_auth_result(&result_frame)? {
        true => Ok(()),
        false => Err(io::Error::other("primary rejected replication key")),
    }
}
