//! Byte-level signing payload for the Ed25519 challenge-response
//! handshake. The `Permission` taxonomy and the `AuthorizedKeys` file
//! loader live in `melin_app::auth`.

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
