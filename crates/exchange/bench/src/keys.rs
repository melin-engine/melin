//! Per-client signing-key derivation for multi-client bench scenarios.
//!
//! Each bench client connection needs its own ed25519 key so the engine's
//! per-key dedup HWM partitions cleanly across connections. A shared
//! signing key collapses every connection into one `(key_hash, request_seq)`
//! namespace — the leader's HWM advances and stale arrivals from peer
//! connections get rejected as `DuplicateRequest`. See
//! `crates/core/transport-core/src/pipeline.rs` for the dedup site.
//!
//! Derivation is deterministic: given the master key and a client index,
//! the child key is fixed. Lets the bench machine derive locally while
//! the script captures the corresponding public keys for the server's
//! `authorized_keys` file.

use base64::Engine;
use ed25519_dalek::SigningKey;

/// Domain separator: keeps this derivation distinct from any other
/// blake3-keyed use of the master seed.
const DERIVE_CONTEXT: &str = "melin-bench client signing key v1";

/// Derive a deterministic per-client signing key from a master seed.
///
/// Mixes the master's 32-byte seed with `client_id` through blake3.
/// Different `client_id`s yield independent keys; the same `(master,
/// client_id)` always yields the same key.
pub fn derive_client_key(master: &SigningKey, client_id: u32) -> SigningKey {
    let mut hasher = blake3::Hasher::new_derive_key(DERIVE_CONTEXT);
    hasher.update(&master.to_bytes());
    hasher.update(&client_id.to_le_bytes());
    let seed: [u8; 32] = hasher.finalize().into();
    SigningKey::from_bytes(&seed)
}

/// Format an `authorized_keys` line for `trader` permission, base64-
/// encoding the verifying key in the format the server's auth loader
/// expects.
pub fn authorized_keys_line(key: &SigningKey, label: &str) -> String {
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
    format!("trader {pub_b64} {label}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn master() -> SigningKey {
        SigningKey::from_bytes(&[0xBE; 32])
    }

    #[test]
    fn derivation_is_deterministic() {
        let a = derive_client_key(&master(), 7);
        let b = derive_client_key(&master(), 7);
        assert_eq!(a.to_bytes(), b.to_bytes());
    }

    #[test]
    fn different_ids_yield_distinct_keys() {
        let a = derive_client_key(&master(), 0);
        let b = derive_client_key(&master(), 1);
        assert_ne!(a.to_bytes(), b.to_bytes());
        assert_ne!(
            a.verifying_key().to_bytes(),
            b.verifying_key().to_bytes(),
            "pubkeys must also differ — the engine keys dedup on the pubkey hash"
        );
    }

    #[test]
    fn different_masters_yield_distinct_keys() {
        let m1 = SigningKey::from_bytes(&[0x01; 32]);
        let m2 = SigningKey::from_bytes(&[0x02; 32]);
        assert_ne!(
            derive_client_key(&m1, 0).to_bytes(),
            derive_client_key(&m2, 0).to_bytes(),
        );
    }

    #[test]
    fn many_clients_are_pairwise_distinct() {
        let keys: Vec<[u8; 32]> = (0..128)
            .map(|i| derive_client_key(&master(), i).verifying_key().to_bytes())
            .collect();
        for i in 0..keys.len() {
            for j in (i + 1)..keys.len() {
                assert_ne!(keys[i], keys[j], "collision between client {i} and {j}");
            }
        }
    }
}
