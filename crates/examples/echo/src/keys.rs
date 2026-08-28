//! Ed25519 client keys, as `openssl` writes them or as the runtime keeps
//! them. Shared by the two client binaries, which send the same wire.

use std::path::Path;

use base64::Engine;
use ed25519_dalek::SigningKey;

/// The DER prefix `openssl genpkey -algorithm ed25519` puts in front of the
/// 32-byte seed: a PKCS#8 v1 `PrivateKeyInfo` for the Ed25519 OID
/// (1.3.101.112) with the seed wrapped in a `CurvePrivateKey` OCTET
/// STRING. Ed25519 has no parameters, so the encoding is fixed and the
/// whole key is this prefix plus the seed — no ASN.1 parser needed.
pub const PKCS8_ED25519_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// Load a signing key from a raw 32-byte seed (the runtime's own
/// convention, e.g. `--replication-key`) or a PKCS#8 PEM.
pub fn load_key(path: &Path) -> Result<SigningKey, String> {
    let bytes =
        std::fs::read(path).map_err(|e| format!("cannot read key {}: {e}", path.display()))?;
    let seed = seed_from(&bytes).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(SigningKey::from_bytes(&seed))
}

pub fn seed_from(bytes: &[u8]) -> Result<[u8; 32], String> {
    if let Ok(seed) = <[u8; 32]>::try_from(bytes) {
        return Ok(seed);
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| "not a 32-byte seed, and not PEM text either".to_string())?;
    let body: String = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("-----"))
        .collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| format!("not a 32-byte seed, and PEM body is not base64: {e}"))?;
    der.strip_prefix(&PKCS8_ED25519_PREFIX)
        .and_then(|seed| <[u8; 32]>::try_from(seed).ok())
        .ok_or_else(|| "PEM is not an unencrypted PKCS#8 Ed25519 private key".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As written by `openssl genpkey -algorithm ed25519`, with the seed
    /// and public key that `openssl pkey` reports for it.
    const OPENSSL_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
        MC4CAQAwBQYDK2VwBCIEIDclw/zwdZEQraidYISn+CjytFLopT9cneV0G7+MvdtR\n\
        -----END PRIVATE KEY-----\n";
    const OPENSSL_SEED: [u8; 32] = [
        0x37, 0x25, 0xc3, 0xfc, 0xf0, 0x75, 0x91, 0x10, 0xad, 0xa8, 0x9d, 0x60, 0x84, 0xa7, 0xf8,
        0x28, 0xf2, 0xb4, 0x52, 0xe8, 0xa5, 0x3f, 0x5c, 0x9d, 0xe5, 0x74, 0x1b, 0xbf, 0x8c, 0xbd,
        0xdb, 0x51,
    ];
    const OPENSSL_PUBKEY_B64: &str = "+tVsQuDHgy200knb+jTv5Zs6XAr4eV5crZS0j/578Ac=";

    #[test]
    fn a_pem_from_openssl_loads_as_the_key_openssl_derives() {
        let seed = seed_from(OPENSSL_PEM.as_bytes()).unwrap();
        assert_eq!(seed, OPENSSL_SEED);
        let key = SigningKey::from_bytes(&seed);
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes()),
            OPENSSL_PUBKEY_B64,
            "the public key must match what the README's authorized_keys recipe produces"
        );
    }

    #[test]
    fn a_raw_seed_loads_as_is() {
        let seed = [0xAA; 32];
        assert_eq!(seed_from(&seed).unwrap(), seed);
    }

    #[test]
    fn other_key_material_is_refused() {
        assert!(seed_from(&[0u8; 31]).is_err(), "short seed");
        assert!(seed_from(&[0u8; 33]).is_err(), "long seed, not text");
        assert!(seed_from(b"not a key at all").is_err(), "text, not base64");
        // Valid base64 but not a PKCS#8 Ed25519 key.
        let rsa_ish = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n";
        assert!(seed_from(rsa_ish.as_bytes()).is_err(), "wrong DER prefix");
        // Right prefix, but the seed is one byte short.
        let short = base64::engine::general_purpose::STANDARD
            .encode([&PKCS8_ED25519_PREFIX[..], &[0u8; 31]].concat());
        assert!(seed_from(short.as_bytes()).is_err(), "truncated seed");
    }

    #[test]
    fn a_key_file_loads_in_either_format() {
        let dir = tempfile::tempdir().unwrap();
        let pem = dir.path().join("pem.key");
        std::fs::write(&pem, OPENSSL_PEM).unwrap();
        assert_eq!(load_key(&pem).unwrap().to_bytes(), OPENSSL_SEED);
        let raw = dir.path().join("raw.key");
        std::fs::write(&raw, [0xCC; 32]).unwrap();
        assert_eq!(load_key(&raw).unwrap().to_bytes(), [0xCC; 32]);
        assert!(load_key(&dir.path().join("missing")).is_err());
    }
}
