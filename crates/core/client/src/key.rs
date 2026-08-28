//! Signing keys as clients keep them, and the public-key forms a node's
//! `authorized_keys` file wants.
//!
//! Two on-disk forms are read: a raw 32-byte seed (the runtime's own
//! convention, e.g. `--replication-key`) and the PKCS#8 PEM that
//! `openssl genpkey -algorithm ed25519` writes. Nothing else — a key is
//! not the place for a lenient parser.

use std::path::Path;

use base64::Engine;
use ed25519_dalek::{SigningKey, VerifyingKey};

use crate::Error;

/// The DER prefix `openssl genpkey -algorithm ed25519` puts in front of the
/// 32-byte seed: a PKCS#8 v1 `PrivateKeyInfo` for the Ed25519 OID
/// (1.3.101.112) with the seed wrapped in a `CurvePrivateKey` OCTET
/// STRING. Ed25519 has no parameters, so the encoding is fixed and the
/// whole key is this prefix plus the seed — no ASN.1 parser needed.
const PKCS8_ED25519_PREFIX: [u8; 16] = [
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

/// Load a signing key from a file holding either a raw 32-byte seed or
/// a PKCS#8 PEM.
pub fn load_signing_key(path: &Path) -> Result<SigningKey, Error> {
    let bytes = std::fs::read(path)
        .map_err(|e| Error::Key(format!("cannot read {}: {e}", path.display())))?;
    signing_key_from_bytes(&bytes).map_err(|e| match e {
        Error::Key(reason) => Error::Key(format!("{}: {reason}", path.display())),
        other => other,
    })
}

/// Parse a signing key from the bytes of either on-disk form.
pub fn signing_key_from_bytes(bytes: &[u8]) -> Result<SigningKey, Error> {
    if let Ok(seed) = <[u8; 32]>::try_from(bytes) {
        return Ok(SigningKey::from_bytes(&seed));
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| Error::Key("not a 32-byte seed, and not PEM text either".into()))?;
    let body: String = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("-----"))
        .collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body)
        .map_err(|e| {
            Error::Key(format!(
                "not a 32-byte seed, and PEM body is not base64: {e}"
            ))
        })?;
    der.strip_prefix(&PKCS8_ED25519_PREFIX)
        .and_then(|seed| <[u8; 32]>::try_from(seed).ok())
        .map(|seed| SigningKey::from_bytes(&seed))
        .ok_or_else(|| Error::Key("PEM is not an unencrypted PKCS#8 Ed25519 private key".into()))
}

/// The public key as a node's `authorized_keys` file spells it: standard
/// base64 of the 32 raw bytes, the same string `openssl pkey -pubout
/// -outform DER | tail -c 32 | base64` prints.
pub fn public_key_base64(key: &VerifyingKey) -> String {
    base64::engine::general_purpose::STANDARD.encode(key.to_bytes())
}

/// One line of a node's `authorized_keys` file: `<role> <public key>
/// <comment>`. The role is one of the runtime's (`operator`, `trader`,
/// `custodian`, `readonly`, `replication`); the comment is free text
/// without spaces, for the operator's benefit.
pub fn authorized_keys_line(role: &str, key: &VerifyingKey, comment: &str) -> String {
    format!("{role} {} {comment}", public_key_base64(key))
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
        let key = signing_key_from_bytes(OPENSSL_PEM.as_bytes()).unwrap();
        assert_eq!(key.to_bytes(), OPENSSL_SEED);
        assert_eq!(
            public_key_base64(&key.verifying_key()),
            OPENSSL_PUBKEY_B64,
            "the public key must match what the documented authorized_keys recipe produces"
        );
    }

    #[test]
    fn a_raw_seed_loads_as_is() {
        let seed = [0xAA; 32];
        assert_eq!(signing_key_from_bytes(&seed).unwrap().to_bytes(), seed);
    }

    #[test]
    fn other_key_material_is_refused() {
        assert!(signing_key_from_bytes(&[0u8; 31]).is_err(), "short seed");
        assert!(
            signing_key_from_bytes(&[0u8; 33]).is_err(),
            "long seed, not text"
        );
        assert!(
            signing_key_from_bytes(b"not a key at all").is_err(),
            "text, not base64"
        );
        // Valid base64 but not a PKCS#8 Ed25519 key.
        let rsa_ish = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n";
        assert!(
            signing_key_from_bytes(rsa_ish.as_bytes()).is_err(),
            "wrong DER prefix"
        );
        // Right prefix, but the seed is one byte short.
        let short = base64::engine::general_purpose::STANDARD
            .encode([&PKCS8_ED25519_PREFIX[..], &[0u8; 31]].concat());
        assert!(
            signing_key_from_bytes(short.as_bytes()).is_err(),
            "truncated seed"
        );
    }

    #[test]
    fn a_file_is_named_in_its_error() {
        let err = load_signing_key(Path::new("/nonexistent/client.pem")).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/client.pem"), "{err}");
    }

    #[test]
    fn an_authorized_keys_line_is_role_key_comment() {
        let key = SigningKey::from_bytes(&OPENSSL_SEED).verifying_key();
        assert_eq!(
            authorized_keys_line("trader", &key, "desk-1"),
            format!("trader {OPENSSL_PUBKEY_B64} desk-1")
        );
    }
}
