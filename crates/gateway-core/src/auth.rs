//! Ed25519 authentication helpers shared by all gateway binaries.

use ed25519_dalek::SigningKey;

/// Load a 32-byte Ed25519 signing key seed from a file.
///
/// The file must contain exactly 32 raw bytes (the Ed25519 seed).
/// Returns the derived `SigningKey`.
pub fn load_signing_key(path: &std::path::Path) -> Result<SigningKey, Box<dyn std::error::Error>> {
    let seed = std::fs::read(path)?;
    if seed.len() != 32 {
        return Err(format!(
            "key file must be 32 bytes, got {} ({})",
            seed.len(),
            path.display()
        )
        .into());
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&seed);
    Ok(SigningKey::from_bytes(&bytes))
}
