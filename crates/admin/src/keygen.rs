//! Generate an Ed25519 keypair for trading engine authentication.
//!
//! Writes:
//!   <name>.key          — 32-byte raw private key seed
//!   <name>.pub          — base64-encoded public key (for authorized_keys file)
//!   authorized_keys     — ready-to-use authorized_keys file entry
//!
//! Usage:
//!     melin-keygen <name> <permission>
//!
//! Example:
//!     melin-keygen ops operator
//!     melin-keygen market-maker trader
//!     melin-keygen monitor readonly

use std::path::Path;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ed25519_dalek::SigningKey;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: melin-keygen <name> <permission>");
        eprintln!("  permission: operator | trader | custodian | readonly | replication");
        eprintln!();
        eprintln!("example:");
        eprintln!("  melin-keygen ops operator");
        eprintln!("  melin-keygen market-maker trader");
        eprintln!("  melin-keygen treasury custodian");
        std::process::exit(1);
    }

    let name = &args[1];
    let permission = &args[2];

    match permission.as_str() {
        "operator" | "trader" | "custodian" | "readonly" | "replication" => {}
        other => {
            eprintln!(
                "error: invalid permission '{other}' (expected operator/trader/custodian/readonly/replication)"
            );
            std::process::exit(1);
        }
    }

    let key_path = format!("{name}.key");
    let pub_path = format!("{name}.pub");

    if Path::new(&key_path).exists() {
        eprintln!("error: {key_path} already exists (refusing to overwrite)");
        std::process::exit(1);
    }

    // Generate random keypair.
    let mut seed = [0u8; 32];
    rand::fill(&mut seed);
    let signing_key = SigningKey::from_bytes(&seed);
    let public_key = signing_key.verifying_key();
    let pub_b64 = BASE64.encode(public_key.as_bytes());

    // Write private key (raw 32-byte seed).
    std::fs::write(&key_path, seed).unwrap_or_else(|e| {
        eprintln!("error writing {key_path}: {e}");
        std::process::exit(1);
    });

    // Write public key (base64).
    std::fs::write(&pub_path, format!("{pub_b64}\n")).unwrap_or_else(|e| {
        eprintln!("error writing {pub_path}: {e}");
        std::process::exit(1);
    });

    // Print authorized_keys line to stdout for easy appending.
    let auth_line = format!("{permission} {pub_b64} {name}");
    println!("Generated keypair:");
    println!("  Private key: {key_path}");
    println!("  Public key:  {pub_path}");
    println!();
    println!("Add this line to your authorized_keys file:");
    println!("  {auth_line}");
}
