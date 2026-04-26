//! CLI tool to promote a replica to primary.
//!
//! Connects to the replica's promotion endpoint, authenticates via
//! Ed25519 challenge-response (operator key required), and sends the
//! PROMOTE command.
//!
//! Usage:
//!   melin-promote <addr> <key-file>
//!
//! Example:
//!   melin-promote 127.0.0.1:9878 ops.key

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use ed25519_dalek::{Signer, SigningKey};
use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: melin-promote <addr> <key-file>");
        eprintln!("  addr:     promote endpoint of the replica (e.g. 127.0.0.1:9878)");
        eprintln!("  key-file: path to the Ed25519 operator private key (32-byte seed)");
        eprintln!();
        eprintln!("example:");
        eprintln!("  melin-promote 127.0.0.1:9878 ops.key");
        std::process::exit(1);
    }

    let addr: std::net::SocketAddr = match args[1].parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: invalid address '{}': {e}", args[1]);
            std::process::exit(1);
        }
    };

    // Load the operator signing key (32-byte raw Ed25519 seed).
    let seed = match std::fs::read(&args[2]) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to read key file '{}': {e}", args[2]);
            std::process::exit(1);
        }
    };
    if seed.len() != 32 {
        eprintln!(
            "error: key file must be exactly 32 bytes (got {})",
            seed.len()
        );
        std::process::exit(1);
    }
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(&seed);
    let signing_key = SigningKey::from_bytes(&key_bytes);

    eprintln!("Connecting to {addr}...");
    let mut stream = match TcpStream::connect_timeout(&addr, Duration::from_secs(5)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: failed to connect to {addr}: {e}");
            std::process::exit(1);
        }
    };

    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");

    // --- Ed25519 challenge-response authentication ---

    // Step 1: Receive Challenge (32-byte nonce).
    let mut len_buf = [0u8; 4];
    if let Err(e) = stream.read_exact(&mut len_buf) {
        eprintln!("error: failed to read challenge: {e}");
        std::process::exit(1);
    }
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    let mut frame_buf = vec![0u8; frame_len];
    if let Err(e) = stream.read_exact(&mut frame_buf) {
        eprintln!("error: failed to read challenge payload: {e}");
        std::process::exit(1);
    }
    let (nonce, server_eph) = match codec::decode_response(&frame_buf) {
        Ok(ResponseKind::Challenge {
            nonce,
            server_x25519_eph,
        }) => (nonce, server_x25519_eph),
        Ok(other) => {
            eprintln!("error: expected Challenge, got {other:?}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("error: failed to decode challenge: {e}");
            std::process::exit(1);
        }
    };

    // Step 2: Sign nonce + ephemerals (admin TCP uses zero ephs) —
    // see `melin_protocol::auth::auth_signing_payload`.
    let client_x25519_eph = [0u8; 32];
    let signing_payload =
        melin_protocol::auth::auth_signing_payload(&nonce, &server_eph, &client_x25519_eph);
    let signature = signing_key.sign(&signing_payload);
    let request = Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: signing_key.verifying_key().to_bytes(),
        client_x25519_eph,
    };
    let mut encode_buf = [0u8; 256];
    let written = match codec::encode_request(&request, 0, &mut encode_buf) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("error: failed to encode ChallengeResponse: {e}");
            std::process::exit(1);
        }
    };
    // Send the full frame (length prefix + payload) — the server reads
    // the 4-byte length first, then the payload.
    if let Err(e) = stream.write_all(&encode_buf[..written]) {
        eprintln!("error: failed to send ChallengeResponse: {e}");
        std::process::exit(1);
    }
    stream.flush().expect("flush");

    // Step 3: Read auth result (ServerReady or AuthFailed).
    if let Err(e) = stream.read_exact(&mut len_buf) {
        eprintln!("error: failed to read auth result: {e}");
        std::process::exit(1);
    }
    let result_len = u32::from_le_bytes(len_buf) as usize;
    let mut result_buf = vec![0u8; result_len];
    if let Err(e) = stream.read_exact(&mut result_buf) {
        eprintln!("error: failed to read auth result payload: {e}");
        std::process::exit(1);
    }
    match codec::decode_response(&result_buf) {
        Ok(ResponseKind::ServerReady) => {
            eprintln!("Authenticated.");
        }
        Ok(ResponseKind::AuthFailed) => {
            eprintln!("error: authentication failed — key not authorized or not an operator key");
            std::process::exit(1);
        }
        Ok(other) => {
            eprintln!("error: unexpected response: {other:?}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("error: failed to decode auth result: {e}");
            std::process::exit(1);
        }
    }

    // --- Send PROMOTE command ---
    if let Err(e) = stream.write_all(b"PROMOTE\n") {
        eprintln!("error: failed to send PROMOTE: {e}");
        std::process::exit(1);
    }
    stream.flush().expect("flush");

    let mut reader = BufReader::new(stream);
    let mut response = String::new();
    match reader.read_line(&mut response) {
        Ok(0) => {
            eprintln!("error: server closed connection without response");
            std::process::exit(1);
        }
        Ok(_) => {
            let trimmed = response.trim();
            if trimmed == "OK" {
                eprintln!("Promotion successful — replica is now primary.");
            } else {
                eprintln!("Promotion failed: {trimmed}");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("error: failed to read response: {e}");
            std::process::exit(1);
        }
    }
}
