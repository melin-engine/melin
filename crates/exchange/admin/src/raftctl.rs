//! CLI tool to change the control-plane raft voter set at runtime.
//!
//! Connects to a node's admin endpoint, authenticates via Ed25519
//! challenge-response (operator key required), and sends a
//! `RAFT-ADD-VOTER` / `RAFT-REMOVE-VOTER` command. The node's raft
//! driver shepherds the change to commitment and replies with the
//! resulting voter set (or a refusal), which this tool prints.
//!
//! Usage:
//!   melin-raftctl <admin-addr> <key-file> add-voter <node-id> <raft-addr> <pubkey-b64>
//!   melin-raftctl <admin-addr> <key-file> remove-voter <node-id>
//!
//! Example:
//!   melin-raftctl 127.0.0.1:9878 ops.key add-voter 4 10.0.0.4:7000 AAAA...=
//!   melin-raftctl 127.0.0.1:9878 ops.key remove-voter 3

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use ed25519_dalek::{Signer, SigningKey};
use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};

fn usage() -> ! {
    eprintln!("usage:");
    eprintln!(
        "  melin-raftctl <admin-addr> <key-file> add-voter <node-id> <raft-addr> <pubkey-b64>"
    );
    eprintln!("  melin-raftctl <admin-addr> <key-file> remove-voter <node-id>");
    std::process::exit(1);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        usage();
    }

    let addr: std::net::SocketAddr = args[1].parse().unwrap_or_else(|e| {
        eprintln!("error: invalid admin address '{}': {e}", args[1]);
        std::process::exit(1);
    });

    // Build the command line from the subcommand + its arguments. The
    // server re-validates every field; this only assembles the request.
    let command = match args[3].as_str() {
        "add-voter" if args.len() == 7 => {
            format!("RAFT-ADD-VOTER {} {} {}", args[4], args[5], args[6])
        }
        "remove-voter" if args.len() == 5 => format!("RAFT-REMOVE-VOTER {}", args[4]),
        _ => usage(),
    };

    let seed = std::fs::read(&args[2]).unwrap_or_else(|e| {
        eprintln!("error: failed to read key file '{}': {e}", args[2]);
        std::process::exit(1);
    });
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
    let mut stream =
        TcpStream::connect_timeout(&addr, Duration::from_secs(5)).unwrap_or_else(|e| {
            eprintln!("error: failed to connect to {addr}: {e}");
            std::process::exit(1);
        });
    // The node blocks up to 15 s waiting on the raft commit before
    // answering; give the read comfortable headroom past that.
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .expect("set read timeout");

    authenticate(&mut stream, &signing_key);

    if let Err(e) = stream.write_all(format!("{command}\n").as_bytes()) {
        eprintln!("error: failed to send command: {e}");
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
            println!("{trimmed}");
            if !trimmed.starts_with("OK") {
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("error: failed to read response: {e}");
            std::process::exit(1);
        }
    }
}

/// Ed25519 challenge-response against the admin endpoint (operator key).
/// Exits the process on any failure — this is a one-shot CLI.
fn authenticate(stream: &mut TcpStream, signing_key: &SigningKey) {
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
    let nonce = match codec::decode_response(&frame_buf) {
        Ok(ResponseKind::Challenge { nonce }) => nonce,
        Ok(other) => {
            eprintln!("error: expected Challenge, got {other:?}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("error: failed to decode challenge: {e}");
            std::process::exit(1);
        }
    };

    let signature = signing_key.sign(&nonce);
    let request = Request::ChallengeResponse {
        signature: signature.to_bytes(),
        public_key: signing_key.verifying_key().to_bytes(),
    };
    let mut encode_buf = [0u8; 256];
    let written = codec::encode_request(&request, 0, &mut encode_buf).unwrap_or_else(|e| {
        eprintln!("error: failed to encode ChallengeResponse: {e}");
        std::process::exit(1);
    });
    if let Err(e) = stream.write_all(&encode_buf[..written]) {
        eprintln!("error: failed to send ChallengeResponse: {e}");
        std::process::exit(1);
    }
    stream.flush().expect("flush");

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
        Ok(ResponseKind::ServerReady) => eprintln!("Authenticated."),
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
}
