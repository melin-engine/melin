//! CLI tool to promote a replica to primary.
//!
//! Connects to the replica's promotion endpoint and sends the PROMOTE command.
//!
//! Usage:
//!   melin-promote <addr>
//!
//! Example:
//!   melin-promote 127.0.0.1:9878

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: melin-promote <addr>");
        eprintln!("  addr: promote endpoint of the replica (e.g. 127.0.0.1:9878)");
        eprintln!();
        eprintln!("example:");
        eprintln!("  melin-promote 127.0.0.1:9878");
        std::process::exit(1);
    }

    let addr = &args[1];
    let addr: std::net::SocketAddr = match addr.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("error: invalid address '{addr}': {e}");
            std::process::exit(1);
        }
    };

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
