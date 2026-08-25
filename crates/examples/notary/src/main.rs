//! Notary server binary — see `lib.rs` for the application implementation.
//!
//! ## Running
//!
//! ```sh
//! # 1. Generate an Ed25519 keypair and write an authorized_keys file:
//! openssl genpkey -algorithm ed25519 -out /tmp/notary-key.pem
//! PUB=$(openssl pkey -in /tmp/notary-key.pem -pubout -outform DER | tail -c 32 | base64)
//! echo "trader $PUB bench" > /tmp/authorized_keys
//!
//! # 2. Start the notary server:
//! cargo run --bin notary-server -- --standalone --ack-policy disk --authorized-keys /tmp/authorized_keys --journal /tmp/notary.journal
//! ```
//!
//! Clients submit a 32-byte digest per request and receive the position
//! it landed at plus the chain head after folding it in. See the `lib.rs`
//! module docs for the wire format and the reasoning behind the shape.

use clap::Parser;
use melin_server_runtime::server::{self, ServerConfig};
use notary_server::{NotaryFactory, RequestDecoder, ResponseEncoder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    let config = ServerConfig::parse();

    server::run(config, NotaryFactory, RequestDecoder, ResponseEncoder, None)
}
