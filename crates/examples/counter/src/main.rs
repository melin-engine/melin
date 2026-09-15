//! Counter server binary — see `lib.rs` for the application implementation.
//!
//! ## Running
//!
//! ```sh
//! # 1. Generate an Ed25519 keypair and write an authorized_keys file:
//! openssl genpkey -algorithm ed25519 -out /tmp/counter-key.pem
//! PUB=$(openssl pkey -in /tmp/counter-key.pem -pubout -outform DER | tail -c 32 | base64)
//! echo "operator $PUB admin" > /tmp/authorized_keys
//!
//! # 2. Start the counter server:
//! cargo run --bin counter-server -- \
//!     --standalone --ack-policy disk \
//!     --authorized-keys /tmp/authorized_keys \
//!     --journal /tmp/counter.journal
//! ```

use clap::Parser;
use counter_server::{Counter, RequestDecoder, ResponseEncoder};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::server::{self, ServerConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    let config = ServerConfig::parse();

    server::run::<Counter>(
        config,
        StartupEvents::none(),
        RequestDecoder,
        ResponseEncoder,
        None,
    )
}
