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
//!
//! # 3. Notarize a file, then verify it against its receipt — offline:
//! cargo run --bin notary-client -- notarize contract.pdf --key /tmp/notary-key.pem
//! cargo run --bin notary-client -- verify contract.pdf
//!
//! # 4. Audit the journal itself: refold the chain from disk and check
//! #    that the receipt is in it — no server, no key:
//! cargo run --bin notary-audit -- /tmp/notary.journal --receipt contract.pdf.receipt
//! ```
//!
//! Clients submit a 32-byte digest per request and receive a receipt: the
//! position it landed at, the time it was sequenced, and the chain head
//! before and after folding it in. See the `lib.rs` module docs for the
//! wire format and the reasoning behind the shape, `client.rs` for the
//! reference client, and `audit.rs` for the auditor.

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
