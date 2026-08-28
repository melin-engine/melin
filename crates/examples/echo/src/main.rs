//! Echo server binary — see `lib.rs` for the application implementation.
//!
//! ## Running
//!
//! ```sh
//! # 1. Generate an Ed25519 keypair and write an authorized_keys file:
//! openssl genpkey -algorithm ed25519 -out /tmp/echo-key.pem
//! PUB=$(openssl pkey -in /tmp/echo-key.pem -pubout -outform DER | tail -c 32 | base64)
//! echo "trader $PUB me" > /tmp/authorized_keys
//!
//! # 2. Start the echo server (in release: a floor measured on a debug build is not one):
//! cargo run --release --bin echo-server -- --standalone --ack-policy disk --authorized-keys /tmp/authorized_keys --journal /tmp/echo.journal
//!
//! # 3. Measure the floor: a closed loop of full-sized requests, one in flight:
//! cargo run --release --bin echo-client -- --key /tmp/echo-key.pem --count 10000
//!
//! # 4. Measure it under load: an open loop at a fixed rate, latency counted from
//! #    when each request was due, written as an HdrHistogram log:
//! cargo run --release --bin echo-bench -- --key /tmp/echo-key.pem --rate 100K --iterations 20 --output-directory /tmp/results --output-file echo
//! ```
//!
//! Both clients print the round-trip distribution — p99 and p99.9 are the
//! figures to read, not the minimum. `--size` takes the payload down from
//! the cap. See the `lib.rs` module docs for what the floor is and why
//! the payload is the width it is, `client.rs` for the closed loop, and
//! `bench.rs` for the paced one and its `--transport dpdk`.
//!
//! ## Taking the floor apart
//!
//! Every cost the runtime adds has a switch, so each can be measured with
//! and without it; the gap between two runs is what that cost is on this
//! host:
//!
//! - `--ack-policy` is what a reply waits for. `disk` — one fsynced copy —
//!   is the standalone floor. With a replica attached (`--replication-bind`
//!   on this node, `--replica-of` on the other; see `docs/replication.md`),
//!   `ram` acknowledges on the second in-memory copy and `disk-and-ram` on
//!   both, which is what a live deployment runs.
//! - `--no-default-features` builds the server without the journal's hash
//!   chain: one BLAKE3 update per entry, and the audit trail with it.
//! - `--features melin-server-runtime/no-persist` skips journal I/O
//!   entirely. Nothing is durable; this is the sequencer with the disk
//!   taken away, and the number to compare `disk` against.
//! - Pipeline threads busy-spin by default and are pinned with `--cores`.
//!   On a shared machine `--yield-idle` keeps them from starving
//!   everything else; on isolated cores, leave it off — the figures that
//!   count are taken with the threads owning their cores.

use clap::Parser;
use echo_server::{EchoFactory, RequestDecoder, ResponseEncoder};
use melin_server_runtime::server::{self, ServerConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    let config = ServerConfig::parse();

    server::run(config, EchoFactory, RequestDecoder, ResponseEncoder, None)
}
