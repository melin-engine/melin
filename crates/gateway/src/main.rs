/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;

use trading_gateway::proxy::{self, GatewayConfig};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let listen_addr: SocketAddr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "0.0.0.0:9001".into())
        .parse()
        .expect("invalid listen address");

    let engine_addr: SocketAddr = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:9000".into())
        .parse()
        .expect("invalid engine address");

    let config = GatewayConfig {
        listen_addr,
        engine_addr,
    };

    if let Err(e) = proxy::run(&config) {
        tracing::error!(error = %e, "gateway exited with error");
        std::process::exit(1);
    }
}
