/// jemalloc: thread-local caches eliminate allocator lock contention,
/// giving more predictable latency than glibc malloc under high throughput.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// jemalloc tuning, applied at allocator init via the well-known
/// `malloc_conf` symbol. Set for tail-latency stability:
///
/// - `background_thread:true` — spawn a dedicated thread to do page
///   purging asynchronously instead of synchronously on the allocating
///   thread. Default jemalloc does the purge work on whatever thread
///   happens to free memory, which on the matching/journal hot path
///   shows up as occasional multi-millisecond stalls in `process_event`.
/// - `dirty_decay_ms:60000` / `muzzy_decay_ms:60000` — hold dirty/muzzy
///   pages for 60 s (vs the 10 s default) before reclaiming. Trades
///   marginally higher steady-state RSS for fewer purge events; with
///   the background thread this also bounds how often that thread runs.
///
/// The trailing NUL is required: jemalloc reads `malloc_conf` as a C
/// string. `non_upper_case_globals` is the documented spelling — the
/// symbol name has to match exactly.
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] =
    b"background_thread:true,dirty_decay_ms:60000,muzzy_decay_ms:60000\0";

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use clap::Parser;
use melin_server::exchange_app::ServerApp;
use melin_server_runtime::server::ServerConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_target(true)
        .with_thread_names(true)
        .init();

    let shutdown = Arc::new(AtomicBool::new(false));
    melin_server_runtime::process::install_shutdown_handler(&shutdown);

    let config = ServerConfig::parse();
    // Wire the trading-side AppFactory the runtime needs for fresh-app
    // construction (replication recovery, snapshot rebuild) and for
    // the bulk-seed events a fresh primary journals at startup. The
    // factory captures `accounts`/`instruments`/`max_orders_*` so the
    // runtime never names trading concepts by their wire variants.
    let factory: Arc<dyn melin_app::app_factory::AppFactory<App = ServerApp>> =
        Arc::new(melin_server::app_factory::ExchangeAppFactory::new(
            melin_server::app_factory::ExchangeAppFactoryConfig {
                accounts: config.accounts,
                instruments: config.instruments,
                max_orders_per_account: config.max_orders_per_account,
                max_orders_per_second: config.max_orders_per_second,
                max_orders_burst: config.max_orders_burst,
            },
        ));
    // Trading-side wire codecs. The runtime takes these as trait objects
    // so non-trading applications plug in their own codecs without
    // touching server.rs.
    let decoder: melin_server_runtime::reader::RequestDecoderArc<ServerApp> =
        Arc::new(melin_server::request_decoder::ExchangeRequestDecoder);
    let encoder: melin_server_runtime::response::ResponseEncoderArc<ServerApp> =
        Arc::new(melin_server::response_encoder::ExchangeResponseEncoder);
    // Event publisher is trading-only: under `trading` the binary wires
    // the market-data publisher fn; under `skip-order-exec` no publisher
    // exists, so we pass `None` and the runtime never allocates the
    // consumer slot.
    let event_publisher: Option<melin_server_runtime::server::EventPublisherFn<ServerApp>> = {
        #[cfg(feature = "trading")]
        {
            Some(melin_server::event_publisher::run)
        }
        #[cfg(not(feature = "trading"))]
        {
            None
        }
    };

    if !config.no_mlock {
        melin_server_runtime::process::try_lock_memory();
    }

    melin_server_runtime::server::run(config, factory, decoder, encoder, event_publisher, shutdown)
}
