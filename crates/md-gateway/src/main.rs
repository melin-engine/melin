//! FIX 4.4 market data gateway for Melin.
//!
//! Connects to the melin event publisher for order book state, then
//! serves FIX 4.4 MarketDataRequest (V) → MarketDataSnapshotFullRefresh (W)
//! and MarketDataRequestReject (Y) to connected clients.
//!
//! Usage:
//!   melin-md-gateway --config md-gateway.toml [--core N]

mod config;
pub mod event_loop;
pub mod translate;

use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let mut config_path: Option<String> = None;

    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config" => {
                i += 1;
                config_path = Some(args.get(i).cloned().unwrap_or_default());
            }
            _ => {
                eprintln!("usage: melin-md-gateway --config <path>");
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let config_path = config_path.unwrap_or_else(|| {
        eprintln!("usage: melin-md-gateway --config <path>");
        std::process::exit(1);
    });

    let config_str = std::fs::read_to_string(&config_path).unwrap_or_else(|e| {
        eprintln!("failed to read config {config_path}: {e}");
        std::process::exit(1);
    });

    let config: config::GatewayConfig = toml::from_str(&config_str).unwrap_or_else(|e| {
        eprintln!("failed to parse config: {e}");
        std::process::exit(1);
    });

    tracing::info!(
        listen = %config.listen,
        event_publisher = %config.event_publisher,
        symbols = config.symbols.len(),
        "melin-md-gateway starting"
    );

    let shutdown = Arc::new(AtomicBool::new(false));

    // Shared book mirror state between the core thread and the event loop.
    let md_state = Arc::new(RwLock::new(melin_market_data::core::MdState::new()));

    // Collect symbol IDs for the Subscribe request.
    let symbol_ids: Vec<melin_trading::types::Symbol> = config
        .symbols
        .values()
        .map(|s| melin_trading::types::Symbol(s.id))
        .collect();

    // Spawn the MarketDataCore thread — connects to the event publisher,
    // receives the snapshot, and applies firehose events to the shared mirrors.
    let core_state = Arc::clone(&md_state);
    let core_shutdown = Arc::clone(&shutdown);
    let core_addr = config.event_publisher;
    let core_key_path = config.subscriber_key.clone();
    let core_handle = std::thread::Builder::new()
        .name("md-core".into())
        .spawn(move || {
            melin_market_data::core::run(
                melin_market_data::core::CoreConfig {
                    event_publisher_addr: core_addr,
                    symbols: symbol_ids,
                    key_path: core_key_path,
                },
                core_state,
                &core_shutdown,
            );
        })
        .expect("spawn md-core thread");

    // Leak config so it can be passed as &'static to the event loop.
    let config: &'static config::GatewayConfig = Box::leak(Box::new(config));

    // Bind and run the io_uring event loop.
    let listener = TcpListener::bind(config.listen).unwrap_or_else(|e| {
        eprintln!("failed to bind {}: {e}", config.listen);
        std::process::exit(1);
    });

    let mut gw = event_loop::MdGateway::new(listener, config, md_state).unwrap_or_else(|e| {
        eprintln!("failed to create gateway: {e}");
        std::process::exit(1);
    });

    if let Err(e) = gw.run(&shutdown) {
        tracing::error!(error = %e, "md-gateway event loop error");
    }

    // Signal core thread to stop and wait for it.
    shutdown.store(true, Ordering::Relaxed);
    let _ = core_handle.join();

    tracing::info!("melin-md-gateway stopped");
}
