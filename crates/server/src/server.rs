//! Server orchestrator — binds the accept loop, pipeline threads, and sessions.
//!
//! On startup:
//! 1. Recovers or creates the `JournaledExchange`.
//! 2. Decomposes it into `(Exchange, JournalWriter)` via `into_parts()`.
//! 3. Builds the disruptor pipeline (input ring buffer + output SPSC).
//! 4. Spawns 4 OS threads: publisher, journal, matching, response.
//! 5. Runs the accept loop, spawning sessions for each connection.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use trading_engine::journal::JournaledExchange;
use trading_engine::journal::pipeline::build_pipeline;

use trading_protocol::message::{ConnectionId, EngineCommand, Response};
use trading_protocol::transport::{TransportListener, TransportStream};

use crate::session;

/// Server configuration.
pub struct ServerConfig {
    /// Address to bind the TCP listener.
    pub bind_addr: SocketAddr,
    /// Capacity of the engine command channel (inbound from all clients).
    /// 256K commands — at 10M orders/sec with 100 clients, gives ~2.5 ms
    /// of burst buffering per client before backpressure.
    pub command_channel_capacity: usize,
    /// Capacity of per-connection response channels.
    /// 64K slots × ~40 bytes = ~2.5 MiB per connection. Must be large
    /// enough that the TCP writer task can keep up under sustained load
    /// without dropping responses via try_send.
    pub response_channel_capacity: usize,
    /// Path to the journal file for durable event sourcing.
    pub journal_path: PathBuf,
    /// Optional path to a snapshot file for faster recovery.
    pub snapshot_path: Option<PathBuf>,
    /// CPU core IDs for pinning the 4 pipeline threads.
    /// Order: [journal, matching, response, publisher].
    /// Default: cores 1–4 (skips core 0 which handles kernel interrupts).
    ///
    /// Production recommendation: use `isolcpus` to reserve cores,
    /// keep all cores on the same NUMA node, avoid hyperthreading
    /// siblings for latency-sensitive threads.
    pub core_affinity: [usize; 4],
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9876".parse().expect("valid default addr"),
            command_channel_capacity: 262_144,
            response_channel_capacity: 65_536,
            journal_path: PathBuf::from("trading.journal"),
            snapshot_path: None,
            core_affinity: [1, 2, 3, 4],
        }
    }
}

/// Run the trading server.
///
/// 1. Initializes (or recovers) the `JournaledExchange`, then decomposes
///    it into `Exchange` and `JournalWriter` for the pipeline.
/// 2. Builds the disruptor pipeline (input ring + output SPSC + stages).
/// 3. Spawns 4 OS threads: publisher, journal, matching, response.
/// 4. Runs the accept loop, spawning sessions for each connection.
///
/// Returns when the listener encounters a fatal error.
pub async fn run<L: TransportListener>(
    listener: L,
    config: ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    run_with_shutdown(listener, config, Arc::new(AtomicBool::new(false))).await
}

/// Run the trading server with an externally controlled shutdown flag.
///
/// Same as [`run`], but the caller can set `shutdown` to `true` to trigger
/// a clean shutdown of all pipeline threads (useful for benchmarks that need
/// to collect latency trace reports).
pub async fn run_with_shutdown<L: TransportListener>(
    mut listener: L,
    config: ServerConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize or recover the exchange.
    let engine = init_engine(&config)?;

    // Decompose into parts for the pipeline.
    let (exchange, writer) = engine.into_parts();

    // Build the disruptor pipeline.
    let (input_producer, journal_stage, matching_stage, output_consumer, journal_cursor) =
        build_pipeline(exchange, writer);

    // Control channel for connect/disconnect events → response stage.
    let (control_tx, control_rx) = std::sync::mpsc::channel();

    // Spawn pipeline OS threads.
    // Copy core_affinity once — [usize; 4] is Copy, moved into each closure.
    let cores = config.core_affinity;

    let s1 = Arc::clone(&shutdown);
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || {
            apply_affinity("journal", cores[0]);
            journal_stage.run(&s1)
        })
        .expect("failed to spawn journal thread");

    let s2 = Arc::clone(&shutdown);
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            apply_affinity("matching", cores[1]);
            matching_stage.run(&s2)
        })
        .expect("failed to spawn matching thread");

    let s3 = Arc::clone(&shutdown);
    let response_handle = std::thread::Builder::new()
        .name("response".into())
        .spawn(move || {
            apply_affinity("response", cores[2]);
            crate::response::run(output_consumer, control_rx, journal_cursor, &s3)
        })
        .expect("failed to spawn response thread");

    // Command channel: all client reader tasks → publisher thread.
    let (engine_tx, engine_rx) = mpsc::channel::<EngineCommand>(config.command_channel_capacity);

    // Spawn the publisher on a dedicated OS thread.
    let publisher_handle = std::thread::Builder::new()
        .name("publisher".into())
        .spawn(move || {
            apply_affinity("publisher", cores[3]);
            crate::engine::run(engine_rx, input_producer, control_tx);
        })
        .expect("failed to spawn publisher thread");

    info!(addr = %config.bind_addr, "listening");

    // Monotonically increasing connection ID counter. AtomicU64 because
    // the accept loop is the only writer, but using atomic for future
    // flexibility (e.g., multiple listeners).
    let next_connection_id = AtomicU64::new(1);

    // Accept loop.
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!(error = %e, "accept error");
                continue;
            }
        };

        let connection_id = ConnectionId(next_connection_id.fetch_add(1, Ordering::Relaxed));

        debug!(connection_id = connection_id.0, addr = %addr, "new connection");

        // Per-connection response channel.
        let (response_tx, response_rx) =
            mpsc::channel::<Response>(config.response_channel_capacity);

        // Register the connection with the engine before spawning tasks.
        // This ensures the response stage has the sender before any
        // requests arrive.
        if engine_tx
            .send(EngineCommand::Connected {
                connection_id,
                sender: response_tx,
            })
            .await
            .is_err()
        {
            info!("engine channel closed, shutting down");
            break;
        }

        let (reader, writer) = stream.into_split();
        session::spawn_session(
            connection_id,
            reader,
            writer,
            engine_tx.clone(),
            response_rx,
            addr,
        );
    }

    // Signal pipeline threads to shut down.
    shutdown.store(true, Ordering::Relaxed);

    // Drop the sender to close the publisher thread's channel.
    drop(engine_tx);
    let _ = publisher_handle.join();
    let _ = journal_handle.join();
    let _ = matching_handle.join();
    let _ = response_handle.join();

    Ok(())
}

/// Initialize or recover the JournaledExchange from disk.
fn init_engine(config: &ServerConfig) -> Result<JournaledExchange, Box<dyn std::error::Error>> {
    if let Some(ref snap_path) = config.snapshot_path
        && snap_path.exists()
        && config.journal_path.exists()
    {
        info!("recovering from snapshot + journal");
        let engine = JournaledExchange::recover_from_snapshot(snap_path, &config.journal_path)?;
        return Ok(engine);
    }

    if config.journal_path.exists() {
        info!("recovering from journal");
        let engine = JournaledExchange::recover(&config.journal_path)?;
        Ok(engine)
    } else {
        info!("creating new journal");
        let mut engine = JournaledExchange::create(&config.journal_path)?;
        seed_test_data(&mut engine)?;
        Ok(engine)
    }
}

/// Apply CPU core affinity for a pipeline thread, logging the result.
fn apply_affinity(thread_name: &str, core_id: usize) {
    match crate::affinity::pin_to_core(core_id) {
        Ok(c) => info!(core = c, thread = thread_name, "pinned to core"),
        Err(e) => tracing::warn!(thread = thread_name, error = e, "core pinning failed"),
    }
}

/// Seed the exchange with test instruments and accounts so the TUI can
/// be used immediately. This runs only on first startup (fresh journal).
fn seed_test_data(engine: &mut JournaledExchange) -> Result<(), Box<dyn std::error::Error>> {
    use trading_engine::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};

    // Currencies: 0 = USD, 1 = BTC, 2 = ETH
    let usd = CurrencyId(0);
    let btc = CurrencyId(1);
    let eth = CurrencyId(2);

    // Instruments: symbol 1 = BTC/USD, symbol 2 = ETH/USD
    engine.add_instrument(InstrumentSpec {
        symbol: Symbol(1),
        base: btc,
        quote: usd,
    })?;
    engine.add_instrument(InstrumentSpec {
        symbol: Symbol(2),
        base: eth,
        quote: usd,
    })?;

    // Two test accounts with generous balances in all currencies.
    for &account in &[AccountId(1), AccountId(2)] {
        engine.deposit(account, usd, 1_000_000)?;
        engine.deposit(account, btc, 1_000)?;
        engine.deposit(account, eth, 10_000)?;
    }

    info!("seeded test data: 2 instruments, 2 accounts");
    Ok(())
}
