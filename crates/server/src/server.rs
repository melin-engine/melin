//! Server orchestrator — binds the accept loop, pipeline threads, and sessions.
//!
//! On startup:
//! 1. Recovers or creates the `JournaledExchange`.
//! 2. Decomposes it into `(Exchange, JournalWriter)` via `into_parts()`.
//! 3. Builds the disruptor pipeline (input ring buffer + output SPSC).
//! 4. Spawns 3 OS threads: journal, matching, response.
//! 5. Runs the accept loop, spawning a reader OS thread per connection.
//!
//! Fully synchronous — no async runtime needed. Reader threads do blocking
//! I/O and publish directly to the disruptor. The response thread writes
//! directly to sockets.

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tracing::{debug, error, info};

use trading_engine::journal::JournaledExchange;
use trading_engine::journal::pipeline::build_pipeline;

use trading_protocol::blocking::BlockingFrameWriter;
use trading_protocol::message::ConnectionId;
use trading_protocol::transport::BlockingTransportListener;

use crate::response::ControlEvent;
use crate::session;

/// Server configuration.
pub struct ServerConfig {
    /// Address to bind the TCP listener.
    pub bind_addr: SocketAddr,
    /// Path to the journal file for durable event sourcing.
    pub journal_path: PathBuf,
    /// Optional path to a snapshot file for faster recovery.
    pub snapshot_path: Option<PathBuf>,
    /// CPU core IDs for pinning the 3 pipeline threads.
    /// Order: [journal, matching, response].
    /// Default: cores 1–3 (skips core 0 which handles kernel interrupts).
    ///
    /// Production recommendation: use `isolcpus` to reserve cores,
    /// keep all cores on the same NUMA node, avoid hyperthreading
    /// siblings for latency-sensitive threads.
    pub core_affinity: [usize; 3],
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9876".parse().expect("valid default addr"),
            journal_path: PathBuf::from("trading.journal"),
            snapshot_path: None,
            core_affinity: [1, 2, 3],
        }
    }
}

/// Run the trading server.
///
/// 1. Initializes (or recovers) the `JournaledExchange`, then decomposes
///    it into `Exchange` and `JournalWriter` for the pipeline.
/// 2. Builds the disruptor pipeline (input ring + output SPSC + stages).
/// 3. Spawns 3 OS threads: journal, matching, response.
/// 4. Runs the accept loop, spawning a reader OS thread per connection.
///
/// Returns when the listener encounters a fatal error.
pub fn run<L: BlockingTransportListener>(
    listener: L,
    config: ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    run_with_shutdown(listener, config, Arc::new(AtomicBool::new(false)))
}

/// Run the trading server with an externally controlled shutdown flag.
///
/// Same as [`run`], but the caller can set `shutdown` to `true` to trigger
/// a clean shutdown of all pipeline threads (useful for benchmarks that need
/// to collect latency trace reports).
pub fn run_with_shutdown<L: BlockingTransportListener>(
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

    // Shared producer for reader threads. Each reader locks to publish.
    // Mutex contention scales with connection count — acceptable for
    // low-medium client counts (Option 1). Thread-per-core or fan-in
    // channels are alternatives for high connection counts.
    let shared_producer = Arc::new(Mutex::new(input_producer));

    // Control channel for connect/disconnect events → response stage.
    let (control_tx, control_rx) = std::sync::mpsc::channel();

    // Spawn pipeline OS threads.
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

    info!(addr = %config.bind_addr, "listening");

    // Monotonically increasing connection ID counter. AtomicU64 because
    // the accept loop is the only writer, but using atomic for future
    // flexibility (e.g., multiple listeners).
    let next_connection_id = AtomicU64::new(1);

    // Accept loop — blocking. Each accepted connection yields blocking
    // read/write halves directly (no async-to-blocking conversion).
    loop {
        let (std_read, std_write, addr) = match listener.accept() {
            Ok(conn) => conn,
            Err(e) => {
                error!(error = %e, "accept error");
                continue;
            }
        };

        let connection_id = ConnectionId(next_connection_id.fetch_add(1, Ordering::Relaxed));

        debug!(connection_id = connection_id.0, addr = %addr, "new connection");

        // Register the writer with the response thread before spawning
        // the reader. This ensures the response stage has the writer
        // before any requests arrive.
        let boxed_writer: Box<dyn Write + Send> = Box::new(std_write);
        if control_tx
            .send(ControlEvent::Connected {
                connection_id: connection_id.0,
                writer: BlockingFrameWriter::new(boxed_writer),
            })
            .is_err()
        {
            info!("response thread gone, shutting down");
            break;
        }

        // Spawn a dedicated reader thread for this connection.
        session::spawn_reader_thread(
            connection_id,
            std_read,
            Arc::clone(&shared_producer),
            control_tx.clone(),
            addr,
        );
    }

    // Signal pipeline threads to shut down.
    shutdown.store(true, Ordering::Relaxed);

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
