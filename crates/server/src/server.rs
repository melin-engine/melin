//! Server orchestrator — binds the accept loop, engine thread, and sessions.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tracing::{debug, error, info};

use trading_engine::journal::JournaledExchange;

use trading_protocol::message::{ConnectionId, EngineCommand, Response};
use trading_protocol::transport::{TransportListener, TransportStream};

use crate::session;

/// Server configuration.
pub struct ServerConfig {
    /// Address to bind the TCP listener.
    pub bind_addr: SocketAddr,
    /// Capacity of the engine command channel (inbound from all clients).
    /// Sized for burst absorption — 64K commands can queue without backpressure.
    pub command_channel_capacity: usize,
    /// Capacity of per-connection response channels.
    /// Smaller than the command channel since each connection handles fewer
    /// concurrent responses.
    pub response_channel_capacity: usize,
    /// Path to the journal file for durable event sourcing.
    pub journal_path: PathBuf,
    /// Optional path to a snapshot file for faster recovery.
    pub snapshot_path: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9876".parse().expect("valid default addr"),
            command_channel_capacity: 65_536,
            response_channel_capacity: 4_096,
            journal_path: PathBuf::from("trading.journal"),
            snapshot_path: None,
        }
    }
}

/// Run the trading server.
///
/// 1. Initializes (or recovers) the `JournaledExchange`.
/// 2. Spawns the engine on a dedicated OS thread.
/// 3. Runs the accept loop, spawning sessions for each connection.
///
/// Returns when the listener encounters a fatal error.
pub async fn run<L: TransportListener>(
    mut listener: L,
    config: ServerConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize or recover the exchange.
    let engine = init_engine(&config)?;

    // Command channel: all client reader tasks → engine thread.
    let (engine_tx, engine_rx) = mpsc::channel::<EngineCommand>(config.command_channel_capacity);

    // Spawn the engine on a dedicated OS thread to avoid tokio scheduler jitter.
    let engine_handle = std::thread::Builder::new()
        .name("engine".into())
        .spawn(move || {
            crate::engine::run(engine, engine_rx);
        })
        .expect("failed to spawn engine thread");

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
        // This ensures the engine has the response sender before any
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

    // Drop the sender to signal the engine thread to shut down.
    drop(engine_tx);
    let _ = engine_handle.join();

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
        let engine = JournaledExchange::create(&config.journal_path)?;
        Ok(engine)
    }
}
