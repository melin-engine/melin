//! Engine loop — runs on a dedicated OS thread.
//!
//! Owns the `JournaledExchange` and the connection table. All commands
//! (orders, connects, disconnects) flow through a single mpsc channel,
//! so no mutex is needed. This is the LMAX single-writer pattern.

use std::collections::HashMap;

use tokio::sync::mpsc;
use tracing::{error, info};

use trading_engine::journal::JournaledExchange;
use trading_engine::types::ExecutionReport;

use trading_protocol::message::{ConnectionId, EngineCommand, Request, Response};

/// Run the engine loop. Blocks the calling thread until the command
/// channel is closed (server shutdown).
///
/// `rx` receives commands from all client reader tasks plus connect/disconnect
/// events from the accept loop.
pub fn run(mut engine: JournaledExchange, mut rx: mpsc::Receiver<EngineCommand>) {
    // Connection table: maps connection IDs to their response senders.
    // HashMap for O(1) lookup/insert/remove. The connection count is
    // bounded by the OS file descriptor limit, so this stays small.
    let mut connections: HashMap<ConnectionId, mpsc::Sender<Response>> = HashMap::new();

    // Pre-allocated report buffer, reused across commands to avoid
    // per-request allocation on the hot path.
    let mut reports: Vec<ExecutionReport> = Vec::with_capacity(64);

    loop {
        let cmd = match rx.blocking_recv() {
            Some(cmd) => cmd,
            None => {
                // All senders dropped — server is shutting down.
                info!("command channel closed, shutting down");
                break;
            }
        };

        match cmd {
            EngineCommand::Connected {
                connection_id,
                sender,
            } => {
                connections.insert(connection_id, sender);
            }
            EngineCommand::Disconnected { connection_id } => {
                connections.remove(&connection_id);
            }
            EngineCommand::Request {
                connection_id,
                request,
            } => {
                reports.clear();
                process_request(&mut engine, &request, &mut reports);

                if let Some(tx) = connections.get(&connection_id) {
                    for report in &reports {
                        // try_send to avoid blocking the engine thread if the
                        // writer task's channel is full (backpressure). If the
                        // channel is full, the response is dropped — the client
                        // will see a gap and can reconnect.
                        let _ = tx.try_send(Response::Report(*report));
                    }
                    let _ = tx.try_send(Response::BatchEnd);
                }
            }
        }
    }
}

/// Execute a single request against the engine.
fn process_request(
    engine: &mut JournaledExchange,
    request: &Request,
    reports: &mut Vec<ExecutionReport>,
) {
    match *request {
        Request::SubmitOrder { symbol, order } => {
            if let Err(e) = engine.execute(symbol, order, reports) {
                error!(error = %e, "journal error on submit");
                // Reports may be empty — the caller will send BatchEnd anyway,
                // which tells the client the request was processed (with no fills).
            }
        }
        Request::CancelOrder { symbol, order_id } => {
            if let Err(e) = engine.cancel(symbol, order_id, reports) {
                error!(error = %e, "journal error on cancel");
            }
        }
    }
}
