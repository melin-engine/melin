//! Response stage — routes matching output to per-connection channels.
//!
//! Consumes from the output SPSC queue (matching → response) and dispatches
//! each response to the appropriate connection's tokio mpsc channel. Before
//! sending, waits for the journal cursor to confirm the event is durable —
//! this is the persist-before-ack boundary.
//!
//! Runs on a dedicated OS thread.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

use tokio::sync::mpsc as tokio_mpsc;

use trading_disruptor::padding::Sequence;
use trading_disruptor::spsc;

use trading_engine::journal::pipeline::{OutputPayload, OutputSlot};

use trading_protocol::message::Response;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Control plane events for connection registration.
///
/// Sent on a `std::sync::mpsc` channel (not the disruptor) because
/// connect/disconnect is rare and not on the hot path.
pub enum ControlEvent {
    /// Register a new connection's response sender.
    Connected {
        connection_id: u64,
        sender: tokio_mpsc::Sender<Response>,
    },
    /// Remove a disconnected connection.
    Disconnected { connection_id: u64 },
}

/// Run the response stage loop. Blocks the calling thread until shutdown.
///
/// Consumes from the output SPSC and routes responses to per-connection
/// tokio mpsc channels. For each output slot, waits until the journal
/// cursor has advanced past `input_seq` before sending — ensuring the
/// client never receives a response for an event that isn't yet durable.
pub fn run(
    mut consumer: spsc::Consumer<OutputSlot>,
    control_rx: mpsc::Receiver<ControlEvent>,
    journal_cursor: Arc<Sequence>,
    shutdown: &AtomicBool,
) {
    // Connection table: maps connection IDs to their response senders.
    // HashMap for O(1) lookup. Connection count bounded by OS fd limits.
    let mut connections: HashMap<u64, tokio_mpsc::Sender<Response>> = HashMap::new();

    let mut batch = [OutputSlot::default(); MAX_BATCH];

    // Cached journal cursor value to avoid atomic reads on every slot.
    let mut cached_journal_pos: u64 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        // Poll control channel (non-blocking) for connect/disconnect.
        while let Ok(event) = control_rx.try_recv() {
            match event {
                ControlEvent::Connected {
                    connection_id,
                    sender,
                } => {
                    connections.insert(connection_id, sender);
                }
                ControlEvent::Disconnected { connection_id } => {
                    connections.remove(&connection_id);
                }
            }
        }

        // Consume output slots from matching stage.
        let count = consumer.consume_batch(&mut batch, MAX_BATCH);
        if count == 0 {
            std::hint::spin_loop();
            continue;
        }

        for slot in &batch[..count] {
            // Wait for the journal to confirm this event is durable.
            // The journal cursor represents total entries processed
            // (fsync'd). We need cursor > input_seq.
            let needed = slot.input_seq + 1;
            if cached_journal_pos < needed {
                // Spin until journal catches up. In the common case this
                // is a single atomic read (journal is ahead or just finished).
                // Under load, the journal batches many events per fsync,
                // so the cursor jumps in chunks.
                loop {
                    cached_journal_pos = journal_cursor.get().load(Ordering::Acquire);
                    if cached_journal_pos >= needed {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }

            if let Some(tx) = connections.get(&slot.connection_id) {
                let response = match slot.payload {
                    OutputPayload::Report(report) => Response::Report(report),
                    OutputPayload::BatchEnd => Response::BatchEnd,
                    OutputPayload::EngineError => Response::EngineError,
                };
                // try_send to avoid blocking — if the channel is full,
                // the response is dropped (backpressure). Client sees gap,
                // can reconnect.
                let _ = tx.try_send(response);
            }
            // Connection not found → response silently dropped.
            // Happens if client disconnected between submit and response.
        }
    }
}
