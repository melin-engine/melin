//! DPDK response stage — encodes responses and queues them for the DPDK
//! poll thread instead of writing to kernel sockets.
//!
//! The response stage still runs on its own pinned thread for cursor
//! gating and response encoding. Instead of calling `write_all` on kernel
//! sockets, it pushes `(connection_id, encoded_bytes)` into a shared
//! lock-free queue. The DPDK poll thread drains this queue into smoltcp
//! TCP sockets during each poll iteration.
//!
//! This decoupling is necessary because smoltcp is single-threaded — only
//! the DPDK poll thread can call `socket.send_slice()`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use melin_disruptor::padding::Sequence;
use melin_disruptor::spsc;

use melin_engine::journal::pipeline::{OutputPayload, OutputSlot};

use melin_protocol::codec;
use melin_protocol::message::ResponseKind;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded response size.
const MAX_RESPONSE_BUF: usize = 128;

/// Maximum wire frame size: 4-byte length prefix + MAX_RESPONSE_BUF payload.
const MAX_TX_FRAME: usize = 4 + MAX_RESPONSE_BUF;

/// An encoded frame destined for a specific connection.
/// Sent from the response stage to the DPDK poll thread via lock-free SPSC.
///
/// Fixed-size and `Copy` to fit the SPSC queue's requirements (no heap
/// allocation per frame). Trading responses are small (~20-80 bytes),
/// well within the 132-byte slot.
#[derive(Clone, Copy)]
pub struct TxFrame {
    pub connection_id: u64,
    /// Number of valid bytes in `data`.
    pub len: u16,
    /// Wire frame: [u32 length prefix][payload]. Only `data[..len]` is valid.
    pub data: [u8; MAX_TX_FRAME],
}

impl Default for TxFrame {
    fn default() -> Self {
        TxFrame {
            connection_id: 0,
            len: 0,
            data: [0u8; MAX_TX_FRAME],
        }
    }
}

impl TxFrame {
    /// The valid wire frame bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data[..self.len as usize]
    }
}

/// Control plane events for connection registration (DPDK variant).
///
/// Unlike the epoll variant, this doesn't carry a socket writer —
/// the DPDK poll thread owns all socket state.
pub enum ControlEvent {
    /// A new connection was accepted by the DPDK poll thread.
    Connected { connection_id: u64 },
    /// A connection was closed.
    Disconnected { connection_id: u64 },
}

/// Run the DPDK response stage loop. Blocks the calling thread until shutdown.
///
/// Identical to the epoll response stage except:
/// - No socket writers — encoded frames are sent via `tx_out` channel
/// - No flush syscalls — the DPDK poll thread handles transmission
/// - Heartbeats are sent via the same `tx_out` channel
pub fn run(
    mut consumer: spsc::Consumer<OutputSlot>,
    control_rx: mpsc::Receiver<ControlEvent>,
    journal_cursor: Arc<Sequence>,
    replication_cursor: Arc<AtomicU64>,
    shutdown: &AtomicBool,
    heartbeat_interval: Option<Duration>,
    active_connections: Arc<AtomicU64>,
    mut tx_out: spsc::Producer<TxFrame>,
) {
    // Track known connections (for heartbeat scheduling).
    let mut connections: HashMap<u64, ConnectionHeartbeat> = HashMap::with_capacity(256);

    let mut batch = [OutputSlot::default(); MAX_BATCH];
    let mut encode_buf = [0u8; MAX_RESPONSE_BUF];

    // Cached journal cursor value to avoid atomic reads on every slot.
    #[cfg(not(feature = "no-fsync"))]
    let mut cached_journal_pos: u64 = 0;
    #[cfg(feature = "no-fsync")]
    let _ = &journal_cursor;

    // Pre-encode heartbeat frame (fixed-size, no heap allocation).
    let mut heartbeat_frame = [0u8; 8];
    let heartbeat_len = codec::encode_response(&ResponseKind::Heartbeat, &mut heartbeat_frame)
        .expect("heartbeat encodes");

    let mut last_heartbeat_scan = Instant::now();
    let mut idle_spins: u32 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }

        // Poll control channel for connect/disconnect.
        while let Ok(event) = control_rx.try_recv() {
            match event {
                ControlEvent::Connected { connection_id } => {
                    connections.insert(
                        connection_id,
                        ConnectionHeartbeat {
                            last_send: Instant::now(),
                        },
                    );
                }
                ControlEvent::Disconnected { connection_id } => {
                    if connections.remove(&connection_id).is_some() {
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }

        // Consume output slots from matching stage.
        let count = consumer.consume_batch(&mut batch, MAX_BATCH);
        if count == 0 {
            // Send heartbeats to idle connections during idle periods.
            if let Some(interval) = heartbeat_interval {
                let now = Instant::now();
                if now.duration_since(last_heartbeat_scan) >= Duration::from_secs(1) {
                    last_heartbeat_scan = now;
                    let mut failed: Vec<u64> = Vec::new();
                    for (&conn_id, state) in connections.iter_mut() {
                        if now.duration_since(state.last_send) >= interval {
                            let mut frame = TxFrame::default();
                            frame.connection_id = conn_id;
                            frame.len = heartbeat_len as u16;
                            frame.data[..heartbeat_len]
                                .copy_from_slice(&heartbeat_frame[..heartbeat_len]);
                            if tx_out.try_publish(frame).is_err() {
                                // SPSC full — DPDK poll thread fell behind.
                                failed.push(conn_id);
                                continue;
                            }
                            state.last_send = now;
                        }
                    }
                    for conn_id in failed {
                        connections.remove(&conn_id);
                        active_connections.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }

            if idle_spins < 1000 {
                idle_spins += 1;
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            continue;
        }
        idle_spins = 0;

        // Wait for journal + replication to confirm the entire batch.
        #[cfg(not(feature = "no-fsync"))]
        {
            let max_seq = batch[..count]
                .iter()
                .map(|s| s.input_seq)
                .max()
                .expect("non-empty batch");
            let needed = max_seq + 1;
            if cached_journal_pos < needed {
                loop {
                    let journal_pos = journal_cursor.get().load(Ordering::Acquire);
                    let repl_pos = replication_cursor.load(Ordering::Acquire);
                    cached_journal_pos = journal_pos.min(repl_pos);
                    if cached_journal_pos >= needed {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }

        // Encode and queue responses.
        for slot in &batch[..count] {
            let kind = match slot.payload {
                OutputPayload::Report(report) => ResponseKind::Report(report),
                OutputPayload::BatchEnd => ResponseKind::BatchEnd,
                OutputPayload::EngineError => ResponseKind::EngineError,
                OutputPayload::StatsHeader {
                    active_connections,
                    events_processed,
                    journal_sequence,
                } => ResponseKind::StatsHeader {
                    active_connections,
                    events_processed,
                    journal_sequence,
                },
            };

            if !connections.contains_key(&slot.connection_id) {
                continue;
            }

            let written = match codec::encode_response(&kind, &mut encode_buf) {
                Ok(n) => n,
                Err(e) => {
                    tracing::error!(
                        connection_id = slot.connection_id,
                        error = %e,
                        "encode error"
                    );
                    continue;
                }
            };

            // Send the complete wire frame (with length prefix) to the
            // DPDK poll thread for transmission via lock-free SPSC.
            let mut frame = TxFrame::default();
            frame.connection_id = slot.connection_id;
            frame.len = written as u16;
            frame.data[..written].copy_from_slice(&encode_buf[..written]);
            tx_out.publish(frame);

            if let Some(state) = connections.get_mut(&slot.connection_id) {
                state.last_send = Instant::now();
            }
        }
    }
}

/// Per-connection heartbeat state. No socket writer — the DPDK poll
/// thread owns socket state.
struct ConnectionHeartbeat {
    last_send: Instant,
}
