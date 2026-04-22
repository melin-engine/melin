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
use melin_disruptor::ring;
use melin_disruptor::spsc;

use crate::{OutputPayload, OutputSlot};
use melin_trading::types::QueryResponse;
use melin_transport_core::pipeline::StageUtilization;

use melin_protocol::codec;
use melin_protocol::message::ResponseKind;

/// Maximum number of output slots consumed per batch.
const MAX_BATCH: usize = 1024;

/// Maximum encoded response size. PositionSnapshot is the largest variant
/// at up to 330 bytes.
const MAX_RESPONSE_BUF: usize = 512;

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
/// Unlike the TCP variant, this doesn't carry a socket writer —
/// the DPDK poll thread owns all socket state.
pub enum ControlEvent {
    /// A new connection was accepted by the DPDK poll thread.
    Connected { connection_id: u64 },
    /// A connection was closed.
    Disconnected { connection_id: u64 },
}

/// Run the DPDK response stage loop. Blocks the calling thread until shutdown.
///
/// Identical to the TCP response stage except:
/// - No socket writers — encoded frames are sent via `tx_out` channel
/// - No flush syscalls — the DPDK poll thread handles transmission
/// - Heartbeats are sent via the same `tx_out` channel
///
/// Top-level thread entry point — the wide arg list mirrors stage state
/// owned elsewhere; bundling into a config struct adds indirection
/// without simplifying.
#[allow(clippy::too_many_arguments)]
pub fn run(
    mut consumer: ring::Consumer<OutputSlot>,
    control_rx: mpsc::Receiver<ControlEvent>,
    journal_cursor: Arc<Sequence>,
    replication_cursor: Arc<AtomicU64>,
    fastest_replica_cursor: Arc<AtomicU64>,
    quorum_durability: bool,
    shutdown: &AtomicBool,
    heartbeat_interval: Option<Duration>,
    active_connections: Arc<AtomicU64>,
    mut tx_producers: Vec<spsc::Producer<TxFrame>>,
    utilization: Arc<StageUtilization>,
) {
    // Track known connections (for heartbeat scheduling).
    let mut connections: HashMap<u64, ConnectionHeartbeat> = HashMap::with_capacity(256);

    let mut batch = [OutputSlot::default(); MAX_BATCH];
    let mut encode_buf = [0u8; MAX_RESPONSE_BUF];

    // Cached durability position (see response.rs for full explanation).
    let mut cached_durable_pos: u64 = 0;

    // Pre-encode heartbeat frame (fixed-size, no heap allocation).
    let mut heartbeat_frame = [0u8; 8];
    let heartbeat_len = codec::encode_response(&ResponseKind::Heartbeat, &mut heartbeat_frame)
        .expect("heartbeat encodes");

    let mut last_heartbeat_scan = Instant::now();
    let mut idle_spins: u32 = 0;
    let mut busy_count: u64 = 0;
    let mut idle_count: u64 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            utilization.busy.store(busy_count, Ordering::Relaxed);
            utilization.idle.store(idle_count, Ordering::Relaxed);
            return;
        }

        // Poll control channel for connect/disconnect.
        // Counter accounting: the response stage is the sole owner of
        // active_connections decrements. The poll thread increments on
        // auth success and sends ControlEvent::Disconnected on close.
        process_control_events(
            &control_rx,
            &mut connections,
            &active_connections,
            last_heartbeat_scan,
        );

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
                            let mut frame = TxFrame {
                                connection_id: conn_id,
                                len: heartbeat_len as u16,
                                ..Default::default()
                            };
                            frame.data[..heartbeat_len]
                                .copy_from_slice(&heartbeat_frame[..heartbeat_len]);
                            let tid = (conn_id >> 56) as usize % tx_producers.len();
                            if tx_producers[tid].try_publish(frame).is_err() {
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

            idle_count += 1;
            if idle_count.is_multiple_of(1024) {
                utilization.busy.store(busy_count, Ordering::Relaxed);
                utilization.idle.store(idle_count, Ordering::Relaxed);
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
        busy_count += 1;

        // Wait for durability (see response.rs for full explanation).
        {
            let max_seq = batch[..count]
                .iter()
                .map(|s| s.input_seq)
                .max()
                .expect("non-empty batch");
            let needed = max_seq + 1;
            if cached_durable_pos < needed {
                loop {
                    let journal_pos = journal_cursor.get().load(Ordering::Acquire);
                    let repl_min = replication_cursor.load(Ordering::Acquire);
                    cached_durable_pos = crate::response::durable_pos(
                        journal_pos,
                        repl_min,
                        fastest_replica_cursor.load(Ordering::Acquire),
                        quorum_durability,
                    );
                    if cached_durable_pos >= needed {
                        // Which cursor was slower — see response.rs comment.
                        if journal_pos <= repl_min {
                            utilization.gate_journal.fetch_add(1, Ordering::Relaxed);
                        } else {
                            utilization.gate_replication.fetch_add(1, Ordering::Relaxed);
                        }
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }

        // One Instant::now() per batch for heartbeat tracking instead of
        // per response — heartbeat interval is 10s, sub-ms precision is plenty.
        let batch_now = Instant::now();

        // Encode and queue responses.
        for slot in &batch[..count] {
            let kind = match slot.payload {
                OutputPayload::QueryResponse(QueryResponse::Stats {
                    active_connections,
                    events_processed,
                    journal_sequence,
                }) => ResponseKind::StatsHeader {
                    active_connections,
                    events_processed,
                    journal_sequence,
                },
                OutputPayload::QueryResponse(QueryResponse::Position {
                    account,
                    balances,
                    count,
                }) => ResponseKind::PositionSnapshot {
                    account,
                    balances,
                    count,
                },
                OutputPayload::Report(report) => ResponseKind::Report(report),
                OutputPayload::BatchEnd => ResponseKind::BatchEnd,
                OutputPayload::EngineError => ResponseKind::EngineError,
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
            let mut frame = TxFrame {
                connection_id: slot.connection_id,
                len: written as u16,
                ..Default::default()
            };
            frame.data[..written].copy_from_slice(&encode_buf[..written]);
            let tid = (slot.connection_id >> 56) as usize % tx_producers.len();
            tx_producers[tid].publish(frame);

            if let Some(state) = connections.get_mut(&slot.connection_id) {
                state.last_send = batch_now;
            }
        }
    }
}

/// Per-connection heartbeat state. No socket writer — the DPDK poll
/// thread owns socket state.
struct ConnectionHeartbeat {
    last_send: Instant,
}

/// Process a batch of control events, updating the connection map and
/// active_connections counter.
///
/// Extracted from the `run()` loop so the counter accounting invariant
/// can be unit-tested: the response stage is the **sole owner** of
/// `active_connections` decrements. The poll thread increments on auth
/// success and sends `Disconnected`; this function handles the decrement.
fn process_control_events(
    control_rx: &mpsc::Receiver<ControlEvent>,
    connections: &mut HashMap<u64, ConnectionHeartbeat>,
    active_connections: &AtomicU64,
    now: Instant,
) {
    while let Ok(event) = control_rx.try_recv() {
        match event {
            ControlEvent::Connected { connection_id } => {
                connections.insert(connection_id, ConnectionHeartbeat { last_send: now });
            }
            ControlEvent::Disconnected { connection_id } => {
                if connections.remove(&connection_id).is_some() {
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc;
    use std::time::Instant;

    /// Simulate the poll thread's side: increment counter on auth, send
    /// Disconnected on close. The response stage (process_control_events)
    /// owns the decrement.
    #[test]
    fn active_connections_single_lifecycle() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = HashMap::new();
        let now = Instant::now();

        // Poll thread: auth succeeds → increment.
        counter.fetch_add(1, Ordering::Relaxed);
        tx.send(ControlEvent::Connected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert_eq!(connections.len(), 1);

        // Poll thread: connection closes → send Disconnected (no decrement).
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(connections.len(), 0);
    }

    /// Disconnected for an unknown connection (e.g., pre-auth drop or
    /// duplicate event) must not decrement the counter.
    #[test]
    fn disconnect_unknown_connection_no_decrement() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = HashMap::new();
        let now = Instant::now();

        tx.send(ControlEvent::Disconnected { connection_id: 999 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        // Counter must stay at 0 — not wrap to u64::MAX.
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    /// Multiple connections with interleaved connect/disconnect.
    #[test]
    fn active_connections_multiple_lifecycle() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = HashMap::new();
        let now = Instant::now();

        // Three connections authenticate.
        for id in 1..=3 {
            counter.fetch_add(1, Ordering::Relaxed);
            tx.send(ControlEvent::Connected { connection_id: id })
                .unwrap();
        }
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 3);
        assert_eq!(connections.len(), 3);

        // Connection 2 disconnects.
        tx.send(ControlEvent::Disconnected { connection_id: 2 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 2);
        assert_eq!(connections.len(), 2);

        // Remaining two disconnect.
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        tx.send(ControlEvent::Disconnected { connection_id: 3 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
        assert_eq!(connections.len(), 0);
    }

    /// Duplicate Disconnected for the same connection must only decrement
    /// once (the second remove returns None).
    #[test]
    fn duplicate_disconnect_single_decrement() {
        let counter = AtomicU64::new(0);
        let (tx, rx) = mpsc::channel();
        let mut connections = HashMap::new();
        let now = Instant::now();

        counter.fetch_add(1, Ordering::Relaxed);
        tx.send(ControlEvent::Connected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);

        // Two Disconnected events for the same connection.
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        tx.send(ControlEvent::Disconnected { connection_id: 1 })
            .unwrap();
        process_control_events(&rx, &mut connections, &counter, now);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
