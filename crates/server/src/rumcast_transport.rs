//! Standalone server with rumcast (reliable UDP) as the order-entry
//! transport. Mutually exclusive with the `dpdk` feature at build time.
//!
//! # What this is for
//!
//! Lets the LAN bench suite (`melin-bench`) compare TCP versus rumcast
//! on the same engine pipeline. Phase 1 deliberately keeps scope tight:
//!
//! - Standalone primary only (no replica, no promotion).
//! - **No authentication.** Bench/server are on a trusted LAN. Auth is
//!   the Phase 2 effort (option-2 session-token MAC over fragments).
//! - Single client / single bench thread. Multi-client routing is
//!   Phase 3.
//! - Kernel UDP only (rumcast's `KernelUdp`). DPDK rumcast backend is
//!   a separate effort tracked under the rumcast crate's deferred list.
//!
//! # Wiring (at a glance)
//!
//! ```text
//! [bench client]                                   [melin-server (this)]
//!   PublicationLog ──orders (UDP)──▶ SubscriptionLog ─▶ in-translator
//!                                                        │
//!                                                  input disruptor
//!                                                        │
//!                                                  engine pipeline
//!                                                        │
//!                                                  output ring
//!                                                        │
//!   SubscriptionLog ◀──responses (UDP)── PublicationLog ◀ out-translator
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use tracing::{error, info, warn};

use melin_protocol::codec;
use melin_rumcast::counters::Counters;
use melin_rumcast::pub_log::{PublicationConfig, PublicationLog};
use melin_rumcast::receiver::{ReceiverConfig, ReceiverLoop};
use melin_rumcast::sender::{SenderConfig, SenderLoop};
use melin_rumcast::sub_log::{SubscriptionConfig, SubscriptionLog};
use melin_rumcast::transport::KernelUdp;
use melin_rumcast::wire::{FrameView, data_flags};
use melin_trading::types::QueryResponse;
use melin_transport_core::pipeline::{OutputPayload, Pipeline, build_pipeline_with_replication};

use crate::server::{ServerConfig, init_engine};
use crate::{InputSlot, JournalEvent, OutputSlot};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration specific to the rumcast standalone path. Built from
/// `ServerConfig` by `main.rs::rumcast_config_from`.
#[derive(Debug, Clone, Copy)]
pub struct RumcastConfig {
    /// Local address the server binds for incoming order datagrams.
    /// Reuses the existing `--bind` ServerConfig flag so users don't
    /// have to learn a new knob.
    pub bind: SocketAddr,
    /// Client address responses are unicast to. From `--rumcast-client-addr`.
    /// Required because Phase 1 doesn't yet learn the client address
    /// from incoming frames.
    pub client_addr: SocketAddr,
}

// ---------------------------------------------------------------------------
// Wire-format constants
// ---------------------------------------------------------------------------

/// Logical session and stream IDs for the order-entry channels. Phase 1
/// uses a single fixed pair; Phase 3 (multi-client) will allocate them
/// per client.
const RUMCAST_SESSION_ID: u32 = 0xCAFEBABE;
const RUMCAST_ORDERS_STREAM: u32 = 1; // client → server
const RUMCAST_RESP_STREAM: u32 = 2; // server → client

/// 16 MiB term length. Same as the rumcast bench example. Plenty of
/// headroom; keeps rotation rare during bursts.
const TERM_LENGTH: u32 = 16 * 1024 * 1024;
/// Conservative MTU for kernel UDP — leaves ~92 bytes of headroom
/// below the typical 1500-byte Ethernet payload to absorb any IP+UDP
/// header growth (no VLAN/IPv6 surprises).
const MTU: u32 = 1408;
/// Both sides start at term_id = 1 by convention.
const INITIAL_TERM_ID: u32 = 1;
/// Single fixed receiver_id for Phase 1 (one bench client).
const SERVER_RECEIVER_ID: u64 = 1;

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Entry point for the rumcast standalone server.
pub fn run_rumcast(
    config: ServerConfig,
    rumcast_config: RumcastConfig,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        bind = %rumcast_config.bind,
        client = %rumcast_config.client_addr,
        "starting rumcast standalone server"
    );

    // ---- Engine pipeline ----
    let (app, writer, needs_seeding) = init_engine(&config)?;

    let active_connections = Arc::new(AtomicU64::new(1));
    let pipeline: Pipeline<crate::App> = build_pipeline_with_replication(
        app,
        writer,
        Duration::from_micros(config.group_commit_us),
        Arc::clone(&active_connections),
        false, // enable_replication
        config.max_journal_batch,
        config.replication_ring_size,
        !config.yield_idle, // busy_spin
        false,              // enable_event_publisher
        false,              // enable_shadow
    );

    let Pipeline {
        mut input_producer,
        journal_stage,
        matching_stage,
        mut output_consumers,
        journal_cursor,
        matching_cursor,
        ..
    } = pipeline;

    let response_consumer = output_consumers
        .pop()
        .expect("response consumer (output_consumers must have at least one)");

    // ---- Rumcast endpoints ----

    // server-side INBOUND: SubscriptionLog (subscriber to client's PublicationLog).
    let orders_sub_log = Arc::new(
        SubscriptionLog::new(SubscriptionConfig {
            session_id: RUMCAST_SESSION_ID,
            stream_id: RUMCAST_ORDERS_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
        })
        .map_err(|e| format!("rumcast SubscriptionLog config: {e:?}"))?,
    );
    let orders_socket = KernelUdp::bind(rumcast_config.bind)?;
    // Receiver dst = client's address (where SMs and NAKs flow back).
    let orders_recv_config = {
        let mut c = ReceiverConfig::defaults(rumcast_config.client_addr, SERVER_RECEIVER_ID);
        c.sm_interval = Duration::from_millis(2);
        c.nak_backoff_min = Duration::from_micros(50);
        c.nak_backoff_jitter = Duration::from_micros(50);
        c.max_recv_per_tick = 1024;
        c
    };
    let orders_receiver = ReceiverLoop::new(
        Arc::clone(&orders_sub_log),
        orders_socket,
        orders_recv_config,
    );

    // server-side OUTBOUND: PublicationLog → client's SubscriptionLog.
    let resp_pub_log = Arc::new(
        PublicationLog::new(PublicationConfig {
            session_id: RUMCAST_SESSION_ID,
            stream_id: RUMCAST_RESP_STREAM,
            initial_term_id: INITIAL_TERM_ID,
            term_length: TERM_LENGTH,
            mtu: MTU,
        })
        .map_err(|e| format!("rumcast PublicationLog config: {e:?}"))?,
    );
    // Phase 1: no SM-driven flow control yet (single client we trust).
    // Set the limit wide-open; the bench publishes orders at a rate
    // the server processes at, so backpressure here would only stem
    // from a slow subscriber — out of scope for Phase 1.
    resp_pub_log.set_publisher_limit(u64::MAX);
    let resp_socket = KernelUdp::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())?;
    let resp_send_config = {
        let mut c = SenderConfig::defaults(rumcast_config.client_addr);
        c.setup_interval = Duration::from_millis(100);
        c.heartbeat_interval = Duration::from_millis(50);
        c.max_drain_per_tick = 1024 * 1024;
        c
    };
    let resp_sender = SenderLoop::new(Arc::clone(&resp_pub_log), resp_socket, resp_send_config);

    // Shared counters (helpful for bench observability; cheap when nobody reads).
    let counters = Arc::new(Counters::new());

    // ---- Thread plumbing ----

    let mut handles: Vec<thread::JoinHandle<()>> = Vec::new();

    // Pipeline: journal stage.
    let journal_shutdown = Arc::clone(&shutdown);
    handles.push(
        thread::Builder::new()
            .name("journal".into())
            .spawn(move || {
                if let Err(e) = journal_stage.run(&journal_shutdown) {
                    error!(error = ?e, "journal stage exited with error");
                }
            })?,
    );

    // Pipeline: matching stage.
    let matching_shutdown = Arc::clone(&shutdown);
    handles.push(
        thread::Builder::new()
            .name("matching".into())
            .spawn(move || {
                let _final_app = matching_stage.run(&matching_shutdown);
            })?,
    );

    // ---- Seed accounts and instruments on first startup ----
    //
    // The bench publishes orders against a fixed set of (instrument,
    // account) IDs. Without seeding, the matching engine rejects every
    // request as "unknown instrument" / "unknown account". Mirrors the
    // TCP path's `if needs_seeding` block.
    if needs_seeding {
        seed_and_drain(
            &mut input_producer,
            &journal_cursor,
            &matching_cursor,
            config.instruments,
            config.accounts,
            &shutdown,
        );
    }

    // Idle strategy: default (no flag) = busy-spin (lowest latency on
    // isolated cores). `--yield-idle` switches all rumcast tick loops
    // and translators to sleep-tick (saves CPU on shared machines).
    // Matches the existing pipeline convention used by JournalStage /
    // MatchingStage (which take the same flag inverted as `busy_spin`).
    let yield_idle = config.yield_idle;

    // Rumcast receiver (orders) tick loop.
    {
        let shutdown = Arc::clone(&shutdown);
        let counters = Arc::clone(&counters);
        let mut receiver = orders_receiver;
        receiver.set_counters(Some(Arc::clone(&counters)));
        handles.push(
            thread::Builder::new()
                .name("rumcast-orders-recv".into())
                .spawn(move || tick_loop(&shutdown, yield_idle, || receiver.tick()))?,
        );
    }

    // Rumcast sender (responses) tick loop.
    {
        let shutdown = Arc::clone(&shutdown);
        let counters = Arc::clone(&counters);
        let mut sender = resp_sender;
        sender.set_counters(Some(Arc::clone(&counters)));
        handles.push(
            thread::Builder::new()
                .name("rumcast-resp-send".into())
                .spawn(move || tick_loop(&shutdown, yield_idle, || sender.tick()))?,
        );
    }

    // In-translator: rumcast SubscriptionLog → input disruptor.
    {
        let shutdown = Arc::clone(&shutdown);
        let log = Arc::clone(&orders_sub_log);
        handles.push(
            thread::Builder::new()
                .name("rumcast-in-xlate".into())
                .spawn(move || in_translator(log, &mut input_producer, &shutdown, yield_idle))?,
        );
    }

    // Out-translator: output ring → rumcast PublicationLog.
    {
        let shutdown = Arc::clone(&shutdown);
        let log = Arc::clone(&resp_pub_log);
        let cursor = Arc::clone(&journal_cursor);
        handles.push(
            thread::Builder::new()
                .name("rumcast-out-xlate".into())
                .spawn(move || {
                    out_translator(log, response_consumer, cursor, &shutdown, yield_idle);
                })?,
        );
    }

    info!("rumcast standalone server up; awaiting shutdown");

    // Wait for shutdown.
    while !shutdown.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(100));
    }

    info!("shutdown signalled; joining threads");
    for h in handles {
        if let Err(e) = h.join() {
            warn!(?e, "thread join error");
        }
    }
    info!("rumcast standalone server stopped");
    Ok(())
}

/// Generic tick loop body shared by the rumcast sender / receiver
/// threads. With `yield_idle = false` (default), busy-spins between
/// ticks — `spin_loop` hint, no syscall, lowest latency on an isolated
/// core. With `yield_idle = true`, sleeps for 10µs between ticks —
/// gives ~100k ticks/sec while remaining friendly to the OS scheduler
/// on shared machines.
#[inline]
fn tick_loop<F: FnMut() -> R, R>(shutdown: &AtomicBool, yield_idle: bool, mut tick: F) {
    while !shutdown.load(Ordering::Acquire) {
        let _ = tick();
        if yield_idle {
            thread::sleep(Duration::from_micros(10));
        } else {
            std::hint::spin_loop();
        }
    }
}

// ---------------------------------------------------------------------------
// Translators
// ---------------------------------------------------------------------------

/// Drains incoming order frames from the rumcast subscription log,
/// decodes them via `melin-protocol::codec`, converts to `InputSlot`
/// via the existing `crate::request::to_event` helper, and pushes to
/// the input disruptor.
///
/// Phase 1 simplifications (each documented as a Phase-N TODO):
/// - Hard-coded `connection_id = 1`. Multi-client (Phase 3) will
///   allocate one per client and route responses back accordingly.
/// - `key_hash = 1` (non-zero — non-zero values participate in the
///   engine's idempotency dedup). Phase 2 (auth) will derive this
///   from the authenticated public key.
/// - Skips `should_filter` checks (no Heartbeat / Subscribe in the
///   bench-only request stream).
/// - No permission check — Phase 2 with auth.
fn in_translator(
    log: Arc<SubscriptionLog>,
    input_producer: &mut melin_disruptor::ring::Producer<InputSlot>,
    shutdown: &AtomicBool,
    yield_idle: bool,
) {
    while !shutdown.load(Ordering::Acquire) {
        log.poll(64 * 1024, |view| {
            let FrameView::Data { header, payload } = view else {
                return;
            };
            if header.common.flags & data_flags::PADDING != 0 {
                return;
            }
            let (request_seq, request) = match codec::decode_request(payload) {
                Ok(r) => r,
                Err(e) => {
                    error!(error = ?e, "failed to decode rumcast order frame");
                    return;
                }
            };
            // Phase 2 (auth): replace with `crate::request::should_filter`
            // + `check_permission` once authenticated peers are wired in.
            let event: JournalEvent = crate::request::to_event(&request);
            let timestamp_ns = wall_clock_nanos();
            let slot = InputSlot {
                connection_id: 1,
                key_hash: 1,
                request_seq,
                sequence: 0, // assigned by journal stage
                timestamp_ns,
                event,
                ..Default::default()
            };
            input_producer.publish(slot);
        });
        if yield_idle {
            thread::sleep(Duration::from_micros(10));
        } else {
            std::hint::spin_loop();
        }
    }
}

/// Drains output slots from the matching pipeline, gates on journal
/// durability, encodes them via `melin-protocol::codec`, and publishes
/// to the rumcast response log.
///
/// Phase 1 simplification: ignores `slot.connection_id` and routes
/// every response to the single rumcast publication. Multi-client
/// (Phase 3) will look up the right publication per connection_id.
fn out_translator(
    log: Arc<PublicationLog>,
    mut consumer: melin_disruptor::ring::Consumer<OutputSlot>,
    journal_cursor: Arc<melin_disruptor::padding::Sequence>,
    shutdown: &AtomicBool,
    yield_idle: bool,
) {
    use melin_protocol::message::ResponseKind;

    let mut encode_buf = vec![0u8; 1024];

    while !shutdown.load(Ordering::Acquire) {
        let Some((_seq, slot)) = consumer.try_consume() else {
            if yield_idle {
                thread::sleep(Duration::from_micros(10));
            } else {
                std::hint::spin_loop();
            }
            continue;
        };

        // Filter out seed events: they have connection_id = 0 (set
        // by `seed_and_drain`) and aren't addressed to any client.
        // Publishing them to the rumcast response log would advance
        // publisher_position before the bench's subscription socket
        // is bound, and the bench would then see an irrecoverable
        // gap at positions 0..N (it can't NAK because it doesn't
        // know the server resp_socket's ephemeral source addr).
        if slot.connection_id == 0 {
            continue;
        }

        // Wait for journal cursor to reach this slot's input_seq —
        // persist-before-ack semantics, same as the TCP response path.
        while journal_cursor.get().load(Ordering::Acquire) <= slot.input_seq
            && !shutdown.load(Ordering::Acquire)
        {
            std::hint::spin_loop();
        }

        let kind = match slot.payload {
            OutputPayload::Report(report) => ResponseKind::Report(report),
            OutputPayload::QueryResponse(QueryResponse::Position {
                account,
                balances,
                count,
            }) => ResponseKind::PositionSnapshot {
                account,
                balances,
                count,
            },
            OutputPayload::QueryResponse(QueryResponse::Stats {
                active_connections,
                events_processed,
                journal_sequence,
            }) => ResponseKind::StatsHeader {
                active_connections,
                events_processed,
                journal_sequence,
            },
            OutputPayload::BatchEnd => ResponseKind::BatchEnd,
            OutputPayload::EngineError => ResponseKind::EngineError,
        };

        let written = match codec::encode_response(&kind, &mut encode_buf) {
            Ok(n) => n,
            Err(e) => {
                error!(error = ?e, "failed to encode response");
                continue;
            }
        };

        // `encode_response` writes the 4-byte length prefix needed by
        // TCP's byte-stream framing. Rumcast already provides per-
        // message framing, so we strip the prefix and publish only the
        // codec payload (`tag + body`). The bench's rumcast receiver
        // calls `decode_response` on this directly.
        let payload = &encode_buf[4..written];
        // Spin-claim — single response producer per connection means
        // BackPressure is rare and short.
        loop {
            match log.try_claim(payload.len() as u32) {
                Ok(mut claim) => {
                    claim.payload_mut().copy_from_slice(payload);
                    claim.publish(data_flags::UNFRAGMENTED);
                    break;
                }
                Err(_) => {
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    std::hint::spin_loop();
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Seed instruments and accounts on first startup, then wait for the
/// pipeline's journal + matching cursors to drain past the last seed
/// event. Inlined from `run_as_primary`'s seeding block — Phase 1 only
/// supports a subset (no replication ring drain wait, no event
/// publisher) so the inlined version stays small.
fn seed_and_drain(
    input_producer: &mut melin_disruptor::ring::Producer<InputSlot>,
    journal_cursor: &Arc<melin_disruptor::padding::Sequence>,
    matching_cursor: &Arc<melin_disruptor::padding::Sequence>,
    instruments: u32,
    accounts: u32,
    shutdown: &AtomicBool,
) {
    use melin_journal::trace::trace_ts;
    use melin_journal::wall_clock_nanos as journal_wall_clock_nanos;
    use melin_trading::trading_event::TradingEvent;
    use melin_trading::types::{AccountId, CurrencyId, InstrumentSpec, Symbol};

    let seed_start = std::time::Instant::now();

    // Instruments first — accounts may need them present.
    for i in 0..instruments {
        input_producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: journal_wall_clock_nanos(),
            event: JournalEvent::App(TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(i),
                    base: CurrencyId(i * 2),
                    quote: CurrencyId(i * 2 + 1),
                },
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
    }

    let mut last_published_seq = 0u64;
    for acct in 1..=accounts {
        last_published_seq = input_producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: journal_wall_clock_nanos(),
            event: JournalEvent::App(TradingEvent::ProvisionAccount {
                account: AccountId(acct),
                amount: u64::MAX / 4,
            }),
            publish_ts: trace_ts(),
            recv_ts: trace_ts(),
        });
    }

    // Wait for both stages to drain past the last seed event.
    let target = last_published_seq + 1;
    info!(
        instruments,
        accounts, target, "seeding: waiting for pipeline to drain"
    );
    while !shutdown.load(Ordering::Relaxed)
        && (journal_cursor.get().load(Ordering::Acquire) < target
            || matching_cursor.get().load(Ordering::Acquire) < target)
    {
        std::hint::spin_loop();
    }
    info!(elapsed = ?seed_start.elapsed(), "seeding complete");
}

fn wall_clock_nanos() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
