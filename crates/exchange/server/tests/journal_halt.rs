//! Halt-gate behaviour tests for the matching stage.
//!
//! These exercise the `replicas_connected` halt gate against trading
//! semantics — when no replica is connected the stage must reject
//! state-mutating events (orders, deposits) with
//! `RejectReason::ReplicaDisconnected` and still allow read-only
//! queries (`QueryStats`) through. Reconnecting the replica must let
//! the same `request_seq` retry succeed, proving the rejection didn't
//! advance the per-key idempotency HWM.
//!
//! Application-agnostic pipeline tests (sequence allocation, segment
//! rotation, replication batch shape, divergence detection, …) live in
//! `melin-transport-core::pipeline_tests` against `TestApp`/`TestEvent`.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use melin_journal::JournalEvent;
use melin_pipeline::padding::Sequence;
use melin_pipeline::ring;
use melin_server::exchange_app::ServerApp as App;
use melin_trading::trading_event::TradingEvent;
use melin_transport_core::pipeline::MatchingStage;
use melin_transport_core::trace::mono_trace_ns;
use melin_types::types::*;

// Trading-bound aliases scoped to this integration test. Mirror the
// concrete ring-slot shapes the server's runtime monomorphises against.
type InputSlot = melin_transport_core::pipeline::InputSlot<TradingEvent>;
type OutputSlot = melin_transport_core::pipeline::OutputSlot<ExecutionReport, QueryResponse>;
type OutputPayload = melin_transport_core::pipeline::OutputPayload<ExecutionReport, QueryResponse>;

/// Return type for `start_matching_with_halt`:
/// (input_producer, output_consumer, connected_counter, shutdown, join_handle).
type MatchingHaltResult = (
    ring::Producer<InputSlot>,
    ring::Consumer<OutputSlot>,
    Arc<AtomicU32>,
    Arc<AtomicBool>,
    std::thread::JoinHandle<App>,
);

/// Return type for `build_matching_with_halt`:
/// (input_producer, output_consumer, connected_counter, shutdown, stage).
/// Same shape as `MatchingHaltResult` but with the stage handed back
/// unspawned so a test can publish events and toggle `shutdown` before
/// the matching loop ever runs.
type UnspawnedMatchingHaltResult = (
    ring::Producer<InputSlot>,
    ring::Consumer<OutputSlot>,
    Arc<AtomicU32>,
    Arc<AtomicBool>,
    MatchingStage<App>,
);

fn limit_order(id: u64, account: AccountId, side: Side, price: u64, qty: u64) -> Order {
    Order {
        id: OrderId(id),
        account,
        side,
        order_type: OrderType::Limit {
            price: Price(NonZeroU64::new(price).unwrap()),
            post_only: false,
        },
        time_in_force: TimeInForce::GTC,
        quantity: Quantity(NonZeroU64::new(qty).unwrap()),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    }
}

/// Unspawned counterpart to [`start_matching_with_halt`]. Lets a test
/// publish events into the input ring and tweak `shutdown` before the
/// matching thread starts — required to exercise the drain-on-shutdown
/// path of the halt check separately from the main run loop.
fn build_matching_with_halt(initial_connected: u32) -> UnspawnedMatchingHaltResult {
    let mut app = App::new();
    app.add_instrument(InstrumentSpec {
        symbol: Symbol(1),
        base: CurrencyId(0),
        quote: CurrencyId(1),
    });
    app.deposit(AccountId(1), CurrencyId(1), 1_000_000);

    let (input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
        .add_consumer()
        .build();
    let consumer = consumers.pop().unwrap();
    let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<OutputSlot>::new(64)
        .add_consumer()
        .build();
    let output_consumer = output_consumers.pop().unwrap();

    let dummy_cursor = Arc::new(Sequence::new(AtomicU64::new(0)));
    let events_counter = Arc::new(AtomicU64::new(0));
    let active_conns = Arc::new(AtomicU64::new(0));
    let counter = Arc::new(AtomicU32::new(initial_connected));

    let stage = MatchingStage::new(
        app,
        consumer,
        output_producer,
        events_counter,
        dummy_cursor,
        active_conns,
        Some(Arc::clone(&counter)),
        false,
        1, // starting_wire_seq (halt test does not exercise the gate)
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    (input_producer, output_consumer, counter, shutdown, stage)
}

/// Build a minimal matching stage wired with a `replicas_connected`
/// counter, spawn its run loop, and hand back the controls. The seeded
/// exchange has one instrument plus a funded account so a `SubmitOrder`
/// would normally succeed — the halt gate is what we're isolating.
fn start_matching_with_halt(initial_connected: u32) -> MatchingHaltResult {
    let (input, output, counter, shutdown, stage) = build_matching_with_halt(initial_connected);
    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));
    (input, output, counter, shutdown, handle)
}

/// Consume outputs until the request terminator, returning all
/// reports. The terminator is `is_last_in_request=true` on the
/// final slot — it may be a `Report` (when the event produced at
/// least one) or a `BatchEnd`-payload slot (zero-report case).
fn collect_reports(output: &mut ring::Consumer<OutputSlot>) -> Vec<ExecutionReport> {
    let mut reports = Vec::new();
    loop {
        if let Some((_, slot)) = output.try_consume() {
            if let OutputPayload::Report(r) = slot.payload {
                reports.push(r);
            }
            if slot.is_last_in_request {
                return reports;
            }
        }
        std::hint::spin_loop();
    }
}

#[test]
fn halt_rejects_submit_order() {
    let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

    input.publish(InputSlot {
        connection_id: 1,
        key_hash: 0xAA,
        request_seq: 1,
        sequence: 0,
        timestamp_ns: 0,
        event: JournalEvent::App(TradingEvent::SubmitOrder {
            symbol: Symbol(1),
            order: limit_order(100, AccountId(1), Side::Buy, 50, 10),
        }),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    let reports = collect_reports(&mut output);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            order_id: OrderId(100),
            account: AccountId(1),
            reason: RejectReason::ReplicaDisconnected,
            ..
        }
    ));

    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap();
}

#[test]
fn halt_rejects_deposit() {
    let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

    input.publish(InputSlot {
        connection_id: 1,
        key_hash: 0,
        request_seq: 0,
        sequence: 0,
        timestamp_ns: 0,
        event: JournalEvent::App(TradingEvent::Deposit {
            account: AccountId(1),
            currency: CurrencyId(1),
            amount: 100,
        }),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    let reports = collect_reports(&mut output);
    assert_eq!(reports.len(), 1);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ReplicaDisconnected,
            ..
        }
    ));

    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap();
}

#[test]
fn halt_allows_query_stats() {
    let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

    input.publish(InputSlot {
        connection_id: 1,
        key_hash: 0,
        request_seq: 0,
        sequence: 0,
        timestamp_ns: 0,
        event: JournalEvent::App(TradingEvent::QueryStats),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    // QueryStats always produces a single output slot — StatsHeader
    // carrying the request terminator (`is_last_in_request=true`).
    // Spin-poll without an iteration cap: under load the matching
    // thread can take longer than any fixed iteration budget, but
    // the response is guaranteed to arrive.
    let mut got_stats = false;
    loop {
        if let Some((_, slot)) = output.try_consume() {
            match slot.payload {
                OutputPayload::QueryResponse(QueryResponse::Stats { .. }) => got_stats = true,
                OutputPayload::Report(ExecutionReport::Rejected { reason, .. }) => {
                    panic!("QueryStats should not be rejected, got: {reason:?}");
                }
                _ => {}
            }
            if slot.is_last_in_request {
                break;
            }
        }
        std::hint::spin_loop();
    }
    assert!(got_stats, "should have received StatsHeader");

    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap();
}

#[test]
fn halt_then_reconnect_resumes_trading() {
    let (mut input, mut output, flag, shutdown, handle) = start_matching_with_halt(0);

    // Submit while halted — rejected.
    input.publish(InputSlot {
        connection_id: 1,
        key_hash: 0xBB,
        request_seq: 1,
        sequence: 0,
        timestamp_ns: 0,
        event: JournalEvent::App(TradingEvent::SubmitOrder {
            symbol: Symbol(1),
            order: limit_order(200, AccountId(1), Side::Buy, 50, 10),
        }),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    let reports = collect_reports(&mut output);
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ReplicaDisconnected,
            ..
        }
    ));

    // Reconnect replica.
    flag.store(1, Ordering::Relaxed);

    // Retry the same seq — should succeed now (HWM was not advanced).
    input.publish(InputSlot {
        connection_id: 1,
        key_hash: 0xBB,
        request_seq: 1,
        sequence: 0,
        timestamp_ns: 0,
        event: JournalEvent::App(TradingEvent::SubmitOrder {
            symbol: Symbol(1),
            order: limit_order(200, AccountId(1), Side::Buy, 50, 10),
        }),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    let reports = collect_reports(&mut output);
    assert!(
        reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Placed { .. })),
        "order should be placed after reconnect, got: {reports:?}"
    );

    shutdown.store(true, Ordering::Relaxed);
    handle.join().unwrap();
}

/// Transport-internal events (`connection_id == 0`) are seed/recovery
/// events the runtime publishes before any client connects — there's
/// no client to whom a `ReplicaDisconnected` rejection would be
/// addressed, and they predate any client write whose persist-before-
/// ack invariant the halt gate exists to defend. The matching stage
/// must apply them even while halted.
///
/// Without the halt exemption, a fresh primary that seeds before its
/// first replica connects (the only viable startup order on the DPDK
/// single-queue path) ends up with an empty instrument table — every
/// subsequent client request rejects with `UnknownSymbol`.
#[test]
fn seed_event_bypasses_halt() {
    let (mut input, mut output, _flag, shutdown, handle) = start_matching_with_halt(0);

    input.publish(InputSlot {
        connection_id: 0,
        key_hash: 0,
        request_seq: 0,
        sequence: 0,
        timestamp_ns: 0,
        event: JournalEvent::App(TradingEvent::AddInstrument {
            spec: InstrumentSpec {
                symbol: Symbol(99),
                base: CurrencyId(2),
                quote: CurrencyId(3),
            },
        }),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });

    // Transport-internal events have no client to reply to, so the
    // matching stage emits no output slot (see the `connection_id !=
    // 0` guard in pipeline.rs around the BatchEnd terminator). Let
    // the run loop consume the event, then shut down and inspect the
    // returned app to confirm the seed was applied.
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(
        output.try_consume().is_none(),
        "transport-internal event emitted an output slot"
    );

    shutdown.store(true, Ordering::Relaxed);
    let app = handle.join().unwrap();

    // The constructor seeded Symbol(1); the halt-time seed added
    // Symbol(99). If the halt exemption is missing, the second add
    // would have been rejected and we'd see only Symbol(1).
    assert_eq!(
        app.instrument_count(),
        2,
        "halt-time seed should have been applied; count={}",
        app.instrument_count()
    );
    let symbols: Vec<u32> = app.instrument_specs().map(|s| s.symbol.0).collect();
    assert!(
        symbols.contains(&99),
        "Symbol(99) seed not applied; have {symbols:?}"
    );
}

/// The same exemption must hold in the drain-on-shutdown path, which
/// runs when the matching loop observes `shutdown == true` and walks
/// any remaining slots in the input ring before exiting. Without it,
/// a primary shutting down with seed events still in flight would
/// silently drop them — and the next startup would recover a journal
/// whose seeds were never applied to the live engine in the first
/// place.
#[test]
fn seed_event_during_drain_bypasses_halt() {
    let (mut input, mut output, _flag, shutdown, stage) = build_matching_with_halt(0);

    // Publish the seed before the matching thread starts so the event
    // sits in the input ring. Flipping `shutdown` to true before spawn
    // forces the run loop's first iteration into `drain_remaining` —
    // exercising the second halt-check site (the drain path), not the
    // main loop.
    input.publish(InputSlot {
        connection_id: 0,
        key_hash: 0,
        request_seq: 0,
        sequence: 0,
        timestamp_ns: 0,
        event: JournalEvent::App(TradingEvent::AddInstrument {
            spec: InstrumentSpec {
                symbol: Symbol(99),
                base: CurrencyId(2),
                quote: CurrencyId(3),
            },
        }),
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });
    shutdown.store(true, Ordering::Relaxed);

    let s = Arc::clone(&shutdown);
    let handle = std::thread::spawn(move || stage.run(&s));
    let app = handle.join().unwrap();

    // The drain path emits a BatchEnd terminator for every consumed
    // input slot — including transport-internal ones, where the run
    // loop's `connection_id != 0` guard would have suppressed it.
    // That asymmetry is harmless (downstream consumers drop slots
    // routed to connection_id 0) and orthogonal to what we're
    // verifying. What matters is that the slot is a BatchEnd marker,
    // *not* a `Rejected{ReplicaDisconnected}` report.
    if let Some((_, slot)) = output.try_consume() {
        assert!(
            matches!(slot.payload, OutputPayload::BatchEnd),
            "drain emitted a non-BatchEnd payload for a transport-internal event: {:?}",
            slot.payload
        );
        assert_eq!(slot.connection_id, 0);
    }

    assert_eq!(
        app.instrument_count(),
        2,
        "drain-time seed should have been applied; count={}",
        app.instrument_count()
    );
    let symbols: Vec<u32> = app.instrument_specs().map(|s| s.symbol.0).collect();
    assert!(
        symbols.contains(&99),
        "Symbol(99) seed not applied via drain; have {symbols:?}"
    );
}
