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

#![cfg(test)]

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

    use melin_disruptor::padding::Sequence;
    use melin_disruptor::ring;
    use melin_transport_core::trace::mono_trace_ns;

    use crate::exchange::Exchange;
    use crate::journal::{InputSlot, JournalEvent, MatchingStage, OutputPayload, OutputSlot};
    use crate::types::RejectReason;
    use crate::types::*;

    /// Return type for `start_matching_with_halt`:
    /// (input_producer, output_consumer, connected_counter, shutdown, join_handle).
    type MatchingHaltResult = (
        ring::Producer<InputSlot>,
        ring::Consumer<OutputSlot>,
        Arc<AtomicU32>,
        Arc<AtomicBool>,
        std::thread::JoinHandle<Exchange>,
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

    /// Build a minimal matching stage wired with a `replicas_connected`
    /// counter. The seeded exchange has one instrument plus a funded
    /// account so a `SubmitOrder` would normally succeed — the halt
    /// gate is what we're isolating.
    fn start_matching_with_halt(initial_connected: u32) -> MatchingHaltResult {
        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);

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
            exchange,
            consumer,
            output_producer,
            events_counter,
            dummy_cursor,
            active_conns,
            Some(Arc::clone(&counter)),
            false,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        (input_producer, output_consumer, counter, shutdown, handle)
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
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
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
            event: JournalEvent::App(crate::trading_event::TradingEvent::Deposit {
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
            event: JournalEvent::App(crate::trading_event::TradingEvent::QueryStats),
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
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
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
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
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

    #[test]
    fn standalone_mode_no_halt() {
        // replicas_connected = None → no halt check, events always processed.
        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 1_000_000);

        let (mut input_producer, mut consumers) = ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();
        let (output_producer, mut output_consumers) = ring::DisruptorBuilder::<OutputSlot>::new(64)
            .add_consumer()
            .build();
        let mut output_consumer = output_consumers.pop().unwrap();

        let stage = MatchingStage::new(
            exchange,
            consumer,
            output_producer,
            Arc::new(AtomicU64::new(0)),
            Arc::new(Sequence::new(AtomicU64::new(0))),
            Arc::new(AtomicU64::new(0)),
            None, // standalone
            false,
        );

        let shutdown = Arc::new(AtomicBool::new(false));
        let s = Arc::clone(&shutdown);
        let handle = std::thread::spawn(move || stage.run(&s));

        input_producer.publish(InputSlot {
            connection_id: 1,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(crate::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: limit_order(1, AccountId(1), Side::Buy, 50, 10),
            }),
            publish_ts: mono_trace_ns(),
            recv_ts: mono_trace_ns(),
        });

        let reports = collect_reports(&mut output_consumer);
        assert!(
            reports
                .iter()
                .any(|r| matches!(r, ExecutionReport::Placed { .. })),
            "standalone mode should process normally, got: {reports:?}"
        );

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
