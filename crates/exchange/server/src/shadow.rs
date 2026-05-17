//! Server-crate facade for the shadow snapshot stage.
//!
//! The stage itself lives in `melin_transport_core::shadow` and is generic
//! over `A: Application`. This module re-exports the run loop at its
//! existing path so call sites (`crate::shadow::run`) keep compiling, and
//! hosts the trading-flavoured test suite that exercises every
//! `TradingEvent` branch against a real `Exchange`.

pub use melin_transport_core::shadow::{dispatch_event, run};

// The shadow module's test suite exercises every trading-event branch
// against a real `Exchange`. Under `skip-order-exec` the equivalent
// assertions would be trivial (every order produces the same
// `NoLiquidity` rejection), so the suite is gated to the trading
// build rather than rewritten.
#[cfg(all(test, feature = "trading", not(feature = "skip-order-exec")))]
mod tests {
    use super::*;
    use crate::App;
    use crate::JournalEvent;
    use melin_app::Application;
    use melin_transport_core::snapshot;
    use melin_types::types::*;
    use std::num::NonZeroU64;

    fn nz(v: u64) -> NonZeroU64 {
        NonZeroU64::new(v).unwrap()
    }

    fn price(p: u64) -> Price {
        Price(nz(p))
    }

    fn qty(q: u64) -> Quantity {
        Quantity(nz(q))
    }

    #[test]
    fn dispatch_event_produces_identical_state_to_direct_calls() {
        // Process the same events two ways: dispatch_event (shadow path)
        // and direct App method calls (matching path). Exercises
        // every JournalEvent variant that mutates exchange state.
        let mut shadow = App::new();
        let mut primary = App::new();
        let mut reports = Vec::new();

        let events = vec![
            // --- Instrument setup ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(0),
                    quote: CurrencyId(1),
                },
            }),
            // --- Account provisioning and deposits ---
            JournalEvent::App(
                melin_trading::trading_event::TradingEvent::ProvisionAccount {
                    account: AccountId(1),
                    amount: 200_000,
                },
            ),
            JournalEvent::App(melin_trading::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100_000,
            }),
            JournalEvent::App(melin_trading::trading_event::TradingEvent::Deposit {
                account: AccountId(2),
                currency: CurrencyId(0),
                amount: 500,
            }),
            JournalEvent::App(melin_trading::trading_event::TradingEvent::Deposit {
                account: AccountId(2),
                currency: CurrencyId(1),
                amount: 50_000,
            }),
            // --- Risk limits ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::SetRiskLimits {
                symbol: Symbol(1),
                limits: RiskLimits {
                    max_order_qty: Some(qty(1000)),
                    max_order_notional: None,
                },
            }),
            // --- Circuit breaker ---
            JournalEvent::App(
                melin_trading::trading_event::TradingEvent::SetCircuitBreaker {
                    symbol: Symbol(1),
                    config: CircuitBreakerConfig {
                        price_band_lower: Some(price(50)),
                        price_band_upper: Some(price(200)),
                        halted: false,
                    },
                },
            ),
            // --- Fee schedule ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::SetFeeSchedule {
                symbol: Symbol(1),
                schedule: FeeSchedule {
                    maker_fee_bps: -5,
                    taker_fee_bps: 10,
                },
            }),
            // --- Place a sell order (rests on book) ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(1),
                    account: AccountId(2),
                    side: Side::Sell,
                    order_type: OrderType::Limit {
                        price: price(100),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: qty(50),
                    stp: SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            }),
            // --- Place a second sell order to cancel later ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(2),
                    account: AccountId(2),
                    side: Side::Sell,
                    order_type: OrderType::Limit {
                        price: price(110),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: qty(30),
                    stp: SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            }),
            // --- Cancel-replace: move order 2 to price 105, qty 25 ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelReplace {
                symbol: Symbol(1),
                account: AccountId(2),
                order_id: OrderId(2),
                new_price: price(105),
                new_quantity: qty(25),
            }),
            // --- Cancel order 2 ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelOrder {
                account: AccountId(2),
                order_id: OrderId(2),
                symbol: Symbol(1),
            }),
            // --- Partial fill: buy 20 of the 50-lot sell ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(1),
                    account: AccountId(1),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: price(100),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: qty(20),
                    stp: SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            }),
            // --- Withdraw some funds ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::Withdraw {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 5_000,
            }),
            // --- Place a GTD order, then drive a Tick past its expiry to
            //     trigger the scheduler-driven cancel ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(3),
                    account: AccountId(1),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: price(90),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTD,
                    quantity: qty(10),
                    stp: SelfTradeProtection::Allow,
                    expiry_ns: 1_000_000,
                },
            }),
            JournalEvent::Tick { now_ns: 2_000_000 },
            // --- Place an order then cancel all for that account ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(4),
                    account: AccountId(1),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: price(80),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: qty(5),
                    stp: SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            }),
            JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelAll {
                account: AccountId(1),
            }),
            // --- No-ops that should not affect state ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::QueryStats),
            JournalEvent::GenesisHash { hash: [0xAA; 32] },
            JournalEvent::Checkpoint {
                chain_hash: [0xBB; 32],
                events_since_checkpoint: 99,
            },
            // --- Instrument lifecycle ---
            // Add a second instrument, place an order, then disable (cancels order),
            // enable, and disable+remove.
            JournalEvent::App(melin_trading::trading_event::TradingEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(2),
                    base: CurrencyId(2),
                    quote: CurrencyId(1),
                },
            }),
            JournalEvent::App(melin_trading::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(2),
                amount: 10_000,
            }),
            JournalEvent::App(melin_trading::trading_event::TradingEvent::SubmitOrder {
                symbol: Symbol(2),
                order: Order {
                    id: OrderId(10),
                    account: AccountId(1),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: price(50),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: qty(5),
                    stp: SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            }),
            JournalEvent::App(
                melin_trading::trading_event::TradingEvent::DisableInstrument { symbol: Symbol(2) },
            ),
            JournalEvent::App(
                melin_trading::trading_event::TradingEvent::EnableInstrument { symbol: Symbol(2) },
            ),
            JournalEvent::App(
                melin_trading::trading_event::TradingEvent::DisableInstrument { symbol: Symbol(2) },
            ),
            JournalEvent::App(
                melin_trading::trading_event::TradingEvent::RemoveInstrument { symbol: Symbol(2) },
            ),
            // --- End of day ---
            JournalEvent::App(melin_trading::trading_event::TradingEvent::EndOfDay),
        ];

        // Shadow path: dispatch_event. Pass timestamp 0 throughout — this
        // test isn't exercising the per-event scheduler drain, so the
        // timestamp/last_drain_ns plumbing stays inert. Use non-zero
        // key_hash / increasing request_seq so HWM state gets populated;
        // this is what would diverge if dispatch_event skipped
        // check_request_seq.
        const KEY_HASH: u64 = 0xDEAD_BEEF;
        let mut last_drain_ns: u64 = 0;
        for (i, event) in events.iter().enumerate() {
            let request_seq = (i as u64) + 1;
            dispatch_event(
                &mut shadow,
                event,
                0,
                KEY_HASH,
                request_seq,
                &mut last_drain_ns,
                &mut reports,
            );
        }

        // Primary path: direct method calls (mirrors dispatch_event logic).
        // Apply check_request_seq in lockstep with the shadow — skipping
        // queries, matching the matching stage's `!is_query` gate — so HWM
        // state matches; the final snapshot-byte comparison catches
        // divergence.
        let mut primary_reports = Vec::new();
        for (i, event) in events.iter().enumerate() {
            let request_seq = (i as u64) + 1;
            if !event.is_query() {
                assert!(primary.check_request_seq(KEY_HASH, request_seq));
            }
            primary_reports.clear();
            match *event {
                JournalEvent::App(melin_trading::trading_event::TradingEvent::AddInstrument {
                    spec,
                }) => primary.add_instrument(spec),
                JournalEvent::App(melin_trading::trading_event::TradingEvent::Deposit {
                    account,
                    currency,
                    amount,
                }) => primary.deposit(account, currency, amount),
                JournalEvent::App(melin_trading::trading_event::TradingEvent::SubmitOrder {
                    symbol,
                    order,
                }) => {
                    primary.execute(symbol, order, &mut primary_reports);
                }
                JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelOrder {
                    account,
                    order_id,
                    symbol,
                }) => {
                    primary.cancel(symbol, account, order_id, &mut primary_reports);
                }
                JournalEvent::App(melin_trading::trading_event::TradingEvent::SetRiskLimits {
                    symbol,
                    limits,
                }) => {
                    primary.set_risk_limits(symbol, limits);
                }
                JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelAll {
                    account,
                }) => {
                    primary.cancel_all(account, &mut primary_reports);
                }
                JournalEvent::App(melin_trading::trading_event::TradingEvent::EndOfDay) => {
                    primary.end_of_day(&mut primary_reports);
                }
                JournalEvent::App(
                    melin_trading::trading_event::TradingEvent::SetCircuitBreaker {
                        symbol,
                        config,
                    },
                ) => {
                    primary.set_circuit_breaker(symbol, config);
                }
                JournalEvent::App(melin_trading::trading_event::TradingEvent::CancelReplace {
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                }) => {
                    primary.cancel_replace(
                        symbol,
                        account,
                        order_id,
                        new_price,
                        new_quantity,
                        &mut primary_reports,
                    );
                }
                JournalEvent::App(melin_trading::trading_event::TradingEvent::SetFeeSchedule {
                    symbol,
                    schedule,
                }) => {
                    primary.set_fee_schedule(symbol, schedule, &mut primary_reports);
                }
                JournalEvent::App(
                    melin_trading::trading_event::TradingEvent::ProvisionAccount {
                        account,
                        amount,
                    },
                ) => {
                    primary.provision_account(account, amount);
                }
                JournalEvent::App(melin_trading::trading_event::TradingEvent::Withdraw {
                    account,
                    currency,
                    amount,
                }) => {
                    // Replay path: deterministic — see note in apply_event.
                    let _ = primary.withdraw(account, currency, amount);
                }
                JournalEvent::App(
                    melin_trading::trading_event::TradingEvent::DisableInstrument { symbol },
                ) => {
                    primary.disable_instrument(symbol, &mut primary_reports);
                }
                JournalEvent::App(
                    melin_trading::trading_event::TradingEvent::EnableInstrument { symbol },
                ) => {
                    primary.enable_instrument(symbol, &mut primary_reports);
                }
                JournalEvent::App(
                    melin_trading::trading_event::TradingEvent::RemoveInstrument { symbol },
                ) => {
                    primary.remove_instrument(symbol, &mut primary_reports);
                }
                JournalEvent::Tick { now_ns } => {
                    primary.drain_due_scheduled_tasks(now_ns, &mut primary_reports);
                }
                JournalEvent::App(melin_trading::trading_event::TradingEvent::QueryStats)
                | JournalEvent::App(melin_trading::trading_event::TradingEvent::QueryPosition {
                    ..
                })
                | JournalEvent::App(melin_trading::trading_event::TradingEvent::QueryRequestSeq)
                | JournalEvent::GenesisHash { .. }
                | JournalEvent::Checkpoint { .. }
                | JournalEvent::Shutdown => {}
            }
        }

        // Verify identical state by saving both exchanges to snapshot
        // files and comparing the raw bytes. This catches differences in
        // any internal structure (balances, order books, reservations,
        // instrument config, risk limits, circuit breakers, fee schedules).
        let dir = tempfile::tempdir().unwrap();
        let shadow_path = dir.path().join("shadow.snapshot");
        let primary_path = dir.path().join("primary.snapshot");
        let hash = [0u8; 32];

        snapshot::save::<App>(&shadow, 1, hash, &shadow_path).unwrap();
        snapshot::save::<App>(&primary, 1, hash, &primary_path).unwrap();

        let shadow_bytes = std::fs::read(&shadow_path).unwrap();
        let primary_bytes = std::fs::read(&primary_path).unwrap();
        assert_eq!(shadow_bytes, primary_bytes, "snapshot state diverged");
    }

    #[test]
    fn query_does_not_advance_shadow_hwm() {
        // The shadow reads from the pre-journal input ring, so it sees
        // queries. The matching stage skips `check_request_seq` for
        // queries (pipeline.rs `!is_query` gate), so the shadow must
        // skip too — otherwise shadow's `key_hwm` would overshoot
        // primary's and a restore would reject legitimate requests
        // whose seq falls between primary's HWM and shadow's HWM.
        //
        // Regression test: dispatch a query with a high seq, then
        // verify the app still accepts a same-seq non-query — which
        // it would not if the query had advanced the HWM.
        let mut shadow = App::new();
        let mut reports = Vec::new();
        let mut last_drain_ns: u64 = 0;
        const KEY_HASH: u64 = 0xFEED_FACE;

        let query = JournalEvent::App(melin_trading::trading_event::TradingEvent::QueryStats);
        dispatch_event(
            &mut shadow,
            &query,
            0,
            KEY_HASH,
            100,
            &mut last_drain_ns,
            &mut reports,
        );

        // A non-query request with the same seq must still be accepted —
        // proves the query didn't advance HWM above 100.
        assert!(
            shadow.check_request_seq(KEY_HASH, 100),
            "query at seq=100 must not advance HWM; seq=100 should still pass"
        );
    }

    // Generic lifecycle tests (shutdown promptness, interval-driven
    // snapshotting) live in `melin_transport_core::shadow::tests` against
    // a no-op `TestApp` — they're not trading-specific and don't belong
    // here.
}
