//! Shadow snapshot stage — replays journal events on a cloned App to
//! produce periodic snapshots without blocking the hot path.
//!
//! The shadow consumer is gated on the journal stage (sees only fsynced events),
//! so snapshots are always consistent with durable state. The chain hash is
//! read from a SeqLock published by the journal stage after each fsync batch.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tracing::{error, info};

use crate::App;
use crate::InputSlot;
use crate::JournalEvent;
use melin_app::{Application, ApplyCtx};
use melin_disruptor::ring;
use melin_disruptor::seqlock::SeqLock;
use melin_journal::JournalEvent as RawJournalEvent;
use melin_transport_core::snapshot;

type Report = <App as Application>::Report;

use melin_app::amortized_timer::AmortizedTimer;

/// Maximum events consumed per batch. Matches the journal stage batch size
/// for consistent throughput characteristics.
const SHADOW_BATCH_SIZE: usize = 4096;

/// Spin-wait idle hint — same pattern as other pipeline stages.
#[inline(always)]
fn idle_wait(idle_spins: &mut u32, busy_spin: bool) {
    if busy_spin || *idle_spins < 1000 {
        *idle_spins = idle_spins.wrapping_add(1);
        std::hint::spin_loop();
    } else {
        std::thread::yield_now();
    }
}

/// Run the shadow snapshot stage.
///
/// Consumes events from the input ring (gated on journal fsync), replays them
/// on a cloned App, and saves periodic snapshots with the BLAKE3 chain
/// hash read from the journal stage's SeqLock.
pub fn run(
    mut consumer: ring::Consumer<InputSlot>,
    mut exchange: App,
    snapshot_path: PathBuf,
    snapshot_interval: Duration,
    chain_hash_lock: Arc<SeqLock<[u8; 32]>>,
    shutdown: &AtomicBool,
    busy_spin: bool,
) {
    // Scratch buffer for app methods that require a reports Vec.
    // Cleared after each call — shadow discards all reports.
    let mut reports: Vec<Report> = Vec::with_capacity(64);

    // Batch buffer for consume_batch — stack-allocated InputSlot array would
    // be too large, so use a Vec that's allocated once and reused.
    let mut batch: Vec<InputSlot> = Vec::with_capacity(SHADOW_BATCH_SIZE);
    batch.resize_with(SHADOW_BATCH_SIZE, InputSlot::default);

    // Snapshot-interval check on the busy-spin hot loop. A naive
    // `last_snapshot.elapsed() >= snapshot_interval` per iteration ran
    // `__vdso_clock_gettime` at loop frequency, which showed up in
    // perf profiles as ~10 % of this process's total cycles landing on
    // `clock_gettime` — for a check that fires at most once every
    // 50 min (default `snapshot_interval_ms=3_000_000`). `AmortizedTimer`
    // defers the clock read to roughly 1 Hz, collapsing the overhead
    // to a single `AND` + predictable branch per iteration.
    let mut snapshot_timer = AmortizedTimer::new();
    let mut idle_spins: u32 = 0;
    // Track whether any events have been consumed. Prevents snapshotting
    // empty state before the first event arrives.
    let mut has_events = false;
    // Highest event timestamp the shadow's scheduler has drained against.
    // See `dispatch_event` for the per-event drain rationale.
    let mut last_drain_ns: u64 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("shadow stage shutting down");
            return;
        }

        let count = consumer.consume_batch(&mut batch, SHADOW_BATCH_SIZE);
        if count == 0 {
            // Check snapshot timer even when idle — events may have been
            // consumed before the interval elapsed, and no more events
            // will arrive to trigger the post-consume check.
            if has_events
                && snapshot_timer
                    .tick(snapshot_interval, busy_spin || idle_spins < 1000)
                    .is_some()
            {
                let last_seq = consumer.next_read().saturating_sub(1);
                save_snapshot(&exchange, last_seq, &chain_hash_lock, &snapshot_path);
            }
            idle_wait(&mut idle_spins, busy_spin);
            continue;
        }
        idle_spins = 0;
        has_events = true;

        // Replay each event on the shadow exchange. last_drain_ns lives
        // outside the loop so the per-event drain stays monotonic across
        // batches.
        for slot in &batch[..count] {
            dispatch_event(
                &mut exchange,
                &slot.event,
                slot.timestamp_ns,
                slot.key_hash,
                slot.request_seq,
                &mut last_drain_ns,
                &mut reports,
            );
        }

        // Check if a snapshot is due.
        if snapshot_timer.tick(snapshot_interval, true).is_some() {
            let last_seq = consumer.next_read() - 1;
            save_snapshot(&exchange, last_seq, &chain_hash_lock, &snapshot_path);
        }
    }
}

/// Save a shadow snapshot, logging success or failure.
fn save_snapshot(
    app: &App,
    sequence: u64,
    chain_hash_lock: &Arc<SeqLock<[u8; 32]>>,
    path: &std::path::Path,
) {
    let chain_hash = chain_hash_lock.load();
    match snapshot::save::<App>(app, sequence, chain_hash, path) {
        Ok(()) => {
            info!(
                sequence,
                path = %path.display(),
                "shadow snapshot saved"
            );
        }
        Err(e) => {
            error!(
                sequence,
                error = %e,
                path = %path.display(),
                "shadow snapshot failed"
            );
        }
    }
}

/// Dispatch a single journal event to the shadow app.
///
/// Mirrors `JournaledApp::replay_entry`: rebuild per-key HWM via
/// `check_request_seq`, drain the scheduler clock if `timestamp_ns`
/// advanced, then hand the event to `apply` or `tick`. Without the
/// `check_request_seq` call, the shadow snapshot's `key_hwm` would be
/// empty and a restore would let previously-rejected duplicate
/// `request_seq` values through. `last_drain_ns` is caller-tracked
/// across the consume loop so the drain stays monotonic.
fn dispatch_event(
    app: &mut App,
    event: &JournalEvent,
    timestamp_ns: u64,
    key_hash: u64,
    request_seq: u64,
    last_drain_ns: &mut u64,
    reports: &mut Vec<Report>,
) {
    reports.clear();

    // Gate on `!is_query` to match the matching stage (`pipeline.rs`
    // `check_request_seq` call site). The shadow reads from the pre-journal
    // input ring — unlike `JournaledApp::replay_entry`, which sees only
    // non-queries because the journal stage drops queries — so advancing
    // HWM on queries here would push shadow's `key_hwm` above primary's and
    // cause post-restore to reject legitimate non-duplicate requests.
    // Return discarded: shadow applies the event regardless of the dedup
    // decision (matches `replay_entry` for non-queries).
    if !event.is_query() {
        let _ = app.check_request_seq(key_hash, request_seq);
    }

    if timestamp_ns > *last_drain_ns {
        *last_drain_ns = timestamp_ns;
        app.tick(timestamp_ns, reports);
    }

    match *event {
        RawJournalEvent::App(e) => {
            // The shadow is strictly a secondary observer — the canonical
            // answer (and journal sequence number) is produced by the
            // matching stage. `ApplyCtx` is supplied with the fields the
            // shadow can cheaply compute; `journal_sequence` / connection
            // counts are live-pipeline-only. `key_hash` is threaded so
            // that any self-introspecting query the app supports stays
            // consistent between live and shadow paths.
            let ctx = ApplyCtx {
                now_ns: timestamp_ns,
                journal_sequence: 0,
                active_connections: 0,
                events_processed: 0,
                key_hash,
            };
            // Query response discarded — shadow is a secondary observer,
            // it does not produce client-facing output.
            let _ = app.apply(e, &ctx, reports);
        }
        RawJournalEvent::Tick { now_ns } => {
            // Defensive: the head-of-event drain typically already advanced
            // the clock to this point. Re-draining via `now_ns` keeps the
            // contract consistent for callers that pass `timestamp_ns = 0`
            // (tests, manually constructed events).
            app.tick(now_ns, reports);
        }
        RawJournalEvent::GenesisHash { .. } | RawJournalEvent::Checkpoint { .. } => {
            // Hash chain metadata — no application state change.
        }
        RawJournalEvent::Shutdown => {
            // Pipeline-only sentinel — handled at the run-loop level by
            // exiting; should never reach this dispatch.
        }
    }
}

// The shadow module's test suite exercises every trading-event branch
// against a real `Exchange`. Under `skip-order-exec` the equivalent
// assertions would be trivial (every order produces the same
// `NoLiquidity` rejection), so the suite is gated to the trading
// build rather than rewritten.
#[cfg(all(test, feature = "trading", not(feature = "skip-order-exec")))]
mod tests {
    use super::*;
    use crate::JournalEvent;
    use melin_types::types::*;
    use std::num::NonZeroU64;
    use std::time::Instant;

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

    #[test]
    fn shadow_shutdown_exits_promptly() {
        let (_, mut consumers) = melin_disruptor::ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let exchange = App::new();
        let chain_hash = Arc::new(SeqLock::new([0u8; 32]));
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");

        let handle = std::thread::Builder::new()
            .name("test-shadow".into())
            .spawn(move || {
                run(
                    consumer,
                    exchange,
                    snap_path,
                    Duration::from_secs(3600), // won't fire during test
                    chain_hash,
                    &shutdown2,
                    false,
                );
            })
            .unwrap();

        // Give it a moment to start, then signal shutdown.
        std::thread::sleep(Duration::from_millis(50));
        shutdown.store(true, Ordering::Relaxed);

        // Should exit promptly.
        handle.join().unwrap();
    }

    #[test]
    fn shadow_takes_snapshot_at_interval() {
        let (mut producer, mut consumers) =
            melin_disruptor::ring::DisruptorBuilder::<InputSlot>::new(64)
                .add_consumer()
                .build();
        let consumer = consumers.pop().unwrap();

        let mut exchange = App::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(0),
            quote: CurrencyId(1),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), 100_000);

        let chain_hash = Arc::new(SeqLock::new([0xAB; 32]));
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown2 = Arc::clone(&shutdown);

        let dir = tempfile::tempdir().unwrap();
        let snap_path = dir.path().join("test.snapshot");
        let snap_path2 = snap_path.clone();

        // Very short interval so the snapshot fires quickly.
        let handle = std::thread::Builder::new()
            .name("test-shadow".into())
            .spawn(move || {
                run(
                    consumer,
                    exchange,
                    snap_path2,
                    Duration::from_millis(50),
                    chain_hash,
                    &shutdown2,
                    false,
                );
            })
            .unwrap();

        // Publish both events before the interval elapses so the snapshot
        // captures both deposits. The idle-check fires the snapshot after
        // the 50ms interval even without new events arriving.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(melin_trading::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 1000,
            }),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            event: JournalEvent::App(melin_trading::trading_event::TradingEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 500,
            }),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        // Wait for the snapshot to be written (idle-check triggers it
        // after the 50ms interval elapses). Generous deadline because
        // nextest runs many tests concurrently and the shadow worker can
        // be starved on a busy machine — the test still completes
        // quickly in the common case via the tight poll.
        let deadline = Instant::now() + Duration::from_secs(10);
        while !snap_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();

        // Verify the snapshot file was created and is loadable.
        assert!(snap_path.exists(), "snapshot file should exist");
        let (restored, _seq, chain) = snapshot::load::<App>(&snap_path).unwrap();
        assert_eq!(chain, [0xAB; 32]); // chain hash from SeqLock
        // Both deposits should be reflected: 100K initial + 1K + 500.
        assert_eq!(
            restored
                .accounts()
                .balance(AccountId(1), CurrencyId(1))
                .available,
            101_500
        );
    }
}
