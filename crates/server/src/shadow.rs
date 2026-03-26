//! Shadow snapshot stage — replays journal events on a cloned Exchange to
//! produce periodic snapshots without blocking the hot path.
//!
//! The shadow consumer is gated on the journal stage (sees only fsynced events),
//! so snapshots are always consistent with durable state. The chain hash is
//! read from a SeqLock published by the journal stage after each fsync batch.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tracing::{error, info};

use melin_disruptor::ring;
use melin_disruptor::seqlock::SeqLock;
use melin_engine::exchange::Exchange;
use melin_engine::journal::event::JournalEvent;
use melin_engine::journal::pipeline::InputSlot;
use melin_engine::journal::snapshot;
use melin_engine::types::ExecutionReport;

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
/// on a cloned Exchange, and saves periodic snapshots with the BLAKE3 chain
/// hash read from the journal stage's SeqLock.
pub fn run(
    mut consumer: ring::Consumer<InputSlot>,
    mut exchange: Exchange,
    snapshot_path: PathBuf,
    snapshot_interval: Duration,
    chain_hash_lock: Arc<SeqLock<[u8; 32]>>,
    shutdown: &AtomicBool,
    busy_spin: bool,
) {
    // Scratch buffer for exchange methods that require a reports Vec.
    // Cleared after each call — shadow discards all execution reports.
    let mut reports: Vec<ExecutionReport> = Vec::with_capacity(64);

    // Batch buffer for consume_batch — stack-allocated InputSlot array would
    // be too large, so use a Vec that's allocated once and reused.
    let mut batch: Vec<InputSlot> = Vec::with_capacity(SHADOW_BATCH_SIZE);
    batch.resize_with(SHADOW_BATCH_SIZE, InputSlot::default);

    let mut last_snapshot = Instant::now();
    let mut last_seq: u64 = 0;
    // Suppress "value never read" — last_seq is set in every batch iteration
    // before the snapshot check, but we need a valid initial value for the
    // edge case where shutdown fires before the first batch.
    let _ = &last_seq;
    let mut idle_spins: u32 = 0;

    loop {
        if shutdown.load(Ordering::Relaxed) {
            info!("shadow stage shutting down");
            return;
        }

        let count = consumer.consume_batch(&mut batch, SHADOW_BATCH_SIZE);
        if count == 0 {
            idle_wait(&mut idle_spins, busy_spin);
            continue;
        }
        idle_spins = 0;

        // Replay each event on the shadow exchange.
        for slot in &batch[..count] {
            dispatch_event(&mut exchange, &slot.event, &mut reports);
        }

        // Track the last consumed input sequence.
        // consumer.next_read() is the *next* sequence to read, so the last
        // consumed is next_read - 1.
        last_seq = consumer.next_read() - 1;

        // Check if a snapshot is due. Only runs after processing events —
        // no point snapshotting unchanged state during idle periods.
        if last_snapshot.elapsed() >= snapshot_interval {
            let chain_hash = chain_hash_lock.load();
            match snapshot::save(&exchange, last_seq, chain_hash, &snapshot_path) {
                Ok(()) => {
                    info!(
                        sequence = last_seq,
                        path = %snapshot_path.display(),
                        "shadow snapshot saved"
                    );
                }
                Err(e) => {
                    error!(
                        sequence = last_seq,
                        error = %e,
                        path = %snapshot_path.display(),
                        "shadow snapshot failed"
                    );
                }
            }
            last_snapshot = Instant::now();
        }
    }
}

/// Dispatch a single journal event to the shadow exchange.
///
/// Same event handling as the matching stage's `process_event`, but without
/// output publishing — all execution reports are discarded.
fn dispatch_event(
    exchange: &mut Exchange,
    event: &JournalEvent,
    reports: &mut Vec<ExecutionReport>,
) {
    reports.clear();
    match *event {
        JournalEvent::AddInstrument { spec } => {
            exchange.add_instrument(spec);
        }
        JournalEvent::Deposit {
            account,
            currency,
            amount,
        } => {
            exchange.deposit(account, currency, amount);
        }
        JournalEvent::SubmitOrder { symbol, order } => {
            exchange.execute(symbol, order, reports);
        }
        JournalEvent::CancelOrder {
            account,
            order_id,
            symbol,
        } => {
            exchange.cancel(symbol, account, order_id, reports);
        }
        JournalEvent::SetRiskLimits { symbol, limits } => {
            exchange.set_risk_limits(symbol, limits);
        }
        JournalEvent::CancelAll { account } => {
            exchange.cancel_all(account, reports);
        }
        JournalEvent::EndOfDay => {
            exchange.end_of_day(reports);
        }
        JournalEvent::ExpireOrders { timestamp_ns } => {
            exchange.expire_orders(timestamp_ns, reports);
        }
        JournalEvent::SetCircuitBreaker { symbol, config } => {
            exchange.set_circuit_breaker(symbol, config);
        }
        JournalEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => {
            exchange.cancel_replace(symbol, account, order_id, new_price, new_quantity, reports);
        }
        JournalEvent::SetFeeSchedule { symbol, schedule } => {
            exchange.set_fee_schedule(symbol, schedule);
        }
        JournalEvent::ProvisionAccount { account, amount } => {
            exchange.provision_account(account, amount);
        }
        JournalEvent::Withdraw {
            account,
            currency,
            amount,
        } => {
            // Best-effort — shadow doesn't propagate withdrawal errors.
            let _ = exchange.withdraw(account, currency, amount);
        }
        JournalEvent::QueryStats => {
            // Read-only — no state change.
        }
        JournalEvent::GenesisHash { .. } | JournalEvent::Checkpoint { .. } => {
            // Hash chain metadata — no exchange state change.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_engine::journal::event::JournalEvent;
    use melin_engine::types::*;
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
        // and direct Exchange method calls (matching path). Snapshot both
        // and verify identical state.
        let mut shadow = Exchange::new();
        let mut primary = Exchange::new();
        let mut reports = Vec::new();

        let events = vec![
            JournalEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(0),
                    quote: CurrencyId(1),
                },
            },
            JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100_000,
            },
            JournalEvent::Deposit {
                account: AccountId(2),
                currency: CurrencyId(0),
                amount: 500,
            },
            JournalEvent::SubmitOrder {
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
            },
            JournalEvent::SubmitOrder {
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
            },
        ];

        // Shadow path: dispatch_event.
        for event in &events {
            dispatch_event(&mut shadow, event, &mut reports);
        }

        // Primary path: direct method calls.
        let mut primary_reports = Vec::new();
        for event in &events {
            match *event {
                JournalEvent::AddInstrument { spec } => primary.add_instrument(spec),
                JournalEvent::Deposit {
                    account,
                    currency,
                    amount,
                } => primary.deposit(account, currency, amount),
                JournalEvent::SubmitOrder { symbol, order } => {
                    primary.execute(symbol, order, &mut primary_reports);
                    primary_reports.clear();
                }
                _ => {}
            }
        }

        // Both exchanges should have identical balances.
        let acct1_shadow = shadow.accounts().balance(AccountId(1), CurrencyId(1));
        let acct1_primary = primary.accounts().balance(AccountId(1), CurrencyId(1));
        assert_eq!(acct1_shadow.available, acct1_primary.available);
        assert_eq!(acct1_shadow.reserved, acct1_primary.reserved);

        let acct2_shadow = shadow.accounts().balance(AccountId(2), CurrencyId(0));
        let acct2_primary = primary.accounts().balance(AccountId(2), CurrencyId(0));
        assert_eq!(acct2_shadow.available, acct2_primary.available);
        assert_eq!(acct2_shadow.reserved, acct2_primary.reserved);
    }

    #[test]
    fn shadow_shutdown_exits_promptly() {
        let (_, mut consumers) = melin_disruptor::ring::DisruptorBuilder::<InputSlot>::new(64)
            .add_consumer()
            .build();
        let consumer = consumers.pop().unwrap();

        let exchange = Exchange::new();
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

        let mut exchange = Exchange::new();
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

        // Publish an initial event immediately — processed before the
        // interval elapses, so no snapshot yet.
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            event: JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 1000,
            },
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        // Wait for the interval to elapse, then publish another event
        // to trigger the snapshot check.
        std::thread::sleep(Duration::from_millis(100));
        producer.publish(InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            event: JournalEvent::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 500,
            },
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        });

        // Wait for the snapshot to be written.
        let deadline = Instant::now() + Duration::from_secs(2);
        while !snap_path.exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }

        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();

        // Verify the snapshot file was created and is loadable.
        assert!(snap_path.exists(), "snapshot file should exist");
        let (restored, _seq, chain) = snapshot::load(&snap_path).unwrap();
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
