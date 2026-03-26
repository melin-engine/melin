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

        // Check if a snapshot is due.
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
