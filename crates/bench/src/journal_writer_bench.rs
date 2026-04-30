//! Minimal benchmark for the journal writer stage.
//!
//! This benchmark tests the journal writing and syncing operations directly,
//! without any pipeline or matching engine overhead.
//!
//! Usage:
//!     cargo run --release -p melin-bench --bin journal_writer_bench

use std::num::NonZero;
use std::time::Instant;

use melin_engine::journal::JournalEvent;
use melin_engine::journal::JournalWriter;
use melin_trading::trading_event::TradingEvent;

/// Number of events to write per benchmark run.
const DEFAULT_EVENTS: usize = 1_000_000;

/// Maximum events per journal fsync batch.
const DEFAULT_BATCH: usize = 1_024;

/// Warmup events (not measured).
const WARMUP_EVENTS: usize = 100_000;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_events = args
        .get(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(DEFAULT_EVENTS);
    let batch_size = args
        .get(2)
        .and_then(|a| a.parse().ok())
        .unwrap_or(DEFAULT_BATCH);
    let warmup = args
        .get(3)
        .and_then(|a| a.parse().ok())
        .unwrap_or(WARMUP_EVENTS);

    println!("=== Journal Writer Benchmark ===");
    println!("Events: {}", num_events);
    println!("Batch size: {}", batch_size);
    println!("Warmup: {}", warmup);
    println!();

    run_journal_writer_bench(num_events, batch_size, warmup);
}

fn run_journal_writer_bench(num_events: usize, batch_size: usize, _warmup: usize) {
    let nz = |v: u64| NonZero::new(v).expect("non-zero");

    // Set up minimal exchange state (needed for journal events).
    let mut exchange = melin_engine::exchange::Exchange::with_capacity();
    exchange.add_instrument(melin_trading::InstrumentSpec {
        symbol: melin_trading::Symbol(1),
        base: melin_trading::CurrencyId(1),
        quote: melin_trading::CurrencyId(2),
    });
    exchange.deposit(
        melin_trading::AccountId(1),
        melin_trading::CurrencyId(1),
        u64::MAX / 2,
    );
    exchange.deposit(
        melin_trading::AccountId(1),
        melin_trading::CurrencyId(2),
        u64::MAX / 2,
    );

    // Use a fixed journal file path for benchmarking.
    let journal_path = std::path::PathBuf::from("/tmp/journal_writer_bench.journal");
    // Remove existing file if it exists.
    let _ = std::fs::remove_file(&journal_path);
    let mut writer = JournalWriter::create(&journal_path).expect("create journal");

    // Measurement phase.
    println!("Measurement phase...");
    let start = Instant::now();

    // Write in batches.
    let mut events_written = 0;
    let num_batches = num_events.div_ceil(batch_size);
    for batch_idx in 0..num_batches {
        let batch_start = batch_idx * batch_size;
        let batch_end = std::cmp::min(batch_start + batch_size, num_events);
        for i in batch_start..batch_end {
            let order_id = melin_trading::OrderId((i as u64) + 1);
            let side = if i % 2 == 0 {
                melin_trading::Side::Buy
            } else {
                melin_trading::Side::Sell
            };

            let event = JournalEvent::App(TradingEvent::SubmitOrder {
                symbol: melin_trading::Symbol(1),
                order: melin_trading::Order {
                    id: order_id,
                    account: melin_trading::AccountId(1),
                    side,
                    order_type: melin_trading::OrderType::Limit {
                        price: melin_trading::Price(nz(100)),
                        post_only: false,
                    },
                    time_in_force: melin_trading::TimeInForce::GTC,
                    quantity: melin_trading::Quantity(nz(1)),
                    stp: melin_trading::SelfTradeProtection::Allow,
                    expiry_ns: 0,
                },
            });

            writer
                .batch_append_with_ts(&event, 0, 0, 0)
                .expect("batch_append_with_ts");
            events_written += 1;
            if events_written % 10000 == 0 {
                println!("  Written {} events", events_written);
            }
        }

        // Flush each batch to measure fsync overhead.
        writer.flush_batch_sync().expect("sync");
    }

    let elapsed_us = start.elapsed().as_micros();
    let throughput = (num_events as f64 * 1_000_000.0) / elapsed_us as f64;

    println!("  Events: {}", num_events);
    println!("  Time: {} us", elapsed_us);
    println!("  Throughput: {:.2} events/sec", throughput);
    println!(
        "  Latency: {:.2} us/event",
        elapsed_us as f64 / num_events as f64
    );
    println!();

    // Verify file size.
    if let Ok(metadata) = std::fs::metadata(&journal_path) {
        let size_bytes = metadata.len();
        println!(
            "  Journal file size: {} bytes ({:.2} MB)",
            size_bytes,
            size_bytes as f64 / 1_048_576.0
        );
    }
}

// Use jemalloc for better performance.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
