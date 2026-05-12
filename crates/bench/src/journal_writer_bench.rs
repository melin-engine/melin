//! Minimal benchmark for the journal writer stage.
//!
//! This benchmark tests the journal writing and syncing operations directly,
//! without any pipeline or matching engine overhead.
//!
//! Usage:
//!     cargo run --release -p melin-bench --bin journal_writer_bench -- [OPTIONS]
//!
//! Options:
//!     --mode <primary|replica>   Write path to benchmark (default: primary)
//!     --events <N>               Events to write (default: 1_000_000)
//!     --batch-size <N>           Events per fsync batch (default: 1_024)
//!     --warmup <N>               Warmup events, not measured (default: 100_000)

use clap::Parser;
use io_uring::IoUring;
use std::num::NonZero;
use std::time::Instant;

use melin_engine::journal::JournalEvent;
use melin_engine::journal::JournalWrite;
use melin_engine::journal::SectorWriter;
use melin_trading::trading_event::TradingEvent;

#[derive(Parser)]
struct Args {
    /// Write path to benchmark: "primary" (flush_batch_sync) or "replica" (io_uring).
    #[arg(long, default_value = "primary")]
    mode: String,

    /// Total events to write.
    #[arg(long, default_value_t = 1_000_000)]
    events: usize,

    /// Events per fsync batch.
    #[arg(long, default_value_t = 1_024)]
    batch_size: usize,

    /// Warmup events (not included in measurements).
    #[arg(long, default_value_t = 100_000)]
    warmup: usize,
}

fn main() {
    let args = Args::parse();

    println!("=== Journal Writer Benchmark ===");
    println!("Mode: {}", args.mode);
    println!("Events: {}", args.events);
    println!("Batch size: {}", args.batch_size);
    println!("Warmup: {}", args.warmup);
    println!();

    if args.mode == "replica" {
        run_replica_mode(args.events, args.batch_size, args.warmup);
    } else {
        run_journal_writer_bench(args.events, args.batch_size, args.warmup);
    }
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
    let mut writer = SectorWriter::create(&journal_path).expect("create journal");

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

fn run_replica_mode(num_events: usize, batch_size: usize, _warmup: usize) {
    let nz = |v: u64| NonZero::new(v).expect("non-zero");

    // Set up minimal exchange state.
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

    // Use a fixed journal file path.
    let journal_path = std::path::PathBuf::from("/tmp/journal_writer_bench_replica.journal");
    let _ = std::fs::remove_file(&journal_path);
    let mut writer = SectorWriter::create(&journal_path).expect("create journal");

    // Set up io_uring for async writes.
    let mut io_uring = IoUring::new(256).expect("create io_uring ring");
    let rw_flags = writer.io_uring_rw_flags();

    println!("Measurement phase...");
    let start = Instant::now();

    let mut events_written = 0;
    let num_batches = num_events.div_ceil(batch_size);
    let mut inflight_count: usize = 0;
    let inflight_limit = 32;

    for batch_idx in 0..num_batches {
        let batch_start = batch_idx * batch_size;
        let batch_end = std::cmp::min(batch_start + batch_size, num_events);

        // Encode batch into writer's buffer.
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
        }

        // Take batch for async write (replica path).
        match writer.take_batch_for_async_write() {
            Ok(Some(async_batch)) => {
                let sqe = io_uring::opcode::Write::new(
                    io_uring::types::Fd(writer.fd()),
                    async_batch.buf.as_ptr(),
                    async_batch.len as u32,
                )
                .offset(async_batch.offset)
                .rw_flags(rw_flags)
                .build()
                .user_data(1);

                unsafe {
                    io_uring.submission().push(&sqe).expect("SQ full");
                }
                inflight_count += 1;
            }
            Ok(None) => {
                // Empty batch (shouldn't happen with data), just commit.
                return;
            }
            Err(e) => {
                panic!("take_batch_for_async_write failed: {:?}", e);
            }
        }

        // Wait for inflight limit if needed.
        if inflight_count >= inflight_limit {
            // Wait for one completion.
            while let Some(cqe) = io_uring.completion().next() {
                if cqe.result() < 0 {
                    panic!("io_uring write failed: {}", -cqe.result());
                }
                inflight_count -= 1;
                if inflight_count == 0 {
                    break;
                }
            }
        }

        // Submit pending SQEs.
        if inflight_count > 0 {
            io_uring.submit().expect("io_uring submit");
        }

        // Wait for completions.
        while inflight_count > 0 {
            while let Some(cqe) = io_uring.completion().next() {
                if cqe.result() < 0 {
                    panic!("io_uring write failed: {}", -cqe.result());
                }
                inflight_count -= 1;
            }
        }

        if events_written % 10000 == 0 {
            println!("  Written {} events", events_written);
        }
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
