//! Minimal benchmark for the journal writer stage.
//!
//! Tests journal writing and syncing directly, without any pipeline or
//! matching engine overhead — useful to isolate journal-stage cost when
//! diagnosing roadmap item #1 (1 Hz / ~500 ms p99.99 under O_DIRECT).
//!
//! Two `--mode`s, named for the *syscall path* under measurement rather
//! than the production role that exercises it:
//!
//! * `sync` — drives `flush_batch_sync`. In production this is the
//!   primary's write path (both writer types) and the replica's write
//!   path when `--journal-writer=buffered`. Works with either writer.
//! * `iouring` — drives io_uring async submit + CQE poll directly
//!   against the writer's fd. In production this is the replica's write
//!   path when `--journal-writer=sector`. Sector-only: `BufferedWriter`
//!   has no fd-driven async path.
//!
//! Usage:
//!     cargo run --release -p melin-bench --bin journal_writer_bench -- [OPTIONS]
//!
//! Options:
//!     --mode <sync|iouring>      Syscall path to benchmark (default: sync)
//!     --writer <sector|buffered> Writer implementation (default: sector)
//!     --events <N>               Events to write (default: 1_000_000)
//!     --batch-size <N>           Events per fsync batch (default: 1_024)
//!     --warmup <N>               Warmup events, not measured (default: 100_000)

use clap::Parser;
use io_uring::IoUring;
use std::num::NonZero;
use std::path::Path;
use std::time::Instant;

use melin_journal::BufferedWriter;
use melin_journal::JournalEvent;
use melin_journal::SectorWriter;
use melin_server::JournalWrite;
use melin_trading::trading_event::TradingEvent;

#[derive(Parser)]
struct Args {
    /// Syscall path to benchmark.
    #[arg(long, default_value = "sync")]
    mode: String,

    /// Writer implementation.
    #[arg(long, default_value = "sector")]
    writer: String,

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
    println!("Writer: {}", args.writer);
    println!("Events: {}", args.events);
    println!("Batch size: {}", args.batch_size);
    println!("Warmup: {}", args.warmup);
    println!();

    let journal_path = std::path::PathBuf::from("/tmp/journal_writer_bench.journal");
    let _ = std::fs::remove_file(&journal_path);

    match (args.mode.as_str(), args.writer.as_str()) {
        ("sync", "sector") => {
            let writer = SectorWriter::create(&journal_path).expect("create journal");
            run_sync_mode(writer, args.events, args.batch_size, &journal_path);
        }
        ("sync", "buffered") => {
            let writer = BufferedWriter::create(&journal_path).expect("create journal");
            run_sync_mode(writer, args.events, args.batch_size, &journal_path);
        }
        ("iouring", "sector") => {
            let writer = SectorWriter::create(&journal_path).expect("create journal");
            run_iouring_mode(writer, args.events, args.batch_size, &journal_path);
        }
        ("iouring", "buffered") => {
            eprintln!(
                "error: --mode=iouring requires --writer=sector. \
                 BufferedWriter has no fd-driven async path; in production a \
                 buffered replica uses --mode=sync. Either pick \
                 --writer=sector or --mode=sync."
            );
            std::process::exit(2);
        }
        (mode, writer) => {
            eprintln!(
                "error: unknown --mode={mode} --writer={writer}. \
                 Modes: sync, iouring. Writers: sector, buffered."
            );
            std::process::exit(2);
        }
    }
}

/// Build a `SubmitOrder` event for slot `i`. Alternates Buy/Sell so the
/// generated stream is not trivially compressible.
fn make_event(i: usize) -> JournalEvent<TradingEvent> {
    let nz = |v: u64| NonZero::new(v).expect("non-zero");
    let order_id = melin_types::types::OrderId((i as u64) + 1);
    let side = if i.is_multiple_of(2) {
        melin_types::types::Side::Buy
    } else {
        melin_types::types::Side::Sell
    };
    JournalEvent::App(TradingEvent::SubmitOrder {
        symbol: melin_types::types::Symbol(1),
        order: melin_types::types::Order {
            id: order_id,
            account: melin_types::types::AccountId(1),
            side,
            order_type: melin_types::types::OrderType::Limit {
                price: melin_types::types::Price(nz(100)),
                post_only: false,
            },
            time_in_force: melin_types::types::TimeInForce::GTC,
            quantity: melin_types::types::Quantity(nz(1)),
            stp: melin_types::types::SelfTradeProtection::Allow,
            expiry_ns: 0,
        },
    })
}

fn report(num_events: usize, elapsed_us: u128, journal_path: &Path) {
    let throughput = (num_events as f64 * 1_000_000.0) / elapsed_us as f64;
    println!("  Events: {}", num_events);
    println!("  Time: {} us", elapsed_us);
    println!("  Throughput: {:.2} events/sec", throughput);
    println!(
        "  Latency: {:.2} us/event",
        elapsed_us as f64 / num_events as f64
    );
    println!();
    if let Ok(metadata) = std::fs::metadata(journal_path) {
        let size_bytes = metadata.len();
        println!(
            "  Journal file size: {} bytes ({:.2} MB)",
            size_bytes,
            size_bytes as f64 / 1_048_576.0
        );
    }
}

/// `flush_batch_sync` path — encodes a batch into the writer's internal
/// buffer, then issues a single sync. Same path the journal stage runs
/// in `run_sync`.
fn run_sync_mode<W: JournalWrite<TradingEvent>>(
    mut writer: W,
    num_events: usize,
    batch_size: usize,
    journal_path: &Path,
) {
    println!("Measurement phase...");
    let start = Instant::now();

    let num_batches = num_events.div_ceil(batch_size);
    let mut events_written = 0;
    for batch_idx in 0..num_batches {
        let batch_start = batch_idx * batch_size;
        let batch_end = std::cmp::min(batch_start + batch_size, num_events);
        for i in batch_start..batch_end {
            let event = make_event(i);
            writer
                .batch_append_with_ts(&event, 0, 0, 0)
                .expect("batch_append_with_ts");
            events_written += 1;
            if events_written % 10_000 == 0 {
                println!("  Written {} events", events_written);
            }
        }
        writer.flush_batch_sync().expect("sync");
    }

    report(num_events, start.elapsed().as_micros(), journal_path);
}

/// io_uring async submit + CQE poll path. Drives the writer's fd
/// directly the way `run_uring` does in production on a sector replica.
/// Sector-only; gated at the dispatch site in `main`.
fn run_iouring_mode(
    mut writer: SectorWriter<TradingEvent>,
    num_events: usize,
    batch_size: usize,
    journal_path: &Path,
) {
    let mut io_uring = IoUring::new(256).expect("create io_uring ring");
    let rw_flags = writer.io_uring_rw_flags();

    println!("Measurement phase...");
    let start = Instant::now();

    let num_batches = num_events.div_ceil(batch_size);
    let mut events_written = 0;
    let mut inflight_count: usize = 0;
    let inflight_limit = 32;

    for batch_idx in 0..num_batches {
        let batch_start = batch_idx * batch_size;
        let batch_end = std::cmp::min(batch_start + batch_size, num_events);

        for i in batch_start..batch_end {
            let event = make_event(i);
            writer
                .batch_append_with_ts(&event, 0, 0, 0)
                .expect("batch_append_with_ts");
            events_written += 1;
        }

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
            Ok(None) => return,
            Err(e) => panic!("take_batch_for_async_write failed: {:?}", e),
        }

        if inflight_count >= inflight_limit {
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

        if inflight_count > 0 {
            io_uring.submit().expect("io_uring submit");
        }

        while inflight_count > 0 {
            while let Some(cqe) = io_uring.completion().next() {
                if cqe.result() < 0 {
                    panic!("io_uring write failed: {}", -cqe.result());
                }
                inflight_count -= 1;
            }
        }

        if events_written % 10_000 == 0 {
            println!("  Written {} events", events_written);
        }
    }

    report(num_events, start.elapsed().as_micros(), journal_path);
}

// Use jemalloc for better performance.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
