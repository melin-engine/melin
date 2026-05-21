//! Microbench: false sharing on `InputSlot`-sized disruptor slots.
//!
//! Two slot variants share the harness:
//!   - `Slot104`: 104 bytes — exactly the size of the production
//!     `InputSlot<TradingEvent>`. Each slot straddles two 64-byte cache
//!     lines and adjacent slots share lines, so producer writes to slot
//!     N invalidate the line a consumer needs to finish reading slot N±1.
//!   - `Slot128`: same fields padded to 128 bytes and 64-byte aligned.
//!     Each slot occupies exactly two cache lines; adjacent slots never
//!     share a line.
//!
//! Run with the producer and consumer pinned to two different physical
//! cores (ideally on the same CCX/CCD to keep L3 effects out of the
//! signal):
//!
//!     cargo run --release --example false_sharing -- 4 6 10 65536
//!                                                    │ │ │  └─ ring capacity (pow2)
//!                                                    │ │ └──── duration (seconds)
//!                                                    │ └────── consumer core
//!                                                    └──────── producer core
//!
//! Pair with `perf c2c record -F 4000 -- <bench>` then `perf c2c report`
//! to confirm HITM events drop to ~zero after padding.

use melin_disruptor::ring::DisruptorBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

// 104-byte slot — mirrors the production `InputSlot<TradingEvent>` layout
// (5×u64 metadata header + 64-byte event payload). `#[repr(C)]` so the
// compiler does not reorder fields.
#[derive(Clone, Copy)]
#[repr(C)]
struct Slot104 {
    connection_id: u64,
    key_hash: u64,
    request_seq: u64,
    sequence: u64,
    timestamp_ns: u64,
    payload: [u8; 64],
}

impl Default for Slot104 {
    fn default() -> Self {
        Self {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            payload: [0; 64],
        }
    }
}

const _: () = assert!(std::mem::size_of::<Slot104>() == 104);

// 128-byte padded slot. `align(64)` forces the array start on a cache
// line boundary; the trailing 24-byte pad rounds each slot up to two
// full lines.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Slot128 {
    connection_id: u64,
    key_hash: u64,
    request_seq: u64,
    sequence: u64,
    timestamp_ns: u64,
    payload: [u8; 64],
    _pad: [u8; 24],
}

impl Default for Slot128 {
    fn default() -> Self {
        Self {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: 0,
            timestamp_ns: 0,
            payload: [0; 64],
            _pad: [0; 24],
        }
    }
}

const _: () = assert!(std::mem::size_of::<Slot128>() == 128);
const _: () = assert!(std::mem::align_of::<Slot128>() == 64);

fn pin(core: Option<usize>) {
    let Some(core) = core else {
        return;
    };
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core, &mut set);
        let rc = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);
        if rc != 0 {
            eprintln!(
                "warning: sched_setaffinity({core}) failed: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

trait BenchSlot: Copy + Default + Send + 'static {
    fn fill(&mut self, seq: u64);
}

impl BenchSlot for Slot104 {
    #[inline(always)]
    fn fill(&mut self, seq: u64) {
        self.sequence = seq;
    }
}

impl BenchSlot for Slot128 {
    #[inline(always)]
    fn fill(&mut self, seq: u64) {
        self.sequence = seq;
    }
}

fn run<T: BenchSlot>(
    name: &str,
    capacity: usize,
    duration: Duration,
    prod_core: Option<usize>,
    cons_core: Option<usize>,
) {
    let (mut producer, mut consumers) = DisruptorBuilder::<T>::new(capacity).add_consumer().build();
    let mut consumer = consumers.pop().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);
    let consumed = Arc::new(AtomicU64::new(0));
    let consumed_c = Arc::clone(&consumed);

    let handle = std::thread::Builder::new()
        .name(format!("cons-{name}"))
        .spawn(move || {
            pin(cons_core);
            // Local batch buffer — `consume_batch` copies into this. Keeping
            // it at 64 matches the producer's batch size.
            let mut buf = vec![T::default(); 64];
            let mut local: u64 = 0;
            while !stop_c.load(Ordering::Relaxed) {
                let n = consumer.consume_batch(&mut buf, 64);
                if n == 0 {
                    std::hint::spin_loop();
                } else {
                    local += n as u64;
                }
            }
            // Drain anything the producer left behind so backpressure
            // isn't the bottleneck on the last iteration.
            loop {
                let n = consumer.consume_batch(&mut buf, 64);
                if n == 0 {
                    break;
                }
                local += n as u64;
            }
            consumed_c.store(local, Ordering::Relaxed);
        })
        .unwrap();

    // Give the consumer a moment to pin and start polling.
    std::thread::sleep(Duration::from_millis(50));

    pin(prod_core);
    let start = Instant::now();
    let mut produced: u64 = 0;
    while start.elapsed() < duration {
        // Batch of 64 with `push_with` (spinning) so we don't add allocation
        // or branchy fallback paths to the measurement.
        let mut batch = producer.batch();
        for _ in 0..64 {
            batch.push_with(|s: &mut T| s.fill(produced));
            produced += 1;
        }
        batch.commit();
    }
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Relaxed);
    handle.join().unwrap();

    let ns_per = elapsed.as_nanos() as f64 / produced as f64;
    let mops = produced as f64 / elapsed.as_secs_f64() / 1e6;
    println!(
        "{name:>8}  size={:>3}B  align={:>2}B  produced={:>12}  consumed={:>12}  {:>7.2} Mops/s  {:>6.2} ns/op",
        std::mem::size_of::<T>(),
        std::mem::align_of::<T>(),
        produced,
        consumed.load(Ordering::Relaxed),
        mops,
        ns_per
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let prod_core: Option<usize> = args.get(1).and_then(|s| s.parse().ok());
    let cons_core: Option<usize> = args.get(2).and_then(|s| s.parse().ok());
    let dur_s: u64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
    // Power-of-two capacity. 65536 slots × 128 B = 8 MiB; fits comfortably
    // in L3 on any modern x86 — keeps the test on the false-sharing axis
    // rather than memory bandwidth.
    let capacity: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1 << 16);

    println!(
        "config: prod_core={:?} cons_core={:?} duration={dur_s}s capacity={capacity}",
        prod_core, cons_core
    );
    println!("note: pin to two distinct cores to surface the false-sharing signal\n");

    // Quick warmup to settle CPU frequency / branch predictor before the
    // real measurement.
    run::<Slot104>(
        "warmup",
        capacity,
        Duration::from_secs(2),
        prod_core,
        cons_core,
    );
    println!("--");

    let dur = Duration::from_secs(dur_s);
    run::<Slot104>("Slot104", capacity, dur, prod_core, cons_core);
    run::<Slot128>("Slot128", capacity, dur, prod_core, cons_core);
}
