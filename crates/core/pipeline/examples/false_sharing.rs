//! Microbench: false sharing on `InputSlot`-sized disruptor slots.
//!
//! Two slot variants share the harness:
//!   - `Slot104`: 104 bytes — exactly the size of the production
//!     `InputSlot<TradingEvent>` before the cache-line padding change.
//!     Each slot straddles two 64-byte cache lines and adjacent slots
//!     share lines, so producer writes to slot N invalidate the line a
//!     consumer needs to finish reading slot N±1.
//!   - `Slot128`: same field set as `Slot104`, with `#[repr(C, align(64))]`
//!     so the compiler rounds the struct size up to two full cache lines
//!     without an explicit pad field. Mirrors what `#[repr(align(64))]`
//!     does on the production `InputSlot` today — only the stride changes;
//!     the bytes written per slot match `Slot104` exactly, because the
//!     trailing 24 B of implicit padding is *not* touched by `fill`.
//!
//! Each iteration the producer overwrites the full slot (every metadata
//! u64 plus the 64-byte payload) — matching what the production decoder
//! does when it stamps a request into the ring. Writing only one field
//! would under-state the cross-slot line traffic.
//!
//! Samples are interleaved (Slot104, Slot128, Slot104, …) so thermal
//! drift cannot bias either variant. Per-sample Mops/s prints inline;
//! a summary at the end gives median / min / max for each variant.
//!
//! Run with the producer and consumer pinned to two different physical
//! cores (ideally on the same CCX/CCD to keep L3 effects out of the
//! signal):
//!
//!     cargo run --release --example false_sharing -- 4 6 10 65536 5
//!                                                    │ │ │  │     └─ samples per variant
//!                                                    │ │ │  └─────── ring capacity (pow2)
//!                                                    │ │ └────────── per-sample duration (seconds)
//!                                                    │ └──────────── consumer core
//!                                                    └────────────── producer core
//!
//! Pair with `perf c2c record -F 4000 -- <bench>` then `perf c2c report`
//! to confirm HITM events drop to ~zero after padding.

use melin_pipeline::ring::DisruptorBuilder;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

// 104-byte slot — mirrors the pre-padding production `InputSlot<TradingEvent>`
// layout (5×u64 metadata header + 64-byte event payload). `#[repr(C)]`
// so the compiler does not reorder fields.
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

// Same fields as `Slot104`; `#[repr(C, align(64))]` rounds the size up
// to a multiple of 64 (→ 128 B) without an explicit pad field. The 24 B
// of trailing padding is implicit and stays untouched by `fill`, matching
// the way the production `InputSlot` behaves after `#[repr(align(64))]`.
#[derive(Clone, Copy)]
#[repr(C, align(64))]
struct Slot128 {
    connection_id: u64,
    key_hash: u64,
    request_seq: u64,
    sequence: u64,
    timestamp_ns: u64,
    payload: [u8; 64],
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
    /// Overwrite every named field of the slot. Mirrors the production
    /// hot path where the reader populates the entire `InputSlot` per
    /// request — touching the full slot is what generates the cross-slot
    /// line traffic this bench is trying to measure. The `seq`-derived
    /// payload defeats compiler hoisting.
    fn fill(&mut self, seq: u64);
}

impl BenchSlot for Slot104 {
    #[inline(always)]
    fn fill(&mut self, seq: u64) {
        *self = Self {
            connection_id: seq,
            key_hash: seq.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            request_seq: seq,
            sequence: seq,
            timestamp_ns: seq,
            payload: [seq as u8; 64],
        };
    }
}

impl BenchSlot for Slot128 {
    #[inline(always)]
    fn fill(&mut self, seq: u64) {
        *self = Self {
            connection_id: seq,
            key_hash: seq.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            request_seq: seq,
            sequence: seq,
            timestamp_ns: seq,
            payload: [seq as u8; 64],
        };
    }
}

/// One measurement: spin a consumer, pin both ends, run the producer
/// for `duration`, return throughput in Mops/s.
fn run_once<T: BenchSlot>(
    capacity: usize,
    duration: Duration,
    prod_core: Option<usize>,
    cons_core: Option<usize>,
) -> f64 {
    let (mut producer, mut consumers) = DisruptorBuilder::<T>::new(capacity).add_consumer().build();
    let mut consumer = consumers.pop().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_c = Arc::clone(&stop);

    let handle = std::thread::Builder::new()
        .name("cons".into())
        .spawn(move || {
            pin(cons_core);
            // Local batch buffer — `consume_batch` copies into this.
            // 64 matches the producer's batch size.
            let mut buf = vec![T::default(); 64];
            while !stop_c.load(Ordering::Relaxed) {
                if consumer.consume_batch(&mut buf, 64) == 0 {
                    std::hint::spin_loop();
                }
            }
            // Drain anything the producer left behind so backpressure
            // isn't the bottleneck on the last iteration.
            while consumer.consume_batch(&mut buf, 64) != 0 {}
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

    produced as f64 / elapsed.as_secs_f64() / 1e6
}

fn summary(label: &str, samples: &[f64]) {
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = sorted[sorted.len() / 2];
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    println!(
        "{label:>8}  median={median:>7.2} Mops/s  min={min:>7.2}  max={max:>7.2}  n={}",
        samples.len()
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
    let samples: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(5);

    println!(
        "config: prod_core={prod_core:?} cons_core={cons_core:?} \
         duration={dur_s}s/sample capacity={capacity} samples={samples}/variant"
    );
    println!("note: pin to two distinct cores to surface the false-sharing signal\n");

    // Warmup: settle CPU frequency / branch predictor before the first
    // measured sample. Result is discarded.
    let _ = run_once::<Slot104>(capacity, Duration::from_secs(2), prod_core, cons_core);
    println!("warmup done\n");

    let dur = Duration::from_secs(dur_s);
    let mut s104 = Vec::with_capacity(samples);
    let mut s128 = Vec::with_capacity(samples);

    // Interleave so thermal drift can't bias either variant.
    for i in 0..samples {
        let r = run_once::<Slot104>(capacity, dur, prod_core, cons_core);
        println!("[{i:>2}] Slot104  {r:>7.2} Mops/s");
        s104.push(r);

        let r = run_once::<Slot128>(capacity, dur, prod_core, cons_core);
        println!("[{i:>2}] Slot128  {r:>7.2} Mops/s");
        s128.push(r);
    }

    println!();
    summary("Slot104", &s104);
    summary("Slot128", &s128);
}
