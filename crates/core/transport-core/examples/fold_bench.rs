//! Micro-benchmark for the output digest's fold: what digesting one report
//! costs, per window size `K` and per hash, on the thread that folds.
//!
//! Three measurements, each over a stream of one report per position:
//!
//! - the real `WindowFolder` (XXH3-128, `K` = 64): fold, close, chain and
//!   publish to a board, exactly what a node runs;
//! - the same folder closing windows with no report: the per-window cost
//!   alone;
//! - a stand-in fold over XXH3-128 and over BLAKE3 for `K` of 16, 64 and
//!   256, the same bytes per report and the same chain step, to compare
//!   the hashes and the window sizes the folder does not build with.
//!
//! Report bodies are 8 bytes (a counter's reply), 24 (an echo of a short
//! request) and 300 (a wide report).
//!
//! Usage:
//!   cargo run --release -p melin-transport-core --example fold_bench -- [REPORTS]

use std::hint::black_box;
use std::time::Instant;

use melin_app::WireSeq;
use melin_transport_core::output_digest::{
    BoardSize, OUTPUT_DIGEST_WINDOW, ReportBody, WindowFolder, digest_board,
};
use twox_hash::xxhash3_128::{self, RawHasher, SecretBuffer};

/// Report body sizes, in bytes. `usize` because they index a slice.
const BODIES: [usize; 3] = [8, 24, 300];
/// Window sizes compared.
const WINDOWS: [u64; 3] = [16, 64, 256];
/// A board well past what the bench closes between reads: nobody reads it.
const BOARD_SLOTS: usize = 1024;

fn main() {
    let reports: u64 = std::env::args()
        .nth(1)
        .map(|n| n.parse().expect("REPORTS is a number"))
        .unwrap_or(4_000_000);
    println!("{reports} reports per run, ns per report (median of 5 runs)\n");

    println!("WindowFolder, XXH3-128, K = {OUTPUT_DIGEST_WINDOW}");
    for body in BODIES {
        let ns = median(|| folder_run(reports, body));
        println!("  body {body:>3} B: {ns:6.2}");
    }
    let empty = median(|| empty_windows_run(reports));
    println!(
        "  per window, no report: {:6.2} ns",
        empty * OUTPUT_DIGEST_WINDOW as f64
    );

    println!("\nStand-in fold, ns per report");
    println!("  {:>8} {:>5} {:>8} {:>8}", "hash", "K", "body", "ns");
    for k in WINDOWS {
        for body in BODIES {
            let xxh = median(|| stand_in_run::<Xxh3>(reports, body, k));
            let b3 = median(|| stand_in_run::<Blake3>(reports, body, k));
            println!("  {:>8} {k:>5} {body:>8} {xxh:8.2}", "xxh3");
            println!("  {:>8} {k:>5} {body:>8} {b3:8.2}", "blake3");
        }
    }
}

/// The median of five runs of `run`, each returning ns per report.
fn median(run: impl Fn() -> f64) -> f64 {
    let mut samples: Vec<f64> = (0..5).map(|_| run()).collect();
    samples.sort_by(f64::total_cmp);
    samples[2]
}

/// Bodies in the pool a run cycles through.
const POOL: usize = 256;

/// `POOL` bodies of `len` bytes, each different, made before the clock
/// starts: a run hashes varied bytes without paying to write them.
fn body_pool(len: usize) -> Vec<Vec<u8>> {
    (0..POOL)
        .map(|n| (0..len).map(|i| (n as u8).wrapping_add(i as u8)).collect())
        .collect()
}

/// The pool's body for `position`.
fn body_at(pool: &[Vec<u8>], position: u64) -> &[u8] {
    &pool[position as usize % POOL]
}

fn folder_run(reports: u64, body_len: usize) -> f64 {
    let (writer, _reader) = digest_board(BoardSize::new(BOARD_SLOTS).expect("above the minimum"));
    let mut folder = WindowFolder::new(WireSeq::new(1), writer);
    let pool = body_pool(body_len);
    let start = Instant::now();
    for position in 1..=reports {
        let body = body_at(&pool, position);
        folder.fold(WireSeq::new(position), ReportBody::Encoded(body));
    }
    folder.close_through(WireSeq::new(reports));
    black_box(&folder);
    start.elapsed().as_nanos() as f64 / reports as f64
}

fn empty_windows_run(positions: u64) -> f64 {
    let (writer, _reader) = digest_board(BoardSize::new(BOARD_SLOTS).expect("above the minimum"));
    let mut folder = WindowFolder::new(WireSeq::new(1), writer);
    let start = Instant::now();
    folder.close_through(WireSeq::new(positions));
    black_box(&folder);
    start.elapsed().as_nanos() as f64 / positions as f64
}

/// The hash a stand-in fold uses: streaming per window, one-shot for the
/// chain step.
trait WindowHash {
    fn new() -> Self;
    fn write(&mut self, bytes: &[u8]);
    fn finish(&self) -> [u8; 16];
    fn chain(previous: [u8; 16], digest: [u8; 16]) -> [u8; 16];
}

struct Xxh3(RawHasher<&'static [u8; xxhash3_128::DEFAULT_SECRET_LENGTH]>);

impl WindowHash for Xxh3 {
    fn new() -> Self {
        Self(RawHasher::new(SecretBuffer::default()))
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
    fn finish(&self) -> [u8; 16] {
        self.0.finish_128().to_be_bytes()
    }
    fn chain(previous: [u8; 16], digest: [u8; 16]) -> [u8; 16] {
        let mut input = [0u8; 32];
        input[..16].copy_from_slice(&previous);
        input[16..].copy_from_slice(&digest);
        xxhash3_128::Hasher::oneshot(&input).to_be_bytes()
    }
}

struct Blake3(blake3::Hasher);

impl WindowHash for Blake3 {
    fn new() -> Self {
        Self(blake3::Hasher::new())
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
    /// The first 16 bytes of the 32: the width the digest's frames carry.
    fn finish(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out.copy_from_slice(&self.0.finalize().as_bytes()[..16]);
        out
    }
    fn chain(previous: [u8; 16], digest: [u8; 16]) -> [u8; 16] {
        let mut input = [0u8; 32];
        input[..16].copy_from_slice(&previous);
        input[16..].copy_from_slice(&digest);
        let mut out = [0u8; 16];
        out.copy_from_slice(&blake3::hash(&input).as_bytes()[..16]);
        out
    }
}

/// The folder's work at window size `k`, over hash `H`: a header per
/// window, a 12-byte prefix and the body per report, a finalize and a
/// chain step per window. No board store: the real folder's figures carry
/// that.
fn stand_in_run<H: WindowHash>(reports: u64, body_len: usize, k: u64) -> f64 {
    let header = |end: u64| {
        let mut h = H::new();
        h.write(b"melin output digest v1");
        h.write(&[1]);
        h.write(&k.to_le_bytes());
        h.write(&end.to_le_bytes());
        h
    };
    let pool = body_pool(body_len);
    let mut chain = [0u8; 16];
    let mut end = k;
    let mut hasher = header(end);
    let start = Instant::now();
    for position in 1..=reports {
        if position > end {
            chain = H::chain(chain, hasher.finish());
            end += k;
            hasher = header(end);
        }
        let body = body_at(&pool, position);
        let mut prefix = [0u8; 12];
        prefix[..8].copy_from_slice(&position.to_le_bytes());
        prefix[8..].copy_from_slice(&(body_len as u32).to_le_bytes());
        hasher.write(&prefix);
        hasher.write(body);
    }
    chain = H::chain(chain, hasher.finish());
    black_box(chain);
    start.elapsed().as_nanos() as f64 / reports as f64
}
