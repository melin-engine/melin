//! Microbenchmark for `JournalReader::next_entry` scan throughput.
//!
//! Generates a synthetic journal with N entries (default 1,000,000), then
//! times a full sequential scan via `JournalReader`. Prints elapsed time
//! and entries/second. Used to gate perf changes to the reader.
//!
//! Usage:
//!   cargo run --release -p melin-journal --example scan_bench -- [N] [ITERS]
//!
//! The first iteration warms the page cache; subsequent iterations report
//! a cache-hot scan, which is the regime journal-verify.sh runs in
//! (sequential read of a freshly-written file on the same host).

use std::path::Path;
use std::time::Instant;

use melin_app::{AppEvent, CodecError};
use melin_journal::buffered_writer::BufferedWriter;
use melin_journal::write::JournalWrite;
use melin_journal::{JournalEvent, JournalReader};

// 40 bytes — sized to approximate a SubmitOrder-shaped TradingEvent so
// the read buffer / decode ratio resembles real-world traffic. u64 array
// because it's the cheapest stable layout for a fixed-width payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BenchEvent {
    fields: [u64; 5],
}

impl AppEvent for BenchEvent {
    fn encoded_size(&self) -> usize {
        40
    }
    fn encode(&self, buf: &mut [u8]) -> usize {
        for (i, v) in self.fields.iter().enumerate() {
            buf[i * 8..(i + 1) * 8].copy_from_slice(&v.to_le_bytes());
        }
        40
    }
    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        if buf.len() < 40 {
            return Err(CodecError::Truncated);
        }
        let mut fields = [0u64; 5];
        for (i, slot) in fields.iter_mut().enumerate() {
            *slot = u64::from_le_bytes(buf[i * 8..(i + 1) * 8].try_into().unwrap());
        }
        Ok(BenchEvent { fields })
    }
    fn is_query(&self) -> bool {
        false
    }
}

fn write_synthetic(path: &Path, n: u64) {
    let mut writer = BufferedWriter::<BenchEvent>::create(path).expect("create journal");
    for i in 0..n {
        let ev = JournalEvent::App(BenchEvent {
            fields: [i, i.wrapping_mul(7), i ^ 0xdead, i.rotate_left(13), i + 1],
        });
        writer.append(&ev).expect("append");
    }
    // `append` flushes each event durably, so the writer holds nothing
    // pending here — the drop just closes the fd.
    drop(writer);
}

fn scan(path: &Path) -> (u64, u64) {
    let mut reader = JournalReader::<BenchEvent>::open(path).expect("open journal");
    let mut count = 0u64;
    let mut last_seq = 0u64;
    while let Some(entry) = reader.next_entry().expect("scan") {
        count += 1;
        last_seq = entry.sequence;
    }
    (count, last_seq)
}

fn main() {
    let n: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);
    let iters: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bench.journal");

    println!("== JournalReader scan bench ==");
    println!("entries: {n}, iters: {iters}");

    let t_write = Instant::now();
    write_synthetic(&path, n);
    let write_ms = t_write.elapsed().as_secs_f64() * 1000.0;
    let file_bytes = std::fs::metadata(&path).expect("stat").len();
    println!(
        "write: {write_ms:.1} ms  ({:.1} MiB on disk)",
        file_bytes as f64 / (1024.0 * 1024.0)
    );

    for i in 0..iters {
        let t = Instant::now();
        let (count, last_seq) = scan(&path);
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let eps = count as f64 / (ms / 1000.0);
        let mibps = (file_bytes as f64 / (1024.0 * 1024.0)) / (ms / 1000.0);
        println!(
            "scan {i}: {ms:8.1} ms  ({eps:>12.0} entries/s, {mibps:6.1} MiB/s)  count={count} last_seq={last_seq}"
        );
    }
}
