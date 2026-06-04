//! Audit a journal lineage: walk every archived segment plus the live
//! segment, verify per-entry CRCs, dense sequences, and cross-segment
//! chain links, and print a per-segment summary with an overall verdict.
//!
//! Usage: cargo run --release -p melin-server --bin journal-verify -- <live-journal-path>
//!
//! Pointing at a single archived segment also works — it is then
//! verified in isolation (no sibling discovery matches its name).
//!
//! Exit code 0 = lineage verified; 1 = verification failed.

use std::path::{Path, PathBuf};

use melin_journal::JournalReader;
use melin_trading::trading_event::TradingEvent;

fn hex(h: [u8; 32]) -> String {
    h.iter().map(|b| format!("{b:02x}")).collect()
}

/// Per-segment summary line. Walks the segment fully so the printed
/// tail hash reflects every entry; per-entry validation errors abort
/// via the caller's lineage walk, so this only runs on segments the
/// verifier already accepted.
fn print_segment(label: &str, path: &Path) {
    let mut reader = match JournalReader::<TradingEvent>::open(path) {
        Ok(r) => r,
        Err(e) => {
            println!("  {label}: <unreadable: {e}>");
            return;
        }
    };
    let start = reader.starting_sequence();
    let anchor = reader.anchor();
    let mut entries = 0u64;
    let mut last_seq = None;
    loop {
        match reader.next_entry() {
            Ok(Some(entry)) => {
                entries += 1;
                last_seq = Some(entry.sequence);
            }
            Ok(None) => break,
            Err(e) => {
                println!("  {label}: <read error after {entries} entries: {e}>");
                return;
            }
        }
    }
    let anchor = anchor.map(hex).unwrap_or_else(|| "-".into());
    let tail = reader.chain_hash().map(hex).unwrap_or_else(|| "-".into());
    let last = last_seq.map_or_else(|| "-".into(), |s| s.to_string());
    println!(
        "  {label}: start={start} last={last} entries={entries}\n    anchor={anchor}\n    tail=  {tail}"
    );
}

fn main() {
    let path: PathBuf = std::env::args()
        .nth(1)
        .expect("usage: journal-verify <live-journal-path>")
        .into();

    // Per-segment detail first, so a failing lineage still shows where
    // each segment stands (the boundary at fault is the one whose
    // anchor differs from its predecessor's tail).
    let mut segments: Vec<(String, PathBuf)> = melin_journal::segment::list_archives(&path)
        .expect("list archives")
        .into_iter()
        .map(|(n, p)| (format!("archive {n:06}"), p))
        .collect();
    if path.exists() {
        segments.push(("live".to_string(), path.clone()));
    }
    println!("segments ({}):", segments.len());
    for (label, p) in &segments {
        print_segment(label, p);
    }

    // Authoritative verdict: dense sequences within and across
    // segments, first entry matching each header, successor anchors
    // equal to predecessor tails.
    match melin_journal::segment::verify_lineage::<TradingEvent>(&path) {
        Ok(report) => {
            println!("lineage:  OK");
            if let Some((expected, found)) = report.live_tail_gap {
                println!(
                    "  note: live segment tail has a sequence gap (expected {expected}, \
                     found {found}) — a recoverable crash artifact, not tampering. \
                     Entries before the gap verified; recovery will truncate at the gap \
                     and nothing past it was ever acknowledged."
                );
            }
            println!("  segments:      {}", report.segments);
            println!("  entries:       {}", report.entries);
            println!("  lineage_start: {}", report.lineage_start);
            if report.lineage_start > 1 {
                println!(
                    "  note: history begins mid-lineage — earlier segments were \
                     trimmed; recovery requires a snapshot covering sequence {}",
                    report.lineage_start.saturating_sub(1)
                );
            }
            match (report.first_sequence, report.last_sequence) {
                (Some(first), Some(last)) => println!("  range:         {first}..={last}"),
                _ => println!("  range:         (empty)"),
            }
            match report.tail_chain_hash {
                Some(h) => println!("  tail_chain:    {}", hex(h)),
                None => println!("  tail_chain:    (hash-chain disabled in this build)"),
            }
        }
        Err(e) => {
            println!("lineage:  FAILED — {e}");
            std::process::exit(1);
        }
    }
}
