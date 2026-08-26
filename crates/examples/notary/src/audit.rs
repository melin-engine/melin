#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Offline auditor for the notary example.
//!
//! Walks a notary server's journal — every archived segment, then the
//! live one — and refolds the chain from the raw entries: each `Notarize`
//! leaf with the timestamp the sequencer journaled beside it. The head it
//! arrives at is what the server reports after replaying the same
//! journal, computed without the server. That is the auditor's side of
//! the story: `notary-client verify` accepts a receipt because it folds;
//! this tool checks that the journal actually holds the link the receipt
//! describes, at the position it claims. The journal is the evidence, and
//! it can be read without the software that wrote it being up — or
//! trusted.
//!
//! Two layers are checked. First the runtime's own tamper evidence, via
//! the journal crate's lineage walk: every entry's CRC, dense sequence
//! numbers, and each segment's header anchored in its predecessor's tail
//! hash — an edited byte or a swapped segment fails here. Then the
//! application's chain, which is the notary's: the refold above, compared
//! against whatever the caller brought — receipts, or the head the live
//! server reports.
//!
//! The refold rests on a property of this application, stated in
//! `lib.rs`: every journaled `Notarize` was folded in. Permission is
//! checked before journaling and the notary accepts every request
//! sequence, so there is no journaled-but-rejected leaf to skip.
//!
//! ```sh
//! notary-audit /tmp/notary.journal                                 # refold, print the head
//! notary-audit /tmp/notary.journal --receipt contract.pdf.receipt  # is this link in the log?
//! notary-audit /tmp/notary.journal --expect-head <hex>             # against `notary-client head`
//! ```
//!
//! Exit 0 when everything checks out; 1 when the evidence does not (a
//! broken lineage, a receipt the log contradicts, a head that differs);
//! 2 when the audit could not run (unreadable journal, malformed
//! receipt). Read-only: the journal is never opened for writing. Audit a
//! stopped server or a copy for a definitive result — against a running
//! one the walk may end at an entry still being written.

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use melin_journal::segment;
use melin_journal::{JournalError, JournalEvent, JournalReader};
use notary_server::receipt::{Receipt, hex, unhex};
use notary_server::{GENESIS_HEAD, HEAD_LEN, NotaryEvent, fold};

type Error = Box<dyn std::error::Error>;

#[derive(Parser)]
#[command(
    name = "notary-audit",
    about = "Refold a notary journal and check receipts against it"
)]
struct Cli {
    /// The live journal file. Archived segments (`<journal>.NNNNNN`) are
    /// found beside it.
    journal: PathBuf,
    /// A receipt to look for in the log. May be repeated.
    #[arg(long = "receipt", value_name = "FILE")]
    receipts: Vec<PathBuf>,
    /// The head the log is expected to refold to, as `notary-client head`
    /// prints it.
    #[arg(long, value_name = "HEX", value_parser = unhex::<HEAD_LEN>)]
    expect_head: Option<[u8; HEAD_LEN]>,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}

/// `Ok(true)` when every check passed. Findings go to stdout, prefixed
/// `FAIL:`, so a report reads top to bottom like the audit ran.
fn run(cli: Cli) -> Result<bool, Error> {
    let mut receipts = Vec::with_capacity(cli.receipts.len());
    for path in &cli.receipts {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let receipt = Receipt::from_text(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        receipts.push((path.as_path(), receipt));
    }
    // Checked as the walk reaches each position, so sorted by position:
    // the walk visits every entry once and never looks back.
    receipts.sort_by_key(|(_, receipt)| receipt.entry);

    // Layer one: the journal's own chain. An I/O error means the audit
    // could not run; any other error is the evidence failing.
    let lineage = match segment::verify_lineage::<NotaryEvent>(&cli.journal) {
        Ok(lineage) => lineage,
        Err(JournalError::Io(e)) => {
            return Err(format!("cannot read {}: {e}", cli.journal.display()).into());
        }
        Err(e) => {
            println!("FAIL: the journal's own hash chain is broken: {e}");
            return Ok(false);
        }
    };
    println!("segments: {}", lineage.segments);
    println!("journal entries: {}", lineage.entries);
    if let Some((expected, found)) = lineage.live_tail_gap {
        println!(
            "note: the live segment ends in a sequence gap (expected {expected}, found \
             {found}); the entries past it were never acknowledged and are ignored, as \
             recovery would"
        );
    }
    if lineage.lineage_start > 1 {
        println!(
            "FAIL: the on-disk history starts at sequence {}, not 1: the head cannot be \
             refolded from genesis (were archived segments removed?)",
            lineage.lineage_start
        );
        return Ok(false);
    }

    // Layer two: the notary's chain.
    let refold = refold(&cli.journal, lineage.last_sequence, &receipts)?;
    println!("notarized: {}", refold.notarized);
    println!("head: {}", hex(&refold.head));
    let mut findings = refold.findings;
    if let Some(expected) = cli.expect_head {
        if expected == refold.head {
            println!("expected head: matches");
        } else {
            findings.push(format!(
                "the log refolds to {}, not the expected {}",
                hex(&refold.head),
                hex(&expected)
            ));
        }
    }
    for finding in &findings {
        println!("FAIL: {finding}");
    }
    Ok(findings.is_empty())
}

/// What the walk arrived at.
struct Refold {
    /// Leaves folded in.
    notarized: u64,
    head: [u8; HEAD_LEN],
    /// Receipts the log contradicts, one line each.
    findings: Vec<String>,
}

/// Walk every segment in lineage order and fold each `Notarize` leaf with
/// the timestamp journaled beside it, checking receipts as their positions
/// are reached. `through` is the last sequence the lineage check
/// validated; the walk stops there rather than re-deciding what to do
/// about a crash-truncated tail. `None` means an empty lineage.
fn refold(
    live: &Path,
    through: Option<u64>,
    receipts: &[(&Path, Receipt)],
) -> Result<Refold, Error> {
    let mut receipts = receipts.iter().peekable();
    let mut findings = Vec::new();
    // Position 0 is nothing's position; drained first so it cannot sit at
    // the front of the queue and block every real receipt behind it.
    while let Some((path, _)) = receipts.next_if(|(_, receipt)| receipt.entry == 0) {
        findings.push(format!("{}: entry 0 is not a position", path.display()));
    }

    let mut head = GENESIS_HEAD;
    let mut notarized = 0u64;
    if let Some(through) = through {
        let mut segments: Vec<PathBuf> = segment::list_archives(live)?
            .into_iter()
            .map(|(_, path)| path)
            .collect();
        if live.exists() {
            segments.push(live.to_path_buf());
        }
        'walk: for path in &segments {
            let mut reader = JournalReader::<NotaryEvent>::open(path)?;
            while let Some(entry) = reader.next_entry()? {
                if let JournalEvent::App(NotaryEvent::Notarize { leaf }) = entry.event {
                    let prev = head;
                    head = fold(&prev, &leaf, entry.timestamp_ns);
                    notarized += 1;
                    let link = Receipt {
                        entry: notarized,
                        timestamp_ns: entry.timestamp_ns,
                        leaf,
                        prev,
                        head,
                    };
                    while let Some((path, receipt)) =
                        receipts.next_if(|(_, receipt)| receipt.entry == notarized)
                    {
                        let differs = differences(receipt, &link);
                        if differs.is_empty() {
                            println!("receipt {}: entry {notarized} OK", path.display());
                        } else {
                            findings.push(format!(
                                "receipt {}: entry {notarized} differs from the journal in {}",
                                path.display(),
                                differs.join(", ")
                            ));
                        }
                    }
                }
                if entry.sequence == through {
                    break 'walk;
                }
            }
        }
    }

    for (path, receipt) in receipts {
        findings.push(format!(
            "receipt {}: claims entry {} but the log has {notarized}",
            path.display(),
            receipt.entry
        ));
    }
    Ok(Refold {
        notarized,
        head,
        findings,
    })
}

/// The fields in which `claimed` differs from the journal's `link` —
/// named, because which one differs says what happened (a different
/// `leaf` is a different document; a different `time_ns` alone is a
/// receipt that was edited).
fn differences(claimed: &Receipt, link: &Receipt) -> Vec<&'static str> {
    let mut out = Vec::new();
    if claimed.leaf != link.leaf {
        out.push("leaf");
    }
    if claimed.timestamp_ns != link.timestamp_ns {
        out.push("time_ns");
    }
    if claimed.prev != link.prev {
        out.push("prev");
    }
    if claimed.head != link.head {
        out.push("head");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn differences_name_every_field_that_differs() {
        let leaf = [0x11; 32];
        let prev = [0x22; 32];
        let link = Receipt {
            entry: 3,
            timestamp_ns: 1_000,
            leaf,
            prev,
            head: fold(&prev, &leaf, 1_000),
        };
        assert!(differences(&link, &link).is_empty());

        let mut edited = link;
        edited.timestamp_ns += 1;
        assert_eq!(differences(&edited, &link), ["time_ns"]);

        let other_leaf = [0x33; 32];
        let other = Receipt {
            leaf: other_leaf,
            head: fold(&prev, &other_leaf, 1_000),
            ..link
        };
        assert_eq!(differences(&other, &link), ["leaf", "head"]);
    }
}
