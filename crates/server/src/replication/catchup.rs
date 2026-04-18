//! Journal-based catch-up — replays historical journal entries to a
//! reconnecting replica before live streaming resumes.
//!
//! Reads raw entry bytes from the primary's journal files and pushes
//! them as `DataBatch` frames over the replica's TCP stream. Does NOT
//! consume from the live replication ring during catch-up — the ring
//! accumulates new data while this runs; the caller drains overlapping
//! ring entries once catch-up completes.

use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{info, warn};

use super::protocol::encode_data_batch;

/// Result of a journal catch-up attempt.
pub(super) enum CatchUpResult {
    /// Catch-up succeeded. Contains the last sequence sent (or the input
    /// last_sequence if no entries were sent).
    Ok(u64),
    /// Replica's last_sequence predates all available journal files.
    /// The primary must transfer a snapshot instead.
    NeedSnapshot,
}

/// Discover journal archive files, sorted oldest to newest.
/// Returns `[path.3, path.2, path.1, path]` — only files that exist.
pub(super) fn discover_journal_files(journal_path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut archives = Vec::new();
    let mut n = 1u32;
    loop {
        let archive = std::path::PathBuf::from(format!("{}.{n}", journal_path.display()));
        if !archive.exists() {
            break;
        }
        archives.push(archive);
        n += 1;
    }
    // Reverse so oldest is first (highest number = oldest).
    archives.reverse();
    // Current journal is newest.
    if journal_path.exists() {
        archives.push(journal_path.to_path_buf());
    }
    archives
}

/// Check if journal catch-up is possible without sending any data.
/// Returns true if the journal archives contain the replica's last_sequence,
/// false if the archives have been purged and a snapshot transfer is needed.
pub(super) fn can_catch_up_from_journal(
    journal_path: &std::path::Path,
    last_sequence: u64,
) -> io::Result<bool> {
    use melin_engine::journal::reader::RawJournalScanner;

    let files = discover_journal_files(journal_path);
    if files.is_empty() || last_sequence == 0 {
        // No files or fresh replica — catch-up will handle it.
        return Ok(true);
    }

    // Check if any file starts at or before the target sequence.
    for path in files.iter().rev() {
        let mut scanner = RawJournalScanner::open(path)
            .map_err(|e| io::Error::other(format!("open journal {}: {e}", path.display())))?;
        if let Some(first_seq) = scanner
            .first_sequence()
            .map_err(|e| io::Error::other(format!("read {}: {e}", path.display())))?
            && first_seq <= last_sequence
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Stream historical journal entries to a catching-up replica.
///
/// Returns the last sequence sent, or the input `last_sequence` if no
/// entries were sent.
pub(super) fn catch_up_from_journal(
    journal_path: &std::path::Path,
    last_sequence: u64,
    writer: &mut TcpStream,
    shutdown: &AtomicBool,
) -> io::Result<CatchUpResult> {
    use melin_engine::journal::reader::RawJournalScanner;

    let files = discover_journal_files(journal_path);
    if files.is_empty() {
        return Ok(CatchUpResult::Ok(last_sequence));
    }

    // Find the first file that contains entries after last_sequence.
    // For a fresh replica (last_sequence=0), start from the oldest file.
    let mut start_file_idx = 0;
    if last_sequence > 0 {
        // Scan files from newest to oldest to find which contains our target.
        let mut found = false;
        for (i, path) in files.iter().enumerate().rev() {
            let mut scanner = RawJournalScanner::open(path)
                .map_err(|e| io::Error::other(format!("open journal {}: {e}", path.display())))?;
            if let Some(first_seq) = scanner
                .first_sequence()
                .map_err(|e| io::Error::other(format!("read {}: {e}", path.display())))?
                && first_seq <= last_sequence
            {
                // This file starts at or before our target — start here.
                start_file_idx = i;
                found = true;
                break;
            }
        }
        if !found {
            // All files start after our target — journal archives were purged.
            // The replica needs a snapshot transfer.
            warn!(
                last_sequence,
                "replica's last_sequence predates all available journal files — snapshot transfer required"
            );
            return Ok(CatchUpResult::NeedSnapshot);
        }
    }

    let mut send_buf = Vec::with_capacity(128 * 1024);
    let mut batch_buf = Vec::with_capacity(64 * 1024);
    let mut end_sequence = last_sequence;
    let mut batches_sent = 0u64;

    info!(
        last_sequence,
        files = files.len(),
        start_file = start_file_idx,
        "starting journal catch-up"
    );

    for path in &files[start_file_idx..] {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(CatchUpResult::Ok(end_sequence));
        }

        let mut scanner = RawJournalScanner::open(path)
            .map_err(|e| io::Error::other(format!("open journal {}: {e}", path.display())))?;

        // Skip entries the replica already has. Always skip at least
        // genesis (seq 1) — it's delivered via StreamStart, not catch-up.
        let skip_to = end_sequence.max(1);
        scanner
            .skip_to_after(skip_to)
            .map_err(|e| io::Error::other(format!("skip in {}: {e}", path.display())))?;

        // Read and send batches of raw entries.
        // Target ~64 KiB per DataBatch frame (~800 entries at ~80 bytes each).
        loop {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(CatchUpResult::Ok(end_sequence));
            }

            batch_buf.clear();
            let batch = scanner
                .read_raw_batch(&mut batch_buf, 64 * 1024)
                .map_err(|e| io::Error::other(format!("read {}: {e}", path.display())))?;

            let Some(batch_end_seq) = batch else {
                break; // EOF on this file.
            };

            // Encode and send DataBatch frame.
            encode_data_batch(batch_end_seq, &batch_buf, &mut send_buf);
            writer
                .write_all(&send_buf)
                .map_err(|e| io::Error::other(format!("write catch-up batch: {e}")))?;
            writer
                .flush()
                .map_err(|e| io::Error::other(format!("flush catch-up batch: {e}")))?;
            send_buf.clear();

            end_sequence = batch_end_seq;
            batches_sent += 1;
        }
    }

    info!(end_sequence, batches_sent, "journal catch-up complete");

    Ok(CatchUpResult::Ok(end_sequence))
}
