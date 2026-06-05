//! Journal-based catch-up — replays historical journal entries to a
//! reconnecting replica before live streaming resumes.
//!
//! Reads raw entry bytes from the primary's journal files (journal-codec
//! format), decodes them into `InputSlot` records, and pushes them as
//! `InputBatch` frames over the replica's transport. Does NOT consume
//! from the live replication ring during catch-up — the ring accumulates
//! new data while this runs; the caller drains overlapping ring entries
//! once catch-up completes.

use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};

use melin_app::AppEvent;
use tracing::{info, warn};

use super::protocol::{decode_journal_to_input_slots, encode_input_batch};

/// Closure-based publisher passed to [`catch_up_from_journal_with`].
/// Receives the fully encoded `InputBatch` frame (length prefix included)
/// and is responsible for actually shipping the bytes to the replica. The
/// closure returns `io::Error` on transport failure so the caller can
/// abandon the catch-up and surface a clean disconnect.
pub type CatchUpPublisher<'a> = &'a mut dyn FnMut(&[u8]) -> io::Result<()>;

/// Result of a journal catch-up attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpResult {
    /// Catch-up succeeded. Contains the last sequence sent (or the input
    /// last_sequence if no entries were sent).
    Ok(u64),
    /// Replica's last_sequence predates all available journal files.
    /// The primary must transfer a snapshot instead.
    NeedSnapshot,
}

/// Discover journal segment files, sorted oldest to newest: archived
/// segments in monotonic order (`.000001` is the oldest), then the live
/// segment. Uses the same discovery as recovery
/// ([`melin_journal::segment::list_archives`]), so catch-up sees exactly
/// the segments a local replay would.
pub fn discover_journal_files(journal_path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<std::path::PathBuf> =
        match melin_journal::segment::list_archives(journal_path) {
            Ok(archives) => archives.into_iter().map(|(_, p)| p).collect(),
            Err(e) => {
                // Degrading to live-only fails safe in both probe
                // directions: a behind replica's coverage check finds no
                // file reaching its last_sequence, and a fresh replica's
                // history check finds the live header starting past 1 —
                // either way `can_catch_up_from_journal` answers
                // NeedSnapshot rather than catching up from partial
                // history. Never silently narrows what gets streamed.
                warn!(error = %e, "archive discovery failed — catch-up limited to live segment");
                Vec::new()
            }
        };
    // Current journal is newest.
    if journal_path.exists() {
        files.push(journal_path.to_path_buf());
    }
    files
}

/// Header identity of the oldest available segment — the lineage origin
/// a fresh replica must create its journal with so full catch-up
/// produces a byte-identical segment (identical until the first
/// rotation on either node; segment boundaries are local after that).
pub fn lineage_origin(journal_path: &std::path::Path) -> io::Result<(u64, [u8; 32])> {
    let files = discover_journal_files(journal_path);
    let oldest = files
        .first()
        .ok_or_else(|| io::Error::other("no journal segments on disk"))?;
    let info = melin_journal::segment::read_header_info(oldest)
        .map_err(|e| io::Error::other(format!("read header of {}: {e}", oldest.display())))?;
    Ok((info.starting_sequence, info.anchor_hash))
}

/// Check if journal catch-up is possible without sending any data.
/// Returns true if the journal archives contain the replica's last_sequence,
/// false if the archives have been purged and a snapshot transfer is needed.
pub fn can_catch_up_from_journal(
    journal_path: &std::path::Path,
    last_sequence: u64,
) -> io::Result<bool> {
    let files = discover_journal_files(journal_path);
    let Some(oldest) = files.first() else {
        // No files at all — catch-up streams nothing, which is correct.
        return Ok(true);
    };

    // One rule covers every replica: the on-disk lineage is dense from
    // the oldest header to the tail (recovery verifies this), so
    // catch-up can serve a replica iff the oldest segment starts at or
    // before the replica's next needed sequence. The header's
    // `starting_sequence` — NOT the first entry — so empty segments
    // (a just-created live, a fresh rotation) count as the history
    // their header says they continue.
    //
    // The fresh-replica case (`last_sequence == 0`) falls out naturally:
    // it needs the oldest header to start at 1, i.e. the COMPLETE
    // history. After archive pruning or a snapshot-only restart the
    // oldest surviving header starts past 1 — streaming from there
    // would build a self-consistent journal on top of an empty
    // exchange, silently missing every pre-trim event (the replica's
    // own next restart would refuse it with MissingHistoryPrefix).
    // Snapshot transfer is the correct route.
    let info = melin_journal::segment::read_header_info(oldest)
        .map_err(|e| io::Error::other(format!("read header of {}: {e}", oldest.display())))?;
    Ok(info.starting_sequence <= last_sequence.saturating_add(1))
}

/// Stream historical journal entries to a catching-up replica via a
/// caller-supplied [`CatchUpPublisher`] closure.
///
/// Returns the last sequence sent, or the input `last_sequence` if no
/// entries were sent. The closure is called once per encoded `InputBatch`
/// frame; it receives the bytes to ship and is responsible for the
/// actual transport write (TCP `write_all`+`flush`).
///
/// Generic over `E: AppEvent` — the journal codec decodes into the
/// application's event type, and the resulting `InputSlot<E>` records
/// are re-encoded as `InputBatch` frames the replica's input ring
/// expects.
pub fn catch_up_from_journal_with<E: AppEvent>(
    journal_path: &std::path::Path,
    last_sequence: u64,
    publisher: CatchUpPublisher<'_>,
    shutdown: &AtomicBool,
) -> io::Result<CatchUpResult> {
    use melin_journal::RawJournalScanner;

    let files = discover_journal_files(journal_path);
    if files.is_empty() {
        return Ok(CatchUpResult::Ok(last_sequence));
    }

    // Find the newest file whose header starts at or before the
    // replica's next needed sequence — streaming begins there and walks
    // forward. Headers, not first entries: an empty segment (e.g. a
    // just-created live after a snapshot-only restart, exactly one past
    // the snapshot the replica just received) is a valid, contiguous
    // start point with nothing to send — NOT a missing-history signal.
    // Treating it as missing caused an infinite NeedSnapshot loop: the
    // post-transfer replica sits exactly at the empty live's boundary.
    let next_needed = last_sequence.saturating_add(1);
    let mut start_file_idx = None;
    for (i, path) in files.iter().enumerate().rev() {
        let info = melin_journal::segment::read_header_info(path)
            .map_err(|e| io::Error::other(format!("read header of {}: {e}", path.display())))?;
        if info.starting_sequence <= next_needed {
            start_file_idx = Some(i);
            break;
        }
    }
    let Some(start_file_idx) = start_file_idx else {
        // Even the oldest header starts past the replica's next needed
        // sequence — the history between is gone (archives purged).
        warn!(
            last_sequence,
            "replica's last_sequence predates all available journal files — snapshot transfer required"
        );
        return Ok(CatchUpResult::NeedSnapshot);
    };

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

        // Skip entries the replica already has. Sequence 1 is a real
        // user event (chain metadata lives in segment headers, not the
        // entry stream), so nothing below `end_sequence` is exempt.
        scanner
            .skip_to_after(end_sequence)
            .map_err(|e| io::Error::other(format!("skip in {}: {e}", path.display())))?;

        // Read and send batches of raw entries.
        // Target ~64 KiB per InputBatch frame (~800 entries at ~80 bytes each).
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

            // Decode the journal-codec bytes into InputSlots and re-encode
            // as an `InputBatch` for the wire. Catch-up reads journal
            // *files* (still journal-codec); the live streaming path's
            // ring chunks are already InputBatch frames so it skips this.
            let slots = decode_journal_to_input_slots::<E>(&batch_buf).map_err(|e| {
                io::Error::other(format!(
                    "catch-up journal decode at seq {batch_end_seq}: {e}"
                ))
            })?;
            encode_input_batch(&slots, &mut send_buf);
            publisher(&send_buf)
                .map_err(|e| io::Error::other(format!("publish catch-up batch: {e}")))?;
            send_buf.clear();

            end_sequence = batch_end_seq;
            batches_sent += 1;
        }
    }

    info!(end_sequence, batches_sent, "journal catch-up complete");

    Ok(CatchUpResult::Ok(end_sequence))
}

/// Transfer a snapshot to a replica, then catch up from journal.
///
/// Reads the snapshot file, validates its header (magic, sequence,
/// chain hash), and streams the bytes as `NeedSnapshot → SnapshotBegin
/// → SnapshotChunk* → SnapshotEnd → StreamStart` followed by journal
/// catch-up from the snapshot's sequence.
///
/// `publisher` is called for each encoded control/chunk frame. The TCP
/// path passes `write_all+flush`; the DPDK path passes
/// `queue_send+poll`.
pub fn snapshot_transfer_with<E: AppEvent>(
    journal_path: &std::path::Path,
    publisher: CatchUpPublisher<'_>,
    shutdown: &AtomicBool,
) -> io::Result<CatchUpResult> {
    use super::protocol::{
        encode_need_snapshot, encode_snapshot_begin, encode_snapshot_chunk, encode_snapshot_end,
        encode_stream_start,
    };

    let snap_path = journal_path.with_extension("snapshot");
    if !snap_path.exists() {
        return Err(io::Error::other(
            "snapshot transfer required but no snapshot available \
             — set --snapshot-interval-ms to a non-zero value so the shadow exchange writes snapshots",
        ));
    }

    let mut send_buf = Vec::with_capacity(64 * 1024 + 128);

    // Send NeedSnapshot.
    encode_need_snapshot(&mut send_buf);
    publisher(&send_buf)?;
    send_buf.clear();

    // Read and validate snapshot.
    let snap_data = std::fs::read(&snap_path)
        .map_err(|e| io::Error::other(format!("read snapshot {}: {e}", snap_path.display())))?;
    if snap_data.len() < 48 {
        return Err(io::Error::other("snapshot file too small for header"));
    }
    let magic = u32::from_le_bytes(
        snap_data[0..4]
            .try_into()
            .expect("bounds checked: snap_data has at least 48 bytes"),
    );
    if magic != 0x534E_4150 {
        return Err(io::Error::other(format!(
            "snapshot file has invalid magic: {magic:#x} (expected 0x534e4150)"
        )));
    }
    let snap_sequence = u64::from_le_bytes(
        snap_data[8..16]
            .try_into()
            .expect("bounds checked: snap_data has at least 48 bytes"),
    );
    let mut snap_chain_hash = [0u8; 32];
    snap_chain_hash.copy_from_slice(&snap_data[16..48]);
    let snap_len = snap_data.len() as u64;

    info!(
        snap_sequence,
        snap_len,
        path = %snap_path.display(),
        "transferring snapshot to replica"
    );

    // Send SnapshotBegin.
    encode_snapshot_begin(snap_len, snap_sequence, &snap_chain_hash, &mut send_buf);
    publisher(&send_buf)?;
    send_buf.clear();

    // Stream snapshot in 64 KiB chunks.
    const CHUNK_SIZE: usize = 64 * 1024;
    let mut offset = 0;
    while offset < snap_data.len() {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(CatchUpResult::Ok(snap_sequence));
        }
        let end = (offset + CHUNK_SIZE).min(snap_data.len());
        encode_snapshot_chunk(&snap_data[offset..end], &mut send_buf);
        publisher(&send_buf)?;
        send_buf.clear();
        offset = end;
    }

    // Send SnapshotEnd with CRC32C.
    let transfer_crc = crc32c::crc32c(&snap_data);
    encode_snapshot_end(transfer_crc, &mut send_buf);
    publisher(&send_buf)?;
    send_buf.clear();

    info!(snap_sequence, "snapshot transfer complete");

    // Send StreamStart so the replica can set up its journal: a fresh
    // segment continuing at `snap_sequence + 1`, anchored to the
    // snapshot's chain hash.
    encode_stream_start(
        snap_sequence,
        snap_sequence + 1,
        snap_chain_hash,
        &mut send_buf,
    );
    publisher(&send_buf)?;
    send_buf.clear();

    // Catch up from the snapshot's sequence.
    catch_up_from_journal_with::<E>(journal_path, snap_sequence, publisher, shutdown)
}

/// Thin wrapper around [`catch_up_from_journal_with`] that ships
/// frames over a TCP stream.
pub fn catch_up_from_journal<E: AppEvent>(
    journal_path: &std::path::Path,
    last_sequence: u64,
    writer: &mut TcpStream,
    shutdown: &AtomicBool,
) -> io::Result<CatchUpResult> {
    let mut publish = |buf: &[u8]| -> io::Result<()> {
        writer.write_all(buf)?;
        writer.flush()
    };
    catch_up_from_journal_with::<E>(journal_path, last_sequence, &mut publish, shutdown)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestEvent;
    use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};

    /// Build a 3-segment journal (two rotations) with one event per
    /// phase, returning the live path.
    fn three_segment_journal(dir: &std::path::Path) -> std::path::PathBuf {
        let live = dir.join("j.journal");
        let mut writer = BufferedWriter::<TestEvent>::create(&live).unwrap();
        writer
            .append(&JournalEvent::App(TestEvent::Add(1)))
            .unwrap();
        writer.rotate_segment().unwrap();
        writer
            .append(&JournalEvent::App(TestEvent::Add(2)))
            .unwrap();
        writer.rotate_segment().unwrap();
        writer
            .append(&JournalEvent::App(TestEvent::Add(3)))
            .unwrap();
        drop(writer);
        live
    }

    /// Catch-up discovery must see monotonic `.NNNNNN` archives in
    /// rotation order, then the live segment — the same set recovery
    /// walks. (Regression: discovery used the legacy `.1`/`.2` naming
    /// and silently saw only the live segment, forcing snapshot
    /// transfers for any replica behind the last rotation.)
    #[test]
    fn discovery_orders_monotonic_archives_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        let live = three_segment_journal(dir.path());

        let files = discover_journal_files(&live);
        assert_eq!(files.len(), 3, "two archives + live: {files:?}");
        assert_eq!(files[0], dir.path().join("j.journal.000001"));
        assert_eq!(files[1], dir.path().join("j.journal.000002"));
        assert_eq!(files[2], live);

        // Header starting sequences must ascend across the walk order.
        let starts: Vec<u64> = files
            .iter()
            .map(|p| {
                melin_journal::segment::read_header_info(p)
                    .unwrap()
                    .starting_sequence
            })
            .collect();
        assert_eq!(starts, vec![1, 2, 3]);
    }

    /// The lineage origin is the oldest segment's header identity —
    /// what a fresh replica must seed its journal with so full catch-up
    /// reproduces the primary's journal byte-for-byte.
    #[test]
    fn lineage_origin_is_oldest_segments_header() {
        let dir = tempfile::tempdir().unwrap();
        let live = three_segment_journal(dir.path());

        let (start, anchor) = lineage_origin(&live).unwrap();
        let oldest =
            melin_journal::segment::read_header_info(&dir.path().join("j.journal.000001")).unwrap();
        assert_eq!(start, 1);
        assert_eq!(start, oldest.starting_sequence);
        assert_eq!(anchor, oldest.anchor_hash);

        // Without rotations, the live segment itself is the origin.
        let solo = dir.path().join("solo.journal");
        drop(BufferedWriter::<TestEvent>::create(&solo).unwrap());
        let (solo_start, solo_anchor) = lineage_origin(&solo).unwrap();
        let solo_info = melin_journal::segment::read_header_info(&solo).unwrap();
        assert_eq!(solo_start, 1);
        assert_eq!(solo_anchor, solo_info.anchor_hash);
    }

    /// Catch-up probing spans the full archive set: a replica whose
    /// `last_sequence` falls inside the oldest archive is reachable by
    /// journal catch-up; one predating a trimmed history is not.
    #[test]
    fn can_catch_up_spans_archives() {
        let dir = tempfile::tempdir().unwrap();
        let live = three_segment_journal(dir.path());

        // Seq 1 lives in archive 000001 — reachable.
        assert!(can_catch_up_from_journal(&live, 1).unwrap());
        // Fresh replica (0) — reachable, history goes back to seq 1.
        assert!(can_catch_up_from_journal(&live, 0).unwrap());

        // Trim the oldest archive (held seq 1). A replica at seq 1 sits
        // exactly at the surviving lineage's boundary (000002 starts at
        // 2) — reachable with nothing missed. A fresh replica is not.
        std::fs::remove_file(dir.path().join("j.journal.000001")).unwrap();
        assert!(can_catch_up_from_journal(&live, 1).unwrap());
        assert!(!can_catch_up_from_journal(&live, 0).unwrap());

        // Trim the next archive too (held seq 2): a replica at seq 1
        // now genuinely predates the surviving history (live starts at
        // 3), while one at the live's boundary remains reachable.
        std::fs::remove_file(dir.path().join("j.journal.000002")).unwrap();
        assert!(!can_catch_up_from_journal(&live, 1).unwrap());
        assert!(can_catch_up_from_journal(&live, 2).unwrap());
    }

    /// A replica sitting exactly at the boundary of an EMPTY live
    /// segment — the position every replica holds right after a
    /// snapshot transfer from a snapshot-only-restarted primary — is
    /// contiguous with the on-disk history: catch-up has nothing to
    /// send, which is success. (Regression: the start-file scan read
    /// first *entries* instead of headers, so the empty live looked
    /// like missing history and the sender looped snapshot transfer →
    /// NeedSnapshot forever, never letting the replica register.)
    #[test]
    fn boundary_replica_of_empty_live_catches_up_with_nothing_to_send() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("resumed.journal");
        drop(BufferedWriter::<TestEvent>::create_continuing(&live, 34, [7u8; 32]).unwrap());

        assert!(can_catch_up_from_journal(&live, 33).unwrap());

        let shutdown = std::sync::atomic::AtomicBool::new(false);
        let mut published = 0usize;
        let mut publish = |_: &[u8]| -> io::Result<()> {
            published += 1;
            Ok(())
        };
        let res = catch_up_from_journal_with::<TestEvent>(&live, 33, &mut publish, &shutdown)
            .unwrap();
        assert!(
            matches!(res, CatchUpResult::Ok(33)),
            "boundary catch-up must succeed with nothing to send, got {res:?}"
        );
        assert_eq!(published, 0, "no batches expected at the boundary");
    }

    /// A fresh replica must be routed to snapshot transfer whenever the
    /// on-disk history doesn't reach back to sequence 1. (Regression:
    /// `last_sequence == 0` returned true unconditionally, so a fresh
    /// replica facing a pruned lineage caught up from the surviving
    /// suffix — a self-consistent journal over an empty exchange,
    /// silently missing every pre-trim event.)
    #[test]
    fn fresh_replica_needs_snapshot_when_history_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        let live = three_segment_journal(dir.path());

        // Trimmed prefix: oldest surviving header starts at 2.
        std::fs::remove_file(dir.path().join("j.journal.000001")).unwrap();
        assert!(
            !can_catch_up_from_journal(&live, 0).unwrap(),
            "fresh replica must not catch up from a trimmed lineage"
        );

        // A snapshot-only restart layout: single live segment whose
        // header starts past 1 (no entries yet).
        let resumed = dir.path().join("resumed.journal");
        drop(BufferedWriter::<TestEvent>::create_continuing(&resumed, 21, [7u8; 32]).unwrap());
        assert!(
            !can_catch_up_from_journal(&resumed, 0).unwrap(),
            "fresh replica must not catch up from a snapshot-anchored journal"
        );

        // An empty-but-complete journal (fresh primary, no events yet)
        // IS reachable: header starts at 1, nothing is missing.
        let fresh = dir.path().join("fresh.journal");
        drop(BufferedWriter::<TestEvent>::create(&fresh).unwrap());
        assert!(
            can_catch_up_from_journal(&fresh, 0).unwrap(),
            "fresh replica of a fresh primary needs no snapshot"
        );
    }
}
