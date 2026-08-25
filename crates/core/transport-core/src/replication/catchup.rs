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
use std::time::{Duration, Instant};

use melin_app::AppEvent;
use tracing::{info, warn};

use super::protocol::{
    decode_journal_to_input_slots, encode_input_batch, encode_rotate, peek_first_sequence,
    peek_frame_tag,
};
use crate::replication_wire::MSG_INPUT_BATCH;

/// Upper bound on how long the catch-up→live handoff waits for the
/// disk to catch up to the ring (see [`drain_into_contiguity`]). The
/// gap it closes is one journal flush of slack, so the bound is
/// calibrated to the *required* production config — PLP NVMe + xfs,
/// where a `buffered` batch flush is ~10–30 µs (`docs/journal.md`) and
/// the multi-millisecond stall sources are engineered out (xfs removes
/// the ext4 jbd2 spike; `sector` mode is barred from production). 30 ms
/// is ~3 orders of magnitude over that close time, so it never expires
/// on in-spec hardware.
///
/// The bound is deliberately tight rather than generous because the
/// spin runs inline on the single-threaded DPDK driver loop, where it
/// head-of-line-blocks the other replica's ring drain and ack
/// processing. A stall long enough to expire 30 ms is a failing drive,
/// not a hiccup — and on expiry the handoff falls back to the
/// receiver's contiguity gate (a reconnect), which is the right
/// outcome when the disk has stopped keeping up. Out-of-spec hardware
/// (e.g. `buffered` on a consumer drive, ~50–200 µs with fatter tails)
/// merely falls back more often: still correct, just less efficient.
/// This is a safety bound, not a steady-state cost.
const HANDOFF_BRIDGE_TIMEOUT: Duration = Duration::from_millis(30);

/// Closure-based publisher passed to [`catch_up_from_journal_with`].
/// Receives the fully encoded `InputBatch` frame (length prefix included)
/// and is responsible for actually shipping the bytes to the replica. The
/// closure returns `io::Error` on transport failure so the caller can
/// abandon the catch-up and surface a clean disconnect.
pub type CatchUpPublisher<'a> = &'a mut dyn FnMut(&[u8]) -> io::Result<()>;

/// Forward-bytes sink used inside [`drain_into_contiguity`] — the
/// transport write for one `InputBatch` frame.
type ForwardFn<'a> = &'a mut dyn FnMut(&[u8]) -> io::Result<()>;

/// Re-read the journal forward from a sequence, forwarding any
/// newly-durable entries via the supplied sink, and return the new
/// high-water (`from` unchanged when nothing new is durable). Injected
/// into [`drain_into_contiguity`] so the disk/ring race is testable
/// without real files; production wraps [`catch_up_from_journal_with`].
type RefillFn<'a> = &'a mut dyn FnMut(u64, ForwardFn<'_>) -> io::Result<u64>;

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
/// produces a byte-identical segment (and the `Rotate` frames emitted
/// at each segment transition keep it byte-identical across rotations).
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
    let Some((start_file_idx, _)) = containing_segment(&files, next_needed)? else {
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

        // Boundary announce: every transition between on-disk segments
        // is a rotation the primary performed, and replicas must rotate
        // at the same sequence (primary-driven rotation). Emit it when
        // the stream position sits exactly on the segment's opening
        // boundary and that opening IS a boundary (`starting_sequence >
        // 1`) — judged from the header, NOT from a predecessor file
        // being present: the predecessor may have been pruned, and a
        // replica that journaled through the boundary but missed its
        // live `Rotate` frame must still learn it here. Covers mid-walk
        // transitions and boundary-exact reconnects alike; a replica
        // that already rotated there detects the duplicate announce and
        // skips it. (A segment whose header starts past 1 without a
        // rotation exists only on snapshot-seeded journals; replicas
        // positioned at such an opening hold the identical segment
        // identity and dedupe the announce the same way.)
        let info = melin_journal::segment::read_header_info(path)
            .map_err(|e| io::Error::other(format!("read header of {}: {e}", path.display())))?;
        if info.starting_sequence > 1 {
            let boundary = info.starting_sequence - 1;
            if end_sequence == boundary {
                encode_rotate(boundary, &info.anchor_hash, &mut send_buf);
                publisher(&send_buf)
                    .map_err(|e| io::Error::other(format!("publish catch-up rotate: {e}")))?;
                send_buf.clear();
            }
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

/// Newest on-disk segment whose header starts at or before `seq` — in a
/// dense lineage, the segment containing `seq`. Returns the index into
/// `files` plus the decoded header, or `None` when every surviving
/// header starts past `seq` (history pruned). Shared by the catch-up
/// start-file scan, the seed-segment lookup, and handshake validation.
pub(crate) fn containing_segment(
    files: &[std::path::PathBuf],
    seq: u64,
) -> io::Result<Option<(usize, melin_journal::FileHeaderInfo)>> {
    for (idx, path) in files.iter().enumerate().rev() {
        let info = melin_journal::segment::read_header_info(path)
            .map_err(|e| io::Error::other(format!("read header of {}: {e}", path.display())))?;
        if info.starting_sequence <= seq {
            return Ok(Some((idx, info)));
        }
    }
    Ok(None)
}

/// Ship `body` as `SnapshotChunk` frames (64 KiB each) terminated by a
/// `SnapshotEnd` carrying the body's CRC32C — the framing shared by the
/// snapshot payload and the segment seed. Returns `false` when shutdown
/// interrupted the transfer.
fn publish_chunked_body(
    body: &[u8],
    send_buf: &mut Vec<u8>,
    publisher: CatchUpPublisher<'_>,
    shutdown: &AtomicBool,
) -> io::Result<bool> {
    use super::protocol::{encode_snapshot_chunk, encode_snapshot_end};

    const CHUNK_SIZE: usize = 64 * 1024;
    let mut offset = 0;
    while offset < body.len() {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let end = (offset + CHUNK_SIZE).min(body.len());
        encode_snapshot_chunk(&body[offset..end], send_buf);
        publisher(send_buf)?;
        send_buf.clear();
        offset = end;
    }
    encode_snapshot_end(crc32c::crc32c(body), send_buf);
    publisher(send_buf)?;
    send_buf.clear();
    Ok(true)
}

/// Transfer a snapshot to a replica, then catch up from journal.
///
/// Reads the snapshot file, validates its header (magic, sequence,
/// chain hash), and streams the bytes as `SnapshotBegin →
/// SnapshotChunk* → SnapshotEnd → SegmentSeedBegin → seed chunks →
/// StreamStart` followed by journal catch-up from the snapshot's
/// sequence.
///
/// The caller must have sent the resync *verdict* frame — plain
/// `NeedSnapshot` or the divergence verdict `HashMismatch` — directly
/// before calling: the receiver acts on the verdict (archiving its
/// local lineage on `HashMismatch`) and then expects `SnapshotBegin`
/// as the very next frame. Emitting `NeedSnapshot` here would inject
/// a stray frame into the divergent flow.
///
/// `publisher` is called for each encoded control/chunk frame. The TCP
/// path passes `write_all+flush`; the DPDK path passes
/// `queue_send+poll`.
/// Cheap pre-flight for [`snapshot_transfer_with`]: confirm a transfer
/// can start before the caller commits to one on the wire.
///
/// The resync verdict frame (`NeedSnapshot`/`HashMismatch`) makes the
/// replica archive its local lineage and expect `SnapshotBegin` as the
/// very next frame — emitting the verdict and then failing to produce a
/// snapshot strands the replica with its history moved aside (possibly
/// the last copy of segments this primary has pruned). Senders call
/// this *before* the verdict; `snapshot_transfer_with` still
/// re-validates from scratch, so the small window between the two
/// degrades to the rare mid-transfer failure case, not the common
/// no-snapshot-configured one.
///
/// Checks: snapshot file exists, its header parses, and an on-disk
/// segment still contains the snapshot boundary. Reads at most the
/// 64-byte snapshot header plus segment headers — never a body.
pub fn preflight_snapshot_transfer(journal_path: &std::path::Path) -> io::Result<()> {
    use std::io::Read as _;

    let snap_path = journal_path.with_extension("snapshot");
    if !snap_path.exists() {
        return Err(io::Error::other(
            "snapshot transfer required but no snapshot available \
             — set --snapshot-interval-ms to a non-zero value so the shadow exchange writes snapshots",
        ));
    }

    // Fixed 64-byte stack buffer: SnapshotHeader::parse needs at most
    // HEADER_SIZE (v2, 56 bytes); reading the whole file here would pull
    // a potentially huge body that snapshot_transfer_with reads again.
    let mut head = [0u8; 64];
    let mut file = std::fs::File::open(&snap_path)
        .map_err(|e| io::Error::other(format!("open snapshot {}: {e}", snap_path.display())))?;
    let mut filled = 0;
    while filled < head.len() {
        match file.read(&mut head[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => {
                return Err(io::Error::other(format!(
                    "read snapshot header {}: {e}",
                    snap_path.display()
                )));
            }
        }
    }
    let header = crate::snapshot::SnapshotHeader::parse(&head[..filled]).map_err(|e| {
        io::Error::other(format!(
            "snapshot {} not servable for transfer: {e}",
            snap_path.display()
        ))
    })?;

    let files = discover_journal_files(journal_path);
    if containing_segment(&files, header.sequence + 1)?.is_none() {
        return Err(io::Error::other(format!(
            "no on-disk segment contains the snapshot boundary {} — archives pruned past \
             the serving snapshot; retain segments at least as far back as the snapshot",
            header.sequence + 1
        )));
    }
    Ok(())
}

pub fn snapshot_transfer_with<E: AppEvent>(
    journal_path: &std::path::Path,
    publisher: CatchUpPublisher<'_>,
    shutdown: &AtomicBool,
    // The primary's current ack policy, stamped on the post-transfer
    // `StreamStart` (see the protocol docs). Opaque byte at this layer.
    ack_policy: u8,
) -> io::Result<CatchUpResult> {
    use super::protocol::{encode_segment_seed_begin, encode_snapshot_begin, encode_stream_start};

    let snap_path = journal_path.with_extension("snapshot");
    if !snap_path.exists() {
        return Err(io::Error::other(
            "snapshot transfer required but no snapshot available \
             — set --snapshot-interval-ms to a non-zero value so the shadow exchange writes snapshots",
        ));
    }

    let mut send_buf = Vec::with_capacity(64 * 1024 + 128);

    // Read and validate snapshot. The shared header parser enforces the
    // magic *and* the transport-version gate — serving a file the replica's
    // `snapshot::load` would reject (after it already wiped its local state
    // for the rebase) must fail here, on the primary, with the path in the
    // error. v1 files (pre-fencing) parse with epoch 0.
    let snap_data = std::fs::read(&snap_path)
        .map_err(|e| io::Error::other(format!("read snapshot {}: {e}", snap_path.display())))?;
    let header = crate::snapshot::SnapshotHeader::parse(&snap_data).map_err(|e| {
        io::Error::other(format!(
            "snapshot {} not servable for transfer: {e}",
            snap_path.display()
        ))
    })?;
    let snap_sequence = header.sequence;
    let snap_chain_hash = header.chain_hash;
    let snap_epoch = header.epoch;
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

    // Stream the snapshot body (chunks + CRC trailer).
    if !publish_chunked_body(&snap_data, &mut send_buf, &mut *publisher, shutdown)? {
        return Ok(CatchUpResult::Ok(snap_sequence));
    }

    // The snapshot body is fully on the wire; release it before the seed
    // prefix is buffered below so peak sender memory is one body, not two
    // (snapshot + seed can each run to hundreds of MiB).
    drop(snap_data);

    info!(snap_sequence, "snapshot transfer complete");

    // Segment seed: the raw byte prefix (file header through entry
    // `snap_sequence`) of our segment containing `snap_sequence + 1`.
    // Written verbatim as the replica's live segment, it makes the
    // replica's segmentation a byte-copy of ours from birth — chain
    // values comparable, `Rotate` verification valid immediately, no
    // alignment grace period. Header-only (4 KiB) when the snapshot
    // sits exactly on a rotation boundary; operators rotate before
    // seeding to keep it near that.
    let files = discover_journal_files(journal_path);
    let Some((seed_idx, seed_info)) = containing_segment(&files, snap_sequence + 1)? else {
        return Err(io::Error::other(format!(
            "no on-disk segment contains the snapshot boundary {} — archives pruned past \
             the serving snapshot; retain segments at least as far back as the snapshot",
            snap_sequence + 1
        )));
    };
    let seed_path = &files[seed_idx];
    let seed_bytes = melin_journal::segment::read_segment_prefix(seed_path, snap_sequence)
        .map_err(|e| io::Error::other(format!("read seed prefix of {}: {e}", seed_path.display())))?
        .ok_or_else(|| {
            io::Error::other(format!(
                "segment {} ends before snapshot sequence {snap_sequence} — inconsistent \
                 snapshot/journal pair",
                seed_path.display()
            ))
        })?;

    encode_segment_seed_begin(seed_bytes.len() as u64, &mut send_buf);
    publisher(&send_buf)?;
    send_buf.clear();
    if !publish_chunked_body(&seed_bytes, &mut send_buf, &mut *publisher, shutdown)? {
        return Ok(CatchUpResult::Ok(snap_sequence));
    }

    info!(
        snap_sequence,
        seed_len = seed_bytes.len(),
        segment_start = seed_info.starting_sequence,
        "segment seed transferred"
    );

    // StreamStart carries the seeded segment's header identity; the
    // replica cross-checks it against the seed bytes it just verified.
    encode_stream_start(
        snap_sequence,
        seed_info.starting_sequence,
        seed_info.anchor_hash,
        snap_epoch,
        ack_policy,
        &mut send_buf,
    );
    publisher(&send_buf)?;
    send_buf.clear();

    // Catch up from the snapshot's sequence.
    catch_up_from_journal_with::<E>(journal_path, snap_sequence, publisher, shutdown)
}

/// Bridge a replica from journal catch-up into live ring streaming,
/// returning the slot's seeded [`SentHighWater`](super::SentHighWater).
///
/// The journal stage only publishes to a slot's ring while the slot's
/// `active_flag` is set — and the bulk catch-up pass runs with it
/// clear (so a long transfer can't overflow the ring). Entries
/// journaled between the bulk pass's scanner EOF and the flag flip are
/// therefore in *neither* the catch-up stream *nor* the ring; they
/// exist only on disk, and — because the journal stage publishes a
/// batch to the ring *before* it flushes that batch (`pipeline.rs`:
/// publish-to-ring precedes `flush_batch_sync`) — an entry skipped from
/// the ring reaches disk only at its later `fdatasync`. This function
/// closes that disk/ring visibility window:
///
/// 1. **Activate the ring first.** From this store on, a batch whose
///    publish-check sees the flag set is published to the ring (or
///    evicts the replica).
/// 2. **Residual journal pass** from `bulk_catchup_end`: re-reads off
///    the disk the entries that fell into the window — those skipped
///    from the ring because their publish-check preceded the store.
/// 3. **Contiguity drain** (`drain_into_contiguity`): before going
///    live, walk the ring's accumulated chunks. The first chunk that
///    is *ahead* of what has been streamed is the boundary — its lead
///    sequence proves the journal observed the activation, so every
///    entry below it was either skipped from the ring or is still
///    mid-flush. Re-read the journal until those entries land,
///    forwarding them ahead of the chunk, then forward the chunk.
///    Consecutive ring chunks are dense by construction (a batch that
///    can't be published evicts the replica rather than skipping it).
///
/// Under load — when handoffs actually happen — the journal has
/// published chunks since activation, so the drain takes the
/// chunk-present path and closes the gap deterministically; the
/// receiver's contiguity gate never fires. The drain returns `Err`
/// only if the journal stalls past `HANDOFF_BRIDGE_TIMEOUT` while a
/// chunk waits (tear down and reconnect, never ship a gap).
///
/// The one case the sender does *not* close is a skipped entry still
/// mid-flush that is the last before a quiescent gap, with no later
/// ring chunk to expose it: the drain sees an empty ring and goes live
/// (spinning for it would tax every ordinary handoff, since an empty
/// ring at the handoff point is the norm once the disk drains the
/// ring). That rare corner is the receiver's sequence-contiguity gate's
/// job — a reconnect, never a silent hole.
///
/// Owning the `active_flag` store here keeps the activate-before-
/// residual order in one place. Callers must still engage the slot's
/// cursors *before* calling this (the seed-before-active ordering
/// contract, B2 in `ReplicaCursors`).
///
/// Regression context: the 2026-06-07 LAN bench reconnected an evicted
/// replica whose live stream resumed 212 entries past its catch-up end
/// — the pre-bridge handoff went live directly off the bulk pass.
pub fn bridge_catchup_to_live<E: AppEvent>(
    journal_path: &std::path::Path,
    handshake_last_sequence: u64,
    bulk_catchup_end: u64,
    active_flag: &AtomicBool,
    consumer: &mut melin_journal::replication::ReplicationConsumer,
    publisher: CatchUpPublisher<'_>,
    shutdown: &AtomicBool,
) -> io::Result<super::sent::SentHighWater> {
    active_flag.store(true, Ordering::Release);

    let residual_end = match catch_up_from_journal_with::<E>(
        journal_path,
        bulk_catchup_end,
        &mut *publisher,
        shutdown,
    )? {
        CatchUpResult::Ok(end) => end,
        // The bulk pass just streamed up to `bulk_catchup_end`; only a
        // concurrent archive prune could make the residual pass lose
        // its start point. Tear the connection down — the replica
        // re-handshakes and gets routed to snapshot transfer.
        CatchUpResult::NeedSnapshot => {
            return Err(io::Error::other(
                "journal history pruned during catch-up handoff — reconnect for snapshot transfer",
            ));
        }
    };

    let mut sent = super::sent::SentHighWater::seed(handshake_last_sequence, residual_end);

    // Deterministically close the disk/ring visibility gap: the first
    // uncovered ring chunk may start past `sent` because entries skipped
    // from the ring (inactive at publish-check) reach disk only at a
    // later flush. `refill` re-reads the journal forward; `expired`
    // bounds the wait. This makes the receiver gate a backstop rather
    // than the load-bearing guarantee for the common (under-load) case.
    let deadline = Instant::now() + HANDOFF_BRIDGE_TIMEOUT;
    let mut refill = |from: u64, fwd: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> {
        match catch_up_from_journal_with::<E>(journal_path, from, fwd, shutdown)? {
            CatchUpResult::Ok(end) => Ok(end),
            CatchUpResult::NeedSnapshot => Err(io::Error::other(
                "journal history pruned during catch-up handoff backfill",
            )),
        }
    };
    let mut expired = || Instant::now() >= deadline;
    drain_into_contiguity(
        &mut sent,
        consumer,
        &mut *publisher,
        &mut refill,
        &mut expired,
    )?;
    Ok(sent)
}

/// Drain the replication ring into sequence-contiguity with `sent`,
/// back-filling from the journal when the first uncovered chunk starts
/// past `sent`+1 — the deterministic close of the catch-up→live handoff
/// race (see [`bridge_catchup_to_live`]).
///
/// The first uncovered ring chunk's lead sequence ([`peek_first_sequence`])
/// is the boundary the journal stage's own behaviour hands us: it
/// publishes a chunk to the ring only after observing the activation, so
/// every entry below that chunk's first sequence was skipped from the
/// ring and reaches the wire only from disk. If those entries haven't
/// flushed yet, `refill` (re-read the journal forward from `sent`) is
/// retried until they land, then the held chunk is forwarded —
/// contiguous by construction. The chunk's peek is held across the
/// retries: `refill` never touches the consumer, so the two-phase read
/// stays valid and the chunk is never lost.
///
/// An empty ring returns `Ok` immediately — it means the disk has
/// drained the ring, so the next published chunk is contiguous (the rare
/// stranded-mid-flush corner with no later chunk is the receiver gate's
/// job; spinning for it would tax every ordinary handoff).
///
/// Injected for testability without real files or sleeps:
/// - `forward` ships chunk bytes to the wire.
/// - `refill(from, fwd)` re-reads the journal from `from`, forwarding any
///   newly-durable entries via `fwd`, and returns the new high-water
///   (production: [`catch_up_from_journal_with`]). Returns `from`
///   unchanged when nothing new is durable yet.
/// - `expired()` bounds the back-fill wait. It is only consulted while a
///   chunk is present but still ahead (entries below it not yet flushed);
///   on expiry that is a fatal `Err` — the journal has stalled, so tear
///   down and reconnect rather than ship a gap.
fn drain_into_contiguity(
    sent: &mut super::sent::SentHighWater,
    consumer: &mut melin_journal::replication::ReplicationConsumer,
    forward: ForwardFn<'_>,
    refill: RefillFn<'_>,
    expired: &mut dyn FnMut() -> bool,
) -> io::Result<()> {
    loop {
        let Some((meta, data)) = consumer.try_read() else {
            // The ring carries nothing past the handoff point. Under load
            // the journal has published chunks since activation, so an
            // empty ring here means the disk has caught up to the ring and
            // the next published chunk will be contiguous — go live. (The
            // rare corner this does NOT cover: a skipped entry still
            // mid-flush that is the last before a quiescent gap, with no
            // later ring chunk to expose it. That falls to the receiver's
            // contiguity gate — a reconnect, never a silent hole. Spinning
            // here to catch it would tax every ordinary handoff with the
            // full deadline, since an empty ring at the handoff point is
            // the common case once the disk has drained the ring.)
            return Ok(());
        };
        // Control frames (`Rotate`, `ChainCheck`) ride the rings between
        // `InputBatch` chunks. One strictly BEHIND the stream position is
        // stale re-delivery (the disk walk already streamed past it) —
        // drop it. One AT or AHEAD of the position is live and must be
        // forwarded: a rotation that happened after the disk walk ran
        // was never re-announced by it, and dropping the only copy here
        // would leave the replica appending into the wrong segment
        // forever (false divergence at the next chain check). When the
        // back-fill DID also announce the boundary, the wire carries a
        // duplicate — the receiver's exact-position rule and the journal
        // stage's already-rotated check drop it deterministically.
        if peek_frame_tag(data)? != MSG_INPUT_BATCH {
            if meta.end_sequence < sent.get() {
                consumer.commit();
                continue;
            }
            while meta.end_sequence > sent.get() {
                let end = refill(sent.get(), forward)?;
                sent.advance(end);
                if meta.end_sequence > sent.get() {
                    if expired() {
                        return Err(io::Error::other(format!(
                            "catch-up handoff: control frame at boundary {} but the \
                             journal stalled at {} — reconnecting",
                            meta.end_sequence,
                            sent.get()
                        )));
                    }
                    std::thread::yield_now();
                }
            }
            forward(data)?;
            consumer.commit();
            continue;
        }
        if meta.end_sequence <= sent.get() {
            // Wholly covered by the bulk/residual pass — discard.
            consumer.commit();
            continue;
        }
        // First uncovered chunk. Its lead sequence is fixed; hold the peek
        // (refill never touches the consumer) and back-fill from disk
        // until the chunk is contiguous with `sent`.
        let first = peek_first_sequence(data)?;
        while first > sent.get() + 1 {
            let end = refill(sent.get(), forward)?;
            sent.advance(end);
            if first > sent.get() + 1 {
                if expired() {
                    return Err(io::Error::other(format!(
                        "catch-up handoff: ring chunk starts at {first} but the journal \
                         stalled at {} — reconnecting",
                        sent.get()
                    )));
                }
                std::thread::yield_now();
            }
        }
        // Contiguous now. Forward the held chunk unless the back-fill
        // already covered it (the receiver also dedups, but skipping
        // avoids a redundant wire frame).
        if meta.end_sequence > sent.get() {
            forward(data)?;
            sent.advance(meta.end_sequence);
        }
        consumer.commit();
        return Ok(());
    }
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
    /// contiguous with the on-disk history: catch-up has no ENTRIES to
    /// send, which is success. (Regression: the start-file scan read
    /// first *entries* instead of headers, so the empty live looked
    /// like missing history and the sender looped snapshot transfer →
    /// NeedSnapshot forever, never letting the replica register.)
    ///
    /// It does receive the opening-boundary announce: a replica whose
    /// own live segment already starts at 34 dedupes it; one whose
    /// segment still spans 33 adopts it and re-aligns with the
    /// primary's segmentation from 34 onward.
    #[test]
    fn boundary_replica_of_empty_live_catches_up_with_nothing_to_send() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("resumed.journal");
        drop(BufferedWriter::<TestEvent>::create_continuing(&live, 34, [7u8; 32]).unwrap());

        assert!(can_catch_up_from_journal(&live, 33).unwrap());

        let (res, frames) = collect_catchup_frames(&live, 33);
        assert!(
            matches!(res, CatchUpResult::Ok(33)),
            "boundary catch-up must succeed with nothing to send, got {res:?}"
        );
        assert_eq!(
            frames,
            vec![Frame::Rotate(33, [7u8; 32])],
            "opening-boundary announce only — no entry batches"
        );
    }

    /// Decoded view of a catch-up publisher's output: one entry per
    /// published frame.
    #[derive(Debug, PartialEq)]
    enum Frame {
        /// `InputBatch` carrying these sequences.
        Batch(Vec<u64>),
        /// `Rotate { boundary_seq, tail_hash }`.
        Rotate(u64, [u8; 32]),
    }

    /// Run a journal catch-up, classifying every published frame.
    fn collect_catchup_frames(
        live: &std::path::Path,
        last_sequence: u64,
    ) -> (CatchUpResult, Vec<Frame>) {
        use super::super::protocol::{PrimaryMessage, decode_primary_message};

        let shutdown = std::sync::atomic::AtomicBool::new(false);
        let mut frames = Vec::new();
        let mut publish = |buf: &[u8]| -> io::Result<()> {
            let payload = &buf[4..];
            if peek_frame_tag(buf).unwrap() == MSG_INPUT_BATCH {
                let slots = crate::replication_wire::try_decode_input_batch::<TestEvent>(payload)
                    .expect("decodable InputBatch");
                frames.push(Frame::Batch(slots.iter().map(|s| s.sequence).collect()));
            } else {
                match decode_primary_message(payload).expect("decodable control frame") {
                    PrimaryMessage::Rotate {
                        boundary_seq,
                        tail_hash,
                    } => frames.push(Frame::Rotate(boundary_seq, tail_hash)),
                    other => panic!("unexpected control frame in catch-up: {other:?}"),
                }
            }
            Ok(())
        };
        let res =
            catch_up_from_journal_with::<TestEvent>(live, last_sequence, &mut publish, &shutdown)
                .unwrap();
        (res, frames)
    }

    /// Catch-up must announce a rotation at every segment transition it
    /// walks, carrying the next segment's header anchor (= the previous
    /// segment's tail) — that's what lets a catching-up replica
    /// reproduce the primary's segment boundaries exactly.
    #[test]
    fn catchup_announces_rotation_at_each_segment_transition() {
        let dir = tempfile::tempdir().unwrap();
        let live = three_segment_journal(dir.path());

        let anchor2 =
            melin_journal::segment::read_header_info(&dir.path().join("j.journal.000002"))
                .unwrap()
                .anchor_hash;
        let anchor_live = melin_journal::segment::read_header_info(&live)
            .unwrap()
            .anchor_hash;

        // Fresh replica: full walk from the lineage origin. The origin's
        // own opening is not a rotation — no leading Rotate.
        let (res, frames) = collect_catchup_frames(&live, 0);
        assert!(matches!(res, CatchUpResult::Ok(3)));
        assert_eq!(
            frames,
            vec![
                Frame::Batch(vec![1]),
                Frame::Rotate(1, anchor2),
                Frame::Batch(vec![2]),
                Frame::Rotate(2, anchor_live),
                Frame::Batch(vec![3]),
            ]
        );
    }

    /// A replica reconnecting exactly at a segment boundary missed the
    /// live-stream Rotate for it (it journaled the boundary entry, then
    /// disconnected before the announce). Catch-up must re-announce that
    /// boundary before streaming the next segment's entries.
    #[test]
    fn catchup_reannounces_boundary_for_replica_sitting_on_it() {
        let dir = tempfile::tempdir().unwrap();
        let live = three_segment_journal(dir.path());

        let anchor2 =
            melin_journal::segment::read_header_info(&dir.path().join("j.journal.000002"))
                .unwrap()
                .anchor_hash;
        let anchor_live = melin_journal::segment::read_header_info(&live)
            .unwrap()
            .anchor_hash;

        let (res, frames) = collect_catchup_frames(&live, 1);
        assert!(matches!(res, CatchUpResult::Ok(3)));
        assert_eq!(
            frames,
            vec![
                Frame::Rotate(1, anchor2),
                Frame::Batch(vec![2]),
                Frame::Rotate(2, anchor_live),
                Frame::Batch(vec![3]),
            ]
        );
    }

    /// End-to-end snapshot transfer with segment seeding: the transfer
    /// must ship `NeedSnapshot → snapshot → segment seed → StreamStart →
    /// catch-up`, the seed must be the exact byte prefix of the
    /// primary's containing segment, and a replica writer opened over
    /// the seed (recovery's resume path) must report the snapshot's
    /// chain hash — proving the seeded replica is born aligned.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn snapshot_transfer_seeds_byte_identical_containing_segment() {
        use crate::cursors::WireSeq;
        use crate::test_support::TestApp;
        use melin_journal::JournalWrite;

        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("p.journal");

        // Primary journal: [1..2] archived, [3..5] live. Snapshot taken
        // mid-live-segment at sequence 4 — the worst case for seeding
        // (the seed must carry the live header plus entries 3..=4).
        let mut w = melin_journal::BufferedWriter::<TestEvent>::create(&live).unwrap();
        let mut chain_at_4 = [0u8; 32];
        for v in 1..=5u64 {
            w.append(&JournalEvent::App(TestEvent::Add(v))).unwrap();
            if v == 2 {
                w.rotate_segment().unwrap();
            }
            if v == 4 {
                chain_at_4 = w.chain_hash().unwrap();
            }
        }
        drop(w);
        let snap_path = live.with_extension("snapshot");
        crate::snapshot::save::<TestApp>(
            &TestApp::new(),
            WireSeq::new(4),
            chain_at_4,
            7,
            &snap_path,
        )
        .unwrap();

        // Drive the transfer, collecting every frame.
        let shutdown = AtomicBool::new(false);
        let mut frames: Vec<Vec<u8>> = Vec::new();
        let mut publish = |buf: &[u8]| -> io::Result<()> {
            frames.push(buf.to_vec());
            Ok(())
        };
        // Opaque at this layer (the policy enum lives in the server
        // crate) — a distinctive non-default byte so the StreamStart
        // assertion below proves it is carried through rather than
        // defaulted.
        const ACK_POLICY: u8 = 2;
        let res = snapshot_transfer_with::<TestEvent>(&live, &mut publish, &shutdown, ACK_POLICY)
            .unwrap();
        assert_eq!(res, CatchUpResult::Ok(5), "catch-up must reach the tip");

        // Walk the frame sequence, reassembling the two chunked bodies.
        use super::super::protocol::{PrimaryMessage, decode_primary_message};
        let decode = |f: &Vec<u8>| decode_primary_message(&f[4..]).unwrap();
        // The verdict frame (NeedSnapshot/HashMismatch) is the caller's
        // responsibility — the transfer itself must open with
        // SnapshotBegin.
        let mut it = frames.iter();
        let snap_len = match decode(it.next().unwrap()) {
            PrimaryMessage::SnapshotBegin {
                snapshot_len,
                snap_sequence,
                snap_chain_hash,
            } => {
                assert_eq!(snap_sequence, 4);
                assert_eq!(snap_chain_hash, chain_at_4);
                snapshot_len
            }
            other => panic!("expected SnapshotBegin, got {other:?}"),
        };
        let collect_body = |it: &mut std::slice::Iter<'_, Vec<u8>>| -> Vec<u8> {
            let mut body = Vec::new();
            loop {
                match decode(it.next().expect("chunks then end")) {
                    PrimaryMessage::SnapshotChunk(data) => body.extend_from_slice(&data),
                    PrimaryMessage::SnapshotEnd { crc32c } => {
                        assert_eq!(crc32c, ::crc32c::crc32c(&body), "body CRC");
                        return body;
                    }
                    other => panic!("expected SnapshotChunk/End, got {other:?}"),
                }
            }
        };
        let snap_body = collect_body(&mut it);
        assert_eq!(snap_body.len() as u64, snap_len);

        let seed_len = match decode(it.next().unwrap()) {
            PrimaryMessage::SegmentSeedBegin { seed_len } => seed_len,
            other => panic!("expected SegmentSeedBegin, got {other:?}"),
        };
        let seed_body = collect_body(&mut it);
        assert_eq!(seed_body.len() as u64, seed_len);

        // The seed is the exact byte prefix of the live (containing)
        // segment, and StreamStart announces that segment's identity.
        let live_bytes = std::fs::read(&live).unwrap();
        assert_eq!(seed_body[..], live_bytes[..seed_body.len()]);
        let live_info = melin_journal::segment::read_header_info(&live).unwrap();
        match decode(it.next().unwrap()) {
            PrimaryMessage::StreamStart {
                start_sequence,
                segment_start_sequence,
                anchor_hash,
                epoch,
                ack_policy,
            } => {
                assert_eq!(start_sequence, 4);
                assert_eq!(segment_start_sequence, live_info.starting_sequence);
                assert_eq!(anchor_hash, live_info.anchor_hash);
                assert_eq!(epoch, 7, "snapshot's epoch rides StreamStart");
                assert_eq!(
                    ack_policy, ACK_POLICY,
                    "primary's ack policy rides StreamStart"
                );
            }
            other => panic!("expected StreamStart, got {other:?}"),
        }

        // Replica-side reconstruction: write the seed verbatim, open for
        // append at the snapshot position — the rebuilt chain must equal
        // the snapshot's hash (born aligned).
        let replica = dir.path().join("r.journal");
        std::fs::write(&replica, &seed_body).unwrap();
        let replica_writer =
            melin_journal::BufferedWriter::<TestEvent>::open_append(&replica, 4, seed_len).unwrap();
        assert_eq!(replica_writer.chain_hash().unwrap(), chain_at_4);
        assert_eq!(replica_writer.segment_starting_sequence(), 3);
        assert_eq!(replica_writer.next_sequence(), 5);
    }

    /// The boundary re-announce must be judged from the segment header
    /// (`starting_sequence > 1`), not from a predecessor file being
    /// present on disk: after the primary prunes archives, a replica
    /// sitting exactly on the pruned boundary still needs the Rotate.
    /// (Regression: the guard was `file_idx > 0`, so the announce was
    /// silently skipped when the walk started at the lineage's oldest
    /// surviving file.)
    #[test]
    fn catchup_reannounces_boundary_after_predecessor_pruned() {
        let dir = tempfile::tempdir().unwrap();
        let live = three_segment_journal(dir.path());
        // Prune everything before the live segment (which starts at 3).
        std::fs::remove_file(dir.path().join("j.journal.000001")).unwrap();
        std::fs::remove_file(dir.path().join("j.journal.000002")).unwrap();
        let anchor_live = melin_journal::segment::read_header_info(&live)
            .unwrap()
            .anchor_hash;

        let (res, frames) = collect_catchup_frames(&live, 2);
        assert!(matches!(res, CatchUpResult::Ok(3)));
        assert_eq!(
            frames,
            vec![Frame::Rotate(2, anchor_live), Frame::Batch(vec![3])]
        );
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

/// Unit tests for [`drain_into_contiguity`] — the deterministic close of
/// the catch-up→live handoff race. The disk re-read (`refill`) and the
/// clock (`expired`) are injected, so the disk/ring visibility race is
/// reproduced exactly without real files, threads, or sleeps.
#[cfg(test)]
mod drain_into_contiguity_tests {
    use super::*;
    use crate::pipeline::InputSlot;
    use crate::test_support::TestEvent;
    use melin_journal::JournalEvent;
    use melin_journal::replication::{
        ReplicationConsumer, ReplicationProducer, build_replication_ring,
    };
    use std::cell::{Cell, RefCell};

    fn slot(seq: u64) -> InputSlot<TestEvent> {
        InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: seq,
            sequence: seq,
            timestamp_ns: 0,
            event: JournalEvent::App(TestEvent::Add(seq)),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        }
    }

    /// Encode a one-or-more-slot `InputBatch` frame (the ring-chunk shape).
    fn frame(seqs: &[u64]) -> Vec<u8> {
        let slots: Vec<InputSlot<TestEvent>> = seqs.iter().map(|&s| slot(s)).collect();
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf);
        buf
    }

    fn ring() -> (ReplicationProducer, ReplicationConsumer) {
        let (producer, mut consumers) = build_replication_ring(1, 8);
        (producer, consumers.pop().expect("one consumer"))
    }

    /// A contiguous first chunk is forwarded directly — no disk re-read.
    #[test]
    fn contiguous_first_chunk_forwards_without_refill() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&frame(&[101, 102]), 102);

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        let forwarded = RefCell::new(Vec::<u64>::new());
        let refill_calls = Cell::new(0usize);

        let mut forward = |data: &[u8]| -> io::Result<()> {
            forwarded
                .borrow_mut()
                .push(peek_first_sequence(data).unwrap());
            Ok(())
        };
        let mut refill =
            |from: u64, _fwd: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> {
                refill_calls.set(refill_calls.get() + 1);
                Ok(from)
            };
        let mut expired = || false;

        drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect("contiguous chunk drains cleanly");

        assert_eq!(*forwarded.borrow(), vec![101]);
        assert_eq!(sent.get(), 102);
        assert_eq!(
            refill_calls.get(),
            0,
            "no back-fill needed for a contiguous chunk"
        );
    }

    /// A chunk wholly covered by the bulk pass is discarded; the next
    /// contiguous chunk is forwarded.
    #[test]
    fn covered_chunk_is_skipped_then_contiguous_chunk_forwards() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&frame(&[98, 99]), 99); // covered (<= sent 100)
        producer.publish(&frame(&[101, 102]), 102);

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        let forwarded = RefCell::new(Vec::<u64>::new());
        let mut forward = |data: &[u8]| -> io::Result<()> {
            forwarded
                .borrow_mut()
                .push(peek_first_sequence(data).unwrap());
            Ok(())
        };
        let mut refill =
            |from: u64, _: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> { Ok(from) };
        let mut expired = || false;

        drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect("drains cleanly");

        assert_eq!(*forwarded.borrow(), vec![101], "covered chunk not re-sent");
        assert_eq!(sent.get(), 102);
    }

    /// The race: the first uncovered ring chunk starts past `sent`+1
    /// because entries 101–102 were skipped from the ring and not yet
    /// flushed. The back-fill re-reads the journal until they land, then
    /// forwards the held chunk — dense, in order.
    #[test]
    fn gap_backfills_from_disk_then_forwards_the_held_chunk() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&frame(&[103, 104]), 104); // first=103, gap over 101,102

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        let forwarded = RefCell::new(Vec::<u64>::new());
        let refill_calls = Cell::new(0usize);

        let mut forward = |data: &[u8]| -> io::Result<()> {
            forwarded
                .borrow_mut()
                .push(peek_first_sequence(data).unwrap());
            Ok(())
        };
        // Call 0: 101–102 not durable yet (no progress). Call 1: they
        // flushed — forward each and advance to 102.
        let mut refill =
            |from: u64, fwd: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> {
                let n = refill_calls.get();
                refill_calls.set(n + 1);
                if n == 0 {
                    return Ok(from);
                }
                fwd(&frame(&[101]))?;
                fwd(&frame(&[102]))?;
                Ok(102)
            };
        let mut expired = || false;

        drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect("gap is back-filled, not fatal");

        assert_eq!(
            *forwarded.borrow(),
            vec![101, 102, 103],
            "back-filled entries precede the held ring chunk"
        );
        assert_eq!(sent.get(), 104);
        assert_eq!(
            refill_calls.get(),
            2,
            "retried once while pending, succeeded on the second"
        );
    }

    /// If the gap never resolves (the journal stalls), the handoff fails
    /// fatally on the deadline so the connection tears down and the
    /// receiver gate / reconnect take over. Nothing past the gap is
    /// forwarded.
    #[test]
    fn gap_that_never_flushes_times_out_fatally() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&frame(&[103, 104]), 104);

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        let forwarded = RefCell::new(Vec::<u64>::new());
        let exp_calls = Cell::new(0usize);

        let mut forward = |data: &[u8]| -> io::Result<()> {
            forwarded
                .borrow_mut()
                .push(peek_first_sequence(data).unwrap());
            Ok(())
        };
        let mut refill =
            |from: u64, _: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> { Ok(from) };
        let mut expired = || {
            exp_calls.set(exp_calls.get() + 1);
            exp_calls.get() >= 3
        };

        let err = drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect_err("a never-flushing gap must be fatal");
        assert!(err.to_string().contains("stalled"), "got: {err}");
        assert!(
            forwarded.borrow().is_empty(),
            "nothing past the gap reaches the wire"
        );
        assert_eq!(sent.get(), 100, "high-water never advances past the gap");
    }

    /// An empty ring goes live immediately (Ok) — the disk has drained
    /// the ring, so the next chunk whenever traffic resumes is contiguous.
    /// No back-fill, no deadline spin: `refill`/`expired` must not even be
    /// consulted (the old design spun here and taxed every handoff).
    #[test]
    fn empty_ring_goes_live_immediately() {
        let (_producer, mut consumer) = ring();

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        let forwarded = RefCell::new(Vec::<u64>::new());

        let mut forward = |data: &[u8]| -> io::Result<()> {
            forwarded
                .borrow_mut()
                .push(peek_first_sequence(data).unwrap());
            Ok(())
        };
        let mut refill = |_: u64, _: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> {
            panic!("empty ring must not back-fill")
        };
        let mut expired = || panic!("empty ring must not consult the deadline");

        drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect("empty ring goes live");
        assert!(forwarded.borrow().is_empty());
        assert_eq!(sent.get(), 100);
    }

    /// Encode a `Rotate` control frame the way the journal stage
    /// publishes it to the rings (boundary as the chunk's end_sequence).
    fn rotate_chunk(boundary: u64) -> Vec<u8> {
        let mut buf = Vec::new();
        super::encode_rotate(boundary, &[0x52u8; 32], &mut buf);
        buf
    }

    /// A Rotate chunk wholly covered by the bulk pass is discarded like
    /// any covered chunk (the catch-up walk already re-announced it).
    #[test]
    fn covered_rotate_chunk_is_discarded() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&rotate_chunk(99), 99); // covered (<= sent 100)
        producer.publish(&frame(&[101, 102]), 102);

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        let forwarded = RefCell::new(Vec::<u64>::new());
        let mut forward = |data: &[u8]| -> io::Result<()> {
            forwarded
                .borrow_mut()
                .push(peek_first_sequence(data).unwrap());
            Ok(())
        };
        let mut refill =
            |from: u64, _: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> { Ok(from) };
        let mut expired = || false;

        drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect("covered rotate chunk drains cleanly");

        assert_eq!(
            *forwarded.borrow(),
            vec![101],
            "rotate chunk never forwarded"
        );
        assert_eq!(sent.get(), 102);
    }

    /// An uncovered Rotate chunk is dropped only after the disk
    /// back-fill has covered its boundary — the back-fill re-announces
    /// the rotation at the segment transition, so forwarding the held
    /// copy would announce the same boundary twice.
    #[test]
    fn uncovered_rotate_chunk_waits_for_backfill_then_forwards() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&rotate_chunk(102), 102); // ahead of sent=100

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        #[derive(Debug, PartialEq)]
        enum Fwd {
            Batch(u64),
            Control(u64),
        }
        let forwarded = RefCell::new(Vec::<Fwd>::new());
        let refill_calls = Cell::new(0usize);

        let mut forward = |data: &[u8]| -> io::Result<()> {
            let fwd = if peek_frame_tag(data).unwrap() == MSG_INPUT_BATCH {
                Fwd::Batch(peek_first_sequence(data).unwrap())
            } else {
                match super::super::protocol::decode_primary_message(&data[4..]).unwrap() {
                    super::super::protocol::PrimaryMessage::Rotate { boundary_seq, .. } => {
                        Fwd::Control(boundary_seq)
                    }
                    other => panic!("unexpected control frame: {other:?}"),
                }
            };
            forwarded.borrow_mut().push(fwd);
            Ok(())
        };
        let mut refill =
            |_from: u64, fwd: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> {
                refill_calls.set(refill_calls.get() + 1);
                fwd(&frame(&[101, 102]))?;
                Ok(102)
            };
        let mut expired = || false;

        drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect("rotate chunk resolves via back-fill");

        // Entries back-filled first, then the rotate FORWARDED — the
        // walk that produced the back-fill predates the rotation, so
        // this ring copy may be the replica's only chance to learn the
        // boundary. (When the back-fill did also announce it, the
        // receiver's exact-position rule drops the duplicate.)
        assert_eq!(
            *forwarded.borrow(),
            vec![Fwd::Batch(101), Fwd::Control(102)],
        );
        assert_eq!(sent.get(), 102);
        assert_eq!(refill_calls.get(), 1);
    }

    /// A rotate chunk sitting exactly at the handoff position (boundary
    /// == sent) is live, not covered: the rotation happened at the
    /// catch-up end after the disk walk ran, so nothing else will ever
    /// re-announce it. It must be forwarded — without back-fill.
    /// (Regression: the covered-chunk discard used `<=` and silently
    /// dropped it, desynchronizing the replica's segmentation.)
    #[test]
    fn boundary_equal_rotate_chunk_is_forwarded() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&rotate_chunk(100), 100); // exactly at sent=100

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        let forwarded_controls = Cell::new(0usize);

        let mut forward = |data: &[u8]| -> io::Result<()> {
            assert_ne!(
                peek_frame_tag(data).unwrap(),
                MSG_INPUT_BATCH,
                "only the rotate chunk should be forwarded"
            );
            forwarded_controls.set(forwarded_controls.get() + 1);
            Ok(())
        };
        let mut refill = |_: u64, _: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> {
            panic!("boundary-equal rotate must not back-fill")
        };
        let mut expired = || panic!("boundary-equal rotate must not consult the deadline");

        drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect("boundary-equal rotate forwards cleanly");

        assert_eq!(forwarded_controls.get(), 1, "rotate chunk forwarded");
        assert_eq!(
            sent.get(),
            100,
            "control frames do not advance the high-water"
        );
    }

    /// Covered chunks are skipped until the ring drains to empty, then the
    /// handoff goes live — the covered-skip loop must terminate at the
    /// empty ring without back-filling or spinning.
    #[test]
    fn covered_chunks_then_empty_goes_live() {
        let (mut producer, mut consumer) = ring();
        producer.publish(&frame(&[98, 99]), 99);
        producer.publish(&frame(&[100]), 100);

        let mut sent = super::super::sent::SentHighWater::seed(100, 100);
        let forwarded = RefCell::new(Vec::<u64>::new());
        let mut forward = |data: &[u8]| -> io::Result<()> {
            forwarded
                .borrow_mut()
                .push(peek_first_sequence(data).unwrap());
            Ok(())
        };
        let mut refill = |_: u64, _: &mut dyn FnMut(&[u8]) -> io::Result<()>| -> io::Result<u64> {
            panic!("no back-fill expected")
        };
        let mut expired = || panic!("no deadline expected");

        drain_into_contiguity(
            &mut sent,
            &mut consumer,
            &mut forward,
            &mut refill,
            &mut expired,
        )
        .expect("covered chunks drain then go live");
        assert!(forwarded.borrow().is_empty(), "all chunks were covered");
        assert_eq!(sent.get(), 100);
    }
}
