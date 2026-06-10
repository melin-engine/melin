//! Generic journal-plus-application wrapper.
//!
//! Holds an `A: Application` and a [`JournalWriter<A::Event>`]; handles
//! the startup paths a server cares about:
//!
//! - [`create`]: fresh journal, fresh app.
//! - [`recover`]: replay the journal into a fresh app.
//! - [`recover_from_snapshot`]: restore from snapshot, replay the
//!   post-snapshot delta.
//! - [`save_snapshot`]: write the current state via the generic
//!   [`crate::snapshot`] framing.
//! - [`rotate_segment`]: archive the live journal segment and start a
//!   fresh one. Snapshots are written separately by the shadow stage.
//! - [`into_parts`]: hand the (app, writer) pair to the disruptor
//!   pipeline.
//!
//! This crate is application-agnostic — the journal replay goes through
//! `Application::apply` / `Application::tick`, and the snapshot payload
//! is whatever bytes `A::snapshot`/`A::restore` round-trip.

use std::path::Path;

use melin_app::{Application, ApplyCtx};
use melin_journal::{JournalError, JournalEvent, JournalReader, JournalWrite};

use crate::snapshot;

/// Error surfaced by [`JournaledApp::*`] — wraps journal I/O errors and
/// snapshot framing errors under one umbrella.
#[derive(Debug)]
pub enum JournaledAppError {
    Journal(JournalError),
    Snapshot(snapshot::SnapshotError),
    Io(std::io::Error),
    /// The journal's history does not reach the snapshot's recorded
    /// anchor sequence — recovery cannot reconcile the two. Causes
    /// include a journal restored from before the snapshot was taken
    /// (audit-trail loss), a snapshot copied from a different
    /// cluster/run, or archive trimming that removed the segment
    /// holding the anchor. Recovery refuses to proceed because the
    /// engine state would silently outrun the journal.
    ///
    /// "Reaches" counts both observed entries and header evidence: a
    /// segment whose header `starting_sequence` is `S + 1` proves
    /// history through `S` existed, so a snapshot anchored exactly at
    /// a rotation boundary is accepted even when the next segment is
    /// still empty.
    SnapshotAnchorMissing {
        snap_sequence: u64,
        journal_last_seq: u64,
    },
    /// The snapshot's recorded chain hash at its anchor sequence does
    /// not match the journal's chain hash at the same sequence — the
    /// snapshot was taken on a journal that has since diverged (e.g.
    /// snapshot from one cluster paired with another cluster's journal,
    /// or a journal with tampered entries). Detected during replay when
    /// the entry at the anchor sequence is observed, or — for a
    /// snapshot anchored exactly at a rotation boundary — by comparing
    /// against the successor segment's header anchor (which *is* the
    /// chain value at that boundary).
    SnapshotChainMismatch {
        snap_sequence: u64,
        expected_chain_hash: [u8; 32],
        actual_chain_hash: [u8; 32],
    },
    /// The oldest available segment begins after the history start that
    /// recovery requires (sequence 1 without a snapshot; the snapshot's
    /// anchor + 1 with one). Replaying would silently reconstruct
    /// partial state — events before `first_segment_start` exist in the
    /// lineage (the header proves it) but are not on disk and not
    /// covered by a snapshot. Causes: archives trimmed to cold storage
    /// without a covering snapshot, a deleted snapshot file on a node
    /// whose journal was created mid-history (e.g. a snapshot-seeded
    /// replica), or a snapshot/journal pair from different points in
    /// the lineage. Recovery refuses to proceed; restore the missing
    /// archives or a snapshot covering them.
    MissingHistoryPrefix {
        first_segment_start: u64,
        required_floor: u64,
    },
}

impl std::fmt::Display for JournaledAppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Journal(e) => write!(f, "journal: {e}"),
            Self::Snapshot(e) => write!(f, "snapshot: {e}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
            Self::SnapshotAnchorMissing {
                snap_sequence,
                journal_last_seq,
            } => write!(
                f,
                "snapshot anchor at sequence {snap_sequence} not present in journal \
                 (journal reaches {journal_last_seq}) — stale journal, mismatched \
                 snapshot, or trimmed archive",
            ),
            Self::SnapshotChainMismatch {
                snap_sequence,
                expected_chain_hash,
                actual_chain_hash,
            } => write!(
                f,
                "snapshot chain hash mismatch at sequence {snap_sequence}: \
                 snapshot recorded {} but journal computes {} — mismatched \
                 snapshot/journal pair (different cluster, divergent history, \
                 or tampered journal)",
                melin_journal::error::hex_prefix(expected_chain_hash),
                melin_journal::error::hex_prefix(actual_chain_hash),
            ),
            Self::MissingHistoryPrefix {
                first_segment_start,
                required_floor,
            } => write!(
                f,
                "journal history begins at sequence {first_segment_start} but recovery \
                 requires it to begin at or before {required_floor} — archives trimmed \
                 without a covering snapshot, or snapshot/journal from different points \
                 in the lineage",
            ),
        }
    }
}

impl std::error::Error for JournaledAppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(e) => Some(e),
            Self::Snapshot(e) => Some(e),
            Self::Io(e) => Some(e),
            Self::SnapshotAnchorMissing { .. } => None,
            Self::SnapshotChainMismatch { .. } => None,
            Self::MissingHistoryPrefix { .. } => None,
        }
    }
}

impl From<JournalError> for JournaledAppError {
    fn from(e: JournalError) -> Self {
        Self::Journal(e)
    }
}
impl From<snapshot::SnapshotError> for JournaledAppError {
    fn from(e: snapshot::SnapshotError) -> Self {
        Self::Snapshot(e)
    }
}
impl From<std::io::Error> for JournaledAppError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// A journaled application: the matching engine (or any other
/// `Application`) paired with a durable journal writer positioned at
/// the next free sequence. Generic over `W` — the caller picks the
/// concrete writer type (typically by dispatching on a runtime mode
/// flag at the boot site) and threads it through.
pub struct JournaledApp<A: Application, W: JournalWrite<A::Event>> {
    app: A,
    writer: W,
    /// Fencing epoch recovered from the snapshot + journal replay (the
    /// highest `EpochBump` observed, or the snapshot's epoch if newer, or
    /// `0` for a genesis node). The boot site reads this via
    /// [`Self::recovered_epoch`] to seed the node's `FenceState` before
    /// the pipeline starts. Not part of `(A, W)` — extracted separately.
    recovered_epoch: u64,
}

impl<A: Application, W: JournalWrite<A::Event>> JournaledApp<A, W> {
    /// Create a new journaled app with a fresh journal file. The caller
    /// supplies the app so production builds can pick an appropriately
    /// pre-sized constructor (e.g. `Exchange::with_capacity()`) rather
    /// than relying on `Default`.
    pub fn create(app: A, journal_path: &Path) -> Result<Self, JournaledAppError> {
        let writer = W::create(journal_path)?;
        // Genesis node — no prior promotion, so epoch starts at 0.
        Ok(Self {
            app,
            writer,
            recovered_epoch: 0,
        })
    }

    /// Recover from an existing journal. Replays every archived segment
    /// in monotonic order, then the live segment, into the caller-
    /// supplied empty app, then reopens the writer for appending.
    pub fn recover(app: A, journal_path: &Path) -> Result<Self, JournaledAppError> {
        Self::recover_inner(app, journal_path, None)
    }

    /// Recover from a snapshot plus a journal directory.
    ///
    /// Loads the snapshot to restore state, then replays journal entries
    /// strictly after the snapshot's recorded sequence — across all
    /// archived segments and the live segment.
    pub fn recover_from_snapshot(
        snapshot_path: &Path,
        journal_path: &Path,
    ) -> Result<Self, JournaledAppError> {
        let (app, snap_sequence, snap_chain_hash, snap_epoch) = snapshot::load::<A>(snapshot_path)?;
        Self::recover_inner(
            app,
            journal_path,
            Some((snap_sequence, snap_chain_hash, snap_epoch)),
        )
    }

    /// Shared multi-segment recovery driver.
    ///
    /// `snapshot` carries `(sequence, chain_hash, epoch)` when the caller
    /// has already restored from a snapshot; events with `seq <= sequence`
    /// are skipped during replay but still walked so per-segment chain
    /// validation runs. The epoch seeds the recovered-epoch accumulator.
    ///
    /// Cross-segment continuity is enforced before each segment is
    /// replayed: its header anchor must equal the previous segment's
    /// tail chain hash ([`JournalError::SegmentChainBreak`] otherwise),
    /// and its header `starting_sequence` must continue the sequence
    /// space without gap or overlap.
    fn recover_inner(
        mut app: A,
        journal_path: &Path,
        snapshot: Option<(u64, [u8; 32], u64)>,
    ) -> Result<Self, JournaledAppError> {
        let archives = melin_journal::segment::list_archives(journal_path)?;
        // Highest fencing epoch observed during replay. Seeded from the
        // snapshot's epoch (replay only walks entries strictly after the
        // snapshot, so an `EpochBump` folded into the snapshot is invisible
        // here) and raised by each replayed `EpochBump`.
        let mut recovered_epoch = snapshot.map(|(_, _, e)| e).unwrap_or(0);

        let has_snapshot = snapshot.is_some();
        let snap_sequence = snapshot.map(|(s, _, _)| s).unwrap_or(0);
        // Expected chain hash at the snapshot's anchor sequence. Compared
        // inside `replay_segment` when the anchor entry is observed, and
        // against a successor segment's header anchor when the snapshot
        // is anchored exactly at a rotation boundary.
        let snap_chain_check: Option<[u8; 32]> = snapshot.map(|(_, h, _)| h);

        let mut reports: Vec<A::Report> = Vec::new();
        let mut last_drain_ns: u64 = 0;
        // The oldest walked segment must start at or before this floor,
        // or a prefix of history is provably missing (see
        // [`Self::MissingHistoryPrefix`]).
        let history_floor = snap_sequence + 1;
        // Tail chain hash carried forward across segments. `None` means
        // no boundary check has anything to compare against yet — the
        // very first segment we walk has no predecessor in this run.
        let mut prev_tail_hash: Option<[u8; 32]> = None;
        // Sequence the next segment's header must start at. `None` for
        // the first walked segment (no predecessor in this run).
        let mut expected_start: Option<u64> = None;
        // Highest sequence observed across walked archives. Used to seed
        // a synthesized live segment when a crash interrupted rotation
        // between the live → archive rename and the new live file's
        // creation.
        let mut last_seq_seen: u64 = snap_sequence;
        // Highest sequence the journal's history provably reaches:
        // observed entries, plus header evidence (a segment starting at
        // `S + 1` proves history through `S`). Used for the
        // [`SnapshotAnchorMissing`] check. Seeded at 0 (not
        // `snap_sequence`) so the error reports the true observed tail
        // when a stale journal never reaches the anchor.
        let mut journal_max_seq: u64 = 0;

        // --- Walk each sealed archive in monotonic order ---
        for (idx, archive_path) in &archives {
            let mut reader = JournalReader::<A::Event>::open(archive_path)?;
            // Verify lineage continuity from the header alone, before
            // any of this segment's events reach the application.
            verify_segment_link(*idx, &reader, prev_tail_hash, expected_start, history_floor)?;
            verify_boundary_snapshot_anchor(&reader, snap_sequence, snap_chain_check)?;
            replay_segment(
                &mut reader,
                &mut app,
                snap_sequence,
                snap_chain_check,
                &mut last_drain_ns,
                &mut recovered_epoch,
                &mut reports,
                /* allow_partial_tail = */ false,
            )?;
            // Carry forward only when this segment actually had a chain
            // (hash-chain feature on). Otherwise leave `prev_tail_hash`
            // unchanged so the next boundary still gets a meaningful
            // compare target.
            if let Some(h) = reader.chain_hash() {
                prev_tail_hash = Some(h);
            }
            journal_max_seq = journal_max_seq.max(reader.starting_sequence().saturating_sub(1));
            if let Some(seq) = reader.last_sequence() {
                last_seq_seen = last_seq_seen.max(seq);
                journal_max_seq = journal_max_seq.max(seq);
            }
            expected_start = Some(
                reader
                    .last_sequence()
                    .map(|s| s + 1)
                    .unwrap_or_else(|| reader.starting_sequence()),
            );
        }

        // --- Walk the live segment, if it exists ---
        let live_exists = journal_path.exists();
        if !live_exists {
            // Phase B recovery: rotation crashed between
            // [`crate::segment::archive_live`] (the live → archive
            // rename) and `JournalWriter::create_continuing` (opening a
            // fresh live). The just-archived segment is intact and was
            // replayed above, so the application state is consistent up
            // through `last_seq_seen`. Synthesize a new live segment
            // continuing from there so the pipeline has somewhere to
            // append.
            //
            // When there are no archives at all, the caller (the server's
            // bootstrap) is responsible for handling the snapshot-only
            // case — that path needs more context (the snapshot's
            // chain hash, choice of starting sequence) than this
            // application-agnostic recovery driver can supply.
            if archives.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no live journal and no archives — nothing to recover",
                )
                .into());
            }
            // Archives-only case: validate the snapshot's anchor sits
            // inside the journal we walked before synthesizing the new
            // live segment. See [`Self::SnapshotAnchorMissing`] for the
            // failure modes this catches.
            if has_snapshot && snap_sequence > 0 && journal_max_seq < snap_sequence {
                return Err(JournaledAppError::SnapshotAnchorMissing {
                    snap_sequence,
                    journal_last_seq: journal_max_seq,
                });
            }
            let anchor = prev_tail_hash.unwrap_or([0u8; 32]);
            let writer = W::create_continuing(journal_path, last_seq_seen + 1, anchor)?;
            return Ok(Self {
                app,
                writer,
                recovered_epoch,
            });
        }

        let mut reader = JournalReader::<A::Event>::open(journal_path)?;
        verify_segment_link(0, &reader, prev_tail_hash, expected_start, history_floor)?;
        verify_boundary_snapshot_anchor(&reader, snap_sequence, snap_chain_check)?;
        // The live segment may have a partial-tail crash: replay loop
        // tolerates `SequenceGap` by stopping early, mirroring legacy
        // behaviour.
        replay_segment(
            &mut reader,
            &mut app,
            snap_sequence,
            snap_chain_check,
            &mut last_drain_ns,
            &mut recovered_epoch,
            &mut reports,
            /* allow_partial_tail = */ true,
        )?;

        // An empty live segment resumes at its header's starting
        // sequence; a non-empty one after its last entry.
        let last_seq = reader
            .last_sequence()
            .unwrap_or_else(|| reader.starting_sequence().saturating_sub(1));
        let valid_end = reader.valid_file_end();
        journal_max_seq = journal_max_seq.max(reader.starting_sequence().saturating_sub(1));
        if let Some(seq) = reader.last_sequence() {
            journal_max_seq = journal_max_seq.max(seq);
        }

        // Final validation: the journal's history must provably reach
        // the snapshot's anchor sequence — via an observed entry or via
        // a segment header starting at `anchor + 1`.
        if has_snapshot && snap_sequence > 0 && journal_max_seq < snap_sequence {
            return Err(JournaledAppError::SnapshotAnchorMissing {
                snap_sequence,
                journal_last_seq: journal_max_seq,
            });
        }

        let writer = W::open_append(journal_path, last_seq, valid_end)?;

        // The writer rebuilt its chain self-containedly (header anchor +
        // raw byte re-absorption to `valid_end`); the reader accumulated
        // the same chain entry-by-entry during the walk. Equality is the
        // invariant `chain::rebuild_from_file` documents — enforce it
        // here so a regression fails loudly at recovery instead of
        // surfacing later as a baffling SegmentChainBreak at the next
        // rotation. Exercised by every recovery test, including the
        // crash-at-every-byte sweeps.
        debug_assert_eq!(
            writer.chain_hash(),
            reader.chain_hash(),
            "writer's rebuilt chain must equal the reader's accumulated chain"
        );

        Ok(Self {
            app,
            writer,
            recovered_epoch,
        })
    }

    /// Save a snapshot of the current application state. The snapshot
    /// records the last journal sequence and current chain hash so
    /// recovery can resume both.
    pub fn save_snapshot(&self, snapshot_path: &Path) -> Result<(), JournaledAppError> {
        let seq = self.writer.next_sequence().saturating_sub(1);
        let chain_hash = self.writer.chain_hash().unwrap_or([0u8; 32]);
        // Runtime snapshots are produced by the shadow stage (which tracks
        // the live epoch); this convenience method snapshots from the
        // recovered epoch, correct for the boot/test paths that use it.
        snapshot::save::<A>(
            &self.app,
            seq,
            chain_hash,
            self.recovered_epoch,
            snapshot_path,
        )?;
        Ok(())
    }

    /// Archive the live journal segment to its next monotonic slot
    /// (`<path>.NNNNNN`) and open a fresh live segment continuing the
    /// sequence. The new segment's header anchor carries the chain
    /// state at the boundary so multi-segment recovery can verify
    /// cross-segment continuity. Snapshots are produced separately by
    /// the shadow exchange.
    pub fn rotate_segment(&mut self) -> Result<(), JournaledAppError> {
        self.writer.rotate_segment()?;
        Ok(())
    }

    /// Size of the current journal file in bytes.
    pub fn journal_size(&self) -> u64 {
        self.writer.valid_end()
    }

    /// Current journal sequence number (next to be assigned).
    pub fn next_sequence(&self) -> u64 {
        self.writer.next_sequence()
    }

    /// Path to the journal file.
    pub fn journal_path(&self) -> &Path {
        self.writer.path()
    }

    /// Current BLAKE3 chain hash (for diagnostics).
    pub fn chain_hash(&self) -> Option<[u8; 32]> {
        self.writer.chain_hash()
    }

    /// Borrow the application (e.g. for pre-pipeline setup or tests).
    pub fn app(&self) -> &A {
        &self.app
    }

    /// Mutable borrow of the application.
    pub fn app_mut(&mut self) -> &mut A {
        &mut self.app
    }

    /// Construct from pre-built parts. Used by the server's
    /// "snapshot-only" recovery path (journal missing post-rotation),
    /// which supplies the epoch read from the snapshot it loaded.
    pub fn from_parts(app: A, writer: W, recovered_epoch: u64) -> Self {
        Self {
            app,
            writer,
            recovered_epoch,
        }
    }

    /// Decompose into parts for the pipeline architecture.
    pub fn into_parts(self) -> (A, W) {
        (self.app, self.writer)
    }

    /// Fencing epoch recovered from the snapshot + journal. The boot site
    /// reads this to seed the node's `FenceState`.
    pub fn recovered_epoch(&self) -> u64 {
        self.recovered_epoch
    }
}

#[cfg(feature = "test-utils")]
impl<A: Application, W: JournalWrite<A::Event>> JournaledApp<A, W> {
    /// Journal an event and apply it to the inner application in one
    /// call. Test-only primitive — production drives events through the
    /// disruptor pipeline (journal stage + matching stage on separate
    /// threads), and never journals-then-applies on the same thread.
    ///
    /// Used by tests that migrated off the now-deleted
    /// `JournaledExchange` wrapper, which exposed the same shape.
    pub fn apply_journaled(
        &mut self,
        event: A::Event,
        out: &mut Vec<A::Report>,
    ) -> Result<Option<A::QueryResponse>, JournalError> {
        let seq = self.writer.append(&JournalEvent::App(event))?;
        let ctx = melin_app::ApplyCtx {
            now_ns: melin_app::unix_epoch_nanos(),
            journal_sequence: seq,
            active_connections: 0,
            events_processed: 0,
            key_hash: 0,
        };
        Ok(self.app.apply(event, &ctx, out))
    }

    /// Journal a tick event and dispatch it to the inner application.
    /// Mirrors `apply_journaled` for the tick path.
    pub fn tick_journaled(
        &mut self,
        now_ns: u64,
        out: &mut Vec<A::Report>,
    ) -> Result<(), JournalError> {
        self.writer.append(&JournalEvent::Tick { now_ns })?;
        self.app.tick(now_ns, out);
        Ok(())
    }

    /// Mutable access to the writer for tests that need writer-level
    /// control the public API doesn't expose (e.g. encoding entries
    /// with hand-picked sequences to exercise recovery edge cases).
    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}

/// Dispatch a single journaled entry back into the application during
/// replay. Mirrors the live matching-stage dispatch: hybrid scheduler
/// clock drain, `check_request_seq` rebuilds the per-key HWM, then the
/// event flows to `apply` or `tick` depending on its kind.
fn replay_entry<A: Application>(
    app: &mut A,
    event: &JournalEvent<A::Event>,
    timestamp_ns: u64,
    key_hash: u64,
    request_seq: u64,
    last_drain_ns: &mut u64,
    recovered_epoch: &mut u64,
    reports: &mut Vec<A::Report>,
) {
    // Rebuild per-key HWM and capture whether this was a new request.
    // The journal stage writes events before the matching stage dedups,
    // so the journal can contain duplicates the primary rejected without
    // calling `apply`. Replay must skip `apply` on those entries or
    // state will diverge from the live primary (e.g. a retried deposit
    // applied twice). For `Tick` events `key_hash == 0`, which
    // `check_request_seq` exempts — `is_new` is always true there.
    let is_new = app.check_request_seq(key_hash, request_seq);

    if timestamp_ns > *last_drain_ns {
        *last_drain_ns = timestamp_ns;
        app.tick(timestamp_ns, reports);
    }

    match event {
        JournalEvent::App(e) => {
            if !is_new {
                // Primary produced a dedup rejection here; replay discards
                // it because the client already received that reject at
                // live time.
                return;
            }
            // Reports produced during replay are discarded — they already
            // went to the client at the time the event was accepted.
            // `key_hash` is the dedup identity threaded through this
            // event so self-introspecting queries see the correct
            // per-key state under replay.
            let ctx = ApplyCtx {
                now_ns: timestamp_ns,
                journal_sequence: 0,
                active_connections: 0,
                events_processed: 0,
                key_hash,
            };
            // Query response discarded during replay — these already
            // went to the client when the event was first accepted.
            let _ = app.apply(*e, &ctx, reports);
        }
        JournalEvent::Tick { now_ns } => {
            app.tick(*now_ns, reports);
        }
        JournalEvent::EpochBump { epoch } => {
            // Lineage metadata — advance the recovered epoch, never touch
            // application state. Mirrors the live matching-stage dispatch.
            *recovered_epoch = (*recovered_epoch).max(*epoch);
        }
        JournalEvent::Shutdown => {
            // Pipeline-only sentinel; never written to disk and so
            // unreachable on the replay path. Treat defensively rather
            // than panic — recovery can't recover from a corrupt journal
            // with a shutdown entry, but it shouldn't crash the process.
        }
    }
}

/// Replay a single segment into the application.
///
/// Skips events with `seq <= snap_sequence` so a snapshot caller can
/// share this routine with no-snapshot recovery (where `snap_sequence`
/// is `0`, accepting all events). When `snap_chain` is supplied, the
/// reader's chain hash is compared against it at the moment the anchor
/// entry (`seq == snap_sequence`) is observed — every entry surfaces to
/// this loop, so no capture machinery is needed.
///
/// `allow_partial_tail` controls how `SequenceGap` is treated: archived
/// segments are sealed and any gap is corruption (returned as an error);
/// the live segment may have a torn tail from a crash, so a gap
/// terminates replay cleanly.
fn replay_segment<A: Application>(
    reader: &mut JournalReader<A::Event>,
    app: &mut A,
    snap_sequence: u64,
    snap_chain: Option<[u8; 32]>,
    last_drain_ns: &mut u64,
    recovered_epoch: &mut u64,
    reports: &mut Vec<A::Report>,
    allow_partial_tail: bool,
) -> Result<(), JournaledAppError> {
    loop {
        match reader.next_entry() {
            Ok(Some(entry)) => {
                // Snapshot/journal cross-check at the anchor: the chain
                // value after absorbing the anchor entry must equal what
                // the snapshot recorded. Fires before any post-anchor
                // event is replayed.
                if entry.sequence == snap_sequence
                    && let Some(expected) = snap_chain
                    && let Some(actual) = reader.chain_hash()
                    && actual != expected
                {
                    return Err(JournaledAppError::SnapshotChainMismatch {
                        snap_sequence,
                        expected_chain_hash: expected,
                        actual_chain_hash: actual,
                    });
                }
                if entry.sequence > snap_sequence {
                    replay_entry(
                        app,
                        &entry.event,
                        entry.timestamp_ns,
                        entry.key_hash,
                        entry.request_seq,
                        last_drain_ns,
                        recovered_epoch,
                        reports,
                    );
                    reports.clear();
                }
            }
            Ok(None) => break,
            Err(JournalError::SequenceGap { expected, actual }) => {
                if allow_partial_tail {
                    tracing::warn!(
                        expected,
                        actual,
                        "sequence gap during recovery — truncating at gap"
                    );
                    break;
                }
                return Err(JournalError::SequenceGap { expected, actual }.into());
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

/// Verify a segment's header links it to the previous segment in the
/// walk: the header anchor must equal the previous segment's tail chain
/// hash, and the header `starting_sequence` must continue the sequence
/// space exactly. Runs *before* the segment is replayed, so a foreign or
/// tampered segment never reaches the application.
///
/// The first walked segment has no predecessor (`expected_start` is
/// `None`); instead it must start at or before `history_floor` —
/// sequence 1 without a snapshot, the snapshot's anchor + 1 with one.
/// Otherwise a prefix of the lineage is provably missing (the header
/// records where the lineage continues from) and replay would silently
/// reconstruct partial state.
///
/// `index = 0` denotes the live segment in diagnostics. The chain
/// compare is a no-op when either side is unavailable (hash-chain off,
/// or no predecessor in this run).
fn verify_segment_link<E: melin_app::AppEvent>(
    index: u32,
    reader: &JournalReader<E>,
    prev_tail: Option<[u8; 32]>,
    expected_start: Option<u64>,
    history_floor: u64,
) -> Result<(), JournaledAppError> {
    if let (Some(expected), Some(actual)) = (prev_tail, reader.anchor())
        && expected != actual
    {
        return Err(JournalError::SegmentChainBreak {
            index,
            expected,
            actual,
        }
        .into());
    }
    match expected_start {
        Some(expected) => {
            if reader.starting_sequence() != expected {
                return Err(JournalError::SequenceGap {
                    expected,
                    actual: reader.starting_sequence(),
                }
                .into());
            }
        }
        None => {
            if reader.starting_sequence() > history_floor {
                return Err(JournaledAppError::MissingHistoryPrefix {
                    first_segment_start: reader.starting_sequence(),
                    required_floor: history_floor,
                });
            }
        }
    }
    Ok(())
}

/// Snapshot anchored exactly at a rotation boundary: the anchor entry
/// lives at the tail of the *previous* segment, but the successor
/// segment's header anchor is, by construction, the chain value at that
/// boundary — so it can stand in for the comparison. This keeps the
/// cross-check effective even when the anchor-bearing segment was
/// archived off-box, and covers the empty-live-after-rotation layout.
fn verify_boundary_snapshot_anchor<E: melin_app::AppEvent>(
    reader: &JournalReader<E>,
    snap_sequence: u64,
    expected: Option<[u8; 32]>,
) -> Result<(), JournaledAppError> {
    if let Some(expected) = expected
        && snap_sequence + 1 == reader.starting_sequence()
        && let Some(anchor) = reader.anchor()
        && anchor != expected
    {
        return Err(JournaledAppError::SnapshotChainMismatch {
            snap_sequence,
            expected_chain_hash: expected,
            actual_chain_hash: anchor,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestEvent};
    use melin_journal::{BufferedWriter, JournalEvent, JournalReader};

    // Concrete writer used by every test. The buffered path covers
    // the same JournaledApp logic without needing PLP hardware.
    type TestApp_ = JournaledApp<TestApp, BufferedWriter<TestEvent>>;

    /// Write events with auto-allocated sequences and fsync them to disk.
    /// Each event is keyed on `(key_hash = 1, request_seq = first_seq + idx)`
    /// so tests that append in multiple phases can offset `first_seq` to
    /// avoid dedup collisions across calls.
    fn append_events(ja: TestApp_, events: &[TestEvent], first_seq: u64) -> TestApp_ {
        let (app, mut writer) = ja.into_parts();
        for (i, e) in events.iter().enumerate() {
            let seq = writer.allocate_sequence();
            writer
                .encode_event(
                    seq,
                    /* timestamp_ns */ 1_000 * (i as u64 + 1),
                    &JournalEvent::App(*e),
                    /* key_hash */ 1,
                    /* request_seq */ first_seq + i as u64,
                )
                .unwrap();
        }
        writer.flush_batch_sync().unwrap();
        JournaledApp::from_parts(app, writer, 0)
    }

    /// Compute the TestApp state that results from applying `events` in
    /// order, using the same `(key_hash, request_seq)` scheme as
    /// `append_events`. Mirrors `replay_entry`'s dedup gate (post-#7) so
    /// the expected state matches what replay produces.
    fn expected_state(events: &[TestEvent], first_seq: u64) -> TestApp {
        let mut app = TestApp::new();
        let mut reports = Vec::new();
        let ctx = ApplyCtx {
            now_ns: 0,
            journal_sequence: 0,
            active_connections: 0,
            events_processed: 0,
            key_hash: 1,
        };
        for (i, e) in events.iter().enumerate() {
            let is_new = app.check_request_seq(1, first_seq + i as u64);
            let ts = 1_000 * (i as u64 + 1);
            app.tick(ts, &mut reports);
            if is_new {
                let _ = app.apply(*e, &ctx, &mut reports);
            }
        }
        app
    }

    #[test]
    fn create_then_recover_empty_journal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.bin");

        let ja = TestApp_::create(TestApp::new(), &path).unwrap();
        // Sequences start at 1: seq=0 is the InputSlot "not yet allocated"
        // sentinel the journal stage branches on. Chain metadata lives in
        // the file header, so creation consumes no sequence.
        assert_eq!(ja.next_sequence(), 1);
        drop(ja);

        let recovered = TestApp_::recover(TestApp::new(), &path).unwrap();
        assert_eq!(*recovered.app(), TestApp::new());
    }

    /// A promoted primary writes an `EpochBump` as a journaled event. On
    /// recovery the bump must advance the node's observed epoch (so it
    /// re-advertises the post-promotion epoch on the next handshake) while
    /// never touching application state — it is lineage metadata, like a
    /// `Tick` the application never sees.
    #[test]
    fn recovers_epoch_from_journaled_epoch_bump() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epoch.journal");

        // Genesis: no promotion yet, so the recovered epoch is 0.
        let ja = TestApp_::create(TestApp::new(), &path).unwrap();
        assert_eq!(ja.recovered_epoch(), 0, "genesis epoch must be 0");
        let (app, mut writer) = ja.into_parts();

        // Journal an App event, then an EpochBump (the promotion marker),
        // then another App event — the shape a promoted primary produces.
        let s0 = writer.allocate_sequence();
        writer
            .encode_event(s0, 1_000, &JournalEvent::App(TestEvent::Add(5)), 1, 1)
            .unwrap();
        let s1 = writer.allocate_sequence();
        writer
            .encode_event(s1, 2_000, &JournalEvent::EpochBump { epoch: 3 }, 0, 0)
            .unwrap();
        let s2 = writer.allocate_sequence();
        writer
            .encode_event(s2, 3_000, &JournalEvent::App(TestEvent::Add(7)), 1, 2)
            .unwrap();
        writer.flush_batch_sync().unwrap();
        drop(JournaledApp::from_parts(app, writer, 0));

        let recovered = TestApp_::recover(TestApp::new(), &path).unwrap();
        assert_eq!(
            recovered.recovered_epoch(),
            3,
            "epoch must be recovered from the journaled EpochBump"
        );
        // Both App events applied (5 + 7); the EpochBump did not.
        assert_eq!(
            recovered.app().total,
            12,
            "App events must apply and the EpochBump must not touch app state"
        );
    }

    /// An old-format journal must fail recovery with
    /// `UnsupportedVersion` — never be misread as corruption (which
    /// reads as "restore from backup" to an operator) nor, worse, as a
    /// fresh/empty layout that a bootstrap path would overwrite. This
    /// is the contract behind the documented upgrade procedure
    /// (snapshot → deploy → fresh journal): the version gate must fail
    /// fast at open, before any entry is decoded, and leave the
    /// old-format file untouched as the operator's upgrade input.
    #[test]
    fn recover_rejects_old_format_version() {
        use std::io::{Read, Seek, SeekFrom, Write};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.bin");

        let ja = TestApp_::create(TestApp::new(), &path).unwrap();
        let ja = append_events(ja, &[TestEvent::Add(1), TestEvent::Add(2)], 1);
        drop(ja);

        // Rewrite the header with an old format version. Header layout
        // (see codec.rs): magic u32 | format_version u16 @4 |
        // sector_size u16 | starting_sequence u64 | anchor_hash [u8;32]
        // | header_crc u32 @48, CRC over bytes 0..48 — fixed up after
        // the patch so the version check is provably the only fault.
        let mut header = [0u8; 52];
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            f.read_exact(&mut header).unwrap();
            header[4..6].copy_from_slice(&13u16.to_le_bytes());
            let crc = crc32c::crc32c(&header[..48]);
            header[48..52].copy_from_slice(&crc.to_le_bytes());
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(&header).unwrap();
            f.sync_all().unwrap();
        }
        let len_before = std::fs::metadata(&path).unwrap().len();

        match TestApp_::recover(TestApp::new(), &path) {
            Err(JournaledAppError::Journal(JournalError::UnsupportedVersion { version })) => {
                assert_eq!(version, 13)
            }
            Err(other) => panic!("expected UnsupportedVersion, got {other:?}"),
            Ok(_) => panic!("recovery accepted an old-format journal"),
        }

        // The failed recovery must not have touched the file — it is
        // the operator's audit evidence and upgrade input.
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len_before);
    }

    #[test]
    fn recover_replays_events_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.bin");

        let events = [TestEvent::Add(3), TestEvent::Add(7), TestEvent::Add(100)];
        let ja = TestApp_::create(TestApp::new(), &path).unwrap();
        let ja = append_events(ja, &events, 1);
        drop(ja);

        let recovered = TestApp_::recover(TestApp::new(), &path).unwrap();
        assert_eq!(*recovered.app(), expected_state(&events, 1));
    }

    #[test]
    fn save_snapshot_round_trips_via_generic_load() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap");

        let events = [TestEvent::Add(10), TestEvent::Add(20)];
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        drop(append_events(ja, &events, 1)); // journal write, writer drops
        let ja = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        ja.save_snapshot(&snap_path).unwrap();

        let (restored, seq, _chain, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(restored, expected_state(&events, 1));
        // Sequences are 1-indexed; after N events, next_sequence = N + 1
        // and save_snapshot records the last issued sequence (next - 1) = N.
        assert_eq!(seq, events.len() as u64);
    }

    #[test]
    fn recover_from_snapshot_applies_post_snapshot_delta() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap");

        let pre = [TestEvent::Add(1), TestEvent::Add(2)];
        let post = [TestEvent::Add(40), TestEvent::Add(50)];

        // Phase 1: create + pre events (request_seqs 1..=2) + snapshot.
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &pre, 1);
        drop(ja);
        let ja = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        ja.save_snapshot(&snap_path).unwrap();

        // Phase 2: append post events (request_seqs 3..=4 — disjoint from
        // pre, so they pass dedup) to the same journal file; no rotation.
        let ja = append_events(ja, &post, pre.len() as u64 + 1);
        drop(ja);

        // Phase 3: recover_from_snapshot should load the snapshot (state
        // after `pre`) and replay only the entries strictly after the
        // snapshot's sequence (i.e. `post`).
        let recovered = TestApp_::recover_from_snapshot(&snap_path, &journal_path).unwrap();

        let all: Vec<TestEvent> = pre.iter().chain(post.iter()).copied().collect();
        assert_eq!(recovered.app().total, expected_state(&all, 1).total);
    }

    #[test]
    fn replay_skips_duplicate_app_events() {
        // The journal stage writes before the matching stage dedups, so
        // the journal can legitimately contain two entries sharing a
        // `(key_hash, request_seq)`. Only the first reaches `apply` on
        // the live primary; replay must mirror that or recovered state
        // will double-apply the duplicate.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.bin");

        let ja = TestApp_::create(TestApp::new(), &path).unwrap();
        let (_app, mut writer) = ja.into_parts();

        let dup = JournalEvent::App(TestEvent::Add(100));
        for _ in 0..2 {
            let seq = writer.allocate_sequence();
            writer
                .encode_event(
                    seq, 1_000, &dup, /* key_hash */ 5, /* request_seq */ 10,
                )
                .unwrap();
        }
        writer.flush_batch_sync().unwrap();
        drop(writer);

        let recovered = TestApp_::recover(TestApp::new(), &path).unwrap();
        // First Add(100) applied; second is a duplicate and must be
        // skipped — total stays at 100, not 200.
        assert_eq!(recovered.app().total, 100);
        // HWM for key 5 should record seq 10 exactly once; a second
        // check_request_seq at seq 10 must still be rejected as a
        // duplicate after recovery.
        let mut app = TestApp {
            total: recovered.app().total,
            ticks: recovered.app().ticks,
            key_hwm: recovered.app().key_hwm.clone(),
        };
        assert!(!app.check_request_seq(5, 10));
        assert!(app.check_request_seq(5, 11));
    }

    #[test]
    fn rotate_archives_and_continues_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap");

        let events = [TestEvent::Add(11), TestEvent::Add(22)];
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &events, 1);
        let pre_rotate_next_seq = ja.next_sequence();
        let pre_rotate_state = TestApp {
            total: ja.app().total,
            ticks: ja.app().ticks,
            key_hwm: ja.app().key_hwm.clone(),
        };

        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();

        // Archived journal lives at `.000001` (monotonic naming).
        let archived = dir.path().join("journal.bin.000001");
        assert!(archived.exists(), "pre-rotate journal must be archived");
        // Sequence continues past the archive cut — rotation consumes
        // no sequence number.
        assert_eq!(ja.next_sequence(), pre_rotate_next_seq);
        // Snapshot captures the pre-rotate state.
        let (snap_app, _seq, _chain, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(snap_app, pre_rotate_state);

        // The new journal is fresh — recovering it without the snapshot
        // yields pre_rotate_state (unchanged — fresh app, no events to
        // replay). recover_from_snapshot composes snapshot + (empty)
        // delta = pre_rotate_state.
        drop(ja);
        let recovered = TestApp_::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(*recovered.app(), pre_rotate_state);
    }

    /// Multi-segment recovery: build state across three rotations,
    /// then `recover` (no snapshot) — must walk all three archives plus
    /// the live segment and produce identical balances to a no-rotation
    /// run with the same events.
    ///
    /// Compares `total` and `key_hwm` only; `ticks` is sensitive to the
    /// per-phase timestamp restart in `append_events` and isn't part of
    /// the rotation behaviour under test.
    #[test]
    fn recover_walks_multiple_archived_segments() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let phase_a = [TestEvent::Add(1), TestEvent::Add(2)];
        let phase_b = [TestEvent::Add(10), TestEvent::Add(20)];
        let phase_c = [TestEvent::Add(100), TestEvent::Add(200)];
        let phase_d = [TestEvent::Add(1000)];

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &phase_a, 1);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        let mut ja = append_events(ja, &phase_b, 1 + phase_a.len() as u64);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        let mut ja = append_events(ja, &phase_c, 1 + (phase_a.len() + phase_b.len()) as u64);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        let ja = append_events(
            ja,
            &phase_d,
            1 + (phase_a.len() + phase_b.len() + phase_c.len()) as u64,
        );
        drop(ja);

        // Sanity: all three archives plus a live segment exist on disk.
        for n in 1..=3u32 {
            let path = dir.path().join(format!("journal.bin.{n:06}"));
            assert!(path.exists(), "archive {n} should exist at {path:?}");
        }
        assert!(journal_path.exists(), "live journal should exist");

        let recovered = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        let total_events = phase_a.len() + phase_b.len() + phase_c.len() + phase_d.len();
        assert_eq!(recovered.app().total, 1 + 2 + 10 + 20 + 100 + 200 + 1000);
        // HWM is per (key_hash=1, request_seq) and append_events uses
        // sequential request_seqs (1..=7).
        assert_eq!(
            recovered.app().key_hwm.get(&1).copied(),
            Some(total_events as u64)
        );
    }

    /// Multi-segment recovery via snapshot: build state through several
    /// rotations, recover the populated state, snapshot it, then re-
    /// recover from that snapshot — the snapshot's last-seq is past
    /// every archived event, so recovery must skip them all and still
    /// produce the snapshot's state. This proves the multi-segment
    /// walker honours `snap_sequence` across archives.
    #[test]
    fn recover_from_snapshot_skips_pre_snapshot_segments() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let phase_a = [TestEvent::Add(1), TestEvent::Add(2)];
        let phase_b = [TestEvent::Add(10), TestEvent::Add(20)];
        let phase_c = [TestEvent::Add(100), TestEvent::Add(200)];

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &phase_a, 1);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        let mut ja = append_events(ja, &phase_b, 1 + phase_a.len() as u64);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        let ja = append_events(ja, &phase_c, 1 + (phase_a.len() + phase_b.len()) as u64);
        drop(ja);

        // Replay everything to populate the app, then snapshot it.
        let recovered = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        let expected = TestApp {
            total: recovered.app().total,
            ticks: recovered.app().ticks,
            key_hwm: recovered.app().key_hwm.clone(),
        };
        let final_snap = dir.path().join("final.snap");
        recovered.save_snapshot(&final_snap).unwrap();
        drop(recovered);

        // Re-recover from the freshly-saved snapshot. snap_sequence is
        // past every event in every archive and the live segment, so
        // all replays should be skipped — the resulting app must match
        // the snapshotted app exactly.
        let re = TestApp_::recover_from_snapshot(&final_snap, &journal_path).unwrap();
        assert_eq!(*re.app(), expected);
    }

    /// Cross-segment chain validation: tampering with an archived
    /// segment (post-rotation) is detectable as a `SegmentChainBreak`
    /// at recovery, because the next segment's header anchor no longer
    /// matches the tampered segment's tail chain hash.
    ///
    /// The tamper is **CRC-consistent**: the entry's payload is altered
    /// and its CRC32C recomputed, so per-entry integrity checks pass —
    /// only the chain can catch it. This is the guarantee the chain
    /// exists for (CRC covers accidental corruption; the chain covers
    /// deliberate, internally-consistent rewrites).
    #[cfg(feature = "hash-chain")]
    #[test]
    fn recover_detects_cross_segment_chain_break() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let phase_a = [TestEvent::Add(1)];
        let phase_b = [TestEvent::Add(2)];

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &phase_a, 1);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap(); // → archive 000001 sealed
        let ja = append_events(ja, &phase_b, 1 + phase_a.len() as u64);
        drop(ja);

        // Rewrite the first entry of archive 000001 with a modified
        // payload byte and a *recomputed* CRC, so the entry itself
        // decodes cleanly. Entry layout: header(20) + length-covered
        // body + crc(4), starting at ENTRY_OFFSET.
        let archive = dir.path().join("journal.bin.000001");
        let mut buf = std::fs::read(&archive).unwrap();
        let entry_start = melin_journal::codec::ENTRY_OFFSET as usize;
        let length = u16::from_le_bytes([buf[entry_start + 2], buf[entry_start + 3]]) as usize;
        let body_end = entry_start + 20 + length;
        // Flip the last payload byte (inside the app event's encoding).
        buf[body_end - 1] ^= 0xFF;
        let new_crc = crc32c::crc32c(&buf[entry_start..body_end]);
        buf[body_end..body_end + 4].copy_from_slice(&new_crc.to_le_bytes());
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&archive)
            .unwrap();
        f.write_all(&buf).unwrap();
        f.sync_all().unwrap();
        drop(f);

        // Tampering with an archived segment must surface as some flavour
        // of detection error — within-segment hash-chain mismatch, the
        // cross-segment compare, an entry-level CRC, or a magic/version
        // check. The *absence* of any error would mean a tampered
        // archive replays silently, which is the regression this test
        // guards against.
        let result = TestApp_::recover(TestApp::new(), &journal_path);
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected recovery to detect tampered archive, but it succeeded"),
        };
        // The tamper is CRC-consistent, so the only mechanism that can
        // catch it is the cross-segment chain compare.
        let msg = format!("{err}");
        assert!(
            msg.contains("segment chain break"),
            "expected SegmentChainBreak for a CRC-consistent rewrite, got: {msg}"
        );
    }

    /// Phase B crash: rotation interrupted between the live → archive
    /// rename and the new live file's creation. On disk the just-
    /// archived segment is intact and the bare path is missing.
    /// Recovery must replay the archive's events and synthesize a
    /// fresh live segment seeded with the previous tail's chain hash
    /// so subsequent appends continue the chain.
    #[test]
    fn recover_synthesizes_live_after_phase_b_crash() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");

        // Build state in the live segment, flush, then rename the live
        // file out from under the writer to simulate the post-rename /
        // pre-create-continuing crash window.
        let events = [TestEvent::Add(7), TestEvent::Add(11), TestEvent::Add(13)];
        let expected_total: u64 = events
            .iter()
            .map(|e| match e {
                TestEvent::Add(n) => *n,
                _ => 0,
            })
            .sum();
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &events, 1);
        let pre_crash_seq = ja.next_sequence();
        // Drop the writer (closes the fd) before the rename so this is
        // a clean simulation of "live archived, no successor file."
        drop(ja);
        let archived = melin_journal::segment::archive_live(&journal_path).unwrap();
        assert!(archived.exists(), "archive must exist after rename");
        assert!(
            !journal_path.exists(),
            "live must be gone — that's the Phase B state"
        );

        // Recovery must replay the archive AND synthesize a new live.
        // `append_events` writes to the journal without applying to the
        // app (test fixture limitation), so the only path that can
        // produce a populated app is the recovery replay — which is
        // exactly what this test validates.
        let recovered = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        assert_eq!(
            recovered.app().total,
            expected_total,
            "archive events must replay despite the missing live"
        );
        assert!(
            journal_path.exists(),
            "recovery should have synthesized a fresh live segment"
        );
        // The synthesized live consumes no sequence — its header records
        // the continuation point.
        assert_eq!(recovered.next_sequence(), pre_crash_seq);

        // Append more events through the synthesized live and re-recover
        // — proves the new live is fully usable, not just a placeholder.
        let post = [TestEvent::Add(100)];
        let ja = append_events(recovered, &post, 1 + events.len() as u64);
        drop(ja);
        let re = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        assert_eq!(re.app().total, expected_total + 100);
    }

    /// Phase B with no archives at all should still error — the caller
    /// (server bootstrap) is responsible for snapshot-only recovery in
    /// that case, since the snapshot supplies the chain hash and
    /// starting sequence we'd need.
    #[test]
    fn recover_errors_when_no_live_and_no_archives() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        // Path doesn't exist, no archives.
        let result = TestApp_::recover(TestApp::new(), &journal_path);
        assert!(result.is_err(), "expected error, got Ok");
    }

    /// Phase C crash: rotation completed (live → archive renamed, new
    /// live created with its header), but no application events landed
    /// in the new live before the crash. On disk: archive(s) + an
    /// empty live segment. Recovery walks both and reproduces the
    /// pre-rotation state exactly.
    #[test]
    fn recover_after_phase_c_crash_yields_pre_rotation_state() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");

        let events = [TestEvent::Add(2), TestEvent::Add(4), TestEvent::Add(6)];
        let expected: u64 = events
            .iter()
            .map(|e| match e {
                TestEvent::Add(n) => *n,
                _ => 0,
            })
            .sum();

        // Append events and run a full rotation, then drop without
        // writing anything else — Phase C state is "rotation finished,
        // no fresh events yet."
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &events, 1);
        let (app, mut writer) = ja.into_parts();
        writer.rotate_segment().unwrap();
        drop(writer);
        drop(app);

        // Both the archive and the new live exist on disk.
        let archive = dir.path().join("journal.bin.000001");
        assert!(archive.exists(), "archive must exist post-rotation");
        assert!(journal_path.exists(), "live must exist post-rotation");

        let recovered = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        assert_eq!(recovered.app().total, expected);
    }

    /// The original silent-data-loss layout, end to end: a snapshot
    /// taken mid-segment, more acked events journaled after it, then a
    /// rotation crash between the live → archive rename and the new
    /// live file's creation. On disk: snapshot + an archive holding
    /// events PAST the snapshot + no live segment.
    ///
    /// `recover_from_snapshot` must compose all three halves at once —
    /// snapshot restore, post-snapshot delta replay out of the archive
    /// (with the chain cross-check at the anchor entry), and Phase-B
    /// synthesis of the missing live continuing the archive's tail.
    /// Bootstrapping "snapshot only" from this layout (the pre-fix
    /// server behaviour) silently rewound the post-snapshot events.
    #[test]
    fn recover_from_snapshot_replays_archived_events_past_snapshot_when_live_missing() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let pre = [TestEvent::Add(1), TestEvent::Add(2)];
        let post = [TestEvent::Add(10), TestEvent::Add(20)];

        // pre events (seqs 1-2), recover to populate the app, snapshot
        // at seq 2, then post events (seqs 3-4) into the SAME segment.
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &pre, 1);
        drop(ja);
        let ja = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        let pre_total = ja.app().total;
        ja.save_snapshot(&snap_path).unwrap();
        let mut ja = append_events(ja, &post, 1 + pre.len() as u64);

        // Rotation seals seqs 1-4 into archive 000001; deleting the
        // fresh live reproduces the crash window between the rename and
        // create_continuing.
        ja.rotate_segment().unwrap();
        drop(ja);
        std::fs::remove_file(&journal_path).unwrap();

        let recovered = TestApp_::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(
            recovered.app().total,
            pre_total + 10 + 20,
            "post-snapshot events in the archive must replay"
        );
        assert_eq!(
            recovered.next_sequence(),
            1 + (pre.len() + post.len()) as u64,
            "synthesized live must continue past the archived tail"
        );
        assert!(
            journal_path.exists(),
            "recovery should have synthesized a fresh live segment"
        );

        // The synthesized live is usable: append, then re-recover the
        // full lineage from the same snapshot.
        let ja = append_events(
            recovered,
            &[TestEvent::Add(100)],
            1 + (pre.len() + post.len()) as u64,
        );
        drop(ja);
        let re = TestApp_::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(re.app().total, pre_total + 10 + 20 + 100);
    }

    /// Recovery accepts a snapshot anchored exactly at a rotation
    /// boundary even when the segment holding the anchor entry has been
    /// trimmed (e.g. moved to cold storage). The successor segment's
    /// header proves history through `starting_sequence - 1` existed,
    /// and its anchor — which *is* the chain value at the boundary —
    /// verifies the snapshot's recorded hash.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn recover_from_snapshot_accepts_trimmed_archive_at_boundary_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let pre = [TestEvent::Add(10), TestEvent::Add(20), TestEvent::Add(30)];
        let post = [TestEvent::Add(40)];
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &pre, 1);
        drop(ja);
        // Recover (populates app state), snapshot at the tail, rotate.
        let mut ja = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        let expected_pre_total = ja.app().total;
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        let ja = append_events(ja, &post, 1 + pre.len() as u64);
        drop(ja);

        // Trim the archive that contains the anchor entry.
        let archive = dir.path().join("journal.bin.000001");
        std::fs::remove_file(&archive).unwrap();

        // Recovery must accept (header evidence reaches the anchor) and
        // verify the snapshot hash against the live segment's anchor.
        let recovered = TestApp_::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(recovered.app().total, expected_pre_total + 40);
    }

    /// Negative companion: a snapshot anchored at the rotation boundary
    /// with the wrong chain hash is rejected against the successor
    /// segment's header anchor — even though no entry at the anchor
    /// sequence is ever observed.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn recover_from_snapshot_rejects_chain_hash_mismatch_at_boundary_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let pre = [TestEvent::Add(1), TestEvent::Add(2)];
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &pre, 1);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        drop(ja);

        let archive = dir.path().join("journal.bin.000001");
        std::fs::remove_file(&archive).unwrap();

        // Forge the snapshot's chain hash; sequence stays at the boundary.
        let (loaded_app, snap_seq, real_hash, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        let bad_hash = [0xEE; 32];
        assert_ne!(real_hash, bad_hash);
        snapshot::save::<TestApp>(&loaded_app, snap_seq, bad_hash, 0, &snap_path).unwrap();

        let err = match TestApp_::recover_from_snapshot(&snap_path, &journal_path) {
            Ok(_) => panic!("expected boundary-anchor mismatch rejection"),
            Err(e) => e,
        };
        match err {
            JournaledAppError::SnapshotChainMismatch {
                snap_sequence,
                expected_chain_hash,
                actual_chain_hash,
            } => {
                assert_eq!(snap_sequence, snap_seq);
                assert_eq!(expected_chain_hash, bad_hash);
                assert_eq!(actual_chain_hash, real_hash);
            }
            other => panic!("expected SnapshotChainMismatch, got {other:?}"),
        }
    }

    /// Snapshot sequences live in journal space: with chain metadata out
    /// of the entry stream, `save_snapshot` records exactly the count of
    /// journaled events — no control-entry inflation.
    #[test]
    fn snapshot_sequence_counts_only_journaled_events() {
        let _prealloc_guard = melin_journal::test_utils::PreallocOverrideGuard::new(1024 * 1024);

        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let pre = [
            TestEvent::Add(1),
            TestEvent::Add(2),
            TestEvent::Add(3),
            TestEvent::Add(4),
            TestEvent::Add(5),
            TestEvent::Add(6),
        ];
        let post = [TestEvent::Add(100), TestEvent::Add(200)];

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &pre, 1);
        drop(ja);
        let ja = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        ja.save_snapshot(&snap_path).unwrap();

        let (_, snap_seq, _, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(
            snap_seq,
            pre.len() as u64,
            "snapshot sequence must equal the journaled event count"
        );

        // Append post-snapshot events and verify round-trip.
        let ja = append_events(ja, &post, 1 + pre.len() as u64);
        drop(ja);

        let recovered = TestApp_::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        let all: Vec<TestEvent> = pre.iter().chain(post.iter()).copied().collect();
        assert_eq!(recovered.app().total, expected_state(&all, 1).total);
    }

    /// Snapshot-less recovery on a journal whose oldest segment begins
    /// mid-history (oldest archive trimmed) must be REJECTED, not
    /// silently replayed into partial state. The segment header records
    /// where the lineage continues from, so the missing prefix is
    /// provable. Before the `MissingHistoryPrefix` guard, this exact
    /// scenario produced a partial ledger with no error.
    #[test]
    fn recover_rejects_trimmed_history_prefix_without_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");

        let phase_a = [TestEvent::Add(1), TestEvent::Add(2)];
        let phase_b = [TestEvent::Add(10), TestEvent::Add(20)];
        let phase_c = [TestEvent::Add(100)];

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &phase_a, 1);
        ja.rotate_segment().unwrap();
        let mut ja = append_events(ja, &phase_b, 1 + phase_a.len() as u64);
        ja.rotate_segment().unwrap();
        let ja = append_events(ja, &phase_c, 1 + (phase_a.len() + phase_b.len()) as u64);
        drop(ja);

        // Trim the oldest archive — the surviving 000002 starts at
        // sequence 3, so history 1..=2 is provably missing.
        std::fs::remove_file(dir.path().join("journal.bin.000001")).unwrap();

        let err = match TestApp_::recover(TestApp::new(), &journal_path) {
            Ok(recovered) => panic!(
                "recovery must reject a trimmed history prefix; \
                 instead produced partial state with total = {}",
                recovered.app().total
            ),
            Err(e) => e,
        };
        match err {
            JournaledAppError::MissingHistoryPrefix {
                first_segment_start,
                required_floor,
            } => {
                assert_eq!(first_segment_start, 1 + phase_a.len() as u64);
                assert_eq!(required_floor, 1);
            }
            other => panic!("expected MissingHistoryPrefix, got {other:?}"),
        }
    }

    /// Snapshot-based recovery must likewise reject a journal whose
    /// oldest surviving segment starts past `snapshot_sequence + 1` —
    /// the events between the snapshot and the first on-disk segment
    /// would be silently lost.
    #[test]
    fn recover_from_snapshot_rejects_gap_between_snapshot_and_oldest_segment() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let phase_a = [TestEvent::Add(1), TestEvent::Add(2)];
        let phase_b = [TestEvent::Add(10), TestEvent::Add(20)];

        // Snapshot at seq 2 (tail of phase_a), then two rotations so
        // the live segment starts at seq 5.
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &phase_a, 1);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        let mut ja = append_events(ja, &phase_b, 1 + phase_a.len() as u64);
        ja.rotate_segment().unwrap();
        drop(ja);

        // Trim BOTH archives: the live segment starts at 5 but the
        // snapshot only covers through 2 — seqs 3..=4 are gone.
        std::fs::remove_file(dir.path().join("journal.bin.000001")).unwrap();
        std::fs::remove_file(dir.path().join("journal.bin.000002")).unwrap();

        let err = match TestApp_::recover_from_snapshot(&snap_path, &journal_path) {
            Ok(_) => panic!("expected rejection of snapshot/segment gap"),
            Err(e) => e,
        };
        match err {
            JournaledAppError::MissingHistoryPrefix {
                first_segment_start,
                required_floor,
            } => {
                assert_eq!(first_segment_start, 5);
                assert_eq!(required_floor, phase_a.len() as u64 + 1);
            }
            other => panic!("expected MissingHistoryPrefix, got {other:?}"),
        }
    }

    /// A missing *middle* archive is caught by the cross-segment link
    /// check before any of the post-gap events replay: the successor's
    /// header fails either the anchor compare (hash-chain on) or the
    /// starting-sequence continuity compare.
    #[test]
    fn recover_rejects_missing_middle_archive() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");

        let phase_a = [TestEvent::Add(1), TestEvent::Add(2)];
        let phase_b = [TestEvent::Add(10), TestEvent::Add(20)];
        let phase_c = [TestEvent::Add(100)];

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &phase_a, 1);
        ja.rotate_segment().unwrap();
        let mut ja = append_events(ja, &phase_b, 1 + phase_a.len() as u64);
        ja.rotate_segment().unwrap();
        let ja = append_events(ja, &phase_c, 1 + (phase_a.len() + phase_b.len()) as u64);
        drop(ja);

        std::fs::remove_file(dir.path().join("journal.bin.000002")).unwrap();

        let err = match TestApp_::recover(TestApp::new(), &journal_path) {
            Ok(_) => panic!("expected rejection of missing middle archive"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("segment chain break") || msg.contains("sequence gap"),
            "expected a lineage-break error, got: {msg}"
        );
    }

    /// Two back-to-back rotations leave an empty archive in the middle
    /// of the lineage. Recovery must walk through it: the empty
    /// segment's tail chain hash is its anchor (identity), and the next
    /// segment's expected start equals the empty segment's own
    /// `starting_sequence` (no sequences were consumed).
    #[test]
    fn recover_walks_empty_segment_from_double_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");

        let phase_a = [TestEvent::Add(1), TestEvent::Add(2)];
        let phase_b = [TestEvent::Add(10), TestEvent::Add(20)];

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &phase_a, 1);
        ja.rotate_segment().unwrap();
        ja.rotate_segment().unwrap(); // archive 000002 is empty
        let ja = append_events(ja, &phase_b, 1 + phase_a.len() as u64);
        drop(ja);

        // Sanity: the empty middle archive exists and starts where the
        // live segment also starts (no sequence consumed by rotation).
        let empty_archive = dir.path().join("journal.bin.000002");
        assert!(empty_archive.exists());
        let info = melin_journal::segment::read_header_info(&empty_archive).unwrap();
        assert_eq!(info.starting_sequence, 1 + phase_a.len() as u64);

        let recovered = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        assert_eq!(recovered.app().total, 1 + 2 + 10 + 20);
    }

    /// A snapshot taken on a fresh journal (sequence 0, chain hash =
    /// the segment anchor) round-trips through snapshot recovery.
    /// Companion negative test below proves the same anchor compare
    /// rejects an empty snapshot from a *different* lineage.
    #[test]
    fn fresh_snapshot_at_sequence_zero_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        ja.save_snapshot(&snap_path).unwrap();
        drop(ja);

        let (_, snap_seq, _, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(snap_seq, 0, "fresh snapshot anchors at sequence 0");

        let recovered = TestApp_::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(*recovered.app(), TestApp::new());
        assert_eq!(recovered.next_sequence(), 1);
    }

    /// An empty snapshot from another lineage (different random anchor)
    /// is rejected by the boundary anchor compare even though both
    /// sides hold zero events — the random anchor is what makes two
    /// independent histories unconfusable.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn recover_from_snapshot_rejects_foreign_empty_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let journal_a = dir.path().join("a.journal");
        let journal_b = dir.path().join("b.journal");
        let snap_a = dir.path().join("a.snap");

        let ja = TestApp_::create(TestApp::new(), &journal_a).unwrap();
        ja.save_snapshot(&snap_a).unwrap();
        drop(ja);
        drop(TestApp_::create(TestApp::new(), &journal_b).unwrap());

        let err = match TestApp_::recover_from_snapshot(&snap_a, &journal_b) {
            Ok(_) => panic!("expected rejection of foreign empty snapshot"),
            Err(e) => e,
        };
        assert!(
            matches!(err, JournaledAppError::SnapshotChainMismatch { .. }),
            "expected SnapshotChainMismatch, got {err:?}"
        );
    }

    /// Phase B crash with an *empty trailing archive*: rotate twice
    /// back-to-back, then crash between the second rotation's rename
    /// and the new live file's creation. On disk: a populated archive,
    /// an empty archive, and no live. Recovery must synthesize the live
    /// continuing from the empty archive's starting sequence (rotation
    /// consumed nothing) with its anchor chained through the empty
    /// segment.
    #[test]
    fn recover_synthesizes_live_after_phase_b_crash_with_empty_trailing_archive() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");

        let events = [TestEvent::Add(5), TestEvent::Add(7)];
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &events, 1);
        ja.rotate_segment().unwrap();
        ja.rotate_segment().unwrap(); // archive 000002 is empty
        drop(ja);
        // Simulate the crash window: the (empty) live vanishes.
        std::fs::remove_file(&journal_path).unwrap();

        let recovered = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        assert_eq!(recovered.app().total, 5 + 7);
        assert_eq!(
            recovered.next_sequence(),
            1 + events.len() as u64,
            "synthesized live must continue from the empty archive's start"
        );

        // The synthesized live must be appendable and re-recoverable —
        // its anchor chained through the empty archive.
        let ja = append_events(recovered, &[TestEvent::Add(100)], 1 + events.len() as u64);
        drop(ja);
        let re = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        assert_eq!(re.app().total, 5 + 7 + 100);
    }

    /// A *foreign* archive restored over a trimmed prefix — internally
    /// valid, right starting sequence, wrong lineage — is rejected by
    /// the snapshot chain cross-check when the anchor entry is
    /// observed. The random per-lineage anchor is what guarantees two
    /// independent histories can never produce the same chain value.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn recover_from_snapshot_rejects_foreign_covering_archive() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let events = [TestEvent::Add(1), TestEvent::Add(2)];

        // Genuine lineage: events, snapshot at the tail, rotate.
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &events, 1);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        drop(ja);

        // Foreign lineage with the same shape (same event count, same
        // request seqs — only the random anchor and payloads differ).
        let foreign_dir = tempfile::tempdir().unwrap();
        let foreign_live = foreign_dir.path().join("journal.bin");
        let fja = TestApp_::create(TestApp::new(), &foreign_live).unwrap();
        let mut fja = append_events(fja, &[TestEvent::Add(9), TestEvent::Add(8)], 1);
        fja.rotate_segment().unwrap();
        drop(fja);

        // "Restore" the foreign archive over the genuine one.
        let archive = dir.path().join("journal.bin.000001");
        std::fs::remove_file(&archive).unwrap();
        std::fs::copy(foreign_dir.path().join("journal.bin.000001"), &archive).unwrap();

        let err = match TestApp_::recover_from_snapshot(&snap_path, &journal_path) {
            Ok(_) => panic!("expected rejection of foreign covering archive"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("snapshot chain hash mismatch") || msg.contains("segment chain break"),
            "expected a chain rejection, got: {msg}"
        );
    }

    /// Recovery refuses to pair a snapshot with a journal that doesn't
    /// reach the snapshot's recorded sequence. Without this guard a
    /// stale journal restored from before the snapshot would be
    /// silently accepted, orphaning every journal record and erasing
    /// the audit trail that proves the snapshot's state was lawfully
    /// produced.
    #[test]
    fn recover_from_snapshot_rejects_stale_journal() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        // Build state and snapshot at the journal's tail.
        let events = [TestEvent::Add(3), TestEvent::Add(5), TestEvent::Add(7)];
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &events, 1);
        drop(ja);
        let ja = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        ja.save_snapshot(&snap_path).unwrap();
        drop(ja);

        let (_app, snap_seq, _hash, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert!(snap_seq > 0);

        // Replace the journal with a stale copy that holds only the
        // first event — its tail sequence sits well below `snap_seq`.
        std::fs::remove_file(&journal_path).unwrap();
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let _ja = append_events(ja, &events[..1], 1);
        drop(_ja);

        let err = match TestApp_::recover_from_snapshot(&snap_path, &journal_path) {
            Ok(_) => panic!("expected recovery to reject snapshot/journal mismatch"),
            Err(e) => e,
        };
        match err {
            JournaledAppError::SnapshotAnchorMissing {
                snap_sequence,
                journal_last_seq,
            } => {
                assert_eq!(snap_sequence, snap_seq);
                assert!(
                    journal_last_seq < snap_sequence,
                    "journal tail {journal_last_seq} should sit below snap_sequence {snap_sequence}"
                );
            }
            other => panic!("expected SnapshotAnchorMissing, got {other:?}"),
        }
    }

    /// Recovery refuses to pair a snapshot whose recorded chain hash at
    /// the anchor sequence doesn't match the journal's chain hash at
    /// the same sequence. Without this guard a snapshot from one
    /// cluster paired with another cluster's journal — or a journal
    /// with tampered entries — would be silently accepted and the
    /// recovered state would diverge from the events that produced the
    /// snapshot. The cross-check fires when the entry at the anchor
    /// sequence is observed during replay.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn recover_from_snapshot_rejects_chain_hash_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        // Three events → seqs 1,2,3. snap_seq = 3.
        let events = [TestEvent::Add(3), TestEvent::Add(5), TestEvent::Add(7)];
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &events, 1);
        drop(ja);
        let ja = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        ja.save_snapshot(&snap_path).unwrap();
        drop(ja);

        // Round-trip the snapshot, re-saving with the same app state and
        // sequence but a deliberately wrong chain hash. The journal
        // itself is unchanged, so recovery should compute the original
        // chain hash at the anchor sequence and detect the mismatch.
        let (loaded_app, snap_seq, real_hash, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_ne!(
            real_hash, [0u8; 32],
            "hash-chain feature must produce a non-sentinel hash for this test"
        );
        let bad_hash = [0xFF; 32];
        snapshot::save::<TestApp>(&loaded_app, snap_seq, bad_hash, 0, &snap_path).unwrap();

        let err = match TestApp_::recover_from_snapshot(&snap_path, &journal_path) {
            Ok(_) => panic!("expected recovery to reject chain-hash mismatch"),
            Err(e) => e,
        };
        match err {
            JournaledAppError::SnapshotChainMismatch {
                snap_sequence,
                expected_chain_hash,
                actual_chain_hash,
            } => {
                assert_eq!(snap_sequence, snap_seq);
                assert_eq!(expected_chain_hash, bad_hash);
                assert_eq!(actual_chain_hash, real_hash);
            }
            other => panic!("expected SnapshotChainMismatch, got {other:?}"),
        }
    }

    /// Multi-segment variant of the chain-hash mismatch check: the
    /// snapshot anchor sits in a sealed archive (not the live
    /// segment), so the cross-check must fire from inside the
    /// archive-walking loop in `recover_inner` and propagate via `?`
    /// out of the loop. Companion to the single-segment negative test
    /// above — proves the second call site of `replay_segment` is
    /// wired correctly and that later archives / the live segment
    /// aren't walked once the mismatch is hit.
    #[cfg(feature = "hash-chain")]
    #[test]
    fn recover_from_snapshot_rejects_chain_hash_mismatch_in_archive() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap.bin");

        let phase_a = [TestEvent::Add(1), TestEvent::Add(2)];
        let phase_b = [TestEvent::Add(10), TestEvent::Add(20)];

        // Phase A: 2 events at seqs 1,2; snapshot anchors at seq 2 —
        // the last event before rotation, which ends up inside archive
        // 000001 after the rotate below.
        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &phase_a, 1);
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        // Phase B lives in the new live segment; recovery must not
        // reach it once the archive's mismatch is detected.
        let ja = append_events(ja, &phase_b, 1 + phase_a.len() as u64);
        drop(ja);

        // Round-trip the snapshot with a deliberately wrong chain hash
        // at the same anchor sequence.
        let (loaded_app, snap_seq, real_hash, _) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_ne!(
            real_hash, [0u8; 32],
            "hash-chain feature must produce a non-sentinel hash for this test"
        );
        let bad_hash = [0xAA; 32];
        snapshot::save::<TestApp>(&loaded_app, snap_seq, bad_hash, 0, &snap_path).unwrap();

        let err = match TestApp_::recover_from_snapshot(&snap_path, &journal_path) {
            Ok(_) => panic!("expected recovery to reject archived chain-hash mismatch"),
            Err(e) => e,
        };
        match err {
            JournaledAppError::SnapshotChainMismatch {
                snap_sequence,
                expected_chain_hash,
                actual_chain_hash,
            } => {
                assert_eq!(snap_sequence, snap_seq);
                assert_eq!(expected_chain_hash, bad_hash);
                assert_eq!(actual_chain_hash, real_hash);
            }
            other => panic!("expected SnapshotChainMismatch, got {other:?}"),
        }
    }

    /// Exhaustive crash simulation: truncate the journal at every byte
    /// from the file header through the valid-data end, and verify
    /// recovery succeeds at each cut. After each recovery, append one
    /// more event and re-recover to prove the recovered writer is
    /// usable, not just readable. Complements the per-phase crash tests
    /// above by sweeping the full corruption-boundary space (mid-CRC,
    /// mid-payload, partial header, …) rather than hitting fixed phases.
    ///
    /// Runs across all available cores. Each iteration is independent
    /// (its own work file), and per-iteration cost is dominated by
    /// kernel I/O wait (`fallocate` + `fdatasync`), so the workers add
    /// negligible CPU pressure for neighbouring tests.
    #[test]
    fn crash_at_every_byte_offset_recovers() {
        // Shrink the prealloc chunk so each recover()-then-append cycle
        // doesn't pay the default 256 MiB fallocate cost. The guard
        // scopes the override to this test and serialises with any
        // sibling test using the same mechanism — without it, the old
        // permanent setter could let an 8 KiB override (from
        // sector_writer's regression test, were it in the same binary)
        // leak across siblings and corrupt assumptions.
        let _prealloc_guard = melin_journal::test_utils::PreallocOverrideGuard::new(1024 * 1024);

        let dir = tempfile::tempdir().unwrap();
        let original = dir.path().join("original.journal");

        let events = [
            TestEvent::Add(3),
            TestEvent::Add(5),
            TestEvent::Add(7),
            TestEvent::Add(11),
            TestEvent::Add(13),
            TestEvent::Add(17),
        ];
        let ja = TestApp_::create(TestApp::new(), &original).unwrap();
        let ja = append_events(ja, &events, 1);
        drop(ja);

        let end = valid_data_end(&original);
        let header_end = melin_journal::codec::FILE_HEADER_SIZE as u64;
        assert!(end > header_end, "journal should have data beyond header");

        // Shrink the original to its valid data size. The pre-allocated
        // tail (1 MiB under the override above) would otherwise be
        // copied per iteration.
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&original)
                .unwrap();
            f.set_len(end).unwrap();
        }

        let num_threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let work_dir = dir.path().to_path_buf();
        std::thread::scope(|scope| {
            for tid in 0..num_threads {
                let original = &original;
                let work_dir = &work_dir;
                scope.spawn(move || {
                    let work = work_dir.join(format!("work-{tid}.journal"));
                    // Stride truncation offsets across threads so each
                    // worker sees a mix of small and large truncations.
                    let mut trunc_at = header_end + tid as u64;
                    while trunc_at <= end {
                        std::fs::copy(original, &work).unwrap();
                        {
                            let f = std::fs::OpenOptions::new().write(true).open(&work).unwrap();
                            f.set_len(trunc_at).unwrap();
                        }

                        let je = TestApp_::recover(TestApp::new(), &work).unwrap();
                        assert!(je.next_sequence() >= 1, "seq underflow at byte {trunc_at}");

                        // Append + re-recover to prove the recovered
                        // writer is usable, not just readable.
                        let ja = append_events(je, &[TestEvent::Add(1)], events.len() as u64 + 1);
                        drop(ja);
                        let je2 = TestApp_::recover(TestApp::new(), &work).unwrap();
                        assert!(
                            je2.next_sequence() >= 2,
                            "double-recovery seq too low at byte {trunc_at}"
                        );

                        trunc_at += num_threads as u64;
                    }
                });
            }
        });
    }

    /// Crash during/after rotation: snapshot + rotate + post-rotation
    /// events, then truncate the new live segment at every byte and
    /// recover from snapshot + truncated journal. Every truncation must
    /// produce a `next_sequence` in `(snap_seq, final_seq]` — the
    /// snapshot always covers pre-rotation state, and truncation cannot
    /// fabricate sequences past what was actually written. The tail of
    /// the test covers the "live file missing entirely after rotation"
    /// edge case, recovering from snapshot alone via `from_parts` +
    /// `create_continuing`.
    #[test]
    fn crash_during_snapshot_rotation_recovers() {
        // Shrink the prealloc chunk so each iteration's
        // `recover_from_snapshot` doesn't pay the default 256 MiB
        // fallocate for the new live segment. RAII guard scopes the
        // override to this test and serialises with any sibling test
        // using the same mechanism.
        let _prealloc_guard = melin_journal::test_utils::PreallocOverrideGuard::new(1024 * 1024);

        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("rotation.journal");
        let snap_path = dir.path().join("rotation.snapshot");

        let pre = [TestEvent::Add(3), TestEvent::Add(5), TestEvent::Add(7)];
        let post = [TestEvent::Add(11), TestEvent::Add(13)];

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &pre, 1);
        drop(ja);
        // Recover to populate app state from journal, then snapshot —
        // `append_events` doesn't apply to the app on the write path,
        // so an immediate snapshot would capture total=0 and trivialise
        // the missing-journal assertion at the end.
        let mut ja = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        ja.save_snapshot(&snap_path).unwrap();
        ja.rotate_segment().unwrap();
        let ja = append_events(ja, &post, 1 + pre.len() as u64);
        let final_seq = ja.next_sequence();
        drop(ja);

        let (snap_app, snap_seq, snap_chain_hash, _) =
            snapshot::load::<TestApp>(&snap_path).unwrap();
        let snap_total = snap_app.total;
        assert!(snap_total > 0, "snapshot must capture pre-rotation state");

        let end = valid_data_end(&journal_path);
        let header_end = melin_journal::codec::FILE_HEADER_SIZE as u64;
        let work = dir.path().join("work.journal");

        // The pre-rotation segment is what holds the snapshot's anchor
        // entry (snap_sequence sits at the tail of `pre`). In production
        // archives sit next to the live in the same directory; mirror
        // that here so `list_archives(work)` finds the anchor and
        // recovery's snapshot/journal cross-check passes. Without this
        // copy the work-path journal omits the archive entirely and
        // looks like a stale-journal misconfiguration.
        let archive_src = dir.path().join("rotation.journal.000001");
        let archive_dst = dir.path().join("work.journal.000001");
        std::fs::copy(&archive_src, &archive_dst).unwrap();

        // Shrink the new live to its valid data size to avoid copying
        // the 256 MiB pre-allocated tail per iteration.
        {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(&journal_path)
                .unwrap();
            f.set_len(end).unwrap();
        }

        for trunc_at in header_end..=end {
            std::fs::copy(&journal_path, &work).unwrap();
            {
                let f = std::fs::OpenOptions::new().write(true).open(&work).unwrap();
                f.set_len(trunc_at).unwrap();
            }

            let je = TestApp_::recover_from_snapshot(&snap_path, &work).unwrap();
            assert!(
                je.next_sequence() <= final_seq,
                "seq overshoot at byte {trunc_at}: {} > {final_seq}",
                je.next_sequence()
            );
            assert!(
                je.next_sequence() > snap_seq,
                "seq undershot snapshot at byte {trunc_at}"
            );
        }

        // Live file missing entirely after rotation — recover from
        // snapshot alone via from_parts + create_continuing, the same
        // path the server's init takes.
        // ok(): best-effort cleanup; the assertion below is what
        // actually guards the path.
        std::fs::remove_file(&journal_path).ok();
        let writer =
            BufferedWriter::create_continuing(&journal_path, snap_seq + 1, snap_chain_hash)
                .unwrap();
        let je = JournaledApp::from_parts(snap_app, writer, 0);
        assert_eq!(je.app().total, snap_total);
    }

    /// Helper for the byte-sweep crash tests: walk every entry in the
    /// journal and return the byte offset where the valid data ends
    /// (before any pre-allocated tail).
    fn valid_data_end(path: &Path) -> u64 {
        let mut reader = JournalReader::<TestEvent>::open(path).unwrap();
        while reader.next_entry().unwrap().is_some() {}
        reader.valid_file_end()
    }

    /// Phase D crash: rotation completed and the new live has begun
    /// accepting events, but the in-memory batch was not yet fsynced
    /// when the process died. Per the persist-before-ack contract, those
    /// in-flight events were never acknowledged to the client and must
    /// be discarded — recovery sees only what the durable storage has,
    /// which is the archive's contents plus an empty live segment.
    #[test]
    fn recover_after_phase_d_crash_drops_unflushed_events() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");

        let archived_events = [TestEvent::Add(10), TestEvent::Add(20)];
        let expected_durable: u64 = archived_events
            .iter()
            .map(|e| match e {
                TestEvent::Add(n) => *n,
                _ => 0,
            })
            .sum();

        let ja = TestApp_::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &archived_events, 1);
        let (app, mut writer) = ja.into_parts();
        writer.rotate_segment().unwrap();
        // Encode an event into the new live's in-memory batch but DON'T
        // flush. The bytes live in `batch_buf` only; the on-disk live
        // is empty past its header.
        let unflushed_seq = writer.allocate_sequence();
        writer
            .encode_event(
                unflushed_seq,
                /* timestamp_ns */ 999_000,
                &JournalEvent::App(TestEvent::Add(99_999)),
                /* key_hash */ 1,
                /* request_seq */ 1 + archived_events.len() as u64,
            )
            .unwrap();
        // Drop without flushing — that's the simulated crash.
        drop(writer);
        drop(app);

        let recovered = TestApp_::recover(TestApp::new(), &journal_path).unwrap();
        // The 99_999 event was never durable; only the archived ones
        // count toward the recovered total.
        assert_eq!(recovered.app().total, expected_durable);
    }
}
