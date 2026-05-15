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
}

impl std::fmt::Display for JournaledAppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Journal(e) => write!(f, "journal: {e}"),
            Self::Snapshot(e) => write!(f, "snapshot: {e}"),
            Self::Io(e) => write!(f, "I/O: {e}"),
        }
    }
}

impl std::error::Error for JournaledAppError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(e) => Some(e),
            Self::Snapshot(e) => Some(e),
            Self::Io(e) => Some(e),
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
}

impl<A: Application, W: JournalWrite<A::Event>> JournaledApp<A, W> {
    /// Create a new journaled app with a fresh journal file. The caller
    /// supplies the app so production builds can pick an appropriately
    /// pre-sized constructor (e.g. `Exchange::with_capacity()`) rather
    /// than relying on `Default`.
    pub fn create(app: A, journal_path: &Path) -> Result<Self, JournaledAppError> {
        let writer = W::create(journal_path)?;
        Ok(Self { app, writer })
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
        let (app, snap_sequence, snap_chain_hash) = snapshot::load::<A>(snapshot_path)?;
        Self::recover_inner(app, journal_path, Some((snap_sequence, snap_chain_hash)))
    }

    /// Shared multi-segment recovery driver.
    ///
    /// `snapshot` carries `(sequence, chain_hash)` when the caller has
    /// already restored from a snapshot; events with `seq <= sequence`
    /// are skipped during replay but still walked so per-segment chain
    /// validation runs.
    ///
    /// Cross-segment chain continuity is enforced: each segment's
    /// `GenesisHash` payload must equal the previous segment's tail
    /// chain hash. A break is reported as
    /// [`JournalError::SegmentChainBreak`] — distinct from a within-
    /// segment `HashChainMismatch` so operators can locate which
    /// archive on disk is at fault.
    fn recover_inner(
        mut app: A,
        journal_path: &Path,
        snapshot: Option<(u64, [u8; 32])>,
    ) -> Result<Self, JournaledAppError> {
        let archives = melin_journal::segment::list_archives(journal_path)?;

        let snap_sequence = snapshot.map(|(s, _)| s).unwrap_or(0);

        let mut reports: Vec<A::Report> = Vec::new();
        let mut last_drain_ns: u64 = 0;
        // Tail chain hash carried forward across segments. `None` means
        // no boundary check has anything to compare against yet — the
        // very first segment we walk has no predecessor in this run.
        let mut prev_tail_hash: Option<[u8; 32]> = None;
        // Highest sequence observed across walked archives. Used to seed
        // a synthesized live segment when a crash interrupted rotation
        // between the live → archive rename and the new live file's
        // creation.
        let mut last_seq_seen: u64 = snap_sequence;

        // --- Walk each sealed archive in monotonic order ---
        for (idx, archive_path) in &archives {
            let mut reader = JournalReader::<A::Event>::open(archive_path)?;
            replay_segment(
                &mut reader,
                &mut app,
                snap_sequence,
                &mut last_drain_ns,
                &mut reports,
                /* allow_partial_tail = */ false,
            )?;
            verify_segment_boundary(*idx, prev_tail_hash, reader.genesis_payload())?;
            // Carry forward only when this segment actually had a chain
            // (hash-chain feature on, segment had at least its
            // GenesisHash entry). Otherwise leave `prev_tail_hash`
            // unchanged so the next boundary still gets a meaningful
            // compare target.
            if let Some(h) = reader.chain_hash() {
                prev_tail_hash = Some(h);
            }
            if let Some(seq) = reader.last_sequence() {
                last_seq_seen = last_seq_seen.max(seq);
            }
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
            let genesis = prev_tail_hash.unwrap_or([0u8; 32]);
            let writer = W::create_continuing(journal_path, last_seq_seen + 1, genesis)?;
            return Ok(Self { app, writer });
        }

        let mut reader = JournalReader::<A::Event>::open(journal_path)?;
        // The live segment may have a partial-tail crash: replay loop
        // tolerates `SequenceGap` by stopping early, mirroring legacy
        // behaviour.
        replay_segment(
            &mut reader,
            &mut app,
            snap_sequence,
            &mut last_drain_ns,
            &mut reports,
            /* allow_partial_tail = */ true,
        )?;
        verify_segment_boundary(0, prev_tail_hash, reader.genesis_payload())?;

        let last_seq = reader.last_sequence().unwrap_or(snap_sequence);
        let valid_end = reader.valid_file_end();
        let chain_hash = reader.chain_hash();
        let events_since_checkpoint = reader.events_since_checkpoint();
        let writer = W::open_append(
            journal_path,
            last_seq,
            valid_end,
            chain_hash,
            events_since_checkpoint,
        )?;

        Ok(Self { app, writer })
    }

    /// Save a snapshot of the current application state. The snapshot
    /// records the last journal sequence and current chain hash so
    /// recovery can resume both.
    pub fn save_snapshot(&self, snapshot_path: &Path) -> Result<(), JournaledAppError> {
        let seq = self.writer.next_sequence().saturating_sub(1);
        let chain_hash = self.writer.chain_hash().unwrap_or([0u8; 32]);
        snapshot::save::<A>(&self.app, seq, chain_hash, snapshot_path)?;
        Ok(())
    }

    /// Archive the live journal segment to its next monotonic slot
    /// (`<path>.NNNNNN`) and open a fresh live segment continuing the
    /// sequence. The new segment's `GenesisHash` carries the chain
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
    /// "snapshot-only" recovery path (journal missing post-rotation).
    pub fn from_parts(app: A, writer: W) -> Self {
        Self { app, writer }
    }

    /// Decompose into parts for the pipeline architecture.
    pub fn into_parts(self) -> (A, W) {
        (self.app, self.writer)
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

    /// Mutable access to the writer for tests that need to journal raw
    /// `JournalEvent` variants the public API doesn't expose (e.g. a
    /// deliberately-mismatched checkpoint to exercise divergence
    /// handling).
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
    reports: &mut Vec<A::Report>,
) {
    // Rebuild per-key HWM and capture whether this was a new request.
    // The journal stage writes events before the matching stage dedups,
    // so the journal can contain duplicates the primary rejected without
    // calling `apply`. Replay must skip `apply` on those entries or
    // state will diverge from the live primary (e.g. a retried deposit
    // applied twice). For transport events (`Tick`, `GenesisHash`,
    // `Checkpoint`) `key_hash == 0`, which `check_request_seq` exempts
    // — `is_new` is always true there.
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
        JournalEvent::GenesisHash { .. } | JournalEvent::Checkpoint { .. } => {
            // Chain metadata — handled by the reader itself during
            // `next_entry`; no application action.
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
/// is `0`, accepting all events).
///
/// `allow_partial_tail` controls how `SequenceGap` is treated: archived
/// segments are sealed and any gap is corruption (returned as an error);
/// the live segment may have a torn tail from a crash, so a gap
/// terminates replay cleanly.
fn replay_segment<A: Application>(
    reader: &mut JournalReader<A::Event>,
    app: &mut A,
    snap_sequence: u64,
    last_drain_ns: &mut u64,
    reports: &mut Vec<A::Report>,
    allow_partial_tail: bool,
) -> Result<(), JournaledAppError> {
    loop {
        match reader.next_entry() {
            Ok(Some(entry)) => {
                if entry.sequence > snap_sequence {
                    replay_entry(
                        app,
                        &entry.event,
                        entry.timestamp_ns,
                        entry.key_hash,
                        entry.request_seq,
                        last_drain_ns,
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

/// Verify that a segment's `GenesisHash` payload equals the previous
/// segment's tail chain hash. `index = 0` denotes the live segment in
/// diagnostics. No-op when either side is `None` (chain feature off, or
/// no predecessor in this recovery run).
fn verify_segment_boundary(
    index: u32,
    prev_tail: Option<[u8; 32]>,
    genesis_payload: Option<[u8; 32]>,
) -> Result<(), JournaledAppError> {
    if let (Some(expected), Some(actual)) = (prev_tail, genesis_payload)
        && expected != actual
    {
        return Err(JournalError::SegmentChainBreak {
            index,
            expected,
            actual,
        }
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestEvent};
    use melin_journal::{BufferedWriter, JournalEvent};

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
        JournaledApp::from_parts(app, writer)
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
        // sentinel the journal stage branches on (see pipeline.rs:488).
        // With `hash-chain`, `create` writes a GenesisHash entry first,
        // consuming seq 1 and leaving next_sequence at 2.
        let genesis_overhead: u64 = if cfg!(feature = "hash-chain") { 1 } else { 0 };
        assert_eq!(ja.next_sequence(), 1 + genesis_overhead);
        drop(ja);

        let recovered = TestApp_::recover(TestApp::new(), &path).unwrap();
        assert_eq!(*recovered.app(), TestApp::new());
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

        let (restored, seq, _chain) = snapshot::load::<TestApp>(&snap_path).unwrap();
        assert_eq!(restored, expected_state(&events, 1));
        // Sequences are 1-indexed; after N events, next_sequence = N + 1
        // and save_snapshot records the last issued sequence (next - 1) = N.
        // Under `hash-chain`, the genesis entry consumes an extra seq.
        let genesis_overhead: u64 = if cfg!(feature = "hash-chain") { 1 } else { 0 };
        assert_eq!(seq, events.len() as u64 + genesis_overhead);
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
        // Sequence continues past the archive cut. With `hash-chain`, the
        // new journal starts with a GenesisHash at `pre_rotate_next_seq`,
        // bumping next_sequence by 1 just like initial `create`.
        let genesis_overhead: u64 = if cfg!(feature = "hash-chain") { 1 } else { 0 };
        assert_eq!(ja.next_sequence(), pre_rotate_next_seq + genesis_overhead);
        // Snapshot captures the pre-rotate state.
        let (snap_app, _seq, _chain) = snapshot::load::<TestApp>(&snap_path).unwrap();
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
    /// segment's tail (post-rotation) is detectable as a SegmentChainBreak
    /// at recovery, because the next segment's GenesisHash no longer
    /// matches the tampered segment's tail chain hash.
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

        // Flip a bit in the middle of archive 000001 to change its
        // tail chain hash. The byte we touch is well past the file
        // header, in the body of the first event.
        let archive = dir.path().join("journal.bin.000001");
        let mut buf = std::fs::read(&archive).unwrap();
        // Find the first non-zero byte past the magic + header range
        // (skip 64 to clear the file header reliably on both 512 and
        // 4Kn devices; entries follow at sector_size offset, but the
        // first sector contains valid header magic + version fields,
        // so the first non-zero byte after offset 64 is in either the
        // header tail or the first entry — both work for inducing a
        // chain mismatch).
        let flip_at = buf
            .iter()
            .enumerate()
            .skip(64)
            .find(|(_, b)| **b != 0)
            .map(|(i, _)| i)
            .expect("archive should have at least one non-zero byte");
        buf[flip_at] ^= 0xFF;
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
        let msg = format!("{err}");
        assert!(
            msg.contains("hash chain mismatch")
                || msg.contains("segment chain break")
                || msg.contains("checksum mismatch")
                || msg.contains("corrupt entry"),
            "expected tamper-detection error, got: {msg}"
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
        // The new live starts with a GenesisHash at pre_crash_seq, so
        // next_sequence advances by one beyond the archive's tail.
        let genesis_overhead: u64 = if cfg!(feature = "hash-chain") { 1 } else { 0 };
        assert_eq!(recovered.next_sequence(), pre_crash_seq + genesis_overhead);

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
    /// live opened with `GenesisHash`), but no application events
    /// landed in the new live before the crash. On disk: archive(s) +
    /// live containing only the GenesisHash entry. Recovery walks both
    /// and reproduces the pre-rotation state exactly.
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

    /// Phase D crash: rotation completed and the new live has begun
    /// accepting events, but the in-memory batch was not yet fsynced
    /// when the process died. Per the persist-before-ack contract, those
    /// in-flight events were never acknowledged to the client and must
    /// be discarded — recovery sees only what the durable storage has,
    /// which is the archive's contents plus the new live's GenesisHash.
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
        // has just its GenesisHash entry.
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
