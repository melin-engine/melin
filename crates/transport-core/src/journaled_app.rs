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
//! - [`rotate`]: snapshot + archive old journal + start fresh.
//! - [`into_parts`]: hand the (app, writer) pair to the disruptor
//!   pipeline.
//!
//! This crate is application-agnostic — the journal replay goes through
//! `Application::apply` / `Application::tick`, and the snapshot payload
//! is whatever bytes `A::snapshot`/`A::restore` round-trip.

use std::path::{Path, PathBuf};

use melin_app::{Application, ApplyCtx};
use melin_journal::{JournalError, JournalEvent, JournalReader, JournalWriter};

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
/// the next free sequence.
pub struct JournaledApp<A: Application> {
    app: A,
    writer: JournalWriter<A::Event>,
}

impl<A: Application> JournaledApp<A> {
    /// Create a new journaled app with a fresh journal file. The
    /// caller supplies the app so production builds can pick an
    /// appropriately pre-sized constructor (e.g.
    /// `Exchange::with_capacity()`) rather than relying on `Default`.
    pub fn create(app: A, journal_path: &Path) -> Result<Self, JournaledAppError> {
        let writer = JournalWriter::<A::Event>::create(journal_path)?;
        Ok(Self { app, writer })
    }

    /// Recover from an existing journal. Replays every entry into the
    /// caller-supplied empty app, then reopens the writer for
    /// appending.
    pub fn recover(app: A, journal_path: &Path) -> Result<Self, JournaledAppError> {
        let mut reader = JournalReader::<A::Event>::open(journal_path)?;
        let mut app = app;
        let mut reports: Vec<A::Report> = Vec::new();
        let mut last_drain_ns: u64 = 0;

        loop {
            match reader.next_entry() {
                Ok(Some(entry)) => {
                    replay_entry(
                        &mut app,
                        &entry.event,
                        entry.timestamp_ns,
                        entry.key_hash,
                        entry.request_seq,
                        &mut last_drain_ns,
                        &mut reports,
                    );
                    reports.clear();
                }
                Ok(None) => break,
                Err(JournalError::SequenceGap { expected, actual }) => {
                    tracing::warn!(
                        expected,
                        actual,
                        "sequence gap during recovery — truncating at gap"
                    );
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let last_seq = reader.last_sequence().unwrap_or(0);
        let valid_end = reader.valid_file_end();
        let chain_hash = reader.chain_hash();
        let events_since_checkpoint = reader.events_since_checkpoint();
        let writer = JournalWriter::<A::Event>::open_append(
            journal_path,
            last_seq,
            valid_end,
            chain_hash,
            events_since_checkpoint,
        )?;

        Ok(Self { app, writer })
    }

    /// Recover from a snapshot plus a journal file.
    ///
    /// Loads the snapshot to restore state, then replays only journal
    /// entries strictly after the snapshot's recorded sequence.
    pub fn recover_from_snapshot(
        snapshot_path: &Path,
        journal_path: &Path,
    ) -> Result<Self, JournaledAppError> {
        let (mut app, snap_sequence, snap_chain_hash) = snapshot::load::<A>(snapshot_path)?;
        let mut reader = JournalReader::<A::Event>::open(journal_path)?;

        // Seed the reader's hash chain from the snapshot so verification
        // continues from the snapshot boundary rather than requiring replay
        // from genesis.
        reader.seed_chain_hash(snap_chain_hash, snap_sequence);

        let mut reports: Vec<A::Report> = Vec::new();
        let mut last_drain_ns: u64 = 0;

        loop {
            match reader.next_entry() {
                Ok(Some(entry)) => {
                    if entry.sequence > snap_sequence {
                        replay_entry(
                            &mut app,
                            &entry.event,
                            entry.timestamp_ns,
                            entry.key_hash,
                            entry.request_seq,
                            &mut last_drain_ns,
                            &mut reports,
                        );
                        reports.clear();
                    }
                }
                Ok(None) => break,
                Err(JournalError::SequenceGap { expected, actual }) => {
                    tracing::warn!(
                        expected,
                        actual,
                        "sequence gap during snapshot recovery — truncating at gap"
                    );
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        let last_seq = reader.last_sequence().unwrap_or(snap_sequence);
        let valid_end = reader.valid_file_end();
        let chain_hash = reader.chain_hash();
        let events_since_checkpoint = reader.events_since_checkpoint();
        let writer = JournalWriter::<A::Event>::open_append(
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

    /// Rotate the journal: snapshot, archive old journal as `<path>.N`,
    /// and start a new journal continuing the sequence. Uses the
    /// current chain hash as the genesis for cryptographic continuity
    /// across rotation boundaries.
    pub fn rotate(&mut self, snapshot_path: &Path) -> Result<(), JournaledAppError> {
        self.save_snapshot(snapshot_path)?;
        let journal_path = self.writer.path().to_path_buf();
        rotate_file(&journal_path)?;
        let next_seq = self.writer.next_sequence();
        let genesis = self.writer.chain_hash().unwrap_or([0u8; 32]);
        self.writer =
            JournalWriter::<A::Event>::create_continuing(&journal_path, next_seq, genesis)?;
        Ok(())
    }

    /// Size of the current journal file in bytes.
    pub fn journal_size(&self) -> u64 {
        self.writer.write_pos()
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
    pub fn from_parts(app: A, writer: JournalWriter<A::Event>) -> Self {
        Self { app, writer }
    }

    /// Decompose into parts for the pipeline architecture.
    pub fn into_parts(self) -> (A, JournalWriter<A::Event>) {
        (self.app, self.writer)
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
            let ctx = ApplyCtx {
                now_ns: timestamp_ns,
                journal_sequence: 0,
                active_connections: 0,
                events_processed: 0,
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
    }
}

fn rotate_file(path: &Path) -> Result<(), std::io::Error> {
    let mut max_n = 0u32;
    loop {
        let archive = format!("{}.{}", path.display(), max_n + 1);
        if !Path::new(&archive).exists() {
            break;
        }
        max_n += 1;
    }
    for n in (1..=max_n).rev() {
        let from = format!("{}.{n}", path.display());
        let to = format!("{}.{}", path.display(), n + 1);
        std::fs::rename(&from, &to)?;
    }
    let archive_1 = format!("{}.1", path.display());
    std::fs::rename(path, PathBuf::from(&archive_1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestEvent};
    use melin_journal::JournalEvent;

    /// Write events with auto-allocated sequences and fsync them to disk.
    /// Each event is keyed on `(key_hash = 1, request_seq = first_seq + idx)`
    /// so tests that append in multiple phases can offset `first_seq` to
    /// avoid dedup collisions across calls.
    fn append_events(
        ja: JournaledApp<TestApp>,
        events: &[TestEvent],
        first_seq: u64,
    ) -> JournaledApp<TestApp> {
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

        let ja = JournaledApp::create(TestApp::new(), &path).unwrap();
        // Sequences start at 1: seq=0 is the InputSlot "not yet allocated"
        // sentinel the journal stage branches on (see pipeline.rs:488).
        // With `hash-chain`, `create` writes a GenesisHash entry first,
        // consuming seq 1 and leaving next_sequence at 2.
        let genesis_overhead: u64 = if cfg!(feature = "hash-chain") { 1 } else { 0 };
        assert_eq!(ja.next_sequence(), 1 + genesis_overhead);
        drop(ja);

        let recovered = JournaledApp::recover(TestApp::new(), &path).unwrap();
        assert_eq!(*recovered.app(), TestApp::new());
    }

    #[test]
    fn recover_replays_events_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("journal.bin");

        let events = [TestEvent::Add(3), TestEvent::Add(7), TestEvent::Add(100)];
        let ja = JournaledApp::create(TestApp::new(), &path).unwrap();
        let ja = append_events(ja, &events, 1);
        drop(ja);

        let recovered = JournaledApp::recover(TestApp::new(), &path).unwrap();
        assert_eq!(*recovered.app(), expected_state(&events, 1));
    }

    #[test]
    fn save_snapshot_round_trips_via_generic_load() {
        let dir = tempfile::tempdir().unwrap();
        let journal_path = dir.path().join("journal.bin");
        let snap_path = dir.path().join("snap");

        let events = [TestEvent::Add(10), TestEvent::Add(20)];
        let ja = JournaledApp::create(TestApp::new(), &journal_path).unwrap();
        drop(append_events(ja, &events, 1)); // journal write, writer drops
        let ja = JournaledApp::recover(TestApp::new(), &journal_path).unwrap();
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
        let ja = JournaledApp::create(TestApp::new(), &journal_path).unwrap();
        let ja = append_events(ja, &pre, 1);
        drop(ja);
        let ja = JournaledApp::recover(TestApp::new(), &journal_path).unwrap();
        ja.save_snapshot(&snap_path).unwrap();

        // Phase 2: append post events (request_seqs 3..=4 — disjoint from
        // pre, so they pass dedup) to the same journal file; no rotation.
        let ja = append_events(ja, &post, pre.len() as u64 + 1);
        drop(ja);

        // Phase 3: recover_from_snapshot should load the snapshot (state
        // after `pre`) and replay only the entries strictly after the
        // snapshot's sequence (i.e. `post`).
        let recovered =
            JournaledApp::<TestApp>::recover_from_snapshot(&snap_path, &journal_path).unwrap();

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

        let ja = JournaledApp::create(TestApp::new(), &path).unwrap();
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

        let recovered = JournaledApp::recover(TestApp::new(), &path).unwrap();
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
        let ja = JournaledApp::create(TestApp::new(), &journal_path).unwrap();
        let mut ja = append_events(ja, &events, 1);
        let pre_rotate_next_seq = ja.next_sequence();
        let pre_rotate_state = TestApp {
            total: ja.app().total,
            ticks: ja.app().ticks,
            key_hwm: ja.app().key_hwm.clone(),
        };

        ja.rotate(&snap_path).unwrap();

        // Archived journal lives at `.1`.
        let archived = dir.path().join("journal.bin.1");
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
        let recovered =
            JournaledApp::<TestApp>::recover_from_snapshot(&snap_path, &journal_path).unwrap();
        assert_eq!(*recovered.app(), pre_rotate_state);
    }
}
