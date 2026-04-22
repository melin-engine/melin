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
    // Rebuild per-key HWM state so live dedup continues correctly post-recovery.
    let _ = app.check_request_seq(key_hash, request_seq);

    if timestamp_ns > *last_drain_ns {
        *last_drain_ns = timestamp_ns;
        app.tick(timestamp_ns, reports);
    }

    match event {
        JournalEvent::App(e) => {
            // Reports produced during replay are discarded — they already
            // went to the client at the time the event was accepted.
            let ctx = ApplyCtx {
                now_ns: timestamp_ns,
                journal_sequence: 0,
                active_connections: 0,
                events_processed: 0,
            };
            app.apply(*e, &ctx, reports);
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
