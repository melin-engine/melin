//! Runtime-selectable journal writer that wraps the two concrete
//! implementations behind a single type.
//!
//! Pipeline and engine code holds a `JournalWriter<E>` and never sees
//! the concrete underlying writer. The variant is chosen once at
//! construction (CLI flag → [`JournalWriterMode`]) and never changes
//! for the lifetime of the value, so every match below resolves to a
//! single arm under branch prediction — the dispatch is effectively
//! free on the hot path.
//!
//! Sector-only methods (io_uring registration, async submit/confirm,
//! sector-size, prepared-segment rotation) are deliberately reachable
//! only via [`JournalWriter::as_sector_mut`]. The pipeline's
//! `run_uring` path uses that accessor; the buffered variant never
//! enters `run_uring`, so the `Option::expect` there is unreachable by
//! construction.

use std::path::{Path, PathBuf};

use melin_app::AppEvent;

use crate::buffered_writer::BufferedWriter;
use crate::error::JournalError;
use crate::event::JournalEvent;
use crate::sector_writer::SectorWriter;

/// Selects which concrete writer [`JournalWriter`] wraps. Set once at
/// startup from the `--journal-writer` CLI flag (or its config-file
/// equivalent) and threaded through every construction site that
/// creates a journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournalWriterMode {
    /// `O_DIRECT` writes, sector-aligned, durability dependent on
    /// capacitor-backed PLP. Lowest-latency on enterprise NVMe with
    /// `VWC=0`; not durable on volatile-write-cache drives.
    Sector,
    /// Page-cache writes with explicit `fdatasync` per batch. Honest
    /// durability on any drive at the cost of one device flush per
    /// flush boundary on `VWC=1` drives. Default.
    #[default]
    Buffered,
}

impl JournalWriterMode {
    /// Parse the value of the `--journal-writer` flag. Accepts
    /// `sector` / `buffered`, case-insensitive.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "sector" => Ok(Self::Sector),
            "buffered" => Ok(Self::Buffered),
            other => Err(format!(
                "unknown journal writer mode '{other}'; expected 'sector' or 'buffered'"
            )),
        }
    }

    /// Stable string form used by the CLI and config serialisation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Sector => "sector",
            Self::Buffered => "buffered",
        }
    }
}

impl std::fmt::Display for JournalWriterMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for JournalWriterMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Runtime-selected journal writer. Holds either a [`SectorWriter`]
/// (O_DIRECT path, requires PLP) or a [`BufferedWriter`] (page cache
/// + fdatasync, honest durability anywhere).
pub enum JournalWriter<E: AppEvent> {
    Sector(SectorWriter<E>),
    Buffered(BufferedWriter<E>),
}

impl<E: AppEvent> JournalWriter<E> {
    /// Create a fresh journal of the given mode.
    pub fn create(mode: JournalWriterMode, path: &Path) -> Result<Self, JournalError> {
        match mode {
            JournalWriterMode::Sector => SectorWriter::create(path).map(Self::Sector),
            JournalWriterMode::Buffered => BufferedWriter::create(path).map(Self::Buffered),
        }
    }

    /// Create a fresh journal using the default mode
    /// ([`JournalWriterMode::Buffered`]). Exists to keep test setup
    /// free of the mode parameter — production call sites always go
    /// through [`create`] with an explicit mode from the CLI.
    pub fn create_default(path: &Path) -> Result<Self, JournalError> {
        Self::create(JournalWriterMode::default(), path)
    }

    /// Create a fresh journal that continues a previous segment's
    /// sequence numbers, anchored to `genesis_hash`.
    pub fn create_continuing(
        mode: JournalWriterMode,
        path: &Path,
        starting_sequence: u64,
        genesis_hash: [u8; 32],
    ) -> Result<Self, JournalError> {
        match mode {
            JournalWriterMode::Sector => {
                SectorWriter::create_continuing(path, starting_sequence, genesis_hash)
                    .map(Self::Sector)
            }
            JournalWriterMode::Buffered => {
                BufferedWriter::create_continuing(path, starting_sequence, genesis_hash)
                    .map(Self::Buffered)
            }
        }
    }

    /// Open an existing journal for appending after recovery.
    pub fn open_append(
        mode: JournalWriterMode,
        path: &Path,
        last_seq: u64,
        valid_end: u64,
        chain_hash: Option<[u8; 32]>,
        events_since_checkpoint: u64,
    ) -> Result<Self, JournalError> {
        match mode {
            JournalWriterMode::Sector => SectorWriter::open_append(
                path,
                last_seq,
                valid_end,
                chain_hash,
                events_since_checkpoint,
            )
            .map(Self::Sector),
            JournalWriterMode::Buffered => BufferedWriter::open_append(
                path,
                last_seq,
                valid_end,
                chain_hash,
                events_since_checkpoint,
            )
            .map(Self::Buffered),
        }
    }

    /// Selected mode. Useful for surfacing the active writer in
    /// metrics or operator-facing logs.
    pub fn mode(&self) -> JournalWriterMode {
        match self {
            Self::Sector(_) => JournalWriterMode::Sector,
            Self::Buffered(_) => JournalWriterMode::Buffered,
        }
    }

    /// Borrow the underlying sector writer for `run_uring`'s
    /// io_uring-specific methods. Returns `None` for the buffered
    /// variant — which the pipeline never reaches on the io_uring
    /// path because `JournalStage::run` dispatches by variant.
    pub fn as_sector_mut(&mut self) -> Option<&mut SectorWriter<E>> {
        match self {
            Self::Sector(w) => Some(w),
            Self::Buffered(_) => None,
        }
    }

    /// Shared-reference counterpart to [`as_sector_mut`] for the
    /// `&self` accessors on `SectorWriter` (`fd`, `sector_size`,
    /// `io_uring_rw_flags`).
    pub fn as_sector(&self) -> Option<&SectorWriter<E>> {
        match self {
            Self::Sector(w) => Some(w),
            Self::Buffered(_) => None,
        }
    }

    /// Infallible variant of [`as_sector`] used inside `run_uring`,
    /// which is only reached on the Sector path. The panic message is
    /// centralised here so every io_uring call site can call the
    /// accessor without restating the invariant.
    #[inline]
    pub fn unwrap_sector(&self) -> &SectorWriter<E> {
        self.as_sector()
            .expect("io_uring journal path requires Sector variant — Buffered routes to run_sync")
    }

    /// `&mut` counterpart to [`unwrap_sector`].
    #[inline]
    pub fn unwrap_sector_mut(&mut self) -> &mut SectorWriter<E> {
        self.as_sector_mut()
            .expect("io_uring journal path requires Sector variant — Buffered routes to run_sync")
    }

    // ---- shared API: shape and order mirror SectorWriter / BufferedWriter ----

    pub fn append(&mut self, event: &JournalEvent<E>) -> Result<u64, JournalError> {
        match self {
            Self::Sector(w) => w.append(event),
            Self::Buffered(w) => w.append(event),
        }
    }

    pub fn batch_append(&mut self, event: &JournalEvent<E>) -> Result<u64, JournalError> {
        match self {
            Self::Sector(w) => w.batch_append(event),
            Self::Buffered(w) => w.batch_append(event),
        }
    }

    pub fn batch_append_with_ts(
        &mut self,
        event: &JournalEvent<E>,
        timestamp_ns: u64,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<u64, JournalError> {
        match self {
            Self::Sector(w) => w.batch_append_with_ts(event, timestamp_ns, key_hash, request_seq),
            Self::Buffered(w) => w.batch_append_with_ts(event, timestamp_ns, key_hash, request_seq),
        }
    }

    pub fn allocate_sequence(&mut self) -> u64 {
        match self {
            Self::Sector(w) => w.allocate_sequence(),
            Self::Buffered(w) => w.allocate_sequence(),
        }
    }

    pub fn encode_event(
        &mut self,
        seq: u64,
        timestamp_ns: u64,
        event: &JournalEvent<E>,
        key_hash: u64,
        request_seq: u64,
    ) -> Result<(), JournalError> {
        match self {
            Self::Sector(w) => w.encode_event(seq, timestamp_ns, event, key_hash, request_seq),
            Self::Buffered(w) => w.encode_event(seq, timestamp_ns, event, key_hash, request_seq),
        }
    }

    pub fn flush_batch_sync(&mut self) -> Result<(), JournalError> {
        match self {
            Self::Sector(w) => w.flush_batch_sync(),
            Self::Buffered(w) => w.flush_batch_sync(),
        }
    }

    pub fn discard_batch_buf(&mut self) {
        match self {
            Self::Sector(w) => w.discard_batch_buf(),
            Self::Buffered(w) => w.discard_batch_buf(),
        }
    }

    pub fn sync(&mut self) -> Result<(), JournalError> {
        match self {
            Self::Sector(w) => w.sync(),
            Self::Buffered(w) => w.sync(),
        }
    }

    pub fn next_sequence(&self) -> u64 {
        match self {
            Self::Sector(w) => w.next_sequence(),
            Self::Buffered(w) => w.next_sequence(),
        }
    }

    pub fn set_next_sequence(&mut self, seq: u64) {
        match self {
            Self::Sector(w) => w.set_next_sequence(seq),
            Self::Buffered(w) => w.set_next_sequence(seq),
        }
    }

    pub fn write_pos(&self) -> u64 {
        match self {
            Self::Sector(w) => w.write_pos(),
            Self::Buffered(w) => w.write_pos(),
        }
    }

    pub fn valid_end(&self) -> u64 {
        match self {
            Self::Sector(w) => w.valid_end(),
            Self::Buffered(w) => w.valid_end(),
        }
    }

    pub fn path(&self) -> &Path {
        match self {
            Self::Sector(w) => w.path(),
            Self::Buffered(w) => w.path(),
        }
    }

    pub fn chain_hash(&self) -> Option<[u8; 32]> {
        match self {
            Self::Sector(w) => w.chain_hash(),
            Self::Buffered(w) => w.chain_hash(),
        }
    }

    pub fn events_since_checkpoint(&self) -> u64 {
        match self {
            Self::Sector(w) => w.events_since_checkpoint(),
            Self::Buffered(w) => w.events_since_checkpoint(),
        }
    }

    pub fn pending_batch_bytes(&self) -> &[u8] {
        match self {
            Self::Sector(w) => w.pending_batch_bytes(),
            Self::Buffered(w) => w.pending_batch_bytes(),
        }
    }

    pub fn last_user_entry_replication_slice(&self) -> &[u8] {
        match self {
            Self::Sector(w) => w.last_user_entry_replication_slice(),
            Self::Buffered(w) => w.last_user_entry_replication_slice(),
        }
    }

    pub fn rotate_segment(&mut self) -> Result<PathBuf, JournalError> {
        match self {
            Self::Sector(w) => w.rotate_segment(),
            Self::Buffered(w) => w.rotate_segment(),
        }
    }

    pub fn read_genesis_entry(&self) -> Result<Vec<u8>, JournalError> {
        match self {
            Self::Sector(w) => w.read_genesis_entry(),
            Self::Buffered(w) => w.read_genesis_entry(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::JournalReader;
    use melin_app::CodecError;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestEvent(u64);

    impl AppEvent for TestEvent {
        fn encoded_size(&self) -> usize {
            8
        }
        fn encode(&self, buf: &mut [u8]) -> usize {
            buf[..8].copy_from_slice(&self.0.to_le_bytes());
            8
        }
        fn decode(buf: &[u8]) -> Result<Self, CodecError> {
            if buf.len() < 8 {
                return Err(CodecError::Truncated);
            }
            Ok(TestEvent(u64::from_le_bytes(buf[..8].try_into().unwrap())))
        }
        fn is_query(&self) -> bool {
            false
        }
    }

    #[test]
    fn mode_parses_both_variants_case_insensitive() {
        assert_eq!(JournalWriterMode::parse("sector"), Ok(JournalWriterMode::Sector));
        assert_eq!(JournalWriterMode::parse("Buffered"), Ok(JournalWriterMode::Buffered));
        assert_eq!(JournalWriterMode::parse("BUFFERED"), Ok(JournalWriterMode::Buffered));
        assert!(JournalWriterMode::parse("direct").is_err());
    }

    #[test]
    fn default_mode_is_buffered() {
        assert_eq!(JournalWriterMode::default(), JournalWriterMode::Buffered);
    }

    #[test]
    fn create_buffered_appends_and_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer =
            JournalWriter::<TestEvent>::create(JournalWriterMode::Buffered, &path).unwrap();
        assert_eq!(writer.mode(), JournalWriterMode::Buffered);
        assert!(writer.as_sector_mut().is_none());

        writer.append(&JournalEvent::App(TestEvent(7))).unwrap();
        writer.append(&JournalEvent::App(TestEvent(8))).unwrap();
        drop(writer);

        let mut reader = JournalReader::<TestEvent>::open(&path).unwrap();
        let mut payloads = Vec::new();
        while let Some(entry) = reader.next_entry().unwrap() {
            if let JournalEvent::App(e) = entry.event {
                payloads.push(e.0);
            }
        }
        assert_eq!(payloads, vec![7, 8]);
    }

    #[test]
    fn create_sector_exposes_underlying_writer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        let mut writer =
            JournalWriter::<TestEvent>::create(JournalWriterMode::Sector, &path).unwrap();
        assert_eq!(writer.mode(), JournalWriterMode::Sector);
        assert!(
            writer.as_sector_mut().is_some(),
            "Sector variant must expose &mut SectorWriter for run_uring"
        );
    }
}
