//! File-backed openraft storage for the control plane.
//!
//! Layout under `--raft-dir`:
//!
//! ```text
//! vote       current Vote — rewritten atomically (tmp + fsync + rename)
//! log        header + append-only entry records; rewritten wholesale on
//!            truncate/purge
//! sm         state machine (last applied, membership) — rewritten atomically
//! snapshot   current snapshot blob — rewritten atomically
//! ```
//!
//! Every file starts with a 4-byte magic + 1-byte format version. The
//! version byte also covers the `single-term-leader` `Vote` layout: flipping
//! that cargo feature changes the serialized shape, so a mismatched dir is
//! refused instead of misread.
//!
//! Durability contract (openraft requires): `save_vote` returns only after
//! the vote is on disk; the `append` callback fires only after `sync_data`.
//! I/O here is blocking inside async fns — deliberate: this runs on the
//! dedicated control-plane current-thread runtime, appends happen only at
//! leader establishment and membership changes (never per-heartbeat), and a
//! ~ms fsync is harmless against a 200 ms heartbeat / 1 s election floor.

use std::collections::BTreeMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::Cursor;
use std::io::Write;
use std::ops::RangeBounds;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use openraft::LogState;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::StorageIOError;
use openraft::StoredMembership;
use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftStateMachine;
use openraft::storage::Snapshot;
use openraft::storage::SnapshotMeta;
use serde::Deserialize;
use serde::Serialize;

use crate::types::Entry;
use crate::types::LogId;
use crate::types::Node;
use crate::types::NodeId;
use crate::types::StorageError;
use crate::types::TypeConfig;
use crate::types::Vote;

/// File magic: "MRft" — Melin Raft.
const MAGIC: [u8; 4] = *b"MRft";
/// Format version. Bump on any layout change, including a change to the
/// openraft `Vote` shape (see module docs re `single-term-leader`).
const VERSION: u8 = 1;
const HEADER_LEN: usize = MAGIC.len() + 1;

const VOTE_FILE: &str = "vote";
const LOG_FILE: &str = "log";
const SM_FILE: &str = "sm";
const SNAPSHOT_FILE: &str = "snapshot";

// ---------------------------------------------------------------------------
// Record framing
// ---------------------------------------------------------------------------

/// One length-prefixed, CRC-protected record: `[len: u32 LE][crc32c: u32 LE]
/// [payload]`. CRC over the payload only — a torn length prefix is caught by
/// the buffer-bounds checks in [`read_record`].
fn encode_record(payload: &[u8], out: &mut Vec<u8>) {
    // u32 length: control-plane records are tiny (a vote, one log entry);
    // MAX_RECORD_LEN bounds decode far below u32::MAX anyway.
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Upper bound on a single decoded record. The largest real record is a log
/// entry holding a membership config (a few hundred bytes); 1 MiB refuses
/// pathological lengths from a corrupt prefix without ever rejecting a
/// legitimate record.
const MAX_RECORD_LEN: usize = 1 << 20;

enum RecordRead<'a> {
    /// A complete, CRC-valid record and the offset just past it.
    Ok { payload: &'a [u8], next: usize },
    /// End of buffer, or an incomplete/corrupt tail starting at this offset.
    Tail,
}

/// Read the record at `off`. Any shortfall or CRC mismatch is reported as
/// `Tail`: with a single append-only writer, damage can only be a torn tail
/// from a crash mid-write, and the caller truncates there. (True mid-file
/// corruption also lands here — it truncates the log at the damage, which
/// raft repairs by re-replicating from the leader.)
fn read_record(buf: &[u8], off: usize) -> RecordRead<'_> {
    let Some(head) = buf.get(off..off + 8) else {
        return RecordRead::Tail;
    };
    // Indexing is in-bounds: `head` is exactly 8 bytes.
    let len = u32::from_le_bytes([head[0], head[1], head[2], head[3]]) as usize;
    let crc = u32::from_le_bytes([head[4], head[5], head[6], head[7]]);
    if len > MAX_RECORD_LEN {
        return RecordRead::Tail;
    }
    let Some(payload) = buf.get(off + 8..off + 8 + len) else {
        return RecordRead::Tail;
    };
    if crc32c::crc32c(payload) != crc {
        return RecordRead::Tail;
    }
    RecordRead::Ok {
        payload,
        next: off + 8 + len,
    }
}

// ---------------------------------------------------------------------------
// Small file helpers
// ---------------------------------------------------------------------------

fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn check_header(buf: &[u8], what: &str) -> Result<(), io::Error> {
    let Some(header) = buf.get(..HEADER_LEN) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{what}: file shorter than header"),
        ));
    };
    if header[..4] != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{what}: bad magic (not a melin-raft file)"),
        ));
    }
    if header[4] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{what}: format version {} (this binary reads {VERSION}); \
                 refusing — see docs/internal/raft-control-plane.md",
                header[4]
            ),
        ));
    }
    Ok(())
}

/// Atomically replace `dir/name` with `header + one record(payload)`:
/// write to a tmp file, fsync it, rename over the target, fsync the dir.
/// A crash at any point leaves either the old file or the new one, never a
/// torn mix.
fn write_file_atomic(dir: &Path, name: &str, payload: &[u8]) -> io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut buf = Vec::with_capacity(HEADER_LEN + 8 + payload.len());
    buf.extend_from_slice(&MAGIC);
    buf.push(VERSION);
    encode_record(payload, &mut buf);
    let mut f = File::create(&tmp)?;
    f.write_all(&buf)?;
    f.sync_data()?;
    fs::rename(&tmp, dir.join(name))?;
    fsync_dir(dir)
}

/// Read a single-record file written by [`write_file_atomic`].
/// `Ok(None)` if the file doesn't exist. Corruption is an error, not `None`:
/// atomic replacement means a torn file is impossible, so damage is real
/// (bit rot, truncation by an outside actor) and must stop the node rather
/// than silently reset state — forgetting a persisted vote can double-vote.
fn read_single_record_file(dir: &Path, name: &str, what: &str) -> io::Result<Option<Vec<u8>>> {
    let buf = match fs::read(dir.join(name)) {
        Ok(buf) => buf,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    check_header(&buf, what)?;
    match read_record(&buf, HEADER_LEN) {
        RecordRead::Ok { payload, next } if next == buf.len() => Ok(Some(payload.to_vec())),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{what}: corrupt record"),
        )),
    }
}

fn decode<T: for<'de> Deserialize<'de>>(payload: &[u8], what: &str) -> io::Result<T> {
    postcard::from_bytes(payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{what}: {e}")))
}

fn encode<T: Serialize>(value: &T, what: &str) -> io::Result<Vec<u8>> {
    postcard::to_stdvec(value)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{what}: {e}")))
}

// ---------------------------------------------------------------------------
// Log store
// ---------------------------------------------------------------------------

struct LogInner {
    dir: PathBuf,
    /// Append handle for the live log file. Replaced whenever
    /// truncate/purge rewrites the file (the old fd would point at the
    /// unlinked inode).
    log_file: File,
    /// Full in-memory copy of the un-purged log, keyed by index.
    /// BTreeMap over Vec/HashMap: `try_get_log_entries` takes arbitrary
    /// index ranges and purge/truncate split at a boundary — ordered range
    /// operations are the access pattern, and the control-plane log is tiny
    /// (entries appear only on leader change and membership change, and are
    /// purged behind snapshots), so node count never matters.
    entries: BTreeMap<u64, Entry>,
    last_purged: Option<LogId>,
    vote: Option<Vote>,
}

/// File-backed [`RaftLogStorage`].
///
/// Cloning shares state: openraft hands out independent log readers, so the
/// store is `Arc<Mutex<_>>` inside. A std `Mutex` (not tokio): every
/// critical section is short and synchronous (no `.await` while held), the
/// runtime is single-threaded, and contention is between the raft core and
/// the occasional replication reader — nanoseconds at control-plane rates.
#[derive(Clone)]
pub struct FileLogStore {
    inner: Arc<Mutex<LogInner>>,
}

impl FileLogStore {
    fn lock(&self) -> MutexGuard<'_, LogInner> {
        // Poisoning is unreachable: the workspace builds with
        // `panic = "abort"`, so no thread can unwind while holding the lock.
        self.inner.lock().expect("raft log store mutex poisoned")
    }
}

impl LogInner {
    /// Serialize header + purge marker + all retained entries.
    fn encode_full(&self) -> Result<Vec<u8>, io::Error> {
        let mut buf = Vec::with_capacity(4096);
        buf.extend_from_slice(&MAGIC);
        buf.push(VERSION);
        let marker = encode(&self.last_purged, "log purge marker")?;
        encode_record(&marker, &mut buf);
        for entry in self.entries.values() {
            let payload = encode(entry, "log entry")?;
            encode_record(&payload, &mut buf);
        }
        Ok(buf)
    }

    /// Rewrite the log file wholesale (tmp + fsync + rename + dir fsync) and
    /// swap in a fresh append handle. Wholesale over segment surgery: the
    /// retained log is dozens of entries, so a full rewrite is a few KiB —
    /// simpler and safer than managing partial truncation of an append file.
    fn rewrite(&mut self) -> Result<(), io::Error> {
        let tmp = self.dir.join(format!("{LOG_FILE}.tmp"));
        let buf = self.encode_full()?;
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_data()?;
        fs::rename(&tmp, self.dir.join(LOG_FILE))?;
        fsync_dir(&self.dir)?;
        self.log_file = OpenOptions::new()
            .append(true)
            .open(self.dir.join(LOG_FILE))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// Everything the control-plane state machine knows. Persisted wholesale on
/// every apply (rare — see module docs), which is why `save_committed` can
/// keep its default no-op: openraft only needs the committed pointer when
/// the state machine is volatile.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SmData {
    last_applied: Option<LogId>,
    membership: StoredMembership<NodeId, Node>,
    /// Monotonic counter making snapshot ids unique without a wall clock.
    snapshot_idx: u64,
}

#[derive(Serialize, Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, Node>,
    data: Vec<u8>,
}

struct SmInner {
    dir: PathBuf,
    data: SmData,
}

/// File-backed [`RaftStateMachine`]. Same sharing/locking rationale as
/// [`FileLogStore`].
#[derive(Clone)]
pub struct FileStateMachine {
    inner: Arc<Mutex<SmInner>>,
}

impl FileStateMachine {
    fn lock(&self) -> MutexGuard<'_, SmInner> {
        // Poisoning unreachable under `panic = "abort"` (no unwinding).
        self.inner
            .lock()
            .expect("raft state machine mutex poisoned")
    }
}

impl SmInner {
    fn persist(&self) -> Result<(), io::Error> {
        let payload = encode(&self.data, "state machine")?;
        write_file_atomic(&self.dir, SM_FILE, &payload)
    }
}

// ---------------------------------------------------------------------------
// Opening
// ---------------------------------------------------------------------------

/// Open (or create) the storage directory and recover both stores.
///
/// Recovery rules:
/// - `vote`/`sm`/`snapshot` are atomically-replaced files — corruption there
///   is real damage and refuses to open (see `read_single_record_file`).
/// - `log` is append-only, so a torn tail is the expected crash artifact:
///   scanning stops at the first bad record and the file is truncated there.
///   Correct because the append callback (and thus any ack built on it)
///   never fired for a record that didn't finish its fsync.
// StorageError is openraft's error type and its size is theirs to choose;
// this is a cold once-per-boot path, so no value in boxing it.
#[allow(clippy::result_large_err)]
pub fn open(dir: &Path) -> Result<(FileLogStore, FileStateMachine), StorageError> {
    open_impl(dir).map_err(|e| StorageIOError::read(&e).into())
}

fn open_impl(dir: &Path) -> io::Result<(FileLogStore, FileStateMachine)> {
    fs::create_dir_all(dir)?;
    // Make the directory entry itself durable before trusting anything in it.
    if let Some(parent) = dir.parent() {
        fsync_dir(parent)?;
    }

    // Vote.
    let vote = match read_single_record_file(dir, VOTE_FILE, "raft vote file")? {
        Some(payload) => Some(decode::<Vote>(&payload, "raft vote file")?),
        None => None,
    };

    // Log.
    let log_path = dir.join(LOG_FILE);
    let (entries, last_purged) = match fs::read(&log_path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // Fresh dir: create an empty log (header + purge marker).
            let inner_tmp = LogInner {
                dir: dir.to_path_buf(),
                // Placeholder handle; `rewrite` swaps in the real one.
                log_file: File::create(dir.join(format!("{LOG_FILE}.tmp")))?,
                entries: BTreeMap::new(),
                last_purged: None,
                vote: None,
            };
            let mut inner_tmp = inner_tmp;
            inner_tmp.rewrite()?;
            (BTreeMap::new(), None)
        }
        Err(e) => return Err(e),
        Ok(buf) => {
            check_header(&buf, "raft log file")?;
            let (marker, mut off) = match read_record(&buf, HEADER_LEN) {
                RecordRead::Ok { payload, next } => (
                    decode::<Option<LogId>>(payload, "raft log purge marker")?,
                    next,
                ),
                RecordRead::Tail => {
                    // The header + purge-marker region is only ever written
                    // by an atomic rewrite (tmp + rename) — a crash cannot
                    // tear it. Damage here is external corruption; recovering
                    // it as "no purge marker" would silently revert the log,
                    // which openraft's default config assumes never happens.
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "raft log file: corrupt purge marker",
                    ));
                }
            };
            let mut entries = BTreeMap::new();
            while let RecordRead::Ok { payload, next } = read_record(&buf, off) {
                let entry = decode::<Entry>(payload, "raft log entry")?;
                entries.insert(entry.log_id.index, entry);
                off = next;
            }
            if off < buf.len() {
                // Torn tail from a crash mid-append: drop it (never acked).
                let f = OpenOptions::new().write(true).open(&log_path)?;
                f.set_len(off as u64)?;
                f.sync_data()?;
            }
            (entries, marker)
        }
    };

    let log_store = FileLogStore {
        inner: Arc::new(Mutex::new(LogInner {
            dir: dir.to_path_buf(),
            log_file: OpenOptions::new().append(true).open(&log_path)?,
            entries,
            last_purged,
            vote,
        })),
    };

    // State machine.
    let data = match read_single_record_file(dir, SM_FILE, "raft state machine file")? {
        Some(payload) => decode::<SmData>(&payload, "raft state machine file")?,
        None => SmData::default(),
    };
    let sm = FileStateMachine {
        inner: Arc::new(Mutex::new(SmInner {
            dir: dir.to_path_buf(),
            data,
        })),
    };

    Ok((log_store, sm))
}

// ---------------------------------------------------------------------------
// Trait impls
// ---------------------------------------------------------------------------

impl RaftLogReader<TypeConfig> for FileLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        Ok(self
            .lock()
            .entries
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for FileLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let inner = self.lock();
        let last_log_id = inner
            .entries
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), StorageError> {
        let mut inner = self.lock();
        let payload = encode(vote, "raft vote").map_err(|e| StorageIOError::write_vote(&e))?;
        write_file_atomic(&inner.dir, VOTE_FILE, &payload)
            .map_err(|e| StorageIOError::write_vote(&e))?;
        inner.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        Ok(self.lock().vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.lock();
        let result = (|| -> io::Result<()> {
            let mut buf = Vec::with_capacity(256);
            for entry in entries {
                let payload = encode(&entry, "raft log entry")?;
                encode_record(&payload, &mut buf);
                inner.entries.insert(entry.log_id.index, entry);
            }
            inner.log_file.write_all(&buf)?;
            inner.log_file.sync_data()
        })();
        match result {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(e) => {
                let storage_err = StorageIOError::write_logs(&e);
                // Report through both channels: the callback is what raft
                // core acts on; the return value is for the caller's error
                // path. The entries may be half-applied in memory — raft
                // treats a log I/O error as fatal, so no repair is attempted.
                callback.log_io_completed(Err(e));
                Err(storage_err.into())
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut inner = self.lock();
        inner.entries.split_off(&log_id.index);
        inner
            .rewrite()
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        let mut inner = self.lock();
        if inner.last_purged.is_none_or(|p| p < log_id) {
            inner.last_purged = Some(log_id);
        }
        // Retain strictly-greater indexes; split_off keeps >= index+1.
        let retained = inner.entries.split_off(&(log_id.index + 1));
        inner.entries = retained;
        inner
            .rewrite()
            .map_err(|e| StorageIOError::write_logs(&e))?;
        Ok(())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for FileStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError> {
        let mut inner = self.lock();
        inner.data.snapshot_idx += 1;
        let meta = SnapshotMeta {
            last_log_id: inner.data.last_applied,
            last_membership: inner.data.membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                inner.data.last_applied.map_or(0, |l| l.index),
                inner.data.snapshot_idx
            ),
        };
        let data = encode(&inner.data, "raft snapshot")
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let payload = encode(&stored, "raft snapshot")
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        write_file_atomic(&inner.dir, SNAPSHOT_FILE, &payload)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        inner
            .persist()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for FileStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership<NodeId, Node>), StorageError> {
        let inner = self.lock();
        Ok((inner.data.last_applied, inner.data.membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<crate::types::ControlResponse>, StorageError>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.lock();
        let mut replies = Vec::new();
        for entry in entries {
            inner.data.last_applied = Some(entry.log_id);
            if let openraft::EntryPayload::Membership(m) = entry.payload {
                inner.data.membership = StoredMembership::new(Some(entry.log_id), m);
            }
            replies.push(crate::types::ControlResponse);
        }
        inner
            .persist()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Box<Cursor<Vec<u8>>>, StorageError> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, Node>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError> {
        let mut inner = self.lock();
        let data = snapshot.into_inner();
        let mut sm: SmData = decode(&data, "raft snapshot")
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        // The meta is authoritative for the pointers — the blob carries the
        // sender's counters, the meta carries where this snapshot sits.
        sm.last_applied = meta.last_log_id;
        sm.membership = meta.last_membership.clone();
        // Keep our own snapshot counter monotonic across installs.
        sm.snapshot_idx = sm.snapshot_idx.max(inner.data.snapshot_idx);
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        let payload = encode(&stored, "raft snapshot")
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        write_file_atomic(&inner.dir, SNAPSHOT_FILE, &payload)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        inner.data = sm;
        inner
            .persist()
            .map_err(|e| StorageIOError::write_state_machine(&e))?;
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<TypeConfig>>, StorageError> {
        let inner = self.lock();
        let payload = match read_single_record_file(&inner.dir, SNAPSHOT_FILE, "raft snapshot file")
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?
        {
            Some(payload) => payload,
            None => return Ok(None),
        };
        let stored: StoredSnapshot = decode(&payload, "raft snapshot file")
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;
        Ok(Some(Snapshot {
            meta: stored.meta,
            snapshot: Box::new(Cursor::new(stored.data)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_round_trip() {
        let mut buf = Vec::new();
        encode_record(b"hello", &mut buf);
        encode_record(b"", &mut buf);
        match read_record(&buf, 0) {
            RecordRead::Ok { payload, next } => {
                assert_eq!(payload, b"hello");
                match read_record(&buf, next) {
                    RecordRead::Ok { payload, next } => {
                        assert_eq!(payload, b"");
                        assert_eq!(next, buf.len());
                    }
                    RecordRead::Tail => panic!("second record should decode"),
                }
            }
            RecordRead::Tail => panic!("first record should decode"),
        }
    }

    #[test]
    fn record_rejects_corruption() {
        let mut buf = Vec::new();
        encode_record(b"payload", &mut buf);
        // Flip one payload byte: CRC must catch it.
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        assert!(matches!(read_record(&buf, 0), RecordRead::Tail));
        // Truncated header and truncated payload both read as Tail.
        assert!(matches!(read_record(&buf[..4], 0), RecordRead::Tail));
        assert!(matches!(read_record(&buf[..10], 0), RecordRead::Tail));
    }

    #[test]
    fn record_rejects_pathological_length() {
        let mut buf = vec![0xffu8; 16];
        buf[..4].copy_from_slice(&(u32::MAX).to_le_bytes());
        assert!(matches!(read_record(&buf, 0), RecordRead::Tail));
    }

    proptest::proptest! {
        /// Any payload round-trips through the record framing.
        #[test]
        fn record_round_trips_arbitrary_payloads(payload in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..2048)) {
            let mut buf = Vec::new();
            encode_record(&payload, &mut buf);
            match read_record(&buf, 0) {
                RecordRead::Ok { payload: got, next } => {
                    proptest::prop_assert_eq!(got, &payload[..]);
                    proptest::prop_assert_eq!(next, buf.len());
                }
                RecordRead::Tail => return Err(proptest::test_runner::TestCaseError::fail("valid record read as Tail")),
            }
        }

        /// Reading arbitrary garbage at arbitrary offsets never panics and
        /// never reads out of bounds — it either yields an in-bounds record
        /// or reports a tail.
        #[test]
        fn record_read_never_panics_on_garbage(buf in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..256), off in 0usize..300) {
            match read_record(&buf, off) {
                RecordRead::Ok { next, .. } => proptest::prop_assert!(next <= buf.len()),
                RecordRead::Tail => {}
            }
        }
    }
}
