//! Durable Raft storage — single-file, atomic-rewrite persistence.
//!
//! Persists the full control-plane Raft state (`HardState`, `ConfState`,
//! truncation marker, log entries) in **one file**, rewritten atomically
//! on every mutation via the same `tmp → fsync → rename → parent-dir
//! fsync` discipline as the application snapshot writer
//! (`melin-transport-core::snapshot`).
//!
//! Why whole-file rewrite instead of an append-only log: the
//! control-plane log is tiny (election no-op entries, occasional
//! membership/config changes) and mutates at human/heartbeat cadence,
//! never on the order hot path. A single atomically-replaced file is
//! either the complete previous state or the complete next state —
//! there is no torn-tail repair, no partially-applied append, and no
//! cross-file ordering to reason about. Durability cost is two fsyncs
//! per mutation, irrelevant against a multi-hundred-millisecond
//! election timeout. If the step-2 config payloads ever make rewrites
//! measurably slow, the format can evolve into an append log behind the
//! same API — but correctness simplicity wins by default here.
//!
//! **Vote durability is safety-critical**: raft requires `HardState`
//! (term, voted-for) to be on disk before the vote response leaves the
//! node — otherwise a crash-restart can double-vote in the same term and
//! elect two leaders. Every write method here fsyncs before returning;
//! the driver must call them before handing persisted messages to the
//! transport. For the same reason a *corrupt* state file is a hard open
//! error, never a silent reset: resetting forgets the vote.
//!
//! Write methods return `io::Result` rather than panicking on contract
//! violations (unlike raft-rs's `MemStorage`): the server builds with
//! `panic = "abort"`, and a control-plane bug must degrade to "raft
//! inoperable, exchange keeps trading" — the caller logs and stops the
//! raft driver — not abort the matching engine.

use std::fs::{File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::path::{Path, PathBuf};

use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::util::limit_size;
use raft::{Error, GetEntriesContext, RaftState, Storage, StorageError};

/// `"MRFT"` little-endian.
const MAGIC: u32 = 0x5446_524D;
const FORMAT_VERSION: u32 = 1;
const STATE_FILE: &str = "raft-state";
const TMP_SUFFIX: &str = ".tmp";

/// Cap on the serialized state file. Guards both directions: `persist`
/// refuses to write a file `open` would reject, and `open` refuses to
/// slurp an absurd file into memory. Far above any realistic
/// control-plane state (a handful of config entries), far below
/// anything that could hurt.
const MAX_STATE_FILE: u64 = 64 << 20;

/// File-backed [`raft::Storage`].
///
/// The in-memory mirror is authoritative for reads; every mutation
/// updates the mirror and atomically rewrites the file before
/// returning. Owned by the `RawNode` (mutations go through
/// `RawNode::mut_store`), so no interior locking is needed — the whole
/// control plane is single-threaded.
#[derive(Debug)]
pub struct FileStorage {
    /// Path of the state file (`<dir>/raft-state`).
    path: PathBuf,
    /// `HardState` + `ConfState` mirror of the file.
    raft_state: RaftState,
    /// Log entries after `truncated_index`; invariant:
    /// `entries[i].index == truncated_index + 1 + i`.
    ///
    /// `Vec` (not `VecDeque`): the log stays tiny and compaction
    /// rewrites the file wholesale anyway, so contiguous storage +
    /// slice access beats ring-buffer bookkeeping.
    entries: Vec<Entry>,
    /// Index/term of the last compacted entry (the position *before*
    /// `entries[0]`), kept so `term(truncated_index)` still answers for
    /// log-matching — the role `snapshot_metadata` plays in
    /// `MemStorage`, made explicit.
    truncated_index: u64,
    truncated_term: u64,
}

impl FileStorage {
    /// Open (or create fresh) storage under `dir`. A missing state file
    /// is a normal first boot; a present-but-invalid one is a hard
    /// error — see the module docs on vote durability.
    pub fn open(dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(STATE_FILE);
        match std::fs::metadata(&path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self {
                path,
                raft_state: RaftState::default(),
                entries: Vec::new(),
                truncated_index: 0,
                truncated_term: 0,
            }),
            Err(e) => Err(e),
            Ok(meta) => {
                if meta.len() > MAX_STATE_FILE {
                    return Err(io::Error::other(format!(
                        "raft state file {} is {} bytes (cap {MAX_STATE_FILE}) — refusing to load",
                        path.display(),
                        meta.len()
                    )));
                }
                let mut buf = Vec::with_capacity(meta.len() as usize);
                File::open(&path)?.read_to_end(&mut buf)?;
                let mut storage = Self::decode(&buf).map_err(|e| {
                    io::Error::other(format!(
                        "raft state file {} is corrupt ({e}); refusing to start raft — \
                         restore the file from the node's backup or re-provision the node \
                         (deleting the file forgets this node's vote and can elect two \
                         leaders in one term)",
                        path.display()
                    ))
                })?;
                storage.path = path;
                Ok(storage)
            }
        }
    }

    /// Whether the storage carries any state (membership, votes, or
    /// entries). Mirrors `RaftState::initialized` plus the log.
    pub fn initialized(&self) -> bool {
        self.raft_state.initialized()
            || self.raft_state.hard_state != HardState::default()
            || !self.entries.is_empty()
            || self.truncated_index != 0
    }

    /// Bootstrap an empty storage with the cluster's initial voter set.
    /// Every node of a new cluster must be bootstrapped with the same
    /// voters (same rule as `MemStorage::new_with_conf_state`).
    pub fn initialize_with_conf_state(&mut self, voters: Vec<u64>) -> io::Result<()> {
        if self.initialized() {
            return Err(io::Error::other(
                "raft storage already initialized — refusing to overwrite membership",
            ));
        }
        self.raft_state.conf_state = ConfState {
            voters,
            ..Default::default()
        };
        self.persist()
    }

    /// Current hard state (term, vote, commit).
    pub fn hard_state(&self) -> &HardState {
        &self.raft_state.hard_state
    }

    /// Durably record a new `HardState`. Must complete before any
    /// message referencing it (vote responses above all) is sent.
    pub fn set_hard_state(&mut self, hs: &HardState) -> io::Result<()> {
        self.raft_state.hard_state = hs.clone();
        self.persist()
    }

    /// Durably advance the commit index (the `LightReady` path). Must
    /// complete *before* the committed entries are applied — on restart
    /// raft panics if applied state runs ahead of the persisted commit.
    pub fn set_commit(&mut self, commit: u64) -> io::Result<()> {
        if commit < self.truncated_index || commit > self.last_index_inner() {
            return Err(io::Error::other(format!(
                "commit {commit} outside stored log [{}, {}] — raft contract violation",
                self.truncated_index,
                self.last_index_inner()
            )));
        }
        self.raft_state.hard_state.commit = commit;
        self.persist()
    }

    /// Durably record a new `ConfState` (after applying a conf-change
    /// entry).
    pub fn set_conf_state(&mut self, cs: ConfState) -> io::Result<()> {
        self.raft_state.conf_state = cs;
        self.persist()
    }

    /// Durably append `ents`, truncating any conflicting suffix first
    /// (a new leader may overwrite uncommitted entries — standard raft).
    pub fn append(&mut self, ents: &[Entry]) -> io::Result<()> {
        let Some(first_new) = ents.first() else {
            return Ok(());
        };
        if first_new.index <= self.truncated_index {
            return Err(io::Error::other(format!(
                "append at {} overwrites compacted log (truncated at {}) — raft contract violation",
                first_new.index, self.truncated_index
            )));
        }
        if first_new.index > self.last_index_inner() + 1 {
            return Err(io::Error::other(format!(
                "append at {} leaves a gap after {} — raft contract violation",
                first_new.index,
                self.last_index_inner()
            )));
        }
        // Drop the conflicting suffix, keep the prefix, extend.
        let keep = (first_new.index - self.truncated_index - 1) as usize;
        self.entries.truncate(keep);
        self.entries.extend_from_slice(ents);
        self.persist()
    }

    /// Discard entries up to (excluding) `compact_index`, recording the
    /// truncation marker so log-matching still answers at the boundary.
    /// Callers must only compact applied indexes.
    pub fn compact(&mut self, compact_index: u64) -> io::Result<()> {
        if compact_index <= self.first_index_inner() {
            return Ok(()); // nothing to discard — not an error
        }
        if compact_index > self.last_index_inner() + 1 {
            return Err(io::Error::other(format!(
                "compact {compact_index} beyond last index {} — raft contract violation",
                self.last_index_inner()
            )));
        }
        let new_truncated = compact_index - 1;
        let drop = (compact_index - self.first_index_inner()) as usize;
        // `drop >= 1` here, so `entries[drop - 1]` is the entry at
        // `new_truncated`.
        self.truncated_term = self.entries[drop - 1].term;
        self.truncated_index = new_truncated;
        self.entries.drain(..drop);
        self.persist()
    }

    /// Replace the log with the state described by `snapshot`'s
    /// metadata (received from a leader further ahead than our log).
    pub fn apply_snapshot(&mut self, mut snapshot: Snapshot) -> io::Result<()> {
        let meta = snapshot.metadata.take().ok_or_else(|| {
            io::Error::other("snapshot without metadata — raft contract violation")
        })?;
        if meta.index < self.first_index_inner() {
            return Err(io::Error::other(format!(
                "snapshot at {} is older than our first index {} — out of date",
                meta.index,
                self.first_index_inner()
            )));
        }
        self.truncated_index = meta.index;
        self.truncated_term = meta.term;
        self.entries.clear();
        self.raft_state.hard_state.term = self.raft_state.hard_state.term.max(meta.term);
        self.raft_state.hard_state.commit = meta.index;
        self.raft_state.conf_state = meta.conf_state.unwrap_or_default();
        self.persist()
    }

    fn first_index_inner(&self) -> u64 {
        self.truncated_index + 1
    }

    fn last_index_inner(&self) -> u64 {
        self.truncated_index + self.entries.len() as u64
    }

    // ---- serialization ----
    //
    // Hand-rolled little-endian layout (the codebase convention — see
    // the replication control codec) rather than prost: the file format
    // must stay stable under raft-rs/prost upgrades, and the subset we
    // persist is five scalar fields plus byte blobs.
    //
    //   magic u32 | version u32
    //   term u64 | vote u64 | commit u64
    //   truncated_index u64 | truncated_term u64
    //   conf: voters, learners, voters_outgoing, learners_next
    //         (each u16 count + u64 ids) | auto_leave u8
    //   entry_count u32
    //   per entry: entry_type i32 | term u64 | index u64
    //              | data u32-len + bytes | context u32-len + bytes
    //   crc32c u32 over everything above
    //
    // `Entry::sync_log` (deprecated upstream) is deliberately not
    // persisted.

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        let hs = &self.raft_state.hard_state;
        buf.extend_from_slice(&hs.term.to_le_bytes());
        buf.extend_from_slice(&hs.vote.to_le_bytes());
        buf.extend_from_slice(&hs.commit.to_le_bytes());
        buf.extend_from_slice(&self.truncated_index.to_le_bytes());
        buf.extend_from_slice(&self.truncated_term.to_le_bytes());
        let cs = &self.raft_state.conf_state;
        for list in [
            &cs.voters,
            &cs.learners,
            &cs.voters_outgoing,
            &cs.learners_next,
        ] {
            encode_id_list(&mut buf, list);
        }
        buf.push(cs.auto_leave as u8);
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            buf.extend_from_slice(&e.entry_type.to_le_bytes());
            buf.extend_from_slice(&e.term.to_le_bytes());
            buf.extend_from_slice(&e.index.to_le_bytes());
            encode_blob(&mut buf, &e.data);
            encode_blob(&mut buf, &e.context);
        }
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode and validate a state file. Returns a storage with an
    /// empty `path` (the caller fills it in).
    fn decode(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < 4 {
            return Err(io::Error::other("file shorter than the CRC trailer"));
        }
        let (body, crc_bytes) = buf.split_at(buf.len() - 4);
        let stored_crc = u32::from_le_bytes(
            crc_bytes
                .try_into()
                .map_err(|_| io::Error::other("bad CRC trailer"))?,
        );
        let actual_crc = crc32c::crc32c(body);
        if stored_crc != actual_crc {
            return Err(io::Error::other(format!(
                "CRC mismatch (stored {stored_crc:#010x}, computed {actual_crc:#010x})"
            )));
        }

        let mut r = Reader { buf: body, pos: 0 };
        let magic = r.u32()?;
        if magic != MAGIC {
            return Err(io::Error::other(format!("bad magic {magic:#010x}")));
        }
        let version = r.u32()?;
        if version != FORMAT_VERSION {
            return Err(io::Error::other(format!(
                "unsupported format version {version} (expected {FORMAT_VERSION})"
            )));
        }
        // Struct-literal field order doubles as the read order from the
        // file — it must match `encode` exactly.
        let hs = HardState {
            term: r.u64()?,
            vote: r.u64()?,
            commit: r.u64()?,
        };
        let truncated_index = r.u64()?;
        let truncated_term = r.u64()?;
        let cs = ConfState {
            voters: r.id_list()?,
            learners: r.id_list()?,
            voters_outgoing: r.id_list()?,
            learners_next: r.id_list()?,
            auto_leave: r.u8()? != 0,
        };
        let entry_count = r.u32()? as usize;
        let mut entries = Vec::with_capacity(entry_count);
        for i in 0..entry_count {
            let e = Entry {
                entry_type: r.i32()?,
                term: r.u64()?,
                index: r.u64()?,
                data: r.blob()?,
                context: r.blob()?,
                ..Default::default()
            };
            let expected = truncated_index + 1 + i as u64;
            if e.index != expected {
                return Err(io::Error::other(format!(
                    "entry {i} has index {} (expected {expected}) — log not contiguous",
                    e.index
                )));
            }
            entries.push(e);
        }
        if r.pos != body.len() {
            return Err(io::Error::other(format!(
                "{} trailing bytes after the last entry",
                body.len() - r.pos
            )));
        }
        let last_index = truncated_index + entries.len() as u64;
        if hs.commit < truncated_index || hs.commit > last_index {
            return Err(io::Error::other(format!(
                "commit {} outside stored log [{truncated_index}, {last_index}]",
                hs.commit
            )));
        }

        Ok(Self {
            path: PathBuf::new(),
            raft_state: RaftState {
                hard_state: hs,
                conf_state: cs,
            },
            entries,
            truncated_index,
            truncated_term,
        })
    }

    /// Atomically replace the state file: write `<path>.tmp`, fsync it,
    /// rename over `path`, fsync the parent directory. Same discipline
    /// (and rationale) as `melin-transport-core::snapshot::save`.
    fn persist(&self) -> io::Result<()> {
        let buf = self.encode();
        if buf.len() as u64 > MAX_STATE_FILE {
            return Err(io::Error::other(format!(
                "raft state serializes to {} bytes (cap {MAX_STATE_FILE})",
                buf.len()
            )));
        }

        let mut tmp_path = self.path.clone().into_os_string();
        tmp_path.push(TMP_SUFFIX);
        let tmp_path = PathBuf::from(tmp_path);
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, &self.path)?;
        // Empty parent means CWD — open "." so the fsync lands somewhere.
        let parent = match self.path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

fn encode_id_list(buf: &mut Vec<u8>, ids: &[u64]) {
    // u16 count: membership lists hold node ids — a cluster of tens of
    // thousands of voters is nonsensical, and the cap keeps a corrupt
    // count from allocating gigabytes on load.
    buf.extend_from_slice(&(ids.len() as u16).to_le_bytes());
    for id in ids {
        buf.extend_from_slice(&id.to_le_bytes());
    }
}

fn encode_blob(buf: &mut Vec<u8>, blob: &[u8]) {
    buf.extend_from_slice(&(blob.len() as u32).to_le_bytes());
    buf.extend_from_slice(blob);
}

/// Bounds-checked little-endian reader over the state-file body. Every
/// accessor fails cleanly on truncation instead of panicking — `decode`
/// turns that into the corrupt-file error.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn take(&mut self, n: usize) -> io::Result<&[u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|&end| end <= self.buf.len())
            .ok_or_else(|| io::Error::other("truncated field"))?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("len 2")))
    }

    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("len 4")))
    }

    fn i32(&mut self) -> io::Result<i32> {
        Ok(i32::from_le_bytes(self.take(4)?.try_into().expect("len 4")))
    }

    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("len 8")))
    }

    fn id_list(&mut self) -> io::Result<Vec<u64>> {
        let count = self.u16()? as usize;
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(self.u64()?);
        }
        Ok(ids)
    }

    fn blob(&mut self) -> io::Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }
}

impl Storage for FileStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        Ok(self.raft_state.clone())
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low < self.first_index_inner() {
            return Err(Error::Store(StorageError::Compacted));
        }
        if high > self.last_index_inner() + 1 {
            // Same contract as `MemStorage`: raft never asks past its
            // own last index, so this is unreachable short of a raft
            // bug. Report Unavailable instead of panicking — the server
            // builds with `panic = "abort"`.
            return Err(Error::Store(StorageError::Unavailable));
        }
        let offset = self.first_index_inner();
        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = self.entries[lo..hi].to_vec();
        limit_size(&mut ents, max_size.into());
        Ok(ents)
    }

    fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index {
            return Ok(self.truncated_term);
        }
        if idx < self.truncated_index {
            return Err(Error::Store(StorageError::Compacted));
        }
        if idx > self.last_index_inner() {
            return Err(Error::Store(StorageError::Unavailable));
        }
        Ok(self.entries[(idx - self.first_index_inner()) as usize].term)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index_inner())
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index_inner())
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        // Metadata-only snapshot at the commit index (the control-plane
        // state machine is tiny; step 2 adds the config payload here).
        let mut snap = Snapshot::default();
        let meta = snap.metadata.get_or_insert_with(Default::default);
        let commit = self.raft_state.hard_state.commit;
        meta.index = commit;
        meta.term = match self.term(commit) {
            Ok(t) => t,
            // Commit is validated against the stored log on every
            // mutation, so this is unreachable; surface as Unavailable
            // rather than panicking.
            Err(_) => return Err(Error::Store(StorageError::SnapshotTemporarilyUnavailable)),
        };
        meta.conf_state = Some(self.raft_state.conf_state.clone());
        if meta.index < request_index {
            // Mirror `MemStorage`: never hand back a snapshot older
            // than the follower asked for.
            meta.index = request_index;
        }
        Ok(snap)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(index: u64, term: u64) -> Entry {
        Entry {
            index,
            term,
            ..Default::default()
        }
    }

    fn entry_with_data(index: u64, term: u64, data: &[u8]) -> Entry {
        let mut e = entry(index, term);
        e.data = data.to_vec();
        e.context = vec![0xEE; 3];
        e
    }

    fn ctx() -> GetEntriesContext {
        GetEntriesContext::empty(false)
    }

    #[test]
    fn fresh_dir_starts_uninitialized() {
        let dir = tempfile::tempdir().unwrap();
        let s = FileStorage::open(dir.path()).unwrap();
        assert!(!s.initialized());
        assert_eq!(s.first_index().unwrap(), 1);
        assert_eq!(s.last_index().unwrap(), 0);
        assert_eq!(s.term(0).unwrap(), 0);
        let state = s.initial_state().unwrap();
        assert_eq!(state.hard_state, HardState::default());
        assert_eq!(state.conf_state, ConfState::default());
    }

    #[test]
    fn bootstrap_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.initialize_with_conf_state(vec![1, 2, 3]).unwrap();

        let reopened = FileStorage::open(dir.path()).unwrap();
        assert!(reopened.initialized());
        assert_eq!(
            reopened.initial_state().unwrap().conf_state.voters,
            vec![1, 2, 3]
        );
    }

    #[test]
    fn bootstrap_refuses_initialized_storage() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.initialize_with_conf_state(vec![1, 2, 3]).unwrap();
        assert!(s.initialize_with_conf_state(vec![4]).is_err());
    }

    #[test]
    fn append_and_hard_state_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.initialize_with_conf_state(vec![1, 2, 3]).unwrap();
        s.append(&[
            entry_with_data(1, 1, b"one"),
            entry_with_data(2, 1, b"two"),
            entry_with_data(3, 2, b"three"),
        ])
        .unwrap();
        let hs = HardState {
            term: 2,
            vote: 3,
            commit: 2,
        };
        s.set_hard_state(&hs).unwrap();

        let r = FileStorage::open(dir.path()).unwrap();
        assert_eq!(r.hard_state(), &hs);
        assert_eq!(r.first_index().unwrap(), 1);
        assert_eq!(r.last_index().unwrap(), 3);
        assert_eq!(r.term(3).unwrap(), 2);
        let ents = r.entries(1, 4, None, ctx()).unwrap();
        assert_eq!(ents.len(), 3);
        assert_eq!(ents[0].data, b"one");
        assert_eq!(ents[2].data, b"three");
        assert_eq!(ents[2].context, vec![0xEE; 3]);
    }

    #[test]
    fn conflicting_append_truncates_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry(1, 1), entry(2, 1), entry(3, 1)]).unwrap();
        // New leader at term 2 overwrites from index 2.
        s.append(&[entry(2, 2)]).unwrap();
        assert_eq!(s.last_index().unwrap(), 2);
        assert_eq!(s.term(2).unwrap(), 2);
        assert_eq!(s.term(1).unwrap(), 1);

        let r = FileStorage::open(dir.path()).unwrap();
        assert_eq!(r.last_index().unwrap(), 2);
        assert_eq!(r.term(2).unwrap(), 2);
    }

    #[test]
    fn gapped_append_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry(1, 1)]).unwrap();
        assert!(s.append(&[entry(3, 1)]).is_err());
        // Storage unchanged.
        assert_eq!(s.last_index().unwrap(), 1);
    }

    #[test]
    fn empty_append_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[]).unwrap();
        assert_eq!(s.last_index().unwrap(), 0);
    }

    #[test]
    fn compact_discards_prefix_and_answers_boundary_term() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry(1, 1), entry(2, 1), entry(3, 2), entry(4, 2)])
            .unwrap();
        s.set_commit(4).unwrap();
        s.compact(3).unwrap();

        assert_eq!(s.first_index().unwrap(), 3);
        assert_eq!(s.last_index().unwrap(), 4);
        // Boundary: term of the last compacted entry stays answerable.
        assert_eq!(s.term(2).unwrap(), 1);
        assert!(matches!(
            s.term(1),
            Err(Error::Store(StorageError::Compacted))
        ));
        assert!(matches!(
            s.entries(1, 3, None, ctx()),
            Err(Error::Store(StorageError::Compacted))
        ));

        let r = FileStorage::open(dir.path()).unwrap();
        assert_eq!(r.first_index().unwrap(), 3);
        assert_eq!(r.term(2).unwrap(), 1);
        // Appends below the truncation are rejected after reopen too.
        let mut r = r;
        assert!(r.append(&[entry(2, 3)]).is_err());
    }

    #[test]
    fn compact_below_first_index_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry(1, 1), entry(2, 1)]).unwrap();
        s.set_commit(2).unwrap();
        s.compact(2).unwrap();
        s.compact(1).unwrap(); // already gone — fine
        assert_eq!(s.first_index().unwrap(), 2);
    }

    #[test]
    fn compact_past_last_index_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry(1, 1)]).unwrap();
        assert!(s.compact(3).is_err());
    }

    #[test]
    fn set_commit_validates_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry(1, 1), entry(2, 1)]).unwrap();
        s.set_commit(2).unwrap();
        assert_eq!(s.hard_state().commit, 2);
        assert!(s.set_commit(5).is_err());
    }

    #[test]
    fn apply_snapshot_resets_log_and_membership() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry(1, 1), entry(2, 1)]).unwrap();

        let mut snap = Snapshot::default();
        let meta = snap.metadata.get_or_insert_with(Default::default);
        meta.index = 10;
        meta.term = 3;
        meta.conf_state = Some(ConfState {
            voters: vec![1, 2, 3],
            ..Default::default()
        });
        s.apply_snapshot(snap).unwrap();

        assert_eq!(s.first_index().unwrap(), 11);
        assert_eq!(s.last_index().unwrap(), 10);
        assert_eq!(s.term(10).unwrap(), 3);
        assert_eq!(s.hard_state().commit, 10);
        assert_eq!(s.hard_state().term, 3);

        let r = FileStorage::open(dir.path()).unwrap();
        assert_eq!(r.first_index().unwrap(), 11);
        assert_eq!(r.initial_state().unwrap().conf_state.voters, vec![1, 2, 3]);
    }

    #[test]
    fn stale_snapshot_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry(1, 1), entry(2, 1)]).unwrap();
        s.set_commit(2).unwrap();
        s.compact(3).unwrap(); // first_index now 3

        let mut snap = Snapshot::default();
        let meta = snap.metadata.get_or_insert_with(Default::default);
        meta.index = 1;
        meta.term = 1;
        assert!(s.apply_snapshot(snap).is_err());
    }

    #[test]
    fn snapshot_reports_commit_and_membership() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.initialize_with_conf_state(vec![1, 2]).unwrap();
        s.append(&[entry(1, 1), entry(2, 1)]).unwrap();
        s.set_commit(2).unwrap();

        let snap = s.snapshot(0, 99).unwrap();
        let meta = snap.metadata.as_ref().unwrap();
        assert_eq!(meta.index, 2);
        assert_eq!(meta.term, 1);
        assert_eq!(meta.conf_state.as_ref().unwrap().voters, vec![1, 2]);

        // Never hand back less than the follower asked for.
        let bumped = s.snapshot(5, 99).unwrap();
        assert_eq!(bumped.metadata.as_ref().unwrap().index, 5);
    }

    #[test]
    fn entries_respects_max_size() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[
            entry_with_data(1, 1, &[0u8; 64]),
            entry_with_data(2, 1, &[0u8; 64]),
            entry_with_data(3, 1, &[0u8; 64]),
        ])
        .unwrap();
        // A cap of one entry's size still returns at least one entry.
        let ents = s.entries(1, 4, Some(1), ctx()).unwrap();
        assert_eq!(ents.len(), 1);
        let all = s.entries(1, 4, None, ctx()).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn empty_range_is_empty_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let s = FileStorage::open(dir.path()).unwrap();
        assert!(s.entries(1, 1, None, ctx()).unwrap().is_empty());
    }

    #[test]
    fn corrupt_file_is_a_hard_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.initialize_with_conf_state(vec![1, 2, 3]).unwrap();
        drop(s);

        let path = dir.path().join(STATE_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[10] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let err = FileStorage::open(dir.path()).unwrap_err();
        assert!(err.to_string().contains("corrupt"), "{err}");
    }

    #[test]
    fn truncated_file_is_a_hard_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.append(&[entry_with_data(1, 1, b"payload")]).unwrap();
        drop(s);

        let path = dir.path().join(STATE_FILE);
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() / 2]).unwrap();
        assert!(FileStorage::open(dir.path()).is_err());
    }

    #[test]
    fn leftover_tmp_from_a_crash_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.initialize_with_conf_state(vec![1, 2, 3]).unwrap();
        drop(s);

        // Simulate a crash mid-persist: a garbage tmp file next to a
        // valid state file.
        std::fs::write(
            dir.path().join(format!("{STATE_FILE}{TMP_SUFFIX}")),
            b"garbage",
        )
        .unwrap();
        let r = FileStorage::open(dir.path()).unwrap();
        assert_eq!(r.initial_state().unwrap().conf_state.voters, vec![1, 2, 3]);
    }

    /// End-to-end: the storage drives a real single-voter `RawNode`
    /// through an election, persisting each ready — then a "restart"
    /// (reopen from disk) carries term and vote forward.
    #[test]
    fn drives_a_real_raw_node_and_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = FileStorage::open(dir.path()).unwrap();
        s.initialize_with_conf_state(vec![1]).unwrap();

        let config = raft::Config {
            id: 1,
            ..Default::default()
        };
        config.validate().unwrap();
        let mut node = raft::RawNode::new(&config, s, &crate::tracing_logger()).expect("raw node");
        node.campaign().unwrap();

        // Drive readies until the node settles as leader.
        for _ in 0..10 {
            if !node.has_ready() {
                break;
            }
            let ready = node.ready();
            if !ready.entries().is_empty() {
                let entries = ready.entries().clone();
                node.mut_store().append(&entries).unwrap();
            }
            if let Some(hs) = ready.hs() {
                let hs = hs.clone();
                node.mut_store().set_hard_state(&hs).unwrap();
            }
            let mut light = node.advance(ready);
            if let Some(commit) = light.commit_index() {
                node.mut_store().set_commit(commit).unwrap();
            }
            let _ = light.take_committed_entries();
            node.advance_apply();
        }
        assert_eq!(node.raft.state, raft::StateRole::Leader);
        let term = node.raft.term;
        assert!(term >= 1);

        // "Restart": reopen storage from disk; term and vote survive.
        drop(node);
        let reopened = FileStorage::open(dir.path()).unwrap();
        assert_eq!(reopened.hard_state().term, term);
        assert_eq!(reopened.hard_state().vote, 1);
        assert_eq!(reopened.initial_state().unwrap().conf_state.voters, vec![1]);
    }
}
