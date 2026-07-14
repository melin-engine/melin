//! Storage compliance and crash-recovery tests for the file-backed store.
//!
//! `Suite::test_all` is openraft's own ~40-case storage semantics suite —
//! if it passes, the store upholds the contract raft correctness rests on.
//! The torn-tail sweep byte-truncates a real log file at every offset to
//! prove recovery never errors and never resurrects unacked entries.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use melin_raft::storage;
use melin_raft::storage::FileLogStore;
use melin_raft::storage::FileStateMachine;
use melin_raft::types::TypeConfig;
use openraft::CommittedLeaderId;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::RaftLogReader;
use openraft::StorageError;
use openraft::Vote;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftLogStorageExt;
use openraft::testing::StoreBuilder;
use openraft::testing::Suite;
use tempfile::TempDir;

struct FileStoreBuilder;

impl StoreBuilder<TypeConfig, FileLogStore, FileStateMachine, TempDir> for FileStoreBuilder {
    async fn build(&self) -> Result<(TempDir, FileLogStore, FileStateMachine), StorageError<u64>> {
        // unwrap: test-only tempdir creation.
        let dir = TempDir::new().unwrap();
        let (log, sm) = storage::open(dir.path())?;
        Ok((dir, log, sm))
    }
}

#[test]
fn openraft_storage_suite() {
    // unwrap: test — and avoids clippy's large-Err lint on the signature.
    Suite::test_all(FileStoreBuilder).unwrap();
}

fn blank_entry(term: u64, index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
        payload: EntryPayload::Blank,
    }
}

fn membership_entry(term: u64, index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(term, 0), index),
        payload: EntryPayload::Membership(Membership::new(
            vec![BTreeSet::from([1u64, 2, 3])],
            None,
        )),
    }
}

async fn open_with_entries(dir: &Path, n: u64) -> FileLogStore {
    let (mut log, _sm) = storage::open(dir).expect("open fresh store");
    let entries: Vec<_> = (1..=n)
        .map(|i| {
            if i % 3 == 0 {
                membership_entry(1, i)
            } else {
                blank_entry(1, i)
            }
        })
        .collect();
    log.blocking_append(entries).await.expect("append");
    log
}

/// Byte-truncate the log file at every length from full size down to zero.
/// Cuts in the append region (past the header + purge marker) are the crash
/// artifact torn-tail recovery exists for: they must reopen cleanly, recover
/// a strict prefix of the appended entries, and leave the file at a clean
/// record boundary (re-reopening recovers the identical state). Cuts inside
/// the header/marker region can only come from external damage (that region
/// is written via atomic rename) and must refuse to open.
#[tokio::test]
async fn torn_tail_sweep_recovers_a_clean_prefix() {
    // The size of a freshly-created empty log is exactly the atomic
    // header + purge-marker region — measure it rather than hardcoding
    // the encoding.
    let atomic_region = {
        let fresh = TempDir::new().unwrap();
        let _ = storage::open(fresh.path()).unwrap();
        fs::read(fresh.path().join("log")).unwrap().len()
    };

    let dir = TempDir::new().unwrap();
    {
        let mut log = open_with_entries(dir.path(), 8).await;
        let state = log.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 8);
    }
    let log_path = dir.path().join("log");
    let pristine = fs::read(&log_path).unwrap();
    assert!(pristine.len() > atomic_region);

    for cut in 0..atomic_region {
        fs::write(&log_path, &pristine[..cut]).unwrap();
        assert!(
            storage::open(dir.path()).is_err(),
            "cut={cut}: damage inside the atomic region must refuse to open"
        );
    }

    let mut prev_recovered = u64::MAX;
    for cut in (atomic_region..=pristine.len()).rev() {
        fs::write(&log_path, &pristine[..cut]).unwrap();

        let (mut log, _sm) = storage::open(dir.path()).expect("torn tail must never fail open");
        let state = log.get_log_state().await.unwrap();
        let recovered = state.last_log_id.map_or(0, |l| l.index);

        // Shorter cut can never recover *more* than a longer one, and
        // recovery is a prefix: indexes 1..=recovered all present.
        assert!(
            recovered <= prev_recovered.min(8),
            "cut={cut}: recovered {recovered}"
        );
        prev_recovered = recovered;
        let entries = log.try_get_log_entries(0..).await.unwrap();
        let indexes: Vec<u64> = entries.iter().map(|e| e.log_id.index).collect();
        assert_eq!(indexes, (1..=recovered).collect::<Vec<_>>(), "cut={cut}");
        drop(log);

        // Idempotent: reopening the truncated-and-repaired file recovers
        // the same state (the repair landed on a clean boundary).
        let (mut log, _sm) = storage::open(dir.path()).expect("second open");
        let state2 = log.get_log_state().await.unwrap();
        assert_eq!(
            state2.last_log_id.map_or(0, |l| l.index),
            recovered,
            "cut={cut}"
        );
    }
}

#[tokio::test]
async fn vote_and_log_survive_reopen() {
    let dir = TempDir::new().unwrap();
    let vote = Vote::new(7, 3);
    {
        let mut log = open_with_entries(dir.path(), 5).await;
        log.save_vote(&vote).await.unwrap();
        log.purge(LogId::new(CommittedLeaderId::new(1, 0), 2))
            .await
            .unwrap();
    }
    let (mut log, _sm) = storage::open(dir.path()).expect("reopen");
    assert_eq!(log.read_vote().await.unwrap(), Some(vote));
    let state = log.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id.unwrap().index, 2);
    assert_eq!(state.last_log_id.unwrap().index, 5);
    let indexes: Vec<u64> = log
        .try_get_log_entries(0..)
        .await
        .unwrap()
        .iter()
        .map(|e| e.log_id.index)
        .collect();
    assert_eq!(indexes, vec![3, 4, 5]);
}

/// A corrupt vote file must refuse to open (a forgotten vote can double-vote
/// within a term), unlike the log's torn tail which is a normal crash artifact.
#[tokio::test]
async fn corrupt_vote_file_refuses_to_open() {
    let dir = TempDir::new().unwrap();
    {
        let mut log = open_with_entries(dir.path(), 1).await;
        log.save_vote(&Vote::new(3, 1)).await.unwrap();
    }
    let vote_path = dir.path().join("vote");
    let mut bytes = fs::read(&vote_path).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(&vote_path, &bytes).unwrap();
    assert!(storage::open(dir.path()).is_err());
}

/// Flipping the format version byte must refuse to open rather than misread
/// (the version gates the `single-term-leader` Vote layout).
#[tokio::test]
async fn version_mismatch_refuses_to_open() {
    let dir = TempDir::new().unwrap();
    {
        let _ = open_with_entries(dir.path(), 1).await;
    }
    let log_path = dir.path().join("log");
    let mut bytes = fs::read(&log_path).unwrap();
    bytes[4] = 99;
    fs::write(&log_path, &bytes).unwrap();
    assert!(storage::open(dir.path()).is_err());
}
