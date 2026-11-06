//! The voter cap on the stored membership: which membership an opened
//! store puts in force, and `spawn` refusing one over the cap before the
//! driver thread exists. The cap on the `--raft-peer` list itself is
//! tested beside `validate_peer_list`.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ed25519_dalek::SigningKey;
use openraft::CommittedLeaderId;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::LogId;
use openraft::Membership;
use openraft::storage::RaftLogStorageExt;
use openraft::storage::RaftStateMachine;
use tempfile::TempDir;

use crate::driver::{MAX_VOTERS, RaftConfig, RaftPeer, spawn};
use crate::storage;
use crate::tests::{authorized_keys_table, tip_at};
use crate::types::TypeConfig;

/// A membership entry at `index` (term 1) naming voters `1..=voters`.
fn membership_entry(index: u64, voters: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
        payload: EntryPayload::Membership(Membership::new(
            vec![(1..=voters).collect::<BTreeSet<u64>>()],
            None,
        )),
    }
}

/// The voter count the store at `dir` puts in force when reopened.
fn stored_voters(dir: &Path) -> usize {
    let (log, sm) = storage::open(dir).expect("reopen the store");
    storage::stored_membership(&log, &sm)
        .membership()
        .voter_ids()
        .count()
}

/// Append each of `logged` to the log of the store at `dir`, then apply
/// each of `applied` to its state machine.
async fn store_with(dir: &Path, logged: Vec<Entry<TypeConfig>>, applied: Vec<Entry<TypeConfig>>) {
    let (mut log, mut sm) = storage::open(dir).expect("open the store");
    if !logged.is_empty() {
        log.blocking_append(logged).await.expect("append");
    }
    if !applied.is_empty() {
        sm.apply(applied).await.expect("apply");
    }
}

#[tokio::test]
async fn a_store_never_initialised_holds_no_voters() {
    let dir = TempDir::new().unwrap();
    assert_eq!(stored_voters(dir.path()), 0);
}

/// Appended by `initialize` but not yet applied when the node stopped:
/// openraft reads it back from the log, so the check must too.
#[tokio::test]
async fn a_membership_only_in_the_log_is_in_force() {
    let dir = TempDir::new().unwrap();
    store_with(dir.path(), vec![membership_entry(1, 4)], vec![]).await;
    assert_eq!(stored_voters(dir.path()), 4);
}

/// A leader appends a blank entry right after winning, so a node stopped
/// before applying holds the membership behind a non-membership entry:
/// the check must walk back past it rather than read the last entry only.
#[tokio::test]
async fn a_logged_membership_behind_a_blank_entry_is_in_force() {
    let dir = TempDir::new().unwrap();
    let blank = Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 0), 2),
        payload: EntryPayload::Blank,
    };
    store_with(dir.path(), vec![membership_entry(1, 4), blank], vec![]).await;
    assert_eq!(stored_voters(dir.path()), 4);
}

#[tokio::test]
async fn a_newer_logged_membership_overrides_the_applied_one() {
    let dir = TempDir::new().unwrap();
    store_with(
        dir.path(),
        vec![membership_entry(1, 4), membership_entry(2, 3)],
        vec![membership_entry(1, 4)],
    )
    .await;
    assert_eq!(stored_voters(dir.path()), 3);
}

/// The log may still hold an older membership entry than the one last
/// applied (purging trails applying); the applied one is the newer.
#[tokio::test]
async fn an_applied_membership_overrides_an_older_logged_one() {
    let dir = TempDir::new().unwrap();
    store_with(
        dir.path(),
        vec![membership_entry(1, 3)],
        vec![membership_entry(1, 3), membership_entry(2, 4)],
    )
    .await;
    assert_eq!(stored_voters(dir.path()), 4);
}

/// `spawn` over the store at `dir`, with a `--raft-peer` list within the
/// cap; returns its refusal.
fn spawn_refusal(dir: &Path) -> std::io::Error {
    let keys: Vec<SigningKey> = (1..=MAX_VOTERS as u8)
        .map(|i| SigningKey::from_bytes(&[i; 32]))
        .collect();
    let peers: Vec<RaftPeer> = keys
        .iter()
        .zip(1u64..)
        .map(|(key, id)| RaftPeer {
            id,
            addr: format!("127.0.0.1:{}", 7000 + id),
            pubkey: key.verifying_key().to_bytes(),
        })
        .collect();
    let entries: Vec<(&SigningKey, &str, &str)> =
        keys.iter().map(|k| (k, "replication", "node")).collect();
    let config = RaftConfig {
        node_id: 1,
        // Any free port: the refusal comes after the bind, before the
        // driver starts.
        bind: "127.0.0.1:0".parse().unwrap(),
        dir: dir.to_path_buf(),
        peers,
    };
    match spawn(
        config,
        Arc::new(keys[0].clone()),
        Arc::new(authorized_keys_table(&entries)),
        tip_at(0, 0),
        None,
        Arc::new(AtomicBool::new(false)),
    ) {
        Ok(_) => panic!("spawn accepted a stored membership over the voter cap"),
        Err(e) => e,
    }
}

fn assert_refused_for_the_stored_membership(dir: &Path) {
    let e = spawn_refusal(dir);
    assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        e.to_string(),
        format!(
            "the membership stored in {} holds {} raft voters, more than the {MAX_VOTERS} a \
             cluster can use (the primary and one per replica slot): a voter that can never be \
             a connected replica adds nothing to failover; re-provision --raft-dir with at most \
             {MAX_VOTERS} --raft-peer entries",
            dir.display(),
            MAX_VOTERS + 1
        )
    );
}

/// A raft directory initialised before the cap, with more voters than it
/// allows, refuses the node's start even though the flags are within it:
/// the stored membership overrides the flags.
#[tokio::test]
async fn spawn_refuses_an_applied_membership_over_the_cap() {
    let dir = TempDir::new().unwrap();
    let over = MAX_VOTERS as u64 + 1;
    store_with(
        dir.path(),
        vec![membership_entry(1, over)],
        vec![membership_entry(1, over)],
    )
    .await;
    assert_refused_for_the_stored_membership(dir.path());
}

#[tokio::test]
async fn spawn_refuses_a_logged_membership_over_the_cap() {
    let dir = TempDir::new().unwrap();
    store_with(
        dir.path(),
        vec![membership_entry(1, MAX_VOTERS as u64 + 1)],
        vec![],
    )
    .await;
    assert_refused_for_the_stored_membership(dir.path());
}
