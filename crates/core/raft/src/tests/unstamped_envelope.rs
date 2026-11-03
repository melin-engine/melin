//! Raft protocol 1 without the sender stamp, against this build's envelope.
//!
//! The stamp joined version 1 after the version exchange did, without a
//! bump (see `crate::wire::RAFT_PROTOCOL_VERSION`), so a build from between
//! the two passes the version exchange with this one and then sends the
//! stampless envelope. That envelope, and its reader, are byte for byte the
//! ones frozen in `pre_version_wire` (the version changed the handshake,
//! not the envelope), which these tests use as the stampless build.
//!
//! What must hold either way, for every body: the stampless build's frame
//! is refused by this build, never read as a vote, a tip or a stamp; and
//! this build's frame is read by the stampless build exactly as sent, the
//! stamp it does not know ignored. Both follow from the stamp's place at
//! the end of the envelope (see `crate::wire::RpcFrame`).

use std::io;

use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{CommittedLeaderId, LogId, SnapshotMeta, Vote};

use crate::tests::pre_version_wire;
use crate::tests::stamped_at;
use crate::tests::storage_suite::{blank_entry, membership_entry};
use crate::wire;

/// Every kind of body, short and long, as `$wire`'s `RpcBody`: the same
/// list for the stampless build's enum and this build's, which share
/// variant names and contents.
macro_rules! every_body {
    ($wire:ident) => {{
        let vote = Vote::new(5, 2);
        let log_id = Some(LogId::new(CommittedLeaderId::new(5, 2), 9));
        vec![
            $wire::RpcBody::VoteReq(VoteRequest {
                vote,
                last_log_id: log_id,
            }),
            $wire::RpcBody::VoteResp(VoteResponse {
                vote,
                vote_granted: true,
                last_log_id: None,
            }),
            // A heartbeat, and an append long enough to hold a stamp's
            // worth of bytes several times over.
            $wire::RpcBody::AppendReq(AppendEntriesRequest {
                vote,
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            }),
            $wire::RpcBody::AppendReq(AppendEntriesRequest {
                vote,
                prev_log_id: log_id,
                entries: (10..20)
                    .map(|i| {
                        if i % 3 == 0 {
                            membership_entry(5, i)
                        } else {
                            blank_entry(5, i)
                        }
                    })
                    .collect(),
                leader_commit: log_id,
            }),
            $wire::RpcBody::AppendResp(AppendEntriesResponse::Success),
            $wire::RpcBody::AppendResp(AppendEntriesResponse::HigherVote(vote)),
            $wire::RpcBody::SnapshotReq(InstallSnapshotRequest {
                vote,
                meta: SnapshotMeta {
                    last_log_id: log_id,
                    last_membership: Default::default(),
                    snapshot_id: "snap-1".to_owned(),
                },
                offset: 0,
                data: vec![0xab; 64],
                done: true,
            }),
            $wire::RpcBody::SnapshotResp(InstallSnapshotResponse { vote }),
            $wire::RpcBody::Error(String::new()),
            $wire::RpcBody::Error("x".repeat(200)),
        ]
    }};
}

/// The stampless build's envelope for `body` at `(epoch, seq)`, on the wire.
async fn unstamped_frame(epoch: u64, seq: u64, body: pre_version_wire::RpcBody) -> Vec<u8> {
    let mut buf = Vec::new();
    pre_version_wire::write_frame(
        &mut buf,
        &pre_version_wire::RpcFrame {
            tip_epoch: epoch,
            tip_seq: seq,
            body,
        },
    )
    .await
    .unwrap();
    buf
}

/// The refusal this build gives a stampless frame: a decode error, as for
/// any malformed envelope, which closes the link (`rpc_server` and the
/// client both drop a link whose frame does not decode).
fn assert_refused(read: io::Result<wire::RpcFrame>, what: &str) {
    match read {
        Err(e) => {
            assert_eq!(e.kind(), io::ErrorKind::InvalidData, "{what}: {e}");
            assert!(e.to_string().starts_with("decode rpc frame"), "{what}: {e}");
        }
        Ok(frame) => panic!("{what}: a stampless envelope was read as {frame:?}"),
    }
}

/// The stampless build's frames, whatever their body and tip, are refused:
/// this build reads every field they hold, then runs out of payload where
/// the stamp belongs.
#[tokio::test]
async fn this_build_refuses_a_stampless_envelope_whatever_its_body() {
    for (epoch, seq) in [(0, 0), (3, 1_000), (u64::MAX, u64::MAX)] {
        for body in every_body!(pre_version_wire) {
            let what = format!("({epoch}, {seq}) {body:?}");
            let frame = unstamped_frame(epoch, seq, body).await;
            assert_refused(wire::read_frame(&mut frame.as_slice()).await, &what);
        }
    }
}

/// The stampless build reads this build's frames exactly as sent, tip and
/// body, whatever the stamp: it decodes the fields it knows, which come
/// first, and ignores the rest.
#[tokio::test]
async fn a_stampless_build_reads_this_builds_envelope_as_sent() {
    for counter in [0, 42, u64::MAX] {
        let sent = stamped_at(7, 1234, counter);
        for (theirs, ours) in every_body!(pre_version_wire)
            .into_iter()
            .zip(every_body!(wire))
        {
            let mut buf = Vec::new();
            wire::write_frame(&mut buf, &wire::RpcFrame::new(sent, ours))
                .await
                .unwrap();
            let read = pre_version_wire::read_frame(&mut buf.as_slice())
                .await
                .unwrap_or_else(|e| panic!("{theirs:?}: {e}"));
            assert_eq!((read.tip_epoch, read.tip_seq), (7, 1234));
            // The two enums share variant names and contents, so equal
            // bodies print alike.
            assert_eq!(format!("{:?}", read.body), format!("{theirs:?}"));
        }
    }
}

proptest::proptest! {
    /// Any stampless envelope with an `Error` body (the one whose contents
    /// the sender picks freely, any length, any text) is refused: no text
    /// can make the frame end in something that reads as a stamp.
    #[test]
    fn a_stampless_error_envelope_is_refused_whatever_its_text(
        epoch: u64, seq: u64, text in ".{0,80}"
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let frame =
                unstamped_frame(epoch, seq, pre_version_wire::RpcBody::Error(text.clone())).await;
            assert_refused(wire::read_frame(&mut frame.as_slice()).await, &text);
        });
    }
}
