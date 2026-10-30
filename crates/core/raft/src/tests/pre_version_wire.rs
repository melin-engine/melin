//! The raft link of the releases before `crate::wire::RAFT_PROTOCOL_VERSION`,
//! frozen, and the proof that they refuse the version frame.
//!
//! **A copy, on purpose.** The types and the reader below are the
//! `RpcFrame` envelope, `RpcBody` and `read_frame`/`write_frame` of
//! `crate::wire` as they were when the version was introduced, the last
//! form of the link without one. They are copied rather than imported
//! because `crate::wire` will change (that is what the version is for),
//! while the releases already deployed will not: an old node reads a new
//! node's version frame with exactly this code, in both directions (an
//! old responder reading the dialer's first frame as a request, an old
//! dialer reading the responder's as a reply). It must refuse it, never
//! read it as a vote or a tip. Do not update this file when the envelope
//! changes; it describes releases that no longer do.
//!
//! The openraft types inside the body are the 0.9 line's, whose serde
//! layout the old releases also carried. An openraft upgrade that changed
//! them would be a raft protocol version bump of its own.

use std::io;

use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use crate::types::NodeId;
use crate::types::TypeConfig;
use crate::wire::VERSION_FRAME_LEN;
use crate::wire::encode_version_frame;

/// The old releases' frame cap.
const MAX_RPC_FRAME: usize = 4 << 20;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RpcFrame {
    pub(crate) tip_epoch: u64,
    pub(crate) tip_seq: u64,
    pub(crate) body: RpcBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RpcBody {
    VoteReq(VoteRequest<NodeId>),
    VoteResp(VoteResponse<NodeId>),
    AppendReq(AppendEntriesRequest<TypeConfig>),
    AppendResp(AppendEntriesResponse<NodeId>),
    SnapshotReq(InstallSnapshotRequest<TypeConfig>),
    SnapshotResp(InstallSnapshotResponse<NodeId>),
    Error(String),
}

pub(crate) async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    frame: &RpcFrame,
) -> io::Result<()> {
    let payload = postcard::to_stdvec(frame).map_err(|e| {
        io::Error::new(io::ErrorKind::InvalidData, format!("encode rpc frame: {e}"))
    })?;
    if payload.len() > MAX_RPC_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "rpc frame {} bytes exceeds cap {MAX_RPC_FRAME}",
                payload.len()
            ),
        ));
    }
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);
    w.write_all(&buf).await?;
    w.flush().await
}

pub(crate) async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<RpcFrame> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_RPC_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("rpc frame length {len} exceeds cap {MAX_RPC_FRAME}"),
        ));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    postcard::from_bytes(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("decode rpc frame: {e}")))
}

/// The guarantee itself: an old release refuses the version frame, as a
/// decode error, whatever version it carries — this release's and every
/// later one's.
#[tokio::test]
async fn an_old_release_refuses_every_version_frame_as_undecodable() {
    for version in 0..=u16::MAX {
        let frame = encode_version_frame(version);
        let err = read_frame(&mut frame.as_slice())
            .await
            .expect_err("a version frame must never decode as an old envelope");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData, "v{version}: {err}");
        assert!(
            err.to_string().starts_with("decode rpc frame"),
            "v{version}: {err}"
        );
    }
}

/// The argument behind it (`VERSION_FRAME_LEN`): the shortest old envelope,
/// an `Error` with an empty message and a zero tip, is longer than the
/// version frame, so the frame runs out mid-envelope.
#[test]
fn the_shortest_old_envelope_is_longer_than_the_version_frame() {
    let shortest = postcard::to_stdvec(&RpcFrame {
        tip_epoch: 0,
        tip_seq: 0,
        body: RpcBody::Error(String::new()),
    })
    .unwrap();
    assert_eq!(shortest.len(), 4);
    assert!(VERSION_FRAME_LEN < shortest.len());
}
