//! Control-plane RPC wire format.
//!
//! Length-prefixed frames (`[len: u32 LE][postcard payload]`) carrying an
//! [`RpcFrame`] envelope. The envelope always includes the sender's journal
//! tip (fencing epoch + last durable sequence) so the format never changes
//! when the vote recency filter and fence-on-supersession land — until then
//! senders fill in zeros.
//!
//! Postcard over bincode: compact varint encoding, and no encoder/decoder
//! configuration knobs (fixint vs varint, endianness, limits) that could
//! silently diverge between peers.

use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

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

/// Frame size cap. The largest legitimate frame is an InstallSnapshot chunk
/// (openraft's default `snapshot_max_chunk_size` is 3 MiB); 4 MiB accepts it
/// with margin while refusing pathological lengths from a corrupt or
/// malicious prefix.
pub const MAX_RPC_FRAME: usize = 4 << 20;

/// Every control-plane message, request or response, rides in this envelope.
#[derive(Debug, Serialize, Deserialize)]
pub struct RpcFrame {
    /// Sender's fencing epoch (journal-derived, not the raft term).
    pub tip_epoch: u64,
    /// Sender's journal tip sequence.
    pub tip_seq: u64,
    pub body: RpcBody,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcBody {
    VoteReq(VoteRequest<NodeId>),
    VoteResp(VoteResponse<NodeId>),
    AppendReq(AppendEntriesRequest<TypeConfig>),
    AppendResp(AppendEntriesResponse<NodeId>),
    SnapshotReq(InstallSnapshotRequest<TypeConfig>),
    SnapshotResp(InstallSnapshotResponse<NodeId>),
    /// The responder could not serve the request (e.g. raft core returned an
    /// error). Stringly-typed on purpose: the caller only retries or logs it,
    /// and a string can't fail to decode across versions.
    Error(String),
}

/// The node-id a request claims to originate from, recovered from the raft
/// payload itself: every request carries the sender's `Vote`, whose leader id
/// names the candidate/leader. Used by the server to enforce that the claimed
/// id matches the one pinned to the connection's authenticated key.
/// `None` for responses (the server never receives those) and for payloads
/// with no self-identification.
pub fn claimed_sender(body: &RpcBody) -> Option<NodeId> {
    match body {
        RpcBody::VoteReq(r) => r.vote.leader_id().voted_for(),
        RpcBody::AppendReq(r) => r.vote.leader_id().voted_for(),
        RpcBody::SnapshotReq(r) => r.vote.leader_id().voted_for(),
        RpcBody::VoteResp(_)
        | RpcBody::AppendResp(_)
        | RpcBody::SnapshotResp(_)
        | RpcBody::Error(_) => None,
    }
}

/// The local journal tip as shared with the RPC layer: two atomics the
/// data plane (fence state + journal cursor) is copied into. Zeros until the
/// journal-tip wiring lands; the RPC layer reads whatever is current.
#[derive(Debug, Default)]
pub struct SharedTip {
    epoch: AtomicU64,
    seq: AtomicU64,
}

impl SharedTip {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Monotonic publish — the tip only ever advances.
    pub fn advance(&self, epoch: u64, seq: u64) {
        self.epoch.fetch_max(epoch, Ordering::Release);
        self.seq.fetch_max(seq, Ordering::Release);
    }

    pub fn load(&self) -> (u64, u64) {
        (
            self.epoch.load(Ordering::Acquire),
            self.seq.load(Ordering::Acquire),
        )
    }
}

/// Write one frame: length prefix + postcard body.
pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, frame: &RpcFrame) -> io::Result<()> {
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
    // u32 length prefix, LE — same convention as the replication protocol.
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&payload);
    w.write_all(&buf).await?;
    w.flush().await
}

/// Read one frame, refusing anything above [`MAX_RPC_FRAME`].
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<RpcFrame> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::Vote;

    #[tokio::test]
    async fn frame_round_trip() {
        let frame = RpcFrame {
            tip_epoch: 7,
            tip_seq: 1234,
            body: RpcBody::VoteReq(VoteRequest {
                vote: Vote::new(3, 42),
                last_log_id: None,
            }),
        };
        let mut buf = Vec::new();
        write_frame(&mut buf, &frame).await.unwrap();
        let decoded = read_frame(&mut buf.as_slice()).await.unwrap();
        assert_eq!(decoded.tip_epoch, 7);
        assert_eq!(decoded.tip_seq, 1234);
        match decoded.body {
            RpcBody::VoteReq(r) => {
                assert_eq!(r.vote, Vote::new(3, 42));
                assert_eq!(claimed_sender(&RpcBody::VoteReq(r)), Some(42));
            }
            other => panic!("wrong body: {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_refuses_oversized_length() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&((MAX_RPC_FRAME as u32) + 1).to_le_bytes());
        buf.extend_from_slice(&[0u8; 16]);
        let err = read_frame(&mut buf.as_slice()).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn shared_tip_is_monotonic() {
        let tip = SharedTip::new();
        tip.advance(2, 100);
        tip.advance(1, 50); // stale publish must not regress
        assert_eq!(tip.load(), (2, 100));
        tip.advance(3, 70); // epoch advances even when seq is behind
        assert_eq!(tip.load(), (3, 100));
    }
}
