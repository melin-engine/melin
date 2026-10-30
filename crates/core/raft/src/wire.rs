//! Control-plane RPC wire format.
//!
//! Length-prefixed frames (`[len: u32 LE][postcard payload]`) carrying an
//! [`RpcFrame`] envelope. The envelope always includes the sender's journal
//! tip (fencing epoch + advertised journal sequence), read live from the
//! node's `TipSource` — the voter side feeds it to the vote recency filter
//! (see `crate::recency`).
//!
//! Postcard over bincode: compact varint encoding, and no encoder/decoder
//! configuration knobs (fixint vs varint, endianness, limits) that could
//! silently diverge between peers.
//!
//! Before the first envelope, right after authentication, both ends of a
//! link send a version frame carrying [`RAFT_PROTOCOL_VERSION`] and read
//! the other's ([`exchange_version`]); a link whose ends speak different
//! versions is closed before either acts on an envelope. Postcard is not
//! self-describing, so without it an envelope change between releases
//! could be misdecoded by the other release rather than refused.

use std::fmt;

use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use serde::Deserialize;
use serde::Serialize;
use std::io;
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

/// Raft protocol version, exchanged in both directions right after
/// authentication ([`exchange_version`]). Bumped whenever what crosses the
/// raft link changes (the [`RpcFrame`] envelope, the openraft types it
/// carries, the handshake), so a mixed-version link is refused with both
/// versions named instead of misdecoding. `u16`, like the replication
/// protocol's version: it counts releases that change the link, and the
/// version frame's layout fixes its width for good.
/// History: 1 = the first versioned link. Releases before it send an
/// `RpcFrame` straight after authentication, and refuse this one's
/// version frame (see [`VERSION_FRAME_LEN`]).
pub const RAFT_PROTOCOL_VERSION: u16 = 1;

/// Tag byte of the version frame. The tag plays no part in old releases
/// refusing the frame (its length does that, see [`VERSION_FRAME_LEN`]);
/// it is distinct from every replication-protocol tag so that, should a
/// raft dialer reach a replication port, the replication decoder refuses
/// the version frame as an unknown tag rather than acting on it.
const MSG_RAFT_VERSION: u8 = 0x52;

/// Payload length of the version frame: `[tag: u8][version: u16 LE]`,
/// after the usual `u32` LE length prefix. Hand-encoded, never postcard,
/// and fixed for good, so every release can read an older or newer peer's
/// version whatever else changes.
///
/// Three bytes is also what makes releases before the version refuse it.
/// They read an `RpcFrame` straight after authentication, and the shortest
/// postcard `RpcFrame` is four bytes: `tip_epoch`, `tip_seq` and the
/// `RpcBody` variant index each take at least one varint byte, and every
/// variant's body at least one more (the smallest is an empty `Error`
/// string's length). A three-byte payload therefore runs out mid-frame in
/// their decoder, whatever the tag and version, and is refused rather than
/// read as a vote or a tip. Pinned against a frozen copy of their decoder
/// in `tests::pre_version_wire`.
pub(crate) const VERSION_FRAME_LEN: usize = 3;

/// Why a raft link was refused at the version exchange: the peer runs
/// another release. Carried inside an `io::Error` on the dialing side, so
/// `network::to_rpc_error` can tell it from a transient failure by type.
/// Every variant names this node's version (`ours`): the operator reading
/// either end's log learns both.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VersionRefusal {
    /// The peer's version frame names another version.
    Mismatch { ours: u16, peer: u16 },
    /// The peer's first frame is not [`VERSION_FRAME_LEN`] bytes long. A
    /// release before the version sends an `RpcFrame` here, which is
    /// always longer.
    WrongLength { ours: u16, len: usize },
    /// The peer's first frame has the version frame's length but another
    /// tag.
    WrongTag { ours: u16, tag: u8 },
}

impl fmt::Display for VersionRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            VersionRefusal::Mismatch { ours, peer } => write!(
                f,
                "raft peer speaks raft protocol v{peer}, this node speaks v{ours}; run the \
                 same release on every node"
            ),
            VersionRefusal::WrongLength { ours, len } => write!(
                f,
                "raft peer sent a {len}-byte frame where its {VERSION_FRAME_LEN}-byte raft \
                 protocol version frame belongs, so it likely runs a release from before raft \
                 protocol versions; this node speaks v{ours}; run the same release on every node"
            ),
            VersionRefusal::WrongTag { ours, tag } => write!(
                f,
                "raft peer sent a frame tagged 0x{tag:02x} where its raft protocol version \
                 frame (tag 0x{MSG_RAFT_VERSION:02x}) belongs; this node speaks v{ours}; run \
                 the same release on every node"
            ),
        }
    }
}

impl std::error::Error for VersionRefusal {}

impl From<VersionRefusal> for io::Error {
    fn from(refusal: VersionRefusal) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, refusal)
    }
}

/// How the version exchange failed: the peer runs another release (a
/// refusal, which needs the operator), or the link failed under it (an I/O
/// error, like any other on the link). An enum rather than an `io::Error`
/// so each side matches both cases exhaustively and logs them at their
/// own level.
#[derive(Debug)]
pub(crate) enum VersionExchangeError {
    Refused(VersionRefusal),
    Io(io::Error),
}

impl From<io::Error> for VersionExchangeError {
    fn from(e: io::Error) -> Self {
        VersionExchangeError::Io(e)
    }
}

impl From<VersionRefusal> for VersionExchangeError {
    fn from(refusal: VersionRefusal) -> Self {
        VersionExchangeError::Refused(refusal)
    }
}

/// The version frame for `version`, length prefix included.
pub(crate) fn encode_version_frame(version: u16) -> [u8; 4 + VERSION_FRAME_LEN] {
    let mut frame = [0u8; 4 + VERSION_FRAME_LEN];
    frame[..4].copy_from_slice(&(VERSION_FRAME_LEN as u32).to_le_bytes());
    frame[4] = MSG_RAFT_VERSION;
    frame[5..].copy_from_slice(&version.to_le_bytes());
    frame
}

/// The version a version-frame payload carries. The array type holds the
/// length check: the caller has read exactly [`VERSION_FRAME_LEN`] bytes.
fn decode_version_frame(
    payload: &[u8; VERSION_FRAME_LEN],
    ours: u16,
) -> Result<u16, VersionRefusal> {
    let [tag, lo, hi] = *payload;
    if tag != MSG_RAFT_VERSION {
        return Err(VersionRefusal::WrongTag { ours, tag });
    }
    Ok(u16::from_le_bytes([lo, hi]))
}

/// Send this node's [`RAFT_PROTOCOL_VERSION`] and check the peer's, on a
/// link that has just authenticated. Both ends call it, dialer and
/// responder alike, and both write before they read: the frames are a few
/// bytes, so neither write waits on the other end's read.
pub(crate) async fn exchange_version<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
) -> Result<(), VersionExchangeError> {
    exchange_version_as(stream, RAFT_PROTOCOL_VERSION).await
}

/// [`exchange_version`] for a node speaking `ours`: the seam that lets a
/// test play a peer on another release.
pub(crate) async fn exchange_version_as<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    ours: u16,
) -> Result<(), VersionExchangeError> {
    stream.write_all(&encode_version_frame(ours)).await?;
    stream.flush().await?;
    // Judged on the length prefix alone, before any payload is read: an
    // older release's first `RpcFrame` may be megabytes long, and nothing
    // in it is worth reading.
    let len = read_length_prefix(stream).await?;
    if len != VERSION_FRAME_LEN {
        return Err(VersionRefusal::WrongLength { ours, len }.into());
    }
    let mut payload = [0u8; VERSION_FRAME_LEN];
    stream.read_exact(&mut payload).await?;
    let peer = decode_version_frame(&payload, ours)?;
    if peer != ours {
        return Err(VersionRefusal::Mismatch { ours, peer }.into());
    }
    Ok(())
}

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
    let payload = read_bounded_payload(r, MAX_RPC_FRAME, "rpc frame").await?;
    postcard::from_bytes(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("decode rpc frame: {e}")))
}

/// Read a frame's `u32` LE length prefix (the convention of every frame on
/// the raft link, and of the replication protocol).
async fn read_length_prefix<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<usize> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    Ok(u32::from_le_bytes(len_buf) as usize)
}

/// Read one length-prefixed payload, refusing a length above `cap` before
/// allocating for it. `what` names the frame in the refusal. The one
/// reader of both the envelope ([`read_frame`]) and the authentication
/// control frames (`crate::auth`), which differ only in their cap.
pub(crate) async fn read_bounded_payload<R: AsyncRead + Unpin>(
    r: &mut R,
    cap: usize,
    what: &str,
) -> io::Result<Vec<u8>> {
    let len = read_length_prefix(r).await?;
    if len > cap {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{what} length {len} exceeds cap {cap}"),
        ));
    }
    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;
    Ok(payload)
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
    fn version_frame_layout_is_a_tag_and_a_little_endian_version() {
        assert_eq!(
            encode_version_frame(0x0102),
            [3, 0, 0, 0, MSG_RAFT_VERSION, 0x02, 0x01]
        );
    }

    #[test]
    fn version_frame_round_trips_every_version() {
        for version in [0, 1, RAFT_PROTOCOL_VERSION, 0x0100, u16::MAX] {
            let frame = encode_version_frame(version);
            let payload: [u8; VERSION_FRAME_LEN] = frame[4..].try_into().unwrap();
            assert_eq!(
                u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize,
                VERSION_FRAME_LEN
            );
            assert_eq!(decode_version_frame(&payload, 9), Ok(version));
        }
    }

    #[test]
    fn version_frame_with_another_tag_is_refused() {
        let refusal = decode_version_frame(&[0x05, 1, 0], 1).unwrap_err();
        assert_eq!(refusal, VersionRefusal::WrongTag { ours: 1, tag: 0x05 });
        let message = refusal.to_string();
        assert!(
            message.contains("0x05") && message.contains("0x52") && message.contains("v1"),
            "{message}"
        );
    }

    /// One end of a link at `ours`, against a peer that sends `peer_bytes`
    /// once authenticated. Returns this end's verdict and what it sent.
    async fn exchange_against(
        ours: u16,
        peer_bytes: &[u8],
    ) -> (Result<(), VersionExchangeError>, Vec<u8>) {
        let (mut ours_end, mut peer_end) = tokio::io::duplex(1024);
        peer_end.write_all(peer_bytes).await.unwrap();
        // The peer stops sending after its bytes, so a read past them ends
        // in EOF rather than a wait.
        peer_end.shutdown().await.unwrap();
        let verdict = exchange_version_as(&mut ours_end, ours).await;
        drop(ours_end);
        let mut sent = Vec::new();
        peer_end.read_to_end(&mut sent).await.unwrap();
        (verdict, sent)
    }

    #[tokio::test]
    async fn matching_versions_pass_and_each_end_sends_its_own() {
        let (verdict, sent) = exchange_against(
            RAFT_PROTOCOL_VERSION,
            &encode_version_frame(RAFT_PROTOCOL_VERSION),
        )
        .await;
        verdict.unwrap();
        assert_eq!(sent, encode_version_frame(RAFT_PROTOCOL_VERSION));
    }

    /// Both ends of a real exchange, at the versions given, run together.
    async fn exchange_between(
        dialer: u16,
        responder: u16,
    ) -> (
        Result<(), VersionExchangeError>,
        Result<(), VersionExchangeError>,
    ) {
        let (mut dialer_end, mut responder_end) = tokio::io::duplex(1024);
        tokio::join!(
            exchange_version_as(&mut dialer_end, dialer),
            exchange_version_as(&mut responder_end, responder),
        )
    }

    fn refusal(verdict: Result<(), VersionExchangeError>) -> VersionRefusal {
        match verdict {
            Err(VersionExchangeError::Refused(refusal)) => refusal,
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn both_ends_at_the_same_version_connect() {
        let (dialer, responder) =
            exchange_between(RAFT_PROTOCOL_VERSION, RAFT_PROTOCOL_VERSION).await;
        dialer.unwrap();
        responder.unwrap();
    }

    /// A newer dialer against an older responder, and the reverse: both
    /// ends refuse, and each names both versions.
    #[tokio::test]
    async fn a_mismatch_is_refused_by_both_ends_in_either_direction_naming_both_versions() {
        for (dialer, responder) in [(2, 1), (1, 2)] {
            let (dialer_verdict, responder_verdict) = exchange_between(dialer, responder).await;
            let at_dialer = refusal(dialer_verdict);
            let at_responder = refusal(responder_verdict);
            assert_eq!(
                at_dialer,
                VersionRefusal::Mismatch {
                    ours: dialer,
                    peer: responder
                }
            );
            assert_eq!(
                at_responder,
                VersionRefusal::Mismatch {
                    ours: responder,
                    peer: dialer
                }
            );
            for message in [at_dialer.to_string(), at_responder.to_string()] {
                assert!(
                    message.contains(&format!("v{dialer}"))
                        && message.contains(&format!("v{responder}")),
                    "{message}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_short_frame_is_refused_without_waiting_for_more() {
        let mut short = Vec::new();
        short.extend_from_slice(&2u32.to_le_bytes());
        short.extend_from_slice(&[MSG_RAFT_VERSION, 1]);
        let (verdict, _) = exchange_against(1, &short).await;
        assert_eq!(
            refusal(verdict),
            VersionRefusal::WrongLength { ours: 1, len: 2 }
        );
    }

    #[tokio::test]
    async fn a_frame_with_another_tag_is_refused() {
        let mut wrong_tag = encode_version_frame(1);
        wrong_tag[4] = 0x03;
        let (verdict, _) = exchange_against(1, &wrong_tag).await;
        assert_eq!(
            refusal(verdict),
            VersionRefusal::WrongTag { ours: 1, tag: 0x03 }
        );
    }

    /// What a release before the version sends first: an envelope. Refused
    /// on its length prefix, its payload never read, with a message that
    /// points at the release.
    #[tokio::test]
    async fn an_envelope_where_the_version_belongs_is_refused_on_its_length() {
        let mut envelope = Vec::new();
        write_frame(
            &mut envelope,
            &RpcFrame {
                tip_epoch: 0,
                tip_seq: 0,
                body: RpcBody::Error(String::new()),
            },
        )
        .await
        .unwrap();
        let len = envelope.len() - 4;
        let (verdict, _) = exchange_against(1, &envelope[..4]).await;
        let refusal = refusal(verdict);
        assert_eq!(refusal, VersionRefusal::WrongLength { ours: 1, len });
        let message = refusal.to_string();
        assert!(
            message.contains("before raft protocol versions") && message.contains("v1"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn a_peer_that_hangs_up_is_an_io_error_not_a_refusal() {
        let (verdict, _) = exchange_against(1, &[]).await;
        match verdict {
            Err(VersionExchangeError::Io(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof)
            }
            other => panic!("expected an I/O error, got {other:?}"),
        }
    }

    #[test]
    fn a_refusal_rides_an_io_error_as_invalid_data_and_stays_typed() {
        let e = io::Error::from(VersionRefusal::Mismatch { ours: 1, peer: 2 });
        assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        assert!(
            e.get_ref()
                .is_some_and(|inner| inner.is::<VersionRefusal>())
        );
    }
}
