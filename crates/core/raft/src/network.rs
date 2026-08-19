//! Outbound control-plane RPC: the [`RaftNetworkFactory`]/[`RaftNetwork`]
//! implementation openraft sends votes, appends, and snapshots through.
//!
//! One [`RaftClient`] per (replication stream × target); each owns its own
//! TCP connection, so requests on a client are strictly serial
//! request/response — no correlation ids needed. Connections are lazy
//! (`new_client` must not connect and cannot fail) and authenticate with the
//! node's replication signing key on first use; any I/O error drops the
//! connection and surfaces to openraft, whose backoff (500 ms default)
//! drives the retry.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::Unreachable;
use openraft::network::RPCOption;
use openraft::network::RaftNetwork;
use openraft::network::RaftNetworkFactory;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use tokio::net::TcpStream;
use tracing::debug;

use crate::auth::authenticate_outbound;
use crate::recency::{JournalTip, PeerTips, TipSource};
use crate::types::Node;
use crate::types::NodeId;
use crate::types::TypeConfig;
use crate::wire::RpcBody;
use crate::wire::RpcFrame;
use crate::wire::read_frame;
use crate::wire::write_frame;

/// Bound on connect + auth, independent of the per-RPC ttl, so a black-holed
/// SYN can't eat the entire RPC budget before the request is even sent.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub struct RaftClientFactory {
    signing_key: Arc<SigningKey>,
    tip: Arc<TipSource>,
    peer_tips: Arc<PeerTips>,
}

impl RaftClientFactory {
    pub fn new(
        signing_key: Arc<SigningKey>,
        tip: Arc<TipSource>,
        peer_tips: Arc<PeerTips>,
    ) -> Self {
        Self {
            signing_key,
            tip,
            peer_tips,
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftClientFactory {
    type Network = RaftClient;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        RaftClient {
            target,
            addr: node.addr.clone(),
            signing_key: Arc::clone(&self.signing_key),
            tip: Arc::clone(&self.tip),
            peer_tips: Arc::clone(&self.peer_tips),
            stream: None,
        }
    }
}

pub struct RaftClient {
    target: NodeId,
    addr: String,
    signing_key: Arc<SigningKey>,
    tip: Arc<TipSource>,
    /// Sink for the target's tip, read off every reply envelope — this
    /// is how a leader learns its followers' tips (followers make no
    /// outbound RPCs, so the server-side sampling never fires on them).
    peer_tips: Arc<PeerTips>,
    /// Live authenticated connection, established on first use and dropped
    /// on any I/O error.
    stream: Option<TcpStream>,
}

impl RaftClient {
    /// Connect + authenticate if there is no live connection.
    async fn ensure_connected(&mut self) -> io::Result<()> {
        if self.stream.is_some() {
            return Ok(());
        }
        let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&self.addr))
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("connect to {} timed out", self.addr),
                )
            })??;
        // Control-plane frames are tiny and latency-sensitive relative to
        // their size; never batch them behind Nagle.
        stream.set_nodelay(true)?;
        tokio::time::timeout(
            CONNECT_TIMEOUT,
            authenticate_outbound(&mut stream, &self.signing_key),
        )
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!("auth with {} timed out", self.addr),
            )
        })??;
        debug!(target = self.target, addr = %self.addr, "raft peer link established");
        self.stream = Some(stream);
        Ok(())
    }

    /// One serial request/response exchange. Any failure drops the
    /// connection so the next call starts fresh.
    async fn request(&mut self, body: RpcBody, ttl: Duration) -> io::Result<RpcBody> {
        let result = tokio::time::timeout(ttl, async {
            self.ensure_connected().await?;
            // `ensure_connected` just set it on the success path.
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| io::Error::other("no connection after connect"))?;
            let local = self.tip.local_tip();
            let (tip_epoch, tip_seq) = (local.epoch, local.last_sequence);
            write_frame(
                stream,
                &RpcFrame {
                    tip_epoch,
                    tip_seq,
                    body,
                },
            )
            .await?;
            read_frame(stream).await
        })
        .await
        .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "rpc timed out")));

        match result {
            Ok(frame) => {
                // Every reply envelope carries the target's journal tip —
                // feed the promotion-time safety check (see `PeerTips`).
                self.peer_tips.record(
                    self.target,
                    JournalTip {
                        epoch: frame.tip_epoch,
                        last_sequence: frame.tip_seq,
                    },
                );
                Ok(frame.body)
            }
            Err(e) => {
                // Tear down so the next attempt reconnects from scratch.
                self.stream = None;
                debug!(target = self.target, error = %e, "raft peer rpc failed");
                Err(e)
            }
        }
    }
}

/// Map a transport failure onto openraft's error taxonomy: failures *before*
/// a request could be sent mean the peer is unreachable (openraft backs off
/// harder); failures mid-exchange are transient network errors.
fn to_rpc_error<E: std::error::Error + 'static>(e: io::Error) -> RPCError<NodeId, Node, E> {
    match e.kind() {
        io::ErrorKind::TimedOut | io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound => {
            RPCError::Unreachable(Unreachable::new(&e))
        }
        _ => RPCError::Network(NetworkError::new(&e)),
    }
}

impl RaftNetwork<TypeConfig> for RaftClient {
    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        match self.request(RpcBody::VoteReq(rpc), option.hard_ttl()).await {
            Ok(RpcBody::VoteResp(resp)) => Ok(resp),
            Ok(RpcBody::Error(msg)) => {
                Err(RPCError::Network(NetworkError::new(&io::Error::other(msg))))
            }
            Ok(other) => Err(RPCError::Network(NetworkError::new(&io::Error::other(
                format!("unexpected response to vote: {other:?}"),
            )))),
            Err(e) => Err(to_rpc_error(e)),
        }
    }

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, Node, RaftError<NodeId>>> {
        match self
            .request(RpcBody::AppendReq(rpc), option.hard_ttl())
            .await
        {
            Ok(RpcBody::AppendResp(resp)) => Ok(resp),
            Ok(RpcBody::Error(msg)) => {
                Err(RPCError::Network(NetworkError::new(&io::Error::other(msg))))
            }
            Ok(other) => Err(RPCError::Network(NetworkError::new(&io::Error::other(
                format!("unexpected response to append_entries: {other:?}"),
            )))),
            Err(e) => Err(to_rpc_error(e)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, Node, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self
            .request(RpcBody::SnapshotReq(rpc), option.hard_ttl())
            .await
        {
            Ok(RpcBody::SnapshotResp(resp)) => Ok(resp),
            Ok(RpcBody::Error(msg)) => {
                Err(RPCError::Network(NetworkError::new(&io::Error::other(msg))))
            }
            Ok(other) => Err(RPCError::Network(NetworkError::new(&io::Error::other(
                format!("unexpected response to install_snapshot: {other:?}"),
            )))),
            Err(e) => Err(to_rpc_error(e)),
        }
    }
}
