//! Outbound control-plane RPC: the [`RaftNetworkFactory`]/[`RaftNetwork`]
//! implementation openraft sends votes, appends, and snapshots through.
//!
//! One [`RaftClient`] per (replication stream × target); each owns its own
//! TCP connection, so requests on a client are strictly serial
//! request/response — no correlation ids needed. Connections are lazy
//! (`new_client` must not connect and cannot fail) and on first use run the
//! handshake: prove the node's replication signing key, exchange the raft
//! protocol version (`crate::wire::exchange_version`), then challenge the
//! responder and accept only the key configured for the node id dialled
//! (`crate::auth::challenge_responder`). Only that last step yields the
//! [`AuthenticatedResponder`] requests are sent and replies read through,
//! so no reply is read from anything else. Any I/O error drops the
//! connection and surfaces to openraft, whose backoff (500 ms default)
//! drives the retry.
//!
//! A node whose raft listener is bound to a specific IP dials its peers
//! from that IP too ([`plan_dial`]), so the address a peer sees a
//! connection come from is the one it dials back, and per-node loopback
//! IPs let a test tell nodes apart by source.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ed25519_dalek::SigningKey;
use melin_app::auth::AuthorizedKeys;
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
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::auth::AuthError;
use crate::auth::AuthenticatedResponder;
use crate::auth::KeyRefusal;
use crate::auth::answer_challenge;
use crate::auth::challenge_responder;
use crate::auth::key_configured_for;
use crate::recency::{PeerTips, TipSource};
use crate::rpc_server::SupersessionPolicy;
use crate::types::Node;
use crate::types::NodeId;
use crate::types::TypeConfig;
use crate::wire::RpcBody;
use crate::wire::RpcFrame;
use crate::wire::VersionExchangeError;
use crate::wire::VersionRefusal;
use crate::wire::exchange_version;

/// Bound on the connect, and separately on the handshake that follows it
/// (this node's key, the version exchange, then the responder's key),
/// independent of the per-RPC ttl, so a black-holed SYN can't eat the
/// entire RPC budget before the request is even sent.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// How an outbound raft connection to one peer is made, decided by
/// [`plan_dial`] from the node's raft listener address and the peer's
/// stored address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dial {
    /// The listener is bound to a wildcard (`0.0.0.0` / `::`): dial the
    /// stored address as given (a hostname still resolves), and let the
    /// kernel pick the source address.
    KernelSource,
    /// The listener is bound to a specific IP: bind the outgoing socket
    /// to `local` (that IP, port 0, scope id kept for a link-local IPv6
    /// listener) and connect to `peer`.
    FromBind { local: SocketAddr, peer: SocketAddr },
}

/// Why a node whose raft listener is bound to a specific IP cannot dial a
/// peer at all. Refused before any syscall, at startup for the configured
/// peers (`spawn`) and on every connect for the stored membership, which
/// is what is dialled after first boot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DialRefusal {
    /// The IP alone, not a `SocketAddr`: the listener's port plays no part
    /// in the dial, so naming it would only mislead.
    bind: std::net::IpAddr,
    /// The stored string, not a parsed address: a refused peer may not
    /// parse as one, and the operator must see what they configured.
    peer: String,
    reason: DialRefusalReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DialRefusalReason {
    /// A bound socket connects to a `SocketAddr`, so nothing resolves a
    /// hostname on this path.
    NotIpLiteral,
    /// An IPv4 source cannot reach an IPv6 peer, nor the reverse.
    FamilyMismatch,
}

impl fmt::Display for DialRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let why = match self.reason {
            DialRefusalReason::NotIpLiteral => {
                "the peer address is not an IP literal, which a control plane bound to a \
                 specific IP needs"
            }
            DialRefusalReason::FamilyMismatch => {
                "the peer address and the bind are of different address families"
            }
        };
        write!(
            f,
            "control plane bound to {} cannot dial peer {}: {why}",
            self.bind, self.peer
        )
    }
}

impl std::error::Error for DialRefusal {}

impl From<DialRefusal> for io::Error {
    fn from(refusal: DialRefusal) -> Self {
        io::Error::new(io::ErrorKind::InvalidInput, refusal)
    }
}

/// The members other than `node_id` that a node whose raft listener is
/// bound to `bind` could never dial, each with why: [`plan_dial`] over a
/// membership. `spawn` refuses to start on the first of the configured
/// peers, and the driver warns of each in a stored membership that
/// differs from them.
pub(crate) fn undialable_members<'a>(
    bind: SocketAddr,
    node_id: NodeId,
    members: impl IntoIterator<Item = (NodeId, &'a str)>,
) -> impl Iterator<Item = (NodeId, DialRefusal)> {
    members
        .into_iter()
        // A node never dials its own entry: it is what the others dial.
        .filter(move |&(id, _)| id != node_id)
        .filter_map(move |(id, addr)| plan_dial(bind, addr).err().map(|refusal| (id, refusal)))
}

/// Decide how to dial `peer` from a node whose raft listener is bound to
/// `bind`. The one statement of the rule: `spawn` applies it to the
/// configured peers at startup, the driver to a stored membership that
/// differs from them, and [`RaftClient`] to every connect.
pub(crate) fn plan_dial(bind: SocketAddr, peer: &str) -> Result<Dial, DialRefusal> {
    if bind.ip().is_unspecified() {
        return Ok(Dial::KernelSource);
    }
    let refuse = |reason| DialRefusal {
        bind: bind.ip(),
        peer: peer.to_owned(),
        reason,
    };
    let peer_addr: SocketAddr = peer
        .parse()
        .map_err(|_| refuse(DialRefusalReason::NotIpLiteral))?;
    if peer_addr.is_ipv4() != bind.is_ipv4() {
        return Err(refuse(DialRefusalReason::FamilyMismatch));
    }
    let mut local = bind;
    local.set_port(0);
    Ok(Dial::FromBind {
        local,
        peer: peer_addr,
    })
}

/// Whether the raft listener's IP was last found unassigned to this host
/// when dialling from it. A node-wide condition, so one flag per node,
/// shared by every client: the warning fires once per loss, not once per
/// peer, per client openraft creates, or per retry. An `AtomicBool` because
/// clients run on the raft runtime's tasks; it is touched once per dial,
/// never per event.
#[derive(Debug, Default)]
pub(crate) struct BindIp {
    missing: AtomicBool,
}

impl BindIp {
    /// Record a bind that failed with the IP unassigned: true on the first
    /// since the IP was last usable.
    fn lost(&self) -> bool {
        !self.missing.swap(true, Ordering::Relaxed)
    }

    /// Record a bind that succeeded: true if the IP had been missing.
    fn regained(&self) -> bool {
        self.missing.swap(false, Ordering::Relaxed)
    }
}

/// Open a TCP connection to `addr` as `dial` says, reporting through
/// `bind_ip` when the listener's IP goes missing or comes back. Not
/// bounded in time: the caller wraps it in `CONNECT_TIMEOUT`.
async fn connect(dial: Dial, addr: &str, bind_ip: &BindIp) -> io::Result<TcpStream> {
    match dial {
        Dial::KernelSource => TcpStream::connect(addr).await,
        Dial::FromBind { local, peer } => {
            let socket = if local.is_ipv4() {
                TcpSocket::new_v4()?
            } else {
                TcpSocket::new_v6()?
            };
            // Bound before connect, so the ephemeral port is taken without
            // knowing the destination: a handful of raft links per node
            // never comes near exhausting the range. The kind is kept so
            // `AddrNotAvailable` (the IP is no longer assigned) still maps
            // to `Unreachable`.
            match socket.bind(local) {
                Ok(()) => {
                    if bind_ip.regained() {
                        info!(
                            bind = %local.ip(),
                            "raft listener IP is assigned again: the control plane can dial its \
                             peers"
                        );
                    }
                }
                Err(e) => {
                    // Every retry ends here while the IP is gone, and the
                    // per-retry log is `debug!`: say it once, louder, since
                    // the node's control plane can neither dial nor be
                    // reached until the IP returns.
                    if e.kind() == io::ErrorKind::AddrNotAvailable && bind_ip.lost() {
                        warn!(
                            bind = %local.ip(),
                            error = %e,
                            "raft listener IP is no longer assigned to this host: the control \
                             plane can neither dial its peers nor be reached until it returns"
                        );
                    }
                    return Err(io::Error::new(
                        e.kind(),
                        format!("bind {local} to dial peer {addr} failed: {e}"),
                    ));
                }
            }
            socket.connect(peer).await
        }
    }
}

pub struct RaftClientFactory {
    signing_key: Arc<SigningKey>,
    /// The operator's key table, against which a responder's key is
    /// checked for the `replication` role (the same `Arc` the RPC server
    /// checks dialers against).
    authorized_keys: Arc<AuthorizedKeys>,
    /// The `--raft-peer` table, public key → node id: the same `Arc` the
    /// RPC server pins dialers with, so the two ends cannot pin against
    /// different tables. Keyed by key rather than by id because that is
    /// the server's lookup; the dialer's id → key direction is a scan of
    /// the handful of voters, once per connect.
    peer_ids: Arc<HashMap<[u8; 32], NodeId>>,
    tip: Arc<TipSource>,
    peer_tips: Arc<PeerTips>,
    /// The node's raft listener address, whose IP every outbound
    /// connection is made from when it is not a wildcard (see
    /// [`plan_dial`]).
    bind: SocketAddr,
    /// Shared with every client this factory makes (see [`BindIp`]).
    bind_ip: Arc<BindIp>,
    /// The same fence-on-supersession policy the RPC server holds (an
    /// `Arc` clone of it), applied to every reply; `None` when
    /// auto-promotion is off, as on the server.
    supersession: Option<Arc<SupersessionPolicy>>,
}

impl RaftClientFactory {
    pub fn new(
        signing_key: Arc<SigningKey>,
        authorized_keys: Arc<AuthorizedKeys>,
        peer_ids: Arc<HashMap<[u8; 32], NodeId>>,
        tip: Arc<TipSource>,
        peer_tips: Arc<PeerTips>,
        bind: SocketAddr,
        supersession: Option<Arc<SupersessionPolicy>>,
    ) -> Self {
        Self {
            signing_key,
            authorized_keys,
            peer_ids,
            tip,
            peer_tips,
            bind,
            bind_ip: Arc::default(),
            supersession,
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
            authorized_keys: Arc::clone(&self.authorized_keys),
            peer_ids: Arc::clone(&self.peer_ids),
            tip: Arc::clone(&self.tip),
            peer_tips: Arc::clone(&self.peer_tips),
            bind: self.bind,
            bind_ip: Arc::clone(&self.bind_ip),
            supersession: self.supersession.clone(),
            stream: None,
        }
    }
}

pub struct RaftClient {
    target: NodeId,
    addr: String,
    /// See [`RaftClientFactory`]'s field of the same name.
    bind: SocketAddr,
    /// See [`RaftClientFactory`]'s field of the same name.
    bind_ip: Arc<BindIp>,
    signing_key: Arc<SigningKey>,
    /// See [`RaftClientFactory`]'s field of the same name.
    authorized_keys: Arc<AuthorizedKeys>,
    /// See [`RaftClientFactory`]'s field of the same name.
    peer_ids: Arc<HashMap<[u8; 32], NodeId>>,
    tip: Arc<TipSource>,
    /// Sink for the target's tip, read off every reply envelope — this
    /// is how a leader learns its followers' tips (followers make no
    /// outbound RPCs, so the server-side sampling never fires on them).
    peer_tips: Arc<PeerTips>,
    /// See [`RaftClientFactory`]'s field of the same name.
    supersession: Option<Arc<SupersessionPolicy>>,
    /// Live connection, established on first use and dropped on any I/O
    /// error. Its type only a full handshake produces: there is no raw
    /// stream to read a reply from before the responder has proved its
    /// key.
    stream: Option<AuthenticatedResponder<TcpStream>>,
}

impl RaftClient {
    /// Connect and run the handshake if there is no live connection,
    /// returning the authenticated link either way.
    async fn ensure_connected(&mut self) -> io::Result<&mut AuthenticatedResponder<TcpStream>> {
        // Taken and put back rather than borrowed across the connect: the
        // borrow checker cannot see that an early return of the live link
        // ends the borrow (the conditional-return case of NLL).
        if let Some(link) = self.stream.take() {
            return Ok(self.stream.insert(link));
        }
        // Refused before any syscall when the peer cannot be dialled from
        // the bind at all; the error names both.
        let dial = plan_dial(self.bind, &self.addr)?;
        // Nor is a node dialled whose key nothing configures: whatever
        // answered could not be told from it.
        if let Err(refusal) = key_configured_for(&self.peer_ids, self.target) {
            // `debug!`, not `warn!`: this fires on every reconnect attempt
            // at openraft's backoff cadence, and the misconfiguration behind
            // it (a stored member missing from `--raft-peer`) is a static
            // one `driver_main` already warns about once at startup, where
            // it compares the stored membership with the configured list
            // (`crate::auth::unkeyed_members`).
            debug!(
                target = self.target,
                addr = %self.addr,
                error = %refusal,
                "raft peer has no configured key — not dialling"
            );
            return Err(refusal.into());
        }
        let mut stream =
            tokio::time::timeout(CONNECT_TIMEOUT, connect(dial, &self.addr, &self.bind_ip))
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
        // This node's key, the protocol version both ways, then the
        // responder's key, under one bound: no envelope crosses, and no
        // reply is read, before all three have passed. The version comes
        // before the responder's challenge, so a peer on another release
        // is refused by version, both named, rather than by a frame it
        // does not know; the version frame carries no epoch and no tip,
        // and is acted on only by refusing the link.
        let handshake = async {
            answer_challenge(&mut stream, &self.signing_key).await?;
            match exchange_version(&mut stream).await {
                Ok(()) => {}
                Err(VersionExchangeError::Refused(refusal)) => {
                    // `warn!`, as on the responding side (see
                    // `rpc_server::handle_connection` for why): this end
                    // may be the only one that knows versions, if the
                    // peer runs a release from before them.
                    warn!(
                        target = self.target,
                        addr = %self.addr,
                        error = %refusal,
                        "raft peer runs another release — closing"
                    );
                    return Err(io::Error::from(refusal));
                }
                Err(VersionExchangeError::Io(e)) => return Err(e),
            }
            match challenge_responder(stream, &self.authorized_keys, &self.peer_ids, self.target)
                .await
            {
                Ok(link) => Ok(link),
                Err(AuthError::Refused(refusal)) => {
                    // `warn!`, as the responding side logs a dialer that
                    // fails authentication (`rpc_server::handle_connection`):
                    // misconfiguration (a stale key or swapped addresses
                    // in --raft-peer) or something else answering at the
                    // peer's address. Either is the peer's or the
                    // operator's doing, not this node's malfunction, but
                    // this node cannot reach that peer until it is fixed.
                    warn!(
                        target = self.target,
                        addr = %self.addr,
                        error = %refusal,
                        "raft peer failed authentication — closing"
                    );
                    Err(io::Error::from(refusal))
                }
                Err(AuthError::Io(e)) => Err(e),
                Err(e @ AuthError::Entropy(_)) => {
                    // This node's own malfunction: no peer can make it happen.
                    let e = io::Error::from(e);
                    error!(
                        target = self.target,
                        addr = %self.addr,
                        error = %e,
                        "raft peer auth: cannot challenge the responder"
                    );
                    Err(e)
                }
            }
        };
        let link = tokio::time::timeout(CONNECT_TIMEOUT, handshake)
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("handshake with {} timed out", self.addr),
                )
            })??;
        debug!(target = self.target, addr = %self.addr, "raft peer link established");
        Ok(self.stream.insert(link))
    }

    /// One serial request/response exchange. Any failure drops the
    /// connection so the next call starts fresh.
    async fn request(&mut self, body: RpcBody, ttl: Duration) -> io::Result<RpcBody> {
        // A clone of the `Arc`, not a borrow: `ensure_connected` borrows
        // all of `self` for as long as the link it returns is in use.
        let tip = Arc::clone(&self.tip);
        let result = tokio::time::timeout(ttl, async {
            let link = self.ensure_connected().await?;
            // Stamped after the connect, which may take a while, so the
            // envelope carries the tip as of sending.
            link.send(&RpcFrame::new(tip.stamp(), body)).await?;
            link.read_reply().await
        })
        .await
        .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "rpc timed out")));

        match result {
            Ok(frame) => {
                // A reply carries the responder's fencing epoch exactly as
                // a request carries the sender's, from the same
                // `TipSource`, so it is the same evidence of a newer
                // tenure. And a serving raft leader learns of a higher
                // epoch among its followers only here: a follower sends
                // its leader no request. Observed whatever the frame's
                // stamp: a frame older than one already heard carries an
                // epoch no higher than the newer one did (the fence epoch
                // never falls), so fencing on it is never wrong.
                let replier_tip = frame.stamped_tip();
                if let Some(policy) = &self.supersession {
                    policy.observe(replier_tip.tip.epoch);
                }
                // Every reply envelope carries the target's journal tip —
                // feed the promotion-time safety check (see `PeerTips`),
                // which drops it, and says so at `debug`, if a newer frame
                // from the target is already recorded. The reply goes to
                // raft either way: only its tip is stale.
                self.peer_tips.record(self.target, replier_tip);
                Ok(frame.body)
            }
            Err(e) => {
                // Tear down so the next attempt reconnects from scratch.
                self.stream = None;
                debug!(
                    target = self.target,
                    addr = %self.addr,
                    bind = %self.bind.ip(),
                    error = %e,
                    "raft peer rpc failed"
                );
                Err(e)
            }
        }
    }
}

/// Map a transport failure onto openraft's error taxonomy: failures *before*
/// a request could be sent mean the peer is unreachable (openraft backs off
/// harder); failures mid-exchange are transient network errors.
///
/// A [`DialRefusal`], a [`VersionRefusal`] (the peer runs another release),
/// a [`KeyRefusal`] (the responder did not prove the key configured for
/// it, or none is configured) and `AddrNotAvailable` (the bind IP is no
/// longer assigned) are failures before sending too, which no retry can
/// cure until the operator acts, and would otherwise be retried on every
/// heartbeat: openraft backs off only on `Unreachable`.
fn to_rpc_error<E: std::error::Error + 'static>(e: io::Error) -> RPCError<NodeId, Node, E> {
    let refused = e.get_ref().is_some_and(|inner| {
        inner.is::<DialRefusal>() || inner.is::<VersionRefusal>() || inner.is::<KeyRefusal>()
    });
    match e.kind() {
        io::ErrorKind::TimedOut
        | io::ErrorKind::ConnectionRefused
        | io::ErrorKind::NotFound
        | io::ErrorKind::AddrNotAvailable => RPCError::Unreachable(Unreachable::new(&e)),
        _ if refused => RPCError::Unreachable(Unreachable::new(&e)),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    fn refusal(bind: &str, peer: &str) -> DialRefusal {
        plan_dial(addr(bind), peer).unwrap_err()
    }

    #[test]
    fn a_specific_bind_dials_a_same_family_peer_from_its_ip_on_any_port() {
        assert_eq!(
            plan_dial(addr("10.0.0.5:7001"), "10.0.0.6:7002"),
            Ok(Dial::FromBind {
                local: addr("10.0.0.5:0"),
                peer: addr("10.0.0.6:7002"),
            })
        );
        assert_eq!(
            plan_dial(addr("[fd00::5]:7001"), "[fd00::6]:7002"),
            Ok(Dial::FromBind {
                local: addr("[fd00::5]:0"),
                peer: addr("[fd00::6]:7002"),
            })
        );
    }

    #[test]
    fn a_link_local_bind_keeps_its_scope_id() {
        let bind = SocketAddr::V6(std::net::SocketAddrV6::new(
            "fe80::5".parse().unwrap(),
            7001,
            0,
            3,
        ));
        let Ok(Dial::FromBind {
            local: SocketAddr::V6(local),
            ..
        }) = plan_dial(bind, "[fe80::6%3]:7002")
        else {
            panic!("a same-family peer is dialled from the bind");
        };
        assert_eq!((local.port(), local.scope_id()), (0, 3));
    }

    #[test]
    fn a_wildcard_bind_leaves_the_source_and_the_name_to_the_kernel() {
        for bind in ["0.0.0.0:7001", "[::]:7001"] {
            for peer in ["10.0.0.6:7002", "[fd00::6]:7002", "node-2.internal:7002"] {
                assert_eq!(
                    plan_dial(addr(bind), peer),
                    Ok(Dial::KernelSource),
                    "{bind} -> {peer}"
                );
            }
        }
    }

    #[test]
    fn a_specific_bind_refuses_a_peer_of_the_other_family() {
        let r = refusal("10.0.0.5:7001", "[fd00::6]:7002");
        assert_eq!(r.reason, DialRefusalReason::FamilyMismatch);
        let r = refusal("[fd00::5]:7001", "10.0.0.6:7002");
        assert_eq!(r.reason, DialRefusalReason::FamilyMismatch);
        assert_eq!(
            r.to_string(),
            "control plane bound to fd00::5 cannot dial peer 10.0.0.6:7002: the peer address \
             and the bind are of different address families"
        );
    }

    #[test]
    fn a_specific_bind_refuses_a_peer_that_is_not_an_ip_literal() {
        for peer in ["node-2.internal:7002", "localhost:7002", "10.0.0.6"] {
            let r = refusal("10.0.0.5:7001", peer);
            assert_eq!(r.reason, DialRefusalReason::NotIpLiteral, "{peer}");
            let message = r.to_string();
            assert!(
                message.contains("10.0.0.5") && message.contains(peer),
                "{message}"
            );
        }
    }

    #[test]
    fn undialable_members_skips_this_node_and_names_each_refused_peer() {
        let members = [
            (1, "node-1.internal:7001"),
            (2, "10.0.0.6:7002"),
            (3, "[fd00::7]:7003"),
            (4, "node-4.internal:7004"),
        ];
        let refused: Vec<(NodeId, DialRefusalReason)> =
            undialable_members(addr("10.0.0.5:7001"), 1, members)
                .map(|(id, refusal)| (id, refusal.reason))
                .collect();
        assert_eq!(
            refused,
            [
                (3, DialRefusalReason::FamilyMismatch),
                (4, DialRefusalReason::NotIpLiteral),
            ]
        );
        assert_eq!(
            undialable_members(addr("0.0.0.0:7001"), 1, members).count(),
            0,
            "a wildcard bind dials anything"
        );
    }

    fn is_unreachable(e: io::Error) -> bool {
        matches!(
            to_rpc_error::<RaftError<NodeId>>(e),
            RPCError::Unreachable(_)
        )
    }

    #[test]
    fn dial_refusals_and_an_unassigned_bind_ip_are_unreachable() {
        assert!(is_unreachable(
            refusal("10.0.0.5:7001", "[fd00::6]:7002").into()
        ));
        assert!(is_unreachable(
            refusal("10.0.0.5:7001", "node-2.internal:7002").into()
        ));
        assert!(is_unreachable(io::Error::from(
            io::ErrorKind::AddrNotAvailable
        )));
    }

    #[test]
    fn a_missing_bind_ip_is_reported_once_per_loss() {
        let bind_ip = BindIp::default();
        assert!(
            !bind_ip.regained(),
            "usable from the start: nothing regained"
        );
        assert!(bind_ip.lost(), "the first failure is the loss");
        assert!(!bind_ip.lost(), "retries during the same loss stay quiet");
        assert!(
            bind_ip.regained(),
            "the first success after it is the return"
        );
        assert!(!bind_ip.regained());
        assert!(bind_ip.lost(), "a second loss is reported again");
    }

    #[tokio::test]
    async fn dialling_from_an_unassigned_ip_marks_it_missing_until_a_bind_succeeds() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let bind_ip = BindIp::default();

        // 192.0.2.1 is TEST-NET-1, never assigned to a host; the bind fails
        // with `AddrNotAvailable` unless `net.ipv4.ip_nonlocal_bind` is set.
        let unassigned = Dial::FromBind {
            local: addr("192.0.2.1:0"),
            peer,
        };
        let e = connect(unassigned, &peer.to_string(), &bind_ip)
            .await
            .expect_err("192.0.2.1 is not assigned (is net.ipv4.ip_nonlocal_bind set?)");
        assert_eq!(e.kind(), io::ErrorKind::AddrNotAvailable, "{e}");
        assert!(is_unreachable(e));
        assert!(!bind_ip.lost(), "the failed bind recorded the loss");

        let assigned = Dial::FromBind {
            local: addr("127.0.0.1:0"),
            peer,
        };
        connect(assigned, &peer.to_string(), &bind_ip)
            .await
            .unwrap();
        assert!(
            !bind_ip.regained(),
            "the successful bind recorded the return"
        );
    }

    #[test]
    fn version_refusals_are_unreachable() {
        for refusal in [
            VersionRefusal::Mismatch { ours: 1, peer: 2 },
            VersionRefusal::WrongLength { ours: 1, len: 40 },
            VersionRefusal::WrongTag { ours: 1, tag: 0 },
        ] {
            assert!(is_unreachable(refusal.into()), "{refusal}");
        }
        // Their kind alone is not enough: other invalid data on the link
        // (a frame that fails to decode) stays a transient network error.
        assert!(!is_unreachable(io::Error::new(
            io::ErrorKind::InvalidData,
            "decode rpc frame: unexpected end"
        )));
    }

    #[test]
    fn key_refusals_are_unreachable() {
        for refusal in [
            KeyRefusal::Malformed("short".to_owned()),
            KeyRefusal::Unknown,
            KeyRefusal::WrongRole {
                listed_as: "operator",
            },
            KeyRefusal::InvalidKey("not a point".to_owned()),
            KeyRefusal::BadSignature("mismatch".to_owned()),
            KeyRefusal::NotThePeer {
                dialled: 1,
                belongs_to: Some(2),
            },
            KeyRefusal::NotThePeer {
                dialled: 1,
                belongs_to: None,
            },
            KeyRefusal::NoKeyConfigured { dialled: 3 },
        ] {
            let message = refusal.to_string();
            assert!(is_unreachable(refusal.into()), "{message}");
        }
        // Their kind alone is not enough.
        assert!(!is_unreachable(io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
    }

    #[test]
    fn other_invalid_input_stays_a_network_error() {
        // The refusals carry `InvalidInput`, but only the typed refusal
        // means "never reachable from here": the kind alone does not.
        assert!(!is_unreachable(io::Error::new(
            io::ErrorKind::InvalidInput,
            "some other invalid input"
        )));
        assert!(!is_unreachable(io::Error::from(
            io::ErrorKind::ConnectionReset
        )));
    }
}
