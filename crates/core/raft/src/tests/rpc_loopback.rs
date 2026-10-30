//! End-to-end RPC transport tests: a real `RaftClient` (via the
//! `RaftNetworkFactory` path) talking to a real `serve()` accept loop over
//! localhost TCP with real Ed25519 authentication — no raft core, the server
//! dispatches into a canned `RaftApi`.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::network::RaftClientFactory;
use crate::recency::{PeerTips, TipSource};
use crate::rpc_server::{RaftApi, RpcServerConfig, SupersessionPolicy, serve};
use crate::tests::tip_at;
use crate::types::{NodeId, TypeConfig};
use base64::Engine;
use ed25519_dalek::SigningKey;
use melin_app::auth::{AuthorizedKeys, NoRoles};
use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;
use openraft::error::RPCError;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Vote};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

/// Server-side canned raft core.
#[derive(Clone)]
struct MockApi;

impl RaftApi for MockApi {
    async fn vote(&self, req: VoteRequest<NodeId>) -> Result<VoteResponse<NodeId>, String> {
        Ok(VoteResponse {
            vote: req.vote,
            vote_granted: true,
            last_log_id: req.last_log_id,
        })
    }
    async fn append_entries(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, String> {
        let _ = req;
        Ok(AppendEntriesResponse::Success)
    }
    async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, String> {
        Ok(InstallSnapshotResponse { vote: req.vote })
    }
}

struct Harness {
    addr: String,
    shutdown: Arc<AtomicBool>,
    /// The key node 2 uses to authenticate (listed under the
    /// `replication` role and mapped to peer id 2).
    client_key: SigningKey,
    /// Listed under the `replication` role but NOT in the peer-id table.
    unlisted_peer_key: SigningKey,
    /// Listed under the `operator` role.
    operator_key: SigningKey,
}

/// A server advertising (0, 0), with no supersession policy.
async fn start_server() -> Harness {
    start_server_with(tip_at(0, 0), None).await
}

/// A server advertising `tip`, under `supersession`.
async fn start_server_with(
    tip: Arc<TipSource>,
    supersession: Option<Arc<SupersessionPolicy>>,
) -> Harness {
    let client_key = SigningKey::from_bytes(&[0x11; 32]);
    let unlisted_peer_key = SigningKey::from_bytes(&[0x22; 32]);
    let operator_key = SigningKey::from_bytes(&[0x33; 32]);

    let b64 = |k: &SigningKey| {
        base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
    };
    let table = format!(
        "replication {} node-2\nreplication {} not-a-peer\noperator {} ops\n",
        b64(&client_key),
        b64(&unlisted_peer_key),
        b64(&operator_key),
    );
    let authorized_keys = Arc::new(AuthorizedKeys::parse::<NoRoles>(&table).unwrap());
    let peer_ids = Arc::new(HashMap::from([(
        client_key.verifying_key().to_bytes(),
        2u64,
    )]));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let shutdown = Arc::new(AtomicBool::new(false));
    let cfg = Arc::new(RpcServerConfig {
        authorized_keys,
        peer_ids,
        tip,
        peer_tips: Arc::new(PeerTips::new()),
        supersession,
        vote_filter: std::sync::Mutex::new(Default::default()),
    });
    tokio::spawn(serve(listener, MockApi, cfg, Arc::clone(&shutdown)));

    Harness {
        addr,
        shutdown,
        client_key,
        unlisted_peer_key,
        operator_key,
    }
}

/// A client factory for a node authenticating with `key`, advertising
/// `tip`, whose raft listener is on `bind`, under `supersession`.
fn client_factory(
    key: &SigningKey,
    tip: Arc<TipSource>,
    bind: SocketAddr,
    supersession: Option<Arc<SupersessionPolicy>>,
) -> RaftClientFactory {
    RaftClientFactory::new(
        Arc::new(key.clone()),
        tip,
        Arc::new(PeerTips::new()),
        bind,
        supersession,
    )
}

async fn client_for(h: &Harness, key: &SigningKey) -> impl RaftNetwork<TypeConfig> {
    client_factory(key, tip_at(0, 0), WILDCARD_BIND, None)
        .new_client(
            1,
            &BasicNode {
                addr: h.addr.clone(),
            },
        )
        .await
}

/// A client node whose raft listener is on a wildcard: it dials as the
/// kernel routes, from no particular source address.
const WILDCARD_BIND: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 7001);

fn opt() -> RPCOption {
    RPCOption::new(Duration::from_secs(2))
}

/// Vote request from node 2 (matches the pinned identity of its key).
fn vote_req_from(node: u64) -> VoteRequest<NodeId> {
    VoteRequest {
        vote: Vote::new(5, node),
        last_log_id: None,
    }
}

/// An empty append (a heartbeat) from `node`'s leadership.
fn append_from(node: u64) -> AppendEntriesRequest<TypeConfig> {
    AppendEntriesRequest {
        vote: Vote::new(5, node),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    }
}

#[tokio::test]
async fn vote_round_trips_with_real_auth() {
    let h = start_server().await;
    let mut client = client_for(&h, &h.client_key).await;
    let resp = client.vote(vote_req_from(2), opt()).await.unwrap();
    assert!(resp.vote_granted);
    assert_eq!(resp.vote, Vote::new(5, 2));

    // Same connection serves a second RPC (append), proving the link is
    // reused, serial, and still framed correctly.
    let resp = client.append_entries(append_from(2), opt()).await.unwrap();
    assert_eq!(resp, AppendEntriesResponse::Success);
    h.shutdown.store(true, Ordering::Relaxed);
}

#[tokio::test]
async fn wrong_key_is_refused() {
    let h = start_server().await;
    // A key the server has never heard of.
    let mut client = client_for(&h, &SigningKey::from_bytes(&[0x77; 32])).await;
    client
        .vote(vote_req_from(2), opt())
        .await
        .expect_err("unknown key must not authenticate");
    h.shutdown.store(true, Ordering::Relaxed);
}

#[tokio::test]
async fn operator_permission_is_refused() {
    let h = start_server().await;
    let key = h.operator_key.clone();
    let mut client = client_for(&h, &key).await;
    client
        .vote(vote_req_from(2), opt())
        .await
        .expect_err("operator keys must not join the control plane");
    h.shutdown.store(true, Ordering::Relaxed);
}

#[tokio::test]
async fn replication_key_not_in_peer_table_is_refused() {
    let h = start_server().await;
    let key = h.unlisted_peer_key.clone();
    let mut client = client_for(&h, &key).await;
    client
        .vote(vote_req_from(2), opt())
        .await
        .expect_err("a replication key that is not a configured raft peer must be refused");
    h.shutdown.store(true, Ordering::Relaxed);
}

#[tokio::test]
async fn identity_pinning_rejects_mismatched_sender() {
    let h = start_server().await;
    let mut client = client_for(&h, &h.client_key).await;
    // The key is pinned to node 2 but the vote claims to be node 3.
    client
        .vote(vote_req_from(3), opt())
        .await
        .expect_err("a request claiming another node id must be rejected");
    h.shutdown.store(true, Ordering::Relaxed);
}

#[tokio::test]
async fn oversized_frame_is_refused() {
    let h = start_server().await;
    // Authenticate a raw connection, then send a pathological length prefix.
    let mut stream = tokio::net::TcpStream::connect(&h.addr).await.unwrap();
    crate::auth::authenticate_outbound(&mut stream, &h.client_key)
        .await
        .unwrap();
    stream.write_all(&u32::MAX.to_le_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    // The server must drop the connection rather than allocate.
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(Duration::from_secs(2), async {
        use tokio::io::AsyncReadExt;
        stream.read(&mut buf).await
    })
    .await
    .expect("server should close, not hang")
    .unwrap_or(0);
    assert_eq!(n, 0, "expected EOF after oversized frame");
    h.shutdown.store(true, Ordering::Relaxed);
}

/// Tip-readiness gate over a real socket: until journal recovery seeds
/// the local tip (`TipSource::ready`), the server must drop every vote
/// request — a vote judged against a default epoch/sequence could admit
/// a candidate behind data this node actually holds. Dropping closes the
/// connection, exactly like the recency filter. Appends are never gated
/// (a legitimately elected leader must still lead), and flipping `ready`
/// restores voting without a reconnect ceremony.
#[tokio::test]
async fn votes_are_dropped_until_the_tip_is_ready() {
    let ready = Arc::new(AtomicBool::new(false));
    let h = start_server_with(
        Arc::new(TipSource {
            fence: Arc::new(FenceState::new(0)),
            seq: AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(0)),
            ready: Arc::clone(&ready),
        }),
        None,
    )
    .await;

    let mut factory = client_factory(&h.client_key, tip_at(0, 0), WILDCARD_BIND, None);
    let node = BasicNode {
        addr: h.addr.clone(),
    };

    let mut client = factory.new_client(1, &node).await;
    client
        .vote(vote_req_from(2), opt())
        .await
        .expect_err("vote requests must be dropped while the local tip is unready");

    // Appends must never be gated — the dropped vote closed that
    // connection, so dial a fresh one.
    let mut client = factory.new_client(1, &node).await;
    let resp = client.append_entries(append_from(2), opt()).await.unwrap();
    assert_eq!(resp, AppendEntriesResponse::Success);

    // Recovery finishes: the same server now delivers votes.
    ready.store(true, Ordering::Release);
    let resp = client.vote(vote_req_from(2), opt()).await.unwrap();
    assert!(resp.vote_granted, "a ready tip must restore vote delivery");

    h.shutdown.store(true, Ordering::Relaxed);
}

/// Fence-on-supersession over a real socket: a serving node that reads a
/// peer envelope advertising a strictly higher fencing epoch self-fences
/// and co-sets the shutdown flag — the raft mesh as a fencing channel.
#[tokio::test]
async fn serving_node_fences_on_higher_peer_epoch() {
    // The server node serves at epoch 5.
    let tip = tip_at(5, 0);
    let (policy, process_shutdown) = supersession_over(&tip, true);
    let h = start_server_with(Arc::clone(&tip), Some(policy)).await;

    // A peer whose envelopes advertise epoch 7 — a newer tenure exists.
    let mut client = client_factory(&h.client_key, tip_at(7, 100), WILDCARD_BIND, None)
        .new_client(
            1,
            &BasicNode {
                addr: h.addr.clone(),
            },
        )
        .await;
    // The RPC's outcome is not the point (the server may begin shutting
    // down mid-exchange): the assertion is the fencing side effect.
    let _ = client.vote(vote_req_from(2), opt()).await;

    assert!(
        tip.fence.is_fenced(),
        "server must self-fence on a higher peer epoch"
    );
    assert!(
        process_shutdown.load(Ordering::Relaxed),
        "supersession must co-set the process shutdown flag"
    );
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A supersession policy over `tip`'s fence, whose node claims to be
/// serving or not as `serving` says, with a process shutdown flag of its
/// own (returned beside it).
fn supersession_over(tip: &TipSource, serving: bool) -> (Arc<SupersessionPolicy>, Arc<AtomicBool>) {
    let process_shutdown = Arc::new(AtomicBool::new(false));
    let policy = Arc::new(SupersessionPolicy {
        fence: Arc::clone(&tip.fence),
        shutdown: Arc::clone(&process_shutdown),
        serving: Arc::new(move || serving),
    });
    (policy, process_shutdown)
}

/// The client node's epoch in the reply-path tests.
const CLIENT_EPOCH: u64 = 5;

/// A client node at [`CLIENT_EPOCH`], claiming to be serving or not as
/// `serving` says, sends one heartbeat to a server advertising
/// `server_epoch`, and gets its reply. Returns whether the client then
/// is fenced, and whether its process shutdown flag is set.
async fn client_after_a_reply_at(server_epoch: u64, serving: bool) -> (bool, bool) {
    let h = start_server_with(tip_at(server_epoch, 0), None).await;
    let tip = tip_at(CLIENT_EPOCH, 0);
    let (policy, process_shutdown) = supersession_over(&tip, serving);
    let mut client = client_factory(&h.client_key, Arc::clone(&tip), WILDCARD_BIND, Some(policy))
        .new_client(
            1,
            &BasicNode {
                addr: h.addr.clone(),
            },
        )
        .await;
    let resp = client
        .append_entries(append_from(2), opt())
        .await
        .expect("the server answers the heartbeat");
    assert_eq!(resp, AppendEntriesResponse::Success);
    h.shutdown.store(true, Ordering::Relaxed);
    (
        tip.fence.is_fenced(),
        process_shutdown.load(Ordering::Relaxed),
    )
}

/// The reply path (`docs/internal/raft-control-plane.md`, "Auto-promotion
/// policy"): a serving node whose request is answered by a peer at a
/// strictly higher epoch fences and co-sets its shutdown flag, as it would
/// on a request from that peer. A serving raft leader hears its followers
/// only in replies.
#[tokio::test]
async fn a_serving_client_fences_on_a_reply_at_a_higher_epoch() {
    assert_eq!(
        client_after_a_reply_at(CLIENT_EPOCH + 1, true).await,
        (true, true)
    );
}

/// A node that does not claim to be serving (a replica with no promotion
/// filed) has nothing to step down from: a higher epoch in a reply is a
/// newer primary it may yet follow.
#[tokio::test]
async fn a_client_not_serving_ignores_a_reply_at_a_higher_epoch() {
    assert_eq!(
        client_after_a_reply_at(CLIENT_EPOCH + 1, false).await,
        (false, false)
    );
}

/// A reply from the same tenure, or an older one, supersedes nobody.
#[tokio::test]
async fn a_reply_at_or_below_the_clients_epoch_does_not_fence() {
    for server_epoch in [CLIENT_EPOCH, CLIENT_EPOCH - 1] {
        assert_eq!(
            client_after_a_reply_at(server_epoch, true).await,
            (false, false),
            "a reply at epoch {server_epoch}"
        );
    }
}

/// A client for `peer` on a node whose raft listener is on `bind`.
async fn client_on(bind: SocketAddr, peer: &str) -> impl RaftNetwork<TypeConfig> {
    client_factory(
        &SigningKey::from_bytes(&[0x11; 32]),
        tip_at(0, 0),
        bind,
        None,
    )
    .new_client(
        1,
        &BasicNode {
            addr: peer.to_owned(),
        },
    )
    .await
}

/// The source address a peer listening on 127.0.0.3 sees when a node
/// whose raft listener is on `bind` dials it. The peer speaks no
/// protocol: it accepts, notes the source, and hangs up, so the vote
/// itself fails. A dial that never connects fails the test with the
/// vote's error rather than leaving the accept waiting forever.
async fn source_seen_by_peer(bind: SocketAddr) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.3:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut client = client_on(bind, &addr).await;
    let vote = client.vote(vote_req_from(2), opt());
    tokio::pin!(vote);
    let from = tokio::select! {
        accepted = listener.accept() => {
            let (stream, from) = accepted.unwrap();
            drop(stream);
            from
        }
        result = &mut vote => panic!("the dial never reached the peer: {result:?}"),
    };
    vote.await
        .expect_err("the peer hung up before authenticating");
    from
}

/// Linux routes all of 127.0.0.0/8 to `lo`, so 127.0.0.2 and 127.0.0.3
/// need no setup.
#[tokio::test]
async fn a_node_bound_to_a_specific_ip_dials_from_it() {
    // The node's own raft listener holds its port: the dialer must bind
    // the IP alone, on an ephemeral port.
    let own_listener = TcpListener::bind("127.0.0.2:0").await.unwrap();
    let bind = own_listener.local_addr().unwrap();
    let from = source_seen_by_peer(bind).await;
    assert_eq!(from.ip(), bind.ip());
    assert_ne!(from.port(), bind.port());
}

#[tokio::test]
async fn a_node_bound_to_a_wildcard_dials_from_a_local_ip() {
    let from = source_seen_by_peer(WILDCARD_BIND).await;
    assert!(from.ip().is_loopback(), "{from}");
}

/// Vote from a node whose raft listener is on `bind` to `peer`, which
/// must fail as an unreachable peer with every one of `parts` in the
/// message.
async fn assert_unreachable_naming(bind: &str, peer: &str, parts: &[&str]) {
    let err = client_on(bind.parse().unwrap(), peer)
        .await
        .vote(vote_req_from(2), opt())
        .await
        .unwrap_err();
    let RPCError::Unreachable(unreachable) = err else {
        panic!("expected Unreachable, got {err:?}");
    };
    let message = unreachable.to_string();
    for part in parts {
        assert!(message.contains(part), "{part:?} not in {message:?}");
    }
}

/// A bind IP the host does not hold (192.0.2.0/24 and 2001:db8::/32 are
/// reserved for documentation) fails the bind with `AddrNotAvailable`,
/// which openraft must treat as an unreachable peer and back off from.
/// The IPv6 case needs IPv6 sockets, not any IPv6 address on the host.
/// Both cases assume the kernel default `net.ipv{4,6}.ip_nonlocal_bind=0`:
/// a host that allows non-local binds (some keepalived setups) lets the
/// bind succeed and the connect then fails or times out instead.
#[tokio::test]
async fn a_bind_ip_the_host_does_not_hold_makes_the_peer_unreachable() {
    assert_unreachable_naming(
        "192.0.2.1:7001",
        "192.0.2.2:7002",
        &["192.0.2.1:0", "192.0.2.2:7002"],
    )
    .await;
    assert_unreachable_naming(
        "[2001:db8::1]:7001",
        "[2001:db8::2]:7002",
        &["[2001:db8::1]:0", "[2001:db8::2]:7002"],
    )
    .await;
}

/// A stored peer address the bind can never dial is refused before any
/// syscall, as an unreachable peer, naming the bind and the peer.
#[tokio::test]
async fn a_peer_the_bind_cannot_dial_is_unreachable() {
    for peer in ["[::1]:7002", "localhost:7002"] {
        assert_unreachable_naming("127.0.0.2:7001", peer, &["127.0.0.2", peer]).await;
    }
}
