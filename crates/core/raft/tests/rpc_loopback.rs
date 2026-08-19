//! End-to-end RPC transport tests: a real `RaftClient` (via the
//! `RaftNetworkFactory` path) talking to a real `serve()` accept loop over
//! localhost TCP with real Ed25519 authentication — no raft core, the server
//! dispatches into a canned `RaftApi`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use base64::Engine;
use ed25519_dalek::SigningKey;
use melin_app::auth::AuthorizedKeys;
use melin_raft::network::RaftClientFactory;
use melin_raft::recency::TipSource;
use melin_raft::rpc_server::{RaftApi, RpcServerConfig, SupersessionPolicy, serve};
use melin_raft::types::{NodeId, TypeConfig};
use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;
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
    /// The key node 2 uses to authenticate (listed with Replication
    /// permission and mapped to peer id 2).
    client_key: SigningKey,
    /// Listed with Replication permission but NOT in the peer-id table.
    unlisted_peer_key: SigningKey,
    /// Listed with Operator permission.
    operator_key: SigningKey,
}

async fn start_server() -> Harness {
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
    let authorized_keys = Arc::new(AuthorizedKeys::parse(&table).unwrap());
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
        tip: tip_at(0, 0),
        peer_tips: Arc::new(melin_raft::recency::PeerTips::new()),
        supersession: None,
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

async fn client_for(h: &Harness, key: &SigningKey) -> impl RaftNetwork<TypeConfig> {
    let mut factory = RaftClientFactory::new(
        Arc::new(key.clone()),
        tip_at(0, 0),
        Arc::new(melin_raft::recency::PeerTips::new()),
    );
    factory
        .new_client(
            1,
            &BasicNode {
                addr: h.addr.clone(),
            },
        )
        .await
}

fn opt() -> RPCOption {
    RPCOption::new(Duration::from_secs(2))
}

/// A ready tip at (epoch, seq) — most tests want (0, 0) so nothing is
/// filtered; the recency tests raise it.
fn tip_at(epoch: u64, seq: u64) -> Arc<TipSource> {
    let fence = Arc::new(FenceState::new(0));
    fence.observe_epoch(epoch);
    let tip = AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(seq));
    Arc::new(TipSource {
        fence,
        seq: tip,
        ready: Arc::new(AtomicBool::new(true)),
    })
}

/// Vote request from node 2 (matches the pinned identity of its key).
fn vote_req_from(node: u64) -> VoteRequest<NodeId> {
    VoteRequest {
        vote: Vote::new(5, node),
        last_log_id: None,
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
    let append = AppendEntriesRequest {
        vote: Vote::new(5, 2),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    };
    let resp = client.append_entries(append, opt()).await.unwrap();
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
    melin_raft::auth::authenticate_outbound(&mut stream, &h.client_key)
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
    let client_key = SigningKey::from_bytes(&[0x11; 32]);
    let table = format!(
        "replication {} node-2\n",
        base64::engine::general_purpose::STANDARD.encode(client_key.verifying_key().to_bytes())
    );
    let authorized_keys = Arc::new(AuthorizedKeys::parse(&table).unwrap());
    let peer_ids = Arc::new(HashMap::from([(
        client_key.verifying_key().to_bytes(),
        2u64,
    )]));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let shutdown = Arc::new(AtomicBool::new(false));

    let ready = Arc::new(AtomicBool::new(false));
    let cfg = Arc::new(RpcServerConfig {
        authorized_keys,
        peer_ids,
        tip: Arc::new(TipSource {
            fence: Arc::new(FenceState::new(0)),
            seq: AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(0)),
            ready: Arc::clone(&ready),
        }),
        peer_tips: Arc::new(melin_raft::recency::PeerTips::new()),
        supersession: None,
        vote_filter: std::sync::Mutex::new(Default::default()),
    });
    tokio::spawn(serve(listener, MockApi, cfg, Arc::clone(&shutdown)));

    let mut factory = RaftClientFactory::new(
        Arc::new(client_key.clone()),
        tip_at(0, 0),
        Arc::new(melin_raft::recency::PeerTips::new()),
    );
    let node = BasicNode { addr };

    let mut client = factory.new_client(1, &node).await;
    client
        .vote(vote_req_from(2), opt())
        .await
        .expect_err("vote requests must be dropped while the local tip is unready");

    // Appends must never be gated — the dropped vote closed that
    // connection, so dial a fresh one.
    let mut client = factory.new_client(1, &node).await;
    let append = AppendEntriesRequest {
        vote: Vote::new(5, 2),
        prev_log_id: None,
        entries: vec![],
        leader_commit: None,
    };
    let resp = client.append_entries(append, opt()).await.unwrap();
    assert_eq!(resp, AppendEntriesResponse::Success);

    // Recovery finishes: the same server now delivers votes.
    ready.store(true, Ordering::Release);
    let resp = client.vote(vote_req_from(2), opt()).await.unwrap();
    assert!(resp.vote_granted, "a ready tip must restore vote delivery");

    shutdown.store(true, Ordering::Relaxed);
}

/// Fence-on-supersession over a real socket: a serving node that reads a
/// peer envelope advertising a strictly higher fencing epoch self-fences
/// and co-sets the shutdown flag — the raft mesh as a fencing channel.
#[tokio::test]
async fn serving_node_fences_on_higher_peer_epoch() {
    let client_key = SigningKey::from_bytes(&[0x11; 32]);
    let table = format!(
        "replication {} node-2\n",
        base64::engine::general_purpose::STANDARD.encode(client_key.verifying_key().to_bytes())
    );
    let authorized_keys = Arc::new(AuthorizedKeys::parse(&table).unwrap());
    let peer_ids = Arc::new(HashMap::from([(
        client_key.verifying_key().to_bytes(),
        2u64,
    )]));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let shutdown = Arc::new(AtomicBool::new(false));
    let process_shutdown = Arc::new(AtomicBool::new(false));

    // The server node serves at epoch 5.
    let fence = Arc::new(FenceState::new(5));
    let cfg = Arc::new(RpcServerConfig {
        authorized_keys,
        peer_ids,
        tip: Arc::new(TipSource {
            fence: Arc::clone(&fence),
            seq: AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(0)),
            ready: Arc::new(AtomicBool::new(true)),
        }),
        peer_tips: Arc::new(melin_raft::recency::PeerTips::new()),
        supersession: Some(SupersessionPolicy {
            fence: Arc::clone(&fence),
            shutdown: Arc::clone(&process_shutdown),
            serving: Arc::new(|| true),
        }),
        vote_filter: std::sync::Mutex::new(Default::default()),
    });
    tokio::spawn(serve(listener, MockApi, cfg, Arc::clone(&shutdown)));

    // A peer whose envelopes advertise epoch 7 — a newer tenure exists.
    let mut factory = RaftClientFactory::new(
        Arc::new(client_key.clone()),
        tip_at(7, 100),
        Arc::new(melin_raft::recency::PeerTips::new()),
    );
    let mut client = factory.new_client(1, &BasicNode { addr }).await;
    // The RPC itself may succeed or fail (the server may begin shutting
    // down mid-exchange) — the assertion is the fencing side effect.
    let _ = client.vote(vote_req_from(2), opt()).await;

    assert!(
        fence.is_fenced(),
        "server must self-fence on a higher peer epoch"
    );
    assert!(
        process_shutdown.load(Ordering::Relaxed),
        "supersession must co-set the process shutdown flag"
    );
    shutdown.store(true, Ordering::Relaxed);
}
