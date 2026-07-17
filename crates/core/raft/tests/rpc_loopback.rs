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
use melin_raft::rpc_server::{RaftApi, RpcServerConfig, serve};
use melin_raft::types::{NodeId, TypeConfig};
use melin_raft::wire::SharedTip;
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
        tip: SharedTip::new(),
    });
    cfg.tip.advance(9, 999);
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
    let mut factory = RaftClientFactory::new(Arc::new(key.clone()), SharedTip::new());
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
