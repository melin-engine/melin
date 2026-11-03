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

use crate::auth::{
    AuthError, AuthenticatedResponder, KeyRefusal, answer_challenge, challenge_peer,
    challenge_responder,
};
use crate::network::{CONNECT_TIMEOUT, RaftClientFactory};
use crate::recency::{JournalTip, PeerTips, StampedTip, TipSource};
use crate::rpc_server::{AUTH_TIMEOUT, RaftApi, RpcServerConfig, SupersessionPolicy, serve};
use crate::tests::authorized_keys_table;
use crate::tests::pre_version_wire;
use crate::tests::{stamped_at, tip_at, tip_source};
use crate::types::{NodeId, TypeConfig};
use crate::wire::{
    RAFT_PROTOCOL_VERSION, RpcBody, RpcFrame, VERSION_FRAME_WIRE_LEN, VersionExchangeError,
    VersionRefusal, encode_version_frame, exchange_version, exchange_version_as, read_frame,
    write_frame,
};
use ed25519_dalek::SigningKey;
use melin_app::auth::AuthorizedKeys;
use melin_transport_core::cursors::AdvertisedJournalTip;
use melin_transport_core::fence::FenceState;
use melin_transport_core::test_logs::{Logs, level};
use openraft::error::{RPCError, RaftError};
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
    /// The tips the server records off the requests it reads.
    peer_tips: Arc<PeerTips>,
}

/// A server advertising (0, 0), with no supersession policy.
async fn start_server() -> Harness {
    start_server_with(tip_at(0, 0), None).await
}

/// The key the server, node 1, proves to its dialers: listed under the
/// `replication` role and configured as node 1.
fn server_key() -> SigningKey {
    SigningKey::from_bytes(&[0x44; 32])
}

/// The key node 2 authenticates with (see [`Harness::client_key`]).
fn client_key() -> SigningKey {
    SigningKey::from_bytes(&[0x11; 32])
}

/// See [`Harness::unlisted_peer_key`].
fn unlisted_peer_key() -> SigningKey {
    SigningKey::from_bytes(&[0x22; 32])
}

/// See [`Harness::operator_key`].
fn operator_key() -> SigningKey {
    SigningKey::from_bytes(&[0x33; 32])
}

/// The harness's authorized-keys table: [`server_key`], [`client_key`]
/// and [`unlisted_peer_key`] under the `replication` role,
/// [`operator_key`] under the `operator` role.
fn authorized_keys() -> Arc<AuthorizedKeys> {
    Arc::new(authorized_keys_table(&[
        (&server_key(), "replication", "node-1"),
        (&client_key(), "replication", "node-2"),
        (&unlisted_peer_key(), "replication", "not-a-peer"),
        (&operator_key(), "operator", "ops"),
    ]))
}

/// The harness's `--raft-peer` table, as both ends hold it: [`server_key`]
/// is node 1, [`client_key`] node 2.
fn peer_ids() -> Arc<HashMap<[u8; 32], NodeId>> {
    Arc::new(HashMap::from([
        (server_key().verifying_key().to_bytes(), 1),
        (client_key().verifying_key().to_bytes(), 2),
    ]))
}

/// A server advertising `tip`, under `supersession`.
async fn start_server_with(
    tip: Arc<TipSource>,
    supersession: Option<Arc<SupersessionPolicy>>,
) -> Harness {
    start_server_signing(server_key(), tip, supersession).await
}

/// A server proving `signing_key` to its dialers, advertising `tip`, under
/// `supersession`. Its own tables are the harness's whatever the key, so
/// a dialer's authentication passes and only the dialer's check of this
/// key decides.
async fn start_server_signing(
    signing_key: SigningKey,
    tip: Arc<TipSource>,
    supersession: Option<Arc<SupersessionPolicy>>,
) -> Harness {
    let client_key = client_key();
    let unlisted_peer_key = unlisted_peer_key();
    let operator_key = operator_key();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let shutdown = Arc::new(AtomicBool::new(false));
    let peer_tips = Arc::new(PeerTips::new());
    let cfg = Arc::new(RpcServerConfig {
        signing_key: Arc::new(signing_key),
        authorized_keys: authorized_keys(),
        peer_ids: peer_ids(),
        tip,
        peer_tips: Arc::clone(&peer_tips),
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
        peer_tips,
    }
}

/// A client factory for a node authenticating with `key`, advertising
/// `tip`, whose raft listener is on `bind`, under `supersession`, holding
/// the harness's tables.
fn client_factory(
    key: &SigningKey,
    tip: Arc<TipSource>,
    bind: SocketAddr,
    supersession: Option<Arc<SupersessionPolicy>>,
) -> RaftClientFactory {
    client_factory_recording(key, tip, bind, supersession, Arc::new(PeerTips::new()))
}

/// [`client_factory`], recording the tips its replies carry in
/// `peer_tips`.
fn client_factory_recording(
    key: &SigningKey,
    tip: Arc<TipSource>,
    bind: SocketAddr,
    supersession: Option<Arc<SupersessionPolicy>>,
    peer_tips: Arc<PeerTips>,
) -> RaftClientFactory {
    RaftClientFactory::new(
        Arc::new(key.clone()),
        authorized_keys(),
        peer_ids(),
        tip,
        peer_tips,
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
    // Through the whole handshake on a raw connection, so the oversized
    // prefix lands in the envelope reader rather than being refused as a
    // wrong-length version or challenge response.
    let mut link = handshaken_link(&h).await;
    let stream = link.get_mut();
    stream.write_all(&u32::MAX.to_le_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    // The server must drop the connection rather than allocate.
    assert_closed_by_peer(stream).await;
    h.shutdown.store(true, Ordering::Relaxed);
}

/// The other end closes `stream` (EOF or a reset) without sending anything
/// more, within a bound.
async fn assert_closed_by_peer(stream: &mut tokio::net::TcpStream) {
    assert_closed_by_peer_within(stream, Duration::from_secs(2)).await;
}

/// [`assert_closed_by_peer`], within `bound`.
async fn assert_closed_by_peer_within(stream: &mut tokio::net::TcpStream, bound: Duration) {
    let mut buf = [0u8; 1];
    let n = tokio::time::timeout(bound, async {
        use tokio::io::AsyncReadExt;
        stream.read(&mut buf).await
    })
    .await
    .expect("the peer should close, not hang")
    // A reset is a close too.
    .unwrap_or(0);
    assert_eq!(n, 0, "expected the peer to close the link");
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
        tip_source(
            Arc::new(FenceState::new(0)),
            AdvertisedJournalTip::new(melin_transport_core::WireSeq::new(0)),
            Arc::clone(&ready),
        ),
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
    let after = client_after_a_heartbeat_to(server_key(), server_epoch, serving).await;
    let resp = after.outcome.expect("the server answers the heartbeat");
    assert_eq!(resp, AppendEntriesResponse::Success);
    assert_eq!(
        after.recorded,
        Some(JournalTip {
            epoch: server_epoch,
            last_sequence: 0
        }),
        "an accepted reply records the server's tip"
    );
    (after.fenced, after.shut_down)
}

/// What a client node saw after one heartbeat to node 1.
struct AfterHeartbeat {
    outcome: Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>>,
    /// Whether the client is fenced, and its process shutdown flag set.
    fenced: bool,
    shut_down: bool,
    /// The tip the client recorded for node 1, if any.
    recorded: Option<JournalTip>,
}

/// A client node at [`CLIENT_EPOCH`], claiming to be serving or not as
/// `serving` says, sends one heartbeat to node 1's address, where a
/// server proving `server_key` advertises `server_epoch`.
async fn client_after_a_heartbeat_to(
    server_key: SigningKey,
    server_epoch: u64,
    serving: bool,
) -> AfterHeartbeat {
    let h = start_server_signing(server_key, tip_at(server_epoch, 0), None).await;
    let after = heartbeat_to_node_1_at(&h.addr, serving).await;
    h.shutdown.store(true, Ordering::Relaxed);
    after
}

/// A client node at [`CLIENT_EPOCH`], claiming to be serving or not as
/// `serving` says, sends one heartbeat to node 1 at `addr`, whatever
/// answers there.
async fn heartbeat_to_node_1_at(addr: &str, serving: bool) -> AfterHeartbeat {
    let tip = tip_at(CLIENT_EPOCH, 0);
    let (policy, process_shutdown) = supersession_over(&tip, serving);
    let peer_tips = Arc::new(PeerTips::new());
    let mut client = client_factory_recording(
        &client_key(),
        Arc::clone(&tip),
        WILDCARD_BIND,
        Some(policy),
        Arc::clone(&peer_tips),
    )
    .new_client(
        1,
        &BasicNode {
            addr: addr.to_owned(),
        },
    )
    .await;
    let outcome = client.append_entries(append_from(2), opt()).await;
    AfterHeartbeat {
        outcome,
        fenced: tip.fence.is_fenced(),
        shut_down: process_shutdown.load(Ordering::Relaxed),
        recorded: peer_tips
            .sample(1, std::time::Instant::now())
            .map(|(tip, _age)| tip),
    }
}

/// The reply path (`docs/internal/raft-control-plane.md`, the
/// fence-on-supersession paragraph): a serving node whose request is
/// answered by a peer at a strictly higher epoch fences and co-sets its
/// shutdown flag, as it would on a request from that peer. A serving raft
/// leader hears its followers only in replies.
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

/// The message of an `Unreachable` error, failing on any other.
fn unreachable_message<E: std::error::Error>(err: RPCError<NodeId, BasicNode, E>) -> String {
    let RPCError::Unreachable(unreachable) = err else {
        panic!("expected Unreachable, got {err:?}");
    };
    unreachable.to_string()
}

/// Nothing a refused responder sent was acted on: a serving dialer facing
/// a higher epoch neither fenced nor stepped down, and recorded no tip.
fn assert_nothing_read(after: &AfterHeartbeat) {
    assert!(!after.fenced, "a refused responder's epoch must not fence");
    assert!(!after.shut_down);
    assert_eq!(
        after.recorded, None,
        "a refused responder's tip is not recorded"
    );
}

/// The one line the dialer logs on refusing a responder's key: a warning
/// (misconfiguration or impersonation, needing the operator, never a
/// malfunction of this node, so never `error!`) carrying `part`. Nothing
/// on the link logged at `error!`.
fn assert_responder_refusal_logged(logs: &Logs, part: &str) {
    let lines = logs.lines("raft peer failed authentication — closing");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(level(&lines[0]), tracing::Level::WARN, "{}", lines[0]);
    assert!(lines[0].contains(part), "{part:?} not in {:?}", lines[0]);
    let errors: Vec<String> = logs
        .lines("")
        .into_iter()
        .filter(|line| level(line) == tracing::Level::ERROR)
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
}

/// The dialer refuses a responder at node 1's address that proves
/// `server_key`, as an unreachable peer (no retry cures it until the
/// operator acts), with `part` in the message, and logs it once at `warn`.
/// The refusal comes before any request is sent, so no reply is read: a
/// serving dialer facing a higher epoch neither fences nor records a tip.
/// The server only logs, at `debug`, that its key was not accepted: the
/// dialer is the side that knows why.
async fn assert_dialer_refuses_responder_key(server_key: SigningKey, part: &str) {
    let (logs, _guard) = capture_logs();
    let after = client_after_a_heartbeat_to(server_key, CLIENT_EPOCH + 1, true).await;
    assert_nothing_read(&after);
    let message = unreachable_message(after.outcome.expect_err("the responder must be refused"));
    assert!(message.contains(part), "{part:?} not in {message:?}");
    assert_responder_refusal_logged(&logs, part);
    // The server's side, which runs on this thread: it may still be
    // reading the dialer's `AuthFailed` when the dialer returns.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    let server_side = loop {
        let lines = logs.lines("raft peer did not accept this node's key");
        if !lines.is_empty() || tokio::time::Instant::now() >= deadline {
            break lines;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(server_side.len(), 1, "{server_side:?}");
    assert_eq!(level(&server_side[0]), tracing::Level::DEBUG);
}

/// A key `authorized_keys` does not list. (A responder proving the key
/// configured for the node dialled passes: every reply-path test above
/// runs against one, and checks its tip is recorded.)
#[tokio::test]
async fn the_dialer_refuses_a_responder_with_an_unknown_key() {
    assert_dialer_refuses_responder_key(
        SigningKey::from_bytes(&[0x77; 32]),
        "unknown control-plane key",
    )
    .await;
}

#[tokio::test]
async fn the_dialer_refuses_a_responder_with_a_key_of_another_role() {
    assert_dialer_refuses_responder_key(
        operator_key(),
        "key listed as operator, expected replication",
    )
    .await;
}

/// Node 2's key at node 1's address (swapped addresses, or node 2
/// answering for node 1): refused, naming both.
#[tokio::test]
async fn the_dialer_refuses_a_responder_with_another_configured_peers_key() {
    assert_dialer_refuses_responder_key(
        client_key(),
        "dialled as node 1 proved the key configured for node 2",
    )
    .await;
}

/// A key in the replication trust domain that is no raft peer's.
#[tokio::test]
async fn the_dialer_refuses_a_responder_with_a_replication_key_no_peer_has() {
    assert_dialer_refuses_responder_key(unlisted_peer_key(), "no --raft-peer entry names").await;
}

/// Whatever answers at node 1's address and, after the version exchange,
/// pushes a well-formed reply envelope at a higher epoch instead of proving
/// a key: the dialer reads those bytes as the challenge response it was
/// waiting for, refuses them as malformed, and never decodes them as a
/// reply. A serving dialer neither fences nor records the forged tip.
#[tokio::test]
async fn a_forged_reply_in_place_of_the_responders_proof_is_refused_unread() {
    let (logs, _guard) = capture_logs();
    let (addr, responder) = responder_then(|mut stream| async move {
        exchange_version(&mut stream).await.unwrap();
        write_frame(
            &mut stream,
            &RpcFrame::new(
                stamped_at(CLIENT_EPOCH + 1, 1_000, 0),
                RpcBody::AppendResp(AppendEntriesResponse::Success),
            ),
        )
        .await
        .unwrap();
        // Hold the link until the dialer hangs up, so its refusal is what
        // ends the exchange, not this end's close.
        use tokio::io::AsyncReadExt;
        let mut rest = Vec::new();
        stream.read_to_end(&mut rest).await.map(|_| rest)
    })
    .await;
    let after = heartbeat_to_node_1_at(&addr, true).await;
    assert_nothing_read(&after);
    let message = unreachable_message(after.outcome.expect_err("a forged reply must be refused"));
    assert!(message.contains("bad challenge response"), "{message}");
    assert_responder_refusal_logged(&logs, "bad challenge response");
    // What the dialer sent after the version frame: its challenge, then
    // its verdict, `AuthFailed`, and no request.
    let rest = responder.await.unwrap().unwrap();
    let mut auth_failed = Vec::new();
    melin_transport_core::replication::protocol::encode_auth_failed(&mut auth_failed);
    let challenge_len = rest.len() - auth_failed.len();
    assert!(rest.ends_with(&auth_failed), "{rest:?}");
    let mut challenge = &rest[..challenge_len];
    crate::wire::read_bounded_payload(&mut challenge, 1024, "challenge")
        .await
        .expect("a single challenge frame before the verdict");
    assert!(challenge.is_empty(), "nothing else was sent: {challenge:?}");
}

/// The server's side of a dialer refusing its key: told so (`AuthFailed`),
/// it closes the link rather than waiting for an envelope.
#[tokio::test]
async fn the_server_closes_a_link_whose_dialer_refuses_its_key() {
    let h = start_server().await;
    let mut stream = authenticated_link(&h).await;
    exchange_version(&mut stream).await.unwrap();
    // Dialled as node 2, so the server's node-1 key is the wrong one.
    match challenge_responder(&mut stream, &authorized_keys(), &peer_ids(), 2).await {
        Err(AuthError::Refused(refusal)) => assert_eq!(
            refusal,
            KeyRefusal::NotThePeer {
                dialled: 2,
                belongs_to: Some(1)
            }
        ),
        Err(other) => panic!("expected the dialer to refuse, got {other:?}"),
        Ok(_) => panic!("expected the dialer to refuse, got an accepted key"),
    }
    assert_closed_by_peer(&mut stream).await;
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A dialer that hangs up before answering the server's challenge is a
/// closed link, logged at `debug` (it proved no key, right or wrong): the
/// server closes its end too.
#[tokio::test]
async fn the_server_closes_a_dialer_that_hangs_up_mid_challenge() {
    let (logs, _guard) = capture_logs();
    let h = start_server().await;
    let mut stream = tokio::net::TcpStream::connect(&h.addr).await.unwrap();
    crate::wire::read_bounded_payload(&mut stream, 1024, "challenge")
        .await
        .unwrap();
    stream.shutdown().await.unwrap();
    assert_closed_by_peer(&mut stream).await;
    // A closed link, not a refusal: `debug!`, never `warn!`.
    let lines = logs.lines("raft peer link closed during authentication");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(level(&lines[0]), tracing::Level::DEBUG);
    assert!(logs.lines("raft peer failed authentication").is_empty());
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A dialer that passes the version exchange and then never challenges the
/// server is dropped at the bound on that step, logged at `debug!` (a
/// stalled link): the server's proof of its own key sits under the same
/// bound as the steps before it.
#[tokio::test]
async fn a_dialer_silent_after_the_version_is_dropped_at_the_challenge_bound() {
    let (logs, _guard) = capture_logs();
    let h = start_server().await;
    let mut stream = authenticated_link(&h).await;
    exchange_version(&mut stream).await.unwrap();
    // The server now waits for this dialer's challenge, which never comes.
    let started = tokio::time::Instant::now();
    assert_closed_by_peer_within(&mut stream, AUTH_TIMEOUT + Duration::from_secs(2)).await;
    assert!(
        started.elapsed() >= AUTH_TIMEOUT - Duration::from_millis(500),
        "closed after {:?}, before the bound",
        started.elapsed()
    );
    let timed_out = logs.lines("raft peer's challenge timed out");
    assert_eq!(timed_out.len(), 1, "{timed_out:?}");
    assert_eq!(level(&timed_out[0]), tracing::Level::DEBUG);
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A responder that hangs up instead of proving its key proved nothing
/// wrong either: a dropped link, which stays a transient network error.
#[tokio::test]
async fn a_responder_hanging_up_before_proving_its_key_is_a_network_error() {
    let (addr, responder) = responder_then(|mut stream| async move {
        exchange_version(&mut stream).await.unwrap();
    })
    .await;
    let err = client_on(WILDCARD_BIND, &addr)
        .await
        .vote(vote_req_from(2), opt())
        .await
        .expect_err("a responder that hangs up never answers");
    assert!(matches!(err, RPCError::Network(_)), "{err:?}");
    responder.await.unwrap();
}

/// A node of the stored membership that no `--raft-peer` entry gives a
/// key is not dialled at all: unreachable, naming it, and nothing
/// connects to its address.
#[tokio::test]
async fn a_node_with_no_configured_key_is_not_dialled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = client_factory(&client_key(), tip_at(0, 0), WILDCARD_BIND, None)
        .new_client(
            3,
            &BasicNode {
                addr: listener.local_addr().unwrap().to_string(),
            },
        )
        .await;
    let message = unreachable_message(
        client
            .vote(vote_req_from(2), opt())
            .await
            .expect_err("a node with no configured key must not be dialled"),
    );
    assert!(message.contains("node 3"), "{message}");
    assert!(
        tokio::time::timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_err(),
        "nothing may connect to a node whose key is not configured"
    );
}

/// A client for `peer` on a node whose raft listener is on `bind`.
async fn client_on(bind: SocketAddr, peer: &str) -> impl RaftNetwork<TypeConfig> {
    client_factory(&client_key(), tip_at(0, 0), bind, None)
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
    let message = unreachable_message(err);
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

/// A version this build does not speak, for a peer on another release.
const OTHER_VERSION: u16 = RAFT_PROTOCOL_VERSION + 1;

/// A raw link to the harness's server, authenticated as node 2.
async fn authenticated_link(h: &Harness) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect(&h.addr).await.unwrap();
    answer_challenge(&mut stream, &h.client_key).await.unwrap();
    stream
}

/// A raw link to the harness's server through the whole handshake, as
/// node 2: ready for envelopes of the test's own making.
async fn handshaken_link(h: &Harness) -> AuthenticatedResponder<tokio::net::TcpStream> {
    let mut stream = authenticated_link(h).await;
    exchange_version(&mut stream).await.unwrap();
    challenge_responder(stream, &authorized_keys(), &peer_ids(), 1)
        .await
        .unwrap()
}

/// `JournalTip { epoch: 3, last_sequence: seq }` on frame `counter` of one
/// sender process.
fn tip_on_frame(counter: u64, seq: u64) -> StampedTip {
    stamped_at(3, seq, counter)
}

/// Send node 2's heartbeat on `link` in an envelope carrying `tip`, and
/// read the server's answer, which must be a success.
async fn heartbeat_stamped(
    link: &mut AuthenticatedResponder<tokio::net::TcpStream>,
    tip: StampedTip,
) {
    link.send(&RpcFrame::new(tip, RpcBody::AppendReq(append_from(2))))
        .await
        .unwrap();
    let reply = link.read_reply().await.unwrap();
    assert!(
        matches!(
            reply.body,
            RpcBody::AppendResp(AppendEntriesResponse::Success)
        ),
        "{reply:?}"
    );
}

/// The receive path end to end, over two real connections from one peer: a
/// frame the peer sent before one the server has already read, arriving
/// after it on the other connection, is still served, but its tip is not
/// recorded and does not refresh the peer's last-heard time. A newer frame
/// on the slow connection is recorded again.
#[tokio::test]
async fn the_server_drops_the_tip_of_a_frame_older_than_one_it_has_read() {
    let h = start_server().await;
    let mut slow = handshaken_link(&h).await;
    let mut fast = handshaken_link(&h).await;

    // Frame 10 overtakes frame 9, which the slow link delivers later.
    heartbeat_stamped(&mut fast, tip_on_frame(10, 100)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    heartbeat_stamped(&mut slow, tip_on_frame(9, 50)).await;

    // Recorded off frame 10, at least the sleep ago; frame 9, just read,
    // would make it a moment.
    let (tip, age) = h.peer_tips.sample(2, std::time::Instant::now()).unwrap();
    assert_eq!(tip, tip_on_frame(10, 100).tip, "the tip must not regress");
    assert!(
        age >= Duration::from_millis(50),
        "the stale frame must not refresh the peer: last heard {age:?} ago"
    );

    // Recorded off frame 11: no older than its send, however slow the host.
    let sent = std::time::Instant::now();
    heartbeat_stamped(&mut slow, tip_on_frame(11, 120)).await;
    let now = std::time::Instant::now();
    let (tip, age) = h.peer_tips.sample(2, now).unwrap();
    assert_eq!(tip, tip_on_frame(11, 120).tip);
    assert!(age <= now - sent, "{age:?}");
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A stand-in node 1 for one connection, through the whole handshake, that
/// answers one request with a success in an envelope carrying `tip`.
async fn responder_answering_with(tip: StampedTip) -> (String, tokio::task::JoinHandle<()>) {
    responder_then(move |mut stream| async move {
        exchange_version(&mut stream).await.unwrap();
        answer_challenge(&mut stream, &server_key()).await.unwrap();
        read_frame(&mut stream).await.unwrap();
        write_frame(
            &mut stream,
            &RpcFrame::new(tip, RpcBody::AppendResp(AppendEntriesResponse::Success)),
        )
        .await
        .unwrap();
    })
    .await
}

/// The reply path: a reply older than a frame already recorded from its
/// sender is returned to raft, but its tip is not recorded; a newer one is.
#[tokio::test]
async fn the_client_drops_the_tip_of_a_reply_older_than_one_it_has_read() {
    let peer_tips = Arc::new(PeerTips::new());
    let t0 = std::time::Instant::now();
    assert!(peer_tips.record_at(1, tip_on_frame(10, 100), t0));
    let mut factory = client_factory_recording(
        &client_key(),
        tip_at(0, 0),
        WILDCARD_BIND,
        None,
        Arc::clone(&peer_tips),
    );

    for (reply, recorded) in [
        (tip_on_frame(9, 50), tip_on_frame(10, 100).tip),
        (tip_on_frame(11, 120), tip_on_frame(11, 120).tip),
    ] {
        let (addr, responder) = responder_answering_with(reply).await;
        let resp = factory
            .new_client(1, &BasicNode { addr })
            .await
            .append_entries(append_from(2), opt())
            .await
            .unwrap();
        assert_eq!(resp, AppendEntriesResponse::Success);
        responder.await.unwrap();
        let (tip, _age) = peer_tips.sample(1, std::time::Instant::now()).unwrap();
        assert_eq!(tip, recorded, "after a reply stamped {:?}", reply.stamp);
    }
}

/// A dialer of raft protocol 1 from before the sender stamp (see
/// `tests::unstamped_envelope`) passes the handshake, and its stampless
/// request is refused: the server closes the link without serving it or
/// recording its tip.
#[tokio::test]
async fn the_server_refuses_a_stampless_request_unrecorded() {
    let h = start_server().await;
    let mut link = handshaken_link(&h).await;
    let stream = link.get_mut();
    pre_version_wire::write_frame(
        stream,
        &pre_version_wire::RpcFrame {
            tip_epoch: 9,
            tip_seq: 1_000,
            body: pre_version_wire::RpcBody::AppendReq(append_from(2)),
        },
    )
    .await
    .unwrap();
    assert_closed_by_peer(stream).await;
    assert_eq!(h.peer_tips.sample(2, std::time::Instant::now()), None);
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A responder of raft protocol 1 from before the sender stamp reads this
/// build's request as sent (its body and tip) and answers in a stampless
/// envelope, which this build refuses: a network error, the reply's epoch
/// neither fenced on nor its tip recorded.
#[tokio::test]
async fn a_stampless_reply_is_refused_unread() {
    let (addr, responder) = responder_then(|mut stream| async move {
        exchange_version(&mut stream).await.unwrap();
        answer_challenge(&mut stream, &server_key()).await.unwrap();
        let request = pre_version_wire::read_frame(&mut stream).await.unwrap();
        pre_version_wire::write_frame(
            &mut stream,
            &pre_version_wire::RpcFrame {
                tip_epoch: CLIENT_EPOCH + 1,
                tip_seq: 1_000,
                body: pre_version_wire::RpcBody::AppendResp(AppendEntriesResponse::Success),
            },
        )
        .await
        .unwrap();
        request
    })
    .await;
    let after = heartbeat_to_node_1_at(&addr, true).await;
    assert_nothing_read(&after);
    assert!(
        matches!(after.outcome, Err(RPCError::Network(_))),
        "{:?}",
        after.outcome
    );
    let request = responder.await.unwrap();
    assert_eq!((request.tip_epoch, request.tip_seq), (CLIENT_EPOCH, 0));
    assert_eq!(
        format!("{:?}", request.body),
        format!("{:?}", pre_version_wire::RpcBody::AppendReq(append_from(2)))
    );
}

/// Capture this thread's log from here until the guard drops: the test's
/// own, and on the current-thread runtime of `#[tokio::test]` the
/// harness server's and stand-in responders' too, as their tasks run on
/// this thread.
fn capture_logs() -> (Logs, tracing::subscriber::DefaultGuard) {
    let logs = Logs::default();
    let guard = tracing::subscriber::set_default(logs.subscriber(tracing::Level::DEBUG));
    (logs, guard)
}

/// The one log line a refused version exchange leaves on this end: a
/// warning (another release is degraded operation needing the operator,
/// not a malfunction of this node, so never `error!`) carrying every one
/// of `parts`. Nothing on the link logged at `error!`.
fn assert_version_refusal_logged(logs: &Logs, parts: &[&str]) {
    let lines = logs.lines("raft peer runs another release");
    assert_eq!(lines.len(), 1, "{lines:?}");
    assert_eq!(level(&lines[0]), tracing::Level::WARN, "{}", lines[0]);
    for part in parts {
        assert!(lines[0].contains(part), "{part:?} not in {:?}", lines[0]);
    }
    let errors: Vec<String> = logs
        .lines("")
        .into_iter()
        .filter(|line| level(line) == tracing::Level::ERROR)
        .collect();
    assert!(errors.is_empty(), "{errors:?}");
}

/// The server refuses a dialer on another release once authenticated,
/// after sending its own version (which is how the dialer can name both),
/// logs both versions, and closes the link.
#[tokio::test]
async fn the_server_refuses_a_dialer_speaking_another_version() {
    let (logs, _guard) = capture_logs();
    let h = start_server().await;
    let mut stream = authenticated_link(&h).await;
    match exchange_version_as(&mut stream, OTHER_VERSION).await {
        Err(VersionExchangeError::Refused(refusal)) => assert_eq!(
            refusal,
            VersionRefusal::Mismatch {
                ours: OTHER_VERSION,
                peer: RAFT_PROTOCOL_VERSION
            }
        ),
        other => panic!("expected a version refusal, got {other:?}"),
    }
    assert_closed_by_peer(&mut stream).await;
    // The server logs before it closes.
    assert_version_refusal_logged(
        &logs,
        &[
            &format!("speaks raft protocol v{OTHER_VERSION}"),
            &format!("this node speaks v{RAFT_PROTOCOL_VERSION}"),
            "peer_id=2",
        ],
    );
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A stand-in responder for one connection: it authenticates the dialer
/// against the harness's table, then hands the link to `then`, playing a
/// node on another release or one that misbehaves later in the handshake.
/// Returns its address and `then`'s outcome.
async fn responder_then<T, F, Fut>(then: F) -> (String, tokio::task::JoinHandle<T>)
where
    F: FnOnce(tokio::net::TcpStream) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = T> + Send,
    T: Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let outcome = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        challenge_peer(&mut stream, &authorized_keys())
            .await
            .unwrap();
        then(stream).await
    });
    (addr, outcome)
}

/// A dialer that hangs up between authentication and the version exchange
/// is a closed link like any other: the server drops it and goes on
/// serving the next one.
#[tokio::test]
async fn a_dialer_hanging_up_before_its_version_leaves_the_server_serving() {
    let h = start_server().await;
    drop(authenticated_link(&h).await);
    let mut client = client_for(&h, &h.client_key).await;
    let resp = client.vote(vote_req_from(2), opt()).await.unwrap();
    assert!(resp.vote_granted);
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A configured peer that authenticates and then sends no version is
/// dropped at the version exchange's bound, as one that never finishes
/// authenticating is at its own, and logged at `debug!` (a stalled link,
/// not another release); the server goes on serving the next one.
#[tokio::test]
async fn a_dialer_silent_after_authenticating_is_dropped_at_the_version_bound() {
    use tokio::io::AsyncReadExt;
    let (logs, _guard) = capture_logs();
    let h = start_server().await;
    let mut stream = authenticated_link(&h).await;
    // The server writes its version before it reads the dialer's, so its
    // bound started before this one does.
    let mut version = [0u8; VERSION_FRAME_WIRE_LEN];
    stream.read_exact(&mut version).await.unwrap();
    assert_eq!(version, encode_version_frame(RAFT_PROTOCOL_VERSION));
    let started = tokio::time::Instant::now();
    assert_closed_by_peer_within(&mut stream, AUTH_TIMEOUT + Duration::from_secs(2)).await;
    assert!(
        started.elapsed() >= AUTH_TIMEOUT - Duration::from_millis(500),
        "closed after {:?}, before the bound",
        started.elapsed()
    );
    let timed_out = logs.lines("raft peer version exchange timed out");
    assert_eq!(timed_out.len(), 1, "{timed_out:?}");
    assert_eq!(level(&timed_out[0]), tracing::Level::DEBUG);
    assert!(logs.lines("raft peer runs another release").is_empty());

    let mut client = client_for(&h, &h.client_key).await;
    assert!(
        client
            .vote(vote_req_from(2), opt())
            .await
            .unwrap()
            .vote_granted
    );
    h.shutdown.store(true, Ordering::Relaxed);
}

/// A responder that authenticates the dialer and then sends no version is
/// given up on at the handshake's bound, as an unreachable peer whose
/// error says the handshake timed out: the version exchange sits under
/// the same bound as authentication, so a stalled peer cannot hold the
/// dialer past it. The dialer sent its own version and then hung up.
#[tokio::test]
async fn a_responder_silent_after_authenticating_is_unreachable_at_the_handshake_bound() {
    use tokio::io::AsyncReadExt;
    let (addr, responder) = responder_then(|mut stream| async move {
        // Everything the dialer sends until it hangs up.
        let mut received = Vec::new();
        stream.read_to_end(&mut received).await.map(|_| received)
    })
    .await;
    // A ttl past the handshake's bound, so it is that bound which fires
    // rather than the RPC's own.
    let ttl = CONNECT_TIMEOUT + Duration::from_secs(3);
    let started = tokio::time::Instant::now();
    let err = client_on(WILDCARD_BIND, &addr)
        .await
        .vote(vote_req_from(2), RPCOption::new(ttl))
        .await
        .expect_err("a silent responder never answers");
    let elapsed = started.elapsed();
    let message = unreachable_message(err);
    let expected = format!("handshake with {addr} timed out");
    assert!(
        message.contains(&expected),
        "{expected:?} not in {message:?}"
    );
    assert!(
        elapsed >= CONNECT_TIMEOUT - Duration::from_millis(500) && elapsed < ttl,
        "gave up after {elapsed:?}, not at the handshake bound"
    );
    let received = responder.await.unwrap().unwrap();
    assert_eq!(received, encode_version_frame(RAFT_PROTOCOL_VERSION));
}

/// A responder on another release, speaking `version`.
async fn responder_speaking(
    version: u16,
) -> (
    String,
    tokio::task::JoinHandle<Result<(), VersionExchangeError>>,
) {
    responder_then(move |mut stream| async move { exchange_version_as(&mut stream, version).await })
        .await
}

/// The dialer refuses a responder on another release, naming both
/// versions, as an unreachable peer: openraft backs off rather than
/// redialling on every heartbeat, as no retry can succeed until the
/// operator finishes the upgrade.
#[tokio::test]
async fn the_dialer_refuses_a_responder_speaking_another_version_as_unreachable() {
    let (logs, _guard) = capture_logs();
    let (addr, responder) = responder_speaking(OTHER_VERSION).await;
    let both_versions = [
        format!("speaks raft protocol v{OTHER_VERSION}"),
        format!("this node speaks v{RAFT_PROTOCOL_VERSION}"),
    ];
    let both_versions = both_versions.each_ref().map(String::as_str);
    assert_unreachable_naming("0.0.0.0:7001", &addr, &both_versions).await;
    // Logged as well as returned: the returned error goes to openraft,
    // which decides what of it to log; this line is ours.
    let mut logged = both_versions.to_vec();
    let addr_field = format!("addr={addr}");
    logged.push(&addr_field);
    assert_version_refusal_logged(&logs, &logged);
    match responder.await.unwrap() {
        Err(VersionExchangeError::Refused(refusal)) => assert_eq!(
            refusal,
            VersionRefusal::Mismatch {
                ours: OTHER_VERSION,
                peer: RAFT_PROTOCOL_VERSION
            }
        ),
        other => panic!("expected the responder to refuse too, got {other:?}"),
    }
}

/// A dialer on a release from before the version, against this server:
/// the version frame the server sends it fails to decode as a reply, and
/// the request it sends is refused and the link closed. The old dialer
/// reads before it writes here (a real one writes first) so the server's
/// close cannot reset the link before the version frame is read. Only
/// this end knows versions, so its log is the one that says why.
#[tokio::test]
async fn a_dialer_from_before_the_version_is_refused_and_cannot_read_the_server() {
    let (logs, _guard) = capture_logs();
    let h = start_server().await;
    let mut stream = authenticated_link(&h).await;
    let err = pre_version_wire::read_frame(&mut stream)
        .await
        .expect_err("an old dialer must not decode the version frame as a reply");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData, "{err}");
    pre_version_wire::write_frame(
        &mut stream,
        &pre_version_wire::RpcFrame {
            tip_epoch: 0,
            tip_seq: 0,
            body: pre_version_wire::RpcBody::VoteReq(vote_req_from(2)),
        },
    )
    .await
    .unwrap();
    assert_closed_by_peer(&mut stream).await;
    assert_version_refusal_logged(
        &logs,
        &[
            "before raft protocol versions",
            &format!("this node speaks v{RAFT_PROTOCOL_VERSION}"),
        ],
    );
    h.shutdown.store(true, Ordering::Relaxed);
}

/// This build dialling a responder from before the version: the responder
/// fails to decode the version frame as a request and hangs up, and the
/// dialer's request fails. A hang-up is all this end sees, which it cannot
/// tell from a dropped link, so it stays a network error.
#[tokio::test]
async fn a_responder_from_before_the_version_cannot_read_the_dialer() {
    let (addr, responder) =
        responder_then(|mut stream| async move { pre_version_wire::read_frame(&mut stream).await })
            .await;
    let err = client_on(WILDCARD_BIND, &addr)
        .await
        .vote(vote_req_from(2), opt())
        .await
        .expect_err("an old responder never answers");
    assert!(matches!(err, RPCError::Network(_)), "{err:?}");
    let decode = responder
        .await
        .unwrap()
        .expect_err("an old responder must not decode the version frame as a request");
    assert_eq!(decode.kind(), std::io::ErrorKind::InvalidData, "{decode}");
}
