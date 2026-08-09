//! Inbound control-plane RPC: accepts authenticated peer connections and
//! dispatches their requests into the local raft core.
//!
//! Dispatch is generic over [`RaftApi`] (implemented by `openraft::Raft`) so
//! the server — auth, identity pinning, framing, error paths — is testable
//! without a running raft instance.
//!
//! This is also where the journal-tip vote recency filter will sit (slice
//! C2): a `VoteReq` from a candidate whose advertised tip is behind ours is
//! dropped *before* it reaches `Raft::vote`, indistinguishable from packet
//! loss — raft safety is untouched, only liveness is shaped.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use melin_app::auth::AuthorizedKeys;
use openraft::Raft;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tracing::debug;
use tracing::warn;

use crate::auth::authenticate_inbound;
use crate::recency::JournalTip;
use crate::recency::TipSource;
use crate::recency::VoteFilter;
use crate::types::NodeId;
use crate::types::TypeConfig;
use crate::wire::RpcBody;
use crate::wire::RpcFrame;
use crate::wire::claimed_sender;
use crate::wire::read_frame;
use crate::wire::write_frame;

/// Bound on the challenge-response exchange — an unauthenticated socket may
/// not sit on the accept path longer than this.
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

/// Idle bound on an authenticated link. Peers only hold a connection open
/// while actively sending (heartbeats arrive every 200 ms from a live
/// leader), so half an hour of silence means the peer forgot about us —
/// reclaim the task; the peer reconnects on next use.
const READ_IDLE_TIMEOUT: Duration = Duration::from_secs(1800);

/// Concurrent inbound connections cap. A full mesh of the largest sensible
/// control plane is single digits; 32 leaves room for reconnect churn while
/// bounding what an accept storm can pin.
const MAX_INBOUND: usize = 32;

/// Shutdown-poll cadence for the accept loop — same convention as the
/// admin/health listener loops in the server runtime.
const ACCEPT_POLL: Duration = Duration::from_millis(100);

/// Bound on writing a single reply frame. Replies are tiny (a vote/append
/// response, or a snapshot-chunk ack) and a healthy peer drains them
/// instantly, so this only fires when a peer's receive window is wedged
/// (crashed mid-read, black-holed link). Without it a stalled writer pins
/// its task — and the [`MAX_INBOUND`] slot it holds — indefinitely; 32
/// such peers would exhaust the accept cap. Reclaim the task instead; the
/// peer reconnects on next use.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// The slice of the raft core the RPC server needs. Implemented by
/// [`openraft::Raft`]; mocked in tests. Errors are stringly-typed because
/// the remote peer only logs/retries them (see `RpcBody::Error`).
pub trait RaftApi: Send + Sync + 'static {
    fn vote(
        &self,
        req: VoteRequest<NodeId>,
    ) -> impl Future<Output = Result<VoteResponse<NodeId>, String>> + Send;
    fn append_entries(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> impl Future<Output = Result<AppendEntriesResponse<NodeId>, String>> + Send;
    fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> impl Future<Output = Result<InstallSnapshotResponse<NodeId>, String>> + Send;
}

impl RaftApi for Raft<TypeConfig> {
    async fn vote(&self, req: VoteRequest<NodeId>) -> Result<VoteResponse<NodeId>, String> {
        Raft::vote(self, req).await.map_err(|e| e.to_string())
    }

    async fn append_entries(
        &self,
        req: AppendEntriesRequest<TypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeId>, String> {
        Raft::append_entries(self, req)
            .await
            .map_err(|e| e.to_string())
    }

    async fn install_snapshot(
        &self,
        req: InstallSnapshotRequest<TypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeId>, String> {
        Raft::install_snapshot(self, req)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Server-side identity and envelope state.
pub struct RpcServerConfig {
    pub authorized_keys: Arc<AuthorizedKeys>,
    /// Verified pubkey → configured node id, from the `--raft-peer` table.
    /// A key that authenticates but isn't a configured peer is refused: the
    /// replication trust domain is broader than the raft voter set.
    pub peer_ids: Arc<HashMap<[u8; 32], NodeId>>,
    /// Local journal tip: stamped on every response envelope, and the
    /// voter side of the recency filter below.
    pub tip: Arc<TipSource>,
    /// Fence-on-supersession (see [`SupersessionPolicy`]); `None` when
    /// auto-promotion is off — without automation, fencing stays a
    /// data-plane-contact concern exactly as documented today.
    pub supersession: Option<SupersessionPolicy>,
    /// Journal-tip vote filter (see `crate::recency`). One per node —
    /// its drop counter is *this voter's* view of election progress, so
    /// it is shared across peer connections. A `std::sync::Mutex` (not
    /// tokio): the critical section is a couple of integer compares at
    /// control-plane RPC rates, and no `.await` ever happens inside it.
    pub vote_filter: std::sync::Mutex<VoteFilter>,
}

/// The raft peer mesh as an additional fencing channel. Every inbound
/// envelope carries the sender's journal-tip *fencing epoch*; a node
/// that currently claims to be serving (a primary, or a replica whose
/// promotion is already in flight) and observes a strictly higher epoch
/// has been superseded — it self-fences and shuts down, exactly like
/// the data-plane handshake path (`FenceState::fence_if_superseded`).
/// This closes the split-brain window faster than waiting for a
/// data-plane connection to cross: raft heartbeats flow continuously.
pub struct SupersessionPolicy {
    pub fence: Arc<melin_transport_core::fence::FenceState>,
    /// Process shutdown flag, co-set on supersession (self-demotion).
    pub shutdown: Arc<AtomicBool>,
    /// Whether this node currently claims to be serving. A closure
    /// because the claim is role-dependent and owned by the server
    /// runtime: primaries always claim; replicas claim once a
    /// promotion has been filed.
    pub serving: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl RpcServerConfig {
    /// Feed an inbound envelope's advertised fencing epoch to the
    /// supersession policy, if one is armed and this node is serving.
    fn observe_peer_epoch(&self, peer_epoch: u64) {
        let Some(policy) = &self.supersession else {
            return;
        };
        if !(policy.serving)() {
            return;
        }
        if policy
            .fence
            .fence_if_superseded(peer_epoch, &policy.shutdown)
            == Some(true)
        {
            warn!(
                peer_epoch,
                "raft peer advertises a higher fencing epoch — this node is superseded; fencing"
            );
        }
    }

    /// Recency-filter verdict for an inbound vote request whose envelope
    /// advertised `candidate_tip`. Also the tip-readiness gate: before
    /// recovery seeds the local tip, no vote may be delivered at all.
    fn admit_vote(&self, candidate_tip: JournalTip) -> bool {
        if !self.tip.is_ready() {
            debug!("dropping vote request — local journal tip not recovered yet");
            return false;
        }
        // Poisoning unreachable under panic=abort (no unwinding).
        let mut filter = self.vote_filter.lock().expect("vote filter mutex poisoned");
        filter.should_deliver(candidate_tip, self.tip.local_tip())
    }
}

/// Accept loop. Runs until `shutdown` flips; each authenticated connection
/// gets its own task.
pub async fn serve<A: RaftApi + Clone>(
    listener: TcpListener,
    api: A,
    cfg: Arc<RpcServerConfig>,
    shutdown: Arc<AtomicBool>,
) {
    // Semaphore over a counter: closing a connection task must release its
    // slot exactly once even on panicky-looking early returns, and
    // `OwnedSemaphorePermit` ties the release to task drop.
    let inbound_slots = Arc::new(tokio::sync::Semaphore::new(MAX_INBOUND));
    while !shutdown.load(Ordering::Relaxed) {
        let accepted = match tokio::time::timeout(ACCEPT_POLL, listener.accept()).await {
            // Poll tick: re-check the shutdown flag.
            Err(_elapsed) => continue,
            Ok(Err(e)) => {
                debug!(error = %e, "raft rpc accept failed");
                continue;
            }
            Ok(Ok(accepted)) => accepted,
        };
        let (stream, peer_addr) = accepted;
        let Ok(permit) = Arc::clone(&inbound_slots).try_acquire_owned() else {
            debug!(%peer_addr, "raft rpc inbound cap reached — dropping connection");
            continue;
        };
        let api = api.clone();
        let cfg = Arc::clone(&cfg);
        tokio::spawn(async move {
            handle_connection(stream, peer_addr, api, cfg).await;
            drop(permit);
        });
    }
}

async fn handle_connection<A: RaftApi>(
    mut stream: TcpStream,
    peer_addr: std::net::SocketAddr,
    api: A,
    cfg: Arc<RpcServerConfig>,
) {
    if let Err(e) = stream.set_nodelay(true) {
        debug!(%peer_addr, error = %e, "set_nodelay failed");
        return;
    }

    // Authenticate, bounded.
    let pubkey = match tokio::time::timeout(
        AUTH_TIMEOUT,
        authenticate_inbound(&mut stream, &cfg.authorized_keys),
    )
    .await
    {
        Ok(Ok(pubkey)) => pubkey,
        Ok(Err(e)) => {
            // A failed peer auth on the control plane is worth attention —
            // it is either misconfiguration or an impersonation attempt.
            warn!(%peer_addr, error = %e, "raft peer failed authentication");
            return;
        }
        Err(_elapsed) => {
            debug!(%peer_addr, "raft peer auth timed out");
            return;
        }
    };

    // Pin the connection to a configured peer id.
    let Some(&peer_id) = cfg.peer_ids.get(&pubkey) else {
        warn!(%peer_addr, "authenticated key is not a configured raft peer — closing");
        return;
    };
    debug!(%peer_addr, peer_id, "raft peer connected");

    loop {
        let frame = match tokio::time::timeout(READ_IDLE_TIMEOUT, read_frame(&mut stream)).await {
            Ok(Ok(frame)) => frame,
            Ok(Err(e)) => {
                debug!(peer_id, error = %e, "raft peer link closed");
                return;
            }
            Err(_elapsed) => {
                debug!(peer_id, "raft peer link idle — closing");
                return;
            }
        };

        // Identity pinning: a request claiming to come from a node other
        // than the one this connection authenticated as is an impersonation
        // attempt (or a key shared between nodes — equally unacceptable).
        if let Some(claimed) = claimed_sender(&frame.body)
            && claimed != peer_id
        {
            warn!(
                peer_id,
                claimed, "raft rpc claims a different node id than its key — closing"
            );
            return;
        }

        // Fence-on-supersession: every peer envelope advertises the
        // sender's fencing epoch (see `SupersessionPolicy`).
        cfg.observe_peer_epoch(frame.tip_epoch);

        // Journal-tip recency filter (see `crate::recency`): a vote
        // request from a candidate behind our own tip is dropped before
        // it can reach `Raft::vote` — indistinguishable from packet loss,
        // so raft safety is untouched. Appends re-arm the filter: they
        // prove a live leader exists.
        match &frame.body {
            RpcBody::VoteReq(_) => {
                let candidate_tip = JournalTip {
                    epoch: frame.tip_epoch,
                    last_sequence: frame.tip_seq,
                };
                if !cfg.admit_vote(candidate_tip) {
                    debug!(
                        peer_id,
                        candidate_epoch = frame.tip_epoch,
                        candidate_seq = frame.tip_seq,
                        "vote request filtered — candidate journal tip behind ours"
                    );
                    // Close rather than answer: to the candidate this is a
                    // network error, exactly like a lost packet.
                    return;
                }
            }
            RpcBody::AppendReq(_) => {
                // Poisoning unreachable under panic=abort.
                cfg.vote_filter
                    .lock()
                    .expect("vote filter mutex poisoned")
                    .leader_observed();
            }
            _ => {}
        }

        let response = handle_body(&api, frame.body).await;
        let local = cfg.tip.local_tip();
        let (tip_epoch, tip_seq) = (local.epoch, local.last_sequence);
        let reply = RpcFrame {
            tip_epoch,
            tip_seq,
            body: response,
        };
        match tokio::time::timeout(WRITE_TIMEOUT, write_frame(&mut stream, &reply)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                debug!(peer_id, error = %e, "raft rpc reply failed");
                return;
            }
            Err(_elapsed) => {
                debug!(peer_id, "raft rpc reply write stalled — closing");
                return;
            }
        }
    }
}

/// Dispatch one request body into the raft core. Non-request bodies are
/// answered with an error rather than dropped so a confused peer fails fast.
async fn handle_body<A: RaftApi>(api: &A, body: RpcBody) -> RpcBody {
    match body {
        RpcBody::VoteReq(req) => match api.vote(req).await {
            Ok(resp) => RpcBody::VoteResp(resp),
            Err(e) => RpcBody::Error(e),
        },
        RpcBody::AppendReq(req) => match api.append_entries(req).await {
            Ok(resp) => RpcBody::AppendResp(resp),
            Err(e) => RpcBody::Error(e),
        },
        RpcBody::SnapshotReq(req) => match api.install_snapshot(req).await {
            Ok(resp) => RpcBody::SnapshotResp(resp),
            Err(e) => RpcBody::Error(e),
        },
        other => RpcBody::Error(format!("not a request: {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::Vote;

    /// Canned-response API for dispatch tests.
    #[derive(Clone)]
    struct MockApi;

    impl RaftApi for MockApi {
        async fn vote(&self, req: VoteRequest<NodeId>) -> Result<VoteResponse<NodeId>, String> {
            Ok(VoteResponse {
                vote: req.vote,
                vote_granted: true,
                last_log_id: None,
            })
        }
        async fn append_entries(
            &self,
            _req: AppendEntriesRequest<TypeConfig>,
        ) -> Result<AppendEntriesResponse<NodeId>, String> {
            Err("append not served in this mock".to_owned())
        }
        async fn install_snapshot(
            &self,
            _req: InstallSnapshotRequest<TypeConfig>,
        ) -> Result<InstallSnapshotResponse<NodeId>, String> {
            Err("snapshot not served in this mock".to_owned())
        }
    }

    #[tokio::test]
    async fn dispatch_answers_vote() {
        let resp = handle_body(
            &MockApi,
            RpcBody::VoteReq(VoteRequest {
                vote: Vote::new(2, 1),
                last_log_id: None,
            }),
        )
        .await;
        match resp {
            RpcBody::VoteResp(r) => assert!(r.vote_granted),
            other => panic!("expected VoteResp, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dispatch_maps_api_error() {
        let resp = handle_body(
            &MockApi,
            RpcBody::AppendReq(AppendEntriesRequest {
                vote: Vote::new(2, 1),
                prev_log_id: None,
                entries: vec![],
                leader_commit: None,
            }),
        )
        .await;
        assert!(matches!(resp, RpcBody::Error(_)));
    }

    #[tokio::test]
    async fn dispatch_rejects_non_requests() {
        let resp = handle_body(&MockApi, RpcBody::Error("hello".to_owned())).await;
        match resp {
            RpcBody::Error(msg) => assert!(msg.contains("not a request")),
            other => panic!("expected Error, got {other:?}"),
        }
    }
}
