//! DPDK replication transport — smoltcp-based sender and receiver paths.
//!
//! Mirrors the kernel-TCP variants in `mod.rs` but uses `DpdkTransport`
//! (a `smoltcp` socket over DPDK queue pairs) instead of `TcpStream`.
//! The wire protocol is identical — see `protocol.rs` for the message catalogue.

#![cfg(feature = "dpdk")]

use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use tracing::{debug, error, info, warn};

use melin_app::Application;
use melin_journal::JournalWrite;
use melin_journal::replication::ReplicationConsumer;
use melin_transport_core::pipeline::{JournalStage, JournalStageRun};

use super::auth::{
    AuthChallenge, AuthOutcome, AuthTransport, PolledAuthStream, authenticate_with_primary,
    generate_challenge_nonce, step_authentication,
};
use super::receiver_transport::{
    ControlFrameSource, FrameResult, ReceiverTransport, compact_recv_buf, streaming_loop,
    try_extract_frame,
};
use super::{
    AfterSession, MAX_BACKOFF, ReceiverResult, ReplicaCursors, ReplicaGate, ReplicaPipelineHandles,
    ReplicationMetrics, ResyncDecision, SentHighWater, build_replica_pipeline_with_threads,
    handle_resync_verdict, handle_session_exit, recover_replica_state, sleep_checking_flags,
    take_pipeline_for_promotion, teardown_replica_pipeline,
};
use melin_app::auth::AuthorizedKeys;
use melin_transport_core::replication::catchup::{
    CatchUpResult, bridge_catchup_to_live, can_catch_up_from_journal, catch_up_from_journal_with,
    preflight_snapshot_transfer, snapshot_transfer_with,
};
use melin_transport_core::replication::protocol::{
    Ack, Handshake, MAX_CONTROL_FRAME, PrimaryMessage, ReplicaMessage, decode_primary_message,
    decode_replica_message, encode_ack, encode_challenge, encode_handshake, encode_hash_mismatch,
    encode_heartbeat, encode_need_snapshot, encode_stream_start,
};
use melin_transport_core::replication::validate::{
    HandshakeValidation, validate_replica_handshake_settled,
};

// ---------------------------------------------------------------------------
// DPDK ReceiverTransport implementation
// ---------------------------------------------------------------------------

/// DPDK/smoltcp-backed receiver transport for the replica side.
///
/// Wraps a `DpdkTransport` + `SocketHandle` and implements
/// [`ReceiverTransport`] so the generic [`streaming_loop`] can drive it
/// identically to the kernel io_uring path.
struct DpdkReceiverTransport<'a> {
    transport: &'a mut melin_dpdk::DpdkTransport,
    handle: melin_dpdk::SocketHandle,
    send_buf: Vec<u8>,
}

const ACK_RETRY_CAP: u32 = 32;

impl ReceiverTransport for DpdkReceiverTransport<'_> {
    fn poll_recv(&mut self, recv_buf: &mut Vec<u8>) -> io::Result<bool> {
        self.transport.poll();
        let before = recv_buf.len();
        self.transport.recv_into_vec(self.handle, recv_buf);
        Ok(recv_buf.len() > before)
    }

    fn send_ack(&mut self, ack: &Ack) -> io::Result<bool> {
        self.send_buf.clear();
        encode_ack(ack, &mut self.send_buf);
        let mut attempts: u32 = 0;
        loop {
            if self.transport.queue_send(self.handle, &self.send_buf) {
                // Flush immediately so the ACK reaches the primary
                // without waiting for the next poll_recv iteration.
                self.transport.poll();
                return Ok(true);
            }
            attempts += 1;
            if attempts >= ACK_RETRY_CAP {
                tracing::warn!("DPDK ack send failed after {ACK_RETRY_CAP} retries");
                return Ok(false);
            }
            self.transport.poll();
            if !self.transport.is_active(self.handle) {
                return Err(io::Error::other("replica disconnected during ack send"));
            }
        }
    }

    fn ack_in_flight(&self) -> bool {
        false
    }

    fn is_connected(&mut self) -> bool {
        self.transport.is_active(self.handle)
    }
}

/// Per-slot state for the DPDK replication sender.
enum SlotState {
    /// No replica connected on this slot.
    Idle,
    /// Replica connected; running the Ed25519 challenge/response before the
    /// handshake. Non-blocking, like `Handshaking` — the poll thread also
    /// serves client traffic and the other slot, so we never block on the
    /// replica's response (see `AuthChallenge`).
    Authenticating(melin_dpdk::SocketHandle),
    /// Replica authenticated, performing handshake.
    Handshaking(melin_dpdk::SocketHandle),
    /// Streaming journal data to replica.
    Streaming(melin_dpdk::SocketHandle),
}

/// Deadline for one side of the DPDK auth exchange to make progress. On the
/// sender it bounds how long a connected-but-silent replica may hold a slot
/// before we reclaim it; on the receiver it bounds the wait for the
/// primary's challenge/result. Enforced across poll ticks (sender) or via the
/// blocking adapter's deadline (receiver), never by stalling the poll thread.
const AUTH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// In-flight handshake chain validation, running on a short-lived
/// background thread. `validate_replica_handshake_settled` can scan a
/// full segment per attempt and sleeps between retries (~400 ms budget
/// on a divergent verdict) — far too long to run inline on the poll
/// thread, which also services client traffic and the other replica
/// slot. The slot stays `Handshaking` and polls `verdict_rx` each tick.
struct PendingValidation {
    /// The handshake that triggered validation — consumed by the
    /// post-verdict catch-up/resync flow.
    handshake: Handshake,
    /// One-shot verdict channel; the sender half lives on the
    /// validation thread.
    verdict_rx: std::sync::mpsc::Receiver<io::Result<HandshakeValidation>>,
}

/// Per-replica slot — owns its ring consumer and state machine.
struct DpdkReplicaSlot {
    state: SlotState,
    consumer: ReplicationConsumer,
    active_flag: Arc<AtomicBool>,
    evict_flag: Arc<AtomicBool>,
    recv_buf: Vec<u8>,
    send_buf: Vec<u8>,
    last_send: std::time::Instant,
    /// Sent high-water for the current connection — the ack-sanity
    /// bound and the heartbeat sequence. Meaningless while `Idle`;
    /// re-seeded on every handshake. See `SentHighWater`.
    sent: SentHighWater,
    /// `Some` while this slot's handshake validation runs off-thread.
    pending_validation: Option<PendingValidation>,
    /// `Some` while this slot is `Authenticating` — the challenge we issued
    /// and its deadline. Cleared on transition out of `Authenticating`.
    auth: Option<AuthChallenge>,
}

// The sender-side auth-step machinery — the `AuthTransport` surface,
// `AuthOutcome`, `AuthChallenge`, and `step_authentication` — lives in
// `super::auth` so the protocol step is shared with the kernel-TCP path,
// transport-agnostic, and unit-testable without the `dpdk` feature. Only the
// concrete transport impl belongs here.
impl AuthTransport for melin_dpdk::DpdkTransport {
    type Handle = melin_dpdk::SocketHandle;
    // Each method forwards to the inherent method of the same name (inherent
    // resolution wins, so there is no recursion).
    fn is_active(&mut self, handle: Self::Handle) -> bool {
        self.is_active(handle)
    }
    fn recv_into_vec(&mut self, handle: Self::Handle, dest: &mut Vec<u8>) {
        self.recv_into_vec(handle, dest);
    }
    fn queue_send(&mut self, handle: Self::Handle, data: &[u8]) -> bool {
        self.queue_send(handle, data)
    }
    fn poll(&mut self) {
        self.poll();
    }
}

impl DpdkReplicaSlot {
    /// The single transition back to `Idle` from a connected state
    /// (`Authenticating` / `Handshaking` / `Streaming`). Every
    /// disconnect / reject / eviction path funnels through here so none can
    /// leak the smoltcp handle or desync the bookkeeping:
    ///
    /// - **Closes the socket** (idempotent) — reclaims it whether the handle is
    ///   already removed *or* still pinned in the `SocketSet` in
    ///   `Closed`/`TimeWait`. Making the close part of "go Idle" (rather than a
    ///   step each arm must remember) is what prevents the
    ///   leak-on-disconnect class of bug.
    /// - Disengages the shared cursors **before** releasing the journal-stage
    ///   gate — ordering contract B2 (a frozen shared-min would otherwise stop
    ///   the primary acking client requests even with a healthy peer).
    /// - Decrements the trading-halt gate **only if the replica was past auth**
    ///   (`Handshaking`/`Streaming`) — an `Authenticating` connection never
    ///   lifted it (the gate is lifted on auth success, not on connect), so it
    ///   must not lower it. Warns if the last authenticated replica just left
    ///   (trading is now unprotected).
    /// - Clears per-connection scratch (recv buffer, in-flight challenge,
    ///   pending handshake validation).
    ///
    /// Callers never assign `SlotState::Idle` directly; they call this and keep
    /// only their state-specific extras (e.g. eviction's ring skip).
    fn go_idle(
        &mut self,
        slot_idx: usize,
        transport: &mut melin_dpdk::DpdkTransport,
        cursors: &ReplicaCursors,
        metrics: &ReplicationMetrics,
        replicas_connected: &AtomicU32,
    ) {
        if let SlotState::Authenticating(h) | SlotState::Handshaking(h) | SlotState::Streaming(h) =
            self.state
        {
            // The gate is lifted only once a replica authenticates (see the
            // `Authenticated` arm); an `Authenticating` connection that drops
            // here never lifted it, so it must not lower it.
            let was_authenticated = matches!(
                self.state,
                SlotState::Handshaking(_) | SlotState::Streaming(_)
            );
            transport.close(h);
            // Disengage cursors before the active_flag Release — contract B2.
            cursors.clear_on_disconnect(slot_idx);
            self.active_flag.store(false, Ordering::Release);
            metrics.catching_up[slot_idx].store(false, Ordering::Relaxed);
            if was_authenticated {
                ReplicaGate::new(replicas_connected).lower();
            }
        }
        self.state = SlotState::Idle;
        self.recv_buf.clear();
        self.auth = None;
        self.pending_validation = None;
    }
}

/// Step-able DPDK replication state. Owns both slot state machines and the
/// shared cursors / metrics, but does NOT own the `DpdkTransport` — it
/// reaches into one supplied by the caller per call. This shape lets the
/// primary's single DPDK poll thread drive replication alongside client
/// traffic by calling `tick()` once per poll iteration and dispatching
/// `accept_connection()` for any `AcceptedConnection` that arrives on the
/// replication listen port.
///
/// Parameterised over `A: Application` so successive `tick` calls cannot
/// disagree on the application type — the journal catch-up and snapshot
/// transfer paths both decode events through `A`, so a mid-flight switch
/// would silently corrupt the wire. The driver carries no `A`-typed
/// data, hence the `PhantomData`.
pub struct DpdkReplicationDriver<A: Application> {
    slots: [DpdkReplicaSlot; 2],
    /// Single owner of the per-replica progress cursors (per-slot
    /// acked positions, shared min/max, and the gate's gauge pair).
    cursors: ReplicaCursors,
    journal_path: std::path::PathBuf,
    replica_ready: Arc<AtomicBool>,
    replicas_connected: Arc<AtomicU32>,
    metrics: Arc<ReplicationMetrics>,
    batch_size: usize,
    heartbeat_interval: std::time::Duration,
    /// Node fencing state — see the kernel-TCP sender. Read to stamp the
    /// primary's epoch onto each `StreamStart` and to self-demote when a
    /// replica handshakes with a higher epoch.
    fence_state: Arc<melin_transport_core::fence::FenceState>,
    /// Operator key table. `Arc` because it's shared, read-only, with the
    /// client-auth path and the kernel-TCP sender; the driver only ever
    /// `lookup`s during a replica's challenge/response. Held by the driver
    /// (not threaded per-call) so the `Authenticating` tick arm can verify
    /// without the caller re-supplying it each poll.
    authorized_keys: Arc<AuthorizedKeys>,
    // Anchors the `A` type parameter — the struct holds no app-typed
    // state, but `tick`'s journal-catchup and snapshot-transfer paths
    // do, and we want the type system to enforce that the same `A`
    // flows through every call rather than relying on every call site
    // to spell `::<A>` consistently.
    _app: PhantomData<fn(A)>,
}

impl<A: Application> DpdkReplicationDriver<A> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repl_consumers: [ReplicationConsumer; 2],
        replication_cursor: Arc<AtomicU64>,
        fastest_replica_cursor: Arc<AtomicU64>,
        journal_path: std::path::PathBuf,
        replica_ready: Arc<AtomicBool>,
        replicas_connected: Arc<AtomicU32>,
        evict_flags: [Arc<AtomicBool>; 2],
        active_flags: [Arc<AtomicBool>; 2],
        metrics: Arc<ReplicationMetrics>,
        batch_size: usize,
        heartbeat_secs: u64,
        fence_state: Arc<melin_transport_core::fence::FenceState>,
        authorized_keys: Arc<AuthorizedKeys>,
    ) -> Self {
        let [consumer_0, consumer_1] = repl_consumers;
        let now = std::time::Instant::now();
        DpdkReplicationDriver {
            slots: [
                DpdkReplicaSlot {
                    state: SlotState::Idle,
                    consumer: consumer_0,
                    active_flag: Arc::clone(&active_flags[0]),
                    evict_flag: Arc::clone(&evict_flags[0]),
                    recv_buf: Vec::with_capacity(4096),
                    send_buf: Vec::with_capacity(512 * 1024),
                    last_send: now,
                    // Placeholder until the slot engages — re-seeded on
                    // every handshake.
                    sent: SentHighWater::seed(0, 0),
                    pending_validation: None,
                    auth: None,
                },
                DpdkReplicaSlot {
                    state: SlotState::Idle,
                    consumer: consumer_1,
                    active_flag: Arc::clone(&active_flags[1]),
                    evict_flag: Arc::clone(&evict_flags[1]),
                    recv_buf: Vec::with_capacity(4096),
                    send_buf: Vec::with_capacity(512 * 1024),
                    last_send: now,
                    // Placeholder until the slot engages — re-seeded on
                    // every handshake.
                    sent: SentHighWater::seed(0, 0),
                    pending_validation: None,
                    auth: None,
                },
            ],
            cursors: ReplicaCursors::new(
                replication_cursor,
                fastest_replica_cursor,
                metrics.clone(),
            ),
            journal_path,
            replica_ready,
            replicas_connected,
            metrics,
            batch_size,
            heartbeat_interval: std::time::Duration::from_secs(heartbeat_secs),
            fence_state,
            authorized_keys,
            _app: PhantomData,
        }
    }

    /// Take ownership of a freshly-accepted connection on the replication
    /// port. Assigns it to the first idle slot, or closes it if both slots
    /// are occupied (dual-repl cap). Caller is responsible for filtering by
    /// `AcceptedConnection::listen_port`.
    pub fn accept_connection(
        &mut self,
        peer: std::net::SocketAddr,
        handle: melin_dpdk::SocketHandle,
        transport: &mut melin_dpdk::DpdkTransport,
    ) {
        let idle_slot = self
            .slots
            .iter()
            .position(|s| matches!(s.state, SlotState::Idle));
        let Some(idx) = idle_slot else {
            debug!(peer = ?peer, "replica rejected — both slots occupied");
            transport.close(handle);
            return;
        };

        // Issue the auth challenge immediately (non-blocking): the replica
        // must sign this nonce with a key carrying Replication permission
        // before we process its handshake. The response is verified across
        // ticks in the `Authenticating` arm — we never block the poll thread
        // on a silent replica. Mirrors the kernel-TCP sender's
        // `authenticate_replica`, sharing the nonce generation and (in tick)
        // the verification.
        let nonce = match generate_challenge_nonce() {
            Ok(n) => n,
            Err(e) => {
                warn!(peer = ?peer, error = %e, "failed to generate auth challenge — dropping replica");
                transport.close(handle);
                return;
            }
        };
        let slot = &mut self.slots[idx];
        slot.recv_buf.clear();
        slot.send_buf.clear();
        encode_challenge(&nonce, &mut slot.send_buf);
        if !transport.queue_send(handle, &slot.send_buf) {
            warn!(peer = ?peer, slot = idx, "TX queue full sending auth challenge — dropping replica");
            transport.close(handle);
            return;
        }
        info!(peer = ?peer, slot = idx, "replica connected via DPDK — authenticating");
        // The trading-halt gate is NOT lifted here: a bare connection hasn't
        // proven a Replication key. It's lifted on auth success (the
        // `Authenticated` arm) so an unauthenticated peer can't re-enable order
        // matching.
        slot.auth = Some(AuthChallenge {
            nonce,
            deadline: std::time::Instant::now() + AUTH_TIMEOUT,
        });
        slot.state = SlotState::Authenticating(handle);
    }

    /// Drive both slots' state machines for one poll iteration. Returns
    /// `true` if at least one slot is currently active (Handshaking or
    /// Streaming) — caller can use this to decide whether to busy-spin
    /// on idle.
    pub fn tick(
        &mut self,
        transport: &mut melin_dpdk::DpdkTransport,
        shutdown: &AtomicBool,
    ) -> bool {
        // Local rebinds for readability — the body below was lifted from
        // the previous run_sender_dpdk thread, mostly verbatim, so keep
        // the variable names matching.
        let slots = &mut self.slots;
        let cursors = &self.cursors;
        let journal_path = &self.journal_path;
        let replica_ready = &self.replica_ready;
        let replicas_connected = &self.replicas_connected;
        let metrics = &self.metrics;
        let fence_state = &self.fence_state;
        let authorized_keys = &self.authorized_keys;
        let batch_size = self.batch_size;
        let heartbeat_interval = self.heartbeat_interval;

        // Check eviction flags from the journal stage.
        for (i, slot) in slots.iter_mut().enumerate() {
            let evicting =
                slot.evict_flag.load(Ordering::Acquire) && !matches!(slot.state, SlotState::Idle);
            if !evicting {
                continue;
            }
            metrics.evictions_total.fetch_add(1, Ordering::Relaxed);
            warn!(
                slot = i,
                "evicting slow replica (ring backpressure timeout, DPDK)"
            );
            // Eviction-specific: drop any unread ring entries so a reconnecting
            // replica on this slot doesn't replay pre-eviction data and stall
            // the primary's replication cursor (see tcp_sender.rs), and clear
            // the latch that fired this eviction. The close + cursor/gate/count
            // teardown is the shared `go_idle`.
            slot.consumer.skip_to_producer();
            slot.evict_flag.store(false, Ordering::Release);
            slot.go_idle(i, transport, cursors, metrics, replicas_connected);
        }

        let mut any_active = false;

        for (slot_idx, slot) in slots.iter_mut().enumerate() {
            match slot.state {
                SlotState::Idle => {
                    // Drain ring to keep it flowing. The journal stage
                    // skips inactive rings (active_flag=false), but there
                    // may be residual entries from before the flag was cleared.
                    while slot.consumer.try_read().is_some() {
                        slot.consumer.commit();
                    }
                }

                SlotState::Authenticating(handle) => {
                    any_active = true;

                    // `auth` is always `Some` here — set in `accept_connection`
                    // and cleared only on transition out of `Authenticating`.
                    // The shared borrow of `slot.auth` coexists with the two
                    // `&mut` buffer borrows below because they are disjoint
                    // fields.
                    let challenge = slot
                        .auth
                        .as_ref()
                        .expect("auth challenge present while Authenticating");

                    match step_authentication(
                        transport,
                        handle,
                        challenge,
                        &mut slot.recv_buf,
                        &mut slot.send_buf,
                        authorized_keys,
                        slot_idx,
                    ) {
                        // Frame not yet complete — wait for more data next tick.
                        AuthOutcome::Pending => {}
                        AuthOutcome::Authenticated => {
                            slot.auth = None;
                            // Proven a Replication key — only now does this
                            // connection lift the trading-halt gate (lowered by
                            // `go_idle` on teardown).
                            ReplicaGate::new(replicas_connected).lift();
                            slot.state = SlotState::Handshaking(handle);
                        }
                        AuthOutcome::Rejected => {
                            slot.go_idle(slot_idx, transport, cursors, metrics, replicas_connected)
                        }
                    }
                    continue;
                }

                SlotState::Handshaking(handle) => {
                    any_active = true;

                    // Check for disconnect during handshake.
                    if !transport.is_active(handle) {
                        warn!(
                            slot = slot_idx,
                            "replica disconnected during handshake (DPDK)"
                        );
                        slot.go_idle(slot_idx, transport, cursors, metrics, replicas_connected);
                        continue;
                    }

                    // A validation verdict may be outstanding — poll it
                    // without blocking. The replica is silent between its
                    // Handshake and our response, so no frames need
                    // processing while waiting.
                    if let Some(pv) = slot.pending_validation.take() {
                        let res = match pv.verdict_rx.try_recv() {
                            Err(std::sync::mpsc::TryRecvError::Empty) => {
                                // Not settled yet — revisit next tick.
                                slot.pending_validation = Some(pv);
                                continue;
                            }
                            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                // The validation thread can only drop the
                                // sender without a verdict by panicking.
                                warn!(
                                    slot = slot_idx,
                                    "handshake validation thread died — disconnecting"
                                );
                                slot.go_idle(
                                    slot_idx,
                                    transport,
                                    cursors,
                                    metrics,
                                    replicas_connected,
                                );
                                continue;
                            }
                            Ok(res) => res,
                        };
                        let h = pv.handshake;

                        // Chain validation verdict — see the kernel-TCP
                        // sender. A divergent replica gets a HashMismatch
                        // frame (below) and the snapshot-resync route on
                        // this same connection.
                        let divergent = match res {
                            Ok(HandshakeValidation::Ok) => false,
                            Ok(HandshakeValidation::Divergent(kind)) => {
                                // Alertable on /metrics: divergence
                                // outside an expected failover rejoin
                                // means corruption or a serious bug.
                                metrics.divergence_total.fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    slot = slot_idx,
                                    last_sequence = h.last_sequence,
                                    ?kind,
                                    "replica journal divergent at handshake — \
                                     routing through snapshot resync"
                                );
                                true
                            }
                            Err(e) => {
                                warn!(slot = slot_idx, error = %e, "handshake validation failed — disconnecting");
                                slot.go_idle(
                                    slot_idx,
                                    transport,
                                    cursors,
                                    metrics,
                                    replicas_connected,
                                );
                                continue;
                            }
                        };
                        // Cursor/stream floor: a divergent replica's
                        // claimed position is meaningless (see the
                        // kernel-TCP sender) — seed from 0 like a fresh
                        // replica.
                        let stream_base = if divergent { 0 } else { h.last_sequence };

                        // Probe whether journal catch-up is possible.
                        let can_catch_up = if divergent {
                            false
                        } else {
                            match can_catch_up_from_journal(journal_path, h.last_sequence) {
                                Ok(v) => v,
                                Err(e) => {
                                    warn!(slot = slot_idx, error = %e, "catch-up probe failed — disconnecting");
                                    slot.go_idle(
                                        slot_idx,
                                        transport,
                                        cursors,
                                        metrics,
                                        replicas_connected,
                                    );
                                    continue;
                                }
                            }
                        };

                        // DPDK publisher: queue_send + poll to keep
                        // smoltcp timers alive during bulk transfer.
                        let mut dpdk_publish = |buf: &[u8]| -> std::io::Result<()> {
                            loop {
                                if transport.queue_send(handle, buf) {
                                    break;
                                }
                                transport.poll();
                                if !transport.is_active(handle) {
                                    return Err(std::io::Error::other(
                                        "replica disconnected during send (TX backpressure)",
                                    ));
                                }
                            }
                            transport.poll();
                            Ok(())
                        };

                        // Highest sequence streamed during catch-up /
                        // snapshot transfer — monotonic from the
                        // stream floor. Seeds the slot's sent
                        // high-water mark (heartbeats + ack-sanity
                        // bound) below.
                        let mut catchup_end = stream_base;
                        let catchup_err = if can_catch_up {
                            slot.send_buf.clear();
                            melin_transport_core::replication::catchup::lineage_origin(journal_path)
                                .and_then(|(lineage_start, lineage_anchor)| {
                                    encode_stream_start(
                                        h.last_sequence,
                                        lineage_start,
                                        lineage_anchor,
                                        fence_state.epoch(),
                                        &mut slot.send_buf,
                                    );
                                    dpdk_publish(&slot.send_buf)
                                })
                                .and_then(|()| {
                                    match catch_up_from_journal_with::<A::Event>(
                                        journal_path,
                                        h.last_sequence,
                                        &mut dpdk_publish,
                                        shutdown,
                                    )? {
                                        CatchUpResult::Ok(end) => {
                                            catchup_end = end;
                                            Ok(())
                                        }
                                        CatchUpResult::NeedSnapshot => Err(io::Error::other(
                                            "catch-up failed unexpectedly after probe",
                                        )),
                                    }
                                })
                                .err()
                        } else {
                            // Resync verdict precedes the snapshot
                            // data — `HashMismatch` makes the
                            // replica archive its local lineage;
                            // plain `NeedSnapshot` is the
                            // too-far-behind rebase. The receiver
                            // expects `SnapshotBegin` as the very
                            // next frame after the verdict.
                            slot.send_buf.clear();
                            if divergent {
                                encode_hash_mismatch(&mut slot.send_buf);
                            } else {
                                encode_need_snapshot(&mut slot.send_buf);
                            }
                            // Pre-flight before the verdict goes on
                            // the wire — see the kernel-TCP sender:
                            // the replica archives its lineage on
                            // receipt, so a snapshot we cannot
                            // produce must fail here, dropping the
                            // connection with the replica's journal
                            // intact.
                            match preflight_snapshot_transfer(journal_path)
                                .and_then(|()| dpdk_publish(&slot.send_buf))
                                .and_then(|()| {
                                    snapshot_transfer_with::<A::Event>(
                                        journal_path,
                                        &mut dpdk_publish,
                                        shutdown,
                                    )
                                }) {
                                Ok(CatchUpResult::Ok(end)) => {
                                    catchup_end = end;
                                    None
                                }
                                Ok(CatchUpResult::NeedSnapshot) => Some(io::Error::other(
                                    "catch-up failed even after snapshot transfer",
                                )),
                                Err(e) => Some(e),
                            }
                        };

                        if let Some(e) = catchup_err {
                            warn!(slot = slot_idx, error = %e, "catch-up/snapshot failed — disconnecting");
                            slot.go_idle(slot_idx, transport, cursors, metrics, replicas_connected);
                            continue;
                        }

                        // Engage this slot's cursors and seed the gauge
                        // pair BEFORE the bridge flips active so a reader
                        // that observes active=true also observes a
                        // non-zero cursor pair — see `ReplicaCursors` for
                        // the ordering contract.
                        cursors.seed_on_handshake(slot_idx, stream_base);

                        // Bridge into live streaming: activates the
                        // ring, re-reads from the journal the entries
                        // that fell into the activation window, then
                        // drains the ring into sequence-contiguity
                        // (back-filling from disk if a skipped entry
                        // hasn't flushed yet) before going live. The
                        // bridge closes the catch-up→live gap under load
                        // (the receiver's contiguity gate backstops only
                        // the rare quiescent corner) — see
                        // `bridge_catchup_to_live`.
                        // Forwards via the retrying DPDK publisher — the
                        // drain may leave bytes in the TX queue, so the
                        // previous fire-and-forget `queue_send` would
                        // silently drop chunks here. Returns the slot's
                        // sent high-water (heartbeats + ack-sanity bound).
                        match bridge_catchup_to_live::<A::Event>(
                            journal_path,
                            stream_base,
                            catchup_end,
                            &slot.active_flag,
                            &mut slot.consumer,
                            &mut dpdk_publish,
                            shutdown,
                        ) {
                            Ok(sent) => slot.sent = sent,
                            Err(e) => {
                                warn!(slot = slot_idx, error = %e, "catch-up handoff failed — disconnecting");
                                slot.go_idle(
                                    slot_idx,
                                    transport,
                                    cursors,
                                    metrics,
                                    replicas_connected,
                                );
                                continue;
                            }
                        }
                        slot.last_send = std::time::Instant::now();

                        replica_ready.store(true, Ordering::Release);
                        metrics.catching_up[slot_idx].store(false, Ordering::Relaxed);
                        slot.state = SlotState::Streaming(handle);
                        continue;
                    }

                    // Try to read handshake frame.
                    transport.recv_into_vec(handle, &mut slot.recv_buf);

                    match try_extract_frame(&slot.recv_buf, MAX_CONTROL_FRAME) {
                        FrameResult::Complete(payload_start, frame_end) => {
                            let payload = &slot.recv_buf[payload_start..frame_end];
                            match decode_replica_message(payload) {
                                Ok(ReplicaMessage::Handshake(h)) => {
                                    info!(
                                        slot = slot_idx,
                                        last_sequence = h.last_sequence,
                                        replica_epoch = h.epoch,
                                        "replica handshake received (DPDK)"
                                    );

                                    // Fence: a replica with a higher epoch
                                    // means we are a superseded ex-primary —
                                    // self-demote and drop the connection.
                                    // Policy (latch + shutdown) lives on
                                    // `FenceState`; see the kernel-TCP
                                    // sender for the rationale.
                                    let our_epoch = fence_state.epoch();
                                    if let Some(first_latch) =
                                        fence_state.fence_if_superseded(h.epoch, shutdown)
                                    {
                                        if first_latch {
                                            error!(
                                                slot = slot_idx,
                                                replica_epoch = h.epoch,
                                                our_epoch,
                                                "fenced: a replica advertises a higher epoch — \
                                                 this primary has been superseded; self-demoting \
                                                 and shutting down (DPDK)"
                                            );
                                        }
                                        slot.go_idle(
                                            slot_idx,
                                            transport,
                                            cursors,
                                            metrics,
                                            replicas_connected,
                                        );
                                        continue;
                                    }

                                    metrics.catching_up[slot_idx].store(true, Ordering::Relaxed);

                                    // The handshake frame is consumed here;
                                    // chain validation runs on a short-lived
                                    // thread (it can scan a full segment per
                                    // attempt and sleeps between retries —
                                    // blocking tick() would freeze client
                                    // traffic and the other slot). The slot
                                    // stays Handshaking; the verdict is picked
                                    // up by the pending_validation poll at the
                                    // top of this arm.
                                    compact_recv_buf(&mut slot.recv_buf, frame_end);
                                    let (verdict_tx, verdict_rx) = std::sync::mpsc::channel();
                                    let validate_path = journal_path.clone();
                                    let validate_hs = h.clone();
                                    match std::thread::Builder::new()
                                        .name(format!("repl-validate-{slot_idx}"))
                                        .spawn(move || {
                                            // Send failure means the slot was
                                            // torn down while validating —
                                            // nobody is waiting for the verdict.
                                            let _ = verdict_tx.send(
                                                validate_replica_handshake_settled(
                                                    &validate_path,
                                                    &validate_hs,
                                                ),
                                            );
                                        }) {
                                        Ok(_detached) => {
                                            slot.pending_validation = Some(PendingValidation {
                                                handshake: h,
                                                verdict_rx,
                                            });
                                        }
                                        Err(e) => {
                                            warn!(slot = slot_idx, error = %e, "failed to spawn handshake validation thread — disconnecting");
                                            slot.go_idle(
                                                slot_idx,
                                                transport,
                                                cursors,
                                                metrics,
                                                replicas_connected,
                                            );
                                        }
                                    }
                                }
                                Ok(ReplicaMessage::Ack(_)) => {
                                    warn!(
                                        slot = slot_idx,
                                        "expected Handshake, got Ack — disconnecting"
                                    );
                                    slot.go_idle(
                                        slot_idx,
                                        transport,
                                        cursors,
                                        metrics,
                                        replicas_connected,
                                    );
                                }
                                Err(e) => {
                                    warn!(slot = slot_idx, error = %e, "failed to decode handshake — disconnecting");
                                    slot.go_idle(
                                        slot_idx,
                                        transport,
                                        cursors,
                                        metrics,
                                        replicas_connected,
                                    );
                                }
                            }
                        }
                        FrameResult::Oversized => {
                            warn!(slot = slot_idx, "oversized handshake frame — disconnecting");
                            slot.go_idle(slot_idx, transport, cursors, metrics, replicas_connected);
                        }
                        FrameResult::Incomplete => {} // Wait for more data.
                    }
                }

                SlotState::Streaming(handle) => {
                    any_active = true;

                    // 1. Process acks (non-blocking).
                    transport.recv_into_vec(handle, &mut slot.recv_buf);
                    let mut consumed = 0;
                    let mut ack_error = false;
                    loop {
                        let remaining = &slot.recv_buf[consumed..];
                        match try_extract_frame(remaining, MAX_CONTROL_FRAME) {
                            FrameResult::Complete(payload_start, frame_end) => {
                                let payload = &remaining[payload_start..frame_end];
                                if let Ok(ReplicaMessage::Ack(ack)) =
                                    decode_replica_message(payload)
                                    && cursors.record_ack(slot_idx, &ack, slot.sent.get()).is_err()
                                {
                                    // Eviction on violation: reuse the ack-error
                                    // teardown below (the store already logged
                                    // the violation at error level).
                                    ack_error = true;
                                    break;
                                }
                                consumed += frame_end;
                            }
                            FrameResult::Oversized => {
                                warn!(
                                    slot = slot_idx,
                                    "oversized ack frame from replica — disconnecting"
                                );
                                ack_error = true;
                                break;
                            }
                            FrameResult::Incomplete => break,
                        }
                    }
                    compact_recv_buf(&mut slot.recv_buf, consumed);
                    if ack_error {
                        slot.go_idle(slot_idx, transport, cursors, metrics, replicas_connected);
                        continue;
                    }

                    // 2. Send data batches. Ring chunks are wire-ready
                    //    `InputBatch` frames produced by the journal stage
                    //    — the sender is a passthrough. Pre-check the
                    //    per-socket TX queue: if we commit a batch from
                    //    the ring but `queue_send` rejects it (TX full),
                    //    the data is gone from the ring without ever
                    //    reaching the replica — replica never acks,
                    //    replication_cursor stalls, and the response
                    //    gate freezes the whole exchange. We saw this
                    //    exact symptom on dpdk-dual-repl.
                    let max_tx = transport.max_tx_queue_size(handle);
                    let used = transport.tx_queue_bytes(handle);
                    let mut available = max_tx.saturating_sub(used);

                    slot.send_buf.clear();
                    let mut batches_sent = 0;
                    let mut tx_overflow = false;
                    while batches_sent < batch_size {
                        let Some((meta, data)) = slot.consumer.try_read() else {
                            break;
                        };
                        let data_len = data.len();
                        if data_len > available {
                            // Batch doesn't fit — flush what we have so
                            // far, poll to drain the wire, then re-check
                            // with a clean slate.
                            if !slot.send_buf.is_empty() {
                                metrics.bytes_sent[slot_idx]
                                    .fetch_add(slot.send_buf.len() as u64, Ordering::Relaxed);
                                if !transport.queue_send(handle, &slot.send_buf) {
                                    slot.send_buf.clear();
                                    tx_overflow = true;
                                    break;
                                }
                                slot.send_buf.clear();
                            }
                            transport.poll();
                            let used = transport.tx_queue_bytes(handle);
                            available = max_tx.saturating_sub(used);
                            if data_len > available {
                                break;
                            }
                        }
                        slot.send_buf.extend_from_slice(data);
                        slot.consumer.commit();
                        slot.sent.advance(meta.end_sequence);
                        batches_sent += 1;
                        available = available.saturating_sub(data_len);
                    }

                    if !tx_overflow && !slot.send_buf.is_empty() {
                        metrics.bytes_sent[slot_idx]
                            .fetch_add(slot.send_buf.len() as u64, Ordering::Relaxed);
                        if !transport.queue_send(handle, &slot.send_buf) {
                            tx_overflow = true;
                        }
                    }
                    if tx_overflow {
                        slot.send_buf.clear();
                        warn!(
                            slot = slot_idx,
                            "TX overflow on replica socket — disconnecting"
                        );
                        slot.go_idle(slot_idx, transport, cursors, metrics, replicas_connected);
                        continue;
                    }
                    if !slot.send_buf.is_empty() {
                        // Flush immediately so replication data hits the
                        // wire without waiting for the next outer-loop
                        // poll. Without this, a client-traffic burst
                        // starves the replication TX path: the TxQueue
                        // fills, the driver backs off, the ring overflows,
                        // and the response gate freezes the exchange.
                        transport.poll();
                        slot.last_send = std::time::Instant::now();
                    }

                    // 3. Heartbeat if idle.
                    if batches_sent == 0 && slot.last_send.elapsed() >= heartbeat_interval {
                        slot.send_buf.clear();
                        encode_heartbeat(slot.sent.get(), &mut slot.send_buf);
                        transport.queue_send(handle, &slot.send_buf);
                        slot.last_send = std::time::Instant::now();
                    }

                    // 4. Check for disconnect.
                    if !transport.is_active(handle) {
                        warn!(slot = slot_idx, "replica disconnected (DPDK)");
                        slot.go_idle(slot_idx, transport, cursors, metrics, replicas_connected);
                        continue;
                    }

                    // Eviction is handled by the journal-stage evict_flag check
                    // at the top of the loop (lines 3254+). No timeout-based
                    // eviction here — try_read() returning None means the
                    // consumer caught up, not that it's slow.
                }
            }
        }

        any_active
    }
}

/// DPDK variant of the replication receiver. Uses a `DpdkTransport` (smoltcp)
/// to connect to the primary via DPDK instead of kernel TCP.
///
/// Includes reconnection with exponential backoff (1s → 30s) and snapshot
/// transfer support — matching the TCP receiver's feature set.
///
/// The protocol is identical to `run_receiver` — same wire format, same
/// fsync-then-ack-then-replay pattern. Only the I/O primitives differ.
#[allow(clippy::too_many_arguments)]
pub fn run_receiver_dpdk<A, W>(
    mut transport: melin_dpdk::DpdkTransport,
    primary_ip: std::net::Ipv4Addr,
    primary_port: u16,
    signing_key: &ed25519_dalek::SigningKey,
    journal_path: &std::path::Path,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_ms: u64,
    snapshot_path: std::path::PathBuf,
    cores: crate::server::PipelineCores,
    group_commit_delay: std::time::Duration,
    pipeline_depth: usize,
    busy_spin: bool,
    // Application factory: see the kernel-TCP `run_receiver` for the
    // shape and rationale. Carries operator policy (rate limits, caps,
    // ...) alongside the empty-app constructor.
    factory: std::sync::Arc<dyn melin_app::app_factory::AppFactory<App = A>>,
    fence_state: Arc<melin_transport_core::fence::FenceState>,
) -> ReceiverResult<A, W>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
    JournalStage<A::Event, W>: JournalStageRun<A::Event, Writer = W>,
{
    // Recover local state from journal whenever any segment survives —
    // live OR archived; fresh replicas get `(None, None, 0, zeros)`.
    // See `recover_replica_state` for the lineage rules.
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        recover_replica_state::<A, W>(
            journal_path,
            &snapshot_path,
            factory.as_ref(),
            &fence_state,
        )?;

    // Exponential backoff for reconnection: 1s → 2s → 4s → … → 30s max.
    // Reset to 1s on successful streaming (first InputBatch received).
    let mut backoff = std::time::Duration::from_secs(1);

    // Mid-stream divergence resyncs this process has attempted — see
    // `MAX_INPROCESS_DIVERGENCE_RESYNCS`.
    let mut divergence_resyncs: u32 = 0;

    // Reusable buffers — survive across reconnections.
    let mut send_buf = Vec::with_capacity(64);
    let mut recv_buf: Vec<u8> = Vec::with_capacity(4096);
    // Ephemeral port counter for outbound connections. Each reconnect uses
    // a different local port to avoid smoltcp TIME_WAIT conflicts.
    let mut local_port: u16 = 40000;

    // Live pipeline state — built once on first connect (or after a snapshot
    // transfer), persists across `Disconnected` reconnects so we don't pay
    // the journal-recover + thread-spawn + warm-up cost on every drop.
    // None = no pipeline yet (first iteration, or just torn down for
    // snapshot transfer); Some = running pipeline with threads + atomics
    // we can read for the next reconnect handshake.
    let mut pipeline: Option<ReplicaPipelineHandles<A, W>> = None;

    // --- Outer reconnect loop ---
    //
    // Each iteration: connect → handshake → (snapshot rebuild?) →
    // (build pipeline if absent) → stream. On disconnect the pipeline
    // stays live — we just refresh handshake state from its atomics and
    // reconnect. Only `Promote` / `Shutdown` / snapshot-transfer / fatal
    // error tear it down.
    loop {
        // Refresh handshake state from the running pipeline, if any.
        // The (last_sequence, chain_hash) pair must come from ONE
        // FsyncState snapshot — a torn pair read from two sources while
        // the journal stage keeps flushing trips the primary's
        // handshake chain validation (false divergence; see the
        // kernel-TCP receiver).
        if let Some(p) = pipeline.as_ref() {
            if let Some(ref lock) = p.chain_hash_lock {
                let fsync_state = lock.load();
                last_sequence = fsync_state.journal_seq.get();
                chain_hash = fsync_state.chain_hash;
            } else {
                last_sequence = p.last_seq.load().get();
            }
        }

        if shutdown.load(Ordering::Relaxed) {
            if let Some(p) = pipeline.take() {
                let _ = teardown_replica_pipeline::<A, W>(p);
            }
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered while disconnected");
            return take_pipeline_for_promotion(&mut pipeline, &mut exchange, &mut journal_writer);
        }

        info!(
            primary_ip = %primary_ip,
            primary_port,
            "connecting to primary as replica (DPDK)"
        );

        // Seed the primary's MAC into smoltcp's neighbor cache. Without
        // this, smoltcp emits a broadcast ARP on connect which the SR-IOV
        // PF silently drops, and the SYN never goes out — the replica
        // spins on "failed to connect (DPDK)" forever. VF MACs follow the
        // 02:00:<IP-bytes> convention set by dpdk-setup.sh, matching
        // what the bench client does on its outbound connect.
        let primary_mac = [
            0x02,
            0x00,
            primary_ip.octets()[0],
            primary_ip.octets()[1],
            primary_ip.octets()[2],
            primary_ip.octets()[3],
        ];
        transport.seed_neighbor(primary_ip, primary_mac);
        // Drain the injected ARP reply through smoltcp so the neighbor
        // cache is populated BEFORE connect_to() runs. Without this poll
        // smoltcp's connect tries to resolve ARP from an empty cache,
        // queues a broadcast request that the PF drops, and the SYN
        // never ships.
        transport.poll();

        // Connect to primary via smoltcp.
        // Replication streams large journal batches; use bigger RX buffer
        // so smoltcp can advertise a window large enough to sustain throughput.
        const REPL_RX_BUF: usize = 512 * 1024;
        const REPL_TX_BUF: usize = 64 * 1024;
        const REPL_TX_QUEUE: usize = 64 * 1024;
        let handle = transport.connect_to_with_buffers(
            primary_ip,
            primary_port,
            local_port,
            REPL_RX_BUF,
            REPL_TX_BUF,
            REPL_TX_QUEUE,
        );
        local_port = local_port.wrapping_add(1).max(40000);

        // Poll until TCP handshake completes (with timeout).
        let connect_start = std::time::Instant::now();
        const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let connected = loop {
            if shutdown.load(Ordering::Relaxed) {
                return Ok(None);
            }
            transport.poll();
            if transport.is_connected(handle) {
                break true;
            }
            if !transport.is_active(handle) || connect_start.elapsed() >= CONNECT_TIMEOUT {
                break false;
            }
            std::thread::yield_now();
        };

        if !connected {
            warn!(
                backoff_secs = backoff.as_secs(),
                "failed to connect to primary (DPDK) — retrying"
            );
            transport.close(handle);
            sleep_checking_flags(backoff, shutdown, promote);
            if shutdown.load(Ordering::Relaxed) {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Ok(None);
            }
            if promote.load(Ordering::Acquire) {
                info!("promotion triggered during reconnect backoff");
                return take_pipeline_for_promotion(
                    &mut pipeline,
                    &mut exchange,
                    &mut journal_writer,
                );
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        info!("connected to primary (DPDK)");

        // Authenticate BEFORE the handshake — mirrors the kernel-TCP receiver.
        // The primary issues a Challenge first and will not accept our
        // Handshake until it verifies our signature. Runs the SHARED
        // `authenticate_with_primary` over a blocking adapter that drives the
        // smoltcp poll loop, so TCP and DPDK can never diverge on the auth flow.
        recv_buf.clear();
        let auth_result = {
            let mut auth_stream = PolledAuthStream {
                transport: &mut transport,
                handle,
                recv_buf: &mut recv_buf,
                shutdown,
                deadline: std::time::Instant::now() + AUTH_TIMEOUT,
            };
            authenticate_with_primary(&mut auth_stream, signing_key)
        };
        if let Err(e) = auth_result {
            transport.close(handle);
            // A shutdown racing the auth window surfaces here as an auth error
            // — that is a clean exit, not degraded operation, so don't warn
            // (the loop falls through to the shutdown check after the backoff
            // sleep). Promotion never aborts the auth stream, so it can't reach
            // this path spuriously; only shutdown can.
            if shutdown.load(Ordering::Relaxed) {
                debug!("authentication aborted — shutting down (DPDK)");
            } else {
                warn!(
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "authentication failed (DPDK) — retrying"
                );
            }
            sleep_checking_flags(backoff, shutdown, promote);
            if shutdown.load(Ordering::Relaxed) {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Ok(None);
            }
            if promote.load(Ordering::Acquire) {
                info!("promotion triggered during reconnect backoff");
                return take_pipeline_for_promotion(
                    &mut pipeline,
                    &mut exchange,
                    &mut journal_writer,
                );
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        info!("authenticated with primary (DPDK)");

        // Send handshake. Advertise our fencing epoch so a stale primary
        // self-demotes when it sees we are ahead (see `crate::fence`).
        send_buf.clear();
        let handshake = Handshake {
            last_sequence,
            chain_hash,
            epoch: fence_state.epoch(),
        };
        encode_handshake(&handshake, &mut send_buf);
        transport.queue_send(handle, &send_buf);
        send_buf.clear();

        // Read protocol response (StreamStart / NeedSnapshot / HashMismatch).
        // Helper macro: shut the pipeline down before bubbling up a fatal
        // error from the handshake. Borrows `pipeline` directly so we don't
        // leak the threads on the way out.
        macro_rules! fatal_err_dpdk {
            ($msg:expr) => {{
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Err($msg);
            }};
        }
        recv_buf.clear();
        // `None` from the loop = failure path (disconnect or snapshot
        // error) → reconnect. `Some(lineage)` = StreamStart received (or a
        // resync that re-seeded and validated its own post-snapshot
        // StreamStart inline, via `handle_resync_verdict`).
        let stream_lineage: Option<(u64, [u8; 32])> = 'handshake: loop {
            if shutdown.load(Ordering::Relaxed) {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Ok(None);
            }
            transport.poll();
            transport.recv_into_vec(handle, &mut recv_buf);

            match try_extract_frame(&recv_buf, MAX_CONTROL_FRAME) {
                FrameResult::Complete(payload_start, frame_end) => {
                    let payload = &recv_buf[payload_start..frame_end];
                    let response = decode_primary_message(payload)?;
                    compact_recv_buf(&mut recv_buf, frame_end);
                    match response {
                        PrimaryMessage::StreamStart {
                            start_sequence,
                            segment_start_sequence,
                            anchor_hash,
                            epoch,
                        } => {
                            // Fence: refuse a primary behind our epoch — its
                            // divergent lineage must not overwrite our more
                            // current state. Mirrors the kernel-TCP receiver.
                            // (A resync rebase adopts the primary's epoch
                            // inside `handle_resync_verdict` instead.)
                            let our_epoch = fence_state.epoch();
                            if fence_state.refuses_primary(epoch) {
                                warn!(
                                    primary_epoch = epoch,
                                    our_epoch,
                                    "primary is behind our fencing epoch — refusing to follow \
                                     stale primary (DPDK)"
                                );
                                backoff = (backoff * 2).min(MAX_BACKOFF);
                                sleep_checking_flags(backoff, shutdown, promote);
                                break 'handshake None; // caught by the None check below
                            }
                            fence_state.observe_epoch(epoch);
                            info!(start_sequence, epoch, "streaming started (DPDK)");
                            break 'handshake Some((segment_start_sequence, anchor_hash));
                        }
                        ref resync @ (PrimaryMessage::NeedSnapshot
                        | PrimaryMessage::HashMismatch) => {
                            let divergent = matches!(resync, PrimaryMessage::HashMismatch);
                            let decision = handle_resync_verdict(
                                divergent,
                                &mut DpdkFrameSource {
                                    transport: &mut transport,
                                    handle,
                                    recv_buf: &mut recv_buf,
                                    shutdown,
                                },
                                &mut pipeline,
                                &mut exchange,
                                &mut journal_writer,
                                journal_path,
                                &snapshot_path,
                                &fence_state,
                                &mut last_sequence,
                                &mut chain_hash,
                            );
                            match decision {
                                Ok(ResyncDecision::Ready {
                                    segment_start_sequence,
                                    anchor_hash,
                                    resume_sequence,
                                }) => {
                                    // DPDK resumes streaming from `last_sequence`
                                    // (the TCP path uses a separate `session_start`).
                                    last_sequence = resume_sequence;
                                    break 'handshake Some((segment_start_sequence, anchor_hash));
                                }
                                Ok(ResyncDecision::Retry) => {
                                    transport.close(handle);
                                    sleep_checking_flags(backoff, shutdown, promote);
                                    backoff = (backoff * 2).min(MAX_BACKOFF);
                                    break 'handshake None; // caught by the None check below
                                }
                                Err(e) => fatal_err_dpdk!(e),
                            }
                        }
                        other => {
                            fatal_err_dpdk!(format!("unexpected response: {other:?}").into());
                        }
                    }
                }
                FrameResult::Oversized => {
                    fatal_err_dpdk!("oversized frame from primary during handshake".into());
                }
                FrameResult::Incomplete => {}
            }

            if !transport.is_active(handle) {
                warn!("disconnected from primary during handshake (DPDK)");
                transport.close(handle);
                sleep_checking_flags(backoff, shutdown, promote);
                backoff = (backoff * 2).min(MAX_BACKOFF);
                break None; // trigger reconnect via the None check below
            }
            std::thread::yield_now();
        };

        // `None` means the handshake loop exited via a failure path
        // (disconnect or snapshot error) — reconnect.
        let Some((lineage_start, lineage_anchor)) = stream_lineage else {
            continue;
        };

        // Create journal for fresh replica (first connection only).
        //
        // Gate on `pipeline.is_none()` rather than `journal_writer.is_none()`:
        // the writer is moved into the pipeline on first connect and never
        // returned, so on every subsequent reconnect `journal_writer` is
        // `None` even though a live writer is still mid-stream inside the
        // pipeline. `pipeline.is_none()` distinguishes "true first connect or
        // post-snapshot rebuild" from "reconnect against an existing
        // pipeline" — the latter must not recreate the journal file.
        //
        // The StreamStart lineage carries the segment header identity
        // (starting sequence + chain anchor); creating the local segment
        // from the same identity makes the replica's segment
        // byte-identical to the primary's, and adopted `Rotate`
        // boundaries keep it that way across rotations (bitwise mirror).
        if pipeline.is_none() && journal_writer.is_none() {
            let writer = W::create_continuing(journal_path, lineage_start, lineage_anchor)?;
            let mut fresh = factory.empty();
            factory.apply_operator_policy(&mut fresh);
            exchange = Some(fresh);
            journal_writer = Some(writer);
        }

        // --- Build pipeline if absent ---
        //
        // Built once on first connect, or after a snapshot transfer tore
        // the previous one down. On disconnect the pipeline lives, so
        // this branch is skipped.
        if pipeline.is_none() {
            // If we still have no state after all the handshake logic, reconnect.
            if exchange.is_none() || journal_writer.is_none() {
                continue;
            }
            let cur_exchange = exchange.take().expect("exchange initialized");
            let cur_writer = journal_writer.take().expect("journal_writer initialized");

            // Unpin before spawning the pipeline. Same rationale as the
            // kernel-TCP receiver: on post-snapshot rebuilds this thread is
            // already pinned to `cores.reader`, so children would inherit that
            // affinity mask (and, on an isolated core, the SCHED_FIFO priority
            // `pin_to_core` granted there) and never preempt the busy-spinning
            // receiver to reach their own self-pin.
            if let Err(e) = melin_app::affinity::clear_affinity() {
                tracing::warn!(error = e, "failed to clear receiver affinity before spawn");
            }

            pipeline = Some(build_replica_pipeline_with_threads::<A, W>(
                cur_exchange,
                cur_writer,
                cores,
                snapshot_interval_ms,
                snapshot_path.clone(),
                group_commit_delay,
                busy_spin,
                Arc::clone(&fence_state),
            )?);

            // Pipeline children are spawned and self-pinned. Now safe to
            // pin the receive thread — mirrors the primary's reader pin
            // so the thread producing input-ring entries from the network
            // isn't migrated across L3s mid-batch.
            melin_app::affinity::pin_thread("receiver", cores.reader);
        }

        // --- Streaming session (transport-agnostic) ---
        let result = {
            let p = pipeline.as_mut().expect("pipeline must exist by here");
            let input_producer = &mut p.input_producer;
            let journal_cursor = p.journal_cursor.as_ref();
            let stream_marks = &p.stream_marks;
            let journal_failed = &p.journal_failed;
            let mut dpdk_transport = DpdkReceiverTransport {
                transport: &mut transport,
                handle,
                send_buf: std::mem::take(&mut send_buf),
            };
            // `last_sequence` is the session's resume point — updated to
            // the snapshot sequence after a transfer, so it anchors the
            // contiguity gate on both negotiation paths.
            let r = streaming_loop::<DpdkReceiverTransport<'_>, A::Event>(
                &mut dpdk_transport,
                input_producer,
                journal_cursor,
                shutdown,
                promote,
                pipeline_depth,
                busy_spin,
                last_sequence,
                std::mem::take(&mut recv_buf),
                None,
                stream_marks,
                journal_failed,
            );
            send_buf = dpdk_transport.send_buf;
            r
        };

        match handle_session_exit(
            result,
            &mut pipeline,
            &mut divergence_resyncs,
            &mut backoff,
            last_sequence,
            journal_path,
            &snapshot_path,
            factory.as_ref(),
            &fence_state,
            shutdown,
            promote,
            // Close the smoltcp session before any reconnect. The TCP
            // connection to the primary may still be healthy (a locally
            // detected divergence, or a half-open drop), so without an
            // explicit FIN the primary's slot stays occupied — and the
            // DPDK primary has no timeout eviction, so a repair handshake
            // could be refused indefinitely. Closing also returns the
            // socket entry to the socket set; each reconnect allocates a
            // fresh one, so skipping it leaks one entry per disconnect.
            || transport.close(handle),
        ) {
            AfterSession::Return(r) => return r,
            AfterSession::Resync {
                exchange: ex,
                journal_writer: wr,
                last_sequence: seq,
                chain_hash: hash,
            } => {
                exchange = ex;
                journal_writer = wr;
                last_sequence = seq;
                chain_hash = hash;
                continue;
            }
            AfterSession::Reconnect => {}
        }
    }
}

/// DPDK control-frame source: polls the smoltcp transport and extracts
/// one length-prefixed frame per call. Drives the shared
/// [`receive_chunked_body`] and the resync prologue reads
/// (`SnapshotBegin` / `SegmentSeedBegin`). See [`ControlFrameSource`].
struct DpdkFrameSource<'a> {
    transport: &'a mut melin_dpdk::DpdkTransport,
    handle: melin_dpdk::SocketHandle,
    recv_buf: &'a mut Vec<u8>,
    shutdown: &'a AtomicBool,
}

impl ControlFrameSource for DpdkFrameSource<'_> {
    fn next_frame(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                return Err("shutdown during transfer".into());
            }
            self.transport.poll();
            self.transport.recv_into_vec(self.handle, self.recv_buf);

            match try_extract_frame(self.recv_buf, max_size) {
                FrameResult::Complete(payload_start, frame_end) => {
                    let payload = self.recv_buf[payload_start..frame_end].to_vec();
                    compact_recv_buf(self.recv_buf, frame_end);
                    return Ok(payload);
                }
                FrameResult::Oversized => {
                    return Err("oversized frame during transfer".into());
                }
                FrameResult::Incomplete => {}
            }

            // A frame arriving in the same poll as the FIN is returned
            // above before we observe the disconnect here.
            if !self.transport.is_active(self.handle) {
                return Err("disconnected during transfer".into());
            }
            std::thread::yield_now();
        }
    }
}
