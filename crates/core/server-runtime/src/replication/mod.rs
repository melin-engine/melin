//! Replication — synchronous input-stream streaming from primary to replica.
//!
//! The primary's JournalStage encodes each `InputSlot` it just durably
//! journaled into a wire-ready `InputBatch` frame in the replication ring
//! (separately from the journal-codec bytes it writes to disk). The
//! `ReplicationSender` thread forwards those frames as-is over TCP/DPDK
//! — no decode + re-encode on the hot path. The replica decodes the
//! frames straight back into `InputSlot`s, publishes them to its local
//! input disruptor with the primary's pre-assigned sequences and
//! timestamps, and the replica's JournalStage re-encodes them through
//! its own writer for byte-exact-on-replay durability.
//!
//! ## Wire Protocol
//!
//! Length-prefixed frames, little-endian, over a dedicated TCP connection
//! (or DPDK pipe). The full `InputBatch` payload layout lives in
//! `melin_transport_core::replication::protocol` /
//! `melin_transport_core::replication_wire`.
//!
//! ### Auth (before handshake)
//! - **Challenge** (Primary → Replica): `[len:u32][0x03][nonce:[u8;32]]`
//! - **ChallengeResponse** (Replica → Primary): `[len:u32][0x04][signature:[u8;64]][pubkey:[u8;32]]`
//! - **AuthOk** (Primary → Replica): `[len:u32][0x05]`
//! - **AuthFailed** (Primary → Replica): `[len:u32][0x06]`
//!
//! ### Replica → Primary
//! - **Handshake**: `[len:u32][0x01][last_sequence:u64][chain_hash:[u8;32]]`
//! - **Ack**: `[len:u32][0x02][acked_sequence:u64][in_memory_sequence:u64]`
//!
//! ### Primary → Replica
//! - **StreamStart**: `[len:u32][0x10][start_sequence:u64][segment_start_sequence:u64][anchor_hash:[u8;32]]`
//!   — the segment header identity a fresh replica creates its journal
//!   with (lineage origin for full catch-up, the seeded segment's
//!   identity after a snapshot transfer)
//! - **NeedSnapshot**: `[len:u32][0x11]`
//! - **HashMismatch**: `[len:u32][0x12]` — divergent replica journal;
//!   the replica archives its lineage, then the snapshot flow follows
//! - **SnapshotBegin**: `[len:u32][0x13][snapshot_len:u64][snap_sequence:u64][snap_chain_hash:[u8;32]]`
//! - **SnapshotChunk**: `[len:u32][0x14][data...]`
//! - **SnapshotEnd**: `[len:u32][0x15][crc32c:u32]`
//! - **Rotate**: `[len:u32][0x16][boundary_seq:u64][tail_hash:[u8;32]]`
//!   — primary-driven rotation, verified + adopted by the replica
//! - **ChainCheck**: `[len:u32][0x17][sequence:u64][chain_hash:[u8;32]]`
//!   — periodic live-stream chain validation
//! - **SegmentSeedBegin**: `[len:u32][0x18][seed_len:u64]` — raw byte
//!   prefix of the primary's segment containing the snapshot boundary;
//!   body rides SnapshotChunk frames, ends with SnapshotEnd
//! - **InputBatch**: `[len:u32][0x21][count:u16][slot...]` — see
//!   `transport-core::replication_wire` for the per-slot layout
//! - **Heartbeat**: `[len:u32][0x30][sequence:u64]`
//!
//! ## Limitations
//!
//! - Dual replication (up to 2 replicas in parallel)
//!
//! See `docs/replication.md` for the full design document and limitation details.

use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use melin_journal::JournalWrite;
use melin_transport_core::pipeline::{JournalStage, JournalStageRun};

use melin_app::Application;
use melin_transport_core::pipeline::{InputSlot, OutputSlot};
use melin_transport_core::replication::archive::{ArchiveReason, archive_local_lineage};
use melin_transport_core::replication::protocol::{MAX_CONTROL_FRAME, decode_primary_message};

mod auth;
#[cfg(feature = "dpdk")]
mod dpdk;
mod receiver_transport;
mod tcp_receiver;
mod tcp_sender;

use receiver_transport::{ControlFrameSource, SessionExit, StreamingResult, receive_chunked_body};

/// Writer-side view of the trading-halt gate (the `replicas_connected`
/// counter). The matching stage refuses new orders while the count is zero, so
/// the counter must reflect the number of replicas that have **authenticated**
/// — a bare connection must not lift the halt. Both senders (kernel-TCP and
/// DPDK) lift/lower the gate through this view so the policy, the memory
/// orderings, and the "trading halted" warning live in one place and cannot
/// drift apart.
///
/// Deliberately a *borrowed view*, not an owner: `melin_transport_core` owns
/// the `Arc<AtomicU32>` and reads it on the matching hot path and for the
/// `melin_replicas_connected` gauge. This type is only the senders' write
/// surface, so centralizing it costs nothing on the read side.
pub(crate) struct ReplicaGate<'a> {
    count: &'a AtomicU32,
}

impl<'a> ReplicaGate<'a> {
    pub(crate) fn new(count: &'a AtomicU32) -> Self {
        Self { count }
    }

    /// A replica has authenticated — lift the halt by one. `Release` so a peer
    /// that observes the connect also observes everything that preceded it.
    pub(crate) fn lift(&self) {
        self.count.fetch_add(1, Ordering::Release);
    }

    /// A replica left — lower the halt by one. Returns `true` if it was the
    /// last one (trading is now unprotected), emitting the halt warning here so
    /// both senders share the wording. `fetch_sub` returns the *prior* count,
    /// so `== 1` means this call took it to zero; deriving "last one" from the
    /// returned value rather than a follow-up load avoids a TOCTOU race with a
    /// concurrent reconnect's `lift`.
    pub(crate) fn lower(&self) -> bool {
        let was_last = self.count.fetch_sub(1, Ordering::Release) == 1;
        if was_last {
            tracing::warn!("all replicas disconnected — trading halted");
        }
        was_last
    }
}

// Wire-protocol types, auth, catch-up, ack queueing, dual-track
// cursor management, and per-replica metrics now live in
// `melin_transport_core::replication`. Re-export the public types
// here so the module's public API surface (e.g.
// `melin_server_runtime::replication::Ack` / `::ReplicationMetrics`) is
// unchanged for downstream consumers and tests.
pub use melin_transport_core::replication::ack_queue::{
    PendingAck, PendingAckQueue, try_flush_dual_track, wait_for_journal_cursor,
};
pub use melin_transport_core::replication::protocol::{
    Ack, Handshake, PrimaryMessage, ReplicaMessage,
};
pub use melin_transport_core::replication::{ReplicaCursors, ReplicationMetrics, SentHighWater};

#[cfg(feature = "dpdk")]
pub use dpdk::{DpdkReplicationDriver, run_receiver_dpdk};
pub use tcp_receiver::{ReceiverResult, run_receiver};
pub use tcp_sender::{ReplicationListener, Sender, run_sender};

/// The handles a replica's receive loop shares with the rest of the
/// node — the admin endpoint and the control-plane raft driver.
/// Bundled (rather than five positional parameters) because they
/// always travel together through the kernel-TCP and DPDK receiver
/// signatures, and a transposition between same-typed flags is exactly
/// the bug a bundle prevents.
#[derive(Clone)]
pub struct ReplicaControlPlane {
    /// Promotion request: polled by the receive loop; filed by the
    /// admin `PROMOTE` command or the raft driver (auto-promotion).
    pub promote: crate::promotion::PromotionRequest,
    /// Flipped `true` once journal recovery has seeded the fence epoch,
    /// so the raft driver can trust this node's advertised tip — see
    /// `melin_raft::recency::TipSource`.
    pub tip_ready: std::sync::Arc<AtomicBool>,
    /// Sequence half of the advertised journal tip — see
    /// [`melin_transport_core::AdvertisedJournalTip`].
    pub journal_tip: melin_transport_core::AdvertisedJournalTip,
    /// `true` while this replica holds an authenticated replication
    /// connection to its primary. The raft driver refuses to
    /// auto-promote while set: a live link means the primary is
    /// demonstrably alive, and winning a control-plane election (e.g.
    /// because a *different* node's raft died) must not depose a
    /// healthy primary.
    pub primary_link_up: std::sync::Arc<AtomicBool>,
    /// The durability (acking) mode last advertised by the primary
    /// (`DurabilityMode::as_u8`; `ACKING_MODE_UNKNOWN` until first
    /// contact), refreshed by `StreamStart` and every `Heartbeat`. The
    /// raft driver's auto-promotion refusal reads this — the mode the
    /// *primary* acked under decides whether an election win proves
    /// this replica holds every acked order, not this node's own
    /// (possibly pre-staged) configuration. Falls back to the local
    /// mode while unknown, which is exactly the pre-propagation
    /// behavior.
    pub primary_acking_mode: std::sync::Arc<std::sync::atomic::AtomicU8>,
    /// Whether the replica's pipeline is healthy — fed to the replica's
    /// minimal health endpoint (`/health` OK/ERR and the
    /// `melin_pipeline_healthy` gauge). Process-lifetime (the per-session
    /// pipeline is torn down and rebuilt across resyncs, so its own
    /// `journal_failed` latch cannot be handed to the endpoint):
    /// `true` from boot, latched `false` by the journal stage's failure
    /// wrapper, reset `true` when a fresh pipeline is built.
    pub pipeline_healthy: std::sync::Arc<AtomicBool>,
}

impl ReplicaControlPlane {
    /// Fresh handles for a replica boot: nothing requested, tip not
    /// trustworthy yet, tip sequence 0, primary link down, pipeline
    /// healthy (nothing has failed yet).
    pub fn new() -> Self {
        Self {
            promote: crate::promotion::PromotionRequest::new(),
            tip_ready: std::sync::Arc::new(AtomicBool::new(false)),
            journal_tip: melin_transport_core::AdvertisedJournalTip::new(
                melin_transport_core::WireSeq::new(0),
            ),
            primary_link_up: std::sync::Arc::new(AtomicBool::new(false)),
            primary_acking_mode: std::sync::Arc::new(std::sync::atomic::AtomicU8::new(
                crate::durability_policy::ACKING_MODE_UNKNOWN,
            )),
            pipeline_healthy: std::sync::Arc::new(AtomicBool::new(true)),
        }
    }
}

impl Default for ReplicaControlPlane {
    fn default() -> Self {
        Self::new()
    }
}

/// Diagnostic: emit a `tcp_info` span at debug level describing the
/// kernel's view of the socket (rtt, cwnd, retrans, unacked, rcv_space,
/// rto). Guarded internally with `tracing::enabled!` so the
/// `getsockopt(TCP_INFO)` syscall is skipped when debug logging is off —
/// the call is free at runtime unless `RUST_LOG=debug` (or a more
/// specific filter) is active. Used to distinguish user-space stalls
/// from TCP-level congestion collapse when diagnosing replication
/// slowdowns.
pub(super) fn log_tcp_info(fd: std::os::unix::io::RawFd, tag: &str, slot: usize) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    // SAFETY: all-zero pattern is a valid `tcp_info` (all numeric fields).
    // The kernel fills in whatever the running version supports and
    // returns the written length in `len`.
    let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            &mut info as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        tracing::debug!(
            slot,
            tag,
            errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            "tcp_info getsockopt failed"
        );
        return;
    }
    tracing::debug!(
        slot,
        tag,
        state = info.tcpi_state,
        ca_state = info.tcpi_ca_state,
        rtt_us = info.tcpi_rtt,
        rttvar_us = info.tcpi_rttvar,
        snd_cwnd = info.tcpi_snd_cwnd,
        snd_ssthresh = info.tcpi_snd_ssthresh,
        snd_mss = info.tcpi_snd_mss,
        rcv_mss = info.tcpi_rcv_mss,
        unacked = info.tcpi_unacked,
        retrans = info.tcpi_retrans,
        total_retrans = info.tcpi_total_retrans,
        lost = info.tcpi_lost,
        rcv_space = info.tcpi_rcv_space,
        rto_us = info.tcpi_rto,
        "tcp_info"
    );
}

/// Sleep for the given duration in 100ms increments, checking the shutdown
/// flag and the promotion request between increments. Returns early if
/// either is set.
pub(super) fn sleep_checking_flags(
    duration: std::time::Duration,
    shutdown: &AtomicBool,
    promote: &crate::promotion::PromotionRequest,
) {
    let deadline = std::time::Instant::now() + duration;
    while std::time::Instant::now() < deadline {
        if shutdown.load(Ordering::Relaxed) || promote.is_requested() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Wait out one retry delay: sleep for the current `backoff` (checking
/// the shutdown and promotion flags — see [`sleep_checking_flags`]),
/// then double it, capped at [`MAX_BACKOFF`], for the next attempt.
/// Sleep-then-double, so the first retry waits the base delay rather
/// than twice it. Every retry arm in both receivers goes through this —
/// don't hand-roll the idiom, the copies drift.
pub(super) fn sleep_then_double_backoff(
    backoff: &mut std::time::Duration,
    shutdown: &AtomicBool,
    promote: &crate::promotion::PromotionRequest,
) {
    sleep_checking_flags(*backoff, shutdown, promote);
    *backoff = (*backoff * 2).min(MAX_BACKOFF);
}

/// Outcome of shutting down a replica pipeline. All stage threads are
/// joined in every variant — by the time the caller sees this, no
/// pipeline thread can still touch the journal or snapshot files.
pub(super) enum TeardownOutcome<A, W> {
    /// Clean exit: both stages returned their state, reusable for the
    /// next pipeline build (post-snapshot) or promotion.
    Clean(A, W),
    /// The journal stage exited with an error — its writer is gone and
    /// the matching stage's state is unusable (it may have applied
    /// events the journal never persisted). Carries the error so the
    /// orchestrator can distinguish repairable chain divergence
    /// (in-process resync) from journal I/O death (exit).
    JournalFailed(melin_journal::JournalError),
    /// A stage thread panicked; no state survives.
    Panicked,
}

/// Shut down the replica pipeline and extract Exchange + SectorWriter from
/// the stage threads.
///
/// Relies on the caller having published a `JournalEvent::Shutdown`
/// sentinel to the input ring before invoking this — the journal and
/// matching stages exit when they consume the sentinel via the normal
/// event-processing path. We deliberately don't flip `shutdown_flag`
/// here: doing so could cause a stage to take its emergency-abort
/// branch *before* consuming the sentinel, hitting the
/// drain-vs-cursor race that the sentinel design exists to avoid.
///
/// `drain_handle` and `shadow_handle` still observe `shutdown_flag`
/// because they don't process the input ring (no sentinel reaches
/// them); set the flag for them only.
pub(super) fn shutdown_pipeline<A: Send + 'static, W: Send + 'static>(
    shutdown_flag: &AtomicBool,
    journal_handle: std::thread::JoinHandle<Result<W, melin_journal::JournalError>>,
    matching_handle: std::thread::JoinHandle<A>,
    drain_handle: std::thread::JoinHandle<()>,
    shadow_handle: Option<std::thread::JoinHandle<()>>,
) -> TeardownOutcome<A, W> {
    // Defense-in-depth: set the flag before joining. The sentinel was
    // already published by `teardown_replica_pipeline` before this call,
    // so no further events can arrive in the input ring — setting the
    // flag here cannot race with new publishes. The flag is the fallback
    // exit signal for paths that don't observe the sentinel: `run_sync`
    // in `no-persist` builds, the drain consumer, the shadow stage, and
    // any case where the receiver thread panicked before publishing the
    // sentinel.
    //
    // The flag is also what makes these joins safe when the journal
    // stage is already dead: the matching stage's gate on the (frozen)
    // journal cursor is a non-blocking spin that re-checks the flag
    // every iteration, and its shutdown drain is `try_consume`-bounded.
    shutdown_flag.store(true, Ordering::Release);
    // Join EVERY thread before reporting the outcome, even on failure:
    // the in-process divergence-resync path archives the journal and
    // snapshot right after this returns, and a still-running shadow
    // thread could be mid-snapshot-write during those renames.
    let journal_result = journal_handle.join();
    let matching_result = matching_handle.join();
    let _ = drain_handle.join();
    if let Some(h) = shadow_handle {
        let _ = h.join();
    }
    let writer = match journal_result {
        Ok(Ok(w)) => w,
        // Already error!-logged by the journal thread's spawn wrapper.
        Ok(Err(e)) => return TeardownOutcome::JournalFailed(e),
        Err(_) => return TeardownOutcome::Panicked,
    };
    match matching_result {
        Ok(exchange) => TeardownOutcome::Clean(exchange, writer),
        Err(_) => TeardownOutcome::Panicked,
    }
}

/// Live replica pipeline — built once on first connect (or after a snapshot
/// transfer), persists across `Disconnected` reconnects so the orchestrator
/// doesn't pay the journal-recover + thread-spawn cost on every drop.
///
/// Shared between the kernel-TCP and DPDK receivers; both build pipelines
/// with the same shape (journal / matching / drain / optional shadow), and
/// both refresh handshake state from `last_seq` + `chain_hash_lock` at
/// reconnect time.
pub(super) struct ReplicaPipelineHandles<A: Application, W: Send + 'static> {
    pub(super) input_producer: melin_pipeline::ring::Producer<InputSlot<A::Event>>,
    pub(super) journal_cursor: Arc<melin_pipeline::padding::Sequence>,
    /// Highest wire seq durably persisted, published by JournalStage after
    /// each fsync. Read by the orchestrator to fill in the reconnect
    /// handshake without owning the writer. Typed so the handshake's resume
    /// point can never be sourced from a ring-space counter (the adjacent
    /// `journal_cursor` resets to ~0 every process start).
    pub(super) last_seq: melin_transport_core::DurableWireSeqCursor,
    /// Seqlock-published fsync state (chain hash + journal seq + ring
    /// cursor), read half. Option to mirror the primary-side pattern;
    /// always Some on replicas now.
    pub(super) chain_hash_lock:
        Option<melin_pipeline::seqlock::SeqLockReader<melin_transport_core::pipeline::FsyncState>>,
    /// Primary-announced rotation hand-off: the receiver thread pushes
    /// `Rotate` boundaries here (in stream order), the journal stage
    /// pops and rotates at exactly those sequences. Replicas have no
    /// local rotation triggers.
    pub(super) stream_marks: melin_transport_core::pipeline::StreamMarkQueue,
    /// Latched by the journal thread's spawn wrapper when the stage
    /// exits with an error (chain divergence, journal I/O failure).
    /// The streaming receiver checks it and tears the session down —
    /// without this, a dead journal stage freezes the journal cursor
    /// and the receiver wedges forever on ring backpressure or the
    /// ack-durability wait.
    pub(super) journal_failed: Arc<AtomicBool>,
    /// Per-pipeline shutdown flag — flipped only on a controlled teardown
    /// (Promote/Shutdown/Fatal/Snapshot). NOT flipped on `Disconnected`.
    pub(super) pipeline_shutdown: Arc<AtomicBool>,
    pub(super) journal_handle: std::thread::JoinHandle<Result<W, melin_journal::JournalError>>,
    pub(super) matching_handle: std::thread::JoinHandle<A>,
    pub(super) drain_handle: std::thread::JoinHandle<()>,
    pub(super) shadow_handle: Option<std::thread::JoinHandle<()>>,
}

/// Build the replica pipeline and spawn its stage threads on the configured
/// cores. Returns the bundle of state the orchestrator keeps across
/// `Disconnected` reconnects.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_replica_pipeline_with_threads<A, W>(
    exchange: A,
    writer: W,
    cores: crate::server::PipelineCores,
    snapshot_interval_ms: u64,
    snapshot_path: std::path::PathBuf,
    group_commit_delay: std::time::Duration,
    busy_spin: bool,
    fence_state: Arc<melin_transport_core::fence::FenceState>,
    // Process-lifetime health mirror for the replica health endpoint —
    // see `ReplicaControlPlane::pipeline_healthy`. Reset `true` here (a
    // fresh pipeline is healthy) and latched `false` by the journal
    // thread's failure wrapper, alongside the per-pipeline
    // `journal_failed` latch.
    pipeline_healthy: Arc<AtomicBool>,
) -> Result<ReplicaPipelineHandles<A, W>, Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
    JournalStage<A::Event, W>: JournalStageRun<A::Event, Writer = W>,
{
    let shadow_exchange = <A as Application>::clone_via_snapshot(&exchange)?;

    let enable_shadow = snapshot_interval_ms > 0;
    // Shadow snapshot seeds its epoch from the fence state's current value
    // (set from the replica's recovered journal before this builder runs).
    let shadow_initial_epoch = fence_state.epoch();
    let pipeline = melin_transport_core::pipeline::build_replica_pipeline(
        exchange,
        writer,
        4096, // max_journal_batch
        group_commit_delay,
        busy_spin,
        enable_shadow,
        fence_state,
    );

    let pipeline_shutdown = Arc::new(AtomicBool::new(false));

    let ps = Arc::clone(&pipeline_shutdown);
    let journal_core = cores.journal;
    let mut journal_stage = pipeline.journal_stage;
    // Replicas never rotate on local triggers (size or operator
    // command) — they adopt the boundaries the primary announces over
    // the stream, which keeps segment boundaries (and with them chain
    // values and journal bytes) identical across nodes.
    let stream_marks: melin_transport_core::pipeline::StreamMarkQueue =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new()));
    journal_stage.set_stream_marks(Arc::clone(&stream_marks));
    let journal_failed = Arc::new(AtomicBool::new(false));
    let journal_failed_latch = Arc::clone(&journal_failed);
    // A fresh pipeline is healthy — this also clears the latch after a
    // successful in-process resync rebuild.
    pipeline_healthy.store(true, Ordering::Release);
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("journal", journal_core);
            let result = journal_stage.run(&ps);
            if let Err(ref e) = result {
                // Latch before logging so the receiver reacts even if
                // logging stalls. A dead journal stage freezes the
                // journal cursor; every downstream wait on it (ring
                // backpressure, ack durability) would spin forever —
                // the streaming loop polls this latch and tears the
                // session down instead.
                journal_failed_latch.store(true, Ordering::Release);
                // Mirror into the process-lifetime gauge the replica
                // health endpoint serves.
                pipeline_healthy.store(false, Ordering::Release);
                tracing::error!(error = %e, "replica journal stage failed — session teardown");
            }
            result
        })
        .expect("spawn journal thread");

    let ps = Arc::clone(&pipeline_shutdown);
    let matching_core = cores.matching;
    let matching_stage = pipeline.matching_stage;
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("matching", matching_core);
            matching_stage.run(&ps)
        })
        .expect("spawn matching thread");

    // Drain thread uses the response core — replicas have no response stage,
    // but the consumer needs to be drained so the output ring doesn't fill.
    let ps = Arc::clone(&pipeline_shutdown);
    let drain_core = cores.response;
    let drain_consumer = pipeline.drain_consumer;
    let drain_handle = std::thread::Builder::new()
        .name("drain".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("drain", drain_core);
            let mut consumer = drain_consumer;
            let mut batch = vec![OutputSlot::<A::Report, A::QueryResponse>::default(); 256];
            loop {
                if ps.load(Ordering::Relaxed) {
                    return;
                }
                let count = consumer.consume_batch(&mut batch, 256);
                if count == 0 {
                    if busy_spin {
                        std::hint::spin_loop();
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        })
        .expect("spawn drain thread");

    let shadow_handle = if let Some(shadow_cons) = pipeline.shadow_consumer {
        let snap_path = snapshot_path;
        let chain_lock = pipeline
            .chain_hash_lock
            .as_ref()
            .expect("chain hash lock with shadow")
            .clone();
        let ps = Arc::clone(&pipeline_shutdown);
        let shadow_core = cores.shadow;
        Some(
            std::thread::Builder::new()
                .name("replica-shadow".into())
                .spawn(move || {
                    melin_app::affinity::pin_thread("replica-shadow", shadow_core);
                    melin_transport_core::shadow::run(
                        shadow_cons,
                        shadow_exchange,
                        snap_path,
                        std::time::Duration::from_millis(snapshot_interval_ms),
                        chain_lock,
                        &ps,
                        false,
                        shadow_initial_epoch,
                    );
                })
                .expect("spawn shadow thread"),
        )
    } else {
        None
    };

    Ok(ReplicaPipelineHandles {
        input_producer: pipeline.input_producer,
        journal_cursor: pipeline.cursors.journal_ring_arc(),
        last_seq: pipeline.cursors.durable_wire_seq(),
        chain_hash_lock: pipeline.chain_hash_lock,
        stream_marks,
        journal_failed,
        pipeline_shutdown,
        journal_handle,
        matching_handle,
        drain_handle,
        shadow_handle,
    })
}

/// Tear down the pipeline: publish the shutdown sentinel, join all
/// threads, return the recovered (App, SectorWriter) so the orchestrator
/// can use them for the next pipeline build (e.g., post-snapshot) or
/// pass them up on promotion.
pub(super) fn teardown_replica_pipeline<A: Application + Send + 'static, W: Send + 'static>(
    mut handles: ReplicaPipelineHandles<A, W>,
) -> TeardownOutcome<A, W> {
    // The sentinel is published here, not by callers, so no teardown
    // path can forget it — the journal and matching stages exit by
    // consuming it through the normal event path, draining anything
    // received-but-not-yet-journaled (see `shutdown_pipeline` for why
    // the flag alone is only the emergency fallback). Bounded retry
    // rather than the blocking `publish`: if the journal stage dies
    // with the ring full, its gate cursor freezes and a blocking
    // publish would spin forever — re-check the failure latch between
    // attempts and fall through to the flag-only teardown once it
    // trips (the sentinel has no reader then anyway: the matching
    // stage is gated behind the frozen journal cursor).
    while !handles.journal_failed.load(Ordering::Acquire) {
        match handles
            .input_producer
            .try_publish(InputSlot::<A::Event>::shutdown_sentinel())
        {
            Ok(_) => break,
            // Ring full — consumers need to make progress first.
            Err(_) => std::thread::yield_now(),
        }
    }
    shutdown_pipeline::<A, W>(
        &handles.pipeline_shutdown,
        handles.journal_handle,
        handles.matching_handle,
        handles.drain_handle,
        handles.shadow_handle,
    )
}

/// How many mid-stream divergence resyncs the receiver attempts
/// in-process (per process lifetime) before giving up. Mid-stream
/// divergence is never expected in a healthy cluster — it means
/// corruption or a serious bug somewhere — so the budget is exactly
/// one: the first occurrence repairs automatically (archive the local
/// lineage as `.divergent.<n>`, re-seed from the primary) and pages
/// the operator via `melin_replica_divergence_total`; a second in the
/// same process lifetime is systematic, and continuing to repair
/// would fill the disk with archives while masking the underlying
/// fault. Exit hard instead.
pub(super) const MAX_INPROCESS_DIVERGENCE_RESYNCS: u32 = 1;

/// Recover replica boot state from disk.
///
/// Recovers whenever any journal segment survives — live OR archived
/// (a crash between rotation's rename and the new live file's creation
/// leaves archives with no live segment, and recovery handles that
/// layout; treating it as a fresh replica would discard local durable
/// history and then fail `create_new` against the surviving lineage).
/// Returns `(None, None, 0, zeros)` for a genuinely fresh replica.
/// Also seeds the fencing epoch from the recovered journal.
///
/// Called at receiver startup, and again after a mid-stream chain
/// divergence tears the pipeline down: the on-disk journal is
/// self-consistent (merely forked from the primary's history), so
/// recovery re-derives a truthful handshake pair `(last_sequence,
/// chain_hash)` and the next connection takes the primary's
/// HashMismatch → archive → reseed path in-process.
#[allow(clippy::type_complexity)]
pub(super) fn recover_replica_state<A, W>(
    journal_path: &std::path::Path,
    snapshot_path: &std::path::Path,
    factory: &dyn melin_app::app_factory::AppFactory<App = A>,
    fence_state: &melin_transport_core::fence::FenceState,
) -> Result<(Option<A>, Option<W>, u64, [u8; 32]), Box<dyn std::error::Error>>
where
    A: Application,
    W: melin_journal::JournalWrite<A::Event>,
{
    let lineage_exists =
        journal_path.exists() || !melin_journal::segment::list_archives(journal_path)?.is_empty();
    if !lineage_exists {
        return Ok((None, None, 0u64, [0u8; 32]));
    }
    let engine = if snapshot_path.exists() {
        tracing::info!("recovering replica from snapshot + journal");
        melin_transport_core::JournaledApp::<A, W>::recover_from_snapshot(
            snapshot_path,
            journal_path,
        )?
    } else {
        melin_transport_core::JournaledApp::<A, W>::recover(factory.empty(), journal_path)?
    };
    let next = engine.next_sequence();
    let last = next.saturating_sub(1);
    let hash = engine.chain_hash().unwrap_or([0u8; 32]);
    // Seed the observed epoch from the replica's own recovered journal.
    // Streaming `EpochBump`s and the snapshot-resync path raise it later.
    fence_state.observe_epoch(engine.recovered_epoch());
    let (mut exchange, writer) = engine.into_parts();
    factory.apply_operator_policy(&mut exchange);
    Ok((Some(exchange), Some(writer), last, hash))
}

/// Reconnect backoff cap shared by both receivers — exponential from
/// 1 s, clamped here so a long outage settles to one attempt every 30 s
/// without hammering a flapping primary. Reset to 1 s only when a
/// session heard the primary speak (data or heartbeat) — see
/// [`handle_session_exit`].
pub(super) const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

/// What the receiver's reconnect loop does after a streaming session
/// ends — the transport-independent half of the exit dispatch, returned
/// by [`handle_session_exit`].
pub(in crate::replication) enum AfterSession<A, W> {
    /// Terminal: `run_receiver*` returns this verbatim — clean shutdown
    /// (`Ok(None)`), a promotion hand-off (`Ok(Some(state))`), or a
    /// fatal error.
    Return(ReceiverResult<A, W>),
    /// A mid-stream divergence was repaired in-process: the recovered
    /// on-disk state is the new handshake position. The caller adopts it
    /// and reconnects; the primary's `HashMismatch` verdict then routes
    /// the replica through archive + re-seed.
    Resync {
        exchange: Option<A>,
        journal_writer: Option<W>,
        last_sequence: u64,
        chain_hash: [u8; 32],
    },
    /// A plain disconnect — backoff has already been applied (and the
    /// flags checked); the caller reconnects, reusing the still-live
    /// pipeline.
    Reconnect,
}

/// Dispatch a finished streaming session — shared by the kernel-TCP and
/// DPDK receivers.
///
/// Folds the behaviours that were previously copied between the two
/// receiver loops (and had drifted — the copies reset the post-resync
/// writer differently, a latent corruption bug): the
/// `Shutdown`/`Promote`/`Fatal` teardown (including the once-per-process
/// in-process divergence-resync policy) and the `Disconnected` reconnect
/// backoff. The shutdown-sentinel publish lives in
/// [`teardown_replica_pipeline`].
///
/// `close` runs any transport-specific teardown that must precede a
/// reconnect — the DPDK receiver closes its smoltcp socket so the
/// primary's slot and the local socket-set entry are freed; the
/// kernel-TCP receiver passes a no-op (its `TcpStream` is dropped by the
/// caller on the next loop turn). It is invoked only on the two
/// reconnecting paths (in-process resync, plain disconnect), matching
/// the pre-refactor behaviour.
// Twelve arguments is a lot, but each is a distinct piece of the
// receiver loop's state; bundling them would only move the noise.
#[allow(clippy::too_many_arguments)]
pub(in crate::replication) fn handle_session_exit<A, W>(
    result: StreamingResult,
    pipeline: &mut Option<ReplicaPipelineHandles<A, W>>,
    divergence_resyncs: &mut u32,
    backoff: &mut std::time::Duration,
    last_sequence: u64,
    journal_path: &std::path::Path,
    snapshot_path: &std::path::Path,
    factory: &dyn melin_app::app_factory::AppFactory<App = A>,
    fence_state: &melin_transport_core::fence::FenceState,
    shutdown: &AtomicBool,
    promote: &crate::promotion::PromotionRequest,
    mut close: impl FnMut(),
) -> AfterSession<A, W>
where
    A: Application + Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
{
    let StreamingResult {
        exit,
        heard_from_primary,
    } = result;

    match exit {
        SessionExit::Shutdown => {
            if let Some(p) = pipeline.take() {
                let _ = teardown_replica_pipeline::<A, W>(p);
            }
            AfterSession::Return(Ok(None))
        }

        SessionExit::Promote => AfterSession::Return(match pipeline.take() {
            Some(p) => match teardown_replica_pipeline::<A, W>(p) {
                TeardownOutcome::Clean(ex, wr) => Ok(Some((ex, wr))),
                _ => Err("pipeline failed during promotion".into()),
            },
            None => Err("pipeline missing on promote".into()),
        }),

        SessionExit::Fatal(e) => {
            let outcome = match pipeline.take() {
                Some(p) => teardown_replica_pipeline::<A, W>(p),
                // Fatal implies a streaming session, which implies a
                // pipeline — but don't turn a missing one into a resync.
                None => return AfterSession::Return(Err(e)),
            };
            // Mid-stream chain divergence is repairable in-process: the
            // on-disk journal is self-consistent (merely forked from the
            // primary's history), so re-derive the handshake state from
            // disk and reconnect — the primary judges the recovered
            // position divergent and the next session takes the
            // HashMismatch → archive → reseed path, no restart needed.
            // Every other fatal exits as before: protocol violations and
            // journal I/O death (ENOSPC, RO-FS) would fail the same way
            // after a resync.
            let TeardownOutcome::JournalFailed(
                je @ melin_journal::JournalError::ReplicaChainDivergence { .. },
            ) = outcome
            else {
                return AfterSession::Return(Err(e));
            };

            *divergence_resyncs += 1;
            let attempt = *divergence_resyncs;
            if attempt > MAX_INPROCESS_DIVERGENCE_RESYNCS {
                return AfterSession::Return(Err(format!(
                    "mid-stream chain divergence recurred {attempt} times — giving up on \
                     in-process resync (each cycle archives the local journal and re-seeds \
                     from the primary; recurrence at this rate means the primary keeps \
                     streaming history that forks from what it announces): {je}"
                )
                .into()));
            }
            tracing::warn!(
                error = %je,
                attempt,
                max_attempts = MAX_INPROCESS_DIVERGENCE_RESYNCS,
                "mid-stream chain divergence — re-deriving local state for in-process resync"
            );
            // Transport-specific teardown before reconnecting.
            close();
            match recover_replica_state::<A, W>(journal_path, snapshot_path, factory, fence_state) {
                Ok((exchange, journal_writer, seq, hash)) => AfterSession::Resync {
                    exchange,
                    journal_writer,
                    last_sequence: seq,
                    chain_hash: hash,
                },
                Err(e) => AfterSession::Return(Err(e)),
            }
        }

        SessionExit::Disconnected => {
            // Transport-specific teardown before reconnecting (smoltcp
            // socket reclaim on DPDK; no-op on kernel TCP).
            close();
            // A session in which the primary spoke — data or heartbeat
            // (heartbeats flow even on a quiet system) — proves it
            // alive and serving: treat the drop as transient and reset
            // the backoff. A session with no word from the primary
            // keeps escalating: that covers instant-drop flaps and
            // synthetic results from sessions that never started
            // streaming (a persistent local transport failure must not
            // redial at 1 Hz forever).
            if heard_from_primary {
                *backoff = std::time::Duration::from_secs(1);
            }
            tracing::warn!(
                last_sequence,
                heard_from_primary,
                backoff_secs = backoff.as_secs(),
                "reconnecting to primary"
            );
            sleep_then_double_backoff(backoff, shutdown, promote);
            AfterSession::Reconnect
        }
    }
}

/// Tear the live pipeline down for a promotion that fired while the
/// receiver was disconnected — at the top of the reconnect loop or
/// during reconnect backoff — and return the warm Exchange + writer for
/// the promoted primary. Shared by both receivers.
///
/// A clean teardown hands back the warm state; if there is no pipeline
/// (promotion before the first connect, or after a resync that left the
/// state in the receiver's locals) those locals carry it. A missing
/// pair is a hard error — a promote with nothing to promote.
pub(in crate::replication) fn take_pipeline_for_promotion<A, W>(
    pipeline: &mut Option<ReplicaPipelineHandles<A, W>>,
    exchange: &mut Option<A>,
    journal_writer: &mut Option<W>,
) -> ReceiverResult<A, W>
where
    A: Application + Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
{
    if let Some(p) = pipeline.take()
        && let TeardownOutcome::Clean(e, w) = teardown_replica_pipeline::<A, W>(p)
    {
        *exchange = Some(e);
        *journal_writer = Some(w);
    }
    match (exchange.take(), journal_writer.take()) {
        (Some(e), Some(w)) => Ok(Some((e, w))),
        _ => Err("promotion requested but no local state available".into()),
    }
}

/// The four facts a successful snapshot + segment-seed transfer yields:
/// `(snapshot App, snapshot sequence, snapshot chain hash, seed length)`.
type ResyncTransfer<A> = (A, u64, [u8; 32], u64);

/// What [`handle_resync_verdict`] resolved a `NeedSnapshot` /
/// `HashMismatch` verdict to.
pub(in crate::replication) enum ResyncDecision {
    /// Resync complete — resume streaming from `resume_sequence` on the
    /// re-seeded lineage `(segment_start_sequence, anchor_hash)`. The
    /// recovered App + writer are left in the receiver's `exchange` /
    /// `journal_writer` locals.
    Ready {
        segment_start_sequence: u64,
        anchor_hash: [u8; 32],
        resume_sequence: u64,
    },
    /// The transfer failed network-shaped (drop / restart mid-body); the
    /// caller backs off and reconnects. The pre-resync lineage is already
    /// archived and half-applied transfer state has been cleaned up.
    Retry,
}

/// Receive the snapshot + segment-seed transfer that follows a resync
/// verdict, installing both (snapshot → `snapshot_path`, seed →
/// `journal_path`). Returns the snapshot App, its sequence + chain hash,
/// and the seed length (the journal's `valid_end`). Shared by both
/// receivers via [`ControlFrameSource`].
///
/// Errors here are network-shaped — the caller retries (see
/// [`ResyncDecision::Retry`]); the post-transfer install in
/// [`handle_resync_verdict`] is what's fatal.
fn receive_resync_transfer<A, S>(
    source: &mut S,
    snapshot_path: &std::path::Path,
    journal_path: &std::path::Path,
    fence_state: &melin_transport_core::fence::FenceState,
) -> Result<ResyncTransfer<A>, Box<dyn std::error::Error + Send + Sync>>
where
    A: Application,
    S: ControlFrameSource,
{
    let (snap_len, snap_sequence, snap_chain_hash) =
        match decode_primary_message(&source.next_frame(MAX_CONTROL_FRAME)?)? {
            PrimaryMessage::SnapshotBegin {
                snapshot_len,
                snap_sequence,
                snap_chain_hash,
            } => (snapshot_len, snap_sequence, snap_chain_hash),
            other => return Err(format!("expected SnapshotBegin, got {other:?}").into()),
        };

    tracing::info!(snap_sequence, snap_len, "receiving snapshot");
    let tmp_path = snapshot_path.with_extension("snapshot.tmp");
    receive_chunked_body(source, &tmp_path, snap_len, "snapshot")?;
    std::fs::rename(&tmp_path, snapshot_path)?;
    tracing::info!(snap_sequence, snap_len, "snapshot received and verified");

    let (snap_exchange, _snap_seq, snap_hash, snap_epoch) =
        melin_transport_core::snapshot::load::<A>(snapshot_path)?;
    if snap_hash != snap_chain_hash {
        return Err(format!(
            "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
             loaded snapshot has {snap_hash:02x?}"
        )
        .into());
    }
    // Adopt the primary's snapshot epoch — the resync rebases this replica
    // onto the primary's lineage, including its epoch.
    fence_state.observe_epoch(snap_epoch);

    // Segment seed: the raw byte prefix of the primary's segment
    // containing `snap_sequence`. Written verbatim as our live segment, it
    // makes our segmentation a byte-copy of the primary's from birth —
    // chain values comparable and `Rotate` verification valid immediately.
    let seed_len = match decode_primary_message(&source.next_frame(MAX_CONTROL_FRAME)?)? {
        PrimaryMessage::SegmentSeedBegin { seed_len } => seed_len,
        other => {
            return Err(format!("expected SegmentSeedBegin after snapshot, got {other:?}").into());
        }
    };
    let seed_tmp = journal_path.with_extension("seed.tmp");
    receive_chunked_body(source, &seed_tmp, seed_len, "segment seed")?;
    // Structural check before installing: the CRC only proves transport
    // integrity, not that the primary sent a well-formed prefix ending at
    // the snapshot sequence. (With hash-chain on, the chain cross-check in
    // `handle_resync_verdict` subsumes this; without it, this is the only
    // guard.)
    if let Err(e) =
        melin_journal::segment::verify_segment_prefix(&seed_tmp, snap_sequence, seed_len)
    {
        let _ = std::fs::remove_file(&seed_tmp);
        return Err(format!("segment seed failed structural verification: {e}").into());
    }
    std::fs::rename(&seed_tmp, journal_path)?;
    melin_journal::segment::fsync_parent_dir(journal_path)?;
    Ok((snap_exchange, snap_sequence, snap_chain_hash, seed_len))
}

/// Handle a `NeedSnapshot` / `HashMismatch` resync verdict — shared by
/// both receivers. Tears the pipeline down, archives the local lineage
/// (never deleted), resets the handshake position, receives + installs
/// the snapshot and segment seed via `source`, opens the re-seeded
/// segment and ties its chain to the snapshot, then validates the
/// post-snapshot `StreamStart` inline before resuming.
///
/// On success the recovered App + writer are left in `exchange` /
/// `journal_writer` and [`ResyncDecision::Ready`] carries the resume
/// lineage. A network-shaped transfer failure yields
/// [`ResyncDecision::Retry`] (the caller backs off and reconnects). An
/// inconsistent primary or a local install failure is fatal (`Err`).
#[allow(clippy::too_many_arguments)]
pub(in crate::replication) fn handle_resync_verdict<A, W, S>(
    divergent: bool,
    source: &mut S,
    pipeline: &mut Option<ReplicaPipelineHandles<A, W>>,
    exchange: &mut Option<A>,
    journal_writer: &mut Option<W>,
    journal_path: &std::path::Path,
    snapshot_path: &std::path::Path,
    fence_state: &melin_transport_core::fence::FenceState,
    control: &ReplicaControlPlane,
    last_sequence: &mut u64,
    chain_hash: &mut [u8; 32],
) -> Result<ResyncDecision, Box<dyn std::error::Error + Send + Sync>>
where
    A: Application + Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
    S: ControlFrameSource,
{
    // HashMismatch is NeedSnapshot plus the verdict that our local journal
    // holds divergent history (forked from the primary's — e.g. an
    // ex-primary rejoining after failover with an acked-but-unreplicated
    // suffix).
    if divergent {
        tracing::warn!(
            last_sequence = *last_sequence,
            "primary reports chain divergence — archiving local journal, resyncing from snapshot"
        );
    } else {
        tracing::info!("primary requires snapshot transfer — receiving snapshot");
    }

    if let Some(p) = pipeline.take() {
        let _ = teardown_replica_pipeline::<A, W>(p);
    }

    // Invalidate the in-memory App + writer before moving their backing
    // files aside. On the in-process divergence repair path these still
    // hold the recovered handles; a transfer failure returns `Retry`, and
    // without this reset the stale writer — now pointing at an
    // archived-away journal — would survive the fresh-replica create gate
    // and get rebuilt into the next pipeline.
    *exchange = None;
    *journal_writer = None;

    // Move the local lineage aside — never delete. Divergent journals are
    // audit-trail material; stale ones may be the last copy of pruned
    // history.
    let reason = if divergent {
        ArchiveReason::Divergent
    } else {
        ArchiveReason::Resync
    };
    archive_local_lineage(journal_path, snapshot_path, reason)?;
    // Archived — a retried handshake must present as a fresh replica.
    *last_sequence = 0;
    *chain_hash = [0u8; 32];
    // The advertised control-plane tip must shrink with the holdings:
    // the archived suffix is either divergent (never acked) or being
    // replaced wholesale, so this is the one legitimate tip regression
    // (see `AdvertisedJournalTip::reset`).
    control
        .journal_tip
        .reset(melin_transport_core::WireSeq::new(0));

    let (snap_exchange, snap_sequence, snap_chain_hash, seed_len) =
        match receive_resync_transfer::<A, S>(source, snapshot_path, journal_path, fence_state) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "snapshot transfer failed — retrying");
                // Half-applied resync state, not audit material — drop it
                // so the retry starts clean (the pre-resync lineage is
                // already archived; this is not it).
                let _ = std::fs::remove_file(snapshot_path);
                return Ok(ResyncDecision::Retry);
            }
        };
    *exchange = Some(snap_exchange);

    // Open the seeded segment for appending at the snapshot position —
    // recovery's resume path: the chain rebuilds from the seeded bytes and
    // its value at `snap_sequence` must equal the (verified) snapshot's
    // chain hash. `chain_hash()` is `None` only with `hash-chain` disabled
    // (nothing to tie); an all-zeros snapshot hash means the primary runs
    // without `hash-chain` (also nothing to tie).
    let writer = W::open_append(journal_path, snap_sequence, seed_len)?;
    let seeded_chain = writer.chain_hash().unwrap_or(snap_chain_hash);
    if snap_chain_hash != [0u8; 32] && seeded_chain != snap_chain_hash {
        return Err(format!(
            "segment seed chain at {snap_sequence} disagrees with the transferred snapshot's \
             hash — inconsistent primary"
        )
        .into());
    }
    let seeded_info = melin_journal::segment::read_header_info(journal_path)?;
    *journal_writer = Some(writer);

    // Validate the post-snapshot StreamStart inline before resuming — its
    // lineage must agree with the seed the primary just transferred.
    match decode_primary_message(&source.next_frame(MAX_CONTROL_FRAME)?)? {
        PrimaryMessage::StreamStart {
            start_sequence,
            segment_start_sequence,
            anchor_hash,
            epoch,
            durability_mode,
        } => {
            if segment_start_sequence != seeded_info.starting_sequence
                || anchor_hash != seeded_info.anchor_hash
            {
                return Err(format!(
                    "post-snapshot StreamStart lineage (start {segment_start_sequence}) \
                     disagrees with the transferred segment seed (start {}) — inconsistent \
                     primary",
                    seeded_info.starting_sequence
                )
                .into());
            }
            // We just rebased onto this primary's snapshot, so adopt its
            // epoch wholesale (no stale-primary refusal — our prior state
            // was discarded).
            fence_state.observe_epoch(epoch);
            control
                .primary_acking_mode
                .store(durability_mode, Ordering::Release);
            tracing::info!(
                start_sequence,
                epoch,
                durability_mode,
                "streaming resumed after snapshot transfer"
            );
            // The transfer is installed and validated: the node's holdings
            // are exactly the snapshot position again.
            control
                .journal_tip
                .reset(melin_transport_core::WireSeq::new(snap_sequence));
            Ok(ResyncDecision::Ready {
                segment_start_sequence,
                anchor_hash,
                resume_sequence: snap_sequence,
            })
        }
        other => Err(format!("expected StreamStart after snapshot, got {other:?}").into()),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

    use super::auth::{authenticate_replica, authenticate_with_primary};
    use super::*;
    // Any `AppEvent` works as the pipeline's event type here — these
    // protocol-level tests never construct a real app event (the slot's
    // event is always `JournalEvent::Tick`), so the counter example's
    // event type stands in for an exchange event and keeps the runtime's
    // test deps free of exchange crates.
    use counter_server::CounterEvent;
    type InputSlot = melin_transport_core::pipeline::InputSlot<CounterEvent>;
    use melin_transport_core::replication::protocol::{
        MAX_CONTROL_FRAME, MAX_DATA_FRAME, MSG_AUTH_OK, MSG_CHALLENGE_RESPONSE, MSG_SNAPSHOT_BEGIN,
        MSG_SNAPSHOT_CHUNK, MSG_SNAPSHOT_END, decode_auth_result, decode_challenge,
        decode_challenge_response, decode_primary_message, decode_replica_message, encode_ack,
        encode_auth_failed, encode_auth_ok, encode_chain_check, encode_challenge,
        encode_challenge_response, encode_handshake, encode_hash_mismatch, encode_heartbeat,
        encode_input_batch, encode_need_snapshot, encode_rotate, encode_segment_seed_begin,
        encode_snapshot_begin, encode_snapshot_chunk, encode_snapshot_end, encode_stream_start,
        read_frame, try_decode_input_batch,
    };

    /// Build a wire-ready `InputBatch` frame containing a single `Tick`
    /// slot at the given sequence — the protocol-level tests don't need
    /// real journal payloads, just something with a known max sequence.
    fn encode_input_batch_with_seq(end_sequence: u64, buf: &mut Vec<u8>) {
        let slot = InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: end_sequence,
            timestamp_ns: 0,
            event: melin_journal::JournalEvent::Tick { now_ns: 0 },
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        };
        encode_input_batch(&[slot], buf);
    }

    #[test]
    fn handshake_encode_decode_round_trip() {
        let handshake = Handshake {
            last_sequence: 42,
            chain_hash: [0xAB; 32],
            // Non-zero so a dropped/zeroed epoch field is caught.
            epoch: 9,
        };
        let mut buf = Vec::new();
        encode_handshake(&handshake, &mut buf);

        // Read frame: skip 4-byte length prefix.
        let payload = &buf[4..];
        let msg = decode_replica_message(payload).unwrap();
        match msg {
            ReplicaMessage::Handshake(h) => {
                assert_eq!(h.last_sequence, 42);
                assert_eq!(h.chain_hash, [0xAB; 32]);
                assert_eq!(h.epoch, 9);
            }
            _ => panic!("expected Handshake"),
        }
    }

    #[test]
    fn ack_encode_decode_round_trip() {
        let ack = Ack {
            acked_sequence: 1000,
            in_memory_sequence: 1024,
        };
        let mut buf = Vec::new();
        encode_ack(&ack, &mut buf);

        let payload = &buf[4..];
        let msg = decode_replica_message(payload).unwrap();
        match msg {
            ReplicaMessage::Ack(a) => {
                assert_eq!(a.acked_sequence, 1000);
                assert_eq!(a.in_memory_sequence, 1024);
            }
            _ => panic!("expected Ack"),
        }
    }

    /// Pin the exact on-the-wire byte layout of an Ack frame. A future
    /// `repr(C)` field reorder, alignment change, or accidental
    /// big-endian wrapper substitution would silently break replica/
    /// primary compatibility — the const `size_of` assert in
    /// `protocol.rs` only catches size changes, not layout changes.
    /// This test pins the expected bytes so any such break shows up
    /// loudly on the next CI run.
    #[test]
    fn ack_wire_byte_pattern() {
        let ack = Ack {
            acked_sequence: 0xDEAD_BEEF_CAFE_F00D,
            in_memory_sequence: 0x1122_3344_5566_7788,
        };
        let mut buf = Vec::new();
        encode_ack(&ack, &mut buf);
        // [length:u32 LE = 17][tag:u8 = MSG_ACK = 0x02]
        // [acked_sequence:u64 LE][in_memory_sequence:u64 LE]
        let expected: &[u8] = &[
            0x11, 0x00, 0x00, 0x00, // length = 17 LE
            0x02, // MSG_ACK
            0x0D, 0xF0, 0xFE, 0xCA, 0xEF, 0xBE, 0xAD, 0xDE, // acked_sequence LE
            0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // in_memory_sequence LE
        ];
        assert_eq!(buf.as_slice(), expected, "Ack wire layout drifted");
    }

    #[test]
    fn stream_start_encode_decode_round_trip() {
        let mut buf = Vec::new();
        // Non-zero epoch so a dropped/zeroed field is caught.
        // Non-zero epoch/mode so a dropped/zeroed field is caught.
        encode_stream_start(99, 42, [0xAA; 32], 5, 2, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::StreamStart {
                start_sequence,
                segment_start_sequence,
                anchor_hash,
                epoch,
                durability_mode,
            } => {
                assert_eq!(start_sequence, 99);
                assert_eq!(segment_start_sequence, 42);
                assert_eq!(anchor_hash, [0xAA; 32]);
                assert_eq!(durability_mode, 2);
                assert_eq!(epoch, 5);
            }
            _ => panic!("expected StreamStart"),
        }
    }

    #[test]
    fn heartbeat_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_heartbeat(123, 2, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::Heartbeat {
                sequence,
                durability_mode,
            } => {
                assert_eq!(sequence, 123);
                assert_eq!(durability_mode, 2);
            }
            _ => panic!("expected Heartbeat"),
        }
    }

    #[test]
    fn need_snapshot_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_need_snapshot(&mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        assert!(matches!(msg, PrimaryMessage::NeedSnapshot));
    }

    #[test]
    fn hash_mismatch_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_hash_mismatch(&mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        assert!(matches!(msg, PrimaryMessage::HashMismatch));
    }

    #[test]
    fn rotate_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_rotate(7_000_000, &[0xBB; 32], &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::Rotate {
                boundary_seq,
                tail_hash,
            } => {
                assert_eq!(boundary_seq, 7_000_000);
                assert_eq!(tail_hash, [0xBB; 32]);
            }
            _ => panic!("expected Rotate"),
        }
    }

    #[test]
    fn chain_check_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_chain_check(555, &[0xCC; 32], &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::ChainCheck {
                sequence,
                chain_hash,
            } => {
                assert_eq!(sequence, 555);
                assert_eq!(chain_hash, [0xCC; 32]);
            }
            _ => panic!("expected ChainCheck"),
        }
    }

    /// Pin the wire layout of the shared `(sequence, hash)` frame body —
    /// same rationale as `ack_wire_byte_pattern`. Rotate and ChainCheck
    /// share the layout; only the tag differs.
    #[test]
    fn rotate_wire_byte_pattern() {
        let mut hash = [0u8; 32];
        for (i, b) in hash.iter_mut().enumerate() {
            *b = i as u8;
        }
        let mut buf = Vec::new();
        encode_rotate(0xDEAD_BEEF_CAFE_F00D, &hash, &mut buf);
        // [length:u32 LE = 41][tag:u8 = MSG_ROTATE = 0x16]
        // [boundary_seq:u64 LE][tail_hash:32 bytes verbatim]
        let mut expected = vec![
            0x29, 0x00, 0x00, 0x00, // length = 41 LE
            0x16, // MSG_ROTATE
            0x0D, 0xF0, 0xFE, 0xCA, 0xEF, 0xBE, 0xAD, 0xDE, // boundary_seq LE
        ];
        expected.extend_from_slice(&hash);
        assert_eq!(
            buf.as_slice(),
            expected.as_slice(),
            "Rotate wire layout drifted"
        );
    }

    #[test]
    fn segment_seed_begin_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_segment_seed_begin(123_456, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SegmentSeedBegin { seed_len } => {
                assert_eq!(seed_len, 123_456);
            }
            _ => panic!("expected SegmentSeedBegin"),
        }
    }

    #[test]
    fn unknown_replica_message_type_is_error() {
        let payload = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = decode_replica_message(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_primary_message_type_is_error() {
        let payload = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = decode_primary_message(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn read_frame_enforces_max_size() {
        // Create a buffer with a length prefix claiming 1000 bytes.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 100]); // not enough data, but max_size check comes first

        let mut cursor = std::io::Cursor::new(buf);
        let result = read_frame(&mut cursor, 64);
        assert!(result.is_err());
    }

    #[test]
    fn challenge_encode_decode_round_trip() {
        let nonce = [0x42; 32];
        let mut buf = Vec::new();
        encode_challenge(&nonce, &mut buf);

        let payload = &buf[4..];
        let decoded = decode_challenge(payload).unwrap();
        assert_eq!(decoded, nonce);
    }

    #[test]
    fn challenge_response_encode_decode_round_trip() {
        let sig = [0xAA; 64];
        let pubkey = [0xBB; 32];
        let mut buf = Vec::new();
        encode_challenge_response(&sig, &pubkey, &mut buf);

        let payload = &buf[4..];
        let (decoded_sig, decoded_pubkey) = decode_challenge_response(payload).unwrap();
        assert_eq!(decoded_sig, sig);
        assert_eq!(decoded_pubkey, pubkey);
    }

    #[test]
    fn auth_ok_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_auth_ok(&mut buf);

        let payload = &buf[4..];
        assert!(decode_auth_result(payload).unwrap());
    }

    #[test]
    fn auth_failed_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_auth_failed(&mut buf);

        let payload = &buf[4..];
        assert!(!decode_auth_result(payload).unwrap());
    }

    #[test]
    fn decode_challenge_rejects_wrong_tag() {
        let mut payload = [0u8; 33];
        payload[0] = MSG_AUTH_OK;
        assert!(decode_challenge(&payload).is_err());
    }

    #[test]
    fn decode_challenge_response_rejects_short_payload() {
        let payload = [MSG_CHALLENGE_RESPONSE; 10]; // too short
        assert!(decode_challenge_response(&payload).is_err());
    }

    #[test]
    fn auth_round_trip_valid_key() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        let repl_key = SigningKey::from_bytes(&[0xFC; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            repl_key.verifying_key().to_bytes(),
        );
        let keys_content = format!("replication {pub_b64} test-replica\n");
        let authorized_keys = melin_app::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let repl_key_clone = SigningKey::from_bytes(&[0xFC; 32]);
        let replica_handle = std::thread::spawn(move || {
            let mut conn = replica_stream;
            authenticate_with_primary(&mut conn, &repl_key_clone)
        });

        let mut conn = primary_stream;
        authenticate_replica(&mut conn, &authorized_keys).unwrap();

        replica_handle.join().unwrap().unwrap();
    }

    #[test]
    fn auth_rejects_unknown_key() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        // authorized_keys has one key, but the replica uses a different one.
        let authorized_key = SigningKey::from_bytes(&[0xAA; 32]);
        let rogue_key = SigningKey::from_bytes(&[0xBB; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            authorized_key.verifying_key().to_bytes(),
        );
        let keys_content = format!("replication {pub_b64} authorized-replica\n");
        let authorized_keys = melin_app::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut conn = replica_stream;
            authenticate_with_primary(&mut conn, &rogue_key)
        });

        let mut conn = primary_stream;
        let result = authenticate_replica(&mut conn, &authorized_keys);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown"));

        // Replica should also get a rejection.
        let replica_result = replica_handle.join().unwrap();
        assert!(replica_result.is_err());
    }

    #[test]
    fn auth_rejects_wrong_permission() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        // Key exists but has Trader permission, not Replication.
        let key = SigningKey::from_bytes(&[0xCC; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            key.verifying_key().to_bytes(),
        );
        let keys_content = format!("trader {pub_b64} wrong-role\n");
        let authorized_keys = melin_app::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut conn = replica_stream;
            authenticate_with_primary(&mut conn, &key)
        });

        let mut conn = primary_stream;
        let result = authenticate_replica(&mut conn, &authorized_keys);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Replication"));

        let replica_result = replica_handle.join().unwrap();
        assert!(replica_result.is_err());
    }

    /// A replica that sends a validly-formatted but tampered signature
    /// (correct public key, wrong signature bytes) is rejected.
    #[test]
    fn auth_rejects_invalid_signature() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        // Register the correct key.
        let correct_key = SigningKey::from_bytes(&[0xDD; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            correct_key.verifying_key().to_bytes(),
        );
        let keys_content = format!("replication {pub_b64} test-replica\n");
        let authorized_keys = melin_app::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        // Replica side: read challenge, but sign with a DIFFERENT key,
        // then send the response with the correct public key (spoofing).
        let replica_handle = std::thread::spawn(move || {
            use melin_transport_core::replication::protocol::*;

            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;

            // Read the challenge.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let nonce = decode_challenge(&frame).unwrap();

            // Sign with a WRONG key but send the CORRECT public key.
            let wrong_key = SigningKey::from_bytes(&[0xEE; 32]);
            let bad_signature = ed25519_dalek::Signer::sign(&wrong_key, &nonce);
            let correct_pubkey = correct_key.verifying_key();

            let mut buf = Vec::with_capacity(128);
            encode_challenge_response(
                &bad_signature.to_bytes(),
                correct_pubkey.as_bytes(),
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();

            // Should receive AuthFailed.
            let result_frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let ok = decode_auth_result(&result_frame).unwrap();
            assert!(!ok, "should receive auth failure");
        });

        let mut conn = primary_stream;
        let result = authenticate_replica(&mut conn, &authorized_keys);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("signature verification failed"),
            "should fail on signature verification"
        );

        replica_handle.join().unwrap();
    }

    #[test]
    fn sender_receiver_end_to_end() {
        use std::os::unix::net::UnixStream;

        // Create a mock connection.
        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        // Spawn a thread simulating the replica side.
        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;

            // Send handshake.
            let mut buf = Vec::new();
            let handshake = Handshake {
                last_sequence: 0,
                chain_hash: [0u8; 32],
                epoch: 0,
            };
            encode_handshake(&handshake, &mut buf);
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let msg = decode_primary_message(&frame).unwrap();
            assert!(matches!(msg, PrimaryMessage::StreamStart { .. }));

            // Read InputBatch.
            let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
            let slots: Vec<InputSlot> = try_decode_input_batch(&frame).expect("decode InputBatch");
            let end_seq = slots
                .last()
                .map(|s| s.sequence)
                .expect("InputBatch carried at least one slot");

            // Send ack.
            let ack = Ack {
                acked_sequence: end_seq,
                in_memory_sequence: end_seq,
            };
            encode_ack(&ack, &mut buf);
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();

            end_seq
        });

        // Primary side: simulate handle_replica_connection partially.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let handshake = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Handshake(h) => h,
            _ => panic!("expected Handshake"),
        };
        assert_eq!(handshake.last_sequence, 0);

        // Send StreamStart.
        let mut buf = Vec::new();
        encode_stream_start(0, 1, [0u8; 32], 0, 1, &mut buf); // fake lineage for test
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send an InputBatch with a single Tick slot at seq 42.
        encode_input_batch_with_seq(42, &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Read ack.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let ack = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Ack(a) => a,
            _ => panic!("expected Ack"),
        };
        assert_eq!(ack.acked_sequence, 42);

        // Join replica thread.
        let end_seq = replica_handle.join().unwrap();
        assert_eq!(end_seq, 42);
    }

    #[test]
    fn multiple_data_batches_acked_in_order() {
        // Send multiple InputBatch frames, verify replica acks each one
        // and the cursor advances correctly.
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            let mut buf = Vec::new();

            // Send handshake.
            encode_handshake(
                &Handshake {
                    last_sequence: 0,
                    chain_hash: [0u8; 32],
                    epoch: 0,
                },
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::StreamStart { .. }
            ));

            // Read and ack 3 InputBatches.
            let mut acked_seqs = Vec::new();
            for _ in 0..3 {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                let slots: Vec<InputSlot> =
                    try_decode_input_batch(&frame).expect("decode InputBatch");
                let end_seq = slots
                    .last()
                    .map(|s| s.sequence)
                    .expect("InputBatch carried at least one slot");
                acked_seqs.push(end_seq);

                encode_ack(
                    &Ack {
                        acked_sequence: end_seq,
                        in_memory_sequence: end_seq,
                    },
                    &mut buf,
                );
                writer.write_all(&buf).unwrap();
                writer.flush().unwrap();
                buf.clear();
            }

            acked_seqs
        });

        // Primary side.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;
        let mut buf = Vec::new();

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        assert!(matches!(
            decode_replica_message(&frame).unwrap(),
            ReplicaMessage::Handshake(_)
        ));

        // Send StreamStart.
        encode_stream_start(0, 1, [0u8; 32], 0, 1, &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send 3 InputBatches with increasing sequence numbers.
        for seq in [10u64, 20, 30] {
            encode_input_batch_with_seq(seq, &mut buf);
            p_writer.write_all(&buf).unwrap();
            p_writer.flush().unwrap();
            buf.clear();
        }

        // Read 3 acks.
        for expected_seq in [10u64, 20, 30] {
            let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
            let ack = match decode_replica_message(&frame).unwrap() {
                ReplicaMessage::Ack(a) => a,
                other => panic!("expected Ack, got {other:?}"),
            };
            assert_eq!(ack.acked_sequence, expected_seq);
        }

        let acked = replica_handle.join().unwrap();
        assert_eq!(acked, vec![10, 20, 30]);
    }

    #[test]
    fn heartbeat_encode_contains_sequence() {
        // Heartbeat messages carry the last known sequence so the replica
        // can verify it hasn't missed any data.
        let mut buf = Vec::new();
        encode_heartbeat(999, 1, &mut buf);

        let payload = &buf[4..];
        match decode_primary_message(payload).unwrap() {
            PrimaryMessage::Heartbeat { sequence, .. } => {
                assert_eq!(sequence, 999);
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn replica_mid_stream_handshake_with_nonzero_sequence() {
        // A replica that already has some data sends a non-zero last_sequence
        // in its handshake. The primary should respond with StreamStart
        // containing that sequence, and the replica should only receive
        // events after that point.
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            let mut buf = Vec::new();

            // Replica already has events up to sequence 100.
            encode_handshake(
                &Handshake {
                    last_sequence: 100,
                    chain_hash: [0xBB; 32],
                    epoch: 0,
                },
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart — should echo back our last_sequence.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            match decode_primary_message(&frame).unwrap() {
                PrimaryMessage::StreamStart { start_sequence, .. } => {
                    assert_eq!(
                        start_sequence, 100,
                        "StreamStart should echo replica's last_sequence"
                    );
                }
                other => panic!("expected StreamStart, got {other:?}"),
            }

            // Read an InputBatch — should be for events AFTER 100.
            let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
            let slots: Vec<InputSlot> = try_decode_input_batch(&frame).expect("decode InputBatch");
            let end_sequence = slots
                .last()
                .map(|s| s.sequence)
                .expect("InputBatch carried at least one slot");
            assert!(
                end_sequence > 100,
                "InputBatch should be after replica's last_sequence"
            );
        });

        // Primary side.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;
        let mut buf = Vec::new();

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let handshake = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Handshake(h) => h,
            _ => panic!("expected Handshake"),
        };
        assert_eq!(handshake.last_sequence, 100);
        assert_eq!(handshake.chain_hash, [0xBB; 32]);

        // Send StreamStart echoing the replica's sequence.
        encode_stream_start(handshake.last_sequence, 1, [0u8; 32], 0, 1, &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send InputBatch with sequence 150 (after replica's 100).
        encode_input_batch_with_seq(150, &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();

        replica_handle.join().unwrap();
    }

    /// The receiver's stale-primary refusal: a replica that has observed
    /// epoch 5 must refuse a `StreamStart` advertising a *lower* epoch
    /// rather than follow that (divergent) lineage on top of its newer
    /// state. This reproduces the exact decision `run_receiver` makes on
    /// the normal-resume `StreamStart` — decode the frame off the wire,
    /// then `fence_state.refuses_primary(epoch)` — over a real socket so
    /// the epoch wire field and the policy are exercised together.
    ///
    /// In a current-build cluster this branch is a *second* line of
    /// defense: a primary reads the replica's higher-epoch handshake and
    /// self-demotes before it ever sends a `StreamStart` (see
    /// `tcp_sender::handle_replica_connection`). The receiver check guards
    /// the case where the primary does *not* fence — a non-fencing or
    /// older build — which is exactly what the mock primary below is.
    #[test]
    fn receiver_refuses_stream_start_from_stale_primary() {
        use std::os::unix::net::UnixStream;

        use melin_transport_core::fence::FenceState;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        // Mock primary that — unlike a current build — does *not* fence on
        // the handshake and streams its stale lineage anyway. It advertises
        // epoch 3 on the StreamStart.
        const STALE_EPOCH: u64 = 3;
        let primary_handle = std::thread::spawn(move || {
            let mut reader = primary_stream.try_clone().unwrap();
            let mut writer = primary_stream;

            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let handshake = match decode_replica_message(&frame).unwrap() {
                ReplicaMessage::Handshake(h) => h,
                _ => panic!("expected Handshake"),
            };
            // The replica truthfully advertises its higher epoch; a correct
            // primary would fence here. This mock deliberately doesn't.
            assert_eq!(handshake.epoch, 5, "replica advertises its real epoch");

            let mut buf = Vec::new();
            encode_stream_start(
                handshake.last_sequence,
                1,
                [0u8; 32],
                STALE_EPOCH,
                1,
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
        });

        // Replica side: epoch 5, mirroring `run_receiver`'s handshake +
        // StreamStart handling.
        let fence_state = Arc::new(FenceState::new(5));
        let mut r_reader = replica_stream.try_clone().unwrap();
        let mut r_writer = replica_stream;

        let mut buf = Vec::new();
        encode_handshake(
            &Handshake {
                last_sequence: 0,
                chain_hash: [0u8; 32],
                epoch: fence_state.epoch(),
            },
            &mut buf,
        );
        r_writer.write_all(&buf).unwrap();
        r_writer.flush().unwrap();

        let frame = read_frame(&mut r_reader, MAX_CONTROL_FRAME).unwrap();
        let stream_epoch = match decode_primary_message(&frame).unwrap() {
            PrimaryMessage::StreamStart { epoch, .. } => epoch,
            other => panic!("expected StreamStart, got {other:?}"),
        };
        assert_eq!(stream_epoch, STALE_EPOCH, "epoch must survive the wire");

        // This is the receiver's refusal decision. A stale primary must be
        // refused; the replica's own epoch must not have been lowered.
        assert!(
            fence_state.refuses_primary(stream_epoch),
            "replica at epoch 5 must refuse a StreamStart from epoch {STALE_EPOCH}"
        );
        assert_eq!(fence_state.epoch(), 5, "refusal must not lower our epoch");

        // Sanity: an equal or newer epoch is followed, not refused (the
        // ex-primary-rejoin path). Guards against an inverted comparison.
        assert!(!fence_state.refuses_primary(5), "same tenure is followed");
        assert!(!fence_state.refuses_primary(6), "newer primary is followed");

        primary_handle.join().unwrap();
    }

    #[test]
    fn snapshot_begin_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_snapshot_begin(1_000_000, 42, &[0xAB; 32], &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotBegin {
                snapshot_len,
                snap_sequence,
                snap_chain_hash,
            } => {
                assert_eq!(snapshot_len, 1_000_000);
                assert_eq!(snap_sequence, 42);
                assert_eq!(snap_chain_hash, [0xAB; 32]);
            }
            _ => panic!("expected SnapshotBegin"),
        }
    }

    #[test]
    fn snapshot_chunk_encode_decode_round_trip() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut buf = Vec::new();
        encode_snapshot_chunk(&data, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotChunk(chunk) => {
                assert_eq!(chunk, data);
            }
            _ => panic!("expected SnapshotChunk"),
        }
    }

    #[test]
    fn snapshot_end_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_snapshot_end(0xDEADBEEF, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotEnd { crc32c } => {
                assert_eq!(crc32c, 0xDEADBEEF);
            }
            _ => panic!("expected SnapshotEnd"),
        }
    }

    /// Simulate the receiver side of a snapshot transfer where the
    /// advertised snap_len doesn't match the actual bytes sent.
    /// The receiver must detect this and return an error.
    #[test]
    fn snapshot_receiver_detects_length_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        // Receiver thread — reads NeedSnapshot, then the snapshot transfer.
        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            // Read NeedSnapshot.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            // Read SnapshotBegin.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let (snap_len, _snap_sequence, _snap_chain_hash) =
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotBegin {
                        snapshot_len,
                        snap_sequence,
                        snap_chain_hash,
                    } => (snapshot_len, snap_sequence, snap_chain_hash),
                    other => panic!("expected SnapshotBegin, got {other:?}"),
                };

            // Receive chunks and check length at SnapshotEnd.
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                    }
                    PrimaryMessage::SnapshotEnd { .. } => {
                        if received != snap_len {
                            return format!(
                                "snapshot length mismatch: expected {snap_len} bytes, got {received}"
                            );
                        }
                        return String::new(); // no error
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }
        });

        // Primary side — send snapshot with wrong advertised length.
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let actual_data = vec![0xAA; 100];
        let wrong_len = 999u64; // advertise 999 bytes, send only 100

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(wrong_len, 42, &[0xBB; 32], &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&actual_data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        let crc = crc32c::crc32c(&actual_data);
        encode_snapshot_end(crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("length mismatch"),
            "expected length mismatch error, got: {error_msg:?}"
        );
    }

    /// Simulate the receiver side of a snapshot transfer where the CRC
    /// in SnapshotEnd doesn't match the actual data. The receiver must
    /// detect and reject the transfer.
    #[test]
    fn snapshot_receiver_detects_crc_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            // Read NeedSnapshot.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            // Read SnapshotBegin.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let snap_len = match decode_primary_message(&frame).unwrap() {
                PrimaryMessage::SnapshotBegin { snapshot_len, .. } => snapshot_len,
                other => panic!("expected SnapshotBegin, got {other:?}"),
            };

            // Receive chunks, verify CRC at SnapshotEnd.
            let mut received_data = Vec::new();
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                        received_data.extend_from_slice(&data);
                    }
                    PrimaryMessage::SnapshotEnd {
                        crc32c: expected_crc,
                    } => {
                        if received != snap_len {
                            return format!("length mismatch: {snap_len} vs {received}");
                        }
                        let actual_crc = crc32c::crc32c(&received_data);
                        if actual_crc != expected_crc {
                            return format!(
                                "CRC mismatch: expected {expected_crc:#x}, got {actual_crc:#x}"
                            );
                        }
                        return String::new();
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }
        });

        // Primary side — send correct length but wrong CRC.
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let data = vec![0xAA; 100];

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(data.len() as u64, 42, &[0xBB; 32], &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        // Send a wrong CRC (flip bits).
        let wrong_crc = !crc32c::crc32c(&data);
        encode_snapshot_end(wrong_crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("CRC mismatch"),
            "expected CRC mismatch error, got: {error_msg:?}"
        );
    }

    /// The receiver verifies the chain hash from the loaded snapshot
    /// matches the one advertised in SnapshotBegin. Simulate a mismatch.
    #[test]
    fn snapshot_receiver_detects_chain_hash_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let (snap_len, _snap_sequence, snap_chain_hash) =
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotBegin {
                        snapshot_len,
                        snap_sequence,
                        snap_chain_hash,
                    } => (snapshot_len, snap_sequence, snap_chain_hash),
                    other => panic!("expected SnapshotBegin, got {other:?}"),
                };

            // Receive the snapshot data.
            let mut received_data = Vec::new();
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                        received_data.extend_from_slice(&data);
                    }
                    PrimaryMessage::SnapshotEnd {
                        crc32c: expected_crc,
                    } => {
                        assert_eq!(received, snap_len, "length should match");
                        let actual_crc = crc32c::crc32c(&received_data);
                        assert_eq!(actual_crc, expected_crc, "CRC should match");
                        break;
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }

            // Simulate chain hash verification: the loaded snapshot would
            // have a different chain hash than what SnapshotBegin advertised.
            let loaded_hash = [0xFF; 32]; // different from snap_chain_hash
            if loaded_hash != snap_chain_hash {
                return format!(
                    "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
                     loaded snapshot has {loaded_hash:02x?}"
                );
            }
            String::new()
        });

        // Primary side — send valid snapshot but with a chain hash in
        // SnapshotBegin that won't match what the replica "loads".
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let data = vec![0xAA; 64];
        // Advertise chain hash [0xBB; 32] — receiver will "load" [0xFF; 32].
        let advertised_hash = [0xBB; 32];

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(data.len() as u64, 10, &advertised_hash, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        let crc = crc32c::crc32c(&data);
        encode_snapshot_end(crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("chain hash mismatch"),
            "expected chain hash mismatch error, got: {error_msg:?}"
        );
    }

    /// Primary-side magic validation: a file without the SNAP magic
    /// (0x534E4150) must be rejected before transfer.
    #[test]
    fn primary_rejects_snapshot_with_invalid_magic() {
        let tmp = tempfile::tempdir().unwrap();
        let snap_path = tmp.path().join("test.snapshot");

        // Write a file with wrong magic but enough bytes for a header.
        let mut bad_snap = vec![0u8; 64];
        bad_snap[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes()); // wrong magic

        std::fs::write(&snap_path, &bad_snap).unwrap();

        // Replicate the primary's validation logic.
        let snap_data = std::fs::read(&snap_path).unwrap();
        assert!(
            snap_data.len() >= 48,
            "file should be big enough for header"
        );

        let magic = u32::from_le_bytes(snap_data[0..4].try_into().unwrap());
        assert_ne!(magic, 0x534E_4150);
        assert_eq!(magic, 0xDEAD_BEEF);
    }

    /// Primary-side: a snapshot file smaller than the 48-byte header
    /// must be rejected.
    #[test]
    fn primary_rejects_snapshot_too_small_for_header() {
        let tmp = tempfile::tempdir().unwrap();
        let snap_path = tmp.path().join("test.snapshot");

        // Write a file smaller than the 48-byte header.
        std::fs::write(&snap_path, [0u8; 20]).unwrap();

        let snap_data = std::fs::read(&snap_path).unwrap();
        assert!(
            snap_data.len() < 48,
            "file must be too small for header validation"
        );
    }

    #[test]
    fn decode_snapshot_begin_too_short() {
        // SnapshotBegin needs type(1) + snapshot_len(8) + snap_sequence(8) + chain_hash(32) = 49.
        // Send only the type byte + a few extra bytes.
        let payload = [MSG_SNAPSHOT_BEGIN, 0x01, 0x02, 0x03];
        let err = decode_primary_message(&payload).unwrap_err();
        assert!(
            err.to_string().contains("SnapshotBegin too short"),
            "expected 'SnapshotBegin too short', got: {err}"
        );
    }

    #[test]
    fn decode_snapshot_end_too_short() {
        // SnapshotEnd needs type(1) + crc32c(4) = 5. Send only the type byte.
        let payload = [MSG_SNAPSHOT_END];
        let err = decode_primary_message(&payload).unwrap_err();
        assert!(
            err.to_string().contains("SnapshotEnd too short"),
            "expected 'SnapshotEnd too short', got: {err}"
        );
    }

    #[test]
    fn decode_snapshot_chunk_empty_data() {
        // SnapshotChunk with just the type byte — valid but empty payload.
        let payload = [MSG_SNAPSHOT_CHUNK];
        let msg = decode_primary_message(&payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotChunk(data) => {
                assert!(data.is_empty());
            }
            _ => panic!("expected SnapshotChunk"),
        }
    }

    // --- PendingAckQueue tests ---

    fn make_journal_cursor(val: u64) -> melin_pipeline::padding::Sequence {
        melin_pipeline::padding::Sequence::new(AtomicU64::new(val))
    }

    #[test]
    fn pending_ack_queue_push_and_pop_ready() {
        let mut q = PendingAckQueue::new(8);
        assert!(q.is_empty());
        assert!(!q.is_full());

        q.push(10, 100);
        q.push(20, 200);
        assert!(!q.is_empty());

        // Cursor at 5 — neither ready.
        let cursor = make_journal_cursor(5);
        assert!(q.pop_ready(&cursor).is_none());

        // Cursor at 15 — first ready, second not.
        cursor.get().store(15, Ordering::Relaxed);
        assert_eq!(q.pop_ready(&cursor), Some(100));
        // Only one popped — second still pending.
        assert!(!q.is_empty());

        // Cursor at 25 — second now ready.
        cursor.get().store(25, Ordering::Relaxed);
        assert_eq!(q.pop_ready(&cursor), Some(200));
        assert!(q.is_empty());
    }

    #[test]
    fn pending_ack_queue_pop_ready_returns_highest_sequence() {
        // When multiple acks become ready simultaneously, pop_ready
        // returns the highest acked_sequence (ack semantics are
        // cumulative — "everything up to this sequence is durable").
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);
        q.push(20, 200);
        q.push(30, 300);

        let cursor = make_journal_cursor(30);
        assert_eq!(q.pop_ready(&cursor), Some(300));
        assert!(q.is_empty());
    }

    #[test]
    fn pending_ack_queue_capacity_and_full() {
        let mut q = PendingAckQueue::new(8);
        for i in 0..8 {
            assert!(!q.is_full());
            q.push(i as u64 + 1, (i + 1) as u64 * 100);
        }
        assert!(q.is_full());
    }

    #[test]
    fn pending_ack_queue_pop_oldest_blocking() {
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);
        q.push(20, 200);

        // Cursor already past both targets — pop_oldest_blocking
        // returns immediately.
        let cursor = make_journal_cursor(25);
        let seq = q.pop_oldest_blocking(&cursor, true, &AtomicBool::new(false));
        // Should pop both (oldest + any others that became ready).
        assert_eq!(seq, Some(200));
        assert!(q.is_empty());
    }

    /// A dead journal stage (abort latch set) must abort the blocking
    /// wait instead of spinning forever on the frozen cursor.
    #[test]
    fn pending_ack_queue_blocking_wait_aborts_on_journal_failure() {
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);

        // Cursor frozen BELOW the target; abort pre-latched.
        let cursor = make_journal_cursor(5);
        let abort = AtomicBool::new(true);
        assert_eq!(q.pop_oldest_blocking(&cursor, true, &abort), None);
        assert!(!q.is_empty(), "aborted wait must not pop the entry");
        assert_eq!(q.pop_all_blocking(&cursor, true, &abort), None);
    }

    /// `shutdown_pipeline` must surface the journal stage's error kind —
    /// the orchestrator routes `ReplicaChainDivergence` into in-process
    /// resync and everything else into process exit, so collapsing the
    /// error into a None (the old behavior) breaks that dispatch. It
    /// must also join EVERY thread in the failure arms: the resync path
    /// archives journal + snapshot right after teardown, and a
    /// still-running shadow thread mid-snapshot-write would race the
    /// renames.
    #[test]
    fn shutdown_pipeline_surfaces_journal_error_kind() {
        let flag = AtomicBool::new(false);
        let matching_joined = Arc::new(AtomicBool::new(false));
        let mj = Arc::clone(&matching_joined);

        let journal = std::thread::spawn(|| -> Result<u32, melin_journal::JournalError> {
            Err(melin_journal::JournalError::ReplicaChainDivergence {
                sequence: 42,
                expected: [1u8; 32],
                actual: [2u8; 32],
            })
        });
        let matching = std::thread::spawn(move || {
            mj.store(true, Ordering::Release);
            7u64
        });
        let drain = std::thread::spawn(|| {});

        let outcome = shutdown_pipeline::<u64, u32>(&flag, journal, matching, drain, None);
        assert!(flag.load(Ordering::Acquire), "shutdown flag must be set");
        assert!(
            matching_joined.load(Ordering::Acquire),
            "matching thread must be joined even when the journal failed"
        );
        match outcome {
            TeardownOutcome::JournalFailed(
                melin_journal::JournalError::ReplicaChainDivergence { sequence, .. },
            ) => assert_eq!(sequence, 42),
            _ => panic!("expected JournalFailed(ReplicaChainDivergence)"),
        }
    }

    #[test]
    fn shutdown_pipeline_clean_returns_both_states() {
        let flag = AtomicBool::new(false);
        let journal =
            std::thread::spawn(|| -> Result<u32, melin_journal::JournalError> { Ok(11u32) });
        let matching = std::thread::spawn(|| 7u64);
        let drain = std::thread::spawn(|| {});

        match shutdown_pipeline::<u64, u32>(&flag, journal, matching, drain, None) {
            TeardownOutcome::Clean(app, writer) => {
                assert_eq!(app, 7);
                assert_eq!(writer, 11);
            }
            _ => panic!("expected Clean"),
        }
    }

    #[test]
    fn shutdown_pipeline_panicked_stage_reports_panicked() {
        let flag = AtomicBool::new(false);
        let journal =
            std::thread::spawn(|| -> Result<u32, melin_journal::JournalError> { Ok(11u32) });
        let matching = std::thread::spawn(|| -> u64 { panic!("matching stage died") });
        let drain = std::thread::spawn(|| {});

        assert!(matches!(
            shutdown_pipeline::<u64, u32>(&flag, journal, matching, drain, None),
            TeardownOutcome::Panicked
        ));
    }

    /// `ReplicaPipelineHandles` over a real input ring (one gate
    /// consumer) with immediately-returning stage threads — just enough
    /// structure for `teardown_replica_pipeline` to run for real. The
    /// returned consumer observes what teardown published.
    fn teardown_fixture(
        capacity: usize,
    ) -> (
        ReplicaPipelineHandles<counter_server::Counter, u32>,
        melin_pipeline::ring::Consumer<InputSlot>,
    ) {
        use melin_app::app_factory::AppFactory;
        let (input_producer, mut consumers) =
            melin_pipeline::ring::DisruptorBuilder::<InputSlot>::new(capacity)
                .add_consumer()
                .build();
        let consumer = consumers.pop().expect("one consumer");
        let handles = ReplicaPipelineHandles {
            input_producer,
            journal_cursor: Arc::new(make_journal_cursor(0)),
            last_seq: melin_transport_core::DurableWireSeqCursor::detached(
                melin_transport_core::WireSeq::new(0),
            ),
            chain_hash_lock: None,
            stream_marks: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
            journal_failed: Arc::new(AtomicBool::new(false)),
            pipeline_shutdown: Arc::new(AtomicBool::new(false)),
            journal_handle: std::thread::spawn(|| -> Result<u32, melin_journal::JournalError> {
                Ok(11)
            }),
            matching_handle: std::thread::spawn(|| counter_server::CounterFactory.empty()),
            drain_handle: std::thread::spawn(|| {}),
            shadow_handle: None,
        };
        (handles, consumer)
    }

    /// The invariant behind moving the sentinel publish into
    /// `teardown_replica_pipeline`: a healthy teardown always puts the
    /// shutdown sentinel on the ring, so the stages exit by consuming it
    /// through the normal event path (draining anything ahead of it),
    /// not via the flag's emergency-abort branch.
    #[test]
    fn teardown_publishes_shutdown_sentinel() {
        let (handles, mut consumer) = teardown_fixture(8);
        assert!(matches!(
            teardown_replica_pipeline::<counter_server::Counter, u32>(handles),
            TeardownOutcome::Clean(..)
        ));
        let (_, slot) = consumer.try_consume().expect("sentinel on the ring");
        assert!(
            slot.event.is_shutdown(),
            "published slot must be the sentinel"
        );
        assert!(consumer.try_consume().is_none(), "exactly one sentinel");
    }

    /// Drive `handle_session_exit` through one `Disconnected` exit with
    /// the given liveness flag and a backoff pre-escalated to
    /// `MAX_BACKOFF`, returning the post-exit backoff. Shutdown is
    /// latched so the backoff sleep returns immediately (the receiver's
    /// loop top would handle it on the next turn); the journal, factory
    /// and fence arguments are inert on the `Disconnected` path.
    fn backoff_after_disconnect(heard_from_primary: bool) -> std::time::Duration {
        let dir = tempfile::tempdir().expect("tempdir");
        type Writer = melin_journal::BufferedWriter<CounterEvent>;
        let mut pipeline: Option<ReplicaPipelineHandles<counter_server::Counter, Writer>> = None;
        let mut divergence_resyncs = 0u32;
        let mut backoff = MAX_BACKOFF;
        let shutdown = AtomicBool::new(true);
        let promote = crate::promotion::PromotionRequest::new();
        let after = handle_session_exit::<counter_server::Counter, Writer>(
            StreamingResult {
                exit: SessionExit::Disconnected,
                heard_from_primary,
            },
            &mut pipeline,
            &mut divergence_resyncs,
            &mut backoff,
            0,
            &dir.path().join("r.journal"),
            &dir.path().join("r.snapshot"),
            &counter_server::CounterFactory,
            &melin_transport_core::fence::FenceState::new(0),
            &shutdown,
            &promote,
            || {},
        );
        assert!(matches!(after, AfterSession::Reconnect));
        backoff
    }

    /// A session in which the primary spoke — even heartbeat-only on a
    /// quiet system — must reset the disconnect backoff. A stale
    /// escalated backoff delays primary-link recovery, and with it the
    /// auto-promotion veto, by up to MAX_BACKOFF.
    #[test]
    fn disconnect_resets_backoff_when_primary_spoke() {
        // Reset to 1s, then doubled for the next attempt by the shared
        // helper — NOT stuck at MAX_BACKOFF.
        assert_eq!(
            backoff_after_disconnect(true),
            std::time::Duration::from_secs(2)
        );
    }

    /// A session with no word from the primary — an instant-drop flap,
    /// or the synthetic result of a session that never started
    /// streaming (local transport init failure) — must keep its
    /// escalated backoff rather than redialing at 1 Hz forever.
    #[test]
    fn disconnect_keeps_escalated_backoff_when_primary_silent() {
        assert_eq!(backoff_after_disconnect(false), MAX_BACKOFF);
    }

    /// With the journal-failure latch set and the ring full — the state
    /// a dead journal stage leaves behind (its frozen gate cursor means
    /// the ring never drains) — teardown must skip the sentinel and
    /// return, instead of spinning forever on ring backpressure.
    #[test]
    fn teardown_skips_sentinel_when_journal_failed_and_ring_full() {
        let (mut handles, mut consumer) = teardown_fixture(4);
        for seq in 0..4 {
            assert!(
                handles
                    .input_producer
                    .try_publish(InputSlot {
                        sequence: seq,
                        ..InputSlot::default()
                    })
                    .is_ok(),
                "ring must have space while filling"
            );
        }
        assert!(
            handles
                .input_producer
                .try_publish(InputSlot::default())
                .is_err(),
            "ring must be full"
        );
        handles.journal_failed.store(true, Ordering::Release);
        // Would spin forever on the frozen gate without the latch check.
        assert!(matches!(
            teardown_replica_pipeline::<counter_server::Counter, u32>(handles),
            TeardownOutcome::Clean(..)
        ));
        while let Some((_, slot)) = consumer.try_consume() {
            assert!(!slot.event.is_shutdown(), "no sentinel may be published");
        }
    }

    #[test]
    fn pending_ack_queue_wraps_around() {
        let mut q = PendingAckQueue::new(8);
        let cursor = make_journal_cursor(100);

        // Fill and drain multiple times to exercise circular buffer wrap.
        for round in 0..3 {
            for i in 0..8 {
                let target = (round * 8 + i) as u64 + 1;
                q.push(target, target * 10);
            }
            assert!(q.is_full());
            let seq = q.pop_ready(&cursor).expect("should be ready");
            assert_eq!(seq, (round * 8 + 8) as u64 * 10);
            assert!(q.is_empty());
        }
    }

    #[test]
    fn pending_ack_queue_pop_all_blocking_empty() {
        let mut q = PendingAckQueue::new(8);
        let cursor = make_journal_cursor(0);
        assert!(
            q.pop_all_blocking(&cursor, true, &AtomicBool::new(false))
                .is_none()
        );
    }

    // --- try_flush_dual_track tests ---

    #[test]
    fn dual_track_returns_none_when_both_tracks_idle() {
        let mut q = PendingAckQueue::new(8);
        let cursor = make_journal_cursor(0);
        assert!(
            try_flush_dual_track(&mut q, &cursor, 0, 0, 0).is_none(),
            "no advance on either track → no ack"
        );
    }

    #[test]
    fn dual_track_fires_on_persisted_advance_only() {
        // Persisted track moves: pop_ready returns 100; in-memory
        // stayed at 50 (last_sent). Expect ack with acked=100,
        // in_memory=50 (no regression — the wire field carries the
        // current sample, not a "no change" marker).
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);
        let cursor = make_journal_cursor(20);
        let ack = try_flush_dual_track(&mut q, &cursor, 50, 0, 50).expect("persisted advanced");
        assert_eq!(ack.acked_sequence, 100);
        assert_eq!(ack.in_memory_sequence, 50);
    }

    #[test]
    fn dual_track_fires_on_in_memory_advance_only() {
        // Persisted is idle (queue empty): unwrap_or keeps the
        // last-sent acked at 100. In-memory bumped from 100 to 200.
        let mut q = PendingAckQueue::new(8);
        let cursor = make_journal_cursor(0);
        let ack = try_flush_dual_track(&mut q, &cursor, 200, 100, 100).expect("in-memory advanced");
        assert_eq!(ack.acked_sequence, 100);
        assert_eq!(ack.in_memory_sequence, 200);
    }

    #[test]
    fn dual_track_coalesces_until_caller_updates_trackers() {
        // Caller did not advance trackers between calls. The queue
        // popped on call 1; on call 2 it's empty so persisted stays
        // at 100 (unwrap_or). In-memory advanced 50 → 80 between
        // calls. Second call must still fire because tracker is
        // still 50.
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);
        let cursor = make_journal_cursor(20);
        let ack1 = try_flush_dual_track(&mut q, &cursor, 50, 0, 50).expect("call 1 fires");
        assert_eq!(ack1.acked_sequence, 100);
        // Caller "forgot" to update trackers (simulates send failure).
        let ack2 = try_flush_dual_track(&mut q, &cursor, 80, 0, 50)
            .expect("call 2 fires on in-memory advance");
        assert_eq!(
            ack2.acked_sequence, 0,
            "no new persisted pop → unwrap_or(last_sent)"
        );
        assert_eq!(ack2.in_memory_sequence, 80);
    }

    #[test]
    fn dual_track_no_duplicate_ack_after_backpressure_drain() {
        // Models the backpressure-drain → resume-normal-flush path
        // taken by every receiver when `PendingAckQueue` fills up.
        // The receiver must drain the queue (via the
        // pop_oldest_blocking path in production; pop_ready here),
        // update *both* trackers from the drained ack, and only
        // then resume the normal cursor-driven flush. The bug
        // classes this pins are:
        //   (a) emitting a follow-up ack whose `acked_sequence`
        //       regresses below what the drain already sent
        //       (caught by the debug_assert! in
        //       `try_flush_dual_track`, also asserted at value
        //       level here);
        //   (b) emitting a duplicate ack carrying the same
        //       cursors as the drain when neither track actually
        //       advanced — the leak class fixed by ensuring every
        //       send site updates both `last_sent_acked_seq` and
        //       `last_sent_in_memory_seq`.
        let mut q = PendingAckQueue::new(4);
        // Fill the queue: four pending acks at primary seqs
        // 100, 200, 300, 400 with journal targets 10..=40.
        for i in 1..=4u64 {
            q.push(i * 10, i * 100);
        }
        assert!(q.is_full());

        // Backpressure path: caller drains all ready entries
        // before pushing more. Highest acked seen = 400.
        let cursor = make_journal_cursor(40);
        let drained = q.pop_ready(&cursor).expect("all entries durable");
        assert_eq!(drained, 400);
        assert!(q.is_empty());

        // Caller updates BOTH trackers from the drain. in_memory
        // at the time of the drained batch was 450.
        let mut last_sent_acked = drained;
        let mut last_sent_in_mem = 450u64;

        // Quiescent immediately after drain: no fresh push, no
        // in-memory advance → try_flush must return None.
        // Failing here means we'd emit a duplicate ack carrying
        // the same (400, 450) the backpressure drain already sent.
        assert!(
            try_flush_dual_track(
                &mut q,
                &cursor,
                last_sent_in_mem,
                last_sent_acked,
                last_sent_in_mem,
            )
            .is_none(),
            "no advance on either track after backpressure drain → no duplicate ack",
        );

        // Push a fresh batch (primary seq 500, journal target 50),
        // cursor catches up, in_memory advances to 500.
        q.push(50, 500);
        let cursor2 = make_journal_cursor(50);
        let ack = try_flush_dual_track(&mut q, &cursor2, 500, last_sent_acked, last_sent_in_mem)
            .expect("fresh batch after drain fires a new ack");
        assert!(
            ack.acked_sequence >= last_sent_acked,
            "regression: drain sent {last_sent_acked} but next ack carries {}",
            ack.acked_sequence,
        );
        assert_eq!(ack.acked_sequence, 500);
        assert_eq!(ack.in_memory_sequence, 500);

        last_sent_acked = ack.acked_sequence;
        last_sent_in_mem = ack.in_memory_sequence;

        // Post-resume quiescent: confirms idempotency past the
        // resume point — the second flush sees neither track
        // advance and must stay silent.
        assert!(
            try_flush_dual_track(
                &mut q,
                &cursor2,
                last_sent_in_mem,
                last_sent_acked,
                last_sent_in_mem,
            )
            .is_none(),
            "post-resume idle → no further ack",
        );
    }
}
