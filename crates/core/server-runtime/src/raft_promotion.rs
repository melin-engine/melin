//! Raft-driven auto-promotion: act on a control-plane election win by
//! filing a [`PromotionRequest`] carrying the election term.
//!
//! A plain `std` thread polling the [`RaftStatus`] atomics the driver
//! publishes — NOT async code inside the driver. The policy's inputs
//! (acking mode, primary link state, tip readiness, fence state) are all
//! data-plane concepts owned by this crate, and a synchronous poll keeps
//! the decision unit-testable without tokio. The 100 ms cadence is far
//! inside the 1–2 s election timeout, so a leadership win is acted on
//! effectively immediately.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use melin_raft::recency::{JournalTip, PeerTips};
use melin_transport_core::health::RaftStatus;
use tracing::{info, warn};

use crate::durability_policy::DurabilityMode;
use crate::replication::ReplicaControlPlane;

/// Poll cadence for the promotion thread. Matches the codebase's
/// listener-loop convention; promotion latency is bounded by this plus
/// the driver's own 100 ms metrics bridge.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long this replica's link to the primary must be *continuously*
/// down before auto-promotion may fire.
///
/// `primary_link_up` reflects only *this* node's replication socket — not
/// whether the primary is actually dead — and the receiver clears it on
/// every reconnect attempt. So the bare flag cannot tell a real primary
/// failure from a transient network blip. Acting on the instantaneous
/// flag lets a replica that happens to hold control-plane leadership
/// depose a perfectly healthy primary the moment its own link hiccups.
///
/// The distinguisher is *duration*: the receiver reconnects a healthy
/// primary in milliseconds, and even a brief unreachability retries on a
/// 1 s → 2 s → … backoff, so a link that stays down well past the first
/// backoff cycle is strong evidence the primary is genuinely gone. Three
/// seconds clears one full reconnect cycle with margin. This bounds how
/// long a real failover waits — a few seconds of extra downtime bought in
/// exchange for never failing over on a blip (correctness first).
///
/// What the grace deliberately does NOT cover: a primary whose *startup*
/// (journal recovery, prefault) outlasts it — the grace clock measures
/// this replica's own view of the link, not the primary's progress. A
/// blank replica is stopped by the genesis refusal in
/// [`auto_promotion_decision`]; a data-bearing replica deposing a
/// still-recovering primary is accepted as a real failover (the
/// ex-primary is fenced on contact), and the operator rule is to bring
/// the cluster up primary-first — see `docs/replication.md`.
const PRIMARY_DOWN_GRACE: Duration = Duration::from_secs(3);

/// How long a configured peer may go unheard before the journal-safety
/// veto treats it as dead rather than merely not-yet-sampled.
///
/// The clock this grace runs against is *leadership tenure*: only
/// leaders and candidates emit control-plane RPCs, so a node that just
/// won an election starts with an empty peer-tip table and fills it
/// from the heartbeat responses that begin immediately (200–350 ms
/// interval — a live peer produces its first sample within one round).
/// A peer still unheard after several rounds is down — typically the
/// dead primary the failover is *for* — and must not hold up promotion
/// forever. 1.5 s covers four-plus heartbeat rounds with margin while
/// bounding the extra failover delay it can add.
///
/// The same window ages recorded samples: a peer whose last tip is
/// older than this has stopped answering heartbeats and is treated as
/// dead too — its data is unreachable, so nothing better is available
/// and promotion may proceed (loudly, if its last tip was ahead).
const PEER_TIP_GRACE: Duration = Duration::from_millis(1500);

/// One configured peer's journal-tip observation, digested from
/// [`PeerTips`] by the poll loop so [`auto_promotion_decision`] stays a
/// pure function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerTipObservation {
    /// Heard within [`PEER_TIP_GRACE`] — the tip is current.
    Fresh(JournalTip),
    /// Dead as far as this node can tell: last heard longer than the
    /// grace ago (with its final tip), or never heard although we have
    /// been leader — i.e. heartbeating it — for longer than the grace.
    Silent { last: Option<JournalTip> },
    /// Never heard, and we have not been leader long enough to conclude
    /// anything. The veto waits.
    Unknown,
}

/// Digest one peer's [`PeerTips`] sample into an observation.
/// `leader_for` is how long this node has continuously been leader in
/// the current term — the only sound baseline for "we have been trying
/// to reach it", since only leaders emit RPCs.
fn observe_peer(
    peer_tips: &PeerTips,
    peer: u64,
    leader_for: Duration,
    now: Instant,
) -> PeerTipObservation {
    match peer_tips.sample(peer, now) {
        Some((tip, age)) if age <= PEER_TIP_GRACE => PeerTipObservation::Fresh(tip),
        Some((tip, _)) => PeerTipObservation::Silent { last: Some(tip) },
        None if leader_for > PEER_TIP_GRACE => PeerTipObservation::Silent { last: None },
        None => PeerTipObservation::Unknown,
    }
}

/// The durability mode the auto-promotion refusal judges: the mode
/// the *primary* last advertised on the replication stream — that is
/// the gate acked orders actually passed through — falling back to
/// this node's own configured mode while no primary has ever been
/// observed (`ACKING_MODE_UNKNOWN`), which is exactly the
/// pre-propagation behavior. `None` for an unrecognised byte (e.g. a
/// newer node's mode) — the caller refuses on it.
fn effective_acking_mode(observed: u8, local_fallback: u8) -> Option<DurabilityMode> {
    let byte = if observed == crate::durability_policy::ACKING_MODE_UNKNOWN {
        local_fallback
    } else {
        observed
    };
    DurabilityMode::from_u8(byte)
}

/// Everything the auto-promotion rule looks at, snapshotted from the
/// shared atomics so the decision itself is a pure function
/// ([`auto_promotion_decision`]) the tests can drive exhaustively.
struct AutoPromotionInputs {
    /// Journal recovery has seeded the fence epoch and advertised tip.
    tip_ready: bool,
    /// This node has been fenced (superseded) — it must never lead.
    fenced: bool,
    /// The acking durability mode ([`effective_acking_mode`]), `None`
    /// for an unrecognised byte.
    durability_mode: Option<DurabilityMode>,
    /// The replication link to the primary is authenticated and live.
    primary_link_up: bool,
    /// How long that link has been *continuously* down (`ZERO` while it
    /// is up). Auto-promotion waits for this to reach
    /// [`PRIMARY_DOWN_GRACE`] so a transient blip cannot trip a
    /// failover; the link state is one-sided (this node's socket only).
    primary_link_down_for: Duration,
    /// A primary has been observed at least once since this process
    /// booted (streaming started, i.e. the acking-mode gauge left
    /// `ACKING_MODE_UNKNOWN`). Process-lifetime, not per-session.
    primary_observed: bool,
    /// This node's own journal tip (fencing epoch + advertised
    /// sequence) — the local side of the peer-tip veto, and
    /// `last_sequence == 0` is the blank-genesis-node signal.
    local_tip: JournalTip,
    /// Journal-tip observation for every *other* configured voter,
    /// digested by the poll loop (see [`observe_peer`]).
    peers: Vec<(u64, PeerTipObservation)>,
    /// The term this node was elected at.
    term: u64,
    /// The fencing epoch currently in force.
    fence_epoch: u64,
}

/// Should a replica that just won a control-plane election promote
/// itself? `Err` carries the operator-facing refusal reason.
///
/// The election itself is the data-safety proof: the recency filter
/// means a quorum of voters held no more data than this node (see
/// `melin_raft::recency`). The rules here cover what an election
/// cannot prove:
///
/// - `tip_ready` / `fenced` — the tip that won the election must be
///   real, and a superseded node must stay down.
/// - `primary_link_up` — a live authenticated link means the primary
///   is alive; leadership may still land here (e.g. the previous raft
///   leader was a *replica* whose process died), and promoting would
///   depose a healthy primary.
/// - `primary_link_down_for` — the link flag is one-sided (this node's
///   socket only), so `link down` on its own cannot tell a transient
///   blip from a real failure. Require the link to have been down
///   continuously past [`PRIMARY_DOWN_GRACE`]; a healthy primary
///   reconnects well inside that window.
/// - `primary_observed` / `journal_empty` — a node that has never seen
///   a primary this boot *and* holds no journal data is a blank genesis
///   node: an election among blank nodes proves nothing about acked
///   data, and the likeliest reason such a node leads is a cluster
///   bring-up race — promoting would depose a primary that is merely
///   slow to start (journal recovery outlasting the link grace). A
///   restarted data-bearing replica (journal non-empty) is unaffected
///   and may still win a real failover.
/// - `local` durability — acks in `local` mode never waited for this
///   replica, so no election can prove it holds every acked order.
///   Failover stays a manual, eyes-on decision.
/// - `peers` — the authoritative journal-safety check the vote filter
///   defers to (see `melin_raft::recency`). The election is only
///   best-effort steering: a behind node can win via the filter's
///   liveness escape, or hold leadership from before the failure. So
///   the decision independently refuses while any reachable peer
///   advertises a tip ahead of ours — under `hybrid`/`replicated` the
///   ack quorum is the primary plus the *fastest* replica, so the
///   slower replica legitimately lacks the newest acked events and
///   must not serve. A peer still `Unknown` (heartbeat responses not
///   yet arrived) also blocks, briefly; a `Silent` peer does not — its
///   data is unreachable, so nothing better is available.
/// - `term > fence_epoch` — the promotion journals `epoch = term`, so
///   the term must be strictly newer than every epoch already in
///   force; two auto-promotions from different elections then always
///   allocate distinct epochs and the newer fences the older. Epochs
///   outrunning terms (a history of manual promotions) breaks the
///   alignment until enough elections pass — refuse rather than risk
///   an epoch collision.
fn auto_promotion_decision(inputs: &AutoPromotionInputs) -> Result<(), &'static str> {
    if !inputs.tip_ready {
        return Err("journal recovery has not seeded this node's tip yet");
    }
    if inputs.fenced {
        return Err("node is fenced (superseded by a newer primary)");
    }
    if inputs.primary_link_up {
        return Err("replication link to the primary is up — refusing to depose a live primary");
    }
    if inputs.primary_link_down_for < PRIMARY_DOWN_GRACE {
        return Err(
            "the primary link dropped only moments ago — waiting out the grace period to tell \
             a transient blip from a real failure before failover",
        );
    }
    if !inputs.primary_observed && inputs.local_tip.last_sequence == 0 {
        return Err(
            "no primary has been observed since boot and the local journal is empty — a blank \
             genesis node must not depose a primary that may still be starting; bring the \
             primary up first, or promote manually",
        );
    }
    for (_peer, obs) in &inputs.peers {
        match obs {
            PeerTipObservation::Fresh(tip) if tip.is_ahead_of(inputs.local_tip) => {
                return Err(
                    "a live peer advertises a journal tip ahead of ours — it holds acked events \
                     this node lacks; refusing to promote so the caught-up peer can take over \
                     (it campaigns on its own once it sees this leadership)",
                );
            }
            PeerTipObservation::Fresh(_) | PeerTipObservation::Silent { .. } => {}
            PeerTipObservation::Unknown => {
                return Err(
                    "a configured peer's journal tip is still unknown — waiting up to one \
                     peer-tip grace to learn whether it holds acked events this node lacks",
                );
            }
        }
    }
    match inputs.durability_mode {
        Some(DurabilityMode::Local) => {
            return Err(
                "the primary acks under `local` durability — an election win cannot prove \
                 this node holds every acked order; promote manually if the lag is acceptable",
            );
        }
        None => return Err("acking durability mode is unrecognised"),
        // Every mode whose policy requires a second node before the ack
        // (`in_memory>=2` or `persisted>=2`) qualifies: the election's
        // recency filter can then prove the winner holds every acked
        // order. `replicated` qualifies on the same grounds as `hybrid`
        // — its acks waited for a second node's in-memory receipt.
        Some(
            DurabilityMode::Hybrid | DurabilityMode::DurablyReplicated | DurabilityMode::Replicated,
        ) => {}
    }
    if inputs.term <= inputs.fence_epoch {
        return Err(
            "election term is not above the fencing epoch (manual promotions outran raft \
             terms) — promote manually; the alignment heals as terms advance",
        );
    }
    Ok(())
}

/// One poll: if this node currently leads the control plane and the
/// policy allows it, file a promotion request carrying the election
/// term. Standing refusals are logged once per term, not once per poll.
#[allow(clippy::too_many_arguments)] // poll-loop assembly point; a struct would just restate it
fn consider_auto_promotion(
    status: &RaftStatus,
    control: &ReplicaControlPlane,
    fence_state: &melin_transport_core::fence::FenceState,
    durability_mode: &AtomicU8,
    peer_tips: &PeerTips,
    // Configured voter ids excluding this node.
    peer_ids: &[u64],
    // How long the primary link has been continuously down — tracked by
    // the poll loop so the decision stays a pure function (see
    // [`PRIMARY_DOWN_GRACE`]). `ZERO` while the link is up.
    primary_link_down_for: Duration,
    // How long this node has continuously been leader in the current
    // term — the baseline for the peer-tip veto's `Unknown` grace.
    leader_for: Duration,
    last_refused_term: &mut u64,
) {
    if !status.running.load(Ordering::Relaxed)
        || status.role.load(Ordering::Relaxed) != RaftStatus::ROLE_LEADER
        || control.promote.is_requested()
    {
        return;
    }
    let term = status.term.load(Ordering::Relaxed);
    // Loaded once: both the effective mode and the observed-a-primary
    // fact must come from the same snapshot of the gauge.
    let observed_mode = control.primary_acking_mode.load(Ordering::Acquire);
    // Loaded once for the same reason: `local_tip.epoch` and the
    // `fence_epoch` input must not tear if a fencing event lands
    // mid-poll.
    let fence_epoch = fence_state.epoch();
    let now = Instant::now();
    let inputs = AutoPromotionInputs {
        tip_ready: control.tip_ready.load(Ordering::Acquire),
        fenced: fence_state.is_fenced(),
        durability_mode: effective_acking_mode(
            observed_mode,
            durability_mode.load(Ordering::Relaxed),
        ),
        primary_link_up: control.primary_link_up.load(Ordering::Acquire),
        primary_link_down_for,
        primary_observed: observed_mode != crate::durability_policy::ACKING_MODE_UNKNOWN,
        local_tip: JournalTip {
            epoch: fence_epoch,
            last_sequence: control.journal_tip.load().get(),
        },
        peers: peer_ids
            .iter()
            .map(|&p| (p, observe_peer(peer_tips, p, leader_for, now)))
            .collect(),
        term,
        fence_epoch,
    };
    match auto_promotion_decision(&inputs) {
        Ok(()) => {
            // A silent peer whose *last known* tip was ahead cannot block
            // promotion — its data is unreachable, so nothing better is
            // available — but the operator must know acked events may be
            // recoverable only from that node's disk when it returns.
            for (peer, obs) in &inputs.peers {
                if let PeerTipObservation::Silent { last: Some(tip) } = obs
                    && tip.is_ahead_of(inputs.local_tip)
                {
                    warn!(
                        node_id = status.node_id,
                        peer,
                        peer_tip = ?tip,
                        local_tip = ?inputs.local_tip,
                        "promoting although an unreachable peer last advertised a journal tip \
                         ahead of ours — events acked past our tip exist only on that node; \
                         reconcile from its journal when it returns"
                    );
                }
            }
            // `request` can only lose to a racing manual PROMOTE; either
            // way a promotion is now in flight.
            if control.promote.request(term) {
                info!(
                    node_id = status.node_id,
                    term, "elected leader — auto-promoting this replica"
                );
            }
        }
        Err(reason) => {
            if *last_refused_term != term {
                *last_refused_term = term;
                warn!(
                    node_id = status.node_id,
                    term,
                    reason,
                    peers = ?inputs.peers,
                    local_tip = ?inputs.local_tip,
                    "elected leader but refusing auto-promotion"
                );
            }
        }
    }
}

/// Voter-side of the takeover handshake: should this *follower* nudge
/// an election because the standing control-plane leader is behind its
/// journal tip during a failover window?
///
/// The peer-tip veto above makes a behind leader refuse to promote —
/// which prevents data loss but leaves the cluster primary-less, since
/// the caught-up node holds the data and not the leadership. This is
/// the other half: the caught-up node sees the leader's tip on every
/// inbound append envelope, recognizes the standoff, and campaigns.
/// The vote filter then steers the election its way (its tip is
/// higher), and its own promotion passes the veto.
///
/// Gated on the same failover conditions as promotion itself
/// (tip ready, not fenced, primary continuously down past the grace)
/// so a healthy cluster — where the control-plane leader's tip
/// routinely trails the data-plane primary's — never churns leadership.
fn challenger_should_campaign(
    tip_ready: bool,
    fenced: bool,
    primary_link_up: bool,
    primary_link_down_for: Duration,
    local_tip: JournalTip,
    // The current leader's tip, if heard within [`PEER_TIP_GRACE`].
    leader_tip: Option<JournalTip>,
) -> bool {
    if !tip_ready || fenced || primary_link_up || primary_link_down_for < PRIMARY_DOWN_GRACE {
        return false;
    }
    matches!(leader_tip, Some(tip) if local_tip.is_ahead_of(tip))
}

/// How long the challenger rule must hold *continuously* — same
/// leader, same term — before the nudge fires.
///
/// Nudging on the first sighting of a behind leader loses a race that
/// can livelock the control plane: openraft appends a blank entry on
/// every leadership win, so at the moment the new leader's first
/// heartbeat delivers its (behind) tip, this node's raft *log* is one
/// entry behind — and the election the nudge triggers is one this node
/// cannot win (log-recency refusal). The deposed behind node then
/// re-wins via the vote filter's liveness escape, appends another
/// blank entry, and the cycle repeats: elections churn, nobody leads
/// long enough to promote, and failover stalls — fail-safe, but
/// unbounded. The repair is the leader's own log replication, which
/// lands within a heartbeat round or two; the hold-down simply lets it
/// win the race, so the election we then trigger is one we win.
/// Several heartbeat intervals with margin; costs at most this much
/// extra failover latency.
const CHALLENGER_HOLD_DOWN: Duration = Duration::from_millis(1500);

/// One poll of the challenger rule: if it has held continuously for
/// [`CHALLENGER_HOLD_DOWN`] (see there for why not immediately),
/// request an election via the driver's nudge flag, at most once per
/// observed term (a campaign bumps the term, so a failed takeover
/// re-arms on its own).
fn consider_challenger_nudge(
    status: &RaftStatus,
    control: &ReplicaControlPlane,
    fence_state: &melin_transport_core::fence::FenceState,
    peer_tips: &PeerTips,
    elect_requested: &AtomicBool,
    primary_link_down_for: Duration,
    // Since when the challenger rule has held against the current
    // `(leader, term)` — the hold-down clock, owned by the poll loop
    // like the other grace timers. `None` while the rule does not hold.
    behind_since: &mut Option<(u64, u64, Instant)>,
    last_nudged_term: &mut u64,
) {
    if !status.running.load(Ordering::Relaxed)
        || status.role.load(Ordering::Relaxed) == RaftStatus::ROLE_LEADER
        || control.promote.is_requested()
    {
        *behind_since = None;
        return;
    }
    let leader_id = status.leader_id.load(Ordering::Relaxed);
    if leader_id == 0 || leader_id == status.node_id {
        *behind_since = None;
        return;
    }
    let term = status.term.load(Ordering::Relaxed);
    let local_tip = JournalTip {
        epoch: fence_state.epoch(),
        last_sequence: control.journal_tip.load().get(),
    };
    let leader_tip = peer_tips
        .sample(leader_id, Instant::now())
        .filter(|(_, age)| *age <= PEER_TIP_GRACE)
        .map(|(tip, _)| tip);
    if !challenger_should_campaign(
        control.tip_ready.load(Ordering::Acquire),
        fence_state.is_fenced(),
        control.primary_link_up.load(Ordering::Acquire),
        primary_link_down_for,
        local_tip,
        leader_tip,
    ) {
        *behind_since = None;
        return;
    }
    // The rule holds. Run the hold-down clock against this exact
    // (leader, term); any change restarts it, so the appends of a
    // *new* leadership get their own window to repair our log.
    let since = match *behind_since {
        Some((l, t, s)) if l == leader_id && t == term => s,
        _ => {
            *behind_since = Some((leader_id, term, Instant::now()));
            return;
        }
    };
    if since.elapsed() < CHALLENGER_HOLD_DOWN || *last_nudged_term == term {
        return;
    }
    *last_nudged_term = term;
    elect_requested.store(true, Ordering::Release);
    info!(
        node_id = status.node_id,
        leader_id,
        term,
        ?local_tip,
        ?leader_tip,
        "control-plane leader has been behind this node's journal tip through the hold-down — \
         campaigning so promotion lands on the caught-up node"
    );
}

/// Freshness window for the election stand-down rule, deliberately
/// wider than [`PEER_TIP_GRACE`] because its evidence arrives more
/// slowly. The veto samples peers it is actively heartbeating as
/// leader (a live peer answers within 200–350 ms), so 1.5 s of silence
/// there means death. The stand-down instead watches a peer it is
/// *losing elections to*: during a leaderless standoff that peer's tip
/// arrives only via its own vote requests — at most once per its
/// election timeout, up to ~2.3 s with the driver's tick quantization.
/// A window shorter than that period lets the rule flap: the behind
/// node re-arms mid-period, campaigns first (openraft 0.9 draws each
/// timeout once per boot, so the shorter draw wins that race every
/// round), self-votes, and the refusal cycle the rule exists to break
/// resumes. Two full worst-case periods of margin; the cost is bounded
/// liveness delay — if the ahead peer dies mid-standoff, this node
/// waits out the window before campaigning, comparable to
/// [`PRIMARY_DOWN_GRACE`] and far cheaper than the livelock.
const STAND_DOWN_GRACE: Duration = Duration::from_secs(5);

/// Election stand-down rule (pure): may this replica start
/// timeout-driven control-plane elections right now?
///
/// It may not while a recently-heard peer journal tip is strictly ahead
/// of its own: an election it could win would only seat a leader whose
/// promotion the journal-safety veto must refuse, and under openraft
/// 0.9 such a doomed leadership is worse than useless — election
/// timeouts are drawn once per boot, so the race between the deposed
/// behind winner and the caught-up challenger has a fixed winner, and
/// the depose/re-elect cycle self-sustains (observed livelocking
/// failover past a 60 s deadline). Standing down is the electoral dual
/// of the vote filter in `melin_raft::recency`: the filter refuses to
/// *vote for* a behind candidate, this refuses to *be* one. The behind
/// node learns where it stands from the ahead node's own vote requests
/// (tips are recorded from every envelope before the filter judges
/// it), so it stops competing after losing at most one round.
///
/// Liveness: freshness bounds the rule — if the ahead peer dies, its
/// tip ages past [`STAND_DOWN_GRACE`] and elections re-enable within
/// one poll. Stale or never-heard peers never stand us down (their
/// data is unreachable; someone must lead). A fenced node stands down
/// unconditionally (it must not lead), as does one whose own tip is
/// not yet recovered (it cannot know where it stands — the vote
/// filter's tip-readiness gate refuses inbound votes in that state for
/// the same reason).
///
/// `peer_samples` holds each heard peer's last tip and its age.
fn election_should_stand_down(
    tip_ready: bool,
    fenced: bool,
    local_tip: JournalTip,
    peer_samples: &[(JournalTip, Duration)],
) -> bool {
    if !tip_ready || fenced {
        return true;
    }
    peer_samples
        .iter()
        .any(|(tip, age)| *age <= STAND_DOWN_GRACE && tip.is_ahead_of(local_tip))
}

/// One poll of the stand-down rule: sample the peer-tip table and
/// publish the verdict to the driver's `elect_enabled` flag. The
/// driver applies changes on its own 100 ms bridge loop.
fn consider_election_stand_down(
    control: &ReplicaControlPlane,
    fence_state: &melin_transport_core::fence::FenceState,
    peer_tips: &PeerTips,
    peer_ids: &[u64],
    elect_enabled: &AtomicBool,
) {
    let now = Instant::now();
    let local_tip = JournalTip {
        epoch: fence_state.epoch(),
        last_sequence: control.journal_tip.load().get(),
    };
    let peer_samples: Vec<(JournalTip, Duration)> = peer_ids
        .iter()
        .filter_map(|&p| peer_tips.sample(p, now))
        .collect();
    let stand_down = election_should_stand_down(
        control.tip_ready.load(Ordering::Acquire),
        fence_state.is_fenced(),
        local_tip,
        &peer_samples,
    );
    elect_enabled.store(!stand_down, Ordering::Release);
}

/// Spawn the auto-promotion thread for a replica node. Only called when
/// `--raft-auto-promote` is set and this node booted as a replica (a
/// genesis primary has nothing to promote). Exits on the process
/// shutdown flag or once a promotion has been filed (by anyone — its
/// job is done either way).
#[allow(clippy::too_many_arguments)] // thread assembly point; a struct would just restate it
pub(crate) fn spawn_auto_promotion(
    status: Arc<RaftStatus>,
    control: ReplicaControlPlane,
    fence_state: Arc<melin_transport_core::fence::FenceState>,
    durability_mode: Arc<AtomicU8>,
    peer_tips: Arc<PeerTips>,
    // Configured voter ids excluding this node — the peers the
    // journal-safety veto must account for.
    peer_ids: Vec<u64>,
    elect_requested: Arc<AtomicBool>,
    elect_enabled: Arc<AtomicBool>,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("raft-promotion".into())
        .spawn(move || {
            let mut last_refused_term = 0u64;
            let mut last_nudged_term = 0u64;
            // Hold-down clock for the challenger nudge — see
            // [`CHALLENGER_HOLD_DOWN`].
            let mut behind_since: Option<(u64, u64, Instant)> = None;
            // When the primary link last went (and has since stayed) down
            // — `None` while it is up. Tracked here, independent of raft
            // leadership, so the grace clock reflects the true unreachable
            // duration by the time an election win arrives. See
            // [`PRIMARY_DOWN_GRACE`].
            let mut primary_link_down_since: Option<Instant> = None;
            // When this node became leader of the current term — `None`
            // while a follower. The peer-tip veto's `Unknown` grace runs
            // against this: heartbeats (and thus peer samples) only flow
            // once we lead. A term change restarts the clock.
            let mut leader_since: Option<(u64, Instant)> = None;
            while !shutdown.load(Ordering::Relaxed) {
                if control.primary_link_up.load(Ordering::Acquire) {
                    primary_link_down_since = None;
                } else if primary_link_down_since.is_none() {
                    primary_link_down_since = Some(Instant::now());
                }
                let primary_link_down_for = primary_link_down_since
                    .map(|since| since.elapsed())
                    .unwrap_or(Duration::ZERO);
                let term = status.term.load(Ordering::Relaxed);
                let is_leader = status.role.load(Ordering::Relaxed) == RaftStatus::ROLE_LEADER;
                if !is_leader {
                    leader_since = None;
                } else if leader_since.is_none_or(|(t, _)| t != term) {
                    leader_since = Some((term, Instant::now()));
                }
                let leader_for = leader_since
                    .map(|(_, since)| since.elapsed())
                    .unwrap_or(Duration::ZERO);
                consider_auto_promotion(
                    &status,
                    &control,
                    &fence_state,
                    &durability_mode,
                    &peer_tips,
                    &peer_ids,
                    primary_link_down_for,
                    leader_for,
                    &mut last_refused_term,
                );
                consider_challenger_nudge(
                    &status,
                    &control,
                    &fence_state,
                    &peer_tips,
                    &elect_requested,
                    primary_link_down_for,
                    &mut behind_since,
                    &mut last_nudged_term,
                );
                consider_election_stand_down(
                    &control,
                    &fence_state,
                    &peer_tips,
                    &peer_ids,
                    &elect_enabled,
                );
                if control.promote.is_requested() {
                    // A promotion is in flight (ours or a manual one) —
                    // this node's replica phase is ending.
                    break;
                }
                std::thread::sleep(POLL_INTERVAL);
            }
            // This thread owns the stand-down verdict; with the replica
            // phase over (promotion or shutdown), nobody is left to
            // clear it, and the node — possibly the new primary — must
            // hold normal elections again.
            elect_enabled.store(true, Ordering::Release);
        })
        .expect("failed to spawn raft-promotion thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability_policy::ACKING_MODE_UNKNOWN;

    fn tip(epoch: u64, seq: u64) -> JournalTip {
        JournalTip {
            epoch,
            last_sequence: seq,
        }
    }

    fn ok_inputs() -> AutoPromotionInputs {
        AutoPromotionInputs {
            tip_ready: true,
            fenced: false,
            durability_mode: Some(DurabilityMode::Hybrid),
            primary_link_up: false,
            // Well past the grace: a sustained outage, not a blip.
            primary_link_down_for: PRIMARY_DOWN_GRACE * 10,
            // A normal failover: the primary streamed to this node
            // before dying, and the journal carries its data.
            primary_observed: true,
            local_tip: tip(3, 100),
            // The dead primary is silent, the other replica is live and
            // equal — the common healthy-failover shape.
            peers: vec![
                (1, PeerTipObservation::Silent { last: None }),
                (3, PeerTipObservation::Fresh(tip(3, 100))),
            ],
            term: 5,
            fence_epoch: 3,
        }
    }

    #[test]
    fn healthy_leader_promotes() {
        assert!(auto_promotion_decision(&ok_inputs()).is_ok());
    }

    #[test]
    fn refuses_before_tip_ready() {
        let inputs = AutoPromotionInputs {
            tip_ready: false,
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("tip")
        );
    }

    #[test]
    fn refuses_when_fenced() {
        let inputs = AutoPromotionInputs {
            fenced: true,
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("fenced")
        );
    }

    #[test]
    fn refuses_while_primary_link_is_up() {
        let inputs = AutoPromotionInputs {
            primary_link_up: true,
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("live primary")
        );
    }

    #[test]
    fn refuses_on_a_transient_primary_link_blip() {
        // The link is down, but not for long enough — a blip or a brief
        // partition must not fail over.
        let inputs = AutoPromotionInputs {
            primary_link_up: false,
            primary_link_down_for: PRIMARY_DOWN_GRACE - Duration::from_millis(1),
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("moments ago"),
            "a sub-grace outage must be refused as a possible blip"
        );

        // Once the link has been down through the grace, a real failure
        // is assumed and promotion proceeds.
        let inputs = AutoPromotionInputs {
            primary_link_up: false,
            primary_link_down_for: PRIMARY_DOWN_GRACE,
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs).is_ok(),
            "a sustained outage past the grace must promote"
        );
    }

    #[test]
    fn refuses_a_blank_genesis_node_but_not_a_restarted_replica() {
        // Never observed a primary this boot AND no journal data: a
        // blank node leading an election is a bring-up race, not a
        // failover — it must not depose a slow-starting primary.
        let inputs = AutoPromotionInputs {
            primary_observed: false,
            local_tip: tip(3, 0),
            // A blank node has heard nobody either.
            peers: vec![
                (1, PeerTipObservation::Silent { last: None }),
                (3, PeerTipObservation::Silent { last: None }),
            ],
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("journal is empty")
        );

        // A data-bearing replica that restarted mid-outage never saw
        // the primary this boot either — it must still be promotable,
        // or a full-cluster outage could never auto-fail-over.
        let inputs = AutoPromotionInputs {
            primary_observed: false,
            ..ok_inputs()
        };
        assert!(auto_promotion_decision(&inputs).is_ok());

        // An observed primary that never streamed any data (empty
        // journal, nothing ever acked): nothing can be lost by
        // promoting, and the outage is real.
        let inputs = AutoPromotionInputs {
            primary_observed: true,
            local_tip: tip(3, 0),
            peers: vec![
                (1, PeerTipObservation::Silent { last: None }),
                (3, PeerTipObservation::Fresh(tip(3, 0))),
            ],
            ..ok_inputs()
        };
        assert!(auto_promotion_decision(&inputs).is_ok());
    }

    #[test]
    fn refuses_local_durability_and_unknown_modes() {
        let inputs = AutoPromotionInputs {
            durability_mode: Some(DurabilityMode::Local),
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("local")
        );

        let inputs = AutoPromotionInputs {
            durability_mode: None,
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("unrecognised")
        );
    }

    /// Auto-promotion qualifies on exactly the modes whose acks waited
    /// for a second node — the property the match arm in
    /// [`auto_promotion_decision`] encodes.
    ///
    /// Checked against the policies themselves rather than a hand-kept
    /// list of variants, and driven by `value_variants` (exhaustive by
    /// construction), so a mode added later is covered without anyone
    /// remembering to extend this test. Listing variants by hand is
    /// what would let a new mode drop out of the qualifying arm with
    /// the suite still green — auto-failover silently off under it.
    #[test]
    fn promotion_qualifies_exactly_on_modes_that_acked_on_a_second_node() {
        use clap::ValueEnum as _;

        for &mode in DurabilityMode::value_variants() {
            let waits_for_a_second_node = mode.to_policy().clauses().iter().any(|c| c.count >= 2);
            let inputs = AutoPromotionInputs {
                durability_mode: Some(mode),
                ..ok_inputs()
            };
            let decision = auto_promotion_decision(&inputs);
            assert_eq!(
                decision.is_ok(),
                waits_for_a_second_node,
                "mode `{mode}` requires a second node: {waits_for_a_second_node}, \
                 but the promotion decision was {decision:?}"
            );
        }
    }

    #[test]
    fn refuses_when_epochs_outran_terms() {
        // Equal is refused too: the promotion journals epoch = term, so
        // an equal term would collide with the epoch already in force.
        for (term, fence_epoch) in [(3, 3), (2, 3)] {
            let inputs = AutoPromotionInputs {
                term,
                fence_epoch,
                ..ok_inputs()
            };
            assert!(
                auto_promotion_decision(&inputs)
                    .unwrap_err()
                    .contains("term"),
                "term {term} epoch {fence_epoch}"
            );
        }
        // Strictly above passes.
        let inputs = AutoPromotionInputs {
            term: 4,
            fence_epoch: 3,
            ..ok_inputs()
        };
        assert!(auto_promotion_decision(&inputs).is_ok());
    }

    // --- Peer-tip veto (the promotion-time journal-safety check) ---

    #[test]
    fn refuses_while_a_live_peer_is_ahead() {
        // The failover data-loss scenario: this node won the election
        // (or held leadership) while the other replica holds an acked
        // event it lacks. Its fresh higher tip must veto promotion.
        let inputs = AutoPromotionInputs {
            local_tip: tip(3, 100),
            peers: vec![
                (1, PeerTipObservation::Silent { last: None }),
                (3, PeerTipObservation::Fresh(tip(3, 101))),
            ],
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("ahead of ours")
        );
    }

    #[test]
    fn epoch_dominates_sequence_in_the_veto() {
        // A peer with a *longer* journal on an older epoch holds a
        // divergent, never-acked suffix from a deposed primary — it is
        // NOT ahead and must not veto (same order as the vote filter).
        let inputs = AutoPromotionInputs {
            local_tip: tip(3, 100),
            peers: vec![(3, PeerTipObservation::Fresh(tip(2, 900)))],
            ..ok_inputs()
        };
        assert!(auto_promotion_decision(&inputs).is_ok());

        // A peer on a newer epoch is ahead regardless of sequence.
        let inputs = AutoPromotionInputs {
            local_tip: tip(3, 100),
            peers: vec![(3, PeerTipObservation::Fresh(tip(4, 1)))],
            ..ok_inputs()
        };
        assert!(auto_promotion_decision(&inputs).is_err());
    }

    #[test]
    fn refuses_while_a_peer_tip_is_still_unknown() {
        // Just became leader; heartbeat responses not in yet. The veto
        // must wait rather than promote blind — the unknown peer may be
        // the one holding the newest acked events.
        let inputs = AutoPromotionInputs {
            peers: vec![
                (1, PeerTipObservation::Silent { last: None }),
                (3, PeerTipObservation::Unknown),
            ],
            ..ok_inputs()
        };
        assert!(
            auto_promotion_decision(&inputs)
                .unwrap_err()
                .contains("unknown")
        );
    }

    #[test]
    fn silent_peers_do_not_block_promotion() {
        // Both peers dead — even one whose last-known tip was ahead.
        // Its data is unreachable; nothing better is available, so
        // promotion proceeds (the caller warns about the higher tip).
        let inputs = AutoPromotionInputs {
            local_tip: tip(3, 100),
            peers: vec![
                (1, PeerTipObservation::Silent { last: None }),
                (
                    3,
                    PeerTipObservation::Silent {
                        last: Some(tip(3, 101)),
                    },
                ),
            ],
            ..ok_inputs()
        };
        assert!(auto_promotion_decision(&inputs).is_ok());
    }

    #[test]
    fn observe_peer_digests_freshness_correctly() {
        let tips = PeerTips::new();
        let t0 = Instant::now();

        // Never heard, short leadership: Unknown (the veto waits).
        assert_eq!(
            observe_peer(&tips, 2, Duration::ZERO, t0),
            PeerTipObservation::Unknown
        );
        // Never heard, but we have been heartbeating past the grace:
        // the peer is dead.
        assert_eq!(
            observe_peer(&tips, 2, PEER_TIP_GRACE * 2, t0),
            PeerTipObservation::Silent { last: None }
        );

        tips.record_at(2, tip(3, 7), t0);
        // Within the grace: fresh.
        assert_eq!(
            observe_peer(&tips, 2, Duration::ZERO, t0 + PEER_TIP_GRACE),
            PeerTipObservation::Fresh(tip(3, 7))
        );
        // Past the grace: silent, last tip retained for the loss warning.
        assert_eq!(
            observe_peer(&tips, 2, Duration::ZERO, t0 + PEER_TIP_GRACE * 2),
            PeerTipObservation::Silent {
                last: Some(tip(3, 7))
            }
        );
    }

    // --- Challenger nudge (the takeover half of the veto) ---

    #[test]
    fn challenger_campaigns_only_against_a_fresh_behind_leader() {
        let ahead = tip(3, 101);
        let behind = tip(3, 100);
        // The standoff: failover conditions hold and the leader is
        // behind us — campaign.
        assert!(challenger_should_campaign(
            true,
            false,
            false,
            PRIMARY_DOWN_GRACE,
            ahead,
            Some(behind)
        ));
        // Leader equal or ahead: its promotion is safe — stay put.
        assert!(!challenger_should_campaign(
            true,
            false,
            false,
            PRIMARY_DOWN_GRACE,
            ahead,
            Some(ahead)
        ));
        // No fresh leader tip: nothing to conclude.
        assert!(!challenger_should_campaign(
            true,
            false,
            false,
            PRIMARY_DOWN_GRACE,
            ahead,
            None
        ));
    }

    #[test]
    fn challenger_never_fires_outside_a_failover_window() {
        let ahead = tip(3, 101);
        let behind = tip(3, 100);
        // Healthy cluster: primary link up. The control-plane leader's
        // tip routinely trails the data-plane primary's — deposing it
        // would churn leadership for nothing.
        assert!(!challenger_should_campaign(
            true,
            false,
            true,
            Duration::ZERO,
            ahead,
            Some(behind)
        ));
        // Link down but inside the blip grace.
        assert!(!challenger_should_campaign(
            true,
            false,
            false,
            PRIMARY_DOWN_GRACE - Duration::from_millis(1),
            ahead,
            Some(behind)
        ));
        // Not tip-ready / fenced nodes must never campaign.
        assert!(!challenger_should_campaign(
            false,
            false,
            false,
            PRIMARY_DOWN_GRACE,
            ahead,
            Some(behind)
        ));
        assert!(!challenger_should_campaign(
            true,
            true,
            false,
            PRIMARY_DOWN_GRACE,
            ahead,
            Some(behind)
        ));
    }

    #[test]
    fn stand_down_only_while_a_recent_peer_tip_is_ahead() {
        let local = tip(3, 100);
        // A recently heard peer strictly ahead: stand down.
        assert!(election_should_stand_down(
            true,
            false,
            local,
            &[(tip(3, 101), Duration::ZERO)],
        ));
        // Heard right at the window edge still counts.
        assert!(election_should_stand_down(
            true,
            false,
            local,
            &[(tip(3, 101), STAND_DOWN_GRACE)],
        ));
        // Recent but equal or behind: compete normally — strictness is
        // what lets tied replicas elect each other.
        assert!(!election_should_stand_down(
            true,
            false,
            local,
            &[(tip(3, 100), Duration::ZERO), (tip(3, 99), Duration::ZERO),],
        ));
        // An ahead peer gone quiet past the window must not hold
        // elections down: its data is unreachable, someone must lead.
        assert!(!election_should_stand_down(
            true,
            false,
            local,
            &[(tip(3, 200), STAND_DOWN_GRACE + Duration::from_millis(1))],
        ));
        // No peers heard at all: compete.
        assert!(!election_should_stand_down(true, false, local, &[]));
    }

    #[test]
    fn stand_down_is_unconditional_when_fenced_or_tip_unready() {
        let local = tip(3, 100);
        // Fenced: must not lead, whatever the peers look like.
        assert!(election_should_stand_down(true, true, local, &[]));
        // Tip not recovered: cannot know where we stand.
        assert!(election_should_stand_down(false, false, local, &[]));
    }

    #[test]
    fn effective_acking_mode_prefers_the_observed_primary_mode() {
        // Observed mode wins over the local fallback.
        assert_eq!(
            effective_acking_mode(
                DurabilityMode::Local.as_u8(),
                DurabilityMode::Hybrid.as_u8()
            ),
            Some(DurabilityMode::Local)
        );
        // Unknown observed falls back to the local mode.
        assert_eq!(
            effective_acking_mode(ACKING_MODE_UNKNOWN, DurabilityMode::Hybrid.as_u8()),
            Some(DurabilityMode::Hybrid)
        );
        // Garbage bytes are None (caller refuses).
        assert_eq!(
            effective_acking_mode(0x7F, DurabilityMode::Hybrid.as_u8()),
            None
        );
    }
}
