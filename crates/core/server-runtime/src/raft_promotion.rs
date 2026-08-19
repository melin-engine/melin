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
    /// This node's advertised journal tip is still at sequence 0 — it
    /// holds no journal data at all.
    journal_empty: bool,
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
    if !inputs.primary_observed && inputs.journal_empty {
        return Err(
            "no primary has been observed since boot and the local journal is empty — a blank \
             genesis node must not depose a primary that may still be starting; bring the \
             primary up first, or promote manually",
        );
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
fn consider_auto_promotion(
    status: &RaftStatus,
    control: &ReplicaControlPlane,
    fence_state: &melin_transport_core::fence::FenceState,
    durability_mode: &AtomicU8,
    // How long the primary link has been continuously down — tracked by
    // the poll loop so the decision stays a pure function (see
    // [`PRIMARY_DOWN_GRACE`]). `ZERO` while the link is up.
    primary_link_down_for: Duration,
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
        journal_empty: control.journal_tip.load().get() == 0,
        term,
        fence_epoch: fence_state.epoch(),
    };
    match auto_promotion_decision(&inputs) {
        Ok(()) => {
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
                    term, reason, "elected leader but refusing auto-promotion"
                );
            }
        }
    }
}

/// Spawn the auto-promotion thread for a replica node. Only called when
/// `--raft-auto-promote` is set and this node booted as a replica (a
/// genesis primary has nothing to promote). Exits on the process
/// shutdown flag or once a promotion has been filed (by anyone — its
/// job is done either way).
pub(crate) fn spawn_auto_promotion(
    status: Arc<RaftStatus>,
    control: ReplicaControlPlane,
    fence_state: Arc<melin_transport_core::fence::FenceState>,
    durability_mode: Arc<AtomicU8>,
    shutdown: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("raft-promotion".into())
        .spawn(move || {
            let mut last_refused_term = 0u64;
            // When the primary link last went (and has since stayed) down
            // — `None` while it is up. Tracked here, independent of raft
            // leadership, so the grace clock reflects the true unreachable
            // duration by the time an election win arrives. See
            // [`PRIMARY_DOWN_GRACE`].
            let mut primary_link_down_since: Option<Instant> = None;
            while !shutdown.load(Ordering::Relaxed) {
                if control.primary_link_up.load(Ordering::Acquire) {
                    primary_link_down_since = None;
                } else if primary_link_down_since.is_none() {
                    primary_link_down_since = Some(Instant::now());
                }
                let primary_link_down_for = primary_link_down_since
                    .map(|since| since.elapsed())
                    .unwrap_or(Duration::ZERO);
                consider_auto_promotion(
                    &status,
                    &control,
                    &fence_state,
                    &durability_mode,
                    primary_link_down_for,
                    &mut last_refused_term,
                );
                if control.promote.is_requested() {
                    // A promotion is in flight (ours or a manual one) —
                    // this node's replica phase is ending.
                    return;
                }
                std::thread::sleep(POLL_INTERVAL);
            }
        })
        .expect("failed to spawn raft-promotion thread")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::durability_policy::ACKING_MODE_UNKNOWN;

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
            journal_empty: false,
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
            journal_empty: true,
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
            journal_empty: false,
            ..ok_inputs()
        };
        assert!(auto_promotion_decision(&inputs).is_ok());

        // An observed primary that never streamed any data (empty
        // journal, nothing ever acked): nothing can be lost by
        // promoting, and the outage is real.
        let inputs = AutoPromotionInputs {
            primary_observed: true,
            journal_empty: true,
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
