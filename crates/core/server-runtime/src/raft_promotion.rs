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
use std::time::Duration;

use melin_transport_core::health::RaftStatus;
use tracing::{info, warn};

use crate::durability_policy::DurabilityMode;
use crate::replication::ReplicaControlPlane;

/// Poll cadence for the promotion thread. Matches the codebase's
/// listener-loop convention; promotion latency is bounded by this plus
/// the driver's own 100 ms metrics bridge.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    match inputs.durability_mode {
        Some(DurabilityMode::Local) => {
            return Err(
                "the primary acks under `local` durability — an election win cannot prove \
                 this node holds every acked order; promote manually if the lag is acceptable",
            );
        }
        None => return Err("acking durability mode is unrecognised"),
        Some(DurabilityMode::Hybrid | DurabilityMode::DurablyReplicated) => {}
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
    last_refused_term: &mut u64,
) {
    if !status.running.load(Ordering::Relaxed)
        || status.role.load(Ordering::Relaxed) != RaftStatus::ROLE_LEADER
        || control.promote.is_requested()
    {
        return;
    }
    let term = status.term.load(Ordering::Relaxed);
    let inputs = AutoPromotionInputs {
        tip_ready: control.tip_ready.load(Ordering::Acquire),
        fenced: fence_state.is_fenced(),
        durability_mode: effective_acking_mode(
            control.primary_acking_mode.load(Ordering::Acquire),
            durability_mode.load(Ordering::Relaxed),
        ),
        primary_link_up: control.primary_link_up.load(Ordering::Acquire),
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
            while !shutdown.load(Ordering::Relaxed) {
                consider_auto_promotion(
                    &status,
                    &control,
                    &fence_state,
                    &durability_mode,
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
