//! Refusing client writes while a node cannot honour durability.
//!
//! A node halts when replication is configured and no replica is
//! streaming (authenticated and its handshake accepted: catching up or
//! live): it can no longer promise what the ack policy promises, so it
//! takes no new writes. Each is answered with the application's rejection
//! for [`RejectReason::ReplicaDisconnected`]; queries still run. A node a
//! newer primary has fenced is stopping and answers nothing: a connection
//! that sends anything while it winds down is closed, and the client
//! reconnects to the new primary.
//!
//! # Lifted by the operator's consent
//!
//! The halt applies under every ack policy, `disk` included: it is what
//! stops a primary a partition has cut off from its replicas, which
//! fencing reaches only on contact. An operator who knows the node has no
//! replica can consent to serving on its disk alone with `ACK-POLICY
//! disk`: sent while no replica is streaming, the change is journaled as
//! forced, and journaling it lifts the halt
//! (`HaltGate::record_journaled`). The override lasts until a replica
//! streams and acknowledges an entry, or a policy that needs one is
//! journaled; the next loss of the last replica then halts the node again.
//! A replica streamed to that leaves without acking (one refusing the
//! stream, retrying) leaves it standing. A swap to `disk` made while a
//! replica is still streaming lifts nothing when it leaves: consent counts
//! only once the operator knows the node has none. The input ring's
//! producer, which journals the change, is also the reader that enforces
//! the halt, so the first write after the change's entry is taken. The
//! override and the count live together in
//! [`ReplicaPresence`], which the health endpoint reads too.
//!
//! # Refused at ingress
//!
//! The readers decide, before a request is published. A refused write
//! never enters the input ring, so the journal never records it, replicas
//! never see it, and replay agrees with the live engine. Refusing any later
//! cannot have that property: the journal stage reads the input ring in
//! parallel with the matching stage, so a write the matching stage refuses
//! is already on its way to disk, and replay would apply what the live
//! engine turned down.
//!
//! A write published just before the halt was observed is applied like any
//! other, and its reply waits on the ack policy — sent once the policy is
//! met again, never if the node is superseded or stopped first.
//!
//! Only client frames pass the gate. Events the runtime publishes itself
//! — the seed set, a promotion marker — go straight to the input ring and
//! are applied whether or not a replica is attached: they have no client
//! to answer, and a primary that seeds before its first replica connects
//! (the DPDK start-up order) must not lose its seeds to the halt. A
//! request refused for the halt never reached the application, so a
//! client may resend it as it was once the node takes writes again.
//!
//! A halt refusal is an early reply: an answer the reader decides on
//! without publishing the request, sent between other requests' replies
//! and counted on `/metrics` ([`crate::early_reply`]). The refusals are
//! counted through [`HaltGate::record_refused`] as
//! `melin_writes_refused_total`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use melin_app::RejectReason;
use melin_journal::AckPolicyChange;
use melin_transport_core::fence::FenceState;
use melin_transport_core::replica_presence::{Observed, ReplicaPresence};

use crate::ack_policy::AckPolicy;
use crate::policy_control::{Origin, is_unwitnessed};

/// What the response stage needs to answer a halted node's held replies
/// once its own journal holds them: the replies to writes it sequenced
/// before the halt, and queries, which no replica can confirm while none
/// is connected. Each goes out terminated by `BatchEndDegraded`, which
/// tells the client only the primary's disk backs it.
///
/// `None` where a stage takes one is the operator switch off
/// (`--no-degraded-acks`): every held reply, and every reply queued behind
/// one, waits for a replica or a policy swap (a refusal does not: it is an
/// early reply, see [`crate::early_reply`]). A standalone
/// node never halts and takes `None` too.
pub struct DegradedRelease {
    /// The node's replica presence: whether a replica is streaming, and
    /// since when none has been.
    pub presence: Arc<ReplicaPresence>,
    /// How long the halt must last before the release starts
    /// (`--degraded-ack-grace-ms`).
    pub grace: Duration,
}

/// What a reader does with a client write right now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Publish it.
    Take,
    /// Refuse it with this reason; the connection stays open.
    Refuse(RejectReason),
    /// Close the connection: the node is superseded and stopping.
    Close,
}

/// A reader's view of whether this node takes client writes.
pub struct HaltGate {
    /// Replicas currently streaming, maintained by the replication
    /// senders, and the operator's halt override. `None` in standalone
    /// mode, which never halts for want of a replica.
    replicas_connected: Option<Arc<ReplicaPresence>>,
    /// Latched once a newer primary is observed.
    fence_state: Arc<FenceState>,
    /// Writes refused so far; shared with the health endpoint.
    refused: Arc<AtomicU64>,
}

/// Whether a node replicates: the one shape of "standalone or replicated"
/// every ack-policy decision reads. Its source is
/// `ServerConfig::replication` (whether `--replication-bind` is set): the
/// ack-policy mailbox is built from it directly, and a primary pipeline
/// gives its halt gate a replica presence exactly when it is `Enabled`, so
/// the gate's start and the producer's entries, which read it back through
/// [`HaltGate::replication`], cannot disagree with the mailbox's refusal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Replication {
    /// Replication is configured: replicas may stream, and witness changes.
    Enabled,
    /// Standalone: nothing can witness a change.
    Disabled,
}

impl Replication {
    /// Whether `policy` can ever be met on a node of this shape. A policy
    /// that needs a replica cannot on a node that accepts none: no replica
    /// will ever attach to supply the second copy, so its gate would hold
    /// every reply. The admin `ACK-POLICY` command refuses such a request
    /// (`AckPolicyControl::request`). An exhaustive match, so a new shape
    /// has to decide.
    pub(crate) fn can_meet(self, policy: AckPolicy) -> bool {
        match self {
            Replication::Enabled => true,
            Replication::Disabled => !policy.needs_replica(),
        }
    }
}

/// An ack-policy change decided on one look at who was streaming, taken
/// just before the input ring's producer journals it: the policy, whether
/// its entry is unwitnessed, and what was seen. Built only by
/// [`HaltGate::decide`] and consumed only by [`HaltGate::record_journaled`],
/// both private to [`HaltGate::journal`], so the entry the producer
/// publishes and the policy and replica count the halt acts on cannot
/// disagree, and a published entry is recorded exactly once. Neither
/// `Copy` nor `Clone`, for the same reason (and the observation it holds
/// is neither).
#[derive(Debug)]
struct Decision {
    policy: AckPolicy,
    /// Whether the entry is unwitnessed ([`is_unwitnessed`]).
    unwitnessed: bool,
    /// Who was streaming; `None` in standalone mode.
    seen: Option<Observed>,
}

impl Decision {
    /// The entry that journals the change: the policy's byte, derived
    /// here rather than stored beside the policy, so the two cannot
    /// disagree.
    fn change(&self) -> AckPolicyChange {
        AckPolicyChange {
            policy: self.policy.to_byte(),
            unwitnessed: self.unwitnessed,
        }
    }

    /// Whether the change is a forced swap: unwitnessed for want of a
    /// connected replica, rather than because replication is disabled.
    /// Only an explicit runtime `disk` is (see [`is_unwitnessed`]). It
    /// warns, and lifts the halt ([`HaltGate::record_journaled`]).
    fn is_forced(&self) -> bool {
        self.unwitnessed && matches!(self.seen, Some(Observed::NoneConnected(_)))
    }
}

/// An ack-policy change the input ring's producer has journaled, and the
/// halt has recorded ([`HaltGate::journal`]): what the caller logs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Journaled {
    policy: AckPolicy,
    unwitnessed: bool,
    forced: bool,
    /// The input-ring sequence the entry was published at: what the
    /// producer's publish returned, so a change is recorded only with
    /// the index of the entry that journals it.
    input_seq: u64,
}

impl Journaled {
    /// The policy changed to.
    pub(crate) fn policy(self) -> AckPolicy {
        self.policy
    }

    /// Whether the entry was journaled unwitnessed.
    pub(crate) fn unwitnessed(self) -> bool {
        self.unwitnessed
    }

    /// Whether it was a forced swap, which lifted the halt (see
    /// [`HaltGate::record_journaled`]).
    pub(crate) fn is_forced(self) -> bool {
        self.forced
    }

    /// The input-ring sequence the entry was published at.
    pub(crate) fn input_seq(self) -> u64 {
        self.input_seq
    }
}

impl HaltGate {
    /// A gate over the node's replica presence (`None` in standalone mode)
    /// and its fence latch — the same two the health endpoint folds into
    /// its status word — that counts what it refuses into `refused`.
    pub fn new(
        replicas_connected: Option<Arc<ReplicaPresence>>,
        fence_state: Arc<FenceState>,
        refused: Arc<AtomicU64>,
    ) -> Self {
        Self {
            replicas_connected,
            fence_state,
            refused,
        }
    }

    /// Whether this node replicates: whether the gate watches a replica
    /// presence at all.
    pub(crate) fn replication(&self) -> Replication {
        match self.replicas_connected {
            Some(_) => Replication::Enabled,
            None => Replication::Disabled,
        }
    }

    /// Journal a change to `policy` from `origin`: decide its entry on who
    /// is streaming now, hand the entry to `publish` (the input ring's
    /// producer putting it on the ring, returning the sequence it was
    /// published at), and, once it is published, record it against the
    /// halt ([`Self::record_journaled`]). An entry `publish` refuses is
    /// recorded nowhere, and its error returned.
    ///
    /// The one way to journal a change, so a published entry is always
    /// recorded, once, from the very look at who was streaming that
    /// decided it. `publish` returns the ring's publish index, which the
    /// record carries: recording a change without publishing its entry
    /// takes a closure that makes an index up, not one that merely forgot
    /// to push.
    pub(crate) fn journal<E>(
        &self,
        policy: AckPolicy,
        origin: Origin,
        publish: impl FnOnce(AckPolicyChange) -> Result<u64, E>,
    ) -> Result<Journaled, E> {
        let decision = self.decide(policy, origin);
        let input_seq = publish(decision.change())?;
        Ok(self.record_journaled(decision, input_seq))
    }

    /// Decide the entry that journals a change to `policy` from `origin`,
    /// on who is streaming now (two loads) — see [`is_unwitnessed`].
    fn decide(&self, policy: AckPolicy, origin: Origin) -> Decision {
        let seen = self.replicas_connected.as_ref().map(|p| p.observe());
        Decision {
            policy,
            unwitnessed: is_unwitnessed(policy, origin, seen.as_ref()),
            seen,
        }
    }

    /// The input ring's producer journaled `decision`'s change, at
    /// `input_seq`: a forced change (the operator's explicit `disk` with
    /// no replica streaming) lifts the halt; a change to a policy that
    /// needs a replica drops any such override, so the node halts again
    /// while none is streaming. Any other change leaves the override as it
    /// is: a witnessed `disk` was decided with a replica streaming, which
    /// ends any override with its first ack (this change's entry, if no
    /// earlier one); if it leaves without acking anything, the operator's
    /// earlier consent stands. Standalone never halts, so there is nothing
    /// to record. Either change to the override consumes the decision's
    /// observation, the evidence the presence asks for.
    fn record_journaled(&self, decision: Decision, input_seq: u64) -> Journaled {
        let forced = decision.is_forced();
        let Decision {
            policy,
            unwitnessed,
            seen,
        } = decision;
        let journaled = Journaled {
            policy,
            unwitnessed,
            forced,
            input_seq,
        };
        let (Some(presence), Some(seen)) = (&self.replicas_connected, seen) else {
            return journaled;
        };
        if policy.needs_replica() {
            presence.clear_override(seen);
        } else if let Observed::NoneConnected(since) = seen
            && forced
        {
            presence.latch_override(since);
        }
        journaled
    }

    /// Count `count` refused writes. Meant for one call per receive with
    /// the receive's tally, not one per write: a single relaxed add is the
    /// whole cost.
    #[inline]
    pub fn record_refused(&self, count: u64) {
        self.refused.fetch_add(count, Ordering::Relaxed);
    }

    /// The verdict on a client write arriving now. Fencing wins: a
    /// superseded node is shutting down whatever its replica count.
    ///
    /// Two relaxed loads while a replica is connected, the override's two
    /// more while none is. Relaxed is enough: a halt that starts between
    /// the check and the publish is the case a write published just before
    /// the halt already covers — see the module docs.
    #[inline]
    pub fn verdict(&self) -> Verdict {
        if self.fence_state.is_fenced() {
            Verdict::Close
        } else if self
            .replicas_connected
            .as_ref()
            .is_some_and(|presence| presence.halted())
        {
            Verdict::Refuse(RejectReason::ReplicaDisconnected)
        } else {
            Verdict::Take
        }
    }
}

/// Halt gates for the crate's tests.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// A replica presence with `connected` replicas streaming, none of
    /// which has acked yet. (The health endpoint's tests in
    /// `melin-transport-core` keep their own: a test helper cannot cross
    /// the crate boundary.)
    pub(crate) fn presence(connected: u32) -> Arc<ReplicaPresence> {
        let presence = Arc::new(ReplicaPresence::new());
        for _ in 0..connected {
            drop(presence.lift());
        }
        presence
    }

    /// An unfenced gate over `replicas` (`None`: replication disabled)
    /// that counts its refusals nowhere anyone reads.
    pub(crate) fn gate_over(replicas: Option<Arc<ReplicaPresence>>) -> HaltGate {
        HaltGate::new(
            replicas,
            Arc::new(FenceState::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_transport_core::replica_presence::Lowered;
    use std::sync::atomic::AtomicBool;

    fn gate(replicas_connected: Option<Arc<ReplicaPresence>>, fence: Arc<FenceState>) -> HaltGate {
        HaltGate::new(replicas_connected, fence, Arc::new(AtomicU64::new(0)))
    }

    const HALTED: Verdict = Verdict::Refuse(RejectReason::ReplicaDisconnected);

    /// Standalone never refuses, and journaling a change records nothing.
    #[test]
    fn standalone_never_refuses() {
        let gate = gate(None, Arc::new(FenceState::new(0)));
        assert_eq!(gate.verdict(), Verdict::Take);
        for policy in [AckPolicy::Disk, AckPolicy::TwoDisks] {
            journal(&gate, policy, Origin::Runtime);
            assert_eq!(gate.verdict(), Verdict::Take);
        }
    }

    #[test]
    fn no_replica_refuses_with_replica_disconnected_until_one_connects() {
        let replicas = test_support::presence(0);
        let gate = gate(Some(Arc::clone(&replicas)), Arc::new(FenceState::new(0)));
        assert_eq!(gate.verdict(), HALTED);
        drop(replicas.lift());
        assert_eq!(gate.verdict(), Verdict::Take);
    }

    /// A decision follows the replica count at the time it is made: a
    /// runtime `disk` is unwitnessed standalone (not forced), forced with
    /// no replica connected, and witnessed once one connects.
    #[test]
    fn a_decision_follows_the_replica_count() {
        let standalone =
            gate(None, Arc::new(FenceState::new(0))).decide(AckPolicy::Disk, Origin::Runtime);
        assert!(standalone.unwitnessed && !standalone.is_forced());
        let replicas = test_support::presence(0);
        let gate = gate(Some(Arc::clone(&replicas)), Arc::new(FenceState::new(0)));
        let none = gate.decide(AckPolicy::Disk, Origin::Runtime);
        assert!(none.unwitnessed && none.is_forced());
        drop(replicas.lift());
        drop(replicas.lift());
        let connected = gate.decide(AckPolicy::Disk, Origin::Runtime);
        assert!(!connected.unwitnessed && !connected.is_forced());
    }

    /// Journal `policy` as the producer would, onto a ring that always
    /// takes the entry (at a sequence nothing here reads).
    fn journal(gate: &HaltGate, policy: AckPolicy, origin: Origin) -> Journaled {
        let Ok(journaled) = gate.journal(policy, origin, |_| Ok::<_, std::convert::Infallible>(0));
        journaled
    }

    /// The entry a journaled change carries is the policy's byte, and it
    /// is the one handed to the producer, recorded at the sequence the
    /// producer published it at; standalone, it is unwitnessed (nothing
    /// can witness it) but not forced, whatever the policy and origin. The
    /// rule with replication on is `policy_control`'s `the_forced_rule`,
    /// and the count it reads is `a_decision_follows_the_replica_count`.
    #[test]
    fn a_journaled_change_carries_its_policy_as_the_observation_rules() {
        let standalone = gate(None, Arc::new(FenceState::new(0)));
        let mut published = None;
        let Ok(journaled) = standalone.journal(AckPolicy::Ram, Origin::Staged, |change| {
            published = Some(change);
            Ok::<_, std::convert::Infallible>(42)
        });
        assert_eq!(journaled.policy(), AckPolicy::Ram);
        assert_eq!(
            published,
            Some(AckPolicyChange {
                policy: AckPolicy::Ram.to_byte(),
                unwitnessed: journaled.unwitnessed(),
            }),
            "the entry published"
        );
        assert_eq!(journaled.input_seq(), 42, "where it was published");
        assert!(journaled.unwitnessed(), "nothing can witness it");
        assert!(
            !journaled.is_forced(),
            "unwitnessed for want of replication"
        );
    }

    /// An entry the producer could not publish (a full ring) is recorded
    /// nowhere: a forced `disk` refused that way lifts no halt, and the
    /// error comes back.
    #[test]
    fn an_entry_not_published_is_not_recorded() {
        let gate = gate(
            Some(test_support::presence(0)),
            Arc::new(FenceState::new(0)),
        );
        assert_eq!(
            gate.journal(AckPolicy::Disk, Origin::Runtime, |_| Err("full")),
            Err("full")
        );
        assert_eq!(gate.verdict(), HALTED, "still halted");
        assert!(journal(&gate, AckPolicy::Disk, Origin::Runtime).is_forced());
        assert_eq!(gate.verdict(), Verdict::Take, "published: lifted");
    }

    /// The replication shape is the presence's: watched or not.
    #[test]
    fn the_replication_shape_is_the_presence() {
        let fence = || Arc::new(FenceState::new(0));
        assert_eq!(gate(None, fence()).replication(), Replication::Disabled);
        assert_eq!(
            gate(Some(test_support::presence(0)), fence()).replication(),
            Replication::Enabled
        );
    }

    /// A replicating node can meet every policy; one that accepts no
    /// replica only those that need none.
    #[test]
    fn a_node_without_replication_meets_only_policies_that_need_no_replica() {
        for policy in [
            AckPolicy::Disk,
            AckPolicy::Ram,
            AckPolicy::DiskAndRam,
            AckPolicy::TwoDisks,
        ] {
            assert!(Replication::Enabled.can_meet(policy), "{policy}");
            assert_eq!(
                Replication::Disabled.can_meet(policy),
                policy == AckPolicy::Disk,
                "{policy}"
            );
        }
    }

    /// Halt-plan step 0. A node under `disk` that loses its replica halts
    /// like any other; the operator's `disk` during the halt lifts it; a
    /// returning replica that acks voids the override, so its next
    /// departure halts the node again.
    #[test]
    fn a_forced_disk_lifts_the_halt_until_a_replica_returns() {
        let replicas = test_support::presence(1);
        let gate = gate(Some(Arc::clone(&replicas)), Arc::new(FenceState::new(0)));
        assert_eq!(gate.verdict(), Verdict::Take);
        assert_eq!(replicas.lower(), Lowered::LastHalted);
        assert_eq!(gate.verdict(), HALTED, "the halt ignores the policy");

        journal(&gate, AckPolicy::Disk, Origin::Runtime);
        assert_eq!(gate.verdict(), Verdict::Take, "consent lifts the halt");
        assert!(
            gate.decide(AckPolicy::Disk, Origin::Runtime).is_forced(),
            "still no witness: a later `disk` is forced too"
        );
        journal(&gate, AckPolicy::Disk, Origin::Runtime);
        assert_eq!(gate.verdict(), Verdict::Take, "re-sent, it stays lifted");

        let mut first_ack = replicas.lift();
        assert_eq!(gate.verdict(), Verdict::Take);
        first_ack.acked(&replicas);
        assert_eq!(replicas.lower(), Lowered::LastHalted);
        assert_eq!(
            gate.verdict(),
            HALTED,
            "the override did not outlive the replica"
        );
    }

    /// A replica that authenticates and is then refused (a lineage it will
    /// not follow, a primary it takes for stale) never acks: counted once
    /// its handshake is accepted, then leaving, however often it retries,
    /// it leaves the operator's override standing, and a decision while it
    /// is counted is witnessed. One that acks ends it.
    #[test]
    fn a_replica_refused_after_authenticating_leaves_the_override() {
        let replicas = test_support::presence(0);
        let gate = gate(Some(Arc::clone(&replicas)), Arc::new(FenceState::new(0)));
        journal(&gate, AckPolicy::Disk, Origin::Runtime);
        assert_eq!(gate.verdict(), Verdict::Take);
        for _ in 0..3 {
            let refused = replicas.lift();
            assert!(
                !gate.decide(AckPolicy::Disk, Origin::Runtime).is_forced(),
                "streaming, so witnessed"
            );
            drop(refused);
            assert_eq!(replicas.lower(), Lowered::LastOverridden);
            assert_eq!(gate.verdict(), Verdict::Take, "the override held");
        }

        replicas.lift().acked(&replicas);
        assert_eq!(replicas.lower(), Lowered::LastHalted);
        assert_eq!(gate.verdict(), HALTED, "a replica that acked ended it");
    }

    /// A swap back to a policy that needs a replica, with none connected,
    /// restores the halt; each of them does.
    #[test]
    fn a_swap_to_a_replica_backed_policy_restores_the_halt() {
        for policy in [AckPolicy::Ram, AckPolicy::DiskAndRam, AckPolicy::TwoDisks] {
            let gate = gate(
                Some(test_support::presence(0)),
                Arc::new(FenceState::new(0)),
            );
            journal(&gate, AckPolicy::Disk, Origin::Runtime);
            assert_eq!(gate.verdict(), Verdict::Take);
            journal(&gate, policy, Origin::Runtime);
            assert_eq!(gate.verdict(), HALTED, "{policy}");
            journal(&gate, AckPolicy::Disk, Origin::Runtime);
            assert_eq!(
                gate.verdict(),
                Verdict::Take,
                "a forced swap again: {policy}"
            );
        }
    }

    /// Consent counts only once the operator knows the node has no
    /// replica: a `disk` journaled while one is connected lifts nothing
    /// when it leaves, and neither does a staged one (a boot or promotion
    /// entry) with none connected.
    #[test]
    fn a_disk_not_forced_does_not_lift_the_halt() {
        let replicas = test_support::presence(1);
        let gate = gate(Some(Arc::clone(&replicas)), Arc::new(FenceState::new(0)));
        journal(&gate, AckPolicy::Disk, Origin::Runtime);
        assert_eq!(replicas.lower(), Lowered::LastHalted);
        assert_eq!(gate.verdict(), HALTED, "witnessed: no consent");

        journal(&gate, AckPolicy::Disk, Origin::Staged);
        assert_eq!(gate.verdict(), HALTED, "staged: never forced");
    }

    /// The fence wins over the override: a superseded node closes.
    #[test]
    fn a_fenced_node_closes_despite_the_override() {
        let fence = Arc::new(FenceState::new(0));
        let gate = gate(Some(test_support::presence(0)), Arc::clone(&fence));
        journal(&gate, AckPolicy::Disk, Origin::Runtime);
        fence.fence();
        assert_eq!(gate.verdict(), Verdict::Close);
    }

    #[test]
    fn a_fenced_node_closes_whatever_its_replica_count() {
        let fence = Arc::new(FenceState::new(1));
        let gate = gate(Some(test_support::presence(1)), Arc::clone(&fence));
        assert_eq!(gate.verdict(), Verdict::Take);
        assert_eq!(
            fence.fence_if_superseded(2, &AtomicBool::new(false)),
            Some(true)
        );
        assert_eq!(gate.verdict(), Verdict::Close);
    }

    #[test]
    fn refusals_are_counted() {
        let refused = Arc::new(AtomicU64::new(0));
        let gate = HaltGate::new(None, Arc::new(FenceState::new(0)), Arc::clone(&refused));
        gate.record_refused(3);
        gate.record_refused(2);
        assert_eq!(refused.load(Ordering::Relaxed), 5);
    }
}
