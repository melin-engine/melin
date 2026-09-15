//! Refusing client writes while a node cannot honour durability.
//!
//! A node halts when replication is configured and no replica is
//! connected: it can no longer promise what the ack policy promises, so it
//! takes no new writes. Each is answered with the application's rejection
//! for [`RejectReason::ReplicaDisconnected`]; queries still run. A node a
//! newer primary has fenced is stopping and answers nothing: a connection
//! that sends anything while it winds down is closed, and the client
//! reconnects to the new primary.
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
//! request refused for the halt does not count as the request sequence
//! it carried, so a client may resend it under the same sequence once the
//! node takes writes again.
//!
//! # Delivered in order
//!
//! Replies carry no request identifier: a client pairs them with its
//! requests by order. A refusal can therefore not go out as soon as it is
//! decided — the connection may still be waiting on replies to requests it
//! sent before, held by the ack policy. The reader stamps the refusal with
//! the input-ring sequence the request would have taken, and the response
//! stage sends it once every event published before that sequence has been
//! answered ([`RefusalQueue::release`]). The reader makes each refusal
//! visible before it commits anything published after it, so the response
//! stage cannot reach a later event without seeing the refusal first.
//!
//! The response stage works from a snapshot of the queue, taken once per
//! iteration between reading the output ring and draining its control
//! channel ([`RefusalQueue::sync`]). Before the drain, because a refusal
//! must never reach the stage ahead of its connection's registration;
//! after the read, because that is when every refusal due before an event
//! in the batch is already visible. One snapshot per iteration is also
//! all the steady state pays: no poll per event.
//!
//! # Counted
//!
//! A refused write leaves no other trace — nothing is journaled — so the
//! reader counts them ([`HaltGate::record_refused`]) and the health
//! endpoint exports the count as `melin_writes_refused_total`. A write
//! shed for a full refusal queue is counted too: the halt is why it was
//! turned away, whatever the client was told.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use melin_app::RejectReason;
use melin_pipeline::padding::Sequence;
use melin_pipeline::spsc;
use melin_pipeline::wait::WaitStrategy;
use melin_transport_core::fence::FenceState;

/// Refusals the response stage can hold before a reader has to shed load.
///
/// Refusals only queue while the response stage is held up behind a reply
/// the ack policy has not released, so this bounds memory during a halt
/// rather than sizing a steady-state flow. Past it a refused write is
/// answered like a full input ring, with `ServerBusy`. A power of two, as
/// the ring requires; each entry is the application's rejection plus two
/// words.
pub(crate) const REFUSAL_QUEUE_CAPACITY: usize = 4096;

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
    /// senders. `None` in standalone mode, which never halts for want of
    /// a replica.
    replicas_connected: Option<Arc<AtomicU32>>,
    /// Latched once a newer primary is observed.
    fence_state: Arc<FenceState>,
    /// Writes refused so far; shared with the health endpoint.
    refused: Arc<AtomicU64>,
}

impl HaltGate {
    /// A gate over the node's replica count (`None` in standalone mode)
    /// and its fence latch — the same two the health endpoint folds into
    /// its status word — that counts what it refuses into `refused`.
    pub fn new(
        replicas_connected: Option<Arc<AtomicU32>>,
        fence_state: Arc<FenceState>,
        refused: Arc<AtomicU64>,
    ) -> Self {
        Self {
            replicas_connected,
            fence_state,
            refused,
        }
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
    /// Two relaxed loads. Relaxed is enough: a halt that starts between the
    /// check and the publish is the case a write published just before the
    /// halt already covers — see the module docs.
    #[inline]
    pub fn verdict(&self) -> Verdict {
        if self.fence_state.is_fenced() {
            Verdict::Close
        } else if self
            .replicas_connected
            .as_ref()
            .is_some_and(|count| count.load(Ordering::Relaxed) == 0)
        {
            Verdict::Refuse(RejectReason::ReplicaDisconnected)
        } else {
            Verdict::Take
        }
    }
}

/// A client write refused at ingress, on its way to the response stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Refusal<R> {
    /// Connection the request arrived on.
    pub connection_id: u64,
    /// Input-ring sequence the request would have taken: every event
    /// published before it has a lower one.
    pub input_seq: u64,
    /// The application's rejection for the request.
    pub report: R,
}

/// Creates the queue a reader hands refusals to the response stage through.
///
/// `matching_progress` is the matching stage's consumer progress on the
/// input ring, which tells an idle response stage that everything
/// published before a refusal has been answered.
pub fn refusal_channel<R: Copy>(
    matching_progress: Arc<Sequence>,
) -> (RefusalSender<R>, RefusalQueue<R>) {
    // `Option` because the ring wants `Default` slots and an application's
    // report need not have one. The wait strategy is unused: the reader
    // never blocks on this queue (see `RefusalSender::try_send`).
    let (tx, rx) =
        spsc::channel::<Option<Refusal<R>>>(REFUSAL_QUEUE_CAPACITY, WaitStrategy::SpinThenYield);
    (
        RefusalSender { tx },
        RefusalQueue {
            rx,
            head: None,
            matching_progress,
        },
    )
}

/// The reader's end of the refusal queue.
pub struct RefusalSender<R> {
    tx: spsc::Producer<Option<Refusal<R>>>,
}

impl<R: Copy> RefusalSender<R> {
    /// Queue a refusal, invisible to the response stage until
    /// [`flush`](Self::flush). Never blocks: a full queue is `Err`, and the
    /// caller sheds the request as it would on a full input ring.
    pub(crate) fn try_send(&mut self, refusal: Refusal<R>) -> Result<(), spsc::Full> {
        self.tx
            .try_push_with(|slot| *slot = Some(refusal))
            .map(|_| ())
    }

    /// Make every queued refusal visible to the response stage. Call before
    /// committing the input-ring entries published after them: the
    /// response stage relies on a refusal being visible by the time it
    /// handles any event that came later.
    pub(crate) fn flush(&mut self) {
        self.tx.flush();
    }
}

/// The response stage's end of the refusal queue.
///
/// Everything here works on the refusals taken in by the last
/// [`sync`](Self::sync); those made visible since wait for the next one.
/// A stage iteration goes:
///
/// 1. [`idle_bound`](Self::idle_bound), then read the output ring;
/// 2. [`sync`](Self::sync), then drain the control channel;
/// 3. [`release`](Self::release) — with the idle bound if the read came
///    back empty, else before the first slot of each event in the batch.
pub struct RefusalQueue<R> {
    rx: spsc::Consumer<Option<Refusal<R>>>,
    /// The oldest synced refusal, taken off the ring but not yet released.
    head: Option<Refusal<R>>,
    matching_progress: Arc<Sequence>,
}

impl<R: Copy> RefusalQueue<R> {
    /// The release bound for an iteration whose output read comes back
    /// empty: the matching stage's progress on the input ring. `None` when
    /// no synced refusal waits, so the steady state leaves the matching
    /// stage's counter alone.
    ///
    /// Must be taken *before* the read. Every event below the progress had
    /// all its output slots published before the progress moved, so a read
    /// that finds the ring empty afterwards has seen all of them.
    #[inline]
    pub fn idle_bound(&mut self) -> Option<u64> {
        self.peek()?;
        Some(self.matching_progress.get().load(Ordering::Acquire))
    }

    /// Take in the refusals the reader has made visible so far, and report
    /// whether any waits. One load of the queue's cursor.
    ///
    /// Call after reading the output ring, so that every refusal due before
    /// an event in that batch is taken in, and before draining the control
    /// channel, so that every refusal taken in belongs to a connection the
    /// drain registers.
    #[inline]
    pub fn sync(&mut self) -> bool {
        self.rx.refresh();
        self.peek().is_some()
    }

    /// Hand `emit` every synced refusal whose request came before `bound`,
    /// oldest first.
    ///
    /// Call with the input sequence of an output slot before handling the
    /// first slot of that event — everything published before it has been
    /// answered — or with [`idle_bound`](Self::idle_bound) when the output
    /// ring is empty. Never between two slots of one event: the refusal
    /// would split that event's reply.
    #[inline]
    pub fn release(&mut self, bound: u64, mut emit: impl FnMut(Refusal<R>)) {
        while let Some(refusal) = self.peek() {
            if refusal.input_seq > bound {
                return;
            }
            self.head = None;
            emit(refusal);
        }
    }

    /// The oldest synced refusal. Never reads the queue's cursor, so it
    /// cannot see past the last [`sync`](Self::sync).
    fn peek(&mut self) -> Option<Refusal<R>> {
        if self.head.is_none() {
            // Only `try_send` writes the ring, and it never writes `None`.
            self.head = self
                .rx
                .try_consume_visible()
                .and_then(|(_, refusal)| refusal);
        }
        self.head
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_pipeline::padding::CachePadded;
    use std::sync::atomic::AtomicBool;

    fn progress(value: u64) -> Arc<Sequence> {
        Arc::new(CachePadded::new(AtomicU64::new(value)))
    }

    fn refusal(connection_id: u64, input_seq: u64) -> Refusal<u64> {
        Refusal {
            connection_id,
            input_seq,
            report: connection_id * 100,
        }
    }

    fn gate(replicas_connected: Option<Arc<AtomicU32>>, fence: Arc<FenceState>) -> HaltGate {
        HaltGate::new(replicas_connected, fence, Arc::new(AtomicU64::new(0)))
    }

    #[test]
    fn standalone_never_refuses() {
        let gate = gate(None, Arc::new(FenceState::new(0)));
        assert_eq!(gate.verdict(), Verdict::Take);
    }

    #[test]
    fn no_replica_refuses_with_replica_disconnected_until_one_connects() {
        let count = Arc::new(AtomicU32::new(0));
        let gate = gate(Some(Arc::clone(&count)), Arc::new(FenceState::new(0)));
        assert_eq!(
            gate.verdict(),
            Verdict::Refuse(RejectReason::ReplicaDisconnected)
        );
        count.store(1, Ordering::Relaxed);
        assert_eq!(gate.verdict(), Verdict::Take);
    }

    #[test]
    fn a_fenced_node_closes_whatever_its_replica_count() {
        let fence = Arc::new(FenceState::new(1));
        let gate = gate(Some(Arc::new(AtomicU32::new(1))), Arc::clone(&fence));
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

    #[test]
    fn a_refusal_waits_for_both_the_flush_and_a_sync() {
        let (mut tx, mut rx) = refusal_channel::<u64>(progress(10));
        tx.try_send(refusal(1, 0)).unwrap();
        assert!(!rx.sync(), "not flushed");

        tx.flush();
        assert_eq!(rx.idle_bound(), None, "flushed, not yet synced");
        let mut released = Vec::new();
        rx.release(u64::MAX, |r| released.push(r.connection_id));
        assert!(released.is_empty(), "release sees only synced refusals");

        assert!(rx.sync());
        assert_eq!(rx.idle_bound(), Some(10));
    }

    /// The snapshot is what keeps a refusal behind its connection's
    /// registration: one made visible after the sync — after the stage has
    /// drained its control channel — waits for the next iteration.
    #[test]
    fn release_does_not_reach_past_the_sync() {
        let (mut tx, mut rx) = refusal_channel::<u64>(progress(0));
        tx.try_send(refusal(1, 0)).unwrap();
        tx.flush();
        assert!(rx.sync());
        tx.try_send(refusal(2, 0)).unwrap();
        tx.flush();

        let mut released = Vec::new();
        rx.release(u64::MAX, |r| released.push(r.connection_id));
        assert_eq!(released, [1]);

        assert!(rx.sync());
        rx.release(u64::MAX, |r| released.push(r.connection_id));
        assert_eq!(released, [1, 2]);
        assert!(!rx.sync(), "queue drained");
    }

    #[test]
    fn release_stops_at_the_first_refusal_past_the_bound() {
        let (mut tx, mut rx) = refusal_channel::<u64>(progress(0));
        for (conn, seq) in [(1, 3), (2, 3), (3, 5), (4, 9)] {
            tx.try_send(refusal(conn, seq)).unwrap();
        }
        tx.flush();
        assert!(rx.sync());

        let mut released = Vec::new();
        rx.release(2, |r| released.push(r.connection_id));
        assert!(released.is_empty(), "no event before seq 3 is answered yet");

        rx.release(5, |r| released.push(r.connection_id));
        assert_eq!(
            released,
            [1, 2, 3],
            "in order, up to and including the bound"
        );

        rx.release(8, |r| released.push(r.connection_id));
        assert_eq!(released, [1, 2, 3], "the held head is not re-emitted");

        rx.release(9, |r| released.push(r.connection_id));
        assert_eq!(released, [1, 2, 3, 4]);
        assert!(!rx.sync(), "queue drained");
        assert_eq!(rx.idle_bound(), None);
    }

    #[test]
    fn a_full_queue_is_an_error_not_a_wait() {
        let (mut tx, _rx) = refusal_channel::<u64>(progress(0));
        for seq in 0..REFUSAL_QUEUE_CAPACITY as u64 {
            tx.try_send(refusal(1, seq)).unwrap();
        }
        assert!(tx.try_send(refusal(1, 0)).is_err());
    }
}
