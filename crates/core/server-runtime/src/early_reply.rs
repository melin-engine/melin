//! The answers a reader decides on without publishing the request, on
//! their way to the response stage.
//!
//! # Early replies
//!
//! Four answers need nothing from the pipeline: a write refused while the
//! node is halted (the application's rejection, see [`crate::halt`]), a
//! request the application's decoder filters (an empty batch) or refuses
//! (a `Refused` frame naming why), and a request shed because the input
//! ring is full (`ServerBusy`). All of them reach the response stage
//! through one queue, from the reader (or the DPDK poll thread) that
//! decided them, and every one names its request's ID.
//!
//! # Released between replies, in no order
//!
//! Every final frame names its request, so an early reply does not wait
//! for the replies to requests sent before it: a refusal goes out while
//! a write ahead of it is still held by the ack policy. The one rule left
//! is that it never splits another request's reply, so the response stage
//! sends early replies only between two events' replies, which a
//! `BatchBoundary` tracker vouches for with an `AtBoundary` token (both
//! crate-internal, in `batch_boundary.rs`).
//!
//! The response stage works from a snapshot of the queue: it takes one,
//! then drains its control channel, and only then sends what the snapshot
//! holds (`EarlyReplyQueue::register`). In that order because an early
//! reply must never reach the stage ahead of its connection's
//! registration: the reader answers a connection only once its
//! `Connected` is queued, so a snapshot taken before the drain holds no
//! reply for a connection the drain does not register. The stage takes a
//! snapshot once per iteration, after reading the output ring, and again
//! on each turn of a wait on the durability gate that holds an event's
//! first slot. One per iteration is all the steady state pays: no poll per
//! event.
//!
//! # Counted
//!
//! An early reply's request leaves no other trace — nothing is journaled
//! — so the reader counts each kind once per receive, once its answer is
//! queued (a request parked on is counted when it is finally answered),
//! and the health endpoint exports the counts: halt refusals through
//! [`HaltGate::record_refused`](crate::halt::HaltGate::record_refused) as
//! `melin_writes_refused_total`; shed, filtered and refused requests
//! through [`IngressMetrics`](melin_transport_core::health::IngressMetrics)
//! as `melin_requests_shed_total`, `melin_requests_filtered_total` and
//! `melin_requests_refused_total` (by kind), beside the connections
//! parked on a full queue.

use melin_pipeline::spsc;
use melin_pipeline::wait::WaitStrategy;
use melin_transport_core::pipeline::RequestId;
use melin_wire_protocol::control::RefusalKind;

use crate::batch_boundary::AtBoundary;

/// Early replies in flight from a reader to the response stage before the
/// reader can queue no more: the capacity a node gives
/// [`early_reply_channel`].
///
/// The response stage drains the queue between every two events' replies,
/// and while the ack policy holds an event's first slot, so it fills only
/// if the stage stops draining (an event whose slots it is waiting on, or
/// a stage that is not running), not because a write is held. Past it the
/// reader parks the connection whose answer has nowhere to go, and stops
/// reading it until the queue has room. A power of two, as the ring
/// requires; each entry is the application's rejection, or a refusal's
/// reason, plus a few words.
pub const EARLY_REPLY_QUEUE_CAPACITY: usize = 4096;

/// An answer a reader decided on without publishing the request, on its
/// way to the response stage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EarlyReply<R> {
    /// Connection the request arrived on.
    pub connection_id: u64,
    /// The client's ID for the request, echoed on the reply's final frame.
    pub request_id: RequestId,
    /// What the answer is.
    pub kind: EarlyKind<R>,
}

/// What an [`EarlyReply`] answers, and so the frames it is sent as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EarlyKind<R> {
    /// A write refused while the node is halted: the application's
    /// rejection, then `BatchEnd`.
    Rejected(R),
    /// A request the application's decoder filters: an empty batch, a
    /// `BatchEnd` alone.
    Filtered,
    /// A request the application's decoder refuses: a `Refused` frame with
    /// the decoder's reason.
    Refused {
        kind: RefusalKind,
        reason: &'static str,
    },
    /// A request shed because the input ring is full: `ServerBusy`.
    Busy,
}

/// Creates the queue a reader hands early replies to the response stage
/// through, holding at most `capacity` replies in flight: a node passes
/// [`EARLY_REPLY_QUEUE_CAPACITY`]; a test that wants the queue to fill
/// passes less.
///
/// # Panics
///
/// If `capacity` is not a power of two of at least 2, as the ring
/// requires.
pub fn early_reply_channel<R: Copy>(capacity: usize) -> (EarlyReplySender<R>, EarlyReplyQueue<R>) {
    // `Option` because the ring wants `Default` slots and an application's
    // report need not have one. The wait strategy is unused: the reader
    // never blocks on this queue (see `EarlyReplySender::try_send`).
    let (tx, rx) = spsc::channel::<Option<EarlyReply<R>>>(capacity, WaitStrategy::SpinThenYield);
    (EarlyReplySender { tx }, EarlyReplyQueue { rx })
}

/// The reader's end of the early-reply queue.
pub struct EarlyReplySender<R> {
    tx: spsc::Producer<Option<EarlyReply<R>>>,
}

impl<R: Copy> EarlyReplySender<R> {
    /// Queue an early reply, invisible to the response stage until
    /// [`flush`](Self::flush). Never blocks: a full queue is `Err`, and the
    /// caller parks the connection whose answer has nowhere to go.
    pub(crate) fn try_send(&mut self, reply: EarlyReply<R>) -> Result<(), spsc::Full> {
        self.tx
            .try_push_with(|slot| *slot = Some(reply))
            .map(|_| ())
    }

    /// Make every queued early reply visible to the response stage. The
    /// reader calls it once on every way out of processing a receive
    /// (`process_client_frames`), so no reply it decided is left unseen;
    /// where that falls relative to the input ring's commit does not
    /// matter, since an early reply waits for nothing published.
    pub(crate) fn flush(&mut self) {
        self.tx.flush();
    }
}

/// The response stage's end of the early-reply queue.
///
/// Public only so it can be handed to a response stage: it has no method
/// outside the crate, so nothing else can read it. Read only through the
/// crate's `register`, which takes a snapshot of
/// what the reader has made visible and then drains the stage's control
/// channel, and hands back the token that sends the snapshot's replies.
/// Those made visible since wait for the next snapshot.
pub struct EarlyReplyQueue<R> {
    rx: spsc::Consumer<Option<EarlyReply<R>>>,
}

impl<R: Copy> EarlyReplyQueue<R> {
    /// Take a snapshot of the early replies the reader has made visible,
    /// then run `drain_control`, the stage's drain of its control channel,
    /// and return the token that sends the snapshot's replies. One load of
    /// the queue's cursor.
    ///
    /// The one place the two happen, in that order, so a reply sent
    /// through the token always finds its connection registered: the
    /// reader answers a connection only once its `Connected` is queued,
    /// so every reply in a snapshot taken before the drain belongs to a
    /// connection the drain registers.
    #[inline]
    pub(crate) fn register(&mut self, drain_control: impl FnOnce()) -> Registered<'_, R> {
        let mut registered = Registered { queue: self };
        registered.reregister(drain_control);
        registered
    }
}

/// A snapshot of the early-reply queue taken before the response stage's
/// control channel was drained (see [`EarlyReplyQueue::register`]), and so
/// the right to send what it holds. Borrows the queue: nothing reads it
/// past the token's back.
pub(crate) struct Registered<'q, R> {
    queue: &'q mut EarlyReplyQueue<R>,
}

impl<R: Copy> Registered<'_, R> {
    /// Take a fresh snapshot, then run `drain_control`, as
    /// [`EarlyReplyQueue::register`] does: for a stage that keeps the token
    /// across a wait (on the durability gate) and wants what the reader
    /// decided meanwhile.
    #[inline]
    pub(crate) fn reregister(&mut self, drain_control: impl FnOnce()) {
        self.queue.rx.refresh();
        drain_control();
    }

    /// Hand `emit` every early reply in the snapshot, oldest first, and
    /// return how many.
    ///
    /// `AtBoundary` is the stage's word that it is between two events'
    /// replies: an early reply sent anywhere else would split one.
    #[inline]
    pub(crate) fn drain(
        &mut self,
        _between_events: &AtBoundary<'_>,
        mut emit: impl FnMut(EarlyReply<R>),
    ) -> usize {
        let mut sent = 0;
        // Only `try_send` writes the ring, and it never writes `None`.
        // `try_consume_visible` never reads the queue's cursor, so this
        // cannot see past the snapshot.
        while let Some((_, Some(reply))) = self.queue.rx.try_consume_visible() {
            emit(reply);
            sent += 1;
        }
        sent
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_boundary::BatchBoundary;

    fn refusal(connection_id: u64) -> EarlyReply<u64> {
        EarlyReply {
            connection_id,
            request_id: RequestId(connection_id * 10),
            kind: EarlyKind::Rejected(connection_id * 100),
        }
    }

    /// Every early reply `registered` holds, by connection, drained
    /// between two events.
    fn drained(registered: &mut Registered<'_, u64>) -> Vec<u64> {
        let boundary = BatchBoundary::new();
        let at = boundary
            .at_boundary()
            .expect("a fresh tracker is between events");
        let mut out = Vec::new();
        let count = registered.drain(&at, |reply| out.push(reply.connection_id));
        assert_eq!(count, out.len(), "the count is of what was handed over");
        out
    }

    /// A reply is seen only once the reader has flushed it and a snapshot
    /// has been taken since; a snapshot drained is not drained twice.
    #[test]
    fn a_refusal_waits_for_both_the_flush_and_a_snapshot() {
        let (mut tx, mut rx) = early_reply_channel::<u64>(EARLY_REPLY_QUEUE_CAPACITY);
        tx.try_send(refusal(1)).unwrap();
        let mut registered = rx.register(|| {});
        assert!(drained(&mut registered).is_empty(), "not flushed");

        tx.flush();
        assert!(
            drained(&mut registered).is_empty(),
            "flushed, but after the snapshot"
        );

        registered.reregister(|| {});
        assert_eq!(drained(&mut registered), [1]);
        assert!(drained(&mut registered).is_empty(), "drained once");
        registered.reregister(|| {});
        assert!(drained(&mut registered).is_empty(), "the queue is empty");
    }

    /// The snapshot is taken before the control channel is drained, so a
    /// reply made visible during the drain (a reply for a connection whose
    /// `Connected` the drain may already be past) waits for the next
    /// snapshot, which a drain follows again. On both ways to take one.
    #[test]
    fn a_snapshot_is_taken_before_the_control_drain() {
        let (mut tx, mut rx) = early_reply_channel::<u64>(EARLY_REPLY_QUEUE_CAPACITY);
        tx.try_send(refusal(1)).unwrap();
        tx.flush();
        let mut drains = 0;
        let mut registered = rx.register(|| {
            drains += 1;
            tx.try_send(refusal(2)).unwrap();
            tx.flush();
        });
        assert_eq!(drains, 1, "registering drains the control channel once");
        assert_eq!(drained(&mut registered), [1], "the snapshot predates 2");

        registered.reregister(|| {
            drains += 1;
            tx.try_send(refusal(3)).unwrap();
            tx.flush();
        });
        assert_eq!(drains, 2);
        assert_eq!(drained(&mut registered), [2], "the snapshot predates 3");

        let mut registered = rx.register(|| drains += 1);
        assert_eq!(drained(&mut registered), [3]);
        assert_eq!(drains, 3);
    }

    #[test]
    fn a_full_queue_is_an_error_not_a_wait() {
        let (mut tx, _rx) = early_reply_channel::<u64>(EARLY_REPLY_QUEUE_CAPACITY);
        for _ in 0..EARLY_REPLY_QUEUE_CAPACITY {
            tx.try_send(refusal(1)).unwrap();
        }
        assert!(tx.try_send(refusal(1)).is_err());
    }

    /// The capacity is the caller's: a queue built smaller fills at its
    /// own size.
    #[test]
    fn a_queue_holds_the_capacity_it_was_built_with() {
        for capacity in [2, 8] {
            let (mut tx, _rx) = early_reply_channel::<u64>(capacity);
            for _ in 0..capacity {
                tx.try_send(refusal(1)).unwrap();
            }
            assert!(tx.try_send(refusal(1)).is_err(), "{capacity}");
        }
    }

    /// A drained queue has room again: capacity bounds replies in flight,
    /// not replies over the queue's life.
    #[test]
    fn a_drained_queue_takes_more() {
        let (mut tx, mut rx) = early_reply_channel::<u64>(EARLY_REPLY_QUEUE_CAPACITY);
        for round in 0..3u64 {
            for _ in 0..EARLY_REPLY_QUEUE_CAPACITY {
                tx.try_send(refusal(round)).unwrap();
            }
            tx.flush();
            let mut registered = rx.register(|| {});
            assert_eq!(
                drained(&mut registered).len(),
                EARLY_REPLY_QUEUE_CAPACITY,
                "round {round}"
            );
        }
    }

    /// Every kind of early reply goes through the queue as it was sent,
    /// with its request's ID, in order among the others.
    #[test]
    fn every_kind_travels_with_its_request_id() {
        let (mut tx, mut rx) = early_reply_channel::<u64>(EARLY_REPLY_QUEUE_CAPACITY);
        let kinds = [
            EarlyKind::Rejected(7),
            EarlyKind::Filtered,
            EarlyKind::Refused {
                kind: RefusalKind::Malformed,
                reason: "bad",
            },
            EarlyKind::Busy,
        ];
        for (n, kind) in (0u64..).zip(kinds) {
            tx.try_send(EarlyReply {
                connection_id: 1,
                request_id: RequestId(100 + n),
                kind,
            })
            .unwrap();
        }
        tx.flush();
        let boundary = BatchBoundary::new();
        let at = boundary.at_boundary().expect("between events");
        let mut released = Vec::new();
        rx.register(|| {})
            .drain(&at, |r| released.push((r.request_id, r.kind)));
        assert_eq!(
            released,
            (100..).map(RequestId).zip(kinds).collect::<Vec<_>>()
        );
    }
}
