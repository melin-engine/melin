//! The requests a connection has sent and not yet had answered, shared by
//! the blocking and the polled connection so both apply the same rules
//! from one implementation; and the lookup underneath, [`InFlight`],
//! public for a caller that pairs answers with state of its own.
//!
//! A node answers every request exactly once, with a final frame naming
//! it: a batch terminator, `ServerBusy` or `Refused`. A request the node
//! did not accept may be answered ahead of requests sent before it, so a
//! final frame is looked up by its ID, never assumed to settle the oldest
//! request. `Response` frames carry no ID: they belong to the next batch
//! terminator, and no other final frame may come between them and it.

use std::collections::VecDeque;

use crate::{Error, Frame, RequestId};

/// Requests in flight on one connection, each with a value the caller
/// keeps for it (when it was sent, what its reply is checked against),
/// found again by the ID its final frame names.
///
/// The lookup the connections pair answers with, for a caller that keeps
/// state of its own per request, such as a load generator timing round
/// trips: a request the node did not accept may be answered ahead of the
/// ones sent before it, so an answer is looked up by its ID, never assumed
/// to be the oldest's.
///
/// A map from ID to value, built for how a connection's IDs behave: they
/// are allocated in increasing order, so [`insert`](Self::insert) is a
/// push at the back, and mostly answered in that order, so
/// [`remove`](Self::remove) is mostly a pop at the front. An answer out
/// of order is found by binary search. An ID inserted out of order is
/// placed where it sorts, which moves up to half the entries: for IDs in
/// no particular order (another client's, relayed), a hash map is the
/// better fit.
#[derive(Debug, Clone)]
pub struct InFlight<T> {
    /// Sorted by ID, without repeats. A `VecDeque` rather than a
    /// `BTreeMap`: the in-order insert and remove are O(1) at the two ends
    /// of one contiguous buffer, allocated once at the window's size; an
    /// out-of-order removal moves at most the shorter side, a few entries
    /// for a window of in-flight requests.
    entries: VecDeque<(RequestId, T)>,
}

impl<T> Default for InFlight<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> InFlight<T> {
    /// None in flight. Allocates nothing until the first insert.
    pub fn new() -> Self {
        Self {
            entries: VecDeque::new(),
        }
    }

    /// None in flight, with room for `capacity` before it reallocates: a
    /// caller that keeps a window of requests in flight passes its width.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(capacity),
        }
    }

    /// Request `id` is in flight, with `value`. Returns the value `id`
    /// already held, replaced, as a map does; a connection never sends two
    /// requests under one ID, so for IDs a connection returned it is
    /// `None`.
    pub fn insert(&mut self, id: RequestId, value: T) -> Option<T> {
        // The common case: the newest ID, at the back.
        if self.entries.back().is_none_or(|(last, _)| *last < id) {
            self.entries.push_back((id, value));
            return None;
        }
        match self.find(id) {
            Ok(at) => Some(std::mem::replace(&mut self.entries[at].1, value)),
            Err(at) => {
                self.entries.insert(at, (id, value));
                None
            }
        }
    }

    /// Take request `id` out of flight, answered: its value, or `None` if
    /// it is not in flight. The front first, the in-order answer; never the
    /// front in another's place.
    pub fn remove(&mut self, id: RequestId) -> Option<T> {
        let at = self.find(id).ok()?;
        self.entries.remove(at).map(|(_, value)| value)
    }

    /// Whether request `id` is in flight.
    pub fn contains(&self, id: RequestId) -> bool {
        self.find(id).is_ok()
    }

    /// How many requests are in flight.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no request is in flight.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Where `id` is, or where it would sort: the front checked first, as
    /// the in-order answer, then a binary search.
    fn find(&self, id: RequestId) -> Result<usize, usize> {
        if self.entries.front().is_some_and(|(first, _)| *first == id) {
            return Ok(0);
        }
        self.entries.binary_search_by_key(&id, |(entry, _)| *entry)
    }
}

/// The IDs a connection has allocated and sent, and whether the reply in
/// progress has had `Response` frames.
#[derive(Debug)]
pub(crate) struct Outstanding {
    /// The ID the next request takes. From 1, so a zeroed buffer is never
    /// mistaken for a connection's first request; `u64`, as the wire's ID
    /// is, so it never wraps on a connection that lives for months.
    next: u64,
    /// IDs sent and not yet answered. Nothing kept per request: the
    /// connection only checks that each answer names one.
    ids: InFlight<()>,
    /// Whether `Response` frames have come since the last final frame: the
    /// reply in progress belongs to the next batch terminator, and a
    /// `ServerBusy` or `Refused` there would split it.
    mid_batch: bool,
}

impl Default for Outstanding {
    fn default() -> Self {
        Self {
            next: 1,
            ids: InFlight::new(),
            mid_batch: false,
        }
    }
}

impl Outstanding {
    /// The ID the next request sent takes. Taken only by
    /// [`sent`](Self::sent), so a request that fails to frame leaves it
    /// for the next.
    #[inline]
    pub(crate) fn next_id(&self) -> RequestId {
        RequestId(self.next)
    }

    /// The request [`next_id`](Self::next_id) named has been framed and
    /// queued: it is owed an answer, and the next request takes the next
    /// ID.
    #[inline]
    pub(crate) fn sent(&mut self) -> RequestId {
        let id = RequestId(self.next);
        self.next += 1;
        self.ids.insert(id, ());
        id
    }

    /// Whether no request is owed an answer.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// How many requests are owed an answer.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.ids.len()
    }

    /// Account for one frame from the node, before it is handed to the
    /// caller. Heartbeats never come here: they change nothing, and a
    /// connection skips them before it classifies a frame. A `Response`
    /// opens a batch (nothing outstanding to attribute it to is
    /// [`Error::Protocol`]); a final frame settles the request it names,
    /// which must be outstanding, and a `ServerBusy` or `Refused` must not
    /// fall inside a batch that holds `Response` frames. Each violation is
    /// [`Error::Protocol`], loudly, not a reply paired with the wrong
    /// request.
    pub(crate) fn observe(&mut self, frame: &Frame<'_>) -> Result<(), Error> {
        match *frame {
            Frame::Response(_) => {
                if self.ids.is_empty() {
                    return Err(Error::Protocol(
                        "a response with no request outstanding".into(),
                    ));
                }
                self.mid_batch = true;
                Ok(())
            }
            Frame::BatchEnd { id, .. } => self.settle(id, "batch end"),
            Frame::ServerBusy { id } => self.early(id, "busy"),
            Frame::Refused { id, .. } => self.early(id, "refusal"),
        }
    }

    /// A final frame that closes no batch: refused inside one.
    fn early(&mut self, id: RequestId, what: &str) -> Result<(), Error> {
        if self.mid_batch {
            return Err(Error::Protocol(format!(
                "a {what} for request {} inside another request's reply",
                id.0
            )));
        }
        self.settle(id, what)
    }

    /// Settle `id`, which must be outstanding.
    fn settle(&mut self, id: RequestId, what: &str) -> Result<(), Error> {
        if self.ids.remove(id).is_none() {
            return Err(Error::Protocol(format!(
                "a {what} for request {}, which is not outstanding",
                id.0
            )));
        }
        self.mid_batch = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::{Ack, RefusalKind};

    fn batch_end(id: u64) -> Frame<'static> {
        Frame::BatchEnd {
            id: RequestId(id),
            ack: Ack::Policy,
        }
    }

    fn refused(id: u64) -> Frame<'static> {
        Frame::Refused {
            id: RequestId(id),
            kind: RefusalKind::Malformed,
            reason: "no",
        }
    }

    fn busy(id: u64) -> Frame<'static> {
        Frame::ServerBusy { id: RequestId(id) }
    }

    /// A tracker with requests 1..=n sent.
    fn with_sent(n: u64) -> Outstanding {
        let mut outstanding = Outstanding::default();
        for expected in 1..=n {
            assert_eq!(outstanding.next_id(), RequestId(expected));
            assert_eq!(outstanding.sent(), RequestId(expected));
        }
        outstanding
    }

    #[test]
    fn ids_start_at_one_and_increase() {
        let mut outstanding = Outstanding::default();
        assert!(outstanding.is_empty());
        assert_eq!(outstanding.next_id(), RequestId(1));
        // Peeking takes nothing.
        assert_eq!(outstanding.next_id(), RequestId(1));
        assert_eq!(outstanding.sent(), RequestId(1));
        assert_eq!(outstanding.sent(), RequestId(2));
        assert_eq!(outstanding.len(), 2);
        assert_eq!(outstanding.next_id(), RequestId(3));
    }

    #[test]
    fn in_order_answers_settle_the_front() {
        let mut outstanding = with_sent(3);
        outstanding.observe(&Frame::Response(b"a")).unwrap();
        outstanding.observe(&batch_end(1)).unwrap();
        outstanding.observe(&busy(2)).unwrap();
        outstanding.observe(&refused(3)).unwrap();
        assert!(outstanding.is_empty());
    }

    /// An early answer settles its own request and leaves the front owed.
    #[test]
    fn an_early_answer_settles_its_own_request() {
        for early in [busy(2), refused(2), batch_end(2)] {
            let mut outstanding = with_sent(3);
            outstanding.observe(&early).unwrap();
            assert_eq!(outstanding.len(), 2, "{early:?}");
            // 2 is gone: answering it again is a violation.
            assert!(matches!(
                outstanding.observe(&batch_end(2)),
                Err(Error::Protocol(_))
            ));
            outstanding.observe(&batch_end(1)).unwrap();
            outstanding.observe(&batch_end(3)).unwrap();
            assert!(outstanding.is_empty());
        }
    }

    #[test]
    fn an_answer_for_no_outstanding_request_is_a_protocol_error() {
        let mut outstanding = with_sent(2);
        for unknown in [batch_end(0), batch_end(3), busy(9), refused(u64::MAX)] {
            let err = outstanding.observe(&unknown).unwrap_err();
            assert!(err.to_string().contains("not outstanding"), "{err}");
        }
        assert_eq!(outstanding.len(), 2, "nothing settled in their place");
        // A response with nothing owed.
        let mut idle = Outstanding::default();
        assert!(matches!(
            idle.observe(&Frame::Response(b"x")),
            Err(Error::Protocol(_))
        ));
    }

    /// Only a batch terminator closes a batch that holds responses. (A
    /// heartbeat may fall anywhere, but never reaches the tracker: the
    /// connections' tests pin one inside a batch.)
    #[test]
    fn a_busy_or_refusal_inside_a_batch_is_a_protocol_error() {
        for early in [busy(2), refused(2)] {
            let mut outstanding = with_sent(2);
            outstanding.observe(&Frame::Response(b"part")).unwrap();
            let err = outstanding.observe(&early).unwrap_err();
            assert!(
                err.to_string().contains("inside another request's reply"),
                "{err}"
            );
        }
        let mut outstanding = with_sent(2);
        outstanding.observe(&Frame::Response(b"part")).unwrap();
        outstanding.observe(&batch_end(1)).unwrap();
        // Closed: an early answer may follow.
        outstanding.observe(&busy(2)).unwrap();
    }

    #[test]
    fn in_flight_pairs_each_answer_with_its_own_value() {
        let mut in_flight = InFlight::with_capacity(4);
        assert!(in_flight.is_empty());
        for id in 1..=4 {
            assert_eq!(in_flight.insert(RequestId(id), id * 10), None);
        }
        // An answer out of order takes its own value, not the front's.
        assert_eq!(in_flight.remove(RequestId(3)), Some(30));
        assert_eq!(in_flight.remove(RequestId(3)), None, "answered once");
        assert_eq!(in_flight.remove(RequestId(1)), Some(10));
        assert!(!in_flight.contains(RequestId(1)));
        assert!(in_flight.contains(RequestId(2)) && in_flight.contains(RequestId(4)));
        assert_eq!(in_flight.remove(RequestId(9)), None);
        assert_eq!(in_flight.len(), 2);
        // Inserted out of order, placed where it sorts; inserted again,
        // replaced, as a map does.
        assert_eq!(in_flight.insert(RequestId(3), 33), None);
        assert_eq!(in_flight.insert(RequestId(2), 22), Some(20));
        assert_eq!(in_flight.remove(RequestId(2)), Some(22));
        assert_eq!(in_flight.remove(RequestId(3)), Some(33));
        assert_eq!(in_flight.remove(RequestId(4)), Some(40));
        assert!(in_flight.is_empty());
    }

    proptest! {
        /// Whatever the order of inserts and removals, `InFlight` answers
        /// as a map does: the model is a `BTreeMap`.
        #[test]
        fn in_flight_behaves_as_a_map(
            ops in prop::collection::vec((any::<bool>(), 0u64..24, any::<u32>()), 0..200),
        ) {
            let mut in_flight = InFlight::new();
            let mut model = std::collections::BTreeMap::new();
            for (insert, id, value) in ops {
                let id = RequestId(id);
                if insert {
                    prop_assert_eq!(in_flight.insert(id, value), model.insert(id, value));
                } else {
                    prop_assert_eq!(in_flight.remove(id), model.remove(&id));
                }
                prop_assert_eq!(in_flight.len(), model.len());
                prop_assert_eq!(in_flight.contains(id), model.contains_key(&id));
            }
            let entries: Vec<_> = in_flight.entries.iter().copied().collect();
            let expected: Vec<_> = model.into_iter().collect();
            prop_assert_eq!(entries, expected);
        }

        /// Requests sent, answered in send order except for some answered
        /// early, ahead of the requests before them, with a batch
        /// terminator, a busy or a refusal (batch terminators among them,
        /// as a filtered request's empty batch is): each ID is settled
        /// exactly once, and an early answer never settles the front in
        /// its place.
        #[test]
        fn every_id_is_settled_once_whatever_the_interleaving(
            count in 1u64..40,
            early_mask in any::<u64>(),
            kinds in prop::collection::vec(0u8..3, 40),
            responses in prop::collection::vec(0usize..3, 40),
        ) {
            let mut outstanding = Outstanding::default();
            let mut sent = Vec::new();
            for _ in 0..count {
                sent.push(outstanding.sent());
            }
            // The early ones go first, in send order among themselves,
            // then the rest in send order.
            let (early, late): (Vec<RequestId>, Vec<RequestId>) =
                sent.iter().partition(|id| early_mask & (1 << (id.0 % 64)) != 0);
            for id in &early {
                let reply = match kinds[(id.0 - 1) as usize] {
                    0 => busy(id.0),
                    1 => refused(id.0),
                    _ => batch_end(id.0),
                };
                outstanding.observe(&reply).unwrap();
                // Only that one went: every request answered later, the
                // front among them, is still owed.
                prop_assert!(!outstanding.ids.contains(*id));
                for owed in &late {
                    prop_assert!(outstanding.ids.contains(*owed));
                }
            }
            for id in &late {
                for _ in 0..responses[(id.0 - 1) as usize] {
                    outstanding.observe(&Frame::Response(b"r")).unwrap();
                }
                outstanding.observe(&batch_end(id.0)).unwrap();
            }
            prop_assert!(outstanding.is_empty());
            // Every ID answered once: a second answer for any of them is
            // a protocol error.
            for id in &sent {
                prop_assert!(outstanding.observe(&batch_end(id.0)).is_err());
            }
        }
    }
}
