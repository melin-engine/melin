//! The requests a connection has sent and not yet had answered, shared by
//! the blocking and the polled connection so both apply the same rules
//! from one implementation.
//!
//! A node answers every request exactly once, with a final frame naming
//! it: a batch terminator, `ServerBusy` or `Refused`. A request the node
//! did not accept may be answered ahead of requests sent before it, so a
//! final frame is looked up by its ID, never assumed to settle the oldest
//! request. `Response` frames carry no ID: they belong to the next batch
//! terminator, and no other final frame may come between them and it.

use std::collections::VecDeque;

use crate::{Error, Reply, RequestId};

/// The IDs a connection has allocated and sent, and whether the reply in
/// progress has had `Response` frames.
#[derive(Debug)]
pub(crate) struct Outstanding {
    /// The ID the next request takes. From 1, so a zeroed buffer is never
    /// mistaken for a connection's first request; `u64`, as the wire's ID
    /// is, so it never wraps on a connection that lives for months.
    next: u64,
    /// IDs sent and not yet answered, in send order, and so sorted: IDs
    /// are allocated in increasing order. A `VecDeque` because the common
    /// answer is in order, the front, popped in O(1); an answer out of
    /// order is found by binary search and removed from the middle, which
    /// moves at most the shorter side.
    ids: VecDeque<RequestId>,
    /// Whether `Response` frames have come since the last final frame: the
    /// reply in progress belongs to the next batch terminator, and a
    /// `ServerBusy` or `Refused` there would split it.
    mid_batch: bool,
}

impl Default for Outstanding {
    fn default() -> Self {
        Self {
            next: 1,
            ids: VecDeque::new(),
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
        self.ids.push_back(id);
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

    /// Account for one reply frame from the node, before it is handed to
    /// the caller. A heartbeat changes nothing. A `Response` opens a batch
    /// (nothing outstanding to attribute it to is [`Error::Protocol`]); a
    /// final frame settles the request it names, which must be
    /// outstanding, and a `ServerBusy` or `Refused` must not fall inside a
    /// batch that holds `Response` frames. Each violation is
    /// [`Error::Protocol`], loudly, not a reply paired with the wrong
    /// request.
    pub(crate) fn observe(&mut self, reply: &Reply<'_>) -> Result<(), Error> {
        match *reply {
            Reply::Heartbeat => Ok(()),
            Reply::Response(_) => {
                if self.ids.is_empty() {
                    return Err(Error::Protocol(
                        "a response with no request outstanding".into(),
                    ));
                }
                self.mid_batch = true;
                Ok(())
            }
            Reply::BatchEnd { id, .. } => self.settle(id, "batch end"),
            Reply::ServerBusy { id } => self.early(id, "busy"),
            Reply::Refused { id, .. } => self.early(id, "refusal"),
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

    /// Settle `id`: the front first, the in-order fast path, else found
    /// by binary search. Never the front in another's place.
    fn settle(&mut self, id: RequestId, what: &str) -> Result<(), Error> {
        if self.ids.front() == Some(&id) {
            self.ids.pop_front();
        } else {
            match self.ids.binary_search(&id) {
                Ok(at) => {
                    self.ids.remove(at);
                }
                Err(_) => {
                    return Err(Error::Protocol(format!(
                        "a {what} for request {}, which is not outstanding",
                        id.0
                    )));
                }
            }
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

    fn batch_end(id: u64) -> Reply<'static> {
        Reply::BatchEnd {
            id: RequestId(id),
            ack: Ack::Policy,
        }
    }

    fn refused(id: u64) -> Reply<'static> {
        Reply::Refused {
            id: RequestId(id),
            kind: RefusalKind::Malformed,
            reason: "no",
        }
    }

    fn busy(id: u64) -> Reply<'static> {
        Reply::ServerBusy { id: RequestId(id) }
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
        outstanding.observe(&Reply::Response(b"a")).unwrap();
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
            idle.observe(&Reply::Response(b"x")),
            Err(Error::Protocol(_))
        ));
    }

    /// Only a batch terminator closes a batch that holds responses; a
    /// heartbeat may fall anywhere.
    #[test]
    fn a_busy_or_refusal_inside_a_batch_is_a_protocol_error() {
        for early in [busy(2), refused(2)] {
            let mut outstanding = with_sent(2);
            outstanding.observe(&Reply::Response(b"part")).unwrap();
            outstanding.observe(&Reply::Heartbeat).unwrap();
            let err = outstanding.observe(&early).unwrap_err();
            assert!(
                err.to_string().contains("inside another request's reply"),
                "{err}"
            );
        }
        let mut outstanding = with_sent(2);
        outstanding.observe(&Reply::Response(b"part")).unwrap();
        outstanding.observe(&Reply::Heartbeat).unwrap();
        outstanding.observe(&batch_end(1)).unwrap();
        // Closed: an early answer may follow.
        outstanding.observe(&busy(2)).unwrap();
    }

    proptest! {
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
                prop_assert!(!outstanding.ids.contains(id));
                for owed in &late {
                    prop_assert!(outstanding.ids.contains(owed));
                }
            }
            for id in &late {
                for _ in 0..responses[(id.0 - 1) as usize] {
                    outstanding.observe(&Reply::Response(b"r")).unwrap();
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
