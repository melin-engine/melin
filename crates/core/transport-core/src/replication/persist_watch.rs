//! Notices a replica that keeps accepting entries but persists none.
//!
//! A replica acks on two tracks: what it has taken into its pipeline
//! (`in_memory_sequence`) and what its journal has made durable
//! (`acked_sequence`). When its journal stops advancing while its
//! receiver keeps taking entries, nothing else says so: the in-memory
//! track keeps satisfying an ack policy that counts it, and the replica
//! looks healthy until its input ring fills and it stalls — then it is
//! evicted for ring backpressure, which points at the network or the
//! replica's speed rather than at its pipeline. A journal stage that never
//! started, a stage that wedged and a disk that hangs all look like this.
//! [`PersistWatch`] turns that into a warning within seconds.

use std::time::{Duration, Instant};

use super::protocol::Ack;

/// How long a replica may hold accepted entries without persisting any
/// before [`PersistWatch`] reports it. Long enough that a group commit, a
/// slow fsync or a segment rotation never trips it; short enough to warn
/// long before a stalled replica's input ring fills under load.
pub const PERSIST_STALL_BOUND: Duration = Duration::from_secs(5);

/// One replica connection's persist watch, fed every ack the primary
/// applies. Starts afresh with each connection: the first ack sets the
/// persisted baseline.
#[derive(Debug, Default)]
pub struct PersistWatch {
    /// The highest persisted sequence acked so far on this connection.
    persisted: u64,
    /// Since when the replica has held accepted entries beyond
    /// `persisted` without persisting any; `None` while it holds none.
    /// Counted from when they appeared, not from the last ack, so a replica
    /// back from an idle stretch is not reported on its first ack.
    pending_since: Option<Instant>,
    /// Whether the current stall has been reported, so it is once.
    reported: bool,
}

/// What [`PersistWatch::observe`] found worth reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistEvent {
    /// The replica has held accepted entries for at least
    /// [`PERSIST_STALL_BOUND`] without persisting any.
    Stalled {
        persisted: u64,
        in_memory: u64,
        pending_for: Duration,
    },
    /// A reported stall ended: the replica persisted again.
    Resumed {
        persisted: u64,
        stalled_for: Duration,
    },
}

impl PersistWatch {
    /// Record one ack, received at `now`. Returns an event once when the
    /// replica has stalled, and once when a reported stall ends.
    pub fn observe(&mut self, ack: &Ack, now: Instant) -> Option<PersistEvent> {
        if ack.acked_sequence > self.persisted {
            self.persisted = ack.acked_sequence;
            let resumed = self.reported.then(|| PersistEvent::Resumed {
                persisted: ack.acked_sequence,
                stalled_for: self
                    .pending_since
                    .map_or(Duration::ZERO, |since| now.saturating_duration_since(since)),
            });
            self.reported = false;
            self.pending_since = (ack.in_memory_sequence > ack.acked_sequence).then_some(now);
            return resumed;
        }
        if ack.in_memory_sequence <= self.persisted {
            self.pending_since = None;
            return None;
        }
        let since = *self.pending_since.get_or_insert(now);
        let pending_for = now.saturating_duration_since(since);
        if self.reported || pending_for < PERSIST_STALL_BOUND {
            return None;
        }
        self.reported = true;
        Some(PersistEvent::Stalled {
            persisted: self.persisted,
            in_memory: ack.in_memory_sequence,
            pending_for,
        })
    }
}

impl PersistEvent {
    /// Log the event for replica `slot`: a stall at `warn!` — degraded
    /// replication an operator has to look at — and its end at `info!`.
    pub fn log(&self, slot: usize) {
        match *self {
            Self::Stalled {
                persisted,
                in_memory,
                pending_for,
            } => tracing::warn!(
                slot,
                persisted,
                in_memory,
                pending_ms = pending_for.as_millis() as u64,
                "replica accepts entries but persists none: its journal stage or disk is \
                 stalled, and it will be evicted once its input ring fills"
            ),
            Self::Resumed {
                persisted,
                stalled_for,
            } => tracing::info!(
                slot,
                persisted,
                stalled_ms = stalled_for.as_millis() as u64,
                "replica persisting again"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(persisted: u64, in_memory: u64) -> Ack {
        Ack {
            acked_sequence: persisted,
            in_memory_sequence: in_memory,
        }
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }

    /// The wedge this exists for: the in-memory track climbs, the
    /// persisted one never moves. Reported once, after the bound.
    #[test]
    fn accepted_entries_never_persisted_are_reported_once() {
        let start = Instant::now();
        let mut watch = PersistWatch::default();
        // Handshake baseline: everything up to 100 persisted.
        assert_eq!(watch.observe(&ack(100, 100), start), None);
        assert_eq!(watch.observe(&ack(100, 200), start + secs(1)), None);
        assert_eq!(watch.observe(&ack(100, 900), start + secs(5)), None);
        assert_eq!(
            watch.observe(&ack(100, 1_000), start + secs(6)),
            Some(PersistEvent::Stalled {
                persisted: 100,
                in_memory: 1_000,
                pending_for: secs(5),
            })
        );
        assert_eq!(watch.observe(&ack(100, 2_000), start + secs(20)), None);
    }

    /// Under load the in-memory track always leads; a persisted track
    /// that keeps up, however far behind, is no stall.
    #[test]
    fn a_replica_that_keeps_persisting_is_never_reported() {
        let start = Instant::now();
        let mut watch = PersistWatch::default();
        for second in 0..60 {
            let persisted = second * 1_000;
            let now = start + secs(second);
            assert_eq!(
                watch.observe(&ack(persisted, persisted + 50_000), now),
                None
            );
        }
    }

    /// A replica caught up and idle, then given work: its first ack with
    /// entries pending is not a stall, however long the idle stretch.
    #[test]
    fn pending_time_counts_from_when_entries_appear_not_from_the_last_ack() {
        let start = Instant::now();
        let mut watch = PersistWatch::default();
        assert_eq!(watch.observe(&ack(100, 100), start), None);
        assert_eq!(watch.observe(&ack(100, 150), start + secs(60)), None);
        assert_eq!(watch.observe(&ack(150, 150), start + secs(61)), None);
    }

    /// A reported stall that ends is reported as ended, and a later stall
    /// is reported again.
    #[test]
    fn a_reported_stall_that_ends_says_so_and_can_be_reported_again() {
        let start = Instant::now();
        let mut watch = PersistWatch::default();
        assert_eq!(watch.observe(&ack(10, 20), start), None);
        assert!(matches!(
            watch.observe(&ack(10, 30), start + secs(5)),
            Some(PersistEvent::Stalled { .. })
        ));
        assert_eq!(
            watch.observe(&ack(30, 30), start + secs(8)),
            Some(PersistEvent::Resumed {
                persisted: 30,
                stalled_for: secs(8),
            })
        );
        assert_eq!(watch.observe(&ack(30, 40), start + secs(9)), None);
        assert!(matches!(
            watch.observe(&ack(30, 50), start + secs(14)),
            Some(PersistEvent::Stalled { persisted: 30, .. })
        ));
    }

    /// Persisted progress that does not end the stall — the replica
    /// persisted something but still holds entries — resets the clock
    /// without being reported, as no stall was.
    #[test]
    fn progress_before_the_bound_restarts_the_clock_silently() {
        let start = Instant::now();
        let mut watch = PersistWatch::default();
        assert_eq!(watch.observe(&ack(10, 100), start), None);
        assert_eq!(watch.observe(&ack(20, 100), start + secs(4)), None);
        assert_eq!(watch.observe(&ack(20, 100), start + secs(8)), None);
        assert!(matches!(
            watch.observe(&ack(20, 100), start + secs(9)),
            Some(PersistEvent::Stalled { .. })
        ));
    }
}
