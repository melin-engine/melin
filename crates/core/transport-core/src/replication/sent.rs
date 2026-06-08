//! Sent high-water tracking for one replica connection.
//!
//! [`SentHighWater`] is the primary's record of the highest sequence it
//! has actually streamed to a replica — the bound
//! [`ReplicaCursors::record_ack`] checks acks against, and the value
//! heartbeats advertise. The type exists for the same reason
//! [`ReplicaCursors`] does: the bound is load-bearing (an ack beyond it
//! evicts the replica), so its updates must be impossible to get wrong
//! at the call sites. Two guarantees by construction:
//!
//! - **Monotonic within a connection** — [`SentHighWater::advance`] is
//!   a saturating max, so a ring chunk already covered by catch-up can
//!   never regress the bound below the catch-up end. A regression
//!   would make the replica's next truthful ack (`in_memory` up to the
//!   catch-up end) look like a protocol violation and falsely evict a
//!   healthy replica — exactly the "never a load effect" guarantee the
//!   ack-sanity invariant promises.
//! - **Single drain criterion** — the catch-up→live drain
//!   (`drain_into_contiguity`, reached by both the kernel-TCP and DPDK
//!   senders through `bridge_catchup_to_live`) decides skip-vs-forward
//!   from this bound rather than restating it. The two senders had
//!   previously each restated the condition and drifted — TCP compared
//!   against the catch-up end, DPDK against the handshake sequence — so
//!   funnelling both through one bound is what keeps them aligned.
//!
//! [`ReplicaCursors`]: super::cursors::ReplicaCursors
//! [`ReplicaCursors::record_ack`]: super::cursors::ReplicaCursors::record_ack

/// Highest primary sequence streamed to one replica connection.
///
/// Plain `u64`, not atomic: the value is owned by the slot's sender
/// (the per-slot handler thread on the kernel-TCP path, the
/// single-threaded driver loop on the DPDK path) and never shared —
/// unlike [`ReplicaCursors`], whose cursors are read concurrently by
/// the response gate and the health endpoint.
///
/// [`ReplicaCursors`]: super::cursors::ReplicaCursors
pub struct SentHighWater(u64);

impl SentHighWater {
    /// Seed after handshake + catch-up: everything up to `catchup_end`
    /// has been streamed. `CatchUpResult::Ok` is monotonic from the
    /// handshake value, so the max normally resolves to `catchup_end`;
    /// taking it explicitly makes that assumption load-bearing instead
    /// of implicit.
    pub fn seed(handshake_last_sequence: u64, catchup_end: u64) -> Self {
        Self(handshake_last_sequence.max(catchup_end))
    }

    /// Record that sequences up to `end_sequence` have been streamed.
    /// Saturating max — an already-covered chunk must not regress the
    /// bound (see the module docs for why a regression is an eviction
    /// hazard).
    #[inline]
    pub fn advance(&mut self, end_sequence: u64) {
        self.0 = self.0.max(end_sequence);
    }

    /// The current bound — passed to `record_ack` as
    /// `highest_sent_sequence` and advertised in heartbeats.
    #[inline]
    pub fn get(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_takes_the_max_of_handshake_and_catchup_end() {
        assert_eq!(SentHighWater::seed(100, 300).get(), 300);
        // Defensive: catch-up is monotonic from the handshake value,
        // but if that ever breaks the seed must not regress.
        assert_eq!(SentHighWater::seed(100, 50).get(), 100);
        assert_eq!(SentHighWater::seed(0, 0).get(), 0);
    }

    #[test]
    fn advance_never_regresses() {
        let mut sent = SentHighWater::seed(0, 10);
        sent.advance(25);
        assert_eq!(sent.get(), 25);
        // A lower end_sequence (re-sent / already-covered chunk) holds
        // the bound rather than regressing it.
        sent.advance(5);
        assert_eq!(sent.get(), 25);
    }
}
