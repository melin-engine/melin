//! Cumulative counters and loss-event reporting for monitoring.
//!
//! [`Counters`] is a flat struct of `AtomicU64`s — the [`SenderLoop`]
//! and [`ReceiverLoop`] each hold an `Arc<Counters>` (optional) and
//! bump the relevant fields after every tick. External monitors
//! (`melin-admin`, Prometheus exporters, etc.) read via
//! [`Counters::snapshot`].
//!
//! Counters are written via `Relaxed` ordering — the per-tick bump is
//! the hottest non-data path and we don't need cross-counter
//! consistency. Snapshots are eventually consistent: individual fields
//! are atomic, but a snapshot loads each field independently, so a
//! reader may see counter A from `t1` and counter B from `t2 > t1`.
//! This is the standard behavior of stats counters in the workspace
//! and is appropriate for monitoring; it would NOT be appropriate as
//! a basis for protocol decisions.
//!
//! [`LossEvent`] carries the (term_id, term_offset, gap_length) of a
//! gap as soon as the receiver detects it. A test or production
//! caller can install a [`LossCallback`] on the receiver to forward
//! these to a journal, log file, or alerting system. The callback is
//! optional — when absent, only the [`Counters::gaps_detected`] /
//! [`Counters::bytes_in_gaps`] atomic counters are bumped.
//!
//! [`SenderLoop`]: crate::sender::SenderLoop
//! [`ReceiverLoop`]: crate::receiver::ReceiverLoop

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Cumulative atomic counters. Sender and receiver loops update
/// disjoint subsets of these (and never the same field), so cross-
/// thread contention on any single field is rare. Cache-padding the
/// hottest counters (e.g. `bytes_sent`, `bytes_received`) is a
/// deferred optimization — measure first.
#[derive(Debug, Default)]
pub struct Counters {
    // ---- Sender-side ----
    pub bytes_sent: AtomicU64,
    pub fragments_sent: AtomicU64,
    pub retransmits_sent: AtomicU64,
    pub setups_sent: AtomicU64,
    pub heartbeats_sent: AtomicU64,
    pub naks_received: AtomicU64,
    pub sms_received: AtomicU64,
    pub send_errors_sender: AtomicU64,
    pub control_drops_sender: AtomicU64,

    // ---- Receiver-side ----
    pub bytes_received: AtomicU64,
    pub fragments_accepted: AtomicU64,
    pub fragments_dropped: AtomicU64,
    pub setups_received: AtomicU64,
    pub heartbeats_received: AtomicU64,
    pub naks_sent: AtomicU64,
    pub naks_suppressed: AtomicU64,
    pub sms_sent: AtomicU64,
    pub send_errors_receiver: AtomicU64,
    pub recv_errors: AtomicU64,
    pub control_drops_receiver: AtomicU64,

    // ---- Loss tracking ----
    /// Number of distinct gap-detection events raised by the receiver.
    pub gaps_detected: AtomicU64,
    /// Sum of `gap_length` over all detected gaps. Best-effort total
    /// "bytes that needed recovery" — note that NAK suppression /
    /// out-of-order arrival means many of these bytes were never
    /// actually retransmitted on the wire.
    pub bytes_in_gaps: AtomicU64,
}

/// Eventually-consistent snapshot of [`Counters`] — each field loaded
/// independently with `Relaxed`. Fine for monitoring dashboards;
/// don't gate protocol decisions on it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CountersSnapshot {
    pub bytes_sent: u64,
    pub fragments_sent: u64,
    pub retransmits_sent: u64,
    pub setups_sent: u64,
    pub heartbeats_sent: u64,
    pub naks_received: u64,
    pub sms_received: u64,
    pub send_errors_sender: u64,
    pub control_drops_sender: u64,

    pub bytes_received: u64,
    pub fragments_accepted: u64,
    pub fragments_dropped: u64,
    pub setups_received: u64,
    pub heartbeats_received: u64,
    pub naks_sent: u64,
    pub naks_suppressed: u64,
    pub sms_sent: u64,
    pub send_errors_receiver: u64,
    pub recv_errors: u64,
    pub control_drops_receiver: u64,

    pub gaps_detected: u64,
    pub bytes_in_gaps: u64,
}

impl Counters {
    pub fn new() -> Self {
        Self::default()
    }

    /// Load every field with `Relaxed`. The result is eventually
    /// consistent — see the module docs.
    pub fn snapshot(&self) -> CountersSnapshot {
        let r = Ordering::Relaxed;
        CountersSnapshot {
            bytes_sent: self.bytes_sent.load(r),
            fragments_sent: self.fragments_sent.load(r),
            retransmits_sent: self.retransmits_sent.load(r),
            setups_sent: self.setups_sent.load(r),
            heartbeats_sent: self.heartbeats_sent.load(r),
            naks_received: self.naks_received.load(r),
            sms_received: self.sms_received.load(r),
            send_errors_sender: self.send_errors_sender.load(r),
            control_drops_sender: self.control_drops_sender.load(r),

            bytes_received: self.bytes_received.load(r),
            fragments_accepted: self.fragments_accepted.load(r),
            fragments_dropped: self.fragments_dropped.load(r),
            setups_received: self.setups_received.load(r),
            heartbeats_received: self.heartbeats_received.load(r),
            naks_sent: self.naks_sent.load(r),
            naks_suppressed: self.naks_suppressed.load(r),
            sms_sent: self.sms_sent.load(r),
            send_errors_receiver: self.send_errors_receiver.load(r),
            recv_errors: self.recv_errors.load(r),
            control_drops_receiver: self.control_drops_receiver.load(r),

            gaps_detected: self.gaps_detected.load(r),
            bytes_in_gaps: self.bytes_in_gaps.load(r),
        }
    }
}

/// A gap detected by the receiver — `gap_length` bytes are missing
/// in `[term_id, term_offset, term_offset + gap_length)`.
///
/// Reported as soon as the receiver schedules a NAK for the gap.
/// Note: a reported event isn't necessarily an actual network loss;
/// out-of-order delivery can produce transient gaps that resolve
/// before the NAK fires. Treat as "delivery was incomplete at this
/// instant", not "the network dropped these bytes for sure".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LossEvent {
    pub session_id: u32,
    pub stream_id: u32,
    pub term_id: u32,
    pub term_offset: u32,
    pub gap_length: u32,
    pub detected_at: Instant,
}

/// Optional callback installed on the receiver to forward
/// [`LossEvent`]s to a journal, log file, or alerting system. The
/// callback runs on the receiver thread — keep it cheap (don't
/// block, don't allocate large buffers, prefer pushing onto an
/// SPSC ring).
pub type LossCallback = Box<dyn Fn(&LossEvent) + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_snapshot_is_zeroes() {
        let c = Counters::new();
        let snap = c.snapshot();
        assert_eq!(snap, CountersSnapshot::default());
    }

    #[test]
    fn fetch_add_then_snapshot_observes_value() {
        let c = Counters::new();
        c.bytes_sent.fetch_add(100, Ordering::Relaxed);
        c.fragments_sent.fetch_add(7, Ordering::Relaxed);
        c.gaps_detected.fetch_add(1, Ordering::Relaxed);
        c.bytes_in_gaps.fetch_add(96, Ordering::Relaxed);
        let snap = c.snapshot();
        assert_eq!(snap.bytes_sent, 100);
        assert_eq!(snap.fragments_sent, 7);
        assert_eq!(snap.gaps_detected, 1);
        assert_eq!(snap.bytes_in_gaps, 96);
        // Untouched fields stay zero.
        assert_eq!(snap.bytes_received, 0);
        assert_eq!(snap.naks_sent, 0);
    }

    #[test]
    fn snapshot_reads_all_field_groups() {
        let c = Counters::new();
        // Touch one field from each group (sender / receiver / loss).
        c.bytes_sent.fetch_add(1, Ordering::Relaxed);
        c.bytes_received.fetch_add(2, Ordering::Relaxed);
        c.gaps_detected.fetch_add(3, Ordering::Relaxed);
        let snap = c.snapshot();
        assert_eq!(snap.bytes_sent, 1);
        assert_eq!(snap.bytes_received, 2);
        assert_eq!(snap.gaps_detected, 3);
    }

    #[test]
    fn loss_callback_runs_when_invoked() {
        use std::sync::Mutex;
        let collected: std::sync::Arc<Mutex<Vec<LossEvent>>> =
            std::sync::Arc::new(Mutex::new(Vec::new()));
        let collected_for_cb = std::sync::Arc::clone(&collected);
        let cb: LossCallback = Box::new(move |ev: &LossEvent| {
            collected_for_cb.lock().unwrap().push(*ev);
        });
        let event = LossEvent {
            session_id: 7,
            stream_id: 11,
            term_id: 100,
            term_offset: 4096,
            gap_length: 192,
            detected_at: Instant::now(),
        };
        cb(&event);
        let got = collected.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], event);
    }
}
