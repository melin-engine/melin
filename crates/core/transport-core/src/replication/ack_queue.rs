//! Ack queueing and dual-track cursor coordination for the
//! replication response path.
//!
//! The primary's response gate evaluates a multi-level durability
//! policy (see [`crate::durability_policy`]) against per-node cursor
//! pairs. The types here translate between local-disruptor-ring
//! positions and primary-sequence positions, queue pending acks while
//! the journal stage catches up, and decide when a fresh ack frame
//! needs to be sent on the wire.

use std::sync::atomic::Ordering;

use melin_pipeline::padding::Sequence;

use super::protocol::Ack;

/// Pending ack waiting for journal durability confirmation.
#[derive(Clone)]
pub struct PendingAck {
    /// Disruptor sequence target — ack is safe to send once the journal
    /// cursor reaches this value.
    journal_target: u64,
    /// Wire-protocol sequence to include in the ack frame.
    acked_sequence: u64,
}

/// Fixed-capacity circular buffer of pending acks. Decouples TCP receives
/// from journal fsync by allowing up to `cap` batches to be submitted to
/// the journal stage before any ack is sent. Acks are flushed in FIFO
/// order as the journal cursor advances.
///
/// `Box<[PendingAck]>` rather than `Vec<PendingAck>`: the capacity is
/// fixed at construction; a slice avoids the `Vec`'s capacity field on
/// every push/pop. `cap` is constrained to a power of two so the
/// modulo on `head + len` lowers to a single `AND`.
pub struct PendingAckQueue {
    /// Heap-allocated circular buffer. Length equals `cap`.
    buf: Box<[PendingAck]>,
    /// Capacity; must be a power of two.
    cap: usize,
    /// Index of the oldest pending ack (next to flush).
    head: usize,
    /// Number of pending acks in the queue.
    len: usize,
}

impl PendingAckQueue {
    /// `cap` must be a power of two and ≥ 1.
    pub fn new(cap: usize) -> Self {
        assert!(
            cap.is_power_of_two(),
            "PendingAckQueue cap must be a power of two"
        );
        Self {
            buf: vec![
                PendingAck {
                    journal_target: 0,
                    acked_sequence: 0,
                };
                cap
            ]
            .into_boxed_slice(),
            cap,
            head: 0,
            len: 0,
        }
    }

    pub fn is_full(&self) -> bool {
        self.len >= self.cap
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// Record a pending ack. Caller must ensure `!is_full()`.
    pub fn push(&mut self, journal_target: u64, acked_sequence: u64) {
        debug_assert!(!self.is_full());
        let idx = (self.head + self.len) & (self.cap - 1);
        self.buf[idx] = PendingAck {
            journal_target,
            acked_sequence,
        };
        self.len += 1;
    }

    /// Pop acks for all batches where the journal cursor has caught up.
    /// Non-blocking — returns `None` immediately if the oldest pending
    /// batch isn't durable yet. Returns the highest acked sequence
    /// among the flushed entries.
    pub fn pop_ready(&mut self, journal_cursor: &Sequence) -> Option<u64> {
        if self.is_empty() {
            return None;
        }
        let cursor_val = journal_cursor.get().load(Ordering::Acquire);
        let mut last_acked = None;
        while self.len > 0 {
            let entry = &self.buf[self.head];
            if cursor_val < entry.journal_target {
                break; // Not durable yet.
            }
            last_acked = Some(entry.acked_sequence);
            self.head = (self.head + 1) & (self.cap - 1);
            self.len -= 1;
        }
        last_acked
    }

    /// Block until the oldest pending ack is durable, then pop all
    /// ready entries. Returns the highest acked sequence.
    pub fn pop_oldest_blocking(&mut self, journal_cursor: &Sequence, busy_spin: bool) -> u64 {
        debug_assert!(!self.is_empty());
        let target = self.buf[self.head].journal_target;
        wait_for_journal_cursor(journal_cursor, target, busy_spin);
        // The cursor advanced — pop this entry plus any others that
        // are now also durable.
        self.pop_ready(journal_cursor)
            .expect("at least one entry became ready after wait")
    }

    /// Block until ALL pending acks are durable. Returns the highest
    /// acked sequence, or `None` if the queue was already empty.
    pub fn pop_all_blocking(&mut self, journal_cursor: &Sequence, busy_spin: bool) -> Option<u64> {
        let mut last = None;
        while !self.is_empty() {
            last = Some(self.pop_oldest_blocking(journal_cursor, busy_spin));
        }
        last
    }
}

/// Wait until the journal cursor crosses `target`. `busy_spin=true` keeps
/// the wait on the CPU (production default — ack RTT is on the primary's
/// response-gate critical path, where a `yield_now` scheduler tick adds
/// ~1ms to client p99). `busy_spin=false` yields after a short spin so a
/// stalled cursor doesn't peg a core under `--yield-idle` (tests / CI /
/// shared boxes).
pub fn wait_for_journal_cursor(journal_cursor: &Sequence, target: u64, busy_spin: bool) {
    let mut spins: u32 = 0;
    while journal_cursor.get().load(Ordering::Acquire) < target {
        if busy_spin || spins < 1000 {
            spins = spins.wrapping_add(1);
            std::hint::spin_loop();
        } else {
            std::thread::yield_now();
        }
    }
}

/// Build the next replica → primary [`Ack`] to fire under the
/// dual-track model. Returns `Some(Ack)` when one of the two cursor
/// tracks has advanced beyond the last value sent on the wire.
///
/// Tracks:
///
/// 1. **Persisted** — `pop_ready(journal_cursor)` returns the latest
///    primary sequence whose local-ring target the journal has crossed.
///    [`PendingAckQueue`] is what maps local-ring positions (the
///    space `journal_cursor` indexes) to primary sequences (the space
///    the wire `acked_sequence` field is in). The receive path no
///    longer waits on the queue for ack-on-receive, but the queue is
///    still load-bearing for this namespace translation.
/// 2. **In-memory** — `accum_end_sequence` directly. That's the
///    highest primary sequence the receiver has published to the
///    local input ring (pre-journal). Advances on every received
///    batch.
///
/// Either track advancing past the corresponding `last_sent_*` value
/// triggers an ack. Coalescing is per-call: while a SEND is in flight
/// the caller gates this function (skipping it), so both tracks can
/// advance freely; the next call after SEND completes fires one ack
/// carrying the latest values.
///
/// # Tracker update contract
///
/// **Callers MUST update `*last_sent_acked` / `*last_sent_in_memory`
/// to the returned ack's fields AFTER the wire SEND succeeds, not
/// before.** Marking a value as sent before the actual send completes
/// would silently skip resending after a transient send failure
/// (e.g. DPDK `ACK_RETRY_CAP` drop). Cursors are monotonic,
/// so a subsequent successful pop subsumes any lost value — but only
/// if the trackers haven't been advanced past it.
#[inline]
pub fn try_flush_dual_track(
    pending_acks: &mut PendingAckQueue,
    journal_cursor: &Sequence,
    accum_end_sequence: u64,
    last_sent_acked: u64,
    last_sent_in_memory: u64,
) -> Option<Ack> {
    let acked_now = pending_acks
        .pop_ready(journal_cursor)
        .unwrap_or(last_sent_acked);
    debug_assert!(
        acked_now >= last_sent_acked,
        "acked_sequence regression: {last_sent_acked} -> {acked_now} \
         (queue popped a value below last-sent; namespace-translation bug?)",
    );
    let in_mem_now = accum_end_sequence;
    if acked_now > last_sent_acked || in_mem_now > last_sent_in_memory {
        Some(Ack {
            acked_sequence: acked_now,
            in_memory_sequence: in_mem_now,
        })
    } else {
        None
    }
}
