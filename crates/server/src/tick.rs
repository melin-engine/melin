//! Tick-generation helpers shared by the io_uring reader and the DPDK
//! poll thread. Both transports embed the engine's tick generator into
//! their existing ingress loop instead of running it as a separate
//! thread, which keeps the input ring single-producer in steady state
//! and removes one source of multi-producer ordering races.
//!
//! ## Monotonic clamp
//!
//! `SystemTime::now()` can step backwards (NTP, manual clock skew). The
//! engine's scheduler keys due-task firings on `now_ns >= fire_ns`, so a
//! backwards step followed by a forward step could re-fire tasks that
//! were already drained on replay. Clamping each tick's `now_ns` to
//! `max(prev + 1, raw_now_ns)` keeps the journaled stream strictly
//! monotonic, so live and replay produce byte-identical state.
//!
//! ## What this module is *not*
//!
//! There is no longer a standalone tick thread. The matching stage
//! also advances its scheduler clock from `slot.timestamp_ns` on every
//! event (see `journal::pipeline::MatchingStage::process_event`), so the
//! tick is the safety net that keeps time moving forward during quiet
//! periods rather than the sole source of clock progress.

use crate::InputSlot;
use crate::JournalEvent;
use melin_disruptor::ring;
use melin_transport_core::trace::mono_trace_ns;

/// Strict-monotonic clamp on the wall-clock timestamp emitted by each tick.
/// `last_now_ns == 0` is the initial-state sentinel — the first tick is
/// stamped with `raw_now_ns` (or `1` if even the wall clock returns 0,
/// which only happens on a pre-epoch system clock).
pub(crate) fn clamp_monotonic(raw_now_ns: u64, last_now_ns: u64) -> u64 {
    if last_now_ns == 0 {
        raw_now_ns.max(1)
    } else if raw_now_ns > last_now_ns {
        raw_now_ns
    } else {
        last_now_ns + 1
    }
}

/// Publish a `JournalEvent::Tick { now_ns }` onto the input ring.
///
/// Internal/server-originated: no client connection, no auth key.
/// `key_hash = 0` is exempt from idempotency dedup in the engine.
///
/// `sequence: 0` because the journal stage is the authoritative sequence
/// allocator on the primary — see `InputSlot::sequence`.
///
/// On a full ring the publish drops; the next successful tick still
/// carries the latest wall-clock time, so a missed tick only delays
/// scheduler firings by one cadence at worst.
pub(crate) fn publish_tick(producer: &mut ring::Producer<InputSlot>, now_ns: u64) {
    // try_publish drop is intentional: on a full ring we'd rather skip a
    // tick than block the ingress thread (see fn doc).
    let _ = producer.try_publish(InputSlot {
        connection_id: 0,
        key_hash: 0,
        request_seq: 0,
        sequence: 0,
        timestamp_ns: now_ns,
        event: JournalEvent::Tick { now_ns },
        publish_ts: mono_trace_ns(),
        recv_ts: mono_trace_ns(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_tick_uses_raw_when_nonzero() {
        assert_eq!(clamp_monotonic(1_000, 0), 1_000);
    }

    #[test]
    fn first_tick_clamps_zero_to_one() {
        // `unix_epoch_nanos` returns 0 only on a pre-epoch clock — bump
        // to 1 so the journal's per-tick monotonic invariant holds even
        // in that pathological case.
        assert_eq!(clamp_monotonic(0, 0), 1);
    }

    #[test]
    fn forward_clock_passes_through() {
        assert_eq!(clamp_monotonic(2_000, 1_000), 2_000);
    }

    #[test]
    fn backward_clock_clamped_to_prev_plus_one() {
        assert_eq!(clamp_monotonic(500, 1_000), 1_001);
    }

    #[test]
    fn equal_clock_clamped_to_prev_plus_one() {
        assert_eq!(clamp_monotonic(1_000, 1_000), 1_001);
    }
}
