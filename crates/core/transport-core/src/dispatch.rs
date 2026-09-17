//! The one path by which an input event reaches the application.
//!
//! Three consumers rebuild application state from the same input stream:
//! the live matching stage, journal replay on recovery, and the shadow
//! stage whose snapshots recovery restores. They must land on identical
//! state — a replayed node where the primary was, a restored snapshot where
//! the primary was when it was taken. Separate copies of this sequence drift
//! apart unnoticed — a duplicate refused live but applied in a snapshot, a
//! clock advanced on replay but not live — so all three call [`dispatch`],
//! and there is one sequence to get right.

use melin_app::{Application, ApplyCtx, WireSeq};
use melin_journal::JournalEvent;

/// What became of a dispatched event.
///
/// `#[must_use]` because only the live matching stage replies to a
/// refusal; every other caller must say that it is dropping it.
#[must_use]
pub(crate) enum Dispatched<Q> {
    /// Refused as a duplicate request: not applied, and the scheduler
    /// clock did not advance for it.
    Refused,
    /// Reached the application, carrying the response when the event was
    /// a query.
    Applied(Option<Q>),
}

/// Hand one input event to the application.
///
/// `ctx.now_ns` must be the event's timestamp and `ctx.key_hash` its
/// client's identity; the other `ctx` fields are advisory counters that
/// only the live stage can fill (see [`offline_ctx`]).
///
/// In order:
/// 1. **Duplicate check.** The journal stage records an event before the
///    matching stage decides on it, so the input stream — and the journal —
///    holds duplicates the primary refused. They are refused here on every
///    path, before anything else, or replay and snapshots would hold the
///    effect of a request whose client was told it was rejected. Queries
///    bypass the check: they change no state and are never journaled, so
///    counting them would advance the per-key high-water mark on the live
///    and shadow paths but not on replay.
/// 2. **Clock.** A timestamp newer than `last_drain_ns` fires the
///    application's due scheduled work, so under load time advances at
///    every-event resolution rather than only on `Tick`. The strict
///    greater-than tolerates the rare producer race that publishes a slot
///    with an earlier timestamp than its predecessor. Queries carry a zero
///    timestamp and so never move the clock.
/// 3. **The event itself.** `EpochBump` is lineage metadata, not
///    application state, so it goes to `on_epoch` — the fencing state on
///    the live stage (a replica following the stream, or a new primary's
///    own promotion injection), a tracked epoch on replay and in the shadow.
///
/// Reports are appended to `reports`, which the caller clears.
#[inline]
pub(crate) fn dispatch<A: Application>(
    app: &mut A,
    event: JournalEvent<A::Event>,
    request_seq: u64,
    ctx: &ApplyCtx,
    last_drain_ns: &mut u64,
    on_epoch: impl FnOnce(u64),
    reports: &mut Vec<A::Report>,
) -> Dispatched<A::QueryResponse> {
    if !event.is_query() && !app.check_request_seq(ctx.key_hash, request_seq) {
        return Dispatched::Refused;
    }

    if ctx.now_ns > *last_drain_ns {
        *last_drain_ns = ctx.now_ns;
        app.tick(ctx.now_ns, reports);
    }

    match event {
        JournalEvent::App(event) => return Dispatched::Applied(app.apply(event, ctx, reports)),
        JournalEvent::Tick { now_ns } => {
            // Usually a no-op: the clock step above has already advanced
            // to the slot timestamp, which equals `now_ns` for a tick the
            // generator published. Kept so time still advances when the
            // timestamp is zero (hand-built ticks in tests).
            app.tick(now_ns, reports);
        }
        JournalEvent::EpochBump { epoch } => on_epoch(epoch),
        JournalEvent::Shutdown => {
            // Pipeline sentinel: the live stage exits on it before
            // dispatching, and it is never written to disk.
        }
    }
    Dispatched::Applied(None)
}

/// An [`ApplyCtx`] for an event rebuilt away from the live stage — replay
/// and the shadow. The journal sequence, connection count and events
/// processed are live-pipeline counters with no meaning there, so they
/// are zero; the timestamp and key are the event's own, which is all that
/// state may depend on.
pub(crate) fn offline_ctx(timestamp_ns: u64, key_hash: u64) -> ApplyCtx {
    ApplyCtx {
        now_ns: timestamp_ns,
        journal_sequence: WireSeq::new(0),
        active_connections: 0,
        events_processed: 0,
        key_hash,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestEvent};

    const KEY: u64 = 0xDEAD_BEEF;

    /// Dispatch one event with a fresh clock and a discarded epoch.
    fn dispatch_once(
        app: &mut TestApp,
        event: JournalEvent<TestEvent>,
        timestamp_ns: u64,
        key_hash: u64,
        request_seq: u64,
    ) -> Dispatched<crate::test_support::TestQuery> {
        dispatch(
            app,
            event,
            request_seq,
            &offline_ctx(timestamp_ns, key_hash),
            &mut 0,
            |_| {},
            &mut Vec::new(),
        )
    }

    #[test]
    fn app_event_advances_hwm_and_reaches_apply() {
        let mut app = TestApp::new();
        let outcome = dispatch_once(&mut app, JournalEvent::App(TestEvent::Add(42)), 0, KEY, 10);

        assert!(matches!(outcome, Dispatched::Applied(None)));
        assert_eq!(app.total, 42, "apply must have run");
        assert_eq!(app.key_hwm.get(&KEY).copied(), Some(10));
    }

    #[test]
    fn query_bypasses_the_duplicate_check_and_returns_its_response() {
        // The live and shadow paths see queries; replay never does. A
        // query advancing the HWM would put those two above replay, and a
        // restored node would refuse a legitimate request.
        let mut app = TestApp::new();
        let outcome = dispatch_once(&mut app, JournalEvent::App(TestEvent::Query), 0, KEY, 100);

        assert!(matches!(outcome, Dispatched::Applied(Some(_))));
        assert!(app.key_hwm.is_empty(), "a query must not advance the HWM");
        assert!(app.check_request_seq(KEY, 100));
    }

    #[test]
    fn duplicate_is_refused_without_apply_or_clock() {
        let mut app = TestApp::new();
        let mut drain = 0;
        let mut reports = Vec::new();
        let mut send = |app: &mut TestApp, timestamp_ns| {
            dispatch(
                app,
                JournalEvent::App(TestEvent::Add(5)),
                10,
                &offline_ctx(timestamp_ns, KEY),
                &mut drain,
                |_| {},
                &mut reports,
            )
        };

        assert!(matches!(send(&mut app, 1_000), Dispatched::Applied(None)));
        assert_eq!((app.total, app.ticks), (5, 1));

        assert!(matches!(send(&mut app, 2_000), Dispatched::Refused));
        assert_eq!(app.total, 5, "a duplicate must not be applied");
        assert_eq!(app.ticks, 1, "a duplicate must not advance the clock");
        assert_eq!(
            app.key_hwm.get(&KEY).copied(),
            Some(10),
            "HWM must not regress"
        );
    }

    #[test]
    fn key_hash_zero_bypasses_the_duplicate_check() {
        // Internal events (Tick, seed inserts) carry key_hash 0, which the
        // application exempts: the same request_seq applies every time.
        let mut app = TestApp::new();
        for _ in 0..3 {
            let outcome = dispatch_once(&mut app, JournalEvent::App(TestEvent::Add(7)), 0, 0, 1);
            assert!(matches!(outcome, Dispatched::Applied(None)));
        }
        assert_eq!(app.total, 21, "every internal event must apply");
        assert!(
            app.key_hwm.is_empty(),
            "key_hash 0 must not allocate an HWM entry"
        );
    }

    #[test]
    fn timestamp_drives_a_monotonic_clock() {
        let mut app = TestApp::new();
        let mut drain = 0;
        let mut reports = Vec::new();
        // (timestamp, request_seq, ticks after, drain after)
        let steps = [
            (100, 1, 1, 100),
            (50, 2, 1, 100),
            (100, 3, 1, 100),
            (200, 4, 2, 200),
        ];
        for (timestamp_ns, request_seq, ticks, drained) in steps {
            let outcome = dispatch(
                &mut app,
                JournalEvent::App(TestEvent::Add(1)),
                request_seq,
                &offline_ctx(timestamp_ns, KEY),
                &mut drain,
                |_| {},
                &mut reports,
            );
            assert!(matches!(outcome, Dispatched::Applied(None)));
            assert_eq!(app.ticks, ticks, "ticks after the event at {timestamp_ns}");
            assert_eq!(drain, drained, "clock after the event at {timestamp_ns}");
        }
    }

    #[test]
    fn tick_reaches_tick_and_not_apply() {
        let mut app = TestApp::new();
        let outcome = dispatch_once(&mut app, JournalEvent::Tick { now_ns: 1_000 }, 0, 0, 0);

        assert!(matches!(outcome, Dispatched::Applied(None)));
        assert_eq!(app.total, 0, "Tick must not call apply");
        assert_eq!(app.ticks, 1, "Tick must call Application::tick");
    }

    #[test]
    fn epoch_bump_goes_to_the_epoch_sink_and_not_the_application() {
        let mut app = TestApp::new();
        let mut observed = None;
        let outcome = dispatch(
            &mut app,
            JournalEvent::EpochBump { epoch: 7 },
            0,
            &offline_ctx(0, 0),
            &mut 0,
            |epoch| observed = Some(epoch),
            &mut Vec::new(),
        );

        assert!(matches!(outcome, Dispatched::Applied(None)));
        assert_eq!(observed, Some(7));
        assert_eq!(
            app,
            TestApp::new(),
            "an epoch bump must not touch application state"
        );
    }

    #[test]
    fn shutdown_is_a_state_noop() {
        let mut app = TestApp::new();
        let outcome = dispatch_once(&mut app, JournalEvent::Shutdown, 0, 0, 3);

        assert!(matches!(outcome, Dispatched::Applied(None)));
        assert_eq!(
            app,
            TestApp::new(),
            "Shutdown must not touch application state"
        );
    }
}
