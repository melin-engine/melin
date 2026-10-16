//! The one path by which a journaled event reaches the application.
//!
//! Three consumers rebuild application state from the same input stream:
//! the live matching stage, journal replay on recovery, and the shadow
//! stage whose snapshots recovery restores. They must land on identical
//! state — a replayed node where the primary was, a restored snapshot where
//! the primary was when it was taken. Separate copies of this sequence drift
//! apart unnoticed — a clock advanced on replay but not live, a key handed
//! to `apply` on one path and not another — so all three call [`dispatch`],
//! and there is one sequence to get right.
//!
//! A query never comes here. It is not journaled, so nothing it did could
//! be replayed: the matching stage answers it through
//! [`Application::query`], which cannot change state, and the shadow stage
//! skips it. Replay never sees one.

use melin_app::{Application, ApplyCtx};
use melin_journal::{AckPolicyChange, JournalEvent};

/// Lineage metadata a journaled event carries instead of application
/// state: what [`dispatch`] hands its lineage sink.
///
/// One sink for every kind, rather than a callback per kind, so each
/// consumer matches this enum and a new kind of lineage metadata fails
/// to compile until every consumer has decided what it means there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineageEvent {
    /// A fencing epoch bump ([`JournalEvent::EpochBump`]).
    Epoch(u64),
    /// An ack-policy change ([`JournalEvent::AckPolicyChange`]).
    AckPolicy(AckPolicyChange),
}

/// Hand one journaled event to the application.
///
/// `ctx.now_ns` must be the event's timestamp and `ctx.key_hash` its
/// client's identity, both journaled with it — the application may derive
/// state from either.
///
/// Every event reaches the application: the runtime refuses nothing on
/// its behalf. An application that refuses repeats does so in `apply`,
/// keyed on `ctx.key_hash`, and reaches the same verdict on every path
/// because every path hands it the same event under the same key.
///
/// In order:
/// 1. **Clock.** A timestamp newer than `last_drain_ns` fires the
///    application's due scheduled work, so under load time advances at
///    every-event resolution rather than only on `Tick`. The strict
///    greater-than tolerates the rare producer race that publishes a slot
///    with an earlier timestamp than its predecessor.
/// 2. **The event itself.** `EpochBump` and `AckPolicyChange` are lineage
///    metadata, not application state, so they go to `on_lineage` as a
///    [`LineageEvent`] — on the live stage the fencing state (a replica
///    following the stream, or a new primary's own promotion injection),
///    on replay and in the shadow the lineage they track.
///
/// Reports are appended to `reports`, which the caller clears.
#[inline]
pub(crate) fn dispatch<A: Application>(
    app: &mut A,
    event: JournalEvent<A::Event>,
    ctx: &ApplyCtx,
    last_drain_ns: &mut u64,
    on_lineage: impl FnOnce(LineageEvent),
    reports: &mut Vec<A::Report>,
) {
    debug_assert!(
        !event.is_query(),
        "a query is answered by Application::query, never dispatched"
    );

    if ctx.now_ns > *last_drain_ns {
        *last_drain_ns = ctx.now_ns;
        app.tick(ctx.now_ns, reports);
    }

    match event {
        JournalEvent::App(event) => app.apply(event, ctx, reports),
        JournalEvent::Tick { now_ns } => {
            // Usually a no-op: the clock step above has already advanced
            // to the slot timestamp, which equals `now_ns` for a tick the
            // generator published. Kept so time still advances when the
            // timestamp is zero (hand-built ticks in tests).
            app.tick(now_ns, reports);
        }
        JournalEvent::EpochBump { epoch } => on_lineage(LineageEvent::Epoch(epoch)),
        JournalEvent::AckPolicyChange(change) => on_lineage(LineageEvent::AckPolicy(change)),
        JournalEvent::Shutdown => {
            // Pipeline sentinel: the live stage exits on it before
            // dispatching, and it is never written to disk.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestEvent, ack_policy_change};
    use std::collections::HashMap;

    const KEY: u64 = 0xDEAD_BEEF;

    fn ctx(now_ns: u64, key_hash: u64) -> ApplyCtx {
        ApplyCtx { now_ns, key_hash }
    }

    /// Dispatch one event with a fresh clock and a discarded epoch.
    fn dispatch_once(
        app: &mut TestApp,
        event: JournalEvent<TestEvent>,
        timestamp_ns: u64,
        key_hash: u64,
    ) {
        dispatch(
            app,
            event,
            &ctx(timestamp_ns, key_hash),
            &mut 0,
            |_| {},
            &mut Vec::new(),
        );
    }

    #[test]
    fn app_event_reaches_apply_under_its_key() {
        let mut app = TestApp::new();
        dispatch_once(&mut app, JournalEvent::App(TestEvent::Add(42)), 0, KEY);

        assert_eq!(app.total, 42, "apply must have run");
        assert_eq!(
            app.per_key_total,
            HashMap::from([(KEY, 42)]),
            "apply must see the submitting key"
        );
    }

    /// A repeated submission reaches `apply` every time, on every path.
    /// The application decides what a repeat means, and it decides it
    /// the same way live, on replay and in the shadow, so a snapshot
    /// holds the state the primary holds.
    #[test]
    fn repeated_submission_reaches_apply_every_time() {
        let mut app = TestApp::new();
        for _ in 0..2 {
            dispatch_once(&mut app, JournalEvent::App(TestEvent::Add(5)), 0, KEY);
        }

        assert_eq!(app.total, 10, "the runtime must not filter a repeat");
        assert_eq!(app.per_key_total, HashMap::from([(KEY, 10)]));
    }

    #[test]
    fn key_hash_zero_reaches_apply_without_per_key_state() {
        // Internal events (Tick, seed inserts) carry key_hash 0, which the
        // application treats as no client at all.
        let mut app = TestApp::new();
        for _ in 0..3 {
            dispatch_once(&mut app, JournalEvent::App(TestEvent::Add(7)), 0, 0);
        }
        assert_eq!(app.total, 21, "every internal event must apply");
        assert!(
            app.per_key_total.is_empty(),
            "key_hash 0 must not allocate a per-key entry"
        );
    }

    #[test]
    fn timestamp_drives_a_monotonic_clock() {
        let mut app = TestApp::new();
        let mut drain = 0;
        let mut reports = Vec::new();
        // (timestamp, ticks after, drain after)
        let steps = [(100, 1, 100), (50, 1, 100), (100, 1, 100), (200, 2, 200)];
        for (timestamp_ns, ticks, drained) in steps {
            dispatch(
                &mut app,
                JournalEvent::App(TestEvent::Add(1)),
                &ctx(timestamp_ns, KEY),
                &mut drain,
                |_| {},
                &mut reports,
            );
            assert_eq!(app.ticks, ticks, "ticks after the event at {timestamp_ns}");
            assert_eq!(drain, drained, "clock after the event at {timestamp_ns}");
        }
    }

    #[test]
    fn tick_reaches_tick_and_not_apply() {
        let mut app = TestApp::new();
        dispatch_once(&mut app, JournalEvent::Tick { now_ns: 1_000 }, 0, 0);

        assert_eq!(app.total, 0, "Tick must not call apply");
        assert_eq!(app.ticks, 1, "Tick must call Application::tick");
    }

    /// Dispatch one event with a fresh clock, and return what reached the
    /// lineage sink.
    fn lineage_of(app: &mut TestApp, event: JournalEvent<TestEvent>) -> Option<LineageEvent> {
        let mut observed = None;
        dispatch(
            app,
            event,
            &ctx(0, 0),
            &mut 0,
            |lineage| observed = Some(lineage),
            &mut Vec::new(),
        );
        observed
    }

    /// Lineage metadata, and only it, reaches the lineage sink — and not
    /// the application, whose state it leaves untouched.
    #[test]
    fn only_lineage_metadata_reaches_the_lineage_sink() {
        let policy_change = ack_policy_change(1, true);
        let JournalEvent::AckPolicyChange(change) = policy_change else {
            unreachable!("the helper builds an ack-policy change")
        };
        for (event, lineage) in [
            (
                JournalEvent::EpochBump { epoch: 7 },
                Some(LineageEvent::Epoch(7)),
            ),
            (policy_change, Some(LineageEvent::AckPolicy(change))),
            (JournalEvent::App(TestEvent::Add(1)), None),
            (JournalEvent::Tick { now_ns: 5 }, None),
            (JournalEvent::Shutdown, None),
        ] {
            let mut app = TestApp::new();
            assert_eq!(lineage_of(&mut app, event), lineage, "{event:?}");
            if lineage.is_some() {
                assert_eq!(app, TestApp::new(), "{event:?} touched application state");
            }
        }
    }

    #[test]
    fn shutdown_is_a_state_noop() {
        let mut app = TestApp::new();
        dispatch_once(&mut app, JournalEvent::Shutdown, 0, 0);

        assert_eq!(
            app,
            TestApp::new(),
            "Shutdown must not touch application state"
        );
    }
}
