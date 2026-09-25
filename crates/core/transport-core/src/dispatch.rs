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
//! The sequence is stateless: what reaches the application depends on the
//! entry alone, never on what the consumer saw before it. That is what
//! makes a consumer that started mid-stream (a node restored from a
//! snapshot, a replica whose matching stage started after replay) call the
//! application exactly as one that saw every entry. It rests on journaled
//! time being strictly increasing (see [`melin_app::SequencerTime`]): every
//! entry is its own instant, so the clock step before each needs no memory
//! of the last.
//!
//! A query never comes here. It is not journaled, so nothing it did could
//! be replayed: the matching stage answers it through
//! [`Application::query`], which cannot change state, and the shadow stage
//! skips it. Replay never sees one.

use melin_app::{Application, ApplyCtx};
use melin_journal::JournalEvent;

/// Hand one journaled event to the application.
///
/// `ctx.now` must be the event's timestamp and `ctx.key_hash` its client's
/// identity, both journaled with it: the application may derive state
/// from either.
///
/// Every event reaches the application: the runtime refuses nothing on
/// its behalf. An application that refuses repeats does so in `apply`,
/// keyed on `ctx.key_hash`, and reaches the same verdict on every path
/// because every path hands it the same event under the same key.
///
/// In order:
/// 1. **Clock.** [`Application::tick`] at the entry's time, for every
///    journaled entry, so due work fires at the first entry past its
///    deadline. The pipeline's `Shutdown` sentinel is not an entry: it
///    returns before this step, and changes nothing.
/// 2. **The event itself.** An application event goes to `apply`. A
///    journaled `Tick` has nothing more to do: the clock step was its
///    whole purpose, so `tick` runs exactly once for it. `EpochBump` is
///    lineage metadata, not application state, so it goes to `on_epoch`:
///    the fencing state on the live stage (a replica following the
///    stream, or a new primary's own promotion injection), a tracked
///    epoch on replay and in the shadow.
///
/// Reports are appended to `reports`, which the caller clears.
#[inline]
pub(crate) fn dispatch<A: Application>(
    app: &mut A,
    event: JournalEvent<A::Event>,
    ctx: &ApplyCtx,
    on_epoch: impl FnOnce(u64),
    reports: &mut Vec<A::Report>,
) {
    debug_assert!(
        !event.is_query(),
        "a query is answered by Application::query, never dispatched"
    );
    // Pipeline sentinel: the live stage's shutdown drain can hand it
    // here, and it is never journaled, so it has no time to tick to.
    if event.is_shutdown() {
        return;
    }

    app.tick(ctx.now, reports);

    match event {
        JournalEvent::App(event) => app.apply(event, ctx, reports),
        JournalEvent::EpochBump { epoch } => on_epoch(epoch),
        // A tick's whole purpose was the clock step above; the sentinel
        // returned before it.
        JournalEvent::Tick | JournalEvent::Shutdown => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestEvent};
    use melin_app::SequencerTime;
    use std::collections::HashMap;

    const KEY: u64 = 0xDEAD_BEEF;

    fn ctx(now_ns: u64, key_hash: u64) -> ApplyCtx {
        ApplyCtx {
            now: SequencerTime::from_ns(now_ns),
            key_hash,
        }
    }

    /// Dispatch one event with a discarded epoch.
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
            |_| {},
            &mut Vec::new(),
        );
    }

    #[test]
    fn app_event_reaches_apply_under_its_key() {
        let mut app = TestApp::new();
        dispatch_once(&mut app, JournalEvent::App(TestEvent::Add(42)), 1, KEY);

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
        for stamp in [1, 2] {
            dispatch_once(&mut app, JournalEvent::App(TestEvent::Add(5)), stamp, KEY);
        }

        assert_eq!(app.total, 10, "the runtime must not filter a repeat");
        assert_eq!(app.per_key_total, HashMap::from([(KEY, 10)]));
    }

    #[test]
    fn key_hash_zero_reaches_apply_without_per_key_state() {
        // Internal events (Tick, seed inserts) carry key_hash 0, which the
        // application treats as no client at all.
        let mut app = TestApp::new();
        for stamp in [1, 2, 3] {
            dispatch_once(&mut app, JournalEvent::App(TestEvent::Add(7)), stamp, 0);
        }
        assert_eq!(app.total, 21, "every internal event must apply");
        assert!(
            app.per_key_total.is_empty(),
            "key_hash 0 must not allocate a per-key entry"
        );
    }

    /// Every journaled entry ticks the clock once, at its own time, before
    /// anything else: with no watermark, what reaches the application
    /// depends on the entry alone, so a consumer that starts mid-stream
    /// calls it exactly as one that saw everything.
    #[test]
    fn every_entry_ticks_once_at_its_own_time() {
        let mut app = TestApp::new();
        let entries = [
            JournalEvent::App(TestEvent::Add(1)),
            JournalEvent::Tick,
            JournalEvent::EpochBump { epoch: 2 },
            JournalEvent::App(TestEvent::Add(1)),
        ];
        for (i, event) in entries.into_iter().enumerate() {
            dispatch_once(&mut app, event, 100 + i as u64, KEY);
            assert_eq!(app.ticks, i as u64 + 1, "one tick per entry");
        }
    }

    #[test]
    fn tick_reaches_tick_once_and_not_apply() {
        let mut app = TestApp::new();
        dispatch_once(&mut app, JournalEvent::Tick, 1_000, 0);

        assert_eq!(app.total, 0, "Tick must not call apply");
        assert_eq!(
            app.ticks, 1,
            "Tick must call Application::tick exactly once"
        );
    }

    #[test]
    fn epoch_bump_goes_to_the_epoch_sink_and_ticks_the_clock() {
        let mut app = TestApp::new();
        let mut observed = None;
        dispatch(
            &mut app,
            JournalEvent::EpochBump { epoch: 7 },
            &ctx(1, 0),
            |epoch| observed = Some(epoch),
            &mut Vec::new(),
        );

        assert_eq!(observed, Some(7));
        assert_eq!(app.total, 0, "an epoch bump must not reach apply");
        assert_eq!(app.ticks, 1, "an epoch bump is an entry: the clock ticks");
    }

    /// The shutdown sentinel is not an entry: it has no journaled time, so
    /// it must not reach the clock (the live stage's shutdown drain hands
    /// it here with a zero time).
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
