//! Per-stage latency tracing for the disruptor pipeline.
//!
//! Behind the `latency-trace` feature gate. When disabled, `MonoTraceInstant`
//! is `()` (zero-sized) and all tracing helpers are no-ops — zero overhead.
//!
//! ## Stats registry
//!
//! Stages register their per-stage histograms with a process-global
//! `StatsRegistry` (single server per process). Each registered stage
//! is backed by a `hdrhistogram::sync::SyncHistogram`: every recording
//! thread holds its own `hdrhistogram::sync::Recorder` (a per-thread
//! lock-free local buffer), and the health endpoint snapshots all of
//! them via `global_registry().snapshot_all()` for the bench's
//! tick-to-trade dump.
//!
//! Why SyncHistogram (vs `Mutex<Histogram>`): under saturation the
//! mutex variant cost ~50 % of throughput when `tick-to-trade` was on
//! (5.6 M ops/s → 2.5 M). SyncHistogram's record path is wait-free
//! against other recorders — the only synchronization is a per-record
//! atomic load of the phase counter (one atomic per record at steady
//! state, zero contention with other writers). Reads pay a phase-shift
//! cost on `refresh`, but reads happen once per `/stats-dump` request.
//!
//! Production builds collapse the entire path to ZSTs and inlined
//! no-ops, so this is dev/bench only.
//!
//! ## Recorder ownership
//!
//! `StageRecorder` owns a `Recorder` (not shared via Arc). Each call
//! to `register_stage(name)` returns a fresh `Recorder` clone that
//! feeds the same `SyncHistogram`; multiple threads recording for the
//! same stage simply each hold their own recorder. The API takes
//! `&mut self` on `record_ns` because `Recorder::record` does — the
//! local buffer is mutated without synchronization.
//!
//! ## Why quiet threads must flush
//!
//! A `Recorder` hands its buffered samples to the `SyncHistogram` only
//! on its *next* `record` call after the reader starts a phase shift.
//! A thread that stops recording — the response stage once the bench
//! disconnects, the reader parked in `submit_and_wait` — never reaches
//! that call, so `refresh` waits for an acknowledgement that never
//! arrives, times out, and the whole run's samples stay stranded in the
//! thread-local buffer. `/stats-dump` is normally fetched right after
//! the workload ends, which is exactly when that happens.
//!
//! [`StageRecorder::flush`] forces the handover. Every stage thread
//! calls it from its idle path on a coarse timer, so a scrape taken
//! after traffic stops still sees the run's samples.

/// Monotonic timestamp carried through pipeline slots.
///
/// Backed by `Instant::now()` — never goes backwards, ignores NTP. Used
/// only for stage-to-stage latency measurement; never persisted, never
/// compared across processes. For wall-clock timestamps stamped into
/// journal records, see [`melin_app::unix_epoch_nanos`].
///
/// `u64` nanoseconds when tracing is enabled, `()` (ZST, optimized away)
/// when disabled. This avoids `#[cfg]` on struct fields while adding
/// zero bytes to slot layouts in production builds.
#[cfg(feature = "latency-trace")]
pub type MonoTraceInstant = u64;

#[cfg(not(feature = "latency-trace"))]
pub type MonoTraceInstant = ();

/// Capture a trace timestamp. Returns `()` when tracing is disabled.
#[cfg(feature = "latency-trace")]
#[inline]
pub fn mono_trace_ns() -> MonoTraceInstant {
    mono_nanos()
}

#[cfg(not(feature = "latency-trace"))]
#[inline]
pub fn mono_trace_ns() -> MonoTraceInstant {}

/// Monotonic nanoseconds since process start. Uses a static epoch to
/// avoid overflow and keep values small.
#[cfg(feature = "latency-trace")]
fn mono_nanos() -> u64 {
    use std::sync::OnceLock;
    use std::time::Instant;

    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    epoch.elapsed().as_nanos() as u64
}

/// Elapsed nanoseconds between two trace timestamps.
#[cfg(feature = "latency-trace")]
#[inline]
pub fn mono_trace_elapsed_ns(start: MonoTraceInstant, end: MonoTraceInstant) -> u64 {
    end.saturating_sub(start)
}

// ---------------------------------------------------------------------------
// StageRecorder + StatsRegistry
// ---------------------------------------------------------------------------

/// Snapshot of a stage's histogram percentiles. Returned by
/// `StatsRegistry::snapshot_all` — the stable structure the health
/// endpoint serializes to wire format.
#[cfg(feature = "latency-trace")]
#[derive(Debug, Clone)]
pub struct StageSnapshot {
    pub name: &'static str,
    pub samples: u64,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p99_9_ns: u64,
    pub max_ns: u64,
}

/// How often a stage thread should call [`StageRecorder::flush`] from
/// its idle path.
///
/// Short enough that a `/stats-dump` fetched right after the workload
/// ends sees the run's samples, long enough that the flush cost (a
/// mutex plus a histogram allocation per recorder) is irrelevant even
/// on a thread that is idle continuously.
#[cfg(feature = "latency-trace")]
pub const IDLE_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Total time [`StatsRegistry::snapshot_all`] will spend waiting for
/// recorders to acknowledge a phase shift, across all stages.
///
/// Comfortably above [`IDLE_FLUSH_INTERVAL`] so a thread that went
/// quiet just before the scrape gets a chance to flush, and low enough
/// that a `/stats-dump` stays responsive when every thread is idle.
#[cfg(feature = "latency-trace")]
const REFRESH_BUDGET: std::time::Duration = std::time::Duration::from_millis(500);

/// A handle for recording samples into a registered stage histogram.
///
/// Owns a per-thread `Recorder` (no Arc, no Mutex on the record path).
/// Each `record_ns` call writes to the recorder's local buffer; samples
/// are merged into the underlying `SyncHistogram` lazily on the next
/// `refresh` call from the reader (the health endpoint).
///
/// `record_ns` takes `&mut self` because the underlying
/// [`hdrhistogram::sync::Recorder`] mutates its local buffer. Stage
/// threads therefore declare `let mut rec = register_stage(...)`.
#[cfg(feature = "latency-trace")]
pub struct StageRecorder {
    rec: hdrhistogram::sync::Recorder<u64>,
    /// The registry entry this recorder feeds, so [`Self::flush`] can
    /// mint a replacement `Recorder` without a by-name registry lookup
    /// (which would take the registry-wide lock, not just this stage's).
    ///
    /// `Arc` rather than a borrow because a stage thread typically holds
    /// its recorder for the process lifetime; the registry stores the
    /// same entries behind `Arc` already, so this adds a refcount, not
    /// an allocation.
    entry: std::sync::Arc<StageEntry>,
}

#[cfg(feature = "latency-trace")]
impl Clone for StageRecorder {
    fn clone(&self) -> Self {
        Self {
            rec: self.rec.clone(),
            entry: std::sync::Arc::clone(&self.entry),
        }
    }
}

#[cfg(feature = "latency-trace")]
impl StageRecorder {
    /// Record a single sample in nanoseconds.
    ///
    /// Saturates instead of returning an error when `ns` exceeds the
    /// histogram's max bound — diagnostic samples are best-effort, and
    /// dropping a single very-out-of-range sample is preferable to
    /// crashing the trading thread.
    #[inline]
    pub fn record_ns(&mut self, ns: u64) {
        self.rec.saturating_record(ns);
    }

    /// Record the elapsed nanoseconds between two trace timestamps.
    #[inline]
    pub fn record_elapsed(&mut self, start: MonoTraceInstant, end: MonoTraceInstant) {
        self.record_ns(mono_trace_elapsed_ns(start, end));
    }

    /// Hand this recorder's buffered samples to the `SyncHistogram` so
    /// the next snapshot sees them.
    ///
    /// **Idle path only** — never call this per event. It takes the
    /// stage mutex and allocates a fresh thread-local histogram. Stage
    /// threads call it from their no-work branch on a ~100 ms timer;
    /// see the module docs for why a quiet thread otherwise loses its
    /// samples entirely.
    ///
    /// Replacing the inner `Recorder` is the flush: the old one's
    /// `Drop` ships its local histogram down the `SyncHistogram`'s
    /// channel unconditionally, which is the only unconditional
    /// handover hdrhistogram's API offers. (`Recorder::idle` sheds only
    /// when a phase shift is already pending.)
    pub fn flush(&mut self) {
        match self.entry.sync.try_lock() {
            Ok(sync) => self.rec = sync.recorder(),
            Err(std::sync::TryLockError::Poisoned(p)) => self.rec = p.into_inner().recorder(),
            Err(std::sync::TryLockError::WouldBlock) => {
                // Never wait: the contender that matters is
                // `snapshot_all`, which holds this mutex across the
                // whole refresh. Blocking would stall us for the refresh
                // budget and shed too late to be merged — the bug this
                // method exists to fix.
                //
                // `idle()` is the non-blocking substitute. Whether it
                // recovers anything depends on who we lost the race to:
                //
                // - `snapshot_all` past its phase bump — `deactivate`
                //   sees the shift and sheds into the very channel the
                //   refresh is waiting on, so the samples land in *this*
                //   snapshot rather than the next.
                // - `register`, a sibling recorder's `flush`, or
                //   `snapshot_all` before its phase bump — no shift is
                //   pending, so this sheds nothing and the samples roll
                //   over to the next flush one interval from now. Still
                //   correct, just one cycle later.
                //
                // Dropping the guard immediately rejoins the current
                // phase either way.
                drop(self.rec.idle());
            }
        }
    }
}

#[cfg(not(feature = "latency-trace"))]
#[derive(Clone, Copy, Default)]
pub struct StageRecorder;

#[cfg(not(feature = "latency-trace"))]
impl StageRecorder {
    #[inline]
    pub fn record_ns(&mut self, _ns: u64) {}

    #[inline]
    pub fn record_elapsed(&mut self, _start: MonoTraceInstant, _end: MonoTraceInstant) {}

    #[inline]
    pub fn flush(&mut self) {}
}

/// One stage's storage in the registry: a stable name + the
/// `SyncHistogram` that all `Recorder`s for this stage feed into.
///
/// The Mutex is held only during `refresh` + percentile reads from
/// the snapshot path (rare — once per `/stats-dump` call), never on
/// the record-side hot path.
#[cfg(feature = "latency-trace")]
struct StageEntry {
    name: &'static str,
    sync: std::sync::Mutex<hdrhistogram::sync::SyncHistogram<u64>>,
}

/// Process-wide registry of stage histograms.
///
/// One instance per process via `global_registry()`. Stages register
/// themselves at startup; the health endpoint dumps the registry on
/// demand for the bench's tick-to-trade decomposition.
#[cfg(feature = "latency-trace")]
pub struct StatsRegistry {
    // Vec, not HashMap: tens of entries at most, stable insertion
    // order in dumps, lookup-by-name only at register time. Mutex
    // protects the Vec only during register / snapshot iteration —
    // never on the per-event record path.
    entries: std::sync::Mutex<Vec<std::sync::Arc<StageEntry>>>,
}

#[cfg(feature = "latency-trace")]
impl StatsRegistry {
    fn new() -> Self {
        Self {
            entries: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Register a stage and return a `Recorder` for it. Idempotent —
    /// calling twice with the same name returns sibling recorders that
    /// feed the same underlying `SyncHistogram`.
    pub fn register(&self, name: &'static str) -> StageRecorder {
        let mut entries = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        for existing in entries.iter() {
            if existing.name == name {
                let rec = {
                    let sync = match existing.sync.lock() {
                        Ok(g) => g,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    sync.recorder()
                };
                return StageRecorder {
                    rec,
                    entry: std::sync::Arc::clone(existing),
                };
            }
        }
        // Range: 1 ns to 100 ms, 3 significant digits — same as the
        // pre-SyncHistogram design; matches the expected per-stage
        // percentile shape.
        let hist = hdrhistogram::Histogram::<u64>::new_with_bounds(1, 100_000_000, 3)
            .expect("valid histogram bounds");
        let sync: hdrhistogram::sync::SyncHistogram<u64> = hist.into();
        let recorder = sync.recorder();
        let entry = std::sync::Arc::new(StageEntry {
            name,
            sync: std::sync::Mutex::new(sync),
        });
        entries.push(std::sync::Arc::clone(&entry));
        StageRecorder {
            rec: recorder,
            entry,
        }
    }

    /// Snapshot every registered stage, including stages that hold no
    /// samples — those come back with `samples == 0` and zeroed
    /// percentiles.
    ///
    /// Zero-sample stages are reported rather than dropped so a stage
    /// that registered but produced nothing is distinguishable from one
    /// that was never compiled in. Silently omitting them made a
    /// missing stage look like a build-configuration problem.
    ///
    /// Refresh waits for each recorder to acknowledge the phase shift
    /// via its next `record` call. Stage threads flush explicitly from
    /// their idle paths (see [`StageRecorder::flush`]), so a recorder
    /// that has gone quiet has normally already handed its samples over
    /// by the time we get here — the wait is only a backstop for a
    /// thread that went quiet inside the flush interval.
    ///
    /// That wait is bounded by a single `REFRESH_BUDGET` (500 ms)
    /// shared across the whole snapshot, not per stage — so that is
    /// also this call's worst-case duration. A dormant recorder
    /// never acknowledges, so a per-stage timeout would multiply by the
    /// stage count — with a dozen-odd stages and every thread idle,
    /// which is exactly the state at end of run, a `/stats-dump` would
    /// block for seconds. Stages reached after the budget is spent
    /// still merge everything already in the channel (refresh drains it
    /// before waiting), so the flush is what keeps this lossless and
    /// the budget only bounds how long we hope for a straggler.
    ///
    /// A recorder still dormant past the budget has its pending samples
    /// rolled over into the next snapshot. Worst case the data is
    /// slightly stale; never wrong, never hung.
    pub fn snapshot_all(&self) -> Vec<StageSnapshot> {
        let entries = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let deadline = std::time::Instant::now() + REFRESH_BUDGET;
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            let mut sync = match entry.sync.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            // Pull pending samples from all recorders into the main
            // histogram. `saturating_duration_since` yields ZERO once
            // the budget is spent, which still performs the drain — see
            // the doc on `snapshot_all`.
            sync.refresh_timeout(deadline.saturating_duration_since(std::time::Instant::now()));
            if sync.is_empty() {
                // Percentile queries on an empty histogram are defined
                // but meaningless; report explicit zeros instead.
                out.push(StageSnapshot {
                    name: entry.name,
                    samples: 0,
                    min_ns: 0,
                    p50_ns: 0,
                    p90_ns: 0,
                    p99_ns: 0,
                    p99_9_ns: 0,
                    max_ns: 0,
                });
                continue;
            }
            out.push(StageSnapshot {
                name: entry.name,
                samples: sync.len(),
                min_ns: sync.min(),
                p50_ns: sync.value_at_quantile(0.50),
                p90_ns: sync.value_at_quantile(0.90),
                p99_ns: sync.value_at_quantile(0.99),
                p99_9_ns: sync.value_at_quantile(0.999),
                max_ns: sync.max(),
            });
        }
        out
    }

    /// Print every registered stage's percentile report to stderr.
    /// Called from the server's shutdown path so dev runs without the
    /// bench still see the per-stage breakdown — the bench fetches the
    /// same data via the health endpoint instead.
    pub fn print_report_all(&self) {
        use std::io::Write as _;

        for snap in self.snapshot_all() {
            let us = |ns: u64| ns as f64 / 1000.0;
            if snap.samples == 0 {
                // Printing the percentile block would show seven
                // `0.00 µs` rows that read as measurements rather than
                // as an absence of them.
                let buf = format!("  {}\n\x20   samples: 0 (never recorded)\n", snap.name);
                // Best-effort diagnostic output on shutdown.
                let _ = std::io::stderr().lock().write_all(buf.as_bytes());
                continue;
            }
            let buf = format!(
                "  {name}\n\
                 \x20   samples: {samples}\n\
                 \x20   min:    {min:>8.2} µs\n\
                 \x20   p50:    {p50:>8.2} µs\n\
                 \x20   p90:    {p90:>8.2} µs\n\
                 \x20   p99:    {p99:>8.2} µs\n\
                 \x20   p99.9:  {p999:>8.2} µs\n\
                 \x20   max:    {max:>8.2} µs\n",
                name = snap.name,
                samples = snap.samples,
                min = us(snap.min_ns),
                p50 = us(snap.p50_ns),
                p90 = us(snap.p90_ns),
                p99 = us(snap.p99_ns),
                p999 = us(snap.p99_9_ns),
                max = us(snap.max_ns),
            );
            // Best-effort diagnostic output on shutdown.
            let _ = std::io::stderr().lock().write_all(buf.as_bytes());
        }
    }
}

/// Process-shutdown hook. Prints all registered stage histograms via
/// `print_report_all` when `latency-trace` is enabled, no-op otherwise.
#[cfg(feature = "latency-trace")]
pub fn print_report_all() {
    global_registry().print_report_all();
}

#[cfg(not(feature = "latency-trace"))]
#[inline]
pub fn print_report_all() {}

#[cfg(feature = "latency-trace")]
static GLOBAL_REGISTRY: std::sync::OnceLock<StatsRegistry> = std::sync::OnceLock::new();

/// Process-global registry. Created on first access.
#[cfg(feature = "latency-trace")]
pub fn global_registry() -> &'static StatsRegistry {
    GLOBAL_REGISTRY.get_or_init(StatsRegistry::new)
}

/// Register a stage with the global registry and return a recorder.
///
/// Convenience for the common case `let mut h = register_stage("…");`.
/// Idempotent — calling twice with the same name returns sibling
/// recorders that feed the same underlying `SyncHistogram`.
#[cfg(feature = "latency-trace")]
pub fn register_stage(name: &'static str) -> StageRecorder {
    global_registry().register(name)
}

#[cfg(not(feature = "latency-trace"))]
#[inline]
pub fn register_stage(_name: &'static str) -> StageRecorder {
    StageRecorder
}

#[cfg(all(test, feature = "latency-trace"))]
mod tests {
    use super::*;

    // SyncHistogram caveat for tests: `refresh` waits for active
    // recorders to acknowledge the phase shift via their next
    // `record` call. A dormant recorder (one that recorded but
    // hasn't recorded since refresh started) holds up the refresh
    // until it times out, at which point its pending samples are
    // still in its local buffer — invisible to the snapshot.
    //
    // `StageRecorder::flush` is the fix for that; tests that keep a
    // recorder alive across a snapshot must call it first. Dropping
    // the recorder works too (the Drop impl ships pending samples via
    // the same channel), which is what the older tests below rely on.

    #[test]
    fn registry_register_returns_recorder_that_records() {
        let reg = StatsRegistry::new();
        {
            let mut rec = reg.register("test::stage_one");
            rec.record_ns(1_000);
            rec.record_ns(2_000);
            rec.record_ns(3_000);
            // `rec` dropped at end of scope → samples shipped via channel.
        }

        let snaps = reg.snapshot_all();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].name, "test::stage_one");
        assert_eq!(snaps[0].samples, 3);
        assert!(snaps[0].min_ns >= 1_000);
        assert!(snaps[0].max_ns >= 3_000);
    }

    #[test]
    fn registry_register_is_idempotent() {
        let reg = StatsRegistry::new();
        {
            let mut a = reg.register("test::dup");
            let mut b = reg.register("test::dup");
            a.record_ns(100);
            b.record_ns(200);
            // Both recorders dropped at end of scope.
        }
        let snaps = reg.snapshot_all();
        // Both recorders point at the same SyncHistogram.
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].samples, 2);
    }

    #[test]
    fn snapshot_reports_empty_stages_with_zero_samples() {
        // A registered-but-silent stage must stay visible: dropping it
        // makes a stage that recorded nothing indistinguishable from a
        // stage that was never compiled in.
        let reg = StatsRegistry::new();
        let _empty = reg.register("test::empty");
        {
            let mut used = reg.register("test::used");
            used.record_ns(500);
        }

        let snaps = reg.snapshot_all();
        assert_eq!(snaps.len(), 2);

        let empty = snaps
            .iter()
            .find(|s| s.name == "test::empty")
            .expect("empty stage must still be reported");
        assert_eq!(empty.samples, 0);
        assert_eq!(empty.min_ns, 0);
        assert_eq!(empty.p99_ns, 0);
        assert_eq!(empty.max_ns, 0);

        let used = snaps
            .iter()
            .find(|s| s.name == "test::used")
            .expect("used stage missing");
        assert_eq!(used.samples, 1);
    }

    #[test]
    fn flush_recovers_samples_from_a_dormant_recorder() {
        // The regression test for the stranded-sample bug: a thread
        // records, goes quiet without dropping its recorder, and a
        // snapshot is taken. Without the flush the stage is absent
        // from the dump entirely.
        use std::sync::Arc;
        use std::sync::mpsc;
        use std::thread;

        let reg = Arc::new(StatsRegistry::new());
        // Channels rather than a barrier: the worker must be *parked*,
        // not spinning, when the snapshot runs — spinning would let it
        // ack the phase shift and mask the bug.
        let (go_tx, go_rx) = mpsc::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel::<()>();

        let worker_reg = Arc::clone(&reg);
        let worker = thread::spawn(move || {
            let mut rec = worker_reg.register("test::dormant");
            for i in 0..1_000u64 {
                rec.record_ns(1_000 + i);
            }
            rec.flush();
            done_tx.send(()).expect("main thread alive");
            // Park holding the recorder — no further records, so no
            // phase-shift acknowledgement will ever come.
            go_rx.recv().expect("main thread alive");
            drop(rec);
        });

        done_rx.recv().expect("worker recorded");
        let snaps = reg.snapshot_all();

        go_tx.send(()).expect("worker alive");
        worker.join().expect("worker did not panic");

        let stage = snaps
            .iter()
            .find(|s| s.name == "test::dormant")
            .expect("dormant stage missing from snapshot");
        assert_eq!(stage.samples, 1_000);
        assert!(stage.min_ns >= 1_000);
    }

    #[test]
    fn flush_is_idempotent_and_loses_nothing() {
        let reg = StatsRegistry::new();
        let mut rec = reg.register("test::double_flush");
        rec.record_ns(10_000);
        rec.record_ns(20_000);
        rec.flush();
        // Second flush sheds an empty local histogram — merging it must
        // neither duplicate nor drop the first flush's samples.
        rec.flush();

        let snaps = reg.snapshot_all();
        let stage = snaps
            .iter()
            .find(|s| s.name == "test::double_flush")
            .expect("stage missing");
        assert_eq!(stage.samples, 2);

        // Recording again after a flush keeps working.
        rec.record_ns(30_000);
        rec.flush();
        let snaps = reg.snapshot_all();
        let stage = snaps
            .iter()
            .find(|s| s.name == "test::double_flush")
            .expect("stage missing");
        assert_eq!(stage.samples, 3);
    }

    #[test]
    fn snapshot_refresh_budget_is_shared_across_stages() {
        // Every recorder here is dormant, so none will ever acknowledge
        // the phase shift and each stage burns whatever timeout it is
        // given. A per-stage budget would make the dump take
        // stages × REFRESH_BUDGET — seconds, at the real stage count,
        // in exactly the all-idle state a post-run scrape hits.
        use std::time::Instant;

        const STAGES: usize = 6;
        let reg = StatsRegistry::new();
        // Held for the whole test: a dropped recorder sheds and
        // acknowledges, which is what we are deliberately preventing.
        let mut recorders = Vec::with_capacity(STAGES);
        for name in [
            "test::budget_0",
            "test::budget_1",
            "test::budget_2",
            "test::budget_3",
            "test::budget_4",
            "test::budget_5",
        ] {
            let mut rec = reg.register(name);
            rec.record_ns(7_000);
            rec.flush();
            recorders.push(rec);
        }

        let start = Instant::now();
        let snaps = reg.snapshot_all();
        let elapsed = start.elapsed();

        // One budget plus slack, not STAGES budgets.
        assert!(
            elapsed < REFRESH_BUDGET * 2,
            "snapshot took {elapsed:?} for {STAGES} dormant stages; \
             budget is {REFRESH_BUDGET:?} shared across all of them"
        );

        // Bounding the wait must not cost samples — the flush already
        // delivered them, and refresh drains the channel before waiting.
        for name in ["test::budget_0", "test::budget_5"] {
            let stage = snaps
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("{name} missing from snapshot"));
            assert_eq!(stage.samples, 1, "{name} lost its sample to the budget");
        }
    }

    #[test]
    fn flush_does_not_block_on_a_contended_stage_mutex() {
        // `snapshot_all` holds the stage mutex across a 500 ms refresh.
        // A flush landing in that window must take the `idle()` path —
        // return promptly *and* still get its samples merged into the
        // snapshot that is in flight, not the one after it.
        use std::sync::Arc;
        use std::sync::mpsc;
        use std::thread;
        use std::time::Instant;

        let reg = Arc::new(StatsRegistry::new());
        let (recorded_tx, recorded_rx) = mpsc::channel::<()>();
        let (elapsed_tx, elapsed_rx) = mpsc::channel::<std::time::Duration>();

        let worker_reg = Arc::clone(&reg);
        let worker = thread::spawn(move || {
            let mut rec = worker_reg.register("test::contended");
            rec.record_ns(4_000);
            recorded_tx.send(()).expect("main thread alive");
            // Let the snapshot get inside `refresh_timeout` and take
            // the mutex before we flush against it.
            thread::sleep(std::time::Duration::from_millis(50));
            let start = Instant::now();
            rec.flush();
            elapsed_tx.send(start.elapsed()).expect("main thread alive");
            // Hold the recorder so the samples can only have arrived
            // via the flush, never via Drop.
            thread::park();
            drop(rec);
        });

        recorded_rx.recv().expect("worker recorded");
        let snaps = reg.snapshot_all();

        let flush_took = elapsed_rx.recv().expect("worker flushed");
        assert!(
            flush_took < std::time::Duration::from_millis(400),
            "flush blocked on the refresh instead of taking the idle path: {flush_took:?}"
        );

        let stage = snaps
            .iter()
            .find(|s| s.name == "test::contended")
            .expect("contended stage missing from snapshot");
        assert_eq!(
            stage.samples, 1,
            "flush shed too late to be merged into the in-flight refresh"
        );

        worker.thread().unpark();
        worker.join().expect("worker did not panic");
    }

    #[test]
    fn refresh_during_active_recording() {
        // Production-shape test: a recorder is alive and recording
        // when refresh fires. Refresh waits for the recorder to ack
        // the phase shift via its next record call. Verifies the
        // steady-state path works (no drop required).
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;

        let reg = Arc::new(StatsRegistry::new());
        let stop = Arc::new(AtomicBool::new(false));

        let writer_reg = Arc::clone(&reg);
        let writer_stop = Arc::clone(&stop);
        let writer = thread::spawn(move || {
            let mut rec = writer_reg.register("test::active");
            while !writer_stop.load(Ordering::Relaxed) {
                rec.record_ns(42);
                std::thread::sleep(std::time::Duration::from_micros(100));
            }
        });

        // Give the writer a moment to record some samples + pick up
        // the phase shift on the next record after refresh starts.
        std::thread::sleep(std::time::Duration::from_millis(20));
        let snaps = reg.snapshot_all();

        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();

        let stage = snaps
            .iter()
            .find(|s| s.name == "test::active")
            .expect("active stage missing from snapshot");
        assert!(stage.samples > 0);
    }
}
