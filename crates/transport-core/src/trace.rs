//! Per-stage latency tracing for the disruptor pipeline.
//!
//! Behind the `latency-trace` feature gate. When disabled, `MonoTraceInstant`
//! is `()` (zero-sized) and all tracing helpers are no-ops — zero overhead.
//!
//! ## Stats registry
//!
//! Stages register their per-stage histograms with a process-global
//! `StatsRegistry` (single server per process). Each registered stage
//! is backed by a [`hdrhistogram::sync::SyncHistogram`]: every recording
//! thread holds its own [`hdrhistogram::sync::Recorder`] (a per-thread
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
}

#[cfg(feature = "latency-trace")]
impl Clone for StageRecorder {
    fn clone(&self) -> Self {
        Self {
            rec: self.rec.clone(),
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
                let sync = match existing.sync.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                return StageRecorder {
                    rec: sync.recorder(),
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
        entries.push(std::sync::Arc::new(StageEntry {
            name,
            sync: std::sync::Mutex::new(sync),
        }));
        StageRecorder { rec: recorder }
    }

    /// Snapshot every registered stage. Stages with zero samples are
    /// omitted from the result.
    ///
    /// Refresh waits up to 500 ms for each recorder to acknowledge the
    /// phase shift via its next `record` call. The bench's `/stats-dump`
    /// fetch typically happens immediately after the workload completes
    /// — at which point the stage threads have just gone idle and may
    /// not record again for hundreds of milliseconds (busy-spin, no
    /// inbound traffic). A short 10 ms timeout missed those tail
    /// records; 500 ms is generous enough to catch stragglers from
    /// heartbeats, scheduler ticks, or one-off control messages while
    /// still bounding the dump's worst-case wall time.
    ///
    /// Recorders that stay fully dormant past the timeout have their
    /// last samples skipped (rolled over to the next snapshot when the
    /// recorder records again). Worst case the bench gets slightly
    /// stale data; never wrong, never hung.
    pub fn snapshot_all(&self) -> Vec<StageSnapshot> {
        let entries = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            let mut sync = match entry.sync.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            // Pull pending samples from all recorders into the main
            // histogram. Bounded wait so an idle recorder can't hang
            // a /stats-dump request — see the doc on `snapshot_all`
            // for why 500 ms (vs e.g. 10 ms).
            sync.refresh_timeout(std::time::Duration::from_millis(500));
            if sync.is_empty() {
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
    // Production stage threads record continuously, so refresh
    // completes well below the timeout. Tests work around this by
    // dropping recorders before snapshot: the Recorder Drop impl
    // ships pending samples to the SyncHistogram via an unbounded
    // channel, which the next refresh picks up.

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
    fn snapshot_omits_empty_stages() {
        let reg = StatsRegistry::new();
        let _empty = reg.register("test::empty");
        {
            let mut used = reg.register("test::used");
            used.record_ns(500);
        }

        let snaps = reg.snapshot_all();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].name, "test::used");
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
