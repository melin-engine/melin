//! Per-stage latency tracing for the disruptor pipeline.
//!
//! Behind the `latency-trace` feature gate. When disabled, `TraceTimestamp`
//! is `()` (zero-sized) and all tracing helpers are no-ops — zero overhead.
//!
//! ## Stats registry
//!
//! Stages register their per-stage histograms with a process-global
//! `StatsRegistry` (single server per process). Recorders are
//! `Arc<Mutex<StageHistogram>>` clones — stage threads record into them
//! lock-only-when-tracing-is-on; the health endpoint snapshots all of
//! them via `global_registry().snapshot_all()` for the bench's
//! tick-to-trade dump. Mutex cost is irrelevant in production builds
//! because the entire path collapses to ZSTs and inlined no-ops.

/// Timestamp carried through pipeline slots.
///
/// `u64` nanoseconds when tracing is enabled, `()` (ZST, optimized away)
/// when disabled. This avoids `#[cfg]` on struct fields while adding
/// zero bytes to slot layouts in production builds.
#[cfg(feature = "latency-trace")]
pub type TraceTimestamp = u64;

#[cfg(not(feature = "latency-trace"))]
pub type TraceTimestamp = ();

/// Capture a trace timestamp. Returns `()` when tracing is disabled.
#[cfg(feature = "latency-trace")]
#[inline]
pub fn trace_ts() -> TraceTimestamp {
    mono_nanos()
}

#[cfg(not(feature = "latency-trace"))]
#[inline]
pub fn trace_ts() -> TraceTimestamp {}

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
pub fn trace_elapsed_ns(start: TraceTimestamp, end: TraceTimestamp) -> u64 {
    end.saturating_sub(start)
}

/// Per-stage latency histogram. Collects nanosecond samples and prints
/// a summary when `print_report` is called.
#[cfg(feature = "latency-trace")]
pub struct StageHistogram {
    name: &'static str,
    hist: hdrhistogram::Histogram<u64>,
    /// Whether a report has already been printed (prevents double-print from
    /// both explicit `print_report` and `Drop`).
    printed: bool,
}

#[cfg(feature = "latency-trace")]
impl StageHistogram {
    /// Create a new histogram for the named stage.
    /// Range: 1 ns to 100 ms, 3 significant digits.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            hist: hdrhistogram::Histogram::new_with_bounds(1, 100_000_000, 3)
                .expect("valid histogram bounds"),
            printed: false,
        }
    }

    /// Record a latency sample in nanoseconds.
    #[inline]
    pub fn record_ns(&mut self, ns: u64) {
        // Best-effort: record() only fails if ns exceeds the histogram's
        // configured max, which is non-critical for diagnostics.
        let _ = self.hist.record(ns);
    }

    /// Snapshot the current state as a `StageSnapshot`. Returns `None`
    /// when no samples have been recorded.
    pub fn snapshot(&self) -> Option<StageSnapshot> {
        if self.hist.is_empty() {
            return None;
        }
        Some(StageSnapshot {
            name: self.name,
            samples: self.hist.len(),
            min_ns: self.hist.min(),
            p50_ns: self.hist.value_at_quantile(0.50),
            p90_ns: self.hist.value_at_quantile(0.90),
            p99_ns: self.hist.value_at_quantile(0.99),
            p99_9_ns: self.hist.value_at_quantile(0.999),
            max_ns: self.hist.max(),
        })
    }
}

#[cfg(feature = "latency-trace")]
impl Drop for StageHistogram {
    fn drop(&mut self) {
        if !self.printed {
            self.print_report_inner();
        }
    }
}

#[cfg(feature = "latency-trace")]
impl StageHistogram {
    /// Print a formatted latency summary to stderr.
    pub fn print_report(&mut self) {
        self.print_report_inner();
        self.printed = true;
    }

    fn print_report_inner(&self) {
        use std::io::Write as _;

        if self.hist.is_empty() {
            return;
        }
        let us = |ns: u64| ns as f64 / 1000.0;

        // Build the full report in memory, then write atomically to stderr
        // so concurrent threads don't interleave output.
        let buf = format!(
            "  {name}\n\
             \x20   samples: {samples}\n\
             \x20   min:    {min:>8.2} µs\n\
             \x20   p50:    {p50:>8.2} µs\n\
             \x20   p90:    {p90:>8.2} µs\n\
             \x20   p99:    {p99:>8.2} µs\n\
             \x20   p99.9:  {p999:>8.2} µs\n\
             \x20   max:    {max:>8.2} µs\n",
            name = self.name,
            samples = self.hist.len(),
            min = us(self.hist.min()),
            p50 = us(self.hist.value_at_quantile(0.50)),
            p90 = us(self.hist.value_at_quantile(0.90)),
            p99 = us(self.hist.value_at_quantile(0.99)),
            p999 = us(self.hist.value_at_quantile(0.999)),
            max = us(self.hist.max()),
        );

        // Best-effort diagnostic output on shutdown.
        let _ = std::io::stderr().lock().write_all(buf.as_bytes());
    }
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
/// Cheap to clone — wraps an `Arc<Mutex<StageHistogram>>` when
/// `latency-trace` is on, ZST when off. Each `record_ns` call locks
/// the mutex briefly; lock cost is irrelevant because it only exists
/// in dev/bench builds with `--features latency-trace`.
#[cfg(feature = "latency-trace")]
#[derive(Clone)]
pub struct StageRecorder {
    hist: std::sync::Arc<std::sync::Mutex<StageHistogram>>,
}

#[cfg(feature = "latency-trace")]
impl StageRecorder {
    /// Record a single sample in nanoseconds.
    #[inline]
    pub fn record_ns(&self, ns: u64) {
        // Mutex poisoning ignored: a poisoned histogram is still
        // valid for recording; the panic that poisoned it is the
        // operator's primary signal, not a missing percentile.
        if let Ok(mut h) = self.hist.lock() {
            h.record_ns(ns);
        }
    }

    /// Record the elapsed nanoseconds between two trace timestamps.
    #[inline]
    pub fn record_elapsed(&self, start: TraceTimestamp, end: TraceTimestamp) {
        self.record_ns(trace_elapsed_ns(start, end));
    }
}

#[cfg(not(feature = "latency-trace"))]
#[derive(Clone, Copy, Default)]
pub struct StageRecorder;

#[cfg(not(feature = "latency-trace"))]
impl StageRecorder {
    #[inline]
    pub fn record_ns(&self, _ns: u64) {}

    #[inline]
    pub fn record_elapsed(&self, _start: TraceTimestamp, _end: TraceTimestamp) {}
}

/// Process-wide registry of stage histograms.
///
/// One instance per process via `global_registry()`. Stages register
/// themselves at startup; the health endpoint dumps the registry on
/// demand for the bench's tick-to-trade decomposition.
#[cfg(feature = "latency-trace")]
pub struct StatsRegistry {
    // Vec, not HashMap, because: (a) tens of entries at most, (b) we
    // want stable insertion order in dumps, (c) lookup-by-name is
    // never on the hot path. Mutex protects the Vec only during
    // register / iterate; the per-stage histograms are independently
    // locked for recording.
    entries: std::sync::Mutex<Vec<std::sync::Arc<std::sync::Mutex<StageHistogram>>>>,
}

#[cfg(feature = "latency-trace")]
impl StatsRegistry {
    fn new() -> Self {
        Self {
            entries: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// Register a new stage and return a recorder for it.
    ///
    /// If a stage with the same name was already registered, the
    /// existing recorder is returned (re-registration is a no-op).
    /// This makes the API safe to call from a stage thread that
    /// might be restarted within the same process (tests, in-process
    /// failover).
    pub fn register(&self, name: &'static str) -> StageRecorder {
        let mut entries = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        for existing in entries.iter() {
            // Lock briefly to read the name; if poisoned, fall through
            // and create a new entry — the old one is unrecoverable.
            if let Ok(h) = existing.lock()
                && h.name == name
            {
                return StageRecorder {
                    hist: existing.clone(),
                };
            }
        }
        let h = std::sync::Arc::new(std::sync::Mutex::new(StageHistogram::new(name)));
        entries.push(h.clone());
        StageRecorder { hist: h }
    }

    /// Snapshot every registered stage. Stages with zero samples are
    /// omitted from the result.
    pub fn snapshot_all(&self) -> Vec<StageSnapshot> {
        let entries = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        entries
            .iter()
            .filter_map(|h| h.lock().ok().and_then(|h| h.snapshot()))
            .collect()
    }

    /// Print every registered stage's percentile report to stderr.
    /// Called from the server's shutdown path so dev runs without the
    /// bench still see the per-stage breakdown — the bench fetches the
    /// same data via the health endpoint instead.
    pub fn print_report_all(&self) {
        let entries = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        for h in entries.iter() {
            if let Ok(mut h) = h.lock() {
                h.print_report();
            }
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
/// Convenience for the common case `let h = register_stage("…");`.
/// Idempotent — calling twice with the same name returns recorders
/// pointing at the same underlying histogram.
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

    #[test]
    fn registry_register_returns_recorder_that_records() {
        let reg = StatsRegistry::new();
        let rec = reg.register("test::stage_one");
        rec.record_ns(1_000);
        rec.record_ns(2_000);
        rec.record_ns(3_000);

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
        let a = reg.register("test::dup");
        let b = reg.register("test::dup");
        a.record_ns(100);
        b.record_ns(200);
        let snaps = reg.snapshot_all();
        // Both recorders point at the same histogram.
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].samples, 2);
    }

    #[test]
    fn snapshot_omits_empty_stages() {
        let reg = StatsRegistry::new();
        let _empty = reg.register("test::empty");
        let used = reg.register("test::used");
        used.record_ns(500);

        let snaps = reg.snapshot_all();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].name, "test::used");
    }
}
