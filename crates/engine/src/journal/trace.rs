//! Per-stage latency tracing for the disruptor pipeline.
//!
//! Behind the `latency-trace` feature gate. When disabled, `TraceTimestamp`
//! is `()` (zero-sized) and all tracing helpers are no-ops — zero overhead.

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
        let _ = self.hist.record(ns);
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

        let _ = std::io::stderr().lock().write_all(buf.as_bytes());
    }
}
