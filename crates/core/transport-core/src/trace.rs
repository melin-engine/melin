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

/// What one sample of a stage counts.
///
/// Declared per stage because the dump puts stages side by side and so
/// invites summing their percentiles — which is only meaningful between
/// stages sharing a denominator. `egress` takes one sample per io_uring
/// flush covering many slots; `journal-wait` takes one only for slots
/// that actually blocked, so its percentiles are conditional on having
/// waited and its sample count is not the event count. Publishing the
/// unit lets a consumer tell those apart without reading the pipeline.
///
/// Not gated on `latency-trace`: `register_stage` takes one in both
/// build configurations so call sites need no `#[cfg]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageUnit {
    /// One sample per pipeline slot — an input event or an output.
    Slot,
    /// One sample per decoded wire frame.
    Frame,
    /// One sample per client request, however many frames it produced.
    Request,
    /// One sample per disruptor batch, however many slots it held.
    Batch,
    /// One sample per I/O flush, covering every slot whose bytes it
    /// shipped.
    Flush,
    /// One sample per slot that actually blocked. The fast path records
    /// nothing, so percentiles describe only the slots that waited —
    /// never comparable to a per-slot stage, and never summable with
    /// one.
    BlockedSlot,
    /// One sample per poll-loop iteration that found work.
    Iteration,
}

impl StageUnit {
    /// Stable wire token for the dump's `unit` field.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Slot => "slot",
            Self::Frame => "frame",
            Self::Request => "request",
            Self::Batch => "batch",
            Self::Flush => "flush",
            Self::BlockedSlot => "blocked-slot",
            Self::Iteration => "iteration",
        }
    }
}

/// Snapshot of a stage's histogram percentiles. Returned by
/// `StatsRegistry::snapshot_all` — the stable structure the health
/// endpoint serializes to wire format.
#[cfg(feature = "latency-trace")]
#[derive(Debug, Clone)]
pub struct StageSnapshot {
    pub name: &'static str,
    /// What one `samples` count represents — see [`StageUnit`].
    pub unit: StageUnit,
    pub samples: u64,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p99_9_ns: u64,
    pub max_ns: u64,
    /// How many of `samples` exceeded [`MAX_TRACKED_NS`] and were
    /// clamped to it on the way in. When non-zero, `max_ns` is the
    /// ceiling bucket's upper edge (marginally above `MAX_TRACKED_NS`
    /// at 3 significant digits), not an observed duration.
    ///
    /// Reported because a clamped sample is otherwise
    /// indistinguishable from a real one at the ceiling: a replica
    /// wait that spanned a failover and a wait that happened to take
    /// exactly 100 ms produce the same `max_ns`. A non-zero count says
    /// "the tail is longer than this histogram can express" — which is
    /// information; a silently clamped max is not.
    pub clipped: u64,
}

/// Upper bound of every stage histogram. Samples above it are clamped
/// on record and counted in [`StageSnapshot::clipped`].
///
/// 100 ms is generous for the per-stage spans this feature exists to
/// measure (sub-microsecond to low-millisecond). The durability-gate
/// waits can exceed it during a failover, which is exactly why the
/// clamp is counted rather than widened: a wider bound costs bucket
/// memory on every stage to accommodate an event that is better
/// reported as "off the scale" than measured imprecisely.
#[cfg(feature = "latency-trace")]
pub const MAX_TRACKED_NS: u64 = 100_000_000;

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
    /// Saturates instead of returning an error when `ns` exceeds
    /// [`MAX_TRACKED_NS`] — diagnostic samples are best-effort, and
    /// dropping a single very-out-of-range sample is preferable to
    /// crashing the trading thread. Saturated samples are counted so
    /// the snapshot can say the tail ran off the scale.
    ///
    /// The out-of-range branch is the only added hot-path cost: one
    /// compare, predicted not-taken, and the atomic increment is
    /// reached only by samples that were already pathological.
    #[inline]
    pub fn record_ns(&mut self, ns: u64) {
        if ns > MAX_TRACKED_NS {
            self.entry
                .clipped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
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
    /// Fixed at first registration; siblings inherit it.
    unit: StageUnit,
    sync: std::sync::Mutex<hdrhistogram::sync::SyncHistogram<u64>>,
    /// Count of samples clamped to [`MAX_TRACKED_NS`] on record.
    ///
    /// An atomic on the entry rather than a field on `StageRecorder`
    /// because every sibling recorder for a stage must contribute to
    /// one count, and the snapshot reads it without taking the stage
    /// mutex. Relaxed throughout: the count is diagnostic and is not
    /// ordered against anything.
    clipped: std::sync::atomic::AtomicU64,
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
    ///
    /// `unit` declares what one sample counts; the first registration
    /// fixes it and siblings inherit it. Two stages sharing a name but
    /// disagreeing on their unit would merge samples with different
    /// denominators into one histogram, so that is a bug in the caller
    /// rather than something to reconcile here — debug builds assert.
    pub fn register(&self, name: &'static str, unit: StageUnit) -> StageRecorder {
        let mut entries = match self.entries.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        for existing in entries.iter() {
            if existing.name == name {
                debug_assert_eq!(
                    existing.unit, unit,
                    "stage {name:?} registered with conflicting units"
                );
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
        // Range: 1 ns to MAX_TRACKED_NS, 3 significant digits — same as
        // the pre-SyncHistogram design; matches the expected per-stage
        // percentile shape.
        let hist = hdrhistogram::Histogram::<u64>::new_with_bounds(1, MAX_TRACKED_NS, 3)
            .expect("valid histogram bounds");
        let sync: hdrhistogram::sync::SyncHistogram<u64> = hist.into();
        let recorder = sync.recorder();
        let entry = std::sync::Arc::new(StageEntry {
            name,
            unit,
            sync: std::sync::Mutex::new(sync),
            clipped: std::sync::atomic::AtomicU64::new(0),
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
        // Copy the entry handles out and release the registry lock
        // before refreshing anything. The refresh below can block for
        // the whole budget; holding the registry lock across it made
        // concurrent scrapes strictly serial, and because each waiting
        // caller starts its deadline only after acquiring the lock, N
        // overlapping scrapes cost N × REFRESH_BUDGET rather than one.
        // It also stalled any stage thread still in `register`, which
        // takes the same lock. Per-stage mutexes still serialize the
        // refreshes themselves, but a second caller now arrives with
        // its deadline already running and finishes promptly.
        //
        // Cost is a refcount bump per stage, on a path that runs once
        // per `/stats-dump`.
        let entries: Vec<std::sync::Arc<StageEntry>> = match self.entries.lock() {
            Ok(g) => g.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
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
            let clipped = entry.clipped.load(std::sync::atomic::Ordering::Relaxed);
            if sync.is_empty() {
                // Percentile queries on an empty histogram are defined
                // but meaningless; report explicit zeros instead.
                out.push(StageSnapshot {
                    name: entry.name,
                    unit: entry.unit,
                    samples: 0,
                    min_ns: 0,
                    p50_ns: 0,
                    p90_ns: 0,
                    p99_ns: 0,
                    p99_9_ns: 0,
                    max_ns: 0,
                    clipped,
                });
                continue;
            }
            out.push(StageSnapshot {
                name: entry.name,
                unit: entry.unit,
                samples: sync.len(),
                min_ns: sync.min(),
                p50_ns: sync.value_at_quantile(0.50),
                p90_ns: sync.value_at_quantile(0.90),
                p99_ns: sync.value_at_quantile(0.99),
                p99_9_ns: sync.value_at_quantile(0.999),
                max_ns: sync.max(),
                clipped,
            });
        }
        out
    }

    /// Drain every recorder and clear all stage histograms, starting a
    /// fresh measurement window. Returns the number of stages cleared.
    ///
    /// The histograms are otherwise cumulative for the whole process
    /// lifetime, so a run that warms up and then measures folds the
    /// warmup into the same percentiles, and two runs against one
    /// server cannot be told apart. Issue this between the warmup and
    /// the measured phase.
    ///
    /// Refreshes before clearing so samples already handed over are
    /// discarded with the window they belong to instead of surviving
    /// into the next one. A recorder that has not flushed since its
    /// last sample still holds those samples thread-locally and will
    /// contribute them to the *new* window; stage threads flush on an
    /// [`IDLE_FLUSH_INTERVAL`] timer, so allow that much settling time
    /// after traffic stops if the boundary has to be exact.
    pub fn reset_all(&self) -> usize {
        // Same lock discipline as `snapshot_all` — the refresh below
        // can block, and holding the registry lock across it would
        // serialize concurrent callers and stall `register`.
        let entries: Vec<std::sync::Arc<StageEntry>> = match self.entries.lock() {
            Ok(g) => g.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        let deadline = std::time::Instant::now() + REFRESH_BUDGET;
        for entry in entries.iter() {
            let mut sync = match entry.sync.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            sync.refresh_timeout(deadline.saturating_duration_since(std::time::Instant::now()));
            sync.reset();
            entry.clipped.store(0, std::sync::atomic::Ordering::Relaxed);
        }
        entries.len()
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
                let buf = format!(
                    "  {}\n\x20   samples: 0 (never recorded, per {})\n",
                    snap.name,
                    snap.unit.as_str()
                );
                // Best-effort diagnostic output on shutdown.
                let _ = std::io::stderr().lock().write_all(buf.as_bytes());
                continue;
            }
            // A clipped count means `max` is the ceiling, not the real
            // worst case — say so on the same line rather than leaving
            // the reader to notice a separate row.
            let max_note = if snap.clipped > 0 {
                format!(" (ceiling; {} sample(s) clipped)", snap.clipped)
            } else {
                String::new()
            };
            let buf = format!(
                "  {name}\n\
                 \x20   samples: {samples} (per {unit})\n\
                 \x20   min:    {min:>8.2} µs\n\
                 \x20   p50:    {p50:>8.2} µs\n\
                 \x20   p90:    {p90:>8.2} µs\n\
                 \x20   p99:    {p99:>8.2} µs\n\
                 \x20   p99.9:  {p999:>8.2} µs\n\
                 \x20   max:    {max:>8.2} µs{max_note}\n",
                name = snap.name,
                samples = snap.samples,
                unit = snap.unit.as_str(),
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
/// Convenience for the common case
/// `let mut h = register_stage("…", StageUnit::Slot);`.
/// Idempotent — calling twice with the same name returns sibling
/// recorders that feed the same underlying `SyncHistogram`.
#[cfg(feature = "latency-trace")]
pub fn register_stage(name: &'static str, unit: StageUnit) -> StageRecorder {
    global_registry().register(name, unit)
}

#[cfg(not(feature = "latency-trace"))]
#[inline]
pub fn register_stage(_name: &'static str, _unit: StageUnit) -> StageRecorder {
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
            let mut rec = reg.register("test::stage_one", StageUnit::Slot);
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
            let mut a = reg.register("test::dup", StageUnit::Slot);
            let mut b = reg.register("test::dup", StageUnit::Slot);
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
        let _empty = reg.register("test::empty", StageUnit::Slot);
        {
            let mut used = reg.register("test::used", StageUnit::Slot);
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
            let mut rec = worker_reg.register("test::dormant", StageUnit::Slot);
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
    fn out_of_range_samples_are_counted_not_silently_clamped() {
        // A wait longer than the histogram's ceiling — a failover-scale
        // replica wait — must not read back as an ordinary sample that
        // happened to land at 100 ms. `max_ns` still reports the
        // ceiling (that is what saturation means), but `clipped` says
        // the real tail ran past it.
        let reg = StatsRegistry::new();
        let mut rec = reg.register("test::clipped", StageUnit::BlockedSlot);
        rec.record_ns(5_000);
        rec.record_ns(MAX_TRACKED_NS + 1);
        rec.record_ns(MAX_TRACKED_NS * 30);
        rec.flush();

        let snaps = reg.snapshot_all();
        let stage = snaps
            .iter()
            .find(|s| s.name == "test::clipped")
            .expect("stage missing");
        assert_eq!(stage.samples, 3, "clipped samples must still be recorded");
        assert_eq!(stage.clipped, 2);
        // `max_ns` pins to the ceiling bucket regardless of how far the
        // real value overshot — 30× the bound reads back the same as
        // 1 ns past it, which is precisely why `clipped` exists. The
        // reported value is the bucket's upper edge, so it sits a
        // fraction of a percent above the bound at 3 significant
        // digits rather than exactly on it.
        assert!(
            stage.max_ns >= MAX_TRACKED_NS && stage.max_ns < MAX_TRACKED_NS + MAX_TRACKED_NS / 100,
            "expected max pinned to the ceiling bucket, got {}",
            stage.max_ns
        );
    }

    #[test]
    fn a_sample_exactly_at_the_bound_is_not_clipped() {
        // The boundary is inclusive — MAX_TRACKED_NS is representable,
        // so counting it would overstate censoring on a stage whose
        // tail merely touches the ceiling.
        let reg = StatsRegistry::new();
        let mut rec = reg.register("test::at_bound", StageUnit::Slot);
        rec.record_ns(MAX_TRACKED_NS);
        rec.flush();

        let snaps = reg.snapshot_all();
        let stage = snaps
            .iter()
            .find(|s| s.name == "test::at_bound")
            .expect("stage missing");
        assert_eq!(stage.samples, 1);
        assert_eq!(stage.clipped, 0);
    }

    #[test]
    fn clipped_count_is_shared_across_sibling_recorders() {
        // Sibling recorders feed one histogram; they must also feed one
        // clipped count, or a per-thread count would under-report by
        // however many threads happened to record the stage.
        let reg = StatsRegistry::new();
        let mut a = reg.register("test::clipped_siblings", StageUnit::Slot);
        let mut b = reg.register("test::clipped_siblings", StageUnit::Slot);
        a.record_ns(MAX_TRACKED_NS + 1);
        b.record_ns(MAX_TRACKED_NS + 1);
        a.flush();
        b.flush();

        let snaps = reg.snapshot_all();
        let stage = snaps
            .iter()
            .find(|s| s.name == "test::clipped_siblings")
            .expect("stage missing");
        assert_eq!(stage.clipped, 2);
    }

    #[test]
    fn flush_is_idempotent_and_loses_nothing() {
        let reg = StatsRegistry::new();
        let mut rec = reg.register("test::double_flush", StageUnit::Slot);
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
            let mut rec = reg.register(name, StageUnit::Slot);
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
    fn reset_starts_a_fresh_window() {
        // Warmup samples must not survive into the measured window —
        // the whole reason the endpoint exists. Stages stay registered
        // (so the next dump still lists them) but come back empty.
        let reg = StatsRegistry::new();
        let mut rec = reg.register("test::reset_window", StageUnit::Slot);
        rec.record_ns(9_000);
        rec.record_ns(MAX_TRACKED_NS * 2);
        rec.flush();

        let before = reg.snapshot_all();
        let before = before
            .iter()
            .find(|s| s.name == "test::reset_window")
            .expect("stage missing before reset");
        assert_eq!(before.samples, 2);
        assert_eq!(before.clipped, 1);

        assert_eq!(reg.reset_all(), 1, "reset should report the stage count");

        let after = reg.snapshot_all();
        let after = after
            .iter()
            .find(|s| s.name == "test::reset_window")
            .expect("reset must not deregister the stage");
        assert_eq!(after.samples, 0, "warmup samples survived the reset");
        assert_eq!(after.clipped, 0, "clipped count survived the reset");
        assert_eq!(after.max_ns, 0);

        // The recorder is still usable and feeds the new window.
        rec.record_ns(1_500);
        rec.flush();
        let next = reg.snapshot_all();
        let next = next
            .iter()
            .find(|s| s.name == "test::reset_window")
            .expect("stage missing after reset");
        assert_eq!(next.samples, 1);
        assert!(
            next.max_ns < 10_000,
            "pre-reset max leaked: {}",
            next.max_ns
        );
    }

    #[test]
    fn concurrent_snapshots_do_not_serialize_budgets() {
        // Overlapping scrapes must not each pay a fresh REFRESH_BUDGET.
        // While the registry lock was held across the refresh, a waiting
        // caller only started its deadline after acquiring it, so N
        // concurrent dumps took N × budget — enough to blow past a
        // client read timeout with a handful of scrapers.
        use std::sync::Arc;
        use std::thread;
        use std::time::Instant;

        let reg = Arc::new(StatsRegistry::new());
        // Dormant recorders held for the test's duration: they never
        // acknowledge the phase shift, so every refresh burns its full
        // remaining budget. That is what makes the serialization
        // measurable at all.
        let mut recorders = Vec::new();
        for name in ["test::concurrent_a", "test::concurrent_b"] {
            let mut rec = reg.register(name, StageUnit::Slot);
            rec.record_ns(1_000);
            rec.flush();
            recorders.push(rec);
        }

        const SCRAPERS: usize = 3;
        let start = Instant::now();
        let handles: Vec<_> = (0..SCRAPERS)
            .map(|_| {
                let reg = Arc::clone(&reg);
                thread::spawn(move || reg.snapshot_all())
            })
            .collect();
        for h in handles {
            let snaps = h.join().expect("scraper did not panic");
            assert!(!snaps.is_empty(), "scrape returned no stages");
        }
        let elapsed = start.elapsed();

        assert!(
            elapsed < REFRESH_BUDGET * 2,
            "{SCRAPERS} concurrent scrapes took {elapsed:?}; a single shared \
             budget is {REFRESH_BUDGET:?}"
        );
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
            let mut rec = worker_reg.register("test::contended", StageUnit::Slot);
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
            let mut rec = writer_reg.register("test::active", StageUnit::Slot);
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
