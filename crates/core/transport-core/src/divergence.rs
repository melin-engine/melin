//! The stop on divergence: what a node does once two copies of the
//! application are seen to answer the same input differently.
//!
//! A divergence stops the whole cluster, for good: the way back is a new
//! cluster. This module holds the node's half of that. [`DivergenceLatch`]
//! is tripped once, by whichever cause comes first ([`HaltCause`]); it
//! fences the node, which closes every client and makes the control plane
//! refuse to promote it, sets the node's shutdown flag, logs the cause,
//! and writes a marker file beside the journal, which makes every later
//! start of the node refuse ([`crate::node_store::NodeStore::refuse_if_diverged`]).
//! Telling the other nodes is the replication links' job, which waits for
//! the marker within [`HALT_NOTICE_BOUND`] so a crash cannot leave a node
//! that restarts into the cluster it has just stopped.
//!
//! Plan: `docs/internal/divergence-detection-2026-10.md`, decisions 10
//! and 11.

use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use melin_journal::segment::parent_dir;

use crate::fence::FenceState;
use crate::output_digest::{Divergence, Trip};

/// How long a notice to another node waits for the marker to be durable
/// before it is sent anyway, and how long a stopped node's exit waits for
/// it. A notice must not be held by a hung disk: a node killed while
/// holding it would have told nobody, and the cluster could fail over onto
/// the build already seen to diverge.
pub const HALT_NOTICE_BOUND: Duration = Duration::from_secs(5);

/// Why a node stopped. Exhaustive: a new cause must decide how it is
/// logged, recorded and refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HaltCause {
    /// This node's own comparison found its output differing from its
    /// primary's.
    Detected(Divergence),
    /// Another node told this one that the cluster has stopped.
    ClusterHalted {
        /// Who told it.
        by: HaltedBy,
        /// How.
        via: NoticePath,
    },
}

/// The node that told this one the cluster had stopped.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HaltedBy {
    /// This node's primary.
    Primary,
    /// A replica streaming from this node, by the replication slot it
    /// streamed on. Naming its key is diagnostics the plan defers with the
    /// build ids.
    Replica {
        /// The replication slot it streamed on.
        slot: usize,
    },
}

/// How a notice of the stop arrived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoticePath {
    /// A replica's acknowledgement flagged as stopped.
    Ack,
    /// A replica's handshake flagged as stopped.
    Handshake,
    /// The primary's halt frame.
    HaltFrame,
}

impl fmt::Display for HaltedBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Primary => write!(f, "the primary"),
            Self::Replica { slot } => write!(f, "the replica on slot {slot}"),
        }
    }
}

impl fmt::Display for NoticePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Ack => "its acknowledgement",
            Self::Handshake => "its handshake",
            Self::HaltFrame => "the halt frame",
        })
    }
}

/// How the marker reaches stable storage: the file's contents, then the
/// directory entry its rename made. A trait so a test can make either
/// fail, or hang, without a faulty disk.
pub trait MarkerSync: Send + Sync {
    /// Make the written marker file's contents durable.
    fn sync_file(&self, file: &File) -> io::Result<()>;
    /// Make the rename in `dir` durable.
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

/// The real disk: `fsync` of the file, then of its directory.
pub struct FsSync;

impl MarkerSync for FsSync {
    fn sync_file(&self, file: &File) -> io::Result<()> {
        file.sync_all()
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }
}

/// Where the marker write stands.
#[derive(Debug)]
enum MarkerSlot {
    /// Not tripped, or the write is under way.
    Pending,
    Written,
    /// The write failed. An `Arc` so the outcome can be handed to every
    /// waiter; `io::Error` is not `Clone`.
    Failed(Arc<io::Error>),
}

/// What became of the marker, as a waiter sees it.
#[derive(Clone, Debug)]
pub enum MarkerOutcome {
    /// Written and durable: every later start of this node refuses.
    Written,
    /// The write failed: a later start may not be refused.
    Failed(Arc<io::Error>),
    /// Not settled within the wait: the disk may have hung.
    Unconfirmed,
}

/// Proof that this caller tripped the latch: returned once, to the first
/// tripper only. Neither `Clone` nor `Copy`, and only the latch makes one,
/// so whatever must happen once per stop (the notice) can demand it.
#[must_use = "the first tripper owes the other nodes a notice"]
#[derive(Debug)]
pub struct Halted {
    _private: (),
}

/// The node's divergence stop: tripped once, by the first cause. Cheap to
/// clone; every clone is the same latch.
#[derive(Clone)]
pub struct DivergenceLatch {
    inner: Arc<Inner>,
}

struct Inner {
    /// Set first on a trip, before anything else: what the response
    /// stage's per-slot check reads. Relaxed loads only; the cause behind
    /// it is read through `cause`.
    tripped: AtomicBool,
    /// The first cause. `OnceLock`: set once, read by any thread after.
    cause: OnceLock<HaltCause>,
    fence: Arc<FenceState>,
    shutdown: Arc<AtomicBool>,
    /// `<journal>.diverged`.
    marker: PathBuf,
    sync: Arc<dyn MarkerSync>,
    /// The marker write's outcome. A mutex and a condvar: touched only on
    /// this cold path, and a waiter must be able to sleep on it.
    state: Mutex<MarkerSlot>,
    settled: Condvar,
}

impl fmt::Debug for DivergenceLatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DivergenceLatch")
            .field("cause", &self.inner.cause.get())
            .field("marker", &self.inner.marker)
            .finish_non_exhaustive()
    }
}

impl DivergenceLatch {
    /// The latch of a node whose fence and shutdown flag are these, its
    /// marker at `marker` ([`crate::node_store::NodeStore::divergence_marker`]),
    /// made durable through `sync`.
    pub fn new(
        fence: Arc<FenceState>,
        shutdown: Arc<AtomicBool>,
        marker: PathBuf,
        sync: Arc<dyn MarkerSync>,
    ) -> Self {
        let latch = Self {
            inner: Arc::new(Inner {
                tripped: AtomicBool::new(false),
                cause: OnceLock::new(),
                fence,
                shutdown,
                marker,
                sync,
                state: Mutex::new(MarkerSlot::Pending),
                settled: Condvar::new(),
            }),
        };
        #[cfg(feature = "test-utils")]
        test_seam::register(&latch);
        latch
    }

    /// Whether the node has stopped. One relaxed load: the response stage
    /// reads it before every send.
    #[inline]
    pub fn is_tripped(&self) -> bool {
        self.inner.tripped.load(Ordering::Relaxed)
    }

    /// Why the node stopped, once it has.
    pub fn cause(&self) -> Option<&HaltCause> {
        self.inner.cause.get()
    }

    /// The marker's path.
    pub fn marker(&self) -> &Path {
        &self.inner.marker
    }

    /// Stop the node for `cause`, unless it has stopped already. The first
    /// trip, in order: marks the latch, fences the node and sets its
    /// shutdown flag, logs the cause at `error!`, starts the marker write
    /// on a thread of its own, and returns the [`Halted`] token. Later
    /// trips change nothing and return `None`.
    pub fn trip_with(&self, cause: HaltCause) -> Option<Halted> {
        if self.inner.cause.set(cause).is_err() {
            return None;
        }
        let cause = self
            .inner
            .cause
            .get()
            .expect("the cause was set just above");
        self.inner.tripped.store(true, Ordering::Release);
        // Fenced first, then shutting down, as `fence_if_superseded`
        // orders them: every reader closes its clients, the control plane
        // refuses to promote, and every stage leaves at its next check.
        self.inner.fence.fence();
        self.inner.shutdown.store(true, Ordering::Release);
        log_cause(cause);
        self.start_marker_write();
        Some(Halted { _private: () })
    }

    /// Write the marker on a short-lived thread, so the tripping thread,
    /// which may own a replication link, never sits in `fsync`. The
    /// tripping thread writes it itself only if the thread cannot start.
    fn start_marker_write(&self) {
        let latch = self.clone();
        let spawned = std::thread::Builder::new()
            .name("divergence-marker".into())
            .spawn(move || latch.write_marker());
        if let Err(e) = spawned {
            tracing::error!(error = %e, "cannot start the divergence marker thread: writing it here");
            self.write_marker();
        }
    }

    fn write_marker(&self) {
        let cause = self
            .inner
            .cause
            .get()
            .expect("the marker is written only after a trip");
        let outcome = write_marker_file(&self.inner.marker, cause, &*self.inner.sync);
        if let Err(e) = &outcome {
            tracing::error!(
                marker = %self.inner.marker.display(),
                error = %e,
                "cannot write the divergence marker: a restart of this node may not be refused"
            );
        }
        let mut state = self.lock_state();
        *state = match outcome {
            Ok(()) => MarkerSlot::Written,
            Err(e) => MarkerSlot::Failed(Arc::new(e)),
        };
        self.inner.settled.notify_all();
    }

    fn lock_state(&self) -> MutexGuard<'_, MarkerSlot> {
        // A panic while holding the lock leaves a value still meaningful:
        // the slot is replaced whole.
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The marker's outcome if it has settled, without waiting: for a
    /// thread that polls rather than blocks.
    pub fn marker_settled(&self) -> Option<MarkerOutcome> {
        settled_outcome(&self.lock_state())
    }

    /// Wait up to `bound` for the marker to settle.
    pub fn wait_marker(&self, bound: Duration) -> MarkerOutcome {
        let deadline = Instant::now() + bound;
        let mut state = self.lock_state();
        loop {
            if let Some(outcome) = settled_outcome(&state) {
                return outcome;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return MarkerOutcome::Unconfirmed;
            }
            state = self
                .inner
                .settled
                .wait_timeout(state, left)
                .map(|(guard, _)| guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner().0);
        }
    }
}

fn settled_outcome(state: &MarkerSlot) -> Option<MarkerOutcome> {
    match state {
        MarkerSlot::Pending => None,
        MarkerSlot::Written => Some(MarkerOutcome::Written),
        MarkerSlot::Failed(e) => Some(MarkerOutcome::Failed(Arc::clone(e))),
    }
}

impl Trip for DivergenceLatch {
    type Token = Halted;

    fn trip(&self, divergence: Divergence) -> Option<Halted> {
        self.trip_with(HaltCause::Detected(divergence))
    }
}

/// Where the operator is sent from every line and message about the stop.
const SEE_DOCS: &str = "see docs/replication.md, Output verification";

/// The stop's one `error!` line.
fn log_cause(cause: &HaltCause) {
    match cause {
        HaltCause::Detected(d) => {
            let (from, through) = d.positions();
            tracing::error!(
                from = from.get(),
                through = through.get(),
                expected = ?d.expected,
                actual = ?d.actual,
                run_from = d.run_from.get(),
                "this node's output diverged from its primary's between these positions: the \
                 cluster is stopping: {SEE_DOCS}"
            );
        }
        HaltCause::ClusterHalted { by, via } => tracing::error!(
            by = %by,
            via = %via,
            "told that the cluster's output diverged: the cluster is stopping: {SEE_DOCS}"
        ),
    }
}

// ---------------------------------------------------------------------------
// The marker
// ---------------------------------------------------------------------------

/// The marker's first line: names the file for whoever opens it.
const MARKER_HEADER: &str = "melin output divergence marker";
/// The marker's format version, the second line.
const MARKER_VERSION: u32 = 1;

/// What a marker records, as the startup refusal reads it back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkerRecord {
    /// When the node stopped, in seconds since the Unix epoch.
    pub stopped_at_unix: u64,
    pub cause: MarkerCause,
}

/// The cause a marker records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MarkerCause {
    /// Detected here: the first differing report lies between these
    /// positions, both included.
    Detected { from: u64, through: u64 },
    /// Told by another node: who, and how, as the stop logged them.
    Told { by: String, via: String },
}

/// The marker's text for `cause`, stopped at `stopped_at_unix`.
fn marker_text(cause: &HaltCause, stopped_at_unix: u64) -> String {
    let mut text =
        format!("{MARKER_HEADER}\nversion: {MARKER_VERSION}\nstopped_at_unix: {stopped_at_unix}\n");
    match cause {
        HaltCause::Detected(d) => {
            let (from, through) = d.positions();
            text.push_str(&format!(
                "cause: detected\nfrom: {}\nthrough: {}\nrun_from: {}\nexpected: {:?}\nactual: {:?}\n",
                from.get(),
                through.get(),
                d.run_from.get(),
                d.expected,
                d.actual,
            ));
        }
        HaltCause::ClusterHalted { by, via } => {
            text.push_str(&format!("cause: told\nby: {by}\nvia: {via}\n"));
        }
    }
    text
}

/// Parse a marker's text, or `None` if it is not one this build wrote.
pub fn parse_marker(text: &str) -> Option<MarkerRecord> {
    let mut lines = text.lines();
    if lines.next()? != MARKER_HEADER {
        return None;
    }
    // `BTreeMap`: a handful of fields, read by name.
    let fields: std::collections::BTreeMap<&str, &str> =
        lines.filter_map(|line| line.split_once(": ")).collect();
    if fields.get("version")?.parse::<u32>().ok()? != MARKER_VERSION {
        return None;
    }
    let stopped_at_unix = fields.get("stopped_at_unix")?.parse().ok()?;
    let cause = match *fields.get("cause")? {
        "detected" => MarkerCause::Detected {
            from: fields.get("from")?.parse().ok()?,
            through: fields.get("through")?.parse().ok()?,
        },
        "told" => MarkerCause::Told {
            by: (*fields.get("by")?).to_owned(),
            via: (*fields.get("via")?).to_owned(),
        },
        _ => return None,
    };
    Some(MarkerRecord {
        stopped_at_unix,
        cause,
    })
}

/// Write the marker durably: to a temporary file beside it, synced,
/// renamed over it, and the directory synced.
fn write_marker_file(marker: &Path, cause: &HaltCause, sync: &dyn MarkerSync) -> io::Result<()> {
    let stopped_at_unix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        // A clock before 1970 is recorded as 0 rather than not at all: the
        // marker's purpose is the refusal, not the time.
        .unwrap_or(0);
    let tmp = crate::snapshot::with_suffix(marker, ".tmp");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(marker_text(cause, stopped_at_unix).as_bytes())?;
        sync.sync_file(&file)?;
    }
    std::fs::rename(&tmp, marker)?;
    sync.sync_dir(parent_dir(marker))
}

/// Test-only: trip a running node's latch from outside it, as
/// `melin-test-node` does until something in production detects. Latches
/// register themselves by marker path when this feature is on.
#[cfg(feature = "test-utils")]
pub mod test_seam {
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};
    use std::sync::{Mutex, OnceLock, Weak};

    use super::{DivergenceLatch, HaltCause, Halted, Inner};

    /// `BTreeMap` by marker path: a handful of nodes per test process,
    /// looked up by path. `Weak`, so a stopped node's latch is not kept.
    type Registry = Mutex<BTreeMap<PathBuf, Weak<Inner>>>;

    fn registry() -> &'static Registry {
        static REGISTRY: OnceLock<Registry> = OnceLock::new();
        REGISTRY.get_or_init(Default::default)
    }

    pub(super) fn register(latch: &DivergenceLatch) {
        registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                latch.inner.marker.clone(),
                std::sync::Arc::downgrade(&latch.inner),
            );
    }

    /// The latch of the running node whose marker is `marker`, if any.
    pub fn latch(marker: &Path) -> Option<DivergenceLatch> {
        registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(marker)
            .and_then(Weak::upgrade)
            .map(|inner| DivergenceLatch { inner })
    }

    /// Trip the running node whose marker is `marker` with `cause`.
    /// `None` if no such node runs, or it had stopped already.
    pub fn trip(marker: &Path, cause: HaltCause) -> Option<Halted> {
        latch(marker)?.trip_with(cause)
    }
}

/// Test-only disks for the marker writer: one that fails its fsync, one
/// that hangs in it until released. Shared by this module's tests and the
/// runtime's.
#[cfg(any(test, feature = "test-utils"))]
pub mod test_sync {
    use std::fs::File;
    use std::io;
    use std::path::Path;
    use std::sync::{Arc, Barrier};

    use super::MarkerSync;

    /// A failing disk: the marker's fsync returns an error.
    pub struct Failing;

    impl MarkerSync for Failing {
        fn sync_file(&self, _: &File) -> io::Result<()> {
            Err(io::Error::other("injected"))
        }
        fn sync_dir(&self, _: &Path) -> io::Result<()> {
            Ok(())
        }
    }

    /// A disk that hangs in the marker's fsync until the barrier is
    /// passed: the marker thread waits there, never the tripping thread.
    pub struct Hanging(pub Arc<Barrier>);

    impl MarkerSync for Hanging {
        fn sync_file(&self, _: &File) -> io::Result<()> {
            self.0.wait();
            Ok(())
        }
        fn sync_dir(&self, _: &Path) -> io::Result<()> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;

    use melin_app::WireSeq;

    use super::test_sync::{Failing, Hanging};
    use super::*;
    use crate::output_digest::ChainValue;

    fn divergence() -> Divergence {
        Divergence {
            end: WireSeq::new(192),
            run_from: WireSeq::new(65),
            last_match: Some(WireSeq::new(128)),
            expected: ChainValue::from_bytes([0xAA; 16]),
            actual: ChainValue::from_bytes([0xBB; 16]),
        }
    }

    struct Node {
        latch: DivergenceLatch,
        fence: Arc<FenceState>,
        shutdown: Arc<AtomicBool>,
        _dir: tempfile::TempDir,
    }

    fn node(sync: Arc<dyn MarkerSync>) -> Node {
        let dir = tempfile::tempdir().expect("tempdir");
        let fence = Arc::new(FenceState::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let latch = DivergenceLatch::new(
            Arc::clone(&fence),
            Arc::clone(&shutdown),
            dir.path().join("node.journal.diverged"),
            sync,
        );
        Node {
            latch,
            fence,
            shutdown,
            _dir: dir,
        }
    }

    /// The first trip wins: it fences the node and sets its shutdown flag
    /// before returning the token, and a second trip, whatever its cause,
    /// changes nothing.
    #[test]
    fn the_first_trip_wins_and_stops_the_node() {
        let n = node(Arc::new(FsSync));
        assert!(!n.latch.is_tripped());
        let halted = n.latch.trip(divergence());
        assert!(halted.is_some());
        assert!(n.latch.is_tripped());
        assert!(n.fence.is_fenced());
        assert!(n.shutdown.load(Ordering::Relaxed));
        let second = n.latch.trip_with(HaltCause::ClusterHalted {
            by: HaltedBy::Primary,
            via: NoticePath::HaltFrame,
        });
        assert!(second.is_none());
        assert_eq!(n.latch.cause(), Some(&HaltCause::Detected(divergence())));
    }

    /// The marker lands durably and reads back with every field.
    #[test]
    fn the_marker_is_written_and_parses_back() {
        let n = node(Arc::new(FsSync));
        let _halted = n.latch.trip(divergence());
        assert!(matches!(
            n.latch.wait_marker(HALT_NOTICE_BOUND),
            MarkerOutcome::Written
        ));
        let text = std::fs::read_to_string(n.latch.marker()).expect("the marker");
        let record = parse_marker(&text).expect("parses");
        assert_eq!(
            record.cause,
            MarkerCause::Detected {
                from: 129,
                through: 192
            }
        );
        assert!(record.stopped_at_unix > 0);
        assert!(text.contains(&format!("{:?}", ChainValue::from_bytes([0xAA; 16]))));
    }

    #[test]
    fn a_told_marker_parses_back() {
        let n = node(Arc::new(FsSync));
        let _halted = n.latch.trip_with(HaltCause::ClusterHalted {
            by: HaltedBy::Replica { slot: 1 },
            via: NoticePath::Ack,
        });
        assert!(matches!(
            n.latch.wait_marker(HALT_NOTICE_BOUND),
            MarkerOutcome::Written
        ));
        let record = parse_marker(&std::fs::read_to_string(n.latch.marker()).expect("marker"))
            .expect("parses");
        assert_eq!(
            record.cause,
            MarkerCause::Told {
                by: "the replica on slot 1".into(),
                via: "its acknowledgement".into()
            }
        );
    }

    #[test]
    fn text_that_is_not_a_marker_does_not_parse() {
        assert_eq!(parse_marker(""), None);
        assert_eq!(parse_marker("something else\n"), None);
        assert_eq!(
            parse_marker(&format!("{MARKER_HEADER}\nversion: 99\n")),
            None
        );
    }

    #[test]
    fn a_failed_write_is_reported_to_the_waiter() {
        let n = node(Arc::new(Failing));
        let _halted = n.latch.trip(divergence());
        assert!(matches!(
            n.latch.wait_marker(HALT_NOTICE_BOUND),
            MarkerOutcome::Failed(_)
        ));
    }

    /// A hung marker write neither holds the tripping thread nor a waiter
    /// past its bound: the trip returns at once, the waiter at its bound
    /// with the outcome unconfirmed, and a poll finds nothing settled.
    #[test]
    fn a_hung_write_holds_no_one_past_the_bound() {
        let barrier = Arc::new(Barrier::new(2));
        let n = node(Arc::new(Hanging(Arc::clone(&barrier))));
        let _halted = n.latch.trip(divergence());
        assert!(n.latch.marker_settled().is_none());
        let started = Instant::now();
        let bound = Duration::from_millis(50);
        assert!(matches!(
            n.latch.wait_marker(bound),
            MarkerOutcome::Unconfirmed
        ));
        assert!(started.elapsed() >= bound);
        // Release the writer: the marker then lands.
        barrier.wait();
        assert!(matches!(
            n.latch.wait_marker(HALT_NOTICE_BOUND),
            MarkerOutcome::Written
        ));
    }

    #[test]
    fn the_range_runs_from_after_the_last_match_or_from_the_run_start() {
        let d = divergence();
        assert_eq!(d.positions(), (WireSeq::new(129), WireSeq::new(192)));
        let unmatched = Divergence {
            last_match: None,
            ..d
        };
        assert_eq!(unmatched.positions(), (WireSeq::new(65), WireSeq::new(192)));
    }
}
