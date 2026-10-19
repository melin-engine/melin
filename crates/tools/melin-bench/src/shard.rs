//! A run split across threads: which clients each thread drives, what the
//! threads share while they run, and the threads themselves.
//!
//! Every thread (a shard) connects and authenticates its own clients, then
//! meets the others; the last to arrive starts the run's clock, so every
//! shard warms up, measures and cools down over the same window, and their
//! histograms add up. A shard that fails records the run's error and stops
//! the rest, wherever they are: still setting up, waiting for the others,
//! or loading the node.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::app::ClientId;

/// How a run's clients are dealt to its threads: at least one thread, and
/// no more threads than clients, checked once here so that nothing past it
/// handles a thread with no client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shards {
    threads: usize,
    clients: usize,
}

impl Shards {
    /// `threads` threads for `clients` clients. Refused rather than
    /// clamped: a run must never drive fewer threads than its results
    /// say.
    pub fn new(threads: usize, clients: usize) -> Result<Self, String> {
        if threads == 0 || threads > clients {
            return Err(format!(
                "--threads {threads}: a run drives 1 to --clients ({clients}) threads"
            ));
        }
        // Every client's number fits a request's fingerprint.
        if u32::try_from(clients - 1).is_err() {
            return Err(format!(
                "--clients {clients}: at most {}",
                u64::from(u32::MAX) + 1
            ));
        }
        Ok(Self { threads, clients })
    }

    pub fn threads(self) -> usize {
        self.threads
    }

    /// Each thread's clients, round-robin: client `k` to thread
    /// `k % threads`, numbered across the whole run.
    pub fn deal(self) -> Vec<Vec<ClientId>> {
        (0..self.threads)
            .map(|shard| {
                (shard..self.clients)
                    .step_by(self.threads)
                    // Checked in `new`: every index fits.
                    .map(|k| ClientId(k as u32))
                    .collect()
            })
            .collect()
    }
}

/// Proof that a shard stopped short and the run's error is recorded: made
/// only by [`Shared::fail`] and [`Shared::stopped`], so a shard cannot
/// stop without a reason for the run to report.
#[derive(Debug)]
pub struct Stopped(());

/// What a run's shards share.
///
/// Read on the hot path: `aborted`, once per pass, and never written while
/// the run goes well, so its cache line stays shared by every core.
#[derive(Debug)]
pub struct Shared {
    shards: usize,
    /// Shards that have authenticated their clients.
    arrived: AtomicUsize,
    /// The run's start on the shards' common clock, set by the last shard
    /// to arrive.
    start_ns: OnceLock<u64>,
    /// Set once `error` is: a shard has failed, and every other stops.
    aborted: AtomicBool,
    /// The first failure, which is the run's: later ones follow from it.
    error: OnceLock<String>,
}

impl Shared {
    /// For `shards` threads. Private: [`run`] makes it from the same
    /// count as the threads it starts, as a rendezvous expecting another
    /// count would wait for a thread that never comes, or start without
    /// one.
    fn new(shards: usize) -> Self {
        Self {
            shards,
            arrived: AtomicUsize::new(0),
            start_ns: OnceLock::new(),
            aborted: AtomicBool::new(false),
            error: OnceLock::new(),
        }
    }

    /// Record `error` as the run's, unless another shard's came first, and
    /// stop every shard.
    pub fn fail(&self, error: String) -> Stopped {
        // A later failure was caused or raced by the first: the run
        // reports the first.
        self.error.get_or_init(|| error);
        self.aborted.store(true, Ordering::Release);
        Stopped(())
    }

    /// Whether a shard has failed, as the proof a shard needs to stop.
    /// Acquire, pairing with `fail`'s release: a shard that sees the flag
    /// sees the error recorded before it.
    pub fn stopped(&self) -> Option<Stopped> {
        self.aborted.load(Ordering::Acquire).then_some(Stopped(()))
    }

    /// Wait for every shard to arrive, and return the run's start: the time
    /// `now_ns` reads for the last of them. A shard that fails never
    /// arrives, and the others stop waiting for it.
    pub fn meet(&self, now_ns: impl FnOnce() -> u64) -> Result<u64, Stopped> {
        if self.arrived.fetch_add(1, Ordering::AcqRel) + 1 == self.shards {
            return Ok(*self.start_ns.get_or_init(now_ns));
        }
        loop {
            if let Some(&start_ns) = self.start_ns.get() {
                return Ok(start_ns);
            }
            if let Some(stopped) = self.stopped() {
                return Err(stopped);
            }
            // Only while the slowest shard finishes its handshakes, before
            // any load: yielding leaves its core to it.
            std::thread::yield_now();
        }
    }

    /// The run's error, which `stopped` proves was recorded.
    fn error(&self, _stopped: Stopped) -> String {
        self.error
            .get()
            .cloned()
            .expect("a shard stops only once the run's error is recorded")
    }
}

/// Run `shard` once per thread of `shards`, with the thread's index, its
/// clients and what the threads share: on this thread when there is one,
/// so a single-threaded run is today's, and the DPDK EAL is initialised on
/// the thread that started the process; on a thread each otherwise, all of
/// them joined before this returns. Every thread's result, in no
/// particular order, or the run's error: the first failure of any thread.
/// A thread that panics fails the run like any other failure (its panic
/// printed as the default hook prints it), and the others stop.
pub fn run<T, F>(shards: Shards, shard: F) -> Result<Vec<T>, String>
where
    T: Send,
    F: Fn(usize, Vec<ClientId>, &Shared) -> Result<T, Stopped> + Sync,
{
    let shared = Shared::new(shards.threads());
    let results = start(&shared, shards.deal(), shard);
    results
        .into_iter()
        .map(|result| result.map_err(|stopped| shared.error(stopped)))
        .collect()
}

/// [`run`]'s threads, each result as it came.
fn start<T, F>(shared: &Shared, ids: Vec<Vec<ClientId>>, shard: F) -> Vec<Result<T, Stopped>>
where
    T: Send,
    F: Fn(usize, Vec<ClientId>, &Shared) -> Result<T, Stopped> + Sync,
{
    if ids.len() == 1 {
        return ids
            .into_iter()
            .map(|ids| guarded(shared, 0, || shard(0, ids, shared)))
            .collect();
    }
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(ids.len());
        let mut results = Vec::with_capacity(ids.len());
        for (index, ids) in ids.into_iter().enumerate() {
            let shard = &shard;
            let spawned = std::thread::Builder::new()
                .name(format!("load-{index}"))
                .spawn_scoped(scope, move || {
                    guarded(shared, index, || shard(index, ids, shared))
                });
            match spawned {
                Ok(handle) => handles.push(handle),
                Err(e) => {
                    // The shards already running stop waiting for this one.
                    results.push(Err(
                        shared.fail(format!("cannot start load thread {index}: {e}"))
                    ));
                    break;
                }
            }
        }
        for (index, handle) in handles.into_iter().enumerate() {
            // `guarded` caught the shard's own panic; this is one that
            // escaped it (a panic while dropping the first one's payload).
            results.push(
                handle
                    .join()
                    .unwrap_or_else(|_| Err(shared.fail(format!("load thread {index} panicked")))),
            );
        }
        results
    })
}

/// `shard`, its panic caught on its own thread and recorded as the run's
/// failure at once: caught only at the join, it would leave the other
/// shards waiting for it to arrive.
fn guarded<T>(
    shared: &Shared,
    index: usize,
    shard: impl FnOnce() -> Result<T, Stopped>,
) -> Result<T, Stopped> {
    // Unwind-safe as used: a shard that panicked is never looked at again,
    // and what it shares with the others is atomics and once-set cells.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(shard)) {
        Ok(result) => result,
        Err(payload) => {
            let why = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("a panic with no message");
            Err(shared.fail(format!("load thread {index} panicked: {why}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clients_are_dealt_round_robin_with_their_run_wide_numbers() {
        let ids = Shards::new(3, 7).unwrap().deal();
        let numbers: Vec<Vec<u32>> = ids
            .iter()
            .map(|shard| shard.iter().map(|id| id.0).collect())
            .collect();
        assert_eq!(numbers, [vec![0, 3, 6], vec![1, 4], vec![2, 5]]);
        assert_eq!(
            Shards::new(1, 2).unwrap().deal(),
            [vec![ClientId(0), ClientId(1)]]
        );
        assert_eq!(
            Shards::new(2, 2).unwrap().deal(),
            [vec![ClientId(0)], vec![ClientId(1)]]
        );
    }

    #[test]
    fn a_thread_count_outside_one_to_the_clients_is_refused() {
        for (threads, clients) in [(0, 4), (5, 4), (2, 1)] {
            let err = Shards::new(threads, clients).unwrap_err();
            assert!(err.contains(&format!("--threads {threads}")), "{err}");
        }
        assert_eq!(Shards::new(4, 4).unwrap().threads(), 4);
    }

    #[test]
    fn every_shard_starts_from_one_shard_s_clock() {
        let shared = Shared::new(3);
        let starts: Vec<u64> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..3u64)
                .map(|n| {
                    let shared = &shared;
                    scope.spawn(move || shared.meet(|| 100 + n).unwrap())
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(starts.iter().all(|&s| s == starts[0]), "{starts:?}");
        assert!((100..103).contains(&starts[0]));
    }

    #[test]
    fn a_shard_that_fails_before_arriving_releases_the_others() {
        let shared = Shared::new(2);
        std::thread::scope(|scope| {
            let waiting = scope.spawn(|| shared.meet(|| 0));
            std::thread::sleep(std::time::Duration::from_millis(20));
            let stopped = shared.fail("handshake refused".into());
            assert!(waiting.join().unwrap().is_err(), "released, not started");
            assert_eq!(shared.error(stopped), "handshake refused");
        });
    }

    #[test]
    fn the_first_failure_is_the_run_s_error() {
        let shared = Shared::new(2);
        assert!(shared.stopped().is_none());
        let first = shared.fail("first".into());
        let second = shared.fail("second".into());
        assert!(shared.stopped().is_some());
        assert_eq!(shared.error(first), "first");
        assert_eq!(shared.error(second), "first");
    }

    #[test]
    fn a_panicking_shard_fails_the_run_and_the_others_return() {
        let err = run(Shards::new(2, 2).unwrap(), |index, _, shared| {
            if index == 1 {
                panic!("a bug in shard 1");
            }
            // The healthy shard waits for the other, which never arrives.
            shared.meet(|| 0).map(|_| ())
        })
        .unwrap_err();
        assert_eq!(err, "load thread 1 panicked: a bug in shard 1");
    }

    #[test]
    fn every_shard_s_result_comes_back() {
        let mut results = run(Shards::new(3, 5).unwrap(), |index, ids, shared| {
            shared.meet(|| 0)?;
            Ok((index, ids.len()))
        })
        .unwrap();
        results.sort();
        assert_eq!(results, [(0, 2), (1, 2), (2, 1)]);
    }
}
