//! A node thread that sets itself up, says whether it could, and then
//! runs until the node's shutdown flag is set: the replication poll thread
//! is one (see `crate::dpdk_repl_poll`).
//!
//! What it adds to a bare `std::thread`, and why: its setup's error comes
//! back to the spawner, which refuses to start the node with it; a panic on
//! it sets the shutdown flag, so the node does not go on without it; and
//! dropped without being joined (a boot failing after it is up), it sets
//! the shutdown flag and joins it, so no thread is left running behind the
//! boot's error.
//!
//! Ungated, though only the DPDK build spawns one: it needs no NIC, and so
//! is tested on every host.

// Without `dpdk` nothing but the tests spawns one: the DPDK node is gated.
#![cfg_attr(not(feature = "dpdk"), allow(dead_code))]

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;

/// A running reporting thread; see the module docs.
///
/// [`join`](Self::join) it once the node is shutting down. Dropped without
/// being joined, it sets the node's shutdown flag and joins the thread.
pub(crate) struct ReportingThread {
    /// Always `Some` until [`join`](Self::join) or the drop takes it.
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl ReportingThread {
    /// Wait for the thread to exit, which it does once the node's shutdown
    /// flag is set.
    pub(crate) fn join(mut self) -> std::thread::Result<()> {
        self.handle.take().map_or(Ok(()), JoinHandle::join)
    }
}

impl Drop for ReportingThread {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.shutdown.store(true, Ordering::Relaxed);
            // Dropped result: this runs only on a boot already failing,
            // whose own error is the one returned; a panic in the thread
            // has been reported by the panic hook.
            let _ = handle.join();
        }
    }
}

/// How the thread reports its setup: once, with what the spawner gets
/// back, or with the error that kept it from running.
pub(crate) struct Ready<R>(mpsc::SyncSender<io::Result<R>>);

impl<R> Ready<R> {
    /// Report `result` to the spawner, which is waiting for it.
    pub(crate) fn report(self, result: Result<R, String>) {
        // Dropped result: the spawner waits until this is sent, so it is
        // there to take it.
        let _ = self.0.send(result.map_err(io::Error::other));
    }
}

/// Spawn thread `name`, placed on `core` (`0` = unpinned) by
/// [`melin_app::affinity::spawn_placed`], running `body` until `shutdown`
/// is set.
///
/// `body` reports through its [`Ready`] once it is set up, and returns
/// when it has nothing more to do (on `shutdown`, or straight after
/// reporting an error). Returns once it has reported: with the thread and
/// what it reported, or with its error, the thread then joined. A body
/// that returns or panics without reporting is an error too.
pub(crate) fn spawn<R, F>(
    name: &'static str,
    core: usize,
    shutdown: Arc<AtomicBool>,
    body: F,
) -> Result<(ReportingThread, R), String>
where
    R: Send + 'static,
    F: FnOnce(Ready<R>) + Send + 'static,
{
    // A rendezvous: the thread reports once, and waits for nothing.
    let (ready_tx, ready_rx) = mpsc::sync_channel::<io::Result<R>>(1);
    let thread_shutdown = Arc::clone(&shutdown);
    let handle = melin_app::affinity::spawn_placed(name, core, move || {
        let _stop_node_on_panic = StopNodeOnPanic(&thread_shutdown);
        body(Ready(ready_tx));
    })
    .map_err(|e| format!("spawn the {name} thread: {e}"))?;
    let thread = ReportingThread {
        handle: Some(handle),
        shutdown,
    };
    match crate::connection_limit::await_startup(name, &ready_rx) {
        Ok(reported) => Ok((thread, reported)),
        Err(e) => {
            // Dropped result: the thread has returned on its own, having
            // reported the error returned here, or panicked, which the
            // panic hook has reported; only the join is left.
            let _ = thread.join();
            Err(e.to_string())
        }
    }
}

/// Sets the node's shutdown flag if the thread unwinds: a thread the node
/// needs (the replication poll thread: without it, clients would be served
/// with no replication behind them) must not die and leave the rest
/// running. The panic is reported when the thread is joined.
struct StopNodeOnPanic<'a>(&'a AtomicBool);

impl Drop for StopNodeOnPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Generous: only a hang reaches it.
    const LIMIT: Duration = Duration::from_secs(10);

    /// The tests' threads are placed nowhere in particular.
    const UNPINNED: usize = 0;

    /// Spin until `flag` is set, failing after `LIMIT`.
    fn wait_for(flag: &AtomicBool) {
        let deadline = Instant::now() + LIMIT;
        while !flag.load(Ordering::Relaxed) {
            assert!(Instant::now() < deadline, "flag never set");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn a_thread_that_reports_runs_until_shutdown_and_leaves_the_flag_alone() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let ran_to_shutdown = Arc::new(AtomicBool::new(false));
        let thread_ran = Arc::clone(&ran_to_shutdown);
        let thread_shutdown = Arc::clone(&shutdown);
        let (thread, reported) = spawn("test-ok", UNPINNED, Arc::clone(&shutdown), move |ready| {
            ready.report(Ok(7_u32));
            wait_for(&thread_shutdown);
            thread_ran.store(true, Ordering::Relaxed);
        })
        .expect("the thread reported Ok");
        assert_eq!(reported, 7);
        assert!(!shutdown.load(Ordering::Relaxed), "nothing stops the node");

        shutdown.store(true, Ordering::Relaxed);
        thread.join().expect("a clean exit");
        assert!(ran_to_shutdown.load(Ordering::Relaxed));
    }

    #[test]
    fn a_setup_error_comes_back_with_the_thread_joined_and_the_node_not_stopped() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let err = spawn::<(), _>("test-err", UNPINNED, Arc::clone(&shutdown), |ready| {
            ready.report(Err("add replication listener: refused".into()));
        })
        .map(|_| ())
        .expect_err("the thread reported an error");
        assert_eq!(err, "add replication listener: refused");
        assert!(
            !shutdown.load(Ordering::Relaxed),
            "an error is the spawner's to act on, not a panic"
        );
    }

    #[test]
    fn a_thread_that_panics_before_reporting_is_an_error_and_stops_the_node() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let err = spawn::<(), _>(
            "test-early-panic",
            UNPINNED,
            Arc::clone(&shutdown),
            |_ready| {
                panic!("before the transport was up");
            },
        )
        .map(|_| ())
        .expect_err("no report");
        assert!(err.contains("test-early-panic"), "{err}");
        assert!(err.contains("exited before reporting"), "{err}");
        assert!(shutdown.load(Ordering::Relaxed), "the panic stops the node");
    }

    #[test]
    fn a_thread_that_returns_without_reporting_is_an_error() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let err = spawn::<(), _>("test-silent", UNPINNED, Arc::clone(&shutdown), drop)
            .map(|_| ())
            .expect_err("no report");
        assert!(err.contains("exited before reporting"), "{err}");
        assert!(!shutdown.load(Ordering::Relaxed), "no panic, nothing set");
    }

    #[test]
    fn a_panic_once_running_stops_the_node() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let (thread, ()) = spawn(
            "test-late-panic",
            UNPINNED,
            Arc::clone(&shutdown),
            |ready| {
                ready.report(Ok(()));
                panic!("the replication driver failed");
            },
        )
        .expect("the thread reported Ok");
        wait_for(&shutdown);
        assert!(thread.join().is_err(), "the panic comes out of the join");
    }

    #[test]
    fn dropping_a_running_thread_stops_and_joins_it() {
        let shutdown = Arc::new(AtomicBool::new(false));
        let exited = Arc::new(AtomicBool::new(false));
        let thread_exited = Arc::clone(&exited);
        let thread_shutdown = Arc::clone(&shutdown);
        let (thread, ()) = spawn("test-drop", UNPINNED, Arc::clone(&shutdown), move |ready| {
            ready.report(Ok(()));
            wait_for(&thread_shutdown);
            thread_exited.store(true, Ordering::Relaxed);
        })
        .expect("the thread reported Ok");
        drop(thread);
        assert!(shutdown.load(Ordering::Relaxed));
        assert!(exited.load(Ordering::Relaxed), "joined by the drop");
    }
}
