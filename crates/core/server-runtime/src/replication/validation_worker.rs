//! Long-lived handshake chain-validation worker.
//!
//! Validating a replica's handshake against the local journal can scan a
//! full segment per attempt and sleeps between retries (~400 ms budget on
//! a divergent verdict), so it must not run inline on a sender's poll
//! loop. The obvious shape — spawn a thread per handshake — is a trap on
//! the DPDK path, where the sender *is* the client poll thread: pinned to
//! an isolated core at `SCHED_FIFO` and busy-spinning without ever
//! blocking.
//!
//! A thread inherits its creator's affinity mask and scheduling policy at
//! creation, and Linux offers no way to set another thread's affinity
//! before it is first scheduled. A child of that poll thread therefore
//! starts life pinned to the one core its real-time parent never yields,
//! at equal `SCHED_FIFO` priority — so it is never scheduled, and cannot
//! move itself, because moving itself requires running. It does not even
//! reach `Builder::name`'s `prctl`, so it shows up in `/proc` under the
//! process name rather than the name it was given. The handshake then
//! hangs forever: the verdict channel stays `Empty`, the slot stays
//! `Handshaking`, and the replica waits for a `StreamStart` that is never
//! sent. See [`melin_app::affinity::prepare_child_context`].
//!
//! The fix is to take thread creation off the handshake path entirely.
//! The worker is spawned once, at driver construction — which happens on
//! the startup thread, before the poll thread pins itself — and parks on
//! a channel. Handshake-time cost drops to one channel send. The spawn is
//! still wrapped in the prepare/restore dance so the worker lands
//! unpinned under `SCHED_OTHER` regardless of who constructs the driver.

use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;

use melin_transport_core::replication::protocol::Handshake;
use melin_transport_core::replication::validate::HandshakeValidation;

/// One validation job handed to the worker.
struct ValidationJob {
    handshake: Handshake,
    /// One-shot verdict channel back to the slot. The slot may drop its
    /// receiver (teardown mid-validation), which is why the worker
    /// ignores send failures.
    verdict_tx: Sender<io::Result<HandshakeValidation>>,
}

/// Handle to a parked validation thread, owned by the sender that feeds it.
///
/// One worker per replica slot: validations for the two slots stay
/// independent, exactly as they were when each handshake got its own
/// thread, so a slow verdict on one slot cannot delay the other.
pub(crate) struct ValidationWorker {
    /// Job queue. `Option` so [`Drop`] can close the channel — which is
    /// what tells the worker to exit — before joining it.
    ///
    /// An unbounded `mpsc::Sender` rather than a `SyncSender`: the only
    /// producer is the poll thread, which must never block.
    ///
    /// Depth is bounded by the protocol — the slot submits only when it
    /// has no verdict outstanding, so a queue forms only when a slot is
    /// torn down mid-validation and its replacement handshakes before the
    /// abandoned job finishes. That job's verdict goes nowhere, but the
    /// new one waits behind it, where a per-handshake thread would have
    /// run alongside. The wait is one retry budget, and only on the
    /// divergent path (a healthy chain settles on the first attempt) —
    /// cheap next to putting thread creation back on the poll thread.
    jobs: Option<Sender<ValidationJob>>,
    handle: Option<JoinHandle<()>>,
}

impl ValidationWorker {
    /// Spawn the worker for one slot. `journal_path` is fixed for the
    /// driver's lifetime, so it lives on the worker rather than being
    /// cloned into every job — keeping the handshake path allocation-free
    /// apart from the job itself.
    ///
    /// `validate` is a parameter rather than a direct call to
    /// `validate_replica_handshake_settled` so the worker's mechanics can
    /// be tested without building a journal on disk.
    pub(crate) fn spawn<F>(name: String, journal_path: PathBuf, validate: F) -> io::Result<Self>
    where
        F: Fn(&std::path::Path, &Handshake) -> io::Result<HandshakeValidation> + Send + 'static,
    {
        let (jobs_tx, jobs_rx) = channel::<ValidationJob>();

        // Hand the worker its scheduling context before it exists — see
        // the module docs. `0` is the "do not pin" sentinel: this thread
        // sleeps between retries and does blocking file I/O, so it wants
        // the whole machine to float on, not a reserved core.
        let saved = melin_app::affinity::take_context();
        if let Err(ref e) = saved {
            tracing::warn!(worker = %name, error = %e, "cannot snapshot scheduling context");
        }
        if let Err(e) = melin_app::affinity::prepare_child_context(0) {
            tracing::warn!(worker = %name, error = %e, "cannot prepare child context");
        }
        let spawned = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || worker_loop(journal_path, jobs_rx, validate));
        // Restore before propagating a spawn failure: a failed spawn must
        // not strand the caller on the child's (unpinned) context.
        if let Ok(ctx) = saved
            && let Err(e) = melin_app::affinity::restore_context(&ctx)
        {
            tracing::error!(worker = %name, error = %e, "caller could not restore its own affinity");
        }

        Ok(ValidationWorker {
            jobs: Some(jobs_tx),
            handle: Some(spawned?),
        })
    }

    /// Queue a handshake for validation and hand back the one-shot
    /// verdict channel to poll.
    ///
    /// Returns `Err` only if the worker thread is gone (it panicked), in
    /// which case the caller should drop the replica — the same response
    /// the per-handshake spawn gave when thread creation failed.
    pub(crate) fn submit(
        &self,
        handshake: Handshake,
    ) -> Result<Receiver<io::Result<HandshakeValidation>>, WorkerGone> {
        let (verdict_tx, verdict_rx) = channel();
        let jobs = self.jobs.as_ref().ok_or(WorkerGone)?;
        jobs.send(ValidationJob {
            handshake,
            verdict_tx,
        })
        .map_err(|_| WorkerGone)?;
        Ok(verdict_rx)
    }
}

impl Drop for ValidationWorker {
    fn drop(&mut self) {
        // Close the queue first — that is the worker's exit signal — then
        // join so no validation is still reading the journal after the
        // driver that owns it is gone. The wait is bounded: a single
        // validation is capped by its own retry budget.
        self.jobs = None;
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            tracing::warn!("handshake validation worker panicked");
        }
    }
}

/// The worker thread is gone (it panicked); no verdict will ever arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WorkerGone;

impl std::fmt::Display for WorkerGone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("handshake validation worker is no longer running")
    }
}

fn worker_loop<F>(journal_path: PathBuf, jobs: Receiver<ValidationJob>, validate: F)
where
    F: Fn(&std::path::Path, &Handshake) -> io::Result<HandshakeValidation>,
{
    // Ends when the owning worker handle drops the sender.
    for job in jobs {
        let verdict = validate(&journal_path, &job.handshake);
        // Send failure means the slot was torn down while validating —
        // nobody is waiting for the verdict.
        let _ = job.verdict_tx.send(verdict);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::TryRecvError;

    fn handshake(last_sequence: u64) -> Handshake {
        Handshake {
            last_sequence,
            chain_hash: [0u8; 32],
            epoch: 1,
        }
    }

    /// Verdicts come back on the one-shot channel the submission returned.
    #[test]
    fn a_submitted_handshake_gets_its_verdict() {
        let worker = ValidationWorker::spawn("test-validate".into(), PathBuf::from("/journal"), {
            |path: &std::path::Path, hs: &Handshake| {
                assert_eq!(path, std::path::Path::new("/journal"));
                assert_eq!(hs.last_sequence, 42);
                Ok(HandshakeValidation::Ok)
            }
        })
        .unwrap();

        let rx = worker.submit(handshake(42)).unwrap();
        assert_eq!(
            rx.recv().unwrap().unwrap(),
            HandshakeValidation::Ok,
            "the verdict should reach the submitter"
        );
    }

    /// The whole point of the fix: successive handshakes reuse one parked
    /// thread instead of spawning a fresh one on the poll thread.
    #[test]
    fn successive_handshakes_run_on_the_same_thread() {
        let ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = Arc::clone(&ids);
        let worker = ValidationWorker::spawn(
            "test-validate".into(),
            PathBuf::from("/journal"),
            move |_p: &std::path::Path, _h: &Handshake| {
                seen.lock()
                    .expect("test mutex poisoned")
                    .push(std::thread::current().id());
                Ok(HandshakeValidation::Ok)
            },
        )
        .unwrap();

        for seq in 0..4u64 {
            let rx = worker.submit(handshake(seq)).unwrap();
            assert_eq!(rx.recv().unwrap().unwrap(), HandshakeValidation::Ok);
        }

        let ids = ids.lock().expect("test mutex poisoned");
        assert_eq!(ids.len(), 4);
        assert!(
            ids.windows(2).all(|w| w[0] == w[1]),
            "every validation must run on the one long-lived worker thread, saw {ids:?}"
        );
    }

    /// A slot torn down mid-validation drops its verdict receiver. The
    /// worker must shrug that off and keep serving — the per-handshake
    /// thread died there, so nothing noticed.
    #[test]
    fn a_dropped_verdict_receiver_does_not_kill_the_worker() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&calls);
        let worker = ValidationWorker::spawn(
            "test-validate".into(),
            PathBuf::from("/journal"),
            move |_p: &std::path::Path, _h: &Handshake| {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(HandshakeValidation::Ok)
            },
        )
        .unwrap();

        drop(worker.submit(handshake(1)).unwrap());

        let rx = worker.submit(handshake(2)).unwrap();
        assert_eq!(rx.recv().unwrap().unwrap(), HandshakeValidation::Ok);
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "the abandoned job must still have been serviced"
        );
    }

    /// An I/O error is delivered as a verdict rather than killing the
    /// worker — the slot's `Err` arm needs it to disconnect cleanly.
    #[test]
    fn an_io_error_is_delivered_as_a_verdict() {
        let worker = ValidationWorker::spawn(
            "test-validate".into(),
            PathBuf::from("/journal"),
            |_p: &std::path::Path, _h: &Handshake| {
                Err(io::Error::new(io::ErrorKind::NotFound, "no segment"))
            },
        )
        .unwrap();

        let rx = worker.submit(handshake(7)).unwrap();
        assert_eq!(
            rx.recv().unwrap().unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    /// The verdict is genuinely asynchronous: submission returns before
    /// the validation finishes, which is what keeps the poll thread free.
    #[test]
    fn submission_does_not_wait_for_the_verdict() {
        let (release_tx, release_rx) = channel::<()>();
        let gate = std::sync::Mutex::new(release_rx);
        let worker = ValidationWorker::spawn(
            "test-validate".into(),
            PathBuf::from("/journal"),
            move |_p: &std::path::Path, _h: &Handshake| {
                gate.lock()
                    .expect("test mutex poisoned")
                    .recv()
                    .expect("release channel closed");
                Ok(HandshakeValidation::Ok)
            },
        )
        .unwrap();

        let rx = worker.submit(handshake(1)).unwrap();
        // `io::Error` is not `PartialEq`, so match rather than assert_eq.
        assert!(
            matches!(rx.try_recv(), Err(TryRecvError::Empty)),
            "the verdict must not be ready while the worker is still blocked"
        );
        release_tx.send(()).unwrap();
        assert_eq!(rx.recv().unwrap().unwrap(), HandshakeValidation::Ok);
    }

    /// The regression test for the reported hang: a worker spawned from a
    /// single-core-pinned parent must not inherit that mask, or it can
    /// never be scheduled against a busy-spinning parent.
    ///
    /// Only the affinity half of the trap is exercised — granting
    /// `SCHED_FIFO` needs `CAP_SYS_NICE`, which tests do not have. Pinning
    /// is the part that confines the child to the parent's core, so it is
    /// the part worth pinning down here.
    #[test]
    fn the_worker_does_not_inherit_a_pinned_parents_affinity() {
        let total = cpu_count_in_mask();
        if total < 2 {
            // Nothing to prove on a single-CPU runner: every mask is the
            // same mask. Not a skip of a failing assertion — the property
            // is vacuous here.
            return;
        }

        let saved = melin_app::affinity::take_context().expect("snapshot scheduling context");
        let core = first_cpu_in_mask().expect("at least one CPU in the mask");
        melin_app::affinity::pin_to_core(core).expect("pin the test thread");

        let observed = Arc::new(AtomicUsize::new(0));
        let sink = Arc::clone(&observed);
        let worker = ValidationWorker::spawn(
            "test-validate".into(),
            PathBuf::from("/journal"),
            move |_p: &std::path::Path, _h: &Handshake| {
                sink.store(cpu_count_in_mask(), Ordering::Relaxed);
                Ok(HandshakeValidation::Ok)
            },
        )
        .unwrap();
        let rx = worker.submit(handshake(1)).unwrap();
        let verdict = rx.recv().unwrap().unwrap();

        melin_app::affinity::restore_context(&saved).expect("restore scheduling context");
        assert_eq!(verdict, HandshakeValidation::Ok);
        assert_eq!(
            observed.load(Ordering::Relaxed),
            total,
            "the worker must run on the full CPU mask, not the parent's single core"
        );
    }

    /// CPUs the calling thread is allowed to run on.
    fn cpu_count_in_mask() -> usize {
        // SAFETY: `sched_getaffinity` only writes into the zeroed
        // `cpu_set_t` we hand it, sized by `size_of`.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
                return 0;
            }
            (0..libc::CPU_SETSIZE as usize)
                .filter(|&i| libc::CPU_ISSET(i, &set))
                .count()
        }
    }

    /// Lowest CPU the calling thread may run on — a core it is already
    /// allowed to use, so pinning to it cannot fail on a restricted runner.
    fn first_cpu_in_mask() -> Option<usize> {
        // SAFETY: same contract as `cpu_count_in_mask`.
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) != 0 {
                return None;
            }
            (0..libc::CPU_SETSIZE as usize).find(|&i| libc::CPU_ISSET(i, &set))
        }
    }
}
