//! Long-lived join worker: the disk side of a replica's catch-up, off the
//! DPDK poll thread.
//!
//! Once a replica's handshake is validated, the primary must decide how
//! to bring it up to date (the catch-up probe), and then stream what it
//! lacks: journal history, or a snapshot (its pre-flight, the snapshot
//! file, the segment seed) followed by journal history. All of it is file
//! I/O whose duration depends on the disk and on how far behind the
//! replica is — a large snapshot on a cold disk takes seconds.
//!
//! On the DPDK primary the replication sender *is* the client poll
//! thread, and the poll thread is also the only thing that runs the TCP
//! stack. Doing that I/O inline stalls client traffic, the other replica's
//! stream, and every replication link's keep-alive answers at once — and
//! past the peers' liveness deadline (`melin_dpdk::PeerLiveness`) a
//! healthy replica resets its link to a live primary, which halts if that
//! leaves it no replica.
//!
//! So the I/O runs here, on a parked thread per replica slot, and only the
//! wire half stays on the poll thread: the worker encodes the frames the
//! join sends and hands them over a short bounded channel
//! ([`JOIN_FRAMES_IN_FLIGHT`]); the slot moves them into its socket a tick
//! at a time ([`JoinStream::pump`]), as TX space allows, between the poll
//! thread's other work. The frames, and their order, are exactly what the
//! inline join sent; only who reads the disk changed.
//!
//! The worker is spawned once, at driver construction, for the reason
//! the validation worker is (see `validation_worker`): a thread created
//! from the pinned, real-time poll thread would never be scheduled.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TryRecvError, channel, sync_channel};
use std::thread::JoinHandle;

use melin_app::AppEvent;
use melin_transport_core::fence::FenceState;
use melin_transport_core::replication::catchup::{
    CatchUpPublisher, CatchUpResult, can_catch_up_from_journal, catch_up_from_journal_with,
    lineage_origin, preflight_snapshot_transfer, snapshot_transfer_with,
};
use melin_transport_core::replication::protocol::{
    encode_hash_mismatch, encode_need_snapshot, encode_stream_start,
};

use super::validation_worker::{WorkerGone, spawn_unpinned};

/// Frames the worker may have encoded ahead of the socket.
///
/// A handful: enough that the wire never waits on a disk read in the
/// steady state (the worker refills while the slot drains), few enough
/// that the memory a join holds stays a few frames (each at most a
/// 64 KiB journal batch or snapshot chunk, plus framing) whatever the
/// size of what it streams. A bounded `sync_channel` is what makes the
/// worker wait for the wire, rather than read the disk ahead of it.
const JOIN_FRAMES_IN_FLIGHT: usize = 4;

/// What the worker needs to know about the replica it brings up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinRequest {
    /// The replica's handshake position.
    pub(crate) last_sequence: u64,
    /// The handshake validation found the replica's chain divergent: it
    /// gets the `HashMismatch` verdict and a snapshot, whatever its
    /// position.
    pub(crate) divergent: bool,
}

/// One message from the worker to the slot, in wire order.
enum JoinMessage {
    /// An encoded frame (length prefix included), to send as is.
    Frame(Vec<u8>),
    /// The join is over: the last sequence it streamed, or why it failed.
    /// Always the last message of a job.
    Done(io::Result<u64>),
}

/// One join handed to the worker.
struct JoinJob {
    request: JoinRequest,
    /// Frames out, bounded (see [`JOIN_FRAMES_IN_FLIGHT`]).
    frames: SyncSender<JoinMessage>,
    /// Buffers the slot has sent and hands back for reuse, so a long join
    /// allocates a few frame buffers rather than one per frame.
    spares: Receiver<Vec<u8>>,
    /// Set when the slot abandons the join (its replica left, or the
    /// node is stopping): the worker stops at its next frame.
    cancel: Arc<AtomicBool>,
}

/// The job a worker runs: stream `request`'s join through the publisher,
/// stopping early once the flag is set, and return the last sequence
/// streamed. A boxed trait object rather than a type parameter so the
/// worker type does not carry the application's event type; it is called
/// once per join, off the hot path.
type JoinFn =
    Box<dyn FnMut(&JoinRequest, CatchUpPublisher<'_>, &AtomicBool) -> io::Result<u64> + Send>;

/// Handle to a parked join thread, owned by the replica slot it serves.
///
/// One per slot, so that one replica's join never waits behind the
/// other's.
pub(crate) struct JoinWorker {
    /// Job queue. `Option` so [`Drop`] can close it — the worker's exit
    /// signal — before joining.
    ///
    /// Unbounded `mpsc::Sender`: its only producer is the poll thread,
    /// which must never block. Its depth is bounded by the protocol — a
    /// slot submits once per handshake, so a queue forms only when a slot
    /// was torn down mid-join and its next replica reached the same point
    /// before the abandoned job noticed its cancel flag (one disk read).
    jobs: Option<Sender<JoinJob>>,
    handle: Option<JoinHandle<()>>,
}

impl JoinWorker {
    /// Spawn the worker for one slot. `join` is a parameter, rather than
    /// a direct call to [`stream_join`], so the worker's mechanics can be
    /// tested without a journal on disk.
    pub(crate) fn spawn<F>(name: String, join: F) -> io::Result<Self>
    where
        F: FnMut(&JoinRequest, CatchUpPublisher<'_>, &AtomicBool) -> io::Result<u64>
            + Send
            + 'static,
    {
        let (jobs_tx, jobs_rx) = channel::<JoinJob>();
        let join: JoinFn = Box::new(join);
        let handle = spawn_unpinned(name, move || worker_loop(jobs_rx, join))?;
        Ok(JoinWorker {
            jobs: Some(jobs_tx),
            handle: Some(handle),
        })
    }

    /// Start a join and hand back the stream of its frames to pump.
    ///
    /// Returns `Err` only if the worker thread is gone (it panicked).
    pub(crate) fn submit(&self, request: JoinRequest) -> Result<JoinStream, WorkerGone> {
        let (frames_tx, frames_rx) = sync_channel(JOIN_FRAMES_IN_FLIGHT);
        let (spares_tx, spares_rx) = channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let jobs = self.jobs.as_ref().ok_or(WorkerGone)?;
        jobs.send(JoinJob {
            request,
            frames: frames_tx,
            spares: spares_rx,
            cancel: Arc::clone(&cancel),
        })
        .map_err(|_| WorkerGone)?;
        Ok(JoinStream {
            frames: frames_rx,
            spares: spares_tx,
            held: None,
            cancel,
        })
    }
}

impl Drop for JoinWorker {
    /// Close the queue, then wait for the thread, so no join is still
    /// reading the journal once the driver that owns it has gone.
    ///
    /// The [`JoinStream`] of a join in progress must be dropped first: it
    /// cancels the join and closes the channel the worker may be blocked
    /// sending on. The slot declares its stream before its worker, so
    /// field drop order does this. The wait is then at most one disk read.
    fn drop(&mut self) {
        self.jobs = None;
        if let Some(handle) = self.handle.take()
            && handle.join().is_err()
        {
            // `error!`: the worker only dies by panicking, a bug in us.
            tracing::error!("replica join worker panicked");
        }
    }
}

fn worker_loop(jobs: Receiver<JoinJob>, mut join: JoinFn) {
    // Ends when the owning handle drops the sender.
    for job in jobs {
        let JoinJob {
            request,
            frames,
            spares,
            cancel,
        } = job;
        let mut publish = |bytes: &[u8]| -> io::Result<()> {
            if cancel.load(Ordering::Acquire) {
                return Err(io::Error::other("join abandoned"));
            }
            // A spare when the slot has handed one back; otherwise (none
            // yet, or the slot has gone, which the send below reports) a
            // new one.
            let mut buf = spares
                .try_recv()
                .unwrap_or_else(|_| Vec::with_capacity(bytes.len()));
            buf.clear();
            buf.extend_from_slice(bytes);
            frames
                .send(JoinMessage::Frame(buf))
                .map_err(|_| io::Error::other("join abandoned"))
        };
        let result = join(&request, &mut publish, &cancel);
        // The catch-up steps return early, as if done, when the flag is
        // set; a cancelled join must not read as a finished one.
        let result = if cancel.load(Ordering::Acquire) {
            Err(io::Error::other("join abandoned"))
        } else {
            result
        };
        // Deliberately ignored: a failed send means the slot abandoned
        // the join, and nobody is waiting for its result.
        let _ = frames.send(JoinMessage::Done(result));
    }
}

/// The slot's end of a join in progress: the worker's frames, to move
/// into the socket. Dropping it abandons the join.
pub(crate) struct JoinStream {
    frames: Receiver<JoinMessage>,
    spares: Sender<Vec<u8>>,
    /// A frame taken from the worker that the socket had no room for
    /// yet; sent first on the next pump, so the order holds.
    held: Option<Vec<u8>>,
    cancel: Arc<AtomicBool>,
}

/// What one [`JoinStream::pump`] came to.
#[derive(Debug)]
pub(crate) enum Pump {
    /// More to come: `queued` bytes were handed to the socket this time.
    Pending { queued: usize },
    /// Every frame of the join has been handed to the socket: the last
    /// sequence it streamed, or why it failed (on the disk, or a frame
    /// the socket can never take).
    Done(io::Result<u64>),
    /// The worker died without a result — it panicked, a bug.
    WorkerGone,
}

impl JoinStream {
    /// Move the worker's frames into the socket through `queue` (which
    /// takes a whole frame or none of it, as `DpdkTransport::queue_send`
    /// does), until it refuses one, the worker has nothing ready, or
    /// `budget` bytes have gone (always at least one frame, so a frame
    /// larger than the budget still moves). Never blocks.
    ///
    /// `capacity` is the most the socket's queue can ever hold: a frame
    /// larger than that would never be taken, and fails the join rather
    /// than wedging it.
    pub(crate) fn pump(
        &mut self,
        budget: usize,
        capacity: usize,
        mut queue: impl FnMut(&[u8]) -> bool,
    ) -> Pump {
        let mut queued = 0usize;
        loop {
            if queued > 0 && queued >= budget {
                return Pump::Pending { queued };
            }
            let frame = match self.held.take() {
                Some(frame) => frame,
                None => match self.frames.try_recv() {
                    Ok(JoinMessage::Frame(frame)) => frame,
                    Ok(JoinMessage::Done(result)) => return Pump::Done(result),
                    Err(TryRecvError::Empty) => return Pump::Pending { queued },
                    Err(TryRecvError::Disconnected) => return Pump::WorkerGone,
                },
            };
            if frame.len() > capacity {
                return Pump::Done(Err(io::Error::other(format!(
                    "a {}-byte join frame exceeds the replica socket's {capacity}-byte queue",
                    frame.len()
                ))));
            }
            if !queue(&frame) {
                self.held = Some(frame);
                return Pump::Pending { queued };
            }
            queued += frame.len();
            // Deliberately ignored: the worker has gone only once the join
            // is over, and then the buffer is simply freed.
            let _ = self.spares.send(frame);
        }
    }
}

impl Drop for JoinStream {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
    }
}

/// What [`stream_join`] reads besides the request: fixed for the
/// driver's lifetime, so held by the worker rather than sent per join.
// Used by the DPDK sender and by the tests that run a join against a real
// journal, which a `no-persist` build has none of: a `no-persist` test
// build without `dpdk` compiles this module for its mechanics alone.
#[cfg_attr(all(feature = "no-persist", not(feature = "dpdk")), allow(dead_code))]
pub(crate) struct JoinContext {
    pub(crate) journal_path: PathBuf,
    /// Read when the `StreamStart` is encoded, as the inline join did.
    pub(crate) fence_state: Arc<FenceState>,
    /// Read when the frame carrying it is encoded, as the inline join did.
    pub(crate) ack_policy: Arc<AtomicU8>,
}

/// A replica's join, as the primary streams it: the probe, then either
/// `StreamStart` and journal catch-up, or the resync verdict
/// (`HashMismatch` for a divergent replica, `NeedSnapshot` for one too far
/// behind) and the snapshot transfer, which ends in its own `StreamStart`
/// and catch-up. Returns the last sequence streamed.
///
/// The snapshot route's pre-flight runs before the verdict is published:
/// the replica archives its lineage on the verdict, so a snapshot that
/// cannot be produced must fail the join with nothing sent (see
/// `preflight_snapshot_transfer`).
// See `JoinContext` for the `no-persist` allowance.
#[cfg_attr(all(feature = "no-persist", not(feature = "dpdk")), allow(dead_code))]
pub(crate) fn stream_join<E: AppEvent>(
    ctx: &JoinContext,
    request: &JoinRequest,
    publish: CatchUpPublisher<'_>,
    cancel: &AtomicBool,
) -> io::Result<u64> {
    let journal_path = ctx.journal_path.as_path();
    let can_catch_up = !request.divergent
        && can_catch_up_from_journal(journal_path, request.last_sequence)
            .map_err(|e| io::Error::other(format!("catch-up probe: {e}")))?;

    let mut frame = Vec::with_capacity(128);
    let result = if can_catch_up {
        // The lineage origin's identity, and the lineage's genesis length
        // from the same header.
        let origin = lineage_origin(journal_path)?;
        encode_stream_start(
            request.last_sequence,
            origin.starting_sequence,
            origin.anchor_hash,
            origin.genesis_entries,
            ctx.fence_state.epoch(),
            ctx.ack_policy.load(Ordering::Relaxed),
            &mut frame,
        );
        publish(&frame)?;
        catch_up_from_journal_with::<E>(journal_path, request.last_sequence, publish, cancel)?
    } else {
        if request.divergent {
            encode_hash_mismatch(&mut frame);
        } else {
            encode_need_snapshot(&mut frame);
        }
        preflight_snapshot_transfer(journal_path)?;
        publish(&frame)?;
        snapshot_transfer_with::<E>(
            journal_path,
            publish,
            cancel,
            ctx.ack_policy.load(Ordering::Relaxed),
        )?
    };
    match result {
        CatchUpResult::Ok(end) => Ok(end),
        CatchUpResult::NeedSnapshot if can_catch_up => Err(io::Error::other(
            "catch-up found no starting segment after the probe found one",
        )),
        CatchUpResult::NeedSnapshot => Err(io::Error::other(
            "catch-up failed even after snapshot transfer",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    /// A job that publishes `count` one-byte frames `0, 1, …` and returns
    /// `count`.
    fn frames_job(
        count: u8,
    ) -> impl FnMut(&JoinRequest, CatchUpPublisher<'_>, &AtomicBool) -> io::Result<u64> + Send {
        move |_req, publish, _cancel| {
            for i in 0..count {
                publish(&[i])?;
            }
            Ok(u64::from(count))
        }
    }

    fn request() -> JoinRequest {
        JoinRequest {
            last_sequence: 0,
            divergent: false,
        }
    }

    /// Pump until the join is over, taking every frame; returns the frames
    /// in the order the socket got them, and the result.
    fn pump_to_end(stream: &mut JoinStream) -> (Vec<Vec<u8>>, io::Result<u64>) {
        let mut sent = Vec::new();
        loop {
            match stream.pump(usize::MAX, usize::MAX, |f| {
                sent.push(f.to_vec());
                true
            }) {
                Pump::Pending { .. } => std::thread::yield_now(),
                Pump::Done(result) => return (sent, result),
                Pump::WorkerGone => panic!("the worker died"),
            }
        }
    }

    /// Every frame reaches the socket, in the order the job published
    /// them, and the result comes after the last.
    #[test]
    fn frames_arrive_in_order_then_the_result() {
        let worker = JoinWorker::spawn("test-join".into(), frames_job(20)).unwrap();
        let mut stream = worker.submit(request()).unwrap();
        let (sent, result) = pump_to_end(&mut stream);
        assert_eq!(sent, (0..20u8).map(|i| vec![i]).collect::<Vec<_>>());
        assert_eq!(result.unwrap(), 20);
    }

    /// The point of the worker: while the job is stuck on its disk, a
    /// pump returns at once, with nothing to send, rather than waiting.
    #[test]
    fn a_pump_never_waits_on_a_stalled_job() {
        let (release_tx, release_rx) = channel::<()>();
        let gate = Mutex::new(release_rx);
        let worker = JoinWorker::spawn(
            "test-join".into(),
            move |_req: &JoinRequest, publish: CatchUpPublisher<'_>, _c: &AtomicBool| {
                publish(b"before")?;
                gate.lock()
                    .expect("test mutex poisoned")
                    .recv()
                    .expect("release channel closed");
                publish(b"after")?;
                Ok(7)
            },
        )
        .unwrap();
        let mut stream = worker.submit(request()).unwrap();

        let mut sent = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while sent.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "the first frame never came"
            );
            let _ = stream.pump(usize::MAX, usize::MAX, |f| {
                sent.push(f.to_vec());
                true
            });
        }
        assert_eq!(sent, [b"before".to_vec()]);
        for _ in 0..100 {
            let started = std::time::Instant::now();
            assert!(matches!(
                stream.pump(usize::MAX, usize::MAX, |_| panic!("nothing is ready")),
                Pump::Pending { queued: 0 }
            ));
            assert!(
                started.elapsed() < Duration::from_secs(1),
                "the pump waited"
            );
        }
        release_tx.send(()).unwrap();
        let (rest, result) = pump_to_end(&mut stream);
        assert_eq!(rest, [b"after".to_vec()]);
        assert_eq!(result.unwrap(), 7);
    }

    /// A frame the socket refuses is held and sent first next time:
    /// nothing is lost or reordered across a full queue.
    #[test]
    fn a_refused_frame_is_resent_first() {
        let worker = JoinWorker::spawn("test-join".into(), frames_job(6)).unwrap();
        let mut stream = worker.submit(request()).unwrap();
        let mut sent = Vec::new();
        // Refuse every other offer.
        let mut accept = false;
        let result = loop {
            match stream.pump(usize::MAX, usize::MAX, |f| {
                accept = !accept;
                if accept {
                    sent.push(f.to_vec());
                }
                accept
            }) {
                Pump::Pending { .. } => std::thread::yield_now(),
                Pump::Done(result) => break result,
                Pump::WorkerGone => panic!("the worker died"),
            }
        };
        assert_eq!(sent, (0..6u8).map(|i| vec![i]).collect::<Vec<_>>());
        assert_eq!(result.unwrap(), 6);
    }

    /// The budget bounds one pump, but at least one frame always moves.
    #[test]
    fn the_budget_bounds_a_pump_but_one_frame_always_moves() {
        let worker = JoinWorker::spawn(
            "test-join".into(),
            |_req: &JoinRequest, publish: CatchUpPublisher<'_>, _c: &AtomicBool| {
                for _ in 0..3 {
                    publish(&[0u8; 100])?;
                }
                Ok(3)
            },
        )
        .unwrap();
        let mut stream = worker.submit(request()).unwrap();
        let mut frames_this_pump = Vec::new();
        let result = loop {
            let mut count = 0;
            match stream.pump(10, usize::MAX, |_| {
                count += 1;
                true
            }) {
                Pump::Pending { queued } => {
                    assert!(count <= 1, "a 10-byte budget took {count} 100-byte frames");
                    assert_eq!(queued, count * 100);
                    if count > 0 {
                        frames_this_pump.push(count);
                    }
                    std::thread::yield_now();
                }
                Pump::Done(result) => break result,
                Pump::WorkerGone => panic!("the worker died"),
            }
        };
        assert_eq!(frames_this_pump.len(), 3);
        assert_eq!(result.unwrap(), 3);
    }

    /// A frame the socket could never hold fails the join instead of
    /// being offered for ever.
    #[test]
    fn a_frame_larger_than_the_socket_fails_the_join() {
        let worker = JoinWorker::spawn(
            "test-join".into(),
            |_req: &JoinRequest, publish: CatchUpPublisher<'_>, _c: &AtomicBool| {
                publish(&[0u8; 100])?;
                Ok(1)
            },
        )
        .unwrap();
        let mut stream = worker.submit(request()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match stream.pump(usize::MAX, 99, |_| panic!("must not be offered")) {
                Pump::Pending { .. } => {
                    assert!(std::time::Instant::now() < deadline, "the frame never came");
                    std::thread::yield_now();
                }
                Pump::Done(result) => {
                    assert!(result.is_err());
                    break;
                }
                Pump::WorkerGone => panic!("the worker died"),
            }
        }
    }

    /// The job's error is the join's result, after the frames sent
    /// before it.
    #[test]
    fn a_failed_job_reports_its_error() {
        let worker = JoinWorker::spawn(
            "test-join".into(),
            |_req: &JoinRequest, publish: CatchUpPublisher<'_>, _c: &AtomicBool| {
                publish(b"verdict")?;
                Err(io::Error::new(io::ErrorKind::NotFound, "no snapshot"))
            },
        )
        .unwrap();
        let mut stream = worker.submit(request()).unwrap();
        let (sent, result) = pump_to_end(&mut stream);
        assert_eq!(sent, [b"verdict".to_vec()]);
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    /// A slot that abandons a join mid-way (its replica left) does not
    /// strand the worker blocked on the full channel: the next join on
    /// the same worker runs, and its frames are its own.
    #[test]
    fn an_abandoned_join_frees_the_worker_for_the_next() {
        let worker = JoinWorker::spawn(
            "test-join".into(),
            |req: &JoinRequest, publish: CatchUpPublisher<'_>, cancel: &AtomicBool| {
                // Far more than the channel holds: the first join blocks
                // on it once the slot stops pumping.
                for _ in 0..1000 {
                    if cancel.load(Ordering::Acquire) {
                        break;
                    }
                    publish(&req.last_sequence.to_le_bytes())?;
                }
                Ok(req.last_sequence)
            },
        )
        .unwrap();
        let abandoned = worker
            .submit(JoinRequest {
                last_sequence: 1,
                divergent: false,
            })
            .unwrap();
        // Let the worker fill the channel, then walk away.
        std::thread::sleep(Duration::from_millis(50));
        drop(abandoned);

        let mut stream = worker
            .submit(JoinRequest {
                last_sequence: 2,
                divergent: false,
            })
            .unwrap();
        let (sent, result) = pump_to_end(&mut stream);
        assert_eq!(sent.len(), 1000);
        assert!(sent.iter().all(|f| f == &2u64.to_le_bytes()));
        assert_eq!(result.unwrap(), 2);
    }

    /// Dropping the stream, then the worker, mid-join returns promptly:
    /// the order the slot's fields drop in.
    #[test]
    fn dropping_a_slot_mid_join_does_not_hang() {
        let worker = JoinWorker::spawn(
            "test-join".into(),
            |_req: &JoinRequest, publish: CatchUpPublisher<'_>, _c: &AtomicBool| {
                loop {
                    publish(&[0u8; 8])?;
                }
            },
        )
        .unwrap();
        let stream = worker.submit(request()).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let (done_tx, done_rx) = channel();
        std::thread::spawn(move || {
            drop(stream);
            drop(worker);
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("dropping the stream and then the worker must not hang");
    }

    /// A cancelled join never reports success, even if its job returned
    /// as if done (the catch-up steps do, on their stop flag).
    #[test]
    fn a_cancelled_join_is_not_reported_as_done() {
        let (seen_tx, seen_rx) = channel::<()>();
        let (go_tx, go_rx) = channel::<()>();
        let gates = Mutex::new((seen_tx, go_rx));
        let worker = JoinWorker::spawn(
            "test-join".into(),
            move |_req: &JoinRequest, _p: CatchUpPublisher<'_>, _c: &AtomicBool| {
                let gates = gates.lock().expect("test mutex poisoned");
                gates.0.send(()).expect("test alive");
                gates.1.recv().expect("test alive");
                Ok(5)
            },
        )
        .unwrap();
        let stream = worker.submit(request()).unwrap();
        seen_rx.recv().unwrap();
        // Cancel without closing the channel, to read what the worker
        // reports.
        stream.cancel.store(true, Ordering::Release);
        go_tx.send(()).unwrap();
        match stream.frames.recv().unwrap() {
            JoinMessage::Done(result) => assert!(result.is_err()),
            JoinMessage::Frame(_) => panic!("no frame was published"),
        }
    }

    /// A dead worker is reported as such, not as a pending join.
    #[test]
    fn a_panicking_job_is_reported_as_a_dead_worker() {
        let worker = JoinWorker::spawn(
            "test-join".into(),
            |_req: &JoinRequest, _p: CatchUpPublisher<'_>, _c: &AtomicBool| -> io::Result<u64> {
                panic!("job bug")
            },
        )
        .unwrap();
        let mut stream = worker.submit(request()).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match stream.pump(usize::MAX, usize::MAX, |_| true) {
                Pump::Pending { .. } => {
                    assert!(std::time::Instant::now() < deadline, "no outcome");
                    std::thread::yield_now();
                }
                Pump::WorkerGone => break,
                Pump::Done(r) => panic!("expected a dead worker, got {r:?}"),
            }
        }
        // Dropping the handle of a panicked worker logs and returns.
        drop(worker);
    }

    /// [`stream_join`] against a real journal.
    #[cfg(not(feature = "no-persist"))]
    mod stream {
        use super::*;
        use counter_server::CounterEvent;
        use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};
        use melin_transport_core::replication::protocol::{PrimaryMessage, decode_primary_message};

        fn ctx(journal_path: PathBuf) -> JoinContext {
            JoinContext {
                journal_path,
                fence_state: Arc::new(FenceState::new(3)),
                ack_policy: Arc::new(AtomicU8::new(1)),
            }
        }

        fn journal(dir: &std::path::Path, events: u64) -> PathBuf {
            let path = dir.join("primary.journal");
            let mut w = BufferedWriter::<CounterEvent>::create(&path).expect("create");
            for _ in 0..events {
                w.append(&JournalEvent::App(CounterEvent::Increment { amount: 1 }))
                    .expect("append");
            }
            drop(w);
            path
        }

        fn run(ctx: &JoinContext, request: JoinRequest) -> (Vec<Vec<u8>>, io::Result<u64>) {
            let mut sent = Vec::new();
            let never = AtomicBool::new(false);
            let result = stream_join::<CounterEvent>(
                ctx,
                &request,
                &mut |frame: &[u8]| {
                    sent.push(frame.to_vec());
                    Ok(())
                },
                &never,
            );
            (sent, result)
        }

        fn decode(frame: &[u8]) -> PrimaryMessage {
            decode_primary_message(&frame[4..]).expect("a primary message")
        }

        /// A replica the journal can serve gets `StreamStart` (with the
        /// primary's epoch and ack policy), then the history it lacks.
        #[test]
        fn a_replica_within_the_journal_gets_stream_start_then_history() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = ctx(journal(dir.path(), 5));
            let (sent, result) = run(
                &ctx,
                JoinRequest {
                    last_sequence: 2,
                    divergent: false,
                },
            );
            assert_eq!(result.unwrap(), 5);
            match decode(&sent[0]) {
                PrimaryMessage::StreamStart {
                    start_sequence,
                    epoch,
                    ack_policy,
                    ..
                } => {
                    assert_eq!((start_sequence, epoch, ack_policy), (2, 3, 1));
                }
                other => panic!("expected StreamStart first, got {other:?}"),
            }
            assert!(sent.len() > 1, "the history follows the StreamStart");
        }

        /// A divergent replica with no snapshot to serve gets nothing:
        /// the verdict must not reach it before the snapshot is known to
        /// be servable, or it archives its lineage for nothing.
        #[test]
        fn a_divergent_replica_gets_no_verdict_without_a_snapshot() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = ctx(journal(dir.path(), 3));
            let (sent, result) = run(
                &ctx,
                JoinRequest {
                    last_sequence: 2,
                    divergent: true,
                },
            );
            assert!(result.is_err());
            assert!(
                sent.is_empty(),
                "nothing may be sent: {} frames",
                sent.len()
            );
        }
    }
}
