//! io_uring-based multiplexed reader with multishot RECV.
//!
//! Uses `IORING_OP_RECV` with `IORING_RECV_MULTISHOT` — a single SQE per
//! connection produces multiple CQEs as data arrives, eliminating the
//! resubmission overhead of standard RECV. Combined with a ring-mapped
//! provided-buffer ring (`IOSQE_BUFFER_SELECT` + `buf_ring`), the kernel
//! selects a buffer from a shared pool for each recv, and consumed
//! buffers are recycled with a shared-memory store — no per-recv
//! ProvideBuffers SQE/CQE round trip. Requires kernel ≥ 5.19.
//!
//! Uses a single reader thread — io_uring is efficient enough for hundreds
//! of connections. New connections are registered via eventfd wakeup.
//!
//! Connection state is stored in a slab (index-stable Vec) so that io_uring
//! user_data carries a slab index, not an fd. This avoids fd-reuse races
//! where a recycled fd number could match a stale CQE.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};
use tracing::{debug, error};

use crate::ControlEvent;
use crate::buf_ring::BufRing;
use crate::halt::{EarlyReplySender, HaltGate};
use crate::policy_control::{AckPolicyPublisher, BootedClaim};
use melin_app::Application;
use melin_app::auth::{ClientRole, RoleId};
use melin_app::decoder::ErasedDecoder;

/// Decoder type alias: the application's request decoder, bound to its
/// `Event` type, with its role type erased (see [`ErasedDecoder`]).
/// Mirrors [`crate::response::ResponseEncoderArc`]; hides the
/// `dyn ErasedDecoder<…>` spelling at call sites that thread the decoder
/// through several functions.
pub type RequestDecoderArc<A> = Arc<dyn ErasedDecoder<<A as Application>::Event>>;
use melin_app::unix_epoch_nanos;
use melin_pipeline::ring;
use melin_transport_core::pipeline::InputSlot;
use melin_transport_core::tick::TickSchedule;

/// Size of each provided buffer. 4 KiB accommodates multiple frames per
/// recv (frames are typically <100 bytes).
///
/// The number of buffers in the shared pool, and the ring's depth, are
/// not constants: both are derived from `--max-connections` at startup —
/// see [`RingSizing`]. The pool is a power of two (buf_ring ABI) with two
/// buffers per connection. On exhaustion the kernel completes the
/// multishot with `ENOBUFS` (no data consumed) and the loop re-arms it
/// after the drain's recycles refill the ring. The ring holds a drain's
/// worth of SQEs: two per connection (a multishot re-arm and a teardown
/// cancel) plus housekeeping; buffer recycling goes through the buf_ring,
/// not the SQ.
const BUF_SIZE: usize = 4096;

/// Buffer group ID for the provided recv buffer pool.
const BUF_GROUP_ID: u16 = 0;

use crate::client_frames::{ClientIngress, FrameAction, LONG_PARK, LongParkReport, MAX_FRAME_SIZE};
use crate::connection_limit::{RingSizing, await_startup, ring_setup_error};

/// User data sentinel for the eventfd read SQE.
const EVENTFD_TOKEN: u64 = u64::MAX;

/// User data sentinel for legacy ProvideBuffers CQEs (fallback recycle
/// path only). Best-effort re-provisions: we log errors but don't act
/// on success.
const PROVIDE_BUFS_TOKEN: u64 = u64::MAX - 1;

/// User data sentinel for AsyncCancel SQEs (connection teardown and the
/// shutdown quiesce). The completion carries no actionable information:
/// `ENOENT`/`EALREADY` just mean the target op already finished.
const CANCEL_TOKEN: u64 = u64::MAX - 3;

/// User data sentinel for the tick timeout SQE. The reader arms a single
/// `IORING_OP_TIMEOUT` per cadence so `submit_and_wait` returns at the tick
/// deadline even when no client traffic is flowing. The CQE itself carries
/// no information; the loop body checks `Instant::now()` against the next
/// deadline and emits the actual `JournalEvent::Tick`.
const TICK_TIMEOUT_TOKEN: u64 = u64::MAX - 2;

/// User data sentinel for the park-retry timeout SQE: armed while any
/// connection is parked, so the reader comes back round to retry them
/// even when nothing else wakes it (see [`PARK_RETRY`]). Like the tick's,
/// its CQE carries no information.
const PARK_RETRY_TOKEN: u64 = u64::MAX - 4;

/// The longest a parked connection waits for its retry once the
/// early-reply queue has room again.
///
/// Nothing tells the reader that the response stage drained the queue,
/// so while any connection is parked its wait in the kernel is bounded by
/// a timeout of this length. A wake from the response stage (the eventfd)
/// would resume sooner, but needs a fenced park/drain handshake against a
/// lost wake, and puts a `write` on the response stage's drain, to shorten
/// a path that is degraded by definition: a park happens only while that
/// stage is not draining. Any other wake retries parked connections too.
const PARK_RETRY: Duration = Duration::from_millis(1);

/// The longest a park's `AsyncCancel` may go unanswered before the reader
/// stops waiting on it and closes the connection.
///
/// An honoured cancel's terminal CQE lands within the same submit or the
/// next, so a cancel still in flight after this long was not honoured:
/// some hosts filter io_uring opcodes (see the shutdown quiesce), and a
/// filtered cancel completes with `EINVAL`, leaving the multishot armed.
/// The connection's bytes would then keep landing while parked, growing
/// its buffer with nothing pushing back on the client. Closing it is the
/// behaviour before parking existed; teardown's `shutdown(2)` ends the
/// multishot whatever the host filters.
const PARK_CANCEL_DEADLINE: Duration = Duration::from_secs(1);

/// CQE flag: buffer ID is valid in upper 16 bits of flags.
const IORING_CQE_F_BUFFER: u32 = 1 << 0;

/// CQE flag: more completions coming from this multishot operation.
const IORING_CQE_F_MORE: u32 = 1 << 1;

/// Bit shift to extract buffer ID from CQE flags.
const IORING_CQE_BUFFER_SHIFT: u32 = 16;

use melin_wire_protocol::control::ConnectionId;

/// Command sent from the accept loop to a reader thread.
pub struct ReaderRegistration<R> {
    pub connection_id: ConnectionId,
    pub reader: R,
    pub addr: SocketAddr,
    /// The client's role, established during the auth handshake.
    pub role: ClientRole<RoleId>,
    /// FxHash of the client's Ed25519 public key. Stored per-connection
    /// and copied into every InputSlot as the submitting key's identity.
    pub key_hash: u64,
}

/// Handle for the accept loop to register connections with the io_uring reader.
pub struct UringReaderHandle<R> {
    tx: mpsc::Sender<ReaderRegistration<R>>,
    /// Wakeup eventfd, shared with the reader thread.
    ///
    /// `Arc<OwnedFd>` rather than a `RawFd` so the descriptor cannot be
    /// closed while the other side still holds it: the close happens in
    /// `OwnedFd`'s `Drop`, which runs when the last holder goes away, and
    /// no code here is able to call `close` early because none of it owns
    /// a raw descriptor to close.
    ///
    /// The reader thread used to close this itself while this handle was
    /// still writing wakeups to it — a use-after-close whose real danger
    /// was descriptor reuse, not `EBADF`: once the number is free, a
    /// journal segment or an accepted connection can be assigned it, and
    /// the next wakeup writes eight bytes into that instead. Found by
    /// ThreadSanitizer.
    event_fd: Arc<OwnedFd>,
    join_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl<R> UringReaderHandle<R> {
    /// Register a new connection with the reader thread.
    ///
    /// If the reader thread's channel is dead (thread panicked), logs an
    /// error and signals shutdown so the server can restart cleanly.
    pub fn register(&mut self, registration: ReaderRegistration<R>) {
        if self.tx.send(registration).is_ok() {
            wake_reader(&self.event_fd);
        } else {
            error!("reader thread dead, cannot register connection");
            self.shutdown.store(true, Ordering::Relaxed);
        }
    }

    /// Signal the reader thread to shut down and wake it from io_uring_enter.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        wake_reader(&self.event_fd);
    }

    /// Join the reader thread. Call after `shutdown()`.
    pub fn join(mut self) {
        if let Some(handle) = self.join_handle.take()
            && let Err(panic) = handle.join()
        {
            let msg = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(|s| s.as_str()))
                .unwrap_or("<non-string panic>");
            error!(message = msg, "reader thread panicked");
        }
    }
}

/// Wake the reader from `io_uring_enter` through its eventfd: the loop
/// comes back round, and sees whatever it was woken for (a registration,
/// shutdown, an ack-policy request).
///
/// A failed write is not reported: on a nonblocking eventfd it can only be
/// `EAGAIN`, the counter at its maximum, which means a wakeup is already
/// pending.
pub(crate) fn wake_reader(event_fd: &OwnedFd) {
    let val: u64 = 1;
    // SAFETY: `event_fd` is a live descriptor (borrowed from its owner), and
    // the buffer is a live `u64`, of the length passed.
    unsafe {
        libc::write(
            event_fd.as_raw_fd(),
            &val as *const u64 as *const libc::c_void,
            8,
        );
    }
}

/// Spawn the io_uring reader thread. Returns a handle for registering
/// connections.
///
/// One reader thread serves every TCP connection on the server. io_uring
/// with multishot RECV multiplexes thousands of sockets efficiently and the
/// matching stage is the throughput limit, so adding more reader threads
/// would not raise throughput — it would only re-introduce contention on
/// the input ring's multi-producer cursor.
///
/// `tick_cadence: Some(d)` makes the reader the clock-tick generator: it
/// arms an `IORING_OP_TIMEOUT` so `submit_and_wait` returns at the tick
/// deadline even when no client traffic is flowing, then publishes a
/// `JournalEvent::Tick { now_ns }` onto the same input ring it uses for
/// client requests. Pass `None` to disable the tick (useful for benchmarks
/// that don't exercise time-driven features).
///
/// Every request it does not publish — one the application's decoder
/// filters or refuses, a write refused while `halt` refuses writes, one
/// shed on a full input ring — the reader answers through
/// `early_replies` — see [`crate::halt`].
///
/// The reader also journals the admin `ACK-POLICY` command's requests: it
/// attaches `ack_policy` with its wakeup eventfd, so a request posted while
/// it is parked brings it back round its loop, which publishes the entry
/// (see [`crate::policy_control`]).
///
/// `sizing` sets the ring's depth and the provided-buffer pool (see
/// [`RingSizing`]). Returns once the reader holds its io_uring instance
/// and buffer pool, or with the error that kept it from getting them —
/// a startup error, not a dead thread: the kernel charges ring memory
/// against the locked-memory limit, and running out of it is an
/// operator-fixable condition the node has to report.
#[allow(clippy::too_many_arguments)]
pub fn spawn_reader<A: Application, R: AsRawFd + Send + 'static>(
    mut producer: ring::Producer<InputSlot<A::Event>>,
    decoder: RequestDecoderArc<A>,
    halt: HaltGate,
    early_replies: EarlyReplySender<A::Report>,
    ack_policy: BootedClaim,
    control_tx: mpsc::Sender<ControlEvent>,
    core: usize,
    connection_timeout: Option<Duration>,
    tick_cadence: Option<Duration>,
    sizing: RingSizing,
    shutdown: Arc<AtomicBool>,
) -> std::io::Result<UringReaderHandle<R>>
where
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
{
    let (tx, rx) = mpsc::channel();

    let raw_event_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    if raw_event_fd < 0 {
        let err = std::io::Error::last_os_error();
        return Err(std::io::Error::new(
            err.kind(),
            format!("uring-reader: cannot create its wakeup eventfd: {err}"),
        ));
    }

    // SAFETY: `eventfd` returned a fresh descriptor (checked non-negative
    // above) that nothing else owns, so transferring ownership to `OwnedFd`
    // is sound. From here the raw number is never closed by hand — the
    // descriptor lives exactly as long as the last `Arc` holder.
    let event_fd = Arc::new(unsafe { OwnedFd::from_raw_fd(raw_event_fd) });
    let wakeup_fd = Arc::clone(&event_fd);
    let shutdown_clone = Arc::clone(&shutdown);
    // Attached here, on this thread, which still owns the producer alone
    // (a policy staged during bring-up is journaled now), with the
    // eventfd: if the thread never starts, the publisher is dropped with
    // its closure and detaches.
    let ack_policy = ack_policy.attach(&mut producer, &halt, Some(Arc::clone(&event_fd)));

    // The thread's startup report. The ring has to be created on the
    // thread that submits to it (it is SINGLE_ISSUER: the kernel binds
    // it to its creator, and a ring moved across threads fails every
    // submit), so the spawner cannot create it itself and learns the
    // outcome here instead. Capacity one so the report never blocks the
    // worker on the spawner reaching `recv`.
    let (ready_tx, ready_rx) = mpsc::sync_channel::<std::io::Result<()>>(1);

    // `core == 0` is the unpinned sentinel, which puts the thread on the
    // process's home CPU mask — see the `melin_app::affinity` module docs.
    let handle = melin_app::affinity::spawn_placed("uring-reader", core, move || {
        reader_loop::<A, R>(
            rx,
            wakeup_fd,
            producer,
            &*decoder,
            &halt,
            early_replies,
            ack_policy,
            &control_tx,
            connection_timeout,
            tick_cadence,
            sizing,
            ready_tx,
            &shutdown_clone,
        );
    })
    .map_err(|e| {
        let e = std::io::Error::from(e);
        std::io::Error::new(
            e.kind(),
            format!("uring-reader: cannot start its thread: {e}"),
        )
    })?;

    if let Err(e) = await_startup("uring-reader", &ready_rx) {
        // The thread has returned (or panicked) by now; a panic was
        // already reported by the panic hook, and the startup error is
        // what the caller needs, so the join result adds nothing.
        let _ = handle.join();
        return Err(e);
    }

    Ok(UringReaderHandle {
        tx,
        event_fd,
        join_handle: Some(handle),
        shutdown,
    })
}

// ---------------------------------------------------------------------------
// Slab-based connection storage
// ---------------------------------------------------------------------------

/// Per-connection state for multishot io_uring recv + incremental frame parsing.
struct ConnectionEntry<R> {
    connection_id: u64,
    addr: SocketAddr,
    /// The client's role, from the auth handshake. Checked per-request
    /// by the decoder on the reader thread (cold path), zero cost on the
    /// matching stage.
    role: ClientRole<RoleId>,
    /// FxHash of the client's Ed25519 public key. Copied into every
    /// InputSlot as the submitting key's identity.
    key_hash: u64,
    /// Owned reader — keeps the fd alive. Dropping closes the fd.
    _reader: R,
    fd: RawFd,
    /// Bytes received and not yet processed, and what is left of a shed
    /// tail: the input state the DPDK poll loop keeps too.
    ingress: ClientIngress,
    /// Where the connection's receive stands with the kernel: armed,
    /// between two arms, parked, or dying.
    recv: RecvState,
    /// Last time any data was received on this connection. Used for
    /// idle timeout detection.
    last_activity: Instant,
}

/// Where a connection's receive stands with the kernel.
///
/// One enum rather than flags, so a parked connection cannot be re-armed
/// or freed by the paths written for the others: every change goes
/// through a method here, each an exhaustive `match`, and the reader acts
/// on the step it returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecvState {
    /// A multishot receive is armed: the kernel posts CQEs for it until
    /// one without `IORING_CQE_F_MORE`.
    Armed,
    /// No receive armed, none owed: the multishot ended (pool exhaustion,
    /// or the kernel's own decision) and is re-armed in the same drain.
    Idle,
    /// Parked (see `client_frames`): the early-reply queue had no room for
    /// an answer, so the connection is not read, and its buffer is retried
    /// on every wake of the reader. `cancelling`: the multishot's
    /// `AsyncCancel` is in flight, so the kernel still owes its terminal
    /// CQE, and data CQEs that land before it are kept. `since`: when it
    /// parked, for the long-park report.
    Parked { cancelling: bool, since: Instant },
    /// A retry came back with the buffer processed while the park's cancel
    /// was still in flight: the receive is re-armed on the cancelled one's
    /// terminal CQE, never before, so two multishots never feed one buffer.
    Resuming,
    /// Torn down while the kernel still owes a terminal CQE. The slab index
    /// must stay allocated until it arrives: freed early, the LIFO free
    /// list hands the index to a new registration while the kernel can
    /// still post CQEs carrying it, and the old peer's bytes would be
    /// parsed under the new connection's identity and permissions (audit
    /// review F1). Its CQEs recycle their buffers and are otherwise
    /// discarded.
    Dying,
}

/// What a receive CQE carries, as far as the receive state cares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecvCompletion {
    /// Bytes, in a provided buffer.
    Data,
    /// Nothing, and not the end of the connection: the pool ran dry at
    /// buffer selection (`ENOBUFS`, nothing read: the bytes wait in the
    /// socket), the park's cancel landed (`ECANCELED`), or a data CQE
    /// without a buffer.
    Empty,
    /// The peer closed the connection (0), or the receive failed.
    Closed,
}

/// What the reader does with a receive CQE ([`RecvState::on_cqe`]). Its
/// buffer, if it carries one, is recycled whatever the step, once any
/// bytes are copied out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CqeStep {
    /// A torn-down connection's CQE: discarded, and the slab index freed
    /// when it is the terminal one.
    Discard { free: bool },
    /// The connection is over: free it and tell the response stage.
    Remove,
    /// Append the bytes and process the buffer, then act on
    /// [`RecvState::after_frames`].
    Process,
    /// Append the bytes and nothing more: the connection is parked, and the
    /// retry processes them.
    Keep,
    /// Arm a new multishot receive (the state is `Armed` already).
    Arm,
    /// Nothing to do.
    Nothing,
}

/// What the reader does once a connection's buffer has been processed
/// ([`RecvState::after_frames`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FramesStep {
    /// Nothing to do.
    Nothing,
    /// Arm a new multishot receive (the state is `Armed` already).
    Arm,
    /// Cancel the armed multishot: the connection has parked.
    Cancel,
    /// Tear the connection down ([`begin_teardown`]).
    Teardown,
}

/// What tearing a connection down takes ([`RecvState::on_teardown`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TeardownStep {
    /// Already dying: nothing more, no second `Disconnected`.
    AlreadyDying,
    /// Nothing can post a CQE for it: free the slab index now.
    FreeNow,
    /// Cancel the armed multishot, and free on its terminal CQE.
    Cancel,
    /// A cancel is already in flight (the park's): free on its terminal
    /// CQE, with no second `AsyncCancel`.
    AwaitCancel,
}

impl RecvState {
    /// The state after a receive CQE, and what the reader does with it.
    /// `terminal`: the CQE ends the armed multishot (no `F_MORE`).
    fn on_cqe(self, completion: RecvCompletion, terminal: bool) -> (RecvState, CqeStep) {
        if self == RecvState::Dying {
            return (self, CqeStep::Discard { free: terminal });
        }
        let after = if terminal {
            self.after_terminal()
        } else {
            self
        };
        match completion {
            // Terminal for the armed op, so the index is free to reuse.
            RecvCompletion::Closed => (after, CqeStep::Remove),
            RecvCompletion::Empty => match after {
                RecvState::Idle => (RecvState::Armed, CqeStep::Arm),
                _ => (after, CqeStep::Nothing),
            },
            RecvCompletion::Data => match after {
                RecvState::Parked { .. } => (after, CqeStep::Keep),
                _ => (after, CqeStep::Process),
            },
        }
    }

    /// The state once the armed multishot has posted its last CQE.
    fn after_terminal(self) -> RecvState {
        match self {
            RecvState::Armed | RecvState::Resuming => RecvState::Idle,
            RecvState::Parked { since, .. } => RecvState::Parked {
                cancelling: false,
                since,
            },
            // Nothing was armed, or the caller frees it.
            RecvState::Idle | RecvState::Dying => self,
        }
    }

    /// The state after the connection's buffer was processed with
    /// `action` at `now`, and what the reader does next. A parked
    /// connection that parks again keeps its `since`.
    fn after_frames(self, action: FrameAction, now: Instant) -> (RecvState, FramesStep) {
        match action {
            FrameAction::Disconnect => (self, FramesStep::Teardown),
            FrameAction::Continue => match self {
                RecvState::Armed | RecvState::Resuming | RecvState::Dying => {
                    (self, FramesStep::Nothing)
                }
                RecvState::Parked {
                    cancelling: true, ..
                } => (RecvState::Resuming, FramesStep::Nothing),
                RecvState::Idle
                | RecvState::Parked {
                    cancelling: false, ..
                } => (RecvState::Armed, FramesStep::Arm),
            },
            FrameAction::Park => match self {
                RecvState::Armed => (
                    RecvState::Parked {
                        cancelling: true,
                        since: now,
                    },
                    FramesStep::Cancel,
                ),
                RecvState::Resuming => (
                    RecvState::Parked {
                        cancelling: true,
                        since: now,
                    },
                    FramesStep::Nothing,
                ),
                RecvState::Idle => (
                    RecvState::Parked {
                        cancelling: false,
                        since: now,
                    },
                    FramesStep::Nothing,
                ),
                RecvState::Parked { .. } | RecvState::Dying => (self, FramesStep::Nothing),
            },
        }
    }

    /// The state once teardown has begun, and what it takes.
    fn on_teardown(self) -> (RecvState, TeardownStep) {
        match self {
            RecvState::Dying => (self, TeardownStep::AlreadyDying),
            RecvState::Idle
            | RecvState::Parked {
                cancelling: false, ..
            } => (self, TeardownStep::FreeNow),
            RecvState::Armed => (RecvState::Dying, TeardownStep::Cancel),
            RecvState::Resuming
            | RecvState::Parked {
                cancelling: true, ..
            } => (RecvState::Dying, TeardownStep::AwaitCancel),
        }
    }

    /// Whether, at `now`, a park's cancel has been in flight for longer
    /// than [`PARK_CANCEL_DEADLINE`]: the host did not honour it.
    fn park_cancel_overdue(self, now: Instant) -> bool {
        match self {
            RecvState::Parked {
                cancelling: true,
                since,
            } => now.duration_since(since) > PARK_CANCEL_DEADLINE,
            RecvState::Armed
            | RecvState::Idle
            | RecvState::Resuming
            | RecvState::Dying
            | RecvState::Parked {
                cancelling: false, ..
            } => false,
        }
    }

    /// Whether the kernel still owes this connection a CQE: what the
    /// shutdown quiesce waits out.
    fn kernel_owes_cqe(self) -> bool {
        match self {
            RecvState::Armed
            | RecvState::Resuming
            | RecvState::Dying
            | RecvState::Parked {
                cancelling: true, ..
            } => true,
            RecvState::Idle
            | RecvState::Parked {
                cancelling: false, ..
            } => false,
        }
    }
}

/// Index-stable allocator for connection state. Slab indices are used as
/// io_uring user_data, avoiding fd-reuse races.
struct ConnectionSlab<R> {
    entries: Vec<Option<ConnectionEntry<R>>>,
    /// Recycled indices for O(1) allocation.
    free: Vec<usize>,
}

impl<R> ConnectionSlab<R> {
    fn new() -> Self {
        Self {
            entries: Vec::with_capacity(256),
            free: Vec::new(),
        }
    }

    /// Insert a connection, returning its stable slab index.
    fn insert(&mut self, entry: ConnectionEntry<R>) -> usize {
        if let Some(idx) = self.free.pop() {
            self.entries[idx] = Some(entry);
            idx
        } else {
            let idx = self.entries.len();
            self.entries.push(Some(entry));
            idx
        }
    }

    fn get_mut(&mut self, idx: usize) -> Option<&mut ConnectionEntry<R>> {
        self.entries.get_mut(idx).and_then(|e| e.as_mut())
    }

    /// Remove and return a connection entry, recycling its index.
    fn remove(&mut self, idx: usize) -> Option<ConnectionEntry<R>> {
        if let Some(slot) = self.entries.get_mut(idx) {
            let removed = slot.take();
            if removed.is_some() {
                self.free.push(idx);
            }
            removed
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Main loop
// ---------------------------------------------------------------------------

/// How the reader should approach the kernel before draining the CQ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RingEntry {
    /// Completions are visible and there is nothing to hand over — drain
    /// from the mmap'd ring without a syscall.
    Skip,
    /// SQEs to submit, but completions are already visible: hand them
    /// over and come straight back rather than asking to wait.
    Submit,
    /// Nothing to process yet — submit whatever is queued and park.
    SubmitAndWait,
}

/// The reader's enter policy. Two rules: never park while completions
/// are already visible, and never skip the syscall while SQEs are
/// waiting to be handed over.
#[inline]
fn ring_entry(sq_pending: usize, cq_ready: bool) -> RingEntry {
    match (sq_pending > 0, cq_ready) {
        (false, true) => RingEntry::Skip,
        (true, true) => RingEntry::Submit,
        (_, false) => RingEntry::SubmitAndWait,
    }
}

/// Main io_uring reader loop. Runs until channel disconnection.
///
/// When `tick_cadence` is `Some`, the loop also generates the application's
/// clock ticks — see [`spawn_reader`] for the rationale.
#[allow(clippy::too_many_arguments)]
fn reader_loop<A: Application, R: AsRawFd>(
    command_rx: mpsc::Receiver<ReaderRegistration<R>>,
    // Shared with `UringReaderHandle`. Taken by value so this thread keeps
    // the descriptor alive for as long as it is armed in the ring, and
    // releases it by dropping the `Arc` rather than by closing the fd.
    wakeup_fd: Arc<OwnedFd>,
    mut producer: ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn ErasedDecoder<A::Event>,
    halt: &HaltGate,
    mut early_replies: EarlyReplySender<A::Report>,
    mut ack_policy: AckPolicyPublisher,
    control_tx: &mpsc::Sender<ControlEvent>,
    connection_timeout: Option<Duration>,
    tick_cadence: Option<Duration>,
    sizing: RingSizing,
    // Startup report to `spawn_reader`: `Ok` once the ring and the buffer
    // pool are in place, the error otherwise (and the loop never starts).
    ready: mpsc::SyncSender<std::io::Result<()>>,
    shutdown: &AtomicBool,
) {
    let num_buffers = sizing.provided_buffers();
    let ring_entries = sizing.ring_entries();

    // Kernel-referenced memory is declared BEFORE the io_uring so it
    // drops AFTER the ring on every exit path, including panic unwind
    // (locals drop in reverse declaration order). The kernel holds live
    // references into all three for as long as the ring fd is open:
    // armed multishot RECVs select entries from the buf_ring and write
    // into the pool, and the armed eventfd READ writes its buffer.

    // Eventfd read buffer — boxed for pointer stability across SQE lifetimes.
    let mut eventfd_buf: Box<[u8; 8]> = Box::new([0u8; 8]);

    // Shared buffer pool for provided buffers. Contiguous allocation of
    // `num_buffers × BUF_SIZE` bytes. The kernel selects a buffer from
    // this pool for each recv completion, identified by buffer ID in the
    // CQE.
    let mut buffer_pool = vec![0u8; num_buffers as usize * BUF_SIZE].into_boxed_slice();

    // Ring-mapped provided-buffer ring: recycling a consumed buffer is a
    // shared-memory store, not a ProvideBuffers SQE — see `buf_ring`.
    // Allocated unconditionally (16 bytes per buffer, page-aligned) so
    // its declaration precedes the io_uring's even when the legacy
    // fallback below ends up in use.
    let mut buf_ring = BufRing::new(num_buffers, buffer_pool.as_mut_ptr(), BUF_SIZE);

    // SINGLE_ISSUER: this thread creates the ring and is the only one
    // that ever submits — lets the kernel skip SQ locking, and turns any
    // future cross-thread submission bug into an immediate EEXIST
    // instead of a silent race. Same rationale as the journal and
    // replication rings. (COOP_TASKRUN/DEFER_TASKRUN deliberately not
    // set — see the journal ring's measured rationale in
    // melin-transport-core::pipeline.)
    //
    // A failure here is reported to `spawn_reader` as a startup error
    // rather than panicking the thread: the usual cause is the
    // locked-memory limit, which the operator can raise.
    let mut ring: IoUring = match IoUring::builder().setup_single_issuer().build(ring_entries) {
        Ok(ring) => ring,
        Err(e) => {
            // Dropped send: a spawner that stopped waiting is unwinding,
            // and the thread returns either way.
            let _ = ready.send(Err(ring_setup_error("uring-reader", ring_entries, e)));
            return;
        }
    };

    // Prefer the buf_ring; fall back to legacy ProvideBuffers SQEs if
    // the kernel rejects the registration. Not theoretical: PBUF_RING
    // needs kernel ≥ 5.19, and some virtualized hosts filter newer
    // io_uring register opcodes (EINVAL) while reporting a modern
    // uname. The fallback costs one SQE + one CQE per received chunk —
    // degraded but fully functional, hence warn (see log conventions).
    let use_buf_ring = match buf_ring.register(&ring, BUF_GROUP_ID) {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!(
                error = %e,
                "buf_ring registration rejected — falling back to legacy \
                 ProvideBuffers recycling (kernel < 5.19, or a hypervisor \
                 filtering io_uring register opcodes)"
            );
            if let Err(e) = register_buffer_pool(&mut ring, buffer_pool.as_mut_ptr(), num_buffers) {
                // Dropped send: as above.
                let _ = ready.send(Err(std::io::Error::new(
                    e.kind(),
                    format!("uring-reader: cannot provide its receive buffers: {e}"),
                )));
                return;
            }
            false
        }
    };

    // Started. A failed send means `spawn_reader` is no longer waiting —
    // its thread is unwinding — so nothing would ever register a
    // connection or stop this loop: return instead of running headless.
    if ready.send(Ok(())).is_err() {
        return;
    }

    let mut slab = ConnectionSlab::<R>::new();
    // Reverse map for cleanup when a connection's fd needs removal.
    // HashMap for O(1) lookup by fd. Sized for typical connection counts.
    let mut fd_to_slab: HashMap<RawFd, usize> = HashMap::with_capacity(256);

    // Pre-allocated CQE collection buffer. We must collect CQEs before
    // processing because the CQ borrow must end before pushing new SQEs.
    // Stores (user_data, result, flags) — flags needed for buffer ID and
    // multishot continuation. Sized to the CQ depth (2× the SQ) so even
    // a maximal drain never reallocates mid-loop.
    let mut cqes: Vec<(u64, i32, u32)> = Vec::with_capacity(ring_entries as usize * 2);

    // Submit the initial eventfd read so we wake on first connection.
    // `eventfd_armed` mirrors whether that READ is pushed/armed — the
    // kernel owes its CQE and writes `eventfd_buf` when it lands — and
    // is maintained at every push/consume site so the teardown quiesce
    // below can prove when the buffer is free of kernel references.
    push_eventfd_read(&mut ring, wakeup_fd.as_raw_fd(), eventfd_buf.as_mut_ptr());
    let mut eventfd_armed = true;

    // Stage histograms via the global registry. `publish` is the
    // narrow ring-publish call cost (lightweight, gated on
    // `latency-trace`); `ingest` is the full per-frame reader cost
    // and feeds the bench's tick-to-trade decomposition (heavier,
    // gated on `tick-to-trade`).
    #[cfg(feature = "latency-trace")]
    let mut publish_rec =
        melin_transport_core::trace::register_stage("reader: publish (decode → disruptor publish)");
    #[cfg(feature = "tick-to-trade")]
    let mut ingest_rec =
        melin_transport_core::trace::register_stage("reader: ingest (recv_ts → publish complete)");
    // Paces the recorder flush at the tail of each loop iteration. This
    // thread parks in `submit_and_wait` when there is no traffic, so
    // without an explicit flush its samples never reach the registry.
    #[cfg(feature = "latency-trace")]
    let mut last_stats_flush = Instant::now();

    // Coarse gate for timeout scanning — avoids scanning on every
    // submit_and_wait return during high throughput.
    let mut last_timeout_scan = Instant::now();
    // Pre-allocated buffer for stale connection indices to avoid
    // heap allocation inside the hot loop.
    let mut stale: Vec<usize> = Vec::new();

    // Parking state. `any_parked` is set whenever a connection parks and
    // recomputed by each retry pass, so it may overstate (a parked
    // connection removed since) but never understate: an overstatement
    // costs one pass and one timer. `park_retry_ts` is the timer's length;
    // like `tick_ts` it must outlive the submit that reads it, so it lives
    // as long as the loop.
    let mut any_parked = false;
    let mut park_retry_armed = false;
    let park_retry_ts = types::Timespec::new()
        .sec(PARK_RETRY.as_secs())
        .nsec(PARK_RETRY.subsec_nanos());
    let mut long_park_report = LongParkReport::default();

    // Tick generator state. `tick_schedule` holds the monotonic instant the
    // next `JournalEvent::Tick` should fire and the floor that keeps the
    // published wall-clock timestamps strictly monotonic (NTP can step the
    // wall clock backwards) — shared with the DPDK poll thread, see
    // `TickSchedule`. `tick_armed` tracks whether
    // an `IORING_OP_TIMEOUT` SQE is currently pending; we keep at most one.
    //
    // `tick_ts` lives across loop iterations because the kernel reads its
    // bytes via the SQE's addr field at submit time, not at push time. If
    // we declared it inside the `if !tick_armed` arm-timeout block, the
    // value would be dropped before the `submit_and_wait` below — the
    // kernel would then read freed stack memory. (See `md-gateway` for the
    // same pattern: it stores Timespec as a long-lived struct field.)
    let tick_enabled = tick_cadence.is_some();
    let cadence = tick_cadence.unwrap_or(Duration::ZERO);
    let mut tick_schedule = TickSchedule::new(cadence, Instant::now());
    let mut tick_armed = false;
    // Arm the very first timeout here, before entering the loop. This both
    // (a) makes the initial `tick_ts` value actually read by the kernel
    // (silencing the unused-assignment lint, since rustc cannot see kernel
    // pointer reads) and (b) ensures the first `submit_and_wait` returns at
    // the cadence even if no client traffic ever arrives.
    let mut tick_ts = types::Timespec::new()
        .sec(cadence.as_secs())
        .nsec(cadence.subsec_nanos());
    if tick_enabled {
        let sqe = opcode::Timeout::new(&tick_ts)
            .build()
            .user_data(TICK_TIMEOUT_TOKEN);
        unsafe {
            ring.submission()
                .push(&sqe)
                .expect("io_uring SQ full while arming initial tick timeout");
        }
        tick_armed = true;
        tracing::info!(
            cadence_ms = cadence.as_millis() as u64,
            "tick generator integrated into reader thread"
        );
    }

    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // An `ACK-POLICY` request: journaled here, by the ring's one
        // producer, before parking again. One load when none waits; the
        // requester's eventfd write is what brought a parked loop here.
        ack_policy.poll(&mut producer, halt);

        // Tick generator: emit any due tick before sleeping again. Done before
        // the timeout-arm so that a freshly-emitted tick re-arms a timeout for
        // the *new* deadline.
        if tick_enabled {
            if tick_schedule.publish_if_due(Instant::now(), unix_epoch_nanos, &mut producer) {
                // The previous timeout (if any) is now stale; let it fire and
                // be ignored, then arm a new one below.
                tick_armed = false;
            }

            if !tick_armed {
                let remaining = tick_schedule
                    .next_deadline()
                    .saturating_duration_since(Instant::now());
                // Update the loop-scoped Timespec in place. The kernel reads
                // it via the SQE's addr pointer on submit_and_wait below, so
                // the binding must outlive that call (it does — outer scope).
                tick_ts = types::Timespec::new()
                    .sec(remaining.as_secs())
                    .nsec(remaining.subsec_nanos());
                let sqe = opcode::Timeout::new(&tick_ts)
                    .build()
                    .user_data(TICK_TIMEOUT_TOKEN);
                // SAFETY: `tick_ts` is loop-scoped and outlives the
                // submit that reads it (see its declaration).
                unsafe { push_sqe(&mut ring, &sqe, "the tick TIMEOUT") };
                tick_armed = true;
            }
        }

        // While a connection is parked, nothing but this timer is sure to
        // bring the loop back round to retry it.
        if any_parked && !park_retry_armed {
            let sqe = opcode::Timeout::new(&park_retry_ts)
                .build()
                .user_data(PARK_RETRY_TOKEN);
            // SAFETY: `park_retry_ts` lives as long as the loop, past the
            // submit that reads it.
            unsafe { push_sqe(&mut ring, &sqe, "the park-retry TIMEOUT") };
            park_retry_armed = true;
        }

        // Enter the kernel only when there is a reason to.
        //
        // Under load the CQ usually already holds completions by the
        // time we get here: in the ring's default task-work mode the
        // kernel posts them while this thread is in userspace parsing.
        // Reading that is pure userspace — the CQ is mmap'd and
        // `completion()` loads the tail with `Acquire` — so when there
        // is also nothing to hand over, the `io_uring_enter` is a
        // ~200 ns mode switch that buys nothing. The replication sender
        // measured the same skip at ~6 % of its thread's cycles
        // (`tcp_sender.rs`); this loop makes one enter per drain.
        //
        // When there ARE SQEs to submit we still enter, but we do not
        // ask the kernel to *wait* if completions are already visible.
        // Only a genuinely empty CQ parks the thread.
        //
        // Note this is the half of the io_uring-audit item that pairs
        // *against* `DEFER_TASKRUN`: deferred task work only runs on an
        // enter with GETEVENTS, so under that flag the CQ would be empty
        // here and the skip would simply never fire.
        let sq_pending = ring.submission().len();
        let cq_ready = !ring.completion().is_empty();
        let entered = match ring_entry(sq_pending, cq_ready) {
            RingEntry::Skip => Ok(0),
            RingEntry::Submit => ring.submit(),
            RingEntry::SubmitAndWait => ring.submit_and_wait(1),
        };
        match entered {
            Ok(_) => {}
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => {
                error!(error = %e, "io_uring submit/wait error");
                break;
            }
        }

        // Drain all available CQEs into the pre-allocated buffer.
        // Must collect before processing because the CQ borrow must end
        // before we can push new SQEs to the SQ.
        cqes.clear();
        cqes.extend(
            ring.completion()
                .map(|cqe| (cqe.user_data(), cqe.result(), cqe.flags())),
        );

        let batch_now = Instant::now();
        // One wall-clock read per CQE batch instead of per request: at
        // peak request rates a per-request `unix_epoch_nanos()` (vDSO
        // `clock_gettime(CLOCK_REALTIME)`) showed up in the primary's
        // profile. All requests in the same batch share the timestamp —
        // precision loss is bounded by the CQE-drain cadence. The timestamp
        // drives the application's clock but orders nothing (the pipeline
        // orders by sequence), so that resolution is enough.
        let batch_wall_ns = unix_epoch_nanos();

        for &(token, result, flags) in &cqes {
            // ── Tick timeout ──
            // The CQE is just a wakeup signal — the actual tick emission
            // happens at the top of the next loop iteration via the
            // deadline check, so the time the tick is stamped with reflects
            // unix_epoch_nanos at fire time, not at submit time.
            if token == TICK_TIMEOUT_TOKEN {
                tick_armed = false;
                continue;
            }

            // ── Park-retry timeout ──
            // A wakeup and nothing more: the retry pass after the drain
            // runs on every wake.
            if token == PARK_RETRY_TOKEN {
                park_retry_armed = false;
                continue;
            }

            // ── Legacy ProvideBuffers completion (fallback mode only) ──
            if token == PROVIDE_BUFS_TOKEN {
                if result < 0 {
                    error!(error = result, "ProvideBuffers failed");
                }
                continue;
            }

            // ── AsyncCancel completion ──
            if token == CANCEL_TOKEN {
                // Nothing to do: the cancelled op's own terminal CQE
                // drives the state machine; ENOENT/EALREADY just mean
                // it already finished on its own. A filtered cancel
                // (EINVAL) leaves a parked connection's receive armed;
                // the retry pass closes it (`PARK_CANCEL_DEADLINE`).
                continue;
            }

            // ── Eventfd wakeup ──
            if token == EVENTFD_TOKEN {
                // This CQE is the armed READ completing; the re-arm at
                // the end of the branch replaces it before anything can
                // observe the gap, so `eventfd_armed` stays `true`
                // through the pair. The teardown drain clears it for
                // the one case where a completion is never processed
                // here (a CQE still queued when the loop breaks).
                if result >= 0 {
                    // Process all pending registrations.
                    while let Ok(reg) = command_rx.try_recv() {
                        let fd = reg.reader.as_raw_fd();
                        let entry = ConnectionEntry {
                            connection_id: reg.connection_id.0,
                            addr: reg.addr,
                            role: reg.role,
                            key_hash: reg.key_hash,
                            fd,
                            _reader: reg.reader,
                            ingress: ClientIngress::new(Vec::with_capacity(MAX_FRAME_SIZE + 4)),
                            // Armed just below.
                            recv: RecvState::Armed,
                            last_activity: Instant::now(),
                        };
                        let idx = slab.insert(entry);
                        fd_to_slab.insert(fd, idx);

                        // Submit multishot RECV for this connection.
                        push_recv_multi(&mut ring, fd, idx);
                    }
                } else {
                    error!(error = result, "eventfd read error");
                }

                // Re-submit eventfd read for the next wakeup
                // (`eventfd_armed` stays `true` — see above).
                push_eventfd_read(&mut ring, wakeup_fd.as_raw_fd(), eventfd_buf.as_mut_ptr());
                continue;
            }

            // ── Connection multishot RECV completion ──

            let slab_idx = token as usize;
            // A cleared F_MORE means THIS armed op posts no further CQEs.
            let terminal = (flags & IORING_CQE_F_MORE) == 0;

            // Extract the buffer (if any) before acting on `result`: a
            // buffer can ride ANY recv CQE, and one that is not recycled
            // is leaked from the pool forever. On 6.8 the kernel
            // recycles internally before posting error/EOF CQEs, but the
            // supported floor is 5.19, where that guarantee could not be
            // established — the defensive recycle is one branch.
            let buf_id = if (flags & IORING_CQE_F_BUFFER) != 0 {
                Some((flags >> IORING_CQE_BUFFER_SHIFT) as usize)
            } else {
                None
            };

            let Some(entry) = slab.get_mut(slab_idx) else {
                // No connection holds the index: an index is freed only
                // once nothing armed can post for it, so this does not
                // happen; its buffer goes back all the same.
                if let Some(bid) = buf_id {
                    recycle_buffer(&mut ring, &mut buf_ring, use_buf_ring, &buffer_pool, bid);
                }
                continue;
            };

            // `ENOBUFS`: the pool ran dry at buffer-selection time, and
            // the multishot ended WITHOUT reading — the bytes sit in the
            // socket buffer. Not a client error, and it must not
            // disconnect (a bare `result <= 0 ⇒ remove` here dropped
            // innocent clients under burst): the receive is re-armed, and
            // recycles from data CQEs earlier in this drain have already
            // refilled the buf_ring (publication is immediate), while the
            // re-arm SQE submits after the whole drain, so the retry finds
            // buffers. `ECANCELED`: a park's cancel landed. A data CQE
            // without a buffer cannot happen with buffer-select receives;
            // defensively, it is taken as no bytes.
            let completion = if result == -libc::ENOBUFS || result == -libc::ECANCELED {
                RecvCompletion::Empty
            } else if result <= 0 {
                RecvCompletion::Closed
            } else if buf_id.is_none() {
                debug!(slab_idx, "recv CQE without buffer flag");
                RecvCompletion::Empty
            } else {
                RecvCompletion::Data
            };
            let (recv, step) = entry.recv.on_cqe(completion, terminal);
            entry.recv = recv;
            let fd = entry.fd;

            let (Some(buf_id), CqeStep::Process | CqeStep::Keep) = (buf_id, step) else {
                // No bytes to take: a buffer riding the CQE (one can ride
                // ANY recv CQE, as above) goes straight back.
                if let Some(bid) = buf_id {
                    recycle_buffer(&mut ring, &mut buf_ring, use_buf_ring, &buffer_pool, bid);
                }
                match step {
                    CqeStep::Discard { free } => {
                        // A torn-down connection: its bytes belong to a
                        // repudiated peer, and its index is freed only at
                        // the terminal CQE, so it cannot be handed to a
                        // new registration while the kernel can still post
                        // CQEs carrying it.
                        if free && let Some(dead) = slab.remove(slab_idx) {
                            debug!(
                                connection_id = dead.connection_id,
                                "teardown complete, slab index released"
                            );
                        }
                    }
                    CqeStep::Remove => {
                        // Disconnect (0) or error (negative errno) —
                        // terminal for the armed op, so the index is
                        // immediately safe to free.
                        if let Some(removed) = slab.remove(slab_idx) {
                            if result == 0 {
                                debug!(
                                    connection_id = removed.connection_id,
                                    addr = %removed.addr,
                                    "client disconnected"
                                );
                            } else {
                                debug!(
                                    connection_id = removed.connection_id,
                                    addr = %removed.addr,
                                    error = result,
                                    "recv error"
                                );
                            }
                            fd_to_slab.remove(&removed.fd);
                            // Dropped error: a dead control channel means
                            // the response stage is gone and the server is
                            // shutting down.
                            let _ = control_tx.send(ControlEvent::Disconnected {
                                connection_id: removed.connection_id,
                            });
                        }
                    }
                    CqeStep::Arm => push_recv_multi(&mut ring, fd, slab_idx),
                    // `Process` and `Keep` without a buffer are `Empty`'s,
                    // which never yields them.
                    CqeStep::Nothing | CqeStep::Process | CqeStep::Keep => {}
                }
                continue;
            };

            let n = result as usize;

            // Trace timestamp: the moment the kernel handed us this recv's
            // bytes. Captured once per CQE — not per frame — and stamped
            // onto every InputSlot parsed from this buffer below, so the
            // reader-ingest and server-e2e stages measure from true wire
            // receipt (frame decode included) rather than re-sampling after
            // each decode (which excluded decode and drifted forward for
            // later frames in a multi-frame recv).
            #[allow(clippy::let_unit_value)] // ZST when latency-trace is off
            let recv_ts = melin_transport_core::trace::mono_trace_ns();

            // Any successful recv resets the idle timeout.
            entry.last_activity = batch_now;

            // Copy received data from the shared buffer pool into the
            // connection's parse buffer: kept even while it is parked (a
            // CQE that landed before the park's cancel), for the retry.
            let buf_start = buf_id * BUF_SIZE;
            entry
                .ingress
                .extend_from_slice(&buffer_pool[buf_start..buf_start + n]);

            // Recycle the consumed buffer. Must happen after the copy-out
            // above — from this line on the kernel may write fresh recv
            // data into the slot.
            recycle_buffer(&mut ring, &mut buf_ring, use_buf_ring, &buffer_pool, buf_id);

            if step == CqeStep::Keep {
                continue;
            }

            // Extract and publish complete frames.
            let action = process_frames::<A, R>(
                entry,
                &mut producer,
                decoder,
                halt,
                &mut early_replies,
                batch_wall_ns,
                recv_ts,
                #[cfg(feature = "latency-trace")]
                &mut publish_rec,
                #[cfg(feature = "tick-to-trade")]
                &mut ingest_rec,
            );
            let (recv, step) = entry.recv.after_frames(action, batch_now);
            entry.recv = recv;
            if matches!(recv, RecvState::Parked { .. }) {
                any_parked = true;
                debug!(connection_id = entry.connection_id, "connection parked");
            }
            apply_frames_step(
                &mut ring,
                &mut slab,
                &mut fd_to_slab,
                control_tx,
                slab_idx,
                step,
            );
        }

        // Retry every parked connection's buffer: on the park-retry
        // timer, and on any other wake. One that comes back processed is
        // read again; one that parks again stays parked.
        if any_parked {
            any_parked = false;
            let mut long_parked = 0usize;
            #[allow(clippy::let_unit_value)] // ZST when latency-trace is off
            let recv_ts = melin_transport_core::trace::mono_trace_ns();
            for idx in 0..slab.entries.len() {
                let Some(entry) = slab.get_mut(idx) else {
                    continue;
                };
                let RecvState::Parked { since, .. } = entry.recv else {
                    continue;
                };
                if entry.recv.park_cancel_overdue(batch_now) {
                    tracing::warn!(
                        connection_id = entry.connection_id,
                        "a parked connection's receive cancel went unanswered (is io_uring \
                         AsyncCancel filtered on this host?): closing the connection"
                    );
                    begin_teardown(&mut ring, &mut slab, &mut fd_to_slab, control_tx, idx);
                    continue;
                }
                let action = process_frames::<A, R>(
                    entry,
                    &mut producer,
                    decoder,
                    halt,
                    &mut early_replies,
                    batch_wall_ns,
                    recv_ts,
                    #[cfg(feature = "latency-trace")]
                    &mut publish_rec,
                    #[cfg(feature = "tick-to-trade")]
                    &mut ingest_rec,
                );
                let (recv, step) = entry.recv.after_frames(action, batch_now);
                entry.recv = recv;
                match action {
                    // Still parked, since it first parked.
                    FrameAction::Park => {
                        any_parked = true;
                        if batch_now.duration_since(since) > LONG_PARK {
                            long_parked += 1;
                        }
                    }
                    FrameAction::Continue => {
                        // Not read while parked: its idle clock restarts
                        // now, not at its last bytes.
                        entry.last_activity = batch_now;
                        debug!(connection_id = entry.connection_id, "connection resumed");
                    }
                    // Torn down below.
                    FrameAction::Disconnect => {}
                }
                apply_frames_step(&mut ring, &mut slab, &mut fd_to_slab, control_tx, idx, step);
            }
            if long_park_report.due(long_parked, batch_now) {
                tracing::warn!(
                    connections = long_parked,
                    "client connections parked for over a second: the response stage \
                     is not taking early replies, and these clients are not read"
                );
            }
        }

        // Scan for idle connections that have exceeded the timeout.
        // Coarse gate: only scan once per second to avoid unnecessary
        // iteration during high-throughput phases when submit_and_wait
        // returns immediately with CQEs.
        if let Some(timeout) = connection_timeout {
            let now = Instant::now();
            if now.duration_since(last_timeout_scan) >= Duration::from_secs(1) {
                last_timeout_scan = now;
                stale.clear();
                for (idx, slot) in slab.entries.iter().enumerate() {
                    // Dying entries are already mid-teardown: their
                    // `last_activity` stays stale by design, and a second
                    // teardown would emit a duplicate Disconnected. A
                    // parked connection is not idle: the node is not
                    // reading it.
                    if let Some(entry) = slot
                        && !matches!(entry.recv, RecvState::Dying | RecvState::Parked { .. })
                        && now.duration_since(entry.last_activity) > timeout
                    {
                        debug!(
                            connection_id = entry.connection_id,
                            addr = %entry.addr,
                            "connection timed out"
                        );
                        stale.push(idx);
                    }
                }
                for &idx in &stale {
                    begin_teardown(&mut ring, &mut slab, &mut fd_to_slab, control_tx, idx);
                }
            }
        }

        // Hand buffered latency samples to the stats registry before
        // parking in `submit_and_wait` at the top of the next
        // iteration — once traffic stops this thread can sleep in the
        // kernel indefinitely, which is precisely when the bench
        // scrapes /stats-dump. Reuses the per-batch clock read.
        #[cfg(feature = "latency-trace")]
        if batch_now.duration_since(last_stats_flush)
            >= melin_transport_core::trace::IDLE_FLUSH_INTERVAL
        {
            last_stats_flush = batch_now;
            publish_rec.flush();
            #[cfg(feature = "tick-to-trade")]
            ingest_rec.flush();
        }
    }

    // ── Teardown quiesce ────────────────────────────────────────────
    // Armed operations hold kernel pointers into the buffer pool, the
    // buf_ring memory, and the eventfd buffer. Closing the ring fd only
    // triggers ASYNCHRONOUS cancellation (the kernel's ring-exit work),
    // which can still be touching those allocations after this function
    // returns and frees them — the same corruption class fixed on both
    // replication transports (see `crate::uring_teardown`). Quiesce
    // *provably*: wake every armed operation, then reap CQEs until none
    // is owed, and leak the buffers if that cannot be shown within the
    // deadline. "CQ went quiet" is not proof — a wake that silently
    // failed leaves armed ops and a quiet CQ.
    //
    // The wake cannot rely on the cancel opcode alone: some hosts
    // filter io_uring opcodes (the reason the buf_ring path has a
    // legacy fallback), and a filtered cancel completes with EINVAL
    // while the armed operations live on. So every operation that has
    // to be proven down gets a wake it must answer: `shutdown(2)` on
    // each connection socket completes its multishot RECV with EOF,
    // and a wakeup write completes the armed eventfd READ. The cancel
    // is still pushed as an accelerator where the host honours it. The
    // tick timeout is not in the proof at all — see `owed` below.
    // Panic unwind skips all of this and accepts the (tiny,
    // process-is-dying) exit-work window; declaration order still
    // closes the ring fd before the frees.
    let proven = {
        let cancel_all = opcode::AsyncCancel2::new(types::CancelBuilder::any())
            .build()
            .user_data(CANCEL_TOKEN);
        unsafe {
            // Ignore a full SQ: the wakes below do not depend on it.
            let _ = ring.submission().push(&cancel_all);
        }
        for entry in slab.entries.iter().flatten() {
            crate::uring_teardown::wake_pending_ops(entry.fd);
        }
        // Completes the armed eventfd READ into `eventfd_buf` (still
        // alive). Best-effort: on failure the cancel is the fallback.
        let wake: u64 = 1;
        unsafe {
            libc::write(
                wakeup_fd.as_raw_fd(),
                &wake as *const u64 as *const libc::c_void,
                8,
            );
        }
        // Flush SQEs pushed but never submitted (re-arms from the last
        // batch, the cancel above): until they reach the kernel no CQE
        // is coming for them, but their armed-state flags are already
        // set, so abandoning them in the SQ would stall the proof
        // below. On a submit error the flags stay set and the deadline
        // routes to the leak path — the conservative outcome.
        loop {
            match ring.submit() {
                Ok(_) => break,
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(_) => break,
            }
        }
        let mut backoff = crate::uring_teardown::DrainBackoff::new();
        loop {
            for cqe in ring.completion() {
                match cqe.user_data() {
                    EVENTFD_TOKEN => eventfd_armed = false,
                    // None of these touch the allocations freed below,
                    // so reaping them settles nothing: cancel results
                    // carry no information, legacy ProvideBuffers
                    // completions only registered addresses, and the
                    // tick timeout reads nothing after submit (see
                    // `owed`), nor does the park-retry timeout, for the
                    // same reason.
                    PROVIDE_BUFS_TOKEN | CANCEL_TOKEN | TICK_TIMEOUT_TOKEN | PARK_RETRY_TOKEN => {}
                    // Connection tokens are slab indices.
                    idx => {
                        // A terminal CQE retires that connection's arm.
                        // Data CQEs landing mid-teardown are discarded
                        // and their buffers deliberately not recycled —
                        // nothing re-arms, so the pool cannot be
                        // selected from again once every arm is down.
                        // Whatever state it was in, the kernel owes it
                        // nothing more: `Idle` says so (the entry is never
                        // used again).
                        if (cqe.flags() & IORING_CQE_F_MORE) == 0
                            && let Some(entry) = slab.get_mut(idx as usize)
                        {
                            entry.recv = RecvState::Idle;
                        }
                    }
                }
            }
            // `tick_armed` is deliberately absent: an armed
            // `IORING_OP_TIMEOUT` holds no pointer into any of the
            // allocations below. The kernel copies its `Timespec` out
            // of `tick_ts` when the SQE is *submitted* (which is why
            // that binding only has to outlive the submit — see its
            // declaration), and the ring's own exit work retires the
            // timer. Waiting for it would prove nothing and cost
            // plenty: nothing here can wake a timeout, so the only
            // things that retire one are it firing at its own cadence
            // — `--tick-interval-ms`, operator-set and unbounded — or
            // the cancel above, which is exactly what a host that
            // filters opcodes refuses. Gating the proof on it would
            // hand those hosts a full-deadline stall and a spurious
            // "leaking" warning on every clean shutdown.
            let owed = eventfd_armed
                || slab
                    .entries
                    .iter()
                    .flatten()
                    .any(|e| e.recv.kernel_owes_cqe());
            if !owed {
                break true;
            }
            if !backoff.wait() {
                break false;
            }
        }
    };
    if !proven {
        // The kernel may still write the pool or the eventfd buffer, or
        // read buf_ring entries. Leak all three rather than hand the
        // allocator memory the kernel still touches — a bounded one-off
        // cost on a thread that is exiting anyway. `mem::forget` on the
        // `BufRing` also skips its ring-memory dealloc, which is the
        // point.
        // Report what was still owed, not `tick_armed` — naming a
        // timer that was never part of the proof would point the next
        // reader at the wrong operation.
        tracing::warn!(
            eventfd_armed,
            armed_connections = slab
                .entries
                .iter()
                .flatten()
                .filter(|e| e.recv.kernel_owes_cqe())
                .count(),
            "io_uring teardown drain did not complete; leaking reader buffers"
        );
        std::mem::forget(buffer_pool);
        std::mem::forget(buf_ring);
        std::mem::forget(eventfd_buf);
    }

    // No `close` here. This thread shares the eventfd with
    // `UringReaderHandle`, which writes wakeups to it, and closing it from
    // this side left that write racing a reused descriptor number. Dropping
    // the `Arc` at the end of this scope releases our share; the descriptor
    // itself closes when the handle drops too.
}

// ---------------------------------------------------------------------------
// SQE helpers
// ---------------------------------------------------------------------------

/// Queue an SQE, first handing the already-queued ones to the kernel if
/// the submission queue is full.
///
/// [`RingSizing`] sizes the SQ for a drain's worth of SQEs on the
/// buf_ring path — two per connection plus housekeeping — so there the
/// flush fires only when the reader transiently tracks more connections
/// than the cap: a dying connection stays in the reader until its cancel
/// completes, while the response stage has already released its slot and
/// the accept loop may have admitted a replacement. The legacy
/// ProvideBuffers fallback adds one SQE per
/// recycled buffer on top, which the sizing does not count; this is what
/// keeps that path from overrunning the queue. Submitting mid-drain is
/// harmless: the CQEs being processed were copied out before any SQE was
/// pushed, and the queued SQEs only reach the kernel sooner.
///
/// Panics only if the submit itself fails, which leaves queued SQEs that
/// point into live buffers with nowhere to go.
///
/// # Safety
///
/// As for [`io_uring::SubmissionQueue::push`]: every buffer the SQE
/// references must stay valid until its operation completes.
unsafe fn push_sqe(ring: &mut IoUring, sqe: &io_uring::squeue::Entry, what: &str) {
    // SAFETY: forwarded from the caller.
    if unsafe { ring.submission().push(sqe) }.is_ok() {
        return;
    }
    loop {
        match ring.submit() {
            Ok(_) => break,
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => panic!("io_uring submit failed while making room for {what}: {e}"),
        }
    }
    // SAFETY: forwarded from the caller.
    unsafe { ring.submission().push(sqe) }.unwrap_or_else(|_| {
        panic!("io_uring SQ still full for {what} after submitting every queued SQE")
    });
}

/// Register the provided buffer pool via a legacy ProvideBuffers op —
/// the fallback when buf_ring registration is rejected. Submits
/// synchronously — called once at startup, before the reader reports
/// that it started, and only after the preferred path already failed.
fn register_buffer_pool(
    ring: &mut IoUring,
    pool_ptr: *mut u8,
    num_buffers: u16,
) -> std::io::Result<()> {
    let sqe = opcode::ProvideBuffers::new(pool_ptr, BUF_SIZE as i32, num_buffers, BUF_GROUP_ID, 0)
        .build()
        .user_data(PROVIDE_BUFS_TOKEN);

    // SAFETY: the pool outlives the ring (declared before it in
    // `reader_loop`), and ProvideBuffers only records its address.
    unsafe {
        ring.submission().push(&sqe).map_err(|_| {
            std::io::Error::other("io_uring SQ full during buffer pool registration")
        })?;
    }

    loop {
        match ring.submit_and_wait(1) {
            Ok(_) => break,
            Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
            Err(e) => return Err(e),
        }
    }

    // Check the completion result.
    let cqe = ring
        .completion()
        .next()
        .ok_or_else(|| std::io::Error::other("no CQE after ProvideBuffers"))?;
    if cqe.result() < 0 {
        return Err(std::io::Error::from_raw_os_error(-cqe.result()));
    }
    Ok(())
}

/// Re-provide a single consumed buffer back to the pool (legacy fallback
/// mode). Pushed to SQ without immediate submission — batched with the
/// next submit_and_wait. A drain can recycle up to the whole pool on top
/// of the SQEs the ring is sized for, so this is the push that relies on
/// [`push_sqe`] making room.
fn re_provide_buffer(ring: &mut IoUring, pool_ptr: *mut u8, buf_id: usize) {
    let buf_ptr = unsafe { pool_ptr.add(buf_id * BUF_SIZE) };
    let sqe = opcode::ProvideBuffers::new(buf_ptr, BUF_SIZE as i32, 1, BUF_GROUP_ID, buf_id as u16)
        .build()
        .user_data(PROVIDE_BUFS_TOKEN);

    // SAFETY: the buffer lies inside the pool, which outlives the ring.
    unsafe { push_sqe(ring, &sqe, "a legacy ProvideBuffers recycle") };
}

/// Return a consumed buffer to the shared pool, in whichever recycle
/// mode registration selected.
///
/// Hard assert, not debug: the bid comes from a kernel CQE, this path
/// runs per CQE (off the per-frame budget), and an out-of-range bid
/// handed back to the kernel would point a future recv's DMA at
/// arbitrary heap — the one failure mode worth an unconditional branch.
fn recycle_buffer(
    ring: &mut IoUring,
    buf_ring: &mut BufRing,
    use_buf_ring: bool,
    buffer_pool: &[u8],
    buf_id: usize,
) {
    assert!(
        buf_id < buffer_pool.len() / BUF_SIZE,
        "kernel returned out-of-pool buffer id {buf_id}"
    );
    if use_buf_ring {
        buf_ring.push(buf_id as u16);
    } else {
        re_provide_buffer(ring, buffer_pool.as_ptr() as *mut u8, buf_id);
    }
}

/// Begin tearing down a connection the reader decided to drop while its
/// multishot RECV may still be armed (malformed frame, idle timeout, a
/// superseded node), parked or not.
///
/// The slab index must NOT be freed yet: the kernel can still post CQEs
/// carrying it, and the slab's LIFO free list would hand the index to
/// the next registration — the old peer's bytes would then be parsed
/// under the new connection's identity, key hash, and permissions
/// (request injection; audit review F1). Instead: sever the peer,
/// mark the entry dying, cancel the armed op (unless a park's cancel is
/// already in flight), and let the terminal CQE free the index. When
/// nothing is owed there is nothing that can post — free immediately.
fn begin_teardown<R>(
    ring: &mut IoUring,
    slab: &mut ConnectionSlab<R>,
    fd_to_slab: &mut HashMap<RawFd, usize>,
    control_tx: &mpsc::Sender<ControlEvent>,
    idx: usize,
) {
    let Some(entry) = slab.get_mut(idx) else {
        return;
    };
    let (recv, step) = entry.recv.on_teardown();
    if step == TeardownStep::AlreadyDying {
        return;
    }
    entry.recv = recv;
    let connection_id = entry.connection_id;
    let fd = entry.fd;
    // Sever the peer now. The fd itself must stay open until the armed
    // op completes (the kernel holds a file reference for it anyway);
    // shutdown(2) stops both directions immediately. Best-effort:
    // ENOTCONN just means the peer already went away.
    unsafe {
        libc::shutdown(fd, libc::SHUT_RDWR);
    }
    fd_to_slab.remove(&fd);
    // Dropped error: a dead control channel means the response stage is
    // gone and the server is shutting down.
    let _ = control_tx.send(ControlEvent::Disconnected { connection_id });

    match step {
        TeardownStep::Cancel => push_recv_cancel(ring, idx, "a connection teardown's AsyncCancel"),
        // The park's cancel is in flight: its terminal CQE frees the index.
        TeardownStep::AwaitCancel => {}
        // Nothing owed ⇒ no future CQEs can carry this index.
        TeardownStep::FreeNow => {
            slab.remove(idx);
        }
        // Returned above, before the peer was told anything twice.
        TeardownStep::AlreadyDying => {}
    }
}

/// Act on what [`RecvState::after_frames`] decided for connection `idx`.
fn apply_frames_step<R>(
    ring: &mut IoUring,
    slab: &mut ConnectionSlab<R>,
    fd_to_slab: &mut HashMap<RawFd, usize>,
    control_tx: &mpsc::Sender<ControlEvent>,
    idx: usize,
    step: FramesStep,
) {
    match step {
        FramesStep::Nothing => {}
        FramesStep::Arm => {
            if let Some(entry) = slab.get_mut(idx) {
                push_recv_multi(ring, entry.fd, idx);
            }
        }
        // With nothing reading the socket its receive buffer fills, and
        // TCP's window closes on the client.
        FramesStep::Cancel => push_recv_cancel(ring, idx, "a park's AsyncCancel"),
        // Deferred teardown, NOT an immediate slab free — the multishot
        // may still be armed and its index must not be reused until the
        // terminal CQE (audit review F1).
        FramesStep::Teardown => begin_teardown(ring, slab, fd_to_slab, control_tx, idx),
    }
}

/// Push a multishot RECV SQE for connection `idx` on `fd`. The kernel will
/// produce CQEs continuously until EOF, error, or buffer pool exhaustion —
/// no resubmission needed unless multishot terminates. Only on a
/// transition of [`RecvState`] to `Armed`.
fn push_recv_multi(ring: &mut IoUring, fd: RawFd, idx: usize) {
    let sqe = opcode::RecvMulti::new(types::Fd(fd), BUF_GROUP_ID)
        .build()
        .user_data(idx as u64);

    // SAFETY: buffer-select RECV writes only into the provided-buffer
    // pool, which outlives the ring.
    unsafe { push_sqe(ring, &sqe, "a multishot RECV") };
}

/// Cancel connection `idx`'s armed multishot RECV (`what` names why, for
/// a failed push). Its terminal CQE, `ECANCELED` or whatever completed
/// first, drives the state machine; the cancel's own completion carries
/// nothing.
fn push_recv_cancel(ring: &mut IoUring, idx: usize, what: &str) {
    let sqe = opcode::AsyncCancel::new(idx as u64)
        .build()
        .user_data(CANCEL_TOKEN);
    // SAFETY: AsyncCancel references no memory.
    unsafe { push_sqe(ring, &sqe, what) };
}

/// Push a READ SQE for the eventfd (wakeup notification).
fn push_eventfd_read(ring: &mut IoUring, wakeup_fd: RawFd, buf: *mut u8) {
    let sqe = opcode::Read::new(types::Fd(wakeup_fd), buf, 8)
        .build()
        .user_data(EVENTFD_TOKEN);

    // SAFETY: `buf` is the boxed eventfd buffer, declared before the
    // ring in `reader_loop` so it outlives it.
    unsafe { push_sqe(ring, &sqe, "the eventfd READ") };
}

// ---------------------------------------------------------------------------
// Frame parsing
// ---------------------------------------------------------------------------

/// Extract complete frames from `conn.ingress` and publish them as
/// `InputSlot`s, answering through `early_replies` the requests not
/// published (see [`process_client_frames`](crate::client_frames::process_client_frames)).
/// `batch_wall_ns` is the wall-clock timestamp captured once per CQE batch
/// by the caller (see `reader_loop`); all non-query requests published in
/// this call share it, sparing the reader a per-request
/// `clock_gettime(CLOCK_REALTIME)` on the hot path. Returns what to do
/// with the connection: keep reading, close it, or park it.
#[allow(clippy::too_many_arguments)]
fn process_frames<A: Application, R>(
    conn: &mut ConnectionEntry<R>,
    producer: &mut ring::Producer<InputSlot<A::Event>>,
    decoder: &dyn ErasedDecoder<A::Event>,
    halt: &HaltGate,
    early_replies: &mut EarlyReplySender<A::Report>,
    batch_wall_ns: u64,
    recv_ts: melin_transport_core::trace::MonoTraceInstant,
    #[cfg(feature = "latency-trace")] publish_rec: &mut melin_transport_core::trace::StageRecorder,
    #[cfg(feature = "tick-to-trade")] ingest_rec: &mut melin_transport_core::trace::StageRecorder,
) -> FrameAction {
    // Every answer the reader decides on, a busy for a full ring among
    // them, goes to the response stage through the early-reply queue: the
    // response stage owns all egress on a client socket, so no reader-side
    // send can land between the two halves of a partially flushed reply.
    crate::client_frames::process_client_frames::<A>(
        &mut conn.ingress,
        conn.connection_id,
        conn.key_hash,
        conn.role,
        producer,
        decoder,
        halt,
        early_replies,
        batch_wall_ns,
        recv_ts,
        #[cfg(feature = "latency-trace")]
        publish_rec,
        #[cfg(feature = "tick-to-trade")]
        ingest_rec,
    )
}

#[cfg(test)]
mod tests {
    //! Unit tests for [`process_frames`]. The function has several exit
    //! paths (normal end, partial parse-buf, pipeline-full, oversize or
    //! non-request frame, full early-reply queue), each with subtle
    //! batch-commit ordering requirements. These tests pin that behaviour
    //! against a synthetic decoder so refactors of the batch path (e.g.
    //! moving the batch up to span the whole CQE drain) can't silently
    //! regress the "earlier frames must be visible before ServerBusy /
    //! disconnect" guarantees.
    use super::*;
    use crate::client_frames::MAX_REQUEST_BODY;
    use crate::halt::{EarlyKind, EarlyReply};
    use melin_app::auth::NoRoles;
    use melin_app::decoder::{Decoded, RequestDecoder};
    use melin_app::{AppEvent, Application, ApplyCtx, CodecError, QueryCtx, RejectReason};
    use melin_journal::JournalEvent;
    use melin_pipeline::ring::DisruptorBuilder;
    use melin_transport_core::pipeline::RequestId;
    use melin_wire_protocol::control::RefusalKind;
    use melin_wire_protocol::control_codec::{
        TAG_APP, TAG_CHALLENGE_RESPONSE, TAG_LEN, TAG_RESPONSE_HEARTBEAT,
    };
    use std::io::{ErrorKind, Read};
    use std::os::unix::net::UnixStream;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    /// The reader's enter policy. Each case is a syscall-per-drain
    /// decision on the hot path, and getting one wrong is either a lost
    /// wakeup (skipping with an empty CQ) or unsubmitted SQEs (skipping
    /// with a non-empty SQ) — both silent.
    mod ring_entry {
        use super::super::{RingEntry, ring_entry};

        #[test]
        fn visible_completions_and_nothing_to_submit_need_no_syscall() {
            assert_eq!(ring_entry(0, true), RingEntry::Skip);
        }

        #[test]
        fn queued_sqes_are_always_handed_over() {
            // Never skip with work in the SQ: those entries would sit
            // there until some later iteration happened to enter.
            assert_eq!(ring_entry(1, true), RingEntry::Submit);
            assert_eq!(ring_entry(8, false), RingEntry::SubmitAndWait);
        }

        #[test]
        fn an_empty_cq_is_the_only_thing_that_parks_the_thread() {
            assert_eq!(ring_entry(0, false), RingEntry::SubmitAndWait);
            // ...and visible work never does, whatever the SQ holds.
            for sq in [0, 1, 64] {
                assert_ne!(ring_entry(sq, true), RingEntry::SubmitAndWait);
            }
        }
    }

    /// Minimal `AppEvent` for these tests. `Copy` is required by `AppEvent`;
    /// the on-wire codec is unused because [`ByteDecoder`] never invokes it
    /// (frames are interpreted directly from their one body byte).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestEvent {
        Cmd(u8),
        Query,
    }

    impl AppEvent for TestEvent {
        const MAX_ENCODED_SIZE: usize = 2;

        fn encoded_size(&self) -> usize {
            // Unused — the tests never round-trip through encode/decode.
            2
        }
        fn encode(&self, _buf: &mut [u8]) -> usize {
            unreachable!("process_frames does not encode app events")
        }
        fn decode(_buf: &[u8]) -> Result<Self, CodecError> {
            unreachable!("process_frames does not decode app events directly")
        }
        fn is_query(&self) -> bool {
            matches!(self, TestEvent::Query)
        }
    }

    /// Placeholder `Application` impl. `process_frames` calls only
    /// `build_reject`, for a write refused while halted; the rest
    /// `unreachable!`.
    #[derive(Default)]
    struct TestApp;

    /// What [`TestApp::build_reject`] returns: the refused event and why,
    /// so a test can tell refusals apart.
    type TestReport = (TestEvent, RejectReason);

    impl Application for TestApp {
        type Event = TestEvent;
        type Report = TestReport;
        type QueryResponse = ();
        type Sizing = ();
        const APP_VERSION: u16 = 0;
        fn apply(&mut self, _event: TestEvent, _ctx: &ApplyCtx, _out: &mut Vec<TestReport>) {
            unreachable!()
        }
        fn query(&self, _event: TestEvent, _ctx: &QueryCtx) -> Option<()> {
            unreachable!()
        }
        fn tick(&mut self, _now_ns: u64, _out: &mut Vec<TestReport>) {
            unreachable!()
        }
        fn build_reject(event: &TestEvent, reason: RejectReason) -> TestReport {
            (*event, reason)
        }
        fn snapshot<W: std::io::Write>(&self, _w: &mut W) -> std::io::Result<()> {
            unreachable!()
        }
        fn restore<R: Read>(_r: &mut R) -> std::io::Result<Self> {
            unreachable!()
        }
    }

    /// Stateless decoder that maps a request's single body byte to a
    /// [`Decoded`] outcome. Lets each test feed a precise mix of permitted,
    /// filtered, denied, and decode-error frames without standing up the
    /// real wire codec.
    ///
    /// Body-byte mapping (`0x00..=0xFB` map 1:1 to a Permitted command,
    /// reserving the top four byte values for the non-Permitted outcomes):
    ///   * `0xFC` -> `Filter`
    ///   * `0xFD` -> `PermissionDenied`
    ///   * `0xFE` -> `DecodeError`
    ///   * `0xFF` -> `Permitted` with `is_query == true`
    ///   * `0x00..=0xFB` -> `Permitted(Cmd(byte))`
    struct ByteDecoder;

    impl RequestDecoder for ByteDecoder {
        type Event = TestEvent;
        // The reader never looks at a role: operator keys are enough.
        type Role = NoRoles;
        fn decode(&self, body: &[u8], _role: ClientRole<NoRoles>) -> Decoded<TestEvent> {
            match body.first().copied() {
                None => Decoded::DecodeError("empty body"),
                Some(0xFC) => Decoded::Filter,
                Some(0xFD) => Decoded::PermissionDenied("denied"),
                Some(0xFE) => Decoded::DecodeError("bad"),
                Some(0xFF) => Decoded::Permitted(TestEvent::Query),
                Some(b) => Decoded::Permitted(TestEvent::Cmd(b)),
            }
        }
    }

    /// A frame under any tag — `[u32 LE length][tag][rest]` — as no
    /// well-behaved client sends after the handshake unless `tag` is the
    /// request's and `rest` starts with an ID.
    fn tagged_frame(tag: u8, rest: &[u8]) -> Vec<u8> {
        let len = (TAG_LEN + rest.len()) as u32;
        let mut f = Vec::with_capacity(4 + len as usize);
        f.extend_from_slice(&len.to_le_bytes());
        f.push(tag);
        f.extend_from_slice(rest);
        f
    }

    /// Request `id`'s frame as a client sends it —
    /// `[u32 LE length][TAG_REQUEST][id][body]`.
    fn request_frame(id: u64, body: &[u8]) -> Vec<u8> {
        let mut buf = vec![0u8; melin_wire_protocol::framing::REQUEST_HEADER_LEN + body.len()];
        let n = melin_wire_protocol::framing::frame_request(&mut buf, RequestId(id), |out| {
            out[..body.len()].copy_from_slice(body);
            Ok::<_, melin_wire_protocol::framing::RequestFrameError>(body.len())
        })
        .expect("a test request fits");
        buf.truncate(n);
        buf
    }

    /// The ID [`frame`] gives a request carrying `byte`: distinct per byte,
    /// and never the byte itself, so a test cannot pass by confusing the
    /// two.
    fn id_of(byte: u8) -> RequestId {
        RequestId(0x1_0000 + u64::from(byte))
    }

    /// The frame most tests use: a request whose ID is [`id_of`] `byte`,
    /// and `byte` as the one body byte [`ByteDecoder`] keys on.
    fn frame(byte: u8) -> Vec<u8> {
        request_frame(id_of(byte).0, &[byte])
    }

    /// Length prefix announcing an oversize frame. No payload bytes follow —
    /// `process_frames` decides on the prefix alone, before waiting for the
    /// body.
    fn oversize_prefix() -> [u8; 4] {
        ((MAX_FRAME_SIZE as u32) + 1).to_le_bytes()
    }

    /// Test fixture bundle. Grouped into a struct rather than returned as a
    /// 4-tuple to keep clippy happy (`type_complexity`) and to give each
    /// field a name at call sites.
    struct Fixture {
        conn: ConnectionEntry<UnixStream>,
        producer: ring::Producer<InputSlot<TestEvent>>,
        consumer: ring::Consumer<InputSlot<TestEvent>>,
        /// Client-side end of the socket pair — read from this to inspect
        /// any `ServerBusy` bytes the function under test writes.
        peer: UnixStream,
    }

    /// Build a fresh fixture: a `ConnectionEntry` backed by a `UnixStream`
    /// pair plus a single-consumer disruptor of the requested capacity.
    fn make_fixture(ring_capacity: usize) -> Fixture {
        let (server_side, peer) = UnixStream::pair().expect("UnixStream::pair");
        // Short read timeout on the peer so assertions of "no ServerBusy
        // written" return promptly instead of hanging the test.
        peer.set_read_timeout(Some(Duration::from_millis(50)))
            .expect("set_read_timeout");

        let entry = ConnectionEntry::<UnixStream> {
            connection_id: 7,
            addr: "127.0.0.1:1".parse().expect("addr parses"),
            role: ClientRole::Operator,
            key_hash: 0xC0FFEE_u64,
            fd: server_side.as_raw_fd(),
            _reader: server_side,
            ingress: ClientIngress::new(Vec::with_capacity(64)),
            recv: RecvState::Idle,
            last_activity: Instant::now(),
        };

        let (producer, mut consumers) =
            DisruptorBuilder::<InputSlot<TestEvent>>::new(ring_capacity)
                .add_consumer()
                .build(melin_pipeline::wait::WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().expect("consumer present");

        Fixture {
            conn: entry,
            producer,
            consumer,
            peer,
        }
    }

    /// Invoke `process_frames::<TestApp, UnixStream>` on a standalone gate,
    /// threading the feature-gated histogram args when the relevant
    /// features are on so the call compiles in every `cargo test`
    /// configuration.
    ///
    /// Returns the call's outcome plus every early reply the call queued:
    /// a busy goes to the response stage through the early-reply queue,
    /// never as reader-side socket bytes, so busy assertions read the
    /// queue, not the peer.
    fn run_process_frames(
        conn: &mut ConnectionEntry<UnixStream>,
        producer: &mut ring::Producer<InputSlot<TestEvent>>,
    ) -> (FrameAction, Vec<EarlyReply<TestReport>>) {
        let (mut early, mut queue) = early_channel();
        let action = run_process_frames_with(conn, producer, &gate(None, false), &mut early);
        (action, released(&mut queue))
    }

    /// Every early reply visible on `queue`, oldest first: what the
    /// response stage would send, with no flush on the reader's side
    /// after the call that queued them.
    fn released(
        queue: &mut crate::halt::EarlyReplyQueue<TestReport>,
    ) -> Vec<EarlyReply<TestReport>> {
        let boundary = crate::batch_boundary::BatchBoundary::new();
        let at = boundary.at_boundary().expect("between events");
        let mut out = Vec::new();
        queue.register(|| {}).drain(&at, |reply| out.push(reply));
        out
    }

    /// The early reply the fixture connection (id 7) gets for request
    /// `id`.
    fn early_reply(id: RequestId, kind: EarlyKind<TestReport>) -> EarlyReply<TestReport> {
        EarlyReply {
            connection_id: 7,
            request_id: id,
            kind,
        }
    }

    /// A halt gate: `replicas` connected (`None` for standalone), fenced
    /// or not.
    fn gate(replicas: Option<u32>, fenced: bool) -> HaltGate {
        gate_counting_into(replicas, fenced, Arc::new(AtomicU64::new(0)))
    }

    /// [`gate`], counting what it refuses into `refused`.
    fn gate_counting_into(
        replicas: Option<u32>,
        fenced: bool,
        refused: Arc<AtomicU64>,
    ) -> HaltGate {
        let fence = Arc::new(melin_transport_core::fence::FenceState::new(0));
        if fenced {
            fence.fence();
        }
        HaltGate::new(
            replicas.map(crate::halt::test_support::presence),
            fence,
            refused,
        )
    }

    fn refused_count(refused: &AtomicU64) -> u64 {
        refused.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The ack-policy mailbox's producer side, for a reader no test sends
    /// a request to.
    fn policy_claim() -> crate::policy_control::BootedClaim {
        crate::policy_control::test_support::booted(
            &crate::policy_control::test_support::replicated(crate::ack_policy::AckPolicy::Disk),
        )
    }

    /// A reader parked with nothing to do (no connection, no tick) is
    /// woken by an `ACK-POLICY` request and journals it. Halt-plan step 0
    /// through the reader: on a halted primary the operator's `disk` is
    /// journaled forced, and the halt is lifted before the request returns
    /// published, so the reader takes the next write. Once stopped, the
    /// reader is detached.
    #[test]
    fn a_parked_reader_journals_an_ack_policy_request() {
        use crate::ack_policy::AckPolicy;
        use crate::halt::Verdict;
        use crate::halt::test_support::{gate_over, presence};
        use crate::policy_control::RequestOutcome;

        let replicas = presence(0);
        let halt = || gate_over(Some(Arc::clone(&replicas)));
        assert_eq!(
            halt().verdict(),
            Verdict::Refuse(RejectReason::ReplicaDisconnected)
        );
        let (handle, mut consumer, control) = spawn_parked_reader(halt(), None);
        assert_eq!(control.request(AckPolicy::Disk), RequestOutcome::Published);
        let (_, slot) = consumer.try_consume().expect("the entry is on the ring");
        assert_eq!(
            slot.event,
            JournalEvent::AckPolicyChange(melin_journal::AckPolicyChange {
                policy: AckPolicy::Disk.to_byte(),
                unwitnessed: true,
            })
        );
        assert_eq!(halt().verdict(), Verdict::Take);
        handle.shutdown();
        handle.join();
        assert_eq!(
            control.request(AckPolicy::Ram),
            RequestOutcome::Staged,
            "a stopped reader is detached"
        );
    }

    /// A policy staged while the primary started is journaled as the
    /// reader attaches, before it runs: ahead of a runtime request it
    /// then publishes, and never forced.
    #[test]
    fn a_reader_journals_a_policy_staged_during_bring_up_first() {
        use crate::ack_policy::AckPolicy;
        use crate::halt::test_support::{gate_over, presence};
        use crate::policy_control::RequestOutcome;

        let (handle, mut consumer, control) =
            spawn_parked_reader(gate_over(Some(presence(0))), Some(AckPolicy::Disk));
        assert_eq!(control.request(AckPolicy::Disk), RequestOutcome::Published);
        handle.shutdown();
        handle.join();
        let mut changes = Vec::new();
        while let Some((_, slot)) = consumer.try_consume() {
            if let JournalEvent::AckPolicyChange(change) = slot.event {
                changes.push(change.unwitnessed);
            }
        }
        assert_eq!(
            changes,
            [false, true],
            "staged first, then the forced runtime one"
        );
    }

    /// A reader over `halt` with no connection and no tick, parked (in all
    /// likelihood: the callers must not depend on it either way), whose
    /// mailbox starts at `disk+ram` and had `staged` requested between its
    /// claim and the reader's start (a primary still starting); and the
    /// consumer of its input ring.
    fn spawn_parked_reader(
        halt: HaltGate,
        staged: Option<crate::ack_policy::AckPolicy>,
    ) -> (
        UringReaderHandle<UnixStream>,
        ring::Consumer<InputSlot<TestEvent>>,
        Arc<crate::policy_control::AckPolicyControl>,
    ) {
        use crate::ack_policy::AckPolicy;
        use crate::policy_control::RequestOutcome;
        use crate::policy_control::test_support::{control_with_publish_wait, first_change};

        let (mut producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(melin_pipeline::wait::WaitStrategy::SpinThenYield);
        let consumer = consumers.pop().expect("consumer");
        // Dropped: the reader's sends to it are best-effort.
        let (control_tx, _) = mpsc::channel();
        // Long enough that a reader that is never woken fails the request.
        let control = control_with_publish_wait(AckPolicy::DiskAndRam, Duration::from_secs(30));
        // Booted on a lineage already under the claim's policy: no entry.
        let claim =
            control
                .claim()
                .journal_boot(first_change(AckPolicy::DiskAndRam), &mut producer, &halt);
        if let Some(policy) = staged {
            assert_eq!(control.request(policy), RequestOutcome::Staged);
        }
        let handle = spawn_reader::<TestApp, UnixStream>(
            producer,
            Arc::new(ByteDecoder),
            halt,
            early_channel().0,
            claim,
            control_tx,
            0,
            None,
            None, // no tick: nothing but the wakeup brings the loop round
            RingSizing::for_max_connections(4).expect("a supported cap"),
            Arc::new(AtomicBool::new(false)),
        )
        .expect("the reader starts");
        std::thread::sleep(Duration::from_millis(50));
        (handle, consumer, control)
    }

    /// An early-reply queue, at a node's capacity.
    fn early_channel() -> (
        EarlyReplySender<TestReport>,
        crate::halt::EarlyReplyQueue<TestReport>,
    ) {
        early_channel_of(crate::halt::EARLY_REPLY_QUEUE_CAPACITY)
    }

    /// An early-reply queue that fills at `capacity` (a power of two).
    fn early_channel_of(
        capacity: usize,
    ) -> (
        EarlyReplySender<TestReport>,
        crate::halt::EarlyReplyQueue<TestReport>,
    ) {
        crate::halt::early_reply_channel(capacity)
    }

    /// [`run_process_frames`] against a given halt gate and early-reply
    /// queue.
    fn run_process_frames_with(
        conn: &mut ConnectionEntry<UnixStream>,
        producer: &mut ring::Producer<InputSlot<TestEvent>>,
        halt: &HaltGate,
        early: &mut EarlyReplySender<TestReport>,
    ) -> FrameAction {
        #[cfg(feature = "latency-trace")]
        let mut publish_rec = melin_transport_core::trace::register_stage("test: publish");
        #[cfg(feature = "tick-to-trade")]
        let mut ingest_rec = melin_transport_core::trace::register_stage("test: ingest");

        #[allow(clippy::let_unit_value)] // ZST when latency-trace is off
        let recv_ts = melin_transport_core::trace::mono_trace_ns();

        process_frames::<TestApp, UnixStream>(
            conn,
            producer,
            &ByteDecoder,
            halt,
            early,
            0xDEAD_BEEF,
            recv_ts,
            #[cfg(feature = "latency-trace")]
            &mut publish_rec,
            #[cfg(feature = "tick-to-trade")]
            &mut ingest_rec,
        )
    }

    /// Drain `consumer` into a Vec of `(seq, slot)` until it yields `None`.
    /// Used to assert exact event sequences after `process_frames` returns.
    fn drain(
        consumer: &mut ring::Consumer<InputSlot<TestEvent>>,
    ) -> Vec<(u64, InputSlot<TestEvent>)> {
        let mut out = Vec::new();
        while let Some(pair) = consumer.try_consume() {
            out.push(pair);
        }
        out
    }

    /// Whether the peer can read anything within its configured timeout:
    /// the reader never writes the client socket (egress belongs to the
    /// response stage), so it must not.
    fn peer_got_bytes(peer: &mut UnixStream) -> bool {
        let mut buf = [0u8; 1];
        match peer.read(&mut buf) {
            Ok(n) => n > 0,
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => false,
            Err(e) => panic!("unexpected peer read error: {e}"),
        }
    }

    #[test]
    fn process_frames_publishes_all_frames_after_single_commit() {
        // Capacity > number of frames — every frame must succeed and become
        // visible to the consumer after the trailing `batch.commit()`.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(16);
        for byte in [0x01, 0x02, 0x03, 0x04, 0x05] {
            conn.ingress.extend_from_slice(&frame(byte));
        }

        let (action, early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(
            action,
            FrameAction::Continue,
            "no oversize frame ⇒ no disconnect"
        );

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 5, "all 5 frames must be visible");
        for (i, (seq, slot)) in events.iter().enumerate() {
            assert_eq!(*seq, i as u64, "seq monotonic from 0");
            assert_eq!(slot.connection_id, 7);
            assert_eq!(slot.key_hash, 0xC0FFEE_u64);
            let byte = (i + 1) as u8;
            // The client's ID travels in the slot.
            assert_eq!(slot.request_id, id_of(byte));
            assert_eq!(slot.event, JournalEvent::App(TestEvent::Cmd(byte)));
            // Non-query event ⇒ inherits the caller-supplied wall-clock.
            assert_eq!(slot.timestamp_ns, 0xDEAD_BEEF);
        }
        // Parse buffer fully consumed.
        assert!(conn.ingress.bytes().is_empty());
        // Every request published: nothing answered early, and the
        // reader never writes the client socket (egress belongs to the
        // response stage).
        assert!(early.is_empty(), "{early:?}");
        assert!(
            !peer_got_bytes(&mut peer),
            "reader must never write the client socket"
        );
    }

    /// A ring slot is reused once its consumer has passed it, and a request
    /// published into it is built whole: nothing of the previous occupant
    /// survives, a request ID and a journal sequence included (a stale
    /// sequence would be adopted by the journal stage as a replica's).
    #[test]
    fn process_frames_rebuilds_every_field_of_a_reused_slot() {
        const CAPACITY: usize = 4;
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(CAPACITY);
        let stale = InputSlot {
            connection_id: 99,
            request_id: RequestId(0xBAD),
            key_hash: 1,
            sequence: 77,
            timestamp_ns: 5,
            event: JournalEvent::App(TestEvent::Cmd(0xEE)),
            ..InputSlot::default()
        };
        for _ in 0..CAPACITY {
            producer.publish(stale);
        }
        assert_eq!(drain(&mut consumer).len(), CAPACITY, "every slot dirtied");

        for byte in 1..=CAPACITY as u8 {
            conn.ingress.extend_from_slice(&frame(byte));
        }
        let (action, _early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(action, FrameAction::Continue);

        let events = drain(&mut consumer);
        assert_eq!(events.len(), CAPACITY, "every dirtied slot reused");
        for (i, (_, slot)) in events.iter().enumerate() {
            assert_eq!(slot.connection_id, 7);
            assert_eq!(slot.request_id, id_of(i as u8 + 1), "the frame's own ID");
            assert_eq!(slot.key_hash, 0xC0FFEE_u64);
            assert_eq!(slot.sequence, 0, "the journal stage allocates it");
            assert_eq!(slot.timestamp_ns, 0xDEAD_BEEF);
            assert_eq!(slot.event, JournalEvent::App(TestEvent::Cmd(i as u8 + 1)));
        }
    }

    /// A halted reader publishes no write: each is refused under its ID,
    /// while queries still go through. Nothing refused can reach the journal, so replay cannot
    /// apply a write the client was told failed.
    #[test]
    fn a_halted_reader_refuses_writes_before_publishing_and_still_publishes_queries() {
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        // Write, query, write, query.
        for byte in [0x01, 0xFF, 0x02, 0xFF] {
            conn.ingress.extend_from_slice(&frame(byte));
        }
        let (mut early, mut queue) = early_channel();
        let counted = Arc::new(AtomicU64::new(0));

        let action = run_process_frames_with(
            &mut conn,
            &mut producer,
            &gate_counting_into(Some(0), false, Arc::clone(&counted)),
            &mut early,
        );
        assert_eq!(action, FrameAction::Continue);
        assert!(conn.ingress.bytes().is_empty(), "every frame consumed");
        assert_eq!(
            refused_count(&counted),
            2,
            "the two writes are counted, the queries are not"
        );

        let published: Vec<_> = drain(&mut consumer)
            .into_iter()
            .map(|(seq, slot)| (seq, slot.event))
            .collect();
        assert_eq!(
            published,
            [
                (0, JournalEvent::App(TestEvent::Query)),
                (1, JournalEvent::App(TestEvent::Query)),
            ],
            "only the queries enter the pipeline"
        );

        let expect = |byte| {
            early_reply(
                id_of(byte),
                EarlyKind::Rejected((TestEvent::Cmd(byte), RejectReason::ReplicaDisconnected)),
            )
        };
        assert_eq!(
            released(&mut queue),
            [expect(0x01), expect(0x02)],
            "each write is answered under its ID"
        );
    }

    /// A fenced node closes the connection, whatever its replica count:
    /// nothing is published, nothing is refused, nothing is counted.
    #[test]
    fn a_fenced_reader_closes_the_connection() {
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        for byte in [0x01, 0xFF] {
            conn.ingress.extend_from_slice(&frame(byte));
        }
        let (mut early, mut queue) = early_channel();
        let counted = Arc::new(AtomicU64::new(0));

        let action = run_process_frames_with(
            &mut conn,
            &mut producer,
            &gate_counting_into(Some(1), true, Arc::clone(&counted)),
            &mut early,
        );

        assert_eq!(action, FrameAction::Disconnect, "the connection is dropped");
        assert!(
            drain(&mut consumer).is_empty(),
            "not even the query is published"
        );
        assert!(released(&mut queue).is_empty(), "no early reply is queued");
        assert_eq!(refused_count(&counted), 0);
    }

    /// An answer with nowhere to go parks its connection: the frames
    /// before it are committed, nothing from the frame that met the full
    /// queue on is published or answered, and that frame and the one behind
    /// it stay buffered. Once the queue has room, a call with no new bytes
    /// answers or publishes them as it would have. Each kind of early reply
    /// meets it the same way: a halt refusal (counted once answered), a
    /// filtered and a refused request, and a busy on a full ring, whose
    /// shed tail survives the park, so the frame behind it is shed too
    /// though the ring has room by then.
    #[test]
    fn a_full_early_reply_queue_parks_before_the_frame() {
        let refused_as =
            |byte: u8, kind, reason| early_reply(id_of(byte), EarlyKind::Refused { kind, reason });
        let rejected = |byte: u8| {
            early_reply(
                id_of(byte),
                EarlyKind::Rejected((TestEvent::Cmd(byte), RejectReason::ReplicaDisconnected)),
            )
        };
        let cmd_3 = [JournalEvent::App(TestEvent::Cmd(0x03))];
        // (slots already taken in a two-slot ring, halted, the frame that
        // meets the full queue, what the call after the park answers, and
        // publishes)
        type Case<'a> = (
            usize,
            bool,
            u8,
            Vec<EarlyReply<TestReport>>,
            &'a [JournalEvent<TestEvent>],
        );
        let cases: [Case; 5] = [
            (0, true, 0x02, vec![rejected(0x02), rejected(0x03)], &[]),
            (
                0,
                false,
                0xFC,
                vec![early_reply(id_of(0xFC), EarlyKind::Filtered)],
                &cmd_3,
            ),
            (
                0,
                false,
                0xFD,
                vec![refused_as(0xFD, RefusalKind::PermissionDenied, "denied")],
                &cmd_3,
            ),
            (
                0,
                false,
                0xFE,
                vec![refused_as(0xFE, RefusalKind::Malformed, "bad")],
                &cmd_3,
            ),
            // The ring has room for the first frame only: the second is
            // shed, and the third with it.
            (
                1,
                false,
                0x02,
                vec![
                    early_reply(id_of(0x02), EarlyKind::Busy),
                    early_reply(id_of(0x03), EarlyKind::Busy),
                ],
                &[],
            ),
        ];
        for (prefill, halted, byte, answers, publishes) in cases {
            let Fixture {
                mut conn,
                mut producer,
                mut consumer,
                ..
            } = make_fixture(2);
            for _ in 0..prefill {
                producer.publish(InputSlot::default());
            }
            // A query first, which is published even while halted.
            conn.ingress.extend_from_slice(&frame(0xFF));
            conn.ingress.extend_from_slice(&frame(byte));
            conn.ingress.extend_from_slice(&frame(0x03));
            let (mut early, mut queue) = early_channel_of(4);
            let filler = early_reply(RequestId(0), EarlyKind::Busy);
            let mut filled = 0;
            while early.try_send(filler).is_ok() {
                filled += 1;
            }

            let counted = Arc::new(AtomicU64::new(0));
            let halt = gate_counting_into(halted.then_some(0), false, Arc::clone(&counted));
            let action = run_process_frames_with(&mut conn, &mut producer, &halt, &mut early);

            assert_eq!(action, FrameAction::Park, "byte {byte:#04x}");
            assert_eq!(
                refused_count(&counted),
                0,
                "byte {byte:#04x}: a write parked on is not refused yet"
            );
            let published: Vec<_> = drain(&mut consumer)
                .into_iter()
                .skip(prefill)
                .map(|(_, slot)| slot.event)
                .collect();
            assert_eq!(
                published,
                [JournalEvent::App(TestEvent::Query)],
                "byte {byte:#04x}: the frame before is committed, nothing after"
            );
            assert_eq!(
                conn.ingress.bytes(),
                [frame(byte), frame(0x03)].concat(),
                "byte {byte:#04x}: the frame parked on stays, with the tail"
            );
            let queued = released(&mut queue);
            assert_eq!(
                queued.len(),
                filled,
                "byte {byte:#04x}: nothing more was queued"
            );
            assert!(queued.iter().all(|reply| *reply == filler));

            // The queue has room again (and so does the ring): the retry
            // takes no new bytes.
            let action = run_process_frames_with(&mut conn, &mut producer, &halt, &mut early);
            assert_eq!(action, FrameAction::Continue, "byte {byte:#04x}");
            assert_eq!(released(&mut queue), answers, "byte {byte:#04x}");
            let published: Vec<_> = drain(&mut consumer)
                .into_iter()
                .map(|(_, slot)| slot.event)
                .collect();
            assert_eq!(published, publishes, "byte {byte:#04x}");
            assert!(conn.ingress.bytes().is_empty(), "byte {byte:#04x}");
            assert_eq!(conn.ingress.shed_remaining(), 0, "byte {byte:#04x}");
            assert_eq!(
                refused_count(&counted),
                if halted { 2 } else { 0 },
                "byte {byte:#04x}: each refused write counted once"
            );
        }
    }

    /// Whatever the call returns, every early reply it staged is visible
    /// to the response stage by then: the queue is flushed on every way
    /// out, and nothing here flushes it after the call (`released` reads
    /// it as the stage does). One case per way out: every frame
    /// processed, a shed tail answered whole, a close on a frame that is
    /// not a request (also in the middle of a shed tail) and on an
    /// oversized prefix, and a park (also in the middle of a shed tail),
    /// each after an answer was staged. A fenced node stages nothing.
    #[test]
    fn every_way_out_leaves_the_early_replies_it_staged_visible() {
        let refused = |byte: u8| {
            early_reply(
                id_of(byte),
                EarlyKind::Refused {
                    kind: if byte == 0xFD {
                        RefusalKind::PermissionDenied
                    } else {
                        RefusalKind::Malformed
                    },
                    reason: if byte == 0xFD { "denied" } else { "bad" },
                },
            )
        };
        let busy = |byte: u8| early_reply(id_of(byte), EarlyKind::Busy);
        let not_a_request = tagged_frame(0x00, &[0x03; 9]);
        // (case, ring capacity, early-reply queue capacity, frames, the
        // outcome, what it stages)
        let cases = [
            (
                "every frame processed",
                16,
                16,
                vec![frame(0xFC), frame(0x01)],
                FrameAction::Continue,
                vec![early_reply(id_of(0xFC), EarlyKind::Filtered)],
            ),
            (
                "a full ring",
                2,
                16,
                vec![
                    frame(0xFD),
                    frame(0x01),
                    frame(0x02),
                    frame(0x03),
                    frame(0x04),
                ],
                FrameAction::Continue,
                vec![refused(0xFD), busy(0x03), busy(0x04)],
            ),
            (
                "a frame that is not a request",
                16,
                16,
                vec![frame(0xFE), not_a_request.clone()],
                FrameAction::Disconnect,
                vec![refused(0xFE)],
            ),
            (
                "a frame that is not a request, in a shed tail",
                2,
                16,
                vec![
                    frame(0x01),
                    frame(0x02),
                    frame(0x03),
                    not_a_request,
                    frame(0x04),
                ],
                FrameAction::Disconnect,
                vec![busy(0x03)],
            ),
            (
                "an oversized prefix",
                16,
                16,
                vec![frame(0xFC), oversize_prefix().to_vec()],
                FrameAction::Disconnect,
                vec![early_reply(id_of(0xFC), EarlyKind::Filtered)],
            ),
            (
                "a park",
                16,
                2,
                vec![frame(0xFC), frame(0xFE), frame(0xFD)],
                FrameAction::Park,
                vec![early_reply(id_of(0xFC), EarlyKind::Filtered), refused(0xFE)],
            ),
            (
                "a park in a shed tail",
                2,
                2,
                vec![
                    frame(0x01),
                    frame(0x02),
                    frame(0x03),
                    frame(0x04),
                    frame(0x05),
                ],
                FrameAction::Park,
                vec![busy(0x03), busy(0x04)],
            ),
        ];
        for (case, ring_capacity, queue_capacity, frames, outcome, expected) in cases {
            let Fixture {
                mut conn,
                mut producer,
                ..
            } = make_fixture(ring_capacity);
            for frame in frames {
                conn.ingress.extend_from_slice(&frame);
            }
            let (mut early, mut queue) = early_channel_of(queue_capacity);
            let action =
                run_process_frames_with(&mut conn, &mut producer, &gate(None, false), &mut early);
            assert_eq!(action, outcome, "{case}");
            assert_eq!(released(&mut queue), expected, "{case}");
        }
    }

    /// The caller stamps `recv_ts` once per recv (at the kernel-return
    /// site) and every frame parsed from that buffer must carry that exact
    /// value. Guards against a regression to per-frame `recv_ts` capture,
    /// which excluded decode from the reader-ingest / server-e2e windows
    /// and drifted forward for later frames in a multi-frame recv. Gated on
    /// `latency-trace` because `recv_ts` is a real `u64` only then (`()`
    /// otherwise, leaving nothing to assert).
    #[cfg(feature = "latency-trace")]
    #[test]
    fn process_frames_stamps_every_slot_with_caller_recv_ts() {
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        for byte in [0x01, 0x02, 0x03, 0x04] {
            conn.ingress.extend_from_slice(&frame(byte));
        }

        // A recognizable sentinel the caller would have captured at the
        // recv site; distinct from the wall-clock stamp (0xDEAD_BEEF).
        const RECV_TS: u64 = 0x5EED_5EED;

        let mut publish_rec = melin_transport_core::trace::register_stage("test: publish recv_ts");
        #[cfg(feature = "tick-to-trade")]
        let mut ingest_rec = melin_transport_core::trace::register_stage("test: ingest recv_ts");

        let (mut early, _queue) = early_channel();
        let action = process_frames::<TestApp, UnixStream>(
            &mut conn,
            &mut producer,
            &ByteDecoder,
            &gate(None, false),
            &mut early,
            0xDEAD_BEEF,
            RECV_TS,
            &mut publish_rec,
            #[cfg(feature = "tick-to-trade")]
            &mut ingest_rec,
        );
        assert_eq!(action, FrameAction::Continue);

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 4, "all frames published");
        for (_seq, slot) in &events {
            assert_eq!(
                slot.recv_ts, RECV_TS,
                "every frame from one recv shares the single caller-supplied recv_ts"
            );
        }
    }

    #[test]
    fn process_frames_rotates_batch_at_commit_every_cap() {
        // Push more events than `COMMIT_EVERY` (= 16) into a recv-cycle.
        // The cap must trigger at least one mid-loop commit so the
        // consumer sees the first capacity-many events before the
        // remainder lands. Validates the visibility-delay cap from the
        // perf branch — without it, all 32 events would commit together
        // and the first frame would wait for the 32nd to decode.
        //
        // Capacity 64 leaves room for the entire input (no Full); ring
        // backpressure is exercised separately in
        // `a_full_ring_queues_busy_for_the_frame_that_met_it`.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(64);
        const EVENT_COUNT: usize = 32;
        for i in 0..EVENT_COUNT {
            // Use bytes 1..=32 (each ≤ 0xFB so ByteDecoder yields
            // `Permitted` with `TestEvent::Cmd(byte)`).
            conn.ingress.extend_from_slice(&frame((i + 1) as u8));
        }

        let (action, _early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(
            action,
            FrameAction::Continue,
            "no oversize / no Full ⇒ no disconnect"
        );

        let events = drain(&mut consumer);
        assert_eq!(events.len(), EVENT_COUNT, "every event visible");
        for (i, (seq, slot)) in events.iter().enumerate() {
            assert_eq!(*seq, i as u64, "seq contiguous across batch rotations");
            let byte = (i + 1) as u8;
            assert_eq!(slot.event, JournalEvent::App(TestEvent::Cmd(byte)));
        }
        assert!(conn.ingress.bytes().is_empty());
    }

    #[test]
    fn process_frames_query_event_skips_wall_clock_stamp() {
        // `AppEvent::is_query` events bypass the journal stamp — verify
        // the timestamp is zeroed even when a non-zero batch_wall_ns was
        // supplied.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(8);
        conn.ingress.extend_from_slice(&frame(0xFF)); // body byte → Query

        let (action, _early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(action, FrameAction::Continue);

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 1);
        let (_, slot) = &events[0];
        assert_eq!(slot.event, JournalEvent::App(TestEvent::Query));
        assert_eq!(
            slot.timestamp_ns, 0,
            "query events must skip the wall-clock stamp"
        );
    }

    #[test]
    fn a_full_ring_sheds_the_frame_that_met_it_and_every_complete_frame_behind() {
        // Ring capacity 4 + 7 frames, the last cut short ⇒ first 4 commit,
        // the 5th triggers Full, the 6th is shed with it, the partial 7th
        // waits. Validates:
        //   * `Err(Full)` does not roll back the prior 4.
        //   * The frame that triggered Full, and the complete frame behind
        //     it, are answered `Busy`, each with its ID, without being
        //     decoded or published, and the connection is kept.
        //   * The busy goes to the response stage through the early-reply
        //     queue, and nothing is written to the socket from here.
        //   * A trailing partial frame is not part of the tail: it stays,
        //     and once whole it is judged on its own.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(4);
        for byte in [0x01, 0x02, 0x03, 0x04, 0x05, 0x06] {
            conn.ingress.extend_from_slice(&frame(byte));
        }
        let partial = frame(0x07);
        conn.ingress.extend_from_slice(&partial[..5]);

        let (action, early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(
            action,
            FrameAction::Continue,
            "Full does not drop the connection"
        );

        let events = drain(&mut consumer);
        assert_eq!(
            events.len(),
            4,
            "only the first capacity-many frames are visible"
        );
        for (i, (_, slot)) in events.iter().enumerate() {
            let byte = (i + 1) as u8;
            assert_eq!(slot.event, JournalEvent::App(TestEvent::Cmd(byte)));
        }

        // The busy goes to the response stage — never directly onto the
        // socket, where it could tear a partially-flushed response frame
        // (audit review F3).
        assert_eq!(
            early,
            [
                early_reply(id_of(0x05), EarlyKind::Busy),
                early_reply(id_of(0x06), EarlyKind::Busy),
            ],
            "a busy for the frame that met the full ring, and for the one behind"
        );
        assert!(
            !peer_got_bytes(&mut peer),
            "reader must never write the client socket"
        );

        // Only the partial frame is left, outside any tail.
        assert_eq!(conn.ingress.bytes(), &partial[..5]);
        assert_eq!(conn.ingress.shed_remaining(), 0);

        // The ring has room again (drained above) and the 7th completes:
        // published, as a request on a later receive is.
        conn.ingress.extend_from_slice(&partial[5..]);
        let (action, early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(action, FrameAction::Continue);
        assert!(early.is_empty(), "{early:?}");
        let events = drain(&mut consumer);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x07)));
    }

    /// Regression for the 2026-08 io_uring audit, finding 2 and review
    /// finding F3 (docs/internal/io-uring-audit-2026-08.md): the
    /// pipeline-full path must not touch the client socket at all. The
    /// original defect was a blocking `write(2)` of ServerBusy that
    /// parked the reader thread whenever the offender's socket buffer
    /// was also full — stalling ingress for *every* connection exactly
    /// on the overload path (against that code this test never
    /// returns). The busy notice now routes to the response stage
    /// through the early-reply queue, which also closes the
    /// frame-tearing race of any reader-side send. The socket-buffer
    /// fill stays: it proves the path is insensitive to socket state.
    #[test]
    fn pipeline_full_never_touches_the_client_socket() {
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(2);

        // Fill the server→peer direction until the kernel refuses more —
        // the state a non-reading client leaves the connection in.
        let junk = [0u8; 4096];
        loop {
            let n = unsafe {
                libc::send(
                    conn.fd,
                    junk.as_ptr().cast(),
                    junk.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            if n < 0 {
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EAGAIN),
                    "unexpected errno while filling socket"
                );
                break;
            }
        }

        // Capacity 2 + 3 frames ⇒ the third triggers Full ⇒ ServerBusy.
        conn.ingress.extend_from_slice(&frame(0x01));
        conn.ingress.extend_from_slice(&frame(0x02));
        conn.ingress.extend_from_slice(&frame(0x03));

        let start = std::time::Instant::now();
        let (action, early) = run_process_frames(&mut conn, &mut producer);
        // Generous bound to stay unflaky under CI load — the fixed path
        // returns in microseconds, the broken one only when the peer
        // reads (here: never).
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "pipeline-full path blocked on a full socket"
        );
        assert_eq!(
            action,
            FrameAction::Continue,
            "Full does not drop the connection"
        );
        assert_eq!(
            drain(&mut consumer).len(),
            2,
            "the frames that fit are still published"
        );
        assert_eq!(
            early,
            [early_reply(id_of(0x03), EarlyKind::Busy)],
            "busy notice routed to response stage"
        );
        // Nothing beyond the pre-fill junk arrives: drain it, then the
        // read must time out rather than yield a reader-written frame.
        let mut sink = [0u8; 4096];
        loop {
            match peer.read(&mut sink) {
                Ok(0) => panic!("unexpected EOF"),
                Ok(_) => continue,
                Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                    break;
                }
                Err(e) => panic!("unexpected peer read error: {e}"),
            }
        }
    }

    #[test]
    fn process_frames_oversize_commits_prior_frames_then_signals_disconnect() {
        // Two valid frames followed by an oversize length prefix must:
        //   * publish the two valid frames (commit-before-break) so the
        //     pipeline observes them even though we're about to tear the
        //     connection down,
        //   * return `true` so the caller drops the connection,
        //   * answer nothing early (a busy is for a full ring).
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(16);
        conn.ingress.extend_from_slice(&frame(0x01));
        conn.ingress.extend_from_slice(&frame(0x02));
        conn.ingress.extend_from_slice(&oversize_prefix());

        let (action, early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(
            action,
            FrameAction::Disconnect,
            "oversize frame must request disconnect"
        );

        let events = drain(&mut consumer);
        assert_eq!(
            events.len(),
            2,
            "prior frames are committed before the break"
        );
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x01)));
        assert_eq!(events[1].1.event, JournalEvent::App(TestEvent::Cmd(0x02)));

        assert!(early.is_empty(), "{early:?}");
        assert!(
            !peer_got_bytes(&mut peer),
            "reader must never write the client socket"
        );
    }

    /// Each decoder outcome that publishes nothing yields its early reply,
    /// with the frame's ID, in order among the other early replies; all
    /// bytes are consumed.
    #[test]
    fn each_decoder_outcome_yields_its_early_reply_with_the_frames_id() {
        // Mixed batch: Permitted, Filter, PermissionDenied, DecodeError,
        // Permitted, and a request with an empty body (a decode error).
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        for byte in [0x01, 0xFC, 0xFD, 0xFE, 0x02] {
            conn.ingress.extend_from_slice(&frame(byte));
        }
        conn.ingress.extend_from_slice(&request_frame(99, b""));

        let (action, early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(
            action,
            FrameAction::Continue,
            "a refused request keeps the connection"
        );

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x01)));
        assert_eq!(events[1].1.event, JournalEvent::App(TestEvent::Cmd(0x02)));
        assert_eq!(
            early,
            [
                early_reply(id_of(0xFC), EarlyKind::Filtered),
                early_reply(
                    id_of(0xFD),
                    EarlyKind::Refused {
                        kind: RefusalKind::PermissionDenied,
                        reason: "denied"
                    }
                ),
                early_reply(
                    id_of(0xFE),
                    EarlyKind::Refused {
                        kind: RefusalKind::Malformed,
                        reason: "bad"
                    }
                ),
                early_reply(
                    RequestId(99),
                    EarlyKind::Refused {
                        kind: RefusalKind::Malformed,
                        reason: "empty body"
                    }
                ),
            ]
        );
        assert!(
            conn.ingress.bytes().is_empty(),
            "all bytes advanced past compaction"
        );
    }

    /// The request tag and ID are the runtime's to read: a frame that is
    /// not a request — an empty one, one under any other tag, a request
    /// too short for its ID — closes the connection before any decoder
    /// sees it, with the frames before it committed and nothing after it
    /// read. One such frame per connection. The decoder gets the body
    /// alone, so a body byte equal to a protocol tag is just that —
    /// `frame(0x01)` carries the heartbeat's value as a command.
    #[test]
    fn a_frame_that_is_not_a_request_closes_the_connection() {
        let mut not_requests = vec![
            // An empty frame: no tag at all.
            0u32.to_le_bytes().to_vec(),
            // A request tag with fewer bytes than an ID behind it.
            tagged_frame(melin_wire_protocol::control_codec::TAG_REQUEST, &[0x03; 7]),
            tagged_frame(melin_wire_protocol::control_codec::TAG_REQUEST, &[]),
        ];
        // A zeroed tag, the protocol's own frames, the application tag of
        // a protocol-0 request, and an application tag as a pre-release
        // build framed one — each would decode to `Cmd(0x03)` if a
        // decoder saw it.
        for tag in [
            0x00,
            TAG_CHALLENGE_RESPONSE,
            TAG_RESPONSE_HEARTBEAT,
            TAG_APP,
            0x10,
        ] {
            not_requests.push(tagged_frame(tag, &[0x03; 9]));
        }
        for bad in not_requests {
            let Fixture {
                mut conn,
                mut producer,
                mut consumer,
                ..
            } = make_fixture(16);
            conn.ingress.extend_from_slice(&frame(0x01));
            conn.ingress.extend_from_slice(&frame(0xFC));
            conn.ingress.extend_from_slice(&bad);
            conn.ingress.extend_from_slice(&frame(0x02));

            let (action, early) = run_process_frames(&mut conn, &mut producer);
            assert_eq!(
                action,
                FrameAction::Disconnect,
                "{bad:?} must close the connection"
            );

            let events = drain(&mut consumer);
            assert_eq!(events.len(), 1, "{bad:?}");
            assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x01)));
            // The answer decided before it is still sent.
            assert_eq!(early, [early_reply(id_of(0xFC), EarlyKind::Filtered)]);
        }
    }

    #[test]
    fn process_frames_preserves_partial_trailing_frame() {
        // One complete frame followed by a truncated length prefix. The
        // complete frame must publish; the partial bytes must survive
        // compaction at the front of `parse_buf` for the next recv-cycle.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        conn.ingress.extend_from_slice(&frame(0x42));
        // Three of four length-prefix bytes — not even a length to judge
        // yet, so the loop breaks before consuming anything from the
        // partial.
        conn.ingress.extend_from_slice(&[0xDE, 0xAD, 0xBE]);

        let (action, _early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(action, FrameAction::Continue);

        let events = drain(&mut consumer);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x42)));
        assert_eq!(
            conn.ingress.bytes(),
            [0xDE, 0xAD, 0xBE],
            "partial length prefix preserved for next recv-cycle"
        );
    }

    /// The cap is inclusive: a frame of exactly `MAX_FRAME_SIZE` payload
    /// bytes is waited for while it arrives and then published, and a
    /// prefix one past it drops the connection on its own, with no payload
    /// behind it and nothing in front.
    #[test]
    fn process_frames_admits_a_frame_at_the_limit_and_drops_one_past_it() {
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            ..
        } = make_fixture(16);
        let mut body = vec![0x05; MAX_REQUEST_BODY];
        body[0] = 0x2A;
        let at_limit = request_frame(5, &body);
        assert_eq!(at_limit.len(), 4 + MAX_FRAME_SIZE);

        // The prefix and part of the payload: kept, not judged oversized.
        conn.ingress.extend_from_slice(&at_limit[..4 + 1]);
        let (action, _early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(
            action,
            FrameAction::Continue,
            "a frame at the limit is not oversized"
        );
        assert_eq!(conn.ingress.bytes(), &at_limit[..4 + 1]);
        assert!(drain(&mut consumer).is_empty());

        // The rest of it arrives: published, buffer drained.
        conn.ingress.extend_from_slice(&at_limit[4 + 1..]);
        let (action, _early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(action, FrameAction::Continue);
        let events = drain(&mut consumer);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1.event, JournalEvent::App(TestEvent::Cmd(0x2A)));
        assert_eq!(events[0].1.request_id, RequestId(5));
        assert!(conn.ingress.bytes().is_empty());

        // One byte more, announced by a prefix alone: dropped at once.
        conn.ingress.extend_from_slice(&oversize_prefix());
        let (action, _early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(
            action,
            FrameAction::Disconnect,
            "oversize prefix alone must request disconnect"
        );
        assert!(drain(&mut consumer).is_empty());
    }

    #[test]
    fn process_frames_empty_buffer_is_noop() {
        // No bytes in parse_buf ⇒ loop never enters; commit is the
        // documented zero-slot no-op; no busy event; no disconnect.
        let Fixture {
            mut conn,
            mut producer,
            mut consumer,
            mut peer,
        } = make_fixture(4);

        let (action, early) = run_process_frames(&mut conn, &mut producer);
        assert_eq!(action, FrameAction::Continue);
        assert_eq!(drain(&mut consumer).len(), 0);
        assert!(conn.ingress.bytes().is_empty());
        assert!(early.is_empty());
        assert!(!peer_got_bytes(&mut peer));
    }

    /// The shedding and parking rules over random streams of requests of
    /// every decoder outcome, split into random receives, through a ring
    /// and an early-reply queue of random small sizes that the "pipeline"
    /// drains at random. A receive arrives only once the previous call did
    /// not park; a park is followed by a retry with no new bytes once the
    /// queue was drained, as the reader's retry is.
    mod shedding {
        use super::*;
        use proptest::prelude::*;

        /// The body bytes [`ByteDecoder`] maps to each outcome: commands,
        /// a filtered one, a denied one, a malformed one, a query.
        fn body() -> impl Strategy<Value = u8> {
            prop_oneof![1u8..=8, Just(0xFC), Just(0xFD), Just(0xFE), Just(0xFF)]
        }

        prop_compose! {
            fn script()(
                bodies in prop::collection::vec(body(), 1..40),
                ring_capacity in prop_oneof![Just(2usize), Just(4), Just(8)],
                queue_capacity in prop_oneof![Just(2usize), Just(4)],
            )(
                cuts in prop::collection::vec(0..bodies.len() * 14 + 1, 0..12),
                drains in prop::collection::vec((0usize..4, any::<bool>()), 64),
                bodies in Just(bodies),
                ring_capacity in Just(ring_capacity),
                queue_capacity in Just(queue_capacity),
            ) -> (Vec<u8>, Vec<usize>, Vec<(usize, bool)>, usize, usize) {
                (bodies, cuts, drains, ring_capacity, queue_capacity)
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]
            #[test]
            fn every_request_is_published_or_answered_once_and_a_shed_tail_whole(
                (bodies, mut cuts, drains, ring_capacity, queue_capacity) in script()
            ) {
                // Request `i` carries ID `i`; every frame is 14 bytes.
                let frames: Vec<Vec<u8>> = bodies
                    .iter()
                    .enumerate()
                    .map(|(i, body)| request_frame(i as u64, &[*body]))
                    .collect();
                let stream = frames.concat();
                cuts.push(0);
                cuts.push(stream.len());
                cuts.sort_unstable();
                cuts.dedup();

                let Fixture { mut conn, mut producer, mut consumer, .. } =
                    make_fixture(ring_capacity);
                let (mut early, mut queue) = early_channel_of(queue_capacity);
                let halt = gate(None, false);
                let mut published: Vec<u64> = Vec::new();
                let mut answered: Vec<EarlyReply<TestReport>> = Vec::new();
                // Per receive (an episode: the call after it and the
                // retries of its parks), how many frames were complete.
                let mut episodes: Vec<usize> = Vec::new();
                let mut drains = drains.into_iter().cycle();
                let mut take = |consumer: &mut ring::Consumer<InputSlot<TestEvent>>,
                                queue: &mut crate::halt::EarlyReplyQueue<TestReport>,
                                published: &mut Vec<u64>,
                                answered: &mut Vec<EarlyReply<TestReport>>,
                                all: bool| {
                    let (events, replies) = drains.next().expect("cycled");
                    for _ in 0..if all { usize::MAX } else { events } {
                        let Some((_, slot)) = consumer.try_consume() else { break };
                        published.push(slot.request_id.0);
                    }
                    if all || replies {
                        answered.extend(released(queue));
                    }
                };

                let mut receives = cuts.windows(2).map(|w| &stream[w[0]..w[1]]);
                let mut action = FrameAction::Continue;
                loop {
                    if action == FrameAction::Park {
                        // The response stage drains the queue; the retry
                        // brings no new bytes.
                        answered.extend(released(&mut queue));
                    } else if let Some(bytes) = receives.next() {
                        conn.ingress.extend_from_slice(bytes);
                        let received = stream.len() - receives.clone().map(<[u8]>::len).sum::<usize>();
                        episodes.push(received / frames[0].len());
                    } else {
                        break;
                    }
                    action = run_process_frames_with(&mut conn, &mut producer, &halt, &mut early);
                    prop_assert_ne!(action, FrameAction::Disconnect);
                    take(&mut consumer, &mut queue, &mut published, &mut answered, false);
                }
                take(&mut consumer, &mut queue, &mut published, &mut answered, true);
                prop_assert!(conn.ingress.bytes().is_empty());
                prop_assert_eq!(conn.ingress.shed_remaining(), 0);

                // Every request exactly once: published, or answered as its
                // decoder outcome says, or busy.
                let mut seen: Vec<u64> = published.clone();
                seen.extend(answered.iter().map(|reply| reply.request_id.0));
                seen.sort_unstable();
                prop_assert_eq!(seen, (0..bodies.len() as u64).collect::<Vec<_>>());
                for id in &published {
                    let body = bodies[*id as usize];
                    prop_assert!(body <= 8 || body == 0xFF, "published {body:#04x}");
                }
                let mut busy: Vec<u64> = Vec::new();
                for reply in &answered {
                    let body = bodies[reply.request_id.0 as usize];
                    match reply.kind {
                        EarlyKind::Busy => busy.push(reply.request_id.0),
                        EarlyKind::Filtered => prop_assert_eq!(body, 0xFC),
                        EarlyKind::Refused { kind: RefusalKind::PermissionDenied, .. } => {
                            prop_assert_eq!(body, 0xFD)
                        }
                        EarlyKind::Refused { kind: RefusalKind::Malformed, .. } => {
                            prop_assert_eq!(body, 0xFE)
                        }
                        EarlyKind::Rejected(_) => prop_assert!(false, "not halted"),
                    }
                }
                // A request is answered in the first episode in which it
                // was complete (an episode ends only once every complete
                // frame is processed), and a shed request takes every
                // complete frame behind it then: busy too, never
                // published, never decoded.
                for &k in &busy {
                    let complete = episodes
                        .iter()
                        .copied()
                        .find(|&complete| complete > k as usize)
                        .expect("answered once complete");
                    for j in k + 1..complete as u64 {
                        prop_assert!(
                            busy.contains(&j),
                            "request {j} was in the tail shed at {k}, \
                             {complete} complete: {busy:?}"
                        );
                    }
                }
            }
        }
    }

    /// Every way a connection's receive state can change, against the
    /// CQEs the kernel can post and the outcomes a buffer can have: a park
    /// cancels an armed receive, a parked connection keeps the bytes that
    /// land before the cancel and is never re-armed while a cancel is in
    /// flight, a teardown issues no second cancel, and a dying connection
    /// is freed only on its terminal CQE.
    mod recv_state {
        use super::super::{
            CqeStep, FrameAction, FramesStep, RecvCompletion, RecvState, TeardownStep,
        };
        use std::time::{Duration, Instant};

        const DATA: RecvCompletion = RecvCompletion::Data;
        const EMPTY: RecvCompletion = RecvCompletion::Empty;
        const CLOSED: RecvCompletion = RecvCompletion::Closed;

        fn parked(cancelling: bool, since: Instant) -> RecvState {
            RecvState::Parked { cancelling, since }
        }

        /// The normal path: data on an armed receive is processed; its
        /// end, with or without data, re-arms it once processed, and an
        /// empty end (a dry pool) re-arms at once.
        #[test]
        fn an_armed_receive_is_processed_and_re_armed_when_it_ends() {
            let now = Instant::now();
            assert_eq!(
                RecvState::Armed.on_cqe(DATA, false),
                (RecvState::Armed, CqeStep::Process)
            );
            assert_eq!(
                RecvState::Armed.on_cqe(DATA, true),
                (RecvState::Idle, CqeStep::Process)
            );
            assert_eq!(
                RecvState::Idle.after_frames(FrameAction::Continue, now),
                (RecvState::Armed, FramesStep::Arm)
            );
            assert_eq!(
                RecvState::Armed.after_frames(FrameAction::Continue, now),
                (RecvState::Armed, FramesStep::Nothing)
            );
            assert_eq!(
                RecvState::Armed.on_cqe(EMPTY, true),
                (RecvState::Armed, CqeStep::Arm)
            );
            assert_eq!(
                RecvState::Armed.on_cqe(EMPTY, false),
                (RecvState::Armed, CqeStep::Nothing)
            );
            assert_eq!(
                RecvState::Armed.on_cqe(CLOSED, true),
                (RecvState::Idle, CqeStep::Remove)
            );
            // Nothing was armed: an end changes nothing, and one with no
            // bytes arms a receive.
            assert_eq!(RecvState::Idle.after_terminal(), RecvState::Idle);
            assert_eq!(RecvState::Dying.after_terminal(), RecvState::Dying);
            assert_eq!(
                RecvState::Idle.on_cqe(EMPTY, true),
                (RecvState::Armed, CqeStep::Arm)
            );
        }

        /// A park on an armed receive cancels it; bytes landing before the
        /// cancel's terminal CQE are kept, not processed; the terminal CQE
        /// (the cancel's, or an end of the receive's own) leaves it parked
        /// with nothing owed and does not re-arm; a retry that processes
        /// the buffer re-arms it.
        #[test]
        fn a_park_cancels_keeps_late_bytes_and_re_arms_on_resume() {
            let since = Instant::now();
            let later = since + std::time::Duration::from_secs(5);
            assert_eq!(
                RecvState::Armed.after_frames(FrameAction::Park, since),
                (parked(true, since), FramesStep::Cancel)
            );
            assert_eq!(
                parked(true, since).on_cqe(DATA, false),
                (parked(true, since), CqeStep::Keep)
            );
            assert_eq!(
                parked(true, since).on_cqe(DATA, true),
                (parked(false, since), CqeStep::Keep)
            );
            assert_eq!(
                parked(true, since).on_cqe(EMPTY, true),
                (parked(false, since), CqeStep::Nothing),
                "no re-arm while parked"
            );
            assert_eq!(
                parked(true, since).on_cqe(EMPTY, false),
                (parked(true, since), CqeStep::Nothing)
            );
            // Parked again by a retry: still parked since it first did.
            assert_eq!(
                parked(false, since).after_frames(FrameAction::Park, later),
                (parked(false, since), FramesStep::Nothing)
            );
            assert_eq!(
                parked(false, since).after_frames(FrameAction::Continue, later),
                (RecvState::Armed, FramesStep::Arm)
            );
            // A park on a receive already ended needs no cancel.
            assert_eq!(
                RecvState::Idle.after_frames(FrameAction::Park, since),
                (parked(false, since), FramesStep::Nothing)
            );
        }

        /// A retry that processes the buffer before the cancel's terminal
        /// CQE arms nothing: the connection resumes on that CQE, and the
        /// bytes that land before it are processed, as the connection is
        /// no longer parked. Parking again there needs no second cancel.
        #[test]
        fn a_resume_before_the_cancel_lands_arms_on_its_terminal_cqe() {
            let since = Instant::now();
            assert_eq!(
                parked(true, since).after_frames(FrameAction::Continue, since),
                (RecvState::Resuming, FramesStep::Nothing)
            );
            assert_eq!(
                RecvState::Resuming.on_cqe(DATA, false),
                (RecvState::Resuming, CqeStep::Process)
            );
            assert_eq!(
                RecvState::Resuming.after_frames(FrameAction::Continue, since),
                (RecvState::Resuming, FramesStep::Nothing)
            );
            assert_eq!(
                RecvState::Resuming.on_cqe(EMPTY, true),
                (RecvState::Armed, CqeStep::Arm)
            );
            assert_eq!(
                RecvState::Resuming.on_cqe(DATA, true),
                (RecvState::Idle, CqeStep::Process)
            );
            assert_eq!(
                RecvState::Resuming.after_frames(FrameAction::Park, since),
                (parked(true, since), FramesStep::Nothing)
            );
        }

        /// Teardown by state: an armed receive is cancelled; a cancel in
        /// flight (a park's, or a resume's) is awaited, never doubled;
        /// nothing owed frees at once; a second teardown does nothing.
        /// A dying connection discards its CQEs and is freed on the
        /// terminal one only. A connection whose peer closes while parked
        /// is removed.
        #[test]
        fn teardown_cancels_once_and_frees_once() {
            let since = Instant::now();
            assert_eq!(
                RecvState::Armed.on_teardown(),
                (RecvState::Dying, TeardownStep::Cancel)
            );
            for cancelling in [parked(true, since), RecvState::Resuming] {
                assert_eq!(
                    cancelling.on_teardown(),
                    (RecvState::Dying, TeardownStep::AwaitCancel),
                    "{cancelling:?}"
                );
            }
            for owed_nothing in [parked(false, since), RecvState::Idle] {
                assert_eq!(
                    owed_nothing.on_teardown(),
                    (owed_nothing, TeardownStep::FreeNow),
                    "{owed_nothing:?}"
                );
            }
            assert_eq!(
                RecvState::Dying.on_teardown(),
                (RecvState::Dying, TeardownStep::AlreadyDying)
            );
            for completion in [DATA, EMPTY, CLOSED] {
                assert_eq!(
                    RecvState::Dying.on_cqe(completion, false),
                    (RecvState::Dying, CqeStep::Discard { free: false })
                );
                assert_eq!(
                    RecvState::Dying.on_cqe(completion, true),
                    (RecvState::Dying, CqeStep::Discard { free: true })
                );
            }
            assert_eq!(parked(true, since).on_cqe(CLOSED, true).1, CqeStep::Remove);
            // A buffer that closes the connection tears it down, whatever
            // the state.
            for state in [
                RecvState::Armed,
                RecvState::Idle,
                RecvState::Resuming,
                parked(true, since),
            ] {
                assert_eq!(
                    state.after_frames(FrameAction::Disconnect, since),
                    (state, FramesStep::Teardown)
                );
            }
        }

        /// Only a park whose cancel is still in flight past the deadline
        /// counts as unhonoured: not one at the deadline, not one whose
        /// cancel has landed however long it has been parked, and no
        /// other state.
        #[test]
        fn only_a_park_cancel_in_flight_past_the_deadline_is_overdue() {
            let since = Instant::now();
            let past = since + super::super::PARK_CANCEL_DEADLINE + Duration::from_nanos(1);
            let at = since + super::super::PARK_CANCEL_DEADLINE;
            assert!(parked(true, since).park_cancel_overdue(past));
            assert!(!parked(true, since).park_cancel_overdue(at));
            assert!(!parked(true, since).park_cancel_overdue(since));
            for state in [
                parked(false, since),
                RecvState::Armed,
                RecvState::Idle,
                RecvState::Resuming,
                RecvState::Dying,
            ] {
                assert!(!state.park_cancel_overdue(past), "{state:?}");
            }
        }

        /// What the shutdown quiesce waits for: a CQE the kernel owes.
        #[test]
        fn the_kernel_owes_a_cqe_while_a_receive_or_a_cancel_is_in_flight() {
            let since = Instant::now();
            for owed in [
                RecvState::Armed,
                RecvState::Resuming,
                RecvState::Dying,
                parked(true, since),
            ] {
                assert!(owed.kernel_owes_cqe(), "{owed:?}");
            }
            for free in [RecvState::Idle, parked(false, since)] {
                assert!(!free.kernel_owes_cqe(), "{free:?}");
            }
        }
    }

    /// End-to-end soak through the real reader loop: 4 connections
    /// concurrently write 10 000 frames each, one `write(2)` per frame,
    /// so the receive path churns through the shared buffer pool and
    /// its recycle machinery under genuine concurrency (buf_ring where
    /// the kernel supports it, legacy ProvideBuffers otherwise — this
    /// test is the coverage for whichever mode the host engages).
    ///
    /// The failure signatures of a recycle bug are exactly what is
    /// asserted: a torn or misordered frame (a buffer reused while the
    /// kernel still owned it), a lost frame, or a spurious disconnect
    /// (`ENOBUFS` mishandled as a client error).
    #[test]
    fn reader_loop_soak_delivers_every_frame_in_order() {
        use std::io::Write as _;

        const CONNS: u64 = 4;
        const FRAMES: u32 = 10_000;

        // Capacity ≥ total frames: even if this thread is descheduled
        // and stops draining, the ring cannot fill, so the reader never
        // sheds load (which would legitimately drop frames and turn a
        // CI hiccup into a false failure).
        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(65536)
            .add_consumer()
            .build(melin_pipeline::wait::WaitStrategy::SpinThenYield);
        let mut consumer = consumers.pop().expect("consumer");
        let (control_tx, control_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));

        let mut handle = spawn_reader::<TestApp, UnixStream>(
            producer,
            Arc::new(ByteDecoder),
            gate(None, false),
            early_channel().0,
            policy_claim(),
            control_tx,
            0,    // "do not pin" sentinel
            None, // no idle timeout — a CI stall must not disconnect
            None, // no tick generator
            // Sized for exactly these connections: the smallest pool the
            // sizing hands out, so the soak runs the pool dry (ENOBUFS,
            // re-arm) far more often than a production-sized one would.
            RingSizing::for_max_connections(CONNS).expect("a supported cap"),
            Arc::clone(&shutdown),
        )
        .expect("the reader starts");

        let mut writers = Vec::new();
        for id in 0..CONNS {
            let (client, server_side) = UnixStream::pair().expect("socketpair");
            handle.register(ReaderRegistration {
                connection_id: ConnectionId(id),
                reader: server_side,
                addr: "127.0.0.1:1".parse().expect("addr"),
                role: ClientRole::Operator,
                key_hash: id,
            });
            writers.push(std::thread::spawn(move || {
                let mut client = client;
                for i in 0..FRAMES {
                    let byte = (i % 200 + 1) as u8;
                    client.write_all(&frame(byte)).expect("client write");
                    // Brief pauses fragment the stream so the reader
                    // sees many small recvs (heavy buffer churn) rather
                    // than a few large coalesced ones.
                    if i % 512 == 0 {
                        std::thread::sleep(Duration::from_micros(200));
                    }
                }
                client // keep the socket alive until after the drain
            }));
        }

        // Drain until every frame arrived or the deadline passes. The
        // deadline is generous — the run takes well under a second on
        // an idle machine — because a genuine recycle deadlock must
        // fail loudly, not hang CI.
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut per_conn_count = [0u32; CONNS as usize];
        let mut total: u64 = 0;
        while total < CONNS * FRAMES as u64 {
            assert!(
                Instant::now() < deadline,
                "soak stalled: {total} of {} frames after 60s ({per_conn_count:?})",
                CONNS * FRAMES as u64
            );
            while let Some((_seq, slot)) = consumer.try_consume() {
                let conn = slot.connection_id as usize;
                assert!(conn < CONNS as usize, "unknown connection id");
                let expected = (per_conn_count[conn] % 200 + 1) as u8;
                assert_eq!(
                    slot.event,
                    JournalEvent::App(TestEvent::Cmd(expected)),
                    "conn {conn} frame {} out of order or corrupted",
                    per_conn_count[conn]
                );
                per_conn_count[conn] += 1;
                total += 1;
            }
            std::hint::spin_loop();
        }

        // Any control event at this point is a defect: sockets are
        // still alive (held by the writer join handles) and the ring
        // never filled. Every arm diverges, so inspecting the first
        // queued event suffices.
        if let Ok(event) = control_rx.try_recv() {
            match event {
                ControlEvent::Disconnected { connection_id } => {
                    panic!("spurious disconnect of connection {connection_id}")
                }
                ControlEvent::Connected { .. } => {
                    unreachable!("reader never sends Connected")
                }
            }
        }

        let _clients: Vec<UnixStream> = writers
            .into_iter()
            .map(|w| w.join().expect("writer thread"))
            .collect();
        handle.shutdown();
        handle.join();
    }

    /// Shutdown with live, armed connections must quiesce the ring
    /// provably and promptly. The armed multishot RECVs sit on sockets
    /// whose peers never disconnect, so nothing completes them on its
    /// own — the teardown's wakes (per-connection `shutdown(2)`, the
    /// eventfd write) are what retire them, including on hosts that
    /// filter the AsyncCancel opcode, where the old cancel-then-quiet
    /// teardown could conclude "done" with arms still live against the
    /// soon-freed pool. A drain that cannot prove quiescence only
    /// returns at the full deadline (and leaks the buffers), so
    /// returning well inside it is the proof this test can observe.
    #[test]
    fn shutdown_quiesces_armed_connections_promptly() {
        use std::io::Write as _;

        const CONNS: u64 = 3;
        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(1024)
            .add_consumer()
            .build(melin_pipeline::wait::WaitStrategy::SpinThenYield);
        let mut consumer = consumers.pop().expect("consumer");
        let (control_tx, _control_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut handle = spawn_reader::<TestApp, UnixStream>(
            producer,
            Arc::new(ByteDecoder),
            gate(None, false),
            early_channel().0,
            policy_claim(),
            control_tx,
            0,    // "do not pin" sentinel
            None, // no idle timeout
            None, // no tick generator
            RingSizing::for_max_connections(CONNS).expect("a supported cap"),
            Arc::clone(&shutdown),
        )
        .expect("the reader starts");

        let mut clients = Vec::new();
        for id in 0..CONNS {
            let (mut client, server_side) = UnixStream::pair().expect("socketpair");
            handle.register(ReaderRegistration {
                connection_id: ConnectionId(id),
                reader: server_side,
                addr: "127.0.0.1:1".parse().expect("addr"),
                role: ClientRole::Operator,
                key_hash: id,
            });
            // One delivered frame per connection proves its multishot
            // is armed and active before the shutdown fires.
            client.write_all(&frame(7)).expect("client write");
            clients.push(client);
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut seen = 0u64;
        while seen < CONNS {
            assert!(
                Instant::now() < deadline,
                "frames never arrived ({seen}/{CONNS})"
            );
            while consumer.try_consume().is_some() {
                seen += 1;
            }
            std::hint::spin_loop();
        }

        // The clients stay alive across the shutdown: no peer
        // disconnect can complete the armed RECVs — only the teardown's
        // own wakes can.
        let started = Instant::now();
        handle.shutdown();
        handle.join();
        let elapsed = started.elapsed();
        assert!(
            elapsed < crate::uring_teardown::DRAIN_TIMEOUT / 4,
            "reader teardown took {elapsed:?}, close to the drain deadline — \
             armed operations were not woken"
        );
        drop(clients);
    }

    /// Regression for audit review F1
    /// (docs/internal/io-uring-audit-2026-08.md): a connection torn down
    /// while its multishot RECV is still armed must not have its slab
    /// index reused while the kernel can still post CQEs carrying it.
    /// Pre-fix, the LIFO free list handed client A's index straight to
    /// client B; A's socket stayed open (the armed op holds a file
    /// reference past the fd close), and A's continued bytes were parsed
    /// under B's connection id, key hash, and permissions — request
    /// injection under someone else's identity. Post-fix, A's entry dies
    /// in place until the cancelled op's terminal CQE, so B gets a fresh
    /// index and A's post-teardown bytes are discarded.
    #[test]
    fn torn_down_connection_cannot_inject_frames_into_a_reused_slot() {
        use std::io::Write as _;

        let (producer, mut consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(65536)
            .add_consumer()
            .build(melin_pipeline::wait::WaitStrategy::SpinThenYield);
        let mut consumer = consumers.pop().expect("consumer");
        let (control_tx, control_rx) = mpsc::channel();
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut handle = spawn_reader::<TestApp, UnixStream>(
            producer,
            Arc::new(ByteDecoder),
            gate(None, false),
            early_channel().0,
            policy_claim(),
            control_tx,
            0,
            None,
            None,
            // A and B, plus room for A's entry to linger while dying.
            RingSizing::for_max_connections(4).expect("a supported cap"),
            Arc::clone(&shutdown),
        )
        .expect("the reader starts");

        const A_ID: u64 = 100;
        const B_ID: u64 = 200;

        // Client A: valid frames (0x01..=0x08), an oversize prefix (the
        // teardown trigger), then a sustained stream of would-be
        // injection frames from A's byte range.
        let (a_client, a_server) = UnixStream::pair().expect("socketpair");
        handle.register(ReaderRegistration {
            connection_id: ConnectionId(A_ID),
            reader: a_server,
            addr: "127.0.0.1:1".parse().expect("addr"),
            role: ClientRole::Operator,
            key_hash: A_ID,
        });
        let a_writer = std::thread::spawn(move || {
            let mut a = a_client;
            for byte in 0x01..=0x08u8 {
                let _ = a.write_all(&frame(byte));
            }
            let _ = a.write_all(&oversize_prefix());
            // Keep writing after the reader repudiates us. Write errors
            // (EPIPE once teardown's shutdown(2) lands) are the fix
            // doing its job — ignore them and keep trying.
            let stop = Instant::now() + Duration::from_millis(300);
            while Instant::now() < stop {
                let _ = a.write_all(&frame(0x05));
                std::thread::sleep(Duration::from_micros(50));
            }
            a
        });

        // Wait for A's teardown announcement, then register B — the
        // instant a buggy free list would hand B the still-armed index.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match control_rx.try_recv() {
                Ok(ControlEvent::Disconnected { connection_id }) if connection_id == A_ID => break,
                Ok(_) => {}
                Err(_) => {
                    assert!(Instant::now() < deadline, "no Disconnected for A");
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
        }

        const B_FRAMES: u32 = 2_000;
        let (b_client, b_server) = UnixStream::pair().expect("socketpair");
        handle.register(ReaderRegistration {
            connection_id: ConnectionId(B_ID),
            reader: b_server,
            addr: "127.0.0.1:2".parse().expect("addr"),
            role: ClientRole::Operator,
            key_hash: B_ID,
        });
        let b_writer = std::thread::spawn(move || {
            let mut b = b_client;
            for i in 0..B_FRAMES {
                let byte = 0x21 + (i % 0x10) as u8; // 0x21..=0x30, disjoint from A
                b.write_all(&frame(byte)).expect("B write");
                if i % 128 == 0 {
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
            b
        });

        // Drain until all of B's frames arrive. Every B-attributed slot
        // must carry B's key hash and a B-range byte; anything else is
        // the injection this test exists to prevent.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut b_seen = 0u32;
        while b_seen < B_FRAMES {
            assert!(
                Instant::now() < deadline,
                "stalled: {b_seen}/{B_FRAMES} B frames"
            );
            while let Some((_seq, slot)) = consumer.try_consume() {
                match slot.event {
                    JournalEvent::App(TestEvent::Cmd(byte)) => {
                        if slot.connection_id == B_ID {
                            assert_eq!(slot.key_hash, B_ID, "B slot with foreign key hash");
                            assert!(
                                (0x21..=0x30).contains(&byte),
                                "byte {byte:#x} injected into B's stream"
                            );
                            b_seen += 1;
                        } else {
                            assert_eq!(slot.connection_id, A_ID, "unknown connection id");
                            assert!(
                                (0x01..=0x08).contains(&byte),
                                "unexpected byte {byte:#x} on A"
                            );
                        }
                    }
                    ref other => panic!("unexpected event {other:?}"),
                }
            }
            std::hint::spin_loop();
        }

        let _a = a_writer.join().expect("A writer");
        let _b = b_writer.join().expect("B writer");
        handle.shutdown();
        handle.join();
    }

    /// Parking through the real reader loop, on a real io_uring over
    /// socket pairs: a small input ring the test drains (or not), and an
    /// early-reply queue of capacity 2 that the test drains in the
    /// response stage's place, so it fills on demand. Filtered requests
    /// (`0xFC`) are the early replies that fill it.
    mod parking {
        use super::*;
        use crate::halt::EarlyReplyQueue;
        use melin_transport_core::fence::FenceState;
        use std::io::Write as _;

        const FILTERED: u8 = 0xFC;

        /// A running reader, the ends the test plays the pipeline with,
        /// and its client connections.
        struct Rig {
            handle: UringReaderHandle<UnixStream>,
            consumer: ring::Consumer<InputSlot<TestEvent>>,
            queue: EarlyReplyQueue<TestReport>,
            control_rx: mpsc::Receiver<ControlEvent>,
            fence: Arc<FenceState>,
        }

        impl Rig {
            /// A reader over an input ring of `ring_capacity` and a
            /// two-entry early-reply queue, with `connection_timeout`.
            fn start(ring_capacity: usize, connection_timeout: Option<Duration>) -> Self {
                let (producer, mut consumers) =
                    DisruptorBuilder::<InputSlot<TestEvent>>::new(ring_capacity)
                        .add_consumer()
                        .build(melin_pipeline::wait::WaitStrategy::SpinThenYield);
                let consumer = consumers.pop().expect("consumer");
                let (control_tx, control_rx) = mpsc::channel();
                let fence = Arc::new(FenceState::new(0));
                let halt = HaltGate::new(None, Arc::clone(&fence), Arc::new(AtomicU64::new(0)));
                let (early, queue) = early_channel_of(2);
                let handle = spawn_reader::<TestApp, UnixStream>(
                    producer,
                    Arc::new(ByteDecoder),
                    halt,
                    early,
                    policy_claim(),
                    control_tx,
                    0,
                    connection_timeout,
                    None,
                    RingSizing::for_max_connections(4).expect("a supported cap"),
                    Arc::new(AtomicBool::new(false)),
                )
                .expect("the reader starts");
                Rig {
                    handle,
                    consumer,
                    queue,
                    control_rx,
                    fence,
                }
            }

            /// Register connection `id`; returns the client's end and a
            /// second handle on the server's end (the response stage's
            /// dup, in a node).
            fn connect(&mut self, id: u64) -> (UnixStream, UnixStream) {
                let (client, server_side) = UnixStream::pair().expect("socketpair");
                let dup = server_side.try_clone().expect("dup");
                self.handle.register(ReaderRegistration {
                    connection_id: ConnectionId(id),
                    reader: server_side,
                    addr: "127.0.0.1:1".parse().expect("addr"),
                    role: ClientRole::Operator,
                    key_hash: id,
                });
                (client, dup)
            }

            /// Every early reply queued so far, as the response stage
            /// would take them: making room in the queue.
            fn take_replies(&mut self) -> Vec<EarlyReply<TestReport>> {
                released(&mut self.queue)
            }

            /// Every request published so far, by ID.
            fn take_published(&mut self) -> Vec<RequestId> {
                drain(&mut self.consumer)
                    .into_iter()
                    .map(|(_, slot)| slot.request_id)
                    .collect()
            }

            /// Wait until `done` holds of what was published and answered
            /// so far (accumulated into the two vectors), taking both as
            /// the pipeline would; panics after ten seconds.
            fn wait_for(
                &mut self,
                published: &mut Vec<RequestId>,
                replies: &mut Vec<EarlyReply<TestReport>>,
                done: impl Fn(&[RequestId], &[EarlyReply<TestReport>]) -> bool,
            ) {
                let deadline = Instant::now() + Duration::from_secs(10);
                loop {
                    published.extend(self.take_published());
                    replies.extend(self.take_replies());
                    if done(published, replies) {
                        return;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "stalled: published {published:?}, answered {replies:?}"
                    );
                    std::thread::sleep(Duration::from_millis(1));
                }
            }

            /// The `Disconnected` events sent so far, by connection.
            fn disconnects(&self) -> Vec<u64> {
                self.control_rx
                    .try_iter()
                    .map(|event| match event {
                        ControlEvent::Disconnected { connection_id } => connection_id,
                        ControlEvent::Connected { .. } => {
                            unreachable!("the reader never sends Connected")
                        }
                    })
                    .collect()
            }

            fn stop(self) {
                self.handle.shutdown();
                self.handle.join();
            }
        }

        /// What every test's connection writes: three filtered requests,
        /// the third of which parks it (two fill the queue), then five
        /// writes, in one burst.
        fn burst() -> Vec<u8> {
            let mut bytes = Vec::new();
            for id in 1..=3 {
                bytes.extend(request_frame(id, &[FILTERED]));
            }
            for id in 4..=8 {
                bytes.extend(request_frame(id, &[0x01]));
            }
            bytes
        }

        fn filtered_ids(replies: &[EarlyReply<TestReport>]) -> Vec<RequestId> {
            replies
                .iter()
                .map(|reply| {
                    assert_eq!(reply.kind, EarlyKind::Filtered, "{reply:?}");
                    reply.request_id
                })
                .collect()
        }

        /// Park the connection: write the burst, give the reader time to
        /// answer the first two (left in the queue, which they fill) and
        /// park on the third, and check that nothing behind it was
        /// published.
        fn park(rig: &mut Rig, client: &mut UnixStream) {
            client.write_all(&burst()).expect("client write");
            std::thread::sleep(Duration::from_millis(100));
            let published = rig.take_published();
            assert!(published.is_empty(), "published past a park: {published:?}");
        }

        /// The lost-signals scenario: a parked connection resumes without
        /// a byte more from its client once the queue has room, and its
        /// pipelined writes all complete, in order, each once. Bytes keep
        /// coming while it is parked too, and are kept.
        #[test]
        fn a_parked_connection_resumes_without_new_bytes() {
            let mut rig = Rig::start(64, None);
            let (mut client, _dup) = rig.connect(1);
            park(&mut rig, &mut client);
            // While parked, more writes behind: kept for the retry (a
            // data CQE landing before the park's cancel, or the socket's
            // buffer after it).
            for id in 9..=12 {
                client
                    .write_all(&request_frame(id, &[0x02]))
                    .expect("client write");
            }
            std::thread::sleep(Duration::from_millis(50));
            assert!(
                rig.take_published().is_empty(),
                "nothing read past the park"
            );

            let (mut published, mut replies) = (Vec::new(), Vec::new());
            rig.wait_for(&mut published, &mut replies, |p, _| p.len() == 9);
            assert_eq!(published, (4..=12).map(RequestId).collect::<Vec<_>>());
            assert_eq!(
                filtered_ids(&replies),
                (1..=3).map(RequestId).collect::<Vec<_>>()
            );
            assert!(rig.disconnects().is_empty(), "the connection is kept");

            // Read again normally once resumed.
            client
                .write_all(&request_frame(13, &[0x03]))
                .expect("client write");
            rig.wait_for(&mut published, &mut replies, |p, _| p.len() == 10);
            assert_eq!(published.last(), Some(&RequestId(13)));
            drop(client);
            rig.stop();
        }

        /// Many parks and resumes in a row, with the client writing one
        /// frame per `write` as fast as it can, so data CQEs keep landing
        /// around each park's cancel: every request is published or
        /// answered exactly once, the writes in order.
        #[test]
        fn repeated_parks_lose_and_repeat_nothing() {
            const REQUESTS: u64 = 600;
            let mut rig = Rig::start(1024, None);
            let (client, _dup) = rig.connect(1);
            let writer = std::thread::spawn(move || {
                let mut client = client;
                for id in 1..=REQUESTS {
                    let body = if id % 3 == 0 { 0x01 } else { FILTERED };
                    client
                        .write_all(&request_frame(id, &[body]))
                        .expect("client write");
                }
                client
            });
            let (mut published, mut replies) = (Vec::new(), Vec::new());
            rig.wait_for(&mut published, &mut replies, |p, r| {
                (p.len() + r.len()) as u64 >= REQUESTS
            });
            let _client = writer.join().expect("writer");
            assert_eq!(
                published,
                (1..=REQUESTS)
                    .filter(|id| id % 3 == 0)
                    .map(RequestId)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                filtered_ids(&replies),
                (1..=REQUESTS)
                    .filter(|id| id % 3 != 0)
                    .map(RequestId)
                    .collect::<Vec<_>>()
            );
            assert!(rig.disconnects().is_empty());
            rig.stop();
        }

        /// The idle scan skips a parked connection: its client is not
        /// idle, the node is not reading it. Resumed, it is judged on its
        /// idle clock restarted then.
        #[test]
        fn a_parked_connection_is_not_reaped_as_idle() {
            let mut rig = Rig::start(64, Some(Duration::from_millis(100)));
            let (mut client, _dup) = rig.connect(1);
            park(&mut rig, &mut client);
            // Past several scans (one a second) and many timeouts.
            std::thread::sleep(Duration::from_millis(2_500));
            assert!(rig.disconnects().is_empty(), "reaped while parked");

            let (mut published, mut replies) = (Vec::new(), Vec::new());
            rig.wait_for(&mut published, &mut replies, |p, _| p.len() == 5);
            // Idle from its resume on, it is reaped as any other. The scan
            // runs only when the reader wakes, so a second, busy client
            // keeps waking it.
            let (mut other, _other_dup) = rig.connect(2);
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut disconnects = Vec::new();
            let mut id = 100;
            while disconnects.is_empty() {
                assert!(Instant::now() < deadline, "never reaped once resumed");
                id += 1;
                other.write_all(&request_frame(id, &[0x01])).expect("write");
                rig.take_published();
                disconnects.extend(rig.disconnects());
                std::thread::sleep(Duration::from_millis(10));
            }
            assert_eq!(disconnects, [1], "the resumed connection, not the busy one");
            drop((client, other));
            rig.stop();
        }

        /// A parked connection whose retry closes it — the node fenced
        /// while it was parked — is torn down once: one `Disconnected`,
        /// and its index is freed for the next connection.
        #[test]
        fn a_parked_connection_closed_by_its_retry_is_torn_down_once() {
            let mut rig = Rig::start(64, None);
            let (mut client, _dup) = rig.connect(1);
            park(&mut rig, &mut client);
            // Its cancel has landed by now: torn down with nothing owed.
            rig.fence.fence();
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut disconnects = Vec::new();
            while disconnects.is_empty() {
                assert!(Instant::now() < deadline, "never torn down");
                disconnects.extend(rig.disconnects());
                std::thread::sleep(Duration::from_millis(1));
            }
            std::thread::sleep(Duration::from_millis(50));
            disconnects.extend(rig.disconnects());
            assert_eq!(disconnects, [1], "torn down once");
            assert!(rig.take_published().is_empty());
            rig.stop();
        }

        /// A request that is not a request, in a shed tail a park cut
        /// short, closes the connection when the retry reaches it: the
        /// tail before it answered busy, nothing past the full ring
        /// published, one `Disconnected`.
        #[test]
        fn a_non_request_in_a_parked_shed_tail_closes_the_connection_once() {
            // Two slots, never drained: the third write is shed.
            let mut rig = Rig::start(2, None);
            let (mut client, _dup) = rig.connect(1);
            let mut bytes = Vec::new();
            for id in 1..=5 {
                bytes.extend(request_frame(id, &[0x01]));
            }
            bytes.extend(tagged_frame(0x00, &[0x03; 9]));
            client.write_all(&bytes).expect("client write");

            let (mut published, mut replies) = (Vec::new(), Vec::new());
            // Writes 3 and 4 are shed and fill the queue; 5 parks.
            let deadline = Instant::now() + Duration::from_secs(10);
            while published.len() < 2 {
                assert!(Instant::now() < deadline, "the ring never filled");
                published.extend(
                    drain(&mut rig.consumer)
                        .into_iter()
                        .map(|(_, s)| s.request_id),
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            // Room in the ring now, but 5 is in the tail.
            std::thread::sleep(Duration::from_millis(50));
            assert!(rig.disconnects().is_empty(), "parked, not closed");
            let mut disconnects = Vec::new();
            rig.wait_for(&mut published, &mut replies, |_, r| r.len() == 3);
            let deadline = Instant::now() + Duration::from_secs(10);
            while disconnects.is_empty() {
                assert!(Instant::now() < deadline, "never closed");
                disconnects.extend(rig.disconnects());
                std::thread::sleep(Duration::from_millis(1));
            }
            std::thread::sleep(Duration::from_millis(50));
            disconnects.extend(rig.disconnects());
            assert_eq!(disconnects, [1], "torn down once");
            assert_eq!(published, [RequestId(1), RequestId(2)]);
            assert_eq!(
                replies,
                (3..=5)
                    .map(|id| early_reply_for(1, RequestId(id), EarlyKind::Busy))
                    .collect::<Vec<_>>()
            );
            rig.stop();
        }

        /// A parked connection the response stage tears down (a
        /// `shutdown(2)` on its own dup, after a send failure) is freed
        /// once: the stage drops the connection's early replies, which
        /// makes room, the retry re-arms the receive, and the receive
        /// reports the close. One `Disconnected`, and the next connection
        /// is served.
        #[test]
        fn a_parked_connection_torn_down_by_the_response_stage_is_freed_once() {
            let mut rig = Rig::start(64, None);
            let (mut client, dup) = rig.connect(1);
            park(&mut rig, &mut client);
            // SAFETY: a live descriptor, owned by `dup`.
            unsafe { libc::shutdown(dup.as_raw_fd(), libc::SHUT_RDWR) };
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut disconnects = Vec::new();
            while disconnects.is_empty() {
                assert!(Instant::now() < deadline, "never freed");
                rig.take_replies();
                rig.take_published();
                disconnects.extend(rig.disconnects());
                std::thread::sleep(Duration::from_millis(1));
            }
            std::thread::sleep(Duration::from_millis(50));
            disconnects.extend(rig.disconnects());
            assert_eq!(disconnects, [1], "freed once");

            let (mut next, _next_dup) = rig.connect(2);
            next.write_all(&request_frame(1, &[0x01])).expect("write");
            let mut published = Vec::new();
            rig.wait_for(&mut published, &mut Vec::new(), |p, _| {
                p.contains(&RequestId(1))
            });
            rig.stop();
        }

        /// [`early_reply`] for connection `connection_id`.
        fn early_reply_for(
            connection_id: u64,
            request_id: RequestId,
            kind: EarlyKind<TestReport>,
        ) -> EarlyReply<TestReport> {
            EarlyReply {
                connection_id,
                request_id,
                kind,
            }
        }
    }

    /// A reader that cannot create its io_uring instance reports it to
    /// the spawner as an error naming the thread, instead of panicking
    /// on a thread nobody is watching. A zero-entry ring stands in for
    /// the locked-memory limit: both fail `io_uring_setup`.
    #[test]
    fn a_ring_that_cannot_be_created_is_a_startup_error() {
        let (producer, _consumers) = DisruptorBuilder::<InputSlot<TestEvent>>::new(64)
            .add_consumer()
            .build(melin_pipeline::wait::WaitStrategy::SpinThenYield);
        let (control_tx, _control_rx) = mpsc::channel();
        let result = spawn_reader::<TestApp, UnixStream>(
            producer,
            Arc::new(ByteDecoder),
            gate(None, false),
            early_channel().0,
            policy_claim(),
            control_tx,
            0,
            None,
            None,
            RingSizing::unchecked(0, 32),
            Arc::new(AtomicBool::new(false)),
        );
        let err = match result {
            Ok(_) => panic!("a zero-entry ring cannot be created"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(msg.contains("uring-reader"), "{msg}");
        assert!(msg.contains("io_uring"), "{msg}");
    }

    /// `push_sqe` on a full SQ submits what is queued and pushes again,
    /// instead of failing: pushing more SQEs than the ring holds must
    /// lose none of them.
    #[test]
    fn push_sqe_makes_room_when_the_sq_is_full() {
        const ENTRIES: u32 = 8;
        // Three ring-fulls, so the full-SQ branch is taken more than once.
        const PUSHES: u64 = 3 * ENTRIES as u64;
        // The CQ is twice the SQ by default; size it to hold every
        // completion so none is dropped before it is counted.
        let mut ring = IoUring::builder()
            .setup_cqsize(PUSHES as u32)
            .build(ENTRIES)
            .expect("create an 8-entry io_uring");
        for token in 0..PUSHES {
            let nop = opcode::Nop::new().build().user_data(token);
            // SAFETY: a NOP references no memory.
            unsafe { push_sqe(&mut ring, &nop, "test NOP") };
        }
        ring.submit_and_wait(PUSHES as usize)
            .expect("submit the remaining NOPs");
        let mut seen: Vec<u64> = ring.completion().map(|cqe| cqe.user_data()).collect();
        seen.sort_unstable();
        assert_eq!(seen, (0..PUSHES).collect::<Vec<_>>());
    }
}
