//! Shared io_uring teardown policy for every ring-owning thread: the
//! replication sender's live-stream ring, the replication receiver's
//! session ring, and the client-facing reader.
//!
//! All of them hand the kernel pointers into heap buffers — a landing
//! area a RECV writes into, a source buffer a SEND reads out of, a
//! provided-buffer pool a multishot RECV selects from. Closing the ring
//! fd does not retract those pointers: ring teardown is *asynchronous*,
//! so an operation still in flight can complete after the allocation
//! has been returned to the allocator. On the replication sender that
//! surfaced as malloc-metadata corruption and a SIGSEGV at thread exit;
//! being a kernel-side store, it is invisible to AddressSanitizer.
//!
//! Every side therefore tears down the same way, and shares the policy
//! here so it cannot drift apart: wake whatever the kernel holds with a
//! `shutdown(2)` (never an AsyncCancel alone — some hosts filter
//! io_uring opcodes), reap completions until it holds nothing, and only
//! then release the buffers — leaking them if the drain cannot be
//! proven complete.

use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};

/// Upper bound on a teardown drain. The wake is a `shutdown(2)` on the
/// very socket the pending operations are bound to, so they are
/// expected to complete within microseconds; this bounds only the
/// pathological case (a wedged io-wq worker), where waiting forever
/// would hang the transport thread and everything that joins it.
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Poll iterations spent spinning before backing off to a sleep.
const SPIN_LIMIT: u32 = 1024;

/// Sleep between polls once [`SPIN_LIMIT`] is exhausted. Long enough
/// that a pathological drain costs nothing, short enough that it adds
/// no meaningful latency to an ordinary teardown.
const BACKOFF_SLEEP: Duration = Duration::from_micros(200);

/// Wake every operation the kernel is holding against `fd`: a shutdown
/// socket completes a pending RECV with EOF and a pending SEND with an
/// error, immediately and without relying on AsyncCancel opcode
/// availability — this must also work on hosts that filter io_uring
/// opcodes.
///
/// Best-effort by design, hence the discarded return: callers hold an
/// fd that outlives the ring being torn down, and a socket the peer
/// already reset simply answers `ENOTCONN`, which changes nothing about
/// the drain that follows.
pub(crate) fn wake_pending_ops(fd: RawFd) {
    // SAFETY: `fd` belongs to a socket that outlives the ring being
    // torn down — see the fd-holding field docs on each caller.
    unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
}

/// Deadline and backoff for a teardown drain loop.
///
/// Drains poll the memory-mapped completion queue rather than blocking
/// in `submit_and_wait`, which takes no timeout: an operation the wake
/// failed to rouse would hang the thread — and everything that joins it
/// — forever, and trading a use-after-free for a deadlock is not a
/// trade worth making.
pub(crate) struct DrainBackoff {
    deadline: Instant,
    spins: u32,
}

impl DrainBackoff {
    pub(crate) fn new() -> Self {
        Self {
            deadline: Instant::now() + DRAIN_TIMEOUT,
            spins: 0,
        }
    }

    /// Pause before the next completion-queue poll. Returns `false` once
    /// the deadline has passed, at which point the caller must keep
    /// treating its buffers as kernel-visible.
    pub(crate) fn wait(&mut self) -> bool {
        if Instant::now() >= self.deadline {
            return false;
        }
        // The expected wait is a few microseconds, so spin first; back
        // off afterwards so a pathological drain does not burn a core
        // for the whole timeout.
        if self.spins < SPIN_LIMIT {
            self.spins += 1;
            std::hint::spin_loop();
        } else {
            std::thread::sleep(BACKOFF_SLEEP);
        }
        true
    }
}
