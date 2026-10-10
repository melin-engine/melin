//! The client connection cap, and the io_uring sizing derived from it.
//!
//! `--max-connections` bounds how many authenticated clients a node
//! serves at once, and that bound is also what the kernel-TCP transport's
//! two client-facing io_uring instances — the reader's and the response
//! stage's — have to hold. The rings are sized from it once, at startup,
//! rather than fixed for the largest deployment: the kernel charges ring
//! memory against the locked-memory limit (`RLIMIT_MEMLOCK`), so a ring
//! sized for thousands of connections costs a node that serves a handful
//! the same budget as one that serves thousands.
//!
//! What has to fit, per drain of the reader's completion queue:
//!
//! - **Two SQEs per connection.** A connection's multishot RECV can
//!   terminate and be re-armed, and the same drain can begin its teardown
//!   (an `AsyncCancel`) — malformed frame or idle timeout.
//! - **Housekeeping**: the eventfd READ re-arm, the tick `TIMEOUT`, the
//!   shutdown `AsyncCancel`, and the legacy fallback's one-off
//!   `ProvideBuffers` registration.
//!
//! This bounds the steady state, not every drain: a connection being torn
//! down stays in the reader until its cancel completes, by which time the
//! accept loop may already have admitted its replacement, so the reader
//! can briefly track more connections than the cap. The reader hands
//! queued SQEs to the kernel when the submission queue fills rather than
//! relying on the bound, so that overshoot costs an extra submit, not a
//! panic.
//!
//! The response stage submits at most one SEND per connection per flush,
//! so the same ring rule covers it with room to spare.
//!
//! The provided-buffer pool holds two buffers per connection, so a
//! connection can be refilled while its last chunk is still being parsed,
//! and stays strictly smaller than the reader's ring (the legacy
//! `ProvideBuffers` fallback re-provides one buffer per SQE).

use std::fmt;
use std::io;

/// Largest ring io_uring creates (`IORING_MAX_ENTRIES`), which is also
/// the largest provided-buffer ring the kernel registers.
pub const MAX_RING_ENTRIES: u32 = 32768;

/// Smallest ring the sizing hands out. A floor rather than the exact
/// requirement: a ring of a few entries saves nothing worth having (the
/// kernel charges ring memory in whole pages) and leaves no headroom for a
/// housekeeping SQE added later.
pub const MIN_RING_ENTRIES: u32 = 64;

/// Smallest provided-buffer pool. Two buffers per connection is plenty
/// for many connections, where one connection's burst is spread over the
/// pool, and starves a node with a cap of one or two: a single client
/// pipelining requests would exhaust it on every drain and pay a re-arm
/// round trip for each. Half the ring floor, so the pool stays strictly
/// below the ring.
const MIN_PROVIDED_BUFFERS: u16 = 32;

/// SQEs one connection can put on the reader's submission queue in one
/// drain: a multishot RECV re-arm and a teardown `AsyncCancel`.
const SQES_PER_CONNECTION: u64 = 2;

/// SQEs the reader queues beyond the per-connection ones in one drain:
/// the eventfd READ re-arm, the tick `TIMEOUT`, the shutdown
/// `AsyncCancel`, and the legacy fallback's one-off `ProvideBuffers`
/// registration.
const HOUSEKEEPING_SQES: u64 = 4;

/// Provided buffers per connection.
const BUFFERS_PER_CONNECTION: u64 = 2;

/// Largest `--max-connections` a node accepts.
///
/// The provided-buffer pool must stay strictly below the reader's ring,
/// and both are powers of two, so the largest pool is half the largest
/// ring; two buffers per connection then gives the cap. One more
/// connection would round the pool up to the ring's own maximum.
pub const MAX_SUPPORTED_CONNECTIONS: u64 = MAX_RING_ENTRIES as u64 / 2 / BUFFERS_PER_CONNECTION;

/// io_uring sizes for the kernel-TCP transport, derived from the
/// connection cap by [`RingSizing::for_max_connections`].
///
/// `u32` ring entries because that is io_uring's own type for them;
/// `u16` buffers because the buf_ring ABI addresses a buffer by a 16-bit
/// id (the kernel's maximum, 32768, fits).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RingSizing {
    ring_entries: u32,
    provided_buffers: u16,
}

impl RingSizing {
    /// Size the rings for a node serving at most `max_connections`
    /// clients.
    ///
    /// Provided buffers are the next power of two at or above two per
    /// connection, with a small floor. Ring entries are the next power of
    /// two at or above two SQEs per connection plus housekeeping, and
    /// strictly above the pool, clamped to
    /// [[`MIN_RING_ENTRIES`], [`MAX_RING_ENTRIES`]]. At the default cap
    /// (1024) that is 4096 ring entries and 2048 buffers.
    ///
    /// Refuses `0` (formerly "unlimited": the rings need a bound to be
    /// sized from) and anything above [`MAX_SUPPORTED_CONNECTIONS`].
    pub fn for_max_connections(max_connections: u64) -> Result<Self, MaxConnectionsError> {
        if max_connections == 0 {
            return Err(MaxConnectionsError::Unlimited);
        }
        if max_connections > MAX_SUPPORTED_CONNECTIONS {
            return Err(MaxConnectionsError::AboveCeiling(max_connections));
        }
        // No overflow: `max_connections` is at most the ceiling, so every
        // product below is a few tens of thousands.
        let sqes = SQES_PER_CONNECTION * max_connections + HOUSEKEEPING_SQES;
        let buffers = (BUFFERS_PER_CONNECTION * max_connections)
            .next_power_of_two()
            .max(u64::from(MIN_PROVIDED_BUFFERS));
        // The ring also has to stay strictly above the pool. Both are
        // powers of two, so that is at least twice the pool — which only
        // binds where two buffers per connection round up to the same
        // power of two as the SQE count does.
        let ring_entries = sqes
            .next_power_of_two()
            .max(2 * buffers)
            .clamp(u64::from(MIN_RING_ENTRIES), u64::from(MAX_RING_ENTRIES));
        let sizing = Self {
            ring_entries: u32::try_from(ring_entries)
                .expect("ring entries are clamped to MAX_RING_ENTRIES"),
            provided_buffers: u16::try_from(buffers)
                .expect("buffers stay below MAX_RING_ENTRIES at the connection ceiling"),
        };
        // Checked once at startup, so unconditional: the reader's
        // `expect`s on SQ space and the buf_ring's ABI rest on these.
        assert!(
            u64::from(sizing.ring_entries) >= sqes,
            "the ring holds a drain's worth of SQEs"
        );
        assert!(
            sizing.provided_buffers.is_power_of_two(),
            "the buf_ring ABI requires a power-of-two pool"
        );
        assert!(
            u32::from(sizing.provided_buffers) < sizing.ring_entries,
            "the provided-buffer pool stays strictly below the ring"
        );
        Ok(sizing)
    }

    /// Entries in each of the reader's and the response stage's
    /// submission queues (the completion queue is twice that).
    pub fn ring_entries(&self) -> u32 {
        self.ring_entries
    }

    /// Buffers in the reader's provided-buffer pool.
    pub fn provided_buffers(&self) -> u16 {
        self.provided_buffers
    }

    /// A sizing that skips the invariants, for tests that need ring
    /// creation to fail.
    #[cfg(test)]
    pub(crate) fn unchecked(ring_entries: u32, provided_buffers: u16) -> Self {
        Self {
            ring_entries,
            provided_buffers,
        }
    }
}

/// A `--max-connections` the node cannot size its rings for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaxConnectionsError {
    /// `0`, which used to mean "unlimited".
    Unlimited,
    /// Above [`MAX_SUPPORTED_CONNECTIONS`].
    AboveCeiling(u64),
}

impl fmt::Display for MaxConnectionsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unlimited => write!(
                f,
                "--max-connections 0 (unlimited) is no longer supported: the io_uring rings \
                 are sized from the connection cap. Set a cap between 1 and \
                 {MAX_SUPPORTED_CONNECTIONS}"
            ),
            Self::AboveCeiling(n) => write!(
                f,
                "--max-connections {n} is above the largest supported value, \
                 {MAX_SUPPORTED_CONNECTIONS}: the io_uring rings are sized from the \
                 connection cap, and the kernel's largest ring bounds them"
            ),
        }
    }
}

impl std::error::Error for MaxConnectionsError {}

/// Whether a node holding `active` authenticated connections must refuse
/// the next one. The one gate both transports apply.
#[inline]
pub(crate) fn connection_cap_reached(active: u64, max_connections: u64) -> bool {
    active >= max_connections
}

/// The startup error for an io_uring instance a stage could not create.
///
/// `ENOMEM` from `io_uring_setup` is, in practice, the locked-memory
/// limit: the kernel charges ring memory against `RLIMIT_MEMLOCK`, per
/// user and across all of that user's processes, and systemd's default
/// limit is a few megabytes. The message says so and names the fix.
/// Other errors (io_uring disabled by sysctl, filtered by seccomp) are
/// reported as they are, since the limit has nothing to do with them.
pub(crate) fn ring_setup_error(stage: &str, entries: u32, err: io::Error) -> io::Error {
    let kind = err.kind();
    if err.raw_os_error() == Some(libc::ENOMEM) {
        io::Error::new(
            kind,
            format!(
                "{stage}: cannot create its io_uring instance ({entries} entries): {err}. \
                 The kernel charges io_uring ring memory against the locked-memory limit \
                 (RLIMIT_MEMLOCK), per user across all processes. Raise it for the node's \
                 service (systemd: LimitMEMLOCK=infinity) or grant it CAP_IPC_LOCK, or \
                 lower --max-connections"
            ),
        )
    } else {
        io::Error::new(
            kind,
            format!("{stage}: cannot create its io_uring instance ({entries} entries): {err}"),
        )
    }
}

/// Wait for a worker thread's startup report: `Ok` once it holds its
/// io_uring instance, the creation error otherwise.
///
/// A worker that dies before reporting (a panic during setup) drops its
/// sender, which ends the wait with an error rather than a hang.
pub(crate) fn await_startup(
    stage: &str,
    report: &std::sync::mpsc::Receiver<io::Result<()>>,
) -> io::Result<()> {
    match report.recv() {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvError) => Err(io::Error::other(format!(
            "{stage}: thread exited before reporting whether it started"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sizing(max_connections: u64) -> RingSizing {
        RingSizing::for_max_connections(max_connections).expect("a supported cap")
    }

    /// The default cap keeps the sizes the rings had when they were
    /// fixed, so a production node's memory and behaviour do not move.
    #[test]
    fn the_default_cap_keeps_the_fixed_sizes() {
        let s = sizing(1024);
        assert_eq!(s.ring_entries(), 4096);
        assert_eq!(s.provided_buffers(), 2048);
    }

    #[test]
    fn sizes_round_up_to_powers_of_two() {
        let s = sizing(64);
        assert_eq!(s.ring_entries(), 256);
        assert_eq!(s.provided_buffers(), 128);
        let s = sizing(1025);
        assert_eq!(s.ring_entries(), 8192);
        assert_eq!(s.provided_buffers(), 4096);
    }

    /// Where two buffers per connection and the SQE count round up to the
    /// same power of two, the ring doubles to stay above the pool.
    #[test]
    fn the_ring_stays_strictly_above_the_pool() {
        // 2 × 100 + 4 = 204 and 2 × 100 = 200 both round to 256.
        let s = sizing(100);
        assert_eq!(s.provided_buffers(), 256);
        assert_eq!(s.ring_entries(), 512);
    }

    #[test]
    fn small_caps_take_the_floors() {
        let s = sizing(1);
        assert_eq!(s.ring_entries(), MIN_RING_ENTRIES);
        assert_eq!(s.provided_buffers(), MIN_PROVIDED_BUFFERS);
    }

    #[test]
    fn the_ceiling_is_the_largest_cap_that_sizes() {
        assert_eq!(MAX_SUPPORTED_CONNECTIONS, 8192);
        let s = sizing(MAX_SUPPORTED_CONNECTIONS);
        assert_eq!(s.ring_entries(), MAX_RING_ENTRIES);
        assert_eq!(s.provided_buffers(), 16384);
        assert_eq!(
            RingSizing::for_max_connections(MAX_SUPPORTED_CONNECTIONS + 1),
            Err(MaxConnectionsError::AboveCeiling(
                MAX_SUPPORTED_CONNECTIONS + 1
            ))
        );
        assert_eq!(
            RingSizing::for_max_connections(u64::MAX),
            Err(MaxConnectionsError::AboveCeiling(u64::MAX))
        );
    }

    #[test]
    fn zero_is_refused_and_says_why() {
        let err = RingSizing::for_max_connections(0).expect_err("0 is refused");
        assert_eq!(err, MaxConnectionsError::Unlimited);
        let msg = err.to_string();
        assert!(msg.contains("unlimited"), "{msg}");
        assert!(msg.contains("io_uring"), "{msg}");
        assert!(
            msg.contains(&MAX_SUPPORTED_CONNECTIONS.to_string()),
            "{msg}"
        );
    }

    #[test]
    fn above_the_ceiling_names_it() {
        let msg = MaxConnectionsError::AboveCeiling(9000).to_string();
        assert!(msg.contains("9000"), "{msg}");
        assert!(
            msg.contains(&MAX_SUPPORTED_CONNECTIONS.to_string()),
            "{msg}"
        );
    }

    /// Every supported cap satisfies the invariants the reader relies on
    /// (`for_max_connections` asserts them; this walks the whole range).
    #[test]
    fn every_supported_cap_holds_the_invariants() {
        for n in 1..=MAX_SUPPORTED_CONNECTIONS {
            let s = sizing(n);
            assert!(s.ring_entries().is_power_of_two());
            assert!(u64::from(s.ring_entries()) >= SQES_PER_CONNECTION * n + HOUSEKEEPING_SQES);
            assert!(u64::from(s.provided_buffers()) >= BUFFERS_PER_CONNECTION * n);
            assert!(u32::from(s.provided_buffers()) < s.ring_entries());
            assert!(s.ring_entries() <= MAX_RING_ENTRIES);
        }
    }

    #[test]
    fn the_gate_admits_up_to_the_cap() {
        assert!(!connection_cap_reached(0, 1));
        assert!(connection_cap_reached(1, 1));
        assert!(!connection_cap_reached(1023, 1024));
        assert!(connection_cap_reached(1024, 1024));
        assert!(!connection_cap_reached(
            MAX_SUPPORTED_CONNECTIONS - 1,
            MAX_SUPPORTED_CONNECTIONS
        ));
        assert!(connection_cap_reached(
            MAX_SUPPORTED_CONNECTIONS,
            MAX_SUPPORTED_CONNECTIONS
        ));
    }

    #[test]
    fn enomem_names_the_locked_memory_limit() {
        let err = ring_setup_error(
            "uring-reader",
            4096,
            io::Error::from_raw_os_error(libc::ENOMEM),
        );
        let msg = err.to_string();
        assert!(msg.contains("RLIMIT_MEMLOCK"), "{msg}");
        assert!(msg.contains("LimitMEMLOCK="), "{msg}");
        assert!(msg.contains("uring-reader"), "{msg}");
        assert_eq!(err.kind(), io::ErrorKind::OutOfMemory);
    }

    #[test]
    fn other_setup_errors_do_not_blame_the_limit() {
        let msg =
            ring_setup_error("response", 64, io::Error::from_raw_os_error(libc::EPERM)).to_string();
        assert!(!msg.contains("RLIMIT_MEMLOCK"), "{msg}");
        assert!(msg.contains("response"), "{msg}");
    }

    #[test]
    fn a_worker_that_dies_before_reporting_is_an_error() {
        let (tx, rx) = std::sync::mpsc::sync_channel::<io::Result<()>>(1);
        drop(tx);
        let err = await_startup("response", &rx).expect_err("no report");
        assert!(err.to_string().contains("response"));
    }
}
