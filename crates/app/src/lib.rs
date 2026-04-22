//! Application abstraction for the Melin durable transport.
//!
//! The transport (journal, replication, pipeline, snapshot framing) is generic
//! over an [`Application`]: a state machine that defines the semantics of the
//! events the transport persists, replicates, and dispatches. This crate
//! holds only the trait definitions and small transport-shared types — no
//! matching logic, no wire codec, no I/O.
//!
//! Split rationale: the transport is the reusable, commercial core; apps
//! (trading engines, bespoke matchers, no-op benchmarks) plug in. Keeping
//! trait definitions in their own crate means an app can depend on the
//! abstraction without pulling transport internals.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

use std::io::{self, Read, Write};

/// Codec failures surfaced by [`AppEvent::decode`]. Kept deliberately small:
/// the transport only needs to distinguish "malformed tag", "truncated
/// buffer", and "invalid field value" to decide whether to abort replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    /// Encountered a variant tag not recognised by this build.
    UnknownTag(u8),
    /// Encoded length shorter than required for the declared variant.
    Truncated,
    /// A field violated an invariant (e.g. `NonZeroU64` observed as 0).
    InvalidField,
}

impl core::fmt::Display for CodecError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            CodecError::UnknownTag(t) => write!(f, "unknown event tag {t:#x}"),
            CodecError::Truncated => f.write_str("truncated event buffer"),
            CodecError::InvalidField => f.write_str("invalid event field"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Transport-originated rejection reasons. These are the rejections the
/// transport itself synthesises before an event reaches the application
/// (duplicate request on the dedup path, halted pipeline). App-originated
/// rejections (insufficient balance, risk limits, unknown symbol) are
/// modelled inside the app's own [`Application::Report`] type and do not
/// appear here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Per-key request sequence was not strictly greater than the
    /// recorded high-water mark for this authentication key.
    DuplicateRequest,
    /// Replication is configured but no replica is currently connected;
    /// the transport refuses state-mutating events to preserve the
    /// persist-before-ack invariant.
    ReplicaDisconnected,
}

/// Transport state observable by the application during event application.
///
/// Passed by reference into [`Application::apply`] so the app can synthesise
/// query responses (stats snapshots, health-style reports) that reference
/// counters the transport owns. The transport never pattern-matches on app
/// event variants — all such concerns live on the app side, reading from
/// this context.
///
/// Layout: plain `Copy` struct, eight-byte aligned fields. Zero-cost to pass
/// by `&ApplyCtx` on the hot path.
#[derive(Debug, Clone, Copy)]
pub struct ApplyCtx {
    /// Wall-clock time at which the transport dispatched this event, in
    /// nanoseconds since the Unix epoch. Identical across primary and
    /// replica for deterministic replay.
    pub now_ns: u64,
    /// Journal sequence number of the last event durably persisted.
    /// Advances on every fsynced batch.
    pub journal_sequence: u64,
    /// Count of client connections currently attached to this server.
    pub active_connections: u64,
    /// Monotonic count of events the matching stage has applied since
    /// this process started (includes the event currently being applied).
    pub events_processed: u64,
}

/// An application event that can be round-tripped through the journal.
///
/// Implementors are responsible for their own wire format. The transport
/// frames each encoded event with a length prefix and a transport tag, so
/// implementations encode only the *payload* and must round-trip exactly.
///
/// `Copy` is required so events can live inside the disruptor ring slots
/// without heap indirection — the disruptor publishes by byte-copy.
pub trait AppEvent: Copy {
    /// Number of bytes [`AppEvent::encode`] will write for this value.
    ///
    /// The transport uses this to allocate a single batch buffer and to
    /// compute the per-entry length prefix. Must be exact, not an upper
    /// bound.
    fn encoded_size(&self) -> usize;

    /// Encode this event into `buf`. Caller guarantees `buf.len() >=
    /// self.encoded_size()`. Returns the number of bytes written, which
    /// must equal `self.encoded_size()`.
    fn encode(&self, buf: &mut [u8]) -> usize;

    /// Decode an event from `buf`. `buf` contains exactly one encoded
    /// event (no trailing bytes); the transport has already stripped the
    /// framing.
    fn decode(buf: &[u8]) -> Result<Self, CodecError>;

    /// Read-only query events bypass the journal (no state change, no
    /// durability requirement) but still flow through the matching stage
    /// so the app can publish a synchronous response from its in-memory
    /// state. All other events are journaled.
    fn is_query(&self) -> bool;
}

/// Wire encoder for application reports.
///
/// Declared here so the transport's response stage can be generic over
/// `R: EncodeReport` without depending on any concrete protocol. Defined in
/// this crate rather than bound on [`Application::Report`] today — the
/// bound is only required at the response-stage integration in Phase 3.
pub trait EncodeReport: Copy {
    /// Exact number of bytes [`EncodeReport::encode`] will write.
    fn encoded_size(&self) -> usize;

    /// Encode into `buf`. Caller guarantees capacity.
    fn encode(&self, buf: &mut [u8]) -> usize;
}

/// An application driven by the Melin durable transport.
///
/// The transport feeds events into [`apply`](Application::apply) in a
/// single-threaded, deterministic order matching the journal. Snapshots
/// and journal replay guarantee that re-running the same stream of
/// `(event, ApplyCtx)` pairs against a freshly [`restore`](Application::restore)-d
/// instance produces byte-identical state.
///
/// Implementors should keep [`apply`](Application::apply) free of
/// allocation and I/O. Reports are pushed into the caller-provided buffer,
/// reused across calls on the hot path.
pub trait Application: Sized {
    /// The application-defined event type. One variant per business
    /// operation (submit order, cancel, deposit, …).
    type Event: AppEvent;

    /// Per-event output payloads. One input event may produce many
    /// reports (fills, acks, query rows). `Copy` keeps the output ring
    /// buffer allocation-free.
    type Report: Copy;

    /// 1:1 query responses returned directly from [`apply`](Self::apply),
    /// bypassing the fan-out scratch `Vec`. Routed through
    /// `OutputPayload::QueryResponse` on the output ring.
    ///
    /// Separated from `Report` so that large query payloads (e.g. a
    /// balance snapshot) don't inflate the per-element size of the
    /// scratch vec on the hot path.
    type QueryResponse: Copy;

    /// Apply a single event to the application state. Must be
    /// deterministic given `(self, event, ctx)`: replay depends on it.
    ///
    /// The implementation is free to read any field of `ctx`; the
    /// transport guarantees those fields reflect its live state at
    /// dispatch time.
    ///
    /// Fan-out reports (fills, acks, cancels) go into `out`. Query
    /// responses that are always 1:1 with the input event (e.g.
    /// position snapshots, stats) should be returned directly — the
    /// transport writes them to the output ring without touching the
    /// scratch vec, keeping the per-element size of `out` small.
    fn apply(
        &mut self,
        event: Self::Event,
        ctx: &ApplyCtx,
        out: &mut Vec<Self::Report>,
    ) -> Option<Self::QueryResponse>;

    /// Advance the application's wall-clock without applying a business
    /// event. The transport calls [`tick`](Application::tick) once per
    /// dispatched slot, before [`apply`](Application::apply), to fire
    /// time-driven tasks (expiries, session transitions) with
    /// monotonically increasing `now_ns`.
    fn tick(&mut self, now_ns: u64, out: &mut Vec<Self::Report>);

    /// Per-key idempotency gate. Returns `true` if `seq` is strictly
    /// greater than the previously seen sequence for `key_hash` (and
    /// the high-water mark has been advanced), `false` on a duplicate.
    ///
    /// Transport-owned data eventually; in Phase 1 the trading app
    /// implements this against the existing `Exchange` HWM map. Phase 3
    /// moves the map into the transport and this method becomes a
    /// default impl.
    fn check_request_seq(&mut self, key_hash: u64, seq: u64) -> bool;

    /// Synthesise a rejection report for a transport-originated reject.
    /// Called by the transport before `apply` has observed the event.
    /// No access to `&self` — the reject must be constructible from the
    /// event alone (plus the transport's reason).
    fn build_reject(event: &Self::Event, reason: RejectReason) -> Self::Report;

    /// Serialise the application's live state into `w`. The transport
    /// wraps `w` with its own framing (magic, version, CRC); the app
    /// writes only its payload. Must pair with [`restore`](Application::restore)
    /// to produce bit-identical state.
    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()>;

    /// Reconstruct application state from a snapshot produced by
    /// [`snapshot`](Application::snapshot). `r` yields exactly the bytes
    /// that `snapshot` wrote — the transport has already stripped its
    /// framing.
    fn restore<R: Read>(r: &mut R) -> io::Result<Self>;

    /// Schema version for the application's snapshot payload. Bumped
    /// whenever [`snapshot`](Application::snapshot)'s byte layout
    /// changes. The transport stores this alongside its own framing
    /// version so operators can detect incompatible upgrades.
    const APP_VERSION: u16;

    /// Pre-fault any application memory that would otherwise soft-fault
    /// on the first hot-path access. Called once on startup before the
    /// matching stage takes the input ring. Default: no-op — apps that
    /// pre-allocate large indices / slab backing stores (`Exchange`
    /// does) should override to touch every page.
    fn prefault(&mut self) {}

    /// Return a byte-identical clone of the application by round-trip
    /// through [`snapshot`](Application::snapshot) +
    /// [`restore`](Application::restore). Used by the shadow-snapshot
    /// stage when an application is not `Clone`. The default
    /// implementation is correct for any app with a working snapshot
    /// codec; override only if a cheaper same-process clone is
    /// possible.
    fn clone_via_snapshot(&self) -> io::Result<Self> {
        let mut buf = Vec::new();
        self.snapshot(&mut buf)?;
        let mut cursor = std::io::Cursor::new(buf);
        Self::restore(&mut cursor)
    }
}
