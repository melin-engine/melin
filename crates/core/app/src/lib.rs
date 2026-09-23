//! Application abstraction for the Melin durable transport.
//!
//! The transport (journal, replication, pipeline, snapshot framing) is generic
//! over an [`Application`]: a state machine that defines the semantics of the
//! events the transport persists, replicates, and dispatches. This crate
//! holds only the trait definitions and small transport-shared types — no
//! matching logic, no wire codec, no I/O.
//!
//! Split rationale: the transport is the reusable, commercial core, and
//! applications (any deterministic state machine, down to a no-op
//! benchmark) plug in. Keeping
//! trait definitions in their own crate means an app can depend on the
//! abstraction without pulling transport internals.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

/// Linux `sched_setaffinity` + `SCHED_FIFO` helpers used by every
/// pipeline thread in the transport (journal, matching, response,
/// shadow, replication sender/receiver). Generic OS plumbing — the
/// application never references it directly.
pub mod affinity;
/// Clock-read amortization for busy-spin loops. The shadow stage,
/// replication sender, and replica receiver all hit this on the hot
/// path. Pure timing utility — no transport coupling.
pub mod amortized_timer;
/// Connection-level permission model — application-shaped access
/// control (operator / trader / custodian / read-only / replication).
/// Consumed by the wire-side auth handshake in `melin-protocol::auth`.
pub mod auth;
/// Request-decoder seam: `RequestDecoder` + `Decoded<E>`. Lets the
/// server runtime turn wire frames into application events without
/// naming the concrete wire enum. See [`decoder`] for the trait shape
/// and the four outcomes the runtime branches on.
pub mod decoder;
/// Response-encoder seam: `ResponseEncoder`. Mirror of [`decoder`]
/// on the outbound path — application reports / query responses get
/// encoded to wire bytes through this trait, while transport-shaped
/// envelope variants (`BatchEnd`, `EngineError`) stay in runtime.
pub mod encoder;

use std::io::{self, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

/// Wall-clock nanoseconds since the Unix epoch. Used for the informational
/// `timestamp_ns` field stamped into journal records and replication frames,
/// and for any context that needs a real-world timestamp.
///
/// Not monotonic — subject to NTP adjustments. Never use this for ordering;
/// sequence numbers handle that.
///
/// The `u128 as u64` truncation is safe: u64 nanos covers ~584 years from
/// epoch (until 2554). Falls back to 0 if system clock is before epoch.
#[inline]
pub fn unix_epoch_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

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
/// (a halted pipeline). Rejections the application decides on — a
/// duplicate request, an invalid operation — are modelled inside its own
/// [`Application::Report`] type and do not appear here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// Replication is configured but no replica is currently connected;
    /// the transport refuses state-mutating events to preserve the
    /// persist-before-ack invariant. A refused event is not journaled.
    ///
    /// The other way a node stops taking writes — superseded by a
    /// higher-epoch primary — has no reason code: such a node is stopping
    /// and closes its client connections instead of answering, and the
    /// client reconnects to the new primary.
    ReplicaDisconnected,
}

/// Wire-sequence space: the monotonic sequence the journal allocates per
/// durable event. Comparable across nodes and stable across recovery (a
/// fresh vs recovered primary), unlike disruptor ring positions which reset
/// every process start — the newtype exists so the two spaces cannot be
/// mixed (see `melin-transport-core`'s `cursors` module, which re-exports
/// this and defines the sibling spaces). Defined here, in the trait crate,
/// so [`QueryCtx`] can carry it across the application boundary.
///
/// A position, not a count; subtract two of them with
/// [`WireSeq::saturating_sub`] to get a lag.
///
/// `#[repr(transparent)]` so it is layout-identical to `u64` and can be a
/// field of `#[repr(C)]` structs without changing their layout.
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, Debug)]
pub struct WireSeq(u64);

impl WireSeq {
    #[inline]
    pub const fn new(seq: u64) -> Self {
        Self(seq)
    }

    /// Unwrap to the raw `u64` — used only at the wire-encode / display
    /// boundaries where the value leaves the type system.
    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Lag between two wire-seq positions, saturating at zero. Returns a raw
    /// `u64` because a lag is a count, not a position.
    #[inline]
    pub const fn saturating_sub(self, earlier: WireSeq) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

/// What a journaled event is applied under, beside the event itself.
///
/// Every field is journaled with the event, so replay, a replica and the
/// shadow stage hand [`Application::apply`] the same values the primary
/// did: an application may derive state from any of them. Node-local
/// facts — connection counts, durability progress — are deliberately
/// absent; they reach [`Application::query`] through [`QueryCtx`] instead.
///
/// Layout: plain `Copy` struct, eight-byte aligned fields. Zero-cost to pass
/// by `&ApplyCtx` on the hot path.
#[derive(Debug, Clone, Copy)]
pub struct ApplyCtx {
    /// Wall-clock time the primary stamped on this event when it read it
    /// from the client, in nanoseconds since the Unix epoch. Journaled
    /// with the event, so the primary, every replica and every replay see
    /// the same value.
    ///
    /// Not monotonic from one event to the next, and not unique. Events
    /// read together share one stamp; a rare race between the node's
    /// producers can sequence an event slightly out of stamp order; and a
    /// clock step on
    /// the primary, or a failover to a node whose clock is behind, moves
    /// it backwards. Order is the sequence, never this. An application
    /// that reports or attests to this time should not promise its
    /// readers that it increases.
    pub now_ns: u64,
    /// FxHash of the public key that authenticated the connection that
    /// submitted this event. `0` for events the node journals on its own
    /// behalf, which carry no client identity. Journaled with the event,
    /// so replay hands `apply` the same value the live dispatch did. Lets
    /// the application keep per-key state (an idempotency sequence, a
    /// rate limit) without embedding identity in the event payload — the
    /// transport already knows it from the connection registration.
    pub key_hash: u64,
}

/// Transport state a query may report, beside the query event itself.
///
/// Passed to [`Application::query`], which answers from the application's
/// state without changing it. Unlike [`ApplyCtx`], these values are the
/// node's own, read when the query runs, and appear in no journal — which
/// is why only a query, whose answer is never replayed, may see them.
#[derive(Debug, Clone, Copy)]
pub struct QueryCtx {
    /// Journal sequence of the last event durably persisted (same value as
    /// the health endpoint's `journal_seq` gauge — survives recovery and
    /// does not count queries). Advances on every fsynced batch;
    /// batch-stale by up to one matching batch.
    pub journal_sequence: WireSeq,
    /// Count of client connections currently attached to this server.
    pub active_connections: u64,
    /// Monotonic count of events the matching stage has processed since
    /// this process started.
    pub events_processed: u64,
    /// FxHash of the public key that authenticated the connection asking.
    /// Lets a self-introspecting query ("what is my own state?") look up
    /// per-key state without embedding identity in the event — the
    /// transport already knows it from the connection.
    pub key_hash: u64,
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
    /// Upper bound on [`encoded_size`](AppEvent::encoded_size) across
    /// every value of this type.
    ///
    /// The journal reserves this much per entry, and it is what decides
    /// how many events fit in one fsync batch — the transport divides the
    /// hand-off chunk by it, so a wider event yields a shorter batch
    /// rather than a larger allocation.
    ///
    /// This is a *bound*, not a measurement: it must be at least the
    /// largest `encoded_size` this type can return. Declaring it too
    /// small is a bug the journal cannot paper over — every reservation
    /// downstream is computed from this number — so it is checked at
    /// compile time against the journal's entry ceiling, and at encode
    /// time against each event's actual `encoded_size`, which is refused
    /// if it exceeds what was declared.
    ///
    /// The compile-time check fires when the journal is instantiated for
    /// this type, so it surfaces on `cargo build` and `cargo test`, not on
    /// `cargo check` — a check-only CI or rust-analyzer will stay green
    /// on a bound the journal cannot carry.
    ///
    /// Deliberately has no default. The right value is a property of the
    /// implementor's wire format and nothing else can infer it; a default
    /// would silently hand a wrong bound to exactly the application that
    /// most needed to think about it.
    const MAX_ENCODED_SIZE: usize;

    /// Number of bytes [`AppEvent::encode`] will write for this value.
    ///
    /// The transport uses this to allocate a single batch buffer and to
    /// compute the per-entry length prefix. Must be exact, not an upper
    /// bound — see [`MAX_ENCODED_SIZE`](AppEvent::MAX_ENCODED_SIZE) for
    /// the type-level bound.
    fn encoded_size(&self) -> usize;

    /// Encode this event into `buf`. Caller guarantees `buf.len() >=
    /// self.encoded_size()`. Returns the number of bytes written, which
    /// must equal `self.encoded_size()`: the journal refuses an event
    /// whose two figures disagree rather than persist it.
    fn encode(&self, buf: &mut [u8]) -> usize;

    /// Decode an event from `buf`. `buf` contains exactly one encoded
    /// event (no trailing bytes); the transport has already stripped the
    /// framing.
    fn decode(buf: &[u8]) -> Result<Self, CodecError>;

    /// Query events bypass the journal (no state change, no durability
    /// requirement) but still flow through the matching stage, which hands
    /// them to [`Application::query`] rather than
    /// [`Application::apply`] so the app can answer from its in-memory
    /// state. All other events are journaled.
    fn is_query(&self) -> bool;
}

/// The [`Application::QueryResponse`] of an application that answers no
/// queries.
///
/// It has no values, so [`Application::query`] can only return `None`,
/// and a response encoder's `encode_query` is `match *query {}`: the
/// compiler proves the arm unreachable, where a comment could only claim
/// it. That is why it is an empty enum rather than `()`: `()` has a
/// value, so code handling it has to invent an answer, or an error, for
/// a case that cannot occur.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoQuery {}

/// An application driven by the Melin durable transport.
///
/// The transport feeds events into [`apply`](Application::apply) in a
/// single-threaded, deterministic order matching the journal. Snapshots
/// and journal replay guarantee that re-running the same stream of
/// `(event, ApplyCtx)` pairs against a freshly [`restore`](Application::restore)-d
/// instance produces byte-identical state.
///
/// Queries take the other door: [`query`](Application::query), which
/// borrows the application immutably. A query is never journaled, so a
/// query that changed state would change it on one node and nowhere
/// else; the signature makes that impossible rather than a rule to keep.
///
/// Implementors should keep [`apply`](Application::apply) free of
/// allocation and I/O. Reports are pushed into the caller-provided buffer,
/// reused across calls on the hot path.
///
/// # Genesis state
///
/// [`Default`] is the state before the first event, and every history
/// replays from it: a fresh node, a replica catching up from sequence 1,
/// a restart recovering a journal with no snapshot. It must therefore
/// depend on nothing local to the node — no flags, no environment. What
/// an operator configures (rate limits, caps, sizing) reaches the
/// application as journaled events instead, so that replay reproduces
/// the decisions made under it and every replica holds the primary's
/// values. `Default` may still pre-allocate: capacity is not state.
///
/// # Sizing
///
/// Capacity, unlike state, may come from the node: how many entities
/// to reserve room for, how large an index to expect. That
/// reaches the application through [`Sizing`](Application::Sizing) and
/// [`prefault`](Application::prefault) only — a hook that runs after
/// the state exists, on every node, and whose contract is to touch and
/// reserve memory, never to change what the state means. `Default`
/// stays the small, parameterless genesis that unit tests build by the
/// thousand; production sizing is applied on top of whatever state the
/// node starts from, a fresh genesis or a restored snapshot alike.
pub trait Application: Sized + Default {
    /// The application-defined event type. One variant per business
    /// operation, plus any queries.
    type Event: AppEvent;

    /// What the node's operator tells the application about the
    /// workload to size for, passed to [`prefault`](Application::prefault)
    /// on every node. Capacity only: nothing in it may influence what
    /// [`apply`](Application::apply) decides, or replicas started with
    /// other values would diverge from the primary. `()` for an
    /// application with nothing to reserve.
    ///
    /// `Send + Sync + 'static` because the runtime keeps it for the
    /// life of the process: a replica sizes every instance it builds,
    /// including one rebuilt after a resync.
    type Sizing: Send + Sync + 'static;

    /// Per-event output payloads. One input event may produce many
    /// reports (an acknowledgement and the effects it caused, say).
    /// `Copy` keeps the output ring buffer allocation-free.
    type Report: Copy;

    /// 1:1 query responses returned by [`query`](Self::query). Routed
    /// through `OutputPayload::QueryResponse` on the output ring.
    ///
    /// [`NoQuery`] for an application that answers none.
    ///
    /// Separated from `Report` so that large query payloads (e.g. a
    /// summary of many entries) don't inflate the per-element size of the
    /// scratch vec on the hot path.
    type QueryResponse: Copy;

    /// Apply a single journaled event to the application state. Must be
    /// deterministic given `(self, event, ctx)`: replay depends on it.
    ///
    /// The runtime never passes a query here (see
    /// [`AppEvent::is_query`]); a match arm for a query variant can do
    /// nothing. Every field of `ctx` is journaled with the event, so the
    /// implementation may derive state from any of them.
    ///
    /// Reports go into `out`; the client that submitted the event gets
    /// them as its reply batch, in order.
    fn apply(&mut self, event: Self::Event, ctx: &ApplyCtx, out: &mut Vec<Self::Report>);

    /// Answer a query from the application's current state, without
    /// changing it. Called on the node serving the client only — never on
    /// replay, a replica, or the shadow stage — and not journaled.
    ///
    /// The runtime passes only events whose [`AppEvent::is_query`] is
    /// true. `None` is the answer for any other event, so an
    /// implementation needs no unreachable arm; the client then gets an
    /// empty reply batch.
    ///
    /// Default: `None` for every event, right for an application with no
    /// queries, whose `is_query` is never true.
    fn query(&self, _event: Self::Event, _ctx: &QueryCtx) -> Option<Self::QueryResponse> {
        None
    }

    /// Advance the application's wall-clock without applying a business
    /// event, to fire whatever time-driven work has come due (expiries,
    /// session transitions). Reports go into `out`, as from `apply`.
    ///
    /// The transport calls it before [`apply`](Application::apply)
    /// whenever an event's timestamp is past the latest time it has
    /// handed the application, and for each journaled clock tick, which
    /// keeps time moving while no client traffic arrives. Live, on
    /// replay and on a replica, the calls follow the same rules from the
    /// same journaled times.
    ///
    /// `now_ns` is wall-clock time, so do not assume it strictly
    /// increases: the same value may arrive more than once, and a tick
    /// may carry a time earlier than one already seen — after the
    /// primary's clock steps back, or after a failover to a node whose
    /// clock runs behind. In that case a node that restarted may also
    /// make calls a node that kept running did not, since the latest
    /// time handed out is not carried across a restart. So a call for a
    /// time already passed must change nothing — no due work fires
    /// again, no state records the earlier time — and elapsed-time
    /// arithmetic must saturate.
    ///
    /// Default: nothing, right for an application with no time-driven
    /// work. Under load it runs ahead of nearly every event, so an
    /// override should make "nothing is due" a cheap check.
    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<Self::Report>) {}

    /// Synthesise a rejection report for a transport-originated reject.
    /// No access to `&self` — the reject must be constructible from the
    /// event alone (plus the transport's reason).
    ///
    /// Today the one reason is a halted node
    /// ([`ReplicaDisconnected`](RejectReason::ReplicaDisconnected)): it
    /// rejects on the thread that reads client requests, before the event
    /// is sequenced, so the event is never journaled and never reaches
    /// [`apply`](Application::apply).
    fn build_reject(event: &Self::Event, reason: RejectReason) -> Self::Report;

    /// Serialise the application's live state into `w`. The transport
    /// wraps `w` with its own framing (magic, version, CRC); the app
    /// writes only its payload. Must pair with [`restore`](Application::restore)
    /// to produce bit-identical state.
    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()>;

    /// Reconstruct application state from a snapshot produced by
    /// [`snapshot`](Application::snapshot). `r` yields exactly the bytes
    /// that `snapshot` wrote — the transport has already stripped its
    /// framing. It must read all of them: bytes left unread mean the two
    /// disagree about the layout, and the transport refuses the snapshot
    /// rather than run on state that is not the one saved.
    fn restore<R: Read>(r: &mut R) -> io::Result<Self>;

    /// Schema version for the application's snapshot payload. Bumped
    /// whenever [`snapshot`](Application::snapshot)'s byte layout
    /// changes. The transport stores this alongside its own framing
    /// version so operators can detect incompatible upgrades.
    const APP_VERSION: u16;

    /// Reserve and pre-fault the application's memory for the workload
    /// `sizing` describes, so that growth and first-touch page faults
    /// happen here and not on the hot path.
    ///
    /// Called on a genesis instance before a history is applied to it —
    /// a journal replayed on a primary or a replica, a primary's stream
    /// on a replica — and again before the pipeline takes an instance:
    /// on a primary at boot, on a replica before its pipeline starts
    /// (including one rebuilt after a resync), and once more when a
    /// replica is promoted. A snapshot has no genesis instance, so a
    /// restored one is sized only after. The instance may therefore hold
    /// state already, and the contract is that the call changes capacity
    /// only: every entry survives, every decision `apply` would make
    /// afterwards is the same, and a second call with the same sizing is
    /// a no-op. How the capacity gets there is the implementation's
    /// choice — a collection with no in-place reserve may be rebuilt at
    /// the larger size, entries and all. A rebuild may change iteration
    /// order, and with it the bytes a snapshot of the same state
    /// produces; that is fine, a snapshot is per node and nothing
    /// compares bytes across nodes.
    /// Default: no-op. An application that pre-allocates large indices
    /// or slab backing stores should override it to size them and touch
    /// every page.
    fn prefault(&mut self, _sizing: &Self::Sizing) {}

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
