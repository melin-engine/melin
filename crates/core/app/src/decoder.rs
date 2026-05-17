//! Wire-side request decoder seam.
//!
//! The server runtime (accept loop, frame reader, DPDK transport)
//! consumes incoming frames from the network and needs to turn them
//! into application events to publish to the pipeline. The decoding
//! itself — pattern-matching on the wire enum, mapping per-variant
//! fields, enforcing per-connection permission policy — is
//! application-shaped: a trading server decodes order submissions, a
//! payments server decodes transfers, a logistics server decodes
//! shipment events. This trait is the seam that lets the runtime
//! delegate that decoding to the application without ever naming the
//! concrete wire enum.
//!
//! The runtime calls [`RequestDecoder::decode`] once per incoming
//! frame; the [`Decoded`] return value encodes exactly the four
//! outcomes the runtime acts on (drop, publish, reject with reason,
//! log decode error).

use crate::AppEvent;
use crate::auth::Permission;

/// Decode an authenticated client frame into an application event the
/// runtime can publish to the pipeline.
///
/// Stateless on the connection (the runtime carries connection-level
/// state — `Permission`, `key_hash`, etc. — and feeds the relevant
/// piece in per call). Implementors are typically zero-sized types.
pub trait RequestDecoder: Send + Sync {
    /// Application event type produced on a successful decode. The
    /// runtime wraps this in a transport-level envelope (e.g.
    /// `JournalEvent::App`) before publishing.
    type Event: AppEvent;

    /// Decode a wire frame. `bytes` is the framed payload (length
    /// prefix already stripped by the caller). `permission` is the
    /// role established during the auth handshake and stored on the
    /// connection.
    fn decode(&self, bytes: &[u8], permission: Permission) -> Decoded<Self::Event>;
}

/// Outcome of a single [`RequestDecoder::decode`] call. The runtime
/// branches on this and never needs to know the underlying wire enum.
pub enum Decoded<E: AppEvent> {
    /// Drop the frame silently. Used for transport-level messages
    /// (heartbeats, post-auth handshakes, subscription control) that
    /// the runtime never publishes to the pipeline.
    Filter,
    /// Frame OK and authorized. Caller publishes `event` with the
    /// per-key sequence `request_seq`. Whether the event needs a
    /// timestamp is derived by the runtime from [`AppEvent::is_query`]
    /// — query events bypass the journal and skip the wall-clock
    /// stamp.
    Permitted {
        /// Per-key idempotency sequence carried in the wire frame.
        request_seq: u64,
        /// Decoded application event.
        event: E,
    },
    /// Authenticated connection lacks the permission level for this
    /// operation. The static string is logged at debug level on the
    /// reader thread; the runtime drops the frame.
    PermissionDenied(&'static str),
    /// Wire-level decode failure (malformed length, unknown variant
    /// tag, invalid field). The runtime logs at debug level and drops
    /// the frame; the connection is not closed (a misbehaving client
    /// drops itself on the next read timeout).
    DecodeError(&'static str),
}
