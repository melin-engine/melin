//! Wire-side request decoder seam.
//!
//! The server runtime (accept loop, frame reader, DPDK transport)
//! consumes incoming frames from the network and needs to turn them
//! into application events to publish to the pipeline. The decoding
//! itself — pattern-matching on the application's tags, mapping
//! per-variant fields, enforcing per-connection permission policy — is
//! application-shaped: a trading server decodes order submissions, a
//! payments server decodes transfers, a logistics server decodes
//! shipment events. This trait is the seam that lets the runtime
//! delegate that decoding to the application without ever naming the
//! concrete wire enum.
//!
//! The frame around the request is not the application's. Every client
//! request is `[request_seq: u64 LE][tag: u8][body]`, and the runtime
//! reads that header itself: it drops a frame too short to carry it and a
//! frame whose tag is in the protocol's reserved range (below `0x10`),
//! and hands the decoder only the tag and the body. An application's
//! tags therefore start at `0x10`.
//!
//! The runtime calls
//! [`RequestDecoder::decode`](crate::decoder::RequestDecoder::decode) once
//! per such frame; the [`Decoded`](crate::decoder::Decoded) return value
//! encodes exactly the four outcomes the runtime acts on (drop, publish,
//! reject with reason, log decode error).

use crate::AppEvent;
use crate::auth::Permission;

/// Decode an authenticated client request into an application event the
/// runtime can publish to the pipeline.
///
/// Stateless on the connection (the runtime carries connection-level
/// state — `Permission`, `key_hash`, the request sequence — and feeds
/// the relevant piece in per call). Implementors are typically
/// zero-sized types.
pub trait RequestDecoder: Send + Sync {
    /// Application event type produced on a successful decode. The
    /// runtime wraps this in a transport-level envelope (e.g.
    /// `JournalEvent::App`) before publishing.
    type Event: AppEvent;

    /// Decode one request. `tag` is the request's tag, never in the
    /// protocol's reserved range; `body` is everything after it, up to
    /// the end of the frame. `permission` is the role established during
    /// the auth handshake and stored on the connection.
    fn decode(&self, tag: u8, body: &[u8], permission: Permission) -> Decoded<Self::Event>;
}

/// Outcome of a single [`RequestDecoder::decode`] call. The runtime
/// branches on this and never needs to know the underlying wire enum.
pub enum Decoded<E: AppEvent> {
    /// Drop the request silently. For application messages that are not
    /// events — a subscription request arriving on the order-entry
    /// connection, say. The protocol's own frames never reach the
    /// decoder.
    Filter,
    /// Request OK and authorized. The runtime publishes the event under
    /// the request's sequence. Whether the event needs a timestamp is
    /// derived by the runtime from [`AppEvent::is_query`] — query events
    /// bypass the journal and skip the wall-clock stamp.
    Permitted(E),
    /// Authenticated connection lacks the permission level for this
    /// operation. The static string is logged at debug level on the
    /// reader thread; the runtime drops the request.
    PermissionDenied(&'static str),
    /// Decode failure (unknown tag, malformed body, invalid field). The
    /// runtime logs at debug level and drops the request; the connection
    /// is not closed (a misbehaving client drops itself on the next read
    /// timeout).
    DecodeError(&'static str),
}
