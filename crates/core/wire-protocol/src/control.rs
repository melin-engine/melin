//! Transport-level control types shared between the server runtime and
//! the application-specific protocol crate. These are domain-free:
//! they exist for connection management, auth handshakes, and pipeline
//! control — not for business messages.

/// Connection identifier assigned by the server.
///
/// `u64` — monotonically increasing, never reused within a server
/// lifetime. Fits in a register and supports more connections than any
/// single server will ever handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// Transport-level response frames that the server runtime encodes
/// directly, bypassing the application's `ResponseEncoder` trait.
///
/// These are the pipeline-control and auth-handshake messages that
/// every server sends regardless of the application it hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportResponse {
    /// Periodic keep-alive sent to idle connections.
    Heartbeat,
    /// Marks the end of a response batch for a single request, backed as
    /// the ack policy in force requires.
    BatchEnd,
    /// Marks the end of a response batch for a single request that only
    /// the primary's own disk backs, a weaker guarantee than the ack
    /// policy in force. Sent by a primary halted for want of a replica
    /// for a request it had sequenced before the halt (or a query that
    /// may reflect such requests), once its own journal holds it. A
    /// client that needs the policy's guarantee treats the request as
    /// unconfirmed and reconciles.
    BatchEndDegraded,
    /// The matching stage encountered an internal error processing
    /// the request. The client should not retry.
    EngineError,
    /// The server's accept queue is full; the client should back off
    /// and reconnect.
    ServerBusy,
    /// Auth handshake: server → client challenge carrying a 32-byte
    /// random nonce.
    Challenge { nonce: [u8; 32] },
    /// Auth handshake: server → client rejection.
    AuthFailed,
    /// Auth handshake: server → client success.
    ServerReady,
}

/// Auth handshake: client → server response to a [`TransportResponse::Challenge`].
///
/// Contains the Ed25519 signature over the nonce and the client's
/// public key for lookup in the authorized-keys table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChallengeResponse {
    pub signature: [u8; 64],
    pub public_key: [u8; 32],
}
