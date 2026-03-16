//! Wire message types for the trading protocol.
//!
//! Only trading operations (submit/cancel) are exposed to clients.
//! Administrative operations (add instrument, deposit) are server-side
//! only — they'll be configured at startup or via a separate admin API.

use trading_engine::types::{AccountId, ExecutionReport, Order, OrderId, Symbol};

/// Connection identifier assigned by the server.
///
/// Uses `u64` — monotonically increasing, never reused within a server
/// lifetime. Fits in a register and supports more connections than any
/// single server will ever handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// Client → server request.
///
/// Limited to trading operations. Administrative actions (instrument
/// registration, deposits) are not client-facing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    /// Submit an order for matching.
    SubmitOrder { symbol: Symbol, order: Order },
    /// Cancel a resting or pending stop order.
    CancelOrder { symbol: Symbol, order_id: OrderId },
    /// Cancel all resting orders and pending stops for an account
    /// across all instruments (kill switch).
    CancelAll { account: AccountId },
    /// Keepalive heartbeat. Resets the server's idle timeout for this
    /// connection. Tag-only, no payload.
    Heartbeat,
    /// Challenge-response authentication. Sent after receiving a
    /// `Challenge` from the server. Contains the Ed25519 signature
    /// over the nonce and the client's public key.
    ChallengeResponse {
        /// Ed25519 signature of the server-provided nonce (64 bytes).
        signature: [u8; 64],
        /// Client's Ed25519 public key (32 bytes).
        public_key: [u8; 32],
    },
}

/// Server → client response payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseKind {
    /// An execution report from the matching engine.
    Report(ExecutionReport),
    /// The engine encountered an internal error processing the request.
    EngineError,
    /// Signals the end of a response batch for a single request.
    /// A single request (e.g., SubmitOrder) can produce multiple Reports
    /// (fills, placements, triggers). BatchEnd tells the client that all
    /// reports for this request have been sent.
    BatchEnd,
    /// Sent by the server immediately after accepting a connection.
    /// Signals that the pipeline is ready and the client may begin
    /// sending requests. Used for readiness synchronization in LAN
    /// benchmarks where the client can't observe server startup.
    ServerReady,
    /// Keepalive heartbeat sent during idle periods. Tag-only, no payload.
    Heartbeat,
    /// Challenge sent by the server after accepting a connection.
    /// Contains a 32-byte random nonce for the client to sign.
    Challenge {
        /// Random nonce (32 bytes) that the client must sign with its
        /// Ed25519 private key.
        nonce: [u8; 32],
    },
    /// Authentication failed — invalid signature, unknown key, or
    /// other auth error. Server drops the connection after sending this.
    AuthFailed,
}
