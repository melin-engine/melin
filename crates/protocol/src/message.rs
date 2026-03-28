//! Wire message types for the trading protocol.
//!
//! Includes both trading operations (submit/cancel) and administrative
//! commands (add instrument, deposit, set risk limits). Administrative
//! commands require `Permission::Operator` and are gated on the reader thread.

use melin_engine::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule, InstrumentSpec,
    Order, OrderId, Price, Quantity, RiskLimits, Symbol,
};

/// Connection identifier assigned by the server.
///
/// Uses `u64` — monotonically increasing, never reused within a server
/// lifetime. Fits in a register and supports more connections than any
/// single server will ever handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

/// Client → server request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Request {
    // --- Trading operations (Admin + Trader) ---
    /// Submit an order for matching.
    SubmitOrder { symbol: Symbol, order: Order },
    /// Cancel a resting or pending stop order.
    CancelOrder {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
    },
    /// Cancel all resting orders and pending stops for an account
    /// across all instruments (kill switch).
    CancelAll { account: AccountId },
    /// Atomically amend a resting limit order's price and/or quantity.
    /// If the amendment fails (e.g. insufficient balance), the original
    /// order remains intact. `new_quantity` is the desired new remaining.
    CancelReplace {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    },

    // --- Administrative operations (Admin only) ---
    /// Register a new instrument with its base/quote currency pair.
    AddInstrument { spec: InstrumentSpec },
    /// Credit funds to an account. Used for initial seeding and
    /// operational adjustments.
    Deposit {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    /// Debit available funds from an account. Rejects if the account
    /// has resting orders (must CancelAll first) or insufficient balance.
    /// Removes the balance entry when it reaches zero.
    Withdraw {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    /// Set or update fat-finger risk limits for an instrument.
    /// `None` fields clear the corresponding limit.
    SetRiskLimits { symbol: Symbol, limits: RiskLimits },
    /// Configure circuit breakers for an instrument: price bands
    /// and/or trading halt. Replaces the current configuration.
    SetCircuitBreaker {
        symbol: Symbol,
        config: CircuitBreakerConfig,
    },
    /// Set maker/taker fee schedule for an instrument.
    SetFeeSchedule {
        symbol: Symbol,
        schedule: FeeSchedule,
    },

    /// Cancel all resting orders and pending stops with `TimeInForce::Day`
    /// across all instruments. Triggered by an operator at end-of-session.
    EndOfDay,

    /// Expire all resting orders and pending stops with `TimeInForce::GTD`
    /// whose `expiry_ns` <= `timestamp_ns`. Triggered by an operator.
    ExpireOrders { timestamp_ns: u64 },

    /// Disable an instrument: reject new orders and cancel all resting
    /// orders and pending stops. Re-enable is possible.
    DisableInstrument { symbol: Symbol },
    /// Re-enable a previously disabled instrument for trading.
    EnableInstrument { symbol: Symbol },
    /// Permanently remove a disabled instrument. Only succeeds if the
    /// instrument is disabled and has no resting orders.
    RemoveInstrument { symbol: Symbol },

    // --- Query operations (Admin only) ---
    /// Request a snapshot of server stats (connections, throughput, book
    /// depth, balances). Tag-only, no payload. Flows through the pipeline
    /// like any other request so the matching stage can read Exchange state
    /// without concurrency issues.
    QueryStats,

    // --- Control messages (all permission levels) ---
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

impl Request {
    /// Whether this request requires `Permission::Operator`.
    /// Deposit and Withdraw are excluded — they require `can_manage_funds`
    /// instead, which is satisfied by Custodian only.
    pub fn requires_operator(&self) -> bool {
        matches!(
            self,
            Request::AddInstrument { .. }
                | Request::SetRiskLimits { .. }
                | Request::SetCircuitBreaker { .. }
                | Request::SetFeeSchedule { .. }
                | Request::EndOfDay
                | Request::ExpireOrders { .. }
                | Request::DisableInstrument { .. }
                | Request::EnableInstrument { .. }
                | Request::RemoveInstrument { .. }
                | Request::QueryStats
        )
    }

    /// Whether this request is a fund management operation (deposit/withdraw).
    /// Requires `Permission::Custodian`.
    pub fn is_fund_management(&self) -> bool {
        matches!(self, Request::Deposit { .. } | Request::Withdraw { .. })
    }
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

    /// The server's input pipeline is full. The client should retry
    /// after a brief backoff. Sent directly by the reader thread
    /// without entering the pipeline. Tag-only, no payload.
    ServerBusy,

    // --- Stats response ---
    /// Server stats snapshot. Sent in response to `QueryStats`.
    StatsHeader {
        /// Number of currently authenticated connections.
        active_connections: u64,
        /// Total events processed by the matching engine since startup.
        events_processed: u64,
        /// Current journal sequence number (last durable event).
        journal_sequence: u64,
    },
}
