//! Client library for connecting to the trading server.
//!
//! Provides a typed API over the binary wire protocol. The public
//! `Client` type speaks TCP via blocking I/O against the server's
//! TCP listener.

use std::io;

use melin_protocol::error::ProtocolError;

/// Error returned by client operations.
#[derive(Debug)]
pub enum ClientError {
    /// I/O error (connection lost, etc.).
    Io(io::Error),
    /// Protocol encoding/decoding error.
    Protocol(ProtocolError),
    /// Server closed the connection before sending BatchEnd.
    Disconnected,
    /// Server rejected the Ed25519 challenge-response authentication
    /// (unknown key, invalid signature, or wrong key permissions).
    AuthFailed,
    /// Server pipeline is full. The caller should retry after a brief backoff.
    ServerBusy,
    /// Operation didn't complete within the implementation's deadline.
    /// The TCP path uses blocking I/O and surfaces socket-level
    /// timeouts as `Io`, so this variant is currently unreachable —
    /// kept in the API for forward compatibility.
    Timeout,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::Disconnected => write!(f, "disconnected from server"),
            Self::AuthFailed => write!(f, "authentication failed"),
            Self::ServerBusy => write!(f, "server busy (pipeline full), retry after backoff"),
            Self::Timeout => write!(f, "operation timed out"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

/// Snapshot of server stats returned by [`Client::query_stats`].
#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub active_connections: u64,
    pub events_processed: u64,
    pub journal_sequence: u64,
}

mod tcp;
pub use tcp::Client;
