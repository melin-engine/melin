//! Connection-level permission model for application access control.
//!
//! Lives in `melin-app` (next to [`Application`](crate::Application))
//! because the role taxonomy ("who can do what to my app") is an
//! application-shaped concept, not a wire-format concept. Wire-side
//! auth (challenge-response, authorized-keys file parsing, signing
//! payloads) lives in `melin-protocol::auth` and depends on this
//! enum.

/// Permission level assigned to an authenticated connection.
///
/// Five specialized roles with no overlap — separation of duties:
///   Operator: exchange configuration (instruments, risk, circuit breakers)
///   Trader: order submission and cancellation
///   Custodian: fund management (deposit/withdraw)
///   ReadOnly: observation only (heartbeats, future market data)
///   Replication: journal streaming between primary and replica servers
///
/// No single role has full access. An organization needing both trading
/// and admin uses separate keys for each role.
///
/// Checked on the reader thread (cold per-request check) with zero
/// cost on the matching engine hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// Exchange configuration: instrument management, circuit breakers,
    /// risk limits, fee schedules, end-of-day, stats. Cannot trade or
    /// manage funds.
    Operator,
    /// Submit/cancel orders and heartbeats. Cannot perform admin ops
    /// or fund management (deposit/withdraw).
    Trader,
    /// Deposit and withdraw only. Cannot trade or perform admin ops.
    /// Separates fund management from trading and exchange administration.
    Custodian,
    /// Heartbeats only. Future: market data subscriptions.
    ReadOnly,
    /// Replication only. Authorizes a replica to connect and receive
    /// journal streams. Cannot trade, manage funds, or configure the
    /// exchange. Infrastructure role, not client-facing.
    Replication,
}

impl Permission {
    /// Whether this permission level allows trading operations
    /// (submit order, cancel order, cancel all, cancel-replace).
    pub fn can_trade(self) -> bool {
        matches!(self, Permission::Trader)
    }

    /// Whether this permission level allows administrative operations
    /// (add instrument, set risk limits, circuit breakers, fee schedules,
    /// end-of-day, query stats).
    pub fn is_operator(self) -> bool {
        matches!(self, Permission::Operator)
    }

    /// Whether this permission level allows fund management operations
    /// (deposit, withdraw).
    pub fn can_manage_funds(self) -> bool {
        matches!(self, Permission::Custodian)
    }

    /// Whether this permission level authorizes replication connections
    /// (journal streaming between primary and replica).
    pub fn is_replication(self) -> bool {
        matches!(self, Permission::Replication)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_can_trade() {
        assert!(!Permission::Operator.can_trade());
        assert!(Permission::Trader.can_trade());
        assert!(!Permission::Custodian.can_trade());
        assert!(!Permission::ReadOnly.can_trade());
        assert!(!Permission::Replication.can_trade());
    }

    #[test]
    fn permission_is_operator() {
        assert!(Permission::Operator.is_operator());
        assert!(!Permission::Trader.is_operator());
        assert!(!Permission::Custodian.is_operator());
        assert!(!Permission::ReadOnly.is_operator());
        assert!(!Permission::Replication.is_operator());
    }

    #[test]
    fn permission_can_manage_funds() {
        assert!(!Permission::Operator.can_manage_funds());
        assert!(!Permission::Trader.can_manage_funds());
        assert!(Permission::Custodian.can_manage_funds());
        assert!(!Permission::ReadOnly.can_manage_funds());
        assert!(!Permission::Replication.can_manage_funds());
    }

    #[test]
    fn permission_is_replication() {
        assert!(!Permission::Operator.is_replication());
        assert!(!Permission::Trader.is_replication());
        assert!(!Permission::Custodian.is_replication());
        assert!(!Permission::ReadOnly.is_replication());
        assert!(Permission::Replication.is_replication());
    }
}
