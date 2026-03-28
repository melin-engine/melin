//! Shared request processing logic used by all transport backends
//! (epoll reader, io_uring reader, DPDK transport).
//!
//! Extracting these functions avoids duplicating critical permission
//! enforcement and request→event conversion across transport impls.

use melin_engine::journal::event::JournalEvent;
use melin_protocol::auth::Permission;
use melin_protocol::message::Request;

/// Returns `true` if the request should be filtered out (not published
/// to the pipeline). Heartbeats and post-auth ChallengeResponse are
/// filtered.
pub fn should_filter(request: &Request) -> bool {
    matches!(
        request,
        Request::Heartbeat | Request::ChallengeResponse { .. }
    )
}

/// Check whether the given request is permitted for the connection's
/// permission level. Returns `Ok(())` if allowed, `Err(reason)` if not.
///
/// Permission model:
/// - Operator: exchange configuration (add instrument, risk limits, circuit breakers, fee schedules, end-of-day, stats)
/// - Custodian: fund management (deposit, withdraw)
/// - Trader: order submission, cancellation
/// - ReadOnly: heartbeats only (filtered before this function)
pub fn check_permission(request: &Request, permission: Permission) -> Result<(), &'static str> {
    if request.requires_operator() && !permission.is_operator() {
        return Err("non-operator attempted operator command");
    }
    if request.is_fund_management() && !permission.can_manage_funds() {
        return Err("non-custodian attempted fund management");
    }
    if !request.requires_operator() && !request.is_fund_management() && !permission.can_trade() {
        return Err("connection lacks trading permission");
    }
    Ok(())
}

/// Convert a wire `Request` to a `JournalEvent` for the pipeline.
///
/// The request must have been filtered via [`should_filter`] first —
/// this function panics on `Heartbeat` and `ChallengeResponse`.
pub fn to_event(request: &Request) -> JournalEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => JournalEvent::SubmitOrder { symbol, order },
        Request::CancelOrder {
            symbol,
            account,
            order_id,
        } => JournalEvent::CancelOrder {
            symbol,
            account,
            order_id,
        },
        Request::CancelAll { account } => JournalEvent::CancelAll { account },
        Request::AddInstrument { spec } => JournalEvent::AddInstrument { spec },
        Request::Deposit {
            account,
            currency,
            amount,
        } => JournalEvent::Deposit {
            account,
            currency,
            amount,
        },
        Request::Withdraw {
            account,
            currency,
            amount,
        } => JournalEvent::Withdraw {
            account,
            currency,
            amount,
        },
        Request::SetRiskLimits { symbol, limits } => JournalEvent::SetRiskLimits { symbol, limits },
        Request::SetCircuitBreaker { symbol, config } => {
            JournalEvent::SetCircuitBreaker { symbol, config }
        }
        Request::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => JournalEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        },
        Request::SetFeeSchedule { symbol, schedule } => {
            JournalEvent::SetFeeSchedule { symbol, schedule }
        }
        Request::QueryStats => JournalEvent::QueryStats,
        Request::EndOfDay => JournalEvent::EndOfDay,
        Request::ExpireOrders { timestamp_ns } => JournalEvent::ExpireOrders { timestamp_ns },
        Request::DisableInstrument { symbol } => JournalEvent::DisableInstrument { symbol },
        Request::EnableInstrument { symbol } => JournalEvent::EnableInstrument { symbol },
        Request::RemoveInstrument { symbol } => JournalEvent::RemoveInstrument { symbol },
        Request::Heartbeat | Request::ChallengeResponse { .. } => {
            unreachable!("heartbeats and auth messages must be filtered before to_event")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    use melin_engine::types::*;

    #[test]
    fn filter_heartbeat() {
        assert!(should_filter(&Request::Heartbeat));
    }

    #[test]
    fn filter_challenge_response() {
        assert!(should_filter(&Request::ChallengeResponse {
            signature: [0u8; 64],
            public_key: [0u8; 32],
        }));
    }

    #[test]
    fn do_not_filter_submit_order() {
        let req = Request::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(1),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: Price(NonZeroU64::new(100).unwrap()),
                    post_only: false,
                },
                quantity: Quantity(NonZeroU64::new(10).unwrap()),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::CancelNewest,
                expiry_ns: 0,
            },
        };
        assert!(!should_filter(&req));
    }

    #[test]
    fn do_not_filter_cancel() {
        assert!(!should_filter(&Request::CancelAll {
            account: AccountId(1),
        }));
    }

    #[test]
    fn permission_trader_can_trade() {
        let req = Request::CancelAll {
            account: AccountId(1),
        };
        assert!(check_permission(&req, Permission::Trader).is_ok());
    }

    #[test]
    fn permission_readonly_cannot_trade() {
        let req = Request::CancelAll {
            account: AccountId(1),
        };
        assert!(check_permission(&req, Permission::ReadOnly).is_err());
    }

    #[test]
    fn permission_operator_can_operate() {
        let req = Request::AddInstrument {
            spec: InstrumentSpec {
                symbol: Symbol(1),
                base: CurrencyId(1),
                quote: CurrencyId(2),
            },
        };
        assert!(check_permission(&req, Permission::Operator).is_ok());
    }

    #[test]
    fn permission_trader_cannot_operate() {
        let req = Request::AddInstrument {
            spec: InstrumentSpec {
                symbol: Symbol(1),
                base: CurrencyId(1),
                quote: CurrencyId(2),
            },
        };
        assert!(check_permission(&req, Permission::Trader).is_err());
    }

    #[test]
    fn permission_custodian_can_deposit() {
        let req = Request::Deposit {
            account: AccountId(1),
            currency: CurrencyId(1),
            amount: 100,
        };
        assert!(check_permission(&req, Permission::Custodian).is_ok());
    }

    #[test]
    fn permission_trader_cannot_deposit() {
        let req = Request::Deposit {
            account: AccountId(1),
            currency: CurrencyId(1),
            amount: 100,
        };
        assert!(check_permission(&req, Permission::Trader).is_err());
    }

    #[test]
    fn to_event_all_trading_variants() {
        // Just verify they don't panic.
        let order = Order {
            id: OrderId(1),
            account: AccountId(1),
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            time_in_force: TimeInForce::GTC,
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        };
        to_event(&Request::SubmitOrder {
            symbol: Symbol(1),
            order,
        });
        to_event(&Request::CancelOrder {
            symbol: Symbol(1),
            account: AccountId(1),
            order_id: OrderId(1),
        });
        to_event(&Request::CancelAll {
            account: AccountId(1),
        });
        to_event(&Request::QueryStats);
    }

    #[test]
    #[should_panic(expected = "must be filtered")]
    fn to_event_panics_on_heartbeat() {
        to_event(&Request::Heartbeat);
    }
}
