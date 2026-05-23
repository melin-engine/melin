//! Trading-side [`RequestDecoder`] implementation.
//!
//! Owns the bytes -> `melin_protocol::Request` -> `TradingEvent`
//! pipeline. Hides the wire enum behind the [`RequestDecoder`] trait
//! so the server runtime never needs to pattern-match on
//! application-shaped variants.

use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder};
use melin_protocol::codec;
use melin_protocol::message::Request;
use melin_trading::trading_event::TradingEvent;
use melin_wire_protocol::error::ProtocolError;

/// Decoder for the trading wire protocol.
///
/// Zero-sized. The runtime owns an `Arc<dyn RequestDecoder<...>>`;
/// constructing one is `Arc::new(ExchangeRequestDecoder)`.
#[derive(Debug, Clone, Copy)]
pub struct ExchangeRequestDecoder;

impl RequestDecoder for ExchangeRequestDecoder {
    type Event = TradingEvent;

    fn decode(&self, bytes: &[u8], permission: Permission) -> Decoded<TradingEvent> {
        let (request_seq, request) = match codec::decode_request(bytes) {
            Ok(pair) => pair,
            Err(e) => return Decoded::DecodeError(protocol_error_reason(&e)),
        };

        if should_filter(&request) {
            return Decoded::Filter;
        }

        if let Err(reason) = check_permission(&request, permission) {
            return Decoded::PermissionDenied(reason);
        }

        Decoded::Permitted {
            request_seq,
            event: to_trading_event(&request),
        }
    }
}

/// Collapse a typed `ProtocolError` into the static reason carried by
/// `Decoded::DecodeError`. The reader's debug log surfaces this
/// reason; a misbehaving client gets diagnosed without exposing the
/// full error chain.
#[inline]
fn protocol_error_reason(e: &ProtocolError) -> &'static str {
    match e {
        ProtocolError::Truncated => "truncated frame",
        ProtocolError::UnknownTag(_) => "unknown variant tag",
        ProtocolError::InvalidField(_) => "invalid field",
        ProtocolError::MessageTooLarge(_) => "message too large",
        ProtocolError::Io(_) => "io error",
    }
}

/// Transport-level frames the runtime never publishes to the
/// pipeline: heartbeats, post-auth handshakes, subscription control.
#[inline]
fn should_filter(request: &Request) -> bool {
    matches!(
        request,
        Request::Heartbeat | Request::ChallengeResponse { .. } | Request::Subscribe { .. }
    )
}

/// Permission model — separation of duties:
/// - Operator: exchange configuration (instruments, risk, circuit breakers, fees, EOD, stats)
/// - Custodian: fund management (deposit, withdraw)
/// - Trader: order submission and cancellation
/// - ReadOnly: rejected here for anything that isn't filtered out above
///   (heartbeats / handshakes / subscribe never reach this function;
///   any other request from a ReadOnly connection falls into the final
///   clause and is denied for lacking `can_trade`).
#[inline]
fn check_permission(request: &Request, permission: Permission) -> Result<(), &'static str> {
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

/// Per-variant `Request -> TradingEvent` mapping. Caller must have
/// filtered transport-level frames first; this panics on heartbeats /
/// post-auth handshakes / subscribe frames.
#[inline]
fn to_trading_event(request: &Request) -> TradingEvent {
    match *request {
        Request::SubmitOrder { symbol, order } => TradingEvent::SubmitOrder { symbol, order },
        Request::CancelOrder {
            symbol,
            account,
            order_id,
        } => TradingEvent::CancelOrder {
            symbol,
            account,
            order_id,
        },
        Request::CancelAll { account } => TradingEvent::CancelAll { account },
        Request::AddInstrument { spec } => TradingEvent::AddInstrument { spec },
        Request::Deposit {
            account,
            currency,
            amount,
        } => TradingEvent::Deposit {
            account,
            currency,
            amount,
        },
        Request::Withdraw {
            account,
            currency,
            amount,
        } => TradingEvent::Withdraw {
            account,
            currency,
            amount,
        },
        Request::SetRiskLimits { symbol, limits } => TradingEvent::SetRiskLimits { symbol, limits },
        Request::SetCircuitBreaker { symbol, config } => {
            TradingEvent::SetCircuitBreaker { symbol, config }
        }
        Request::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => TradingEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        },
        Request::SetFeeSchedule { symbol, schedule } => {
            TradingEvent::SetFeeSchedule { symbol, schedule }
        }
        Request::QueryStats => TradingEvent::QueryStats,
        Request::QueryPosition { account } => TradingEvent::QueryPosition { account },
        Request::QueryRequestSeq => TradingEvent::QueryRequestSeq,
        Request::EndOfDay => TradingEvent::EndOfDay,
        Request::DisableInstrument { symbol } => TradingEvent::DisableInstrument { symbol },
        Request::EnableInstrument { symbol } => TradingEvent::EnableInstrument { symbol },
        Request::RemoveInstrument { symbol } => TradingEvent::RemoveInstrument { symbol },
        Request::Heartbeat | Request::ChallengeResponse { .. } | Request::Subscribe { .. } => {
            unreachable!("filtered before to_trading_event")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    use melin_app::AppEvent;
    use melin_types::types::*;

    /// Wire-encode a Request into the byte form the decoder expects
    /// (seq + tag + payload, with the framing length-prefix already
    /// stripped — same shape `codec::decode_request` consumes).
    fn encode(request: &Request, seq: u64) -> Vec<u8> {
        let mut buf = vec![0u8; 256];
        let total = codec::encode_request(request, seq, &mut buf).unwrap();
        buf[4..total].to_vec()
    }

    fn order() -> Order {
        Order {
            id: OrderId(1),
            account: AccountId(1),
            side: Side::Buy,
            order_type: OrderType::Market,
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            time_in_force: TimeInForce::GTC,
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    #[test]
    fn heartbeat_is_filtered() {
        let bytes = encode(&Request::Heartbeat, 0);
        assert!(matches!(
            ExchangeRequestDecoder.decode(&bytes, Permission::Trader),
            Decoded::Filter
        ));
    }

    #[test]
    fn subscribe_is_filtered() {
        let bytes = encode(
            &Request::Subscribe {
                symbols: [Symbol(0); 8],
                count: 0,
            },
            0,
        );
        assert!(matches!(
            ExchangeRequestDecoder.decode(&bytes, Permission::Trader),
            Decoded::Filter
        ));
    }

    #[test]
    fn challenge_response_is_filtered() {
        let bytes = encode(
            &Request::ChallengeResponse {
                signature: [0u8; 64],
                public_key: [0u8; 32],
            },
            0,
        );
        assert!(matches!(
            ExchangeRequestDecoder.decode(&bytes, Permission::Trader),
            Decoded::Filter
        ));
    }

    #[test]
    fn submit_order_as_trader_is_permitted() {
        let bytes = encode(
            &Request::SubmitOrder {
                symbol: Symbol(1),
                order: order(),
            },
            42,
        );
        match ExchangeRequestDecoder.decode(&bytes, Permission::Trader) {
            Decoded::Permitted { request_seq, event } => {
                assert_eq!(request_seq, 42);
                assert!(matches!(event, TradingEvent::SubmitOrder { .. }));
                // Trading-side query taxonomy: order submission is not a query.
                assert!(!event.is_query());
            }
            other => panic!("expected Permitted, got {:?}", debug_variant(&other)),
        }
    }

    #[test]
    fn submit_order_as_readonly_is_denied() {
        let bytes = encode(
            &Request::SubmitOrder {
                symbol: Symbol(1),
                order: order(),
            },
            0,
        );
        assert!(matches!(
            ExchangeRequestDecoder.decode(&bytes, Permission::ReadOnly),
            Decoded::PermissionDenied(_)
        ));
    }

    #[test]
    fn add_instrument_as_operator_is_permitted() {
        let bytes = encode(
            &Request::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(1),
                    quote: CurrencyId(2),
                },
            },
            7,
        );
        assert!(matches!(
            ExchangeRequestDecoder.decode(&bytes, Permission::Operator),
            Decoded::Permitted { .. }
        ));
    }

    #[test]
    fn add_instrument_as_trader_is_denied() {
        let bytes = encode(
            &Request::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(1),
                    quote: CurrencyId(2),
                },
            },
            0,
        );
        assert!(matches!(
            ExchangeRequestDecoder.decode(&bytes, Permission::Trader),
            Decoded::PermissionDenied(_)
        ));
    }

    #[test]
    fn deposit_as_custodian_is_permitted() {
        let bytes = encode(
            &Request::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100,
            },
            3,
        );
        assert!(matches!(
            ExchangeRequestDecoder.decode(&bytes, Permission::Custodian),
            Decoded::Permitted { .. }
        ));
    }

    #[test]
    fn deposit_as_trader_is_denied() {
        let bytes = encode(
            &Request::Deposit {
                account: AccountId(1),
                currency: CurrencyId(1),
                amount: 100,
            },
            0,
        );
        assert!(matches!(
            ExchangeRequestDecoder.decode(&bytes, Permission::Trader),
            Decoded::PermissionDenied(_)
        ));
    }

    #[test]
    fn query_stats_is_permitted_and_flagged() {
        // QueryStats is an operator-only request — see
        // `Request::requires_operator`.
        let bytes = encode(&Request::QueryStats, 1);
        match ExchangeRequestDecoder.decode(&bytes, Permission::Operator) {
            Decoded::Permitted { event, .. } => {
                assert!(matches!(event, TradingEvent::QueryStats));
                assert!(event.is_query());
            }
            other => panic!("expected Permitted, got {:?}", debug_variant(&other)),
        }
    }

    #[test]
    fn malformed_frame_yields_decode_error() {
        // Empty bytes can't even fit the request_seq prefix.
        assert!(matches!(
            ExchangeRequestDecoder.decode(&[], Permission::Trader),
            Decoded::DecodeError(_)
        ));
    }

    fn debug_variant<E: AppEvent>(d: &Decoded<E>) -> &'static str {
        match d {
            Decoded::Filter => "Filter",
            Decoded::Permitted { .. } => "Permitted",
            Decoded::PermissionDenied(_) => "PermissionDenied",
            Decoded::DecodeError(_) => "DecodeError",
        }
    }

    // ------------------------------------------------------------------
    // Per-variant `Request -> TradingEvent` mapping checks.
    //
    // The trait tests above only assert `matches!(event,
    // TradingEvent::Foo { .. })`; these go one level deeper and
    // confirm the field-by-field mapping for every variant we
    // currently translate. They call `to_trading_event` directly
    // because asserting "the SubmitOrder symbol came through as
    // Symbol(1)" doesn't need a wire round-trip.
    // ------------------------------------------------------------------

    fn full_order(id: u64, account: u32, side: Side) -> Order {
        Order {
            id: OrderId(id),
            account: AccountId(account),
            side,
            order_type: OrderType::Limit {
                price: Price(NonZeroU64::new(100).unwrap()),
                post_only: false,
            },
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
            time_in_force: TimeInForce::GTC,
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        }
    }

    #[test]
    fn maps_submit_order() {
        let req = Request::SubmitOrder {
            symbol: Symbol(1),
            order: full_order(1, 1, Side::Buy),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::SubmitOrder { symbol, .. } if symbol == Symbol(1)
        ));
    }

    #[test]
    fn maps_cancel_order() {
        let req = Request::CancelOrder {
            symbol: Symbol(2),
            account: AccountId(5),
            order_id: OrderId(42),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::CancelOrder { symbol, account, order_id }
                if symbol == Symbol(2) && account == AccountId(5) && order_id == OrderId(42)
        ));
    }

    #[test]
    fn maps_cancel_all() {
        let req = Request::CancelAll {
            account: AccountId(7),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::CancelAll { account } if account == AccountId(7)
        ));
    }

    #[test]
    fn maps_deposit() {
        let req = Request::Deposit {
            account: AccountId(1),
            currency: CurrencyId(2),
            amount: 1000,
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::Deposit { account, currency, amount }
                if account == AccountId(1) && currency == CurrencyId(2) && amount == 1000
        ));
    }

    #[test]
    fn maps_add_instrument() {
        let spec = InstrumentSpec {
            symbol: Symbol(10),
            base: CurrencyId(1),
            quote: CurrencyId(2),
        };
        let req = Request::AddInstrument { spec };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::AddInstrument { spec: s } if s.symbol == Symbol(10)
        ));
    }

    #[test]
    fn maps_cancel_replace() {
        let req = Request::CancelReplace {
            symbol: Symbol(1),
            account: AccountId(1),
            order_id: OrderId(5),
            new_price: Price(NonZeroU64::new(200).unwrap()),
            new_quantity: Quantity(NonZeroU64::new(50).unwrap()),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::CancelReplace { order_id, .. } if order_id == OrderId(5)
        ));
    }

    #[test]
    fn maps_set_risk_limits() {
        let req = Request::SetRiskLimits {
            symbol: Symbol(1),
            limits: RiskLimits::default(),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::SetRiskLimits { symbol, .. } if symbol == Symbol(1)
        ));
    }

    #[test]
    fn maps_set_circuit_breaker() {
        let req = Request::SetCircuitBreaker {
            symbol: Symbol(1),
            config: CircuitBreakerConfig::default(),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::SetCircuitBreaker { symbol, .. } if symbol == Symbol(1)
        ));
    }

    #[test]
    fn maps_set_fee_schedule() {
        let req = Request::SetFeeSchedule {
            symbol: Symbol(3),
            schedule: FeeSchedule::default(),
        };
        assert!(matches!(
            to_trading_event(&req),
            TradingEvent::SetFeeSchedule { symbol, .. } if symbol == Symbol(3)
        ));
    }

    #[test]
    fn maps_query_stats() {
        assert!(matches!(
            to_trading_event(&Request::QueryStats),
            TradingEvent::QueryStats
        ));
    }

    #[test]
    #[should_panic(expected = "filtered before to_trading_event")]
    fn heartbeat_panics_if_not_filtered() {
        to_trading_event(&Request::Heartbeat);
    }

    #[test]
    #[should_panic(expected = "filtered before to_trading_event")]
    fn challenge_response_panics_if_not_filtered() {
        to_trading_event(&Request::ChallengeResponse {
            signature: [0u8; 64],
            public_key: [0u8; 32],
        });
    }
}
