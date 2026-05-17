//! Trading-side [`RequestDecoder`] implementation.
//!
//! Owns the bytes -> `melin_protocol::Request` -> `TradingEvent`
//! pipeline. Hides the wire enum behind the [`RequestDecoder`] trait
//! so the server runtime never needs to pattern-match on
//! application-shaped variants.

use crate::JournalEvent;
use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder};
use melin_protocol::codec;
use melin_protocol::message::Request;
use melin_trading::trading_event::TradingEvent;

/// Decoder for the trading wire protocol.
///
/// Zero-sized. The runtime owns an `Arc<dyn RequestDecoder<...>>`;
/// constructing one is `Arc::new(TradingRequestDecoder)`.
#[derive(Debug, Default, Clone, Copy)]
pub struct TradingRequestDecoder;

impl RequestDecoder for TradingRequestDecoder {
    type Event = TradingEvent;

    fn decode(&self, bytes: &[u8], permission: Permission) -> Decoded<TradingEvent> {
        let (request_seq, request) = match codec::decode_request(bytes) {
            Ok(pair) => pair,
            // The protocol codec returns a typed error; collapsing to a
            // static reason matches the reader's debug-log granularity
            // and keeps the trait return type small.
            Err(_) => return Decoded::DecodeError("malformed request frame"),
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

// The three helpers below are public for transitional callers (the
// DPDK transport still calls them directly; Commit D rewires it
// through the trait). After that, they become private and folded
// inline into `decode`.

/// Transport-level frames the runtime never publishes to the
/// pipeline: heartbeats, post-auth handshakes, subscription control.
#[inline]
pub fn should_filter(request: &Request) -> bool {
    matches!(
        request,
        Request::Heartbeat | Request::ChallengeResponse { .. } | Request::Subscribe { .. }
    )
}

/// Permission model — separation of duties:
/// - Operator: exchange configuration (instruments, risk, circuit breakers, fees, EOD, stats)
/// - Custodian: fund management (deposit, withdraw)
/// - Trader: order submission and cancellation
/// - ReadOnly: heartbeats only (filtered before this function)
#[inline]
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

/// Wrap a decoded request as a `JournalEvent::App`. Transitional —
/// the DPDK transport still wants the journal-wrapped form; once
/// Commit D routes DPDK through the decoder trait, this and the
/// helpers above are folded inline.
pub fn to_event(request: &Request) -> JournalEvent {
    JournalEvent::App(to_trading_event(request))
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
            unreachable!("filtered before to_event")
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
            TradingRequestDecoder.decode(&bytes, Permission::Trader),
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
            TradingRequestDecoder.decode(&bytes, Permission::Trader),
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
        match TradingRequestDecoder.decode(&bytes, Permission::Trader) {
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
            TradingRequestDecoder.decode(&bytes, Permission::ReadOnly),
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
            TradingRequestDecoder.decode(&bytes, Permission::Operator),
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
            TradingRequestDecoder.decode(&bytes, Permission::Trader),
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
            TradingRequestDecoder.decode(&bytes, Permission::Custodian),
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
            TradingRequestDecoder.decode(&bytes, Permission::Trader),
            Decoded::PermissionDenied(_)
        ));
    }

    #[test]
    fn query_stats_is_permitted_and_flagged() {
        // QueryStats is an operator-only request — see
        // `Request::requires_operator`.
        let bytes = encode(&Request::QueryStats, 1);
        match TradingRequestDecoder.decode(&bytes, Permission::Operator) {
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
            TradingRequestDecoder.decode(&[], Permission::Trader),
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
}
