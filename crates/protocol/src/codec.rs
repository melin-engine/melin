//! Binary wire codec for the trading protocol.
//!
//! Manual serialization (no serde) for zero allocation, predictable layout,
//! and no format stability concerns across dependency versions.
//!
//! ## Frame layout (little-endian)
//!
//! | Field     | Type | Bytes | Purpose                              |
//! |-----------|------|-------|--------------------------------------|
//! | length    | u32  | 4     | Byte count of type_tag + payload     |
//! | type_tag  | u8   | 1     | Message discriminant                 |
//! | payload   | ...  | var   | Variant-specific fields              |
//!
//! No CRC on the wire — TCP handles integrity. The 4-byte length prefix
//! provides framing; the type tag selects the variant.
//!
//! Only trading operations (submit/cancel) are on the wire. Administrative
//! operations (instrument registration, deposits) use a separate admin API.

use std::num::NonZeroU64;

use trading_engine::le;
use trading_engine::types::{
    AccountId, ExecutionReport, Order, OrderId, OrderType, Price, Quantity, RejectReason, Symbol,
};

use crate::error::ProtocolError;
use crate::message::{Request, ResponseKind};

// --- Request tags ---
const TAG_SUBMIT_ORDER: u8 = 1;
const TAG_CANCEL_ORDER: u8 = 2;
const TAG_REQUEST_HEARTBEAT: u8 = 3;

// --- Response tags ---
const TAG_PLACED: u8 = 11;
const TAG_FILL: u8 = 12;
const TAG_CANCELLED: u8 = 13;
const TAG_TRIGGERED: u8 = 14;
const TAG_REJECTED: u8 = 15;
const TAG_ENGINE_ERROR: u8 = 16;
const TAG_BATCH_END: u8 = 17;
const TAG_SERVER_READY: u8 = 18;
const TAG_RESPONSE_HEARTBEAT: u8 = 19;

// --- OrderType tags (wire-specific, not shared with journal) ---
const ORDER_TYPE_MARKET: u8 = 0;
const ORDER_TYPE_LIMIT: u8 = 1;
const ORDER_TYPE_STOP: u8 = 2;
const ORDER_TYPE_STOP_LIMIT: u8 = 3;

// --- RejectReason tags ---
const REJECT_NO_LIQUIDITY: u8 = 0;
const REJECT_FOK_CANNOT_FILL: u8 = 1;
const REJECT_INSUFFICIENT_BALANCE: u8 = 2;
const REJECT_UNKNOWN_ACCOUNT: u8 = 3;
const REJECT_UNKNOWN_SYMBOL: u8 = 4;
const REJECT_SELF_TRADE_PREVENTED: u8 = 5;
const REJECT_DUPLICATE_ORDER_ID: u8 = 6;
const REJECT_EXCEEDS_MAX_ORDER_QTY: u8 = 7;
const REJECT_EXCEEDS_MAX_NOTIONAL: u8 = 8;

/// Encode a request into `buf`. Returns total bytes written (length prefix + tag + payload).
///
/// The caller must ensure `buf` is large enough (128 bytes is always sufficient).
pub fn encode_request(request: &Request, buf: &mut [u8]) -> Result<usize, ProtocolError> {
    // Reserve 4 bytes for the length prefix, write tag + payload after it.
    let mut pos = 4;

    match request {
        Request::SubmitOrder { symbol, order } => {
            buf[pos] = TAG_SUBMIT_ORDER;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            pos += encode_order(order, &mut buf[pos..]);
        }
        Request::CancelOrder { symbol, order_id } => {
            buf[pos] = TAG_CANCEL_ORDER;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
        }
        Request::Heartbeat => {
            buf[pos] = TAG_REQUEST_HEARTBEAT;
            pos += 1;
        }
    }

    // Write the length prefix (excludes the 4-byte length field itself).
    let payload_len = pos - 4;
    le::put_u32(&mut buf[0..], payload_len as u32);

    Ok(pos)
}

/// Decode a request from `buf` (after the length prefix has been stripped).
///
/// `buf` should contain exactly the tag + payload bytes (no length prefix).
pub fn decode_request(buf: &[u8]) -> Result<Request, ProtocolError> {
    if buf.is_empty() {
        return Err(ProtocolError::Truncated);
    }

    let tag = buf[0];
    let payload = &buf[1..];

    match tag {
        TAG_SUBMIT_ORDER => {
            if payload.len() < 4 {
                return Err(ProtocolError::Truncated);
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let (_, order) = decode_order(&payload[4..])?;
            Ok(Request::SubmitOrder { symbol, order })
        }
        TAG_CANCEL_ORDER => {
            if payload.len() < 12 {
                return Err(ProtocolError::Truncated);
            }
            Ok(Request::CancelOrder {
                symbol: Symbol(le::get_u32(&payload[0..])),
                order_id: OrderId(le::get_u64(&payload[4..])),
            })
        }
        TAG_REQUEST_HEARTBEAT => Ok(Request::Heartbeat),
        _ => Err(ProtocolError::UnknownTag(tag)),
    }
}

/// Encode a response into `buf`. Returns total bytes written (length prefix + tag + payload).
///
/// The caller must ensure `buf` is large enough (128 bytes is always sufficient).
pub fn encode_response(response: &ResponseKind, buf: &mut [u8]) -> Result<usize, ProtocolError> {
    let mut pos = 4; // reserve for length prefix

    match response {
        ResponseKind::Report(report) => {
            pos += encode_execution_report(report, &mut buf[pos..]);
        }
        ResponseKind::EngineError => {
            buf[pos] = TAG_ENGINE_ERROR;
            pos += 1;
        }
        ResponseKind::BatchEnd => {
            buf[pos] = TAG_BATCH_END;
            pos += 1;
        }
        ResponseKind::ServerReady => {
            buf[pos] = TAG_SERVER_READY;
            pos += 1;
        }
        ResponseKind::Heartbeat => {
            buf[pos] = TAG_RESPONSE_HEARTBEAT;
            pos += 1;
        }
    }

    let payload_len = pos - 4;
    le::put_u32(&mut buf[0..], payload_len as u32);

    Ok(pos)
}

/// Decode a response from `buf` (after the length prefix has been stripped).
pub fn decode_response(buf: &[u8]) -> Result<ResponseKind, ProtocolError> {
    if buf.is_empty() {
        return Err(ProtocolError::Truncated);
    }

    let tag = buf[0];
    let payload = &buf[1..];

    match tag {
        TAG_ENGINE_ERROR => Ok(ResponseKind::EngineError),
        TAG_BATCH_END => Ok(ResponseKind::BatchEnd),
        TAG_SERVER_READY => Ok(ResponseKind::ServerReady),
        TAG_RESPONSE_HEARTBEAT => Ok(ResponseKind::Heartbeat),
        TAG_PLACED | TAG_FILL | TAG_CANCELLED | TAG_TRIGGERED | TAG_REJECTED => {
            let report = decode_execution_report(tag, payload)?;
            Ok(ResponseKind::Report(report))
        }
        _ => Err(ProtocolError::UnknownTag(tag)),
    }
}

// --- Order encoding (mirrors journal/codec.rs but decoupled) ---

/// Encode an `Order` into `buf`. Returns bytes written.
///
/// Layout: id(8) + account(4) + side(1) + order_type_tag(1) + order_type_fields(0..16) +
///         tif(1) + quantity(8)
fn encode_order(order: &Order, buf: &mut [u8]) -> usize {
    let mut pos = 0;
    le::put_u64(&mut buf[pos..], order.id.0);
    pos += 8;
    le::put_u32(&mut buf[pos..], order.account.0);
    pos += 4;
    buf[pos] = le::encode_side(order.side);
    pos += 1;

    match order.order_type {
        OrderType::Market => {
            buf[pos] = ORDER_TYPE_MARKET;
            pos += 1;
        }
        OrderType::Limit { price } => {
            buf[pos] = ORDER_TYPE_LIMIT;
            pos += 1;
            le::put_u64(&mut buf[pos..], price.get());
            pos += 8;
        }
        OrderType::Stop { trigger_price } => {
            buf[pos] = ORDER_TYPE_STOP;
            pos += 1;
            le::put_u64(&mut buf[pos..], trigger_price.get());
            pos += 8;
        }
        OrderType::StopLimit {
            trigger_price,
            limit_price,
        } => {
            buf[pos] = ORDER_TYPE_STOP_LIMIT;
            pos += 1;
            le::put_u64(&mut buf[pos..], trigger_price.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], limit_price.get());
            pos += 8;
        }
    }

    buf[pos] = le::encode_tif(order.time_in_force);
    pos += 1;
    le::put_u64(&mut buf[pos..], order.quantity.get());
    pos += 8;
    buf[pos] = le::encode_stp(order.stp);
    pos += 1;

    pos
}

/// Decode an `Order` from `buf`. Returns `(bytes_consumed, Order)`.
fn decode_order(buf: &[u8]) -> Result<(usize, Order), ProtocolError> {
    if buf.len() < 22 {
        return Err(ProtocolError::Truncated);
    }

    let mut pos = 0;
    let id = OrderId(le::get_u64(&buf[pos..]));
    pos += 8;
    let account = AccountId(le::get_u32(&buf[pos..]));
    pos += 4;
    let side = le::decode_side(buf[pos]).ok_or(ProtocolError::InvalidField("side"))?;
    pos += 1;

    let order_type_tag = buf[pos];
    pos += 1;

    let order_type = match order_type_tag {
        ORDER_TYPE_MARKET => OrderType::Market,
        ORDER_TYPE_LIMIT => {
            if buf.len() < pos + 8 {
                return Err(ProtocolError::Truncated);
            }
            let price = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(ProtocolError::InvalidField("limit price is zero"))?;
            pos += 8;
            OrderType::Limit {
                price: Price(price),
            }
        }
        ORDER_TYPE_STOP => {
            if buf.len() < pos + 8 {
                return Err(ProtocolError::Truncated);
            }
            let trigger = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(ProtocolError::InvalidField("stop trigger price is zero"))?;
            pos += 8;
            OrderType::Stop {
                trigger_price: Price(trigger),
            }
        }
        ORDER_TYPE_STOP_LIMIT => {
            if buf.len() < pos + 16 {
                return Err(ProtocolError::Truncated);
            }
            let trigger = NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(
                ProtocolError::InvalidField("stop-limit trigger price is zero"),
            )?;
            pos += 8;
            let limit = NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(
                ProtocolError::InvalidField("stop-limit limit price is zero"),
            )?;
            pos += 8;
            OrderType::StopLimit {
                trigger_price: Price(trigger),
                limit_price: Price(limit),
            }
        }
        _ => return Err(ProtocolError::InvalidField("order type tag")),
    };

    if buf.len() < pos + 10 {
        return Err(ProtocolError::Truncated);
    }

    let time_in_force =
        le::decode_tif(buf[pos]).ok_or(ProtocolError::InvalidField("time-in-force"))?;
    pos += 1;

    let quantity = NonZeroU64::new(le::get_u64(&buf[pos..]))
        .ok_or(ProtocolError::InvalidField("quantity is zero"))?;
    pos += 8;

    let stp =
        le::decode_stp(buf[pos]).ok_or(ProtocolError::InvalidField("self-trade protection"))?;
    pos += 1;

    Ok((
        pos,
        Order {
            id,
            account,
            side,
            order_type,
            time_in_force,
            quantity: Quantity(quantity),
            stp,
        },
    ))
}

// --- ExecutionReport encoding ---

/// Encode an `ExecutionReport` into `buf`. Returns bytes written (includes tag byte).
fn encode_execution_report(report: &ExecutionReport, buf: &mut [u8]) -> usize {
    let mut pos = 0;

    match report {
        ExecutionReport::Placed {
            order_id,
            side,
            price,
            quantity,
        } => {
            buf[pos] = TAG_PLACED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            buf[pos] = le::encode_side(*side);
            pos += 1;
            le::put_u64(&mut buf[pos..], price.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], quantity.get());
            pos += 8;
        }
        ExecutionReport::Fill {
            maker_order_id,
            taker_order_id,
            maker_account,
            taker_account,
            price,
            quantity,
        } => {
            buf[pos] = TAG_FILL;
            pos += 1;
            le::put_u64(&mut buf[pos..], maker_order_id.0);
            pos += 8;
            le::put_u64(&mut buf[pos..], taker_order_id.0);
            pos += 8;
            le::put_u32(&mut buf[pos..], maker_account.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], taker_account.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], price.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], quantity.get());
            pos += 8;
        }
        ExecutionReport::Cancelled {
            order_id,
            remaining_quantity,
        } => {
            buf[pos] = TAG_CANCELLED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            le::put_u64(&mut buf[pos..], remaining_quantity.get());
            pos += 8;
        }
        ExecutionReport::Triggered {
            order_id,
            trigger_price,
        } => {
            buf[pos] = TAG_TRIGGERED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            le::put_u64(&mut buf[pos..], trigger_price.get());
            pos += 8;
        }
        ExecutionReport::Rejected { order_id, reason } => {
            buf[pos] = TAG_REJECTED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            buf[pos] = encode_reject_reason(*reason);
            pos += 1;
        }
    }

    pos
}

/// Decode an `ExecutionReport` from tag + payload.
fn decode_execution_report(tag: u8, payload: &[u8]) -> Result<ExecutionReport, ProtocolError> {
    match tag {
        TAG_PLACED => {
            if payload.len() < 25 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let side = le::decode_side(payload[8]).ok_or(ProtocolError::InvalidField("side"))?;
            let price = NonZeroU64::new(le::get_u64(&payload[9..]))
                .ok_or(ProtocolError::InvalidField("placed price is zero"))?;
            let quantity = NonZeroU64::new(le::get_u64(&payload[17..]))
                .ok_or(ProtocolError::InvalidField("placed quantity is zero"))?;
            Ok(ExecutionReport::Placed {
                order_id,
                side,
                price: Price(price),
                quantity: Quantity(quantity),
            })
        }
        TAG_FILL => {
            if payload.len() < 40 {
                return Err(ProtocolError::Truncated);
            }
            let maker_order_id = OrderId(le::get_u64(&payload[0..]));
            let taker_order_id = OrderId(le::get_u64(&payload[8..]));
            let maker_account = AccountId(le::get_u32(&payload[16..]));
            let taker_account = AccountId(le::get_u32(&payload[20..]));
            let price = NonZeroU64::new(le::get_u64(&payload[24..]))
                .ok_or(ProtocolError::InvalidField("fill price is zero"))?;
            let quantity = NonZeroU64::new(le::get_u64(&payload[32..]))
                .ok_or(ProtocolError::InvalidField("fill quantity is zero"))?;
            Ok(ExecutionReport::Fill {
                maker_order_id,
                taker_order_id,
                maker_account,
                taker_account,
                price: Price(price),
                quantity: Quantity(quantity),
            })
        }
        TAG_CANCELLED => {
            if payload.len() < 16 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let remaining = NonZeroU64::new(le::get_u64(&payload[8..]))
                .ok_or(ProtocolError::InvalidField("cancelled remaining is zero"))?;
            Ok(ExecutionReport::Cancelled {
                order_id,
                remaining_quantity: Quantity(remaining),
            })
        }
        TAG_TRIGGERED => {
            if payload.len() < 16 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let trigger_price = NonZeroU64::new(le::get_u64(&payload[8..]))
                .ok_or(ProtocolError::InvalidField("trigger price is zero"))?;
            Ok(ExecutionReport::Triggered {
                order_id,
                trigger_price: Price(trigger_price),
            })
        }
        TAG_REJECTED => {
            if payload.len() < 9 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let reason = decode_reject_reason(payload[8])?;
            Ok(ExecutionReport::Rejected { order_id, reason })
        }
        _ => Err(ProtocolError::UnknownTag(tag)),
    }
}

fn encode_reject_reason(reason: RejectReason) -> u8 {
    match reason {
        RejectReason::NoLiquidity => REJECT_NO_LIQUIDITY,
        RejectReason::FOKCannotFill => REJECT_FOK_CANNOT_FILL,
        RejectReason::InsufficientBalance => REJECT_INSUFFICIENT_BALANCE,
        RejectReason::UnknownAccount => REJECT_UNKNOWN_ACCOUNT,
        RejectReason::UnknownSymbol => REJECT_UNKNOWN_SYMBOL,
        RejectReason::SelfTradePrevented => REJECT_SELF_TRADE_PREVENTED,
        RejectReason::DuplicateOrderId => REJECT_DUPLICATE_ORDER_ID,
        RejectReason::ExceedsMaxOrderQty => REJECT_EXCEEDS_MAX_ORDER_QTY,
        RejectReason::ExceedsMaxNotional => REJECT_EXCEEDS_MAX_NOTIONAL,
    }
}

fn decode_reject_reason(b: u8) -> Result<RejectReason, ProtocolError> {
    match b {
        REJECT_NO_LIQUIDITY => Ok(RejectReason::NoLiquidity),
        REJECT_FOK_CANNOT_FILL => Ok(RejectReason::FOKCannotFill),
        REJECT_INSUFFICIENT_BALANCE => Ok(RejectReason::InsufficientBalance),
        REJECT_UNKNOWN_ACCOUNT => Ok(RejectReason::UnknownAccount),
        REJECT_UNKNOWN_SYMBOL => Ok(RejectReason::UnknownSymbol),
        REJECT_SELF_TRADE_PREVENTED => Ok(RejectReason::SelfTradePrevented),
        REJECT_DUPLICATE_ORDER_ID => Ok(RejectReason::DuplicateOrderId),
        REJECT_EXCEEDS_MAX_ORDER_QTY => Ok(RejectReason::ExceedsMaxOrderQty),
        REJECT_EXCEEDS_MAX_NOTIONAL => Ok(RejectReason::ExceedsMaxNotional),
        _ => Err(ProtocolError::InvalidField("reject reason")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_engine::types::{SelfTradeProtection, Side, TimeInForce};

    fn nz(v: u64) -> NonZeroU64 {
        NonZeroU64::new(v).unwrap()
    }

    fn make_requests() -> Vec<Request> {
        vec![
            Request::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(100),
                    account: AccountId(42),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: Price(nz(5000)),
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: Quantity(nz(10)),
                    stp: SelfTradeProtection::CancelNewest,
                },
            },
            Request::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(101),
                    account: AccountId(42),
                    side: Side::Sell,
                    order_type: OrderType::Market,
                    time_in_force: TimeInForce::IOC,
                    quantity: Quantity(nz(5)),
                    stp: SelfTradeProtection::Allow,
                },
            },
            Request::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(102),
                    account: AccountId(42),
                    side: Side::Buy,
                    order_type: OrderType::Stop {
                        trigger_price: Price(nz(4500)),
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: Quantity(nz(20)),
                    stp: SelfTradeProtection::CancelOldest,
                },
            },
            Request::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(103),
                    account: AccountId(42),
                    side: Side::Sell,
                    order_type: OrderType::StopLimit {
                        trigger_price: Price(nz(6000)),
                        limit_price: Price(nz(5900)),
                    },
                    time_in_force: TimeInForce::FOK,
                    quantity: Quantity(nz(15)),
                    stp: SelfTradeProtection::CancelBoth,
                },
            },
            Request::CancelOrder {
                symbol: Symbol(1),
                order_id: OrderId(100),
            },
            Request::Heartbeat,
        ]
    }

    fn make_responses() -> Vec<ResponseKind> {
        vec![
            ResponseKind::Report(ExecutionReport::Placed {
                order_id: OrderId(1),
                side: Side::Buy,
                price: Price(nz(100)),
                quantity: Quantity(nz(50)),
            }),
            ResponseKind::Report(ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                maker_account: AccountId(10),
                taker_account: AccountId(20),
                price: Price(nz(100)),
                quantity: Quantity(nz(10)),
            }),
            ResponseKind::Report(ExecutionReport::Cancelled {
                order_id: OrderId(3),
                remaining_quantity: Quantity(nz(5)),
            }),
            ResponseKind::Report(ExecutionReport::Triggered {
                order_id: OrderId(4),
                trigger_price: Price(nz(200)),
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(5),
                reason: RejectReason::NoLiquidity,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(6),
                reason: RejectReason::FOKCannotFill,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(7),
                reason: RejectReason::InsufficientBalance,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(8),
                reason: RejectReason::UnknownAccount,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(9),
                reason: RejectReason::UnknownSymbol,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(10),
                reason: RejectReason::SelfTradePrevented,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(11),
                reason: RejectReason::DuplicateOrderId,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(12),
                reason: RejectReason::ExceedsMaxOrderQty,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(13),
                reason: RejectReason::ExceedsMaxNotional,
            }),
            ResponseKind::EngineError,
            ResponseKind::BatchEnd,
            ResponseKind::ServerReady,
            ResponseKind::Heartbeat,
        ]
    }

    #[test]
    fn request_round_trip_all_variants() {
        let requests = make_requests();
        let mut buf = [0u8; 128];

        for (i, request) in requests.iter().enumerate() {
            let written = encode_request(request, &mut buf).unwrap();
            // Skip the 4-byte length prefix for decode.
            let decoded = decode_request(&buf[4..written]).unwrap();
            assert_eq!(&decoded, request, "request variant {i}");
        }
    }

    #[test]
    fn response_round_trip_all_variants() {
        let responses = make_responses();
        let mut buf = [0u8; 128];

        for (i, response) in responses.iter().enumerate() {
            let written = encode_response(response, &mut buf).unwrap();
            let decoded = decode_response(&buf[4..written]).unwrap();
            assert_eq!(&decoded, response, "response variant {i}");
        }
    }

    #[test]
    fn truncated_request_detected() {
        let result = decode_request(&[]);
        assert!(matches!(result, Err(ProtocolError::Truncated)));

        // Tag present but payload too short for SubmitOrder.
        let result = decode_request(&[TAG_SUBMIT_ORDER, 0, 0]);
        assert!(matches!(result, Err(ProtocolError::Truncated)));
    }

    #[test]
    fn unknown_request_tag_detected() {
        let result = decode_request(&[255]);
        assert!(matches!(result, Err(ProtocolError::UnknownTag(255))));
    }

    #[test]
    fn truncated_response_detected() {
        let result = decode_response(&[]);
        assert!(matches!(result, Err(ProtocolError::Truncated)));
    }

    #[test]
    fn unknown_response_tag_detected() {
        let result = decode_response(&[99]);
        assert!(matches!(result, Err(ProtocolError::UnknownTag(99))));
    }

    #[test]
    fn length_prefix_is_correct() {
        let request = Request::CancelOrder {
            symbol: Symbol(1),
            order_id: OrderId(42),
        };
        let mut buf = [0u8; 128];
        let written = encode_request(&request, &mut buf).unwrap();

        let length = le::get_u32(&buf[0..]) as usize;
        assert_eq!(length, written - 4, "length prefix should be total - 4");
    }
}
