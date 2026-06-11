//! Binary wire codec for the trading protocol.
//!
//! Manual serialization (no serde) for zero allocation, predictable layout,
//! and no format stability concerns across dependency versions.
//!
//! ## Request frame layout (little-endian)
//!
//! | Field     | Type | Bytes | Purpose                              |
//! |-----------|------|-------|--------------------------------------|
//! | length    | u32  | 4     | Byte count of seq + type_tag + payload |
//! | seq       | u64  | 8     | Per-key request sequence (idempotency) |
//! | type_tag  | u8   | 1     | Message discriminant                 |
//! | payload   | ...  | var   | Variant-specific fields              |
//!
//! ## Response frame layout (little-endian)
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

use melin_types::le;
use melin_types::types::{
    AccountBalance, AccountId, CircuitBreakerConfig, CurrencyId, ExecutionReport, FeeSchedule,
    InstrumentSpec, InstrumentStatus, Order, OrderId, OrderType, Price, Quantity, RejectReason,
    RiskLimits, Symbol, TimeInForce,
};
use zerocopy::little_endian::{U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use crate::message::{Request, ResponseKind};
use melin_wire_protocol::error::ProtocolError;

// --- Wire header structs ---
//
// Variant payloads are NOT typed: they're tagged unions with variable-
// length fields (Order has Market/Limit/Stop/StopLimit variants of
// 0/8/8/16 extra bytes, plus an optional 8-byte expiry for GTD).
// Per-variant zerocopy structs would multiply the type surface without
// matching gain. The frame headers are universal and fixed-shape, so
// they're typed; payloads keep the explicit le::put / le::get chain.

/// Length-prefixed frame header for requests:
/// `[length:u32] [seq:u64] [tag:u8] [payload]`. The 4-byte length value
/// covers `seq + tag + payload`. Encoders back-fill this at the end.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct RequestFrameHeader {
    length: U32,
    seq: U64,
}

const REQUEST_FRAME_HEADER_LEN: usize = core::mem::size_of::<RequestFrameHeader>();
const _: () = assert!(REQUEST_FRAME_HEADER_LEN == 12);

/// Post-length-prefix view of a request received from the wire:
/// `[seq:u64] [tag:u8]` (the 4-byte length prefix has been stripped
/// by the framing layer). The decoder peels this 8-byte typed prefix
/// and reads `tag` from the byte that follows.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct RequestSeqHeader {
    seq: U64,
}

const REQUEST_SEQ_HEADER_LEN: usize = core::mem::size_of::<RequestSeqHeader>();
const _: () = assert!(REQUEST_SEQ_HEADER_LEN == 8);

// Transport-level tags — imported from wire-protocol (single source of truth).
use melin_wire_protocol::control_codec::{
    TAG_AUTH_FAILED, TAG_BATCH_END, TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_ENGINE_ERROR,
    TAG_RESPONSE_HEARTBEAT, TAG_SERVER_BUSY, TAG_SERVER_READY,
};

// --- Domain request tags (0x10–0x2F) ---
const TAG_SUBMIT_ORDER: u8 = 0x10;
const TAG_CANCEL_ORDER: u8 = 0x11;
const TAG_REQUEST_HEARTBEAT: u8 = 0x12;
const TAG_CANCEL_ALL: u8 = 0x13;
const TAG_CANCEL_REPLACE: u8 = 0x14;
const TAG_ADD_INSTRUMENT: u8 = 0x15;
const TAG_DEPOSIT: u8 = 0x16;
const TAG_WITHDRAW: u8 = 0x17;
const TAG_SET_RISK_LIMITS: u8 = 0x18;
const TAG_SET_CIRCUIT_BREAKER: u8 = 0x19;
const TAG_SET_FEE_SCHEDULE: u8 = 0x1A;
const TAG_END_OF_DAY: u8 = 0x1B;
const TAG_DISABLE_INSTRUMENT: u8 = 0x1C;
const TAG_ENABLE_INSTRUMENT: u8 = 0x1D;
const TAG_REMOVE_INSTRUMENT: u8 = 0x1E;
const TAG_SUBSCRIBE: u8 = 0x1F;
const TAG_QUERY_STATS: u8 = 0x20;
const TAG_QUERY_POSITION: u8 = 0x21;
const TAG_QUERY_REQUEST_SEQ: u8 = 0x22;

// --- Domain response tags (0x30–0x4F) ---
// Transport-level response tags (0x01–0x0F) imported from wire-protocol above.
const TAG_PLACED: u8 = 0x30;
const TAG_FILL: u8 = 0x31;
const TAG_CANCELLED: u8 = 0x32;
const TAG_TRIGGERED: u8 = 0x33;
const TAG_REJECTED: u8 = 0x34;
const TAG_REPLACED: u8 = 0x35;
const TAG_INSTRUMENT_STATUS_CHANGED: u8 = 0x36;
const TAG_STATS_HEADER: u8 = 0x37;
const TAG_BOOK_SNAPSHOT_BEGIN: u8 = 0x38;
const TAG_BOOK_SNAPSHOT_LEVEL: u8 = 0x39;
const TAG_BOOK_SNAPSHOT_END: u8 = 0x3A;
const TAG_SNAPSHOT_COMPLETE: u8 = 0x3B;
const TAG_POSITION_SNAPSHOT: u8 = 0x3C;
const TAG_REQUEST_SEQ_HWM: u8 = 0x3D;

// --- OrderType tags (wire-specific, not shared with journal) ---
const ORDER_TYPE_MARKET: u8 = 0;
const ORDER_TYPE_LIMIT: u8 = 1;
const ORDER_TYPE_STOP: u8 = 2;
const ORDER_TYPE_STOP_LIMIT: u8 = 3;
const ORDER_TYPE_LIMIT_POST_ONLY: u8 = 4;

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
const REJECT_TRADING_HALTED: u8 = 9;
const REJECT_OUTSIDE_PRICE_BAND: u8 = 10;
const REJECT_UNKNOWN_ORDER: u8 = 11;
const REJECT_PRICE_WOULD_CROSS: u8 = 12;
const REJECT_POST_ONLY_WOULD_CROSS: u8 = 13;
const REJECT_HAS_RESTING_ORDERS: u8 = 14;
const REJECT_DUPLICATE_REQUEST: u8 = 15;
const REJECT_REPLICA_DISCONNECTED: u8 = 16;
const REJECT_INVALID_EXPIRY: u8 = 17;
const REJECT_INSTRUMENT_DISABLED: u8 = 18;
const REJECT_EXCEEDS_MAX_OPEN_ORDERS: u8 = 19;
const REJECT_EXCEEDS_ORDER_RATE: u8 = 20;
const REJECT_SUPERSEDED: u8 = 21;

/// Encode a request into `buf`. Returns total bytes written (length prefix + seq + tag + payload).
///
/// The caller must ensure `buf` is large enough (128 bytes is always sufficient
/// — bound is set by `ChallengeResponse`: 4 prefix + 8 seq + 1 tag + 64 sig +
/// 32 pubkey + 19 slack).
/// `seq` is the per-key monotonic request sequence for idempotency dedup.
/// Heartbeat and ChallengeResponse use `seq = 0` (exempt from dedup).
pub fn encode_request(request: &Request, seq: u64, buf: &mut [u8]) -> Result<usize, ProtocolError> {
    // Reserve the request frame header (length + seq); back-filled below.
    let mut pos = REQUEST_FRAME_HEADER_LEN;

    match request {
        Request::SubmitOrder { symbol, order } => {
            buf[pos] = TAG_SUBMIT_ORDER;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            pos += encode_order(order, &mut buf[pos..]);
        }
        Request::CancelOrder {
            symbol,
            account,
            order_id,
        } => {
            buf[pos] = TAG_CANCEL_ORDER;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
        }
        Request::CancelAll { account } => {
            buf[pos] = TAG_CANCEL_ALL;
            pos += 1;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
        }
        Request::Heartbeat => {
            buf[pos] = TAG_REQUEST_HEARTBEAT;
            pos += 1;
        }
        Request::ChallengeResponse {
            signature,
            public_key,
        } => {
            buf[pos] = TAG_CHALLENGE_RESPONSE;
            pos += 1;
            buf[pos..pos + 64].copy_from_slice(signature);
            pos += 64;
            buf[pos..pos + 32].copy_from_slice(public_key);
            pos += 32;
        }
        Request::AddInstrument { spec } => {
            buf[pos] = TAG_ADD_INSTRUMENT;
            pos += 1;
            le::put_u32(&mut buf[pos..], spec.symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], spec.base.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], spec.quote.0);
            pos += 4;
        }
        Request::Deposit {
            account,
            currency,
            amount,
        } => {
            buf[pos] = TAG_DEPOSIT;
            pos += 1;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], currency.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], *amount);
            pos += 8;
        }
        Request::Withdraw {
            account,
            currency,
            amount,
        } => {
            buf[pos] = TAG_WITHDRAW;
            pos += 1;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], currency.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], *amount);
            pos += 8;
        }
        Request::SetRiskLimits { symbol, limits } => {
            buf[pos] = TAG_SET_RISK_LIMITS;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            // Flags byte: bit 0 = has max_order_qty, bit 1 = has max_order_notional.
            let flags = (limits.max_order_qty.is_some() as u8)
                | ((limits.max_order_notional.is_some() as u8) << 1);
            buf[pos] = flags;
            pos += 1;
            if let Some(qty) = limits.max_order_qty {
                le::put_u64(&mut buf[pos..], qty.get());
                pos += 8;
            }
            if let Some(notional) = limits.max_order_notional {
                le::put_u64(&mut buf[pos..], notional);
                pos += 8;
            }
        }
        Request::SetCircuitBreaker { symbol, config } => {
            buf[pos] = TAG_SET_CIRCUIT_BREAKER;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            // Flags: bit 0 = has lower band, bit 1 = has upper band, bit 2 = halted.
            let flags = (config.price_band_lower.is_some() as u8)
                | ((config.price_band_upper.is_some() as u8) << 1)
                | ((config.halted as u8) << 2);
            buf[pos] = flags;
            pos += 1;
            if let Some(lower) = config.price_band_lower {
                le::put_u64(&mut buf[pos..], lower.get());
                pos += 8;
            }
            if let Some(upper) = config.price_band_upper {
                le::put_u64(&mut buf[pos..], upper.get());
                pos += 8;
            }
        }
        Request::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => {
            buf[pos] = TAG_CANCEL_REPLACE;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            le::put_u64(&mut buf[pos..], new_price.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], new_quantity.get());
            pos += 8;
        }
        Request::SetFeeSchedule { symbol, schedule } => {
            buf[pos] = TAG_SET_FEE_SCHEDULE;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_i16(&mut buf[pos..], schedule.maker_fee_bps);
            pos += 2;
            le::put_i16(&mut buf[pos..], schedule.taker_fee_bps);
            pos += 2;
        }
        Request::QueryStats => {
            buf[pos] = TAG_QUERY_STATS;
            pos += 1;
        }
        Request::EndOfDay => {
            buf[pos] = TAG_END_OF_DAY;
            pos += 1;
        }
        Request::DisableInstrument { symbol } => {
            buf[pos] = TAG_DISABLE_INSTRUMENT;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
        }
        Request::EnableInstrument { symbol } => {
            buf[pos] = TAG_ENABLE_INSTRUMENT;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
        }
        Request::RemoveInstrument { symbol } => {
            buf[pos] = TAG_REMOVE_INSTRUMENT;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
        }
        Request::Subscribe { symbols, count } => {
            buf[pos] = TAG_SUBSCRIBE;
            pos += 1;
            buf[pos] = *count;
            pos += 1;
            for sym in &symbols[..(*count as usize)] {
                le::put_u32(&mut buf[pos..], sym.0);
                pos += 4;
            }
        }
        Request::QueryPosition { account } => {
            buf[pos] = TAG_QUERY_POSITION;
            pos += 1;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
        }
        Request::QueryRequestSeq => {
            buf[pos] = TAG_QUERY_REQUEST_SEQ;
            pos += 1;
        }
    }

    // Write the length prefix (excludes the 4-byte length field itself).
    let payload_len = pos - 4;
    let header = RequestFrameHeader::mut_from_bytes(&mut buf[..REQUEST_FRAME_HEADER_LEN])
        .expect("REQUEST_FRAME_HEADER_LEN slice matches struct size");
    header.length = U32::new(payload_len as u32);
    header.seq = U64::new(seq);

    Ok(pos)
}

/// Decode a request from `buf` (after the length prefix has been stripped).
///
/// `buf` should contain exactly the seq + tag + payload bytes (no length prefix).
/// Returns `(seq, Request)` where `seq` is the per-key idempotency sequence.
pub fn decode_request(buf: &[u8]) -> Result<(u64, Request), ProtocolError> {
    // Need at least seq(8) + tag(1) = 9 bytes.
    let (header, after_header) =
        RequestSeqHeader::ref_from_prefix(buf).map_err(|_| ProtocolError::Truncated)?;
    if after_header.is_empty() {
        return Err(ProtocolError::Truncated);
    }

    let seq = header.seq.get();
    let tag = after_header[0];
    let payload = &after_header[1..];

    match tag {
        TAG_SUBMIT_ORDER => {
            if payload.len() < 4 {
                return Err(ProtocolError::Truncated);
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let (_, order) = decode_order(&payload[4..])?;
            Ok((seq, Request::SubmitOrder { symbol, order }))
        }
        TAG_CANCEL_ORDER => {
            // symbol(4) + account(4) + order_id(8) = 16
            if payload.len() < 16 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::CancelOrder {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                    account: AccountId(le::get_u32(&payload[4..])),
                    order_id: OrderId(le::get_u64(&payload[8..])),
                },
            ))
        }
        TAG_CANCEL_ALL => {
            if payload.len() < 4 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::CancelAll {
                    account: AccountId(le::get_u32(&payload[0..])),
                },
            ))
        }
        TAG_REQUEST_HEARTBEAT => Ok((seq, Request::Heartbeat)),
        TAG_CHALLENGE_RESPONSE => {
            if payload.len() < 96 {
                return Err(ProtocolError::Truncated);
            }
            let mut signature = [0u8; 64];
            signature.copy_from_slice(&payload[..64]);
            let mut public_key = [0u8; 32];
            public_key.copy_from_slice(&payload[64..96]);
            Ok((
                seq,
                Request::ChallengeResponse {
                    signature,
                    public_key,
                },
            ))
        }
        TAG_ADD_INSTRUMENT => {
            if payload.len() < 12 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::AddInstrument {
                    spec: InstrumentSpec {
                        symbol: Symbol(le::get_u32(&payload[0..])),
                        base: CurrencyId(le::get_u32(&payload[4..])),
                        quote: CurrencyId(le::get_u32(&payload[8..])),
                    },
                },
            ))
        }
        TAG_DEPOSIT => {
            if payload.len() < 16 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::Deposit {
                    account: AccountId(le::get_u32(&payload[0..])),
                    currency: CurrencyId(le::get_u32(&payload[4..])),
                    amount: le::get_u64(&payload[8..]),
                },
            ))
        }
        TAG_WITHDRAW => {
            if payload.len() < 16 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::Withdraw {
                    account: AccountId(le::get_u32(&payload[0..])),
                    currency: CurrencyId(le::get_u32(&payload[4..])),
                    amount: le::get_u64(&payload[8..]),
                },
            ))
        }
        TAG_SET_RISK_LIMITS => {
            if payload.len() < 5 {
                return Err(ProtocolError::Truncated);
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let flags = payload[4];
            let mut off = 5;

            let max_order_qty = if flags & 1 != 0 {
                if payload.len() < off + 8 {
                    return Err(ProtocolError::Truncated);
                }
                let v = NonZeroU64::new(le::get_u64(&payload[off..]))
                    .ok_or(ProtocolError::InvalidField("max_order_qty is zero"))?;
                off += 8;
                Some(Quantity(v))
            } else {
                None
            };

            let max_order_notional = if flags & 2 != 0 {
                if payload.len() < off + 8 {
                    return Err(ProtocolError::Truncated);
                }
                let v = le::get_u64(&payload[off..]);
                Some(v)
            } else {
                None
            };

            Ok((
                seq,
                Request::SetRiskLimits {
                    symbol,
                    limits: RiskLimits {
                        max_order_qty,
                        max_order_notional,
                    },
                },
            ))
        }
        TAG_SET_CIRCUIT_BREAKER => {
            if payload.len() < 5 {
                return Err(ProtocolError::Truncated);
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let flags = payload[4];
            let mut off = 5;

            let price_band_lower = if flags & 1 != 0 {
                if payload.len() < off + 8 {
                    return Err(ProtocolError::Truncated);
                }
                let v = NonZeroU64::new(le::get_u64(&payload[off..]))
                    .ok_or(ProtocolError::InvalidField("price_band_lower is zero"))?;
                off += 8;
                Some(Price(v))
            } else {
                None
            };

            let price_band_upper = if flags & 2 != 0 {
                if payload.len() < off + 8 {
                    return Err(ProtocolError::Truncated);
                }
                let v = NonZeroU64::new(le::get_u64(&payload[off..]))
                    .ok_or(ProtocolError::InvalidField("price_band_upper is zero"))?;
                Some(Price(v))
            } else {
                None
            };

            let halted = flags & 4 != 0;

            Ok((
                seq,
                Request::SetCircuitBreaker {
                    symbol,
                    config: CircuitBreakerConfig {
                        price_band_lower,
                        price_band_upper,
                        halted,
                    },
                },
            ))
        }
        TAG_CANCEL_REPLACE => {
            // symbol(4) + account(4) + order_id(8) + new_price(8) + new_quantity(8) = 32
            if payload.len() < 32 {
                return Err(ProtocolError::Truncated);
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let account = AccountId(le::get_u32(&payload[4..]));
            let order_id = OrderId(le::get_u64(&payload[8..]));
            let new_price = NonZeroU64::new(le::get_u64(&payload[16..])).ok_or(
                ProtocolError::InvalidField("cancel-replace new_price is zero"),
            )?;
            let new_quantity = NonZeroU64::new(le::get_u64(&payload[24..])).ok_or(
                ProtocolError::InvalidField("cancel-replace new_quantity is zero"),
            )?;
            Ok((
                seq,
                Request::CancelReplace {
                    symbol,
                    account,
                    order_id,
                    new_price: Price(new_price),
                    new_quantity: Quantity(new_quantity),
                },
            ))
        }
        TAG_QUERY_STATS => Ok((seq, Request::QueryStats)),
        TAG_END_OF_DAY => Ok((seq, Request::EndOfDay)),
        TAG_SET_FEE_SCHEDULE => {
            // symbol(4) + maker_fee_bps(2) + taker_fee_bps(2) = 8
            if payload.len() < 8 {
                return Err(ProtocolError::Truncated);
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let maker_fee_bps = le::get_i16(&payload[4..]);
            let taker_fee_bps = le::get_i16(&payload[6..]);
            Ok((
                seq,
                Request::SetFeeSchedule {
                    symbol,
                    schedule: FeeSchedule {
                        maker_fee_bps,
                        taker_fee_bps,
                    },
                },
            ))
        }
        TAG_DISABLE_INSTRUMENT => {
            if payload.len() < 4 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::DisableInstrument {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                },
            ))
        }
        TAG_ENABLE_INSTRUMENT => {
            if payload.len() < 4 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::EnableInstrument {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                },
            ))
        }
        TAG_REMOVE_INSTRUMENT => {
            if payload.len() < 4 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::RemoveInstrument {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                },
            ))
        }
        TAG_SUBSCRIBE => {
            // count(1) + count×symbol(4)
            if payload.is_empty() {
                return Err(ProtocolError::Truncated);
            }
            let count = payload[0];
            if count > 8 {
                return Err(ProtocolError::InvalidField("subscribe count > 8"));
            }
            let needed = 1 + (count as usize) * 4;
            if payload.len() < needed {
                return Err(ProtocolError::Truncated);
            }
            let mut symbols = [Symbol(0); 8];
            for i in 0..count as usize {
                symbols[i] = Symbol(le::get_u32(&payload[1 + i * 4..]));
            }
            Ok((seq, Request::Subscribe { symbols, count }))
        }
        TAG_QUERY_POSITION => {
            // account(4)
            if payload.len() < 4 {
                return Err(ProtocolError::Truncated);
            }
            Ok((
                seq,
                Request::QueryPosition {
                    account: AccountId(le::get_u32(&payload[0..])),
                },
            ))
        }
        TAG_QUERY_REQUEST_SEQ => Ok((seq, Request::QueryRequestSeq)),
        _ => Err(ProtocolError::UnknownTag(tag)),
    }
}

/// Encode a response into `buf`. Returns total bytes written (length prefix + tag + payload).
///
/// The caller must ensure `buf` is large enough. PositionSnapshot is the
/// largest variant at up to 330 bytes (length(4) + tag(1) + account(4) +
/// count(1) + 16*(currency(4)+free(8)+reserved(8))). 512 bytes is generous.
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
        ResponseKind::Challenge { nonce } => {
            buf[pos] = TAG_CHALLENGE;
            pos += 1;
            buf[pos..pos + 32].copy_from_slice(nonce);
            pos += 32;
        }
        ResponseKind::AuthFailed => {
            buf[pos] = TAG_AUTH_FAILED;
            pos += 1;
        }
        ResponseKind::ServerBusy => {
            buf[pos] = TAG_SERVER_BUSY;
            pos += 1;
        }
        ResponseKind::StatsHeader {
            active_connections,
            events_processed,
            journal_sequence,
        } => {
            buf[pos] = TAG_STATS_HEADER;
            pos += 1;
            le::put_u64(&mut buf[pos..], *active_connections);
            pos += 8;
            le::put_u64(&mut buf[pos..], *events_processed);
            pos += 8;
            le::put_u64(&mut buf[pos..], *journal_sequence);
            pos += 8;
        }
        ResponseKind::BookSnapshotBegin {
            symbol,
            last_applied_seq,
        } => {
            buf[pos] = TAG_BOOK_SNAPSHOT_BEGIN;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], *last_applied_seq);
            pos += 8;
        }
        ResponseKind::BookSnapshotLevel {
            symbol,
            side,
            price,
            qty,
            order_count,
        } => {
            buf[pos] = TAG_BOOK_SNAPSHOT_LEVEL;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            buf[pos] = le::encode_side(*side);
            pos += 1;
            le::put_u64(&mut buf[pos..], price.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], *qty);
            pos += 8;
            le::put_u32(&mut buf[pos..], *order_count);
            pos += 4;
        }
        ResponseKind::BookSnapshotEnd {
            symbol,
            level_count,
        } => {
            buf[pos] = TAG_BOOK_SNAPSHOT_END;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], *level_count);
            pos += 4;
        }
        ResponseKind::SnapshotComplete { last_applied_seq } => {
            buf[pos] = TAG_SNAPSHOT_COMPLETE;
            pos += 1;
            le::put_u64(&mut buf[pos..], *last_applied_seq);
            pos += 8;
        }
        ResponseKind::PositionSnapshot {
            account,
            balances,
            count,
        } => {
            buf[pos] = TAG_POSITION_SNAPSHOT;
            pos += 1;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            buf[pos] = *count;
            pos += 1;
            // Each entry: currency(4) + free(8) + reserved(8) = 20 bytes.
            let n = std::cmp::min(*count as usize, balances.len());
            for entry in &balances[..n] {
                le::put_u32(&mut buf[pos..], entry.currency.0);
                pos += 4;
                le::put_u64(&mut buf[pos..], entry.free);
                pos += 8;
                le::put_u64(&mut buf[pos..], entry.reserved);
                pos += 8;
            }
        }
        ResponseKind::RequestSeqHwm { hwm } => {
            buf[pos] = TAG_REQUEST_SEQ_HWM;
            pos += 1;
            le::put_u64(&mut buf[pos..], *hwm);
            pos += 8;
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
        TAG_CHALLENGE => {
            if payload.len() < 32 {
                return Err(ProtocolError::Truncated);
            }
            let mut nonce = [0u8; 32];
            nonce.copy_from_slice(&payload[..32]);
            Ok(ResponseKind::Challenge { nonce })
        }
        TAG_AUTH_FAILED => Ok(ResponseKind::AuthFailed),
        TAG_SERVER_BUSY => Ok(ResponseKind::ServerBusy),
        TAG_PLACED
        | TAG_FILL
        | TAG_CANCELLED
        | TAG_TRIGGERED
        | TAG_REJECTED
        | TAG_REPLACED
        | TAG_INSTRUMENT_STATUS_CHANGED => {
            let report = decode_execution_report(tag, payload)?;
            Ok(ResponseKind::Report(report))
        }
        TAG_STATS_HEADER => {
            // active_connections(8) + events_processed(8) + journal_sequence(8) = 24
            if payload.len() < 24 {
                return Err(ProtocolError::Truncated);
            }
            Ok(ResponseKind::StatsHeader {
                active_connections: le::get_u64(&payload[0..]),
                events_processed: le::get_u64(&payload[8..]),
                journal_sequence: le::get_u64(&payload[16..]),
            })
        }
        TAG_BOOK_SNAPSHOT_BEGIN => {
            // symbol(4) + last_applied_seq(8) = 12
            if payload.len() < 12 {
                return Err(ProtocolError::Truncated);
            }
            Ok(ResponseKind::BookSnapshotBegin {
                symbol: Symbol(le::get_u32(&payload[0..])),
                last_applied_seq: le::get_u64(&payload[4..]),
            })
        }
        TAG_BOOK_SNAPSHOT_LEVEL => {
            // symbol(4) + side(1) + price(8) + qty(8) + order_count(4) = 25
            if payload.len() < 25 {
                return Err(ProtocolError::Truncated);
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let side = le::decode_side(payload[4]).ok_or(ProtocolError::InvalidField("side"))?;
            let price = NonZeroU64::new(le::get_u64(&payload[5..]))
                .ok_or(ProtocolError::InvalidField("snapshot level price is zero"))?;
            let qty = le::get_u64(&payload[13..]);
            let order_count = le::get_u32(&payload[21..]);
            Ok(ResponseKind::BookSnapshotLevel {
                symbol,
                side,
                price: Price(price),
                qty,
                order_count,
            })
        }
        TAG_BOOK_SNAPSHOT_END => {
            // symbol(4) + level_count(4) = 8
            if payload.len() < 8 {
                return Err(ProtocolError::Truncated);
            }
            Ok(ResponseKind::BookSnapshotEnd {
                symbol: Symbol(le::get_u32(&payload[0..])),
                level_count: le::get_u32(&payload[4..]),
            })
        }
        TAG_SNAPSHOT_COMPLETE => {
            // last_applied_seq(8)
            if payload.len() < 8 {
                return Err(ProtocolError::Truncated);
            }
            Ok(ResponseKind::SnapshotComplete {
                last_applied_seq: le::get_u64(&payload[0..]),
            })
        }
        TAG_POSITION_SNAPSHOT => {
            // account(4) + count(1) + count*(currency(4) + free(8) + reserved(8))
            if payload.len() < 5 {
                return Err(ProtocolError::Truncated);
            }
            let account = AccountId(le::get_u32(&payload[0..]));
            let count = payload[4];
            if count > 16 {
                return Err(ProtocolError::InvalidField("position snapshot count > 16"));
            }
            let needed = 5 + (count as usize) * 20;
            if payload.len() < needed {
                return Err(ProtocolError::Truncated);
            }
            let mut balances = [AccountBalance::ZERO; 16];
            for (i, entry) in balances.iter_mut().enumerate().take(count as usize) {
                let off = 5 + i * 20;
                *entry = AccountBalance {
                    currency: CurrencyId(le::get_u32(&payload[off..])),
                    free: le::get_u64(&payload[off + 4..]),
                    reserved: le::get_u64(&payload[off + 12..]),
                };
            }
            Ok(ResponseKind::PositionSnapshot {
                account,
                balances,
                count,
            })
        }
        TAG_REQUEST_SEQ_HWM => {
            if payload.len() < 8 {
                return Err(ProtocolError::Truncated);
            }
            Ok(ResponseKind::RequestSeqHwm {
                hwm: le::get_u64(&payload[0..]),
            })
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
        OrderType::Limit { price, post_only } => {
            buf[pos] = if post_only {
                ORDER_TYPE_LIMIT_POST_ONLY
            } else {
                ORDER_TYPE_LIMIT
            };
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

    // Conditional expiry_ns: only written for GTD orders to avoid inflating
    // every order's wire footprint by 8 bytes.
    if order.time_in_force == TimeInForce::GTD {
        le::put_u64(&mut buf[pos..], order.expiry_ns);
        pos += 8;
    }

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
        ORDER_TYPE_LIMIT | ORDER_TYPE_LIMIT_POST_ONLY => {
            if buf.len() < pos + 8 {
                return Err(ProtocolError::Truncated);
            }
            let price = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(ProtocolError::InvalidField("limit price is zero"))?;
            pos += 8;
            OrderType::Limit {
                price: Price(price),
                post_only: order_type_tag == ORDER_TYPE_LIMIT_POST_ONLY,
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

    // Conditional expiry_ns: only present for GTD orders.
    let expiry_ns = if time_in_force == TimeInForce::GTD {
        if buf.len() < pos + 8 {
            return Err(ProtocolError::Truncated);
        }
        let v = le::get_u64(&buf[pos..]);
        pos += 8;
        v
    } else {
        0
    };

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
            expiry_ns,
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
            symbol,
            account,
            side,
            price,
            quantity,
        } => {
            buf[pos] = TAG_PLACED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
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
            symbol,
            maker_account,
            taker_account,
            price,
            quantity,
            maker_fee,
            taker_fee,
        } => {
            buf[pos] = TAG_FILL;
            pos += 1;
            le::put_u64(&mut buf[pos..], maker_order_id.0);
            pos += 8;
            le::put_u64(&mut buf[pos..], taker_order_id.0);
            pos += 8;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], maker_account.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], taker_account.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], price.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], quantity.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], *maker_fee as u64);
            pos += 8;
            le::put_u64(&mut buf[pos..], *taker_fee as u64);
            pos += 8;
        }
        ExecutionReport::Cancelled {
            order_id,
            symbol,
            account,
            remaining_quantity,
        } => {
            buf[pos] = TAG_CANCELLED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], remaining_quantity.get());
            pos += 8;
        }
        ExecutionReport::Triggered {
            order_id,
            symbol,
            account,
            trigger_price,
        } => {
            buf[pos] = TAG_TRIGGERED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], trigger_price.get());
            pos += 8;
        }
        ExecutionReport::Rejected {
            order_id,
            symbol,
            account,
            reason,
        } => {
            buf[pos] = TAG_REJECTED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            buf[pos] = encode_reject_reason(*reason);
            pos += 1;
        }
        ExecutionReport::Replaced {
            order_id,
            symbol,
            account,
            side,
            old_price,
            new_price,
            old_remaining,
            new_remaining,
        } => {
            buf[pos] = TAG_REPLACED;
            pos += 1;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            buf[pos] = le::encode_side(*side);
            pos += 1;
            le::put_u64(&mut buf[pos..], old_price.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], new_price.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], old_remaining.get());
            pos += 8;
            le::put_u64(&mut buf[pos..], new_remaining.get());
            pos += 8;
        }
        ExecutionReport::InstrumentStatusChanged { symbol, status } => {
            buf[pos] = TAG_INSTRUMENT_STATUS_CHANGED;
            pos += 1;
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            buf[pos] = *status as u8;
            pos += 1;
        }
    }

    pos
}

/// Decode an `ExecutionReport` from tag + payload.
fn decode_execution_report(tag: u8, payload: &[u8]) -> Result<ExecutionReport, ProtocolError> {
    match tag {
        TAG_PLACED => {
            // order_id(8) + symbol(4) + account(4) + side(1) + price(8) + quantity(8) = 33
            if payload.len() < 33 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let symbol = Symbol(le::get_u32(&payload[8..]));
            let account = AccountId(le::get_u32(&payload[12..]));
            let side = le::decode_side(payload[16]).ok_or(ProtocolError::InvalidField("side"))?;
            let price = NonZeroU64::new(le::get_u64(&payload[17..]))
                .ok_or(ProtocolError::InvalidField("placed price is zero"))?;
            let quantity = NonZeroU64::new(le::get_u64(&payload[25..]))
                .ok_or(ProtocolError::InvalidField("placed quantity is zero"))?;
            Ok(ExecutionReport::Placed {
                order_id,
                symbol,
                account,
                side,
                price: Price(price),
                quantity: Quantity(quantity),
            })
        }
        TAG_FILL => {
            // maker_id(8) + taker_id(8) + symbol(4) + maker_acct(4) + taker_acct(4) +
            // price(8) + qty(8) + maker_fee(8) + taker_fee(8) = 60
            if payload.len() < 60 {
                return Err(ProtocolError::Truncated);
            }
            let maker_order_id = OrderId(le::get_u64(&payload[0..]));
            let taker_order_id = OrderId(le::get_u64(&payload[8..]));
            let symbol = Symbol(le::get_u32(&payload[16..]));
            let maker_account = AccountId(le::get_u32(&payload[20..]));
            let taker_account = AccountId(le::get_u32(&payload[24..]));
            let price = NonZeroU64::new(le::get_u64(&payload[28..]))
                .ok_or(ProtocolError::InvalidField("fill price is zero"))?;
            let quantity = NonZeroU64::new(le::get_u64(&payload[36..]))
                .ok_or(ProtocolError::InvalidField("fill quantity is zero"))?;
            let maker_fee = le::get_u64(&payload[44..]) as i64;
            let taker_fee = le::get_u64(&payload[52..]) as i64;
            Ok(ExecutionReport::Fill {
                maker_order_id,
                taker_order_id,
                symbol,
                maker_account,
                taker_account,
                price: Price(price),
                quantity: Quantity(quantity),
                maker_fee,
                taker_fee,
            })
        }
        TAG_CANCELLED => {
            // order_id(8) + symbol(4) + account(4) + remaining(8) = 24
            if payload.len() < 24 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let symbol = Symbol(le::get_u32(&payload[8..]));
            let account = AccountId(le::get_u32(&payload[12..]));
            let remaining = NonZeroU64::new(le::get_u64(&payload[16..]))
                .ok_or(ProtocolError::InvalidField("cancelled remaining is zero"))?;
            Ok(ExecutionReport::Cancelled {
                order_id,
                symbol,
                account,
                remaining_quantity: Quantity(remaining),
            })
        }
        TAG_TRIGGERED => {
            // order_id(8) + symbol(4) + account(4) + trigger_price(8) = 24
            if payload.len() < 24 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let symbol = Symbol(le::get_u32(&payload[8..]));
            let account = AccountId(le::get_u32(&payload[12..]));
            let trigger_price = NonZeroU64::new(le::get_u64(&payload[16..]))
                .ok_or(ProtocolError::InvalidField("trigger price is zero"))?;
            Ok(ExecutionReport::Triggered {
                order_id,
                symbol,
                account,
                trigger_price: Price(trigger_price),
            })
        }
        TAG_REJECTED => {
            // order_id(8) + symbol(4) + account(4) + reason(1) = 17
            if payload.len() < 17 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let symbol = Symbol(le::get_u32(&payload[8..]));
            let account = AccountId(le::get_u32(&payload[12..]));
            let reason = decode_reject_reason(payload[16])?;
            Ok(ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason,
            })
        }
        TAG_REPLACED => {
            // order_id(8) + symbol(4) + account(4) + side(1) + old_price(8) + new_price(8) +
            // old_remaining(8) + new_remaining(8) = 49
            if payload.len() < 49 {
                return Err(ProtocolError::Truncated);
            }
            let order_id = OrderId(le::get_u64(&payload[0..]));
            let symbol = Symbol(le::get_u32(&payload[8..]));
            let account = AccountId(le::get_u32(&payload[12..]));
            let side = le::decode_side(payload[16]).ok_or(ProtocolError::InvalidField("side"))?;
            let old_price = NonZeroU64::new(le::get_u64(&payload[17..]))
                .ok_or(ProtocolError::InvalidField("replaced old_price is zero"))?;
            let new_price = NonZeroU64::new(le::get_u64(&payload[25..]))
                .ok_or(ProtocolError::InvalidField("replaced new_price is zero"))?;
            let old_remaining = NonZeroU64::new(le::get_u64(&payload[33..])).ok_or(
                ProtocolError::InvalidField("replaced old_remaining is zero"),
            )?;
            let new_remaining = NonZeroU64::new(le::get_u64(&payload[41..])).ok_or(
                ProtocolError::InvalidField("replaced new_remaining is zero"),
            )?;
            Ok(ExecutionReport::Replaced {
                order_id,
                symbol,
                account,
                side,
                old_price: Price(old_price),
                new_price: Price(new_price),
                old_remaining: Quantity(old_remaining),
                new_remaining: Quantity(new_remaining),
            })
        }
        TAG_INSTRUMENT_STATUS_CHANGED => {
            // symbol(4) + status(1) = 5
            if payload.len() < 5 {
                return Err(ProtocolError::Truncated);
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let status = match payload[4] {
                0 => InstrumentStatus::Enabled,
                1 => InstrumentStatus::Disabled,
                2 => InstrumentStatus::Removed,
                _ => return Err(ProtocolError::InvalidField("instrument status")),
            };
            Ok(ExecutionReport::InstrumentStatusChanged { symbol, status })
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
        RejectReason::TradingHalted => REJECT_TRADING_HALTED,
        RejectReason::OutsidePriceBand => REJECT_OUTSIDE_PRICE_BAND,
        RejectReason::UnknownOrder => REJECT_UNKNOWN_ORDER,
        RejectReason::PriceWouldCross => REJECT_PRICE_WOULD_CROSS,
        RejectReason::PostOnlyWouldCross => REJECT_POST_ONLY_WOULD_CROSS,
        RejectReason::HasRestingOrders => REJECT_HAS_RESTING_ORDERS,
        RejectReason::DuplicateRequest => REJECT_DUPLICATE_REQUEST,
        RejectReason::ReplicaDisconnected => REJECT_REPLICA_DISCONNECTED,
        RejectReason::InvalidExpiry => REJECT_INVALID_EXPIRY,
        RejectReason::InstrumentDisabled => REJECT_INSTRUMENT_DISABLED,
        RejectReason::ExceedsMaxOpenOrders => REJECT_EXCEEDS_MAX_OPEN_ORDERS,
        RejectReason::ExceedsOrderRate => REJECT_EXCEEDS_ORDER_RATE,
        RejectReason::Superseded => REJECT_SUPERSEDED,
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
        REJECT_TRADING_HALTED => Ok(RejectReason::TradingHalted),
        REJECT_OUTSIDE_PRICE_BAND => Ok(RejectReason::OutsidePriceBand),
        REJECT_UNKNOWN_ORDER => Ok(RejectReason::UnknownOrder),
        REJECT_PRICE_WOULD_CROSS => Ok(RejectReason::PriceWouldCross),
        REJECT_POST_ONLY_WOULD_CROSS => Ok(RejectReason::PostOnlyWouldCross),
        REJECT_HAS_RESTING_ORDERS => Ok(RejectReason::HasRestingOrders),
        REJECT_DUPLICATE_REQUEST => Ok(RejectReason::DuplicateRequest),
        REJECT_REPLICA_DISCONNECTED => Ok(RejectReason::ReplicaDisconnected),
        REJECT_INVALID_EXPIRY => Ok(RejectReason::InvalidExpiry),
        REJECT_INSTRUMENT_DISABLED => Ok(RejectReason::InstrumentDisabled),
        REJECT_EXCEEDS_MAX_OPEN_ORDERS => Ok(RejectReason::ExceedsMaxOpenOrders),
        REJECT_EXCEEDS_ORDER_RATE => Ok(RejectReason::ExceedsOrderRate),
        REJECT_SUPERSEDED => Ok(RejectReason::Superseded),
        _ => Err(ProtocolError::InvalidField("reject reason")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_types::types::{SelfTradeProtection, Side, TimeInForce};

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
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: Quantity(nz(10)),
                    stp: SelfTradeProtection::CancelNewest,
                    expiry_ns: 0,
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
                    expiry_ns: 0,
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
                    expiry_ns: 0,
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
                    expiry_ns: 0,
                },
            },
            Request::CancelOrder {
                symbol: Symbol(1),
                account: AccountId(42),
                order_id: OrderId(100),
            },
            Request::CancelAll {
                account: AccountId(42),
            },
            Request::Heartbeat,
            Request::ChallengeResponse {
                signature: [0xAA; 64],
                public_key: [0xBB; 32],
            },
            Request::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(3),
                    base: CurrencyId(5),
                    quote: CurrencyId(6),
                },
            },
            Request::Deposit {
                account: AccountId(1),
                currency: CurrencyId(2),
                amount: 1_000_000,
            },
            Request::Withdraw {
                account: AccountId(1),
                currency: CurrencyId(2),
                amount: 500_000,
            },
            Request::SetRiskLimits {
                symbol: Symbol(1),
                limits: RiskLimits {
                    max_order_qty: Some(Quantity(nz(1000))),
                    max_order_notional: Some(500_000),
                },
            },
            Request::SetRiskLimits {
                symbol: Symbol(2),
                limits: RiskLimits {
                    max_order_qty: None,
                    max_order_notional: None,
                },
            },
            Request::SetRiskLimits {
                symbol: Symbol(3),
                limits: RiskLimits {
                    max_order_qty: Some(Quantity(nz(100))),
                    max_order_notional: None,
                },
            },
            Request::SetRiskLimits {
                symbol: Symbol(4),
                limits: RiskLimits {
                    max_order_qty: None,
                    max_order_notional: Some(999_999),
                },
            },
            Request::SetCircuitBreaker {
                symbol: Symbol(1),
                config: CircuitBreakerConfig {
                    price_band_lower: Some(Price(nz(900))),
                    price_band_upper: Some(Price(nz(1100))),
                    halted: false,
                },
            },
            Request::SetCircuitBreaker {
                symbol: Symbol(2),
                config: CircuitBreakerConfig {
                    price_band_lower: None,
                    price_band_upper: None,
                    halted: true,
                },
            },
            Request::CancelReplace {
                symbol: Symbol(1),
                account: AccountId(42),
                order_id: OrderId(42),
                new_price: Price(nz(5500)),
                new_quantity: Quantity(nz(30)),
            },
            Request::SetFeeSchedule {
                symbol: Symbol(1),
                schedule: FeeSchedule {
                    maker_fee_bps: 5,
                    taker_fee_bps: 10,
                },
            },
            Request::QueryStats,
            Request::QueryRequestSeq,
            Request::EndOfDay,
            // GTD order — exercises conditional expiry_ns encoding.
            Request::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    id: OrderId(200),
                    account: AccountId(42),
                    side: Side::Buy,
                    order_type: OrderType::Limit {
                        price: Price(nz(5000)),
                        post_only: false,
                    },
                    time_in_force: TimeInForce::GTD,
                    quantity: Quantity(nz(10)),
                    stp: SelfTradeProtection::CancelNewest,
                    expiry_ns: 1_800_000_000_000_000_000,
                },
            },
            Request::DisableInstrument { symbol: Symbol(1) },
            Request::EnableInstrument { symbol: Symbol(2) },
            Request::RemoveInstrument { symbol: Symbol(3) },
            Request::Subscribe {
                symbols: [
                    Symbol(10),
                    Symbol(20),
                    Symbol(0),
                    Symbol(0),
                    Symbol(0),
                    Symbol(0),
                    Symbol(0),
                    Symbol(0),
                ],
                count: 2,
            },
            Request::QueryPosition {
                account: AccountId(42),
            },
        ]
    }

    fn make_responses() -> Vec<ResponseKind> {
        vec![
            ResponseKind::Report(ExecutionReport::Placed {
                order_id: OrderId(1),
                symbol: Symbol(7),
                account: AccountId(42),
                side: Side::Buy,
                price: Price(nz(100)),
                quantity: Quantity(nz(50)),
            }),
            ResponseKind::Report(ExecutionReport::Fill {
                maker_order_id: OrderId(1),
                taker_order_id: OrderId(2),
                symbol: Symbol(3),
                maker_account: AccountId(10),
                taker_account: AccountId(20),
                price: Price(nz(100)),
                quantity: Quantity(nz(10)),
                maker_fee: 5,
                taker_fee: 10,
            }),
            ResponseKind::Report(ExecutionReport::Cancelled {
                order_id: OrderId(3),
                symbol: Symbol(2),
                account: AccountId(10),
                remaining_quantity: Quantity(nz(5)),
            }),
            ResponseKind::Report(ExecutionReport::Triggered {
                order_id: OrderId(4),
                symbol: Symbol(4),
                account: AccountId(33),
                trigger_price: Price(nz(200)),
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(5),
                symbol: Symbol(6),
                account: AccountId(10),
                reason: RejectReason::NoLiquidity,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(6),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::FOKCannotFill,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(7),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::InsufficientBalance,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(8),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::UnknownAccount,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(9),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::UnknownSymbol,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(10),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::SelfTradePrevented,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(11),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::DuplicateOrderId,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(12),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::ExceedsMaxOrderQty,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(13),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::ExceedsMaxNotional,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(14),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::TradingHalted,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(15),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::OutsidePriceBand,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(16),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::UnknownOrder,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(17),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::PriceWouldCross,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(18),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::PostOnlyWouldCross,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(19),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::HasRestingOrders,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(20),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::DuplicateRequest,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(21),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::ReplicaDisconnected,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(22),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::InvalidExpiry,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(23),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::InstrumentDisabled,
            }),
            ResponseKind::Report(ExecutionReport::Rejected {
                order_id: OrderId(24),
                symbol: Symbol(1),
                account: AccountId(10),
                reason: RejectReason::Superseded,
            }),
            ResponseKind::Report(ExecutionReport::InstrumentStatusChanged {
                symbol: Symbol(1),
                status: InstrumentStatus::Disabled,
            }),
            ResponseKind::Report(ExecutionReport::InstrumentStatusChanged {
                symbol: Symbol(2),
                status: InstrumentStatus::Enabled,
            }),
            ResponseKind::Report(ExecutionReport::InstrumentStatusChanged {
                symbol: Symbol(3),
                status: InstrumentStatus::Removed,
            }),
            ResponseKind::Report(ExecutionReport::Replaced {
                order_id: OrderId(42),
                symbol: Symbol(5),
                account: AccountId(99),
                side: Side::Buy,
                old_price: Price(nz(5000)),
                new_price: Price(nz(5500)),
                old_remaining: Quantity(nz(50)),
                new_remaining: Quantity(nz(30)),
            }),
            ResponseKind::EngineError,
            ResponseKind::BatchEnd,
            ResponseKind::ServerReady,
            ResponseKind::Heartbeat,
            ResponseKind::Challenge { nonce: [0xCC; 32] },
            ResponseKind::AuthFailed,
            ResponseKind::ServerBusy,
            ResponseKind::StatsHeader {
                active_connections: 5,
                events_processed: 1_234_567,
                journal_sequence: 1_234_567,
            },
            ResponseKind::BookSnapshotBegin {
                symbol: Symbol(42),
                last_applied_seq: 9_876_543,
            },
            ResponseKind::BookSnapshotLevel {
                symbol: Symbol(42),
                side: Side::Buy,
                price: Price(nz(50_000)),
                qty: 1_500,
                order_count: 12,
            },
            ResponseKind::BookSnapshotEnd {
                symbol: Symbol(42),
                level_count: 25,
            },
            ResponseKind::SnapshotComplete {
                last_applied_seq: 9_876_543,
            },
            ResponseKind::PositionSnapshot {
                account: AccountId(42),
                balances: {
                    let mut b = [AccountBalance::ZERO; 16];
                    b[0] = AccountBalance {
                        currency: CurrencyId(1),
                        free: 100_000,
                        reserved: 25_000,
                    };
                    b[1] = AccountBalance {
                        currency: CurrencyId(2),
                        free: 50_000,
                        reserved: 10_000,
                    };
                    b
                },
                count: 2,
            },
            ResponseKind::RequestSeqHwm { hwm: 0 },
            ResponseKind::RequestSeqHwm {
                hwm: 12_345_678_900,
            },
        ]
    }

    #[test]
    fn request_round_trip_all_variants() {
        let requests = make_requests();
        let mut buf = [0u8; 256];

        for (i, request) in requests.iter().enumerate() {
            let seq = (i as u64) + 1;
            let written = encode_request(request, seq, &mut buf).unwrap();
            // Skip the 4-byte length prefix for decode.
            let (decoded_seq, decoded) = decode_request(&buf[4..written]).unwrap();
            assert_eq!(decoded_seq, seq, "seq mismatch for variant {i}");
            assert_eq!(&decoded, request, "request variant {i}");
        }
    }

    #[test]
    fn response_round_trip_all_variants() {
        let responses = make_responses();
        let mut buf = [0u8; 512];

        for (i, response) in responses.iter().enumerate() {
            let written = encode_response(response, &mut buf).unwrap();
            let decoded = decode_response(&buf[4..written]).unwrap();
            assert_eq!(&decoded, response, "response variant {i}");
        }
    }

    #[test]
    fn truncated_request_detected() {
        // Empty buffer — not enough for seq(8) + tag(1).
        let result = decode_request(&[]);
        assert!(matches!(result, Err(ProtocolError::Truncated)));

        // Only 3 bytes — still too short for seq(8) + tag(1).
        let result = decode_request(&[0; 3]);
        assert!(matches!(result, Err(ProtocolError::Truncated)));

        // seq(8) + tag present but payload too short for SubmitOrder.
        let mut short = [0u8; 11];
        short[8] = TAG_SUBMIT_ORDER;
        let result = decode_request(&short);
        assert!(matches!(result, Err(ProtocolError::Truncated)));

        // ChallengeResponse needs sig(64) + pubkey(32) = 96 bytes
        // after the tag. 95 bytes after the tag must be rejected.
        let mut short = [0u8; 9 + 95];
        short[8] = TAG_CHALLENGE_RESPONSE;
        let result = decode_request(&short);
        assert!(matches!(result, Err(ProtocolError::Truncated)));
        // Exactly 96 bytes after the tag is the boundary — must succeed.
        let mut ok_buf = [0u8; 9 + 96];
        ok_buf[8] = TAG_CHALLENGE_RESPONSE;
        assert!(decode_request(&ok_buf).is_ok());
    }

    #[test]
    fn unknown_request_tag_detected() {
        // seq(8) + unknown tag byte.
        let mut buf = [0u8; 9];
        buf[8] = 255;
        let result = decode_request(&buf);
        assert!(matches!(result, Err(ProtocolError::UnknownTag(255))));
    }

    #[test]
    fn truncated_response_detected() {
        let result = decode_response(&[]);
        assert!(matches!(result, Err(ProtocolError::Truncated)));

        // Challenge needs nonce(32) bytes after the tag. 31 bytes
        // must be rejected.
        let mut short = [0u8; 1 + 31];
        short[0] = TAG_CHALLENGE;
        let result = decode_response(&short);
        assert!(matches!(result, Err(ProtocolError::Truncated)));
        // Exactly 32 bytes after the tag is the boundary — must succeed.
        let mut ok_buf = [0u8; 1 + 32];
        ok_buf[0] = TAG_CHALLENGE;
        assert!(decode_response(&ok_buf).is_ok());
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
            account: AccountId(1),
            order_id: OrderId(42),
        };
        let mut buf = [0u8; 136];
        let written = encode_request(&request, 42, &mut buf).unwrap();

        let length = le::get_u32(&buf[0..]) as usize;
        assert_eq!(length, written - 4, "length prefix should be total - 4");
    }

    #[test]
    fn seq_zero_round_trips() {
        // Heartbeat and ChallengeResponse use seq=0.
        let request = Request::Heartbeat;
        let mut buf = [0u8; 136];
        let written = encode_request(&request, 0, &mut buf).unwrap();
        let (decoded_seq, decoded) = decode_request(&buf[4..written]).unwrap();
        assert_eq!(decoded_seq, 0);
        assert_eq!(decoded, request);
    }

    #[test]
    fn seq_max_round_trips() {
        let request = Request::Deposit {
            account: AccountId(1),
            currency: CurrencyId(2),
            amount: 100,
        };
        let mut buf = [0u8; 136];
        let written = encode_request(&request, u64::MAX, &mut buf).unwrap();
        let (decoded_seq, decoded) = decode_request(&buf[4..written]).unwrap();
        assert_eq!(decoded_seq, u64::MAX);
        assert_eq!(decoded, request);
    }

    #[test]
    fn length_prefix_includes_seq_bytes() {
        // The length field must include seq(8) + tag(1) + payload.
        let request = Request::Heartbeat;
        let mut buf = [0u8; 136];
        let written = encode_request(&request, 0, &mut buf).unwrap();
        let length = le::get_u32(&buf[0..]) as usize;
        // Heartbeat has no payload, so length = seq(8) + tag(1) = 9.
        assert_eq!(length, 9);
        assert_eq!(written, 4 + 9); // 4-byte prefix + 9 payload
    }

    #[test]
    fn subscribe_request_zero_symbols_roundtrip() {
        // count=0 means wildcard subscribe (all symbols).
        let request = Request::Subscribe {
            symbols: [Symbol(0); 8],
            count: 0,
        };
        let mut buf = [0u8; 136];
        let written = encode_request(&request, 1, &mut buf).unwrap();
        let (seq, decoded) = decode_request(&buf[4..written]).unwrap();
        assert_eq!(seq, 1);
        if let Request::Subscribe { count, .. } = decoded {
            assert_eq!(count, 0);
        } else {
            panic!("expected Subscribe, got {decoded:?}");
        }
    }

    #[test]
    fn subscribe_request_max_symbols_roundtrip() {
        // count=8, all symbol slots filled with distinct values.
        let symbols = [
            Symbol(1),
            Symbol(2),
            Symbol(3),
            Symbol(4),
            Symbol(5),
            Symbol(6),
            Symbol(7),
            Symbol(8),
        ];
        let request = Request::Subscribe { symbols, count: 8 };
        let mut buf = [0u8; 136];
        let written = encode_request(&request, 1, &mut buf).unwrap();
        let (_seq, decoded) = decode_request(&buf[4..written]).unwrap();
        if let Request::Subscribe {
            symbols: decoded_syms,
            count,
        } = decoded
        {
            assert_eq!(count, 8);
            for i in 0..8 {
                assert_eq!(decoded_syms[i], symbols[i], "symbol mismatch at index {i}");
            }
        } else {
            panic!("expected Subscribe, got {decoded:?}");
        }
    }

    #[test]
    fn position_snapshot_empty_roundtrip() {
        let response = ResponseKind::PositionSnapshot {
            account: AccountId(99),
            balances: [AccountBalance::ZERO; 16],
            count: 0,
        };
        let mut buf = [0u8; 512];
        let written = encode_response(&response, &mut buf).unwrap();
        let decoded = decode_response(&buf[4..written]).unwrap();
        if let ResponseKind::PositionSnapshot { account, count, .. } = decoded {
            assert_eq!(account, AccountId(99));
            assert_eq!(count, 0);
        } else {
            panic!("expected PositionSnapshot, got {decoded:?}");
        }
    }

    #[test]
    fn position_snapshot_max_currencies_roundtrip() {
        // Fill all 16 slots with distinct values.
        let mut balances = [AccountBalance::ZERO; 16];
        for (i, entry) in balances.iter_mut().enumerate() {
            *entry = AccountBalance {
                currency: CurrencyId((i + 1) as u32),
                free: (i as u64 + 1) * 1000,
                reserved: (i as u64 + 1) * 100,
            };
        }
        let response = ResponseKind::PositionSnapshot {
            account: AccountId(7),
            balances,
            count: 16,
        };
        let mut buf = [0u8; 512];
        let written = encode_response(&response, &mut buf).unwrap();
        let decoded = decode_response(&buf[4..written]).unwrap();
        if let ResponseKind::PositionSnapshot {
            account,
            balances: dec_bal,
            count,
        } = decoded
        {
            assert_eq!(account, AccountId(7));
            assert_eq!(count, 16);
            for i in 0..16 {
                assert_eq!(dec_bal[i], balances[i], "balance mismatch at {i}");
            }
        } else {
            panic!("expected PositionSnapshot, got {decoded:?}");
        }
    }

    #[test]
    fn position_snapshot_count_over_16_rejected() {
        // Manually build a payload with count=17 — should be rejected.
        // Layout: tag(1) + account(4) + count(1) = 6 bytes minimum.
        let mut payload = vec![0u8; 6];
        payload[0] = TAG_POSITION_SNAPSHOT;
        le::put_u32(&mut payload[1..], 1); // account
        payload[5] = 17; // count > 16
        let result = decode_response(&payload);
        assert!(
            matches!(result, Err(ProtocolError::InvalidField(_))),
            "expected InvalidField, got {result:?}"
        );
    }
}
