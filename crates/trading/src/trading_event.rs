//! `TradingEvent` — the `melin-app` `AppEvent` for the Melin trading engine.
//!
//! Mirrors the state-mutating and read-only query variants of the current
//! `JournalEvent`, minus transport-intrinsic variants (GenesisHash,
//! Checkpoint, Tick) which stay with the transport. Phase 2 of the
//! transport/app split will unify the journal's wire format around
//! `JournalEvent<TradingEvent>`; for Phase 1 this enum and its codec live
//! alongside the existing `JournalEvent` with deliberately independent
//! encoding — both are reachable from tests so we can prove the
//! `Application` trait round-trips.
//!
//! Wire layout (payload-only — transport supplies framing):
//!
//! | Byte | Field      | Purpose                                       |
//! |------|------------|-----------------------------------------------|
//! | 0    | tag        | Variant discriminant (see `TAG_*` constants)  |
//! | 1..  | payload    | Per-variant fields, little-endian             |

use std::num::NonZeroU64;

use melin_app::{AppEvent, CodecError};

use crate::le;
use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, FeeSchedule, InstrumentSpec, Order, OrderId,
    OrderType, Price, Quantity, RiskLimits, SelfTradeProtection, Side, Symbol, TimeInForce,
};

// Variant tag space — numbered 1.. to leave 0 as a reserved "invalid" guard.
// Kept close to the existing journal-codec tag ordering so Phase 2's
// unification is a diff rather than a renumber.
const TAG_ADD_INSTRUMENT: u8 = 1;
const TAG_DEPOSIT: u8 = 2;
const TAG_SUBMIT_ORDER: u8 = 3;
const TAG_CANCEL_ORDER: u8 = 4;
const TAG_SET_RISK_LIMITS: u8 = 5;
const TAG_CANCEL_ALL: u8 = 6;
const TAG_SET_CIRCUIT_BREAKER: u8 = 7;
const TAG_CANCEL_REPLACE: u8 = 8;
const TAG_SET_FEE_SCHEDULE: u8 = 9;
const TAG_PROVISION_ACCOUNT: u8 = 10;
const TAG_WITHDRAW: u8 = 11;
const TAG_END_OF_DAY: u8 = 12;
const TAG_DISABLE_INSTRUMENT: u8 = 13;
const TAG_ENABLE_INSTRUMENT: u8 = 14;
const TAG_REMOVE_INSTRUMENT: u8 = 15;
const TAG_QUERY_STATS: u8 = 16;
const TAG_QUERY_POSITION: u8 = 17;

// Per-OrderType nested tag space inside SubmitOrder's payload.
const ORDER_TYPE_MARKET: u8 = 0;
const ORDER_TYPE_LIMIT: u8 = 1;
const ORDER_TYPE_STOP: u8 = 2;
const ORDER_TYPE_STOP_LIMIT: u8 = 3;
const ORDER_TYPE_LIMIT_POST_ONLY: u8 = 4;

/// Application-level events for the Melin trading engine.
///
/// `Copy` so the event can live inside the disruptor ring slot without
/// heap indirection — the ring publishes by byte copy. State-mutating
/// variants are journaled; the two `Query*` variants are read-only and
/// bypass the journal (see [`AppEvent::is_query`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingEvent {
    /// Register a new instrument with its currency pair.
    AddInstrument { spec: InstrumentSpec },
    /// Credit funds to an account.
    Deposit {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    /// Submit an order for matching.
    SubmitOrder { symbol: Symbol, order: Order },
    /// Cancel a resting or pending-stop order.
    CancelOrder {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
    },
    /// Set per-instrument fat-finger risk limits.
    SetRiskLimits { symbol: Symbol, limits: RiskLimits },
    /// Cancel every resting order and pending stop for `account` across
    /// all instruments (kill switch).
    CancelAll { account: AccountId },
    /// Set circuit breaker configuration for an instrument.
    SetCircuitBreaker {
        symbol: Symbol,
        config: CircuitBreakerConfig,
    },
    /// Atomically amend a resting limit order's price and/or quantity.
    CancelReplace {
        symbol: Symbol,
        account: AccountId,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    },
    /// Set the maker/taker fee schedule for an instrument.
    SetFeeSchedule {
        symbol: Symbol,
        schedule: FeeSchedule,
    },
    /// Seed an account with `amount` in every registered currency.
    /// Internal to server-side bootstrap — not exposed on the wire.
    ProvisionAccount { account: AccountId, amount: u64 },
    /// Debit available funds from an account. Rejects on resting orders
    /// or insufficient balance.
    Withdraw {
        account: AccountId,
        currency: CurrencyId,
        amount: u64,
    },
    /// Cancel all Day-TIF resting orders across instruments.
    EndOfDay,
    /// Disable an instrument: reject new orders, cancel resting orders.
    DisableInstrument { symbol: Symbol },
    /// Re-enable a previously disabled instrument.
    EnableInstrument { symbol: Symbol },
    /// Permanently remove a disabled, empty instrument.
    RemoveInstrument { symbol: Symbol },
    /// Read-only query for server stats (not journaled).
    QueryStats,
    /// Read-only query for an account's balances (not journaled).
    QueryPosition { account: AccountId },
}

impl AppEvent for TradingEvent {
    fn encoded_size(&self) -> usize {
        // 1 byte tag + per-variant payload size.
        1 + match self {
            TradingEvent::AddInstrument { .. } => 4 + 4 + 4, // symbol + base + quote
            TradingEvent::Deposit { .. } => 4 + 4 + 8,
            TradingEvent::SubmitOrder { order, .. } => 4 + encoded_order_size(order),
            TradingEvent::CancelOrder { .. } => 4 + 4 + 8,
            TradingEvent::SetRiskLimits { limits, .. } => {
                4 + option_u64_size(limits.max_order_qty.is_some())
                    + option_u64_size(limits.max_order_notional.is_some())
            }
            TradingEvent::CancelAll { .. } => 4,
            TradingEvent::SetCircuitBreaker { config, .. } => {
                4 + option_u64_size(config.price_band_lower.is_some())
                    + option_u64_size(config.price_band_upper.is_some())
                    + 1
            }
            TradingEvent::CancelReplace { .. } => 4 + 4 + 8 + 8 + 8,
            TradingEvent::SetFeeSchedule { .. } => 4 + 2 + 2,
            TradingEvent::ProvisionAccount { .. } => 4 + 8,
            TradingEvent::Withdraw { .. } => 4 + 4 + 8,
            TradingEvent::EndOfDay => 0,
            TradingEvent::DisableInstrument { .. } => 4,
            TradingEvent::EnableInstrument { .. } => 4,
            TradingEvent::RemoveInstrument { .. } => 4,
            TradingEvent::QueryStats => 0,
            TradingEvent::QueryPosition { .. } => 4,
        }
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        let (tag, payload_len) = match self {
            TradingEvent::AddInstrument { spec } => {
                buf[0] = TAG_ADD_INSTRUMENT;
                le::put_u32(&mut buf[1..], spec.symbol.0);
                le::put_u32(&mut buf[5..], spec.base.0);
                le::put_u32(&mut buf[9..], spec.quote.0);
                (TAG_ADD_INSTRUMENT, 12)
            }
            TradingEvent::Deposit {
                account,
                currency,
                amount,
            } => {
                le::put_u32(&mut buf[1..], account.0);
                le::put_u32(&mut buf[5..], currency.0);
                le::put_u64(&mut buf[9..], *amount);
                (TAG_DEPOSIT, 16)
            }
            TradingEvent::SubmitOrder { symbol, order } => {
                le::put_u32(&mut buf[1..], symbol.0);
                let n = encode_order(order, &mut buf[5..]);
                (TAG_SUBMIT_ORDER, 4 + n)
            }
            TradingEvent::CancelOrder {
                symbol,
                account,
                order_id,
            } => {
                le::put_u32(&mut buf[1..], symbol.0);
                le::put_u32(&mut buf[5..], account.0);
                le::put_u64(&mut buf[9..], order_id.0);
                (TAG_CANCEL_ORDER, 16)
            }
            TradingEvent::SetRiskLimits { symbol, limits } => {
                le::put_u32(&mut buf[1..], symbol.0);
                let mut p = 5;
                p += put_option_u64(&mut buf[p..], limits.max_order_qty.map(|q| q.get()));
                p += put_option_u64(&mut buf[p..], limits.max_order_notional);
                (TAG_SET_RISK_LIMITS, p - 1)
            }
            TradingEvent::CancelAll { account } => {
                le::put_u32(&mut buf[1..], account.0);
                (TAG_CANCEL_ALL, 4)
            }
            TradingEvent::SetCircuitBreaker { symbol, config } => {
                le::put_u32(&mut buf[1..], symbol.0);
                let mut p = 5;
                p += put_option_u64(&mut buf[p..], config.price_band_lower.map(|pr| pr.get()));
                p += put_option_u64(&mut buf[p..], config.price_band_upper.map(|pr| pr.get()));
                buf[p] = u8::from(config.halted);
                p += 1;
                (TAG_SET_CIRCUIT_BREAKER, p - 1)
            }
            TradingEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                le::put_u32(&mut buf[1..], symbol.0);
                le::put_u32(&mut buf[5..], account.0);
                le::put_u64(&mut buf[9..], order_id.0);
                le::put_u64(&mut buf[17..], new_price.get());
                le::put_u64(&mut buf[25..], new_quantity.get());
                (TAG_CANCEL_REPLACE, 32)
            }
            TradingEvent::SetFeeSchedule { symbol, schedule } => {
                le::put_u32(&mut buf[1..], symbol.0);
                le::put_i16(&mut buf[5..], schedule.maker_fee_bps);
                le::put_i16(&mut buf[7..], schedule.taker_fee_bps);
                (TAG_SET_FEE_SCHEDULE, 8)
            }
            TradingEvent::ProvisionAccount { account, amount } => {
                le::put_u32(&mut buf[1..], account.0);
                le::put_u64(&mut buf[5..], *amount);
                (TAG_PROVISION_ACCOUNT, 12)
            }
            TradingEvent::Withdraw {
                account,
                currency,
                amount,
            } => {
                le::put_u32(&mut buf[1..], account.0);
                le::put_u32(&mut buf[5..], currency.0);
                le::put_u64(&mut buf[9..], *amount);
                (TAG_WITHDRAW, 16)
            }
            TradingEvent::EndOfDay => (TAG_END_OF_DAY, 0),
            TradingEvent::DisableInstrument { symbol } => {
                le::put_u32(&mut buf[1..], symbol.0);
                (TAG_DISABLE_INSTRUMENT, 4)
            }
            TradingEvent::EnableInstrument { symbol } => {
                le::put_u32(&mut buf[1..], symbol.0);
                (TAG_ENABLE_INSTRUMENT, 4)
            }
            TradingEvent::RemoveInstrument { symbol } => {
                le::put_u32(&mut buf[1..], symbol.0);
                (TAG_REMOVE_INSTRUMENT, 4)
            }
            TradingEvent::QueryStats => (TAG_QUERY_STATS, 0),
            TradingEvent::QueryPosition { account } => {
                le::put_u32(&mut buf[1..], account.0);
                (TAG_QUERY_POSITION, 4)
            }
        };
        buf[0] = tag;
        1 + payload_len
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        if buf.is_empty() {
            return Err(CodecError::Truncated);
        }
        let tag = buf[0];
        let payload = &buf[1..];
        match tag {
            TAG_ADD_INSTRUMENT => {
                need(payload, 12)?;
                Ok(TradingEvent::AddInstrument {
                    spec: InstrumentSpec {
                        symbol: Symbol(le::get_u32(&payload[0..])),
                        base: CurrencyId(le::get_u32(&payload[4..])),
                        quote: CurrencyId(le::get_u32(&payload[8..])),
                    },
                })
            }
            TAG_DEPOSIT => {
                need(payload, 16)?;
                Ok(TradingEvent::Deposit {
                    account: AccountId(le::get_u32(&payload[0..])),
                    currency: CurrencyId(le::get_u32(&payload[4..])),
                    amount: le::get_u64(&payload[8..]),
                })
            }
            TAG_SUBMIT_ORDER => {
                need(payload, 4)?;
                let symbol = Symbol(le::get_u32(&payload[0..]));
                let (_, order) = decode_order(&payload[4..])?;
                Ok(TradingEvent::SubmitOrder { symbol, order })
            }
            TAG_CANCEL_ORDER => {
                need(payload, 16)?;
                Ok(TradingEvent::CancelOrder {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                    account: AccountId(le::get_u32(&payload[4..])),
                    order_id: OrderId(le::get_u64(&payload[8..])),
                })
            }
            TAG_SET_RISK_LIMITS => {
                need(payload, 4)?;
                let symbol = Symbol(le::get_u32(&payload[0..]));
                let mut p = 4;
                let (consumed, max_order_qty) = get_option_u64(&payload[p..])?;
                p += consumed;
                let max_order_qty = max_order_qty
                    .map(|v| {
                        NonZeroU64::new(v)
                            .ok_or(CodecError::InvalidField)
                            .map(Quantity)
                    })
                    .transpose()?;
                let (consumed, max_order_notional) = get_option_u64(&payload[p..])?;
                let _ = consumed;
                Ok(TradingEvent::SetRiskLimits {
                    symbol,
                    limits: RiskLimits {
                        max_order_qty,
                        max_order_notional,
                    },
                })
            }
            TAG_CANCEL_ALL => {
                need(payload, 4)?;
                Ok(TradingEvent::CancelAll {
                    account: AccountId(le::get_u32(&payload[0..])),
                })
            }
            TAG_SET_CIRCUIT_BREAKER => {
                need(payload, 4)?;
                let symbol = Symbol(le::get_u32(&payload[0..]));
                let mut p = 4;
                let (consumed, price_band_lower) = get_option_u64(&payload[p..])?;
                p += consumed;
                let price_band_lower = price_band_lower
                    .map(|v| {
                        NonZeroU64::new(v)
                            .ok_or(CodecError::InvalidField)
                            .map(Price)
                    })
                    .transpose()?;
                let (consumed, price_band_upper) = get_option_u64(&payload[p..])?;
                p += consumed;
                let price_band_upper = price_band_upper
                    .map(|v| {
                        NonZeroU64::new(v)
                            .ok_or(CodecError::InvalidField)
                            .map(Price)
                    })
                    .transpose()?;
                need(&payload[p..], 1)?;
                let halted = payload[p] != 0;
                Ok(TradingEvent::SetCircuitBreaker {
                    symbol,
                    config: CircuitBreakerConfig {
                        price_band_lower,
                        price_band_upper,
                        halted,
                    },
                })
            }
            TAG_CANCEL_REPLACE => {
                need(payload, 32)?;
                let new_price =
                    NonZeroU64::new(le::get_u64(&payload[16..])).ok_or(CodecError::InvalidField)?;
                let new_quantity =
                    NonZeroU64::new(le::get_u64(&payload[24..])).ok_or(CodecError::InvalidField)?;
                Ok(TradingEvent::CancelReplace {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                    account: AccountId(le::get_u32(&payload[4..])),
                    order_id: OrderId(le::get_u64(&payload[8..])),
                    new_price: Price(new_price),
                    new_quantity: Quantity(new_quantity),
                })
            }
            TAG_SET_FEE_SCHEDULE => {
                need(payload, 8)?;
                Ok(TradingEvent::SetFeeSchedule {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                    schedule: FeeSchedule {
                        maker_fee_bps: le::get_i16(&payload[4..]),
                        taker_fee_bps: le::get_i16(&payload[6..]),
                    },
                })
            }
            TAG_PROVISION_ACCOUNT => {
                need(payload, 12)?;
                Ok(TradingEvent::ProvisionAccount {
                    account: AccountId(le::get_u32(&payload[0..])),
                    amount: le::get_u64(&payload[4..]),
                })
            }
            TAG_WITHDRAW => {
                need(payload, 16)?;
                Ok(TradingEvent::Withdraw {
                    account: AccountId(le::get_u32(&payload[0..])),
                    currency: CurrencyId(le::get_u32(&payload[4..])),
                    amount: le::get_u64(&payload[8..]),
                })
            }
            TAG_END_OF_DAY => Ok(TradingEvent::EndOfDay),
            TAG_DISABLE_INSTRUMENT => {
                need(payload, 4)?;
                Ok(TradingEvent::DisableInstrument {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                })
            }
            TAG_ENABLE_INSTRUMENT => {
                need(payload, 4)?;
                Ok(TradingEvent::EnableInstrument {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                })
            }
            TAG_REMOVE_INSTRUMENT => {
                need(payload, 4)?;
                Ok(TradingEvent::RemoveInstrument {
                    symbol: Symbol(le::get_u32(&payload[0..])),
                })
            }
            TAG_QUERY_STATS => Ok(TradingEvent::QueryStats),
            TAG_QUERY_POSITION => {
                need(payload, 4)?;
                Ok(TradingEvent::QueryPosition {
                    account: AccountId(le::get_u32(&payload[0..])),
                })
            }
            other => Err(CodecError::UnknownTag(other)),
        }
    }

    #[inline]
    fn is_query(&self) -> bool {
        matches!(
            self,
            TradingEvent::QueryStats | TradingEvent::QueryPosition { .. }
        )
    }
}

#[inline]
fn need(buf: &[u8], n: usize) -> Result<(), CodecError> {
    if buf.len() < n {
        Err(CodecError::Truncated)
    } else {
        Ok(())
    }
}

/// Byte count for a flag(1) + optional u64(8) field.
#[inline]
fn option_u64_size(present: bool) -> usize {
    if present { 9 } else { 1 }
}

/// Write an `Option<u64>` as [flag:u8][value:u64?]. Returns bytes written.
fn put_option_u64(buf: &mut [u8], v: Option<u64>) -> usize {
    match v {
        Some(x) => {
            buf[0] = 1;
            le::put_u64(&mut buf[1..], x);
            9
        }
        None => {
            buf[0] = 0;
            1
        }
    }
}

/// Read an `Option<u64>` written by [`put_option_u64`]. Returns
/// `(bytes_consumed, value)`.
fn get_option_u64(buf: &[u8]) -> Result<(usize, Option<u64>), CodecError> {
    need(buf, 1)?;
    match buf[0] {
        0 => Ok((1, None)),
        1 => {
            need(buf, 9)?;
            Ok((9, Some(le::get_u64(&buf[1..]))))
        }
        _ => Err(CodecError::InvalidField),
    }
}

/// Encoded size of an `Order`. Mirrors [`encode_order`] byte-for-byte.
fn encoded_order_size(order: &Order) -> usize {
    // id(8) + account(4) + side(1) + order_type_tag(1) = 14
    let mut n = 14;
    n += match order.order_type {
        OrderType::Market => 0,
        OrderType::Limit { .. } => 8,
        OrderType::Stop { .. } => 8,
        OrderType::StopLimit { .. } => 16,
    };
    // tif(1) + quantity(8) + stp(1) = 10
    n += 10;
    if order.time_in_force == TimeInForce::GTD {
        n += 8; // expiry_ns
    }
    n
}

/// Encode an `Order` payload — mirrors `journal::codec::encode_order` so
/// Phase 2 can unify without a wire-format change. Returns bytes written.
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

    if order.time_in_force == TimeInForce::GTD {
        le::put_u64(&mut buf[pos..], order.expiry_ns);
        pos += 8;
    }

    pos
}

/// Decode an `Order` — mirrors `journal::codec::decode_order`. Returns
/// `(bytes_consumed, Order)`.
fn decode_order(buf: &[u8]) -> Result<(usize, Order), CodecError> {
    need(buf, 14)?;
    let mut pos = 0;
    let id = OrderId(le::get_u64(&buf[pos..]));
    pos += 8;
    let account = AccountId(le::get_u32(&buf[pos..]));
    pos += 4;
    let side = le::decode_side(buf[pos]).ok_or(CodecError::InvalidField)?;
    pos += 1;

    let order_type_tag = buf[pos];
    pos += 1;
    let order_type = match order_type_tag {
        ORDER_TYPE_MARKET => OrderType::Market,
        ORDER_TYPE_LIMIT | ORDER_TYPE_LIMIT_POST_ONLY => {
            need(&buf[pos..], 8)?;
            let price =
                NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(CodecError::InvalidField)?;
            pos += 8;
            OrderType::Limit {
                price: Price(price),
                post_only: order_type_tag == ORDER_TYPE_LIMIT_POST_ONLY,
            }
        }
        ORDER_TYPE_STOP => {
            need(&buf[pos..], 8)?;
            let trigger =
                NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(CodecError::InvalidField)?;
            pos += 8;
            OrderType::Stop {
                trigger_price: Price(trigger),
            }
        }
        ORDER_TYPE_STOP_LIMIT => {
            need(&buf[pos..], 16)?;
            let trigger =
                NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(CodecError::InvalidField)?;
            pos += 8;
            let limit =
                NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(CodecError::InvalidField)?;
            pos += 8;
            OrderType::StopLimit {
                trigger_price: Price(trigger),
                limit_price: Price(limit),
            }
        }
        _ => return Err(CodecError::InvalidField),
    };

    need(&buf[pos..], 10)?;
    let time_in_force = le::decode_tif(buf[pos]).ok_or(CodecError::InvalidField)?;
    pos += 1;
    let quantity =
        Quantity(NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(CodecError::InvalidField)?);
    pos += 8;
    let stp = le::decode_stp(buf[pos]).ok_or(CodecError::InvalidField)?;
    pos += 1;

    let expiry_ns = if time_in_force == TimeInForce::GTD {
        need(&buf[pos..], 8)?;
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
            quantity,
            stp,
            expiry_ns,
        },
    ))
}

// Silence unused-import warnings for the few types only touched in tests.
#[cfg(not(test))]
const _: fn() = || {
    let _ = core::mem::size_of::<SelfTradeProtection>();
    let _ = core::mem::size_of::<Side>();
};

// The cache-line size bound on `JournalEvent<TradingEvent>` lives in
// `melin-engine` alongside the other InputSlot assertions — this crate
// stays dependency-free of the journal framing.

#[cfg(test)]
mod tests {
    use super::*;

    fn price(p: u64) -> Price {
        Price(NonZeroU64::new(p).unwrap())
    }
    fn qty(q: u64) -> Quantity {
        Quantity(NonZeroU64::new(q).unwrap())
    }

    fn round_trip(ev: TradingEvent) {
        let mut buf = [0u8; 256];
        let n = ev.encode(&mut buf);
        assert_eq!(n, ev.encoded_size(), "encoded_size disagrees with encode");
        let decoded = TradingEvent::decode(&buf[..n]).expect("decode");
        assert_eq!(ev, decoded);
    }

    #[test]
    fn round_trip_add_instrument() {
        round_trip(TradingEvent::AddInstrument {
            spec: InstrumentSpec {
                symbol: Symbol(42),
                base: CurrencyId(1),
                quote: CurrencyId(2),
            },
        });
    }

    #[test]
    fn round_trip_deposit_withdraw() {
        round_trip(TradingEvent::Deposit {
            account: AccountId(7),
            currency: CurrencyId(1),
            amount: 1_000_000,
        });
        round_trip(TradingEvent::Withdraw {
            account: AccountId(7),
            currency: CurrencyId(1),
            amount: 500,
        });
    }

    #[test]
    fn round_trip_submit_order_each_type() {
        let base = Order {
            id: OrderId(1),
            account: AccountId(2),
            side: Side::Buy,
            order_type: OrderType::Market,
            time_in_force: TimeInForce::GTC,
            quantity: qty(10),
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns: 0,
        };
        for ot in [
            OrderType::Market,
            OrderType::Limit {
                price: price(100),
                post_only: false,
            },
            OrderType::Limit {
                price: price(100),
                post_only: true,
            },
            OrderType::Stop {
                trigger_price: price(90),
            },
            OrderType::StopLimit {
                trigger_price: price(90),
                limit_price: price(95),
            },
        ] {
            round_trip(TradingEvent::SubmitOrder {
                symbol: Symbol(1),
                order: Order {
                    order_type: ot,
                    ..base
                },
            });
        }
    }

    #[test]
    fn round_trip_submit_order_gtd_carries_expiry() {
        let ev = TradingEvent::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(1),
                account: AccountId(2),
                side: Side::Sell,
                order_type: OrderType::Limit {
                    price: price(200),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(5),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 1_700_000_000_000_000_000,
            },
        };
        round_trip(ev);
    }

    #[test]
    fn round_trip_cancel_variants() {
        round_trip(TradingEvent::CancelOrder {
            symbol: Symbol(1),
            account: AccountId(2),
            order_id: OrderId(99),
        });
        round_trip(TradingEvent::CancelAll {
            account: AccountId(2),
        });
        round_trip(TradingEvent::CancelReplace {
            symbol: Symbol(1),
            account: AccountId(2),
            order_id: OrderId(99),
            new_price: price(105),
            new_quantity: qty(7),
        });
    }

    #[test]
    fn round_trip_risk_limits_none_and_some() {
        round_trip(TradingEvent::SetRiskLimits {
            symbol: Symbol(1),
            limits: RiskLimits {
                max_order_qty: None,
                max_order_notional: None,
            },
        });
        round_trip(TradingEvent::SetRiskLimits {
            symbol: Symbol(1),
            limits: RiskLimits {
                max_order_qty: Some(qty(1000)),
                max_order_notional: Some(1_000_000),
            },
        });
    }

    #[test]
    fn round_trip_circuit_breaker_all_permutations() {
        for lower in [None, Some(price(50))] {
            for upper in [None, Some(price(150))] {
                for halted in [false, true] {
                    round_trip(TradingEvent::SetCircuitBreaker {
                        symbol: Symbol(1),
                        config: CircuitBreakerConfig {
                            price_band_lower: lower,
                            price_band_upper: upper,
                            halted,
                        },
                    });
                }
            }
        }
    }

    #[test]
    fn round_trip_fee_schedule_negative_bps_is_rebate() {
        round_trip(TradingEvent::SetFeeSchedule {
            symbol: Symbol(1),
            schedule: FeeSchedule {
                maker_fee_bps: -10,
                taker_fee_bps: 20,
            },
        });
    }

    #[test]
    fn round_trip_instrument_lifecycle() {
        round_trip(TradingEvent::EndOfDay);
        round_trip(TradingEvent::DisableInstrument { symbol: Symbol(1) });
        round_trip(TradingEvent::EnableInstrument { symbol: Symbol(1) });
        round_trip(TradingEvent::RemoveInstrument { symbol: Symbol(1) });
        round_trip(TradingEvent::ProvisionAccount {
            account: AccountId(1),
            amount: 10_000,
        });
    }

    #[test]
    fn round_trip_queries_are_marked_as_such() {
        let q1 = TradingEvent::QueryStats;
        let q2 = TradingEvent::QueryPosition {
            account: AccountId(3),
        };
        assert!(q1.is_query());
        assert!(q2.is_query());
        round_trip(q1);
        round_trip(q2);
    }

    #[test]
    fn state_mutating_variants_are_not_queries() {
        let ev = TradingEvent::Deposit {
            account: AccountId(1),
            currency: CurrencyId(1),
            amount: 1,
        };
        assert!(!ev.is_query());
    }

    #[test]
    fn decode_unknown_tag_fails_cleanly() {
        let err = TradingEvent::decode(&[0xFF, 0, 0, 0]).unwrap_err();
        assert_eq!(err, CodecError::UnknownTag(0xFF));
    }

    #[test]
    fn decode_truncated_fails_cleanly() {
        let err = TradingEvent::decode(&[]).unwrap_err();
        assert_eq!(err, CodecError::Truncated);
        let err = TradingEvent::decode(&[TAG_DEPOSIT, 0, 0]).unwrap_err();
        assert_eq!(err, CodecError::Truncated);
    }

    #[test]
    fn decode_zero_price_rejected() {
        // Construct a CancelReplace with new_price = 0 — should fail
        // InvalidField rather than producing an invalid Price.
        let mut buf = [0u8; 64];
        buf[0] = TAG_CANCEL_REPLACE;
        le::put_u32(&mut buf[1..], 1); // symbol
        le::put_u32(&mut buf[5..], 2); // account
        le::put_u64(&mut buf[9..], 99); // order_id
        le::put_u64(&mut buf[17..], 0); // new_price = 0 (invalid)
        le::put_u64(&mut buf[25..], 7); // new_quantity
        let err = TradingEvent::decode(&buf[..33]).unwrap_err();
        assert_eq!(err, CodecError::InvalidField);
    }
}
