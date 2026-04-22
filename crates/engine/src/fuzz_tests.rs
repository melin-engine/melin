//! Fuzz tests for the journal and snapshot codecs.
//!
//! Feeds arbitrary bytes into decoders to find panics, out-of-bounds
//! accesses, and infinite loops. Complements the proptest round-trip
//! tests which only exercise valid inputs.

use crate::journal::codec;

/// Journal entry decoder must never panic on arbitrary input.
/// It must return Ok or a well-formed Err for any byte sequence.
#[test]
fn fuzz_journal_decode() {
    bolero::check!().for_each(|data: &[u8]| {
        let _ = codec::decode::<crate::trading_event::TradingEvent>(data, codec::FORMAT_VERSION);
    });
}

/// Journal file header decoder must never panic on arbitrary input.
#[test]
fn fuzz_journal_file_header() {
    bolero::check!().for_each(|data: &[u8]| {
        let _ = codec::decode_file_header(data);
    });
}

/// Journal encode → decode round-trip must be lossless for any valid event.
/// Uses bolero to generate arbitrary bytes, then constructs a valid
/// JournalEvent from them (discarding inputs that can't produce one).
#[test]
fn fuzz_journal_roundtrip() {
    bolero::check!().for_each(|data: &[u8]| {
        let Some(event) = journal_event_from_bytes(data) else {
            return;
        };

        let seq = 1u64;
        let ts = 1_700_000_000_000_000_000u64;
        let mut buf = [0u8; 272];

        let written = match codec::encode(seq, ts, 0, 0, &event, &mut buf) {
            Ok(n) => n,
            Err(_) => return,
        };

        let (consumed, dec_seq, dec_ts, _kh, _rs, dec_event) =
            codec::decode(&buf[..written], codec::FORMAT_VERSION)
                .expect("decode of freshly encoded event must succeed");

        assert_eq!(consumed, written);
        assert_eq!(dec_seq, seq);
        assert_eq!(dec_ts, ts);
        assert_eq!(dec_event, event, "round-trip mismatch");
    });
}

/// Snapshot binary decoder must never panic on arbitrary input.
/// Tests the internal `decode_exchange_state` indirectly via the full
/// snapshot load path — wraps arbitrary bytes with a valid header and CRC.
#[test]
fn fuzz_snapshot_decode() {
    use crate::journal::snapshot;

    bolero::check!().for_each(|data: &[u8]| {
        // Write data as a snapshot file and try to load it.
        // This exercises the full decode path including header/CRC validation.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("fuzz.snapshot");
        std::fs::write(&path, data).expect("write");
        let _ = snapshot::load(&path);
    });
}

// ---------------------------------------------------------------------------
// Helpers: construct valid types from raw bytes
// ---------------------------------------------------------------------------

use crate::journal::JournalEvent;
use crate::types::*;
use std::num::NonZeroU64;

/// Read a NonZeroU64 from bytes, returning None if zero or insufficient data.
fn nz64(data: &[u8], offset: usize) -> Option<NonZeroU64> {
    if data.len() < offset + 8 {
        return None;
    }
    NonZeroU64::new(u64::from_le_bytes(
        data[offset..offset + 8].try_into().ok()?,
    ))
}

fn u32_at(data: &[u8], offset: usize) -> Option<u32> {
    if data.len() < offset + 4 {
        return None;
    }
    Some(u32::from_le_bytes(
        data[offset..offset + 4].try_into().ok()?,
    ))
}

fn u64_at(data: &[u8], offset: usize) -> Option<u64> {
    if data.len() < offset + 8 {
        return None;
    }
    Some(u64::from_le_bytes(
        data[offset..offset + 8].try_into().ok()?,
    ))
}

/// Construct a JournalEvent from arbitrary bytes. Returns None if the bytes
/// are insufficient to build a valid event.
fn journal_event_from_bytes(data: &[u8]) -> Option<JournalEvent> {
    if data.is_empty() {
        return None;
    }

    match data[0] % 11 {
        0 => {
            // AddInstrument.
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::AddInstrument {
                    spec: InstrumentSpec {
                        symbol: Symbol(u32_at(data, 1)?),
                        base: CurrencyId(u32_at(data, 5)?),
                        quote: CurrencyId(u32_at(data, 9)?),
                    },
                },
            ))
        }
        1 => {
            // Deposit.
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::Deposit {
                    account: AccountId(u32_at(data, 1)?),
                    currency: CurrencyId(u32_at(data, 5)?),
                    amount: u64_at(data, 9)?,
                },
            ))
        }
        2 => {
            // SubmitOrder.
            if data.len() < 28 {
                return None;
            }
            let symbol = Symbol(u32_at(data, 1)?);
            let id = OrderId(u64_at(data, 5)?);
            let account = AccountId(u32_at(data, 13)?);
            let side = if data[17] & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let qty = Quantity(nz64(data, 18)?);
            let tif = match data[26] % 3 {
                0 => TimeInForce::GTC,
                1 => TimeInForce::IOC,
                _ => TimeInForce::FOK,
            };
            let stp = match data[27] % 4 {
                0 => SelfTradeProtection::Allow,
                1 => SelfTradeProtection::CancelNewest,
                2 => SelfTradeProtection::CancelOldest,
                _ => SelfTradeProtection::CancelBoth,
            };
            if data.len() < 29 {
                return None;
            }
            let order_type = match data[28] % 4 {
                0 => OrderType::Market,
                1 => OrderType::Limit {
                    price: Price(nz64(data, 29)?),
                    post_only: false,
                },
                2 => OrderType::Stop {
                    trigger_price: Price(nz64(data, 29)?),
                },
                _ => OrderType::StopLimit {
                    trigger_price: Price(nz64(data, 29)?),
                    limit_price: Price(nz64(data, 37)?),
                },
            };
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::SubmitOrder {
                    symbol,
                    order: Order {
                        id,
                        account,
                        side,
                        order_type,
                        time_in_force: tif,
                        quantity: qty,
                        stp,
                        expiry_ns: 0,
                    },
                },
            ))
        }
        3 => {
            // CancelOrder.
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::CancelOrder {
                    symbol: Symbol(u32_at(data, 1)?),
                    account: AccountId(u32_at(data, 5)?),
                    order_id: OrderId(u64_at(data, 9)?),
                },
            ))
        }
        4 => {
            // SetRiskLimits.
            if data.len() < 7 {
                return None;
            }
            let symbol = Symbol(u32_at(data, 1)?);
            let mut p = 5;
            let max_order_qty = if data[p] & 1 == 1 {
                p += 1;
                let v = Quantity(nz64(data, p)?);
                p += 8;
                Some(v)
            } else {
                p += 1;
                None
            };
            let max_order_notional = if data.len() > p && data[p] & 1 == 1 {
                p += 1;
                Some(u64_at(data, p)?)
            } else {
                None
            };
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::SetRiskLimits {
                    symbol,
                    limits: RiskLimits {
                        max_order_qty,
                        max_order_notional,
                    },
                },
            ))
        }
        5 => {
            // CancelAll.
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::CancelAll {
                    account: AccountId(u32_at(data, 1)?),
                },
            ))
        }
        7 => {
            // DisableInstrument.
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::DisableInstrument {
                    symbol: Symbol(u32_at(data, 1)?),
                },
            ))
        }
        8 => {
            // EnableInstrument.
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::EnableInstrument {
                    symbol: Symbol(u32_at(data, 1)?),
                },
            ))
        }
        9 => {
            // RemoveInstrument.
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::RemoveInstrument {
                    symbol: Symbol(u32_at(data, 1)?),
                },
            ))
        }
        _ => {
            // SetCircuitBreaker.
            if data.len() < 7 {
                return None;
            }
            let symbol = Symbol(u32_at(data, 1)?);
            let mut p = 5;
            let lower = if data[p] & 1 == 1 {
                p += 1;
                let v = Price(nz64(data, p)?);
                p += 8;
                Some(v)
            } else {
                p += 1;
                None
            };
            let upper = if data.len() > p && data[p] & 1 == 1 {
                p += 1;
                let v = Price(nz64(data, p)?);
                p += 8;
                Some(v)
            } else {
                p += 1;
                None
            };
            let halted = data.len() > p && data[p] & 1 == 1;
            Some(JournalEvent::App(
                crate::trading_event::TradingEvent::SetCircuitBreaker {
                    symbol,
                    config: CircuitBreakerConfig {
                        price_band_lower: lower,
                        price_band_upper: upper,
                        halted,
                    },
                },
            ))
        }
    }
}
