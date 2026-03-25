//! Binary codec for journal entries.
//!
//! Manual serialization (no serde) for zero allocation, predictable layout,
//! and no format stability concerns across dependency versions.
//!
//! ## File header (8 bytes, written once at creation)
//!
//! | Field          | Type | Bytes | Purpose                                |
//! |----------------|------|-------|----------------------------------------|
//! | file_magic     | u32  | 4     | `0x4A4F5552` ("JOUR")                  |
//! | format_version | u16  | 2     | Current version = 8                    |
//! | reserved       | u16  | 2     | Padding for alignment, zeroed          |
//!
//! ## Entry layout (little-endian, repeats after file header)
//!
//! | Field        | Type   | Bytes | Purpose                               |
//! |--------------|--------|-------|---------------------------------------|
//! | magic        | u16    | 2     | `0x4A45` — misalignment detection     |
//! | length       | u16    | 2     | Byte count after header, before CRC   |
//! | sequence     | u64    | 8     | Monotonically increasing, starts at 1 |
//! | timestamp_ns | u64    | 8     | Wall-clock nanos since epoch           |
//! | key_hash     | u64    | 8     | FxHash of client Ed25519 pubkey (v8+) |
//! | request_seq  | u64    | 8     | Per-key request sequence (v8+)        |
//! | event_tag    | u8     | 1     | Discriminant for JournalEvent variant  |
//! | payload      | varies | ≤64   | Variant fields                        |
//! | crc32c       | u32    | 4     | CRC32C of all preceding bytes         |
//!
//! `length` = size of (key_hash + request_seq + event_tag + payload).
//! Total entry size = 20 + length + 4.

use std::num::NonZeroU64;

use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, FeeSchedule, InstrumentSpec, Order, OrderId,
    OrderType, Price, Quantity, RiskLimits, Symbol,
};

use super::error::JournalError;
use super::event::JournalEvent;
use crate::le;

/// File magic bytes: "JOUR" in ASCII (little-endian u32).
pub const FILE_MAGIC: u32 = 0x4A4F_5552;

/// Current format version. Bumped on any layout change.
/// v1 → v2: added SelfTradeProtection byte to Order encoding.
/// v2 → v3: added SetRiskLimits event for fat finger checks.
/// v3 → v4: added CancelAll event for kill switch.
/// v4 → v5: added SetCircuitBreaker event for price bands + trading halts.
/// v5 → v6: added GenesisHash + Checkpoint events for BLAKE3 hash chain.
/// v6 → v7: added Withdraw event for sparse account lifecycle.
/// v7 → v8: added per-entry key_hash(8) + request_seq(8) for admin idempotency.
pub const FORMAT_VERSION: u16 = 8;

/// File header size in bytes.
pub const FILE_HEADER_SIZE: usize = 8;

/// Entry header size: magic(2) + length(2) + sequence(8) + timestamp(8) = 20 bytes.
const ENTRY_HEADER_SIZE: usize = 20;

/// Entry magic bytes for corruption/misalignment detection.
const ENTRY_MAGIC: u16 = 0x4A45;

/// CRC32C checksum size in bytes.
const CRC_SIZE: usize = 4;

/// Event tag discriminants.
const TAG_ADD_INSTRUMENT: u8 = 1;
const TAG_DEPOSIT: u8 = 2;
const TAG_SUBMIT_ORDER: u8 = 3;
const TAG_CANCEL_ORDER: u8 = 4;
const TAG_SET_RISK_LIMITS: u8 = 5;
const TAG_CANCEL_ALL: u8 = 6;
const TAG_SET_CIRCUIT_BREAKER: u8 = 7;
const TAG_CANCEL_REPLACE: u8 = 8;
const TAG_GENESIS_HASH: u8 = 9;
const TAG_CHECKPOINT: u8 = 10;
const TAG_SET_FEE_SCHEDULE: u8 = 11;
const TAG_PROVISION_ACCOUNT: u8 = 12;
const TAG_WITHDRAW: u8 = 13;

/// OrderType tag encoding (codec-specific, not shared — order types are only
/// in the journal format, not in snapshots).
const ORDER_TYPE_MARKET: u8 = 0;
const ORDER_TYPE_LIMIT: u8 = 1;
const ORDER_TYPE_STOP: u8 = 2;
const ORDER_TYPE_STOP_LIMIT: u8 = 3;
const ORDER_TYPE_LIMIT_POST_ONLY: u8 = 4;

/// Encode the file header into `buf`.
pub fn encode_file_header(buf: &mut [u8]) {
    buf[0..4].copy_from_slice(&FILE_MAGIC.to_le_bytes());
    buf[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes());
}

/// Validate a file header. Returns `Ok(version)` on success.
pub fn decode_file_header(buf: &[u8]) -> Result<u16, JournalError> {
    if buf.len() < FILE_HEADER_SIZE {
        return Err(JournalError::TruncatedEntry);
    }
    let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != FILE_MAGIC {
        return Err(JournalError::InvalidFile);
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    // Accept v5 (pre-hash-chain), v7 (pre-idempotency), and v8 (current).
    // v5 journals are readable — the reader simply won't have hash chain
    // verification. v7 journals lack key_hash/request_seq fields.
    if version != FORMAT_VERSION && version != 7 && version != 5 {
        return Err(JournalError::UnsupportedVersion { version });
    }
    Ok(version)
}

/// Encode a journal entry into `buf`.
///
/// Returns the total number of bytes written (header + event_tag + payload + CRC).
/// The caller must ensure `buf` is large enough (128 bytes is always sufficient).
pub fn encode(
    sequence: u64,
    timestamp_ns: u64,
    key_hash: u64,
    request_seq: u64,
    event: &JournalEvent,
    buf: &mut [u8],
) -> Result<usize, JournalError> {
    // Leave room for header + key_hash(8) + request_seq(8) + event_tag(1).
    let payload_start = ENTRY_HEADER_SIZE + 16 + 1;
    let mut pos = payload_start;

    let event_tag = match event {
        JournalEvent::AddInstrument { spec } => {
            le::put_u32(&mut buf[pos..], spec.symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], spec.base.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], spec.quote.0);
            pos += 4;
            TAG_ADD_INSTRUMENT
        }
        JournalEvent::Deposit {
            account,
            currency,
            amount,
        } => {
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], currency.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], *amount);
            pos += 8;
            TAG_DEPOSIT
        }
        JournalEvent::SubmitOrder { symbol, order } => {
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            pos += encode_order(order, &mut buf[pos..]);
            TAG_SUBMIT_ORDER
        }
        JournalEvent::CancelOrder {
            symbol,
            account,
            order_id,
        } => {
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            TAG_CANCEL_ORDER
        }
        JournalEvent::SetRiskLimits { symbol, limits } => {
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            // max_order_qty: option tag (1) + value if Some (8).
            match limits.max_order_qty {
                Some(qty) => {
                    buf[pos] = 1;
                    pos += 1;
                    le::put_u64(&mut buf[pos..], qty.get());
                    pos += 8;
                }
                None => {
                    buf[pos] = 0;
                    pos += 1;
                }
            }
            // max_order_notional: option tag (1) + value if Some (8).
            match limits.max_order_notional {
                Some(notional) => {
                    buf[pos] = 1;
                    pos += 1;
                    le::put_u64(&mut buf[pos..], notional);
                    pos += 8;
                }
                None => {
                    buf[pos] = 0;
                    pos += 1;
                }
            }
            TAG_SET_RISK_LIMITS
        }
        JournalEvent::CancelAll { account } => {
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            TAG_CANCEL_ALL
        }
        JournalEvent::SetCircuitBreaker { symbol, config } => {
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            // price_band_lower: option tag (1) + value if Some (8).
            match config.price_band_lower {
                Some(price) => {
                    buf[pos] = 1;
                    pos += 1;
                    le::put_u64(&mut buf[pos..], price.get());
                    pos += 8;
                }
                None => {
                    buf[pos] = 0;
                    pos += 1;
                }
            }
            // price_band_upper: option tag (1) + value if Some (8).
            match config.price_band_upper {
                Some(price) => {
                    buf[pos] = 1;
                    pos += 1;
                    le::put_u64(&mut buf[pos..], price.get());
                    pos += 8;
                }
                None => {
                    buf[pos] = 0;
                    pos += 1;
                }
            }
            // halted: bool (1).
            buf[pos] = u8::from(config.halted);
            pos += 1;
            TAG_SET_CIRCUIT_BREAKER
        }
        JournalEvent::CancelReplace {
            symbol,
            account,
            order_id,
            new_price,
            new_quantity,
        } => {
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
            TAG_CANCEL_REPLACE
        }
        JournalEvent::QueryStats => {
            // QueryStats is never journaled — the journal stage filters it
            // out before calling batch_append. This arm should never execute.
            return Err(JournalError::CorruptEntry {
                sequence,
                reason: "QueryStats must not be journaled",
            });
        }
        JournalEvent::GenesisHash { hash } => {
            buf[pos..pos + 32].copy_from_slice(hash);
            pos += 32;
            TAG_GENESIS_HASH
        }
        JournalEvent::Checkpoint {
            chain_hash,
            events_since_checkpoint,
        } => {
            buf[pos..pos + 32].copy_from_slice(chain_hash);
            pos += 32;
            le::put_u64(&mut buf[pos..], *events_since_checkpoint);
            pos += 8;
            TAG_CHECKPOINT
        }
        JournalEvent::SetFeeSchedule { symbol, schedule } => {
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_i16(&mut buf[pos..], schedule.maker_fee_bps);
            pos += 2;
            le::put_i16(&mut buf[pos..], schedule.taker_fee_bps);
            pos += 2;
            TAG_SET_FEE_SCHEDULE
        }
        JournalEvent::ProvisionAccount { account, amount } => {
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], *amount);
            pos += 8;
            TAG_PROVISION_ACCOUNT
        }
        JournalEvent::Withdraw {
            account,
            currency,
            amount,
        } => {
            le::put_u32(&mut buf[pos..], account.0);
            pos += 4;
            le::put_u32(&mut buf[pos..], currency.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], *amount);
            pos += 8;
            TAG_WITHDRAW
        }
    };

    // `length` covers key_hash(8) + request_seq(8) + event_tag(1) + payload bytes.
    let length = pos - ENTRY_HEADER_SIZE;
    // Guard against future event types exceeding u16 range.
    let length_u16 = u16::try_from(length).map_err(|_| JournalError::CorruptEntry {
        sequence,
        reason: "encoded payload exceeds u16 max",
    })?;

    // Write entry header.
    let mut h = 0;
    le::put_u16(&mut buf[h..], ENTRY_MAGIC);
    h += 2;
    le::put_u16(&mut buf[h..], length_u16);
    h += 2;
    le::put_u64(&mut buf[h..], sequence);
    h += 8;
    le::put_u64(&mut buf[h..], timestamp_ns);
    h += 8;
    debug_assert_eq!(h, ENTRY_HEADER_SIZE);

    // Write key_hash and request_seq (v8+, for admin idempotency dedup).
    le::put_u64(&mut buf[ENTRY_HEADER_SIZE..], key_hash);
    le::put_u64(&mut buf[ENTRY_HEADER_SIZE + 8..], request_seq);

    // Write event tag (after key_hash + request_seq).
    buf[ENTRY_HEADER_SIZE + 16] = event_tag;

    // CRC32C over everything before the checksum.
    let crc = crc32c::crc32c(&buf[..pos]);
    le::put_u32(&mut buf[pos..], crc);
    pos += CRC_SIZE;

    Ok(pos)
}

/// Decode a journal entry from `buf`.
///
/// Returns `(bytes_consumed, sequence, timestamp_ns, key_hash, request_seq, event)`.
/// `version` determines the layout: v8+ entries have key_hash(8) + request_seq(8)
/// after the header; older versions return (0, 0) for those fields.
/// Returns `Err(TruncatedEntry)` if the buffer doesn't contain a complete entry.
pub fn decode(
    buf: &[u8],
    version: u16,
) -> Result<(usize, u64, u64, u64, u64, JournalEvent), JournalError> {
    // Need at least the fixed header to read length.
    if buf.len() < ENTRY_HEADER_SIZE + 1 + CRC_SIZE {
        return Err(JournalError::TruncatedEntry);
    }

    let magic = le::get_u16(&buf[0..]);
    if magic != ENTRY_MAGIC {
        return Err(JournalError::CorruptEntry {
            sequence: 0,
            reason: "bad entry magic",
        });
    }

    let payload_len = le::get_u16(&buf[2..]) as usize;
    let total_len = ENTRY_HEADER_SIZE + payload_len + CRC_SIZE;

    if buf.len() < total_len {
        return Err(JournalError::TruncatedEntry);
    }

    let sequence = le::get_u64(&buf[4..]);
    let timestamp_ns = le::get_u64(&buf[12..]);

    // Validate CRC.
    let data_end = ENTRY_HEADER_SIZE + payload_len;
    let expected_crc = le::get_u32(&buf[data_end..]);
    let actual_crc = crc32c::crc32c(&buf[..data_end]);
    if expected_crc != actual_crc {
        return Err(JournalError::ChecksumMismatch {
            sequence,
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    // Decode key_hash and request_seq (v8+). Older versions lack these
    // fields — default to 0 (exempt from idempotency checking).
    let (key_hash, request_seq, event_tag_offset) = if version >= 8 {
        if payload_len < 17 {
            return Err(JournalError::CorruptEntry {
                sequence,
                reason: "v8 entry too short for key_hash + request_seq + tag",
            });
        }
        let kh = le::get_u64(&buf[ENTRY_HEADER_SIZE..]);
        let rs = le::get_u64(&buf[ENTRY_HEADER_SIZE + 8..]);
        (kh, rs, ENTRY_HEADER_SIZE + 16)
    } else {
        (0u64, 0u64, ENTRY_HEADER_SIZE)
    };

    // Decode event.
    let event_tag = buf[event_tag_offset];
    let payload = &buf[event_tag_offset + 1..data_end];

    let event = match event_tag {
        TAG_ADD_INSTRUMENT => {
            if payload.len() < 12 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "AddInstrument payload too short",
                });
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let base = CurrencyId(le::get_u32(&payload[4..]));
            let quote = CurrencyId(le::get_u32(&payload[8..]));
            JournalEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol,
                    base,
                    quote,
                },
            }
        }
        TAG_DEPOSIT => {
            if payload.len() < 16 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "Deposit payload too short",
                });
            }
            JournalEvent::Deposit {
                account: AccountId(le::get_u32(&payload[0..])),
                currency: CurrencyId(le::get_u32(&payload[4..])),
                amount: le::get_u64(&payload[8..]),
            }
        }
        TAG_SUBMIT_ORDER => {
            if payload.len() < 4 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "SubmitOrder payload too short",
                });
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let (_, order) = decode_order(&payload[4..], sequence)?;
            JournalEvent::SubmitOrder { symbol, order }
        }
        TAG_CANCEL_ORDER => {
            // symbol(4) + account(4) + order_id(8) = 16
            if payload.len() < 16 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "CancelOrder payload too short",
                });
            }
            JournalEvent::CancelOrder {
                symbol: Symbol(le::get_u32(&payload[0..])),
                account: AccountId(le::get_u32(&payload[4..])),
                order_id: OrderId(le::get_u64(&payload[8..])),
            }
        }
        TAG_SET_RISK_LIMITS => {
            // symbol(4) + option_tag(1) [+ qty(8)] + option_tag(1) [+ notional(8)]
            if payload.len() < 6 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "SetRiskLimits payload too short",
                });
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let mut p = 4;
            let max_order_qty = match payload[p] {
                1 => {
                    p += 1;
                    if p + 8 > payload.len() {
                        return Err(JournalError::CorruptEntry {
                            sequence,
                            reason: "SetRiskLimits max_order_qty truncated",
                        });
                    }
                    let v = NonZeroU64::new(le::get_u64(&payload[p..])).ok_or(
                        JournalError::CorruptEntry {
                            sequence,
                            reason: "SetRiskLimits max_order_qty is zero",
                        },
                    )?;
                    p += 8;
                    Some(Quantity(v))
                }
                0 => {
                    p += 1;
                    None
                }
                _ => {
                    return Err(JournalError::CorruptEntry {
                        sequence,
                        reason: "SetRiskLimits invalid max_order_qty tag",
                    });
                }
            };
            if p >= payload.len() {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "SetRiskLimits max_order_notional tag missing",
                });
            }
            let max_order_notional = match payload[p] {
                1 => {
                    p += 1;
                    if p + 8 > payload.len() {
                        return Err(JournalError::CorruptEntry {
                            sequence,
                            reason: "SetRiskLimits max_order_notional truncated",
                        });
                    }
                    let v = le::get_u64(&payload[p..]);
                    Some(v)
                }
                0 => None,
                _ => {
                    return Err(JournalError::CorruptEntry {
                        sequence,
                        reason: "SetRiskLimits invalid max_order_notional tag",
                    });
                }
            };
            JournalEvent::SetRiskLimits {
                symbol,
                limits: RiskLimits {
                    max_order_qty,
                    max_order_notional,
                },
            }
        }
        TAG_CANCEL_ALL => {
            if payload.len() < 4 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "CancelAll payload too short",
                });
            }
            JournalEvent::CancelAll {
                account: AccountId(le::get_u32(&payload[0..])),
            }
        }
        TAG_SET_CIRCUIT_BREAKER => {
            // symbol(4) + option_tag(1) [+ price(8)] + option_tag(1) [+ price(8)] + halted(1)
            if payload.len() < 7 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "SetCircuitBreaker payload too short",
                });
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let mut p = 4;
            let price_band_lower = match payload[p] {
                1 => {
                    p += 1;
                    if p + 8 > payload.len() {
                        return Err(JournalError::CorruptEntry {
                            sequence,
                            reason: "SetCircuitBreaker price_band_lower truncated",
                        });
                    }
                    let v = NonZeroU64::new(le::get_u64(&payload[p..])).ok_or(
                        JournalError::CorruptEntry {
                            sequence,
                            reason: "SetCircuitBreaker price_band_lower is zero",
                        },
                    )?;
                    p += 8;
                    Some(Price(v))
                }
                0 => {
                    p += 1;
                    None
                }
                _ => {
                    return Err(JournalError::CorruptEntry {
                        sequence,
                        reason: "SetCircuitBreaker invalid price_band_lower tag",
                    });
                }
            };
            if p >= payload.len() {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "SetCircuitBreaker price_band_upper tag missing",
                });
            }
            let price_band_upper = match payload[p] {
                1 => {
                    p += 1;
                    if p + 8 > payload.len() {
                        return Err(JournalError::CorruptEntry {
                            sequence,
                            reason: "SetCircuitBreaker price_band_upper truncated",
                        });
                    }
                    let v = NonZeroU64::new(le::get_u64(&payload[p..])).ok_or(
                        JournalError::CorruptEntry {
                            sequence,
                            reason: "SetCircuitBreaker price_band_upper is zero",
                        },
                    )?;
                    p += 8;
                    Some(Price(v))
                }
                0 => {
                    p += 1;
                    None
                }
                _ => {
                    return Err(JournalError::CorruptEntry {
                        sequence,
                        reason: "SetCircuitBreaker invalid price_band_upper tag",
                    });
                }
            };
            if p >= payload.len() {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "SetCircuitBreaker halted byte missing",
                });
            }
            let halted = payload[p] != 0;
            JournalEvent::SetCircuitBreaker {
                symbol,
                config: CircuitBreakerConfig {
                    price_band_lower,
                    price_band_upper,
                    halted,
                },
            }
        }
        TAG_CANCEL_REPLACE => {
            // symbol(4) + account(4) + order_id(8) + new_price(8) + new_quantity(8) = 32
            if payload.len() < 32 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "CancelReplace payload too short",
                });
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let account = AccountId(le::get_u32(&payload[4..]));
            let order_id = OrderId(le::get_u64(&payload[8..]));
            let new_price =
                NonZeroU64::new(le::get_u64(&payload[16..])).ok_or(JournalError::CorruptEntry {
                    sequence,
                    reason: "CancelReplace new_price is zero",
                })?;
            let new_quantity =
                NonZeroU64::new(le::get_u64(&payload[24..])).ok_or(JournalError::CorruptEntry {
                    sequence,
                    reason: "CancelReplace new_quantity is zero",
                })?;
            JournalEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price: Price(new_price),
                new_quantity: Quantity(new_quantity),
            }
        }
        TAG_GENESIS_HASH => {
            if payload.len() < 32 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "GenesisHash payload too short",
                });
            }
            let mut hash = [0u8; 32];
            hash.copy_from_slice(&payload[..32]);
            JournalEvent::GenesisHash { hash }
        }
        TAG_CHECKPOINT => {
            // chain_hash(32) + events_since_checkpoint(8) = 40
            if payload.len() < 40 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "Checkpoint payload too short",
                });
            }
            let mut chain_hash = [0u8; 32];
            chain_hash.copy_from_slice(&payload[..32]);
            let events_since_checkpoint = le::get_u64(&payload[32..]);
            JournalEvent::Checkpoint {
                chain_hash,
                events_since_checkpoint,
            }
        }
        TAG_SET_FEE_SCHEDULE => {
            // symbol(4) + maker_fee_bps(2) + taker_fee_bps(2) = 8
            if payload.len() < 8 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "SetFeeSchedule payload too short",
                });
            }
            let symbol = Symbol(le::get_u32(&payload[0..]));
            let maker_fee_bps = le::get_i16(&payload[4..]);
            let taker_fee_bps = le::get_i16(&payload[6..]);
            JournalEvent::SetFeeSchedule {
                symbol,
                schedule: FeeSchedule {
                    maker_fee_bps,
                    taker_fee_bps,
                },
            }
        }
        TAG_PROVISION_ACCOUNT => {
            // account(4) + amount(8) = 12
            if payload.len() < 12 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "ProvisionAccount payload too short",
                });
            }
            let account = AccountId(le::get_u32(&payload[0..]));
            let amount = le::get_u64(&payload[4..]);
            JournalEvent::ProvisionAccount { account, amount }
        }
        TAG_WITHDRAW => {
            if payload.len() < 16 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "Withdraw payload too short",
                });
            }
            JournalEvent::Withdraw {
                account: AccountId(le::get_u32(&payload[0..])),
                currency: CurrencyId(le::get_u32(&payload[4..])),
                amount: le::get_u64(&payload[8..]),
            }
        }
        _ => {
            return Err(JournalError::CorruptEntry {
                sequence,
                reason: "unknown event tag",
            });
        }
    };

    Ok((
        total_len,
        sequence,
        timestamp_ns,
        key_hash,
        request_seq,
        event,
    ))
}

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

    pos
}

/// Decode an `Order` from `buf`. Returns `(bytes_consumed, Order)`.
fn decode_order(buf: &[u8], sequence: u64) -> Result<(usize, Order), JournalError> {
    let corrupt = |reason: &'static str| JournalError::CorruptEntry { sequence, reason };

    if buf.len() < 22 {
        return Err(corrupt("order payload too short"));
    }

    let mut pos = 0;
    let id = OrderId(le::get_u64(&buf[pos..]));
    pos += 8;
    let account = AccountId(le::get_u32(&buf[pos..]));
    pos += 4;
    let side = le::decode_side(buf[pos]).ok_or(corrupt("invalid side"))?;
    pos += 1;

    let order_type_tag = buf[pos];
    pos += 1;

    let order_type = match order_type_tag {
        ORDER_TYPE_MARKET => OrderType::Market,
        ORDER_TYPE_LIMIT | ORDER_TYPE_LIMIT_POST_ONLY => {
            if buf.len() < pos + 8 {
                return Err(corrupt("limit order missing price"));
            }
            let price =
                NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(corrupt("limit price is zero"))?;
            pos += 8;
            OrderType::Limit {
                price: Price(price),
                post_only: order_type_tag == ORDER_TYPE_LIMIT_POST_ONLY,
            }
        }
        ORDER_TYPE_STOP => {
            if buf.len() < pos + 8 {
                return Err(corrupt("stop order missing trigger price"));
            }
            let trigger = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(corrupt("stop trigger price is zero"))?;
            pos += 8;
            OrderType::Stop {
                trigger_price: Price(trigger),
            }
        }
        ORDER_TYPE_STOP_LIMIT => {
            if buf.len() < pos + 16 {
                return Err(corrupt("stop-limit order missing prices"));
            }
            let trigger = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(corrupt("stop-limit trigger price is zero"))?;
            pos += 8;
            let limit = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(corrupt("stop-limit limit price is zero"))?;
            pos += 8;
            OrderType::StopLimit {
                trigger_price: Price(trigger),
                limit_price: Price(limit),
            }
        }
        _ => return Err(corrupt("invalid order type tag")),
    };

    if buf.len() < pos + 10 {
        return Err(corrupt("order missing tif/quantity/stp"));
    }

    let time_in_force = le::decode_tif(buf[pos]).ok_or(corrupt("invalid time-in-force"))?;
    pos += 1;

    let quantity = NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(corrupt("quantity is zero"))?;
    pos += 8;

    let stp = le::decode_stp(buf[pos]).ok_or(corrupt("invalid self-trade protection"))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{CircuitBreakerConfig, FeeSchedule, SelfTradeProtection, Side, TimeInForce};
    use std::num::NonZeroU64;

    fn nz(v: u64) -> NonZeroU64 {
        NonZeroU64::new(v).unwrap()
    }

    fn make_events() -> Vec<JournalEvent> {
        vec![
            JournalEvent::AddInstrument {
                spec: InstrumentSpec {
                    symbol: Symbol(1),
                    base: CurrencyId(10),
                    quote: CurrencyId(20),
                },
            },
            JournalEvent::Deposit {
                account: AccountId(42),
                currency: CurrencyId(20),
                amount: 1_000_000,
            },
            JournalEvent::SubmitOrder {
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
                },
            },
            JournalEvent::SubmitOrder {
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
            JournalEvent::SubmitOrder {
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
            JournalEvent::SubmitOrder {
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
            JournalEvent::CancelOrder {
                symbol: Symbol(1),
                account: AccountId(42),
                order_id: OrderId(100),
            },
            JournalEvent::SetRiskLimits {
                symbol: Symbol(1),
                limits: RiskLimits {
                    max_order_qty: Some(Quantity(nz(1000))),
                    max_order_notional: Some(500_000),
                },
            },
            JournalEvent::SetRiskLimits {
                symbol: Symbol(2),
                limits: RiskLimits {
                    max_order_qty: None,
                    max_order_notional: None,
                },
            },
            JournalEvent::CancelAll {
                account: AccountId(42),
            },
            JournalEvent::SetCircuitBreaker {
                symbol: Symbol(1),
                config: CircuitBreakerConfig {
                    price_band_lower: Some(Price(nz(900))),
                    price_band_upper: Some(Price(nz(1100))),
                    halted: false,
                },
            },
            JournalEvent::SetCircuitBreaker {
                symbol: Symbol(2),
                config: CircuitBreakerConfig {
                    price_band_lower: None,
                    price_band_upper: None,
                    halted: true,
                },
            },
            JournalEvent::CancelReplace {
                symbol: Symbol(1),
                account: AccountId(42),
                order_id: OrderId(100),
                new_price: Price(nz(5500)),
                new_quantity: Quantity(nz(8)),
            },
            JournalEvent::GenesisHash { hash: [0xAB; 32] },
            JournalEvent::Checkpoint {
                chain_hash: [0xCD; 32],
                events_since_checkpoint: 100_000,
            },
            JournalEvent::SetFeeSchedule {
                symbol: Symbol(1),
                schedule: FeeSchedule {
                    maker_fee_bps: 5,
                    taker_fee_bps: 10,
                },
            },
            JournalEvent::ProvisionAccount {
                account: AccountId(42),
                amount: u64::MAX / 4,
            },
            JournalEvent::Withdraw {
                account: AccountId(42),
                currency: CurrencyId(20),
                amount: 500_000,
            },
        ]
    }

    #[test]
    fn round_trip_all_variants() {
        let events = make_events();
        let mut buf = [0u8; 128];

        for (i, event) in events.iter().enumerate() {
            let seq = (i as u64) + 1;
            let ts = 1_700_000_000_000_000_000 + (i as u64);

            let written = encode(seq, ts, 0, 0, event, &mut buf).unwrap();
            let (consumed, dec_seq, dec_ts, _kh, _rs, dec_event) =
                decode(&buf[..written], FORMAT_VERSION).unwrap();

            assert_eq!(consumed, written, "variant {i}");
            assert_eq!(dec_seq, seq, "variant {i}");
            assert_eq!(dec_ts, ts, "variant {i}");
            assert_eq!(&dec_event, event, "variant {i}");
        }
    }

    #[test]
    fn crc_corruption_detected() {
        let event = JournalEvent::Deposit {
            account: AccountId(1),
            currency: CurrencyId(2),
            amount: 999,
        };
        let mut buf = [0u8; 128];
        let written = encode(1, 0, 0, 0, &event, &mut buf).unwrap();

        // Flip a bit in the payload.
        buf[ENTRY_HEADER_SIZE + 2] ^= 0x01;

        let result = decode(&buf[..written], FORMAT_VERSION);
        assert!(
            matches!(result, Err(JournalError::ChecksumMismatch { .. })),
            "expected ChecksumMismatch, got {result:?}"
        );
    }

    #[test]
    fn truncated_entry_detected() {
        let event = JournalEvent::Deposit {
            account: AccountId(1),
            currency: CurrencyId(2),
            amount: 999,
        };
        let mut buf = [0u8; 128];
        let written = encode(1, 0, 0, 0, &event, &mut buf).unwrap();

        // Pass truncated buffer.
        let result = decode(&buf[..written - 1], FORMAT_VERSION);
        assert!(
            matches!(result, Err(JournalError::TruncatedEntry)),
            "expected TruncatedEntry, got {result:?}"
        );
    }

    #[test]
    fn file_header_round_trip() {
        let mut buf = [0u8; 8];
        encode_file_header(&mut buf);

        let version = decode_file_header(&buf).unwrap();
        assert_eq!(version, FORMAT_VERSION);
    }

    #[test]
    fn file_header_bad_magic() {
        let buf = [0u8; 8];
        assert!(matches!(
            decode_file_header(&buf),
            Err(JournalError::InvalidFile)
        ));
    }

    #[test]
    fn file_header_bad_version() {
        let mut buf = [0u8; 8];
        encode_file_header(&mut buf);
        // Overwrite version to 99.
        buf[4..6].copy_from_slice(&99u16.to_le_bytes());
        assert!(matches!(
            decode_file_header(&buf),
            Err(JournalError::UnsupportedVersion { version: 99 })
        ));
    }

    #[test]
    fn unknown_event_tag_is_corrupt() {
        let event = JournalEvent::Deposit {
            account: AccountId(1),
            currency: CurrencyId(2),
            amount: 100,
        };
        let mut buf = [0u8; 128];
        let written = encode(1, 0, 0, 0, &event, &mut buf).unwrap();

        // Overwrite event tag with invalid value, then fix CRC.
        // v8: event tag is at ENTRY_HEADER_SIZE + 16 (after key_hash + request_seq).
        buf[ENTRY_HEADER_SIZE + 16] = 255;
        let data_end = written - CRC_SIZE;
        let new_crc = crc32c::crc32c(&buf[..data_end]);
        buf[data_end..written].copy_from_slice(&new_crc.to_le_bytes());

        let result = decode(&buf[..written], FORMAT_VERSION);
        assert!(
            matches!(
                result,
                Err(JournalError::CorruptEntry {
                    reason: "unknown event tag",
                    ..
                })
            ),
            "expected CorruptEntry, got {result:?}"
        );
    }

    #[test]
    fn key_hash_and_request_seq_round_trip() {
        let event = JournalEvent::Deposit {
            account: AccountId(7),
            currency: CurrencyId(3),
            amount: 999,
        };
        let key_hash: u64 = 0xDEAD_BEEF_CAFE_1234;
        let request_seq: u64 = 42;
        let mut buf = [0u8; 256];
        let written = encode(1, 100, key_hash, request_seq, &event, &mut buf).unwrap();

        let (consumed, seq, _ts, kh, rs, decoded) =
            decode(&buf[..written], FORMAT_VERSION).unwrap();
        assert_eq!(consumed, written);
        assert_eq!(seq, 1);
        assert_eq!(kh, key_hash);
        assert_eq!(rs, request_seq);
        assert_eq!(decoded, event);
    }

    #[test]
    fn v7_decode_returns_zero_key_hash_and_seq() {
        // Simulate a v7 entry (no key_hash/request_seq) by encoding with the
        // old layout. Since we can't use the old encoder, manually build a v7
        // entry: header(20) + event_tag(1) + payload + crc(4).
        let event = JournalEvent::CancelAll {
            account: AccountId(5),
        };
        // Encode as v8 first, then manually construct a v7 entry.
        let mut buf_v8 = [0u8; 256];
        let _ = encode(10, 200, 0xAA, 0xBB, &event, &mut buf_v8).unwrap();

        // Build a v7 entry manually: header(20) + tag(1) + account(4) + crc(4)
        let mut buf_v7 = [0u8; 256];
        let payload_len: u16 = 1 + 4; // tag(1) + account(4)
        let mut h = 0;
        le::put_u16(&mut buf_v7[h..], ENTRY_MAGIC);
        h += 2;
        le::put_u16(&mut buf_v7[h..], payload_len);
        h += 2;
        le::put_u64(&mut buf_v7[h..], 10); // sequence
        h += 8;
        le::put_u64(&mut buf_v7[h..], 200); // timestamp
        h += 8;
        assert_eq!(h, ENTRY_HEADER_SIZE);
        buf_v7[h] = 6; // TAG_CANCEL_ALL
        h += 1;
        le::put_u32(&mut buf_v7[h..], 5); // account
        h += 4;
        let data_end = ENTRY_HEADER_SIZE + payload_len as usize;
        let crc = crc32c::crc32c(&buf_v7[..data_end]);
        le::put_u32(&mut buf_v7[data_end..], crc);
        let total = data_end + CRC_SIZE;

        let (consumed, seq, _ts, kh, rs, decoded) = decode(&buf_v7[..total], 7).unwrap();
        assert_eq!(consumed, total);
        assert_eq!(seq, 10);
        assert_eq!(kh, 0); // v7: no key_hash
        assert_eq!(rs, 0); // v7: no request_seq
        assert_eq!(decoded, event);
    }

    #[test]
    fn zero_key_hash_and_seq_encoded_correctly() {
        // Internal/seed events use key_hash=0, request_seq=0.
        let event = JournalEvent::Deposit {
            account: AccountId(1),
            currency: CurrencyId(2),
            amount: 100,
        };
        let mut buf = [0u8; 256];
        let written = encode(1, 50, 0, 0, &event, &mut buf).unwrap();
        let (_, _, _, kh, rs, decoded) = decode(&buf[..written], FORMAT_VERSION).unwrap();
        assert_eq!(kh, 0);
        assert_eq!(rs, 0);
        assert_eq!(decoded, event);
    }
}
