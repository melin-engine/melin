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
//! | format_version | u16  | 2     | Current version = 1                    |
//! | reserved       | u16  | 2     | Padding for alignment, zeroed          |
//!
//! ## Entry layout (little-endian, repeats after file header)
//!
//! | Field        | Type   | Bytes | Purpose                               |
//! |--------------|--------|-------|---------------------------------------|
//! | magic        | u16    | 2     | `0x4A45` — misalignment detection     |
//! | length       | u16    | 2     | Byte count after header, before CRC   |
//! |              |        |       | (includes event_tag + payload)        |
//! | sequence     | u64    | 8     | Monotonically increasing, starts at 1 |
//! | timestamp_ns | u64    | 8     | Wall-clock nanos since epoch           |
//! | event_tag    | u8     | 1     | Discriminant for JournalEvent variant  |
//! | payload      | varies | ≤64   | Variant fields                        |
//! | crc32c       | u32    | 4     | CRC32C of all preceding bytes         |
//!
//! `length` = size of (event_tag + payload). Total entry size = 20 + length + 4.

use std::num::NonZeroU64;

use crate::types::{
    AccountId, CurrencyId, InstrumentSpec, Order, OrderId, OrderType, Price, Quantity, Symbol,
};

use super::error::JournalError;
use super::event::JournalEvent;
use super::le;

/// File magic bytes: "JOUR" in ASCII (little-endian u32).
pub const FILE_MAGIC: u32 = 0x4A4F_5552;

/// Current format version. Bumped on any layout change.
pub const FORMAT_VERSION: u16 = 1;

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

/// OrderType tag encoding (codec-specific, not shared — order types are only
/// in the journal format, not in snapshots).
const ORDER_TYPE_MARKET: u8 = 0;
const ORDER_TYPE_LIMIT: u8 = 1;
const ORDER_TYPE_STOP: u8 = 2;
const ORDER_TYPE_STOP_LIMIT: u8 = 3;

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
    if version != FORMAT_VERSION {
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
    event: &JournalEvent,
    buf: &mut [u8],
) -> Result<usize, JournalError> {
    // Leave room for header, write event_tag + payload after it.
    let payload_start = ENTRY_HEADER_SIZE + 1; // +1 for event_tag
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
        JournalEvent::CancelOrder { symbol, order_id } => {
            le::put_u32(&mut buf[pos..], symbol.0);
            pos += 4;
            le::put_u64(&mut buf[pos..], order_id.0);
            pos += 8;
            TAG_CANCEL_ORDER
        }
    };

    // `length` covers event_tag(1) + payload bytes.
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

    // Write event tag.
    buf[ENTRY_HEADER_SIZE] = event_tag;

    // CRC32C over everything before the checksum.
    let crc = crc32c::crc32c(&buf[..pos]);
    le::put_u32(&mut buf[pos..], crc);
    pos += CRC_SIZE;

    Ok(pos)
}

/// Decode a journal entry from `buf`.
///
/// Returns `(bytes_consumed, sequence, timestamp_ns, event)` on success.
/// Returns `Err(TruncatedEntry)` if the buffer doesn't contain a complete entry.
pub fn decode(buf: &[u8]) -> Result<(usize, u64, u64, JournalEvent), JournalError> {
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

    // Decode event.
    let event_tag = buf[ENTRY_HEADER_SIZE];
    let payload = &buf[ENTRY_HEADER_SIZE + 1..data_end];

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
            if payload.len() < 12 {
                return Err(JournalError::CorruptEntry {
                    sequence,
                    reason: "CancelOrder payload too short",
                });
            }
            JournalEvent::CancelOrder {
                symbol: Symbol(le::get_u32(&payload[0..])),
                order_id: OrderId(le::get_u64(&payload[4..])),
            }
        }
        _ => {
            return Err(JournalError::CorruptEntry {
                sequence,
                reason: "unknown event tag",
            });
        }
    };

    Ok((total_len, sequence, timestamp_ns, event))
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
        ORDER_TYPE_LIMIT => {
            if buf.len() < pos + 8 {
                return Err(corrupt("limit order missing price"));
            }
            let price =
                NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(corrupt("limit price is zero"))?;
            pos += 8;
            OrderType::Limit {
                price: Price(price),
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

    if buf.len() < pos + 9 {
        return Err(corrupt("order missing tif/quantity"));
    }

    let time_in_force = le::decode_tif(buf[pos]).ok_or(corrupt("invalid time-in-force"))?;
    pos += 1;

    let quantity = NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(corrupt("quantity is zero"))?;
    pos += 8;

    Ok((
        pos,
        Order {
            id,
            account,
            side,
            order_type,
            time_in_force,
            quantity: Quantity(quantity),
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Side, TimeInForce};
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
                    },
                    time_in_force: TimeInForce::GTC,
                    quantity: Quantity(nz(10)),
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
                },
            },
            JournalEvent::CancelOrder {
                symbol: Symbol(1),
                order_id: OrderId(100),
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

            let written = encode(seq, ts, event, &mut buf).unwrap();
            let (consumed, dec_seq, dec_ts, dec_event) = decode(&buf[..written]).unwrap();

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
        let written = encode(1, 0, &event, &mut buf).unwrap();

        // Flip a bit in the payload.
        buf[ENTRY_HEADER_SIZE + 2] ^= 0x01;

        let result = decode(&buf[..written]);
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
        let written = encode(1, 0, &event, &mut buf).unwrap();

        // Pass truncated buffer.
        let result = decode(&buf[..written - 1]);
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
        let written = encode(1, 0, &event, &mut buf).unwrap();

        // Overwrite event tag with invalid value, then fix CRC.
        buf[ENTRY_HEADER_SIZE] = 255;
        let data_end = written - CRC_SIZE;
        let new_crc = crc32c::crc32c(&buf[..data_end]);
        buf[data_end..written].copy_from_slice(&new_crc.to_le_bytes());

        let result = decode(&buf[..written]);
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
}
