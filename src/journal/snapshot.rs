//! Snapshot save/load for Exchange state.
//!
//! Snapshots bridge version boundaries: before an engine upgrade, snapshot
//! current state; the new version loads the snapshot and starts a fresh
//! journal. Old journals are archived for audit (replayed only with the
//! matching engine version).
//!
//! Uses manual binary serialization (same approach as the journal codec)
//! to avoid serde dependency.
//!
//! ## File format
//!
//! | Field          | Type | Bytes | Purpose                       |
//! |----------------|------|-------|-------------------------------|
//! | file_magic     | u32  | 4     | `0x534E4150` ("SNAP")         |
//! | format_version | u16  | 2     | Current version = 1           |
//! | reserved       | u16  | 2     | Padding, zeroed               |
//! | sequence       | u64  | 8     | Journal sequence at snapshot   |
//! | data           | ...  | var   | Serialized Exchange state      |
//! | crc32c         | u32  | 4     | CRC32C of everything above     |

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::num::NonZeroU64;
use std::path::Path;

use crate::account::{AccountManager, Balance, Reservation};
use crate::exchange::Exchange;
use crate::orderbook::OrderBook;
use crate::types::{AccountId, CurrencyId, InstrumentSpec, OrderId, Price, Quantity, Side, Symbol};

use super::error::JournalError;
use super::le;

/// Decoded book-side levels: Vec of (price, orders-at-that-level).
type RestingLevels = Vec<(Price, Vec<RestingOrderSnapshot>)>;

/// Decoded stop-side levels: Vec of (trigger_price, stops-at-that-level).
type StopLevels = Vec<(Price, Vec<PendingStopSnapshot>)>;

/// Snapshot file magic: "SNAP" in ASCII (little-endian u32).
const SNAP_MAGIC: u32 = 0x534E_4150;

/// Current snapshot format version.
const SNAP_VERSION: u16 = 1;

/// Snapshot header size: magic(4) + version(2) + reserved(2) + sequence(8) = 16.
const SNAP_HEADER_SIZE: usize = 16;

/// Maximum snapshot file size (256 MiB). Prevents OOM from malicious or corrupt
/// files. A snapshot with millions of orders is well under this limit.
const MAX_SNAPSHOT_SIZE: u64 = 256 * 1024 * 1024;

/// Save a snapshot of the exchange state to disk.
///
/// The `journal_sequence` records the journal position at snapshot time,
/// so recovery knows where to start replaying.
pub fn save(exchange: &Exchange, journal_sequence: u64, path: &Path) -> Result<(), JournalError> {
    // Vec used as a growable byte buffer — avoids multiple small writes
    // to disk. The entire snapshot is built in memory then written atomically.
    let mut buf = Vec::with_capacity(4096);

    // Header.
    buf.extend_from_slice(&SNAP_MAGIC.to_le_bytes());
    buf.extend_from_slice(&SNAP_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes());
    buf.extend_from_slice(&journal_sequence.to_le_bytes());

    // Serialize exchange state.
    let state = exchange.snapshot_state();
    encode_exchange_state(&state, &mut buf);

    // CRC32C over everything.
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Write atomically: temp file → fsync → rename. A crash mid-write
    // leaves only the temp file; the previous snapshot (if any) is intact.
    let tmp_path = path.with_extension("snap.tmp");
    let mut file = File::create(&tmp_path)?;
    file.write_all(&buf)?;
    file.sync_data()?;
    drop(file);
    fs::rename(&tmp_path, path)?;

    Ok(())
}

/// Load a snapshot from disk. Returns the Exchange and the journal sequence
/// number at which to resume replay.
pub fn load(path: &Path) -> Result<(Exchange, u64), JournalError> {
    let mut file = File::open(path)?;

    // Check file size before reading to prevent OOM on malicious files.
    let metadata = file.metadata()?;
    if metadata.len() > MAX_SNAPSHOT_SIZE {
        return Err(JournalError::CorruptEntry {
            sequence: 0,
            reason: "snapshot file exceeds size limit",
        });
    }

    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    if buf.len() < SNAP_HEADER_SIZE + 4 {
        return Err(JournalError::TruncatedEntry);
    }

    // Validate CRC.
    let data_len = buf.len() - 4;
    let expected_crc = u32::from_le_bytes([
        buf[data_len],
        buf[data_len + 1],
        buf[data_len + 2],
        buf[data_len + 3],
    ]);
    let actual_crc = crc32c::crc32c(&buf[..data_len]);
    if expected_crc != actual_crc {
        return Err(JournalError::ChecksumMismatch {
            sequence: 0,
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    // Validate header.
    let magic = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != SNAP_MAGIC {
        return Err(JournalError::InvalidFile);
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != SNAP_VERSION {
        return Err(JournalError::UnsupportedVersion { version });
    }
    let sequence = u64::from_le_bytes([
        buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
    ]);

    // Decode exchange state.
    let (_, state) = decode_exchange_state(&buf[SNAP_HEADER_SIZE..data_len])?;
    let exchange = Exchange::restore_state(state);

    Ok((exchange, sequence))
}

/// Serialized exchange state — all the data needed to reconstruct an Exchange.
///
/// Separate from Exchange to keep serialization concerns out of the core
/// engine types. Uses Vec (not HashMap) for deterministic-order serialization.
#[derive(Debug)]
pub(crate) struct ExchangeSnapshot {
    pub(crate) instruments: Vec<InstrumentSpec>,
    pub(crate) balances: Vec<((AccountId, CurrencyId), Balance)>,
    pub(crate) reservations: Vec<(OrderId, AccountId, CurrencyId, u64)>,
    pub(crate) order_sides: Vec<(OrderId, Side)>,
    pub(crate) books: Vec<(Symbol, BookSnapshot)>,
}

/// Serialized order book state for a single instrument.
/// Uses Vec for each level to preserve insertion-order fidelity.
#[derive(Debug)]
pub(crate) struct BookSnapshot {
    pub(crate) bids: Vec<(Price, Vec<RestingOrderSnapshot>)>,
    pub(crate) asks: Vec<(Price, Vec<RestingOrderSnapshot>)>,
    pub(crate) order_index: Vec<(OrderId, Side, Price)>,
    pub(crate) stop_buys: Vec<(Price, Vec<PendingStopSnapshot>)>,
    pub(crate) stop_sells: Vec<(Price, Vec<PendingStopSnapshot>)>,
    pub(crate) stop_index: Vec<(OrderId, Side, Price)>,
    pub(crate) last_trade_price: Option<Price>,
}

/// Serialized resting order.
#[derive(Debug)]
pub(crate) struct RestingOrderSnapshot {
    pub(crate) id: OrderId,
    pub(crate) account: AccountId,
    pub(crate) remaining: Quantity,
}

/// Serialized pending stop.
#[derive(Debug)]
pub(crate) struct PendingStopSnapshot {
    pub(crate) id: OrderId,
    pub(crate) account: AccountId,
    pub(crate) side: Side,
    pub(crate) trigger_price: Price,
    pub(crate) quantity: Quantity,
    pub(crate) time_in_force: crate::types::TimeInForce,
    pub(crate) limit_price: Option<Price>,
}

// --- Encoding helpers ---

fn encode_exchange_state(state: &ExchangeSnapshot, buf: &mut Vec<u8>) {
    // Instruments.
    le::push_u32(buf, state.instruments.len() as u32);
    for spec in &state.instruments {
        le::push_u32(buf, spec.symbol.0);
        le::push_u32(buf, spec.base.0);
        le::push_u32(buf, spec.quote.0);
    }

    // Balances.
    le::push_u32(buf, state.balances.len() as u32);
    for ((account, currency), balance) in &state.balances {
        le::push_u32(buf, account.0);
        le::push_u32(buf, currency.0);
        le::push_u64(buf, balance.available);
        le::push_u64(buf, balance.reserved);
    }

    // Reservations.
    le::push_u32(buf, state.reservations.len() as u32);
    for (order_id, account, currency, remaining) in &state.reservations {
        le::push_u64(buf, order_id.0);
        le::push_u32(buf, account.0);
        le::push_u32(buf, currency.0);
        le::push_u64(buf, *remaining);
    }

    // Order sides.
    le::push_u32(buf, state.order_sides.len() as u32);
    for (order_id, side) in &state.order_sides {
        le::push_u64(buf, order_id.0);
        buf.push(le::encode_side(*side));
    }

    // Books.
    le::push_u32(buf, state.books.len() as u32);
    for (symbol, book) in &state.books {
        le::push_u32(buf, symbol.0);
        encode_book_snapshot(book, buf);
    }
}

fn encode_book_snapshot(book: &BookSnapshot, buf: &mut Vec<u8>) {
    // Bids.
    encode_book_side(&book.bids, buf);
    // Asks.
    encode_book_side(&book.asks, buf);

    // Order index.
    le::push_u32(buf, book.order_index.len() as u32);
    for (order_id, side, price) in &book.order_index {
        le::push_u64(buf, order_id.0);
        buf.push(le::encode_side(*side));
        le::push_u64(buf, price.get());
    }

    // Stop buys.
    encode_stop_side(&book.stop_buys, buf);
    // Stop sells.
    encode_stop_side(&book.stop_sells, buf);

    // Stop index.
    le::push_u32(buf, book.stop_index.len() as u32);
    for (order_id, side, price) in &book.stop_index {
        le::push_u64(buf, order_id.0);
        buf.push(le::encode_side(*side));
        le::push_u64(buf, price.get());
    }

    // Last trade price.
    match book.last_trade_price {
        Some(p) => {
            buf.push(1);
            le::push_u64(buf, p.get());
        }
        None => buf.push(0),
    }
}

fn encode_book_side(levels: &[(Price, Vec<RestingOrderSnapshot>)], buf: &mut Vec<u8>) {
    le::push_u32(buf, levels.len() as u32);
    for (price, orders) in levels {
        le::push_u64(buf, price.get());
        le::push_u32(buf, orders.len() as u32);
        for order in orders {
            le::push_u64(buf, order.id.0);
            le::push_u32(buf, order.account.0);
            le::push_u64(buf, order.remaining.get());
        }
    }
}

fn encode_stop_side(levels: &[(Price, Vec<PendingStopSnapshot>)], buf: &mut Vec<u8>) {
    le::push_u32(buf, levels.len() as u32);
    for (trigger_price, stops) in levels {
        le::push_u64(buf, trigger_price.get());
        le::push_u32(buf, stops.len() as u32);
        for stop in stops {
            le::push_u64(buf, stop.id.0);
            le::push_u32(buf, stop.account.0);
            buf.push(le::encode_side(stop.side));
            le::push_u64(buf, stop.trigger_price.get());
            le::push_u64(buf, stop.quantity.get());
            buf.push(le::encode_tif(stop.time_in_force));
            match stop.limit_price {
                Some(p) => {
                    buf.push(1);
                    le::push_u64(buf, p.get());
                }
                None => buf.push(0),
            }
        }
    }
}

// --- Decoding helpers ---

/// Validate that a claimed count `n` of items each `item_size` bytes can
/// actually fit in the remaining buffer. Prevents memory exhaustion from
/// crafted count values.
fn validate_count(remaining: usize, n: usize, item_size: usize) -> Result<(), JournalError> {
    let needed = n.saturating_mul(item_size);
    if needed > remaining {
        Err(JournalError::CorruptEntry {
            sequence: 0,
            reason: "count exceeds remaining buffer",
        })
    } else {
        Ok(())
    }
}

fn decode_exchange_state(buf: &[u8]) -> Result<(usize, ExchangeSnapshot), JournalError> {
    let corrupt = |reason: &'static str| JournalError::CorruptEntry {
        sequence: 0,
        reason,
    };
    let mut pos = 0;

    let check = |pos: usize, need: usize| -> Result<(), JournalError> {
        if pos + need > buf.len() {
            Err(JournalError::TruncatedEntry)
        } else {
            Ok(())
        }
    };

    // Instruments.
    check(pos, 4)?;
    let n_instruments = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    validate_count(buf.len() - pos, n_instruments, 12)?;
    let mut instruments = Vec::with_capacity(n_instruments);
    for _ in 0..n_instruments {
        check(pos, 12)?;
        instruments.push(InstrumentSpec {
            symbol: Symbol(le::get_u32(&buf[pos..])),
            base: CurrencyId(le::get_u32(&buf[pos + 4..])),
            quote: CurrencyId(le::get_u32(&buf[pos + 8..])),
        });
        pos += 12;
    }

    // Balances.
    check(pos, 4)?;
    let n_balances = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    validate_count(buf.len() - pos, n_balances, 24)?;
    let mut balances = Vec::with_capacity(n_balances);
    for _ in 0..n_balances {
        check(pos, 24)?;
        let account = AccountId(le::get_u32(&buf[pos..]));
        let currency = CurrencyId(le::get_u32(&buf[pos + 4..]));
        let available = le::get_u64(&buf[pos + 8..]);
        let reserved = le::get_u64(&buf[pos + 16..]);
        balances.push((
            (account, currency),
            Balance {
                available,
                reserved,
            },
        ));
        pos += 24;
    }

    // Reservations.
    check(pos, 4)?;
    let n_reservations = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    validate_count(buf.len() - pos, n_reservations, 24)?;
    let mut reservations = Vec::with_capacity(n_reservations);
    for _ in 0..n_reservations {
        check(pos, 24)?;
        let order_id = OrderId(le::get_u64(&buf[pos..]));
        let account = AccountId(le::get_u32(&buf[pos + 8..]));
        let currency = CurrencyId(le::get_u32(&buf[pos + 12..]));
        let remaining = le::get_u64(&buf[pos + 16..]);
        reservations.push((order_id, account, currency, remaining));
        pos += 24;
    }

    // Order sides.
    check(pos, 4)?;
    let n_order_sides = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    validate_count(buf.len() - pos, n_order_sides, 9)?;
    let mut order_sides = Vec::with_capacity(n_order_sides);
    for _ in 0..n_order_sides {
        check(pos, 9)?;
        let order_id = OrderId(le::get_u64(&buf[pos..]));
        let side = le::decode_side(buf[pos + 8]).ok_or(corrupt("invalid side in snapshot"))?;
        order_sides.push((order_id, side));
        pos += 9;
    }

    // Books.
    check(pos, 4)?;
    let n_books = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    // Minimum per-book overhead: at least a few bytes for the empty-book structure.
    validate_count(buf.len() - pos, n_books, 4)?;
    let mut books = Vec::with_capacity(n_books);
    for _ in 0..n_books {
        check(pos, 4)?;
        let symbol = Symbol(le::get_u32(&buf[pos..]));
        pos += 4;
        let (consumed, book) = decode_book_snapshot(&buf[pos..])?;
        pos += consumed;
        books.push((symbol, book));
    }

    Ok((
        pos,
        ExchangeSnapshot {
            instruments,
            balances,
            reservations,
            order_sides,
            books,
        },
    ))
}

fn decode_book_snapshot(buf: &[u8]) -> Result<(usize, BookSnapshot), JournalError> {
    let corrupt = |reason: &'static str| JournalError::CorruptEntry {
        sequence: 0,
        reason,
    };
    let mut pos = 0;

    let check = |pos: usize, need: usize| -> Result<(), JournalError> {
        if pos + need > buf.len() {
            Err(JournalError::TruncatedEntry)
        } else {
            Ok(())
        }
    };

    // Bids.
    let (consumed, bids) = decode_book_side_levels(&buf[pos..])?;
    pos += consumed;

    // Asks.
    let (consumed, asks) = decode_book_side_levels(&buf[pos..])?;
    pos += consumed;

    // Order index.
    check(pos, 4)?;
    let n_order_index = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    validate_count(buf.len() - pos, n_order_index, 17)?;
    let mut order_index = Vec::with_capacity(n_order_index);
    for _ in 0..n_order_index {
        check(pos, 17)?;
        let order_id = OrderId(le::get_u64(&buf[pos..]));
        let side = le::decode_side(buf[pos + 8]).ok_or(corrupt("invalid side"))?;
        let price_val =
            NonZeroU64::new(le::get_u64(&buf[pos + 9..])).ok_or(corrupt("zero price in index"))?;
        order_index.push((order_id, side, Price(price_val)));
        pos += 17;
    }

    // Stop buys.
    let (consumed, stop_buys) = decode_stop_side_levels(&buf[pos..])?;
    pos += consumed;

    // Stop sells.
    let (consumed, stop_sells) = decode_stop_side_levels(&buf[pos..])?;
    pos += consumed;

    // Stop index.
    check(pos, 4)?;
    let n_stop_index = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    validate_count(buf.len() - pos, n_stop_index, 17)?;
    let mut stop_index = Vec::with_capacity(n_stop_index);
    for _ in 0..n_stop_index {
        check(pos, 17)?;
        let order_id = OrderId(le::get_u64(&buf[pos..]));
        let side = le::decode_side(buf[pos + 8]).ok_or(corrupt("invalid side"))?;
        let price_val = NonZeroU64::new(le::get_u64(&buf[pos + 9..]))
            .ok_or(corrupt("zero price in stop index"))?;
        stop_index.push((order_id, side, Price(price_val)));
        pos += 17;
    }

    // Last trade price.
    check(pos, 1)?;
    let last_trade_price = match buf[pos] {
        1 => {
            pos += 1;
            check(pos, 8)?;
            let p = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(corrupt("zero last trade price"))?;
            pos += 8;
            Some(Price(p))
        }
        0 => {
            pos += 1;
            None
        }
        _ => return Err(corrupt("invalid last_trade_price tag")),
    };

    Ok((
        pos,
        BookSnapshot {
            bids,
            asks,
            order_index,
            stop_buys,
            stop_sells,
            stop_index,
            last_trade_price,
        },
    ))
}

fn decode_book_side_levels(buf: &[u8]) -> Result<(usize, RestingLevels), JournalError> {
    let corrupt = |reason: &'static str| JournalError::CorruptEntry {
        sequence: 0,
        reason,
    };
    let mut pos = 0;

    if buf.len() < 4 {
        return Err(JournalError::TruncatedEntry);
    }
    let n_levels = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    // Each level has at least 12 bytes (price + order count).
    validate_count(buf.len() - pos, n_levels, 12)?;

    let mut levels = Vec::with_capacity(n_levels);
    for _ in 0..n_levels {
        if pos + 12 > buf.len() {
            return Err(JournalError::TruncatedEntry);
        }
        let price_val =
            NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(corrupt("zero price in book level"))?;
        pos += 8;
        let n_orders = le::get_u32(&buf[pos..]) as usize;
        pos += 4;

        // Each order is 20 bytes.
        validate_count(buf.len() - pos, n_orders, 20)?;
        let mut orders = Vec::with_capacity(n_orders);
        for _ in 0..n_orders {
            if pos + 20 > buf.len() {
                return Err(JournalError::TruncatedEntry);
            }
            let id = OrderId(le::get_u64(&buf[pos..]));
            let account = AccountId(le::get_u32(&buf[pos + 8..]));
            let remaining_val = NonZeroU64::new(le::get_u64(&buf[pos + 12..]))
                .ok_or(corrupt("zero remaining quantity"))?;
            orders.push(RestingOrderSnapshot {
                id,
                account,
                remaining: Quantity(remaining_val),
            });
            pos += 20;
        }
        levels.push((Price(price_val), orders));
    }

    Ok((pos, levels))
}

fn decode_stop_side_levels(buf: &[u8]) -> Result<(usize, StopLevels), JournalError> {
    let corrupt = |reason: &'static str| JournalError::CorruptEntry {
        sequence: 0,
        reason,
    };
    let mut pos = 0;

    if buf.len() < 4 {
        return Err(JournalError::TruncatedEntry);
    }
    let n_levels = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    // Each level has at least 12 bytes (trigger price + stop count).
    validate_count(buf.len() - pos, n_levels, 12)?;

    let mut levels = Vec::with_capacity(n_levels);
    for _ in 0..n_levels {
        if pos + 12 > buf.len() {
            return Err(JournalError::TruncatedEntry);
        }
        let trigger_val = NonZeroU64::new(le::get_u64(&buf[pos..]))
            .ok_or(corrupt("zero trigger price in stop level"))?;
        pos += 8;
        let n_stops = le::get_u32(&buf[pos..]) as usize;
        pos += 4;

        // Each stop is at least 31 bytes.
        validate_count(buf.len() - pos, n_stops, 31)?;
        let mut stops = Vec::with_capacity(n_stops);
        for _ in 0..n_stops {
            // id(8) + account(4) + side(1) + trigger(8) + qty(8) + tif(1) + limit_tag(1) = 31 min
            if pos + 31 > buf.len() {
                return Err(JournalError::TruncatedEntry);
            }
            let id = OrderId(le::get_u64(&buf[pos..]));
            pos += 8;
            let account = AccountId(le::get_u32(&buf[pos..]));
            pos += 4;
            let side = le::decode_side(buf[pos]).ok_or(corrupt("invalid side in stop"))?;
            pos += 1;
            let tp = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(corrupt("zero trigger price in stop"))?;
            pos += 8;
            let qty = NonZeroU64::new(le::get_u64(&buf[pos..]))
                .ok_or(corrupt("zero quantity in stop"))?;
            pos += 8;
            let tif = le::decode_tif(buf[pos]).ok_or(corrupt("invalid tif in stop"))?;
            pos += 1;

            let limit_price = match buf[pos] {
                1 => {
                    pos += 1;
                    if pos + 8 > buf.len() {
                        return Err(JournalError::TruncatedEntry);
                    }
                    let lp = NonZeroU64::new(le::get_u64(&buf[pos..]))
                        .ok_or(corrupt("zero limit price in stop"))?;
                    pos += 8;
                    Some(Price(lp))
                }
                0 => {
                    pos += 1;
                    None
                }
                _ => return Err(corrupt("invalid limit_price tag in stop")),
            };

            stops.push(PendingStopSnapshot {
                id,
                account,
                side,
                trigger_price: Price(tp),
                quantity: Quantity(qty),
                time_in_force: tif,
                limit_price,
            });
        }
        levels.push((Price(trigger_val), stops));
    }

    Ok((pos, levels))
}

// --- Conversion: ExchangeSnapshot <-> actual types ---

impl Exchange {
    /// Create a snapshot of all internal state for serialization.
    pub(crate) fn snapshot_state(&self) -> ExchangeSnapshot {
        let instruments: Vec<InstrumentSpec> = self.instruments().values().copied().collect();
        let balances = self.accounts().snapshot_balances();
        let reservations = self.accounts().snapshot_reservations();
        let order_sides: Vec<(OrderId, Side)> = self.snapshot_order_sides();

        let books: Vec<(Symbol, BookSnapshot)> = self
            .books()
            .iter()
            .map(|(&symbol, book)| (symbol, book.snapshot()))
            .collect();

        ExchangeSnapshot {
            instruments,
            balances,
            reservations,
            order_sides,
            books,
        }
    }

    /// Reconstruct an Exchange from a snapshot.
    pub(crate) fn restore_state(state: ExchangeSnapshot) -> Self {
        let mut instruments_map = HashMap::new();
        let mut books_map = HashMap::new();

        for spec in &state.instruments {
            instruments_map.insert(spec.symbol, *spec);
        }

        for (symbol, book_snap) in state.books {
            books_map.insert(symbol, OrderBook::restore(book_snap));
        }

        // Ensure all instruments have books (some may have been empty).
        for spec in &state.instruments {
            books_map.entry(spec.symbol).or_default();
        }

        let accounts = AccountManager::restore(state.balances, state.reservations);
        let order_sides: HashMap<OrderId, Side> = state.order_sides.into_iter().collect();

        Self::from_parts(books_map, instruments_map, accounts, order_sides)
    }
}

impl AccountManager {
    /// Snapshot all balances for serialization.
    pub(crate) fn snapshot_balances(&self) -> Vec<((AccountId, CurrencyId), Balance)> {
        self.balances_iter().map(|(&k, &v)| (k, v)).collect()
    }

    /// Snapshot all reservations for serialization.
    pub(crate) fn snapshot_reservations(&self) -> Vec<(OrderId, AccountId, CurrencyId, u64)> {
        self.reservations_iter()
            .map(|(&order_id, res)| (order_id, res.account(), res.currency(), res.remaining()))
            .collect()
    }

    /// Restore from snapshot data.
    pub(crate) fn restore(
        balances: Vec<((AccountId, CurrencyId), Balance)>,
        reservations: Vec<(OrderId, AccountId, CurrencyId, u64)>,
    ) -> Self {
        let balance_map: HashMap<(AccountId, CurrencyId), Balance> = balances.into_iter().collect();
        let reservation_map: HashMap<OrderId, Reservation> = reservations
            .into_iter()
            .map(|(order_id, account, currency, remaining)| {
                (order_id, Reservation::new(account, currency, remaining))
            })
            .collect();
        Self::from_parts(balance_map, reservation_map)
    }
}

impl OrderBook {
    /// Create a snapshot of the order book state.
    pub(crate) fn snapshot(&self) -> BookSnapshot {
        let snapshot_side =
            |side: &crate::orderbook::BookSide| -> Vec<(Price, Vec<RestingOrderSnapshot>)> {
                side.levels_iter()
                    .map(|(&price, queue)| {
                        let orders = queue
                            .iter()
                            .map(|o| RestingOrderSnapshot {
                                id: o.id(),
                                account: o.account(),
                                remaining: o.remaining(),
                            })
                            .collect();
                        (price, orders)
                    })
                    .collect()
            };

        let snapshot_stops = |stops: &BTreeMap<Price, Vec<crate::orderbook::PendingStop>>| {
            stops
                .iter()
                .map(|(&trigger_price, pending)| {
                    let snaps = pending
                        .iter()
                        .map(|s| PendingStopSnapshot {
                            id: s.id(),
                            account: s.account(),
                            side: s.side(),
                            trigger_price: s.trigger_price(),
                            quantity: s.quantity(),
                            time_in_force: s.time_in_force(),
                            limit_price: s.limit_price(),
                        })
                        .collect();
                    (trigger_price, snaps)
                })
                .collect()
        };

        BookSnapshot {
            bids: snapshot_side(self.bids()),
            asks: snapshot_side(self.asks()),
            order_index: self.snapshot_order_index(),
            stop_buys: snapshot_stops(self.stop_buys()),
            stop_sells: snapshot_stops(self.stop_sells()),
            stop_index: self.snapshot_stop_index(),
            last_trade_price: self.last_trade_price(),
        }
    }

    /// Restore an order book from a snapshot.
    pub(crate) fn restore(snap: BookSnapshot) -> Self {
        let restore_side = |levels: Vec<(Price, Vec<RestingOrderSnapshot>)>| {
            let mut btree = BTreeMap::new();
            for (price, orders) in levels {
                let queue: VecDeque<crate::orderbook::RestingOrder> = orders
                    .into_iter()
                    .map(|o| crate::orderbook::RestingOrder::new(o.id, o.account, o.remaining))
                    .collect();
                btree.insert(price, queue);
            }
            crate::orderbook::BookSide::from_levels(btree)
        };

        let restore_stops = |levels: Vec<(Price, Vec<PendingStopSnapshot>)>| {
            let mut btree = BTreeMap::new();
            for (trigger_price, stops) in levels {
                let pending: Vec<crate::orderbook::PendingStop> = stops
                    .into_iter()
                    .map(|s| {
                        crate::orderbook::PendingStop::new(
                            s.id,
                            s.account,
                            s.side,
                            s.trigger_price,
                            s.quantity,
                            s.time_in_force,
                            s.limit_price,
                        )
                    })
                    .collect();
                btree.insert(trigger_price, pending);
            }
            btree
        };

        let order_index: HashMap<OrderId, (Side, Price)> = snap
            .order_index
            .into_iter()
            .map(|(id, side, price)| (id, (side, price)))
            .collect();

        let stop_index: HashMap<OrderId, (Side, Price)> = snap
            .stop_index
            .into_iter()
            .map(|(id, side, price)| (id, (side, price)))
            .collect();

        Self::from_parts(
            restore_side(snap.bids),
            restore_side(snap.asks),
            order_index,
            restore_stops(snap.stop_buys),
            restore_stops(snap.stop_sells),
            stop_index,
            snap.last_trade_price,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;
    use crate::exchange::Exchange;
    use crate::types::*;

    const ACCT_A: AccountId = AccountId(1);
    const ACCT_B: AccountId = AccountId(2);
    const BTC: CurrencyId = CurrencyId(0);
    const USD: CurrencyId = CurrencyId(1);

    fn btc_usd_spec() -> InstrumentSpec {
        InstrumentSpec {
            symbol: Symbol(1),
            base: BTC,
            quote: USD,
        }
    }

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    fn price_val(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn limit_order(id: u64, account: AccountId, side: Side, p: u64, q: u64) -> Order {
        Order {
            id: OrderId(id),
            account,
            side,
            order_type: OrderType::Limit {
                price: price_val(p),
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(q),
        }
    }

    #[test]
    fn snapshot_save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.snapshot");

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 500);

        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 50),
            &mut reports,
        );
        exchange.execute(
            Symbol(1),
            limit_order(2, ACCT_A, Side::Buy, 100, 30),
            &mut reports,
        );

        save(&exchange, 42, &path).unwrap();

        let (restored, seq) = load(&path).unwrap();
        assert_eq!(seq, 42);
        assert_eq!(
            restored.accounts().balance(ACCT_A, USD).available,
            exchange.accounts().balance(ACCT_A, USD).available
        );
        assert_eq!(
            restored.accounts().balance(ACCT_A, USD).reserved,
            exchange.accounts().balance(ACCT_A, USD).reserved
        );
        assert_eq!(
            restored.accounts().balance(ACCT_A, BTC).available,
            exchange.accounts().balance(ACCT_A, BTC).available
        );
        assert_eq!(
            restored.accounts().balance(ACCT_B, USD).available,
            exchange.accounts().balance(ACCT_B, USD).available
        );
        assert_eq!(
            restored.accounts().balance(ACCT_B, BTC).available,
            exchange.accounts().balance(ACCT_B, BTC).available
        );
        assert_eq!(
            restored.accounts().balance(ACCT_B, BTC).reserved,
            exchange.accounts().balance(ACCT_B, BTC).reserved
        );
    }

    #[test]
    fn snapshot_with_resting_orders_replays_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resting.snapshot");

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);
        exchange.deposit(ACCT_B, BTC, 500);

        let mut reports = Vec::new();
        // Place resting sell.
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 50),
            &mut reports,
        );
        reports.clear();

        save(&exchange, 10, &path).unwrap();

        let (mut restored, _seq) = load(&path).unwrap();

        // Buy should match against the resting sell from snapshot.
        let mut new_reports = Vec::new();
        restored.execute(
            Symbol(1),
            limit_order(2, ACCT_A, Side::Buy, 100, 20),
            &mut new_reports,
        );

        assert!(matches!(new_reports[0], ExecutionReport::Fill { .. }));
        assert_eq!(restored.accounts().balance(ACCT_A, BTC).available, 20);
    }

    #[test]
    fn corrupt_snapshot_detected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.snapshot");

        let exchange = Exchange::new();
        save(&exchange, 0, &path).unwrap();

        // Corrupt a byte.
        let mut data = std::fs::read(&path).unwrap();
        data[SNAP_HEADER_SIZE] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        assert!(matches!(
            load(&path),
            Err(JournalError::ChecksumMismatch { .. })
        ));
    }
}
