//! Payload codec for Exchange snapshot state.
//!
//! Snapshots bridge version boundaries: before an engine upgrade, snapshot
//! current state; the new version loads the snapshot and starts a fresh
//! journal. Old journals are archived for audit (replayed only with the
//! matching engine version).
//!
//! Uses manual binary serialization (same approach as the journal codec)
//! to avoid serde dependency.
//!
//! On-disk framing (magic, versions, sequence, chain hash, CRC, atomic
//! rename) lives in `melin_transport_core::snapshot` — generic over the
//! `melin_app::Application` trait, which `melin_server::domain::exchange_app::ServerApp`
//! implements as a thin newtype around `Exchange`. This module owns the
//! engine-specific payload bytes only.

use std::collections::HashMap as StdHashMap;
use std::num::NonZeroU64;

use crate::account::{AccountManager, Balance};
use crate::exchange::Exchange;
use crate::orderbook::OrderBook;
use crate::scheduler::{ScheduledTask, ScheduledTaskHeap, ScheduledTaskKind};
use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, FeeSchedule, InstrumentSpec, OrderId, Price,
    Quantity, ReservationSlot, RiskLimits, Side, Symbol, TimeInForce,
};

use crate::le;

/// Failure modes for [`decode_exchange_payload`]. Engine-local so the
/// snapshot codec doesn't depend on `melin-journal` — the journal layer
/// has its own broader error type, but snapshot payload decoding only
/// ever produces these two outcomes.
#[derive(Debug)]
pub enum SnapshotDecodeError {
    /// Buffer ended before a section could be fully read.
    Truncated,
    /// Bytes were structurally invalid (bad count, overflow, unknown
    /// discriminant, etc). `reason` is a static description suitable
    /// for surfacing in `io::Error`'s message.
    Corrupt { reason: &'static str },
}

impl std::fmt::Display for SnapshotDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated snapshot payload"),
            Self::Corrupt { reason } => write!(f, "corrupt snapshot payload: {reason}"),
        }
    }
}

impl std::error::Error for SnapshotDecodeError {}

/// Decoded book-side levels: Vec of (price, orders-at-that-level).
type RestingLevels = Vec<(Price, Vec<RestingOrderSnapshot>)>;

/// Decoded stop-side levels: Vec of (trigger_price, stops-at-that-level).
type StopLevels = Vec<(Price, Vec<PendingStopSnapshot>)>;

/// Current snapshot payload version. Surfaced through
/// `<Exchange as Application>::APP_VERSION` and embedded in the on-disk
/// frame by the transport.
/// v1 → v2: added SelfTradeProtection byte to PendingStopSnapshot.
/// v2 → v3: added per-account OrderId high-water marks for client dedup.
/// v3 → v4: added per-instrument RiskLimits for fat finger checks.
/// v4 → v5: added per-instrument CircuitBreakerConfig for price bands + halts.
/// v5 → v6: added chain_hash for BLAKE3 hash chain continuity across snapshots.
/// v6 → v7: order_sides keyed by (AccountId, OrderId), added fee schedules.
/// v7 → v8: order_index and stop_index now store AccountId (21 bytes/entry vs 17).
/// v8 → v9: added per-key request sequence HWMs for admin idempotency.
/// v10 → v11: added expiry_ns to resting orders and pending stops (GTD support).
/// v11 → v12: added per-instrument disabled flag for instrument lifecycle management.
/// v12 → v13: added scheduled_tasks heap for the engine-internal scheduler.
/// v13 → v14: scheduler heap removed from snapshot — rebuilt on restore from
///            GTD orders + pending stops (derived state).
/// v14 → v15: per-account OrderId HWMs removed — replaced by a live-orders-only
///            `(AccountId, OrderId)` set rebuilt on restore from `order_index`.
///            Dedup semantics changed to allow OrderId reuse after the original
///            closes (previously forbidden for the lifetime of the account).
/// v15 → v16: added per-currency fee-account deficits. The fee account is
///            now a signed ledger (`available - deficit`); rebates that
///            exceed `available` accumulate on `deficit` rather than
///            silently shortchanging the trader.
/// v16 → v17: reservation semantics changed. Reservations now lock pure
///            notional (no fee cushion); fees are settled from the fill's
///            received asset (buyer pays in base out of base credit;
///            seller pays in quote out of proceeds). v16 reservations
///            include a fee cushion and would over-reserve when read
///            under v17 semantics — bumping the version so old snapshots
///            are explicitly rejected.
/// v17 → v18: SEC-04 per-account rate-limiter bucket state. Without it, a
///            replica that restored from a snapshot taken while the
///            primary had partially-depleted buckets would re-initialise
///            buckets lazily as full and diverge on accept/reject
///            decisions for the bounded `burst/rate` window after
///            restore. v18 carries the bucket map (`account`, `tokens`,
///            `last_refill_ns`) so primary and replica converge bit-for-
///            bit on the very next event after restore.
pub const PAYLOAD_VERSION: u16 = 18;

/// Encode the Exchange's full state (the "payload" portion of a snapshot —
/// everything between the header and the CRC) into a freshly allocated
/// `Vec<u8>`. The caller owns framing and checksum.
pub fn encode_exchange_payload(exchange: &Exchange) -> Vec<u8> {
    let state = exchange.snapshot_state();
    // Exchange snapshots grow with account/order count; start with a
    // generously sized buffer to minimise reallocations but avoid
    // pre-reserving the 256 MiB cap.
    let mut buf = Vec::with_capacity(64 * 1024);
    encode_exchange_state(&state, &mut buf);
    buf
}

/// Decode an Exchange from the payload bytes produced by
/// [`encode_exchange_payload`]. The caller is responsible for verifying
/// framing and CRC before handing bytes to this function. Decoding is
/// always at [`PAYLOAD_VERSION`]; the transport rejects mismatched
/// `APP_VERSION` before this is ever called.
pub fn decode_exchange_payload(buf: &[u8]) -> Result<Exchange, SnapshotDecodeError> {
    let (_consumed, state) = decode_exchange_state(buf, PAYLOAD_VERSION)?;
    Ok(Exchange::restore_state(state))
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
    pub(crate) order_sides: Vec<((AccountId, OrderId), Side)>,
    pub(crate) books: Vec<(Symbol, BookSnapshot)>,
    /// Per-instrument fat finger risk limits.
    pub(crate) risk_limits: Vec<(Symbol, RiskLimits)>,
    /// Per-instrument circuit breaker configurations.
    pub(crate) circuit_breakers: Vec<(Symbol, CircuitBreakerConfig)>,
    /// Per-instrument maker/taker fee schedules.
    pub(crate) fee_schedules: Vec<(Symbol, FeeSchedule)>,
    /// Per-key request sequence HWMs for admin idempotency (v9+).
    pub(crate) key_hwm: Vec<(u64, u64)>,
    /// Set of disabled instrument symbols (v12+).
    pub(crate) disabled_instruments: Vec<Symbol>,
    /// Per-currency fee-account deficits (v16+). Records how much the
    /// fee account owes for rebates paid in excess of accumulated fee
    /// revenue. The logical fee balance is `available - deficit`. Sparse:
    /// only currencies with a non-zero deficit are present.
    pub(crate) fee_account_deficits: Vec<(CurrencyId, u64)>,
    /// Per-account rate-limiter bucket state (v18+, SEC-04). Each entry
    /// is `(account, tokens, last_refill_ns)`. Empty when the limiter
    /// is disabled or no account has yet submitted an order. Carrying
    /// this in the snapshot is what closes the SEC-04
    /// divergence window — see the version-history comment on
    /// `PAYLOAD_VERSION` for the v17 → v18 motivation.
    pub(crate) order_buckets: Vec<(AccountId, u64, u64)>,
}

/// Serialized order book state for a single instrument.
/// Uses Vec for each level to preserve insertion-order fidelity.
#[derive(Debug)]
pub(crate) struct BookSnapshot {
    pub(crate) bids: Vec<(Price, Vec<RestingOrderSnapshot>)>,
    pub(crate) asks: Vec<(Price, Vec<RestingOrderSnapshot>)>,
    pub(crate) order_index: Vec<(OrderId, AccountId, Side, Price)>,
    pub(crate) stop_buys: Vec<(Price, Vec<PendingStopSnapshot>)>,
    pub(crate) stop_sells: Vec<(Price, Vec<PendingStopSnapshot>)>,
    pub(crate) stop_index: Vec<(OrderId, AccountId, Side, Price)>,
    pub(crate) last_trade_price: Option<Price>,
}

/// Serialized resting order.
#[derive(Debug)]
pub(crate) struct RestingOrderSnapshot {
    pub(crate) id: OrderId,
    pub(crate) account: AccountId,
    pub(crate) remaining: Quantity,
    pub(crate) time_in_force: TimeInForce,
    pub(crate) expiry_ns: u64,
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
    /// Quote budget for buy-side market/stop-market orders.
    pub(crate) quote_budget: Option<u64>,
    /// Self-trade prevention mode.
    pub(crate) stp: crate::types::SelfTradeProtection,
    /// Expiry time in nanoseconds (GTD orders). Zero for non-GTD.
    pub(crate) expiry_ns: u64,
}

// --- Encoding helpers ---

// Encode an `Option<NonZeroU64>` as a 1-byte tag (0 = None, 1 = Some)
// optionally followed by the 8-byte value. Dual of `decode_opt_nz_u64`.
fn encode_opt_nz_u64(buf: &mut Vec<u8>, v: Option<NonZeroU64>) {
    match v {
        Some(n) => {
            buf.push(1);
            le::push_u64(buf, n.get());
        }
        None => buf.push(0),
    }
}

// Each `encode_*` helper writes its section header (4-byte length) plus
// the per-entry bytes. Helpers are split for symmetry with the matching
// `decode_*` helpers — keeping the wire format auditable from both sides.

fn encode_instruments(buf: &mut Vec<u8>, instruments: &[InstrumentSpec]) {
    le::push_u32(buf, instruments.len() as u32);
    for spec in instruments {
        le::push_u32(buf, spec.symbol.0);
        le::push_u32(buf, spec.base.0);
        le::push_u32(buf, spec.quote.0);
    }
}

fn encode_balances(buf: &mut Vec<u8>, balances: &[BalanceEntry]) {
    le::push_u32(buf, balances.len() as u32);
    for ((account, currency), balance) in balances {
        le::push_u32(buf, account.0);
        le::push_u32(buf, currency.0);
        le::push_u64(buf, balance.available);
        le::push_u64(buf, balance.reserved);
    }
}

fn encode_reservations(buf: &mut Vec<u8>, reservations: &[ReservationEntry]) {
    le::push_u32(buf, reservations.len() as u32);
    for (order_id, account, currency, remaining) in reservations {
        le::push_u64(buf, order_id.0);
        le::push_u32(buf, account.0);
        le::push_u32(buf, currency.0);
        le::push_u64(buf, *remaining);
    }
}

// Order sides: (account_id, order_id, side) per entry.
fn encode_order_sides(buf: &mut Vec<u8>, order_sides: &[OrderSideEntry]) {
    le::push_u32(buf, order_sides.len() as u32);
    for ((account, order_id), side) in order_sides {
        le::push_u32(buf, account.0);
        le::push_u64(buf, order_id.0);
        buf.push(le::encode_side(*side));
    }
}

fn encode_books(buf: &mut Vec<u8>, books: &[(Symbol, BookSnapshot)]) {
    le::push_u32(buf, books.len() as u32);
    for (symbol, book) in books {
        le::push_u32(buf, symbol.0);
        encode_book_snapshot(book, buf);
    }
}

fn encode_risk_limits(buf: &mut Vec<u8>, risk_limits: &[(Symbol, RiskLimits)]) {
    le::push_u32(buf, risk_limits.len() as u32);
    for (symbol, limits) in risk_limits {
        le::push_u32(buf, symbol.0);
        encode_opt_nz_u64(buf, limits.max_order_qty.map(|q| q.0));
        match limits.max_order_notional {
            Some(notional) => {
                buf.push(1);
                le::push_u64(buf, notional);
            }
            None => buf.push(0),
        }
    }
}

fn encode_circuit_breakers(buf: &mut Vec<u8>, circuit_breakers: &[(Symbol, CircuitBreakerConfig)]) {
    le::push_u32(buf, circuit_breakers.len() as u32);
    for (symbol, config) in circuit_breakers {
        le::push_u32(buf, symbol.0);
        encode_opt_nz_u64(buf, config.price_band_lower.map(|p| p.0));
        encode_opt_nz_u64(buf, config.price_band_upper.map(|p| p.0));
        buf.push(u8::from(config.halted));
    }
}

fn encode_fee_schedules(buf: &mut Vec<u8>, fee_schedules: &[(Symbol, FeeSchedule)]) {
    le::push_u32(buf, fee_schedules.len() as u32);
    for (symbol, schedule) in fee_schedules {
        le::push_u32(buf, symbol.0);
        le::push_i16(buf, schedule.maker_fee_bps);
        le::push_i16(buf, schedule.taker_fee_bps);
    }
}

fn encode_key_hwm(buf: &mut Vec<u8>, key_hwm: &[(u64, u64)]) {
    le::push_u32(buf, key_hwm.len() as u32);
    for (key_hash, hwm) in key_hwm {
        le::push_u64(buf, *key_hash);
        le::push_u64(buf, *hwm);
    }
}

fn encode_disabled_instruments(buf: &mut Vec<u8>, disabled: &[Symbol]) {
    le::push_u32(buf, disabled.len() as u32);
    for symbol in disabled {
        le::push_u32(buf, symbol.0);
    }
}

fn encode_fee_account_deficits(buf: &mut Vec<u8>, deficits: &[(CurrencyId, u64)]) {
    le::push_u32(buf, deficits.len() as u32);
    for (currency, amount) in deficits {
        le::push_u32(buf, currency.0);
        le::push_u64(buf, *amount);
    }
}

// Per-account rate-limiter bucket state (SEC-04). Each entry is
// account(4) + tokens(8) + last_refill_ns(8) = 20 bytes.
fn encode_order_buckets(buf: &mut Vec<u8>, buckets: &[OrderBucketEntry]) {
    le::push_u32(buf, buckets.len() as u32);
    for (account, tokens, last_refill_ns) in buckets {
        le::push_u32(buf, account.0);
        le::push_u64(buf, *tokens);
        le::push_u64(buf, *last_refill_ns);
    }
}

fn encode_exchange_state(state: &ExchangeSnapshot, buf: &mut Vec<u8>) {
    // Exhaustive destructure (no `..`): if a new field is added to
    // `ExchangeSnapshot`, the compiler errors here, forcing us to update
    // the wire format intentionally rather than silently shipping a
    // snapshot that drops the new field.
    let ExchangeSnapshot {
        instruments,
        balances,
        reservations,
        order_sides,
        books,
        risk_limits,
        circuit_breakers,
        fee_schedules,
        key_hwm,
        disabled_instruments,
        fee_account_deficits,
        order_buckets,
    } = state;
    encode_instruments(buf, instruments);
    encode_balances(buf, balances);
    encode_reservations(buf, reservations);
    encode_order_sides(buf, order_sides);
    encode_books(buf, books);
    encode_risk_limits(buf, risk_limits);
    encode_circuit_breakers(buf, circuit_breakers);
    encode_fee_schedules(buf, fee_schedules);
    encode_key_hwm(buf, key_hwm);
    encode_disabled_instruments(buf, disabled_instruments);
    encode_fee_account_deficits(buf, fee_account_deficits);
    encode_order_buckets(buf, order_buckets);
}

fn encode_book_snapshot(book: &BookSnapshot, buf: &mut Vec<u8>) {
    // Bids.
    encode_book_side(&book.bids, buf);
    // Asks.
    encode_book_side(&book.asks, buf);

    // Order index: (order_id, account_id, side, price) — 21 bytes each.
    le::push_u32(buf, book.order_index.len() as u32);
    for (order_id, account, side, price) in &book.order_index {
        le::push_u64(buf, order_id.0);
        le::push_u32(buf, account.0);
        buf.push(le::encode_side(*side));
        le::push_u64(buf, price.get());
    }

    // Stop buys.
    encode_stop_side(&book.stop_buys, buf);
    // Stop sells.
    encode_stop_side(&book.stop_sells, buf);

    // Stop index: (order_id, account_id, side, price) — 21 bytes each.
    le::push_u32(buf, book.stop_index.len() as u32);
    for (order_id, account, side, price) in &book.stop_index {
        le::push_u64(buf, order_id.0);
        le::push_u32(buf, account.0);
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
            buf.push(le::encode_tif(order.time_in_force));
            // expiry_ns (v11+): needed for GTD orders to survive snapshot/restore.
            le::push_u64(buf, order.expiry_ns);
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
            match stop.quote_budget {
                Some(budget) => {
                    buf.push(1);
                    le::push_u64(buf, budget);
                }
                None => buf.push(0),
            }
            buf.push(le::encode_stp(stop.stp));
            // expiry_ns (v11+): needed for GTD stop orders to survive snapshot/restore.
            le::push_u64(buf, stop.expiry_ns);
        }
    }
}

// --- Decoding helpers ---

/// Validate that a claimed count `n` of items each `item_size` bytes can
/// actually fit in the remaining buffer. Prevents memory exhaustion from
/// crafted count values.
fn validate_count(remaining: usize, n: usize, item_size: usize) -> Result<(), SnapshotDecodeError> {
    let needed = n.saturating_mul(item_size);
    if needed > remaining {
        Err(SnapshotDecodeError::Corrupt {
            reason: "count exceeds remaining buffer",
        })
    } else {
        Ok(())
    }
}

// Type aliases mirroring the corresponding `ExchangeSnapshot` fields, kept
// here only to keep the decode helper signatures legible (clippy
// type_complexity).
type BalanceEntry = ((AccountId, CurrencyId), Balance);
type ReservationEntry = (OrderId, AccountId, CurrencyId, u64);
type OrderSideEntry = ((AccountId, OrderId), Side);
type OrderBucketEntry = (AccountId, u64, u64);

// Reusable corrupt-entry helper.
fn corrupt(reason: &'static str) -> SnapshotDecodeError {
    SnapshotDecodeError::Corrupt { reason }
}

// Bounds check: returns TruncatedEntry if `pos + need` exceeds the buffer.
fn check(buf: &[u8], pos: usize, need: usize) -> Result<(), SnapshotDecodeError> {
    if pos + need > buf.len() {
        Err(SnapshotDecodeError::Truncated)
    } else {
        Ok(())
    }
}

// Read the 4-byte length prefix at `buf[0..4]` and return (length, body)
// where body is the slice past the prefix. Caller adds the consumed bytes
// to its own cursor.
fn read_section_len(buf: &[u8]) -> Result<usize, SnapshotDecodeError> {
    check(buf, 0, 4)?;
    Ok(le::get_u32(buf) as usize)
}

fn decode_instruments(buf: &[u8]) -> Result<(usize, Vec<InstrumentSpec>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    validate_count(buf.len() - pos, n, 12)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 12)?;
        out.push(InstrumentSpec {
            symbol: Symbol(le::get_u32(&buf[pos..])),
            base: CurrencyId(le::get_u32(&buf[pos + 4..])),
            quote: CurrencyId(le::get_u32(&buf[pos + 8..])),
        });
        pos += 12;
    }
    Ok((pos, out))
}

fn decode_balances(buf: &[u8]) -> Result<(usize, Vec<BalanceEntry>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    validate_count(buf.len() - pos, n, 24)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 24)?;
        let account = AccountId(le::get_u32(&buf[pos..]));
        let currency = CurrencyId(le::get_u32(&buf[pos + 4..]));
        let available = le::get_u64(&buf[pos + 8..]);
        let reserved = le::get_u64(&buf[pos + 16..]);
        out.push((
            (account, currency),
            Balance {
                available,
                reserved,
            },
        ));
        pos += 24;
    }
    Ok((pos, out))
}

fn decode_reservations(buf: &[u8]) -> Result<(usize, Vec<ReservationEntry>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    validate_count(buf.len() - pos, n, 24)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 24)?;
        let order_id = OrderId(le::get_u64(&buf[pos..]));
        let account = AccountId(le::get_u32(&buf[pos + 8..]));
        let currency = CurrencyId(le::get_u32(&buf[pos + 12..]));
        let remaining = le::get_u64(&buf[pos + 16..]);
        out.push((order_id, account, currency, remaining));
        pos += 24;
    }
    Ok((pos, out))
}

// Order sides: v7+ stores (account_id(4) + order_id(8) + side(1)) = 13 bytes.
// v5/v6 stores (order_id(8) + side(1)) = 9 bytes (no account in key) — uses
// AccountId(0) as placeholder. Lossy but allows loading old snapshots; v6
// will be re-saved as v7 on the next rotation.
fn decode_order_sides(
    buf: &[u8],
    version: u16,
) -> Result<(usize, Vec<OrderSideEntry>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    let mut out = Vec::with_capacity(n);
    if version >= 7 {
        validate_count(buf.len() - pos, n, 13)?;
        for _ in 0..n {
            check(buf, pos, 13)?;
            let account = AccountId(le::get_u32(&buf[pos..]));
            let order_id = OrderId(le::get_u64(&buf[pos + 4..]));
            let side = le::decode_side(buf[pos + 12]).ok_or(corrupt("invalid side in snapshot"))?;
            out.push(((account, order_id), side));
            pos += 13;
        }
    } else {
        validate_count(buf.len() - pos, n, 9)?;
        for _ in 0..n {
            check(buf, pos, 9)?;
            let order_id = OrderId(le::get_u64(&buf[pos..]));
            let side = le::decode_side(buf[pos + 8]).ok_or(corrupt("invalid side in snapshot"))?;
            out.push(((AccountId(0), order_id), side));
            pos += 9;
        }
    }
    Ok((pos, out))
}

fn decode_books(
    buf: &[u8],
    version: u16,
) -> Result<(usize, Vec<(Symbol, BookSnapshot)>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    // Minimum per-book overhead: at least a few bytes for the empty-book structure.
    validate_count(buf.len() - pos, n, 4)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 4)?;
        let symbol = Symbol(le::get_u32(&buf[pos..]));
        pos += 4;
        let (consumed, book) = decode_book_snapshot(&buf[pos..], version)?;
        pos += consumed;
        out.push((symbol, book));
    }
    Ok((pos, out))
}

// Decode an optional NonZeroU64 prefixed with a 1-byte tag (0 = None, 1 = Some).
// Returns the new position and the parsed value.
fn decode_opt_nz_u64(
    buf: &[u8],
    mut pos: usize,
    invalid_tag_reason: &'static str,
    zero_value_reason: &'static str,
) -> Result<(usize, Option<NonZeroU64>), SnapshotDecodeError> {
    check(buf, pos, 1)?;
    match buf[pos] {
        1 => {
            pos += 1;
            check(buf, pos, 8)?;
            let v = NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(corrupt(zero_value_reason))?;
            pos += 8;
            Ok((pos, Some(v)))
        }
        0 => Ok((pos + 1, None)),
        _ => Err(corrupt(invalid_tag_reason)),
    }
}

fn decode_risk_limits(
    buf: &[u8],
) -> Result<(usize, Vec<(Symbol, RiskLimits)>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    // Each entry is at least 6 bytes: symbol(4) + two option tags(1+1).
    validate_count(buf.len() - pos, n, 6)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 6)?;
        let symbol = Symbol(le::get_u32(&buf[pos..]));
        pos += 4;
        let (new_pos, max_order_qty) = decode_opt_nz_u64(
            buf,
            pos,
            "invalid max_order_qty tag in risk limits",
            "zero max_order_qty in risk limits",
        )?;
        pos = new_pos;
        let max_order_qty = max_order_qty.map(Quantity);
        check(buf, pos, 1)?;
        let max_order_notional = match buf[pos] {
            1 => {
                pos += 1;
                check(buf, pos, 8)?;
                let v = le::get_u64(&buf[pos..]);
                pos += 8;
                Some(v)
            }
            0 => {
                pos += 1;
                None
            }
            _ => return Err(corrupt("invalid max_order_notional tag in risk limits")),
        };
        out.push((
            symbol,
            RiskLimits {
                max_order_qty,
                max_order_notional,
            },
        ));
    }
    Ok((pos, out))
}

fn decode_circuit_breakers(
    buf: &[u8],
) -> Result<(usize, Vec<(Symbol, CircuitBreakerConfig)>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    // Each entry is at least 7 bytes: symbol(4) + two option tags(1+1) + halted(1).
    validate_count(buf.len() - pos, n, 7)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 7)?;
        let symbol = Symbol(le::get_u32(&buf[pos..]));
        pos += 4;
        let (new_pos, lower) = decode_opt_nz_u64(
            buf,
            pos,
            "invalid price_band_lower tag in circuit breaker",
            "zero price_band_lower in circuit breaker",
        )?;
        pos = new_pos;
        let (new_pos, upper) = decode_opt_nz_u64(
            buf,
            pos,
            "invalid price_band_upper tag in circuit breaker",
            "zero price_band_upper in circuit breaker",
        )?;
        pos = new_pos;
        check(buf, pos, 1)?;
        let halted = buf[pos] != 0;
        pos += 1;
        out.push((
            symbol,
            CircuitBreakerConfig {
                price_band_lower: lower.map(Price),
                price_band_upper: upper.map(Price),
                halted,
            },
        ));
    }
    Ok((pos, out))
}

fn decode_fee_schedules(
    buf: &[u8],
) -> Result<(usize, Vec<(Symbol, FeeSchedule)>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    // Each fee schedule: symbol(4) + maker_bps(2) + taker_bps(2) = 8 bytes.
    validate_count(buf.len() - pos, n, 8)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 8)?;
        let symbol = Symbol(le::get_u32(&buf[pos..]));
        pos += 4;
        let maker_fee_bps = le::get_i16(&buf[pos..]);
        pos += 2;
        let taker_fee_bps = le::get_i16(&buf[pos..]);
        pos += 2;
        out.push((
            symbol,
            FeeSchedule {
                maker_fee_bps,
                taker_fee_bps,
            },
        ));
    }
    Ok((pos, out))
}

fn decode_key_hwm(buf: &[u8]) -> Result<(usize, Vec<(u64, u64)>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    // Each entry: key_hash(8) + hwm(8) = 16 bytes.
    validate_count(buf.len() - pos, n, 16)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 16)?;
        let key_hash = le::get_u64(&buf[pos..]);
        let hwm = le::get_u64(&buf[pos + 8..]);
        out.push((key_hash, hwm));
        pos += 16;
    }
    Ok((pos, out))
}

fn decode_disabled_instruments(buf: &[u8]) -> Result<(usize, Vec<Symbol>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    // Each entry: symbol(4) = 4 bytes.
    validate_count(buf.len() - pos, n, 4)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 4)?;
        out.push(Symbol(le::get_u32(&buf[pos..])));
        pos += 4;
    }
    Ok((pos, out))
}

fn decode_fee_account_deficits(
    buf: &[u8],
) -> Result<(usize, Vec<(CurrencyId, u64)>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    // Each entry: currency(4) + amount(8) = 12 bytes.
    validate_count(buf.len() - pos, n, 12)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 12)?;
        let currency = CurrencyId(le::get_u32(&buf[pos..]));
        let amount = le::get_u64(&buf[pos + 4..]);
        out.push((currency, amount));
        pos += 12;
    }
    Ok((pos, out))
}

// Per-account rate-limiter bucket state (SEC-04). Each entry is
// account(4) + tokens(8) + last_refill_ns(8) = 20 bytes.
fn decode_order_buckets(buf: &[u8]) -> Result<(usize, Vec<OrderBucketEntry>), SnapshotDecodeError> {
    let n = read_section_len(buf)?;
    let mut pos = 4;
    validate_count(buf.len() - pos, n, 20)?;
    let mut out = Vec::with_capacity(n);
    // Track seen accounts to reject duplicate keys: the encoder writes
    // each AccountId at most once (HashMap iteration), so a duplicate
    // here means the snapshot is corrupt or tampered. Silent overwrite
    // would let an attacker shadow a legitimate bucket with a synthetic
    // full-credit one. HashSet is u32-keyed and only built during
    // recovery — not on the hot path.
    let mut seen: std::collections::HashSet<AccountId> =
        std::collections::HashSet::with_capacity(n);
    for _ in 0..n {
        check(buf, pos, 20)?;
        let account = AccountId(le::get_u32(&buf[pos..]));
        let tokens = le::get_u64(&buf[pos + 4..]);
        let last_refill_ns = le::get_u64(&buf[pos + 12..]);
        if !seen.insert(account) {
            return Err(corrupt("duplicate account in order_buckets section"));
        }
        out.push((account, tokens, last_refill_ns));
        pos += 20;
    }
    Ok((pos, out))
}

fn decode_exchange_state(
    buf: &[u8],
    version: u16,
) -> Result<(usize, ExchangeSnapshot), SnapshotDecodeError> {
    let mut pos = 0;

    let (consumed, instruments) = decode_instruments(&buf[pos..])?;
    pos += consumed;
    let (consumed, balances) = decode_balances(&buf[pos..])?;
    pos += consumed;
    let (consumed, reservations) = decode_reservations(&buf[pos..])?;
    pos += consumed;
    let (consumed, order_sides) = decode_order_sides(&buf[pos..], version)?;
    pos += consumed;
    let (consumed, books) = decode_books(&buf[pos..], version)?;
    pos += consumed;
    let (consumed, risk_limits) = decode_risk_limits(&buf[pos..])?;
    pos += consumed;
    let (consumed, circuit_breakers) = decode_circuit_breakers(&buf[pos..])?;
    pos += consumed;

    // v7+ and v9+ sections may be absent on legacy snapshots that ended
    // before the section was introduced (encoder writes at least a 4-byte
    // length when the section exists, so EOF here means the snapshot
    // predates the section). Newer versioned sections below require the
    // section to be present — physical truncation surfaces as
    // `TruncatedEntry` rather than a silent empty vec.
    let fee_schedules = if version >= 7 && pos < buf.len() {
        let (consumed, v) = decode_fee_schedules(&buf[pos..])?;
        pos += consumed;
        v
    } else {
        Vec::new()
    };

    let key_hwm = if version >= 9 && pos < buf.len() {
        let (consumed, v) = decode_key_hwm(&buf[pos..])?;
        pos += consumed;
        v
    } else {
        Vec::new()
    };

    let disabled_instruments = if version >= 12 {
        let (consumed, v) = decode_disabled_instruments(&buf[pos..])?;
        pos += consumed;
        v
    } else {
        Vec::new()
    };

    let fee_account_deficits = if version >= 16 {
        let (consumed, v) = decode_fee_account_deficits(&buf[pos..])?;
        pos += consumed;
        v
    } else {
        Vec::new()
    };

    let order_buckets = if version >= 18 {
        let (consumed, v) = decode_order_buckets(&buf[pos..])?;
        pos += consumed;
        v
    } else {
        Vec::new()
    };

    Ok((
        pos,
        ExchangeSnapshot {
            instruments,
            balances,
            reservations,
            order_sides,
            books,
            risk_limits,
            circuit_breakers,
            fee_schedules,
            key_hwm,
            disabled_instruments,
            fee_account_deficits,
            order_buckets,
        },
    ))
}

fn decode_book_snapshot(
    buf: &[u8],
    version: u16,
) -> Result<(usize, BookSnapshot), SnapshotDecodeError> {
    let corrupt = |reason: &'static str| SnapshotDecodeError::Corrupt { reason };
    let mut pos = 0;

    let check = |pos: usize, need: usize| -> Result<(), SnapshotDecodeError> {
        if pos + need > buf.len() {
            Err(SnapshotDecodeError::Truncated)
        } else {
            Ok(())
        }
    };

    // Bids.
    let (consumed, bids) = decode_book_side_levels(&buf[pos..], version)?;
    pos += consumed;

    // Asks.
    let (consumed, asks) = decode_book_side_levels(&buf[pos..], version)?;
    pos += consumed;

    // Order index: v8+ stores (order_id, account_id, side, price) — 21 bytes each.
    // v5-v7 stores (order_id, side, price) — 17 bytes each (no account; uses AccountId(0) placeholder).
    check(pos, 4)?;
    let n_order_index = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    let mut order_index = Vec::with_capacity(n_order_index);
    if version >= 8 {
        validate_count(buf.len() - pos, n_order_index, 21)?;
        for _ in 0..n_order_index {
            check(pos, 21)?;
            let order_id = OrderId(le::get_u64(&buf[pos..]));
            let account = AccountId(le::get_u32(&buf[pos + 8..]));
            let side = le::decode_side(buf[pos + 12]).ok_or(corrupt("invalid side"))?;
            let price_val = NonZeroU64::new(le::get_u64(&buf[pos + 13..]))
                .ok_or(corrupt("zero price in index"))?;
            order_index.push((order_id, account, side, Price(price_val)));
            pos += 21;
        }
    } else {
        validate_count(buf.len() - pos, n_order_index, 17)?;
        for _ in 0..n_order_index {
            check(pos, 17)?;
            let order_id = OrderId(le::get_u64(&buf[pos..]));
            let side = le::decode_side(buf[pos + 8]).ok_or(corrupt("invalid side"))?;
            let price_val = NonZeroU64::new(le::get_u64(&buf[pos + 9..]))
                .ok_or(corrupt("zero price in index"))?;
            // Pre-v8 snapshots lack AccountId in the index; use placeholder.
            // The account can be recovered from the BookSide resting orders.
            order_index.push((order_id, AccountId(0), side, Price(price_val)));
            pos += 17;
        }
    }

    // Stop buys.
    let (consumed, stop_buys) = decode_stop_side_levels(&buf[pos..], version)?;
    pos += consumed;

    // Stop sells.
    let (consumed, stop_sells) = decode_stop_side_levels(&buf[pos..], version)?;
    pos += consumed;

    // Stop index: v8+ stores (order_id, account_id, side, price) — 21 bytes each.
    // v5-v7 stores (order_id, side, price) — 17 bytes each.
    check(pos, 4)?;
    let n_stop_index = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    let mut stop_index = Vec::with_capacity(n_stop_index);
    if version >= 8 {
        validate_count(buf.len() - pos, n_stop_index, 21)?;
        for _ in 0..n_stop_index {
            check(pos, 21)?;
            let order_id = OrderId(le::get_u64(&buf[pos..]));
            let account = AccountId(le::get_u32(&buf[pos + 8..]));
            let side = le::decode_side(buf[pos + 12]).ok_or(corrupt("invalid side"))?;
            let price_val = NonZeroU64::new(le::get_u64(&buf[pos + 13..]))
                .ok_or(corrupt("zero price in stop index"))?;
            stop_index.push((order_id, account, side, Price(price_val)));
            pos += 21;
        }
    } else {
        validate_count(buf.len() - pos, n_stop_index, 17)?;
        for _ in 0..n_stop_index {
            check(pos, 17)?;
            let order_id = OrderId(le::get_u64(&buf[pos..]));
            let side = le::decode_side(buf[pos + 8]).ok_or(corrupt("invalid side"))?;
            let price_val = NonZeroU64::new(le::get_u64(&buf[pos + 9..]))
                .ok_or(corrupt("zero price in stop index"))?;
            // Pre-v8 snapshots lack AccountId in the stop index; use placeholder.
            stop_index.push((order_id, AccountId(0), side, Price(price_val)));
            pos += 17;
        }
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

fn decode_book_side_levels(
    buf: &[u8],
    version: u16,
) -> Result<(usize, RestingLevels), SnapshotDecodeError> {
    let corrupt = |reason: &'static str| SnapshotDecodeError::Corrupt { reason };
    let mut pos = 0;

    if buf.len() < 4 {
        return Err(SnapshotDecodeError::Truncated);
    }
    let n_levels = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    // Each level has at least 12 bytes (price + order count).
    validate_count(buf.len() - pos, n_levels, 12)?;

    // Per-order size: v11+ adds expiry_ns(8) after tif.
    let order_size: usize = if version >= 11 { 29 } else { 21 };

    let mut levels = Vec::with_capacity(n_levels);
    for _ in 0..n_levels {
        if pos + 12 > buf.len() {
            return Err(SnapshotDecodeError::Truncated);
        }
        let price_val =
            NonZeroU64::new(le::get_u64(&buf[pos..])).ok_or(corrupt("zero price in book level"))?;
        pos += 8;
        let n_orders = le::get_u32(&buf[pos..]) as usize;
        pos += 4;

        // Each order is id(8) + account(4) + remaining(8) + tif(1) [+ expiry_ns(8) in v11+].
        validate_count(buf.len() - pos, n_orders, order_size)?;
        let mut orders = Vec::with_capacity(n_orders);
        for _ in 0..n_orders {
            if pos + order_size > buf.len() {
                return Err(SnapshotDecodeError::Truncated);
            }
            let id = OrderId(le::get_u64(&buf[pos..]));
            let account = AccountId(le::get_u32(&buf[pos + 8..]));
            let remaining_val = NonZeroU64::new(le::get_u64(&buf[pos + 12..]))
                .ok_or(corrupt("zero remaining quantity"))?;
            let time_in_force = le::decode_tif(buf[pos + 20])
                .ok_or(corrupt("invalid time-in-force on resting order"))?;
            pos += 21;
            let expiry_ns = if version >= 11 {
                let v = le::get_u64(&buf[pos..]);
                pos += 8;
                v
            } else {
                0
            };
            orders.push(RestingOrderSnapshot {
                id,
                account,
                remaining: Quantity(remaining_val),
                time_in_force,
                expiry_ns,
            });
        }
        levels.push((Price(price_val), orders));
    }

    Ok((pos, levels))
}

fn decode_stop_side_levels(
    buf: &[u8],
    version: u16,
) -> Result<(usize, StopLevels), SnapshotDecodeError> {
    let corrupt = |reason: &'static str| SnapshotDecodeError::Corrupt { reason };
    let mut pos = 0;

    if buf.len() < 4 {
        return Err(SnapshotDecodeError::Truncated);
    }
    let n_levels = le::get_u32(&buf[pos..]) as usize;
    pos += 4;
    // Each level has at least 12 bytes (trigger price + stop count).
    validate_count(buf.len() - pos, n_levels, 12)?;

    let mut levels = Vec::with_capacity(n_levels);
    for _ in 0..n_levels {
        if pos + 12 > buf.len() {
            return Err(SnapshotDecodeError::Truncated);
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
                return Err(SnapshotDecodeError::Truncated);
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
                        return Err(SnapshotDecodeError::Truncated);
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

            // Decode quote_budget (Option<u64>).
            if pos >= buf.len() {
                return Err(SnapshotDecodeError::Truncated);
            }
            let quote_budget = match buf[pos] {
                1 => {
                    pos += 1;
                    if pos + 8 > buf.len() {
                        return Err(SnapshotDecodeError::Truncated);
                    }
                    let budget = le::get_u64(&buf[pos..]);
                    pos += 8;
                    Some(budget)
                }
                0 => {
                    pos += 1;
                    None
                }
                _ => return Err(corrupt("invalid quote_budget tag in stop")),
            };

            if pos >= buf.len() {
                return Err(SnapshotDecodeError::Truncated);
            }
            let stp = le::decode_stp(buf[pos]).ok_or(corrupt("invalid stp in stop"))?;
            pos += 1;

            // expiry_ns (v11+): needed for GTD stop orders.
            let expiry_ns = if version >= 11 {
                if pos + 8 > buf.len() {
                    return Err(SnapshotDecodeError::Truncated);
                }
                let v = le::get_u64(&buf[pos..]);
                pos += 8;
                v
            } else {
                0
            };

            stops.push(PendingStopSnapshot {
                id,
                account,
                side,
                trigger_price: Price(tp),
                quantity: Quantity(qty),
                time_in_force: tif,
                limit_price,
                quote_budget,
                stp,
                expiry_ns,
            });
        }
        levels.push((Price(trigger_val), stops));
    }

    Ok((pos, levels))
}

// --- Conversion: ExchangeSnapshot <-> actual types ---

/// Rebuild the engine's scheduler heap by walking every restored instrument
/// for GTD orders. The heap is derived state — not stored in the snapshot —
/// so a fresh restore must re-emit one `ExpireOrder` task per live GTD
/// resting order or pending stop.
fn rebuild_scheduler_heap(
    instruments: &[Option<Box<crate::exchange::InstrumentState>>],
) -> ScheduledTaskHeap {
    let mut heap = ScheduledTaskHeap::new();
    for inst in instruments.iter().flatten() {
        let symbol = inst.spec.symbol;
        for (account, order_id, expiry_ns) in inst.book.iter_gtd_orders() {
            heap.push(ScheduledTask {
                fire_ns: expiry_ns,
                kind: ScheduledTaskKind::ExpireOrder {
                    symbol,
                    account,
                    order_id,
                },
            });
        }
    }
    heap
}

/// Assemble the symbol-indexed `InstrumentState` Vec from the flat snapshot
/// Vecs. The output is the storage shape the live Exchange uses: a sparse
/// `Vec<Option<Box<InstrumentState>>>` where `Symbol.0` is the index. We
/// pick sparse Vec over `HashMap<Symbol, InstrumentState>` because
/// instrument lookup happens on every order — a Vec indexing op is
/// cache-friendly and branch-light, whereas HashMap probing pays a hash +
/// possible collision chase per access. Wasted slots for sparse symbol
/// allocations are acceptable (32 bytes per gap; symbol space is small).
fn build_indexed_instruments(
    specs: Vec<InstrumentSpec>,
    books: Vec<(Symbol, BookSnapshot)>,
    risk_limits: Vec<(Symbol, RiskLimits)>,
    circuit_breakers: Vec<(Symbol, CircuitBreakerConfig)>,
    fee_schedules: Vec<(Symbol, FeeSchedule)>,
    disabled_instruments: Vec<Symbol>,
) -> Vec<Option<Box<crate::exchange::InstrumentState>>> {
    use crate::exchange::InstrumentState;

    let mut books_map: StdHashMap<Symbol, OrderBook> = StdHashMap::new();
    for (symbol, book_snap) in books {
        books_map.insert(symbol, OrderBook::restore(symbol, book_snap));
    }
    let risk_map: StdHashMap<Symbol, RiskLimits> = risk_limits.into_iter().collect();
    let cb_map: StdHashMap<Symbol, CircuitBreakerConfig> = circuit_breakers.into_iter().collect();
    let fee_map: StdHashMap<Symbol, FeeSchedule> = fee_schedules.into_iter().collect();
    let disabled_set: std::collections::HashSet<Symbol> =
        disabled_instruments.into_iter().collect();

    let max_sym = specs.iter().map(|s| s.symbol.0 as usize).max().unwrap_or(0);
    let mut instruments: Vec<Option<Box<InstrumentState>>> = Vec::new();
    instruments.resize_with(max_sym + 1, || None);
    for spec in &specs {
        let idx = spec.symbol.0 as usize;
        let book = books_map
            .remove(&spec.symbol)
            .unwrap_or_else(|| OrderBook::new(spec.symbol));
        instruments[idx] = Some(Box::new(InstrumentState {
            spec: *spec,
            book,
            risk_limits: risk_map.get(&spec.symbol).copied().unwrap_or_default(),
            circuit_breaker: cb_map.get(&spec.symbol).copied().unwrap_or_default(),
            fee_schedule: fee_map.get(&spec.symbol).copied().unwrap_or_default(),
            disabled: disabled_set.contains(&spec.symbol),
        }));
    }
    instruments
}

/// Patch each instrument's `OrderBook` with the real reservation slots
/// produced by `AccountManager::from_parts`. Books are restored with
/// `ReservationSlot::DUMMY` placeholders; this step replaces them with the
/// live slab handles so settlements can release the reserved balance.
fn inject_reservation_slots_into_instruments(
    instruments: &mut [Option<Box<crate::exchange::InstrumentState>>],
    slot_assignments: &[((AccountId, OrderId), ReservationSlot)],
) {
    for inst in instruments {
        if let Some(inst) = inst.as_deref_mut() {
            inst.book.inject_reservation_slots(slot_assignments);
        }
    }
}

impl Exchange {
    /// Create a snapshot of all internal state for serialization.
    pub(crate) fn snapshot_state(&self) -> ExchangeSnapshot {
        let instruments: Vec<InstrumentSpec> = self.instrument_specs().copied().collect();
        let balances = self.accounts().snapshot_balances();
        let reservations = self.snapshot_reservations();
        let order_sides: Vec<((AccountId, OrderId), Side)> = self.snapshot_order_sides();

        let books: Vec<(Symbol, BookSnapshot)> = self
            .books()
            .map(|(symbol, book)| (symbol, book.snapshot()))
            .collect();

        let risk_limits = self.snapshot_risk_limits();
        let circuit_breakers = self.snapshot_circuit_breakers();
        let fee_schedules = self.snapshot_fee_schedules();
        let key_hwm = self.snapshot_key_hwm();
        let disabled_instruments = self.snapshot_disabled_instruments();
        let fee_account_deficits = self.accounts().snapshot_fee_deficits();
        let order_buckets = self.snapshot_order_buckets();

        ExchangeSnapshot {
            instruments,
            balances,
            reservations,
            order_sides,
            books,
            risk_limits,
            circuit_breakers,
            fee_schedules,
            key_hwm,
            disabled_instruments,
            fee_account_deficits,
            order_buckets,
        }
    }

    /// Reconstruct an Exchange from a snapshot.
    pub(crate) fn restore_state(state: ExchangeSnapshot) -> Self {
        // Exhaustive destructure (no `..`): if a new field is added to
        // `ExchangeSnapshot`, the compiler errors here, forcing us to wire
        // it through `restore_state` instead of silently dropping it on
        // recovery.
        // `order_sides` is derived state — `Exchange::snapshot_order_sides`
        // regenerates it from each book's active order/stop slots, so the
        // rebuilt books below produce it identically. We don't *use* the
        // snapshot's copy to construct anything, but we do verify it
        // matches the regenerated value after restore as a corruption
        // detector (catches torn writes or encoder bugs where books and
        // order_sides disagree). See the assertion at the end of this
        // function.
        let ExchangeSnapshot {
            instruments: instrument_specs,
            balances,
            reservations,
            order_sides: snapshot_order_sides,
            books,
            risk_limits,
            circuit_breakers,
            fee_schedules,
            key_hwm: key_hwm_entries,
            disabled_instruments,
            fee_account_deficits,
            order_buckets,
        } = state;

        let mut instruments = build_indexed_instruments(
            instrument_specs,
            books,
            risk_limits,
            circuit_breakers,
            fee_schedules,
            disabled_instruments,
        );

        let (accounts, slot_assignments) =
            AccountManager::from_parts(balances, reservations, fee_account_deficits);
        inject_reservation_slots_into_instruments(&mut instruments, &slot_assignments);

        // Per-key request sequence HWM map (v9+). Uses the same custom
        // hasher as the live map so lookup behavior matches the running
        // engine; capacity sized to the snapshot to avoid mid-restore
        // rehashes.
        let mut key_hwm: crate::types::HashMap<u64, u64> =
            crate::types::HashMap::with_capacity_and_hasher(
                key_hwm_entries.len(),
                Default::default(),
            );
        for (key_hash, hwm) in key_hwm_entries {
            key_hwm.insert(key_hash, hwm);
        }

        // Rebuild the scheduler heap from order state. Every GTD order that
        // is currently resting (or pending as a stop) needs an ExpireOrder
        // task — the heap is derived state, never stored in the snapshot.
        // `live_order_ids` is rebuilt the same way inside `from_parts`,
        // straight from the per-instrument order_index.
        let scheduled_tasks = rebuild_scheduler_heap(&instruments);

        let mut exchange = Self::from_parts(instruments, accounts, key_hwm, scheduled_tasks);
        // Restore per-account rate-limiter bucket state (v18+). Empty
        // for older snapshots, in which case the limiter starts with
        // every account at full burst — same shape as a fresh start.
        // The operator-config knobs (`max_orders_per_second`,
        // `max_orders_burst`) are reapplied separately by the receiver
        // wiring; the bucket state restored here will only be observed
        // by the limiter once those knobs are non-zero.
        exchange.restore_order_buckets(order_buckets);

        // Snapshot-corruption detector: the rebuilt books must produce the
        // same `order_sides` set the snapshot serialized. A mismatch means
        // the snapshot is internally inconsistent (e.g., torn write, encoder
        // bug, or a books-but-not-order_sides drift in some future change)
        // and continuing would silently restore wrong state. Sort both
        // sides before comparing — HashMap iteration order in
        // `order_index` is non-deterministic, but the set of entries must
        // be identical. This runs once at restore (not the hot path).
        let mut regenerated = exchange.snapshot_order_sides();
        let mut from_snapshot = snapshot_order_sides;
        // Sort by (AccountId, OrderId) key — keys are unique per entry, so
        // post-sort the vectors are canonical and structural equality
        // detects any side or key disagreement. `Side` itself isn't `Ord`,
        // so we can't fall back to a derived total order on the full tuple.
        regenerated.sort_unstable_by_key(|(k, _)| *k);
        from_snapshot.sort_unstable_by_key(|(k, _)| *k);
        if regenerated != from_snapshot {
            // Localize the divergence so an operator has something to act
            // on. Prefer the first per-entry disagreement over the
            // shared-prefix length, since that's the actionable signal.
            let diff = regenerated
                .iter()
                .zip(from_snapshot.iter())
                .position(|(a, b)| a != b);
            match diff {
                Some(i) => panic!(
                    "snapshot corruption: order_sides mismatch at sorted index {i} — \
                     books regenerated {:?}, snapshot had {:?}",
                    regenerated[i], from_snapshot[i],
                ),
                None => panic!(
                    "snapshot corruption: order_sides length mismatch — \
                     books regenerated {} entries, snapshot had {}",
                    regenerated.len(),
                    from_snapshot.len(),
                ),
            }
        }

        exchange
    }

    /// Create a deep copy of this Exchange by round-tripping through the
    /// snapshot representation. Used by the shadow snapshot stage to obtain
    /// an independent replica of the exchange state at startup.
    ///
    /// Not suitable for the hot path — allocates extensively.
    pub fn clone_via_snapshot(&self) -> Self {
        let mut cloned = Self::restore_state(self.snapshot_state());
        // The cap is operator config, not journaled state, so it isn't in
        // the snapshot payload. Carry it over in-process so the shadow
        // clone applies the same Rejected reasons as the primary —
        // otherwise a capped account on the primary would be unbounded
        // on the shadow, and shadow validation would diverge.
        cloned.set_max_open_orders_per_account(self.max_open_orders_per_account());
        // Same reasoning as above for the SEC-04 rate-limit config: not
        // journaled (operator config), but Rejected reports differ if
        // the shadow clone runs unthrottled — carry it over so the
        // shadow makes identical accept/reject decisions. The cloned
        // engine starts at default `(0, 0)`; transitioning from
        // disabled-to-active does NOT clear buckets (see the rule on
        // `set_max_orders_per_second`), so the snapshot-restored bucket
        // state is preserved through this call.
        let (rate, burst) = self.max_orders_per_second();
        cloned.set_max_orders_per_second(rate, burst);
        cloned
    }
}

impl OrderBook {
    /// Create a snapshot of the order book state.
    pub(crate) fn snapshot(&self) -> BookSnapshot {
        let snapshot_side =
            |side: &crate::orderbook::BookSide| -> Vec<(Price, Vec<RestingOrderSnapshot>)> {
                side.levels_snapshot()
                    .into_iter()
                    .map(|(price, orders)| {
                        let snaps = orders
                            .into_iter()
                            .map(|o| RestingOrderSnapshot {
                                id: o.id(),
                                account: o.account(),
                                remaining: o.remaining(),
                                time_in_force: o.time_in_force(),
                                expiry_ns: o.expiry_ns(),
                            })
                            .collect();
                        (price, snaps)
                    })
                    .collect()
            };

        let snapshot_stops = |stops: &crate::orderbook::StopSide| {
            stops
                .levels_snapshot()
                .into_iter()
                .map(|(trigger_price, pending)| {
                    let snaps = pending
                        .into_iter()
                        .map(|s| PendingStopSnapshot {
                            id: s.id(),
                            account: s.account(),
                            side: s.side(),
                            trigger_price: s.trigger_price(),
                            quantity: s.quantity(),
                            time_in_force: s.time_in_force(),
                            limit_price: s.limit_price(),
                            quote_budget: s.quote_budget(),
                            stp: s.stp(),
                            expiry_ns: s.expiry_ns(),
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
    pub(crate) fn restore(symbol: Symbol, snap: BookSnapshot) -> Self {
        // Reconstruct a side and return the slab-index assignments so the
        // caller can populate `order_index` with valid node handles.
        let restore_side = |levels: Vec<(Price, Vec<RestingOrderSnapshot>)>, side: Side| {
            let materialized: Vec<(Price, Vec<crate::orderbook::RestingOrder>)> = levels
                .into_iter()
                .map(|(price, orders)| {
                    let restored = orders
                        .into_iter()
                        .map(|o| {
                            crate::orderbook::RestingOrder::new(
                                o.id,
                                o.account,
                                o.remaining,
                                o.time_in_force,
                                o.expiry_ns,
                                side,
                                ReservationSlot::DUMMY,
                            )
                        })
                        .collect();
                    (price, restored)
                })
                .collect();
            crate::orderbook::BookSide::from_levels_snapshot(materialized)
        };

        let restore_stops = |levels: Vec<(Price, Vec<PendingStopSnapshot>)>| {
            let materialized: Vec<(Price, Vec<crate::orderbook::PendingStop>)> = levels
                .into_iter()
                .map(|(trigger_price, stops)| {
                    let pending = stops
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
                                s.quote_budget,
                                s.stp,
                                s.expiry_ns,
                                ReservationSlot::DUMMY,
                            )
                        })
                        .collect();
                    (trigger_price, pending)
                })
                .collect();
            crate::orderbook::StopSide::from_levels_snapshot(materialized)
        };

        // Build sides first; they tell us each order's slab index, which
        // we need to populate `order_index` so cancel/amend stay O(1).
        let (bids, bid_node_idx) = restore_side(snap.bids, Side::Buy);
        let (asks, ask_node_idx) = restore_side(snap.asks, Side::Sell);

        // Combine slab-index assignments into a lookup keyed by
        // (account, order_id). Both sides share the (account, order_id)
        // namespace via the snapshot codec, but each order lives in
        // exactly one side, so there are no key collisions.
        let mut node_for: std::collections::HashMap<(AccountId, OrderId), u32> =
            std::collections::HashMap::with_capacity(bid_node_idx.len() + ask_node_idx.len());
        node_for.extend(bid_node_idx);
        node_for.extend(ask_node_idx);

        let order_index: crate::types::HashMap4<
            (AccountId, OrderId),
            (Side, Price, ReservationSlot, u32),
        > = snap
            .order_index
            .into_iter()
            .map(|(id, account, side, price)| {
                let node_idx = node_for
                    .get(&(account, id))
                    .copied()
                    // Snapshot self-consistency: every order_index entry
                    // must correspond to a resting order in the same
                    // snapshot. If it doesn't, the snapshot is corrupt and
                    // we'd rather fail loudly than silently skip cancels.
                    .expect("snapshot order_index references missing book entry");
                (
                    (account, id),
                    (side, price, ReservationSlot::DUMMY, node_idx),
                )
            })
            .collect();

        // Build stop sides; collect the slab-index mapping the same way
        // as for resting orders so we can populate `stop_index` with
        // valid handles. Buy and sell stops live in disjoint slabs but
        // share the (account, order_id) namespace via the snapshot.
        let (stop_buys, buy_stop_idx) = restore_stops(snap.stop_buys);
        let (stop_sells, sell_stop_idx) = restore_stops(snap.stop_sells);
        let mut stop_node_for: std::collections::HashMap<(AccountId, OrderId), u32> =
            std::collections::HashMap::with_capacity(buy_stop_idx.len() + sell_stop_idx.len());
        stop_node_for.extend(buy_stop_idx);
        stop_node_for.extend(sell_stop_idx);

        let stop_index: crate::types::HashMap4<(AccountId, OrderId), (Side, Price, u32)> = snap
            .stop_index
            .into_iter()
            .map(|(id, account, side, price)| {
                let node_idx = stop_node_for
                    .get(&(account, id))
                    .copied()
                    .expect("snapshot stop_index references missing stop entry");
                ((account, id), (side, price, node_idx))
            })
            .collect();

        Self::from_parts(
            symbol,
            bids,
            asks,
            order_index,
            stop_buys,
            stop_sells,
            stop_index,
            snap.last_trade_price,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::path::Path;

    use super::*;
    use crate::exchange::Exchange;
    use crate::types::*;

    // Engine-local round-trip framing for the snapshot codec tests
    // below. The production on-disk path lives in
    // `melin_transport_core::snapshot` (generic over `Application`,
    // including CRC32C framing) and is exercised by the integration
    // tests in `melin-server/tests/`. Engine tests only need to verify
    // the payload codec (`encode_exchange_payload` /
    // `decode_exchange_payload`) — the seq + chain_hash are persisted
    // alongside so existing tests that assert on them keep working.
    type SnapResult<T> = std::io::Result<T>;

    fn save(exchange: &Exchange, seq: u64, chain_hash: [u8; 32], path: &Path) -> SnapResult<()> {
        let payload = encode_exchange_payload(exchange);
        let mut framed = Vec::with_capacity(40 + payload.len());
        framed.extend_from_slice(&seq.to_le_bytes());
        framed.extend_from_slice(&chain_hash);
        framed.extend_from_slice(&payload);
        std::fs::write(path, framed)
    }

    fn load(path: &Path) -> SnapResult<(Exchange, u64, [u8; 32])> {
        let bytes = std::fs::read(path)?;
        if bytes.len() < 40 {
            return Err(std::io::Error::other("truncated test snapshot header"));
        }
        let seq = u64::from_le_bytes(bytes[..8].try_into().unwrap());
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[8..40]);
        let exchange = decode_exchange_payload(&bytes[40..])
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        Ok((exchange, seq, hash))
    }

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
                post_only: false,
            },
            time_in_force: TimeInForce::GTC,
            quantity: qty(q),
            stp: SelfTradeProtection::Allow,
            expiry_ns: 0,
        }
    }

    // Note: the previous engine-side `checksum_mismatch_surfaces_as_snapshot_error`
    // test was deleted in the engine ↔ core decoupling. That guarantee
    // belongs to `melin_transport_core::snapshot`, which has its own
    // framing-corruption tests, and `melin-server/tests/` exercises the
    // full production framing end-to-end via `Application`.

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

        save(&exchange, 42, [0u8; 32], &path).unwrap();

        let (restored, seq, _chain_hash) = load(&path).unwrap();
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

        save(&exchange, 10, [0u8; 32], &path).unwrap();

        let (mut restored, _seq, _chain_hash) = load(&path).unwrap();

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
    fn snapshot_preserves_circuit_breaker_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cb.snapshot");

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        // Set circuit breaker with price bands + halt.
        exchange.set_circuit_breaker(
            Symbol(1),
            CircuitBreakerConfig {
                price_band_lower: Some(price_val(90)),
                price_band_upper: Some(price_val(110)),
                halted: true,
            },
        );

        save(&exchange, 5, [0u8; 32], &path).unwrap();
        let (mut restored, _, _) = load(&path).unwrap();

        // Halt should still be active after restore.
        let mut reports = Vec::new();
        restored.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 10),
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::TradingHalted,
                ..
            }
        ));

        // Unhalt, price bands should still be active.
        restored.set_circuit_breaker(
            Symbol(1),
            CircuitBreakerConfig {
                price_band_lower: Some(price_val(90)),
                price_band_upper: Some(price_val(110)),
                halted: false,
            },
        );

        reports.clear();
        restored.execute(
            Symbol(1),
            limit_order(2, ACCT_A, Side::Buy, 80, 10),
            &mut reports,
        );
        assert!(matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::OutsidePriceBand,
                ..
            }
        ));

        // In-range order should succeed.
        reports.clear();
        restored.execute(
            Symbol(1),
            limit_order(3, ACCT_A, Side::Buy, 100, 10),
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
    }

    #[test]
    fn snapshot_preserves_gtd_expiry() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gtd.snapshot");

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 100_000);

        let mut reports = Vec::new();

        // Place a GTD order with expiry_ns = 5_000_000.
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price_val(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(10),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 5_000_000,
            },
            &mut reports,
        );
        assert!(matches!(reports[0], ExecutionReport::Placed { .. }));
        reports.clear();

        save(&exchange, 20, [0u8; 32], &path).unwrap();
        let (mut restored, _, _) = load(&path).unwrap();

        // The GTD order should still be on the book and the scheduler heap
        // must have been rebuilt from order state. A pre-expiry tick is a
        // no-op; an at-expiry tick fires the rebuilt task and cancels.
        restored.drain_due_scheduled_tasks(4_999_999, &mut reports);
        assert!(reports.is_empty(), "should not expire before timestamp");

        restored.drain_due_scheduled_tasks(5_000_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
    }

    #[test]
    fn clone_via_snapshot_produces_identical_state() {
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
        reports.clear();

        let cloned = exchange.clone_via_snapshot();

        // Balances should match.
        assert_eq!(
            cloned.accounts().balance(ACCT_A, USD).available,
            exchange.accounts().balance(ACCT_A, USD).available,
        );
        assert_eq!(
            cloned.accounts().balance(ACCT_B, BTC).reserved,
            exchange.accounts().balance(ACCT_B, BTC).reserved,
        );

        // Resting order should match — buy against it on the clone.
        let mut clone_reports = Vec::new();
        let mut mutable_clone = cloned;
        mutable_clone.execute(
            Symbol(1),
            limit_order(2, ACCT_A, Side::Buy, 100, 10),
            &mut clone_reports,
        );
        assert!(matches!(clone_reports[0], ExecutionReport::Fill { .. }));
    }

    #[test]
    #[should_panic(expected = "snapshot corruption: order_sides mismatch")]
    fn restore_detects_order_sides_mismatch() {
        // Build an exchange with one resting order so `order_sides` is
        // non-empty and the mismatch is observable.
        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_B, BTC, 500);
        let mut reports = Vec::new();
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_B, Side::Sell, 100, 50),
            &mut reports,
        );

        // Mutate the snapshot's `order_sides` so it disagrees with what
        // the rebuilt books will regenerate. Flipping the recorded side
        // is enough — the entry count still matches, but the value set
        // doesn't.
        let mut state = exchange.snapshot_state();
        assert!(!state.order_sides.is_empty(), "test prerequisite");
        state.order_sides[0].1 = Side::Buy;

        // `restore_state` must panic on the inconsistency rather than
        // silently restore wrong state.
        let _ = Exchange::restore_state(state);
    }

    #[test]
    fn snapshot_rebuilds_scheduler_heap_from_gtd_orders() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rebuild.snapshot");

        let mut exchange = Exchange::new();
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 10_000_000);

        // Mix resting GTD limits with a GTD pending stop so the rebuild
        // path covers both `iter_gtd_orders` branches (book + stop_index).
        let mut reports = Vec::new();
        // Resting GTD limit at expiry 5_000.
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(1),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price_val(100),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 5_000,
            },
            &mut reports,
        );
        // Pending GTD stop-limit at expiry 6_000. Stop-limit (rather than
        // bare Stop) keeps the reservation bounded to trigger_price × qty
        // so the third order below can also reserve.
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::StopLimit {
                    trigger_price: price_val(200),
                    limit_price: price_val(200),
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 6_000,
            },
            &mut reports,
        );
        // Second resting GTD limit at expiry 8_000.
        exchange.execute(
            Symbol(1),
            Order {
                id: OrderId(3),
                account: ACCT_A,
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price_val(101),
                    post_only: false,
                },
                time_in_force: TimeInForce::GTD,
                quantity: qty(1),
                stp: SelfTradeProtection::Allow,
                expiry_ns: 8_000,
            },
            &mut reports,
        );
        reports.clear();

        // Sanity: all 3 orders should have scheduled tasks before the snapshot.
        assert_eq!(exchange.scheduled_task_count(), 3, "pre-snapshot heap");

        save(&exchange, 7, [0u8; 32], &path).unwrap();
        let (mut restored, _, _) = load(&path).unwrap();

        // Sanity: rebuild restored all 3 tasks from the order books.
        assert_eq!(restored.scheduled_task_count(), 3, "post-restore heap");

        // Pre-expiry tick: nothing fires.
        restored.drain_due_scheduled_tasks(4_999, &mut reports);
        assert!(reports.is_empty());

        // Drain at 5_000: only the first limit fires.
        restored.drain_due_scheduled_tasks(5_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(1),
                ..
            }
        ));
        reports.clear();

        // Drain at 6_000: the pending stop fires (rebuilt from stop_index).
        restored.drain_due_scheduled_tasks(6_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(2),
                ..
            }
        ));
        reports.clear();

        // Drain at 8_000: the second resting limit fires.
        restored.drain_due_scheduled_tasks(8_000, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(
            reports[0],
            ExecutionReport::Cancelled {
                order_id: OrderId(3),
                ..
            }
        ));
    }

    /// SEC-04 v18+ regression: per-account rate-limiter bucket state must
    /// survive a snapshot round-trip so a replica restoring from a
    /// snapshot taken mid-throttle sees the same `tokens` /
    /// `last_refill_ns` the primary had — and therefore makes identical
    /// accept/reject decisions on the very next event. Without this,
    /// the replica would re-initialise buckets lazily as full and
    /// diverge for the bounded `burst/rate` window.
    #[test]
    fn snapshot_round_trip_preserves_rate_limit_buckets() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rate_limit.snapshot");

        let mut exchange = Exchange::new();
        exchange.set_max_orders_per_second(1_000, 5);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000_000);
        exchange.deposit(ACCT_B, USD, 1_000_000);

        // Drive the limiter so both accounts have non-trivial bucket
        // state. ACCT_A burns 3/5 of its burst at t=1s; ACCT_B burns
        // 1/5 at t=2s. Distinct `last_refill_ns` per bucket so a wrong
        // restore (e.g. snapping to 0) would be caught by the equality
        // assertion below.
        let mut reports = Vec::new();
        for i in 0..3u64 {
            exchange.set_current_event_ts_ns(1_000_000_000);
            exchange.execute(
                Symbol(1),
                limit_order(i + 1, ACCT_A, Side::Buy, 100, 1),
                &mut reports,
            );
        }
        exchange.set_current_event_ts_ns(2_000_000_000);
        exchange.execute(
            Symbol(1),
            limit_order(100, ACCT_B, Side::Buy, 101, 1),
            &mut reports,
        );

        let pre = exchange.snapshot_order_buckets();
        assert_eq!(pre.len(), 2, "two buckets should be populated");

        save(&exchange, 1, [0u8; 32], &path).unwrap();
        let (mut restored, _seq, _hash) = load(&path).unwrap();
        // The receiver wiring re-applies the operator config after
        // load. Use the same values to exercise the no-clear path.
        restored.set_max_orders_per_second(1_000, 5);

        let post = restored.snapshot_order_buckets();
        // Bucket maps must be equal as sets (HashMap iteration order is
        // unspecified); compare via sorted Vecs.
        let mut pre_sorted = pre.clone();
        let mut post_sorted = post;
        pre_sorted.sort_by_key(|(a, _, _)| a.0);
        post_sorted.sort_by_key(|(a, _, _)| a.0);
        assert_eq!(pre_sorted, post_sorted);

        // Functional check: an immediate next event on the restored
        // engine must see the same accept/reject decision the primary
        // would. ACCT_A burned 3 of 5 tokens at t=1s, so at t=1s+1ns it
        // has 2 tokens left — exactly two more accepts before rejection.
        let mut after = Vec::new();
        for i in 0..2u64 {
            restored.set_current_event_ts_ns(1_000_000_000 + 1 + i);
            restored.execute(
                Symbol(1),
                limit_order(200 + i, ACCT_A, Side::Buy, 102 + i, 1),
                &mut after,
            );
        }
        assert!(
            !after
                .iter()
                .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
            "two more orders should fit in the restored bucket: {after:?}",
        );
        after.clear();
        // Third post-restore order with negligible elapsed time must
        // reject — proves the bucket really was at 2 tokens, not 5.
        restored.set_current_event_ts_ns(1_000_000_000 + 10);
        restored.execute(
            Symbol(1),
            limit_order(999, ACCT_A, Side::Buy, 200, 1),
            &mut after,
        );
        assert!(
            matches!(
                after[0],
                ExecutionReport::Rejected {
                    reason: RejectReason::ExceedsOrderRate,
                    ..
                }
            ),
            "restored bucket lost throttle state: {after:?}",
        );
    }

    /// A v18 snapshot whose bucket section is missing — physically
    /// truncated mid-stream — must fail decode rather than silently
    /// returning empty buckets. The pre-SF2 guard
    /// `if version >= 18 && pos < buf.len()` swallowed truncation as
    /// "no entries", which would let a corrupt snapshot restore an
    /// exchange that diverges from the primary on the very next event.
    #[test]
    fn truncated_v18_snapshot_payload_errors_instead_of_emptying_buckets() {
        let mut exchange = Exchange::new();
        exchange.set_max_orders_per_second(1_000, 5);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000_000);
        let mut reports = Vec::new();
        exchange.set_current_event_ts_ns(1_000_000_000);
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 1),
            &mut reports,
        );

        // Encode, then strip the trailing rate-limiter bucket section
        // (length u32 + 1 entry of 20 bytes = 24 bytes). The truncated
        // payload looks valid up to the bucket boundary, mirroring a
        // real on-disk truncation.
        let full = encode_exchange_payload(&exchange);
        let truncated = &full[..full.len() - 24];
        match decode_exchange_payload(truncated) {
            Err(SnapshotDecodeError::Truncated) => {}
            Err(other) => panic!("expected TruncatedEntry, got {other:?}"),
            Ok(_) => panic!("truncated v18 payload must not decode silently as empty"),
        }
    }

    /// SEC-04 v18+: the decoder must reject a payload that contains the
    /// same `AccountId` twice in the rate-limiter bucket section. The
    /// encoder writes each account at most once (HashMap iteration), so
    /// a duplicate means the snapshot was tampered or corrupted. Silent
    /// overwrite would let an attacker shadow a depleted bucket with a
    /// synthetic full-credit one.
    #[test]
    fn duplicate_account_in_v18_bucket_section_rejected() {
        let mut exchange = Exchange::new();
        exchange.set_max_orders_per_second(1_000, 5);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000_000);
        let mut reports = Vec::new();
        exchange.set_current_event_ts_ns(1_000_000_000);
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 1),
            &mut reports,
        );

        let mut payload = encode_exchange_payload(&exchange);
        // Bucket section is the trailing run: [u32 count][entry × count],
        // entry = AccountId(u32) + tokens(u64) + last_refill_ns(u64) = 20 B.
        // Bump the count by one and append a duplicate of the existing entry.
        let entry_start = payload.len() - 20;
        let dup_entry = payload[entry_start..].to_vec();
        let count_pos = entry_start - 4;
        let count = le::get_u32(&payload[count_pos..]);
        // u32 is the on-wire count type; if this ever overflows the test
        // setup is the bug, not the production code.
        let new_count = count
            .checked_add(1)
            .expect("test fixture must keep count within u32");
        payload[count_pos..count_pos + 4].copy_from_slice(&new_count.to_le_bytes());
        payload.extend_from_slice(&dup_entry);

        match decode_exchange_payload(&payload) {
            Err(SnapshotDecodeError::Corrupt { reason, .. }) => {
                assert!(
                    reason.contains("duplicate account"),
                    "expected duplicate-account corruption, got: {reason}",
                );
            }
            Err(other) => panic!("expected CorruptEntry, got {other:?}"),
            Ok(_) => panic!("duplicate-account payload must not decode silently"),
        }
    }

    /// SEC-04 v18+: `set_max_orders_per_second` must NOT clear bucket
    /// state when called with the same `(rate, burst)` already in
    /// effect. This is what allows the receiver wiring to re-apply
    /// operator config after a snapshot restore without wiping the
    /// state we just restored.
    #[test]
    fn rate_limit_set_idempotent_preserves_buckets() {
        let mut exchange = Exchange::new();
        exchange.set_max_orders_per_second(500, 3);
        exchange.add_instrument(btc_usd_spec());
        exchange.deposit(ACCT_A, USD, 1_000_000);
        let mut reports = Vec::new();
        exchange.set_current_event_ts_ns(1_000);
        exchange.execute(
            Symbol(1),
            limit_order(1, ACCT_A, Side::Buy, 100, 1),
            &mut reports,
        );
        let before = exchange.snapshot_order_buckets();
        assert_eq!(before.len(), 1);
        // Same values — must be a no-op for buckets.
        exchange.set_max_orders_per_second(500, 3);
        let after = exchange.snapshot_order_buckets();
        assert_eq!(before, after, "same-config call must not clear");
        // Different values — must clear.
        exchange.set_max_orders_per_second(500, 4);
        assert!(
            exchange.snapshot_order_buckets().is_empty(),
            "changed-config call must clear",
        );
    }
}
