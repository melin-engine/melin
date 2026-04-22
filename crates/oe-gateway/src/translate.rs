//! FIX ↔ Melin message translation.
//!
//! Converts FIX NewOrderSingle/Cancel/CancelReplace into Melin `Request`
//! variants, and Melin execution reports back into FIX messages.

use std::num::NonZeroU64;

use melin_protocol::message::Request;
use melin_trading::types::{
    AccountId, ExecutionReport, OrderId, OrderType, Price, Quantity, RejectReason,
    SelfTradeProtection, Side, Symbol, TimeInForce,
};

use crate::config::SymbolConfig;
use crate::id_map::ClOrdIdMap;
use crate::price;
use melin_gateway_core::fix::parse::FixMessage;
use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;

/// Look up a ClOrdID for an OrderId, logging a warning on miss.
///
/// A miss means the gateway is emitting a FIX exec report for an
/// OrderId it doesn't have in its id_map — usually a bug in routing
/// or in id_map upkeep, occasionally a legitimate cross-session fill
/// (maker side of a self-trade where the maker is on a different
/// session). Either way it should not pass silently: callers used to
/// substitute "UNKNOWN" with no audit trail, hiding both bugs and
/// genuinely orphaned reports. We still emit "UNKNOWN" on the wire
/// (dropping the report would be worse — clients reconcile by
/// OrderID too) but log it as a warning so operators can grep for it.
fn resolve_clord_id<'a>(id_map: &'a ClOrdIdMap, order_id: OrderId, context: &str) -> &'a str {
    match id_map.get_clord_id(order_id) {
        Some(id) => id,
        None => {
            tracing::warn!(
                order_id = order_id.0,
                context,
                "no ClOrdID in id_map for order; emitting UNKNOWN"
            );
            "UNKNOWN"
        }
    }
}

/// Errors during FIX → Melin translation.
#[derive(Debug)]
pub enum TranslateError {
    MissingTag(u32),
    InvalidValue { tag: u32, value: String },
    UnknownSymbol(String),
    InvalidPrice(String),
    InvalidQuantity(String),
    ZeroPrice,
    ZeroQuantity,
}

impl std::fmt::Display for TranslateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTag(t) => write!(f, "missing required tag {t}"),
            Self::InvalidValue { tag, value } => {
                write!(f, "invalid value for tag {tag}: '{value}'")
            }
            Self::UnknownSymbol(s) => write!(f, "unknown symbol: '{s}'"),
            Self::InvalidPrice(s) => write!(f, "invalid price: '{s}'"),
            Self::InvalidQuantity(s) => write!(f, "invalid quantity: '{s}'"),
            Self::ZeroPrice => write!(f, "price must be non-zero"),
            Self::ZeroQuantity => write!(f, "quantity must be non-zero"),
        }
    }
}

impl std::error::Error for TranslateError {}

/// Context for translating messages within a session.
pub struct TranslateContext<'a> {
    /// Melin AccountId for this session.
    pub account_id: AccountId,
    /// Symbol lookup: FIX symbol → config.
    pub symbols: &'a std::collections::HashMap<String, SymbolConfig>,
    /// ClOrdID ↔ OrderId map (mutable — insert on new orders).
    pub id_map: &'a mut ClOrdIdMap,
}

/// Translate a FIX NewOrderSingle (35=D) into a Melin SubmitOrder request.
pub fn new_order_single(
    msg: &FixMessage<'_>,
    ctx: &mut TranslateContext<'_>,
) -> Result<Request, TranslateError> {
    let clord_id = msg
        .get_str(tags::CL_ORD_ID)
        .ok_or(TranslateError::MissingTag(tags::CL_ORD_ID))?;
    let fix_symbol = msg
        .get_str(tags::SYMBOL)
        .ok_or(TranslateError::MissingTag(tags::SYMBOL))?;
    let side_str = msg
        .get_str(tags::SIDE)
        .ok_or(TranslateError::MissingTag(tags::SIDE))?;
    let ord_type_str = msg
        .get_str(tags::ORD_TYPE)
        .ok_or(TranslateError::MissingTag(tags::ORD_TYPE))?;
    let qty_str = msg
        .get_str(tags::ORDER_QTY)
        .ok_or(TranslateError::MissingTag(tags::ORDER_QTY))?;

    // Symbol lookup.
    let sym_config = ctx
        .symbols
        .get(fix_symbol)
        .ok_or_else(|| TranslateError::UnknownSymbol(fix_symbol.to_owned()))?;

    let symbol = Symbol(sym_config.melin_symbol);

    // Side.
    let side = match side_str {
        "1" => Side::Buy,
        "2" => Side::Sell,
        _ => {
            return Err(TranslateError::InvalidValue {
                tag: tags::SIDE,
                value: side_str.to_owned(),
            });
        }
    };

    // Quantity.
    let qty_ticks = price::decimal_to_ticks(qty_str, sym_config.lot_size_inverse)
        .ok_or_else(|| TranslateError::InvalidQuantity(qty_str.to_owned()))?;
    let quantity = Quantity(NonZeroU64::new(qty_ticks).ok_or(TranslateError::ZeroQuantity)?);

    // OrderType + Price.
    let order_type = match ord_type_str {
        "1" => OrderType::Market,
        "2" => {
            // Limit — price required.
            let price_str = msg
                .get_str(tags::PRICE)
                .ok_or(TranslateError::MissingTag(tags::PRICE))?;
            let ticks = price::decimal_to_ticks(price_str, sym_config.tick_size_inverse)
                .ok_or_else(|| TranslateError::InvalidPrice(price_str.to_owned()))?;
            let p = Price(NonZeroU64::new(ticks).ok_or(TranslateError::ZeroPrice)?);

            // Post-only: ExecInst (18) = "6" (participate don't initiate).
            let post_only = msg.get_str(tags::EXEC_INST) == Some("6");

            OrderType::Limit {
                price: p,
                post_only,
            }
        }
        "3" => {
            // Stop — StopPx required.
            let stop_str = msg
                .get_str(tags::STOP_PX)
                .ok_or(TranslateError::MissingTag(tags::STOP_PX))?;
            let ticks = price::decimal_to_ticks(stop_str, sym_config.tick_size_inverse)
                .ok_or_else(|| TranslateError::InvalidPrice(stop_str.to_owned()))?;
            let trigger = Price(NonZeroU64::new(ticks).ok_or(TranslateError::ZeroPrice)?);
            OrderType::Stop {
                trigger_price: trigger,
            }
        }
        "4" => {
            // StopLimit — both StopPx and Price required.
            let stop_str = msg
                .get_str(tags::STOP_PX)
                .ok_or(TranslateError::MissingTag(tags::STOP_PX))?;
            let price_str = msg
                .get_str(tags::PRICE)
                .ok_or(TranslateError::MissingTag(tags::PRICE))?;
            let trigger_ticks = price::decimal_to_ticks(stop_str, sym_config.tick_size_inverse)
                .ok_or_else(|| TranslateError::InvalidPrice(stop_str.to_owned()))?;
            let limit_ticks = price::decimal_to_ticks(price_str, sym_config.tick_size_inverse)
                .ok_or_else(|| TranslateError::InvalidPrice(price_str.to_owned()))?;
            OrderType::StopLimit {
                trigger_price: Price(
                    NonZeroU64::new(trigger_ticks).ok_or(TranslateError::ZeroPrice)?,
                ),
                limit_price: Price(NonZeroU64::new(limit_ticks).ok_or(TranslateError::ZeroPrice)?),
            }
        }
        _ => {
            return Err(TranslateError::InvalidValue {
                tag: tags::ORD_TYPE,
                value: ord_type_str.to_owned(),
            });
        }
    };

    // TimeInForce (default GTC).
    let tif = match msg.get_str(tags::TIME_IN_FORCE).unwrap_or("1") {
        "0" => TimeInForce::Day,
        "1" => TimeInForce::GTC,
        "3" => TimeInForce::IOC,
        "4" => TimeInForce::FOK,
        "6" => TimeInForce::GTD,
        other => {
            return Err(TranslateError::InvalidValue {
                tag: tags::TIME_IN_FORCE,
                value: other.to_owned(),
            });
        }
    };

    // GTD expiry: FIX tag 126 (ExpireTime) as nanoseconds since epoch.
    // For v1, we pass 0 — per-order GTD is not yet parsed from FIX.
    let expiry_ns = 0;

    // Assign OrderId from ClOrdID.
    let order_id = ctx.id_map.insert(clord_id);

    // Account: use tag 1 if present, else session default.
    let account = if let Some(acct_str) = msg.get_str(tags::ACCOUNT) {
        AccountId(
            acct_str
                .parse::<u32>()
                .map_err(|_| TranslateError::InvalidValue {
                    tag: tags::ACCOUNT,
                    value: acct_str.to_owned(),
                })?,
        )
    } else {
        ctx.account_id
    };

    Ok(Request::SubmitOrder {
        symbol,
        order: melin_trading::types::Order {
            id: order_id,
            account,
            side,
            order_type,
            time_in_force: tif,
            quantity,
            stp: SelfTradeProtection::CancelNewest,
            expiry_ns,
        },
    })
}

/// Translate a FIX OrderCancelRequest (35=F) into a Melin CancelOrder.
pub fn cancel_order(
    msg: &FixMessage<'_>,
    ctx: &mut TranslateContext<'_>,
) -> Result<Request, TranslateError> {
    let orig_clord_id = msg
        .get_str(tags::ORIG_CL_ORD_ID)
        .ok_or(TranslateError::MissingTag(tags::ORIG_CL_ORD_ID))?;
    let fix_symbol = msg
        .get_str(tags::SYMBOL)
        .ok_or(TranslateError::MissingTag(tags::SYMBOL))?;

    let sym_config = ctx
        .symbols
        .get(fix_symbol)
        .ok_or_else(|| TranslateError::UnknownSymbol(fix_symbol.to_owned()))?;

    let order_id =
        ctx.id_map
            .get_order_id(orig_clord_id)
            .ok_or_else(|| TranslateError::InvalidValue {
                tag: tags::ORIG_CL_ORD_ID,
                value: orig_clord_id.to_owned(),
            })?;

    // Register the cancel's own ClOrdID for the cancel ack.
    let clord_id = msg
        .get_str(tags::CL_ORD_ID)
        .ok_or(TranslateError::MissingTag(tags::CL_ORD_ID))?;
    ctx.id_map.insert(clord_id);

    Ok(Request::CancelOrder {
        symbol: Symbol(sym_config.melin_symbol),
        account: ctx.account_id,
        order_id,
    })
}

/// Translate a FIX OrderCancelReplaceRequest (35=G) into a Melin CancelReplace.
pub fn cancel_replace(
    msg: &FixMessage<'_>,
    ctx: &mut TranslateContext<'_>,
) -> Result<Request, TranslateError> {
    let orig_clord_id = msg
        .get_str(tags::ORIG_CL_ORD_ID)
        .ok_or(TranslateError::MissingTag(tags::ORIG_CL_ORD_ID))?;
    let fix_symbol = msg
        .get_str(tags::SYMBOL)
        .ok_or(TranslateError::MissingTag(tags::SYMBOL))?;

    let sym_config = ctx
        .symbols
        .get(fix_symbol)
        .ok_or_else(|| TranslateError::UnknownSymbol(fix_symbol.to_owned()))?;

    let order_id =
        ctx.id_map
            .get_order_id(orig_clord_id)
            .ok_or_else(|| TranslateError::InvalidValue {
                tag: tags::ORIG_CL_ORD_ID,
                value: orig_clord_id.to_owned(),
            })?;

    // Register the replace's ClOrdID.
    let clord_id = msg
        .get_str(tags::CL_ORD_ID)
        .ok_or(TranslateError::MissingTag(tags::CL_ORD_ID))?;
    ctx.id_map.insert(clord_id);

    // New price.
    let price_str = msg
        .get_str(tags::PRICE)
        .ok_or(TranslateError::MissingTag(tags::PRICE))?;
    let new_price = Price(
        NonZeroU64::new(
            price::decimal_to_ticks(price_str, sym_config.tick_size_inverse)
                .ok_or_else(|| TranslateError::InvalidPrice(price_str.to_owned()))?,
        )
        .ok_or(TranslateError::ZeroPrice)?,
    );

    // New quantity.
    let qty_str = msg
        .get_str(tags::ORDER_QTY)
        .ok_or(TranslateError::MissingTag(tags::ORDER_QTY))?;
    let new_quantity = Quantity(
        NonZeroU64::new(
            price::decimal_to_ticks(qty_str, sym_config.lot_size_inverse)
                .ok_or_else(|| TranslateError::InvalidQuantity(qty_str.to_owned()))?,
        )
        .ok_or(TranslateError::ZeroQuantity)?,
    );

    Ok(Request::CancelReplace {
        symbol: Symbol(sym_config.melin_symbol),
        account: ctx.account_id,
        order_id,
        new_price,
        new_quantity,
    })
}

/// Session-stable context required to render a Melin execution into a
/// FIX message: identity (`sender`/`target`), the symbol mapping
/// (`symbol_str`/`tick_inverse`/`lot_inverse`), and the ClOrdID lookup.
/// Bundled into one struct so the per-call render functions don't grow
/// argument lists.
pub struct FixCtx<'a> {
    pub id_map: &'a ClOrdIdMap,
    pub symbol_str: &'a str,
    pub tick_inverse: u64,
    pub lot_inverse: u64,
    pub sender: &'a str,
    pub target: &'a str,
}

/// Translate a Melin execution report into a FIX ExecutionReport (35=8).
///
/// Returns the serialized FIX message bytes ready to send.
/// `exec_id` is a monotonic counter for ExecID (tag 17).
pub fn execution_report_to_fix(
    report: &ExecutionReport,
    ctx: &FixCtx<'_>,
    side_hint: Option<Side>,
    seq: u64,
    exec_id: u64,
) -> Vec<u8> {
    let FixCtx {
        id_map,
        symbol_str,
        tick_inverse,
        lot_inverse,
        sender,
        target,
    } = *ctx;
    let hint_side = side_hint.map(fix_side).unwrap_or("1");
    match report {
        ExecutionReport::Placed {
            order_id,
            symbol: _,
            account: _,
            side,
            price,
            quantity,
        } => {
            let clord_id = resolve_clord_id(id_map, *order_id, "Placed");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TYPE, "0") // New
                .str_tag(tags::ORD_STATUS, "0") // New
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, fix_side(*side))
                .str_tag(
                    tags::ORDER_QTY,
                    &price::ticks_to_decimal(quantity.get(), lot_inverse),
                )
                .str_tag(
                    tags::PRICE,
                    &price::ticks_to_decimal(price.get(), tick_inverse),
                )
                .str_tag(
                    tags::LEAVES_QTY,
                    &price::ticks_to_decimal(quantity.get(), lot_inverse),
                )
                .str_tag(tags::CUM_QTY, "0")
                .str_tag(tags::AVG_PX, "0")
                .build(sender, target, seq)
        }
        ExecutionReport::Fill {
            maker_order_id,
            taker_order_id,
            price: fill_price,
            quantity,
            taker_fee,
            ..
        } => {
            // Fallback: emit for the taker. The session layer handles
            // Fill reports directly via fill_report_for_order to emit
            // separate reports for maker and taker with correct sides.
            let order_id = taker_order_id;
            let clord_id = match id_map
                .get_clord_id(*order_id)
                .or_else(|| id_map.get_clord_id(*maker_order_id))
            {
                Some(id) => id,
                None => {
                    tracing::warn!(
                        taker_order_id = order_id.0,
                        maker_order_id = maker_order_id.0,
                        "no ClOrdID in id_map for fill (taker or maker); emitting UNKNOWN"
                    );
                    "UNKNOWN"
                }
            };
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TYPE, "F") // Trade
                .str_tag(tags::ORD_STATUS, "2") // Filled (conservative)
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, hint_side)
                .str_tag(
                    tags::LAST_SHARES,
                    &price::ticks_to_decimal(quantity.get(), lot_inverse),
                )
                .str_tag(
                    tags::LAST_PX,
                    &price::ticks_to_decimal(fill_price.get(), tick_inverse),
                )
                .str_tag(tags::LEAVES_QTY, "0")
                .str_tag(
                    tags::CUM_QTY,
                    &price::ticks_to_decimal(quantity.get(), lot_inverse),
                )
                .str_tag(
                    tags::AVG_PX,
                    &price::ticks_to_decimal(fill_price.get(), tick_inverse),
                )
                .str_tag(tags::TEXT, &taker_fee.to_string())
                .build(sender, target, seq)
        }
        ExecutionReport::Cancelled {
            order_id,
            remaining_quantity: _,
            ..
        } => {
            let clord_id = resolve_clord_id(id_map, *order_id, "Cancelled");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TYPE, "4") // Canceled
                .str_tag(tags::ORD_STATUS, "4") // Canceled
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, hint_side)
                .str_tag(tags::LEAVES_QTY, "0")
                .str_tag(tags::CUM_QTY, "0")
                .str_tag(tags::AVG_PX, "0")
                .build(sender, target, seq)
        }
        ExecutionReport::Rejected {
            order_id, reason, ..
        } => {
            let clord_id = resolve_clord_id(id_map, *order_id, "Rejected");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TYPE, "8") // Rejected
                .str_tag(tags::ORD_STATUS, "8") // Rejected
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, hint_side)
                .str_tag(tags::ORDER_QTY, "0")
                .str_tag(tags::LEAVES_QTY, "0")
                .str_tag(tags::CUM_QTY, "0")
                .str_tag(tags::AVG_PX, "0")
                .str_tag(tags::ORD_REJ_REASON, &reject_reason_code(reason))
                .str_tag(tags::TEXT, &format!("{reason:?}"))
                .build(sender, target, seq)
        }
        ExecutionReport::Replaced {
            order_id,
            side,
            new_price,
            new_remaining,
            ..
        } => {
            let clord_id = resolve_clord_id(id_map, *order_id, "Replaced");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TYPE, "5") // Replace
                .str_tag(tags::ORD_STATUS, "0") // New (still resting)
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, fix_side(*side))
                .str_tag(
                    tags::PRICE,
                    &price::ticks_to_decimal(new_price.get(), tick_inverse),
                )
                .str_tag(
                    tags::LEAVES_QTY,
                    &price::ticks_to_decimal(new_remaining.get(), lot_inverse),
                )
                .str_tag(tags::CUM_QTY, "0")
                .str_tag(tags::AVG_PX, "0")
                .build(sender, target, seq)
        }
        ExecutionReport::Triggered {
            order_id,
            symbol: _,
            account: _,
            trigger_price,
        } => {
            let clord_id = resolve_clord_id(id_map, *order_id, "Triggered");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TYPE, "L") // Triggered
                .str_tag(tags::ORD_STATUS, "0") // New
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, hint_side)
                .str_tag(
                    tags::STOP_PX,
                    &price::ticks_to_decimal(trigger_price.get(), tick_inverse),
                )
                .str_tag(tags::LEAVES_QTY, "0")
                .str_tag(tags::CUM_QTY, "0")
                .str_tag(tags::AVG_PX, "0")
                .build(sender, target, seq)
        }
        ExecutionReport::InstrumentStatusChanged { .. } => {
            // No FIX equivalent in order entry — skip.
            Vec::new()
        }
    }
}

pub(crate) fn fix_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "1",
        Side::Sell => "2",
    }
}

/// Map Melin RejectReason to FIX OrdRejReason (tag 103) code.
fn reject_reason_code(reason: &RejectReason) -> String {
    let code = match reason {
        RejectReason::InsufficientBalance => 3, // Not enough buying power
        RejectReason::DuplicateOrderId => 6,    // Duplicate order
        RejectReason::UnknownSymbol => 11,      // Unsupported instrument
        RejectReason::TradingHalted => 14,      // Exchange closed
        RejectReason::ExceedsMaxOrderQty => 99, // Other (no standard code)
        RejectReason::ExceedsMaxNotional => 99,
        RejectReason::OutsidePriceBand => 99,
        RejectReason::NoLiquidity => 99,
        RejectReason::FOKCannotFill => 99,
        RejectReason::SelfTradePrevented => 99,
        RejectReason::PostOnlyWouldCross => 99,
        RejectReason::DuplicateRequest => 6,
        RejectReason::ReplicaDisconnected => 99,
        RejectReason::InvalidExpiry => 99,
        RejectReason::InstrumentDisabled => 14,
        _ => 99, // Other
    };
    code.to_string()
}

/// Build a FIX ExecutionReport (35=8) for a single side of a fill.
///
/// Unlike the Fill arm of `execution_report_to_fix`, this emits a
/// report for a specific `order_id` with the correct `side` and `fee`.
/// The session calls this once per order that belongs to the current
/// FIX session (maker, taker, or both).
pub fn fill_report_for_order(
    order_id: OrderId,
    side: Side,
    fill_price: Price,
    fill_quantity: Quantity,
    fee: i64,
    ctx: &FixCtx<'_>,
    seq: u64,
    exec_id: u64,
) -> Vec<u8> {
    let FixCtx {
        id_map,
        symbol_str,
        tick_inverse,
        lot_inverse,
        sender,
        target,
    } = *ctx;
    let clord_id = resolve_clord_id(id_map, order_id, "fill_report");
    FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
        .str_tag(tags::ORDER_ID, &order_id.0.to_string())
        .str_tag(tags::CL_ORD_ID, clord_id)
        .str_tag(tags::EXEC_ID, &exec_id.to_string())
        .str_tag(tags::EXEC_TYPE, "F") // Trade
        .str_tag(tags::ORD_STATUS, "2") // Filled (conservative)
        .str_tag(tags::SYMBOL, symbol_str)
        .str_tag(tags::SIDE, fix_side(side))
        .str_tag(
            tags::LAST_SHARES,
            &price::ticks_to_decimal(fill_quantity.get(), lot_inverse),
        )
        .str_tag(
            tags::LAST_PX,
            &price::ticks_to_decimal(fill_price.get(), tick_inverse),
        )
        .str_tag(tags::LEAVES_QTY, "0")
        .str_tag(
            tags::CUM_QTY,
            &price::ticks_to_decimal(fill_quantity.get(), lot_inverse),
        )
        .str_tag(
            tags::AVG_PX,
            &price::ticks_to_decimal(fill_price.get(), tick_inverse),
        )
        .str_tag(tags::TEXT, &fee.to_string())
        .build(sender, target, seq)
}

/// Build a FIX OrderCancelReject (35=9) for a failed cancel or
/// cancel-replace request.
pub fn cancel_reject_to_fix(
    order_id: OrderId,
    cancel_clord_id: &str,
    orig_clord_id: &str,
    reason: &RejectReason,
    is_replace: bool,
    sender: &str,
    target: &str,
    seq: u64,
) -> Vec<u8> {
    // CxlRejReason (102): 1=Unknown order, 3=Already pending, 99=Other.
    let cxl_reason = match reason {
        RejectReason::UnknownOrder => "1",
        RejectReason::DuplicateRequest => "3",
        _ => "99",
    };
    FixMessageBuilder::new(tags::MSG_ORDER_CANCEL_REJECT)
        .str_tag(tags::ORDER_ID, &order_id.0.to_string())
        .str_tag(tags::CL_ORD_ID, cancel_clord_id)
        .str_tag(tags::ORIG_CL_ORD_ID, orig_clord_id)
        .str_tag(tags::ORD_STATUS, "8") // Rejected
        .str_tag(
            tags::CXL_REJ_RESPONSE_TO,
            if is_replace { "2" } else { "1" },
        )
        .str_tag(tags::CXL_REJ_REASON, cxl_reason)
        .str_tag(tags::TEXT, &format!("{reason:?}"))
        .build(sender, target, seq)
}

// ---------------------------------------------------------------------------
// Order status queries (H, AF)
// ---------------------------------------------------------------------------

/// Live state of a single order, tracked per-session from ExecutionReports.
#[derive(Debug, Clone)]
pub struct OrderLiveState {
    pub symbol_str: String,
    pub side: &'static str,
    pub price: String,
    /// FIX OrdStatus(39): "0"=New, "1"=PartiallyFilled, "2"=Filled, "4"=Canceled, "8"=Rejected
    pub ord_status: &'static str,
    pub leaves_qty: String,
    pub cum_qty: u64,
    pub avg_px: u64,
    pub order_qty: String,
}

/// Build an ExecutionReport (35=8) for an OrderStatusRequest (H) response.
///
/// ExecType=I (OrderStatus) — a non-event report purely answering a query.
pub fn order_status_report(
    sender: &str,
    target: &str,
    seq: u64,
    order_id: u64,
    clord_id: &str,
    exec_id: &str,
    state: &OrderLiveState,
) -> Vec<u8> {
    FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
        .str_tag(tags::ORDER_ID, &order_id.to_string())
        .str_tag(tags::CL_ORD_ID, clord_id)
        .str_tag(tags::EXEC_ID, exec_id)
        .str_tag(tags::EXEC_TYPE, "I") // OrderStatus
        .str_tag(tags::ORD_STATUS, state.ord_status)
        .str_tag(tags::SYMBOL, &state.symbol_str)
        .str_tag(tags::SIDE, state.side)
        .str_tag(tags::ORDER_QTY, &state.order_qty)
        .str_tag(tags::PRICE, &state.price)
        .str_tag(tags::LEAVES_QTY, &state.leaves_qty)
        .str_tag(tags::CUM_QTY, &state.cum_qty.to_string())
        .str_tag(tags::AVG_PX, &state.avg_px.to_string())
        .build(sender, target, seq)
}

/// Build an ExecutionReport (35=8) as the terminating message for
/// OrderMassStatusRequest (AF) when there are no matching orders.
///
/// `TotNumReports=0` signals "query complete, nothing found".
pub fn order_mass_status_empty(
    sender: &str,
    target: &str,
    seq: u64,
    mass_status_req_id: &str,
    exec_id: &str,
) -> Vec<u8> {
    FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
        .str_tag(tags::MASS_STATUS_REQ_ID, mass_status_req_id)
        .str_tag(tags::EXEC_ID, exec_id)
        .str_tag(tags::EXEC_TYPE, "I") // OrderStatus
        .str_tag(tags::ORD_STATUS, "0") // New (nominal)
        .str_tag(tags::TOT_NUM_REPORTS, "0")
        .str_tag(tags::LAST_RPT_REQUESTED, "Y")
        .build(sender, target, seq)
}

// ---------------------------------------------------------------------------
// Position queries (AN → AP)
// ---------------------------------------------------------------------------

/// One currency balance entry in a PositionReport.
pub struct BalanceEntry {
    pub currency: String,
    pub free: u64,
    pub reserved: u64,
}

/// Build a PositionReport (35=AP) for a RequestForPositions (AN) response.
pub fn position_report_to_fix(
    sender: &str,
    target: &str,
    seq: u64,
    pos_req_id: &str,
    account: &str,
    balances: &[BalanceEntry],
) -> Vec<u8> {
    let mut builder = FixMessageBuilder::new(tags::MSG_POSITION_REPORT)
        .str_tag(tags::POS_REQ_ID, pos_req_id)
        .str_tag(tags::POS_REQ_RESULT, "0") // Valid request
        .str_tag(tags::ACCOUNT, account)
        .str_tag(tags::TOTAL_NUM_POS_REPORTS, "1")
        .str_tag(tags::NO_POSITIONS, &balances.len().to_string());

    for bal in balances {
        builder = builder
            .str_tag(tags::CURRENCY, &bal.currency)
            .str_tag(tags::LONG_QTY, &bal.free.to_string())
            .str_tag(tags::SHORT_QTY, &bal.reserved.to_string());
    }

    builder.build(sender, target, seq)
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_gateway_core::fix::parse::FixMessage;
    use melin_gateway_core::fix::serialize::FixMessageBuilder;
    use melin_trading::types::OrderId;
    use std::collections::HashMap;

    /// Build a `FixCtx` for tests using the standard MELIN/FIRM pair.
    fn ctx<'a>(id_map: &'a ClOrdIdMap, tick_inverse: u64, lot_inverse: u64) -> FixCtx<'a> {
        FixCtx {
            id_map,
            symbol_str: "BTC/USD",
            tick_inverse,
            lot_inverse,
            sender: "MELIN",
            target: "FIRM",
        }
    }

    fn sample_symbols() -> HashMap<String, SymbolConfig> {
        let mut m = HashMap::new();
        m.insert(
            "BTC/USD".to_owned(),
            SymbolConfig {
                fix_symbol: "BTC/USD".to_owned(),
                melin_symbol: 1,
                tick_size_inverse: 100,
                lot_size_inverse: 1,
            },
        );
        m
    }

    #[test]
    fn translate_limit_buy() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD001")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        let request = new_order_single(&msg, &mut ctx).unwrap();
        match request {
            Request::SubmitOrder { symbol, order } => {
                assert_eq!(symbol, Symbol(1));
                assert_eq!(order.side, Side::Buy);
                assert_eq!(order.id, OrderId(1));
                assert_eq!(order.account, AccountId(1));
                match order.order_type {
                    OrderType::Limit { price, post_only } => {
                        assert_eq!(price.get(), 5_000_000); // 50000.00 * 100
                        assert!(!post_only);
                    }
                    _ => panic!("expected Limit"),
                }
                assert_eq!(order.quantity.get(), 10);
                assert_eq!(order.time_in_force, TimeInForce::GTC);
            }
            _ => panic!("expected SubmitOrder"),
        }
    }

    #[test]
    fn translate_market_sell() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD002")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "2")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "5")
            .build("FIRM", "MELIN", 2);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        let request = new_order_single(&msg, &mut ctx).unwrap();
        match request {
            Request::SubmitOrder { order, .. } => {
                assert_eq!(order.side, Side::Sell);
                assert!(matches!(order.order_type, OrderType::Market));
            }
            _ => panic!("expected SubmitOrder"),
        }
    }

    #[test]
    fn translate_unknown_symbol_rejected() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD003")
            .str_tag(tags::SYMBOL, "DOGE/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "100")
            .build("FIRM", "MELIN", 3);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        assert!(matches!(
            new_order_single(&msg, &mut ctx),
            Err(TranslateError::UnknownSymbol(_))
        ));
    }

    #[test]
    fn translate_post_only() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD004")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "100.00")
            .str_tag(tags::ORDER_QTY, "1")
            .str_tag(tags::EXEC_INST, "6") // Post-only
            .build("FIRM", "MELIN", 4);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        match new_order_single(&msg, &mut ctx).unwrap() {
            Request::SubmitOrder { order, .. } => match order.order_type {
                OrderType::Limit { post_only, .. } => assert!(post_only),
                _ => panic!("expected Limit"),
            },
            _ => panic!("expected SubmitOrder"),
        }
    }

    #[test]
    fn translate_stop_order() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "STOP1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "2")
            .str_tag(tags::ORD_TYPE, "3") // Stop
            .str_tag(tags::STOP_PX, "48000.00")
            .str_tag(tags::ORDER_QTY, "5")
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        match new_order_single(&msg, &mut ctx).unwrap() {
            Request::SubmitOrder { order, .. } => match order.order_type {
                OrderType::Stop { trigger_price } => {
                    assert_eq!(trigger_price.get(), 4_800_000);
                }
                _ => panic!("expected Stop"),
            },
            _ => panic!("expected SubmitOrder"),
        }
    }

    #[test]
    fn translate_stop_limit_order() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "STOPLIM1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "4") // StopLimit
            .str_tag(tags::STOP_PX, "49000.00")
            .str_tag(tags::PRICE, "49500.00")
            .str_tag(tags::ORDER_QTY, "3")
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        match new_order_single(&msg, &mut ctx).unwrap() {
            Request::SubmitOrder { order, .. } => match order.order_type {
                OrderType::StopLimit {
                    trigger_price,
                    limit_price,
                } => {
                    assert_eq!(trigger_price.get(), 4_900_000);
                    assert_eq!(limit_price.get(), 4_950_000);
                }
                _ => panic!("expected StopLimit"),
            },
            _ => panic!("expected SubmitOrder"),
        }
    }

    #[test]
    fn translate_time_in_force_ioc() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "IOC1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "3") // IOC
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        match new_order_single(&msg, &mut ctx).unwrap() {
            Request::SubmitOrder { order, .. } => {
                assert_eq!(order.time_in_force, TimeInForce::IOC);
            }
            _ => panic!("expected SubmitOrder"),
        }
    }

    #[test]
    fn translate_time_in_force_fok() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "FOK1")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "4") // FOK
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        match new_order_single(&msg, &mut ctx).unwrap() {
            Request::SubmitOrder { order, .. } => {
                assert_eq!(order.time_in_force, TimeInForce::FOK);
            }
            _ => panic!("expected SubmitOrder"),
        }
    }

    #[test]
    fn translate_invalid_time_in_force() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "BAD_TIF")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "10")
            .str_tag(tags::TIME_IN_FORCE, "9") // Invalid
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        assert!(matches!(
            new_order_single(&msg, &mut ctx),
            Err(TranslateError::InvalidValue { tag: 59, .. })
        ));
    }

    #[test]
    fn translate_invalid_side() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "BAD_SIDE")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "9")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "10")
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        assert!(matches!(
            new_order_single(&msg, &mut ctx),
            Err(TranslateError::InvalidValue { tag: 54, .. })
        ));
    }

    #[test]
    fn translate_invalid_ord_type() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "BAD_OT")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "X")
            .str_tag(tags::ORDER_QTY, "10")
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        assert!(matches!(
            new_order_single(&msg, &mut ctx),
            Err(TranslateError::InvalidValue { tag: 40, .. })
        ));
    }

    #[test]
    fn translate_missing_clord_id() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "10")
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        assert!(matches!(
            new_order_single(&msg, &mut ctx),
            Err(TranslateError::MissingTag(11))
        ));
    }

    #[test]
    fn translate_zero_quantity_rejected() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ZERO_QTY")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "0")
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        assert!(matches!(
            new_order_single(&msg, &mut ctx),
            Err(TranslateError::ZeroQuantity)
        ));
    }

    #[test]
    fn translate_account_override() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ACCT_OVR")
            .str_tag(tags::ACCOUNT, "42")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "1")
            .build("FIRM", "MELIN", 1);

        let msg = FixMessage::parse(&raw).unwrap();
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        match new_order_single(&msg, &mut ctx).unwrap() {
            Request::SubmitOrder { order, .. } => {
                assert_eq!(order.account, AccountId(42));
            }
            _ => panic!("expected SubmitOrder"),
        }
    }

    #[test]
    fn translate_cancel_order() {
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();

        // First, submit an order so it's registered in the id_map.
        let submit_raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORIG001")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .str_tag(tags::ORDER_QTY, "10")
            .build("FIRM", "MELIN", 1);
        let submit_msg = FixMessage::parse(&submit_raw).unwrap();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };
        new_order_single(&submit_msg, &mut ctx).unwrap();

        // Now cancel it.
        let cancel_raw = FixMessageBuilder::new(tags::MSG_ORDER_CANCEL_REQUEST)
            .str_tag(tags::CL_ORD_ID, "CXL001")
            .str_tag(tags::ORIG_CL_ORD_ID, "ORIG001")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .build("FIRM", "MELIN", 2);
        let cancel_msg = FixMessage::parse(&cancel_raw).unwrap();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        match cancel_order(&cancel_msg, &mut ctx).unwrap() {
            Request::CancelOrder {
                symbol,
                account,
                order_id,
            } => {
                assert_eq!(symbol, Symbol(1));
                assert_eq!(account, AccountId(1));
                assert_eq!(order_id, OrderId(1));
            }
            _ => panic!("expected CancelOrder"),
        }
    }

    #[test]
    fn translate_cancel_unknown_order() {
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();

        let cancel_raw = FixMessageBuilder::new(tags::MSG_ORDER_CANCEL_REQUEST)
            .str_tag(tags::CL_ORD_ID, "CXL002")
            .str_tag(tags::ORIG_CL_ORD_ID, "NONEXISTENT")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .build("FIRM", "MELIN", 1);
        let cancel_msg = FixMessage::parse(&cancel_raw).unwrap();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        assert!(matches!(
            cancel_order(&cancel_msg, &mut ctx),
            Err(TranslateError::InvalidValue { tag: 41, .. })
        ));
    }

    #[test]
    fn translate_cancel_replace_order() {
        let symbols = sample_symbols();
        let mut id_map = ClOrdIdMap::new();

        // Submit original order.
        let submit_raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORIG002")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::ORDER_QTY, "10")
            .build("FIRM", "MELIN", 1);
        let submit_msg = FixMessage::parse(&submit_raw).unwrap();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };
        new_order_single(&submit_msg, &mut ctx).unwrap();

        // Cancel-replace with new price and quantity.
        let replace_raw = FixMessageBuilder::new(tags::MSG_ORDER_CANCEL_REPLACE)
            .str_tag(tags::CL_ORD_ID, "REP001")
            .str_tag(tags::ORIG_CL_ORD_ID, "ORIG002")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "51000.00")
            .str_tag(tags::ORDER_QTY, "15")
            .build("FIRM", "MELIN", 2);
        let replace_msg = FixMessage::parse(&replace_raw).unwrap();
        let mut ctx = TranslateContext {
            account_id: AccountId(1),
            symbols: &symbols,
            id_map: &mut id_map,
        };

        match cancel_replace(&replace_msg, &mut ctx).unwrap() {
            Request::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                assert_eq!(symbol, Symbol(1));
                assert_eq!(account, AccountId(1));
                assert_eq!(order_id, OrderId(1));
                assert_eq!(new_price.get(), 5_100_000); // 51000.00 * 100
                assert_eq!(new_quantity.get(), 15);
            }
            _ => panic!("expected CancelReplace"),
        }
    }

    #[test]
    fn exec_report_placed() {
        let mut id_map = ClOrdIdMap::new();
        id_map.insert("ORD_P1");

        let report = ExecutionReport::Placed {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Buy,
            price: Price(NonZeroU64::new(5_000_000).unwrap()),
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
        };

        let fix_bytes = execution_report_to_fix(&report, &ctx(&id_map, 100, 1), None, 1, 1);

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("ORD_P1"));
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("0")); // New
        assert_eq!(msg.get_str(tags::ORD_STATUS), Some("0")); // New
        assert_eq!(msg.get_str(tags::SYMBOL), Some("BTC/USD"));
        assert_eq!(msg.get_str(tags::SIDE), Some("1")); // Buy
        assert_eq!(msg.get_str(tags::PRICE), Some("50000.00"));
        assert_eq!(msg.get_str(tags::ORDER_QTY), Some("10"));
    }

    #[test]
    fn exec_report_fill() {
        let mut id_map = ClOrdIdMap::new();
        id_map.insert("MAKER1");
        id_map.insert("TAKER1");

        let report = ExecutionReport::Fill {
            maker_order_id: OrderId(1),
            taker_order_id: OrderId(2),
            symbol: Symbol(1),
            maker_account: AccountId(10),
            taker_account: AccountId(20),
            price: Price(NonZeroU64::new(5_000_000).unwrap()),
            quantity: Quantity(NonZeroU64::new(5).unwrap()),
            maker_fee: -10,
            taker_fee: 25,
        };

        let fix_bytes = execution_report_to_fix(&report, &ctx(&id_map, 100, 1), None, 1, 1);

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("F")); // Trade
        assert_eq!(msg.get_str(tags::LAST_PX), Some("50000.00"));
        assert_eq!(msg.get_str(tags::LAST_SHARES), Some("5"));
        // Taker fee in Text field.
        assert_eq!(msg.get_str(tags::TEXT), Some("25"));
    }

    #[test]
    fn exec_report_cancelled() {
        let mut id_map = ClOrdIdMap::new();
        id_map.insert("CXL_ORD");

        let report = ExecutionReport::Cancelled {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            remaining_quantity: Quantity(NonZeroU64::new(5).unwrap()),
        };

        let fix_bytes =
            execution_report_to_fix(&report, &ctx(&id_map, 100, 1), Some(Side::Sell), 1, 1);

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("4")); // Canceled
        assert_eq!(msg.get_str(tags::ORD_STATUS), Some("4"));
        assert_eq!(msg.get_str(tags::LEAVES_QTY), Some("0"));
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("CXL_ORD"));
        assert_eq!(msg.get_str(tags::SIDE), Some("2")); // Sell via side_hint
    }

    #[test]
    fn exec_report_rejected() {
        let mut id_map = ClOrdIdMap::new();
        id_map.insert("REJ_ORD");

        let report = ExecutionReport::Rejected {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            reason: RejectReason::InsufficientBalance,
        };

        let fix_bytes = execution_report_to_fix(&report, &ctx(&id_map, 100, 1), None, 1, 1);

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("8")); // Rejected
        assert_eq!(msg.get_str(tags::ORD_STATUS), Some("8"));
        assert_eq!(msg.get_str(tags::ORD_REJ_REASON), Some("3")); // Insufficient buying power
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("REJ_ORD"));
    }

    #[test]
    fn exec_report_replaced() {
        let mut id_map = ClOrdIdMap::new();
        id_map.insert("REP_ORD");

        let report = ExecutionReport::Replaced {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Sell,
            old_price: Price(NonZeroU64::new(5_000_000).unwrap()),
            new_price: Price(NonZeroU64::new(5_100_000).unwrap()),
            old_remaining: Quantity(NonZeroU64::new(10).unwrap()),
            new_remaining: Quantity(NonZeroU64::new(15).unwrap()),
        };

        let fix_bytes = execution_report_to_fix(&report, &ctx(&id_map, 100, 1), None, 1, 1);

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("5")); // Replace
        assert_eq!(msg.get_str(tags::ORD_STATUS), Some("0")); // Still resting
        assert_eq!(msg.get_str(tags::SIDE), Some("2")); // Sell
        assert_eq!(msg.get_str(tags::PRICE), Some("51000.00"));
        assert_eq!(msg.get_str(tags::LEAVES_QTY), Some("15"));
    }

    #[test]
    fn exec_report_triggered() {
        let mut id_map = ClOrdIdMap::new();
        id_map.insert("TRIG_ORD");

        let report = ExecutionReport::Triggered {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            trigger_price: Price(NonZeroU64::new(4_800_000).unwrap()),
        };

        let fix_bytes = execution_report_to_fix(&report, &ctx(&id_map, 100, 1), None, 1, 1);

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("L")); // Triggered
        assert_eq!(msg.get_str(tags::STOP_PX), Some("48000.00"));
    }

    #[test]
    fn exec_report_instrument_status_empty() {
        let id_map = ClOrdIdMap::new();

        let report = ExecutionReport::InstrumentStatusChanged {
            symbol: Symbol(1),
            status: melin_trading::types::InstrumentStatus::Enabled,
        };

        let fix_bytes = execution_report_to_fix(&report, &ctx(&id_map, 100, 1), None, 1, 1);

        // InstrumentStatusChanged has no FIX equivalent — empty output.
        assert!(fix_bytes.is_empty());
    }

    #[test]
    fn exec_report_unknown_clord_id_uses_unknown() {
        let id_map = ClOrdIdMap::new(); // Empty map.

        let report = ExecutionReport::Placed {
            order_id: OrderId(999),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Buy,
            price: Price(NonZeroU64::new(100).unwrap()),
            quantity: Quantity(NonZeroU64::new(1).unwrap()),
        };

        let fix_bytes = execution_report_to_fix(&report, &ctx(&id_map, 1, 1), None, 1, 1);

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("UNKNOWN"));
    }

    #[test]
    fn fill_report_for_specific_order() {
        let mut id_map = ClOrdIdMap::new();
        id_map.insert("BUY_ORD");

        let fix_bytes = fill_report_for_order(
            OrderId(1),
            Side::Buy,
            Price(NonZeroU64::new(5_000_000).unwrap()),
            Quantity(NonZeroU64::new(10).unwrap()),
            -5, // Maker rebate
            &ctx(&id_map, 100, 1),
            1,
            1,
        );

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("BUY_ORD"));
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("F"));
        assert_eq!(msg.get_str(tags::SIDE), Some("1")); // Buy — correct side
        assert_eq!(msg.get_str(tags::LAST_PX), Some("50000.00"));
        assert_eq!(msg.get_str(tags::LAST_SHARES), Some("10"));
        assert_eq!(msg.get_str(tags::TEXT), Some("-5")); // Rebate
    }

    #[test]
    fn fill_report_sell_side() {
        let mut id_map = ClOrdIdMap::new();
        id_map.insert("SELL_ORD");

        let fix_bytes = fill_report_for_order(
            OrderId(1),
            Side::Sell,
            Price(NonZeroU64::new(100).unwrap()),
            Quantity(NonZeroU64::new(1).unwrap()),
            10,
            &ctx(&id_map, 1, 1),
            1,
            1,
        );

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.get_str(tags::SIDE), Some("2")); // Sell
    }

    #[test]
    fn cancel_reject_for_cancel() {
        let fix_bytes = cancel_reject_to_fix(
            OrderId(42),
            "CXL_REQ_1",
            "ORIG_ORD_1",
            &RejectReason::UnknownOrder,
            false, // cancel, not replace
            "MELIN",
            "FIRM",
            1,
        );

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_ORDER_CANCEL_REJECT);
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("CXL_REQ_1"));
        assert_eq!(msg.get_str(tags::ORIG_CL_ORD_ID), Some("ORIG_ORD_1"));
        assert_eq!(msg.get_str(tags::ORD_STATUS), Some("8"));
        assert_eq!(msg.get_str(tags::CXL_REJ_RESPONSE_TO), Some("1")); // Cancel
        assert_eq!(msg.get_str(tags::CXL_REJ_REASON), Some("1")); // Unknown order
    }

    #[test]
    fn cancel_reject_for_replace() {
        let fix_bytes = cancel_reject_to_fix(
            OrderId(42),
            "REP_REQ_1",
            "ORIG_ORD_1",
            &RejectReason::DuplicateRequest,
            true, // replace
            "MELIN",
            "FIRM",
            1,
        );

        let msg = FixMessage::parse(&fix_bytes).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_ORDER_CANCEL_REJECT);
        assert_eq!(msg.get_str(tags::CXL_REJ_RESPONSE_TO), Some("2")); // Replace
        assert_eq!(msg.get_str(tags::CXL_REJ_REASON), Some("3")); // Already pending
    }

    #[test]
    fn order_status_report_builds_valid_fix() {
        let state = OrderLiveState {
            symbol_str: "BTC/USD".to_string(),
            side: "1", // Buy
            price: "50000".to_string(),
            ord_status: "0", // New
            leaves_qty: "10".to_string(),
            cum_qty: 0,
            avg_px: 0,
            order_qty: "10".to_string(),
        };
        let msg_bytes = order_status_report("MELIN", "FIRM", 1, 42, "CLO1", "E1", &state);
        let msg = FixMessage::parse(&msg_bytes).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("I"));
        assert_eq!(msg.get_str(tags::ORD_STATUS), Some("0"));
        assert_eq!(msg.get_str(tags::ORDER_ID), Some("42"));
        assert_eq!(msg.get_str(tags::SYMBOL), Some("BTC/USD"));
        assert_eq!(msg.get_str(tags::LEAVES_QTY), Some("10"));
    }

    #[test]
    fn order_mass_status_empty_builds_valid_fix() {
        let msg_bytes = order_mass_status_empty("MELIN", "FIRM", 1, "MSR1", "E2");
        let msg = FixMessage::parse(&msg_bytes).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_EXECUTION_REPORT);
        assert_eq!(msg.get_str(tags::EXEC_TYPE), Some("I"));
        assert_eq!(msg.get_str(tags::TOT_NUM_REPORTS), Some("0"));
        assert_eq!(msg.get_str(tags::LAST_RPT_REQUESTED), Some("Y"));
        assert_eq!(msg.get_str(tags::MASS_STATUS_REQ_ID), Some("MSR1"));
    }

    #[test]
    fn position_report_builds_valid_fix() {
        let balances = vec![
            BalanceEntry {
                currency: "BTC".to_string(),
                free: 100,
                reserved: 20,
            },
            BalanceEntry {
                currency: "USD".to_string(),
                free: 50000,
                reserved: 10000,
            },
        ];
        let msg_bytes = position_report_to_fix("MELIN", "FIRM", 1, "PR1", "ACCT1", &balances);
        let msg = FixMessage::parse(&msg_bytes).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_POSITION_REPORT);
        assert_eq!(msg.get_str(tags::POS_REQ_ID), Some("PR1"));
        assert_eq!(msg.get_str(tags::ACCOUNT), Some("ACCT1"));
        assert_eq!(msg.get_str(tags::NO_POSITIONS), Some("2"));

        let currencies: Vec<_> = msg
            .fields_iter()
            .filter(|f| f.tag == tags::CURRENCY)
            .collect();
        assert_eq!(currencies.len(), 2);
        assert_eq!(std::str::from_utf8(currencies[0].value).unwrap(), "BTC");
        assert_eq!(std::str::from_utf8(currencies[1].value).unwrap(), "USD");
    }
}
