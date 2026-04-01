//! FIX ↔ Melin message translation.
//!
//! Converts FIX NewOrderSingle/Cancel/CancelReplace into Melin `Request`
//! variants, and Melin execution reports back into FIX messages.

use std::num::NonZeroU64;

use melin_engine::types::{
    AccountId, ExecutionReport, OrderType, Price, Quantity, RejectReason,
    SelfTradeProtection, Side, Symbol, TimeInForce,
};
use melin_protocol::message::Request;

use crate::config::SymbolConfig;
use crate::fix::parse::FixMessage;
use crate::fix::serialize::FixMessageBuilder;
use crate::fix::tags;
use crate::id_map::ClOrdIdMap;
use crate::price;

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
            Self::InvalidValue { tag, value } => write!(f, "invalid value for tag {tag}: '{value}'"),
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
    let quantity = Quantity(
        NonZeroU64::new(qty_ticks).ok_or(TranslateError::ZeroQuantity)?,
    );

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
            let trigger_ticks =
                price::decimal_to_ticks(stop_str, sym_config.tick_size_inverse)
                    .ok_or_else(|| TranslateError::InvalidPrice(stop_str.to_owned()))?;
            let limit_ticks =
                price::decimal_to_ticks(price_str, sym_config.tick_size_inverse)
                    .ok_or_else(|| TranslateError::InvalidPrice(price_str.to_owned()))?;
            OrderType::StopLimit {
                trigger_price: Price(
                    NonZeroU64::new(trigger_ticks).ok_or(TranslateError::ZeroPrice)?,
                ),
                limit_price: Price(
                    NonZeroU64::new(limit_ticks).ok_or(TranslateError::ZeroPrice)?,
                ),
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
    // For v1, we pass 0 — GTD expiry is managed by the operator via
    // ExpireOrders commands, not per-order timestamps from FIX.
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
        order: melin_engine::types::Order {
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

    let order_id = ctx
        .id_map
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

    let order_id = ctx
        .id_map
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

/// Translate a Melin execution report into a FIX ExecutionReport (35=8).
///
/// Returns the serialized FIX message bytes ready to send.
/// `exec_id` is a monotonic counter for ExecID (tag 17).
pub fn execution_report_to_fix(
    report: &ExecutionReport,
    id_map: &ClOrdIdMap,
    symbol_str: &str,
    tick_inverse: u64,
    lot_inverse: u64,
    sender: &str,
    target: &str,
    seq: u64,
    exec_id: u64,
) -> Vec<u8> {
    match report {
        ExecutionReport::Placed {
            order_id,
            side,
            price,
            quantity,
        } => {
            let clord_id = id_map
                .get_clord_id(*order_id)
                .unwrap_or("UNKNOWN");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TRANS_TYPE, "0") // New
                .str_tag(tags::EXEC_TYPE, "0") // New
                .str_tag(tags::ORD_STATUS, "0") // New
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, fix_side(*side))
                .str_tag(tags::ORDER_QTY, &price::ticks_to_decimal(quantity.get(), lot_inverse))
                .str_tag(tags::PRICE, &price::ticks_to_decimal(price.get(), tick_inverse))
                .str_tag(tags::LEAVES_QTY, &price::ticks_to_decimal(quantity.get(), lot_inverse))
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
            // Emit for the taker side (the most recent aggressor).
            // A proper implementation would emit separate reports for
            // maker and taker; for v1 we emit for the taker.
            let order_id = taker_order_id;
            let clord_id = id_map
                .get_clord_id(*order_id)
                .or_else(|| id_map.get_clord_id(*maker_order_id))
                .unwrap_or("UNKNOWN");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TRANS_TYPE, "0")
                .str_tag(tags::EXEC_TYPE, "F") // Trade
                .str_tag(tags::ORD_STATUS, "2") // Filled (conservative)
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, "1") // Placeholder — Fill doesn't carry side
                .str_tag(tags::LAST_SHARES, &price::ticks_to_decimal(quantity.get(), lot_inverse))
                .str_tag(tags::LAST_PX, &price::ticks_to_decimal(fill_price.get(), tick_inverse))
                .str_tag(tags::LEAVES_QTY, "0")
                .str_tag(tags::CUM_QTY, &price::ticks_to_decimal(quantity.get(), lot_inverse))
                .str_tag(tags::AVG_PX, &price::ticks_to_decimal(fill_price.get(), tick_inverse))
                .str_tag(tags::TEXT, &taker_fee.to_string()) // Fee in text for visibility
                .build(sender, target, seq)
        }
        ExecutionReport::Cancelled {
            order_id,
            remaining_quantity: _,
            ..
        } => {
            let clord_id = id_map
                .get_clord_id(*order_id)
                .unwrap_or("UNKNOWN");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TRANS_TYPE, "0")
                .str_tag(tags::EXEC_TYPE, "4") // Canceled
                .str_tag(tags::ORD_STATUS, "4") // Canceled
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, "1") // Placeholder
                .str_tag(tags::LEAVES_QTY, "0")
                .str_tag(tags::CUM_QTY, "0")
                .str_tag(tags::AVG_PX, "0")
                .build(sender, target, seq)
        }
        ExecutionReport::Rejected {
            order_id,
            reason,
            ..
        } => {
            let clord_id = id_map
                .get_clord_id(*order_id)
                .unwrap_or("UNKNOWN");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TRANS_TYPE, "0")
                .str_tag(tags::EXEC_TYPE, "8") // Rejected
                .str_tag(tags::ORD_STATUS, "8") // Rejected
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, "1") // Placeholder
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
            let clord_id = id_map
                .get_clord_id(*order_id)
                .unwrap_or("UNKNOWN");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TRANS_TYPE, "0")
                .str_tag(tags::EXEC_TYPE, "5") // Replace
                .str_tag(tags::ORD_STATUS, "0") // New (still resting)
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, fix_side(*side))
                .str_tag(tags::PRICE, &price::ticks_to_decimal(new_price.get(), tick_inverse))
                .str_tag(tags::LEAVES_QTY, &price::ticks_to_decimal(new_remaining.get(), lot_inverse))
                .str_tag(tags::CUM_QTY, "0")
                .str_tag(tags::AVG_PX, "0")
                .build(sender, target, seq)
        }
        ExecutionReport::Triggered {
            order_id,
            trigger_price,
        } => {
            let clord_id = id_map
                .get_clord_id(*order_id)
                .unwrap_or("UNKNOWN");
            FixMessageBuilder::new(tags::MSG_EXECUTION_REPORT)
                .str_tag(tags::ORDER_ID, &order_id.0.to_string())
                .str_tag(tags::CL_ORD_ID, clord_id)
                .str_tag(tags::EXEC_ID, &exec_id.to_string())
                .str_tag(tags::EXEC_TRANS_TYPE, "0")
                .str_tag(tags::EXEC_TYPE, "L") // Triggered
                .str_tag(tags::ORD_STATUS, "0") // New
                .str_tag(tags::SYMBOL, symbol_str)
                .str_tag(tags::SIDE, "1") // Placeholder
                .str_tag(tags::STOP_PX, &price::ticks_to_decimal(trigger_price.get(), tick_inverse))
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

fn fix_side(side: Side) -> &'static str {
    match side {
        Side::Buy => "1",
        Side::Sell => "2",
    }
}

/// Map Melin RejectReason to FIX OrdRejReason (tag 103) code.
fn reject_reason_code(reason: &RejectReason) -> String {
    let code = match reason {
        RejectReason::InsufficientBalance => 3,    // Not enough buying power
        RejectReason::DuplicateOrderId => 6,       // Duplicate order
        RejectReason::UnknownSymbol => 11,         // Unsupported instrument
        RejectReason::TradingHalted => 14,         // Exchange closed
        RejectReason::ExceedsMaxOrderQty => 99,    // Other (no standard code)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fix::parse::FixMessage;
    use crate::fix::serialize::FixMessageBuilder;
    use melin_engine::types::OrderId;
    use std::collections::HashMap;

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
}
