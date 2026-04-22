//! FIX 4.4 market data message builders.
//!
//! Translates internal `MdOutput` types into FIX MarketDataSnapshotFullRefresh (W)
//! and MarketDataRequestReject (Y) messages.

use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;
use melin_market_data::mirror::Level;
use melin_trading::types::Price;

/// Build a `MarketDataSnapshotFullRefresh` (35=W) message for a single symbol.
///
/// Contains bid and ask levels as `NoMDEntries` repeating group entries.
/// Bids are emitted first (descending price), then asks (ascending price).
pub fn md_snapshot_to_fix(
    md_req_id: &str,
    symbol_str: &str,
    bids: &[(Price, Level)],
    asks: &[(Price, Level)],
    tick_inverse: u64,
) -> FixMessageBuilder {
    let mut builder = FixMessageBuilder::new(tags::MSG_MD_SNAPSHOT);
    builder = builder
        .str_tag(tags::MD_REQ_ID, md_req_id)
        .str_tag(tags::SYMBOL, symbol_str);

    let total_entries = bids.len() + asks.len();
    builder = builder.str_tag(tags::NO_MD_ENTRIES, &total_entries.to_string());

    // Bids (MDEntryType=0).
    for (price, level) in bids {
        builder = builder
            .str_tag(tags::MD_ENTRY_TYPE, "0") // Bid
            .str_tag(
                tags::MD_ENTRY_PX,
                &ticks_to_decimal(price.get(), tick_inverse),
            )
            .str_tag(tags::MD_ENTRY_SIZE, &level.total_qty.to_string())
            .str_tag(tags::NUMBER_OF_ORDERS, &level.order_count.to_string());
    }

    // Asks (MDEntryType=1).
    for (price, level) in asks {
        builder = builder
            .str_tag(tags::MD_ENTRY_TYPE, "1") // Offer
            .str_tag(
                tags::MD_ENTRY_PX,
                &ticks_to_decimal(price.get(), tick_inverse),
            )
            .str_tag(tags::MD_ENTRY_SIZE, &level.total_qty.to_string())
            .str_tag(tags::NUMBER_OF_ORDERS, &level.order_count.to_string());
    }

    builder
}

/// Build a `MarketDataRequestReject` (35=Y) message.
///
/// `reason`: FIX MDReqRejReason(281) — 0=UnknownSymbol, 1=DuplicateMDReqID, etc.
pub fn md_request_reject(md_req_id: &str, reason: &str, text: &str) -> FixMessageBuilder {
    let mut builder = FixMessageBuilder::new(tags::MSG_MD_REQUEST_REJECT);
    builder = builder
        .str_tag(tags::MD_REQ_ID, md_req_id)
        .str_tag(tags::MD_REQ_REJ_REASON, reason);
    if !text.is_empty() {
        builder = builder.str_tag(tags::TEXT, text);
    }
    builder
}

/// Build a `MarketDataIncrementalRefresh` (35=X) message.
///
/// Each update carries `MDUpdateAction` (0=New, 1=Change, 2=Delete),
/// `MDEntryType` (0=Bid, 1=Offer), price, size, and order count.
pub fn md_incremental_to_fix(
    md_req_id: &str,
    updates: &[IncrementalEntry<'_>],
) -> FixMessageBuilder {
    let mut builder = FixMessageBuilder::new(tags::MSG_MD_INCREMENTAL);
    builder = builder
        .str_tag(tags::MD_REQ_ID, md_req_id)
        .str_tag(tags::NO_MD_ENTRIES, &updates.len().to_string());

    for entry in updates {
        builder = builder
            .str_tag(tags::MD_UPDATE_ACTION, entry.action)
            .str_tag(tags::MD_ENTRY_TYPE, entry.entry_type)
            .str_tag(tags::SYMBOL, entry.symbol)
            .str_tag(tags::MD_ENTRY_PX, &entry.price)
            .str_tag(tags::MD_ENTRY_SIZE, &entry.size)
            .str_tag(tags::NUMBER_OF_ORDERS, &entry.order_count);
    }

    builder
}

/// One entry in an incremental refresh message.
pub struct IncrementalEntry<'a> {
    /// "0" = New, "1" = Change, "2" = Delete
    pub action: &'a str,
    /// "0" = Bid, "1" = Offer, "2" = Trade
    pub entry_type: &'a str,
    pub symbol: &'a str,
    pub price: String,
    pub size: String,
    pub order_count: String,
}

/// Build a `SecurityList` (35=y) message.
///
/// Responds to a `SecurityListRequest (x)` with all configured symbols.
pub fn security_list_to_fix(security_req_id: &str, symbols: &[SecurityInfo]) -> FixMessageBuilder {
    let mut builder = FixMessageBuilder::new(tags::MSG_SECURITY_LIST);
    builder = builder
        .str_tag(tags::SECURITY_REQ_ID, security_req_id)
        .str_tag(tags::SECURITY_RESPONSE_ID, security_req_id)
        // SecurityRequestResult=0 (valid request)
        .str_tag(tags::SECURITY_REQUEST_RESULT, "0")
        .str_tag(tags::NO_RELATED_SYM, &symbols.len().to_string());

    for sym in symbols {
        builder = builder
            .str_tag(tags::SYMBOL, &sym.symbol)
            .str_tag(tags::CURRENCY, &sym.base_ccy)
            .str_tag(tags::SETTL_CURRENCY, &sym.quote_ccy);
        if !sym.min_price_increment.is_empty() {
            builder = builder.str_tag(tags::MIN_PRICE_INCREMENT, &sym.min_price_increment);
        }
        if !sym.round_lot.is_empty() {
            builder = builder.str_tag(tags::ROUND_LOT, &sym.round_lot);
        }
    }

    builder
}

/// Security metadata for the SecurityList response.
pub struct SecurityInfo {
    pub symbol: String,
    pub base_ccy: String,
    pub quote_ccy: String,
    pub min_price_increment: String,
    pub round_lot: String,
}

/// Convert integer ticks to a decimal string.
///
/// `tick_inverse` is the divisor (e.g. 100 → 2 decimal places).
/// tick_inverse=1 produces integer output (no decimal point).
pub fn ticks_to_decimal(ticks: u64, tick_inverse: u64) -> String {
    if tick_inverse <= 1 {
        return ticks.to_string();
    }
    let whole = ticks / tick_inverse;
    let frac = ticks % tick_inverse;
    if frac == 0 {
        format!("{whole}.0")
    } else {
        // Compute decimal digits needed for the fractional part.
        let width = tick_inverse.ilog10() as usize;
        format!("{whole}.{frac:0>width$}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_gateway_core::fix::parse::FixMessage;
    use std::num::NonZeroU64;

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    #[test]
    fn snapshot_empty_book() {
        let builder = md_snapshot_to_fix("REQ1", "BTCUSD", &[], &[], 1);
        let msg = builder.build("MELIN", "CLIENT", 1);
        let parsed = FixMessage::parse(&msg).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_MD_SNAPSHOT);
        assert_eq!(parsed.get_str(tags::MD_REQ_ID), Some("REQ1"));
        assert_eq!(parsed.get_str(tags::SYMBOL), Some("BTCUSD"));
        assert_eq!(parsed.get_str(tags::NO_MD_ENTRIES), Some("0"));
    }

    #[test]
    fn snapshot_with_levels() {
        let bids = vec![(
            price(100),
            Level {
                total_qty: 50,
                order_count: 3,
            },
        )];
        let asks = vec![(
            price(200),
            Level {
                total_qty: 30,
                order_count: 2,
            },
        )];
        let builder = md_snapshot_to_fix("REQ2", "ETHUSD", &bids, &asks, 1);
        let msg = builder.build("MELIN", "CLIENT", 1);
        let parsed = FixMessage::parse(&msg).unwrap();

        assert_eq!(parsed.get_str(tags::NO_MD_ENTRIES), Some("2"));

        // Verify the first entry is a bid.
        let entries: Vec<_> = parsed
            .fields_iter()
            .filter(|f| f.tag == tags::MD_ENTRY_TYPE)
            .collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].value, b"0"); // Bid
        assert_eq!(entries[1].value, b"1"); // Offer
    }

    #[test]
    fn snapshot_with_decimals() {
        let bids = vec![(
            price(12345),
            Level {
                total_qty: 10,
                order_count: 1,
            },
        )];
        let builder = md_snapshot_to_fix("REQ3", "BTCUSD", &bids, &[], 100);
        let msg = builder.build("MELIN", "CLIENT", 1);
        let parsed = FixMessage::parse(&msg).unwrap();

        // 12345 / 100 = 123.45
        let px: Vec<_> = parsed
            .fields_iter()
            .filter(|f| f.tag == tags::MD_ENTRY_PX)
            .collect();
        assert_eq!(px.len(), 1);
        assert_eq!(std::str::from_utf8(px[0].value).unwrap(), "123.45");
    }

    #[test]
    fn reject_message() {
        let builder = md_request_reject("REQ1", "0", "Unknown symbol");
        let msg = builder.build("MELIN", "CLIENT", 1);
        let parsed = FixMessage::parse(&msg).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_MD_REQUEST_REJECT);
        assert_eq!(parsed.get_str(tags::MD_REQ_ID), Some("REQ1"));
        assert_eq!(parsed.get_str(tags::MD_REQ_REJ_REASON), Some("0"));
        assert_eq!(parsed.get_str(tags::TEXT), Some("Unknown symbol"));
    }

    #[test]
    fn ticks_to_decimal_integer() {
        assert_eq!(ticks_to_decimal(12345, 1), "12345");
    }

    #[test]
    fn ticks_to_decimal_two_places() {
        assert_eq!(ticks_to_decimal(12345, 100), "123.45");
        assert_eq!(ticks_to_decimal(12300, 100), "123.0");
        assert_eq!(ticks_to_decimal(5, 100), "0.05");
    }

    #[test]
    fn ticks_to_decimal_three_places() {
        assert_eq!(ticks_to_decimal(12345, 1000), "12.345");
    }

    #[test]
    fn security_list_response() {
        let symbols = vec![
            SecurityInfo {
                symbol: "BTCUSD".to_string(),
                base_ccy: "BTC".to_string(),
                quote_ccy: "USD".to_string(),
                min_price_increment: "0.01".to_string(),
                round_lot: "1".to_string(),
            },
            SecurityInfo {
                symbol: "ETHUSD".to_string(),
                base_ccy: "ETH".to_string(),
                quote_ccy: "USD".to_string(),
                min_price_increment: "0.1".to_string(),
                round_lot: "1".to_string(),
            },
        ];

        let builder = security_list_to_fix("REQ42", &symbols);
        let msg = builder.build("MELIN", "CLIENT", 1);
        let parsed = FixMessage::parse(&msg).unwrap();

        assert_eq!(parsed.msg_type(), tags::MSG_SECURITY_LIST);
        assert_eq!(parsed.get_str(tags::SECURITY_REQ_ID), Some("REQ42"));
        assert_eq!(parsed.get_str(tags::SECURITY_REQUEST_RESULT), Some("0"));
        assert_eq!(parsed.get_str(tags::NO_RELATED_SYM), Some("2"));

        let sym_fields: Vec<_> = parsed
            .fields_iter()
            .filter(|f| f.tag == tags::SYMBOL)
            .collect();
        assert_eq!(sym_fields.len(), 2);
        assert_eq!(std::str::from_utf8(sym_fields[0].value).unwrap(), "BTCUSD");
        assert_eq!(std::str::from_utf8(sym_fields[1].value).unwrap(), "ETHUSD");
    }

    #[test]
    fn incremental_refresh() {
        let updates = vec![
            IncrementalEntry {
                action: "1",     // Change
                entry_type: "0", // Bid
                symbol: "BTCUSD",
                price: "100.50".to_string(),
                size: "25".to_string(),
                order_count: "3".to_string(),
            },
            IncrementalEntry {
                action: "2",     // Delete
                entry_type: "1", // Offer
                symbol: "BTCUSD",
                price: "101.00".to_string(),
                size: "0".to_string(),
                order_count: "0".to_string(),
            },
        ];

        let builder = md_incremental_to_fix("REQ1", &updates);
        let msg = builder.build("MELIN", "CLIENT", 1);
        let parsed = FixMessage::parse(&msg).unwrap();

        assert_eq!(parsed.msg_type(), tags::MSG_MD_INCREMENTAL);
        assert_eq!(parsed.get_str(tags::MD_REQ_ID), Some("REQ1"));
        assert_eq!(parsed.get_str(tags::NO_MD_ENTRIES), Some("2"));

        let actions: Vec<_> = parsed
            .fields_iter()
            .filter(|f| f.tag == tags::MD_UPDATE_ACTION)
            .collect();
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0].value, b"1"); // Change
        assert_eq!(actions[1].value, b"2"); // Delete
    }
}
