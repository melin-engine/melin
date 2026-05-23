//! Shared fixtures and order-builder helpers used by both
//! `exchange_tests.rs` and the per-submodule test files (currently
//! only `token_bucket_tests.rs`). Keeping them here avoids duplicating
//! the constants and tiny builders across siblings under `mod exchange`.

#![cfg(test)]

use std::num::NonZeroU64;

use super::Exchange;
use crate::types::{
    AccountId, CurrencyId, InstrumentSpec, Order, OrderId, OrderType, Price, Quantity,
    SelfTradeProtection, Side, Symbol, TimeInForce,
};

pub(super) const ACCT_A: AccountId = AccountId(1);
pub(super) const ACCT_B: AccountId = AccountId(2);
pub(super) const BTC: CurrencyId = CurrencyId(0);
pub(super) const USD: CurrencyId = CurrencyId(1);
pub(super) const ETH: CurrencyId = CurrencyId(2);

pub(super) fn btc_usd_spec() -> InstrumentSpec {
    InstrumentSpec {
        symbol: Symbol(1),
        base: BTC,
        quote: USD,
    }
}

pub(super) fn eth_usd_spec() -> InstrumentSpec {
    InstrumentSpec {
        symbol: Symbol(2),
        base: ETH,
        quote: USD,
    }
}

pub(super) fn qty(n: u64) -> Quantity {
    Quantity(NonZeroU64::new(n).unwrap())
}

pub(super) fn price(n: u64) -> Price {
    Price(NonZeroU64::new(n).unwrap())
}

pub(super) fn limit_order(
    id: u64,
    account: AccountId,
    side: Side,
    p: u64,
    q: u64,
    tif: TimeInForce,
) -> Order {
    Order {
        id: OrderId(id),
        account,
        side,
        order_type: OrderType::Limit {
            price: price(p),
            post_only: false,
        },
        time_in_force: tif,
        quantity: qty(q),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    }
}

pub(super) fn market_order(id: u64, account: AccountId, side: Side, q: u64) -> Order {
    Order {
        id: OrderId(id),
        account,
        side,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::IOC,
        quantity: qty(q),
        stp: SelfTradeProtection::Allow,
        expiry_ns: 0,
    }
}

/// Mirror what `Application::apply` would do: stash the event timestamp,
/// then dispatch. Direct `Exchange::execute` callers bypass `apply`, so
/// rate-limit (and any other clock-dependent) tests must set the
/// timestamp explicitly.
pub(super) fn execute_at(
    exchange: &mut Exchange,
    now_ns: u64,
    symbol: Symbol,
    order: Order,
    reports: &mut Vec<crate::types::ExecutionReport>,
) {
    exchange.set_current_event_ts_ns(now_ns);
    exchange.execute(symbol, order, reports);
}
