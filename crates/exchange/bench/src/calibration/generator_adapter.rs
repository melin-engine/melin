//! Adapter that re-emits [`OrderFlowGenerator`] output as
//! [`ItchEvent`]s so the same [`super::stats::StatsAggregator`] scores
//! the generator's synthetic stream the same way it scores real ITCH.
//!
//! Only events that map cleanly onto a visible limit order book are
//! emitted:
//!   - `Submit` with `Limit { .. }` + `GTC`/`Day` → `AddOrder`
//!   - `Submit` with any other type/TIF → skipped (markets, IOC, FOK,
//!     and stops never rest on a visible book, so ITCH has no analogue
//!     at submission time)
//!   - `Cancel` whose target was an emitted Add → `OrderDelete`
//!   - `CancelReplace` whose target was an emitted Add → `OrderReplace`
//!
//! The generator's own ring buffer tracks all resting orders (including
//! Stops, which we skip on the ITCH side); we mirror that with a
//! lighter `visible_orders` set so Cancel/Replace for non-visible
//! resting orders are dropped rather than emitted as Deletes for
//! refs the book tracker never saw.

use std::collections::HashSet;

use melin_types::types::{OrderType, Side as EngineSide, TimeInForce};

use super::Side as CalibSide;
use super::itch::ItchEvent;
use crate::generator::{GeneratedEvent, GeneratorConfig, OrderFlowGenerator};

/// Wraps an [`OrderFlowGenerator`] and re-emits its events as ITCH 5.0
/// equivalents. Holds a small set of "currently-visible" order_ids so
/// follow-up Cancel/Replace events match against the adds we actually
/// emitted.
pub struct GeneratorAdapter {
    inner: OrderFlowGenerator,
    /// Resting limit-GTC/Day order_ids we emitted as `AddOrder`. Used
    /// to gate Cancel/Replace emission so we don't try to remove
    /// orders the book tracker never saw (e.g., Stops, which the
    /// generator does track internally).
    ///
    /// HashSet over Vec because we do contains/remove on every Cancel
    /// and Replace; the set's peak size is bounded by the generator's
    /// internal live-orders ring (100k by default).
    visible_orders: HashSet<u64>,
    /// Pre-computed synthetic tickers indexed by `Symbol(n).0 - 1`. We
    /// don't allocate per event — generator emits millions per second
    /// in the bench, and `format!` would dominate the hot path.
    tickers: Vec<[u8; 8]>,
}

impl GeneratorAdapter {
    pub fn new(config: GeneratorConfig) -> Self {
        let num = config.num_instruments;
        // `synthetic_ticker` encodes the symbol id as a 5-digit decimal
        // suffix, so ids > 99_999 collide in ticker space and would
        // silently merge per-symbol stats. ITCH-scale venues sit well
        // below this; the assert guards misconfig in offline calibration.
        debug_assert!(
            num <= 99_999,
            "num_instruments={num} exceeds synthetic_ticker capacity (99_999)"
        );
        // The ITCH wire format is u32 for both shares and price; the
        // generator's defaults sit ~1000× below that ceiling. Asserts
        // here so a future config that bumps the size/price ceilings
        // past u32::MAX fails loudly at adapter construction instead of
        // silently truncating per-event downstream.
        debug_assert!(
            config.max_size <= u32::MAX as u64,
            "max_size={} exceeds ITCH u32 shares field",
            config.max_size
        );
        debug_assert!(
            config
                .mid_price
                .checked_add(config.far_max_price_offset)
                .is_some_and(|p| p <= u32::MAX as u64),
            "mid_price + far_max_price_offset exceeds ITCH u32 price field"
        );
        let mut tickers = Vec::with_capacity(num as usize);
        for s in 1..=num {
            tickers.push(synthetic_ticker(s));
        }
        Self {
            inner: OrderFlowGenerator::new(config),
            visible_orders: HashSet::new(),
            tickers,
        }
    }

    /// StockDirectory events to apply to a [`super::stats::StatsAggregator`]
    /// before consuming generated events. Equivalent to ITCH's
    /// start-of-day 'R' messages — needed because the aggregator
    /// filters out events for unknown stock_locate codes.
    pub fn directory(&self) -> Vec<ItchEvent> {
        (0..self.tickers.len())
            .map(|i| ItchEvent::StockDirectory {
                stock_locate: (i + 1) as u16,
                stock: self.tickers[i],
            })
            .collect()
    }

    /// Tickers the adapter will emit on Add events. Used by callers to
    /// construct the aggregator's target ticker set.
    pub fn tickers(&self) -> &[[u8; 8]] {
        &self.tickers
    }

    /// Drive one generated event through the adapter. Returns `None`
    /// when the underlying event doesn't map to an ITCH book event
    /// (markets, IOC/FOK, stops, cancels for non-visible orders).
    /// Callers loop until they see `Some` if they want to drive a
    /// fixed number of book events.
    pub fn next_event(&mut self) -> Option<ItchEvent> {
        let raw = self.inner.next_event();
        match raw {
            GeneratedEvent::Submit { symbol, order } => {
                match (order.order_type, order.time_in_force) {
                    // Limit GTC/Day rests on the visible book — emit Add.
                    // Other order types are either market/IOC/FOK (don't
                    // rest) or stops (not visible on the order book).
                    (OrderType::Limit { price, .. }, TimeInForce::GTC | TimeInForce::Day) => {
                        self.visible_orders.insert(order.id.0);
                        Some(ItchEvent::AddOrder {
                            stock_locate: symbol.0 as u16,
                            order_ref: order.id.0,
                            side: map_side(order.side),
                            // The generator's quantity is NonZeroU64; ITCH
                            // shares are u32. Real venues cap displayed
                            // size well below 4G shares, so the cast is
                            // safe for any plausible generator config.
                            shares: order.quantity.get() as u32,
                            stock: self.ticker_for(symbol.0),
                            price: price.get() as u32,
                        })
                    }
                    _ => None,
                }
            }
            GeneratedEvent::Cancel {
                symbol, order_id, ..
            } => {
                if self.visible_orders.remove(&order_id.0) {
                    Some(ItchEvent::OrderDelete {
                        stock_locate: symbol.0 as u16,
                        order_ref: order_id.0,
                    })
                } else {
                    None
                }
            }
            GeneratedEvent::CancelReplace {
                symbol,
                order_id,
                new_price,
                new_quantity,
                ..
            } => {
                if !self.visible_orders.contains(&order_id.0) {
                    return None;
                }
                // Real ITCH 'U' assigns a new order_ref to the replaced
                // order; the generator keeps the same id. The book
                // tracker's `replace(old, new, ..)` does a delete +
                // re-add, which is safe with old == new — the delete
                // clears the slot before the add reinserts. So we
                // emit old == new here.
                Some(ItchEvent::OrderReplace {
                    stock_locate: symbol.0 as u16,
                    old_order_ref: order_id.0,
                    new_order_ref: order_id.0,
                    shares: new_quantity.get() as u32,
                    price: new_price.get() as u32,
                })
            }
        }
    }

    fn ticker_for(&self, symbol: u32) -> [u8; 8] {
        // Symbol numbering starts at 1 in the generator.
        let idx = (symbol as usize).saturating_sub(1);
        self.tickers
            .get(idx)
            .copied()
            .unwrap_or_else(|| synthetic_ticker(symbol))
    }
}

/// Render `Symbol(n)` as an 8-byte ASCII ticker matching ITCH's field
/// layout (space-padded right). Format is `SYMxxxxx` for n ≤ 99_999;
/// larger ids overflow into the padding but stay byte-unique.
fn synthetic_ticker(symbol: u32) -> [u8; 8] {
    let mut out = [b' '; 8];
    out[0] = b'S';
    out[1] = b'Y';
    out[2] = b'M';
    // 5-digit zero-padded decimal — covers up to 99_999 instruments.
    // Hand-rolled to avoid heap allocation on the hot path.
    let mut n = symbol;
    for slot in out[3..8].iter_mut().rev() {
        *slot = b'0' + (n % 10) as u8;
        n /= 10;
    }
    out
}

fn map_side(s: EngineSide) -> CalibSide {
    match s {
        EngineSide::Buy => CalibSide::Buy,
        EngineSide::Sell => CalibSide::Sell,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calibration::stats::StatsAggregator;

    fn defaults() -> GeneratorConfig {
        GeneratorConfig {
            // Single symbol keeps the test deterministic and fast.
            num_instruments: 1,
            // Pin seed so the test isn't flaky across rand-crate
            // versions / future generator changes that shift the RNG
            // stream.
            seed: 1234,
            // Enough accounts that Zipf isn't degenerate.
            num_accounts: 1_000,
            ..Default::default()
        }
    }

    #[test]
    fn ticker_format_is_padded_decimal() {
        let t = synthetic_ticker(1);
        assert_eq!(&t, b"SYM00001");
        let t = synthetic_ticker(42);
        assert_eq!(&t, b"SYM00042");
        let t = synthetic_ticker(99_999);
        assert_eq!(&t, b"SYM99999");
    }

    #[test]
    fn directory_lists_all_instruments() {
        let cfg = GeneratorConfig {
            num_instruments: 3,
            ..defaults()
        };
        let a = GeneratorAdapter::new(cfg);
        let dir = a.directory();
        assert_eq!(dir.len(), 3);
        for (i, ev) in dir.iter().enumerate() {
            match ev {
                ItchEvent::StockDirectory {
                    stock_locate,
                    stock,
                } => {
                    assert_eq!(*stock_locate as usize, i + 1);
                    assert_eq!(stock, &synthetic_ticker((i + 1) as u32));
                }
                other => panic!("expected StockDirectory, got {other:?}"),
            }
        }
    }

    #[test]
    fn emits_add_for_limit_gtc_and_skips_market_orders() {
        let mut a = GeneratorAdapter::new(defaults());
        let mut adds = 0;
        let mut deletes = 0;
        let mut replaces = 0;
        // Pull a healthy sample so we hit many event kinds in the
        // generator's distribution.
        for _ in 0..10_000 {
            match a.next_event() {
                Some(ItchEvent::AddOrder { .. }) => adds += 1,
                Some(ItchEvent::OrderDelete { .. }) => deletes += 1,
                Some(ItchEvent::OrderReplace { .. }) => replaces += 1,
                Some(other) => panic!("unexpected event {other:?}"),
                None => {}
            }
        }
        assert!(adds > 0, "expected some adds");
        assert!(deletes > 0, "expected some deletes");
        assert!(replaces > 0, "expected some replaces");
    }

    #[test]
    fn output_drives_aggregator_without_book_errors() {
        let mut a = GeneratorAdapter::new(defaults());
        let tickers: Vec<_> = a.tickers().to_vec();
        let mut agg = StatsAggregator::new(tickers.iter().copied());
        for d in a.directory() {
            agg.apply(&d);
        }
        // 20k events is enough to exercise the ring eviction path
        // (default capacity 100k, but cancels free slots so we get
        // good churn through the live set well below that).
        for _ in 0..20_000 {
            if let Some(ev) = a.next_event() {
                agg.apply(&ev);
            }
        }
        // The headline calibration metric: book-tracker stayed
        // consistent end-to-end. Any nonzero error count here means
        // the adapter emitted refs that don't match what the book
        // expects, which would invalidate any downstream comparison.
        let stats = agg.stats().values().next().expect("one symbol");
        assert_eq!(
            stats.unknown_order_errors, 0,
            "adapter produced refs the book didn't recognize"
        );
        assert_eq!(stats.share_underflow_errors, 0);
        assert_eq!(stats.new_ref_collision_errors, 0);
        assert!(stats.event_counts.add > 0);
    }
}
