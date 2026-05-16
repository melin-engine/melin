//! Calibration statistics: consume an `ItchEvent` stream filtered to a
//! configured symbol set, drive the book tracker, and accumulate the
//! marginal distributions used to compare the [`crate::generator`]
//! against real venue load.
//!
//! Time-invariant marginals only (this branch's scope — see the
//! generator audit for the deferred temporal + joint metrics):
//!
//! 1. Event-type counts (mix).
//! 2. Side balance over Adds / Deletes / Executes / Cancels.
//! 3. Add-order size distribution.
//! 4. Signed price-distance-from-mid distribution per side.
//! 5. Crossing-add counts per side (price reached the opposite BBO).
//! 6. Partial-cancel share-fraction distribution.
//! 7. Replace price-delta and size-delta distributions.
//!
//! Hidden Trades ('P') are counted but bypass the book tracker — the
//! order was never visible. Stock Directory ('R') messages populate
//! the symbol filter and are otherwise opaque.

use std::collections::{HashMap, HashSet};

use hdrhistogram::Histogram;

use super::Side;
use super::book::{BookTracker, TrackerError};
use super::itch::ItchEvent;

/// Outcome of feeding one event into the aggregator. Returned so the
/// caller (the extractor binary) can log progress and surface error
/// rates without owning the aggregator's internal counters.
#[derive(Debug, Clone, Copy)]
pub enum StatsOutcome {
    /// Event was for an untracked symbol and ignored.
    Filtered,
    /// Event was applied successfully.
    Applied,
    /// Event referenced an unknown order — likely state inherited from
    /// before the parser joined the stream.
    UnknownOrder,
    /// Execute/cancel asked for more shares than the order had.
    ShareUnderflow,
    /// Replace's new ref collided with an existing one.
    NewRefCollision,
}

/// Counts of each ITCH event kind, restricted to tracked symbols.
#[derive(Debug, Default, Clone)]
pub struct EventCounts {
    pub add: u64,
    pub add_attr: u64,
    pub exec: u64,
    pub exec_with_price: u64,
    pub cancel: u64,
    pub delete: u64,
    pub replace: u64,
    pub hidden_trade: u64,
}

/// Buy/sell tallies bucketed by what kind of event produced them. For
/// non-Add events the side is taken from the resting order's state.
#[derive(Debug, Default, Clone)]
pub struct SideBalance {
    pub add_buy: u64,
    pub add_sell: u64,
    pub delete_buy: u64,
    pub delete_sell: u64,
    pub exec_buy: u64,
    pub exec_sell: u64,
    pub cancel_buy: u64,
    pub cancel_sell: u64,
}

/// Two histograms (negative and positive magnitudes) plus a zero
/// counter. hdrhistogram is u64-only; signed quantities are split so
/// each side keeps full precision instead of being shifted into a
/// single uniform-precision histogram.
#[derive(Debug)]
pub struct SignedHist {
    pub negative: Histogram<u64>,
    pub positive: Histogram<u64>,
    pub zero: u64,
}

impl SignedHist {
    /// Constructor matching the precision settings used elsewhere in
    /// the bench (3 significant digits). Range chosen to comfortably
    /// hold ITCH price units (up to ~$10_000 = 100M units) and order
    /// share counts.
    fn new() -> Self {
        Self {
            negative: Histogram::new_with_bounds(1, 1_000_000_000, 3)
                .expect("valid hdrhistogram bounds"),
            positive: Histogram::new_with_bounds(1, 1_000_000_000, 3)
                .expect("valid hdrhistogram bounds"),
            zero: 0,
        }
    }

    fn record(&mut self, value: i64) {
        if value > 0 {
            // hdrhistogram silently saturates on overflow — fine for
            // our purposes (tail clipping at 1B price units).
            self.positive.saturating_record(value as u64);
        } else if value < 0 {
            self.negative.saturating_record((-value) as u64);
        } else {
            self.zero += 1;
        }
    }
}

/// The full set of calibration distributions and counters. Lives in
/// memory during extraction; the fixture exporter (task #6) downsamples
/// and serializes a subset.
#[derive(Debug)]
pub struct CalibrationStats {
    pub event_counts: EventCounts,
    pub side_balance: SideBalance,
    pub add_size: Histogram<u64>,
    pub buy_distance_from_mid: SignedHist,
    pub sell_distance_from_mid: SignedHist,
    /// (cancelled_shares / original_shares) × 1000 as integer ppm-like
    /// units. Range is 1..=1000 because a full cancel uses 'D' not 'X'.
    pub partial_cancel_fraction_per_mille: Histogram<u64>,
    pub replace_price_delta: SignedHist,
    pub replace_size_delta: SignedHist,
    /// Adds that arrived when one side of the book was empty (mid
    /// undefined). Tracked so the distance histograms can be
    /// interpreted relative to the fraction of adds they cover.
    pub adds_without_mid: u64,
    /// Buy adds with price >= current best ask — would have crossed.
    pub crossing_buys: u64,
    /// Sell adds with price <= current best bid — would have crossed.
    pub crossing_sells: u64,
    /// Diagnostics: order-map miss rate. Should stay near zero on
    /// well-formed ITCH for symbols added after our filter activates.
    pub unknown_order_errors: u64,
    pub share_underflow_errors: u64,
    pub new_ref_collision_errors: u64,
}

impl CalibrationStats {
    fn new() -> Self {
        Self {
            event_counts: EventCounts::default(),
            side_balance: SideBalance::default(),
            // Order size: 1..=10M shares covers every plausible
            // institutional block; hdrhistogram saturates beyond.
            add_size: Histogram::new_with_bounds(1, 10_000_000, 3).expect("valid bounds"),
            buy_distance_from_mid: SignedHist::new(),
            sell_distance_from_mid: SignedHist::new(),
            partial_cancel_fraction_per_mille: Histogram::new_with_bounds(1, 1_000, 3)
                .expect("valid bounds"),
            replace_price_delta: SignedHist::new(),
            replace_size_delta: SignedHist::new(),
            adds_without_mid: 0,
            crossing_buys: 0,
            crossing_sells: 0,
            unknown_order_errors: 0,
            share_underflow_errors: 0,
            new_ref_collision_errors: 0,
        }
    }
}

/// Stats aggregator. Owns its book tracker and the symbol filter.
pub struct StatsAggregator {
    book: BookTracker,
    /// Tickers the caller asked us to track. `[u8; 8]` matches ITCH's
    /// space-padded 8-byte stock field.
    target_tickers: HashSet<[u8; 8]>,
    /// stock_locate codes resolved from `R` Stock Directory messages.
    /// `HashMap<u16, bool>` over `HashSet<u16>` would be wasteful;
    /// `HashSet` is what we want here.
    interesting_locates: HashSet<u16>,
    /// Per-ticker stats. Indexing per-ticker lets us spot symbols whose
    /// microstructure differs without
    /// re-running extraction. Keyed by ticker for stable JSON keys in
    /// the fixture.
    per_symbol: HashMap<[u8; 8], CalibrationStats>,
    /// Mapping for hot-path lookups: stock_locate → ticker. Avoids
    /// scanning the ticker set on every event.
    locate_to_ticker: HashMap<u16, [u8; 8]>,
}

impl StatsAggregator {
    pub fn new(target_tickers: impl IntoIterator<Item = [u8; 8]>) -> Self {
        let target_tickers: HashSet<_> = target_tickers.into_iter().collect();
        let mut per_symbol = HashMap::new();
        for t in &target_tickers {
            per_symbol.insert(*t, CalibrationStats::new());
        }
        Self {
            book: BookTracker::new(),
            target_tickers,
            interesting_locates: HashSet::new(),
            per_symbol,
            locate_to_ticker: HashMap::new(),
        }
    }

    pub fn book(&self) -> &BookTracker {
        &self.book
    }

    pub fn stats(&self) -> &HashMap<[u8; 8], CalibrationStats> {
        &self.per_symbol
    }

    /// Drive one event through the aggregator. Returns a coarse
    /// outcome for caller-side logging.
    pub fn apply(&mut self, event: &ItchEvent) -> StatsOutcome {
        // Stock Directory always processed: it builds the filter.
        if let ItchEvent::StockDirectory {
            stock_locate,
            stock,
        } = event
        {
            if self.target_tickers.contains(stock) {
                self.interesting_locates.insert(*stock_locate);
                self.locate_to_ticker.insert(*stock_locate, *stock);
            }
            return StatsOutcome::Applied;
        }

        let stock_locate = match event {
            ItchEvent::AddOrder { stock_locate, .. }
            | ItchEvent::AddOrderAttributed { stock_locate, .. }
            | ItchEvent::OrderExecuted { stock_locate, .. }
            | ItchEvent::OrderExecutedWithPrice { stock_locate, .. }
            | ItchEvent::OrderCancel { stock_locate, .. }
            | ItchEvent::OrderDelete { stock_locate, .. }
            | ItchEvent::OrderReplace { stock_locate, .. }
            | ItchEvent::HiddenTrade { stock_locate, .. } => *stock_locate,
            ItchEvent::StockDirectory { .. } => unreachable!("handled above"),
        };

        if !self.interesting_locates.contains(&stock_locate) {
            return StatsOutcome::Filtered;
        }
        // Safe: we only insert into `locate_to_ticker` alongside
        // `interesting_locates`, so presence in the set implies a key
        // in the map.
        let ticker = *self
            .locate_to_ticker
            .get(&stock_locate)
            .expect("ticker resolved for filtered locate");
        // Same: we initialized `per_symbol` for every target ticker.
        let stats = self
            .per_symbol
            .get_mut(&ticker)
            .expect("per-symbol stats present");
        process(stats, &mut self.book, event)
    }
}

fn process(
    stats: &mut CalibrationStats,
    book: &mut BookTracker,
    event: &ItchEvent,
) -> StatsOutcome {
    match *event {
        ItchEvent::StockDirectory { .. } => StatsOutcome::Applied,
        ItchEvent::AddOrder {
            stock_locate,
            order_ref,
            side,
            shares,
            price,
            ..
        }
        | ItchEvent::AddOrderAttributed {
            stock_locate,
            order_ref,
            side,
            shares,
            price,
            ..
        } => {
            let attributed = matches!(event, ItchEvent::AddOrderAttributed { .. });
            record_add(
                stats,
                book,
                stock_locate,
                order_ref,
                side,
                shares,
                price,
                attributed,
            )
        }
        ItchEvent::OrderExecuted {
            order_ref, shares, ..
        }
        | ItchEvent::OrderExecutedWithPrice {
            order_ref, shares, ..
        } => match book.execute(order_ref, shares) {
            Ok(state) => {
                count_side(&mut stats.side_balance, state.side, SideEvent::Exec);
                if matches!(event, ItchEvent::OrderExecutedWithPrice { .. }) {
                    stats.event_counts.exec_with_price += 1;
                } else {
                    stats.event_counts.exec += 1;
                }
                StatsOutcome::Applied
            }
            Err(e) => classify_err(stats, e),
        },
        ItchEvent::OrderCancel {
            order_ref,
            cancelled_shares,
            ..
        } => {
            let prior = match book.get(order_ref) {
                Some(s) => *s,
                None => {
                    stats.unknown_order_errors += 1;
                    return StatsOutcome::UnknownOrder;
                }
            };
            match book.cancel_partial(order_ref, cancelled_shares) {
                Ok(_) => {
                    let frac_ppm = (cancelled_shares as u64 * 1000) / prior.original_shares as u64;
                    let frac_clamped = frac_ppm.clamp(1, 1000);
                    // saturating_record is no-op above range; clamp keeps
                    // us inside the configured 1..=1000 bounds.
                    stats
                        .partial_cancel_fraction_per_mille
                        .saturating_record(frac_clamped);
                    count_side(&mut stats.side_balance, prior.side, SideEvent::Cancel);
                    stats.event_counts.cancel += 1;
                    StatsOutcome::Applied
                }
                Err(e) => classify_err(stats, e),
            }
        }
        ItchEvent::OrderDelete { order_ref, .. } => match book.delete(order_ref) {
            Ok(state) => {
                count_side(&mut stats.side_balance, state.side, SideEvent::Delete);
                stats.event_counts.delete += 1;
                StatsOutcome::Applied
            }
            Err(e) => classify_err(stats, e),
        },
        ItchEvent::OrderReplace {
            old_order_ref,
            new_order_ref,
            shares,
            price,
            ..
        } => {
            // Capture the prior state before mutation so we can compute
            // signed deltas; `replace` returns it but at this point we
            // also need it for delta math regardless.
            match book.replace(old_order_ref, new_order_ref, price, shares) {
                Ok(prior) => {
                    let price_delta = price as i64 - prior.price as i64;
                    let size_delta = shares as i64 - prior.original_shares as i64;
                    stats.replace_price_delta.record(price_delta);
                    stats.replace_size_delta.record(size_delta);
                    stats.event_counts.replace += 1;
                    StatsOutcome::Applied
                }
                Err(e) => classify_err(stats, e),
            }
        }
        ItchEvent::HiddenTrade { .. } => {
            stats.event_counts.hidden_trade += 1;
            StatsOutcome::Applied
        }
    }
}

fn record_add(
    stats: &mut CalibrationStats,
    book: &mut BookTracker,
    stock_locate: u16,
    order_ref: u64,
    side: Side,
    shares: u32,
    price: u32,
    attributed: bool,
) -> StatsOutcome {
    // Capture BBO BEFORE the add applies. After the add, the new order
    // could itself become the best, which would zero our distance.
    let best_bid = book.best_bid(stock_locate);
    let best_ask = book.best_ask(stock_locate);

    // Gate every per-add stat on `book.add()` succeeding — otherwise a
    // duplicate order_ref (NewRefAlreadyExists) gets counted both as a
    // successful add (size histogram, distance-from-mid, side balance,
    // event_counts) and as an error via `classify_err`, double-attributing
    // it and skewing the marginals the calibration is trying to measure.
    // Symmetric with the Cancel/Delete/Replace/Exec handlers above, which
    // all gate their bookkeeping on the Ok branch.
    match book.add(order_ref, stock_locate, side, price, shares) {
        Ok(()) => {
            match (best_bid, best_ask) {
                (Some(bb), Some(ba)) => {
                    // Integer mid: tick precision is plenty for distance
                    // stats, and f64 here would only hide rounding.
                    let mid = ((bb as u64 + ba as u64) / 2) as u32;
                    let signed = price as i64 - mid as i64;
                    match side {
                        Side::Buy => {
                            stats.buy_distance_from_mid.record(signed);
                            if price >= ba {
                                stats.crossing_buys += 1;
                            }
                        }
                        Side::Sell => {
                            stats.sell_distance_from_mid.record(signed);
                            if price <= bb {
                                stats.crossing_sells += 1;
                            }
                        }
                    }
                }
                _ => stats.adds_without_mid += 1,
            }

            stats.add_size.saturating_record(shares as u64);
            count_side(&mut stats.side_balance, side, SideEvent::Add);
            if attributed {
                stats.event_counts.add_attr += 1;
            } else {
                stats.event_counts.add += 1;
            }
            StatsOutcome::Applied
        }
        Err(e) => classify_err(stats, e),
    }
}

enum SideEvent {
    Add,
    Delete,
    Exec,
    Cancel,
}

fn count_side(sb: &mut SideBalance, side: Side, kind: SideEvent) {
    match (side, kind) {
        (Side::Buy, SideEvent::Add) => sb.add_buy += 1,
        (Side::Sell, SideEvent::Add) => sb.add_sell += 1,
        (Side::Buy, SideEvent::Delete) => sb.delete_buy += 1,
        (Side::Sell, SideEvent::Delete) => sb.delete_sell += 1,
        (Side::Buy, SideEvent::Exec) => sb.exec_buy += 1,
        (Side::Sell, SideEvent::Exec) => sb.exec_sell += 1,
        (Side::Buy, SideEvent::Cancel) => sb.cancel_buy += 1,
        (Side::Sell, SideEvent::Cancel) => sb.cancel_sell += 1,
    }
}

fn classify_err(stats: &mut CalibrationStats, e: TrackerError) -> StatsOutcome {
    match e {
        TrackerError::UnknownOrder { .. } => {
            stats.unknown_order_errors += 1;
            StatsOutcome::UnknownOrder
        }
        TrackerError::ShareUnderflow { .. } => {
            stats.share_underflow_errors += 1;
            StatsOutcome::ShareUnderflow
        }
        TrackerError::NewRefAlreadyExists { .. } => {
            stats.new_ref_collision_errors += 1;
            StatsOutcome::NewRefCollision
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &[u8]) -> [u8; 8] {
        let mut out = *b"        ";
        for (i, b) in s.iter().take(8).enumerate() {
            out[i] = *b;
        }
        out
    }

    #[test]
    fn filters_untracked_symbol() {
        let mut agg = StatsAggregator::new([t(b"TEST1")]);
        // Directory for our test ticker: locate 1.
        let r = ItchEvent::StockDirectory {
            stock_locate: 1,
            stock: t(b"TEST1"),
        };
        assert!(matches!(agg.apply(&r), StatsOutcome::Applied));
        // Add for tracked ticker — kept.
        let add_aapl = ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 1,
            side: Side::Buy,
            shares: 100,
            stock: t(b"TEST1"),
            price: 1_000_000,
        };
        assert!(matches!(agg.apply(&add_aapl), StatsOutcome::Applied));
        // Add for an unknown locate — filtered.
        let add_other = ItchEvent::AddOrder {
            stock_locate: 99,
            order_ref: 2,
            side: Side::Buy,
            shares: 100,
            stock: t(b"MSFT"),
            price: 2_000_000,
        };
        assert!(matches!(agg.apply(&add_other), StatsOutcome::Filtered));
        let stats = agg.stats().get(&t(b"TEST1")).unwrap();
        assert_eq!(stats.event_counts.add, 1);
    }

    #[test]
    fn distance_from_mid_recorded_for_passive_buy() {
        let mut agg = StatsAggregator::new([t(b"TEST1")]);
        agg.apply(&ItchEvent::StockDirectory {
            stock_locate: 1,
            stock: t(b"TEST1"),
        });
        // Seed a bid at 9900 and an ask at 10100 — mid = 10000.
        agg.apply(&ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 1,
            side: Side::Buy,
            shares: 100,
            stock: t(b"TEST1"),
            price: 9_900,
        });
        agg.apply(&ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 2,
            side: Side::Sell,
            shares: 100,
            stock: t(b"TEST1"),
            price: 10_100,
        });
        // Now a passive buy at 9_800 (200 below mid).
        agg.apply(&ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 3,
            side: Side::Buy,
            shares: 50,
            stock: t(b"TEST1"),
            price: 9_800,
        });
        let stats = agg.stats().get(&t(b"TEST1")).unwrap();
        assert_eq!(stats.buy_distance_from_mid.negative.len(), 1);
        // The first two adds (9900, 10100) arrived without a mid since
        // the opposite side was empty when they came in — counted as
        // adds_without_mid.
        assert_eq!(stats.adds_without_mid, 2);
    }

    #[test]
    fn crossing_buy_detected() {
        let mut agg = StatsAggregator::new([t(b"TEST1")]);
        agg.apply(&ItchEvent::StockDirectory {
            stock_locate: 1,
            stock: t(b"TEST1"),
        });
        // Seed bid + ask.
        agg.apply(&ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 1,
            side: Side::Buy,
            shares: 100,
            stock: t(b"TEST1"),
            price: 9_900,
        });
        agg.apply(&ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 2,
            side: Side::Sell,
            shares: 100,
            stock: t(b"TEST1"),
            price: 10_100,
        });
        // Aggressive buy at 10_200 (>= best_ask 10_100): crosses.
        agg.apply(&ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 3,
            side: Side::Buy,
            shares: 25,
            stock: t(b"TEST1"),
            price: 10_200,
        });
        let stats = agg.stats().get(&t(b"TEST1")).unwrap();
        assert_eq!(stats.crossing_buys, 1);
    }

    #[test]
    fn partial_cancel_fraction_recorded() {
        let mut agg = StatsAggregator::new([t(b"TEST1")]);
        agg.apply(&ItchEvent::StockDirectory {
            stock_locate: 1,
            stock: t(b"TEST1"),
        });
        agg.apply(&ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 1,
            side: Side::Buy,
            shares: 1000,
            stock: t(b"TEST1"),
            price: 9_900,
        });
        // Cancel half — 500/1000 = 0.5 → 500 per-mille.
        agg.apply(&ItchEvent::OrderCancel {
            stock_locate: 1,
            order_ref: 1,
            cancelled_shares: 500,
        });
        let stats = agg.stats().get(&t(b"TEST1")).unwrap();
        assert_eq!(stats.partial_cancel_fraction_per_mille.len(), 1);
        // hdrhistogram min should be near 500.
        let val = stats.partial_cancel_fraction_per_mille.min();
        assert!((490..=510).contains(&val), "got fraction {val}");
    }

    #[test]
    fn replace_records_signed_deltas() {
        let mut agg = StatsAggregator::new([t(b"TEST1")]);
        agg.apply(&ItchEvent::StockDirectory {
            stock_locate: 1,
            stock: t(b"TEST1"),
        });
        agg.apply(&ItchEvent::AddOrder {
            stock_locate: 1,
            order_ref: 1,
            side: Side::Buy,
            shares: 100,
            stock: t(b"TEST1"),
            price: 9_900,
        });
        // Replace at higher price, smaller size.
        agg.apply(&ItchEvent::OrderReplace {
            stock_locate: 1,
            old_order_ref: 1,
            new_order_ref: 2,
            shares: 80,
            price: 9_950,
        });
        let stats = agg.stats().get(&t(b"TEST1")).unwrap();
        assert_eq!(stats.replace_price_delta.positive.len(), 1);
        assert_eq!(stats.replace_size_delta.negative.len(), 1);
    }
}
