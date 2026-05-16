//! Serializable summary of [`super::stats::CalibrationStats`] for a
//! reference fixture file.
//!
//! Distributions are reduced to a fixed set of quantiles plus summary
//! scalars (count/min/max/mean) — dense enough to drive KS-style
//! comparison in the calibration tests, without carrying per-event
//! records or raw histogram buckets.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use super::stats::{CalibrationStats, EventCounts, SideBalance, SignedHist};

/// Quantile points we extract from every distribution. Chosen to cover
/// the body (median ± 1σ-ish) and both tails — calibration is mostly
/// about tail behavior. Bare-`f64` instead of `&str` keys because that
/// is what hdrhistogram's `value_at_quantile` takes and what KS tests
/// consume; the JSON renders them as numeric strings anyway.
const FIXTURE_QUANTILES: &[f64] = &[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999];

/// One distribution's summary. `count == 0` means the distribution had
/// no observations; quantiles will all be zero and should not be used
/// for comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionSummary {
    pub count: u64,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    /// Quantile → value at that quantile, indexed by [`FIXTURE_QUANTILES`].
    pub quantiles: Vec<QuantilePoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantilePoint {
    pub q: f64,
    pub value: u64,
}

impl DistributionSummary {
    fn from_hist(h: &Histogram<u64>) -> Self {
        if h.is_empty() {
            return Self {
                count: 0,
                min: 0,
                max: 0,
                mean: 0.0,
                quantiles: FIXTURE_QUANTILES
                    .iter()
                    .map(|&q| QuantilePoint { q, value: 0 })
                    .collect(),
            };
        }
        Self {
            count: h.len(),
            min: h.min(),
            max: h.max(),
            mean: h.mean(),
            quantiles: FIXTURE_QUANTILES
                .iter()
                .map(|&q| QuantilePoint {
                    q,
                    value: h.value_at_quantile(q),
                })
                .collect(),
        }
    }
}

/// Signed distribution: one summary for the negative magnitudes, one
/// for the positive magnitudes, and a zero count. Lets KS tests on
/// either side compare separately so a generator that's correct on one
/// side but wrong on the other shows up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedSummary {
    pub negative: DistributionSummary,
    pub positive: DistributionSummary,
    pub zero: u64,
}

impl SignedSummary {
    fn from_signed(sh: &SignedHist) -> Self {
        Self {
            negative: DistributionSummary::from_hist(&sh.negative),
            positive: DistributionSummary::from_hist(&sh.positive),
            zero: sh.zero,
        }
    }
}

/// Per-symbol fixture entry. Mirrors [`CalibrationStats`] but with
/// histograms reduced to summaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolFixture {
    pub event_counts: EventCountsOwned,
    pub side_balance: SideBalanceOwned,
    pub add_size: DistributionSummary,
    pub buy_distance_from_mid: SignedSummary,
    pub sell_distance_from_mid: SignedSummary,
    pub partial_cancel_fraction_per_mille: DistributionSummary,
    pub replace_price_delta: SignedSummary,
    pub replace_size_delta: SignedSummary,
    pub adds_without_mid: u64,
    pub crossing_buys: u64,
    pub crossing_sells: u64,
    pub unknown_order_errors: u64,
    pub share_underflow_errors: u64,
    pub new_ref_collision_errors: u64,
}

/// Owned serialization-friendly mirror of [`EventCounts`]. The stats
/// module's struct is Serialize-derive-able directly but living here
/// keeps the fixture schema independent of internal stats changes —
/// reordering fields in `EventCounts` shouldn't silently invalidate
/// every committed fixture.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCountsOwned {
    pub add: u64,
    pub add_attr: u64,
    pub exec: u64,
    pub exec_with_price: u64,
    pub cancel: u64,
    pub delete: u64,
    pub replace: u64,
    pub hidden_trade: u64,
}

impl From<&EventCounts> for EventCountsOwned {
    fn from(c: &EventCounts) -> Self {
        Self {
            add: c.add,
            add_attr: c.add_attr,
            exec: c.exec,
            exec_with_price: c.exec_with_price,
            cancel: c.cancel,
            delete: c.delete,
            replace: c.replace,
            hidden_trade: c.hidden_trade,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SideBalanceOwned {
    pub add_buy: u64,
    pub add_sell: u64,
    pub delete_buy: u64,
    pub delete_sell: u64,
    pub exec_buy: u64,
    pub exec_sell: u64,
    pub cancel_buy: u64,
    pub cancel_sell: u64,
}

impl From<&SideBalance> for SideBalanceOwned {
    fn from(s: &SideBalance) -> Self {
        Self {
            add_buy: s.add_buy,
            add_sell: s.add_sell,
            delete_buy: s.delete_buy,
            delete_sell: s.delete_sell,
            exec_buy: s.exec_buy,
            exec_sell: s.exec_sell,
            cancel_buy: s.cancel_buy,
            cancel_sell: s.cancel_sell,
        }
    }
}

impl SymbolFixture {
    pub fn from_stats(s: &CalibrationStats) -> Self {
        Self {
            event_counts: (&s.event_counts).into(),
            side_balance: (&s.side_balance).into(),
            add_size: DistributionSummary::from_hist(&s.add_size),
            buy_distance_from_mid: SignedSummary::from_signed(&s.buy_distance_from_mid),
            sell_distance_from_mid: SignedSummary::from_signed(&s.sell_distance_from_mid),
            partial_cancel_fraction_per_mille: DistributionSummary::from_hist(
                &s.partial_cancel_fraction_per_mille,
            ),
            replace_price_delta: SignedSummary::from_signed(&s.replace_price_delta),
            replace_size_delta: SignedSummary::from_signed(&s.replace_size_delta),
            adds_without_mid: s.adds_without_mid,
            crossing_buys: s.crossing_buys,
            crossing_sells: s.crossing_sells,
            unknown_order_errors: s.unknown_order_errors,
            share_underflow_errors: s.share_underflow_errors,
            new_ref_collision_errors: s.new_ref_collision_errors,
        }
    }
}

/// Top-level fixture file structure. The metadata block records what
/// the fixture was derived from so future maintainers can re-extract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceFixture {
    pub metadata: FixtureMetadata,
    /// Keyed by ticker (8-byte ASCII, trailing spaces trimmed for JSON
    /// readability).
    pub symbols: std::collections::BTreeMap<String, SymbolFixture>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureMetadata {
    pub source: String,
    pub date: String,
    pub attribution: String,
    pub generated_by: String,
    pub schema_version: u32,
}

impl FixtureMetadata {
    pub fn new(source: &str, date: &str, attribution: &str) -> Self {
        Self {
            source: source.to_string(),
            date: date.to_string(),
            attribution: attribution.to_string(),
            generated_by: "melin-bench/examples/extract_itch_stats.rs".to_string(),
            schema_version: 1,
        }
    }
}

/// Trim ITCH's space-padded 8-byte ticker for use as a JSON key.
pub fn ticker_key(stock: &[u8; 8]) -> String {
    // Diagnostic JSON key only — a non-UTF-8 ticker would never appear in
    // a valid ITCH 5.0 'R' message (the field is ASCII alphanumeric +
    // space), but if a corrupt dump ever surfaced one we'd rather emit
    // "?" into the report than abort the calibration run.
    let s = std::str::from_utf8(stock).unwrap_or("?");
    s.trim_end().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calibration::Side;
    use crate::calibration::itch::ItchEvent;
    use crate::calibration::stats::StatsAggregator;

    fn t(s: &[u8]) -> [u8; 8] {
        let mut out = *b"        ";
        for (i, b) in s.iter().take(8).enumerate() {
            out[i] = *b;
        }
        out
    }

    #[test]
    fn empty_hist_serializes_cleanly() {
        let h: Histogram<u64> = Histogram::new_with_bounds(1, 1_000, 3).unwrap();
        let s = DistributionSummary::from_hist(&h);
        assert_eq!(s.count, 0);
        assert_eq!(s.min, 0);
        assert_eq!(s.max, 0);
        assert!(s.quantiles.iter().all(|q| q.value == 0));
    }

    #[test]
    fn populated_hist_quantiles_make_sense() {
        let mut h: Histogram<u64> = Histogram::new_with_bounds(1, 10_000, 3).unwrap();
        for v in 1..=100 {
            h.saturating_record(v);
        }
        let s = DistributionSummary::from_hist(&h);
        assert_eq!(s.count, 100);
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 100);
        let p50 = s.quantiles.iter().find(|q| q.q == 0.5).unwrap().value;
        assert!((48..=52).contains(&p50), "p50 should be near 50, got {p50}");
        let p99 = s.quantiles.iter().find(|q| q.q == 0.99).unwrap().value;
        assert!(
            (97..=100).contains(&p99),
            "p99 should be near 100, got {p99}"
        );
    }

    #[test]
    fn roundtrip_through_json() {
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
        let stats = agg.stats().get(&t(b"TEST1")).unwrap();
        let fixture = SymbolFixture::from_stats(stats);
        let json = serde_json::to_string(&fixture).expect("serialize");
        let _parsed: SymbolFixture = serde_json::from_str(&json).expect("deserialize");
    }

    #[test]
    fn ticker_key_trims_padding() {
        assert_eq!(ticker_key(b"TEST1   "), "TEST1");
        assert_eq!(ticker_key(b"TEST3   "), "TEST3");
        assert_eq!(ticker_key(b"TEST2   "), "TEST2");
    }
}
