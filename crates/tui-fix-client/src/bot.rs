//! Pure helpers for the synthetic order-flow bot.
//!
//! Isolated from `run_bot_session` so the price curve, RNG, order
//! parameter sampling, and FIX message construction can be unit tested
//! without opening a real gateway connection.

use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;

// --- Price model ---

/// Sine-wave period for the midprice walk. 10 s is short enough that a
/// full cycle is visible during a casual demo.
pub(crate) const PERIOD_SECS: f64 = 10.0;
/// Midprice base (FIX decimal). The sinusoid oscillates around this.
pub(crate) const MID_BASE: f64 = 100.0;
/// Peak-to-mean amplitude of the midprice in FIX decimal units. With
/// `MID_BASE = 100` and `MID_AMP = 5` the mid walks 95 → 105 over one
/// cycle. The amplitude is deliberately ~17× the per-side spread
/// (`MAX_OFFSET_TICKS = 3` × 0.1 = 0.3) so the wave is clearly visible
/// in the book panel and isn't swamped by per-order noise.
pub(crate) const MID_AMP: f64 = 5.0;
/// Constant submission rate (orders/sec). The book's *visual* sine
/// comes from the mid moving, not the rate, so a flat rate is the
/// right shape here. ~30/s is leisurely enough to keep the engine
/// idle and frequent enough to populate the moving cluster densely.
pub(crate) const BOT_RATE: f64 = 30.0;

/// Midprice (FIX decimal) `t` seconds after bot start, snapped to a
/// 0.1 grid for visually clean prices in the book panel. The engine's
/// underlying tick is 0.01 (`tick_size_inverse = 100`), so 0.1-aligned
/// values are still on-grid; we just expose a coarser visible
/// resolution. Snapping here keeps `next_bot_order`'s integer-tick
/// offsets producing grid-aligned final prices regardless of where on
/// the sine we are.
pub(crate) fn bot_mid_price(t: f64) -> f64 {
    let raw = MID_BASE + MID_AMP * (std::f64::consts::TAU * t / PERIOD_SECS).sin();
    (raw * 10.0).round() / 10.0
}

// --- RNG ---

/// xorshift64: ~1 ns/sample, single-u64 state, non-cryptographic —
/// adequate for bot order-parameter jitter.
pub(crate) fn xs64(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

// --- Order parameter sampling ---

/// Account pool: 2..=32. Fixed-size array (not Vec) since the pool is
/// known at compile time — avoids a heap allocation per thread start.
/// Accounts start at 2 to leave account 1 for the interactive trader,
/// so the user's balances/active-orders panels aren't polluted.
pub(crate) const BOT_ACCOUNTS: [u32; 31] = {
    let mut a = [0u32; 31];
    let mut i = 0;
    while i < 31 {
        a[i] = (i as u32) + 2;
        i += 1;
    }
    a
};
/// Matches the FIX symbols configured in the OE gateway.
pub(crate) const BOT_SYMBOLS: [&str; 2] = ["BTC/USD", "ETH/USD"];
/// Bot offset granularity is 0.1 (one decimal). Offsets are drawn
/// uniformly in [1, 3] of these 0.1-units, giving a [0.1, 0.3] spread
/// each side of the mid (max book-wide spread = 0.6). Tight enough to
/// keep the cluster around the moving mid dense, wide enough to leave
/// room for the user's manual orders to sit between.
pub(crate) const MAX_OFFSET_TICKS: u64 = 3;
/// Max order quantity in lots. Quantities are drawn uniformly in [1, 50].
pub(crate) const MAX_QTY: u64 = 50;

/// Parameters for a single synthetic order.
pub(crate) struct BotOrder {
    pub account_id: u32,
    pub symbol: &'static str,
    /// FIX 4.4: "1" = BUY, "2" = SELL.
    pub side_code: &'static str,
    pub price: f64,
    pub qty: u64,
}

/// Draw the next order's parameters from the RNG state at wall-time
/// `t` (seconds since bot start). Buys sit below the *current* mid,
/// sells above — the bot never self-crosses, even as the mid moves.
/// Prices stay on the 0.1 grid because `bot_mid_price` snaps and the
/// offset is in 0.1-units.
pub(crate) fn next_bot_order(rng: &mut u64, t: f64) -> BotOrder {
    let account_id = BOT_ACCOUNTS[(xs64(rng) as usize) % BOT_ACCOUNTS.len()];
    let symbol = BOT_SYMBOLS[(xs64(rng) as usize) % BOT_SYMBOLS.len()];
    let side_code = if xs64(rng) & 1 == 0 { "1" } else { "2" };
    let offset_ticks = (xs64(rng) % MAX_OFFSET_TICKS) + 1;
    let mid = bot_mid_price(t);
    let price = if side_code == "1" {
        mid - (offset_ticks as f64) / 10.0
    } else {
        mid + (offset_ticks as f64) / 10.0
    };
    let qty = (xs64(rng) % MAX_QTY) + 1;
    BotOrder {
        account_id,
        symbol,
        side_code,
        price,
        qty,
    }
}

// --- FIX construction ---

/// Build a FIX NewOrderSingle from a bot order and ClOrdID.
pub(crate) fn build_bot_nos(clord: &str, order: &BotOrder) -> FixMessageBuilder {
    FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
        .str_tag(tags::CL_ORD_ID, clord)
        .str_tag(tags::SYMBOL, order.symbol)
        .str_tag(tags::SIDE, order.side_code)
        .str_tag(tags::ORD_TYPE, "2") // Limit
        .str_tag(tags::PRICE, &format!("{:.1}", order.price))
        .str_tag(tags::ORDER_QTY, &format!("{}", order.qty))
        .str_tag(tags::TIME_IN_FORCE, "1") // GTC
        .str_tag(tags::ACCOUNT, &format!("{}", order.account_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_gateway_core::fix::parse::FixMessage;

    // --- bot_mid_price ---

    #[test]
    fn bot_mid_price_at_zero_equals_base() {
        assert!((bot_mid_price(0.0) - MID_BASE).abs() < 1e-9);
    }

    #[test]
    fn bot_mid_price_at_quarter_period_is_peak() {
        let peak = bot_mid_price(PERIOD_SECS / 4.0);
        assert!((peak - (MID_BASE + MID_AMP)).abs() < 1e-6);
    }

    #[test]
    fn bot_mid_price_at_three_quarter_period_is_trough() {
        let trough = bot_mid_price(PERIOD_SECS * 3.0 / 4.0);
        assert!((trough - (MID_BASE - MID_AMP)).abs() < 1e-6);
    }

    #[test]
    fn bot_mid_price_is_periodic() {
        // Periodicity is what makes the wave repeat cleanly across runs.
        // A drift here would indicate a unit-conversion bug.
        let t = 3.7;
        assert!((bot_mid_price(t) - bot_mid_price(t + PERIOD_SECS)).abs() < 1e-9);
    }

    #[test]
    fn bot_mid_price_lands_on_tick_grid() {
        // The bot snaps the mid to a 0.1 grid (coarser than the engine's
        // 0.01 tick) for visual cleanliness. Any mid we feed into
        // next_bot_order must already be on that grid, otherwise
        // (mid ± offset/10) drifts off it.
        for i in 0..=300 {
            let t = i as f64 * 0.1;
            let mid = bot_mid_price(t);
            let in_ticks = (mid * 10.0).round();
            let on_grid = (mid * 10.0 - in_ticks).abs() < 1e-9;
            assert!(on_grid, "mid {mid} at t={t} is not on the 0.1 grid");
        }
    }

    #[test]
    fn bot_mid_price_stays_within_amplitude_band() {
        for i in 0..=300 {
            let t = i as f64 * 0.1;
            let mid = bot_mid_price(t);
            assert!(
                (MID_BASE - MID_AMP - 0.01..=MID_BASE + MID_AMP + 0.01).contains(&mid),
                "mid {mid} at t={t} escaped the [{}..{}] band",
                MID_BASE - MID_AMP,
                MID_BASE + MID_AMP,
            );
        }
    }

    // --- xs64 ---

    #[test]
    fn xs64_nonzero_from_nonzero_seed() {
        let mut s = 0xC0FF_EE00_DEAD_BEEF;
        for _ in 0..1000 {
            assert!(xs64(&mut s) != 0);
        }
    }

    #[test]
    fn xs64_is_deterministic() {
        let mut a = 42;
        let mut b = 42;
        for _ in 0..100 {
            assert_eq!(xs64(&mut a), xs64(&mut b));
        }
    }

    // --- next_bot_order ---

    #[test]
    fn next_bot_order_stays_in_account_pool() {
        let mut rng = 0xC0FF_EE00_DEAD_BEEF;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng, 0.0);
            assert!(
                (2..=32).contains(&o.account_id),
                "account {} out of range",
                o.account_id
            );
        }
    }

    #[test]
    fn next_bot_order_uses_configured_symbols() {
        let mut rng = 1;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng, 0.0);
            assert!(o.symbol == "BTC/USD" || o.symbol == "ETH/USD");
        }
    }

    #[test]
    fn next_bot_order_side_is_buy_or_sell() {
        let mut rng = 1;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng, 0.0);
            assert!(o.side_code == "1" || o.side_code == "2");
        }
    }

    #[test]
    fn next_bot_order_does_not_cross_current_mid() {
        // Buys strictly below the *current* mid, sells strictly above —
        // even as the mid walks the sine, the bot doesn't cross itself.
        let mut rng = 1;
        for i in 0..1000 {
            let t = (i as f64) * 0.05; // sweep the wave
            let mid = bot_mid_price(t);
            let o = next_bot_order(&mut rng, t);
            match o.side_code {
                "1" => assert!(o.price < mid, "buy at {} vs mid {}", o.price, mid),
                "2" => assert!(o.price > mid, "sell at {} vs mid {}", o.price, mid),
                other => panic!("unexpected side {other}"),
            }
            // Offset bounded to [1, MAX_OFFSET_TICKS] units of 0.1.
            let max = MAX_OFFSET_TICKS as f64 / 10.0;
            let abs_offset = (o.price - mid).abs();
            assert!(
                (0.1 - 1e-9..=max + 1e-9).contains(&abs_offset),
                "offset {abs_offset} out of range [0.1..={max}]",
            );
        }
    }

    #[test]
    fn next_bot_order_qty_in_range() {
        let mut rng = 1;
        for _ in 0..1000 {
            let o = next_bot_order(&mut rng, 0.0);
            assert!((1..=MAX_QTY).contains(&o.qty), "qty {} out of range", o.qty);
        }
    }

    #[test]
    fn next_bot_order_price_tracks_moving_mid() {
        // The whole point of the rework: at distinct moments the *mid*
        // the orders cluster around must be different. Compare the mean
        // price of orders sampled at t=0 vs t=PERIOD/4 (peak); they
        // should differ by approximately MID_AMP.
        fn mean_price(rng_seed: u64, t: f64, n: u32) -> f64 {
            let mut rng = rng_seed;
            let mut sum = 0.0;
            for _ in 0..n {
                sum += next_bot_order(&mut rng, t).price;
            }
            sum / n as f64
        }
        let at_zero = mean_price(7, 0.0, 1000);
        let at_peak = mean_price(7, PERIOD_SECS / 4.0, 1000);
        let delta = at_peak - at_zero;
        // Allow generous tolerance: the per-order ±0.50 spread averages
        // out across 1000 samples but doesn't vanish.
        assert!(
            (MID_AMP - 0.5..=MID_AMP + 0.5).contains(&delta),
            "expected ~{MID_AMP} drift between t=0 and peak, got {delta}",
        );
    }

    // --- build_bot_nos ---

    #[test]
    fn build_bot_nos_produces_parseable_fix_with_expected_fields() {
        let order = BotOrder {
            account_id: 7,
            symbol: "BTC/USD",
            side_code: "1",
            price: 99.3,
            qty: 12,
        };
        let raw = build_bot_nos("BOT42", &order).build("BOT", "MELIN-OE", 1);
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_NEW_ORDER_SINGLE);
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("BOT42"));
        assert_eq!(msg.get_str(tags::SYMBOL), Some("BTC/USD"));
        assert_eq!(msg.get_str(tags::SIDE), Some("1"));
        assert_eq!(msg.get_str(tags::ORD_TYPE), Some("2"));
        assert_eq!(msg.get_str(tags::PRICE), Some("99.3"));
        assert_eq!(msg.get_str(tags::ORDER_QTY), Some("12"));
        assert_eq!(msg.get_str(tags::TIME_IN_FORCE), Some("1"));
        assert_eq!(msg.get_str(tags::ACCOUNT), Some("7"));
    }

    #[test]
    fn build_bot_nos_rounds_price_to_one_decimal() {
        // Formatter rounds to 1 decimal. Values drawn by next_bot_order
        // already land on the 0.1 grid, but this guards against future
        // drift in the sampling logic producing non-grid prices.
        let order = BotOrder {
            account_id: 2,
            symbol: "ETH/USD",
            side_code: "2",
            price: 100.056,
            qty: 1,
        };
        let raw = build_bot_nos("X", &order).build("BOT", "MELIN-OE", 1);
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.get_str(tags::PRICE), Some("100.1"));
    }
}
