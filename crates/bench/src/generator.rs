//! Realistic order flow generator based on empirical market microstructure.
//!
//! Generates synthetic order streams that mimic real exchange order flow
//! patterns: a mix of limit orders and cancels with resting book depth.
//!
//! ## Empirical basis
//!
//! The generator parameters are drawn from published academic research on
//! limit order book microstructure. See the bench README for full citations.
//!
//! Key properties reproduced:
//! - **High cancel ratio** (~90% of orders are cancelled before filling)
//! - **Price placement** follows a power-law around the mid-price
//! - **Order sizes** follow a power-law distribution
//! - **Multiple accounts** with Zipf-distributed activity
//! - **Book depth** builds up naturally across multiple price levels

use std::num::NonZeroU64;

use rand::distr::{Distribution, Uniform};

use trading_engine::types::{
    AccountId, Order, OrderId, OrderType, Price, Quantity, SelfTradeProtection, Side, Symbol,
    TimeInForce,
};
use trading_protocol::codec;
use trading_protocol::message::Request;

/// Configuration for the realistic order flow generator.
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Number of distinct accounts submitting orders.
    /// Activity follows a Zipf distribution (few heavy, many light).
    pub num_accounts: u32,
    /// Number of instruments to trade.
    pub num_instruments: u32,
    /// Probability that an event is a cancel (vs a new order).
    /// Real markets: ~0.85-0.95. Default: 0.90.
    pub cancel_ratio: f64,
    /// Mid-price around which limit orders are placed (in ticks).
    pub mid_price: u64,
    /// Power-law exponent for price offset from mid.
    /// Empirical: ~1.5. Higher = orders cluster tighter around mid.
    pub price_alpha: f64,
    /// Maximum price offset from mid (in ticks).
    pub max_price_offset: u64,
    /// Power-law exponent for order sizes.
    /// Empirical: ~1.5-2.5.
    pub size_alpha: f64,
    /// Minimum order size.
    pub min_size: u64,
    /// Maximum order size.
    pub max_size: u64,
    /// Probability that a new order is aggressive (crosses the spread).
    /// Aggressive buys are placed above mid, aggressive sells below mid,
    /// producing immediate fills. Default: 0.10 (10% of submits fill).
    pub aggression_ratio: f64,
    /// Probability that a submit is a market order (no price, IOC-like).
    /// Default: 0.05 (5% of submits).
    pub market_order_ratio: f64,
    /// Probability that a limit order uses IOC time-in-force instead of GTC.
    /// Default: 0.05 (5% of limit submits).
    pub ioc_ratio: f64,
    /// Probability that a limit order uses FOK time-in-force instead of GTC.
    /// Default: 0.02 (2% of limit submits).
    pub fok_ratio: f64,
    /// Starting order ID. Used to partition ID ranges across multiple
    /// bench clients to avoid collisions.
    pub start_order_id: u64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            num_accounts: 100,
            num_instruments: 1,
            cancel_ratio: 0.90,
            mid_price: 10_000,
            price_alpha: 1.5,
            max_price_offset: 200,
            size_alpha: 2.0,
            min_size: 1,
            max_size: 1000,
            aggression_ratio: 0.10,
            market_order_ratio: 0.05,
            ioc_ratio: 0.05,
            fok_ratio: 0.02,
            start_order_id: 1,
        }
    }
}

/// A generated event: either a new order or a cancel.
#[derive(Debug, Clone, Copy)]
pub enum GeneratedEvent {
    Submit { symbol: Symbol, order: Order },
    Cancel { symbol: Symbol, order_id: OrderId },
}

/// Generates a realistic stream of order events.
pub struct OrderFlowGenerator {
    config: GeneratorConfig,
    rng: rand::rngs::ThreadRng,
    next_order_id: u64,
    /// Ring buffer of recently submitted order IDs available for cancellation.
    /// Fixed-size circular buffer to bound memory. OrderId(0) = empty slot.
    live_orders: Vec<(OrderId, Symbol)>,
    /// Write position in the live_orders ring buffer.
    live_cursor: usize,
    /// Number of valid entries in live_orders (up to capacity).
    live_count: usize,
    /// Cancels for orders evicted from the ring buffer when it wraps.
    /// Drained before generating new events to prevent orphaned orders.
    pending_cancels: Vec<GeneratedEvent>,
    /// Uniform distribution for [0, 1) sampling.
    unit_dist: Uniform<f64>,
    /// Uniform distribution for side selection.
    side_dist: Uniform<u32>,
}

impl OrderFlowGenerator {
    /// Create a new generator with the given configuration.
    pub fn new(config: GeneratorConfig) -> Self {
        let capacity = 100_000; // track up to 100K live orders for cancellation
        let start_id = config.start_order_id;
        Self {
            config,
            rng: rand::rng(),
            next_order_id: start_id,
            live_orders: vec![(OrderId(0), Symbol(0)); capacity],
            live_cursor: 0,
            live_count: 0,
            pending_cancels: Vec::new(),
            unit_dist: Uniform::new(0.0, 1.0).expect("valid range"),
            side_dist: Uniform::new(0, 2).expect("valid range"),
        }
    }

    /// Generate the next event.
    pub fn next_event(&mut self) -> GeneratedEvent {
        // Drain pending cancels for orders evicted from the ring buffer.
        if let Some(cancel) = self.pending_cancels.pop() {
            return cancel;
        }
        if self.live_count > 0 && self.unit_dist.sample(&mut self.rng) < self.config.cancel_ratio {
            self.generate_cancel()
        } else {
            self.generate_submit()
        }
    }

    /// Pre-generate a batch of events for engine-only benchmarks.
    /// Generates all events upfront so RNG overhead doesn't pollute timing.
    pub fn generate_events(&mut self, count: usize) -> Vec<GeneratedEvent> {
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            events.push(self.next_event());
        }
        events
    }

    /// Pre-generate a batch of pre-encoded wire frames for roundtrip benchmarks.
    /// Generates all events upfront so RNG overhead doesn't pollute timing.
    pub fn generate_frames(&mut self, count: usize) -> Vec<Vec<u8>> {
        let mut frames = Vec::with_capacity(count);
        let mut encode_buf = [0u8; 128];

        for _ in 0..count {
            let event = self.next_event();
            let request = match event {
                GeneratedEvent::Submit { symbol, order } => Request::SubmitOrder { symbol, order },
                GeneratedEvent::Cancel { symbol, order_id } => {
                    Request::CancelOrder { symbol, order_id }
                }
            };

            let written = codec::encode_request(&request, &mut encode_buf).expect("encode");
            frames.push(encode_buf[4..written].to_vec());
        }

        frames
    }

    fn generate_submit(&mut self) -> GeneratedEvent {
        let order_id = OrderId(self.next_order_id);
        self.next_order_id += 1;

        let account = self.pick_account();
        let symbol = self.pick_symbol();
        let side = if self.side_dist.sample(&mut self.rng) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let quantity = self.pick_size();

        // Pick order type and time-in-force.
        let roll: f64 = self.unit_dist.sample(&mut self.rng);
        let (order_type, time_in_force) = if roll < self.config.market_order_ratio {
            // Market order — no price, always IOC semantics.
            (OrderType::Market, TimeInForce::IOC)
        } else {
            let price = self.pick_price(side);
            let tif_roll: f64 = self.unit_dist.sample(&mut self.rng);
            let tif = if tif_roll < self.config.fok_ratio {
                TimeInForce::FOK
            } else if tif_roll < self.config.fok_ratio + self.config.ioc_ratio {
                TimeInForce::IOC
            } else {
                TimeInForce::GTC
            };
            (OrderType::Limit { price }, tif)
        };

        // Only track GTC limit orders for cancellation — market/IOC/FOK
        // orders don't rest on the book.
        let rests = matches!(
            (&order_type, time_in_force),
            (OrderType::Limit { .. }, TimeInForce::GTC)
        );
        if rests {
            let cap = self.live_orders.len();
            let write_idx = self.live_cursor % cap;
            if self.live_count == cap {
                let (evicted_id, evicted_sym) = self.live_orders[write_idx];
                if evicted_id.0 != 0 {
                    self.pending_cancels.push(GeneratedEvent::Cancel {
                        symbol: evicted_sym,
                        order_id: evicted_id,
                    });
                }
            }
            self.live_orders[write_idx] = (order_id, symbol);
            self.live_cursor += 1;
            if self.live_count < cap {
                self.live_count += 1;
            }
        }

        GeneratedEvent::Submit {
            symbol,
            order: Order {
                id: order_id,
                account,
                side,
                order_type,
                time_in_force,
                quantity,
                stp: self.pick_stp(),
            },
        }
    }

    fn generate_cancel(&mut self) -> GeneratedEvent {
        // Bias toward recent orders (high index = newest). In real markets,
        // cancels are dominated by rapid quote updates (cancel-replace) on
        // recent orders. Use U^2 to skew the distribution toward the
        // newest entries: squaring a uniform [0,1) concentrates mass near 1.
        let u: f64 = self.unit_dist.sample(&mut self.rng);
        let biased = u * u; // skew toward 1.0 (newest)
        let idx = (biased * self.live_count as f64) as usize;
        let idx = idx.min(self.live_count - 1);

        let cap = self.live_orders.len();
        // Ring entries are oldest-first: index 0 = oldest, live_count-1 = newest.
        let ring_idx = (self.live_cursor + cap - self.live_count + idx) % cap;
        let (order_id, symbol) = self.live_orders[ring_idx];

        // Swap-remove: replace with the oldest entry to keep the ring valid.
        let oldest_idx = (self.live_cursor + cap - self.live_count) % cap;
        self.live_orders[ring_idx] = self.live_orders[oldest_idx];
        self.live_count -= 1;

        GeneratedEvent::Cancel { symbol, order_id }
    }

    /// Pick an account with Zipf-like distribution.
    /// Account 1 trades most frequently, account N least.
    fn pick_account(&mut self) -> AccountId {
        // Zipf: P(k) ~ 1/k. Use inverse transform: k = ceil(n / U)
        // clamped to [1, num_accounts].
        let u: f64 = self.unit_dist.sample(&mut self.rng);
        let k = (1.0 / (1.0 - u + u / self.config.num_accounts as f64)).ceil() as u32;
        AccountId(k.min(self.config.num_accounts))
    }

    /// Pick a symbol uniformly.
    fn pick_symbol(&mut self) -> Symbol {
        if self.config.num_instruments == 1 {
            Symbol(1)
        } else {
            let idx = Uniform::new(1, self.config.num_instruments + 1)
                .expect("valid range")
                .sample(&mut self.rng);
            Symbol(idx)
        }
    }

    /// Pick a price offset from mid using a power-law distribution, then
    /// add/subtract from mid based on side.
    ///
    /// Power-law with exponent alpha: P(x) ~ x^(-alpha).
    /// Inverse CDF: x = x_min * (1 - U)^(-1/(alpha-1)) for alpha > 1.
    ///
    /// With probability `aggression_ratio`, the order is aggressive: buys
    /// are placed above mid (crossing into the ask side) and sells below
    /// mid (crossing into the bid side), producing immediate fills.
    fn pick_price(&mut self, side: Side) -> Price {
        let u: f64 = self.unit_dist.sample(&mut self.rng);
        let alpha = self.config.price_alpha;
        let raw = (1.0 - u).powf(-1.0 / (alpha - 1.0));
        let offset = (raw as u64).clamp(1, self.config.max_price_offset);

        // Aggressive orders cross the spread: buy above mid, sell below.
        let aggressive = self.unit_dist.sample(&mut self.rng) < self.config.aggression_ratio;

        let price_val = match (side, aggressive) {
            (Side::Buy, false) => self.config.mid_price.saturating_sub(offset),
            (Side::Buy, true) => self.config.mid_price.saturating_add(offset),
            (Side::Sell, false) => self.config.mid_price.saturating_add(offset),
            (Side::Sell, true) => self.config.mid_price.saturating_sub(offset),
        };
        let price_val = price_val.max(1);
        Price(NonZeroU64::new(price_val).expect("price > 0"))
    }

    /// Pick an order size from a power-law distribution.
    fn pick_size(&mut self) -> Quantity {
        let u: f64 = self.unit_dist.sample(&mut self.rng);
        let alpha = self.config.size_alpha;
        let raw = self.config.min_size as f64 * (1.0 - u).powf(-1.0 / (alpha - 1.0));
        let size = (raw as u64).clamp(self.config.min_size, self.config.max_size);
        Quantity(NonZeroU64::new(size).expect("size > 0"))
    }

    /// Pick a self-trade prevention mode. Most orders use Allow (70%),
    /// with the three active modes sharing the remainder.
    fn pick_stp(&mut self) -> SelfTradeProtection {
        let u: f64 = self.unit_dist.sample(&mut self.rng);
        if u < 0.70 {
            SelfTradeProtection::Allow
        } else if u < 0.80 {
            SelfTradeProtection::CancelNewest
        } else if u < 0.90 {
            SelfTradeProtection::CancelOldest
        } else {
            SelfTradeProtection::CancelBoth
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generator_produces_events() {
        let mut ofg = OrderFlowGenerator::new(GeneratorConfig::default());
        let mut submits = 0;
        let mut cancels = 0;

        for _ in 0..100_000 {
            match ofg.next_event() {
                GeneratedEvent::Submit { .. } => submits += 1,
                GeneratedEvent::Cancel { .. } => cancels += 1,
            }
        }

        assert!(submits > 0, "should have submits");
        assert!(cancels > 0, "should have cancels");
        // The realized cancel ratio is lower than the configured 0.9
        // because each cancel consumes a live order — the pool drains
        // quickly and forces new submits. In steady state, the ratio
        // converges to ~0.47-0.52. This is correct: the *conditional*
        // probability of cancel (when orders exist) is 0.9, but the
        // *unconditional* ratio is bounded by the submit rate.
        let ratio = cancels as f64 / (submits + cancels) as f64;
        assert!(
            ratio > 0.3 && ratio < 0.7,
            "unexpected cancel ratio {ratio}"
        );
    }

    #[test]
    fn order_ids_are_monotonic() {
        let mut ofg = OrderFlowGenerator::new(GeneratorConfig::default());
        let mut max_id = 0u64;

        for _ in 0..1000 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                assert!(order.id.0 > max_id, "order IDs must be monotonic");
                max_id = order.id.0;
            }
        }
        assert!(max_id > 0);
    }

    #[test]
    fn prices_cluster_around_mid() {
        let config = GeneratorConfig {
            mid_price: 10_000,
            ..Default::default()
        };
        let mut ofg = OrderFlowGenerator::new(config);
        let mut within_10_ticks = 0;
        let mut total = 0;

        for _ in 0..10_000 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                let OrderType::Limit { price } = order.order_type else {
                    continue;
                };
                let price = price.get();
                let dist = (price as i64 - 10_000).unsigned_abs();
                if dist <= 10 {
                    within_10_ticks += 1;
                }
                total += 1;
            }
        }

        let pct = within_10_ticks as f64 / total as f64;
        assert!(
            pct > 0.2,
            "expected >20% of orders within 10 ticks of mid, got {pct:.2}"
        );
    }

    #[test]
    fn generate_frames_produces_valid_wire_data() {
        let mut ofg = OrderFlowGenerator::new(GeneratorConfig::default());
        let frames = ofg.generate_frames(100);
        assert_eq!(frames.len(), 100);

        for frame in &frames {
            let result = codec::decode_request(frame);
            assert!(result.is_ok(), "frame should decode: {result:?}");
        }
    }

    #[test]
    fn multiple_accounts_used() {
        let config = GeneratorConfig {
            num_accounts: 10,
            ..Default::default()
        };
        let mut ofg = OrderFlowGenerator::new(config);
        let mut seen = std::collections::HashSet::new();

        for _ in 0..10_000 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                seen.insert(order.account.0);
            }
        }

        assert!(
            seen.len() >= 5,
            "expected at least 5 distinct accounts, got {}",
            seen.len()
        );
    }

    #[test]
    fn start_order_id_respected() {
        let config = GeneratorConfig {
            start_order_id: 1_000_000,
            ..Default::default()
        };
        let mut ofg = OrderFlowGenerator::new(config);

        for _ in 0..100 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                assert!(order.id.0 >= 1_000_000, "order ID should start at offset");
                return;
            }
        }
        panic!("no submit generated");
    }

    #[test]
    fn order_type_diversity() {
        let mut ofg = OrderFlowGenerator::new(GeneratorConfig::default());
        let mut markets = 0u64;
        let mut limit_gtc = 0u64;
        let mut limit_ioc = 0u64;
        let mut limit_fok = 0u64;

        for _ in 0..100_000 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                match (&order.order_type, order.time_in_force) {
                    (OrderType::Market, _) => markets += 1,
                    (OrderType::Limit { .. }, TimeInForce::GTC) => limit_gtc += 1,
                    (OrderType::Limit { .. }, TimeInForce::IOC) => limit_ioc += 1,
                    (OrderType::Limit { .. }, TimeInForce::FOK) => limit_fok += 1,
                    _ => {}
                }
            }
        }

        assert!(markets > 0, "should have market orders");
        assert!(limit_gtc > 0, "should have limit GTC orders");
        assert!(limit_ioc > 0, "should have limit IOC orders");
        assert!(limit_fok > 0, "should have limit FOK orders");
        // GTC should be the majority.
        assert!(
            limit_gtc > markets + limit_ioc + limit_fok,
            "GTC should dominate"
        );
    }

    #[test]
    fn aggressive_orders_produce_fills() {
        use trading_engine::exchange::Exchange;
        use trading_engine::types::{CurrencyId, ExecutionReport, InstrumentSpec};

        let config = GeneratorConfig {
            num_accounts: 2,
            aggression_ratio: 0.50, // 50% aggressive for faster convergence
            ..Default::default()
        };
        let mut ofg = OrderFlowGenerator::new(config);

        let mut exchange = Exchange::new();
        exchange.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(1),
            quote: CurrencyId(2),
        });
        exchange.deposit(AccountId(1), CurrencyId(1), u64::MAX / 4);
        exchange.deposit(AccountId(1), CurrencyId(2), u64::MAX / 4);
        exchange.deposit(AccountId(2), CurrencyId(1), u64::MAX / 4);
        exchange.deposit(AccountId(2), CurrencyId(2), u64::MAX / 4);

        let mut reports = Vec::new();
        let mut fills = 0u64;

        for _ in 0..10_000 {
            reports.clear();
            match ofg.next_event() {
                GeneratedEvent::Submit { symbol, order } => {
                    exchange.execute(symbol, order, &mut reports);
                }
                GeneratedEvent::Cancel { symbol, order_id } => {
                    exchange.cancel(symbol, order_id, &mut reports);
                }
            }
            fills += reports
                .iter()
                .filter(|r| matches!(r, ExecutionReport::Fill { .. }))
                .count() as u64;
        }

        assert!(fills > 0, "expected some fills with 50% aggression ratio");
    }

    #[test]
    fn pre_generated_events_match_count() {
        let mut ofg = OrderFlowGenerator::new(GeneratorConfig::default());
        let events = ofg.generate_events(500);
        assert_eq!(events.len(), 500);
    }
}
