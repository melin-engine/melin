//! Realistic order flow generator based on empirical market microstructure.
//!
//! Generates synthetic order streams that mimic real exchange order flow
//! patterns: a mix of limit orders, cancels, amendments, market orders,
//! and stop orders with resting book depth.
//!
//! ## Empirical basis
//!
//! The generator parameters are drawn from published academic research on
//! limit order book microstructure. See the bench README for full citations.
//!
//! Key properties reproduced:
//! - **High cancel ratio** (~90% of orders are cancelled before filling)
//! - **Cancel-replace amendments** (~15% of events — simulates market maker quote updates)
//! - **Price placement** follows a power-law around the mid-price
//! - **Order sizes** follow a power-law distribution
//! - **Multiple accounts** with Zipf-distributed activity
//! - **Book depth** builds up naturally across multiple price levels
//! - **Order type diversity**: limit, market, stop, stop-limit, IOC, FOK
//! - **Self-trade prevention** mode variety across orders

use std::num::NonZeroU64;

use rand::SeedableRng;
use rand::distr::{Distribution, Uniform};
use rand::rngs::SmallRng;

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
    /// Conditional probability of a pure cancel when live orders exist.
    /// Combined with `cancel_replace_ratio`, these determine the cancel+amend
    /// rate. The remainder `1 - cancel_ratio - cancel_replace_ratio` produces
    /// new submits that replenish the book. Must satisfy
    /// `cancel_ratio + cancel_replace_ratio < 1.0`.
    /// Default: 0.60 (with cancel_replace at 0.30, total cancel+amend = 0.90).
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
    /// Probability that a submit is a stop order (Stop or StopLimit).
    /// Trigger price is placed on the opposite side of mid from the
    /// limit price, simulating stop-loss protection.
    /// Default: 0.03 (3% of submits).
    pub stop_order_ratio: f64,
    /// Conditional probability of a cancel-replace amendment when live orders
    /// exist. In real markets, market makers rapidly amend resting quotes —
    /// cancel-replace is more common than outright cancel.
    /// Default: 0.30 (with cancel at 0.60, total cancel+amend = 0.90).
    pub cancel_replace_ratio: f64,
    /// Starting order ID. Used to partition ID ranges across multiple
    /// bench clients to avoid collisions.
    pub start_order_id: u64,
    /// PRNG seed for deterministic order flow. Same seed produces the
    /// exact same event sequence, making benchmarks reproducible.
    /// Default: 42.
    pub seed: u64,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            num_accounts: 100,
            num_instruments: 1,
            cancel_ratio: 0.60,
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
            stop_order_ratio: 0.03,
            cancel_replace_ratio: 0.30,
            start_order_id: 1,
            seed: 42,
        }
    }
}

/// A generated event: submit, cancel, or amend.
#[derive(Debug, Clone, Copy)]
pub enum GeneratedEvent {
    Submit {
        symbol: Symbol,
        order: Order,
    },
    Cancel {
        symbol: Symbol,
        order_id: OrderId,
    },
    /// Atomic price/quantity amendment of a resting order.
    CancelReplace {
        symbol: Symbol,
        order_id: OrderId,
        new_price: Price,
        new_quantity: Quantity,
    },
}

/// Generates a realistic stream of order events.
pub struct OrderFlowGenerator {
    config: GeneratorConfig,
    /// Deterministic PRNG (xoshiro256++). Fast, no syscalls, seedable.
    rng: SmallRng,
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
        let seed = config.seed;
        Self {
            config,
            rng: SmallRng::seed_from_u64(seed),
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
        if self.live_count > 0 {
            let roll: f64 = self.unit_dist.sample(&mut self.rng);
            if roll < self.config.cancel_replace_ratio {
                return self.generate_cancel_replace();
            }
            if roll < self.config.cancel_replace_ratio + self.config.cancel_ratio {
                return self.generate_cancel();
            }
        }
        self.generate_submit()
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
                GeneratedEvent::CancelReplace {
                    symbol,
                    order_id,
                    new_price,
                    new_quantity,
                } => Request::CancelReplace {
                    symbol,
                    order_id,
                    new_price,
                    new_quantity,
                },
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
        } else if roll < self.config.market_order_ratio + self.config.stop_order_ratio {
            // Stop order — trigger on the opposite side of the current position.
            // Stop buys trigger above mid (protecting short positions),
            // stop sells trigger below mid (protecting long positions).
            let trigger = self.pick_price(side);
            let stop_roll: f64 = self.unit_dist.sample(&mut self.rng);
            if stop_roll < 0.5 {
                // Plain stop → becomes market order on trigger.
                (
                    OrderType::Stop {
                        trigger_price: trigger,
                    },
                    TimeInForce::GTC,
                )
            } else {
                // Stop-limit → becomes limit order on trigger.
                // Limit price is slightly worse than trigger to increase
                // fill probability (buy: limit above trigger, sell: limit below).
                let limit_offset = (self.config.max_price_offset / 10).max(1);
                let limit_val = match side {
                    Side::Buy => trigger.get().saturating_add(limit_offset),
                    Side::Sell => trigger.get().saturating_sub(limit_offset).max(1),
                };
                let limit_price = Price(NonZeroU64::new(limit_val).expect("price > 0"));
                (
                    OrderType::StopLimit {
                        trigger_price: trigger,
                        limit_price,
                    },
                    TimeInForce::GTC,
                )
            }
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

        // Track orders that rest on the book (GTC limits and pending stops)
        // for cancellation and amendment. Market/IOC/FOK don't rest.
        let rests = matches!(
            (&order_type, time_in_force),
            (OrderType::Limit { .. }, TimeInForce::GTC)
                | (OrderType::Stop { .. }, TimeInForce::GTC)
                | (OrderType::StopLimit { .. }, TimeInForce::GTC)
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

    fn generate_cancel_replace(&mut self) -> GeneratedEvent {
        // Amend a recent resting order — simulates market maker quote updates.
        // Biased toward newest orders (same as cancel).
        let u: f64 = self.unit_dist.sample(&mut self.rng);
        let biased = u * u;
        let idx = (biased * self.live_count as f64) as usize;
        let idx = idx.min(self.live_count - 1);

        let cap = self.live_orders.len();
        let ring_idx = (self.live_cursor + cap - self.live_count + idx) % cap;
        let (order_id, symbol) = self.live_orders[ring_idx];

        // New price: small random offset from mid (tighter than initial placement).
        let side = if self.side_dist.sample(&mut self.rng) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let new_price = self.pick_price(side);
        let new_quantity = self.pick_size();

        GeneratedEvent::CancelReplace {
            symbol,
            order_id,
            new_price,
            new_quantity,
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
        let mut amends = 0;

        for _ in 0..100_000 {
            match ofg.next_event() {
                GeneratedEvent::Submit { .. } => submits += 1,
                GeneratedEvent::Cancel { .. } => cancels += 1,
                GeneratedEvent::CancelReplace { .. } => amends += 1,
            }
        }

        assert!(submits > 0, "should have submits");
        assert!(cancels > 0, "should have cancels");
        assert!(amends > 0, "should have cancel-replace amends");
        let total = submits + cancels + amends;
        let cancel_ratio = cancels as f64 / total as f64;
        assert!(
            cancel_ratio > 0.2 && cancel_ratio < 0.7,
            "unexpected cancel ratio {cancel_ratio}"
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
        let mut stops = 0u64;
        let mut stop_limits = 0u64;

        for _ in 0..100_000 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                match (&order.order_type, order.time_in_force) {
                    (OrderType::Market, _) => markets += 1,
                    (OrderType::Limit { .. }, TimeInForce::GTC) => limit_gtc += 1,
                    (OrderType::Limit { .. }, TimeInForce::IOC) => limit_ioc += 1,
                    (OrderType::Limit { .. }, TimeInForce::FOK) => limit_fok += 1,
                    (OrderType::Stop { .. }, _) => stops += 1,
                    (OrderType::StopLimit { .. }, _) => stop_limits += 1,
                    _ => {}
                }
            }
        }

        assert!(markets > 0, "should have market orders");
        assert!(limit_gtc > 0, "should have limit GTC orders");
        assert!(limit_ioc > 0, "should have limit IOC orders");
        assert!(limit_fok > 0, "should have limit FOK orders");
        assert!(stops > 0, "should have stop orders");
        assert!(stop_limits > 0, "should have stop-limit orders");
        // GTC limits should be the majority.
        assert!(
            limit_gtc > markets + limit_ioc + limit_fok + stops + stop_limits,
            "GTC limits should dominate"
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
                GeneratedEvent::CancelReplace {
                    symbol,
                    order_id,
                    new_price,
                    new_quantity,
                } => {
                    exchange.cancel_replace(
                        symbol,
                        order_id,
                        new_price,
                        new_quantity,
                        &mut reports,
                    );
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

    #[test]
    fn same_seed_produces_identical_sequence() {
        let config = GeneratorConfig::default();
        let mut gen1 = OrderFlowGenerator::new(config.clone());
        let mut gen2 = OrderFlowGenerator::new(config);

        let events1 = gen1.generate_frames(1000);
        let events2 = gen2.generate_frames(1000);

        assert_eq!(events1.len(), events2.len());
        for (i, (a, b)) in events1.iter().zip(events2.iter()).enumerate() {
            assert_eq!(a, b, "divergence at event {i}");
        }
    }

    #[test]
    fn different_seed_produces_different_sequence() {
        let mut gen1 = OrderFlowGenerator::new(GeneratorConfig {
            seed: 1,
            ..Default::default()
        });
        let mut gen2 = OrderFlowGenerator::new(GeneratorConfig {
            seed: 2,
            ..Default::default()
        });

        let events1 = gen1.generate_frames(100);
        let events2 = gen2.generate_frames(100);

        // At least some frames should differ.
        let differ = events1
            .iter()
            .zip(events2.iter())
            .filter(|(a, b)| a != b)
            .count();
        assert!(
            differ > 0,
            "different seeds should produce different output"
        );
    }
}
