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

/// Fast power function for power-law sampling. Specializes common integer
/// exponents to avoid libm `pow()` (~50-100ns). For fractional exponents,
/// uses `exp2(exp * log2(base))` which is ~3-5x faster than `pow()` on
/// most hardware (single transcendental vs two).
#[inline(always)]
fn fast_powf(base: f64, exp: f64) -> f64 {
    // Check for common integer exponents used with typical alpha values.
    // alpha=1.5 → exp=-2.0, alpha=2.0 → exp=-1.0, alpha=3.0 → exp=-0.5.
    if exp == -1.0 {
        1.0 / base
    } else if exp == -2.0 {
        1.0 / (base * base)
    } else if exp == -0.5 {
        1.0 / base.sqrt()
    } else {
        // General case: exp2(exp * log2(base)) is faster than pow().
        f64::exp2(exp * base.log2())
    }
}

use melin_protocol::codec;
use melin_protocol::message::Request;

/// Upper bound on the size (in bytes) of any single length-prefixed request
/// frame this generator can produce. Matches the largest fixed encoding the
/// codec emits for the request variants used by the bench (`SubmitOrder`,
/// `CancelOrder`, `CancelReplace`) including the 4-byte LE length prefix.
/// Callers should size scratch buffers to this so the encoder never reallocates.
pub const MAX_REQUEST_FRAME_BYTES: usize = 136;
use melin_types::types::{
    AccountId, Order, OrderId, OrderType, Price, Quantity, SelfTradeProtection, Side, Symbol,
    TimeInForce,
};

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
    /// Default: 0.45. Calibrated against ITCH 5.0 reference (real-venue
    /// `submit:cancel:replace ≈ 48:45:7`) so the steady-state removes-per-add
    /// lands near 1.07 (real-venue typical) once `live_orders` is large enough
    /// for ring eviction to be rare on bench-sized runs.
    pub cancel_ratio: f64,
    /// Mid-price around which limit orders are placed (in ticks).
    pub mid_price: u64,
    /// Power-law exponent for price offset from mid.
    /// Empirical: ~1.5. Higher = orders cluster tighter around mid.
    pub price_alpha: f64,
    /// Minimum price offset from mid (in ticks). The near-mid power-law
    /// scales its raw draw by this floor so the body of the distance
    /// distribution sits at a realistic spread rather than collapsing
    /// onto the touch (real venues quote tens to hundreds of ticks of
    /// spread; on AAPL the empirical p10 distance-from-mid is ~100
    /// ticks). Default: 50 ticks.
    pub min_price_offset: u64,
    /// Hard cap on price offset from mid (in ticks). Acts as the upper
    /// bound for the *body* of the price distribution; the heavy tail
    /// extends well past this via [`far_price_offset_fraction`] up to
    /// [`far_max_price_offset`].
    pub max_price_offset: u64,
    /// Fraction of price placements drawn from the heavy-tail far-from-mid
    /// regime instead of the near-mid power-law. Real venues advertise stub
    /// quotes and protection-order ladders many orders of magnitude past
    /// the best bid/ask; this knob controls how often the generator places
    /// an order in that regime. Default: 0.05 — calibrated so the
    /// distance-from-mid distribution's p90–p99.9 tail matches the ITCH
    /// reference.
    pub far_price_offset_fraction: f64,
    /// Maximum tail offset (in ticks) when a far-from-mid placement is
    /// sampled. The far sampler uses the same alpha as the near sampler
    /// but is scaled into `[max_price_offset, far_max_price_offset]`.
    /// Default: 2_000_000 ticks (≈ ITCH p99.9 distance for AAPL).
    pub far_max_price_offset: u64,
    /// Power-law exponent for order sizes (continuous tail).
    /// Empirical: ~1.5-2.5.
    pub size_alpha: f64,
    /// Minimum order size.
    pub min_size: u64,
    /// Maximum order size.
    pub max_size: u64,
    /// Modal lot size — the dominant exact share count produced by the
    /// discrete mixture in `pick_size`. Real equity flow is dominated by
    /// round-lot orders (e.g., 100 shares on NASDAQ-listed names), so a
    /// pure power-law over `[min_size, max_size]` undersizes the body of
    /// the distribution. Set to 1 to fall back to pure power-law sampling.
    pub round_lot_size: u64,
    /// Fraction of submits below the modal round lot (odd-lot retail and
    /// algo slicing). Sampled with a flatter power-law than the main size
    /// tail so the sub-modal range covers `[min_size, round_lot_size - 1]`
    /// without collapsing onto `min_size`. Default: 0.25.
    pub odd_lot_fraction: f64,
    /// Power-law alpha for the odd-lot sampler. Smaller than `size_alpha`
    /// so odd lots spread across the sub-modal range instead of all
    /// landing on `min_size`. Default: 1.2 — matches the empirical
    /// odd-lot body shape (p5 ~ 10, p10 ~ 25 against AAPL reference).
    pub odd_lot_alpha: f64,
    /// Fraction of submits placed at exactly `round_lot_size`. This is
    /// the dominant mass in real equity flow. Default: 0.70.
    pub modal_lot_fraction: f64,
    /// Fraction of submits at 2×, 3×, 5×, or 10× the modal round lot,
    /// weighted toward the smaller multiples. Default: 0.10.
    pub multi_round_fraction: f64,
    /// Probability that a new order is aggressive (crosses the spread).
    /// Aggressive buys are placed above mid, aggressive sells below mid,
    /// producing immediate fills. Default: 0.10 (10% of submits fill).
    //
    // TODO(calibration): aggression is currently a flat per-submit Bernoulli;
    // real markets condition it on book imbalance and spread compression.
    // Once that's modeled, crossing-Adds should also be suppressed at the
    // adapter (real ITCH never broadcasts an Add for an order that crosses
    // at submit time — those surface only as Executes against resting
    // liquidity), so the crossing-fraction comparison becomes apples-to-apples
    // instead of structurally zero on the ITCH side.
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
    /// Probability that a non-aggressive limit GTC order is post-only.
    /// Post-only orders are rejected if they would cross the spread,
    /// guaranteeing maker-only execution. Only applied to non-aggressive
    /// GTC limits since post-only + IOC/FOK is pointless.
    /// Default: 0.05 (5% of eligible limits).
    pub post_only_ratio: f64,
    /// Probability that a resting limit order uses Day TIF instead of GTC.
    /// Day orders are cancelled at end-of-session via `EndOfDay`.
    /// Default: 0.10 (10% of resting limits).
    pub day_order_ratio: f64,
    /// Conditional probability of a cancel-replace amendment when live orders
    /// exist. Calibrated against ITCH 5.0 reference (real-venue
    /// `submit:cancel:replace ≈ 48:45:7`). Default: 0.07.
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
            num_accounts: 1_000_000,
            num_instruments: 1,
            cancel_ratio: 0.45,
            mid_price: 10_000,
            price_alpha: 1.5,
            min_price_offset: 50,
            max_price_offset: 1_500,
            far_price_offset_fraction: 0.05,
            far_max_price_offset: 2_000_000,
            size_alpha: 2.0,
            min_size: 1,
            max_size: 100_000,
            round_lot_size: 100,
            odd_lot_fraction: 0.25,
            odd_lot_alpha: 1.15,
            modal_lot_fraction: 0.65,
            multi_round_fraction: 0.07,
            aggression_ratio: 0.10,
            market_order_ratio: 0.05,
            ioc_ratio: 0.05,
            fok_ratio: 0.02,
            stop_order_ratio: 0.03,
            post_only_ratio: 0.05,
            day_order_ratio: 0.10,
            cancel_replace_ratio: 0.07,
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
        account: AccountId,
        order_id: OrderId,
    },
    /// Atomic price/quantity amendment of a resting order.
    CancelReplace {
        symbol: Symbol,
        account: AccountId,
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
    /// Stores (OrderId, AccountId, Symbol, Side) so cancel/amend can use
    /// the original order's side for price generation.
    live_orders: Vec<(OrderId, AccountId, Symbol, Side)>,
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
    /// Pre-computed exponent for power-law price distribution.
    /// `= -1.0 / (price_alpha - 1.0)`. Avoids recomputing per call.
    price_exponent: f64,
    /// Pre-computed exponent for power-law size distribution.
    /// `= -1.0 / (size_alpha - 1.0)`. Avoids recomputing per call.
    size_exponent: f64,
    /// Pre-computed exponent for the odd-lot power-law sampler.
    /// `= -1.0 / (odd_lot_alpha - 1.0)`. Cached to avoid the log/exp
    /// chain on every odd-lot draw.
    odd_lot_exponent: f64,
    /// Pre-built uniform distribution for symbol selection.
    symbol_dist: Uniform<u32>,
    /// Per-generator monotonic sequence for idempotency dedup. Increments on
    /// every wire frame produced. Lives in the generator so streaming callers
    /// can keep generator + seq paired in one place (was a local in the old
    /// `generate_frames` helper).
    seq: u64,
}

impl OrderFlowGenerator {
    /// Create a new generator with the given configuration.
    pub fn new(config: GeneratorConfig) -> Self {
        // 1M slots: real venues carry hundreds of thousands of resting
        // orders per name, and a 100k ring filled within seconds at bench
        // throughput, contaminating the cancel-ratio with eviction-driven
        // cancels. Vec layout = `(OrderId, AccountId, Symbol, Side)` = 24B;
        // 1M slots ≈ 24 MiB — negligible at bench scale.
        let capacity = 1_000_000;
        let start_id = config.start_order_id;
        let seed = config.seed;
        let price_exponent = -1.0 / (config.price_alpha - 1.0);
        let size_exponent = -1.0 / (config.size_alpha - 1.0);
        let odd_lot_exponent = -1.0 / (config.odd_lot_alpha - 1.0);
        let symbol_dist = Uniform::new(1, config.num_instruments + 1).expect("valid range");
        Self {
            config,
            rng: SmallRng::seed_from_u64(seed),
            next_order_id: start_id,
            live_orders: vec![(OrderId(0), AccountId(0), Symbol(0), Side::Buy); capacity],
            live_cursor: 0,
            live_count: 0,
            pending_cancels: Vec::new(),
            unit_dist: Uniform::new(0.0, 1.0).expect("valid range"),
            side_dist: Uniform::new(0, 2).expect("valid range"),
            price_exponent,
            size_exponent,
            odd_lot_exponent,
            symbol_dist,
            seq: 0,
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

    /// Generate the next event and append its length-prefixed wire frame
    /// (`[u32 LE length][payload]`) to `out`. Used by transport benchmarks
    /// that generate orders on-the-fly to keep bench memory bounded.
    pub fn next_wire_frame(&mut self, out: &mut Vec<u8>) {
        let event = self.next_event();
        let request = match event {
            GeneratedEvent::Submit { symbol, order } => Request::SubmitOrder { symbol, order },
            GeneratedEvent::Cancel {
                symbol,
                account,
                order_id,
            } => Request::CancelOrder {
                symbol,
                account,
                order_id,
            },
            GeneratedEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => Request::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            },
        };

        self.seq += 1;
        // Stack scratch sized to the largest encoded request — codec writes
        // [u32 LE length][payload] starting at offset 0.
        let mut encode_buf = [0u8; MAX_REQUEST_FRAME_BYTES];
        let written = codec::encode_request(&request, self.seq, &mut encode_buf).expect("encode");
        out.extend_from_slice(&encode_buf[..written]);
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
            } else if tif_roll
                < self.config.fok_ratio + self.config.ioc_ratio + self.config.day_order_ratio
            {
                TimeInForce::Day
            } else {
                TimeInForce::GTC
            };
            // Post-only is only generated for GTC limits — IOC/FOK post-only
            // is pointless (the order would just rest or get rejected).
            // Aggressive post-only orders will be rejected by the engine,
            // which is realistic (clients sometimes submit post-only orders
            // that race against incoming fills and get rejected).
            let post_only = matches!(tif, TimeInForce::GTC | TimeInForce::Day)
                && self.unit_dist.sample(&mut self.rng) < self.config.post_only_ratio;
            (OrderType::Limit { price, post_only }, tif)
        };

        // Track orders that rest on the book (GTC/Day limits and pending stops)
        // for cancellation and amendment. Market/IOC/FOK don't rest.
        let rests = matches!(
            (&order_type, time_in_force),
            (OrderType::Limit { .. }, TimeInForce::GTC | TimeInForce::Day)
                | (OrderType::Stop { .. }, TimeInForce::GTC | TimeInForce::Day)
                | (
                    OrderType::StopLimit { .. },
                    TimeInForce::GTC | TimeInForce::Day
                )
        );
        if rests {
            let cap = self.live_orders.len();
            let write_idx = self.live_cursor % cap;
            if self.live_count == cap {
                let (evicted_id, evicted_acct, evicted_sym, _) = self.live_orders[write_idx];
                if evicted_id.0 != 0 {
                    self.pending_cancels.push(GeneratedEvent::Cancel {
                        symbol: evicted_sym,
                        account: evicted_acct,
                        order_id: evicted_id,
                    });
                }
            }
            self.live_orders[write_idx] = (order_id, account, symbol, side);
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
                expiry_ns: 0,
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
        let (order_id, account, symbol, side) = self.live_orders[ring_idx];

        // New price uses the original order's side to avoid price-cross
        // rejections (a buy amended to a sell-side price would cross).
        let new_price = self.pick_price(side);
        let new_quantity = self.pick_size();

        GeneratedEvent::CancelReplace {
            symbol,
            account,
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
        let (order_id, account, symbol, _) = self.live_orders[ring_idx];

        // Swap-remove: replace with the oldest entry to keep the ring valid.
        let oldest_idx = (self.live_cursor + cap - self.live_count) % cap;
        self.live_orders[ring_idx] = self.live_orders[oldest_idx];
        self.live_count -= 1;

        GeneratedEvent::Cancel {
            symbol,
            account,
            order_id,
        }
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
            let idx = self.symbol_dist.sample(&mut self.rng);
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
        let cfg = &self.config;
        let u: f64 = self.unit_dist.sample(&mut self.rng);
        // Two-regime mixture: near-mid power-law for the body, plus a
        // heavy-tail far-from-mid regime for stub quotes / protection
        // ladders. A single power-law clamped to a small range collapses
        // the tail and dramatically under-shoots the empirical p90+
        // distance-from-mid.
        let offset = if self.unit_dist.sample(&mut self.rng) < cfg.far_price_offset_fraction {
            let raw = cfg.max_price_offset as f64 * fast_powf(1.0 - u, self.price_exponent);
            (raw as u64).clamp(cfg.max_price_offset, cfg.far_max_price_offset)
        } else {
            let raw = cfg.min_price_offset as f64 * fast_powf(1.0 - u, self.price_exponent);
            (raw as u64).clamp(cfg.min_price_offset, cfg.max_price_offset)
        };

        // Aggressive orders cross the spread: buy above mid, sell below.
        let aggressive = self.unit_dist.sample(&mut self.rng) < cfg.aggression_ratio;

        let price_val = match (side, aggressive) {
            (Side::Buy, false) => cfg.mid_price.saturating_sub(offset),
            (Side::Buy, true) => cfg.mid_price.saturating_add(offset),
            (Side::Sell, false) => cfg.mid_price.saturating_add(offset),
            (Side::Sell, true) => cfg.mid_price.saturating_sub(offset),
        };
        let price_val = price_val.max(1);
        Price(NonZeroU64::new(price_val).expect("price > 0"))
    }

    /// Pick an order size from a discrete-plus-tail mixture. Real equity
    /// flow has heavy mass at the modal round lot (e.g., 100 shares on
    /// NASDAQ-listed names) and lighter mass on integer multiples, with
    /// odd lots filling the sub-modal range and a power-law tail
    /// capturing institutional blocks. A pure power-law produces a
    /// continuous distribution that doesn't reproduce this shape.
    fn pick_size(&mut self) -> Quantity {
        let cfg = &self.config;
        let u: f64 = self.unit_dist.sample(&mut self.rng);
        let size = if cfg.round_lot_size <= 1 {
            // Round-lot snapping disabled: fall back to pure power-law.
            let raw = cfg.min_size as f64 * fast_powf(1.0 - u, self.size_exponent);
            (raw as u64).clamp(cfg.min_size, cfg.max_size)
        } else if u < cfg.odd_lot_fraction {
            // Odd lots below the modal round lot. Uses a flatter alpha
            // (`odd_lot_alpha`) than the main size tail so the sub-modal
            // body spreads across `[min_size, round_lot_size - 1]` instead
            // of collapsing onto `min_size`.
            let v = self.unit_dist.sample(&mut self.rng);
            let raw = cfg.min_size as f64 * fast_powf(1.0 - v, self.odd_lot_exponent);
            (raw as u64).clamp(cfg.min_size, cfg.round_lot_size.saturating_sub(1).max(1))
        } else if u < cfg.odd_lot_fraction + cfg.modal_lot_fraction {
            // Modal mass — the dominant exact round lot.
            cfg.round_lot_size
        } else if u < cfg.odd_lot_fraction + cfg.modal_lot_fraction + cfg.multi_round_fraction {
            // Other round lots at integer multiples of the modal size,
            // weighted toward smaller multiples (which dominate real
            // flow: 200 > 300 > 500 > 1000).
            let v: f64 = self.unit_dist.sample(&mut self.rng);
            let multiplier: u64 = if v < 0.4 {
                2
            } else if v < 0.6 {
                3
            } else if v < 0.8 {
                5
            } else {
                10
            };
            (cfg.round_lot_size * multiplier).min(cfg.max_size)
        } else {
            // Heavy tail: power-law starting from the modal size, capturing
            // institutional block orders.
            let v = self.unit_dist.sample(&mut self.rng);
            let raw = cfg.round_lot_size as f64 * fast_powf(1.0 - v, self.size_exponent);
            (raw as u64).clamp(cfg.round_lot_size, cfg.max_size)
        };
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
    fn size_mixture_produces_modal_round_lot_majority() {
        // With the default mixture (modal_lot_fraction=0.70 at round_lot_size=100),
        // a large sample should show >60% orders at exactly 100 shares.
        let mut ofg = OrderFlowGenerator::new(GeneratorConfig::default());
        let mut total = 0usize;
        let mut at_modal = 0usize;
        let mut sub_modal = 0usize;
        for _ in 0..20_000 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                let q = order.quantity.get();
                total += 1;
                if q == 100 {
                    at_modal += 1;
                } else if q < 100 {
                    sub_modal += 1;
                }
            }
        }
        let modal_frac = at_modal as f64 / total as f64;
        let sub_frac = sub_modal as f64 / total as f64;
        assert!(
            modal_frac > 0.6,
            "expected >60% of orders at modal size 100, got {modal_frac:.4}"
        );
        // Odd-lot mass should be non-trivial too (we want some sub-100 orders).
        assert!(
            (0.05..0.30).contains(&sub_frac),
            "odd-lot fraction off: {sub_frac:.4}"
        );
    }

    #[test]
    fn size_mixture_disabled_falls_back_to_power_law() {
        // round_lot_size=1 disables the mixture and restores pure power-law.
        let config = GeneratorConfig {
            round_lot_size: 1,
            ..Default::default()
        };
        let mut ofg = OrderFlowGenerator::new(config);
        let mut at_modal = 0usize;
        let mut total = 0usize;
        for _ in 0..5_000 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                if order.quantity.get() == 100 {
                    at_modal += 1;
                }
                total += 1;
            }
        }
        // With pure power-law alpha=2.0, exactly-100 should be vanishingly
        // rare — well under the modal-mixture rate.
        let frac = at_modal as f64 / total as f64;
        assert!(
            frac < 0.05,
            "without mixture, exact-100 mass should stay <5%, got {frac:.4}"
        );
    }

    #[test]
    fn prices_cluster_around_mid() {
        let config = GeneratorConfig {
            mid_price: 10_000,
            ..Default::default()
        };
        let mut ofg = OrderFlowGenerator::new(config);
        let mut within_body = 0;
        let mut total = 0;

        // The near-mid power-law is floored at `min_price_offset` (50 by
        // default), so prices won't sit on top of the mid. "Clustering"
        // here means "stays in the near-mid body", i.e., well inside
        // `max_price_offset` (1500 default) rather than landing in the
        // heavy far-from-mid tail.
        for _ in 0..10_000 {
            if let GeneratedEvent::Submit { order, .. } = ofg.next_event() {
                let OrderType::Limit { price, .. } = order.order_type else {
                    continue;
                };
                let price = price.get();
                let dist = (price as i64 - 10_000).unsigned_abs();
                if dist <= 1_500 {
                    within_body += 1;
                }
                total += 1;
            }
        }

        let pct = within_body as f64 / total as f64;
        assert!(
            pct > 0.85,
            "expected >85% of orders inside the near-mid body, got {pct:.2}"
        );
    }

    #[test]
    fn next_wire_frame_produces_valid_wire_data() {
        let mut ofg = OrderFlowGenerator::new(GeneratorConfig::default());
        let mut buf = Vec::new();

        // Decode each frame as it's produced. Each call appends one
        // length-prefixed frame to `buf`; we walk the cursor forward.
        let mut cursor = 0usize;
        for _ in 0..100 {
            ofg.next_wire_frame(&mut buf);
            assert!(buf.len() >= cursor + 4, "frame must include length prefix");
            let len = u32::from_le_bytes(buf[cursor..cursor + 4].try_into().unwrap()) as usize;
            let payload = &buf[cursor + 4..cursor + 4 + len];
            let (seq, _request) = codec::decode_request(payload).expect("decode");
            assert!(seq > 0, "frame seq should be > 0");
            cursor += 4 + len;
        }
        assert_eq!(cursor, buf.len(), "all bytes consumed");
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
                    (
                        OrderType::Limit { .. },
                        TimeInForce::GTC | TimeInForce::Day | TimeInForce::GTD,
                    ) => {
                        limit_gtc += 1;
                    }
                    (OrderType::Limit { .. }, TimeInForce::IOC) => limit_ioc += 1,
                    (OrderType::Limit { .. }, TimeInForce::FOK) => limit_fok += 1,
                    (OrderType::Stop { .. }, _) => stops += 1,
                    (OrderType::StopLimit { .. }, _) => stop_limits += 1,
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
        use melin_engine::exchange::Exchange;
        use melin_types::types::{CurrencyId, ExecutionReport, InstrumentSpec};

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
                GeneratedEvent::Cancel {
                    symbol,
                    account,
                    order_id,
                } => {
                    exchange.cancel(symbol, account, order_id, &mut reports);
                }
                GeneratedEvent::CancelReplace {
                    symbol,
                    account,
                    order_id,
                    new_price,
                    new_quantity,
                } => {
                    exchange.cancel_replace(
                        symbol,
                        account,
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

    fn collect_frames(ofg: &mut OrderFlowGenerator, count: usize) -> Vec<Vec<u8>> {
        let mut frames = Vec::with_capacity(count);
        let mut scratch = Vec::new();
        for _ in 0..count {
            scratch.clear();
            ofg.next_wire_frame(&mut scratch);
            frames.push(scratch.clone());
        }
        frames
    }

    #[test]
    fn same_seed_produces_identical_sequence() {
        let config = GeneratorConfig::default();
        let mut gen1 = OrderFlowGenerator::new(config.clone());
        let mut gen2 = OrderFlowGenerator::new(config);

        let events1 = collect_frames(&mut gen1, 1000);
        let events2 = collect_frames(&mut gen2, 1000);

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

        let events1 = collect_frames(&mut gen1, 100);
        let events2 = collect_frames(&mut gen2, 100);

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
