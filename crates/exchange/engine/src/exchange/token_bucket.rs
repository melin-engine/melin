//! Per-account token bucket for the order-submission rate limiter (SEC-04).
//!
//! Algorithm is integer-only for cross-platform determinism; see [`TokenBucket::refill`]
//! for the elapsed-time accounting that preserves sub-token fractional time across
//! polls.

/// Per-account token-bucket state for the order-submission rate limiter.
///
/// Sized to one cache line for the inevitable cache miss on first lookup
/// (16 B = `tokens`(8) + `last_refill_ns`(8); the rest of the line is
/// shared with the next entry in the HashMap4 bucket).
#[derive(Debug, Clone, Copy)]
pub(super) struct TokenBucket {
    /// Available tokens. Decremented by 1 on every accepted order. Refilled
    /// up to `max_orders_burst` based on `now_ns - last_refill_ns`. `u64`
    /// rather than `u32` so the bucket capacity check is a single 64-bit
    /// compare against the configured burst (which fits in `u32` but is
    /// widened on read).
    pub(super) tokens: u64,
    /// Wall-clock-equivalent timestamp (event `ts_ns`) of the last refill.
    /// Advanced by exactly the time consumed by tokens added during the
    /// last `refill_and_consume` call so that fractional time below one
    /// token is preserved across calls — e.g. at 1000 ord/s, two calls
    /// 600 µs and then 600 µs apart correctly issue exactly one token
    /// (not zero, not two).
    pub(super) last_refill_ns: u64,
}

impl TokenBucket {
    /// Initialize a fresh bucket: full tokens, refill clock anchored at
    /// the current event time. First-touch sees a full burst — same shape
    /// as a real-world reservation system (you don't penalise an account
    /// for being newly active).
    #[inline]
    pub(super) fn new(burst: u32, now_ns: u64) -> Self {
        Self {
            tokens: burst as u64,
            last_refill_ns: now_ns,
        }
    }

    /// Refill the bucket based on elapsed time, but do not consume.
    /// Used both by [`Self::refill_and_consume`] (the order-submission
    /// path) and by the bucket-eviction probe in
    /// [`super::Exchange::try_evict_bucket`], which needs to know whether a
    /// quiet account has converged back to full capacity without
    /// charging a token for the privilege.
    ///
    /// Integer math only — no floats — for cross-platform determinism.
    /// The refill formula is `earned = elapsed_ns * rate / 1e9`. Two cases:
    ///
    /// 1. The new token count caps at `burst` (bucket overflows). Any
    ///    elapsed time beyond the point at which the bucket reached
    ///    `burst` is "wasted" — there's no headroom to absorb new
    ///    tokens — so `last_refill_ns` is snapped to `now_ns` to
    ///    discard that idle slack. Without this snap, the wasted time
    ///    would accumulate as phantom credit on `last_refill_ns`,
    ///    letting subsequent close-spaced events draw the burst again
    ///    (issuing far more tokens than `rate` supports).
    /// 2. The new token count stays below `burst`. `last_refill_ns` is
    ///    advanced by exactly the time corresponding to tokens earned
    ///    (`earned * 1e9 / rate`) so sub-token fractional time
    ///    accumulates across calls — a 1000 ord/s bucket polled twice
    ///    600 µs apart correctly issues exactly one token, not zero.
    #[inline]
    pub(super) fn refill(&mut self, now_ns: u64, rate: u32, burst: u32) {
        // Defensive cap: a tampered snapshot, a primary/replica `--max-orders-burst`
        // mismatch, or any future bug that produces `tokens > burst` would otherwise
        // grant unbounded credit on the next event (the `now_ns > last_refill_ns`
        // branch below can leave `tokens` untouched). Clamping at the point of use
        // keeps the bucket invariant `tokens <= burst` independent of how the state
        // was loaded. One cmp on the hot path.
        if self.tokens > burst as u64 {
            self.tokens = burst as u64;
        }
        // Clock can only go forward in our timeline (event ts_ns is
        // assigned by the reader at ingest and journaled). If we ever
        // see now_ns < last_refill_ns it means the operator changed the
        // clock or there is a bug upstream — be defensive: don't panic,
        // skip the refill (`refill_and_consume` will still allow consume
        // so we don't reject every order until time catches up). Locked
        // in by `rate_limit_clock_backwards_is_defensive_not_panic`.
        if now_ns > self.last_refill_ns {
            let elapsed = now_ns - self.last_refill_ns;
            // saturating_mul instead of u128: at u32::MAX rate × ~4.3e9
            // elapsed ns the product overflows u64. On overflow we cap at
            // u64::MAX which, divided by 1e9, still exceeds any u32 burst,
            // so the .min(burst) below yields the same result as the
            // u128 form (saturation absorbs the overflow case). The u64
            // form lets the compiler emit a magic-number multiply for
            // the constant-1e9 divide and a single `div` for /rate, vs
            // the ~50ns __udivti3 library call per event the u128 form
            // emitted on the matching hot path (perf: ~2.6% of total
            // CPU).
            let earned = (elapsed.saturating_mul(rate as u64) / 1_000_000_000).min(burst as u64);
            let new_tokens = (self.tokens + earned).min(burst as u64);
            if new_tokens >= burst as u64 {
                // Bucket is at capacity — discard any remaining elapsed
                // time so phantom credit can't accumulate. See doc above.
                self.last_refill_ns = now_ns;
            } else if earned > 0 {
                // Below cap and we earned tokens — advance by exactly the
                // time those tokens consumed, preserving fractional-token
                // time below one token's worth. We reach this branch only
                // when new_tokens < burst, so earned < burst ≤ u32::MAX,
                // and earned × 1e9 < 4.3e18 fits in u64 with room to spare.
                let consumed_ns = (earned * 1_000_000_000) / rate as u64;
                self.last_refill_ns += consumed_ns;
            }
            // else: earned == 0 (sub-token time elapsed, bucket below
            // cap) — leave last_refill_ns unchanged so the fractional
            // time accumulates into the next call.
            self.tokens = new_tokens;
        }
    }

    /// Refill the bucket based on elapsed time, then attempt to consume
    /// one token. Returns `true` if the order is allowed.
    #[inline]
    pub(super) fn refill_and_consume(&mut self, now_ns: u64, rate: u32, burst: u32) -> bool {
        self.refill(now_ns, rate, burst);
        if self.tokens > 0 {
            self.tokens -= 1;
            true
        } else {
            false
        }
    }
}
