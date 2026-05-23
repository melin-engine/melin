//! SEC-04: per-account order-rate limiter tests. Exercise the
//! `TokenBucket` algorithm end-to-end through `Exchange`, including
//! the eviction policy in `try_evict_bucket` and the snapshot
//! continuity contract on `restore_order_buckets`.

use super::Exchange;
use super::test_helpers::*;
use super::token_bucket::TokenBucket;
use crate::types::{
    AccountId, ExecutionReport, Order, OrderId, OrderType, RejectReason, SelfTradeProtection, Side,
    Symbol, TimeInForce,
};

#[test]
fn rate_limit_default_disabled_in_engine_constructor() {
    // The engine library default is `0/0` (disabled). The CLI in
    // `melin-server` applies its own non-zero default; this test
    // guards against silent flips that would break in-process users.
    let exchange = Exchange::new();
    assert_eq!(exchange.max_orders_per_second(), (0, 0));
}

#[test]
fn rate_limit_zero_rate_or_zero_burst_disables() {
    // Either knob set to zero must short-circuit the limiter — even if
    // the other is huge. This is the documented opt-out.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(0, 1_000_000);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    for i in 0..50u64 {
        execute_at(
            &mut exchange,
            0, // zero clock — would refill 0 tokens if active
            Symbol(1),
            limit_order(i + 1, ACCT_A, Side::Buy, 100 + i, 1, TimeInForce::GTC),
            &mut reports,
        );
    }
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "rate=0 should disable, got {reports:?}"
    );
}

#[test]
fn rate_limit_first_burst_passes_then_rejects() {
    // First-touch initialises the bucket to a full burst, so the first
    // `burst` orders at the same timestamp all succeed; the next must
    // reject.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 5);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    for i in 0..5u64 {
        execute_at(
            &mut exchange,
            1_000,
            Symbol(1),
            limit_order(i + 1, ACCT_A, Side::Buy, 100 + i, 1, TimeInForce::GTC),
            &mut reports,
        );
    }
    assert!(
        reports
            .iter()
            .all(|r| !matches!(r, ExecutionReport::Rejected { .. })),
        "first burst should all rest, got {reports:?}"
    );
    reports.clear();
    // Same timestamp — bucket is empty, no refill possible.
    execute_at(
        &mut exchange,
        1_000,
        Symbol(1),
        limit_order(6, ACCT_A, Side::Buy, 200, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(
        matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ExceedsOrderRate,
                ..
            }
        ),
        "expected ExceedsOrderRate, got {:?}",
        reports[0],
    );
    // No reservation should have been charged for the rejected order.
    let bal = exchange.accounts().balance(ACCT_A, USD);
    // 5 reservations × (price × qty) = 100+101+102+103+104 = 510.
    assert_eq!(bal.reserved, 510);
}

#[test]
fn rate_limit_refill_after_elapsed_time() {
    // Burn the burst, advance the clock past the configured rate, the
    // bucket should refill at least one token and the next order must
    // succeed.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(1_000, 2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    // Burn burst of 2 at t=0.
    for i in 0..2u64 {
        execute_at(
            &mut exchange,
            0,
            Symbol(1),
            limit_order(i + 1, ACCT_A, Side::Buy, 100 + i, 1, TimeInForce::GTC),
            &mut reports,
        );
    }
    // Confirm immediate retry rejects.
    reports.clear();
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(3, ACCT_A, Side::Buy, 200, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsOrderRate,
            ..
        }
    ));
    reports.clear();
    // Advance 1 ms — at 1000/s, that's exactly 1 token refilled.
    execute_at(
        &mut exchange,
        1_000_000, // 1 ms in ns
        Symbol(1),
        limit_order(4, ACCT_A, Side::Buy, 201, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "refill should permit one more order, got {reports:?}"
    );
}

#[test]
fn rate_limit_per_account_independent() {
    // Capping ACCT_A must not affect ACCT_B — each account has its own
    // bucket. Mirrors how SEC-03's open-orders cap is per-account.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    exchange.deposit(ACCT_B, USD, 1_000_000);
    let mut reports = Vec::new();
    // ACCT_A burns its single token at t=0.
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    // ACCT_A retry rejects.
    reports.clear();
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 101, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::ExceedsOrderRate,
            ..
        }
    ));
    // ACCT_B still has a full bucket.
    reports.clear();
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(1, ACCT_B, Side::Buy, 102, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "ACCT_B should be unaffected, got {reports:?}"
    );
}

#[test]
fn rate_limit_runs_after_cap_does_not_shadow_other_rejects() {
    // A duplicate-id order must still report DuplicateOrderId, not
    // ExceedsOrderRate, even when the bucket is empty. The limiter
    // sits *after* dedup and the open-orders cap, so an order that
    // would have rejected for an order-shape reason still reports
    // that reason.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    // Burn the bucket.
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();
    // Resubmit same id — dedup fires, NOT the rate limiter.
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(matches!(
        reports[0],
        ExecutionReport::Rejected {
            reason: RejectReason::DuplicateOrderId,
            ..
        }
    ));
}

#[test]
fn rate_limit_survives_clone_via_snapshot() {
    // The shadow snapshot stage clones via `clone_via_snapshot` and
    // must produce the same Rejected reports as the primary —
    // otherwise shadow validation diverges. Carry the rate config.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(123, 7);
    let cloned = exchange.clone_via_snapshot();
    assert_eq!(cloned.max_orders_per_second(), (123, 7));
}

#[test]
fn rate_limit_no_phantom_credit_after_quiet_period() {
    // Regression test for a bookkeeping bug: when a bucket caps at
    // `burst` after a long idle gap, the wasted-while-full time must
    // not accumulate as `last_refill_ns` lag. If it did, every
    // subsequent close-spaced event would refill to the full burst
    // again, letting the limiter issue far more tokens than `rate`
    // supports. We submit `burst + 1` events with negligible spacing
    // immediately after a 1-second idle period; the (burst+1)th must
    // reject because the bucket can hold exactly `burst` post-quiet.
    let mut exchange = Exchange::new();
    let burst: u32 = 5;
    exchange.set_max_orders_per_second(1_000, burst);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    // Anchor the bucket at t=0 with one event (pulls the first token).
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();
    // 1-second quiet period. Bucket should refill to exactly `burst`
    // — no more, even though 1s of accumulated time at 1000/s would
    // notionally credit 1000 tokens.
    let post_quiet_ns: u64 = 1_000_000_000;
    // Issue exactly `burst` events at 1ns spacing — all should accept
    // (consuming the post-quiet refill).
    for i in 0..burst as u64 {
        execute_at(
            &mut exchange,
            post_quiet_ns + i,
            Symbol(1),
            limit_order(2 + i, ACCT_A, Side::Buy, 100 + i, 1, TimeInForce::GTC),
            &mut reports,
        );
    }
    assert!(
        reports
            .iter()
            .all(|r| !matches!(r, ExecutionReport::Rejected { .. })),
        "first {burst} post-quiet events must all accept, got {reports:?}",
    );
    reports.clear();
    // The (burst+1)th, only `burst` ns after the first, must reject.
    // No phantom credit — the bucket was truly empty after burning
    // `burst` tokens.
    execute_at(
        &mut exchange,
        post_quiet_ns + burst as u64,
        Symbol(1),
        limit_order(
            2 + burst as u64,
            ACCT_A,
            Side::Buy,
            200,
            1,
            TimeInForce::GTC,
        ),
        &mut reports,
    );
    assert_eq!(reports.len(), 1);
    assert!(
        matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ExceedsOrderRate,
                ..
            }
        ),
        "phantom-credit regression: expected ExceedsOrderRate, got {:?}",
        reports[0],
    );
}

#[test]
fn rate_limit_set_clears_existing_buckets() {
    // Changing the rate config must not leave a stale bucket carrying
    // tokens credited at the old rate.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 1);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    // Swap policy. The bucket map is cleared so the next request
    // re-initialises with the new burst.
    exchange.set_max_orders_per_second(100, 3);
    reports.clear();
    for i in 0..3u64 {
        execute_at(
            &mut exchange,
            0,
            Symbol(1),
            limit_order(2 + i, ACCT_A, Side::Buy, 200 + i, 1, TimeInForce::GTC),
            &mut reports,
        );
    }
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "fresh burst after policy change, got {reports:?}"
    );
}

#[test]
fn rate_limit_clock_backwards_is_defensive_not_panic() {
    // `now_ns` is journaled by the reader. If an operator clock step
    // (NTP correction) ever produces an event whose timestamp is
    // earlier than the bucket's `last_refill_ns`, the limiter must
    // not panic and must not reject every order until time catches
    // up — the documented behavior is "skip refill, allow consume."
    // Locks in that contract so a future refactor of
    // `refill_and_consume` doesn't silently regress to either panic
    // (DoS surface) or reject (false-positive surface).
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    // Anchor bucket at t = 1s.
    execute_at(
        &mut exchange,
        1_000_000_000,
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    reports.clear();
    // Clock jumps backward to t = 0. With burst = 2 we had one token
    // left after the t=1s call; the t=0 call must accept (consume
    // the remaining token), not panic and not reject.
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(2, ACCT_A, Side::Buy, 101, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "clock-backwards must allow consume, got {reports:?}",
    );
    reports.clear();
    // Bucket is now at zero (consumed both burst tokens). Another
    // call at t = 0 — still no refill possible — must reject cleanly
    // with ExceedsOrderRate (not panic, not silently accept).
    execute_at(
        &mut exchange,
        0,
        Symbol(1),
        limit_order(3, ACCT_A, Side::Buy, 102, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ExceedsOrderRate,
                ..
            }
        ),
        "expected ExceedsOrderRate after burst exhausted with backwards clock, got {:?}",
        reports[0],
    );
}

/// Place a far-from-market GTD limit order that will rest (never match)
/// and expire at `expiry_ns`. Used by the eviction tests to control
/// when the close-path runs (via `drain_due_scheduled_tasks`).
fn submit_resting_gtd(
    exchange: &mut Exchange,
    now_ns: u64,
    account: AccountId,
    order_id: u64,
    expiry_ns: u64,
    reports: &mut Vec<ExecutionReport>,
) {
    execute_at(
        exchange,
        now_ns,
        Symbol(1),
        Order {
            id: OrderId(order_id),
            account,
            side: Side::Buy,
            order_type: OrderType::Limit {
                price: price(50),
                post_only: false,
            },
            quantity: qty(1),
            time_in_force: TimeInForce::GTD,
            stp: SelfTradeProtection::Allow,
            expiry_ns,
        },
        reports,
    );
}

#[test]
fn rate_limit_bucket_evicted_when_idle_account_refills_to_full() {
    // Memory bound: the bucket map must drain entries for accounts
    // whose buckets have refilled to capacity AND have no open
    // orders left. Otherwise `order_buckets` grows monotonically
    // with every ever-active account.
    //
    // Driving the close path via GTD expiry lets us advance the
    // event clock past the refill window in a single step:
    // `drain_due_scheduled_tasks` stamps `current_event_ts_ns`
    // before invoking the eviction probe.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    submit_resting_gtd(
        &mut exchange,
        1_000_000_000,
        ACCT_A,
        1,
        1_100_000_000,
        &mut reports,
    );
    assert_eq!(exchange.order_bucket_count(), 1);
    // Drain past expiry + well past the 20ms refill window for burst=2
    // at 100/s. Eviction probe sees an at-capacity bucket and removes it.
    exchange.drain_due_scheduled_tasks(10_000_000_000, &mut reports);
    assert_eq!(exchange.open_order_count(ACCT_A), 0);
    assert_eq!(
        exchange.order_bucket_count(),
        0,
        "at-capacity bucket on a closed account should be evicted",
    );
}

#[test]
fn rate_limit_bucket_not_evicted_when_below_capacity() {
    // The eviction policy is "at full capacity" — partial buckets
    // must stay so an account can't escape an in-progress throttle
    // by cancelling all its orders. Without this guard, a hot
    // account could cycle submit → cancel → fresh-burst-on-next.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    // Submit consumes one token (bucket: 2→1). Cancel runs the
    // eviction probe at the same timestamp — refill earns 0 tokens,
    // bucket stays at 1, NOT evicted.
    execute_at(
        &mut exchange,
        1_000_000_000,
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    exchange.set_current_event_ts_ns(1_000_000_000);
    exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
    assert_eq!(exchange.open_order_count(ACCT_A), 0);
    assert_eq!(
        exchange.order_bucket_count(),
        1,
        "partial bucket must survive close-to-zero so the throttle holds",
    );
}

#[test]
fn rate_limit_eviction_is_observationally_equivalent_to_full_bucket() {
    // The "evict only at capacity" rule is justified by saying a
    // fresh post-eviction bucket is observationally identical to a
    // kept full bucket. Lock in that contract: after eviction, an
    // account must see exactly the same accept/reject sequence as
    // it would if its bucket had been preserved.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    submit_resting_gtd(
        &mut exchange,
        1_000_000_000,
        ACCT_A,
        1,
        1_100_000_000,
        &mut reports,
    );
    exchange.drain_due_scheduled_tasks(10_000_000_000, &mut reports);
    assert_eq!(exchange.order_bucket_count(), 0, "precondition: evicted");

    // First two submits at a fresh timestamp must accept (full burst);
    // the third at the same timestamp must reject — same as if a
    // kept-but-refilled bucket had been at capacity.
    reports.clear();
    for i in 0..2u64 {
        execute_at(
            &mut exchange,
            10_000_000_000,
            Symbol(1),
            limit_order(2 + i, ACCT_A, Side::Buy, 100 + i, 1, TimeInForce::GTC),
            &mut reports,
        );
    }
    assert!(
        !reports
            .iter()
            .any(|r| matches!(r, ExecutionReport::Rejected { .. })),
        "post-eviction first-touch should see full burst, got {reports:?}",
    );
    reports.clear();
    execute_at(
        &mut exchange,
        10_000_000_000,
        Symbol(1),
        limit_order(99, ACCT_A, Side::Buy, 200, 1, TimeInForce::GTC),
        &mut reports,
    );
    assert!(
        matches!(
            reports[0],
            ExecutionReport::Rejected {
                reason: RejectReason::ExceedsOrderRate,
                ..
            }
        ),
        "third order must throttle: got {:?}",
        reports[0],
    );
}

#[test]
fn rate_limit_bucket_preserved_across_disable_reenable() {
    // `set_max_orders_per_second(0, _)` is a "deactivation" transition
    // that intentionally preserves bucket state for re-activation.
    // The eviction probe must respect that — otherwise an account
    // that closes its last order during the disabled window silently
    // loses its preserved throttle state, defeating the documented
    // contract.
    let mut exchange = Exchange::new();
    exchange.set_max_orders_per_second(100, 2);
    exchange.add_instrument(btc_usd_spec());
    exchange.deposit(ACCT_A, USD, 1_000_000);
    let mut reports = Vec::new();
    // Drive bucket below capacity so we can detect erosion.
    execute_at(
        &mut exchange,
        1_000_000_000,
        Symbol(1),
        limit_order(1, ACCT_A, Side::Buy, 100, 1, TimeInForce::GTC),
        &mut reports,
    );
    let buckets_before = exchange.order_bucket_count();
    assert_eq!(buckets_before, 1);
    // Deactivate. Per `set_max_orders_per_second` doc: buckets preserved.
    exchange.set_max_orders_per_second(0, 0);
    // Now run a close path that would otherwise evict. The probe
    // must short-circuit on `rate == 0` and leave the bucket alone.
    exchange.set_current_event_ts_ns(2_000_000_000);
    exchange.cancel(Symbol(1), ACCT_A, OrderId(1), &mut reports);
    assert_eq!(
        exchange.order_bucket_count(),
        1,
        "deactivation preserves buckets — close path must not erode them",
    );
}

#[test]
fn rate_limit_refill_clamps_tokens_above_burst() {
    // A tampered snapshot, a primary/replica burst-config mismatch,
    // or any future bug producing `tokens > burst` must NOT grant
    // unbounded credit. Without the clamp at the top of `refill`,
    // a bucket with `tokens = u64::MAX` and `last_refill_ns` in the
    // future would skip the elapsed-time branch and let the caller
    // drain ~u64::MAX orders before reaching zero.
    let mut bucket = TokenBucket {
        tokens: u64::MAX,
        // In the future relative to the call — refill's elapsed-time
        // branch is skipped, exercising ONLY the new defensive clamp.
        last_refill_ns: 10_000_000_000,
    };
    let burst: u32 = 5;
    let rate: u32 = 100;
    let allowed = bucket.refill_and_consume(1_000_000_000, rate, burst);
    assert!(allowed, "first call should consume one token");
    assert_eq!(
        bucket.tokens,
        burst as u64 - 1,
        "tokens must be clamped to burst before consume",
    );
    // Drain the rest of the burst.
    for _ in 0..(burst - 1) {
        assert!(bucket.refill_and_consume(1_000_000_000, rate, burst));
    }
    assert!(
        !bucket.refill_and_consume(1_000_000_000, rate, burst),
        "after burst orders the bucket must reject — no phantom credit from u64::MAX",
    );
}

#[test]
fn rate_limit_refill_saturation_grants_full_burst() {
    // The u64 saturating_mul rewrite (replacing the u128 intermediate)
    // is correct iff overflow of `elapsed * rate` produces u64::MAX,
    // which after /1e9 still exceeds any u32 burst — so .min(burst)
    // yields the same answer the u128 version would have produced
    // (a fully-refilled bucket). Drive that path directly: at the
    // largest legal rate and a near-u64::MAX elapsed time, the
    // multiply saturates and the bucket must end up at exactly
    // `burst` tokens (no fewer due to truncation, no more due to
    // forgotten clamp).
    let mut bucket = TokenBucket {
        tokens: 0,
        last_refill_ns: 0,
    };
    let burst: u32 = 1000;
    let rate: u32 = u32::MAX;
    // now_ns chosen so `elapsed * rate` overflows u64
    // (u64::MAX / u32::MAX ≈ 4.29e9 — pick larger).
    bucket.refill(u64::MAX, rate, burst);
    assert_eq!(
        bucket.tokens, burst as u64,
        "saturation path must grant exactly `burst` tokens",
    );
    assert_eq!(
        bucket.last_refill_ns,
        u64::MAX,
        "bucket-at-capacity branch must snap last_refill_ns to now",
    );
}
