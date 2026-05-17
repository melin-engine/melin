//! `Application` impl for the trading engine.
//!
//! `melin-engine` owns the matching domain (`Exchange`) and knows nothing
//! about the LMAX transport pipeline. The transport's `Application`
//! contract lives in `melin-app`, and `melin-server` is what wires the
//! two together — so the trait impl lives here, on a thin newtype around
//! `Exchange` that satisfies the orphan rule.
//!
//! The newtype is transparent: `Deref`/`DerefMut` forward every non-trait
//! call to the inner `Exchange`, so callers that need direct engine
//! methods (`set_max_orders_per_second`, `add_instrument`, etc.) keep
//! their existing call sites unchanged.

use std::io::{self, Read, Write};
use std::ops::{Deref, DerefMut};

use melin_app::{Application, ApplyCtx, RejectReason as TransportRejectReason};
use melin_engine::exchange::Exchange;
use melin_engine::snapshot as engine_snapshot;
use melin_trading::trading_event::TradingEvent;
use melin_types::types::{
    AccountId, ExecutionReport, OrderId, QueryResponse, RejectReason as EngineRejectReason, Symbol,
};

// Hot-path size budget. Disruptor slots are copied by value on every
// publish/consume — growing these silently would tax cache footprint
// across the whole pipeline. A prior review caught `ExecutionReport`
// ballooning from 64 B → 392 B via an inlined `Position` variant; these
// assertions would have failed at compile time and tripped CI.
// Numbers match the layout on x86_64 Linux; bump deliberately if a
// genuine field addition requires it.
//
// Skipped under `latency-trace` because the trace timestamps grow each
// slot by 16 bytes — that growth is deliberate and the feature is
// dev/bench only, not the production cache footprint we're guarding.
#[cfg(not(feature = "latency-trace"))]
const _: () = assert!(size_of::<melin_transport_core::pipeline::InputSlot<TradingEvent>>() == 104);
// Bumped from 416 → 424 (one extra u64) when `OutputSlot.wire_seq` was
// added so the response stage's durability gate can compare against
// replica metrics in wire-seq space rather than the unsound local-vs-wire
// mix that previously let the gate open on un-replicated events on a
// recovered primary. Correctness > footprint here.
#[cfg(not(feature = "latency-trace"))]
const _: () = assert!(
    size_of::<melin_transport_core::pipeline::OutputSlot<ExecutionReport, QueryResponse>>() == 424
);
const _: () = assert!(size_of::<melin_journal::JournalEvent<TradingEvent>>() == 64);
const _: () = assert!(size_of::<ExecutionReport>() == 64);

/// Transparent newtype around [`Exchange`] that carries the
/// `Application` trait impl. Exists solely so the impl can live in
/// `melin-server` (the wiring crate) without violating the orphan rule —
/// neither `Application` (in `melin-app`) nor `Exchange` (in
/// `melin-engine`) is local to `melin-server`, but `ServerApp` is.
///
/// The inner field is `pub` because the server frequently constructs an
/// `Exchange` directly (`Exchange::with_capacity`, `with_seed_capacity`)
/// and wraps it; making the wrap explicit at every construction site is
/// cheaper than introducing a parallel set of constructors here.
pub struct ServerApp(pub Exchange);

impl ServerApp {
    /// Construct a `ServerApp` wrapping a freshly-initialised `Exchange`.
    /// Convenience for tests and bootstrap paths that want the default
    /// `Exchange::new()` sizing without spelling the wrap.
    pub fn new() -> Self {
        ServerApp(Exchange::new())
    }
}

impl Default for ServerApp {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for ServerApp {
    type Target = Exchange;

    #[inline]
    fn deref(&self) -> &Exchange {
        &self.0
    }
}

impl DerefMut for ServerApp {
    #[inline]
    fn deref_mut(&mut self) -> &mut Exchange {
        &mut self.0
    }
}

impl Application for ServerApp {
    type Event = TradingEvent;
    type Report = ExecutionReport;
    type QueryResponse = QueryResponse;

    /// Schema version for the snapshot payload. Tracks the underlying
    /// `snapshot` module's `PAYLOAD_VERSION` — any change there forces a
    /// bump here too, surfaced through the transport-owned frame.
    const APP_VERSION: u16 = engine_snapshot::PAYLOAD_VERSION;

    /// Thin dispatcher over `TradingEvent`. Marked `#[inline]` so the
    /// matching stage's monomorphised hot loop can see through to each
    /// concrete `Exchange` method: the inner methods (`execute`, `cancel`,
    /// …) own the real work and keep their own inlining attrs.
    #[inline]
    fn apply(
        &mut self,
        event: Self::Event,
        ctx: &ApplyCtx,
        out: &mut Vec<Self::Report>,
    ) -> Option<Self::QueryResponse> {
        // Stash the journaled event timestamp so per-event methods
        // (`execute` and friends) can read a deterministic clock for the
        // SEC-04 rate limiter without taking a `now_ns` parameter. Set
        // unconditionally so the value reflects exactly the event being
        // applied — no risk of reading a stale stamp from an earlier event.
        self.0.set_current_event_ts_ns(ctx.now_ns);
        match event {
            TradingEvent::AddInstrument { spec } => {
                self.0.add_instrument(spec);
                None
            }
            TradingEvent::Deposit {
                account,
                currency,
                amount,
            } => {
                self.0.deposit(account, currency, amount);
                None
            }
            TradingEvent::SubmitOrder { symbol, order } => {
                self.0.execute(symbol, order, out);
                None
            }
            TradingEvent::CancelOrder {
                symbol,
                account,
                order_id,
            } => {
                self.0.cancel(symbol, account, order_id, out);
                None
            }
            TradingEvent::SetRiskLimits { symbol, limits } => {
                self.0.set_risk_limits(symbol, limits);
                None
            }
            TradingEvent::CancelAll { account } => {
                self.0.cancel_all(account, out);
                None
            }
            TradingEvent::SetCircuitBreaker { symbol, config } => {
                self.0.set_circuit_breaker(symbol, config);
                None
            }
            TradingEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                self.0
                    .cancel_replace(symbol, account, order_id, new_price, new_quantity, out);
                None
            }
            TradingEvent::SetFeeSchedule { symbol, schedule } => {
                self.0.set_fee_schedule(symbol, schedule, out);
                None
            }
            TradingEvent::ProvisionAccount { account, amount } => {
                self.0.provision_account(account, amount);
                None
            }
            TradingEvent::Withdraw {
                account,
                currency,
                amount,
            } => {
                // Withdraw rejections (insufficient balance, unknown
                // account, has resting orders) are deliberately dropped
                // today — the engine still applies any state changes
                // deterministically on both primary and replica, but the
                // client receives no `Rejected` report. Tracked on the
                // roadmap as "Withdraw rejections silently dropped in
                // the pipeline".
                let _ = self.0.withdraw(account, currency, amount);
                None
            }
            TradingEvent::EndOfDay => {
                self.0.end_of_day(out);
                None
            }
            TradingEvent::DisableInstrument { symbol } => {
                self.0.disable_instrument(symbol, out);
                None
            }
            TradingEvent::EnableInstrument { symbol } => {
                self.0.enable_instrument(symbol, out);
                None
            }
            TradingEvent::RemoveInstrument { symbol } => {
                self.0.remove_instrument(symbol, out);
                None
            }
            TradingEvent::QueryStats => {
                // Read-only query: the transport owns the counters, so
                // the app synthesises the report directly from the
                // `ApplyCtx` it was handed. No `Exchange` state touched.
                Some(QueryResponse::Stats {
                    active_connections: ctx.active_connections,
                    events_processed: ctx.events_processed,
                    journal_sequence: ctx.journal_sequence,
                })
            }
            TradingEvent::QueryPosition { account } => {
                let (balances, count) = self.0.accounts().balances_for(account);
                Some(QueryResponse::Position {
                    account,
                    balances,
                    count,
                })
            }
            TradingEvent::QueryRequestSeq => {
                // Self-introspection: read the dedup HWM for the
                // calling connection's key (transport-supplied via
                // `ApplyCtx`). The event itself carries no identity,
                // so a client cannot ask about other keys.
                Some(QueryResponse::RequestSeqHwm {
                    hwm: self.0.request_seq_hwm(ctx.key_hash),
                })
            }
        }
    }

    #[inline]
    fn tick(&mut self, now_ns: u64, out: &mut Vec<Self::Report>) {
        self.0.drain_due_scheduled_tasks(now_ns, out);
    }

    #[inline]
    fn check_request_seq(&mut self, key_hash: u64, seq: u64) -> bool {
        Exchange::check_request_seq(&mut self.0, key_hash, seq)
    }

    /// Route through `Exchange::prefault`, which walks the pre-allocated
    /// slabs and indices so the first hot-path access after startup
    /// doesn't soft-fault. Avoids the default snapshot-round-trip
    /// implementation on a cold allocator.
    fn prefault(&mut self) {
        Exchange::prefault(&mut self.0);
    }

    /// `Exchange` exposes an in-memory `clone_via_snapshot` that skips
    /// the byte serialisation — faster than the default
    /// serialise-then-deserialise path. Keep the optimisation for the
    /// shadow-snapshot stage.
    fn clone_via_snapshot(&self) -> std::io::Result<Self> {
        Ok(ServerApp(Exchange::clone_via_snapshot(&self.0)))
    }

    fn build_reject(event: &Self::Event, reason: TransportRejectReason) -> Self::Report {
        let engine_reason = match reason {
            TransportRejectReason::DuplicateRequest => EngineRejectReason::DuplicateRequest,
            TransportRejectReason::ReplicaDisconnected => EngineRejectReason::ReplicaDisconnected,
        };
        ExecutionReport::Rejected {
            order_id: extract_order_id(event),
            symbol: extract_symbol(event),
            account: extract_account_id(event),
            reason: engine_reason,
        }
    }

    /// Writes the engine payload bytes verbatim. The transport stores
    /// `APP_VERSION` in its frame and rejects mismatching files before
    /// `restore` is ever called, so duplicating the version in the
    /// payload would be unreachable. If multi-version migration ever
    /// lands, drop the transport-side `APP_VERSION` check and reintroduce
    /// an in-payload version prefix here.
    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let bytes = engine_snapshot::encode_exchange_payload(&self.0);
        w.write_all(&bytes)
    }

    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut bytes = Vec::new();
        r.read_to_end(&mut bytes)?;
        engine_snapshot::decode_exchange_payload(&bytes)
            .map(ServerApp)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// Order ID attached to reject reports, or `OrderId(0)` if the variant
/// does not carry one. Mirrors `journal::pipeline::MatchingStage::extract_order_id`
/// so the reject-report shape stays consistent across the pipeline.
fn extract_order_id(event: &TradingEvent) -> OrderId {
    match event {
        TradingEvent::SubmitOrder { order, .. } => order.id,
        TradingEvent::CancelOrder { order_id, .. }
        | TradingEvent::CancelReplace { order_id, .. } => *order_id,
        _ => OrderId(0),
    }
}

fn extract_account_id(event: &TradingEvent) -> AccountId {
    match event {
        TradingEvent::SubmitOrder { order, .. } => order.account,
        TradingEvent::CancelOrder { account, .. }
        | TradingEvent::CancelAll { account }
        | TradingEvent::CancelReplace { account, .. }
        | TradingEvent::Deposit { account, .. }
        | TradingEvent::Withdraw { account, .. }
        | TradingEvent::ProvisionAccount { account, .. }
        | TradingEvent::QueryPosition { account } => *account,
        _ => AccountId(0),
    }
}

fn extract_symbol(event: &TradingEvent) -> Symbol {
    match event {
        TradingEvent::SubmitOrder { symbol, .. }
        | TradingEvent::CancelOrder { symbol, .. }
        | TradingEvent::CancelReplace { symbol, .. }
        | TradingEvent::SetRiskLimits { symbol, .. }
        | TradingEvent::SetCircuitBreaker { symbol, .. }
        | TradingEvent::SetFeeSchedule { symbol, .. }
        | TradingEvent::DisableInstrument { symbol }
        | TradingEvent::EnableInstrument { symbol }
        | TradingEvent::RemoveInstrument { symbol } => *symbol,
        _ => Symbol(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::Cursor;
    use std::num::NonZeroU64;

    use melin_types::types::{
        CurrencyId, InstrumentSpec, Order, OrderType, Price, Quantity, SelfTradeProtection, Side,
        TimeInForce,
    };

    fn price(p: u64) -> Price {
        Price(NonZeroU64::new(p).unwrap())
    }
    fn qty(q: u64) -> Quantity {
        Quantity(NonZeroU64::new(q).unwrap())
    }

    /// A freshly-constructed `ServerApp` with one registered instrument
    /// and a deposited account. Enough to exercise the full `apply` path.
    fn seeded_app() -> ServerApp {
        let mut ex = Exchange::new();
        ex.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(1),
            quote: CurrencyId(2),
        });
        ex.deposit(AccountId(1), CurrencyId(2), 1_000_000);
        ServerApp(ex)
    }

    #[test]
    fn apply_submit_order_produces_placed_report() {
        let mut app = seeded_app();
        let mut reports = Vec::new();
        let ctx = ApplyCtx {
            now_ns: 0,
            journal_sequence: 0,
            active_connections: 0,
            events_processed: 0,
            key_hash: 0,
        };
        let ev = TradingEvent::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(1),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                quantity: qty(10),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
        };
        <ServerApp as Application>::apply(&mut app, ev, &ctx, &mut reports);
        assert!(
            !reports.is_empty(),
            "apply should emit at least one report for a resting order"
        );
    }

    #[test]
    fn tick_advances_scheduler_clock() {
        // No scheduled tasks yet — just assert the method is callable via
        // the trait without panicking. Real scheduler exercise is covered
        // by exchange.rs unit tests.
        let mut app = ServerApp(Exchange::new());
        let mut reports = Vec::new();
        <ServerApp as Application>::tick(&mut app, 1_000_000_000, &mut reports);
        assert!(reports.is_empty());
    }

    #[test]
    fn apply_query_request_seq_returns_per_key_hwm() {
        let mut app = seeded_app();

        // Advance two distinct keys to different HWMs via the dedup gate.
        // Same key+seq combinations the live pipeline would emit.
        let key_a: u64 = 0xAAAA_AAAA_AAAA_AAAA;
        let key_b: u64 = 0xBBBB_BBBB_BBBB_BBBB;
        for seq in 1..=7 {
            assert!(<ServerApp as Application>::check_request_seq(
                &mut app, key_a, seq
            ));
        }
        for seq in 1..=3 {
            assert!(<ServerApp as Application>::check_request_seq(
                &mut app, key_b, seq
            ));
        }

        let mut reports = Vec::new();
        let mk_ctx = |kh| ApplyCtx {
            now_ns: 0,
            journal_sequence: 0,
            active_connections: 0,
            events_processed: 0,
            key_hash: kh,
        };

        // Each key sees only its own HWM — the engine reads ctx.key_hash,
        // not anything from the (payloadless) event itself.
        let resp_a = <ServerApp as Application>::apply(
            &mut app,
            TradingEvent::QueryRequestSeq,
            &mk_ctx(key_a),
            &mut reports,
        );
        assert_eq!(resp_a, Some(QueryResponse::RequestSeqHwm { hwm: 7 }));

        let resp_b = <ServerApp as Application>::apply(
            &mut app,
            TradingEvent::QueryRequestSeq,
            &mk_ctx(key_b),
            &mut reports,
        );
        assert_eq!(resp_b, Some(QueryResponse::RequestSeqHwm { hwm: 3 }));

        // A key with no prior activity reads back as zero.
        let resp_unknown = <ServerApp as Application>::apply(
            &mut app,
            TradingEvent::QueryRequestSeq,
            &mk_ctx(0xDEAD_BEEF),
            &mut reports,
        );
        assert_eq!(resp_unknown, Some(QueryResponse::RequestSeqHwm { hwm: 0 }));

        // Query is read-only: HWMs are unchanged after the queries above.
        assert_eq!(app.0.request_seq_hwm(key_a), 7);
        assert_eq!(app.0.request_seq_hwm(key_b), 3);
    }

    #[test]
    fn check_request_seq_rejects_duplicates() {
        let mut app = ServerApp(Exchange::new());
        assert!(<ServerApp as Application>::check_request_seq(
            &mut app, 42, 1
        ));
        assert!(<ServerApp as Application>::check_request_seq(
            &mut app, 42, 2
        ));
        assert!(!<ServerApp as Application>::check_request_seq(
            &mut app, 42, 2
        ));
        assert!(!<ServerApp as Application>::check_request_seq(
            &mut app, 42, 1
        ));
    }

    #[test]
    fn build_reject_maps_transport_reasons() {
        let ev = TradingEvent::SubmitOrder {
            symbol: Symbol(7),
            order: Order {
                id: OrderId(42),
                account: AccountId(3),
                side: Side::Buy,
                order_type: OrderType::Market,
                quantity: qty(1),
                time_in_force: TimeInForce::IOC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
        };
        let r =
            <ServerApp as Application>::build_reject(&ev, TransportRejectReason::DuplicateRequest);
        match r {
            ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason,
            } => {
                assert_eq!(order_id, OrderId(42));
                assert_eq!(symbol, Symbol(7));
                assert_eq!(account, AccountId(3));
                assert_eq!(reason, EngineRejectReason::DuplicateRequest);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }

        let r = <ServerApp as Application>::build_reject(
            &TradingEvent::CancelAll {
                account: AccountId(9),
            },
            TransportRejectReason::ReplicaDisconnected,
        );
        match r {
            ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason,
            } => {
                assert_eq!(order_id, OrderId(0));
                assert_eq!(symbol, Symbol(0));
                assert_eq!(account, AccountId(9));
                assert_eq!(reason, EngineRejectReason::ReplicaDisconnected);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_restore_round_trip_preserves_state() {
        let mut before = seeded_app();
        let mut reports = Vec::new();
        // Submit a resting order so there's non-trivial book state to
        // round-trip through the snapshot.
        before.0.execute(
            Symbol(1),
            Order {
                id: OrderId(1),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(100),
                    post_only: false,
                },
                quantity: qty(10),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports,
        );
        let reports_before = reports.clone();

        let mut buf = Vec::new();
        <ServerApp as Application>::snapshot(&before, &mut buf).expect("snapshot");

        let mut cursor = Cursor::new(buf);
        let mut after = <ServerApp as Application>::restore(&mut cursor).expect("restore");

        // Placing an additional order against both and comparing the
        // emitted reports is a cheap proxy for structural equality —
        // the restored book must match price-time priority.
        let mut reports_after = reports_before.clone();
        reports_after.clear();
        after.0.execute(
            Symbol(1),
            Order {
                id: OrderId(2),
                account: AccountId(1),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: price(99),
                    post_only: false,
                },
                quantity: qty(5),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
            &mut reports_after,
        );
        assert!(
            !reports_after.is_empty(),
            "restored exchange must accept orders"
        );
    }
}
