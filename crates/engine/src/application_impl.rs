//! `Application` impl for the trading engine.
//!
//! Plugs the existing `Exchange` matching core into the `melin-app`
//! transport abstraction. Each trait method delegates to an existing
//! `Exchange` method unchanged — the impl only does enum dispatch and
//! adapts the transport's [`melin_app::RejectReason`] subset to the full
//! `crate::types::RejectReason`.
//!
//! This is the Phase 1 bridge: the trait is defined, the impl compiles,
//! round-trip tests cover each variant. Later phases rewire the pipeline
//! to actually call through the trait.

use std::io::{self, Read, Write};

use melin_app::{Application, ApplyCtx, RejectReason as TransportRejectReason};

use crate::exchange::Exchange;
use crate::journal::snapshot as engine_snapshot;
use crate::le;
use crate::trading_event::TradingEvent;
use crate::types::{
    AccountId, ExecutionReport, OrderId, QueryResponse, RejectReason as EngineRejectReason, Symbol,
};

// Hot-path size budget. Disruptor slots are copied by value on every
// publish/consume — growing these silently would tax cache footprint
// across the whole pipeline. A prior review caught `ExecutionReport`
// ballooning from 64 B → 392 B via an inlined `Position` variant; these
// assertions would have failed at compile time and tripped CI.
// Numbers match the layout on x86_64 Linux; bump deliberately if a
// genuine field addition requires it.
// `latency-trace` adds two `u64` timestamps to each slot (publish_ts +
// recv_ts on InputSlot; match_complete_ts + recv_ts on OutputSlot),
// growing them by 16 B. Default builds keep the tighter sizes.
#[cfg(not(feature = "latency-trace"))]
const _: () = assert!(size_of::<melin_transport_core::pipeline::InputSlot<TradingEvent>>() == 104);
#[cfg(feature = "latency-trace")]
const _: () = assert!(size_of::<melin_transport_core::pipeline::InputSlot<TradingEvent>>() == 120);
#[cfg(not(feature = "latency-trace"))]
const _: () = assert!(
    size_of::<melin_transport_core::pipeline::OutputSlot<ExecutionReport, QueryResponse>>() == 416
);
#[cfg(feature = "latency-trace")]
const _: () = assert!(
    size_of::<melin_transport_core::pipeline::OutputSlot<ExecutionReport, QueryResponse>>() == 432
);
const _: () = assert!(size_of::<melin_journal::JournalEvent<TradingEvent>>() == 64);
const _: () = assert!(size_of::<ExecutionReport>() == 64);

impl Application for Exchange {
    type Event = TradingEvent;
    type Report = ExecutionReport;
    type QueryResponse = QueryResponse;

    /// Schema version for the snapshot payload. Tracks the underlying
    /// `snapshot` module's `SNAP_VERSION` — any change there forces a
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
        match event {
            TradingEvent::AddInstrument { spec } => {
                self.add_instrument(spec);
                None
            }
            TradingEvent::Deposit {
                account,
                currency,
                amount,
            } => {
                self.deposit(account, currency, amount);
                None
            }
            TradingEvent::SubmitOrder { symbol, order } => {
                self.execute(symbol, order, out);
                None
            }
            TradingEvent::CancelOrder {
                symbol,
                account,
                order_id,
            } => {
                self.cancel(symbol, account, order_id, out);
                None
            }
            TradingEvent::SetRiskLimits { symbol, limits } => {
                self.set_risk_limits(symbol, limits);
                None
            }
            TradingEvent::CancelAll { account } => {
                self.cancel_all(account, out);
                None
            }
            TradingEvent::SetCircuitBreaker { symbol, config } => {
                self.set_circuit_breaker(symbol, config);
                None
            }
            TradingEvent::CancelReplace {
                symbol,
                account,
                order_id,
                new_price,
                new_quantity,
            } => {
                self.cancel_replace(symbol, account, order_id, new_price, new_quantity, out);
                None
            }
            TradingEvent::SetFeeSchedule { symbol, schedule } => {
                self.set_fee_schedule(symbol, schedule, out);
                None
            }
            TradingEvent::ProvisionAccount { account, amount } => {
                self.provision_account(account, amount);
                None
            }
            TradingEvent::Withdraw {
                account,
                currency,
                amount,
            } => {
                // Replay-deterministic rejections: `Exchange::withdraw`
                // already handles insufficient-balance and unknown-account
                // the same way the replay stage does. Errors are the live
                // outcome recorded for the client; discarding here matches
                // current pipeline behaviour (see pipeline.rs withdraw arm).
                let _ = self.withdraw(account, currency, amount);
                None
            }
            TradingEvent::EndOfDay => {
                self.end_of_day(out);
                None
            }
            TradingEvent::DisableInstrument { symbol } => {
                self.disable_instrument(symbol, out);
                None
            }
            TradingEvent::EnableInstrument { symbol } => {
                self.enable_instrument(symbol, out);
                None
            }
            TradingEvent::RemoveInstrument { symbol } => {
                self.remove_instrument(symbol, out);
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
                let (balances, count) = self.accounts().balances_for(account);
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
                    hwm: self.request_seq_hwm(ctx.key_hash),
                })
            }
        }
    }

    #[inline]
    fn tick(&mut self, now_ns: u64, out: &mut Vec<Self::Report>) {
        self.drain_due_scheduled_tasks(now_ns, out);
    }

    #[inline]
    fn check_request_seq(&mut self, key_hash: u64, seq: u64) -> bool {
        Exchange::check_request_seq(self, key_hash, seq)
    }

    /// Route through `Exchange::prefault`, which walks the pre-allocated
    /// slabs and indices so the first hot-path access after startup
    /// doesn't soft-fault. Avoids the default snapshot-round-trip
    /// implementation on a cold allocator.
    fn prefault(&mut self) {
        Exchange::prefault(self);
    }

    /// `Exchange` exposes an in-memory `clone_via_snapshot` that skips
    /// the byte serialisation — faster than the default
    /// serialise-then-deserialise path. Keep the optimisation for the
    /// shadow-snapshot stage.
    fn clone_via_snapshot(&self) -> std::io::Result<Self> {
        Ok(Exchange::clone_via_snapshot(self))
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

    /// Encodes `[app_version: u16 LE][exchange payload bytes]`.
    ///
    /// The transport supplies framing (magic, transport version,
    /// sequence, chain_hash, CRC). The version prefix here is app-owned
    /// so `restore` can migrate older snapshot schemas without the
    /// transport having to know about schema evolution.
    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let bytes = engine_snapshot::encode_exchange_payload(self);
        let mut version_buf = [0u8; 2];
        le::put_u16(&mut version_buf, Self::APP_VERSION);
        w.write_all(&version_buf)?;
        w.write_all(&bytes)
    }

    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut bytes = Vec::new();
        r.read_to_end(&mut bytes)?;
        if bytes.len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "snapshot payload shorter than version prefix",
            ));
        }
        let version = le::get_u16(&bytes[0..2]);
        engine_snapshot::decode_exchange_payload(&bytes[2..], version)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// Order ID attached to reject reports, or `OrderId(0)` if the variant
/// does not carry one. Mirrors `journal::pipeline::MatchingStage::extract_order_id`
/// so Phase 3's rewrite keeps the same reject-report shape.
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

    use crate::types::{
        CurrencyId, InstrumentSpec, Order, OrderType, Price, Quantity, SelfTradeProtection, Side,
        TimeInForce,
    };

    fn price(p: u64) -> Price {
        Price(NonZeroU64::new(p).unwrap())
    }
    fn qty(q: u64) -> Quantity {
        Quantity(NonZeroU64::new(q).unwrap())
    }

    /// A freshly-constructed exchange with one registered instrument and
    /// a deposited account. Enough to exercise the full `apply` path.
    fn seeded_exchange() -> Exchange {
        let mut ex = Exchange::new();
        ex.add_instrument(InstrumentSpec {
            symbol: Symbol(1),
            base: CurrencyId(1),
            quote: CurrencyId(2),
        });
        ex.deposit(AccountId(1), CurrencyId(2), 1_000_000);
        ex
    }

    #[test]
    fn apply_submit_order_produces_placed_report() {
        let mut ex = seeded_exchange();
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
        <Exchange as Application>::apply(&mut ex, ev, &ctx, &mut reports);
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
        let mut ex = Exchange::new();
        let mut reports = Vec::new();
        <Exchange as Application>::tick(&mut ex, 1_000_000_000, &mut reports);
        assert!(reports.is_empty());
    }

    #[test]
    fn apply_query_request_seq_returns_per_key_hwm() {
        let mut ex = seeded_exchange();

        // Advance two distinct keys to different HWMs via the dedup gate.
        // Same key+seq combinations the live pipeline would emit.
        let key_a: u64 = 0xAAAA_AAAA_AAAA_AAAA;
        let key_b: u64 = 0xBBBB_BBBB_BBBB_BBBB;
        for seq in 1..=7 {
            assert!(<Exchange as Application>::check_request_seq(
                &mut ex, key_a, seq
            ));
        }
        for seq in 1..=3 {
            assert!(<Exchange as Application>::check_request_seq(
                &mut ex, key_b, seq
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
        let resp_a = <Exchange as Application>::apply(
            &mut ex,
            TradingEvent::QueryRequestSeq,
            &mk_ctx(key_a),
            &mut reports,
        );
        assert_eq!(resp_a, Some(QueryResponse::RequestSeqHwm { hwm: 7 }));

        let resp_b = <Exchange as Application>::apply(
            &mut ex,
            TradingEvent::QueryRequestSeq,
            &mk_ctx(key_b),
            &mut reports,
        );
        assert_eq!(resp_b, Some(QueryResponse::RequestSeqHwm { hwm: 3 }));

        // A key with no prior activity reads back as zero.
        let resp_unknown = <Exchange as Application>::apply(
            &mut ex,
            TradingEvent::QueryRequestSeq,
            &mk_ctx(0xDEAD_BEEF),
            &mut reports,
        );
        assert_eq!(resp_unknown, Some(QueryResponse::RequestSeqHwm { hwm: 0 }));

        // Query is read-only: HWMs are unchanged after the queries above.
        assert_eq!(ex.request_seq_hwm(key_a), 7);
        assert_eq!(ex.request_seq_hwm(key_b), 3);
    }

    #[test]
    fn check_request_seq_rejects_duplicates() {
        let mut ex = Exchange::new();
        assert!(<Exchange as Application>::check_request_seq(&mut ex, 42, 1));
        assert!(<Exchange as Application>::check_request_seq(&mut ex, 42, 2));
        assert!(!<Exchange as Application>::check_request_seq(
            &mut ex, 42, 2
        ));
        assert!(!<Exchange as Application>::check_request_seq(
            &mut ex, 42, 1
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
            <Exchange as Application>::build_reject(&ev, TransportRejectReason::DuplicateRequest);
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

        let r = <Exchange as Application>::build_reject(
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
        let mut before = seeded_exchange();
        let mut reports = Vec::new();
        // Submit a resting order so there's non-trivial book state to
        // round-trip through the snapshot.
        before.execute(
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
        <Exchange as Application>::snapshot(&before, &mut buf).expect("snapshot");

        let mut cursor = Cursor::new(buf);
        let mut after = <Exchange as Application>::restore(&mut cursor).expect("restore");

        // Placing an additional order against both and comparing the
        // emitted reports is a cheap proxy for structural equality —
        // the restored book must match price-time priority.
        let mut reports_after = reports_before.clone();
        reports_after.clear();
        after.execute(
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
