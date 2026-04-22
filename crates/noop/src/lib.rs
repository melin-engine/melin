//! No-op [`Application`] for the Melin durable transport.
//!
//! `NoopApp` implements the `melin-app` contract with trading-compatible
//! wire types (`TradingEvent` as the event, `ExecutionReport` as the
//! report) so the same bench harness and same TCP protocol that drive
//! the real matching engine can drive the no-op server. `apply` never
//! mutates business state and emits a trivial rejection per incoming
//! order so clients see a response — the transport (journal, disruptor,
//! replication, response gating) is exercised exactly as it is for the
//! trading app; the matching engine simply isn't in the dep graph.
//!
//! Intended uses:
//! - Isolating transport throughput / latency from matching cost.
//! - Demonstrating `Application`-level pluggability without needing
//!   `melin-engine`.
//! - Regression surface for the transport-core crate.

#![cfg_attr(not(test), deny(clippy::unwrap_used))]

use std::io::{self, Read, Write};

use melin_app::{Application, ApplyCtx, RejectReason as TransportRejectReason};
use melin_trading::trading_event::TradingEvent;
use melin_trading::types::{
    AccountId, ExecutionReport, OrderId, QueryResponse, RejectReason as EngineRejectReason, Symbol,
};

/// A stateless application that accepts any `TradingEvent` and emits a
/// single trivial report. Tick and hash-chain events are silent.
///
/// Snapshots store a single monotonic counter of applied events — enough
/// to exercise the transport's snapshot framing without pretending to
/// preserve real state.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopApp {
    /// Monotonic count of events seen via [`Application::apply`]. Only
    /// purpose is to make the snapshot payload non-empty so the
    /// transport-level framing has something to round-trip.
    events_applied: u64,
}

impl NoopApp {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn events_applied(&self) -> u64 {
        self.events_applied
    }
}

impl Application for NoopApp {
    type Event = TradingEvent;
    type Report = ExecutionReport;
    type QueryResponse = QueryResponse;

    const APP_VERSION: u16 = 1;

    /// Accept any event, bump the counter, emit one Rejected report so
    /// the client sees a response. `NoLiquidity` is used as the
    /// rejection reason — deliberately innocuous; real matching would
    /// pick a reason based on state, noop doesn't have state.
    #[inline]
    fn apply(
        &mut self,
        event: Self::Event,
        _ctx: &ApplyCtx,
        out: &mut Vec<Self::Report>,
    ) -> Option<Self::QueryResponse> {
        self.events_applied = self.events_applied.wrapping_add(1);
        out.push(rejected_from_event(&event, EngineRejectReason::NoLiquidity));
        None
    }

    /// Ticks advance the transport clock but noop has no scheduled tasks.
    #[inline]
    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<Self::Report>) {}

    /// Noop doesn't dedup — every request is accepted. The transport
    /// still gets an answer so its rejection path is a no-op.
    #[inline]
    fn check_request_seq(&mut self, _key_hash: u64, _seq: u64) -> bool {
        true
    }

    fn build_reject(event: &Self::Event, reason: TransportRejectReason) -> Self::Report {
        let engine_reason = match reason {
            TransportRejectReason::DuplicateRequest => EngineRejectReason::DuplicateRequest,
            TransportRejectReason::ReplicaDisconnected => EngineRejectReason::ReplicaDisconnected,
        };
        rejected_from_event(event, engine_reason)
    }

    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&Self::APP_VERSION.to_le_bytes())?;
        w.write_all(&self.events_applied.to_le_bytes())
    }

    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut version_bytes = [0u8; 2];
        r.read_exact(&mut version_bytes)?;
        let version = u16::from_le_bytes(version_bytes);
        if version != Self::APP_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown noop snapshot version: {version}"),
            ));
        }
        let mut counter_bytes = [0u8; 8];
        r.read_exact(&mut counter_bytes)?;
        Ok(Self {
            events_applied: u64::from_le_bytes(counter_bytes),
        })
    }
}

/// Extract routing metadata (order/account/symbol) from a trading event
/// so rejection reports can be attributed to the caller's order. Mirrors
/// the engine's `extract_*` helpers in spirit but lives here so noop
/// doesn't need to import `melin-engine`.
fn rejected_from_event(event: &TradingEvent, reason: EngineRejectReason) -> ExecutionReport {
    let (order_id, symbol, account) = match event {
        TradingEvent::SubmitOrder { symbol, order } => (order.id, *symbol, order.account),
        TradingEvent::CancelOrder {
            symbol,
            account,
            order_id,
        } => (*order_id, *symbol, *account),
        TradingEvent::CancelReplace {
            symbol,
            account,
            order_id,
            ..
        } => (*order_id, *symbol, *account),
        TradingEvent::CancelAll { account } => (OrderId(0), Symbol(0), *account),
        TradingEvent::Deposit { account, .. } => (OrderId(0), Symbol(0), *account),
        TradingEvent::Withdraw { account, .. } => (OrderId(0), Symbol(0), *account),
        TradingEvent::ProvisionAccount { account, .. } => (OrderId(0), Symbol(0), *account),
        TradingEvent::QueryPosition { account } => (OrderId(0), Symbol(0), *account),
        TradingEvent::SetRiskLimits { symbol, .. }
        | TradingEvent::SetCircuitBreaker { symbol, .. }
        | TradingEvent::SetFeeSchedule { symbol, .. }
        | TradingEvent::DisableInstrument { symbol }
        | TradingEvent::EnableInstrument { symbol }
        | TradingEvent::RemoveInstrument { symbol }
        | TradingEvent::AddInstrument {
            spec: melin_trading::types::InstrumentSpec { symbol, .. },
        } => (OrderId(0), *symbol, AccountId(0)),
        TradingEvent::EndOfDay | TradingEvent::QueryStats => (OrderId(0), Symbol(0), AccountId(0)),
    };
    ExecutionReport::Rejected {
        order_id,
        symbol,
        account,
        reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_trading::types::{
        Order, OrderType, Price, Quantity, SelfTradeProtection, Side, TimeInForce,
    };
    use std::io::Cursor;
    use std::num::NonZeroU64;

    fn ctx() -> ApplyCtx {
        ApplyCtx {
            now_ns: 0,
            journal_sequence: 0,
            active_connections: 0,
            events_processed: 0,
        }
    }

    fn sample_submit() -> TradingEvent {
        TradingEvent::SubmitOrder {
            symbol: Symbol(1),
            order: Order {
                id: OrderId(42),
                account: AccountId(7),
                side: Side::Buy,
                order_type: OrderType::Limit {
                    price: Price(NonZeroU64::new(100).unwrap()),
                    post_only: false,
                },
                quantity: Quantity(NonZeroU64::new(10).unwrap()),
                time_in_force: TimeInForce::GTC,
                stp: SelfTradeProtection::Allow,
                expiry_ns: 0,
            },
        }
    }

    #[test]
    fn apply_rejects_with_routing_info() {
        let mut app = NoopApp::new();
        let mut out = Vec::new();
        app.apply(sample_submit(), &ctx(), &mut out);
        assert_eq!(out.len(), 1);
        match out[0] {
            ExecutionReport::Rejected {
                order_id,
                symbol,
                account,
                reason,
            } => {
                assert_eq!(order_id, OrderId(42));
                assert_eq!(symbol, Symbol(1));
                assert_eq!(account, AccountId(7));
                assert_eq!(reason, EngineRejectReason::NoLiquidity);
            }
            other => panic!("expected Rejected, got {other:?}"),
        }
        assert_eq!(app.events_applied(), 1);
    }

    #[test]
    fn check_request_seq_always_true() {
        let mut app = NoopApp::new();
        assert!(app.check_request_seq(1, 1));
        assert!(app.check_request_seq(1, 1));
    }

    #[test]
    fn snapshot_round_trip() {
        let mut app = NoopApp::new();
        let mut out = Vec::new();
        for _ in 0..7 {
            app.apply(sample_submit(), &ctx(), &mut out);
        }
        assert_eq!(app.events_applied(), 7);

        let mut buf = Vec::new();
        app.snapshot(&mut buf).unwrap();

        let restored = NoopApp::restore(&mut Cursor::new(buf)).unwrap();
        assert_eq!(restored.events_applied(), 7);
    }
}
