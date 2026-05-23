//! Snapshot/restore accessors on `Exchange`. Grouped here because the
//! top-level `crate::snapshot` module is the only consumer of most of
//! them — keeping them in one file makes the snapshot contract easier
//! to audit.
//!
//! Constructors that participate in restore (`from_parts`) and the
//! field-state setters they pair with stay in `exchange.rs` for now;
//! only the pure read/write accessors live here.

use super::Exchange;
use super::token_bucket::TokenBucket;
use crate::orderbook::OrderBook;
use crate::types::{
    AccountId, CircuitBreakerConfig, CurrencyId, FeeSchedule, InstrumentSpec, OrderId,
    ReservationSlot, RiskLimits, Side, Symbol,
};

impl Exchange {
    /// Snapshot per-key request sequence HWMs for serialization.
    pub fn snapshot_key_hwm(&self) -> Vec<(u64, u64)> {
        self.key_hwm
            .iter()
            .filter(|(_, hwm)| **hwm > 0)
            .map(|(&key, &hwm)| (key, hwm))
            .collect()
    }

    /// Snapshot per-account rate-limiter bucket state for serialization
    /// (SEC-04 v18+ snapshots). Each tuple is `(account, tokens,
    /// last_refill_ns)`. Returned in unspecified order — callers must
    /// not depend on stability across runs.
    ///
    /// Without this, a replica that restored from a snapshot taken at
    /// time T while the primary's bucket for some account A was
    /// partially depleted would re-initialise A's bucket lazily as full
    /// at the next event, while the primary kept the depleted state —
    /// the divergence window flagged in the SEC-04 audit. Closing it
    /// requires carrying the bucket map in the snapshot.
    pub(crate) fn snapshot_order_buckets(&self) -> Vec<(AccountId, u64, u64)> {
        self.order_buckets
            .iter()
            .map(|(&account, bucket)| (account, bucket.tokens, bucket.last_refill_ns))
            .collect()
    }

    /// Repopulate the rate-limiter bucket map from a deserialised
    /// snapshot. Called by `restore_state` after the rest of the engine
    /// is reconstructed. Existing entries are cleared first so a stale
    /// in-process bucket cannot survive a restore.
    ///
    /// Preserving exact bucket state (`tokens` + `last_refill_ns`) is
    /// what closes the SEC-04 snapshot-divergence window: the next
    /// event's `refill_and_consume` call will see the same elapsed-time
    /// math the primary would have, producing identical accept/reject
    /// decisions.
    pub(crate) fn restore_order_buckets(&mut self, buckets: Vec<(AccountId, u64, u64)>) {
        self.order_buckets.clear();
        for (account, tokens, last_refill_ns) in buckets {
            self.order_buckets.insert(
                account,
                TokenBucket {
                    tokens,
                    last_refill_ns,
                },
            );
        }
    }

    /// Iterate over instrument specs (for snapshot serialization).
    pub fn instrument_specs(&self) -> impl Iterator<Item = &InstrumentSpec> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| &inst.spec)
    }

    /// Iterate over (symbol, book) pairs (for snapshot serialization and proptests).
    pub(crate) fn books(&self) -> impl Iterator<Item = (Symbol, &OrderBook)> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| (inst.spec.symbol, &inst.book))
    }

    /// Snapshot the order-side map as a Vec for serialization.
    /// Only serializes the side; reservation slots are ephemeral and
    /// reassigned on restore.
    pub fn snapshot_order_sides(&self) -> Vec<((AccountId, OrderId), Side)> {
        let mut sides = Vec::new();
        for inst in &self.instruments {
            if let Some(inst) = inst.as_deref() {
                for (key, (side, _slot)) in inst.book.active_order_slots() {
                    sides.push((key, side));
                }
                for (key, (side, _slot)) in inst.book.active_stop_slots() {
                    sides.push((key, side));
                }
            }
        }
        sides
    }

    /// Collect active reservation slot assignments from all instruments.
    fn active_reservation_slots(&self) -> Vec<((AccountId, OrderId), ReservationSlot)> {
        let mut slots = Vec::new();
        for inst in &self.instruments {
            if let Some(inst) = inst.as_deref() {
                for (key, (_side, slot)) in inst.book.active_order_slots() {
                    slots.push((key, slot));
                }
                for (key, (_side, slot)) in inst.book.active_stop_slots() {
                    slots.push((key, slot));
                }
            }
        }
        slots
    }

    /// Snapshot all active reservations. Delegates to AccountManager with
    /// the active slot assignments.
    pub(crate) fn snapshot_reservations(&self) -> Vec<(OrderId, AccountId, CurrencyId, u64)> {
        let active = self.active_reservation_slots();
        self.accounts.snapshot_reservations(&active)
    }

    /// Snapshot the per-instrument risk limits for serialization.
    pub fn snapshot_risk_limits(&self) -> Vec<(Symbol, RiskLimits)> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| (inst.spec.symbol, inst.risk_limits))
            .collect()
    }

    /// Snapshot the fee schedules for serialization.
    pub(crate) fn snapshot_fee_schedules(&self) -> Vec<(Symbol, FeeSchedule)> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| (inst.spec.symbol, inst.fee_schedule))
            .collect()
    }

    /// Snapshot the per-instrument circuit breaker configs for serialization.
    pub fn snapshot_circuit_breakers(&self) -> Vec<(Symbol, CircuitBreakerConfig)> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .map(|inst| (inst.spec.symbol, inst.circuit_breaker))
            .collect()
    }

    /// Snapshot the disabled instrument symbols for serialization.
    pub(crate) fn snapshot_disabled_instruments(&self) -> Vec<Symbol> {
        self.instruments
            .iter()
            .filter_map(|slot| slot.as_deref())
            .filter(|inst| inst.disabled)
            .map(|inst| inst.spec.symbol)
            .collect()
    }
}
