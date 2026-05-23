//! Per-instrument state struct and the index helpers that look it up
//! by `Symbol`. Pulled into its own submodule so the substrate type
//! (`InstrumentState`) and its `Symbol`-indexed access pattern live
//! next to each other rather than being scattered through `exchange.rs`.

use crate::orderbook::OrderBook;
use crate::types::{CircuitBreakerConfig, FeeSchedule, InstrumentSpec, RiskLimits, Symbol};

/// All per-instrument state in one struct for cache-friendly single-lookup
/// access. On every order the engine does one HashMap lookup instead of 5,
/// turning 5 potential cache misses into 1.
pub(crate) struct InstrumentState {
    pub(crate) spec: InstrumentSpec,
    pub(crate) book: OrderBook,
    pub(crate) risk_limits: RiskLimits,
    pub(crate) circuit_breaker: CircuitBreakerConfig,
    pub(crate) fee_schedule: FeeSchedule,
    /// When true, the instrument is disabled — no new orders or amendments
    /// are accepted. All resting orders are cancelled on disable.
    pub(crate) disabled: bool,
}

/// Helper: get an immutable reference to the InstrumentState at `symbol`.
#[inline]
pub(super) fn inst_ref(
    instruments: &[Option<Box<InstrumentState>>],
    symbol: Symbol,
) -> Option<&InstrumentState> {
    instruments
        .get(symbol.0 as usize)
        .and_then(|o| o.as_deref())
}

/// Helper: get a mutable reference to the InstrumentState at `symbol`.
#[inline]
pub(super) fn inst_mut(
    instruments: &mut [Option<Box<InstrumentState>>],
    symbol: Symbol,
) -> Option<&mut InstrumentState> {
    instruments
        .get_mut(symbol.0 as usize)
        .and_then(|o| o.as_deref_mut())
}
