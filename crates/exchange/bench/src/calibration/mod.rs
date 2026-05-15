//! Calibration pipeline: extract distributional statistics from an
//! ITCH 5.0 market-data dump and compare them to the synthetic output
//! of [`crate::generator::OrderFlowGenerator`] to verify the generator
//! is representative of real venue load.
//!
//! Temporal and joint-structure metrics are out of scope on this branch
//! (the generator has no inter-arrival or joint sampling model yet);
//! see the audit in the generator-calibration branch description for
//! the deferred items.

pub mod book;
pub mod itch;
pub mod stats;

/// Order side. Buys and sells are kept symmetric throughout the
/// pipeline; ratios are reported per-side so imbalance is visible.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}
