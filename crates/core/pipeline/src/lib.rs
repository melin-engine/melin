//! LMAX Disruptor-style ring buffers for low-latency inter-thread communication.
//!
//! Provides two ring buffer variants:
//! - [`ring`]: Multi-consumer disruptor with dependency-gated consumers (1 producer, N consumers).
//! - [`spsc`]: Single-producer, single-consumer queue for simple point-to-point channels.
//!
//! Both are lock-free, use cache-line padding to avoid false sharing, and support
//! batch consumption for amortized throughput.
//!
//! Every blocking wait — a producer on a full ring, a consumer loop on an
//! empty one — goes through [`wait::WaitStrategy`], chosen once per
//! deployment: pure busy-spin on isolated cores, spin-then-yield where
//! threads share a core.

pub mod padding;
pub mod ring;
pub mod seqlock;
pub mod spsc;
pub mod wait;
