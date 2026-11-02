//! Test-only items. Enabled by depending on `melin-journal` with the
//! `test-utils` feature flag — typical use is to list the dependency a
//! second time under `[dev-dependencies]` with the feature on, so
//! production builds never see this surface.

use std::num::NonZeroU64;

use crate::Prealloc;

/// The reservation a test's journal is created with: 1 MiB, against the
/// 256 MiB default. A test writes far less than either, and a suite runs
/// many journals at once in a temporary directory that is often memory
/// (tmpfs), where every reserved byte is a byte of RAM. Small enough that
/// a parallel suite's journals stay small, large enough that a test's few
/// batches rarely extend it (and one that does exercises extension).
pub const PREALLOC: Prealloc =
    Prealloc::from_bytes(NonZeroU64::new(1024 * 1024).expect("not zero"));
