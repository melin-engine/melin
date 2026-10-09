//! Test-only hooks. Enabled by depending on `melin-journal` with the
//! `test-utils` feature flag — typical use is to list the dependency a
//! second time under `[dev-dependencies]` with the feature on, so
//! production builds never see this surface.

/// Override the journal pre-allocation chunk size for the process.
/// Pass `Some(bytes)` to shrink the per-prealloc `fallocate` from the
/// 256 MiB default; pass `None` to clear and fall back to the env
/// variable / default.
///
/// Affects every journal writer constructed *after* the call in this
/// process. Persists for the process lifetime — tests that depend on
/// the production default must not enable this feature.
///
/// **Prefer [`PreallocOverrideGuard`]** for new tests: the guard
/// scopes the override to a single test and serialises concurrent
/// users via a process-wide lock, eliminating the "test A's override
/// is silently overwritten by test B running in parallel" failure
/// mode. This setter remains for callers that need permanent
/// process-wide overrides (rare).
pub fn set_prealloc_chunk_bytes_override(bytes: Option<u64>) {
    crate::prealloc::set_override(bytes);
}

pub use crate::prealloc::PreallocOverrideGuard;

/// Make the next `fdatasync` of the live segment at `path` fail with
/// `EIO`, as a device that rejected the write-back would. One call fails
/// one sync; the failure is consumed when it fires.
///
/// `path` is the live segment's path exactly as the writer was opened
/// with it (a node's `--journal`). The failure enters the writer where
/// the kernel's would, so the error the caller sees, and its
/// classification as a journal write failure, are the real ones.
pub fn fail_next_sync(path: &std::path::Path) {
    crate::sync_fault::arm(path);
}

/// Clear the process-wide write-failure latch (see
/// [`crate::write_failure_latched`]), which otherwise stays set for the
/// rest of the process once any journal sync has failed.
///
/// Tests share a process, so a test that asserts on the latch, or on an
/// exit status derived from it, must not run alongside one that makes a
/// journal sync fail: serialize them (a lock shared by those tests) and
/// call this at the start of each, under the lock.
pub fn reset_write_failure_latch() {
    crate::error::reset_write_failure_latch();
}
