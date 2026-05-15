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
pub fn set_prealloc_chunk_bytes_override(bytes: Option<u64>) {
    crate::prealloc::set_override(bytes);
}
