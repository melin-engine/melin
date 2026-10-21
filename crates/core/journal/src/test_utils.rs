//! Test-only hooks. Enabled by depending on `melin-journal` with the
//! `test-utils` feature flag — typical use is to list the dependency a
//! second time under `[dev-dependencies]` with the feature on, so
//! production builds never see this surface.
//!
//! [`PreallocOverrideGuard`] shrinks the per-prealloc `fallocate` from
//! the 256 MiB default for the guard's lifetime, and serialises
//! concurrent users through a process-wide lock, so one test's override
//! cannot silently replace another's.

pub use crate::prealloc::PreallocOverrideGuard;
