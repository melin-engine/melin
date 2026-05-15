//! Journal pre-allocation chunk size, shared between `SectorWriter` and
//! `BufferedWriter`. Centralising the policy avoids drift between the
//! two writers — a switch between them must not change the disk-space
//! cadence under matched configuration.
//!
//! Resolution order, highest precedence first:
//!
//! 1. Runtime override set via `test_utils::set_prealloc_chunk_bytes_override`
//!    (only callable when the `test-utils` feature is enabled). Used by
//!    library tests that recover/append in tight loops where the 256 MiB
//!    `fallocate` dominates wall time.
//! 2. Environment variable `MELIN_JOURNAL_PREALLOC_MIB`. Used by
//!    integration tests that spawn the server binary and can't reach a
//!    Rust API. Floored at 1 MiB.
//! 3. `DEFAULT_PREALLOC_CHUNK` (256 MiB) — the production default.

use std::sync::atomic::{AtomicU64, Ordering};

/// Default pre-allocation chunk size (256 MiB). Matches the journal
/// rotation threshold so a freshly created journal never needs mid-run
/// extension at production scale (~80 B/entry × 256 MiB ≈ 3.2 M entries
/// per chunk).
const DEFAULT_PREALLOC_CHUNK: u64 = 256 * 1024 * 1024;

/// In-process override. 0 means "no override; consult env / default".
/// `AtomicU64` so the override can be set from any thread without
/// blocking — the value is read on each prealloc call (off the hot
/// path) with `Relaxed` ordering: writers care that *some* value
/// arrived, not that it synchronises with any specific event.
static OVERRIDE_BYTES: AtomicU64 = AtomicU64::new(0);

/// Resolve the chunk size for the next prealloc call.
pub(crate) fn prealloc_chunk_bytes() -> u64 {
    let o = OVERRIDE_BYTES.load(Ordering::Relaxed);
    if o > 0 {
        return o;
    }
    std::env::var("MELIN_JOURNAL_PREALLOC_MIB")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(|m| m.max(1) * 1024 * 1024)
        .unwrap_or(DEFAULT_PREALLOC_CHUNK)
}

/// Internal hook for `test_utils::set_prealloc_chunk_bytes_override`.
/// `None` clears the override; `Some(0)` is treated as "clear".
#[cfg(feature = "test-utils")]
pub(crate) fn set_override(bytes: Option<u64>) {
    OVERRIDE_BYTES.store(bytes.unwrap_or(0), Ordering::Relaxed);
}
