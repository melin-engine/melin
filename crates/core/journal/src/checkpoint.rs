//! Hash-chain checkpoint interval, shared between `SectorWriter` and
//! `BufferedWriter`. Centralising the policy avoids drift between the
//! two writers — a switch between them must not change when checkpoints
//! fire under matched configuration.
//!
//! Resolution order, highest precedence first:
//!
//! 1. Runtime override via [`CheckpointIntervalOverrideGuard`] (only
//!    available under the `test-utils` feature). Used by in-process
//!    tests that need checkpoints to fire after a handful of events
//!    instead of 100K.
//! 2. Environment variable `MELIN_JOURNAL_CHECKPOINT_INTERVAL`. Used by
//!    integration tests that spawn the server binary. Floored at 1.
//! 3. `DEFAULT_CHECKPOINT_INTERVAL` (100K) — the production default.

use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "test-utils")]
use std::sync::{Mutex, MutexGuard};

/// 100K events × ~80 bytes = ~8 MB of journal data between checkpoints.
/// The checkpoint itself is ~77 bytes — negligible overhead.
const DEFAULT_CHECKPOINT_INTERVAL: u64 = 100_000;

/// In-process override. 0 means "no override; consult env / default".
static OVERRIDE: AtomicU64 = AtomicU64::new(0);

/// Env-var result, cached on first read. `OnceLock` is fine here
/// because the env var is a process-wide constant; the test override
/// bypasses it entirely via the `OVERRIDE` atomic.
static ENV_CACHE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

/// Resolve the checkpoint interval for the current `encode_event` call.
/// Read on every event (hot path) but the cost is a single
/// `Relaxed` load + a predictable branch — one cycle on x86 when no
/// test override is active.
pub fn checkpoint_interval() -> u64 {
    let o = OVERRIDE.load(Ordering::Relaxed);
    if o > 0 {
        return o;
    }
    *ENV_CACHE.get_or_init(|| {
        std::env::var("MELIN_JOURNAL_CHECKPOINT_INTERVAL")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(|v| v.max(1))
            .unwrap_or(DEFAULT_CHECKPOINT_INTERVAL)
    })
}

#[cfg(feature = "test-utils")]
fn set_override(interval: Option<u64>) {
    OVERRIDE.store(interval.unwrap_or(0), Ordering::Relaxed);
}

#[cfg(feature = "test-utils")]
static OVERRIDE_LOCK: Mutex<()> = Mutex::new(());

/// RAII guard scoping a checkpoint-interval override to the lifetime of
/// a test. Mirrors [`crate::prealloc::PreallocOverrideGuard`].
///
/// Bind to a *named* variable (`let _guard = ...;`), never bare `_`.
#[cfg(feature = "test-utils")]
pub struct CheckpointIntervalOverrideGuard {
    _lock: MutexGuard<'static, ()>,
}

#[cfg(feature = "test-utils")]
impl CheckpointIntervalOverrideGuard {
    pub fn new(interval: u64) -> Self {
        let lock = OVERRIDE_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        set_override(Some(interval.max(1)));
        Self { _lock: lock }
    }
}

#[cfg(feature = "test-utils")]
impl Drop for CheckpointIntervalOverrideGuard {
    fn drop(&mut self) {
        set_override(None);
    }
}
