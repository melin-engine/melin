//! Process-level setup for latency-sensitive servers: signal handling
//! and memory locking. Generic OS plumbing — no application coupling.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Pointer to the shared shutdown flag, set once before signals can fire.
/// `AtomicUsize` stores the raw pointer as an integer — signal-safe.
static SHUTDOWN_PTR: AtomicUsize = AtomicUsize::new(0);

/// Signal handler for SIGINT/SIGTERM. Sets the shutdown flag.
/// Second signal force-exits (user is impatient).
extern "C" fn signal_handler(_sig: libc::c_int) {
    let ptr = SHUTDOWN_PTR.load(Ordering::Relaxed);
    if ptr != 0 {
        let flag = unsafe { &*(ptr as *const AtomicBool) };
        if flag.swap(true, Ordering::Relaxed) {
            unsafe { libc::_exit(1) };
        }
    }
}

/// Install SIGINT/SIGTERM handlers that flip `shutdown` on first signal
/// and force-exit on the second. The caller must keep the `Arc` alive
/// for the program's lifetime — we publish its pointer to a signal-safe
/// static so the handler can reach the flag.
pub fn install_shutdown_handler(shutdown: &Arc<AtomicBool>) {
    SHUTDOWN_PTR.store(Arc::as_ptr(shutdown) as usize, Ordering::Relaxed);
    unsafe {
        libc::signal(
            libc::SIGINT,
            signal_handler as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            signal_handler as *const () as libc::sighandler_t,
        );
    }
}

/// Pin engine pages into RAM with `mlockall`, lock-on-fault.
///
/// Goal: prevent the kernel from faulting *out* engine pages under memory
/// pressure, which otherwise surfaces as 100µs–10ms tail spikes.
///
/// We pass `MCL_ONFAULT` alongside `MCL_CURRENT | MCL_FUTURE` deliberately.
/// This runs early — before the order book and the rest of the engine are
/// built — so `MCL_FUTURE` is required to cover those later allocations.
/// But plain `MCL_FUTURE` marks every future mapping `VM_LOCKED` and makes
/// the kernel *eagerly populate* the whole mapping at `mmap` time, inside an
/// uninterruptible `__mm_populate` walk. Any sizeable post-init allocation
/// (a large malloc glibc serves via `mmap`, a snapshot/clone) then stalls
/// the allocating thread until every page is faulted in — long enough to
/// even outlast SIGKILL. `MCL_ONFAULT` switches future mappings to
/// lock-on-fault: pages are locked as they are touched, not populated up
/// front. The fault-out protection we actually want is preserved (a page,
/// once touched on the hot path, stays resident), without the populate stall.
///
/// Best-effort: requires `CAP_IPC_LOCK` (or root) and a sufficient
/// `RLIMIT_MEMLOCK`. Logs a warning and continues on failure.
pub fn try_lock_memory() {
    let unlim = libc::rlimit {
        rlim_cur: libc::RLIM_INFINITY,
        rlim_max: libc::RLIM_INFINITY,
    };
    let setrlimit_rc = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &unlim) };
    if setrlimit_rc != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(
            error = %err,
            "could not raise RLIMIT_MEMLOCK; mlockall may fail or be capped"
        );
    }

    let flags = libc::MCL_CURRENT | libc::MCL_FUTURE | libc::MCL_ONFAULT;
    let rc = unsafe { libc::mlockall(flags) };
    if rc == 0 {
        tracing::info!("mlockall(MCL_CURRENT | MCL_FUTURE | MCL_ONFAULT) succeeded");
    } else {
        let err = std::io::Error::last_os_error();
        tracing::warn!(
            error = %err,
            "mlockall failed; running without memory lock — tail latency may be affected"
        );
    }
}
