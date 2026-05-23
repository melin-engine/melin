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

/// Pin the entire process address space into RAM with `mlockall`.
///
/// Prevents the kernel from faulting out engine pages under memory
/// pressure, which otherwise surfaces as 100µs–10ms tail spikes.
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

    let rc = unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) };
    if rc == 0 {
        tracing::info!("mlockall(MCL_CURRENT | MCL_FUTURE) succeeded");
    } else {
        let err = std::io::Error::last_os_error();
        tracing::warn!(
            error = %err,
            "mlockall failed; running without memory lock — tail latency may be affected"
        );
    }
}
