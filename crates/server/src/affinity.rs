//! CPU core pinning and real-time scheduling for pipeline threads.
//!
//! Uses `sched_setaffinity` and `sched_setscheduler` directly via libc.
//! Pinning each pipeline thread to a dedicated core eliminates involuntary
//! context switches and keeps hot data in L1/L2 cache, reducing p99/p99.9
//! latency jitter from ~5-20µs per core migration to near zero.
//!
//! `SCHED_FIFO` (real-time FIFO scheduling) prevents the CFS scheduler from
//! preempting pipeline threads for lower-priority work. On isolated cores
//! (`isolcpus` + `nohz_full`) this is belt-and-suspenders — the kernel
//! rarely schedules anything else there — but it eliminates the residual
//! risk of a kernel thread or workqueue temporarily preempting a pipeline
//! thread. Requires `CAP_SYS_NICE` or root; degrades gracefully to
//! `SCHED_OTHER` if unavailable.
//!
//! **Pipeline `--cores 0` means "do not pin"**. The pipeline-thread
//! wrapper [`pin_thread`] treats `0` as a sentinel and skips affinity
//! entirely, leaving the thread on the default OS scheduler across all
//! CPUs. Production deployments never run pipeline threads on core 0
//! (it is reserved for the kernel, IRQ handlers, and other system
//! processes), so the value is free to repurpose. This lets the
//! integration tests pass `--cores 0,0,0,...` without cramming every
//! pipeline thread of every spawned server onto a single physical CPU
//! — which previously caused the io_uring reader to starve under
//! contention and the failover suite to time out.
//!
//! The lower-level [`pin_to_core`] still pins literally — non-pipeline
//! callers (e.g. the bench progress thread that pins to core 0 on
//! purpose to stay off the bench cores) keep the old semantics.

/// Pin the calling thread to the specified logical CPU core and set
/// `SCHED_FIFO` real-time scheduling priority.
///
/// Must be called from within the target thread (uses tid 0 = "self").
/// Returns the core ID on success for logging convenience.
///
/// `SCHED_FIFO` is only set on non-zero cores. Core 0 is the shared
/// housekeeping core — RT priority there starves the kernel and other
/// processes.
///
/// `SCHED_FIFO` failure is non-fatal: the thread continues with default
/// scheduling. This allows running without `CAP_SYS_NICE` during
/// development while getting real-time priority in production.
pub fn pin_to_core(core_id: usize) -> Result<usize, String> {
    // cpu_set_t supports up to 1024 CPUs on Linux. Validate before
    // calling CPU_SET to avoid a panic in the libc wrapper.
    const MAX_CPUS: usize = 1024;
    if core_id >= MAX_CPUS {
        return Err(format!("core_id {core_id} exceeds maximum ({MAX_CPUS})"));
    }

    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(core_id, &mut set);

        let ret = libc::sched_setaffinity(
            0, // 0 = calling thread
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        );

        if ret != 0 {
            return Err(format!(
                "sched_setaffinity failed for core {core_id}: {}",
                std::io::Error::last_os_error()
            ));
        }
    }

    // Set SCHED_FIFO with minimum real-time priority (1) on isolated
    // cores only. Core 0 is the housekeeping core — RT priority there
    // would starve the kernel, IRQ handlers, and other processes.
    if core_id > 0 {
        set_realtime_fifo(1);
    }

    Ok(core_id)
}

/// Attempt to set `SCHED_FIFO` real-time scheduling on the calling thread.
fn set_realtime_fifo(priority: i32) {
    unsafe {
        let param = libc::sched_param {
            sched_priority: priority,
        };
        let ret = libc::sched_setscheduler(0, libc::SCHED_FIFO, &param);
        if ret != 0 {
            // Non-fatal: EPERM when running without CAP_SYS_NICE.
            tracing::warn!(
                error = %std::io::Error::last_os_error(),
                "SCHED_FIFO failed (run as root or grant CAP_SYS_NICE)"
            );
        }
    }
}

/// Pin the calling thread to `core` with logging on success/failure.
///
/// Convenience wrapper around [`pin_to_core`] for pipeline threads
/// (primary and replica, journal/matching/response/shadow/sender/
/// receiver). Emits a structured log entry — `info!` on success,
/// `warn!` on failure — so every pipeline thread reports its pin
/// outcome consistently.
///
/// `core == 0` is treated as a sentinel: affinity is skipped and the
/// thread is left on the default OS scheduler. See module docs for
/// rationale.
pub(crate) fn pin_thread(name: &str, core: usize) {
    if core == 0 {
        tracing::info!(thread = name, "thread left unpinned (core 0 sentinel)");
        return;
    }
    match pin_to_core(core) {
        Ok(c) => tracing::info!(core = c, thread = name, "pinned to core"),
        Err(e) => tracing::warn!(thread = name, error = e, "core pinning failed"),
    }
}

/// Clear CPU affinity and reset scheduling policy for the calling thread.
///
/// Child threads spawned from a pinned parent inherit both the parent's
/// single-core affinity mask and its `SCHED_FIFO` policy. Call this at
/// the start of the child thread to restore the full core set and
/// default `SCHED_OTHER` scheduling.
pub fn clear_affinity() -> Result<(), String> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        // Set all cores. On systems with fewer cores, the extra bits
        // are ignored by the kernel.
        for i in 0..libc::CPU_SETSIZE as usize {
            libc::CPU_SET(i, &mut set);
        }

        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);

        if ret != 0 {
            return Err(format!(
                "sched_setaffinity (clear) failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        // Reset to default CFS scheduling. If the parent was
        // SCHED_FIFO, the child inherits it — a non-pinned thread
        // with SCHED_FIFO could starve other work on shared cores.
        let param = libc::sched_param { sched_priority: 0 };
        let ret = libc::sched_setscheduler(0, libc::SCHED_OTHER, &param);
        if ret != 0 {
            return Err(format!(
                "sched_setscheduler (SCHED_OTHER) failed: {}",
                std::io::Error::last_os_error()
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_to_core_0_succeeds() {
        // Core 0 always exists on any machine.
        assert!(pin_to_core(0).is_ok());
    }

    #[test]
    fn pin_to_invalid_core_fails() {
        // A core ID beyond any real hardware should fail.
        assert!(pin_to_core(99999).is_err());
    }
}
