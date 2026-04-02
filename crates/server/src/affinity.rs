//! CPU core pinning for pipeline threads.
//!
//! Uses `sched_setaffinity` directly via libc — no wrapper crate needed.
//! Pinning each pipeline thread to a dedicated core eliminates involuntary
//! context switches and keeps hot data in L1/L2 cache, reducing p99/p99.9
//! latency jitter from ~5-20µs per core migration to near zero.

/// Pin the calling thread to the specified logical CPU core.
///
/// Must be called from within the target thread (uses tid 0 = "self").
/// Returns the core ID on success for logging convenience.
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

        if ret == 0 {
            Ok(core_id)
        } else {
            Err(format!(
                "sched_setaffinity failed for core {core_id}: {}",
                std::io::Error::last_os_error()
            ))
        }
    }
}

/// Clear CPU affinity for the calling thread, allowing it to run on any core.
///
/// Child threads spawned from a pinned parent inherit the parent's
/// single-core affinity mask. Call this at the start of the child thread
/// to restore the full core set.
pub fn clear_affinity() -> Result<(), String> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        // Set all cores. On systems with fewer cores, the extra bits
        // are ignored by the kernel.
        for i in 0..libc::CPU_SETSIZE as usize {
            libc::CPU_SET(i, &mut set);
        }

        let ret = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);

        if ret == 0 {
            Ok(())
        } else {
            Err(format!(
                "sched_setaffinity (clear) failed: {}",
                std::io::Error::last_os_error()
            ))
        }
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
