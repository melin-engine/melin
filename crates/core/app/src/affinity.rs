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

/// Pin the calling thread to the specified logical CPU core, and grant it
/// `SCHED_FIFO` real-time scheduling priority when the core is isolated.
///
/// Must be called from within the target thread (uses tid 0 = "self").
/// Returns the core ID on success for logging convenience.
///
/// Affinity is always set. `SCHED_FIFO` is granted only on a non-zero core
/// that the kernel reports isolated (listed in
/// `/sys/devices/system/cpu/isolated`, i.e. booted with `isolcpus=`) — see
/// [`core_is_isolated`]. On a shared core a busy-spinning RT thread would
/// starve every `SCHED_OTHER` thread co-located with it, so RT priority is
/// withheld there (the thread keeps plain affinity). Core 0 is the shared
/// housekeeping core and never gets RT priority regardless.
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

    // Real-time priority (SCHED_FIFO) is safe ONLY on isolated cores. On a
    // shared core a busy-spinning RT thread starves every SCHED_OTHER thread
    // pinned there. Under DPDK this is a concrete deadlock: EAL reserves cores
    // and runs its control threads (mp-msg/intr/telemetry/workers) on them, so
    // on a non-`isolcpus` host those threads share cores with the pinned
    // pipeline threads — and one of them holding the glibc malloc arena lock
    // while starved wedges graceful shutdown forever. (Kernel-TCP reserves no
    // cores, so it never collides.) So: pin affinity always, but grant
    // SCHED_FIFO only when the kernel actually reports this core isolated
    // (`isolcpus=`). Core 0 is the housekeeping core and is excluded
    // regardless — RT there would starve the kernel, IRQ handlers, and others.
    if core_id > 0 && core_is_isolated(core_id) {
        set_realtime_fifo(1);
    } else if core_id > 0 {
        tracing::warn!(
            core = core_id,
            "core not isolated (no isolcpus); pinned affinity only, no SCHED_FIFO \
             (real-time busy-spin on a shared core would starve co-located threads). \
             Boot with isolcpus on the pipeline cores for lowest tail latency."
        );
    }

    Ok(core_id)
}

/// Whether `core_id` is in the kernel's isolated-CPU set, i.e. listed in
/// `/sys/devices/system/cpu/isolated` (populated from the `isolcpus=` boot
/// parameter). [`pin_to_core`] grants `SCHED_FIFO` only to isolated cores.
///
/// Best-effort: a missing or unreadable sysfs file is treated as "not
/// isolated" (the safe default — affinity without real-time priority), which
/// is the reality on any host booted without `isolcpus`.
fn core_is_isolated(core_id: usize) -> bool {
    match std::fs::read_to_string("/sys/devices/system/cpu/isolated") {
        Ok(list) => cpu_list_contains(list.trim(), core_id),
        // No isolcpus configured (or sysfs unavailable) → not isolated.
        Err(_) => false,
    }
}

/// Test membership in a Linux CPU-list string: comma-separated singletons and
/// inclusive ranges, e.g. `"2-7"`, `"1,3,5"`, `"2-4,6-8"`, or empty (no
/// isolated cores). Pure + total so it is unit-tested without touching sysfs.
fn cpu_list_contains(list: &str, core_id: usize) -> bool {
    list.split(',').filter(|p| !p.is_empty()).any(|part| {
        match part.split_once('-') {
            // Inclusive range "lo-hi".
            Some((lo, hi)) => matches!(
                (lo.parse::<usize>(), hi.parse::<usize>()),
                (Ok(lo), Ok(hi)) if lo <= core_id && core_id <= hi
            ),
            // Single CPU "n"; a malformed (non-numeric) token never matches.
            None => matches!(part.parse::<usize>(), Ok(n) if n == core_id),
        }
    })
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
pub fn pin_thread(name: &str, core: usize) {
    if core == 0 {
        tracing::info!(thread = name, "thread left unpinned (core 0 sentinel)");
        return;
    }
    match pin_to_core(core) {
        Ok(c) => tracing::info!(core = c, thread = name, "pinned to core"),
        Err(e) => tracing::warn!(thread = name, error = e, "core pinning failed"),
    }
}

/// A thread's scheduling context: CPU affinity mask plus policy and
/// priority. Captured by [`take_context`] and put back by
/// [`restore_context`].
pub struct SchedContext {
    mask: libc::cpu_set_t,
    policy: libc::c_int,
    priority: libc::c_int,
}

/// Snapshot the calling thread's affinity mask, scheduling policy and
/// priority.
pub fn take_context() -> Result<SchedContext, String> {
    unsafe {
        let mut mask: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut mask) != 0 {
            return Err(format!(
                "sched_getaffinity failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let policy = libc::sched_getscheduler(0);
        if policy < 0 {
            return Err(format!(
                "sched_getscheduler failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        let mut param: libc::sched_param = std::mem::zeroed();
        if libc::sched_getparam(0, &mut param) != 0 {
            return Err(format!(
                "sched_getparam failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(SchedContext {
            mask,
            policy,
            priority: param.sched_priority,
        })
    }
}

/// Put a snapshot from [`take_context`] back on the calling thread.
pub fn restore_context(ctx: &SchedContext) -> Result<(), String> {
    unsafe {
        let param = libc::sched_param {
            sched_priority: ctx.priority,
        };
        // Policy first: dropping out of `SCHED_FIFO` while holding a
        // single-core mask is the safe ordering — the reverse briefly
        // leaves an RT thread on a wider mask.
        if libc::sched_setscheduler(0, ctx.policy, &param) != 0 {
            return Err(format!(
                "sched_setscheduler (restore) failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &ctx.mask) != 0 {
            return Err(format!(
                "sched_setaffinity (restore) failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }
}

/// Put the calling thread into the scheduling context a thread spawned
/// *next* should inherit: pinned to `core`, or unpinned across every CPU
/// when `core` is the `0` sentinel, and always `SCHED_OTHER`.
///
/// # Why this exists
///
/// A new thread inherits its creator's affinity mask and scheduling
/// policy at creation, and Linux offers no way to set another thread's
/// affinity before it is first scheduled. So a child spawned from a
/// pinned `SCHED_FIFO` thread starts life sharing one core with a
/// busy-spinning real-time thread — and cannot fix itself, because
/// fixing itself requires running. On an isolated core the parent never
/// yields, so the child never executes its first instruction. Its
/// `comm` still reads as the parent's name, because even
/// `Builder::name` is applied from inside the new thread.
///
/// The only place that ordering can be broken is the parent, before the
/// child exists. Call this, spawn, then [`restore_context`]:
///
/// ```ignore
/// let saved = take_context()?;
/// prepare_child_context(child_core);
/// let handle = std::thread::Builder::new().spawn(move || { … })?;
/// restore_context(&saved)?;
/// ```
///
/// The child then starts already on its own core under `SCHED_OTHER`,
/// free to run and promote itself with [`pin_thread`].
pub fn prepare_child_context(core: usize) -> Result<(), String> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        if core == 0 {
            // The `0` sentinel means "do not pin" — hand over the full
            // mask so the child floats, rather than the parent's core.
            for i in 0..libc::CPU_SETSIZE as usize {
                libc::CPU_SET(i, &mut set);
            }
        } else {
            const MAX_CPUS: usize = 1024;
            if core >= MAX_CPUS {
                return Err(format!("core {core} exceeds maximum ({MAX_CPUS})"));
            }
            libc::CPU_SET(core, &mut set);
        }

        // Drop to SCHED_OTHER before widening the mask: the child must
        // not inherit real-time priority it has not earned, and the
        // parent must not sit at RT priority on a wide mask even
        // momentarily.
        let param = libc::sched_param { sched_priority: 0 };
        if libc::sched_setscheduler(0, libc::SCHED_OTHER, &param) != 0 {
            return Err(format!(
                "sched_setscheduler (child prep) failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(format!(
                "sched_setaffinity (child prep) failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }
}

/// Clear CPU affinity and reset scheduling policy for the calling thread.
///
/// Child threads spawned from a pinned parent inherit both the parent's
/// single-core affinity mask and its `SCHED_FIFO` policy. Call this at
/// the start of the child thread to restore the full core set and
/// default `SCHED_OTHER` scheduling.
///
/// Only usable by a child that can actually run — see
/// [`prepare_child_context`] for why a child of a pinned RT parent
/// cannot, and must be handed its context instead.
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

    /// Number of CPUs in the calling thread's affinity mask.
    #[cfg(test)]
    fn affinity_width() -> usize {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            assert_eq!(
                libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set),
                0,
                "sched_getaffinity failed: {}",
                std::io::Error::last_os_error()
            );
            (0..libc::CPU_SETSIZE as usize)
                .filter(|&c| libc::CPU_ISSET(c, &set))
                .count()
        }
    }

    /// A thread spawned from a pinned parent inherits the parent's
    /// single-core mask, and [`clear_affinity`] — called *from the
    /// child* — widens the child's own mask without touching the
    /// parent's.
    ///
    /// Note what this does **not** establish: that the child ever gets
    /// to make that call. Under `SCHED_OTHER` (all this test can reach
    /// without `CAP_SYS_NICE`) CFS timeslices the child in regardless of
    /// the shared mask. Under the `SCHED_FIFO` a pinned isolated core
    /// grants in production, a busy-spinning parent never yields and the
    /// child never runs at all. That is why the disk thread is handed
    /// its context by the parent instead — see
    /// [`child_spawned_after_prepare_context_lands_on_its_own_core`].
    #[test]
    fn child_inherits_parent_affinity_and_can_clear_its_own() {
        let full_width = affinity_width();
        if full_width < 2 {
            // A single-CPU machine cannot distinguish inherited from
            // cleared. Assert what still holds and stop.
            assert!(pin_to_core(0).is_ok());
            return;
        }

        // Pin a parent thread (not the test thread — the pin would
        // outlive the test and skew whatever runs next on it).
        let widths = std::thread::spawn(|| {
            pin_to_core(0).expect("core 0 always exists");
            let parent_before = affinity_width();

            let child = std::thread::spawn(|| {
                let inherited = affinity_width();
                clear_affinity().expect("clear affinity");
                (inherited, affinity_width())
            })
            .join()
            .expect("child thread");

            (parent_before, child, affinity_width())
        })
        .join()
        .expect("parent thread");

        let (parent_before, (child_inherited, child_cleared), parent_after) = widths;

        assert_eq!(parent_before, 1, "the parent pinned itself to one core");
        assert_eq!(
            child_inherited, 1,
            "child must inherit the parent's single-core mask — if this ever \
             stops holding, the disk thread's clear_affinity call is obsolete"
        );
        assert_eq!(
            child_cleared, full_width,
            "clear_affinity must restore the full mask on the calling thread"
        );
        assert_eq!(
            parent_after, 1,
            "the child's clear_affinity must not touch the parent's pin"
        );
    }

    /// Mask of the calling thread, as a sorted core list.
    fn affinity_cores() -> Vec<usize> {
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            assert_eq!(
                libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set),
                0
            );
            (0..libc::CPU_SETSIZE as usize)
                .filter(|&c| libc::CPU_ISSET(c, &set))
                .collect()
        }
    }

    /// The property the journal's disk thread actually depends on: a
    /// child spawned between [`prepare_child_context`] and
    /// [`restore_context`] starts on **its own** core, not the parent's,
    /// and the parent gets its pin back.
    ///
    /// This is what makes the disk thread schedulable at all. The child
    /// cannot move itself off a core whose real-time occupant never
    /// yields — it would have to run in order to try — so the handover
    /// has to happen before it exists. A regression to child-side
    /// configuration leaves the child on the parent's core here, where
    /// this test sees it, rather than only on a tuned `isolcpus` host
    /// where it deadlocks.
    #[test]
    fn child_spawned_after_prepare_context_lands_on_its_own_core() {
        if affinity_width() < 2 {
            return; // one CPU: parent and child cannot be distinguished
        }

        let (parent_before, child_cores, parent_after) = std::thread::spawn(|| {
            pin_to_core(0).expect("core 0 always exists");
            let parent_before = affinity_cores();

            let saved = take_context().expect("snapshot context");
            prepare_child_context(1).expect("prepare child context");
            let child_cores = std::thread::spawn(affinity_cores)
                .join()
                .expect("child thread");
            restore_context(&saved).expect("restore context");

            (parent_before, child_cores, affinity_cores())
        })
        .join()
        .expect("parent thread");

        assert_eq!(parent_before, vec![0], "parent pinned itself to core 0");
        assert_eq!(
            child_cores,
            vec![1],
            "the child must start on the core it was prepared for — inheriting \
             the parent's core is the deadlock this whole handover prevents"
        );
        assert_eq!(
            parent_after, parent_before,
            "the parent must get its own pin back after the spawn"
        );
    }

    /// The `0` sentinel means "do not pin", and it has to mean that for
    /// the *child* too: an unpinned child of a pinned parent must get
    /// the full mask, not the parent's single core. `pin_thread(_, 0)`
    /// returns without touching affinity, so if the handover did not
    /// widen the mask the child would silently inherit the parent's
    /// core — which is exactly the `compact` core profile's
    /// configuration.
    #[test]
    fn an_unpinned_child_of_a_pinned_parent_gets_the_full_mask() {
        let full_width = affinity_width();
        if full_width < 2 {
            return;
        }

        let child_width = std::thread::spawn(move || {
            pin_to_core(0).expect("core 0 always exists");
            let saved = take_context().expect("snapshot context");
            prepare_child_context(0).expect("prepare unpinned child");
            let width = std::thread::spawn(affinity_width)
                .join()
                .expect("child thread");
            restore_context(&saved).expect("restore context");
            width
        })
        .join()
        .expect("parent thread");

        assert_eq!(
            child_width, full_width,
            "an unpinned child must float across every CPU, not inherit the \
             parent's pin"
        );
    }

    /// `restore_context` must put back the policy and priority it was
    /// given, not just the mask — the journal thread's `SCHED_FIFO` is
    /// dropped during the handover and has to come back.
    #[test]
    fn restore_context_round_trips_policy_and_priority() {
        std::thread::spawn(|| {
            let before = take_context().expect("snapshot");
            let (policy_before, prio_before) = (before.policy, before.priority);

            prepare_child_context(0).expect("prepare");
            restore_context(&before).expect("restore");

            let after = take_context().expect("snapshot again");
            assert_eq!(after.policy, policy_before, "policy must round-trip");
            assert_eq!(
                after.priority, prio_before,
                "priority must round-trip alongside the policy"
            );
        })
        .join()
        .expect("thread");
    }

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

    #[test]
    fn cpu_list_membership() {
        // Single inclusive range.
        assert!(cpu_list_contains("2-7", 2));
        assert!(cpu_list_contains("2-7", 7));
        assert!(cpu_list_contains("2-7", 5));
        assert!(!cpu_list_contains("2-7", 1));
        assert!(!cpu_list_contains("2-7", 8));
        // Singletons.
        assert!(cpu_list_contains("1,3,5", 3));
        assert!(!cpu_list_contains("1,3,5", 4));
        // Mixed ranges + singletons.
        assert!(cpu_list_contains("2-4,6-8", 7));
        assert!(cpu_list_contains("2-4,6-8", 3));
        assert!(!cpu_list_contains("2-4,6-8", 5));
        assert!(cpu_list_contains("0,2-4,9", 9));
        // Empty (no isolcpus) — nothing is isolated.
        assert!(!cpu_list_contains("", 0));
        assert!(!cpu_list_contains("", 2));
        // Malformed tokens never match (defensive parse of external data).
        assert!(!cpu_list_contains("x,2-", 2));
        assert!(!cpu_list_contains("foo", 0));
    }
}
