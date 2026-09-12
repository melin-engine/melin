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
//! **A pipeline core of `0` means "unpinned"**, and unpinned means the
//! process's *home mask*: the CPU set it was started with (see
//! [`capture_home_mask`](crate::affinity::capture_home_mask)). That set
//! already reflects everything that restricts the process — `isolcpus`
//! confines pid 1, and so every process, to the non-isolated CPUs; a
//! cgroup cpuset, systemd's `CPUAffinity=` or `taskset` narrow it
//! further — so an unpinned thread runs where the operator left room and
//! never on an isolated core. [`pin_thread`](crate::affinity::pin_thread)
//! and [`prepare_child_context`](crate::affinity::prepare_child_context)
//! with `0`, and [`clear_affinity`](crate::affinity::clear_affinity), all
//! apply it. Handing out "every CPU" instead is not the same thing: a
//! thread whose mask allows its creator's isolated core starts on that
//! core, and the scheduler never moves it off. Production deployments
//! never run pipeline threads on core 0 (it is reserved for the kernel,
//! IRQ handlers, and other system processes), so the value is free to
//! repurpose. This lets the
//! integration tests pass `--cores none` without cramming every
//! pipeline thread of every spawned server onto a single physical CPU
//! — which previously caused the io_uring reader to starve under
//! contention and the failover suite to time out.
//!
//! The lower-level [`pin_to_core`](crate::affinity::pin_to_core) still
//! pins literally — non-pipeline
//! callers (e.g. the bench progress thread that pins to core 0 on
//! purpose to stay off the bench cores) keep the old semantics.

/// The process's home mask — the CPU set an unpinned thread runs on.
///
/// The calling thread's affinity at capture, taken before anything here
/// changes a thread's placement: explicitly by [`capture_home_mask`] at
/// startup, or else implicitly by the first function here that changes
/// one. `None` when the mask could not be read, in which case unpinned
/// threads get every CPU, as they did before there was a home mask.
///
/// A `OnceLock` because it is written once and read from threads that may
/// start concurrently; `cpu_set_t` because it is what the affinity
/// syscalls take, so applying it converts nothing.
static HOME_MASK: std::sync::OnceLock<Option<libc::cpu_set_t>> = std::sync::OnceLock::new();

/// Record the calling thread's CPU mask as the process's home mask, unless
/// one is already recorded.
///
/// Call it first thing at startup, on the main thread: before any thread is
/// pinned, and before initialising anything that narrows the main thread's
/// own mask — DPDK's EAL can pin it to one lcore, and a home mask captured
/// after that would confine every unpinned thread to that core. Without a
/// call, the first function here that changes a thread's placement captures
/// it from its caller, which is right as long as nothing outside this
/// module narrowed that thread first.
pub fn capture_home_mask() {
    home_mask();
}

/// The home mask, captured from the calling thread on first use.
fn home_mask() -> Option<libc::cpu_set_t> {
    *HOME_MASK.get_or_init(|| match current_mask() {
        Ok(mask) => Some(mask),
        Err(e) => {
            tracing::warn!(
                error = e,
                "cannot read the process CPU mask; unpinned threads get every CPU"
            );
            None
        }
    })
}

/// The calling thread's affinity mask.
fn current_mask() -> Result<libc::cpu_set_t, String> {
    unsafe {
        let mut mask: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut mask) != 0 {
            return Err(format!(
                "sched_getaffinity failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(mask)
    }
}

/// Every CPU the mask type can name; the kernel ignores the bits of CPUs
/// the machine does not have.
fn all_cpus() -> libc::cpu_set_t {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        for i in 0..libc::CPU_SETSIZE as usize {
            libc::CPU_SET(i, &mut set);
        }
        set
    }
}

/// Set the calling thread's affinity mask. `what` names the caller in the
/// error.
fn set_mask(mask: &libc::cpu_set_t, what: &str) -> Result<(), String> {
    let ret = unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), mask) };
    if ret != 0 {
        return Err(format!(
            "sched_setaffinity ({what}) failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

/// Put the calling thread on the home mask. If the kernel refuses it — the
/// process's cpuset shrank since startup, leaving no CPU in common — fall
/// back to every CPU, which the kernel intersects with whatever the process
/// may still use.
fn set_unpinned_mask() -> Result<(), String> {
    let Some(home) = home_mask() else {
        return set_mask(&all_cpus(), "unpin");
    };
    set_mask(&home, "unpin").or_else(|e| {
        tracing::warn!(
            error = e,
            "the home CPU mask no longer applies; unpinning across every CPU instead"
        );
        set_mask(&all_cpus(), "unpin")
    })
}

/// Drop the calling thread to default `SCHED_OTHER` scheduling. `what`
/// names the caller in the error.
fn set_other_policy(what: &str) -> Result<(), String> {
    let param = libc::sched_param { sched_priority: 0 };
    if unsafe { libc::sched_setscheduler(0, libc::SCHED_OTHER, &param) } != 0 {
        return Err(format!(
            "sched_setscheduler ({what}) failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

/// Pin the calling thread to the specified logical CPU core, and grant it
/// `SCHED_FIFO` real-time scheduling priority when the core is isolated.
///
/// Must be called from within the target thread (uses tid 0 = "self").
/// Returns the core ID on success for logging convenience.
///
/// Affinity is always set. `SCHED_FIFO` is granted only on a non-zero core
/// that the kernel reports isolated (listed in
/// `/sys/devices/system/cpu/isolated`, i.e. booted with `isolcpus=`) — see
/// `core_is_isolated`. On a shared core a busy-spinning RT thread would
/// starve every `SCHED_OTHER` thread co-located with it, so RT priority is
/// withheld there (the thread keeps plain affinity). Core 0 is the shared
/// housekeeping core and never gets RT priority regardless.
///
/// `SCHED_FIFO` failure is non-fatal: the thread continues with default
/// scheduling. This allows running without `CAP_SYS_NICE` during
/// development while getting real-time priority in production.
pub fn pin_to_core(core_id: usize) -> Result<usize, String> {
    // Record the home mask before the first change to any thread's
    // placement, if startup did not already.
    home_mask();
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
/// `core == 0` is the unpinned sentinel: the thread is put on the home
/// mask under default scheduling, whatever it inherited. See module docs
/// for rationale.
pub fn pin_thread(name: &str, core: usize) {
    if core == 0 {
        match clear_affinity() {
            Ok(()) => tracing::info!(thread = name, "thread left unpinned, on the home CPU mask"),
            Err(e) => tracing::warn!(thread = name, error = e, "could not unpin thread"),
        }
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
/// *next* should inherit: pinned to `core`, or on the home mask when
/// `core` is the `0` sentinel, and always `SCHED_OTHER`.
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
/// ```text
/// let saved = take_context()?;
/// prepare_child_context(child_core);
/// let handle = std::thread::Builder::new().spawn(move || { … })?;
/// restore_context(&saved)?;
/// ```
///
/// The child then starts already on its own core under `SCHED_OTHER`,
/// free to run and promote itself with [`pin_thread`].
pub fn prepare_child_context(core: usize) -> Result<(), String> {
    // Record the home mask while this thread still has its own placement.
    home_mask();
    const MAX_CPUS: usize = 1024;
    if core >= MAX_CPUS {
        return Err(format!("core {core} exceeds maximum ({MAX_CPUS})"));
    }
    // Drop to SCHED_OTHER before widening the mask: the child must
    // not inherit real-time priority it has not earned, and the
    // parent must not sit at RT priority on a wide mask even
    // momentarily.
    set_other_policy("child prep")?;
    if core == 0 {
        // The `0` sentinel means "unpinned": the home mask. Not this
        // thread's core, and not every CPU either, which would let the
        // child start on this thread's core when that core is isolated.
        return set_unpinned_mask();
    }
    let set = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(core, &mut set);
        set
    };
    set_mask(&set, "child prep")
}

/// Put the calling thread on the home mask under default `SCHED_OTHER`
/// scheduling.
///
/// Child threads spawned from a pinned parent inherit both the parent's
/// single-core affinity mask and its `SCHED_FIFO` policy. Call this at
/// the start of the child thread to give it the mask every unpinned
/// thread gets, and default scheduling. A thread calling it on itself
/// moves off its current core at once if that core is not in the home
/// mask, so threads it spawns afterwards start inside the home mask too.
///
/// Only usable by a child that can actually run — see
/// [`prepare_child_context`] for why a child of a pinned RT parent
/// cannot, and must be handed its context instead.
pub fn clear_affinity() -> Result<(), String> {
    home_mask();
    // Policy before mask, as in `prepare_child_context`: a non-pinned
    // thread left at SCHED_FIFO could starve other work on shared cores,
    // and must not sit at real-time priority on a wide mask even
    // momentarily.
    set_other_policy("clear")?;
    set_unpinned_mask()
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
            "clear_affinity must put the calling thread on the home mask"
        );
        assert_eq!(
            parent_after, 1,
            "the child's clear_affinity must not touch the parent's pin"
        );
    }

    /// Mask of the calling thread, as a sorted core list.
    fn affinity_cores() -> Vec<usize> {
        cores(&current_mask().expect("sched_getaffinity"))
    }

    /// A mask as a sorted core list.
    fn cores(set: &libc::cpu_set_t) -> Vec<usize> {
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| unsafe { libc::CPU_ISSET(c, set) })
            .collect()
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

    /// The `0` sentinel means "unpinned", and it has to mean that for the
    /// *child* too: an unpinned child of a pinned parent must get the home
    /// mask — not the parent's single core, and not every CPU, which on an
    /// isolated parent core would let it start there and stay.
    #[test]
    fn an_unpinned_child_of_a_pinned_parent_gets_the_home_mask() {
        capture_home_mask();
        let home = cores(&home_mask().expect("the test thread's mask is readable"));
        if home.len() < 2 {
            return; // one CPU: parent and child cannot be told apart
        }

        let child_cores = std::thread::spawn(|| {
            pin_to_core(0).expect("core 0 always exists");
            let saved = take_context().expect("snapshot context");
            prepare_child_context(0).expect("prepare unpinned child");
            let child = std::thread::spawn(affinity_cores)
                .join()
                .expect("child thread");
            restore_context(&saved).expect("restore context");
            child
        })
        .join()
        .expect("parent thread");

        assert_eq!(
            child_cores, home,
            "an unpinned child must get the home mask, not the parent's pin"
        );
    }

    /// A thread told to run unpinned goes back on the home mask whatever
    /// it inherited — a single core included. `pin_thread(_, 0)` used to
    /// leave the mask alone, so a thread spawned from a narrowed parent
    /// stayed narrowed.
    #[test]
    fn pin_thread_zero_puts_a_pinned_thread_back_on_the_home_mask() {
        capture_home_mask();
        let home = cores(&home_mask().expect("the test thread's mask is readable"));
        if home.len() < 2 {
            return; // one CPU: pinned and unpinned cannot be told apart
        }

        let (pinned, unpinned) = std::thread::spawn(|| {
            pin_to_core(0).expect("core 0 always exists");
            let pinned = affinity_cores();
            pin_thread("unpinned-test", 0);
            (pinned, affinity_cores())
        })
        .join()
        .expect("thread");

        assert_eq!(pinned, vec![0], "the thread pinned itself to core 0");
        assert_eq!(
            unpinned, home,
            "pin_thread(_, 0) must put the thread back on the home mask"
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
