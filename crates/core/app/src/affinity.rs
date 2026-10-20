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
//! [`capture_home_mask`](crate::affinity::capture_home_mask)), less the
//! CPUs the kernel reports isolated. The startup set reflects what the
//! operator restricted the process to — a cgroup cpuset, systemd's
//! `CPUAffinity=`, `taskset` — so an unpinned thread runs where they left
//! room. It does not reliably exclude isolated CPUs: `isolcpus` confines
//! pid 1 to the housekeeping CPUs, but a process started under
//! `numactl --cpunodebind` or `taskset` is handed isolated ones back. An
//! isolated CPU is never load-balanced, so a thread that lands on one
//! stays, and a thread spawned from there starts there — beside whatever
//! real-time thread spins on it, and never runs. So the isolated CPUs
//! come out of the home mask, unless they are all the process was given.
//! [`pin_thread`](crate::affinity::pin_thread) and
//! [`spawn_placed`](crate::affinity::spawn_placed) with `0`, and
//! [`clear_affinity`](crate::affinity::clear_affinity), all apply it. Handing out "every CPU" instead is not the same thing, for
//! the same reason. Production deployments
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
/// The calling thread's affinity at capture, less the isolated CPUs (see
/// [`without_isolated`]), taken before anything here changes a thread's
/// placement: explicitly by [`capture_home_mask`] at startup, or else
/// implicitly by the first function here that changes one. `None` when
/// the mask could not be read, in which case unpinned threads get every
/// housekeeping CPU.
///
/// A `OnceLock` because it is written once and read from threads that may
/// start concurrently; `cpu_set_t` because it is what the affinity
/// syscalls take, so applying it converts nothing.
static HOME_MASK: std::sync::OnceLock<Option<libc::cpu_set_t>> = std::sync::OnceLock::new();

/// Record the calling thread's CPU mask, less the isolated CPUs, as the
/// process's home mask, unless one is already recorded; then move the
/// calling thread into it.
///
/// Call it first thing at startup, on the main thread: before any thread is
/// pinned, and before initialising anything that narrows the main thread's
/// own mask — DPDK's EAL can pin it to one lcore, and a home mask captured
/// after that would confine every unpinned thread to that core. Without a
/// call, the first function here that changes a thread's placement captures
/// it from its caller, which is right as long as nothing outside this
/// module narrowed that thread first.
///
/// The move matters as much as the record: every thread the main thread
/// spawns without a placement of its own starts on the CPU the main thread
/// is on, and if that CPU is isolated, it starts — and stays — there. The
/// caller is only ever narrowed, never widened, so a second call from a
/// thread the EAL already pinned leaves it alone.
pub fn capture_home_mask() {
    let Some(home) = home_mask() else {
        return;
    };
    let current = match current_mask() {
        Ok(mask) => mask,
        Err(e) => {
            tracing::warn!(error = e, "cannot read the startup thread's CPU mask");
            return;
        }
    };
    let narrowed = intersection(&current, &home);
    if count(&narrowed) > 0
        && count(&narrowed) < count(&current)
        && let Err(e) = set_mask(&narrowed, "home")
    {
        tracing::warn!(
            error = e,
            "cannot move the startup thread off the isolated CPUs; threads it spawns \
             may start on one"
        );
    }
}

/// The home mask, captured from the calling thread on first use.
fn home_mask() -> Option<libc::cpu_set_t> {
    *HOME_MASK.get_or_init(|| match current_mask() {
        Ok(mask) => Some(home_from(&mask, &isolated_cpu_list())),
        Err(e) => {
            tracing::warn!(
                error = e,
                "cannot read the process CPU mask; unpinned threads get every housekeeping CPU"
            );
            None
        }
    })
}

/// The home mask a startup mask makes, given the kernel's isolated-CPU
/// list: the startup mask without the isolated CPUs, or the whole startup
/// mask, with a warning, when it holds nothing else.
fn home_from(startup: &libc::cpu_set_t, isolated: &str) -> libc::cpu_set_t {
    match without_isolated(startup, isolated) {
        Some(home) => home,
        None => {
            tracing::warn!(
                isolated,
                "every CPU this process may use is isolated; unpinned threads share \
                 isolated CPUs, which the scheduler does not balance — a thread placed \
                 beside a real-time pipeline thread there will not run"
            );
            *startup
        }
    }
}

/// `mask` without the CPUs in `isolated` (a Linux CPU list, as in
/// `/sys/devices/system/cpu/isolated`), or `None` when that leaves no CPU.
fn without_isolated(mask: &libc::cpu_set_t, isolated: &str) -> Option<libc::cpu_set_t> {
    let mut kept = *mask;
    for cpu in cpus_of(mask) {
        if cpu_list_contains(isolated, cpu) {
            unsafe { libc::CPU_CLR(cpu, &mut kept) };
        }
    }
    (count(&kept) > 0).then_some(kept)
}

/// The CPUs in both masks.
fn intersection(a: &libc::cpu_set_t, b: &libc::cpu_set_t) -> libc::cpu_set_t {
    let mut both = *a;
    for cpu in cpus_of(a) {
        if !unsafe { libc::CPU_ISSET(cpu, b) } {
            unsafe { libc::CPU_CLR(cpu, &mut both) };
        }
    }
    both
}

/// The CPUs in a mask, in ascending order.
fn cpus_of(mask: &libc::cpu_set_t) -> impl Iterator<Item = usize> + '_ {
    (0..libc::CPU_SETSIZE as usize).filter(move |&cpu| unsafe { libc::CPU_ISSET(cpu, mask) })
}

/// The lowest CPU the calling thread may run on: one it is already
/// allowed, so pinning to it cannot fail on a host that restricts the
/// process (a cgroup cpuset, `taskset`), where CPU 0 need not be in the
/// set. `None` when the mask cannot be read.
pub fn first_allowed_cpu() -> Option<usize> {
    // Dropped error: `None` is the answer for an unreadable mask, and a
    // caller that needs a CPU fails on it with its own context.
    let mask = current_mask().ok()?;
    cpus_of(&mask).next()
}

/// How many CPUs a mask holds.
fn count(mask: &libc::cpu_set_t) -> usize {
    cpus_of(mask).count()
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

/// Every housekeeping CPU the mask type can name: [`all_cpus`] without the
/// isolated ones, for the same reason the home mask leaves them out.
fn housekeeping_cpus() -> libc::cpu_set_t {
    home_from(&all_cpus(), &isolated_cpu_list())
}

/// Put the calling thread on the home mask. If the kernel refuses it — the
/// process's cpuset shrank since startup, leaving no CPU in common — fall
/// back to every housekeeping CPU, which the kernel intersects with
/// whatever the process may still use.
fn set_unpinned_mask() -> Result<(), String> {
    let Some(home) = home_mask() else {
        return set_mask(&housekeeping_cpus(), "unpin");
    };
    set_mask(&home, "unpin")
        .or_else(|e| {
            tracing::warn!(
                error = e,
                "the home CPU mask no longer applies; unpinning across every housekeeping CPU \
                 instead"
            );
            set_mask(&housekeeping_cpus(), "unpin")
        })
        // The cpuset now holds isolated CPUs only: those are all the
        // kernel will let the thread run on.
        .or_else(|_| set_mask(&all_cpus(), "unpin"))
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
    cpu_list_contains(&isolated_cpu_list(), core_id)
}

/// The kernel's isolated-CPU list, `/sys/devices/system/cpu/isolated`;
/// empty when nothing is isolated.
fn isolated_cpu_list() -> String {
    match std::fs::read_to_string("/sys/devices/system/cpu/isolated") {
        Ok(list) => list.trim().to_owned(),
        // No isolcpus configured (or sysfs unavailable) → nothing isolated.
        Err(_) => String::new(),
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

/// A thread's scheduling context: CPU affinity mask, the CPU it is on,
/// policy and priority. Captured by [`take_context`] and put back by
/// [`restore_context`].
///
/// This and the three steps of the handover (take, prepare, restore) are
/// private: [`spawn_placed`] and [`with_child_context`] are the only way to
/// spawn a placed thread, so the handover cannot be done by hand and done
/// partly, which is how pipeline threads came to start on a core a
/// real-time thread held.
struct SchedContext {
    mask: libc::cpu_set_t,
    /// The CPU the thread was running on, when the kernel could say. A
    /// mask alone does not say where a thread is, and the handover in
    /// [`prepare_child_context`] moves it — see [`restore_context`].
    /// `usize` because it indexes a `cpu_set_t`.
    cpu: Option<usize>,
    policy: libc::c_int,
    priority: libc::c_int,
}

/// Snapshot the calling thread's affinity mask, the CPU it is on,
/// scheduling policy and priority.
fn take_context() -> Result<SchedContext, String> {
    unsafe {
        let mut mask: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut mask) != 0 {
            return Err(format!(
                "sched_getaffinity failed: {}",
                std::io::Error::last_os_error()
            ));
        }
        // -1 when the kernel cannot say; restoring then falls back to
        // the mask alone, which is all there was before this field.
        let cpu = usize::try_from(libc::sched_getcpu()).ok();
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
            cpu,
            policy,
            priority: param.sched_priority,
        })
    }
}

/// Put a snapshot from [`take_context`] back on the calling thread: its
/// policy and mask, and the CPU it was on.
///
/// The CPU is the part a mask cannot restore. [`prepare_child_context`]
/// moves the thread onto the child's core; widening the mask back would
/// leave it there whenever the mask allows that core, because the kernel
/// moves a thread only off CPUs its new mask excludes. On an isolated core
/// nothing moves it later, so it would stay beside the child it just
/// spawned — which turns real-time and spins — and so would every thread
/// it spawns after.
fn restore_context(ctx: &SchedContext) -> Result<(), String> {
    // Back to the CPU first, while still at the handover's SCHED_OTHER,
    // so the thread is never real-time on the child's core.
    if let Some(cpu) = ctx.cpu.filter(|&cpu| {
        cpu < libc::CPU_SETSIZE as usize && unsafe { libc::CPU_ISSET(cpu, &ctx.mask) }
    }) {
        let only = unsafe {
            let mut only: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_SET(cpu, &mut only);
            only
        };
        if let Err(e) = set_mask(&only, "restore") {
            // The CPU went away since the snapshot (offline, or the
            // cpuset shrank). The mask below still applies; only the
            // return to that exact CPU is lost.
            tracing::warn!(
                cpu,
                error = e,
                "could not move back to the CPU the thread was on"
            );
        }
    }
    unsafe {
        let param = libc::sched_param {
            sched_priority: ctx.priority,
        };
        // Policy before the mask widens: the thread is on its own CPU
        // again (or still on the single core it was handed), so even a
        // real-time policy never lands on a wider mask than it had.
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
/// `core` is the `0` sentinel, and always `SCHED_OTHER`. The middle step
/// of the handover [`with_child_context`] runs; see [`spawn_placed`] for
/// why it exists.
fn prepare_child_context(core: usize) -> Result<(), String> {
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

/// Run `spawn`, which creates the thread `name`, with the calling thread
/// in the scheduling context that thread must start in — pinned to
/// `core`, or on the home mask for the `0` sentinel, at `SCHED_OTHER` —
/// then put the calling thread back where it was, CPU included.
///
/// For a spawn [`spawn_placed`] cannot express, such as a scoped thread.
/// The spawned thread still has to [`pin_thread`] itself to `core` first
/// thing, which is what promotes it to real-time on an isolated core.
///
/// A failed snapshot skips the handover rather than strand the calling
/// thread on the child's core: the child then inherits the caller's
/// context, as every thread did before this existed.
pub fn with_child_context<R>(name: &str, core: usize, spawn: impl FnOnce() -> R) -> R {
    let saved = match take_context() {
        Ok(saved) => saved,
        Err(e) => {
            tracing::warn!(
                thread = name,
                error = e,
                "cannot snapshot the spawning thread's scheduling context; the thread \
                 starts in it instead of its own"
            );
            return spawn();
        }
    };
    if let Err(e) = prepare_child_context(core) {
        tracing::warn!(
            thread = name,
            error = e,
            "cannot prepare the thread's scheduling context"
        );
    }
    let spawned = spawn();
    // Restored whatever the spawn returned: a failed spawn must not leave
    // the calling thread on the child's core either.
    if let Err(e) = restore_context(&saved) {
        tracing::error!(
            thread = name,
            error = e,
            "the spawning thread could not restore its own scheduling context"
        );
    }
    spawned
}

/// How long [`spawn_placed`] waits for a new thread to start and pin
/// itself. A thread that cannot start never does — something on its core
/// never yields — so any bound catches it; this one is generous so that a
/// loaded machine (a CI host running many nodes at once) never trips it on
/// a thread that is merely slow to be scheduled. It is paid only by a
/// thread that is stuck.
const PLACED_START_BOUND: std::time::Duration = std::time::Duration::from_secs(10);

/// Why [`spawn_placed`] did not hand back a running thread.
pub enum SpawnPlacedError<T> {
    /// The thread could not be created.
    Spawn(std::io::Error),
    /// The thread was created but did not start and pin itself within
    /// the bound: whatever holds its core never yields. It carries the
    /// thread's handle, because the thread exists and may yet run; the
    /// caller decides whether that fails it or is kept and reported.
    NotStarted {
        name: String,
        core: usize,
        handle: std::thread::JoinHandle<T>,
    },
}

impl<T> SpawnPlacedError<T> {
    /// For a caller that keeps a stuck thread rather than fail on it:
    /// reports it at `error!` and hands its handle back. A thread that
    /// could not be created at all is still an error.
    pub fn keep_stuck(self) -> std::io::Result<std::thread::JoinHandle<T>> {
        match self {
            Self::Spawn(e) => Err(e),
            Self::NotStarted { name, core, handle } => {
                tracing::error!(
                    "{}; kept, in case it runs later",
                    not_started_message(&name, core)
                );
                Ok(handle)
            }
        }
    }
}

/// What [`SpawnPlacedError::NotStarted`] says.
fn not_started_message(name: &str, core: usize) -> String {
    let bound = PLACED_START_BOUND.as_secs();
    if core == 0 {
        format!(
            "thread {name} did not start within {bound}s on the home CPU mask; \
             something there never yields the CPU"
        )
    } else {
        format!(
            "thread {name} did not start within {bound}s on core {core}; another \
             thread is likely spinning there at real-time priority — check the \
             --cores layout and what else is pinned to that core"
        )
    }
}

impl<T> std::fmt::Display for SpawnPlacedError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spawn(e) => write!(f, "cannot create the thread: {e}"),
            Self::NotStarted { name, core, .. } => f.write_str(&not_started_message(name, *core)),
        }
    }
}

impl<T> std::fmt::Debug for SpawnPlacedError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl<T> std::error::Error for SpawnPlacedError<T> {}

impl<T> From<SpawnPlacedError<T>> for std::io::Error {
    fn from(e: SpawnPlacedError<T>) -> Self {
        match e {
            SpawnPlacedError::Spawn(e) => e,
            not_started => {
                std::io::Error::new(std::io::ErrorKind::TimedOut, not_started.to_string())
            }
        }
    }
}

/// Spawn the thread `name` placed on `core` (`0` = unpinned): created
/// already in that placement's context by [`with_child_context`], and
/// pinned by [`pin_thread`] before it runs `f`. Returns once the thread
/// has pinned itself, or with [`SpawnPlacedError::NotStarted`] when it has
/// not within a bound.
///
/// The way to spawn any thread that has a placement.
///
/// # Why the placement cannot be left to the thread itself
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
/// child exists: it adopts the child's context, spawns, and goes back to
/// its own, CPU included (left on the child's core, it would create every
/// later thread there). The child then starts already on its own core
/// under `SCHED_OTHER`, free to run and promote itself with
/// [`pin_thread`]. That handover is private to this function and
/// [`with_child_context`]: a thread that pins itself and is spawned any
/// other way is this bug waiting for a host that triggers it.
///
/// # The wait
///
/// It turns the failure that remains — a thread whose core is held by
/// something that never yields — from a silent stall, noticed only when
/// the work it never does backs up, into an error that names the thread
/// and its core.
pub fn spawn_placed<T, F>(
    name: &str,
    core: usize,
    f: F,
) -> Result<std::thread::JoinHandle<T>, SpawnPlacedError<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    spawn_checked_in(name, core, PLACED_START_BOUND, pin_thread, f)
}

/// [`spawn_placed`] with its bound and the thread's first step, `start`
/// (pinning, in production), as parameters, so a test can stand in for a
/// thread that cannot start.
fn spawn_checked_in<T, F>(
    name: &str,
    core: usize,
    bound: std::time::Duration,
    start: fn(&str, usize),
    f: F,
) -> Result<std::thread::JoinHandle<T>, SpawnPlacedError<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let thread_name = name.to_owned();
    // Capacity one so the check-in never blocks the thread, whether or
    // not the spawner is still waiting for it.
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel::<()>(1);
    let handle = with_child_context(name, core, || {
        std::thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                start(&thread_name, core);
                // Fails only when the spawner gave up waiting and dropped
                // the receiver; it has reported the thread stuck by then,
                // and the thread runs on regardless.
                let _ = started_tx.send(());
                f()
            })
    })
    .map_err(SpawnPlacedError::Spawn)?;
    // Waited for after the spawner is back in its own context, so the
    // wait is not spent on the thread's core.
    match started_rx.recv_timeout(bound) {
        Ok(()) => Ok(handle),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(SpawnPlacedError::NotStarted {
            name: name.to_owned(),
            core,
            handle,
        }),
        // The sender went away unsent: the thread ran, and panicked in
        // `start`. Joining the handle reports the panic.
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Ok(handle),
    }
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
/// Only usable by a child that can actually run — see [`spawn_placed`]
/// for why a child of a pinned RT parent cannot, and must be handed its
/// context instead.
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
        cpus_of(set).collect()
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

    /// A mask holding exactly `cpus`.
    fn mask_of(cpus: &[usize]) -> libc::cpu_set_t {
        let mut mask: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        for &cpu in cpus {
            unsafe { libc::CPU_SET(cpu, &mut mask) };
        }
        mask
    }

    /// The CPU the calling thread is running on.
    fn current_cpu() -> usize {
        usize::try_from(unsafe { libc::sched_getcpu() }).expect("sched_getcpu")
    }

    /// The calling thread's scheduling policy.
    fn current_policy() -> libc::c_int {
        let policy = unsafe { libc::sched_getscheduler(0) };
        assert!(policy >= 0, "sched_getscheduler failed");
        policy
    }

    /// Put the calling thread at `SCHED_BATCH`: a policy no thread here
    /// is ever given, that needs no privilege, and that pinning to a core
    /// that is not isolated leaves alone — so a thread found at it after
    /// pinning itself inherited it.
    fn set_batch_policy() {
        let param = libc::sched_param { sched_priority: 0 };
        assert_eq!(
            unsafe { libc::sched_setscheduler(0, libc::SCHED_BATCH, &param) },
            0,
            "sched_setscheduler(SCHED_BATCH) failed: {}",
            std::io::Error::last_os_error()
        );
    }

    /// The home mask is the startup mask without the isolated CPUs: an
    /// isolated CPU is never balanced, so an unpinned thread that lands on
    /// one stays, beside whatever real-time thread spins there. A process
    /// started on isolated CPUs only keeps them — that was the operator's
    /// choice, and there is nowhere else to run.
    #[test]
    fn the_home_mask_leaves_out_isolated_cpus_unless_nothing_else_is_left() {
        let startup = mask_of(&[0, 1, 2, 3, 8, 9]);
        assert_eq!(cores(&home_from(&startup, "1-3,9-15")), vec![0, 8]);
        assert_eq!(cores(&home_from(&startup, "")), vec![0, 1, 2, 3, 8, 9]);
        assert_eq!(cores(&home_from(&startup, "4-7")), vec![0, 1, 2, 3, 8, 9]);

        let isolated_only = mask_of(&[2, 3]);
        assert!(without_isolated(&isolated_only, "1-7").is_none());
        assert_eq!(cores(&home_from(&isolated_only, "1-7")), vec![2, 3]);
    }

    /// The handover moves the spawning thread onto the child's core, and
    /// restoring has to move it back — not only widen its mask again,
    /// which leaves it wherever the wider mask allows. On an isolated core
    /// that is for good, beside the child it just spawned; every plain
    /// spawn after that starts there too. That is how four replica stage
    /// threads came to start, and starve, on the journal disk thread's
    /// core.
    #[test]
    fn restore_context_returns_the_thread_to_the_cpu_it_was_on() {
        let [a, b, ..] = affinity_cores()[..] else {
            return; // one CPU: nowhere to move to and back from
        };

        let (cpu_after, mask_after) = std::thread::spawn(move || {
            // On `a`, with a mask that also allows the child's core `b`.
            set_mask(&mask_of(&[a]), "test").expect("move to a");
            set_mask(&mask_of(&[a, b]), "test").expect("widen to a and b");

            let saved = take_context().expect("snapshot context");
            prepare_child_context(b).expect("prepare child context");
            std::thread::spawn(|| {}).join().expect("child thread");
            restore_context(&saved).expect("restore context");

            (current_cpu(), affinity_cores())
        })
        .join()
        .expect("spawning thread");

        assert_eq!(
            cpu_after, a,
            "the spawning thread must be back on its own CPU, not left on the child's"
        );
        assert_eq!(mask_after, vec![a, b], "and on its own mask");
    }

    /// [`spawn_placed`] creates the thread already in its placement's
    /// context — not the spawner's — and leaves the spawner with its own
    /// mask and policy.
    ///
    /// Not its CPU: `spawn_placed` sleeps until the thread checks in, and
    /// a thread that wakes may be placed on any CPU its mask allows (here,
    /// often the child's, which woke it). An isolated CPU is outside the
    /// scheduler domains a wakeup searches, so that never lands it on one.
    /// The return to its CPU, before the wait, is [`restore_context`]'s,
    /// tested in [`restore_context_returns_the_thread_to_the_cpu_it_was_on`].
    #[test]
    fn spawn_placed_starts_the_thread_in_its_own_context_and_puts_the_spawner_back() {
        let available = affinity_cores();
        // The child's core must not be the `0` "unpinned" sentinel.
        let Some(&child_core) = available.iter().find(|&&cpu| cpu != 0) else {
            return; // no CPU but 0
        };
        let Some(&spawner_cpu) = available.iter().find(|&&cpu| cpu != child_core) else {
            return; // one CPU: the spawner and the child cannot be told apart
        };

        let ((child_policy, child_cores), spawner) = std::thread::spawn(move || {
            set_mask(&mask_of(&[spawner_cpu]), "test").expect("move to the spawner's CPU");
            set_mask(&mask_of(&[spawner_cpu, child_core]), "test").expect("widen");
            set_batch_policy();

            let child = spawn_placed("placed-test", child_core, || {
                (current_policy(), affinity_cores())
            })
            .expect("spawn");
            let spawner = (affinity_cores(), current_policy());
            (child.join().expect("child thread"), spawner)
        })
        .join()
        .expect("spawning thread");

        assert_eq!(
            child_policy,
            libc::SCHED_OTHER,
            "the child must start in its own context, not inherit the spawner's"
        );
        assert_eq!(child_cores, vec![child_core], "and run pinned to its core");
        let mut spawner_mask = vec![spawner_cpu, child_core];
        spawner_mask.sort_unstable();
        assert_eq!(
            spawner,
            (spawner_mask, libc::SCHED_BATCH),
            "the spawner must be left with its own mask and policy"
        );
    }

    /// A thread that does not start within the bound comes back as
    /// `NotStarted`, naming it and its core and carrying its handle — and
    /// the thread still runs once it can. A first step that sleeps past
    /// the bound stands in for a core held by a real-time spinner, which
    /// a test cannot get without privilege.
    #[test]
    fn spawn_placed_reports_a_thread_that_does_not_start_in_time() {
        fn stuck(_: &str, _: usize) {
            std::thread::sleep(std::time::Duration::from_millis(300));
        }

        let spawned = spawn_checked_in(
            "stuck-test",
            0,
            std::time::Duration::from_millis(20),
            stuck,
            || 7,
        );

        match spawned {
            Err(SpawnPlacedError::NotStarted { name, core, handle }) => {
                assert_eq!((name.as_str(), core), ("stuck-test", 0));
                assert_eq!(handle.join().expect("thread"), 7, "it runs once it can");
            }
            Err(e) => panic!("expected NotStarted, got: {e}"),
            Ok(_) => panic!("a thread that never checked in was reported started"),
        }
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
