//! Scheduling helpers for tests that check where, and in what context, a
//! node's threads start.
//!
//! A new thread inherits its creator's CPU mask and scheduling policy, and
//! the runtime hands each pipeline thread its own before it exists (see
//! `melin_app::affinity::spawn_placed`). Where a thread *started* cannot be
//! read back once it has pinned itself, but its policy can: pinning to a
//! core that is not isolated leaves the policy alone. So a test spawns the
//! node from a thread at [`SCHED_BATCH`](set_batch_policy), which needs no
//! privilege, and a placed thread still at `SCHED_BATCH` afterwards
//! inherited its spawner's context instead of being handed its own.

/// The first CPU other than 0 that the calling thread may run on: a core a
/// test can place a thread on that is not the `0` "unpinned" sentinel.
/// `None` on a host, or in a cpuset, that offers no such CPU.
pub fn first_pinnable_cpu() -> Option<usize> {
    let mask = unsafe {
        let mut mask: libc::cpu_set_t = std::mem::zeroed();
        assert_eq!(
            libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut mask),
            0,
            "sched_getaffinity failed: {}",
            std::io::Error::last_os_error()
        );
        mask
    };
    (1..libc::CPU_SETSIZE as usize).find(|&cpu| unsafe { libc::CPU_ISSET(cpu, &mask) })
}

/// Put the calling thread at `SCHED_BATCH`: a policy the runtime never
/// gives a thread, that needs no privilege, and that pinning to a core that
/// is not isolated leaves alone. Every thread spawned from this one after
/// starts at it unless handed a context of its own.
pub fn set_batch_policy() {
    let param = libc::sched_param { sched_priority: 0 };
    assert_eq!(
        unsafe { libc::sched_setscheduler(0, libc::SCHED_BATCH, &param) },
        0,
        "sched_setscheduler(SCHED_BATCH) failed: {}",
        std::io::Error::last_os_error()
    );
}

/// The name a thread called `name` shows in `/proc`: the kernel keeps the
/// first 15 bytes of a thread's name (`TASK_COMM_LEN` less the NUL).
pub fn comm(name: &str) -> &str {
    let mut end = name.len().min(15);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    &name[..end]
}

/// Every live thread of this process whose name is one of `names`, as
/// `(name as /proc shows it, scheduling policy)`.
pub fn threads_named(names: &[&str]) -> Vec<(String, libc::c_int)> {
    let wanted: Vec<&str> = names.iter().map(|name| comm(name)).collect();
    let mut found = Vec::new();
    let tasks = std::fs::read_dir("/proc/self/task").expect("list this process's threads");
    for entry in tasks {
        // A thread can exit between the listing and any read below; it is
        // then simply not live any more, which is what this reports.
        let Ok(entry) = entry else { continue };
        let Ok(name) = std::fs::read_to_string(entry.path().join("comm")) else {
            continue;
        };
        let name = name.trim_end();
        if !wanted.contains(&name) {
            continue;
        }
        let Ok(tid) = entry.file_name().to_string_lossy().parse::<libc::pid_t>() else {
            continue;
        };
        let policy = unsafe { libc::sched_getscheduler(tid) };
        // Negative when the thread exited after its name was read.
        if policy < 0 {
            continue;
        }
        found.push((name.to_owned(), policy));
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comm_keeps_what_the_kernel_keeps() {
        assert_eq!(comm("journal-seq"), "journal-seq");
        assert_eq!(comm("replica-receiver"), "replica-receive");
    }

    /// A thread is found by its name, with the policy it runs at.
    #[test]
    fn threads_named_finds_a_named_thread_and_its_policy() {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("sched-test-a-long-name".into())
            .spawn(move || {
                set_batch_policy();
                ready_tx.send(()).expect("signal ready");
                // Dropped sender ends the wait: the test is done looking.
                let _ = done_rx.recv();
            })
            .expect("spawn");
        ready_rx.recv().expect("thread ready");

        let found = threads_named(&["sched-test-a-long-name"]);
        drop(done_tx);
        thread.join().expect("thread");
        assert_eq!(
            found,
            vec![("sched-test-a-lo".to_owned(), libc::SCHED_BATCH)]
        );
    }
}
