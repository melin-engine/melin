//! Test-only allocation of localhost listen addresses that a server will
//! re-bind later — see [`free_addr`]. Compiled only under the
//! `test-utils` feature; activate from a consuming crate's
//! `[dev-dependencies]`.

use std::collections::HashMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::SystemTime;

/// Ports per pid-derived block. Under plain `cargo test` every test in a
/// binary shares one process, and so one block, and this allocator never
/// reissues a port within a process: a binary whose tests together take
/// more than a block must hand ports back and run its tests one at a time
/// (as `melin-server-runtime`'s `ack_policy_changes.rs` and
/// `raft_failover.rs` do, through `tests/common/port_pool.rs`).
const BLOCK: u16 = 50;
/// Blocks per range: each `base` owns `base..base + BLOCK * NBLOCKS`
/// (5000 ports). Callers must space their bases accordingly.
const NBLOCKS: u16 = 100;

/// Allocate a localhost listen address on a probed-free port *below* the
/// kernel's ephemeral range (`net.ipv4.ip_local_port_range`, ≥32768 by
/// default), for tests that must hand a not-yet-bound address to a
/// server. Each calling test file owns a disjoint 5000-port range
/// starting at `base`; keep the ranges documented at the call sites.
///
/// The classic reserve-and-drop trick (`bind(port 0)`, read the port,
/// drop) hands back a port the kernel may immediately reissue — to a
/// concurrent test process's listener or as some outgoing connection's
/// source port — before the server re-binds it (observed as `AddrInUse`
/// spawn failures and never-forming clusters under full-suite load).
/// Below the ephemeral floor only explicit binds can collide, and those
/// are managed:
///
/// - The range is split into per-process blocks by pid, so concurrently
///   spawned test processes (near-adjacent pids) probe disjoint blocks.
/// - Two processes whose pids collide modulo [`NBLOCKS`] share a block,
///   but probe it under different random rotations, so they claim from
///   different parts of the block rather than racing for the same ports
///   in the same order.
/// - A port handed out once is never probed again by this process — it
///   stays unbound until its server starts, and re-probing it in that
///   window would reissue it.
/// - A port handed out is also claimed for the rest of the process's
///   life ([`claim`]), and no other process hands out a claimed port. The
///   bind probe alone cannot see a port whose server has stopped while
///   its test still uses the address: a stopped primary's replication
///   port, which its replicas keep redialing. Reissued to another test
///   process, that port once put a raft listener behind a replica's
///   primary address, and the replica, its key authorized there too,
///   took the raft node for its primary.
/// - The bind probe itself filters anything else alive on a port.
///
/// Panics when the block is exhausted: with [`BLOCK`] ports per block
/// and about a dozen listeners per test process, that means leaked
/// listeners, a foreign process squatting the range, or too many test
/// processes sharing the block. The message tells claimed ports from
/// bound ones.
pub fn free_addr(base: u16) -> SocketAddr {
    // Every port this process has handed out, with the claim that keeps
    // other processes off it: dropping the file would release the claim,
    // so it lives here until the process exits. HashMap over a bitmap or
    // sorted Vec: a handful of scattered u16s where membership is the
    // only query — no ordering or density to exploit. Mutex over an
    // atomic scheme: allocation runs a few times per test, so contention
    // is irrelevant, and holding the lock across the whole probe loop
    // serializes concurrent callers so they cannot race each other to
    // the same free port or observe a partially-updated map.
    static ISSUED: OnceLock<Mutex<HashMap<u16, File>>> = OnceLock::new();
    // Per-process random probe rotation — see the doc comment. Derived
    // from `RandomState`, std's per-process randomly-seeded hasher:
    // cheap decorrelation without a rand dependency.
    static ROTATION: OnceLock<u16> = OnceLock::new();

    // Keep the whole range below the default ephemeral floor — the
    // premise the scheme rests on.
    assert!(
        base.checked_add(BLOCK * NBLOCKS)
            .is_some_and(|top| top <= 32_768),
        "port range {base}..{} reaches into the kernel ephemeral range",
        u32::from(base) + u32::from(BLOCK) * u32::from(NBLOCKS),
    );

    // u16 arithmetic throughout — ports are u16 and every intermediate
    // value fits; pid is only reduced modulo NBLOCKS.
    let block_base = base + (std::process::id() % u32::from(NBLOCKS)) as u16 * BLOCK;
    let rotation = *ROTATION.get_or_init(|| {
        use std::hash::{BuildHasher, Hasher};
        let hasher = std::collections::hash_map::RandomState::new().build_hasher();
        (hasher.finish() % u64::from(BLOCK)) as u16
    });
    let mut issued = ISSUED
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("free_addr lock poisoned");
    // Why each port was passed over, for the exhaustion message. u16: a
    // count of ports within one block.
    let (mut held_here, mut held_elsewhere, mut bound) = (0u16, 0u16, 0u16);
    for i in 0..BLOCK {
        let port = block_base + (rotation + i) % BLOCK;
        if issued.contains_key(&port) {
            held_here += 1;
            continue;
        }
        // Claim before probing, so no other process can take the port
        // between the probe and the insert. A port the probe then finds
        // bound is released again as the claim drops.
        let Some(claimed) = claim(claims_dir(), port) else {
            held_elsewhere += 1;
            continue;
        };
        if let Ok(listener) = std::net::TcpListener::bind(("127.0.0.1", port)) {
            issued.insert(port, claimed);
            return listener
                .local_addr()
                .expect("local_addr on a bound listener");
        }
        bound += 1;
    }
    panic!(
        "no free port in block {block_base}..{}: {held_here} issued to this process, \
         {held_elsewhere} claimed by other test processes (claims in {}), {bound} bound",
        block_base + BLOCK,
        claims_dir().display(),
    );
}

/// The directory holding the claim files of this user's test processes,
/// created on first use: `melin-test-ports` in `$XDG_RUNTIME_DIR` when it
/// is set, else `melin-test-ports-<uid>` in the system temp directory.
///
/// The runtime directory is the user's own and is emptied at logout and
/// on reboot, when no claim can still be held; the temp directory is the
/// fallback where it is not set (CI runners), named per user since a
/// directory shared between users would hold files another user could
/// not open. Either may be cleaned of old files while a test runs, which
/// [`claim`] guards against. Processes that resolve different directories
/// (another user, another `$XDG_RUNTIME_DIR` or `$TMPDIR`) do not see
/// each other's claims, and are left to the bind probe and to their
/// distinct pid blocks.
fn claims_dir() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = match std::env::var_os("XDG_RUNTIME_DIR").map(PathBuf::from) {
            // The spec requires an absolute path and says to ignore any
            // other.
            Some(runtime) if runtime.is_absolute() => runtime.join("melin-test-ports"),
            _ => {
                // SAFETY: `geteuid` takes no arguments, touches no memory
                // of ours and cannot fail.
                let uid = unsafe { libc::geteuid() };
                std::env::temp_dir().join(format!("melin-test-ports-{uid}"))
            }
        };
        std::fs::create_dir_all(&dir)
            .unwrap_or_else(|e| panic!("create the port claims directory {}: {e}", dir.display()));
        dir
    })
}

/// Claim `port` for as long as the returned file is open, or `None` when
/// another holder has it. The claim is an exclusive `flock` on the port's
/// file in `dir`: the kernel drops it when the holder exits, however it
/// exits, so a killed test process leaves no stale claim behind. Two
/// opens of the file conflict even within one process, which is what lets
/// a test stand in for another process.
///
/// A claim file is never deleted by this code: unlinking one while it is
/// held would let the next claimant create a new file at the same path
/// and lock that, and two processes would hold the port. A cleaner that
/// removes files by age could do the same, and claim files are reused
/// from one run to the next, so each claim sets its file's modified time
/// to now: no age-based cleaning removes a file a test claimed moments
/// ago.
fn claim(dir: &Path, port: u16) -> Option<File> {
    let path = dir.join(format!("{port}.claim"));
    // Never truncated or written: the lock is the claim, the file's
    // contents nothing.
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap_or_else(|e| panic!("open the port claim {}: {e}", path.display()));
    match file.try_lock() {
        Ok(()) => {
            file.set_modified(SystemTime::now())
                .unwrap_or_else(|e| panic!("refresh the port claim {}: {e}", path.display()));
            Some(file)
        }
        Err(TryLockError::WouldBlock) => None,
        Err(TryLockError::Error(e)) => panic!("lock the port claim {}: {e}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // The unit tests take bases outside the ranges the cluster tests own
    // (election.rs: 15000, raft_failover.rs: 20000, raft_smoke.rs:
    // 25000). The issued set is process-global: the two tests sharing
    // base 5000 take `SHARED_BASE` (below), since each briefly claims
    // ports the other inspects.

    /// Serializes the tests on base 5000 when they run as threads of one
    /// process (`cargo test`); under nextest each has a process anyway.
    /// A `Mutex<()>`: nothing to guard but the order of the tests.
    static SHARED_BASE: Mutex<()> = Mutex::new(());

    /// Take [`SHARED_BASE`], whether or not a test that held it failed.
    fn shared_base() -> std::sync::MutexGuard<'static, ()> {
        SHARED_BASE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn never_reissues_and_stays_in_block() {
        const BASE: u16 = 10_000;
        let block_base = BASE + (std::process::id() % u32::from(NBLOCKS)) as u16 * BLOCK;
        let mut seen = HashSet::new();
        // No server ever binds these — every issued port stays unbound,
        // the worst case for accidental reissue.
        for _ in 0..10 {
            let port = free_addr(BASE).port();
            assert!(seen.insert(port), "port {port} issued twice");
            assert!(
                (block_base..block_base + BLOCK).contains(&port),
                "port {port} outside block {block_base}..{}",
                block_base + BLOCK
            );
            // Unbound, yet held: another process asking for a port is
            // refused this one.
            assert!(
                claim(claims_dir(), port).is_none(),
                "issued port {port} is not claimed"
            );
        }
    }

    /// A port another process holds is skipped though nothing is bound
    /// on it: the stopped server whose clients still dial its address.
    #[test]
    fn skips_ports_another_process_claims() {
        const BASE: u16 = 5_000;
        /// How many free ports to hold past the one issued: one is
        /// enough to prove the skip, a few keep it provable while a
        /// test sharing the base takes ports from the same run.
        const HOLD: usize = 3;
        let _shared = shared_base();
        let block_base = BASE + (std::process::id() % u32::from(NBLOCKS)) as u16 * BLOCK;
        let first = free_addr(BASE);
        // Hold the next free ports in probe order, as another process
        // would: a second open of a claim file conflicts like another
        // process's. Ports skipped here (issued, held or bound) are
        // skipped by `free_addr` too, so with claims ignored it would
        // issue the first port held. A Vec: a few entries, only scanned.
        let mut held: Vec<(u16, File)> = Vec::new();
        for offset in 1..BLOCK {
            if held.len() == HOLD {
                break;
            }
            let port = block_base + (first.port() - block_base + offset) % BLOCK;
            let Some(claimed) = claim(claims_dir(), port) else {
                continue;
            };
            // A bound port would be skipped by the bind probe, which
            // proves nothing about claims.
            if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
                held.push((port, claimed));
            }
        }
        assert!(
            !held.is_empty(),
            "no free port left in block {block_base}..{} to hold",
            block_base + BLOCK
        );
        let second = free_addr(BASE).port();
        assert!(
            !held.iter().any(|(port, _)| *port == second),
            "issued port {second}, which another process claims"
        );
    }

    /// A claim excludes every other holder until it is dropped, and the
    /// port is free to claim again after.
    #[test]
    fn a_claim_is_exclusive_until_dropped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let held = claim(dir.path(), 4242).expect("a fresh port is claimable");
        assert!(claim(dir.path(), 4242).is_none(), "claimed twice");
        assert!(
            claim(dir.path(), 4243).is_some(),
            "another port's claim is its own"
        );
        drop(held);
        assert!(
            claim(dir.path(), 4242).is_some(),
            "a dropped claim is released"
        );
    }

    #[test]
    fn skips_externally_bound_ports() {
        const BASE: u16 = 5_000;
        let _shared = shared_base();
        let block_base = BASE + (std::process::id() % u32::from(NBLOCKS)) as u16 * BLOCK;
        let first = free_addr(BASE);
        // Squat the next port in probe order, so the following call must
        // hit the bind-probe filter (not just the issued-set skip).
        let next = block_base + (first.port() - block_base + 1) % BLOCK;
        // A foreign service already on `next` proves the same filtering
        // free_addr relies on, but leaves nothing for us to squat — the
        // scenario under test cannot be set up, so end quietly.
        let Ok(squatted) = std::net::TcpListener::bind(("127.0.0.1", next)) else {
            return;
        };
        // Likewise when another test process holds `next`: it is skipped
        // as claimed, and its release below could not be observed.
        if claim(claims_dir(), next).is_none() {
            return;
        }
        let second = free_addr(BASE);
        assert_ne!(second.port(), next, "issued an externally bound port");
        // The probe found `next` bound, so its claim was let go again:
        // another process may still take it once the squatter leaves.
        assert!(
            claim(claims_dir(), next).is_some(),
            "port {next}, rejected by the bind probe, stayed claimed"
        );
        drop(squatted);
    }

    /// A claim refreshes its file's modified time, so a cleaner that
    /// removes old files from the claims directory spares a file a test
    /// holds, even one first created long ago.
    #[test]
    fn a_claim_refreshes_its_files_age() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("4242.claim");
        let old = SystemTime::now() - std::time::Duration::from_secs(30 * 24 * 3600);
        File::create(&path)
            .expect("create the claim file")
            .set_modified(old)
            .expect("age the claim file");
        let before_claim = SystemTime::now();
        let _held = claim(dir.path(), 4242).expect("claimable");
        let modified = std::fs::metadata(&path)
            .expect("stat the claim file")
            .modified()
            .expect("modified time");
        assert!(
            modified >= before_claim,
            "claim left the file's modified time at {modified:?}"
        );
    }
}
