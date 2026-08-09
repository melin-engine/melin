//! Test-only allocation of localhost listen addresses that a server will
//! re-bind later — see [`free_addr`]. Compiled only under the
//! `test-utils` feature; activate from a consuming crate's
//! `[dev-dependencies]`.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};

/// Ports per pid-derived block. The hungriest test binary allocates ~9
/// addresses per process; 50 leaves generous slack even under plain
/// `cargo test`, where every test in a binary shares one process (and
/// this allocator never reissues a port within a process).
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
/// - The bind probe itself filters anything else alive on a port.
///
/// Panics when the block is exhausted: with [`BLOCK`] ports per block
/// and about a dozen listeners per test process, that means leaked
/// listeners or a foreign process squatting the range.
pub fn free_addr(base: u16) -> SocketAddr {
    // Every port this process has handed out. HashSet over a bitmap or
    // sorted Vec: a handful of scattered u16s where `contains` is the
    // only query — no ordering or density to exploit. Mutex over an
    // atomic scheme: allocation runs a few times per test, so contention
    // is irrelevant, and holding the lock across the whole probe loop
    // serializes concurrent callers so they cannot race each other to
    // the same free port or observe a partially-updated set.
    static ISSUED: OnceLock<Mutex<HashSet<u16>>> = OnceLock::new();
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
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .expect("free_addr lock poisoned");
    for i in 0..BLOCK {
        let port = block_base + (rotation + i) % BLOCK;
        if issued.contains(&port) {
            continue;
        }
        if let Ok(listener) = std::net::TcpListener::bind(("127.0.0.1", port)) {
            issued.insert(port);
            return listener
                .local_addr()
                .expect("local_addr on a bound listener");
        }
    }
    panic!("no free port in block {block_base}..{}", block_base + BLOCK);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Each unit test gets its own base (disjoint block spans), outside
    // every range the integration tests own (election.rs: 15000,
    // raft_failover.rs: 20000, raft_smoke.rs: 25000). The issued set is
    // process-global, but disjoint spans keep the tests independent.

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
        }
    }

    #[test]
    fn skips_externally_bound_ports() {
        const BASE: u16 = 5_000;
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
        let second = free_addr(BASE);
        assert_ne!(second.port(), next, "issued an externally bound port");
        drop(squatted);
    }
}
