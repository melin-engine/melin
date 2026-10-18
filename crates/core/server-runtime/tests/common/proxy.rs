//! A replication link a test can break the way a crash or a partition
//! does, for the tests that need one (`degraded_acks.rs`).
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy. It carries no dead-code allowance: an item
//! one includer leaves unused warns there, and is moved or dropped rather
//! than hidden.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

/// A TCP relay between a replica and its primary, bound where the nodes
/// reach a test's own sockets (`melin_test_node::local_ip`), so the same
/// test runs on both transports. The replica takes [`Proxy::addr`] as its
/// `replica_of`.
///
/// - [`freeze`](Proxy::freeze): nothing is relayed either way, as on a
///   link gone silent: the primary's entries stop reaching the replica
///   and its acks stop coming back, so the writes in flight stay
///   unconfirmed.
/// - [`cut`](Proxy::cut): every relayed connection is closed, and new
///   ones are refused, so the primary loses the replica for good and the
///   replica's reconnects fail.
/// - [`restore`](Proxy::restore): new connections are relayed again; the
///   replica gets back in on its next reconnect.
pub struct Proxy {
    addr: SocketAddr,
    /// `FORWARDING`, `FROZEN` or `CUT`. A `u8` in an atomic: three states,
    /// read by every relay thread on every pass.
    mode: Arc<AtomicU8>,
    /// Bumped by every cut: a relay serves only the generation it was
    /// opened in. `u64`, so it never wraps.
    generation: Arc<AtomicU64>,
}

const FORWARDING: u8 = 0;
const FROZEN: u8 = 1;
const CUT: u8 = 2;

impl Proxy {
    /// Relay connections to `upstream`, on a thread that runs as long as
    /// the test process.
    pub fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind((melin_test_node::local_ip(), 0)).expect("bind the proxy");
        let addr = listener.local_addr().expect("the proxy's address");
        let mode = Arc::new(AtomicU8::new(FORWARDING));
        let generation = Arc::new(AtomicU64::new(0));
        let (accept_mode, accept_generation) = (Arc::clone(&mode), Arc::clone(&generation));
        std::thread::spawn(move || {
            for downstream in listener.incoming() {
                let Ok(downstream) = downstream else {
                    continue;
                };
                if accept_mode.load(Ordering::Acquire) == CUT {
                    // Refused: dropped unanswered, as a dead host's port.
                    continue;
                }
                let Ok(upstream) = TcpStream::connect(upstream) else {
                    continue;
                };
                let born = accept_generation.load(Ordering::Acquire);
                for (from, to) in [
                    (downstream.try_clone(), upstream.try_clone()),
                    (upstream.try_clone(), downstream.try_clone()),
                ] {
                    let (Ok(from), Ok(to)) = (from, to) else {
                        continue;
                    };
                    let (mode, generation) =
                        (Arc::clone(&accept_mode), Arc::clone(&accept_generation));
                    std::thread::spawn(move || relay(from, to, &mode, &generation, born));
                }
            }
        });
        Self {
            addr,
            mode,
            generation,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn freeze(&self) {
        self.mode.store(FROZEN, Ordering::Release);
    }

    pub fn cut(&self) {
        self.mode.store(CUT, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
    }

    pub fn restore(&self) {
        self.mode.store(FORWARDING, Ordering::Release);
    }
}

/// Copy `from` to `to` until either closes or the proxy cuts the
/// generation the relay was opened in; hold the bytes while frozen.
fn relay(
    mut from: TcpStream,
    mut to: TcpStream,
    mode: &AtomicU8,
    generation: &AtomicU64,
    born: u64,
) {
    // A short timeout, so a cut is noticed on an idle link.
    if from
        .set_read_timeout(Some(Duration::from_millis(10)))
        .is_err()
    {
        return;
    }
    let mut buf = [0u8; 64 * 1024];
    loop {
        if generation.load(Ordering::Acquire) != born {
            // Both directions of the pair go: a cut link carries nothing.
            // Best-effort: either end may already be gone.
            let _ = from.shutdown(std::net::Shutdown::Both);
            let _ = to.shutdown(std::net::Shutdown::Both);
            return;
        }
        if mode.load(Ordering::Acquire) == FROZEN {
            std::thread::sleep(Duration::from_millis(5));
            continue;
        }
        match from.read(&mut buf) {
            Ok(0) => {
                // Best-effort, as above.
                let _ = to.shutdown(std::net::Shutdown::Write);
                return;
            }
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(_) => return,
        }
    }
}
