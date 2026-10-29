//! A link a test can break the way a crash or a partition does, for the
//! tests that need one: the replication link (`degraded_acks.rs`,
//! `raft_failover.rs`) and the raft links between nodes, which
//! `raft_failover.rs` partitions pair by pair through the source
//! predicate of [`Proxy::start_on`].
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy. It carries no dead-code allowance: an item
//! one includer leaves unused warns there, and is moved or dropped rather
//! than hidden.

use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A TCP relay in front of a node's listener. [`Proxy::start`] binds it
/// where the nodes reach a test's own sockets (`melin_test_node::local_ip`),
/// so the same test runs on both transports; a replica takes
/// [`Proxy::addr`] as its `replica_of`.
///
/// - [`freeze`](Proxy::freeze): nothing is relayed either way, as on a
///   link gone silent: the primary's entries stop reaching the replica
///   and its acks stop coming back, so the writes in flight stay
///   unconfirmed. A connection accepted while frozen is held, unread.
/// - [`cut`](Proxy::cut): every relayed or held connection is closed, and
///   new ones are refused, so the primary loses the replica for good and
///   the replica's reconnects fail.
/// - [`restore`](Proxy::restore): new connections are relayed again; the
///   replica gets back in on its next reconnect.
///
/// A connection whose source the proxy's `stalled` predicate names is
/// treated as if the proxy were frozen, for that connection alone: see
/// [`Proxy::start_on`].
pub struct Proxy {
    addr: SocketAddr,
    link: Arc<Link>,
}

/// What the accept thread and every relay thread of one proxy share.
struct Link {
    /// `FORWARDING`, `FROZEN` or `CUT`. A `u8` in an atomic: three states,
    /// read by every relay thread on every pass.
    mode: AtomicU8,
    /// Bumped by every cut: a relay serves only the generation it was
    /// opened in. `u64`, so it never wraps.
    generation: AtomicU64,
    /// Whether connections from a source IP are stalled.
    stalled: Box<dyn Fn(IpAddr) -> bool + Send + Sync>,
    /// Connections accepted while stalled, held unread and without relay
    /// threads (so a peer redialling through a long partition costs a
    /// socket per attempt, not two threads) until the next cut closes
    /// them. A `Vec` under a `Mutex`: pushed once per accepted attempt and
    /// drained by `cut`, never iterated on a relay path.
    held: Mutex<Vec<TcpStream>>,
}

const FORWARDING: u8 = 0;
const FROZEN: u8 = 1;
const CUT: u8 = 2;

impl Link {
    /// Whether a connection from `source` is to carry nothing for now.
    fn stalls(&self, source: IpAddr) -> bool {
        self.mode.load(Ordering::Acquire) == FROZEN || (self.stalled)(source)
    }
}

impl Proxy {
    /// Relay connections to `upstream`, listening on
    /// `melin_test_node::local_ip`, on a thread that runs as long as the
    /// test process.
    pub fn start(upstream: SocketAddr) -> Self {
        Self::start_on(melin_test_node::local_ip(), upstream, |_| false)
    }

    /// Relay connections to `upstream`, listening on `ip`, on a thread
    /// that runs as long as the test process.
    ///
    /// `stalled` is asked about each connection's source IP, on accept and
    /// on every relay pass: while it holds, that connection carries
    /// nothing either way and is not closed (no RST), as on a link gone
    /// silent; one accepted while it holds is held unread until the next
    /// cut. A partition between two nodes is so a stall of each one's
    /// connections at the other's proxy, with the other links untouched.
    pub fn start_on(
        ip: IpAddr,
        upstream: SocketAddr,
        stalled: impl Fn(IpAddr) -> bool + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind((ip, 0)).expect("bind the proxy");
        let addr = listener.local_addr().expect("the proxy's address");
        let link = Arc::new(Link {
            mode: AtomicU8::new(FORWARDING),
            generation: AtomicU64::new(0),
            stalled: Box::new(stalled),
            held: Mutex::new(Vec::new()),
        });
        let accept_link = Arc::clone(&link);
        std::thread::spawn(move || {
            let link = accept_link;
            for downstream in listener.incoming() {
                let Ok(downstream) = downstream else {
                    continue;
                };
                // Taken first, so a cut from here on closes whatever this
                // connection becomes.
                let born = link.generation.load(Ordering::Acquire);
                if link.mode.load(Ordering::Acquire) == CUT {
                    // Refused: dropped unanswered, as a dead host's port.
                    continue;
                }
                let Ok(source) = downstream.peer_addr().map(|a| a.ip()) else {
                    continue;
                };
                if link.stalls(source) {
                    let mut held = link.held.lock().unwrap();
                    // A cut that has drained the list since `born` would
                    // never see this one: closed now instead, as the cut
                    // would have.
                    if link.generation.load(Ordering::Acquire) == born {
                        held.push(downstream);
                    }
                    continue;
                }
                let Ok(upstream) = TcpStream::connect(upstream) else {
                    continue;
                };
                for (from, to) in [
                    (downstream.try_clone(), upstream.try_clone()),
                    (upstream.try_clone(), downstream.try_clone()),
                ] {
                    let (Ok(from), Ok(to)) = (from, to) else {
                        continue;
                    };
                    let link = Arc::clone(&link);
                    std::thread::spawn(move || relay(from, to, &link, born, source));
                }
            }
        });
        Self { addr, link }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn freeze(&self) {
        self.link.mode.store(FROZEN, Ordering::Release);
    }

    pub fn cut(&self) {
        self.link.mode.store(CUT, Ordering::Release);
        self.link.generation.fetch_add(1, Ordering::AcqRel);
        // Dropping them closes them.
        self.link.held.lock().unwrap().clear();
    }

    /// Relay new connections again. Connections held since the last cut
    /// stay held: cut before restoring to close them.
    pub fn restore(&self) {
        self.link.mode.store(FORWARDING, Ordering::Release);
    }
}

/// Copy `from` to `to` until either closes or the proxy cuts the
/// generation the relay was opened in; hold the bytes while the
/// connection from `source` is stalled.
fn relay(mut from: TcpStream, mut to: TcpStream, link: &Link, born: u64, source: IpAddr) {
    // A short timeout, so a cut is noticed on an idle link.
    if from
        .set_read_timeout(Some(Duration::from_millis(10)))
        .is_err()
    {
        return;
    }
    let mut buf = [0u8; 64 * 1024];
    loop {
        if link.generation.load(Ordering::Acquire) != born {
            // Both directions of the pair go: a cut link carries nothing.
            // Best-effort: either end may already be gone.
            let _ = from.shutdown(std::net::Shutdown::Both);
            let _ = to.shutdown(std::net::Shutdown::Both);
            return;
        }
        if link.stalls(source) {
            std::thread::sleep(Duration::from_millis(5));
            continue;
        }
        match from.read(&mut buf) {
            Ok(0) => {
                // Best-effort, as above.
                let _ = to.shutdown(std::net::Shutdown::Write);
                return;
            }
            // Cut since the check above: the chunk may be what the kernel
            // buffered while the link was stalled, which a cut link never
            // delivers. Dropped, and the pair closed at the top.
            Ok(_) if link.generation.load(Ordering::Acquire) != born => {}
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
