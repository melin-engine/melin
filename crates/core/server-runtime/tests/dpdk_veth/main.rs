//! The DPDK transport, end to end, on a veth pair instead of a NIC.
//!
//! Each test runs a counter node on DPDK through the `net_af_packet` PMD,
//! with no hugepages, no bound NIC and no root, and drives it from a
//! kernel-TCP client on the other end of the pair. That checks the
//! transport's logic — the poll loop, the auth state machine, what a close
//! does on the wire — and nothing about its speed: af_packet is not a NIC.
//! See `docs/internal/dpdk-veth-testing.md`.
//!
//! # Why each test re-executes itself
//!
//! EAL initialises once per process, and the network a test builds must
//! not be the host's. So each test re-runs its own binary under
//! `unshare -rnm` (new user, network and mount namespaces), filtered to
//! itself and marked by an environment variable. The child builds the
//! veth pair, runs the node in-process and does the real work; the parent
//! only asserts that the child passed. `unshare(2)` itself is no option in
//! the parent: a user namespace needs a single-threaded caller, and the
//! test runner is not one.
//!
//! The host must allow unprivileged user namespaces. Ubuntu 24.04 restricts
//! them by default; a test fails, saying so, rather than skipping.
//!
//! Built only with the `dpdk` feature (`required-features` in the
//! manifest), so a plain `cargo test` never needs libdpdk. Run with:
//!
//! ```sh
//! cargo nextest run -p melin-server-runtime --features dpdk --test dpdk_veth
//! ```

mod netns;

use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use counter_server::{Counter, KIND_RESP_ACK, RequestDecoder, ResponseEncoder, increment_request};
use melin_client::{Connection, Handshake, SigningKey, Step, key};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{self, ServerConfig};
use melin_wire_protocol::control_codec::{TAG_AUTH_FAILED, TAG_CHALLENGE};

// ---------------------------------------------------------------------------
// The network
// ---------------------------------------------------------------------------

/// The node's address, owned by DPDK's userspace stack on `veth0`.
const NODE_IP: Ipv4Addr = Ipv4Addr::new(10, 99, 0, 2);
/// The client's address, on the kernel's `veth1`.
const CLIENT_IP: Ipv4Addr = Ipv4Addr::new(10, 99, 0, 1);
const PREFIX_LEN: u8 = 24;
const NODE_PORT: u16 = 9876;

fn node_addr() -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(NODE_IP, NODE_PORT))
}

// ---------------------------------------------------------------------------
// Time limits
//
// Generous, because a CI runner is slow and shared, and bounded, so a hang
// fails here rather than being killed by nextest's slow-timeout (two
// 60-second periods, `.config/nextest.toml`) with nothing said about where.
// ---------------------------------------------------------------------------

/// How long the parent waits for its child, the whole test.
const CHILD_LIMIT: Duration = Duration::from_secs(100);
/// How long the node may take to come up: EAL init, journal creation.
const STARTUP_LIMIT: Duration = Duration::from_secs(45);
/// How long a clean shutdown may take.
const SHUTDOWN_LIMIT: Duration = Duration::from_secs(20);
/// How long to wait for any single frame the node owes the client.
const FRAME_LIMIT: Duration = Duration::from_secs(10);
/// How long a client waits to be sure nothing more is coming.
const SILENCE: Duration = Duration::from_secs(2);
/// How soon a refused connection's slot must be free again.
///
/// Under the node's 5-second auth timeout on purpose: that timeout also
/// frees a slot that was never closed, so a check slower than it would
/// pass on a node that forgot to close the connection at all.
const SLOT_FREED_WITHIN: Duration = Duration::from_secs(3);
/// The node's heartbeat interval: an idle authenticated connection gets a
/// heartbeat this long after its last request. Set explicitly because a
/// test relies on it, not just on the default.
const HEARTBEAT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// Declare a test that runs `body` in a fresh namespace, against a fresh
/// node. The macro keeps the name the child is filtered to and the
/// function's own name one and the same.
macro_rules! veth_test {
    ($(#[doc = $doc:literal])* $name:ident, $body:expr) => {
        $(#[doc = $doc])*
        #[test]
        fn $name() {
            in_namespace(stringify!($name), $body);
        }
    };
}

veth_test!(
    /// A key the node does not know gets `AuthFailed`, and the node then
    /// closes the connection and frees its slot. With `max_connections`
    /// at one, an authorised client can connect only once that slot is
    /// back — while the refused client still holds its end open, so the
    /// close cannot be one the client started.
    an_unknown_key_is_refused_and_its_slot_freed,
    || {
        let (mut refused, challenge) = connect_for_challenge();
        refused
            .write_all(&answer(&challenge, &unknown_key()))
            .expect("send the challenge response");
        assert_eq!(
            read_frame(&mut refused).expect("the node answers the attempt"),
            [TAG_AUTH_FAILED],
            "an unknown key is refused"
        );

        assert_slot_freed("after a refused key");
        drop(refused);
    }
);

veth_test!(
    /// One attempt per connection. A second ChallengeResponse after a
    /// failed one is not read, even one that would pass: no second
    /// verdict comes back, and the connection is closed all the same.
    ///
    /// Two shapes of the retry: pipelined behind the failed attempt, so
    /// both are in the node's buffer when it judges the first; and sent
    /// after the client has the `AuthFailed`.
    a_second_attempt_after_a_failure_is_not_answered,
    || {
        // Pipelined: both answers go in one write.
        let (mut client, challenge) = connect_for_challenge();
        let mut both = answer(&challenge, &unknown_key());
        both.extend_from_slice(&answer(&challenge, &writer_key()));
        client.write_all(&both).expect("send both attempts");
        assert_eq!(
            read_frame(&mut client).expect("the node answers the first attempt"),
            [TAG_AUTH_FAILED],
            "the first attempt is refused"
        );
        assert_nothing_more(&mut client, "a pipelined second attempt");
        assert_slot_freed("after a pipelined second attempt");
        drop(client);

        // Sent after the verdict.
        let (mut client, challenge) = connect_for_challenge();
        client
            .write_all(&answer(&challenge, &unknown_key()))
            .expect("send the first attempt");
        assert_eq!(
            read_frame(&mut client).expect("the node answers the first attempt"),
            [TAG_AUTH_FAILED],
            "the first attempt is refused"
        );
        // Ignored if the node has already closed: that refusal is what
        // the check below is about.
        let _ = client.write_all(&answer(&challenge, &writer_key()));
        assert_nothing_more(&mut client, "a second attempt after the verdict");
        assert_slot_freed("after a second attempt following the verdict");
        drop(client);
    }
);

veth_test!(
    /// When the node closes a client's connection the client is not told:
    /// no FIN, no RST. It learns only when it next sends, from the RST
    /// that answers a segment for a connection the node no longer has.
    ///
    /// This pins a known divergence from the kernel-TCP transport, whose
    /// close the peer sees as EOF ("DPDK closes a client connection
    /// without telling the peer", `docs/internal/transport-divergences-2026-10.md`).
    /// When that is fixed this test fails, and is to be flipped to require
    /// the EOF.
    a_server_side_close_is_silent,
    || {
        let (mut client, challenge) = connect_for_challenge();
        client
            .write_all(&answer(&challenge, &unknown_key()))
            .expect("send the challenge response");
        assert_eq!(
            read_frame(&mut client).expect("the node answers the attempt"),
            [TAG_AUTH_FAILED],
            "an unknown key is refused"
        );
        // The slot is released only together with the socket, so once it
        // is free the node has closed its side.
        assert_slot_freed("after a refused key");

        client
            .set_read_timeout(Some(SILENCE))
            .expect("set read timeout");
        let mut buf = [0u8; 64];
        match client.read(&mut buf) {
            Err(e) if is_timeout(&e) => {}
            Ok(0) => panic!(
                "the client saw EOF after the node closed its connection: DPDK no longer \
                 closes silently. Flip this test to require the EOF and close the entry in \
                 docs/internal/transport-divergences-2026-10.md"
            ),
            Ok(n) => panic!("{n} unexpected bytes after AuthFailed: {:02x?}", &buf[..n]),
            Err(e) => panic!("expected silence after the node's close, the read failed: {e}"),
        }

        // Sending is what surfaces the close.
        client
            .write_all(&[0u8; 4])
            .expect("the first write after the close is buffered locally");
        client
            .set_read_timeout(Some(FRAME_LIMIT))
            .expect("set read timeout");
        match client.read(&mut buf) {
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => {}
            other => panic!(
                "a segment for a closed connection should be answered with RST, the client \
                 read {other:?}"
            ),
        }
    }
);

veth_test!(
    /// When an authorised client closes its connection, the node does not
    /// see the FIN. The connection, and its slot, are released only when
    /// the node's next heartbeat is answered with an RST.
    ///
    /// This pins a known divergence from the kernel-TCP transport, which
    /// releases the connection on EOF ("DPDK does not see a client's
    /// close", `docs/internal/transport-divergences-2026-10.md`). When that
    /// is fixed this test fails, and is to be flipped to require the slot
    /// back within [`SLOT_FREED_WITHIN`].
    a_client_close_is_seen_only_at_the_next_heartbeat,
    || {
        let conn = served_within(STARTUP_LIMIT, "the first client");
        // Closes the socket: the client's FIN goes out now.
        drop(conn);

        if challenge_within(SLOT_FREED_WITHIN).is_ok() {
            panic!(
                "the node freed a closed client's slot within {SLOT_FREED_WITHIN:?}, before \
                 its heartbeat: DPDK now sees a client's FIN. Flip this test to require the \
                 slot back promptly and close the entry in \
                 docs/internal/transport-divergences-2026-10.md"
            );
        }

        // The heartbeat goes out within a second of HEARTBEAT after the
        // last reply; the margin covers that and the RST's way back.
        served_within(HEARTBEAT + FRAME_LIMIT, "after the node's heartbeat");
    }
);

// ---------------------------------------------------------------------------
// Clients
// ---------------------------------------------------------------------------

/// Listed in the node's `authorized_keys` as a writer.
fn writer_key() -> SigningKey {
    SigningKey::from_bytes(&[0xA1; 32])
}

/// Listed nowhere.
fn unknown_key() -> SigningKey {
    SigningKey::from_bytes(&[0xB2; 32])
}

/// Connect a bare socket and read the node's challenge, retrying until
/// the node is serving. Returns the socket and the challenge's payload.
///
/// The retries are load-bearing, and not only at startup, when nothing
/// answers ARP until the port is up. A connection the node turns away at
/// `max_connections` is accepted and then never challenged, and the one
/// slot can stay taken for up to [`HEARTBEAT`] after an authorised client
/// has closed: the node does not see a client's FIN, only the RST that
/// answers its next heartbeat (pinned by
/// `a_client_close_is_seen_only_at_the_next_heartbeat`). Every test that
/// has called [`assert_slot_freed`] leaves such a connection behind.
fn connect_for_challenge() -> (TcpStream, Vec<u8>) {
    challenge_within(STARTUP_LIMIT).unwrap_or_else(|e| {
        panic!(
            "no challenge from {} within {STARTUP_LIMIT:?}: {e}",
            node_addr()
        )
    })
}

/// [`connect_for_challenge`], giving up after `limit` with the last
/// attempt's error.
fn challenge_within(limit: Duration) -> io::Result<(TcpStream, Vec<u8>)> {
    let deadline = Instant::now() + limit;
    loop {
        let attempt = TcpStream::connect_timeout(&node_addr(), Duration::from_millis(500))
            .and_then(|mut stream| {
                stream.set_read_timeout(Some(Duration::from_secs(1)))?;
                stream.set_nodelay(true)?;
                let frame = read_frame(&mut stream)?;
                Ok((stream, frame))
            });
        match attempt {
            Ok((stream, frame)) if frame.first() == Some(&TAG_CHALLENGE) => {
                stream
                    .set_read_timeout(Some(FRAME_LIMIT))
                    .expect("set read timeout");
                return Ok((stream, frame));
            }
            Ok((_, frame)) => panic!("expected a challenge, got {frame:02x?}"),
            Err(e) if Instant::now() >= deadline => return Err(e),
            Err(_) => std::thread::sleep(Duration::from_millis(100)),
        }
    }
}

/// The ChallengeResponse `key` sends for `challenge`, length prefix and
/// all, as `melin-client` builds it.
fn answer(challenge: &[u8], key: &SigningKey) -> Vec<u8> {
    match Handshake::new(key).feed(challenge) {
        Ok(Step::Send(frame)) => frame.to_vec(),
        other => panic!("the handshake did not answer the challenge: {other:?}"),
    }
}

/// One length-prefixed frame. Handshake frames are tiny; anything larger
/// is a framing error, refused before reading it.
fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix)?;
    let len = u32::from_le_bytes(prefix) as usize;
    if len > 256 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a {len}-byte frame during the handshake"),
        ));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

fn is_timeout(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

/// Nothing more arrives on `client` for [`SILENCE`]. A close, silent or
/// not, is fine; bytes are not — they would be a second verdict.
fn assert_nothing_more(client: &mut TcpStream, after: &str) {
    client
        .set_read_timeout(Some(SILENCE))
        .expect("set read timeout");
    let mut buf = [0u8; 64];
    match client.read(&mut buf) {
        Ok(0) => {}
        Err(e) if is_timeout(&e) || e.kind() == io::ErrorKind::ConnectionReset => {}
        Ok(n) => panic!(
            "{after}: the node answered again with {n} bytes: {:02x?}",
            &buf[..n]
        ),
        Err(e) => panic!("{after}: unexpected read error {e}"),
    }
}

/// An authorised client connects, and is served, within
/// [`SLOT_FREED_WITHIN`]. The node runs with `max_connections` at one, so
/// this proves the previous connection's slot was released.
///
/// The client then closes, and its slot stays taken until the node's next
/// heartbeat (see [`connect_for_challenge`]).
fn assert_slot_freed(after: &str) {
    drop(served_within(SLOT_FREED_WITHIN, after));
}

/// An authorised client that connected, and was served, within `limit`.
fn served_within(limit: Duration, after: &str) -> Connection {
    let deadline = Instant::now() + limit;
    let mut conn =
        Connection::connect_by(node_addr(), &writer_key(), deadline).unwrap_or_else(|e| {
            panic!("{after}: the node's only connection slot was not free within {limit:?}: {e}")
        });
    let reply = conn
        .request_one(&increment_request(1))
        .unwrap_or_else(|e| panic!("{after}: the authorised client was not served: {e}"));
    assert_eq!(
        reply.first(),
        Some(&KIND_RESP_ACK),
        "{after}: increment acked"
    );
    conn
}

// ---------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------

/// A counter node on DPDK, running in this (child) process.
///
/// No `Drop`: a test that panics fails the child, whose exit takes the
/// node, its namespaces and its tmpfs with it, so there is nothing to
/// stop on that path.
struct Node {
    thread: JoinHandle<Result<(), String>>,
    /// Journal and `authorized_keys`; held so they outlive the node.
    _dir: tempfile::TempDir,
}

impl Node {
    fn start() -> Node {
        let dir = tempfile::tempdir_in(temp_parent()).expect("tempdir");
        let authorized_keys = dir.path().join("authorized_keys");
        std::fs::write(
            &authorized_keys,
            format!(
                "{}\n",
                key::authorized_keys_line("writer", &writer_key().verifying_key(), "veth-test")
            ),
        )
        .expect("write authorized_keys");

        let config = ServerConfig {
            bind: node_addr(),
            journal: dir.path().join("counter.journal"),
            authorized_keys,
            standalone: true,
            ack_policy: AckPolicy::Disk,
            no_mlock: true,
            // Unpinned: the node shares the host with the test's client,
            // and a CI runner with everything else.
            cores: PipelineCores::unpinned(),
            snapshot_interval_ms: 0,
            health_bind: None,
            // One slot, so a test can tell whether a closed connection's
            // slot came back: the next client gets in only if it did.
            max_connections: 1,
            heartbeat_interval_secs: HEARTBEAT.as_secs(),
            dpdk_eal_args: eal_args(),
            dpdk_ip: NODE_IP.to_string(),
            dpdk_prefix_len: PREFIX_LEN,
            ..ServerConfig::default()
        };

        let thread = std::thread::Builder::new()
            .name("node".into())
            .spawn(move || {
                server::run::<Counter>(
                    config,
                    StartupEvents::none(),
                    (),
                    RequestDecoder,
                    ResponseEncoder,
                    None,
                )
                .map_err(|e| e.to_string())
            })
            .expect("spawn the node thread");
        Node { thread, _dir: dir }
    }

    /// Shut the node down the way an operator does, with SIGTERM: `run`
    /// owns its shutdown flag and sets it from its signal handler.
    fn stop(self) {
        let Node { thread, _dir } = self;
        assert!(
            !thread.is_finished(),
            "the node stopped before the test asked it to: {:?}",
            thread.join()
        );
        // SAFETY: kill(2) on our own pid; `run` has installed its handler,
        // since the node has been serving.
        let rc = unsafe { libc::kill(libc::getpid(), libc::SIGTERM) };
        assert_eq!(rc, 0, "SIGTERM: {}", io::Error::last_os_error());
        let deadline = Instant::now() + SHUTDOWN_LIMIT;
        while !thread.is_finished() {
            assert!(
                Instant::now() < deadline,
                "the node did not shut down within {SHUTDOWN_LIMIT:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        thread
            .join()
            .expect("node thread panicked")
            .expect("node returned an error");
    }
}

/// EAL arguments for af_packet on `veth0`, in ordinary memory.
///
/// `--in-memory` is not among them: EAL refuses it with `--no-huge`, which
/// implies legacy memory, and the private `/var/run` makes it unnecessary.
/// The main lcore is the first CPU this process may run on, rather than
/// CPU 0, which a restricted runner need not allow.
fn eal_args() -> String {
    format!(
        "--no-huge -m 512 --no-pci --vdev=net_af_packet0,iface={} -l {}",
        netns::DPDK_IFACE,
        first_allowed_cpu()
    )
}

fn first_allowed_cpu() -> usize {
    // SAFETY: `cpu_set_t` is plain old data; all-zero is the empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `set` is a live, writable cpu_set_t of the size passed.
    let rc = unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) };
    assert_eq!(rc, 0, "sched_getaffinity: {}", io::Error::last_os_error());
    (0..libc::CPU_SETSIZE as usize)
        // SAFETY: `c` is below CPU_SETSIZE, inside the set.
        .find(|&c| unsafe { libc::CPU_ISSET(c, &set) })
        .expect("this process may run on at least one CPU")
}

// ---------------------------------------------------------------------------
// The re-exec harness
// ---------------------------------------------------------------------------

/// Set in the child; its value is the file the child creates once the
/// test body has passed.
const CHILD_ENV: &str = "MELIN_DPDK_VETH_CHILD";

/// Serialises the tests under a plain `cargo test`, which runs them on
/// parallel threads. Each would work alongside the others — every child
/// has its own namespaces — but each node busy-polls a core, and the plan
/// is for these to run one at a time. Under nextest the `dpdk-serial`
/// test group does the same across processes.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// Run `body` against a fresh node in fresh namespaces: in the child,
/// do it; in the parent, re-execute this test under `unshare -rnm` and
/// assert that it passed.
fn in_namespace(name: &str, body: fn()) {
    match std::env::var_os(CHILD_ENV) {
        Some(done) => run_child(body, Path::new(&done)),
        None => run_parent(name),
    }
}

fn run_child(body: fn(), done: &Path) {
    // Deliberately ignored: only one test runs in the child, and a
    // subscriber already installed would be just as good.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "info,melin_server_runtime::dpdk_transport=debug",
                )
            }),
        )
        .with_writer(io::stderr)
        .with_thread_names(true)
        .try_init();

    if let Err(e) = netns::build(CLIENT_IP, PREFIX_LEN) {
        panic!("{e}\n{}", namespace_hint());
    }
    if let Err(e) = netns::private_var_run() {
        panic!("{e}\n{}", namespace_hint());
    }

    let node = Node::start();
    body();
    node.stop();

    std::fs::write(done, b"passed").expect("record that the test body passed");
}

fn run_parent(name: &str) {
    // A poisoned lock only means an earlier test failed; that one has
    // reported itself, and this one has nothing to inherit from it.
    let _serial = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    preflight();

    let marker_dir = tempfile::tempdir_in(temp_parent()).expect("tempdir");
    let done = marker_dir.path().join("done");
    let exe = std::env::current_exe().expect("this test binary's path");
    let mut child = Command::new("unshare")
        .arg("-rnm")
        .arg("--")
        .arg(&exe)
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, &done)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the test under unshare");

    // Drained on threads so a chatty child cannot fill a pipe and stall,
    // and replayed through print!/eprint! so the test runner shows them
    // with this test's output, only on failure.
    let stdout = drain(child.stdout.take().expect("piped stdout"));
    let stderr = drain(child.stderr.take().expect("piped stderr"));

    let deadline = Instant::now() + CHILD_LIMIT;
    let status = loop {
        match child.try_wait().expect("wait for the child") {
            Some(status) => break Some(status),
            None if Instant::now() >= deadline => {
                // Best effort: the child may exit between the check and
                // the kill, and either way it is reaped just below.
                let _ = child.kill();
                child.wait().expect("reap the killed child");
                break None;
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    print!(
        "{}",
        String::from_utf8_lossy(&stdout.join().expect("stdout reader"))
    );
    eprint!(
        "{}",
        String::from_utf8_lossy(&stderr.join().expect("stderr reader"))
    );

    let status = status.unwrap_or_else(|| {
        panic!("{name} did not finish within {CHILD_LIMIT:?} in its namespace; killed")
    });
    assert!(
        status.success(),
        "{name} failed in its namespace ({status}); its output is above"
    );
    assert!(
        done.exists(),
        "the child exited cleanly but never ran {name}: the test filter matched nothing"
    );
}

/// Where the done marker and the node's directory go: the temp directory,
/// unless it lies under `/run` (`TMPDIR=/run/user/$UID`, say), which the
/// child's private tmpfs on `/var/run` hides — the parent's marker from
/// the child, and the directory itself once the child is inside. Then
/// `/tmp`.
fn temp_parent() -> std::path::PathBuf {
    let tmp = std::env::temp_dir();
    // An unresolvable temp dir is left as is: creating the marker
    // directory in it fails just after, with the error that says why.
    let resolved = std::fs::canonicalize(&tmp).unwrap_or_else(|_| tmp.clone());
    if resolved.starts_with("/run") || resolved.starts_with("/var/run") {
        std::path::PathBuf::from("/tmp")
    } else {
        tmp
    }
}

/// Fail early, and say why, when the host cannot give us the namespaces.
fn preflight() {
    let output = Command::new("unshare")
        .args(["-rnm", "true"])
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "cannot run `unshare` (util-linux): {e}\n{}",
                namespace_hint()
            )
        });
    assert!(
        output.status.success(),
        "this host cannot create an unprivileged user + network + mount namespace \
         (`unshare -rnm true` failed: {})\n{}",
        String::from_utf8_lossy(&output.stderr).trim(),
        namespace_hint()
    );
}

fn namespace_hint() -> &'static str {
    "These tests need unprivileged user namespaces, with network and mount namespaces \
     inside them. On Ubuntu 24.04 and later, AppArmor restricts them by default: lift it with \
     `sudo sysctl -w kernel.apparmor_restrict_unprivileged_userns=0`. Elsewhere, check \
     `kernel.unprivileged_userns_clone` and `user.max_user_namespaces`."
}

fn drain(mut pipe: impl Read + Send + 'static) -> JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let mut bytes = Vec::new();
        // A read error only truncates what is replayed; the exit status
        // still decides the test.
        if let Err(e) = pipe.read_to_end(&mut bytes) {
            bytes.extend_from_slice(
                format!("\n[reading the child's output failed: {e}]\n").as_bytes(),
            );
        }
        bytes
    })
}
