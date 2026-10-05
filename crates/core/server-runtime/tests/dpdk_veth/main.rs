//! The DPDK transport's client path, end to end, on veth instead of a NIC.
//!
//! Each test runs a counter node on DPDK through the `net_af_packet` PMD,
//! with no hugepages, no bound NIC and no root, and drives it from a
//! kernel-TCP client on the runner's network. That checks the transport's
//! logic — the poll loop, the auth state machine, what a close does on the
//! wire — and nothing about its speed: af_packet is not a NIC. See
//! `docs/internal/dpdk-testing.md`.
//!
//! Unlike the rest of this crate's integration tests, which run on DPDK
//! only with the `dpdk` feature, these pin what the DPDK transport does
//! where it differs from kernel TCP, so they exist only on DPDK. Built only
//! with the `dpdk` feature (`required-features` in the manifest), so a
//! plain `cargo test` never needs libdpdk.
//!
//! The node is started through `melin-test-node`, so the test process must
//! run under `scripts/dpdk/netns-runner.sh`, which gives it namespaces of
//! its own with the network built (see
//! `docs/internal/dpdk-testing.md`). The host must allow
//! unprivileged user namespaces; the runner fails, saying so, when it does
//! not. Run with:
//!
//! ```sh
//! CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run -p melin-server-runtime --features dpdk --test dpdk_veth -j 1
//! ```

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use counter_server::{Counter, KIND_RESP_ACK, RequestDecoder, ResponseEncoder, increment_request};
use melin_client::{Connection, Handshake, SigningKey, Step, key};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_wire_protocol::control_codec::{TAG_AUTH_FAILED, TAG_CHALLENGE};

// ---------------------------------------------------------------------------
// Time limits
//
// Generous, because a CI runner is slow and shared, and bounded, so a hang
// fails here rather than being killed by nextest's slow-timeout (two
// 60-second periods, `.config/nextest.toml`) with nothing said about where.
// ---------------------------------------------------------------------------

/// How long the node may take to come up: EAL init, port start, journal
/// creation.
const STARTUP_LIMIT: Duration = melin_test_node::STARTUP_LIMIT;
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

/// Declare a test that runs `body` against a fresh node, given the node's
/// client address.
macro_rules! veth_test {
    ($(#[doc = $doc:literal])* $name:ident, $body:expr) => {
        $(#[doc = $doc])*
        #[test]
        fn $name() {
            with_node($body);
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
    |node| {
        let (mut refused, challenge) = connect_for_challenge(node);
        refused
            .write_all(&answer(&challenge, &unknown_key()))
            .expect("send the challenge response");
        assert_eq!(
            read_frame(&mut refused).expect("the node answers the attempt"),
            [TAG_AUTH_FAILED],
            "an unknown key is refused"
        );

        assert_slot_freed(node, "after a refused key");
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
    |node| {
        // Pipelined: both answers go in one write.
        let (mut client, challenge) = connect_for_challenge(node);
        let mut both = answer(&challenge, &unknown_key());
        both.extend_from_slice(&answer(&challenge, &writer_key()));
        client.write_all(&both).expect("send both attempts");
        assert_eq!(
            read_frame(&mut client).expect("the node answers the first attempt"),
            [TAG_AUTH_FAILED],
            "the first attempt is refused"
        );
        assert_nothing_more(&mut client, "a pipelined second attempt");
        assert_slot_freed(node, "after a pipelined second attempt");
        drop(client);

        // Sent after the verdict.
        let (mut client, challenge) = connect_for_challenge(node);
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
        assert_slot_freed(node, "after a second attempt following the verdict");
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
    |node| {
        let (mut client, challenge) = connect_for_challenge(node);
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
        assert_slot_freed(node, "after a refused key");

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
    |node| {
        let conn = served_within(node, STARTUP_LIMIT, "the first client");
        // Closes the socket: the client's FIN goes out now.
        drop(conn);

        if challenge_within(node, SLOT_FREED_WITHIN).is_ok() {
            panic!(
                "the node freed a closed client's slot within {SLOT_FREED_WITHIN:?}, before \
                 its heartbeat: DPDK now sees a client's FIN. Flip this test to require the \
                 slot back promptly and close the entry in \
                 docs/internal/transport-divergences-2026-10.md"
            );
        }

        // The heartbeat goes out within a second of HEARTBEAT after the
        // last reply; the margin covers that and the RST's way back.
        served_within(node, HEARTBEAT + FRAME_LIMIT, "after the node's heartbeat");
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

/// Connect a bare socket to `node` and read its challenge, retrying until
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
fn connect_for_challenge(node: SocketAddr) -> (TcpStream, Vec<u8>) {
    challenge_within(node, STARTUP_LIMIT)
        .unwrap_or_else(|e| panic!("no challenge from {node} within {STARTUP_LIMIT:?}: {e}"))
}

/// [`connect_for_challenge`], giving up after `limit` with the last
/// attempt's error.
fn challenge_within(node: SocketAddr, limit: Duration) -> io::Result<(TcpStream, Vec<u8>)> {
    let deadline = Instant::now() + limit;
    loop {
        let attempt =
            TcpStream::connect_timeout(&node, Duration::from_millis(500)).and_then(|mut stream| {
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

/// An authorised client connects to `node`, and is served, within
/// [`SLOT_FREED_WITHIN`]. The node runs with `max_connections` at one, so
/// this proves the previous connection's slot was released.
///
/// The client then closes, and its slot stays taken until the node's next
/// heartbeat (see [`connect_for_challenge`]).
fn assert_slot_freed(node: SocketAddr, after: &str) {
    drop(served_within(node, SLOT_FREED_WITHIN, after));
}

/// An authorised client that connected to `node`, and was served, within
/// `limit`.
fn served_within(node: SocketAddr, limit: Duration, after: &str) -> Connection {
    let deadline = Instant::now() + limit;
    let mut conn = Connection::connect_by(node, &writer_key(), deadline).unwrap_or_else(|e| {
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

/// Serialises the tests under a plain `cargo test`, which runs them on
/// parallel threads of one process: each starts its node on the same slot
/// of the runner's network, and each node busy-polls a core. Under nextest
/// every test has a process of its own, and the `dpdk-serial` test group
/// keeps them one at a time.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

/// Run `body` against a fresh counter node on DPDK, then stop the node.
///
/// No cleanup on a panic: the test has failed, and its process exiting
/// takes the node, the namespaces and the tmpfs with it.
fn with_node(body: fn(SocketAddr)) {
    // A poisoned lock only means an earlier test failed; that one has
    // reported itself, and this one has nothing to inherit from it.
    let _serial = ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Deliberately ignored: a subscriber an earlier test in this process
    // installed is just as good.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "info,melin_server_runtime::dpdk_transport=debug",
                )
            }),
        )
        .with_test_writer()
        .with_thread_names(true)
        .try_init();

    let dir = tempfile::tempdir().expect("tempdir");
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
        journal: dir.path().join("counter.journal"),
        authorized_keys,
        standalone: true,
        ack_policy: AckPolicy::Disk,
        no_mlock: true,
        // Unpinned: the node shares the host with the test's client, and
        // a CI runner with everything else.
        cores: PipelineCores::unpinned(),
        snapshot_interval_ms: 0,
        health_bind: None,
        // One slot, so a test can tell whether a closed connection's slot
        // came back: the next client gets in only if it did.
        max_connections: 1,
        heartbeat_interval_secs: HEARTBEAT.as_secs(),
        ..ServerConfig::default()
    };
    let node = melin_test_node::start::<Counter>(
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    );
    body(node.addr());
    node.stop();
}
