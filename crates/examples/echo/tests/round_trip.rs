//! The example's behaviour, end to end, in three sections:
//!
//! 1. Over raw frames: start echo-server, connect a TCP client with
//!    Ed25519 auth, and check that what goes in comes back — at every
//!    size, and not for what the server must refuse.
//! 2. Against the journal on disk: every echo is there, in order, and the
//!    node comes back from a snapshot plus the tail.
//! 3. Through the command-line client, run as a real process.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`).

use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use std::io::{Read, Write};

use melin_client::{Connection, SigningKey, key};
use melin_journal::{JournalEvent, JournalReader};
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::Node as Server;
use melin_wire_protocol::control_codec::{
    TAG_APP, TAG_BATCH_END, TAG_CHALLENGE_RESPONSE, TAG_ENGINE_ERROR, TAG_RESPONSE_HEARTBEAT,
    TAG_SERVER_BUSY,
};

use echo_server::{Echo, KIND_RESP_ECHO, MAX_PAYLOAD, Payload, RequestDecoder, ResponseEncoder};
use melin_server_runtime::StartupEvents;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Echo `payload` and return the reply's `(kind, bytes)`.
fn exchange(node: &mut Connection, payload: &[u8]) -> (u8, Vec<u8>) {
    let reply = node.request_one(payload).expect("one reply");
    (reply[0], reply[1..].to_vec())
}

/// Send a request the server is expected to drop: no reply is read.
fn send(node: &mut Connection, payload: &[u8]) {
    node.send(payload).expect("send");
}

/// `len` bytes that no other length or seed produces, so a reply can only
/// match the request it answers.
fn bytes(len: usize, seed: u8) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
}

/// Connect and authenticate, retrying until the server is serving: the
/// kernel accepts the connection before the accept loop runs, so the
/// client has to get through the handshake to know.
fn connect_authenticated(addr: SocketAddr, key: &SigningKey) -> Connection {
    let mut node =
        Connection::connect_by(addr, key, Instant::now() + melin_test_node::STARTUP_LIMIT)
            .expect("a serving node");
    // Generous: the suite shares the machine, and how fast a node answers
    // under full-suite load is not what these tests check. A refused
    // request still surfaces — as a 30 s `NoReply` rather than a 5 s one:
    // the node's idle-connection heartbeats do not push the timeout back.
    node.set_read_timeout(Duration::from_secs(30))
        .expect("set timeout");
    node
}

/// The writing identity. `writer`, not `operator`: echoing is gated on a
/// writing role, so the round trip must authenticate as one.
fn writer_key() -> SigningKey {
    SigningKey::from_bytes(&[0xAA; 32])
}

/// May ping, may not echo.
fn reader_key() -> SigningKey {
    SigningKey::from_bytes(&[0xBB; 32])
}

fn pubkey_b64(key: &SigningKey) -> String {
    key::public_key_base64(&key.verifying_key())
}

/// Start a server whose journal and `authorized_keys` live in `dir`.
///
/// Starting twice on the same `dir` restarts the node on its existing
/// journal (and snapshot, if one was taken), which is how the recovery
/// test exercises replay. Both test identities are authorized every
/// time, so a test picks its role by picking its key.
fn start_server_in(dir: &Path) -> Server {
    start_server_with(dir, |_| {})
}

/// [`start_server_in`], with `configure` applied to the config first.
fn start_server_with(dir: &Path, configure: impl FnOnce(&mut ServerConfig)) -> Server {
    melin_test_node::logs::capture("debug");

    let auth_path = dir.join("authorized_keys");
    std::fs::write(
        &auth_path,
        format!(
            "writer {} test\nreader {} watch\n",
            pubkey_b64(&writer_key()),
            pubkey_b64(&reader_key()),
        ),
    )
    .expect("write auth keys");

    let mut config = ServerConfig {
        journal: dir.join("echo.journal"),
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: AckPolicy::Disk,
        // Unpinned, and therefore yielding: the suite runs many nodes at
        // once, and the default layout would stack every node's same-role
        // thread on one core while a spinner would starve whatever shares
        // its core, the test's own client included.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..melin_test_node::config()
    };
    configure(&mut config);

    melin_test_node::start::<Echo>(
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

/// Start a server in a fresh temporary directory. The directory is
/// returned so the caller keeps it alive for as long as the server runs —
/// the journal lives inside it.
fn start_server() -> (tempfile::TempDir, Server) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let server = start_server_in(tmp.path());
    (tmp, server)
}

/// The payloads of every journaled echo, in journal order.
fn journaled_echoes(dir: &Path) -> Vec<Vec<u8>> {
    let mut reader =
        JournalReader::<Payload>::open(&dir.join("echo.journal")).expect("open journal");
    let mut echoes = Vec::new();
    while let Some(entry) = reader.next_entry().expect("read entry") {
        if let JournalEvent::App(payload) = entry.event {
            echoes.push(payload.as_bytes().to_vec());
        }
    }
    echoes
}

// ---------------------------------------------------------------------------
// Over raw frames
// ---------------------------------------------------------------------------

#[test]
fn an_echo_returns_the_bytes_it_was_sent() {
    let (_tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr(), &writer_key());

    // Sizes either side of the `u8` boundary included: the length is
    // carried in two bytes, and 256 is where a one-byte length would wrap.
    for (i, len) in [0, 1, 7, 255, 256, MAX_PAYLOAD].into_iter().enumerate() {
        let sent = bytes(len, i as u8);
        let (kind, back) = exchange(&mut stream, &sent);
        assert_eq!(kind, KIND_RESP_ECHO, "{len} bytes");
        assert_eq!(back, sent, "{len} bytes");
    }

    drop(stream);
    server.stop();
}

#[test]
fn an_oversized_payload_is_refused_without_dropping_the_connection() {
    let (_tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr(), &writer_key());

    // One byte past the cap. The runtime drops the frame at the decoder
    // without a response and keeps the connection, so the refusal is
    // observable only as the next reply answering the next request rather
    // than this one.
    send(&mut stream, &bytes(MAX_PAYLOAD + 1, 1));

    let sent = bytes(MAX_PAYLOAD, 2);
    assert_eq!(exchange(&mut stream, &sent), (KIND_RESP_ECHO, sent));

    drop(stream);
    server.stop();
}

#[test]
fn a_reader_key_cannot_echo() {
    let (tmp, server) = start_server();

    // Refused at the decoder: no reply, connection kept — there is no
    // request a reader key can make that would be answered, so the
    // refusal is observable only in the journal afterwards.
    let mut watcher = connect_authenticated(server.addr(), &reader_key());
    send(&mut watcher, b"not mine to journal");

    let sent = bytes(MAX_PAYLOAD, 9);
    let mut stream = connect_authenticated(server.addr(), &writer_key());
    assert_eq!(exchange(&mut stream, &sent), (KIND_RESP_ECHO, sent.clone()));

    drop(stream);
    drop(watcher);
    server.stop();
    assert_eq!(
        journaled_echoes(tmp.path()),
        [sent],
        "a reader key must not be able to write the journal"
    );
}

/// The body is the application's from its first byte: a payload that
/// starts with a byte the protocol uses as a tag, or that is empty, is
/// carried to the application and back as it is — neither the node nor
/// the client reads it as one of the protocol's own frames.
#[test]
fn a_payload_that_looks_like_a_protocol_frame_is_echoed() {
    let (tmp, server) = start_server();
    let mut stream = connect_authenticated(server.addr(), &writer_key());

    let payloads: Vec<Vec<u8>> = vec![
        vec![],
        vec![0x00],
        vec![TAG_RESPONSE_HEARTBEAT],
        vec![TAG_BATCH_END, 0xAA],
        vec![TAG_ENGINE_ERROR],
        vec![TAG_SERVER_BUSY],
        vec![TAG_CHALLENGE_RESPONSE; 97],
        vec![TAG_APP, TAG_APP],
    ];
    for sent in &payloads {
        assert_eq!(
            exchange(&mut stream, sent),
            (KIND_RESP_ECHO, sent.clone()),
            "{sent:02x?}"
        );
    }

    drop(stream);
    server.stop();
    assert_eq!(journaled_echoes(tmp.path()), payloads);
}

/// A frame that is not an application frame is dropped before the
/// application sees it, and the connection kept: a request framed the way
/// a pre-release build did (an application tag in the protocol's place),
/// one of the protocol's own tags, and an empty frame. Only the
/// application frame after them is answered, and only it is journaled.
#[test]
fn a_frame_that_is_not_an_application_frame_is_dropped() {
    let (tmp, server) = start_server();
    // The raw socket, authenticated: `melin-client` only sends
    // application frames, and these are the frames it cannot send.
    let mut stream = connect_authenticated(server.addr(), &writer_key()).into_stream();

    let raw_frame = |payload: &[u8]| -> Vec<u8> {
        [&(payload.len() as u32).to_le_bytes()[..], payload].concat()
    };
    let sent = bytes(16, 7);
    let wire = [
        raw_frame(&[&[0x10][..], &bytes(8, 1)].concat()),
        raw_frame(&[TAG_RESPONSE_HEARTBEAT]),
        raw_frame(&[]),
        raw_frame(&[&[TAG_APP][..], &sent].concat()),
    ]
    .concat();
    stream.write_all(&wire).expect("write frames");

    let mut read_payload = || {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).expect("read length");
        let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
        stream.read_exact(&mut payload).expect("read payload");
        payload
    };
    assert_eq!(
        read_payload(),
        [&[TAG_APP, KIND_RESP_ECHO][..], &sent].concat(),
        "the first reply answers the application frame"
    );
    assert_eq!(read_payload(), [TAG_BATCH_END]);

    drop(stream);
    server.stop();
    assert_eq!(journaled_echoes(tmp.path()), [sent]);
}

// ---------------------------------------------------------------------------
// The client on DPDK too
// ---------------------------------------------------------------------------

/// A client on DPDK, beside its node on DPDK: `melin-client`'s polled
/// connection on a NIC handle, from the runner's client pair, through the
/// whole path (EAL shared with the node, the dial, the handshake, the
/// in-place send, the reply loop), and again on the same handle once the
/// first connection has gone. Then every way a dial can fail, on that same
/// handle: a port nothing listens on (refused, not timed out), an address
/// nothing answers for (timed out, the error naming the MAC the SYN went
/// to), and a key the node refuses; and after each, the handle dials
/// again. Then several streams on the handle at once, as a load generator
/// runs them: one polled connection per stream, driven from one loop that
/// services the handle once per iteration, with requests in flight on
/// every stream together; each closed with the handle's own close. Every
/// close, and a dropped connection's, frees its slot on the node at once,
/// as the node's health endpoint counts them. Last, the handle dials once
/// more. One test rather than several: the
/// launcher opens the client NIC once per process, and the rest of the
/// case is what a client does with the handle it has.
///
/// The node's MAC is passed through. The runner's MACs are not the
/// `02:00:<ip>` convention the dialer would fall back to without one, so a
/// dial that took the fallback would time out.
#[cfg(feature = "dpdk")]
#[test]
fn a_dpdk_client_echoes_through_a_dpdk_node() {
    use melin_client::{NicConfig, PeerMacSource, StreamConnection};

    // The node's count of authenticated connections, read from its health
    // endpoint, is how the case sees a closed stream free its slot. With
    // no heartbeat and no idle timeout, nothing but the client's reset can
    // free one, however long the case runs; the bound it is given is in
    // any case well under the defaults of both, which would free it
    // eventually.
    let defaults = ServerConfig::default();
    for (what, eventually) in [
        ("heartbeat", defaults.heartbeat_interval()),
        ("idle timeout", defaults.connection_timeout()),
    ] {
        let eventually = eventually.expect("on by default");
        assert!(
            SLOT_FREED_WITHIN * 4 <= eventually,
            "the bound on a freed slot ({SLOT_FREED_WITHIN:?}) must be well under the default \
             {what} ({eventually:?}), or it proves nothing about the reset"
        );
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let health = free_loopback_addr();
    let server = start_server_with(tmp.path(), |config| {
        config.health_bind = Some(health);
        config.heartbeat_interval_secs = 0;
        config.connection_timeout_secs = 0;
    });
    // The launcher returns before the node serves, and the polled
    // connection has no retry-until-a-deadline twin of `connect_by`: the
    // kernel client, which has, waits for it, so the DPDK dial below is
    // one attempt at a node already serving.
    drop(connect_authenticated(server.addr(), &writer_key()));
    // That kernel client's close frees its slot: the node takes the FIN as
    // kernel TCP takes EOF, without waiting for a heartbeat, which this
    // node never sends. Every count below is on top of an idle node.
    let idle = 0;
    wait_for_active(
        health,
        idle,
        SLOT_FREED_WITHIN,
        "once the kernel client that waited for the node closed",
    );

    let SocketAddr::V4(node) = server.addr() else {
        panic!("a DPDK node is on IPv4: {}", server.addr());
    };
    let node_mac = melin_test_node::mac_of(server.addr());
    let o = node.ip().octets();
    assert_ne!(
        node_mac,
        [0x02, 0x00, o[0], o[1], o[2], o[3]],
        "the runner's MACs must not be the fallback convention, or this test proves nothing"
    );
    let mut nic = melin_test_node::open_dpdk_client(NicConfig {
        peer_mac: Some(node_mac),
        ..NicConfig::default()
    });
    assert_eq!(nic.mac(), melin_test_node::dpdk_client().mac);
    assert_eq!(
        nic.peer_mac(*node.ip()),
        (node_mac, PeerMacSource::Supplied)
    );

    let timeout = Duration::from_secs(30);
    let first: Vec<Vec<u8>> = [0, 1, 255, 256, MAX_PAYLOAD]
        .into_iter()
        .enumerate()
        .map(|(i, len)| bytes(len, i as u8))
        .collect();
    {
        let mut client = StreamConnection::connect_dpdk_on(
            &mut nic,
            node,
            &writer_key(),
            SOCKET_BUFFER,
            timeout,
        )
        .expect("a DPDK client connected to the node");
        for sent in &first {
            let reply = client.request_one(sent).expect("one reply");
            assert_eq!(reply[0], KIND_RESP_ECHO, "{} bytes", sent.len());
            assert_eq!(&reply[1..], &sent[..], "{} bytes", sent.len());
        }
    }

    // The handle outlives its connections: a second one dials on it again,
    // from a fresh local port.
    let again = bytes(16, 0x40);
    {
        let mut client = StreamConnection::connect_dpdk_on(
            &mut nic,
            node,
            &writer_key(),
            SOCKET_BUFFER,
            timeout,
        )
        .expect("a second DPDK connection on the same NIC handle");
        let reply = client.request_one(&again).expect("one reply");
        assert_eq!(reply, [&[KIND_RESP_ECHO][..], &again].concat());
    }

    // --- The ways a dial fails, each leaving the handle good for the next.

    // Nothing listens on this port of the node, and a DPDK node's stack
    // answers a SYN that matches no listener with an RST: refused, at once,
    // and told apart from a timeout. Not the replication port, which a
    // standalone node does not open either but which a replica's would.
    let nobody = std::net::SocketAddrV4::new(*node.ip(), node.port() + 100);
    let refused =
        StreamConnection::connect_dpdk_on(&mut nic, nobody, &writer_key(), SOCKET_BUFFER, timeout)
            .expect_err("nothing listens there");
    match refused {
        melin_client::Error::Connect { addr, source } => {
            assert_eq!(addr, SocketAddr::V4(nobody));
            assert_eq!(
                source.kind(),
                std::io::ErrorKind::ConnectionRefused,
                "{source}"
            );
        }
        other => panic!("expected a refused dial, got {other}"),
    }

    // An address on the runner's network (a /24; the last octet is free)
    // that nothing owns. The SYN goes to the node's MAC, the one seeded for
    // every peer, with an IP the node's stack does not answer for, so
    // nothing comes back: a timeout, whose error names that MAC and where
    // it came from as the first thing to check.
    let o = node.ip().octets();
    let nowhere =
        std::net::SocketAddrV4::new(std::net::Ipv4Addr::new(o[0], o[1], o[2], 250), node.port());
    let timed_out = StreamConnection::connect_dpdk_on(
        &mut nic,
        nowhere,
        &writer_key(),
        SOCKET_BUFFER,
        Duration::from_secs(3),
    )
    .expect_err("nothing answers there");
    match timed_out {
        melin_client::Error::Connect { source, .. } => {
            assert_eq!(source.kind(), std::io::ErrorKind::TimedOut, "{source}");
            let message = source.to_string();
            assert!(
                message.contains(&melin_client::fmt_mac(node_mac)),
                "{message}"
            );
            assert!(message.contains("supplied"), "{message}");
        }
        other => panic!("expected a timed-out dial, got {other}"),
    }

    // A key the node has never heard of: the dial succeeds, the handshake
    // is refused, and the stream is closed with the error.
    let stranger = SigningKey::from_bytes(&[0xCC; 32]);
    let unauthorized =
        StreamConnection::connect_dpdk_on(&mut nic, node, &stranger, SOCKET_BUFFER, timeout)
            .expect_err("an unauthorized key");
    assert!(
        matches!(unauthorized, melin_client::Error::AuthFailed { .. }),
        "{unauthorized}"
    );

    // After all three, the handle dials and echoes as before.
    let last = bytes(32, 0x50);
    {
        let mut client = StreamConnection::connect_dpdk_on(
            &mut nic,
            node,
            &writer_key(),
            SOCKET_BUFFER,
            timeout,
        )
        .expect("a DPDK connection after the failed dials");
        let reply = client.request_one(&last).expect("one reply");
        assert_eq!(reply, [&[KIND_RESP_ECHO][..], &last].concat());
    }

    // --- Several streams on the handle, driven from one loop, each closed
    // with the handle's own close.
    let interleaved = several_streams_on_one_handle(&mut nic, node, timeout, health, idle);

    // --- A closed stream frees the node's slot at once: here a connection
    // that owns its stream, which it closes as it drops.
    let dropped = bytes(24, 0x58);
    {
        let mut client = StreamConnection::connect_dpdk_on(
            &mut nic,
            node,
            &writer_key(),
            SOCKET_BUFFER,
            timeout,
        )
        .expect("a DPDK connection for the slot check");
        let reply = client.request_one(&dropped).expect("one reply");
        assert_eq!(reply, [&[KIND_RESP_ECHO][..], &dropped].concat());
        wait_for_active(
            health,
            idle + 1,
            SLOT_FREED_WITHIN,
            "with the connection open",
        );
    }
    wait_for_active(
        health,
        idle,
        SLOT_FREED_WITHIN,
        "once the connection dropped",
    );

    // With every stream closed, the handle dials once more.
    let closing = bytes(48, 0x60);
    {
        let mut client = StreamConnection::connect_dpdk_on(
            &mut nic,
            node,
            &writer_key(),
            SOCKET_BUFFER,
            timeout,
        )
        .expect("a DPDK connection after the handle's streams were closed");
        let reply = client.request_one(&closing).expect("one reply");
        assert_eq!(reply, [&[KIND_RESP_ECHO][..], &closing].concat());
    }

    drop(nic);
    server.stop();

    // The sections that ran one connection at a time are in the journal in
    // the order they ran. The interleaved section's order across streams is
    // the node's to choose, so its echoes are checked stream by stream:
    // each stream's in the order it sent them, and nothing else among them.
    let mut sequential = first;
    sequential.push(again);
    sequential.push(last);
    let interleaved_count: usize = interleaved.iter().map(Vec::len).sum();
    let journal = journaled_echoes(tmp.path());
    assert_eq!(
        journal.len(),
        sequential.len() + interleaved_count + 2,
        "every echo the case was answered for is journaled, and nothing else"
    );
    let (before, rest) = journal.split_at(sequential.len());
    assert_eq!(before, sequential);
    let (during, after) = rest.split_at(interleaved_count);
    assert_eq!(after, [dropped, closing]);
    for (stream, sent) in interleaved.iter().enumerate() {
        let journaled: Vec<&Vec<u8>> = during
            .iter()
            .filter(|echo| echo.first() == Some(&stream_marker(stream)))
            .collect();
        assert_eq!(
            journaled,
            sent.iter().collect::<Vec<_>>(),
            "stream {stream}'s echoes, in its order"
        );
    }
}

/// A DPDK stream's socket buffers, in the DPDK client case: room for a few
/// full-sized requests.
#[cfg(feature = "dpdk")]
const SOCKET_BUFFER: usize = 64 * 1024;

/// How many streams the multi-stream section dials on the one handle: a
/// handful, as a bench's connections are, within the runner's one client
/// pair.
#[cfg(feature = "dpdk")]
const STREAMS: usize = 4;

/// Requests each stream sends in the multi-stream section.
#[cfg(feature = "dpdk")]
const REQUESTS_PER_STREAM: usize = 8;

/// Requests a stream keeps in flight at once in the multi-stream section:
/// more than one, so the node has several streams' requests queued
/// together and a reply can only be told apart by the stream it comes
/// back on.
#[cfg(feature = "dpdk")]
const IN_FLIGHT_PER_STREAM: usize = 3;

/// The first byte of every request stream `stream` sends: no other stream's
/// request starts with it, so a reply that came back on the wrong stream,
/// or a journal entry, names the stream it belongs to.
#[cfg(feature = "dpdk")]
fn stream_marker(stream: usize) -> u8 {
    0xE0 + stream as u8
}

/// Request `request` of stream `stream`: its marker, its index, then
/// filler, at a length that cycles through the sizes the single-stream
/// section uses (the `u8` boundary of the length, and the cap), so the
/// streams' replies differ in size as well as content at any one moment.
#[cfg(feature = "dpdk")]
fn stream_request(stream: usize, request: usize) -> Vec<u8> {
    const LENGTHS: [usize; 5] = [2, 17, 255, 256, MAX_PAYLOAD];
    let len = LENGTHS[(stream + request) % LENGTHS.len()];
    let mut payload = bytes(len, (stream * 31 + request * 7) as u8);
    payload[0] = stream_marker(stream);
    payload[1] = request as u8;
    payload
}

/// One stream of the client's NIC handle and the protocol state over it,
/// as a load generator holds them: the stream is the handle's, lent per
/// call by its id, and the [`melin_client::PolledConnection`] is handed
/// the view and the loop's time on every call.
#[cfg(feature = "dpdk")]
struct HandleStream {
    id: melin_client::StreamId,
    connection: melin_client::PolledConnection,
    /// The requests this stream sends, in order. `Vec`s: a handful each,
    /// compared whole at the end.
    requests: Vec<Vec<u8>>,
    /// How many of `requests` have been sent.
    sent: usize,
    /// The application frames that came back, in order.
    replies: Vec<Vec<u8>>,
    /// How many reply batches have ended.
    answered: usize,
    /// The most requests this stream has had sent and unanswered at once,
    /// measured as its sends of an iteration end: the proof that its own
    /// requests were in flight together, not one at a time.
    deepest: usize,
}

#[cfg(feature = "dpdk")]
impl HandleStream {
    /// Requests sent and not yet answered.
    fn owed(&self) -> usize {
        self.sent - self.answered
    }
}

/// One loop over every stream of `nic`, until `done` holds for each, as a
/// client with several connections on one NIC runs it (the shape of the
/// client binding's multi-stream example, and of the exchange's bench):
/// the handle's service step once per iteration, at the handle's clock,
/// then for each stream in turn its queued requests topped up to `depth`
/// in flight and every frame that has arrived for it taken. The
/// handshakes run inside the polls; a request waits for its stream's.
///
/// Returns the most streams that owed a reply at the end of one
/// iteration: the proof that their requests were in flight together.
#[cfg(feature = "dpdk")]
fn drive_streams(
    nic: &mut melin_client::NicHandle,
    streams: &mut [HandleStream],
    depth: usize,
    limit: Duration,
    done: impl Fn(&HandleStream) -> bool,
) -> usize {
    use melin_client::Frame;

    let clock = nic.clock();
    let deadline = Instant::now() + limit;
    let mut most_owing = 0;
    while !streams.iter().all(&done) {
        assert!(
            Instant::now() < deadline,
            "the streams did not get through within {limit:?}: {:?}",
            streams
                .iter()
                .map(|s| (s.id.local_port(), s.sent, s.answered, &s.connection))
                .collect::<Vec<_>>()
        );
        let now_ns = clock.now_ns();
        // Once per iteration, for every stream: the views below only lend
        // the stream, and their own service step is the same handle's.
        nic.service(now_ns);
        for (index, stream) in streams.iter_mut().enumerate() {
            while stream.connection.is_ready()
                && stream.sent < stream.requests.len()
                && stream.owed() < depth
            {
                stream
                    .connection
                    .send(
                        &mut nic.stream(&stream.id),
                        now_ns,
                        &stream.requests[stream.sent],
                    )
                    .unwrap_or_else(|e| panic!("stream {index}: send: {e}"));
                stream.sent += 1;
            }
            stream.deepest = stream.deepest.max(stream.owed());
            loop {
                let frame = stream
                    .connection
                    .poll(&mut nic.stream(&stream.id), now_ns)
                    .unwrap_or_else(|e| panic!("stream {index}: poll: {e}"));
                match frame {
                    None => break,
                    Some(Frame::Response(body)) => stream.replies.push(body.to_vec()),
                    Some(Frame::BatchEnd) => stream.answered += 1,
                    Some(other) => panic!("stream {index}: {other:?}"),
                }
            }
        }
        most_owing = most_owing.max(streams.iter().filter(|s| s.owed() > 0).count());
        std::hint::spin_loop();
    }
    most_owing
}

/// The multi-stream section of the DPDK client case: [`STREAMS`] streams
/// dialed on `nic` in turn, driven from one loop ([`drive_streams`]) with
/// several requests in flight on each at once, then closed with the
/// handle's own close, each freeing its slot on the node at once: the
/// node's health endpoint, `health`, counts `idle` connections beside
/// these. Returns each stream's requests, in the order sent, for the
/// journal check.
#[cfg(feature = "dpdk")]
fn several_streams_on_one_handle(
    nic: &mut melin_client::NicHandle,
    node: std::net::SocketAddrV4,
    timeout: Duration,
    health: SocketAddr,
    idle: u64,
) -> Vec<Vec<Vec<u8>>> {
    use melin_client::PolledConnection;

    let clock = nic.clock();
    let timeout_ns = u64::try_from(timeout.as_nanos()).expect("a timeout of seconds");
    // A `Vec`: a handful of streams, iterated in order every turn.
    let mut streams: Vec<HandleStream> = Vec::with_capacity(STREAMS);
    for index in 0..STREAMS {
        let id = nic
            .dial(node, SOCKET_BUFFER, clock.now_ns() + timeout_ns, || {
                clock.now_ns()
            })
            .unwrap_or_else(|e| panic!("dial stream {index}: {e}"));
        streams.push(HandleStream {
            id,
            connection: PolledConnection::with_timeout(&writer_key(), timeout),
            requests: (0..REQUESTS_PER_STREAM)
                .map(|request| stream_request(index, request))
                .collect(),
            sent: 0,
            replies: Vec::new(),
            answered: 0,
            deepest: 0,
        });
        // The loop runs until this stream's handshake is done before the
        // next dial, the earlier streams serviced meanwhile. Dials are
        // sequential on the handle (one returns once the connection is
        // established), and the node's side is stricter still: its one
        // listener on the port is replaced only once it has taken the
        // connection, so a SYN that reached it in the same burst as the
        // previous dial's last ACK would be refused. The node's challenge,
        // which the ready stream has answered, comes after that.
        drive_streams(nic, &mut streams, 0, timeout, |s| s.connection.is_ready());
    }

    let ports: std::collections::BTreeSet<u16> =
        streams.iter().map(|s| s.id.local_port()).collect();
    assert_eq!(ports.len(), STREAMS, "a local port per stream: {ports:?}");
    assert!(
        streams.iter().all(|s| s.id.peer() == node),
        "every stream is to the node"
    );

    let most_owing = drive_streams(nic, &mut streams, IN_FLIGHT_PER_STREAM, timeout, |s| {
        s.answered == s.requests.len()
    });
    assert_eq!(
        most_owing, STREAMS,
        "every stream had requests in flight at once"
    );
    for (index, stream) in streams.iter().enumerate() {
        assert_eq!(
            stream.deepest, IN_FLIGHT_PER_STREAM,
            "stream {index} had {IN_FLIGHT_PER_STREAM} of its own requests in flight at once"
        );
    }

    // Each reply came back on the stream that sent its request, in that
    // stream's order: the request's marker and index are in the echo.
    for (index, stream) in streams.iter().enumerate() {
        let expected: Vec<Vec<u8>> = stream
            .requests
            .iter()
            .map(|request| [&[KIND_RESP_ECHO][..], request].concat())
            .collect();
        assert_eq!(stream.replies, expected, "stream {index}'s replies");
    }

    // Closed one at a time, the node's count falling by one each time
    // without the handle being serviced again: the close sends the reset
    // itself.
    wait_for_active(
        health,
        idle + STREAMS as u64,
        SLOT_FREED_WITHIN,
        "with every stream open",
    );
    let mut open = STREAMS as u64;
    let mut requests = Vec::with_capacity(STREAMS);
    for (index, stream) in streams.into_iter().enumerate() {
        nic.close(stream.id);
        open -= 1;
        wait_for_active(
            health,
            idle + open,
            SLOT_FREED_WITHIN,
            &format!("once stream {index} was closed"),
        );
        requests.push(stream.requests);
    }
    requests
}

/// How soon a closed DPDK stream's slot must be free on the node: the
/// close resets the stream, and the node frees the slot on the reset.
/// Without the reset only the node's heartbeat or idle timeout would free
/// it; the case checks that this bound is well under the default of each,
/// and runs its node with neither.
#[cfg(feature = "dpdk")]
const SLOT_FREED_WITHIN: Duration = Duration::from_secs(2);

/// An address on the loopback that nothing listens on, for a node's health
/// endpoint. Under the runner the loopback is the test process's own.
#[cfg(feature = "dpdk")]
fn free_loopback_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free loopback port")
}

/// The node's count of authenticated connections, from the line its health
/// endpoint answers a plain connection with:
/// `OK <active_connections> <journal_seq> <replication_lag> trading|halted`.
#[cfg(feature = "dpdk")]
fn active_connections(health: SocketAddr) -> u64 {
    let mut stream = std::net::TcpStream::connect_timeout(&health, Duration::from_secs(2))
        .unwrap_or_else(|e| panic!("connect to the health endpoint at {health}: {e}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set timeout");
    let mut line = String::new();
    stream
        .read_to_string(&mut line)
        .unwrap_or_else(|e| panic!("read the health line from {health}: {e}"));
    let mut fields = line.split_whitespace();
    assert_eq!(fields.next(), Some("OK"), "the health line: {line:?}");
    fields
        .next()
        .and_then(|count| count.parse().ok())
        .unwrap_or_else(|| panic!("no connection count in the health line: {line:?}"))
}

/// Wait, at most `within`, for the node's count of authenticated
/// connections to be `expected`, reading it again every few milliseconds.
/// Panics with the last count read, and `when`.
#[cfg(feature = "dpdk")]
fn wait_for_active(health: SocketAddr, expected: u64, within: Duration, when: &str) {
    let started = Instant::now();
    loop {
        let count = active_connections(health);
        if count == expected {
            return;
        }
        assert!(
            started.elapsed() < within,
            "{when}: the node counts {count} connections, not {expected}, after {within:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

// ---------------------------------------------------------------------------
// Against the journal
// ---------------------------------------------------------------------------

#[test]
fn the_journal_holds_every_echo_in_order() {
    let (tmp, server) = start_server();
    let echoes = [bytes(0, 0), bytes(MAX_PAYLOAD, 1), bytes(5, 2)];
    {
        let mut stream = connect_authenticated(server.addr(), &writer_key());
        for sent in &echoes {
            exchange(&mut stream, sent);
        }
    }
    server.stop();

    // Under the `disk` ack policy a reply means the entry is fsynced, so
    // every echo replied to above is on disk — the audit trail of what
    // was sequenced, and in what order.
    assert_eq!(journaled_echoes(tmp.path()), echoes);
}

#[test]
fn the_node_recovers_from_a_snapshot_and_the_journal_tail() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let snapshot = tmp.path().join("echo.snapshot");
    let before = [bytes(16, 1), bytes(16, 2)];
    let after = bytes(16, 3);

    // First life, with the shadow stage taking snapshots often. A
    // snapshot of this application is zero bytes of payload inside the
    // runtime's framing — the case worth proving restores.
    let server = start_server_with(tmp.path(), |config| config.snapshot_interval_ms = 50);
    {
        let mut stream = connect_authenticated(server.addr(), &writer_key());
        for sent in &before {
            exchange(&mut stream, sent);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while !snapshot.exists() {
            assert!(Instant::now() < deadline, "no snapshot was taken");
            std::thread::sleep(Duration::from_millis(20));
        }
        // Journaled after the snapshot's anchor, or at worst inside a
        // later one: either way, the tail replay has work to do.
        exchange(&mut stream, &after);
    }
    server.stop();

    // Second life: the runtime finds the snapshot next to the journal,
    // restores the (empty) state from it and replays the tail. If either
    // step failed the node would not come up; the echo after it shows
    // the pipeline is whole, and the journal that it lost nothing.
    let server = start_server_in(tmp.path());
    let mut stream = connect_authenticated(server.addr(), &writer_key());
    let sent = bytes(MAX_PAYLOAD, 4);
    assert_eq!(exchange(&mut stream, &sent), (KIND_RESP_ECHO, sent.clone()));
    drop(stream);
    server.stop();

    let mut expected = before.to_vec();
    expected.push(after);
    expected.push(sent);
    assert_eq!(journaled_echoes(tmp.path()), expected);
}

// ---------------------------------------------------------------------------
// The command-line client
// ---------------------------------------------------------------------------

/// Run the client with `args`, returning `(exit code, stdout, stderr)`.
fn echo_client(args: &[&str]) -> (i32, String, String) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_echo-client"))
        .args(args)
        .output()
        .expect("spawn echo-client");
    (
        output.status.code().expect("exited normally"),
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        String::from_utf8(output.stderr).expect("utf-8 stderr"),
    )
}

#[test]
fn the_client_measures_a_closed_loop() {
    let (tmp, server) = start_server();
    // The binary connects once, without retrying, so wait for the server
    // to be ready the way the in-process tests do before spawning it.
    drop(connect_authenticated(server.addr(), &writer_key()));

    let key = tmp.path().join("writer.key");
    std::fs::write(&key, writer_key().to_bytes()).expect("write key");
    let addr = server.addr().to_string();
    let key_arg = key.to_str().expect("utf-8 path");
    let common = ["--server", &addr, "--key", key_arg];

    // A single request reports its one round trip.
    let (code, stdout, stderr) = echo_client(&[&common[..], &["--size", "8"]].concat());
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.starts_with("8 bytes back in "), "{stdout}");

    // A run reports the distribution. The default size is the cap.
    let (code, stdout, stderr) = echo_client(&[&common[..], &["--count", "200"]].concat());
    assert_eq!(code, 0, "{stderr}");
    assert!(
        stdout.starts_with(&format!("200 × {MAX_PAYLOAD}-byte echoes to ")),
        "{stdout}"
    );
    for label in ["min", "p50", "p90", "p99", "p99.9", "max"] {
        assert!(stdout.contains(&format!("  {label}")), "{label}: {stdout}");
    }

    // Everything the runs sent is on disk.
    server.stop();
    assert_eq!(journaled_echoes(tmp.path()).len(), 1 + 200);
}

#[test]
fn the_client_refuses_a_size_the_server_would_drop() {
    // Checked before connecting: no server is needed, and none is running
    // at this address.
    let (code, _, stderr) = echo_client(&[
        "--server",
        "127.0.0.1:1",
        "--key",
        "/nonexistent",
        "--size",
        &(MAX_PAYLOAD + 1).to_string(),
    ]);
    assert_eq!(code, 2, "{stderr}");
    assert!(
        stderr.contains(&format!("at most {MAX_PAYLOAD} bytes")),
        "{stderr}"
    );
}

#[test]
fn the_client_reports_an_unauthorized_key() {
    let (tmp, server) = start_server();
    drop(connect_authenticated(server.addr(), &writer_key()));

    // A key the server has never heard of: the handshake fails, and the
    // client says which public key to authorize.
    let key = tmp.path().join("stranger.key");
    std::fs::write(&key, [0xCC; 32]).expect("write key");
    let (code, _, stderr) = echo_client(&[
        "--server",
        &server.addr().to_string(),
        "--key",
        key.to_str().expect("utf-8 path"),
    ]);
    assert_eq!(code, 2, "{stderr}");
    assert!(
        stderr.contains(&pubkey_b64(&SigningKey::from_bytes(&[0xCC; 32]))),
        "{stderr}"
    );

    server.stop();
}
