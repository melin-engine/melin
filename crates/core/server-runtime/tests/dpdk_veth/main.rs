//! The DPDK transport's client path, end to end, on veth instead of a NIC,
//! the liveness deadline on its replication links, and a primary that keeps
//! serving while a replica's join waits on its disk or on a replica that
//! has stopped reading.
//!
//! Each test runs counter nodes on DPDK through the `net_af_packet` PMD,
//! with no hugepages, no bound NIC and no root, and drives them from a
//! kernel-TCP client on the runner's network. That checks the transport's
//! logic — the poll loop, the auth state machine, what a close does on the
//! wire, when a silent peer counts as gone — and nothing about its speed:
//! af_packet is not a NIC. See `docs/internal/dpdk-testing.md`.
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
//! CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER="$PWD/scripts/dpdk/netns-runner.sh" cargo nextest run --profile dpdk -p melin-server-runtime --features dpdk --test dpdk_veth
//! ```

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use counter_server::{
    Counter, CounterEvent, KIND_RESP_ACK, KIND_RESP_REJECTED, RequestDecoder, ResponseEncoder,
    increment_request,
};
use melin_client::{Connection, Handshake, SigningKey, Step, key};
use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};
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

/// A replica that goes without a word — cut off the network, as a crashed
/// host or a pulled cable leaves it — is dropped by its primary within
/// the replication liveness bound, and the primary, its last replica
/// gone, halts and refuses writes.
///
/// The case the liveness deadline exists for: a replica that stops
/// cleanly tells its primary (`halt_refusal` covers that, on both
/// transports), so only a link that falls silent reaches the deadline.
/// DPDK only, because a DPDK node is its own TCP stack: on kernel TCP the
/// kernel speaks for a crashed process.
#[test]
fn a_replica_cut_off_is_dropped_and_its_primary_halts() {
    let _serial = serialise();
    let cluster = Cluster::new();
    let primary_health = Cluster::health(0);

    let primary = cluster.start_primary();
    let replica = cluster.start_replica(1, "replica");

    wait_for_gauge(primary_health, "melin_replicas_connected", 1, STARTUP_LIMIT);
    let mut conn = served_within(primary.addr(), STARTUP_LIMIT, "with the replica attached");

    replica.cut_off();
    // The peer is gone at most twice the timeout after its last word (the
    // primary is sending to it: heartbeats); the margin covers the gauge
    // poll and a slow runner.
    let bound = melin_dpdk::PeerLiveness::REPLICATION.timeout() * 2 + FRAME_LIMIT;
    wait_for_gauge(primary_health, "melin_replicas_connected", 0, bound);
    let refused = conn
        .request_one(&increment_request(1))
        .expect("the halted primary answers");
    assert_eq!(
        refused.first(),
        Some(&KIND_RESP_REJECTED),
        "a primary whose last replica has gone must refuse writes"
    );
    drop(conn);

    replica.stop();
    primary.stop();
}

/// A DPDK replica refuses a client's connection outright (its stack
/// listens on no port until promotion), rather than leaving it unanswered
/// as a kernel-TCP replica does: the behaviour docs/replication.md
/// promises operators. The replica is attached first, so its port is up
/// and answering ARP: a refusal then comes from the replica's stack, not
/// from an address nobody holds yet.
#[test]
fn a_replica_refuses_clients_until_promoted() {
    let _serial = serialise();
    let cluster = Cluster::new();

    let primary = cluster.start_primary();
    let replica = cluster.start_replica(1, "replica");
    wait_for_gauge(
        Cluster::health(0),
        "melin_replicas_connected",
        1,
        STARTUP_LIMIT,
    );

    match TcpStream::connect_timeout(&replica.addr(), FRAME_LIMIT) {
        Err(e) => assert_eq!(
            e.kind(),
            io::ErrorKind::ConnectionRefused,
            "a replica's client port must refuse, not time out: {e}"
        ),
        Ok(_) => panic!("a replica accepted a client's connection"),
    }

    replica.stop();
    primary.stop();
}

/// A replica's join that stalls on the primary's disk — here the snapshot
/// read, held for longer than the replication liveness deadline twice
/// over — holds up nothing else: the primary goes on serving its clients
/// and answering its other replica, which keeps its link throughout.
///
/// DPDK only, because only there is the replication sender the thread
/// that runs the TCP stack: when that thread did the join's disk work
/// itself, a stall past the deadline left every other replica's link
/// unanswered, and healthy replicas reset their links to a live primary.
///
/// The stall is injected without a hook: the primary's snapshot is a FIFO
/// the test holds open and writes nothing into, so the join blocks reading
/// it exactly as it would on a slow disk. The joining replica is made to
/// need a snapshot by giving it a journal of its own that the primary's
/// does not share (a divergent replica gets a snapshot, whatever its
/// position).
#[test]
fn a_stalled_join_costs_the_other_replica_nothing() {
    let _serial = serialise();
    let cluster = Cluster::new();
    let primary_health = Cluster::health(0);

    let primary = cluster.start_primary();
    let streaming = cluster.start_replica(1, "streaming");
    wait_for_gauge(primary_health, "melin_replicas_connected", 1, STARTUP_LIMIT);
    let mut conn = served_within(primary.addr(), STARTUP_LIMIT, "with one replica attached");
    for _ in 0..4 {
        assert_acked(&mut conn, "before the join");
    }

    // The primary's snapshot, as a FIFO. Created only now: a primary
    // reads its snapshot at startup.
    let snapshot = cluster.dir.path().join("primary.snapshot");
    let stalled_snapshot = StalledFile::create(&snapshot);

    // The joining replica's own history: one event the primary never had.
    let joining_journal = cluster.dir.path().join("joining.journal");
    let mut writer =
        BufferedWriter::<CounterEvent>::create(&joining_journal).expect("create journal");
    writer
        .append(&JournalEvent::App(CounterEvent::Increment { amount: 7 }))
        .expect("append");
    drop(writer);
    let joining = cluster.start_replica(2, "joining");

    // The join has begun once the primary has judged the replica
    // divergent: from there the join reads the snapshot, and blocks.
    wait_for_gauge(
        primary_health,
        "melin_replica_divergence_total",
        1,
        STARTUP_LIMIT,
    );

    // Requests during the stall are each answered promptly, from the
    // first second to past twice the liveness timeout (the longest a dead
    // peer can take to be noticed), and both replicas stay counted: the
    // streaming one never lost its link, and the joining one is still in
    // its join.
    conn.set_read_timeout(SERVED_DURING_STALL)
        .expect("set the client's read timeout");
    let stall = melin_dpdk::PeerLiveness::REPLICATION.timeout() * 2 + Duration::from_secs(2);
    let stall_end = Instant::now() + stall;
    while Instant::now() < stall_end {
        let asked = Instant::now();
        assert_acked(&mut conn, "during the stalled join");
        assert!(
            asked.elapsed() < SERVED_DURING_STALL,
            "a request took {:?} during the stalled join",
            asked.elapsed()
        );
        // Sampled, so a link dropped and re-made between two samples would
        // slip past this check alone; the bound above is what rules that
        // out for a stall, since the requests share the poll thread with
        // the links and a peer is only declared gone after the poll
        // thread has been silent for the whole liveness timeout.
        assert_eq!(
            gauge(primary_health, "melin_replicas_connected"),
            Some(2),
            "a replica was dropped during the stalled join"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        stalled_snapshot.has_reader(),
        "the join must still be blocked on the snapshot for the test to mean anything"
    );

    // Unblock the join (it reads the end of an empty snapshot and fails,
    // dropping the joining replica, which then retries without one).
    drop(stalled_snapshot);
    drop(conn);
    joining.stop();
    streaming.stop();
    primary.stop();
}

/// Both kinds of join complete over DPDK when what they stream is many
/// times what a replication socket's queue holds, so the primary hands it
/// over across many ticks: a fresh replica's journal catch-up, and a
/// divergent replica's snapshot, segment seed and catch-up. Each replica
/// ends up with the primary's whole history. (Being served while a join
/// is held up is `a_stalled_join_costs_the_other_replica_nothing`'s to
/// check; this one checks only that a join spread over many ticks
/// completes intact.)
#[test]
fn large_joins_complete_a_tick_at_a_time() {
    let _serial = serialise();
    let cluster = Cluster::new();
    let primary_health = Cluster::health(0);

    // A history of a few MiB, written before the primary starts so that
    // it costs no round trips.
    const HISTORY: u64 = 150_000;
    let mut writer =
        BufferedWriter::<CounterEvent>::create(&cluster.dir.path().join("primary.journal"))
            .expect("create journal");
    for _ in 0..HISTORY {
        writer
            .append(&JournalEvent::App(CounterEvent::Increment { amount: 1 }))
            .expect("append");
    }
    drop(writer);

    let mut primary_config = cluster.config(0, "primary");
    // A snapshot to serve the divergent replica.
    primary_config.snapshot_interval_ms = 200;
    let primary_addrs = Cluster::addrs(0);
    primary_config.replication_bind = Some(primary_addrs.replication());
    let primary = Cluster::start(&primary_addrs, primary_config);

    // The fresh replica: full catch-up from the journal.
    let fresh = cluster.start_replica(1, "fresh");
    wait_for_gauge(primary_health, "melin_replicas_connected", 1, STARTUP_LIMIT);
    let mut conn = served_within(primary.addr(), STARTUP_LIMIT, "with the fresh replica");
    // Events through the pipeline, so the shadow stage snapshots.
    for _ in 0..4 {
        assert_acked(&mut conn, "before the snapshot");
    }
    let snapshot = cluster.dir.path().join("primary.snapshot");
    let deadline = Instant::now() + STARTUP_LIMIT;
    while !snapshot.exists() {
        assert!(Instant::now() < deadline, "the primary wrote no snapshot");
        std::thread::sleep(Duration::from_millis(100));
    }

    // The divergent replica: its own one-event history.
    let divergent_journal = cluster.dir.path().join("divergent.journal");
    let mut writer =
        BufferedWriter::<CounterEvent>::create(&divergent_journal).expect("create journal");
    writer
        .append(&JournalEvent::App(CounterEvent::Increment { amount: 7 }))
        .expect("append");
    drop(writer);
    let divergent = cluster.start_replica(2, "divergent");
    wait_for_gauge(
        primary_health,
        "melin_replica_divergence_total",
        1,
        STARTUP_LIMIT,
    );
    wait_for_gauge(primary_health, "melin_replicas_connected", 2, STARTUP_LIMIT);

    // One more event, past every join, and both replicas journal it: each
    // acks the primary's whole history.
    assert_acked(&mut conn, "with both replicas attached");
    let head = gauge(primary_health, "melin_journal_sequence").expect("the primary's sequence");
    assert!(head > HISTORY, "the primary's history ends at {head}");
    for slot in ["0", "1"] {
        wait_for_gauge(
            primary_health,
            &format!("melin_replica_acked_sequence{{slot=\"{slot}\"}}"),
            head,
            STARTUP_LIMIT,
        );
    }

    drop(conn);
    divergent.stop();
    fresh.stop();
    primary.stop();
}

/// A joining replica that stops reading in the middle of its handoff into
/// the live stream holds up nothing: the primary goes on answering its
/// clients promptly and streaming to its other replica, and drops the
/// stalled one once, having acked, it has refused the handoff's bytes for
/// the join stall limit (the replication liveness timeout). Until it acks,
/// it is spared in both phases of its join, however long it reads
/// nothing: a replica installing a snapshot reads nothing for as long as
/// its state takes to load, and acks only once it is streaming.
///
/// DPDK only: the handoff runs on the thread that is also client ingress,
/// and when it waited there for room in the joiner's socket, a joiner alive
/// but not reading (its stack acknowledging with a zero window, so its
/// liveness deadline never fires) held every client with no bound.
///
/// The joiner is a raw replica in the test, which authenticates and
/// handshakes as a fresh replica and then reads only what the test says,
/// so the stall needs no hook in the node. The handoff is the part of a
/// join that starts once every catch-up frame is queued on the joiner's
/// socket, and has data to send only when entries were journaled during
/// the catch-up, so the test arranges both:
///
/// 1. The primary's history is sized to the primary's buffers for the
///    replication socket ([`JOIN_HISTORY_WIRE_BYTES`]): with the joiner
///    reading nothing, the join worker reads all of it, but its last frames
///    find no room in the socket, so the catch-up is not over.
/// 2. Clients then write a few MiB (the joiner's slot is still catching
///    up, so its ring is inactive: nothing of this reaches it but the
///    disk).
/// 3. The joiner reads exactly the history, and stops. The catch-up ends,
///    the handoff starts, and its first journal pass carries those MiB:
///    far more than the socket holds, with nobody reading.
/// 4. The joiner acks what it read, and still reads nothing.
///
/// Through steps 1 to 3 the joiner, unacked, outlives the stall limit
/// first in its catch-up, then in its handoff; after step 4 it is dropped.
///
/// The steps check their own premises, so a primary whose buffers change
/// fails here with the reason rather than passing without a stall.
#[test]
fn a_joiner_that_stops_reading_mid_handoff_holds_up_nothing() {
    let _serial = serialise();
    let cluster = Cluster::new();
    let primary_health = Cluster::health(0);

    let entry_wire_bytes = write_history(
        &cluster.dir.path().join("primary.journal"),
        JOIN_HISTORY_WIRE_BYTES,
    );
    let primary = cluster.start_primary();
    let streaming = cluster.start_replica(1, "streaming");
    wait_for_gauge(primary_health, "melin_replicas_connected", 1, STARTUP_LIMIT);
    let mut conn = served_within(primary.addr(), STARTUP_LIMIT, "with one replica attached");
    let history_end =
        gauge(primary_health, "melin_journal_sequence").expect("the primary's sequence");

    // 1. The joiner handshakes, then reads nothing. Its slot is the
    // second: the streaming replica's is 0.
    let stall_limit = melin_dpdk::PeerLiveness::REPLICATION.timeout();
    let joined = Instant::now();
    let mut joiner = RawReplica::join(Cluster::addrs(0).replication(), &Cluster::node_key(2));
    wait_for_gauge(primary_health, "melin_replicas_connected", 2, STARTUP_LIMIT);
    // Time for the worker to read the history to its end.
    std::thread::sleep(Duration::from_secs(1));

    // 2. Entries the handoff will have to send: written while the joiner
    // is still catching up.
    let residual_entries = (JOIN_RESIDUAL_WIRE_BYTES / entry_wire_bytes) as u64;
    let flood = Flood::start(primary.addr(), 4);
    let deadline = Instant::now() + Duration::from_secs(60);
    while gauge(primary_health, "melin_journal_sequence").unwrap_or(0)
        < history_end + residual_entries
    {
        assert!(
            Instant::now() < deadline,
            "clients wrote too slowly to build the handoff's backlog"
        );
        assert_eq!(
            gauge(primary_health, "melin_replica_catching_up{slot=\"1\"}"),
            Some(1),
            "the joiner's catch-up ended while it was reading nothing: the history no longer \
             exceeds what the primary queues for a replica (JOIN_HISTORY_WIRE_BYTES)"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    flood.stop();
    conn.set_read_timeout(SERVED_DURING_STALL)
        .expect("set the client's read timeout");

    // A joiner that has not acked is never stalled, however long it reads
    // nothing: it may be installing a snapshot. Its catch-up has been
    // refused since step 1; it outlives the stall limit.
    serve_sparing_the_joiner(
        &mut conn,
        primary_health,
        joined + stall_limit + SPARED_MARGIN,
        "while the joiner, unacked, reads none of its catch-up",
    );

    // 3. The joiner reads the history, and stops reading for good.
    let caught_up_to = joiner.read_through(history_end);
    assert_eq!(
        caught_up_to, history_end,
        "the catch-up went on past the history into the clients' writes: the join worker \
         had not reached the history's end before they began (JOIN_HISTORY_WIRE_BYTES too \
         large for the primary's queue)"
    );

    // Still unacked, its handoff refused: spared again.
    serve_sparing_the_joiner(
        &mut conn,
        primary_health,
        Instant::now() + stall_limit + SPARED_MARGIN,
        "while the joiner, unacked, reads none of its handoff",
    );

    // 4. It acks what it read, as a replica past its install does, and
    // still reads nothing. The primary serves its clients throughout, and
    // drops the joiner within the stall limit, plus a margin for a slow
    // runner.
    joiner.ack(history_end);
    let stopped = Instant::now();
    let bound = stall_limit + FRAME_LIMIT;
    let mut served_while_stalled = 0;
    while gauge(primary_health, "melin_replicas_connected") != Some(1) {
        assert!(
            stopped.elapsed() < bound,
            "the joiner that stopped reading was not dropped within {bound:?}"
        );
        let asked = Instant::now();
        assert_acked(&mut conn, "while the joiner is stalled in its handoff");
        assert!(
            asked.elapsed() < SERVED_DURING_STALL,
            "a request took {:?} while the joiner was stalled",
            asked.elapsed()
        );
        served_while_stalled += 1;
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        served_while_stalled > 3,
        "the joiner was dropped after {served_while_stalled} requests, too soon to be its stall"
    );
    assert!(
        stopped.elapsed() >= melin_dpdk::PeerLiveness::REPLICATION.timeout() / 2,
        "the joiner was dropped after {:?}, before it could have been stalled for the limit",
        stopped.elapsed()
    );
    assert_eq!(
        gauge(primary_health, "melin_replica_evictions_total"),
        Some(0),
        "dropped by the stall limit, not by a full replication ring"
    );

    // The other replica streamed throughout, and is current.
    assert_acked(&mut conn, "after the joiner was dropped");
    let head = gauge(primary_health, "melin_journal_sequence").expect("the primary's sequence");
    wait_for_gauge(
        primary_health,
        "melin_replica_acked_sequence{slot=\"0\"}",
        head,
        STARTUP_LIMIT,
    );

    drop(joiner);
    drop(conn);
    streaming.stop();
    primary.stop();
}

/// How far past the stall limit a joiner that has not acked is watched for
/// being dropped: enough that a deadline counted from before its ack would
/// have fired, with a margin for a slow runner.
const SPARED_MARGIN: Duration = Duration::from_secs(2);

/// Ask the primary for an answer every 200 ms until `until`, each answered
/// promptly, with the joiner of
/// `a_joiner_that_stops_reading_mid_handoff_holds_up_nothing` connected,
/// and still catching up, throughout.
fn serve_sparing_the_joiner(
    conn: &mut Connection,
    primary_health: SocketAddr,
    until: Instant,
    when: &str,
) {
    while Instant::now() < until {
        assert_eq!(
            gauge(primary_health, "melin_replicas_connected"),
            Some(2),
            "the joiner was dropped {when}"
        );
        assert_eq!(
            gauge(primary_health, "melin_replica_catching_up{slot=\"1\"}"),
            Some(1),
            "the joiner's join ended {when}"
        );
        let asked = Instant::now();
        assert_acked(conn, when);
        assert!(
            asked.elapsed() < SERVED_DURING_STALL,
            "a request took {:?} {when}",
            asked.elapsed()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// A joining replica that acks part of its catch-up and then stops
/// reading is dropped once it has refused the catch-up for the join stall
/// limit, while the primary goes on answering its clients and streaming to
/// its other replica. The catch-up runs with the joiner's replication ring
/// inactive, so no full ring would ever evict it: without the limit it
/// would hold its slot for as long as it stayed connected.
///
/// The history is several times what the primary queues for a replica,
/// so the catch-up cannot end with the joiner reading only its start.
#[test]
fn a_joiner_that_acks_then_stops_reading_its_catch_up_is_dropped() {
    let _serial = serialise();
    let cluster = Cluster::new();
    let primary_health = Cluster::health(0);

    let journal = cluster.dir.path().join("primary.journal");
    append_increments(&journal, 200_000);
    let history = catch_up_bytes(&journal);
    assert!(
        history > 2 * JOIN_HISTORY_WIRE_BYTES,
        "the history's catch-up is {history} bytes, too little to outlast what the primary \
         queues for a replica"
    );
    let primary = cluster.start_primary();
    let streaming = cluster.start_replica(1, "streaming");
    wait_for_gauge(primary_health, "melin_replicas_connected", 1, STARTUP_LIMIT);
    let mut conn = served_within(primary.addr(), STARTUP_LIMIT, "with one replica attached");

    // The joiner reads the catch-up's first entries, acks them as a
    // replica that has journaled them does, and stops reading.
    let mut joiner = RawReplica::join(Cluster::addrs(0).replication(), &Cluster::node_key(2));
    wait_for_gauge(primary_health, "melin_replicas_connected", 2, STARTUP_LIMIT);
    let read = joiner.read_through(1);
    joiner.ack(read);
    let stopped = Instant::now();

    conn.set_read_timeout(SERVED_DURING_STALL)
        .expect("set the client's read timeout");
    let stall_limit = melin_dpdk::PeerLiveness::REPLICATION.timeout();
    let bound = stall_limit + FRAME_LIMIT;
    let mut served_while_stalled = 0;
    while gauge(primary_health, "melin_replicas_connected") != Some(1) {
        assert!(
            stopped.elapsed() < bound,
            "the joiner that stopped reading was not dropped within {bound:?}"
        );
        if gauge(primary_health, "melin_replica_catching_up{slot=\"1\"}") != Some(1) {
            // Either the drop, which clears this gauge and the count in
            // turn, landed between the two reads, or the catch-up ended.
            let settle = Instant::now() + Duration::from_secs(1);
            while gauge(primary_health, "melin_replicas_connected") != Some(1) {
                assert!(
                    Instant::now() < settle,
                    "the joiner's catch-up ended while it was reading nothing: the history \
                     no longer exceeds what the primary queues for a replica"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            break;
        }
        let asked = Instant::now();
        assert_acked(&mut conn, "while the joiner is stalled in its catch-up");
        assert!(
            asked.elapsed() < SERVED_DURING_STALL,
            "a request took {:?} while the joiner was stalled",
            asked.elapsed()
        );
        served_while_stalled += 1;
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        stopped.elapsed() >= stall_limit / 2,
        "the joiner was dropped after {:?} ({served_while_stalled} requests), before it \
         could have been stalled for the limit",
        stopped.elapsed()
    );

    // The other replica streamed throughout, and is current.
    assert_acked(&mut conn, "after the joiner was dropped");
    let head = gauge(primary_health, "melin_journal_sequence").expect("the primary's sequence");
    wait_for_gauge(
        primary_health,
        "melin_replica_acked_sequence{slot=\"0\"}",
        head,
        STARTUP_LIMIT,
    );

    drop(joiner);
    drop(conn);
    streaming.stop();
    primary.stop();
}

/// How much catch-up the history of
/// `a_joiner_that_stops_reading_mid_handoff_holds_up_nothing` makes, on the
/// wire: more than the primary queues for a replica that reads nothing, so
/// the catch-up cannot end, and not so much more that the join worker
/// cannot read all of it.
///
/// What a replica reading nothing has queued for it is its own receive
/// buffer (the [`RawReplica`]'s, a few tens of KiB), the replication
/// socket's 512 KiB send buffer, and its 512 KiB transmit queue (`server.rs`):
/// a little over 1 MiB. Past that the join's frames wait in the worker's
/// hand-off (a held frame and a channel of four, `join_worker.rs`), of up
/// to 64 KiB each, so the worker reaches the end of a history of up to
/// about 1 MiB + 256 KiB. The figure is in the middle.
const JOIN_HISTORY_WIRE_BYTES: usize = 1_180 * 1024;

/// How much the clients write for the handoff to send: well over what the
/// primary queues for the joiner.
const JOIN_RESIDUAL_WIRE_BYTES: usize = 3 * 1024 * 1024;

/// Write a journal at `path` whose catch-up is about `wire_bytes` long.
/// Returns the catch-up bytes per entry.
fn write_history(path: &std::path::Path, wire_bytes: usize) -> usize {
    // One entry's share of the catch-up, measured on a scratch journal.
    let scratch = path.with_extension("scratch");
    const SAMPLE: usize = 1_000;
    append_increments(&scratch, SAMPLE);
    let per_entry = catch_up_bytes(&scratch).div_ceil(SAMPLE);
    std::fs::remove_file(&scratch).expect("remove the scratch journal");

    append_increments(path, wire_bytes / per_entry);
    let bytes = catch_up_bytes(path);
    assert!(
        bytes.abs_diff(wire_bytes) < 32 * 1024,
        "the history's catch-up is {bytes} bytes, not about {wire_bytes}"
    );
    per_entry
}

fn append_increments(path: &std::path::Path, count: usize) {
    let mut writer = BufferedWriter::<CounterEvent>::create(path).expect("create journal");
    for _ in 0..count {
        writer
            .append(&JournalEvent::App(CounterEvent::Increment { amount: 1 }))
            .expect("append");
    }
}

/// The bytes a fresh replica's journal catch-up of `path` puts on the wire.
fn catch_up_bytes(path: &std::path::Path) -> usize {
    use melin_transport_core::replication::catchup::catch_up_from_journal_with;
    let never = std::sync::atomic::AtomicBool::new(false);
    let mut bytes = 0;
    catch_up_from_journal_with::<CounterEvent>(
        path,
        0,
        &mut |frame: &[u8]| {
            bytes += frame.len();
            Ok(())
        },
        &never,
    )
    .expect("catch up from the journal");
    bytes
}

/// Clients writing as fast as the primary answers, until stopped.
struct Flood {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    writers: Vec<std::thread::JoinHandle<()>>,
}

impl Flood {
    fn start(node: SocketAddr, clients: usize) -> Self {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let writers = (0..clients)
            .map(|_| {
                let stop = std::sync::Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut conn = served_within(node, STARTUP_LIMIT, "a flooding client");
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        assert_acked(&mut conn, "flooding");
                    }
                })
            })
            .collect();
        Flood { stop, writers }
    }

    fn stop(self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for writer in self.writers {
            writer.join().expect("a flooding client failed");
        }
    }
}

/// A replica written by hand: it authenticates and handshakes as a fresh
/// replica, then reads the stream only when told to. A real replica reads
/// as fast as its journal allows; this one is what one that has stopped
/// reading looks like to its primary.
struct RawReplica {
    stream: TcpStream,
}

impl RawReplica {
    /// The receive buffer asked for: small, so that what the replica holds
    /// unread is a known, small part of what the primary has queued.
    const RECEIVE_BUFFER: libc::c_int = 32 * 1024;

    fn join(primary: SocketAddr, key: &SigningKey) -> Self {
        use ed25519_dalek::Signer;
        use melin_transport_core::replication::protocol::{
            Handshake as ReplicaHandshake, decode_auth_result, decode_challenge,
            encode_challenge_response, encode_handshake,
        };

        let mut stream = connect_with_receive_buffer(primary, Self::RECEIVE_BUFFER);
        stream
            .set_read_timeout(Some(FRAME_LIMIT))
            .expect("set read timeout");
        let challenge = read_any_frame(&mut stream).expect("the primary's challenge");
        let nonce = decode_challenge(&challenge).expect("a challenge");
        let mut out = Vec::new();
        encode_challenge_response(
            &key.sign(&nonce).to_bytes(),
            key.verifying_key().as_bytes(),
            &mut out,
        );
        stream.write_all(&out).expect("send the challenge response");
        let verdict = read_any_frame(&mut stream).expect("the primary's verdict");
        assert!(
            decode_auth_result(&verdict).expect("an auth result"),
            "the primary refused the replication key"
        );
        out.clear();
        encode_handshake(
            &ReplicaHandshake {
                last_sequence: 0,
                chain_hash: [0; 32],
                epoch: 0,
            },
            &mut out,
        );
        stream.write_all(&out).expect("send the handshake");
        RawReplica { stream }
    }

    /// Read the stream until an entry batch carries `sequence`, and return
    /// that batch's last sequence.
    fn read_through(&mut self, sequence: u64) -> u64 {
        use melin_transport_core::replication_wire::{MSG_INPUT_BATCH, try_decode_input_batch};
        loop {
            let frame = read_any_frame(&mut self.stream).expect("the catch-up");
            if frame.first() != Some(&MSG_INPUT_BATCH) {
                continue; // StreamStart, and any other control frame.
            }
            let slots = try_decode_input_batch::<CounterEvent>(&frame).expect("an entry batch");
            let last = slots.last().expect("a batch carries entries").sequence;
            if last >= sequence {
                return last;
            }
        }
    }

    /// Ack `sequence` as journaled, as a replica streaming does: the first
    /// sign the primary has that the replica is past its install.
    fn ack(&mut self, sequence: u64) {
        use melin_transport_core::replication::protocol::{Ack, encode_ack};
        let mut out = Vec::new();
        encode_ack(
            &Ack {
                acked_sequence: sequence,
                in_memory_sequence: sequence,
            },
            &mut out,
        );
        self.stream.write_all(&out).expect("send an ack");
    }
}

/// A kernel-TCP connection to `peer` whose receive buffer was set before
/// connecting, when it still bounds the window offered.
fn connect_with_receive_buffer(peer: SocketAddr, receive_buffer: libc::c_int) -> TcpStream {
    use std::os::fd::FromRawFd;
    let SocketAddr::V4(peer) = peer else {
        panic!("the runner's network is IPv4");
    };
    // SAFETY: a plain socket(2); its descriptor is owned by the `TcpStream`
    // at once, which closes it on every path out.
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    assert!(fd >= 0, "socket: {}", io::Error::last_os_error());
    // SAFETY: `fd` is a fresh socket that nothing else owns.
    let stream = unsafe { TcpStream::from_raw_fd(fd) };
    // SAFETY: the option value is a live `c_int`, of the length passed.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&receive_buffer as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "SO_RCVBUF: {}", io::Error::last_os_error());
    let address = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: peer.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(*peer.ip()).to_be(),
        },
        sin_zero: [0; 8],
    };
    // SAFETY: `address` is a live `sockaddr_in`, of the length passed.
    let rc = unsafe {
        libc::connect(
            fd,
            (&address as *const libc::sockaddr_in).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "connect to {peer}: {}", io::Error::last_os_error());
    stream
}

/// One length-prefixed frame's payload, of any size.
fn read_any_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix)?;
    let mut payload = vec![0u8; u32::from_le_bytes(prefix) as usize];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}

/// How long a request may take while another replica's join is stalled:
/// far above an answer's round trip, far below the stall.
const SERVED_DURING_STALL: Duration = Duration::from_secs(2);

/// A file that blocks whoever reads it, until dropped: a FIFO the test
/// holds open for writing (so a reader's open returns at once, and its
/// read waits for data) and never writes into. Dropping it removes the
/// FIFO and closes the write end, so a blocked reader sees the end of
/// the file.
struct StalledFile {
    path: std::path::PathBuf,
    /// Opened read-write, which on Linux never blocks on a FIFO and keeps
    /// a writer present.
    _held: std::fs::File,
}

impl StalledFile {
    fn create(path: &std::path::Path) -> Self {
        use std::os::unix::ffi::OsStrExt;
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("path has no NUL");
        // SAFETY: `c_path` is a valid NUL-terminated path for the call.
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(
            rc,
            0,
            "mkfifo {}: {}",
            path.display(),
            io::Error::last_os_error()
        );
        let held = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .expect("hold the FIFO open");
        StalledFile {
            path: path.to_owned(),
            _held: held,
        }
    }

    /// Whether anyone besides this holder has the FIFO open. The nodes
    /// run in this process, so their open files are in `/proc/self/fd`
    /// beside this holder's own.
    fn has_reader(&self) -> bool {
        let Ok(entries) = std::fs::read_dir("/proc/self/fd") else {
            return false;
        };
        entries
            .filter_map(Result::ok)
            .filter(|e| std::fs::read_link(e.path()).is_ok_and(|target| target == self.path))
            .count()
            > 1
    }
}

impl Drop for StalledFile {
    fn drop(&mut self) {
        // Deliberately ignored: the FIFO is in the test's temp dir, which
        // goes with the test.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// An increment on `conn` is acked.
fn assert_acked(conn: &mut Connection, when: &str) {
    let reply = conn
        .request_one(&increment_request(1))
        .unwrap_or_else(|e| panic!("{when}: the request was not answered: {e}"));
    assert_eq!(
        reply.first(),
        Some(&KIND_RESP_ACK),
        "{when}: increment acked"
    );
}

/// A primary on slot 0 and replicas on the other slots of the runner's
/// network, in a temp dir with the keys they share.
struct Cluster {
    dir: tempfile::TempDir,
    authorized_keys: std::path::PathBuf,
}

impl Cluster {
    /// The keys: the primary's and three replicas', and the test's writer.
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let authorized_keys = dir.path().join("authorized_keys");
        let mut lines = vec![key::authorized_keys_line(
            "writer",
            &writer_key().verifying_key(),
            "veth-test",
        )];
        for slot in 0..3 {
            lines.push(key::authorized_keys_line(
                "replication",
                &Self::node_key(slot).verifying_key(),
                &format!("node-{slot}"),
            ));
        }
        std::fs::write(&authorized_keys, lines.join("\n")).expect("write authorized_keys");
        Cluster {
            dir,
            authorized_keys,
        }
    }

    fn node_key(slot: usize) -> SigningKey {
        SigningKey::from_bytes(&[0xC1 + slot as u8; 32])
    }

    /// A node's health endpoint. On kernel TCP, on the loopback of the
    /// runner's network namespace: this process's own, so any port is
    /// free.
    fn health(slot: usize) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 9_101 + slot as u16))
    }

    fn addrs(slot: usize) -> melin_test_node::Addrs {
        melin_test_node::addrs(slot, || {
            unreachable!("on DPDK the slot gives the addresses")
        })
    }

    /// The configuration every node shares; journal `{name}.journal`.
    fn config(&self, slot: usize, name: &str) -> ServerConfig {
        let key_path = self.dir.path().join(format!("{name}.key"));
        std::fs::write(&key_path, Self::node_key(slot).to_bytes()).expect("write node key");
        ServerConfig {
            journal: self.dir.path().join(format!("{name}.journal")),
            authorized_keys: self.authorized_keys.clone(),
            ack_policy: AckPolicy::Disk,
            cores: PipelineCores::unpinned(),
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            health_bind: Some(Self::health(slot)),
            replication_key: Some(key_path),
            ..melin_test_node::config()
        }
    }

    /// The primary, on slot 0, journal `primary.journal`.
    fn start_primary(&self) -> melin_test_node::Node {
        let addrs = Self::addrs(0);
        let mut config = self.config(0, "primary");
        config.replication_bind = Some(addrs.replication());
        Self::start(&addrs, config)
    }

    /// A replica of the primary, on `slot`, journal `{name}.journal`.
    fn start_replica(&self, slot: usize, name: &str) -> melin_test_node::Node {
        let mut config = self.config(slot, name);
        config.replica_of = Some(Self::addrs(0).replication());
        Self::start(&Self::addrs(slot), config)
    }

    fn start(addrs: &melin_test_node::Addrs, config: ServerConfig) -> melin_test_node::Node {
        melin_test_node::start_at::<Counter>(
            addrs,
            config,
            StartupEvents::none(),
            (),
            RequestDecoder,
            ResponseEncoder,
        )
    }
}

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

/// Take this test's turn (see [`ONE_AT_A_TIME`]) and install the log
/// subscriber. Hold the guard for the whole test.
fn serialise() -> MutexGuard<'static, ()> {
    // A poisoned lock only means an earlier test failed; that one has
    // reported itself, and this one has nothing to inherit from it.
    let serial = ONE_AT_A_TIME
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
    serial
}

/// The value of one Prometheus gauge on a node's health endpoint, or
/// `None` while the endpoint is not up.
fn gauge(health: SocketAddr, name: &str) -> Option<u64> {
    let mut stream = TcpStream::connect_timeout(&health, Duration::from_millis(200)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut body = String::new();
    stream.read_to_string(&mut body).ok()?;
    body.lines()
        .find_map(|line| line.strip_prefix(name)?.trim().parse().ok())
}

/// Wait, at most `limit`, for gauge `name` to read `value`.
fn wait_for_gauge(health: SocketAddr, name: &str, value: u64, limit: Duration) {
    let deadline = Instant::now() + limit;
    while gauge(health, name) != Some(value) {
        assert!(
            Instant::now() < deadline,
            "{name} did not reach {value} within {limit:?} (last: {:?})",
            gauge(health, name)
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Run `body` against a fresh counter node on DPDK, then stop the node.
///
/// No cleanup on a panic: the test has failed, and its process exiting
/// takes the node, the namespaces and the tmpfs with it.
fn with_node(body: fn(SocketAddr)) {
    let _serial = serialise();
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
        // Unpinned: the node shares the host with the test's client, and
        // a CI runner with everything else.
        cores: PipelineCores::unpinned(),
        snapshot_interval_ms: 0,
        health_bind: None,
        // One slot, so a test can tell whether a closed connection's slot
        // came back: the next client gets in only if it did.
        max_connections: 1,
        heartbeat_interval_secs: HEARTBEAT.as_secs(),
        ..melin_test_node::config()
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
