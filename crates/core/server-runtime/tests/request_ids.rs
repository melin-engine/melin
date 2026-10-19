//! Every request gets exactly one final frame naming it, end to end on a
//! standalone primary: a request the application's decoder refuses is
//! answered with a `Refused` frame carrying its kind and reason, and one
//! it filters with an empty batch, so a pipelining client pairs every
//! reply with its request by ID. Only the accepted requests' replies are
//! pinned to request order: a refused or filtered request may be answered
//! ahead of a write before it that is still held for durability.
//!
//! These used to go unanswered: the node dropped the request, and every
//! later reply on a pipelining connection was paired with the request
//! before it (S1 of `docs/internal/application-api-review-2026-09.md`).
//!
//! A request that finds the input ring full is answered `ServerBusy`, as
//! is every request buffered behind it, and the connection is kept. The
//! requests behind it used to wait unread for bytes a client waiting on
//! its replies never sends ("Pipelined writes after a full input ring are
//! never parsed" in `docs/internal/lost-signals-2026-10.md`).
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use counter_server::{
    Counter, CounterEvent, CounterQuery, CounterReport, CounterRole, GET_VALUE_REQUEST,
    KIND_RESP_ACK, KIND_RESP_VALUE, RequestDecoder, ResponseEncoder, increment_request,
};
use melin_app::auth::ClientRole;
use melin_app::decoder::Decoded;
use melin_app::{Application, ApplyCtx, QueryCtx, RejectReason};
use melin_client::{Connection, Frame, RefusalKind, RequestId, SigningKey, StreamConnection, key};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::probe::wait_for_gauge;
use melin_transport_core::pipeline::INPUT_RING_CAPACITY;
use melin_transport_core::test_ports::free_addr;

/// The writer's key, and the reader's.
const WRITER: [u8; 32] = [0xA1; 32];
const READER: [u8; 32] = [0xA2; 32];

/// The body the test's decoder filters: a counter decoder for everything
/// else.
const FILTERED: [u8; 1] = [0x7F];

/// A body the counter's decoder does not know.
const MALFORMED: [u8; 1] = [0x55];

/// The counter's decoder, with [`FILTERED`] filtered: an application
/// message that is not an event, as a subscription request is on a
/// connection that only submits events.
struct FilteringDecoder;

impl melin_app::decoder::RequestDecoder for FilteringDecoder {
    type Event = CounterEvent;
    type Role = CounterRole;

    fn decode(&self, body: &[u8], role: ClientRole<CounterRole>) -> Decoded<CounterEvent> {
        if body == FILTERED {
            return Decoded::Filter;
        }
        RequestDecoder.decode(body, role)
    }
}

/// The counter, whose `apply` waits for the test's leave: the test holds
/// the matching stage at the first write, and the input ring fills behind
/// it. Same events and codecs as the counter.
///
/// The leave is a count of applies rather than a switch, so the test can
/// let the stage go a window at a time as it reads the replies: let go at
/// once, a full ring's replies leave faster than one client thread reads
/// them, and the node drops it as a reader that has fallen behind.
#[derive(Default)]
struct Latched {
    counter: Counter,
    /// How many writes may be applied. Handed over as the node's sizing,
    /// the one thing the runtime passes an instance from the test
    /// (`prefault`); `None` until then. `u64`, as a count of events.
    leave: Option<Arc<AtomicU64>>,
    /// Writes applied so far.
    applied: u64,
}

impl Application for Latched {
    type Event = CounterEvent;
    type Report = CounterReport;
    type QueryResponse = CounterQuery;
    type Sizing = Arc<AtomicU64>;
    const APP_VERSION: u16 = Counter::APP_VERSION;

    fn apply(&mut self, event: Self::Event, ctx: &ApplyCtx, out: &mut Vec<Self::Report>) {
        while self
            .leave
            .as_ref()
            .is_some_and(|leave| self.applied >= leave.load(Ordering::Acquire))
        {
            std::thread::sleep(Duration::from_micros(50));
        }
        self.applied += 1;
        self.counter.apply(event, ctx, out)
    }

    fn query(&self, event: Self::Event, ctx: &QueryCtx) -> Option<Self::QueryResponse> {
        self.counter.query(event, ctx)
    }

    fn tick(&mut self, now_ns: u64, out: &mut Vec<Self::Report>) {
        self.counter.tick(now_ns, out)
    }

    fn build_reject(event: &Self::Event, reason: RejectReason) -> Self::Report {
        Counter::build_reject(event, reason)
    }

    fn snapshot<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        self.counter.snapshot(w)
    }

    fn restore<R: std::io::Read>(r: &mut R) -> std::io::Result<Self> {
        Counter::restore(r).map(|counter| Latched {
            counter,
            ..Latched::default()
        })
    }

    fn prefault(&mut self, leave: &Self::Sizing) {
        self.leave = Some(Arc::clone(leave));
    }
}

/// The authorized-keys file on `dir`, with a writer and a reader key.
fn authorized_keys(dir: &Path) -> std::path::PathBuf {
    let auth_path = dir.join("authorized_keys");
    let line = |role: &str, secret: [u8; 32]| {
        key::authorized_keys_line(role, &SigningKey::from_bytes(&secret).verifying_key(), role)
    };
    std::fs::write(
        &auth_path,
        format!("{}\n{}\n", line("writer", WRITER), line("reader", READER)),
    )
    .expect("write auth keys");
    auth_path
}

/// Port range for the health endpoint's `free_addr`, shared with the
/// other cluster tests (`halt_cluster.rs`): safe only because this binary
/// is in nextest's `cluster-serial` group with them.
const PORT_BASE: u16 = 10_000;

/// A standalone node's configuration on `dir`, its journal named `name`,
/// with the health endpoint the tests read its counters through.
fn config(dir: &Path, name: &str) -> ServerConfig {
    ServerConfig {
        journal: dir.join(name),
        authorized_keys: authorized_keys(dir),
        standalone: true,
        ack_policy: melin_server_runtime::ack_policy::AckPolicy::Disk,
        // Unpinned, and therefore yielding: see `startup_events.rs`.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: Some(free_addr(PORT_BASE)),
        ..melin_test_node::config()
    }
}

/// A standalone counter node on `dir`, with a writer and a reader key,
/// and its health endpoint.
fn start(dir: &Path) -> (melin_test_node::Node, SocketAddr) {
    let config = config(dir, "counter.journal");
    let health = config.health_bind.expect("set by `config`");
    let node = melin_test_node::start::<Counter>(
        config,
        StartupEvents {
            genesis: Vec::new(),
            on_primary: Vec::new(),
        },
        (),
        FilteringDecoder,
        ResponseEncoder,
    );
    (node, health)
}

/// A standalone node of the latched counter, applying as many writes as
/// `leave` says, and its health endpoint.
fn start_latched(dir: &Path, leave: &Arc<AtomicU64>) -> (melin_test_node::Node, SocketAddr) {
    let config = config(dir, "latched.journal");
    let health = config.health_bind.expect("set by `config`");
    let node = melin_test_node::start::<Latched>(
        config,
        StartupEvents {
            genesis: Vec::new(),
            on_primary: Vec::new(),
        },
        Arc::clone(leave),
        RequestDecoder,
        ResponseEncoder,
    );
    (node, health)
}

/// The counters a node's health endpoint reports for the requests it
/// answered without applying, and the connections it parked, in the
/// order: shed, refused as permission denied, refused as malformed,
/// filtered, parked now.
const EARLY_ANSWER_COUNTERS: [&str; 5] = [
    "melin_requests_shed_total",
    "melin_requests_refused_total{kind=\"permission_denied\"}",
    "melin_requests_refused_total{kind=\"malformed\"}",
    "melin_requests_filtered_total",
    "melin_connections_parked",
];

/// Wait until the node's early-answer counters read `expected`, in
/// [`EARLY_ANSWER_COUNTERS`]' order. Each is counted when its answer is
/// queued, which may trail the answer the client has read.
fn wait_for_counters(health: SocketAddr, expected: [u64; 5]) {
    for (name, value) in EARLY_ANSWER_COUNTERS.into_iter().zip(expected) {
        wait_for_gauge(health, name, value);
    }
}

/// What a request was answered with: the application frames of its batch
/// and the final frame, rendered so a whole exchange compares at once.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// A batch: its frames, as their first byte (the counter's response
    /// kind) and value.
    Batch(Vec<(u8, u64)>),
    Refused(RefusalKind, String),
    Busy,
}

/// Check that `answers` names every ID in `sent` exactly once, each with
/// the answer `expected` gives at its position, and that the accepted
/// requests (those answered with frames) were answered in request order.
/// A refused or filtered request's answer may come at any point.
fn assert_answered(sent: &[RequestId], answers: &[(RequestId, Answer)], expected: Vec<Answer>) {
    let wanted: Vec<(RequestId, Answer)> = sent.iter().copied().zip(expected).collect();
    fn by_id(list: &[(RequestId, Answer)]) -> Vec<&(RequestId, Answer)> {
        let mut list: Vec<_> = list.iter().collect();
        list.sort_by_key(|(id, _)| *id);
        list
    }
    assert_eq!(
        by_id(answers),
        by_id(&wanted),
        "every request answered once, naming it"
    );
    let accepted = |list: &[(RequestId, Answer)]| -> Vec<RequestId> {
        list.iter()
            .filter(|(_, answer)| matches!(answer, Answer::Batch(frames) if !frames.is_empty()))
            .map(|(id, _)| *id)
            .collect()
    };
    assert_eq!(
        accepted(answers),
        accepted(&wanted),
        "the accepted requests answered in request order"
    );
}

/// Send every body pipelined, then read every answer, pairing each final
/// frame with the request it names. Returns the answers in the order they
/// came, each with its request's ID, and the IDs sent, in order.
fn pipelined(
    conn: &mut Connection,
    bodies: &[&[u8]],
) -> (Vec<RequestId>, Vec<(RequestId, Answer)>) {
    let sent: Vec<RequestId> = bodies
        .iter()
        .map(|body| conn.send(body).expect("send"))
        .collect();
    let mut answers = Vec::new();
    let mut frames = Vec::new();
    while answers.len() < bodies.len() {
        match conn.next_frame().expect("every request is answered") {
            Frame::Response(body) => frames.push((
                body[0],
                u64::from_le_bytes(body[1..9].try_into().expect("an 8-byte value")),
            )),
            Frame::BatchEnd { id, .. } => {
                answers.push((id, Answer::Batch(std::mem::take(&mut frames))));
            }
            Frame::Refused { id, kind, reason } => {
                assert!(frames.is_empty(), "a refusal inside a batch");
                answers.push((id, Answer::Refused(kind, reason.to_string())));
            }
            Frame::ServerBusy { id } => answers.push((id, Answer::Busy)),
        }
    }
    (sent, answers)
}

/// S1: a pipelined client whose requests include a refused and a
/// malformed one, and a filtered one, gets every request answered once,
/// each answer naming its request; nothing refused or filtered is
/// applied; and the connection serves the next request.
#[test]
fn every_pipelined_request_is_answered_and_paired_by_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (node, health) = start(dir.path());

    let mut writer = melin_test_node::connect(node.addr(), &SigningKey::from_bytes(&WRITER));
    writer
        .set_read_timeout(Duration::from_secs(30))
        .expect("set the read timeout");
    let (sent, answers) = pipelined(
        &mut writer,
        &[
            &increment_request(1),
            &MALFORMED,
            &increment_request(2),
            &FILTERED,
            &increment_request(4),
            &GET_VALUE_REQUEST,
        ],
    );
    let malformed = Answer::Refused(RefusalKind::Malformed, "unknown kind".into());
    let expected = vec![
        Answer::Batch(vec![(KIND_RESP_ACK, 1)]),
        malformed,
        Answer::Batch(vec![(KIND_RESP_ACK, 3)]),
        // Filtered: an empty batch.
        Answer::Batch(vec![]),
        Answer::Batch(vec![(KIND_RESP_ACK, 7)]),
        Answer::Batch(vec![(KIND_RESP_VALUE, 7)]),
    ];
    assert_answered(&sent, &answers, expected);

    // A reader's increment is refused as not permitted, with the
    // decoder's reason, among queries that are answered.
    let mut reader = melin_test_node::connect(node.addr(), &SigningKey::from_bytes(&READER));
    reader
        .set_read_timeout(Duration::from_secs(30))
        .expect("set the read timeout");
    let (sent, answers) = pipelined(
        &mut reader,
        &[
            &GET_VALUE_REQUEST,
            &increment_request(100),
            &GET_VALUE_REQUEST,
        ],
    );
    let denied = Answer::Refused(
        RefusalKind::PermissionDenied,
        "incrementing requires a writing role".into(),
    );
    let expected = vec![
        Answer::Batch(vec![(KIND_RESP_VALUE, 7)]),
        denied,
        Answer::Batch(vec![(KIND_RESP_VALUE, 7)]),
    ];
    assert_answered(&sent, &answers, expected);

    // Each connection numbered its own requests from 1, and both still
    // serve: the convenience calls report a refusal as their error.
    assert_eq!(sent[0], RequestId(1));
    let err = reader.request(&MALFORMED).unwrap_err();
    assert!(
        matches!(&err, melin_client::Error::Refused { kind: RefusalKind::Malformed, reason } if reason == "unknown kind"),
        "{err:?}"
    );
    assert_eq!(
        writer.request(&FILTERED).expect("an empty batch"),
        Vec::<Vec<u8>>::new()
    );
    let value = writer.request_one(&GET_VALUE_REQUEST).expect("the query");
    assert_eq!(value[0], KIND_RESP_VALUE);
    assert_eq!(
        u64::from_le_bytes(value[1..9].try_into().expect("8 bytes")),
        7
    );

    // Counted on the health endpoint, each once and as its kind: none
    // shed, one denied, two malformed, two filtered, nothing parked.
    wait_for_counters(health, [0, 1, 2, 2, 0]);

    drop((writer, reader));
    node.stop();
}

/// A full input ring sheds load and keeps the connection: one client fills
/// the ring of a node whose matching stage the test holds, and every
/// request past it is answered `ServerBusy { id }` at once (the ones
/// buffered behind a shed request with it), while the client is still
/// sending. Once the test lets the matching stage go, every request is
/// answered exactly once, applied or busy; no request the node shed is
/// followed by an applied one (the ring stayed full, so everything behind
/// a shed request was shed); and the connection serves the next request.
///
/// The client reads while it sends, and the stage is let go a window at
/// a time as the replies are read (see [`Latched`]), so an unread socket
/// never trips the node's send cap and the test measures shedding, not a
/// slow reader.
/// Nothing is answered until the ring is full, and the read deadline runs
/// while any request is owed, so it is long enough to cover filling the
/// ring on a slow host. The input ring is the node's fixed one, so more
/// requests than it holds are sent, and journaled.
#[test]
fn a_full_input_ring_answers_busy_and_keeps_the_connection() {
    /// Requests sent past the ring's capacity, all of which find it full.
    const PAST_THE_RING: usize = 20_000;
    /// How long the busy answers must stay still before the test takes
    /// the node to have read every request: the reader takes millions a
    /// second, so a pause this long means it has caught up.
    const SETTLED: Duration = Duration::from_secs(2);
    /// Writes the node may apply ahead of the replies read: a few tens of
    /// kilobytes of replies, well inside what a node buffers for a client.
    const WINDOW: u64 = 1024;

    // Opt-in diagnostics: with RUST_LOG set, the node says why it closed
    // a connection or shed a request. No-op when RUST_LOG is unset.
    melin_test_node::logs::capture_if_requested();
    let dir = tempfile::tempdir().expect("tempdir");
    let leave = Arc::new(AtomicU64::new(0));
    let (node, health) = start_latched(dir.path(), &leave);
    let key = SigningKey::from_bytes(&WRITER);
    // Up and serving: the handshake goes through.
    drop(melin_test_node::connect(node.addr(), &key));
    // Well past filling the ring, inside the suite's per-test limit.
    let mut conn = StreamConnection::connect_timeout(node.addr(), &key, Duration::from_secs(60))
        .expect("the client connects");

    let total = INPUT_RING_CAPACITY + PAST_THE_RING;
    let body = increment_request(1);
    let mut busy: Vec<RequestId> = Vec::new();
    let mut applied: Vec<RequestId> = Vec::new();
    // Every answer that has arrived, without waiting for more.
    fn read_answers(
        conn: &mut StreamConnection<melin_client::NonBlockingTcp>,
        busy: &mut Vec<RequestId>,
        applied: &mut Vec<RequestId>,
    ) {
        while let Some(frame) = conn.poll().expect("the connection stays up") {
            match frame {
                Frame::ServerBusy { id } => busy.push(id),
                Frame::BatchEnd { id, .. } => applied.push(id),
                Frame::Response(_) => {}
                Frame::Refused { id, kind, reason } => {
                    panic!("request {id:?} refused ({kind:?}: {reason})")
                }
            }
        }
    }
    let mut sent = Vec::with_capacity(total);
    for n in 0..total {
        sent.push(conn.send(&body).expect("send"));
        if n % 256 == 255 {
            read_answers(&mut conn, &mut busy, &mut applied);
        }
    }
    // Every request handed over, and read by the node: the busies stop.
    let mut last_change = (busy.len(), Instant::now());
    while conn.connection().unsent() > 0 || last_change.1.elapsed() < SETTLED {
        read_answers(&mut conn, &mut busy, &mut applied);
        if busy.len() != last_change.0 {
            last_change = (busy.len(), Instant::now());
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        applied.is_empty(),
        "nothing applied while the stage is held"
    );
    assert!(
        busy.len() >= PAST_THE_RING / 2,
        "the requests past the ring are shed: {} of {total}",
        busy.len()
    );

    while busy.len() + applied.len() < total {
        leave.store(applied.len() as u64 + WINDOW, Ordering::Release);
        read_answers(&mut conn, &mut busy, &mut applied);
        std::thread::yield_now();
    }
    // Leave for the next request.
    leave.store(u64::MAX, Ordering::Release);
    // Each ID once (the client refuses an answer for none, or a second
    // one), applied in request order, and every one shed after every one
    // applied.
    assert_eq!(busy.len() + applied.len(), total);
    assert!(applied.windows(2).all(|w| w[0] < w[1]), "in request order");
    let last_applied = *applied.last().expect("the ring's requests are applied");
    let first_busy = *busy.iter().min().expect("some are shed");
    assert!(
        last_applied < first_busy,
        "applied {last_applied:?} after shedding {first_busy:?}"
    );
    assert_eq!(*sent.last().expect("sent"), RequestId(total as u64));

    // The connection serves the next request, on a counter that applied
    // exactly the requests answered as applied.
    let ack = conn
        .request_one(&increment_request(5))
        .expect("the next request");
    assert_eq!(ack[0], KIND_RESP_ACK);
    assert_eq!(
        u64::from_le_bytes(ack[1..9].try_into().expect("8 bytes")),
        applied.len() as u64 + 5
    );

    // Every busy answer counted as shed, once; nothing refused, filtered
    // or parked now.
    wait_for_counters(health, [busy.len() as u64, 0, 0, 0, 0]);

    drop(conn);
    node.stop();
}
