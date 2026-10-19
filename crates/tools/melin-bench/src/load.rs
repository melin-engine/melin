//! The closed loop: one thread's clients, each keeping `window` requests
//! in flight, over whichever streams carry them. A run is one such loop
//! per thread (see [`crate::shard`]), all over the same measured window.

use std::collections::VecDeque;
use std::time::Duration;

use hdrhistogram::Histogram;
use melin_client::{Ack, ByteStream, Frame, PolledConnection, SigningKey};

use crate::app::{App, ClientId};
use crate::shard::{Shared, Stopped};

/// The streams a thread's connections ride on, one per client, and the
/// clock the loop reads: kernel sockets, or a DPDK NIC's streams.
pub trait Streams {
    /// One client's stream, lent for a call.
    type Stream<'a>: ByteStream
    where
        Self: 'a;

    /// Monotonic nanoseconds, on the origin the streams' own timers use,
    /// and on the same origin for every thread of a run: the run's start
    /// is read on one thread's clock and compared on all of them.
    fn now_ns(&self) -> u64;

    /// Advance every stream once: a user-space stack's step; nothing for
    /// kernel sockets.
    fn service(&mut self, now_ns: u64);

    /// The stream of this thread's client `client`.
    fn stream(&mut self, client: usize) -> Self::Stream<'_>;

    /// Put on the wire what client `client` sent since the last call, as
    /// much as the stream takes now, the rest held for the next call: the
    /// end of its sends for this pass. Nothing for a stream that already
    /// sends from a buffer of its own on [`service`](Self::service).
    fn end_sends(&mut self, client: usize) -> std::io::Result<()>;
}

/// Streams lent are the streams: so a caller keeps its own and reads them
/// once the run is over.
impl<S: Streams> Streams for &mut S {
    type Stream<'a>
        = S::Stream<'a>
    where
        Self: 'a;

    fn now_ns(&self) -> u64 {
        (**self).now_ns()
    }

    fn service(&mut self, now_ns: u64) {
        (**self).service(now_ns);
    }

    fn stream(&mut self, client: usize) -> S::Stream<'_> {
        (**self).stream(client)
    }

    fn end_sends(&mut self, client: usize) -> std::io::Result<()> {
        (**self).end_sends(client)
    }
}

/// How long a run loads the node, and how wide.
#[derive(Clone, Copy, Debug)]
pub struct Plan {
    pub window: usize,
    pub warmup: Duration,
    pub duration: Duration,
    pub cooldown: Duration,
    /// How long the connections may take to authenticate, and the
    /// in-flight requests to drain at the end.
    pub timeout: Duration,
}

/// What a run, or one thread of it, measured.
#[derive(Debug)]
pub struct Outcome {
    /// Round trips in nanoseconds, of the replies that arrived inside the
    /// measured phase.
    pub latency: Histogram<u64>,
    /// The measured phase, as it ran: the same window on every thread.
    pub measured: Duration,
    /// From the run's start to the last reply.
    pub wall: Duration,
    /// Every request answered, timed or not: what the node has
    /// sequenced for this run.
    pub completed: u64,
}

/// A run's outcome from its threads': every reply timed and every
/// request answered by any of them, to the last thread's last reply.
pub fn merge(outcomes: Vec<Outcome>) -> Result<Outcome, String> {
    let mut outcomes = outcomes.into_iter();
    let mut merged = outcomes.next().ok_or("a run with no load thread")?;
    for outcome in outcomes {
        merged
            .latency
            .add(&outcome.latency)
            .map_err(|e| format!("histogram: {e}"))?;
        merged.completed += outcome.completed;
        merged.wall = merged.wall.max(outcome.wall);
    }
    Ok(merged)
}

/// One client: its connection and its requests in flight.
struct Client {
    id: ClientId,
    connection: PolledConnection,
    /// When each request still owed its reply was sent, oldest first: a
    /// connection's replies come in the order of its requests.
    sent_ns: VecDeque<u64>,
    /// Requests whose reply has come but whose batch has not ended: each
    /// still holds its place in the window.
    replied: usize,
    /// The number of the next request.
    next_seq: u64,
}

impl Client {
    fn in_flight(&self) -> usize {
        self.sent_ns.len() + self.replied
    }
}

/// Why a thread stopped short.
enum Stop {
    /// It failed: the run's error, unless another thread's came first.
    Failed(String),
    /// Another thread failed.
    Aborted(Stopped),
}

impl From<String> for Stop {
    fn from(error: String) -> Self {
        Self::Failed(error)
    }
}

/// One thread's share of a run: open a stream for each client `ids`
/// names with `connect`, authenticate them, wait for every other thread,
/// load the node until the plan has run, then wait for the requests in
/// flight. A failure is recorded in `shared` as the run's, and stops the
/// other threads.
pub fn shard<S: Streams>(
    connect: impl FnOnce(&[ClientId]) -> Result<S, String>,
    ids: Vec<ClientId>,
    key: &SigningKey,
    app: App,
    plan: Plan,
    shared: &Shared,
) -> Result<Outcome, Stopped> {
    match load(connect, ids, key, app, plan, shared) {
        Ok(outcome) => Ok(outcome),
        Err(Stop::Failed(error)) => Err(shared.fail(error)),
        Err(Stop::Aborted(stopped)) => Err(stopped),
    }
}

fn load<S: Streams>(
    connect: impl FnOnce(&[ClientId]) -> Result<S, String>,
    ids: Vec<ClientId>,
    key: &SigningKey,
    app: App,
    plan: Plan,
    shared: &Shared,
) -> Result<Outcome, Stop> {
    let mut streams = connect(&ids)?;
    let mut clients: Vec<Client> = ids
        .into_iter()
        .map(|id| Client {
            id,
            connection: PolledConnection::new(key),
            sent_ns: VecDeque::with_capacity(plan.window),
            replied: 0,
            next_seq: 1,
        })
        .collect();
    authenticate(&mut streams, &mut clients, plan.timeout, shared)?;
    let start_ns = shared.meet(|| streams.now_ns()).map_err(Stop::Aborted)?;
    timed(&mut streams, &mut clients, app, plan, start_ns, shared)
}

/// The load itself, from the run's start at `start_ns`.
fn timed<S: Streams>(
    streams: &mut S,
    clients: &mut [Client],
    app: App,
    plan: Plan,
    start_ns: u64,
    shared: &Shared,
) -> Result<Outcome, Stop> {
    // Three significant figures, from a nanosecond to an hour: the
    // resolution a round trip has, in a fixed few tens of kilobytes.
    let mut latency = Histogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3)
        .map_err(|e| format!("histogram: {e}"))?;
    let measure_from = start_ns + nanos(plan.warmup);
    let measure_until = measure_from + nanos(plan.duration);
    let stop_sending = measure_until + nanos(plan.cooldown);
    let mut drain_deadline = None;
    // `u64`: a count of requests, which no run comes near overflowing.
    let mut completed: u64 = 0;

    loop {
        if let Some(stopped) = shared.stopped() {
            return Err(Stop::Aborted(stopped));
        }
        let now_ns = streams.now_ns();
        streams.service(now_ns);
        let sending = now_ns < stop_sending;
        if !sending && drain_deadline.is_none() {
            drain_deadline = Some(now_ns + nanos(plan.timeout));
        }
        let mut in_flight = 0;
        for (index, client) in clients.iter_mut().enumerate() {
            let id = client.id;
            while sending && client.in_flight() < plan.window {
                let seq = client.next_seq;
                client
                    .connection
                    .send_with(&mut streams.stream(index), now_ns, app.max_body(), |out| {
                        Ok::<_, melin_client::Error>(app.encode(out, id, seq))
                    })
                    .map_err(|e| format!("client {id}: {e}"))?;
                client.sent_ns.push_back(now_ns);
                client.next_seq += 1;
            }
            // The pass's requests, in one write.
            streams
                .end_sends(index)
                .map_err(|e| format!("client {id}: {e}"))?;
            loop {
                let frame = client
                    .connection
                    .poll(&mut streams.stream(index), now_ns)
                    .map_err(|e| format!("client {id}: {e}"))?;
                let Some(frame) = frame else { break };
                match frame {
                    Frame::Response(reply) => {
                        let Some(sent) = client.sent_ns.pop_front() else {
                            return Err(format!("client {id}: a reply to no request").into());
                        };
                        let seq = client.next_seq - client.sent_ns.len() as u64 - 1;
                        app.check(reply, id, seq)?;
                        client.replied += 1;
                        completed += 1;
                        let received_ns = streams.now_ns();
                        if (measure_from..measure_until).contains(&received_ns) {
                            latency
                                .record(received_ns.saturating_sub(sent).max(1))
                                .map_err(|e| format!("histogram: {e}"))?;
                        }
                    }
                    Frame::BatchEnd(Ack::Policy) => {
                        client.replied = client.replied.checked_sub(1).ok_or_else(|| {
                            format!("client {id}: a batch ended with no reply in it")
                        })?;
                    }
                    // Not the sequencer's latency: the primary lost its
                    // replica mid-run and answered on its own disk.
                    Frame::BatchEnd(Ack::PrimaryOnly) => {
                        return Err(format!(
                            "client {id}: a reply backed by the primary's disk alone \
                             (the primary lost its replica mid-run)"
                        )
                        .into());
                    }
                    Frame::ServerBusy => {
                        return Err(format!("client {id}: the server is busy").into());
                    }
                    Frame::EngineError => {
                        return Err(format!("client {id}: the application failed a request").into());
                    }
                }
            }
            // Whatever the polls queued: bytes a full buffer refused
            // earlier, which a poll hands the stream again. Nothing to
            // write in the usual pass.
            streams
                .end_sends(index)
                .map_err(|e| format!("client {id}: {e}"))?;
            in_flight += client.in_flight();
        }
        if let Some(deadline) = drain_deadline {
            if in_flight == 0 {
                let end_ns = streams.now_ns();
                return Ok(Outcome {
                    latency,
                    measured: plan.duration,
                    wall: Duration::from_nanos(end_ns.saturating_sub(start_ns)),
                    completed,
                });
            }
            if now_ns > deadline {
                return Err(format!(
                    "{in_flight} requests still owed their reply {:?} after the load stopped",
                    plan.timeout
                )
                .into());
            }
        }
    }
}

/// Drive every connection's handshake until all are ready.
fn authenticate<S: Streams>(
    streams: &mut S,
    clients: &mut [Client],
    timeout: Duration,
    shared: &Shared,
) -> Result<(), Stop> {
    let deadline = streams.now_ns() + nanos(timeout);
    loop {
        if let Some(stopped) = shared.stopped() {
            return Err(Stop::Aborted(stopped));
        }
        let now_ns = streams.now_ns();
        streams.service(now_ns);
        let mut waiting = None;
        let mut ready = 0;
        for (index, client) in clients.iter_mut().enumerate() {
            let id = client.id;
            if let Some(frame) = client
                .connection
                .poll(&mut streams.stream(index), now_ns)
                .map_err(|e| format!("client {id}: {e}"))?
            {
                return Err(format!("client {id}: {frame:?} before any request").into());
            }
            // The challenge response a poll queued.
            streams
                .end_sends(index)
                .map_err(|e| format!("client {id}: {e}"))?;
            if client.connection.is_ready() {
                ready += 1;
            } else {
                waiting.get_or_insert(id);
            }
        }
        let Some(waiting) = waiting else {
            return Ok(());
        };
        if now_ns > deadline {
            return Err(format!(
                "{} of this load thread's {} connections not authenticated within {timeout:?} \
                 (client {waiting} among them)",
                clients.len() - ready,
                clients.len()
            )
            .into());
        }
    }
}

/// `d` in nanoseconds, saturating: a run is never centuries long.
pub fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::app::AppKind;
    use crate::batched::Batched;
    use crate::batched::testing::Counting;
    use crate::shard::{self, Shards};
    use melin_client::framing::{PREFIX_LEN, split_frame};
    use melin_wire_protocol::control::TransportResponse;
    use melin_wire_protocol::control_codec::{TAG_APP, TAG_LEN, encode_transport_response};

    /// How a played node treats its clients.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Mode {
        /// Authenticates them and echoes every request.
        Echo,
        /// Never sends the challenge: the handshake never completes.
        Silent,
        /// Authenticates them and refuses every request as busy.
        Busy,
    }

    /// An echo node played from each stream's sent bytes, at the start of
    /// every pass: the challenge, then the verdict once the response is
    /// in, then a reply and a batch end per request. The client's side is
    /// the kernel path's batching stream over a buffer that takes
    /// everything.
    struct EchoNode {
        mode: Mode,
        /// The run-wide number of the client on each stream, which every
        /// request's fingerprint must carry.
        ids: Vec<ClientId>,
        streams: Vec<Batched<Counting>>,
        ready: Vec<bool>,
        passes: usize,
        origin: Instant,
    }

    impl EchoNode {
        fn new(mode: Mode, ids: &[ClientId], window: usize, origin: Instant) -> Self {
            let streams = ids
                .iter()
                .map(|_| {
                    let mut stream = Batched::new(Counting::default(), window, frame_len());
                    if mode != Mode::Silent {
                        let challenge = TransportResponse::Challenge { nonce: [0x5A; 32] };
                        push_control(stream.inner_mut(), &challenge);
                    }
                    stream
                })
                .collect();
            Self {
                mode,
                ids: ids.to_vec(),
                streams,
                ready: vec![false; ids.len()],
                passes: 0,
                origin,
            }
        }
    }

    fn push_control(stream: &mut Counting, response: &TransportResponse) {
        let mut buf = [0u8; 64];
        let n = encode_transport_response(response, &mut buf).unwrap();
        stream.stream.push_received(&buf[..n]);
    }

    impl Streams for EchoNode {
        type Stream<'a> = &'a mut Batched<Counting>;

        fn now_ns(&self) -> u64 {
            nanos(self.origin.elapsed())
        }

        fn service(&mut self, _now_ns: u64) {
            self.passes += 1;
            let clients = self.streams.iter_mut().zip(&mut self.ready).zip(&self.ids);
            for ((stream, ready), id) in clients {
                assert_eq!(stream.held(), 0, "a pass's bytes held into the next");
                let inner = stream.inner_mut();
                let sent = inner.stream.sent().to_vec();
                let mut at = 0;
                while let Some(frame) = split_frame(&sent[at..]).unwrap() {
                    at += frame.consumed();
                    if !*ready {
                        // The challenge response: the node's verdict.
                        *ready = true;
                        push_control(inner, &TransportResponse::ServerReady);
                        continue;
                    }
                    let body = &frame.payload[TAG_LEN..];
                    assert_eq!(
                        body[..4],
                        id.0.to_le_bytes(),
                        "the client's run-wide number"
                    );
                    if self.mode == Mode::Busy {
                        push_control(inner, &TransportResponse::ServerBusy);
                        continue;
                    }
                    let mut reply = Vec::with_capacity(PREFIX_LEN + 2 + body.len());
                    reply.extend_from_slice(&((2 + body.len()) as u32).to_le_bytes());
                    reply.extend_from_slice(&[TAG_APP, echo_server::KIND_RESP_ECHO]);
                    reply.extend_from_slice(body);
                    inner.stream.push_received(&reply);
                    push_control(inner, &TransportResponse::BatchEnd);
                }
                inner.stream.consume_sent(at);
            }
        }

        fn stream(&mut self, client: usize) -> &mut Batched<Counting> {
            &mut self.streams[client]
        }

        fn end_sends(&mut self, client: usize) -> std::io::Result<()> {
            self.streams[client].write()
        }
    }

    fn app() -> App {
        App::new(AppKind::Echo, 16).unwrap()
    }

    fn frame_len() -> usize {
        melin_client::REQUEST_HEADER_LEN + app().max_body()
    }

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[7; 32])
    }

    fn plan(window: usize, duration: Duration, timeout: Duration) -> Plan {
        Plan {
            window,
            warmup: Duration::ZERO,
            duration,
            cooldown: Duration::ZERO,
            timeout,
        }
    }

    /// Run `threads` shards over played nodes, shard `i` in `modes[i]`;
    /// each shard's outcome, or the run's error.
    fn run_shards(clients: usize, modes: &[Mode], plan: Plan) -> Result<Vec<Outcome>, String> {
        let origin = Instant::now();
        let key = key();
        let shards = Shards::new(modes.len(), clients).unwrap();
        shard::run(shards, |index, ids, shared| {
            let node = |ids: &[ClientId]| Ok(EchoNode::new(modes[index], ids, plan.window, origin));
            shard(node, ids, &key, app(), plan, shared)
        })
    }

    #[test]
    fn a_pass_s_requests_leave_in_one_write() {
        let (clients, window) = (3, 4);
        let plan = plan(window, Duration::from_millis(20), Duration::from_secs(1));
        let key = key();
        let mut runs = shard::run(Shards::new(1, clients).unwrap(), |_, ids, shared| {
            let mut node = EchoNode::new(Mode::Echo, &ids, window, Instant::now());
            let outcome = shard(|_| Ok(&mut node), ids, &key, app(), plan, shared)?;
            let writes: usize = node.streams.iter_mut().map(|s| s.inner_mut().sends).sum();
            Ok((outcome, writes, node.passes))
        })
        .unwrap();
        let (outcome, writes, passes) = runs.pop().unwrap();
        // At most one write per client per pass, and every write but the
        // challenge response carrying a full window: the node answers a
        // pass's requests by the next pass.
        assert!(
            writes <= clients * passes,
            "{writes} writes in {passes} passes"
        );
        assert_eq!(
            outcome.completed,
            ((writes - clients) * window) as u64,
            "{writes} writes"
        );
    }

    #[test]
    fn shards_load_their_own_clients_and_add_up() {
        let plan = plan(2, Duration::from_millis(30), Duration::from_secs(1));
        let outcomes = run_shards(5, &[Mode::Echo, Mode::Echo], plan).unwrap();
        assert_eq!(outcomes.len(), 2);
        for outcome in &outcomes {
            assert!(outcome.completed > 0);
            assert_eq!(outcome.measured, plan.duration);
        }
        let completed: u64 = outcomes.iter().map(|o| o.completed).sum();
        let merged = merge(outcomes).unwrap();
        assert_eq!(merged.completed, completed);
    }

    #[test]
    fn a_handshake_that_never_completes_stops_every_shard() {
        let started = Instant::now();
        // Long enough that only the failure can end the run in time.
        let plan = plan(1, Duration::from_secs(30), Duration::from_millis(200));
        let err = run_shards(4, &[Mode::Echo, Mode::Silent], plan).unwrap_err();
        assert!(
            err.contains("2 of this load thread's 2 connections not authenticated")
                && err.contains("client 1"),
            "{err}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_shard_that_fails_mid_run_stops_the_others() {
        let started = Instant::now();
        let plan = plan(1, Duration::from_secs(30), Duration::from_secs(1));
        let err = run_shards(2, &[Mode::Echo, Mode::Busy], plan).unwrap_err();
        assert_eq!(err, "client 1: the server is busy");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn a_failure_to_connect_stops_a_shard_still_in_its_handshake() {
        let started = Instant::now();
        let origin = Instant::now();
        let key = key();
        // Shard 0's handshake never completes, and would only time out
        // long after the test's bound: the other shard's failure ends it.
        let plan = plan(1, Duration::from_secs(30), Duration::from_secs(30));
        let err = shard::run(Shards::new(2, 2).unwrap(), |index, ids, shared| {
            let connect = |ids: &[ClientId]| match index {
                0 => Ok(EchoNode::new(Mode::Silent, ids, 1, origin)),
                _ => Err("cannot connect to the node".to_string()),
            };
            shard(connect, ids, &key, app(), plan, shared)
        })
        .unwrap_err();
        assert_eq!(err, "cannot connect to the node");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn outcomes_merge_into_the_run_s() {
        let outcome = |samples: &[u64], wall_ms: u64, completed: u64| {
            let mut latency = Histogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
            for &s in samples {
                latency.record(s).unwrap();
            }
            Outcome {
                latency,
                measured: Duration::from_millis(100),
                wall: Duration::from_millis(wall_ms),
                completed,
            }
        };
        let merged = merge(vec![outcome(&[10, 20], 150, 5), outcome(&[30], 170, 4)]).unwrap();
        assert_eq!(merged.latency.len(), 3);
        assert_eq!(merged.latency.max(), 30);
        assert_eq!(merged.completed, 9);
        assert_eq!(merged.wall, Duration::from_millis(170));
        assert_eq!(merged.measured, Duration::from_millis(100));
        assert!(merge(Vec::new()).is_err());
    }
}
