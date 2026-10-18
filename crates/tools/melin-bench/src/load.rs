//! The closed loop: every client's connection driven from one thread,
//! each keeping `window` requests in flight, over whichever streams carry
//! them.

use std::collections::VecDeque;
use std::time::Duration;

use hdrhistogram::Histogram;
use melin_client::{Ack, ByteStream, Frame, PolledConnection, RequestId};

use crate::app::App;

/// The streams a run's connections ride on, one per client, and the
/// clock the loop reads: kernel sockets, or a DPDK NIC's streams.
pub trait Streams {
    /// One client's stream, lent for a call.
    type Stream<'a>: ByteStream
    where
        Self: 'a;

    /// Monotonic nanoseconds, on the origin the streams' own timers use.
    fn now_ns(&self) -> u64;

    /// Advance every stream once: a user-space stack's step; nothing for
    /// kernel sockets.
    fn service(&mut self, now_ns: u64);

    /// Client `client`'s stream.
    fn stream(&mut self, client: usize) -> Self::Stream<'_>;
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

/// What a run measured.
#[derive(Debug)]
pub struct Outcome {
    /// Round trips in nanoseconds, of the replies that arrived inside the
    /// measured phase.
    pub latency: Histogram<u64>,
    /// The measured phase, as it ran.
    pub measured: Duration,
    /// From the first request to the last reply.
    pub wall: Duration,
    /// Every request answered, timed or not: what the node has
    /// sequenced for this run.
    pub completed: u64,
    /// Requests the node shed at ingress (`ServerBusy`): not applied, and
    /// not resent. Load shedding is behaviour the benchmark measures.
    pub shed: u64,
}

/// A request still owed its final frame.
#[derive(Clone, Copy, Debug)]
struct Owed {
    id: RequestId,
    /// When it was sent, on the streams' clock.
    sent_ns: u64,
    /// The application sequence its reply is checked against.
    seq: u64,
}

/// One client: its connection and its requests in flight.
struct Client {
    connection: PolledConnection,
    /// The requests still owed their final frame, in send order, and so
    /// sorted by ID (a connection allocates them in increasing order): a
    /// final frame is looked up by the ID it names, the front first (the
    /// in-order answer, O(1)), else by binary search, since a request the
    /// node did not accept may be answered ahead of earlier ones. A
    /// `VecDeque` for the push at the back and the pop at the front.
    owed: VecDeque<Owed>,
    /// The reply of the batch in progress, which belongs to the next batch
    /// end: its bytes, copied (a reused buffer, allocated once), and when
    /// it arrived, which is when its round trip is timed.
    reply: Vec<u8>,
    replied_ns: Option<u64>,
    /// The number of the next request.
    next_seq: u64,
}

impl Client {
    fn in_flight(&self) -> usize {
        self.owed.len()
    }

    /// Take the request `id` names off the owed ones.
    ///
    /// The same lookup as `melin-client`'s `Outstanding::settle` (front
    /// first, else a binary search of the sorted deque), copied because
    /// that type is crate-private and the bench keeps its own fields per
    /// request.
    fn settle(&mut self, id: RequestId, client: u32) -> Result<Owed, String> {
        let at = if self.owed.front().is_some_and(|owed| owed.id == id) {
            0
        } else {
            self.owed
                .binary_search_by_key(&id, |owed| owed.id)
                .map_err(|_| format!("client {client}: an answer for request {}, not owed", id.0))?
        };
        Ok(self.owed.remove(at).expect("found above"))
    }
}

/// Load the node through `connections`, one per stream of `streams`,
/// until the plan has run, then wait for the requests in flight.
pub fn run<S: Streams>(
    streams: &mut S,
    connections: Vec<PolledConnection>,
    app: App,
    plan: Plan,
) -> Result<Outcome, String> {
    let mut clients: Vec<Client> = connections
        .into_iter()
        .map(|connection| Client {
            connection,
            owed: VecDeque::with_capacity(plan.window),
            reply: Vec::with_capacity(app.max_body()),
            replied_ns: None,
            next_seq: 1,
        })
        .collect();
    authenticate(streams, &mut clients, plan.timeout)?;

    // Three significant figures, from a nanosecond to an hour: the
    // resolution a round trip has, in a fixed few tens of kilobytes.
    let mut latency = Histogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3)
        .map_err(|e| format!("histogram: {e}"))?;
    let start_ns = streams.now_ns();
    let measure_from = start_ns + nanos(plan.warmup);
    let measure_until = measure_from + nanos(plan.duration);
    let stop_sending = measure_until + nanos(plan.cooldown);
    let mut drain_deadline = None;
    // `u64`: counts of requests, which no run comes near overflowing.
    let mut completed: u64 = 0;
    let mut shed: u64 = 0;

    loop {
        let now_ns = streams.now_ns();
        streams.service(now_ns);
        let sending = now_ns < stop_sending;
        if !sending && drain_deadline.is_none() {
            drain_deadline = Some(now_ns + nanos(plan.timeout));
        }
        let mut in_flight = 0;
        for (index, client) in clients.iter_mut().enumerate() {
            let id = index as u32;
            while sending && client.in_flight() < plan.window {
                let seq = client.next_seq;
                let request = client
                    .connection
                    .send_with(&mut streams.stream(index), now_ns, app.max_body(), |out| {
                        Ok::<_, melin_client::Error>(app.encode(out, id, seq))
                    })
                    .map_err(|e| format!("client {id}: {e}"))?;
                client.owed.push_back(Owed {
                    id: request,
                    sent_ns: now_ns,
                    seq,
                });
                client.next_seq += 1;
            }
            loop {
                let frame = client
                    .connection
                    .poll(&mut streams.stream(index), now_ns)
                    .map_err(|e| format!("client {id}: {e}"))?;
                let Some(frame) = frame else { break };
                match frame {
                    // Timed as it arrives, attributed to its request at the
                    // batch end, which names it.
                    Frame::Response(reply) => {
                        if client.replied_ns.is_some() {
                            return Err(format!("client {id}: a batch with two replies"));
                        }
                        client.replied_ns = Some(streams.now_ns());
                        client.reply.clear();
                        client.reply.extend_from_slice(reply);
                    }
                    Frame::BatchEnd {
                        id: request,
                        ack: Ack::Policy,
                    } => {
                        let owed = client.settle(request, id)?;
                        let received_ns = client.replied_ns.take().ok_or_else(|| {
                            format!("client {id}: a batch ended with no reply in it")
                        })?;
                        app.check(&client.reply, id, owed.seq)?;
                        completed += 1;
                        if (measure_from..measure_until).contains(&received_ns) {
                            latency
                                .record(received_ns.saturating_sub(owed.sent_ns).max(1))
                                .map_err(|e| format!("histogram: {e}"))?;
                        }
                    }
                    // Not the sequencer's latency: the primary lost its
                    // replica mid-run and answered on its own disk.
                    Frame::BatchEnd {
                        ack: Ack::PrimaryOnly,
                        ..
                    } => {
                        return Err(format!(
                            "client {id}: a reply backed by the primary's disk alone \
                             (the primary lost its replica mid-run)"
                        ));
                    }
                    // Shed at ingress: not applied, counted, and its place
                    // in the window freed. The connection stays open.
                    Frame::ServerBusy { id: request } => {
                        client.settle(request, id)?;
                        shed += 1;
                    }
                    // The bench sends only well-formed requests: a refusal
                    // is a key or a build that does not fit the node.
                    Frame::Refused { kind, reason, .. } => {
                        return Err(format!(
                            "client {id}: {}",
                            melin_client::Error::Refused {
                                kind,
                                reason: reason.to_string()
                            }
                        ));
                    }
                }
            }
            in_flight += client.in_flight();
        }
        if let Some(deadline) = drain_deadline {
            if in_flight == 0 {
                let end_ns = streams.now_ns();
                return Ok(Outcome {
                    latency,
                    measured: plan.duration,
                    wall: Duration::from_nanos(end_ns - start_ns),
                    completed,
                    shed,
                });
            }
            if now_ns > deadline {
                return Err(format!(
                    "{in_flight} requests still owed their reply {:?} after the load stopped",
                    plan.timeout
                ));
            }
        }
    }
}

/// Drive every connection's handshake until all are ready.
fn authenticate<S: Streams>(
    streams: &mut S,
    clients: &mut [Client],
    timeout: Duration,
) -> Result<(), String> {
    let deadline = streams.now_ns() + nanos(timeout);
    loop {
        let now_ns = streams.now_ns();
        streams.service(now_ns);
        let mut ready = 0;
        for (index, client) in clients.iter_mut().enumerate() {
            if let Some(frame) = client
                .connection
                .poll(&mut streams.stream(index), now_ns)
                .map_err(|e| format!("client {index}: {e}"))?
            {
                return Err(format!("client {index}: {frame:?} before any request"));
            }
            ready += usize::from(client.connection.is_ready());
        }
        if ready == clients.len() {
            return Ok(());
        }
        if now_ns > deadline {
            return Err(format!(
                "{} of {} connections not authenticated within {timeout:?}",
                clients.len() - ready,
                clients.len()
            ));
        }
    }
}

/// `d` in nanoseconds, saturating: a run is never centuries long.
pub fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::AppKind;
    use melin_client::BufferStream;

    /// A node played by hand: each client's stream is a buffer the test
    /// answers from.
    struct Buffers {
        streams: Vec<BufferStream>,
        origin: std::time::Instant,
    }

    impl Streams for Buffers {
        type Stream<'a> = &'a mut BufferStream;

        fn now_ns(&self) -> u64 {
            nanos(self.origin.elapsed())
        }

        fn service(&mut self, _now_ns: u64) {}

        fn stream(&mut self, client: usize) -> &mut BufferStream {
            &mut self.streams[client]
        }
    }

    use echo_server::KIND_RESP_ECHO;
    use melin_client::framing::{FrameDecoder, split_request};
    use melin_client::{RefusalKind, RequestId};
    use melin_wire_protocol::control::TransportResponse;
    use melin_wire_protocol::control_codec::{
        CHALLENGE_RESPONSE_LEN, MAX_CONTROL_FRAME_LEN, TAG_APP, encode_transport_response,
    };

    /// How the scripted node answers a request.
    #[derive(Clone, Copy, Debug)]
    enum Answer {
        Echo,
        Busy,
        Refuse,
        /// A batch with no reply in it.
        Empty,
        /// A batch with the echo twice.
        Twice,
    }

    /// An echo node played in process: each client's stream is a buffer,
    /// and every service step answers what the clients sent, request `n`
    /// (from 0, per client) as `script` says. Requests answered `Busy`
    /// are answered ahead of the request before them when `overtake` is
    /// set, as a node answers a request it did not accept.
    struct ScriptedNode {
        streams: Vec<BufferStream>,
        decoders: Vec<FrameDecoder>,
        /// Whether each client's challenge response has been taken.
        answered_challenge: Vec<bool>,
        seen: Vec<u64>,
        script: fn(u64) -> Answer,
        overtake: bool,
        /// A request held for the next one to overtake: its ID and body.
        held: Vec<Option<(RequestId, Vec<u8>)>>,
        origin: std::time::Instant,
    }

    fn control(response: TransportResponse) -> Vec<u8> {
        let mut buf = [0u8; MAX_CONTROL_FRAME_LEN];
        let n = encode_transport_response(&response, &mut buf).unwrap();
        buf[..n].to_vec()
    }

    impl ScriptedNode {
        fn new(clients: usize, script: fn(u64) -> Answer, overtake: bool) -> Self {
            let mut streams = Vec::new();
            for _ in 0..clients {
                let mut stream = BufferStream::new();
                stream.push_received(&control(TransportResponse::Challenge { nonce: [1; 32] }));
                stream.push_received(&control(TransportResponse::ServerReady));
                streams.push(stream);
            }
            Self {
                streams,
                decoders: (0..clients).map(|_| FrameDecoder::new()).collect(),
                answered_challenge: vec![false; clients],
                seen: vec![0; clients],
                script,
                overtake,
                held: vec![None; clients],
                origin: std::time::Instant::now(),
            }
        }

        fn echo(stream: &mut BufferStream, id: RequestId, body: &[u8]) {
            Self::reply(stream, body);
            stream.push_received(&control(TransportResponse::BatchEnd { id }));
        }

        /// The echo of `body`, with no batch end after it.
        fn reply(stream: &mut BufferStream, body: &[u8]) {
            let mut frame = ((2 + body.len()) as u32).to_le_bytes().to_vec();
            frame.extend([TAG_APP, KIND_RESP_ECHO]);
            frame.extend_from_slice(body);
            stream.push_received(&frame);
        }
    }

    impl Streams for ScriptedNode {
        type Stream<'a> = &'a mut BufferStream;

        fn now_ns(&self) -> u64 {
            nanos(self.origin.elapsed())
        }

        fn service(&mut self, _now_ns: u64) {
            for client in 0..self.streams.len() {
                let stream = &mut self.streams[client];
                let mut sent = stream.sent().to_vec();
                stream.consume_sent(sent.len());
                if !self.answered_challenge[client] && sent.len() >= 4 + CHALLENGE_RESPONSE_LEN {
                    sent.drain(..4 + CHALLENGE_RESPONSE_LEN);
                    self.answered_challenge[client] = true;
                }
                let decoder = &mut self.decoders[client];
                decoder.push(&sent);
                let held = &mut self.held[client];
                while let Some(payload) = decoder.next().unwrap() {
                    let (id, body) = split_request(payload).expect("a request");
                    let n = self.seen[client];
                    self.seen[client] += 1;
                    let early = match (self.script)(n) {
                        Answer::Echo => {
                            // The echo held before goes out: nothing
                            // overtook it.
                            if let Some((held_id, held_body)) = held.take() {
                                Self::echo(stream, held_id, &held_body);
                            }
                            if self.overtake {
                                *held = Some((id, body.to_vec()));
                            } else {
                                Self::echo(stream, id, body);
                            }
                            continue;
                        }
                        Answer::Empty => TransportResponse::BatchEnd { id },
                        Answer::Twice => {
                            Self::reply(stream, body);
                            Self::echo(stream, id, body);
                            continue;
                        }
                        Answer::Busy => TransportResponse::ServerBusy { id },
                        Answer::Refuse => TransportResponse::Refused {
                            id,
                            kind: RefusalKind::Malformed,
                            reason: "unknown kind",
                        },
                    };
                    // An early answer, ahead of the echo held before it.
                    stream.push_received(&control(early));
                    if let Some((held_id, held_body)) = held.take() {
                        Self::echo(stream, held_id, &held_body);
                    }
                }
                // Nothing is held past a service step: no later request
                // would overtake it.
                if let Some((held_id, held_body)) = held.take() {
                    Self::echo(stream, held_id, &held_body);
                }
            }
        }

        fn stream(&mut self, client: usize) -> &mut BufferStream {
            &mut self.streams[client]
        }
    }

    /// A short run of `clients` clients against `node`.
    fn run_against(node: &mut ScriptedNode, clients: usize) -> Result<Outcome, String> {
        let key = melin_client::SigningKey::from_bytes(&[7; 32]);
        let plan = Plan {
            window: 4,
            warmup: Duration::ZERO,
            duration: Duration::from_millis(30),
            cooldown: Duration::ZERO,
            timeout: Duration::from_secs(5),
        };
        let app = App::new(AppKind::Echo, 16).unwrap();
        let connections = (0..clients).map(|_| PolledConnection::new(&key)).collect();
        run(node, connections, app, plan)
    }

    /// A busy is counted as shed and frees its place in the window; the
    /// run goes on, and only echoes count as completed.
    #[test]
    fn a_shed_request_is_counted_and_the_run_goes_on() {
        let mut node = ScriptedNode::new(
            2,
            |n| {
                if n % 3 == 2 {
                    Answer::Busy
                } else {
                    Answer::Echo
                }
            },
            false,
        );
        let outcome = run_against(&mut node, 2).expect("the run completes");
        let sent: u64 = node.seen.iter().sum();
        assert!(outcome.shed > 0, "{outcome:?}");
        assert!(outcome.completed > 0, "{outcome:?}");
        assert_eq!(
            outcome.completed + outcome.shed,
            sent,
            "every request answered once"
        );
        assert_eq!(outcome.shed, node.seen.iter().map(|n| n / 3).sum::<u64>());
    }

    /// A busy that overtakes the request before it is paired with its own
    /// request by ID, and the request it overtook is still checked against
    /// its own sequence when its echo comes.
    #[test]
    fn an_early_busy_is_paired_by_id_not_by_order() {
        let mut node = ScriptedNode::new(
            1,
            |n| {
                if n % 2 == 1 {
                    Answer::Busy
                } else {
                    Answer::Echo
                }
            },
            true,
        );
        let outcome = run_against(&mut node, 1).expect("every echo checks out");
        assert!(outcome.shed > 0 && outcome.completed > 0, "{outcome:?}");
        assert_eq!(outcome.completed + outcome.shed, node.seen[0]);
    }

    /// The bench sends only well-formed requests: a refusal fails the run,
    /// with the node's reason.
    #[test]
    fn a_refusal_fails_the_run() {
        let mut node = ScriptedNode::new(
            1,
            |n| if n == 3 { Answer::Refuse } else { Answer::Echo },
            false,
        );
        let err = run_against(&mut node, 1).unwrap_err();
        assert!(
            err.contains("refused") && err.contains("unknown kind"),
            "{err}"
        );
    }

    /// An echo request's batch must hold exactly its echo: an empty one
    /// and one with two replies both fail the run.
    #[test]
    fn a_batch_without_its_one_reply_fails_the_run() {
        let mut node = ScriptedNode::new(1, |_| Answer::Empty, false);
        let err = run_against(&mut node, 1).unwrap_err();
        assert!(err.contains("no reply in it"), "{err}");

        let mut node = ScriptedNode::new(1, |_| Answer::Twice, false);
        let err = run_against(&mut node, 1).unwrap_err();
        assert!(err.contains("two replies"), "{err}");
    }

    #[test]
    fn a_handshake_that_never_completes_times_out() {
        let key = melin_client::SigningKey::from_bytes(&[7; 32]);
        let mut buffers = Buffers {
            streams: vec![BufferStream::new()],
            origin: std::time::Instant::now(),
        };
        let plan = Plan {
            window: 1,
            warmup: Duration::ZERO,
            duration: Duration::from_millis(1),
            cooldown: Duration::ZERO,
            timeout: Duration::from_millis(20),
        };
        let app = App::new(AppKind::Notary, 0).unwrap();
        let err = run(&mut buffers, vec![PolledConnection::new(&key)], app, plan).unwrap_err();
        assert!(err.contains("not authenticated"), "{err}");
    }
}
