//! The closed loop: every client's connection driven from one thread,
//! each keeping `window` requests in flight, over whichever streams carry
//! them.

use std::collections::VecDeque;
use std::time::Duration;

use hdrhistogram::Histogram;
use melin_client::{Ack, ByteStream, Frame, PolledConnection};

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

    /// Put on the wire what client `client` sent since the last call, as
    /// much as the stream takes now, the rest held for the next call: the
    /// end of its sends for this pass. Nothing for a stream that already
    /// sends from a buffer of its own on [`service`](Self::service).
    fn end_sends(&mut self, client: usize) -> std::io::Result<()>;
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
}

/// One client: its connection and its requests in flight.
struct Client {
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
            sent_ns: VecDeque::with_capacity(plan.window),
            replied: 0,
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
    // `u64`: a count of requests, which no run comes near overflowing.
    let mut completed: u64 = 0;

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
                            return Err(format!("client {id}: a reply to no request"));
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
                        ));
                    }
                    Frame::ServerBusy => return Err(format!("client {id}: the server is busy")),
                    Frame::EngineError => {
                        return Err(format!("client {id}: the application failed a request"));
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
                    wall: Duration::from_nanos(end_ns - start_ns),
                    completed,
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
            // The challenge response a poll queued.
            streams
                .end_sends(index)
                .map_err(|e| format!("client {index}: {e}"))?;
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
    use crate::batched::Batched;
    use crate::batched::testing::Counting;
    use melin_client::BufferStream;
    use melin_client::framing::{PREFIX_LEN, split_frame};
    use melin_wire_protocol::control::TransportResponse;
    use melin_wire_protocol::control_codec::{TAG_APP, TAG_LEN, encode_transport_response};

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

        fn end_sends(&mut self, _client: usize) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// An echo node played from each stream's sent bytes, at the start of
    /// every pass: the challenge, then the verdict once the response is
    /// in, then a reply and a batch end per request. The client's side is
    /// the kernel path's batching stream over a buffer that takes
    /// everything.
    struct EchoNode {
        streams: Vec<Batched<Counting>>,
        ready: Vec<bool>,
        passes: usize,
        origin: std::time::Instant,
    }

    impl EchoNode {
        fn new(clients: usize, window: usize, frame_len: usize) -> Self {
            let streams = (0..clients)
                .map(|_| {
                    let mut stream = Batched::new(Counting::default(), window, frame_len);
                    let challenge = TransportResponse::Challenge { nonce: [0x5A; 32] };
                    push_control(stream.inner_mut(), &challenge);
                    stream
                })
                .collect();
            Self {
                streams,
                ready: vec![false; clients],
                passes: 0,
                origin: std::time::Instant::now(),
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
            for (stream, ready) in self.streams.iter_mut().zip(&mut self.ready) {
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

    #[test]
    fn a_pass_s_requests_leave_in_one_write() {
        let (clients, window) = (3, 4);
        let app = App::new(AppKind::Echo, 16).unwrap();
        let frame_len = melin_client::REQUEST_HEADER_LEN + app.max_body();
        let mut node = EchoNode::new(clients, window, frame_len);
        let key = melin_client::SigningKey::from_bytes(&[7; 32]);
        let plan = Plan {
            window,
            warmup: Duration::ZERO,
            duration: Duration::from_millis(20),
            cooldown: Duration::ZERO,
            timeout: Duration::from_secs(1),
        };
        let connections = (0..clients).map(|_| PolledConnection::new(&key)).collect();
        let outcome = run(&mut node, connections, app, plan).unwrap();

        let writes: usize = node.streams.iter_mut().map(|s| s.inner_mut().sends).sum();
        // At most one write per client per pass, and every write but the
        // challenge response carrying a full window: the node answers a
        // pass's requests by the next pass.
        assert!(
            writes <= clients * node.passes,
            "{writes} writes in {} passes",
            node.passes
        );
        assert_eq!(
            outcome.completed,
            ((writes - clients) * window) as u64,
            "{writes} writes"
        );
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
