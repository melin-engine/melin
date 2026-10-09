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
