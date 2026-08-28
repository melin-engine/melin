#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Client side of the Melin wire protocol: connect to a node, prove a
//! key, send requests, read the replies.
//!
//! Every program that talks to a node — a trading gateway, an operator's
//! tool, a benchmark, an example — does the same five things before any
//! application logic runs: frame bytes with a length prefix, answer the
//! Ed25519 challenge, stamp each request with a per-key sequence, read
//! replies until the batch ends while ignoring heartbeats, and turn a
//! node's silence into an error. This crate is those five things, once.
//!
//! What it is not: the application's protocol. A node hosts an
//! application whose requests and responses are its own bytes behind a
//! tag; this crate carries them and never looks inside. Decoding a reply
//! is the caller's, with the tag constants the application publishes.
//!
//! ## Shape
//!
//! [`Connection::connect`] dials and authenticates. From there, either
//! [`Connection::request`] — one request, and the domain frames of its
//! reply batch — or the pair [`Connection::send`] and
//! [`Connection::next_frame`], for callers that keep several requests in
//! flight or want to time the reply frame itself. Blocking, one thread
//! per connection, `std::net` only: the shape a gateway thread or a load
//! generator wants, and a receive path that allocates nothing per frame.
//!
//! ## Silence
//!
//! A node does not answer a request it refuses — a key whose role may
//! not perform the operation, a malformed frame — it drops the frame and
//! keeps the connection. The only signal is the read timeout, which this
//! crate reports as [`Error::NoReply`] with that explanation attached, so
//! callers do not each have to know it.
//!
//! ```no_run
//! use melin_client::{Connection, key};
//!
//! let key = key::load_signing_key("client.pem".as_ref())?;
//! let mut node = Connection::connect("127.0.0.1:9876".parse()?, &key)?;
//! // `0x10` is whatever the application defines as its request tag; the
//! // reply is its bytes, tag first.
//! let reply = node.request_one(1, 0x10, b"payload")?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fmt;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use base64::Engine;
use ed25519_dalek::Signer;
use melin_wire_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
use melin_wire_protocol::control::ChallengeResponse;
use melin_wire_protocol::control_codec::{
    CHALLENGE_RESPONSE_LEN, TAG_AUTH_FAILED, TAG_BATCH_END, TAG_CHALLENGE, TAG_ENGINE_ERROR,
    TAG_RESPONSE_HEARTBEAT, TAG_SERVER_BUSY, TAG_SERVER_READY, encode_challenge_response,
};

pub mod key;

// The key types a caller needs to hold, so that depending on this crate
// is enough to authenticate.
pub use ed25519_dalek::{SigningKey, VerifyingKey};

/// Read and connect timeout used by [`Connection::connect`] and
/// [`Connection::connect_by`]. Generous for a round trip anywhere on a
/// LAN; short enough that a silently dropped request (see the crate
/// docs) becomes an error rather than a hang.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Everything that can go wrong between a client and a node.
#[derive(Debug)]
pub enum Error {
    /// The socket failed underneath the protocol.
    Io(io::Error),
    /// The node could not be reached at `addr`.
    Connect { addr: SocketAddr, source: io::Error },
    /// The node was not serving by the deadline given to
    /// [`Connection::connect_by`]; `last` is what the final attempt saw.
    Deadline { addr: SocketAddr, last: Box<Error> },
    /// Key material could not be read or parsed — see [`key`].
    Key(String),
    /// The node refused the key. Carries the public key's raw bytes so
    /// the message can say what to put in `authorized_keys` (raw rather
    /// than a [`VerifyingKey`], which also holds its decompressed point
    /// and would make every `Result` in this crate a wide one).
    AuthFailed { public_key: [u8; 32] },
    /// Nothing arrived within the read timeout. Almost always a request
    /// the node refused and silently dropped — see the crate docs.
    NoReply { timeout: Duration },
    /// The node closed the connection.
    Disconnected,
    /// The node sent something the protocol does not allow here.
    Protocol(String),
    /// The node is shedding load; retry later, on a new connection.
    ServerBusy,
    /// The node's application failed on the request; do not retry.
    EngineError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "connection lost: {e}"),
            Error::Connect { addr, source } => write!(f, "cannot connect to {addr}: {source}"),
            Error::Deadline { addr, last } => {
                write!(
                    f,
                    "{addr} was not serving by the deadline (last attempt: {last})"
                )
            }
            Error::Key(reason) => f.write_str(reason),
            Error::AuthFailed { public_key } => write!(
                f,
                "authentication failed: is {} listed in the node's authorized_keys?",
                base64::engine::general_purpose::STANDARD.encode(public_key)
            ),
            Error::NoReply { timeout } => write!(
                f,
                "no reply within {:.1}s: a node silently drops requests it refuses — check \
                 that the key's role in authorized_keys may perform this operation, and that \
                 the request is well-formed",
                timeout.as_secs_f64()
            ),
            Error::Disconnected => f.write_str("the node closed the connection"),
            Error::Protocol(what) => write!(f, "protocol violation: {what}"),
            Error::ServerBusy => f.write_str("the node is busy: retry later on a new connection"),
            Error::EngineError => f.write_str("the node reported an engine error; do not retry"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) | Error::Connect { source: e, .. } => Some(e),
            Error::Deadline { last, .. } => Some(last.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// One frame from the node, as [`Connection::next_frame`] hands it over.
/// Heartbeats never surface: they carry nothing and are skipped.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    /// An application response: its bytes, tag first. Borrowed from the
    /// connection's buffer, valid until the next read.
    Response(&'a [u8]),
    /// The last frame of one request's reply batch.
    BatchEnd,
    /// The node is shedding load; nothing further will come for the
    /// request, and the connection should be dropped.
    ServerBusy,
    /// The application failed on the request.
    EngineError,
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// An authenticated connection to a node.
///
/// After any error the connection's framing can no longer be trusted
/// (a timeout may have cut a frame in half); drop it and connect again.
pub struct Connection {
    reader: BlockingFrameReader<TcpStream>,
    writer: BlockingFrameWriter<TcpStream>,
    /// The socket itself, for options and for handing over to a caller.
    /// The reader and writer hold duplicates of the same descriptor, so
    /// an option set here applies to them.
    stream: TcpStream,
    read_timeout: Duration,
    public_key: VerifyingKey,
    /// Reused across `send` calls: a request is the sequence, the tag and
    /// the body in one frame, and the writer takes one slice.
    scratch: Vec<u8>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection")
            .field("peer", &self.stream.peer_addr().ok())
            .field("public_key", &key::public_key_base64(&self.public_key))
            .finish_non_exhaustive()
    }
}

impl Connection {
    /// Connect to `addr` and authenticate with `key`, using
    /// [`DEFAULT_TIMEOUT`] for the connection and for every read after.
    pub fn connect(addr: SocketAddr, key: &SigningKey) -> Result<Self, Error> {
        Self::connect_timeout(addr, key, DEFAULT_TIMEOUT)
    }

    /// [`connect`](Self::connect) with an explicit timeout, applied to
    /// the connection attempt, the handshake, and every read after —
    /// [`set_read_timeout`](Self::set_read_timeout) changes the last.
    pub fn connect_timeout(
        addr: SocketAddr,
        key: &SigningKey,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let stream = TcpStream::connect_timeout(&addr, timeout)
            .map_err(|source| Error::Connect { addr, source })?;
        stream.set_read_timeout(Some(timeout))?;
        // A request is one small frame and the reply is what the caller is
        // waiting for: never hold it for coalescing.
        stream.set_nodelay(true)?;
        let mut connection = Connection {
            reader: BlockingFrameReader::new(stream.try_clone()?),
            writer: BlockingFrameWriter::new(stream.try_clone()?),
            stream,
            read_timeout: timeout,
            public_key: key.verifying_key(),
            scratch: Vec::new(),
        };
        connection.authenticate(key)?;
        Ok(connection)
    }

    /// Keep trying to connect until `deadline`, for a node that is still
    /// starting. Retries what time can fix — a refused connection, a
    /// listener whose backlog took the connection before the node was
    /// serving (the handshake then times out) — and gives up at once on
    /// a refused key. The connection returned reads with
    /// [`DEFAULT_TIMEOUT`].
    pub fn connect_by(
        addr: SocketAddr,
        key: &SigningKey,
        deadline: Instant,
    ) -> Result<Self, Error> {
        /// Pause between attempts.
        const RETRY: Duration = Duration::from_millis(100);
        /// Cap on one attempt, so a backlog-accepted connection to a node
        /// that is not yet serving is abandoned quickly and retried.
        const ATTEMPT: Duration = Duration::from_millis(500);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            // A zero timeout is an error to `TcpStream::connect_timeout`.
            let attempt = remaining.clamp(Duration::from_millis(1), ATTEMPT);
            match Self::connect_timeout(addr, key, attempt) {
                Ok(mut connection) => {
                    connection.set_read_timeout(DEFAULT_TIMEOUT)?;
                    return Ok(connection);
                }
                Err(refused @ Error::AuthFailed { .. }) => return Err(refused),
                Err(last) if remaining <= RETRY => {
                    return Err(Error::Deadline {
                        addr,
                        last: Box::new(last),
                    });
                }
                Err(_) => std::thread::sleep(RETRY),
            }
        }
    }

    /// Change how long a read waits before reporting [`Error::NoReply`].
    pub fn set_read_timeout(&mut self, timeout: Duration) -> Result<(), Error> {
        self.stream.set_read_timeout(Some(timeout))?;
        self.read_timeout = timeout;
        Ok(())
    }

    /// The key this connection authenticated with.
    pub fn public_key(&self) -> &VerifyingKey {
        &self.public_key
    }

    /// The node's address.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    /// Send one request: `[request_seq: u64][tag][body]`, flushed.
    ///
    /// `request_seq` is the per-key idempotency sequence the application
    /// checks (see `Application::check_request_seq` in `melin-app`);
    /// applications that accept every request still want it monotonic
    /// per connection, which is what a counter gives.
    pub fn send(&mut self, request_seq: u64, tag: u8, body: &[u8]) -> Result<(), Error> {
        self.scratch.clear();
        self.scratch.extend_from_slice(&request_seq.to_le_bytes());
        self.scratch.push(tag);
        self.scratch.extend_from_slice(body);
        self.writer.write_frame(&self.scratch)?;
        self.writer.flush()?;
        Ok(())
    }

    /// The next frame from the node, heartbeats skipped. Blocks up to
    /// the read timeout.
    pub fn next_frame(&mut self) -> Result<Frame<'_>, Error> {
        loop {
            // Decide on the tag alone, then borrow the frame back for the
            // one arm that returns it: a borrow that lives across the
            // loop and is conditionally returned is what the borrow
            // checker cannot follow.
            match self.raw_frame()?.first().copied() {
                None => return Err(Error::Protocol("empty frame".into())),
                Some(TAG_RESPONSE_HEARTBEAT) => continue,
                Some(TAG_BATCH_END) => return Ok(Frame::BatchEnd),
                Some(TAG_SERVER_BUSY) => return Ok(Frame::ServerBusy),
                Some(TAG_ENGINE_ERROR) => return Ok(Frame::EngineError),
                // The rest of the control range is the handshake, which
                // is over.
                Some(tag @ 0x01..=0x0F) => {
                    return Err(Error::Protocol(format!(
                        "unexpected control frame {tag:#04x} after the handshake"
                    )));
                }
                Some(_) => return Ok(Frame::Response(self.reader.frame())),
            }
        }
    }

    /// Send one request and collect the application frames of its reply
    /// batch, in order. A batch may hold none (the application had
    /// nothing to say) or several (a fill and its acknowledgement, say).
    pub fn request(
        &mut self,
        request_seq: u64,
        tag: u8,
        body: &[u8],
    ) -> Result<Vec<Vec<u8>>, Error> {
        self.send(request_seq, tag, body)?;
        let mut frames = Vec::new();
        loop {
            match self.next_frame()? {
                Frame::Response(bytes) => frames.push(bytes.to_vec()),
                Frame::BatchEnd => return Ok(frames),
                Frame::ServerBusy => return Err(Error::ServerBusy),
                Frame::EngineError => return Err(Error::EngineError),
            }
        }
    }

    /// [`request`](Self::request) for the common case of exactly one
    /// frame in reply; any other count is a protocol error.
    pub fn request_one(
        &mut self,
        request_seq: u64,
        tag: u8,
        body: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let mut frames = self.request(request_seq, tag, body)?;
        match frames.len() {
            1 => Ok(frames.swap_remove(0)),
            n => Err(Error::Protocol(format!(
                "expected one frame in reply, got {n}"
            ))),
        }
    }

    /// Give up the framed protocol and hand over the authenticated
    /// socket — for the node's admin listener, which authenticates the
    /// same way and then speaks text lines. Call it straight after
    /// connecting: anything the connection had already read past the
    /// handshake is discarded with it.
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }

    /// Answer the node's challenge: sign its nonce, send the signature
    /// with the public key, and wait for it to say it is ready.
    fn authenticate(&mut self, key: &SigningKey) -> Result<(), Error> {
        // `[tag][nonce: 32]`
        let nonce: [u8; 32] = match self.raw_frame()? {
            [TAG_CHALLENGE, nonce @ ..] if nonce.len() == 32 => {
                nonce.try_into().expect("length checked")
            }
            other => {
                return Err(Error::Protocol(format!(
                    "expected an auth challenge, got a {}-byte frame with tag {:?}",
                    other.len(),
                    other.first()
                )));
            }
        };
        let response = ChallengeResponse {
            signature: key.sign(&nonce).to_bytes(),
            public_key: self.public_key.to_bytes(),
        };
        let mut frame = [0u8; CHALLENGE_RESPONSE_LEN];
        // The handshake is not a request, so it carries sequence 0.
        encode_challenge_response(0, &response, &mut frame)
            .map_err(|e| Error::Protocol(format!("cannot encode the challenge response: {e}")))?;
        self.writer.write_frame(&frame)?;
        self.writer.flush()?;

        match self.raw_frame()?.first() {
            Some(&TAG_SERVER_READY) => Ok(()),
            Some(&TAG_AUTH_FAILED) => Err(Error::AuthFailed {
                public_key: self.public_key.to_bytes(),
            }),
            other => Err(Error::Protocol(format!(
                "expected the node to be ready or to refuse the key, got tag {other:?}"
            ))),
        }
    }

    /// One frame's payload, with the transport's outcomes mapped: a clean
    /// close is [`Error::Disconnected`], a timed-out read is
    /// [`Error::NoReply`].
    fn raw_frame(&mut self) -> Result<&[u8], Error> {
        let timeout = self.read_timeout;
        match self.reader.read_frame() {
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err(Error::Disconnected),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                Err(Error::NoReply { timeout })
            }
            Err(e) => Err(Error::Io(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests, against a fake node
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;

    use ed25519_dalek::{Signature, Verifier};
    use melin_wire_protocol::control::TransportResponse;
    use melin_wire_protocol::control_codec::{
        decode_challenge_response, encode_transport_response,
    };

    use super::*;

    /// The application's side of the fake: a request tag and its reply.
    const TAG_REQUEST: u8 = 0x10;
    const TAG_REPLY: u8 = 0x30;

    /// How the fake node behaves once a client is authenticated.
    #[derive(Clone, Copy)]
    enum Behaviour {
        /// Reply to every request with its body behind `TAG_REPLY`, then
        /// end the batch.
        Echo,
        /// A heartbeat before every reply, and two reply frames per batch.
        ChattyEcho,
        /// Read requests and never answer.
        Silent,
        /// Answer every request with `ServerBusy`.
        Busy,
        /// Answer every request with `EngineError`.
        Failing,
        /// Close the connection once authenticated.
        Hangup,
        /// The admin listener's shape: one text line in, `OK` out.
        AdminLines,
    }

    fn control(response: TransportResponse) -> Vec<u8> {
        let mut buf = [0u8; 64];
        let n = encode_transport_response(&response, &mut buf).unwrap();
        buf[..n].to_vec()
    }

    fn app_frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = ((1 + body.len()) as u32).to_le_bytes().to_vec();
        frame.push(tag);
        frame.extend_from_slice(body);
        frame
    }

    /// Serve one client on `stream` the way a node does: challenge,
    /// verify, then `behaviour`.
    fn serve(mut stream: TcpStream, allowed: VerifyingKey, behaviour: Behaviour) {
        let nonce = [0x5A; 32];
        stream
            .write_all(&control(TransportResponse::Challenge { nonce }))
            .unwrap();
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
        let (seq, response) =
            decode_challenge_response(reader.read_frame().unwrap().unwrap()).unwrap();
        assert_eq!(seq, 0, "the handshake carries sequence 0");
        let presented = VerifyingKey::from_bytes(&response.public_key).unwrap();
        let signature = Signature::from_bytes(&response.signature);
        if presented != allowed || presented.verify(&nonce, &signature).is_err() {
            stream
                .write_all(&control(TransportResponse::AuthFailed))
                .unwrap();
            return;
        }
        stream
            .write_all(&control(TransportResponse::ServerReady))
            .unwrap();

        match behaviour {
            Behaviour::Hangup => return,
            Behaviour::AdminLines => {
                let mut lines = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();
                lines.read_line(&mut line).unwrap();
                assert_eq!(line.trim_end(), "STATUS");
                stream.write_all(b"OK\n").unwrap();
                return;
            }
            _ => {}
        }
        while let Ok(Some(request)) = reader.read_frame() {
            // `[seq][tag][body]`
            assert_eq!(request[8], TAG_REQUEST);
            let body = request[9..].to_vec();
            let reply: Vec<u8> = match behaviour {
                Behaviour::Echo => [
                    app_frame(TAG_REPLY, &body),
                    control(TransportResponse::BatchEnd),
                ]
                .concat(),
                Behaviour::ChattyEcho => [
                    control(TransportResponse::Heartbeat),
                    app_frame(TAG_REPLY, &body),
                    app_frame(TAG_REPLY, b"again"),
                    control(TransportResponse::BatchEnd),
                ]
                .concat(),
                Behaviour::Silent => continue,
                Behaviour::Busy => control(TransportResponse::ServerBusy),
                Behaviour::Failing => control(TransportResponse::EngineError),
                Behaviour::Hangup | Behaviour::AdminLines => unreachable!(),
            };
            stream.write_all(&reply).unwrap();
        }
    }

    /// A fake node on a kernel-assigned port, serving clients on a
    /// thread until the listener is dropped.
    fn fake_node(allowed: VerifyingKey, behaviour: Behaviour) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                std::thread::spawn(move || serve(stream, allowed, behaviour));
            }
        });
        addr
    }

    fn client_key() -> SigningKey {
        SigningKey::from_bytes(&[0x11; 32])
    }

    #[test]
    fn a_request_gets_its_reply_batch() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut node = Connection::connect(addr, &key).unwrap();
        assert_eq!(node.public_key(), &key.verifying_key());
        assert_eq!(node.peer_addr().unwrap(), addr);

        let reply = node.request_one(1, TAG_REQUEST, b"hello").unwrap();
        assert_eq!(reply, [&[TAG_REPLY][..], b"hello"].concat());

        // The same over the pipelined pair, several requests in flight.
        for seq in 2..=4 {
            node.send(seq, TAG_REQUEST, &seq.to_le_bytes()).unwrap();
        }
        for seq in 2..=4u64 {
            assert_eq!(
                node.next_frame().unwrap(),
                Frame::Response(&[&[TAG_REPLY][..], &seq.to_le_bytes()].concat())
            );
            assert_eq!(node.next_frame().unwrap(), Frame::BatchEnd);
        }
    }

    #[test]
    fn heartbeats_are_skipped_and_a_batch_may_carry_several_frames() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::ChattyEcho);
        let mut node = Connection::connect(addr, &key).unwrap();

        let frames = node.request(1, TAG_REQUEST, b"x").unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], [TAG_REPLY, b'x']);
        assert_eq!(frames[1], [&[TAG_REPLY][..], b"again"].concat());

        assert!(matches!(
            node.request_one(2, TAG_REQUEST, b"y"),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn an_unknown_key_is_told_which_key_to_authorize() {
        let allowed = SigningKey::from_bytes(&[0x22; 32]).verifying_key();
        let addr = fake_node(allowed, Behaviour::Echo);
        let stranger = client_key();

        let err = Connection::connect(addr, &stranger).unwrap_err();
        assert!(
            matches!(err, Error::AuthFailed { public_key } if public_key == stranger.verifying_key().to_bytes())
        );
        assert!(
            err.to_string()
                .contains(&key::public_key_base64(&stranger.verifying_key())),
            "{err}"
        );
    }

    #[test]
    fn silence_is_reported_as_no_reply() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Silent);
        let timeout = Duration::from_millis(200);
        let mut node = Connection::connect_timeout(addr, &key, timeout).unwrap();

        let started = Instant::now();
        let err = node.request(1, TAG_REQUEST, b"dropped").unwrap_err();
        assert!(started.elapsed() >= timeout);
        assert!(matches!(err, Error::NoReply { timeout: t } if t == timeout));
        assert!(err.to_string().contains("authorized_keys"), "{err}");
    }

    #[test]
    fn busy_and_engine_error_are_frames_when_pipelining_and_errors_from_request() {
        let key = client_key();

        let addr = fake_node(key.verifying_key(), Behaviour::Busy);
        let mut node = Connection::connect(addr, &key).unwrap();
        node.send(1, TAG_REQUEST, b"").unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::ServerBusy);
        assert!(matches!(
            node.request(2, TAG_REQUEST, b""),
            Err(Error::ServerBusy)
        ));

        let addr = fake_node(key.verifying_key(), Behaviour::Failing);
        let mut node = Connection::connect(addr, &key).unwrap();
        node.send(1, TAG_REQUEST, b"").unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::EngineError);
        assert!(matches!(
            node.request(2, TAG_REQUEST, b""),
            Err(Error::EngineError)
        ));
    }

    #[test]
    fn a_closed_connection_is_reported() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Hangup);
        let mut node = Connection::connect(addr, &key).unwrap();
        assert!(matches!(node.next_frame(), Err(Error::Disconnected)));
    }

    #[test]
    fn connect_by_waits_for_a_node_that_is_still_starting() {
        let key = client_key();
        // The listener is bound now — the kernel will accept the client's
        // connection into the backlog — but nothing serves it until later,
        // so the first attempts' handshakes time out and are retried.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let allowed = key.verifying_key();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(700));
            for stream in listener.incoming().flatten() {
                std::thread::spawn(move || serve(stream, allowed, Behaviour::Echo));
            }
        });

        let started = Instant::now();
        let mut node =
            Connection::connect_by(addr, &key, Instant::now() + Duration::from_secs(10)).unwrap();
        assert!(started.elapsed() >= Duration::from_millis(500));
        assert_eq!(
            node.request_one(1, TAG_REQUEST, b"up").unwrap(),
            [&[TAG_REPLY][..], b"up"].concat()
        );
    }

    #[test]
    fn connect_by_gives_up_at_the_deadline() {
        let key = client_key();
        // A port that was free a moment ago and has no listener now.
        let addr = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap();
        let started = Instant::now();
        let err = Connection::connect_by(addr, &key, Instant::now() + Duration::from_millis(400))
            .unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(
            matches!(err, Error::Deadline { addr: a, ref last } if a == addr && matches!(**last, Error::Connect { .. })),
            "{err:?}"
        );
    }

    #[test]
    fn connect_by_does_not_retry_a_refused_key() {
        let allowed = SigningKey::from_bytes(&[0x22; 32]).verifying_key();
        let addr = fake_node(allowed, Behaviour::Echo);
        let started = Instant::now();
        let err = Connection::connect_by(
            addr,
            &client_key(),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap_err();
        assert!(matches!(err, Error::AuthFailed { .. }));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn into_stream_hands_over_an_authenticated_socket() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::AdminLines);
        let mut stream = Connection::connect(addr, &key).unwrap().into_stream();
        stream.write_all(b"STATUS\n").unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).unwrap();
        assert_eq!(reply, "OK\n");
    }
}
