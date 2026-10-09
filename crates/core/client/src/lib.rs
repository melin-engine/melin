#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Client side of the Melin wire protocol: connect to a node, prove a
//! key, send requests, read the replies.
//!
//! Every program that talks to a node — a client gateway, an operator's
//! tool, a benchmark, an example — does the same four things before any
//! application logic runs: frame bytes with a length prefix, answer the
//! Ed25519 challenge, read replies until the batch ends while ignoring
//! heartbeats, and turn a node's silence into an error. This crate is
//! those four things, once.
//!
//! What it is not: the application's protocol. A node hosts an
//! application whose requests and responses are its own bytes; this crate
//! frames them as application frames and never looks inside. Encoding a
//! request and decoding a reply are the caller's, with whatever layout
//! the application publishes.
//!
//! ## Shape
//!
//! [`Connection::connect`] dials and authenticates. From there, either
//! [`Connection::request`] — one request, and the domain frames of its
//! reply batch — or the pair [`Connection::send`] and
//! [`Connection::next_frame`], for callers that keep several requests in
//! flight or want to time the reply frame itself.
//!
//! Every reply batch ends by saying what backs it ([`Ack`]): the node's
//! ack policy, or, from a primary halted for want of a replica, its own
//! disk alone. [`Connection::request`] takes only the first, and reports
//! the second as [`Error::Degraded`] with the reply's frames;
//! [`Connection::request_batch`] returns either, and the caller decides.
//!
//! Blocking, one thread per connection, `std::net` only: the shape a
//! gateway thread or a load generator wants, with no allocation and no
//! staging copy per frame in either direction. A program that owns its
//! socket — a Unix socket, or a descriptor its own I/O loop takes over —
//! runs the handshake alone with [`authenticate`].
//!
//! A program that runs its own I/O loop (`io_uring` completions, DPDK
//! receive bursts, a non-blocking socket polled by hand) uses the
//! I/O-free path, none of which reads, writes or measures time:
//!
//! - receiving, [`framing`] finds the frames in the bytes it has: a
//!   [`framing::FrameDecoder`] to push received chunks into, or
//!   [`framing::split_frame`] over a buffer of its own with no copy;
//! - a [`Handshake`] takes the node's first frames and hands back the
//!   answer to send;
//! - [`next_reply`] (or [`classify`], on a payload already split off)
//!   tells each reply apart, heartbeats included;
//! - sending, a request is framed in place in its send buffer, at any
//!   offset, so several batch into one write: the body is encoded into
//!   the region [`request_body`] hands out, behind room for a header of
//!   [`REQUEST_HEADER_LEN`] bytes, and [`seal_request`] writes the header
//!   in front of it ([`frame_request`] does both around a closure).
//!
//! The request framing is the wire protocol's own, re-exported here: an
//! application's codec crate that needs only framing can depend on
//! `melin-wire-protocol` alone, and gets a conversion of its errors into
//! that crate's `ProtocolError` as well as into this crate's [`Error`].
//!
//! The read timeout is then the caller's to keep, and so is the rule
//! that a heartbeat does not extend it (see [Silence](#silence)). An
//! error from [`next_reply`] means the connection is to be dropped:
//! [`Error::FrameTooLarge`] for a length prefix over the limit (the
//! decoder is poisoned), [`Error::Protocol`] for a frame no reply
//! carries.
//!
//! ## Silence
//!
//! A node does not answer a request it refuses — a key whose role may
//! not perform the operation, a malformed frame, a request laid out for
//! another version of the node's application — it drops the frame and
//! keeps the connection. The only signal is the read timeout, which this
//! crate reports as [`Error::NoReply`] with that explanation attached, so
//! callers do not each have to know it. A node heartbeats idle
//! connections, and a heartbeat is not an answer: it does not push the
//! timeout back.
//!
//! ```no_run
//! use melin_client::{Connection, key};
//!
//! let key = key::load_signing_key("client.pem".as_ref())?;
//! let mut node = Connection::connect("127.0.0.1:9876".parse()?, &key)?;
//! // The request and the reply are the application's bytes, laid out as
//! // it defines them.
//! let reply = node.request_one(b"request")?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fmt;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use base64::Engine;
use ed25519_dalek::Signer;
use melin_wire_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
use melin_wire_protocol::control::ChallengeResponse;
use melin_wire_protocol::control_codec::{
    CHALLENGE_RESPONSE_LEN, TAG_APP, TAG_AUTH_FAILED, TAG_BATCH_END, TAG_BATCH_END_DEGRADED,
    TAG_CHALLENGE, TAG_ENGINE_ERROR, TAG_LEN, TAG_RESPONSE_HEARTBEAT, TAG_SERVER_BUSY,
    TAG_SERVER_READY, encode_challenge_response,
};

pub mod key;

// The key types a caller needs to hold, so that depending on this crate
// is enough to authenticate.
pub use ed25519_dalek::{SigningKey, VerifyingKey};
// The bound on a frame. A caller sizing its widest request body wants
// `MAX_REQUEST_BODY`, which already takes off the protocol's tag.
pub use melin_wire_protocol::blocking::MAX_FRAME_SIZE;
// The I/O-free frame splitter and decoder, so a program running its own
// I/O loop frames the node's bytes without depending on the wire crate.
// Re-exported as a module rather than item by item: its `Frame` is the
// raw length-prefixed frame, not this crate's classified [`Frame`].
pub use melin_wire_protocol::framing;
// Request framing lives with the wire protocol, so a codec crate that
// needs only framing does not depend on this one; re-exported here so a
// client finds it next to the rest of the I/O-free path. Its error
// converts into [`Error`].
pub use melin_wire_protocol::framing::{
    MAX_REQUEST_BODY, REQUEST_HEADER_LEN, RequestFrameError, frame_request, request_body,
    seal_request,
};

use framing::{FrameDecoder, FrameTooLarge, PREFIX_LEN, request_payload_len};

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
    /// The node sent something the protocol does not allow here: a frame
    /// no reply carries, or one out of place in the handshake. The
    /// connection is to be dropped.
    Protocol(String),
    /// A length prefix from the node declared a payload of `declared`
    /// bytes, over the limit of `max`. Past it the stream has no frame
    /// boundary left: a [`FrameDecoder`] that reported it is poisoned,
    /// and the connection is to be dropped, as after
    /// [`Protocol`](Error::Protocol). A variant of its own so a caller
    /// can tell the two apart (and log the length) without asking the
    /// decoder, which the `Result` of [`next_reply`] still borrows.
    FrameTooLarge { declared: u32, max: usize },
    /// The request, with the protocol's tag in front of it, would not
    /// fit in one frame of [`MAX_FRAME_SIZE`] bytes; nothing was sent
    /// (from [`Connection::send`]) or framed (from [`frame_request`] or
    /// [`seal_request`]).
    RequestTooLarge { len: usize },
    /// A request framed in place did not fit its buffer: `needed` bytes,
    /// header included, against the `available` length of the buffer.
    /// Either the buffer is shorter than [`REQUEST_HEADER_LEN`], or the
    /// body runs past its end (from an encoder, a bug: it reported more
    /// than it was handed). Nothing was framed. The
    /// [`RequestFrameError::BufferTooSmall`] of [`frame_request`] and
    /// [`seal_request`], for a codec whose error is this one.
    BufferTooSmall { needed: usize, available: usize },
    /// The node is shedding load; retry later, on a new connection.
    ServerBusy,
    /// The node's application failed on the request; do not retry.
    EngineError,
    /// The reply came back backed by the primary's disk alone
    /// ([`Ack::PrimaryOnly`]), weaker than the node's ack policy, from a
    /// convenience method that takes only a full one. The reply's frames
    /// are here, for a caller that accepts it after all; one that does
    /// not treats the request as unconfirmed and reconciles. The
    /// connection is still good.
    Degraded { frames: Vec<Vec<u8>> },
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
                 the request is well-formed for the application version the node runs",
                timeout.as_secs_f64()
            ),
            Error::Disconnected => f.write_str("the node closed the connection"),
            Error::Protocol(what) => write!(f, "protocol violation: {what}"),
            Error::FrameTooLarge { declared, max } => write!(
                f,
                "protocol violation: the node declared a {declared}-byte frame, the limit is {max}"
            ),
            Error::RequestTooLarge { len } => write!(
                f,
                "request too large: {len} bytes with its header, the frame limit is {MAX_FRAME_SIZE}"
            ),
            Error::BufferTooSmall { needed, available } => write!(
                f,
                "request buffer too small: the frame needs {needed} bytes, the buffer holds {available}"
            ),
            Error::ServerBusy => f.write_str("the node is busy: retry later on a new connection"),
            Error::EngineError => f.write_str("the node reported an engine error; do not retry"),
            Error::Degraded { .. } => f.write_str(
                "the reply is backed by the primary's disk alone, not by the node's ack policy: \
                 the request is unconfirmed until the cluster is whole again",
            ),
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

impl From<FrameTooLarge> for Error {
    /// [`Error::FrameTooLarge`], with the declared length and the limit.
    fn from(e: FrameTooLarge) -> Self {
        Error::FrameTooLarge {
            declared: e.declared,
            max: e.max,
        }
    }
}

impl From<RequestFrameError> for Error {
    /// Variant for variant: [`Error::RequestTooLarge`] and
    /// [`Error::BufferTooSmall`].
    fn from(e: RequestFrameError) -> Self {
        match e {
            RequestFrameError::RequestTooLarge { len } => Error::RequestTooLarge { len },
            RequestFrameError::BufferTooSmall { needed, available } => {
                Error::BufferTooSmall { needed, available }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/// What backs a reply, as the end of its batch says.
///
/// A node acks a request once the copies its ack policy requires exist.
/// A primary halted for want of a replica cannot get them for the
/// requests it had already sequenced when the halt began; after a grace
/// period it answers those once its own journal holds them, and says so.
/// A query answered then may reflect such requests, and is marked the
/// same way.
///
/// The client decides what a weaker reply means: one that needs the
/// policy's guarantee treats [`PrimaryOnly`](Ack::PrimaryOnly) as
/// unconfirmed and reconciles once the cluster is whole again (a
/// failover to a node that never received the request loses it); one
/// that accepts a single disk copy treats it as done.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ack {
    /// Backed as the node's ack policy requires.
    Policy,
    /// Backed by the primary's own disk alone: weaker than the ack
    /// policy, which the node could not meet.
    PrimaryOnly,
}

/// One request's reply batch, from [`Connection::request_batch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// The application frames of the reply, in order.
    pub frames: Vec<Vec<u8>>,
    /// What backs them.
    pub ack: Ack,
}

/// One frame from the node, as [`Connection::next_frame`] hands it over.
/// Heartbeats never surface: they carry nothing and are skipped.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    /// An application response: its body, the application's bytes with
    /// the protocol's framing stripped. Borrowed from the connection's
    /// buffer, valid until the next read.
    Response(&'a [u8]),
    /// The last frame of one request's reply batch, saying what backs
    /// the reply.
    BatchEnd(Ack),
    /// The node is shedding load; nothing further will come for the
    /// request, and the connection should be dropped.
    ServerBusy,
    /// The application failed on the request.
    EngineError,
}

/// One frame from the node as [`classify`] sees it: a [`Frame`], or the
/// heartbeat that [`Connection::next_frame`] skips. A type of its own
/// rather than a variant of `Frame`, so that `next_frame`'s promise —
/// heartbeats never surface — is in its type, and no caller of it
/// matches an arm that cannot come.
#[derive(Debug, PartialEq, Eq)]
pub enum Reply<'a> {
    /// As [`Frame::Response`]: the response's body, borrowed from the
    /// payload.
    Response(&'a [u8]),
    /// The node is alive and has nothing to say. Not an answer: a caller
    /// waiting on a reply keeps its own deadline, as `next_frame` does.
    Heartbeat,
    /// As [`Frame::BatchEnd`].
    BatchEnd(Ack),
    /// As [`Frame::ServerBusy`].
    ServerBusy,
    /// As [`Frame::EngineError`].
    EngineError,
}

/// One frame's payload from the node, told apart without I/O: the
/// decision [`Connection::next_frame`] makes on each frame it reads, for
/// a program that reads its own — a gateway session on `io_uring`, a
/// load generator on a user-space TCP stack — so it need not know the
/// protocol's tags.
///
/// A tag that is not one of the six a reply may carry — the
/// handshake's tags, which are over before any reply, and any the
/// protocol does not define — is [`Error::Protocol`]. So is an empty
/// frame, and `0x00` with it: a zeroed buffer on the wire is a loud
/// error, not an application response.
pub fn classify(payload: &[u8]) -> Result<Reply<'_>, Error> {
    match payload.split_first() {
        None => Err(Error::Protocol("empty frame".into())),
        Some((&TAG_APP, body)) => Ok(Reply::Response(body)),
        Some((&TAG_RESPONSE_HEARTBEAT, _)) => Ok(Reply::Heartbeat),
        Some((&TAG_BATCH_END, _)) => Ok(Reply::BatchEnd(Ack::Policy)),
        Some((&TAG_BATCH_END_DEGRADED, _)) => Ok(Reply::BatchEnd(Ack::PrimaryOnly)),
        Some((&TAG_SERVER_BUSY, _)) => Ok(Reply::ServerBusy),
        Some((&TAG_ENGINE_ERROR, _)) => Ok(Reply::EngineError),
        Some((&tag, _)) => Err(Error::Protocol(format!(
            "unexpected tag {tag:#04x} in a response frame"
        ))),
    }
}

/// The next reply in `decoder`, classified: [`classify`] over
/// [`FrameDecoder::next`], for a program that pushes the bytes its own
/// I/O loop received into a decoder and wants replies, not raw frames.
///
/// - `Ok(Some(reply))`: one complete frame, consumed from the decoder.
///   [`Reply::Heartbeat`] is returned like any other reply, for the
///   caller to skip.
/// - `Ok(None)`: no complete frame yet; push more bytes and call again.
/// - `Err(`[`Error::FrameTooLarge`]`)`: a length prefix over the
///   decoder's limit, with the length it declared. The decoder is
///   poisoned and every later call fails the same way.
/// - `Err(`[`Error::Protocol`]`)`: a frame no reply carries, as
///   [`classify`] reports it.
///
/// Neither error is recoverable: either way the connection is to be
/// dropped. The variants tell them apart for the caller's log, in the
/// `Err` arm itself, where the decoder cannot be asked while the result
/// still borrows it.
///
/// The decoder does no I/O, so it measures no time: the read timeout,
/// and with it [`Error::NoReply`], are the caller's. So is the rule that
/// [`Connection::next_frame`] follows: a heartbeat is not an answer and
/// does not push a reply's deadline back, or a node that heartbeats
/// idle connections would keep a silently dropped request waiting
/// forever.
///
/// The handshake's frames come out of the same decoder before any reply:
/// take them with [`FrameDecoder::next`] and feed them to a
/// [`Handshake`], then switch to this function once it is done.
///
/// ```
/// use std::time::{Duration, Instant};
/// use melin_client::framing::FrameDecoder;
/// use melin_client::{Ack, Error, Reply, next_reply};
///
/// # use melin_wire_protocol::control_codec::{TAG_APP, TAG_BATCH_END, TAG_RESPONSE_HEARTBEAT};
/// # let stream = [
/// #     &[1, 0, 0, 0, TAG_RESPONSE_HEARTBEAT][..],
/// #     &[3, 0, 0, 0, TAG_APP, b'o', b'k'],
/// #     &[1, 0, 0, 0, TAG_BATCH_END],
/// # ]
/// # .concat();
/// // What an I/O loop received, in arbitrary chunks: a heartbeat, then a
/// // response and the end of its batch.
/// let received = [&stream[..6], &stream[6..9], &stream[9..]];
///
/// let deadline = Instant::now() + Duration::from_secs(5);
/// let mut decoder = FrameDecoder::new();
/// let mut responses = Vec::new();
/// 'io: for bytes in received {
///     decoder.push(bytes);
///     while let Some(reply) = next_reply(&mut decoder)? {
///         match reply {
///             // Not an answer: the deadline stands.
///             Reply::Heartbeat => {}
///             Reply::Response(body) => responses.push(body.to_vec()),
///             Reply::BatchEnd(Ack::Policy) => break 'io,
///             // Only the primary's disk holds it: this caller wants the
///             // policy's guarantee, so the request is unconfirmed.
///             Reply::BatchEnd(Ack::PrimaryOnly) => {
///                 return Err(Error::Degraded { frames: responses });
///             }
///             Reply::ServerBusy => return Err(Error::ServerBusy),
///             Reply::EngineError => return Err(Error::EngineError),
///         }
///     }
///     if Instant::now() >= deadline {
///         return Err(Error::NoReply { timeout: Duration::from_secs(5) });
///     }
/// }
/// assert_eq!(responses, [b"ok".to_vec()]);
/// # Ok::<(), Error>(())
/// ```
///
/// A program that keeps its own receive buffer rather than a decoder
/// does the same with [`framing::split_frame`] and [`classify`], with no
/// copy at all: split a frame off the front, classify its payload, and
/// advance its cursor by [`framing::Frame::consumed`].
pub fn next_reply(decoder: &mut FrameDecoder) -> Result<Option<Reply<'_>>, Error> {
    match decoder.next()? {
        Some(payload) => classify(payload).map(Some),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// An authenticated connection to a node.
///
/// Every read is bounded by the read timeout; there is no unbounded
/// read. A node drops a request it refuses without a word (see the
/// crate docs), so a read with no deadline could wait forever on a reply
/// that is never coming. The timeout bounds a wait for a reply, nothing
/// else: a connection that sends nothing reads nothing, and is not timed
/// out by this crate.
///
/// The node has a timeout of its own: it closes a connection that has
/// sent it nothing for longer than its configured connection timeout,
/// and its heartbeats to the client do not count. A client kept for
/// longer than that sends something within the window, or finds
/// [`Error::Disconnected`] on its next request.
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
        let mut stream = TcpStream::connect_timeout(&addr, timeout)
            .map_err(|source| Error::Connect { addr, source })?;
        stream.set_read_timeout(Some(timeout))?;
        // A request is one small frame and the reply is what the caller is
        // waiting for: never hold it for coalescing.
        stream.set_nodelay(true)?;
        authenticate(&mut stream, key).map_err(|e| match e {
            Error::Io(e) => io_error(e, timeout),
            other => other,
        })?;
        Ok(Connection {
            reader: BlockingFrameReader::new(stream.try_clone()?),
            writer: BlockingFrameWriter::new(stream.try_clone()?),
            stream,
            read_timeout: timeout,
            public_key: key.verifying_key(),
        })
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
    ///
    /// For a request the node may legitimately hold past
    /// [`DEFAULT_TIMEOUT`] — one waiting on the node's durability policy,
    /// say — raise the timeout rather than work around it. When it does
    /// fire the connection is to be dropped. Whether the same request can
    /// then be sent again, on a new connection, without applying it twice
    /// is the application's protocol to say — typically a per-client
    /// sequence carried in the request body.
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

    /// Send one request, the application's bytes, as an application
    /// frame, flushed.
    ///
    /// The body is copied once in user space, into the writer's buffer;
    /// there is no staging buffer in between. A body that would take the
    /// frame over [`MAX_FRAME_SIZE`] is [`Error::RequestTooLarge`], and
    /// nothing is written: the node would drop the connection on it.
    pub fn send(&mut self, body: &[u8]) -> Result<(), Error> {
        // Not built on `seal_request`: that frames into a caller's
        // buffer, and here the body would then be copied a second time,
        // into the writer's. The limit is the shared one.
        request_payload_len(body.len())?;
        self.writer.write_frame_parts(&[&[TAG_APP], body])?;
        self.writer.flush()?;
        Ok(())
    }

    /// The next frame from the node, heartbeats skipped. Blocks up to
    /// the read timeout: heartbeats do not extend it, or a node that
    /// heartbeats idle connections more often than the timeout would
    /// keep a silently dropped request waiting forever.
    pub fn next_frame(&mut self) -> Result<Frame<'_>, Error> {
        // Each read re-arms the socket's timeout, so silence is measured
        // against this deadline; a heartbeat that arrives past it is the
        // node saying "still nothing", which is what NoReply means. The
        // wait can overshoot the timeout by up to one heartbeat gap —
        // the deadline is only checked when a frame arrives.
        let deadline = Instant::now() + self.read_timeout;
        loop {
            // Decide on the classification alone, then borrow the frame
            // back for the one arm that returns it: a borrow that lives
            // across the loop and is conditionally returned is what the
            // borrow checker cannot follow.
            match classify(self.raw_frame()?)? {
                Reply::Heartbeat => {
                    if Instant::now() >= deadline {
                        return Err(Error::NoReply {
                            timeout: self.read_timeout,
                        });
                    }
                    continue;
                }
                Reply::BatchEnd(ack) => return Ok(Frame::BatchEnd(ack)),
                Reply::ServerBusy => return Ok(Frame::ServerBusy),
                Reply::EngineError => return Ok(Frame::EngineError),
                // `classify` saw the tag, so the frame holds at least it.
                Reply::Response(_) => {
                    return Ok(Frame::Response(&self.reader.frame()[TAG_LEN..]));
                }
            }
        }
    }

    /// Send one request and collect the application frames of its reply
    /// batch, in order, with what backs them. A batch may hold none (the
    /// application had nothing to say) or several (an acknowledgement and
    /// the reports the request caused, say).
    pub fn request_batch(&mut self, body: &[u8]) -> Result<Batch, Error> {
        self.send(body)?;
        let mut frames = Vec::new();
        loop {
            match self.next_frame()? {
                Frame::Response(bytes) => frames.push(bytes.to_vec()),
                Frame::BatchEnd(ack) => return Ok(Batch { frames, ack }),
                Frame::ServerBusy => return Err(Error::ServerBusy),
                Frame::EngineError => return Err(Error::EngineError),
            }
        }
    }

    /// [`request_batch`](Self::request_batch) for a caller that takes
    /// only a reply backed as the node's ack policy requires: the frames
    /// of a batch that ends in [`Ack::Policy`]. A batch only the
    /// primary's disk backs is [`Error::Degraded`], which carries its
    /// frames, so that a weaker reply is never taken for a full one
    /// unawares.
    pub fn request(&mut self, body: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        let Batch { frames, ack } = self.request_batch(body)?;
        match ack {
            Ack::Policy => Ok(frames),
            Ack::PrimaryOnly => Err(Error::Degraded { frames }),
        }
    }

    /// [`request`](Self::request) for the common case of exactly one
    /// frame in reply; any other count is a protocol error.
    pub fn request_one(&mut self, body: &[u8]) -> Result<Vec<u8>, Error> {
        let mut frames = self.request(body)?;
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
    /// connecting: the handshake reads nothing past the node's answer,
    /// but anything a [`next_frame`](Self::next_frame) since buffered is
    /// discarded with the reader.
    pub fn into_stream(self) -> TcpStream {
        self.stream
    }

    /// One frame's payload, with the transport's outcomes mapped: a clean
    /// close is [`Error::Disconnected`], a timed-out read is
    /// [`Error::NoReply`].
    // Kept on the blocking reader rather than a `FrameDecoder`: the
    // reader parses in its own read buffer, where a decoder would need a
    // second copy of every byte read off the socket.
    fn raw_frame(&mut self) -> Result<&[u8], Error> {
        let timeout = self.read_timeout;
        match self.reader.read_frame() {
            Ok(Some(frame)) => Ok(frame),
            Ok(None) => Err(Error::Disconnected),
            Err(e) => Err(io_error(e, timeout)),
        }
    }
}

/// A read that failed under a socket timeout of `timeout` is the node's
/// silence, [`Error::NoReply`]; an oversized length prefix, which the
/// blocking reader reports wrapped in an I/O error, is
/// [`Error::FrameTooLarge`] as from [`next_reply`]; anything else is the
/// socket failing.
fn io_error(e: io::Error, timeout: Duration) -> Error {
    match e.kind() {
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => Error::NoReply { timeout },
        io::ErrorKind::InvalidData => {
            match e
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<FrameTooLarge>())
            {
                Some(&too_large) => too_large.into(),
                None => Error::Io(e),
            }
        }
        _ => Error::Io(e),
    }
}

// ---------------------------------------------------------------------------
// The handshake on its own
// ---------------------------------------------------------------------------

/// The client's half of the handshake with no I/O in it: feed it the
/// node's frames, send what it hands back. For a program whose frames
/// arrive through an I/O loop of its own — a gateway session, a load
/// generator on a user-space TCP stack — so it need not carry the
/// handshake's layout; [`authenticate`] drives it over a blocking
/// stream, and [`Connection::connect`] goes through that.
///
/// The node sends a challenge, the client answers with the nonce signed
/// and its public key, and the node says it is ready or refuses the key.
/// [`feed`](Self::feed) takes each of the node's frames in turn.
///
/// Owns a copy of the key, so it can sit in the struct that owns the
/// original across the two frames; the copy is dropped, and zeroed, as
/// soon as it has signed, since the verdict needs only the public key.
pub struct Handshake {
    /// Raw rather than a [`VerifyingKey`], as in [`Error::AuthFailed`]:
    /// it goes on the wire and into that error, and nothing verifies
    /// with it.
    public_key: [u8; 32],
    state: HandshakeState,
    /// The challenge response with its length prefix, once the nonce is
    /// known: held here so [`Step::Send`] can borrow it, and a caller
    /// writes it as one piece.
    response: [u8; PREFIX_LEN + CHALLENGE_RESPONSE_LEN],
}

impl fmt::Debug for Handshake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let awaiting = match self.state {
            HandshakeState::Challenge { .. } => "challenge",
            HandshakeState::Verdict => "verdict",
            HandshakeState::Done => "nothing",
        };
        f.debug_struct("Handshake")
            .field(
                "public_key",
                &base64::engine::general_purpose::STANDARD.encode(self.public_key),
            )
            .field("awaiting", &awaiting)
            .finish_non_exhaustive()
    }
}

/// Which of the node's frames [`Handshake`] is waiting for, carrying
/// what that step needs: the key lives in the state that signs, and
/// leaving that state drops it.
// The key is what makes the signing state wide; there is one handshake
// per connection, never a table of them, and boxing the key would put
// secret material on the heap for no size that matters.
#[expect(clippy::large_enum_variant)]
enum HandshakeState {
    Challenge { key: SigningKey },
    Verdict,
    Done,
}

/// What a [`Handshake`] wants done after a frame.
#[derive(Debug, PartialEq, Eq)]
pub enum Step<'a> {
    /// Send these bytes to the node as they are, length prefix included,
    /// then feed the node's next frame.
    Send(&'a [u8]),
    /// The node accepted the key: the stream is the caller's from here.
    Ready,
}

impl Handshake {
    /// A handshake that will prove `key`, from a copy of it.
    pub fn new(key: &SigningKey) -> Self {
        Handshake {
            public_key: key.verifying_key().to_bytes(),
            state: HandshakeState::Challenge { key: key.clone() },
            response: [0u8; PREFIX_LEN + CHALLENGE_RESPONSE_LEN],
        }
    }

    /// Whether the node has said it is ready: there is nothing more to
    /// feed, and the stream is the caller's.
    pub fn is_done(&self) -> bool {
        matches!(self.state, HandshakeState::Done)
    }

    /// One frame from the node, its payload after the length prefix.
    /// The first must be the challenge and the answer is
    /// [`Step::Send`]; the second is the node's verdict, [`Step::Ready`]
    /// or [`Error::AuthFailed`]. Anything else, in either place, is
    /// [`Error::Protocol`], as is a frame after the handshake is done.
    pub fn feed(&mut self, payload: &[u8]) -> Result<Step<'_>, Error> {
        match &self.state {
            HandshakeState::Challenge { key } => {
                // `[tag][nonce: 32]`
                let nonce: [u8; 32] = match payload {
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
                    public_key: self.public_key,
                };
                self.response[..PREFIX_LEN]
                    .copy_from_slice(&(CHALLENGE_RESPONSE_LEN as u32).to_le_bytes());
                encode_challenge_response(&response, &mut self.response[PREFIX_LEN..]).map_err(
                    |e| Error::Protocol(format!("cannot encode the challenge response: {e}")),
                )?;
                self.state = HandshakeState::Verdict;
                Ok(Step::Send(&self.response))
            }
            HandshakeState::Verdict => match payload.first() {
                Some(&TAG_SERVER_READY) => {
                    self.state = HandshakeState::Done;
                    Ok(Step::Ready)
                }
                Some(&TAG_AUTH_FAILED) => Err(Error::AuthFailed {
                    public_key: self.public_key,
                }),
                other => Err(Error::Protocol(format!(
                    "expected the node to be ready or to refuse the key, got tag {other:?}"
                ))),
            },
            HandshakeState::Done => Err(Error::Protocol(
                "a frame fed to a handshake that is complete".to_string(),
            )),
        }
    }
}

/// Answer a node's challenge on a bare stream: read the nonce, sign it,
/// send the signature with the public key, and wait for the node to say
/// it is ready. [`Connection::connect`] does this on the socket it
/// dials; the function is for a program that owns its own — a load
/// generator whose I/O loop takes the descriptor over once the
/// handshake is done, a client on a Unix socket — so it need not carry
/// a copy of the handshake. A program that reads its frames itself
/// drives a [`Handshake`] directly, as this function does.
///
/// The reads are unbuffered: nothing past the node's answer is taken
/// from the stream, so whatever arrives next (a heartbeat, say) is
/// there for the caller's own reader. The stream is not this function's
/// to configure: without a read timeout of the caller's own (a socket's
/// `set_read_timeout`) a peer that never answers hangs the handshake,
/// and a read that times out under one comes back as [`Error::Io`].
pub fn authenticate(stream: &mut (impl Read + Write), key: &SigningKey) -> Result<(), Error> {
    // The node's two frames are a tag and a 32-byte nonce, then a tag: a
    // frame that does not fit here is not a handshake.
    let mut buf = [0u8; 64];
    let mut handshake = Handshake::new(key);
    loop {
        let payload = read_unbuffered_frame(stream, &mut buf)?;
        match handshake.feed(payload)? {
            Step::Send(frame) => {
                // There is no write buffer in front of a bare stream, so
                // the frame goes out as one write.
                stream.write_all(frame)?;
                stream.flush()?;
            }
            Step::Ready => return Ok(()),
        }
    }
}

/// One frame read straight off `stream` into `buf`, with the node's
/// close reported as [`Error::Disconnected`], a prefix over
/// [`MAX_FRAME_SIZE`] as [`Error::FrameTooLarge`], and any other frame
/// `buf` cannot hold refused as [`Error::Protocol`], before any of it is
/// read.
// Not built on the splitter: it reads the prefix and then exactly the
// payload, so no byte past the frame leaves the stream, where splitting
// needs the bytes read into a buffer first.
fn read_unbuffered_frame<'a>(stream: &mut impl Read, buf: &'a mut [u8]) -> Result<&'a [u8], Error> {
    let closed = |e: io::Error| match e.kind() {
        io::ErrorKind::UnexpectedEof => Error::Disconnected,
        _ => Error::Io(e),
    };
    let mut prefix = [0u8; PREFIX_LEN];
    stream.read_exact(&mut prefix).map_err(closed)?;
    let declared = u32::from_le_bytes(prefix);
    let len = declared as usize;
    // A prefix over the frame limit is the same error as after the
    // handshake; one within it but too long for a handshake frame is the
    // node breaking the handshake.
    if len > MAX_FRAME_SIZE {
        return Err(Error::FrameTooLarge {
            declared,
            max: MAX_FRAME_SIZE,
        });
    }
    let Some(frame) = buf.get_mut(..len) else {
        return Err(Error::Protocol(format!(
            "a {len}-byte frame where a handshake frame was expected"
        )));
    };
    stream.read_exact(frame).map_err(closed)?;
    Ok(frame)
}

// ---------------------------------------------------------------------------
// Tests, against a fake node
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;
    use std::os::unix::net::UnixStream;

    use ed25519_dalek::{Signature, Verifier};
    use melin_wire_protocol::control::TransportResponse;
    use melin_wire_protocol::control_codec::{
        decode_challenge_response, encode_transport_response,
    };

    use super::*;

    /// How the fake node behaves once a client is authenticated.
    #[derive(Clone, Copy)]
    enum Behaviour {
        /// Reply to every request with its body, then end the batch.
        Echo,
        /// As `Echo`, with every batch backed by the primary's disk
        /// alone: what a halted primary sends for a request it held.
        DegradedEcho,
        /// A heartbeat before every reply, and two reply frames per batch.
        ChattyEcho,
        /// Read requests and never answer.
        Silent,
        /// Never answer, but heartbeat steadily — the idle-connection
        /// heartbeats a real node sends.
        HeartbeatingSilent,
        /// A heartbeat on the heels of `ServerReady`, before any request,
        /// then `Echo`.
        EagerHeartbeat,
        /// Answer every request with a zero-tagged frame — what a
        /// zeroed or corrupt buffer looks like on the wire.
        ZeroTag,
        /// Answer every request with a length prefix over the frame
        /// limit, and nothing after it.
        Oversized,
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

    /// A frame under any tag, length prefix included.
    fn tagged_frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = ((TAG_LEN + body.len()) as u32).to_le_bytes().to_vec();
        frame.push(tag);
        frame.extend_from_slice(body);
        frame
    }

    /// An application frame carrying `body`, length prefix included.
    fn app_frame(body: &[u8]) -> Vec<u8> {
        tagged_frame(TAG_APP, body)
    }

    /// The node's half of the handshake on `stream`, any kind of stream:
    /// challenge, verify, then ready or refused. Whether the key was
    /// accepted.
    fn challenge<S>(stream: &S, allowed: VerifyingKey) -> bool
    where
        for<'a> &'a S: Read + Write,
    {
        let nonce = [0x5A; 32];
        let mut writer = stream;
        writer
            .write_all(&control(TransportResponse::Challenge { nonce }))
            .unwrap();
        let mut reader = BlockingFrameReader::new(stream);
        let response = decode_challenge_response(reader.read_frame().unwrap().unwrap()).unwrap();
        let presented = VerifyingKey::from_bytes(&response.public_key).unwrap();
        let signature = Signature::from_bytes(&response.signature);
        if presented != allowed || presented.verify(&nonce, &signature).is_err() {
            writer
                .write_all(&control(TransportResponse::AuthFailed))
                .unwrap();
            return false;
        }
        writer
            .write_all(&control(TransportResponse::ServerReady))
            .unwrap();
        true
    }

    /// Serve one client on `stream` the way a node does: challenge,
    /// verify, then `behaviour`.
    fn serve(mut stream: TcpStream, allowed: VerifyingKey, behaviour: Behaviour) {
        if !challenge(&stream, allowed) {
            return;
        }
        // A client sends nothing before it is told the node is ready, so
        // the handshake's reader cannot have buffered a request.
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());

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
            Behaviour::EagerHeartbeat => {
                stream
                    .write_all(&control(TransportResponse::Heartbeat))
                    .unwrap();
            }
            _ => {}
        }
        while let Ok(Some(request)) = reader.read_frame() {
            // `[tag][body]`
            assert_eq!(request[0], TAG_APP);
            let body = request[TAG_LEN..].to_vec();
            let reply: Vec<u8> = match behaviour {
                Behaviour::Echo | Behaviour::EagerHeartbeat => {
                    [app_frame(&body), control(TransportResponse::BatchEnd)].concat()
                }
                Behaviour::DegradedEcho => [
                    app_frame(&body),
                    control(TransportResponse::BatchEndDegraded),
                ]
                .concat(),
                Behaviour::ChattyEcho => [
                    control(TransportResponse::Heartbeat),
                    app_frame(&body),
                    app_frame(b"again"),
                    control(TransportResponse::BatchEnd),
                ]
                .concat(),
                Behaviour::Silent => continue,
                Behaviour::HeartbeatingSilent => {
                    // Heartbeat until the client gives up and drops the
                    // connection.
                    while stream
                        .write_all(&control(TransportResponse::Heartbeat))
                        .is_ok()
                    {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    return;
                }
                Behaviour::ZeroTag => tagged_frame(0x00, b"looks zeroed"),
                Behaviour::Oversized => ((MAX_FRAME_SIZE + 1) as u32).to_le_bytes().to_vec(),
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

        let reply = node.request_one(b"hello").unwrap();
        assert_eq!(reply, b"hello");

        // The same over the pipelined pair, several requests in flight.
        for n in 2..=4u64 {
            node.send(&n.to_le_bytes()).unwrap();
        }
        for n in 2..=4u64 {
            assert_eq!(
                node.next_frame().unwrap(),
                Frame::Response(&n.to_le_bytes())
            );
            assert_eq!(node.next_frame().unwrap(), Frame::BatchEnd(Ack::Policy));
        }

        let batch = node.request_batch(b"backed").unwrap();
        assert_eq!(batch.frames, [b"backed".to_vec()]);
        assert_eq!(batch.ack, Ack::Policy);
    }

    /// A reply only the primary's disk backs says so on every path: the
    /// frame, the batch, and the convenience methods, which never pass it
    /// off as a full one.
    #[test]
    fn a_degraded_reply_is_marked_and_never_taken_for_a_full_one() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::DegradedEcho);
        let mut node = Connection::connect(addr, &key).unwrap();

        node.send(b"held").unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::Response(b"held"));
        assert_eq!(
            node.next_frame().unwrap(),
            Frame::BatchEnd(Ack::PrimaryOnly)
        );

        let batch = node.request_batch(b"held").unwrap();
        assert_eq!(batch.frames, [b"held".to_vec()]);
        assert_eq!(batch.ack, Ack::PrimaryOnly);

        let err = node.request_one(b"held").unwrap_err();
        assert!(
            matches!(&err, Error::Degraded { frames } if frames == &[b"held".to_vec()]),
            "{err:?}"
        );
        assert!(err.to_string().contains("primary's disk"), "{err}");
        // The connection is still good.
        assert_eq!(
            node.request_batch(b"after").unwrap().frames,
            [b"after".to_vec()]
        );
    }

    /// The body is the application's from its first byte: one equal to a
    /// protocol tag, or no byte at all, goes out and comes back as it is.
    #[test]
    fn a_body_is_carried_whatever_its_bytes() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut node = Connection::connect(addr, &key).unwrap();
        for body in [
            &[][..],
            &[0x00],
            &[TAG_BATCH_END],
            &[TAG_APP, TAG_APP],
            &[TAG_SERVER_BUSY, 1, 2, 3],
        ] {
            assert_eq!(node.request_one(body).unwrap(), body);
        }
    }

    #[test]
    fn heartbeats_are_skipped_and_a_batch_may_carry_several_frames() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::ChattyEcho);
        let mut node = Connection::connect(addr, &key).unwrap();

        let frames = node.request(b"x").unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], b"x");
        assert_eq!(frames[1], b"again");

        assert!(matches!(node.request_one(b"y"), Err(Error::Protocol(_))));
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
        let err = node.request(b"dropped").unwrap_err();
        assert!(started.elapsed() >= timeout);
        assert!(matches!(err, Error::NoReply { timeout: t } if t == timeout));
        assert!(err.to_string().contains("authorized_keys"), "{err}");
    }

    #[test]
    fn silence_during_the_handshake_is_no_reply_too() {
        // A listener whose backlog took the connection before anything
        // serves it: the socket is open, the challenge never comes. The
        // handshake's timeout is the node's silence, the same error a
        // dropped request gives — which is what `connect_by` retries.
        let key = client_key();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let timeout = Duration::from_millis(200);
        std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            // Hold the socket open past the client's timeout.
            std::thread::sleep(timeout * 4);
        });

        let started = Instant::now();
        let err = Connection::connect_timeout(addr, &key, timeout).unwrap_err();
        assert!(started.elapsed() >= timeout);
        assert!(matches!(err, Error::NoReply { timeout: t } if t == timeout));
    }

    #[test]
    fn an_oversized_request_is_refused_and_the_connection_kept() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut node = Connection::connect(addr, &key).unwrap();

        // One byte over the widest body a frame takes once the tag is
        // counted.
        let body = vec![0xAB; MAX_FRAME_SIZE];
        let err = node.send(&body).unwrap_err();
        assert!(matches!(err, Error::RequestTooLarge { len } if len == MAX_FRAME_SIZE + 1));
        assert!(err.to_string().contains("request too large"), "{err}");

        // Nothing reached the node, so the connection is as good as new,
        // and the widest body that fits goes through it.
        assert_eq!(node.request_one(b"still here").unwrap(), b"still here");
        assert_eq!(node.request_one(&body[1..]).unwrap(), &body[1..]);
    }

    #[test]
    fn a_zero_tag_is_a_protocol_error_not_a_response() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::ZeroTag);
        let mut node = Connection::connect(addr, &key).unwrap();
        node.send(b"").unwrap();
        assert!(matches!(node.next_frame(), Err(Error::Protocol(_))));
    }

    #[test]
    fn heartbeats_do_not_defer_no_reply_forever() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::HeartbeatingSilent);
        let timeout = Duration::from_millis(200);
        let mut node = Connection::connect_timeout(addr, &key, timeout).unwrap();

        let started = Instant::now();
        let err = node.request(b"dropped").unwrap_err();
        assert!(started.elapsed() >= timeout);
        // The deadline is checked as each heartbeat arrives, so the
        // wait ends within a heartbeat gap of the timeout — the node's
        // heartbeats must not keep a dropped request waiting forever.
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(matches!(err, Error::NoReply { timeout: t } if t == timeout));
    }

    #[test]
    fn busy_and_engine_error_are_frames_when_pipelining_and_errors_from_request() {
        let key = client_key();

        let addr = fake_node(key.verifying_key(), Behaviour::Busy);
        let mut node = Connection::connect(addr, &key).unwrap();
        node.send(b"").unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::ServerBusy);
        assert!(matches!(node.request(b""), Err(Error::ServerBusy)));

        let addr = fake_node(key.verifying_key(), Behaviour::Failing);
        let mut node = Connection::connect(addr, &key).unwrap();
        node.send(b"").unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::EngineError);
        assert!(matches!(node.request(b""), Err(Error::EngineError)));
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
        assert_eq!(node.request_one(b"up").unwrap(), b"up");
    }

    #[test]
    fn connect_by_gives_up_at_the_deadline() {
        let key = client_key();
        // A port bound but never listened on refuses every connect, and
        // stays this test's for as long as `refusing` lives. A port
        // merely freed could be taken by another test process before
        // the connect, and the handshake timeout, not the refusal, would
        // then be what the client reports.
        let refusing =
            socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None).unwrap();
        refusing
            .bind(&"127.0.0.1:0".parse::<SocketAddr>().unwrap().into())
            .unwrap();
        let addr = refusing.local_addr().unwrap().as_socket().unwrap();
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

    /// A socket to `addr` with no protocol on it, bounded so a fake that
    /// does not answer fails the test rather than hanging it.
    fn bare_stream(addr: SocketAddr) -> TcpStream {
        let stream = TcpStream::connect(addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
    }

    /// One frame read by hand off a bare stream: `[len: u32][payload]`.
    fn read_raw_frame(stream: &mut impl Read) -> Vec<u8> {
        let mut prefix = [0u8; 4];
        stream.read_exact(&mut prefix).unwrap();
        let mut payload = vec![0u8; u32::from_le_bytes(prefix) as usize];
        stream.read_exact(&mut payload).unwrap();
        payload
    }

    #[test]
    fn authenticate_serves_a_bare_stream() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut stream = bare_stream(addr);
        authenticate(&mut stream, &key).unwrap();

        // The stream is the caller's from here: a request framed by hand
        // gets its reply batch.
        stream.write_all(&app_frame(b"raw")).unwrap();
        assert_eq!(
            read_raw_frame(&mut stream),
            [&[TAG_APP][..], b"raw"].concat()
        );
        assert_eq!(read_raw_frame(&mut stream), [TAG_BATCH_END]);
    }

    #[test]
    fn authenticate_leaves_what_follows_in_the_stream() {
        // The node heartbeats as soon as the client is authenticated. The
        // handshake must not have swallowed it into a buffer of its own:
        // the caller's reader gets it, and then the reply to a request.
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::EagerHeartbeat);
        let mut stream = bare_stream(addr);
        authenticate(&mut stream, &key).unwrap();
        assert_eq!(read_raw_frame(&mut stream), [TAG_RESPONSE_HEARTBEAT]);

        stream.write_all(&app_frame(b"after")).unwrap();
        assert_eq!(
            read_raw_frame(&mut stream),
            [&[TAG_APP][..], b"after"].concat()
        );
    }

    #[test]
    fn authenticate_works_over_a_unix_socket() {
        let key = client_key();
        let allowed = key.verifying_key();

        let (mut client, node) = UnixStream::pair().unwrap();
        let accepted = std::thread::spawn(move || challenge(&node, allowed));
        authenticate(&mut client, &key).unwrap();
        assert!(accepted.join().unwrap());

        let (mut client, node) = UnixStream::pair().unwrap();
        let other = SigningKey::from_bytes(&[0x22; 32]).verifying_key();
        let accepted = std::thread::spawn(move || challenge(&node, other));
        assert!(matches!(
            authenticate(&mut client, &key),
            Err(Error::AuthFailed { public_key }) if public_key == allowed.to_bytes()
        ));
        assert!(!accepted.join().unwrap());
    }

    #[test]
    fn authenticate_reports_a_stream_that_is_not_a_node() {
        let key = client_key();

        // Accepted and closed without a word.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || drop(listener.accept().unwrap()));
        let mut stream = bare_stream(addr);
        assert!(matches!(
            authenticate(&mut stream, &key),
            Err(Error::Disconnected)
        ));

        // Closed in the middle of a frame: a challenge's prefix and half
        // its payload, then nothing.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let challenge = control(TransportResponse::Challenge { nonce: [0x5A; 32] });
            stream.write_all(&challenge[..challenge.len() / 2]).unwrap();
        });
        let mut stream = bare_stream(addr);
        assert!(matches!(
            authenticate(&mut stream, &key),
            Err(Error::Disconnected)
        ));

        // A frame far larger than any handshake frame, refused before it
        // is read.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.write_all(&512u32.to_le_bytes()).unwrap();
            // Keep the socket open until the client has answered.
            std::thread::sleep(Duration::from_millis(200));
        });
        let mut stream = bare_stream(addr);
        assert!(matches!(
            authenticate(&mut stream, &key),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn handshake_read_reports_a_prefix_over_the_frame_limit_as_frame_too_large() {
        let mut buf = [0u8; 64];

        // Over the frame limit: the same error as after the handshake.
        let declared = MAX_FRAME_SIZE as u32 + 1;
        let prefix = declared.to_le_bytes();
        let err = read_unbuffered_frame(&mut &prefix[..], &mut buf).unwrap_err();
        assert!(matches!(
            err,
            Error::FrameTooLarge { declared: d, max: MAX_FRAME_SIZE } if d == declared
        ));

        // Exactly at the limit: no handshake frame, but within the limit.
        let prefix = (MAX_FRAME_SIZE as u32).to_le_bytes();
        let err = read_unbuffered_frame(&mut &prefix[..], &mut buf).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)));
    }

    /// A node's control frame as a `Handshake` is fed it: the payload
    /// after the length prefix.
    fn payload(response: TransportResponse) -> Vec<u8> {
        control(response)[4..].to_vec()
    }

    #[test]
    fn handshake_answers_the_challenge_and_finishes() {
        let key = client_key();
        let nonce = [0x5A; 32];
        let mut handshake = Handshake::new(&key);
        assert!(!handshake.is_done());
        // Debug says which key and where it stands, never the secret.
        let shown = format!("{handshake:?}");
        assert!(
            shown.contains(&key::public_key_base64(&key.verifying_key())),
            "{shown}"
        );
        assert!(shown.contains("challenge"), "{shown}");

        let Step::Send(frame) = handshake
            .feed(&payload(TransportResponse::Challenge { nonce }))
            .unwrap()
        else {
            panic!("a challenge wants an answer");
        };
        // Prefixed, and the frame a node decodes: the nonce signed by the
        // key, the key.
        assert_eq!(frame.len(), 4 + CHALLENGE_RESPONSE_LEN);
        assert_eq!(
            u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize,
            CHALLENGE_RESPONSE_LEN
        );
        let response = decode_challenge_response(&frame[4..]).unwrap();
        assert_eq!(response.public_key, key.verifying_key().to_bytes());
        key.verifying_key()
            .verify(&nonce, &Signature::from_bytes(&response.signature))
            .unwrap();

        assert_eq!(
            handshake
                .feed(&payload(TransportResponse::ServerReady))
                .unwrap(),
            Step::Ready
        );
        // Done is done: a heartbeat that follows is the caller's.
        assert!(handshake.is_done());
        assert!(matches!(
            handshake.feed(&payload(TransportResponse::Heartbeat)),
            Err(Error::Protocol(_))
        ));
        assert!(handshake.is_done());
    }

    #[test]
    fn handshake_outlives_the_key_it_was_given() {
        // The shape a session wants: the key it owns goes away, or moves,
        // while the handshake it started waits for the node's next frame.
        let allowed = client_key().verifying_key();
        let mut handshake = {
            let key = client_key();
            Handshake::new(&key)
        };
        let Step::Send(frame) = handshake
            .feed(&payload(TransportResponse::Challenge { nonce: [7; 32] }))
            .unwrap()
        else {
            panic!("a challenge wants an answer");
        };
        let response = decode_challenge_response(&frame[4..]).unwrap();
        allowed
            .verify(&[7; 32], &Signature::from_bytes(&response.signature))
            .unwrap();
        assert_eq!(
            handshake
                .feed(&payload(TransportResponse::ServerReady))
                .unwrap(),
            Step::Ready
        );
    }

    #[test]
    fn handshake_reports_a_refused_key() {
        let key = client_key();
        let mut handshake = Handshake::new(&key);
        handshake
            .feed(&payload(TransportResponse::Challenge { nonce: [0; 32] }))
            .unwrap();
        assert!(matches!(
            handshake.feed(&payload(TransportResponse::AuthFailed)),
            Err(Error::AuthFailed { public_key }) if public_key == key.verifying_key().to_bytes()
        ));
    }

    #[test]
    fn handshake_refuses_a_frame_out_of_place() {
        let key = client_key();

        // Anything but a challenge first: a verdict, a heartbeat, a
        // challenge with a short nonce, nothing.
        for wrong in [
            payload(TransportResponse::ServerReady),
            payload(TransportResponse::Heartbeat),
            vec![TAG_CHALLENGE; 32],
            Vec::new(),
        ] {
            let mut handshake = Handshake::new(&key);
            assert!(
                matches!(handshake.feed(&wrong), Err(Error::Protocol(_))),
                "{wrong:?}"
            );
        }

        // Anything but a verdict second.
        let mut handshake = Handshake::new(&key);
        handshake
            .feed(&payload(TransportResponse::Challenge { nonce: [0; 32] }))
            .unwrap();
        assert!(matches!(
            handshake.feed(&payload(TransportResponse::Heartbeat)),
            Err(Error::Protocol(_))
        ));
        assert!(matches!(
            handshake.feed(&payload(TransportResponse::Challenge { nonce: [1; 32] })),
            Err(Error::Protocol(_))
        ));
    }

    #[test]
    fn classify_tells_each_reply_apart() {
        assert_eq!(
            classify(&payload(TransportResponse::Heartbeat)).unwrap(),
            Reply::Heartbeat
        );
        assert_eq!(
            classify(&payload(TransportResponse::BatchEnd)).unwrap(),
            Reply::BatchEnd(Ack::Policy)
        );
        assert_eq!(
            classify(&payload(TransportResponse::BatchEndDegraded)).unwrap(),
            Reply::BatchEnd(Ack::PrimaryOnly)
        );
        assert_eq!(
            classify(&payload(TransportResponse::ServerBusy)).unwrap(),
            Reply::ServerBusy
        );
        assert_eq!(
            classify(&payload(TransportResponse::EngineError)).unwrap(),
            Reply::EngineError
        );

        // A response is handed back as its body, the tag stripped, and
        // the body may start with any byte or be empty.
        let response = app_frame(b"body");
        assert_eq!(classify(&response[4..]).unwrap(), Reply::Response(b"body"));
        for body in [&[][..], &[0x00], &[TAG_BATCH_END], &[TAG_APP]] {
            assert_eq!(
                classify(&app_frame(body)[4..]).unwrap(),
                Reply::Response(body)
            );
        }
    }

    #[test]
    fn classify_refuses_what_a_reply_never_carries() {
        // Nothing, a zeroed frame, the handshake's frames, a tag the
        // protocol does not define, and an application tag as a
        // pre-release build framed one.
        assert!(matches!(classify(&[]), Err(Error::Protocol(_))));
        for wrong in [
            vec![0x00],
            [&[0x00][..], b"looks zeroed"].concat(),
            payload(TransportResponse::Challenge { nonce: [0x5A; 32] }),
            payload(TransportResponse::AuthFailed),
            payload(TransportResponse::ServerReady),
            vec![0x0F],
            vec![0x10, 0x01],
        ] {
            let err = classify(&wrong).unwrap_err();
            assert!(matches!(err, Error::Protocol(_)), "{wrong:?}: {err}");
            assert!(
                err.to_string().contains("unexpected tag"),
                "{wrong:?}: {err}"
            );
        }
    }

    // --- the I/O-free path ---

    /// `frame_request` with an encoder that copies `body`, under the
    /// crate's own error type.
    fn frame_body(buf: &mut [u8], body: &[u8]) -> Result<usize, Error> {
        frame_request(buf, |out| {
            let Some(dst) = out.get_mut(..body.len()) else {
                return Err(Error::Protocol("encoder: no room".into()));
            };
            dst.copy_from_slice(body);
            Ok(body.len())
        })
    }

    #[test]
    fn next_reply_surfaces_heartbeats_and_waits_for_whole_frames() {
        let stream = [
            control(TransportResponse::Heartbeat),
            app_frame(b"body"),
            control(TransportResponse::BatchEnd),
            control(TransportResponse::BatchEndDegraded),
            control(TransportResponse::ServerBusy),
            control(TransportResponse::EngineError),
        ]
        .concat();
        let mut decoder = FrameDecoder::new();
        assert!(next_reply(&mut decoder).unwrap().is_none());

        // Byte by byte: nothing until each frame is whole, then exactly it.
        let mut replies = Vec::new();
        for byte in &stream {
            decoder.push(std::slice::from_ref(byte));
            while let Some(reply) = next_reply(&mut decoder).unwrap() {
                replies.push(format!("{reply:?}"));
            }
        }
        assert_eq!(
            replies,
            [
                format!("{:?}", Reply::Heartbeat),
                format!("{:?}", Reply::Response(b"body")),
                format!("{:?}", Reply::BatchEnd(Ack::Policy)),
                format!("{:?}", Reply::BatchEnd(Ack::PrimaryOnly)),
                format!("{:?}", Reply::ServerBusy),
                format!("{:?}", Reply::EngineError),
            ]
        );
        assert!(decoder.pending().is_empty());
    }

    #[test]
    fn next_reply_refuses_an_oversized_prefix_for_good() {
        let mut decoder = FrameDecoder::new();
        decoder.push(&app_frame(b"ok"));
        decoder.push(&((MAX_FRAME_SIZE + 1) as u32).to_le_bytes());
        assert_eq!(
            next_reply(&mut decoder).unwrap(),
            Some(Reply::Response(b"ok"))
        );
        // Its own variant, told apart in the `Err` arm with no need to ask
        // the decoder, and carrying the length the prefix declared.
        let declared = MAX_FRAME_SIZE as u32 + 1;
        match next_reply(&mut decoder) {
            Err(Error::FrameTooLarge { declared: d, max }) => {
                assert_eq!((d, max), (declared, MAX_FRAME_SIZE));
            }
            other => panic!("expected FrameTooLarge, got {other:?}"),
        }
        assert!(decoder.is_poisoned());
        // Poisoned: more bytes change nothing, and the length is the
        // original one.
        decoder.push(&app_frame(b"later"));
        let err = next_reply(&mut decoder).unwrap_err();
        assert!(
            matches!(err, Error::FrameTooLarge { declared: d, .. } if d == declared),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            format!(
                "protocol violation: the node declared a {declared}-byte frame, the limit is {MAX_FRAME_SIZE}"
            )
        );

        // A decoder with a tighter limit reports that limit.
        let mut decoder = FrameDecoder::with_max_payload(16);
        decoder.push(&u32::MAX.to_le_bytes());
        assert!(matches!(
            next_reply(&mut decoder),
            Err(Error::FrameTooLarge {
                declared: u32::MAX,
                max: 16
            })
        ));
    }

    #[test]
    fn next_reply_refuses_a_frame_no_reply_carries() {
        let mut decoder = FrameDecoder::new();
        decoder.push(&tagged_frame(0x00, b"zeroed"));
        assert!(matches!(next_reply(&mut decoder), Err(Error::Protocol(_))));
    }

    #[test]
    fn request_framing_is_the_wire_protocols_reexported() {
        // The same items, reachable from this crate's root and through
        // its `framing` module.
        assert_eq!(REQUEST_HEADER_LEN, framing::REQUEST_HEADER_LEN);
        assert_eq!(REQUEST_HEADER_LEN, PREFIX_LEN + TAG_LEN);
        assert_eq!(MAX_REQUEST_BODY, framing::MAX_REQUEST_BODY);
        assert_eq!(MAX_REQUEST_BODY, MAX_FRAME_SIZE - TAG_LEN);
        // Compiles only if the root's names are the module's items.
        let _: fn(&mut [u8], usize) -> Result<usize, framing::RequestFrameError> = seal_request;
        let _: fn(&mut [u8]) -> Result<&mut [u8], RequestFrameError> = framing::request_body;

        // The closure-free form through the root names.
        let mut buf = [0xEEu8; 32];
        let body = request_body(&mut buf).unwrap();
        body[..3].copy_from_slice(b"abc");
        let n = seal_request(&mut buf, 3).unwrap();
        assert_eq!(&buf[..n], app_frame(b"abc").as_slice());
        assert!(buf[n..].iter().all(|&b| b == 0xEE));
    }

    #[test]
    fn request_framing_errors_map_into_the_clients_error() {
        // With this crate's `Error` as the encoder's error type, each
        // framing failure arrives as its own variant.
        let mut wide = vec![0x5C; REQUEST_HEADER_LEN + MAX_FRAME_SIZE + 16];
        let err = frame_request(&mut wide, |_| Ok::<_, Error>(MAX_REQUEST_BODY + 1)).unwrap_err();
        assert!(
            matches!(err, Error::RequestTooLarge { len } if len == MAX_FRAME_SIZE + 1),
            "{err:?}"
        );
        assert!(err.to_string().contains("request too large"), "{err}");

        let mut short = [0u8; 2];
        let err = frame_request(&mut short, |_| -> Result<usize, Error> {
            panic!("the encoder must not run without room for the header")
        })
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::BufferTooSmall {
                    needed: REQUEST_HEADER_LEN,
                    available: 2
                }
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains("too small"), "{err}");

        let mut buf = [0u8; REQUEST_HEADER_LEN + 4];
        assert!(matches!(
            seal_request(&mut buf, 5).map_err(Error::from),
            Err(Error::BufferTooSmall { needed, available })
                if needed == REQUEST_HEADER_LEN + 5 && available == REQUEST_HEADER_LEN + 4
        ));
        // An encoder's own failure passes through untouched.
        assert!(matches!(
            frame_request(&mut buf, |_| Err(Error::Protocol("mine".into()))),
            Err(Error::Protocol(what)) if what == "mine"
        ));
    }

    #[test]
    fn an_oversized_frame_on_a_connection_is_frame_too_large() {
        // The blocking path reports what `next_reply` reports.
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Oversized);
        let mut node = Connection::connect(addr, &key).unwrap();
        node.send(b"").unwrap();
        let declared = MAX_FRAME_SIZE as u32 + 1;
        assert!(matches!(
            node.next_frame(),
            Err(Error::FrameTooLarge { declared: d, max: MAX_FRAME_SIZE }) if d == declared
        ),);
    }

    #[test]
    fn frame_request_frames_at_an_offset_and_batches_back_to_back() {
        let bodies: [&[u8]; 4] = [b"one", b"", &[TAG_BATCH_END, 0x00], b"four!"];
        let mut buf = [0xEE; 128];
        // Start part-way in, as after bytes already queued for sending.
        let start = 7;
        let mut end = start;
        for body in bodies {
            end += frame_body(&mut buf[end..], body).unwrap();
        }
        assert!(buf[..start].iter().all(|&b| b == 0xEE));
        assert!(buf[end..].iter().all(|&b| b == 0xEE));
        assert_eq!(&buf[start..end], bodies.map(app_frame).concat().as_slice());

        // A decoder on the other side gets each body back, in order.
        let mut decoder = FrameDecoder::new();
        decoder.push(&buf[start..end]);
        for body in bodies {
            assert_eq!(
                classify(decoder.next().unwrap().unwrap()).unwrap(),
                Reply::Response(body)
            );
        }
        assert_eq!(decoder.next().unwrap(), None);
    }

    #[test]
    fn a_frame_from_frame_request_is_what_a_node_reads_from_send() {
        // Sent over a live connection by hand, a framed request gets the
        // same reply batch `send` would.
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut stream = bare_stream(addr);
        authenticate(&mut stream, &key).unwrap();
        let mut buf = [0u8; 64];
        let n = frame_body(&mut buf, b"by hand").unwrap();
        stream.write_all(&buf[..n]).unwrap();

        let mut decoder = FrameDecoder::new();
        let mut chunk = [0u8; 64];
        let mut replies = Vec::new();
        let end = format!("{:?}", Reply::BatchEnd(Ack::Policy));
        while !replies.contains(&end) {
            let got = stream.read(&mut chunk).unwrap();
            assert_ne!(got, 0, "the node closed the connection");
            decoder.push(&chunk[..got]);
            while let Some(reply) = next_reply(&mut decoder).unwrap() {
                replies.push(match reply {
                    Reply::Response(body) => String::from_utf8(body.to_vec()).unwrap(),
                    other => format!("{other:?}"),
                });
            }
        }
        assert_eq!(replies, ["by hand".to_string(), end]);
    }

    proptest::proptest! {
        /// Any run of bodies that fit, framed back to back from any
        /// offset, decodes to the same bodies however the bytes arrive.
        #[test]
        fn framed_requests_round_trip_through_the_decoder(
            bodies in proptest::collection::vec(
                proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=64),
                0..8,
            ),
            offset in 0usize..16,
            chunk in 1usize..32,
        ) {
            let mut buf = vec![0u8; offset + bodies.len() * (REQUEST_HEADER_LEN + 64)];
            let mut end = offset;
            for body in &bodies {
                end += frame_body(&mut buf[end..], body).unwrap();
            }
            let mut decoder = FrameDecoder::new();
            let mut got = Vec::new();
            for piece in buf[offset..end].chunks(chunk) {
                decoder.push(piece);
                while let Some(reply) = next_reply(&mut decoder).unwrap() {
                    let Reply::Response(body) = reply else {
                        panic!("a request frame classified as {reply:?}");
                    };
                    got.push(body.to_vec());
                }
            }
            proptest::prop_assert_eq!(got, bodies);
            proptest::prop_assert!(decoder.pending().is_empty());
        }
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
