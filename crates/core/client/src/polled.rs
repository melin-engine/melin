//! A connection driven over a polled byte stream: the blocking
//! [`Connection`](crate::Connection)'s protocol, with no blocking call in
//! it.
//!
//! Three layers, each usable on its own:
//!
//! - [`ByteStream`], the seam to the transport: a non-blocking send, a
//!   service step at the caller's time, a receive that may find nothing,
//!   and an in-place send that lends the transmit buffer to frame a request
//!   into. [`NonBlockingTcp`] is the kernel's, [`BufferStream`] is one whose
//!   bytes the caller's own I/O loop moves in and out.
//! - [`PolledConnection`], the protocol state with no stream and no clock
//!   of its own: the handshake, the receive buffer, the scratch buffer and
//!   the read deadline. Every call takes the stream and the caller's time,
//!   and returns at once. For a caller that runs several connections on one
//!   NIC, keeps its own clock, or is handed its bytes by an `io_uring`
//!   completion ([`PolledConnection::feed`]).
//! - [`StreamConnection`], one stream and a cached clock around a
//!   [`PolledConnection`], with the blocking connection's calls: the
//!   single-stream caller's form. It and the blocking connection both
//!   implement [`Session`], so a request loop is written once and the
//!   transport picked at runtime.
//!
//! What the blocking connection promises, this one keeps: the handshake is
//! not pipelined (nothing but the challenge response goes out before the
//! node says it is ready), a frame is split where it was received, a
//! heartbeat never surfaces and never pushes the read deadline back, and a
//! node's silence is [`Error::NoReply`].

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use melin_wire_protocol::control_codec::{CHALLENGE_RESPONSE_LEN, TAG_LEN};

use crate::framing::{PREFIX_LEN, RequestFrameError, request_payload_len, split_frame};
use crate::{
    Connection, DEFAULT_TIMEOUT, Error, Frame, Handshake, MAX_FRAME_SIZE, MAX_HANDSHAKE_FRAME,
    MAX_REQUEST_BODY, REQUEST_HEADER_LEN, Reply, SigningKey, Step, VerifyingKey, classify,
    frame_request,
};

// ---------------------------------------------------------------------------
// The byte stream
// ---------------------------------------------------------------------------

/// A non-blocking byte stream to a node: the transport under a
/// [`PolledConnection`].
///
/// No call waits. A send may take part of what it is given, or none; a
/// receive may find nothing; and [`service`](Self::service) is where a
/// stream with work of its own between the two (a user-space TCP stack)
/// does it. A connection is generic over the trait rather than holding a
/// trait object, so the hot loop pays no indirection.
pub trait ByteStream {
    /// Queue as much of `data` as the stream will take now. `Ok(0)` means
    /// none of it: try again after a [`service`](Self::service) step.
    fn send(&mut self, data: &[u8]) -> io::Result<usize>;

    /// Move queued bytes toward the wire and received ones toward
    /// [`recv`](Self::recv). `now_ns` is the caller's monotonic clock in
    /// nanoseconds, from an origin fixed for the life of whatever owns the
    /// stack's timers. A stream with nothing to do here (a kernel socket)
    /// ignores it.
    fn service(&mut self, now_ns: u64);

    /// Read what has arrived into `buf`. `Ok(0)` means nothing yet; a
    /// connection the node has closed is an error
    /// ([`io::ErrorKind::UnexpectedEof`] for a clean close), never a zero,
    /// once every byte it sent before closing has been read.
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    /// Lend `fill` the contiguous free region of the stream's transmit
    /// buffer, and queue the bytes it returns having written there, from
    /// the region's start. A request is then encoded where the stream sends
    /// it from, with no copy in between.
    ///
    /// `fill` may be lent a region too short for what it has to write; it
    /// then writes nothing and returns zero, and nothing is queued. It is
    /// not called at all when the stream has no region to lend, and
    /// `Ok(0)` is returned.
    ///
    /// The default lends nothing: a stream with no transmit buffer of its
    /// own (a kernel socket, a [`BufferStream`]) implements only the other
    /// three steps, and a [`PolledConnection`] then frames each request in
    /// a scratch buffer of its own and sends it through
    /// [`send`](Self::send).
    ///
    /// # Errors
    ///
    /// As [`send`](Self::send). A `fill` that returns more than the
    /// region's length is a bug in it; an implementation queues nothing and
    /// reports [`io::ErrorKind::InvalidInput`].
    fn send_in_place(&mut self, fill: impl FnOnce(&mut [u8]) -> usize) -> io::Result<usize> {
        // Nothing to lend: `fill` is never called.
        drop(fill);
        Ok(0)
    }
}

/// A stream lent as `&mut` is the stream: so a caller holding its streams
/// in a collection lends one per call, as a NIC handle lends its views.
impl<S: ByteStream + ?Sized> ByteStream for &mut S {
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        (**self).send(data)
    }

    fn service(&mut self, now_ns: u64) {
        (**self).service(now_ns);
    }

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (**self).recv(buf)
    }

    fn send_in_place(&mut self, fill: impl FnOnce(&mut [u8]) -> usize) -> io::Result<usize> {
        (**self).send_in_place(fill)
    }
}

/// Whether a non-blocking socket call failed only because it would have
/// waited.
fn would_block(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

/// A kernel TCP socket in non-blocking mode, with Nagle off: a polled
/// kernel client in its own right, and the stream a [`StreamConnection`]
/// dials by default.
///
/// It has no transmit buffer to lend, so a request is framed in the
/// connection's scratch buffer and copied once, into the kernel, as the
/// blocking connection copies it once, into its writer's buffer. A reply
/// is read once, from the kernel into the connection's receive buffer, and
/// split there.
#[derive(Debug)]
pub struct NonBlockingTcp {
    stream: TcpStream,
}

impl NonBlockingTcp {
    /// Dial `addr`, waiting at most `timeout` for the connection, then
    /// switch the socket to non-blocking mode with Nagle off.
    pub fn connect(addr: SocketAddr, timeout: Duration) -> io::Result<Self> {
        Self::from_stream(TcpStream::connect_timeout(&addr, timeout)?)
    }

    /// A socket the caller dialed, switched to non-blocking mode with
    /// Nagle off: a request is one small frame and the reply is what the
    /// caller is waiting for, so it is never held for coalescing.
    pub fn from_stream(stream: TcpStream) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        stream.set_nonblocking(true)?;
        Ok(Self { stream })
    }

    /// The socket, for its addresses and options.
    pub fn get_ref(&self) -> &TcpStream {
        &self.stream
    }

    /// The socket, still non-blocking.
    pub fn into_inner(self) -> TcpStream {
        self.stream
    }
}

impl ByteStream for NonBlockingTcp {
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        match self.stream.write(data) {
            Ok(n) => Ok(n),
            Err(e) if would_block(&e) => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn service(&mut self, _now_ns: u64) {}

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // A read into no room returns 0 too: not the node closing.
        if buf.is_empty() {
            return Ok(0);
        }
        match self.stream.read(buf) {
            Ok(0) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the node closed the connection",
            )),
            Ok(n) => Ok(n),
            Err(e) if would_block(&e) => Ok(0),
            Err(e) => Err(e),
        }
    }
}

/// A byte stream whose bytes the caller moves: received bytes are pushed
/// in with [`push_received`](Self::push_received), and what the connection
/// sends is taken out with [`sent`](Self::sent) and
/// [`consume_sent`](Self::consume_sent).
///
/// For a program whose own I/O loop owns the socket (an `io_uring`
/// completion loop, a test harness, a replay of a captured session) and
/// wants a [`PolledConnection`] to do the protocol. The transmit side has
/// a capacity, unbounded unless set, so a loop that submits sends of a
/// fixed size can make the stream push back as a socket's full send buffer
/// does.
///
/// A loop that receives into buffers of its own need not go through this
/// type on the receive side: [`PolledConnection::feed`] takes a
/// completion's bytes directly, one copy, where pushing them here and
/// receiving them again would be two.
#[derive(Debug, Default)]
pub struct BufferStream {
    /// Bytes pushed in and not yet received, from `received_head`. A `Vec`
    /// drained by a cursor and reset when empty, rather than a
    /// `VecDeque`: a receive copies one contiguous slice, and the common
    /// case (everything received before more is pushed) never moves a
    /// byte.
    received: Vec<u8>,
    received_head: usize,
    /// Bytes the connection sent and the caller has not yet consumed, from
    /// `sent_head`; drained the same way as `received`.
    sent: Vec<u8>,
    sent_head: usize,
    /// How many unconsumed sent bytes the stream holds before it takes no
    /// more; `None` for no limit.
    send_capacity: Option<usize>,
    /// Whether the peer's end is closed: once the received bytes are read,
    /// a receive reports [`io::ErrorKind::UnexpectedEof`].
    closed: bool,
}

impl BufferStream {
    /// An empty stream with an unbounded transmit side.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bytes from the peer, for the connection's next receive.
    pub fn push_received(&mut self, bytes: &[u8]) {
        if self.received_head == self.received.len() {
            self.received.clear();
            self.received_head = 0;
        }
        self.received.extend_from_slice(bytes);
    }

    /// The peer has closed its end: once the bytes pushed so far are
    /// received, a receive is [`io::ErrorKind::UnexpectedEof`].
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Bytes pushed in and not yet received.
    pub fn unreceived(&self) -> usize {
        self.received.len() - self.received_head
    }

    /// What the connection has sent and the caller not yet consumed, in
    /// order.
    pub fn sent(&self) -> &[u8] {
        &self.sent[self.sent_head..]
    }

    /// Mark the first `n` bytes of [`sent`](Self::sent) as gone to the
    /// wire, making room for more under the transmit capacity.
    ///
    /// # Panics
    ///
    /// If `n` is more than [`sent`](Self::sent) holds: consuming bytes
    /// that were never sent is the caller's bug.
    pub fn consume_sent(&mut self, n: usize) {
        assert!(
            n <= self.sent.len() - self.sent_head,
            "consumed {n} bytes of the {} sent",
            self.sent.len() - self.sent_head
        );
        self.sent_head += n;
        if self.sent_head == self.sent.len() {
            self.sent.clear();
            self.sent_head = 0;
        }
    }

    /// Hold at most `capacity` unconsumed sent bytes, as a socket's send
    /// buffer does; `None` for no limit. Bytes already held stay, even past
    /// a lowered capacity, and further sends take nothing until they are
    /// consumed below it.
    pub fn set_send_capacity(&mut self, capacity: Option<usize>) {
        self.send_capacity = capacity;
    }

    /// Room under the transmit capacity.
    fn send_room(&self) -> usize {
        match self.send_capacity {
            Some(capacity) => capacity.saturating_sub(self.sent.len() - self.sent_head),
            None => usize::MAX,
        }
    }
}

impl ByteStream for BufferStream {
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        let n = data.len().min(self.send_room());
        if self.sent_head > 0 && self.sent_head == self.sent.len() {
            self.sent.clear();
            self.sent_head = 0;
        }
        self.sent.extend_from_slice(&data[..n]);
        Ok(n)
    }

    fn service(&mut self, _now_ns: u64) {}

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let pending = &self.received[self.received_head..];
        if pending.is_empty() {
            return if self.closed {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the peer closed the stream",
                ))
            } else {
                Ok(0)
            };
        }
        let n = pending.len().min(buf.len());
        buf[..n].copy_from_slice(&pending[..n]);
        self.received_head += n;
        Ok(n)
    }
}

// ---------------------------------------------------------------------------
// The state-only connection
// ---------------------------------------------------------------------------

/// The longest frame on the wire, length prefix included: what the receive
/// buffer must hold whole, and what the scratch buffer frames a request in.
const MAX_WIRE_FRAME: usize = PREFIX_LEN + MAX_FRAME_SIZE;

/// The receive buffer's initial size: several frames of the longest kind,
/// so one receive takes a burst of replies, and a partial frame left at
/// its end always has room to complete after the buffer is compacted.
const RECEIVE_BUFFER: usize = 16 * MAX_WIRE_FRAME;

/// One frame found in the receive buffer, as positions rather than a
/// borrow: [`PolledConnection::poll`] decides on a frame in a loop and
/// borrows it back only to return it, which the borrow checker cannot
/// follow for a borrow held across the loop.
#[derive(Clone, Copy, Debug)]
enum Found {
    /// An application response's body: `rx[start..end]`.
    Response {
        start: usize,
        end: usize,
    },
    BatchEnd,
    ServerBusy,
    EngineError,
}

/// The client side of one connection to a node, as protocol state with no
/// stream and no clock of its own.
///
/// It holds the handshake, a receive buffer that frames are split in, a
/// scratch buffer that a request goes through when the stream cannot lend
/// it room, and the read deadline. Every call takes the stream and the
/// caller's time (a monotonic nanosecond count whose origin is the
/// caller's, fixed for the connection's life), and returns at once: no
/// call spins, so a caller with several connections on one NIC services
/// them all from one loop, and a caller with a clock of its own (a TSC it
/// already reads for its samples) passes that reading rather than have the
/// library read another. [`StreamConnection`] is the form that owns its
/// stream and waits.
///
/// - **The handshake** runs inside [`poll`](Self::poll): the node's
///   challenge is answered as it arrives, and the node's verdict makes the
///   connection [ready](Self::is_ready) or fails it with
///   [`Error::AuthFailed`]. A request sent before then is held, and goes
///   out only once the node has said it is ready: never in the same
///   segment as the challenge response, which a node may stall on.
/// - **Receiving**, [`poll`](Self::poll) reads what the stream has into
///   the receive buffer and splits one frame off it where it lies.
///   [`feed`](Self::feed) puts bytes there directly, for a caller whose
///   I/O loop received them into a buffer of its own.
/// - **Sending**, [`send`](Self::send) and [`send_with`](Self::send_with)
///   frame a request where the stream lends room for it
///   ([`ByteStream::send_in_place`]); a request that does not fit the
///   contiguous room (the transmit ring is about to wrap, say) is framed in
///   the scratch buffer and copied through [`ByteStream::send`] instead.
///   What the stream does not take at once stays queued, in order, and goes
///   out on later calls. The queue is not bounded: a caller that keeps
///   sending into a stream that takes nothing grows it by a frame a
///   request, so a loop of its own gates on [`unsent`](Self::unsent) (or
///   on the stream's own room) before queueing more.
///
/// The read deadline runs while a reply is owed: during the handshake,
/// and while a request sent has not had its batch end (or the node's
/// refusal, which ends it without one). It starts with the
/// request, or with the first poll that finds nothing after a frame, ends
/// with the next frame that is not a heartbeat, and expires as
/// [`Error::NoReply`]. A heartbeat does not push it back. A connection
/// with nothing owed is never timed out, however long it is polled.
///
/// After [`Error::FrameTooLarge`] the stream has no frame boundary left
/// and every later poll fails the same way; after any error but
/// [`Error::Protocol`] (whose frame is consumed) the connection is to be
/// dropped.
pub struct PolledConnection {
    handshake: Handshake,
    public_key: VerifyingKey,
    /// The challenge response, length prefix included, once the handshake
    /// has made it: a copy of the handshake's own, since the stream may not
    /// take it in one send and the handshake lends it for one call. A fixed
    /// array: it is the one frame the handshake sends, of a fixed size.
    challenge_response: [u8; PREFIX_LEN + CHALLENGE_RESPONSE_LEN],
    /// How much of `challenge_response` is to be sent (zero before the
    /// challenge, its whole length after), and how much the stream has
    /// taken.
    challenge_response_len: usize,
    challenge_response_sent: usize,
    /// Received bytes not yet split, at `rx[rx_head..rx_tail]`; the rest of
    /// the `Vec` is room to receive into. A flat buffer compacted at the
    /// start of a call rather than a ring: a frame is split, and handed
    /// out, as one contiguous slice where it was received.
    rx: Vec<u8>,
    rx_head: usize,
    rx_tail: usize,
    /// Requests framed here and not yet taken by the stream, from
    /// `tx_head`: the scratch buffer. A `Vec` drained by a cursor: empty in
    /// the steady state, where requests are framed in the stream's own
    /// buffer, and reset whenever the stream has taken all of it.
    tx: Vec<u8>,
    tx_head: usize,
    read_timeout: Duration,
    /// `read_timeout` in nanoseconds, the unit deadlines are kept in.
    /// `u64`, as the caller's clock is: centuries of range.
    read_timeout_ns: u64,
    /// When the node's silence becomes [`Error::NoReply`], on the caller's
    /// clock; `None` while no wait is running.
    deadline_ns: Option<u64>,
    /// Requests sent whose batch has not ended. `u64`: a count of
    /// requests, never near overflow, and wide enough not to wrap if a
    /// node stopped ending batches.
    in_flight: u64,
}

impl std::fmt::Debug for PolledConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PolledConnection")
            .field("handshake", &self.handshake)
            .field("buffered", &(self.rx_tail - self.rx_head))
            .field("unsent", &self.unsent())
            .field("in_flight", &self.in_flight)
            .field("deadline_ns", &self.deadline_ns)
            .finish_non_exhaustive()
    }
}

/// `timeout` in nanoseconds, saturating: a timeout past `u64::MAX`
/// nanoseconds (centuries) is as good as none.
pub(crate) fn duration_ns(timeout: Duration) -> u64 {
    u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX)
}

impl PolledConnection {
    /// A connection that will prove `key`, with [`DEFAULT_TIMEOUT`] as its
    /// read timeout. It sends nothing until polled with the node's
    /// challenge.
    pub fn new(key: &SigningKey) -> Self {
        Self::with_timeout(key, DEFAULT_TIMEOUT)
    }

    /// [`new`](Self::new) with an explicit read timeout, which bounds the
    /// handshake and every wait for a reply after it.
    pub fn with_timeout(key: &SigningKey, read_timeout: Duration) -> Self {
        Self {
            handshake: Handshake::new(key),
            public_key: key.verifying_key(),
            challenge_response: [0; PREFIX_LEN + CHALLENGE_RESPONSE_LEN],
            challenge_response_len: 0,
            challenge_response_sent: 0,
            rx: vec![0; RECEIVE_BUFFER],
            rx_head: 0,
            rx_tail: 0,
            tx: Vec::with_capacity(MAX_WIRE_FRAME),
            tx_head: 0,
            read_timeout,
            read_timeout_ns: duration_ns(read_timeout),
            deadline_ns: None,
            in_flight: 0,
        }
    }

    /// Whether the node has accepted the key: requests sent from here go
    /// out at once.
    pub fn is_ready(&self) -> bool {
        self.handshake.is_done()
    }

    /// The key this connection proves.
    pub fn public_key(&self) -> &VerifyingKey {
        &self.public_key
    }

    /// How long the node may stay silent while a reply is owed.
    pub fn read_timeout(&self) -> Duration {
        self.read_timeout
    }

    /// Change the read timeout. A wait already running keeps the deadline
    /// it started with.
    pub fn set_read_timeout(&mut self, timeout: Duration) {
        self.read_timeout = timeout;
        self.read_timeout_ns = duration_ns(timeout);
    }

    /// Bytes framed and not yet taken by the stream: the challenge
    /// response, and requests framed in the scratch buffer. They go out on
    /// the next call that is given the stream, [`flush`](Self::flush)
    /// among them.
    pub fn unsent(&self) -> usize {
        (self.challenge_response_len - self.challenge_response_sent)
            + (self.tx.len() - self.tx_head)
    }

    /// Bytes the caller received from the node itself, copied into the
    /// receive buffer once, as a frame decoder's push would: for an I/O
    /// loop that is handed received bytes in a buffer of its own (an
    /// `io_uring` completion's), which a stream would only copy a second
    /// time. They are split by the next [`poll`](Self::poll).
    pub fn feed(&mut self, bytes: &[u8]) {
        self.make_room(bytes.len());
        self.rx[self.rx_tail..self.rx_tail + bytes.len()].copy_from_slice(bytes);
        self.rx_tail += bytes.len();
    }

    /// Hand `stream` whatever is queued for it, as much as it takes now:
    /// the challenge response first, then, once the node has said it is
    /// ready, requests framed in the scratch buffer. Never waits.
    pub fn flush<S: ByteStream>(&mut self, stream: &mut S) -> Result<(), Error> {
        while self.challenge_response_sent < self.challenge_response_len {
            let n = stream.send(
                &self.challenge_response[self.challenge_response_sent..self.challenge_response_len],
            )?;
            if n == 0 {
                return Ok(());
            }
            self.challenge_response_sent += n;
        }
        // A request is held until the node is ready: never pipelined with
        // the challenge response.
        if !self.is_ready() {
            return Ok(());
        }
        while self.tx_head < self.tx.len() {
            let n = stream.send(&self.tx[self.tx_head..])?;
            if n == 0 {
                break;
            }
            self.tx_head += n;
        }
        if self.tx_head == self.tx.len() {
            self.tx.clear();
            self.tx_head = 0;
        }
        Ok(())
    }

    /// Send one request, the application's bytes, as an application frame.
    ///
    /// Framed where the stream lends room for it, or in the scratch buffer
    /// and copied through the stream's plain send when it does not (see the
    /// type's docs); whatever the stream does not take now is queued and
    /// goes out on later calls. `now_ns` starts the wait for its reply. A
    /// body that would take the frame over [`MAX_FRAME_SIZE`] is
    /// [`Error::RequestTooLarge`], and nothing is queued.
    pub fn send<S: ByteStream>(
        &mut self,
        stream: &mut S,
        now_ns: u64,
        body: &[u8],
    ) -> Result<(), Error> {
        request_payload_len(body.len())?;
        self.send_with(stream, now_ns, body.len(), |out: &mut [u8]| {
            // `send_with` hands no less than `body.len()` bytes; checked
            // all the same rather than trusted with a panic.
            let Some(dst) = out.get_mut(..body.len()) else {
                return Err(Error::BufferTooSmall {
                    needed: REQUEST_HEADER_LEN + body.len(),
                    available: REQUEST_HEADER_LEN + out.len(),
                });
            };
            dst.copy_from_slice(body);
            Ok(body.len())
        })
    }

    /// Send one request whose body `encode` writes, as [`frame_request`]
    /// frames it: `encode` is handed the region behind the header, at least
    /// `max_body` bytes long, and returns how many bytes it wrote there.
    ///
    /// `max_body` is the widest body the encoder may write. The request is
    /// framed in the room the stream lends when that room holds a header
    /// and `max_body` bytes, and in the scratch buffer otherwise, so the
    /// encoder never sees a region shorter than it was promised (a codec
    /// that indexes its buffer on that promise does not panic at a ring's
    /// wrap), and runs exactly once. [`MAX_REQUEST_BODY`] serves when the
    /// bound is not known, at the cost of the scratch buffer for any room
    /// shorter than the widest frame; a value over it is taken as it, the
    /// widest body a frame carries. Nothing is queued on an error.
    ///
    /// The error type is the encoder's, as with [`frame_request`]; it takes
    /// this crate's [`Error`] too, for the stream's own failures, so
    /// `Error` itself serves.
    pub fn send_with<S, E, F>(
        &mut self,
        stream: &mut S,
        now_ns: u64,
        max_body: usize,
        encode: F,
    ) -> Result<(), E>
    where
        S: ByteStream,
        E: From<RequestFrameError> + From<Error>,
        F: FnOnce(&mut [u8]) -> Result<usize, E>,
    {
        let needed = REQUEST_HEADER_LEN + max_body.min(MAX_REQUEST_BODY);
        // Whatever is queued goes first: requests leave in order.
        self.flush(stream)?;
        let encode = if self.is_ready() && self.tx_head == self.tx.len() {
            match frame_in_place(stream, needed, encode).map_err(Error::Io)? {
                InPlace::Framed(Ok(written), queued) if queued == written => {
                    self.request_sent(now_ns);
                    return Ok(());
                }
                InPlace::Framed(Ok(written), queued) => {
                    return Err(Error::Io(io::Error::other(format!(
                        "the stream queued {queued} of the {written} bytes framed in place"
                    )))
                    .into());
                }
                InPlace::Framed(Err(e), _) => return Err(e),
                // No room lent, or too little of it: the scratch buffer
                // frames the request instead.
                InPlace::NotTaken(encode) => encode,
            }
        } else {
            encode
        };
        self.frame_in_scratch(needed, encode)?;
        self.request_sent(now_ns);
        self.flush(stream)?;
        Ok(())
    }

    /// The next frame from the node, heartbeats skipped, or `None` when
    /// none has arrived whole.
    ///
    /// Sends what is queued, splits a frame already received if there is
    /// one, and otherwise receives once from `stream` and tries again. The
    /// node's frames during the handshake are taken here and never
    /// surface: the connection answers the challenge, and becomes
    /// [ready](Self::is_ready) on the node's verdict. A frame returned is
    /// borrowed from the receive buffer, valid until the next call.
    ///
    /// # Errors
    ///
    /// - [`Error::NoReply`] once a reply is owed (see the type's docs) and
    ///   `now_ns` is past the deadline.
    /// - [`Error::AuthFailed`], [`Error::Protocol`] from the handshake.
    /// - [`Error::Protocol`] for a frame no reply carries; the frame is
    ///   consumed.
    /// - [`Error::FrameTooLarge`] for a length prefix over the limit, every
    ///   time from then on.
    /// - [`Error::Disconnected`] once the node has closed the connection
    ///   and every frame before the close has been taken; [`Error::Io`] for
    ///   the stream failing.
    pub fn poll<S: ByteStream>(
        &mut self,
        stream: &mut S,
        now_ns: u64,
    ) -> Result<Option<Frame<'_>>, Error> {
        Ok(self
            .poll_found(stream, now_ns, false)?
            .map(|found| self.frame(found)))
    }

    /// [`poll`](Self::poll), without the borrow: `waiting` makes the wait
    /// run even with nothing owed, as a blocking next-frame call does.
    fn poll_found<S: ByteStream>(
        &mut self,
        stream: &mut S,
        now_ns: u64,
        waiting: bool,
    ) -> Result<Option<Found>, Error> {
        self.flush(stream)?;
        let mut received = false;
        loop {
            if !self.handshake.is_done() {
                refuse_long_handshake_frame(&self.rx[self.rx_head..self.rx_tail])?;
            }
            let Some(frame) = split_frame(&self.rx[self.rx_head..self.rx_tail])? else {
                if !received {
                    received = true;
                    if self.receive(stream)? > 0 {
                        continue;
                    }
                }
                return self.nothing_yet(now_ns, waiting).map(|()| None);
            };
            let start = self.rx_head + PREFIX_LEN;
            let end = self.rx_head + frame.consumed();
            // Consumed now, whatever comes of it: the bytes stay where they
            // are until the next call compacts the buffer, so a frame
            // handed out stays valid until then.
            self.rx_head = end;
            if !self.handshake.is_done() {
                self.handshake_frame(stream, start, end)?;
                continue;
            }
            let found = match classify(&self.rx[start..end])? {
                Reply::Heartbeat => continue,
                Reply::Response(_) => Found::Response {
                    start: start + TAG_LEN,
                    end,
                },
                Reply::BatchEnd => {
                    self.in_flight = self.in_flight.saturating_sub(1);
                    Found::BatchEnd
                }
                // The node refuses a request with this frame alone, no
                // batch end after it, and keeps the connection: the refused
                // request is owed nothing more. An engine error is not the
                // same: the batch end the node sends after it settles it.
                Reply::ServerBusy => {
                    self.in_flight = self.in_flight.saturating_sub(1);
                    Found::ServerBusy
                }
                Reply::EngineError => Found::EngineError,
            };
            // An answer: the wait is over.
            self.deadline_ns = None;
            return Ok(Some(found));
        }
    }

    /// The frame `found` describes, borrowed back from the receive buffer.
    fn frame(&self, found: Found) -> Frame<'_> {
        match found {
            Found::Response { start, end } => Frame::Response(&self.rx[start..end]),
            Found::BatchEnd => Frame::BatchEnd,
            Found::ServerBusy => Frame::ServerBusy,
            Found::EngineError => Frame::EngineError,
        }
    }

    /// A poll that found no frame: start the wait if a reply is owed and
    /// none is running, or report the silence if it has run out.
    fn nothing_yet(&mut self, now_ns: u64, waiting: bool) -> Result<(), Error> {
        if !(waiting || self.in_flight > 0 || !self.handshake.is_done()) {
            return Ok(());
        }
        match self.deadline_ns {
            None => {
                self.deadline_ns = Some(now_ns.saturating_add(self.read_timeout_ns));
                Ok(())
            }
            Some(deadline) if now_ns >= deadline => Err(Error::NoReply {
                timeout: self.read_timeout,
            }),
            Some(_) => Ok(()),
        }
    }

    /// One of the node's handshake frames, at `rx[start..end]`.
    fn handshake_frame<S: ByteStream>(
        &mut self,
        stream: &mut S,
        start: usize,
        end: usize,
    ) -> Result<(), Error> {
        match self.handshake.feed(&self.rx[start..end])? {
            Step::Send(response) => {
                self.challenge_response[..response.len()].copy_from_slice(response);
                self.challenge_response_len = response.len();
                self.challenge_response_sent = 0;
            }
            // Requests held until now go out below.
            Step::Ready => {}
        }
        // The node answered: the next wait starts afresh.
        self.deadline_ns = None;
        self.flush(stream)
    }

    /// A request is queued: count it owed, and start the wait for its reply
    /// if none is running.
    fn request_sent(&mut self, now_ns: u64) {
        self.in_flight = self.in_flight.saturating_add(1);
        if self.deadline_ns.is_none() {
            self.deadline_ns = Some(now_ns.saturating_add(self.read_timeout_ns));
        }
    }

    /// Frame a request at the end of the scratch buffer, behind anything
    /// still queued there, in `needed` bytes of room: the header and the
    /// widest body the encoder was promised, not the widest frame, so the
    /// kernel path (every request of which goes through here) zeroes only
    /// what the request can take.
    fn frame_in_scratch<E, F>(&mut self, needed: usize, encode: F) -> Result<(), E>
    where
        E: From<RequestFrameError>,
        F: FnOnce(&mut [u8]) -> Result<usize, E>,
    {
        if self.tx_head > 0 {
            // Rare: the stream took part of what was queued and more is
            // coming behind it. Reclaim the front before growing.
            self.tx.drain(..self.tx_head);
            self.tx_head = 0;
        }
        let start = self.tx.len();
        self.tx.resize(start + needed, 0);
        match frame_request(&mut self.tx[start..], encode) {
            Ok(written) => {
                self.tx.truncate(start + written);
                Ok(())
            }
            Err(e) => {
                self.tx.truncate(start);
                Err(e)
            }
        }
    }

    /// Receive once from `stream` into the receive buffer's room.
    fn receive<S: ByteStream>(&mut self, stream: &mut S) -> Result<usize, Error> {
        self.make_room(MAX_WIRE_FRAME);
        match stream.recv(&mut self.rx[self.rx_tail..]) {
            Ok(n) => {
                self.rx_tail += n;
                Ok(n)
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Err(Error::Disconnected),
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// At least `wanted` bytes of room behind the received bytes: reset the
    /// buffer when everything in it has been split, move the unsplit tail
    /// to the front when the room behind it is short, and grow it only for
    /// bytes fed in faster than they are polled out.
    fn make_room(&mut self, wanted: usize) {
        if self.rx_head == self.rx_tail {
            self.rx_head = 0;
            self.rx_tail = 0;
        } else if self.rx.len() - self.rx_tail < wanted && self.rx_head > 0 {
            self.rx.copy_within(self.rx_head..self.rx_tail, 0);
            self.rx_tail -= self.rx_head;
            self.rx_head = 0;
        }
        if self.rx.len() - self.rx_tail < wanted {
            self.rx.resize(self.rx_tail + wanted, 0);
        }
    }
}

/// What the in-place step did with the encoder it was handed.
enum InPlace<E, F> {
    /// The request was framed in the lent region: the encoder's outcome,
    /// and how many bytes the stream queued.
    Framed(Result<usize, E>, usize),
    /// No region was lent, or one too short for what the encoder was
    /// promised: the encoder, untouched, for the scratch buffer.
    NotTaken(F),
}

/// The encoder as the in-place step holds it: the step's closure may not
/// run at all, and an encoder moved into a closure that never runs is
/// lost, so the closure borrows this slot and swaps the encoder for its
/// outcome in one move. Two states, no third: after the step the slot
/// holds exactly one of them.
enum EncoderSlot<E, F> {
    Encoder(F),
    Framed(Result<usize, E>),
}

/// Frame a request through `stream`'s in-place send, when the region it
/// lends holds `needed` bytes. Never frames into a shorter region, so the
/// encoder sees the room it was promised and runs at most once.
fn frame_in_place<S, E, F>(stream: &mut S, needed: usize, encode: F) -> io::Result<InPlace<E, F>>
where
    S: ByteStream,
    E: From<RequestFrameError>,
    F: FnOnce(&mut [u8]) -> Result<usize, E>,
{
    let mut slot = EncoderSlot::Encoder(encode);
    let queued = stream.send_in_place(|region| {
        if region.len() < needed {
            return 0;
        }
        // The placeholder is overwritten below before the closure returns;
        // the `else` is the closure running twice, which the step's
        // `FnOnce` contract rules out.
        let EncoderSlot::Encoder(encode) = std::mem::replace(&mut slot, EncoderSlot::Framed(Ok(0)))
        else {
            return 0;
        };
        let outcome = frame_request(region, encode);
        // Nothing is written on an error, which is returned as it is.
        let written = match &outcome {
            Ok(written) => *written,
            Err(_) => 0,
        };
        slot = EncoderSlot::Framed(outcome);
        written
    })?;
    Ok(match slot {
        EncoderSlot::Encoder(encode) => InPlace::NotTaken(encode),
        EncoderSlot::Framed(outcome) => InPlace::Framed(outcome, queued),
    })
}

/// Refuse a frame at the front of `buf` too long for a handshake frame, as
/// soon as its prefix is in: the blocking handshake's error, without
/// waiting for a payload no handshake frame has. A prefix over the frame
/// limit is left to [`split_frame`], for the same error as after the
/// handshake.
fn refuse_long_handshake_frame(buf: &[u8]) -> Result<(), Error> {
    let Some(prefix) = buf.first_chunk::<PREFIX_LEN>() else {
        return Ok(());
    };
    let len = u32::from_le_bytes(*prefix) as usize;
    if len > MAX_HANDSHAKE_FRAME && len <= MAX_FRAME_SIZE {
        return Err(Error::Protocol(format!(
            "a {len}-byte frame where a handshake frame was expected"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The single-stream wrapper
// ---------------------------------------------------------------------------

/// How many polls the wrapper's clock is read once in. An empty poll is
/// little work, a clock read would be a measurable share of it, and a
/// deadline measured in seconds does not need a reading per poll. Every
/// so many polls rather than every poll, as the node's DPDK transport
/// refreshes its stack's clock.
const CLOCK_REFRESH_POLLS: u32 = 100;

/// A monotonic clock read once every [`CLOCK_REFRESH_POLLS`] ticks, and on
/// demand at the start of each blocking call.
#[derive(Debug)]
pub(crate) struct CachedClock {
    /// What the nanosecond counts are counted from: the wrapper's creation,
    /// or, over a stream whose stack outlives the wrapper (a DPDK NIC
    /// handle's), that stack's own origin, so the two agree. An `Instant`,
    /// not the wall clock, which can be stepped.
    origin: Instant,
    /// The latest reading, in nanoseconds since `origin`.
    now_ns: u64,
    /// Ticks since the latest reading. `u32`: reset at the interval, never
    /// near its range.
    ticks: u32,
}

impl CachedClock {
    /// A clock counting from now.
    fn start() -> Self {
        Self::counting_from(Instant::now())
    }

    /// A clock counting from `origin`, an instant already past: readings
    /// are the time since it, from the first one on.
    pub(crate) fn counting_from(origin: Instant) -> Self {
        let mut clock = Self {
            origin,
            now_ns: 0,
            ticks: 0,
        };
        clock.refresh();
        clock
    }

    /// Read the clock now.
    fn refresh(&mut self) -> u64 {
        self.ticks = 0;
        self.now_ns = duration_ns(self.origin.elapsed());
        self.now_ns
    }

    /// The cached reading, refreshed if this tick completes the interval.
    fn tick(&mut self) -> u64 {
        self.ticks += 1;
        if self.ticks >= CLOCK_REFRESH_POLLS {
            self.refresh()
        } else {
            self.now_ns
        }
    }
}

/// A [`PolledConnection`] that owns its stream and a clock: the blocking
/// [`Connection`]'s calls over any [`ByteStream`].
///
/// [`next_frame`](Self::next_frame) and [`request`](Self::request) spin on
/// the stream, servicing it on every turn, until a frame arrives or the
/// read deadline passes, with the blocking connection's errors.
/// [`poll`](Self::poll) is one turn of that loop, for a caller that runs
/// its own and does other work between frames. A spinning call keeps its
/// core busy: on kernel bypass, pin the thread.
///
/// The clock is an [`Instant`] read once every so many polls rather than
/// on every one, and at the start of each blocking call; the read deadline
/// and the stream's service step both take that cached reading. A caller
/// that wants nanosecond timestamps for its own samples reads its own
/// clock. Over a DPDK NIC handle (the `dpdk` feature) the clock counts
/// from the handle's own origin rather than the wrapper's creation, so the
/// stack, whose timers outlive any one connection, sees one clock across
/// the dial, this connection and the next.
///
/// After any error the connection's framing can no longer be trusted;
/// drop it and connect again.
pub struct StreamConnection<S> {
    stream: S,
    connection: PolledConnection,
    clock: CachedClock,
}

impl<S> std::fmt::Debug for StreamConnection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamConnection")
            .field("connection", &self.connection)
            .finish_non_exhaustive()
    }
}

impl StreamConnection<NonBlockingTcp> {
    /// Dial `addr` on a non-blocking kernel socket and authenticate with
    /// `key`, using [`DEFAULT_TIMEOUT`] for the dial and for every wait
    /// after: [`Connection::connect`], polled.
    pub fn connect(addr: SocketAddr, key: &SigningKey) -> Result<Self, Error> {
        Self::connect_timeout(addr, key, DEFAULT_TIMEOUT)
    }

    /// [`connect`](Self::connect) with an explicit timeout, applied to the
    /// dial, the handshake, and every wait after.
    pub fn connect_timeout(
        addr: SocketAddr,
        key: &SigningKey,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let stream = NonBlockingTcp::connect(addr, timeout)
            .map_err(|source| Error::Connect { addr, source })?;
        Self::handshake(stream, key, timeout)
    }
}

impl<S: ByteStream> StreamConnection<S> {
    /// Authenticate with `key` over `stream`, an open connection to a node
    /// on which nothing has been read or sent, spinning until the node says
    /// it is ready. `timeout` bounds the handshake and every wait after; a
    /// node that does not answer within it is [`Error::NoReply`], a refused
    /// key [`Error::AuthFailed`].
    pub fn handshake(stream: S, key: &SigningKey, timeout: Duration) -> Result<Self, Error> {
        Self::handshake_on(stream, key, timeout, CachedClock::start())
    }

    /// [`handshake`](Self::handshake) on `clock`: the clock the stream's
    /// service step and the read deadline will run on, from the handshake
    /// on.
    pub(crate) fn handshake_on(
        stream: S,
        key: &SigningKey,
        timeout: Duration,
        clock: CachedClock,
    ) -> Result<Self, Error> {
        let mut this = Self {
            stream,
            connection: PolledConnection::with_timeout(key, timeout),
            clock,
        };
        let mut now_ns = this.clock.refresh();
        while !this.connection.is_ready() {
            this.stream.service(now_ns);
            if let Some(found) = this
                .connection
                .poll_found(&mut this.stream, now_ns, false)?
            {
                // A reply before any request: the node is not keeping the
                // protocol.
                return Err(Error::Protocol(format!(
                    "a {:?} frame before any request",
                    this.connection.frame(found)
                )));
            }
            std::hint::spin_loop();
            now_ns = this.clock.tick();
        }
        Ok(this)
    }

    /// The key this connection authenticated with.
    pub fn public_key(&self) -> &VerifyingKey {
        self.connection.public_key()
    }

    /// Change how long a wait for a reply lasts before
    /// [`Error::NoReply`], as [`Connection::set_read_timeout`].
    pub fn set_read_timeout(&mut self, timeout: Duration) {
        self.connection.set_read_timeout(timeout);
    }

    /// The stream, for its addresses and options.
    pub fn stream(&self) -> &S {
        &self.stream
    }

    /// The stream. Bytes read or written on it directly are not the
    /// connection's, and break its framing.
    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// The protocol state, for what is queued and owed.
    pub fn connection(&self) -> &PolledConnection {
        &self.connection
    }

    /// Give up the wrapper: the stream, and the protocol state with
    /// whatever it has received and queued, to drive by hand from here.
    pub fn into_parts(self) -> (S, PolledConnection) {
        (self.stream, self.connection)
    }

    /// Send one request, the application's bytes, as an application frame,
    /// and spin until the stream has taken it: [`Connection::send`],
    /// polled. A stream that has not taken the whole request within the
    /// read timeout (a node that has stopped reading, or one taking a few
    /// bytes at a time for longer than that) is [`Error::Io`] with
    /// [`io::ErrorKind::TimedOut`]. A body over the frame limit is
    /// [`Error::RequestTooLarge`], and nothing is sent.
    pub fn send(&mut self, body: &[u8]) -> Result<(), Error> {
        let now_ns = self.clock.refresh();
        self.connection.send(&mut self.stream, now_ns, body)?;
        self.drain(now_ns)
    }

    /// Send one request whose body `encode` writes, as
    /// [`PolledConnection::send_with`], and spin until the stream has taken
    /// it, as [`send`](Self::send).
    pub fn send_with<E, F>(&mut self, max_body: usize, encode: F) -> Result<(), E>
    where
        E: From<RequestFrameError> + From<Error>,
        F: FnOnce(&mut [u8]) -> Result<usize, E>,
    {
        let now_ns = self.clock.refresh();
        self.connection
            .send_with(&mut self.stream, now_ns, max_body, encode)?;
        Ok(self.drain(now_ns)?)
    }

    /// Service the stream until it has taken everything queued, bounded by
    /// the read timeout from `started_ns` as a whole (not per byte taken, so
    /// a stream that trickles for longer than the timeout fails too).
    fn drain(&mut self, started_ns: u64) -> Result<(), Error> {
        let mut now_ns = started_ns;
        loop {
            self.stream.service(now_ns);
            self.connection.flush(&mut self.stream)?;
            if self.connection.unsent() == 0 {
                // One more turn, so a stream with a stack of its own puts
                // what it took on the wire now rather than on the next call.
                self.stream.service(now_ns);
                return Ok(());
            }
            if now_ns.saturating_sub(started_ns) >= duration_ns(self.connection.read_timeout()) {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the stream did not take the whole queued request within the read timeout",
                )));
            }
            std::hint::spin_loop();
            now_ns = self.clock.tick();
        }
    }

    /// One turn of the loop: service the stream, send what is queued, and
    /// return the next frame if one has arrived whole. Never waits. The
    /// read deadline runs only while a reply is owed, as on
    /// [`PolledConnection::poll`], whose errors these are.
    pub fn poll(&mut self) -> Result<Option<Frame<'_>>, Error> {
        let now_ns = self.clock.tick();
        self.stream.service(now_ns);
        self.connection.poll(&mut self.stream, now_ns)
    }

    /// The next frame from the node, heartbeats skipped, spinning on the
    /// stream until one arrives or the read timeout passes:
    /// [`Connection::next_frame`], polled, with its errors. Heartbeats do
    /// not extend the wait.
    pub fn next_frame(&mut self) -> Result<Frame<'_>, Error> {
        let mut now_ns = self.clock.refresh();
        loop {
            self.stream.service(now_ns);
            if let Some(found) = self.connection.poll_found(&mut self.stream, now_ns, true)? {
                return Ok(self.connection.frame(found));
            }
            std::hint::spin_loop();
            now_ns = self.clock.tick();
        }
    }

    /// Send one request and collect the application frames of its reply
    /// batch, in order: [`Connection::request`], polled.
    pub fn request(&mut self, body: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        self.send(body)?;
        collect_batch(self)
    }

    /// [`request`](Self::request) for exactly one frame in reply; any
    /// other count is a protocol error.
    pub fn request_one(&mut self, body: &[u8]) -> Result<Vec<u8>, Error> {
        one_frame(self.request(body)?)
    }
}

/// The frames of one reply batch, read through `session`'s next-frame
/// call: [`Connection::request`]'s loop, for any session.
fn collect_batch(session: &mut (impl Session + ?Sized)) -> Result<Vec<Vec<u8>>, Error> {
    let mut frames = Vec::new();
    loop {
        match session.next_frame()? {
            Frame::Response(bytes) => frames.push(bytes.to_vec()),
            Frame::BatchEnd => return Ok(frames),
            Frame::ServerBusy => return Err(Error::ServerBusy),
            Frame::EngineError => return Err(Error::EngineError),
        }
    }
}

/// The one frame of a batch that must hold exactly one.
fn one_frame(mut frames: Vec<Vec<u8>>) -> Result<Vec<u8>, Error> {
    match frames.len() {
        1 => Ok(frames.swap_remove(0)),
        n => Err(Error::Protocol(format!(
            "expected one frame in reply, got {n}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// The session trait
// ---------------------------------------------------------------------------

/// The calls a request loop makes on a connection to a node, whatever
/// carries it: implemented by the blocking [`Connection`] and by
/// [`StreamConnection`], so a loop is written once, over
/// `&mut dyn Session` or a generic, and the transport picked at runtime.
///
/// Each call is the blocking connection's, with its errors: see
/// [`Connection::send`], [`Connection::next_frame`] and
/// [`Connection::request`].
pub trait Session {
    /// Send one request, the application's bytes, as an application frame.
    fn send(&mut self, body: &[u8]) -> Result<(), Error>;

    /// The next frame from the node, heartbeats skipped, waiting up to the
    /// read timeout.
    fn next_frame(&mut self) -> Result<Frame<'_>, Error>;

    /// Send one request and collect the application frames of its reply
    /// batch, in order.
    fn request(&mut self, body: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        self.send(body)?;
        collect_batch(self)
    }

    /// [`request`](Self::request) for exactly one frame in reply; any other
    /// count is a protocol error.
    fn request_one(&mut self, body: &[u8]) -> Result<Vec<u8>, Error> {
        one_frame(self.request(body)?)
    }
}

impl Session for Connection {
    fn send(&mut self, body: &[u8]) -> Result<(), Error> {
        Connection::send(self, body)
    }

    fn next_frame(&mut self) -> Result<Frame<'_>, Error> {
        Connection::next_frame(self)
    }

    fn request(&mut self, body: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        Connection::request(self, body)
    }

    fn request_one(&mut self, body: &[u8]) -> Result<Vec<u8>, Error> {
        Connection::request_one(self, body)
    }
}

impl<S: ByteStream> Session for StreamConnection<S> {
    fn send(&mut self, body: &[u8]) -> Result<(), Error> {
        StreamConnection::send(self, body)
    }

    fn next_frame(&mut self) -> Result<Frame<'_>, Error> {
        StreamConnection::next_frame(self)
    }

    fn request(&mut self, body: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        StreamConnection::request(self, body)
    }

    fn request_one(&mut self, body: &[u8]) -> Result<Vec<u8>, Error> {
        StreamConnection::request_one(self, body)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use melin_wire_protocol::control::TransportResponse;
    use melin_wire_protocol::control_codec::{
        TAG_APP, TAG_BATCH_END, TAG_SERVER_BUSY, decode_challenge_response,
    };

    use super::*;
    use crate::key;
    use crate::tests::{Behaviour, app_frame, client_key, control, fake_node, tagged_frame};

    type Polled = StreamConnection<NonBlockingTcp>;

    // --- twins of the blocking connection's tests, against the fake node ---
    //
    // Each runs the same scenario as the blocking test it is named after,
    // through a non-blocking kernel socket.

    #[test]
    fn a_request_gets_its_reply_batch() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut node = Polled::connect(addr, &key).unwrap();
        assert_eq!(node.public_key(), &key.verifying_key());
        assert_eq!(node.stream().get_ref().peer_addr().unwrap(), addr);

        assert_eq!(node.request_one(b"hello").unwrap(), b"hello");

        // The same over the pipelined pair, several requests in flight.
        for n in 2..=4u64 {
            node.send(&n.to_le_bytes()).unwrap();
        }
        for n in 2..=4u64 {
            assert_eq!(
                node.next_frame().unwrap(),
                Frame::Response(&n.to_le_bytes())
            );
            assert_eq!(node.next_frame().unwrap(), Frame::BatchEnd);
        }
    }

    #[test]
    fn a_body_is_carried_whatever_its_bytes() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut node = Polled::connect(addr, &key).unwrap();
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
        let mut node = Polled::connect(addr, &key).unwrap();

        let frames = node.request(b"x").unwrap();
        assert_eq!(frames, [b"x".to_vec(), b"again".to_vec()]);

        assert!(matches!(node.request_one(b"y"), Err(Error::Protocol(_))));
    }

    #[test]
    fn an_unknown_key_is_told_which_key_to_authorize() {
        let allowed = SigningKey::from_bytes(&[0x22; 32]).verifying_key();
        let addr = fake_node(allowed, Behaviour::Echo);
        let stranger = client_key();

        let err = Polled::connect(addr, &stranger).unwrap_err();
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
        let mut node = Polled::connect_timeout(addr, &key, timeout).unwrap();

        let started = Instant::now();
        let err = node.request(b"dropped").unwrap_err();
        assert!(started.elapsed() >= timeout);
        assert!(matches!(err, Error::NoReply { timeout: t } if t == timeout));
        assert!(err.to_string().contains("authorized_keys"), "{err}");
    }

    #[test]
    fn silence_during_the_handshake_is_no_reply_too() {
        // A listener whose backlog took the connection before anything
        // serves it: the challenge never comes.
        let key = client_key();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let timeout = Duration::from_millis(200);
        std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(timeout * 4);
        });

        let started = Instant::now();
        let err = Polled::connect_timeout(addr, &key, timeout).unwrap_err();
        assert!(started.elapsed() >= timeout);
        assert!(matches!(err, Error::NoReply { timeout: t } if t == timeout));
    }

    #[test]
    fn an_oversized_request_is_refused_and_the_connection_kept() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut node = Polled::connect(addr, &key).unwrap();

        let body = vec![0xAB; MAX_FRAME_SIZE];
        let err = node.send(&body).unwrap_err();
        assert!(matches!(err, Error::RequestTooLarge { len } if len == MAX_FRAME_SIZE + 1));
        assert_eq!(node.connection().unsent(), 0);

        assert_eq!(node.request_one(b"still here").unwrap(), b"still here");
        assert_eq!(node.request_one(&body[1..]).unwrap(), &body[1..]);
    }

    #[test]
    fn a_zero_tag_is_a_protocol_error_not_a_response() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::ZeroTag);
        let mut node = Polled::connect(addr, &key).unwrap();
        node.send(b"").unwrap();
        assert!(matches!(node.next_frame(), Err(Error::Protocol(_))));
    }

    #[test]
    fn heartbeats_do_not_defer_no_reply_forever() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::HeartbeatingSilent);
        let timeout = Duration::from_millis(200);
        let mut node = Polled::connect_timeout(addr, &key, timeout).unwrap();

        let started = Instant::now();
        let err = node.request(b"dropped").unwrap_err();
        assert!(started.elapsed() >= timeout);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(matches!(err, Error::NoReply { timeout: t } if t == timeout));
    }

    #[test]
    fn busy_and_engine_error_are_frames_when_pipelining_and_errors_from_request() {
        let key = client_key();

        let addr = fake_node(key.verifying_key(), Behaviour::Busy);
        let mut node = Polled::connect(addr, &key).unwrap();
        node.send(b"").unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::ServerBusy);
        assert!(matches!(node.request(b""), Err(Error::ServerBusy)));

        let addr = fake_node(key.verifying_key(), Behaviour::Failing);
        let mut node = Polled::connect(addr, &key).unwrap();
        node.send(b"").unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::EngineError);
        assert!(matches!(node.request(b""), Err(Error::EngineError)));
    }

    #[test]
    fn authenticate_reports_a_stream_that_is_not_a_node() {
        let key = client_key();
        let timeout = Duration::from_secs(5);

        // Accepted and closed without a word.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || drop(listener.accept().unwrap()));
        assert!(matches!(
            Polled::connect_timeout(addr, &key, timeout),
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
        assert!(matches!(
            Polled::connect_timeout(addr, &key, timeout),
            Err(Error::Disconnected)
        ));

        // A frame far larger than any handshake frame, refused on its
        // prefix, before its payload (which never comes) is waited for.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream.write_all(&512u32.to_le_bytes()).unwrap();
            // Keep the socket open until the client has answered.
            std::thread::sleep(Duration::from_millis(200));
        });
        let started = Instant::now();
        assert!(matches!(
            Polled::connect_timeout(addr, &key, timeout),
            Err(Error::Protocol(_))
        ));
        assert!(started.elapsed() < timeout);
    }

    #[test]
    fn a_closed_connection_is_reported() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Hangup);
        let mut node = Polled::connect(addr, &key).unwrap();
        assert!(matches!(node.next_frame(), Err(Error::Disconnected)));
    }

    #[test]
    fn an_oversized_frame_on_a_connection_is_frame_too_large() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Oversized);
        let mut node = Polled::connect(addr, &key).unwrap();
        node.send(b"").unwrap();
        let declared = MAX_FRAME_SIZE as u32 + 1;
        assert!(matches!(
            node.next_frame(),
            Err(Error::FrameTooLarge { declared: d, max: MAX_FRAME_SIZE }) if d == declared
        ));
        // No frame boundary left: it stays that way.
        assert!(matches!(
            node.next_frame(),
            Err(Error::FrameTooLarge { .. })
        ));
    }

    /// Twin of `authenticate_leaves_what_follows_in_the_stream`: a heartbeat
    /// on the heels of the node's ready is skipped, and the reply follows.
    #[test]
    fn a_heartbeat_right_after_ready_is_skipped() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::EagerHeartbeat);
        let mut node = Polled::connect(addr, &key).unwrap();
        assert_eq!(node.request_one(b"after").unwrap(), b"after");
    }

    /// Twin of `a_frame_from_frame_request_is_what_a_node_reads_from_send`:
    /// a request whose body an encoder writes in place.
    #[test]
    fn a_request_framed_by_an_encoder_gets_its_reply() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut node = Polled::connect(addr, &key).unwrap();
        node.send_with(MAX_REQUEST_BODY, |out: &mut [u8]| {
            // The widest body was promised: the encoder may index freely.
            assert!(out.len() >= MAX_REQUEST_BODY);
            out[..7].copy_from_slice(b"by hand");
            Ok::<_, Error>(7)
        })
        .unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::Response(b"by hand"));
        assert_eq!(node.next_frame().unwrap(), Frame::BatchEnd);
    }

    /// Twin of `into_stream_hands_over_an_authenticated_socket`.
    #[test]
    fn into_parts_hands_over_an_authenticated_stream() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::AdminLines);
        let (stream, connection) = Polled::connect(addr, &key).unwrap().into_parts();
        assert!(connection.is_ready());
        assert_eq!(connection.unsent(), 0);
        let mut stream = stream.into_inner();
        stream.set_nonblocking(false).unwrap();
        stream.write_all(b"STATUS\n").unwrap();
        let mut reply = String::new();
        stream.read_to_string(&mut reply).unwrap();
        assert_eq!(reply, "OK\n");
    }

    // --- beyond the blocking connection ---

    #[test]
    fn a_ready_connection_with_nothing_owed_is_never_timed_out() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let timeout = Duration::from_millis(50);
        let mut node = Polled::connect_timeout(addr, &key, timeout).unwrap();
        let started = Instant::now();
        while started.elapsed() < timeout * 4 {
            assert!(node.poll().unwrap().is_none());
        }
        // And a request on it still gets its reply, through the polled form.
        node.send(b"late").unwrap();
        let mut got = Vec::new();
        while !got.contains(&"BatchEnd".to_string()) {
            match node.poll().unwrap() {
                Some(Frame::Response(body)) => got.push(String::from_utf8(body.to_vec()).unwrap()),
                Some(other) => got.push(format!("{other:?}")),
                None => {}
            }
        }
        assert_eq!(got, ["late", "BatchEnd"]);
    }

    /// The exchange bench's shape: several state-only connections, each on
    /// its own stream, driven from one loop on the caller's own clock.
    #[test]
    fn several_state_only_connections_share_one_loop_and_one_clock() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut streams: Vec<NonBlockingTcp> = (0..3)
            .map(|_| NonBlockingTcp::connect(addr, DEFAULT_TIMEOUT).unwrap())
            .collect();
        let mut connections: Vec<PolledConnection> =
            (0..3).map(|_| PolledConnection::new(&key)).collect();
        let origin = Instant::now();
        let now = || u64::try_from(origin.elapsed().as_nanos()).unwrap();

        // The handshakes run side by side. The first connection's request
        // is sent before it is ready, and held until it is; the others
        // send theirs once ready.
        connections[0]
            .send(&mut streams[0], now(), b"early")
            .unwrap();
        let mut sent = [true, false, false];
        let mut replies: Vec<Vec<Vec<u8>>> = vec![Vec::new(); 3];
        let mut ended = [false; 3];
        while !ended.iter().all(|&e| e) {
            for (i, (connection, stream)) in connections.iter_mut().zip(&mut streams).enumerate() {
                if !sent[i] && connection.is_ready() {
                    connection.send(stream, now(), &[i as u8]).unwrap();
                    sent[i] = true;
                }
                match connection.poll(stream, now()).unwrap() {
                    Some(Frame::Response(body)) => replies[i].push(body.to_vec()),
                    Some(Frame::BatchEnd) => ended[i] = true,
                    Some(other) => panic!("unexpected {other:?}"),
                    None => {}
                }
            }
        }
        assert_eq!(
            replies,
            [vec![b"early".to_vec()], vec![vec![1]], vec![vec![2]]]
        );
    }

    /// One request loop over either transport, picked at runtime.
    #[test]
    fn a_session_loop_runs_over_either_connection() {
        fn round_trip(session: &mut dyn Session, body: &[u8]) -> Vec<u8> {
            session.send(body).unwrap();
            let Frame::Response(reply) = session.next_frame().unwrap() else {
                panic!("expected a response");
            };
            let reply = reply.to_vec();
            assert_eq!(session.next_frame().unwrap(), Frame::BatchEnd);
            assert_eq!(session.request(body).unwrap(), std::slice::from_ref(&reply));
            assert_eq!(session.request_one(body).unwrap(), reply);
            reply
        }
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut sessions: Vec<Box<dyn Session>> = vec![
            Box::new(Connection::connect(addr, &key).unwrap()),
            Box::new(Polled::connect(addr, &key).unwrap()),
        ];
        for session in &mut sessions {
            assert_eq!(round_trip(session.as_mut(), b"either"), b"either");
        }
    }

    // --- the buffer-fed stream ---

    /// The node's side of a handshake that accepts: the challenge and the
    /// ready, length prefixes included.
    fn node_handshake() -> Vec<u8> {
        [
            control(TransportResponse::Challenge { nonce: [0x5A; 32] }),
            control(TransportResponse::ServerReady),
        ]
        .concat()
    }

    /// A connection made ready over a buffer-fed stream, its challenge
    /// response consumed from the stream.
    fn ready(timeout: Duration) -> (PolledConnection, BufferStream) {
        let mut connection = PolledConnection::with_timeout(&client_key(), timeout);
        let mut stream = BufferStream::new();
        stream.push_received(&node_handshake());
        assert!(connection.poll(&mut stream, 0).unwrap().is_none());
        assert!(connection.is_ready());
        stream.consume_sent(stream.sent().len());
        (connection, stream)
    }

    /// Every frame `poll` hands out until it finds nothing, written out.
    fn drain_frames<S: ByteStream>(
        connection: &mut PolledConnection,
        stream: &mut S,
        now_ns: u64,
    ) -> Vec<String> {
        let mut got = Vec::new();
        while let Some(frame) = connection.poll(stream, now_ns).unwrap() {
            got.push(match frame {
                Frame::Response(body) => format!("Response({})", String::from_utf8_lossy(body)),
                other => format!("{other:?}"),
            });
        }
        got
    }

    #[test]
    fn a_frame_split_at_every_boundary_comes_out_whole() {
        let key = client_key();
        // The handshake, an eager heartbeat, and the reply batch to a
        // request sent before the node was ready.
        let wire = [
            node_handshake(),
            control(TransportResponse::Heartbeat),
            app_frame(b"body"),
            control(TransportResponse::BatchEnd),
        ]
        .concat();
        let request = app_frame(b"early");
        // Both ways in: through the stream, and fed directly.
        for fed in [false, true] {
            for split in 0..=wire.len() {
                let mut connection = PolledConnection::new(&key);
                let mut stream = BufferStream::new();
                connection.send(&mut stream, 0, b"early").unwrap();
                let mut got = Vec::new();
                for part in [&wire[..split], &wire[split..]] {
                    if fed {
                        connection.feed(part);
                    } else {
                        stream.push_received(part);
                    }
                    got.extend(drain_frames(&mut connection, &mut stream, 0));
                }
                assert_eq!(got, ["Response(body)", "BatchEnd"], "split at {split}");
                // The challenge response, then the request: the request was
                // held until the node was ready.
                let sent = stream.sent();
                let response_len = PREFIX_LEN + CHALLENGE_RESPONSE_LEN;
                assert_eq!(sent.len(), response_len + request.len());
                decode_challenge_response(&sent[PREFIX_LEN..response_len]).unwrap();
                assert_eq!(&sent[response_len..], request.as_slice());
            }
        }
    }

    #[test]
    fn a_request_is_held_until_the_node_is_ready() {
        let mut connection = PolledConnection::new(&client_key());
        let mut stream = BufferStream::new();
        connection.send(&mut stream, 0, b"early").unwrap();
        assert!(stream.sent().is_empty());

        // The challenge: only its answer goes out.
        stream.push_received(&control(TransportResponse::Challenge { nonce: [1; 32] }));
        assert!(connection.poll(&mut stream, 0).unwrap().is_none());
        assert_eq!(stream.sent().len(), PREFIX_LEN + CHALLENGE_RESPONSE_LEN);
        assert!(!connection.is_ready());
        stream.consume_sent(stream.sent().len());

        // The ready: now the request.
        stream.push_received(&control(TransportResponse::ServerReady));
        assert!(connection.poll(&mut stream, 0).unwrap().is_none());
        assert_eq!(stream.sent(), app_frame(b"early").as_slice());
    }

    #[test]
    fn a_refused_key_over_a_buffer_fed_stream() {
        let key = client_key();
        let mut connection = PolledConnection::new(&key);
        let mut stream = BufferStream::new();
        stream.push_received(
            &[
                control(TransportResponse::Challenge { nonce: [0; 32] }),
                control(TransportResponse::AuthFailed),
            ]
            .concat(),
        );
        assert!(matches!(
            connection.poll(&mut stream, 0),
            Err(Error::AuthFailed { public_key }) if public_key == key.verifying_key().to_bytes()
        ));
    }

    #[test]
    fn a_send_buffer_that_takes_nothing_and_then_a_few_bytes() {
        let (mut connection, mut stream) = ready(DEFAULT_TIMEOUT);
        stream.set_send_capacity(Some(0));
        connection.send(&mut stream, 0, b"hello").unwrap();
        connection.send(&mut stream, 0, b"world").unwrap();
        let both = [app_frame(b"hello"), app_frame(b"world")].concat();
        assert!(stream.sent().is_empty());
        assert_eq!(connection.unsent(), both.len());
        // A poll tries again, and the stream still takes nothing.
        assert!(connection.poll(&mut stream, 0).unwrap().is_none());
        assert_eq!(connection.unsent(), both.len());

        // A few bytes of room: a few bytes go, the rest stays queued.
        stream.set_send_capacity(Some(3));
        connection.flush(&mut stream).unwrap();
        assert_eq!(stream.sent(), &both[..3]);
        assert_eq!(connection.unsent(), both.len() - 3);

        // A third request queues behind the rest rather than jumping ahead.
        connection.send(&mut stream, 0, b"third").unwrap();
        stream.set_send_capacity(None);
        connection.flush(&mut stream).unwrap();
        assert_eq!(
            stream.sent(),
            [both.as_slice(), app_frame(b"third").as_slice()].concat()
        );
        assert_eq!(connection.unsent(), 0);
    }

    #[test]
    fn the_challenge_response_waits_for_room_too() {
        let mut connection = PolledConnection::new(&client_key());
        let mut stream = BufferStream::new();
        stream.set_send_capacity(Some(0));
        stream.push_received(&control(TransportResponse::Challenge { nonce: [3; 32] }));
        assert!(connection.poll(&mut stream, 0).unwrap().is_none());
        assert_eq!(connection.unsent(), PREFIX_LEN + CHALLENGE_RESPONSE_LEN);
        stream.set_send_capacity(Some(10));
        connection.flush(&mut stream).unwrap();
        assert_eq!(stream.sent().len(), 10);
        stream.set_send_capacity(None);
        assert!(connection.poll(&mut stream, 0).unwrap().is_none());
        assert_eq!(stream.sent().len(), PREFIX_LEN + CHALLENGE_RESPONSE_LEN);
        decode_challenge_response(&stream.sent()[PREFIX_LEN..]).unwrap();
    }

    /// A buffer-fed stream with a transmit ring of its own, lent through
    /// the in-place step as a user-space stack lends its socket's.
    struct RingStream {
        /// The ring: a fixed array, as a stack's transmit buffer is.
        ring: [u8; 128],
        /// Where the queued bytes start, and how many there are.
        head: usize,
        len: usize,
        /// Requests committed in place, and plain sends that took bytes.
        in_place: usize,
        plain: usize,
    }

    impl RingStream {
        fn new() -> Self {
            Self {
                ring: [0; 128],
                head: 0,
                len: 0,
                in_place: 0,
                plain: 0,
            }
        }

        /// Everything queued, as the wire would carry it, emptying the
        /// ring.
        fn transmit(&mut self) -> Vec<u8> {
            let out: Vec<u8> = (0..self.len)
                .map(|i| self.ring[(self.head + i) % self.ring.len()])
                .collect();
            self.head = (self.head + self.len) % self.ring.len();
            self.len = 0;
            out
        }
    }

    impl ByteStream for RingStream {
        fn send(&mut self, data: &[u8]) -> io::Result<usize> {
            let n = data.len().min(self.ring.len() - self.len);
            for (i, &byte) in data[..n].iter().enumerate() {
                let at = (self.head + self.len + i) % self.ring.len();
                self.ring[at] = byte;
            }
            self.len += n;
            if n > 0 {
                self.plain += 1;
            }
            Ok(n)
        }

        fn service(&mut self, _now_ns: u64) {}

        fn recv(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Ok(0)
        }

        fn send_in_place(&mut self, fill: impl FnOnce(&mut [u8]) -> usize) -> io::Result<usize> {
            if self.len == self.ring.len() {
                return Ok(0);
            }
            let tail = (self.head + self.len) % self.ring.len();
            // Contiguous to the end of the ring, or to the head if the
            // queued bytes wrap round past the tail.
            let end = if tail >= self.head && self.len < self.ring.len() {
                self.ring.len()
            } else {
                self.head
            };
            let lent = end - tail;
            let written = fill(&mut self.ring[tail..end]);
            if written > lent {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "overrun"));
            }
            self.len += written;
            if written > 0 {
                self.in_place += 1;
            }
            Ok(written)
        }
    }

    /// A connection made ready over a [`RingStream`], with the ring
    /// emptied, rewound to its start and its counters cleared. The
    /// handshake's answer goes through the plain send: it is not a request.
    fn ready_on_a_ring() -> (PolledConnection, RingStream) {
        let mut connection = PolledConnection::new(&client_key());
        let mut ring = RingStream::new();
        connection.feed(&node_handshake());
        assert!(connection.poll(&mut ring, 0).unwrap().is_none());
        assert!(connection.is_ready());
        assert_eq!(ring.transmit().len(), PREFIX_LEN + CHALLENGE_RESPONSE_LEN);
        ring.head = 0;
        ring.plain = 0;
        (connection, ring)
    }

    #[test]
    fn a_request_that_meets_the_rings_wrap_goes_through_the_scratch_buffer() {
        let (mut connection, mut ring) = ready_on_a_ring();

        // 40-byte bodies make 45-byte frames: two fit the 128-byte ring's
        // contiguous room in turn, framed where the ring sends them from.
        let body = |n: u8| [n; 40];
        let mut wire = Vec::new();
        for n in 0..2 {
            connection.send(&mut ring, 0, &body(n)).unwrap();
            wire.extend(ring.transmit());
        }
        assert_eq!((ring.in_place, ring.plain), (2, 0));
        assert_eq!(connection.unsent(), 0);

        // The ring's tail is now 38 bytes from its end: the third frame
        // does not fit there, so it is framed in the scratch buffer and
        // copied through the plain send, round the wrap.
        connection.send(&mut ring, 0, &body(2)).unwrap();
        assert_eq!((ring.in_place, ring.plain), (2, 1));
        assert_eq!(connection.unsent(), 0);
        wire.extend(ring.transmit());

        // And the next, at the ring's new tail, is in place again.
        connection.send(&mut ring, 0, &body(3)).unwrap();
        assert_eq!((ring.in_place, ring.plain), (3, 1));
        wire.extend(ring.transmit());

        let expected: Vec<u8> = (0..4).flat_map(|n| app_frame(&body(n))).collect();
        assert_eq!(wire, expected);
    }

    #[test]
    fn an_encoder_that_fails_queues_nothing() {
        let (mut connection, mut stream) = ready(DEFAULT_TIMEOUT);
        // Over the scratch buffer.
        let err = connection
            .send_with(&mut stream, 0, 8, |_: &mut [u8]| {
                Err::<usize, _>(Error::Protocol("mine".into()))
            })
            .unwrap_err();
        assert!(matches!(err, Error::Protocol(what) if what == "mine"));
        assert_eq!(connection.unsent(), 0);
        assert!(stream.sent().is_empty());

        // Over the ring's room: returned as it is, not retried.
        let (mut connection, mut ring) = ready_on_a_ring();
        let err = connection
            .send_with(&mut ring, 0, 8, |_: &mut [u8]| {
                Err::<usize, _>(Error::Protocol("mine".into()))
            })
            .unwrap_err();
        assert!(matches!(err, Error::Protocol(what) if what == "mine"));
        assert_eq!((ring.len, ring.in_place, ring.plain), (0, 0, 0));
        assert_eq!(connection.unsent(), 0);
    }

    #[test]
    fn an_encoder_never_sees_less_room_than_it_was_promised() {
        let (mut connection, mut ring) = ready_on_a_ring();
        // The widest body does not fit the 128-byte ring's room, however
        // empty: the scratch buffer frames it, though the body it writes
        // is short.
        connection
            .send_with(&mut ring, 0, MAX_REQUEST_BODY, |out: &mut [u8]| {
                assert!(out.len() >= MAX_REQUEST_BODY);
                out[..3].copy_from_slice(b"abc");
                Ok::<_, Error>(3)
            })
            .unwrap();
        assert_eq!((ring.in_place, ring.plain), (0, 1));
        assert_eq!(ring.transmit(), app_frame(b"abc"));

        // A bound that the room holds: framed in place.
        connection
            .send_with(&mut ring, 0, 16, |out: &mut [u8]| {
                assert!(out.len() >= 16);
                out[..3].copy_from_slice(b"def");
                Ok::<_, Error>(3)
            })
            .unwrap();
        assert_eq!((ring.in_place, ring.plain), (1, 1));
        assert_eq!(ring.transmit(), app_frame(b"def"));
    }

    #[test]
    fn the_deadline_runs_only_while_a_reply_is_owed_and_heartbeats_do_not_move_it() {
        let timeout = Duration::from_millis(100);
        let t = duration_ns(timeout);
        let (mut connection, mut stream) = ready(timeout);

        // Nothing owed: never timed out, however late.
        assert!(connection.poll(&mut stream, 10 * t).unwrap().is_none());

        // A request at 10t: its reply is owed by 11t.
        connection.send(&mut stream, 10 * t, b"x").unwrap();
        assert!(
            connection
                .poll(&mut stream, 10 * t + t / 2)
                .unwrap()
                .is_none()
        );
        // A heartbeat just before the deadline does not push it back.
        stream.push_received(&control(TransportResponse::Heartbeat));
        assert!(connection.poll(&mut stream, 11 * t - 1).unwrap().is_none());
        assert!(matches!(
            connection.poll(&mut stream, 11 * t),
            Err(Error::NoReply { timeout: d }) if d == timeout
        ));

        // A reply ends the wait; the batch's end clears what was owed.
        let (mut connection, mut stream) = ready(timeout);
        connection.send(&mut stream, 0, b"x").unwrap();
        stream.push_received(&app_frame(b"x"));
        assert_eq!(
            connection.poll(&mut stream, t - 1).unwrap(),
            Some(Frame::Response(b"x"))
        );
        // Still owed its batch end: a fresh wait, from the next empty poll.
        assert!(connection.poll(&mut stream, 2 * t).unwrap().is_none());
        assert!(connection.poll(&mut stream, 3 * t - 1).unwrap().is_none());
        stream.push_received(&control(TransportResponse::BatchEnd));
        assert_eq!(
            connection.poll(&mut stream, 3 * t - 1).unwrap(),
            Some(Frame::BatchEnd)
        );
        assert!(connection.poll(&mut stream, 100 * t).unwrap().is_none());

        // The handshake is owed from the first poll.
        let mut connection = PolledConnection::with_timeout(&client_key(), timeout);
        let mut stream = BufferStream::new();
        assert!(connection.poll(&mut stream, 5).unwrap().is_none());
        assert!(matches!(
            connection.poll(&mut stream, 5 + t),
            Err(Error::NoReply { .. })
        ));
    }

    #[test]
    fn a_handshake_frame_too_long_is_refused_on_its_prefix() {
        let mut connection = PolledConnection::new(&client_key());
        let mut stream = BufferStream::new();
        // Within the frame limit, past any handshake frame: refused before
        // its payload arrives, as the blocking handshake refuses it.
        stream.push_received(&512u32.to_le_bytes());
        assert!(matches!(
            connection.poll(&mut stream, 0),
            Err(Error::Protocol(_))
        ));

        // Over the frame limit: the same error as after the handshake.
        let mut connection = PolledConnection::new(&client_key());
        let mut stream = BufferStream::new();
        let declared = MAX_FRAME_SIZE as u32 + 1;
        stream.push_received(&declared.to_le_bytes());
        assert!(matches!(
            connection.poll(&mut stream, 0),
            Err(Error::FrameTooLarge { declared: d, max: MAX_FRAME_SIZE }) if d == declared
        ));

        // The longest handshake frame is waited for.
        let mut connection = PolledConnection::new(&client_key());
        let mut stream = BufferStream::new();
        stream.push_received(&(MAX_HANDSHAKE_FRAME as u32).to_le_bytes());
        assert!(connection.poll(&mut stream, 0).unwrap().is_none());
    }

    #[test]
    fn a_refused_request_is_owed_nothing_more() {
        let timeout = Duration::from_millis(10);
        let t = timeout.as_nanos() as u64;
        let (mut connection, mut stream) = ready(timeout);
        connection.send(&mut stream, 0, b"refused").unwrap();
        // The node refuses it with a busy frame alone, no batch end.
        stream.push_received(&tagged_frame(TAG_SERVER_BUSY, b""));
        assert_eq!(
            connection.poll(&mut stream, 0).unwrap(),
            Some(Frame::ServerBusy)
        );
        // Idle well past the read timeout: nothing is owed, so nothing
        // times out.
        for now in [t, 2 * t, 10 * t] {
            assert!(connection.poll(&mut stream, now).unwrap().is_none());
        }

        // An engine error is still owed its batch end.
        connection.send(&mut stream, 10 * t, b"failing").unwrap();
        stream.push_received(&control(TransportResponse::EngineError));
        assert_eq!(
            connection.poll(&mut stream, 10 * t).unwrap(),
            Some(Frame::EngineError)
        );
        assert!(connection.poll(&mut stream, 10 * t).unwrap().is_none());
        assert!(matches!(
            connection.poll(&mut stream, 12 * t),
            Err(Error::NoReply { .. })
        ));
    }

    #[test]
    fn a_close_is_reported_after_the_frames_before_it() {
        let (mut connection, mut stream) = ready(DEFAULT_TIMEOUT);
        stream.push_received(&[app_frame(b"last"), control(TransportResponse::BatchEnd)].concat());
        stream.close();
        assert_eq!(
            drain_frames_until_err(&mut connection, &mut stream),
            ["Response(last)", "BatchEnd"]
        );
        assert!(matches!(
            connection.poll(&mut stream, 0),
            Err(Error::Disconnected)
        ));
    }

    /// Frames until `poll` fails, which it must.
    fn drain_frames_until_err(
        connection: &mut PolledConnection,
        stream: &mut BufferStream,
    ) -> Vec<String> {
        let mut got = Vec::new();
        loop {
            match connection.poll(stream, 0) {
                Ok(Some(Frame::Response(body))) => {
                    got.push(format!("Response({})", String::from_utf8_lossy(body)));
                }
                Ok(Some(other)) => got.push(format!("{other:?}")),
                Ok(None) => panic!("a closed stream found nothing"),
                Err(_) => return got,
            }
        }
    }

    #[test]
    fn a_frame_no_reply_carries_is_consumed_with_its_error() {
        let (mut connection, mut stream) = ready(DEFAULT_TIMEOUT);
        stream.push_received(
            &[
                tagged_frame(0x00, b"zeroed"),
                control(TransportResponse::BatchEnd),
            ]
            .concat(),
        );
        assert!(matches!(
            connection.poll(&mut stream, 0),
            Err(Error::Protocol(_))
        ));
        assert_eq!(
            connection.poll(&mut stream, 0).unwrap(),
            Some(Frame::BatchEnd)
        );
    }

    #[test]
    fn fed_bytes_beyond_the_buffer_grow_it() {
        let (mut connection, mut stream) = ready(DEFAULT_TIMEOUT);
        // More replies in one completion than the receive buffer holds.
        let one = app_frame(&[0x42; 1000]);
        let count = RECEIVE_BUFFER / one.len() + 4;
        connection.feed(&one.repeat(count));
        let mut seen = 0;
        while let Some(frame) = connection.poll(&mut stream, 0).unwrap() {
            assert_eq!(frame, Frame::Response(&[0x42; 1000]));
            seen += 1;
        }
        assert_eq!(seen, count);
    }

    #[test]
    fn the_clock_is_read_once_per_interval() {
        let mut clock = CachedClock::start();
        let first = clock.refresh();
        std::thread::sleep(Duration::from_millis(2));
        for _ in 1..CLOCK_REFRESH_POLLS {
            assert_eq!(clock.tick(), first);
        }
        assert!(clock.tick() > first);
    }

    /// A clock on an origin already past (a NIC handle's, opened before
    /// the connection) reads the time since that origin from its first
    /// reading, so its readings and the origin's owner's agree.
    #[test]
    fn a_clock_on_an_earlier_origin_counts_from_it() {
        let origin = Instant::now();
        std::thread::sleep(Duration::from_millis(5));
        let mut clock = CachedClock::counting_from(origin);
        let at_start = clock.tick();
        assert!(at_start >= 5_000_000, "{at_start}");
        let reading = clock.refresh();
        assert!(reading >= at_start);
        assert!(reading <= duration_ns(origin.elapsed()));
    }
}
