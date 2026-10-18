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
//! Every request carries an ID ([`RequestId`]) the connection allocates,
//! from 1 upward, and returns from every send. The node answers every
//! request exactly once, with a final frame naming it: the end of its
//! reply batch, [`Frame::ServerBusy`] (shed at ingress, safe to resend),
//! or [`Frame::Refused`] (the application refused it, with the reason).
//! Replies to requests the node accepted arrive in request order; a
//! request it did not accept may be answered ahead of requests sent
//! before it, but never in the middle of another request's reply. A
//! connection pairs each final frame with its request by ID and reports
//! a final frame for no outstanding request as [`Error::Protocol`].
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
//! flight or want to time the reply frame itself: `send` returns the
//! request's ID, and each final frame names the ID it answers.
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
//!   [`REQUEST_HEADER_LEN`] bytes, and [`seal_request`] writes the header,
//!   the request's ID among it, in front of it ([`frame_request`] does
//!   both around a closure). The ID is the caller's to choose here, and
//!   to keep unique among its requests in flight.
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
//! Between the two sits the polled connection, the blocking connection's
//! protocol over a non-blocking [`ByteStream`] with no blocking call in
//! it:
//!
//! - [`PolledConnection`] is the protocol state alone (the handshake, a
//!   receive buffer frames are split in, a scratch buffer, the read
//!   deadline) with no stream and no clock: every call takes the stream
//!   and the caller's monotonic time, and returns at once. For a program
//!   with several connections on one NIC, or a clock of its own, or bytes
//!   handed to it by an `io_uring` completion, which
//!   [`PolledConnection::feed`] takes directly. A request is framed where
//!   the stream lends room for it ([`ByteStream::send_in_place`]), or in
//!   the scratch buffer when it lends none.
//! - [`StreamConnection`] owns one stream and a cached clock and offers
//!   the blocking connection's calls, spinning on the stream where those
//!   block, and a non-blocking [`poll`](StreamConnection::poll) besides.
//! - [`NonBlockingTcp`] is a kernel socket as a byte stream, and
//!   [`BufferStream`] one whose bytes the caller's own I/O loop moves.
//! - [`Session`] is what a request loop needs (send, next frame,
//!   request), implemented by [`Connection`] and [`StreamConnection`],
//!   so the loop is written once and the transport picked at runtime.
//!
//! ## Kernel bypass: the `dpdk` feature
//!
//! Off by default. It runs the polled connection over DPDK, on
//! `melin-dpdk`'s client NIC handle, `NicHandle`: the node's own DPDK
//! transport, listening on no port, re-exported here with its types so a
//! client needs no direct dependency on `melin-dpdk`. Its configuration,
//! `NicConfig`, is the node's own (the NIC half of `melin-dpdk`'s
//! `DpdkConfig`, one field per `--dpdk-*` flag), not a copy of it. Two
//! shapes:
//!
//! - **One connection**: `StreamConnection::connect_dpdk` brings a NIC up
//!   from a `NicConfig`, dials and authenticates; the connection owns the
//!   NIC. `StreamConnection::connect_dpdk_on` does the same on a
//!   `NicHandle` already up, owned or lent as `&mut`, so a client
//!   reconnects without touching EAL, which initialises once per process.
//! - **Several connections on one NIC**: the caller keeps the handle, dials
//!   each stream with `NicHandle::dial`, and drives one
//!   [`PolledConnection`] per stream, handing each call the stream's view,
//!   `NicHandle::stream(&id)`, which is a [`ByteStream`] and borrows the
//!   handle for that call only. `NicHandle::service` advances every stream
//!   at once, so it is called once per loop iteration, at the caller's own
//!   monotonic nanosecond time (a calibrated TSC's will do). Dials are
//!   sequential: a dial returns only once its connection is established
//!   or has failed, since a DPDK node takes only a few handshakes at once
//!   on a port and refuses a SYN beyond them.
//!
//! Either way the stream's in-place send frames a request straight into
//! the socket's transmit buffer. A NIC that cannot be brought up is
//! [`Error::Nic`]: a variant in every build, with or without the feature,
//! so a match on [`Error`] compiles the same whichever crate in a build
//! turns the feature on. A dial that fails is [`Error::Connect`].
//!
//! Turning it on links `melin-dpdk`, which is BUSL-1.1, not Apache-2.0
//! like this crate, into the binary; without it nothing of `melin-dpdk` is
//! compiled. The build host needs libdpdk (found through `pkg-config`) and
//! libclang, and the host it runs on a DPDK-capable port and hugepages. A
//! client beside a node needs an EAL file prefix of its own, which the
//! handle adds (`melin-client`) when the EAL arguments name none, and a
//! client on a fabric that assigns MACs (an AWS ENI) must be given the
//! server's MAC; `NicConfig`'s fields say why. A DPDK client thread spins
//! at full load, so pin it to a core of its own.
//!
//! ## Silence
//!
//! A node answers every request, including one it refuses (a key whose
//! role may not perform the operation, a request laid out for another
//! version of the node's application: [`Error::Refused`], with the
//! application's reason) and one it sheds under load
//! ([`Error::ServerBusy`]); both leave the connection usable. Silence
//! therefore means the node or the network: a read that waits longer
//! than the read timeout is [`Error::NoReply`]. A node heartbeats idle
//! connections, and a heartbeat is not an answer: it does not push the
//! timeout back.
//!
//! The node and the client must speak the same client protocol
//! ([`CLIENT_PROTOCOL_VERSION`]): a node of another release is refused
//! during the handshake, before anything is signed, as
//! [`Error::ProtocolVersion`]. Upgrade clients with the nodes.
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
    BATCH_END_FRAME_LEN, BadChallenge, CHALLENGE_RESPONSE_LEN, TAG_APP, TAG_AUTH_FAILED,
    TAG_BATCH_END, TAG_BATCH_END_DEGRADED, TAG_REFUSED, TAG_RESPONSE_HEARTBEAT, TAG_SERVER_BUSY,
    TAG_SERVER_READY, decode_challenge, decode_refused, encode_challenge_response,
    terminal_request_id,
};

#[cfg(feature = "dpdk")]
mod dpdk;
pub mod key;
mod outstanding;
mod polled;

use outstanding::Outstanding;

// A request's ID and a refusal's kind are the wire protocol's, re-exported
// so a client needs no direct dependency on the wire crate to name them;
// so is the protocol version a node must speak.
pub use melin_wire_protocol::control::{RefusalKind, RequestId};
pub use melin_wire_protocol::control_codec::CLIENT_PROTOCOL_VERSION;

// The polled connection lives in a module of its own, re-exported item by
// item so it sits beside the blocking connection at the crate's root.
pub use polled::{
    BufferStream, ByteStream, NonBlockingTcp, PolledConnection, Session, StreamConnection,
};

// Kernel bypass: the single-stream owned form of a NIC stream, and the NIC
// handle's own types, re-exported so that a client needs no direct
// dependency on `melin-dpdk` (nor a second copy of the node's DPDK
// configuration: `NicConfig` is the node's own). The MAC parser is here for
// the binaries that map `--dpdk-peer-mac` and `--dpdk-gateway-mac` onto it.
#[cfg(feature = "dpdk")]
pub use dpdk::DpdkStream;
#[cfg(feature = "dpdk")]
pub use melin_dpdk::{
    MacParseError, NextHop, NicClock, NicConfig, NicHandle, NicStream, PeerMacSource, StreamId,
    dial_addr, fmt_mac, try_parse_mac,
};

// The key types a caller needs to hold, so that depending on this crate
// is enough to authenticate.
pub use ed25519_dalek::{SigningKey, VerifyingKey};
// The bound on a frame. A caller sizing its widest request body wants
// `MAX_REQUEST_BODY`, which already takes off the protocol's tag and the
// request's ID.
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

use framing::{FrameDecoder, FrameTooLarge, PREFIX_LEN, request_payload_len, request_tag_and_id};

/// Read and connect timeout used by [`Connection::connect`] and
/// [`Connection::connect_by`]. Generous for a round trip anywhere on a
/// LAN; short enough that a node or network gone quiet becomes an error
/// rather than a hang.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Everything that can go wrong between a client and a node.
#[derive(Debug)]
pub enum Error {
    /// The socket failed underneath the protocol.
    Io(io::Error),
    /// The node could not be reached at `addr`. Over DPDK (the `dpdk`
    /// feature) the source tells a dial nothing answered
    /// ([`io::ErrorKind::TimedOut`], its message naming the MAC the SYN
    /// went to as the first thing to check) from one refused
    /// ([`io::ErrorKind::ConnectionRefused`]).
    Connect { addr: SocketAddr, source: io::Error },
    /// The DPDK NIC could not be brought up: EAL initialisation (the
    /// message names the file prefix, the usual culprit when another DPDK
    /// process runs on the host), a missing port, the mbuf pool, or the
    /// port's configuration. Only the `dpdk` feature's constructors return
    /// it; the variant exists in every build, so that a match on this type
    /// compiles the same whichever crate in a build turns the feature on.
    Nic(io::Error),
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
    /// Nothing arrived within the read timeout: the node, or the network
    /// to it, stopped answering. The connection is to be dropped.
    NoReply { timeout: Duration },
    /// The node closed the connection.
    Disconnected,
    /// The node sent something the protocol does not allow here: a frame
    /// no reply carries, one out of place in the handshake, an answer for
    /// a request not outstanding, or a final frame inside another
    /// request's reply. The connection is to be dropped.
    Protocol(String),
    /// The node speaks client protocol `node`, this build `client`
    /// ([`CLIENT_PROTOCOL_VERSION`]): found in the node's challenge,
    /// before anything was signed. Protocol 0 is a node of a release
    /// before request IDs. Upgrade clients with the nodes.
    ProtocolVersion { node: u16, client: u16 },
    /// A length prefix from the node declared a payload of `declared`
    /// bytes, over the limit of `max`. Past it the stream has no frame
    /// boundary left: a [`FrameDecoder`] that reported it is poisoned,
    /// and the connection is to be dropped, as after
    /// [`Protocol`](Error::Protocol). A variant of its own so a caller
    /// can tell the two apart (and log the length) without asking the
    /// decoder, which the `Result` of [`next_reply`] still borrows.
    FrameTooLarge { declared: u32, max: usize },
    /// The request, with the protocol's tag and its ID in front of it,
    /// would not fit in one frame of [`MAX_FRAME_SIZE`] bytes; nothing was
    /// sent
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
    /// The node shed the request at ingress, under load: it was not
    /// applied, and may be resent. Requests sent after it may have been
    /// applied, so a caller whose requests depend on each other's order
    /// waits for the replies it is owed and resends from this one on. The
    /// connection is still good.
    ServerBusy,
    /// The node's application refused the request, which was not applied.
    /// `reason` is the application's text, for people. The connection is
    /// still good.
    Refused { kind: RefusalKind, reason: String },
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
            Error::Nic(e) => write!(f, "cannot bring the DPDK NIC up: {e}"),
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
                "no reply within {:.1}s: the node or the network stopped answering",
                timeout.as_secs_f64()
            ),
            Error::Disconnected => f.write_str("the node closed the connection"),
            Error::Protocol(what) => write!(f, "protocol violation: {what}"),
            Error::ProtocolVersion { node, client } => write!(
                f,
                "the node speaks client protocol {node}, this client {client}: upgrade clients \
                 with the nodes"
            ),
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
            Error::ServerBusy => {
                f.write_str("the node is busy: the request was not applied, resend it")
            }
            Error::Refused { kind, reason } => {
                let kind = match kind {
                    RefusalKind::PermissionDenied => "not permitted",
                    RefusalKind::Malformed => "malformed",
                };
                write!(f, "the node refused the request ({kind}): {reason}")
            }
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
            Error::Io(e) | Error::Connect { source: e, .. } | Error::Nic(e) => Some(e),
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
///
/// Every request gets exactly one final frame naming it: `BatchEnd`,
/// `ServerBusy` or `Refused`. `Response` frames carry no ID: they belong
/// to the next `BatchEnd`, and nothing but heartbeats falls between them
/// and it.
#[derive(Debug, PartialEq, Eq)]
pub enum Frame<'a> {
    /// An application response: its body, the application's bytes with
    /// the protocol's framing stripped. Borrowed from the connection's
    /// buffer, valid until the next read.
    Response(&'a [u8]),
    /// The last frame of request `id`'s reply batch, saying what backs
    /// the reply. A batch may hold no `Response` at all: the application
    /// had nothing to say, or filtered the request.
    BatchEnd { id: RequestId, ack: Ack },
    /// Request `id` was shed at ingress, under load: not applied, safe to
    /// resend. The connection stays open.
    ServerBusy { id: RequestId },
    /// The application refused request `id`, which was not applied:
    /// `kind` says why, `reason` is the application's text, for people.
    /// The connection stays open.
    Refused {
        id: RequestId,
        kind: RefusalKind,
        reason: &'a str,
    },
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
    BatchEnd { id: RequestId, ack: Ack },
    /// As [`Frame::ServerBusy`].
    ServerBusy { id: RequestId },
    /// As [`Frame::Refused`].
    Refused {
        id: RequestId,
        kind: RefusalKind,
        reason: &'a str,
    },
}

impl<'a> Reply<'a> {
    /// The frame a connection hands over for this reply; `None` for a
    /// heartbeat, which never surfaces.
    fn frame(self) -> Option<Frame<'a>> {
        match self {
            Reply::Heartbeat => None,
            Reply::Response(body) => Some(Frame::Response(body)),
            Reply::BatchEnd { id, ack } => Some(Frame::BatchEnd { id, ack }),
            Reply::ServerBusy { id } => Some(Frame::ServerBusy { id }),
            Reply::Refused { id, kind, reason } => Some(Frame::Refused { id, kind, reason }),
        }
    }
}

/// One frame's payload from the node, told apart without I/O: the
/// decision [`Connection::next_frame`] makes on each frame it reads, for
/// a program that reads its own — a gateway session on `io_uring`, a
/// load generator on a user-space TCP stack — so it need not know the
/// protocol's tags.
///
/// A tag that is not one of those a reply may carry — the handshake's
/// tags, which are over before any reply, and any the protocol does not
/// define — is [`Error::Protocol`]. So is an empty frame, and `0x00` with
/// it: a zeroed buffer on the wire is a loud error, not an application
/// response. A final frame's length is checked exactly: a batch end or
/// busy frame one byte short of its ID or one byte long, and a refusal
/// whose kind names none, whose reason is not UTF-8 or does not fill the
/// rest of the frame exactly, are [`Error::Protocol`] too.
///
/// The ID of a final frame is read here; whether it names a request
/// outstanding on the connection is the connection's to check, as
/// [`Connection`] and [`PolledConnection`] do.
pub fn classify(payload: &[u8]) -> Result<Reply<'_>, Error> {
    let Some((&tag, body)) = payload.split_first() else {
        return Err(Error::Protocol("empty frame".into()));
    };
    match tag {
        TAG_APP => Ok(Reply::Response(body)),
        TAG_RESPONSE_HEARTBEAT => Ok(Reply::Heartbeat),
        TAG_BATCH_END => Ok(Reply::BatchEnd {
            id: id_frame(payload, "batch end")?,
            ack: Ack::Policy,
        }),
        TAG_BATCH_END_DEGRADED => Ok(Reply::BatchEnd {
            id: id_frame(payload, "degraded batch end")?,
            ack: Ack::PrimaryOnly,
        }),
        TAG_SERVER_BUSY => Ok(Reply::ServerBusy {
            id: id_frame(payload, "busy frame")?,
        }),
        TAG_REFUSED => {
            let refused = decode_refused(payload)
                .map_err(|e| Error::Protocol(format!("{e} in a response frame")))?;
            Ok(Reply::Refused {
                id: refused.id,
                kind: refused.kind,
                reason: refused.reason,
            })
        }
        tag => Err(Error::Protocol(format!(
            "unexpected tag {tag:#04x} in a response frame"
        ))),
    }
}

/// The ID of a final frame whose payload is its tag and the ID alone,
/// through the one decoder of a final frame's ID, its length checked
/// exactly.
fn id_frame(payload: &[u8], what: &str) -> Result<RequestId, Error> {
    match terminal_request_id(payload) {
        Some(id) if payload.len() == BATCH_END_FRAME_LEN - PREFIX_LEN => Ok(id),
        _ => Err(Error::Protocol(format!(
            "a {}-byte {what}, not {}",
            payload.len(),
            BATCH_END_FRAME_LEN - PREFIX_LEN
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
/// idle connections would keep a caller waiting forever on a node that
/// has stopped answering. And so is the pairing of each final frame with
/// the request whose ID it names.
///
/// The handshake's frames come out of the same decoder before any reply:
/// take them with [`FrameDecoder::next`] and feed them to a
/// [`Handshake`], then switch to this function once it is done.
///
/// ```
/// use std::time::{Duration, Instant};
/// use melin_client::framing::FrameDecoder;
/// use melin_client::{Ack, Error, Reply, RequestId, next_reply};
///
/// # use melin_wire_protocol::control_codec::{TAG_APP, TAG_BATCH_END, TAG_RESPONSE_HEARTBEAT};
/// # let stream = [
/// #     &[1, 0, 0, 0, TAG_RESPONSE_HEARTBEAT][..],
/// #     &[3, 0, 0, 0, TAG_APP, b'o', b'k'],
/// #     &[9, 0, 0, 0, TAG_BATCH_END, 1, 0, 0, 0, 0, 0, 0, 0],
/// # ]
/// # .concat();
/// // What an I/O loop received, in arbitrary chunks: a heartbeat, then a
/// // response and the end of its batch, for the request sent as ID 1.
/// let received = [&stream[..6], &stream[6..9], &stream[9..]];
/// let sent = RequestId(1);
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
///             Reply::BatchEnd { id, .. } if id != sent => {
///                 return Err(Error::Protocol(format!("an answer for {id:?}")));
///             }
///             Reply::BatchEnd { ack: Ack::Policy, .. } => break 'io,
///             // Only the primary's disk holds it: this caller wants the
///             // policy's guarantee, so the request is unconfirmed.
///             Reply::BatchEnd { ack: Ack::PrimaryOnly, .. } => {
///                 return Err(Error::Degraded { frames: responses });
///             }
///             Reply::ServerBusy { .. } => return Err(Error::ServerBusy),
///             Reply::Refused { kind, reason, .. } => {
///                 return Err(Error::Refused { kind, reason: reason.to_string() });
///             }
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
/// read: a node, or the network to it, can stop answering, and a read
/// with no deadline would then wait forever. The timeout bounds a wait
/// for a reply, nothing else: a connection that sends nothing reads
/// nothing, and is not timed out by this crate.
///
/// Requests are numbered from 1 ([`send`](Self::send) returns each one's
/// ID), and every final frame [`next_frame`](Self::next_frame) hands over
/// is checked against the requests outstanding: an answer for none, or a
/// busy or refusal inside another request's reply, is
/// [`Error::Protocol`].
///
/// The node has a timeout of its own: it closes a connection that has
/// sent it nothing for longer than its configured connection timeout,
/// and its heartbeats to the client do not count. A client kept for
/// longer than that sends something within the window, or finds
/// [`Error::Disconnected`] on its next request.
///
/// After any error but two the connection's framing can no longer be
/// trusted (a timeout may have cut a frame in half); drop it and connect
/// again. The two are answers to one request, which leave the connection
/// usable for the next: [`Error::ServerBusy`] and [`Error::Refused`]
/// (and [`Error::Degraded`], a reply that did arrive).
pub struct Connection {
    reader: BlockingFrameReader<TcpStream>,
    writer: BlockingFrameWriter<TcpStream>,
    /// The socket itself, for options and for handing over to a caller.
    /// The reader and writer hold duplicates of the same descriptor, so
    /// an option set here applies to them.
    stream: TcpStream,
    read_timeout: Duration,
    public_key: VerifyingKey,
    /// The requests sent and not yet answered, and the next one's ID.
    outstanding: Outstanding,
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
            outstanding: Outstanding::default(),
        })
    }

    /// Keep trying to connect until `deadline`, for a node that is still
    /// starting. Retries what time can fix — a refused connection, a
    /// listener whose backlog took the connection before the node was
    /// serving (the handshake then times out) — and gives up at once on
    /// a refused key or a node on another client protocol
    /// ([`Error::ProtocolVersion`]). The connection returned reads with
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
                Err(mismatch @ Error::ProtocolVersion { .. }) => return Err(mismatch),
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
    /// sequence carried in the request body; a request ID means nothing
    /// past its connection.
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

    /// Send one request, the application's bytes, as a request frame,
    /// flushed, and return the ID the connection gave it: the next in
    /// order, from 1.
    ///
    /// The body is copied once in user space, into the writer's buffer;
    /// there is no staging buffer in between. A body that would take the
    /// frame over [`MAX_FRAME_SIZE`] is [`Error::RequestTooLarge`], and
    /// nothing is written (nor an ID taken): the node would drop the
    /// connection on it.
    pub fn send(&mut self, body: &[u8]) -> Result<RequestId, Error> {
        // Not built on `seal_request`: that frames into a caller's
        // buffer, and here the body would then be copied a second time,
        // into the writer's. The limit and the header's layout are the
        // shared ones.
        request_payload_len(body.len())?;
        let header = request_tag_and_id(self.outstanding.next_id());
        self.writer.write_frame_parts(&[&header, body])?;
        self.writer.flush()?;
        Ok(self.outstanding.sent())
    }

    /// The next frame from the node, heartbeats skipped. Blocks up to
    /// the read timeout: heartbeats do not extend it, or a node that
    /// heartbeats idle connections more often than the timeout would
    /// keep a caller waiting forever on a node that has stopped
    /// answering. A final frame is checked against the requests
    /// outstanding (see the type's docs).
    pub fn next_frame(&mut self) -> Result<Frame<'_>, Error> {
        // Each read re-arms the socket's timeout, so silence is measured
        // against this deadline; a heartbeat that arrives past it is the
        // node saying "still nothing", which is what NoReply means. The
        // wait can overshoot the timeout by up to one heartbeat gap —
        // the deadline is only checked when a frame arrives.
        let deadline = Instant::now() + self.read_timeout;
        loop {
            // Decide on the classification alone, then borrow the frame
            // back to return it: a borrow that lives across the loop and
            // is conditionally returned is what the borrow checker cannot
            // follow.
            let reply = classify(raw_frame(&mut self.reader, self.read_timeout)?)?;
            self.outstanding.observe(&reply)?;
            if !matches!(reply, Reply::Heartbeat) {
                break;
            }
            if Instant::now() >= deadline {
                return Err(Error::NoReply {
                    timeout: self.read_timeout,
                });
            }
        }
        Ok(reclassified(self.reader.frame()))
    }

    /// Send one request and collect the application frames of its reply
    /// batch, in order, with what backs them. A batch may hold none (the
    /// application had nothing to say) or several (an acknowledgement and
    /// the reports the request caused, say).
    ///
    /// Assumes nothing else is in flight on the connection: an answer for
    /// another request is [`Error::Protocol`], not a reply taken for this
    /// one. A busy or refused request is [`Error::ServerBusy`] or
    /// [`Error::Refused`], and the connection stays usable.
    pub fn request_batch(&mut self, body: &[u8]) -> Result<Batch, Error> {
        let id = self.send(body)?;
        polled::collect_batch(self, id)
    }

    /// [`request_batch`](Self::request_batch) for a caller that takes
    /// only a reply backed as the node's ack policy requires: the frames
    /// of a batch that ends in [`Ack::Policy`]. A batch only the
    /// primary's disk backs is [`Error::Degraded`], which carries its
    /// frames, so that a weaker reply is never taken for a full one
    /// unawares.
    pub fn request(&mut self, body: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        polled::policy_frames(self.request_batch(body)?)
    }

    /// [`request`](Self::request) for the common case of exactly one
    /// frame in reply; any other count is a protocol error.
    pub fn request_one(&mut self, body: &[u8]) -> Result<Vec<u8>, Error> {
        polled::one_frame(self.request(body)?)
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
}

/// One frame's payload off `reader`, with the transport's outcomes
/// mapped: a clean close is [`Error::Disconnected`], a read timed out
/// under `timeout` is [`Error::NoReply`]. A function of the reader alone,
/// not of the connection, so the connection's other fields stay free
/// while the frame is borrowed.
// Kept on the blocking reader rather than a `FrameDecoder`: the reader
// parses in its own read buffer, where a decoder would need a second copy
// of every byte read off the socket.
fn raw_frame(
    reader: &mut BlockingFrameReader<TcpStream>,
    timeout: Duration,
) -> Result<&[u8], Error> {
    match reader.read_frame() {
        Ok(Some(frame)) => Ok(frame),
        Ok(None) => Err(Error::Disconnected),
        Err(e) => Err(io_error(e, timeout)),
    }
}

/// The frame a payload already classified, accounted for and found not
/// to be a heartbeat holds, borrowed back for the caller: what both
/// connections return once their loop over the payloads they read has
/// decided on one (see [`Connection::next_frame`]). Classifying again is a
/// few compares, a UTF-8 check for a refusal's reason, and spares a second
/// description of every reply as positions.
pub(crate) fn reclassified(payload: &[u8]) -> Frame<'_> {
    classify(payload)
        .ok()
        .and_then(Reply::frame)
        .expect("a payload classified as a frame moments ago")
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
    /// or [`Error::AuthFailed`]. A challenge from a node speaking another
    /// client protocol than [`CLIENT_PROTOCOL_VERSION`] (a protocol-0
    /// challenge, which carries no version, among them) is
    /// [`Error::ProtocolVersion`], before anything is signed. Anything
    /// else, in either place, is [`Error::Protocol`], as is a frame after
    /// the handshake is done.
    pub fn feed(&mut self, payload: &[u8]) -> Result<Step<'_>, Error> {
        match &self.state {
            HandshakeState::Challenge { key } => {
                let nonce = decode_challenge(payload).map_err(|e| match e {
                    BadChallenge::Version { node } => Error::ProtocolVersion {
                        node,
                        client: CLIENT_PROTOCOL_VERSION,
                    },
                    BadChallenge::NotAChallenge { len, tag } => Error::Protocol(format!(
                        "expected an auth challenge, got a {len}-byte frame with tag {tag:?}"
                    )),
                })?;
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

/// The longest frame the node sends during the handshake. Its two frames
/// are a tag, a version and a 32-byte nonce, then a tag: a longer frame
/// is not a handshake.
pub(crate) const MAX_HANDSHAKE_FRAME: usize = 64;

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
    let mut buf = [0u8; MAX_HANDSHAKE_FRAME];
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
        CHALLENGE_LEN, MAX_CONTROL_FRAME_LEN, TAG_CHALLENGE, TAG_LEN, TAG_REQUEST,
        decode_challenge_response, encode_transport_response,
    };
    use melin_wire_protocol::framing::split_request;

    use super::*;

    // The fake node and its frame builders are `pub(crate)` so the polled
    // connection's tests (`polled.rs`) run against the same node.

    /// How the fake node behaves once a client is authenticated.
    #[derive(Clone, Copy)]
    pub(crate) enum Behaviour {
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
        /// Judge each request by its body: [`SHED`] is answered
        /// `ServerBusy`, [`REFUSE`] is refused with [`REASON`], anything
        /// else is echoed as by `Echo`. The connection stays open
        /// throughout, as a node keeps it.
        Judging,
        /// End every request's batch under an ID one past the request's:
        /// an answer for a request the client never sent.
        WrongId,
        /// Take requests in pairs and answer the second first, with the
        /// answer [`early_answer`] gives its body, then the first as by
        /// `Echo`: a node answering a request it did not accept ahead of
        /// one the ack policy holds (determinism-audit finding 18).
        Overtaking,
        /// Close the connection once authenticated.
        Hangup,
        /// The admin listener's shape: one text line in, `OK` out.
        AdminLines,
    }

    /// The body [`Behaviour::Judging`] sheds.
    pub(crate) const SHED: &[u8] = b"shed me";
    /// The body [`Behaviour::Judging`] refuses.
    pub(crate) const REFUSE: &[u8] = b"refuse me";
    /// The reason [`Behaviour::Judging`] refuses with.
    pub(crate) const REASON: &str = "auditor role may not notarize";
    /// The body [`early_answer`] answers as a filtered request: an empty
    /// batch.
    pub(crate) const FILTER: &[u8] = b"filter me";
    /// The body [`early_answer`] answers as a halt rejection: the
    /// application's report, [`REJECTION`], then the batch end.
    pub(crate) const REJECT: &[u8] = b"reject me";
    /// The report a halt rejection carries.
    pub(crate) const REJECTION: &[u8] = b"rejected: no replica";

    /// What a node sends for request `id` it does not accept, by its
    /// body: [`SHED`] is answered `ServerBusy`, [`REFUSE`] is refused with
    /// [`REASON`], [`FILTER`] gets an empty batch and [`REJECT`] the
    /// rejection's report and batch end. `None` for any other body.
    pub(crate) fn early_answer(id: RequestId, body: &[u8]) -> Option<Vec<u8>> {
        Some(match body {
            SHED => control(TransportResponse::ServerBusy { id }),
            REFUSE => control(TransportResponse::Refused {
                id,
                kind: RefusalKind::PermissionDenied,
                reason: REASON,
            }),
            FILTER => control(TransportResponse::BatchEnd { id }),
            REJECT => [
                app_frame(REJECTION),
                control(TransportResponse::BatchEnd { id }),
            ]
            .concat(),
            _ => return None,
        })
    }

    pub(crate) fn control(response: TransportResponse) -> Vec<u8> {
        let mut buf = [0u8; MAX_CONTROL_FRAME_LEN];
        let n = encode_transport_response(&response, &mut buf).unwrap();
        buf[..n].to_vec()
    }

    /// The end of request `id`'s batch, backed by the policy.
    pub(crate) fn batch_end(id: u64) -> Vec<u8> {
        control(TransportResponse::BatchEnd { id: RequestId(id) })
    }

    /// Request `id`'s frame carrying `body`, length prefix included, as a
    /// client sends it.
    pub(crate) fn request_frame(id: u64, body: &[u8]) -> Vec<u8> {
        tagged_frame(TAG_REQUEST, &[&id.to_le_bytes()[..], body].concat())
    }

    /// A frame under any tag, length prefix included.
    pub(crate) fn tagged_frame(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = ((TAG_LEN + body.len()) as u32).to_le_bytes().to_vec();
        frame.push(tag);
        frame.extend_from_slice(body);
        frame
    }

    /// An application frame carrying `body`, length prefix included.
    pub(crate) fn app_frame(body: &[u8]) -> Vec<u8> {
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
    pub(crate) fn serve(mut stream: TcpStream, allowed: VerifyingKey, behaviour: Behaviour) {
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
            // `[tag][id][body]`
            let (id, body) = split_request(request).expect("a request frame");
            let body = body.to_vec();
            let reply: Vec<u8> = match behaviour {
                Behaviour::Echo | Behaviour::EagerHeartbeat => [
                    app_frame(&body),
                    control(TransportResponse::BatchEnd { id }),
                ]
                .concat(),
                Behaviour::DegradedEcho => [
                    app_frame(&body),
                    control(TransportResponse::BatchEndDegraded { id }),
                ]
                .concat(),
                Behaviour::ChattyEcho => [
                    control(TransportResponse::Heartbeat),
                    app_frame(&body),
                    app_frame(b"again"),
                    control(TransportResponse::BatchEnd { id }),
                ]
                .concat(),
                Behaviour::Judging => early_answer(id, &body).unwrap_or_else(|| {
                    [
                        app_frame(&body),
                        control(TransportResponse::BatchEnd { id }),
                    ]
                    .concat()
                }),
                Behaviour::Overtaking => {
                    let Ok(Some(second)) = reader.read_frame() else {
                        return;
                    };
                    let (second_id, second_body) = split_request(second).expect("a request frame");
                    [
                        early_answer(second_id, second_body).expect("an early answer's body"),
                        app_frame(&body),
                        control(TransportResponse::BatchEnd { id }),
                    ]
                    .concat()
                }
                Behaviour::WrongId => [
                    app_frame(&body),
                    control(TransportResponse::BatchEnd {
                        id: RequestId(id.0 + 1),
                    }),
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
                Behaviour::Hangup | Behaviour::AdminLines => unreachable!(),
            };
            stream.write_all(&reply).unwrap();
        }
    }

    /// A fake node on a kernel-assigned port, serving clients on a
    /// thread until the listener is dropped.
    pub(crate) fn fake_node(allowed: VerifyingKey, behaviour: Behaviour) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                std::thread::spawn(move || serve(stream, allowed, behaviour));
            }
        });
        addr
    }

    pub(crate) fn client_key() -> SigningKey {
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

        // The same over the pipelined pair, several requests in flight,
        // each batch naming the request it ends: IDs follow on from the
        // first request's.
        for n in 2..=4u64 {
            assert_eq!(node.send(&n.to_le_bytes()).unwrap(), RequestId(n));
        }
        for n in 2..=4u64 {
            assert_eq!(
                node.next_frame().unwrap(),
                Frame::Response(&n.to_le_bytes())
            );
            assert_eq!(
                node.next_frame().unwrap(),
                Frame::BatchEnd {
                    id: RequestId(n),
                    ack: Ack::Policy
                }
            );
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

        let id = node.send(b"held").unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::Response(b"held"));
        assert_eq!(
            node.next_frame().unwrap(),
            Frame::BatchEnd {
                id,
                ack: Ack::PrimaryOnly
            }
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
        assert!(err.to_string().contains("stopped answering"), "{err}");
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

        // One byte over the widest body a frame takes once the tag and the
        // ID are counted.
        let body = vec![0xAB; MAX_REQUEST_BODY + 1];
        let err = node.send(&body).unwrap_err();
        assert!(matches!(err, Error::RequestTooLarge { len } if len == MAX_FRAME_SIZE + 1));
        assert!(err.to_string().contains("request too large"), "{err}");

        // Nothing reached the node, so the connection is as good as new,
        // no ID was taken, and the widest body that fits goes through it.
        assert_eq!(node.send(b"still here").unwrap(), RequestId(1));
        assert_eq!(node.next_frame().unwrap(), Frame::Response(b"still here"));
        assert!(matches!(node.next_frame().unwrap(), Frame::BatchEnd { .. }));
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

    /// A shed and a refused request are frames naming them when
    /// pipelining, and errors from the convenience calls, which leave the
    /// connection usable for the next request.
    #[test]
    fn busy_and_refused_are_frames_when_pipelining_and_errors_from_request() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Judging);
        let mut node = Connection::connect(addr, &key).unwrap();

        let shed = node.send(SHED).unwrap();
        let refused = node.send(REFUSE).unwrap();
        assert_eq!(node.next_frame().unwrap(), Frame::ServerBusy { id: shed });
        assert_eq!(
            node.next_frame().unwrap(),
            Frame::Refused {
                id: refused,
                kind: RefusalKind::PermissionDenied,
                reason: REASON
            }
        );
        assert_judged_by_the_convenience_calls(&mut node);
    }

    /// `session`'s convenience calls against a [`Behaviour::Judging`]
    /// node: a busy and a refusal are their errors, and the next request
    /// on the same connection still gets its reply.
    pub(crate) fn assert_judged_by_the_convenience_calls(session: &mut dyn polled::Session) {
        let err = session.request(SHED).unwrap_err();
        assert!(matches!(err, Error::ServerBusy), "{err:?}");
        assert!(err.to_string().contains("resend"), "{err}");
        assert_eq!(
            session.request_one(b"after a shed").unwrap(),
            b"after a shed"
        );

        let err = session.request_batch(REFUSE).unwrap_err();
        assert!(
            matches!(
                &err,
                Error::Refused { kind: RefusalKind::PermissionDenied, reason } if reason == REASON
            ),
            "{err:?}"
        );
        assert!(err.to_string().contains(REASON), "{err}");
        assert!(err.to_string().contains("not permitted"), "{err}");
        assert_eq!(
            session.request_one(b"after a refusal").unwrap(),
            b"after a refusal"
        );
    }

    /// Finding 18 at the client, against a [`Behaviour::Overtaking`] node:
    /// each kind of answer a node gives a request it did not accept — a
    /// busy, a refusal, a filtered request's empty batch, a halt
    /// rejection's report and batch end — arriving for W2 ahead of the
    /// reply to W1, sent before it, is paired with W2 and leaves W1 owed;
    /// W1's own batch then settles W1 (were W1 already settled, its batch
    /// end would be an answer for a request not outstanding). `owed` is
    /// how many requests `session` counts outstanding.
    pub(crate) fn assert_an_early_answer_is_paired_with_its_own_request<S: polled::Session>(
        session: &mut S,
        owed: impl Fn(&S) -> usize,
    ) {
        for body in [SHED, REFUSE, FILTER, REJECT] {
            let w1 = session.send(b"held write").unwrap();
            let w2 = session.send(body).unwrap();
            let early: Vec<String> = match body {
                REJECT => vec![
                    format!("{:?}", session.next_frame().unwrap()),
                    format!("{:?}", session.next_frame().unwrap()),
                ],
                _ => vec![format!("{:?}", session.next_frame().unwrap())],
            };
            let expected: Vec<String> = match body {
                SHED => vec![format!("{:?}", Frame::ServerBusy { id: w2 })],
                REFUSE => vec![format!(
                    "{:?}",
                    Frame::Refused {
                        id: w2,
                        kind: RefusalKind::PermissionDenied,
                        reason: REASON
                    }
                )],
                FILTER => vec![format!(
                    "{:?}",
                    Frame::BatchEnd {
                        id: w2,
                        ack: Ack::Policy
                    }
                )],
                _ => vec![
                    format!("{:?}", Frame::Response(REJECTION)),
                    format!(
                        "{:?}",
                        Frame::BatchEnd {
                            id: w2,
                            ack: Ack::Policy
                        }
                    ),
                ],
            };
            assert_eq!(early, expected, "{}", String::from_utf8_lossy(body));
            assert_eq!(owed(session), 1, "W1 still owed");
            assert_eq!(
                session.next_frame().unwrap(),
                Frame::Response(b"held write")
            );
            assert_eq!(
                session.next_frame().unwrap(),
                Frame::BatchEnd {
                    id: w1,
                    ack: Ack::Policy
                },
                "{}: W1's own batch settles W1",
                String::from_utf8_lossy(body)
            );
            assert_eq!(owed(session), 0, "every request settled once");
        }
    }

    #[test]
    fn an_early_answer_is_paired_with_its_own_request() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Overtaking);
        let mut node = Connection::connect(addr, &key).unwrap();
        assert_an_early_answer_is_paired_with_its_own_request(&mut node, |node| {
            node.outstanding.len()
        });
    }

    /// An answer for another request is a protocol error from the
    /// convenience calls, not a reply taken for the one they sent; and it
    /// is one from the pipelined pair too, since nothing outstanding has
    /// that ID.
    #[test]
    fn an_answer_for_another_request_is_refused() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::WrongId);
        let mut node = Connection::connect(addr, &key).unwrap();
        let err = node.request(b"mine").unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "{err:?}");
        assert!(err.to_string().contains("not outstanding"), "{err}");
    }

    /// The pairing check in the convenience calls, apart from the
    /// connection's: a request in flight before the call, answered first,
    /// is outstanding (so the connection accepts the answer) but not the
    /// call's.
    #[test]
    fn a_convenience_call_does_not_take_another_requests_answer() {
        let key = client_key();
        let addr = fake_node(key.verifying_key(), Behaviour::Echo);
        let mut node = Connection::connect(addr, &key).unwrap();
        let earlier = node.send(b"earlier").unwrap();
        let err = node.request(b"mine").unwrap_err();
        assert!(
            err.to_string().contains(&format!(
                "an answer for request {} while waiting on request {}",
                earlier.0,
                earlier.0 + 1
            )),
            "{err}"
        );
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
        // gets its reply batch, under the ID it chose.
        stream.write_all(&request_frame(77, b"raw")).unwrap();
        assert_eq!(
            read_raw_frame(&mut stream),
            [&[TAG_APP][..], b"raw"].concat()
        );
        assert_eq!(read_raw_frame(&mut stream), batch_end(77)[PREFIX_LEN..]);
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

        stream.write_all(&request_frame(1, b"after")).unwrap();
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
    pub(crate) fn payload(response: TransportResponse) -> Vec<u8> {
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

    /// A challenge from a node on another client protocol, by hand: a
    /// protocol-0 one carries no version, a later one names its own.
    fn challenge_of(version: Option<u16>) -> Vec<u8> {
        let version = version.map(u16::to_le_bytes);
        let mut payload = vec![TAG_CHALLENGE];
        payload.extend(version.iter().flatten());
        payload.extend([0x5A; 32]);
        payload
    }

    #[test]
    fn a_node_on_another_protocol_is_refused_before_anything_is_signed() {
        let key = client_key();
        for (version, node) in [
            (None, 0),
            (Some(0), 0),
            (Some(2), 2),
            (Some(u16::MAX), u16::MAX),
        ] {
            let mut handshake = Handshake::new(&key);
            let err = handshake.feed(&challenge_of(version)).unwrap_err();
            assert!(
                matches!(
                    err,
                    Error::ProtocolVersion { node: n, client: CLIENT_PROTOCOL_VERSION } if n == node
                ),
                "{version:?}: {err:?}"
            );
            let shown = err.to_string();
            assert!(shown.contains(&format!("protocol {node}")), "{shown}");
            assert!(
                shown.contains(&format!("this client {CLIENT_PROTOCOL_VERSION}")),
                "{shown}"
            );
            // Nothing was signed: the handshake still waits for a
            // challenge, and takes a good one.
            assert!(format!("{handshake:?}").contains("challenge"));
            assert!(matches!(
                handshake.feed(&payload(TransportResponse::Challenge { nonce: [1; 32] })),
                Ok(Step::Send(_))
            ));
        }
        // This build's own protocol: answered.
        let ours = challenge_of(Some(CLIENT_PROTOCOL_VERSION));
        assert_eq!(ours.len(), CHALLENGE_LEN);
        assert!(matches!(
            Handshake::new(&key).feed(&ours),
            Ok(Step::Send(_))
        ));
    }

    /// Over a socket: a node on protocol 0 sees the client close without a
    /// byte sent, and the client reports both versions.
    #[test]
    fn a_protocol_0_node_gets_nothing_from_the_client() {
        let key = client_key();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let node = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let challenge = challenge_of(None);
            stream
                .write_all(&tagged_frame(challenge[0], &challenge[1..]))
                .unwrap();
            let mut got = Vec::new();
            stream.read_to_end(&mut got).unwrap();
            got
        });
        let err = Connection::connect(addr, &key).unwrap_err();
        assert!(
            matches!(
                err,
                Error::ProtocolVersion {
                    node: 0,
                    client: CLIENT_PROTOCOL_VERSION
                }
            ),
            "{err:?}"
        );
        assert!(node.join().unwrap().is_empty(), "the client sent something");
    }

    /// A protocol mismatch is a definite answer, as a refused key is:
    /// `connect_by` reports it at once rather than retrying to the deadline.
    #[test]
    fn connect_by_does_not_retry_a_node_on_another_protocol() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            // Answer every attempt the same way: a client that retried
            // would keep failing until the deadline.
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let challenge = challenge_of(None);
                let _ = stream.write_all(&tagged_frame(challenge[0], &challenge[1..]));
            }
        });
        let started = Instant::now();
        let err = Connection::connect_by(
            addr,
            &client_key(),
            Instant::now() + Duration::from_secs(10),
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::ProtocolVersion {
                    node: 0,
                    client: CLIENT_PROTOCOL_VERSION
                }
            ),
            "{err:?}"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[test]
    fn classify_tells_each_reply_apart() {
        let id = RequestId(0x1122_3344_5566_7788);
        assert_eq!(
            classify(&payload(TransportResponse::Heartbeat)).unwrap(),
            Reply::Heartbeat
        );
        assert_eq!(
            classify(&payload(TransportResponse::BatchEnd { id })).unwrap(),
            Reply::BatchEnd {
                id,
                ack: Ack::Policy
            }
        );
        assert_eq!(
            classify(&payload(TransportResponse::BatchEndDegraded { id })).unwrap(),
            Reply::BatchEnd {
                id,
                ack: Ack::PrimaryOnly
            }
        );
        assert_eq!(
            classify(&payload(TransportResponse::ServerBusy { id })).unwrap(),
            Reply::ServerBusy { id }
        );
        for kind in [RefusalKind::PermissionDenied, RefusalKind::Malformed] {
            assert_eq!(
                classify(&payload(TransportResponse::Refused {
                    id,
                    kind,
                    reason: "why ✓"
                }))
                .unwrap(),
                Reply::Refused {
                    id,
                    kind,
                    reason: "why ✓"
                }
            );
        }
        // An empty reason.
        assert!(matches!(
            classify(&payload(TransportResponse::Refused {
                id,
                kind: RefusalKind::Malformed,
                reason: ""
            })),
            Ok(Reply::Refused { reason: "", .. })
        ));

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
            // A request, which a node never sends, and the retired
            // engine-error tag.
            request_frame(1, b"x")[PREFIX_LEN..].to_vec(),
            vec![0x03],
        ] {
            let err = classify(&wrong).unwrap_err();
            assert!(matches!(err, Error::Protocol(_)), "{wrong:?}: {err}");
            assert!(
                err.to_string().contains("unexpected tag"),
                "{wrong:?}: {err}"
            );
        }
    }

    /// A final frame is checked to its exact length: one byte short of
    /// its ID or one byte long is a protocol error, as is a refusal whose
    /// kind names none, whose reason is not UTF-8, or whose reason's
    /// length disagrees with the frame.
    #[test]
    fn classify_checks_each_final_frame_exactly() {
        let id = RequestId(5);
        for terminal in [
            TransportResponse::BatchEnd { id },
            TransportResponse::BatchEndDegraded { id },
            TransportResponse::ServerBusy { id },
        ] {
            let exact = payload(terminal);
            assert!(classify(&exact).is_ok(), "{terminal:?}");
            let short = &exact[..exact.len() - 1];
            let long = [&exact[..], &[0]].concat();
            for wrong in [short, &long[..], &exact[..1]] {
                let err = classify(wrong).unwrap_err();
                assert!(matches!(err, Error::Protocol(_)), "{terminal:?}: {err:?}");
                assert!(err.to_string().contains("-byte"), "{err}");
            }
        }

        let refused = payload(TransportResponse::Refused {
            id,
            kind: RefusalKind::Malformed,
            reason: "abc",
        });
        assert!(classify(&refused).is_ok());
        let kind_at = TAG_LEN + 8;
        let mut wrong = Vec::new();
        // An unknown kind and a zeroed one.
        for kind in [0x00, 0x03] {
            let mut bad = refused.clone();
            bad[kind_at] = kind;
            wrong.push(bad);
        }
        // A reason that is not UTF-8.
        let mut bad = refused.clone();
        bad[kind_at + 2] = 0xFF;
        wrong.push(bad);
        // A reason's length past the end of the frame, and short of it.
        let mut bad = refused.clone();
        bad[kind_at + 1] = 4;
        wrong.push(bad);
        wrong.push([&refused[..], b"x"].concat());
        // Cut inside the ID, before the kind, before the length.
        for cut in [5, kind_at, kind_at + 1] {
            wrong.push(refused[..cut].to_vec());
        }
        for bad in wrong {
            let err = classify(&bad).unwrap_err();
            assert!(matches!(err, Error::Protocol(_)), "{bad:?}: {err:?}");
            assert!(err.to_string().contains("refusal"), "{err}");
        }
    }

    /// Every final frame's ID is read through the wire protocol's one
    /// decoder of it.
    #[test]
    fn classify_reads_ids_through_the_wire_protocol() {
        for id in [0, 1, u64::MAX] {
            for terminal in [
                TransportResponse::BatchEnd { id: RequestId(id) },
                TransportResponse::BatchEndDegraded { id: RequestId(id) },
                TransportResponse::ServerBusy { id: RequestId(id) },
                TransportResponse::Refused {
                    id: RequestId(id),
                    kind: RefusalKind::PermissionDenied,
                    reason: "r",
                },
            ] {
                let payload = payload(terminal);
                let classified = match classify(&payload).unwrap() {
                    Reply::BatchEnd { id, .. }
                    | Reply::ServerBusy { id }
                    | Reply::Refused { id, .. } => id,
                    other => panic!("{other:?}"),
                };
                assert_eq!(Some(classified), terminal_request_id(&payload));
                assert_eq!(classified, RequestId(id));
            }
        }
    }

    // --- the I/O-free path ---

    /// `frame_request` with an encoder that copies `body`, under the
    /// crate's own error type, as request 1.
    fn frame_body(buf: &mut [u8], body: &[u8]) -> Result<usize, Error> {
        frame_body_as(buf, 1, body)
    }

    /// [`frame_body`] as request `id`.
    fn frame_body_as(buf: &mut [u8], id: u64, body: &[u8]) -> Result<usize, Error> {
        frame_request(buf, RequestId(id), |out| {
            let Some(dst) = out.get_mut(..body.len()) else {
                return Err(Error::Protocol("encoder: no room".into()));
            };
            dst.copy_from_slice(body);
            Ok(body.len())
        })
    }

    #[test]
    fn next_reply_surfaces_heartbeats_and_waits_for_whole_frames() {
        let refused = TransportResponse::Refused {
            id: RequestId(4),
            kind: RefusalKind::Malformed,
            reason: "bad",
        };
        let stream = [
            control(TransportResponse::Heartbeat),
            app_frame(b"body"),
            batch_end(1),
            control(TransportResponse::BatchEndDegraded { id: RequestId(2) }),
            control(TransportResponse::ServerBusy { id: RequestId(3) }),
            control(refused),
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
                format!(
                    "{:?}",
                    Reply::BatchEnd {
                        id: RequestId(1),
                        ack: Ack::Policy
                    }
                ),
                format!(
                    "{:?}",
                    Reply::BatchEnd {
                        id: RequestId(2),
                        ack: Ack::PrimaryOnly
                    }
                ),
                format!("{:?}", Reply::ServerBusy { id: RequestId(3) }),
                format!(
                    "{:?}",
                    Reply::Refused {
                        id: RequestId(4),
                        kind: RefusalKind::Malformed,
                        reason: "bad"
                    }
                ),
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
        assert_eq!(REQUEST_HEADER_LEN, PREFIX_LEN + TAG_LEN + 8);
        assert_eq!(MAX_REQUEST_BODY, framing::MAX_REQUEST_BODY);
        assert_eq!(MAX_REQUEST_BODY, MAX_FRAME_SIZE - TAG_LEN - 8);
        // Compiles only if the root's names are the module's items.
        let _: fn(&mut [u8], RequestId, usize) -> Result<usize, framing::RequestFrameError> =
            seal_request;
        let _: fn(&mut [u8]) -> Result<&mut [u8], RequestFrameError> = framing::request_body;
        let _: RequestId = melin_wire_protocol::control::RequestId(1);

        // The closure-free form through the root names.
        let mut buf = [0xEEu8; 32];
        let body = request_body(&mut buf).unwrap();
        body[..3].copy_from_slice(b"abc");
        let n = seal_request(&mut buf, RequestId(9), 3).unwrap();
        assert_eq!(&buf[..n], request_frame(9, b"abc").as_slice());
        assert!(buf[n..].iter().all(|&b| b == 0xEE));
    }

    /// The NIC's own message, the EAL's file prefix among it, reaches the
    /// operator through `Display`, and the error itself through `source`.
    #[test]
    fn a_nic_that_cannot_come_up_says_why() {
        use std::error::Error as _;

        let err = Error::Nic(io::Error::other(
            "DPDK EAL init failed (file prefix melin-client; is another DPDK process on this \
             host using it?): code -1",
        ));
        let msg = err.to_string();
        assert!(msg.starts_with("cannot bring the DPDK NIC up: "), "{msg}");
        assert!(msg.contains("file prefix melin-client"), "{msg}");
        let source = err.source().expect("the NIC's error is the source");
        assert!(source.to_string().contains("code -1"), "{source}");
    }

    #[test]
    fn request_framing_errors_map_into_the_clients_error() {
        // With this crate's `Error` as the encoder's error type, each
        // framing failure arrives as its own variant.
        let mut wide = vec![0x5C; REQUEST_HEADER_LEN + MAX_FRAME_SIZE + 16];
        let err = frame_request(&mut wide, RequestId(1), |_| {
            Ok::<_, Error>(MAX_REQUEST_BODY + 1)
        })
        .unwrap_err();
        assert!(
            matches!(err, Error::RequestTooLarge { len } if len == MAX_FRAME_SIZE + 1),
            "{err:?}"
        );
        assert!(err.to_string().contains("request too large"), "{err}");

        let mut short = [0u8; 2];
        let err = frame_request(&mut short, RequestId(1), |_| -> Result<usize, Error> {
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
            seal_request(&mut buf, RequestId(1), 5).map_err(Error::from),
            Err(Error::BufferTooSmall { needed, available })
                if needed == REQUEST_HEADER_LEN + 5 && available == REQUEST_HEADER_LEN + 4
        ));
        // An encoder's own failure passes through untouched.
        assert!(matches!(
            frame_request(&mut buf, RequestId(1), |_| Err(Error::Protocol("mine".into()))),
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
        let mut expected = Vec::new();
        for (id, body) in (1..).zip(bodies) {
            end += frame_body_as(&mut buf[end..], id, body).unwrap();
            expected.extend(request_frame(id, body));
        }
        assert!(buf[..start].iter().all(|&b| b == 0xEE));
        assert!(buf[end..].iter().all(|&b| b == 0xEE));
        assert_eq!(&buf[start..end], expected.as_slice());

        // A node on the other side gets each request back, in order.
        let mut decoder = FrameDecoder::new();
        decoder.push(&buf[start..end]);
        for (id, body) in (1..).zip(bodies) {
            assert_eq!(
                split_request(decoder.next().unwrap().unwrap()).unwrap(),
                (RequestId(id), body)
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
        let end = format!(
            "{:?}",
            Reply::BatchEnd {
                id: RequestId(1),
                ack: Ack::Policy
            }
        );
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
        /// Any run of requests that fit, under any IDs, framed back to back
        /// from any offset, decodes to the same IDs and bodies however the
        /// bytes arrive.
        #[test]
        fn framed_requests_round_trip_through_the_decoder(
            requests in proptest::collection::vec(
                (
                    proptest::prelude::any::<u64>(),
                    proptest::collection::vec(proptest::prelude::any::<u8>(), 0..=64),
                ),
                0..8,
            ),
            offset in 0usize..16,
            chunk in 1usize..32,
        ) {
            let mut buf = vec![0u8; offset + requests.len() * (REQUEST_HEADER_LEN + 64)];
            let mut end = offset;
            for (id, body) in &requests {
                end += frame_body_as(&mut buf[end..], *id, body).unwrap();
            }
            let mut decoder = FrameDecoder::new();
            let mut got = Vec::new();
            for piece in buf[offset..end].chunks(chunk) {
                decoder.push(piece);
                while let Some(payload) = decoder.next().unwrap() {
                    let (id, body) = split_request(payload).unwrap();
                    got.push((id.0, body.to_vec()));
                }
            }
            proptest::prop_assert_eq!(got, requests);
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
