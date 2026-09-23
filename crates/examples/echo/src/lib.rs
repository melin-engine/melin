#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! An echo server built on the Melin core runtime: the smallest
//! application that can exist, and the floor every other one is measured
//! against.
//!
//! A client sends up to [`MAX_PAYLOAD`] bytes and gets those bytes back.
//! In between, the sequencer has given the request a position in the
//! total order, journaled it, replicated it if a replica is attached, and
//! waited for the copies the ack policy demands — everything the runtime
//! does for a real application, with the application's own cost removed.
//! The state machine has no state: [`Application::apply`] copies the
//! payload into the reply and does nothing else.
//!
//! ## Why this makes a good example
//!
//! Two reasons, one for readers and one for operators.
//!
//! For a reader, it is the reference plug-in with nothing in the way. This
//! one file holds the five traits an application implements — event codec,
//! state machine, factory, request decoder, response encoder — and none of
//! them is obscured by business logic. `melin-example-counter` is the step
//! up (it has a value to keep and snapshot), `melin-example-notary` the
//! one after (its state is a commitment the guarantees can be checked
//! against).
//!
//! For an operator, it is the sequencer floor. The latency a client
//! measures against this server is the cost of the runtime alone —
//! transport, ordering, journal, replication — on a given host, disk and
//! network. The same measurement against a real application is that
//! application's cost on top, and the two numbers are what an evaluator
//! wants separately. Every request is a state-mutating event as far as
//! the runtime is concerned: sequenced, journaled, replicated, and
//! acknowledged only once the copies the ack policy demands exist. This is
//! what a request costs before the application does any work, and the
//! floor is the runtime as shipped — each cost it adds has a switch (see
//! `main.rs`), not a corner cut in the application.
//!
//! ## Sizing
//!
//! [`MAX_PAYLOAD`] is the one knob, and it is set to the wide end of what
//! the pipeline is sized for rather than the narrow one, so that the floor
//! is measured with a full-sized message and the cost of width is on
//! display. The event is variable-length — the first among the examples —
//! so it shows both halves of [`AppEvent`]'s contract:
//! [`AppEvent::MAX_ENCODED_SIZE`] is a *bound* the journal reserves per
//! entry and sizes its fsync batch by, while [`AppEvent::encoded_size`] is
//! *exact* per event, so a short payload costs the journal only its own
//! bytes. Ring slots always pay for the bound — `AppEvent` is `Copy` and a
//! slot holds the widest event inline — so the cap is what the rings'
//! footprint follows.
//!
//! At this width an entry no longer fits the journal's full fsync batch:
//! the transport shortens the batch to keep it inside one hand-off chunk,
//! and the rings are several times what a digest-sized event would make
//! them. `tests/footprint.rs` and `tests/journal_limit.rs` print both
//! figures and assert them as the price of the width — an application
//! that can commit to a narrower event (the notary's 32-byte digest is
//! the model) gets the full batch and the small rings back. The wire has
//! two bounds of its own — the widest request frame the reader accepts
//! and the widest reply the response stage can encode — and the cap is
//! checked against both at compile time, next to its definition.
//!
//! ## Where to look
//!
//! - `lib.rs` (this file): the application — the payload, which is the
//!   event; the state machine; and the request/response codecs the
//!   runtime plugs into.
//! - `main.rs`: the server binary, the recipe for running it, and the
//!   runtime switches that take the floor apart.
//! - `client.rs`: the client — a closed loop of requests and the
//!   round-trip latency distribution.
//! - `tests/round_trip.rs`: the behaviour, end to end — over raw frames,
//!   against the journal on disk, and through the client as a process.
//! - `tests/footprint.rs`, `tests/journal_limit.rs`: what the payload
//!   width costs the rings and the journal, printed and pinned.

use std::fmt;
use std::io::{self, Read, Write};

use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder as RequestDecoderTrait};
use melin_app::encoder::ResponseEncoder as ResponseEncoderTrait;
use melin_app::{AppEvent, Application, ApplyCtx, CodecError, NoQuery, RejectReason};

// ---------------------------------------------------------------------------
// Response kinds — the first byte of every response body. A request needs
// none: there is one kind of request, and its body is the payload whole.
// ---------------------------------------------------------------------------

pub const KIND_RESP_ECHO: u8 = 0x30;
pub const KIND_RESP_REJECTED: u8 = 0x31;

/// Bytes a response's kind takes ahead of the echoed payload.
const KIND_LEN: usize = 1;

/// Most bytes one request may carry, and therefore one reply.
///
/// The one sizing decision in this crate: see the module docs. A
/// full-sized message rather than a digest, so the floor is measured at
/// the width a real application's widest event has, and the cost of that
/// width is what the sizing tests report.
pub const MAX_PAYLOAD: usize = 288;

// The wire has bounds of its own, and both fail at runtime rather than
// at build time: a request frame past the reader's limit costs the
// client its connection, and a reply the response stage cannot encode is
// dropped with an `error!`. Checked here so that raising the cap past
// either is a compile error naming the reason. The journal's bound is
// checked the same way by the journal itself, from `MAX_ENCODED_SIZE`.
const _: () = assert!(
    MAX_PAYLOAD <= melin_server_runtime::MAX_REQUEST_BODY,
    "a request's payload must fit one client frame's body"
);
const _: () = assert!(
    KIND_LEN + MAX_PAYLOAD <= melin_server_runtime::MAX_RESPONSE_BODY,
    "a reply's kind and payload must fit the response stage's body buffer"
);

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

/// The bytes of one request, as they travel through the pipeline: a
/// length and a fixed buffer, so the type is `Copy` and holds no pointer.
/// This *is* the event — there is nothing else to say about a request.
///
/// `len` is a `u16` because [`MAX_PAYLOAD`] does not fit a `u8`, and two
/// bytes are nothing next to the buffer; the buffer is the cap, not the
/// actual length, for the same reason a ring slot is — the type has one
/// size.
#[derive(Clone, Copy)]
pub struct Payload {
    len: u16,
    bytes: [u8; MAX_PAYLOAD],
}

const _: () = assert!(
    MAX_PAYLOAD <= u16::MAX as usize,
    "Payload stores its length in a u16"
);

impl Payload {
    /// `None` if `bytes` is longer than [`MAX_PAYLOAD`].
    pub fn new(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_PAYLOAD {
            return None;
        }
        let mut payload = Payload {
            // Lossless: bounded by `MAX_PAYLOAD`, which fits a `u16`.
            len: bytes.len() as u16,
            bytes: [0; MAX_PAYLOAD],
        };
        payload.bytes[..bytes.len()].copy_from_slice(bytes);
        Some(payload)
    }

    /// The bytes the request carried — only those, not the buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

// Equality and Debug look at the carried bytes only: what sits in the
// buffer past `len` is padding, and two payloads that differ there alone
// are the same payload.
impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for Payload {}

impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Payload({:02x?})", self.as_bytes())
    }
}

/// Encoded form: `[len: u16 LE][bytes: len]`. No kind: this application
/// has one kind of event, and the journal frames each entry itself.
impl AppEvent for Payload {
    // len(2) + the widest payload. A bound, checked against the journal's
    // entry ceiling at compile time; `encoded_size` below is the exact
    // figure per event.
    const MAX_ENCODED_SIZE: usize = 2 + MAX_PAYLOAD;

    fn encoded_size(&self) -> usize {
        2 + self.as_bytes().len()
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        let bytes = self.as_bytes();
        buf[..2].copy_from_slice(&self.len.to_le_bytes());
        buf[2..2 + bytes.len()].copy_from_slice(bytes);
        2 + bytes.len()
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let [lo, hi, bytes @ ..] = buf else {
            return Err(CodecError::Truncated);
        };
        let len = usize::from(u16::from_le_bytes([*lo, *hi]));
        if len > MAX_PAYLOAD {
            return Err(CodecError::InvalidField);
        }
        // The journal hands over exactly one event, so the length has to
        // account for every byte that follows it: fewer is a truncated
        // entry, more is one whose length field is wrong.
        if bytes.len() < len {
            return Err(CodecError::Truncated);
        }
        if bytes.len() > len {
            return Err(CodecError::InvalidField);
        }
        Payload::new(bytes).ok_or(CodecError::InvalidField)
    }

    // Every request is journaled; this application has no queries.
    fn is_query(&self) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

/// Fan-out report emitted by `apply`. One per event.
///
/// The variants differ by a whole payload in size, which clippy flags;
/// its remedy — boxing — is what the transport rules out: a report is
/// `Copy` and lives inline in an output ring slot, so the payload *is*
/// the slot's width whichever variant it holds. See `tests/footprint.rs`.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EchoReport {
    /// The request's bytes, back.
    Echoed(Payload),
    /// The runtime refused the event before `apply` saw it (see
    /// [`Application::build_reject`]). Nothing was echoed.
    Rejected,
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/// The state machine. There is no state: the reply is a function of the
/// request alone, which is what makes this the floor.
#[derive(Default)]
pub struct Echo;

impl Application for Echo {
    type Event = Payload;
    type Report = EchoReport;
    // No queries, so no query response: `NoQuery` has no values, so
    // there is nothing `query` could return and nothing to encode.
    type QueryResponse = NoQuery;
    // No state, so nothing to size.
    type Sizing = ();

    fn apply(&mut self, event: Payload, _ctx: &ApplyCtx, out: &mut Vec<EchoReport>) {
        out.push(EchoReport::Echoed(event));
    }

    // No `query` and no `tick`: no payload is a query and nothing here is
    // time-driven, so the defaults (no answer, no work) are the whole
    // story.

    fn build_reject(_event: &Self::Event, _reason: RejectReason) -> Self::Report {
        EchoReport::Rejected
    }

    // A snapshot of no state is zero bytes, and restores from zero bytes.
    // The runtime still wraps it in its own framing (magic, version,
    // CRC), so an empty payload is a valid snapshot, not a missing one.
    fn snapshot<W: Write>(&self, _w: &mut W) -> io::Result<()> {
        Ok(())
    }

    fn restore<R: Read>(_r: &mut R) -> io::Result<Self> {
        Ok(Echo)
    }

    const APP_VERSION: u16 = 1;
}

// ---------------------------------------------------------------------------
// Request decoder
// ---------------------------------------------------------------------------

/// Decodes client requests into `Payload`.
///
/// The body of a request is the payload, whole: there is one kind of
/// request, so no byte of it says which. It needs no length of its own
/// either: the frame is already length-prefixed, so the body the runtime
/// hands over is exactly the payload.
pub struct RequestDecoder;

impl RequestDecoderTrait for RequestDecoder {
    type Event = Payload;

    fn decode(&self, body: &[u8], permission: Permission) -> Decoded<Payload> {
        // An echo appends to the journal, so the read-only role is refused,
        // as it would be for any state-mutating event. (A replication key
        // never gets this far: the client listener refuses it.)
        if permission == Permission::ReadOnly {
            return Decoded::PermissionDenied("echoing requires a writing role");
        }
        match Payload::new(body) {
            Some(event) => Decoded::Permitted(event),
            None => Decoded::DecodeError("payload longer than MAX_PAYLOAD"),
        }
    }
}

// ---------------------------------------------------------------------------
// Response encoder
// ---------------------------------------------------------------------------

/// Encodes `EchoReport` into response bodies; the runtime frames them. A
/// response says which it is, since an empty echo and a rejection would
/// otherwise look alike:
///   - echo: `[KIND_RESP_ECHO][the request's bytes]`
///   - rejected: `[KIND_RESP_REJECTED]`
pub struct ResponseEncoder;

impl ResponseEncoderTrait for ResponseEncoder {
    type Report = EchoReport;
    type Query = NoQuery;

    fn encode_report(&self, report: &EchoReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        match report {
            EchoReport::Echoed(payload) => {
                let bytes = payload.as_bytes();
                let body = buf
                    .get_mut(..KIND_LEN + bytes.len())
                    .ok_or("buffer too small")?;
                body[0] = KIND_RESP_ECHO;
                body[KIND_LEN..].copy_from_slice(bytes);
                Ok(body.len())
            }
            EchoReport::Rejected => {
                *buf.first_mut().ok_or("buffer too small")? = KIND_RESP_REJECTED;
                Ok(1)
            }
        }
    }

    // A `NoQuery` cannot exist, so neither can a call to this: the empty
    // match is the compiler's proof, not a runtime error to hope for.
    fn encode_query(&self, query: &NoQuery, _buf: &mut [u8]) -> Result<usize, &'static str> {
        match *query {}
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(bytes: &[u8]) -> Payload {
        Payload::new(bytes).expect("within the cap")
    }

    fn ctx() -> ApplyCtx {
        ApplyCtx {
            now_ns: 0,
            key_hash: 0,
        }
    }

    // --- Payload ---

    #[test]
    fn a_payload_carries_its_bytes_and_only_those() {
        assert_eq!(payload(b"").as_bytes(), b"");
        assert_eq!(payload(b"hello").as_bytes(), b"hello");
        let full = [0xAB; MAX_PAYLOAD];
        assert_eq!(payload(&full).as_bytes(), &full);
        assert!(Payload::new(&[0; MAX_PAYLOAD + 1]).is_none());
    }

    #[test]
    fn payload_equality_ignores_the_buffer_past_the_length() {
        let mut a = payload(b"abc");
        let b = payload(b"abc");
        a.bytes[10] = 0xFF;
        assert_eq!(a, b);
        assert_ne!(payload(b"abc"), payload(b"abd"));
        assert_ne!(payload(b"abc"), payload(b"ab"));
    }

    // --- Event codec ---

    #[test]
    fn events_round_trip_at_every_size() {
        for len in [0, 1, 7, 255, 256, MAX_PAYLOAD - 1, MAX_PAYLOAD] {
            let bytes: Vec<u8> = (0..len).map(|i| i as u8).collect();
            let event = payload(&bytes);
            let mut buf = [0u8; Payload::MAX_ENCODED_SIZE];
            let n = event.encode(&mut buf);
            assert_eq!(n, event.encoded_size(), "{len} bytes");
            assert_eq!(n, 2 + len);
            assert_eq!(Payload::decode(&buf[..n]).unwrap(), event);
        }
    }

    #[test]
    fn the_widest_event_is_the_declared_bound() {
        let event = payload(&[0; MAX_PAYLOAD]);
        assert_eq!(event.encoded_size(), Payload::MAX_ENCODED_SIZE);
    }

    #[test]
    fn decode_refuses_malformed_entries() {
        let mut buf = [0u8; Payload::MAX_ENCODED_SIZE];
        let n = payload(b"four").encode(&mut buf);

        assert_eq!(Payload::decode(&[]), Err(CodecError::Truncated));
        assert_eq!(Payload::decode(&[4]), Err(CodecError::Truncated));
        assert_eq!(
            Payload::decode(&buf[..n - 1]),
            Err(CodecError::Truncated),
            "fewer bytes than the length claims"
        );
        assert_eq!(
            Payload::decode(&buf[..n + 1]),
            Err(CodecError::InvalidField),
            "more bytes than the length claims"
        );

        let mut oversized = [0u8; 2 + MAX_PAYLOAD + 1];
        oversized[..2].copy_from_slice(&((MAX_PAYLOAD + 1) as u16).to_le_bytes());
        assert_eq!(
            Payload::decode(&oversized),
            Err(CodecError::InvalidField),
            "a length past the cap"
        );
    }

    #[test]
    fn nothing_is_a_query() {
        assert!(!payload(b"x").is_query());
        assert!(!payload(b"").is_query());
    }

    // --- Application ---

    #[test]
    fn an_echo_is_reported() {
        let mut app = Echo;
        let mut reports = Vec::new();
        app.apply(payload(b"durable"), &ctx(), &mut reports);
        assert_eq!(reports, [EchoReport::Echoed(payload(b"durable"))]);
    }

    #[test]
    fn build_reject() {
        assert_eq!(
            Echo::build_reject(&payload(b"x"), RejectReason::ReplicaDisconnected),
            EchoReport::Rejected
        );
    }

    #[test]
    fn a_snapshot_is_empty_and_restores() {
        let mut buf = Vec::new();
        Echo.snapshot(&mut buf).unwrap();
        assert!(buf.is_empty());
        Echo::restore(&mut &buf[..]).unwrap();
    }

    // --- Request decoder ---

    #[test]
    fn writing_roles_may_echo() {
        for permission in [
            Permission::Operator,
            Permission::Trader,
            Permission::Custodian,
        ] {
            match RequestDecoder.decode(b"hi", permission) {
                Decoded::Permitted(event) => assert_eq!(event, payload(b"hi")),
                _ => panic!("expected Permitted for {permission:?}"),
            }
        }
    }

    #[test]
    fn read_only_role_may_not_echo() {
        assert!(matches!(
            RequestDecoder.decode(b"hi", Permission::ReadOnly),
            Decoded::PermissionDenied(_)
        ));
    }

    #[test]
    fn the_payload_is_the_whole_body() {
        for len in [0, 3, MAX_PAYLOAD] {
            let bytes = vec![0x5A; len];
            match RequestDecoder.decode(&bytes, Permission::Trader) {
                Decoded::Permitted(event) => assert_eq!(event.as_bytes(), bytes),
                _ => panic!("expected Permitted for {len} bytes"),
            }
        }
    }

    #[test]
    fn decoder_refuses_what_it_cannot_carry() {
        let too_long = vec![0; MAX_PAYLOAD + 1];
        assert!(matches!(
            RequestDecoder.decode(&too_long, Permission::Trader),
            Decoded::DecodeError(_)
        ));
    }

    // --- Response encoder ---

    #[test]
    fn an_echo_body_is_its_kind_then_the_bytes() {
        let mut buf = [0u8; KIND_LEN + MAX_PAYLOAD];

        let len = ResponseEncoder
            .encode_report(&EchoReport::Echoed(payload(b"back")), &mut buf)
            .unwrap();
        assert_eq!(buf[..len], [&[KIND_RESP_ECHO][..], b"back"].concat());

        let len = ResponseEncoder
            .encode_report(&EchoReport::Echoed(payload(b"")), &mut buf)
            .unwrap();
        assert_eq!(buf[..len], [KIND_RESP_ECHO]);

        let len = ResponseEncoder
            .encode_report(&EchoReport::Rejected, &mut buf)
            .unwrap();
        assert_eq!(buf[..len], [KIND_RESP_REJECTED]);
        // No query case to test: `encode_query` takes a `NoQuery`, which
        // cannot be built.
    }

    #[test]
    fn encoder_refuses_a_buffer_too_small_for_the_body() {
        let mut buf = [0u8; 4];
        assert_eq!(
            ResponseEncoder.encode_report(&EchoReport::Echoed(payload(b"four")), &mut buf),
            Err("buffer too small")
        );
        assert!(
            ResponseEncoder
                .encode_report(&EchoReport::Echoed(payload(b"thr")), &mut buf)
                .is_ok()
        );
        assert_eq!(
            ResponseEncoder.encode_report(&EchoReport::Rejected, &mut []),
            Err("buffer too small")
        );
    }
}
