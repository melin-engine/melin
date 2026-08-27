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
//! what an order costs before any matching happens, and the floor is the
//! runtime as shipped — each cost it adds has a switch (see `main.rs`),
//! not a corner cut in the application.
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

use melin_app::app_factory::AppFactory;
use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder as RequestDecoderTrait};
use melin_app::encoder::ResponseEncoder as ResponseEncoderTrait;
use melin_app::{AppEvent, Application, ApplyCtx, CodecError, RejectReason};

// ---------------------------------------------------------------------------
// Wire tags — domain tags start at 0x10 to avoid colliding with transport-
// level control tags (0x01–0x0F) reserved by melin-wire-protocol.
// ---------------------------------------------------------------------------

pub const TAG_ECHO: u8 = 0x10;

pub const TAG_RESP_ECHO: u8 = 0x30;
pub const TAG_RESP_REJECTED: u8 = 0x31;

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
    8 + 1 + MAX_PAYLOAD <= melin_server_runtime::MAX_FRAME_SIZE,
    "a request (sequence, tag, payload) must fit one client frame"
);
const _: () = assert!(
    4 + 1 + MAX_PAYLOAD <= melin_server_runtime::MAX_RESPONSE_BUF,
    "a reply (length prefix, tag, payload) must fit the response stage's encode buffer"
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

/// Encoded form: `[len: u16 LE][bytes: len]`. No tag: this application
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
pub struct Echo;

impl Application for Echo {
    type Event = Payload;
    type Report = EchoReport;
    // No queries, so no query response. `()` is `Copy`, which is all the
    // transport asks of the type; `apply` never returns `Some`.
    type QueryResponse = ();

    fn apply(&mut self, event: Payload, _ctx: &ApplyCtx, out: &mut Vec<EchoReport>) -> Option<()> {
        out.push(EchoReport::Echoed(event));
        None
    }

    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<Self::Report>) {}

    // Every request is accepted: an echo is idempotent by nature, so a
    // repeated request sequence is not a fault worth refusing. See the
    // trait docs for what an application with state would track here.
    fn check_request_seq(&mut self, _key_hash: u64, _seq: u64) -> bool {
        true
    }

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
// Factory
// ---------------------------------------------------------------------------

/// Constructs `Echo` instances for the runtime.
pub struct EchoFactory;

impl AppFactory for EchoFactory {
    type App = Echo;

    fn empty(&self) -> Echo {
        Echo
    }

    fn prefault(&self, _app: &mut Echo) {}
}

// ---------------------------------------------------------------------------
// Request decoder
// ---------------------------------------------------------------------------

/// Decodes length-prefixed client frames into `Payload`.
///
/// Wire format (after the 4-byte length prefix is stripped by the runtime):
///   `[request_seq: u64][tag: u8][bytes: the rest of the frame]`
///
/// The payload needs no length of its own: the frame is already
/// length-prefixed, so whatever follows the tag is the payload.
pub struct RequestDecoder;

impl RequestDecoderTrait for RequestDecoder {
    type Event = Payload;

    fn decode(&self, bytes: &[u8], permission: Permission) -> Decoded<Payload> {
        // seq(8) + tag(1) = minimum 9 bytes
        if bytes.len() < 9 {
            return Decoded::DecodeError("frame too short");
        }

        let request_seq = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
        let tag = bytes[8];
        let body = &bytes[9..];

        match tag {
            TAG_ECHO => {
                // An echo appends to the journal, so the read-only and
                // replication roles are refused, as they would be for any
                // state-mutating event.
                if matches!(permission, Permission::ReadOnly | Permission::Replication) {
                    return Decoded::PermissionDenied("echoing requires a writing role");
                }
                match Payload::new(body) {
                    Some(event) => Decoded::Permitted { request_seq, event },
                    None => Decoded::DecodeError("payload longer than MAX_PAYLOAD"),
                }
            }
            // Transport-level heartbeats and auth frames — filter silently.
            0x01..=0x0F => Decoded::Filter,
            _ => Decoded::DecodeError("unknown tag"),
        }
    }
}

// ---------------------------------------------------------------------------
// Response encoder
// ---------------------------------------------------------------------------

/// Encodes `EchoReport` into length-prefixed wire frames.
///
/// Wire format: `[length: u32 LE][tag: u8][bytes...]`, where the bytes are
/// the request's for an echo and absent for a rejection.
pub struct ResponseEncoder;

/// Write one frame — length prefix, tag, bytes — checking `buf` can hold
/// the whole of it first so the fixed-offset writes cannot panic.
fn frame(buf: &mut [u8], tag: u8, bytes: &[u8]) -> Result<usize, &'static str> {
    let frame_len = 4 + 1 + bytes.len();
    if buf.len() < frame_len {
        return Err("buffer too small");
    }
    // Lossless: a frame is at most a tag plus `MAX_PAYLOAD` bytes.
    let payload_len = (1 + bytes.len()) as u32;
    buf[..4].copy_from_slice(&payload_len.to_le_bytes());
    buf[4] = tag;
    buf[5..frame_len].copy_from_slice(bytes);
    Ok(frame_len)
}

impl ResponseEncoderTrait for ResponseEncoder {
    type Report = EchoReport;
    type Query = ();

    fn encode_report(&self, report: &EchoReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        match report {
            EchoReport::Echoed(payload) => frame(buf, TAG_RESP_ECHO, payload.as_bytes()),
            EchoReport::Rejected => frame(buf, TAG_RESP_REJECTED, &[]),
        }
    }

    // Unreachable: no event is a query, so the runtime never has a query
    // response to encode. An error rather than a panic, so that if that
    // ever changes the failure is a logged encode error, not a crash.
    fn encode_query(&self, _query: &(), _buf: &mut [u8]) -> Result<usize, &'static str> {
        Err("this application has no queries")
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
            journal_sequence: melin_app::WireSeq::new(0),
            active_connections: 0,
            events_processed: 0,
            key_hash: 0,
        }
    }

    /// `[request_seq][tag][body]` as a client would send it.
    fn request(seq: u64, tag: u8, body: &[u8]) -> Vec<u8> {
        let mut frame = Vec::with_capacity(9 + body.len());
        frame.extend_from_slice(&seq.to_le_bytes());
        frame.push(tag);
        frame.extend_from_slice(body);
        frame
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
        let query = app.apply(payload(b"durable"), &ctx(), &mut reports);
        assert!(query.is_none());
        assert_eq!(reports, [EchoReport::Echoed(payload(b"durable"))]);
    }

    #[test]
    fn build_reject() {
        assert_eq!(
            Echo::build_reject(&payload(b"x"), RejectReason::DuplicateRequest),
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
            match RequestDecoder.decode(&request(7, TAG_ECHO, b"hi"), permission) {
                Decoded::Permitted { request_seq, event } => {
                    assert_eq!(request_seq, 7);
                    assert_eq!(event, payload(b"hi"));
                }
                _ => panic!("expected Permitted for {permission:?}"),
            }
        }
    }

    #[test]
    fn read_only_roles_may_not_echo() {
        for permission in [Permission::ReadOnly, Permission::Replication] {
            assert!(
                matches!(
                    RequestDecoder.decode(&request(1, TAG_ECHO, b"hi"), permission),
                    Decoded::PermissionDenied(_)
                ),
                "{permission:?} must not be able to echo"
            );
        }
    }

    #[test]
    fn the_payload_is_the_rest_of_the_frame() {
        for len in [0, 3, MAX_PAYLOAD] {
            let bytes = vec![0x5A; len];
            match RequestDecoder.decode(&request(1, TAG_ECHO, &bytes), Permission::Trader) {
                Decoded::Permitted { event, .. } => assert_eq!(event.as_bytes(), bytes),
                _ => panic!("expected Permitted for {len} bytes"),
            }
        }
    }

    #[test]
    fn decoder_refuses_what_it_cannot_carry() {
        let too_long = vec![0; MAX_PAYLOAD + 1];
        assert!(matches!(
            RequestDecoder.decode(&request(1, TAG_ECHO, &too_long), Permission::Trader),
            Decoded::DecodeError(_)
        ));
        assert!(matches!(
            RequestDecoder.decode(&request(1, TAG_ECHO, b"")[..8], Permission::Trader),
            Decoded::DecodeError("frame too short")
        ));
        assert!(matches!(
            RequestDecoder.decode(&request(1, 0x7F, b""), Permission::Trader),
            Decoded::DecodeError("unknown tag")
        ));
    }

    #[test]
    fn decoder_filters_transport_tags() {
        // TAG_RESPONSE_HEARTBEAT
        assert!(matches!(
            RequestDecoder.decode(&request(0, 0x01, b""), Permission::Trader),
            Decoded::Filter
        ));
    }

    // --- Response encoder ---

    #[test]
    fn encoder_frames_carry_the_bytes_after_the_tag() {
        let mut buf = [0u8; 4 + 1 + MAX_PAYLOAD];

        let n = ResponseEncoder
            .encode_report(&EchoReport::Echoed(payload(b"back")), &mut buf)
            .unwrap();
        assert_eq!(n, 4 + 1 + 4);
        assert_eq!(u32::from_le_bytes(buf[..4].try_into().unwrap()), 5);
        assert_eq!(buf[4], TAG_RESP_ECHO);
        assert_eq!(&buf[5..n], b"back");

        let n = ResponseEncoder
            .encode_report(&EchoReport::Rejected, &mut buf)
            .unwrap();
        assert_eq!(n, 5);
        assert_eq!(u32::from_le_bytes(buf[..4].try_into().unwrap()), 1);
        assert_eq!(buf[4], TAG_RESP_REJECTED);

        assert!(ResponseEncoder.encode_query(&(), &mut buf).is_err());
    }

    #[test]
    fn encoder_refuses_a_buffer_too_small_for_the_frame() {
        let mut buf = [0u8; 8];
        assert_eq!(
            ResponseEncoder.encode_report(&EchoReport::Echoed(payload(b"four")), &mut buf),
            Err("buffer too small")
        );
        assert!(
            ResponseEncoder
                .encode_report(&EchoReport::Echoed(payload(b"thr")), &mut buf)
                .is_ok()
        );
    }
}
