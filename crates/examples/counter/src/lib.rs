#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Minimal application built on the Melin core runtime.
//!
//! Demonstrates the four traits needed to plug a custom state machine into
//! Melin's durable, replicated pipeline:
//!
//!   1. [`AppEvent`]        — the event type (journal codec)
//!   2. [`Application`]     — the state machine, starting from `Default`
//!   3. [`RequestDecoder`]  — request body → event
//!   4. [`ResponseEncoder`] — report → response body
//!
//! The application is a simple counter: clients send `Increment(amount)`
//! commands and receive the new total. A `GetValue` query returns the
//! current count without journaling.

use std::io::{self, Read, Write};

// The application guide walks through this crate, and its code blocks are
// compiled and run as this crate's doctests: a guide whose examples no
// longer build fails the suite instead of misleading its reader.
#[cfg(doctest)]
#[doc = include_str!("../../../../docs/building-an-application.md")]
pub struct ApplicationGuide;

use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder as RequestDecoderTrait};
use melin_app::encoder::ResponseEncoder as ResponseEncoderTrait;
use melin_app::{AppEvent, Application, ApplyCtx, CodecError, QueryCtx, RejectReason};

// ---------------------------------------------------------------------------
// Message kinds — the first byte of every request and response body. The
// body is the application's from its first byte, so any value will do:
// these are the counter's own, and the request kinds double as its
// journal encoding.
// ---------------------------------------------------------------------------

pub const KIND_INCREMENT: u8 = 0x10;
pub const KIND_GET_VALUE: u8 = 0x11;

pub const KIND_RESP_ACK: u8 = 0x30;
pub const KIND_RESP_VALUE: u8 = 0x31;
pub const KIND_RESP_REJECTED: u8 = 0x32;

/// The body of an `Increment` request, as a client sends it — the
/// inverse of [`RequestDecoder`] for this kind.
pub fn increment_request(amount: u64) -> [u8; 9] {
    let mut body = [0u8; 9];
    body[0] = KIND_INCREMENT;
    body[1..].copy_from_slice(&amount.to_le_bytes());
    body
}

/// The body of a `GetValue` request: the kind alone.
pub const GET_VALUE_REQUEST: [u8; 1] = [KIND_GET_VALUE];

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

/// State-mutating events journaled by the pipeline, plus a read-only query.
#[derive(Debug, Clone, Copy)]
pub enum CounterEvent {
    /// Add `amount` to the counter. Journaled.
    Increment { amount: u64 },
    /// Return the current value. Not journaled (query).
    GetValue,
}

impl AppEvent for CounterEvent {
    // The widest variant: `Increment`'s kind(1) + amount(8).
    const MAX_ENCODED_SIZE: usize = 9;

    fn encoded_size(&self) -> usize {
        match self {
            // kind(1) + amount(8)
            CounterEvent::Increment { .. } => 9,
            // kind(1)
            CounterEvent::GetValue => 1,
        }
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        match *self {
            CounterEvent::Increment { amount } => {
                buf[0] = KIND_INCREMENT;
                buf[1..9].copy_from_slice(&amount.to_le_bytes());
                9
            }
            CounterEvent::GetValue => {
                buf[0] = KIND_GET_VALUE;
                1
            }
        }
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        if buf.is_empty() {
            return Err(CodecError::Truncated);
        }
        match buf[0] {
            KIND_INCREMENT => {
                if buf.len() < 9 {
                    return Err(CodecError::Truncated);
                }
                let amount = u64::from_le_bytes(buf[1..9].try_into().expect("8 bytes"));
                Ok(CounterEvent::Increment { amount })
            }
            KIND_GET_VALUE => Ok(CounterEvent::GetValue),
            kind => Err(CodecError::UnknownTag(kind)),
        }
    }

    fn is_query(&self) -> bool {
        matches!(self, CounterEvent::GetValue)
    }
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

/// Fan-out report emitted by `apply`. One per state-mutating event.
#[derive(Debug, Clone, Copy)]
pub enum CounterReport {
    Ack { new_value: u64 },
    Rejected,
}

/// 1:1 query response returned by `query`.
#[derive(Debug, Clone, Copy)]
pub struct CounterQuery {
    pub value: u64,
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/// The counter state machine: a single `u64` value, zero at genesis.
#[derive(Default)]
pub struct Counter {
    value: u64,
}

impl Application for Counter {
    type Event = CounterEvent;
    type Report = CounterReport;
    type QueryResponse = CounterQuery;
    // A single integer has nothing to reserve for.
    type Sizing = ();

    fn apply(&mut self, event: Self::Event, _ctx: &ApplyCtx, out: &mut Vec<Self::Report>) {
        match event {
            CounterEvent::Increment { amount } => {
                // Wraps on overflow — a deliberate simplification for this example.
                // A production app would saturate, reject, or use a wider type.
                // Also a simplification: a client that retries an Increment
                // adds twice. An application whose requests are not
                // idempotent carries a per-client sequence in its events
                // and refuses a repeat here, keyed on `ctx.key_hash`.
                self.value = self.value.wrapping_add(amount);
                out.push(CounterReport::Ack {
                    new_value: self.value,
                });
            }
            // A query: answered by `query`, never applied.
            CounterEvent::GetValue => {}
        }
    }

    fn query(&self, event: Self::Event, _ctx: &QueryCtx) -> Option<Self::QueryResponse> {
        match event {
            CounterEvent::GetValue => Some(CounterQuery { value: self.value }),
            CounterEvent::Increment { .. } => None,
        }
    }

    // No `tick`: the counter has no time-driven work, and the default
    // does nothing.

    fn build_reject(_event: &Self::Event, _reason: RejectReason) -> Self::Report {
        CounterReport::Rejected
    }

    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.value.to_le_bytes())
    }

    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; 8];
        r.read_exact(&mut buf)?;
        Ok(Counter {
            value: u64::from_le_bytes(buf),
        })
    }

    const APP_VERSION: u16 = 1;
}

// ---------------------------------------------------------------------------
// Request decoder
// ---------------------------------------------------------------------------

/// Decodes client requests into `CounterEvent`. A request body is its
/// kind, then the kind's fields:
///   - increment: `[KIND_INCREMENT][amount: u64 LE]`
///   - get value: `[KIND_GET_VALUE]`
pub struct RequestDecoder;

impl RequestDecoderTrait for RequestDecoder {
    type Event = CounterEvent;

    fn decode(&self, body: &[u8], _permission: Permission) -> Decoded<CounterEvent> {
        let Some((&kind, fields)) = body.split_first() else {
            return Decoded::DecodeError("empty request");
        };
        match kind {
            KIND_INCREMENT => match fields.first_chunk::<8>() {
                Some(amount) => Decoded::Permitted(CounterEvent::Increment {
                    amount: u64::from_le_bytes(*amount),
                }),
                None => Decoded::DecodeError("increment too short"),
            },
            KIND_GET_VALUE => Decoded::Permitted(CounterEvent::GetValue),
            _ => Decoded::DecodeError("unknown kind"),
        }
    }
}

// ---------------------------------------------------------------------------
// Response encoder
// ---------------------------------------------------------------------------

/// Encodes `CounterReport` / `CounterQuery` into response bodies; the
/// runtime frames them. A response body is its kind, then the kind's
/// fields:
///   - ack, value: `[KIND_RESP_ACK | KIND_RESP_VALUE][value: u64 LE]`
///   - rejected: `[KIND_RESP_REJECTED]`
pub struct ResponseEncoder;

/// Write `value` as a body under `kind`.
fn value_body(buf: &mut [u8], kind: u8, value: u64) -> Result<usize, &'static str> {
    let body = buf.first_chunk_mut::<9>().ok_or("buffer too small")?;
    body[0] = kind;
    body[1..].copy_from_slice(&value.to_le_bytes());
    Ok(9)
}

impl ResponseEncoderTrait for ResponseEncoder {
    type Report = CounterReport;
    type Query = CounterQuery;

    fn encode_report(&self, report: &CounterReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        match *report {
            CounterReport::Ack { new_value } => value_body(buf, KIND_RESP_ACK, new_value),
            CounterReport::Rejected => {
                *buf.first_mut().ok_or("buffer too small")? = KIND_RESP_REJECTED;
                Ok(1)
            }
        }
    }

    fn encode_query(&self, query: &CounterQuery, buf: &mut [u8]) -> Result<usize, &'static str> {
        value_body(buf, KIND_RESP_VALUE, query.value)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_round_trip_increment() {
        let event = CounterEvent::Increment { amount: 42 };
        let mut buf = [0u8; 64];
        let n = event.encode(&mut buf);
        let decoded = CounterEvent::decode(&buf[..n]).unwrap();
        assert!(matches!(decoded, CounterEvent::Increment { amount: 42 }));
    }

    #[test]
    fn event_round_trip_get_value() {
        let event = CounterEvent::GetValue;
        let mut buf = [0u8; 64];
        let n = event.encode(&mut buf);
        let decoded = CounterEvent::decode(&buf[..n]).unwrap();
        assert!(matches!(decoded, CounterEvent::GetValue));
    }

    #[test]
    fn apply_increment() {
        let mut counter = Counter { value: 0 };
        let ctx = ApplyCtx {
            now_ns: 0,
            key_hash: 0,
        };
        let mut reports = Vec::new();

        counter.apply(CounterEvent::Increment { amount: 10 }, &ctx, &mut reports);
        assert_eq!(reports.len(), 1);
        assert!(matches!(reports[0], CounterReport::Ack { new_value: 10 }));

        reports.clear();
        counter.apply(CounterEvent::Increment { amount: 32 }, &ctx, &mut reports);
        assert!(matches!(reports[0], CounterReport::Ack { new_value: 42 }));
    }

    #[test]
    fn query_get_value() {
        let counter = Counter { value: 99 };
        let ctx = QueryCtx {
            journal_sequence: melin_app::WireSeq::new(0),
            active_connections: 0,
            events_processed: 0,
            key_hash: 0,
        };

        let query = counter.query(CounterEvent::GetValue, &ctx);
        assert_eq!(query.unwrap().value, 99);
        assert!(
            counter
                .query(CounterEvent::Increment { amount: 1 }, &ctx)
                .is_none(),
            "an increment is not a query"
        );
    }

    #[test]
    fn build_reject() {
        let event = CounterEvent::Increment { amount: 1 };
        let report = Counter::build_reject(&event, RejectReason::ReplicaDisconnected);
        assert!(matches!(report, CounterReport::Rejected));
    }

    #[test]
    fn snapshot_restore_round_trip() {
        let counter = Counter { value: 12345 };
        let mut buf = Vec::new();
        counter.snapshot(&mut buf).unwrap();

        let restored = Counter::restore(&mut &buf[..]).unwrap();
        assert_eq!(restored.value, 12345);
    }

    /// A request or response body: the kind, then its fields.
    fn message(kind: u8, fields: &[u8]) -> Vec<u8> {
        [&[kind][..], fields].concat()
    }

    #[test]
    fn decoder_increment() {
        let body = increment_request(100);
        assert_eq!(body[..], message(KIND_INCREMENT, &100u64.to_le_bytes()));
        match RequestDecoder.decode(&body, Permission::Operator) {
            Decoded::Permitted(event) => {
                assert!(matches!(event, CounterEvent::Increment { amount: 100 }));
            }
            _ => panic!("expected Permitted"),
        }
    }

    #[test]
    fn decoder_get_value() {
        match RequestDecoder.decode(&GET_VALUE_REQUEST, Permission::Operator) {
            Decoded::Permitted(event) => {
                assert!(matches!(event, CounterEvent::GetValue));
                assert!(event.is_query());
            }
            _ => panic!("expected Permitted"),
        }
    }

    #[test]
    fn decoder_refuses_empty_short_and_unknown() {
        assert!(matches!(
            RequestDecoder.decode(&[], Permission::Operator),
            Decoded::DecodeError("empty request")
        ));
        assert!(matches!(
            RequestDecoder.decode(&message(KIND_INCREMENT, &[0; 7]), Permission::Operator),
            Decoded::DecodeError("increment too short")
        ));
        assert!(matches!(
            RequestDecoder.decode(&[0x7F], Permission::Operator),
            Decoded::DecodeError("unknown kind")
        ));
    }

    #[test]
    fn encoder_report_ack() {
        let mut buf = [0u8; 64];
        let len = ResponseEncoder
            .encode_report(&CounterReport::Ack { new_value: 42 }, &mut buf)
            .unwrap();
        assert_eq!(buf[..len], message(KIND_RESP_ACK, &42u64.to_le_bytes()));
    }

    #[test]
    fn encoder_report_rejected() {
        let mut buf = [0u8; 64];
        let len = ResponseEncoder
            .encode_report(&CounterReport::Rejected, &mut buf)
            .unwrap();
        assert_eq!(buf[..len], [KIND_RESP_REJECTED]);
    }

    #[test]
    fn encoder_query() {
        let mut buf = [0u8; 64];
        let len = ResponseEncoder
            .encode_query(&CounterQuery { value: 99 }, &mut buf)
            .unwrap();
        assert_eq!(buf[..len], message(KIND_RESP_VALUE, &99u64.to_le_bytes()));
    }

    #[test]
    fn encoder_refuses_a_buffer_too_small() {
        assert_eq!(
            ResponseEncoder.encode_query(&CounterQuery { value: 1 }, &mut [0u8; 8]),
            Err("buffer too small")
        );
        assert_eq!(
            ResponseEncoder.encode_report(&CounterReport::Rejected, &mut []),
            Err("buffer too small")
        );
    }
}
