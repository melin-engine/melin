#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! Minimal application built on the Melin core runtime.
//!
//! Demonstrates the five traits needed to plug a custom state machine into
//! Melin's durable, replicated pipeline:
//!
//!   1. [`AppEvent`]        — the event type (journal codec)
//!   2. [`Application`]     — the state machine
//!   3. [`AppFactory`]      — construction + seeding
//!   4. [`RequestDecoder`]  — wire bytes → event
//!   5. [`ResponseEncoder`] — report → wire bytes
//!
//! The application is a simple counter: clients send `Increment(amount)`
//! commands and receive the new total. A `GetValue` query returns the
//! current count without journaling.

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

pub const TAG_INCREMENT: u8 = 0x10;
pub const TAG_GET_VALUE: u8 = 0x11;

pub const TAG_RESP_ACK: u8 = 0x30;
pub const TAG_RESP_VALUE: u8 = 0x31;
pub const TAG_RESP_REJECTED: u8 = 0x32;

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
    fn encoded_size(&self) -> usize {
        match self {
            // tag(1) + amount(8)
            CounterEvent::Increment { .. } => 9,
            // tag(1)
            CounterEvent::GetValue => 1,
        }
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        match *self {
            CounterEvent::Increment { amount } => {
                buf[0] = TAG_INCREMENT;
                buf[1..9].copy_from_slice(&amount.to_le_bytes());
                9
            }
            CounterEvent::GetValue => {
                buf[0] = TAG_GET_VALUE;
                1
            }
        }
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        if buf.is_empty() {
            return Err(CodecError::Truncated);
        }
        match buf[0] {
            TAG_INCREMENT => {
                if buf.len() < 9 {
                    return Err(CodecError::Truncated);
                }
                let amount = u64::from_le_bytes(buf[1..9].try_into().expect("8 bytes"));
                Ok(CounterEvent::Increment { amount })
            }
            TAG_GET_VALUE => Ok(CounterEvent::GetValue),
            tag => Err(CodecError::UnknownTag(tag)),
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

/// 1:1 query response returned directly from `apply`.
#[derive(Debug, Clone, Copy)]
pub struct CounterQuery {
    pub value: u64,
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/// The counter state machine: a single `u64` value.
pub struct Counter {
    value: u64,
}

impl Application for Counter {
    type Event = CounterEvent;
    type Report = CounterReport;
    type QueryResponse = CounterQuery;

    fn apply(
        &mut self,
        event: Self::Event,
        _ctx: &ApplyCtx,
        out: &mut Vec<Self::Report>,
    ) -> Option<Self::QueryResponse> {
        match event {
            CounterEvent::Increment { amount } => {
                // Wraps on overflow — a deliberate simplification for this example.
                // A production app would saturate, reject, or use a wider type.
                self.value = self.value.wrapping_add(amount);
                out.push(CounterReport::Ack {
                    new_value: self.value,
                });
                None
            }
            CounterEvent::GetValue => Some(CounterQuery { value: self.value }),
        }
    }

    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<Self::Report>) {}

    // Simplification: always accepts. A production app should track per-key
    // high-water marks and reject duplicates.
    fn check_request_seq(&mut self, _key_hash: u64, _seq: u64) -> bool {
        true
    }

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
// Factory
// ---------------------------------------------------------------------------

/// Constructs `Counter` instances for the runtime.
pub struct CounterFactory;

impl AppFactory for CounterFactory {
    type App = Counter;

    fn empty(&self) -> Counter {
        Counter { value: 0 }
    }

    fn prefault(&self, _app: &mut Counter) {}
}

// ---------------------------------------------------------------------------
// Request decoder
// ---------------------------------------------------------------------------

/// Decodes length-prefixed client frames into `CounterEvent`.
///
/// Wire format (after the 4-byte length prefix is stripped by the runtime):
///   `[request_seq: u64][tag: u8][payload...]`
pub struct RequestDecoder;

impl RequestDecoderTrait for RequestDecoder {
    type Event = CounterEvent;

    fn decode(&self, bytes: &[u8], _permission: Permission) -> Decoded<CounterEvent> {
        // seq(8) + tag(1) = minimum 9 bytes
        if bytes.len() < 9 {
            return Decoded::DecodeError("frame too short");
        }

        let request_seq = u64::from_le_bytes(bytes[..8].try_into().expect("8 bytes"));
        let tag = bytes[8];
        let payload = &bytes[9..];

        match tag {
            TAG_INCREMENT => {
                if payload.len() < 8 {
                    return Decoded::DecodeError("increment payload too short");
                }
                let amount = u64::from_le_bytes(payload[..8].try_into().expect("8 bytes"));
                Decoded::Permitted {
                    request_seq,
                    event: CounterEvent::Increment { amount },
                }
            }
            TAG_GET_VALUE => Decoded::Permitted {
                request_seq,
                event: CounterEvent::GetValue,
            },
            // Transport-level heartbeats and auth frames — filter silently.
            0x01..=0x0F => Decoded::Filter,
            _ => Decoded::DecodeError("unknown tag"),
        }
    }
}

// ---------------------------------------------------------------------------
// Response encoder
// ---------------------------------------------------------------------------

/// Encodes `CounterReport` / `CounterQuery` into length-prefixed wire frames.
///
/// Wire format: `[length: u32 LE][tag: u8][payload...]`
pub struct ResponseEncoder;

impl ResponseEncoderTrait for ResponseEncoder {
    type Report = CounterReport;
    type Query = CounterQuery;

    fn encode_report(&self, report: &CounterReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        match *report {
            CounterReport::Ack { new_value } => {
                // len(4) + tag(1) + value(8) = 13
                if buf.len() < 13 {
                    return Err("buffer too small");
                }
                let payload_len: u32 = 9;
                buf[..4].copy_from_slice(&payload_len.to_le_bytes());
                buf[4] = TAG_RESP_ACK;
                buf[5..13].copy_from_slice(&new_value.to_le_bytes());
                Ok(13)
            }
            CounterReport::Rejected => {
                // len(4) + tag(1) = 5
                if buf.len() < 5 {
                    return Err("buffer too small");
                }
                let payload_len: u32 = 1;
                buf[..4].copy_from_slice(&payload_len.to_le_bytes());
                buf[4] = TAG_RESP_REJECTED;
                Ok(5)
            }
        }
    }

    fn encode_query(&self, query: &CounterQuery, buf: &mut [u8]) -> Result<usize, &'static str> {
        // len(4) + tag(1) + value(8) = 13
        if buf.len() < 13 {
            return Err("buffer too small");
        }
        let payload_len: u32 = 9;
        buf[..4].copy_from_slice(&payload_len.to_le_bytes());
        buf[4] = TAG_RESP_VALUE;
        buf[5..13].copy_from_slice(&query.value.to_le_bytes());
        Ok(13)
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
            journal_sequence: melin_app::WireSeq::new(0),
            active_connections: 0,
            events_processed: 0,
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
    fn apply_get_value() {
        let mut counter = Counter { value: 99 };
        let ctx = ApplyCtx {
            now_ns: 0,
            journal_sequence: melin_app::WireSeq::new(0),
            active_connections: 0,
            events_processed: 0,
            key_hash: 0,
        };
        let mut reports = Vec::new();

        let query = counter.apply(CounterEvent::GetValue, &ctx, &mut reports);
        assert!(reports.is_empty());
        assert_eq!(query.unwrap().value, 99);
    }

    #[test]
    fn build_reject() {
        let event = CounterEvent::Increment { amount: 1 };
        let report = Counter::build_reject(&event, RejectReason::DuplicateRequest);
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

    #[test]
    fn decoder_increment() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&7u64.to_le_bytes());
        frame.push(TAG_INCREMENT);
        frame.extend_from_slice(&100u64.to_le_bytes());

        match RequestDecoder.decode(&frame, Permission::Operator) {
            Decoded::Permitted { request_seq, event } => {
                assert_eq!(request_seq, 7);
                assert!(matches!(event, CounterEvent::Increment { amount: 100 }));
            }
            _ => panic!("expected Permitted"),
        }
    }

    #[test]
    fn decoder_get_value() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&1u64.to_le_bytes());
        frame.push(TAG_GET_VALUE);

        match RequestDecoder.decode(&frame, Permission::Operator) {
            Decoded::Permitted { event, .. } => {
                assert!(matches!(event, CounterEvent::GetValue));
                assert!(event.is_query());
            }
            _ => panic!("expected Permitted"),
        }
    }

    #[test]
    fn decoder_filters_transport_tags() {
        let mut frame = Vec::new();
        frame.extend_from_slice(&0u64.to_le_bytes());
        frame.push(0x01); // TAG_RESPONSE_HEARTBEAT

        assert!(matches!(
            RequestDecoder.decode(&frame, Permission::Operator),
            Decoded::Filter
        ));
    }

    #[test]
    fn encoder_report_ack() {
        let mut buf = [0u8; 64];
        let n = ResponseEncoder
            .encode_report(&CounterReport::Ack { new_value: 42 }, &mut buf)
            .unwrap();
        assert_eq!(n, 13);
        assert_eq!(u32::from_le_bytes(buf[..4].try_into().unwrap()), 9);
        assert_eq!(buf[4], TAG_RESP_ACK);
        assert_eq!(u64::from_le_bytes(buf[5..13].try_into().unwrap()), 42);
    }

    #[test]
    fn encoder_report_rejected() {
        let mut buf = [0u8; 64];
        let n = ResponseEncoder
            .encode_report(&CounterReport::Rejected, &mut buf)
            .unwrap();
        assert_eq!(n, 5);
        assert_eq!(u32::from_le_bytes(buf[..4].try_into().unwrap()), 1);
        assert_eq!(buf[4], TAG_RESP_REJECTED);
    }

    #[test]
    fn encoder_query() {
        let mut buf = [0u8; 64];
        let n = ResponseEncoder
            .encode_query(&CounterQuery { value: 99 }, &mut buf)
            .unwrap();
        assert_eq!(n, 13);
        assert_eq!(buf[4], TAG_RESP_VALUE);
        assert_eq!(u64::from_le_bytes(buf[5..13].try_into().unwrap()), 99);
    }
}
