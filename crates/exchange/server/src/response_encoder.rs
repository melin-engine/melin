//! Trading-side [`ResponseEncoder`] implementation.
//!
//! Mirror of [`crate::request::ExchangeRequestDecoder`] on the
//! outbound path: maps trading-shaped output payloads
//! (`ExecutionReport`, `QueryResponse`) to wire frames. Transport-
//! shaped variants (`BatchEnd`, `EngineError`) are handled by the
//! runtime directly and never reach this encoder.

use melin_app::encoder::ResponseEncoder;
use melin_protocol::codec;
use melin_protocol::message::ResponseKind;
use melin_types::types::{ExecutionReport, QueryResponse};

/// Encoder for the trading wire protocol.
///
/// Zero-sized. The runtime owns an `Arc<dyn ResponseEncoder<...>>`;
/// constructing one is `Arc::new(ExchangeResponseEncoder)`.
#[derive(Debug, Clone, Copy)]
pub struct ExchangeResponseEncoder;

impl ResponseEncoder for ExchangeResponseEncoder {
    type Report = ExecutionReport;
    type Query = QueryResponse;

    fn encode_report(
        &self,
        report: &ExecutionReport,
        buf: &mut [u8],
    ) -> Result<usize, &'static str> {
        codec::encode_response(&ResponseKind::Report(*report), buf).map_err(|_| "encode error")
    }

    fn encode_query(&self, query: &QueryResponse, buf: &mut [u8]) -> Result<usize, &'static str> {
        let kind = match *query {
            QueryResponse::Stats {
                active_connections,
                events_processed,
                journal_sequence,
            } => ResponseKind::StatsHeader {
                active_connections,
                events_processed,
                journal_sequence,
            },
            QueryResponse::Position {
                account,
                balances,
                count,
            } => ResponseKind::PositionSnapshot {
                account,
                balances,
                count,
            },
            QueryResponse::RequestSeqHwm { hwm } => ResponseKind::RequestSeqHwm { hwm },
        };
        codec::encode_response(&kind, buf).map_err(|_| "encode error")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU64;

    use melin_types::types::*;

    const SCRATCH: usize = 512;

    /// Strip the 4-byte length prefix `encode_response` writes and
    /// hand the payload to `decode_response`. Keeps the round-trip
    /// asserts below symmetric.
    fn round_trip(written: &[u8]) -> ResponseKind {
        codec::decode_response(&written[4..]).expect("decode")
    }

    fn sample_placed() -> ExecutionReport {
        ExecutionReport::Placed {
            order_id: OrderId(1),
            symbol: Symbol(1),
            account: AccountId(1),
            side: Side::Buy,
            price: Price(NonZeroU64::new(100).unwrap()),
            quantity: Quantity(NonZeroU64::new(10).unwrap()),
        }
    }

    #[test]
    fn encodes_report() {
        let mut buf = [0u8; SCRATCH];
        let n = ExchangeResponseEncoder
            .encode_report(&sample_placed(), &mut buf)
            .unwrap();
        assert!(matches!(
            round_trip(&buf[..n]),
            ResponseKind::Report(ExecutionReport::Placed { order_id, .. })
                if order_id == OrderId(1)
        ));
    }

    #[test]
    fn encodes_query_stats() {
        let q = QueryResponse::Stats {
            active_connections: 7,
            events_processed: 12345,
            journal_sequence: 999,
        };
        let mut buf = [0u8; SCRATCH];
        let n = ExchangeResponseEncoder.encode_query(&q, &mut buf).unwrap();
        assert!(matches!(
            round_trip(&buf[..n]),
            ResponseKind::StatsHeader {
                active_connections: 7,
                events_processed: 12345,
                journal_sequence: 999,
            }
        ));
    }

    #[test]
    fn encodes_query_position() {
        let mut balances = [AccountBalance::ZERO; 16];
        balances[0] = AccountBalance {
            currency: CurrencyId(1),
            free: 100,
            reserved: 0,
        };
        let q = QueryResponse::Position {
            account: AccountId(42),
            balances,
            count: 1,
        };
        let mut buf = [0u8; SCRATCH];
        let n = ExchangeResponseEncoder.encode_query(&q, &mut buf).unwrap();
        assert!(matches!(
            round_trip(&buf[..n]),
            ResponseKind::PositionSnapshot { account, count: 1, .. }
                if account == AccountId(42)
        ));
    }

    #[test]
    fn encodes_query_request_seq_hwm() {
        let q = QueryResponse::RequestSeqHwm { hwm: 4242 };
        let mut buf = [0u8; SCRATCH];
        let n = ExchangeResponseEncoder.encode_query(&q, &mut buf).unwrap();
        assert!(matches!(
            round_trip(&buf[..n]),
            ResponseKind::RequestSeqHwm { hwm: 4242 }
        ));
    }

    // Note: the encoder's `Err` arm exists for codec-level failures
    // (e.g. an `InvalidField` propagated up); the codec does NOT
    // check buffer length and will panic with index-out-of-bounds
    // on an undersized scratch. The runtime always passes
    // `MAX_RESPONSE_BUF` (8 KiB), which is sized to fit any single
    // wire response, so this is a caller-guarantee contract — not
    // something the encoder defends against.
}
