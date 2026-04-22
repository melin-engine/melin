//! Client-side snapshot parser for the SubscribeWithSnapshot protocol.
//!
//! Reads the sequence of `BookSnapshotBegin/Level/End` + `SnapshotComplete`
//! frames from the event publisher's TCP stream and seeds per-symbol
//! `BookMirror` instances.

use std::collections::HashSet;
use std::io;
use std::num::NonZeroU64;

use melin_protocol::codec;
use melin_protocol::message::ResponseKind;
use melin_trading::types::{AccountId, OrderId, Price, Quantity, Side, Symbol};

use crate::index::RestingOrder;
use crate::mirror::{BookMirror, Level};

/// Result of parsing a complete snapshot stream.
pub struct SnapshotResult {
    /// Per-symbol mirrors seeded from the snapshot.
    pub mirrors: Vec<(Symbol, BookMirror)>,
    /// The ring sequence the snapshot was taken at. The firehose
    /// resumes from `last_applied_seq + 1`.
    pub last_applied_seq: u64,
}

/// Parse a complete snapshot from a blocking TCP stream.
///
/// Expects the caller to have already completed auth and sent Subscribe.
/// Reads frames until `SnapshotComplete` is received, then returns.
///
/// Each frame is prefixed with an 8-byte ring sequence (u64 LE) followed
/// by the standard 4-byte length-prefixed response.
pub fn parse_snapshot(reader: &mut dyn io::Read) -> Result<SnapshotResult, SnapshotError> {
    let mut mirrors: Vec<(Symbol, BookMirror)> = Vec::new();
    // Track symbols we've already finalized to reject duplicates.
    let mut seen_symbols: HashSet<Symbol> = HashSet::new();
    let mut current_symbol: Option<Symbol> = None;
    let mut current_mirror: Option<BookMirror> = None;
    let mut level_count: u32 = 0;

    loop {
        let (_seq, response) = read_frame(reader)?;

        match response {
            ResponseKind::BookSnapshotBegin {
                symbol,
                last_applied_seq: _,
            } => {
                // Finalize previous symbol if any.
                if let Some((mirror, sym)) = current_mirror.take().zip(current_symbol) {
                    if !seen_symbols.insert(sym) {
                        return Err(SnapshotError::DuplicateSymbol(sym));
                    }
                    mirrors.push((sym, mirror));
                }
                current_symbol = Some(symbol);
                current_mirror = Some(BookMirror::new(symbol));
                level_count = 0;
            }

            ResponseKind::BookSnapshotLevel {
                symbol: _,
                side,
                price,
                qty,
                order_count,
            } => {
                if let Some(mirror) = current_mirror.as_mut() {
                    // Inject the level directly into the mirror's BTreeMap.
                    // We use a synthetic Placed event per level since the
                    // mirror only exposes apply(). This is a cold-start path
                    // so the overhead is acceptable.
                    //
                    // We create a synthetic order per level rather than per
                    // individual order (we don't have L3 data in the snapshot).
                    // The order_count is recorded in the Level but the index
                    // only gets one synthetic entry per level.
                    let synthetic_order_id = OrderId(level_count as u64);
                    seed_level(mirror, side, price, qty, order_count, synthetic_order_id);

                    level_count += 1;
                }
            }

            ResponseKind::BookSnapshotEnd {
                symbol: _,
                level_count: _,
            } => {
                // End of this symbol's snapshot. Finalize in the next Begin
                // or at SnapshotComplete.
            }

            ResponseKind::SnapshotComplete { last_applied_seq } => {
                // Finalize last symbol.
                if let Some((mirror, sym)) = current_mirror.take().zip(current_symbol) {
                    if !seen_symbols.insert(sym) {
                        return Err(SnapshotError::DuplicateSymbol(sym));
                    }
                    mirrors.push((sym, mirror));
                }
                return Ok(SnapshotResult {
                    mirrors,
                    last_applied_seq,
                });
            }

            // Ignore any other frame types during snapshot parse
            // (e.g., BatchEnd, Heartbeat could arrive).
            _ => {}
        }
    }
}

/// Seed a mirror level directly from snapshot data.
///
/// We can't use `BookMirror::apply(Placed)` because that always sets
/// `order_count=1`. Snapshot levels carry the actual aggregate count.
/// Instead, use the mirror's public credit interface to set the level.
fn seed_level(
    mirror: &mut BookMirror,
    side: Side,
    price: Price,
    qty: u64,
    order_count: u32,
    synthetic_order_id: OrderId,
) {
    // Insert a synthetic index entry so the mirror can process subsequent
    // fills/cancels for orders at this level. We use a single synthetic
    // order representing the entire level (L2 snapshot doesn't have L3).
    if let Some(nz_qty) = NonZeroU64::new(qty) {
        mirror.seed_level(
            synthetic_order_id,
            RestingOrder {
                symbol: mirror.symbol(),
                side,
                price,
                remaining: Quantity(nz_qty),
                account: AccountId(0), // synthetic, not a real account
            },
            Level {
                total_qty: qty,
                order_count,
            },
        );
    }
}

/// Read a single sequence-prefixed response frame from the stream.
fn read_frame(reader: &mut dyn io::Read) -> Result<(u64, ResponseKind), SnapshotError> {
    // 8-byte sequence prefix.
    let mut seq_buf = [0u8; 8];
    reader.read_exact(&mut seq_buf)?;
    let seq = u64::from_le_bytes(seq_buf);

    // 4-byte length prefix.
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let frame_len = u32::from_le_bytes(len_buf) as usize;

    if frame_len > 4096 {
        return Err(SnapshotError::FrameTooLarge(frame_len));
    }

    let mut frame_buf = vec![0u8; frame_len];
    reader.read_exact(&mut frame_buf)?;

    let response = codec::decode_response(&frame_buf)?;
    Ok((seq, response))
}

/// Errors during snapshot parsing.
#[derive(Debug)]
pub enum SnapshotError {
    Io(io::Error),
    Protocol(melin_protocol::error::ProtocolError),
    FrameTooLarge(usize),
    DuplicateSymbol(Symbol),
}

impl From<io::Error> for SnapshotError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<melin_protocol::error::ProtocolError> for SnapshotError {
    fn from(e: melin_protocol::error::ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::FrameTooLarge(n) => write!(f, "frame too large: {n} bytes"),
            Self::DuplicateSymbol(s) => write!(f, "duplicate symbol in snapshot: {:?}", s),
        }
    }
}

impl std::error::Error for SnapshotError {}

#[cfg(test)]
mod tests {
    use super::*;
    use melin_protocol::codec::encode_response;
    use std::num::NonZeroU64;

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    /// Helper: encode a sequence-prefixed frame into a byte vec.
    fn encode_frame(seq: u64, kind: &ResponseKind) -> Vec<u8> {
        let mut buf = [0u8; 256];
        let response_len = encode_response(kind, &mut buf).unwrap();
        let mut frame = Vec::with_capacity(8 + response_len);
        frame.extend_from_slice(&seq.to_le_bytes());
        frame.extend_from_slice(&buf[..response_len]);
        frame
    }

    #[test]
    fn parse_empty_snapshot() {
        // One symbol with no levels.
        let mut data = Vec::new();
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotBegin {
                symbol: Symbol(1),
                last_applied_seq: 42,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotEnd {
                symbol: Symbol(1),
                level_count: 0,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::SnapshotComplete {
                last_applied_seq: 42,
            },
        ));

        let mut cursor = io::Cursor::new(data);
        let result = parse_snapshot(&mut cursor).unwrap();

        assert_eq!(result.mirrors.len(), 1);
        assert_eq!(result.mirrors[0].0, Symbol(1));
        assert!(result.mirrors[0].1.bids().is_empty());
        assert!(result.mirrors[0].1.asks().is_empty());
        assert_eq!(result.last_applied_seq, 42);
    }

    #[test]
    fn parse_snapshot_with_levels() {
        let mut data = Vec::new();
        data.extend(encode_frame(
            10,
            &ResponseKind::BookSnapshotBegin {
                symbol: Symbol(1),
                last_applied_seq: 100,
            },
        ));
        data.extend(encode_frame(
            10,
            &ResponseKind::BookSnapshotLevel {
                symbol: Symbol(1),
                side: Side::Buy,
                price: price(100),
                qty: 50,
                order_count: 3,
            },
        ));
        data.extend(encode_frame(
            10,
            &ResponseKind::BookSnapshotLevel {
                symbol: Symbol(1),
                side: Side::Sell,
                price: price(200),
                qty: 30,
                order_count: 2,
            },
        ));
        data.extend(encode_frame(
            10,
            &ResponseKind::BookSnapshotEnd {
                symbol: Symbol(1),
                level_count: 2,
            },
        ));
        data.extend(encode_frame(
            10,
            &ResponseKind::SnapshotComplete {
                last_applied_seq: 100,
            },
        ));

        let mut cursor = io::Cursor::new(data);
        let result = parse_snapshot(&mut cursor).unwrap();

        assert_eq!(result.mirrors.len(), 1);
        let mirror = &result.mirrors[0].1;
        assert_eq!(mirror.best_bid(), Some(price(100)));
        assert_eq!(mirror.best_ask(), Some(price(200)));
        assert_eq!(mirror.bids().get(&price(100)).unwrap().total_qty, 50);
        assert_eq!(mirror.bids().get(&price(100)).unwrap().order_count, 3);
        assert_eq!(mirror.asks().get(&price(200)).unwrap().total_qty, 30);
        assert_eq!(mirror.asks().get(&price(200)).unwrap().order_count, 2);
        assert_eq!(result.last_applied_seq, 100);
    }

    #[test]
    fn parse_snapshot_truncated_stream() {
        // Stream ends after BookSnapshotBegin — no End or Complete.
        let mut data = Vec::new();
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotBegin {
                symbol: Symbol(1),
                last_applied_seq: 42,
            },
        ));

        let mut cursor = io::Cursor::new(data);
        match parse_snapshot(&mut cursor) {
            Err(SnapshotError::Io(e)) => {
                assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
            }
            Err(other) => panic!("expected Io(UnexpectedEof), got: {other}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn parse_snapshot_ignores_non_snapshot_frames() {
        // Insert a Heartbeat between Begin and End — it should be silently
        // ignored by the `_ => {}` catch-all arm.
        let mut data = Vec::new();
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotBegin {
                symbol: Symbol(1),
                last_applied_seq: 42,
            },
        ));
        data.extend(encode_frame(0, &ResponseKind::Heartbeat));
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotEnd {
                symbol: Symbol(1),
                level_count: 0,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::SnapshotComplete {
                last_applied_seq: 42,
            },
        ));

        let mut cursor = io::Cursor::new(data);
        let result = parse_snapshot(&mut cursor).unwrap();

        assert_eq!(result.mirrors.len(), 1);
        assert_eq!(result.mirrors[0].0, Symbol(1));
        assert!(result.mirrors[0].1.bids().is_empty());
        assert!(result.mirrors[0].1.asks().is_empty());
        assert_eq!(result.last_applied_seq, 42);
    }

    #[test]
    fn parse_snapshot_zero_qty_level_skipped() {
        // A BookSnapshotLevel with qty=0 should be ignored by the
        // NonZeroU64 guard in seed_level.
        let mut data = Vec::new();
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotBegin {
                symbol: Symbol(1),
                last_applied_seq: 42,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotLevel {
                symbol: Symbol(1),
                side: Side::Buy,
                price: price(100),
                qty: 0,
                order_count: 1,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotEnd {
                symbol: Symbol(1),
                level_count: 0,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::SnapshotComplete {
                last_applied_seq: 42,
            },
        ));

        let mut cursor = io::Cursor::new(data);
        let result = parse_snapshot(&mut cursor).unwrap();

        assert_eq!(result.mirrors.len(), 1);
        // The zero-qty level must not appear in the mirror.
        assert!(result.mirrors[0].1.bids().is_empty());
        assert!(result.mirrors[0].1.asks().is_empty());
    }

    #[test]
    fn parse_multi_symbol_snapshot() {
        let mut data = Vec::new();
        // Symbol 1
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotBegin {
                symbol: Symbol(1),
                last_applied_seq: 50,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotLevel {
                symbol: Symbol(1),
                side: Side::Buy,
                price: price(100),
                qty: 10,
                order_count: 1,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotEnd {
                symbol: Symbol(1),
                level_count: 1,
            },
        ));
        // Symbol 2
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotBegin {
                symbol: Symbol(2),
                last_applied_seq: 50,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::BookSnapshotEnd {
                symbol: Symbol(2),
                level_count: 0,
            },
        ));
        data.extend(encode_frame(
            0,
            &ResponseKind::SnapshotComplete {
                last_applied_seq: 50,
            },
        ));

        let mut cursor = io::Cursor::new(data);
        let result = parse_snapshot(&mut cursor).unwrap();

        assert_eq!(result.mirrors.len(), 2);
        assert_eq!(result.mirrors[0].0, Symbol(1));
        assert_eq!(result.mirrors[1].0, Symbol(2));
        assert!(!result.mirrors[0].1.bids().is_empty());
        assert!(result.mirrors[1].1.bids().is_empty());
    }
}
