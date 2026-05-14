//! Fuzz tests for the wire protocol codec.
//!
//! Feeds arbitrary bytes into request/response decoders to find panics.
//! A malicious client or corrupted network data must never crash the
//! server or client — decoders must return Err gracefully.

use melin_protocol::codec;

/// Wire request decoder must never panic on arbitrary input.
#[test]
fn fuzz_wire_request_decode() {
    bolero::check!().for_each(|data: &[u8]| {
        let _ = codec::decode_request(data);
    });
}

/// Wire response decoder must never panic on arbitrary input.
#[test]
fn fuzz_wire_response_decode() {
    bolero::check!().for_each(|data: &[u8]| {
        let _ = codec::decode_response(data);
    });
}

/// Wire request encode → decode round-trip must be lossless.
#[test]
fn fuzz_wire_request_roundtrip() {
    bolero::check!().for_each(|data: &[u8]| {
        let Some(request) = request_from_bytes(data) else {
            return;
        };

        let mut buf = [0u8; 256];
        let seq = 42u64;
        let written = match codec::encode_request(&request, seq, &mut buf) {
            Ok(n) => n,
            Err(_) => return,
        };

        // decode_request expects payload after the 4-byte length prefix.
        let (decoded_seq, decoded) = codec::decode_request(&buf[4..written])
            .expect("decode of freshly encoded request must succeed");
        assert_eq!(decoded_seq, seq, "seq round-trip mismatch");
        assert_eq!(decoded, request, "request round-trip mismatch");
    });
}

/// Wire response encode → decode round-trip must be lossless.
#[test]
fn fuzz_wire_response_roundtrip() {
    bolero::check!().for_each(|data: &[u8]| {
        let Some(response) = response_from_bytes(data) else {
            return;
        };

        let mut buf = [0u8; 512];
        let written = match codec::encode_response(&response, &mut buf) {
            Ok(n) => n,
            Err(_) => return,
        };

        let decoded = codec::decode_response(&buf[4..written])
            .expect("decode of freshly encoded response must succeed");
        assert_eq!(decoded, response, "response round-trip mismatch");
    });
}

// ---------------------------------------------------------------------------
// Helpers: construct valid protocol types from raw bytes
// ---------------------------------------------------------------------------

use melin_protocol::message::{Request, ResponseKind};
use melin_types::types::*;
use std::num::NonZeroU64;

fn nz64(data: &[u8], offset: usize) -> Option<NonZeroU64> {
    if data.len() < offset + 8 {
        return None;
    }
    NonZeroU64::new(u64::from_le_bytes(
        data[offset..offset + 8].try_into().ok()?,
    ))
}

fn u32_at(data: &[u8], offset: usize) -> Option<u32> {
    if data.len() < offset + 4 {
        return None;
    }
    Some(u32::from_le_bytes(
        data[offset..offset + 4].try_into().ok()?,
    ))
}

fn u64_at(data: &[u8], offset: usize) -> Option<u64> {
    if data.len() < offset + 8 {
        return None;
    }
    Some(u64::from_le_bytes(
        data[offset..offset + 8].try_into().ok()?,
    ))
}

fn request_from_bytes(data: &[u8]) -> Option<Request> {
    if data.is_empty() {
        return None;
    }

    match data[0] % 7 {
        0 => {
            // SubmitOrder.
            if data.len() < 29 {
                return None;
            }
            let symbol = Symbol(u32_at(data, 1)?);
            let id = OrderId(u64_at(data, 5)?);
            let account = AccountId(u32_at(data, 13)?);
            let side = if data[17] & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let qty = Quantity(nz64(data, 18)?);
            let tif = match data[26] % 3 {
                0 => TimeInForce::GTC,
                1 => TimeInForce::IOC,
                _ => TimeInForce::FOK,
            };
            let stp = match data[27] % 4 {
                0 => SelfTradeProtection::Allow,
                1 => SelfTradeProtection::CancelNewest,
                2 => SelfTradeProtection::CancelOldest,
                _ => SelfTradeProtection::CancelBoth,
            };
            let order_type = match data[28] % 4 {
                0 => OrderType::Market,
                1 => OrderType::Limit {
                    price: Price(nz64(data, 29)?),
                    post_only: false,
                },
                2 => OrderType::Stop {
                    trigger_price: Price(nz64(data, 29)?),
                },
                _ => OrderType::StopLimit {
                    trigger_price: Price(nz64(data, 29)?),
                    limit_price: Price(nz64(data, 37)?),
                },
            };
            Some(Request::SubmitOrder {
                symbol,
                order: Order {
                    id,
                    account,
                    side,
                    order_type,
                    time_in_force: tif,
                    quantity: qty,
                    stp,
                    expiry_ns: 0,
                },
            })
        }
        1 => Some(Request::CancelOrder {
            symbol: Symbol(u32_at(data, 1)?),
            account: AccountId(u32_at(data, 5)?),
            order_id: OrderId(u64_at(data, 9)?),
        }),
        2 => Some(Request::CancelAll {
            account: AccountId(u32_at(data, 1)?),
        }),
        3 => Some(Request::Heartbeat),
        4 => {
            // ChallengeResponse.
            if data.len() < 129 {
                return None;
            }
            let mut signature = [0u8; 64];
            signature.copy_from_slice(&data[1..65]);
            let mut public_key = [0u8; 32];
            public_key.copy_from_slice(&data[65..97]);
            let mut client_x25519_eph = [0u8; 32];
            client_x25519_eph.copy_from_slice(&data[97..129]);
            Some(Request::ChallengeResponse {
                signature,
                public_key,
                client_x25519_eph,
            })
        }
        5 => {
            // Subscribe.
            if data.len() < 2 {
                return None;
            }
            let count = data[1] % 9; // 0..=8
            if data.len() < 2 + (count as usize) * 4 {
                return None;
            }
            let mut symbols = [Symbol(0); 8];
            for (i, sym) in symbols.iter_mut().enumerate().take(count as usize) {
                *sym = Symbol(u32_at(data, 2 + i * 4)?);
            }
            Some(Request::Subscribe { symbols, count })
        }
        _ => {
            // QueryPosition.
            Some(Request::QueryPosition {
                account: AccountId(u32_at(data, 1)?),
            })
        }
    }
}

fn response_from_bytes(data: &[u8]) -> Option<ResponseKind> {
    if data.is_empty() {
        return None;
    }

    match data[0] % 14 {
        0 => {
            // Placed.
            let order_id = OrderId(u64_at(data, 1)?);
            let side = if data.len() > 9 && data[9] & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = Price(nz64(data, 10)?);
            let quantity = Quantity(nz64(data, 18)?);
            Some(ResponseKind::Report(ExecutionReport::Placed {
                order_id,
                symbol: Symbol(1),
                account: AccountId(1),
                side,
                price,
                quantity,
            }))
        }
        1 => {
            // Fill.
            let maker_order_id = OrderId(u64_at(data, 1)?);
            let taker_order_id = OrderId(u64_at(data, 9)?);
            let maker_account = AccountId(u32_at(data, 17)?);
            let taker_account = AccountId(u32_at(data, 21)?);
            let price = Price(nz64(data, 25)?);
            let quantity = Quantity(nz64(data, 33)?);
            Some(ResponseKind::Report(ExecutionReport::Fill {
                maker_order_id,
                taker_order_id,
                symbol: Symbol(1),
                maker_account,
                taker_account,
                price,
                quantity,
                maker_fee: 0,
                taker_fee: 0,
            }))
        }
        2 => {
            // Cancelled.
            let order_id = OrderId(u64_at(data, 1)?);
            let account = AccountId(u32_at(data, 9)?);
            let remaining = Quantity(nz64(data, 13)?);
            Some(ResponseKind::Report(ExecutionReport::Cancelled {
                order_id,
                symbol: Symbol(1),
                account,
                remaining_quantity: remaining,
            }))
        }
        3 => {
            // Triggered.
            let order_id = OrderId(u64_at(data, 1)?);
            let trigger_price = Price(nz64(data, 9)?);
            Some(ResponseKind::Report(ExecutionReport::Triggered {
                order_id,
                symbol: Symbol(1),
                account: AccountId(1),
                trigger_price,
            }))
        }
        4 => {
            // Rejected.
            if data.len() < 14 {
                return None;
            }
            let order_id = OrderId(u64_at(data, 1)?);
            let account = AccountId(u32_at(data, 9)?);
            let reason = match data[13] % 11 {
                0 => RejectReason::NoLiquidity,
                1 => RejectReason::FOKCannotFill,
                2 => RejectReason::InsufficientBalance,
                3 => RejectReason::UnknownAccount,
                4 => RejectReason::UnknownSymbol,
                5 => RejectReason::SelfTradePrevented,
                6 => RejectReason::DuplicateOrderId,
                7 => RejectReason::ExceedsMaxOrderQty,
                8 => RejectReason::ExceedsMaxNotional,
                9 => RejectReason::TradingHalted,
                _ => RejectReason::OutsidePriceBand,
            };
            Some(ResponseKind::Report(ExecutionReport::Rejected {
                order_id,
                symbol: Symbol(1),
                account,
                reason,
            }))
        }
        5 => Some(ResponseKind::EngineError),
        6 => Some(ResponseKind::BatchEnd),
        7 => Some(ResponseKind::ServerReady),
        8 => Some(ResponseKind::Heartbeat),
        9 => {
            // BookSnapshotBegin.
            let symbol = Symbol(u32_at(data, 1)?);
            let last_applied_seq = u64_at(data, 5)?;
            Some(ResponseKind::BookSnapshotBegin {
                symbol,
                last_applied_seq,
            })
        }
        10 => {
            // BookSnapshotLevel.
            if data.len() < 26 {
                return None;
            }
            let symbol = Symbol(u32_at(data, 1)?);
            let side = if data[5] & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let price = Price(nz64(data, 6)?);
            let qty = u64_at(data, 14)?;
            let order_count = u32_at(data, 22)?;
            Some(ResponseKind::BookSnapshotLevel {
                symbol,
                side,
                price,
                qty,
                order_count,
            })
        }
        11 => {
            // BookSnapshotEnd.
            let symbol = Symbol(u32_at(data, 1)?);
            let level_count = u32_at(data, 5)?;
            Some(ResponseKind::BookSnapshotEnd {
                symbol,
                level_count,
            })
        }
        12 => {
            // SnapshotComplete.
            let last_applied_seq = u64_at(data, 1)?;
            Some(ResponseKind::SnapshotComplete { last_applied_seq })
        }
        _ => {
            // PositionSnapshot.
            let account = AccountId(u32_at(data, 1)?);
            if data.len() < 6 {
                return None;
            }
            let count = data[5] % 17; // 0..=16
            if data.len() < 6 + (count as usize) * 20 {
                return None;
            }
            let mut balances = [AccountBalance::ZERO; 16];
            for (i, entry) in balances.iter_mut().enumerate().take(count as usize) {
                let off = 6 + i * 20;
                *entry = AccountBalance {
                    currency: CurrencyId(u32_at(data, off)?),
                    free: u64_at(data, off + 4)?,
                    reserved: u64_at(data, off + 12)?,
                };
            }
            Some(ResponseKind::PositionSnapshot {
                account,
                balances,
                count,
            })
        }
    }
}
