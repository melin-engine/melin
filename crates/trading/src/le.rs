//! Shared little-endian encoding/decoding helpers.
//!
//! Used by both the journal codec and snapshot serialization to avoid
//! duplicating these primitives.

use crate::types::{SelfTradeProtection, Side, TimeInForce};

// --- Fixed-buffer writers (for journal codec's pre-allocated buffer) ---

pub fn put_u16(buf: &mut [u8], v: u16) {
    buf[..2].copy_from_slice(&v.to_le_bytes());
}

/// Signed 16-bit LE write. Delegates to `put_u16` since the byte
/// representation is identical — the type signature gives call-site clarity.
pub fn put_i16(buf: &mut [u8], v: i16) {
    put_u16(buf, v as u16);
}

pub fn put_u32(buf: &mut [u8], v: u32) {
    buf[..4].copy_from_slice(&v.to_le_bytes());
}

pub fn put_u64(buf: &mut [u8], v: u64) {
    buf[..8].copy_from_slice(&v.to_le_bytes());
}

// --- Readers ---

pub fn get_u16(buf: &[u8]) -> u16 {
    u16::from_le_bytes([buf[0], buf[1]])
}

/// Signed 16-bit LE read. Delegates to `get_u16` since the byte
/// representation is identical — the type signature gives call-site clarity.
pub fn get_i16(buf: &[u8]) -> i16 {
    get_u16(buf) as i16
}

pub fn get_u32(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

pub fn get_u64(buf: &[u8]) -> u64 {
    u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ])
}

// --- Vec-appending writers (for snapshot's growable buffer) ---

pub fn push_u16(buf: &mut Vec<u8>, v: u16) {
    buf.extend_from_slice(&v.to_le_bytes());
}

/// Signed 16-bit LE append. Delegates to `push_u16` since the byte
/// representation is identical.
pub fn push_i16(buf: &mut Vec<u8>, v: i16) {
    push_u16(buf, v as u16);
}

pub fn push_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

pub fn push_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

// --- Enum encoding (shared by journal codec and snapshot) ---

/// Side encoding: Buy=0, Sell=1. Single source of truth for both
/// journal and snapshot formats.
pub fn encode_side(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

pub fn decode_side(b: u8) -> Option<Side> {
    match b {
        0 => Some(Side::Buy),
        1 => Some(Side::Sell),
        _ => None,
    }
}

/// TimeInForce encoding: GTC=0, IOC=1, FOK=2, Day=3, GTD=4.
pub fn encode_tif(tif: TimeInForce) -> u8 {
    match tif {
        TimeInForce::GTC => 0,
        TimeInForce::IOC => 1,
        TimeInForce::FOK => 2,
        TimeInForce::Day => 3,
        TimeInForce::GTD => 4,
    }
}

pub fn decode_tif(b: u8) -> Option<TimeInForce> {
    match b {
        0 => Some(TimeInForce::GTC),
        1 => Some(TimeInForce::IOC),
        2 => Some(TimeInForce::FOK),
        3 => Some(TimeInForce::Day),
        4 => Some(TimeInForce::GTD),
        _ => None,
    }
}

/// SelfTradeProtection encoding: Allow=0, CancelNewest=1, CancelOldest=2, CancelBoth=3.
pub fn encode_stp(stp: SelfTradeProtection) -> u8 {
    match stp {
        SelfTradeProtection::Allow => 0,
        SelfTradeProtection::CancelNewest => 1,
        SelfTradeProtection::CancelOldest => 2,
        SelfTradeProtection::CancelBoth => 3,
    }
}

pub fn decode_stp(b: u8) -> Option<SelfTradeProtection> {
    match b {
        0 => Some(SelfTradeProtection::Allow),
        1 => Some(SelfTradeProtection::CancelNewest),
        2 => Some(SelfTradeProtection::CancelOldest),
        3 => Some(SelfTradeProtection::CancelBoth),
        _ => None,
    }
}
