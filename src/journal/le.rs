//! Shared little-endian encoding/decoding helpers.
//!
//! Used by both the journal codec and snapshot serialization to avoid
//! duplicating these primitives.

use crate::types::{Side, TimeInForce};

// --- Fixed-buffer writers (for journal codec's pre-allocated buffer) ---

pub fn put_u16(buf: &mut [u8], v: u16) {
    buf[..2].copy_from_slice(&v.to_le_bytes());
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

pub fn get_u32(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

pub fn get_u64(buf: &[u8]) -> u64 {
    u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ])
}

// --- Vec-appending writers (for snapshot's growable buffer) ---

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

/// TimeInForce encoding: GTC=0, IOC=1, FOK=2.
pub fn encode_tif(tif: TimeInForce) -> u8 {
    match tif {
        TimeInForce::GTC => 0,
        TimeInForce::IOC => 1,
        TimeInForce::FOK => 2,
    }
}

pub fn decode_tif(b: u8) -> Option<TimeInForce> {
    match b {
        0 => Some(TimeInForce::GTC),
        1 => Some(TimeInForce::IOC),
        2 => Some(TimeInForce::FOK),
        _ => None,
    }
}
