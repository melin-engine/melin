//! Little-endian read/write helpers.
//!
//! Duplicated from `melin-engine::le` to keep the journal crate free of
//! engine-side trading types. Phase 3 may consolidate these into a single
//! shared utility crate if more consumers emerge.

pub fn put_u32(buf: &mut [u8], v: u32) {
    buf[..4].copy_from_slice(&v.to_le_bytes());
}

pub fn put_u64(buf: &mut [u8], v: u64) {
    buf[..8].copy_from_slice(&v.to_le_bytes());
}

pub fn get_u32(buf: &[u8]) -> u32 {
    u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]])
}

pub fn get_u64(buf: &[u8]) -> u64 {
    u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ])
}
