//! Shared FIX protocol and session infrastructure for Melin gateways.
//!
//! Contains the FIX 4.4 parser/serializer, tag constants, and Ed25519
//! auth helpers. Both `melin-oe-gateway` (order entry) and
//! `melin-md-gateway` (market data) depend on this crate.

pub mod auth;
pub mod fix;
