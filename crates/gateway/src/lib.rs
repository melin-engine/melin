//! Trading gateway — client-facing proxy to the matching engine.
//!
//! Accepts client TCP connections and forwards requests to the engine
//! server, relaying responses back. Currently a transparent pass-through;
//! will later add market data dissemination, rate limiting, and auth.

pub mod proxy;
