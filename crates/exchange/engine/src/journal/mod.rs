//! Exchange-aware snapshot codec.
//!
//! The journal-transport plumbing (codec, writer, reader, replication
//! channel, generic pipeline) lives in `melin-journal` and
//! `melin-transport-core`, and the `TradingEvent`-bound aliases for
//! that plumbing live in `melin-server`. This module is left here only
//! for the `Exchange`-state serialization that necessarily entangles
//! with engine internals.
//!
//! Slated for promotion to `melin_engine::snapshot` at the engine
//! top-level; the `journal/` path is historical.

pub mod snapshot;
