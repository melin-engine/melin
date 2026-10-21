//! Tests that drive the response stage directly, with a hand-held journal
//! cursor, rather than through a running node. They live inside the crate
//! rather than under `tests/` because the stage, its early-reply channel
//! and its ring sizing are private: an application starts the stage
//! through the server, never by hand.

mod response_flush_before_gate;
mod response_flush_within_batch;
