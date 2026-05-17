//! Server-crate facade for the shadow snapshot stage.
//!
//! The stage itself lives in `melin_transport_core::shadow` and is generic
//! over `A: Application`. This module re-exports the run loop at its
//! existing path so call sites (`crate::shadow::run`) keep compiling. The
//! contract tests for `dispatch_event` and the lifecycle tests for `run`
//! both live with the implementation in `transport-core::shadow::tests`.

pub use melin_transport_core::shadow::{dispatch_event, run};
