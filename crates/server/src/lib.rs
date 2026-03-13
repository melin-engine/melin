//! Trading server library — exposes server startup for embedding (benchmarks, tests).

mod affinity;
#[cfg(not(feature = "io-uring"))]
mod reader;
mod response;
pub mod server;

#[cfg(feature = "io-uring")]
mod uring_reader;
