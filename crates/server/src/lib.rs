//! Trading server library — exposes server startup for embedding (benchmarks, tests).

pub mod affinity;
#[cfg(not(feature = "io-uring"))]
mod reader;
pub mod replication;
#[cfg(not(feature = "io-uring"))]
mod response;
pub mod server;

#[cfg(feature = "io-uring")]
mod uring_reader;
#[cfg(feature = "io-uring")]
mod uring_response;
