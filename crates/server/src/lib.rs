//! Trading server library — exposes server startup for embedding (benchmarks, tests).

pub mod affinity;
pub mod event_publisher;
pub mod health;
pub mod promote;
#[cfg(not(feature = "io-uring"))]
mod reader;
pub mod replication;
pub mod request;
#[cfg(not(feature = "io-uring"))]
mod response;
pub mod server;
pub mod shadow;

#[cfg(feature = "io-uring")]
mod uring_reader;
#[cfg(feature = "io-uring")]
mod uring_response;

#[cfg(feature = "dpdk")]
pub mod dpdk_response;
#[cfg(feature = "dpdk")]
pub mod dpdk_transport;
