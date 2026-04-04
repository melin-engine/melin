//! Trading server library — exposes server startup for embedding (benchmarks, tests).

pub mod affinity;
pub mod event_publisher;
pub mod health;
pub mod promote;
mod reader;
pub mod replication;
pub mod request;
mod response;
pub mod server;
pub mod shadow;

#[cfg(feature = "dpdk")]
pub mod dpdk_response;
#[cfg(feature = "dpdk")]
pub mod dpdk_transport;
