//! Trading server library — exposes server startup for embedding (benchmarks, tests).

pub mod affinity;
pub(crate) mod amortized_timer;
pub mod event_publisher;
pub mod health;
pub mod promote;
mod reader;
pub mod replication;
pub mod request;
mod response;
pub mod server;
pub mod shadow;
pub mod tick;

#[cfg(feature = "dpdk")]
pub mod dpdk_response;
#[cfg(feature = "dpdk")]
pub mod dpdk_transport;
