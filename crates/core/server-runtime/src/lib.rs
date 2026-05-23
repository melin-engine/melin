//! Application-agnostic LMAX-pipeline server runtime.
//!
//! Groups the application-agnostic parts of the Melin server: the
//! accept loop, frame reader, durability-policy wiring, admin
//! endpoint, replication, and the optional DPDK transport. Generic
//! over `A: Application` — the binary supplies a concrete app via
//! [`server::run`] (and `server::run_dpdk` under `feature = "dpdk"`) along with caller-supplied
//! `AppFactory`, `RequestDecoder`, `ResponseEncoder`, and event-
//! publisher fn.
//!
//! The trading-side wiring (`ServerApp`, `ExchangeRequestDecoder`,
//! `ExchangeResponseEncoder`, market-data firehose) lives in the
//! separate `melin-server` crate.

pub mod admin;
pub mod durability_policy;
pub mod process;
pub mod reader;
pub mod replication;
pub mod response;
pub mod server;

#[cfg(feature = "dpdk")]
pub mod dpdk_response;
#[cfg(feature = "dpdk")]
pub mod dpdk_transport;

/// Control-plane event the accept loop and response stage exchange.
/// Transport-agnostic — the payload is a socket fd + writer, not an
/// app event — so both build modes refer to the same type.
pub enum ControlEvent {
    Connected {
        connection_id: u64,
        fd: std::os::unix::io::RawFd,
        writer: melin_wire_protocol::blocking::BlockingFrameWriter<Box<dyn std::io::Write + Send>>,
    },
    Disconnected {
        connection_id: u64,
    },
}
