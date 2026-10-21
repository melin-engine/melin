//! Application-agnostic LMAX-pipeline server runtime.
//!
//! Groups the application-agnostic parts of the Melin server: the
//! accept loop, frame reader, ack-policy wiring, admin
//! endpoint, replication, and the optional DPDK transport. Generic
//! over `A: Application` — the binary supplies a concrete app via
//! [`server::run`] (or `server::run_dpdk` under `feature = "dpdk"`) along with caller-supplied
//! [`StartupEvents`], `RequestDecoder`, `ResponseEncoder`, and event-
//! publisher fn.
//!
//! The application-side wiring (the `Application` itself, its decoder
//! and encoder, any event publisher) lives in the application's own
//! crate — see the examples under `crates/examples/`.

pub mod ack_policy;
mod admin;
mod batch_boundary;
mod buf_ring;
pub mod client_auth;
mod client_frames;
mod connection_limit;
mod durability_gate;
mod early_reply;
mod halt;
pub mod layout;
mod policy_control;
mod process;
mod promotion;
mod raft;
mod raft_promotion;
mod reader;
pub mod replication;
mod response;
mod response_frame;
pub mod server;
mod startup;
#[cfg(test)]
mod test_roles;
#[cfg(test)]
mod tests;
mod uring_teardown;

pub use startup::StartupEvents;

#[cfg(feature = "dpdk")]
mod dpdk_response;
#[cfg(feature = "dpdk")]
mod dpdk_transport;

// The two wire bounds an application's codecs have to fit: the widest
// request body the readers hand a decoder, and the widest response body
// the response stages give an encoder room for — both after the
// protocol's framing, which the runtime reads and writes itself.
// Re-exported here so an application can assert against them at compile
// time, next to the bounds it declares (`AppEvent::MAX_ENCODED_SIZE` is
// the journal's; these are the wire's).
pub use client_frames::MAX_REQUEST_BODY;
pub use response_frame::MAX_RESPONSE_BODY;

/// Control-plane event the accept loop, reader, and response stage
/// exchange. Transport-agnostic — the payload is a socket fd + writer,
/// not an app event — so both build modes refer to the same type.
pub(crate) enum ControlEvent {
    Connected {
        connection_id: u64,
        fd: std::os::unix::io::RawFd,
        writer: melin_wire_protocol::blocking::BlockingFrameWriter<Box<dyn std::io::Write + Send>>,
    },
    Disconnected {
        connection_id: u64,
    },
}
