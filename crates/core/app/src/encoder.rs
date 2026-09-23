//! Wire-side response encoder seam.
//!
//! Mirror of [`crate::decoder::RequestDecoder`] on the outbound path:
//! the runtime's response stage consumes
//! `OutputPayload<A::Report, A::QueryResponse>` from the matching
//! stage and needs to turn the application-shaped halves
//! (`Report`, `QueryResponse`) into wire bytes. The encoding is
//! application-shaped (a trading server emits execution reports, a
//! payments server emits settlement acks), so it lives behind this
//! trait. Transport-shaped output-payload variants (`BatchEnd`,
//! `EngineError`) are encoded by the runtime directly and never
//! reach this trait.
//!
//! As on the request side, the frame is the runtime's. Every response is
//! `[length: u32 LE][tag: u8][body]`: the encoder writes the body, and
//! the runtime writes the length and the protocol's tag in front of it.
//! The body's layout is the application's from its first byte, with no
//! value reserved.
//!
//! The trait does not take the full `OutputPayload` envelope —
//! splitting `Report` and `Query` into separate methods keeps the
//! trait's view of the application's output identical to
//! [`crate::Application`]'s `Report` / `QueryResponse` associated
//! types, with no coupling to the transport's envelope type.

/// Encode application-shaped output payloads into response bodies.
///
/// The encoder writes the body into the start of the caller's scratch
/// slice and returns its length. The runtime frames it in place, then
/// copies the frame into the per-connection send buffer (TCP) or a DPDK
/// tx frame, and reuses the scratch slice for the next slot — no
/// per-slot allocation on the hot path. Implementors are typically
/// zero-sized types.
pub trait ResponseEncoder: Send + Sync {
    /// Per-event fan-out report type. Must match
    /// [`crate::Application::Report`] at the call site.
    type Report: Copy;
    /// 1:1 query response type. Must match
    /// [`crate::Application::QueryResponse`] at the call site.
    type Query: Copy;

    /// Encode an application report's body into `buf` and return how
    /// many bytes it took. `buf` holds the runtime's bound on one
    /// response body (`MAX_RESPONSE_BODY`). Returns `Err` with a static
    /// reason on encode failure (the runtime logs at error level and
    /// drops the response; the connection stays open).
    fn encode_report(&self, report: &Self::Report, buf: &mut [u8]) -> Result<usize, &'static str>;

    /// Encode an application query response's body into `buf`. Same
    /// buffer bound, return value and error semantics as
    /// [`Self::encode_report`].
    fn encode_query(&self, query: &Self::Query, buf: &mut [u8]) -> Result<usize, &'static str>;
}
