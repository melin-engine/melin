//! Wire-side response encoder seam.
//!
//! Mirror of [`crate::decoder::RequestDecoder`] on the outbound path:
//! the runtime's response stage consumes
//! `OutputPayload<A::Report, A::QueryResponse>` from the matching
//! engine and needs to turn the application-shaped halves
//! (`Report`, `QueryResponse`) into wire bytes. The encoding is
//! application-shaped (a trading server emits execution reports, a
//! payments server emits settlement acks), so it lives behind this
//! trait. Transport-shaped output-payload variants (`BatchEnd`,
//! `EngineError`) are encoded by the runtime directly and never
//! reach this trait.
//!
//! The trait does not take the full `OutputPayload` envelope —
//! splitting `Report` and `Query` into separate methods keeps the
//! trait's view of the application's output identical to
//! [`crate::Application`]'s `Report` / `QueryResponse` associated
//! types, with no coupling to the transport's envelope type.

/// Encode application-shaped output payloads into wire bytes.
///
/// The encoder writes the full wire frame (length prefix included)
/// into the caller's scratch slice and returns the number of bytes
/// written. The runtime then copies that prefix into the per-
/// connection send buffer (TCP) or a DPDK tx frame, and reuses the
/// scratch slice for the next slot — no per-slot allocation on the
/// hot path. Implementors are typically zero-sized types.
pub trait ResponseEncoder: Send + Sync {
    /// Per-event fan-out report type. Must match
    /// [`crate::Application::Report`] at the call site.
    type Report: Copy;
    /// 1:1 query response type. Must match
    /// [`crate::Application::QueryResponse`] at the call site.
    type Query: Copy;

    /// Encode an application report into `buf`, returning the number
    /// of bytes written. `buf` is guaranteed by the caller to be
    /// large enough for any single response (`MAX_RESPONSE_BUF`).
    /// Returns `Err` with a static reason on encode failure (the
    /// runtime logs at error level and drops the frame; the
    /// connection stays open).
    fn encode_report(&self, report: &Self::Report, buf: &mut [u8]) -> Result<usize, &'static str>;

    /// Encode an application query response into `buf`, returning the
    /// number of bytes written. Same buffer-size guarantee and error
    /// semantics as [`Self::encode_report`].
    fn encode_query(&self, query: &Self::Query, buf: &mut [u8]) -> Result<usize, &'static str>;
}
