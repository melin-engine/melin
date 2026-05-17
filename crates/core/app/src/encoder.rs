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
/// The encoder appends framed bytes (length prefix included) to the
/// caller's reusable scratch buffer. The runtime then flushes the
/// buffer to the socket and clears it. Implementors are typically
/// zero-sized types.
pub trait ResponseEncoder: Send + Sync {
    /// Per-event fan-out report type. Must match
    /// [`crate::Application::Report`] at the call site.
    type Report: Copy;
    /// 1:1 query response type. Must match
    /// [`crate::Application::QueryResponse`] at the call site.
    type Query: Copy;

    /// Encode an application report. Appends framed bytes to `buf`.
    /// Returns `Err` with a static reason on encode failure (the
    /// runtime logs and drops the frame; the connection stays open).
    fn encode_report(&self, report: &Self::Report, buf: &mut Vec<u8>) -> Result<(), &'static str>;

    /// Encode an application query response. Appends framed bytes to
    /// `buf`. Same error semantics as [`Self::encode_report`].
    fn encode_query(&self, query: &Self::Query, buf: &mut Vec<u8>) -> Result<(), &'static str>;
}
