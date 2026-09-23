//! Protocol error types.

use std::fmt;

/// Errors that can occur during protocol encoding/decoding.
#[derive(Debug)]
pub enum ProtocolError {
    /// Buffer does not contain a complete message.
    Truncated,
    /// Unknown message type tag.
    UnknownTag(u8),
    /// A field value is invalid (e.g., zero where a count is required, bad enum discriminant).
    InvalidField(&'static str),
    /// Message exceeds maximum allowed frame size.
    MessageTooLarge(usize),
    /// Underlying I/O error.
    Io(std::io::Error),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "truncated message"),
            Self::UnknownTag(tag) => write!(f, "unknown message tag: {tag}"),
            Self::InvalidField(field) => write!(f, "invalid field: {field}"),
            Self::MessageTooLarge(size) => write!(f, "message too large: {size} bytes"),
            Self::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ProtocolError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
