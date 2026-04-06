//! FIX message parser: split on SOH, extract tag=value pairs, validate
//! BeginString, BodyLength, and CheckSum.

use super::checksum;
use super::tags;

/// A parsed FIX field: (tag number, raw value bytes).
#[derive(Debug, Clone)]
pub struct Field<'a> {
    pub tag: u32,
    pub value: &'a [u8],
}

/// A parsed FIX message with validated header and checksum.
#[derive(Debug)]
pub struct FixMessage<'a> {
    fields: Vec<Field<'a>>,
}

/// Errors during FIX message parsing.
#[derive(Debug)]
pub enum ParseError {
    /// Message is empty or has no fields.
    Empty,
    /// A field is missing the '=' separator.
    MalformedField,
    /// Tag is not a valid integer.
    InvalidTag,
    /// First field must be BeginString (8).
    MissingBeginString,
    /// BeginString value is not FIX.4.2.
    UnsupportedVersion,
    /// Second field must be BodyLength (9).
    MissingBodyLength,
    /// BodyLength value is not a valid integer.
    InvalidBodyLength,
    /// Actual body length doesn't match declared BodyLength.
    BodyLengthMismatch { declared: usize, actual: usize },
    /// Last field must be CheckSum (10).
    MissingCheckSum,
    /// CheckSum doesn't match computed value.
    CheckSumMismatch { declared: u8, computed: u8 },
    /// Missing required MsgType (35) field.
    MissingMsgType,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => write!(f, "empty message"),
            Self::MalformedField => write!(f, "malformed field (missing '=')"),
            Self::InvalidTag => write!(f, "invalid tag number"),
            Self::MissingBeginString => write!(f, "first field must be BeginString (8)"),
            Self::UnsupportedVersion => write!(f, "unsupported FIX version (expected FIX.4.2)"),
            Self::MissingBodyLength => write!(f, "second field must be BodyLength (9)"),
            Self::InvalidBodyLength => write!(f, "invalid BodyLength value"),
            Self::BodyLengthMismatch { declared, actual } => {
                write!(f, "BodyLength mismatch: declared {declared}, actual {actual}")
            }
            Self::MissingCheckSum => write!(f, "last field must be CheckSum (10)"),
            Self::CheckSumMismatch { declared, computed } => {
                write!(f, "CheckSum mismatch: declared {declared}, computed {computed}")
            }
            Self::MissingMsgType => write!(f, "missing MsgType (35)"),
        }
    }
}

impl std::error::Error for ParseError {}

impl<'a> FixMessage<'a> {
    /// Parse a raw FIX message from bytes. The input must be a complete
    /// message ending with the CheckSum field and a trailing SOH.
    ///
    /// Validates:
    /// - BeginString (8) = FIX.4.2
    /// - BodyLength (9) matches actual body length
    /// - CheckSum (10) matches computed checksum
    /// - MsgType (35) is present
    pub fn parse(data: &'a [u8]) -> Result<Self, ParseError> {
        if data.is_empty() {
            return Err(ParseError::Empty);
        }

        // Split on SOH into fields. Trailing SOH produces an empty last
        // element which we skip.
        let mut fields = Vec::new();
        for chunk in data.split(|&b| b == tags::SOH) {
            if chunk.is_empty() {
                continue;
            }
            let eq_pos = chunk
                .iter()
                .position(|&b| b == b'=')
                .ok_or(ParseError::MalformedField)?;
            let tag_bytes = &chunk[..eq_pos];
            let value = &chunk[eq_pos + 1..];
            let tag = parse_u32(tag_bytes).ok_or(ParseError::InvalidTag)?;
            fields.push(Field { tag, value });
        }

        if fields.is_empty() {
            return Err(ParseError::Empty);
        }

        // Validate BeginString (first field).
        if fields[0].tag != tags::BEGIN_STRING {
            return Err(ParseError::MissingBeginString);
        }
        if fields[0].value != tags::FIX_4_2 {
            return Err(ParseError::UnsupportedVersion);
        }

        // Validate BodyLength (second field).
        if fields.len() < 2 || fields[1].tag != tags::BODY_LENGTH {
            return Err(ParseError::MissingBodyLength);
        }
        let declared_len =
            parse_usize(fields[1].value).ok_or(ParseError::InvalidBodyLength)?;

        // Validate CheckSum (last field).
        let last = fields.last().ok_or(ParseError::MissingCheckSum)?;
        if last.tag != tags::CHECK_SUM {
            return Err(ParseError::MissingCheckSum);
        }

        // Compute body length: from after "9=<len>\x01" through the SOH
        // before "10=". Find the byte offset where the body starts and
        // where the checksum field starts.
        //
        // Body = everything after the BodyLength SOH up to (and including)
        // the SOH before tag 10.
        let body_start = find_body_start(data).ok_or(ParseError::MissingBodyLength)?;
        let checksum_start = find_checksum_start(data).ok_or(ParseError::MissingCheckSum)?;
        let actual_len = checksum_start - body_start;

        if actual_len != declared_len {
            return Err(ParseError::BodyLengthMismatch {
                declared: declared_len,
                actual: actual_len,
            });
        }

        // Validate checksum: sum of all bytes from tag 8 up to (not
        // including) "10=".
        let declared_checksum =
            parse_u8(last.value).ok_or(ParseError::CheckSumMismatch {
                declared: 0,
                computed: 0,
            })?;
        let computed_checksum = checksum::compute(&data[..checksum_start]);
        if declared_checksum != computed_checksum {
            return Err(ParseError::CheckSumMismatch {
                declared: declared_checksum,
                computed: computed_checksum,
            });
        }

        // Validate MsgType exists.
        if !fields.iter().any(|f| f.tag == tags::MSG_TYPE) {
            return Err(ParseError::MissingMsgType);
        }

        Ok(FixMessage { fields })
    }

    /// Get the first value for a given tag, as raw bytes.
    pub fn get(&self, tag: u32) -> Option<&'a [u8]> {
        self.fields.iter().find(|f| f.tag == tag).map(|f| f.value)
    }

    /// Get the first value for a tag as a UTF-8 string.
    pub fn get_str(&self, tag: u32) -> Option<&'a str> {
        self.get(tag).and_then(|v| std::str::from_utf8(v).ok())
    }

    /// Get the MsgType (tag 35) as bytes.
    pub fn msg_type(&self) -> &'a [u8] {
        // Safe: parse() validated MsgType exists.
        self.get(tags::MSG_TYPE).unwrap()
    }

    /// Get the SenderCompID (tag 49) as a string.
    pub fn sender_comp_id(&self) -> Option<&'a str> {
        self.get_str(tags::SENDER_COMP_ID)
    }

    /// Get the TargetCompID (tag 56) as a string.
    pub fn target_comp_id(&self) -> Option<&'a str> {
        self.get_str(tags::TARGET_COMP_ID)
    }

    /// Get the MsgSeqNum (tag 34) as u64.
    pub fn msg_seq_num(&self) -> Option<u64> {
        self.get(tags::MSG_SEQ_NUM).and_then(parse_u64_slice)
    }

    /// Get all fields (for iteration).
    pub fn fields(&self) -> &[Field<'a>] {
        &self.fields
    }
}

/// Find the byte offset where the body starts (after the BodyLength SOH).
/// Body starts after "9=<digits>\x01".
fn find_body_start(data: &[u8]) -> Option<usize> {
    // Find first SOH (after BeginString).
    let first_soh = data.iter().position(|&b| b == tags::SOH)?;
    // Find second SOH (after BodyLength).
    let second_soh = data[first_soh + 1..]
        .iter()
        .position(|&b| b == tags::SOH)?;
    Some(first_soh + 1 + second_soh + 1)
}

/// Find the byte offset where the CheckSum field starts ("10=...").
fn find_checksum_start(data: &[u8]) -> Option<usize> {
    // Search backwards for "10=" preceded by SOH (or start of data).
    // The checksum is always the last field.
    let needle = b"10=";
    for i in (0..data.len().saturating_sub(needle.len())).rev() {
        if &data[i..i + needle.len()] == needle && (i == 0 || data[i - 1] == tags::SOH) {
            return Some(i);
        }
    }
    None
}

fn parse_u32(bytes: &[u8]) -> Option<u32> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn parse_usize(bytes: &[u8]) -> Option<usize> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn parse_u8(bytes: &[u8]) -> Option<u8> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn parse_u64_slice(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// Read one complete FIX message from a buffered reader.
///
/// FIX messages don't have a fixed framing — we read until we find
/// the CheckSum field (10=xxx|). Returns the raw bytes of the complete
/// message, or None on EOF.
pub fn read_message(reader: &mut impl std::io::Read) -> std::io::Result<Option<Vec<u8>>> {
    let mut buf = Vec::with_capacity(512);
    let mut byte = [0u8; 1];

    // Read bytes one at a time looking for the complete message.
    // This is simple but correct — FIX messages are small (~200 bytes)
    // and the gateway handles tens of sessions, not millions.
    //
    // We detect message completion by looking for the pattern:
    // "10=xxx\x01" at the end of the buffer (checksum is always last).
    loop {
        match reader.read(&mut byte) {
            Ok(0) => {
                return if buf.is_empty() { Ok(None) } else { Ok(Some(buf)) };
            }
            Ok(_) => {
                buf.push(byte[0]);
                // Check if we've received a complete checksum field.
                // Pattern: SOH + "10=" + 3 digits + SOH at end of buffer.
                if buf.len() >= 7 && buf[buf.len() - 1] == tags::SOH {
                    // Look for "10=" near the end.
                    let tail = &buf[buf.len().saturating_sub(8)..];
                    if let Some(pos) = tail
                        .windows(3)
                        .position(|w| w == b"10=")
                    {
                        // Verify the "10=" is preceded by SOH.
                        let abs_pos = buf.len().saturating_sub(8) + pos;
                        if abs_pos == 0 || buf[abs_pos - 1] == tags::SOH {
                            return Ok(Some(buf));
                        }
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Try to extract one complete FIX message from the front of `buf`.
///
/// Scans for the CheckSum terminator pattern (`\x0110=xxx\x01`). If a
/// complete message is found, drains it from `buf` and returns it.
/// Returns `None` if the buffer does not yet contain a complete message.
///
/// This is the io_uring-friendly counterpart to `read_message`: it
/// operates on an accumulated byte buffer instead of a streaming reader.
pub fn try_extract_message(buf: &mut Vec<u8>) -> Option<Vec<u8>> {
    // Minimum valid FIX message: "8=FIX.4.2\x019=N\x0135=X\x0110=000\x01"
    // That's ~30 bytes. Short-circuit if obviously incomplete.
    if buf.len() < 20 {
        return None;
    }

    // Scan for the checksum terminator: SOH + "10=" + 3 digits + SOH.
    // The checksum is always the last field, so the first occurrence of
    // this pattern marks the end of the first complete message.
    let bytes = buf.as_slice();
    for i in 0..bytes.len().saturating_sub(7) {
        // Match: \x0110=ddd\x01
        if bytes[i] == tags::SOH
            && bytes[i + 1] == b'1'
            && bytes[i + 2] == b'0'
            && bytes[i + 3] == b'='
        {
            // Find the trailing SOH after the 3-digit checksum value.
            // Checksum is exactly 3 digits, so the SOH is at i+7.
            if i + 7 < bytes.len() && bytes[i + 7] == tags::SOH {
                let msg_end = i + 8; // inclusive of trailing SOH
                let msg = buf[..msg_end].to_vec();
                buf.drain(..msg_end);
                return Some(msg);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fix::serialize::FixMessageBuilder;

    /// Build a minimal valid FIX message for testing.
    fn sample_heartbeat() -> Vec<u8> {
        FixMessageBuilder::new(tags::MSG_HEARTBEAT)
            .build("SENDER", "TARGET", 1)
    }

    #[test]
    fn parse_valid_heartbeat() {
        let raw = sample_heartbeat();
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_HEARTBEAT);
        assert_eq!(msg.sender_comp_id(), Some("SENDER"));
        assert_eq!(msg.target_comp_id(), Some("TARGET"));
        assert_eq!(msg.msg_seq_num(), Some(1));
    }

    #[test]
    fn parse_empty_is_error() {
        assert!(matches!(FixMessage::parse(b""), Err(ParseError::Empty)));
    }

    #[test]
    fn parse_bad_version() {
        let raw = b"8=FIX.4.4\x019=5\x0135=0\x0110=000\x01";
        let result = FixMessage::parse(raw);
        assert!(matches!(result, Err(ParseError::UnsupportedVersion)));
    }

    #[test]
    fn parse_bad_checksum() {
        let mut raw = sample_heartbeat();
        // Corrupt a byte in the body.
        if let Some(b) = raw.iter_mut().find(|b| **b == b'S') {
            *b = b'X';
        }
        let result = FixMessage::parse(&raw);
        assert!(matches!(result, Err(ParseError::CheckSumMismatch { .. })));
    }

    #[test]
    fn round_trip_with_serializer() {
        let raw = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD001")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORDER_QTY, "100")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .str_tag(tags::TIME_IN_FORCE, "1")
            .build("FIRM_A", "MELIN", 42);

        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_NEW_ORDER_SINGLE);
        assert_eq!(msg.get_str(tags::CL_ORD_ID), Some("ORD001"));
        assert_eq!(msg.get_str(tags::SYMBOL), Some("BTC/USD"));
        assert_eq!(msg.get_str(tags::SIDE), Some("1"));
        assert_eq!(msg.get_str(tags::PRICE), Some("50000.00"));
        assert_eq!(msg.msg_seq_num(), Some(42));
    }

    #[test]
    fn read_message_from_stream() {
        let raw = sample_heartbeat();
        let mut cursor = std::io::Cursor::new(&raw);
        let result = read_message(&mut cursor).unwrap().unwrap();
        assert_eq!(result, raw);
    }

    #[test]
    fn read_message_eof() {
        let mut cursor = std::io::Cursor::new(&[] as &[u8]);
        let result = read_message(&mut cursor).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn extract_complete_message() {
        let raw = sample_heartbeat();
        let mut buf = raw.clone();
        let extracted = try_extract_message(&mut buf).unwrap();
        assert_eq!(extracted, raw);
        assert!(buf.is_empty());
    }

    #[test]
    fn extract_incomplete_message() {
        let raw = sample_heartbeat();
        // Truncate — missing the trailing SOH of the checksum.
        let mut buf = raw[..raw.len() - 1].to_vec();
        assert!(try_extract_message(&mut buf).is_none());
        // Buffer unchanged.
        assert_eq!(buf.len(), raw.len() - 1);
    }

    #[test]
    fn extract_two_messages() {
        let msg1 = sample_heartbeat();
        let msg2 = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "X")
            .str_tag(tags::SYMBOL, "A")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORDER_QTY, "1")
            .str_tag(tags::ORD_TYPE, "1")
            .build("S", "T", 2);

        let mut buf = Vec::new();
        buf.extend_from_slice(&msg1);
        buf.extend_from_slice(&msg2);

        let first = try_extract_message(&mut buf).unwrap();
        assert_eq!(first, msg1);
        let second = try_extract_message(&mut buf).unwrap();
        assert_eq!(second, msg2);
        assert!(buf.is_empty());
    }

    #[test]
    fn extract_empty_buffer() {
        let mut buf = Vec::new();
        assert!(try_extract_message(&mut buf).is_none());
    }
}
