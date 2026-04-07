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
                write!(
                    f,
                    "BodyLength mismatch: declared {declared}, actual {actual}"
                )
            }
            Self::MissingCheckSum => write!(f, "last field must be CheckSum (10)"),
            Self::CheckSumMismatch { declared, computed } => {
                write!(
                    f,
                    "CheckSum mismatch: declared {declared}, computed {computed}"
                )
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
        let declared_len = parse_usize(fields[1].value).ok_or(ParseError::InvalidBodyLength)?;

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
        let declared_checksum = parse_u8(last.value).ok_or(ParseError::CheckSumMismatch {
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
        // Safe: parse() rejects messages without MsgType, so any
        // FixMessage value reaching this method has tag 35.
        self.get(tags::MSG_TYPE)
            .expect("MsgType validated by FixMessage::parse")
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

    /// Iterate all parsed fields in order. Used by the resend path
    /// to rebuild a stored message with PossDupFlag/OrigSendingTime
    /// while preserving the original payload.
    pub fn fields_iter(&self) -> impl Iterator<Item = &Field<'a>> {
        self.fields.iter()
    }
}

/// Find the byte offset where the body starts (after the BodyLength SOH).
/// Body starts after "9=<digits>\x01".
fn find_body_start(data: &[u8]) -> Option<usize> {
    // Find first SOH (after BeginString).
    let first_soh = data.iter().position(|&b| b == tags::SOH)?;
    // Find second SOH (after BodyLength).
    let second_soh = data[first_soh + 1..].iter().position(|&b| b == tags::SOH)?;
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

    // Defense in depth: a valid FIX 4.2 frame always starts with the
    // `8=FIX.4.2\x01` prefix. If the buffer starts with anything else,
    // we're either looking at a stream framing bug or actively
    // misaligned data — refuse to extract garbage that just happens
    // to contain a `\x0110=xxx\x01` pattern downstream.
    const PREFIX: &[u8] = b"8=FIX.4.2\x01";
    if !buf.starts_with(PREFIX) {
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
        FixMessageBuilder::new(tags::MSG_HEARTBEAT).build("SENDER", "TARGET", 1)
    }

    #[test]
    fn parse_valid_heartbeat() {
        let raw = sample_heartbeat();
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_HEARTBEAT);
        assert_eq!(msg.sender_comp_id(), Some("SENDER"));
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

#[cfg(test)]
mod proptests {
    //! Property-based tests for the FIX parser. The parser is the
    //! gateway's outermost trust boundary; these tests aim at:
    //!   * round-trip stability with the serializer,
    //!   * no panics on arbitrary inputs,
    //!   * single-byte corruption is always detected, and
    //!   * frame extraction is stable under trailing garbage.
    use super::*;
    use crate::fix::serialize::FixMessageBuilder;
    use proptest::prelude::*;

    /// Bytes that are safe to embed inside a FIX field value:
    /// printable ASCII (0x20..=0x7e) excluding `=` (the tag/value
    /// separator) and SOH (the field terminator). Anything else would
    /// either confuse the parser or be rejected at a higher layer.
    fn fix_safe_string(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(
            (0x20u8..=0x7eu8).prop_filter("no '=' or SOH", |b| *b != b'=' && *b != tags::SOH),
            1..=max_len,
        )
        .prop_map(|v| String::from_utf8(v).unwrap())
    }

    /// One of the standard FIX 4.2 MsgType bytes the gateway emits.
    /// Restricted to known types so the strategy never produces
    /// something the builder couldn't legitimately have created.
    fn msg_type() -> impl Strategy<Value = &'static [u8]> {
        prop_oneof![
            Just(tags::MSG_HEARTBEAT),
            Just(tags::MSG_TEST_REQUEST),
            Just(tags::MSG_LOGON),
            Just(tags::MSG_LOGOUT),
            Just(tags::MSG_NEW_ORDER_SINGLE),
            Just(tags::MSG_EXECUTION_REPORT),
            Just(tags::MSG_RESEND_REQUEST),
            Just(tags::MSG_SEQUENCE_RESET),
        ]
    }

    /// User-defined field tag. Avoids the small standard tag space the
    /// builder injects (so the round-trip assertions stay clean).
    fn user_tag() -> impl Strategy<Value = u32> {
        // Tags > 1000 are well clear of every constant in tags.rs.
        1001u32..10_000u32
    }

    proptest! {
        // Trust-boundary properties run with a higher case count than
        // proptest's default 256 — the parser is the gateway's outer
        // attack surface and the extra cases are cheap (~ms total).
        #![proptest_config(ProptestConfig::with_cases(2048))]

        /// Round-trip: build a message, parse it back, every field we
        /// put in shows up in the parse output with the right value.
        #[test]
        fn round_trip_preserves_header_and_user_fields(
            mt in msg_type(),
            sender in fix_safe_string(16),
            target in fix_safe_string(16),
            seq in 1u64..=u64::MAX,
            user_fields in proptest::collection::vec(
                (user_tag(), fix_safe_string(32)),
                0..8,
            ),
        ) {
            // Deduplicate tags so the "first occurrence" semantics of
            // FixMessage::get are unambiguous in the assertion below.
            let mut seen = std::collections::HashSet::new();
            let user_fields: Vec<_> = user_fields
                .into_iter()
                .filter(|(t, _)| seen.insert(*t))
                .collect();

            let mut builder = FixMessageBuilder::new(mt);
            for (t, v) in &user_fields {
                builder = builder.str_tag(*t, v);
            }
            let raw = builder.build(&sender, &target, seq);

            let msg = FixMessage::parse(&raw).expect("valid round trip");
            prop_assert_eq!(msg.msg_type(), mt);
            prop_assert_eq!(msg.sender_comp_id(), Some(sender.as_str()));
            prop_assert_eq!(msg.target_comp_id(), Some(target.as_str()));
            prop_assert_eq!(msg.msg_seq_num(), Some(seq));
            for (t, v) in &user_fields {
                prop_assert_eq!(msg.get_str(*t), Some(v.as_str()));
            }
        }

        /// The parser must never panic on arbitrary bytes — the worst
        /// it may do is return Err. This is the trust-boundary
        /// guarantee: a hostile peer cannot crash the gateway by
        /// sending malformed FIX-ish bytes.
        #[test]
        fn parse_does_not_panic_on_arbitrary_bytes(
            data in proptest::collection::vec(any::<u8>(), 0..512)
        ) {
            let _ = FixMessage::parse(&data);
        }

        /// try_extract_message must also never panic and must never
        /// return more bytes than the input. If it returns Some, the
        /// returned slice is what was at the front of the buffer.
        #[test]
        fn try_extract_message_does_not_panic_on_arbitrary_bytes(
            data in proptest::collection::vec(any::<u8>(), 0..512)
        ) {
            let mut buf = data.clone();
            let _ = try_extract_message(&mut buf);
            prop_assert!(buf.len() <= data.len());
        }

        /// Flipping a single byte in the body of a valid message
        /// must always be detected. The parser may surface this as
        /// any of: malformed field, body-length mismatch, checksum
        /// mismatch, or one of the structural errors — but it must
        /// not silently accept a tampered message.
        #[test]
        fn single_byte_corruption_in_body_is_always_detected(
            mt in msg_type(),
            sender in fix_safe_string(8),
            target in fix_safe_string(8),
            seq in 1u64..=1_000_000u64,
            body_field in fix_safe_string(8),
            flip_idx in any::<usize>(),
            xor_mask in 1u8..=255u8,
        ) {
            let mut raw = FixMessageBuilder::new(mt)
                .str_tag(tags::TEXT, &body_field)
                .build(&sender, &target, seq);

            // Pick an index inside the body region (between the
            // BodyLength SOH and the start of "10="). Corrupting bytes
            // outside that range would either invalidate framing in
            // ways the test doesn't care about (header) or rebuild a
            // self-consistent checksum (trailer).
            let body_start = find_body_start(&raw).unwrap();
            let cs_start = find_checksum_start(&raw).unwrap();
            prop_assume!(cs_start > body_start);
            let target_idx = body_start + (flip_idx % (cs_start - body_start));

            let original = raw[target_idx];
            raw[target_idx] = original ^ xor_mask;
            // Flipping `=` or SOH is allowed — it's still a corrupted
            // byte the parser must reject.
            prop_assert!(
                FixMessage::parse(&raw).is_err(),
                "tampered byte at {} ({:#x} -> {:#x}) was silently accepted",
                target_idx, original, raw[target_idx]
            );
        }

        /// Concatenating a valid message with arbitrary trailing bytes
        /// must yield the original message back from
        /// try_extract_message, leaving the trailing bytes in the
        /// buffer. This is the framing contract the event loop relies
        /// on under partial reads.
        #[test]
        fn extract_is_stable_under_trailing_garbage(
            mt in msg_type(),
            sender in fix_safe_string(8),
            target in fix_safe_string(8),
            seq in 1u64..=1_000_000u64,
            tail in proptest::collection::vec(any::<u8>(), 0..64),
        ) {
            let raw = FixMessageBuilder::new(mt).build(&sender, &target, seq);
            let mut buf = raw.clone();
            buf.extend_from_slice(&tail);
            let extracted = try_extract_message(&mut buf).expect("frame visible");
            prop_assert_eq!(&extracted, &raw);
            prop_assert_eq!(buf.as_slice(), tail.as_slice());
        }
    }
}
