//! FIX message builder: construct messages with auto-computed BodyLength
//! and CheckSum.

use super::checksum;
use super::tags;

/// Builder for constructing FIX messages.
///
/// Usage:
/// ```ignore
/// let msg = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
///     .str_tag(tags::CL_ORD_ID, "ORD001")
///     .str_tag(tags::SYMBOL, "BTC/USD")
///     .build("SENDER", "TARGET", 42);
/// ```
pub struct FixMessageBuilder {
    /// Body fields (after BeginString and BodyLength, before CheckSum).
    body: Vec<u8>,
}

impl FixMessageBuilder {
    /// Create a new builder with the given MsgType (tag 35).
    pub fn new(msg_type: &[u8]) -> Self {
        let mut body = Vec::with_capacity(256);
        append_field(&mut body, tags::MSG_TYPE, msg_type);
        Self { body }
    }

    /// Add a field with raw byte value.
    pub fn tag(mut self, tag: u32, value: &[u8]) -> Self {
        append_field(&mut self.body, tag, value);
        self
    }

    /// Add a field with a string value.
    pub fn str_tag(self, tag: u32, value: &str) -> Self {
        self.tag(tag, value.as_bytes())
    }

    /// Add a field with a u64 value.
    pub fn u64_tag(self, tag: u32, value: u64) -> Self {
        let s = value.to_string();
        self.tag(tag, s.as_bytes())
    }

    /// Build the complete FIX message with header and trailer.
    ///
    /// Adds: BeginString (8), BodyLength (9), SenderCompID (49),
    /// TargetCompID (56), MsgSeqNum (34), SendingTime (52) to the header,
    /// and CheckSum (10) to the trailer.
    pub fn build(mut self, sender: &str, target: &str, seq_num: u64) -> Vec<u8> {
        // Insert standard header fields after MsgType.
        let mut header_fields = Vec::with_capacity(128);
        append_field(&mut header_fields, tags::SENDER_COMP_ID, sender.as_bytes());
        append_field(&mut header_fields, tags::TARGET_COMP_ID, target.as_bytes());
        append_field(
            &mut header_fields,
            tags::MSG_SEQ_NUM,
            seq_num.to_string().as_bytes(),
        );
        append_field(
            &mut header_fields,
            tags::SENDING_TIME,
            sending_time().as_bytes(),
        );

        // Body = MsgType + header fields + user fields
        // (MsgType is already in self.body from new())
        // Safe: `new()` always pushes "35=<type>\x01" into self.body,
        // so an SOH is guaranteed to exist.
        let msg_type_end = self
            .body
            .iter()
            .position(|&b| b == tags::SOH)
            .expect("MsgType SOH inserted by FixMessageBuilder::new")
            + 1;
        let mut full_body = Vec::with_capacity(self.body.len() + header_fields.len());
        full_body.extend_from_slice(&self.body[..msg_type_end]);
        full_body.extend_from_slice(&header_fields);
        full_body.extend_from_slice(&self.body[msg_type_end..]);
        self.body = full_body;

        // Compute body length: byte count of body (everything between
        // BodyLength SOH and CheckSum tag).
        let body_len = self.body.len();
        let body_len_str = body_len.to_string();

        // Build the full message.
        let mut msg = Vec::with_capacity(body_len + 32);
        // BeginString
        append_field(&mut msg, tags::BEGIN_STRING, tags::FIX_VERSION);
        // BodyLength
        append_field(&mut msg, tags::BODY_LENGTH, body_len_str.as_bytes());
        // Body
        msg.extend_from_slice(&self.body);
        // CheckSum
        let cs = checksum::compute(&msg);
        let cs_str = checksum::format(cs);
        append_field(&mut msg, tags::CHECK_SUM, &cs_str);

        msg
    }
}

/// Append "tag=value\x01" to a buffer.
fn append_field(buf: &mut Vec<u8>, tag: u32, value: &[u8]) {
    // Write tag as decimal digits.
    let tag_str = tag.to_string();
    buf.extend_from_slice(tag_str.as_bytes());
    buf.push(b'=');
    buf.extend_from_slice(value);
    buf.push(tags::SOH);
}

/// Current UTC time in FIX format: YYYYMMDD-HH:MM:SS.sss
fn sending_time() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    // A pre-epoch system clock means the host is fundamentally
    // misconfigured; emitting a bogus 1970 SendingTime would silently
    // produce non-compliant FIX messages. Crash loudly instead.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before UNIX_EPOCH");
    let secs = now.as_secs();
    let millis = now.subsec_millis();

    // Manual UTC formatting (no chrono dependency).
    let days = secs / 86400;
    let time_secs = secs % 86400;
    let hours = time_secs / 3600;
    let minutes = (time_secs % 3600) / 60;
    let seconds = time_secs % 60;

    // Days since epoch → year/month/day (simplified Gregorian).
    let (year, month, day) = days_to_ymd(days);

    format!("{year:04}{month:02}{day:02}-{hours:02}:{minutes:02}:{seconds:02}.{millis:03}")
}

/// Convert days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u32, u32, u32) {
    // Algorithm from Howard Hinnant's civil_from_days.
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as u32, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_heartbeat() {
        let msg = FixMessageBuilder::new(tags::MSG_HEARTBEAT).build("SENDER", "TARGET", 1);
        // Should start with "8=FIX.4.4\x01"
        assert!(msg.starts_with(b"8=FIX.4.4\x01"));
        // Should end with "10=xxx\x01"
        let s = std::str::from_utf8(&msg).unwrap();
        assert!(s.contains("35=0\x01"));
        assert!(s.contains("49=SENDER\x01"));
        assert!(s.contains("56=TARGET\x01"));
        assert!(s.contains("34=1\x01"));
        assert!(s.ends_with('\x01'));
    }

    #[test]
    fn build_with_fields() {
        let msg = FixMessageBuilder::new(tags::MSG_NEW_ORDER_SINGLE)
            .str_tag(tags::CL_ORD_ID, "ORD001")
            .str_tag(tags::SYMBOL, "BTC/USD")
            .str_tag(tags::SIDE, "1")
            .str_tag(tags::ORDER_QTY, "100")
            .str_tag(tags::ORD_TYPE, "2")
            .str_tag(tags::PRICE, "50000.00")
            .build("FIRM_A", "MELIN", 5);

        let s = std::str::from_utf8(&msg).unwrap();
        assert!(s.contains("35=D\x01"));
        assert!(s.contains("11=ORD001\x01"));
        assert!(s.contains("55=BTC/USD\x01"));
        assert!(s.contains("44=50000.00\x01"));
    }

    #[test]
    fn sending_time_format() {
        let ts = sending_time();
        // Format: YYYYMMDD-HH:MM:SS.mmm
        assert_eq!(ts.len(), 21);
        assert_eq!(ts.as_bytes()[8], b'-');
        assert_eq!(ts.as_bytes()[11], b':');
        assert_eq!(ts.as_bytes()[14], b':');
        assert_eq!(ts.as_bytes()[17], b'.');
    }
}
