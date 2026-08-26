//! The receipt as clients keep it, and its text form.
//!
//! Shared by `notary-client`, which writes receipts, and `notary-audit`,
//! which checks them against the journal. The server itself never sees
//! this type: its receipt is the wire frame, decoded by
//! [`Receipt::from_frame`](crate::receipt::Receipt::from_frame).

use crate::{HEAD_LEN, LEAF_LEN, TAG_RESP_RECEIPT, fold};

/// One link of the chain.
///
/// The server's receipt frame carries `entry`, `timestamp_ns`, `prev` and
/// `head`; the client adds the `leaf` it submitted, so the saved receipt
/// states what was attested as well as where and when.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Receipt {
    /// 1-based position in the chain.
    pub entry: u64,
    /// When the sequencer dispatched the leaf, nanoseconds since the Unix
    /// epoch. Folded into `head`.
    pub timestamp_ns: u64,
    /// The digest that was attested.
    pub leaf: [u8; LEAF_LEN],
    /// Commitment before this leaf was folded in.
    pub prev: [u8; HEAD_LEN],
    /// Commitment after.
    pub head: [u8; HEAD_LEN],
}

impl Receipt {
    /// Decode a receipt frame: `[tag][entry: u64][timestamp_ns: u64][prev: 32][head: 32]`.
    pub fn from_frame(frame: &[u8], leaf: [u8; LEAF_LEN]) -> Result<Self, String> {
        if frame.first() != Some(&TAG_RESP_RECEIPT)
            || frame.len() != 1 + 8 + 8 + HEAD_LEN + HEAD_LEN
        {
            return Err(format!("unexpected response to notarize: {}", hex(frame)));
        }
        // Slicing at fixed offsets of a length-checked frame: the
        // conversions cannot fail.
        Ok(Receipt {
            entry: u64::from_le_bytes(frame[1..9].try_into().expect("8-byte entry")),
            timestamp_ns: u64::from_le_bytes(frame[9..17].try_into().expect("8-byte time")),
            leaf,
            prev: frame[17..49].try_into().expect("32-byte prev"),
            head: frame[49..81].try_into().expect("32-byte head"),
        })
    }

    /// The check that makes a receipt self-contained.
    pub fn verifies(&self) -> bool {
        fold(&self.prev, &self.leaf, self.timestamp_ns) == self.head
    }

    /// The text form: one `key: value` line per field, so a receipt can
    /// be read, mailed and diffed without tooling.
    pub fn to_text(&self) -> String {
        format!(
            "entry: {}\ntime_ns: {}\nleaf: {}\nprev: {}\nhead: {}\n",
            self.entry,
            self.timestamp_ns,
            hex(&self.leaf),
            hex(&self.prev),
            hex(&self.head)
        )
    }

    /// Parse the text form. Every field is required, none may repeat, and
    /// unknown keys are an error: a receipt is evidence, so a file that
    /// only mostly parses should not pass. Blank lines and `#` comments
    /// are allowed.
    pub fn from_text(text: &str) -> Result<Self, String> {
        let mut entry = None;
        let mut timestamp_ns = None;
        let mut leaf = None;
        let mut prev = None;
        let mut head = None;

        for (i, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (key, value) = line
                .split_once(':')
                .ok_or_else(|| format!("line {}: expected `key: value`", i + 1))?;
            let value = value.trim();
            let slot = match key.trim() {
                "entry" => &mut entry,
                "time_ns" => &mut timestamp_ns,
                "leaf" => &mut leaf,
                "prev" => &mut prev,
                "head" => &mut head,
                other => return Err(format!("line {}: unknown field `{other}`", i + 1)),
            };
            if slot.replace(value).is_some() {
                return Err(format!("line {}: `{}` given twice", i + 1, key.trim()));
            }
        }

        fn field<'a>(name: &str, value: Option<&'a str>) -> Result<&'a str, String> {
            value.ok_or_else(|| format!("missing field `{name}`"))
        }
        fn number(name: &str, value: Option<&str>) -> Result<u64, String> {
            field(name, value)?
                .parse()
                .map_err(|e| format!("`{name}`: {e}"))
        }
        fn bytes<const N: usize>(name: &str, value: Option<&str>) -> Result<[u8; N], String> {
            unhex(field(name, value)?).map_err(|e| format!("`{name}`: {e}"))
        }

        Ok(Receipt {
            entry: number("entry", entry)?,
            timestamp_ns: number("time_ns", timestamp_ns)?,
            leaf: bytes("leaf", leaf)?,
            prev: bytes("prev", prev)?,
            head: bytes("head", head)?,
        })
    }
}

/// Lower-case hex, no prefix.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Exactly `N` bytes from `2 * N` hex digits, either case.
pub fn unhex<const N: usize>(text: &str) -> Result<[u8; N], String> {
    // Checked up front so the byte-indexed slicing below can never land
    // inside a multi-byte character and panic on a mangled receipt.
    if !text.is_ascii() {
        return Err(format!("not hex: {text}"));
    }
    if text.len() != 2 * N {
        return Err(format!("expected {} hex digits, got {}", 2 * N, text.len()));
    }
    let mut out = [0u8; N];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * i..2 * i + 2], 16)
            .map_err(|_| format!("not hex: {text}"))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TAG_RESP_HEAD;

    fn sample() -> Receipt {
        let leaf = [0x11; LEAF_LEN];
        let prev = [0x22; HEAD_LEN];
        let timestamp_ns = 1_756_211_472_123_456_789;
        Receipt {
            entry: 7,
            timestamp_ns,
            leaf,
            prev,
            head: fold(&prev, &leaf, timestamp_ns),
        }
    }

    #[test]
    fn text_round_trips() {
        let receipt = sample();
        let text = receipt.to_text();
        assert_eq!(Receipt::from_text(&text).unwrap(), receipt);
        assert!(receipt.verifies());
    }

    #[test]
    fn text_tolerates_comments_and_whitespace() {
        let text = format!("# a comment\n\n  entry :  7 \n{}", &sample().to_text()[9..]);
        assert_eq!(Receipt::from_text(&text).unwrap(), sample());
    }

    #[test]
    fn text_is_strict() {
        let good = sample().to_text();
        let cases = [
            ("missing field", good.replace("entry: 7\n", "")),
            ("duplicate field", format!("{good}entry: 7\n")),
            ("unknown field", format!("{good}note: hello\n")),
            ("no colon", format!("{good}garbage\n")),
            ("bad number", good.replace("entry: 7", "entry: seven")),
            ("short hex", good.replace("leaf: 11", "leaf: 1")),
            ("not hex", good.replace("leaf: 11", "leaf: zz")),
        ];
        for (what, text) in cases {
            assert!(Receipt::from_text(&text).is_err(), "{what} must not parse");
        }
    }

    #[test]
    fn a_tampered_receipt_does_not_verify() {
        let mut receipt = sample();
        receipt.timestamp_ns += 1;
        assert!(!receipt.verifies());
    }

    #[test]
    fn frame_is_decoded_at_the_documented_offsets() {
        let receipt = sample();
        let mut frame = vec![TAG_RESP_RECEIPT];
        frame.extend_from_slice(&receipt.entry.to_le_bytes());
        frame.extend_from_slice(&receipt.timestamp_ns.to_le_bytes());
        frame.extend_from_slice(&receipt.prev);
        frame.extend_from_slice(&receipt.head);
        assert_eq!(Receipt::from_frame(&frame, receipt.leaf).unwrap(), receipt);

        assert!(Receipt::from_frame(&frame[..80], receipt.leaf).is_err());
        frame[0] = TAG_RESP_HEAD;
        assert!(Receipt::from_frame(&frame, receipt.leaf).is_err());
    }

    #[test]
    fn hex_round_trips() {
        let bytes = [0x00, 0x7f, 0x80, 0xff];
        assert_eq!(hex(&bytes), "007f80ff");
        assert_eq!(unhex::<4>("007f80ff").unwrap(), bytes);
        assert_eq!(unhex::<4>("007F80FF").unwrap(), bytes);
        assert!(unhex::<4>("007f80").is_err());
        assert!(unhex::<4>("007f80fg").is_err());
        // Eight bytes of UTF-8 but not eight ASCII digits: must be an
        // error, not a panic from slicing mid-character.
        assert!(unhex::<4>("00é7f80f").is_err());
    }
}
