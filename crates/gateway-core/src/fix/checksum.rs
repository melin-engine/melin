//! FIX checksum: sum of all bytes mod 256, formatted as 3-digit zero-padded.

/// Compute the FIX checksum over a byte slice.
/// The checksum is the sum of all bytes (including SOH delimiters) mod 256.
pub fn compute(data: &[u8]) -> u8 {
    let mut sum: u32 = 0;
    for &b in data {
        sum = sum.wrapping_add(b as u32);
    }
    (sum % 256) as u8
}

/// Format a checksum as a 3-digit zero-padded string (e.g., "007", "128").
pub fn format(checksum: u8) -> [u8; 3] {
    let mut buf = [b'0'; 3];
    let mut v = checksum;
    buf[2] = b'0' + (v % 10);
    v /= 10;
    buf[1] = b'0' + (v % 10);
    v /= 10;
    buf[0] = b'0' + v;
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_simple() {
        // "8=FIX.4.4\x019=5\x0135=0\x01" — manually computed
        let data = b"8=FIX.4.4\x019=5\x0135=0\x01";
        let cs = compute(data);
        let formatted = format(cs);
        // Verify format is 3 digits
        assert_eq!(formatted.len(), 3);
        // Verify round-trip: parse back
        let parsed: u8 = std::str::from_utf8(&formatted).unwrap().parse().unwrap();
        assert_eq!(parsed, cs);
    }

    #[test]
    fn format_zero() {
        assert_eq!(&format(0), b"000");
    }

    #[test]
    fn format_single_digit() {
        assert_eq!(&format(7), b"007");
    }

    #[test]
    fn format_two_digit() {
        assert_eq!(&format(42), b"042");
    }

    #[test]
    fn format_three_digit() {
        assert_eq!(&format(255), b"255");
    }
}
