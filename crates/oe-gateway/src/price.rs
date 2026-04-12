//! Decimal string ↔ u64 tick conversion using pure integer arithmetic.
//!
//! FIX prices are decimal strings (e.g., "50100.50"). Melin prices are
//! u64 ticks. The conversion uses a configurable `tick_size_inverse`
//! (e.g., tick_size=0.01 → inverse=100).
//!
//! Example: "50100.50" × 100 = 5_010_050 ticks.

/// Convert a FIX decimal price string to Melin ticks.
///
/// `inverse` is `1 / tick_size` — e.g., if tick_size is 0.01 then
/// inverse is 100. Pure integer arithmetic: no floats.
///
/// Returns `None` if the string is malformed, negative, has more
/// decimal places than the inverse supports, `inverse` is zero, or
/// the result would overflow `u64`.
pub fn decimal_to_ticks(s: &str, inverse: u64) -> Option<u64> {
    if inverse == 0 {
        return None;
    }
    let s = s.trim();
    if s.is_empty() || s.starts_with('-') {
        return None;
    }

    // Split on decimal point.
    let (integer_part, frac_part) = if let Some(dot) = s.find('.') {
        (&s[..dot], &s[dot + 1..])
    } else {
        (s, "")
    };

    // Parse integer part.
    let int_val: u64 = if integer_part.is_empty() {
        0
    } else {
        // Discard parse error: a malformed integer part is the same
        // outcome as None for the caller — `decimal_to_ticks` returns
        // None for any non-numeric input.
        integer_part.parse().ok()?
    };

    // Determine how many decimal digits the inverse represents.
    let inverse_digits = count_digits(inverse) - 1; // e.g., 100 → 2 digits

    // Parse fractional part, padded/truncated to inverse_digits.
    let frac_val: u64 = if frac_part.is_empty() {
        0
    } else if frac_part.len() <= inverse_digits {
        // Pad with trailing zeros: "5" with inverse=100 → "50" → 50
        let mut padded = String::from(frac_part);
        while padded.len() < inverse_digits {
            padded.push('0');
        }
        // Discard parse error: same rationale as the integer part —
        // a malformed fractional part propagates as None.
        padded.parse().ok()?
    } else {
        // More decimal places than tick size supports — reject.
        // (Could truncate, but that silently loses precision.)
        return None;
    };

    int_val.checked_mul(inverse)?.checked_add(frac_val)
}

/// Convert Melin ticks back to a FIX decimal price string.
///
/// `inverse` is `1 / tick_size` — e.g., 100 for tick_size=0.01.
pub fn ticks_to_decimal(ticks: u64, inverse: u64) -> String {
    if inverse <= 1 {
        return ticks.to_string();
    }

    let int_part = ticks / inverse;
    let frac_part = ticks % inverse;
    let digits = count_digits(inverse) - 1;

    if frac_part == 0 {
        format!("{int_part}.{:0>width$}", 0, width = digits)
    } else {
        format!("{int_part}.{frac_part:0>width$}", width = digits)
    }
}

/// Count the number of decimal digits in a number.
fn count_digits(mut n: u64) -> usize {
    if n == 0 {
        return 1;
    }
    let mut count = 0;
    while n > 0 {
        count += 1;
        n /= 10;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_conversion() {
        assert_eq!(decimal_to_ticks("50000.00", 100), Some(5_000_000));
        assert_eq!(decimal_to_ticks("50000.50", 100), Some(5_000_050));
        assert_eq!(decimal_to_ticks("0.01", 100), Some(1));
        assert_eq!(decimal_to_ticks("1", 100), Some(100));
        assert_eq!(decimal_to_ticks("1.0", 100), Some(100));
    }

    #[test]
    fn inverse_1() {
        // No decimal places — prices are integer ticks.
        assert_eq!(decimal_to_ticks("42", 1), Some(42));
        assert_eq!(ticks_to_decimal(42, 1), "42");
    }

    #[test]
    fn inverse_1000() {
        assert_eq!(decimal_to_ticks("1.234", 1000), Some(1234));
        assert_eq!(decimal_to_ticks("1.2", 1000), Some(1200));
        assert_eq!(ticks_to_decimal(1234, 1000), "1.234");
        assert_eq!(ticks_to_decimal(1200, 1000), "1.200");
    }

    #[test]
    fn too_many_decimals_rejected() {
        // tick_size=0.01 (inverse=100) can't represent 3 decimal places.
        assert_eq!(decimal_to_ticks("1.234", 100), None);
    }

    #[test]
    fn round_trip() {
        for ticks in [0, 1, 100, 5_010_050, 99_999_999] {
            let s = ticks_to_decimal(ticks, 100);
            let back = decimal_to_ticks(&s, 100).unwrap();
            assert_eq!(back, ticks, "round-trip failed for {ticks}: '{s}'");
        }
    }

    #[test]
    fn negative_rejected() {
        assert_eq!(decimal_to_ticks("-1.00", 100), None);
    }

    #[test]
    fn empty_rejected() {
        assert_eq!(decimal_to_ticks("", 100), None);
    }

    #[test]
    fn no_integer_part() {
        assert_eq!(decimal_to_ticks(".50", 100), Some(50));
    }

    #[test]
    fn overflow_rejected() {
        // u64::MAX * 100 overflows.
        assert_eq!(decimal_to_ticks("18446744073709551615", 100), None);
    }

    #[test]
    fn zero_inverse_rejected() {
        assert_eq!(decimal_to_ticks("1.00", 0), None);
    }

    #[test]
    fn trailing_zeros_preserved() {
        assert_eq!(ticks_to_decimal(100, 100), "1.00");
        assert_eq!(ticks_to_decimal(0, 100), "0.00");
    }
}
