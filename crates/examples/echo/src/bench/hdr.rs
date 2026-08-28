//! The histogram on disk, named and encoded the way the Aeron benchmark
//! harness does it, so its tooling reads a run from here without knowing
//! the difference.
//!
//! The file is one HdrHistogram interval-log line: start timestamp,
//! interval length, maximum value, and the histogram itself in the V2
//! compressed encoding. Values are nanoseconds. The name is
//! `<prefix>_rate=<rate>_batch=<batch>_length=<length>.hdr`, with `.FAIL`
//! appended when the run lost a message, and the rate is spelled the way
//! the harness spells it: `1M`, `100K`, or the plain number.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use hdrhistogram::serialization::V2DeflateSerializer;
use hdrhistogram::serialization::interval_log::IntervalLogWriterBuilder;

/// `100K`, `1M`, or a plain integer, to a rate in messages per second.
/// The two suffixes are the ones the harness accepts.
pub fn parse_rate(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let (digits, multiplier) = match s.as_bytes().last() {
        Some(b'K' | b'k') => (&s[..s.len() - 1], 1_000u64),
        Some(b'M' | b'm') => (&s[..s.len() - 1], 1_000_000u64),
        _ => (s, 1u64),
    };
    let value: u64 = digits.parse().map_err(|_| {
        format!("`{s}` is not a rate: use a number, or a number with a K or M suffix")
    })?;
    value
        .checked_mul(multiplier)
        .filter(|&r| r > 0)
        .ok_or_else(|| format!("`{s}` is not a usable rate"))
}

/// The harness's spelling: `M` when whole millions, `K` when whole
/// thousands, otherwise the number.
pub fn rate_as_string(rate: u64) -> String {
    if rate.is_multiple_of(1_000_000) {
        format!("{}M", rate / 1_000_000)
    } else if rate.is_multiple_of(1_000) {
        format!("{}K", rate / 1_000)
    } else {
        rate.to_string()
    }
}

pub fn file_name(prefix: &str, rate: u64, batch: u64, length: usize, ok: bool) -> String {
    let name = format!(
        "{prefix}_rate={}_batch={batch}_length={length}.hdr",
        rate_as_string(rate)
    );
    if ok { name } else { format!("{name}.FAIL") }
}

/// Write `histogram` as one interval, `duration` long, starting at `start`.
pub fn write(
    path: &Path,
    histogram: &Histogram<u64>,
    start: SystemTime,
    duration: Duration,
) -> Result<(), String> {
    let file = File::create(path).map_err(|e| format!("cannot create {}: {e}", path.display()))?;
    let mut writer = BufWriter::new(file);
    let mut serializer = V2DeflateSerializer::new();
    let mut log = IntervalLogWriterBuilder::new()
        .begin_log_with(&mut writer, &mut serializer)
        .map_err(|e| format!("cannot start {}: {e}", path.display()))?;
    // A clock before the epoch is not something a run recovers from
    // meaningfully; zero is the least misleading timestamp to record.
    let since_epoch = start.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    log.write_histogram(histogram, since_epoch, duration, None)
        .map_err(|e| format!("cannot write {}: {e:?}", path.display()))?;
    drop(log);
    writer
        .flush()
        .map_err(|e| format!("cannot flush {}: {e}", path.display()))
}

/// The distribution, in microseconds, the way the harness prints it.
pub fn summary(out: &mut impl Write, histogram: &Histogram<u64>) -> io::Result<()> {
    let us = |ns: u64| ns as f64 / 1_000.0;
    writeln!(out, "Histogram of RTT latencies in MICROSECONDS.")?;
    writeln!(out, "  samples {:>12}", histogram.len())?;
    writeln!(out, "  mean    {:>12.1}", histogram.mean() / 1_000.0)?;
    writeln!(out, "  min     {:>12.1}", us(histogram.min()))?;
    for (label, q) in [
        ("p50", 0.50),
        ("p90", 0.90),
        ("p99", 0.99),
        ("p99.9", 0.999),
        ("p99.99", 0.9999),
    ] {
        writeln!(
            out,
            "  {label:<7} {:>12.1}",
            us(histogram.value_at_quantile(q))
        )?;
    }
    writeln!(out, "  max     {:>12.1}", us(histogram.max()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use hdrhistogram::serialization::Deserializer;

    #[test]
    fn rates_parse_and_print_the_way_the_harness_spells_them() {
        assert_eq!(parse_rate("100K").unwrap(), 100_000);
        assert_eq!(parse_rate("1M").unwrap(), 1_000_000);
        assert_eq!(parse_rate("250000").unwrap(), 250_000);
        assert_eq!(parse_rate("2m").unwrap(), 2_000_000);
        assert!(parse_rate("0").is_err());
        assert!(parse_rate("fast").is_err());
        assert!(parse_rate("").is_err());

        assert_eq!(rate_as_string(100_000), "100K");
        assert_eq!(rate_as_string(1_000_000), "1M");
        assert_eq!(rate_as_string(2_500_000), "2500K");
        assert_eq!(rate_as_string(1_500), "1500");
        for rate in [1u64, 999, 1_000, 25_000, 1_000_000, 3_000_000] {
            assert_eq!(parse_rate(&rate_as_string(rate)).unwrap(), rate);
        }
    }

    #[test]
    fn the_file_is_named_like_the_harness_names_it() {
        assert_eq!(
            file_name("melin-cluster_x", 100_000, 1, 288, true),
            "melin-cluster_x_rate=100K_batch=1_length=288.hdr"
        );
        assert_eq!(
            file_name("t", 1_000_000, 4, 32, false),
            "t_rate=1M_batch=4_length=32.hdr.FAIL"
        );
    }

    /// The file is one interval line whose last field is the V2 compressed
    /// histogram, and decoding it gives the histogram back.
    #[test]
    fn the_written_log_decodes_to_the_same_histogram() {
        let mut h = Histogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        for v in [100_000u64, 150_000, 200_000, 1_000_000, 42_000_000] {
            h.record(v).unwrap();
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.hdr");
        write(&path, &h, SystemTime::now(), Duration::from_secs(20)).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().filter(|l| !l.starts_with('#')).collect();
        assert_eq!(lines.len(), 1, "one interval:\n{text}");
        let fields: Vec<&str> = lines[0].split(',').collect();
        assert_eq!(fields.len(), 4, "{}", lines[0]);
        assert!(fields[3].starts_with("HISTFAAA"), "V2 compressed encoding");
        assert_eq!(fields[1].parse::<f64>().unwrap(), 20.0, "interval length");

        let bytes = base64::engine::general_purpose::STANDARD
            .decode(fields[3])
            .unwrap();
        let back: Histogram<u64> = Deserializer::new().deserialize(&mut &bytes[..]).unwrap();
        assert_eq!(back.len(), h.len());
        assert_eq!(back.max(), h.max());
        assert_eq!(back.value_at_quantile(0.5), h.value_at_quantile(0.5));
    }

    #[test]
    fn the_summary_reports_in_microseconds() {
        let mut h = Histogram::<u64>::new_with_bounds(1, 3_600_000_000_000, 3).unwrap();
        h.record(123_456).unwrap();
        let mut out = Vec::new();
        summary(&mut out, &h).unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("MICROSECONDS"));
        assert!(text.contains("123.4") || text.contains("123.5"), "{text}");
    }
}
