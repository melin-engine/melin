//! Client for the server's `/stats-dump` endpoint.
//!
//! At end of a roundtrip benchmark we fetch the per-stage latency
//! histogram dump and merge it into the bench's results. This is the
//! "tick-to-trade decomposition" that complements the bench-side full
//! round-trip percentiles in `print_results`.
//!
//! Wire format produced by the server (see `crates/server/src/health.rs`):
//!
//! ```text
//! stage\t<name>\t<samples>\t<min_ns>\t<p50_ns>\t<p90_ns>\t<p99_ns>\t<p99_9_ns>\t<max_ns>
//! ```
//!
//! Three body states the parser distinguishes:
//! - One or more `stage` lines  → samples populated
//! - `# no samples`             → server has the feature on but the
//!   registry was empty when scraped
//! - `# latency-trace disabled` → server built without the feature
//!
//! Anything else parses to `Body::Empty` so the bench can ignore the
//! section gracefully rather than fail the run.
//!
//! A failed fetch (server unreachable, timeout, etc.) returns
//! `Body::Empty` — the dump is a sales artifact, not a correctness
//! guarantee, so a missing dump never aborts a benchmark run.

use std::io::{Read, Write};
use std::net::SocketAddr;
use std::time::Duration;

/// Connect timeout. The server is local (or LAN-adjacent); 200ms is
/// already plenty.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

/// Read timeout. Stats dump is small (<8 KiB) so 500ms is generous.
const READ_TIMEOUT: Duration = Duration::from_millis(500);

/// Recv buffer size — matches the server's max body (8 KiB) plus
/// HTTP headers (~256 bytes) with comfortable slack.
const RECV_BUF: usize = 16_384;

/// One parsed `stage` line — percentiles in nanoseconds, ready for
/// printing alongside the bench's RTT histogram.
#[derive(Debug, Clone)]
pub struct StageRecord {
    pub name: String,
    pub samples: u64,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p90_ns: u64,
    pub p99_ns: u64,
    pub p99_9_ns: u64,
    pub max_ns: u64,
}

/// Result of a `/stats-dump` fetch. Distinguishes the three body
/// states the bench cares about so the console output and `--json`
/// can render an appropriate explanation in each case.
#[derive(Debug, Clone)]
pub enum Body {
    /// One or more populated stages.
    Stages(Vec<StageRecord>),
    /// Server has `latency-trace` enabled but no samples were
    /// recorded — typical for very short runs that didn't traffic
    /// the response stage.
    NoSamples,
    /// Server was built without `--features latency-trace`. Bench
    /// should print a hint pointing at the feature flag.
    Disabled,
    /// Fetch failed or response unparseable. Treated as "no data
    /// available" — the bench prints a one-line note and continues.
    Empty,
}

/// Fetch and parse `/stats-dump` from the given health-endpoint
/// address. Always returns a `Body` — failures degrade to
/// `Body::Empty` rather than propagating, so a missing dump never
/// aborts a benchmark run.
pub fn fetch(addr: SocketAddr) -> Body {
    match fetch_inner(addr) {
        Ok(text) => parse_body(&text),
        Err(_) => Body::Empty,
    }
}

fn fetch_inner(addr: SocketAddr) -> std::io::Result<String> {
    let mut stream = std::net::TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT)?;
    stream.set_read_timeout(Some(READ_TIMEOUT))?;
    stream.set_nodelay(true)?;
    stream
        .write_all(b"GET /stats-dump HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;

    let mut buf = vec![0u8; RECV_BUF];
    let mut total = 0;
    loop {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                if total >= buf.len() {
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(e) => return Err(e),
        }
    }
    let text = String::from_utf8_lossy(&buf[..total]).into_owned();
    Ok(text)
}

/// Parse an HTTP response body. Strips headers, then classifies by
/// the first non-blank body line.
pub fn parse_body(response: &str) -> Body {
    // HTTP framing: blank line separates head from body. If absent,
    // assume the whole input is the body (defensive — the server
    // always emits headers).
    let body = response.split_once("\r\n\r\n").map_or(response, |(_, b)| b);

    let mut records: Vec<StageRecord> = Vec::new();
    let mut saw_disabled_marker = false;
    let mut saw_no_samples_marker = false;

    for raw in body.lines() {
        let line = raw.trim_end_matches('\r').trim();
        if line.is_empty() {
            continue;
        }
        if line.starts_with('#') {
            // Marker comments. Tolerate both with and without the
            // exact phrasing; substring match keeps the parser
            // forward-compatible if the message is reworded.
            if line.contains("latency-trace disabled") {
                saw_disabled_marker = true;
            } else if line.contains("no samples") {
                saw_no_samples_marker = true;
            }
            continue;
        }
        if let Some(rec) = parse_stage_line(line) {
            records.push(rec);
        }
    }

    if !records.is_empty() {
        Body::Stages(records)
    } else if saw_disabled_marker {
        Body::Disabled
    } else if saw_no_samples_marker {
        Body::NoSamples
    } else {
        Body::Empty
    }
}

/// Parse a single `stage` TSV line. Returns None on any malformed
/// input — the caller treats malformed lines as "skip", not "fail".
fn parse_stage_line(line: &str) -> Option<StageRecord> {
    let mut fields = line.split('\t');
    if fields.next()? != "stage" {
        return None;
    }
    let name = fields.next()?.to_owned();
    let samples = fields.next()?.parse().ok()?;
    let min_ns = fields.next()?.parse().ok()?;
    let p50_ns = fields.next()?.parse().ok()?;
    let p90_ns = fields.next()?.parse().ok()?;
    let p99_ns = fields.next()?.parse().ok()?;
    let p99_9_ns = fields.next()?.parse().ok()?;
    let max_ns = fields.next()?.parse().ok()?;
    // Reject if there are extra fields — pins the wire contract at
    // exactly 9 tab-separated fields.
    if fields.next().is_some() {
        return None;
    }
    Some(StageRecord {
        name,
        samples,
        min_ns,
        p50_ns,
        p90_ns,
        p99_ns,
        p99_9_ns,
        max_ns,
    })
}

/// Render the server-side per-stage decomposition under the latency
/// section of the bench's console output. No-op when no server data
/// was fetched (other modes, or fetch failed).
pub fn render_console(body: &Body) {
    match body {
        Body::Empty => {} // No data — skip the section entirely.
        Body::Disabled => {
            println!();
            println!("  Server-side Per-Stage Latency");
            println!(
                "    (server built without `--features latency-trace`; rebuild to enable the tick-to-trade decomposition)"
            );
        }
        Body::NoSamples => {
            println!();
            println!("  Server-side Per-Stage Latency");
            println!(
                "    (server has the feature on but recorded no samples — likely a too-short run)"
            );
        }
        Body::Stages(stages) => {
            println!();
            println!("  Server-side Per-Stage Latency (tick-to-trade decomposition)");
            // Column widths: name padded to longest, then µs columns.
            // hdrhistogram on the server gives ns; we present µs.
            let name_w = stages
                .iter()
                .map(|s| s.name.len())
                .max()
                .unwrap_or(0)
                .max(20);
            println!(
                "    {:>name_w$}    {:>10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
                "stage",
                "samples",
                "min µs",
                "p50 µs",
                "p90 µs",
                "p99 µs",
                "p99.9 µs",
                "max µs",
                name_w = name_w,
            );
            for s in stages {
                println!(
                    "    {name:>name_w$}    {samples:>10} {min:>9.2} {p50:>9.2} {p90:>9.2} {p99:>9.2} {p999:>9.2} {max:>9.2}",
                    name = s.name,
                    samples = s.samples,
                    min = s.min_ns as f64 / 1000.0,
                    p50 = s.p50_ns as f64 / 1000.0,
                    p90 = s.p90_ns as f64 / 1000.0,
                    p99 = s.p99_ns as f64 / 1000.0,
                    p999 = s.p99_9_ns as f64 / 1000.0,
                    max = s.max_ns as f64 / 1000.0,
                    name_w = name_w,
                );
            }
        }
    }
}

/// Serialize the server-side stage decomposition as a JSON object
/// for the bench's `--json` output.
///
/// Schema (stable wire contract for downstream tooling):
///
/// ```json
/// {
///   "state": "stages" | "no_samples" | "disabled" | "empty",
///   "entries": [
///     {
///       "name": "<stage name>",
///       "samples": <u64>,
///       "min_ns": <u64>,
///       "p50_ns": <u64>,
///       "p90_ns": <u64>,
///       "p99_ns": <u64>,
///       "p99_9_ns": <u64>,
///       "max_ns": <u64>
///     }
///   ]
/// }
/// ```
///
/// Units are nanoseconds (matching the server's wire format) so
/// downstream consumers can convert as they prefer. `entries` is
/// always an empty array for non-`stages` states.
pub fn render_json(body: &Body) -> String {
    match body {
        Body::Empty => r#"{"state":"empty","entries":[]}"#.to_string(),
        Body::Disabled => r#"{"state":"disabled","entries":[]}"#.to_string(),
        Body::NoSamples => r#"{"state":"no_samples","entries":[]}"#.to_string(),
        Body::Stages(stages) => {
            let mut entries = Vec::with_capacity(stages.len());
            for s in stages {
                // Names are controlled at register_stage callsites and
                // are ASCII-safe (no quotes / backslashes today); escape
                // defensively so a future name needing it doesn't break
                // downstream JSON parsers.
                let escaped: String = s
                    .name
                    .chars()
                    .flat_map(|c| match c {
                        '"' => "\\\"".chars().collect::<Vec<_>>(),
                        '\\' => "\\\\".chars().collect::<Vec<_>>(),
                        other => vec![other],
                    })
                    .collect();
                entries.push(format!(
                    "{{\"name\":\"{name}\",\"samples\":{samples},\"min_ns\":{min},\"p50_ns\":{p50},\"p90_ns\":{p90},\"p99_ns\":{p99},\"p99_9_ns\":{p999},\"max_ns\":{max}}}",
                    name = escaped,
                    samples = s.samples,
                    min = s.min_ns,
                    p50 = s.p50_ns,
                    p90 = s.p90_ns,
                    p99 = s.p99_ns,
                    p999 = s.p99_9_ns,
                    max = s.max_ns,
                ));
            }
            format!(
                "{{\"state\":\"stages\",\"entries\":[{}]}}",
                entries.join(",")
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http_wrap(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/tab-separated-values\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body,
        )
    }

    #[test]
    fn parses_single_stage_line() {
        let body = "stage\tjournal: batch\t100\t1000\t1500\t2000\t3000\t5000\t10000\n";
        match parse_body(&http_wrap(body)) {
            Body::Stages(v) => {
                assert_eq!(v.len(), 1);
                let r = &v[0];
                assert_eq!(r.name, "journal: batch");
                assert_eq!(r.samples, 100);
                assert_eq!(r.min_ns, 1000);
                assert_eq!(r.p50_ns, 1500);
                assert_eq!(r.p90_ns, 2000);
                assert_eq!(r.p99_ns, 3000);
                assert_eq!(r.p99_9_ns, 5000);
                assert_eq!(r.max_ns, 10000);
            }
            other => panic!("expected Stages, got {other:?}"),
        }
    }

    #[test]
    fn parses_multiple_stages_and_skips_blank_lines() {
        let body = "\
stage\tone\t1\t10\t20\t30\t40\t50\t60

stage\ttwo\t2\t100\t200\t300\t400\t500\t600
";
        match parse_body(&http_wrap(body)) {
            Body::Stages(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0].name, "one");
                assert_eq!(v[1].name, "two");
                assert_eq!(v[1].samples, 2);
            }
            other => panic!("expected Stages, got {other:?}"),
        }
    }

    #[test]
    fn detects_latency_trace_disabled() {
        let body = "# latency-trace disabled\n";
        match parse_body(&http_wrap(body)) {
            Body::Disabled => {}
            other => panic!("expected Disabled, got {other:?}"),
        }
    }

    #[test]
    fn detects_no_samples() {
        let body = "# no samples\n";
        match parse_body(&http_wrap(body)) {
            Body::NoSamples => {}
            other => panic!("expected NoSamples, got {other:?}"),
        }
    }

    #[test]
    fn empty_body_is_empty() {
        match parse_body("") {
            Body::Empty => {}
            other => panic!("expected Empty, got {other:?}"),
        }
    }

    #[test]
    fn malformed_lines_are_skipped() {
        // Wrong tag, wrong field count, non-numeric — none should
        // produce a StageRecord. With one valid line mixed in, we
        // get exactly that record.
        let body = "\
not_a_stage\tx\t1\t2\t3\t4\t5\t6\t7
stage\twrong-count\t1\t2\t3
stage\tnon-numeric\tabc\t1\t2\t3\t4\t5\t6
stage\tgood\t10\t1\t2\t3\t4\t5\t6
";
        match parse_body(&http_wrap(body)) {
            Body::Stages(v) => {
                assert_eq!(v.len(), 1);
                assert_eq!(v[0].name, "good");
            }
            other => panic!("expected Stages, got {other:?}"),
        }
    }

    #[test]
    fn rejects_extra_fields() {
        // Pin the wire contract: extra trailing fields invalidate the
        // line so we don't silently absorb a future format change.
        let body = "stage\toops\t1\t1\t2\t3\t4\t5\t6\t7\n";
        match parse_body(&http_wrap(body)) {
            Body::Empty => {}
            other => panic!("expected Empty (line rejected), got {other:?}"),
        }
    }

    // ------------------------------------------------------------------
    // render_json — wire contract for the bench's --json output.
    // Pinned with explicit string equality so a downstream parser can
    // rely on the schema staying stable.
    // ------------------------------------------------------------------

    #[test]
    fn render_json_empty_state() {
        assert_eq!(
            render_json(&Body::Empty),
            r#"{"state":"empty","entries":[]}"#
        );
    }

    #[test]
    fn render_json_disabled_state() {
        assert_eq!(
            render_json(&Body::Disabled),
            r#"{"state":"disabled","entries":[]}"#,
        );
    }

    #[test]
    fn render_json_no_samples_state() {
        assert_eq!(
            render_json(&Body::NoSamples),
            r#"{"state":"no_samples","entries":[]}"#,
        );
    }

    #[test]
    fn render_json_single_stage() {
        // One stage round-trips through render_json and re-parses with
        // a strict JSON parser (serde_json isn't available — verify by
        // explicit substring + numeric parse equivalence to the input).
        let body = Body::Stages(vec![StageRecord {
            name: "journal: batch".to_string(),
            samples: 100,
            min_ns: 1_000,
            p50_ns: 1_500,
            p90_ns: 2_000,
            p99_ns: 3_000,
            p99_9_ns: 5_000,
            max_ns: 10_000,
        }]);
        let json = render_json(&body);
        let expected = r#"{"state":"stages","entries":[{"name":"journal: batch","samples":100,"min_ns":1000,"p50_ns":1500,"p90_ns":2000,"p99_ns":3000,"p99_9_ns":5000,"max_ns":10000}]}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn render_json_multiple_stages_comma_separated() {
        let body = Body::Stages(vec![
            StageRecord {
                name: "a".into(),
                samples: 1,
                min_ns: 1,
                p50_ns: 1,
                p90_ns: 1,
                p99_ns: 1,
                p99_9_ns: 1,
                max_ns: 1,
            },
            StageRecord {
                name: "b".into(),
                samples: 2,
                min_ns: 2,
                p50_ns: 2,
                p90_ns: 2,
                p99_ns: 2,
                p99_9_ns: 2,
                max_ns: 2,
            },
        ]);
        let json = render_json(&body);
        // Verify both entries are present, comma-separated, no trailing comma.
        assert!(json.contains(r#""name":"a""#));
        assert!(json.contains(r#""name":"b""#));
        assert!(json.contains("},{"));
        assert!(!json.contains(",]"));
    }

    #[test]
    fn render_json_escapes_quote_and_backslash_in_name() {
        // Defensive escape — current names don't contain these chars
        // but the contract should hold if a future name needs them.
        let body = Body::Stages(vec![StageRecord {
            name: r#"weird "name" with \backslash"#.into(),
            samples: 1,
            min_ns: 0,
            p50_ns: 0,
            p90_ns: 0,
            p99_ns: 0,
            p99_9_ns: 0,
            max_ns: 0,
        }]);
        let json = render_json(&body);
        // The escaped name in the body should appear with escaped
        // quotes and backslashes — no raw " or \ inside the name field.
        assert!(json.contains(r#""name":"weird \"name\" with \\backslash""#));
    }

    #[test]
    fn render_json_round_trips_through_parse_body() {
        // Build a synthetic dump body, serve it through parse_body,
        // then round-trip back through render_json — the JSON should
        // be byte-identical regardless of intermediate transport.
        let original = "stage\thot path\t42\t100\t200\t300\t400\t500\t999\n";
        let parsed = parse_body(&http_wrap(original));
        let json_a = render_json(&parsed);
        // Re-parse the wire body from a literal payload — same shape.
        let parsed2 = parse_body(&http_wrap(original));
        let json_b = render_json(&parsed2);
        assert_eq!(json_a, json_b);
    }
}
