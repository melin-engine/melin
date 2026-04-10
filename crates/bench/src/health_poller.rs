//! Background health poller that periodically scrapes the server's Prometheus
//! `/metrics` endpoint and collects timestamped samples for post-run analysis.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Polling interval between metric scrapes.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Read timeout for the HTTP response. Short because the metrics endpoint
/// is a small, fast response — if it takes longer something is wrong.
const READ_TIMEOUT: Duration = Duration::from_millis(50);

/// A single timestamped snapshot of server health metrics.
#[derive(Debug, Clone)]
pub struct HealthSample {
    /// Seconds elapsed since poller start.
    pub elapsed_secs: f64,
    /// Current authenticated client connections.
    pub active_connections: u64,
    /// Total events processed by the matching engine.
    pub events_processed: u64,
    /// Latest durable journal sequence number.
    pub journal_sequence: u64,
    /// Journal sequence minus replication cursor.
    pub replication_lag: u64,
    /// Items pending in the input disruptor.
    pub input_queue_depth: u64,
    /// Total input ring buffer capacity.
    pub input_queue_capacity: u64,
    /// Whether the pipeline is healthy.
    pub pipeline_healthy: bool,
    /// Whether the engine is accepting orders.
    pub trading_active: bool,
    /// Additional metrics not in the fixed fields. Forward-compatible:
    /// new Prometheus metrics are captured automatically without code
    /// changes. Keys are metric names, values are parsed as f64.
    pub extra: HashMap<String, f64>,
}

/// Background poller that scrapes the Prometheus `/metrics` endpoint at
/// regular intervals and collects timestamped health samples.
pub struct HealthPoller {
    handle: Option<std::thread::JoinHandle<Vec<HealthSample>>>,
    shutdown: Arc<AtomicBool>,
}

impl HealthPoller {
    /// Start polling the given health endpoint address in a background thread.
    pub fn start(addr: SocketAddr) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_flag = Arc::clone(&shutdown);

        let handle = std::thread::Builder::new()
            .name("health-poller".into())
            .spawn(move || poll_loop(addr, shutdown_flag))
            .expect("spawn health-poller thread");

        Self {
            handle: Some(handle),
            shutdown,
        }
    }

    /// Signal the poller to stop and return all collected samples.
    pub fn stop(mut self) -> Vec<HealthSample> {
        self.shutdown.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .expect("poller handle")
            .join()
            .expect("join health-poller thread")
    }
}

/// Main polling loop. Runs until `shutdown` is set.
fn poll_loop(addr: SocketAddr, shutdown: Arc<AtomicBool>) -> Vec<HealthSample> {
    let start = Instant::now();
    let mut samples = Vec::new();

    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(POLL_INTERVAL);

        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        if let Some(sample) = scrape_metrics(addr, start) {
            samples.push(sample);
        }
    }

    samples
}

/// Connect to the health endpoint, send `GET /metrics`, parse the response.
/// Returns `None` on any connection/parse failure (server not ready, shutting down, etc.).
fn scrape_metrics(addr: SocketAddr, start: Instant) -> Option<HealthSample> {
    let mut stream = std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(50)).ok()?;
    stream.set_read_timeout(Some(READ_TIMEOUT)).ok()?;
    stream.set_nodelay(true).ok()?;

    // Send minimal HTTP/1.1 GET request.
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .ok()?;

    // Read the full response. The metrics payload is ~3 KiB with
    // per-replica replication metrics.
    let mut buf = [0u8; 8192];
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
            Err(_) => return None,
        }
    }

    let body = std::str::from_utf8(&buf[..total]).ok()?;

    // Skip HTTP headers — find the blank line separating headers from body.
    let metrics_text = body.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or(body);

    parse_prometheus(metrics_text, start.elapsed().as_secs_f64())
}

/// Minimal Prometheus text format parser. Extracts known metric names,
/// ignores comments and unknown metrics (forward-compatible).
fn parse_prometheus(text: &str, elapsed_secs: f64) -> Option<HealthSample> {
    let mut sample = HealthSample {
        elapsed_secs,
        active_connections: 0,
        events_processed: 0,
        journal_sequence: 0,
        replication_lag: 0,
        input_queue_depth: 0,
        input_queue_capacity: 0,
        pipeline_healthy: false,
        trading_active: false,
        extra: HashMap::new(),
    };

    let mut found_any = false;

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Prometheus text format: `metric_name value` or
        // `metric_name{label="value"} value`
        let (name, value_str) = match line.split_once(' ') {
            Some(pair) => pair,
            None => continue,
        };

        let value_str = value_str.trim();
        found_any = true;
        match name {
            "melin_active_connections" => {
                sample.active_connections = value_str.parse().unwrap_or(0);
            }
            "melin_events_processed" => {
                sample.events_processed = value_str.parse().unwrap_or(0);
            }
            "melin_journal_sequence" => {
                sample.journal_sequence = value_str.parse().unwrap_or(0);
            }
            "melin_replication_lag" => {
                sample.replication_lag = value_str.parse().unwrap_or(0);
            }
            "melin_input_queue_depth" => {
                sample.input_queue_depth = value_str.parse().unwrap_or(0);
            }
            "melin_input_queue_capacity" => {
                sample.input_queue_capacity = value_str.parse().unwrap_or(0);
            }
            "melin_pipeline_healthy" => {
                sample.pipeline_healthy = value_str == "1";
            }
            "melin_trading_active" => {
                sample.trading_active = value_str == "1";
            }
            other => {
                // Capture all other melin_ metrics (including labeled
                // ones like `melin_replica_lag{slot="0"}`). Store the
                // full metric name + labels as the key.
                if let Ok(v) = value_str.parse::<f64>() {
                    sample.extra.insert(other.to_string(), v);
                }
            }
        }
    }

    if found_any { Some(sample) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_prometheus_text() {
        let text = "\
# HELP melin_active_connections Current authenticated client connections.
# TYPE melin_active_connections gauge
melin_active_connections 16
# HELP melin_events_processed Total events processed by the matching engine.
# TYPE melin_events_processed counter
melin_events_processed 50000
# HELP melin_journal_sequence Latest durable journal sequence number.
# TYPE melin_journal_sequence counter
melin_journal_sequence 150000
# HELP melin_replication_lag Journal sequence minus replication cursor.
# TYPE melin_replication_lag gauge
melin_replication_lag 42
# HELP melin_input_queue_depth Items pending in the input disruptor.
# TYPE melin_input_queue_depth gauge
melin_input_queue_depth 128
# HELP melin_input_queue_capacity Total input ring buffer capacity.
# TYPE melin_input_queue_capacity gauge
melin_input_queue_capacity 1048576
# HELP melin_pipeline_healthy Pipeline health status.
# TYPE melin_pipeline_healthy gauge
melin_pipeline_healthy 1
# HELP melin_trading_active Trading status.
# TYPE melin_trading_active gauge
melin_trading_active 1
";
        let sample = parse_prometheus(text, 1.5).expect("should parse");
        assert_eq!(sample.active_connections, 16);
        assert_eq!(sample.events_processed, 50000);
        assert_eq!(sample.journal_sequence, 150000);
        assert_eq!(sample.replication_lag, 42);
        assert_eq!(sample.input_queue_depth, 128);
        assert_eq!(sample.input_queue_capacity, 1048576);
        assert!(sample.pipeline_healthy);
        assert!(sample.trading_active);
        assert!((sample.elapsed_secs - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_unknown_metrics_captured_in_extra() {
        let text = "\
melin_active_connections 5
melin_future_metric 999
melin_trading_active 0
";
        let sample = parse_prometheus(text, 0.0).expect("should parse");
        assert_eq!(sample.active_connections, 5);
        assert!(!sample.trading_active);
        // Unknown metrics are captured in the extra map.
        assert_eq!(sample.extra.get("melin_future_metric"), Some(&999.0));
    }

    #[test]
    fn parse_empty_returns_none() {
        assert!(parse_prometheus("", 0.0).is_none());
        assert!(parse_prometheus("# just comments\n# more comments\n", 0.0).is_none());
    }

    #[test]
    fn parse_malformed_values_default_to_zero() {
        let text = "\
melin_active_connections not_a_number
melin_events_processed 100
";
        let sample = parse_prometheus(text, 0.0).expect("should parse");
        assert_eq!(sample.active_connections, 0);
        assert_eq!(sample.events_processed, 100);
    }

    #[test]
    fn parse_boolean_edge_cases() {
        // "0" → false
        let text = "melin_pipeline_healthy 0\nmelin_trading_active 1\n";
        let sample = parse_prometheus(text, 0.0).expect("should parse");
        assert!(!sample.pipeline_healthy);
        assert!(sample.trading_active);

        // Non-"1" value → false (forward-compatible).
        let text = "melin_pipeline_healthy 2\n";
        let sample = parse_prometheus(text, 0.0).expect("should parse");
        assert!(!sample.pipeline_healthy);
    }

    #[test]
    fn parse_partial_metrics_defaults() {
        // Only one metric present — others should default to 0/false.
        let text = "melin_journal_sequence 42\n";
        let sample = parse_prometheus(text, 2.5).expect("should parse");
        assert_eq!(sample.journal_sequence, 42);
        assert_eq!(sample.active_connections, 0);
        assert_eq!(sample.events_processed, 0);
        assert_eq!(sample.input_queue_depth, 0);
        assert!(!sample.pipeline_healthy);
        assert!(!sample.trading_active);
    }

    #[test]
    fn parse_lines_without_space_skipped() {
        let text = "bare_metric_no_value\nmelin_active_connections 7\n";
        let sample = parse_prometheus(text, 0.0).expect("should parse");
        assert_eq!(sample.active_connections, 7);
    }

    #[test]
    fn parse_trailing_carriage_return() {
        // HTTP responses use \r\n — if the header split leaves \r on body lines,
        // the trim in parse should handle it.
        let text = "melin_active_connections 10\r\nmelin_events_processed 200\r\n";
        let sample = parse_prometheus(text, 0.0).expect("should parse");
        assert_eq!(sample.active_connections, 10);
        assert_eq!(sample.events_processed, 200);
    }
}
