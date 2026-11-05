//! Test-only capture of `tracing` output, for a test that asserts what was
//! logged and at which level.
//!
//! The subscriber is installed for one thread only
//! (`tracing::subscriber::with_default` / `set_default`), so tests running
//! beside it neither see nor disturb its log; a current-thread async
//! runtime's spawned tasks run on that thread too. Not
//! `melin_test_node::logs`, which shows a whole process's log in a failing
//! test's report and is built on this crate, nor `stall_guards`' sink,
//! which is another crate's integration test.

use std::sync::Arc;
use std::sync::Mutex;

use tracing::Level;
use tracing::Subscriber;

/// A log sink shared between a subscriber's writer and the test reading it.
/// A `Mutex<Vec<u8>>` rather than a channel: the test reads the whole log,
/// any number of times, and contention is a test's handful of lines.
#[derive(Clone, Default)]
pub struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            // A test that panicked while writing has failed already; keep
            // what it wrote readable for the report.
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Logs {
    /// A subscriber writing every event at `max_level` or more severe into
    /// this sink, one line each, without colour codes: `<LEVEL> <target>:
    /// <message> <field>=<value> ...` after a timestamp.
    ///
    /// The timestamp is the time since the subscriber was made, read from
    /// the monotonic clock, not the wall clock the default timer reads: Miri
    /// runs this crate's tests isolated from the host, which refuses a
    /// wall-clock read, and an elapsed time orders a test's lines as well.
    pub fn subscriber(&self, max_level: Level) -> impl Subscriber + Send + Sync + 'static {
        let writer = self.clone();
        tracing_subscriber::fmt()
            .with_timer(tracing_subscriber::fmt::time::Uptime::default())
            .with_ansi(false)
            .with_max_level(max_level)
            .with_writer(move || writer.clone())
            .finish()
    }

    /// The captured lines carrying `message`.
    pub fn lines(&self, message: &str) -> Vec<String> {
        let log = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        String::from_utf8_lossy(&log)
            .lines()
            .filter(|line| line.contains(message))
            .map(str::to_owned)
            .collect()
    }
}

/// The level a captured line was logged at: the word after its timestamp.
pub fn level(line: &str) -> Level {
    line.split_whitespace()
        .nth(1)
        .and_then(|word| word.parse().ok())
        .unwrap_or_else(|| panic!("no level in {line:?}"))
}

/// The value of `field=` on a captured line, up to the next space.
pub fn field<'a>(line: &'a str, field: &str) -> &'a str {
    let start = line
        .find(&format!(" {field}="))
        .unwrap_or_else(|| panic!("no {field}= in {line:?}"))
        + field.len()
        + 2;
    line[start..]
        .split_whitespace()
        .next()
        .unwrap_or_else(|| panic!("empty {field}= in {line:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn captures_a_threads_events_at_their_level_with_their_fields() {
        let logs = Logs::default();
        tracing::subscriber::with_default(logs.subscriber(Level::INFO), || {
            tracing::warn!(peer = 7, "something degraded");
            tracing::debug!("below the level");
        });
        let lines = logs.lines("something degraded");
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(level(&lines[0]), Level::WARN);
        assert_eq!(field(&lines[0], "peer"), "7");
        assert!(logs.lines("below the level").is_empty());
    }
}
