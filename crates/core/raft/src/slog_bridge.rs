//! Bridge raft-rs's mandatory `slog` logging into `tracing`.
//!
//! raft-rs takes a `slog::Logger` at construction and cannot be built
//! without one. The rest of the codebase logs through `tracing`
//! exclusively, so this drain forwards every raft record — message plus
//! structured key/values flattened into one field — to the equivalent
//! `tracing` event. Nothing else in the workspace should ever touch
//! `slog`.

use slog::{Drain, Level, OwnedKVList, Record};

/// A `slog::Drain` that re-emits records as `tracing` events under the
/// `melin_raft::raft` target.
struct TracingDrain;

/// `slog::Serializer` that flattens key/value pairs into `k=v` segments.
/// Allocates one small `String` per record — fine on the control plane
/// (raft logs at human cadence, not per-order).
struct KvCollector(String);

impl slog::Serializer for KvCollector {
    fn emit_arguments(&mut self, key: slog::Key, val: &std::fmt::Arguments<'_>) -> slog::Result {
        use std::fmt::Write;
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        // Infallible: `write!` into a `String` cannot fail.
        let _ = write!(self.0, "{key}={val}");
        Ok(())
    }
}

impl Drain for TracingDrain {
    type Ok = ();
    type Err = slog::Never;

    fn log(&self, record: &Record<'_>, values: &OwnedKVList) -> Result<(), Self::Err> {
        use slog::KV;

        let mut kvs = KvCollector(String::new());
        // Infallible: the collector's serializer never errors.
        let _ = record.kv().serialize(record, &mut kvs);
        let _ = values.serialize(record, &mut kvs);
        let msg = record.msg();
        let kvs = kvs.0;

        // Level mapping follows the project log policy (`CLAUDE.md`):
        // raft's `Error` records are peer-connectivity and proposal-drop
        // conditions — degraded operation, not server bugs — so they land
        // at `warn`. Only `Critical` (consensus invariant violations)
        // maps to `error`.
        match record.level() {
            Level::Critical => tracing::error!(target: "melin_raft::raft", kvs, "{msg}"),
            Level::Error | Level::Warning => {
                tracing::warn!(target: "melin_raft::raft", kvs, "{msg}")
            }
            Level::Info => tracing::info!(target: "melin_raft::raft", kvs, "{msg}"),
            Level::Debug => tracing::debug!(target: "melin_raft::raft", kvs, "{msg}"),
            Level::Trace => tracing::trace!(target: "melin_raft::raft", kvs, "{msg}"),
        }
        Ok(())
    }
}

/// Build the `slog::Logger` handed to every [`raft::RawNode`], forwarding
/// into `tracing`. `fuse()` is safe: the drain's error type is `Never`.
pub fn tracing_logger() -> slog::Logger {
    slog::Logger::root(TracingDrain.fuse(), slog::o!())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bridge must not panic on records carrying structured KVs —
    /// exercise it end-to-end through a real slog macro call.
    #[test]
    fn forwards_records_without_panicking() {
        let logger = tracing_logger();
        slog::info!(logger, "raft state change"; "term" => 3, "role" => "candidate");
        slog::error!(logger, "peer unreachable"; "peer" => 2);
        slog::debug!(logger, "plain message");
    }
}
