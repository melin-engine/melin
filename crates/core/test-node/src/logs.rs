//! The nodes' log, in a failing test's report.
//!
//! A test's nodes run in its own process, so their tracing output can go
//! to the test harness's capture, which shows it only for a test that
//! fails: what each node did is then beside the panic. One subscriber per
//! process: the first call installs it, and later calls, from other tests
//! in the same process, keep it.

/// Capture the nodes' log at `default_filter` (an `EnvFilter` directive,
/// such as `"debug"`), or at `RUST_LOG` when it is set.
pub fn capture(default_filter: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter));
    install(filter);
}

/// Capture the nodes' log only when `RUST_LOG` is set, at what it says:
/// for a test whose log is too long to keep on every run.
pub fn capture_if_requested() {
    if std::env::var_os("RUST_LOG").is_some() {
        install(tracing_subscriber::EnvFilter::from_default_env());
    }
}

fn install(filter: tracing_subscriber::EnvFilter) {
    // Deliberately ignored: it fails only when a subscriber is installed
    // already, by an earlier test in this process, which is as good.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .with_thread_names(true)
        .try_init();
}
