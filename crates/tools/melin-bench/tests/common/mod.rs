//! What the binary's tests share: the nodes they load, the binary run
//! against them, and its results read back.

// Each test binary uses its own subset.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use melin_client::{SigningKey, key};
use melin_journal::{JournalEvent, JournalReader};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;

pub fn bench_key() -> SigningKey {
    SigningKey::from_bytes(&[0x5A; 32])
}

/// A standalone node's config, its journal and `authorized_keys` in
/// `dir`, the bench's key authorized in `role`.
pub fn config(dir: &Path, journal: &str, role: &str) -> ServerConfig {
    let auth_path = dir.join("authorized_keys");
    std::fs::write(
        &auth_path,
        key::authorized_keys_line(role, &bench_key().verifying_key(), "bench"),
    )
    .expect("write authorized_keys");
    ServerConfig {
        journal: dir.join(journal),
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: AckPolicy::Disk,
        // Unpinned and yielding: the node shares the host with the
        // binary's spinning loop.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..melin_test_node::config()
    }
}

/// An echo node, its journal in `dir`, the bench's key in `role`.
pub fn start_echo(dir: &Path, role: &str) -> melin_test_node::Node {
    melin_test_node::start::<echo_server::Echo>(
        config(dir, "echo.journal", role),
        StartupEvents::none(),
        (),
        echo_server::RequestDecoder,
        echo_server::ResponseEncoder,
    )
}

/// The bench's key, written into `dir` for the binary to read.
pub fn key_file(dir: &Path) -> PathBuf {
    let path = dir.join("bench.key");
    std::fs::write(&path, bench_key().to_bytes()).expect("write key");
    path
}

/// Run the binary with `args`, its `--key` the bench's.
pub fn run(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_melin-bench"))
        .args(["--key", key_file(dir).to_str().expect("utf-8 path")])
        .args(args)
        .output()
        .expect("spawn melin-bench")
}

/// Load a node for a moment, three clients × four in flight, with `args`
/// saying which node and how; the results' JSON, after checking the run
/// succeeded.
pub fn load(dir: &Path, args: &[&str]) -> String {
    let json = dir.join("results.json");
    let mut all = vec![
        "--json",
        json.to_str().expect("utf-8 path"),
        "--clients",
        "3",
        "--window",
        "4",
        "--warmup-duration",
        "100ms",
        "--duration",
        "300ms",
        "--cooldown-duration",
        "50ms",
    ];
    all.extend_from_slice(args);
    let output = run(dir, &all);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("throughput:"), "{stdout}");
    let json = std::fs::read_to_string(json).expect("read results");
    check_results(&json);
    json
}

/// The value of a top-level integer field of the results.
pub fn field(json: &str, name: &str) -> u64 {
    let key = format!("\"{name}\":");
    let start = json
        .find(&key)
        .unwrap_or_else(|| panic!("{name} in {json}"))
        + key.len();
    let digits: String = json[start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().expect("an integer")
}

/// What every run's results hold.
fn check_results(json: &str) {
    assert!(field(json, "measured_requests") > 0, "{json}");
    assert!(
        field(json, "completed_requests") >= field(json, "measured_requests"),
        "{json}"
    );
    for key in [
        "\"throughput_ops\":",
        "\"p99_us\":",
        "\"p99.9_us\":",
        "\"max_us\":",
    ] {
        assert!(json.contains(key), "{key} in {json}");
    }
}

/// The echoes in `dir`'s echo journal, each checked to be `size` bytes.
pub fn journaled_echoes(dir: &Path, size: usize) -> u64 {
    let mut reader = JournalReader::<echo_server::Payload>::open(&dir.join("echo.journal"))
        .expect("open journal");
    let mut echoes = 0;
    while let Some(entry) = reader.next_entry().expect("read entry") {
        if let JournalEvent::App(payload) = entry.event {
            assert_eq!(payload.as_bytes().len(), size);
            echoes += 1;
        }
    }
    echoes
}
