//! The binary against real nodes, in process, on kernel TCP: every
//! request it reports answered is in the node's journal, and its results
//! are what the suite reads.

mod common;

use melin_client::key;
use melin_journal::{JournalEvent, JournalReader};
use melin_server_runtime::StartupEvents;

use common::{bench_key, config, field, journaled_echoes, load, run, serving, start_echo};

#[test]
fn every_echo_answered_is_journaled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start_echo(dir.path(), "writer");
    let addr = node.addr().to_string();
    let json = load(
        dir.path(),
        &["--app", "echo", "--size", "48", "--addr", &addr],
    );
    node.stop();
    assert_eq!(
        journaled_echoes(dir.path(), 48),
        field(&json, "completed_requests"),
        "{json}"
    );
}

#[test]
fn every_notarization_answered_is_journaled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = serving(melin_test_node::start::<notary_server::Notary>(
        config(dir.path(), "notary.journal", "submitter"),
        StartupEvents::none(),
        (),
        notary_server::RequestDecoder,
        notary_server::ResponseEncoder,
    ));
    let addr = node.addr().to_string();
    let json = load(dir.path(), &["--app", "notary", "--addr", &addr]);
    node.stop();

    let mut reader =
        JournalReader::<notary_server::NotaryEvent>::open(&dir.path().join("notary.journal"))
            .expect("open journal");
    let mut leaves = 0;
    while let Some(entry) = reader.next_entry().expect("read entry") {
        if let JournalEvent::App(notary_server::NotaryEvent::Notarize { .. }) = entry.event {
            leaves += 1;
        }
    }
    assert_eq!(leaves, field(&json, "completed_requests"), "{json}");
}

#[test]
fn every_echo_answered_across_threads_is_journaled() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start_echo(dir.path(), "writer");
    let addr = node.addr().to_string();
    // Three clients over two threads: one thread drives two of them.
    let json = load(
        dir.path(),
        &["--app", "echo", "--threads", "2", "--addr", &addr],
    );
    node.stop();
    assert_eq!(field(&json, "threads"), 2, "{json}");
    assert_eq!(
        journaled_echoes(dir.path(), 64),
        field(&json, "completed_requests"),
        "{json}"
    );
}

#[test]
fn a_thread_count_the_run_cannot_use_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    for (threads, transport) in [("0", "kernel"), ("4", "kernel"), ("2", "dpdk")] {
        let output = run(
            dir.path(),
            &[
                "--app",
                "echo",
                "--clients",
                "3",
                "--threads",
                threads,
                "--transport",
                transport,
            ],
        );
        assert_eq!(output.status.code(), Some(2));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.starts_with(&format!("error: --threads {threads}")),
            "{stderr}"
        );
    }
}

#[test]
fn a_key_without_the_writing_role_fails_the_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Authorized as a reader: may connect, may not echo.
    let node = start_echo(dir.path(), "reader");
    let addr = node.addr().to_string();
    let output = run(
        dir.path(),
        &[
            "--app",
            "echo",
            "--addr",
            &addr,
            "--warmup-duration",
            "0s",
            "--duration",
            "100ms",
        ],
    );
    node.stop();
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.starts_with("error: "), "{stderr}");
    // The node's refusal, the request dropped unanswered, and not any other
    // failure: a handshake that times out exits 2 with `error: ` as well.
    let refused = melin_client::Error::NoReply {
        timeout: melin_client::DEFAULT_TIMEOUT,
    }
    .to_string();
    assert!(stderr.contains(&refused), "{stderr}");
}

#[test]
fn the_authorized_keys_line_is_the_node_s_format() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = run(dir.path(), &["--print-authorized-key", "writer"]);
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim_end(),
        key::authorized_keys_line("writer", &bench_key().verifying_key(), "melin-bench")
    );
}
