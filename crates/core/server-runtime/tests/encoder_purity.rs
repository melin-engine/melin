//! The debug-build encoder purity check, end to end: a node whose
//! application encoder skips a body byte stops on the first response it
//! encodes that way, rather than serve it.
//!
//! A standalone counter node, its responses encoded by an encoder whose
//! query responses are pure and whose reports leave one body byte
//! unwritten. A query is answered; the first increment's report trips the
//! check, which panics the response stage, and the node fails.
//!
//! Debug builds only, as the check is. The release guarantee the check
//! stands beside, that a skipped byte goes out as zero and never as a byte
//! of an earlier response, is pinned by the unit tests in
//! `response_frame.rs`, which run in every build. An end-to-end test of it
//! here would panic in debug builds, the only ones CI tests.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`).

#![cfg(debug_assertions)]

use std::sync::Mutex;
use std::time::{Duration, Instant};

use counter_server::{
    Counter, CounterQuery, CounterReport, GET_VALUE_REQUEST, KIND_RESP_ACK, RequestDecoder,
    increment_request,
};
use melin_app::encoder::ResponseEncoder;
use melin_client::SigningKey;
use melin_server_runtime::StartupEvents;

#[path = "common/standalone.rs"]
mod standalone;
use standalone::standalone_config;

const CLIENT_KEY: [u8; 32] = [0xA7; 32];

/// Body length both of [`SkippingEncoder`]'s methods return.
const BODY_LEN: usize = 16;
/// The report body byte [`SkippingEncoder`] leaves unwritten.
const SKIPPED: usize = 1;

/// How long the node has to fail once the report is due.
const FAIL_WITHIN: Duration = Duration::from_secs(10);
/// How long the client waits for the report that must never come: long
/// enough that a report the check failed to stop would arrive in it.
const REPORT_WITHIN: Duration = Duration::from_secs(2);

/// The counter's response types, encoded so that a report leaves body
/// byte [`SKIPPED`] unwritten, as an encoder with a reserved or padding
/// field might: a query response fills its whole body with `0xFF`; a
/// report writes its kind at byte 0 and `0x11` from byte 2 on.
///
/// A deliberate twin of the `SkippingEncoder` in `response_frame.rs`'s unit
/// tests: an integration test cannot reach that private test module, and
/// this one must encode the counter's report and query types to run a node.
struct SkippingEncoder;

impl ResponseEncoder for SkippingEncoder {
    type Report = CounterReport;
    type Query = CounterQuery;

    fn encode_report(&self, _: &CounterReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        buf[0] = KIND_RESP_ACK;
        buf[SKIPPED + 1..BODY_LEN].fill(0x11);
        Ok(BODY_LEN)
    }

    fn encode_query(&self, _: &CounterQuery, buf: &mut [u8]) -> Result<usize, &'static str> {
        buf[..BODY_LEN].fill(0xFF);
        Ok(BODY_LEN)
    }
}

/// Every panic message in this process, on whatever thread: a node's
/// response stage panics on a thread of its own, and the node reports
/// only that a stage failed, not why.
static PANICS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Record every panic's message in [`PANICS`], then report it as before.
/// Process-wide: this binary's one test installs it.
fn record_panics() {
    let report = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let payload = info.payload();
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        // A poisoned lock means a panic while recording one; the message
        // is still worth keeping.
        PANICS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(message);
        report(info);
    }));
}

/// Whether a recorded panic message starts with `prefix`.
fn panicked_with(prefix: &str) -> bool {
    PANICS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .any(|message| message.starts_with(prefix))
}

#[test]
fn an_encoder_that_skips_a_body_byte_stops_the_node() {
    record_panics();
    let dir = tempfile::tempdir().expect("tempdir");
    let node = melin_test_node::start::<Counter>(
        standalone_config(dir.path(), &SigningKey::from_bytes(&CLIENT_KEY)),
        StartupEvents::none(),
        (),
        RequestDecoder,
        SkippingEncoder,
    );
    let mut conn = melin_test_node::connect(node.addr(), &SigningKey::from_bytes(&CLIENT_KEY));
    conn.set_read_timeout(FAIL_WITHIN).expect("set timeout");

    // A pure encode passes the check and is answered.
    let query = conn.request_one(&GET_VALUE_REQUEST).expect("the query");
    assert_eq!(query, [0xFF; BODY_LEN], "the query response");

    // The report skips a byte: the response stage panics instead of
    // sending it, so the reply never comes. A kernel TCP node closes the
    // connection as it stops; a DPDK node keeps it open, so the read waits
    // out its timeout, kept short here: the panic is recorded within
    // milliseconds, and the loop below still allows it `FAIL_WITHIN`.
    conn.set_read_timeout(REPORT_WITHIN).expect("set timeout");
    let report = conn.request_one(&increment_request(1));
    assert!(
        report.is_err(),
        "a report with a skipped byte was sent: {report:?}"
    );
    let deadline = Instant::now() + FAIL_WITHIN;
    while !panicked_with("impure response encoder: body byte 1 of 16 differs") {
        assert!(
            Instant::now() < deadline,
            "the purity check did not fire; panics: {:?}",
            PANICS.lock().map(|p| p.clone())
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    drop(conn);

    // A kernel TCP node watches its stages: it stops itself on the dead
    // response stage and returns the failure.
    #[cfg(not(feature = "dpdk"))]
    assert_eq!(node.join(), Err("pipeline failure".to_string()));
    // A DPDK node does not watch its stages, so it serves on until it is
    // stopped, and only its stop reports the failure.
    #[cfg(feature = "dpdk")]
    {
        let stopped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| node.stop()));
        assert!(stopped.is_err(), "the node stopped cleanly");
        assert!(panicked_with("node returned an error"));
    }
}
