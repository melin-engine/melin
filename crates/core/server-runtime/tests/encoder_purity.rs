//! A response body byte the application's encoder skips, end to end: it
//! reaches the client as zero, never as a byte of an earlier response,
//! which may have been another client's.
//!
//! A standalone counter node, its responses encoded by an encoder that
//! leaves one body byte of every report unwritten. Client A queries, then
//! client B increments: B's report is encoded in the buffer A's query
//! response was, and must carry nothing of it.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`).

use std::time::Duration;

use counter_server::{
    Counter, CounterQuery, CounterReport, GET_VALUE_REQUEST, KIND_RESP_ACK, RequestDecoder,
    increment_request,
};
use melin_app::encoder::ResponseEncoder;
use melin_client::{Connection, SigningKey};
use melin_server_runtime::StartupEvents;

#[path = "common/standalone.rs"]
mod standalone;
use standalone::standalone_config;

const CLIENT_KEY: [u8; 32] = [0xA7; 32];

/// Body length both of [`SkippingEncoder`]'s methods return.
const BODY_LEN: usize = 16;
/// The report body byte [`SkippingEncoder`] leaves unwritten.
const SKIPPED: usize = 1;

/// The counter's response types, encoded so that a report leaves body
/// byte [`SKIPPED`] unwritten, as an encoder with a reserved or padding
/// field might: a query response fills its whole body with `0xFF`; a
/// report writes its kind at byte 0 and `0x11` from byte 2 on.
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

fn client(node: &melin_test_node::Node) -> Connection {
    let mut conn = melin_test_node::connect(node.addr(), &SigningKey::from_bytes(&CLIENT_KEY));
    conn.set_read_timeout(Duration::from_secs(30))
        .expect("set timeout");
    conn
}

#[test]
fn a_skipped_body_byte_carries_nothing_of_another_clients_response() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = melin_test_node::start::<Counter>(
        standalone_config(dir.path(), &SigningKey::from_bytes(&CLIENT_KEY)),
        StartupEvents::none(),
        (),
        RequestDecoder,
        SkippingEncoder,
    );
    let mut a = client(&node);
    let mut b = client(&node);

    let query = a.request_one(&GET_VALUE_REQUEST).expect("A's query");
    assert_eq!(query, [0xFF; BODY_LEN], "A's query response");
    let report = b.request_one(&increment_request(1)).expect("B's increment");
    let mut expected = [0x11; BODY_LEN];
    expected[0] = KIND_RESP_ACK;
    expected[SKIPPED] = 0;
    assert_eq!(
        report, expected,
        "B's report carries a byte of A's query response"
    );

    drop((a, b));
    node.stop();
}
