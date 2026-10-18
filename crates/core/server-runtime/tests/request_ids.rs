//! Every request gets exactly one final frame naming it, end to end on a
//! standalone primary: a request the application's decoder refuses is
//! answered with a `Refused` frame carrying its kind and reason, and one
//! it filters with an empty batch, so a pipelining client pairs every
//! reply with its request by ID. Only the accepted requests' replies are
//! pinned to request order: a refused or filtered request may be answered
//! ahead of a write before it that is still held for durability.
//!
//! These used to go unanswered: the node dropped the request, and every
//! later reply on a pipelining connection was paired with the request
//! before it (S1 of `docs/internal/application-api-review-2026-09.md`).
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).

use std::path::Path;
use std::time::Duration;

use counter_server::{
    Counter, CounterEvent, CounterRole, GET_VALUE_REQUEST, KIND_RESP_ACK, KIND_RESP_VALUE,
    RequestDecoder, ResponseEncoder, increment_request,
};
use melin_app::auth::ClientRole;
use melin_app::decoder::Decoded;
use melin_client::{Connection, Frame, RefusalKind, RequestId, SigningKey, key};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;

/// The writer's key, and the reader's.
const WRITER: [u8; 32] = [0xA1; 32];
const READER: [u8; 32] = [0xA2; 32];

/// The body the test's decoder filters: a counter decoder for everything
/// else.
const FILTERED: [u8; 1] = [0x7F];

/// A body the counter's decoder does not know.
const MALFORMED: [u8; 1] = [0x55];

/// The counter's decoder, with [`FILTERED`] filtered: an application
/// message that is not an event, as a subscription request is on a
/// connection that only submits events.
struct FilteringDecoder;

impl melin_app::decoder::RequestDecoder for FilteringDecoder {
    type Event = CounterEvent;
    type Role = CounterRole;

    fn decode(&self, body: &[u8], role: ClientRole<CounterRole>) -> Decoded<CounterEvent> {
        if body == FILTERED {
            return Decoded::Filter;
        }
        RequestDecoder.decode(body, role)
    }
}

/// A standalone counter node on `dir`, with a writer and a reader key.
fn start(dir: &Path) -> melin_test_node::Node {
    let auth_path = dir.join("authorized_keys");
    let line = |role: &str, secret: [u8; 32]| {
        key::authorized_keys_line(role, &SigningKey::from_bytes(&secret).verifying_key(), role)
    };
    std::fs::write(
        &auth_path,
        format!("{}\n{}\n", line("writer", WRITER), line("reader", READER)),
    )
    .expect("write auth keys");
    let config = ServerConfig {
        journal: dir.join("counter.journal"),
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: melin_server_runtime::ack_policy::AckPolicy::Disk,
        // Unpinned, and therefore yielding: see `startup_events.rs`.
        cores: PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..melin_test_node::config()
    };
    melin_test_node::start::<Counter>(
        config,
        StartupEvents {
            genesis: Vec::new(),
            on_primary: Vec::new(),
        },
        (),
        FilteringDecoder,
        ResponseEncoder,
    )
}

/// What a request was answered with: the application frames of its batch
/// and the final frame, rendered so a whole exchange compares at once.
#[derive(Debug, PartialEq, Eq)]
enum Answer {
    /// A batch: its frames, as their first byte (the counter's response
    /// kind) and value.
    Batch(Vec<(u8, u64)>),
    Refused(RefusalKind, String),
    Busy,
}

/// Check that `answers` names every ID in `sent` exactly once, each with
/// the answer `expected` gives at its position, and that the accepted
/// requests (those answered with frames) were answered in request order.
/// A refused or filtered request's answer may come at any point.
fn assert_answered(sent: &[RequestId], answers: &[(RequestId, Answer)], expected: Vec<Answer>) {
    let wanted: Vec<(RequestId, Answer)> = sent.iter().copied().zip(expected).collect();
    fn by_id(list: &[(RequestId, Answer)]) -> Vec<&(RequestId, Answer)> {
        let mut list: Vec<_> = list.iter().collect();
        list.sort_by_key(|(id, _)| *id);
        list
    }
    assert_eq!(
        by_id(answers),
        by_id(&wanted),
        "every request answered once, naming it"
    );
    let accepted = |list: &[(RequestId, Answer)]| -> Vec<RequestId> {
        list.iter()
            .filter(|(_, answer)| matches!(answer, Answer::Batch(frames) if !frames.is_empty()))
            .map(|(id, _)| *id)
            .collect()
    };
    assert_eq!(
        accepted(answers),
        accepted(&wanted),
        "the accepted requests answered in request order"
    );
}

/// Send every body pipelined, then read every answer, pairing each final
/// frame with the request it names. Returns the answers in the order they
/// came, each with its request's ID, and the IDs sent, in order.
fn pipelined(
    conn: &mut Connection,
    bodies: &[&[u8]],
) -> (Vec<RequestId>, Vec<(RequestId, Answer)>) {
    let sent: Vec<RequestId> = bodies
        .iter()
        .map(|body| conn.send(body).expect("send"))
        .collect();
    let mut answers = Vec::new();
    let mut frames = Vec::new();
    while answers.len() < bodies.len() {
        match conn.next_frame().expect("every request is answered") {
            Frame::Response(body) => frames.push((
                body[0],
                u64::from_le_bytes(body[1..9].try_into().expect("an 8-byte value")),
            )),
            Frame::BatchEnd { id, .. } => {
                answers.push((id, Answer::Batch(std::mem::take(&mut frames))));
            }
            Frame::Refused { id, kind, reason } => {
                assert!(frames.is_empty(), "a refusal inside a batch");
                answers.push((id, Answer::Refused(kind, reason.to_string())));
            }
            Frame::ServerBusy { id } => answers.push((id, Answer::Busy)),
        }
    }
    (sent, answers)
}

/// S1: a pipelined client whose requests include a refused and a
/// malformed one, and a filtered one, gets every request answered once,
/// each answer naming its request; nothing refused or filtered is
/// applied; and the connection serves the next request.
#[test]
fn every_pipelined_request_is_answered_and_paired_by_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let node = start(dir.path());

    let mut writer = melin_test_node::connect(node.addr(), &SigningKey::from_bytes(&WRITER));
    writer
        .set_read_timeout(Duration::from_secs(30))
        .expect("set the read timeout");
    let (sent, answers) = pipelined(
        &mut writer,
        &[
            &increment_request(1),
            &MALFORMED,
            &increment_request(2),
            &FILTERED,
            &increment_request(4),
            &GET_VALUE_REQUEST,
        ],
    );
    let malformed = Answer::Refused(RefusalKind::Malformed, "unknown kind".into());
    let expected = vec![
        Answer::Batch(vec![(KIND_RESP_ACK, 1)]),
        malformed,
        Answer::Batch(vec![(KIND_RESP_ACK, 3)]),
        // Filtered: an empty batch.
        Answer::Batch(vec![]),
        Answer::Batch(vec![(KIND_RESP_ACK, 7)]),
        Answer::Batch(vec![(KIND_RESP_VALUE, 7)]),
    ];
    assert_answered(&sent, &answers, expected);

    // A reader's increment is refused as not permitted, with the
    // decoder's reason, among queries that are answered.
    let mut reader = melin_test_node::connect(node.addr(), &SigningKey::from_bytes(&READER));
    reader
        .set_read_timeout(Duration::from_secs(30))
        .expect("set the read timeout");
    let (sent, answers) = pipelined(
        &mut reader,
        &[
            &GET_VALUE_REQUEST,
            &increment_request(100),
            &GET_VALUE_REQUEST,
        ],
    );
    let denied = Answer::Refused(
        RefusalKind::PermissionDenied,
        "incrementing requires a writing role".into(),
    );
    let expected = vec![
        Answer::Batch(vec![(KIND_RESP_VALUE, 7)]),
        denied,
        Answer::Batch(vec![(KIND_RESP_VALUE, 7)]),
    ];
    assert_answered(&sent, &answers, expected);

    // Each connection numbered its own requests from 1, and both still
    // serve: the convenience calls report a refusal as their error.
    assert_eq!(sent[0], RequestId(1));
    let err = reader.request(&MALFORMED).unwrap_err();
    assert!(
        matches!(&err, melin_client::Error::Refused { kind: RefusalKind::Malformed, reason } if reason == "unknown kind"),
        "{err:?}"
    );
    assert_eq!(
        writer.request(&FILTERED).expect("an empty batch"),
        Vec::<Vec<u8>>::new()
    );
    let value = writer.request_one(&GET_VALUE_REQUEST).expect("the query");
    assert_eq!(value[0], KIND_RESP_VALUE);
    assert_eq!(
        u64::from_le_bytes(value[1..9].try_into().expect("8 bytes")),
        7
    );

    drop((writer, reader));
    node.stop();
}
