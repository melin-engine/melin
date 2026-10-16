//! The event publisher sees an event only once the event holds the copies
//! the ack policy demands, the same moment its client is answered.
//!
//! It used to read the output ring straight behind the matching stage, so
//! it broadcast a report the moment the event was applied: before the
//! journal's fsync, before any replica held it. A crash or a failover in
//! between unwound an event its subscribers had already acted on (finding
//! 9 of `docs/internal/determinism-audit-2026-09.md`). It is now chained
//! behind the response stage, and reads only what the durability gate has
//! released.
//!
//! A primary (counter app, `disk+ram` ack policy) with one scripted
//! replica, which receives the client's write and does not ack it: neither
//! the client nor the publisher may see it. The replica then acks, and
//! both do. A second write is never acked, and the node, its response
//! stage and publisher both held on it, must still stop.
//!
//! Kernel TCP only: a DPDK node never runs the event publisher (see "DPDK
//! never runs the event publisher" in
//! `docs/internal/transport-divergences-2026-10.md`), so under the DPDK
//! build this binary holds no test.
#![cfg(not(feature = "dpdk"))]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{
    Counter, CounterQuery, CounterReport, KIND_RESP_ACK, RequestDecoder, ResponseEncoder,
    increment_request,
};
use ed25519_dalek::SigningKey;
use melin_app::auth::AuthorizedKeys;
use melin_client::{Connection, Frame};
use melin_pipeline::ring::Consumer;
use melin_pipeline::wait::WaitStrategy;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::{JournalStagingMode, ServerConfig};
use melin_transport_core::pipeline::OutputSlot;
use melin_transport_core::test_ports::free_addr;

#[path = "common/raw_replica.rs"]
mod raw_replica;
use raw_replica::RawReplica;

#[path = "common/health.rs"]
mod health;
use health::{gauge, wait_for_gauge, wait_for_gauge_where};

/// Port range for `free_addr`, shared with the other cluster tests: every
/// other range below the ephemeral floor is taken. Sharing is safe only
/// because this binary is in nextest's `cluster-serial` group with them.
const PORT_BASE: u16 = 10_000;

/// How long anything the test waits on may take: generous, for a slow and
/// shared CI runner, and bounded, so a hang fails here with a message.
const LIMIT: Duration = Duration::from_secs(60);

/// How long to wait for a frame the primary owes the scripted replica.
const FRAME_LIMIT: Duration = Duration::from_secs(10);

/// How long the publisher is given to read an event it must not see. It
/// polls the ring continuously, so a slot it could read would be read
/// within microseconds; this only has to outlast a descheduled thread.
const SETTLE: Duration = Duration::from_millis(500);

/// The wire sequence of every slot the publisher was given, in order. A
/// static because the runtime takes the publisher as a function pointer,
/// which carries no state of its own; one test in this binary uses it.
static PUBLISHED: Mutex<Vec<u64>> = Mutex::new(Vec::new());

fn published() -> Vec<u64> {
    PUBLISHED
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

/// The event publisher under test: records the wire sequence of each slot
/// it is given, where a real one would broadcast the slot's reports
/// stamped with it. Observes `shutdown` on every pass, slot or no slot, as
/// a publisher must: while the ack policy holds no new event back, none
/// comes.
fn record_wire_seqs(
    mut consumer: Consumer<OutputSlot<CounterReport, CounterQuery>>,
    _bind: SocketAddr,
    _keys: Arc<AuthorizedKeys>,
    shutdown: &AtomicBool,
    wait: WaitStrategy,
) {
    let mut waiter = wait.waiter();
    while !shutdown.load(Ordering::Relaxed) {
        let slots = consumer.read_contiguous(64);
        if slots.is_empty() {
            waiter.idle();
            continue;
        }
        waiter.reset();
        PUBLISHED
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(slots.iter().map(|slot| slot.wire_seq));
        consumer.commit();
    }
}

#[test]
fn the_publisher_sees_an_event_only_once_the_ack_policy_holds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let primary_key = SigningKey::from_bytes(&[0x91; 32]);
    let scripted_key = SigningKey::from_bytes(&[0x92; 32]);
    let client_key = SigningKey::from_bytes(&[0x93; 32]);
    let b64 = |k: &SigningKey| {
        base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
    };
    let keys_file = tmp.path().join("authorized_keys");
    std::fs::write(
        &keys_file,
        format!(
            "replication {} primary\nreplication {} scripted\noperator {} client\n",
            b64(&primary_key),
            b64(&scripted_key),
            b64(&client_key)
        ),
    )
    .expect("write authorized_keys");
    let key_path = tmp.path().join("primary.key");
    std::fs::write(&key_path, primary_key.to_bytes()).expect("write node key");

    let primary_addrs = melin_test_node::addrs(0, || free_addr(PORT_BASE));
    let health = free_addr(PORT_BASE);
    let config = ServerConfig {
        journal: tmp.path().join("primary.journal"),
        authorized_keys: keys_file,
        replication_key: Some(key_path),
        replication_bind: Some(primary_addrs.replication()),
        // An ack needs the replica to hold the event.
        ack_policy: AckPolicy::DiskAndRam,
        health_bind: Some(health),
        // The publisher runs only with an address to serve on; this one
        // binds nothing.
        event_bind: Some(free_addr(PORT_BASE)),
        // Unpinned, and therefore yielding: the node shares this process
        // with the test's client and scripted replica.
        cores: PipelineCores::unpinned(),
        // No ticks and no snapshots: the client's write is the only event
        // the publisher can be given.
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        journal_staging_mode: JournalStagingMode::Allocate,
        ..melin_test_node::config()
    };
    let primary = melin_test_node::start_at_with_publisher::<Counter>(
        &primary_addrs,
        config,
        StartupEvents::none(),
        (),
        RequestDecoder,
        ResponseEncoder,
        record_wire_seqs,
    );

    // Up, so its replication endpoint is listening; then a replica, so it
    // takes writes.
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let mut replica = RawReplica::join(primary_addrs.replication(), &scripted_key, FRAME_LIMIT);
    wait_for_gauge(health, "melin_replicas_connected", 1);

    let deadline = Instant::now() + LIMIT;
    let mut client =
        Connection::connect_by(primary.addr(), &client_key, deadline).expect("client connects");
    client
        .set_read_timeout(LIMIT)
        .expect("set the client's read timeout");

    let processed = gauge(health, "melin_events_processed").expect("the events processed");
    let head = gauge(health, "melin_journal_sequence").expect("the primary's sequence");
    client.send(&increment_request(1)).expect("send the write");

    // The primary applies the write, so its report is on the output ring,
    // and journals it; the replica receives it, and acks nothing.
    wait_for_gauge_where(health, "melin_events_processed", "counted the write", |v| {
        v > processed
    });
    let written = wait_for_gauge_where(
        health,
        "melin_journal_sequence",
        "journaled the write",
        |v| v > head,
    );
    let taken = replica.read_through(written);
    std::thread::sleep(SETTLE);
    let before_ack = published();

    // Acked: the policy holds, the client is answered, and the publisher
    // is given the event.
    replica.ack(taken);
    let reply = match client.next_frame() {
        Ok(Frame::Response(body)) => Ok(body.to_vec()),
        Ok(other) => Err(format!("{other:?}")),
        Err(e) => Err(e.to_string()),
    };
    let mut after_ack = published();
    while after_ack.is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
        after_ack = published();
    }

    // A second write the replica never acks: the response stage and the
    // publisher both wait on it, and the node must still stop.
    let processed = gauge(health, "melin_events_processed").expect("the events processed");
    client
        .send(&increment_request(1))
        .expect("send the second write");
    wait_for_gauge_where(
        health,
        "melin_events_processed",
        "counted the second write",
        |v| v > processed,
    );
    std::thread::sleep(SETTLE);

    drop(client);
    drop(replica);
    primary.stop();
    let after_stop = published();

    assert!(
        before_ack.is_empty(),
        "the publisher was given wire sequences {before_ack:?} before the replica the ack \
         policy needs held them"
    );
    let reply = reply.expect("the write's reply once the replica acked");
    assert_eq!(reply[0], KIND_RESP_ACK, "the write is acked");
    assert!(
        !after_ack.is_empty(),
        "the publisher was never given the write"
    );
    assert!(
        after_ack.iter().all(|&seq| seq == written),
        "the publisher was given {after_ack:?}, not the write's wire sequence {written}"
    );
    assert_eq!(
        after_stop, after_ack,
        "the publisher was given the second write, which no replica held"
    );
}
