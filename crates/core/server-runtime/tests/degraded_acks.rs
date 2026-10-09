//! End-to-end contract for degraded acks: a primary that loses its last
//! replica, at load, answers what it holds once its own disk holds it,
//! and says so.
//!
//! Under each policy that needs a replica (`ram`, `disk+ram`,
//! `two-disks`), a primary and one replica (counter app) take pipelined
//! writes on two connections. The replication link runs through a relay
//! the test controls: it goes silent first, so the writes in flight
//! cannot be confirmed, then breaks for good, so the primary halts. Then:
//!
//! - the writes in flight are answered once the grace period has passed,
//!   ending in `BatchEndDegraded`, never before it and never in full;
//! - later writes are refused (`ReplicaDisconnected`), in a plain
//!   `BatchEnd`, and keep their place behind the held writes;
//! - a query answers, marked degraded, since the state it reads holds
//!   writes no replica confirmed;
//! - a replica that returns within the grace period confirms everything
//!   in full, and nothing is marked;
//! - with degraded acks switched off, nothing held is answered until the
//!   replica returns, as before degraded acks existed.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).
//! The relay is a kernel socket on `melin_test_node::local_ip`, which the
//! nodes reach on either transport.

mod halt_cluster;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use counter_server::{
    GET_VALUE_REQUEST, KIND_RESP_ACK, KIND_RESP_REJECTED, KIND_RESP_VALUE, increment_request,
};
use melin_client::{Ack, Error, Frame};
use melin_server_runtime::ack_policy::AckPolicy;
use melin_test_node::Node;

use halt_cluster::{Fixture, Proxy, addrs, gauge, spawn_node, wait_for_gauge};

/// The grace period of the degraded-ack scenario: short, so the test is.
const GRACE: Duration = Duration::from_secs(1);

/// The node's default grace, which the switched-off scenario waits out
/// to show that nothing is released after it either.
const DEFAULT_GRACE: Duration = Duration::from_secs(2);

/// Long enough that a replica reconnecting through the restored relay
/// (its first retry comes a second after it loses the link) and catching
/// up is well inside it.
const LONG_GRACE: Duration = Duration::from_secs(60);

/// A client's patience: past every wait these tests make, so a reply is
/// only ever missing because the node held it.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// One reply as a client saw it.
#[derive(Debug, Clone, Copy)]
struct Reply {
    /// The counter response's kind.
    kind: u8,
    /// What its batch end said backs it.
    ack: Ack,
    at: Instant,
}

/// Increments pipelined on a connection of its own until `stop`: a burst,
/// then every reply to it. Returns the replies in order, and the error
/// that ended the stream, if one did.
fn writer(
    fx: &Fixture,
    node: &Node,
    stop: Arc<AtomicBool>,
) -> JoinHandle<(Vec<Reply>, Option<String>)> {
    const BURST: usize = 16;
    let mut conn = fx.client(node);
    conn.set_read_timeout(READ_TIMEOUT)
        .expect("set the read timeout");
    std::thread::spawn(move || {
        let mut replies = Vec::new();
        while !stop.load(Ordering::Relaxed) {
            for _ in 0..BURST {
                if let Err(e) = conn.send(&increment_request(1)) {
                    return (replies, Some(e.to_string()));
                }
            }
            let mut kind = None;
            let mut ends = 0;
            while ends < BURST {
                match conn.next_frame() {
                    Ok(Frame::Response(body)) => kind = body.first().copied(),
                    Ok(Frame::BatchEnd(ack)) => {
                        let Some(kind) = kind.take() else {
                            return (replies, Some("a batch with no response".into()));
                        };
                        replies.push(Reply {
                            kind,
                            ack,
                            at: Instant::now(),
                        });
                        ends += 1;
                    }
                    Ok(other) => return (replies, Some(format!("unexpected {other:?}"))),
                    Err(e) => return (replies, Some(e.to_string())),
                }
            }
        }
        (replies, None)
    })
}

/// One request on its own connection, answered as `Reply`, or the error
/// the connection gave up on.
fn ask(fx: &Fixture, node: &Node, body: &[u8], patience: Duration) -> Result<Reply, Error> {
    let mut conn = fx.client(node);
    conn.set_read_timeout(patience)
        .expect("set the read timeout");
    let batch = conn.request_batch(body)?;
    let [frame] = &batch.frames[..] else {
        panic!("one frame expected, got {:?}", batch.frames);
    };
    Ok(Reply {
        kind: frame[0],
        ack: batch.ack,
        at: Instant::now(),
    })
}

/// What the scenario does once the primary has lost its replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    /// Nothing comes back: degraded acks after the grace period.
    Degraded,
    /// The replica reconnects well within a long grace period.
    ReturnsWithinGrace,
    /// Degraded acks switched off; the replica returns after a while.
    SwitchedOff,
}

fn run(policy: AckPolicy, scenario: Scenario) {
    let fx = Fixture::new(0xA1);
    let primary_addrs = addrs(0);
    let mut primary_config = fx.primary_config(&primary_addrs, policy);
    match scenario {
        Scenario::Degraded => primary_config.degraded_ack_grace_ms = GRACE.as_millis() as u64,
        Scenario::ReturnsWithinGrace => {
            primary_config.degraded_ack_grace_ms = LONG_GRACE.as_millis() as u64;
        }
        Scenario::SwitchedOff => primary_config.no_degraded_acks = true,
    }
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node(&primary_addrs, primary_config);

    // The replica reaches its primary through the relay.
    let proxy = Proxy::start(primary_addrs.replication());
    let mut replica_config = fx.replica_config("replica", &primary_addrs, policy);
    replica_config.replica_of = Some(proxy.addr());
    let replica = spawn_node(&addrs(1), replica_config);
    wait_for_gauge(health, "melin_replicas_connected", 1);

    // --- At load: two connections of pipelined writes. ---
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..2)
        .map(|_| writer(&fx, &primary, Arc::clone(&stop)))
        .collect();
    std::thread::sleep(Duration::from_millis(300));

    // --- The link goes silent: the writes in flight stay unconfirmed. ---
    proxy.freeze();
    std::thread::sleep(Duration::from_millis(200));

    // --- Then it breaks: the primary halts. ---
    proxy.cut();
    let cut_at = Instant::now();
    wait_for_gauge(health, "melin_replicas_connected", 0);

    let restore_at = match scenario {
        Scenario::Degraded => {
            // A later write is refused, behind the held writes, in a
            // plain BatchEnd.
            let refused = ask(&fx, &primary, &increment_request(1), READ_TIMEOUT)
                .expect("the refusal is answered");
            assert_eq!(refused.kind, KIND_RESP_REJECTED);
            assert_eq!(refused.ack, Ack::Policy, "a refusal is never marked");

            let query = ask(&fx, &primary, &GET_VALUE_REQUEST, READ_TIMEOUT)
                .expect("the query is answered");
            assert_eq!(query.kind, KIND_RESP_VALUE);
            assert_eq!(
                query.ack,
                Ack::PrimaryOnly,
                "the state holds writes no replica confirmed"
            );
            assert!(
                query.at >= cut_at + GRACE,
                "a degraded reply before the grace period ran out"
            );
            Instant::now()
        }
        Scenario::ReturnsWithinGrace => {
            // Refused while the replica is gone, and answered — behind the
            // held writes — once it is back.
            let mut refused = fx.client(&primary);
            refused
                .set_read_timeout(READ_TIMEOUT)
                .expect("set the read timeout");
            // The writers are all waiting on held replies by now, so
            // nothing else is refused meanwhile.
            let refused_before = gauge(health, "melin_writes_refused_total").expect("health is up");
            refused
                .send(&increment_request(1))
                .expect("send the write the halt refuses");
            // The refusal is decided on arrival: wait for the reader to
            // count it before the replica may come back.
            wait_for_gauge(health, "melin_writes_refused_total", refused_before + 1);
            let restore_at = Instant::now();
            proxy.restore();
            wait_for_gauge(health, "melin_replicas_connected", 1);
            match refused.next_frame() {
                Ok(Frame::Response(body)) => assert_eq!(body[0], KIND_RESP_REJECTED),
                other => panic!("expected the refusal, got {other:?}"),
            }
            assert_eq!(
                refused.next_frame().expect("the refusal's batch end"),
                Frame::BatchEnd(Ack::Policy)
            );
            let query = ask(&fx, &primary, &GET_VALUE_REQUEST, READ_TIMEOUT)
                .expect("the query is answered");
            assert_eq!(query.ack, Ack::Policy, "confirmed in full");
            restore_at
        }
        Scenario::SwitchedOff => {
            // Past the grace degraded acks would have used, the refusal
            // and the query still wait behind the held writes.
            let silence = DEFAULT_GRACE + Duration::from_secs(1);
            for body in [&increment_request(1)[..], &GET_VALUE_REQUEST] {
                match ask(&fx, &primary, body, silence) {
                    Err(Error::NoReply { .. }) => {}
                    other => panic!("answered while halted with the switch off: {other:?}"),
                }
            }
            let restore_at = Instant::now();
            proxy.restore();
            wait_for_gauge(health, "melin_replicas_connected", 1);
            let query = ask(&fx, &primary, &GET_VALUE_REQUEST, READ_TIMEOUT)
                .expect("the query is answered once the replica is back");
            assert_eq!(query.ack, Ack::Policy);
            restore_at
        }
    };

    stop.store(true, Ordering::Relaxed);
    let mut degraded = 0;
    for writer in writers {
        let (replies, error) = writer.join().expect("writer thread");
        assert_eq!(error, None, "a writer's stream broke after {replies:?}");
        degraded += check_writer(&replies, scenario, cut_at, restore_at);
    }

    let counted = gauge(health, "melin_degraded_acks_total").expect("health is up");
    match scenario {
        Scenario::Degraded => {
            assert!(degraded > 0, "no write was in flight when the link broke");
            // The writers' degraded acks, and the query's.
            assert_eq!(counted, degraded + 1, "melin_degraded_acks_total");
        }
        Scenario::ReturnsWithinGrace | Scenario::SwitchedOff => {
            assert_eq!(counted, 0, "melin_degraded_acks_total");
        }
    }

    replica.stop();
    primary.stop();
}

/// Check one writer's replies against the scenario, in order; returns how
/// many were degraded acks. `restore_at` is when the relay let the
/// replica back in (the end of the test where it never was).
fn check_writer(
    replies: &[Reply],
    scenario: Scenario,
    cut_at: Instant,
    restore_at: Instant,
) -> u64 {
    let mut degraded = 0;
    let mut refused_yet = false;
    for (i, reply) in replies.iter().enumerate() {
        let context = || format!("reply {i} of {replies:?}");
        match (reply.kind, reply.ack) {
            (KIND_RESP_ACK, Ack::Policy) => {
                assert!(
                    scenario != Scenario::Degraded || (degraded == 0 && !refused_yet),
                    "a full ack after the halt: {}",
                    context()
                );
            }
            (KIND_RESP_ACK, Ack::PrimaryOnly) => {
                assert_eq!(
                    scenario,
                    Scenario::Degraded,
                    "a degraded ack: {}",
                    context()
                );
                assert!(
                    !refused_yet,
                    "a held write answered after a refusal: {}",
                    context()
                );
                assert!(
                    reply.at >= cut_at + GRACE,
                    "a degraded ack before the grace period ran out: {}",
                    context()
                );
                degraded += 1;
            }
            (KIND_RESP_REJECTED, Ack::Policy) => refused_yet = true,
            (KIND_RESP_REJECTED, Ack::PrimaryOnly) => {
                panic!("a refusal marked degraded: {}", context())
            }
            (kind, _) => panic!("unexpected reply kind {kind:#04x}: {}", context()),
        }
        if scenario == Scenario::SwitchedOff {
            // Held, and not answered, from shortly after the cut (replies
            // confirmed before the link went silent may still be on their
            // way) until the replica was let back in.
            let silent = cut_at + Duration::from_millis(500)..restore_at;
            assert!(
                !silent.contains(&reply.at),
                "answered while halted with the switch off: {}",
                context()
            );
        }
    }
    degraded
}

#[test]
fn ram_degraded_acks_after_the_grace_period() {
    run(AckPolicy::Ram, Scenario::Degraded);
}

#[test]
fn disk_and_ram_degraded_acks_after_the_grace_period() {
    run(AckPolicy::DiskAndRam, Scenario::Degraded);
}

#[test]
fn two_disks_degraded_acks_after_the_grace_period() {
    run(AckPolicy::TwoDisks, Scenario::Degraded);
}

#[test]
fn ram_a_replica_back_within_the_grace_confirms_in_full() {
    run(AckPolicy::Ram, Scenario::ReturnsWithinGrace);
}

#[test]
fn disk_and_ram_a_replica_back_within_the_grace_confirms_in_full() {
    run(AckPolicy::DiskAndRam, Scenario::ReturnsWithinGrace);
}

#[test]
fn two_disks_a_replica_back_within_the_grace_confirms_in_full() {
    run(AckPolicy::TwoDisks, Scenario::ReturnsWithinGrace);
}

#[test]
fn ram_switched_off_holds_everything_until_the_replica_returns() {
    run(AckPolicy::Ram, Scenario::SwitchedOff);
}

#[test]
fn disk_and_ram_switched_off_holds_everything_until_the_replica_returns() {
    run(AckPolicy::DiskAndRam, Scenario::SwitchedOff);
}

#[test]
fn two_disks_switched_off_holds_everything_until_the_replica_returns() {
    run(AckPolicy::TwoDisks, Scenario::SwitchedOff);
}
