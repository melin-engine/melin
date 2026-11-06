//! The output check, end to end: replicas compare their output with their
//! primary's, a replica whose output differs stops the whole cluster, and
//! nothing an operator or a fault does to a healthy cluster makes one stop.
//!
//! Detection is driven by a counter whose sizing can change one of its
//! reports (`FaultyCounter`); the cases that must never stop are driven by
//! `TickCountingApp`, whose reports carry its tick-call count, so a copy
//! restarted, rejoined or promoted with one tick call more or fewer than
//! its primary's reports differently from then on, and is caught.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see `docs/internal/dpdk-testing.md`).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use counter_server::{
    Counter, CounterEvent, CounterQuery, CounterReport, GET_VALUE_REQUEST, KIND_RESP_VALUE,
    RequestDecoder, ResponseEncoder, increment_request,
};
use melin_app::{Application, ApplyCtx, QueryCtx, RejectReason};
use melin_client::Connection;
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::probe::{
    admin_reply_when_up, gauge, wait_for_gauge, wait_for_gauge_where, wait_for_snapshot_where,
    wait_released,
};
use melin_test_node::{Addrs, Node};
use melin_transport_core::output_digest::OUTPUT_DIGEST_WINDOW as K;
use melin_transport_core::test_support::{TestEvent, TickCountingApp, TickCountingCodec};

#[path = "common/halt_cluster.rs"]
mod halt_cluster;
use halt_cluster::{Fixture, addrs, endpoint, spawn_node};

#[path = "common/proxy.rs"]
mod proxy;
use proxy::Proxy;

/// How long a stopped node has to return.
const STOP_WITHIN: Duration = Duration::from_secs(20);

/// The ack policy of every cluster here: a write is answered once a
/// replica holds it, so the writes run at the replicas' pace.
const POLICY: AckPolicy = AckPolicy::DiskAndRam;

// ---------------------------------------------------------------------------
// A counter whose output its sizing can change
// ---------------------------------------------------------------------------

/// The counter, with one report changed: the `Ack` of the increment that
/// brings it to the value its sizing names. A node sized with a fault
/// produces output its primary's does not, from the same journal: the
/// stand-in for a build that differs, or a nondeterministic application.
/// It breaks `prefault`'s contract (sizing must never change what the
/// application does) on purpose.
#[derive(Default)]
struct FaultyCounter {
    counter: Counter,
    fault_at: Option<u64>,
}

impl Application for FaultyCounter {
    type Event = CounterEvent;
    type Report = CounterReport;
    type QueryResponse = CounterQuery;
    /// The counter value whose report is changed; `None`: none is.
    type Sizing = Option<u64>;
    const APP_VERSION: u16 = Counter::APP_VERSION;

    fn apply(&mut self, event: Self::Event, ctx: &ApplyCtx, out: &mut Vec<Self::Report>) {
        let from = out.len();
        self.counter.apply(event, ctx, out);
        for report in &mut out[from..] {
            if let CounterReport::Ack { new_value } = report
                && Some(*new_value) == self.fault_at
            {
                *new_value += 1_000_000;
            }
        }
    }

    fn query(&self, event: Self::Event, ctx: &QueryCtx) -> Option<Self::QueryResponse> {
        self.counter.query(event, ctx)
    }

    fn tick(&mut self, now_ns: u64, out: &mut Vec<Self::Report>) {
        self.counter.tick(now_ns, out)
    }

    fn build_reject(event: &Self::Event, reason: RejectReason) -> Self::Report {
        Counter::build_reject(event, reason)
    }

    fn snapshot<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        self.counter.snapshot(w)
    }

    fn restore<R: std::io::Read>(r: &mut R) -> std::io::Result<Self> {
        Counter::restore(r).map(|counter| FaultyCounter {
            counter,
            fault_at: None,
        })
    }

    fn prefault(&mut self, fault: &Self::Sizing) {
        self.fault_at = *fault;
    }
}

/// A `FaultyCounter` node at `addrs`, its report at `fault` changed.
fn spawn_faulty(addrs: &Addrs, config: ServerConfig, fault: Option<u64>) -> Node {
    melin_test_node::start_at::<FaultyCounter>(
        addrs,
        config,
        StartupEvents::none(),
        fault,
        RequestDecoder,
        ResponseEncoder,
    )
}

// ---------------------------------------------------------------------------
// Probes
// ---------------------------------------------------------------------------

/// Wait for `node` to return, and return its error.
fn stopped(node: Node) -> String {
    node.join_within(STOP_WITHIN)
        .expect_err("a stopped node returns an error")
}

/// The positions a detecting node's error names: "between positions A
/// and B".
fn diverged_between(error: &str) -> (u64, u64) {
    let rest = error
        .split_once("between positions ")
        .unwrap_or_else(|| panic!("no positions in {error}"))
        .1;
    let mut words = rest.split_whitespace();
    let from = words.next().and_then(|w| w.parse().ok());
    let and = words.next();
    let through = words.next().and_then(|w| {
        w.trim_end_matches(|c: char| !c.is_ascii_digit())
            .parse()
            .ok()
    });
    match (from, and, through) {
        (Some(from), Some("and"), Some(through)) => (from, through),
        _ => panic!("no positions in {error}"),
    }
}

/// The checks a replica's health endpoint reports, 0 before its first.
fn checks(health: SocketAddr) -> u64 {
    gauge(health, "melin_output_checks_total").unwrap_or(0)
}

/// Wait until the replica behind `health` has made `more` checks past
/// `from`.
fn wait_for_checks(health: SocketAddr, from: u64, more: u64, what: &str) -> u64 {
    wait_for_gauge_where(
        health,
        "melin_output_checks_total",
        &format!("made {more} checks past {from} {what}"),
        |checks| checks >= from + more,
    )
}

/// `count` writes to a `TickCountingApp` node, each answered.
fn add(client: &mut Connection, count: u64) {
    let add = TickCountingCodec::request(TestEvent::Add(1));
    for _ in 0..count {
        client.request_one(&add).expect("a write is served");
    }
}

/// A replica's configuration, with the health endpoint its checks are
/// read on; returns it with that endpoint and the marker its stop leaves.
fn replica_config(
    fx: &Fixture,
    name: &str,
    primary: SocketAddr,
) -> (ServerConfig, SocketAddr, PathBuf) {
    let mut config = fx.replica_config(name, primary, POLICY);
    let health = endpoint();
    config.health_bind = Some(health);
    let marker = config.store().expect("store").divergence_marker();
    (config, health, marker)
}

/// A primary of `TickCountingApp` journaling a tick every few
/// milliseconds, so ticks interleave the writes throughout.
fn ticking_primary_config(fx: &Fixture, addrs: &Addrs) -> ServerConfig {
    let mut config = fx.primary_config(addrs, POLICY);
    config.tick_interval_ms = 5;
    config
}

/// Run `writes` writes through `client`, then wait until every replica in
/// `healths` has made two checks more than it had, so the checks span
/// what came before; and every node in `nodes` is still serving.
fn checks_go_on(client: &mut Connection, writes: u64, healths: &[SocketAddr], nodes: &[&Node]) {
    let before: Vec<u64> = healths.iter().map(|&h| checks(h)).collect();
    add(client, writes);
    for (&health, from) in healths.iter().zip(before) {
        wait_for_checks(health, from, 2, "after the writes");
    }
    for node in nodes {
        assert!(!node.is_finished(), "a healthy node stopped");
    }
}

fn assert_no_marker(markers: &[&PathBuf]) {
    for marker in markers {
        assert!(!marker.exists(), "{}", marker.display());
    }
}

// ---------------------------------------------------------------------------
// Detection
// ---------------------------------------------------------------------------

/// The counter value whose report the faulted replicas change: inside a
/// window, past the first.
const FAULT_AT: u64 = 100;

/// A faulted cluster, stopped: the position of the faulted report on the
/// primary, the errors its nodes returned, and their markers (the
/// primary's first).
struct Stopped {
    fault_position: u64,
    primary_error: String,
    replica_errors: Vec<String>,
    markers: Vec<PathBuf>,
}

/// A primary and two replicas, those `faulted` changing the report at
/// [`FAULT_AT`]; written to until the cluster stops.
fn run_until_stopped(fx: &Fixture, faulted: [bool; 2]) -> Stopped {
    let primary_addrs = addrs(0);
    let primary_config = fx.primary_config(&primary_addrs, POLICY);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary_marker = primary_config.store().expect("store").divergence_marker();
    let primary = spawn_faulty(&primary_addrs, primary_config, None);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let replicas: Vec<(Node, PathBuf)> = ["replica-a", "replica-b"]
        .into_iter()
        .zip(faulted)
        .enumerate()
        .map(|(i, (name, faulted))| {
            let (config, _health, marker) = replica_config(fx, name, primary_addrs.replication());
            let fault = faulted.then_some(FAULT_AT);
            (spawn_faulty(&addrs(i + 1), config, fault), marker)
        })
        .collect();
    wait_for_gauge(health, "melin_replicas_connected", 2);

    let mut client = fx.client(&primary);
    client
        .set_read_timeout(Duration::from_secs(5))
        .expect("set the read timeout");
    for _ in 0..FAULT_AT {
        client
            .request_one(&increment_request(1))
            .expect("served before the fault");
    }
    // Nothing else is journaled (no ticks): the last write is the fault's.
    let fault_position = gauge(health, "melin_journal_sequence").expect("the primary's sequence");
    // The faulted window's check needs a value of the primary's at or past
    // it, which a later batch carries: write on until the cluster stops.
    let mut served_after = 0u64;
    while client.request_one(&increment_request(1)).is_ok() {
        served_after += 1;
        assert!(
            served_after < 64 * K,
            "the cluster served {served_after} writes past the fault"
        );
    }

    let primary_error = stopped(primary);
    let mut markers = vec![primary_marker];
    let replica_errors = replicas
        .into_iter()
        .map(|(node, marker)| {
            markers.push(marker);
            stopped(node)
        })
        .collect();
    Stopped {
        fault_position,
        primary_error,
        replica_errors,
        markers,
    }
}

/// A replica whose output differs from its primary's stops on its own
/// detection, naming a range that holds the differing report; its stop
/// stops the primary, whose `Halt` stops the other replica. Every node
/// leaves its marker, and the faulted one refuses to start again.
#[test]
fn a_replica_whose_output_differs_stops_the_whole_cluster() {
    let fx = Fixture::new(0xE1);
    let stopped = run_until_stopped(&fx, [true, false]);

    let detector = &stopped.replica_errors[0];
    assert!(
        detector.contains("its output diverged from its primary's"),
        "detector: {detector}"
    );
    let (from, through) = diverged_between(detector);
    assert!(
        from <= stopped.fault_position && stopped.fault_position <= through,
        "the fault at {} is outside {from}..={through}",
        stopped.fault_position
    );
    assert!(
        stopped
            .primary_error
            .contains("told by the replica on slot"),
        "primary: {}",
        stopped.primary_error
    );
    let other = &stopped.replica_errors[1];
    assert!(
        other.contains("told by the primary via the halt frame"),
        "other: {other}"
    );
    for marker in &stopped.markers {
        assert!(marker.exists(), "{}", marker.display());
    }

    let (config, _, _) = replica_config(&fx, "replica-a", addrs(0).replication());
    let restarted = spawn_faulty(&addrs(1), config, None)
        .join()
        .expect_err("a restart is refused");
    assert!(
        restarted.contains("do not delete this marker"),
        "{restarted}"
    );
}

/// Both replicas faulted: the first to find it stops the cluster, the
/// other stops on its own detection or on the primary's `Halt`; nothing
/// serves after.
#[test]
fn replicas_that_both_differ_stop_the_cluster_once() {
    let fx = Fixture::new(0xE5);
    let stopped = run_until_stopped(&fx, [true, true]);

    assert!(
        stopped
            .primary_error
            .contains("told by the replica on slot"),
        "primary: {}",
        stopped.primary_error
    );
    let detected = stopped
        .replica_errors
        .iter()
        .filter(|error| error.contains("its output diverged from its primary's"))
        .inspect(|error| {
            let (from, through) = diverged_between(error);
            assert!(from <= stopped.fault_position && stopped.fault_position <= through);
        })
        .count();
    assert!(detected >= 1, "{:?}", stopped.replica_errors);
    for error in &stopped.replica_errors {
        assert!(
            error.contains("its output diverged from its primary's")
                || error.contains("told by the primary via the halt frame"),
            "{error}"
        );
    }
    for marker in &stopped.markers {
        assert!(marker.exists(), "{}", marker.display());
    }
}

/// The documented way out of a stop: a new cluster from the primary's
/// journal, on a build without the fault. The markers are moved aside and
/// kept; the primary recovers its journal; the replicas start on clean
/// data and reseed from it; the cluster serves, and its replicas check.
#[test]
fn a_stopped_cluster_is_replaced_by_a_new_one() {
    let fx = Fixture::new(0xE9);
    let stopped = run_until_stopped(&fx, [true, false]);
    for marker in &stopped.markers {
        let mut kept = marker.clone().into_os_string();
        kept.push(".kept");
        std::fs::rename(marker, &kept).expect("move the marker aside");
    }

    let primary_addrs = addrs(0);
    let primary_config = fx.primary_config(&primary_addrs, POLICY);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_faulty(&primary_addrs, primary_config, None);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let (replicas, healths): (Vec<_>, Vec<_>) = ["fresh-a", "fresh-b"]
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let (config, health, _) = replica_config(&fx, name, primary_addrs.replication());
            (spawn_faulty(&addrs(i + 1), config, None), health)
        })
        .unzip();
    wait_for_gauge(health, "melin_replicas_connected", 2);

    let mut client = fx.client(&primary);
    let reply = client.request_one(&GET_VALUE_REQUEST).expect("a query");
    assert_eq!(reply[0], KIND_RESP_VALUE);
    let value = u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"));
    assert!(
        value >= FAULT_AT,
        "the journal's history is served: {value}"
    );
    let before: Vec<u64> = healths.iter().map(|&h| checks(h)).collect();
    for _ in 0..4 * K {
        client
            .request_one(&increment_request(1))
            .expect("the new cluster serves");
    }
    for (&health, from) in healths.iter().zip(before) {
        wait_for_checks(health, from, 2, "in the new cluster");
    }
    for node in replicas.iter().chain([&primary]) {
        assert!(!node.is_finished(), "a node of the new cluster stopped");
    }
}

// ---------------------------------------------------------------------------
// No false positives
// ---------------------------------------------------------------------------

/// A replica that joins while the cluster runs catches up from the
/// primary's journal, and checks the windows that close after the
/// primary's first digest of the session reaches it.
#[test]
fn a_replica_joining_mid_run_checks_without_stopping() {
    let fx = Fixture::new(0xF1);
    let primary_addrs = addrs(0);
    let primary_config = ticking_primary_config(&fx, &primary_addrs);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary_marker = primary_config.store().expect("store").divergence_marker();
    let primary = spawn_node::<TickCountingApp>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let (config, health_a, marker_a) =
        replica_config(&fx, "replica-a", primary_addrs.replication());
    let a = spawn_node::<TickCountingApp>(&addrs(1), config);
    wait_for_gauge(health, "melin_replicas_connected", 1);
    let mut client = fx.client(&primary);
    add(&mut client, 5 * K + 7);

    let (config, health_b, marker_b) =
        replica_config(&fx, "replica-b", primary_addrs.replication());
    let b = spawn_node::<TickCountingApp>(&addrs(2), config);
    wait_for_gauge(health, "melin_replicas_connected", 2);
    checks_go_on(
        &mut client,
        4 * K,
        &[health_a, health_b],
        &[&primary, &a, &b],
    );
    assert_no_marker(&[&primary_marker, &marker_a, &marker_b]);
}

/// A replica restarted from its own snapshot, the journal tail replayed
/// over it, goes on checking.
#[test]
fn a_replica_restarted_from_its_snapshot_checks_without_stopping() {
    let fx = Fixture::new(0xF5);
    let primary_addrs = addrs(0);
    let primary_config = ticking_primary_config(&fx, &primary_addrs);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node::<TickCountingApp>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let snapshotting = || {
        let (mut config, health, marker) =
            replica_config(&fx, "replica", primary_addrs.replication());
        config.snapshot_interval_ms = 20;
        (config, health, marker)
    };
    let (config, _, marker) = snapshotting();
    let snapshot = config.store().expect("store").snapshot().to_path_buf();
    let replica = spawn_node::<TickCountingApp>(&addrs(1), config);
    wait_for_gauge(health, "melin_replicas_connected", 1);
    let mut client = fx.client(&primary);
    add(&mut client, 3 * K);
    wait_for_snapshot_where::<TickCountingApp>(&snapshot, "was taken", |header| {
        header.sequence > K
    });
    replica.stop();

    let (config, replica_health, _) = snapshotting();
    let replica = spawn_node::<TickCountingApp>(&addrs(1), config);
    wait_for_gauge(health, "melin_replicas_connected", 1);
    checks_go_on(&mut client, 4 * K, &[replica_health], &[&primary, &replica]);
    assert_no_marker(&[&marker]);
}

/// A replica whose link is lost while the cluster runs on reconnects,
/// catches up on what it missed, and goes on checking with the windows
/// it kept.
#[test]
fn a_replica_reconnecting_after_a_long_catch_up_checks_without_stopping() {
    let fx = Fixture::new(0xF9);
    let primary_addrs = addrs(0);
    let primary_config = ticking_primary_config(&fx, &primary_addrs);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node::<TickCountingApp>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let (config, health_a, marker_a) =
        replica_config(&fx, "replica-a", primary_addrs.replication());
    let a = spawn_node::<TickCountingApp>(&addrs(1), config);
    let proxy = Proxy::start(primary_addrs.replication());
    let (config, health_b, marker_b) = replica_config(&fx, "replica-b", proxy.addr());
    let b = spawn_node::<TickCountingApp>(&addrs(2), config);
    wait_for_gauge(health, "melin_replicas_connected", 2);
    let mut client = fx.client(&primary);
    add(&mut client, 2 * K);
    let checked = wait_for_checks(health_b, 0, 1, "before the link was lost");

    proxy.freeze();
    add(&mut client, 16 * K);
    proxy.cut();
    proxy.restore();
    wait_for_checks(health_b, checked, 2, "after the reconnect");
    checks_go_on(
        &mut client,
        4 * K,
        &[health_a, health_b],
        &[&primary, &a, &b],
    );
    assert_no_marker(&[&marker_a, &marker_b]);
}

/// A primary restarted on its own journal, at its address and identity,
/// starts its chain afresh; its replicas reconnect, anchor on it anew, and
/// go on checking.
#[test]
fn a_restarted_primary_is_checked_without_stopping_its_replicas() {
    let fx = Fixture::new(0xFD);
    let primary_addrs = addrs(0);
    // Both starts on the same health endpoint.
    let health = endpoint();
    let primary_config = || {
        let mut config = ticking_primary_config(&fx, &primary_addrs);
        config.health_bind = Some(health);
        config
    };
    let primary = spawn_node::<TickCountingApp>(&primary_addrs, primary_config());
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let (config, health_a, marker_a) =
        replica_config(&fx, "replica-a", primary_addrs.replication());
    let a = spawn_node::<TickCountingApp>(&addrs(1), config);
    let (config, health_b, marker_b) =
        replica_config(&fx, "replica-b", primary_addrs.replication());
    let b = spawn_node::<TickCountingApp>(&addrs(2), config);
    wait_for_gauge(health, "melin_replicas_connected", 2);
    let mut client = fx.client(&primary);
    add(&mut client, 3 * K + 11);
    wait_for_checks(health_a, 0, 1, "before the restart");

    drop(client);
    primary.stop();
    wait_released(&[health]);
    let primary = spawn_node::<TickCountingApp>(&primary_addrs, primary_config());
    wait_for_gauge(health, "melin_replicas_connected", 2);
    let mut client = fx.client(&primary);
    checks_go_on(
        &mut client,
        4 * K,
        &[health_a, health_b],
        &[&primary, &a, &b],
    );
    assert_no_marker(&[&marker_a, &marker_b]);
}

/// A replica promoted after its primary is gone serves on, and a replica
/// of it checks its output against it.
#[test]
fn a_promoted_replica_is_checked_by_its_own_replica() {
    let fx = Fixture::new(0xC1);
    let primary_addrs = addrs(0);
    let primary_config = ticking_primary_config(&fx, &primary_addrs);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node::<TickCountingApp>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let promoted_addrs = addrs(1);
    let (mut config, promoted_health, marker_a) =
        replica_config(&fx, "replica-a", primary_addrs.replication());
    let admin = endpoint();
    config.admin_bind = Some(admin);
    config.replication_bind = Some(promoted_addrs.replication());
    config.tick_interval_ms = 5;
    let a = spawn_node::<TickCountingApp>(&promoted_addrs, config);
    wait_for_gauge(health, "melin_replicas_connected", 1);
    let mut client = fx.client(&primary);
    add(&mut client, 3 * K + 5);
    let head = gauge(health, "melin_journal_sequence").expect("the primary's sequence");
    // The primary journals ticks meanwhile: its head moves on.
    wait_for_gauge_where(
        health,
        "melin_replica_acked_sequence{slot=\"0\"}",
        "caught up",
        |acked| acked >= head,
    );
    drop(client);
    primary.stop();

    admin_reply_when_up(admin, &fx.client_key, "PROMOTE");
    // The new primary's own replica, on the old primary's slot.
    let (config, health_c, marker_c) =
        replica_config(&fx, "replica-c", promoted_addrs.replication());
    let c = spawn_node::<TickCountingApp>(&addrs(0), config);
    wait_for_gauge(promoted_health, "melin_replicas_connected", 1);
    let mut client = fx.client(&a);
    checks_go_on(&mut client, 4 * K, &[health_c], &[&a, &c]);
    assert_no_marker(&[&marker_a, &marker_c]);
}

/// Idle with ticks on, the ticks alone close windows, and the checks keep
/// growing; with ticks off nothing closes one, and none grows until the
/// writes resume.
#[test]
fn idle_checks_follow_the_ticks() {
    let fx = Fixture::new(0xC5);
    let primary_addrs = addrs(0);
    let primary_config = ticking_primary_config(&fx, &primary_addrs);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node::<TickCountingApp>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let (config, replica_health, marker) =
        replica_config(&fx, "replica", primary_addrs.replication());
    let replica = spawn_node::<TickCountingApp>(&addrs(1), config);
    wait_for_gauge(health, "melin_replicas_connected", 1);
    let from = checks(replica_health);
    wait_for_checks(replica_health, from, 3, "on ticks alone");
    assert!(!primary.is_finished() && !replica.is_finished());
    assert_no_marker(&[&marker]);
    drop(replica);
    drop(primary);

    let fx = Fixture::new(0xC9);
    let primary_config = fx.primary_config(&primary_addrs, POLICY);
    wait_released(&[replica_health]);
    let health = primary_config.health_bind.expect("set by the fixture");
    let primary = spawn_node::<TickCountingApp>(&primary_addrs, primary_config);
    wait_for_gauge(health, "melin_replicas_connected", 0);
    let (config, replica_health, _) = replica_config(&fx, "replica", primary_addrs.replication());
    let replica = spawn_node::<TickCountingApp>(&addrs(1), config);
    wait_for_gauge(health, "melin_replicas_connected", 1);
    let mut client = fx.client(&primary);
    checks_go_on(&mut client, 4 * K, &[replica_health], &[&primary, &replica]);
    // Settled: the last value the writes carried has been compared.
    let mut settled = checks(replica_health);
    loop {
        std::thread::sleep(Duration::from_millis(300));
        let now = checks(replica_health);
        if now == settled {
            break;
        }
        settled = now;
    }
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(
        checks(replica_health),
        settled,
        "a check without a tick or a write"
    );
    checks_go_on(&mut client, 4 * K, &[replica_health], &[&primary, &replica]);
}
