//! End-to-end contract for a halted primary: a write it refuses is gone,
//! what it answers it answers promptly, and only an operator lifts it.
//!
//! A primary whose last replica has left refuses client writes. The client
//! is told so, and the refusal has to be the whole truth: the write must
//! not be applied, and it must not be journaled either, or the next replay
//! applies what the live engine refused. That second half used to fail —
//! the refusal was decided after the journal had already recorded the
//! write — and only a restart shows it.
//!
//! A primary and one replica (counter app, `disk` ack policy). One
//! increment is acked while the replica is attached; the replica is then
//! stopped, and a second increment must be refused while a query still
//! answers, and counted on the health endpoint. A replica then attaches
//! again, and the client resends the refused increment, which is taken.
//! The primary is restarted standalone on its own journal, and replay must
//! reach the value the client was told.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-testing.md`).

mod halt_cluster;

use counter_server::{KIND_RESP_ACK, KIND_RESP_REJECTED, increment_request};
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::server::ServerConfig;

use halt_cluster::{
    Fixture, addrs, admin, free, gauge, one_reply, spawn_node, value_of, wait_for_gauge,
    wait_until_streaming,
};

#[test]
fn a_write_refused_while_halted_is_not_replayed() {
    let fx = Fixture::new(0x71);
    let primary_addrs = addrs(0);
    let primary_config = fx.primary_config(&primary_addrs, AckPolicy::Disk);
    let primary_health = primary_config.health_bind.expect("set by the fixture");
    let primary_journal = primary_config.journal.clone();

    let primary = spawn_node(&primary_addrs, primary_config);
    let replica = spawn_node(
        &addrs(1),
        fx.replica_config("replica", &primary_addrs, AckPolicy::Disk),
    );

    // --- Taking writes: the replica is attached, the write is taken. ---
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let mut conn = fx.client(&primary);
    let ack = one_reply(&mut conn, &increment_request(1));
    assert_eq!(ack[0], KIND_RESP_ACK, "the first increment is acked");

    // --- Halted: the replica leaves, the next write is refused. ---
    replica.stop();
    wait_for_gauge(primary_health, "melin_replicas_connected", 0);
    let refused = one_reply(&mut conn, &increment_request(5));
    assert_eq!(
        refused[0], KIND_RESP_REJECTED,
        "a halted primary must refuse the write"
    );
    assert_eq!(
        value_of(&mut conn),
        1,
        "queries still answer, and the refused write is not applied"
    );
    // The reader counts the refusal after committing the receive it came
    // in; the reply can beat it, hence the wait.
    wait_for_gauge(primary_health, "melin_writes_refused_total", 1);

    // --- Taking writes again: a replica attaches, the refused write is resent. ---
    let replica = spawn_node(
        &addrs(2),
        fx.replica_config("replica2", &primary_addrs, AckPolicy::Disk),
    );
    wait_for_gauge(primary_health, "melin_replicas_connected", 1);
    let ack = one_reply(&mut conn, &increment_request(5));
    assert_eq!(ack[0], KIND_RESP_ACK, "the resend is taken");
    assert_eq!(value_of(&mut conn), 6);
    drop(conn);
    replica.stop();
    primary.stop();

    // --- Replay: the journal holds only what the client was told. ---
    let restart_config = ServerConfig {
        journal: primary_journal,
        authorized_keys: fx.auth_path.clone(),
        standalone: true,
        ack_policy: AckPolicy::Disk,
        cores: melin_server_runtime::layout::PipelineCores::unpinned(),
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        ..melin_test_node::config()
    };
    let restarted = spawn_node(&addrs(0), restart_config);
    let mut conn = fx.client(&restarted);
    let replayed = value_of(&mut conn);
    drop(conn);
    restarted.stop();

    assert_eq!(
        replayed, 6,
        "replay applied a write the halted primary refused"
    );
}

/// The halt follows replica loss under every policy, `disk` included, and
/// only an operator's explicit `ACK-POLICY disk`, sent while the node has
/// no replica, lifts it. The lift holds until a replica streams again or
/// the policy goes back to one that needs a replica.
#[test]
fn an_explicit_swap_to_disk_with_no_replica_lifts_the_halt() {
    let fx = Fixture::new(0x81);
    let primary_addrs = addrs(0);
    let mut primary_config = fx.primary_config(&primary_addrs, AckPolicy::Disk);
    let admin_addr = free();
    primary_config.admin_bind = Some(admin_addr);
    let health = primary_config.health_bind.expect("set by the fixture");
    let operator = &fx.client_key;

    let primary = spawn_node(&primary_addrs, primary_config);
    let replica = spawn_node(
        &addrs(1),
        fx.replica_config("replica", &primary_addrs, AckPolicy::Disk),
    );
    // A primary that began the history serves its first client once its
    // first replica is streaming: the connect below waits for that.
    wait_for_gauge(health, "melin_replicas_connected", 1);
    let mut conn = fx.client(&primary);
    let write = |conn: &mut _, amount: u64| one_reply(conn, &increment_request(amount))[0];

    // --- A swap made while a replica is connected latches nothing. ---
    assert_eq!(admin(admin_addr, operator, "ACK-POLICY disk"), "OK");
    assert_eq!(write(&mut conn, 1), KIND_RESP_ACK);
    replica.stop();
    wait_for_gauge(health, "melin_replicas_connected", 0);
    assert_eq!(
        write(&mut conn, 100),
        KIND_RESP_REJECTED,
        "a disk primary that loses its replica halts, whatever was swapped before"
    );
    assert_eq!(gauge(health, "melin_trading_active"), Some(0));

    // --- A swap to disk during the halt lifts it. ---
    assert_eq!(admin(admin_addr, operator, "ACK-POLICY disk"), "OK");
    assert_eq!(
        write(&mut conn, 2),
        KIND_RESP_ACK,
        "the swap lifts the halt"
    );
    assert_eq!(gauge(health, "melin_trading_active"), Some(1));

    // --- A swap back to a policy that needs a replica halts again. ---
    assert_eq!(admin(admin_addr, operator, "ACK-POLICY two-disks"), "OK");
    assert_eq!(write(&mut conn, 100), KIND_RESP_REJECTED);
    assert_eq!(gauge(health, "melin_trading_active"), Some(0));
    assert_eq!(admin(admin_addr, operator, "ACK-POLICY disk"), "OK");
    assert_eq!(write(&mut conn, 4), KIND_RESP_ACK);

    // --- A replica that streams again clears the lift: its departure halts. ---
    let replica = spawn_node(
        &addrs(2),
        fx.replica_config("replica2", &primary_addrs, AckPolicy::Disk),
    );
    wait_until_streaming(health);
    assert_eq!(write(&mut conn, 8), KIND_RESP_ACK);
    replica.stop();
    wait_for_gauge(health, "melin_replicas_connected", 0);
    assert_eq!(
        write(&mut conn, 100),
        KIND_RESP_REJECTED,
        "the replica's return cleared the operator's lift"
    );
    assert_eq!(value_of(&mut conn), 15, "only the acked writes applied");

    drop(conn);
    primary.stop();
}
