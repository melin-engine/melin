//! End-to-end contract for the promotion check on the genesis: a
//! replica promoted before it holds its lineage's whole genesis refuses,
//! and one that holds it is promoted — judged on the genesis length the
//! primary recorded in the lineage, never on the genesis the replica is
//! configured with.
//!
//! Two cases, each with one replica (counter app, `disk` ack policy)
//! promoted by the operator's `PROMOTE` over the admin endpoint:
//!
//! - The protected case. A brand-new cluster whose primary dies while
//!   its first replica is still copying the genesis entry by entry. The
//!   primary is scripted, so it can die exactly two entries into a
//!   three-entry genesis; the replica is configured with no genesis at
//!   all. It must refuse the promotion and exit with the reason.
//! - A replica configured with a larger genesis than its primary's, which
//!   holds the primary's whole genesis, against a real primary. It must
//!   be promoted and serve the primary's state.
//!
//! Nodes are started through `melin-test-node`: on kernel TCP by default,
//! on DPDK with this crate's `dpdk` feature, under
//! `scripts/dpdk/netns-runner.sh` (see
//! `docs/internal/dpdk-transparent-tests.md`). The scripted primary is a
//! kernel socket of the test's own, on the address the nodes reach the
//! test at.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::{Duration, Instant};

use base64::Engine;
use counter_server::{Counter, CounterEvent, RequestDecoder, ResponseEncoder};
use ed25519_dalek::{Signer, SigningKey};
use melin_journal::{JournalEvent, JournalReader};
use melin_server_runtime::StartupEvents;
use melin_server_runtime::ack_policy::AckPolicy;
use melin_server_runtime::layout::PipelineCores;
use melin_server_runtime::server::ServerConfig;
use melin_test_node::{Addrs, Node};
use melin_transport_core::pipeline::InputSlot;
use melin_transport_core::replication::protocol::{
    MAX_CONTROL_FRAME, ReplicaMessage, decode_replica_message, encode_auth_ok, encode_challenge,
    encode_stream_start, read_frame,
};
use melin_transport_core::replication_wire::encode_input_batch;
use melin_transport_core::test_ports::free_addr;
use melin_wire_protocol::control_codec::{TAG_CHALLENGE, TAG_CHALLENGE_RESPONSE, TAG_SERVER_READY};

/// Port range for `free_addr`, shared with the other cluster binaries:
/// safe because they are all in nextest's `cluster-serial` group.
const PORT_BASE: u16 = 10_000;

/// Node `slot`'s addresses (`melin_test_node::addrs`), from this file's
/// port range on kernel TCP.
fn addrs(slot: usize) -> Addrs {
    melin_test_node::addrs(slot, || free_addr(PORT_BASE))
}

fn spawn_node(addrs: &Addrs, config: ServerConfig, startup: StartupEvents<CounterEvent>) -> Node {
    melin_test_node::start_at::<Counter>(
        addrs,
        config,
        startup,
        (),
        RequestDecoder,
        ResponseEncoder,
    )
}

fn genesis(amounts: &[u64]) -> StartupEvents<CounterEvent> {
    StartupEvents {
        genesis: amounts
            .iter()
            .map(|&amount| CounterEvent::Increment { amount })
            .collect(),
        on_primary: Vec::new(),
    }
}

/// Keys and configuration shared by a test's nodes.
struct Cluster {
    tmp: tempfile::TempDir,
    operator_key: SigningKey,
    auth_path: std::path::PathBuf,
}

impl Cluster {
    fn new() -> Self {
        let tmp = tempfile::tempdir().expect("tempdir");
        let primary_key = SigningKey::from_bytes(&[0x81; 32]);
        let replica_key = SigningKey::from_bytes(&[0x82; 32]);
        let operator_key = SigningKey::from_bytes(&[0x83; 32]);
        let b64 = |k: &SigningKey| {
            base64::engine::general_purpose::STANDARD.encode(k.verifying_key().to_bytes())
        };
        let auth_path = tmp.path().join("authorized_keys");
        std::fs::write(
            &auth_path,
            format!(
                "replication {} primary\nreplication {} replica\noperator {} operator\n",
                b64(&primary_key),
                b64(&replica_key),
                b64(&operator_key)
            ),
        )
        .expect("write authorized_keys");
        std::fs::write(tmp.path().join("primary.key"), primary_key.to_bytes())
            .expect("write primary key");
        std::fs::write(tmp.path().join("replica.key"), replica_key.to_bytes())
            .expect("write replica key");
        Self {
            tmp,
            operator_key,
            auth_path,
        }
    }

    fn config(&self, name: &str) -> ServerConfig {
        ServerConfig {
            journal: self.tmp.path().join(format!("{name}.journal")),
            authorized_keys: self.auth_path.clone(),
            ack_policy: AckPolicy::Disk,
            no_mlock: true,
            // Unpinned, and therefore yielding: the nodes share this
            // process with the test's own clients.
            cores: PipelineCores::unpinned(),
            tick_interval_ms: 0,
            snapshot_interval_ms: 0,
            // Off: the default port would collide between the nodes.
            health_bind: None,
            admin_bind: Some(free_addr(PORT_BASE)),
            replication_key: Some(self.tmp.path().join(format!("{name}.key"))),
            ..ServerConfig::default()
        }
    }
}

/// Write one length-prefixed frame.
fn write_frame(stream: &mut TcpStream, payload: &[u8]) {
    stream
        .write_all(&(payload.len() as u32).to_le_bytes())
        .expect("write frame length");
    stream.write_all(payload).expect("write frame payload");
    stream.flush().expect("flush");
}

/// Send `PROMOTE` to the admin endpoint at `addr`, retrying until the
/// endpoint answers. Returns its reply line.
fn promote(addr: SocketAddr, key: &SigningKey) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(reply) = admin_command(addr, key, "PROMOTE") {
            return reply;
        }
        assert!(Instant::now() < deadline, "admin endpoint never answered");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// One admin command over a fresh authenticated connection; `None` if
/// the endpoint is not up yet.
fn admin_command(addr: SocketAddr, key: &SigningKey, command: &str) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(300)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let read_one = |stream: &mut TcpStream| -> Option<Vec<u8>> {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).ok()?;
        let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
        stream.read_exact(&mut payload).ok()?;
        Some(payload)
    };
    let challenge = read_one(&mut stream)?;
    if challenge.first() != Some(&TAG_CHALLENGE) {
        return None;
    }
    let mut response = vec![TAG_CHALLENGE_RESPONSE];
    response.extend_from_slice(&key.sign(&challenge[1..33]).to_bytes());
    response.extend_from_slice(&key.verifying_key().to_bytes());
    write_frame(&mut stream, &response);
    let ready = read_one(&mut stream)?;
    if ready.first() != Some(&TAG_SERVER_READY) {
        return None;
    }
    stream.write_all(command.as_bytes()).ok()?;
    stream.write_all(b"\n").ok()?;
    stream.flush().ok()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).ok()?;
    Some(line.trim_end().to_owned())
}

/// The counter's value, read by a client of the node at `addr` once it
/// serves.
fn value_at(addr: SocketAddr, key: &SigningKey) -> u64 {
    use counter_server::{GET_VALUE_REQUEST, KIND_RESP_VALUE};
    use melin_client::Connection;

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = Connection::connect_by(addr, key, deadline).expect("client connects");
    let reply = conn
        .request_one(&GET_VALUE_REQUEST)
        .expect("value request answered");
    assert_eq!(reply[0], KIND_RESP_VALUE);
    u64::from_le_bytes(reply[1..9].try_into().expect("8 bytes"))
}

/// How many entries the journal at `path` holds; 0 while it does not
/// exist yet.
fn journaled_entries(path: &std::path::Path) -> usize {
    let Ok(mut reader) = JournalReader::<CounterEvent>::open(path) else {
        return 0;
    };
    let mut entries = 0;
    while let Ok(Some(_)) = reader.next_entry() {
        entries += 1;
    }
    entries
}

/// The protected case: the primary dies two entries into a three-entry
/// genesis, and the replica — configured with no genesis at all — learned
/// the genesis length from it, so it refuses the promotion.
#[test]
fn a_replica_configured_without_genesis_refuses_promotion_mid_genesis() {
    let cluster = Cluster::new();

    // A scripted primary: a new cluster's, whose journal records a
    // genesis of three entries.
    let primary =
        TcpListener::bind((melin_test_node::local_ip(), 0)).expect("bind replication listener");
    let mut replica_config = cluster.config("replica");
    replica_config.replica_of = Some(primary.local_addr().expect("addr"));
    let replica_admin = replica_config.admin_bind.expect("set above");
    let replica_journal = replica_config.journal.clone();
    let replica = spawn_node(&addrs(0), replica_config, StartupEvents::none());
    let replica_client = replica.addr();

    let (mut stream, _) = primary.accept().expect("the replica connects");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    // Authentication: the scripted primary takes the replica's word.
    let mut buf = Vec::new();
    encode_challenge(&[0x5A; 32], &mut buf);
    stream.write_all(&buf).expect("challenge");
    read_frame(&mut stream, MAX_CONTROL_FRAME).expect("challenge response");
    buf.clear();
    encode_auth_ok(&mut buf);
    stream.write_all(&buf).expect("auth ok");
    let handshake = read_frame(&mut stream, MAX_CONTROL_FRAME).expect("handshake");
    match decode_replica_message(&handshake).expect("decode handshake") {
        ReplicaMessage::Handshake(h) => assert_eq!(h.last_sequence, 0, "a fresh replica"),
        other => panic!("expected a handshake, got {other:?}"),
    }
    buf.clear();
    encode_stream_start(
        0,
        1,
        [0x3D; 32],
        Some(3),
        0,
        AckPolicy::Disk.as_u8(),
        &mut buf,
    );
    stream.write_all(&buf).expect("StreamStart");
    // The first two genesis entries.
    let slots: Vec<InputSlot<CounterEvent>> = [1_000, 2_000]
        .iter()
        .zip(1u64..)
        .map(|(&amount, sequence)| InputSlot {
            connection_id: 0,
            key_hash: 0,
            sequence,
            timestamp_ns: 1,
            event: JournalEvent::App(CounterEvent::Increment { amount }),
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        })
        .collect();
    buf.clear();
    encode_input_batch(&slots, &mut buf).expect("encode InputBatch");
    stream.write_all(&buf).expect("InputBatch");
    loop {
        let frame = read_frame(&mut stream, MAX_CONTROL_FRAME).expect("ack");
        if let ReplicaMessage::Ack(ack) = decode_replica_message(&frame).expect("decode ack")
            && ack.acked_sequence >= 2
        {
            break;
        }
    }
    // The primary dies.
    drop(stream);
    drop(primary);

    promote(replica_admin, &cluster.operator_key);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !replica.is_finished() {
        assert!(
            Instant::now() < deadline,
            "the replica neither refused the promotion nor exited"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let err = replica.join().expect_err("the promotion must be refused");
    assert!(err.contains("refusing promotion"), "{err}");
    assert!(err.contains("genesis takes 3"), "{err}");
    assert_eq!(
        journaled_entries(&replica_journal),
        2,
        "the partial copy is left as it was"
    );
    assert!(
        TcpStream::connect_timeout(&replica_client, Duration::from_millis(300)).is_err(),
        "a refused promotion serves no client"
    );
}

/// A replica configured with a larger genesis than its primary's holds
/// the primary's whole genesis: it is promoted, and serves the primary's
/// state — its own configured genesis plays no part. On DPDK it serves
/// as a DPDK primary, at its own address.
#[test]
fn a_replica_configured_with_a_larger_genesis_is_promoted() {
    const GENESIS: u64 = 1_000_000;
    let cluster = Cluster::new();

    let primary_addrs = addrs(0);
    let mut primary_config = cluster.config("primary");
    primary_config.replication_bind = Some(primary_addrs.replication());
    let primary = spawn_node(&primary_addrs, primary_config, genesis(&[GENESIS]));

    let mut replica_config = cluster.config("replica");
    replica_config.replica_of = Some(primary_addrs.replication());
    let replica_admin = replica_config.admin_bind.expect("set above");
    let replica_journal = replica_config.journal.clone();
    let replica = spawn_node(&addrs(1), replica_config, genesis(&[1, 2, 4]));

    // The primary's whole genesis — one entry — is on the replica's disk.
    let deadline = Instant::now() + Duration::from_secs(60);
    while journaled_entries(&replica_journal) < 1 {
        assert!(
            !primary.is_finished(),
            "the primary exited: {:?}",
            primary.join()
        );
        assert!(
            !replica.is_finished(),
            "the replica exited: {:?}",
            replica.join()
        );
        assert!(
            Instant::now() < deadline,
            "the replica never copied the genesis"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    primary.stop();

    promote(replica_admin, &cluster.operator_key);
    assert_eq!(
        value_at(replica.addr(), &cluster.operator_key),
        GENESIS,
        "the promoted replica serves the primary's genesis, not its own"
    );
    replica.stop();
}
