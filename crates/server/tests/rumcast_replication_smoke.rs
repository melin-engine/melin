//! Smoke test for replication over rumcast. Spawns a primary and a
//! replica in-process (each via `run_rumcast` on its own thread),
//! waits for the replica to auth + handshake + create its journal,
//! verifies the replica's journal contains the same genesis entry as
//! the primary, then tears both down cleanly.
//!
//! Doesn't yet exercise live event flow under replication —  the
//! point is to validate the rumcast wire-format path through
//! Challenge → ChallengeResponse → AuthOk → Handshake → StreamStart →
//! (empty catchup, fresh replica) → Live. The bench suite covers the
//! data-plane behavior; this test is for catching protocol regressions
//! at the auth + handshake layer.
//!
//! Only compiled / run when the `rumcast` feature is enabled. Run
//! with: `cargo test -p melin-server --features rumcast --test
//! rumcast_replication_smoke -- --nocapture`.

#![cfg(feature = "rumcast")]

use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine;
use ed25519_dalek::SigningKey;

use melin_server::rumcast_transport::{RumcastConfig, run_rumcast};
use melin_server::server::ServerConfig;

/// Find an unused UDP port by binding ephemeral and dropping. There's
/// a tiny race between this and the actual `run_rumcast` bind, but in
/// practice the loopback range churns slowly enough that two
/// consecutive `free_udp_port()` calls don't collide.
fn free_udp_port() -> u16 {
    let s = UdpSocket::bind("127.0.0.1:0").unwrap();
    s.local_addr().unwrap().port()
}

fn loopback(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
}

/// Write an authorized_keys file with the supplied (permission, key)
/// pairs. Permissions are the textual labels the server's auth loader
/// recognizes (e.g. "replication", "trader").
fn write_authorized_keys(dir: &std::path::Path, entries: &[(&str, &SigningKey)]) -> PathBuf {
    let path = dir.join("authorized_keys");
    let mut content = String::new();
    for (i, (perm, key)) in entries.iter().enumerate() {
        let pub_b64 =
            base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes());
        content.push_str(&format!("{perm} {pub_b64} rumcast-repl-test-{i}\n"));
    }
    let mut f = std::fs::File::create(&path).expect("create authorized_keys");
    f.write_all(content.as_bytes()).expect("write keys");
    path
}

/// Write a 32-byte Ed25519 signing-key seed to disk in the format
/// `--replication-key` expects (raw bytes, no header).
fn write_replication_key_seed(dir: &std::path::Path, key: &SigningKey) -> PathBuf {
    let path = dir.join("replication.key");
    let mut f = std::fs::File::create(&path).expect("create repl key");
    f.write_all(&key.to_bytes()).expect("write key");
    path
}

#[test]
fn rumcast_replica_handshakes_and_creates_journal() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    let primary_orders_addr = loopback(free_udp_port());
    let primary_repl_addr = loopback(free_udp_port());
    let replica_local_addr = loopback(free_udp_port());

    let primary_tmp = tempfile::tempdir().unwrap();
    let replica_tmp = tempfile::tempdir().unwrap();
    let primary_journal = primary_tmp.path().join("primary.journal");
    let replica_journal = replica_tmp.path().join("replica.journal");

    // Two identities: a replication key (granted "replication"
    // permission) and an unused trader key for variety so the
    // authorized_keys file looks like a real deployment's. The
    // replication key must be on the primary's authorized_keys list
    // and its seed must be readable from the replica's
    // --replication-key path.
    let replica_key = SigningKey::from_bytes(&[0x55; 32]);
    let trader_key = SigningKey::from_bytes(&[0xAB; 32]);
    let primary_authorized_keys = write_authorized_keys(
        primary_tmp.path(),
        &[("replication", &replica_key), ("trader", &trader_key)],
    );
    // The replica also loads an authorized_keys file (used by
    // promotion auth); reuse the same entries so a future promotion
    // path lands cleanly. Phase 4 doesn't trigger promotion.
    let replica_authorized_keys = write_authorized_keys(
        replica_tmp.path(),
        &[("replication", &replica_key), ("trader", &trader_key)],
    );
    let replica_key_path = write_replication_key_seed(replica_tmp.path(), &replica_key);

    // ---- Primary config ----
    let primary_config = ServerConfig {
        bind: primary_orders_addr,
        replication_bind: Some(primary_repl_addr),
        journal: primary_journal.clone(),
        snapshot_path: Some(primary_tmp.path().join("primary.snapshot")),
        accounts: 4,
        instruments: 4,
        authorized_keys: primary_authorized_keys,
        ..ServerConfig::default()
    };

    // ---- Replica config ----
    let replica_config = ServerConfig {
        bind: replica_local_addr,
        replica_of: Some(primary_repl_addr),
        replication_key: Some(replica_key_path),
        journal: replica_journal.clone(),
        snapshot_path: Some(replica_tmp.path().join("replica.snapshot")),
        accounts: 4,
        instruments: 4,
        authorized_keys: replica_authorized_keys,
        ..ServerConfig::default()
    };

    // ---- Spawn primary ----
    let primary_shutdown = Arc::new(AtomicBool::new(false));
    let primary_shutdown_clone = Arc::clone(&primary_shutdown);
    let primary_handle = thread::Builder::new()
        .name("test-rumcast-primary".into())
        .spawn(move || {
            run_rumcast(
                primary_config,
                RumcastConfig {
                    bind: primary_orders_addr,
                },
                primary_shutdown_clone,
            )
            .map_err(|e| e.to_string())
        })
        .unwrap();

    // Give the primary a moment to bind and start the replication
    // listener before the replica races out a Setup.
    thread::sleep(Duration::from_millis(500));

    // ---- Spawn replica ----
    let replica_shutdown = Arc::new(AtomicBool::new(false));
    let replica_shutdown_clone = Arc::clone(&replica_shutdown);
    let replica_handle = thread::Builder::new()
        .name("test-rumcast-replica".into())
        .spawn(move || {
            run_rumcast(
                replica_config,
                RumcastConfig {
                    bind: replica_local_addr,
                },
                replica_shutdown_clone,
            )
            .map_err(|e| e.to_string())
        })
        .unwrap();

    // ---- Wait for handshake + genesis flush ----
    //
    // The replica creates its journal file at handshake time; its
    // length grows as: file header → genesis entry → catch-up batch.
    // Polling for "size matches the primary's genesis byte count"
    // covers the race between `create_new` and the genesis write
    // inside `create_fresh_replica_journal` AND the immediately
    // following catch-up batch that adds the seeded events.
    let primary_genesis_min = melin_journal::codec::FILE_HEADER_SIZE + 24;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut handshake_done = false;
    while Instant::now() < deadline {
        if let Ok(meta) = std::fs::metadata(&replica_journal)
            && (meta.len() as usize) >= primary_genesis_min
        {
            handshake_done = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    assert!(
        handshake_done,
        "replica journal did not reach genesis size within 15s — handshake / catchup likely failed"
    );

    // Compare the primary's and replica's first-entry bytes — they
    // must match exactly because the StreamStart message ships the
    // primary's raw genesis entry and the replica writes it byte-for-
    // byte.
    let primary_bytes = std::fs::read(&primary_journal).expect("read primary journal");
    let replica_bytes = std::fs::read(&replica_journal).expect("read replica journal");

    use melin_journal::codec::FILE_HEADER_SIZE;
    assert!(
        primary_bytes.len() >= FILE_HEADER_SIZE + 24,
        "primary journal too small ({} bytes)",
        primary_bytes.len()
    );
    assert!(
        replica_bytes.len() >= FILE_HEADER_SIZE + 24,
        "replica journal too small ({} bytes)",
        replica_bytes.len()
    );

    // Genesis entry layout: 20-byte header + payload + 4-byte CRC.
    // Length is encoded at offset+2..+4 of the entry header.
    let entry_len_offset = FILE_HEADER_SIZE + 2;
    let primary_entry_len = u16::from_le_bytes([
        primary_bytes[entry_len_offset],
        primary_bytes[entry_len_offset + 1],
    ]) as usize;
    let primary_genesis_total = 20 + primary_entry_len + 4;
    assert!(
        primary_bytes.len() >= FILE_HEADER_SIZE + primary_genesis_total,
        "primary journal truncated at genesis"
    );
    let primary_genesis =
        &primary_bytes[FILE_HEADER_SIZE..FILE_HEADER_SIZE + primary_genesis_total];

    let replica_entry_len = u16::from_le_bytes([
        replica_bytes[entry_len_offset],
        replica_bytes[entry_len_offset + 1],
    ]) as usize;
    assert_eq!(
        primary_entry_len, replica_entry_len,
        "primary/replica genesis entry length mismatch"
    );
    let replica_genesis =
        &replica_bytes[FILE_HEADER_SIZE..FILE_HEADER_SIZE + primary_genesis_total];
    assert_eq!(
        primary_genesis, replica_genesis,
        "primary and replica genesis entries differ — replication wire format broke byte-equality"
    );

    // ---- Tear down ----
    replica_shutdown.store(true, Ordering::Release);
    primary_shutdown.store(true, Ordering::Release);

    // The replica's run_receiver_rumcast is in its outer reconnect
    // loop, which checks `shutdown` between each step. It can take a
    // few hundred ms to exit cleanly during a streaming-loop sleep.
    let replica_join_deadline = Instant::now() + Duration::from_secs(10);
    while !replica_handle.is_finished() && Instant::now() < replica_join_deadline {
        thread::sleep(Duration::from_millis(50));
    }
    let replica_result = replica_handle.join().expect("replica thread panicked");
    if let Err(e) = replica_result {
        // Non-fatal: a clean shutdown sometimes surfaces as a transient
        // error (e.g. a publish racing with shutdown). Log but don't
        // fail the test as long as the journal exists with valid
        // contents — which we already asserted above.
        eprintln!("replica exited with: {e}");
    }

    let primary_join_deadline = Instant::now() + Duration::from_secs(10);
    while !primary_handle.is_finished() && Instant::now() < primary_join_deadline {
        thread::sleep(Duration::from_millis(50));
    }
    let primary_result = primary_handle.join().expect("primary thread panicked");
    if let Err(e) = primary_result {
        eprintln!("primary exited with: {e}");
    }
}
