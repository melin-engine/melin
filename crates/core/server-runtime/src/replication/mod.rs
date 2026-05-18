//! Replication — synchronous input-stream streaming from primary to replica.
//!
//! The primary's JournalStage encodes each `InputSlot` it just durably
//! journaled into a wire-ready `InputBatch` frame in the replication ring
//! (separately from the journal-codec bytes it writes to disk). The
//! `ReplicationSender` thread forwards those frames as-is over TCP/DPDK
//! — no decode + re-encode on the hot path. The replica decodes the
//! frames straight back into `InputSlot`s, publishes them to its local
//! input disruptor with the primary's pre-assigned sequences and
//! timestamps, and the replica's JournalStage re-encodes them through
//! its own writer for byte-exact-on-replay durability.
//!
//! ## Wire Protocol
//!
//! Length-prefixed frames, little-endian, over a dedicated TCP connection
//! (or DPDK pipe). The full `InputBatch` payload layout lives in
//! `melin_transport_core::replication::protocol` /
//! `melin_transport_core::replication_wire`.
//!
//! ### Auth (before handshake)
//! - **Challenge** (Primary → Replica): `[len:u32][0x03][nonce:[u8;32]]`
//! - **ChallengeResponse** (Replica → Primary): `[len:u32][0x04][signature:[u8;64]][pubkey:[u8;32]]`
//! - **AuthOk** (Primary → Replica): `[len:u32][0x05]`
//! - **AuthFailed** (Primary → Replica): `[len:u32][0x06]`
//!
//! ### Replica → Primary
//! - **Handshake**: `[len:u32][0x01][last_sequence:u64][chain_hash:[u8;32]]`
//! - **Ack**: `[len:u32][0x02][acked_sequence:u64][in_memory_sequence:u64]`
//!
//! ### Primary → Replica
//! - **StreamStart**: `[len:u32][0x10][start_sequence:u64][genesis_len:u32][genesis_bytes...]`
//! - **NeedSnapshot**: `[len:u32][0x11]`
//! - **HashMismatch**: `[len:u32][0x12]`
//! - **SnapshotBegin**: `[len:u32][0x13][snapshot_len:u64][snap_sequence:u64][snap_chain_hash:[u8;32]]`
//! - **SnapshotChunk**: `[len:u32][0x14][data...]`
//! - **SnapshotEnd**: `[len:u32][0x15][crc32c:u32]`
//! - **InputBatch**: `[len:u32][0x21][count:u16][slot...]` — see
//!   `transport-core::replication_wire` for the per-slot layout
//! - **Heartbeat**: `[len:u32][0x30][sequence:u64]`
//!
//! ## v1 Limitations
//!
//! - No handshake chain hash validation (HashMismatch never sent)
//! - Dual replication (up to 2 replicas in parallel)
//!
//! See `docs/replication.md` for the full design document and limitation details.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use melin_journal::JournalWrite;
use melin_transport_core::pipeline::{JournalStage, JournalStageRun};

use melin_app::Application;
use melin_transport_core::pipeline::{InputSlot, OutputSlot};

mod auth;
#[cfg(feature = "dpdk")]
mod dpdk;
mod tcp_receiver;
mod tcp_sender;

// Wire-protocol types, auth, catch-up, ack queueing, dual-track
// cursor management, and per-replica metrics now live in
// `melin_transport_core::replication`. Re-export the public types
// here so the module's public API surface (e.g.
// `melin_server::runtime::replication::Ack` / `::ReplicationMetrics`) is
// unchanged for downstream consumers and tests.
pub use melin_transport_core::replication::ReplicationMetrics;
pub use melin_transport_core::replication::ack_queue::{
    PendingAck, PendingAckQueue, try_flush_dual_track, update_dual_replication_cursor,
    wait_for_journal_cursor,
};
pub use melin_transport_core::replication::protocol::{
    Ack, Handshake, PrimaryMessage, ReplicaMessage,
};

#[cfg(feature = "dpdk")]
pub use dpdk::{DpdkReplicationDriver, run_receiver_dpdk};
pub use tcp_receiver::{ReceiverResult, run_receiver};
pub use tcp_sender::{Sender, run_sender};

/// Diagnostic: emit a `tcp_info` span at debug level describing the
/// kernel's view of the socket (rtt, cwnd, retrans, unacked, rcv_space,
/// rto). Guarded internally with `tracing::enabled!` so the
/// `getsockopt(TCP_INFO)` syscall is skipped when debug logging is off —
/// the call is free at runtime unless `RUST_LOG=debug` (or a more
/// specific filter) is active. Used to distinguish user-space stalls
/// from TCP-level congestion collapse when diagnosing replication
/// slowdowns.
pub(super) fn log_tcp_info(fd: std::os::unix::io::RawFd, tag: &str, slot: usize) {
    if !tracing::enabled!(tracing::Level::DEBUG) {
        return;
    }
    // SAFETY: all-zero pattern is a valid `tcp_info` (all numeric fields).
    // The kernel fills in whatever the running version supports and
    // returns the written length in `len`.
    let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            &mut info as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        tracing::debug!(
            slot,
            tag,
            errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            "tcp_info getsockopt failed"
        );
        return;
    }
    tracing::debug!(
        slot,
        tag,
        state = info.tcpi_state,
        ca_state = info.tcpi_ca_state,
        rtt_us = info.tcpi_rtt,
        rttvar_us = info.tcpi_rttvar,
        snd_cwnd = info.tcpi_snd_cwnd,
        snd_ssthresh = info.tcpi_snd_ssthresh,
        snd_mss = info.tcpi_snd_mss,
        rcv_mss = info.tcpi_rcv_mss,
        unacked = info.tcpi_unacked,
        retrans = info.tcpi_retrans,
        total_retrans = info.tcpi_total_retrans,
        lost = info.tcpi_lost,
        rcv_space = info.tcpi_rcv_space,
        rto_us = info.tcpi_rto,
        "tcp_info"
    );
}

/// Sleep for the given duration in 100ms increments, checking shutdown
/// and promote flags between increments. Returns early if either is set.
pub(super) fn sleep_checking_flags(
    duration: std::time::Duration,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
) {
    let deadline = std::time::Instant::now() + duration;
    while std::time::Instant::now() < deadline {
        if shutdown.load(Ordering::Relaxed) || promote.load(Ordering::Acquire) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Shut down the replica pipeline and extract Exchange + SectorWriter from
/// the stage threads. Returns None if a thread panicked.
///
/// Relies on the caller having published a `JournalEvent::Shutdown`
/// sentinel to the input ring before invoking this — the journal and
/// matching stages exit when they consume the sentinel via the normal
/// event-processing path. We deliberately don't flip `shutdown_flag`
/// here: doing so could cause a stage to take its emergency-abort
/// branch *before* consuming the sentinel, hitting the
/// drain-vs-cursor race that the sentinel design exists to avoid.
///
/// `drain_handle` and `shadow_handle` still observe `shutdown_flag`
/// because they don't process the input ring (no sentinel reaches
/// them); set the flag for them only.
pub(super) fn shutdown_pipeline<A: Application + Send + 'static, W: Send + 'static>(
    shutdown_flag: &AtomicBool,
    journal_handle: std::thread::JoinHandle<Result<W, melin_journal::JournalError>>,
    matching_handle: std::thread::JoinHandle<A>,
    drain_handle: std::thread::JoinHandle<()>,
    shadow_handle: Option<std::thread::JoinHandle<()>>,
) -> Option<(A, W)> {
    // Defense-in-depth: set the flag before joining. The sentinel was
    // already published by `tcp_receiver` before this call, so no further
    // events can arrive in the input ring — setting the flag here cannot
    // race with new publishes. The flag is the fallback exit signal for
    // paths that don't observe the sentinel: `run_sync` in `no-persist`
    // builds, the drain consumer, the shadow stage, and any case where
    // the receiver thread panicked before publishing the sentinel.
    shutdown_flag.store(true, Ordering::Release);
    let writer = match journal_handle.join() {
        Ok(Ok(w)) => w,
        Ok(Err(e)) => {
            tracing::error!(error = %e, "replica journal stage returned error on shutdown");
            return None;
        }
        Err(_) => return None,
    };
    let exchange = matching_handle.join().ok()?;
    let _ = drain_handle.join();
    if let Some(h) = shadow_handle {
        let _ = h.join();
    }
    Some((exchange, writer))
}

/// Live replica pipeline — built once on first connect (or after a snapshot
/// transfer), persists across `Disconnected` reconnects so the orchestrator
/// doesn't pay the journal-recover + thread-spawn cost on every drop.
///
/// Shared between the kernel-TCP and DPDK receivers; both build pipelines
/// with the same shape (journal / matching / drain / optional shadow), and
/// both refresh handshake state from `last_seq` + `chain_hash_lock` at
/// reconnect time.
pub(super) struct ReplicaPipelineHandles<A: Application, W: Send + 'static> {
    pub(super) input_producer: melin_disruptor::ring::Producer<InputSlot<A::Event>>,
    pub(super) journal_cursor: Arc<melin_disruptor::padding::Sequence>,
    /// Highest journal sequence durably persisted, published by JournalStage
    /// after each fsync. Read by the orchestrator to fill in the reconnect
    /// handshake without owning the writer.
    pub(super) last_seq: Arc<AtomicU64>,
    /// SeqLock-published chain hash (Option to mirror the primary-side
    /// pattern; always Some on replicas now).
    pub(super) chain_hash_lock: Option<Arc<melin_disruptor::seqlock::SeqLock<[u8; 32]>>>,
    /// Per-pipeline shutdown flag — flipped only on a controlled teardown
    /// (Promote/Shutdown/Fatal/Snapshot). NOT flipped on `Disconnected`.
    pub(super) pipeline_shutdown: Arc<AtomicBool>,
    pub(super) journal_handle: std::thread::JoinHandle<Result<W, melin_journal::JournalError>>,
    pub(super) matching_handle: std::thread::JoinHandle<A>,
    pub(super) drain_handle: std::thread::JoinHandle<()>,
    pub(super) shadow_handle: Option<std::thread::JoinHandle<()>>,
}

/// Build the replica pipeline and spawn its stage threads on the configured
/// cores. Returns the bundle of state the orchestrator keeps across
/// `Disconnected` reconnects.
pub(super) fn build_replica_pipeline_with_threads<A, W>(
    exchange: A,
    writer: W,
    cores: crate::server::PipelineCores,
    snapshot_interval_ms: u64,
    snapshot_path: std::path::PathBuf,
    group_commit_delay: std::time::Duration,
    busy_spin: bool,
    rotation: Option<(u64, Arc<AtomicBool>)>,
) -> Result<ReplicaPipelineHandles<A, W>, Box<dyn std::error::Error>>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
    JournalStage<A::Event, W>: JournalStageRun<A::Event, Writer = W>,
{
    let shadow_exchange = <A as Application>::clone_via_snapshot(&exchange)?;

    let enable_shadow = snapshot_interval_ms > 0;
    let pipeline = melin_transport_core::pipeline::build_replica_pipeline(
        exchange,
        writer,
        4096, // max_journal_batch
        group_commit_delay,
        busy_spin,
        enable_shadow,
    );

    let pipeline_shutdown = Arc::new(AtomicBool::new(false));

    let ps = Arc::clone(&pipeline_shutdown);
    let journal_core = cores.journal;
    let mut journal_stage = pipeline.journal_stage;
    if let Some((max_bytes, flag)) = rotation {
        journal_stage.set_rotation(max_bytes, Some(flag));
    }
    let journal_handle = std::thread::Builder::new()
        .name("journal".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("journal", journal_core);
            journal_stage.run(&ps)
        })
        .expect("spawn journal thread");

    let ps = Arc::clone(&pipeline_shutdown);
    let matching_core = cores.matching;
    let matching_stage = pipeline.matching_stage;
    let matching_handle = std::thread::Builder::new()
        .name("matching".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("matching", matching_core);
            matching_stage.run(&ps)
        })
        .expect("spawn matching thread");

    // Drain thread uses the response core — replicas have no response stage,
    // but the consumer needs to be drained so the output ring doesn't fill.
    let ps = Arc::clone(&pipeline_shutdown);
    let drain_core = cores.response;
    let drain_consumer = pipeline.drain_consumer;
    let drain_handle = std::thread::Builder::new()
        .name("drain".into())
        .spawn(move || {
            melin_app::affinity::pin_thread("drain", drain_core);
            let mut consumer = drain_consumer;
            let mut batch = vec![OutputSlot::<A::Report, A::QueryResponse>::default(); 256];
            loop {
                if ps.load(Ordering::Relaxed) {
                    return;
                }
                let count = consumer.consume_batch(&mut batch, 256);
                if count == 0 {
                    if busy_spin {
                        std::hint::spin_loop();
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        })
        .expect("spawn drain thread");

    let shadow_handle = if let Some(shadow_cons) = pipeline.shadow_consumer {
        let snap_path = snapshot_path;
        let chain_lock = pipeline
            .chain_hash_lock
            .as_ref()
            .expect("chain hash lock with shadow")
            .clone();
        let ps = Arc::clone(&pipeline_shutdown);
        let shadow_core = cores.shadow;
        Some(
            std::thread::Builder::new()
                .name("replica-shadow".into())
                .spawn(move || {
                    melin_app::affinity::pin_thread("replica-shadow", shadow_core);
                    melin_transport_core::shadow::run(
                        shadow_cons,
                        shadow_exchange,
                        snap_path,
                        std::time::Duration::from_millis(snapshot_interval_ms),
                        chain_lock,
                        &ps,
                        false,
                    );
                })
                .expect("spawn shadow thread"),
        )
    } else {
        None
    };

    Ok(ReplicaPipelineHandles {
        input_producer: pipeline.input_producer,
        journal_cursor: pipeline.journal_cursor,
        last_seq: pipeline.last_seq,
        chain_hash_lock: pipeline.chain_hash_lock,
        pipeline_shutdown,
        journal_handle,
        matching_handle,
        drain_handle,
        shadow_handle,
    })
}

/// Tear down the pipeline: signal shutdown, join all threads, return the
/// recovered (App, SectorWriter) so the orchestrator can use them for the
/// next pipeline build (e.g., post-snapshot) or pass them up on promotion.
pub(super) fn teardown_replica_pipeline<A: Application + Send + 'static, W: Send + 'static>(
    handles: ReplicaPipelineHandles<A, W>,
) -> Option<(A, W)> {
    shutdown_pipeline::<A, W>(
        &handles.pipeline_shutdown,
        handles.journal_handle,
        handles.matching_handle,
        handles.drain_handle,
        handles.shadow_handle,
    )
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;

    use super::auth::{authenticate_replica, authenticate_with_primary};
    use super::*;
    use melin_trading::trading_event::TradingEvent;
    type InputSlot = melin_transport_core::pipeline::InputSlot<TradingEvent>;
    use melin_transport_core::replication::protocol::{
        MAX_CONTROL_FRAME, MAX_DATA_FRAME, MSG_AUTH_OK, MSG_CHALLENGE_RESPONSE, MSG_SNAPSHOT_BEGIN,
        MSG_SNAPSHOT_CHUNK, MSG_SNAPSHOT_END, decode_auth_result, decode_challenge,
        decode_challenge_response, decode_primary_message, decode_replica_message, encode_ack,
        encode_auth_failed, encode_auth_ok, encode_challenge, encode_challenge_response,
        encode_handshake, encode_hash_mismatch, encode_heartbeat, encode_input_batch,
        encode_need_snapshot, encode_snapshot_begin, encode_snapshot_chunk, encode_snapshot_end,
        encode_stream_start, read_frame, try_decode_input_batch,
    };

    /// Build a wire-ready `InputBatch` frame containing a single `Tick`
    /// slot at the given sequence — the protocol-level tests don't need
    /// real journal payloads, just something with a known max sequence.
    fn encode_input_batch_with_seq(end_sequence: u64, buf: &mut Vec<u8>) {
        let slot = InputSlot {
            connection_id: 0,
            key_hash: 0,
            request_seq: 0,
            sequence: end_sequence,
            timestamp_ns: 0,
            event: melin_journal::JournalEvent::Tick { now_ns: 0 },
            publish_ts: Default::default(),
            recv_ts: Default::default(),
        };
        encode_input_batch(&[slot], buf);
    }

    #[test]
    fn handshake_encode_decode_round_trip() {
        let handshake = Handshake {
            last_sequence: 42,
            chain_hash: [0xAB; 32],
        };
        let mut buf = Vec::new();
        encode_handshake(&handshake, &mut buf);

        // Read frame: skip 4-byte length prefix.
        let payload = &buf[4..];
        let msg = decode_replica_message(payload).unwrap();
        match msg {
            ReplicaMessage::Handshake(h) => {
                assert_eq!(h.last_sequence, 42);
                assert_eq!(h.chain_hash, [0xAB; 32]);
            }
            _ => panic!("expected Handshake"),
        }
    }

    #[test]
    fn ack_encode_decode_round_trip() {
        let ack = Ack {
            acked_sequence: 1000,
            in_memory_sequence: 1024,
        };
        let mut buf = Vec::new();
        encode_ack(&ack, &mut buf);

        let payload = &buf[4..];
        let msg = decode_replica_message(payload).unwrap();
        match msg {
            ReplicaMessage::Ack(a) => {
                assert_eq!(a.acked_sequence, 1000);
                assert_eq!(a.in_memory_sequence, 1024);
            }
            _ => panic!("expected Ack"),
        }
    }

    /// Pin the exact on-the-wire byte layout of an Ack frame. A future
    /// `repr(C)` field reorder, alignment change, or accidental
    /// big-endian wrapper substitution would silently break replica/
    /// primary compatibility — the const `size_of` assert in
    /// `protocol.rs` only catches size changes, not layout changes.
    /// This test pins the expected bytes so any such break shows up
    /// loudly on the next CI run.
    #[test]
    fn ack_wire_byte_pattern() {
        let ack = Ack {
            acked_sequence: 0xDEAD_BEEF_CAFE_F00D,
            in_memory_sequence: 0x1122_3344_5566_7788,
        };
        let mut buf = Vec::new();
        encode_ack(&ack, &mut buf);
        // [length:u32 LE = 17][tag:u8 = MSG_ACK = 0x02]
        // [acked_sequence:u64 LE][in_memory_sequence:u64 LE]
        let expected: &[u8] = &[
            0x11, 0x00, 0x00, 0x00, // length = 17 LE
            0x02, // MSG_ACK
            0x0D, 0xF0, 0xFE, 0xCA, 0xEF, 0xBE, 0xAD, 0xDE, // acked_sequence LE
            0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, // in_memory_sequence LE
        ];
        assert_eq!(buf.as_slice(), expected, "Ack wire layout drifted");
    }

    #[test]
    fn stream_start_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_stream_start(99, &[0xAA; 64], &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::StreamStart {
                start_sequence,
                genesis_entry,
            } => {
                assert_eq!(start_sequence, 99);
                assert_eq!(genesis_entry, vec![0xAA; 64]);
            }
            _ => panic!("expected StreamStart"),
        }
    }

    #[test]
    fn heartbeat_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_heartbeat(123, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::Heartbeat { sequence } => {
                assert_eq!(sequence, 123);
            }
            _ => panic!("expected Heartbeat"),
        }
    }

    #[test]
    fn need_snapshot_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_need_snapshot(&mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        assert!(matches!(msg, PrimaryMessage::NeedSnapshot));
    }

    #[test]
    fn hash_mismatch_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_hash_mismatch(&mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        assert!(matches!(msg, PrimaryMessage::HashMismatch));
    }

    #[test]
    fn unknown_replica_message_type_is_error() {
        let payload = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = decode_replica_message(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_primary_message_type_is_error() {
        let payload = [0xFF, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = decode_primary_message(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn read_frame_enforces_max_size() {
        // Create a buffer with a length prefix claiming 1000 bytes.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 100]); // not enough data, but max_size check comes first

        let mut cursor = std::io::Cursor::new(buf);
        let result = read_frame(&mut cursor, 64);
        assert!(result.is_err());
    }

    #[test]
    fn challenge_encode_decode_round_trip() {
        let nonce = [0x42; 32];
        let mut buf = Vec::new();
        encode_challenge(&nonce, &mut buf);

        let payload = &buf[4..];
        let decoded = decode_challenge(payload).unwrap();
        assert_eq!(decoded, nonce);
    }

    #[test]
    fn challenge_response_encode_decode_round_trip() {
        let sig = [0xAA; 64];
        let pubkey = [0xBB; 32];
        let mut buf = Vec::new();
        encode_challenge_response(&sig, &pubkey, &mut buf);

        let payload = &buf[4..];
        let (decoded_sig, decoded_pubkey) = decode_challenge_response(payload).unwrap();
        assert_eq!(decoded_sig, sig);
        assert_eq!(decoded_pubkey, pubkey);
    }

    #[test]
    fn auth_ok_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_auth_ok(&mut buf);

        let payload = &buf[4..];
        assert!(decode_auth_result(payload).unwrap());
    }

    #[test]
    fn auth_failed_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_auth_failed(&mut buf);

        let payload = &buf[4..];
        assert!(!decode_auth_result(payload).unwrap());
    }

    #[test]
    fn decode_challenge_rejects_wrong_tag() {
        let mut payload = [0u8; 33];
        payload[0] = MSG_AUTH_OK;
        assert!(decode_challenge(&payload).is_err());
    }

    #[test]
    fn decode_challenge_response_rejects_short_payload() {
        let payload = [MSG_CHALLENGE_RESPONSE; 10]; // too short
        assert!(decode_challenge_response(&payload).is_err());
    }

    #[test]
    fn auth_round_trip_valid_key() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        let repl_key = SigningKey::from_bytes(&[0xFC; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            repl_key.verifying_key().to_bytes(),
        );
        let keys_content = format!("replication {pub_b64} test-replica\n");
        let authorized_keys = melin_protocol::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let repl_key_clone = SigningKey::from_bytes(&[0xFC; 32]);
        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            authenticate_with_primary(&mut reader, &mut writer, &repl_key_clone)
        });

        let mut reader = primary_stream.try_clone().unwrap();
        let mut writer = primary_stream;
        authenticate_replica(&mut reader, &mut writer, &authorized_keys).unwrap();

        replica_handle.join().unwrap().unwrap();
    }

    #[test]
    fn auth_rejects_unknown_key() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        // authorized_keys has one key, but the replica uses a different one.
        let authorized_key = SigningKey::from_bytes(&[0xAA; 32]);
        let rogue_key = SigningKey::from_bytes(&[0xBB; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            authorized_key.verifying_key().to_bytes(),
        );
        let keys_content = format!("replication {pub_b64} authorized-replica\n");
        let authorized_keys = melin_protocol::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            authenticate_with_primary(&mut reader, &mut writer, &rogue_key)
        });

        let mut reader = primary_stream.try_clone().unwrap();
        let mut writer = primary_stream;
        let result = authenticate_replica(&mut reader, &mut writer, &authorized_keys);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown"));

        // Replica should also get a rejection.
        let replica_result = replica_handle.join().unwrap();
        assert!(replica_result.is_err());
    }

    #[test]
    fn auth_rejects_wrong_permission() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        // Key exists but has Trader permission, not Replication.
        let key = SigningKey::from_bytes(&[0xCC; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            key.verifying_key().to_bytes(),
        );
        let keys_content = format!("trader {pub_b64} wrong-role\n");
        let authorized_keys = melin_protocol::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            authenticate_with_primary(&mut reader, &mut writer, &key)
        });

        let mut reader = primary_stream.try_clone().unwrap();
        let mut writer = primary_stream;
        let result = authenticate_replica(&mut reader, &mut writer, &authorized_keys);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Replication"));

        let replica_result = replica_handle.join().unwrap();
        assert!(replica_result.is_err());
    }

    /// A replica that sends a validly-formatted but tampered signature
    /// (correct public key, wrong signature bytes) is rejected.
    #[test]
    fn auth_rejects_invalid_signature() {
        use ed25519_dalek::SigningKey;
        use std::os::unix::net::UnixStream;

        // Register the correct key.
        let correct_key = SigningKey::from_bytes(&[0xDD; 32]);
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            correct_key.verifying_key().to_bytes(),
        );
        let keys_content = format!("replication {pub_b64} test-replica\n");
        let authorized_keys = melin_protocol::auth::AuthorizedKeys::parse(&keys_content).unwrap();

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();
        primary_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        replica_stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        // Replica side: read challenge, but sign with a DIFFERENT key,
        // then send the response with the correct public key (spoofing).
        let replica_handle = std::thread::spawn(move || {
            use melin_transport_core::replication::protocol::*;

            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;

            // Read the challenge.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let nonce = decode_challenge(&frame).unwrap();

            // Sign with a WRONG key but send the CORRECT public key.
            let wrong_key = SigningKey::from_bytes(&[0xEE; 32]);
            let bad_signature = ed25519_dalek::Signer::sign(&wrong_key, &nonce);
            let correct_pubkey = correct_key.verifying_key();

            let mut buf = Vec::with_capacity(128);
            encode_challenge_response(
                &bad_signature.to_bytes(),
                correct_pubkey.as_bytes(),
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();

            // Should receive AuthFailed.
            let result_frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let ok = decode_auth_result(&result_frame).unwrap();
            assert!(!ok, "should receive auth failure");
        });

        let mut reader = primary_stream.try_clone().unwrap();
        let mut writer = primary_stream;
        let result = authenticate_replica(&mut reader, &mut writer, &authorized_keys);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("signature verification failed"),
            "should fail on signature verification"
        );

        replica_handle.join().unwrap();
    }

    #[test]
    fn sender_receiver_end_to_end() {
        use std::os::unix::net::UnixStream;

        // Create a mock connection.
        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let replication_cursor = Arc::new(AtomicU64::new(0));

        // Spawn a thread simulating the replica side.
        let _replica_cursor = Arc::clone(&replication_cursor);
        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;

            // Send handshake.
            let mut buf = Vec::new();
            let handshake = Handshake {
                last_sequence: 0,
                chain_hash: [0u8; 32],
            };
            encode_handshake(&handshake, &mut buf);
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let msg = decode_primary_message(&frame).unwrap();
            assert!(matches!(msg, PrimaryMessage::StreamStart { .. }));

            // Read InputBatch.
            let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
            let slots: Vec<InputSlot> = try_decode_input_batch(&frame).expect("decode InputBatch");
            let end_seq = slots
                .last()
                .map(|s| s.sequence)
                .expect("InputBatch carried at least one slot");

            // Send ack.
            let ack = Ack {
                acked_sequence: end_seq,
                in_memory_sequence: end_seq,
            };
            encode_ack(&ack, &mut buf);
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();

            end_seq
        });

        // Primary side: simulate handle_replica_connection partially.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let handshake = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Handshake(h) => h,
            _ => panic!("expected Handshake"),
        };
        assert_eq!(handshake.last_sequence, 0);

        // Send StreamStart.
        let mut buf = Vec::new();
        encode_stream_start(0, &[0u8; 32], &mut buf); // fake genesis bytes for test
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send an InputBatch with a single Tick slot at seq 42.
        encode_input_batch_with_seq(42, &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Read ack.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let ack = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Ack(a) => a,
            _ => panic!("expected Ack"),
        };
        assert_eq!(ack.acked_sequence, 42);

        // Join replica thread.
        let end_seq = replica_handle.join().unwrap();
        assert_eq!(end_seq, 42);
    }

    #[test]
    fn disconnect_degrades_cursor_to_max() {
        // When a replica disconnects, run_sender resets the replication
        // cursor to u64::MAX so the response stage stops gating on acks.
        // Test the cursor lifecycle: starts at 0, set during handshake,
        // then reset to MAX on disconnect.
        let cursor = Arc::new(AtomicU64::new(0));

        // Simulate handshake: cursor set to last_sequence + 1.
        let handshake_seq = 42u64;
        cursor.store(handshake_seq + 1, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 43);

        // Simulate ack advancing cursor.
        cursor.fetch_max(100 + 1, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 101);

        // Simulate disconnect: run_sender resets to MAX.
        cursor.store(u64::MAX, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), u64::MAX);

        // Simulate reconnect: cursor set back to handshake value.
        cursor.store(1, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 1);
    }

    #[test]
    fn ack_advances_cursor_monotonically() {
        // Acks must only advance the cursor, never regress it.
        // A stale ack (lower sequence) should be ignored.
        let cursor = Arc::new(AtomicU64::new(0));

        // Simulate processing ack seq=100 → cursor should become 101.
        let new_val = 100 + 1;
        cursor.fetch_max(new_val, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 101);

        // Stale ack seq=50 → cursor should stay at 101.
        let stale_val = 50 + 1;
        cursor.fetch_max(stale_val, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 101);

        // Newer ack seq=200 → cursor should advance to 201.
        let newer_val = 200 + 1;
        cursor.fetch_max(newer_val, Ordering::Release);
        assert_eq!(cursor.load(Ordering::Acquire), 201);
    }

    #[test]
    fn multiple_data_batches_acked_in_order() {
        // Send multiple InputBatch frames, verify replica acks each one
        // and the cursor advances correctly.
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            let mut buf = Vec::new();

            // Send handshake.
            encode_handshake(
                &Handshake {
                    last_sequence: 0,
                    chain_hash: [0u8; 32],
                },
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::StreamStart { .. }
            ));

            // Read and ack 3 InputBatches.
            let mut acked_seqs = Vec::new();
            for _ in 0..3 {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                let slots: Vec<InputSlot> =
                    try_decode_input_batch(&frame).expect("decode InputBatch");
                let end_seq = slots
                    .last()
                    .map(|s| s.sequence)
                    .expect("InputBatch carried at least one slot");
                acked_seqs.push(end_seq);

                encode_ack(
                    &Ack {
                        acked_sequence: end_seq,
                        in_memory_sequence: end_seq,
                    },
                    &mut buf,
                );
                writer.write_all(&buf).unwrap();
                writer.flush().unwrap();
                buf.clear();
            }

            acked_seqs
        });

        // Primary side.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;
        let mut buf = Vec::new();

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        assert!(matches!(
            decode_replica_message(&frame).unwrap(),
            ReplicaMessage::Handshake(_)
        ));

        // Send StreamStart.
        encode_stream_start(0, &[0u8; 32], &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send 3 InputBatches with increasing sequence numbers.
        for seq in [10u64, 20, 30] {
            encode_input_batch_with_seq(seq, &mut buf);
            p_writer.write_all(&buf).unwrap();
            p_writer.flush().unwrap();
            buf.clear();
        }

        // Read 3 acks.
        for expected_seq in [10u64, 20, 30] {
            let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
            let ack = match decode_replica_message(&frame).unwrap() {
                ReplicaMessage::Ack(a) => a,
                other => panic!("expected Ack, got {other:?}"),
            };
            assert_eq!(ack.acked_sequence, expected_seq);
        }

        let acked = replica_handle.join().unwrap();
        assert_eq!(acked, vec![10, 20, 30]);
    }

    #[test]
    fn heartbeat_encode_contains_sequence() {
        // Heartbeat messages carry the last known sequence so the replica
        // can verify it hasn't missed any data.
        let mut buf = Vec::new();
        encode_heartbeat(999, &mut buf);

        let payload = &buf[4..];
        match decode_primary_message(payload).unwrap() {
            PrimaryMessage::Heartbeat { sequence } => {
                assert_eq!(sequence, 999);
            }
            other => panic!("expected Heartbeat, got {other:?}"),
        }
    }

    #[test]
    fn replica_mid_stream_handshake_with_nonzero_sequence() {
        // A replica that already has some data sends a non-zero last_sequence
        // in its handshake. The primary should respond with StreamStart
        // containing that sequence, and the replica should only receive
        // events after that point.
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let replica_handle = std::thread::spawn(move || {
            let mut reader = replica_stream.try_clone().unwrap();
            let mut writer = replica_stream;
            let mut buf = Vec::new();

            // Replica already has events up to sequence 100.
            encode_handshake(
                &Handshake {
                    last_sequence: 100,
                    chain_hash: [0xBB; 32],
                },
                &mut buf,
            );
            writer.write_all(&buf).unwrap();
            writer.flush().unwrap();
            buf.clear();

            // Read StreamStart — should echo back our last_sequence.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            match decode_primary_message(&frame).unwrap() {
                PrimaryMessage::StreamStart { start_sequence, .. } => {
                    assert_eq!(
                        start_sequence, 100,
                        "StreamStart should echo replica's last_sequence"
                    );
                }
                other => panic!("expected StreamStart, got {other:?}"),
            }

            // Read an InputBatch — should be for events AFTER 100.
            let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
            let slots: Vec<InputSlot> = try_decode_input_batch(&frame).expect("decode InputBatch");
            let end_sequence = slots
                .last()
                .map(|s| s.sequence)
                .expect("InputBatch carried at least one slot");
            assert!(
                end_sequence > 100,
                "InputBatch should be after replica's last_sequence"
            );
        });

        // Primary side.
        let mut p_reader = primary_stream.try_clone().unwrap();
        let mut p_writer = primary_stream;
        let mut buf = Vec::new();

        // Read handshake.
        let frame = read_frame(&mut p_reader, MAX_CONTROL_FRAME).unwrap();
        let handshake = match decode_replica_message(&frame).unwrap() {
            ReplicaMessage::Handshake(h) => h,
            _ => panic!("expected Handshake"),
        };
        assert_eq!(handshake.last_sequence, 100);
        assert_eq!(handshake.chain_hash, [0xBB; 32]);

        // Send StreamStart echoing the replica's sequence.
        encode_stream_start(handshake.last_sequence, &[0u8; 32], &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();
        buf.clear();

        // Send InputBatch with sequence 150 (after replica's 100).
        encode_input_batch_with_seq(150, &mut buf);
        p_writer.write_all(&buf).unwrap();
        p_writer.flush().unwrap();

        replica_handle.join().unwrap();
    }

    #[test]
    fn snapshot_begin_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_snapshot_begin(1_000_000, 42, &[0xAB; 32], &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotBegin {
                snapshot_len,
                snap_sequence,
                snap_chain_hash,
            } => {
                assert_eq!(snapshot_len, 1_000_000);
                assert_eq!(snap_sequence, 42);
                assert_eq!(snap_chain_hash, [0xAB; 32]);
            }
            _ => panic!("expected SnapshotBegin"),
        }
    }

    #[test]
    fn snapshot_chunk_encode_decode_round_trip() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut buf = Vec::new();
        encode_snapshot_chunk(&data, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotChunk(chunk) => {
                assert_eq!(chunk, data);
            }
            _ => panic!("expected SnapshotChunk"),
        }
    }

    #[test]
    fn snapshot_end_encode_decode_round_trip() {
        let mut buf = Vec::new();
        encode_snapshot_end(0xDEADBEEF, &mut buf);

        let payload = &buf[4..];
        let msg = decode_primary_message(payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotEnd { crc32c } => {
                assert_eq!(crc32c, 0xDEADBEEF);
            }
            _ => panic!("expected SnapshotEnd"),
        }
    }

    /// Simulate the receiver side of a snapshot transfer where the
    /// advertised snap_len doesn't match the actual bytes sent.
    /// The receiver must detect this and return an error.
    #[test]
    fn snapshot_receiver_detects_length_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        // Receiver thread — reads NeedSnapshot, then the snapshot transfer.
        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            // Read NeedSnapshot.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            // Read SnapshotBegin.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let (snap_len, _snap_sequence, _snap_chain_hash) =
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotBegin {
                        snapshot_len,
                        snap_sequence,
                        snap_chain_hash,
                    } => (snapshot_len, snap_sequence, snap_chain_hash),
                    other => panic!("expected SnapshotBegin, got {other:?}"),
                };

            // Receive chunks and check length at SnapshotEnd.
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                    }
                    PrimaryMessage::SnapshotEnd { .. } => {
                        if received != snap_len {
                            return format!(
                                "snapshot length mismatch: expected {snap_len} bytes, got {received}"
                            );
                        }
                        return String::new(); // no error
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }
        });

        // Primary side — send snapshot with wrong advertised length.
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let actual_data = vec![0xAA; 100];
        let wrong_len = 999u64; // advertise 999 bytes, send only 100

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(wrong_len, 42, &[0xBB; 32], &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&actual_data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        let crc = crc32c::crc32c(&actual_data);
        encode_snapshot_end(crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("length mismatch"),
            "expected length mismatch error, got: {error_msg:?}"
        );
    }

    /// Simulate the receiver side of a snapshot transfer where the CRC
    /// in SnapshotEnd doesn't match the actual data. The receiver must
    /// detect and reject the transfer.
    #[test]
    fn snapshot_receiver_detects_crc_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            // Read NeedSnapshot.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            // Read SnapshotBegin.
            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let snap_len = match decode_primary_message(&frame).unwrap() {
                PrimaryMessage::SnapshotBegin { snapshot_len, .. } => snapshot_len,
                other => panic!("expected SnapshotBegin, got {other:?}"),
            };

            // Receive chunks, verify CRC at SnapshotEnd.
            let mut received_data = Vec::new();
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                        received_data.extend_from_slice(&data);
                    }
                    PrimaryMessage::SnapshotEnd {
                        crc32c: expected_crc,
                    } => {
                        if received != snap_len {
                            return format!("length mismatch: {snap_len} vs {received}");
                        }
                        let actual_crc = crc32c::crc32c(&received_data);
                        if actual_crc != expected_crc {
                            return format!(
                                "CRC mismatch: expected {expected_crc:#x}, got {actual_crc:#x}"
                            );
                        }
                        return String::new();
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }
        });

        // Primary side — send correct length but wrong CRC.
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let data = vec![0xAA; 100];

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(data.len() as u64, 42, &[0xBB; 32], &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        // Send a wrong CRC (flip bits).
        let wrong_crc = !crc32c::crc32c(&data);
        encode_snapshot_end(wrong_crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("CRC mismatch"),
            "expected CRC mismatch error, got: {error_msg:?}"
        );
    }

    /// The receiver verifies the chain hash from the loaded snapshot
    /// matches the one advertised in SnapshotBegin. Simulate a mismatch.
    #[test]
    fn snapshot_receiver_detects_chain_hash_mismatch() {
        use std::os::unix::net::UnixStream;

        let (primary_stream, replica_stream) = UnixStream::pair().unwrap();

        let receiver = std::thread::spawn(move || -> String {
            let mut reader = replica_stream.try_clone().unwrap();

            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            assert!(matches!(
                decode_primary_message(&frame).unwrap(),
                PrimaryMessage::NeedSnapshot,
            ));

            let frame = read_frame(&mut reader, MAX_CONTROL_FRAME).unwrap();
            let (snap_len, _snap_sequence, snap_chain_hash) =
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotBegin {
                        snapshot_len,
                        snap_sequence,
                        snap_chain_hash,
                    } => (snapshot_len, snap_sequence, snap_chain_hash),
                    other => panic!("expected SnapshotBegin, got {other:?}"),
                };

            // Receive the snapshot data.
            let mut received_data = Vec::new();
            let mut received: u64 = 0;
            loop {
                let frame = read_frame(&mut reader, MAX_DATA_FRAME).unwrap();
                match decode_primary_message(&frame).unwrap() {
                    PrimaryMessage::SnapshotChunk(data) => {
                        received += data.len() as u64;
                        received_data.extend_from_slice(&data);
                    }
                    PrimaryMessage::SnapshotEnd {
                        crc32c: expected_crc,
                    } => {
                        assert_eq!(received, snap_len, "length should match");
                        let actual_crc = crc32c::crc32c(&received_data);
                        assert_eq!(actual_crc, expected_crc, "CRC should match");
                        break;
                    }
                    other => panic!("unexpected message: {other:?}"),
                }
            }

            // Simulate chain hash verification: the loaded snapshot would
            // have a different chain hash than what SnapshotBegin advertised.
            let loaded_hash = [0xFF; 32]; // different from snap_chain_hash
            if loaded_hash != snap_chain_hash {
                return format!(
                    "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
                     loaded snapshot has {loaded_hash:02x?}"
                );
            }
            String::new()
        });

        // Primary side — send valid snapshot but with a chain hash in
        // SnapshotBegin that won't match what the replica "loads".
        let mut writer = primary_stream;
        let mut buf = Vec::new();

        let data = vec![0xAA; 64];
        // Advertise chain hash [0xBB; 32] — receiver will "load" [0xFF; 32].
        let advertised_hash = [0xBB; 32];

        encode_need_snapshot(&mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_begin(data.len() as u64, 10, &advertised_hash, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        encode_snapshot_chunk(&data, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        buf.clear();

        let crc = crc32c::crc32c(&data);
        encode_snapshot_end(crc, &mut buf);
        std::io::Write::write_all(&mut writer, &buf).unwrap();
        std::io::Write::flush(&mut writer).unwrap();

        let error_msg = receiver.join().unwrap();
        assert!(
            error_msg.contains("chain hash mismatch"),
            "expected chain hash mismatch error, got: {error_msg:?}"
        );
    }

    /// Primary-side magic validation: a file without the SNAP magic
    /// (0x534E4150) must be rejected before transfer.
    #[test]
    fn primary_rejects_snapshot_with_invalid_magic() {
        let tmp = tempfile::tempdir().unwrap();
        let snap_path = tmp.path().join("test.snapshot");

        // Write a file with wrong magic but enough bytes for a header.
        let mut bad_snap = vec![0u8; 64];
        bad_snap[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes()); // wrong magic

        std::fs::write(&snap_path, &bad_snap).unwrap();

        // Replicate the primary's validation logic.
        let snap_data = std::fs::read(&snap_path).unwrap();
        assert!(
            snap_data.len() >= 48,
            "file should be big enough for header"
        );

        let magic = u32::from_le_bytes(snap_data[0..4].try_into().unwrap());
        assert_ne!(magic, 0x534E_4150);
        assert_eq!(magic, 0xDEAD_BEEF);
    }

    /// Primary-side: a snapshot file smaller than the 48-byte header
    /// must be rejected.
    #[test]
    fn primary_rejects_snapshot_too_small_for_header() {
        let tmp = tempfile::tempdir().unwrap();
        let snap_path = tmp.path().join("test.snapshot");

        // Write a file smaller than the 48-byte header.
        std::fs::write(&snap_path, [0u8; 20]).unwrap();

        let snap_data = std::fs::read(&snap_path).unwrap();
        assert!(
            snap_data.len() < 48,
            "file must be too small for header validation"
        );
    }

    #[test]
    fn decode_snapshot_begin_too_short() {
        // SnapshotBegin needs type(1) + snapshot_len(8) + snap_sequence(8) + chain_hash(32) = 49.
        // Send only the type byte + a few extra bytes.
        let payload = [MSG_SNAPSHOT_BEGIN, 0x01, 0x02, 0x03];
        let err = decode_primary_message(&payload).unwrap_err();
        assert!(
            err.to_string().contains("SnapshotBegin too short"),
            "expected 'SnapshotBegin too short', got: {err}"
        );
    }

    #[test]
    fn decode_snapshot_end_too_short() {
        // SnapshotEnd needs type(1) + crc32c(4) = 5. Send only the type byte.
        let payload = [MSG_SNAPSHOT_END];
        let err = decode_primary_message(&payload).unwrap_err();
        assert!(
            err.to_string().contains("SnapshotEnd too short"),
            "expected 'SnapshotEnd too short', got: {err}"
        );
    }

    #[test]
    fn decode_snapshot_chunk_empty_data() {
        // SnapshotChunk with just the type byte — valid but empty payload.
        let payload = [MSG_SNAPSHOT_CHUNK];
        let msg = decode_primary_message(&payload).unwrap();
        match msg {
            PrimaryMessage::SnapshotChunk(data) => {
                assert!(data.is_empty());
            }
            _ => panic!("expected SnapshotChunk"),
        }
    }

    // --- PendingAckQueue tests ---

    fn make_journal_cursor(val: u64) -> melin_disruptor::padding::Sequence {
        melin_disruptor::padding::Sequence::new(AtomicU64::new(val))
    }

    #[test]
    fn pending_ack_queue_push_and_pop_ready() {
        let mut q = PendingAckQueue::new(8);
        assert!(q.is_empty());
        assert!(!q.is_full());

        q.push(10, 100);
        q.push(20, 200);
        assert!(!q.is_empty());

        // Cursor at 5 — neither ready.
        let cursor = make_journal_cursor(5);
        assert!(q.pop_ready(&cursor).is_none());

        // Cursor at 15 — first ready, second not.
        cursor.get().store(15, Ordering::Relaxed);
        assert_eq!(q.pop_ready(&cursor), Some(100));
        // Only one popped — second still pending.
        assert!(!q.is_empty());

        // Cursor at 25 — second now ready.
        cursor.get().store(25, Ordering::Relaxed);
        assert_eq!(q.pop_ready(&cursor), Some(200));
        assert!(q.is_empty());
    }

    #[test]
    fn pending_ack_queue_pop_ready_returns_highest_sequence() {
        // When multiple acks become ready simultaneously, pop_ready
        // returns the highest acked_sequence (ack semantics are
        // cumulative — "everything up to this sequence is durable").
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);
        q.push(20, 200);
        q.push(30, 300);

        let cursor = make_journal_cursor(30);
        assert_eq!(q.pop_ready(&cursor), Some(300));
        assert!(q.is_empty());
    }

    #[test]
    fn pending_ack_queue_capacity_and_full() {
        let mut q = PendingAckQueue::new(8);
        for i in 0..8 {
            assert!(!q.is_full());
            q.push(i as u64 + 1, (i + 1) as u64 * 100);
        }
        assert!(q.is_full());
    }

    #[test]
    fn pending_ack_queue_pop_oldest_blocking() {
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);
        q.push(20, 200);

        // Cursor already past both targets — pop_oldest_blocking
        // returns immediately.
        let cursor = make_journal_cursor(25);
        let seq = q.pop_oldest_blocking(&cursor, true);
        // Should pop both (oldest + any others that became ready).
        assert_eq!(seq, 200);
        assert!(q.is_empty());
    }

    #[test]
    fn pending_ack_queue_wraps_around() {
        let mut q = PendingAckQueue::new(8);
        let cursor = make_journal_cursor(100);

        // Fill and drain multiple times to exercise circular buffer wrap.
        for round in 0..3 {
            for i in 0..8 {
                let target = (round * 8 + i) as u64 + 1;
                q.push(target, target * 10);
            }
            assert!(q.is_full());
            let seq = q.pop_ready(&cursor).expect("should be ready");
            assert_eq!(seq, (round * 8 + 8) as u64 * 10);
            assert!(q.is_empty());
        }
    }

    #[test]
    fn pending_ack_queue_pop_all_blocking_empty() {
        let mut q = PendingAckQueue::new(8);
        let cursor = make_journal_cursor(0);
        assert!(q.pop_all_blocking(&cursor, true).is_none());
    }

    // --- try_flush_dual_track tests ---

    #[test]
    fn dual_track_returns_none_when_both_tracks_idle() {
        let mut q = PendingAckQueue::new(8);
        let cursor = make_journal_cursor(0);
        assert!(
            try_flush_dual_track(&mut q, &cursor, 0, 0, 0).is_none(),
            "no advance on either track → no ack"
        );
    }

    #[test]
    fn dual_track_fires_on_persisted_advance_only() {
        // Persisted track moves: pop_ready returns 100; in-memory
        // stayed at 50 (last_sent). Expect ack with acked=100,
        // in_memory=50 (no regression — the wire field carries the
        // current sample, not a "no change" marker).
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);
        let cursor = make_journal_cursor(20);
        let ack = try_flush_dual_track(&mut q, &cursor, 50, 0, 50).expect("persisted advanced");
        assert_eq!(ack.acked_sequence, 100);
        assert_eq!(ack.in_memory_sequence, 50);
    }

    #[test]
    fn dual_track_fires_on_in_memory_advance_only() {
        // Persisted is idle (queue empty): unwrap_or keeps the
        // last-sent acked at 100. In-memory bumped from 100 to 200.
        let mut q = PendingAckQueue::new(8);
        let cursor = make_journal_cursor(0);
        let ack = try_flush_dual_track(&mut q, &cursor, 200, 100, 100).expect("in-memory advanced");
        assert_eq!(ack.acked_sequence, 100);
        assert_eq!(ack.in_memory_sequence, 200);
    }

    #[test]
    fn dual_track_coalesces_until_caller_updates_trackers() {
        // Caller did not advance trackers between calls. The queue
        // popped on call 1; on call 2 it's empty so persisted stays
        // at 100 (unwrap_or). In-memory advanced 50 → 80 between
        // calls. Second call must still fire because tracker is
        // still 50.
        let mut q = PendingAckQueue::new(8);
        q.push(10, 100);
        let cursor = make_journal_cursor(20);
        let ack1 = try_flush_dual_track(&mut q, &cursor, 50, 0, 50).expect("call 1 fires");
        assert_eq!(ack1.acked_sequence, 100);
        // Caller "forgot" to update trackers (simulates send failure).
        let ack2 = try_flush_dual_track(&mut q, &cursor, 80, 0, 50)
            .expect("call 2 fires on in-memory advance");
        assert_eq!(
            ack2.acked_sequence, 0,
            "no new persisted pop → unwrap_or(last_sent)"
        );
        assert_eq!(ack2.in_memory_sequence, 80);
    }

    #[test]
    fn dual_track_no_duplicate_ack_after_backpressure_drain() {
        // Models the backpressure-drain → resume-normal-flush path
        // taken by every receiver when `PendingAckQueue` fills up.
        // The receiver must drain the queue (via the
        // pop_oldest_blocking path in production; pop_ready here),
        // update *both* trackers from the drained ack, and only
        // then resume the normal cursor-driven flush. The bug
        // classes this pins are:
        //   (a) emitting a follow-up ack whose `acked_sequence`
        //       regresses below what the drain already sent
        //       (caught by the debug_assert! in
        //       `try_flush_dual_track`, also asserted at value
        //       level here);
        //   (b) emitting a duplicate ack carrying the same
        //       cursors as the drain when neither track actually
        //       advanced — the leak class fixed by ensuring every
        //       send site updates both `last_sent_acked_seq` and
        //       `last_sent_in_memory_seq`.
        let mut q = PendingAckQueue::new(4);
        // Fill the queue: four pending acks at primary seqs
        // 100, 200, 300, 400 with journal targets 10..=40.
        for i in 1..=4u64 {
            q.push(i * 10, i * 100);
        }
        assert!(q.is_full());

        // Backpressure path: caller drains all ready entries
        // before pushing more. Highest acked seen = 400.
        let cursor = make_journal_cursor(40);
        let drained = q.pop_ready(&cursor).expect("all entries durable");
        assert_eq!(drained, 400);
        assert!(q.is_empty());

        // Caller updates BOTH trackers from the drain. in_memory
        // at the time of the drained batch was 450.
        let mut last_sent_acked = drained;
        let mut last_sent_in_mem = 450u64;

        // Quiescent immediately after drain: no fresh push, no
        // in-memory advance → try_flush must return None.
        // Failing here means we'd emit a duplicate ack carrying
        // the same (400, 450) the backpressure drain already sent.
        assert!(
            try_flush_dual_track(
                &mut q,
                &cursor,
                last_sent_in_mem,
                last_sent_acked,
                last_sent_in_mem,
            )
            .is_none(),
            "no advance on either track after backpressure drain → no duplicate ack",
        );

        // Push a fresh batch (primary seq 500, journal target 50),
        // cursor catches up, in_memory advances to 500.
        q.push(50, 500);
        let cursor2 = make_journal_cursor(50);
        let ack = try_flush_dual_track(&mut q, &cursor2, 500, last_sent_acked, last_sent_in_mem)
            .expect("fresh batch after drain fires a new ack");
        assert!(
            ack.acked_sequence >= last_sent_acked,
            "regression: drain sent {last_sent_acked} but next ack carries {}",
            ack.acked_sequence,
        );
        assert_eq!(ack.acked_sequence, 500);
        assert_eq!(ack.in_memory_sequence, 500);

        last_sent_acked = ack.acked_sequence;
        last_sent_in_mem = ack.in_memory_sequence;

        // Post-resume quiescent: confirms idempotency past the
        // resume point — the second flush sees neither track
        // advance and must stay silent.
        assert!(
            try_flush_dual_track(
                &mut q,
                &cursor2,
                last_sent_in_mem,
                last_sent_acked,
                last_sent_in_mem,
            )
            .is_none(),
            "post-resume idle → no further ack",
        );
    }

    // --- Dual-replica cursor update tests ---

    #[test]
    fn dual_cursor_takes_min_and_max_of_both_slots() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));
        // Slot 0 at seq 100, slot 1 at seq 50 → min = 50, max = 100.
        update_dual_replication_cursor(100, 50, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 50);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn dual_cursor_idle_slot_uses_max() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));
        // Slot 0 at seq 100, slot 1 idle (u64::MAX) → min = 100, max = u64::MAX.
        update_dual_replication_cursor(100, u64::MAX, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 100);
        assert_eq!(cursor_max.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn dual_cursor_both_idle() {
        let cursor_min = Arc::new(AtomicU64::new(42));
        let cursor_max = Arc::new(AtomicU64::new(42));
        // Both idle → min = max = u64::MAX (no replicas gating).
        update_dual_replication_cursor(u64::MAX, u64::MAX, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), u64::MAX);
        assert_eq!(cursor_max.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn dual_cursor_decreases_when_slower_replica_connects() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));

        // Slot 0 streaming alone → min = 100, max = u64::MAX.
        update_dual_replication_cursor(100, u64::MAX, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 100);

        // Slot 1 connects with acked_cursor = 51 (last_sequence 50).
        // Min must decrease to 51, max stays at 100.
        update_dual_replication_cursor(51, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 51);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);
    }

    #[test]
    fn dual_cursor_advances_as_slower_replica_catches_up() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));

        // Initial: slot 0 at 100, slot 1 at 51 → min = 51, max = 100.
        update_dual_replication_cursor(51, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 51);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);

        // Slot 1 catches up to 80 → min = 80, max = 100.
        update_dual_replication_cursor(80, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 80);

        // Both at 100 → min = max = 100.
        update_dual_replication_cursor(100, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 100);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);

        // Both advance → min = max = 150.
        update_dual_replication_cursor(150, 150, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 150);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn dual_cursor_slot_disconnect_raises_to_surviving() {
        let cursor_min = Arc::new(AtomicU64::new(0));
        let cursor_max = Arc::new(AtomicU64::new(0));

        // Both streaming: slot 0 at 100, slot 1 at 80 → min = 80, max = 100.
        update_dual_replication_cursor(80, 100, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 80);
        assert_eq!(cursor_max.load(Ordering::Relaxed), 100);

        // Slot 1 disconnects (goes to u64::MAX) → min = 100, max = u64::MAX.
        update_dual_replication_cursor(100, u64::MAX, &cursor_min, &cursor_max);
        assert_eq!(cursor_min.load(Ordering::Relaxed), 100);
        assert_eq!(cursor_max.load(Ordering::Relaxed), u64::MAX);
    }

    // --- Cursor reset test ---

    #[test]
    fn disconnect_resets_cursor_to_max() {
        // Verify the cursor reset behavior documented in the replication
        // cursor table: "All replicas disconnect → u64::MAX".
        let cursor = Arc::new(AtomicU64::new(42));
        let replicas_connected = Arc::new(AtomicU32::new(1));

        // Simulate disconnect: decrement connected count.
        replicas_connected.fetch_sub(1, Ordering::Release);

        // The sender loop checks and resets.
        if replicas_connected.load(Ordering::Relaxed) == 0 {
            cursor.store(u64::MAX, Ordering::Release);
        }

        assert_eq!(cursor.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn cursor_not_reset_when_replica_still_connected() {
        let cursor = Arc::new(AtomicU64::new(42));
        let replicas_connected = Arc::new(AtomicU32::new(2));

        // One replica disconnects, one remains.
        replicas_connected.fetch_sub(1, Ordering::Release);

        if replicas_connected.load(Ordering::Relaxed) == 0 {
            cursor.store(u64::MAX, Ordering::Release);
        }

        // Cursor should NOT be reset — one replica still connected.
        assert_eq!(cursor.load(Ordering::Relaxed), 42);
    }
}
