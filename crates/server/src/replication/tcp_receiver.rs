//! TCP replication receiver (replica side).
//!
//! Connects to the primary, authenticates, performs catch-up / snapshot
//! recovery, and runs the io_uring streaming receive loop. Builds the
//! replica's local pipeline (journal + matching engine + drain stages)
//! to apply incoming events and ack durable batches back to the primary.

use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{debug, error, info, warn};

/// Force the kernel to send a TCP ACK immediately rather than holding
/// it in the delayed-ACK timer (~40 ms on Linux). Linux clears
/// `TCP_QUICKACK` after each ACK it sends, so this must be re-armed
/// after every received batch — otherwise the next ACK falls back to
/// delayed-ACK behavior. Best-effort: a failure here only costs
/// latency, not correctness.
#[inline]
fn arm_tcp_quickack(fd: RawFd) {
    let on: libc::c_int = 1;
    // SAFETY: fd is a live socket fd owned by the caller for the
    // lifetime of this call; the option pointer is to a stack-local
    // i32 with the right size.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_QUICKACK,
            &on as *const libc::c_int as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        // Discarded errno: TCP_QUICKACK is best-effort. The hot
        // re-arm path runs once per RECV completion and we don't
        // want to allocate a tracing event for every received
        // batch in the steady state.
        let _ = rc;
    }
}

use super::auth::authenticate_with_primary;
use super::protocol::{
    Ack, Handshake, MAX_CONTROL_FRAME, MAX_DATA_FRAME, PrimaryMessage, decode_primary_message,
    encode_ack, encode_handshake, read_frame, try_decode_data_batch,
};
use crate::amortized_timer::AmortizedTimer;

use super::{
    PendingAckQueue, log_tcp_info, pin_replica_thread, shutdown_pipeline, sleep_checking_flags,
    submit_batch_to_pipeline,
};

/// io_uring streaming receive loop for the replica.
///
/// Uses `IORING_OP_RECV_MULTI` against a 16-buffer provided buffer pool
/// for incoming `DataBatch` frames, so the kernel can deliver multiple
/// completions while the receive thread is decoding the previous one.
/// Acks are sent via single-shot SEND when the gating condition is
/// satisfied:
///
/// - Sync mode (default): the local journal cursor must advance past
///   the batch's target sequence — guarantees the data is fsynced on
///   this replica's disk before the primary considers it durable.
/// - Async mode (`async_ack = true`, set by `--async-replica-ack`):
///   pop and ack as soon as the previous SEND completes; durability is
///   asserted by "data is queued for the local journal stage" rather
///   than "data is on disk." Removes ~50–80µs from the replication
///   round-trip; see `docs/replication.md` for the failure-mode analysis.
///
/// Frame parsing uses the same accumulate-and-extract pattern as the
/// bench client and reader.
#[allow(clippy::too_many_arguments)]
fn replica_stream_uring(
    tcp_stream: &TcpStream,
    input_producer: &melin_disruptor::ring::MultiProducer<
        melin_engine::journal::pipeline::InputSlot,
    >,
    journal_cursor: &melin_disruptor::padding::Sequence,
    pending_acks: &mut PendingAckQueue,
    received_data: &mut bool,
    journal_accum: &mut Vec<u8>,
    accum_end_sequence: &mut u64,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    async_ack: bool,
) -> SessionExit {
    use io_uring::{IoUring, opcode, types};
    use std::os::unix::io::AsRawFd;

    const TOKEN_RECV: u64 = 0;
    const TOKEN_SEND: u64 = 1;
    const TOKEN_PROVIDE: u64 = 2;

    // Multishot RECV buffer pool. The kernel selects a buffer per
    // completion from this pool, eliminating per-recv resubmission and
    // letting multiple cqes queue up while the receive thread is
    // CPU-bound on decode. Decouples network arrival from parse.
    //
    // 16 buffers × MAX_DATA_FRAME each. The pool only needs to absorb
    // bursts that arrive during a single parse pass; 16 frames is more
    // than enough at any realistic batch rate.
    const NUM_RECV_BUFFERS: u16 = 16;
    const RECV_BUF_SIZE: usize = MAX_DATA_FRAME;
    const RECV_BUF_GROUP_ID: u16 = 0;

    // CQE flag bits (io_uring ABI; not exposed by io-uring crate constants).
    const IORING_CQE_F_BUFFER: u32 = 1 << 0;
    const IORING_CQE_F_MORE: u32 = 1 << 1;
    const IORING_CQE_BUFFER_SHIFT: u32 = 16;

    let tcp_fd = tcp_stream.as_raw_fd();

    // SQ depth must accommodate: 1 multishot RECV, 1 ack SEND, plus
    // up to NUM_RECV_BUFFERS ProvideBuffers re-provisions per loop
    // iteration in the worst case. 64 is comfortable headroom.
    let mut ring: IoUring = match IoUring::builder().setup_single_issuer().build(64) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "io_uring init failed for replica receiver");
            return SessionExit::Disconnected;
        }
    };

    if let Err(e) = ring.submitter().register_files(&[tcp_fd]) {
        tracing::error!(error = %e, "io_uring register_files failed");
        return SessionExit::Disconnected;
    }

    // Arm TCP_QUICKACK so the kernel ACKs incoming WAL batches
    // immediately instead of waiting on the delayed-ACK timer. The
    // sender's TCP send window can't advance until the ACK lands;
    // a 40 ms delay here directly bottlenecks replication throughput.
    // Linux clears the flag after each ACK, so we re-arm it on every
    // RECV completion below.
    arm_tcp_quickack(tcp_fd);

    // Pin io-wq workers to core 0.
    {
        let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        unsafe { libc::CPU_SET(0, &mut cpuset) };
        let _ = ring.submitter().register_iowq_aff(&cpuset);
    }

    // Provided buffer pool for multishot RECV. Vec<u8> backs the pool;
    // the kernel reads/writes via raw pointers at fixed offsets, so the
    // Vec must not move (no reallocation) for the lifetime of the loop.
    let mut recv_pool: Vec<u8> = vec![0u8; NUM_RECV_BUFFERS as usize * RECV_BUF_SIZE];
    let pool_ptr = recv_pool.as_mut_ptr();

    // Register all buffers with io_uring as one group via ProvideBuffers.
    {
        let sqe = opcode::ProvideBuffers::new(
            pool_ptr,
            RECV_BUF_SIZE as i32,
            NUM_RECV_BUFFERS,
            RECV_BUF_GROUP_ID,
            0,
        )
        .build()
        .user_data(TOKEN_PROVIDE);
        unsafe { ring.submission().push(&sqe).expect("SQ full") };
        if let Err(e) = ring.submit_and_wait(1) {
            error!(error = %e, "ProvideBuffers submit failed");
            return SessionExit::Disconnected;
        }
        let cqe = match ring.completion().next() {
            Some(c) => c,
            None => {
                error!("no cqe after ProvideBuffers");
                return SessionExit::Disconnected;
            }
        };
        if cqe.result() < 0 {
            error!(rc = cqe.result(), "ProvideBuffers failed");
            return SessionExit::Disconnected;
        }
    }

    let mut parse_buf: Vec<u8> = Vec::with_capacity(MAX_DATA_FRAME + 4);
    let mut ack_send_buf: Vec<u8> = Vec::with_capacity(64);
    let mut ack_send_offset: usize = 0;
    let mut ack_send_in_flight = false;
    let mut idle_spins: u32 = 0;
    // Submit initial multishot RECV. The kernel will produce CQEs
    // continuously until EOF, error, or buffer pool exhaustion.
    let sqe = opcode::RecvMulti::new(types::Fd(tcp_fd), RECV_BUF_GROUP_ID)
        .build()
        .user_data(TOKEN_RECV);
    unsafe { ring.submission().push(&sqe).expect("SQ full") };
    let mut multishot_active = true;

    // Diagnostic: once a second dump userspace queue depths and the
    // kernel's view of the socket (RUST_LOG=debug). Under healthy
    // streaming parse_buf stays near-empty, pending_acks holds ~1
    // entry, ack_send_in_flight is 0–1. Deviations tell us where data
    // is piling up during a slowdown. `AmortizedTimer` keeps the
    // per-iteration cost to a single `AND` + predictable branch.
    let mut info_log_timer = AmortizedTimer::new();
    let mut bytes_received_since_log: u64 = 0;
    let mut acks_sent_since_log: u64 = 0;

    loop {
        // --- Check flags ---
        if shutdown.load(Ordering::Relaxed) {
            info!("replica shutting down");
            return SessionExit::Shutdown;
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered — stopping replication, transitioning to primary");
            // Drain any complete frames already in parse_buf for
            // maximum data freshness during promotion.
            let mut cursor = 0;
            while cursor + 4 <= parse_buf.len() {
                let frame_len =
                    u32::from_le_bytes(parse_buf[cursor..cursor + 4].try_into().unwrap()) as usize;
                if frame_len == 0
                    || frame_len > MAX_DATA_FRAME
                    || cursor + 4 + frame_len > parse_buf.len()
                {
                    break;
                }
                let payload = &parse_buf[cursor + 4..cursor + 4 + frame_len];
                // Same fast path as the steady-state loop — promotion
                // drain runs once per failover, so the saved allocation
                // only matters if there's a large pre-promotion backlog
                // in parse_buf. Consistency with the main loop keeps
                // the two code paths in sync.
                if let Some((end_sequence, journal_bytes)) = try_decode_data_batch(payload) {
                    journal_accum.extend_from_slice(journal_bytes);
                    *accum_end_sequence = end_sequence;
                }
                cursor += 4 + frame_len;
            }
            // Submit any accumulated data before returning.
            if !journal_accum.is_empty() && !pending_acks.is_full() {
                if let Ok(target) = submit_batch_to_pipeline(journal_accum, input_producer) {
                    pending_acks.push(target, *accum_end_sequence);
                }
                journal_accum.clear();
            }
            return SessionExit::Promote;
        }

        // --- Flush acks ---
        // Sync mode (default): wait for the journal cursor to advance past
        // each pending batch's target before acking — guarantees the data
        // is fsynced locally before the primary considers this replica
        // durable.
        //
        // Async mode (`--async-replica-ack`): pop the oldest pending ack
        // ignoring the journal cursor — acks as soon as the SEND slot is
        // free. Removes ~50–80µs of fsync latency from the critical path
        // at the cost of weaker per-replica durability semantics; see the
        // CLI flag docs for the failure-mode analysis.
        let ready_seq = if !ack_send_in_flight {
            if async_ack {
                pending_acks.pop_all_async()
            } else {
                pending_acks.pop_ready(journal_cursor)
            }
        } else {
            None
        };
        if let Some(seq) = ready_seq {
            ack_send_buf.clear();
            encode_ack(
                &Ack {
                    acked_sequence: seq,
                },
                &mut ack_send_buf,
            );
            let sqe = opcode::Send::new(
                types::Fixed(0),
                ack_send_buf.as_ptr(),
                ack_send_buf.len() as u32,
            )
            .build()
            .user_data(TOKEN_SEND);
            unsafe { ring.submission().push(&sqe).expect("SQ full") };
            ack_send_in_flight = true;
            ack_send_offset = 0;
            acks_sent_since_log += 1;
        }

        // --- Backpressure: if pending_acks full, drain in-flight SEND
        // then pop + send the oldest ack. Must not pop while a SEND is
        // in-flight — the popped sequence would be lost (no buffer to
        // defer it).
        if pending_acks.is_full() {
            // Wait for any in-flight ack SEND to complete first.
            // Collect CQEs into stack buffer to avoid CQ/SQ borrow conflict.
            while ack_send_in_flight {
                let _ = ring.submit();
                let mut bp_cqes: [(u64, i32, u32); 16] = [(0, 0, 0); 16];
                let mut bp_count = 0;
                {
                    // Drain only what fits in the local buffer; iterating
                    // past the cap would consume cqes from the CQ and
                    // silently drop their data — including the buffer id
                    // for multishot recv, which would leak provided buffers.
                    let mut cq = ring.completion();
                    while bp_count < bp_cqes.len() {
                        match cq.next() {
                            Some(cqe) => {
                                bp_cqes[bp_count] = (cqe.user_data(), cqe.result(), cqe.flags());
                                bp_count += 1;
                            }
                            None => break,
                        }
                    }
                }
                for &(bp_token, bp_result, bp_flags) in &bp_cqes[..bp_count] {
                    match bp_token {
                        TOKEN_PROVIDE => {
                            if bp_result < 0 {
                                debug!(
                                    "ProvideBuffers re-provision failed during backpressure drain: {bp_result}"
                                );
                            }
                        }
                        TOKEN_SEND => {
                            if bp_result < 0 {
                                warn!("ack send error during backpressure drain");
                                return SessionExit::Disconnected;
                            }
                            let sent = bp_result as usize;
                            ack_send_offset += sent;
                            if ack_send_offset >= ack_send_buf.len() {
                                ack_send_buf.clear();
                                ack_send_offset = 0;
                                ack_send_in_flight = false;
                            } else {
                                let sqe = opcode::Send::new(
                                    types::Fixed(0),
                                    ack_send_buf[ack_send_offset..].as_ptr(),
                                    (ack_send_buf.len() - ack_send_offset) as u32,
                                )
                                .build()
                                .user_data(TOKEN_SEND);
                                unsafe { ring.submission().push(&sqe).expect("SQ full") };
                            }
                        }
                        TOKEN_RECV => {
                            // Stash RECV CQE data into parse_buf for later
                            // processing — don't handle frames here to keep
                            // the backpressure drain simple. Mirror the
                            // multishot handling from the main loop.
                            if (bp_flags & IORING_CQE_F_MORE) == 0 {
                                multishot_active = false;
                            }
                            if bp_result < 0 {
                                if bp_result == -libc::ENOBUFS {
                                    debug!("recv multishot ENOBUFS during backpressure drain");
                                    continue;
                                }
                                warn!("primary disconnected during backpressure drain");
                                return SessionExit::Disconnected;
                            }
                            if bp_result == 0 {
                                warn!("primary disconnected during backpressure drain");
                                return SessionExit::Disconnected;
                            }
                            if (bp_flags & IORING_CQE_F_BUFFER) == 0 {
                                error!("recv cqe missing F_BUFFER flag in drain");
                                return SessionExit::Disconnected;
                            }
                            arm_tcp_quickack(tcp_fd);
                            let n = bp_result as usize;
                            let buf_id = (bp_flags >> IORING_CQE_BUFFER_SHIFT) as usize;
                            let buf_ptr = unsafe { pool_ptr.add(buf_id * RECV_BUF_SIZE) };
                            // SAFETY: same invariant as the main loop —
                            // kernel wrote `n` bytes into our owned pool
                            // at this offset.
                            let slice = unsafe { std::slice::from_raw_parts(buf_ptr, n) };
                            parse_buf.extend_from_slice(slice);
                            // Re-provide the buffer.
                            let provide_sqe = opcode::ProvideBuffers::new(
                                buf_ptr,
                                RECV_BUF_SIZE as i32,
                                1,
                                RECV_BUF_GROUP_ID,
                                buf_id as u16,
                            )
                            .build()
                            .user_data(TOKEN_PROVIDE);
                            unsafe { ring.submission().push(&provide_sqe).expect("SQ full") };
                        }
                        _ => {}
                    }
                }
                std::hint::spin_loop();
            }

            // After draining the in-flight SEND, drop all pending acks at
            // once. In sync mode, wait for the journal to catch up first;
            // in async mode, ack immediately without waiting on fsync.
            let seq = if async_ack {
                pending_acks
                    .pop_all_async()
                    .expect("non-empty queue after backpressure drain")
            } else {
                pending_acks.pop_oldest_blocking(journal_cursor)
            };
            ack_send_buf.clear();
            encode_ack(
                &Ack {
                    acked_sequence: seq,
                },
                &mut ack_send_buf,
            );
            let sqe = opcode::Send::new(
                types::Fixed(0),
                ack_send_buf.as_ptr(),
                ack_send_buf.len() as u32,
            )
            .build()
            .user_data(TOKEN_SEND);
            unsafe { ring.submission().push(&sqe).expect("SQ full") };
            ack_send_in_flight = true;
            ack_send_offset = 0;
            acks_sent_since_log += 1;
        }

        // Periodic userspace + TCP summary (debug level). Amortized to
        // ~1 Hz with negligible per-iteration cost — gives a
        // time-aligned view of bytes/sec RECV rate, ack submission
        // rate, parse_buf accumulation (user-space queue from RECV to
        // push), pending_acks depth (journal fsync wait queue,
        // typically ~1), and in-flight SEND state.
        if let Some(elapsed) = info_log_timer.tick(std::time::Duration::from_secs(1)) {
            let secs = elapsed.as_secs_f64();
            tracing::debug!(
                bytes_per_sec = (bytes_received_since_log as f64 / secs) as u64,
                acks_per_sec = (acks_sent_since_log as f64 / secs) as u64,
                parse_buf_len = parse_buf.len(),
                pending_acks_len = pending_acks.len(),
                ack_send_in_flight,
                "replica userspace"
            );
            log_tcp_info(tcp_fd, "replica_recv", 0);
            bytes_received_since_log = 0;
            acks_sent_since_log = 0;
        }

        // --- Submit SQEs and drain CQEs ---
        if let Err(e) = ring.submit() {
            tracing::error!(error = %e, "io_uring submit failed");
            return SessionExit::Disconnected;
        }

        let mut cqes: [(u64, i32, u32); 16] = [(0, 0, 0); 16];
        let mut cqe_count = 0;
        {
            // Drain only what fits in the local buffer; iterating past
            // the cap would consume cqes from the CQ and silently drop
            // their data — including the buffer id for multishot recv,
            // which would leak provided buffers and corrupt parse_buf.
            let mut cq = ring.completion();
            while cqe_count < cqes.len() {
                match cq.next() {
                    Some(cqe) => {
                        cqes[cqe_count] = (cqe.user_data(), cqe.result(), cqe.flags());
                        cqe_count += 1;
                    }
                    None => break,
                }
            }
        }

        let any_cqe = cqe_count > 0;
        for &(token, result, flags) in &cqes[..cqe_count] {
            idle_spins = 0;
            match token {
                TOKEN_RECV => {
                    // Track multishot termination — when F_MORE is not
                    // set the kernel will not produce more cqes for
                    // this submission, so we must resubmit below.
                    if (flags & IORING_CQE_F_MORE) == 0 {
                        multishot_active = false;
                    }
                    if result < 0 {
                        // ENOBUFS means the provided buffer pool was
                        // exhausted; re-provisions are in flight, just
                        // resubmit. Other errors are fatal.
                        if result == -libc::ENOBUFS {
                            debug!("recv multishot ENOBUFS — pool exhausted, will resubmit");
                            continue;
                        }
                        warn!("primary disconnected (recv returned {result})");
                        return SessionExit::Disconnected;
                    }
                    if result == 0 {
                        warn!("primary disconnected (recv returned 0)");
                        return SessionExit::Disconnected;
                    }
                    if (flags & IORING_CQE_F_BUFFER) == 0 {
                        error!("recv cqe missing F_BUFFER flag");
                        return SessionExit::Disconnected;
                    }
                    // Re-arm TCP_QUICKACK: Linux clears it after each
                    // kernel ACK, so we have to set it again for the
                    // ACK that this recv just generated to bypass the
                    // delayed-ACK timer.
                    arm_tcp_quickack(tcp_fd);
                    let n = result as usize;
                    bytes_received_since_log += n as u64;
                    let buf_id = (flags >> IORING_CQE_BUFFER_SHIFT) as usize;
                    let buf_ptr = unsafe { pool_ptr.add(buf_id * RECV_BUF_SIZE) };
                    // SAFETY: kernel wrote `n` bytes (n = result) into
                    // the buffer at offset `buf_id * RECV_BUF_SIZE`,
                    // which is within `recv_pool`. We own the pool and
                    // the buffer is not in flight until re-provided.
                    let slice = unsafe { std::slice::from_raw_parts(buf_ptr, n) };
                    parse_buf.extend_from_slice(slice);
                    // Re-provide the consumed buffer to the pool.
                    let provide_sqe = opcode::ProvideBuffers::new(
                        buf_ptr,
                        RECV_BUF_SIZE as i32,
                        1,
                        RECV_BUF_GROUP_ID,
                        buf_id as u16,
                    )
                    .build()
                    .user_data(TOKEN_PROVIDE);
                    unsafe { ring.submission().push(&provide_sqe).expect("SQ full") };

                    // Extract complete frames from parse_buf.
                    let mut cursor = 0;
                    while cursor + 4 <= parse_buf.len() {
                        let frame_len =
                            u32::from_le_bytes(parse_buf[cursor..cursor + 4].try_into().unwrap())
                                as usize;
                        if frame_len == 0 || frame_len > MAX_DATA_FRAME {
                            warn!(frame_len, "invalid frame length from primary");
                            return SessionExit::Disconnected;
                        }
                        if cursor + 4 + frame_len > parse_buf.len() {
                            break; // Incomplete frame — wait for more data.
                        }
                        let payload = &parse_buf[cursor + 4..cursor + 4 + frame_len];
                        // Fast path: steady-state traffic is ~100% DataBatch
                        // frames. `try_decode_data_batch` returns a slice
                        // borrowed directly from `parse_buf`, avoiding the
                        // ~40 KB Vec allocation that the general decoder
                        // would perform on every batch.
                        if let Some((end_sequence, journal_bytes)) = try_decode_data_batch(payload)
                        {
                            *received_data = true;
                            journal_accum.extend_from_slice(journal_bytes);
                            *accum_end_sequence = end_sequence;
                        } else {
                            // Control messages (heartbeat, need-snapshot,
                            // hash-mismatch) fall through to the general
                            // decoder. Rare compared to DataBatch, so the
                            // allocation cost here is irrelevant.
                            match decode_primary_message(payload) {
                                Ok(PrimaryMessage::Heartbeat { sequence }) => {
                                    debug!(sequence, "heartbeat from primary");
                                }
                                Ok(PrimaryMessage::NeedSnapshot) => {
                                    return SessionExit::Fatal(
                                        "primary says we need a snapshot transfer mid-stream"
                                            .into(),
                                    );
                                }
                                Ok(PrimaryMessage::HashMismatch) => {
                                    return SessionExit::Fatal(
                                        "chain hash mismatch from primary".into(),
                                    );
                                }
                                Ok(PrimaryMessage::DataBatch { .. }) => {
                                    // `try_decode_data_batch` rejected this
                                    // payload as too short for the fixed
                                    // header, so the general decoder should
                                    // have surfaced it as an error. Reach
                                    // here means the general decoder accepted
                                    // it — treat as a protocol violation.
                                    warn!("malformed DataBatch slipped past fast path");
                                    return SessionExit::Disconnected;
                                }
                                Ok(_) => {
                                    debug!("unexpected message during streaming");
                                }
                                Err(e) => {
                                    warn!(error = %e, "failed to decode primary message");
                                    return SessionExit::Disconnected;
                                }
                            }
                        }
                        cursor += 4 + frame_len;
                    }
                    // Compact parse_buf.
                    if cursor > 0 {
                        let remaining = parse_buf.len() - cursor;
                        parse_buf.copy_within(cursor.., 0);
                        parse_buf.truncate(remaining);
                    }

                    // Submit accumulated data to pipeline (if room in pending acks).
                    if !journal_accum.is_empty() && !pending_acks.is_full() {
                        match submit_batch_to_pipeline(journal_accum, input_producer) {
                            Ok(target) => {
                                pending_acks.push(target, *accum_end_sequence);
                                journal_accum.clear();
                            }
                            Err(e) => {
                                error!(error = %e, "failed to submit batch to pipeline");
                                return SessionExit::Disconnected;
                            }
                        }
                    }
                }

                TOKEN_PROVIDE => {
                    // Best-effort re-provision; failures only manifest
                    // as ENOBUFS later, which we handle by resubmitting.
                    if result < 0 {
                        debug!("ProvideBuffers re-provision failed: {result}");
                    }
                }

                TOKEN_SEND => {
                    if result < 0 {
                        warn!("ack send error (returned {result})");
                        return SessionExit::Disconnected;
                    }
                    let sent = result as usize;
                    ack_send_offset += sent;
                    if ack_send_offset >= ack_send_buf.len() {
                        ack_send_buf.clear();
                        ack_send_offset = 0;
                        ack_send_in_flight = false;
                    } else {
                        // Partial send — resubmit remainder.
                        let sqe = opcode::Send::new(
                            types::Fixed(0),
                            ack_send_buf[ack_send_offset..].as_ptr(),
                            (ack_send_buf.len() - ack_send_offset) as u32,
                        )
                        .build()
                        .user_data(TOKEN_SEND);
                        unsafe { ring.submission().push(&sqe).expect("SQ full") };
                    }
                }

                _ => {}
            }
        }

        // --- Resubmit multishot if terminated ---
        // Multishot can terminate on transient conditions (ENOBUFS,
        // kernel buffer pool reset). Re-arm so the kernel keeps
        // pushing cqes as data arrives.
        if !multishot_active {
            let sqe = opcode::RecvMulti::new(types::Fd(tcp_fd), RECV_BUF_GROUP_ID)
                .build()
                .user_data(TOKEN_RECV);
            unsafe { ring.submission().push(&sqe).expect("SQ full") };
            multishot_active = true;
        }

        // --- Idle wait ---
        if !any_cqe {
            if idle_spins < 1000 {
                idle_spins = idle_spins.wrapping_add(1);
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
        }
    }
}

fn send_ack_tcp(
    acked_sequence: u64,
    writer: &mut TcpStream,
    send_buf: &mut Vec<u8>,
) -> io::Result<()> {
    encode_ack(&Ack { acked_sequence }, send_buf);
    writer.write_all(send_buf)?;
    writer.flush()?;
    send_buf.clear();
    Ok(())
}

/// Outcome of the inner streaming receive loop.
enum SessionExit {
    Shutdown,
    Promote,
    Disconnected,
    Fatal(Box<dyn std::error::Error>),
}

/// Run the replication receiver. Connects to a primary, receives journal
/// entries, persists them locally, replays into the Exchange, and sends acks.
///
/// Blocks until the connection drops or shutdown is signaled.
/// Result of `run_receiver`: `None` = clean shutdown, `Some` = promotion
/// triggered with the fully-replayed Exchange and positioned JournalWriter.
pub type ReceiverResult = Result<
    Option<(
        melin_engine::exchange::Exchange,
        melin_engine::journal::JournalWriter,
    )>,
    Box<dyn std::error::Error>,
>;

#[allow(clippy::too_many_arguments)]
pub fn run_receiver(
    primary_addr: SocketAddr,
    journal_path: &std::path::Path,
    signing_key: &ed25519_dalek::SigningKey,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_secs: u64,
    snapshot_path: std::path::PathBuf,
    cores: crate::server::PipelineCores,
    async_ack: bool,
) -> ReceiverResult {
    use melin_engine::exchange::Exchange;
    use melin_engine::journal::JournalWriter;

    // Recover local state from journal (if any). On first call this may
    // be (None, None) for a fresh replica. After a reconnect, the pipeline
    // shutdown returns the Exchange + JournalWriter directly.
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        if journal_path.exists() {
            let engine = if snapshot_path.exists() {
                info!("recovering replica from snapshot + journal");
                melin_engine::journal::JournaledExchange::recover_from_snapshot(
                    &snapshot_path,
                    journal_path,
                )?
            } else {
                melin_engine::journal::JournaledExchange::recover(journal_path)?
            };
            let next = engine.next_sequence();
            let last = next.saturating_sub(1);
            let hash = engine.writer_chain_hash().unwrap_or([0u8; 32]);
            let (exchange, writer) = engine.into_parts();
            (Some(exchange), Some(writer), last, hash)
        } else {
            (None, None, 0u64, [0u8; 32])
        };

    // Exponential backoff for reconnection: 1s → 2s → 4s → … → 30s max.
    // Reset to 1s on successful streaming (first DataBatch received).
    let mut backoff = std::time::Duration::from_secs(1);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

    // Reusable buffers — survive across reconnections.
    let mut send_buf = Vec::with_capacity(64);
    let mut journal_accum: Vec<u8> = Vec::with_capacity(128 * 1024);
    let mut accum_end_sequence: u64 = 0;

    // --- Outer reconnect loop ---
    //
    // Each iteration: connect → auth → handshake → pipeline → stream.
    // On disconnect (eviction or crash): shut down pipeline, recover
    // Exchange + JournalWriter, backoff, reconnect.
    loop {
        // Check shutdown/promote before attempting to connect.
        if shutdown.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered while disconnected");
            return match (exchange, journal_writer) {
                (Some(e), Some(w)) => Ok(Some((e, w))),
                _ => Err("promotion requested but no local state available".into()),
            };
        }

        // --- Connect and authenticate ---

        info!(primary = %primary_addr, "connecting to primary as replica");

        let stream = match TcpStream::connect(primary_addr) {
            Ok(s) => s,
            Err(e) => {
                warn!(
                    error = %e,
                    backoff_secs = backoff.as_secs(),
                    "failed to connect to primary — retrying"
                );
                sleep_checking_flags(backoff, shutdown, promote);
                if shutdown.load(Ordering::Relaxed) {
                    return Ok(None);
                }
                if promote.load(Ordering::Acquire) {
                    info!("promotion triggered during reconnect backoff");
                    return match (exchange, journal_writer) {
                        (Some(e), Some(w)) => Ok(Some((e, w))),
                        _ => Err("promotion requested but no local state available".into()),
                    };
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
                continue;
            }
        };
        if let Err(e) = stream.set_nodelay(true) {
            // Don't bail — Nagle adds latency but doesn't break correctness.
            // Surface it so a misconfigured kernel doesn't silently kill
            // replication throughput.
            warn!(error = %e, "failed to set TCP_NODELAY on replica receive socket");
        }
        // Enable SO_BUSY_POLL: kernel busy-polls the NIC for incoming data
        // for up to N µs after each blocking read instead of going to sleep.
        // Removes softirq → wakeup handoff latency, which dominates
        // replica recv jitter on high-IRQ-cost NICs (e.g. ixgbe). Costs CPU
        // cycles in the receiver thread — acceptable since we already
        // spin-wait there. 50 µs covers a typical LAN ack RTT.
        {
            use std::os::unix::io::AsRawFd;
            let busy_poll: libc::c_int = 50;
            let rc = unsafe {
                libc::setsockopt(
                    stream.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_BUSY_POLL,
                    &busy_poll as *const libc::c_int as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            if rc != 0 {
                // Best-effort: requires CAP_NET_ADMIN and a NIC driver that
                // supports it. Surface as warn so a misconfigured kernel is
                // visible without halting replication.
                let err = std::io::Error::last_os_error();
                warn!(error = %err, "failed to set SO_BUSY_POLL on replica receive socket");
            }
        }
        stream.set_read_timeout(Some(std::time::Duration::from_secs(5)))?;

        let mut reader = stream.try_clone()?;
        let mut tcp_writer = stream;

        if let Err(e) = authenticate_with_primary(&mut reader, &mut tcp_writer, signing_key) {
            warn!(error = %e, "authentication failed — retrying");
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        info!("authenticated with primary");

        // --- Handshake ---

        let handshake = Handshake {
            last_sequence,
            chain_hash,
        };
        send_buf.clear();
        encode_handshake(&handshake, &mut send_buf);
        tcp_writer.write_all(&send_buf)?;
        tcp_writer.flush()?;
        send_buf.clear();

        // --- Protocol negotiation (StreamStart / NeedSnapshot) ---

        let response_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
        let response = decode_primary_message(&response_frame)?;
        let primary_genesis_entry = match response {
            PrimaryMessage::StreamStart {
                start_sequence,
                genesis_entry,
            } => {
                info!(start_sequence, "streaming started");
                genesis_entry
            }
            PrimaryMessage::NeedSnapshot => {
                info!("primary requires snapshot transfer — receiving snapshot");

                let _ = std::fs::remove_file(journal_path);
                let _ = std::fs::remove_file(&snapshot_path);

                let begin_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
                let (snap_len, snap_sequence, snap_chain_hash) =
                    match decode_primary_message(&begin_frame)? {
                        PrimaryMessage::SnapshotBegin {
                            snapshot_len,
                            snap_sequence,
                            snap_chain_hash,
                        } => (snapshot_len, snap_sequence, snap_chain_hash),
                        other => {
                            return Err(format!("expected SnapshotBegin, got {other:?}").into());
                        }
                    };

                info!(snap_sequence, snap_len, "receiving snapshot");

                let tmp_path = snapshot_path.with_extension("snapshot.tmp");
                {
                    let mut tmp_file = std::fs::File::create(&tmp_path)?;
                    let mut received: u64 = 0;
                    let mut running_crc: u32 = 0;
                    loop {
                        let chunk_frame = read_frame(&mut reader, MAX_DATA_FRAME)?;
                        match decode_primary_message(&chunk_frame)? {
                            PrimaryMessage::SnapshotChunk(data) => {
                                std::io::Write::write_all(&mut tmp_file, &data)?;
                                received += data.len() as u64;
                                running_crc = crc32c::crc32c_append(running_crc, &data);
                            }
                            PrimaryMessage::SnapshotEnd {
                                crc32c: expected_crc,
                            } => {
                                tmp_file.sync_all()?;
                                drop(tmp_file);

                                if received != snap_len {
                                    let _ = std::fs::remove_file(&tmp_path);
                                    return Err(format!(
                                        "snapshot length mismatch: expected {snap_len} bytes, got {received}"
                                    )
                                    .into());
                                }

                                if running_crc != expected_crc {
                                    let _ = std::fs::remove_file(&tmp_path);
                                    return Err(format!(
                                        "snapshot CRC mismatch: expected {expected_crc:#x}, got {running_crc:#x}"
                                    )
                                    .into());
                                }

                                std::fs::rename(&tmp_path, &snapshot_path)?;
                                info!(snap_sequence, received, "snapshot received and verified");
                                break;
                            }
                            other => {
                                let _ = std::fs::remove_file(&tmp_path);
                                return Err(
                                    format!("expected SnapshotChunk/End, got {other:?}").into()
                                );
                            }
                        }
                    }
                }

                let (snap_exchange, snap_seq, snap_hash) =
                    melin_engine::journal::snapshot::load(&snapshot_path)?;
                if snap_hash != snap_chain_hash {
                    return Err(format!(
                        "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
                         loaded snapshot has {snap_hash:02x?}"
                    )
                    .into());
                }
                exchange = Some(snap_exchange);

                let writer =
                    JournalWriter::create_continuing(journal_path, snap_seq + 1, snap_hash)?;
                journal_writer = Some(writer);

                let ss_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
                match decode_primary_message(&ss_frame)? {
                    PrimaryMessage::StreamStart {
                        start_sequence,
                        genesis_entry,
                    } => {
                        info!(start_sequence, "streaming resumed after snapshot transfer");
                        genesis_entry
                    }
                    other => {
                        return Err(
                            format!("expected StreamStart after snapshot, got {other:?}").into(),
                        );
                    }
                }
            }
            PrimaryMessage::HashMismatch => {
                return Err("chain hash mismatch — replica has divergent history".into());
            }
            _ => {
                return Err(format!("unexpected response: {response:?}").into());
            }
        };

        // --- Create journal for fresh replica (first connection only) ---

        if journal_writer.is_none() {
            use melin_engine::journal::codec::{self as journal_codec, FILE_HEADER_SIZE};
            use std::fs::OpenOptions;
            use std::os::unix::fs::FileExt;

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(journal_path)?;
            let mut header = [0u8; FILE_HEADER_SIZE];
            journal_codec::encode_file_header(&mut header);
            file.write_all_at(&header, 0)?;
            file.write_all_at(&primary_genesis_entry, FILE_HEADER_SIZE as u64)?;
            file.sync_all()?;

            let genesis_chain_hash = {
                let entry_len = primary_genesis_entry.len();
                let hash = blake3::hash(&primary_genesis_entry[..entry_len - 4]);
                *hash.as_bytes()
            };

            let valid_end = FILE_HEADER_SIZE as u64 + primary_genesis_entry.len() as u64;
            let writer = JournalWriter::open_append(
                journal_path,
                1, // genesis consumed sequence 1
                valid_end,
                Some(genesis_chain_hash),
                0, // events_since_checkpoint
            )?;
            exchange = Some(Exchange::new());
            journal_writer = Some(writer);
        }

        let cur_exchange = exchange.take().expect("exchange initialized");
        let cur_writer = journal_writer.take().expect("journal_writer initialized");

        // --- Build pipeline and spawn threads ---

        let shadow_exchange = cur_exchange.clone_via_snapshot();

        let enable_shadow = snapshot_interval_secs > 0;
        let pipeline = melin_engine::journal::pipeline::build_replica_pipeline(
            cur_exchange,
            cur_writer,
            4096,  // max_journal_batch
            false, // don't busy-spin on replica
            enable_shadow,
        );
        let input_producer = pipeline.input_producer;
        let journal_stage = pipeline.journal_stage;
        let matching_stage = pipeline.matching_stage;
        let drain_consumer = pipeline.drain_consumer;
        let journal_cursor = pipeline.journal_cursor;
        let shadow_consumer = pipeline.shadow_consumer;
        let chain_hash_lock = pipeline.chain_hash_lock;

        let pipeline_shutdown = Arc::new(AtomicBool::new(false));

        let ps = Arc::clone(&pipeline_shutdown);
        let journal_core = cores.journal;
        let journal_handle = std::thread::Builder::new()
            .name("journal".into())
            .spawn(move || {
                pin_replica_thread("journal", journal_core);
                journal_stage.run(&ps)
            })
            .expect("spawn journal thread");

        let ps = Arc::clone(&pipeline_shutdown);
        let matching_core = cores.matching;
        let matching_handle = std::thread::Builder::new()
            .name("matching".into())
            .spawn(move || {
                pin_replica_thread("matching", matching_core);
                matching_stage.run(&ps)
            })
            .expect("spawn matching thread");

        // Drain thread uses the response core — on the primary this core
        // runs the response stage, but replicas have no response stage.
        let ps = Arc::clone(&pipeline_shutdown);
        let drain_core = cores.response;
        let drain_handle = std::thread::Builder::new()
            .name("drain".into())
            .spawn(move || {
                pin_replica_thread("drain", drain_core);
                let mut consumer = drain_consumer;
                let mut batch = vec![melin_engine::journal::pipeline::OutputSlot::default(); 256];
                loop {
                    if ps.load(Ordering::Relaxed) {
                        return;
                    }
                    let count = consumer.consume_batch(&mut batch, 256);
                    if count == 0 {
                        std::thread::yield_now();
                    }
                }
            })
            .expect("spawn drain thread");

        let shadow_handle = if let Some(shadow_cons) = shadow_consumer {
            let snap_path = snapshot_path.clone();
            let chain_lock = chain_hash_lock.expect("chain hash lock with shadow");
            let ps = Arc::clone(&pipeline_shutdown);
            let shadow_core = cores.shadow;
            Some(
                std::thread::Builder::new()
                    .name("replica-shadow".into())
                    .spawn(move || {
                        pin_replica_thread("replica-shadow", shadow_core);
                        crate::shadow::run(
                            shadow_cons,
                            shadow_exchange,
                            snap_path,
                            std::time::Duration::from_secs(snapshot_interval_secs),
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

        // --- Inner streaming receive loop ---
        //
        // Exits via `break` with a SessionExit value. All pipeline
        // teardown happens after the loop to avoid ownership issues
        // with thread handles across multiple break paths.

        let mut pending_acks = PendingAckQueue::new();
        let mut received_data = false;

        let exit_reason: SessionExit = replica_stream_uring(
            &tcp_writer,
            &input_producer,
            &journal_cursor,
            &mut pending_acks,
            &mut received_data,
            &mut journal_accum,
            &mut accum_end_sequence,
            shutdown,
            promote,
            async_ack,
        );

        // --- Common teardown (all exit paths) ---

        // Flush any accumulated data not yet submitted.
        if !journal_accum.is_empty() {
            if let Ok(target) = submit_batch_to_pipeline(&journal_accum, &input_producer) {
                pending_acks.push(target, accum_end_sequence);
            }
            journal_accum.clear();
        }
        // Wait for all pending batches to become durable.
        if let Some(seq) = pending_acks.pop_all_blocking(&journal_cursor) {
            let _ = send_ack_tcp(seq, &mut tcp_writer, &mut send_buf);
        }

        // Shut down pipeline and recover state.
        let pipeline_state = shutdown_pipeline(
            &pipeline_shutdown,
            journal_handle,
            matching_handle,
            drain_handle,
            shadow_handle,
        );

        match exit_reason {
            SessionExit::Shutdown => return Ok(None),

            SessionExit::Promote => {
                return match pipeline_state {
                    Some((e, w)) => Ok(Some((e, w))),
                    None => Err("pipeline failed during promotion".into()),
                };
            }

            SessionExit::Fatal(e) => return Err(e),

            SessionExit::Disconnected => {
                // Recover Exchange + JournalWriter for the next iteration.
                match pipeline_state {
                    Some((e, w)) => {
                        last_sequence = w.next_sequence().saturating_sub(1);
                        chain_hash = w.chain_hash().unwrap_or([0u8; 32]);
                        exchange = Some(e);
                        journal_writer = Some(w);
                    }
                    None => {
                        error!("pipeline thread panicked during disconnect recovery");
                        if journal_path.exists() {
                            match melin_engine::journal::JournaledExchange::recover(journal_path) {
                                Ok(engine) => {
                                    last_sequence = engine.next_sequence().saturating_sub(1);
                                    chain_hash = engine.writer_chain_hash().unwrap_or([0u8; 32]);
                                    let (e, w) = engine.into_parts();
                                    exchange = Some(e);
                                    journal_writer = Some(w);
                                }
                                Err(e) => {
                                    return Err(format!(
                                        "pipeline panicked and journal recovery failed: {e}"
                                    )
                                    .into());
                                }
                            }
                        } else {
                            return Err("pipeline panicked and no journal to recover from".into());
                        }
                    }
                }

                if received_data {
                    backoff = std::time::Duration::from_secs(1);
                }

                warn!(
                    last_sequence,
                    backoff_secs = backoff.as_secs(),
                    "reconnecting to primary"
                );
                sleep_checking_flags(backoff, shutdown, promote);
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
}
