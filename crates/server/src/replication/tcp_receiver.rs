//! TCP replication receiver (replica side).
//!
//! Connects to the primary, authenticates, performs catch-up / snapshot
//! recovery, and runs the io_uring streaming receive loop. Builds the
//! replica's local pipeline (journal + matching engine + drain stages)
//! to apply incoming events and ack durable batches back to the primary.

use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{debug, error, info, warn};

use melin_journal::JournalWrite;
use melin_transport_core::pipeline::{JournalStage, JournalStageRun};

use crate::TradingEvent;

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
    encode_ack, encode_handshake, read_frame, try_decode_input_batch, try_decode_input_batch_into,
};
use crate::amortized_timer::AmortizedTimer;

use super::{
    PendingAckQueue, ReplicaPipelineHandles, build_replica_pipeline_with_threads, log_tcp_info,
    sleep_checking_flags, teardown_replica_pipeline, try_flush_dual_track,
};

/// io_uring streaming receive loop for the replica.
///
/// Uses `IORING_OP_RECV_MULTI` against a 16-buffer provided buffer pool
/// for incoming `InputBatch` frames, so the kernel can deliver multiple
/// completions while the receive thread is decoding the previous one.
/// Acks are sent via single-shot SEND when the dual-track flush
/// (`try_flush_dual_track`) reports an advance on either the persisted
/// or in-memory track. The persisted side requires the local journal
/// cursor to have caught up past the batch's target sequence; the
/// in-memory side is advanced on batch receipt and lets `in_memory>=N`
/// clauses on the primary fire without waiting on this replica's
/// fsync.
///
/// Frame parsing uses the same accumulate-and-extract pattern as the
/// bench client and reader.
#[allow(clippy::too_many_arguments)]
fn replica_stream_uring(
    tcp_stream: &TcpStream,
    input_producer: &mut melin_disruptor::ring::Producer<crate::InputSlot>,
    journal_cursor: &melin_disruptor::padding::Sequence,
    pending_acks: &mut PendingAckQueue,
    received_data: &mut bool,
    accum_end_sequence: &mut u64,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    busy_spin: bool,
    slot_buf: &mut Vec<crate::InputSlot>,
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
    // Last cursor pair sent on the wire. Used to coalesce: the flush
    // block fires an ack iff either cursor has advanced past these
    // values since the last send. Cursors are monotonic, so multiple
    // advances during an in-flight SEND collapse into one ack at the
    // next iteration after SEND completes.
    let mut last_sent_acked_seq: u64 = 0;
    let mut last_sent_in_memory_seq: u64 = 0;
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
            let mut last_target: u64 = 0;
            let mut any_published = false;
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
                if let Ok(slots) = try_decode_input_batch(payload) {
                    let mut batch = input_producer.batch();
                    for slot in slots {
                        let primary_seq = slot.sequence;
                        last_target = batch.push_with(|s| *s = slot);
                        *accum_end_sequence = primary_seq;
                        any_published = true;
                    }
                    batch.commit();
                }
                cursor += 4 + frame_len;
            }
            // Submit any accumulated data before returning.
            if any_published && !pending_acks.is_full() {
                pending_acks.push(last_target, *accum_end_sequence);
            }
            return SessionExit::Promote;
        }

        // --- Flush acks (dual-track) ---
        //
        // See `try_flush_dual_track` in `replication/mod.rs` for the
        // persisted-vs-in-memory model. The helper centralises the
        // namespace translation between local-ring positions
        // (`journal_cursor` space) and primary sequences (wire space)
        // so this receiver and the DPDK sibling can't drift on that
        // translation.
        if !ack_send_in_flight
            && let Some(ack) = try_flush_dual_track(
                pending_acks,
                journal_cursor,
                *accum_end_sequence,
                last_sent_acked_seq,
                last_sent_in_memory_seq,
            )
        {
            ack_send_buf.clear();
            encode_ack(&ack, &mut ack_send_buf);
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
            // Update trackers AFTER successful submission. io_uring SEND
            // submission panics on SQ full (no recoverable error path),
            // so reaching this line means the wire send is enqueued.
            last_sent_acked_seq = ack.acked_sequence;
            last_sent_in_memory_seq = ack.in_memory_sequence;
            acks_sent_since_log += 1;
        }

        // --- Backpressure: if pending_acks full, drain in-flight SEND
        // then pop + send the oldest ack. Must not pop while a SEND is
        // in-flight — the popped sequence would be lost (no buffer to
        // defer it).
        if pending_acks.is_full() {
            // Wait for any in-flight ack SEND to complete first.
            // Collect CQEs into stack buffer to avoid CQ/SQ borrow conflict.
            let mut bp_idle_spins: u32 = 0;
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
                // Mirror the main-loop idle wait: with `busy_spin`, never
                // yield (ack RTT sits on the primary's response-gate
                // critical path); otherwise yield after a short spin so a
                // wedged SEND CQE doesn't peg this core under `--yield-idle`
                // (e.g. in CI / failover tests).
                if bp_count == 0 {
                    if busy_spin || bp_idle_spins < 1000 {
                        bp_idle_spins = bp_idle_spins.wrapping_add(1);
                        std::hint::spin_loop();
                    } else {
                        std::thread::yield_now();
                    }
                } else {
                    bp_idle_spins = 0;
                }
            }

            // After draining the in-flight SEND, drop all pending acks at
            // once — waiting for the journal cursor to catch up to the
            // oldest pending target before sending one cumulative ack.
            let seq = pending_acks.pop_oldest_blocking(journal_cursor, busy_spin);
            let in_mem_now = *accum_end_sequence;
            ack_send_buf.clear();
            encode_ack(
                &Ack {
                    acked_sequence: seq,
                    in_memory_sequence: in_mem_now,
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
            // Backpressure-drain just sent an ack carrying (seq,
            // in_mem_now). Update trackers so the next flush-block
            // call doesn't refire — without this the dual-track
            // coalescer would see "in_mem_now > last_sent_in_memory_seq"
            // (or "seq > last_sent_acked_seq") and emit a duplicate
            // ack right after, with the worst case being a wire-side
            // regression of `acked_sequence` if the flush block then
            // popped something smaller.
            last_sent_acked_seq = seq;
            last_sent_in_memory_seq = in_mem_now;
            acks_sent_since_log += 1;
        }

        // Periodic userspace + TCP summary (debug level). Amortized to
        // ~1 Hz with negligible per-iteration cost — gives a
        // time-aligned view of bytes/sec RECV rate, ack submission
        // rate, parse_buf accumulation (user-space queue from RECV to
        // push), pending_acks depth (journal fsync wait queue,
        // typically ~1), and in-flight SEND state.
        if let Some(elapsed) = info_log_timer.tick(
            std::time::Duration::from_secs(1),
            busy_spin || idle_spins < 1000,
        ) {
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
        // Skip the syscall when no SQEs are pending — same reasoning as in
        // the sender: empty io_uring_enter costs ~200 ns of mode-switch
        // overhead and appeared as several percent of total CPU in profiles.
        let pending = ring.submission().len();
        if pending > 0
            && let Err(e) = ring.submit()
        {
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

                    // Extract complete frames from parse_buf and publish
                    // their InputSlots into the local input ring. Track the
                    // burst's max producer-publish target so a single
                    // pending_acks entry covers everything from this CQE.
                    let mut cursor = 0;
                    let mut burst_any_published = false;
                    let mut burst_last_target: u64 = 0;
                    // Open one batch across all frames in this CQE buffer so the
                    // consumer-visible cursor advances with a single Release store
                    // per recv, not once per InputSlot. A 100KB recv carries 100+
                    // slots; this collapses 100+ Release stores into one.
                    let mut batch = input_producer.batch();
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
                        // Fast path: steady-state traffic is ~100% InputBatch
                        // frames. `try_decode_input_batch_into` decodes the
                        // wire format directly into `InputSlot`s — no
                        // journal-codec round-trip, no per-entry CRC, and no
                        // per-batch Vec allocation (slot_buf is reused).
                        match try_decode_input_batch_into(payload, slot_buf) {
                            Ok(()) => {
                                if !slot_buf.is_empty() {
                                    *received_data = true;
                                    for slot in slot_buf.drain(..) {
                                        let primary_seq = slot.sequence;
                                        burst_last_target = batch.push_with(|s| *s = slot);
                                        *accum_end_sequence = primary_seq;
                                        burst_any_published = true;
                                    }
                                }
                            }
                            Err(_) => {
                                // Not an InputBatch — fall through to the
                                // general decoder for control messages
                                // (heartbeat, need-snapshot, hash-mismatch).
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
                                    Ok(_) => {
                                        debug!("unexpected message during streaming");
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "failed to decode primary message");
                                        return SessionExit::Disconnected;
                                    }
                                }
                            }
                        }
                        cursor += 4 + frame_len;
                    }
                    // Single Release store on the producer cursor, making every
                    // slot written above visible to the apply consumer at once.
                    batch.commit();

                    // Compact parse_buf.
                    if cursor > 0 {
                        let remaining = parse_buf.len() - cursor;
                        parse_buf.copy_within(cursor.., 0);
                        parse_buf.truncate(remaining);
                    }

                    // Submit one pending_acks entry covering everything
                    // published from this RECV CQE's buffer.
                    if burst_any_published && !pending_acks.is_full() {
                        pending_acks.push(burst_last_target, *accum_end_sequence);
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
        // With busy_spin, never yield: every ack round-trip to the primary
        // sits in the critical path for that primary's response gate, so
        // even one scheduler tick of wake-up latency (~1ms on stock Linux)
        // shows up directly as extra p99/p99.9 on client responses.
        if !any_cqe {
            if busy_spin || idle_spins < 1000 {
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
    in_memory_sequence: u64,
    writer: &mut TcpStream,
    send_buf: &mut Vec<u8>,
) -> io::Result<()> {
    encode_ack(
        &Ack {
            acked_sequence,
            in_memory_sequence,
        },
        send_buf,
    );
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
    // `Send + Sync` so the enum can cross thread boundaries (the streaming
    // loop runs on a pinned receiver thread; the orchestrator joins on it).
    Fatal(Box<dyn std::error::Error + Send + Sync>),
}

/// Run the replication receiver. Connects to a primary, receives journal
/// entries, persists them locally, replays into the App, and sends acks.
///
/// Blocks until the connection drops or shutdown is signaled.
/// Result of `run_receiver`: `None` = clean shutdown, `Some` = promotion
/// triggered with the fully-replayed App and positioned writer.
pub type ReceiverResult<W> = Result<Option<(crate::App, W)>, Box<dyn std::error::Error>>;

#[allow(clippy::too_many_arguments)]
pub fn run_receiver<W>(
    primary_addr: SocketAddr,
    journal_path: &std::path::Path,
    signing_key: &ed25519_dalek::SigningKey,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_ms: u64,
    snapshot_path: std::path::PathBuf,
    cores: crate::server::PipelineCores,
    receiver_core: usize,
    group_commit_delay: std::time::Duration,
    pipeline_depth: usize,
    busy_spin: bool,
    // Runtime rotation knobs: (max_journal_bytes, rotate_flag). The flag
    // is a shared AtomicBool flipped by the `rotate` admin endpoint;
    // max_journal_bytes == 0 disables size-driven rotation. None means
    // runtime rotation is off entirely on this replica.
    rotation: Option<(u64, std::sync::Arc<AtomicBool>)>,
    // Per-account open-order cap (SEC-03). Must match the primary or
    // replay diverges on Rejected reports. Forwarded to every
    // freshly-constructed engine in this receiver.
    max_orders_per_account: u32,
    // Per-account order-rate limit (SEC-04). Same determinism caveat —
    // primary and replicas must agree on rate + burst.
    max_orders_per_second: u32,
    max_orders_burst: u32,
) -> ReceiverResult<W>
where
    W: JournalWrite<TradingEvent> + Send + 'static,
    JournalStage<TradingEvent, W>: JournalStageRun<TradingEvent, Writer = W>,
{
    use crate::App;

    // Recover local state from journal (if any). On first call this may
    // be (None, None) for a fresh replica. After a reconnect, the pipeline
    // shutdown returns the App + writer directly.
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        if journal_path.exists() {
            let engine = if snapshot_path.exists() {
                info!("recovering replica from snapshot + journal");
                melin_transport_core::JournaledApp::<App, W>::recover_from_snapshot(
                    &snapshot_path,
                    journal_path,
                )?
            } else {
                melin_transport_core::JournaledApp::<App, W>::recover(
                    crate::server::empty_app(),
                    journal_path,
                )?
            };
            let next = engine.next_sequence();
            let last = next.saturating_sub(1);
            let hash = engine.chain_hash().unwrap_or([0u8; 32]);
            let (mut exchange, writer) = engine.into_parts();
            crate::server::apply_max_orders(
                &mut exchange,
                max_orders_per_account,
                max_orders_per_second,
                max_orders_burst,
            );
            (Some(exchange), Some(writer), last, hash)
        } else {
            (None, None, 0u64, [0u8; 32])
        };

    // Exponential backoff for reconnection: 1s → 2s → 4s → … → 30s max.
    // Reset to 1s on successful streaming (first InputBatch received).
    let mut backoff = std::time::Duration::from_secs(1);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

    // Reusable buffers — survive across reconnections.
    let mut send_buf = Vec::with_capacity(64);
    // Grows to the sender's batch size on the first batch, then never
    // reallocates — even across reconnects.
    let mut slot_buf: Vec<crate::InputSlot> = Vec::new();
    let mut accum_end_sequence: u64 = 0;

    // Live pipeline state — built once on first connect (or after a snapshot
    // transfer), persists across `Disconnected` reconnects so we don't pay
    // the journal-recover + thread-spawn + warm-up cost on every TCP drop.
    // None = no pipeline yet (first iteration, or just torn down for
    // snapshot transfer); Some = running pipeline with threads + atomics
    // we can read for the next reconnect handshake.
    let mut pipeline: Option<ReplicaPipelineHandles<W>> = None;

    // --- Outer reconnect loop ---
    //
    // Each iteration: connect → auth → handshake → (snapshot rebuild?) →
    // (build pipeline if absent) → stream. On `Disconnected` the pipeline
    // stays live — we just refresh handshake state from its atomics and
    // reconnect. Only `Promote` / `Shutdown` / `Fatal` / snapshot-transfer
    // tear it down.
    loop {
        // Refresh handshake state from the running pipeline, if any. The
        // primary uses this `last_sequence` to decide between live streaming,
        // catch-up, and snapshot transfer; reading it from atomics rather
        // than from locals keeps it accurate across reconnects without
        // having to tear down the pipeline.
        if let Some(p) = pipeline.as_ref() {
            last_sequence = p.last_seq.load(Ordering::Acquire);
            if let Some(ref lock) = p.chain_hash_lock {
                chain_hash = lock.load();
            }
        }

        // Check shutdown/promote before attempting to connect.
        if shutdown.load(Ordering::Relaxed) {
            if let Some(mut p) = pipeline.take() {
                p.input_producer
                    .publish(crate::InputSlot::shutdown_sentinel());
                let _ = teardown_replica_pipeline(p);
            }
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered while disconnected");
            if let Some(mut p) = pipeline.take() {
                p.input_producer
                    .publish(crate::InputSlot::shutdown_sentinel());
                if let Some((e, w)) = teardown_replica_pipeline(p) {
                    exchange = Some(e);
                    journal_writer = Some(w);
                }
            }
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
                    if let Some(mut p) = pipeline.take() {
                        p.input_producer
                            .publish(crate::InputSlot::shutdown_sentinel());
                        if let Some((e, w)) = teardown_replica_pipeline(p) {
                            exchange = Some(e);
                            journal_writer = Some(w);
                        }
                    }
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

                // Tear down the running pipeline before wiping its journal
                // file from under it. The recovered (App, SectorWriter) is
                // discarded — snapshot loading reconstructs both fresh below
                // and reassigns `exchange` / `journal_writer`.
                if let Some(mut p) = pipeline.take() {
                    p.input_producer
                        .publish(crate::InputSlot::shutdown_sentinel());
                    let _ = teardown_replica_pipeline(p);
                }

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
                    melin_transport_core::snapshot::load::<App>(&snapshot_path)?;
                if snap_hash != snap_chain_hash {
                    return Err(format!(
                        "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
                         loaded snapshot has {snap_hash:02x?}"
                    )
                    .into());
                }
                exchange = Some(snap_exchange);

                let writer = W::create_continuing(journal_path, snap_seq + 1, snap_hash)?;
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
            let writer =
                melin_journal::create_fresh_replica::<_, W>(journal_path, &primary_genesis_entry)?;
            let mut fresh = crate::server::empty_app();
            crate::server::apply_max_orders(
                &mut fresh,
                max_orders_per_account,
                max_orders_per_second,
                max_orders_burst,
            );
            exchange = Some(fresh);
            journal_writer = Some(writer);
        }

        // --- Build pipeline if absent ---
        //
        // Built once on first connect, or after a snapshot transfer tore
        // the previous one down. On `Disconnected` the pipeline lives, so
        // this branch is skipped.
        if pipeline.is_none() {
            let cur_exchange = exchange.take().expect("exchange initialized");
            let cur_writer = journal_writer.take().expect("journal_writer initialized");
            pipeline = Some(build_replica_pipeline_with_threads(
                cur_exchange,
                cur_writer,
                cores,
                snapshot_interval_ms,
                snapshot_path.clone(),
                group_commit_delay,
                busy_spin,
                rotation.clone(),
            )?);
        }

        // --- Streaming session ---
        //
        // The streaming loop runs on its own pinned thread (mirrors the
        // primary's `reader.rs` layout). Borrows the input producer + journal
        // cursor from the live pipeline; on `Disconnected` we just retake
        // them next iteration.

        let mut pending_acks = PendingAckQueue::new(pipeline_depth);
        let mut received_data = false;

        let exit_reason: SessionExit = {
            let p = pipeline.as_mut().expect("pipeline must exist by here");
            let input_producer = &mut p.input_producer;
            let journal_cursor = p.journal_cursor.as_ref();
            std::thread::scope(|s| {
                let handle = std::thread::Builder::new()
                    .name("replica-receiver".into())
                    .spawn_scoped(s, || {
                        crate::affinity::pin_thread("replica-receiver", receiver_core);
                        replica_stream_uring(
                            &tcp_writer,
                            input_producer,
                            journal_cursor,
                            &mut pending_acks,
                            &mut received_data,
                            &mut accum_end_sequence,
                            shutdown,
                            promote,
                            busy_spin,
                            &mut slot_buf,
                        )
                    })
                    .expect("spawn replica-receiver thread");
                handle.join().expect("replica-receiver thread panicked")
            })
        };

        // Wait for all pending batches to become durable, then ack.
        if let Some(p) = pipeline.as_ref()
            && let Some(seq) = pending_acks.pop_all_blocking(p.journal_cursor.as_ref(), busy_spin)
        {
            let _ = send_ack_tcp(seq, accum_end_sequence, &mut tcp_writer, &mut send_buf);
        }

        // For terminal session exits (Shutdown / Promote / Fatal) the
        // pipeline will be torn down. Publish a sentinel slot so the
        // journal and matching stages can drain everything we've already
        // published and exit cleanly via the normal consume path — no
        // shutdown-flag/cursor-race dance. Disconnected exits are
        // transient (pipeline persists across reconnects) and must not
        // emit a sentinel.
        if !matches!(exit_reason, SessionExit::Disconnected)
            && let Some(p) = pipeline.as_mut()
        {
            p.input_producer
                .publish(crate::InputSlot::shutdown_sentinel());
        }

        match exit_reason {
            SessionExit::Shutdown => {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline(p);
                }
                return Ok(None);
            }

            SessionExit::Promote => {
                return match pipeline.take() {
                    Some(p) => match teardown_replica_pipeline(p) {
                        Some((e, w)) => Ok(Some((e, w))),
                        None => Err("pipeline failed during promotion".into()),
                    },
                    None => Err("pipeline missing on promote".into()),
                };
            }

            SessionExit::Fatal(e) => {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline(p);
                }
                return Err(e);
            }

            SessionExit::Disconnected => {
                // Pipeline stays live — `last_sequence` and `chain_hash`
                // refresh from its atomics at the top of the next iteration.
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
