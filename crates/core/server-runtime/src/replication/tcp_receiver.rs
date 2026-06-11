//! TCP replication receiver (replica side).
//!
//! Connects to the primary, authenticates, performs catch-up / snapshot
//! recovery, and runs the streaming receive loop via [`streaming_loop`].
//! Builds the replica's local pipeline (journal + matching engine + drain
//! stages) to apply incoming events and ack durable batches back to the
//! primary.

use std::io::{self, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{info, warn};

use melin_app::Application;
use melin_journal::JournalWrite;
use melin_transport_core::pipeline::{InputSlot, JournalStage, JournalStageRun};

use super::auth::authenticate_with_primary;
use super::receiver_transport::{ReceiverTransport, SessionExit, streaming_loop};
use super::{
    ReplicaPipelineHandles, build_replica_pipeline_with_threads, sleep_checking_flags,
    teardown_replica_pipeline,
};
use melin_transport_core::replication::protocol::{
    Ack, Handshake, MAX_CONTROL_FRAME, MAX_DATA_FRAME, PrimaryMessage, decode_primary_message,
    encode_ack, encode_handshake, read_frame,
};

/// Force the kernel to send a TCP ACK immediately rather than holding
/// it in the delayed-ACK timer (~40 ms on Linux). Linux clears
/// `TCP_QUICKACK` after each ACK it sends, so this must be re-armed
/// after every received batch.
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
        let _ = rc;
    }
}

// ---------------------------------------------------------------------------
// io_uring ReceiverTransport implementation
// ---------------------------------------------------------------------------

const TOKEN_RECV: u64 = 0;
const TOKEN_SEND: u64 = 1;
const TOKEN_PROVIDE: u64 = 2;

// Multishot RECV buffer pool. 16 buffers × MAX_DATA_FRAME each.
const NUM_RECV_BUFFERS: u16 = 16;
const RECV_BUF_SIZE: usize = MAX_DATA_FRAME;
const RECV_BUF_GROUP_ID: u16 = 0;

// CQE flag bits (io_uring ABI).
const IORING_CQE_F_BUFFER: u32 = 1 << 0;
const IORING_CQE_F_MORE: u32 = 1 << 1;
const IORING_CQE_BUFFER_SHIFT: u32 = 16;

/// io_uring–backed receiver transport for kernel TCP.
///
/// Uses `IORING_OP_RECV_MULTI` against a provided buffer pool for
/// incoming frames, and single-shot SEND for acks. Owns the io_uring
/// ring, buffer pool, and ack send state.
struct UringTransport {
    ring: io_uring::IoUring,
    tcp_fd: RawFd,
    // Backing storage for the io_uring provided-buffer pool. Never read
    // directly — the kernel accesses it via raw pointers registered in
    // ProvideBuffers. Must stay alive for the lifetime of the ring.
    _recv_pool: Vec<u8>,
    pool_ptr: *mut u8,
    multishot_active: bool,
    connected: bool,
    ack_buf: Vec<u8>,
    ack_offset: usize,
    ack_in_flight: bool,
    /// Newest ack accepted while a SEND was in flight — sent the moment
    /// the in-flight SEND's CQE is reaped (send-latest-on-completion).
    /// `Option<Ack>` rather than a queue: ack cursors are cumulative
    /// and monotonic, so when several acks arrive while one send is in
    /// flight only the newest pair needs the wire — intermediate values
    /// are subsumed. Exactly one SEND is ever outstanding, which keeps
    /// the TCP byte stream ordered (io_uring gives no ordering between
    /// independent SQEs, and interleaved partial sends would corrupt
    /// the frame stream).
    pending_ack: Option<Ack>,
}

impl UringTransport {
    fn new(tcp_stream: &TcpStream) -> io::Result<Self> {
        use io_uring::{IoUring, opcode, types};
        use std::os::unix::io::AsRawFd;

        let tcp_fd = tcp_stream.as_raw_fd();

        let mut ring: IoUring = IoUring::builder()
            .setup_single_issuer()
            .build(64)
            .map_err(|e| io::Error::other(format!("io_uring init failed: {e}")))?;

        ring.submitter()
            .register_files(&[tcp_fd])
            .map_err(|e| io::Error::other(format!("io_uring register_files failed: {e}")))?;

        arm_tcp_quickack(tcp_fd);

        // Pin io-wq workers to core 0.
        {
            let mut cpuset: libc::cpu_set_t = unsafe { std::mem::zeroed() };
            unsafe { libc::CPU_SET(0, &mut cpuset) };
            let _ = ring.submitter().register_iowq_aff(&cpuset);
        }

        let mut recv_pool: Vec<u8> = vec![0u8; NUM_RECV_BUFFERS as usize * RECV_BUF_SIZE];
        let pool_ptr = recv_pool.as_mut_ptr();

        // Provide the buffer pool to io_uring.
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
            ring.submit_and_wait(1)?;
            let cqe = ring
                .completion()
                .next()
                .ok_or_else(|| io::Error::other("no cqe after ProvideBuffers"))?;
            if cqe.result() < 0 {
                return Err(io::Error::other(format!(
                    "ProvideBuffers failed: {}",
                    cqe.result()
                )));
            }
        }

        // Submit the initial multishot RECV.
        let sqe = opcode::RecvMulti::new(types::Fd(tcp_fd), RECV_BUF_GROUP_ID)
            .build()
            .user_data(TOKEN_RECV);
        unsafe { ring.submission().push(&sqe).expect("SQ full") };

        Ok(UringTransport {
            ring,
            tcp_fd,
            _recv_pool: recv_pool,
            pool_ptr,
            multishot_active: true,
            connected: true,
            ack_buf: Vec::with_capacity(64),
            ack_offset: 0,
            ack_in_flight: false,
            pending_ack: None,
        })
    }

    /// Encode `ack` into the send buffer and push its SEND SQE. The
    /// caller is responsible for submitting (next `ring.submit()` —
    /// either `poll_recv`'s top-of-call submit or the eager submit
    /// after a chained send). Must only be called with no SEND in
    /// flight: the buffer is pinned by `ack_in_flight` until the CQE.
    fn push_ack_send(&mut self, ack: &Ack) {
        use io_uring::{opcode, types};

        debug_assert!(!self.ack_in_flight);
        self.ack_buf.clear();
        encode_ack(ack, &mut self.ack_buf);
        let sqe = opcode::Send::new(
            types::Fixed(0),
            self.ack_buf.as_ptr(),
            self.ack_buf.len() as u32,
        )
        .build()
        .user_data(TOKEN_SEND);
        // SAFETY: `ack_buf` is owned by this transport and pinned by
        // `ack_in_flight = true` until the matching CQE is reaped. The
        // ring is single-threaded.
        unsafe { self.ring.submission().push(&sqe).expect("SQ full") };
        self.ack_in_flight = true;
        self.ack_offset = 0;
    }
}

impl ReceiverTransport for UringTransport {
    fn poll_recv(&mut self, recv_buf: &mut Vec<u8>) -> io::Result<bool> {
        use io_uring::{opcode, types};

        if !self.connected {
            return Err(io::Error::other("not connected"));
        }

        // Submit pending SQEs (ProvideBuffers re-provisions, Send
        // remainders, multishot resubmissions).
        let pending = self.ring.submission().len();
        if pending > 0 {
            self.ring.submit()?;
        }

        // Drain CQEs.
        let mut cqes: [(u64, i32, u32); 16] = [(0, 0, 0); 16];
        let mut cqe_count = 0;
        {
            let mut cq = self.ring.completion();
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

        let mut any_recv = false;
        // Set when a queued ack is chained onto a completed SEND below —
        // it must hit the wire within THIS call, not ride the next
        // poll_recv's top-of-call submit one loop iteration later.
        let mut submit_chained_ack = false;

        for &(token, result, flags) in &cqes[..cqe_count] {
            match token {
                TOKEN_RECV => {
                    if (flags & IORING_CQE_F_MORE) == 0 {
                        self.multishot_active = false;
                    }
                    if result < 0 {
                        if result == -libc::ENOBUFS {
                            tracing::debug!("recv multishot ENOBUFS — pool exhausted");
                            continue;
                        }
                        self.connected = false;
                        return Err(io::Error::other(format!(
                            "primary disconnected (recv returned {result})"
                        )));
                    }
                    if result == 0 {
                        self.connected = false;
                        return Err(io::Error::other("primary disconnected (recv returned 0)"));
                    }
                    if (flags & IORING_CQE_F_BUFFER) == 0 {
                        self.connected = false;
                        return Err(io::Error::other("recv cqe missing F_BUFFER flag"));
                    }

                    arm_tcp_quickack(self.tcp_fd);
                    let n = result as usize;
                    let buf_id = (flags >> IORING_CQE_BUFFER_SHIFT) as usize;
                    // SAFETY: buf_id from kernel CQE for a buffer we
                    // provided from recv_pool; kernel wrote n bytes.
                    let buf_ptr = unsafe { self.pool_ptr.add(buf_id * RECV_BUF_SIZE) };
                    let slice = unsafe { std::slice::from_raw_parts(buf_ptr, n) };
                    recv_buf.extend_from_slice(slice);
                    any_recv = true;

                    // Re-provide the consumed buffer.
                    let provide_sqe = opcode::ProvideBuffers::new(
                        buf_ptr,
                        RECV_BUF_SIZE as i32,
                        1,
                        RECV_BUF_GROUP_ID,
                        buf_id as u16,
                    )
                    .build()
                    .user_data(TOKEN_PROVIDE);
                    unsafe { self.ring.submission().push(&provide_sqe).expect("SQ full") };
                }

                TOKEN_PROVIDE if result < 0 => {
                    tracing::debug!("ProvideBuffers re-provision failed: {result}");
                }

                TOKEN_SEND => {
                    if result < 0 {
                        self.connected = false;
                        return Err(io::Error::other(format!(
                            "ack send error (returned {result})"
                        )));
                    }
                    let sent = result as usize;
                    self.ack_offset += sent;
                    if self.ack_offset >= self.ack_buf.len() {
                        self.ack_buf.clear();
                        self.ack_offset = 0;
                        self.ack_in_flight = false;
                        // Send-latest-on-completion: an ack accepted
                        // while this send was in flight goes out now
                        // instead of waiting a full loop iteration for
                        // the next flush. Chained only after FULL
                        // completion — a partial send's remainder keeps
                        // exclusive ownership of the byte stream.
                        if let Some(ack) = self.pending_ack.take() {
                            self.push_ack_send(&ack);
                            submit_chained_ack = true;
                        }
                    } else {
                        // Partial send — resubmit remainder.
                        let sqe = opcode::Send::new(
                            types::Fixed(0),
                            self.ack_buf[self.ack_offset..].as_ptr(),
                            (self.ack_buf.len() - self.ack_offset) as u32,
                        )
                        .build()
                        .user_data(TOKEN_SEND);
                        unsafe { self.ring.submission().push(&sqe).expect("SQ full") };
                    }
                }

                _ => {}
            }
        }

        // Resubmit multishot if terminated.
        if !self.multishot_active {
            let sqe = opcode::RecvMulti::new(types::Fd(self.tcp_fd), RECV_BUF_GROUP_ID)
                .build()
                .user_data(TOKEN_RECV);
            unsafe { self.ring.submission().push(&sqe).expect("SQ full") };
            self.multishot_active = true;
        }

        // Flush a chained ack to the kernel before returning (also
        // carries any multishot resubmission pushed above).
        if submit_chained_ack {
            self.ring.submit()?;
        }

        Ok(any_recv)
    }

    fn send_ack(&mut self, ack: &Ack) -> io::Result<bool> {
        if self.ack_in_flight {
            // Coalesce: overwrite any previously queued value — the
            // cursors are cumulative, so the newest pair subsumes
            // everything before it. Sent on the in-flight SEND's CQE.
            self.pending_ack = Some(*ack);
            return Ok(true);
        }
        self.push_ack_send(ack);
        Ok(true)
    }

    fn ack_in_flight(&self) -> bool {
        // Pending counts as in flight: the drain paths use this to mean
        // "everything offered has reached the wire", and the flush gate
        // in `streaming_loop` recomputes a fresh ack next iteration
        // anyway.
        self.ack_in_flight || self.pending_ack.is_some()
    }

    fn is_connected(&mut self) -> bool {
        self.connected
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Outcome of `run_receiver`: `None` = clean shutdown, `Some` = promotion.
pub type ReceiverResult<A, W> = Result<Option<(A, W)>, Box<dyn std::error::Error>>;

#[allow(clippy::too_many_arguments)]
pub fn run_receiver<A, W>(
    primary_addr: SocketAddr,
    journal_path: &std::path::Path,
    signing_key: &ed25519_dalek::SigningKey,
    shutdown: &AtomicBool,
    promote: &AtomicBool,
    snapshot_interval_ms: u64,
    snapshot_path: std::path::PathBuf,
    cores: crate::server::PipelineCores,
    group_commit_delay: std::time::Duration,
    pipeline_depth: usize,
    busy_spin: bool,
    rotation: Option<(u64, std::sync::Arc<AtomicBool>)>,
    factory: std::sync::Arc<dyn melin_app::app_factory::AppFactory<App = A>>,
) -> ReceiverResult<A, W>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
    JournalStage<A::Event, W>: JournalStageRun<A::Event, Writer = W>,
{
    // Recover whenever any journal segment survives — live OR archived.
    // A crash between rotation's rename and the new live file's creation
    // leaves archives with no live segment; recovery handles that layout
    // (replays the archives, synthesizes a fresh live). Treating it as a
    // fresh replica would discard the local durable history and then
    // fail `create_new` against the surviving archives' lineage.
    let lineage_exists =
        journal_path.exists() || !melin_journal::segment::list_archives(journal_path)?.is_empty();
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) = if lineage_exists {
        let engine = if snapshot_path.exists() {
            info!("recovering replica from snapshot + journal");
            melin_transport_core::JournaledApp::<A, W>::recover_from_snapshot(
                &snapshot_path,
                journal_path,
            )?
        } else {
            melin_transport_core::JournaledApp::<A, W>::recover(factory.empty(), journal_path)?
        };
        let next = engine.next_sequence();
        let last = next.saturating_sub(1);
        let hash = engine.chain_hash().unwrap_or([0u8; 32]);
        let (mut exchange, writer) = engine.into_parts();
        factory.apply_operator_policy(&mut exchange);
        (Some(exchange), Some(writer), last, hash)
    } else {
        (None, None, 0u64, [0u8; 32])
    };

    let mut backoff = std::time::Duration::from_secs(1);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);

    let mut send_buf = Vec::with_capacity(64);
    let mut pipeline: Option<ReplicaPipelineHandles<A, W>> = None;

    // --- Outer reconnect loop ---
    loop {
        if let Some(p) = pipeline.as_ref() {
            last_sequence = p.last_seq.load().get();
            if let Some(ref lock) = p.chain_hash_lock {
                chain_hash = lock.load().chain_hash;
            }
        }

        if shutdown.load(Ordering::Relaxed) {
            if let Some(mut p) = pipeline.take() {
                p.input_producer
                    .publish(InputSlot::<A::Event>::shutdown_sentinel());
                let _ = teardown_replica_pipeline::<A, W>(p);
            }
            return Ok(None);
        }
        if promote.load(Ordering::Acquire) {
            info!("promotion triggered while disconnected");
            if let Some(mut p) = pipeline.take() {
                p.input_producer
                    .publish(InputSlot::<A::Event>::shutdown_sentinel());
                if let Some((e, w)) = teardown_replica_pipeline::<A, W>(p) {
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
                            .publish(InputSlot::<A::Event>::shutdown_sentinel());
                        if let Some((e, w)) = teardown_replica_pipeline::<A, W>(p) {
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
            warn!(error = %e, "failed to set TCP_NODELAY on replica receive socket");
        }
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

        // --- Protocol negotiation ---
        // `session_start` is the resume point this streaming session
        // continues from — the value that anchors the receiver's
        // sequence-contiguity gate. Derived from local knowledge (our
        // own handshake, or the verified snapshot), never from the
        // wire.
        let response_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
        let response = decode_primary_message(&response_frame)?;
        let (stream_lineage, session_start) = match response {
            PrimaryMessage::StreamStart {
                start_sequence,
                segment_start_sequence,
                anchor_hash,
            } => {
                info!(start_sequence, "streaming started");
                ((segment_start_sequence, anchor_hash), last_sequence)
            }
            PrimaryMessage::NeedSnapshot => {
                info!("primary requires snapshot transfer — receiving snapshot");

                if let Some(mut p) = pipeline.take() {
                    p.input_producer
                        .publish(InputSlot::<A::Event>::shutdown_sentinel());
                    let _ = teardown_replica_pipeline::<A, W>(p);
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

                let (snap_exchange, _snap_seq, snap_hash) =
                    melin_transport_core::snapshot::load::<A>(&snapshot_path)?;
                if snap_hash != snap_chain_hash {
                    return Err(format!(
                        "snapshot chain hash mismatch: primary sent {snap_chain_hash:02x?}, \
                         loaded snapshot has {snap_hash:02x?}"
                    )
                    .into());
                }
                exchange = Some(snap_exchange);

                let writer = W::create_continuing(journal_path, snap_sequence + 1, snap_hash)?;
                journal_writer = Some(writer);

                let ss_frame = read_frame(&mut reader, MAX_CONTROL_FRAME)?;
                let lineage = match decode_primary_message(&ss_frame)? {
                    PrimaryMessage::StreamStart {
                        start_sequence,
                        segment_start_sequence,
                        anchor_hash,
                    } => {
                        // The lineage must agree with the snapshot the
                        // primary just transferred — the local journal
                        // was created from the (verified) snapshot body,
                        // so an inconsistent StreamStart means a buggy
                        // or mismatched primary. Trust nothing
                        // unvalidated at this boundary: a future chain
                        // verifier would inherit the value.
                        if segment_start_sequence != snap_sequence + 1
                            || anchor_hash != snap_chain_hash
                        {
                            return Err(format!(
                                "post-snapshot StreamStart lineage (start \
                                 {segment_start_sequence}) disagrees with the transferred \
                                 snapshot (sequence {snap_sequence}) — inconsistent primary"
                            )
                            .into());
                        }
                        info!(start_sequence, "streaming resumed after snapshot transfer");
                        (segment_start_sequence, anchor_hash)
                    }
                    other => {
                        return Err(
                            format!("expected StreamStart after snapshot, got {other:?}").into(),
                        );
                    }
                };
                // Post-snapshot streaming resumes one past the
                // (verified) snapshot sequence.
                (lineage, snap_sequence)
            }
            PrimaryMessage::HashMismatch => {
                return Err("chain hash mismatch — replica has divergent history".into());
            }
            _ => {
                return Err(format!("unexpected response: {response:?}").into());
            }
        };

        // --- Create journal for fresh replica ---
        // The StreamStart lineage gives the segment header identity
        // (starting sequence + chain anchor) the primary's own journal
        // lineage began with; creating the local segment from the same
        // identity makes the replica's segment byte-identical to the
        // primary's until the first rotation on either node (rotations
        // are local, so segment boundaries diverge after that even
        // though the entry stream stays identical).
        if pipeline.is_none() && journal_writer.is_none() {
            let (lineage_start, lineage_anchor) = stream_lineage;
            let writer = W::create_continuing(journal_path, lineage_start, lineage_anchor)?;
            let mut fresh = factory.empty();
            factory.apply_operator_policy(&mut fresh);
            exchange = Some(fresh);
            journal_writer = Some(writer);
        }

        // --- Build pipeline if absent ---
        if pipeline.is_none() {
            let cur_exchange = exchange.take().expect("exchange initialized");
            let cur_writer = journal_writer.take().expect("journal_writer initialized");
            pipeline = Some(build_replica_pipeline_with_threads::<A, W>(
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
        let result = {
            let p = pipeline.as_mut().expect("pipeline must exist by here");
            let input_producer = &mut p.input_producer;
            let journal_cursor = p.journal_cursor.as_ref();
            std::thread::scope(|s| {
                let handle = std::thread::Builder::new()
                    .name("replica-receiver".into())
                    .spawn_scoped(s, || {
                        melin_app::affinity::pin_thread("replica-receiver", cores.reader);
                        let mut transport = match UringTransport::new(&tcp_writer) {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::error!(error = %e, "UringTransport init failed");
                                return super::receiver_transport::StreamingResult {
                                    exit: SessionExit::Disconnected,
                                    received_data: false,
                                };
                            }
                        };
                        streaming_loop::<UringTransport, A::Event>(
                            &mut transport,
                            input_producer,
                            journal_cursor,
                            shutdown,
                            promote,
                            pipeline_depth,
                            busy_spin,
                            session_start,
                            Vec::with_capacity(MAX_DATA_FRAME + 4),
                            None,
                        )
                    })
                    .expect("spawn replica-receiver thread");
                handle.join().expect("replica-receiver thread panicked")
            })
        };

        // Publish sentinel for terminal exits.
        if !matches!(result.exit, SessionExit::Disconnected)
            && let Some(p) = pipeline.as_mut()
        {
            p.input_producer
                .publish(InputSlot::<A::Event>::shutdown_sentinel());
        }

        match result.exit {
            SessionExit::Shutdown => {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Ok(None);
            }

            SessionExit::Promote => {
                return match pipeline.take() {
                    Some(p) => match teardown_replica_pipeline::<A, W>(p) {
                        Some((e, w)) => Ok(Some((e, w))),
                        None => Err("pipeline failed during promotion".into()),
                    },
                    None => Err("pipeline missing on promote".into()),
                };
            }

            SessionExit::Fatal(e) => {
                if let Some(p) = pipeline.take() {
                    let _ = teardown_replica_pipeline::<A, W>(p);
                }
                return Err(e);
            }

            SessionExit::Disconnected => {
                if result.received_data {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    use melin_transport_core::replication::protocol::{ReplicaMessage, decode_replica_message};

    /// Connected localhost pair: (transport side, peer side).
    fn socket_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        (client, server)
    }

    fn ack(seq: u64) -> Ack {
        Ack {
            acked_sequence: seq,
            in_memory_sequence: seq,
        }
    }

    /// Drive `poll_recv` until every accepted ack has reached the wire.
    fn flush_acks(transport: &mut UringTransport) {
        let mut recv_buf = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(5);
        while transport.ack_in_flight() {
            transport.poll_recv(&mut recv_buf).unwrap();
            assert!(Instant::now() < deadline, "ack send never completed");
            std::hint::spin_loop();
        }
    }

    /// Read `expect` length-prefixed ack frames from the peer socket.
    fn read_acks(peer: &mut TcpStream, expect: usize) -> Vec<Ack> {
        peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 256];
        let mut acks = Vec::new();
        while acks.len() < expect {
            let n = peer.read(&mut chunk).expect("peer read");
            assert!(n > 0, "peer closed before all acks arrived");
            buf.extend_from_slice(&chunk[..n]);
            // Parse complete frames.
            loop {
                if buf.len() < 4 {
                    break;
                }
                let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
                if buf.len() < 4 + len {
                    break;
                }
                match decode_replica_message(&buf[4..4 + len]).expect("decodable frame") {
                    ReplicaMessage::Ack(a) => acks.push(a),
                    other => panic!("expected Ack frame, got {other:?}"),
                }
                buf.drain(..4 + len);
            }
        }
        assert!(buf.is_empty(), "trailing bytes after expected acks");
        acks
    }

    #[test]
    fn coalesces_to_latest_ack_while_send_in_flight() {
        let (client, mut peer) = socket_pair();
        let mut transport = UringTransport::new(&client).unwrap();

        // First ack goes straight onto the wire path; the next two are
        // accepted while it is in flight (the flag clears only when the
        // CQE is reaped in poll_recv, so this is deterministic) and
        // coalesce — only the newest survives.
        assert!(transport.send_ack(&ack(1)).unwrap());
        assert!(transport.ack_in_flight());
        assert!(transport.send_ack(&ack(2)).unwrap());
        assert!(transport.send_ack(&ack(3)).unwrap());

        flush_acks(&mut transport);

        // Exactly two frames, in cursor order: ack(2) was subsumed.
        let acks = read_acks(&mut peer, 2);
        let seqs: Vec<u64> = acks.iter().map(|a| a.acked_sequence).collect();
        assert_eq!(seqs, vec![1, 3]);
    }

    #[test]
    fn sequential_acks_all_reach_the_wire_in_order() {
        let (client, mut peer) = socket_pair();
        let mut transport = UringTransport::new(&client).unwrap();

        for seq in 1..=3 {
            assert!(transport.send_ack(&ack(seq)).unwrap());
            flush_acks(&mut transport);
        }

        let acks = read_acks(&mut peer, 3);
        let seqs: Vec<u64> = acks.iter().map(|a| a.acked_sequence).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }
}
