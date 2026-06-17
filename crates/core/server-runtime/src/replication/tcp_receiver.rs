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
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tracing::{info, warn};

use melin_app::Application;
use melin_journal::JournalWrite;
use melin_transport_core::pipeline::{InputSlot, JournalStage, JournalStageRun};

use super::auth::authenticate_with_primary;
use super::receiver_transport::{
    ControlFrameSource, ReceiverTransport, SessionExit, streaming_loop,
};
use super::{
    AfterSession, MAX_BACKOFF, ReplicaPipelineHandles, ResyncDecision,
    build_replica_pipeline_with_threads, handle_resync_verdict, handle_session_exit,
    recover_replica_state, sleep_checking_flags, take_pipeline_for_promotion,
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

/// Kernel-TCP control-frame source: a blocking `read_frame` on the
/// replication stream. Drives the shared [`receive_chunked_body`] (and,
/// later, the rest of the resync transfer). See [`ControlFrameSource`].
struct TcpFrameSource<'a> {
    reader: &'a mut TcpStream,
}

impl ControlFrameSource for TcpFrameSource<'_> {
    fn next_frame(
        &mut self,
        max_size: usize,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(read_frame(self.reader, max_size)?)
    }
}

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
    factory: std::sync::Arc<dyn melin_app::app_factory::AppFactory<App = A>>,
    fence_state: std::sync::Arc<melin_transport_core::fence::FenceState>,
) -> ReceiverResult<A, W>
where
    A: Application + Send + 'static,
    A::Event: Send + Sync + 'static,
    A::Report: Send + 'static,
    A::QueryResponse: Send + 'static,
    W: JournalWrite<A::Event> + Send + 'static,
    JournalStage<A::Event, W>: JournalStageRun<A::Event, Writer = W>,
{
    // Recover whenever any journal segment survives — live OR archived;
    // fresh replicas get `(None, None, 0, zeros)`. See
    // `recover_replica_state` for the lineage rules.
    let (mut exchange, mut journal_writer, mut last_sequence, mut chain_hash) =
        recover_replica_state::<A, W>(
            journal_path,
            &snapshot_path,
            factory.as_ref(),
            &fence_state,
        )?;

    let mut backoff = std::time::Duration::from_secs(1);

    // Consecutive mid-stream divergence resyncs this process has
    // attempted — see `MAX_INPROCESS_DIVERGENCE_RESYNCS`.
    let mut divergence_resyncs: u32 = 0;

    let mut send_buf = Vec::with_capacity(64);
    let mut pipeline: Option<ReplicaPipelineHandles<A, W>> = None;

    // --- Outer reconnect loop ---
    loop {
        if let Some(p) = pipeline.as_ref() {
            // The handshake pair (last_sequence, chain_hash) must come
            // from ONE FsyncState snapshot — the journal stage keeps
            // flushing while we reconnect, and the primary's handshake
            // validation recomputes its chain at exactly the sequence
            // we claim. Reading the sequence and the hash from two
            // separate sources would tear under load, and a torn pair
            // is indistinguishable from divergence (false resync of a
            // healthy replica).
            if let Some(ref lock) = p.chain_hash_lock {
                let fsync_state = lock.load();
                last_sequence = fsync_state.journal_seq.get();
                chain_hash = fsync_state.chain_hash;
            } else {
                last_sequence = p.last_seq.load().get();
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
            return take_pipeline_for_promotion(&mut pipeline, &mut exchange, &mut journal_writer);
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
                    return take_pipeline_for_promotion(
                        &mut pipeline,
                        &mut exchange,
                        &mut journal_writer,
                    );
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

        // `reader`/`tcp_writer` are clones of one socket; auth is sequential so
        // a single handle works. Use `reader` (carries the read timeout above).
        if let Err(e) = authenticate_with_primary(&mut reader, signing_key) {
            warn!(error = %e, "authentication failed — retrying");
            backoff = (backoff * 2).min(MAX_BACKOFF);
            continue;
        }
        info!("authenticated with primary");

        // --- Handshake ---
        // Advertise our fencing epoch. If we are ahead of the primary it
        // sees this and self-demotes (it's a stale ex-primary); see
        // `crate::fence` and the sender's handshake handling.
        let handshake = Handshake {
            last_sequence,
            chain_hash,
            epoch: fence_state.epoch(),
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
                epoch,
            } => {
                // Fence: refuse to follow a primary whose epoch is behind
                // ours — following its (divergent) lineage on top of our
                // more-current state would corrupt the journal. Disconnect
                // and retry with backoff; the operator's logs flag the
                // misdirected `--replica-of`. Our handshake (already sent)
                // carries our higher epoch, so the stale primary also fences
                // itself on its side.
                let our_epoch = fence_state.epoch();
                if fence_state.refuses_primary(epoch) {
                    warn!(
                        primary_epoch = epoch,
                        our_epoch,
                        "primary is behind our fencing epoch — refusing to follow stale primary"
                    );
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    sleep_checking_flags(backoff, shutdown, promote);
                    continue;
                }
                // Adopt the primary's epoch immediately; streamed `EpochBump`s
                // keep it current thereafter.
                fence_state.observe_epoch(epoch);
                info!(start_sequence, epoch, "streaming started");
                ((segment_start_sequence, anchor_hash), last_sequence)
            }
            ref resync @ (PrimaryMessage::NeedSnapshot | PrimaryMessage::HashMismatch) => {
                let divergent = matches!(resync, PrimaryMessage::HashMismatch);
                // `map_err` drops `Send + Sync` by coercion — the `?` `From`
                // bridge doesn't apply directly across the two boxed trait
                // objects (`Box<dyn Error + Send + Sync>` → `Box<dyn Error>`).
                let decision = handle_resync_verdict(
                    divergent,
                    &mut TcpFrameSource {
                        reader: &mut reader,
                    },
                    &mut pipeline,
                    &mut exchange,
                    &mut journal_writer,
                    journal_path,
                    &snapshot_path,
                    &fence_state,
                    &mut last_sequence,
                    &mut chain_hash,
                )
                .map_err(|e| -> Box<dyn std::error::Error> { e })?;
                match decision {
                    ResyncDecision::Ready {
                        segment_start_sequence,
                        anchor_hash,
                        resume_sequence,
                    } => ((segment_start_sequence, anchor_hash), resume_sequence),
                    ResyncDecision::Retry => {
                        sleep_checking_flags(backoff, shutdown, promote);
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                }
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
        // primary's, and adopted `Rotate` boundaries keep it that way
        // across rotations (bitwise mirror).
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
                Arc::clone(&fence_state),
            )?);
        }

        // --- Streaming session ---
        let result = {
            let p = pipeline.as_mut().expect("pipeline must exist by here");
            let input_producer = &mut p.input_producer;
            let journal_cursor = p.journal_cursor.as_ref();
            let stream_marks = &p.stream_marks;
            let journal_failed = &p.journal_failed;
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
                            stream_marks,
                            journal_failed,
                        )
                    })
                    .expect("spawn replica-receiver thread");
                handle.join().expect("replica-receiver thread panicked")
            })
        };

        match handle_session_exit(
            result,
            &mut pipeline,
            &mut divergence_resyncs,
            &mut backoff,
            last_sequence,
            journal_path,
            &snapshot_path,
            factory.as_ref(),
            &fence_state,
            shutdown,
            promote,
            // Kernel TCP needs no explicit close — the `TcpStream` is
            // dropped on the next loop turn when a fresh connection
            // replaces it.
            || {},
        ) {
            AfterSession::Return(r) => return r,
            AfterSession::Resync {
                exchange: ex,
                journal_writer: wr,
                last_sequence: seq,
                chain_hash: hash,
            } => {
                exchange = ex;
                journal_writer = wr;
                last_sequence = seq;
                chain_hash = hash;
                continue;
            }
            AfterSession::Reconnect => {}
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

    // -----------------------------------------------------------------
    // In-process divergence resync — end to end against a scripted
    // primary. Needs hash-chain (divergence is a chain verdict) and
    // real persistence (the resync re-derives state from disk).
    // -----------------------------------------------------------------
    #[cfg(all(feature = "hash-chain", not(feature = "no-persist")))]
    mod divergence_resync {
        use super::super::super::auth::authenticate_replica;
        use super::*;
        use melin_app::app_factory::AppFactory;
        use melin_app::{AppEvent, Application, ApplyCtx, CodecError, RejectReason};
        use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};
        use melin_transport_core::cursors::WireSeq;
        use melin_transport_core::replication::catchup::{lineage_origin, snapshot_transfer_with};
        use melin_transport_core::replication::protocol::{
            encode_hash_mismatch, encode_rotate, encode_stream_start,
        };
        use std::sync::Arc;
        use std::sync::atomic::AtomicBool;

        #[derive(Debug, Clone, Copy, PartialEq)]
        struct EvtAdd(u64);

        impl AppEvent for EvtAdd {
            fn encoded_size(&self) -> usize {
                8
            }
            fn encode(&self, buf: &mut [u8]) -> usize {
                buf[..8].copy_from_slice(&self.0.to_le_bytes());
                8
            }
            fn decode(buf: &[u8]) -> Result<Self, CodecError> {
                if buf.len() < 8 {
                    return Err(CodecError::Truncated);
                }
                Ok(EvtAdd(u64::from_le_bytes(buf[..8].try_into().expect("8"))))
            }
            fn is_query(&self) -> bool {
                false
            }
        }

        #[derive(Debug, Clone, Copy)]
        struct Rpt;

        struct App;

        impl Application for App {
            type Event = EvtAdd;
            type Report = Rpt;
            type QueryResponse = Rpt;
            const APP_VERSION: u16 = 1;

            fn apply(&mut self, _e: EvtAdd, _ctx: &ApplyCtx, _out: &mut Vec<Rpt>) -> Option<Rpt> {
                None
            }
            fn tick(&mut self, _now_ns: u64, _out: &mut Vec<Rpt>) {}
            fn check_request_seq(&mut self, _key_hash: u64, _seq: u64) -> bool {
                true
            }
            fn build_reject(_e: &EvtAdd, _r: RejectReason) -> Rpt {
                Rpt
            }
            fn snapshot<W: std::io::Write>(&self, _w: &mut W) -> std::io::Result<()> {
                Ok(())
            }
            fn restore<R: std::io::Read>(_r: &mut R) -> std::io::Result<Self> {
                Ok(App)
            }
        }

        struct Factory;

        impl AppFactory for Factory {
            type App = App;
            fn empty(&self) -> App {
                App
            }
            fn prefault(&self, _app: &mut App) {}
        }

        /// Read one length-prefixed `ReplicaMessage` frame.
        fn read_replica_msg(stream: &mut TcpStream) -> ReplicaMessage {
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).expect("frame length");
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            stream.read_exact(&mut payload).expect("frame payload");
            decode_replica_message(&payload).expect("decodable replica frame")
        }

        /// Skip frames until an `Ack` at or past `seq` arrives.
        fn wait_for_ack(stream: &mut TcpStream, seq: u64) {
            loop {
                if let ReplicaMessage::Ack(a) = read_replica_msg(stream)
                    && a.acked_sequence >= seq
                {
                    return;
                }
            }
        }

        /// Accept with a deadline so a replica that fails to (re)connect
        /// fails the test instead of hanging it.
        fn accept_within(listener: &std::net::TcpListener, secs: u64) -> TcpStream {
            let deadline = Instant::now() + Duration::from_secs(secs);
            loop {
                match listener.accept() {
                    Ok((s, _)) => {
                        s.set_read_timeout(Some(Duration::from_secs(10)))
                            .expect("read timeout");
                        return s;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < deadline, "replica never connected");
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(e) => panic!("accept failed: {e}"),
                }
            }
        }

        /// A scripted primary brings a fresh replica up, streams one
        /// event, then announces a rotation with a deliberately wrong
        /// tail hash. The replica's journal stage detects divergence —
        /// `run_receiver` must repair it WITHOUT exiting: tear the
        /// pipeline down, re-derive its handshake position from disk,
        /// reconnect, take the HashMismatch verdict, archive the forked
        /// lineage, and complete the snapshot + segment-seed resync,
        /// then resume streaming on the rebased lineage.
        #[test]
        fn mid_stream_divergence_resyncs_in_process() {
            let dir = tempfile::tempdir().expect("tempdir");

            // --- The scripted primary's journal + snapshot, served by
            // the real sender-side transfer code in session 2. Entries
            // 1..=5, rotation after 2, snapshot at 4 (mid-live-segment).
            let primary_journal = dir.path().join("primary.journal");
            let mut w = BufferedWriter::<EvtAdd>::create(&primary_journal).expect("create");
            let mut chain_at_4 = [0u8; 32];
            for v in 1..=5u64 {
                w.append(&JournalEvent::App(EvtAdd(v))).expect("append");
                if v == 2 {
                    w.rotate_segment().expect("rotate");
                }
                if v == 4 {
                    chain_at_4 = w.chain_hash().expect("chain");
                }
            }
            drop(w);
            melin_transport_core::snapshot::save::<App>(
                &App,
                WireSeq::new(4),
                chain_at_4,
                0,
                &primary_journal.with_extension("snapshot"),
            )
            .expect("save snapshot");
            let (lineage_start, lineage_anchor) =
                lineage_origin(&primary_journal).expect("lineage");

            // --- Auth: one replica key, authorized.
            let repl_key = ed25519_dalek::SigningKey::from_bytes(&[0xFC; 32]);
            let pub_b64 = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                repl_key.verifying_key().to_bytes(),
            );
            let authorized_keys = melin_app::auth::AuthorizedKeys::parse(&format!(
                "replication {pub_b64} test-replica\n"
            ))
            .expect("parse keys");

            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.set_nonblocking(true).expect("nonblocking");
            let addr = listener.local_addr().expect("addr");

            // --- Replica under test.
            let replica_journal = dir.path().join("replica.journal");
            let replica_snapshot = dir.path().join("replica.snapshot");
            let shutdown = Arc::new(AtomicBool::new(false));
            let promote = Arc::new(AtomicBool::new(false));
            let cores = crate::server::PipelineCores {
                // 0 = unpinned sentinel for every stage.
                journal: 0,
                matching: 0,
                response: 0,
                reader: 0,
                repl_sender: 0,
                event_publisher: 0,
                shadow: 0,
                repl_handler_0: 0,
                repl_handler_1: 0,
            };
            let replica = {
                let journal = replica_journal.clone();
                let shutdown = Arc::clone(&shutdown);
                let promote = Arc::clone(&promote);
                std::thread::spawn(move || -> Result<bool, String> {
                    run_receiver::<App, BufferedWriter<EvtAdd>>(
                        addr,
                        &journal,
                        &ed25519_dalek::SigningKey::from_bytes(&[0xFC; 32]),
                        &shutdown,
                        &promote,
                        3_600_000,
                        replica_snapshot,
                        cores,
                        Duration::ZERO,
                        64,
                        false,
                        Arc::new(Factory),
                        Arc::new(melin_transport_core::fence::FenceState::new(0)),
                    )
                    // ReceiverResult's error is !Send — stringify for join().
                    .map(|state| state.is_none())
                    .map_err(|e| e.to_string())
                })
            };

            // --- Session 1: fresh sync, one event, then the poisoned
            // rotation announce.
            let mut s1 = accept_within(&listener, 30);
            let mut s1r = s1.try_clone().expect("clone");
            authenticate_replica(&mut s1r, &authorized_keys).expect("auth 1");
            match read_replica_msg(&mut s1) {
                ReplicaMessage::Handshake(h) => {
                    assert_eq!(h.last_sequence, 0, "fresh replica handshake")
                }
                other => panic!("expected Handshake, got {other:?}"),
            }
            let mut buf = Vec::new();
            encode_stream_start(0, lineage_start, lineage_anchor, 0, &mut buf);
            s1.write_all(&buf).expect("StreamStart 1");
            buf.clear();
            melin_transport_core::replication_wire::encode_input_batch(
                &[InputSlot::<EvtAdd> {
                    connection_id: 0,
                    key_hash: 0,
                    request_seq: 0,
                    sequence: 1,
                    timestamp_ns: 1,
                    event: JournalEvent::App(EvtAdd(1)),
                    // `()` without latency-trace, a timestamp with it.
                    publish_ts: Default::default(),
                    recv_ts: Default::default(),
                }],
                &mut buf,
            );
            s1.write_all(&buf).expect("InputBatch");
            wait_for_ack(&mut s1, 1);

            // Wrong tail hash at the announced boundary — the replica's
            // local chain at sequence 1 cannot match this.
            buf.clear();
            encode_rotate(1, &[0xEE; 32], &mut buf);
            s1.write_all(&buf).expect("Rotate");

            // --- Session 2: the SAME process reconnects with its
            // position re-derived from the forked on-disk journal.
            let mut s2 = accept_within(&listener, 30);
            let mut s2r = s2.try_clone().expect("clone");
            authenticate_replica(&mut s2r, &authorized_keys).expect("auth 2");
            match read_replica_msg(&mut s2) {
                ReplicaMessage::Handshake(h) => assert_eq!(
                    h.last_sequence, 1,
                    "post-divergence handshake must carry the recovered journal position"
                ),
                other => panic!("expected Handshake, got {other:?}"),
            }
            buf.clear();
            encode_hash_mismatch(&mut buf);
            s2.write_all(&buf).expect("HashMismatch");
            let transfer_shutdown = AtomicBool::new(false);
            let mut publish = |b: &[u8]| -> std::io::Result<()> {
                s2.write_all(b)?;
                s2.flush()
            };
            // Real sender-side resync: SnapshotBegin → chunks →
            // SegmentSeedBegin → seed → StreamStart → catch-up (entry 5).
            let end = snapshot_transfer_with::<EvtAdd>(
                &primary_journal,
                &mut publish,
                &transfer_shutdown,
            )
            .expect("snapshot transfer");
            assert_eq!(
                end,
                melin_transport_core::replication::catchup::CatchUpResult::Ok(5)
            );
            let mut s2_acks = s2.try_clone().expect("clone");
            wait_for_ack(&mut s2_acks, 5);

            // --- Clean shutdown; the receiver must return Ok(None) —
            // the divergence never escaped as a process-fatal error.
            shutdown.store(true, Ordering::Relaxed);
            let result = replica.join().expect("replica thread panicked");
            assert_eq!(result, Ok(true), "receiver must exit cleanly via shutdown");

            // The forked lineage was archived (never deleted), and the
            // live journal is the re-seeded one.
            assert!(
                dir.path().join("replica.journal.divergent.0").exists(),
                "divergent lineage must be archived"
            );
            assert!(replica_journal.exists(), "re-seeded live journal");
        }

        /// A resync whose snapshot transfer drops mid-flight must retry
        /// as a fresh replica with NO leftover in-memory state. On the
        /// in-process divergence repair path `recover_replica_state`
        /// leaves `exchange`/`journal_writer` populated; the resync arm
        /// archives the live journal (renaming it aside) before the
        /// transfer. If the transfer then fails and those handles are not
        /// nulled, the stale writer — its backing file now under the
        /// `.divergent.<n>` archive — survives the fresh-replica create
        /// gate (`journal_writer.is_none()` false) and is rebuilt into the
        /// next pipeline, so the live journal is never recreated and the
        /// new session streams onto an archived-away inode. This pins the
        /// fix (mirrors the DPDK receiver): after the failed transfer the
        /// retry creates a fresh live journal from the StreamStart lineage.
        #[test]
        fn resync_transfer_failure_rebuilds_clean_not_over_stale_writer() {
            let dir = tempfile::tempdir().expect("tempdir");

            // Minimal scripted primary — only its lineage identity is used
            // (session 3 streams from genesis; the dropped session 2 never
            // reaches the snapshot body).
            let primary_journal = dir.path().join("primary.journal");
            let mut w = BufferedWriter::<EvtAdd>::create(&primary_journal).expect("create");
            for v in 1..=2u64 {
                w.append(&JournalEvent::App(EvtAdd(v))).expect("append");
            }
            drop(w);
            let (lineage_start, lineage_anchor) =
                lineage_origin(&primary_journal).expect("lineage");

            let repl_key = ed25519_dalek::SigningKey::from_bytes(&[0xFC; 32]);
            let pub_b64 = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                repl_key.verifying_key().to_bytes(),
            );
            let authorized_keys = melin_app::auth::AuthorizedKeys::parse(&format!(
                "replication {pub_b64} test-replica\n"
            ))
            .expect("parse keys");

            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.set_nonblocking(true).expect("nonblocking");
            let addr = listener.local_addr().expect("addr");

            let replica_journal = dir.path().join("replica.journal");
            let replica_snapshot = dir.path().join("replica.snapshot");
            let shutdown = Arc::new(AtomicBool::new(false));
            let promote = Arc::new(AtomicBool::new(false));
            let cores = crate::server::PipelineCores {
                journal: 0,
                matching: 0,
                response: 0,
                reader: 0,
                repl_sender: 0,
                event_publisher: 0,
                shadow: 0,
                repl_handler_0: 0,
                repl_handler_1: 0,
            };
            let replica = {
                let journal = replica_journal.clone();
                let shutdown = Arc::clone(&shutdown);
                let promote = Arc::clone(&promote);
                std::thread::spawn(move || -> Result<bool, String> {
                    run_receiver::<App, BufferedWriter<EvtAdd>>(
                        addr,
                        &journal,
                        &ed25519_dalek::SigningKey::from_bytes(&[0xFC; 32]),
                        &shutdown,
                        &promote,
                        3_600_000,
                        replica_snapshot,
                        cores,
                        Duration::ZERO,
                        64,
                        false,
                        Arc::new(Factory),
                        Arc::new(melin_transport_core::fence::FenceState::new(0)),
                    )
                    .map(|state| state.is_none())
                    .map_err(|e| e.to_string())
                })
            };

            // Helper: stream a single event (seq 1) over an established
            // session and wait for its ack.
            let stream_one = |s: &mut TcpStream, buf: &mut Vec<u8>| {
                buf.clear();
                encode_stream_start(0, lineage_start, lineage_anchor, 0, buf);
                s.write_all(buf).expect("StreamStart");
                buf.clear();
                melin_transport_core::replication_wire::encode_input_batch(
                    &[InputSlot::<EvtAdd> {
                        connection_id: 0,
                        key_hash: 0,
                        request_seq: 0,
                        sequence: 1,
                        timestamp_ns: 1,
                        event: JournalEvent::App(EvtAdd(1)),
                        publish_ts: Default::default(),
                        recv_ts: Default::default(),
                    }],
                    buf,
                );
                s.write_all(buf).expect("InputBatch");
            };

            // --- Session 1: fresh sync, event 1, poisoned rotation —
            // the journal stage detects divergence and the receiver
            // repairs in-process (`recover_replica_state` repopulates
            // `exchange`/`journal_writer`).
            let mut buf = Vec::new();
            let mut s1 = accept_within(&listener, 30);
            let mut s1r = s1.try_clone().expect("clone");
            authenticate_replica(&mut s1r, &authorized_keys).expect("auth 1");
            match read_replica_msg(&mut s1) {
                ReplicaMessage::Handshake(h) => assert_eq!(h.last_sequence, 0, "fresh"),
                other => panic!("expected Handshake, got {other:?}"),
            }
            stream_one(&mut s1, &mut buf);
            wait_for_ack(&mut s1, 1);
            buf.clear();
            encode_rotate(1, &[0xEE; 32], &mut buf);
            s1.write_all(&buf).expect("Rotate");

            // --- Session 2: reconnect at the recovered position (seq 1),
            // verdict HashMismatch (archives the live journal aside), then
            // DROP the connection before SnapshotBegin — the transfer
            // fails and the receiver must retry as a fresh replica.
            let mut s2 = accept_within(&listener, 30);
            let mut s2r = s2.try_clone().expect("clone");
            authenticate_replica(&mut s2r, &authorized_keys).expect("auth 2");
            match read_replica_msg(&mut s2) {
                ReplicaMessage::Handshake(h) => assert_eq!(
                    h.last_sequence, 1,
                    "reconnect carries the recovered forked position"
                ),
                other => panic!("expected Handshake, got {other:?}"),
            }
            buf.clear();
            encode_hash_mismatch(&mut buf);
            s2.write_all(&buf).expect("HashMismatch");
            s2.flush().expect("flush");
            // Both fds reference the same socket (try_clone dups) — drop
            // both to send FIN so the replica's SnapshotBegin read hits EOF.
            drop(s2);
            drop(s2r);

            // --- Session 3: the receiver reconnects as a FRESH replica
            // (the archived lineage's position is meaningless, so
            // `last_sequence` is 0). It must recreate a live journal from
            // the StreamStart lineage, NOT rebuild over the stale writer.
            let mut s3 = accept_within(&listener, 30);
            let mut s3r = s3.try_clone().expect("clone");
            authenticate_replica(&mut s3r, &authorized_keys).expect("auth 3");
            match read_replica_msg(&mut s3) {
                ReplicaMessage::Handshake(h) => assert_eq!(
                    h.last_sequence, 0,
                    "after a failed transfer the retry handshakes as fresh"
                ),
                other => panic!("expected Handshake, got {other:?}"),
            }
            stream_one(&mut s3, &mut buf);
            wait_for_ack(&mut s3, 1);

            shutdown.store(true, Ordering::Relaxed);
            let result = replica.join().expect("replica thread panicked");
            assert_eq!(result, Ok(true), "receiver exits cleanly via shutdown");

            // The forked lineage stays archived, and a fresh live journal
            // was recreated from the lineage. With the bug, the stale
            // writer would be reused and this path would not exist (its
            // inode stranded under `.divergent.0`).
            assert!(
                dir.path().join("replica.journal.divergent.0").exists(),
                "forked lineage archived, never deleted"
            );
            assert!(
                replica_journal.exists(),
                "fresh live journal recreated after the failed transfer"
            );
        }

        /// The in-process repair budget is one per process lifetime: a
        /// second mid-stream divergence is systematic (corruption or a
        /// serious bug that the first re-seed did not cure) and the
        /// receiver must exit hard instead of looping — each repair
        /// cycle archives a full journal copy, and a repair loop would
        /// fill the disk while masking the underlying fault.
        #[test]
        fn second_mid_stream_divergence_exits_hard() {
            let dir = tempfile::tempdir().expect("tempdir");

            let primary_journal = dir.path().join("primary.journal");
            let mut w = BufferedWriter::<EvtAdd>::create(&primary_journal).expect("create");
            let mut chain_at_4 = [0u8; 32];
            for v in 1..=5u64 {
                w.append(&JournalEvent::App(EvtAdd(v))).expect("append");
                if v == 2 {
                    w.rotate_segment().expect("rotate");
                }
                if v == 4 {
                    chain_at_4 = w.chain_hash().expect("chain");
                }
            }
            drop(w);
            melin_transport_core::snapshot::save::<App>(
                &App,
                WireSeq::new(4),
                chain_at_4,
                0,
                &primary_journal.with_extension("snapshot"),
            )
            .expect("save snapshot");
            let (lineage_start, lineage_anchor) =
                lineage_origin(&primary_journal).expect("lineage");

            let repl_key = ed25519_dalek::SigningKey::from_bytes(&[0xFC; 32]);
            let pub_b64 = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                repl_key.verifying_key().to_bytes(),
            );
            let authorized_keys = melin_app::auth::AuthorizedKeys::parse(&format!(
                "replication {pub_b64} test-replica\n"
            ))
            .expect("parse keys");

            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
            listener.set_nonblocking(true).expect("nonblocking");
            let addr = listener.local_addr().expect("addr");

            let replica_journal = dir.path().join("replica.journal");
            let replica_snapshot = dir.path().join("replica.snapshot");
            let shutdown = Arc::new(AtomicBool::new(false));
            let promote = Arc::new(AtomicBool::new(false));
            let cores = crate::server::PipelineCores {
                journal: 0,
                matching: 0,
                response: 0,
                reader: 0,
                repl_sender: 0,
                event_publisher: 0,
                shadow: 0,
                repl_handler_0: 0,
                repl_handler_1: 0,
            };
            let replica = {
                let journal = replica_journal.clone();
                let shutdown = Arc::clone(&shutdown);
                let promote = Arc::clone(&promote);
                std::thread::spawn(move || -> Result<bool, String> {
                    run_receiver::<App, BufferedWriter<EvtAdd>>(
                        addr,
                        &journal,
                        &ed25519_dalek::SigningKey::from_bytes(&[0xFC; 32]),
                        &shutdown,
                        &promote,
                        3_600_000,
                        replica_snapshot,
                        cores,
                        Duration::ZERO,
                        64,
                        false,
                        Arc::new(Factory),
                        Arc::new(melin_transport_core::fence::FenceState::new(0)),
                    )
                    .map(|state| state.is_none())
                    .map_err(|e| e.to_string())
                })
            };

            // Session 1: fresh sync, one event, poisoned rotation.
            let mut s1 = accept_within(&listener, 30);
            let mut s1r = s1.try_clone().expect("clone");
            authenticate_replica(&mut s1r, &authorized_keys).expect("auth 1");
            match read_replica_msg(&mut s1) {
                ReplicaMessage::Handshake(h) => assert_eq!(h.last_sequence, 0),
                other => panic!("expected Handshake, got {other:?}"),
            }
            let mut buf = Vec::new();
            encode_stream_start(0, lineage_start, lineage_anchor, 0, &mut buf);
            s1.write_all(&buf).expect("StreamStart 1");
            buf.clear();
            melin_transport_core::replication_wire::encode_input_batch(
                &[InputSlot::<EvtAdd> {
                    connection_id: 0,
                    key_hash: 0,
                    request_seq: 0,
                    sequence: 1,
                    timestamp_ns: 1,
                    event: JournalEvent::App(EvtAdd(1)),
                    publish_ts: Default::default(),
                    recv_ts: Default::default(),
                }],
                &mut buf,
            );
            s1.write_all(&buf).expect("InputBatch");
            wait_for_ack(&mut s1, 1);
            buf.clear();
            encode_rotate(1, &[0xEE; 32], &mut buf);
            s1.write_all(&buf).expect("Rotate 1");

            // Session 2: in-process repair (the one allowed), then a
            // SECOND poisoned rotation after streaming resumes.
            let mut s2 = accept_within(&listener, 30);
            let mut s2r = s2.try_clone().expect("clone");
            authenticate_replica(&mut s2r, &authorized_keys).expect("auth 2");
            match read_replica_msg(&mut s2) {
                ReplicaMessage::Handshake(h) => assert_eq!(h.last_sequence, 1),
                other => panic!("expected Handshake, got {other:?}"),
            }
            buf.clear();
            encode_hash_mismatch(&mut buf);
            s2.write_all(&buf).expect("HashMismatch");
            let transfer_shutdown = AtomicBool::new(false);
            {
                let mut publish = |b: &[u8]| -> std::io::Result<()> {
                    s2.write_all(b)?;
                    s2.flush()
                };
                snapshot_transfer_with::<EvtAdd>(
                    &primary_journal,
                    &mut publish,
                    &transfer_shutdown,
                )
                .expect("snapshot transfer");
            }
            buf.clear();
            melin_transport_core::replication_wire::encode_input_batch(
                &[InputSlot::<EvtAdd> {
                    connection_id: 0,
                    key_hash: 0,
                    request_seq: 0,
                    sequence: 6,
                    timestamp_ns: 6,
                    event: JournalEvent::App(EvtAdd(6)),
                    publish_ts: Default::default(),
                    recv_ts: Default::default(),
                }],
                &mut buf,
            );
            s2.write_all(&buf).expect("InputBatch 6");
            let mut s2_acks = s2.try_clone().expect("clone");
            wait_for_ack(&mut s2_acks, 6);
            buf.clear();
            encode_rotate(6, &[0xEE; 32], &mut buf);
            s2.write_all(&buf).expect("Rotate 2");

            // No third connection: the receiver must give up and exit
            // with an error naming the recurrence.
            let result = replica.join().expect("replica thread panicked");
            let err = result.expect_err("second divergence must exit hard");
            assert!(
                err.contains("divergence recurred"),
                "error must name the recurrence: {err}"
            );
        }

        // -------------------------------------------------------------
        // Promotion teardown helper (`take_pipeline_for_promotion`) — the
        // no-live-pipeline decision branches. With no pipeline to tear
        // down it hands back the warm state held in the receiver's locals
        // (a clean promotion) or errors when none is present. Colocated
        // here for the `App` + `BufferedWriter` fixtures above; the
        // Clean-pipeline branch needs a live pipeline (failover IT).
        // -------------------------------------------------------------
        type PromoteHandles =
            crate::replication::ReplicaPipelineHandles<App, BufferedWriter<EvtAdd>>;

        #[test]
        fn promotion_hands_back_local_state_when_present() {
            let dir = tempfile::tempdir().unwrap();
            let mut pipeline: Option<PromoteHandles> = None;
            let mut exchange = Some(App);
            let mut journal_writer =
                Some(BufferedWriter::<EvtAdd>::create(&dir.path().join("p.journal")).unwrap());

            let result = crate::replication::take_pipeline_for_promotion(
                &mut pipeline,
                &mut exchange,
                &mut journal_writer,
            );

            assert!(matches!(result, Ok(Some(_))), "warm state must be promoted");
            assert!(
                exchange.is_none() && journal_writer.is_none(),
                "state must be moved into the result"
            );
        }

        #[test]
        fn promotion_errs_when_no_local_state() {
            let mut pipeline: Option<PromoteHandles> = None;
            let mut exchange: Option<App> = None;
            let mut journal_writer: Option<BufferedWriter<EvtAdd>> = None;

            let result = crate::replication::take_pipeline_for_promotion(
                &mut pipeline,
                &mut exchange,
                &mut journal_writer,
            );

            // Not `expect_err` — the Ok payload (App) isn't `Debug`.
            let err = match result {
                Err(e) => e,
                Ok(_) => panic!("a promote with nothing to promote must error"),
            };
            assert!(
                err.to_string().contains("no local state available"),
                "{err}"
            );
        }

        #[test]
        fn promotion_errs_on_partial_state() {
            // Exchange present, writer missing — not a usable hand-off.
            let mut pipeline: Option<PromoteHandles> = None;
            let mut exchange = Some(App);
            let mut journal_writer: Option<BufferedWriter<EvtAdd>> = None;

            let result = crate::replication::take_pipeline_for_promotion(
                &mut pipeline,
                &mut exchange,
                &mut journal_writer,
            );

            assert!(result.is_err(), "partial state is not a valid promotion");
        }
    }
}
