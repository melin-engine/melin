//! Multi-session publication sender.
//!
//! Sibling to [`crate::muxed_receiver::MuxedReceiver`] for the
//! outbound direction. Owns one socket and a map of per-session
//! outbound state — each session has its own [`PublicationLog`],
//! retransmit window, flow-control receiver map, and periodic
//! Setup/Heartbeat schedule. Ticks fan out across all sessions.
//!
//! # Allocation policy
//!
//! Unlike the receiver, sessions are NOT allocated lazily on
//! incoming traffic. The embedding server explicitly calls
//! [`MuxedSender::create_session`] when a client completes
//! authentication. This means a NAK or SM arriving for an unknown
//! session_id is silently dropped — no session is created on
//! unauthenticated inbound control traffic.
//!
//! # Threading
//!
//! Single-threaded — the embedding code drives [`MuxedSender::tick`]
//! and [`MuxedSender::create_session`] / [`MuxedSender::evict`] on
//! the same thread. Per-session `Arc<PublicationLog>` clones are
//! handed to the embedder for `try_claim`-side publishing; the
//! sender's tick remains the sole reader of `publisher_position`,
//! preserving the SPSC contract.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::counters::Counters;
use crate::flow_control::{FlowControl, ReceiverState};
use crate::pub_log::{FRAGMENT_ALIGNMENT, PublicationConfig, PublicationLog};
use crate::storage::align_up;
use crate::transport::{DatagramBuf, UdpTransport};
use crate::wire::{
    FrameView, HeartbeatFrame, NakFrame, SetupFrame, StatusMessage, parse_frame, position,
};

// Re-use the per-tick stats type from the single-session sender so
// observability tooling doesn't need to learn two shapes.
pub use crate::sender::TickStats;

/// Fragments to drain per session per sendmmsg round. Matches the
/// single-session sender's BATCH; kept consistent so per-session
/// max_drain_per_tick budgets work the same way.
const DRAIN_BATCH: usize = 32;

/// One fragment staged for the cross-session sendmmsg call in
/// [`MuxedSender::tick`]. Carries only the fields needed by Phase 3
/// (position accounting); destination and offset are captured in
/// `send_entries` for the actual syscall.
struct StagedFrag {
    /// Payload length (wire bytes, not aligned). Used for stats.
    len: usize,
    /// Position advance: aligned frame size in the pub_log.
    aligned: u32,
}

/// Per-session slice of `MuxedSender::send_staging` for the
/// current tick, used in phase 3 to distribute the sent count.
struct SessionSendRange {
    session_id: u32,
    start: usize,
    count: usize,
}

/// Configuration for [`MuxedSender`]. Differs from the single-
/// session [`crate::sender::SenderConfig`] in two ways:
///
/// 1. No `dst` — each session carries its own destination,
///    supplied at [`MuxedSender::create_session`] time.
/// 2. Adds `max_sessions`, `initial_term_id`, `term_length`, `mtu`,
///    and `stream_id` so the muxer can build per-session
///    `PublicationLog`s on demand.
#[derive(Debug, Clone, Copy)]
pub struct MuxedSenderConfig {
    /// Stream ID stamped into every outbound frame and into newly-
    /// allocated `PublicationLog`s.
    pub stream_id: u32,
    /// Initial term_id used when allocating a fresh
    /// `PublicationLog`. Must match what the corresponding remote
    /// `MuxedReceiver` (or `SubscriptionLog`) expects.
    pub initial_term_id: u32,
    /// Per-session term length. See
    /// [`crate::muxed_receiver::MuxedReceiverConfig::term_length`]
    /// for the memory-budget discussion.
    pub term_length: u32,
    /// Per-session MTU forwarded to `PublicationLog::new`.
    pub mtu: u32,
    /// Send a fresh `SetupFrame` every this often, per session.
    pub setup_interval: Duration,
    /// Send a `HeartbeatFrame` every this often when a session has
    /// been idle.
    pub heartbeat_interval: Duration,
    /// Maximum bytes to drain from each session's log per tick.
    /// Bounds work per session per tick.
    pub max_drain_per_tick: u32,
    /// Maximum incoming control frames to process per tick (across
    /// all sessions; the shared socket reads them undifferentiated
    /// and the muxer routes by `session_id`).
    pub max_control_per_tick: u32,
    /// Flow-control strategy applied per session. Each session's
    /// publisher_limit is computed independently from its own
    /// receivers' SMs.
    pub flow_control: FlowControl,
    /// Maximum concurrent sessions. Past this cap,
    /// [`MuxedSender::create_session`] returns
    /// [`MuxedSenderError::SessionsExhausted`].
    pub max_sessions: u32,
}

impl MuxedSenderConfig {
    /// Reasonable defaults — production code should pin
    /// `term_length`, `mtu`, `max_sessions`, and intervals
    /// deliberately.
    pub fn defaults() -> Self {
        Self {
            stream_id: 1,
            initial_term_id: 1,
            term_length: 1024 * 1024, // 1 MiB — see MuxedReceiver doc.
            mtu: 1408,
            setup_interval: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(50),
            max_drain_per_tick: 64 * 1024,
            max_control_per_tick: 32,
            flow_control: FlowControl::Min,
            max_sessions: 1024,
        }
    }
}

/// Errors specific to the muxed sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MuxedSenderError {
    /// `max_sessions` cap reached.
    SessionsExhausted,
    /// `session_id` is already allocated. Caller likely has a logic
    /// bug — the server should generate a fresh session_id on each
    /// successful handshake.
    SessionExists,
    /// `PublicationLog::new` rejected the requested config (e.g.
    /// `term_length` not a power of two between 64 KiB and 1 GiB,
    /// or `mtu < HEADER_LEN + FRAGMENT_ALIGNMENT`).
    InvalidConfig,
}

impl std::fmt::Display for MuxedSenderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionsExhausted => write!(f, "max_sessions reached; create_session refused"),
            Self::SessionExists => write!(f, "session_id already allocated"),
            Self::InvalidConfig => write!(f, "publication-log config rejected"),
        }
    }
}

impl std::error::Error for MuxedSenderError {}

/// Per-session outbound state owned by the muxer. One per
/// distinct `session_id` we've authenticated.
struct SessionOutbound {
    log: Arc<PublicationLog>,
    /// Highest position whose fragment we have sent at least once.
    /// New fragments published past this are eligible for drain.
    last_sent_position: u64,
    /// Last instant we sent a Setup frame for this session.
    last_setup_at: Instant,
    /// Last instant we sent ANY frame for this session (data,
    /// retransmit, setup, heartbeat). Heartbeat fires when this
    /// is older than `heartbeat_interval`.
    last_send_at: Instant,
    /// Flow-control receiver state per `receiver_id`.
    receivers: HashMap<u64, ReceiverState>,
    /// Where to send this session's frames. Set at
    /// `create_session` time from the receiver-side
    /// `effective_dst` discovery.
    dst: SocketAddr,
}

/// Multi-session publication sender. See module docs.
pub struct MuxedSender<T: UdpTransport> {
    transport: T,
    config: MuxedSenderConfig,
    sessions: HashMap<u32, SessionOutbound>,
    /// Pool of receive slots reused for the batched control-recv
    /// path. Sized to `max_control_per_tick`.
    batch_slots: Vec<DatagramBuf>,
    counters: Option<Arc<Counters>>,
    /// Flat byte buffer for staged fragment data. Pre-allocated; grows
    /// to steady state after the first tick, then `clear()` reuses
    /// capacity. Avoids lifetime ties back to the pub_log term buffers.
    staging_data: Vec<u8>,
    /// Metadata for each staged fragment. Pre-allocated to
    /// `max_sessions × DRAIN_BATCH` to eliminate hot-path allocation.
    send_staging: Vec<StagedFrag>,
    /// `(dst, offset, len)` entries parallel to `send_staging`, passed
    /// directly to `send_staged` so Phase 2 needs no per-tick allocation.
    send_entries: Vec<(SocketAddr, usize, usize)>,
    /// Per-session range within `send_staging` for the current tick.
    session_ranges: Vec<SessionSendRange>,
    /// `(dst, offset, total_len, segment_size)` entries collapsing
    /// runs of consecutive same-size fragments per session into a
    /// single GSO mmsghdr. Used by Phase 2 when the kernel supports
    /// `UDP_SEGMENT`. Worst case (all distinct sizes) is one entry
    /// per `send_staging` entry, so we share the same capacity.
    segmented_entries: Vec<(SocketAddr, usize, usize, u16)>,
    /// Parallel to `segmented_entries`: number of `send_staging`
    /// entries each segmented entry covers. Phase 3 sums prefix
    /// values to recover the staging-entry-equivalent of an accepted
    /// mmsghdr count.
    segmented_runs: Vec<u32>,
    /// Set after the first `EINVAL` from `send_segmented_staged`.
    /// Older kernels and some virt environments reject `UDP_SEGMENT`;
    /// detect once and fall back to plain `send_staged` permanently.
    gso_unsupported: bool,
}

impl<T: UdpTransport> MuxedSender<T> {
    pub fn new(transport: T, config: MuxedSenderConfig) -> Self {
        let batch_slots = (0..config.max_control_per_tick)
            .map(|_| DatagramBuf::new(2048))
            .collect();
        let max_s = config.max_sessions as usize;
        Self {
            transport,
            config,
            sessions: HashMap::new(),
            batch_slots,
            counters: None,
            // staging_data grows to steady state on the first tick;
            // Vec::clear() preserves capacity so no re-alloc on
            // subsequent ticks.
            staging_data: Vec::new(),
            send_staging: Vec::with_capacity(max_s * DRAIN_BATCH),
            send_entries: Vec::with_capacity(max_s * DRAIN_BATCH),
            session_ranges: Vec::with_capacity(max_s),
            segmented_entries: Vec::with_capacity(max_s * DRAIN_BATCH),
            segmented_runs: Vec::with_capacity(max_s * DRAIN_BATCH),
            // RUMCAST_DISABLE_GSO=1 forces the per-fragment fallback
            // path. Diagnostic only — used to A/B whether UDP-GSO
            // offload helps or hurts on a given NIC/path.
            gso_unsupported: std::env::var("RUMCAST_DISABLE_GSO")
                .map(|v| !v.is_empty() && v != "0")
                .unwrap_or(false),
        }
    }

    pub fn set_counters(&mut self, counters: Option<Arc<Counters>>) {
        self.counters = counters;
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    /// Allocate a new outbound session. Returns the
    /// `Arc<PublicationLog>` the caller uses for `try_claim` /
    /// `publish` on the data plane. The muxer keeps another
    /// `Arc` clone for its own draining.
    ///
    /// Errors:
    /// - [`MuxedSenderError::SessionsExhausted`] — hit the cap.
    /// - [`MuxedSenderError::SessionExists`] — the caller passed
    ///   a session_id that's still allocated. Either evict first or
    ///   pick a fresh ID.
    /// - [`MuxedSenderError::InvalidConfig`] — `PublicationLog::new`
    ///   rejected the parameters.
    pub fn create_session(
        &mut self,
        session_id: u32,
        dst: SocketAddr,
    ) -> Result<Arc<PublicationLog>, MuxedSenderError> {
        if self.sessions.contains_key(&session_id) {
            return Err(MuxedSenderError::SessionExists);
        }
        if self.sessions.len() as u32 >= self.config.max_sessions {
            return Err(MuxedSenderError::SessionsExhausted);
        }
        let log = PublicationLog::new(PublicationConfig {
            session_id,
            stream_id: self.config.stream_id,
            initial_term_id: self.config.initial_term_id,
            term_length: self.config.term_length,
            mtu: self.config.mtu,
        })
        .map_err(|_| MuxedSenderError::InvalidConfig)?;
        let log = Arc::new(log);
        let now = Instant::now();
        let last_sent_position = position(self.config.initial_term_id, 0, log.term_length_bits());
        self.sessions.insert(
            session_id,
            SessionOutbound {
                log: Arc::clone(&log),
                last_sent_position,
                last_setup_at: now,
                last_send_at: now,
                receivers: HashMap::new(),
                dst,
            },
        );
        if let Some(c) = &self.counters {
            c.sessions_created
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(log)
    }

    /// Drop a session's outbound state. The caller's
    /// `Arc<PublicationLog>` clone stays valid (Arcs don't get
    /// invalidated by an evict); subsequent `try_claim`s on it
    /// just won't be drained anymore. Idempotent.
    pub fn evict(&mut self, session_id: u32) {
        self.sessions.remove(&session_id);
    }

    /// Run one tick: drain control frames (NAKs/SMs) on the shared
    /// socket and route by session_id, drain each session's
    /// PublicationLog, send periodic Setup/Heartbeat per session.
    /// Returns aggregated [`TickStats`] across sessions.
    ///
    /// Outbound data is sent in three phases:
    /// 1. Stage fragments from all sessions into flat buffers (no send yet).
    /// 2. One cross-session `send_multi_to` call — one `sendmmsg(2)`
    ///    syscall for all sessions instead of one per session.
    /// 3. Advance per-session `last_sent_position` based on sent count.
    pub fn tick(&mut self) -> TickStats {
        let mut stats = TickStats::default();
        let now = Instant::now();
        self.drain_control(&mut stats, now);
        let max_drain = self.config.max_drain_per_tick;
        let setup_interval = self.config.setup_interval;
        let heartbeat_interval = self.config.heartbeat_interval;
        let stream_id = self.config.stream_id;

        // Phase 1: stage outbound fragments from every session.
        self.staging_data.clear();
        self.send_staging.clear();
        self.send_entries.clear();
        self.session_ranges.clear();

        for (session_id, session) in &self.sessions {
            let stage_start = self.send_staging.len();
            let mut probe_pos = session.last_sent_position;
            let mut total_staged_aligned = 0u32;

            'outer: loop {
                let mut count = 0;
                let mut aligned_this_round = 0u32;

                while count < DRAIN_BATCH {
                    let Some(fragment) = session.log.published_fragment(probe_pos) else {
                        break;
                    };
                    let aligned = align_up(fragment.len() as u32, FRAGMENT_ALIGNMENT);
                    if total_staged_aligned + aligned_this_round + aligned > max_drain {
                        break;
                    }
                    let data_start = self.staging_data.len();
                    self.staging_data.extend_from_slice(fragment);
                    self.send_staging.push(StagedFrag {
                        len: fragment.len(),
                        aligned,
                    });
                    self.send_entries
                        .push((session.dst, data_start, fragment.len()));
                    probe_pos += aligned as u64;
                    aligned_this_round += aligned;
                    count += 1;
                }

                if count == 0 {
                    if probe_pos < session.log.publisher_position() {
                        stats.partition_misses += 1;
                    }
                    break 'outer;
                }

                total_staged_aligned += aligned_this_round;
                if total_staged_aligned >= max_drain {
                    break;
                }
            }

            let count = self.send_staging.len() - stage_start;
            if count > 0 {
                self.session_ranges.push(SessionSendRange {
                    session_id: *session_id,
                    start: stage_start,
                    count,
                });
            }
        }

        // Phase 1.5: coalesce same-size consecutive runs per session
        // into GSO entries. Each segmented entry collapses N adjacent
        // mmsghdrs whose `len`s match into one mmsghdr that the kernel
        // splits at egress (or in hardware on real NICs). Skip the
        // build entirely when GSO is known unsupported — saves the
        // walk on every tick.
        if !self.gso_unsupported {
            self.segmented_entries.clear();
            self.segmented_runs.clear();
            for range in &self.session_ranges {
                let mut i = range.start;
                let end = range.start + range.count;
                while i < end {
                    let frag_len = self.send_staging[i].len;
                    let run_offset = self.send_entries[i].1;
                    let run_dst = self.send_entries[i].0;
                    let mut run = 1usize;
                    while i + run < end && self.send_staging[i + run].len == frag_len {
                        run += 1;
                    }
                    self.segmented_entries.push((
                        run_dst,
                        run_offset,
                        frag_len * run,
                        frag_len as u16,
                    ));
                    self.segmented_runs.push(run as u32);
                    i += run;
                }
            }
        }

        // Phase 2: one cross-session sendmmsg call. GSO path uses
        // segmented_entries (one mmsghdr per same-size run); fallback
        // uses send_entries (one mmsghdr per fragment). On the first
        // EINVAL the GSO path is disabled permanently and we re-issue
        // the send via the fallback path so this tick's data isn't
        // lost.
        let staging_sent = if self.send_entries.is_empty() {
            0
        } else if self.gso_unsupported {
            self.send_via_staged(&mut stats)
        } else {
            match self
                .transport
                .send_segmented_staged(&self.staging_data, &self.segmented_entries)
            {
                Ok(mmsghdrs_sent) => self.segmented_runs[..mmsghdrs_sent]
                    .iter()
                    .map(|&n| n as usize)
                    .sum(),
                Err(e) if e.raw_os_error() == Some(libc::EINVAL) => {
                    // First time we observe EINVAL: kernel has no
                    // UDP_SEGMENT support. Fall back this tick and
                    // for all future ticks. No data lost: nothing
                    // was sent on the failing call.
                    self.gso_unsupported = true;
                    self.send_via_staged(&mut stats)
                }
                Err(_) => {
                    stats.send_errors += 1;
                    0
                }
            }
        };
        let total_sent = staging_sent;

        // Phase 3: advance per-session positions based on sent count.
        // Reuse the tick-start `now` for `last_send_at`. The few µs
        // drift across the sendmmsg call doesn't matter — heartbeat
        // gating uses ~100ms intervals.
        let mut remaining = total_sent;
        for range in &self.session_ranges {
            if remaining == 0 {
                break;
            }
            let Some(session) = self.sessions.get_mut(&range.session_id) else {
                continue;
            };
            let sent_this = remaining.min(range.count);
            for f in &self.send_staging[range.start..range.start + sent_this] {
                session.last_sent_position += f.aligned as u64;
                stats.bytes_sent += f.len as u64;
                stats.fragments_sent += 1;
            }
            if sent_this > 0 {
                session.last_send_at = now;
            }
            remaining -= sent_this;
        }

        // Phase 4: periodic Setup/Heartbeat per session.
        for session in self.sessions.values_mut() {
            session.maybe_send_periodic(
                &self.transport,
                &mut stats,
                now,
                setup_interval,
                heartbeat_interval,
                stream_id,
            );
        }

        if let Some(c) = &self.counters {
            crate::sender::fold_into_counters(c, &stats);
        }
        stats
    }

    /// Send a Setup for one session immediately, regardless of the
    /// `setup_interval`. Useful at startup so a subscriber that
    /// joined just after auth doesn't have to wait one full
    /// interval to learn stream parameters. No-op if the session
    /// doesn't exist.
    pub fn send_setup_now(&mut self, session_id: u32) -> TickStats {
        let mut stats = TickStats::default();
        if let Some(session) = self.sessions.get_mut(&session_id) {
            let now = Instant::now();
            session.send_setup(&self.transport, &mut stats, now, self.config.stream_id);
        }
        if let Some(c) = &self.counters {
            crate::sender::fold_into_counters(c, &stats);
        }
        stats
    }

    /// Fallback send path used when the kernel rejects `UDP_SEGMENT`
    /// or after the first such rejection. Returns the count of
    /// `send_staging` entries the kernel accepted (i.e. fragments
    /// sent), which the caller feeds into Phase 3 bookkeeping.
    fn send_via_staged(&self, stats: &mut TickStats) -> usize {
        match self
            .transport
            .send_staged(&self.staging_data, &self.send_entries)
        {
            Ok(n) => n,
            Err(_) => {
                stats.send_errors += 1;
                0
            }
        }
    }

    fn drain_control(&mut self, stats: &mut TickStats, now: Instant) {
        // One batched recv per tick — sender-side control traffic
        // (NAK/SM) is lower volume than data, but the same N→1
        // syscall amortization applies.
        let n = match self.transport.recv_batch(&mut self.batch_slots) {
            Ok(n) => n,
            Err(_) => {
                stats.control_drops += 1;
                return;
            }
        };
        if n == 0 {
            return;
        }

        // Copy out the structured frame so we can route through
        // &mut self after — same two-phase trick as MuxedReceiver.
        enum Routed {
            Nak { session_id: u32, nak: NakFrame },
            Sm { session_id: u32, sm: StatusMessage },
            Drop,
        }

        for slot in &self.batch_slots[..n] {
            let bytes = slot.payload();
            let routed = match parse_frame(bytes) {
                Ok(FrameView::Nak(nak)) => Routed::Nak {
                    session_id: nak.session_id,
                    nak: *nak,
                },
                Ok(FrameView::StatusMessage(sm)) => Routed::Sm {
                    session_id: sm.session_id,
                    sm: *sm,
                },
                // Sender side ignores Data / Setup / Heartbeat —
                // subscriber-bound or own echoes (multicast loop).
                _ => Routed::Drop,
            };
            match routed {
                Routed::Nak { session_id, nak } => {
                    stats.naks_received += 1;
                    if let Some(session) = self.sessions.get_mut(&session_id) {
                        session.handle_nak(&self.transport, &nak, stats, now);
                    } else {
                        stats.control_drops += 1;
                    }
                }
                Routed::Sm { session_id, sm } => {
                    stats.sms_received += 1;
                    if let Some(session) = self.sessions.get_mut(&session_id) {
                        session.handle_sm(&sm, &self.config.flow_control);
                    } else {
                        stats.control_drops += 1;
                    }
                }
                Routed::Drop => {
                    stats.control_drops += 1;
                }
            }
        }
    }
}

impl SessionOutbound {
    fn handle_nak<T: UdpTransport>(
        &mut self,
        transport: &T,
        nak: &NakFrame,
        stats: &mut TickStats,
        now: Instant,
    ) {
        let Some(window) = self
            .log
            .retransmit_window(nak.term_id, nak.term_offset, nak.gap_length)
        else {
            stats.control_drops += 1;
            return;
        };
        let mut offset = 0usize;
        while offset + 4 <= window.len() {
            let frame_length =
                u32::from_le_bytes(window[offset..offset + 4].try_into().expect("4 bytes"));
            if frame_length == 0 {
                break;
            }
            let aligned = align_up(frame_length, FRAGMENT_ALIGNMENT) as usize;
            if offset + frame_length as usize > window.len() {
                break;
            }
            let fragment = &window[offset..offset + frame_length as usize];
            match transport.send_to(self.dst, fragment) {
                Ok(_) => {
                    stats.retransmits_sent += 1;
                    stats.bytes_sent += fragment.len() as u64;
                    self.last_send_at = now;
                }
                Err(_) => stats.send_errors += 1,
            }
            offset += aligned;
        }
    }

    fn handle_sm(&mut self, sm: &StatusMessage, flow_control: &FlowControl) {
        let bits = self.log.term_length_bits();
        let consumption_pos = position(sm.consumption_term_id, sm.consumption_term_offset, bits);
        self.receivers
            .entry(sm.receiver_id)
            .and_modify(|r| {
                if consumption_pos > r.consumption_position {
                    r.consumption_position = consumption_pos;
                }
                r.receiver_window = sm.receiver_window;
            })
            .or_insert(ReceiverState {
                consumption_position: consumption_pos,
                receiver_window: sm.receiver_window,
            });

        for slow_id in flow_control.find_slow_consumers(&self.receivers) {
            self.receivers.remove(&slow_id);
        }

        if let Some(new_limit) = flow_control.compute_publisher_limit(&self.receivers) {
            self.log.set_publisher_limit(new_limit);
        }
    }

    fn maybe_send_periodic<T: UdpTransport>(
        &mut self,
        transport: &T,
        stats: &mut TickStats,
        now: Instant,
        setup_interval: Duration,
        heartbeat_interval: Duration,
        stream_id: u32,
    ) {
        if now.duration_since(self.last_setup_at) >= setup_interval {
            self.send_setup(transport, stats, now, stream_id);
        }
        if now.duration_since(self.last_send_at) >= heartbeat_interval {
            self.send_heartbeat(transport, stats, now, stream_id);
        }
    }

    fn send_setup<T: UdpTransport>(
        &mut self,
        transport: &T,
        stats: &mut TickStats,
        now: Instant,
        stream_id: u32,
    ) {
        let bits = self.log.term_length_bits();
        let cfg = self.log.config();
        let pub_pos = self.log.publisher_position();
        let active_term_id = (pub_pos >> bits) as u32;
        let term_offset = (pub_pos & ((1u64 << bits) - 1)) as u32;
        let setup = SetupFrame::new(
            cfg.session_id,
            stream_id,
            cfg.initial_term_id,
            active_term_id,
            term_offset,
            cfg.term_length,
        );
        match transport.send_to(self.dst, bytemuck::bytes_of(&setup)) {
            Ok(_) => {
                stats.setup_sent += 1;
                stats.bytes_sent += SetupFrame::HEADER_LEN as u64;
                self.last_setup_at = now;
                self.last_send_at = now;
            }
            Err(_) => stats.send_errors += 1,
        }
    }

    fn send_heartbeat<T: UdpTransport>(
        &mut self,
        transport: &T,
        stats: &mut TickStats,
        now: Instant,
        stream_id: u32,
    ) {
        let cfg = self.log.config();
        let hb = HeartbeatFrame::new(cfg.session_id, stream_id);
        match transport.send_to(self.dst, bytemuck::bytes_of(&hb)) {
            Ok(_) => {
                stats.heartbeats_sent += 1;
                stats.bytes_sent += HeartbeatFrame::HEADER_LEN as u64;
                self.last_send_at = now;
            }
            Err(_) => stats.send_errors += 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::KernelUdp;
    use crate::wire::data_flags;
    use std::net::{IpAddr, Ipv4Addr};

    const STREAM_ID: u32 = 11;
    const INITIAL_TERM_ID: u32 = 100;
    const TERM_LENGTH: u32 = 64 * 1024;

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn cfg() -> MuxedSenderConfig {
        let mut c = MuxedSenderConfig::defaults();
        c.stream_id = STREAM_ID;
        c.initial_term_id = INITIAL_TERM_ID;
        c.term_length = TERM_LENGTH;
        // Long intervals so periodic Setup/Heartbeat don't pollute
        // tests that count specific frame types.
        c.setup_interval = Duration::from_secs(3600);
        c.heartbeat_interval = Duration::from_secs(3600);
        c
    }

    /// Drain `count` datagrams from `recv` (deadline-bounded so
    /// tests fail loud rather than hang).
    fn drain_n(recv: &KernelUdp, count: usize) -> Vec<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut buf = [0u8; 2048];
        while out.len() < count {
            if Instant::now() > deadline {
                panic!("timeout: got {}/{} datagrams", out.len(), count);
            }
            match recv.recv_from(&mut buf).unwrap() {
                Some((_, n)) => out.push(buf[..n].to_vec()),
                None => std::thread::sleep(Duration::from_micros(100)),
            }
        }
        out
    }

    #[test]
    fn create_session_returns_publog_and_counts() {
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let mut sender = MuxedSender::new(send_socket, cfg());
        assert_eq!(sender.session_count(), 0);

        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let log = sender
            .create_session(7, recv.local_addr().unwrap())
            .unwrap();
        assert_eq!(sender.session_count(), 1);
        assert_eq!(log.config().session_id, 7);
        assert_eq!(log.config().stream_id, STREAM_ID);
    }

    #[test]
    fn create_session_rejects_duplicate_session_id() {
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let mut sender = MuxedSender::new(send_socket, cfg());
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let dst = recv.local_addr().unwrap();
        let _ = sender.create_session(42, dst).unwrap();
        // Can't use unwrap_err here — Arc<PublicationLog> isn't
        // Debug, which Result::unwrap_err requires.
        match sender.create_session(42, dst) {
            Err(MuxedSenderError::SessionExists) => {}
            Err(other) => panic!("expected SessionExists, got {other:?}"),
            Ok(_) => panic!("expected SessionExists, got Ok"),
        }
        assert_eq!(sender.session_count(), 1);
    }

    #[test]
    fn create_session_rejects_past_max_sessions() {
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let mut config = cfg();
        config.max_sessions = 2;
        let mut sender = MuxedSender::new(send_socket, config);
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let dst = recv.local_addr().unwrap();
        sender.create_session(1, dst).unwrap();
        sender.create_session(2, dst).unwrap();
        match sender.create_session(3, dst) {
            Err(MuxedSenderError::SessionsExhausted) => {}
            Err(other) => panic!("expected SessionsExhausted, got {other:?}"),
            Ok(_) => panic!("expected SessionsExhausted, got Ok"),
        }
        assert_eq!(sender.session_count(), 2);
    }

    #[test]
    fn evict_removes_session_state() {
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let mut sender = MuxedSender::new(send_socket, cfg());
        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let _ = sender
            .create_session(5, recv.local_addr().unwrap())
            .unwrap();
        sender.evict(5);
        assert_eq!(sender.session_count(), 0);
        // Idempotent.
        sender.evict(5);
        assert_eq!(sender.session_count(), 0);
        // Re-allocation of the same session_id is now allowed.
        let _ = sender
            .create_session(5, recv.local_addr().unwrap())
            .unwrap();
        assert_eq!(sender.session_count(), 1);
    }

    #[test]
    fn two_sessions_drain_to_their_own_destinations() {
        // Each session's data fragments must arrive at its own
        // destination — NOT cross-routed. Locks down the
        // fundamental demux property of MuxedSender.
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let mut sender = MuxedSender::new(send_socket, cfg());

        let recv_a = KernelUdp::bind(loopback(0)).unwrap();
        let recv_b = KernelUdp::bind(loopback(0)).unwrap();
        let log_a = sender
            .create_session(1, recv_a.local_addr().unwrap())
            .unwrap();
        let log_b = sender
            .create_session(2, recv_b.local_addr().unwrap())
            .unwrap();

        // Publish on both publogs.
        {
            let mut claim = log_a.try_claim(8).unwrap();
            claim.payload_mut().copy_from_slice(b"AAAAAAAA");
            claim.publish(data_flags::UNFRAGMENTED);
        }
        {
            let mut claim = log_b.try_claim(8).unwrap();
            claim.payload_mut().copy_from_slice(b"BBBBBBBB");
            claim.publish(data_flags::UNFRAGMENTED);
        }

        // One tick should drain both.
        let stats = sender.tick();
        assert_eq!(stats.fragments_sent, 2);

        // Each receiver socket sees exactly one Data frame, with
        // its own session_id stamped in.
        let dgrams_a = drain_n(&recv_a, 1);
        let dgrams_b = drain_n(&recv_b, 1);
        match parse_frame(&dgrams_a[0]).unwrap() {
            FrameView::Data { header, payload } => {
                assert_eq!(header.session_id, 1);
                assert_eq!(payload, b"AAAAAAAA");
            }
            other => panic!("expected Data on recv_a, got {other:?}"),
        }
        match parse_frame(&dgrams_b[0]).unwrap() {
            FrameView::Data { header, payload } => {
                assert_eq!(header.session_id, 2);
                assert_eq!(payload, b"BBBBBBBB");
            }
            other => panic!("expected Data on recv_b, got {other:?}"),
        }
    }

    #[test]
    fn nak_for_session_a_does_not_retransmit_session_b() {
        // Cross-session NAK isolation: if session B sends a NAK
        // (perhaps a forged one with B's session_id but for
        // bytes that only exist in A's log), the muxer must
        // route by NAK's session_id, NOT broadcast across all
        // sessions. Locks down the routing-by-NAK-session_id
        // contract.
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let mut sender = MuxedSender::new(send_socket, cfg());

        let recv_a = KernelUdp::bind(loopback(0)).unwrap();
        let recv_a_addr = recv_a.local_addr().unwrap();
        let recv_b = KernelUdp::bind(loopback(0)).unwrap();
        let recv_b_addr = recv_b.local_addr().unwrap();
        let log_a = sender.create_session(1, recv_a_addr).unwrap();
        let log_b = sender.create_session(2, recv_b_addr).unwrap();

        // Publish on both so both have retransmit data available.
        // 64-byte payload → ~96B aligned fragment in the log; we
        // NAK that whole aligned fragment below.
        for log in [&log_a, &log_b] {
            let mut claim = log.try_claim(64).unwrap();
            claim.payload_mut().fill(0x77);
            claim.publish(data_flags::UNFRAGMENTED);
        }
        // Tick to push the original sends out so they're past
        // last_sent_position and eligible for retransmit.
        let stats = sender.tick();
        assert_eq!(stats.fragments_sent, 2);
        // Drain the original sends.
        let _ = drain_n(&recv_a, 1);
        let _ = drain_n(&recv_b, 1);

        // recv_a sends a NAK with session_id=1, asking for the
        // full 96-byte aligned fragment at offset 0.
        let nak = NakFrame::new(/*session_id*/ 1, STREAM_ID, INITIAL_TERM_ID, 0, 96);
        recv_a
            .send_to(
                sender.transport.local_addr().unwrap(),
                bytemuck::bytes_of(&nak),
            )
            .unwrap();

        // Tick to process the NAK — only session 1 should
        // retransmit.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got_retransmit = false;
        while Instant::now() < deadline && !got_retransmit {
            let s = sender.tick();
            if s.retransmits_sent >= 1 {
                got_retransmit = true;
            }
        }
        assert!(got_retransmit, "session A's NAK didn't trigger retransmit");

        // recv_a should see a retransmit Data frame (the original
        // bytes); recv_b should see nothing extra.
        let mut buf = [0u8; 2048];
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut a_got_retx = false;
        while !a_got_retx && Instant::now() < deadline {
            if let Some((_, n)) = recv_a.recv_from(&mut buf).unwrap() {
                if let Ok(FrameView::Data { header, .. }) = parse_frame(&buf[..n])
                    && header.session_id == 1
                {
                    a_got_retx = true;
                }
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(a_got_retx, "recv_a didn't see the retransmit");

        // recv_b: drain anything in flight; assert no Data with
        // session_id=1 (or 2 — there shouldn't be a B retransmit
        // either since we didn't NAK B).
        let mut leaked = false;
        for _ in 0..32 {
            match recv_b.recv_from(&mut buf).unwrap() {
                Some((_, n)) => {
                    if matches!(parse_frame(&buf[..n]), Ok(FrameView::Data { .. })) {
                        leaked = true;
                    }
                }
                None => break,
            }
        }
        assert!(
            !leaked,
            "B's recv socket got a retransmit it shouldn't have"
        );
    }

    #[test]
    fn nak_for_unknown_session_silently_dropped() {
        // An attacker (or a stale frame from an evicted session)
        // sends a NAK with a session_id we never allocated. The
        // muxer must NOT crash, NOT retransmit, NOT create a
        // session — just drop and count.
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let send_addr = send_socket.local_addr().unwrap();
        let mut sender = MuxedSender::new(send_socket, cfg());

        let attacker = KernelUdp::bind(loopback(0)).unwrap();
        let nak = NakFrame::new(/*session_id*/ 999, STREAM_ID, INITIAL_TERM_ID, 0, 96);
        attacker
            .send_to(send_addr, bytemuck::bytes_of(&nak))
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(1);
        let mut total_naks = 0u32;
        let mut total_retx = 0u32;
        while Instant::now() < deadline {
            let s = sender.tick();
            total_naks += s.naks_received;
            total_retx += s.retransmits_sent;
            if total_naks >= 1 {
                break;
            }
        }
        assert!(total_naks >= 1, "the NAK was never received");
        assert_eq!(total_retx, 0, "no session existed; must not retransmit");
        assert_eq!(sender.session_count(), 0);
    }

    #[test]
    fn sm_for_session_a_does_not_update_session_b_publisher_limit() {
        // Symmetric to the NAK isolation test, but for SMs. An SM
        // mutates the addressed session's `publisher_limit` via
        // `flow_control.compute_publisher_limit`. Cross-session
        // routing leakage would let session B's SM bump session
        // A's limit (or vice versa). Lock that down.
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let send_addr = send_socket.local_addr().unwrap();
        let mut sender = MuxedSender::new(send_socket, cfg());

        let recv_a = KernelUdp::bind(loopback(0)).unwrap();
        let recv_b = KernelUdp::bind(loopback(0)).unwrap();
        let log_a = sender
            .create_session(1, recv_a.local_addr().unwrap())
            .unwrap();
        let log_b = sender
            .create_session(2, recv_b.local_addr().unwrap())
            .unwrap();

        // Snapshot both publisher_limits before any SM lands.
        let limit_a_before = log_a.publisher_limit();
        let limit_b_before = log_b.publisher_limit();

        // Send an SM addressed at session 1 from a fresh socket.
        // consumption_position = end of the first term so flow
        // control's Min strategy will set publisher_limit to a
        // value derived from receiver_window (advancing it).
        let receiver_id = 100u64;
        let consumption_term_id = INITIAL_TERM_ID;
        let consumption_term_offset = 0u32;
        let receiver_window = 3 * TERM_LENGTH; // a typical SM window
        let sm = StatusMessage::new(
            /*session_id*/ 1,
            STREAM_ID,
            consumption_term_id,
            consumption_term_offset,
            receiver_window,
            receiver_id,
        );
        let sm_socket = KernelUdp::bind(loopback(0)).unwrap();
        sm_socket
            .send_to(send_addr, bytemuck::bytes_of(&sm))
            .unwrap();

        // Spin ticks until the SM is processed.
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let s = sender.tick();
            if s.sms_received >= 1 {
                break;
            }
        }

        // Session A's publisher_limit must have moved; session
        // B's must be unchanged. (The Min flow-control strategy
        // sets it to consumption_position + receiver_window.)
        assert_ne!(
            log_a.publisher_limit(),
            limit_a_before,
            "session A's publisher_limit should have updated from the SM",
        );
        assert_eq!(
            log_b.publisher_limit(),
            limit_b_before,
            "session B's publisher_limit must NOT have moved — SM was for A",
        );
    }

    #[test]
    fn periodic_setup_carries_per_session_session_id() {
        // The periodic Setup path runs per-session inside
        // `tick()`. Each session must stamp its own session_id
        // into the frame, NOT the muxer's config-level value. If
        // the wiring leaked, both sessions' Setups would carry
        // the same session_id and a multi-client subscriber would
        // get cross-routed.
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        let mut config = cfg();
        config.setup_interval = Duration::from_micros(100);
        let mut sender = MuxedSender::new(send_socket, config);

        let recv_a = KernelUdp::bind(loopback(0)).unwrap();
        let recv_b = KernelUdp::bind(loopback(0)).unwrap();
        let _log_a = sender
            .create_session(7, recv_a.local_addr().unwrap())
            .unwrap();
        let _log_b = sender
            .create_session(13, recv_b.local_addr().unwrap())
            .unwrap();

        // Sleep past setup_interval and tick — both sessions
        // should fire a Setup.
        std::thread::sleep(Duration::from_millis(2));
        let mut total_setups = 0u32;
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && total_setups < 2 {
            total_setups += sender.tick().setup_sent;
        }
        assert!(
            total_setups >= 2,
            "got {total_setups} setups across sessions"
        );

        // recv_a should see a Setup with session_id=7.
        let mut buf = [0u8; 2048];
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got_a = false;
        while !got_a && Instant::now() < deadline {
            if let Some((_, n)) = recv_a.recv_from(&mut buf).unwrap() {
                if let Ok(FrameView::Setup(s)) = parse_frame(&buf[..n])
                    && s.session_id == 7
                {
                    got_a = true;
                }
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(got_a, "recv_a did not receive a Setup with session_id=7");

        // recv_b: session_id=13.
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut got_b = false;
        while !got_b && Instant::now() < deadline {
            if let Some((_, n)) = recv_b.recv_from(&mut buf).unwrap() {
                if let Ok(FrameView::Setup(s)) = parse_frame(&buf[..n])
                    && s.session_id == 13
                {
                    got_b = true;
                }
            } else {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        assert!(got_b, "recv_b did not receive a Setup with session_id=13");
    }

    #[test]
    fn send_setup_now_fires_immediately_for_one_session() {
        // The on-demand `send_setup_now` should bypass the
        // setup_interval timer and fire a single Setup for the
        // requested session — useful at startup. No-op for
        // unknown sessions.
        let send_socket = KernelUdp::bind(loopback(0)).unwrap();
        // Long interval so periodic Setup doesn't muddy the test.
        let mut sender = MuxedSender::new(send_socket, cfg());

        let recv = KernelUdp::bind(loopback(0)).unwrap();
        let _log = sender
            .create_session(42, recv.local_addr().unwrap())
            .unwrap();

        let stats = sender.send_setup_now(42);
        assert_eq!(stats.setup_sent, 1);

        let dgrams = drain_n(&recv, 1);
        match parse_frame(&dgrams[0]).unwrap() {
            FrameView::Setup(s) => assert_eq!(s.session_id, 42),
            other => panic!("expected Setup, got {other:?}"),
        }

        // Calling for an unknown session is a no-op (no panic, no
        // emit).
        let stats = sender.send_setup_now(999);
        assert_eq!(stats.setup_sent, 0);
    }
}
