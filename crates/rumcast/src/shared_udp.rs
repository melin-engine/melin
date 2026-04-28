//! Shared UDP socket for peers that publish AND subscribe through
//! the same kernel endpoint.
//!
//! A typical rumcast peer pairs a publication (Sender) and a
//! subscription (Receiver). Each existing `SenderLoop` /
//! `ReceiverLoop` owns its own `UdpTransport` outright, so a peer
//! that wants both halves on the same kernel port can't directly
//! plug them in — two `KernelUdp::bind`s on the same address fail
//! with EADDRINUSE.
//!
//! This module provides [`SharedUdp`]: one bound socket, two
//! halves. Each half implements `UdpTransport`, so existing
//! `SenderLoop` / `ReceiverLoop` constructors take a half
//! unchanged. Internally, every `recv_from` drains the underlying
//! socket and routes incoming frames by type:
//!
//! - Data / Setup / Heartbeat → recv half (subscriber-bound).
//! - NAK / StatusMessage → send half (publisher-bound flow-control).
//! - Malformed bytes → dropped, no panic, counter bumped.
//!
//! # Why this matters for multi-client demux
//!
//! With two sockets per peer (one publisher, one subscriber), the
//! server's `MuxedReceiver` learns each peer's *publisher* source
//! addr — but the peer's *subscriber* lives on a different port,
//! so responses go to the wrong endpoint and only the first peer
//! gets routed correctly. With one socket per peer, the publisher
//! source addr IS the subscriber addr, and the server's auto-
//! discovery routes responses correctly to every client.
//!
//! # Threading
//!
//! Both halves are `Send + Sync`. They share state behind an
//! `Arc<Mutex<...>>`, so they may be used from different threads
//! safely. The lock is held only across the per-call drain
//! sequence (a few microseconds at most under normal load) — no
//! lock is taken on `send_to`. Single-thread embedders (e.g. the
//! server's `session_translator`) pay the uncontended-mutex cost
//! (~10ns per call) which is negligible vs. the syscall.
//!
//! ## Best-effort drain
//!
//! `recv_from` first checks its own queue under the lock, drops
//! the lock, then drains the socket. With two threads
//! concurrently in `recv_from` it's possible for thread A to enter
//! the slow path with an empty queue, find the socket empty, and
//! return `None` — even if thread B simultaneously enqueued a
//! frame for A while A was checking the socket. A's caller will
//! get the frame on its next `recv_from` call (they run in tight
//! loops), so the cost is one extra loop iteration (~µs), not
//! lost data.

use std::collections::VecDeque;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::{Arc, Mutex};

use crate::transport::{KernelUdp, UdpTransport};
use crate::wire::{FrameView, parse_frame};

/// Maximum frames queued for the OTHER half before we start dropping.
/// Bounds memory under one-sided drain. At 1024-byte frames and 64
/// entries per direction, the worst case per `SharedUdp` instance is
/// ~128 KiB of queued buffers.
const PER_DIRECTION_QUEUE_CAP: usize = 64;

/// Maximum bytes we copy out of the kernel per receive (matches
/// the rumcast `AlignedBuf<2048>` size used elsewhere).
const RECV_BUF_SIZE: usize = 2048;

/// One bound UDP socket whose incoming frames are demultiplexed
/// between a publisher half and a subscriber half. See module docs.
///
/// Construct via [`bind`], then call [`split`] to obtain the two
/// halves. `SharedUdp` itself can't be used as a transport — it's
/// purely the factory.
///
/// [`bind`]: SharedUdp::bind
/// [`split`]: SharedUdp::split
pub struct SharedUdp {
    inner: Arc<SharedInner>,
}

struct SharedInner {
    socket: KernelUdp,
    queues: Mutex<Queues>,
}

struct Queues {
    /// Frames classified as subscriber-bound (Data/Setup/Heartbeat)
    /// queued for the [`SharedUdpRecv`] half.
    recv: VecDeque<QueuedFrame>,
    /// Frames classified as publisher-bound (NAK/StatusMessage)
    /// queued for the [`SharedUdpSend`] half.
    send: VecDeque<QueuedFrame>,
    /// Frames dropped because the destination half's queue was full.
    /// Surfaced through [`SharedUdp::dropped_counts`] for diagnostics.
    recv_dropped: u64,
    send_dropped: u64,
    /// Frames dropped because they were unparseable / unknown frame
    /// type. Same surface.
    parse_dropped: u64,
}

#[derive(Debug)]
struct QueuedFrame {
    from: SocketAddr,
    bytes: Vec<u8>,
}

impl SharedUdp {
    /// Bind a fresh `KernelUdp` to `local` (non-blocking) and wrap
    /// it for shared use.
    pub fn bind(local: SocketAddr) -> io::Result<Self> {
        let socket = KernelUdp::bind(local)?;
        Ok(Self {
            inner: Arc::new(SharedInner {
                socket,
                queues: Mutex::new(Queues {
                    recv: VecDeque::with_capacity(PER_DIRECTION_QUEUE_CAP),
                    send: VecDeque::with_capacity(PER_DIRECTION_QUEUE_CAP),
                    recv_dropped: 0,
                    send_dropped: 0,
                    parse_dropped: 0,
                }),
            }),
        })
    }

    /// Bound local address — useful for tests and for embedders
    /// that need the actual port after binding ephemeral.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    /// Drain into the two halves. The `SharedUdp` is consumed —
    /// once split, the underlying socket is referenced only via
    /// the halves' `Arc` clones.
    pub fn split(self) -> (SharedUdpSend, SharedUdpRecv) {
        let send = SharedUdpSend {
            inner: Arc::clone(&self.inner),
        };
        let recv = SharedUdpRecv { inner: self.inner };
        (send, recv)
    }
}

/// Publisher-side half. Implements [`UdpTransport`]: `send_to`
/// passes through to the underlying socket; `recv_from` returns
/// only NAK / StatusMessage frames (publisher's flow-control inbox).
pub struct SharedUdpSend {
    inner: Arc<SharedInner>,
}

/// Subscriber-side half. Implements [`UdpTransport`]: `send_to`
/// passes through (used for SMs/NAKs going OUT to the publisher);
/// `recv_from` returns only Data / Setup / Heartbeat frames.
pub struct SharedUdpRecv {
    inner: Arc<SharedInner>,
}

/// Direction a parsed frame is bound for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    /// Subscriber-bound: Data, Setup, Heartbeat.
    Recv,
    /// Publisher-bound: NAK, StatusMessage.
    Send,
    /// Unparseable — drop and count.
    Drop,
}

fn classify(bytes: &[u8]) -> Direction {
    match parse_frame(bytes) {
        Ok(FrameView::Data { .. }) | Ok(FrameView::Setup(_)) | Ok(FrameView::Heartbeat(_)) => {
            Direction::Recv
        }
        Ok(FrameView::Nak(_)) | Ok(FrameView::StatusMessage(_)) => Direction::Send,
        Err(_) => Direction::Drop,
    }
}

/// Drain the underlying socket, dispatching every parseable frame
/// to the appropriate queue, and stop as soon as we get one that
/// matches `want`. Returns the (from, bytes) of that frame on
/// success, or `Ok(None)` if the socket has nothing pending.
///
/// Caller must NOT be holding `inner.queues` lock — we acquire it
/// per dispatch to keep the lock-hold window short.
fn drain_until(inner: &SharedInner, want: Direction) -> io::Result<Option<QueuedFrame>> {
    let mut tmp = [0u8; RECV_BUF_SIZE];
    loop {
        match inner.socket.recv_from(&mut tmp) {
            Ok(None) => return Ok(None),
            Err(e) => return Err(e),
            Ok(Some((from, len))) => {
                let dir = classify(&tmp[..len]);
                if dir == want {
                    return Ok(Some(QueuedFrame {
                        from,
                        bytes: tmp[..len].to_vec(),
                    }));
                }
                // Route to the OTHER half's queue or drop.
                let mut q = inner.queues.lock().expect("queues mutex poisoned");
                match dir {
                    Direction::Recv => {
                        if q.recv.len() >= PER_DIRECTION_QUEUE_CAP {
                            q.recv_dropped += 1;
                        } else {
                            q.recv.push_back(QueuedFrame {
                                from,
                                bytes: tmp[..len].to_vec(),
                            });
                        }
                    }
                    Direction::Send => {
                        if q.send.len() >= PER_DIRECTION_QUEUE_CAP {
                            q.send_dropped += 1;
                        } else {
                            q.send.push_back(QueuedFrame {
                                from,
                                bytes: tmp[..len].to_vec(),
                            });
                        }
                    }
                    Direction::Drop => {
                        q.parse_dropped += 1;
                    }
                }
            }
        }
    }
}

/// Try the local queue first; if empty, drain the socket dispatching
/// non-matching frames to the other half. Common body for both
/// halves' `recv_from`.
fn try_recv(
    inner: &SharedInner,
    direction: Direction,
    buf: &mut [u8],
) -> io::Result<Option<(SocketAddr, usize)>> {
    // Fast path: pop from our own queue if anything's waiting.
    {
        let mut q = inner.queues.lock().expect("queues mutex poisoned");
        let queue = match direction {
            Direction::Recv => &mut q.recv,
            Direction::Send => &mut q.send,
            Direction::Drop => unreachable!("Drop is not a callable direction"),
        };
        if let Some(frame) = queue.pop_front() {
            let n = frame.bytes.len().min(buf.len());
            buf[..n].copy_from_slice(&frame.bytes[..n]);
            return Ok(Some((frame.from, n)));
        }
    }
    // Slow path: drain the socket until we find one for us or it's empty.
    match drain_until(inner, direction)? {
        None => Ok(None),
        Some(frame) => {
            let n = frame.bytes.len().min(buf.len());
            buf[..n].copy_from_slice(&frame.bytes[..n]);
            Ok(Some((frame.from, n)))
        }
    }
}

impl UdpTransport for SharedUdpSend {
    #[inline]
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        // Send needs no synchronization — kernel handles concurrent
        // sends on the same socket.
        self.inner.socket.send_to(dst, bytes)
    }

    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        try_recv(&self.inner, Direction::Send, buf)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    fn join_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
        self.inner.socket.join_multicast_v4(group, iface)
    }

    fn leave_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
        self.inner.socket.leave_multicast_v4(group, iface)
    }
}

impl UdpTransport for SharedUdpRecv {
    #[inline]
    fn send_to(&self, dst: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.inner.socket.send_to(dst, bytes)
    }

    fn recv_from(&self, buf: &mut [u8]) -> io::Result<Option<(SocketAddr, usize)>> {
        try_recv(&self.inner, Direction::Recv, buf)
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    fn join_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
        self.inner.socket.join_multicast_v4(group, iface)
    }

    fn leave_multicast_v4(&self, group: Ipv4Addr, iface: Ipv4Addr) -> io::Result<()> {
        self.inner.socket.leave_multicast_v4(group, iface)
    }
}

/// Diagnostic: counts of frames dropped at the muxer due to queue
/// pressure or unparseable bytes. Useful for tests and for an
/// embedder's health endpoint. Returns `(recv_dropped,
/// send_dropped, parse_dropped)`.
impl SharedUdpSend {
    pub fn dropped_counts(&self) -> (u64, u64, u64) {
        let q = self.inner.queues.lock().expect("queues mutex poisoned");
        (q.recv_dropped, q.send_dropped, q.parse_dropped)
    }
}

impl SharedUdpRecv {
    pub fn dropped_counts(&self) -> (u64, u64, u64) {
        let q = self.inner.queues.lock().expect("queues mutex poisoned");
        (q.recv_dropped, q.send_dropped, q.parse_dropped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{DataFrame, HeartbeatFrame, NakFrame, SetupFrame, StatusMessage, data_flags};
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::{Duration, Instant};

    const SESSION: u32 = 7;
    const STREAM: u32 = 11;

    fn loopback(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn data_frame(payload: &[u8]) -> Vec<u8> {
        let header = DataFrame::new(
            SESSION,
            STREAM,
            /*term_id*/ 100,
            /*term_offset*/ 0,
            data_flags::UNFRAGMENTED,
            payload.len() as u32,
        );
        let mut buf = Vec::with_capacity(DataFrame::HEADER_LEN + payload.len());
        buf.extend_from_slice(bytemuck::bytes_of(&header));
        buf.extend_from_slice(payload);
        buf
    }

    fn nak_frame() -> Vec<u8> {
        let nak = NakFrame::new(SESSION, STREAM, 100, 0, 96);
        bytemuck::bytes_of(&nak).to_vec()
    }

    fn sm_frame() -> Vec<u8> {
        let sm = StatusMessage::new(SESSION, STREAM, 100, 0, 64 * 1024, 1);
        bytemuck::bytes_of(&sm).to_vec()
    }

    fn setup_frame() -> Vec<u8> {
        let s = SetupFrame::new(SESSION, STREAM, 100, 100, 0, 64 * 1024);
        bytemuck::bytes_of(&s).to_vec()
    }

    fn heartbeat_frame() -> Vec<u8> {
        let h = HeartbeatFrame::new(SESSION, STREAM);
        bytemuck::bytes_of(&h).to_vec()
    }

    /// Spin a half's `recv_from` for up to `deadline` until it
    /// returns Some. Panics on timeout — keeps tests loud.
    fn recv_one<T: UdpTransport>(t: &T, deadline: Instant) -> (SocketAddr, Vec<u8>) {
        let mut buf = [0u8; 2048];
        while Instant::now() < deadline {
            if let Some((from, len)) = t.recv_from(&mut buf).expect("recv_from failed") {
                return (from, buf[..len].to_vec());
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        panic!("no datagram within deadline");
    }

    #[test]
    fn local_addr_is_reported_consistently_across_halves() {
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (send_half, recv_half) = shared.split();
        assert_eq!(send_half.local_addr().unwrap(), bound);
        assert_eq!(recv_half.local_addr().unwrap(), bound);
    }

    #[test]
    fn data_frame_routes_to_recv_half() {
        // Data / Setup / Heartbeat are subscriber-bound; the recv
        // half's recv_from should yield them, the send half's
        // recv_from should NOT.
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (send_half, recv_half) = shared.split();

        // External peer fires a Data frame at our shared socket.
        let peer = KernelUdp::bind(loopback(0)).unwrap();
        let frame = data_frame(b"hello");
        peer.send_to(bound, &frame).unwrap();

        // Recv half receives it.
        let (_from, bytes) = recv_one(&recv_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(&bytes, &frame);

        // Send half sees nothing right now (queue empty,
        // socket drained).
        let mut buf = [0u8; 2048];
        assert!(send_half.recv_from(&mut buf).unwrap().is_none());
    }

    #[test]
    fn nak_routes_to_send_half() {
        // NAK / StatusMessage are publisher-bound; the send half
        // gets them.
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (send_half, recv_half) = shared.split();

        let peer = KernelUdp::bind(loopback(0)).unwrap();
        let frame = nak_frame();
        peer.send_to(bound, &frame).unwrap();

        let (_from, bytes) = recv_one(&send_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(&bytes, &frame);

        let mut buf = [0u8; 2048];
        assert!(recv_half.recv_from(&mut buf).unwrap().is_none());
    }

    #[test]
    fn sm_routes_to_send_half() {
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (send_half, _recv_half) = shared.split();

        let peer = KernelUdp::bind(loopback(0)).unwrap();
        let frame = sm_frame();
        peer.send_to(bound, &frame).unwrap();

        let (_from, bytes) = recv_one(&send_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(&bytes, &frame);
    }

    #[test]
    fn setup_routes_to_recv_half() {
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (_send_half, recv_half) = shared.split();

        let peer = KernelUdp::bind(loopback(0)).unwrap();
        let frame = setup_frame();
        peer.send_to(bound, &frame).unwrap();

        let (_from, bytes) = recv_one(&recv_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(&bytes, &frame);
    }

    #[test]
    fn heartbeat_routes_to_recv_half() {
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (_send_half, recv_half) = shared.split();

        let peer = KernelUdp::bind(loopback(0)).unwrap();
        let frame = heartbeat_frame();
        peer.send_to(bound, &frame).unwrap();

        let (_from, bytes) = recv_one(&recv_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(&bytes, &frame);
    }

    #[test]
    fn cross_route_buffers_for_other_half() {
        // The CALLING half drains the socket. Frames not for it get
        // queued for the other half. This test sends a NAK then a
        // Data; the recv half drains both (its drain enqueues the
        // NAK for send half), then the send half pops the NAK from
        // its queue without touching the socket.
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (send_half, recv_half) = shared.split();

        let peer = KernelUdp::bind(loopback(0)).unwrap();
        let nak = nak_frame();
        let data = data_frame(b"after");
        peer.send_to(bound, &nak).unwrap();
        peer.send_to(bound, &data).unwrap();

        // Recv half: drains BOTH packets from the socket — Data
        // returns directly, NAK queued for send half.
        let (_, recv_bytes) = recv_one(&recv_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(&recv_bytes, &data);

        // Send half: pops the queued NAK without going to socket.
        let (_, send_bytes) = recv_one(&send_half, Instant::now() + Duration::from_secs(2));
        assert_eq!(&send_bytes, &nak);
    }

    #[test]
    fn malformed_bytes_dropped_with_counter() {
        // An attacker (or a buggy peer) sends garbage. Both halves
        // should drain the socket without crashing or returning
        // the bytes; `dropped_counts` reflects the parse drop.
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (send_half, recv_half) = shared.split();

        let peer = KernelUdp::bind(loopback(0)).unwrap();
        peer.send_to(bound, &[0xFFu8; 10]).unwrap(); // not a valid frame

        // Drain — neither half should return anything.
        std::thread::sleep(Duration::from_millis(20));
        let mut buf = [0u8; 2048];
        assert!(recv_half.recv_from(&mut buf).unwrap().is_none());
        assert!(send_half.recv_from(&mut buf).unwrap().is_none());

        // The parse_dropped counter should be at least 1.
        let (_, _, parse_dropped) = recv_half.dropped_counts();
        assert!(parse_dropped >= 1, "expected parse_dropped >= 1");
    }

    #[test]
    fn send_to_is_pass_through_on_both_halves() {
        // Both halves can `send_to` independently — kernel handles
        // concurrent writes on a single UDP socket. No queue
        // interaction required.
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let (send_half, recv_half) = shared.split();

        let peer = KernelUdp::bind(loopback(0)).unwrap();
        let peer_addr = peer.local_addr().unwrap();

        send_half.send_to(peer_addr, b"from-send").unwrap();
        recv_half.send_to(peer_addr, b"from-recv").unwrap();

        // Peer sees both, in some order. UDP loopback preserves
        // order in practice; assert as a set rather than ordered
        // pair to keep the test robust.
        let mut got: Vec<Vec<u8>> = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut buf = [0u8; 64];
        while got.len() < 2 && Instant::now() < deadline {
            if let Some((_, n)) = peer.recv_from(&mut buf).unwrap() {
                got.push(buf[..n].to_vec());
            } else {
                std::thread::sleep(Duration::from_micros(100));
            }
        }
        assert_eq!(got.len(), 2, "expected 2 datagrams, got {}", got.len());
        let texts: Vec<&[u8]> = got.iter().map(|v| v.as_slice()).collect();
        assert!(texts.contains(&b"from-send".as_slice()));
        assert!(texts.contains(&b"from-recv".as_slice()));
    }

    #[test]
    fn queue_overflow_drops_with_counter() {
        // Stuff > PER_DIRECTION_QUEUE_CAP NAKs in while only
        // recv_half drains. The send_half's queue fills; further
        // NAKs get counted in send_dropped.
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (send_half, recv_half) = shared.split();

        let peer = KernelUdp::bind(loopback(0)).unwrap();
        let nak = nak_frame();

        // Send 2× the cap so the second half overflows.
        let total = PER_DIRECTION_QUEUE_CAP * 2 + 4;
        for _ in 0..total {
            peer.send_to(bound, &nak).unwrap();
        }
        // Give the kernel a moment to deliver.
        std::thread::sleep(Duration::from_millis(50));

        // recv_half drains the socket — finds NAKs (not for it),
        // queues for send_half. The send_half's queue fills at
        // PER_DIRECTION_QUEUE_CAP; the rest get dropped.
        let mut buf = [0u8; 2048];
        for _ in 0..total {
            // recv_half.recv_from returns None each time (no Data
            // for it), but along the way it drains the socket and
            // dispatches/drops NAKs.
            let _ = recv_half.recv_from(&mut buf).unwrap();
        }

        let (_, send_dropped, _) = send_half.dropped_counts();
        assert!(
            send_dropped > 0,
            "expected send_dropped > 0 after queue overflow, got {send_dropped}",
        );
    }

    #[test]
    fn halves_are_send_and_sync() {
        // Compile-time assert via trait bound. If `Arc<Mutex<...>>`
        // ever changes such that the halves stop being thread-safe,
        // this fails to compile rather than at runtime.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SharedUdpSend>();
        assert_send_sync::<SharedUdpRecv>();
    }

    #[test]
    fn halves_can_be_polled_concurrently_from_different_threads() {
        // The Send + Sync claim is a runtime property too: two
        // threads concurrently calling `recv_from` on different
        // halves must each see only their own type, never the
        // other half's. Verifies the Mutex actually protects the
        // shared state under real concurrent access (compile-time
        // Send + Sync alone wouldn't catch a missing lock).
        let shared = SharedUdp::bind(loopback(0)).unwrap();
        let bound = shared.local_addr().unwrap();
        let (send_half, recv_half) = shared.split();

        let send_handle = std::thread::spawn(move || {
            // Each thread polls until its deadline OR until it
            // collects a few of its expected frames. The strict
            // assertion is: every frame the thread sees has to be
            // of its expected type — never the wrong type.
            let mut got: Vec<Vec<u8>> = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut buf = [0u8; 2048];
            while Instant::now() < deadline && got.len() < 8 {
                if let Some((_, len)) = send_half.recv_from(&mut buf).unwrap() {
                    got.push(buf[..len].to_vec());
                } else {
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
            got
        });

        let recv_handle = std::thread::spawn(move || {
            let mut got: Vec<Vec<u8>> = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(2);
            let mut buf = [0u8; 2048];
            while Instant::now() < deadline && got.len() < 8 {
                if let Some((_, len)) = recv_half.recv_from(&mut buf).unwrap() {
                    got.push(buf[..len].to_vec());
                } else {
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
            got
        });

        // Pump 8 NAKs and 8 Data frames at the shared socket.
        let peer = KernelUdp::bind(loopback(0)).unwrap();
        for _ in 0..8 {
            peer.send_to(bound, &nak_frame()).unwrap();
            peer.send_to(bound, &data_frame(b"hi")).unwrap();
            // Slight pacing so the kernel buffer doesn't overflow
            // while threads are spinning up.
            std::thread::sleep(Duration::from_micros(100));
        }

        let send_got = send_handle.join().unwrap();
        let recv_got = recv_handle.join().unwrap();

        // Some frames may be lost to kernel buffer overflow or
        // missed-wakeup latency (documented). The strict
        // assertion: every frame the threads DID see is of the
        // correct type. No leakage in either direction.
        assert!(
            !send_got.is_empty(),
            "send half saw no frames; cross-thread visibility broken?"
        );
        assert!(
            !recv_got.is_empty(),
            "recv half saw no frames; cross-thread visibility broken?"
        );
        for bytes in &send_got {
            assert!(
                matches!(parse_frame(bytes), Ok(FrameView::Nak(_))),
                "send half got non-NAK frame: {bytes:?}",
            );
        }
        for bytes in &recv_got {
            assert!(
                matches!(parse_frame(bytes), Ok(FrameView::Data { .. })),
                "recv half got non-Data frame: {bytes:?}",
            );
        }
    }
}
