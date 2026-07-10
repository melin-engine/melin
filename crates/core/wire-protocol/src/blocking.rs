//! Blocking (synchronous) frame reader/writer for dedicated I/O threads.
//!
//! Same length-prefixed framing as the async TCP/UDS transports, but uses
//! `std::io::Read`/`Write` directly. Used by the server's reader and
//! response threads to avoid tokio task scheduling overhead on the hot path.

use std::io::{self, BufReader, BufWriter, Read, Write};
use std::time::{Duration, Instant};

/// Maximum frame payload size (1 KiB). Same limit as the async transports.
const MAX_FRAME_SIZE: usize = 1024;

/// Remaining budget until `deadline`, floored at 1 ms. The socket
/// timeout APIs (`SO_RCVTIMEO`/`SO_SNDTIMEO`) treat a zero timeval as
/// "block forever", so a sub-millisecond remainder must round UP to a
/// real 1 ms timeout, never truncate to the zero the kernel reads as
/// "no timeout"; a fully spent budget is [`io::ErrorKind::TimedOut`],
/// never a syscall with an unbounded wait. This is the single source of
/// truth for the timeval-zero hazard that every deadline-bounded
/// blocking handshake in the codebase must respect.
pub fn remaining_budget(deadline: Instant) -> io::Result<Duration> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining < Duration::from_millis(1) {
        return Err(io::Error::new(io::ErrorKind::TimedOut, "deadline exceeded"));
    }
    Ok(remaining)
}

/// A socket whose read/write timeouts can be re-armed, so the deadline
/// helpers below stay generic over `TcpStream` (production) and
/// `UnixStream` (tests) without raw-fd `setsockopt` plumbing. Arming
/// with a [`remaining_budget`]-vetted duration is what keeps a
/// near-expired remainder from truncating to the "no timeout" zero
/// timeval.
pub trait DeadlineSocket {
    fn arm_read_deadline(&self, dur: Duration) -> io::Result<()>;
    fn arm_write_deadline(&self, dur: Duration) -> io::Result<()>;
}

impl DeadlineSocket for std::net::TcpStream {
    fn arm_read_deadline(&self, dur: Duration) -> io::Result<()> {
        self.set_read_timeout(Some(dur))
    }
    fn arm_write_deadline(&self, dur: Duration) -> io::Result<()> {
        self.set_write_timeout(Some(dur))
    }
}

impl DeadlineSocket for std::os::unix::net::UnixStream {
    fn arm_read_deadline(&self, dur: Duration) -> io::Result<()> {
        self.set_read_timeout(Some(dur))
    }
    fn arm_write_deadline(&self, dur: Duration) -> io::Result<()> {
        self.set_write_timeout(Some(dur))
    }
}

/// `read_exact` under a whole-transfer deadline: the socket read timeout
/// is re-armed with the *remaining* budget before every syscall, so
/// partial progress (a byte-dribbling peer) shrinks the budget instead
/// of resetting it — total wall time is bounded by `deadline` no matter
/// how the bytes arrive. `WouldBlock`/`TimedOut` are a timeout tick and
/// `Interrupted` a signal; both loop back to re-check the budget, which
/// errors out once it is spent.
pub fn read_exact_deadline<S: Read + DeadlineSocket>(
    s: &mut S,
    buf: &mut [u8],
    deadline: Instant,
) -> io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        s.arm_read_deadline(remaining_budget(deadline)?)?;
        match s.read(&mut buf[filled..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "peer closed during deadline read",
                ));
            }
            Ok(n) => filled += n,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut
                    || e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// `write_all` + flush under the same whole-transfer deadline contract
/// as [`read_exact_deadline`].
pub fn write_all_deadline<S: Write + DeadlineSocket>(
    s: &mut S,
    buf: &[u8],
    deadline: Instant,
) -> io::Result<()> {
    let mut written = 0;
    while written < buf.len() {
        s.arm_write_deadline(remaining_budget(deadline)?)?;
        match s.write(&buf[written..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "peer stopped accepting bytes during deadline write",
                ));
            }
            Ok(n) => written += n,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut
                    || e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    s.flush()
}

/// Read one length-prefixed frame (4-byte LE length, then payload) under
/// a whole-read deadline, rejecting a length over `max_len` before
/// allocating. Per-syscall re-arm via [`read_exact_deadline`], so a peer
/// cannot hold the read open past `deadline` by trickling bytes.
pub fn read_frame_deadline<S: Read + DeadlineSocket>(
    s: &mut S,
    max_len: usize,
    deadline: Instant,
) -> io::Result<Vec<u8>> {
    let mut len_bytes = [0u8; 4];
    read_exact_deadline(s, &mut len_bytes, deadline)?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > max_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {len} bytes (max {max_len})"),
        ));
    }
    let mut frame = vec![0u8; len];
    read_exact_deadline(s, &mut frame, deadline)?;
    Ok(frame)
}

/// Blocking frame reader. Reads length-prefixed frames from any `Read` source.
///
/// Uses `BufReader` to amortize read syscalls — a single recv fills the
/// buffer with many frames, so subsequent `read_exact` calls hit the
/// buffer instead of making kernel transitions. This is critical for
/// round-trip latency: without buffering, each frame requires 2 read
/// syscalls (4-byte length prefix + payload).
///
/// Generic over the reader type so it works with both `std::net::TcpStream`
/// and `std::os::unix::net::UnixStream`.
pub struct BlockingFrameReader<R> {
    reader: BufReader<R>,
    /// Reusable frame buffer — avoids a heap allocation per frame.
    /// Fixed at MAX_FRAME_SIZE (1 KiB); the valid slice is `&buf[..len]`.
    buf: [u8; MAX_FRAME_SIZE],
    /// Length of the last successfully read frame (valid bytes in `buf`).
    frame_len: usize,
}

impl<R: Read> BlockingFrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            buf: [0u8; MAX_FRAME_SIZE],
            frame_len: 0,
        }
    }

    /// Read the next complete frame into the internal buffer.
    /// Returns a borrowed slice of the frame payload, or `None` on clean
    /// disconnect. The slice is valid until the next `read_frame()` call.
    pub fn read_frame(&mut self) -> io::Result<Option<&[u8]>> {
        // Read the 4-byte length prefix.
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame too large: {len} bytes (max {MAX_FRAME_SIZE})"),
            ));
        }

        self.reader.read_exact(&mut self.buf[..len])?;
        self.frame_len = len;

        Ok(Some(&self.buf[..len]))
    }

    /// Borrow the underlying reader. Mirrors `BufReader::get_ref` — used
    /// by callers that need to reach the raw stream for socket-level
    /// configuration (`set_read_timeout`, `set_nodelay`, …) without
    /// going through the framed layer.
    pub fn get_ref(&self) -> &R {
        self.reader.get_ref()
    }
}

/// Blocking frame writer. Writes length-prefixed frames to any `Write` sink.
///
/// Uses `BufWriter` to batch small writes (length prefix + payload) into
/// fewer syscalls. Flushed explicitly after each batch.
pub struct BlockingFrameWriter<W: Write> {
    writer: BufWriter<W>,
}

impl<W: Write> BlockingFrameWriter<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: BufWriter::new(writer),
        }
    }

    /// Write a complete frame (prepends the 4-byte LE length prefix).
    pub fn write_frame(&mut self, data: &[u8]) -> io::Result<()> {
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(data)?;
        Ok(())
    }

    /// Flush buffered data to the underlying writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    use std::io::Write;
    use std::os::unix::net::UnixStream;
    use std::thread;

    fn far() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    #[test]
    fn remaining_budget_floors_and_expires() {
        // A comfortably future deadline yields a positive budget.
        assert!(remaining_budget(far()).unwrap() > Duration::from_secs(1));
        // A spent deadline is TimedOut, never a zero duration.
        let spent = Instant::now();
        let err = remaining_budget(spent).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        // A sub-millisecond remainder still errors (rounds to expiry, not
        // to the zero timeval the kernel reads as "no timeout").
        let almost = Instant::now() + Duration::from_micros(200);
        thread::sleep(Duration::from_micros(50));
        // Either already spent, or floored — never a zero-or-below wait.
        match remaining_budget(almost) {
            Ok(d) => assert!(d >= Duration::from_millis(1)),
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::TimedOut),
        }
    }

    #[test]
    fn spent_budget_errors_without_a_syscall() {
        // A past deadline must fail before touching the socket — no bytes
        // are consumed even though the peer sent some.
        let (mut a, mut b) = UnixStream::pair().unwrap();
        b.write_all(&[1, 2, 3, 4]).unwrap();
        let mut buf = [0u8; 4];
        let err = read_exact_deadline(&mut a, &mut buf, Instant::now()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(buf, [0u8; 4], "no bytes should have been read");
    }

    #[test]
    fn frame_round_trips_within_the_deadline() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let payload = b"hello deadline";
        let mut framed = (payload.len() as u32).to_le_bytes().to_vec();
        framed.extend_from_slice(payload);
        b.write_all(&framed).unwrap();
        let got = read_frame_deadline(&mut a, 1024, far()).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn read_frame_deadline_rejects_oversized_length() {
        let (mut a, mut b) = UnixStream::pair().unwrap();
        b.write_all(&2_000_000u32.to_le_bytes()).unwrap();
        let err = read_frame_deadline(&mut a, 1024, far()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn dribbled_frame_cannot_outlive_the_deadline() {
        // A peer trickling bytes must be cut off at the whole-read
        // deadline: each partial read re-arms the timeout with the
        // remaining budget, so progress does not reset the clock.
        let (mut a, mut b) = UnixStream::pair().unwrap();
        let deadline = Instant::now() + Duration::from_millis(300);
        let started = Instant::now();
        let dribble = thread::spawn(move || {
            // A 4-byte length prefix promising a payload, dribbled one
            // byte every 80 ms — under any per-syscall timeout but past
            // the 300 ms whole-read deadline collectively.
            for byte in [8u8, 0, 0, 0, 1, 2, 3] {
                if b.write_all(&[byte]).is_err() {
                    return;
                }
                thread::sleep(Duration::from_millis(80));
            }
        });
        let err = read_frame_deadline(&mut a, 1024, deadline).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "deadline must bound the dribbler (took {:?})",
            started.elapsed()
        );
        dribble.join().unwrap();
    }

    #[test]
    fn write_all_deadline_delivers() {
        let (a, mut b) = UnixStream::pair().unwrap();
        let mut a = a;
        write_all_deadline(&mut a, b"payload", far()).unwrap();
        let mut buf = [0u8; 7];
        std::io::Read::read_exact(&mut b, &mut buf).unwrap();
        assert_eq!(&buf, b"payload");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};

    #[test]
    fn frame_round_trip_blocking() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        let mut writer = BlockingFrameWriter::new(client);
        let mut reader = BlockingFrameReader::new(server);

        let data = b"hello trading";
        writer.write_frame(data).unwrap();
        writer.flush().unwrap();

        let received = reader.read_frame().unwrap().unwrap();
        assert_eq!(received, data);
    }

    #[test]
    fn clean_disconnect_returns_none() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        drop(client);

        let mut reader = BlockingFrameReader::new(server);
        let result = reader.read_frame().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn oversized_frame_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        // Send a length prefix claiming 2 MiB.
        let fake_len = 2_000_000u32;
        client.write_all(&fake_len.to_le_bytes()).unwrap();

        let mut reader = BlockingFrameReader::new(server);
        let result = reader.read_frame();
        assert!(result.is_err());
    }

    #[test]
    fn multiple_frames_in_sequence() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();

        let mut writer = BlockingFrameWriter::new(client);
        let mut reader = BlockingFrameReader::new(server);

        for i in 0u32..100 {
            writer.write_frame(&i.to_le_bytes()).unwrap();
        }
        writer.flush().unwrap();

        for i in 0u32..100 {
            let frame = reader.read_frame().unwrap().unwrap();
            assert_eq!(frame, i.to_le_bytes());
        }
    }
}
