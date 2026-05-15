//! Blocking (synchronous) frame reader/writer for dedicated I/O threads.
//!
//! Same length-prefixed framing as the async TCP/UDS transports, but uses
//! `std::io::Read`/`Write` directly. Used by the server's reader and
//! response threads to avoid tokio task scheduling overhead on the hot path.

use std::io::{self, BufReader, BufWriter, Read, Write};

/// Maximum frame payload size (1 KiB). Same limit as the async transports.
const MAX_FRAME_SIZE: usize = 1024;

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
