//! TCP transport implementation.
//!
//! Sets `TCP_NODELAY = true` to avoid Nagle's algorithm adding latency.
//! Uses a 4-byte little-endian length prefix for framing.

use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};

use crate::transport::{TransportListener, TransportRead, TransportStream, TransportWrite};

/// Maximum frame payload size (1 KiB). Orders and execution reports are
/// well under 128 bytes; this limit guards against malformed length fields
/// consuming unbounded memory.
const MAX_FRAME_SIZE: usize = 1024;

/// TCP transport listener backed by `tokio::net::TcpListener`.
pub struct TcpTransportListener {
    listener: TcpListener,
}

impl TcpTransportListener {
    /// Bind to the given address and start listening.
    pub async fn bind(addr: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self { listener })
    }

    /// Returns the local address this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

impl TransportListener for TcpTransportListener {
    type Stream = TcpTransportStream;

    async fn accept(&mut self) -> io::Result<(TcpTransportStream, SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        stream.set_nodelay(true)?;
        Ok((TcpTransportStream { stream }, addr))
    }
}

/// A TCP stream that can be split into read/write halves.
pub struct TcpTransportStream {
    stream: TcpStream,
}

impl TcpTransportStream {
    /// Wrap an existing `TcpStream` (e.g., from a client connection).
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

impl TransportStream for TcpTransportStream {
    type Read = TcpTransportRead;
    type Write = TcpTransportWrite;

    fn into_split(self) -> (TcpTransportRead, TcpTransportWrite) {
        let (read, write) = self.stream.into_split();
        (
            TcpTransportRead { reader: read },
            TcpTransportWrite {
                writer: BufWriter::new(write),
            },
        )
    }
}

/// Read half of a TCP transport. Reads length-prefixed frames.
pub struct TcpTransportRead {
    reader: tokio::net::tcp::OwnedReadHalf,
}

impl TransportRead for TcpTransportRead {
    async fn read_frame(&mut self) -> io::Result<Option<Vec<u8>>> {
        // Read the 4-byte length prefix.
        let mut len_buf = [0u8; 4];
        match self.reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
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

        let mut frame = vec![0u8; len];
        self.reader.read_exact(&mut frame).await?;

        Ok(Some(frame))
    }
}

/// Write half of a TCP transport. Writes length-prefixed frames.
///
/// Uses `BufWriter` to batch small writes (tag + payload) into fewer
/// syscalls. Flushed explicitly after each batch (BatchEnd response).
pub struct TcpTransportWrite {
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
}

impl TransportWrite for TcpTransportWrite {
    async fn write_frame(&mut self, data: &[u8]) -> io::Result<()> {
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes()).await?;
        self.writer.write_all(data).await?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{TransportRead, TransportStream, TransportWrite};

    #[tokio::test]
    async fn frame_round_trip_over_loopback() {
        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        // Client connects.
        let client_stream = TcpStream::connect(addr).await.unwrap();
        client_stream.set_nodelay(true).unwrap();
        let client = TcpTransportStream::new(client_stream);
        let (mut client_read, mut client_write) = client.into_split();

        // Server accepts.
        let mut listener = listener;
        let (server_stream, _) = listener.accept().await.unwrap();
        let (mut server_read, mut server_write) = server_stream.into_split();

        // Client sends, server receives.
        let data = b"hello trading";
        client_write.write_frame(data).await.unwrap();
        client_write.flush().await.unwrap();

        let received = server_read.read_frame().await.unwrap().unwrap();
        assert_eq!(received, data);

        // Server sends back, client receives.
        let reply = b"ack";
        server_write.write_frame(reply).await.unwrap();
        server_write.flush().await.unwrap();

        let received = client_read.read_frame().await.unwrap().unwrap();
        assert_eq!(received, reply);
    }

    #[tokio::test]
    async fn clean_disconnect_returns_none() {
        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let client_stream = TcpStream::connect(addr).await.unwrap();
        let mut listener = listener;
        let (server_stream, _) = listener.accept().await.unwrap();
        let (mut server_read, _server_write) = server_stream.into_split();

        // Drop client → server sees clean disconnect.
        drop(client_stream);

        let result = server_read.read_frame().await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn oversized_frame_rejected() {
        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let mut client_stream = TcpStream::connect(addr).await.unwrap();
        let mut listener = listener;
        let (server_stream, _) = listener.accept().await.unwrap();
        let (mut server_read, _server_write) = server_stream.into_split();

        // Send a length prefix claiming 2 MiB.
        let fake_len = 2_000_000u32;
        client_stream
            .write_all(&fake_len.to_le_bytes())
            .await
            .unwrap();

        let result = server_read.read_frame().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn multiple_frames_in_sequence() {
        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let client_stream = TcpStream::connect(addr).await.unwrap();
        client_stream.set_nodelay(true).unwrap();
        let client = TcpTransportStream::new(client_stream);
        let (_client_read, mut client_write) = client.into_split();

        let mut listener = listener;
        let (server_stream, _) = listener.accept().await.unwrap();
        let (mut server_read, _server_write) = server_stream.into_split();

        // Send 100 frames.
        for i in 0u32..100 {
            let data = i.to_le_bytes();
            client_write.write_frame(&data).await.unwrap();
        }
        client_write.flush().await.unwrap();

        // Read them all back.
        for i in 0u32..100 {
            let frame = server_read.read_frame().await.unwrap().unwrap();
            assert_eq!(frame, i.to_le_bytes());
        }
    }
}
