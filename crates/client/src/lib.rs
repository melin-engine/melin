//! Client library for connecting to the trading server.
//!
//! Provides a typed API over the binary wire protocol. Connects via TCP,
//! sends requests, and collects response batches.

use std::io;
use std::net::SocketAddr;

use tokio::net::TcpStream;

use trading_protocol::codec;
use trading_protocol::error::ProtocolError;
use trading_protocol::message::{Request, Response};
use trading_protocol::tcp::{TcpTransportRead, TcpTransportStream, TcpTransportWrite};
use trading_protocol::transport::{TransportRead, TransportStream, TransportWrite};

/// Error returned by client operations.
#[derive(Debug)]
pub enum ClientError {
    /// I/O error (connection lost, etc.).
    Io(io::Error),
    /// Protocol encoding/decoding error.
    Protocol(ProtocolError),
    /// Server closed the connection before sending BatchEnd.
    Disconnected,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::Disconnected => write!(f, "disconnected from server"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(e: ProtocolError) -> Self {
        Self::Protocol(e)
    }
}

/// Client connection to the trading server.
///
/// Sends requests and receives response batches synchronously (one
/// request at a time). For pipelining, a more sophisticated approach
/// with request IDs and multiplexing would be needed.
pub struct Client {
    reader: TcpTransportRead,
    writer: TcpTransportWrite,
    /// Pre-allocated encode buffer. 128 bytes is sufficient for all
    /// request types (the largest is SubmitOrder with a StopLimit at ~60 bytes).
    encode_buf: [u8; 128],
}

impl Client {
    /// Connect to a trading server at the given address.
    pub async fn connect(addr: SocketAddr) -> Result<Self, ClientError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let transport = TcpTransportStream::new(stream);
        let (reader, writer) = transport.into_split();
        Ok(Self {
            reader,
            writer,
            encode_buf: [0u8; 128],
        })
    }

    /// Send a request and collect all responses until BatchEnd.
    ///
    /// Returns the list of responses (excluding the BatchEnd marker itself).
    pub async fn send_request(&mut self, request: &Request) -> Result<Vec<Response>, ClientError> {
        // Encode and send.
        let written = codec::encode_request(request, &mut self.encode_buf)?;
        // write_frame expects payload without length prefix; encode_request
        // writes [length(4) | tag+payload], so skip the prefix.
        self.writer
            .write_frame(&self.encode_buf[4..written])
            .await?;
        self.writer.flush().await?;

        // Collect responses until BatchEnd.
        let mut responses = Vec::new();
        loop {
            let frame = self
                .reader
                .read_frame()
                .await?
                .ok_or(ClientError::Disconnected)?;

            let response = codec::decode_response(&frame)?;
            match response {
                Response::BatchEnd => break,
                other => responses.push(other),
            }
        }

        Ok(responses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_protocol::tcp::TcpTransportListener;
    use trading_protocol::transport::TransportListener;
    use trading_protocol::types::{OrderId, Symbol};

    /// Mock server that reads one request and responds with BatchEnd.
    async fn mock_batch_end_server(mut listener: TcpTransportListener) {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = stream.into_split();

        // Read one request frame (discard it).
        let _frame = reader.read_frame().await.unwrap().unwrap();

        // Respond with BatchEnd.
        let mut buf = [0u8; 128];
        let written = codec::encode_response(&Response::BatchEnd, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).await.unwrap();
        writer.flush().await.unwrap();
    }

    #[tokio::test]
    async fn connect_send_receive_batch_end() {
        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(mock_batch_end_server(listener));

        let mut client = Client::connect(addr).await.unwrap();
        let responses = client
            .send_request(&Request::CancelOrder {
                symbol: Symbol(1),
                order_id: OrderId(42),
            })
            .await
            .unwrap();

        // No reports before BatchEnd — just an empty batch.
        assert!(responses.is_empty());
    }

    #[tokio::test]
    async fn disconnect_before_batch_end_is_error() {
        let listener = TcpTransportListener::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        // Server accepts and immediately drops the connection.
        tokio::spawn(async move {
            let mut listener = listener;
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, _writer) = stream.into_split();
            let _frame = reader.read_frame().await.unwrap();
            // Drop without sending BatchEnd.
        });

        let mut client = Client::connect(addr).await.unwrap();
        let result = client
            .send_request(&Request::CancelOrder {
                symbol: Symbol(1),
                order_id: OrderId(42),
            })
            .await;

        assert!(result.is_err());
    }
}
