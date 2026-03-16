//! Client library for connecting to the trading server.
//!
//! Provides a typed API over the binary wire protocol. Connects via TCP,
//! sends requests, and collects response batches using blocking I/O.

use std::io;
use std::net::SocketAddr;

use ed25519_dalek::{Signer, SigningKey};

use trading_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
use trading_protocol::codec;
use trading_protocol::error::ProtocolError;
use trading_protocol::message::{Request, ResponseKind};

/// Error returned by client operations.
#[derive(Debug)]
pub enum ClientError {
    /// I/O error (connection lost, etc.).
    Io(io::Error),
    /// Protocol encoding/decoding error.
    Protocol(ProtocolError),
    /// Server closed the connection before sending BatchEnd.
    Disconnected,
    /// Server rejected the Ed25519 challenge-response authentication
    /// (unknown key, invalid signature, or wrong key permissions).
    AuthFailed,
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(e) => write!(f, "protocol error: {e}"),
            Self::Disconnected => write!(f, "disconnected from server"),
            Self::AuthFailed => write!(f, "authentication failed"),
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
/// request at a time, blocking I/O). For pipelining, use
/// `BlockingFrameReader`/`BlockingFrameWriter` directly.
pub struct Client {
    reader: BlockingFrameReader<std::net::TcpStream>,
    writer: BlockingFrameWriter<std::net::TcpStream>,
    /// Pre-allocated encode buffer. 128 bytes is sufficient for all
    /// request types (the largest is SubmitOrder with a StopLimit at ~60 bytes).
    encode_buf: [u8; 128],
}

impl Client {
    /// Connect to a trading server with Ed25519 challenge-response auth.
    ///
    /// 1. Receives a `Challenge` (32-byte nonce) from the server.
    /// 2. Signs the nonce with the provided `SigningKey`.
    /// 3. Sends a `ChallengeResponse` (signature + public key).
    /// 4. Waits for `ServerReady` (success) or `AuthFailed`.
    pub fn connect(addr: SocketAddr, key: &SigningKey) -> Result<Self, ClientError> {
        let stream = std::net::TcpStream::connect(addr)?;
        stream.set_nodelay(true)?;
        let mut reader = BlockingFrameReader::new(stream.try_clone()?);
        let mut writer = BlockingFrameWriter::new(stream);

        // Step 1: Receive Challenge from server.
        let frame = reader.read_frame()?.ok_or(ClientError::Disconnected)?;
        let response = codec::decode_response(frame)?;
        let nonce = match response {
            ResponseKind::Challenge { nonce } => nonce,
            _ => {
                return Err(ClientError::Protocol(ProtocolError::InvalidField(
                    "expected Challenge",
                )));
            }
        };

        // Step 2: Sign the nonce and send ChallengeResponse.
        let signature = key.sign(&nonce);
        let public_key = key.verifying_key().to_bytes();
        let request = Request::ChallengeResponse {
            signature: signature.to_bytes(),
            public_key,
        };
        let mut encode_buf = [0u8; 256];
        let written = codec::encode_request(&request, &mut encode_buf)?;
        writer.write_frame(&encode_buf[4..written])?;
        writer.flush()?;

        // Step 3: Wait for ServerReady or AuthFailed.
        let frame = reader.read_frame()?.ok_or(ClientError::Disconnected)?;
        let response = codec::decode_response(frame)?;
        match response {
            ResponseKind::ServerReady => {}
            ResponseKind::AuthFailed => {
                return Err(ClientError::AuthFailed);
            }
            _ => {
                return Err(ClientError::Protocol(ProtocolError::InvalidField(
                    "expected ServerReady or AuthFailed",
                )));
            }
        }

        Ok(Self {
            reader,
            writer,
            encode_buf: [0u8; 128],
        })
    }

    /// Send a request and collect all responses until BatchEnd.
    ///
    /// Returns the list of responses (excluding the BatchEnd marker itself).
    pub fn send_request(&mut self, request: &Request) -> Result<Vec<ResponseKind>, ClientError> {
        // Encode and send.
        let written = codec::encode_request(request, &mut self.encode_buf)?;
        // write_frame expects payload without length prefix; encode_request
        // writes [length(4) | tag+payload], so skip the prefix.
        self.writer.write_frame(&self.encode_buf[4..written])?;
        self.writer.flush()?;

        // Collect responses until BatchEnd. Heartbeats received during
        // idle periods are silently consumed (not part of a request batch).
        let mut responses = Vec::new();
        loop {
            let frame = self.reader.read_frame()?.ok_or(ClientError::Disconnected)?;

            let response = codec::decode_response(frame)?;
            match response {
                ResponseKind::BatchEnd => break,
                ResponseKind::Heartbeat | ResponseKind::ServerReady => continue,
                other => responses.push(other),
            }
        }

        Ok(responses)
    }

    /// Query server stats. Returns `(active_connections, events_processed, journal_sequence)`.
    ///
    /// Sends `QueryStats` and extracts the `StatsHeader` from the response batch.
    pub fn query_stats(&mut self) -> Result<StatsSnapshot, ClientError> {
        let responses = self.send_request(&Request::QueryStats)?;
        for resp in &responses {
            if let ResponseKind::StatsHeader {
                active_connections,
                events_processed,
                journal_sequence,
            } = resp
            {
                return Ok(StatsSnapshot {
                    active_connections: *active_connections,
                    events_processed: *events_processed,
                    journal_sequence: *journal_sequence,
                });
            }
        }
        Err(ClientError::Protocol(ProtocolError::InvalidField(
            "no StatsHeader in response",
        )))
    }
}

/// Snapshot of server stats returned by [`Client::query_stats`].
#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub active_connections: u64,
    pub events_processed: u64,
    pub journal_sequence: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use trading_protocol::types::{OrderId, Symbol};

    /// Generate a test signing key from a fixed seed for deterministic tests.
    fn test_key() -> SigningKey {
        SigningKey::from_bytes(&[0xAA; 32])
    }

    /// Run the server side of the challenge-response handshake, accepting
    /// any valid signature from the test key.
    fn mock_auth_handshake(
        reader: &mut BlockingFrameReader<std::net::TcpStream>,
        writer: &mut BlockingFrameWriter<std::net::TcpStream>,
    ) {
        use ed25519_dalek::{Verifier, VerifyingKey};

        // Send Challenge.
        let nonce = [0xBB; 32];
        let mut buf = [0u8; 64];
        let written = codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();

        // Read ChallengeResponse.
        let frame = reader.read_frame().unwrap().unwrap();
        let request = codec::decode_request(frame).unwrap();
        let (sig_bytes, pk_bytes) = match request {
            Request::ChallengeResponse {
                signature,
                public_key,
            } => (signature, public_key),
            _ => panic!("expected ChallengeResponse"),
        };

        // Verify signature.
        let vk = VerifyingKey::from_bytes(&pk_bytes).unwrap();
        let sig = ed25519_dalek::Signature::from_bytes(&sig_bytes);
        vk.verify(&nonce, &sig).unwrap();

        // Send ServerReady.
        let written = codec::encode_response(&ResponseKind::ServerReady, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();
    }

    /// Mock server that authenticates, reads one request, responds with BatchEnd.
    fn mock_batch_end_server(listener: std::net::TcpListener) {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
        let mut writer = BlockingFrameWriter::new(stream);

        mock_auth_handshake(&mut reader, &mut writer);

        // Read one request frame (discard it).
        let _frame = reader.read_frame().unwrap().unwrap();

        // Respond with BatchEnd.
        let mut buf = [0u8; 128];
        let written = codec::encode_response(&ResponseKind::BatchEnd, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn connect_send_receive_batch_end() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || mock_batch_end_server(listener));

        let key = test_key();
        let mut client = Client::connect(addr, &key).unwrap();
        let responses = client
            .send_request(&Request::CancelOrder {
                symbol: Symbol(1),
                order_id: OrderId(42),
            })
            .unwrap();

        // No reports before BatchEnd — just an empty batch.
        assert!(responses.is_empty());
    }

    #[test]
    fn auth_failed_returns_auth_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server sends Challenge then AuthFailed (simulating unknown key).
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);

            // Send Challenge.
            let nonce = [0xBB; 32];
            let mut buf = [0u8; 64];
            let written =
                codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();

            // Read ChallengeResponse (discard it).
            let _frame = reader.read_frame().unwrap().unwrap();

            // Send AuthFailed.
            let written = codec::encode_response(&ResponseKind::AuthFailed, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();
        });

        let key = test_key();
        let result = Client::connect(addr, &key);
        assert!(matches!(result, Err(ClientError::AuthFailed)));
    }

    #[test]
    fn server_disconnects_during_auth_is_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server sends Challenge, reads ChallengeResponse, then drops
        // without sending ServerReady.
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);

            let nonce = [0xBB; 32];
            let mut buf = [0u8; 64];
            let written =
                codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();

            // Consume the ChallengeResponse, then drop.
            let _ = reader.read_frame();
        });

        let key = test_key();
        let result = Client::connect(addr, &key);
        assert!(result.is_err());
    }

    #[test]
    fn server_sends_non_challenge_first_is_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server sends ServerReady instead of Challenge as first message.
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut writer = BlockingFrameWriter::new(stream);

            let mut buf = [0u8; 8];
            let written = codec::encode_response(&ResponseKind::ServerReady, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();
        });

        let key = test_key();
        let result = Client::connect(addr, &key);
        assert!(matches!(result, Err(ClientError::Protocol(_))));
    }

    #[test]
    fn server_sends_unexpected_response_after_auth() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server sends Challenge, reads ChallengeResponse, then sends
        // a Heartbeat instead of ServerReady/AuthFailed.
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);

            // Send Challenge.
            let nonce = [0xBB; 32];
            let mut buf = [0u8; 64];
            let written =
                codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();

            // Read ChallengeResponse.
            let _frame = reader.read_frame().unwrap().unwrap();

            // Send Heartbeat instead of ServerReady/AuthFailed.
            let written = codec::encode_response(&ResponseKind::Heartbeat, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();
        });

        let key = test_key();
        let result = Client::connect(addr, &key);
        assert!(matches!(result, Err(ClientError::Protocol(_))));
    }

    #[test]
    fn disconnect_before_batch_end_is_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server authenticates, reads one request, then drops.
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);
            mock_auth_handshake(&mut reader, &mut writer);
            let _frame = reader.read_frame().unwrap();
            // Drop without sending BatchEnd.
        });

        let key = test_key();
        let mut client = Client::connect(addr, &key).unwrap();
        let result = client.send_request(&Request::CancelOrder {
            symbol: Symbol(1),
            order_id: OrderId(42),
        });

        assert!(result.is_err());
    }
}
