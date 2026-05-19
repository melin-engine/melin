//! TCP-backed client. Default transport. Blocking I/O over a single
//! TCP socket; connect performs the four-message Ed25519 challenge-
//! response handshake and returns a ready-to-use Client.

use std::net::SocketAddr;

use ed25519_dalek::{Signer, SigningKey};

use melin_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
use melin_protocol::codec;
use melin_protocol::error::ProtocolError;
use melin_protocol::message::{Request, ResponseKind};

use crate::{ClientError, StatsSnapshot};

/// Client connection to the trading server.
///
/// Sends requests and receives response batches synchronously (one
/// request at a time, blocking I/O). For pipelining, use
/// `BlockingFrameReader`/`BlockingFrameWriter` directly.
pub struct Client {
    reader: BlockingFrameReader<std::net::TcpStream>,
    writer: BlockingFrameWriter<std::net::TcpStream>,
    /// Pre-allocated encode buffer. 128 bytes is the upper bound,
    /// set by ChallengeResponse (4 prefix + 8 seq + 1 tag + 64 sig +
    /// 32 pubkey + slack). The auth handshake uses its own 256-byte
    /// stack buffer in `connect()` so this buffer only sees post-auth
    /// requests in practice — but keep it sized for the worst case.
    encode_buf: [u8; 128],
    /// Per-connection monotonically increasing request sequence number.
    /// Used with the server-side per-key idempotency dedup. Starts at 0
    /// and increments before each send. Heartbeats use seq=0 (exempt).
    next_seq: u64,
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
        let written = codec::encode_request(&request, 0, &mut encode_buf)?;
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
            next_seq: 0,
        })
    }

    /// Set a read timeout on the underlying TCP socket. A pending
    /// `read_frame` call will return `WouldBlock` / `TimedOut` once the
    /// deadline elapses without bytes arriving, instead of blocking
    /// forever.
    ///
    /// Intended for tests and tools that need to fail fast when a
    /// server stalls; production clients usually want the default
    /// behaviour (no timeout — a healthy connection is just idle).
    pub fn set_read_timeout(&self, dur: Option<std::time::Duration>) -> std::io::Result<()> {
        self.reader.get_ref().set_read_timeout(dur)
    }

    /// Send a request and collect all responses until BatchEnd.
    ///
    /// Returns the list of responses (excluding the BatchEnd marker itself).
    pub fn send_request(&mut self, request: &Request) -> Result<Vec<ResponseKind>, ClientError> {
        // Increment the per-connection request sequence before each send.
        // The server uses (key_hash, request_seq) for idempotency dedup.
        self.next_seq += 1;
        let written = codec::encode_request(request, self.next_seq, &mut self.encode_buf)?;
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
                ResponseKind::ServerBusy => {
                    return Err(ClientError::ServerBusy);
                }
                other => responses.push(other),
            }
        }

        Ok(responses)
    }

    /// Query and adopt the engine's current request_seq HWM for this
    /// connection's authenticated key, then return the value.
    ///
    /// Reconnecting clients should call this immediately after
    /// [`Client::connect`] so subsequent requests skip past the dedup
    /// HWM the engine accumulated under any prior connection lifetime.
    /// Without it, a fresh client process re-uses seqs starting at 1
    /// and every request gets `RejectReason::DuplicateRequest`.
    ///
    /// On return, `self.next_seq == hwm`; the next [`Client::send_request`]
    /// will increment to `hwm + 1` before sending. Safe to call against
    /// a freshly-authenticated key — the engine returns `0` and the
    /// counter stays at its initial value.
    ///
    /// `QueryRequestSeq` itself is a read-only query, so the engine
    /// bypasses dedup for it — the query goes through even though our
    /// local seq is stale.
    pub fn synchronize_request_seq(&mut self) -> Result<u64, ClientError> {
        let responses = self.send_request(&Request::QueryRequestSeq)?;
        for resp in &responses {
            if let ResponseKind::RequestSeqHwm { hwm } = resp {
                self.next_seq = *hwm;
                return Ok(*hwm);
            }
        }
        Err(ClientError::Protocol(ProtocolError::InvalidField(
            "no RequestSeqHwm in response",
        )))
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

#[cfg(test)]
mod tests {
    use super::*;
    use melin_protocol::types::{OrderId, Symbol};

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
        let mut buf = [0u8; 128];
        let written = codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();

        // Read ChallengeResponse.
        let frame = reader.read_frame().unwrap().unwrap();
        let (_seq, request) = codec::decode_request(frame).unwrap();
        let (sig_bytes, pk_bytes) = match request {
            Request::ChallengeResponse {
                signature,
                public_key,
            } => (signature, public_key),
            _ => panic!("expected ChallengeResponse"),
        };

        // Verify signature over the nonce.
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
                account: melin_protocol::types::AccountId(1),
                order_id: OrderId(42),
            })
            .unwrap();

        // No reports before BatchEnd — just an empty batch.
        assert!(responses.is_empty());
    }

    #[test]
    fn synchronize_request_seq_adopts_engine_hwm() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server: complete auth, accept QueryRequestSeq, reply with HWM.
        let server_hwm: u64 = 8423;
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);
            mock_auth_handshake(&mut reader, &mut writer);

            // Read the request and verify it's QueryRequestSeq.
            let frame = reader.read_frame().unwrap().unwrap();
            let (_seq, req) = codec::decode_request(frame).unwrap();
            assert!(matches!(req, Request::QueryRequestSeq));

            // Reply: RequestSeqHwm + BatchEnd, mirroring the live pipeline.
            let mut buf = [0u8; 64];
            let written =
                codec::encode_response(&ResponseKind::RequestSeqHwm { hwm: server_hwm }, &mut buf)
                    .unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            let written = codec::encode_response(&ResponseKind::BatchEnd, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();
        });

        let key = test_key();
        let mut client = Client::connect(addr, &key).unwrap();
        // Pre-call: next_seq advances to 1 on the next send. Post-call:
        // it sits at server_hwm, so the next send increments to hwm+1.
        let returned = client.synchronize_request_seq().unwrap();
        assert_eq!(returned, server_hwm);
        assert_eq!(client.next_seq, server_hwm);
    }

    #[test]
    fn synchronize_request_seq_handles_fresh_key() {
        // A never-before-seen key reads back hwm=0; next_seq stays at 0
        // and the next send increments normally to 1.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);
            mock_auth_handshake(&mut reader, &mut writer);
            let _ = reader.read_frame().unwrap().unwrap();
            let mut buf = [0u8; 64];
            let written =
                codec::encode_response(&ResponseKind::RequestSeqHwm { hwm: 0 }, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            let written = codec::encode_response(&ResponseKind::BatchEnd, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();
        });

        let key = test_key();
        let mut client = Client::connect(addr, &key).unwrap();
        assert_eq!(client.synchronize_request_seq().unwrap(), 0);
        assert_eq!(client.next_seq, 0);
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
            let mut buf = [0u8; 128];
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
            let mut buf = [0u8; 128];
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
            let mut buf = [0u8; 128];
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

    /// When the server pipeline is full, it sends ServerBusy.
    /// The client should surface this as `ClientError::ServerBusy`.
    #[test]
    fn server_busy_returns_backpressure_error() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);

            mock_auth_handshake(&mut reader, &mut writer);

            // Read the request.
            let _frame = reader.read_frame().unwrap().unwrap();

            // Respond with ServerBusy instead of a normal response batch.
            let mut buf = [0u8; 128];
            let written = codec::encode_response(&ResponseKind::ServerBusy, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();
        });

        let key = test_key();
        let mut client = Client::connect(addr, &key).unwrap();
        let result = client.send_request(&Request::CancelOrder {
            symbol: Symbol(1),
            account: melin_protocol::types::AccountId(1),
            order_id: OrderId(42),
        });

        assert!(
            matches!(result, Err(ClientError::ServerBusy)),
            "expected ServerBusy error, got {result:?}"
        );
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
            account: melin_protocol::types::AccountId(1),
            order_id: OrderId(42),
        });

        assert!(result.is_err());
    }
}
