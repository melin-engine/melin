//! TCP-backed client. Default transport. Blocking I/O over a single
//! TCP socket; connect performs the four-message Ed25519 challenge-
//! response handshake and returns a ready-to-use Client.

use std::net::SocketAddr;

use ed25519_dalek::{Signer, SigningKey};

use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};
use melin_wire_protocol::blocking::{BlockingFrameReader, BlockingFrameWriter};
use melin_wire_protocol::error::ProtocolError;

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
    /// 5. Issues a `QueryRequestSeq` and adopts the engine's per-key
    ///    request_seq HWM (see [`Client::synchronize_request_seq`]).
    ///
    /// Step 5 closes a footgun for reconnecting clients: a fresh
    /// `Client` starts at `next_seq = 0`, but the engine remembers the
    /// HWM from any prior session under the same key. Without the
    /// auto-sync, the first ~N post-reconnect requests come back as
    /// `RejectReason::DuplicateRequest` until the local counter catches
    /// up. The cost is one extra round-trip on connect — acceptable, as
    /// connect is not on the hot path.
    ///
    /// # Blocking semantics
    ///
    /// Steps 1, 3 and 5 each wait for a server response and will block
    /// indefinitely if the server never replies. Step 5 in particular
    /// goes through the engine pipeline, so its response is gated on
    /// the configured durability policy: connecting to a primary whose
    /// policy is unsatisfiable (e.g. `primary-needs-replica` with no
    /// replica attached) will hang here forever. Callers that need a
    /// bounded wait should use [`Client::connect_with_timeout`].
    pub fn connect(addr: SocketAddr, key: &SigningKey) -> Result<Self, ClientError> {
        Self::connect_following_redirects(addr, key, None)
    }

    /// Like [`Client::connect`], but bounds every read on the
    /// connection (handshake frames *and* the auto-sync
    /// `QueryRequestSeq` response) by `timeout`. A handshake that
    /// stalls — e.g. against a halted primary — returns an
    /// `io::ErrorKind::WouldBlock` / `TimedOut` error wrapped in
    /// [`ClientError::Io`] instead of hanging.
    ///
    /// The TCP connect itself is also subject to the same `timeout`.
    /// The read timeout on the returned socket is cleared before
    /// return, so post-connect calls (`send_request`, etc.) behave
    /// exactly like the untimed [`Client::connect`] path. Callers that
    /// also want a steady-state read timeout should call
    /// [`Client::set_read_timeout`] after this method returns.
    pub fn connect_with_timeout(
        addr: SocketAddr,
        key: &SigningKey,
        timeout: std::time::Duration,
    ) -> Result<Self, ClientError> {
        Self::connect_following_redirects(addr, key, Some(timeout))
    }

    /// Follow `Redirect` responses from non-primary nodes, bounded so
    /// a confused cluster (e.g. two replicas pointing at each other
    /// mid-election) surfaces [`ClientError::Redirected`] instead of
    /// looping. Two hops cover the real case — a replica naming the
    /// newly promoted primary — with one spare.
    fn connect_following_redirects(
        addr: SocketAddr,
        key: &SigningKey,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self, ClientError> {
        const MAX_REDIRECT_HOPS: usize = 3;
        let mut target = addr;
        let mut last = None;
        for _ in 0..MAX_REDIRECT_HOPS {
            match Self::connect_inner(target, key, timeout) {
                Err(ClientError::Redirected(next)) => {
                    last = Some(next);
                    target = next;
                }
                other => return other,
            }
        }
        Err(ClientError::Redirected(last.unwrap_or(target)))
    }

    /// Shared body for [`Client::connect`] and
    /// [`Client::connect_with_timeout`]. When `timeout` is `Some`, both
    /// the TCP connect and every subsequent read run under that bound;
    /// the timeout is cleared before the constructed client is
    /// returned so the caller sees the same defaults either way.
    fn connect_inner(
        addr: SocketAddr,
        key: &SigningKey,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self, ClientError> {
        let stream = match timeout {
            Some(t) => std::net::TcpStream::connect_timeout(&addr, t)?,
            None => std::net::TcpStream::connect(addr)?,
        };
        stream.set_nodelay(true)?;
        if let Some(t) = timeout {
            stream.set_read_timeout(Some(t))?;
        }
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
            // A replica naming the serving primary — the caller
            // (`connect_following_redirects`) reconnects there.
            ResponseKind::Redirect { addr } => {
                return Err(ClientError::Redirected(addr));
            }
            // A replica that knows no primary yet (mid-election).
            ResponseKind::ServerBusy => {
                return Err(ClientError::ServerBusy);
            }
            _ => {
                return Err(ClientError::Protocol(ProtocolError::InvalidField(
                    "expected ServerReady or AuthFailed",
                )));
            }
        }

        let mut client = Self {
            reader,
            writer,
            encode_buf: [0u8; 128],
            next_seq: 0,
        };

        // Step 5: Adopt the engine's per-key request_seq HWM so the next
        // request lands at HWM + 1 instead of 1 (which would dedup if a
        // prior session under this key already advanced the counter).
        client.synchronize_request_seq()?;

        // Restore the default (untimed) read behaviour before handing
        // the client back, so post-connect calls match `connect`'s
        // contract regardless of which entry point was used.
        if timeout.is_some() {
            client.set_read_timeout(None)?;
        }

        Ok(client)
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
    /// [`Client::connect`] already invokes this automatically — exposed
    /// publicly so callers that have a reason to suspect their local
    /// counter has drifted from the engine's (manual reconnect flows,
    /// scripted recovery tools, long-lived `Client`s carried across
    /// state changes the transport may have observed) can re-sync
    /// without tearing the connection down.
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
    /// any valid signature from the test key, then service the auto-sync
    /// `QueryRequestSeq` that `Client::connect` issues immediately after
    /// auth. `sync_hwm` is the HWM the server reports; pass `0` to mimic
    /// a never-before-seen key.
    fn mock_auth_handshake(
        reader: &mut BlockingFrameReader<std::net::TcpStream>,
        writer: &mut BlockingFrameWriter<std::net::TcpStream>,
        sync_hwm: u64,
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

        // Service the auto-sync QueryRequestSeq: read the query, reply
        // with RequestSeqHwm + BatchEnd.
        let frame = reader.read_frame().unwrap().unwrap();
        let (_seq, req) = codec::decode_request(frame).unwrap();
        assert!(
            matches!(req, Request::QueryRequestSeq),
            "expected auto-sync QueryRequestSeq, got {req:?}"
        );
        let written =
            codec::encode_response(&ResponseKind::RequestSeqHwm { hwm: sync_hwm }, &mut buf)
                .unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        let written = codec::encode_response(&ResponseKind::BatchEnd, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();
    }

    /// Mock server that authenticates, reads one request, responds with BatchEnd.
    fn mock_batch_end_server(listener: std::net::TcpListener) {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
        let mut writer = BlockingFrameWriter::new(stream);

        mock_auth_handshake(&mut reader, &mut writer, 0);

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
    fn connect_auto_syncs_engine_request_seq_hwm() {
        // Reconnecting against an engine that has already advanced this
        // key's HWM: the auto-sync in `connect` must pull the HWM so the
        // first post-connect request lands at HWM + 1 and skips dedup.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server_hwm: u64 = 8423;
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);
            mock_auth_handshake(&mut reader, &mut writer, server_hwm);
        });

        let key = test_key();
        let client = Client::connect(addr, &key).unwrap();
        assert_eq!(client.next_seq, server_hwm);
    }

    #[test]
    fn connect_with_fresh_key_starts_at_zero() {
        // A never-before-seen key: engine replies hwm=0, so next_seq
        // stays at 0 and the first send increments normally to 1.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);
            mock_auth_handshake(&mut reader, &mut writer, 0);
        });

        let key = test_key();
        let client = Client::connect(addr, &key).unwrap();
        assert_eq!(client.next_seq, 0);
    }

    #[test]
    fn synchronize_request_seq_can_be_called_again_mid_session() {
        // The public `synchronize_request_seq` still works after the
        // implicit connect-time sync — exercised by callers that need
        // to re-sync mid-session.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let later_hwm: u64 = 12_345;
        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
            let mut writer = BlockingFrameWriter::new(stream);
            // Auto-sync at connect reports hwm=0; the explicit re-sync
            // below reports a higher HWM the test should adopt.
            mock_auth_handshake(&mut reader, &mut writer, 0);

            let frame = reader.read_frame().unwrap().unwrap();
            let (_seq, req) = codec::decode_request(frame).unwrap();
            assert!(matches!(req, Request::QueryRequestSeq));

            let mut buf = [0u8; 64];
            let written =
                codec::encode_response(&ResponseKind::RequestSeqHwm { hwm: later_hwm }, &mut buf)
                    .unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            let written = codec::encode_response(&ResponseKind::BatchEnd, &mut buf).unwrap();
            writer.write_frame(&buf[4..written]).unwrap();
            writer.flush().unwrap();
        });

        let key = test_key();
        let mut client = Client::connect(addr, &key).unwrap();
        assert_eq!(client.next_seq, 0);
        let returned = client.synchronize_request_seq().unwrap();
        assert_eq!(returned, later_hwm);
        assert_eq!(client.next_seq, later_hwm);
    }

    #[test]
    fn connect_with_timeout_returns_error_when_server_never_responds() {
        // Server accepts the TCP connection but never sends the
        // Challenge — `connect` would block forever on the read in
        // step 1. `connect_with_timeout` must surface a timeout error
        // instead of hanging.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Hold the accepted stream alive for the duration of the test
        // so the client doesn't observe a clean EOF — we want it to
        // genuinely time out reading, not race against close().
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            let _ = rx.recv();
        });

        let key = test_key();
        let started = std::time::Instant::now();
        let result =
            Client::connect_with_timeout(addr, &key, std::time::Duration::from_millis(150));
        let elapsed = started.elapsed();
        let _ = tx.send(());

        assert!(
            matches!(result.as_ref(), Err(ClientError::Io(_))),
            "expected io timeout error, got Err = {:?}",
            result.err()
        );
        // Sanity: returned promptly rather than waiting on the default
        // socket timeout (minutes on Linux).
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "connect_with_timeout took too long ({elapsed:?}) — did the bound apply?"
        );
    }

    #[test]
    fn connect_with_timeout_clears_socket_timeout_on_success() {
        // After `connect_with_timeout` returns successfully, the read
        // timeout the helper installed must be cleared so post-connect
        // calls behave like the untimed `connect` path. Otherwise an
        // idle `send_request` against a slow but healthy server would
        // start failing once the bound elapsed.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || mock_batch_end_server(listener));

        let key = test_key();
        let client = Client::connect_with_timeout(addr, &key, std::time::Duration::from_secs(2))
            .expect("connect_with_timeout");
        let socket_timeout = client.reader.get_ref().read_timeout().unwrap();
        assert!(
            socket_timeout.is_none(),
            "expected read timeout to be cleared after successful connect, got {socket_timeout:?}"
        );
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

            mock_auth_handshake(&mut reader, &mut writer, 0);

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
            mock_auth_handshake(&mut reader, &mut writer, 0);
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
