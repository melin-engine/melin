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
    ///
    /// # Cluster awareness
    ///
    /// Dialing a replica is handled transparently: a `Redirect` answer
    /// is followed to the named primary (bounded hops), and a "busy,
    /// retry" answer (cluster mid-election) is retried with backoff
    /// until the election settles — so, like the pre-redirect behaviour
    /// of parking in a promoting replica's accept backlog, this call
    /// waits out a failover rather than failing.
    pub fn connect(addr: SocketAddr, key: &SigningKey) -> Result<Self, ClientError> {
        Self::connect_following_redirects(addr, key, None)
    }

    /// Like [`Client::connect`], but the **whole** connect — TCP dial,
    /// every handshake read (including the auto-sync `QueryRequestSeq`
    /// response), redirect hops, and busy retries — runs under one
    /// overall `timeout`: each operation is armed with the remaining
    /// budget, so the call returns (client or error) within roughly
    /// `timeout` regardless of how many hops or retries it took. A
    /// handshake that stalls returns an `io::ErrorKind::WouldBlock` /
    /// `TimedOut` error wrapped in [`ClientError::Io`] instead of
    /// hanging.
    ///
    /// Granularity: the budget is re-armed before every frame read, so
    /// the bound can overshoot by at most one in-progress frame — a
    /// pathological peer dribbling bytes *within* a single frame can
    /// stretch that final frame. The nodes this dials are the venue's
    /// own authenticated infrastructure, so the per-frame granularity
    /// is deliberate; it is not a defense against a hostile server.
    ///
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

    /// Follow `Redirect` responses from non-primary nodes (bounded so a
    /// confused cluster — e.g. two replicas pointing at each other
    /// mid-election — surfaces [`ClientError::Redirected`] instead of
    /// looping) and retry `ServerBusy` answers with backoff (a replica
    /// that knows no primary yet; the election settles within seconds).
    /// One `deadline` covers everything; without one, busy retries
    /// continue indefinitely — deliberately matching [`Client::connect`]'s
    /// wait-forever contract from before redirects existed, when the
    /// same client would have parked in the accept backlog instead.
    fn connect_following_redirects(
        addr: SocketAddr,
        key: &SigningKey,
        timeout: Option<std::time::Duration>,
    ) -> Result<Self, ClientError> {
        /// Redirects *followed* per connect. One hop covers the real
        /// case (a replica naming the promoted primary); the slack
        /// absorbs an election settling mid-connect.
        const MAX_REDIRECT_HOPS: usize = 3;
        /// Pause between busy retries — long enough not to hammer a
        /// mid-election replica, short against the seconds-scale
        /// election timeout.
        const BUSY_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);

        let deadline = timeout.map(|t| std::time::Instant::now() + t);
        let mut target = addr;
        let mut hops = 0;
        loop {
            match Self::connect_inner(target, key, deadline) {
                Err(ClientError::Redirected(next)) => {
                    // Check before counting so exactly
                    // MAX_REDIRECT_HOPS redirects are *followed* —
                    // the surplus one is returned unfollowed.
                    if hops >= MAX_REDIRECT_HOPS {
                        return Err(ClientError::Redirected(next));
                    }
                    hops += 1;
                    target = next;
                }
                Err(ClientError::ServerBusy) => {
                    if let Some(d) = deadline
                        && std::time::Instant::now() + BUSY_RETRY_BACKOFF >= d
                    {
                        return Err(ClientError::ServerBusy);
                    }
                    std::thread::sleep(BUSY_RETRY_BACKOFF);
                }
                other => return other,
            }
        }
    }

    /// Remaining budget until `deadline`, as a `Duration` suitable for
    /// socket-timeout arming. `Ok(None)` when no deadline is set;
    /// otherwise delegates to the shared
    /// [`melin_wire_protocol::blocking::remaining_budget`] — the single
    /// source of truth for the 1 ms floor that keeps a sub-millisecond
    /// remainder from truncating to the "no timeout" zero timeval (the
    /// same check the server-side redirect acceptor uses).
    fn remaining(
        deadline: Option<std::time::Instant>,
    ) -> Result<Option<std::time::Duration>, ClientError> {
        match deadline {
            None => Ok(None),
            Some(d) => melin_wire_protocol::blocking::remaining_budget(d)
                .map(Some)
                .map_err(ClientError::Io),
        }
    }

    /// Re-arm the socket read timeout with the budget remaining until
    /// `deadline` (no-op without one). Called before each handshake
    /// step so elapsed time shrinks later steps' waits instead of each
    /// step enjoying the full budget anew.
    fn arm_read_deadline(
        stream: &std::net::TcpStream,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), ClientError> {
        if let Some(rem) = Self::remaining(deadline)? {
            stream.set_read_timeout(Some(rem))?;
        }
        Ok(())
    }

    /// Shared body for [`Client::connect`] and
    /// [`Client::connect_with_timeout`]. When `deadline` is `Some`, the
    /// TCP connect and every subsequent read run under the *remaining*
    /// budget; the timeout is cleared before the constructed client is
    /// returned so the caller sees the same defaults either way.
    fn connect_inner(
        addr: SocketAddr,
        key: &SigningKey,
        deadline: Option<std::time::Instant>,
    ) -> Result<Self, ClientError> {
        let stream = match Self::remaining(deadline)? {
            Some(t) => std::net::TcpStream::connect_timeout(&addr, t)?,
            None => std::net::TcpStream::connect(addr)?,
        };
        stream.set_nodelay(true)?;
        Self::arm_read_deadline(&stream, deadline)?;
        let mut reader = BlockingFrameReader::new(stream.try_clone()?);
        let mut writer = BlockingFrameWriter::new(stream);

        // Step 1: Receive Challenge from server.
        let frame = reader.read_frame()?.ok_or(ClientError::Disconnected)?;
        let response = codec::decode_response(frame)?;
        let nonce = match response {
            ResponseKind::Challenge { nonce } => nonce,
            // A replica shedding load answers busy before the
            // challenge — re-enters the caller's backoff loop.
            ResponseKind::ServerBusy => return Err(ClientError::ServerBusy),
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
        Self::arm_read_deadline(reader.get_ref(), deadline)?;
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
        client.synchronize_request_seq_with_deadline(deadline)?;

        // Restore the default (untimed) read behaviour before handing
        // the client back, so post-connect calls match `connect`'s
        // contract regardless of which entry point was used.
        if deadline.is_some() {
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
        self.send_request_with_deadline(request, None)
    }

    /// [`Client::send_request`] under the connect deadline: the read
    /// timeout is re-armed with the *remaining* budget before every
    /// frame, so a server streaming heartbeats (consumed silently
    /// below) cannot hold a bounded connect open past its deadline.
    /// A `None` deadline makes the arming a no-op.
    fn send_request_with_deadline(
        &mut self,
        request: &Request,
        deadline: Option<std::time::Instant>,
    ) -> Result<Vec<ResponseKind>, ClientError> {
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
            Self::arm_read_deadline(self.reader.get_ref(), deadline)?;
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
        self.synchronize_request_seq_with_deadline(None)
    }

    /// Deadline-carrying body of [`Client::synchronize_request_seq`],
    /// used by the bounded connect path.
    fn synchronize_request_seq_with_deadline(
        &mut self,
        deadline: Option<std::time::Instant>,
    ) -> Result<u64, ClientError> {
        let responses = self.send_request_with_deadline(&Request::QueryRequestSeq, deadline)?;
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
        // Challenge/verify, then ServerReady.
        mock_challenge_verify(reader, writer);
        let mut buf = [0u8; 128];
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

    /// The challenge/verify half of the server-side handshake, shared
    /// by every mock server so the (evolving) handshake exists once in
    /// the test suite: send a Challenge, read the ChallengeResponse,
    /// verify the signature over the nonce.
    fn mock_challenge_verify(
        reader: &mut BlockingFrameReader<std::net::TcpStream>,
        writer: &mut BlockingFrameWriter<std::net::TcpStream>,
    ) {
        use ed25519_dalek::{Verifier, VerifyingKey};

        let nonce = [0xBB; 32];
        let mut buf = [0u8; 128];
        let written = codec::encode_response(&ResponseKind::Challenge { nonce }, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();

        let frame = reader.read_frame().unwrap().unwrap();
        let (_seq, request) = codec::decode_request(frame).unwrap();
        let Request::ChallengeResponse {
            signature,
            public_key,
        } = request
        else {
            panic!("expected ChallengeResponse");
        };
        let vk = VerifyingKey::from_bytes(&public_key).unwrap();
        vk.verify(&nonce, &ed25519_dalek::Signature::from_bytes(&signature))
            .unwrap();
    }

    /// Serve one connection: run the challenge/verify steps, then
    /// answer with `response` instead of `ServerReady` (a replica
    /// answering `Redirect` or `ServerBusy`).
    fn mock_auth_then(stream: std::net::TcpStream, response: &ResponseKind) {
        let mut reader = BlockingFrameReader::new(stream.try_clone().unwrap());
        let mut writer = BlockingFrameWriter::new(stream);

        mock_challenge_verify(&mut reader, &mut writer);

        let mut buf = [0u8; 128];
        let written = codec::encode_response(response, &mut buf).unwrap();
        writer.write_frame(&buf[4..written]).unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn redirect_is_followed_to_the_primary() {
        // "Replica" answers with a redirect naming the "primary";
        // Client::connect must land on the primary transparently.
        let primary = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let primary_addr = primary.local_addr().unwrap();
        let replica = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let replica_addr = replica.local_addr().unwrap();

        std::thread::spawn(move || {
            let (stream, _) = replica.accept().unwrap();
            mock_auth_then(stream, &ResponseKind::Redirect { addr: primary_addr });
        });
        std::thread::spawn(move || mock_batch_end_server(primary));

        let key = test_key();
        let mut client = Client::connect(replica_addr, &key).expect("redirect must be followed");
        // The connection works end-to-end against the primary: the
        // batch-end mock answers one request with an empty batch.
        let responses = client
            .send_request(&Request::QueryStats)
            .expect("request on redirected connection");
        assert!(responses.is_empty(), "batch-end mock sends no reports");
    }

    #[test]
    fn server_busy_is_retried_until_the_election_settles() {
        // First connection: busy (mid-election). Second: full handshake.
        // connect() must absorb the busy answer and succeed.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            mock_auth_then(stream, &ResponseKind::ServerBusy);
            mock_batch_end_server(listener);
        });

        let key = test_key();
        let started = std::time::Instant::now();
        Client::connect(addr, &key).expect("busy answer must be retried");
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(200),
            "a busy retry must back off, not hammer"
        );
    }

    #[test]
    fn overall_deadline_bounds_busy_retries() {
        // A cluster that never settles: every connection gets busy.
        // connect_with_timeout must give up within (roughly) its budget
        // instead of retrying forever.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            loop {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                mock_auth_then(stream, &ResponseKind::ServerBusy);
            }
        });

        let key = test_key();
        let started = std::time::Instant::now();
        let err =
            match Client::connect_with_timeout(addr, &key, std::time::Duration::from_millis(600)) {
                Ok(_) => panic!("must give up at the deadline"),
                Err(e) => e,
            };
        assert!(
            matches!(err, ClientError::ServerBusy | ClientError::Io(_)),
            "expected busy/timeout, got {err:?}"
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(3),
            "deadline must bound the whole connect (took {elapsed:?})"
        );
    }

    #[test]
    fn redirect_loop_is_bounded() {
        // A confused node redirecting to itself: the client must stop
        // after the hop bound and surface Redirected, not loop.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            loop {
                let Ok((stream, _)) = listener.accept() else {
                    return;
                };
                mock_auth_then(stream, &ResponseKind::Redirect { addr });
            }
        });

        let key = test_key();
        let err = match Client::connect(addr, &key) {
            Ok(_) => panic!("must not follow redirects forever"),
            Err(e) => e,
        };
        assert!(
            matches!(err, ClientError::Redirected(a) if a == addr),
            "expected Redirected, got {err:?}"
        );
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
