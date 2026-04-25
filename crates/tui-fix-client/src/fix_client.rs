//! Blocking FIX 4.4 TCP client for the TUI.
//!
//! Manages a single FIX session: Logon, message send/receive,
//! sequence numbering, and heartbeat. Used by the TUI to connect
//! to both the oe-gateway and md-gateway.

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use melin_gateway_core::fix::parse::{self, FixMessage};
use melin_gateway_core::fix::serialize::FixMessageBuilder;
use melin_gateway_core::fix::tags;

/// A blocking FIX 4.4 session client.
pub struct FixClient {
    stream: TcpStream,
    sender_comp_id: String,
    target_comp_id: String,
    outbound_seq: u64,
    /// Accumulates partial reads until a complete FIX message arrives.
    parse_buf: Vec<u8>,
    /// Backing buffer for the most recently returned message. Owned by
    /// the client and re-used across `recv`/`try_recv` calls so that the
    /// returned `FixMessage<'_>` can borrow without leaking. Reusing the
    /// allocation also avoids a per-message malloc on the bot's hot path.
    recv_buf: Vec<u8>,
    /// Negotiated heartbeat interval (FIX tag 108). The gateway will send
    /// a TestRequest after roughly this much outbound silence and reset
    /// the connection if we don't reply, so we proactively heartbeat at
    /// this cadence in `maintain_heartbeat`.
    heart_bt_int: Duration,
    /// Wall-clock time of the last byte we sent. Used to decide whether
    /// the next `maintain_heartbeat` call needs to send a Heartbeat.
    last_sent: Instant,
}

impl FixClient {
    /// Connect to a FIX gateway and perform the Logon handshake.
    ///
    /// `addr` is resolved via DNS so hostnames like "localhost:9000" work.
    /// Blocks until the Logon response is received or the timeout expires.
    pub fn connect(
        addr: &str,
        sender_comp_id: &str,
        target_comp_id: &str,
        heartbeat_secs: u64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        use std::net::ToSocketAddrs;
        let sock = addr
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| format!("no address resolved for {addr}"))?;
        let stream = TcpStream::connect_timeout(&sock, Duration::from_secs(5))?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_nodelay(true)?;

        let mut client = Self {
            stream,
            sender_comp_id: sender_comp_id.to_string(),
            target_comp_id: target_comp_id.to_string(),
            outbound_seq: 1,
            parse_buf: Vec::with_capacity(4096),
            recv_buf: Vec::with_capacity(512),
            heart_bt_int: Duration::from_secs(heartbeat_secs),
            // Initialised at connect; the Logon send below is the first
            // real transmission and refreshes it via `send_builder`.
            last_sent: Instant::now(),
        };

        // Send Logon.
        let logon = FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .u64_tag(tags::HEART_BT_INT, heartbeat_secs);
        client.send_builder(logon)?;

        // Wait for Logon response.
        let response = client.recv()?;
        if response.msg_type() != tags::MSG_LOGON {
            return Err(format!(
                "expected Logon response, got MsgType {:?}",
                std::str::from_utf8(response.msg_type())
            )
            .into());
        }

        Ok(client)
    }

    /// Send a FIX message built from a `FixMessageBuilder`.
    pub fn send_builder(&mut self, builder: FixMessageBuilder) -> io::Result<()> {
        let msg = builder.build(
            &self.sender_comp_id,
            &self.target_comp_id,
            self.outbound_seq,
        );
        self.stream.write_all(&msg)?;
        self.stream.flush()?;
        self.outbound_seq += 1;
        self.last_sent = Instant::now();
        Ok(())
    }

    /// Read one complete FIX message from the connection.
    ///
    /// Blocks until a complete message is available or the read times out.
    /// TestRequest (35=1) messages are answered with a Heartbeat and
    /// swallowed — callers never see them.
    ///
    /// The returned `FixMessage` borrows from an internal buffer owned by
    /// the client; it is invalidated by the next call to `recv`/`try_recv`.
    pub fn recv(&mut self) -> Result<FixMessage<'_>, Box<dyn std::error::Error>> {
        let mut tmp = [0u8; 4096];
        loop {
            // Drain any complete message already in the buffer first.
            // Loops back if it's a TestRequest we auto-answer.
            if let Some(raw) = parse::try_extract_message(&mut self.parse_buf) {
                self.recv_buf = raw;
                if self.handle_session_message()? {
                    continue;
                }
                return FixMessage::parse(&self.recv_buf).map_err(Into::into);
            }

            let n = self.stream.read(&mut tmp)?;
            if n == 0 {
                return Err("connection closed".into());
            }
            self.parse_buf.extend_from_slice(&tmp[..n]);
        }
    }

    /// Set the read timeout for subsequent `recv()` calls.
    /// Call with a short duration before `try_recv` loops, or a longer
    /// duration before blocking `recv` calls.
    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(timeout)
    }

    /// Try to read a FIX message without blocking.
    ///
    /// Requires a short read timeout to have been set via `set_read_timeout`.
    /// Returns `Ok(None)` if no complete message is available yet.
    /// TestRequest (35=1) messages are answered with a Heartbeat and
    /// swallowed — callers never see them.
    ///
    /// The returned `FixMessage` borrows from an internal buffer owned by
    /// the client; it is invalidated by the next call to `recv`/`try_recv`.
    pub fn try_recv(&mut self) -> Result<Option<FixMessage<'_>>, Box<dyn std::error::Error>> {
        let mut tmp = [0u8; 4096];
        match self.stream.read(&mut tmp) {
            Ok(0) => return Err("connection closed".into()),
            Ok(n) => self.parse_buf.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {
            }
            Err(e) => return Err(e.into()),
        }

        // Drain TestRequests inline. Multiple complete messages may have
        // arrived in one read, so loop until we find one to return or the
        // buffer is empty.
        loop {
            let Some(raw) = parse::try_extract_message(&mut self.parse_buf) else {
                return Ok(None);
            };
            self.recv_buf = raw;
            if self.handle_session_message()? {
                continue;
            }
            return Ok(Some(FixMessage::parse(&self.recv_buf)?));
        }
    }

    /// Inspect the message currently in `recv_buf`. If it's a session-level
    /// administrative message we should answer transparently (currently
    /// only TestRequest), reply and return `Ok(true)` so callers know to
    /// skip it. Otherwise return `Ok(false)`.
    fn handle_session_message(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        // Parse against `recv_buf`, extract any owned data we need, drop
        // the borrow before sending so `&mut self` is available again.
        let test_req_id = {
            let msg = FixMessage::parse(&self.recv_buf)?;
            if msg.msg_type() != tags::MSG_TEST_REQUEST {
                return Ok(false);
            }
            msg.get_str(tags::TEST_REQ_ID).unwrap_or("").to_owned()
        };
        let mut hb = FixMessageBuilder::new(tags::MSG_HEARTBEAT);
        if !test_req_id.is_empty() {
            hb = hb.str_tag(tags::TEST_REQ_ID, &test_req_id);
        }
        self.send_builder(hb)?;
        Ok(true)
    }

    /// Send a Heartbeat (35=0) if our outbound has been silent for at
    /// least 80% of the negotiated interval, so the gateway doesn't
    /// TestRequest us into a reset. Cheap to call from a tight polling
    /// loop — the idle check is just an `Instant::elapsed` compare.
    ///
    /// The 80% skew matters because the gateway's heartbeat scan runs
    /// once a second and uses a strict `since_recv > HeartBtInt` check.
    /// Firing exactly at the interval leaves a ~50–100 ms window where
    /// the scan can race ahead of our heartbeat and emit a spurious
    /// TestRequest. At 80%, we're comfortably ahead of the threshold.
    pub fn maintain_heartbeat(&mut self) -> io::Result<()> {
        if self.last_sent.elapsed() >= self.heartbeat_skew() {
            self.send_builder(FixMessageBuilder::new(tags::MSG_HEARTBEAT))?;
        }
        Ok(())
    }

    /// 80% of `heart_bt_int`. Fractional-multiply on `Duration` is
    /// nightly-only, so do the integer arithmetic in nanoseconds.
    fn heartbeat_skew(&self) -> Duration {
        let nanos = self.heart_bt_int.as_nanos() as u64;
        Duration::from_nanos(nanos.saturating_mul(4) / 5)
    }

    /// Send a Logout and close the connection.
    pub fn logout(&mut self) -> io::Result<()> {
        let logout =
            FixMessageBuilder::new(tags::MSG_LOGOUT).str_tag(tags::TEXT, "client shutdown");
        self.send_builder(logout)?;
        // Best-effort read the Logout response.
        let _ = self.stream.set_read_timeout(Some(Duration::from_secs(1)));
        let _ = self.recv();
        Ok(())
    }

    pub fn sender_comp_id(&self) -> &str {
        &self.sender_comp_id
    }

    pub fn target_comp_id(&self) -> &str {
        &self.target_comp_id
    }

    pub fn next_outbound_seq(&self) -> u64 {
        self.outbound_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn fix_client_type_is_constructible() {
        // Smoke test — verify the type compiles and the builder chain works.
        let builder = FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .u64_tag(tags::HEART_BT_INT, 30);
        let msg = builder.build("SENDER", "TARGET", 1);
        let parsed = FixMessage::parse(&msg).unwrap();
        assert_eq!(parsed.msg_type(), tags::MSG_LOGON);
    }

    /// Read until `try_extract_message` succeeds or `deadline` passes.
    /// Test helper — production reads loop in `recv`/`try_recv`.
    fn read_one_fix_message(stream: &mut TcpStream, deadline: Instant) -> Vec<u8> {
        let mut acc: Vec<u8> = Vec::new();
        let mut tmp = [0u8; 4096];
        while Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            stream
                .set_read_timeout(Some(remaining.max(Duration::from_millis(10))))
                .unwrap();
            match stream.read(&mut tmp) {
                Ok(0) => panic!("server peer closed before delivering message"),
                Ok(n) => acc.extend_from_slice(&tmp[..n]),
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock
                        || e.kind() == io::ErrorKind::TimedOut =>
                {
                    continue;
                }
                Err(e) => panic!("server read error: {e}"),
            }
            if let Some(raw) = parse::try_extract_message(&mut acc) {
                return raw;
            }
        }
        panic!("timed out waiting for FIX message");
    }

    /// Bind a localhost listener and spawn a thread that plays the FIX
    /// peer for one connection: reads the client's Logon, echoes a Logon
    /// back, sends a TestRequest (TestReqID=`PING1`), and returns the
    /// next message it receives. The returned join handle yields the raw
    /// bytes of that message for assertions.
    fn spawn_test_request_peer() -> (std::net::SocketAddr, thread::JoinHandle<Vec<u8>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Drain Logon from the client.
            let _logon = read_one_fix_message(&mut stream, Instant::now() + Duration::from_secs(2));
            // Echo a Logon back so `connect` returns.
            let logon_resp = FixMessageBuilder::new(tags::MSG_LOGON)
                .str_tag(tags::ENCRYPT_METHOD, "0")
                .u64_tag(tags::HEART_BT_INT, 30)
                .build("SERVER", "CLIENT", 1);
            stream.write_all(&logon_resp).unwrap();
            // Send a TestRequest the client must auto-answer.
            let test_req = FixMessageBuilder::new(tags::MSG_TEST_REQUEST)
                .str_tag(tags::TEST_REQ_ID, "PING1")
                .build("SERVER", "CLIENT", 2);
            stream.write_all(&test_req).unwrap();
            // Capture whatever the client sends next — we expect a Heartbeat.
            read_one_fix_message(&mut stream, Instant::now() + Duration::from_secs(3))
        });
        (addr, handle)
    }

    #[test]
    fn test_request_is_auto_answered_with_heartbeat() {
        let (addr, server) = spawn_test_request_peer();
        let mut client = FixClient::connect(&addr.to_string(), "CLIENT", "SERVER", 30).unwrap();
        // Short read timeout so try_recv doesn't block the test for 5 s
        // when the buffer is empty after handling the TestRequest.
        client
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        // Drain incoming. The TestRequest should arrive and be swallowed,
        // with a Heartbeat sent back. Bail on any error (including the
        // expected "connection closed" once the server thread has read
        // our Heartbeat and dropped its stream).
        for _ in 0..6 {
            match client.try_recv() {
                Ok(Some(_)) => panic!("TestRequest should have been swallowed"),
                Ok(None) => {}
                Err(_) => break,
            }
        }
        let raw = server.join().unwrap();
        let reply = FixMessage::parse(&raw).unwrap();
        assert_eq!(reply.msg_type(), tags::MSG_HEARTBEAT);
        assert_eq!(reply.get_str(tags::TEST_REQ_ID), Some("PING1"));
    }

    #[test]
    fn maintain_heartbeat_does_not_send_when_recently_sent() {
        // Bind a peer that just accepts + completes Logon, then waits.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _logon = read_one_fix_message(&mut stream, Instant::now() + Duration::from_secs(2));
            let logon_resp = FixMessageBuilder::new(tags::MSG_LOGON)
                .str_tag(tags::ENCRYPT_METHOD, "0")
                .u64_tag(tags::HEART_BT_INT, 30)
                .build("SERVER", "CLIENT", 1);
            stream.write_all(&logon_resp).unwrap();
            // Hold the connection open for a moment so the client can
            // call maintain_heartbeat without errors. The thread exits
            // and stream is dropped, which the client tolerates.
            thread::sleep(Duration::from_millis(200));
        });
        let mut client = FixClient::connect(&addr.to_string(), "CLIENT", "SERVER", 30).unwrap();
        // Right after Logon the outbound is fresh — heartbeat must NOT fire.
        let seq_before = client.next_outbound_seq();
        client.maintain_heartbeat().unwrap();
        assert_eq!(
            client.next_outbound_seq(),
            seq_before,
            "maintain_heartbeat should not have sent anything; outbound_seq advanced"
        );
        let _ = server.join();
    }
}
