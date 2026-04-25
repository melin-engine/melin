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
                // Peek the msg-type without a full parse so the parse on
                // the non-TestRequest path is the *only* parse for this
                // message — handing the FixMessage straight to the caller.
                if peek_msg_type(&self.recv_buf) == tags::MSG_TEST_REQUEST {
                    let id = FixMessage::parse(&self.recv_buf)?
                        .get_str(tags::TEST_REQ_ID)
                        .unwrap_or("")
                        .to_owned();
                    self.reply_to_test_request(&id)?;
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
            // See `recv` — peek msg_type without parsing so the parse on
            // the non-TestRequest path is the only parse for this message.
            if peek_msg_type(&self.recv_buf) == tags::MSG_TEST_REQUEST {
                let id = FixMessage::parse(&self.recv_buf)?
                    .get_str(tags::TEST_REQ_ID)
                    .unwrap_or("")
                    .to_owned();
                self.reply_to_test_request(&id)?;
                continue;
            }
            return Ok(Some(FixMessage::parse(&self.recv_buf)?));
        }
    }

    /// Send a Heartbeat (35=0) echoing the given TestReqID. FIX 4.4 §B.4
    /// requires the response heartbeat to carry tag 112; an empty input
    /// means the inbound TestRequest was malformed (no 112 set), in
    /// which case we send a bare Heartbeat — the spec is silent on this
    /// edge but most peers treat it as harmless keep-alive.
    fn reply_to_test_request(&mut self, test_req_id: &str) -> io::Result<()> {
        let mut hb = FixMessageBuilder::new(tags::MSG_HEARTBEAT);
        if !test_req_id.is_empty() {
            hb = hb.str_tag(tags::TEST_REQ_ID, test_req_id);
        }
        self.send_builder(hb)
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

/// Cheap msg-type extractor that scans for the `\x0135=…\x01` field
/// without doing a full FIX parse. Used by the recv path to decide
/// whether to swallow + auto-answer a TestRequest, so that the full
/// `FixMessage::parse` only runs once per message — on the path that
/// hands the parsed result to the caller. Returns an empty slice on
/// any malformed input; callers fall back to the full parse error.
///
/// Safety against value collisions: FIX field values cannot contain
/// SOH (0x01) by spec — that's the field separator — so the leading
/// SOH guarantees the match is at a field boundary, not inside a
/// value. The first FIX field (8=BeginString) has no leading SOH, so
/// matching `\x0135=` always lands on the MsgType field.
fn peek_msg_type(buf: &[u8]) -> &[u8] {
    let needle = b"\x0135=";
    let Some(start) = buf.windows(needle.len()).position(|w| w == needle) else {
        return &[];
    };
    let val_start = start + needle.len();
    let Some(soh_off) = buf[val_start..].iter().position(|&b| b == 0x01) else {
        return &[];
    };
    &buf[val_start..val_start + soh_off]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    #[test]
    fn peek_msg_type_finds_logon() {
        let raw = FixMessageBuilder::new(tags::MSG_LOGON)
            .str_tag(tags::ENCRYPT_METHOD, "0")
            .u64_tag(tags::HEART_BT_INT, 30)
            .build("S", "T", 1);
        assert_eq!(peek_msg_type(&raw), tags::MSG_LOGON);
    }

    #[test]
    fn peek_msg_type_finds_test_request() {
        let raw = FixMessageBuilder::new(tags::MSG_TEST_REQUEST)
            .str_tag(tags::TEST_REQ_ID, "PING")
            .build("S", "T", 1);
        assert_eq!(peek_msg_type(&raw), tags::MSG_TEST_REQUEST);
    }

    #[test]
    fn peek_msg_type_returns_empty_on_garbage() {
        assert!(peek_msg_type(b"not a fix message at all").is_empty());
        assert!(peek_msg_type(b"").is_empty());
        // Truncated mid-MsgType: leading SOH 35= present but no closing SOH.
        assert!(peek_msg_type(b"\x0135=").is_empty());
    }

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
    fn maintain_heartbeat_sends_when_idle_past_skew() {
        // 1-second HeartBtInt → 800 ms skew. Sleep 1 s (well past),
        // call maintain_heartbeat once, expect one Heartbeat on the wire.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            // Drain client's Logon.
            let _ = read_one_fix_message(&mut stream, Instant::now() + Duration::from_secs(2));
            // Echo a Logon back so connect() returns.
            let logon_resp = FixMessageBuilder::new(tags::MSG_LOGON)
                .str_tag(tags::ENCRYPT_METHOD, "0")
                .u64_tag(tags::HEART_BT_INT, 1)
                .build("SERVER", "CLIENT", 1);
            stream.write_all(&logon_resp).unwrap();
            // Whatever the client sends next is what we want to inspect.
            read_one_fix_message(&mut stream, Instant::now() + Duration::from_secs(3))
        });
        let mut client = FixClient::connect(&addr.to_string(), "CLIENT", "SERVER", 1).unwrap();
        // Sleep past the 80% skew so the next maintain_heartbeat fires.
        thread::sleep(Duration::from_millis(1_000));
        let seq_before = client.next_outbound_seq();
        client.maintain_heartbeat().unwrap();
        assert_eq!(
            client.next_outbound_seq(),
            seq_before + 1,
            "outbound_seq should have advanced, indicating a Heartbeat was sent"
        );
        let raw = server.join().unwrap();
        let msg = FixMessage::parse(&raw).unwrap();
        assert_eq!(msg.msg_type(), tags::MSG_HEARTBEAT);
        // Proactive heartbeat carries no TestReqID (only TestRequest replies do).
        assert_eq!(msg.get_str(tags::TEST_REQ_ID), None);
    }

    #[test]
    fn fresh_connect_after_dropped_session_succeeds() {
        // Reconnect coverage at the FixClient level: the wrapper loop
        // builds a new FixClient on every reconnect, so what matters is
        // that a fresh connect after a server-side drop completes the
        // Logon handshake cleanly. Drives a single listener through two
        // connections to exercise the same path used in production.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for seq in 1..=2u64 {
                let (mut stream, _) = listener.accept().unwrap();
                let _logon =
                    read_one_fix_message(&mut stream, Instant::now() + Duration::from_secs(2));
                let logon_resp = FixMessageBuilder::new(tags::MSG_LOGON)
                    .str_tag(tags::ENCRYPT_METHOD, "0")
                    .u64_tag(tags::HEART_BT_INT, 30)
                    .build("SERVER", "CLIENT", seq);
                stream.write_all(&logon_resp).unwrap();
                // Drop the stream — this is what the wrapper observes as
                // "connection closed" and triggers the reconnect loop.
            }
        });

        // Connection 1: completes Logon, then the server drops us.
        let mut c1 = FixClient::connect(&addr.to_string(), "CLIENT", "SERVER", 30).unwrap();
        c1.set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        // Drain the EOF — try_recv surfaces it as Err once the kernel
        // delivers the FIN. May take a couple of reads to materialise.
        let mut saw_close = false;
        for _ in 0..6 {
            if c1.try_recv().is_err() {
                saw_close = true;
                break;
            }
        }
        assert!(saw_close, "expected try_recv to surface the server drop");
        drop(c1);

        // Connection 2: simulates the wrapper's reconnect — fresh client,
        // same address. Must complete Logon without leaning on any state
        // from c1.
        let _c2 = FixClient::connect(&addr.to_string(), "CLIENT", "SERVER", 30).unwrap();
        server.join().unwrap();
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
