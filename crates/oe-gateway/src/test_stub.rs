//! Loopback Melin server stub for end-to-end gateway tests.
//!
//! Accepts one TCP connection, runs the challenge/response handshake
//! (trusting any signature), and then acts as a request/response
//! playback surface driven by the test via channels.
//!
//! Lifetime:
//! ```text
//!   test                 stub thread            gateway
//!     |                      |                     |
//!     |---- MelinStub::start-|                     |
//!     |    bind 127.0.0.1:0  |                     |
//!     |<---- port ------------|                     |
//!     | (test builds config & spawns gateway)      |
//!     |                      |<-- accept connect --|
//!     |                      |--- Challenge ------>|
//!     |                      |<-- ChallengeResp ---|
//!     |                      |--- ServerReady ---->|
//!     |<-- next_request -----|                     |
//!     |---- send_response ->-|                     |
//!     |                      |--- Response ------->|
//!     |    ...               |                     |
//!     |---- drop() ----------|                     |
//!     |    (joins thread)                          |
//! ```

#![cfg(test)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender, TryRecvError, channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use melin_protocol::codec;
use melin_protocol::message::{Request, ResponseKind};

/// Control handle owned by the test. Starts a stub listener, connects
/// to the first gateway connection, and exposes channels for driving
/// the request/response flow.
pub struct MelinStub {
    port: u16,
    requests: Receiver<(u64, Request)>,
    responses: Sender<ResponseKind>,
    shutdown: Arc<AtomicBool>,
    /// Set by the stub thread when it observes EOF on the gateway
    /// connection (i.e. the gateway closed its Melin socket).
    disconnected: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
    /// Errors observed by the stub thread — pulled in `drop` to fail
    /// the test if the stub crashed.
    errors: Arc<Mutex<Vec<String>>>,
}

impl MelinStub {
    /// Bind a listener on `127.0.0.1:0`, spawn the stub thread, and
    /// return a handle. The thread blocks waiting for one inbound
    /// connection (the gateway).
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub");
        let port = listener.local_addr().unwrap().port();

        let (req_tx, req_rx) = channel::<(u64, Request)>();
        let (resp_tx, resp_rx) = channel::<ResponseKind>();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let disconnected = Arc::new(AtomicBool::new(false));
        let disconnected_clone = disconnected.clone();
        let errors = Arc::new(Mutex::new(Vec::<String>::new()));
        let errors_clone = errors.clone();

        let join = std::thread::spawn(move || {
            if let Err(e) = run_stub(
                listener,
                req_tx,
                resp_rx,
                shutdown_clone,
                disconnected_clone,
            ) {
                errors_clone.lock().unwrap().push(e);
            }
        });

        Self {
            port,
            requests: req_rx,
            responses: resp_tx,
            shutdown,
            disconnected,
            join: Some(join),
            errors,
        }
    }

    /// Wait up to `timeout` for the stub to observe the gateway
    /// closing its Melin socket. Returns true if EOF was seen.
    pub fn wait_for_disconnect(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.disconnected.load(Ordering::Relaxed) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.disconnected.load(Ordering::Relaxed)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Wait up to `timeout` for the next request from the gateway.
    /// Panics if the timeout expires — tests should set this generously
    /// enough to absorb scheduling jitter but short enough to fail fast.
    pub fn next_request(&self, timeout: Duration) -> (u64, Request) {
        self.requests
            .recv_timeout(timeout)
            .expect("stub did not receive a request in time")
    }

    /// Queue a response for the stub to send on the wire. Non-blocking.
    pub fn send_response(&self, resp: ResponseKind) {
        self.responses
            .send(resp)
            .expect("stub thread dropped response channel");
    }
}

impl Drop for MelinStub {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        // Wake the stub thread if it's blocked in accept by dialing it.
        // (If it already accepted, this connect just gets dropped.)
        let _ = TcpStream::connect(("127.0.0.1", self.port));
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
        let errs = self.errors.lock().unwrap();
        if !errs.is_empty() && !std::thread::panicking() {
            panic!("stub thread errors: {:?}", *errs);
        }
    }
}

/// Stub thread main loop. Returns Err with a message if anything
/// unexpected happens — the test Drop surface will then fail.
fn run_stub(
    listener: TcpListener,
    requests: Sender<(u64, Request)>,
    responses: Receiver<ResponseKind>,
    shutdown: Arc<AtomicBool>,
    disconnected: Arc<AtomicBool>,
) -> Result<(), String> {
    // Only accept the first real inbound connection. If shutdown fires
    // before the gateway dials, we bail out cleanly via the dummy dial
    // the handle does on Drop.
    listener
        .set_nonblocking(false)
        .map_err(|e| format!("set_nonblocking: {e}"))?;
    let (mut stream, _peer) = listener.accept().map_err(|e| format!("accept: {e}"))?;
    if shutdown.load(Ordering::Relaxed) {
        return Ok(());
    }

    // Short read timeout so the loop can poll the response channel and
    // the shutdown flag. 50ms is well below any test assertion timeout
    // but keeps the loop responsive.
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .map_err(|e| format!("set_read_timeout: {e}"))?;

    // --- Auth handshake ---
    // Send Challenge with a deterministic nonce. We don't verify the
    // signature the gateway returns — tests only care that the state
    // machine progresses.
    let nonce = [0u8; 32];
    write_response(&mut stream, &ResponseKind::Challenge { nonce })?;

    let (_seq, req) = read_request_blocking(&mut stream, &shutdown)?;
    match req {
        Request::ChallengeResponse { .. } => {}
        other => return Err(format!("expected ChallengeResponse, got {other:?}")),
    }
    write_response(&mut stream, &ResponseKind::ServerReady)?;

    // --- Request/response loop ---
    let mut accum: Vec<u8> = Vec::with_capacity(256);
    let mut tmp = [0u8; 256];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        // Drain any pending responses first.
        loop {
            match responses.try_recv() {
                Ok(resp) => write_response(&mut stream, &resp)?,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return Ok(()),
            }
        }

        // Try to read. 50ms timeout means we wake often enough to
        // notice new queued responses and the shutdown flag.
        match stream.read(&mut tmp) {
            Ok(0) => {
                // Gateway closed.
                disconnected.store(true, Ordering::Relaxed);
                return Ok(());
            }
            Ok(n) => accum.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                disconnected.store(true, Ordering::Relaxed);
                return Ok(());
            }
            Err(e) => return Err(format!("read: {e}")),
        }

        // Frame as many complete requests as `accum` contains.
        while let Some((seq, req)) = try_extract_request(&mut accum)? {
            requests
                .send((seq, req))
                .map_err(|_| "request channel closed".to_string())?;
        }
    }
    Ok(())
}

/// Blocking single-request read used during the auth handshake. Loops
/// until a full `[u32 len][payload]` frame has arrived, honoring the
/// shutdown flag between short read intervals.
fn read_request_blocking(
    stream: &mut TcpStream,
    shutdown: &Arc<AtomicBool>,
) -> Result<(u64, Request), String> {
    let mut accum = Vec::with_capacity(128);
    let mut tmp = [0u8; 128];
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return Err("shutdown during handshake".to_string());
        }
        if let Some(pair) = try_extract_request(&mut accum)? {
            return Ok(pair);
        }
        match stream.read(&mut tmp) {
            Ok(0) => return Err("EOF during handshake".to_string()),
            Ok(n) => accum.extend_from_slice(&tmp[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue;
            }
            Err(e) => return Err(format!("handshake read: {e}")),
        }
    }
}

/// If `buf` contains at least one complete `[u32 len][seq+tag+payload]`
/// frame, drain it and decode. Returns Ok(None) if the frame is not
/// yet complete.
fn try_extract_request(buf: &mut Vec<u8>) -> Result<Option<(u64, Request)>, String> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + len {
        return Ok(None);
    }
    let payload: Vec<u8> = buf.drain(..4 + len).skip(4).collect();
    let (seq, req) =
        codec::decode_request(&payload).map_err(|e| format!("decode_request: {e:?}"))?;
    Ok(Some((seq, req)))
}

/// Encode and write one response on the wire.
fn write_response(stream: &mut TcpStream, resp: &ResponseKind) -> Result<(), String> {
    let mut buf = [0u8; 256];
    let n =
        codec::encode_response(resp, &mut buf).map_err(|e| format!("encode_response: {e:?}"))?;
    stream
        .write_all(&buf[..n])
        .map_err(|e| format!("write: {e}"))?;
    Ok(())
}
