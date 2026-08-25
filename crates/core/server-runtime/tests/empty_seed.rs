//! Regression test: the server must start and accept connections even when
//! `AppFactory::seed_events()` returns an empty Vec. Before the fix, the
//! seed drain gate spin-looped forever because no events were published
//! but the target cursor was 1.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use melin_app::app_factory::AppFactory;
use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder};
use melin_app::encoder::ResponseEncoder;
use melin_app::{AppEvent, Application, ApplyCtx, CodecError, RejectReason};
use melin_server_runtime::server::{self, ServerConfig};
use melin_wire_protocol::control_codec::TAG_CHALLENGE;
use melin_wire_protocol::tcp::BlockingTcpListener;

// ---------------------------------------------------------------------------
// Minimal application — no seed events, no meaningful business logic.
// Just enough to satisfy the trait bounds so the pipeline boots.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct NoOpEvent;

impl AppEvent for NoOpEvent {
    const MAX_ENCODED_SIZE: usize = 1;

    fn encoded_size(&self) -> usize {
        1
    }
    fn encode(&self, buf: &mut [u8]) -> usize {
        buf[0] = 0x10;
        1
    }
    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        if buf.is_empty() {
            return Err(CodecError::Truncated);
        }
        Ok(NoOpEvent)
    }
    fn is_query(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Copy)]
struct NoOpReport;

struct NoOpApp;

impl Application for NoOpApp {
    type Event = NoOpEvent;
    type Report = NoOpReport;
    type QueryResponse = NoOpReport;
    const APP_VERSION: u16 = 1;

    fn apply(
        &mut self,
        _event: NoOpEvent,
        _ctx: &ApplyCtx,
        _out: &mut Vec<NoOpReport>,
    ) -> Option<NoOpReport> {
        None
    }
    fn tick(&mut self, _now_ns: u64, _out: &mut Vec<NoOpReport>) {}
    fn check_request_seq(&mut self, _key_hash: u64, _seq: u64) -> bool {
        true
    }
    fn build_reject(_event: &NoOpEvent, _reason: RejectReason) -> NoOpReport {
        NoOpReport
    }
    fn snapshot<W: Write>(&self, _w: &mut W) -> io::Result<()> {
        Ok(())
    }
    fn restore<R: Read>(_r: &mut R) -> io::Result<Self> {
        Ok(NoOpApp)
    }
}

struct NoOpFactory;

impl AppFactory for NoOpFactory {
    type App = NoOpApp;
    fn empty(&self) -> NoOpApp {
        NoOpApp
    }
    fn prefault(&self, _app: &mut NoOpApp) {}
    // seed_events() intentionally uses the default (empty Vec).
}

struct NoOpDecoder;

impl RequestDecoder for NoOpDecoder {
    type Event = NoOpEvent;
    fn decode(&self, _bytes: &[u8], _permission: Permission) -> Decoded<NoOpEvent> {
        Decoded::Filter
    }
}

struct NoOpEncoder;

impl ResponseEncoder for NoOpEncoder {
    type Report = NoOpReport;
    type Query = NoOpReport;
    fn encode_report(&self, _report: &NoOpReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        buf[..5].copy_from_slice(&[1, 0, 0, 0, 0x10]);
        Ok(5)
    }
    fn encode_query(&self, _query: &NoOpReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        buf[..5].copy_from_slice(&[1, 0, 0, 0, 0x10]);
        Ok(5)
    }
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

/// The server must reach its accept loop within a few seconds even when
/// the factory produces zero seed events. Verifying this is as simple as
/// connecting and reading the auth Challenge — if the seed drain is stuck,
/// the Challenge never arrives and the read times out.
#[test]
fn server_starts_with_empty_seed_events() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let auth_path = tmp.path().join("authorized_keys");
    std::fs::write(&auth_path, "").expect("write empty auth keys");

    let listener = BlockingTcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().expect("parse"))
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let config = ServerConfig {
        bind: addr,
        journal: tmp.path().join("test.journal"),
        authorized_keys: auth_path,
        standalone: true,
        ack_policy: melin_server_runtime::ack_policy::AckPolicy::Disk,
        no_mlock: true,
        tick_interval_ms: 0,
        snapshot_interval_ms: 0,
        health_bind: None,
        accounts: 0,
        instruments: 0,
        ..ServerConfig::default()
    };

    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();

    let handle = std::thread::spawn(move || -> Result<(), String> {
        let _tmp = tmp;
        server::run_with_listener(
            listener,
            config,
            NoOpFactory,
            NoOpDecoder,
            NoOpEncoder,
            None,
            sd,
        )
        .map_err(|e| e.to_string())
    });

    // Try to connect and read the Challenge frame. The 3-second timeout
    // is the regression detector: before the fix this would hang forever.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let got_challenge = loop {
        if handle.is_finished() {
            panic!("server exited early: {:?}", handle.join().unwrap());
        }
        if let Ok(stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(200)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .expect("set timeout");
            let mut stream = stream;
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).is_ok() {
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut payload = vec![0u8; len];
                if stream.read_exact(&mut payload).is_ok() {
                    assert_eq!(payload[0], TAG_CHALLENGE);
                    break true;
                }
            }
        }
        if std::time::Instant::now() > deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(50));
    };

    shutdown.store(true, Ordering::Relaxed);
    let _ = TcpStream::connect_timeout(&addr, Duration::from_millis(100));
    let server_result = handle.join().expect("server thread panicked");

    assert!(
        got_challenge,
        "server never reached accept loop (seed drain deadlock?)"
    );
    server_result.expect("server returned error");
}
