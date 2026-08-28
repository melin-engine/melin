//! The seam between the loop and the wire, and the kernel side of it.
//!
//! Both transports are non-blocking and polled from the one thread: a
//! `send` may take part of the data or none, `recv` may return nothing,
//! and `service` is where a transport that has work to do between the
//! two -- a userspace TCP stack -- does it. The loop is generic over the
//! trait, so there is no indirection on the hot path.

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};

use crate::tsc::{TscClock, rdtscp};
use crate::wire::{self, Frame, Inbound};

pub trait Transport {
    /// Queue as much of `data` as the transport will take right now.
    /// `Ok(0)` means none of it: try again after `service`.
    fn send(&mut self, data: &[u8]) -> io::Result<usize>;

    /// Move queued bytes toward the wire and inbound bytes toward `recv`.
    /// Called after every send and on every idle spin.
    fn service(&mut self, now_unix_ns: u64);

    /// Read what has arrived. `Ok(0)` means nothing yet; a closed
    /// connection is an error, never a zero.
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize>;

    fn name(&self) -> &'static str;
}

/// Kernel TCP, non-blocking, `TCP_NODELAY`. The baseline the DPDK path is
/// measured against, and what makes the rest of this binary testable on
/// a machine with no DPDK.
pub struct KernelTcp {
    stream: TcpStream,
}

impl KernelTcp {
    pub fn connect(addr: SocketAddr, timeout: Duration) -> io::Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, timeout)?;
        stream.set_nodelay(true)?;
        stream.set_nonblocking(true)?;
        Ok(Self { stream })
    }
}

fn would_block(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
    )
}

impl Transport for KernelTcp {
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        match self.stream.write(data) {
            Ok(n) => Ok(n),
            Err(e) if would_block(&e) => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn service(&mut self, _now_unix_ns: u64) {}

    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.stream.read(buf) {
            Ok(0) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the server closed the connection",
            )),
            Ok(n) => Ok(n),
            Err(e) if would_block(&e) => Ok(0),
            Err(e) => Err(e),
        }
    }

    fn name(&self) -> &'static str {
        "kernel TCP"
    }
}

/// The Ed25519 challenge/response, driven non-blocking over any
/// transport: read the challenge, sign its nonce, send the signature with
/// the public key, and wait for the server to say it is ready.
pub fn authenticate<T: Transport>(
    transport: &mut T,
    key: &SigningKey,
    clock: &TscClock,
    inbound: &mut Inbound,
    timeout: Duration,
) -> Result<(), String> {
    // u64 nanoseconds: a timeout of seconds, and `ticks` takes u64.
    let deadline = rdtscp().saturating_add(clock.ticks(timeout.as_nanos() as u64));
    let mut response: Option<Vec<u8>> = None;
    let mut sent = 0usize;

    loop {
        let now = rdtscp();
        if now >= deadline {
            return Err(format!(
                "no authentication reply from the server within {}s",
                timeout.as_secs()
            ));
        }
        transport.service(clock.unix_ns(now));

        if let Some(bytes) = &response
            && sent < bytes.len()
        {
            sent += transport
                .send(&bytes[sent..])
                .map_err(|e| format!("cannot send the challenge response: {e}"))?;
            continue;
        }

        let space = inbound.space();
        if space.is_empty() {
            return Err("inbound buffer full during authentication".into());
        }
        let n = transport
            .recv(space)
            .map_err(|e| format!("connection lost during authentication: {e}"))?;
        inbound.filled(n);

        while let Some(payload) = inbound.pop()? {
            match wire::decode(payload) {
                Frame::Challenge(nonce) => {
                    if nonce.len() != 32 {
                        return Err(format!(
                            "the challenge carries a {}-byte nonce, expected 32",
                            nonce.len()
                        ));
                    }
                    let signature = key.sign(nonce);
                    response = Some(wire::auth_response(
                        &signature.to_bytes(),
                        &key.verifying_key().to_bytes(),
                    ));
                    sent = 0;
                }
                Frame::ServerReady => return Ok(()),
                Frame::AuthFailed => {
                    return Err(format!(
                        "authentication failed: is {} listed in the server's authorized_keys?",
                        base64::engine::general_purpose::STANDARD
                            .encode(key.verifying_key().to_bytes())
                    ));
                }
                Frame::Heartbeat | Frame::BatchEnd => {}
                _ => return Err("unexpected frame from the server during authentication".into()),
            }
        }
    }
}
