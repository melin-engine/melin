//! The Ed25519 challenge/response, driven non-blocking over any transport.

use std::time::Duration;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};

use crate::transport::Transport;
use crate::tsc::{TscClock, rdtscp};
use crate::wire::{self, Frame, Inbound};

/// Read the challenge, sign its nonce, send the signature with the public
/// key, and wait for the server to say it is ready.
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
