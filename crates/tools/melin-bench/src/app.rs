//! What a request is, per application: how one is encoded and how its
//! reply is checked. Everything else in the load generator is the same
//! for every application.

use clap::ValueEnum;

use echo_server::{KIND_RESP_ECHO, KIND_RESP_REJECTED as ECHO_REJECTED, MAX_PAYLOAD};
use notary_server::{
    KIND_NOTARIZE, KIND_RESP_RECEIPT, KIND_RESP_REJECTED as NOTARY_REJECTED, LEAF_LEN,
};

/// Bytes a request's fingerprint takes: the client's index and the
/// request's number on its connection.
const FINGERPRINT_LEN: usize = 12;

#[derive(Clone, Copy, PartialEq, Eq, Debug, ValueEnum)]
pub enum AppKind {
    /// `echo-server`: the request's bytes, echoed after they are
    /// sequenced and journaled.
    Echo,
    /// `notary-server`: a 32-byte leaf, notarized into the log.
    Notary,
}

/// The application under load, with what its requests carry.
#[derive(Clone, Copy, Debug)]
pub enum App {
    Echo { size: usize },
    Notary,
}

impl App {
    /// The application `kind` names, its echo payload `size` bytes.
    pub fn new(kind: AppKind, size: usize) -> Result<Self, String> {
        match kind {
            AppKind::Echo if !(FINGERPRINT_LEN..=MAX_PAYLOAD).contains(&size) => Err(format!(
                "--size {size}: an echo carries {FINGERPRINT_LEN} to {MAX_PAYLOAD} bytes"
            )),
            AppKind::Echo => Ok(Self::Echo { size }),
            AppKind::Notary => Ok(Self::Notary),
        }
    }

    /// The name results are labelled with.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Echo { .. } => "echo",
            Self::Notary => "notary",
        }
    }

    /// The most bytes one request's body takes.
    pub fn max_body(&self) -> usize {
        match self {
            Self::Echo { size } => *size,
            Self::Notary => 1 + LEAF_LEN,
        }
    }

    /// Write request `seq` of `client` into `out`, returning its length.
    /// `out` holds at least [`max_body`](Self::max_body) bytes.
    pub fn encode(&self, out: &mut [u8], client: u32, seq: u64) -> usize {
        let body = &mut out[..self.max_body()];
        match self {
            Self::Echo { .. } => {
                body.fill(0);
                fingerprint(body, client, seq);
            }
            Self::Notary => {
                // A leaf per request, so no two entries of the log are
                // alike; the rest of it is padding.
                body.fill(0);
                body[0] = KIND_NOTARIZE;
                fingerprint(&mut body[1..], client, seq);
            }
        }
        body.len()
    }

    /// Check `reply`, the response to request `seq` of `client`.
    pub fn check(&self, reply: &[u8], client: u32, seq: u64) -> Result<(), String> {
        let (expected, rejected) = match self {
            Self::Echo { .. } => (KIND_RESP_ECHO, ECHO_REJECTED),
            Self::Notary => (KIND_RESP_RECEIPT, NOTARY_REJECTED),
        };
        match reply.split_first() {
            Some((&kind, rest)) if kind == expected => match self {
                // An echo must be the request's own bytes back: a reply
                // to another request, or to none, is caught here.
                Self::Echo { size } => {
                    let mut sent = [0u8; FINGERPRINT_LEN];
                    fingerprint(&mut sent, client, seq);
                    if rest.len() == *size && rest[..FINGERPRINT_LEN] == sent {
                        Ok(())
                    } else {
                        Err(format!(
                            "client {client}: the echo of request {seq} is not what was sent"
                        ))
                    }
                }
                Self::Notary => Ok(()),
            },
            Some((&kind, _)) if kind == rejected => Err(format!(
                "client {client}: the server rejected request {seq}"
            )),
            _ => Err(format!(
                "client {client}: the reply to request {seq} is of no kind the {} server sends",
                self.name()
            )),
        }
    }
}

/// The client's index and the request's number, little-endian, at the
/// start of `out`.
fn fingerprint(out: &mut [u8], client: u32, seq: u64) {
    out[..4].copy_from_slice(&client.to_le_bytes());
    out[4..FINGERPRINT_LEN].copy_from_slice(&seq.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(app: App, client: u32, seq: u64) -> Vec<u8> {
        let mut out = vec![0xAA; app.max_body() + 8];
        let n = app.encode(&mut out, client, seq);
        out.truncate(n);
        out
    }

    #[test]
    fn an_echo_passes_only_as_its_own_request_s_bytes() {
        let app = App::new(AppKind::Echo, 64).unwrap();
        let sent = request(app, 3, 41);
        assert_eq!(sent.len(), 64);

        let mut reply = vec![KIND_RESP_ECHO];
        reply.extend_from_slice(&sent);
        app.check(&reply, 3, 41).unwrap();
        assert!(app.check(&reply, 3, 42).is_err(), "another request's echo");
        assert!(app.check(&reply, 4, 41).is_err(), "another client's echo");
        assert!(app.check(&reply[..32], 3, 41).is_err(), "a short echo");
        let rejected = app.check(&[ECHO_REJECTED], 3, 41).unwrap_err();
        assert!(rejected.contains("rejected"), "{rejected}");
        assert!(app.check(&[], 3, 41).is_err());
    }

    #[test]
    fn a_notarization_is_a_leaf_per_request_answered_by_a_receipt() {
        let app = App::new(AppKind::Notary, 0).unwrap();
        let a = request(app, 0, 1);
        let b = request(app, 0, 2);
        assert_eq!(a.len(), 1 + LEAF_LEN);
        assert_eq!(a[0], KIND_NOTARIZE);
        assert_ne!(a, b, "two requests notarize two leaves");

        app.check(&[KIND_RESP_RECEIPT, 0, 0], 0, 1).unwrap();
        let rejected = app.check(&[NOTARY_REJECTED], 0, 1).unwrap_err();
        assert!(rejected.contains("rejected"), "{rejected}");
        assert!(app.check(&[0x99], 0, 1).is_err());
    }

    #[test]
    fn an_echo_size_must_hold_the_fingerprint_and_fit_the_server() {
        assert!(App::new(AppKind::Echo, FINGERPRINT_LEN - 1).is_err());
        assert!(App::new(AppKind::Echo, MAX_PAYLOAD + 1).is_err());
        assert!(App::new(AppKind::Echo, MAX_PAYLOAD).is_ok());
    }
}
