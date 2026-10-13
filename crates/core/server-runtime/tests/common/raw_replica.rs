//! A replica written by hand, shared by the integration tests that need
//! one: it authenticates and handshakes as a fresh replica, then reads the
//! stream and acks only when, and what, its test tells it to.
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy.

// Each test binary compiles its own copy of this module and uses a subset
// of it; what one binary leaves unused is another's.
#![allow(dead_code)]

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use counter_server::CounterEvent;
use melin_client::SigningKey;

/// A replica written by hand: it authenticates and handshakes as a fresh
/// replica, then reads the stream only when told to. A real replica reads
/// as fast as its journal allows; this one is what one that has stopped
/// reading looks like to its primary, or, acking with
/// [`ack_tracks`](Self::ack_tracks), one whose journal has stopped.
pub struct RawReplica {
    stream: TcpStream,
}

impl RawReplica {
    /// The receive buffer asked for: small, so that what the replica holds
    /// unread is a known, small part of what the primary has queued.
    const RECEIVE_BUFFER: libc::c_int = 32 * 1024;

    /// Join `primary`'s replication endpoint as a fresh replica keyed by
    /// `key`, waiting at most `frame_limit` for each frame the primary owes.
    pub fn join(primary: SocketAddr, key: &SigningKey, frame_limit: Duration) -> Self {
        use ed25519_dalek::Signer;
        use melin_transport_core::replication::protocol::{
            Handshake as ReplicaHandshake, decode_auth_result, decode_challenge,
            encode_challenge_response, encode_handshake,
        };

        let mut stream = connect_with_receive_buffer(primary, Self::RECEIVE_BUFFER);
        stream
            .set_read_timeout(Some(frame_limit))
            .expect("set read timeout");
        let challenge = read_any_frame(&mut stream).expect("the primary's challenge");
        let nonce = decode_challenge(&challenge).expect("a challenge");
        let mut out = Vec::new();
        encode_challenge_response(
            &key.sign(&nonce).to_bytes(),
            key.verifying_key().as_bytes(),
            &mut out,
        );
        stream.write_all(&out).expect("send the challenge response");
        let verdict = read_any_frame(&mut stream).expect("the primary's verdict");
        assert!(
            decode_auth_result(&verdict).expect("an auth result"),
            "the primary refused the replication key"
        );
        out.clear();
        encode_handshake(
            &ReplicaHandshake {
                last_sequence: 0,
                chain_hash: [0; 32],
                epoch: 0,
            },
            &mut out,
        );
        stream.write_all(&out).expect("send the handshake");
        RawReplica { stream }
    }

    /// Read the stream until an entry batch carries `sequence`, and return
    /// that batch's last sequence.
    pub fn read_through(&mut self, sequence: u64) -> u64 {
        use melin_transport_core::replication_wire::{MSG_INPUT_BATCH, try_decode_input_batch};
        loop {
            let frame = read_any_frame(&mut self.stream).expect("the catch-up");
            if frame.first() != Some(&MSG_INPUT_BATCH) {
                continue; // StreamStart, and any other control frame.
            }
            let slots = try_decode_input_batch::<CounterEvent>(&frame).expect("an entry batch");
            let last = slots.last().expect("a batch carries entries").sequence;
            if last >= sequence {
                return last;
            }
        }
    }

    /// Ack `sequence` as journaled, as a replica streaming does: the first
    /// sign the primary has that the replica is past its install.
    pub fn ack(&mut self, sequence: u64) {
        self.ack_tracks(sequence, sequence);
    }

    /// Ack the two tracks apart: `persisted` journaled, `in_memory` taken
    /// into the pipeline. A replica whose journal has stopped keeps the
    /// first still while the second climbs.
    pub fn ack_tracks(&mut self, persisted: u64, in_memory: u64) {
        use melin_transport_core::replication::protocol::{Ack, encode_ack};
        let mut out = Vec::new();
        encode_ack(
            &Ack {
                acked_sequence: persisted,
                in_memory_sequence: in_memory,
            },
            &mut out,
        );
        self.stream.write_all(&out).expect("send an ack");
    }
}

/// A kernel-TCP connection to `peer` whose receive buffer was set before
/// connecting, when it still bounds the window offered.
pub fn connect_with_receive_buffer(peer: SocketAddr, receive_buffer: libc::c_int) -> TcpStream {
    use std::os::fd::FromRawFd;
    let SocketAddr::V4(peer) = peer else {
        panic!("the runner's network is IPv4");
    };
    // SAFETY: a plain socket(2); its descriptor is owned by the `TcpStream`
    // at once, which closes it on every path out.
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
    assert!(fd >= 0, "socket: {}", io::Error::last_os_error());
    // SAFETY: `fd` is a fresh socket that nothing else owns.
    let stream = unsafe { TcpStream::from_raw_fd(fd) };
    // SAFETY: the option value is a live `c_int`, of the length passed.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&receive_buffer as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "SO_RCVBUF: {}", io::Error::last_os_error());
    let address = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: peer.port().to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from(*peer.ip()).to_be(),
        },
        sin_zero: [0; 8],
    };
    // SAFETY: `address` is a live `sockaddr_in`, of the length passed.
    let rc = unsafe {
        libc::connect(
            fd,
            (&address as *const libc::sockaddr_in).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "connect to {peer}: {}", io::Error::last_os_error());
    stream
}

/// One length-prefixed frame's payload, of any size.
pub fn read_any_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut prefix = [0u8; 4];
    stream.read_exact(&mut prefix)?;
    let mut payload = vec![0u8; u32::from_le_bytes(prefix) as usize];
    stream.read_exact(&mut payload)?;
    Ok(payload)
}
