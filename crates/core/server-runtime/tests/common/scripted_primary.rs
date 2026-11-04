//! A primary written by hand, shared by the integration tests that need
//! one: it takes one replica at a time, streams it the entries its test
//! writes, with the sequences and timestamps the test chooses, and waits
//! for the replica's acks. A real primary stamps entries with its own
//! clock and decides its own batches; this one lets a test build a history
//! a real primary on the test's host would not (one that stops part way
//! through its genesis, or whose timestamps run ahead of the host's
//! clock).
//!
//! A kernel socket on `melin_test_node::local_ip`, which the nodes reach
//! on either transport.
//!
//! Each test binary that uses it includes this file as a module (`#[path]`),
//! and compiles its own copy. It carries no dead-code allowance: an item
//! one includer leaves unused warns there, and is moved or dropped rather
//! than hidden.

use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use melin_app::AppEvent;
use melin_journal::JournalEvent;
use melin_transport_core::pipeline::InputSlot;
use melin_transport_core::replication::protocol::{
    MAX_CONTROL_FRAME, ReplicaMessage, decode_replica_message, encode_auth_ok, encode_challenge,
    encode_stream_start, read_frame,
};
use melin_transport_core::replication_wire::encode_input_batch;

/// How long the scripted primary waits for a frame its replica owes.
const FRAME_LIMIT: Duration = Duration::from_secs(30);

/// A replication endpoint written by hand, before its replica connects.
pub struct ScriptedPrimary {
    listener: TcpListener,
}

impl ScriptedPrimary {
    /// Listen on the address the nodes reach the test at, on a port the
    /// kernel picks.
    pub fn bind() -> Self {
        Self {
            listener: TcpListener::bind((melin_test_node::local_ip(), 0))
                .expect("bind the scripted primary's replication listener"),
        }
    }

    /// What a replica of it takes as `replica_of`.
    pub fn addr(&self) -> SocketAddr {
        self.listener.local_addr().expect("the listener's address")
    }

    /// Accept the replica, take its word for its key and for where its
    /// journal ends, and stream to it from there: the history is one, at
    /// epoch 0, whose genesis takes `genesis_entries`, and the replica
    /// joins it fresh or resumes it after a restart.
    pub fn accept(&self, genesis_entries: Option<u64>) -> Stream {
        let (mut stream, _) = self.listener.accept().expect("the replica connects");
        stream
            .set_read_timeout(Some(FRAME_LIMIT))
            .expect("read timeout");
        let mut buf = Vec::new();
        encode_challenge(&[0x5A; 32], &mut buf);
        stream.write_all(&buf).expect("challenge");
        read_frame(&mut stream, MAX_CONTROL_FRAME).expect("challenge response");
        buf.clear();
        encode_auth_ok(&mut buf);
        stream.write_all(&buf).expect("auth ok");
        let handshake = read_frame(&mut stream, MAX_CONTROL_FRAME).expect("handshake");
        let resumes_after = match decode_replica_message(&handshake).expect("decode handshake") {
            ReplicaMessage::Handshake(h) => h.last_sequence,
            other => panic!("expected a handshake, got {other:?}"),
        };
        buf.clear();
        // The same history every time: its first segment's start and
        // anchor, which a resuming replica's journal header holds.
        encode_stream_start(resumes_after, 1, [0x3D; 32], genesis_entries, 0, &mut buf);
        stream.write_all(&buf).expect("StreamStart");
        Stream {
            stream,
            resumes_after,
        }
    }
}

/// The stream to the one replica a [`ScriptedPrimary`] took. Dropping it
/// is the primary dying, as far as the replica can tell.
pub struct Stream {
    stream: TcpStream,
    /// The last sequence the replica's handshake said its journal holds:
    /// 0 for a fresh replica. `u64`, as the wire's.
    resumes_after: u64,
}

impl Stream {
    /// Where the replica's journal ended when it connected: 0 when it
    /// joined fresh.
    pub fn resumes_after(&self) -> u64 {
        self.resumes_after
    }

    /// Stream one batch: application event `events[i]` at sequence
    /// `first + i`, every one stamped `timestamp_ns`, as a primary's
    /// entries of one receive are. No connection, request or key: the
    /// replica applies them as it would any primary's.
    pub fn send<E: AppEvent>(&mut self, first: u64, timestamp_ns: u64, events: &[E]) {
        let slots: Vec<InputSlot<E>> = events
            .iter()
            .zip(first..)
            .map(|(&event, sequence)| InputSlot {
                sequence,
                ..InputSlot::node_event(JournalEvent::App(event), timestamp_ns)
            })
            .collect();
        let mut buf = Vec::new();
        encode_input_batch(&slots, &mut buf).expect("encode InputBatch");
        self.stream.write_all(&buf).expect("InputBatch");
    }

    /// Wait for the replica to ack `sequence` as journaled.
    pub fn wait_acked(&mut self, sequence: u64) {
        loop {
            let frame = read_frame(&mut self.stream, MAX_CONTROL_FRAME).expect("ack");
            if let ReplicaMessage::Ack(ack) = decode_replica_message(&frame).expect("decode ack")
                && ack.acked_sequence >= sequence
            {
                return;
            }
        }
    }
}
