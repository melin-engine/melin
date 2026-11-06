//! Replication wire protocol — message types, framing, encode/decode.
//!
//! Length-prefixed frames, little-endian, over a dedicated TCP connection
//! (or DPDK pipe). See the parent module for the message catalogue.

use std::io::{self, Read};

use zerocopy::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

// The wire format for `MSG_INPUT_BATCH` lives in `crate::replication_wire`,
// so the journal stage can encode directly into the replication ring
// without depending on this submodule.

// --- Wire protocol message tags ---

pub const MSG_HANDSHAKE: u8 = 0x01;
pub const MSG_ACK: u8 = 0x02;
// Auth messages (exchanged before the handshake).
pub const MSG_CHALLENGE: u8 = 0x03;
pub const MSG_CHALLENGE_RESPONSE: u8 = 0x04;
pub const MSG_AUTH_OK: u8 = 0x05;
pub const MSG_AUTH_FAILED: u8 = 0x06;
/// In place of `AuthOk`: the replica's key verified, but it speaks an
/// older protocol than this primary. Carries the primary's version.
pub const MSG_VERSION_REFUSED: u8 = 0x07;
pub const MSG_STREAM_START: u8 = 0x10;
pub const MSG_NEED_SNAPSHOT: u8 = 0x11;
pub const MSG_HASH_MISMATCH: u8 = 0x12;
pub const MSG_SNAPSHOT_BEGIN: u8 = 0x13;
pub const MSG_SNAPSHOT_CHUNK: u8 = 0x14;
pub const MSG_SNAPSHOT_END: u8 = 0x15;
pub const MSG_ROTATE: u8 = 0x16;
pub const MSG_CHAIN_CHECK: u8 = 0x17;
pub const MSG_SEGMENT_SEED_BEGIN: u8 = 0x18;
/// The primary has stopped because the cluster's output diverged: a
/// replica that reads it stops too.
pub const MSG_HALT: u8 = 0x1A;
// `MSG_INPUT_BATCH` (0x21) — in `crate::replication_wire`; carries `InputSlot`
// records on the wire. Replaces the old `MSG_DATA_BATCH = 0x20` (removed
// in phase 3 of feat/unified-pipeline).
pub const MSG_HEARTBEAT: u8 = 0x30;

/// Replication protocol version, advertised in the replica's handshake
/// and validated by the primary. Bumped whenever a frame layout changes
/// so mixed-version pairs fail with a *diagnosable* error instead of an
/// opaque short-frame decode failure (or, worse, a silently-ignored
/// trailing field — zerocopy prefix parsing accepts longer frames).
/// History: 1 = pre-fencing (41-byte handshake, no epoch);
/// 2 = fencing epochs (epoch on handshake/StreamStart) + this field;
/// 3 = primary-driven rotation (`Rotate`) + chain validation (`ChainCheck`);
/// 4 = primary ack policy on `StreamStart` and `Heartbeat`;
/// 5 = `request_seq` dropped from the `InputBatch` slot header;
/// 6 = each `InputBatch` slot carries its journal entry's CRC32C, which
/// the replica verifies before accepting the entry. A v5 node would read
/// the trailer as the next slot, so mixed pairs must not stream;
/// 7 = `StreamStart` carries the lineage's genesis length, which a
/// replica records in its journal header (and checks against the one it
/// already has). A v6 replica would ignore it and lose the promotion
/// check it feeds, so mixed pairs must not stream;
/// 8 = the entry stream can carry an ack-policy change (journal tag
/// 0x05), which a v7 replica refuses as an unknown entry mid-stream, and
/// `StreamStart` and `Heartbeat` no longer carry the policy byte (a
/// replica judges by the changes its lineage holds), so both frames
/// changed layout;
/// 9 = the version moves into authentication: the replica states its own
/// in `ChallengeResponse`, and the primary admits one at or above its own
/// with an `AuthOk` naming the session's version, or answers
/// `VersionRefused`. Each side states one version and the newer one
/// decides whether it can speak the other's, so a later release can talk
/// down to this one without another refused session (the handshake's
/// check stays, against the session's version). Also in 9, unreleased:
/// `Ack` and `Handshake` gain a flag byte, whose one bit says the replica
/// stopped for divergence, and the primary's `Halt` frame says it has.
pub const REPL_PROTOCOL_VERSION: u16 = 9;

/// The flag bit, on a replica's `Ack` and `Handshake`, saying the replica
/// has stopped because the cluster's output diverged. Any other bit is
/// refused, never read as a stop or as no stop.
pub const FLAG_STOPPED: u8 = 0b1;

/// Maximum frame size for control messages (handshake, ack, etc.).
/// `InputBatch` frames can be much larger (up to a full 512 KiB ring chunk).
pub const MAX_CONTROL_FRAME: usize = 256;

/// Maximum `InputBatch` frame size. Must be >= the replication ring's
/// `CHUNK_SIZE` (512 KiB) — the journal stage's `InputBatch` buffer can
/// fill an entire chunk before sync. The 256 KiB headroom covers
/// length-prefix + per-slot framing overhead inside the chunk plus a
/// safety margin.
pub const MAX_DATA_FRAME: usize = 768 * 1024;

// --- Message structs / enums ---

/// Handshake message sent by the replica on connection.
#[derive(Debug, Clone)]
pub struct Handshake {
    pub last_sequence: u64,
    pub chain_hash: [u8; 32],
    /// The replica's current fencing epoch. The primary compares it against
    /// its own: a replica advertising a *higher* epoch has seen a promotion
    /// the primary hasn't, so the primary is stale and self-demotes (see
    /// `crate::fence`).
    pub epoch: u64,
}

/// Ack message sent by the replica.
///
/// Carries two cursors so the primary's response gate can evaluate
/// multi-level ack policies (see
/// `crate::ack_policy`):
///
/// - `acked_sequence` — highest sequence persisted on the replica
///   (`pwrite` and `fdatasync` returned, durable on any drive).
/// - `in_memory_sequence` — highest sequence the replica has accepted
///   into its pipeline pre-journal. Always `>= acked_sequence`.
#[derive(Debug, Clone, Copy)]
pub struct Ack {
    pub acked_sequence: u64,
    pub in_memory_sequence: u64,
}

/// Messages from primary to replica.
#[derive(Debug)]
pub enum PrimaryMessage {
    StreamStart {
        start_sequence: u64,
        /// Header identity for the journal segment a *fresh* replica
        /// should create before consuming the stream: the segment's
        /// `starting_sequence` and chain `anchor_hash`. For full
        /// catch-up this is the primary's oldest segment (lineage
        /// origin); after a snapshot transfer it is `snap_sequence + 1`
        /// anchored to the snapshot's chain hash. Replicas with
        /// existing local state ignore it.
        segment_start_sequence: u64,
        anchor_hash: [u8; 32],
        /// The lineage's genesis length, from the primary's journal
        /// header (`melin_journal::FileHeaderInfo::genesis_entries`;
        /// `None`: unknown, a lineage begun before the length was
        /// recorded). Unlike the segment identity it concerns every
        /// replica: a fresh one writes it into the journal it creates, so
        /// it survives restarts and is there to check at promotion, and
        /// one with a journal compares it with its own header's — two
        /// known values that differ are a lineage error.
        genesis_entries: Option<u64>,
        /// The primary's current fencing epoch. A replica that already
        /// observed a *higher* epoch refuses to follow this (stale) primary;
        /// a replica behind it adopts the epoch as the stream's `EpochBump`s
        /// replay. See `crate::fence`.
        ///
        /// No ack policy rides here: the policy is lineage metadata, which
        /// a replica learns from the `AckPolicyChange` entries it holds, so
        /// it never believes a policy before it holds the entries behind
        /// it.
        epoch: u64,
    },
    NeedSnapshot,
    HashMismatch,
    /// Start of a snapshot transfer. Sent after NeedSnapshot.
    SnapshotBegin {
        /// Total snapshot file size in bytes.
        snapshot_len: u64,
        /// Journal sequence at which the snapshot was taken.
        snap_sequence: u64,
        /// BLAKE3 chain hash at the snapshot point.
        snap_chain_hash: [u8; 32],
    },
    /// A chunk of snapshot data. Sent repeatedly after SnapshotBegin.
    SnapshotChunk(Vec<u8>),
    /// End of snapshot transfer. Contains CRC32C of the entire snapshot file.
    SnapshotEnd {
        crc32c: u32,
    },
    /// Liveness on an idle link: the last sequence sent.
    Heartbeat {
        sequence: u64,
    },
    /// Primary-driven segment rotation. Sent in-stream, after the last
    /// entry of the outgoing segment (`boundary_seq`) and before the
    /// first entry of the new one. The replica rotates its own journal
    /// at exactly this boundary, and `tail_hash` — the primary's chain
    /// value at `boundary_seq`, which is also the new segment's header
    /// anchor — doubles as a divergence check: a healthy replica's own
    /// tail equals it. Replicas never rotate on local triggers; shared
    /// boundaries are what make chain values comparable across nodes
    /// (and healthy replica journals bitwise mirrors of the primary's).
    Rotate {
        boundary_seq: u64,
        tail_hash: [u8; 32],
    },
    /// Live-stream chain validation. `chain_hash` is the primary's
    /// chain value at `sequence` (an fsync-batch boundary); the replica
    /// compares its own value at that sequence and treats a mismatch as
    /// divergence. Extends the handshake-time check to the live stream,
    /// catching corruption that creeps in mid-session.
    ChainCheck {
        sequence: u64,
        chain_hash: [u8; 32],
    },
    /// Start of a segment-seed transfer: the raw byte prefix (file
    /// header through the snapshot's last entry) of the primary segment
    /// containing `snap_sequence + 1`. Sent between the snapshot
    /// transfer and the post-snapshot `StreamStart`; the body rides
    /// `SnapshotChunk` frames and ends with a `SnapshotEnd` carrying the
    /// seed's CRC32C. Writing it verbatim as the replica's live segment
    /// makes the replica's segmentation a byte-copy of the primary's
    /// from birth, so chain values are comparable (and `Rotate`
    /// verification holds) with no alignment grace period.
    SegmentSeedBegin {
        /// Total seed length in bytes (file header included).
        seed_len: u64,
    },
    /// The primary has stopped because the cluster's output diverged.
    Halt,
}

/// A decoded `ChallengeResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChallengeResponse {
    pub signature: [u8; 64],
    pub pubkey: [u8; 32],
    /// The replication protocol version the sender speaks; `None` when it
    /// states none: a replica on v8 or earlier, or a control-plane link,
    /// whose version is exchanged after authentication.
    pub protocol_version: Option<u16>,
}

/// The answer to a `ChallengeResponse`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthResult {
    /// The key is accepted. `protocol_version` is the version the session
    /// runs at, which a replication primary states and a v8 primary or a
    /// control-plane peer does not (`None`).
    Ok { protocol_version: Option<u16> },
    /// The key is refused.
    Failed,
    /// The key is accepted, but the replica speaks an older protocol than
    /// the primary, whose version this is.
    VersionRefused { protocol_version: u16 },
}

/// Whether a primary admits a replica stating `stated` (`None`: a replica
/// on v8 or earlier, which states none): one at or above this primary's
/// version, which is trusted to speak this one. A newer replica that gets
/// that wrong sends a frame this primary cannot decode, which ends the
/// session.
pub fn admits_replica_version(stated: Option<u16>) -> bool {
    stated.is_some_and(|version| version >= REPL_PROTOCOL_VERSION)
}

/// Messages from replica to primary. A stopped replica's frames are
/// variants of their own, so no reader can take one for the ordinary
/// frame without the `match` saying so.
#[derive(Debug)]
pub enum ReplicaMessage {
    Handshake(Handshake),
    Ack(Ack),
    /// A handshake flagged [`FLAG_STOPPED`]: the replica has stopped and
    /// redials to say so.
    StoppedHandshake(Handshake),
    /// An ack flagged [`FLAG_STOPPED`]: the replica has stopped. Its
    /// positions are the last ones it sent, frozen.
    StoppedAck(Ack),
    /// An `Ack` or `Handshake` carrying a flag bit this build does not
    /// know: refused, neither read as a stop nor as an ordinary frame. A
    /// reader treats it as the protocol violation it is.
    UnknownFlags {
        tag: u8,
        flags: u8,
    },
}

// --- Wire structs ---
//
// Post-length form (the byte stripped by `read_frame` is the 4-byte
// length prefix; what remains starts with the tag). Each frame type
// has its own struct so encoders write all fields in one block and
// decoders peel a typed prefix instead of computing offsets.
//
// `little_endian::U{32,64}` are 1-byte-aligned LE wrappers, so a
// `repr(C)` struct of them is byte-packed (no padding) and serialises
// bit-for-bit identically to the previous hand-rolled `to_le_bytes`
// chains. Wire layout is authoritative — assertions below pin it.

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct HandshakeFrame {
    tag: u8,
    last_sequence: U64,
    chain_hash: [u8; 32],
    epoch: U64,
    /// [`REPL_PROTOCOL_VERSION`] — appended *last* deliberately: prefix
    /// parsing on an older peer still reads the fields it knows, and the
    /// new decoder rejects a mismatch with an explicit version error.
    protocol_version: U16,
    /// [`FLAG_STOPPED`] or 0. After the version, so an older primary still
    /// reads the version and refuses it.
    flags: u8,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AckFrame {
    tag: u8,
    acked_sequence: U64,
    in_memory_sequence: U64,
    /// [`FLAG_STOPPED`] or 0.
    flags: u8,
}

/// The `Halt` frame behind its length prefix: the tag alone.
#[derive(IntoBytes, Immutable, Unaligned)]
#[repr(C)]
struct PrefixedHaltFrame {
    len: U32,
    tag: u8,
}

/// Wire length, length prefix included, of the `Halt` frame
/// [`encode_halt`] returns.
pub const HALT_FRAME_LEN: usize = core::mem::size_of::<PrefixedHaltFrame>();

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ChallengeFrame {
    tag: u8,
    nonce: [u8; 32],
}

/// The response as a control-plane (raft) link sends it: its version is
/// exchanged separately, after authentication. Also the prefix of the
/// replication form below, and the whole of a v8 replica's.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ChallengeResponseFrame {
    tag: u8,
    signature: [u8; 64],
    pubkey: [u8; 32],
}

/// The response as a replica sends it: the unversioned frame plus the
/// replica's [`REPL_PROTOCOL_VERSION`], appended last so a v8 primary,
/// which reads the frame by prefix, still verifies the signature.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct VersionedChallengeResponseFrame {
    response: ChallengeResponseFrame,
    protocol_version: U16,
}

/// `AuthOk` and `VersionRefused` as a replication primary sends them: the
/// tag and the primary's version. Frozen, so any later replica reads it.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct VersionFrame {
    tag: u8,
    protocol_version: U16,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SnapshotBeginFrame {
    tag: u8,
    snapshot_len: U64,
    snap_sequence: U64,
    snap_chain_hash: [u8; 32],
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SnapshotEndFrame {
    tag: u8,
    crc32c: U32,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct HeartbeatFrame {
    tag: u8,
    sequence: U64,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SegmentSeedBeginFrame {
    tag: u8,
    seed_len: U64,
}

/// Fixed-size StreamStart frame: stream resume point plus the segment
/// header identity a fresh replica should create its journal with.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct StreamStartFrame {
    tag: u8,
    start_sequence: U64,
    segment_start_sequence: U64,
    anchor_hash: [u8; 32],
    epoch: U64,
    /// 1 when `genesis_entries` is known, 0 when it is not. A flag byte
    /// rather than a sentinel value, so "unknown" can never be read as a
    /// length; any other value is refused.
    genesis_known: u8,
    /// The lineage's genesis length; 0 when `genesis_known` is 0.
    genesis_entries: U64,
}

/// Shared layout for the two `(sequence, hash)` stream-control frames:
/// `Rotate` (tag 0x16, hash = outgoing segment's tail / new anchor) and
/// `ChainCheck` (tag 0x17, hash = chain value at `sequence`).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SeqHashFrame {
    tag: u8,
    sequence: U64,
    hash: [u8; 32],
}

/// A [`SeqHashFrame`] behind its length prefix: the whole frame as it goes
/// on the wire, so its encoders return a fixed-size array (see
/// [`SEQ_HASH_FRAME_LEN`]) rather than extend a `Vec`.
#[derive(IntoBytes, Immutable, Unaligned)]
#[repr(C)]
struct PrefixedSeqHashFrame {
    len: U32,
    frame: SeqHashFrame,
}

/// Wire length, length prefix included, of a `Rotate` or `ChainCheck`
/// frame, the two `(sequence, hash)` frames: what [`encode_rotate`] and
/// [`encode_chain_check`] return. In the type rather than a `Vec`, so a
/// frame of any other length does not compile, and the journal stage
/// publishes one from the stack.
pub const SEQ_HASH_FRAME_LEN: usize = core::mem::size_of::<PrefixedSeqHashFrame>();

const _: () = assert!(core::mem::size_of::<HandshakeFrame>() == 52);
const _: () = assert!(core::mem::size_of::<AckFrame>() == 18);
const _: () = assert!(HALT_FRAME_LEN == 5);
const _: () = assert!(core::mem::size_of::<ChallengeFrame>() == 33);
const _: () = assert!(core::mem::size_of::<ChallengeResponseFrame>() == 97);
const _: () = assert!(core::mem::size_of::<VersionedChallengeResponseFrame>() == 99);
const _: () = assert!(core::mem::size_of::<VersionFrame>() == 3);
const _: () = assert!(core::mem::size_of::<SnapshotBeginFrame>() == 49);
const _: () = assert!(core::mem::size_of::<SnapshotEndFrame>() == 5);
const _: () = assert!(core::mem::size_of::<HeartbeatFrame>() == 9);
const _: () = assert!(core::mem::size_of::<StreamStartFrame>() == 66);
const _: () = assert!(core::mem::size_of::<SeqHashFrame>() == 41);
const _: () = assert!(SEQ_HASH_FRAME_LEN == 45);
const _: () = assert!(core::mem::size_of::<SegmentSeedBeginFrame>() == 9);

/// Helper: `length_prefix(buf, payload_len)` writes the 4-byte LE
/// frame length prefix for a payload of `payload_len` bytes.
#[inline]
fn write_length_prefix(buf: &mut Vec<u8>, payload_len: u32) {
    buf.extend_from_slice(&payload_len.to_le_bytes());
}

// --- Encoders ---

/// Encode a handshake message into a frame (length-prefixed).
pub fn encode_handshake(h: &Handshake, buf: &mut Vec<u8>) {
    encode_handshake_flagged(h, 0, buf);
}

/// Encode a stopped replica's handshake: [`encode_handshake`]'s frame
/// flagged [`FLAG_STOPPED`].
pub fn encode_stopped_handshake(h: &Handshake, buf: &mut Vec<u8>) {
    encode_handshake_flagged(h, FLAG_STOPPED, buf);
}

fn encode_handshake_flagged(h: &Handshake, flags: u8, buf: &mut Vec<u8>) {
    let frame = HandshakeFrame {
        tag: MSG_HANDSHAKE,
        last_sequence: U64::new(h.last_sequence),
        chain_hash: h.chain_hash,
        epoch: U64::new(h.epoch),
        protocol_version: U16::new(REPL_PROTOCOL_VERSION),
        flags,
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode an ack message into a frame.
pub fn encode_ack(ack: &Ack, buf: &mut Vec<u8>) {
    encode_ack_flagged(ack, 0, buf);
}

/// Encode a stopped replica's ack: [`encode_ack`]'s frame flagged
/// [`FLAG_STOPPED`], its positions the last ones the replica sent.
pub fn encode_stopped_ack(ack: &Ack, buf: &mut Vec<u8>) {
    encode_ack_flagged(ack, FLAG_STOPPED, buf);
}

fn encode_ack_flagged(ack: &Ack, flags: u8, buf: &mut Vec<u8>) {
    let frame = AckFrame {
        tag: MSG_ACK,
        acked_sequence: U64::new(ack.acked_sequence),
        in_memory_sequence: U64::new(ack.in_memory_sequence),
        flags,
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode the `Halt` frame, length prefix included: this primary has
/// stopped because the cluster's output diverged.
pub fn encode_halt() -> [u8; HALT_FRAME_LEN] {
    zerocopy::transmute!(PrefixedHaltFrame {
        len: U32::new(1),
        tag: MSG_HALT,
    })
}

/// Encode a Challenge message (primary → replica).
pub fn encode_challenge(nonce: &[u8; 32], buf: &mut Vec<u8>) {
    let frame = ChallengeFrame {
        tag: MSG_CHALLENGE,
        nonce: *nonce,
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

fn challenge_response_frame(signature: &[u8; 64], pubkey: &[u8; 32]) -> ChallengeResponseFrame {
    ChallengeResponseFrame {
        tag: MSG_CHALLENGE_RESPONSE,
        signature: *signature,
        pubkey: *pubkey,
    }
}

/// Encode a ChallengeResponse message (replica → primary), stating this
/// node's [`REPL_PROTOCOL_VERSION`].
pub fn encode_challenge_response(signature: &[u8; 64], pubkey: &[u8; 32], buf: &mut Vec<u8>) {
    let frame = VersionedChallengeResponseFrame {
        response: challenge_response_frame(signature, pubkey),
        protocol_version: U16::new(REPL_PROTOCOL_VERSION),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a ChallengeResponse stating no version, for a control-plane
/// (raft) link: it exchanges its own protocol version after
/// authentication, which the replication protocol's version has no
/// bearing on.
pub fn encode_unversioned_challenge_response(
    signature: &[u8; 64],
    pubkey: &[u8; 32],
    buf: &mut Vec<u8>,
) {
    let frame = challenge_response_frame(signature, pubkey);
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

fn encode_version_frame(tag: u8, buf: &mut Vec<u8>) {
    let frame = VersionFrame {
        tag,
        protocol_version: U16::new(REPL_PROTOCOL_VERSION),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode an AuthOk message (primary → replica), naming the session's
/// version, which is this primary's own.
pub fn encode_auth_ok(buf: &mut Vec<u8>) {
    encode_version_frame(MSG_AUTH_OK, buf);
}

/// Encode an AuthOk naming no version, for a control-plane (raft) link
/// (see [`encode_unversioned_challenge_response`]).
pub fn encode_unversioned_auth_ok(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_OK);
}

/// Encode a VersionRefused message (primary → replica), in place of
/// AuthOk to a replica older than this primary, naming this primary's
/// version.
pub fn encode_version_refused(buf: &mut Vec<u8>) {
    encode_version_frame(MSG_VERSION_REFUSED, buf);
}

/// Encode an AuthFailed message (primary → replica).
pub fn encode_auth_failed(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_AUTH_FAILED);
}

/// Encode a StreamStart message into a frame.
///
/// `segment_start_sequence` and `anchor_hash` identify the journal
/// segment a fresh replica should create before consuming the stream —
/// with the same header identity and the same entry bytes, the
/// replica's segment is byte-identical to the primary's, and rotation
/// being primary-driven (`Rotate` frames) keeps every later segment
/// boundary aligned too: a healthy replica's journal is a bitwise
/// mirror. `genesis_entries` is the lineage's genesis length from the
/// same header (see [`PrimaryMessage::StreamStart`]).
pub fn encode_stream_start(
    start_sequence: u64,
    segment_start_sequence: u64,
    anchor_hash: [u8; 32],
    genesis_entries: Option<u64>,
    epoch: u64,
    buf: &mut Vec<u8>,
) {
    let frame = StreamStartFrame {
        tag: MSG_STREAM_START,
        start_sequence: U64::new(start_sequence),
        segment_start_sequence: U64::new(segment_start_sequence),
        anchor_hash,
        epoch: U64::new(epoch),
        genesis_known: u8::from(genesis_entries.is_some()),
        genesis_entries: U64::new(genesis_entries.unwrap_or(0)),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a NeedSnapshot message.
pub fn encode_need_snapshot(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_NEED_SNAPSHOT);
}

/// Encode a SnapshotBegin message.
pub fn encode_snapshot_begin(
    snapshot_len: u64,
    snap_sequence: u64,
    snap_chain_hash: &[u8; 32],
    buf: &mut Vec<u8>,
) {
    let frame = SnapshotBeginFrame {
        tag: MSG_SNAPSHOT_BEGIN,
        snapshot_len: U64::new(snapshot_len),
        snap_sequence: U64::new(snap_sequence),
        snap_chain_hash: *snap_chain_hash,
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a SnapshotChunk message.
pub fn encode_snapshot_chunk(data: &[u8], buf: &mut Vec<u8>) {
    // type(1) + data
    let payload_len: u32 = (1 + data.len()) as u32;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_SNAPSHOT_CHUNK);
    buf.extend_from_slice(data);
}

/// Encode a SnapshotEnd message.
pub fn encode_snapshot_end(crc32c: u32, buf: &mut Vec<u8>) {
    let frame = SnapshotEndFrame {
        tag: MSG_SNAPSHOT_END,
        crc32c: U32::new(crc32c),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a HashMismatch message.
pub fn encode_hash_mismatch(buf: &mut Vec<u8>) {
    let payload_len: u32 = 1;
    buf.extend_from_slice(&payload_len.to_le_bytes());
    buf.push(MSG_HASH_MISMATCH);
}

/// Encode a Heartbeat message: the last-sent sequence.
pub fn encode_heartbeat(sequence: u64, buf: &mut Vec<u8>) {
    let frame = HeartbeatFrame {
        tag: MSG_HEARTBEAT,
        sequence: U64::new(sequence),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// One `(sequence, hash)` frame, `tag` naming which, length prefix
/// included.
fn encode_seq_hash(tag: u8, sequence: u64, hash: &[u8; 32]) -> [u8; SEQ_HASH_FRAME_LEN] {
    let prefixed = PrefixedSeqHashFrame {
        len: U32::new(core::mem::size_of::<SeqHashFrame>() as u32),
        frame: SeqHashFrame {
            tag,
            sequence: U64::new(sequence),
            hash: *hash,
        },
    };
    // Sizes checked at compile time: the array is exactly the frame.
    zerocopy::transmute!(prefixed)
}

/// Encode a Rotate message, length prefix included. `boundary_seq` is the
/// last sequence of the outgoing segment; `tail_hash` its tail chain value
/// (= the new segment's header anchor). Must be placed in the stream after
/// the frame carrying `boundary_seq` and before any later entry.
pub fn encode_rotate(boundary_seq: u64, tail_hash: &[u8; 32]) -> [u8; SEQ_HASH_FRAME_LEN] {
    encode_seq_hash(MSG_ROTATE, boundary_seq, tail_hash)
}

/// Encode a SegmentSeedBegin message. The seed body follows as
/// `SnapshotChunk` frames terminated by a `SnapshotEnd` with the seed's
/// CRC32C.
pub fn encode_segment_seed_begin(seed_len: u64, buf: &mut Vec<u8>) {
    let frame = SegmentSeedBeginFrame {
        tag: MSG_SEGMENT_SEED_BEGIN,
        seed_len: U64::new(seed_len),
    };
    let payload = frame.as_bytes();
    write_length_prefix(buf, payload.len() as u32);
    buf.extend_from_slice(payload);
}

/// Encode a ChainCheck message, length prefix included: the primary's
/// chain value at `sequence`.
pub fn encode_chain_check(sequence: u64, chain_hash: &[u8; 32]) -> [u8; SEQ_HASH_FRAME_LEN] {
    encode_seq_hash(MSG_CHAIN_CHECK, sequence, chain_hash)
}

// --- Decoders / framing ---

/// Read a length-prefixed frame from a stream. Returns the payload (without length prefix).
pub fn read_frame(reader: &mut impl Read, max_size: usize) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > max_size {
        return Err(io::Error::other(format!(
            "frame too large: {len} > {max_size}"
        )));
    }
    if len == 0 {
        return Err(io::Error::other("empty frame"));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Decode a Challenge payload → 32-byte nonce.
pub fn decode_challenge(payload: &[u8]) -> io::Result<[u8; 32]> {
    let (frame, _) = ChallengeFrame::ref_from_prefix(payload)
        .map_err(|_| io::Error::other("challenge too short"))?;
    if frame.tag != MSG_CHALLENGE {
        return Err(io::Error::other(format!(
            "expected Challenge (0x{:02x}), got 0x{:02x}",
            MSG_CHALLENGE, frame.tag
        )));
    }
    Ok(frame.nonce)
}

/// Decode a ChallengeResponse payload, in either form: the unversioned one
/// (a v8 replica, or a control-plane link) or the replication one, which
/// states a version. A length between the two is refused, never read as
/// either.
pub fn decode_challenge_response(payload: &[u8]) -> io::Result<ChallengeResponse> {
    let (frame, rest) = ChallengeResponseFrame::ref_from_prefix(payload)
        .map_err(|_| io::Error::other("challenge response too short"))?;
    if frame.tag != MSG_CHALLENGE_RESPONSE {
        return Err(io::Error::other(format!(
            "expected ChallengeResponse (0x{:02x}), got 0x{:02x}",
            MSG_CHALLENGE_RESPONSE, frame.tag
        )));
    }
    let protocol_version = if rest.is_empty() {
        None
    } else {
        let (version, _) = U16::ref_from_prefix(rest)
            .map_err(|_| io::Error::other("challenge response's version truncated"))?;
        Some(version.get())
    };
    Ok(ChallengeResponse {
        signature: frame.signature,
        pubkey: frame.pubkey,
        protocol_version,
    })
}

/// Decode an auth result payload: AuthOk (naming the session's version,
/// or, from a v8 primary or a control-plane peer, none), AuthFailed, or
/// VersionRefused.
pub fn decode_auth_result(payload: &[u8]) -> io::Result<AuthResult> {
    let Some(&tag) = payload.first() else {
        return Err(io::Error::other("empty auth result"));
    };
    let stated_version = || {
        VersionFrame::ref_from_prefix(payload)
            .map(|(frame, _)| frame.protocol_version.get())
            .map_err(|_| io::Error::other("auth result's version truncated"))
    };
    match tag {
        MSG_AUTH_OK if payload.len() == 1 => Ok(AuthResult::Ok {
            protocol_version: None,
        }),
        MSG_AUTH_OK => Ok(AuthResult::Ok {
            protocol_version: Some(stated_version()?),
        }),
        MSG_AUTH_FAILED => Ok(AuthResult::Failed),
        MSG_VERSION_REFUSED => Ok(AuthResult::VersionRefused {
            protocol_version: stated_version()?,
        }),
        other => Err(io::Error::other(format!(
            "expected AuthOk/AuthFailed/VersionRefused, got 0x{other:02x}"
        ))),
    }
}

/// Decode a replica message from a frame payload.
pub fn decode_replica_message(payload: &[u8]) -> io::Result<ReplicaMessage> {
    if payload.is_empty() {
        return Err(io::Error::other("empty payload"));
    }
    match payload[0] {
        MSG_HANDSHAKE => {
            let (frame, _) = HandshakeFrame::ref_from_prefix(payload).map_err(|_| {
                io::Error::other(format!(
                    "handshake frame too short ({} bytes, expected {}) — the replica is \
                     likely running an older, incompatible replication protocol version; \
                     upgrade it to this binary's version",
                    payload.len(),
                    core::mem::size_of::<HandshakeFrame>()
                ))
            })?;
            let peer_version = frame.protocol_version.get();
            if peer_version != REPL_PROTOCOL_VERSION {
                return Err(io::Error::other(format!(
                    "replica speaks replication protocol v{peer_version}, this primary \
                     speaks v{REPL_PROTOCOL_VERSION} — upgrade both nodes to the same version"
                )));
            }
            let handshake = Handshake {
                last_sequence: frame.last_sequence.get(),
                chain_hash: frame.chain_hash,
                epoch: frame.epoch.get(),
            };
            Ok(match frame.flags {
                0 => ReplicaMessage::Handshake(handshake),
                FLAG_STOPPED => ReplicaMessage::StoppedHandshake(handshake),
                flags => ReplicaMessage::UnknownFlags {
                    tag: MSG_HANDSHAKE,
                    flags,
                },
            })
        }
        MSG_ACK => {
            let (frame, _) = AckFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("ack too short"))?;
            let ack = Ack {
                acked_sequence: frame.acked_sequence.get(),
                in_memory_sequence: frame.in_memory_sequence.get(),
            };
            Ok(match frame.flags {
                0 => ReplicaMessage::Ack(ack),
                FLAG_STOPPED => ReplicaMessage::StoppedAck(ack),
                flags => ReplicaMessage::UnknownFlags {
                    tag: MSG_ACK,
                    flags,
                },
            })
        }
        other => Err(io::Error::other(format!(
            "unknown replica message type: 0x{other:02x}"
        ))),
    }
}

/// Decode a primary message from a frame payload.
pub fn decode_primary_message(payload: &[u8]) -> io::Result<PrimaryMessage> {
    if payload.is_empty() {
        return Err(io::Error::other("empty payload"));
    }
    match payload[0] {
        MSG_STREAM_START => {
            let (frame, _) = StreamStartFrame::ref_from_prefix(payload).map_err(|_| {
                io::Error::other(format!(
                    "StreamStart frame too short ({} bytes, expected {}) — the primary is \
                     likely running an older, incompatible replication protocol version; \
                     upgrade it to this binary's version",
                    payload.len(),
                    core::mem::size_of::<StreamStartFrame>()
                ))
            })?;
            let genesis_entries = match frame.genesis_known {
                0 => None,
                1 => Some(frame.genesis_entries.get()),
                other => {
                    return Err(io::Error::other(format!(
                        "StreamStart genesis flag {other} is neither 0 nor 1"
                    )));
                }
            };
            Ok(PrimaryMessage::StreamStart {
                start_sequence: frame.start_sequence.get(),
                segment_start_sequence: frame.segment_start_sequence.get(),
                anchor_hash: frame.anchor_hash,
                genesis_entries,
                epoch: frame.epoch.get(),
            })
        }
        MSG_NEED_SNAPSHOT => Ok(PrimaryMessage::NeedSnapshot),
        MSG_HASH_MISMATCH => Ok(PrimaryMessage::HashMismatch),
        MSG_SNAPSHOT_BEGIN => {
            let (frame, _) = SnapshotBeginFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("SnapshotBegin too short"))?;
            Ok(PrimaryMessage::SnapshotBegin {
                snapshot_len: frame.snapshot_len.get(),
                snap_sequence: frame.snap_sequence.get(),
                snap_chain_hash: frame.snap_chain_hash,
            })
        }
        MSG_SNAPSHOT_CHUNK => Ok(PrimaryMessage::SnapshotChunk(payload[1..].to_vec())),
        MSG_SNAPSHOT_END => {
            let (frame, _) = SnapshotEndFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("SnapshotEnd too short"))?;
            Ok(PrimaryMessage::SnapshotEnd {
                crc32c: frame.crc32c.get(),
            })
        }
        MSG_HEARTBEAT => {
            let (frame, _) = HeartbeatFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("Heartbeat too short"))?;
            Ok(PrimaryMessage::Heartbeat {
                sequence: frame.sequence.get(),
            })
        }
        MSG_ROTATE => {
            let (frame, _) = SeqHashFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("Rotate too short"))?;
            Ok(PrimaryMessage::Rotate {
                boundary_seq: frame.sequence.get(),
                tail_hash: frame.hash,
            })
        }
        MSG_CHAIN_CHECK => {
            let (frame, _) = SeqHashFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("ChainCheck too short"))?;
            Ok(PrimaryMessage::ChainCheck {
                sequence: frame.sequence.get(),
                chain_hash: frame.hash,
            })
        }
        MSG_SEGMENT_SEED_BEGIN => {
            let (frame, _) = SegmentSeedBeginFrame::ref_from_prefix(payload)
                .map_err(|_| io::Error::other("SegmentSeedBegin too short"))?;
            Ok(PrimaryMessage::SegmentSeedBegin {
                seed_len: frame.seed_len.get(),
            })
        }
        MSG_HALT => Ok(PrimaryMessage::Halt),
        other => Err(io::Error::other(format!(
            "unknown primary message type: 0x{other:02x}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stream_start(genesis_entries: Option<u64>) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_stream_start(9, 1, [0x77; 32], genesis_entries, 4, &mut buf);
        buf
    }

    /// The genesis length rides `StreamStart` — known, zero (known: no
    /// genesis) and unknown each arrive as sent.
    #[test]
    fn stream_start_carries_the_genesis_length() {
        for sent in [Some(0), Some(5), Some(u64::MAX), None] {
            let frame = stream_start(sent);
            match decode_primary_message(&frame[4..]).unwrap() {
                PrimaryMessage::StreamStart {
                    start_sequence,
                    segment_start_sequence,
                    anchor_hash,
                    genesis_entries,
                    epoch,
                } => {
                    assert_eq!(genesis_entries, sent);
                    assert_eq!(
                        (start_sequence, segment_start_sequence, anchor_hash),
                        (9, 1, [0x77; 32])
                    );
                    assert_eq!(epoch, 4);
                }
                other => panic!("expected StreamStart, got {other:?}"),
            }
        }
    }

    /// The flag byte is either 0 or 1: anything else is a malformed frame,
    /// never a guess at whether the length is known.
    #[test]
    fn a_stream_start_with_a_bad_genesis_flag_is_refused() {
        let mut frame = stream_start(Some(3));
        let flag_at = 4 + core::mem::size_of::<StreamStartFrame>() - 9;
        assert_eq!(frame[flag_at], 1);
        frame[flag_at] = 2;
        let err = decode_primary_message(&frame[4..]).expect_err("flag 2 is refused");
        assert!(err.to_string().contains("genesis flag"), "{err}");
    }

    /// A v6 primary's `StreamStart` lacks the genesis length: refused as
    /// too short, with the upgrade advice, rather than read as unknown.
    #[test]
    fn a_v6_stream_start_is_refused() {
        let frame = stream_start(Some(3));
        let v6_len = core::mem::size_of::<StreamStartFrame>() - 9;
        let err = decode_primary_message(&frame[4..4 + v6_len]).expect_err("short frame");
        assert!(
            err.to_string()
                .contains("incompatible replication protocol"),
            "{err}"
        );
    }

    /// The primary refuses a handshake at another protocol version than
    /// the session's, naming both.
    #[test]
    fn a_handshake_from_another_protocol_version_is_refused() {
        let mut buf = Vec::new();
        encode_handshake(
            &Handshake {
                last_sequence: 3,
                chain_hash: [1; 32],
                epoch: 0,
            },
            &mut buf,
        );
        assert!(matches!(
            decode_replica_message(&buf[4..]).unwrap(),
            ReplicaMessage::Handshake(_)
        ));
        assert_eq!(REPL_PROTOCOL_VERSION, 9);

        // The version is the last field but the flag byte.
        let version_at = buf.len() - 3;
        buf[version_at..version_at + 2].copy_from_slice(&8u16.to_le_bytes());
        let err = decode_replica_message(&buf[4..]).expect_err("v8 replica");
        let msg = err.to_string();
        assert!(msg.contains("v8") && msg.contains("v9"), "{msg}");
    }

    fn payload(encode: impl FnOnce(&mut Vec<u8>)) -> Vec<u8> {
        let mut buf = Vec::new();
        encode(&mut buf);
        let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
        assert_eq!(len, buf.len() - 4, "the length prefix covers the payload");
        buf.split_off(4)
    }

    /// A replica's response states its version, after the 97 bytes a v8
    /// primary reads by prefix.
    #[test]
    fn a_replica_challenge_response_states_its_version() {
        let sent = payload(|buf| encode_challenge_response(&[0xAA; 64], &[0xBB; 32], buf));
        assert_eq!(sent.len(), 99);
        assert_eq!(&sent[97..], &REPL_PROTOCOL_VERSION.to_le_bytes());
        assert_eq!(
            decode_challenge_response(&sent).unwrap(),
            ChallengeResponse {
                signature: [0xAA; 64],
                pubkey: [0xBB; 32],
                protocol_version: Some(REPL_PROTOCOL_VERSION),
            }
        );
    }

    /// The unversioned form, a raft link's and a v8 replica's, is the
    /// versioned one's prefix, and decodes as stating none.
    #[test]
    fn an_unversioned_challenge_response_states_none() {
        let sent =
            payload(|buf| encode_unversioned_challenge_response(&[0xAA; 64], &[0xBB; 32], buf));
        let versioned = payload(|buf| encode_challenge_response(&[0xAA; 64], &[0xBB; 32], buf));
        assert_eq!(sent, versioned[..97]);
        let decoded = decode_challenge_response(&sent).unwrap();
        assert_eq!(decoded.protocol_version, None);
        assert_eq!(
            (decoded.signature, decoded.pubkey),
            ([0xAA; 64], [0xBB; 32])
        );
    }

    /// A response too short for its signature and key, or with a single
    /// byte where a version would be, is refused rather than guessed at.
    #[test]
    fn a_truncated_challenge_response_is_refused() {
        let sent = payload(|buf| encode_challenge_response(&[0xAA; 64], &[0xBB; 32], buf));
        for len in [10, 96, 98] {
            assert!(
                decode_challenge_response(&sent[..len]).is_err(),
                "{len} bytes"
            );
        }
    }

    /// Every auth answer decodes to what was sent, the replication ones
    /// with this primary's version.
    #[test]
    fn auth_results_round_trip() {
        let ours = Some(REPL_PROTOCOL_VERSION);
        for (sent, expected) in [
            (
                payload(encode_auth_ok),
                AuthResult::Ok {
                    protocol_version: ours,
                },
            ),
            (
                payload(encode_unversioned_auth_ok),
                AuthResult::Ok {
                    protocol_version: None,
                },
            ),
            (payload(encode_auth_failed), AuthResult::Failed),
            (
                payload(encode_version_refused),
                AuthResult::VersionRefused {
                    protocol_version: REPL_PROTOCOL_VERSION,
                },
            ),
        ] {
            assert_eq!(decode_auth_result(&sent).unwrap(), expected);
        }
    }

    /// A version cut short is a malformed answer, not a v8 primary's.
    #[test]
    fn a_truncated_auth_result_is_refused() {
        for sent in [payload(encode_auth_ok), payload(encode_version_refused)] {
            assert!(decode_auth_result(&sent[..2]).is_err());
        }
        assert!(decode_auth_result(&[]).is_err());
        assert!(decode_auth_result(&[0x42]).is_err());
    }

    /// A primary admits a replica at or above its own version, and refuses
    /// an older one, including one that states none.
    #[test]
    fn a_primary_admits_replicas_at_or_above_its_version() {
        assert!(admits_replica_version(Some(REPL_PROTOCOL_VERSION)));
        assert!(admits_replica_version(Some(REPL_PROTOCOL_VERSION + 1)));
        assert!(admits_replica_version(Some(u16::MAX)));
        assert!(!admits_replica_version(Some(REPL_PROTOCOL_VERSION - 1)));
        assert!(!admits_replica_version(Some(0)));
        assert!(!admits_replica_version(None));
    }

    const ACK: Ack = Ack {
        acked_sequence: 7,
        in_memory_sequence: 9,
    };

    fn handshake() -> Handshake {
        Handshake {
            last_sequence: 3,
            chain_hash: [1; 32],
            epoch: 2,
        }
    }

    /// An ack and a handshake round-trip as ordinary or stopped, the flag
    /// the frame's last byte.
    #[test]
    fn acks_and_handshakes_carry_the_stopped_flag() {
        let ack = payload(|buf| encode_ack(&ACK, buf));
        assert_eq!(ack.len(), 18);
        assert_eq!(ack[17], 0);
        assert!(matches!(
            decode_replica_message(&ack).unwrap(),
            ReplicaMessage::Ack(Ack {
                acked_sequence: 7,
                in_memory_sequence: 9
            })
        ));
        let stopped = payload(|buf| encode_stopped_ack(&ACK, buf));
        assert_eq!(stopped[17], FLAG_STOPPED);
        assert!(matches!(
            decode_replica_message(&stopped).unwrap(),
            ReplicaMessage::StoppedAck(Ack {
                acked_sequence: 7,
                in_memory_sequence: 9
            })
        ));

        let hs = payload(|buf| encode_handshake(&handshake(), buf));
        assert_eq!(hs.len(), 52);
        assert!(matches!(
            decode_replica_message(&hs).unwrap(),
            ReplicaMessage::Handshake(h) if h.last_sequence == 3 && h.epoch == 2
        ));
        let stopped = payload(|buf| encode_stopped_handshake(&handshake(), buf));
        assert_eq!(stopped[51], FLAG_STOPPED);
        assert!(matches!(
            decode_replica_message(&stopped).unwrap(),
            ReplicaMessage::StoppedHandshake(h) if h.last_sequence == 3
        ));
    }

    /// A flag bit this build does not know is refused as such, never read
    /// as a stop or as an ordinary frame.
    #[test]
    fn unknown_flag_bits_are_refused() {
        for flags in [0b10, 0b11, 0x80] {
            let mut ack = payload(|buf| encode_ack(&ACK, buf));
            ack[17] = flags;
            assert!(matches!(
                decode_replica_message(&ack).unwrap(),
                ReplicaMessage::UnknownFlags { tag: MSG_ACK, flags: f } if f == flags
            ));
            let mut hs = payload(|buf| encode_handshake(&handshake(), buf));
            hs[51] = flags;
            assert!(matches!(
                decode_replica_message(&hs).unwrap(),
                ReplicaMessage::UnknownFlags { tag: MSG_HANDSHAKE, flags: f } if f == flags
            ));
        }
    }

    /// A v8 replica's handshake, without the flag byte, is too short.
    #[test]
    fn a_handshake_without_its_flag_byte_is_refused() {
        let hs = payload(|buf| encode_handshake(&handshake(), buf));
        let err = decode_replica_message(&hs[..51]).expect_err("too short");
        assert!(err.to_string().contains("too short"), "{err}");
    }

    /// The `Halt` frame: its length prefix and tag, nothing else.
    #[test]
    fn halt_wire_byte_pattern() {
        assert_eq!(encode_halt(), [1, 0, 0, 0, MSG_HALT]);
        assert_eq!(MSG_HALT, 0x1A);
        assert!(matches!(
            decode_primary_message(&encode_halt()[4..]).unwrap(),
            PrimaryMessage::Halt
        ));
    }
}
