//! Codec for transport-level control frames.
//!
//! Encodes/decodes [`TransportResponse`] and [`ChallengeResponse`].
//! Tag constants are public so every program that reads or writes frames
//! — the node, the client library, a gateway on its own I/O loop — shares
//! one source of truth for the wire values.
//!
//! Every frame, in either direction, is `[length: u32 LE][tag: u8][body]`.
//! The tag is the protocol's alone: [`TAG_APP`] marks a frame whose body
//! belongs to the application, and every other tag is one of the
//! protocol's own frames. The protocol never reads an application body,
//! and an application never sees the tag, so an application body may
//! start with any byte.
//!
//! A request ([`TAG_REQUEST`]) and every frame that ends a request's
//! reply (the batch terminators, `ServerBusy`, `Refused`) carry the
//! request's [`RequestId`] straight after the tag, as eight
//! little-endian bytes: the client chose it, and the node echoes it on
//! the one final frame it sends for that request.

use crate::control::{
    ChallengeResponse, MAX_REFUSAL_REASON, RefusalKind, RequestId, TransportResponse,
};
use crate::error::ProtocolError;
use crate::framing::PREFIX_LEN;

/// Length of the tag that follows every frame's length prefix, in either
/// direction.
pub const TAG_LEN: usize = 1;

/// Length of a request ID on the wire: a `u64`, little-endian, straight
/// after the tag of a request and of every frame that ends its reply.
pub const REQUEST_ID_LEN: usize = 8;

/// The client protocol this build speaks, carried in the node's auth
/// challenge. Protocol 0 is every release before request IDs (a challenge
/// with no version in it). It changes once per release that changes the
/// client wire, not once per commit. A client refuses any version other
/// than its own, before it signs anything.
pub const CLIENT_PROTOCOL_VERSION: u16 = 1;

/// An application frame: the body is the application's response, opaque
/// to the protocol. A request travels under [`TAG_REQUEST`] instead.
///
/// Not `0x00`, so a zeroed buffer on the wire is an unknown tag rather
/// than an empty application frame. Not in `0x10..`, where application
/// tags sat while applications shared this byte with the protocol (a
/// development build between releases, never a released one). And no
/// control frame's tag, so neither side can read one as the other.
pub const TAG_APP: u8 = 0x09;

pub const TAG_RESPONSE_HEARTBEAT: u8 = 0x01;
/// The end of a request's reply, backed as the ack policy in force
/// requires: `[tag][id: u64 LE]`.
pub const TAG_BATCH_END: u8 = 0x02;
/// Request `id` was shed at ingress, not applied: `[tag][id: u64 LE]`.
pub const TAG_SERVER_BUSY: u8 = 0x04;
pub const TAG_CHALLENGE: u8 = 0x05;
pub const TAG_CHALLENGE_RESPONSE: u8 = 0x06;
pub const TAG_AUTH_FAILED: u8 = 0x07;
pub const TAG_SERVER_READY: u8 = 0x08;
/// The end of a request's reply that only the primary's own disk backs:
/// sent by a node halted for want of a replica, for a request it had
/// already sequenced, once its own journal holds it. Weaker than the ack
/// policy in force, and marked so; see
/// [`TransportResponse::BatchEndDegraded`]. `[tag][id: u64 LE]`.
pub const TAG_BATCH_END_DEGRADED: u8 = 0x0A;
/// A client's request: `[tag][id: u64 LE][body]`, the body the
/// application's.
pub const TAG_REQUEST: u8 = 0x0B;
/// The application's decoder refused request `id`:
/// `[tag][id: u64 LE][kind: u8][len: u8][reason: len bytes, UTF-8]`.
pub const TAG_REFUSED: u8 = 0x0C;

/// Every control frame's tag, in either direction. An array, as the one
/// collection a `const` block can walk.
const CONTROL_TAGS: [u8; 10] = [
    TAG_RESPONSE_HEARTBEAT,
    TAG_BATCH_END,
    TAG_SERVER_BUSY,
    TAG_CHALLENGE,
    TAG_CHALLENGE_RESPONSE,
    TAG_AUTH_FAILED,
    TAG_SERVER_READY,
    TAG_BATCH_END_DEGRADED,
    TAG_REQUEST,
    TAG_REFUSED,
];

/// Tags that frames used to carry and no frame carries any more. Never
/// reused, so a peer still speaking an older protocol cannot have one of
/// its frames read as a different one. `0x03` was `EngineError`, a frame
/// no node ever sent.
pub const RETIRED_TAGS: [u8; 1] = [0x03];

/// A rule of the protocol's tag space a set of tags breaks; see
/// [`tag_rule_violation`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TagRule {
    /// The application tag is a zeroed byte or in `0x10..`.
    AppTagOutOfRange,
    /// A control or retired tag is a zeroed byte or in `0x10..`.
    OutOfRange(u8),
    /// Two entries share a tag.
    Duplicate(u8),
    /// A control or retired tag is the application tag.
    IsAppTag(u8),
}

/// The first rule of the tag space that `control` (the live control tags),
/// `retired` and `app` together break, or `None`: `app` is neither `0x00`
/// nor in `0x10..`; every control and retired tag is neither either, is
/// not `app`, and is distinct from every other. A `const fn`, so the
/// `const` block below fails the build on a tag that breaks a rule, and
/// the unit tests can call it on sets that do.
pub(crate) const fn tag_rule_violation(control: &[u8], retired: &[u8], app: u8) -> Option<TagRule> {
    if app == 0x00 || app >= 0x10 {
        return Some(TagRule::AppTagOutOfRange);
    }
    let total = control.len() + retired.len();
    let mut i = 0;
    while i < total {
        let tag = tag_at(control, retired, i);
        if tag == 0x00 || tag >= 0x10 {
            return Some(TagRule::OutOfRange(tag));
        }
        if tag == app {
            return Some(TagRule::IsAppTag(tag));
        }
        let mut j = i + 1;
        while j < total {
            if tag_at(control, retired, j) == tag {
                return Some(TagRule::Duplicate(tag));
            }
            j += 1;
        }
        i += 1;
    }
    None
}

/// Entry `i` of `control` and `retired` taken as one list.
const fn tag_at(control: &[u8], retired: &[u8], i: usize) -> u8 {
    if i < control.len() {
        control[i]
    } else {
        retired[i - control.len()]
    }
}

const _: () = assert!(
    tag_rule_violation(&CONTROL_TAGS, &RETIRED_TAGS, TAG_APP).is_none(),
    "the protocol's tags break a rule of the tag space: see `tag_rule_violation`"
);

/// The frame of a batch terminator ([`TAG_BATCH_END`] or
/// [`TAG_BATCH_END_DEGRADED`]), length prefix included: what a response
/// stage pre-encodes and writes a request's ID into
/// ([`write_terminal_id`]) before it appends the terminator.
pub const BATCH_END_FRAME_LEN: usize = PREFIX_LEN + TAG_LEN + REQUEST_ID_LEN;

/// The widest frame [`encode_transport_response`] writes, length prefix
/// included: a `Refused` with the longest reason.
pub const MAX_CONTROL_FRAME_LEN: usize =
    PREFIX_LEN + TAG_LEN + REQUEST_ID_LEN + 2 + MAX_REFUSAL_REASON;

/// Where a terminal frame's ID starts, counted from the start of its
/// payload (after the length prefix): straight after the tag.
const ID_OFFSET: usize = TAG_LEN;

/// Write `id` into a terminator frame pre-encoded by
/// [`encode_transport_response`] (`BatchEnd` or `BatchEndDegraded`, any
/// ID): the one place the ID's offset in such a frame is written, so a
/// response stage's template and the encoder cannot drift apart.
#[inline]
pub fn write_terminal_id(frame: &mut [u8; BATCH_END_FRAME_LEN], id: RequestId) {
    frame[PREFIX_LEN + ID_OFFSET..].copy_from_slice(&id.0.to_le_bytes());
}

/// The ID a reply frame's payload (after the length prefix) names, if it
/// is a frame that ends a request's reply: `BatchEnd`, `BatchEndDegraded`,
/// `ServerBusy` or `Refused`. `None` for any other tag, and for a payload
/// too short to hold the ID. Reads only the tag and the ID, so the head
/// of a frame is enough.
#[inline]
pub fn terminal_request_id(payload: &[u8]) -> Option<RequestId> {
    match payload.first()? {
        &TAG_BATCH_END | &TAG_BATCH_END_DEGRADED | &TAG_SERVER_BUSY | &TAG_REFUSED => {
            read_id(payload)
        }
        _ => None,
    }
}

/// The ID straight after the tag of `payload`, or `None` if it is too
/// short to hold one.
#[inline]
pub(crate) fn read_id(payload: &[u8]) -> Option<RequestId> {
    let id = payload.get(ID_OFFSET..ID_OFFSET + REQUEST_ID_LEN)?;
    Some(RequestId(u64::from_le_bytes(
        id.try_into().expect("a slice of the ID's length"),
    )))
}

/// `reason` cut to at most [`MAX_REFUSAL_REASON`] bytes, on a character
/// boundary, so it stays UTF-8: the one place a refusal's reason is
/// truncated.
fn truncate_reason(reason: &str) -> &str {
    if reason.len() <= MAX_REFUSAL_REASON {
        return reason;
    }
    let mut end = MAX_REFUSAL_REASON;
    // A UTF-8 character is at most four bytes, so this walks back at most
    // three; index 0 is always a boundary.
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    &reason[..end]
}

/// Encode a transport-level response into `buf`.
///
/// Returns the total bytes written including the 4-byte LE length
/// prefix. Returns `Err(Truncated)` if `buf` is too small for the
/// variant (5 bytes for tag-only variants, [`BATCH_END_FRAME_LEN`] for
/// `BatchEnd`, `BatchEndDegraded` and `ServerBusy`, 39 for `Challenge`,
/// and up to [`MAX_CONTROL_FRAME_LEN`] for `Refused`).
pub fn encode_transport_response(
    response: &TransportResponse,
    buf: &mut [u8],
) -> Result<usize, ProtocolError> {
    // What follows the tag.
    let reason = match response {
        TransportResponse::Refused { reason, .. } => truncate_reason(reason),
        _ => "",
    };
    let body_len = match response {
        TransportResponse::Heartbeat
        | TransportResponse::AuthFailed
        | TransportResponse::ServerReady => 0,
        TransportResponse::BatchEnd { .. }
        | TransportResponse::BatchEndDegraded { .. }
        | TransportResponse::ServerBusy { .. } => REQUEST_ID_LEN,
        TransportResponse::Refused { .. } => REQUEST_ID_LEN + 2 + reason.len(),
        TransportResponse::Challenge { .. } => CHALLENGE_VERSION_LEN + CHALLENGE_NONCE_LEN,
    };
    let needed = PREFIX_LEN + TAG_LEN + body_len;
    let Some(frame) = buf.get_mut(..needed) else {
        return Err(ProtocolError::Truncated);
    };
    let (tag, id) = match *response {
        TransportResponse::Heartbeat => (TAG_RESPONSE_HEARTBEAT, None),
        TransportResponse::BatchEnd { id } => (TAG_BATCH_END, Some(id)),
        TransportResponse::BatchEndDegraded { id } => (TAG_BATCH_END_DEGRADED, Some(id)),
        TransportResponse::ServerBusy { id } => (TAG_SERVER_BUSY, Some(id)),
        TransportResponse::Refused { id, .. } => (TAG_REFUSED, Some(id)),
        TransportResponse::Challenge { .. } => (TAG_CHALLENGE, None),
        TransportResponse::AuthFailed => (TAG_AUTH_FAILED, None),
        TransportResponse::ServerReady => (TAG_SERVER_READY, None),
    };
    // Lossless: at most `MAX_CONTROL_FRAME_LEN`.
    let payload_len = (TAG_LEN + body_len) as u32;
    frame[..PREFIX_LEN].copy_from_slice(&payload_len.to_le_bytes());
    frame[PREFIX_LEN] = tag;
    let mut pos = PREFIX_LEN + TAG_LEN;
    if let Some(id) = id {
        frame[pos..pos + REQUEST_ID_LEN].copy_from_slice(&id.0.to_le_bytes());
        pos += REQUEST_ID_LEN;
    }
    match *response {
        TransportResponse::Refused { kind, .. } => {
            frame[pos] = kind.to_byte();
            // Lossless: truncated to `MAX_REFUSAL_REASON`, `u8::MAX`.
            frame[pos + 1] = reason.len() as u8;
            frame[pos + 2..].copy_from_slice(reason.as_bytes());
        }
        TransportResponse::Challenge { nonce } => {
            frame[pos..pos + CHALLENGE_VERSION_LEN]
                .copy_from_slice(&CLIENT_PROTOCOL_VERSION.to_le_bytes());
            frame[pos + CHALLENGE_VERSION_LEN..].copy_from_slice(&nonce);
        }
        TransportResponse::Heartbeat
        | TransportResponse::BatchEnd { .. }
        | TransportResponse::BatchEndDegraded { .. }
        | TransportResponse::ServerBusy { .. }
        | TransportResponse::AuthFailed
        | TransportResponse::ServerReady => {}
    }
    Ok(needed)
}

/// A `Refused` frame's fields: the request's ID, the kind, and the
/// reason, borrowed from the payload. See [`decode_refused`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused<'a> {
    pub id: RequestId,
    pub kind: RefusalKind,
    pub reason: &'a str,
}

/// Why a payload under [`TAG_REFUSED`] is not a `Refused` frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadRefusal {
    /// Too short for the ID, the kind and the reason's length.
    Truncated,
    /// The kind byte names no [`RefusalKind`].
    UnknownKind(u8),
    /// The reason's length does not match the bytes the frame has after
    /// it: `declared` against `available`.
    ReasonLength { declared: usize, available: usize },
    /// The reason is not UTF-8.
    ReasonNotUtf8,
}

impl std::fmt::Display for BadRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => f.write_str("a refusal too short for its fields"),
            Self::UnknownKind(kind) => write!(f, "a refusal of unknown kind {kind:#04x}"),
            Self::ReasonLength {
                declared,
                available,
            } => write!(
                f,
                "a refusal declaring a {declared}-byte reason with {available} bytes after it"
            ),
            Self::ReasonNotUtf8 => f.write_str("a refusal whose reason is not UTF-8"),
        }
    }
}

/// Decode a `Refused` frame's payload (after the length prefix, tag
/// included), checking its length exactly: the reason's declared length
/// must be what follows it, and it must be UTF-8. The inverse of
/// [`encode_transport_response`] for that frame, kept beside it so its
/// layout has one home. A payload whose tag is not [`TAG_REFUSED`] is the
/// caller's to rule out; this reads past the tag without looking at it.
pub fn decode_refused(payload: &[u8]) -> Result<Refused<'_>, BadRefusal> {
    let id = read_id(payload).ok_or(BadRefusal::Truncated)?;
    let fields = ID_OFFSET + REQUEST_ID_LEN;
    let [kind, len, reason @ ..] = payload.get(fields..).ok_or(BadRefusal::Truncated)? else {
        return Err(BadRefusal::Truncated);
    };
    let kind = RefusalKind::from_byte(*kind).ok_or(BadRefusal::UnknownKind(*kind))?;
    let declared = usize::from(*len);
    if reason.len() != declared {
        return Err(BadRefusal::ReasonLength {
            declared,
            available: reason.len(),
        });
    }
    let reason = std::str::from_utf8(reason).map_err(|_| BadRefusal::ReasonNotUtf8)?;
    Ok(Refused { id, kind, reason })
}

// ---------------------------------------------------------------------------
// The auth challenge
// ---------------------------------------------------------------------------

/// Bytes of the version a challenge carries ahead of its nonce.
const CHALLENGE_VERSION_LEN: usize = 2;

/// Bytes of the challenge's nonce.
pub const CHALLENGE_NONCE_LEN: usize = 32;

/// A challenge's payload (after the length prefix): `[tag][version: u16
/// LE][nonce: 32]`.
pub const CHALLENGE_LEN: usize = TAG_LEN + CHALLENGE_VERSION_LEN + CHALLENGE_NONCE_LEN;

/// A protocol-0 challenge's payload, from a node of a release before
/// client protocol versions: `[tag][nonce: 32]`, no version.
pub const CHALLENGE_V0_LEN: usize = TAG_LEN + CHALLENGE_NONCE_LEN;

/// Why a node's first frame is not a challenge this build can answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BadChallenge {
    /// Not a challenge at all: another tag, or a length no protocol's
    /// challenge has. `len` and `tag` are the payload's, for the message.
    NotAChallenge { len: usize, tag: Option<u8> },
    /// A challenge from a node speaking client protocol `node`, not
    /// [`CLIENT_PROTOCOL_VERSION`]: a protocol-0 challenge (no version),
    /// or a versioned one naming another.
    Version { node: u16 },
}

/// The nonce of a node's challenge payload (after the length prefix), if
/// the node speaks [`CLIENT_PROTOCOL_VERSION`]. The one parser of the
/// challenge's layout, for a client's handshake and for tests that read
/// a node's challenge by hand.
pub fn decode_challenge(payload: &[u8]) -> Result<[u8; CHALLENGE_NONCE_LEN], BadChallenge> {
    let not_a_challenge = BadChallenge::NotAChallenge {
        len: payload.len(),
        tag: payload.first().copied(),
    };
    let Some((&TAG_CHALLENGE, rest)) = payload.split_first() else {
        return Err(not_a_challenge);
    };
    match payload.len() {
        CHALLENGE_V0_LEN => Err(BadChallenge::Version { node: 0 }),
        CHALLENGE_LEN => {
            let (version, nonce) = rest.split_at(CHALLENGE_VERSION_LEN);
            let version = u16::from_le_bytes(version.try_into().expect("two bytes"));
            if version != CLIENT_PROTOCOL_VERSION {
                return Err(BadChallenge::Version { node: version });
            }
            Ok(nonce.try_into().expect("the nonce's length"))
        }
        _ => Err(not_a_challenge),
    }
}

/// Wire size of a challenge-response frame payload:
/// tag(1) + signature(64) + public_key(32).
pub const CHALLENGE_RESPONSE_LEN: usize = TAG_LEN + 64 + 32;

/// Encode a client's auth challenge-response into `buf`, as the frame
/// payload *without* the 4-byte length prefix:
/// `[tag:u8][signature:64][pubkey:32]`.
///
/// The inverse of [`decode_challenge_response`], kept beside it so the
/// layout has one home. Returns `Err(Truncated)` if `buf` is shorter
/// than [`CHALLENGE_RESPONSE_LEN`].
pub fn encode_challenge_response(
    response: &ChallengeResponse,
    buf: &mut [u8],
) -> Result<usize, ProtocolError> {
    if buf.len() < CHALLENGE_RESPONSE_LEN {
        return Err(ProtocolError::Truncated);
    }
    buf[0] = TAG_CHALLENGE_RESPONSE;
    buf[1..65].copy_from_slice(&response.signature);
    buf[65..97].copy_from_slice(&response.public_key);
    Ok(CHALLENGE_RESPONSE_LEN)
}

/// Decode a client's auth challenge-response from a wire frame.
///
/// `buf` must contain the frame payload *after* the 4-byte length
/// prefix has been stripped: `[tag:u8][signature:64][pubkey:32]`.
///
/// Returns `Err(UnknownTag)` if the tag is not `TAG_CHALLENGE_RESPONSE`.
pub fn decode_challenge_response(buf: &[u8]) -> Result<ChallengeResponse, ProtocolError> {
    if buf.len() < CHALLENGE_RESPONSE_LEN {
        return Err(ProtocolError::Truncated);
    }

    let tag = buf[0];
    if tag != TAG_CHALLENGE_RESPONSE {
        return Err(ProtocolError::UnknownTag(tag));
    }

    let mut signature = [0u8; 64];
    signature.copy_from_slice(&buf[1..65]);
    let mut public_key = [0u8; 32];
    public_key.copy_from_slice(&buf[65..97]);

    Ok(ChallengeResponse {
        signature,
        public_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// One of each response, terminals under `id`.
    fn every_response(id: RequestId) -> [TransportResponse; 8] {
        [
            TransportResponse::Heartbeat,
            TransportResponse::BatchEnd { id },
            TransportResponse::BatchEndDegraded { id },
            TransportResponse::ServerBusy { id },
            TransportResponse::Refused {
                id,
                kind: RefusalKind::Malformed,
                reason: "short",
            },
            TransportResponse::Challenge { nonce: [0x5A; 32] },
            TransportResponse::AuthFailed,
            TransportResponse::ServerReady,
        ]
    }

    /// `response` encoded, length prefix included.
    fn encoded(response: &TransportResponse) -> Vec<u8> {
        let mut buf = [0u8; MAX_CONTROL_FRAME_LEN];
        let n = encode_transport_response(response, &mut buf).unwrap();
        buf[..n].to_vec()
    }

    #[test]
    fn tag_only_variants_are_five_bytes() {
        for variant in [
            TransportResponse::Heartbeat,
            TransportResponse::AuthFailed,
            TransportResponse::ServerReady,
        ] {
            let frame = encoded(&variant);
            assert_eq!(frame.len(), PREFIX_LEN + TAG_LEN, "{variant:?}");
            assert_eq!(u32::from_le_bytes(frame[..4].try_into().unwrap()), 1);
            assert_eq!(terminal_request_id(&frame[PREFIX_LEN..]), None);
        }
    }

    /// Each terminal carries its ID straight after the tag, and
    /// `terminal_request_id` reads it back.
    #[test]
    fn each_terminal_encodes_with_its_id_and_decodes_back() {
        let id = RequestId(0x0102_0304_0506_0708);
        for (response, tag) in [
            (TransportResponse::BatchEnd { id }, TAG_BATCH_END),
            (
                TransportResponse::BatchEndDegraded { id },
                TAG_BATCH_END_DEGRADED,
            ),
            (TransportResponse::ServerBusy { id }, TAG_SERVER_BUSY),
        ] {
            let frame = encoded(&response);
            assert_eq!(frame.len(), BATCH_END_FRAME_LEN, "{response:?}");
            assert_eq!(
                u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize,
                TAG_LEN + REQUEST_ID_LEN
            );
            assert_eq!(frame[PREFIX_LEN], tag);
            assert_eq!(&frame[PREFIX_LEN + 1..], &id.0.to_le_bytes());
            assert_eq!(terminal_request_id(&frame[PREFIX_LEN..]), Some(id));
        }
        let refused = encoded(&TransportResponse::Refused {
            id,
            kind: RefusalKind::PermissionDenied,
            reason: "no",
        });
        assert_eq!(terminal_request_id(&refused[PREFIX_LEN..]), Some(id));
        // `0` is as good an ID as any.
        let zero = encoded(&TransportResponse::BatchEnd { id: RequestId(0) });
        assert_eq!(terminal_request_id(&zero[PREFIX_LEN..]), Some(RequestId(0)));
    }

    #[test]
    fn terminal_request_id_is_none_for_other_tags_and_short_heads() {
        assert_eq!(terminal_request_id(&[]), None);
        for tag in 0..=u8::MAX {
            let head = [&[tag][..], &[7; REQUEST_ID_LEN]].concat();
            let terminal = matches!(
                tag,
                TAG_BATCH_END | TAG_BATCH_END_DEGRADED | TAG_SERVER_BUSY | TAG_REFUSED
            );
            assert_eq!(
                terminal_request_id(&head).is_some(),
                terminal,
                "tag {tag:#04x}"
            );
            // A head cut anywhere inside the ID holds no ID.
            for cut in 0..head.len() {
                assert_eq!(terminal_request_id(&head[..cut]), None, "tag {tag:#04x}");
            }
        }
    }

    /// `CONTROL_TAGS` is checked at compile time; this checks the frames
    /// as the encoder writes them, so a variant written under a tag missing
    /// from that list cannot slip past.
    #[test]
    fn every_encoded_tag_is_a_live_control_tag() {
        for variant in &every_response(RequestId(1)) {
            let frame = encoded(variant);
            assert_ne!(frame[4], TAG_APP, "{variant:?}");
            assert!(CONTROL_TAGS.contains(&frame[4]), "{variant:?}");
            assert!(!RETIRED_TAGS.contains(&frame[4]), "{variant:?}");
        }
    }

    #[test]
    fn the_tag_rules_hold_and_are_checked() {
        assert_eq!(
            tag_rule_violation(&CONTROL_TAGS, &RETIRED_TAGS, TAG_APP),
            None
        );
        assert_eq!(tag_rule_violation(&[1, 2, 3], &[4], 9), None);
        assert_eq!(
            tag_rule_violation(&[1, 2, 1], &[], 9),
            Some(TagRule::Duplicate(1))
        );
        // A live tag reusing a retired one.
        assert_eq!(
            tag_rule_violation(&[1, 3], &[3], 9),
            Some(TagRule::Duplicate(3))
        );
        assert_eq!(
            tag_rule_violation(&[1, 0x00], &[], 9),
            Some(TagRule::OutOfRange(0x00))
        );
        assert_eq!(
            tag_rule_violation(&[1], &[0x10], 9),
            Some(TagRule::OutOfRange(0x10))
        );
        assert_eq!(
            tag_rule_violation(&[1, 9], &[], 9),
            Some(TagRule::IsAppTag(9))
        );
        assert_eq!(
            tag_rule_violation(&[1], &[], 0x00),
            Some(TagRule::AppTagOutOfRange)
        );
        assert_eq!(
            tag_rule_violation(&[1], &[], 0x10),
            Some(TagRule::AppTagOutOfRange)
        );
    }

    #[test]
    fn every_refusal_kind_round_trips_and_no_other_byte_is_one() {
        for kind in [RefusalKind::PermissionDenied, RefusalKind::Malformed] {
            assert_eq!(RefusalKind::from_byte(kind.to_byte()), Some(kind));
        }
        for byte in 0..=u8::MAX {
            match RefusalKind::from_byte(byte) {
                Some(kind) => assert_eq!(kind.to_byte(), byte),
                None => assert!(!(1..=2).contains(&byte), "{byte}"),
            }
        }
        assert_eq!(RefusalKind::from_byte(0), None, "a zeroed byte is no kind");
    }

    /// A refusal's frame, decoded back.
    fn refused_round_trip(reason: &'static str) -> (Vec<u8>, String) {
        let id = RequestId(42);
        let frame = encoded(&TransportResponse::Refused {
            id,
            kind: RefusalKind::Malformed,
            reason,
        });
        let refused = decode_refused(&frame[PREFIX_LEN..]).unwrap();
        assert_eq!(refused.id, id);
        assert_eq!(refused.kind, RefusalKind::Malformed);
        let reason = refused.reason.to_string();
        (frame, reason)
    }

    #[test]
    fn a_refusal_carries_its_kind_and_reason() {
        let (frame, reason) = refused_round_trip("auditor role may not notarize");
        assert_eq!(reason, "auditor role may not notarize");
        assert_eq!(frame[PREFIX_LEN], TAG_REFUSED);
        assert_eq!(
            frame.len(),
            PREFIX_LEN + TAG_LEN + REQUEST_ID_LEN + 2 + reason.len()
        );
        let (_, empty) = refused_round_trip("");
        assert_eq!(empty, "");
    }

    /// The reason is cut at 255 bytes, back to a character boundary: at,
    /// just past and well past the limit, with a multi-byte character
    /// straddling it.
    #[test]
    fn a_refusal_reason_is_truncated_on_a_character_boundary() {
        // Exactly the limit: kept whole, the widest frame.
        let at: &'static str = "a".repeat(MAX_REFUSAL_REASON).leak();
        let (frame, reason) = refused_round_trip(at);
        assert_eq!(reason, at);
        assert_eq!(frame.len(), MAX_CONTROL_FRAME_LEN);
        assert_eq!(MAX_CONTROL_FRAME_LEN, 270);
        assert_eq!(frame.len() - PREFIX_LEN, 266, "with the tag");

        // One past: the last byte goes.
        let past: &'static str = "b".repeat(MAX_REFUSAL_REASON + 1).leak();
        assert_eq!(refused_round_trip(past).1, &past[..MAX_REFUSAL_REASON]);

        // Well past.
        let far: &'static str = "c".repeat(4 * MAX_REFUSAL_REASON).leak();
        assert_eq!(refused_round_trip(far).1.len(), MAX_REFUSAL_REASON);

        // A three-byte character over the limit: cut before it, not inside.
        for lead in MAX_REFUSAL_REASON - 3..=MAX_REFUSAL_REASON {
            let s: &'static str = format!("{}€tail", "d".repeat(lead)).leak();
            let reason = refused_round_trip(s).1;
            assert!(reason.len() <= MAX_REFUSAL_REASON, "lead {lead}");
            assert!(s.starts_with(&reason), "lead {lead}");
            let euro_fits = lead + '€'.len_utf8() <= MAX_REFUSAL_REASON;
            assert_eq!(reason.ends_with('€'), euro_fits, "lead {lead}");
            if !euro_fits {
                assert_eq!(reason.len(), lead, "lead {lead}");
            }
        }
    }

    #[test]
    fn a_malformed_refusal_is_refused() {
        let good = encoded(&TransportResponse::Refused {
            id: RequestId(1),
            kind: RefusalKind::PermissionDenied,
            reason: "abc",
        });
        let payload = &good[PREFIX_LEN..];
        assert!(decode_refused(payload).is_ok());
        // Cut before the length byte, or inside the ID.
        for cut in 0..TAG_LEN + REQUEST_ID_LEN + 2 {
            assert_eq!(decode_refused(&payload[..cut]), Err(BadRefusal::Truncated));
        }
        // One byte short of its reason, one byte long.
        assert_eq!(
            decode_refused(&payload[..payload.len() - 1]),
            Err(BadRefusal::ReasonLength {
                declared: 3,
                available: 2
            })
        );
        let long = [payload, b"x"].concat();
        assert!(matches!(
            decode_refused(&long),
            Err(BadRefusal::ReasonLength {
                declared: 3,
                available: 4
            })
        ));
        // A zero kind and an unknown one.
        let kind_at = TAG_LEN + REQUEST_ID_LEN;
        for kind in [0x00, 0x03, 0xFF] {
            let mut bad = payload.to_vec();
            bad[kind_at] = kind;
            assert_eq!(decode_refused(&bad), Err(BadRefusal::UnknownKind(kind)));
        }
        // A reason that is not UTF-8.
        let mut bad = payload.to_vec();
        bad[kind_at + 2] = 0xFF;
        assert_eq!(decode_refused(&bad), Err(BadRefusal::ReasonNotUtf8));
        // Every error says what is wrong.
        for err in [
            BadRefusal::Truncated,
            BadRefusal::UnknownKind(3),
            BadRefusal::ReasonLength {
                declared: 3,
                available: 2,
            },
            BadRefusal::ReasonNotUtf8,
        ] {
            assert!(err.to_string().contains("refusal"), "{err}");
        }
    }

    #[test]
    fn encode_refuses_a_buffer_too_small_for_the_variant() {
        for variant in every_response(RequestId(9)) {
            let len = encoded(&variant).len();
            let mut buf = vec![0u8; len - 1];
            assert!(
                matches!(
                    encode_transport_response(&variant, &mut buf),
                    Err(ProtocolError::Truncated)
                ),
                "{variant:?}"
            );
            let mut buf = vec![0u8; len];
            assert_eq!(encode_transport_response(&variant, &mut buf).unwrap(), len);
        }
    }

    #[test]
    fn the_challenge_carries_the_version_ahead_of_the_nonce() {
        let nonce = [0xAB; 32];
        let frame = encoded(&TransportResponse::Challenge { nonce });
        assert_eq!(frame.len(), PREFIX_LEN + CHALLENGE_LEN);
        assert_eq!(CHALLENGE_LEN, 35);
        assert_eq!(
            u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize,
            CHALLENGE_LEN
        );
        assert_eq!(frame[4], TAG_CHALLENGE);
        assert_eq!(&frame[5..7], &CLIENT_PROTOCOL_VERSION.to_le_bytes());
        assert_eq!(&frame[7..], &nonce);
        assert_eq!(decode_challenge(&frame[PREFIX_LEN..]), Ok(nonce));
    }

    #[test]
    fn a_challenge_from_another_protocol_names_its_version() {
        // Protocol 0: a 33-byte challenge, no version.
        let v0 = [&[TAG_CHALLENGE][..], &[0x11; 32]].concat();
        assert_eq!(v0.len(), CHALLENGE_V0_LEN);
        assert_eq!(
            decode_challenge(&v0),
            Err(BadChallenge::Version { node: 0 })
        );
        // A version this build does not speak.
        for version in [0, 2, u16::MAX] {
            let other = [&[TAG_CHALLENGE][..], &version.to_le_bytes(), &[0x11; 32]].concat();
            assert_eq!(
                decode_challenge(&other),
                Err(BadChallenge::Version { node: version })
            );
        }
        // Not a challenge: another tag, another length, nothing.
        for wrong in [
            vec![TAG_SERVER_READY],
            vec![TAG_CHALLENGE; 34],
            vec![TAG_CHALLENGE; 36],
            [&[TAG_BATCH_END][..], &[0; 34]].concat(),
        ] {
            assert_eq!(
                decode_challenge(&wrong),
                Err(BadChallenge::NotAChallenge {
                    len: wrong.len(),
                    tag: wrong.first().copied()
                })
            );
        }
        assert_eq!(
            decode_challenge(&[]),
            Err(BadChallenge::NotAChallenge { len: 0, tag: None })
        );
    }

    #[test]
    fn decode_valid_challenge_response() {
        let sig = [0x11; 64];
        let pubkey = [0x22; 32];

        // Build wire frame: [tag:u8][sig:64][pubkey:32]
        let mut buf = [0u8; 97];
        buf[0] = TAG_CHALLENGE_RESPONSE;
        buf[1..65].copy_from_slice(&sig);
        buf[65..97].copy_from_slice(&pubkey);

        let cr = decode_challenge_response(&buf).unwrap();
        assert_eq!(cr.signature, sig);
        assert_eq!(cr.public_key, pubkey);
    }

    #[test]
    fn challenge_response_round_trips() {
        let response = ChallengeResponse {
            signature: [0x33; 64],
            public_key: [0x44; 32],
        };
        let mut buf = [0u8; CHALLENGE_RESPONSE_LEN];
        let written = encode_challenge_response(&response, &mut buf).unwrap();
        assert_eq!(written, CHALLENGE_RESPONSE_LEN);
        assert_eq!(decode_challenge_response(&buf).unwrap(), response);

        let mut short = [0u8; CHALLENGE_RESPONSE_LEN - 1];
        assert!(matches!(
            encode_challenge_response(&response, &mut short),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn decode_truncated() {
        let buf = [0u8; 50]; // too short
        assert!(matches!(
            decode_challenge_response(&buf),
            Err(ProtocolError::Truncated)
        ));
    }

    #[test]
    fn decode_wrong_tag() {
        let mut buf = [0u8; CHALLENGE_RESPONSE_LEN];
        buf[0] = 99; // not TAG_CHALLENGE_RESPONSE
        assert!(matches!(
            decode_challenge_response(&buf),
            Err(ProtocolError::UnknownTag(99))
        ));
    }

    proptest! {
        /// The pre-encoded terminator with an ID written into it is the
        /// frame the encoder writes for that ID, for either terminator,
        /// so a response stage's template and the encoder cannot drift.
        #[test]
        fn the_template_writer_matches_the_encoder(id in any::<u64>(), stale in any::<u64>()) {
            let id = RequestId(id);
            for (make, name) in [
                (
                    (|id| TransportResponse::BatchEnd { id }) as fn(RequestId) -> TransportResponse,
                    "BatchEnd",
                ),
                (|id| TransportResponse::BatchEndDegraded { id }, "BatchEndDegraded"),
            ] {
                let mut template = [0u8; BATCH_END_FRAME_LEN];
                let n = encode_transport_response(&make(RequestId(stale)), &mut template).unwrap();
                prop_assert_eq!(n, BATCH_END_FRAME_LEN, "{}", name);
                write_terminal_id(&mut template, id);
                prop_assert_eq!(template.to_vec(), encoded(&make(id)), "{}", name);
            }
        }
    }
}
