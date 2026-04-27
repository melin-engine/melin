//! Per-message session-token MAC for the rumcast wire path.
//!
//! Once the Ed25519 challenge-response handshake (see [`crate::auth`]
//! and [`crate::message::ResponseKind::Challenge`]) and the X25519
//! ECDH have produced a shared secret, both ends call
//! [`derive_session_token`] to turn that secret + the challenge nonce
//! into a fixed 32-byte symmetric **session token**. The token never
//! crosses the wire — both sides derive it independently from values
//! that were either signed (the nonce, the ephemerals) or generated
//! locally (the X25519 private keys).
//!
//! On the data plane, every payload (an already-encoded
//! [`crate::message::Request`] or response) is wrapped via
//! [`encode_envelope`]:
//!
//! ```text
//! [seq:u64 LE] [mac:16] [inner_payload ...]
//! ```
//!
//! The MAC is `BLAKE3_keyed(token, session_id ‖ seq ‖ inner)`
//! truncated to 16 bytes. The receiver runs
//! [`verify_and_decode_envelope`] which:
//!
//! 1. Rejects packets shorter than the 24-byte header.
//! 2. Rejects sequence numbers that don't strictly advance past the
//!    caller's tracked `last_seq` (replay protection — UDP can
//!    deliver duplicates and reordered frames; TCP got this for
//!    free from the byte-stream guarantee).
//! 3. Recomputes the MAC and compares constant-time against the
//!    received bytes.
//!
//! Why 128-bit MAC: we're authenticating per-client unicast
//! sessions on a private LAN. 2^64 forgery attempts to find a
//! collision is comfortable; 256-bit would just waste 16 bytes per
//! packet on a hot path that already pays for the MAC compute.
//!
//! Why BLAKE3: ~80–120ns per small message on M2/modern x86 with no
//! hardware-AES dependency. Beats HMAC-SHA256 (~400ns) and
//! ChaCha20-Poly1305 (~200ns) for our payload sizes; close to
//! AES-GCM-NI (~30ns) on x86, faster on ARM where AES-NI isn't a
//! given. Single keyed mode, no separate KDF/MAC primitives to
//! manage.
//!
//! What this module does **not** do:
//! - The X25519 ECDH itself (one `x25519_dalek` call at the
//!   handshake site).
//! - Any per-session bookkeeping (token storage, replay state,
//!   permissions). The rumcast in_translator owns that table.
//! - Encryption — payloads stay in the clear. Confidentiality on a
//!   private exchange LAN comes from the LAN being private; if we
//!   ever need it, the envelope grows to ChaCha20-Poly1305 or
//!   similar.

use subtle::ConstantTimeEq;

/// Bytes prepended by [`encode_envelope`] before the inner payload:
/// `seq` (8 bytes LE) + `mac` (16 bytes truncated BLAKE3 keyed hash).
pub const ENVELOPE_OVERHEAD: usize = 8 + 16;

/// Length of the MAC bytes carried in the envelope. 128 bits is
/// plenty for unicast — 2^64 forgery attempts is well past anything
/// realistic on a LAN session before the session is rotated.
pub const MAC_LEN: usize = 16;

/// Domain-separation context for [`derive_session_token`]. Versioned
/// so the KDF schema can change without ambiguity. Bumping the
/// version (e.g. to "v2") forces a fresh derivation, preventing a
/// downgraded peer from tricking either side into reusing a token
/// across schema versions.
const KDF_CONTEXT: &str = "melin-rumcast-session-token v1";

/// Errors returned by the envelope codec. All failures are dropped
/// silently by the rumcast in_translator (no client-facing error) —
/// these variants exist so internal counters and tests can
/// distinguish the cause.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeError {
    /// Input is shorter than [`ENVELOPE_OVERHEAD`] — there isn't
    /// even room for `seq` + `mac`.
    Truncated,
    /// MAC didn't match. Either the bytes were tampered with, the
    /// sender used a different token (wrong session), or the
    /// session_id provided to verify doesn't match what the sender
    /// committed to in the MAC.
    MacInvalid,
    /// Sequence number wasn't strictly greater than `last_seq`.
    /// Either a duplicate (replay), a reorder (UDP can do that), or
    /// an attacker resending an old packet.
    Replay { seq: u64, last_seq: u64 },
    /// Output buffer too small to fit `inner` plus the
    /// [`ENVELOPE_OVERHEAD`] header.
    OutputTooSmall { needed: usize, actual: usize },
}

impl std::fmt::Display for EnvelopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(
                f,
                "envelope truncated (need at least {ENVELOPE_OVERHEAD} bytes)"
            ),
            Self::MacInvalid => write!(f, "envelope MAC verification failed"),
            Self::Replay { seq, last_seq } => {
                write!(
                    f,
                    "envelope replay: seq={seq} not greater than last_seq={last_seq}"
                )
            }
            Self::OutputTooSmall { needed, actual } => {
                write!(
                    f,
                    "envelope output buffer too small: need {needed} bytes, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for EnvelopeError {}

/// Derive the per-session symmetric MAC token from the X25519 ECDH
/// shared secret and the challenge nonce.
///
/// Both ends of the handshake compute this independently from
/// values they either generated locally (the X25519 private keys)
/// or received in a signed Challenge / ChallengeResponse — the
/// token itself never traverses the wire.
///
/// Implemented as `BLAKE3::derive_key(KDF_CONTEXT, secret ‖ nonce)`.
/// The hardcoded context string provides domain separation so the
/// same secret/nonce pair can't accidentally produce a token usable
/// in a different protocol or a future schema version.
pub fn derive_session_token(shared_secret: &[u8; 32], nonce: &[u8; 32]) -> [u8; 32] {
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(shared_secret);
    input[32..].copy_from_slice(nonce);
    blake3::derive_key(KDF_CONTEXT, &input)
}

/// Wrap `inner` in an authenticated envelope.
///
/// Layout written into `out`:
///
/// ```text
/// [0..8]    seq          (u64 LE)
/// [8..24]   mac          (16 bytes — BLAKE3 keyed truncated)
/// [24..]    inner        (caller-provided payload, copied verbatim)
/// ```
///
/// The MAC covers `session_id ‖ seq ‖ inner`. `session_id` is the
/// rumcast wire field, NOT a separate app-level identifier — both
/// ends already see it on every Data frame, so binding it into the
/// MAC ties the envelope to the specific stream and prevents an
/// attacker from cross-injecting an envelope from one session into
/// another.
///
/// **Seq numbering convention**: senders must start at `seq = 1`
/// and increment by 1 for each subsequent message in the session.
/// The receiver initializes `last_seq = 0`, so the first message
/// (`seq = 1`) passes the strict-monotonic check
/// (`seq > last_seq`). Sending `seq = 0` would be rejected as a
/// replay against the initial `last_seq = 0`.
///
/// Returns the total bytes written (`ENVELOPE_OVERHEAD + inner.len()`).
pub fn encode_envelope(
    token: &[u8; 32],
    session_id: u32,
    seq: u64,
    inner: &[u8],
    out: &mut [u8],
) -> Result<usize, EnvelopeError> {
    let needed = ENVELOPE_OVERHEAD + inner.len();
    if out.len() < needed {
        return Err(EnvelopeError::OutputTooSmall {
            needed,
            actual: out.len(),
        });
    }

    out[..8].copy_from_slice(&seq.to_le_bytes());

    // MAC the wire-bound fields. We MAC the inner bytes from the
    // caller's slice (rather than copying first, then MACing the
    // copy) to give the BLAKE3 implementation a single contiguous
    // buffer per `update` call — avoids an extra copy on the hot
    // path.
    let mac = compute_mac(token, session_id, seq, inner);
    out[8..24].copy_from_slice(&mac);

    out[24..24 + inner.len()].copy_from_slice(inner);
    Ok(needed)
}

/// Verify the envelope and return the inner payload along with the
/// authenticated sequence number.
///
/// The caller MUST pass the `last_seq` it has tracked for this
/// session and, on success, update its tracked value to the
/// returned `seq`. Without that bookkeeping, replay protection is
/// gone.
///
/// Verification order (replay check first, MAC second):
///
/// 1. Length check — reject if shorter than [`ENVELOPE_OVERHEAD`].
/// 2. Replay check — reject if `seq <= last_seq`. This is cheap
///    and catches duplicates / reorders before paying for BLAKE3.
///    `seq` is unauthenticated at this point, but tampering with
///    it invalidates the MAC, so the worst an attacker can do by
///    forging a high `seq` is force us to do the MAC compute and
///    then reject anyway.
/// 3. MAC check — constant-time compare of the recomputed MAC
///    against the received 16 bytes.
///
/// Returns `(seq, inner)` on success; `inner` is a borrowed slice
/// into `bytes`.
pub fn verify_and_decode_envelope<'a>(
    token: &[u8; 32],
    session_id: u32,
    last_seq: u64,
    bytes: &'a [u8],
) -> Result<(u64, &'a [u8]), EnvelopeError> {
    if bytes.len() < ENVELOPE_OVERHEAD {
        return Err(EnvelopeError::Truncated);
    }

    // The slice indexing above guarantees these conversions succeed
    // — fixed offsets within a length-checked range.
    let seq = u64::from_le_bytes(bytes[0..8].try_into().expect("8 bytes"));
    let received_mac: [u8; MAC_LEN] = bytes[8..24].try_into().expect("16 bytes");
    let inner = &bytes[ENVELOPE_OVERHEAD..];

    if seq <= last_seq {
        return Err(EnvelopeError::Replay { seq, last_seq });
    }

    let computed_mac = compute_mac(token, session_id, seq, inner);
    if computed_mac.ct_eq(&received_mac).unwrap_u8() != 1 {
        return Err(EnvelopeError::MacInvalid);
    }

    Ok((seq, inner))
}

/// Compute the 16-byte truncated MAC over `session_id ‖ seq ‖
/// inner` using BLAKE3 keyed mode with `token` as the key.
///
/// Internal helper — both encode and verify call this with the
/// same byte assembly so they can't drift out of sync.
fn compute_mac(token: &[u8; 32], session_id: u32, seq: u64, inner: &[u8]) -> [u8; MAC_LEN] {
    let mut hasher = blake3::Hasher::new_keyed(token);
    hasher.update(&session_id.to_le_bytes());
    hasher.update(&seq.to_le_bytes());
    hasher.update(inner);
    let hash = hasher.finalize();
    let mut mac = [0u8; MAC_LEN];
    mac.copy_from_slice(&hash.as_bytes()[..MAC_LEN]);
    mac
}

// ---------------------------------------------------------------------------
// Handshake helpers
//
// Pure-crypto pieces of the rumcast handshake, factored out so the
// bench client, the smoke test, and the server's session_translator
// all use the same byte assembly. The transport-level I/O (publish
// bytes, poll for reply) stays at the caller — keeps this module
// transport-agnostic.
// ---------------------------------------------------------------------------

/// Errors returned by [`verify_client_handshake`]. The session
/// translator turns these into a counter bump + an `AuthFailed`
/// reply on the wire — clients never see the structured variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeError {
    /// Client's `public_key` bytes don't decode as a valid Ed25519
    /// VerifyingKey (e.g. low-order point, malformed encoding).
    BadClientPublicKey,
    /// Ed25519 signature didn't verify against the expected payload
    /// (`nonce ‖ server_eph ‖ client_eph`). Either tampering, key
    /// mismatch, or a signing-payload drift between peers.
    SignatureInvalid,
}

impl std::fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadClientPublicKey => write!(f, "client Ed25519 public key is malformed"),
            Self::SignatureInvalid => write!(f, "client Ed25519 signature did not verify"),
        }
    }
}

impl std::error::Error for HandshakeError {}

/// Output of a successful [`ClientHandshake::finish`]. Carries the
/// `Request::ChallengeResponse` the caller should send and the
/// per-session MAC token both sides have now derived (independently —
/// the token never crosses the wire).
pub struct CompletedHandshake {
    pub challenge_response: crate::message::Request,
    pub session_token: [u8; 32],
}

/// Client-side handshake helper. Wraps the long-term Ed25519
/// identity + a fresh ephemeral X25519 keypair, then completes the
/// handshake when the server's Challenge arrives.
///
/// Holds the ephemeral X25519 secret in a `StaticSecret` (which
/// `zeroize-on-drop`s when the helper is consumed by `finish`),
/// so the secret bytes are wiped from RAM as soon as the shared
/// secret has been derived.
pub struct ClientHandshake<'a> {
    signing_key: &'a ed25519_dalek::SigningKey,
    x25519_secret: x25519_dalek::StaticSecret,
    x25519_public: [u8; 32],
}

impl<'a> ClientHandshake<'a> {
    /// Construct a handshake bound to `signing_key` (the client's
    /// long-term Ed25519 identity) using `x25519_secret_bytes` as
    /// the X25519 ephemeral secret.
    ///
    /// The caller is responsible for sourcing
    /// `x25519_secret_bytes` from a cryptographic RNG (e.g.
    /// `getrandom::fill`). Keeping the source explicit lets the
    /// helper stay free of an `OsRng` dep and keeps tests
    /// deterministic.
    pub fn new(signing_key: &'a ed25519_dalek::SigningKey, x25519_secret_bytes: [u8; 32]) -> Self {
        let secret = x25519_dalek::StaticSecret::from(x25519_secret_bytes);
        let public = x25519_dalek::PublicKey::from(&secret).to_bytes();
        Self {
            signing_key,
            x25519_secret: secret,
            x25519_public: public,
        }
    }

    /// Client's long-term Ed25519 public key. Useful when the
    /// caller needs to write it into an `authorized_keys` file
    /// before connecting (e.g. test setup).
    pub fn ed25519_public_key(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// Process the server's Challenge frame contents. Consumes
    /// `self` so the ephemeral X25519 secret is dropped (and
    /// zeroed) immediately after the shared secret has been
    /// computed.
    pub fn finish(
        self,
        server_nonce: &[u8; 32],
        server_x25519_eph: &[u8; 32],
    ) -> CompletedHandshake {
        use ed25519_dalek::Signer;

        let signing_payload =
            crate::auth::auth_signing_payload(server_nonce, server_x25519_eph, &self.x25519_public);
        let signature = self.signing_key.sign(&signing_payload);
        let public_key = self.signing_key.verifying_key().to_bytes();

        let server_pub = x25519_dalek::PublicKey::from(*server_x25519_eph);
        let shared = self.x25519_secret.diffie_hellman(&server_pub);
        let token = derive_session_token(shared.as_bytes(), server_nonce);

        CompletedHandshake {
            challenge_response: crate::message::Request::ChallengeResponse {
                signature: signature.to_bytes(),
                public_key,
                client_x25519_eph: self.x25519_public,
            },
            session_token: token,
        }
    }
}

/// Server-side handshake completion. Verifies the client's Ed25519
/// signature over `nonce ‖ server_eph ‖ client_eph` and derives the
/// same session token the client computed.
///
/// The caller is responsible for `authorized_keys` lookup BEFORE
/// invoking this — `client_pubkey` is treated as already-trusted
/// here. The split keeps key-source policy (file lookup, ACL,
/// future RBAC) out of the crypto layer.
pub fn verify_client_handshake(
    nonce: &[u8; 32],
    server_x25519_eph: &[u8; 32],
    server_x25519_secret: &x25519_dalek::StaticSecret,
    client_pubkey: &[u8; 32],
    client_x25519_eph: &[u8; 32],
    signature: &[u8; 64],
) -> Result<[u8; 32], HandshakeError> {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

    let signing_payload =
        crate::auth::auth_signing_payload(nonce, server_x25519_eph, client_x25519_eph);
    let vk =
        VerifyingKey::from_bytes(client_pubkey).map_err(|_| HandshakeError::BadClientPublicKey)?;
    let sig = Signature::from_bytes(signature);
    vk.verify(&signing_payload, &sig)
        .map_err(|_| HandshakeError::SignatureInvalid)?;

    let client_pub = x25519_dalek::PublicKey::from(*client_x25519_eph);
    let shared = server_x25519_secret.diffie_hellman(&client_pub);
    Ok(derive_session_token(shared.as_bytes(), nonce))
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- derive_session_token ---

    const SECRET_A: [u8; 32] = [0x11; 32];
    const SECRET_B: [u8; 32] = [0x22; 32];
    const NONCE_A: [u8; 32] = [0xA1; 32];
    const NONCE_B: [u8; 32] = [0xA2; 32];

    #[test]
    fn derive_session_token_is_deterministic() {
        // Same inputs MUST produce the same token — both ends of
        // the handshake derive independently from agreed values.
        assert_eq!(
            derive_session_token(&SECRET_A, &NONCE_A),
            derive_session_token(&SECRET_A, &NONCE_A),
        );
    }

    #[test]
    fn derive_session_token_changes_with_nonce() {
        // Same secret, different nonce → different token. This is
        // the property that makes per-session tokens distinct even
        // when the long-term key material isn't refreshed.
        assert_ne!(
            derive_session_token(&SECRET_A, &NONCE_A),
            derive_session_token(&SECRET_A, &NONCE_B),
        );
    }

    #[test]
    fn derive_session_token_changes_with_secret() {
        // Different shared secret → different token. The whole
        // point of the X25519 ECDH.
        assert_ne!(
            derive_session_token(&SECRET_A, &NONCE_A),
            derive_session_token(&SECRET_B, &NONCE_A),
        );
    }

    #[test]
    fn derive_session_token_swapped_inputs_differ() {
        // Tokens computed from `(secret=X, nonce=Y)` and
        // `(secret=Y, nonce=X)` must NOT collide — protects against
        // a confused-deputy bug where a buffer is wired to the
        // wrong slot.
        let secret = [0x33u8; 32];
        let nonce = [0x99u8; 32];
        assert_ne!(
            derive_session_token(&secret, &nonce),
            derive_session_token(&nonce, &secret),
        );
    }

    // --- envelope round-trip ---

    fn token() -> [u8; 32] {
        derive_session_token(&SECRET_A, &NONCE_A)
    }

    #[test]
    fn envelope_round_trip_returns_original_payload() {
        let token = token();
        let inner = b"hello rumcast";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        let written = encode_envelope(&token, 0xCAFEBABE, 1, inner, &mut out).unwrap();
        assert_eq!(written, ENVELOPE_OVERHEAD + inner.len());

        let (seq, decoded) = verify_and_decode_envelope(&token, 0xCAFEBABE, 0, &out).unwrap();
        assert_eq!(seq, 1);
        assert_eq!(decoded, inner);
    }

    #[test]
    fn envelope_round_trip_empty_inner() {
        // Inner can be empty — useful for control-plane keepalives
        // that carry no payload. The MAC still authenticates the
        // (session_id, seq) pair.
        let token = token();
        let mut out = [0u8; ENVELOPE_OVERHEAD];
        let written = encode_envelope(&token, 1, 1, &[], &mut out).unwrap();
        assert_eq!(written, ENVELOPE_OVERHEAD);
        let (seq, decoded) = verify_and_decode_envelope(&token, 1, 0, &out).unwrap();
        assert_eq!(seq, 1);
        assert!(decoded.is_empty());
    }

    // --- encode error paths ---

    #[test]
    fn encode_envelope_rejects_undersized_output() {
        let token = token();
        let inner = [0u8; 50];
        let mut tiny = [0u8; 10];
        let result = encode_envelope(&token, 1, 1, &inner, &mut tiny);
        assert!(matches!(
            result,
            Err(EnvelopeError::OutputTooSmall { needed, actual })
                if needed == ENVELOPE_OVERHEAD + 50 && actual == 10
        ));
    }

    #[test]
    fn encode_envelope_accepts_exactly_sized_output() {
        // Boundary: out.len() == needed must succeed.
        let token = token();
        let inner = [0xFFu8; 17];
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + 17];
        encode_envelope(&token, 1, 1, &inner, &mut out).unwrap();
    }

    // --- verify error paths ---

    #[test]
    fn verify_envelope_rejects_truncated_input() {
        let token = token();
        // 23 bytes — one short of the header.
        let buf = [0u8; ENVELOPE_OVERHEAD - 1];
        assert_eq!(
            verify_and_decode_envelope(&token, 1, 0, &buf),
            Err(EnvelopeError::Truncated),
        );
        // Empty input.
        assert_eq!(
            verify_and_decode_envelope(&token, 1, 0, &[]),
            Err(EnvelopeError::Truncated),
        );
    }

    #[test]
    fn verify_envelope_rejects_replay_at_equal_seq() {
        let token = token();
        let inner = b"replay me";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token, 1, 5, inner, &mut out).unwrap();
        // last_seq = 5, incoming seq = 5 → not strictly greater.
        let result = verify_and_decode_envelope(&token, 1, 5, &out);
        assert_eq!(
            result,
            Err(EnvelopeError::Replay {
                seq: 5,
                last_seq: 5
            })
        );
    }

    #[test]
    fn verify_envelope_rejects_replay_at_lower_seq() {
        let token = token();
        let inner = b"old packet";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token, 1, 3, inner, &mut out).unwrap();
        let result = verify_and_decode_envelope(&token, 1, 10, &out);
        assert_eq!(
            result,
            Err(EnvelopeError::Replay {
                seq: 3,
                last_seq: 10,
            }),
        );
    }

    #[test]
    fn verify_envelope_accepts_seq_one_above_last() {
        // Boundary: seq = last_seq + 1 must be accepted.
        let token = token();
        let inner = b"next packet";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token, 1, 11, inner, &mut out).unwrap();
        let (seq, _) = verify_and_decode_envelope(&token, 1, 10, &out).unwrap();
        assert_eq!(seq, 11);
    }

    #[test]
    fn verify_envelope_rejects_mac_tamper() {
        let token = token();
        let inner = b"authentic payload";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token, 1, 1, inner, &mut out).unwrap();
        // Flip a bit in the MAC (offset 8..24).
        out[12] ^= 0x01;
        assert_eq!(
            verify_and_decode_envelope(&token, 1, 0, &out),
            Err(EnvelopeError::MacInvalid),
        );
    }

    #[test]
    fn verify_envelope_rejects_payload_tamper() {
        let token = token();
        let inner = b"authentic payload";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token, 1, 1, inner, &mut out).unwrap();
        // Flip a bit in the inner payload (offset >= 24).
        out[ENVELOPE_OVERHEAD + 3] ^= 0x40;
        assert_eq!(
            verify_and_decode_envelope(&token, 1, 0, &out),
            Err(EnvelopeError::MacInvalid),
        );
    }

    #[test]
    fn verify_envelope_rejects_seq_tamper() {
        let token = token();
        let inner = b"authentic payload";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token, 1, 7, inner, &mut out).unwrap();
        // Bump the seq from 7 to 8 — passes the replay check, but
        // the MAC was computed over seq=7, so verification fails.
        out[0] = 8;
        assert_eq!(
            verify_and_decode_envelope(&token, 1, 0, &out),
            Err(EnvelopeError::MacInvalid),
        );
    }

    #[test]
    fn verify_envelope_rejects_session_id_mismatch() {
        let token = token();
        let inner = b"session-bound";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        // Encode bound to session 100.
        encode_envelope(&token, 100, 1, inner, &mut out).unwrap();
        // Verify with session 200 → MAC differs → reject.
        assert_eq!(
            verify_and_decode_envelope(&token, 200, 0, &out),
            Err(EnvelopeError::MacInvalid),
        );
    }

    #[test]
    fn verify_envelope_rejects_wrong_token() {
        let token_a = derive_session_token(&SECRET_A, &NONCE_A);
        let token_b = derive_session_token(&SECRET_B, &NONCE_B);
        let inner = b"signed by A";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token_a, 1, 1, inner, &mut out).unwrap();
        // Same wire bytes, different verification key — rejected.
        assert_eq!(
            verify_and_decode_envelope(&token_b, 1, 0, &out),
            Err(EnvelopeError::MacInvalid),
        );
    }

    // --- MAC stability sanity checks ---

    #[test]
    fn mac_changes_when_session_id_changes() {
        // compute_mac is internal but exposed via encode_envelope:
        // two envelopes with everything else equal but different
        // session_id must have different MACs.
        let token = token();
        let inner = b"x";
        let mut a = vec![0u8; ENVELOPE_OVERHEAD + 1];
        let mut b = vec![0u8; ENVELOPE_OVERHEAD + 1];
        encode_envelope(&token, 1, 1, inner, &mut a).unwrap();
        encode_envelope(&token, 2, 1, inner, &mut b).unwrap();
        assert_ne!(&a[8..24], &b[8..24]);
    }

    #[test]
    fn mac_changes_when_seq_changes() {
        let token = token();
        let inner = b"x";
        let mut a = vec![0u8; ENVELOPE_OVERHEAD + 1];
        let mut b = vec![0u8; ENVELOPE_OVERHEAD + 1];
        encode_envelope(&token, 1, 1, inner, &mut a).unwrap();
        encode_envelope(&token, 1, 2, inner, &mut b).unwrap();
        assert_ne!(&a[8..24], &b[8..24]);
    }

    #[test]
    fn envelope_overhead_constant_matches_layout() {
        // Lock down the on-the-wire size so a future refactor
        // touching the layout (e.g. widening seq to u128, growing
        // mac to 32B) trips this test rather than silently
        // breaking peer compatibility.
        assert_eq!(ENVELOPE_OVERHEAD, 24);
        assert_eq!(MAC_LEN, 16);
    }

    #[test]
    fn verify_envelope_rejects_seq_zero_at_initial_last_seq() {
        // Locks down the seq numbering convention: senders must
        // start at seq=1, not seq=0. With last_seq initialized to
        // 0 (the rumcast in_translator's startup state), seq=0 is
        // a replay against the initial state and gets rejected.
        // This test catches a future regression where someone
        // changes the comparison to `<` instead of `<=`.
        let token = token();
        let inner = b"first packet but using seq=0";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token, 1, 0, inner, &mut out).unwrap();
        let result = verify_and_decode_envelope(&token, 1, 0, &out);
        assert_eq!(
            result,
            Err(EnvelopeError::Replay {
                seq: 0,
                last_seq: 0
            })
        );
    }

    #[test]
    fn verify_envelope_accepts_seq_one_at_initial_last_seq() {
        // The complement of the previous test: seq=1 is the
        // smallest accepted seq when last_seq=0. Together they
        // pin down the boundary on both sides.
        let token = token();
        let inner = b"first packet";
        let mut out = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&token, 1, 1, inner, &mut out).unwrap();
        let (seq, decoded) = verify_and_decode_envelope(&token, 1, 0, &out).unwrap();
        assert_eq!(seq, 1);
        assert_eq!(decoded, inner);
    }

    // --- Real X25519 ECDH integration ---

    #[test]
    fn end_to_end_with_real_x25519_handshake() {
        // Exercise the full cryptographic chain — ephemeral X25519
        // keypairs on both sides, ECDH to a shared secret, KDF to
        // a session token, encode + verify. Catches any regression
        // where the synthetic-32-byte-array tests pass but the
        // type-level integration with x25519_dalek is broken.
        use x25519_dalek::{PublicKey, StaticSecret};

        // Deterministic keypairs from fixed seeds — keeps the test
        // reproducible. StaticSecret accepts a [u8; 32] directly.
        let server_secret = StaticSecret::from([0x11u8; 32]);
        let server_public = PublicKey::from(&server_secret);
        let client_secret = StaticSecret::from([0x22u8; 32]);
        let client_public = PublicKey::from(&client_secret);

        // Both sides do their half of the ECDH and MUST agree on
        // the shared secret bytes. SharedSecret derefs to [u8; 32]
        // via to_bytes / as_bytes.
        let server_shared = server_secret.diffie_hellman(&client_public);
        let client_shared = client_secret.diffie_hellman(&server_public);
        assert_eq!(
            server_shared.as_bytes(),
            client_shared.as_bytes(),
            "X25519 ECDH must produce identical shared secrets on both sides",
        );

        // KDF with the nonce that the Challenge frame would carry.
        let nonce = [0x77u8; 32];
        let server_token = derive_session_token(server_shared.as_bytes(), &nonce);
        let client_token = derive_session_token(client_shared.as_bytes(), &nonce);
        assert_eq!(
            server_token, client_token,
            "session tokens must match — both sides KDF the same inputs",
        );

        // Now exercise the data-plane envelope with the real
        // tokens. This is what each rumcast packet will carry.
        let session_id: u32 = 0xCAFEBABE;
        let inner = b"realistic-encoded-Request-blob-goes-here";
        let mut wire = vec![0u8; ENVELOPE_OVERHEAD + inner.len()];
        encode_envelope(&client_token, session_id, 1, inner, &mut wire).unwrap();

        let (seq, decoded) =
            verify_and_decode_envelope(&server_token, session_id, 0, &wire).unwrap();
        assert_eq!(seq, 1);
        assert_eq!(decoded, inner);
    }

    // --- Handshake helpers ---

    #[test]
    fn handshake_helpers_round_trip_to_matching_token() {
        // Walk the four-message handshake with both helpers and
        // confirm both sides arrive at the same session token —
        // this is the property the rumcast smoke test and the bench
        // both rely on for envelope verification to succeed.
        use ed25519_dalek::SigningKey;

        let client_signing_key = SigningKey::from_bytes(&[0x42u8; 32]);
        let client_pubkey = client_signing_key.verifying_key().to_bytes();

        // Server side: pick a nonce + ephemeral X25519 keypair (in
        // production this comes from `getrandom::fill`).
        let server_nonce = [0xA1u8; 32];
        let server_x25519_secret = x25519_dalek::StaticSecret::from([0xB2u8; 32]);
        let server_x25519_public = x25519_dalek::PublicKey::from(&server_x25519_secret).to_bytes();

        // Client side: process the Challenge → produce
        // ChallengeResponse + token.
        let client_x25519_secret_bytes = [0xC3u8; 32];
        let client_handshake =
            ClientHandshake::new(&client_signing_key, client_x25519_secret_bytes);
        assert_eq!(client_handshake.ed25519_public_key(), client_pubkey);
        let completed = client_handshake.finish(&server_nonce, &server_x25519_public);

        // The Request the client would send.
        let crate::message::Request::ChallengeResponse {
            signature,
            public_key,
            client_x25519_eph,
        } = completed.challenge_response
        else {
            panic!("expected ChallengeResponse");
        };
        assert_eq!(public_key, client_pubkey);

        // Server side: verify + derive token.
        let server_token = verify_client_handshake(
            &server_nonce,
            &server_x25519_public,
            &server_x25519_secret,
            &public_key,
            &client_x25519_eph,
            &signature,
        )
        .unwrap();

        assert_eq!(
            server_token, completed.session_token,
            "client and server must derive the same session token",
        );
    }

    #[test]
    fn verify_client_handshake_rejects_tampered_signature() {
        // If a man-in-the-middle flips a bit in the signature
        // bytes, the server-side verifier must reject. This is
        // the essential property of the auth handshake.
        use ed25519_dalek::SigningKey;

        let key = SigningKey::from_bytes(&[0x42u8; 32]);
        let nonce = [0xA1u8; 32];
        let server_secret = x25519_dalek::StaticSecret::from([0xB2u8; 32]);
        let server_public = x25519_dalek::PublicKey::from(&server_secret).to_bytes();
        let handshake = ClientHandshake::new(&key, [0xC3u8; 32]);
        let completed = handshake.finish(&nonce, &server_public);

        let crate::message::Request::ChallengeResponse {
            mut signature,
            public_key,
            client_x25519_eph,
        } = completed.challenge_response
        else {
            unreachable!();
        };
        signature[0] ^= 0x01;

        assert_eq!(
            verify_client_handshake(
                &nonce,
                &server_public,
                &server_secret,
                &public_key,
                &client_x25519_eph,
                &signature,
            ),
            Err(HandshakeError::SignatureInvalid),
        );
    }

    #[test]
    fn verify_client_handshake_rejects_eph_substitution() {
        // Active downgrade: an MITM substitutes its own X25519
        // ephemeral hoping the server won't notice — but the
        // signature payload covers `nonce ‖ server_eph ‖
        // client_eph`, so swapping the client_eph field invalidates
        // the signature. Locks down the property that motivated
        // option B (binding the ephemerals into the signature).
        use ed25519_dalek::SigningKey;

        let key = SigningKey::from_bytes(&[0x42u8; 32]);
        let nonce = [0xA1u8; 32];
        let server_secret = x25519_dalek::StaticSecret::from([0xB2u8; 32]);
        let server_public = x25519_dalek::PublicKey::from(&server_secret).to_bytes();
        let handshake = ClientHandshake::new(&key, [0xC3u8; 32]);
        let completed = handshake.finish(&nonce, &server_public);

        let crate::message::Request::ChallengeResponse {
            signature,
            public_key,
            ..
        } = completed.challenge_response
        else {
            unreachable!();
        };

        // Substitute a different client_x25519_eph (an MITM's own
        // pubkey); the signature was made over the original.
        let mitm_eph = [0xFFu8; 32];
        assert_eq!(
            verify_client_handshake(
                &nonce,
                &server_public,
                &server_secret,
                &public_key,
                &mitm_eph,
                &signature,
            ),
            Err(HandshakeError::SignatureInvalid),
        );
    }

    #[test]
    fn verify_client_handshake_rejects_malformed_pubkey() {
        // Ed25519 rejects low-order points and other malformed
        // encodings at parse time. Locks down the dedicated
        // BadClientPublicKey variant.
        use ed25519_dalek::SigningKey;

        let key = SigningKey::from_bytes(&[0x42u8; 32]);
        let nonce = [0xA1u8; 32];
        let server_secret = x25519_dalek::StaticSecret::from([0xB2u8; 32]);
        let server_public = x25519_dalek::PublicKey::from(&server_secret).to_bytes();
        let handshake = ClientHandshake::new(&key, [0xC3u8; 32]);
        let completed = handshake.finish(&nonce, &server_public);

        let crate::message::Request::ChallengeResponse {
            signature,
            client_x25519_eph,
            ..
        } = completed.challenge_response
        else {
            unreachable!();
        };

        // 32 bytes of 0x02: encoded y-coordinate corresponds to a
        // non-square value of `y² - 1` mod p, so curve25519-dalek's
        // CompressedEdwardsY::decompress returns None and
        // VerifyingKey::from_bytes errors before any signature math
        // runs. (Most random 32-byte values DO decode — about half.
        // We hand-pick a known-bad one rather than fuzz-search at
        // test time.)
        let bad_pubkey = [0x02u8; 32];
        assert_eq!(
            verify_client_handshake(
                &nonce,
                &server_public,
                &server_secret,
                &bad_pubkey,
                &client_x25519_eph,
                &signature,
            ),
            Err(HandshakeError::BadClientPublicKey),
        );
    }
}
