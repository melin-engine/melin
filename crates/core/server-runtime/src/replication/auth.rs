//! Replication authentication — Ed25519 challenge/response.
//!
//! Both halves of the handshake live here: `authenticate_replica` runs on
//! the primary side and verifies the replica's signature; `authenticate_with_primary`
//! runs on the replica side and signs the challenge.
//!
//! The wire framing and message encoders/decoders live in
//! `melin_transport_core::replication::protocol`; this module is the
//! exchange-side glue that pairs the generic auth flow with the
//! operator-managed `AuthorizedKeys` permission table.

use std::io::{self, Read, Write};

// Used by the non-blocking `step_authentication` below, which only exists for
// the DPDK sender (and tests) — hence the matching cfg gate.
#[cfg(any(feature = "dpdk", test))]
use tracing::{info, warn};

// Used by `PolledAuthStream` (the receiver-side blocking adapter) below — same
// DPDK-or-test gate as the sender step.
#[cfg(any(feature = "dpdk", test))]
use std::sync::atomic::{AtomicBool, Ordering};

use melin_transport_core::replication::protocol::{
    MAX_CONTROL_FRAME, decode_auth_result, decode_challenge, decode_challenge_response,
    encode_auth_failed, encode_auth_ok, encode_challenge, encode_challenge_response, read_frame,
};

#[cfg(any(feature = "dpdk", test))]
use super::receiver_transport::{FrameResult, compact_recv_buf, try_extract_frame};

/// Generate a fresh 32-byte challenge nonce.
///
/// Shared by the blocking [`authenticate_replica`] and the non-blocking DPDK
/// sender state machine so both issue challenges identically.
pub(super) fn generate_challenge_nonce() -> io::Result<[u8; 32]> {
    let mut nonce = [0u8; 32];
    getrandom::fill(&mut nonce).map_err(|e| io::Error::other(format!("getrandom failed: {e}")))?;
    Ok(nonce)
}

/// Verify a replica's `ChallengeResponse` against the `nonce` we issued and
/// the operator's `AuthorizedKeys`: the key must be listed, carry
/// `Replication` permission, and produce a valid Ed25519 signature over the
/// nonce. `response_frame` is the decoded frame payload (length prefix
/// stripped).
///
/// Pure (no I/O) on purpose — the blocking kernel-TCP path and the
/// non-blocking DPDK poll loop both call this, so the security-critical
/// verification can never diverge between transports.
pub(super) fn verify_challenge_response(
    nonce: &[u8; 32],
    response_frame: &[u8],
    authorized_keys: &melin_app::auth::AuthorizedKeys,
) -> io::Result<()> {
    use ed25519_dalek::{Verifier, VerifyingKey};

    let (signature_bytes, pubkey_bytes) = decode_challenge_response(response_frame)
        .map_err(|e| io::Error::other(format!("bad challenge response: {e}")))?;

    let permission = authorized_keys
        .lookup(&pubkey_bytes)
        .ok_or_else(|| io::Error::other("unknown replication key"))?;
    if !permission.is_replication() {
        return Err(io::Error::other(format!(
            "key has {permission:?} permission, expected Replication"
        )));
    }

    let verifying_key = VerifyingKey::from_bytes(&pubkey_bytes)
        .map_err(|e| io::Error::other(format!("invalid public key: {e}")))?;
    let signature = ed25519_dalek::Signature::from_bytes(&signature_bytes);
    verifying_key
        .verify(nonce, &signature)
        .map_err(|e| io::Error::other(format!("signature verification failed: {e}")))?;

    Ok(())
}

/// Authenticate a replica connection (primary side, blocking — used by the
/// kernel-TCP sender and tests).
///
/// Sends a 32-byte nonce challenge, verifies the replica's Ed25519
/// signature, and checks that the key has `Replication` permission. Must
/// complete within the stream's existing read timeout. The DPDK sender runs
/// the same exchange non-blocking on its poll loop, reusing
/// [`generate_challenge_nonce`] and [`verify_challenge_response`].
pub(crate) fn authenticate_replica<S: Read + Write>(
    stream: &mut S,
    authorized_keys: &melin_app::auth::AuthorizedKeys,
) -> io::Result<()> {
    let nonce = generate_challenge_nonce()?;

    // Send Challenge.
    let mut buf = Vec::with_capacity(64);
    encode_challenge(&nonce, &mut buf);
    stream.write_all(&buf)?;
    stream.flush()?;

    // Read and verify the ChallengeResponse.
    let frame = read_frame(stream, MAX_CONTROL_FRAME)?;
    if let Err(e) = verify_challenge_response(&nonce, &frame, authorized_keys) {
        // Best-effort AuthFailed notice before we bail — the connection is
        // about to drop, so a failed write here is not actionable.
        buf.clear();
        encode_auth_failed(&mut buf);
        let _ = stream.write_all(&buf);
        return Err(e);
    }

    // Auth succeeded.
    buf.clear();
    encode_auth_ok(&mut buf);
    stream.write_all(&buf)?;
    stream.flush()?;

    Ok(())
}

/// Authenticate with the primary (replica side).
///
/// Reads the nonce challenge, signs it with the replica's private key,
/// sends the response, and waits for AuthOk/AuthFailed.
pub(crate) fn authenticate_with_primary<S: Read + Write>(
    stream: &mut S,
    signing_key: &ed25519_dalek::SigningKey,
) -> io::Result<()> {
    use ed25519_dalek::Signer;

    // Read Challenge.
    let frame = read_frame(stream, MAX_CONTROL_FRAME)?;
    let nonce = decode_challenge(&frame)?;

    // Sign the nonce.
    let signature = signing_key.sign(&nonce);
    let pubkey = signing_key.verifying_key();

    // Send ChallengeResponse.
    let mut buf = Vec::with_capacity(128);
    encode_challenge_response(&signature.to_bytes(), pubkey.as_bytes(), &mut buf);
    stream.write_all(&buf)?;
    stream.flush()?;

    // Read auth result.
    let result_frame = read_frame(stream, MAX_CONTROL_FRAME)?;
    match decode_auth_result(&result_frame)? {
        true => Ok(()),
        false => Err(io::Error::other("primary rejected replication key")),
    }
}

/// In-flight challenge state while a sender slot is authenticating. Holds the
/// nonce we issued (verified against the replica's signature) and the deadline
/// by which a valid response must arrive — a silent or malicious replica must
/// not occupy a slot forever, but unlike the blocking [`authenticate_replica`]
/// it also must not stall the shared poll thread, so the timeout is enforced
/// across ticks rather than as a blocking read timeout.
#[cfg(any(feature = "dpdk", test))]
pub(super) struct AuthChallenge {
    pub(super) nonce: [u8; 32],
    pub(super) deadline: std::time::Instant,
}

/// The transport I/O surface the non-blocking auth code needs — the sender-side
/// [`step_authentication`] and the receiver-side [`PolledAuthStream`].
/// Abstracted behind a trait so both can be exercised by a mock in unit tests —
/// the concrete (DPDK) poll loop is otherwise only reachable through the DPDK
/// smoke test. The associated `Handle` keeps tests off the real
/// (non-constructible) `SocketHandle`; the impl for the DPDK transport lives in
/// the `dpdk` module.
///
/// Deliberately *I/O only* — no `close`. Connection teardown is the slot
/// lifecycle's job (the DPDK driver's `go_idle`), so the auth step can never be
/// the thing that does (or forgets) the close.
#[cfg(any(feature = "dpdk", test))]
pub(super) trait AuthTransport {
    type Handle: Copy;
    /// Whether the connection is still open.
    fn is_active(&mut self, handle: Self::Handle) -> bool;
    /// Append any bytes received on `handle` to `dest`.
    fn recv_into_vec(&mut self, handle: Self::Handle, dest: &mut Vec<u8>);
    /// Queue `data` for send; `false` if the per-connection TX queue is full.
    fn queue_send(&mut self, handle: Self::Handle, data: &[u8]) -> bool;
    /// Drive the poll loop once (push queued egress, pull ingress).
    fn poll(&mut self);
}

/// Outcome of one [`step_authentication`] call. The caller maps these onto its
/// slot bookkeeping; the protocol/transport work — reading the response, the
/// shared signature verification, and queuing AuthOk/AuthFailed — happens
/// inside the step so it can be unit-tested over a mock transport.
#[cfg(any(feature = "dpdk", test))]
pub(super) enum AuthOutcome {
    /// Response frame not yet complete — remain authenticating.
    Pending,
    /// Verified; AuthOk has been queued. Advance to the handshake.
    Authenticated,
    /// Rejected: bad signature/permission, timeout, oversized frame, or a
    /// mid-auth disconnect. Any AuthFailed notice has already been queued and
    /// flushed; the caller tears the slot down, which closes the connection.
    Rejected,
}

/// Advance one slot's challenge/response by one poll tick — the non-blocking
/// analog of [`authenticate_replica`] for a poll-driven sender. Touches only
/// the transport, the receive/send buffers, and the key table (no slot/cursor/
/// metric state), so the security-critical failure paths — timeout, oversized
/// frame, mid-auth disconnect, bad signature — are unit-testable with a mock
/// [`AuthTransport`]. Shares [`verify_challenge_response`] with the blocking
/// path so the two transports cannot diverge.
#[cfg(any(feature = "dpdk", test))]
pub(super) fn step_authentication<T: AuthTransport>(
    transport: &mut T,
    handle: T::Handle,
    challenge: &AuthChallenge,
    recv_buf: &mut Vec<u8>,
    send_buf: &mut Vec<u8>,
    authorized_keys: &melin_app::auth::AuthorizedKeys,
    slot_idx: usize,
) -> AuthOutcome {
    // Replica gone mid-auth. The caller's teardown (`go_idle`) closes the
    // handle — which `is_active` reads as false both for an already-removed
    // handle and for a socket still pinned in the SocketSet in Closed/TimeWait,
    // and `close` (idempotent) is the only path that reclaims the latter.
    if !transport.is_active(handle) {
        warn!(slot = slot_idx, "replica disconnected during auth (DPDK)");
        return AuthOutcome::Rejected;
    }

    // Deadline enforced across ticks: a connected-but-silent replica frees its
    // slot without ever blocking the poll thread (cf. the blocking sender's
    // read timeout, which it can afford on its per-replica thread).
    if std::time::Instant::now() >= challenge.deadline {
        warn!(
            slot = slot_idx,
            "replica auth timed out (DPDK) — disconnecting"
        );
        return AuthOutcome::Rejected;
    }

    // Accumulate the ChallengeResponse frame.
    transport.recv_into_vec(handle, recv_buf);
    match try_extract_frame(recv_buf, MAX_CONTROL_FRAME) {
        FrameResult::Complete(payload_start, frame_end) => {
            // Shared verification with the kernel-TCP path — the
            // security-critical step (decode, authorized-keys lookup,
            // Replication-permission check, Ed25519 verify over the nonce).
            let verdict = verify_challenge_response(
                &challenge.nonce,
                &recv_buf[payload_start..frame_end],
                authorized_keys,
            );
            compact_recv_buf(recv_buf, frame_end);
            send_buf.clear();
            match verdict {
                Ok(()) => {
                    // Best-effort AuthOk; the tiny frame flushes on the next
                    // poll. A full TX queue surfaces as a disconnect next tick.
                    encode_auth_ok(send_buf);
                    let _ = transport.queue_send(handle, send_buf);
                    info!(slot = slot_idx, "replica authenticated (DPDK)");
                    AuthOutcome::Authenticated
                }
                Err(e) => {
                    warn!(slot = slot_idx, error = %e, "replica auth failed (DPDK) — disconnecting");
                    // Best-effort AuthFailed notice, flushed (poll) before the
                    // caller's teardown closes the connection.
                    encode_auth_failed(send_buf);
                    let _ = transport.queue_send(handle, send_buf);
                    transport.poll();
                    AuthOutcome::Rejected
                }
            }
        }
        FrameResult::Oversized => {
            warn!(
                slot = slot_idx,
                "oversized auth frame (DPDK) — disconnecting"
            );
            AuthOutcome::Rejected
        }
        // Wait for more data next tick.
        FrameResult::Incomplete => AuthOutcome::Pending,
    }
}

/// Blocking `Read`/`Write` adapter over a poll-driven (non-blocking) transport,
/// so the SHARED blocking [`authenticate_with_primary`] can run over a
/// smoltcp-style poll loop without a transport-specific copy of the replica's
/// auth flow. `read` polls until bytes arrive then hands them out from the
/// front of the shared `recv_buf`; `write` queues a frame and `flush` (or the
/// next `read`) drives `poll()` to push it onto the wire. The receiver shares
/// its streaming `recv_buf`, so any bytes that arrive past the auth exchange
/// are carried into the handshake loop. Bounded by `deadline` and the
/// `shutdown` flag so a silent or vanished primary cannot hang the receiver.
///
/// Generic over [`AuthTransport`] — the same surface the sender-side
/// [`step_authentication`] uses — so the adapter's edge cases (deadline,
/// shutdown, disconnect-vs-frame-in-the-same-poll, full TX queue) are
/// unit-testable over a mock without the `dpdk` feature. The concrete DPDK
/// construction lives in the `dpdk` module.
#[cfg(any(feature = "dpdk", test))]
pub(super) struct PolledAuthStream<'a, T: AuthTransport> {
    pub(super) transport: &'a mut T,
    pub(super) handle: T::Handle,
    pub(super) recv_buf: &'a mut Vec<u8>,
    pub(super) shutdown: &'a AtomicBool,
    pub(super) deadline: std::time::Instant,
}

#[cfg(any(feature = "dpdk", test))]
impl<T: AuthTransport> Read for PolledAuthStream<'_, T> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            if !self.recv_buf.is_empty() {
                let n = self.recv_buf.len().min(out.len());
                out[..n].copy_from_slice(&self.recv_buf[..n]);
                // Drain consumed bytes from the front; anything past the auth
                // frames stays for the handshake loop (same shared buffer).
                self.recv_buf.drain(..n);
                return Ok(n);
            }
            if self.shutdown.load(Ordering::Relaxed) {
                return Err(io::Error::other("shutdown during auth"));
            }
            if std::time::Instant::now() >= self.deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "auth timed out"));
            }
            self.transport.poll();
            self.transport.recv_into_vec(self.handle, self.recv_buf);
            // Only treat an empty read as a disconnect; a frame arriving in the
            // same poll as the FIN is drained above before we get here.
            if self.recv_buf.is_empty() && !self.transport.is_active(self.handle) {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "primary disconnected during auth",
                ));
            }
            std::thread::yield_now();
        }
    }
}

#[cfg(any(feature = "dpdk", test))]
impl<T: AuthTransport> Write for PolledAuthStream<'_, T> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // queue_send copies into the per-connection TX queue; poll() (flush
        // below, or the next read) pushes it onto the wire. Returns false only
        // when the queue is full — never expected for the tiny auth frames on a
        // fresh connection, but surface it rather than silently drop.
        if self.transport.queue_send(self.handle, data) {
            Ok(data.len())
        } else {
            Err(io::Error::other("TX queue full during auth"))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        // Drive egress so the queued frame leaves before we block on a read.
        self.transport.poll();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use std::collections::VecDeque;

    /// Build an `AuthorizedKeys` table granting `permission` to `key`.
    fn keys_for(key: &SigningKey, permission: &str) -> melin_app::auth::AuthorizedKeys {
        let pub_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            key.verifying_key().to_bytes(),
        );
        melin_app::auth::AuthorizedKeys::parse(&format!("{permission} {pub_b64} test\n")).unwrap()
    }

    /// Encode a `ChallengeResponse` and return just the payload (4-byte LE
    /// length prefix stripped — see `protocol::read_frame`), matching what
    /// the runtime feeds to `verify_challenge_response`.
    fn response_payload(key: &SigningKey, nonce: &[u8; 32]) -> Vec<u8> {
        let sig = key.sign(nonce);
        let mut frame = Vec::new();
        melin_transport_core::replication::protocol::encode_challenge_response(
            &sig.to_bytes(),
            key.verifying_key().as_bytes(),
            &mut frame,
        );
        frame[4..].to_vec()
    }

    #[test]
    fn verify_accepts_valid_replication_key() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let keys = keys_for(&key, "replication");
        let nonce = [0x42; 32];
        assert!(verify_challenge_response(&nonce, &response_payload(&key, &nonce), &keys).is_ok());
    }

    #[test]
    fn verify_rejects_unknown_key() {
        let signer = SigningKey::from_bytes(&[0x22; 32]);
        let listed = SigningKey::from_bytes(&[0x33; 32]);
        let keys = keys_for(&listed, "replication"); // table lists a different key
        let nonce = [0x42; 32];
        let err = verify_challenge_response(&nonce, &response_payload(&signer, &nonce), &keys)
            .unwrap_err();
        assert!(err.to_string().contains("unknown"));
    }

    #[test]
    fn verify_rejects_non_replication_permission() {
        let key = SigningKey::from_bytes(&[0x44; 32]);
        let keys = keys_for(&key, "trader"); // valid key, wrong permission
        let nonce = [0x42; 32];
        let err =
            verify_challenge_response(&nonce, &response_payload(&key, &nonce), &keys).unwrap_err();
        assert!(err.to_string().contains("Replication"));
    }

    #[test]
    fn verify_rejects_signature_over_wrong_nonce() {
        let key = SigningKey::from_bytes(&[0x55; 32]);
        let keys = keys_for(&key, "replication");
        // Replica signed a different nonce than the one we verify against.
        let signed = [0x01; 32];
        let challenge = [0x02; 32];
        let err = verify_challenge_response(&challenge, &response_payload(&key, &signed), &keys)
            .unwrap_err();
        assert!(err.to_string().contains("signature"));
    }

    // ---- Non-blocking sender step (`step_authentication`) ----

    /// In-memory [`AuthTransport`]: scripts the bytes successive
    /// `recv_into_vec` calls deliver and captures what was sent. `Handle = ()`
    /// keeps the test off the real (non-constructible) `SocketHandle`. There is
    /// no `close` — teardown is the slot lifecycle's job, not the transport
    /// surface's (see the trait doc).
    struct MockAuthTransport {
        active: bool,
        /// Chunks delivered one per `recv_into_vec` call, in order — models a
        /// transport that yields bytes across successive polls. An empty (or
        /// exhausted) queue means "this poll produced nothing."
        recv_rounds: VecDeque<Vec<u8>>,
        /// When `recv_rounds` empties (the final chunk was just delivered, or
        /// there was none), flip `active` to false — models the primary's FIN
        /// riding with or arriving just after the last data.
        close_when_drained: bool,
        /// Concatenation of every `queue_send` payload.
        sent: Vec<u8>,
        /// `queue_send` reports the TX queue full (returns false) when set.
        tx_full: bool,
    }

    impl MockAuthTransport {
        /// One-shot delivery of `incoming` on the first `recv_into_vec` — the
        /// shape the sender-step tests use.
        fn with_incoming(incoming: Vec<u8>) -> Self {
            Self {
                active: true,
                recv_rounds: VecDeque::from([incoming]),
                close_when_drained: false,
                sent: Vec::new(),
                tx_full: false,
            }
        }

        /// Deliver `rounds` one chunk per poll, then report the connection
        /// inactive — used by the receiver-adapter tests, where it guarantees a
        /// `PolledAuthStream` read loop terminates instead of spinning.
        fn scripted(rounds: Vec<Vec<u8>>) -> Self {
            Self {
                active: true,
                recv_rounds: rounds.into(),
                close_when_drained: true,
                sent: Vec::new(),
                tx_full: false,
            }
        }
    }

    impl AuthTransport for MockAuthTransport {
        type Handle = ();
        fn is_active(&mut self, _: ()) -> bool {
            self.active
        }
        fn recv_into_vec(&mut self, _: (), dest: &mut Vec<u8>) {
            if let Some(mut chunk) = self.recv_rounds.pop_front() {
                dest.append(&mut chunk);
            }
            if self.recv_rounds.is_empty() && self.close_when_drained {
                self.active = false;
            }
        }
        fn queue_send(&mut self, _: (), data: &[u8]) -> bool {
            if self.tx_full {
                return false;
            }
            self.sent.extend_from_slice(data);
            true
        }
        fn poll(&mut self) {}
    }

    /// Full wire frame (4-byte LE length prefix + body) of a `ChallengeResponse`
    /// signing `nonce` with `key` — exactly what the replica puts on the wire
    /// (cf. `response_payload`, which strips the prefix).
    fn response_frame(key: &SigningKey, nonce: &[u8; 32]) -> Vec<u8> {
        let sig = key.sign(nonce);
        let mut frame = Vec::new();
        encode_challenge_response(&sig.to_bytes(), key.verifying_key().as_bytes(), &mut frame);
        frame
    }

    fn challenge_at(nonce: [u8; 32], deadline: std::time::Instant) -> AuthChallenge {
        AuthChallenge { nonce, deadline }
    }

    fn far_future() -> std::time::Instant {
        std::time::Instant::now() + std::time::Duration::from_secs(60)
    }

    /// Run one auth step against `incoming`, returning the outcome and the mock
    /// so the caller can assert on the queued bytes.
    fn run_step(
        incoming: Vec<u8>,
        challenge: &AuthChallenge,
        keys: &melin_app::auth::AuthorizedKeys,
    ) -> (AuthOutcome, MockAuthTransport) {
        let mut tx = MockAuthTransport::with_incoming(incoming);
        let mut recv = Vec::new();
        let mut send = Vec::new();
        let outcome = step_authentication(&mut tx, (), challenge, &mut recv, &mut send, keys, 0);
        (outcome, tx)
    }

    #[test]
    fn step_authenticates_valid_response() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let keys = keys_for(&key, "replication");
        let nonce = [0x42; 32];
        let challenge = challenge_at(nonce, far_future());
        let (outcome, tx) = run_step(response_frame(&key, &nonce), &challenge, &keys);
        assert!(matches!(outcome, AuthOutcome::Authenticated));
        assert!(
            decode_auth_result(&tx.sent[4..]).expect("AuthOk frame"),
            "AuthOk should be queued on success"
        );
    }

    #[test]
    fn step_rejects_and_signals_failure_on_bad_signature() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let keys = keys_for(&key, "replication");
        // Replica signs a different nonce than the one we challenged with.
        let challenge = challenge_at([0x42; 32], far_future());
        let (outcome, tx) = run_step(response_frame(&key, &[0x01; 32]), &challenge, &keys);
        assert!(matches!(outcome, AuthOutcome::Rejected));
        // AuthFailed queued + flushed; the caller's teardown then closes.
        assert!(
            !decode_auth_result(&tx.sent[4..]).expect("AuthFailed frame"),
            "AuthFailed should be queued for a verification failure"
        );
    }

    #[test]
    fn step_rejects_unknown_key() {
        let signer = SigningKey::from_bytes(&[0x22; 32]);
        let listed = SigningKey::from_bytes(&[0x33; 32]);
        let keys = keys_for(&listed, "replication"); // table lists a different key
        let nonce = [0x42; 32];
        let challenge = challenge_at(nonce, far_future());
        let (outcome, tx) = run_step(response_frame(&signer, &nonce), &challenge, &keys);
        assert!(matches!(outcome, AuthOutcome::Rejected));
        assert!(
            !decode_auth_result(&tx.sent[4..]).expect("AuthFailed frame"),
            "an unknown key is told AuthFailed"
        );
    }

    #[test]
    fn step_rejects_non_replication_permission() {
        let key = SigningKey::from_bytes(&[0x44; 32]);
        let keys = keys_for(&key, "trader"); // valid key, wrong permission
        let nonce = [0x42; 32];
        let challenge = challenge_at(nonce, far_future());
        let (outcome, tx) = run_step(response_frame(&key, &nonce), &challenge, &keys);
        assert!(matches!(outcome, AuthOutcome::Rejected));
        assert!(
            !decode_auth_result(&tx.sent[4..]).expect("AuthFailed frame"),
            "a wrong-permission key is told AuthFailed"
        );
    }

    #[test]
    fn step_times_out_past_deadline() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let keys = keys_for(&key, "replication");
        // Deadline = now; the monotonic clock read inside the step is `>=` it,
        // so the timeout trips. No data delivered.
        let challenge = challenge_at([0x42; 32], std::time::Instant::now());
        let (outcome, tx) = run_step(Vec::new(), &challenge, &keys);
        assert!(matches!(outcome, AuthOutcome::Rejected));
        assert!(
            tx.sent.is_empty(),
            "no AuthFailed on timeout — nothing to say"
        );
    }

    #[test]
    fn step_rejects_oversized_frame() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let keys = keys_for(&key, "replication");
        let challenge = challenge_at([0x42; 32], far_future());
        // A length prefix declaring more than MAX_CONTROL_FRAME bytes.
        let oversized = ((MAX_CONTROL_FRAME + 1) as u32).to_le_bytes().to_vec();
        let (outcome, tx) = run_step(oversized, &challenge, &keys);
        assert!(matches!(outcome, AuthOutcome::Rejected));
        assert!(tx.sent.is_empty(), "no AuthFailed on a malformed frame");
    }

    #[test]
    fn step_pending_on_partial_frame() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let keys = keys_for(&key, "replication");
        let nonce = [0x42; 32];
        let challenge = challenge_at(nonce, far_future());
        // Deliver fewer than the 4-byte length prefix.
        let mut partial = response_frame(&key, &nonce);
        partial.truncate(3);
        let (outcome, tx) = run_step(partial, &challenge, &keys);
        assert!(matches!(outcome, AuthOutcome::Pending));
        assert!(tx.sent.is_empty(), "an incomplete frame just waits");
    }

    #[test]
    fn step_rejects_on_disconnect() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let keys = keys_for(&key, "replication");
        let challenge = challenge_at([0x42; 32], far_future());
        let mut tx = MockAuthTransport::with_incoming(Vec::new());
        tx.active = false; // replica gone (RST/FIN observed)
        let mut recv = Vec::new();
        let mut send = Vec::new();
        let outcome = step_authentication(&mut tx, (), &challenge, &mut recv, &mut send, &keys, 0);
        // Rejected with nothing said; the caller's `go_idle` reclaims the
        // (possibly still SocketSet-pinned) handle.
        assert!(matches!(outcome, AuthOutcome::Rejected));
        assert!(tx.sent.is_empty());
    }

    // ---- Receiver-side blocking adapter (`PolledAuthStream`) ----

    /// Run `body` over a `PolledAuthStream` wrapping `tx` with the given
    /// `shutdown` flag and `deadline`. Keeps the recv-buf / borrow plumbing out
    /// of each test; `tx` is free to inspect once this returns.
    fn with_stream<R>(
        tx: &mut MockAuthTransport,
        shutdown: &AtomicBool,
        deadline: std::time::Instant,
        body: impl FnOnce(&mut PolledAuthStream<'_, MockAuthTransport>) -> R,
    ) -> R {
        let mut recv = Vec::new();
        let mut stream = PolledAuthStream {
            transport: tx,
            handle: (),
            recv_buf: &mut recv,
            shutdown,
            deadline,
        };
        body(&mut stream)
    }

    #[test]
    fn polled_read_times_out_past_deadline() {
        let mut tx = MockAuthTransport::scripted(vec![]);
        let shutdown = AtomicBool::new(false);
        // Deadline = now; the monotonic clock read inside the adapter is `>=`
        // it, so the timeout trips before any disconnect/EOF handling.
        let err = with_stream(&mut tx, &shutdown, std::time::Instant::now(), |s| {
            std::io::Read::read(s, &mut [0u8; 8]).unwrap_err()
        });
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn polled_read_errors_on_shutdown() {
        let mut tx = MockAuthTransport::scripted(vec![]);
        let shutdown = AtomicBool::new(true);
        let err = with_stream(&mut tx, &shutdown, far_future(), |s| {
            std::io::Read::read(s, &mut [0u8; 8]).unwrap_err()
        });
        assert!(err.to_string().contains("shutdown"));
    }

    #[test]
    fn polled_read_reports_disconnect_as_eof() {
        // No data and the transport reports closed → a clean EOF, not a hang.
        let mut tx = MockAuthTransport::scripted(vec![]);
        let shutdown = AtomicBool::new(false);
        let err = with_stream(&mut tx, &shutdown, far_future(), |s| {
            std::io::Read::read(s, &mut [0u8; 8]).unwrap_err()
        });
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn polled_read_returns_bytes_arriving_with_fin() {
        // The frame and the FIN land in the same poll: the bytes must be handed
        // out, not swallowed as EOF (the property the adapter's comment calls
        // out). A subsequent read then sees the closed transport → EOF.
        let mut tx = MockAuthTransport::scripted(vec![vec![1, 2, 3, 4]]);
        let shutdown = AtomicBool::new(false);
        with_stream(&mut tx, &shutdown, far_future(), |s| {
            let mut out = [0u8; 8];
            let n = std::io::Read::read(s, &mut out)
                .expect("bytes delivered with the FIN are returned");
            assert_eq!(&out[..n], &[1, 2, 3, 4]);
            let err = std::io::Read::read(s, &mut out).unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        });
    }

    #[test]
    fn polled_write_errors_when_tx_queue_full() {
        let mut tx = MockAuthTransport::scripted(vec![]);
        tx.tx_full = true;
        let shutdown = AtomicBool::new(false);
        let err = with_stream(&mut tx, &shutdown, far_future(), |s| {
            std::io::Write::write(s, b"auth-frame").unwrap_err()
        });
        assert!(err.to_string().contains("TX queue full"));
    }

    #[test]
    fn polled_write_queues_frame_then_flush_polls() {
        let mut tx = MockAuthTransport::scripted(vec![]);
        let shutdown = AtomicBool::new(false);
        with_stream(&mut tx, &shutdown, far_future(), |s| {
            assert_eq!(std::io::Write::write(s, b"hello").unwrap(), 5);
            std::io::Write::flush(s).unwrap();
        });
        assert_eq!(tx.sent, b"hello", "the queued frame reaches the TX queue");
    }

    #[test]
    fn polled_stream_drives_authenticate_with_primary_to_ok() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let nonce = [0x42; 32];
        // Primary plays Challenge, then (after our response) AuthOk.
        let mut challenge = Vec::new();
        encode_challenge(&nonce, &mut challenge);
        let mut authok = Vec::new();
        encode_auth_ok(&mut authok);
        let mut tx = MockAuthTransport::scripted(vec![challenge, authok]);
        let shutdown = AtomicBool::new(false);

        let result = with_stream(&mut tx, &shutdown, far_future(), |s| {
            authenticate_with_primary(s, &key)
        });
        result.expect("auth succeeds when the primary sends AuthOk");

        // The adapter put a well-formed ChallengeResponse on the wire: a valid
        // signature over the nonce by `key`, which an authorized table accepts.
        let keys = keys_for(&key, "replication");
        verify_challenge_response(&nonce, &tx.sent[4..], &keys)
            .expect("the response the adapter wrote verifies");
    }

    #[test]
    fn polled_stream_surfaces_auth_failed() {
        let key = SigningKey::from_bytes(&[0x11; 32]);
        let nonce = [0x42; 32];
        let mut challenge = Vec::new();
        encode_challenge(&nonce, &mut challenge);
        let mut authfailed = Vec::new();
        encode_auth_failed(&mut authfailed);
        let mut tx = MockAuthTransport::scripted(vec![challenge, authfailed]);
        let shutdown = AtomicBool::new(false);

        let err = with_stream(&mut tx, &shutdown, far_future(), |s| {
            authenticate_with_primary(s, &key).unwrap_err()
        });
        assert!(err.to_string().contains("rejected"));
    }
}
