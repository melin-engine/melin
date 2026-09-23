#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! A tamper-evident notary built on the Melin core runtime.
//!
//! Clients submit a 32-byte digest of whatever they want attested. The
//! sequencer assigns it a total order and a time, folds both into a
//! rolling BLAKE3 commitment, and returns a receipt: the position the
//! digest landed at, the time it was sequenced, the chain head before it,
//! and the head after folding it in. A receipt is a self-contained link:
//! anyone holding the original document can recompute its digest and
//! check `BLAKE3(prev ‖ digest ‖ time) == head` with nothing else, and
//! consecutive receipts chain — one's `head` is the next's `prev` — so a
//! set of them proves order as well as membership.
//!
//! This is how a real notary or timestamping service works, and the
//! reason it takes a digest rather than a document is not efficiency but
//! design: the service attests to *when and in what order* something
//! existed, without ever holding the thing itself. Clients keep their
//! documents; the log keeps proof. The time is the sequencer's clock at
//! dispatch, journaled with the event, so replay and replicas fold the
//! same value — and because it is folded in, the service cannot later
//! claim a different time for an entry without breaking every receipt
//! after it.
//!
//! ## Why this makes a good example
//!
//! Where `melin-example-echo` is the runtime with nothing on top and
//! `melin-example-counter` the smallest application with state to keep,
//! this one exercises the guarantees the sequencer actually sells.
//! The head is a deterministic function of the ordered leaves, so two
//! nodes that applied the same events agree on it byte for byte. That is
//! a far sharper cross-node assertion than comparing a counter: a counter
//! cannot detect a reordering that happens to commute, and a hash chain
//! cannot miss one.
//!
//! A chain is deliberately the simplest structure that gives this. What
//! it does not give is a short proof that one receipt sits under the
//! current head without walking every link between: that is what a
//! Merkle log (the shape Certificate Transparency uses) adds, at the
//! price of a tree that has to be persisted and compacted as the log
//! grows. It is the upgrade path if inclusion proofs are ever wanted,
//! not something an example needs.
//!
//! ## Sizing
//!
//! A fixed 32-byte leaf keeps [`AppEvent::MAX_ENCODED_SIZE`] at 33, so a
//! journal entry costs 74 bytes and the pipeline's ring slots stay near
//! the size they were tuned for. An application that carried variable
//! payloads inline would pay for its widest payload in *every* ring slot
//! — `AppEvent` is `Copy` and the hot path cannot allocate — and would
//! shorten the journal's fsync batches, since the transport sizes a batch
//! by how many entries fit one hand-off chunk. Neither cost applies here. See
//! `tests/footprint.rs` and `tests/journal_limit.rs`, which assert both.
//!
//! ## Where to look
//!
//! - `lib.rs` (this file): the application — event, state machine, and
//!   the request/response codecs the runtime plugs into. Read top to
//!   bottom; it is short.
//! - `main.rs`: the server binary, and the recipe for running it.
//! - `client.rs`: what a user runs — hash a file, submit, keep the
//!   receipt, verify it offline.
//! - `audit.rs`: what an auditor runs — refold the chain from the journal
//!   on disk, without the server, and check receipts against it.
//! - `receipt.rs`: the receipt as clients keep it; shared by the two above.
//! - `tests/round_trip.rs`: the guarantees, end to end — over raw frames,
//!   through the client and the auditor, and across a replica failover.

use std::io::{self, Read, Write};

use melin_app::auth::Permission;
use melin_app::decoder::{Decoded, RequestDecoder as RequestDecoderTrait};
use melin_app::encoder::ResponseEncoder as ResponseEncoderTrait;
use melin_app::{AppEvent, Application, ApplyCtx, CodecError, QueryCtx, RejectReason};

/// The receipt as clients keep it — shared by the client and the auditor.
pub mod receipt;

// ---------------------------------------------------------------------------
// Message kinds — the first byte of every request and response body. The
// body is the application's from its first byte, so any value will do:
// these are the notary's own, and the request kinds double as its
// journal encoding.
// ---------------------------------------------------------------------------

pub const KIND_NOTARIZE: u8 = 0x10;
pub const KIND_GET_HEAD: u8 = 0x11;

pub const KIND_RESP_RECEIPT: u8 = 0x30;
pub const KIND_RESP_HEAD: u8 = 0x31;
pub const KIND_RESP_REJECTED: u8 = 0x32;

/// Width of a submitted digest, in bytes.
///
/// The log never inspects a leaf, so any 32-byte digest works — BLAKE3,
/// SHA-256, whatever the client's own tooling produces. Fixing the width
/// rather than accepting variable input is what keeps an event small
/// enough to sit in a ring slot without inflating it.
pub const LEAF_LEN: usize = 32;

/// Width of the rolling commitment, in bytes. BLAKE3's default output.
pub const HEAD_LEN: usize = 32;

/// Commitment of an empty log.
///
/// All-zero rather than a domain-separated constant: this chain has only
/// one kind of fold, so there is no leaf/interior-node confusion to
/// defend against, and genesis only has to be a fixed value every node
/// starts from.
pub const GENESIS_HEAD: [u8; HEAD_LEN] = [0u8; HEAD_LEN];

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

/// State-mutating events journaled by the pipeline, plus a read-only query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotaryEvent {
    /// Fold a client-supplied digest into the commitment. Journaled.
    Notarize { leaf: [u8; LEAF_LEN] },
    /// Return the current head and entry count. Not journaled (query).
    GetHead,
}

impl AppEvent for NotaryEvent {
    // The widest variant: `Notarize`'s kind(1) + leaf(32). Checked against
    // the journal's entry ceiling at compile time.
    const MAX_ENCODED_SIZE: usize = 1 + LEAF_LEN;

    fn encoded_size(&self) -> usize {
        match self {
            NotaryEvent::Notarize { .. } => Self::MAX_ENCODED_SIZE,
            // kind(1)
            NotaryEvent::GetHead => 1,
        }
    }

    fn encode(&self, buf: &mut [u8]) -> usize {
        match self {
            NotaryEvent::Notarize { leaf } => {
                buf[0] = KIND_NOTARIZE;
                buf[1..1 + LEAF_LEN].copy_from_slice(leaf);
                Self::MAX_ENCODED_SIZE
            }
            NotaryEvent::GetHead => {
                buf[0] = KIND_GET_HEAD;
                1
            }
        }
    }

    fn decode(buf: &[u8]) -> Result<Self, CodecError> {
        let (&kind, rest) = buf.split_first().ok_or(CodecError::Truncated)?;
        match kind {
            KIND_NOTARIZE => Ok(NotaryEvent::Notarize {
                leaf: leaf_from(rest)?,
            }),
            KIND_GET_HEAD => Ok(NotaryEvent::GetHead),
            kind => Err(CodecError::UnknownTag(kind)),
        }
    }

    fn is_query(&self) -> bool {
        matches!(self, NotaryEvent::GetHead)
    }
}

/// Read a leaf from exactly [`LEAF_LEN`] bytes.
///
/// Exact, not "at least": a longer input means the sender disagrees with
/// this log about the digest width, and silently truncating it would
/// commit to something the client did not intend.
fn leaf_from(bytes: &[u8]) -> Result<[u8; LEAF_LEN], CodecError> {
    if bytes.len() < LEAF_LEN {
        return Err(CodecError::Truncated);
    }
    if bytes.len() > LEAF_LEN {
        return Err(CodecError::InvalidField);
    }
    Ok(bytes.try_into().expect("checked to be LEAF_LEN bytes"))
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

/// Fan-out report emitted by `apply`. One per state-mutating event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotaryReport {
    /// Attestation that a leaf was folded in at position `entry`.
    Receipt {
        /// 1-based position in the chain.
        ///
        /// The application's own counter, not the journal's sequence —
        /// that one is node-local and fsync-timing dependent, which is why
        /// only a query can see it (`QueryCtx::journal_sequence`).
        entry: u64,
        /// When the sequencer received the leaf, in nanoseconds since
        /// the Unix epoch. Folded into `head`, so it is attested, not
        /// merely reported. Not guaranteed to increase from one receipt
        /// to the next (see `ApplyCtx::now_ns`): order is `entry`.
        timestamp_ns: u64,
        /// Commitment before this leaf was folded in. What makes the
        /// receipt verifiable on its own:
        /// `fold(prev, leaf, timestamp_ns) == head`.
        prev: [u8; HEAD_LEN],
        /// Commitment after folding this leaf in.
        head: [u8; HEAD_LEN],
    },
    /// The runtime refused the event before `apply` saw it (see
    /// [`Application::build_reject`]). Nothing was folded in.
    Rejected,
}

/// 1:1 query response returned by `query`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotaryHead {
    /// Leaves folded in so far — the position the next receipt will get,
    /// less one.
    pub entries: u64,
    /// The current commitment.
    pub head: [u8; HEAD_LEN],
}

// ---------------------------------------------------------------------------
// Application
// ---------------------------------------------------------------------------

/// The notary state machine: a rolling commitment and how many leaves
/// have been folded into it.
pub struct Notary {
    head: [u8; HEAD_LEN],
    entries: u64,
}

/// Fold one leaf into the chain: `BLAKE3(prev || leaf || timestamp_ns)`.
///
/// All three inputs are fixed-width (32 + 32 + 8 bytes, the time in
/// little-endian), so the concatenation parses unambiguously and distinct
/// inputs cannot collide by re-splitting at a different point. No length
/// prefix or domain separator is needed.
///
/// Public because it is the verification rule as much as the folding
/// rule: `notary-client verify` recomputes it from a receipt, offline.
pub fn fold(prev: &[u8; HEAD_LEN], leaf: &[u8; LEAF_LEN], timestamp_ns: u64) -> [u8; HEAD_LEN] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prev);
    hasher.update(leaf);
    hasher.update(&timestamp_ns.to_le_bytes());
    *hasher.finalize().as_bytes()
}

/// The genesis state every node's chain starts from: nothing folded in.
impl Default for Notary {
    fn default() -> Self {
        Notary {
            head: GENESIS_HEAD,
            entries: 0,
        }
    }
}

impl Notary {
    /// Current commitment.
    #[inline]
    pub fn head(&self) -> [u8; HEAD_LEN] {
        self.head
    }

    /// Leaves folded in so far.
    #[inline]
    pub fn entries(&self) -> u64 {
        self.entries
    }
}

impl Application for Notary {
    type Event = NotaryEvent;
    type Report = NotaryReport;
    type QueryResponse = NotaryHead;
    // A fixed-width commitment and a counter: nothing to reserve for.
    type Sizing = ();

    fn apply(&mut self, event: Self::Event, ctx: &ApplyCtx, out: &mut Vec<Self::Report>) {
        match event {
            NotaryEvent::Notarize { leaf } => {
                // Simplification, as in the counter example: every request
                // is accepted, so a client retrying after losing its
                // receipt lands a second entry. A production app decides
                // its own policy for repeats, typically a per-client
                // sequence carried in the event and checked here. Not to
                // be confused with re-attesting the same digest under a
                // new request, which is supported by design: a later
                // position and a later time are a different commitment.
                //
                // `now_ns` is the sequencer's dispatch clock, journaled
                // with the entry, so replay and replicas see the same
                // value — which is what makes folding it deterministic.
                let timestamp_ns = ctx.now_ns;
                let prev = self.head;
                self.head = fold(&prev, &leaf, timestamp_ns);
                // Saturating rather than wrapping: a receipt attests to a
                // position, so the counter must never run backwards. At
                // 10M events/sec saturation is ~58,000 years out — this
                // is a statement of intent, not a reachable branch.
                self.entries = self.entries.saturating_add(1);
                out.push(NotaryReport::Receipt {
                    entry: self.entries,
                    timestamp_ns,
                    prev,
                    head: self.head,
                });
            }
            // A query: answered by `query`, never applied.
            NotaryEvent::GetHead => {}
        }
    }

    fn query(&self, event: Self::Event, _ctx: &QueryCtx) -> Option<Self::QueryResponse> {
        match event {
            NotaryEvent::GetHead => Some(NotaryHead {
                entries: self.entries,
                head: self.head,
            }),
            NotaryEvent::Notarize { .. } => None,
        }
    }

    // No `tick`: the notary has no time-driven work, and the default does
    // nothing. Time reaches it through `ApplyCtx::now_ns` alone.

    fn build_reject(_event: &Self::Event, _reason: RejectReason) -> Self::Report {
        NotaryReport::Rejected
    }

    fn snapshot<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(&self.head)?;
        w.write_all(&self.entries.to_le_bytes())
    }

    fn restore<R: Read>(r: &mut R) -> io::Result<Self> {
        let mut head = [0u8; HEAD_LEN];
        r.read_exact(&mut head)?;
        let mut entries = [0u8; 8];
        r.read_exact(&mut entries)?;
        Ok(Notary {
            head,
            entries: u64::from_le_bytes(entries),
        })
    }

    const APP_VERSION: u16 = 1;
}

// ---------------------------------------------------------------------------
// Request decoder
// ---------------------------------------------------------------------------

/// Decodes client requests into `NotaryEvent`. A request body is its kind,
/// then the kind's fields:
///   - notarize: `[KIND_NOTARIZE][leaf: 32 bytes]`
///   - get head: `[KIND_GET_HEAD]`
pub struct RequestDecoder;

impl RequestDecoderTrait for RequestDecoder {
    type Event = NotaryEvent;

    fn decode(&self, body: &[u8], permission: Permission) -> Decoded<NotaryEvent> {
        let Some((&kind, fields)) = body.split_first() else {
            return Decoded::DecodeError("empty request");
        };
        match kind {
            KIND_NOTARIZE => {
                // Unlike the counter example, this one gates on
                // permission: notarizing appends to the log, so the
                // read-only role is refused. (A replication key never gets
                // this far: the client listener refuses it.)
                if permission == Permission::ReadOnly {
                    return Decoded::PermissionDenied("notarizing requires a writing role");
                }
                match leaf_from(fields) {
                    Ok(leaf) => Decoded::Permitted(NotaryEvent::Notarize { leaf }),
                    Err(_) => Decoded::DecodeError("leaf must be exactly 32 bytes"),
                }
            }
            // Queries are readable by every authenticated role.
            KIND_GET_HEAD => Decoded::Permitted(NotaryEvent::GetHead),
            _ => Decoded::DecodeError("unknown kind"),
        }
    }
}

// ---------------------------------------------------------------------------
// Response encoder
// ---------------------------------------------------------------------------

/// Encodes `NotaryReport` / `NotaryHead` into response bodies; the runtime
/// frames them. A response body is its kind, then the kind's fields:
///   - receipt:  `[KIND_RESP_RECEIPT][entry: u64][timestamp_ns: u64][prev: 32 bytes][head: 32 bytes]`
///   - head:     `[KIND_RESP_HEAD][entries: u64][head: 32 bytes]`
///   - rejected: `[KIND_RESP_REJECTED]`
pub struct ResponseEncoder;

/// Kind (1) + entry (8) + timestamp (8) + prev (32) + head (32).
const RECEIPT_BODY_LEN: usize = 1 + 8 + 8 + HEAD_LEN + HEAD_LEN;

/// Kind (1) + entries (8) + head (32).
const HEAD_BODY_LEN: usize = 1 + 8 + HEAD_LEN;

/// The first `len` bytes of `buf`, with `kind` written into the first of
/// them, checked so the callers' fixed-offset writes cannot panic.
fn body(buf: &mut [u8], kind: u8, len: usize) -> Result<&mut [u8], &'static str> {
    let body = buf.get_mut(..len).ok_or("buffer too small")?;
    *body.first_mut().ok_or("buffer too small")? = kind;
    Ok(body)
}

impl ResponseEncoderTrait for ResponseEncoder {
    type Report = NotaryReport;
    type Query = NotaryHead;

    fn encode_report(&self, report: &NotaryReport, buf: &mut [u8]) -> Result<usize, &'static str> {
        match report {
            NotaryReport::Receipt {
                entry,
                timestamp_ns,
                prev,
                head,
            } => {
                let body = body(buf, KIND_RESP_RECEIPT, RECEIPT_BODY_LEN)?;
                body[1..9].copy_from_slice(&entry.to_le_bytes());
                body[9..17].copy_from_slice(&timestamp_ns.to_le_bytes());
                body[17..49].copy_from_slice(prev);
                body[49..].copy_from_slice(head);
                Ok(RECEIPT_BODY_LEN)
            }
            NotaryReport::Rejected => {
                body(buf, KIND_RESP_REJECTED, 1)?;
                Ok(1)
            }
        }
    }

    fn encode_query(&self, query: &NotaryHead, buf: &mut [u8]) -> Result<usize, &'static str> {
        let body = body(buf, KIND_RESP_HEAD, HEAD_BODY_LEN)?;
        body[1..9].copy_from_slice(&query.entries.to_le_bytes());
        body[9..].copy_from_slice(&query.head);
        Ok(HEAD_BODY_LEN)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx_at(now_ns: u64) -> ApplyCtx {
        ApplyCtx {
            now_ns,
            key_hash: 0,
        }
    }

    fn ctx() -> ApplyCtx {
        ctx_at(0)
    }

    /// A distinct, deterministic leaf per `n`.
    fn leaf(n: u8) -> [u8; LEAF_LEN] {
        [n; LEAF_LEN]
    }

    /// Fold a sequence of leaves into a fresh notary and return the head.
    fn head_after(leaves: &[[u8; LEAF_LEN]]) -> [u8; HEAD_LEN] {
        let mut app = Notary::default();
        let mut out = Vec::new();
        for l in leaves {
            app.apply(NotaryEvent::Notarize { leaf: *l }, &ctx(), &mut out);
        }
        app.head()
    }

    // --- Event codec ---

    #[test]
    fn event_round_trip() {
        let mut buf = [0u8; 64];

        let event = NotaryEvent::Notarize { leaf: leaf(0xAB) };
        let n = event.encode(&mut buf);
        assert_eq!(n, event.encoded_size(), "encoded_size must be exact");
        assert_eq!(n, 33);
        assert_eq!(NotaryEvent::decode(&buf[..n]).unwrap(), event);

        let n = NotaryEvent::GetHead.encode(&mut buf);
        assert_eq!(n, 1);
        assert_eq!(
            NotaryEvent::decode(&buf[..n]).unwrap(),
            NotaryEvent::GetHead
        );
    }

    #[test]
    fn encoded_size_never_exceeds_the_declared_bound() {
        for event in [
            NotaryEvent::Notarize { leaf: leaf(1) },
            NotaryEvent::GetHead,
        ] {
            assert!(event.encoded_size() <= NotaryEvent::MAX_ENCODED_SIZE);
        }
    }

    #[test]
    fn event_decode_rejects_empty_and_unknown() {
        assert!(matches!(
            NotaryEvent::decode(&[]),
            Err(CodecError::Truncated)
        ));
        assert!(matches!(
            NotaryEvent::decode(&[0xFF]),
            Err(CodecError::UnknownTag(0xFF))
        ));
    }

    #[test]
    fn event_decode_rejects_wrong_leaf_width() {
        let mut short = vec![KIND_NOTARIZE];
        short.extend_from_slice(&[0u8; LEAF_LEN - 1]);
        assert!(matches!(
            NotaryEvent::decode(&short),
            Err(CodecError::Truncated)
        ));

        let mut long = vec![KIND_NOTARIZE];
        long.extend_from_slice(&[0u8; LEAF_LEN + 1]);
        assert!(
            matches!(NotaryEvent::decode(&long), Err(CodecError::InvalidField)),
            "an over-long leaf must be refused, not truncated — truncating \
             would commit to something the client did not send"
        );
    }

    #[test]
    fn only_get_head_is_a_query() {
        assert!(NotaryEvent::GetHead.is_query());
        assert!(!NotaryEvent::Notarize { leaf: leaf(1) }.is_query());
    }

    // --- Apply ---

    #[test]
    fn notarize_advances_the_chain() {
        let mut app = Notary::default();
        let mut out = Vec::new();

        app.apply(NotaryEvent::Notarize { leaf: leaf(1) }, &ctx(), &mut out);
        let first = app.head();
        assert_ne!(first, GENESIS_HEAD);
        assert_eq!(app.entries(), 1);
        assert_eq!(
            out[0],
            NotaryReport::Receipt {
                entry: 1,
                timestamp_ns: 0,
                prev: GENESIS_HEAD,
                head: first
            }
        );

        out.clear();
        app.apply(NotaryEvent::Notarize { leaf: leaf(1) }, &ctx(), &mut out);
        assert_ne!(app.head(), first, "the same leaf twice must still advance");
        assert_eq!(app.entries(), 2);
        assert_eq!(
            out[0],
            NotaryReport::Receipt {
                entry: 2,
                timestamp_ns: 0,
                prev: first,
                head: app.head()
            },
            "consecutive receipts must chain: prev is the previous head"
        );
    }

    #[test]
    fn a_receipt_verifies_on_its_own() {
        // Fold some history the verifier knows nothing about, then check
        // the next receipt with only the receipt and the leaf in hand.
        let mut app = Notary::default();
        let mut out = Vec::new();
        for n in 1..=5 {
            app.apply(NotaryEvent::Notarize { leaf: leaf(n) }, &ctx(), &mut out);
        }
        out.clear();

        let mine = leaf(0xC3);
        let at = 1_700_000_000_000_000_000;
        app.apply(NotaryEvent::Notarize { leaf: mine }, &ctx_at(at), &mut out);
        let NotaryReport::Receipt {
            entry,
            timestamp_ns,
            prev,
            head,
        } = out[0]
        else {
            panic!("expected a receipt");
        };
        assert_eq!(entry, 6);
        assert_eq!(timestamp_ns, at, "the receipt carries the dispatch time");
        assert_eq!(fold(&prev, &mine, timestamp_ns), head);
        assert_ne!(
            fold(&prev, &leaf(0xC4), timestamp_ns),
            head,
            "a different leaf must not verify"
        );
        assert_ne!(
            fold(&prev, &mine, timestamp_ns + 1),
            head,
            "a different time must not verify"
        );
    }

    #[test]
    fn the_time_is_part_of_the_commitment() {
        let mut a = Notary::default();
        let mut b = Notary::default();
        let mut out = Vec::new();
        a.apply(
            NotaryEvent::Notarize { leaf: leaf(1) },
            &ctx_at(1),
            &mut out,
        );
        b.apply(
            NotaryEvent::Notarize { leaf: leaf(1) },
            &ctx_at(2),
            &mut out,
        );
        assert_ne!(
            a.head(),
            b.head(),
            "the same leaf sequenced at a different time is a different commitment"
        );
    }

    #[test]
    fn chain_is_deterministic() {
        let ls = [leaf(1), leaf(2), leaf(3)];
        assert_eq!(head_after(&ls), head_after(&ls));
    }

    #[test]
    fn chain_is_order_dependent() {
        assert_ne!(
            head_after(&[leaf(1), leaf(2)]),
            head_after(&[leaf(2), leaf(1)]),
            "a reordering must change the head — this is the property that \
             makes the head a cross-node ordering assertion"
        );
    }

    #[test]
    fn distinct_leaves_give_distinct_heads() {
        assert_ne!(head_after(&[leaf(1)]), head_after(&[leaf(2)]));
    }

    #[test]
    fn get_head_reports_state() {
        let mut app = Notary::default();
        let mut out = Vec::new();
        app.apply(NotaryEvent::Notarize { leaf: leaf(7) }, &ctx(), &mut out);

        let query_ctx = QueryCtx {
            journal_sequence: melin_app::WireSeq::new(0),
            active_connections: 0,
            events_processed: 0,
            key_hash: 0,
        };
        let query = app.query(NotaryEvent::GetHead, &query_ctx).unwrap();
        assert_eq!(query.entries, 1);
        assert_eq!(query.head, app.head());
        assert!(
            app.query(NotaryEvent::Notarize { leaf: leaf(8) }, &query_ctx)
                .is_none(),
            "a notarization is not a query"
        );
    }

    #[test]
    fn build_reject() {
        let event = NotaryEvent::Notarize { leaf: leaf(1) };
        let report = Notary::build_reject(&event, RejectReason::ReplicaDisconnected);
        assert_eq!(report, NotaryReport::Rejected);
    }

    // --- Snapshot ---

    #[test]
    fn snapshot_restore_round_trip() {
        let mut app = Notary::default();
        let mut out = Vec::new();
        for n in 1..=3 {
            app.apply(NotaryEvent::Notarize { leaf: leaf(n) }, &ctx(), &mut out);
        }

        let mut buf = Vec::new();
        app.snapshot(&mut buf).unwrap();
        assert_eq!(buf.len(), HEAD_LEN + 8);

        let restored = Notary::restore(&mut &buf[..]).unwrap();
        assert_eq!(restored.head(), app.head());
        assert_eq!(restored.entries(), app.entries());
    }

    #[test]
    fn restore_rejects_truncated_snapshot() {
        let app = Notary::default();
        let mut buf = Vec::new();
        app.snapshot(&mut buf).unwrap();
        buf.truncate(buf.len() - 1);
        assert!(Notary::restore(&mut &buf[..]).is_err());
    }

    // --- Decoder ---

    /// A request body as a client sends it: the kind, then its fields.
    fn request(kind: u8, fields: &[u8]) -> Vec<u8> {
        [&[kind][..], fields].concat()
    }

    #[test]
    fn decoder_accepts_notarize_from_writing_roles() {
        let l = leaf(0x5A);
        for permission in [
            Permission::Operator,
            Permission::Trader,
            Permission::Custodian,
        ] {
            match RequestDecoder.decode(&request(KIND_NOTARIZE, &l), permission) {
                Decoded::Permitted(event) => assert_eq!(event, NotaryEvent::Notarize { leaf: l }),
                _ => panic!("expected Permitted for {permission:?}"),
            }
        }
    }

    #[test]
    fn decoder_denies_notarize_from_the_read_only_role() {
        assert!(matches!(
            RequestDecoder.decode(&request(KIND_NOTARIZE, &leaf(1)), Permission::ReadOnly),
            Decoded::PermissionDenied(_)
        ));
    }

    /// Every role a client can connect with; replication keys are refused
    /// at the handshake and never reach the decoder.
    #[test]
    fn decoder_allows_queries_from_every_client_role() {
        for permission in [
            Permission::Operator,
            Permission::Trader,
            Permission::Custodian,
            Permission::ReadOnly,
        ] {
            assert!(matches!(
                RequestDecoder.decode(&[KIND_GET_HEAD], permission),
                Decoded::Permitted(NotaryEvent::GetHead)
            ));
        }
    }

    #[test]
    fn decoder_rejects_wrong_leaf_width() {
        for fields in [vec![0u8; LEAF_LEN - 1], vec![0u8; LEAF_LEN + 1], Vec::new()] {
            assert!(
                matches!(
                    RequestDecoder.decode(&request(KIND_NOTARIZE, &fields), Permission::Trader),
                    Decoded::DecodeError(_)
                ),
                "a {}-byte leaf must be refused",
                fields.len()
            );
        }
    }

    #[test]
    fn decoder_rejects_empty_and_unknown_kind() {
        assert!(matches!(
            RequestDecoder.decode(&[], Permission::Trader),
            Decoded::DecodeError("empty request")
        ));
        assert!(matches!(
            RequestDecoder.decode(&[0x7F], Permission::Trader),
            Decoded::DecodeError("unknown kind")
        ));
    }

    // --- Encoder ---

    #[test]
    fn encoder_receipt() {
        let mut buf = [0u8; 128];
        let prev = [0xCDu8; HEAD_LEN];
        let head = [0xABu8; HEAD_LEN];
        let len = ResponseEncoder
            .encode_report(
                &NotaryReport::Receipt {
                    entry: 7,
                    timestamp_ns: 9,
                    prev,
                    head,
                },
                &mut buf,
            )
            .unwrap();
        assert_eq!(len, 81);
        assert_eq!(buf[0], KIND_RESP_RECEIPT);
        assert_eq!(u64::from_le_bytes(buf[1..9].try_into().unwrap()), 7);
        assert_eq!(u64::from_le_bytes(buf[9..17].try_into().unwrap()), 9);
        assert_eq!(&buf[17..49], &prev);
        assert_eq!(&buf[49..81], &head);
    }

    #[test]
    fn encoder_rejected() {
        let mut buf = [0u8; 64];
        let len = ResponseEncoder
            .encode_report(&NotaryReport::Rejected, &mut buf)
            .unwrap();
        assert_eq!(buf[..len], [KIND_RESP_REJECTED]);
    }

    #[test]
    fn encoder_head_query() {
        let mut buf = [0u8; 64];
        let head = [0x5Au8; HEAD_LEN];
        let len = ResponseEncoder
            .encode_query(&NotaryHead { entries: 3, head }, &mut buf)
            .unwrap();
        assert_eq!(len, 41);
        assert_eq!(buf[0], KIND_RESP_HEAD);
        assert_eq!(u64::from_le_bytes(buf[1..9].try_into().unwrap()), 3);
        assert_eq!(&buf[9..41], &head);
    }

    #[test]
    fn encoder_reports_buffer_too_small() {
        let mut small = [0u8; 4];
        assert!(
            ResponseEncoder
                .encode_report(
                    &NotaryReport::Receipt {
                        entry: 1,
                        timestamp_ns: 0,
                        prev: GENESIS_HEAD,
                        head: GENESIS_HEAD
                    },
                    &mut small
                )
                .is_err()
        );
        assert!(
            ResponseEncoder
                .encode_query(
                    &NotaryHead {
                        entries: 0,
                        head: GENESIS_HEAD
                    },
                    &mut [0u8; HEAD_BODY_LEN - 1]
                )
                .is_err()
        );
        // A rejection is its kind alone, which still needs a byte.
        assert!(
            ResponseEncoder
                .encode_report(&NotaryReport::Rejected, &mut [])
                .is_err()
        );
    }
}
