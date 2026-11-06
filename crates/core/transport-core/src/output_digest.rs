//! Output digests: what every copy of the application produced, folded
//! into fixed windows of positions and chained, so two copies can compare
//! their output without exchanging it.
//!
//! The journal's hash chain proves that every node holds the same input.
//! Nothing in it proves that the same input produced the same output: a
//! nondeterministic `apply`, or a `snapshot`/`restore` pair that loses a
//! field, leaves every journal identical while the copies answer
//! differently. This module holds the building blocks of the check that
//! catches it (plan: `docs/internal/divergence-detection-2026-10.md`):
//!
//! - [`encode_app_body`], the one place an application body is encoded:
//!   the runtime's zeroing, the debug builds' purity check, the encoder's
//!   call and the bound check. A client's frame and a digested report go
//!   through it alike, so a copy that sends a report and one that only
//!   digests it see the same bytes.
//! - [`WindowFolder`], which digests the reports of each window of
//!   [`OUTPUT_DIGEST_WINDOW`] positions and chains the window digests.
//! - The digest board ([`digest_board`]), where a folder publishes each
//!   window it closes for readers on other threads.
//!
//! Windows are absolute positions: window `m` holds the reports of the
//! entries at `wire_seq` `(m-1)·K + 1 ..= m·K` and is named by its end
//! `m·K`, so every copy cuts the same grid with no coordination.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use melin_app::WireSeq;
use melin_pipeline::seqlock::{self, NoPadding, SeqLockReader, SeqLockWriter};
use twox_hash::xxhash3_128::{self, DEFAULT_SECRET_LENGTH, RawHasher, SecretBuffer};

use crate::cursors::AppliedWireSeqCursor;
use crate::pipeline::{OutputPayload, OutputSlot, RingCapacity};

/// `K`: the number of positions in one window. A constant of
/// [`DigestKind::Output`]'s definition, so every copy of every build cuts
/// the same windows; changing it is a new kind, never a new value of this
/// one.
///
/// 64 sits between two costs. Each window costs a fresh hasher, a
/// finalize, a chain hash and a board store on every copy, and a read and
/// a chain hash where it is compared: divided by 64 that is a small share
/// of the per-event budget, and the board stays a few megabytes at the
/// default ring capacity ([`BoardSize::for_pipeline`]). A larger window
/// delays detection on an idle node, which closes a window only every `K`
/// ticks. `u64` because it divides positions, which are `u64`.
pub const OUTPUT_DIGEST_WINDOW: u64 = 64;

/// The chain value before a folder's first window: 16 zero bytes. Every
/// folder starts from it, so two folders started at the same position
/// chain to the same values.
pub const CHAIN_SEED: ChainValue = ChainValue([0; DIGEST_LEN]);

/// What a window digest's input starts with, ahead of the kind, `K` and
/// the window's end: names the definition, so a digest of another layout
/// can never equal one of this.
const DOMAIN: &[u8] = b"melin output digest v1";

/// Bytes in a [`WindowDigest`] and a [`ChainValue`]: XXH3-128's output.
pub const DIGEST_LEN: usize = 16;

/// What a digest covers. Exhaustive, with an explicit byte per kind, so a
/// new kind breaks every `match` until its handling is decided, and the
/// byte a frame carries can never be renumbered by reordering variants.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DigestKind {
    /// The bytes of every report the application's encoder produced for a
    /// journaled entry, in windows of [`OUTPUT_DIGEST_WINDOW`].
    Output = 1,
}

impl DigestKind {
    /// The kind's byte, as a digest's input and a frame carry it.
    pub const fn byte(self) -> u8 {
        self as u8
    }

    /// The kind `byte` names, or `None` for one this build does not know
    /// (0 included, which names none).
    pub const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Output),
            _ => None,
        }
    }
}

/// The digest of one window's reports. A newtype over the bytes, distinct
/// from [`ChainValue`], so one can never be compared or chained as the
/// other. Bytes rather than a `u128`: no byte-order question, and
/// alignment 1 keeps the board's entry free of padding.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct WindowDigest([u8; DIGEST_LEN]);

impl WindowDigest {
    /// The digest's bytes: XXH3-128's canonical (big-endian) form.
    pub const fn as_bytes(&self) -> &[u8; DIGEST_LEN] {
        &self.0
    }
}

/// The chain over every window a folder has closed:
/// `C_i = XXH3-128(C_{i-1} || d_i)` from [`CHAIN_SEED`]. One equal chain
/// value vouches for every window since the chain's start, so a reader
/// that sees only some windows loses delay, never coverage. A newtype for
/// the same reasons as [`WindowDigest`].
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ChainValue([u8; DIGEST_LEN]);

impl ChainValue {
    /// A chain value from its bytes, as a frame carries it.
    pub const fn from_bytes(bytes: [u8; DIGEST_LEN]) -> Self {
        Self(bytes)
    }

    /// The value's bytes: XXH3-128's canonical (big-endian) form.
    pub const fn as_bytes(&self) -> &[u8; DIGEST_LEN] {
        &self.0
    }

    /// The chain one window further on: `XXH3-128(self || digest)`, one
    /// one-shot hash over 32 bytes.
    pub fn extend(self, digest: WindowDigest) -> Self {
        let mut input = [0u8; 2 * DIGEST_LEN];
        input[..DIGEST_LEN].copy_from_slice(&self.0);
        input[DIGEST_LEN..].copy_from_slice(&digest.0);
        Self(xxhash3_128::Hasher::oneshot(&input).to_be_bytes())
    }
}

/// Hex, so a log line or a failed assertion shows the bytes compared.
impl std::fmt::Debug for WindowDigest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write_hex(f, &self.0)
    }
}

/// Hex, as [`WindowDigest`]'s.
impl std::fmt::Debug for ChainValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write_hex(f, &self.0)
    }
}

fn write_hex(f: &mut std::fmt::Formatter<'_>, bytes: &[u8]) -> std::fmt::Result {
    bytes.iter().try_for_each(|b| write!(f, "{b:02x}"))
}

/// The end of the window holding `position`: the next multiple of
/// [`OUTPUT_DIGEST_WINDOW`] at or above it, 0 for position 0.
const fn window_end_of(position: u64) -> u64 {
    position.div_ceil(OUTPUT_DIGEST_WINDOW) * OUTPUT_DIGEST_WINDOW
}

// ---------------------------------------------------------------------------
// The body encode
// ---------------------------------------------------------------------------

/// Bound on one application response body: what a `ResponseEncoder` may
/// write for a single report or query response.
///
/// The response stages encode into a stack buffer of this size behind the
/// frame header. An application whose encoder needs more does not get a
/// larger buffer: the encode fails, the reply is dropped, and the failure
/// is logged at `error!`. So an application should check its widest body
/// against this bound at compile time rather than discover it under load.
/// The runtime re-exports it for that reason.
pub const MAX_RESPONSE_BODY: usize = 512;

/// The region one application body is encoded into. An array rather than
/// a `Vec`: fixed size, on the stack, no allocation on the hot path.
pub type BodyBuf = [u8; MAX_RESPONSE_BODY];

/// Why [`encode_app_body`] produced no body: the encoder's own error, or a
/// length past the region, which is reported the same way.
pub type EncodeError = &'static str;

/// Encode one application body into `body`: zero the region, run
/// `encode` on it, and refuse a length past it. Returns the body's length.
///
/// Every application body is encoded here, whichever path sends or
/// digests it, so the zeroing and, in debug builds, the purity check
/// cover them all and no caller can forget them.
///
/// A length past the region is the encoder's bug and comes back as `Err`:
/// the caller logs it and drops the response, rather than send a frame
/// that would desync the client's framing.
///
/// # Panics
///
/// In debug builds only, if `encode` is impure: it is encoded twice more
/// first, the region filled with a different pattern each time, and a
/// difference in the result or below the returned length panics. A byte
/// the encoder skips shows the pattern, and output that depends on the
/// encoder's own state differs between the two.
#[inline]
pub fn encode_app_body(
    body: &mut BodyBuf,
    encode: impl Fn(&mut [u8]) -> Result<usize, EncodeError>,
) -> Result<usize, EncodeError> {
    // Triples the encoder calls, in debug builds only; release builds pay
    // nothing for it.
    #[cfg(debug_assertions)]
    check_encoder_purity(body, &encode);
    encode_zeroed(body, encode)
}

/// [`encode_app_body`] without the purity check: the zeroing, the encode
/// and the bound check, which is everything a release build does.
#[inline]
fn encode_zeroed(
    body: &mut BodyBuf,
    encode: impl FnOnce(&mut [u8]) -> Result<usize, EncodeError>,
) -> Result<usize, EncodeError> {
    // A release build cannot tell whether an encoder wrote every byte
    // below the length it returns, and an encoder may use the bytes past
    // it as scratch. The region is reused for every response, so a byte
    // it skips would otherwise carry an earlier body, possibly another
    // client's, and on another copy a different one. Only the whole region
    // guarantees no byte of an earlier body survives: zeroing up to the
    // previous length, or a high-water mark of returned lengths, misses
    // the scratch bytes, which resurface when a later encode skips one.
    // The region is reused, so it stays in L1 and this is one short memset
    // per response.
    body.fill(0);
    let len = encode(body)?;
    if len > MAX_RESPONSE_BODY {
        return Err("encoder reported a body longer than its buffer");
    }
    Ok(len)
}

/// What [`check_encoder_purity`]'s panic message starts with.
#[cfg(debug_assertions)]
const IMPURE_ENCODER: &str = "impure response encoder";

/// The two patterns [`check_encoder_purity`] fills the region with before
/// its two encodes: different at every bit, so a byte an encoder skips
/// differs between the two whatever its position.
#[cfg(debug_assertions)]
const POISON: [u8; 2] = [0xA5, 0x5A];

/// Debug builds' check that `encode` is pure: encode twice, the region
/// filled with a different [`POISON`] pattern each time, and panic unless
/// both return the same `Ok` length with the same bytes below it, or the
/// same `Err`. That catches an encoder that skips a byte below the length
/// it returns (it shows the poison) and one whose output depends on
/// something other than its input, such as its own state.
///
/// A panic, not an error: an impure encoder is a bug in the application.
/// In production the zeroing makes a skipped byte harmless, but not a
/// body that depends on the encoder's state: copies of one report then
/// differ from node to node, which the output digest turns into a stopped
/// cluster. And the skipped byte is still a bug, in a field the
/// application meant to write. Panicking in the tests that encode it
/// finds the bug where it is cheap to fix. Bytes past the returned length
/// are not compared: an encoder may use them as scratch.
#[cfg(debug_assertions)]
fn check_encoder_purity(
    body: &mut BodyBuf,
    encode: &impl Fn(&mut [u8]) -> Result<usize, EncodeError>,
) {
    const CONTRACT: &str = "an encoder must write every byte below the length it returns, \
                            and its output must depend only on the value it encodes";
    body.fill(POISON[0]);
    let first = encode(body);
    // An array, not a `Vec`: one body, its size fixed, no allocation.
    let first_body: BodyBuf = *body;
    body.fill(POISON[1]);
    let second = encode(body);
    if first != second {
        panic!(
            "{IMPURE_ENCODER}: two encodes of one value returned {first:?}, then {second:?}; \
             {CONTRACT}"
        );
    }
    // A length past the region is refused by the caller; compare what
    // lies within it.
    let Ok(len) = first.map(|len| len.min(MAX_RESPONSE_BODY)) else {
        return;
    };
    if let Some(at) = (0..len).find(|&i| first_body[i] != body[i]) {
        panic!(
            "{IMPURE_ENCODER}: body byte {at} of {len} differs between two encodes of one \
             value ({:#04x} over {:#04x}, then {:#04x} over {:#04x}); {CONTRACT}",
            first_body[at], POISON[0], body[at], POISON[1],
        );
    }
}

// ---------------------------------------------------------------------------
// The folder
// ---------------------------------------------------------------------------

/// The streaming XXH3-128 state of one window. `RawHasher` over the
/// default secret rather than the crate's `Hasher`, which boxes its
/// secret: a plain value with no heap, made fresh for every window (it
/// has no reset), so the response thread never allocates.
type WindowHasher = RawHasher<&'static [u8; DEFAULT_SECRET_LENGTH]>;

/// A fresh hasher for the window ending at `end`, its definition's header
/// already written: the domain, the kind, `K` and the window's end.
fn window_hasher(end: u64) -> WindowHasher {
    let mut hasher = RawHasher::new(SecretBuffer::default());
    hasher.write(DOMAIN);
    hasher.write(&[DigestKind::Output.byte()]);
    hasher.write(&OUTPUT_DIGEST_WINDOW.to_le_bytes());
    hasher.write(&end.to_le_bytes());
    hasher
}

/// One report's body as the folder takes it: the bytes the encoder wrote,
/// or the fact that it wrote none.
#[derive(Clone, Copy, Debug)]
pub enum ReportBody<'a> {
    /// What [`encode_app_body`] returned `Ok` for: the body, exactly as
    /// the encoder wrote it.
    Encoded(&'a [u8]),
    /// The encoder's `Err`, or a body over [`MAX_RESPONSE_BODY`]. Folded
    /// as a length of `u32::MAX` and no bytes: copies that fail alike
    /// agree.
    Failed,
}

/// `F`, the end of the first window a copy whose pipeline applies its
/// first live entry at `first_live` digests whole: the one value its
/// [`WindowFolder`] and its [`DigestComparator`] must agree on.
pub fn first_window_end(first_live: WireSeq) -> WireSeq {
    // Position 0 is never allocated; a pipeline starting there starts at
    // 1, whose window is whole.
    let s = first_live.get().max(1);
    WireSeq::new(window_end_of(s - 1) + OUTPUT_DIGEST_WINDOW)
}

/// Digests the reports one copy of the application produces, a window at
/// a time, chains the window digests, and publishes each closed window to
/// the digest board.
///
/// For each window `W`:
///
/// ```text
/// d(W) = XXH3-128 (seed 0, default secret) over
///     "melin output digest v1" || kind (u8 = 1) || K (u64 LE) || end (u64 LE)
///     then, for every report whose position is in W, in output order:
///         wire_seq (u64 LE) || body_len (u32 LE) || body
/// ```
///
/// **A folder digests only windows it saw whole.** Started at the first
/// position its pipeline applies live, `s`, its first window is the one
/// ending at `F = (ceil((s-1)/K) + 1)·K`, the first that starts at or
/// after `s`; reports in the window before it are skipped, since the
/// copy did not see that window's earlier positions. So two folders
/// started at different positions agree on every window both saw whole.
///
/// The folder is driven in position order: [`fold`](Self::fold) each
/// report, and [`close_through`](Self::close_through) every window the
/// copy has applied past. A window closes once a position beyond it is
/// seen, by either call, so a window with no report closes with its
/// header alone. Nothing allocates after construction.
pub struct WindowFolder {
    /// `F`: the end of the first window this folder digests.
    first_end: u64,
    /// The end of the window being folded.
    open_end: u64,
    /// The open window's hash so far.
    hasher: WindowHasher,
    /// The chain through the last window closed.
    chain: ChainValue,
    /// Where each closed window goes.
    board: DigestBoardWriter,
}

impl WindowFolder {
    /// A folder whose pipeline applies its first live entry at
    /// `first_live`, publishing to `board`.
    pub fn new(first_live: WireSeq, board: DigestBoardWriter) -> Self {
        let first_end = first_window_end(first_live).get();
        Self {
            first_end,
            open_end: first_end,
            hasher: window_hasher(first_end),
            chain: CHAIN_SEED,
            board,
        }
    }

    /// `F`, the end of the first window this folder digests.
    pub fn first_window_end(&self) -> WireSeq {
        WireSeq::new(self.first_end)
    }

    /// Fold the report the entry at `position` produced, closing every
    /// window before the one it falls in. Reports of one entry, and of
    /// successive entries, must come in output order; a report in a
    /// window already closed is the caller's bug.
    #[inline]
    pub fn fold(&mut self, position: WireSeq, body: ReportBody<'_>) {
        let position = position.get();
        if position <= self.first_end - OUTPUT_DIGEST_WINDOW {
            // Before the first whole window: not digested.
            return;
        }
        debug_assert!(
            position > self.open_end - OUTPUT_DIGEST_WINDOW,
            "a report at {position} after the window ending at {} closed",
            self.open_end - OUTPUT_DIGEST_WINDOW,
        );
        if position > self.open_end {
            self.close_through(WireSeq::new(window_end_of(position) - OUTPUT_DIGEST_WINDOW));
        }
        // The position and the length in one write: the hasher's per-call
        // overhead is paid twice per report, not three times.
        let mut prefix = [0u8; 12];
        prefix[..8].copy_from_slice(&position.to_le_bytes());
        match body {
            ReportBody::Encoded(bytes) => {
                // Lossless: a body is at most `MAX_RESPONSE_BODY` bytes.
                debug_assert!(
                    bytes.len() <= MAX_RESPONSE_BODY,
                    "a {}-byte body",
                    bytes.len()
                );
                prefix[8..].copy_from_slice(&(bytes.len() as u32).to_le_bytes());
                self.hasher.write(&prefix);
                self.hasher.write(bytes);
            }
            ReportBody::Failed => {
                prefix[8..].copy_from_slice(&u32::MAX.to_le_bytes());
                self.hasher.write(&prefix);
            }
        }
    }

    /// Close every window ending at or below `applied`: the copy has
    /// applied every entry through it, so no further report can fall in
    /// those windows. Windows before the first whole one are not closed,
    /// since they were never opened.
    #[inline]
    pub fn close_through(&mut self, applied: WireSeq) {
        while self.open_end <= applied.get() {
            self.close_open();
        }
    }

    /// Finish the open window, extend the chain, publish both, and open
    /// the next window.
    fn close_open(&mut self) {
        let next_end = self.open_end + OUTPUT_DIGEST_WINDOW;
        let closed = std::mem::replace(&mut self.hasher, window_hasher(next_end));
        let digest = WindowDigest(closed.finish_128().to_be_bytes());
        self.chain = self.chain.extend(digest);
        self.board.publish(ClosedWindow {
            end: WireSeq::new(self.open_end),
            digest,
            chain: self.chain,
        });
        self.open_end = next_end;
    }
}

// ---------------------------------------------------------------------------
// The digester: the folder driven by the output ring
// ---------------------------------------------------------------------------

/// A [`WindowFolder`] driven by an output ring's consumer: the rules for
/// what each slot, and an empty ring, tells the folder. The primary's
/// response stage and a replica's drain both go through it, so the two
/// copies cannot close windows by different rules.
///
/// Per iteration of its consumer's loop:
///
/// 1. [`load_applied`](Self::load_applied), *before* the ring is read;
/// 2. read the ring; if it is empty, [`ring_empty`](Self::ring_empty) with
///    what step 1 loaded;
/// 3. otherwise, for each slot, [`slot`](Self::slot), and for a report,
///    [`fold`](Self::fold) its body.
pub struct OutputDigester {
    folder: WindowFolder,
    applied: AppliedWireSeqCursor,
}

/// How far the matching stage had applied, loaded before a ring read.
/// Only [`OutputDigester::load_applied`] makes one and only
/// [`OutputDigester::ring_empty`] takes one, so the load cannot be taken
/// after the read it must precede without the type showing it.
#[must_use]
pub struct AppliedBeforeRead(WireSeq);

/// A report the digester expects the body of: made by
/// [`OutputDigester::slot`], consumed by [`OutputDigester::fold`]. Holds
/// the report, for a caller that has not encoded it yet.
#[must_use = "a report's body must be folded"]
pub struct ReportAt<'s, R> {
    position: WireSeq,
    report: &'s R,
}

impl<R> ReportAt<'_, R> {
    /// The report to encode.
    pub fn report(&self) -> &R {
        self.report
    }
}

impl<'a> ReportBody<'a> {
    /// A report's body from what an encode into `region` returned: the
    /// bytes it wrote, or `Failed` for an error. The one mapping from an
    /// encode's result to what the folder takes, for every copy.
    #[inline]
    pub fn of(region: &'a [u8], encoded: Result<usize, EncodeError>) -> Self {
        match encoded {
            Ok(len) => Self::Encoded(&region[..len]),
            Err(_) => Self::Failed,
        }
    }
}

impl OutputDigester {
    /// A digester for a pipeline whose first live entry is at
    /// `first_live`, closing windows the matching stage reports applied
    /// through `applied`, publishing to `board`.
    pub fn new(
        first_live: WireSeq,
        applied: AppliedWireSeqCursor,
        board: DigestBoardWriter,
    ) -> Self {
        Self {
            folder: WindowFolder::new(first_live, board),
            applied,
        }
    }

    /// How far the matching stage has applied, with every slot of those
    /// entries published. Load it before reading the ring.
    #[inline]
    pub fn load_applied(&self) -> AppliedBeforeRead {
        AppliedBeforeRead(self.applied.load())
    }

    /// The ring read after `applied` was loaded came back empty: every
    /// slot of every entry through it has been read, so every window
    /// ending there closes, those whose entries published no slot
    /// included.
    #[inline]
    pub fn ring_empty(&mut self, applied: AppliedBeforeRead) {
        self.folder.close_through(applied.0);
    }

    /// A slot was read: close what lies behind it, and for a report,
    /// return what [`fold`](Self::fold) needs. The ring is in position
    /// order, so a slot at `w` means every entry before `w` has published
    /// all its slots. A query response is stamped with the last position
    /// allocated before it, already applied whole, so it closes through
    /// its own position.
    #[inline]
    pub fn slot<'s, R: Copy, Q: Copy>(
        &mut self,
        slot: &'s OutputSlot<R, Q>,
    ) -> Option<ReportAt<'s, R>> {
        let position = WireSeq::new(slot.wire_seq);
        match &slot.payload {
            // `fold` closes the windows before the report's own.
            OutputPayload::Report(report) => Some(ReportAt { position, report }),
            OutputPayload::QueryResponse(_) => {
                self.folder.close_through(position);
                None
            }
            OutputPayload::BatchEnd | OutputPayload::AckPolicyChange(_) => {
                self.folder
                    .close_through(WireSeq::new(slot.wire_seq.saturating_sub(1)));
                None
            }
        }
    }

    /// Fold the body of the report [`slot`](Self::slot) returned `at` for.
    #[inline]
    pub fn fold<R>(&mut self, at: ReportAt<'_, R>, body: ReportBody<'_>) {
        self.folder.fold(at.position, body);
    }

    /// The digester for a pipeline that applies its first live entry at
    /// `first_live`, its rings `capacity` slots each: over the pipeline's
    /// applied cursor, publishing to a board sized for it, whose reader is
    /// returned beside it.
    pub fn for_pipeline(
        first_live: WireSeq,
        cursors: &crate::cursors::PipelineCursors,
        capacity: RingCapacity,
    ) -> (Self, DigestBoardReader) {
        let (writer, reader) = digest_board(BoardSize::for_pipeline(capacity));
        (
            Self::new(first_live, cursors.applied_wire_seq(), writer),
            reader,
        )
    }
}

// ---------------------------------------------------------------------------
// A divergence, and what stops the node on one
// ---------------------------------------------------------------------------

/// Two copies' chains disagreed: what a comparison found, as the stop logs
/// it and the marker records it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Divergence {
    /// The window end the two chains were compared at.
    pub end: WireSeq,
    /// The first position of the comparison run the mismatch ended.
    pub run_from: WireSeq,
    /// The last window end the run found equal, if any.
    pub last_match: Option<WireSeq>,
    /// The other copy's chain value at `end`.
    pub expected: ChainValue,
    /// This copy's chain value at `end`.
    pub actual: ChainValue,
}

impl Divergence {
    /// The positions known to hold the first differing report, both ends
    /// included: everything after the last window found equal (or from the
    /// run's start), through `end`.
    pub fn positions(&self) -> (WireSeq, WireSeq) {
        let from = match self.last_match {
            Some(matched) => WireSeq::new(matched.get() + 1),
            None => self.run_from,
        };
        (from, self.end)
    }
}

/// What a comparator stops the node with, before it reports a mismatch.
///
/// A comparator holds its node's stop and trips it itself before it
/// returns a mismatch, so no caller can forget to: Rust has no linear
/// types, and a `#[must_use]` verdict is only a lint. Generic rather than
/// `dyn`: one implementation per build, monomorphized; tests use a
/// recording one.
pub trait Trip {
    /// What the first trip returns: proof that this caller stopped the
    /// node, for whatever it must then do once (the notice to the others).
    type Token;

    /// Stop the node for `divergence`. `None` when it was already stopped,
    /// by this cause or another.
    fn trip(&self, divergence: Divergence) -> Option<Self::Token>;
}

// ---------------------------------------------------------------------------
// The comparator
// ---------------------------------------------------------------------------

/// What one step of a [`DigestComparator`] found.
#[derive(Debug)]
pub struct Comparison<Token> {
    /// Comparisons that found the two chains equal.
    pub matched: u32,
    /// A run was anchored at this window end.
    pub anchored: Option<WireSeq>,
    /// The run broke: a local window between this end and the next one
    /// was lost before it was read.
    pub broke_after: Option<WireSeq>,
    /// The chains differed. The comparator has tripped its stop, and is
    /// spent; `halted` is what the trip returned (`None` when the node had
    /// already stopped).
    pub mismatch: Option<(Divergence, Option<Token>)>,
}

/// Nothing found: by hand, so the token needs no `Default` (a trip's
/// token, `Halted`, has none on purpose).
impl<Token> Default for Comparison<Token> {
    fn default() -> Self {
        Self {
            matched: 0,
            anchored: None,
            broke_after: None,
            mismatch: None,
        }
    }
}

/// One of this copy's closed windows, as the comparator holds it.
#[derive(Clone, Copy)]
struct LocalWindow {
    /// 0 for a slot never filled.
    end: u64,
    digest: WindowDigest,
    /// This copy's window digests chained from the run's anchor: `Some`
    /// for a window the run has reached.
    chain: Option<ChainValue>,
}

/// One of the primary's chain values, waiting for this copy to reach its
/// window.
#[derive(Clone, Copy)]
struct ExpectedWindow {
    /// 0 for a slot never filled.
    end: u64,
    chain: ChainValue,
}

/// The run being verified: this copy's windows chained from the primary's
/// chain value at the anchor.
#[derive(Clone, Copy)]
struct Run {
    /// The first position the run covers: one past the anchor.
    from: u64,
    /// The newest window end the run has chained to.
    extended_to: u64,
    /// This copy's chain at `extended_to`.
    chain: ChainValue,
    last_match: Option<u64>,
}

/// Compares this copy's output digests with the primary's, run by run.
///
/// Two copies' chains start wherever each copy's pipeline started, so they
/// cannot be compared directly. A run anchors on the primary's chain value
/// at some window end `e` (taken on trust) and extends it with this copy's
/// own window digests: a later primary value at `e'` is compared with this
/// copy's chain there, and a match verifies every window in between. Each
/// replication session starts a new run (`begin_session`): the replica
/// cannot tell a reconnect from a primary that restarted.
///
/// Fed this copy's closed windows in order ([`push_local`](Self::push_local),
/// or from its board with [`absorb`](Self::absorb)) and the primary's
/// values as they arrive ([`push_expected`](Self::push_expected)). Either
/// side may arrive first: values ahead of this copy wait. Both sides are
/// fixed arrays of the board's size, so nothing allocates after
/// construction; an entry evicted before its use costs delay, never a
/// wrong verdict.
///
/// It holds the node's stop and trips it before it reports a mismatch
/// (see [`Trip`]); after one, it is spent.
pub struct DigestComparator<T: Trip> {
    trip: T,
    size: BoardSize,
    /// `F`: this copy's first window end.
    first_end: u64,
    /// Where an anchor's next window must be at or after, so the run
    /// never spans a lost local window: `F - K` until a gap, then the
    /// window before the first one after the gap.
    floor: u64,
    local: Box<[LocalWindow]>,
    expected: Box<[ExpectedWindow]>,
    last_local: Option<u64>,
    run: Option<Run>,
    spent: bool,
}

impl<T: Trip> DigestComparator<T> {
    /// A comparator for a copy whose first window ends at `first_end`
    /// ([`WindowFolder::first_window_end`] of the folder whose board it
    /// absorbs), holding windows as its board of `size` does, tripping
    /// `trip` on a mismatch. A `first_end` below the folder's would find
    /// that window never closing, and absorb nothing.
    pub fn new(trip: T, size: BoardSize, first_end: WireSeq) -> Self {
        let empty_local = LocalWindow {
            end: 0,
            digest: WindowDigest([0; DIGEST_LEN]),
            chain: None,
        };
        let empty_expected = ExpectedWindow {
            end: 0,
            chain: CHAIN_SEED,
        };
        Self {
            trip,
            size,
            first_end: first_end.get(),
            floor: first_end.get() - OUTPUT_DIGEST_WINDOW,
            local: vec![empty_local; size.slots()].into_boxed_slice(),
            expected: vec![empty_expected; size.slots()].into_boxed_slice(),
            last_local: None,
            run: None,
            spent: false,
        }
    }

    /// Whether a mismatch has spent the comparator.
    pub fn is_spent(&self) -> bool {
        self.spent
    }

    /// A new replication session: the run ends and the primary's waiting
    /// values are dropped. This copy's windows are kept, for the next
    /// anchor to extend over.
    pub fn begin_session(&mut self) {
        self.run = None;
        for slot in self.expected.iter_mut() {
            slot.end = 0;
        }
    }

    fn slot(&self, end: u64) -> usize {
        self.size.index(WireSeq::new(end))
    }

    /// This copy's window ending at `end`, if still held.
    fn held(&self, end: u64) -> Option<LocalWindow> {
        let window = self.local[self.slot(end)];
        (window.end == end).then_some(window)
    }

    /// Copy every window this copy's board has closed since the last call,
    /// in order. A window overwritten before it was read is a gap.
    pub fn absorb(&mut self, board: &DigestBoardReader) -> Comparison<T::Token> {
        match board.latest() {
            Some(latest) => self.absorb_through(board, latest),
            None => Comparison::default(),
        }
    }

    /// [`absorb`](Self::absorb) up to `latest`, the board's newest end as
    /// loaded before the reads: the board may have moved on, even lapped
    /// `latest`, by the time a window is read.
    fn absorb_through(
        &mut self,
        board: &DigestBoardReader,
        latest: WireSeq,
    ) -> Comparison<T::Token> {
        let mut found = Comparison::default();
        let mut next = match self.last_local {
            Some(last) => last + OUTPUT_DIGEST_WINDOW,
            None => self.first_end,
        };
        while next <= latest.get() && !self.spent {
            match board.get(WireSeq::new(next)) {
                BoardLookup::Closed(window) => {
                    let step = self.push_local(window);
                    Self::merge(&mut found, step);
                    next += OUTPUT_DIGEST_WINDOW;
                }
                // The newest loaded was itself overwritten since: the board
                // lapped this read. Stop here, so the loop is bounded; the
                // next call starts from the board's new latest, and the
                // windows lost in between surface there as a gap.
                BoardLookup::Overwritten if next >= latest.get() => break,
                // Lost before it was read: resume at the newest, which is
                // a gap from what was last held.
                BoardLookup::Overwritten => next = latest.get(),
                // Not yet visible, though the latest is: try next time.
                BoardLookup::NotYet => break,
            }
        }
        found
    }

    fn merge(into: &mut Comparison<T::Token>, from: Comparison<T::Token>) {
        into.matched += from.matched;
        into.anchored = from.anchored.or(into.anchored);
        into.broke_after = from.broke_after.or(into.broke_after);
        if from.mismatch.is_some() {
            into.mismatch = from.mismatch;
        }
    }

    /// This copy closed `window`; windows come in order.
    pub fn push_local(&mut self, window: ClosedWindow) -> Comparison<T::Token> {
        let mut found = Comparison::default();
        if self.spent {
            return found;
        }
        let end = window.end.get();
        if let Some(last) = self.last_local {
            if end <= last {
                return found;
            }
            if end != last + OUTPUT_DIGEST_WINDOW {
                // A gap: no run spans it, and no anchor may reach back
                // across it.
                if self.run.take().is_some() {
                    found.broke_after = Some(WireSeq::new(last));
                }
                self.clear_expected();
                self.floor = end - OUTPUT_DIGEST_WINDOW;
            }
        }
        self.last_local = Some(end);
        let slot = self.slot(end);
        self.local[slot] = LocalWindow {
            end,
            digest: window.digest,
            chain: None,
        };
        if let Some(run) = self.run.as_mut()
            && end == run.extended_to + OUTPUT_DIGEST_WINDOW
        {
            run.chain = run.chain.extend(window.digest);
            run.extended_to = end;
            self.local[slot].chain = Some(run.chain);
            self.compare_waiting(end, &mut found);
        }
        found
    }

    fn clear_expected(&mut self) {
        for slot in self.expected.iter_mut() {
            slot.end = 0;
        }
    }

    /// Compare the primary's value waiting at `end`, if any, now that the
    /// run has reached it.
    fn compare_waiting(&mut self, end: u64, found: &mut Comparison<T::Token>) {
        let slot = self.slot(end);
        let waiting = self.expected[slot];
        if waiting.end == end {
            self.expected[slot].end = 0;
            self.compare(end, waiting.chain, found);
        }
    }

    /// The primary's chain value at `end` arrived.
    pub fn push_expected(&mut self, end: WireSeq, chain: ChainValue) -> Comparison<T::Token> {
        let mut found = Comparison::default();
        if self.spent {
            return found;
        }
        let end = end.get();
        let Some(run) = self.run else {
            self.try_anchor(end, chain, &mut found);
            return found;
        };
        if end < run.from || run.last_match.is_some_and(|matched| end <= matched) {
            // Re-delivered, or the anchor itself.
            return found;
        }
        if end <= run.extended_to {
            self.compare(end, chain, &mut found);
            return found;
        }
        // Ahead of this copy: wait. The oldest waiting value in the slot
        // gives way.
        let slot = self.slot(end);
        self.expected[slot] = ExpectedWindow { end, chain };
        found
    }

    /// Anchor a run at the primary's value `chain` at `end`, if every
    /// window of this copy after it is held or still to come, unbroken.
    fn try_anchor(&mut self, end: u64, chain: ChainValue, found: &mut Comparison<T::Token>) {
        if end < self.floor {
            return;
        }
        let next = end + OUTPUT_DIGEST_WINDOW;
        if let Some(last) = self.last_local
            && last >= next
            && self.held(next).is_none()
        {
            // Its next window is gone: a later value may still anchor.
            return;
        }
        let mut run = Run {
            from: end + 1,
            extended_to: end,
            chain,
            last_match: None,
        };
        // Extend over every window this copy already holds after it.
        while let Some(window) = self.held(run.extended_to + OUTPUT_DIGEST_WINDOW) {
            run.chain = run.chain.extend(window.digest);
            run.extended_to = window.end;
            let slot = self.slot(window.end);
            self.local[slot].chain = Some(run.chain);
        }
        self.run = Some(run);
        found.anchored = Some(WireSeq::new(end));
    }

    /// Compare the primary's `expected` value at `end` with this copy's
    /// chain there.
    fn compare(&mut self, end: u64, expected: ChainValue, found: &mut Comparison<T::Token>) {
        let Some(actual) = self.held(end).and_then(|window| window.chain) else {
            // This copy's chain there was evicted: a skipped sample.
            return;
        };
        let Some(run) = self.run.as_mut() else {
            return;
        };
        if actual == expected {
            run.last_match = Some(end);
            found.matched += 1;
            return;
        }
        let divergence = Divergence {
            end: WireSeq::new(end),
            run_from: WireSeq::new(run.from),
            last_match: run.last_match.map(WireSeq::new),
            expected,
            actual,
        };
        self.spent = true;
        let halted = self.trip.trip(divergence);
        found.mismatch = Some((divergence, halted));
    }
}

// ---------------------------------------------------------------------------
// The board
// ---------------------------------------------------------------------------

/// One closed window, as the board holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClosedWindow {
    /// The window's last position, a multiple of [`OUTPUT_DIGEST_WINDOW`].
    pub end: WireSeq,
    /// The digest of the window's reports.
    pub digest: WindowDigest,
    /// The folder's chain through this window.
    pub chain: ChainValue,
}

/// A board slot's payload: a [`ClosedWindow`] laid out for the seqlock.
/// `repr(C)` with the `u64` first and two byte arrays after it, so it has
/// no padding (40 bytes, alignment 8) and every byte a reader copies was
/// written.
#[repr(C)]
#[derive(Clone, Copy)]
struct BoardEntry {
    /// The window's end; 0 for a slot never written, since every window
    /// ends at a positive multiple of `K`.
    window_end: u64,
    digest: [u8; DIGEST_LEN],
    chain: [u8; DIGEST_LEN],
}

const _: () = assert!(std::mem::size_of::<BoardEntry>() == 8 + 2 * DIGEST_LEN);

// Safety: `repr(C)`, a `u64` at offset 0 and two byte arrays at 8 and 24,
// 40 bytes in all with alignment 8: no padding byte anywhere, which the
// size assertion above pins.
unsafe impl NoPadding for BoardEntry {}

/// How many windows a digest board holds. A newtype built only by
/// [`for_pipeline`](Self::for_pipeline) (or [`new`](Self::new), for
/// tests), so the board and whatever indexes into it the same way take
/// their size from one function and cannot disagree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BoardSize(usize);

impl BoardSize {
    /// The fewest slots a board may have: two, so the window being
    /// written never shares a slot with the one before it.
    pub const MIN: usize = 2;

    /// The board for a pipeline whose rings hold `capacity` slots each:
    /// `ceil((input slots + output slots) / K) + 2` windows.
    ///
    /// A reader that must see every window (a replica comparing its own
    /// windows against its primary's) falls behind the folder by at most
    /// what the folder can close between two of its reads. The entries
    /// still in the input ring and those whose reports wait in the output
    /// ring are covered by the two rings' capacity; the two extra slots
    /// cover the window being written and the one a read straddles.
    /// Applied entries that publish no report are bounded by no ring, so a
    /// reader stalled through about `size · K` of them loses windows: a
    /// gap the reader detects and handles, never a wrong value.
    pub const fn for_pipeline(capacity: RingCapacity) -> Self {
        let positions = 2 * capacity.slots() as u64;
        // Lossless: at most a few times a ring's slot count, a `usize`.
        Self(positions.div_ceil(OUTPUT_DIGEST_WINDOW) as usize + 2)
    }

    /// A board of `slots` windows, at least [`MIN`](Self::MIN). For tests
    /// that force overwrites with a small board; a node sizes its board
    /// with [`for_pipeline`](Self::for_pipeline).
    pub const fn new(slots: usize) -> Option<Self> {
        if slots < Self::MIN {
            None
        } else {
            Some(Self(slots))
        }
    }

    /// The number of windows the board holds.
    pub const fn slots(self) -> usize {
        self.0
    }

    /// The slot the window ending at `end` lives in. `end` must be a
    /// window end, a multiple of [`OUTPUT_DIGEST_WINDOW`].
    pub const fn index(self, end: WireSeq) -> usize {
        debug_assert!(
            end.get().is_multiple_of(OUTPUT_DIGEST_WINDOW),
            "not a window end"
        );
        // Lossless: the remainder is below `self.0`, a `usize`.
        ((end.get() / OUTPUT_DIGEST_WINDOW) % self.0 as u64) as usize
    }
}

/// A digest board of `size` windows: the writer for its one folder, and a
/// reader for any number of threads.
///
/// One seqlock per slot rather than a queue: a folder must never wait on
/// a reader, and a reader that falls behind should lose old windows, not
/// stall anyone. Each slot is a separately allocated seqlock cell, padded
/// to a cache line, so concurrent reads and writes of neighbouring windows
/// do not false-share; at the default ring capacity that is a few
/// megabytes per board, allocated once when the pipeline is built.
pub fn digest_board(size: BoardSize) -> (DigestBoardWriter, DigestBoardReader) {
    let empty = BoardEntry {
        window_end: 0,
        digest: [0; DIGEST_LEN],
        chain: [0; DIGEST_LEN],
    };
    let (writers, readers): (Vec<_>, Vec<_>) = (0..size.0).map(|_| seqlock::split(empty)).unzip();
    let latest = Arc::new(AtomicU64::new(0));
    (
        DigestBoardWriter {
            size,
            slots: writers.into_boxed_slice(),
            latest: Arc::clone(&latest),
        },
        DigestBoardReader {
            size,
            slots: readers.into(),
            latest,
        },
    )
}

/// The board's one writer, owned by its folder. Not `Clone`: the seqlocks
/// it writes have a single writer.
pub struct DigestBoardWriter {
    size: BoardSize,
    /// One writer per slot. A boxed slice rather than a `Vec`: its length
    /// is fixed at construction.
    slots: Box<[SeqLockWriter<BoardEntry>]>,
    /// The end of the newest window published, 0 before the first.
    latest: Arc<AtomicU64>,
}

impl DigestBoardWriter {
    /// Publish `window` in its slot, then make it the latest.
    fn publish(&mut self, window: ClosedWindow) {
        self.slots[self.size.index(window.end)].store(BoardEntry {
            window_end: window.end.get(),
            digest: window.digest.0,
            chain: window.chain.0,
        });
        // Release: a reader that loads this end (`Acquire`) finds the
        // slot's store above already done.
        self.latest.store(window.end.get(), Ordering::Release);
    }
}

/// A reader of the board, cheap to clone and share.
#[derive(Clone)]
pub struct DigestBoardReader {
    size: BoardSize,
    /// One reader per slot, shared by every clone. An `Arc<[_]>` rather
    /// than a `Vec` per clone: the slots never change after construction.
    slots: Arc<[SeqLockReader<BoardEntry>]>,
    latest: Arc<AtomicU64>,
}

/// What [`DigestBoardReader::get`] found in a window's slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoardLookup {
    /// The window, closed.
    Closed(ClosedWindow),
    /// The slot holds an older window, or none: this one has not closed
    /// yet.
    NotYet,
    /// The slot holds a newer window: this one was overwritten before it
    /// was read.
    Overwritten,
}

impl DigestBoardReader {
    /// The board's size, for a reader that keeps windows of its own in
    /// step with it.
    pub fn size(&self) -> BoardSize {
        self.size
    }

    /// The end of the newest window published, or `None` before the
    /// first.
    #[inline]
    pub fn latest(&self) -> Option<WireSeq> {
        // Acquire: pairs with the writer's `Release`, so the slot of the
        // end loaded holds that window or a newer one.
        match self.latest.load(Ordering::Acquire) {
            0 => None,
            end => Some(WireSeq::new(end)),
        }
    }

    /// The window ending at `end`, if its slot still holds it. `end` must
    /// be a window end, a multiple of [`OUTPUT_DIGEST_WINDOW`]; any other
    /// position would read a neighbouring window's slot.
    #[inline]
    pub fn get(&self, end: WireSeq) -> BoardLookup {
        let entry = self.slots[self.size.index(end)].load();
        match entry.window_end.cmp(&end.get()) {
            std::cmp::Ordering::Equal => BoardLookup::Closed(ClosedWindow {
                end,
                digest: WindowDigest(entry.digest),
                chain: ChainValue(entry.chain),
            }),
            std::cmp::Ordering::Less => BoardLookup::NotYet,
            std::cmp::Ordering::Greater => BoardLookup::Overwritten,
        }
    }
}

/// The hash and the folder: every test here hashes. `twox-hash` runs its
/// stripe loop in NEON inline assembly on aarch64, which Miri cannot
/// execute, so `scripts/miri.sh` skips this module; there is no `unsafe`
/// of ours in it. The board's lock-free code is tested under Miri in
/// [`tests`], which hashes nothing.
#[cfg(test)]
mod folder_tests {
    use super::*;

    const K: u64 = OUTPUT_DIGEST_WINDOW;

    /// A report as these tests write one: its position, and its body or
    /// `None` for a failed encode.
    type Report<'a> = (u64, Option<&'a [u8]>);

    // ---- XXH3-128 against the reference --------------------------------

    /// The input xxHash's own sanity tests hash (`xsum_sanity_check.c`'s
    /// `fillTestBuffer`): each byte the top byte of a running product.
    fn sanity_buffer(len: usize) -> Vec<u8> {
        const PRIME32: u64 = 2_654_435_761;
        const PRIME64: u64 = 11_400_714_785_074_694_797;
        let mut state = PRIME32;
        (0..len)
            .map(|_| {
                let byte = (state >> 56) as u8;
                state = state.wrapping_mul(PRIME64);
                byte
            })
            .collect()
    }

    /// `twox-hash` agrees with the reference implementation: XXH3-128 at
    /// seed 0 over the sanity buffer, at lengths either side of each of
    /// XXH3's internal cutoffs, against the values in xxHash's
    /// `tests/sanity_test_vectors.h` (v0.8.2), given there as (low, high)
    /// halves. The digest's definition is XXH3-128, so this is what makes
    /// a digest computed here comparable with one any other
    /// implementation computes.
    #[test]
    fn the_hash_matches_the_reference_vectors() {
        let vectors: &[(usize, u64, u64)] = &[
            (0, 0x6001C324468D497F, 0x99AA06D3014798D8),
            (1, 0xC44BDFF4074EECDB, 0xA6CD5E9392000F6A),
            (3, 0x54247382A8D6B94D, 0x20EFC49FF02422EA),
            (4, 0x2E7D8D6876A39FE9, 0x970D585AC632BF8E),
            (8, 0x64C69CAB4BB21DC5, 0x47A7F080D82BB456),
            (9, 0xED7CCBC501EB7501, 0x564EF6078950D457),
            (16, 0x562980258A998629, 0xC68C368ECF8A9C05),
            (17, 0xABBC12D11973D7DB, 0x955FA78643ED3669),
            (128, 0xEBB15E34A7FB5AB1, 0x39992220E045260A),
            (129, 0x86C9E3BC8F0A3B5C, 0x03815FC91F1B30B6),
            (240, 0x5C9AAE94C8EBE5A0, 0xAA4202DAA2769DC8),
            (241, 0xC5A639ECD2030E5E, 0x99A80ECF0ECFC647),
            (255, 0xE98F979F4ED8A197, 0x961375C87E09EFBC),
            (256, 0x55DE574AD89D0AC5, 0x8B1C66091423D288),
            (1024, 0xDD85C9B5C1109C5C, 0x0D30D24071C64C57),
            (2048, 0xDD59E2C3A5F038E0, 0xF736557FD47073A5),
            (2240, 0x6E73A90539CF2948, 0xCCB134FBFA7CE49D),
            (2241, 0xE800ECA585FE92D9, 0x64A1592C03F3B910),
            (4096, 0xE91206429D1F48F9, 0xB9CFAEA2CA5626A4),
        ];
        for &(len, low, high) in vectors {
            let expected = (u128::from(high) << 64) | u128::from(low);
            let input = sanity_buffer(len);
            assert_eq!(
                xxhash3_128::Hasher::oneshot(&input),
                expected,
                "one-shot, length {len}"
            );
            // The streaming hasher a window uses, fed in uneven pieces.
            let mut hasher: WindowHasher = RawHasher::new(SecretBuffer::default());
            for piece in input.chunks(7) {
                hasher.write(piece);
            }
            assert_eq!(hasher.finish_128(), expected, "streamed, length {len}");
        }
    }

    // ---- the window digest's definition ---------------------------------

    /// One window's digest input, built by hand from the definition:
    /// the header, then each report's position, length and body.
    fn definition_bytes(end: u64, reports: &[Report<'_>]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"melin output digest v1");
        bytes.push(1);
        bytes.extend_from_slice(&64u64.to_le_bytes());
        bytes.extend_from_slice(&end.to_le_bytes());
        for &(position, body) in reports {
            bytes.extend_from_slice(&position.to_le_bytes());
            match body {
                Some(body) => {
                    bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(body);
                }
                None => bytes.extend_from_slice(&u32::MAX.to_le_bytes()),
            }
        }
        bytes
    }

    fn digest_of(end: u64, reports: &[Report<'_>]) -> WindowDigest {
        WindowDigest(xxhash3_128::Hasher::oneshot(&definition_bytes(end, reports)).to_be_bytes())
    }

    /// A folder over a board of `slots`, its reader beside it.
    fn folder_at(first_live: u64, slots: usize) -> (WindowFolder, DigestBoardReader) {
        let (writer, reader) = digest_board(BoardSize::new(slots).unwrap());
        (WindowFolder::new(WireSeq::new(first_live), writer), reader)
    }

    fn closed(reader: &DigestBoardReader, end: u64) -> ClosedWindow {
        match reader.get(WireSeq::new(end)) {
            BoardLookup::Closed(window) => window,
            other => panic!("window {end}: {other:?}"),
        }
    }

    fn fold(folder: &mut WindowFolder, position: u64, body: Option<&[u8]>) {
        let body = match body {
            Some(bytes) => ReportBody::Encoded(bytes),
            None => ReportBody::Failed,
        };
        folder.fold(WireSeq::new(position), body);
    }

    /// A body longer than any encode can produce would wrap its length
    /// prefix: a caller's bug, caught in debug builds.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "a 513-byte body")]
    fn an_oversized_body_panics_in_debug() {
        let (mut folder, _reader) = folder_at(1, 8);
        fold(&mut folder, 1, Some(&[0; MAX_RESPONSE_BODY + 1]));
    }

    /// The folder's digest and chain are the definition's, hashed in one
    /// shot over bytes built by hand: the window's header, a report, a
    /// failed encode and an empty body in the first window, a lone report
    /// in the second.
    #[test]
    fn a_window_digest_and_the_chain_are_the_definitions() {
        let (mut folder, reader) = folder_at(1, 8);
        let first: &[Report<'_>] = &[
            (1, Some(b"alpha")),
            (1, None),
            (5, Some(b"")),
            (64, Some(b"z")),
        ];
        let second: &[Report<'_>] = &[(100, Some(b"beta"))];
        for &(position, body) in first.iter().chain(second) {
            fold(&mut folder, position, body);
        }
        folder.close_through(WireSeq::new(2 * K));

        let d1 = digest_of(K, first);
        let d2 = digest_of(2 * K, second);
        let c1 =
            ChainValue(xxhash3_128::Hasher::oneshot(&[[0u8; 16], d1.0].concat()).to_be_bytes());
        let c2 = ChainValue(xxhash3_128::Hasher::oneshot(&[c1.0, d2.0].concat()).to_be_bytes());
        assert_eq!(
            closed(&reader, K),
            ClosedWindow {
                end: WireSeq::new(K),
                digest: d1,
                chain: c1
            }
        );
        assert_eq!(
            closed(&reader, 2 * K),
            ClosedWindow {
                end: WireSeq::new(2 * K),
                digest: d2,
                chain: c2
            }
        );
    }

    /// Fed report by report, the streaming hash equals the one-shot hash
    /// of the same bytes, for windows whose input is either side of
    /// XXH3's 240-byte cutoff and well past its internal buffer.
    #[test]
    fn the_streamed_window_equals_the_one_shot_hash_at_every_size() {
        let body = [0xC3u8; 200];
        for reports in [0usize, 1, 4, 5, 6, 30, 64] {
            let (mut folder, reader) = folder_at(1, 4);
            let window: Vec<Report<'_>> = (0..reports)
                .map(|i| (1 + i as u64, Some(&body[..(i * 37) % body.len()])))
                .collect();
            for &(position, bytes) in &window {
                fold(&mut folder, position, bytes);
            }
            folder.close_through(WireSeq::new(K));
            let len = definition_bytes(K, &window).len();
            assert_eq!(
                closed(&reader, K).digest,
                digest_of(K, &window),
                "{reports} reports, {len} bytes"
            );
        }
    }

    /// The window's reports, folded into a fresh folder: `(digest of
    /// window 1, chain through window 3)` with `window_1` in the first
    /// window and the same two reports in the second and third.
    fn fold_three(window_1: &[Report<'_>]) -> (WindowDigest, ChainValue) {
        let (mut folder, reader) = folder_at(1, 8);
        for &(position, body) in window_1 {
            fold(&mut folder, position, body);
        }
        fold(&mut folder, K + 1, Some(b"later"));
        fold(&mut folder, 2 * K + 1, Some(b"later"));
        folder.close_through(WireSeq::new(3 * K));
        (closed(&reader, K).digest, closed(&reader, 3 * K).chain)
    }

    /// Any change to what a window's reports are, where they are, or in
    /// what order, changes its digest and every chain value after it.
    #[test]
    fn a_moved_changed_swapped_or_failed_report_changes_the_digest_and_the_chain() {
        let base: &[Report<'_>] = &[(3, Some(b"ab")), (3, Some(b"cd")), (9, Some(b"ef"))];
        let variants: &[&[Report<'_>]] = &[
            // Moved to another position.
            &[(3, Some(b"ab")), (4, Some(b"cd")), (9, Some(b"ef"))],
            // A byte changed.
            &[(3, Some(b"ab")), (3, Some(b"cD")), (9, Some(b"ef"))],
            // Two reports swapped.
            &[(3, Some(b"cd")), (3, Some(b"ab")), (9, Some(b"ef"))],
            // An encode failed.
            &[(3, Some(b"ab")), (3, None), (9, Some(b"ef"))],
            // A byte moved from one body to the next.
            &[(3, Some(b"a")), (3, Some(b"bcd")), (9, Some(b"ef"))],
        ];
        let (base_digest, base_chain) = fold_three(base);
        for variant in variants {
            let (digest, chain) = fold_three(variant);
            assert_ne!(digest, base_digest, "{variant:?}");
            assert_ne!(chain, base_chain, "{variant:?}");
        }
    }

    /// The first window a folder digests is the first that starts at or
    /// after its first live position: at a window boundary and just past
    /// one.
    #[test]
    fn the_first_window_is_the_first_seen_whole() {
        let first = |s| folder_at(s, 4).0.first_window_end().get();
        assert_eq!(first(0), K);
        assert_eq!(first(1), K);
        assert_eq!(first(2), 2 * K);
        assert_eq!(first(K), 2 * K);
        assert_eq!(first(K + 1), 2 * K);
        assert_eq!(first(K + 2), 3 * K);
    }

    /// Two copies started at different positions agree on every window
    /// both saw whole, and the later one digests no partial window: it
    /// publishes nothing for the window it joined in the middle of.
    #[test]
    fn folders_started_apart_agree_on_every_window_both_saw_whole() {
        let reports: Vec<(u64, Vec<u8>)> = (1..=5 * K)
            .filter(|p| p % 3 != 0)
            .map(|p| (p, p.to_le_bytes()[..(p % 5) as usize].to_vec()))
            .collect();
        let (mut early, early_board) = folder_at(1, 8);
        let (mut late, late_board) = folder_at(K + 10, 8);
        for (position, body) in &reports {
            fold(&mut early, *position, Some(body));
            if *position >= K + 10 {
                fold(&mut late, *position, Some(body));
            }
        }
        early.close_through(WireSeq::new(5 * K));
        late.close_through(WireSeq::new(5 * K));
        assert_eq!(late_board.get(WireSeq::new(2 * K)), BoardLookup::NotYet);
        for end in [3 * K, 4 * K, 5 * K] {
            assert_eq!(
                closed(&early_board, end).digest,
                closed(&late_board, end).digest,
                "window {end}"
            );
        }
    }

    /// A window with no report closes with its header alone, once a later
    /// position is seen or the copy has applied past it, and extends the
    /// chain like any other.
    #[test]
    fn an_empty_window_digests_its_header_and_extends_the_chain() {
        let (mut folder, reader) = folder_at(1, 8);
        folder.close_through(WireSeq::new(K - 1));
        assert_eq!(reader.latest(), None, "not yet applied past its end");
        folder.close_through(WireSeq::new(K));
        let first = closed(&reader, K);
        assert_eq!(first.digest, digest_of(K, &[]));
        assert_eq!(first.chain, CHAIN_SEED.extend(digest_of(K, &[])));
        // A report two windows on closes the empty window between.
        fold(&mut folder, 2 * K + 1, Some(b"x"));
        let second = closed(&reader, 2 * K);
        assert_eq!(second.digest, digest_of(2 * K, &[]));
        assert_eq!(second.chain, first.chain.extend(second.digest));
        assert_eq!(reader.latest(), Some(WireSeq::new(2 * K)));
    }

    // ---- the comparator ---------------------------------------------------

    /// A stop that records every trip; its token is a number, `None` once
    /// `answer` is off (a node already stopped).
    #[derive(Default)]
    struct Recorder {
        trips: std::cell::RefCell<Vec<Divergence>>,
        already_stopped: bool,
    }

    impl Trip for &Recorder {
        type Token = u32;
        fn trip(&self, divergence: Divergence) -> Option<u32> {
            self.trips.borrow_mut().push(divergence);
            (!self.already_stopped).then_some(7)
        }
    }

    /// The body every copy encodes for the report at `position`, or a
    /// different one at `fault`.
    fn body_at(position: u64, fault: Option<u64>) -> Vec<u8> {
        let mut body = position.to_le_bytes()[..(position % 7) as usize + 1].to_vec();
        if fault == Some(position) {
            body.push(0xEE);
        }
        body
    }

    /// The windows a copy started at `first_live` closes through
    /// `through`, one report per position, the one at `fault` changed.
    fn windows(first_live: u64, through: u64, fault: Option<u64>) -> Vec<ClosedWindow> {
        let slots = (through / K) as usize + 2;
        let (writer, reader) = digest_board(BoardSize::new(slots).unwrap());
        let mut folder = WindowFolder::new(WireSeq::new(first_live), writer);
        for position in first_live.max(1)..=through {
            let body = body_at(position, fault);
            folder.fold(WireSeq::new(position), ReportBody::Encoded(&body));
        }
        folder.close_through(WireSeq::new(through));
        let first = folder.first_window_end().get();
        (first..=through)
            .step_by(K as usize)
            .map(|end| closed(&reader, end))
            .collect()
    }

    fn comparator(recorder: &Recorder, first_live: u64) -> DigestComparator<&Recorder> {
        let first = first_window_end(WireSeq::new(first_live));
        DigestComparator::new(recorder, BoardSize::new(16).unwrap(), first)
    }

    /// A run anchored on the primary's first value verifies every later
    /// value it reaches, whichever side arrives first.
    #[test]
    fn agreeing_copies_match_whichever_side_arrives_first() {
        let primary = windows(1, 8 * K, None);
        let replica = windows(1, 8 * K, None);
        let recorder = Recorder::default();
        let mut c = comparator(&recorder, 1);
        let anchored = c.push_expected(primary[0].end, primary[0].chain);
        assert_eq!(anchored.anchored, Some(primary[0].end));
        let mut matched = 0;
        // Windows 2 to 4 local first, then the primary's.
        for w in &replica[..4] {
            matched += c.push_local(*w).matched;
        }
        for w in &primary[1..4] {
            matched += c.push_expected(w.end, w.chain).matched;
        }
        // Windows 5 to 8: the primary's first, waiting.
        for w in &primary[4..] {
            matched += c.push_expected(w.end, w.chain).matched;
        }
        for w in &replica[4..] {
            matched += c.push_local(*w).matched;
        }
        assert_eq!(matched, 7);
        assert!(recorder.trips.borrow().is_empty());
    }

    /// A report that differs in window 5 is a mismatch at the first
    /// value compared at or after it, never later, and its range holds
    /// the report's position. The stop is tripped before the verdict, and
    /// the comparator is spent.
    #[test]
    fn a_differing_report_trips_at_the_first_value_reaching_it() {
        let fault = 4 * K + 9;
        let primary = windows(1, 8 * K, None);
        let replica = windows(1, 8 * K, Some(fault));
        let recorder = Recorder::default();
        let mut c = comparator(&recorder, 1);
        let _ = c.push_expected(primary[0].end, primary[0].chain);
        for w in &replica {
            let _ = c.push_local(*w);
        }
        // Window 3's value matches; window 7's is the first past the fault
        // (5 and 6 are never sent).
        assert_eq!(c.push_expected(primary[2].end, primary[2].chain).matched, 1);
        let found = c.push_expected(primary[6].end, primary[6].chain);
        let (divergence, halted) = found.mismatch.expect("a mismatch");
        assert_eq!(halted, Some(7));
        assert_eq!(divergence.end, primary[6].end);
        let (from, through) = divergence.positions();
        assert!(from.get() <= fault && fault <= through.get());
        assert_eq!(divergence.last_match, Some(primary[2].end));
        assert_eq!(*recorder.trips.borrow(), [divergence]);
        assert!(c.is_spent());
        assert!(
            c.push_expected(primary[7].end, primary[7].chain)
                .mismatch
                .is_none()
        );
        assert_eq!(recorder.trips.borrow().len(), 1);
    }

    /// A stop that had already tripped answers `None`: the mismatch says so.
    #[test]
    fn a_mismatch_on_a_stopped_node_carries_no_token() {
        let primary = windows(1, 3 * K, None);
        let replica = windows(1, 3 * K, Some(K + 1));
        let recorder = Recorder {
            already_stopped: true,
            ..Recorder::default()
        };
        let mut c = comparator(&recorder, 1);
        let _ = c.push_expected(primary[0].end, primary[0].chain);
        for w in &replica {
            let _ = c.push_local(*w);
        }
        let found = c.push_expected(primary[2].end, primary[2].chain);
        assert!(matches!(found.mismatch, Some((_, None))));
    }

    /// A replica that joined mid-window anchors on nothing before its own
    /// first whole window, and verifies from there.
    #[test]
    fn a_late_replica_anchors_at_its_first_whole_window() {
        let primary = windows(1, 6 * K, None);
        let replica = windows(2 * K + 10, 6 * K, None);
        let recorder = Recorder::default();
        let mut c = comparator(&recorder, 2 * K + 10);
        // F = 4K: values at K and 2K are below F - K, and anchor nothing.
        assert!(
            c.push_expected(primary[0].end, primary[0].chain)
                .anchored
                .is_none()
        );
        assert!(
            c.push_expected(primary[1].end, primary[1].chain)
                .anchored
                .is_none()
        );
        assert!(
            c.push_expected(primary[2].end, primary[2].chain)
                .anchored
                .is_some()
        );
        let mut matched = 0;
        for w in &replica {
            matched += c.push_local(*w).matched;
        }
        for w in &primary[3..] {
            matched += c.push_expected(w.end, w.chain).matched;
        }
        assert_eq!(matched, 3);
        assert!(recorder.trips.borrow().is_empty());
    }

    /// A local window lost before it was read breaks the run: no match
    /// spans it, and the next usable value anchors anew.
    #[test]
    fn a_lost_local_window_breaks_the_run() {
        let primary = windows(1, 8 * K, None);
        let replica = windows(1, 8 * K, None);
        let recorder = Recorder::default();
        let mut c = comparator(&recorder, 1);
        let _ = c.push_expected(primary[0].end, primary[0].chain);
        for w in &replica[..3] {
            let _ = c.push_local(*w);
        }
        // Window 4 is lost.
        let broke = c.push_local(replica[4]);
        assert_eq!(broke.broke_after, Some(replica[2].end));
        // Window 5's value cannot be checked across the gap.
        let found = c.push_expected(primary[4].end, primary[4].chain);
        assert_eq!(found.matched, 0);
        assert_eq!(found.anchored, Some(primary[4].end));
        for w in &replica[5..] {
            let _ = c.push_local(*w);
        }
        assert_eq!(c.push_expected(primary[7].end, primary[7].chain).matched, 1);
        assert!(recorder.trips.borrow().is_empty());
    }

    /// A new session ends the run and drops the waiting values: a primary
    /// that restarted, its chain started afresh, anchors anew and never
    /// mismatches.
    #[test]
    fn a_session_against_a_restarted_primary_anchors_anew() {
        let first = windows(1, 4 * K, None);
        let restarted = windows(3 * K + 1, 8 * K, None);
        let replica = windows(1, 8 * K, None);
        let recorder = Recorder::default();
        let mut c = comparator(&recorder, 1);
        let _ = c.push_expected(first[0].end, first[0].chain);
        // A value waiting when the session ends is dropped with it.
        let _ = c.push_expected(first[3].end, first[3].chain);
        c.begin_session();
        let mut matched = 0;
        for w in &restarted {
            matched += c.push_expected(w.end, w.chain).matched;
        }
        for w in &replica {
            matched += c.push_local(*w).matched;
        }
        assert_eq!(matched, restarted.len() as u32 - 1);
        assert!(recorder.trips.borrow().is_empty());
    }

    /// A comparator built for a pipeline, fed from a board of the same
    /// size, absorbs a whole board's worth of windows between two reads
    /// without a gap; more than that is a gap.
    #[test]
    fn absorbing_up_to_the_board_loses_nothing_and_past_it_breaks() {
        let size = BoardSize::new(8).unwrap();
        let (writer, board) = digest_board(size);
        let mut folder = WindowFolder::new(WireSeq::new(1), writer);
        let recorder = Recorder::default();
        let mut c = DigestComparator::new(&recorder, size, folder.first_window_end());
        fold_through(&mut folder, 1, 8 * K);
        assert!(c.absorb(&board).broke_after.is_none());
        // A run anchored on window 1 reaches every window absorbed so far.
        let primary = windows(1, 17 * K, None);
        let anchored = c.push_expected(primary[0].end, primary[0].chain);
        assert_eq!(anchored.anchored, Some(primary[0].end));
        assert_eq!(c.push_expected(primary[7].end, primary[7].chain).matched, 1);
        // Nine more windows close before the next read: window 9 is lost.
        fold_through(&mut folder, 8 * K + 1, 17 * K);
        let found = c.absorb(&board);
        assert_eq!(found.broke_after, Some(WireSeq::new(8 * K)));
        // Nothing spans the gap; the newest window anchors anew.
        let after = c.push_expected(primary[16].end, primary[16].chain);
        assert_eq!(after.matched, 0);
        assert!(recorder.trips.borrow().is_empty());
    }

    /// Fold one agreeing report per position from `from` through
    /// `through`, closing every window through `through`.
    fn fold_through(folder: &mut WindowFolder, from: u64, through: u64) {
        for position in from..=through {
            let body = body_at(position, None);
            folder.fold(WireSeq::new(position), ReportBody::Encoded(&body));
        }
        folder.close_through(WireSeq::new(through));
    }

    /// A board that laps the newest end a read loaded, before that end's
    /// slot is read, ends the read rather than spinning on it; the next
    /// read resumes at the board's new latest, past a gap.
    #[test]
    fn a_board_lapping_the_loaded_latest_ends_the_read() {
        let size = BoardSize::new(8).unwrap();
        let (writer, board) = digest_board(size);
        let mut folder = WindowFolder::new(WireSeq::new(1), writer);
        let recorder = Recorder::default();
        let mut c = DigestComparator::new(&recorder, size, folder.first_window_end());
        fold_through(&mut folder, 1, 8 * K);
        assert!(c.absorb(&board).broke_after.is_none());
        let primary = windows(1, K, None);
        assert!(
            c.push_expected(primary[0].end, primary[0].chain)
                .anchored
                .is_some()
        );
        // The read loads 16K as the latest; before it reads a slot, the
        // board closes through 24K, overwriting windows 9 to 16, 16K's
        // slot included.
        fold_through(&mut folder, 8 * K + 1, 24 * K);
        let stale = c.absorb_through(&board, WireSeq::new(16 * K));
        assert!(stale.broke_after.is_none());
        assert_eq!(c.last_local, Some(8 * K));
        let found = c.absorb(&board);
        assert_eq!(found.broke_after, Some(WireSeq::new(8 * K)));
        assert_eq!(c.last_local, Some(24 * K));
        assert!(recorder.trips.borrow().is_empty());
    }

    mod comparator_properties {
        use proptest::prelude::*;

        use super::*;

        /// What happens, in what order, in a generated scenario.
        #[derive(Clone, Debug)]
        enum Event {
            /// This copy's next window arrives, or is lost (`lost`).
            Local { lost: bool },
            /// The primary's next window's value is sent (`sent`) or not.
            Expected { sent: bool },
            /// A new session, the primary restarted (`restarted`) or not.
            Session { restarted: bool },
        }

        fn event() -> impl Strategy<Value = Event> {
            prop_oneof![
                6 => Just(Event::Local { lost: false }),
                1 => Just(Event::Local { lost: true }),
                6 => any::<bool>().prop_map(|sent| Event::Expected { sent }),
                1 => any::<bool>().prop_map(|restarted| Event::Session { restarted }),
            ]
        }

        /// Run `events` with this copy started at `first_live` and its
        /// report at `fault` changed; return every trip.
        fn run(first_live: u64, fault: Option<u64>, events: &[Event]) -> Vec<Divergence> {
            const THROUGH: u64 = 24 * K;
            let replica = windows(first_live, THROUGH, fault);
            let mut primary = windows(1, THROUGH, None);
            let recorder = Recorder::default();
            let mut c = comparator(&recorder, first_live);
            let (mut local, mut expected) = (0usize, 0usize);
            for event in events {
                match *event {
                    Event::Local { lost } => {
                        if local < replica.len() {
                            if !lost {
                                let _ = c.push_local(replica[local]);
                            }
                            local += 1;
                        }
                    }
                    Event::Expected { sent } => {
                        if expected < primary.len() {
                            if sent {
                                let w = primary[expected];
                                let _ = c.push_expected(w.end, w.chain);
                            }
                            expected += 1;
                        }
                    }
                    Event::Session { restarted } => {
                        c.begin_session();
                        if restarted && expected < primary.len() {
                            // Restarted at the next position: its chain
                            // starts afresh from there.
                            let from = primary[expected].end.get() - K + 1;
                            let fresh = windows(from, THROUGH, None);
                            primary.truncate(expected);
                            primary.extend(fresh);
                        }
                    }
                }
            }
            recorder.trips.take()
        }

        proptest! {
            /// Copies whose reports agree never mismatch, across any
            /// interleaving, session, primary restart or lost window.
            #[test]
            fn agreeing_copies_never_mismatch(
                first_live in 1u64..(4 * K),
                events in proptest::collection::vec(event(), 0..120),
            ) {
                prop_assert!(run(first_live, None, &events).is_empty());
            }

            /// A mismatch's range always holds the differing report, and
            /// at most one trip is made.
            #[test]
            fn a_mismatch_points_at_the_differing_report(
                first_live in 1u64..(4 * K),
                fault_offset in 0u64..(16 * K),
                events in proptest::collection::vec(event(), 0..120),
            ) {
                let fault = first_live + fault_offset;
                let trips = run(first_live, Some(fault), &events);
                prop_assert!(trips.len() <= 1);
                for trip in trips {
                    let (from, through) = trip.positions();
                    prop_assert!(from.get() <= fault && fault <= through.get(), "{trip:?}");
                }
            }

            /// Within one session, nothing lost and nothing evicted, a
            /// differing report is caught: by the first value the primary
            /// sends at or past its window, once this copy reaches that
            /// value, provided the run anchored before the window. Pins
            /// that detection happens, which the property above cannot.
            #[test]
            fn an_unbroken_run_trips_at_the_first_value_past_the_fault(
                first_live in 1u64..(4 * K),
                fault_offset in 0u64..(8 * K),
                events in proptest::collection::vec(
                    prop_oneof![
                        Just(Event::Local { lost: false }),
                        any::<bool>().prop_map(|sent| Event::Expected { sent }),
                    ],
                    0..60,
                ),
            ) {
                // Fewer windows than the comparator holds: nothing evicted.
                const THROUGH: u64 = 12 * K;
                let fault = first_live + fault_offset;
                let replica = windows(first_live, THROUGH, Some(fault));
                let primary = windows(1, THROUGH, None);
                let first_end = replica[0].end.get();
                let window = fault.div_ceil(K) * K;
                let recorder = Recorder::default();
                let mut c = comparator(&recorder, first_live);
                let (mut local, mut expected) = (0usize, 0usize);
                let mut sent = Vec::new();
                for event in &events {
                    match *event {
                        Event::Local { .. } if local < replica.len() => {
                            let _ = c.push_local(replica[local]);
                            local += 1;
                        }
                        Event::Expected { sent: send } if expected < primary.len() => {
                            let w = primary[expected];
                            if send {
                                let _ = c.push_expected(w.end, w.chain);
                                sent.push(w.end.get());
                            }
                            expected += 1;
                        }
                        _ => {}
                    }
                }
                let reached = local.checked_sub(1).map_or(0, |i| replica[i].end.get());
                // The anchor: the first value at or after the window before
                // this copy's first.
                let anchor = sent.iter().copied().find(|&end| end >= first_end - K);
                let caught = sent.iter().copied().find(|&end| end >= window);
                let want = match (anchor, caught) {
                    (Some(anchor), Some(end))
                        if window >= first_end && anchor < window && end <= reached =>
                    {
                        Some(end)
                    }
                    _ => None,
                };
                let trips = recorder.trips.take();
                prop_assert_eq!(trips.iter().map(|t| t.end.get()).collect::<Vec<_>>(),
                    want.into_iter().collect::<Vec<_>>());
            }
        }
    }

    // ---- the digester's slot rules ---------------------------------------

    type Slot = OutputSlot<u8, u16>;

    fn slot(wire_seq: u64, payload: OutputPayload<u8, u16>) -> Slot {
        Slot {
            wire_seq,
            payload,
            ..Default::default()
        }
    }

    /// A digester from position 1 over a board of 8, its applied cursor
    /// and its board's reader.
    fn digester() -> (OutputDigester, AppliedWireSeqCursor, DigestBoardReader) {
        let applied = AppliedWireSeqCursor::detached(WireSeq::new(0));
        let (writer, reader) = digest_board(BoardSize::new(8).unwrap());
        (
            OutputDigester::new(WireSeq::new(1), applied.clone(), writer),
            applied,
            reader,
        )
    }

    /// A report hands back itself and its position to fold; a batch end
    /// or a policy change at `w` closes the windows before `w`, not the
    /// one `w` is in; a query response at `w` closes through `w`, since
    /// it is stamped with a position already applied whole.
    #[test]
    fn each_slot_closes_what_lies_behind_it() {
        let (mut d, _applied, board) = digester();
        let report = slot(3, OutputPayload::Report(7));
        let at = d.slot(&report).expect("a report is folded");
        assert_eq!(*at.report(), 7);
        d.fold(at, ReportBody::Encoded(b"x"));

        // At the first position of window 2: window 1 closes.
        assert!(d.slot(&slot(K + 1, OutputPayload::BatchEnd)).is_none());
        assert_eq!(closed(&board, K).digest, digest_of(K, &[(3, Some(b"x"))]));
        // At window 2's end: a batch end leaves it open...
        assert!(d.slot(&slot(2 * K, OutputPayload::BatchEnd)).is_none());
        let change = OutputPayload::AckPolicyChange(melin_journal::AckPolicyChange {
            policy: melin_journal::AckPolicyByte(0),
            unwitnessed: false,
        });
        assert!(d.slot(&slot(2 * K, change)).is_none());
        assert_eq!(board.get(WireSeq::new(2 * K)), BoardLookup::NotYet);
        // ...a query response stamped there closes it.
        assert!(
            d.slot(&slot(2 * K, OutputPayload::QueryResponse(1)))
                .is_none()
        );
        assert_eq!(closed(&board, 2 * K).digest, digest_of(2 * K, &[]));
    }

    /// An empty ring closes every window through what was loaded before
    /// the read, and nothing past it.
    #[test]
    fn an_empty_ring_closes_through_the_applied_position_loaded() {
        let (mut d, applied, board) = digester();
        applied.store(WireSeq::new(2 * K + 5));
        let loaded = d.load_applied();
        // Moved on after the load: not what the read was taken against.
        applied.store(WireSeq::new(3 * K));
        d.ring_empty(loaded);
        assert_eq!(board.latest(), Some(WireSeq::new(2 * K)));
    }
}

/// The board, the kinds and the body encode: nothing here hashes, so Miri
/// runs all of it, the board's concurrent test included.
#[cfg(test)]
mod tests {
    use super::*;

    const K: u64 = OUTPUT_DIGEST_WINDOW;

    // ---- the board ------------------------------------------------------

    /// A window whose every byte derives from its window number, so a test
    /// can tell which window it read back, and a torn read shows as a
    /// mismatch between the end and the bytes beside it. Each array holds
    /// every byte of the number (byte `i % 8`), so no two windows, and in
    /// particular no two that share a slot, have equal digests or chains.
    fn window(end: u64) -> ClosedWindow {
        let m = (end / K).to_le_bytes();
        ClosedWindow {
            end: WireSeq::new(end),
            digest: WindowDigest(std::array::from_fn(|i| m[i % 8].wrapping_add(i as u8))),
            chain: ChainValue(std::array::from_fn(|i| m[i % 8] ^ i as u8)),
        }
    }

    /// Windows that share a slot never share a byte pattern: otherwise a
    /// read mixing one's end with the other's bytes would pass the torn
    /// read check below.
    #[test]
    fn windows_that_share_a_slot_differ_in_every_array() {
        for m in 1..=1024u64 {
            for lap in [4, 256, 1024] {
                let (a, b) = (window(m * K), window((m + lap) * K));
                assert_ne!(a.digest, b.digest, "{m} {lap}");
                assert_ne!(a.chain, b.chain, "{m} {lap}");
            }
        }
    }

    #[test]
    fn the_board_finds_a_window_until_it_is_overwritten() {
        let (mut writer, reader) = digest_board(BoardSize::new(3).unwrap());
        assert_eq!(reader.latest(), None);
        assert_eq!(reader.get(WireSeq::new(K)), BoardLookup::NotYet);
        for end in [K, 2 * K, 3 * K] {
            writer.publish(window(end));
        }
        assert_eq!(reader.latest(), Some(WireSeq::new(3 * K)));
        for end in [K, 2 * K, 3 * K] {
            assert_eq!(
                reader.get(WireSeq::new(end)),
                BoardLookup::Closed(window(end))
            );
        }
        assert_eq!(reader.get(WireSeq::new(4 * K)), BoardLookup::NotYet);
        // A fourth window takes the first's slot.
        writer.publish(window(4 * K));
        assert_eq!(reader.get(WireSeq::new(K)), BoardLookup::Overwritten);
        assert_eq!(
            reader.get(WireSeq::new(4 * K)),
            BoardLookup::Closed(window(4 * K))
        );
        assert_eq!(reader.latest(), Some(WireSeq::new(4 * K)));
    }

    /// A position that is not a window end is a caller's bug, not a
    /// window that has not closed yet.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "not a window end")]
    fn a_lookup_off_the_window_grid_panics_in_debug() {
        let (_writer, reader) = digest_board(BoardSize::new(2).unwrap());
        // Dropped result: the call panics before it returns one.
        let _ = reader.get(WireSeq::new(K + 1));
    }

    /// The window read back is the one published, every byte of it.
    #[test]
    fn a_window_is_read_back_whole() {
        let (mut writer, reader) = digest_board(BoardSize::new(2).unwrap());
        let window = ClosedWindow {
            end: WireSeq::new(7 * K),
            digest: WindowDigest(std::array::from_fn(|i| i as u8)),
            chain: ChainValue(std::array::from_fn(|i| 0xF0 | i as u8)),
        };
        writer.publish(window);
        assert_eq!(reader.get(WireSeq::new(7 * K)), BoardLookup::Closed(window));
    }

    /// A reader on another thread never sees a torn window: every window
    /// it reads back is one the writer published, whole, while the writer
    /// keeps overwriting the slots under it. Small under Miri, which the
    /// existing Miri job runs.
    #[test]
    fn a_concurrent_reader_never_sees_a_torn_window() {
        let windows: u64 = if cfg!(miri) { 64 } else { 50_000 };
        let (mut writer, reader) = digest_board(BoardSize::new(4).unwrap());
        std::thread::scope(|scope| {
            let reader = reader.clone();
            scope.spawn(move || {
                let last = WireSeq::new(windows * K);
                loop {
                    let latest = reader.latest();
                    if let Some(end) = latest
                        && let BoardLookup::Closed(found) = reader.get(end)
                    {
                        assert_eq!(found, window(end.get()));
                    }
                    if latest == Some(last) {
                        break;
                    }
                }
            });
            for m in 1..=windows {
                writer.publish(window(m * K));
            }
        });
    }

    /// The board holds both rings' worth of positions in windows, plus
    /// two: at the smallest ring, the default and above it.
    #[test]
    fn the_board_is_sized_from_both_rings() {
        for capacity in [
            RingCapacity::MIN,
            RingCapacity::DEFAULT,
            RingCapacity::new(1 << 22).unwrap(),
        ] {
            let size = BoardSize::for_pipeline(capacity);
            let windows = (2 * capacity.slots()).div_ceil(K as usize);
            assert_eq!(size.slots(), windows + 2, "{capacity}");
        }
        assert_eq!(BoardSize::new(1), None);
        assert_eq!(BoardSize::new(2).map(BoardSize::slots), Some(2));
    }

    #[test]
    fn a_kind_round_trips_through_its_byte() {
        assert_eq!(DigestKind::Output.byte(), 1);
        assert_eq!(DigestKind::from_byte(1), Some(DigestKind::Output));
        assert_eq!(DigestKind::from_byte(0), None);
        assert_eq!(DigestKind::from_byte(2), None);
    }

    // ---- the body encode ------------------------------------------------

    /// Body length both of [`skipping`]'s encodes return.
    const SKIPPING_LEN: usize = 16;
    /// The body byte [`skipping`]'s report leaves unwritten.
    const SKIPPED: usize = 1;

    /// An encode that leaves byte [`SKIPPED`] unwritten, as one with a
    /// reserved or padding field might, writing `value` everywhere else.
    fn skipping(value: u8) -> impl Fn(&mut [u8]) -> Result<usize, EncodeError> {
        move |body| {
            body[..SKIPPED].fill(value);
            body[SKIPPED + 1..SKIPPING_LEN].fill(value);
            Ok(SKIPPING_LEN)
        }
    }

    // The zeroing test below encodes through `encode_zeroed`, which skips
    // the purity check: its encoder is impure by design, so through
    // `encode_app_body` a debug build panics before the zeroing is seen.
    // That keeps the release guarantee tested in every build.

    /// The previous body was another response, possibly to another
    /// client: none of it may reach this one through a byte the encoder
    /// skips, and two copies must agree on it whatever each encoded
    /// before.
    #[test]
    fn a_body_byte_the_encoder_skips_is_zero_not_the_previous_bodys() {
        let mut body = [0u8; MAX_RESPONSE_BODY];
        let full = encode_zeroed(&mut body, |b| {
            b.fill(0xFF);
            Ok(SKIPPING_LEN)
        });
        assert_eq!(full, Ok(SKIPPING_LEN));
        assert_eq!(encode_zeroed(&mut body, skipping(7)), Ok(SKIPPING_LEN));
        let expected: Vec<u8> = (0..SKIPPING_LEN)
            .map(|i| if i == SKIPPED { 0 } else { 7 })
            .collect();
        assert_eq!(
            &body[..SKIPPING_LEN],
            &expected[..],
            "the skipped byte carries the previous body's"
        );
    }

    #[test]
    fn the_encoder_is_given_the_whole_body_bound() {
        let mut body = [0u8; MAX_RESPONSE_BODY];
        let n = encode_app_body(&mut body, |b| {
            assert_eq!(b.len(), MAX_RESPONSE_BODY);
            b.fill(1);
            Ok(MAX_RESPONSE_BODY)
        });
        assert_eq!(n, Ok(MAX_RESPONSE_BODY));
    }

    #[test]
    fn a_length_past_the_body_is_refused() {
        let mut body = [0u8; MAX_RESPONSE_BODY];
        let n = encode_app_body(&mut body, |b| {
            b.fill(1);
            Ok(MAX_RESPONSE_BODY + 1)
        });
        assert!(n.is_err());
    }

    #[test]
    fn an_encode_error_passes_through() {
        let mut body = [0u8; MAX_RESPONSE_BODY];
        assert_eq!(encode_app_body(&mut body, |_| Err("no")), Err("no"));
    }

    /// Whatever the purity check does to the region, what is encoded is
    /// what a release build encodes: the body over zeroes, not over
    /// poison.
    #[test]
    fn a_body_is_the_same_in_debug_and_release_builds() {
        let mut checked = [0u8; MAX_RESPONSE_BODY];
        let mut zeroed = [0u8; MAX_RESPONSE_BODY];
        // Writes less than it returns, but the same bytes every time: pure.
        let encode = |body: &mut [u8]| {
            body[0] = 7;
            Ok(1)
        };
        assert_eq!(encode_app_body(&mut checked, encode), Ok(1));
        assert_eq!(encode_zeroed(&mut zeroed, encode), Ok(1));
        assert_eq!(checked, zeroed, "the whole region");
        assert!(
            checked[1..].iter().all(|&b| b == 0),
            "no poison left behind"
        );
    }

    /// Scratch writes past the returned length are not compared, however
    /// they vary: an encoder may use those bytes as it likes.
    #[test]
    fn scratch_past_the_returned_length_passes_the_purity_check() {
        let calls = std::sync::atomic::AtomicU8::new(0);
        let mut body = [0u8; MAX_RESPONSE_BODY];
        let n = encode_app_body(&mut body, |b| {
            b[1] = calls.fetch_add(1, Ordering::Relaxed);
            b[0] = 7;
            Ok(1)
        });
        assert_eq!(n, Ok(1));
        assert_eq!(body[0], 7);
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "impure response encoder: body byte 1 of 16 differs")]
    fn a_skipped_body_byte_fails_the_purity_check() {
        let mut body = [0u8; MAX_RESPONSE_BODY];
        // Dropped result: the call panics before it returns one.
        let _ = encode_app_body(&mut body, skipping(7));
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "impure response encoder: body byte 0 of 1 differs")]
    fn a_body_that_depends_on_the_encoders_state_fails_the_purity_check() {
        let calls = std::sync::atomic::AtomicU8::new(0);
        let mut body = [0u8; MAX_RESPONSE_BODY];
        // Dropped result: the call panics before it returns one.
        let _ = encode_app_body(&mut body, |b| {
            b[0] = calls.fetch_add(1, Ordering::Relaxed);
            Ok(1)
        });
    }

    /// A result that depends on the encoder's state: a length that
    /// changes, or an error one time and not the next.
    #[cfg(debug_assertions)]
    #[test]
    fn a_result_that_depends_on_the_encoders_state_fails_the_purity_check() {
        let results: [[Result<usize, EncodeError>; 2]; 3] = [
            [Ok(1), Ok(2)],
            [Ok(1), Err("no")],
            [Err("no"), Err("other")],
        ];
        for pair in results {
            let calls = std::sync::atomic::AtomicUsize::new(0);
            let encode = |body: &mut [u8]| {
                body[..2].fill(0);
                pair[calls.fetch_add(1, Ordering::Relaxed) % 2]
            };
            let panic = std::panic::catch_unwind(|| {
                let mut body = [0u8; MAX_RESPONSE_BODY];
                encode_app_body(&mut body, encode)
            })
            .expect_err("an impure result must fail the check");
            let message = panic
                .downcast_ref::<String>()
                .expect("a formatted panic message");
            assert!(
                message.starts_with(&format!(
                    "{IMPURE_ENCODER}: two encodes of one value returned {:?}, then {:?}",
                    pair[0], pair[1]
                )),
                "{message}"
            );
        }
    }
}
