//! Handshake chain validation — the primary's cross-node divergence
//! check.
//!
//! A replica's `Handshake` carries `(last_sequence, chain_hash)`: its
//! journal tip and its chain value there. Because segment boundaries
//! are primary-driven (replica journals are bitwise mirrors), the
//! primary can recompute its own chain at that sequence from its
//! journal files and compare. A mismatch — or a claimed sequence beyond
//! the primary's durable tip — means the replica's journal holds
//! divergent history (the normal shape: an ex-primary rejoining after
//! failover with a journaled-but-unreplicated suffix). Streaming new
//! events on top would silently fork the audit trail; the replica must
//! be re-seeded via snapshot resync instead, archiving its divergent
//! journal first.
//!
//! Cold path: runs once per replica connection, before catch-up.

use std::io;

#[cfg(feature = "hash-chain")]
use super::catchup::discover_journal_files;
use super::protocol::Handshake;

/// Outcome of validating a replica's handshake against local history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeValidation {
    /// The replica's chain matches ours at its claimed position — or
    /// the check is not applicable (fresh replica, history pruned past
    /// its position, or `hash-chain` disabled).
    Ok,
    /// The replica's journal is divergent: route it through snapshot
    /// resync (it archives its local journal on receiving the
    /// `HashMismatch` frame).
    Divergent(DivergenceKind),
}

/// Why a handshake was judged divergent — for the operator log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DivergenceKind {
    /// Local chain at the replica's `last_sequence` differs from the
    /// hash it reported.
    ChainMismatch,
    /// The replica claims a sequence beyond this node's durable tip —
    /// history this node never journaled (acked-but-unreplicated
    /// suffix of an ex-primary).
    AheadOfTip,
}

/// Resolution of "this node's chain value at sequence `seq`" against
/// the on-disk lineage.
#[cfg(feature = "hash-chain")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChainAtSequence {
    Value([u8; 32]),
    /// `seq` predates the oldest on-disk segment — pruned history, the
    /// value cannot be recomputed. (Such replicas are routed to
    /// snapshot resync by the catch-up probe anyway.)
    PredatesHistory,
    /// `seq` is beyond this node's durable tip.
    BeyondTip,
}

/// Recompute this node's chain value at `seq` from its journal files.
///
/// Fast path: a segment whose header starts at exactly `seq + 1` —
/// its anchor IS the chain value at `seq`, no scan. Otherwise the
/// containing segment (newest header start ≤ `seq`) is walked,
/// absorbing raw entry bytes up to `seq`.
#[cfg(feature = "hash-chain")]
fn chain_at_sequence(journal_path: &std::path::Path, seq: u64) -> io::Result<ChainAtSequence> {
    use super::catchup::containing_segment;

    let files = discover_journal_files(journal_path);
    // Fast path: a segment opening at exactly `seq + 1` carries the
    // chain value at `seq` as its header anchor — no scan. It is the
    // successor of the containing segment, or the oldest survivor when
    // the containing segment itself was pruned (a replica sitting
    // exactly on the surviving lineage's opening boundary).
    let opener = |idx: usize| -> io::Result<Option<[u8; 32]>> {
        match files.get(idx) {
            Some(path) => {
                let info = melin_journal::segment::read_header_info(path).map_err(|e| {
                    io::Error::other(format!("read header of {}: {e}", path.display()))
                })?;
                Ok((info.starting_sequence == seq + 1).then_some(info.anchor_hash))
            }
            None => Ok(None),
        }
    };
    let Some((idx, _)) = containing_segment(&files, seq)? else {
        return match opener(0)? {
            Some(anchor) => Ok(ChainAtSequence::Value(anchor)),
            None => Ok(ChainAtSequence::PredatesHistory),
        };
    };
    if let Some(anchor) = opener(idx + 1)? {
        return Ok(ChainAtSequence::Value(anchor));
    }
    let path = &files[idx];
    match melin_journal::segment::chain_value_at(path, seq)
        .map_err(|e| io::Error::other(format!("chain value at {seq} in {}: {e}", path.display())))?
    {
        melin_journal::segment::ChainValueAt::Value(v) => Ok(ChainAtSequence::Value(v)),
        // Dense lineage means only the live segment can end before
        // `seq`; either way the replica claims history we don't hold.
        melin_journal::segment::ChainValueAt::BeyondTip => Ok(ChainAtSequence::BeyondTip),
    }
}

/// [`validate_replica_handshake`] with bounded revalidation before a
/// `Divergent` verdict is final. Two transient effects can mimic
/// divergence against a live journal:
///
/// - the journal stage publishes a batch to the replication rings
///   BEFORE its own write is durable, so a fast-reconnecting replica
///   can truthfully claim a sequence this node's files don't contain
///   *yet* (a spurious `AheadOfTip`);
/// - the raw scan races the writer's in-place partial-tail-sector
///   rewrites, so a torn read near the tip can hash garbage (a
///   spurious `ChainMismatch`).
///
/// Both clear within a flush; real divergence is permanent. The retry
/// budget (~400 ms) is noise against the snapshot resync a false
/// verdict would trigger.
pub fn validate_replica_handshake_settled(
    journal_path: &std::path::Path,
    handshake: &Handshake,
) -> io::Result<HandshakeValidation> {
    const ATTEMPTS: u32 = 8;
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

    let mut verdict = validate_replica_handshake(journal_path, handshake)?;
    for _ in 1..ATTEMPTS {
        if verdict == HandshakeValidation::Ok {
            break;
        }
        std::thread::sleep(RETRY_DELAY);
        verdict = validate_replica_handshake(journal_path, handshake)?;
    }
    Ok(verdict)
}

/// Validate a replica's handshake `(last_sequence, chain_hash)` against
/// this node's journal. Shared by the kernel-TCP and DPDK senders —
/// production callers use [`validate_replica_handshake_settled`], which
/// retries transient false-divergence verdicts.
///
/// A fresh replica (`last_sequence == 0`) has nothing to compare — its
/// reported hash is zeros, not a chain value. With `hash-chain`
/// disabled there is no chain to compare either; every handshake
/// validates `Ok` (both sides report zeros).
///
/// A replica built *without* `hash-chain` talking to a primary built
/// with it also reports all-zeros at a non-zero position (it has no
/// chain to report — a real BLAKE3 chain value is never zeros). The
/// chain comparison is skipped for such peers rather than judging the
/// mixed-feature pair divergent on every reconnect, but `AheadOfTip`
/// is still enforced — it is a sequence property, not a chain one.
pub fn validate_replica_handshake(
    journal_path: &std::path::Path,
    handshake: &Handshake,
) -> io::Result<HandshakeValidation> {
    if handshake.last_sequence == 0 {
        return Ok(HandshakeValidation::Ok);
    }
    #[cfg(feature = "hash-chain")]
    {
        let chainless_peer = handshake.chain_hash == [0u8; 32];
        match chain_at_sequence(journal_path, handshake.last_sequence)? {
            ChainAtSequence::Value(_) if chainless_peer => Ok(HandshakeValidation::Ok),
            ChainAtSequence::Value(local) if local == handshake.chain_hash => {
                Ok(HandshakeValidation::Ok)
            }
            ChainAtSequence::Value(_) => Ok(HandshakeValidation::Divergent(
                DivergenceKind::ChainMismatch,
            )),
            ChainAtSequence::PredatesHistory => Ok(HandshakeValidation::Ok),
            ChainAtSequence::BeyondTip => {
                Ok(HandshakeValidation::Divergent(DivergenceKind::AheadOfTip))
            }
        }
    }
    #[cfg(not(feature = "hash-chain"))]
    {
        let _ = journal_path;
        Ok(HandshakeValidation::Ok)
    }
}

#[cfg(all(test, feature = "hash-chain"))]
mod tests {
    use super::*;
    use crate::test_support::TestEvent;
    use melin_journal::{BufferedWriter, JournalEvent, JournalWrite};

    /// Two-segment journal (rotation after seq 2), entries 1..=4.
    fn journal(dir: &std::path::Path) -> (std::path::PathBuf, Vec<[u8; 32]>) {
        let live = dir.join("v.journal");
        let mut w = BufferedWriter::<TestEvent>::create(&live).unwrap();
        let mut chains = Vec::new();
        for v in 1..=4u64 {
            w.append(&JournalEvent::App(TestEvent::Add(v))).unwrap();
            chains.push(w.chain_hash().unwrap());
            if v == 2 {
                w.rotate_segment().unwrap();
            }
        }
        (live, chains)
    }

    fn hs(last_sequence: u64, chain_hash: [u8; 32]) -> Handshake {
        Handshake {
            last_sequence,
            chain_hash,
            epoch: 0,
            authorized_keys_hash: [0u8; 32],
        }
    }

    /// A truthful replica validates at every position — mid-segment,
    /// at the rotation boundary (where the next segment's anchor
    /// answers without a scan), and at the live tip.
    #[test]
    fn truthful_replica_validates_at_every_position() {
        let dir = tempfile::tempdir().unwrap();
        let (live, chains) = journal(dir.path());

        for (i, chain) in chains.iter().enumerate() {
            let seq = i as u64 + 1;
            assert_eq!(
                validate_replica_handshake(&live, &hs(seq, *chain)).unwrap(),
                HandshakeValidation::Ok,
                "seq {seq}"
            );
        }
    }

    /// A replica reporting a wrong hash at any position is divergent.
    #[test]
    fn wrong_hash_is_chain_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let (live, _) = journal(dir.path());

        for seq in 1..=4u64 {
            assert_eq!(
                validate_replica_handshake(&live, &hs(seq, [0xEE; 32])).unwrap(),
                HandshakeValidation::Divergent(DivergenceKind::ChainMismatch),
                "seq {seq}"
            );
        }
    }

    /// A replica claiming history past our tip is divergent — the
    /// rejoining-ex-primary shape.
    #[test]
    fn sequence_beyond_tip_is_divergent() {
        let dir = tempfile::tempdir().unwrap();
        let (live, chains) = journal(dir.path());

        assert_eq!(
            validate_replica_handshake(&live, &hs(5, chains[3])).unwrap(),
            HandshakeValidation::Divergent(DivergenceKind::AheadOfTip)
        );
    }

    /// A replica built without `hash-chain` reports an all-zeros hash
    /// at a real position: the chain comparison is skipped (mixed
    /// feature builds interoperate with reduced verification), but
    /// `AheadOfTip` — a sequence property — is still enforced.
    #[test]
    fn chainless_peer_skips_chain_check_but_not_tip_check() {
        let dir = tempfile::tempdir().unwrap();
        let (live, _chains) = journal(dir.path());

        for seq in 1..=4u64 {
            assert_eq!(
                validate_replica_handshake(&live, &hs(seq, [0u8; 32])).unwrap(),
                HandshakeValidation::Ok,
                "seq {seq}"
            );
        }
        assert_eq!(
            validate_replica_handshake(&live, &hs(5, [0u8; 32])).unwrap(),
            HandshakeValidation::Divergent(DivergenceKind::AheadOfTip),
            "ahead-of-tip must still be caught for chain-less peers"
        );
    }

    /// Fresh replicas and positions behind pruned history can't be
    /// checked — not divergence.
    #[test]
    fn fresh_and_pruned_positions_validate_ok() {
        let dir = tempfile::tempdir().unwrap();
        let (live, chains) = journal(dir.path());

        assert_eq!(
            validate_replica_handshake(&live, &hs(0, [0u8; 32])).unwrap(),
            HandshakeValidation::Ok,
            "fresh replica"
        );

        // Prune the oldest archive: seq 1 now predates history. Seq 2
        // (the surviving lineage's opening boundary) remains checkable
        // via the live... archive's anchor.
        std::fs::remove_file(dir.path().join("v.journal.000001")).unwrap();
        assert_eq!(
            validate_replica_handshake(&live, &hs(1, chains[0])).unwrap(),
            HandshakeValidation::Ok,
            "pruned history cannot be checked"
        );
        assert_eq!(
            validate_replica_handshake(&live, &hs(2, chains[1])).unwrap(),
            HandshakeValidation::Ok,
            "boundary of surviving lineage still checkable"
        );
        assert_eq!(
            validate_replica_handshake(&live, &hs(2, [0xEE; 32])).unwrap(),
            HandshakeValidation::Divergent(DivergenceKind::ChainMismatch),
        );
    }
}
