//! Bootstrap a fresh replica journal from the primary's genesis entry.
//!
//! Replicas join a cluster by receiving the primary's genesis entry over
//! the replication wire; this helper materialises it as a well-formed
//! on-disk journal and returns a writer ready to append. Encapsulates
//! the codec-internal details (header layout at [`crate::codec::ENTRY_OFFSET`],
//! genesis-derived chain anchor, initial `valid_end`, durable `sync_all`
//! before reopening) so transport-layer code (TCP / DPDK receivers)
//! doesn't have to import the codec or compute hashes itself.

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

use melin_app::AppEvent;

use crate::error::JournalError;
use crate::write::JournalWrite;

/// Lay down a journal file at `path` containing the standard file
/// header plus `genesis_entry` verbatim, durably synced. Returns
/// `(valid_end, chain_anchor)` for reopening via
/// [`JournalWrite::open_append`]. The chain anchor is derived from the
/// entry minus its trailing 4 CRC bytes (matching `JournalReader`'s
/// genesis detection); `None` when `hash-chain` is disabled.
fn lay_down_genesis_segment(
    path: &Path,
    genesis_entry: &[u8],
) -> Result<(u64, Option<[u8; 32]>), JournalError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)?;
    // Header is exactly ENTRY_OFFSET bytes under v13, regardless of
    // device logical sector size — see codec::ENTRY_OFFSET docs.
    let mut header = vec![0u8; crate::codec::MAX_SECTOR_SIZE];
    crate::codec::encode_file_header(&mut header, crate::codec::MAX_SECTOR_SIZE);
    file.write_all_at(&header, 0)?;
    file.write_all_at(genesis_entry, crate::codec::ENTRY_OFFSET)?;
    file.sync_all()?;
    drop(file);

    #[cfg(feature = "hash-chain")]
    let chain_hash: Option<[u8; 32]> = {
        // Genesis-entry hash anchors the chain on the replica.
        // Matches reader.rs: hash over entry bytes minus trailing
        // 4 CRC bytes.
        let entry_len = genesis_entry.len();
        if entry_len < 4 {
            return Err(JournalError::Io(std::io::Error::other(
                "genesis entry shorter than CRC suffix",
            )));
        }
        Some(*blake3::hash(&genesis_entry[..entry_len - 4]).as_bytes())
    };
    #[cfg(not(feature = "hash-chain"))]
    let chain_hash: Option<[u8; 32]> = None;

    let valid_end = crate::codec::ENTRY_OFFSET + genesis_entry.len() as u64;
    Ok((valid_end, chain_hash))
}

/// Lay down a fresh replica journal at `path` and reopen it via `W` for
/// append. `genesis_entry` is the raw entry bytes as written on the
/// primary (header + payload + CRC). It is written verbatim at
/// [`crate::codec::ENTRY_OFFSET`].
pub fn create_fresh_replica<E, W>(path: &Path, genesis_entry: &[u8]) -> Result<W, JournalError>
where
    E: AppEvent,
    W: JournalWrite<E>,
{
    let (valid_end, chain_hash) = lay_down_genesis_segment(path, genesis_entry)?;
    W::open_append(
        path, 1, // genesis consumed sequence 1
        valid_end, chain_hash, 0, // events_since_checkpoint
    )
}

/// Rotate a replica's live segment at a primary-driven rotation point.
///
/// The primary's rotation `GenesisHash` entry consumes a sequence
/// number and re-seeds the hash chain, so it is published through the
/// replication stream and replicas must hold the identical entry at the
/// identical position. This helper re-encodes the entry from the
/// decoded slot fields — bit-identical to the primary's on-disk bytes,
/// since the codec is deterministic and the timestamp travels with the
/// slot — archives the current live segment, and reopens a fresh one
/// seeded from that entry. The new segment starts with
/// `events_since_checkpoint = 0`, matching the primary's post-rotation
/// writer so checkpoint auto-emission stays sequence-aligned.
///
/// On laydown failure after the archive rename, the rename is undone
/// (best effort) so recovery still finds a live file — mirroring
/// `rotate_segment`'s rollback contract.
pub fn rotate_adopting_genesis<E, W>(
    writer: &mut W,
    sequence: u64,
    timestamp_ns: u64,
    prev_chain_hash: [u8; 32],
) -> Result<std::path::PathBuf, JournalError>
where
    E: AppEvent,
    W: JournalWrite<E>,
{
    // Flush anything encoded-but-unsynced into the outgoing segment so
    // the archive is complete before the rename.
    writer.flush_batch_sync()?;
    let path = writer.path().to_path_buf();

    // Re-encode the primary's genesis entry from the slot fields.
    // 128 bytes comfortably holds header + metadata + 32-byte payload
    // + CRC (73 bytes today).
    let genesis_event: crate::JournalEvent<E> = crate::JournalEvent::GenesisHash {
        hash: prev_chain_hash,
    };
    let mut buf = [0u8; 128];
    let written = crate::codec::encode(sequence, timestamp_ns, 0, 0, &genesis_event, &mut buf)?;

    let archived = crate::segment::archive_live(&path).map_err(JournalError::Io)?;

    let reopened =
        lay_down_genesis_segment(&path, &buf[..written]).and_then(|(valid_end, chain_hash)| {
            W::open_append(&path, sequence, valid_end, chain_hash, 0)
        });
    match reopened {
        Ok(w) => {
            *writer = w;
            // Durably commit both renamed-archive and new-live dirents
            // in one dir fsync.
            crate::segment::fsync_parent_dir(&path).map_err(JournalError::Io)?;
            Ok(archived)
        }
        Err(e) => {
            // Best-effort: restore the live file so the next recovery
            // sees the pre-rotation layout.
            if let Err(re) = std::fs::rename(&archived, &path) {
                return Err(JournalError::Io(std::io::Error::other(format!(
                    "rotation laydown failed ({e}) and archive restore also failed ({re}) — \
                     live segment left at {}",
                    archived.display()
                ))));
            }
            Err(e)
        }
    }
}
