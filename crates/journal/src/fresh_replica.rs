//! Bootstrap a fresh replica journal from the primary's genesis entry.
//!
//! Replicas join a cluster by receiving the primary's genesis entry over
//! the replication wire; this helper materialises it as a well-formed
//! on-disk journal and returns a writer ready to append. Encapsulates
//! the codec-internal details (header layout at [`crate::codec::ENTRY_OFFSET`],
//! genesis-derived chain anchor, initial `valid_end`, durable `sync_all`
//! before reopening) so transport-layer code (TCP / rumcast / DPDK
//! receivers) doesn't have to import the codec or compute hashes itself.

use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use std::path::Path;

use melin_app::AppEvent;

use crate::error::JournalError;
use crate::write::JournalWrite;

/// Lay down a fresh replica journal at `path` and reopen it via `W` for
/// append. `genesis_entry` is the raw entry bytes as written on the
/// primary (header + payload + CRC). It is written verbatim at
/// [`crate::codec::ENTRY_OFFSET`]. When the `hash-chain` feature is
/// enabled, the chain anchor is derived from the entry minus its
/// trailing 4 CRC bytes (matching `JournalReader`'s genesis detection).
pub fn create_fresh_replica<E, W>(path: &Path, genesis_entry: &[u8]) -> Result<W, JournalError>
where
    E: AppEvent,
    W: JournalWrite<E>,
{
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
    W::open_append(
        path, 1, // genesis consumed sequence 1
        valid_end, chain_hash, 0, // events_since_checkpoint
    )
}
