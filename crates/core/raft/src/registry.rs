//! The cluster membership registry — the control-plane's replicated
//! state machine.
//!
//! Each node's identity record (raft RPC address, replication
//! data-plane address, pinned public key) is proposed into the raft
//! log and applied here, replacing the per-node `--raft-peer` flag
//! lists as the source of truth once the cluster is up. Static flags
//! remain the *bootstrap hint* (initial voter set on first boot, dial
//! targets before any record commits); divergent static configs are
//! then corrected by the log — records are leader-serialized, so every
//! node converges on the same directory.
//!
//! The registry deliberately carries the **directory** only. The voter
//! set itself stays in raft's `ConfState` (changed via `ConfChange`
//! entries, a later step) — conflating the two would put membership
//! *authority* in an ordinary log entry, which raft reserves for conf
//! changes.

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;

/// One node's identity record.
///
/// `PartialEq` drives the "did anything change" signal: re-announcing
/// an identical record must not count as a registry change (nodes
/// re-announce on every leader change).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberRecord {
    /// The node's raft id.
    pub node_id: u64,
    /// The node's raft RPC address, as routable by its peers (its
    /// `--raft-bind`, or an advertise override when binding a
    /// wildcard).
    pub raft_addr: SocketAddr,
    /// The node's replication data-plane listen address
    /// (`--replication-bind`) — where a replica dials to follow this
    /// node when it leads. `None` when the node cannot serve replicas.
    pub replication_addr: Option<SocketAddr>,
    /// The node's client order-entry address (`--bind`) — where a
    /// redirected client reconnects when this node leads. `None` when
    /// the node has no routable client address to announce.
    pub order_entry_addr: Option<SocketAddr>,
    /// The fencing epoch under which this node currently acts as the
    /// serving primary; `None` while it is a replica. Self-reported,
    /// leader-serialized. Resolution picks the live claim with the
    /// highest epoch (fencing order, not announcement order), so a
    /// superseded primary's stale claim is outranked the instant the new
    /// primary announces — no tombstones, no revocation path.
    pub serving_epoch: Option<u64>,
    /// The node's Ed25519 replication public key, pinning its identity
    /// on control-plane connections.
    pub public_key: [u8; 32],
}

/// Registry entry-payload version. Bump when the record layout
/// changes; newer versions are skipped by appliers (a newer node's
/// record must not brick an older one — see [`Registry::apply`]) and
/// older ones decode with their layout's fields (missing fields
/// default, and the node's next re-announce upgrades its record).
///
/// v1: node_id + raft_addr + replication_addr + public_key.
/// v2: + order_entry_addr (before public_key).
/// v3: + serving_epoch (after order_entry_addr, before public_key).
const RECORD_VERSION: u8 = 3;

impl MemberRecord {
    /// Serialize for a raft log entry. Little-endian, length-prefixed
    /// address strings (the codebase's hand-rolled-codec convention;
    /// addresses as display strings keep the format v4/v6-agnostic and
    /// the payload is control-plane-tiny).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(96);
        buf.push(RECORD_VERSION);
        buf.extend_from_slice(&self.node_id.to_le_bytes());
        encode_addr(&mut buf, Some(self.raft_addr));
        encode_addr(&mut buf, self.replication_addr);
        encode_addr(&mut buf, self.order_entry_addr);
        encode_epoch(&mut buf, self.serving_epoch);
        buf.extend_from_slice(&self.public_key);
        buf
    }

    /// Decode a log-entry payload. `Ok(None)` for a *newer* record
    /// version (skip, don't fail — the cluster may be mid-upgrade);
    /// `Err` for a genuinely malformed payload.
    pub fn decode(buf: &[u8]) -> io::Result<Option<Self>> {
        let mut r = buf;
        let version = take_u8(&mut r)?;
        if version > RECORD_VERSION {
            return Ok(None);
        }
        let node_id = take_u64(&mut r)?;
        let raft_addr = take_addr(&mut r)?
            .ok_or_else(|| io::Error::other("member record without a raft address"))?;
        let replication_addr = take_addr(&mut r)?;
        // v1 records predate the order-entry address; the node's next
        // re-announce (always current-version) fills it in. One
        // wrinkle: an interim build shipped the three-address layout
        // still tagged v1, so persisted state may hold either v1
        // shape. The remainder length disambiguates exactly — a true
        // v1 record has only the 32 public-key bytes left here, the
        // interim one has an address block first.
        let order_entry_addr = if version >= 2 || r.len() > 32 {
            take_addr(&mut r)?
        } else {
            None
        };
        // v1/v2 predate the serving claim; those nodes announce as
        // replicas until their next current-version re-announce fills it
        // in. Gated on the version byte (not remainder length) — v2's
        // tail is already an unambiguous 32-byte key, so no v1-style
        // length disambiguation is needed here.
        let serving_epoch = if version >= 3 {
            take_epoch(&mut r)?
        } else {
            None
        };
        let public_key: [u8; 32] = take_bytes(&mut r, 32)?
            .try_into()
            .map_err(|_| io::Error::other("short public key"))?;
        if !r.is_empty() {
            return Err(io::Error::other("trailing bytes after member record"));
        }
        Ok(Some(Self {
            node_id,
            raft_addr,
            replication_addr,
            order_entry_addr,
            serving_epoch,
            public_key,
        }))
    }
}

/// The applied directory: node id → latest committed record.
///
/// `BTreeMap` (not `HashMap`) so the serialized state is deterministic
/// — every node must persist byte-identical registry snapshots for a
/// given applied prefix, and iteration order feeds the encoder.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Registry {
    members: BTreeMap<u64, MemberRecord>,
}

impl Registry {
    /// Apply one committed log-entry payload. Returns `true` when the
    /// registry changed (drives the driver's re-wiring), `false` for
    /// an identical re-announce, a skipped newer-version record, or an
    /// undecodable payload (logged by the caller — a malformed entry
    /// was still *committed*, so failing the apply would wedge every
    /// node identically; skipping it converges).
    pub fn apply(&mut self, payload: &[u8]) -> bool {
        match MemberRecord::decode(payload) {
            Ok(Some(record)) => {
                let changed = self.members.get(&record.node_id) != Some(&record);
                if changed {
                    self.members.insert(record.node_id, record);
                }
                changed
            }
            Ok(None) => false,
            Err(e) => {
                tracing::debug!(error = %e, "skipping undecodable registry entry");
                false
            }
        }
    }

    /// Drop the record for `node_id`. Returns `true` when a record was
    /// actually removed (drives the driver's re-wiring so it stops
    /// dialing the departed node), `false` when there was none. Applied
    /// as the deterministic side effect of a committed `RemoveNode` conf
    /// change, so every node prunes the same record at the same index.
    pub fn remove(&mut self, node_id: u64) -> bool {
        self.members.remove(&node_id).is_some()
    }

    /// The committed record for `node_id`, if any.
    pub fn get(&self, node_id: u64) -> Option<&MemberRecord> {
        self.members.get(&node_id)
    }

    /// All committed records, ascending by node id.
    pub fn iter(&self) -> impl Iterator<Item = &MemberRecord> {
        self.members.values()
    }

    /// Number of committed records.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Serialize the whole registry (the raft snapshot / state-file
    /// payload): record count then each record in ascending-id order.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + self.members.len() * 96);
        // u16 count: same bound rationale as the state file's id lists —
        // node counts are tiny and the cap keeps a corrupt count from
        // allocating gigabytes.
        buf.extend_from_slice(&(self.members.len() as u16).to_le_bytes());
        for record in self.members.values() {
            let bytes = record.encode();
            buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
            buf.extend_from_slice(&bytes);
        }
        buf
    }

    /// Rebuild from a serialized registry (boot, or an incoming raft
    /// snapshot). An empty payload is an empty registry (fresh
    /// cluster). Unknown-version records inside are skipped, same
    /// contract as [`Self::apply`].
    pub fn decode(buf: &[u8]) -> io::Result<Self> {
        if buf.is_empty() {
            return Ok(Self::default());
        }
        let mut r = buf;
        let count = take_u16(&mut r)? as usize;
        let mut members = BTreeMap::new();
        for _ in 0..count {
            let len = take_u16(&mut r)? as usize;
            let bytes = take_bytes(&mut r, len)?;
            if let Some(record) = MemberRecord::decode(bytes)? {
                members.insert(record.node_id, record);
            }
        }
        if !r.is_empty() {
            return Err(io::Error::other("trailing bytes after registry"));
        }
        Ok(Self { members })
    }
}

fn encode_addr(buf: &mut Vec<u8>, addr: Option<SocketAddr>) {
    match addr {
        Some(addr) => {
            let s = addr.to_string();
            buf.push(s.len() as u8);
            buf.extend_from_slice(s.as_bytes());
        }
        // Length 0 = absent ("[::]:0"-style sentinels would be lies).
        None => buf.push(0),
    }
}

/// Length-prefixed optional epoch: a presence byte (0 = absent,
/// 8 = present) followed by the 8 LE bytes when present. Mirrors
/// [`encode_addr`]'s "length 0 = absent" convention so an absent claim
/// costs one byte and the tail stays self-describing.
fn encode_epoch(buf: &mut Vec<u8>, epoch: Option<u64>) {
    match epoch {
        Some(e) => {
            buf.push(8);
            buf.extend_from_slice(&e.to_le_bytes());
        }
        None => buf.push(0),
    }
}

fn take_epoch(r: &mut &[u8]) -> io::Result<Option<u64>> {
    match take_u8(r)? {
        0 => Ok(None),
        8 => Ok(Some(take_u64(r)?)),
        other => Err(io::Error::other(format!(
            "invalid serving-epoch length {other} in member record"
        ))),
    }
}

fn take_u8(r: &mut &[u8]) -> io::Result<u8> {
    Ok(take_bytes(r, 1)?[0])
}

fn take_u16(r: &mut &[u8]) -> io::Result<u16> {
    Ok(u16::from_le_bytes(
        take_bytes(r, 2)?.try_into().expect("len 2"),
    ))
}

fn take_u64(r: &mut &[u8]) -> io::Result<u64> {
    Ok(u64::from_le_bytes(
        take_bytes(r, 8)?.try_into().expect("len 8"),
    ))
}

fn take_bytes<'a>(r: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if r.len() < n {
        return Err(io::Error::other("truncated member record"));
    }
    let (head, tail) = r.split_at(n);
    *r = tail;
    Ok(head)
}

fn take_addr(r: &mut &[u8]) -> io::Result<Option<SocketAddr>> {
    let len = take_u8(r)? as usize;
    if len == 0 {
        return Ok(None);
    }
    let s = std::str::from_utf8(take_bytes(r, len)?)
        .map_err(|_| io::Error::other("non-UTF-8 address in member record"))?;
    let addr = s
        .parse()
        .map_err(|_| io::Error::other("unparseable address in member record"))?;
    Ok(Some(addr))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: u64) -> MemberRecord {
        MemberRecord {
            node_id: id,
            raft_addr: format!("127.0.0.1:{}", 7000 + id).parse().expect("addr"),
            replication_addr: Some(format!("10.0.0.{id}:9877").parse().expect("addr")),
            order_entry_addr: Some(format!("10.0.0.{id}:9876").parse().expect("addr")),
            serving_epoch: None,
            public_key: [id as u8; 32],
        }
    }

    #[test]
    fn record_round_trips() {
        let r = record(3);
        assert_eq!(MemberRecord::decode(&r.encode()).expect("decode"), Some(r));
        // Absent replication address round-trips too.
        let r = MemberRecord {
            replication_addr: None,
            ..record(4)
        };
        assert_eq!(MemberRecord::decode(&r.encode()).expect("decode"), Some(r));
    }

    #[test]
    fn v3_serving_claim_round_trips() {
        // A serving primary announces its fencing epoch; a replica
        // announces none. Both must survive the codec unchanged.
        let claim = MemberRecord {
            serving_epoch: Some(42),
            ..record(3)
        };
        assert_eq!(
            MemberRecord::decode(&claim.encode()).expect("decode"),
            Some(claim)
        );
        let no_claim = record(4); // serving_epoch: None
        assert_eq!(
            MemberRecord::decode(&no_claim.encode()).expect("decode"),
            Some(no_claim)
        );
    }

    #[test]
    fn v2_record_decodes_without_serving_claim() {
        // A record persisted before the serving claim existed (v2: three
        // addresses, no epoch tail) must keep decoding, with
        // serving_epoch defaulting to None until the node re-announces.
        let expected = record(5); // serving_epoch: None
        let mut v2 = Vec::new();
        v2.push(2u8);
        v2.extend_from_slice(&expected.node_id.to_le_bytes());
        encode_addr(&mut v2, Some(expected.raft_addr));
        encode_addr(&mut v2, expected.replication_addr);
        encode_addr(&mut v2, expected.order_entry_addr);
        v2.extend_from_slice(&expected.public_key);

        assert_eq!(MemberRecord::decode(&v2).expect("decode"), Some(expected));
    }

    #[test]
    fn invalid_serving_epoch_length_is_an_error() {
        // A v3 record whose epoch presence byte is neither 0 nor 8 is
        // malformed — reject rather than silently mis-parse the tail.
        let mut bytes = record(1).encode();
        // The epoch presence byte sits just before the 32-byte key.
        let epoch_pos = bytes.len() - 33;
        assert_eq!(bytes[epoch_pos], 0, "helper record has no claim");
        bytes[epoch_pos] = 4; // an impossible epoch length
        assert!(MemberRecord::decode(&bytes).is_err());
    }

    #[test]
    fn newer_record_version_is_skipped_not_an_error() {
        let mut bytes = record(1).encode();
        bytes[0] = RECORD_VERSION + 1;
        assert_eq!(MemberRecord::decode(&bytes).expect("decode"), None);
    }

    #[test]
    fn interim_v1_record_with_order_entry_addr_decodes() {
        // One branch build wrote the three-address layout still tagged
        // version 1. Persisted raft state from those machines must keep
        // loading: the remainder length (address block + key vs bare
        // key) disambiguates the two v1 shapes.
        let expected = record(6);
        let mut interim = Vec::new();
        interim.push(1u8);
        interim.extend_from_slice(&expected.node_id.to_le_bytes());
        encode_addr(&mut interim, Some(expected.raft_addr));
        encode_addr(&mut interim, expected.replication_addr);
        encode_addr(&mut interim, expected.order_entry_addr);
        interim.extend_from_slice(&expected.public_key);

        assert_eq!(
            MemberRecord::decode(&interim).expect("decode"),
            Some(expected)
        );

        // Interim shape with an ABSENT order-entry address (a node
        // without a routable bind): one presence byte more than bare
        // v1 — still disambiguated by length.
        let expected = MemberRecord {
            order_entry_addr: None,
            ..record(7)
        };
        let mut interim = Vec::new();
        interim.push(1u8);
        interim.extend_from_slice(&expected.node_id.to_le_bytes());
        encode_addr(&mut interim, Some(expected.raft_addr));
        encode_addr(&mut interim, expected.replication_addr);
        encode_addr(&mut interim, None);
        interim.extend_from_slice(&expected.public_key);

        assert_eq!(
            MemberRecord::decode(&interim).expect("decode"),
            Some(expected)
        );
    }

    #[test]
    fn v1_record_decodes_without_order_entry_addr() {
        // A record persisted before the order-entry address existed
        // (raft log entries, FileStorage app_state, snapshots) must
        // keep decoding: same fields, order_entry_addr defaults to
        // None until the node's next re-announce upgrades it.
        let expected = MemberRecord {
            order_entry_addr: None,
            ..record(5)
        };
        // Hand-build the v1 layout: version, node_id, raft_addr,
        // replication_addr, public_key — no order-entry address.
        let mut v1 = Vec::new();
        v1.push(1u8);
        v1.extend_from_slice(&expected.node_id.to_le_bytes());
        encode_addr(&mut v1, Some(expected.raft_addr));
        encode_addr(&mut v1, expected.replication_addr);
        v1.extend_from_slice(&expected.public_key);

        assert_eq!(MemberRecord::decode(&v1).expect("decode"), Some(expected));
    }

    #[test]
    fn malformed_record_is_an_error() {
        assert!(MemberRecord::decode(&[]).is_err());
        let mut truncated = record(1).encode();
        truncated.truncate(truncated.len() - 5);
        assert!(MemberRecord::decode(&truncated).is_err());
        let mut trailing = record(1).encode();
        trailing.push(0xFF);
        assert!(MemberRecord::decode(&trailing).is_err());
    }

    #[test]
    fn apply_reports_changes_and_ignores_identical_reannounce() {
        let mut reg = Registry::default();
        assert!(reg.apply(&record(1).encode()));
        assert!(!reg.apply(&record(1).encode()), "identical re-announce");
        // A changed address for the same id is a change.
        let moved = MemberRecord {
            raft_addr: "127.0.0.1:9999".parse().expect("addr"),
            ..record(1)
        };
        assert!(reg.apply(&moved.encode()));
        assert_eq!(reg.get(1), Some(&moved));
        // Garbage payloads are skipped, not fatal (the entry committed).
        assert!(!reg.apply(b"\xffgarbage"));
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn registry_round_trips_and_orders_by_id() {
        let mut reg = Registry::default();
        for id in [3, 1, 2] {
            reg.apply(&record(id).encode());
        }
        let decoded = Registry::decode(&reg.encode()).expect("decode");
        assert_eq!(decoded, reg);
        let ids: Vec<u64> = decoded.iter().map(|r| r.node_id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        // Determinism: same contents ⇒ same bytes.
        assert_eq!(decoded.encode(), reg.encode());
    }

    #[test]
    fn empty_payload_is_an_empty_registry() {
        assert!(Registry::decode(&[]).expect("decode").is_empty());
    }
}
