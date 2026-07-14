//! Control-plane raft wiring: CLI validation, driver spawn, and the
//! replica-mode health endpoint.
//!
//! The consensus machinery lives in `melin-raft` (with its confined tokio
//! runtime); this module is the glue between `ServerConfig` and
//! `melin_raft::driver::spawn`, shared by the kernel-TCP and DPDK paths.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use melin_app::auth::AuthorizedKeys;
use melin_raft::driver::{RaftConfig, RaftHandles, RaftPeer};
use tracing::info;

use crate::server::ServerConfig;

/// Decode a base64 (standard alphabet) Ed25519 public key — the same
/// encoding `authorized_keys` uses — into raw 32 bytes.
fn parse_ed25519_pubkey_b64(b64: &str) -> Result<[u8; 32], String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64)
        .map_err(|e| format!("invalid base64: {e}"))?;
    let arr: [u8; 32] = bytes
        .as_slice()
        .try_into()
        .map_err(|_| format!("expected 32 bytes, got {}", bytes.len()))?;
    Ok(arr)
}

/// Parse one `--raft-peer` entry: `id@host:port#base64-pubkey`.
fn parse_raft_peer(entry: &str) -> Result<RaftPeer, String> {
    let (id_str, rest) = entry
        .split_once('@')
        .ok_or_else(|| format!("--raft-peer '{entry}': expected id@host:port#pubkey"))?;
    let (addr, pubkey_b64) = rest
        .split_once('#')
        .ok_or_else(|| format!("--raft-peer '{entry}': missing '#base64-pubkey'"))?;
    let id: u64 = id_str
        .parse()
        .map_err(|e| format!("--raft-peer '{entry}': bad node id: {e}"))?;
    if addr.parse::<std::net::SocketAddr>().is_err() {
        return Err(format!("--raft-peer '{entry}': '{addr}' is not host:port"));
    }
    let pubkey =
        parse_ed25519_pubkey_b64(pubkey_b64).map_err(|e| format!("--raft-peer '{entry}': {e}"))?;
    Ok(RaftPeer {
        id,
        addr: addr.to_owned(),
        pubkey,
    })
}

/// Validate the raft CLI flags into a [`RaftConfig`].
///
/// `Ok(None)` when raft is not configured. Partial flag sets are refused —
/// a node with `--raft-node-id` but no `--raft-bind` is a misconfiguration
/// the operator must resolve, not a mode.
pub(crate) fn build_raft_config(config: &ServerConfig) -> Result<Option<RaftConfig>, String> {
    let Some(bind) = config.raft_bind else {
        if config.raft_node_id.is_some()
            || !config.raft_peer.is_empty()
            || config.raft_dir.is_some()
            || config.raft_auto_promote
        {
            return Err(
                "raft flags (--raft-node-id/--raft-peer/--raft-dir) require --raft-bind".to_owned(),
            );
        }
        return Ok(None);
    };
    let node_id = config
        .raft_node_id
        .ok_or_else(|| "--raft-bind requires --raft-node-id".to_owned())?;
    if config.replication_key.is_none() {
        return Err(
            "--raft-bind requires --replication-key (peer links authenticate with it)".to_owned(),
        );
    }
    let peers = config
        .raft_peer
        .iter()
        .map(|e| parse_raft_peer(e))
        .collect::<Result<Vec<_>, _>>()?;
    if !peers.iter().any(|p| p.id == node_id) {
        return Err(format!(
            "--raft-peer list must include this node (id {node_id}) with its dialable address"
        ));
    }
    if config.raft_auto_promote && peers.len() < 3 {
        return Err(format!(
            "--raft-auto-promote requires at least 3 configured voters, got {} — a two-node              cluster cannot elect a leader after losing either node, so automatic failover              would never fire while inviting misconfiguration; run manual PROMOTE instead",
            peers.len()
        ));
    }
    let dir = config
        .raft_dir
        .clone()
        .unwrap_or_else(|| config.journal.with_extension("raft"));
    Ok(Some(RaftConfig {
        node_id,
        bind,
        dir,
        peers,
    }))
}

/// Spawn the raft driver if configured. Returns `None` when raft is off.
///
/// Called on every mode path (primary, replica, DPDK variants) right after
/// the fence state exists — the driver shares the process `shutdown` flag
/// and survives a replica → primary promotion untouched.
pub(crate) fn spawn_raft_driver(
    config: &ServerConfig,
    signing_key: &ed25519_dalek::SigningKey,
    authorized_keys: &Arc<AuthorizedKeys>,
    fence_state: &Arc<melin_transport_core::fence::FenceState>,
    journal_tip: melin_transport_core::AdvertisedJournalTip,
    tip_ready: Arc<AtomicBool>,
    // Whether this node currently claims to be serving — the
    // fence-on-supersession trigger (see `SupersessionPolicy`).
    // Primaries pass `|| true`; replicas pass a promotion-filed check.
    serving_claim: Arc<dyn Fn() -> bool + Send + Sync>,
    shutdown: &Arc<AtomicBool>,
) -> Result<Option<RaftHandles>, Box<dyn std::error::Error>> {
    let Some(raft_config) = build_raft_config(config)? else {
        return Ok(None);
    };
    info!(
        node_id = raft_config.node_id,
        bind = %raft_config.bind,
        voters = raft_config.peers.len(),
        dir = %raft_config.dir.display(),
        "starting control-plane raft driver"
    );
    let tip = Arc::new(melin_raft::recency::TipSource {
        fence: Arc::clone(fence_state),
        seq: journal_tip,
        ready: tip_ready,
    });
    // The raft mesh doubles as a fencing channel only under
    // auto-promotion: without automation, a serving node's fencing
    // stays a data-plane-contact concern exactly as documented.
    let supersession =
        config
            .raft_auto_promote
            .then(|| melin_raft::rpc_server::SupersessionPolicy {
                fence: Arc::clone(fence_state),
                shutdown: Arc::clone(shutdown),
                serving: serving_claim,
            });
    let handles = melin_raft::driver::spawn(
        raft_config,
        Arc::new(signing_key.clone()),
        Arc::clone(authorized_keys),
        tip,
        supersession,
        Arc::clone(shutdown),
    )?;
    Ok(Some(handles))
}

/// Stop the raft driver and join its thread. Sets the process `shutdown`
/// flag first (idempotent — it is normally already set) so the join is
/// bounded by the driver's 100 ms poll even on error-return paths that
/// never reached the regular shutdown sequence.
pub(crate) fn stop_raft_driver(handles: Option<RaftHandles>, shutdown: &Arc<AtomicBool>) {
    if let Some(h) = handles {
        shutdown.store(true, Ordering::Relaxed);
        // Best-effort: under panic=abort a join error can't occur; if the
        // thread is already gone the storage was left crash-safe anyway.
        let _ = h.join.join();
    }
}

/// A replica's minimal health endpoint: join handle + its private stop
/// flag (distinct from the process `shutdown` flag so promotion can tear
/// it down early — `run_as_primary` rebinds the same `--health-bind`).
pub(crate) type ReplicaHealth = (std::thread::JoinHandle<()>, Arc<AtomicBool>);

/// Spawn a minimal health endpoint for a replica so its control-plane
/// raft election gauges and liveness are observable — a replica
/// otherwise serves no `/metrics`, which hides election state on exactly
/// the nodes that survive a failover.
///
/// Only spawned when control-plane raft is enabled: without raft a
/// replica stays headless as before, so this doesn't perturb non-raft
/// deployments. `Ok(None)` when raft is off or no `--health-bind` is
/// configured.
pub(crate) fn spawn_replica_health(
    config: &ServerConfig,
    fence_state: &Arc<melin_transport_core::fence::FenceState>,
    raft_status: Option<&Arc<melin_transport_core::health::RaftStatus>>,
) -> Result<Option<ReplicaHealth>, Box<dyn std::error::Error>> {
    if raft_status.is_none() {
        return Ok(None);
    }
    let Some(addr) = config.health_bind else {
        return Ok(None);
    };
    let stop = Arc::new(AtomicBool::new(false));
    let handle = melin_transport_core::health::spawn(
        addr,
        melin_transport_core::health::HealthState::for_replica(
            Arc::clone(fence_state),
            raft_status.map(Arc::clone),
            Arc::new(AtomicBool::new(true)),
        ),
        Arc::clone(&stop),
    )?;
    info!(addr = %addr, "replica health endpoint started (election + liveness)");
    Ok(Some((handle, stop)))
}

/// Stop the replica health endpoint and join its thread so the listen
/// socket is released — called on the promotion path before
/// `run_as_primary` rebinds `--health-bind`, and on clean shutdown.
pub(crate) fn stop_replica_health(health: Option<ReplicaHealth>) {
    if let Some((handle, stop)) = health {
        stop.store(true, Ordering::Release);
        // Best-effort: a join error just means the thread already
        // unwound; the port is freed either way.
        let _ = handle.join();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;

    fn b64_key(seed: u8) -> String {
        let key = ed25519_dalek::SigningKey::from_bytes(&[seed; 32]);
        base64::engine::general_purpose::STANDARD.encode(key.verifying_key().to_bytes())
    }

    fn base_config() -> ServerConfig {
        ServerConfig::default()
    }

    #[test]
    fn no_raft_flags_is_none() {
        assert!(build_raft_config(&base_config()).unwrap().is_none());
    }

    #[test]
    fn partial_flags_are_refused() {
        let mut c = base_config();
        c.raft_node_id = Some(1);
        assert!(build_raft_config(&c).unwrap_err().contains("--raft-bind"));

        let mut c = base_config();
        c.raft_peer = vec![format!("1@127.0.0.1:7001#{}", b64_key(1))];
        assert!(build_raft_config(&c).unwrap_err().contains("--raft-bind"));
    }

    #[test]
    fn raft_bind_requires_node_id_and_key() {
        let mut c = base_config();
        c.raft_bind = Some("127.0.0.1:7001".parse().unwrap());
        assert!(
            build_raft_config(&c)
                .unwrap_err()
                .contains("--raft-node-id")
        );

        c.raft_node_id = Some(1);
        assert!(
            build_raft_config(&c)
                .unwrap_err()
                .contains("--replication-key")
        );
    }

    #[test]
    fn peer_list_must_include_self() {
        let mut c = base_config();
        c.raft_bind = Some("127.0.0.1:7001".parse().unwrap());
        c.raft_node_id = Some(1);
        c.replication_key = Some("key".into());
        c.raft_peer = vec![format!("2@127.0.0.1:7002#{}", b64_key(2))];
        assert!(
            build_raft_config(&c)
                .unwrap_err()
                .contains("must include this node")
        );
    }

    #[test]
    fn valid_config_parses_and_defaults_dir() {
        let mut c = base_config();
        c.journal = "/data/melin.journal".into();
        c.raft_bind = Some("0.0.0.0:7001".parse().unwrap());
        c.raft_node_id = Some(1);
        c.replication_key = Some("key".into());
        c.raft_peer = vec![
            format!("1@10.0.0.1:7001#{}", b64_key(1)),
            format!("2@10.0.0.2:7001#{}", b64_key(2)),
            format!("3@10.0.0.3:7001#{}", b64_key(3)),
        ];
        let rc = build_raft_config(&c).unwrap().unwrap();
        assert_eq!(rc.node_id, 1);
        assert_eq!(rc.peers.len(), 3);
        assert_eq!(rc.dir, std::path::PathBuf::from("/data/melin.raft"));
    }

    #[test]
    fn malformed_peer_entries_are_refused() {
        for bad in [
            "no-separators",
            "1@127.0.0.1:7001",      // missing pubkey
            "x@127.0.0.1:7001#AAAA", // bad id
            "1@not-an-addr#AAAA",    // bad addr
            "1@127.0.0.1:7001#!!",   // bad base64
        ] {
            assert!(parse_raft_peer(bad).is_err(), "should refuse: {bad}");
        }
        let ok = format!("1@127.0.0.1:7001#{}", b64_key(1));
        assert!(parse_raft_peer(&ok).is_ok());
    }
}
