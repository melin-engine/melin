//! Control-plane raft wiring: CLI validation, driver spawn, and the
//! replica-mode health endpoint.
//!
//! The consensus machinery lives in `melin-raft` (with its confined tokio
//! runtime); this module is the glue between `ServerConfig` and
//! `melin_raft::spawn`, shared by the kernel-TCP and DPDK paths.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use melin_app::auth::AuthorizedKeys;
use melin_raft::{RaftConfig, RaftHandles, RaftPeer};
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

/// Proof that this node's flags allow automatic promotion: `--raft-auto-promote`
/// is set, with at least three voters, and clock ticks that land inside the
/// failover grace (see [`AutoPromotion::check_tick_delay`]).
///
/// The private field means only this module makes one, through
/// [`build_raft_config`], and the auto-promotion path takes one by value
/// ([`crate::raft_promotion::spawn_auto_promotion`], and the supersession
/// policy [`spawn_raft_driver`] arms), so a node that skipped or failed
/// the checks has no way to arm automation. `Copy`: a zero-sized proof,
/// handed from the validated setup to the driver guard and on to the
/// promotion thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AutoPromotion {
    _checked: (),
}

impl AutoPromotion {
    /// Check `--raft-auto-promote`'s gates against `config`, for a cluster
    /// of `voters`.
    fn check(config: &ServerConfig, voters: usize) -> Result<Self, String> {
        if voters < 3 {
            return Err(format!(
                "--raft-auto-promote requires at least 3 configured voters, got {voters} — a \
                 two-node cluster cannot elect a leader after losing either node, so automatic \
                 failover would never fire while inviting misconfiguration; run manual PROMOTE \
                 instead"
            ));
        }
        Self::check_tick_delay(config)?;
        Ok(Self { _checked: () })
    }

    /// A primary's journaled ticks are what keep a one-sided partition (a
    /// replica raft leader cut from the primary alone, the other replica
    /// still reaching both) from deposing it: they move the other
    /// replica's tip ahead of the leader's, and the peer-tip veto refuses.
    /// An idle primary with no tick inside the leader's primary-down grace
    /// leaves the tips equal, and the leader promotes: ticks disabled, or
    /// a cadence too slow (the bound's derivation is beside
    /// `raft_promotion::MAX_AUTO_PROMOTE_TICK_DELAY`).
    ///
    /// The group-commit window counts against the same bound: the journal
    /// stage hands a batch to the replication sender only once the window
    /// has passed since its first entry, so an idle primary's tick waits
    /// it out before it leaves for the replicas.
    ///
    /// Refused on every node that sets `--raft-auto-promote`, not only on
    /// a primary: any node may become the primary by promotion, and a
    /// replica can judge only its own flags, not the primary's. The
    /// dangerous cluster spans nodes (the primary ticking too slowly, the
    /// replicas auto-promoting), which no single node can see; this
    /// refusal closes it for a uniformly configured cluster, and the docs
    /// state the rule for the rest: every node of a raft cluster ticks
    /// within the bound.
    fn check_tick_delay(config: &ServerConfig) -> Result<(), String> {
        let max = ServerConfig::MAX_AUTO_PROMOTE_TICK_DELAY;
        let problem = match config.tick_interval() {
            None => "ticks are disabled, so an idle primary journals nothing".to_owned(),
            Some(tick) => {
                // `saturating_add`: two `u64` flags in ms and µs cannot
                // overflow a `Duration` (u64 seconds), and a saturated sum
                // would be above the bound all the same.
                let delay = tick.saturating_add(config.group_commit_delay());
                if delay <= max {
                    return Ok(());
                }
                format!(
                    "a tick may reach replication up to {} after the last (the cadence plus \
                     the group-commit window, which a tick waits out before it replicates)",
                    format_ms(delay)
                )
            }
        };
        Err(format!(
            "--raft-auto-promote cannot be combined with --tick-interval-ms {} and \
             --group-commit-us {}: {problem}. An idle primary that journals no tick within the \
             {} s failover grace can be deposed by a replica raft leader cut off from it alone; \
             set --tick-interval-ms to at least 1 and keep it plus the --group-commit-us window \
             within {} (half the grace), or drop --raft-auto-promote and promote manually",
            config.tick_interval_ms,
            config.group_commit_us,
            crate::raft_promotion::PRIMARY_DOWN_GRACE.as_secs(),
            format_ms(max),
        ))
    }
}

/// `d` in milliseconds, with microseconds when it has any (`1500 ms`,
/// `1500.001 ms`): the units the two flags of the tick bound take.
fn format_ms(d: std::time::Duration) -> String {
    let us = d.as_micros();
    match us % 1000 {
        0 => format!("{} ms", us / 1000),
        frac => format!("{}.{frac:03} ms", us / 1000),
    }
}

/// The raft flags, validated: the driver's configuration and, when
/// `--raft-auto-promote` cleared its checks, the proof that lets the node
/// arm automation.
#[derive(Debug)]
pub(crate) struct RaftSetup {
    pub(crate) config: RaftConfig,
    pub(crate) auto_promotion: Option<AutoPromotion>,
}

/// Validate the raft CLI flags into a [`RaftSetup`].
///
/// `Ok(None)` when raft is not configured. Partial flag sets are refused —
/// a node with `--raft-node-id` but no `--raft-bind` is a misconfiguration
/// the operator must resolve, not a mode. Writes nothing, so a node calls
/// it with its other configuration checks, before it touches the disk.
pub(crate) fn build_raft_config(config: &ServerConfig) -> Result<Option<RaftSetup>, String> {
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
    let auto_promotion = config
        .raft_auto_promote
        .then(|| AutoPromotion::check(config, peers.len()))
        .transpose()?;
    let dir = config
        .raft_dir
        .clone()
        .unwrap_or_else(|| config.journal.with_extension("raft"));
    Ok(Some(RaftSetup {
        config: RaftConfig {
            node_id,
            bind,
            dir,
            peers,
        },
        auto_promotion,
    }))
}

/// Owns the raft driver — and, when armed, the auto-promotion thread —
/// and guarantees teardown on **every** exit path: dropping the guard
/// sets the process `shutdown` flag and joins the threads. Without it, an
/// early `?` return between the driver spawn and the regular shutdown
/// sequence would leak the driver: a zombie voter that keeps its
/// `--raft-bind` port bound and keeps granting votes after the server
/// function returned (`run_with_listener` is a library entry point, so
/// the process does not necessarily exit). Under panic=abort there is no
/// unwinding, so `Drop` runs exactly on the scope-exit paths this
/// protects.
pub(crate) struct RaftDriverGuard {
    /// `None` when raft is not configured — the guard is then inert.
    handles: Option<RaftHandles>,
    /// Configured voter ids excluding this node — what the promotion
    /// policy's journal-safety veto must account for. Empty when raft
    /// is off.
    peer_ids: Vec<u64>,
    /// The proof that `--raft-auto-promote` cleared its checks, from the
    /// [`RaftSetup`] the driver was spawned from; `None` without the flag
    /// and when raft is off. What [`Self::auto_promotion`] hands the
    /// promotion thread.
    auto_promotion: Option<AutoPromotion>,
    /// Auto-promotion poll thread (replica paths under
    /// `--raft-auto-promote`), adopted via [`Self::arm_promotion`]. Exits
    /// within one 100 ms poll of the shutdown flag, or the moment a
    /// promotion is filed.
    promotion: Option<std::thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

/// Everything [`crate::raft_promotion::spawn_auto_promotion`] needs from
/// the driver, and the leadership confirmation the promotion it files
/// asks at mint time; returned by [`RaftDriverGuard::promotion_wiring`].
pub(crate) struct PromotionWiring {
    pub(crate) status: Arc<melin_transport_core::health::RaftStatus>,
    pub(crate) peer_tips: Arc<melin_raft::PeerTips>,
    pub(crate) peer_ids: Vec<u64>,
    pub(crate) elect_requested: Arc<AtomicBool>,
    pub(crate) elect_enabled: Arc<AtomicBool>,
    /// Not the promotion thread's: the confirmation an election's epoch
    /// bump asks before it is published (`PromotedTenure`). Carried here
    /// so the node holds it exactly when the thread that files elections
    /// is armed.
    pub(crate) leader_confirm: melin_raft::LeaderConfirm,
}

impl RaftDriverGuard {
    /// Guard for a node without a control plane (raft off). Dropping it
    /// does nothing.
    pub(crate) fn disabled(shutdown: &Arc<AtomicBool>) -> Self {
        Self {
            handles: None,
            peer_ids: Vec::new(),
            auto_promotion: None,
            promotion: None,
            shutdown: Arc::clone(shutdown),
        }
    }

    /// The proof [`crate::raft_promotion::spawn_auto_promotion`] takes:
    /// `Some` only on a node whose driver runs and whose
    /// `--raft-auto-promote` cleared its checks.
    pub(crate) fn auto_promotion(&self) -> Option<AutoPromotion> {
        self.auto_promotion
    }

    /// Election state for the health gauges; `None` when raft is off.
    pub(crate) fn status(&self) -> Option<Arc<melin_transport_core::health::RaftStatus>> {
        self.handles.as_ref().map(|h| Arc::clone(&h.status))
    }

    /// The promotion thread's inputs; `None` when raft is off.
    pub(crate) fn promotion_wiring(&self) -> Option<PromotionWiring> {
        self.handles.as_ref().map(|h| PromotionWiring {
            status: Arc::clone(&h.status),
            peer_tips: Arc::clone(&h.peer_tips),
            peer_ids: self.peer_ids.clone(),
            elect_requested: Arc::clone(&h.elect_requested),
            elect_enabled: Arc::clone(&h.elect_enabled),
            leader_confirm: h.leader_confirm.clone(),
        })
    }

    /// Adopt the auto-promotion thread so it is joined on every exit
    /// path alongside the driver. At most once — overwriting would drop
    /// the previous handle without joining it.
    pub(crate) fn arm_promotion(&mut self, handle: std::thread::JoinHandle<()>) {
        debug_assert!(
            self.promotion.is_none(),
            "arm_promotion called twice — the first thread would leak"
        );
        self.promotion = Some(handle);
    }
}

impl Drop for RaftDriverGuard {
    fn drop(&mut self) {
        if self.handles.is_none() && self.promotion.is_none() {
            return;
        }
        // Idempotent — normally already set by the shutdown sequence; on
        // error-return paths this is what bounds the joins below to the
        // threads' 100 ms polls.
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.handles.take() {
            // Best-effort: under panic=abort a join error can't occur; if
            // the thread is already gone the storage was left crash-safe.
            let _ = h.join.join();
        }
        if let Some(h) = self.promotion.take() {
            // Exits within one poll of the (now set) shutdown flag, or
            // already exited the moment a promotion was filed.
            let _ = h.join();
        }
    }
}

/// Spawn the raft driver from an already-validated [`RaftSetup`] — the
/// caller matches [`build_raft_config`]'s `None` (raft off) to a
/// [`RaftDriverGuard::disabled`] guard, so the peer list is parsed
/// exactly once per boot. The setup's [`AutoPromotion`], if any, arms the
/// raft mesh as a fencing channel (see `SupersessionPolicy` below) and
/// stays with the guard for the promotion thread.
///
/// Called on every mode path (primary, replica, DPDK variants) right after
/// the fence state exists — the driver shares the process `shutdown` flag
/// and survives a replica → primary promotion untouched: the guard is
/// dropped (stopping the driver) only after `run_as_primary` (or its DPDK
/// twin) returns.
#[allow(clippy::too_many_arguments)] // driver assembly point, same as melin_raft::spawn
pub(crate) fn spawn_raft_driver(
    setup: RaftSetup,
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
) -> Result<RaftDriverGuard, Box<dyn std::error::Error>> {
    let RaftSetup {
        config: raft_config,
        auto_promotion,
    } = setup;
    info!(
        node_id = raft_config.node_id,
        bind = %raft_config.bind,
        voters = raft_config.peers.len(),
        dir = %raft_config.dir.display(),
        "starting control-plane raft driver"
    );
    let tip = Arc::new(melin_raft::TipSource {
        fence: Arc::clone(fence_state),
        seq: journal_tip,
        ready: tip_ready,
    });
    // The raft mesh doubles as a fencing channel only under
    // auto-promotion: without automation, a serving node's fencing
    // stays a data-plane-contact concern exactly as documented.
    let supersession = auto_promotion.map(|_| melin_raft::SupersessionPolicy {
        fence: Arc::clone(fence_state),
        shutdown: Arc::clone(shutdown),
        serving: serving_claim,
    });
    // The voter set the promotion veto must account for: everyone but us.
    let peer_ids: Vec<u64> = raft_config
        .peers
        .iter()
        .map(|p| p.id)
        .filter(|&id| id != raft_config.node_id)
        .collect();
    let handles = melin_raft::spawn(
        raft_config,
        Arc::new(signing_key.clone()),
        Arc::clone(authorized_keys),
        tip,
        supersession,
        Arc::clone(shutdown),
    )?;
    Ok(RaftDriverGuard {
        handles: Some(handles),
        peer_ids,
        auto_promotion,
        promotion: None,
        shutdown: Arc::clone(shutdown),
    })
}

/// Owns a replica's minimal health endpoint: join handle + its private
/// stop flag (distinct from the process `shutdown` flag so promotion can
/// tear it down early — `run_as_primary` (or its DPDK twin) rebinds the same
/// `--health-bind`). [`Self::stop`] is explicit for exactly that
/// pre-rebind moment; `Drop` covers every other exit path (clean
/// shutdown, early `?` errors) so the listener thread and its port can
/// never outlive the server function.
pub(crate) struct ReplicaHealthGuard {
    inner: Option<(std::thread::JoinHandle<()>, Arc<AtomicBool>)>,
}

impl ReplicaHealthGuard {
    /// Stop the endpoint and join its thread so the listen socket is
    /// released. Idempotent.
    pub(crate) fn stop(&mut self) {
        if let Some((handle, stop)) = self.inner.take() {
            stop.store(true, Ordering::Release);
            // Best-effort: a join error just means the thread already
            // unwound; the port is freed either way.
            let _ = handle.join();
        }
    }
}

impl Drop for ReplicaHealthGuard {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Spawn a minimal health endpoint for a replica so its liveness, its
/// `/stats-dump` under `latency-trace`, and — when control-plane raft is
/// enabled — its election gauges are observable. A replica otherwise
/// serves no `/metrics`, which hides election state on exactly the nodes
/// that survive a failover, and hides the replica's half of the
/// replication round trip on exactly the node that measures it.
///
/// Spawned whenever `--health-bind` is configured, raft or not; the
/// election gauges are simply absent without raft. The returned guard is
/// inert when no bind is configured.
pub(crate) fn spawn_replica_health(
    config: &ServerConfig,
    fence_state: &Arc<melin_transport_core::fence::FenceState>,
    raft_status: Option<&Arc<melin_transport_core::health::RaftStatus>>,
    // The replica's live pipeline-health mirror
    // (`ReplicaControlPlane::pipeline_healthy`) — drives the endpoint's
    // OK/ERR status and the `melin_pipeline_healthy` gauge.
    pipeline_healthy: Arc<AtomicBool>,
) -> Result<ReplicaHealthGuard, Box<dyn std::error::Error>> {
    let Some(addr) = config.health_bind else {
        return Ok(ReplicaHealthGuard { inner: None });
    };
    let stop = Arc::new(AtomicBool::new(false));
    let handle = melin_transport_core::health::spawn(
        addr,
        melin_transport_core::health::HealthState::for_replica(
            Arc::clone(fence_state),
            raft_status.map(Arc::clone),
            pipeline_healthy,
        ),
        Arc::clone(&stop),
    )?;
    info!(
        addr = %addr,
        election = raft_status.is_some(),
        "replica health endpoint started"
    );
    Ok(ReplicaHealthGuard {
        inner: Some((handle, stop)),
    })
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

    /// A raft config that clears every other gate: three voters, this node
    /// among them, a replication key.
    fn three_voter_config() -> ServerConfig {
        let mut c = base_config();
        c.raft_bind = Some("127.0.0.1:7001".parse().unwrap());
        c.raft_node_id = Some(1);
        c.replication_key = Some("key".into());
        c.raft_peer = (1..=3)
            .map(|i| format!("{i}@10.0.0.{i}:7001#{}", b64_key(i)))
            .collect();
        c
    }

    /// What `build_raft_config` makes of `c`: `Ok(armed)`, whether the
    /// setup carries the auto-promotion proof, or the refusal.
    fn armed(c: &ServerConfig) -> Result<bool, String> {
        build_raft_config(c)
            .map(|setup| setup.expect("raft is configured").auto_promotion.is_some())
    }

    #[test]
    fn auto_promote_requires_three_voters() {
        let mut c = three_voter_config();
        c.raft_auto_promote = true;
        let third = c.raft_peer.pop().expect("three peers");
        // A two-node cluster cannot elect after losing either node, so
        // automation would never fire when needed — refused at startup.
        let err = build_raft_config(&c).unwrap_err();
        assert!(err.contains("at least 3"), "{err}");

        // Three voters clears the gate with the same flags, and arms
        // automation.
        c.raft_peer.push(third);
        assert_eq!(armed(&c), Ok(true));

        // Without the flag the same cluster runs, unarmed.
        c.raft_auto_promote = false;
        assert_eq!(armed(&c), Ok(false));

        // The flag alone (no --raft-bind) is a partial flag set, refused
        // like the others rather than silently ignored.
        let mut c = base_config();
        c.raft_auto_promote = true;
        assert!(build_raft_config(&c).unwrap_err().contains("--raft-bind"));
    }

    /// The tick-delay refusal: both edges of the accepted range (a cadence
    /// of 1 ms, a cadence plus window of exactly the bound), just outside
    /// them (ticks disabled, one microsecond over), huge flag values whose
    /// sum must neither overflow nor wrap below the bound, and a message
    /// naming both flags, the computed delay, the grace and the bound.
    #[test]
    fn auto_promote_refuses_ticks_outside_the_failover_grace() {
        let max = ServerConfig::MAX_AUTO_PROMOTE_TICK_DELAY;
        let max_ms = u64::try_from(max.as_millis()).expect("the bound fits in u64 milliseconds");
        assert_eq!(
            max.subsec_micros() % 1000,
            0,
            "the bound is whole milliseconds, as the cases below assume"
        );
        let grace = format!(
            "{} s failover grace",
            crate::raft_promotion::PRIMARY_DOWN_GRACE.as_secs()
        );
        let bound = format!("within {max_ms} ms (half the grace)");
        let default_ms = ServerConfig::default().tick_interval_ms;
        assert!(
            default_ms <= max_ms,
            "the default cadence is within the bound"
        );
        let us_per_ms = 1_000;
        let mut c = three_voter_config();
        c.raft_auto_promote = true;

        // Accepted, and armed: from a 1 ms cadence to a cadence plus
        // window of exactly the bound, however it is split.
        for (tick_ms, group_commit_us) in [
            (1, 0),
            (default_ms, 0),
            (max_ms, 0),
            (max_ms - 1, us_per_ms),
            (1, (max_ms - 1) * us_per_ms),
        ] {
            c.tick_interval_ms = tick_ms;
            c.group_commit_us = group_commit_us;
            assert_eq!(armed(&c), Ok(true), "{tick_ms} ms + {group_commit_us} us");
        }

        // Refused: the cadence plus window above the bound, by a single
        // microsecond or by a sum that would overflow a careless `u64`
        // addition, the message naming the delay it computed.
        for (tick_ms, group_commit_us) in [
            (max_ms, 1),
            (max_ms - 1, us_per_ms + 1),
            (max_ms + 1, 0),
            (default_ms, u64::MAX),
            (u64::MAX, 0),
            (u64::MAX, u64::MAX),
        ] {
            c.tick_interval_ms = tick_ms;
            c.group_commit_us = group_commit_us;
            let err = armed(&c).unwrap_err();
            // Exact: a `Duration` (u64 seconds) holds the sum of two u64
            // flags in milliseconds and microseconds without overflow.
            let delay = format_ms(
                std::time::Duration::from_millis(tick_ms)
                    + std::time::Duration::from_micros(group_commit_us),
            );
            for expected in [
                "--raft-auto-promote",
                &format!("--tick-interval-ms {tick_ms} and --group-commit-us {group_commit_us}:"),
                &format!("up to {delay} after the last"),
                &grace,
                &bound,
            ] {
                assert!(err.contains(expected), "{expected:?} missing from: {err}");
            }
        }

        // Refused: ticks disabled, whatever the window, in a message that
        // states the whole rule, so one restart finds it.
        for group_commit_us in [0, 1, u64::MAX] {
            c.tick_interval_ms = 0;
            c.group_commit_us = group_commit_us;
            let err = armed(&c).unwrap_err();
            for expected in [
                &format!("--tick-interval-ms 0 and --group-commit-us {group_commit_us}:"),
                "ticks are disabled",
                "--tick-interval-ms to at least 1",
                &grace,
                &bound,
            ] {
                assert!(err.contains(expected), "{expected:?} missing from: {err}");
            }
        }

        // Without auto-promotion (manual promotion only), any cadence,
        // ticks disabled included, and any window: accepted, unarmed.
        c.raft_auto_promote = false;
        for (tick_ms, group_commit_us) in [(0, 0), (max_ms + 1, 0), (1, u64::MAX)] {
            c.tick_interval_ms = tick_ms;
            c.group_commit_us = group_commit_us;
            assert_eq!(armed(&c), Ok(false), "{tick_ms} ms + {group_commit_us} us");
        }

        // Ticks disabled on a node without raft: no control plane, nothing
        // to refuse.
        let mut c = base_config();
        c.tick_interval_ms = 0;
        assert!(build_raft_config(&c).unwrap().is_none());
    }

    #[test]
    fn delays_are_stated_in_milliseconds_with_microseconds_when_any() {
        use std::time::Duration;
        assert_eq!(format_ms(Duration::ZERO), "0 ms");
        assert_eq!(format_ms(Duration::from_millis(1500)), "1500 ms");
        assert_eq!(format_ms(Duration::from_micros(1_500_001)), "1500.001 ms");
        assert_eq!(format_ms(Duration::from_micros(1_500_120)), "1500.120 ms");
        // Below a microsecond is below either flag's unit: dropped.
        assert_eq!(format_ms(Duration::from_nanos(1_000_999)), "1 ms");
    }

    /// The `--help` of the three flags states the bound in figures (a doc
    /// comment cannot interpolate a constant), so this pins the figures to
    /// the constants: a change to either fails here rather than leaving
    /// the help stating a bound the node no longer enforces.
    #[test]
    fn the_tick_bound_in_the_help_matches_the_constants() {
        use clap::CommandFactory as _;
        let max_ms = ServerConfig::MAX_AUTO_PROMOTE_TICK_DELAY.as_millis();
        let grace_s = crate::raft_promotion::PRIMARY_DOWN_GRACE.as_secs();
        let command = ServerConfig::command();
        for (flag, expected) in [
            (
                "raft_auto_promote",
                format!("1 and {max_ms} ms (half the {grace_s} s failover grace)"),
            ),
            ("raft_auto_promote", format!("window at most {max_ms} ms")),
            (
                "tick_interval_ms",
                format!("above {max_ms} ms (half the {grace_s} s failover grace)"),
            ),
            (
                "group_commit_us",
                format!("plus --tick-interval-ms within {max_ms} ms"),
            ),
        ] {
            let arg = command
                .get_arguments()
                .find(|a| a.get_id() == flag)
                .expect("the flag exists");
            let help = arg
                .get_long_help()
                .or(arg.get_help())
                .expect("the flag has help")
                .to_string();
            // Doc comments wrap where they like; clap keeps the breaks.
            let help = help.split_whitespace().collect::<Vec<_>>().join(" ");
            assert!(
                help.contains(&expected),
                "{flag}: {expected:?} missing from: {help}"
            );
        }
    }

    #[test]
    fn valid_config_parses_and_defaults_dir() {
        let mut c = three_voter_config();
        c.journal = "/data/melin.journal".into();
        let setup = build_raft_config(&c).unwrap().unwrap();
        assert_eq!(setup.config.node_id, 1);
        assert_eq!(setup.config.peers.len(), 3);
        assert_eq!(
            setup.config.dir,
            std::path::PathBuf::from("/data/melin.raft")
        );
        assert_eq!(setup.auto_promotion, None);
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
