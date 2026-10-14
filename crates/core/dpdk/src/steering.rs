//! How a NIC group's ports share their traffic out across queues: how many
//! queue pairs, whether the default RSS may spread flows across them, and
//! the `rte_flow` rules that steer flows to a queue.
//!
//! Decided from the configuration alone, so ungated and tested on every
//! host: the gated port code installs exactly what a [`SteeringPlan`] says
//! and decides nothing itself.
//!
//! # Priorities
//!
//! `rte_flow` ranks the rules of one group by their priority level, the
//! lowest number first (0 is the highest priority). When several rules at
//! one level match a packet, which one acts is undefined. Every rule here
//! is in group 0, the root table.
//!
//! mlx5 offers few levels on its root table. It folds each level it
//! offers into several of the hardware's, one per pattern layer, so that
//! a rule matching the TCP header outranks one matching only IPv4 at the
//! same level; how many levels remain depends on the device, its firmware
//! and the flow engine, and can be as few as two. A plan therefore numbers
//! its levels densely from 0 and uses only as many as its rules need: one
//! in shared mode (as before), two in queue mode, three only on an
//! isolated (bifurcated) replica, the one case where three rankings are
//! all needed (see [`SteeringPlan::for_client_nic`]). A level the device
//! does not offer fails the rule's creation at boot, naming the rule. A
//! replica's ARP rule ([`SteeringPlan::arp_until_promotion`]) takes no
//! level of its own: it matches ARP frames, which no IPv4 rule matches, so
//! it shares the first level without ambiguity.
//!
//! If a device refuses that third level: isolated mode is mlx5-only, and
//! mlx5's per-layer folding already ranks the TCP-matching replication
//! rules above the IPv4-only peer rule at one level, so the peer rule
//! could share the replication rules' level there. That leans on a
//! PMD-specific sub-priority rather than on `rte_flow`'s contract, which
//! is why the plan does not do it unless the hardware forces it.
//!
//! # ARP on a queue-mode replica
//!
//! A replica in queue mode leaves its client queue unpolled until it is
//! promoted, and a port that is not isolated sends every packet no rule
//! takes there, ARP included. Its replication queue's transport has the
//! node's address and MAC, so the plan sends ARP there instead, and the
//! replica answers for its address before promotion: to a kernel-TCP
//! primary, which resolves it by ARP, and to a router. The same rule brings
//! back the replies to that transport's own requests (a neighbour evicted
//! or expired), which would otherwise wait on the unpolled queue.
//!
//! The rule goes at promotion, before the client queue is polled as a
//! primary's, so that a promoted node steers ARP as a primary booted in
//! queue mode does: to the client queue. Left in place it would take the
//! replies to the client transport's own requests (a client evicted from
//! its neighbour cache, which a primary with more on-link peers than the
//! cache holds meets in steady state) to the replication queue, where they
//! answer nothing, and that client would stall for the stack's discovery
//! timeout each time. Matching requests alone would avoid that, but
//! `rte_flow` matches the ARP opcode only through its `ARP_ETH_IPV4` item,
//! which mlx5 does not support, and matching the broadcast destination
//! instead would miss the unicast requests Linux sends to confirm a stale
//! neighbour entry. An isolated port leaves ARP to the kernel, which owns
//! the address there: no rule.

use std::net::Ipv4Addr;

use crate::config::{DpdkConfig, NicConfig, ReplicationNic, ReplicationPorts};

/// The queue client traffic, and everything no rule steers elsewhere,
/// arrives on.
pub const CLIENT_QUEUE: u16 = 0;

/// The queue replication traffic is steered to in
/// [`ReplicationNic::Queue`] mode.
pub const REPLICATION_QUEUE: u16 = 1;

/// The queue pairs a NIC group's ports are configured with, and what the
/// default RSS does across them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueuePlan {
    /// This many queue pairs, as many as the NIC offers at most, with RSS
    /// hashing TCP/IP flows across them when there is more than one: one
    /// poll thread per queue. One queue pair is the node's as it has
    /// always run.
    Rss(u16),
    /// Two queue pairs, [`CLIENT_QUEUE`] and [`REPLICATION_QUEUE`]: the
    /// client queue takes every packet no rule sends to the replication
    /// queue. RSS hashes no header field, and its redirection table points
    /// every entry at the client queue (when the port is not isolated;
    /// isolated, nothing reaches DPDK but what the rules steer), so
    /// whether a rule or the default RSS takes precedence never matters.
    /// The NIC must offer both pairs.
    ReplicationSplit,
}

impl QueuePlan {
    /// The queue pairs a port is configured with under this plan, given
    /// the most RX and TX queues it offers: [`QueuePlan::Rss`]'s count,
    /// clamped to what the port offers (the caller says so when it
    /// clamps), or [`QueuePlan::ReplicationSplit`]'s two, refused when
    /// the port offers fewer. A port reporting no queues counts as one.
    pub fn queue_pairs_on(self, max_rx: u16, max_tx: u16) -> Result<u16, TooFewQueues> {
        let available = max_rx.min(max_tx).max(1);
        match self {
            QueuePlan::Rss(requested) => Ok(requested.min(available)),
            QueuePlan::ReplicationSplit if available < 2 => Err(TooFewQueues {
                required: 2,
                available,
            }),
            QueuePlan::ReplicationSplit => Ok(2),
        }
    }

    /// The queue pairs this plan asks for.
    pub fn requested_queue_pairs(self) -> u16 {
        match self {
            QueuePlan::Rss(requested) => requested,
            QueuePlan::ReplicationSplit => 2,
        }
    }

    /// How many of `configured` queue pairs (what a port configured under
    /// this plan has) carry clients, and so how many client poll threads
    /// the NIC needs, on queues `0..` that count: every one of
    /// [`QueuePlan::Rss`]'s, and only [`CLIENT_QUEUE`] of
    /// [`QueuePlan::ReplicationSplit`]'s, whose other queue is
    /// replication's and has a poll thread of its own.
    pub fn client_queues(self, configured: u16) -> u16 {
        match self {
            QueuePlan::Rss(_) => configured,
            QueuePlan::ReplicationSplit => 1,
        }
    }
}

/// A port offers fewer queue pairs than a [`QueuePlan`] cannot do without.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TooFewQueues {
    /// The queue pairs the plan needs.
    pub required: u16,
    /// The queue pairs the port offers.
    pub available: u16,
}

impl std::fmt::Display for TooFewQueues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} queue pairs are needed, and the NIC offers {}",
            self.required, self.available
        )
    }
}

impl std::error::Error for TooFewQueues {}

/// One `rte_flow` rule: ingress packets matching every field given go to
/// [`queue`](Self::queue). A field not given matches anything; a TCP port
/// given matches TCP over IPv4 only.
///
/// Flat optional fields rather than an enum of rule shapes: they are what
/// the pattern is built from, one pattern item per header, so the code that
/// installs a rule has nothing to decide.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SteeringRule {
    /// The rule's level in group 0: lower is higher priority. `u32`, as
    /// `rte_flow_attr::priority` is.
    pub priority: u32,
    /// The IPv4 source address.
    pub src_ipv4: Option<Ipv4Addr>,
    /// The IPv4 destination address.
    pub dst_ipv4: Option<Ipv4Addr>,
    /// The TCP source port.
    pub tcp_src_port: Option<u16>,
    /// The TCP destination port.
    pub tcp_dst_port: Option<u16>,
    /// The RX queue matching packets go to.
    pub queue: u16,
}

impl SteeringRule {
    /// A rule with no field to match yet, to `queue`, at the highest
    /// priority until its plan ranks it.
    fn to_queue(queue: u16) -> Self {
        SteeringRule {
            priority: 0,
            src_ipv4: None,
            dst_ipv4: None,
            tcp_src_port: None,
            tcp_dst_port: None,
            queue,
        }
    }
}

/// As the rule is logged at boot and named in a failure to install it:
/// `ipv4 dst 10.0.0.1, tcp dst port 9877 -> queue 1 (priority 1)`.
impl std::fmt::Display for SteeringRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fields = [
            self.src_ipv4.map(|ip| format!("ipv4 src {ip}")),
            self.dst_ipv4.map(|ip| format!("ipv4 dst {ip}")),
            self.tcp_src_port.map(|port| format!("tcp src port {port}")),
            self.tcp_dst_port.map(|port| format!("tcp dst port {port}")),
        ];
        let mut fields = fields.into_iter().flatten().peekable();
        if fields.peek().is_none() {
            f.write_str("any ipv4")?;
        }
        for (i, field) in fields.enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            f.write_str(&field)?;
        }
        write!(f, " -> queue {} (priority {})", self.queue, self.priority)
    }
}

/// One `rte_flow` rule matching every frame of one EtherType, whatever it
/// carries, to [`queue`](Self::queue): a replica's ARP
/// ([`SteeringPlan::arp_until_promotion`]).
///
/// A type of its own rather than an EtherType field on [`SteeringRule`],
/// whose IPv4 and TCP fields would then be a combination the NIC has to
/// refuse: each type can only describe a pattern the shim builds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EtherTypeRule {
    /// The rule's level in group 0, as [`SteeringRule::priority`].
    pub priority: u32,
    /// The EtherType matched, in host order. `u16`, as the Ethernet
    /// header's field is.
    pub ether_type: u16,
    /// The RX queue matching frames go to.
    pub queue: u16,
}

/// As the rule is logged at boot and named in a failure to install or
/// remove it: `ether type 0x0806 -> queue 1 (priority 0)`.
impl std::fmt::Display for EtherTypeRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ether type {:#06x} -> queue {} (priority {})",
            self.ether_type, self.queue, self.priority
        )
    }
}

/// How a NIC group's ports split their traffic: the queue pairs, whether
/// the port is isolated, whether RSS is pinned to the client queue, the
/// rules, in the order they are installed, and the ARP rule a replica holds
/// until it is promoted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SteeringPlan {
    /// The queue pairs each port is configured with.
    pub queues: QueuePlan,
    /// Whether each port runs in `rte_flow` isolated mode (bifurcated,
    /// `NicConfig::peer_ip` given): the kernel keeps every packet no rule
    /// steers into DPDK.
    pub isolated: bool,
    /// Whether every entry of each port's RSS redirection table is pointed
    /// at [`CLIENT_QUEUE`] once it has started: with the replication split
    /// on a port that is not isolated, where the default RSS takes what no
    /// rule does.
    pub pin_rss_to_client_queue: bool,
    /// The rules, highest priority first, which is the order they are
    /// installed in: a packet never meets a lower rule without the higher
    /// ones in place. A `Vec`: ordered, built once at boot, and at most
    /// four.
    pub rules: Vec<SteeringRule>,
    /// On a replica in queue mode whose port is not isolated: every ARP
    /// frame to [`REPLICATION_QUEUE`], whose transport answers for the
    /// node's address while the client queue goes unpolled, at the first
    /// level (which the replica's client-port rule always takes; ARP
    /// matches none of the IPv4 rules). Installed after [`Self::rules`]
    /// and removed when the node is promoted, after which ARP goes where
    /// it goes on a primary booted in queue mode: the client queue. See
    /// the module's documentation for why it must go. `None` everywhere
    /// else.
    pub arp_until_promotion: Option<EtherTypeRule>,
}

impl SteeringPlan {
    /// The plan for a NIC as it has always been brought up, with
    /// `queue_pairs` queue pairs hashed by RSS: in isolated (bifurcated)
    /// mode, one rule capturing every IPv4 packet from the peer
    /// (`NicConfig::peer_ip`) into [`CLIENT_QUEUE`]; otherwise none. A
    /// replication NIC of its own ([`ReplicationNic::Port`]) takes it with
    /// one queue pair.
    pub fn for_nic(nic: &NicConfig, queue_pairs: u16) -> Self {
        SteeringPlan {
            queues: QueuePlan::Rss(queue_pairs),
            isolated: nic.peer_ip.is_some(),
            pin_rss_to_client_queue: false,
            rules: nic.peer_ip.map(peer_capture).into_iter().collect(),
            arp_until_promotion: None,
        }
    }

    /// The plan for a node's client NIC: [`Self::for_nic`] with the node's
    /// queue count in shared and port modes, and in
    /// [`ReplicationNic::Queue`] mode the replication split, with these
    /// rules, highest priority first:
    ///
    /// 1. On a replica: TCP to the client port → [`CLIENT_QUEUE`] (in
    ///    isolated mode, from the peer only, so it captures nothing the
    ///    peer rule does not). It ranks above the rule on the primary's
    ///    port, so a client whose ephemeral port happens to be that port
    ///    still reaches the client queue. The reverse collision, the
    ///    replica dialing its primary from the client port, the replica
    ///    avoids: it dials from no port a rule matches as a destination
    ///    ([`Self::destination_ports`]).
    /// 2. TCP to the node's own replication port → [`REPLICATION_QUEUE`]:
    ///    its replicas' links, as a primary or once promoted; and, on a
    ///    replica, TCP from its primary's replication port →
    ///    [`REPLICATION_QUEUE`]: its link to its primary. Both, when both
    ///    ports are given: the two ports can differ. The two rules share a
    ///    level, which is safe, as both send to the same queue. In
    ///    isolated mode both also match the node's own address, so that
    ///    they capture replication from every replica host, whatever the
    ///    peer address, and nothing addressed elsewhere.
    /// 3. In isolated mode, the peer rule of [`Self::for_nic`], below the
    ///    replication rules, so that replication from the peer's host
    ///    still reaches the replication queue.
    ///
    /// Levels are numbered densely from 0 over the ranks present (see the
    /// module's documentation).
    ///
    /// And on a replica whose port is not isolated, ARP to
    /// [`REPLICATION_QUEUE`] until promotion
    /// ([`Self::arp_until_promotion`]), at the client-port rule's level.
    ///
    /// Refused: queue mode with more than one client queue (the second
    /// queue is replication's), with neither replication port (nothing to
    /// steer), or with the node's replication port equal to its client
    /// port (the rules could not tell the two apart).
    pub fn for_client_nic(config: &DpdkConfig) -> Result<Self, SteeringError> {
        match &config.replication {
            ReplicationNic::Shared | ReplicationNic::Port(_) => {
                Ok(Self::for_nic(&config.nic, config.num_queues))
            }
            ReplicationNic::Queue(ports) => {
                Self::replication_split(&config.nic, config.listen_port, config.num_queues, *ports)
            }
        }
    }

    /// The plan of the NIC a node's replication runs on: in
    /// [`ReplicationNic::Port`] mode the replication NIC's, brought up
    /// with one queue pair ([`Self::for_nic`]); otherwise the client NIC's
    /// ([`Self::for_client_nic`]), whose queue 0, or queue 1 in
    /// [`ReplicationNic::Queue`] mode, carries replication.
    ///
    /// Refused as [`Self::for_client_nic`] refuses the client NIC's.
    pub fn for_replication(config: &DpdkConfig) -> Result<Self, SteeringError> {
        match &config.replication {
            ReplicationNic::Port(nic) => Ok(Self::for_nic(nic, 1)),
            ReplicationNic::Shared | ReplicationNic::Queue(_) => Self::for_client_nic(config),
        }
    }

    /// The TCP destination ports this plan's rules match on, in rule
    /// order, a port given once per rule naming it.
    ///
    /// A connection dialed out on this NIC must not take one of them as
    /// its local port: the NIC would steer the replies, addressed to that
    /// port, by the rule matching it, to whichever queue that rule names,
    /// rather than to the queue the connection was dialed from. In queue
    /// mode a replica dialing its primary from the client port would have
    /// the primary's segments steered to the client queue.
    pub fn destination_ports(&self) -> impl Iterator<Item = u16> + '_ {
        self.rules.iter().filter_map(|rule| rule.tcp_dst_port)
    }

    fn replication_split(
        nic: &NicConfig,
        client_port: u16,
        num_queues: u16,
        ports: ReplicationPorts,
    ) -> Result<Self, SteeringError> {
        if num_queues != 1 {
            return Err(SteeringError::SeveralClientQueues { num_queues });
        }
        let ReplicationPorts { listen, primary } = ports;
        if listen.is_none() && primary.is_none() {
            return Err(SteeringError::NoReplicationPort);
        }
        if listen == Some(client_port) {
            return Err(SteeringError::ReplicationPortIsClientPort { port: client_port });
        }
        let peer = nic.peer_ip;
        let own_ip = peer.map(|_| nic.ip_addr);

        let client = primary.map(|_| SteeringRule {
            src_ipv4: peer,
            tcp_dst_port: Some(client_port),
            ..SteeringRule::to_queue(CLIENT_QUEUE)
        });
        let to_listener = listen.map(|port| SteeringRule {
            dst_ipv4: own_ip,
            tcp_dst_port: Some(port),
            ..SteeringRule::to_queue(REPLICATION_QUEUE)
        });
        // Matched on the port alone, not the primary's address: the
        // primary changes on failover and these rules are installed once.
        // The cost, isolated only: a flow the kernel used to own whose
        // remote port is this one (a host process dialing a service on
        // it, or a non-peer client whose ephemeral port it is) is now
        // captured into queue 1 and lost to the kernel.
        let from_primary = primary.map(|port| SteeringRule {
            dst_ipv4: own_ip,
            tcp_src_port: Some(port),
            ..SteeringRule::to_queue(REPLICATION_QUEUE)
        });
        // Ranks, highest first; a rank with no rule takes no level.
        let ranks: [&[Option<SteeringRule>]; 3] = [
            &[client],
            &[to_listener, from_primary],
            &[peer.map(peer_capture)],
        ];
        let mut rules = Vec::with_capacity(4);
        let mut priority = 0;
        for rank in ranks {
            let before = rules.len();
            rules.extend(
                rank.iter()
                    .flatten()
                    .map(|rule| SteeringRule { priority, ..*rule }),
            );
            if rules.len() > before {
                priority += 1;
            }
        }
        // A replica's ARP, unless the port is isolated (the kernel's then):
        // at the client-port rule's level, the first, which a replica
        // always has.
        let arp_until_promotion = match (client, peer) {
            (Some(_), None) => Some(EtherTypeRule {
                priority: 0,
                ether_type: crate::arp::ETHER_TYPE,
                queue: REPLICATION_QUEUE,
            }),
            _ => None,
        };
        Ok(SteeringPlan {
            queues: QueuePlan::ReplicationSplit,
            isolated: peer.is_some(),
            pin_rss_to_client_queue: peer.is_none(),
            rules,
            arp_until_promotion,
        })
    }
}

/// Take the rules a NIC group holds until its node is promoted out of
/// `held` and remove each with `remove`, which installs nothing and names
/// a rule it could not remove in its error. Returns how many it removed.
///
/// Every rule is taken, so a second call finds none and succeeds with 0:
/// a refused rule stays installed until its port closes rather than being
/// tried again. One refusal does not stop the others' removal; the refusals
/// come back together, joined by `; `.
///
/// Generic over the rule so that the gated port code's
/// (`DpdkShared::end_replica_steering`) runs on every host's tests too.
#[cfg_attr(not(feature = "dpdk-sys"), allow(dead_code))]
pub(crate) fn remove_until_promotion<R>(
    held: &std::sync::Mutex<Vec<R>>,
    mut remove: impl FnMut(R) -> Result<(), String>,
) -> Result<usize, String> {
    let rules = std::mem::take(
        &mut *held
            .lock()
            // Nothing panics while the lock is held (a take), so a
            // poisoned lock still guards a whole list.
            .unwrap_or_else(std::sync::PoisonError::into_inner),
    );
    let mut removed = 0;
    // A `Vec`: the refusals in port order, joined once.
    let mut refused = Vec::new();
    for rule in rules {
        match remove(rule) {
            Ok(()) => removed += 1,
            Err(e) => refused.push(e),
        }
    }
    if refused.is_empty() {
        Ok(removed)
    } else {
        Err(refused.join("; "))
    }
}

/// The isolated (bifurcated) mode's capture: every IPv4 packet from the
/// peer into the client queue, at the highest priority until a plan ranks
/// it.
fn peer_capture(peer: Ipv4Addr) -> SteeringRule {
    SteeringRule {
        src_ipv4: Some(peer),
        ..SteeringRule::to_queue(CLIENT_QUEUE)
    }
}

/// Why a node's configuration has no [`SteeringPlan`]. Worded without the
/// node's flags, which this crate does not declare: the node's own error
/// names them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SteeringError {
    /// Queue mode with more than one client queue: the second queue is
    /// replication's.
    SeveralClientQueues {
        /// The client queues asked for.
        num_queues: u16,
    },
    /// Queue mode with neither replication port: no replication to steer.
    NoReplicationPort,
    /// Queue mode with the node's replication port equal to its client
    /// port: no rule can tell the two apart.
    ReplicationPortIsClientPort {
        /// The port both listeners would take.
        port: u16,
    },
}

impl std::fmt::Display for SteeringError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SteeringError::SeveralClientQueues { num_queues } => write!(
                f,
                "replication on a queue of its own takes the client NIC's second queue, so the \
                 client NIC must have one queue, not {num_queues}"
            ),
            SteeringError::NoReplicationPort => f.write_str(
                "replication on a queue of its own needs a replication port to steer: the \
                 node's own replication listener's, or its primary's",
            ),
            SteeringError::ReplicationPortIsClientPort { port } => write!(
                f,
                "replication on a queue of its own is steered by TCP port, so the replication \
                 port must differ from the client port, and both are {port}"
            ),
        }
    }
}

impl std::error::Error for SteeringError {}

#[cfg(test)]
mod tests {
    use super::*;

    const CLIENT_PORT: u16 = 9876;
    const LISTEN_PORT: u16 = 9877;
    const PRIMARY_PORT: u16 = 9900;
    const OWN_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 1);
    const PEER_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 9);
    const OTHER_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 0, 7);

    fn nic(peer_ip: Option<Ipv4Addr>) -> NicConfig {
        NicConfig {
            ip_addr: OWN_IP,
            peer_ip,
            ..NicConfig::default()
        }
    }

    fn config(peer_ip: Option<Ipv4Addr>, replication: ReplicationNic) -> DpdkConfig {
        DpdkConfig {
            nic: nic(peer_ip),
            listen_port: CLIENT_PORT,
            num_queues: 1,
            replication,
        }
    }

    fn queue_mode(listen: Option<u16>, primary: Option<u16>) -> ReplicationNic {
        ReplicationNic::Queue(ReplicationPorts { listen, primary })
    }

    fn plan(peer_ip: Option<Ipv4Addr>, replication: ReplicationNic) -> SteeringPlan {
        SteeringPlan::for_client_nic(&config(peer_ip, replication)).expect("a plan")
    }

    fn rule(priority: u32, queue: u16) -> SteeringRule {
        SteeringRule {
            priority,
            ..SteeringRule::to_queue(queue)
        }
    }

    /// The bifurcated capture as it was installed before steering plans:
    /// IPv4 from the peer, to queue 0, at priority 0.
    fn todays_peer_rule() -> SteeringRule {
        SteeringRule {
            src_ipv4: Some(PEER_IP),
            ..rule(0, 0)
        }
    }

    #[test]
    fn shared_mode_is_todays_bring_up() {
        assert_eq!(
            plan(None, ReplicationNic::Shared),
            SteeringPlan {
                queues: QueuePlan::Rss(1),
                isolated: false,
                pin_rss_to_client_queue: false,
                rules: vec![],
                arp_until_promotion: None,
            }
        );
        assert_eq!(
            plan(Some(PEER_IP), ReplicationNic::Shared),
            SteeringPlan {
                queues: QueuePlan::Rss(1),
                isolated: true,
                pin_rss_to_client_queue: false,
                rules: vec![todays_peer_rule()],
                arp_until_promotion: None,
            }
        );
    }

    /// The node's queue count, RSS and all, as before.
    #[test]
    fn shared_mode_keeps_the_node_s_queue_count() {
        let config = DpdkConfig {
            num_queues: 4,
            ..config(None, ReplicationNic::Shared)
        };
        let plan = SteeringPlan::for_client_nic(&config).expect("a plan");
        assert_eq!(plan.queues, QueuePlan::Rss(4));
    }

    /// In port mode the client NIC is brought up as in shared mode, and
    /// the replication NIC as a NIC of one queue.
    #[test]
    fn port_mode_brings_both_nics_up_as_today() {
        for peer_ip in [None, Some(PEER_IP)] {
            assert_eq!(
                plan(peer_ip, ReplicationNic::Port(nic(None))),
                plan(peer_ip, ReplicationNic::Shared)
            );
            assert_eq!(
                SteeringPlan::for_nic(&nic(peer_ip), 1),
                plan(peer_ip, ReplicationNic::Shared)
            );
        }
    }

    #[test]
    fn a_primary_steers_its_listener_s_port_and_pins_rss() {
        assert_eq!(
            plan(None, queue_mode(Some(LISTEN_PORT), None)),
            SteeringPlan {
                queues: QueuePlan::ReplicationSplit,
                isolated: false,
                pin_rss_to_client_queue: true,
                rules: vec![SteeringRule {
                    tcp_dst_port: Some(LISTEN_PORT),
                    ..rule(0, REPLICATION_QUEUE)
                }],
                // Its client queue is polled from boot, and takes ARP.
                arp_until_promotion: None,
            }
        );
    }

    /// A replica listening for no replicas of its own: its link to its
    /// primary, under the client port's rule.
    #[test]
    fn a_replica_steers_its_primary_s_port_below_the_client_port() {
        let plan = plan(None, queue_mode(None, Some(PRIMARY_PORT)));
        assert!(plan.pin_rss_to_client_queue);
        assert_eq!(
            plan.rules,
            [
                SteeringRule {
                    tcp_dst_port: Some(CLIENT_PORT),
                    ..rule(0, CLIENT_QUEUE)
                },
                SteeringRule {
                    tcp_src_port: Some(PRIMARY_PORT),
                    ..rule(1, REPLICATION_QUEUE)
                },
            ]
        );
    }

    /// A replica's own replication port and its primary's differ: both
    /// are steered.
    #[test]
    fn a_replica_with_a_listener_steers_both_ports() {
        let plan = plan(None, queue_mode(Some(LISTEN_PORT), Some(PRIMARY_PORT)));
        assert_eq!(plan.queues, QueuePlan::ReplicationSplit);
        assert_eq!(
            plan.rules,
            [
                SteeringRule {
                    tcp_dst_port: Some(CLIENT_PORT),
                    ..rule(0, CLIENT_QUEUE)
                },
                SteeringRule {
                    tcp_dst_port: Some(LISTEN_PORT),
                    ..rule(1, REPLICATION_QUEUE)
                },
                SteeringRule {
                    tcp_src_port: Some(PRIMARY_PORT),
                    ..rule(1, REPLICATION_QUEUE)
                },
            ]
        );
    }

    /// The rule sending ARP to the replication queue.
    const REPLICA_ARP: EtherTypeRule = EtherTypeRule {
        priority: 0,
        ether_type: 0x0806,
        queue: REPLICATION_QUEUE,
    };

    /// A replica in queue mode on a port that is not isolated sends ARP to
    /// the queue it polls, whatever its replication ports, at the first
    /// level, beside its client-port rule.
    #[test]
    fn a_queue_mode_replica_sends_arp_to_its_replication_queue() {
        for listen in [None, Some(LISTEN_PORT), Some(PRIMARY_PORT)] {
            let plan = plan(None, queue_mode(listen, Some(PRIMARY_PORT)));
            assert_eq!(plan.arp_until_promotion, Some(REPLICA_ARP), "{listen:?}");
            assert_eq!(plan.rules[0].priority, REPLICA_ARP.priority);
        }
    }

    /// No ARP rule where the client queue is polled from boot (a primary),
    /// where the kernel keeps ARP (an isolated port, in any role), and in
    /// the modes with no split (shared, port), in any role.
    #[test]
    fn no_arp_rule_but_on_a_queue_mode_replica_s_own_port() {
        for peer_ip in [None, Some(PEER_IP)] {
            let primary = plan(peer_ip, queue_mode(Some(LISTEN_PORT), None));
            assert_eq!(primary.arp_until_promotion, None, "{peer_ip:?}");
            for replication in [ReplicationNic::Shared, ReplicationNic::Port(nic(None))] {
                let config = config(peer_ip, replication);
                for plan in [
                    SteeringPlan::for_client_nic(&config),
                    SteeringPlan::for_replication(&config),
                ] {
                    assert_eq!(plan.expect("a plan").arp_until_promotion, None);
                }
            }
        }
        for listen in [None, Some(LISTEN_PORT)] {
            let isolated_replica = plan(Some(PEER_IP), queue_mode(listen, Some(PRIMARY_PORT)));
            assert_eq!(isolated_replica.arp_until_promotion, None, "{listen:?}");
        }
    }

    /// What no rule takes, ARP included once a replica's ARP rule is
    /// removed at promotion, lands on the client queue on a queue-mode
    /// port that is not isolated, a primary's from boot as a replica's: the
    /// RSS table is pinned there. So a promoted replica steers ARP as a
    /// primary booted in queue mode does, and its client queue takes the
    /// replies to its own requests.
    #[test]
    fn without_the_arp_rule_arp_lands_on_the_client_queue() {
        let booted = plan(None, queue_mode(Some(LISTEN_PORT), None));
        for plan in [
            booted,
            plan(None, queue_mode(Some(LISTEN_PORT), Some(PRIMARY_PORT))),
        ] {
            assert!(plan.pin_rss_to_client_queue && !plan.isolated, "{plan:?}");
        }
    }

    /// Removal at promotion takes every held rule, removes all it can, and
    /// names every refusal; a second promotion finds nothing to remove.
    #[test]
    fn removal_at_promotion_takes_every_rule_once() {
        let held = std::sync::Mutex::new(vec![1, 2, 3]);
        let mut tried = Vec::new();
        let result = remove_until_promotion(&held, |rule: u16| {
            tried.push(rule);
            if rule == 2 {
                Ok(())
            } else {
                Err(format!("rule {rule} refused"))
            }
        });
        assert_eq!(result, Err("rule 1 refused; rule 3 refused".to_owned()));
        assert_eq!(tried, [1, 2, 3]);
        assert_eq!(
            remove_until_promotion(&held, |_: u16| -> Result<(), String> {
                panic!("a rule removed twice")
            }),
            Ok(0)
        );

        let held = std::sync::Mutex::new(vec![1, 2]);
        assert_eq!(remove_until_promotion(&held, |_: u16| Ok(())), Ok(2));
    }

    /// The usual deployment: every node replicates on the same port. Still
    /// two rules, one per direction.
    #[test]
    fn equal_replication_ports_still_get_a_rule_each() {
        let plan = plan(None, queue_mode(Some(LISTEN_PORT), Some(LISTEN_PORT)));
        let replication: Vec<_> = plan
            .rules
            .iter()
            .filter(|rule| rule.queue == REPLICATION_QUEUE)
            .map(|rule| (rule.tcp_src_port, rule.tcp_dst_port))
            .collect();
        assert_eq!(
            replication,
            [(None, Some(LISTEN_PORT)), (Some(LISTEN_PORT), None)]
        );
    }

    #[test]
    fn an_isolated_primary_captures_replication_to_its_own_address_above_the_peer() {
        assert_eq!(
            plan(Some(PEER_IP), queue_mode(Some(LISTEN_PORT), None)),
            SteeringPlan {
                queues: QueuePlan::ReplicationSplit,
                isolated: true,
                pin_rss_to_client_queue: false,
                rules: vec![
                    SteeringRule {
                        dst_ipv4: Some(OWN_IP),
                        tcp_dst_port: Some(LISTEN_PORT),
                        ..rule(0, REPLICATION_QUEUE)
                    },
                    SteeringRule {
                        src_ipv4: Some(PEER_IP),
                        ..rule(1, CLIENT_QUEUE)
                    },
                ],
                arp_until_promotion: None,
            }
        );
    }

    /// The one plan that needs three levels.
    #[test]
    fn an_isolated_replica_ranks_client_replication_then_peer() {
        let plan = plan(
            Some(PEER_IP),
            queue_mode(Some(LISTEN_PORT), Some(PRIMARY_PORT)),
        );
        assert!(plan.isolated);
        assert!(!plan.pin_rss_to_client_queue);
        assert_eq!(
            plan.rules,
            [
                SteeringRule {
                    src_ipv4: Some(PEER_IP),
                    tcp_dst_port: Some(CLIENT_PORT),
                    ..rule(0, CLIENT_QUEUE)
                },
                SteeringRule {
                    dst_ipv4: Some(OWN_IP),
                    tcp_dst_port: Some(LISTEN_PORT),
                    ..rule(1, REPLICATION_QUEUE)
                },
                SteeringRule {
                    dst_ipv4: Some(OWN_IP),
                    tcp_src_port: Some(PRIMARY_PORT),
                    ..rule(1, REPLICATION_QUEUE)
                },
                SteeringRule {
                    src_ipv4: Some(PEER_IP),
                    ..rule(2, CLIENT_QUEUE)
                },
            ]
        );
    }

    /// A TCP segment as the rules see it.
    #[derive(Clone, Copy, Debug)]
    struct Segment {
        src_ip: Ipv4Addr,
        dst_ip: Ipv4Addr,
        src_port: u16,
        dst_port: u16,
    }

    fn matches(rule: &SteeringRule, segment: &Segment) -> bool {
        rule.src_ipv4.is_none_or(|ip| ip == segment.src_ip)
            && rule.dst_ipv4.is_none_or(|ip| ip == segment.dst_ip)
            && rule.tcp_src_port.is_none_or(|p| p == segment.src_port)
            && rule.tcp_dst_port.is_none_or(|p| p == segment.dst_port)
    }

    /// Where `plan` sends `segment`, as `rte_flow` would: the queue of the
    /// highest-priority rule matching it, or `None` when none does (the
    /// kernel's, isolated; the default RSS's, otherwise). Fails the test
    /// when rules at that level disagree, which `rte_flow` leaves
    /// undefined.
    fn steer(plan: &SteeringPlan, segment: Segment) -> Option<u16> {
        let best = plan
            .rules
            .iter()
            .filter(|rule| matches(rule, &segment))
            .map(|rule| rule.priority)
            .min()?;
        let mut queues = plan
            .rules
            .iter()
            .filter(|rule| rule.priority == best && matches(rule, &segment))
            .map(|rule| rule.queue);
        let queue = queues.next();
        assert!(
            queues.all(|q| Some(q) == queue),
            "rules at priority {best} disagree on {segment:?}"
        );
        queue
    }

    /// Every flow a node in queue mode carries, in each mode and role, at
    /// the queue it belongs on, including the client whose ephemeral port
    /// is a replication port.
    #[test]
    fn every_flow_lands_on_its_queue() {
        let ephemeral = 50_000;
        for peer_ip in [None, Some(PEER_IP)] {
            for (listen, primary) in [
                (Some(LISTEN_PORT), None),
                (None, Some(PRIMARY_PORT)),
                (Some(LISTEN_PORT), Some(PRIMARY_PORT)),
                (Some(LISTEN_PORT), Some(LISTEN_PORT)),
            ] {
                let plan = plan(peer_ip, queue_mode(listen, primary));
                let context = format!("peer {peer_ip:?}, listen {listen:?}, primary {primary:?}");
                // Unmatched traffic goes to the client queue: the default
                // RSS, pinned to it, or, isolated, the kernel.
                let default = if peer_ip.is_some() {
                    None
                } else {
                    Some(CLIENT_QUEUE)
                };
                let client_ip = peer_ip.unwrap_or(OTHER_IP);
                // Clients, from every ephemeral port, the replication ports
                // included.
                for src_port in [ephemeral, LISTEN_PORT, PRIMARY_PORT] {
                    let to_client_port = Segment {
                        src_ip: client_ip,
                        dst_ip: OWN_IP,
                        src_port,
                        dst_port: CLIENT_PORT,
                    };
                    assert_eq!(
                        steer(&plan, to_client_port).or(default),
                        Some(CLIENT_QUEUE),
                        "{context}: {to_client_port:?}"
                    );
                }
                // The node's replicas, from any host and port.
                if let Some(listen) = listen {
                    for src_ip in [PEER_IP, OTHER_IP] {
                        for src_port in [ephemeral, CLIENT_PORT, PRIMARY_PORT] {
                            let to_listener = Segment {
                                src_ip,
                                dst_ip: OWN_IP,
                                src_port,
                                dst_port: listen,
                            };
                            assert_eq!(
                                steer(&plan, to_listener),
                                Some(REPLICATION_QUEUE),
                                "{context}: {to_listener:?}"
                            );
                        }
                    }
                }
                // The node's primary, from any host, to the node's dial
                // port (never the client port: see `for_client_nic`).
                if let Some(primary) = primary {
                    for src_ip in [PEER_IP, OTHER_IP] {
                        let from_primary = Segment {
                            src_ip,
                            dst_ip: OWN_IP,
                            src_port: primary,
                            dst_port: 40_000,
                        };
                        assert_eq!(
                            steer(&plan, from_primary),
                            Some(REPLICATION_QUEUE),
                            "{context}: {from_primary:?}"
                        );
                    }
                }
                // Anything else from the peer: the client queue.
                let other = Segment {
                    src_ip: client_ip,
                    dst_ip: OWN_IP,
                    src_port: ephemeral,
                    dst_port: 22,
                };
                if peer_ip.is_some() {
                    assert_eq!(steer(&plan, other), Some(CLIENT_QUEUE), "{context}");
                    // Isolated: what is neither the peer's nor replication
                    // stays with the kernel, as before.
                    let stranger = Segment {
                        src_ip: OTHER_IP,
                        ..other
                    };
                    assert_eq!(steer(&plan, stranger), None, "{context}");
                    // The documented cost of matching the primary on its
                    // port alone: a non-peer client whose ephemeral port is
                    // the primary's replication port is captured into the
                    // replication queue rather than left with the kernel
                    // (the client-port rule matches the peer only).
                    if let Some(primary) = primary {
                        let stranger_client = Segment {
                            src_ip: OTHER_IP,
                            dst_ip: OWN_IP,
                            src_port: primary,
                            dst_port: CLIENT_PORT,
                        };
                        assert_eq!(
                            steer(&plan, stranger_client),
                            Some(REPLICATION_QUEUE),
                            "{context}: {stranger_client:?}"
                        );
                    }
                    // Replication ports addressed elsewhere are not
                    // captured either.
                    let elsewhere = Segment {
                        src_ip: OTHER_IP,
                        dst_ip: Ipv4Addr::new(10, 0, 0, 2),
                        src_port: PRIMARY_PORT,
                        dst_port: LISTEN_PORT,
                    };
                    assert_eq!(steer(&plan, elsewhere), None, "{context}");
                } else {
                    assert_eq!(steer(&plan, other).or(default), Some(CLIENT_QUEUE));
                }
            }
        }
    }

    /// Replication runs on the client NIC but in port mode, whose
    /// replication NIC is brought up as a NIC of one queue.
    #[test]
    fn replication_runs_on_the_client_nic_but_in_port_mode() {
        for replication in [
            ReplicationNic::Shared,
            queue_mode(Some(LISTEN_PORT), Some(PRIMARY_PORT)),
        ] {
            for peer_ip in [None, Some(PEER_IP)] {
                let config = config(peer_ip, replication.clone());
                assert_eq!(
                    SteeringPlan::for_replication(&config),
                    SteeringPlan::for_client_nic(&config)
                );
            }
        }
        let replication_nic = NicConfig {
            ip_addr: Ipv4Addr::new(10, 0, 0, 2),
            peer_ip: None,
            ..NicConfig::default()
        };
        // The client NIC's bifurcated capture is not the replication NIC's.
        let port_mode = config(Some(PEER_IP), ReplicationNic::Port(replication_nic.clone()));
        assert_eq!(
            SteeringPlan::for_replication(&port_mode),
            Ok(SteeringPlan::for_nic(&replication_nic, 1))
        );
        // Refused as the client NIC's plan is.
        let refused = config(None, queue_mode(Some(CLIENT_PORT), None));
        assert_eq!(
            SteeringPlan::for_replication(&refused),
            Err(SteeringError::ReplicationPortIsClientPort { port: CLIENT_PORT })
        );
    }

    /// The ports a dial on the NIC must not take: those its rules steer as
    /// a destination, the client port first on a replica in queue mode.
    #[test]
    fn destination_ports_are_the_rules_destination_ports() {
        for peer_ip in [None, Some(PEER_IP)] {
            let replica = plan(peer_ip, queue_mode(Some(LISTEN_PORT), Some(PRIMARY_PORT)));
            assert_eq!(
                replica.destination_ports().collect::<Vec<_>>(),
                [CLIENT_PORT, LISTEN_PORT]
            );
            let replica = plan(peer_ip, queue_mode(None, Some(PRIMARY_PORT)));
            assert_eq!(
                replica.destination_ports().collect::<Vec<_>>(),
                [CLIENT_PORT]
            );
            let primary = plan(peer_ip, queue_mode(Some(LISTEN_PORT), None));
            assert_eq!(
                primary.destination_ports().collect::<Vec<_>>(),
                [LISTEN_PORT]
            );
            // Shared and port modes steer by no port.
            for replication in [ReplicationNic::Shared, ReplicationNic::Port(nic(None))] {
                let config = config(peer_ip, replication);
                for plan in [
                    SteeringPlan::for_client_nic(&config),
                    SteeringPlan::for_replication(&config),
                ] {
                    assert_eq!(plan.expect("a plan").destination_ports().count(), 0);
                }
            }
        }
    }

    /// A dial whose local port no rule matches as a destination is answered
    /// on the queue it was dialed from: on a replica in queue mode, the
    /// replication queue, whatever its port, where the client port would
    /// have the answers steered to the client queue.
    #[test]
    fn a_dial_from_a_port_no_rule_steers_is_answered_on_its_own_queue() {
        let plan = plan(None, queue_mode(Some(LISTEN_PORT), Some(PRIMARY_PORT)));
        let reserved: Vec<u16> = plan.destination_ports().collect();
        let from_primary = |dst_port| Segment {
            src_ip: OTHER_IP,
            dst_ip: OWN_IP,
            src_port: PRIMARY_PORT,
            dst_port,
        };
        let mut ports = crate::LocalPorts::from_first(CLIENT_PORT - 1);
        for _ in 0..4 {
            let port = ports.take(|p| reserved.contains(&p)).expect("a local port");
            assert_ne!(port, CLIENT_PORT);
            assert_eq!(steer(&plan, from_primary(port)), Some(REPLICATION_QUEUE));
        }
        // What the exclusion prevents.
        assert_eq!(steer(&plan, from_primary(CLIENT_PORT)), Some(CLIENT_QUEUE));
    }

    /// Installed highest priority first.
    #[test]
    fn rules_come_highest_priority_first_from_zero_without_gaps() {
        for peer_ip in [None, Some(PEER_IP)] {
            for (listen, primary) in [
                (Some(LISTEN_PORT), None),
                (None, Some(PRIMARY_PORT)),
                (Some(LISTEN_PORT), Some(PRIMARY_PORT)),
            ] {
                let plan = plan(peer_ip, queue_mode(listen, primary));
                let levels: Vec<u32> = plan.rules.iter().map(|rule| rule.priority).collect();
                assert!(levels.is_sorted(), "{levels:?}");
                let mut distinct = levels.clone();
                distinct.dedup();
                let dense: Vec<u32> = (0..distinct.len() as u32).collect();
                assert_eq!(distinct, dense);
                // The ARP rule shares a level the rules take: it adds none.
                if let Some(arp) = plan.arp_until_promotion {
                    assert!(levels.contains(&arp.priority), "{arp} beside {levels:?}");
                }
            }
        }
    }

    #[test]
    fn queue_mode_with_several_client_queues_is_refused() {
        let config = DpdkConfig {
            num_queues: 2,
            ..config(None, queue_mode(Some(LISTEN_PORT), None))
        };
        let error = SteeringPlan::for_client_nic(&config).unwrap_err();
        assert_eq!(error, SteeringError::SeveralClientQueues { num_queues: 2 });
        assert!(error.to_string().contains("not 2"), "{error}");
    }

    #[test]
    fn queue_mode_with_no_replication_port_is_refused() {
        let error =
            SteeringPlan::for_client_nic(&config(None, queue_mode(None, None))).unwrap_err();
        assert_eq!(error, SteeringError::NoReplicationPort);
        assert!(error.to_string().contains("replication port"), "{error}");
    }

    #[test]
    fn a_replication_port_equal_to_the_client_port_is_refused() {
        let error =
            SteeringPlan::for_client_nic(&config(None, queue_mode(Some(CLIENT_PORT), None)))
                .unwrap_err();
        assert_eq!(
            error,
            SteeringError::ReplicationPortIsClientPort { port: CLIENT_PORT }
        );
        assert!(error.to_string().contains("both are 9876"), "{error}");
        // The primary's port may be the node's client port: a primary's
        // segments go to the replica's dial port, never its client port.
        assert!(
            SteeringPlan::for_client_nic(&config(None, queue_mode(None, Some(CLIENT_PORT))))
                .is_ok()
        );
    }

    #[test]
    fn rss_clamps_to_the_nic_and_the_split_needs_two_queues() {
        assert_eq!(QueuePlan::Rss(1).queue_pairs_on(16, 16), Ok(1));
        assert_eq!(QueuePlan::Rss(4).queue_pairs_on(2, 8), Ok(2));
        assert_eq!(QueuePlan::Rss(4).queue_pairs_on(8, 3), Ok(3));
        // A port reporting no queues counts as one.
        assert_eq!(QueuePlan::Rss(2).queue_pairs_on(0, 0), Ok(1));
        assert_eq!(QueuePlan::ReplicationSplit.queue_pairs_on(2, 2), Ok(2));
        assert_eq!(QueuePlan::ReplicationSplit.queue_pairs_on(16, 4), Ok(2));
        for (max_rx, max_tx) in [(1, 1), (1, 8), (8, 1), (0, 0)] {
            let error = QueuePlan::ReplicationSplit
                .queue_pairs_on(max_rx, max_tx)
                .unwrap_err();
            assert_eq!(
                error,
                TooFewQueues {
                    required: 2,
                    available: 1
                }
            );
            assert_eq!(
                error.to_string(),
                "2 queue pairs are needed, and the NIC offers 1"
            );
        }
        assert_eq!(QueuePlan::Rss(3).requested_queue_pairs(), 3);
        assert_eq!(QueuePlan::ReplicationSplit.requested_queue_pairs(), 2);
    }

    /// The client poll threads a NIC needs: one per queue the default RSS
    /// spreads clients over, as many as the port was configured with, and
    /// in the replication split one, whatever the port has, since its
    /// second queue is replication's.
    #[test]
    fn the_split_has_one_client_queue_and_rss_has_every_queue() {
        assert_eq!(QueuePlan::Rss(1).client_queues(1), 1);
        // The configured count, not the requested one: a NIC that clamped
        // the request has fewer queues to poll.
        assert_eq!(QueuePlan::Rss(4).client_queues(2), 2);
        assert_eq!(QueuePlan::ReplicationSplit.client_queues(2), 1);
        // The split's queue count as a port reports it is not the client
        // queue count, which is what a node used to spawn from.
        let split = plan(None, queue_mode(Some(LISTEN_PORT), None));
        let configured = split
            .queues
            .queue_pairs_on(16, 16)
            .expect("two queue pairs");
        assert_eq!(configured, 2);
        assert_eq!(split.queues.client_queues(configured), 1);
        assert_eq!(
            usize::from(split.queues.client_queues(configured)),
            usize::from(CLIENT_QUEUE) + 1,
            "the one client queue is queue 0"
        );
    }

    #[test]
    fn a_rule_displays_its_fields_queue_and_priority() {
        assert_eq!(
            todays_peer_rule().to_string(),
            "ipv4 src 10.0.0.9 -> queue 0 (priority 0)"
        );
        let rule = SteeringRule {
            src_ipv4: Some(PEER_IP),
            dst_ipv4: Some(OWN_IP),
            tcp_src_port: Some(PRIMARY_PORT),
            tcp_dst_port: Some(LISTEN_PORT),
            ..rule(2, REPLICATION_QUEUE)
        };
        assert_eq!(
            rule.to_string(),
            "ipv4 src 10.0.0.9, ipv4 dst 10.0.0.1, tcp src port 9900, tcp dst port 9877 -> queue \
             1 (priority 2)"
        );
        assert_eq!(
            SteeringRule::to_queue(0).to_string(),
            "any ipv4 -> queue 0 (priority 0)"
        );
        assert_eq!(
            REPLICA_ARP.to_string(),
            "ether type 0x0806 -> queue 1 (priority 0)"
        );
    }
}
