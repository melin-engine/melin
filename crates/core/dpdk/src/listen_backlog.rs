//! The node transport's listening sockets: a backlog of them per port.
//!
//! The stack has no accept queue. It hands an inbound SYN to the first
//! socket in its set that is listening on the segment's port (a linear
//! scan in slot order), and answers a segment no socket accepts with an
//! RST. A listening socket stops listening at that SYN: it is bound to the
//! one peer from then on, through its handshake and for the life of the
//! connection. One listening socket per port, replaced once its
//! connection is established, therefore refuses every SYN that reaches the
//! port between a handshake's SYN and the poll that replaces the socket,
//! as two clients dialing in turn do whenever one's final ACK and the
//! next one's SYN arrive in the same poll. A kernel node would queue the
//! second in its listen backlog.
//!
//! So each port keeps a backlog of listening sockets ([`listen_backlog`]),
//! and every poll hands out the ones whose handshake has completed and
//! opens fresh ones in their place: a port takes as many handshakes at
//! once as its backlog holds.
//!
//! Plain smoltcp with no libdpdk in it, so it is tested on every host over
//! an in-memory wire, like `peer_liveness`. The transport
//! (`dpdk::transport`) binds it to its socket set and registers what the
//! NIC needs on each accepted socket.

use std::net::SocketAddr;

use smoltcp::iface::{Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, State};

/// Apply low-latency TCP tuning to a smoltcp socket.
///
/// Called on every socket (listen, accepted and dialed) to configure for
/// low latency, and crate-visible so that every socket the crate opens on
/// a NIC carries the same tuning:
/// - Nagle disabled (TCP_NODELAY): send small messages immediately
/// - Delayed ACK disabled: ACK every segment without waiting 10ms
/// - RTO floor and initial RTO both at 1ms — fastcp's minimum (its RTT
///   estimator is u32-millisecond, so sub-ms truncates to 0 and panics).
///   The response path (server->client) is what gates client-visible
///   latency, and a 50ms initial RTO turned any dropped response segment
///   into a 50ms tail; on a µs-RTT LAN the first retransmit should be ~1ms.
/// - Initial congestion window raised to 64 KiB (skip slow start on LAN)
///
/// These settings sacrifice marginal bandwidth efficiency and RFC
/// compliance for latency on a trusted, dedicated LAN.
pub(crate) fn tune_socket(socket: &mut tcp::Socket<'_>) {
    socket.set_nagle_enabled(false);
    socket.set_ack_delay(None);
    socket.set_min_rto(smoltcp::time::Duration::from_millis(1));
    socket.set_initial_rto(smoltcp::time::Duration::from_millis(1));
    socket.set_initial_congestion_window(64 * 1024);
}

/// The most sockets a port keeps listening.
///
/// A connection holds a listening socket from its SYN until the poll after
/// its handshake completes: a round trip and a poll iteration, on a LAN.
/// Clients that dial in turn need two (one's final ACK and the next one's
/// SYN in the same poll); the rest takes clients that connect at once, as
/// after a failover, whose SYNs land within one handshake of each other.
/// A SYN beyond the backlog is still refused, and a client that retries
/// its connect (`melin-client` does) gets in on a later attempt.
///
/// Every listening socket is a slot of the transport's socket set, which
/// its connections fill too (`MAX_CONNECTIONS` slots, the bound every
/// per-connection table is sized for), so each one a port keeps is one
/// connection fewer the transport can hold; a transport listens on two
/// ports at most (clients, replication). And every one holds its port's
/// socket buffers from the moment it is opened: what they cost picks the
/// depth below this, against [`LISTEN_BACKLOG_BUFFER_BUDGET`], down to the
/// [`MIN_LISTEN_BACKLOG`] floor, which may hold more than the budget.
const MAX_LISTEN_BACKLOG: usize = 8;

/// The fewest sockets a port keeps listening, whatever its buffers cost:
/// two, so that a connection completing its handshake never refuses the
/// next one's SYN. Also what the replication port needs: a primary has at
/// most two replicas, which reconnect together after a failover.
const MIN_LISTEN_BACKLOG: usize = 2;

/// The socket buffers a port's backlog may hold before it is cut below
/// [`MAX_LISTEN_BACKLOG`], in bytes. A client socket's buffers fit the
/// whole backlog in it; a port whose sockets have buffers several times
/// larger (replication's, sized for journal batches) gets fewer listening
/// sockets, down to [`MIN_LISTEN_BACKLOG`].
const LISTEN_BACKLOG_BUFFER_BUDGET: usize = 1024 * 1024;

// `listen_backlog` clamps between the two, which panics if they cross, and
// a backlog below two refuses the SYN behind every handshake.
const _: () = assert!(2 <= MIN_LISTEN_BACKLOG && MIN_LISTEN_BACKLOG <= MAX_LISTEN_BACKLOG);

/// The most ports a transport listens on: clients and replication. Every
/// port's backlog takes slots of the socket set its connections need, so
/// the transport bounds what they take (`MAX_PORTS * MAX_LISTEN_BACKLOG`)
/// against its connection count, and [`Listeners::add`] refuses a port
/// beyond this.
pub(crate) const MAX_PORTS: usize = 2;

/// The most listening sockets a transport holds, every port's backlog full.
/// For the transport's bound, so compiled with it.
#[cfg(feature = "dpdk-sys")]
pub(crate) const MAX_LISTENING_SOCKETS: usize = MAX_PORTS * MAX_LISTEN_BACKLOG;

/// How long a listening socket waits in its handshake for the peer before
/// it gives up, closes, and is replaced by a fresh one.
///
/// The stack retransmits an unanswered SYN-ACK without end, so without it
/// a peer that went away after its SYN (crashed, unplugged, or a source
/// that cannot answer) would hold its socket for good, and as many such
/// SYNs as the backlog holds would leave the port deaf until a restart; a
/// kernel's SYN backlog expires them too. Counted from the last segment
/// the peer sent. A handshake on a LAN takes a round trip, so this only
/// has to outlast a peer briefly stalled mid-handshake, and is short
/// enough that a backlog filled by dead SYNs listens again within seconds,
/// before a client retrying its connect has given up. A `smoltcp`
/// duration: what the socket's timeout takes.
const HANDSHAKE_TIMEOUT: smoltcp::time::Duration = smoltcp::time::Duration::from_secs(5);

/// How many sockets a port whose sockets get `rx_buf_size` and
/// `tx_buf_size` byte buffers keeps listening: as many as
/// [`LISTEN_BACKLOG_BUFFER_BUDGET`] holds, within
/// [`MIN_LISTEN_BACKLOG`]..=[`MAX_LISTEN_BACKLOG`].
pub(crate) fn listen_backlog(rx_buf_size: usize, tx_buf_size: usize) -> usize {
    // Saturating: a socket larger than the address space fits the budget
    // no times, which the floor then lifts.
    let per_socket = rx_buf_size.saturating_add(tx_buf_size).max(1);
    (LISTEN_BACKLOG_BUFFER_BUDGET / per_socket).clamp(MIN_LISTEN_BACKLOG, MAX_LISTEN_BACKLOG)
}

/// What a port's accepted sockets get: their buffers, transmit queue limit
/// and egress burst.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ListenerConfig {
    pub(crate) port: u16,
    /// smoltcp RX buffer size for sockets accepted on this port.
    pub(crate) rx_buf_size: usize,
    /// smoltcp TX buffer size for sockets accepted on this port.
    pub(crate) tx_buf_size: usize,
    /// TX queue limit for sockets accepted on this port, which the
    /// transport keeps above the socket.
    pub(crate) tx_queue_limit: usize,
    /// Data segments a socket accepted on this port may dispatch per
    /// egress pass. See `DpdkTransport::add_listener_with_buffers`.
    pub(crate) dispatch_burst_limit: usize,
}

impl ListenerConfig {
    /// A fresh tuned socket with this port's buffers, listening on it,
    /// with a [`HANDSHAKE_TIMEOUT`] that [`Listeners::accept`] lifts.
    fn listening_socket(&self) -> Result<tcp::Socket<'static>, tcp::ListenError> {
        let rx_buf = tcp::SocketBuffer::new(vec![0u8; self.rx_buf_size]);
        let tx_buf = tcp::SocketBuffer::new(vec![0u8; self.tx_buf_size]);
        let mut socket = tcp::Socket::new(rx_buf, tx_buf);
        tune_socket(&mut socket);
        // The stack only counts it once a peer has sent a segment, so a
        // socket still listening never times out.
        socket.set_timeout(Some(HANDSHAKE_TIMEOUT));
        socket.listen(self.port)?;
        Ok(socket)
    }
}

/// A connection whose handshake completed on a listening socket, handed
/// out by [`Listeners::accept`].
pub(crate) struct Accepted<'a> {
    pub(crate) handle: SocketHandle,
    pub(crate) peer: SocketAddr,
    /// The port it was accepted on, and what its sockets get.
    pub(crate) config: &'a ListenerConfig,
}

/// One port's backlog.
struct PortBacklog {
    config: ListenerConfig,
    /// How many sockets the port keeps listening.
    depth: usize,
    /// The port's sockets that have not been handed out: listening, or in
    /// a handshake. A `Vec`: a few entries, scanned in full every poll,
    /// and kept in the order they were opened so the oldest is looked at
    /// first.
    pending: Vec<SocketHandle>,
}

/// Every port a transport listens on, each with its backlog.
pub(crate) struct Listeners {
    /// A `Vec`: a transport listens on [`MAX_PORTS`] ports at most,
    /// scanned in full every poll.
    ports: Vec<PortBacklog>,
    /// Slots of the socket set a socket may take: a listening socket that
    /// lands at or above it is closed again, so no connection is ever
    /// handed out on a handle beyond the transport's per-connection
    /// tables. The set itself grows past it rather than refusing a socket.
    /// Whether a slot below it is free is looked up when a socket is to be
    /// opened ([`free_slot_below`]), never remembered: nothing has to tell
    /// the backlog when a connection's socket leaves the set.
    slot_cap: usize,
}

impl Listeners {
    /// No port listened on, sockets placed below `slot_cap`.
    pub(crate) fn new(slot_cap: usize) -> Self {
        Self {
            ports: Vec::with_capacity(MAX_PORTS),
            slot_cap,
        }
    }

    /// Listen on `config.port` with a backlog of [`listen_backlog`]
    /// sockets for its buffers.
    ///
    /// # Errors
    ///
    /// The stack refused to listen on the port (port zero), or the
    /// transport already listens on [`MAX_PORTS`] ports.
    pub(crate) fn add(
        &mut self,
        sockets: &mut SocketSet<'static>,
        config: ListenerConfig,
    ) -> Result<(), String> {
        self.add_with_depth(
            sockets,
            config,
            listen_backlog(config.rx_buf_size, config.tx_buf_size),
        )
    }

    /// [`Self::add`] with a backlog of `depth` sockets.
    fn add_with_depth(
        &mut self,
        sockets: &mut SocketSet<'static>,
        config: ListenerConfig,
        depth: usize,
    ) -> Result<(), String> {
        if self.ports.len() >= MAX_PORTS {
            return Err(format!(
                "TCP listen on port {}: the transport already listens on {MAX_PORTS} ports",
                config.port
            ));
        }
        // Refused here, before the port is recorded, rather than on every
        // top-up after.
        let first = config
            .listening_socket()
            .map_err(|e| format!("TCP listen on port {} failed: {e}", config.port))?;
        let mut port = PortBacklog {
            config,
            depth,
            pending: Vec::with_capacity(depth),
        };
        if free_slot_below(sockets, self.slot_cap) {
            port.pending.push(sockets.add(first));
        } else {
            tracing::warn!(
                port = config.port,
                "DPDK: socket set full, port listens once a connection closes"
            );
        }
        self.ports.push(port);
        self.top_up(sockets);
        Ok(())
    }

    /// Hand every connection whose handshake has completed to `on_accept`,
    /// with its socket, and put a fresh listening socket in its place.
    ///
    /// Its socket has its port's egress burst already; the rest of what
    /// the port's sockets get is for `on_accept` to apply. A connection
    /// the peer has already closed (its FIN arrived with the handshake) is
    /// handed out too, as a kernel's accept does, and the reader finds it
    /// closed. One the peer reset before it was handed out, or whose
    /// handshake timed out ([`HANDSHAKE_TIMEOUT`]), is dropped and
    /// replaced; `iface` forgets its socket, which the interface indexed
    /// by the peer's four-tuple when its SYN arrived and cannot see leave
    /// the set (a handshake that timed out is closed on egress, which
    /// never evicts the entry), so each such peer would otherwise hold a
    /// slot of the index for the life of the process. A pending socket
    /// that has left the set is dropped from the backlog and replaced too,
    /// without touching the index: its handle may already be reused. A
    /// connection is handed out with no timeout, the handshake's lifted:
    /// its liveness is the transport's to arm.
    ///
    /// Each port's connections are handed out in the order their sockets
    /// were opened, and every completed handshake is handed out by the
    /// call that sees it, so none waits behind another.
    pub(crate) fn accept(
        &mut self,
        iface: &mut Interface,
        sockets: &mut SocketSet<'static>,
        mut on_accept: impl FnMut(&mut tcp::Socket<'static>, Accepted<'_>),
    ) {
        for port in &mut self.ports {
            let mut i = 0;
            while i < port.pending.len() {
                let handle = port.pending[i];
                let Some(socket) = sockets.try_get_mut::<tcp::Socket>(handle) else {
                    tracing::warn!(
                        handle = ?handle,
                        port = port.config.port,
                        site = "check_listener",
                        "DPDK: stale listener handle, removing from the backlog"
                    );
                    port.pending.remove(i);
                    continue;
                };
                let remote = match socket.state() {
                    State::Listen | State::SynReceived => {
                        i += 1;
                        continue;
                    }
                    // Always with a peer.
                    State::Established | State::CloseWait => socket.remote_endpoint(),
                    // Reset by the peer before it was handed out, or
                    // its handshake timed out.
                    _ => None,
                };
                port.pending.remove(i);
                let Some(remote) = remote else {
                    // Nothing to hand out: its slot goes to a fresh one.
                    // Forgotten before `top_up` can hand its handle to
                    // that fresh socket (see `forget_tcp_socket`).
                    sockets.remove(handle);
                    iface.forget_tcp_socket(handle);
                    continue;
                };
                socket.set_timeout(None);
                socket.set_dispatch_burst_limit(port.config.dispatch_burst_limit);
                on_accept(
                    socket,
                    Accepted {
                        handle,
                        peer: SocketAddr::from(remote),
                        config: &port.config,
                    },
                );
            }
        }
        self.top_up(sockets);
    }

    /// Open listening sockets until every port has its backlog, or the set
    /// has no slot below the cap. One socket per port short of its backlog
    /// in turn, so that slots freed in a full set are shared between the
    /// ports rather than all taken by the first.
    fn top_up(&mut self, sockets: &mut SocketSet<'static>) {
        loop {
            let mut opened = false;
            for port in &mut self.ports {
                if port.pending.len() >= port.depth {
                    continue;
                }
                // Looked up before the socket's buffers are allocated, so
                // that a full set costs no allocation on every poll.
                if !free_slot_below(sockets, self.slot_cap) {
                    return;
                }
                match port.config.listening_socket() {
                    Ok(socket) => {
                        // Takes the slot just found: `add` fills the lowest
                        // free one.
                        port.pending.push(sockets.add(socket));
                        opened = true;
                    }
                    // The port was listened on once, so this cannot fail;
                    // if it did, the port keeps the sockets it has.
                    Err(e) => {
                        tracing::warn!(port = port.config.port, error = %e, "DPDK: re-listen failed");
                    }
                }
            }
            if !opened {
                return;
            }
        }
    }

    /// How many of `port`'s sockets are listening or in a handshake.
    #[cfg(test)]
    fn pending(&self, port: u16) -> usize {
        self.ports
            .iter()
            .find(|p| p.config.port == port)
            .map_or(0, |p| p.pending.len())
    }
}

/// Whether the set has a free slot below `slot_cap`, where the next socket
/// added would land (`add` fills the lowest free slot).
///
/// Found by adding a socket with no buffers, which allocates nothing, and
/// taking it out again: the set keeps no count of its free slots, and its
/// `add` scans for the first one anyway, so this is the cost of the add it
/// precedes. Looked up every time rather than remembered, so that no
/// removal from the set, the transport's or this module's, has to report
/// a freed slot.
fn free_slot_below(sockets: &mut SocketSet<'static>, slot_cap: usize) -> bool {
    let probe = sockets.add(tcp::Socket::new(
        tcp::SocketBuffer::new(Vec::new()),
        tcp::SocketBuffer::new(Vec::new()),
    ));
    let free = probe.index() < slot_cap;
    sockets.remove(probe);
    free
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use smoltcp::time::{Duration, Instant};

    use super::*;
    use crate::test_wire::{End, Wire, interface, ip};

    const PORT: u16 = 9876;
    const OTHER_PORT: u16 = 9877;
    /// Buffers small enough that the budget holds the whole backlog.
    const BUF: usize = 4096;

    fn config(port: u16) -> ListenerConfig {
        ListenerConfig {
            port,
            rx_buf_size: BUF,
            tx_buf_size: BUF,
            tx_queue_limit: 3 * BUF,
            dispatch_burst_limit: 7,
        }
    }

    /// The node (side 0, with its listeners) and a client host (side 1,
    /// dialing from as many sockets as a test opens), on a virtual clock.
    struct Net {
        wire: Rc<RefCell<Wire>>,
        node: Interface,
        node_device: End,
        node_sockets: SocketSet<'static>,
        listeners: Listeners,
        accepted: Vec<(SocketHandle, SocketAddr, ListenerConfig)>,
        client: Interface,
        client_device: End,
        client_sockets: SocketSet<'static>,
        now: Instant,
        next_local_port: u16,
    }

    impl Net {
        /// A node listening on [`PORT`] with a backlog of `depth`, its
        /// socket set capped at `slot_cap`.
        fn new(depth: usize, slot_cap: usize) -> Self {
            let wire = Rc::new(RefCell::new(Wire::default()));
            let now = Instant::from_millis(1_000_000);
            let mut node_device = End::new(&wire, 0);
            let mut client_device = End::new(&wire, 1);
            let mut net = Self {
                wire,
                node: interface(&mut node_device, 0, now),
                node_device,
                node_sockets: SocketSet::new(Vec::new()),
                listeners: Listeners::new(slot_cap),
                accepted: Vec::new(),
                client: interface(&mut client_device, 1, now),
                client_device,
                client_sockets: SocketSet::new(Vec::new()),
                now,
                next_local_port: 40_000,
            };
            net.listeners
                .add_with_depth(&mut net.node_sockets, config(PORT), depth)
                .unwrap();
            // Each host learns the other's MAC from a dial to a port
            // nothing listens on, so that a test's SYNs go out on the poll
            // after their dial rather than behind an ARP exchange.
            let probe = net.dial(1);
            assert!(net.run_until(|n| n.client_state(probe) == State::Closed));
            net.client_sockets.remove(probe);
            net
        }

        /// A client socket dialing the node's `port`; its SYN goes out on
        /// the next [`Self::poll_client`].
        fn dial(&mut self, port: u16) -> SocketHandle {
            let mut socket = tcp::Socket::new(
                tcp::SocketBuffer::new(vec![0; BUF]),
                tcp::SocketBuffer::new(vec![0; BUF]),
            );
            socket
                .connect(self.client.context(), (ip(0), port), self.next_local_port)
                .unwrap();
            self.next_local_port += 1;
            self.client_sockets.add(socket)
        }

        fn client_state(&mut self, handle: SocketHandle) -> State {
            self.client_sockets.get_mut::<tcp::Socket>(handle).state()
        }

        /// The client host's poll: what it sends lands in the node's
        /// inbox, unread until the node polls.
        fn poll_client(&mut self) {
            self.client
                .poll(self.now, &mut self.client_device, &mut self.client_sockets);
        }

        /// The node's poll as the transport runs it: the stack over every
        /// frame in its inbox, then the accept pass.
        fn poll_node(&mut self) {
            self.node
                .poll(self.now, &mut self.node_device, &mut self.node_sockets);
            let accepted = &mut self.accepted;
            self.listeners
                .accept(&mut self.node, &mut self.node_sockets, |_, conn| {
                    accepted.push((conn.handle, conn.peer, *conn.config));
                });
        }

        /// Both hosts in turn, the clock advanced, until `done` or a
        /// second of virtual time.
        fn run_until(&mut self, mut done: impl FnMut(&mut Self) -> bool) -> bool {
            let end = self.now + Duration::from_secs(1);
            while self.now < end {
                if done(self) {
                    return true;
                }
                self.now += Duration::from_millis(1);
                self.poll_client();
                self.poll_node();
            }
            done(self)
        }

        /// Dial the node's `port` and run until the node accepts.
        fn connect(&mut self, port: u16) -> SocketHandle {
            let before = self.accepted.len();
            let client = self.dial(port);
            assert!(
                self.run_until(|n| n.accepted.len() > before),
                "the node accepts"
            );
            client
        }
    }

    /// Run the dial of `client` until it is established on the client's
    /// side, the node's stack polled without its accept pass: the final
    /// ACK is then in the node's inbox, unread, and the node's socket for
    /// it still in its handshake.
    fn handshake_to_final_ack(net: &mut Net, client: SocketHandle) {
        net.poll_client();
        net.node
            .poll(net.now, &mut net.node_device, &mut net.node_sockets);
        net.poll_client();
        assert_eq!(net.client_state(client), State::Established);
    }

    /// Why a port needs a backlog, pinned against the stack: with one
    /// listening socket per port, one client's final ACK and the next
    /// client's SYN in the same poll refuse the second client. Two
    /// clients dialing in turn do exactly this.
    #[test]
    fn one_listener_refuses_a_syn_behind_a_handshake_in_the_same_poll() {
        let mut net = Net::new(1, usize::MAX);
        let first = net.dial(PORT);
        handshake_to_final_ack(&mut net, first);
        let second = net.dial(PORT);
        net.poll_client();
        net.poll_node();
        net.poll_client();

        assert_eq!(net.accepted.len(), 1);
        assert_eq!(net.client_state(second), State::Closed, "refused by an RST");
    }

    /// The same two clients on a port with a backlog: both accepted.
    #[test]
    fn a_syn_behind_a_handshake_in_the_same_poll_is_accepted() {
        let mut net = Net::new(listen_backlog(BUF, BUF), usize::MAX);
        let first = net.dial(PORT);
        handshake_to_final_ack(&mut net, first);
        let second = net.dial(PORT);
        net.poll_client();
        net.poll_node();
        assert_ne!(net.client_state(second), State::Closed);

        assert!(net.run_until(|n| n.accepted.len() == 2));
        assert_eq!(net.client_state(first), State::Established);
        assert_eq!(net.client_state(second), State::Established);
    }

    /// Clients dialing in turn, each as soon as the last is established on
    /// its side, the way a load generator opens its connections: all
    /// accepted, in order, and the backlog back to its depth after.
    #[test]
    fn clients_dialing_back_to_back_are_all_accepted_in_order() {
        let depth = listen_backlog(BUF, BUF);
        let mut net = Net::new(depth, usize::MAX);
        let clients: Vec<SocketHandle> = (0..4 * depth)
            .map(|_| {
                let client = net.dial(PORT);
                assert!(net.run_until(|n| n.client_state(client) == State::Established));
                client
            })
            .collect();
        assert!(net.run_until(|n| n.accepted.len() == clients.len()));

        let ports: Vec<u16> = net
            .accepted
            .iter()
            .map(|(_, peer, _)| peer.port())
            .collect();
        let dialed: Vec<u16> = clients
            .iter()
            .map(|&c| {
                let socket = net.client_sockets.get_mut::<tcp::Socket>(c);
                socket.local_endpoint().expect("connected").port
            })
            .collect();
        assert_eq!(ports, dialed);
        assert_eq!(net.listeners.pending(PORT), depth);
    }

    /// As many clients as the backlog holds, all their SYNs in one poll:
    /// all accepted. One more is refused, and gets in when it dials again.
    #[test]
    fn a_burst_is_accepted_up_to_the_backlog_and_a_redial_gets_in() {
        let depth = listen_backlog(BUF, BUF);
        let mut net = Net::new(depth, usize::MAX);
        let clients: Vec<SocketHandle> = (0..=depth).map(|_| net.dial(PORT)).collect();
        net.poll_client();
        net.poll_node();
        net.poll_client();

        let refused: Vec<bool> = clients
            .iter()
            .map(|&c| net.client_state(c) == State::Closed)
            .collect();
        let mut expected = vec![false; depth];
        expected.push(true);
        assert_eq!(refused, expected);
        assert!(net.run_until(|n| n.accepted.len() == depth));

        net.connect(PORT);
        assert_eq!(net.accepted.len(), depth + 1);
    }

    /// An accepted connection carries its port and the port's settings,
    /// and its socket has the port's egress burst; two ports keep their
    /// backlogs apart.
    #[test]
    fn each_port_hands_out_its_own_connections_with_its_settings() {
        let mut net = Net::new(listen_backlog(BUF, BUF), usize::MAX);
        let other = ListenerConfig {
            dispatch_burst_limit: 256,
            ..config(OTHER_PORT)
        };
        net.listeners.add(&mut net.node_sockets, other).unwrap();

        net.connect(OTHER_PORT);
        net.connect(PORT);

        let [(on_other, peer, got_other), (_, _, got)] = net.accepted[..] else {
            panic!("two connections accepted");
        };
        assert_eq!(got_other, other);
        assert_eq!(got, config(PORT));
        assert_eq!(peer.ip(), std::net::IpAddr::V4(ip(1)));
        let socket = net.node_sockets.get_mut::<tcp::Socket>(on_other);
        assert_eq!(socket.state(), State::Established);
        assert_eq!(socket.local_endpoint().map(|e| e.port), Some(OTHER_PORT));
        assert_eq!(socket.timeout(), None, "the handshake timeout is lifted");
        assert_eq!(net.listeners.pending(OTHER_PORT), listen_backlog(BUF, BUF));
    }

    /// The states of `port`'s pending sockets, oldest first.
    fn pending_states(net: &mut Net, port: u16) -> Vec<State> {
        let Some(backlog) = net.listeners.ports.iter().find(|p| p.config.port == port) else {
            return Vec::new();
        };
        backlog
            .pending
            .iter()
            .map(|&h| net.node_sockets.get_mut::<tcp::Socket>(h).state())
            .collect()
    }

    /// Peers that send their SYN and go away without a word fill the
    /// backlog with handshakes that never complete; each times out and
    /// is replaced by a listening socket, and the port takes a client
    /// again.
    #[test]
    fn handshakes_a_vanished_peer_never_completes_time_out_and_are_replaced() {
        let depth = listen_backlog(BUF, BUF);
        let mut net = Net::new(depth, usize::MAX);
        let ghosts: Vec<SocketHandle> = (0..depth).map(|_| net.dial(PORT)).collect();
        net.poll_client();
        // The SYNs are in the node's inbox; its SYN-ACKs are lost, and the
        // peers are gone, so nothing will answer them.
        net.wire.borrow_mut().cut = true;
        for ghost in ghosts {
            net.client_sockets.remove(ghost);
        }
        net.poll_node();
        assert_eq!(
            pending_states(&mut net, PORT),
            vec![State::SynReceived; depth]
        );

        // Short of the timeout every socket is still waiting: the port is
        // deaf.
        let start = net.now;
        while net.now < start + HANDSHAKE_TIMEOUT - Duration::from_millis(1) {
            net.now += Duration::from_millis(1);
            net.poll_node();
        }
        assert_eq!(
            pending_states(&mut net, PORT),
            vec![State::SynReceived; depth]
        );

        net.now += Duration::from_millis(2);
        net.poll_node();
        assert_eq!(pending_states(&mut net, PORT), vec![State::Listen; depth]);
        assert!(net.accepted.is_empty());

        net.wire.borrow_mut().cut = false;
        net.connect(PORT);
    }

    /// A full socket set: the backlog shrinks rather than placing a socket
    /// beyond the cap, a SYN with no listener left is refused (not
    /// accepted on a handle the transport cannot index), and the backlog
    /// fills again once a connection closes.
    #[test]
    fn a_full_socket_set_shrinks_the_backlog_until_a_slot_is_freed() {
        let depth = 2;
        // Room for the backlog and one connection.
        let slot_cap = depth + 1;
        let mut net = Net::new(depth, slot_cap);
        net.connect(PORT);
        assert_eq!(net.listeners.pending(PORT), depth);
        net.connect(PORT);
        assert_eq!(net.listeners.pending(PORT), depth - 1);
        net.connect(PORT);
        assert_eq!(net.listeners.pending(PORT), 0);
        assert_eq!(net.accepted.len(), slot_cap);
        assert!(
            net.accepted
                .iter()
                .all(|(handle, _, _)| handle.index() < slot_cap)
        );

        let refused = net.dial(PORT);
        net.run_until(|n| n.client_state(refused) == State::Closed);
        assert_eq!(net.client_state(refused), State::Closed);
        assert_eq!(net.accepted.len(), slot_cap);

        // Polls with the set full place nothing.
        net.poll_node();
        assert_eq!(net.listeners.pending(PORT), 0);

        // A slot freed with nothing told: the next poll finds it.
        let (closed, _, _) = net.accepted.remove(0);
        net.node_sockets.remove(closed);
        net.poll_node();
        assert_eq!(net.listeners.pending(PORT), 1);
        net.connect(PORT);
    }

    /// A port added to a set with no slot free below the cap listens once
    /// a slot frees, and takes it over a port that still has a listener.
    #[test]
    fn a_port_added_to_a_full_set_listens_once_a_slot_is_freed() {
        let mut net = Net::new(1, 2);
        net.connect(PORT);
        net.listeners
            .add_with_depth(&mut net.node_sockets, config(OTHER_PORT), 2)
            .unwrap();
        assert_eq!(net.listeners.pending(PORT), 1);
        assert_eq!(net.listeners.pending(OTHER_PORT), 0);

        let refused = net.dial(OTHER_PORT);
        net.run_until(|n| n.client_state(refused) == State::Closed);
        assert_eq!(net.client_state(refused), State::Closed);

        let (on_port, _, _) = net.accepted.remove(0);
        net.node_sockets.remove(on_port);
        net.poll_node();
        assert_eq!(net.listeners.pending(OTHER_PORT), 1);
        net.connect(OTHER_PORT);
        let [(_, _, got)] = net.accepted[..] else {
            panic!("one connection accepted");
        };
        assert_eq!(got.port, OTHER_PORT);
    }

    /// A listening socket taken out of the set behind the backlog's back
    /// is forgotten and replaced.
    #[test]
    fn a_stale_listener_handle_is_replaced() {
        let depth = listen_backlog(BUF, BUF);
        let mut net = Net::new(depth, usize::MAX);
        let stale = net.listeners.ports[0].pending[0];
        net.node_sockets.remove(stale);
        net.poll_node();
        assert_eq!(net.listeners.pending(PORT), depth);
        for &handle in &net.listeners.ports[0].pending {
            let socket = net.node_sockets.get_mut::<tcp::Socket>(handle);
            assert_eq!(socket.state(), State::Listen);
        }
        net.connect(PORT);
    }

    /// A connection the peer resets before it is handed out is dropped,
    /// its socket removed and replaced; one the peer closed (its FIN with
    /// the handshake) is handed out, already closed by the peer.
    #[test]
    fn a_reset_connection_is_dropped_and_a_closed_one_handed_out() {
        let depth = listen_backlog(BUF, BUF);
        let mut net = Net::new(depth, usize::MAX);

        let reset = net.dial(PORT);
        handshake_to_final_ack(&mut net, reset);
        net.client_sockets.get_mut::<tcp::Socket>(reset).abort();
        net.poll_client();
        let sockets_before = net.node_sockets.iter().count();
        net.poll_node();
        assert!(net.accepted.is_empty());
        assert_eq!(net.listeners.pending(PORT), depth);
        assert_eq!(net.node_sockets.iter().count(), sockets_before);

        let closed = net.dial(PORT);
        handshake_to_final_ack(&mut net, closed);
        net.client_sockets.get_mut::<tcp::Socket>(closed).close();
        net.poll_client();
        net.poll_node();
        let [(handle, _, _)] = net.accepted[..] else {
            panic!("the closed connection is handed out");
        };
        let socket = net.node_sockets.get_mut::<tcp::Socket>(handle);
        assert_eq!(socket.state(), State::CloseWait);
        assert!(!socket.may_recv());
    }

    #[test]
    fn listening_on_port_zero_is_refused() {
        let mut net = Net::new(1, usize::MAX);
        let err = net
            .listeners
            .add(&mut net.node_sockets, config(0))
            .unwrap_err();
        assert!(err.contains("TCP listen on port 0 failed"), "{err}");
        assert_eq!(net.listeners.ports.len(), 1);
    }

    /// A port beyond [`MAX_PORTS`] is refused, before any socket of it is
    /// opened: the bound on what the backlogs take holds.
    #[test]
    fn a_port_beyond_the_most_a_transport_listens_on_is_refused() {
        let mut net = Net::new(1, usize::MAX);
        net.listeners
            .add(&mut net.node_sockets, config(OTHER_PORT))
            .expect("a second port");
        let sockets_before = net.node_sockets.iter().count();
        let err = net
            .listeners
            .add(&mut net.node_sockets, config(OTHER_PORT + 1))
            .unwrap_err();
        assert!(err.contains("already listens on 2 ports"), "{err}");
        assert_eq!(net.listeners.ports.len(), MAX_PORTS);
        assert_eq!(net.node_sockets.iter().count(), sockets_before);
    }

    /// Client-sized buffers get the whole backlog; buffers too large for
    /// the budget cut it, never below the floor.
    #[test]
    fn the_backlog_follows_the_buffer_budget_within_its_bounds() {
        assert_eq!(listen_backlog(64 * 1024, 16 * 1024), MAX_LISTEN_BACKLOG);
        assert_eq!(listen_backlog(64 * 1024, 512 * 1024), MIN_LISTEN_BACKLOG);
        let third = LISTEN_BACKLOG_BUFFER_BUDGET / 3;
        assert_eq!(listen_backlog(third, 0), 3);
        assert_eq!(listen_backlog(usize::MAX, usize::MAX), MIN_LISTEN_BACKLOG);
        assert_eq!(listen_backlog(0, 0), MAX_LISTEN_BACKLOG);
    }
}
