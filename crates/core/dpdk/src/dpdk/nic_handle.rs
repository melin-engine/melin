//! A client's NIC: the node transport constructed on no port, dialing
//! TCP streams to a node and servicing all of them in one device poll.
//!
//! [`NicHandle`] outlives any one connection: EAL initializes once per
//! process and never again after cleanup, so a client that reconnects, or
//! a process that hosts nodes and a client, keeps the NIC up and dials
//! again on it. Its streams share its socket set, interface and poll
//! thread, which is the only way several connections can share one DPDK
//! port: a port is configured and started by one owner in a process, and
//! the return traffic of a connection lands on whatever queue its
//! four-tuple hashes to, not on one that could be given to the socket.
//!
//! Everything public is a std type (addresses, byte slices,
//! [`io::Error`], a MAC as six bytes), so a client needs no smoltcp of
//! its own. The logic that needs no NIC (the clock, the local ports, the
//! dial's wait, a stream's send and receive) is in `crate::nic`, tested on
//! every host.

use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};

use smoltcp::iface::SocketHandle;
use smoltcp::time::Instant;

use crate::config::{DpdkConfig, NicConfig};
use crate::eal::Eal;
use crate::mac::{MacAddr, PeerMacSource, fmt_mac, resolve_peer_mac};
use crate::nic::{
    self, DialProgress, LocalPorts, StackTime, dial_error, dial_progress, wait_for_dial,
};
use crate::transport::{DpdkShared, DpdkTransport, MAX_CONNECTIONS, NodeEal};

pub use crate::nic::{NextHop, NicClock};

/// Tells handles apart, so that a [`StreamId`] is only honoured by the
/// handle that dialed it. `u64`: one per handle opened in the process,
/// never exhausted.
static NEXT_HANDLE_TAG: AtomicU64 = AtomicU64::new(0);

/// A client's DPDK NIC: one TCP/IP stack on one port (or an LACP bond's
/// ports), owning the socket set its streams live in.
///
/// Built on the node's transport, listening on no port, so its streams
/// carry the node's socket tuning and its neighbour seeding works in both
/// of the node's modes:
///
/// - **L3 mode** (`gateway` and `gateway_mac` both set): the gateway is
///   seeded from the supplied MAC (and re-seeded before its neighbour
///   entry expires), and nothing is announced: the port is shared with
///   the kernel, whose own address the fabric already knows.
/// - **Otherwise**: the handle announces itself with a gratuitous ARP when
///   it opens, so the switch learns its MAC, and each dial seeds the server
///   from `peer_mac` or, failing that, the SR-IOV `02:00:<ip>` convention
///   ([`crate::resolve_peer_mac`]).
///
/// # One dial at a time
///
/// [`Self::dial`] returns once the connection is established or has
/// failed, so the dials of one handle are sequential. That is the node's
/// constraint, not the client's: a DPDK node keeps one listening socket
/// per port and replaces it only once the connection it accepted is
/// established, and its stack answers a SYN that matches no listener with
/// an RST. A second dial in flight during another's handshake would
/// therefore be refused outright, where a kernel node would queue it in
/// its backlog. The constraint lifts when the node keeps a backlog of
/// listeners.
///
/// # Streams
///
/// A dial hands back a [`StreamId`]; [`Self::stream`] lends the stream it
/// names as a [`NicStream`] view, for one call or a few, borrowing the
/// handle meanwhile. The handle's service step ([`Self::service`], also
/// offered on the view) advances every stream at once: call it once per
/// loop iteration, not once per stream.
///
/// A stream is reset when it is closed ([`Self::close`]) or the handle is
/// dropped, whatever its state, so the node frees the connection's slot at
/// once rather than at its next heartbeat. The socket is aborted, one
/// egress pass sends its RST, the device is flushed, and only then is the
/// socket removed from the set: removed first, it would take its RST with
/// it. Best effort, as on the node: a frame the device cannot take at that
/// moment is not retried, and the node's heartbeat covers that case.
///
/// # Time
///
/// The dial and the service step take the caller's clock: a monotonic
/// nanosecond count whose origin is fixed for the life of the handle,
/// since the interface and its timers outlive any one connection. A caller
/// with a clock of its own (a calibrated TSC) passes its readings; one
/// without reads [`Self::clock`]. A reading that goes backwards is clamped:
/// the stack's time stays where it was until the readings pass it again.
pub struct NicHandle {
    transport: DpdkTransport,
    /// The streams dialed and not yet closed. A `Vec`: a handle carries a
    /// handful (a client one, a bench a few dozen), scanned whole only on
    /// a dial, a close and the handle's drop, never on the data path.
    streams: Vec<OpenStream>,
    time: StackTime,
    clock: NicClock,
    ports: LocalPorts,
    /// L3 mode's gateway: the next hop of a dial off the NIC's network. A
    /// tuple for the same reason as the transport's: the IP is nothing
    /// without its MAC.
    gateway: Option<(Ipv4Addr, MacAddr)>,
    /// The server's MAC, when supplied; derived per dial otherwise.
    peer_mac: Option<MacAddr>,
    /// The NIC's address and the width of its network, which decide
    /// whether a dial goes through the gateway; and its first port, for
    /// the connect line.
    ip_addr: Ipv4Addr,
    prefix_len: u8,
    port: u16,
    /// This handle's tag (see [`NEXT_HANDLE_TAG`]).
    tag: u64,
}

/// A dialed stream's socket, and the local port it holds.
struct OpenStream {
    socket: SocketHandle,
    local_port: u16,
}

/// Names a stream dialed by a [`NicHandle`], for [`NicHandle::stream`] and
/// [`NicHandle::close`].
///
/// Neither `Clone` nor `Copy`: closing a stream consumes its id, so no id
/// outlives the socket it names, whose slot in the set a later dial may
/// take.
#[derive(Debug)]
pub struct StreamId {
    socket: SocketHandle,
    /// The tag of the handle that dialed it.
    handle_tag: u64,
    peer: SocketAddrV4,
    local_port: u16,
}

impl StreamId {
    /// The server this stream is connected to.
    pub fn peer(&self) -> SocketAddrV4 {
        self.peer
    }

    /// The local port this stream was dialed from.
    pub fn local_port(&self) -> u16 {
        self.local_port
    }
}

impl NicHandle {
    /// Bring the NIC up from `config`.
    ///
    /// EAL is initialized from `config.eal_args`, with a file prefix of the
    /// client's own (`melin-client`) added when they name none, and cleaned
    /// up when the handle drops. In a process that runs a process-wide EAL
    /// ([`Eal::init_process_wide`]) the handle shares it instead, as a node
    /// does: its EAL arguments must then be empty, its mbuf pool is named
    /// after its first port, and dropping it leaves EAL running.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] for EAL arguments ending in a
    /// `--file-prefix` with no prefix, or no port; otherwise EAL could not
    /// be initialized (the error names the file prefix) or shared, or a
    /// port, the pool or the port's configuration could not be set up.
    pub fn open(config: &NicConfig) -> io::Result<Self> {
        let shared_eal = Eal::process_wide().is_some();
        let mut config = config.clone();
        if !shared_eal {
            config.eal_args = nic::with_client_file_prefix(&config.eal_args)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        }
        let port = config.port_ids.first().copied().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "DPDK transport requires at least one port id",
            )
        })?;
        let eal = NodeEal::acquire(&config).map_err(|e| {
            io::Error::other(nic::eal_init_failed(&config.eal_args, shared_eal, &e))
        })?;
        let shared =
            DpdkShared::init_on(eal, &config, 1).map_err(|e| io::Error::other(e.to_string()))?;
        let gateway = config.gateway.zip(config.gateway_mac);
        let peer_mac = config.peer_mac;
        let ip_addr = config.ip_addr;
        let prefix_len = config.prefix_len;
        let transport_config = DpdkConfig {
            nic: config,
            ..DpdkConfig::default()
        };
        let mut transport = DpdkTransport::from_shared_unlistening(&shared, &transport_config, 0)
            .map_err(|e| io::Error::other(e.to_string()))?;
        if gateway.is_none() {
            transport.send_gratuitous_arp();
        }
        let time = StackTime::new(transport.stack_now().total_micros());
        Ok(Self {
            transport,
            streams: Vec::new(),
            time,
            clock: NicClock::start(),
            ports: LocalPorts::starting_from(std::process::id()),
            gateway,
            peer_mac,
            ip_addr,
            prefix_len,
            port,
            tag: NEXT_HANDLE_TAG.fetch_add(1, Ordering::Relaxed),
        })
    }

    /// The MAC address of the NIC (its first port's).
    pub fn mac(&self) -> [u8; 6] {
        self.transport.mac()
    }

    /// The MAC a dial to `peer` seeds for it, and where it came from: the
    /// configured peer MAC, or the SR-IOV convention. Seeded on every dial
    /// whatever the mode, since a peer on the NIC's own network is reached
    /// directly even behind a gateway; [`Self::next_hop`] says which MAC a
    /// given dial actually goes to.
    pub fn peer_mac(&self, peer: Ipv4Addr) -> ([u8; 6], PeerMacSource) {
        resolve_peer_mac(self.peer_mac, peer)
    }

    /// Where a dial to `peer` sends its first frame: the gateway's MAC when
    /// there is one and the peer is off the NIC's network, the peer's
    /// otherwise, with where it came from. What a binary prints before
    /// dialing ([`Self::connect_line`]), with [`NextHop::warning`] beside
    /// it, so a wrong MAC is on the screen when the dial times out on it.
    pub fn next_hop(&self, peer: Ipv4Addr) -> NextHop {
        nic::next_hop(
            self.gateway,
            self.ip_addr,
            self.prefix_len,
            self.peer_mac,
            peer,
        )
    }

    /// The line a binary prints before dialing `peer`, as the shm-proxy
    /// and the echo client print it: the NIC's first port, its address and
    /// MAC, and the peer with [`Self::next_hop`]. A caller that wants the
    /// derived-MAC warning as well prints [`NextHop::warning`] after it.
    pub fn connect_line(&self, peer: SocketAddrV4) -> String {
        format!(
            "DPDK: port {} as {} ({}), server {peer} {}",
            self.port,
            self.ip_addr,
            fmt_mac(self.mac()),
            self.next_hop(*peer.ip())
        )
    }

    /// A clock for a caller with none of its own: nanoseconds since the
    /// handle was opened. `Copy`, so it can be read inside the closure a
    /// dial is given.
    pub fn clock(&self) -> NicClock {
        self.clock
    }

    /// Dial `peer` and wait for the connection to be established.
    ///
    /// The socket has `socket_buffer` bytes of receive buffer and as many
    /// of transmit buffer (the transmit buffer is the room
    /// [`NicStream::send`] and [`NicStream::send_in_place`] have), the
    /// node's TCP tuning, and a local port of its own: each dial takes the
    /// next one along the ephemeral range, so a reconnect never reuses the
    /// four-tuple of a connection the node may still hold (a DPDK node does
    /// not see a client's close until its next heartbeat).
    ///
    /// `now_ns` is the caller's clock (see the type's documentation on
    /// time), read on every iteration of the wait, each reading driving one
    /// poll of the stack: the handshake runs on retransmission timers.
    /// `deadline_ns` is on the same clock. One dial at a time (see the
    /// type's documentation): the call returns only once the connection is
    /// established or has failed, and the other streams are serviced
    /// meanwhile.
    ///
    /// # Errors
    ///
    /// - [`io::ErrorKind::TimedOut`]: no SYN-ACK by the deadline. The error
    ///   names the MAC the SYN was addressed to as the first thing to
    ///   check: a wrong one is silent.
    /// - [`io::ErrorKind::ConnectionRefused`]: the peer answered with a
    ///   reset (nothing listens there, or a DPDK node was in another
    ///   client's handshake).
    /// - [`io::ErrorKind::InvalidInput`]: a zero `socket_buffer`, or an
    ///   address the stack cannot dial.
    /// - [`io::ErrorKind::AddrInUse`]: the handle holds as many streams as
    ///   it can, or every ephemeral port.
    ///
    /// A failed dial's socket is reset and removed, like a closed stream's.
    pub fn dial(
        &mut self,
        peer: SocketAddrV4,
        socket_buffer: usize,
        deadline_ns: u64,
        mut now_ns: impl FnMut() -> u64,
    ) -> io::Result<StreamId> {
        if socket_buffer == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a stream needs a non-empty socket buffer",
            ));
        }
        // The transport's per-connection tables are sized for this many
        // sockets, and a handle's set holds nothing but its streams.
        if self.streams.len() >= MAX_CONNECTIONS {
            return Err(io::Error::new(
                io::ErrorKind::AddrInUse,
                format!("this NIC handle already holds {MAX_CONNECTIONS} streams"),
            ));
        }
        let streams = &self.streams;
        let local_port = self
            .ports
            .take(|port| streams.iter().any(|s| s.local_port == port))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "every ephemeral port is held by a stream of this NIC handle",
                )
            })?;

        // The peer's MAC is seeded whatever the mode, as a node seeds its
        // primary's: behind a gateway a peer on the NIC's own network is
        // still reached directly, and an entry for one off it goes unused.
        // Re-seeded on every dial, so an expired neighbour entry cannot
        // strand a reconnect. (The gateway is seeded by the transport on
        // its own interval.)
        let (peer_mac, _) = self.peer_mac(*peer.ip());
        self.transport.seed_neighbor(*peer.ip(), peer_mac);
        let next_hop = self.next_hop(*peer.ip());
        // Drain the injected ARP reply through the stack before the SYN is
        // built: from an empty cache the stack would ARP for the server, on
        // a port where the reply never arrives, and the SYN would wait on it.
        self.service(now_ns());

        let socket = self
            .transport
            .connect_copying(peer, local_port, socket_buffer)
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("connect to {peer}: {e:?}"),
                )
            })?;
        let outcome = wait_for_dial(deadline_ns, &mut now_ns, |now| {
            self.transport
                .poll_copying(Instant::from_micros(self.time.at(now)));
            self.transport
                .tcp_socket(socket)
                .map_or(DialProgress::Refused, |s| dial_progress(s.state()))
        });
        match outcome {
            Ok(()) => {
                self.streams.push(OpenStream { socket, local_port });
                Ok(StreamId {
                    socket,
                    handle_tag: self.tag,
                    peer,
                    local_port,
                })
            }
            Err(failure) => {
                // A SYN-ACK that arrives after the deadline would otherwise
                // leave the node holding a connection nothing reads.
                self.transport.reset(socket);
                Err(dial_error(peer, failure, next_hop))
            }
        }
    }

    /// Lend the stream `id` names. A view of a stream another handle
    /// dialed reports every operation as [`io::ErrorKind::NotFound`].
    pub fn stream(&mut self, id: &StreamId) -> NicStream<'_> {
        let socket = (id.handle_tag == self.tag).then_some(id.socket);
        NicStream { nic: self, socket }
    }

    /// The service step: poll the device, run the stack over every stream,
    /// and flush the transmit queue, at the caller's time `now_ns` (see
    /// the type's documentation on time).
    pub fn service(&mut self, now_ns: u64) {
        let now = Instant::from_micros(self.time.at(now_ns));
        self.transport.poll_copying(now);
    }

    /// Close a stream: reset it (see the type's documentation) and free
    /// its socket. An id another handle dialed is ignored.
    pub fn close(&mut self, id: StreamId) {
        if id.handle_tag != self.tag {
            tracing::warn!(peer = %id.peer, "DPDK: close of a stream another NIC handle dialed");
            return;
        }
        self.streams.retain(|s| s.socket != id.socket);
        self.transport.reset(id.socket);
    }
}

impl Drop for NicHandle {
    /// Reset every stream still open, before the transport (and with it
    /// the device, the pool and an owned EAL) goes.
    fn drop(&mut self) {
        for stream in std::mem::take(&mut self.streams) {
            self.transport.reset(stream.socket);
        }
    }
}

/// One stream of a [`NicHandle`], lent by [`NicHandle::stream`] for as
/// long as the caller holds the view. Dropping the view does nothing to
/// the stream; closing it is [`NicHandle::close`].
pub struct NicStream<'a> {
    nic: &'a mut NicHandle,
    /// `None` for an id another handle dialed.
    socket: Option<SocketHandle>,
}

impl NicStream<'_> {
    fn socket(&mut self) -> io::Result<&mut smoltcp::socket::tcp::Socket<'static>> {
        let socket = match self.socket {
            Some(socket) => self.nic.transport.tcp_socket(socket),
            None => None,
        };
        socket.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "the stream id was dialed by another NIC handle",
            )
        })
    }

    /// Whether a [`Self::send`] would queue anything now: the connection
    /// is open and the transmit buffer has room. A caller that builds a
    /// request only when it can go out asks this first.
    pub fn can_send(&mut self) -> io::Result<bool> {
        Ok(self.socket()?.can_send())
    }

    /// Queue as much of `data` as the socket's transmit buffer has room
    /// for, without blocking. `Ok(0)` means none of it: try again after a
    /// service step.
    pub fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        nic::send(self.socket()?, data)
    }

    /// Lend `fill` the contiguous free region of the socket's transmit
    /// buffer to write into, and queue the bytes it returns having written
    /// there, from the region's start: a request is encoded where the
    /// stack sends it from.
    ///
    /// The region may be shorter than the buffer's free space, when that
    /// space wraps round the end of the ring; a caller with more to write
    /// than the region holds writes nothing and sends through
    /// [`Self::send`] instead. `fill` is not called when the buffer has no
    /// room, and `Ok(0)` is returned.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::InvalidInput`] if `fill` returns more than the
    /// region's length; nothing is queued then.
    pub fn send_in_place(&mut self, fill: impl FnOnce(&mut [u8]) -> usize) -> io::Result<usize> {
        nic::send_in_place(self.socket()?, fill)
    }

    /// Read what has arrived into `buf`. `Ok(0)` means nothing yet. A
    /// connection the server has closed is [`io::ErrorKind::UnexpectedEof`],
    /// never a zero, once the bytes it sent before closing have been read.
    pub fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        nic::recv(self.socket()?, buf)
    }

    /// The handle's service step ([`NicHandle::service`]), offered here so a
    /// single-stream caller never names the handle in its loop. It
    /// advances every stream of the handle, not only this one.
    pub fn service(&mut self, now_ns: u64) {
        self.nic.service(now_ns);
    }
}
