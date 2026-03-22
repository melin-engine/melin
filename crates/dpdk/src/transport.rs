//! High-level DPDK transport: combines EAL, port, mempool, and smoltcp
//! into a single poll-driven interface for the trading server.
//!
//! The transport owns the DPDK port and smoltcp interface. The server's
//! DPDK poll thread calls `poll()` in a tight loop to drive all I/O.

use std::collections::HashMap;
use std::net::Ipv4Addr;

use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::tcp::{self, State};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address};

use crate::device::DpdkDevice;
use crate::eal::Eal;
use crate::mempool::Mempool;
use crate::port::Port;

/// Maximum concurrent TCP connections.
const MAX_CONNECTIONS: usize = 1024;

/// TCP listen port for trading connections.
const LISTEN_PORT: u16 = 9876;

/// TCP receive buffer size per connection.
const TCP_RX_BUF_SIZE: usize = 65536;

/// TCP send buffer size per connection.
const TCP_TX_BUF_SIZE: usize = 65536;

/// Configuration for the DPDK transport.
pub struct DpdkConfig {
    pub eal_args: Vec<String>,
    pub port_id: u16,
    pub ip_addr: Ipv4Addr,
    pub prefix_len: u8,
    pub gateway: Option<Ipv4Addr>,
    pub listen_port: u16,
}

impl Default for DpdkConfig {
    fn default() -> Self {
        DpdkConfig {
            eal_args: Vec::new(),
            port_id: 0,
            ip_addr: Ipv4Addr::new(10, 0, 0, 1),
            prefix_len: 24,
            gateway: None,
            listen_port: LISTEN_PORT,
        }
    }
}

/// A new TCP connection accepted by the transport.
pub struct AcceptedConnection {
    pub handle: SocketHandle,
    pub peer: std::net::SocketAddr,
}

/// The DPDK transport. Owns all DPDK and smoltcp state.
///
/// All methods must be called from the DPDK poll thread.
pub struct DpdkTransport {
    _eal: Eal,
    _mempool: Mempool,
    _port: Port,
    device: DpdkDevice,
    iface: Interface,
    sockets: SocketSet<'static>,
    listen_handle: SocketHandle,
    listen_port: u16,
    accepted: Vec<AcceptedConnection>,
    /// Per-connection TX buffers keyed by SocketHandle.
    tx_queues: HashMap<SocketHandle, Vec<u8>>,
}

impl DpdkTransport {
    /// Initialize the DPDK transport.
    pub fn init(config: &DpdkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let eal_args: Vec<&str> = config.eal_args.iter().map(|s| s.as_str()).collect();
        let eal = Eal::init(&eal_args)?;

        let port_count = eal.port_count();
        if config.port_id >= port_count {
            return Err(format!(
                "DPDK port {} not found (available: {})",
                config.port_id, port_count
            )
            .into());
        }

        let mempool = Mempool::create("pktmbuf_pool", 0)?;
        let mut port = Port::configure(config.port_id, &mempool)?;
        port.start()?;

        let mac = port.mac_addr();
        let device = DpdkDevice::new(config.port_id, mempool.as_raw());

        let hw_addr = HardwareAddress::Ethernet(EthernetAddress(mac));
        let iface_config = Config::new(hw_addr);
        let now = Instant::from_millis(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        );
        let mut iface = Interface::new(iface_config, &mut DpdkDeviceRef(&device), now);

        let ip = Ipv4Address::new(
            config.ip_addr.octets()[0],
            config.ip_addr.octets()[1],
            config.ip_addr.octets()[2],
            config.ip_addr.octets()[3],
        );
        iface.update_ip_addrs(|addrs| {
            addrs
                .push(IpCidr::new(IpAddress::Ipv4(ip), config.prefix_len))
                .expect("IP address capacity");
        });

        if let Some(gw) = config.gateway {
            let gw_addr = Ipv4Address::new(
                gw.octets()[0],
                gw.octets()[1],
                gw.octets()[2],
                gw.octets()[3],
            );
            iface
                .routes_mut()
                .add_default_ipv4_route(gw_addr)
                .expect("default route capacity");
        }

        let mut sockets = SocketSet::new(Vec::with_capacity(MAX_CONNECTIONS));

        let listen_socket = {
            let rx_buf = tcp::SocketBuffer::new(vec![0u8; 1024]);
            let tx_buf = tcp::SocketBuffer::new(vec![0u8; 1024]);
            let mut socket = tcp::Socket::new(rx_buf, tx_buf);
            socket
                .listen(config.listen_port)
                .map_err(|e| format!("TCP listen failed: {e}"))?;
            socket
        };
        let listen_handle = sockets.add(listen_socket);

        tracing::info!(
            ip = %config.ip_addr,
            port = config.listen_port,
            mac = ?mac,
            "DPDK transport initialized"
        );

        Ok(DpdkTransport {
            _eal: eal,
            _mempool: mempool,
            _port: port,
            device,
            iface,
            sockets,
            listen_handle,
            listen_port: config.listen_port,
            accepted: Vec::new(),
            tx_queues: HashMap::new(),
        })
    }

    /// Run one poll iteration.
    pub fn poll(&mut self) -> Instant {
        let timestamp = Instant::from_millis(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        );

        self.device.poll_rx();
        self.iface
            .poll(timestamp, &mut self.device, &mut self.sockets);
        self.check_listener();
        self.flush_tx_queues();

        timestamp
    }

    fn check_listener(&mut self) {
        let socket = self.sockets.get_mut::<tcp::Socket>(self.listen_handle);
        if socket.state() == State::Established {
            let peer = if let Some(remote) = socket.remote_endpoint() {
                match remote.addr {
                    IpAddress::Ipv4(ip) => {
                        let octets = ip.octets();
                        std::net::SocketAddr::new(
                            std::net::IpAddr::V4(Ipv4Addr::new(
                                octets[0], octets[1], octets[2], octets[3],
                            )),
                            remote.port,
                        )
                    }
                    _ => return,
                }
            } else {
                return;
            };

            let accepted_handle = self.listen_handle;

            let new_listener = {
                let rx_buf = tcp::SocketBuffer::new(vec![0u8; 1024]);
                let tx_buf = tcp::SocketBuffer::new(vec![0u8; 1024]);
                let mut socket = tcp::Socket::new(rx_buf, tx_buf);
                socket
                    .listen(self.listen_port)
                    .expect("re-listen after accept");
                socket
            };
            self.listen_handle = self.sockets.add(new_listener);

            self.accepted.push(AcceptedConnection {
                handle: accepted_handle,
                peer,
            });

            tracing::debug!(peer = %peer, "DPDK: TCP connection accepted");
        }
    }

    fn flush_tx_queues(&mut self) {
        let handles: Vec<SocketHandle> = self.tx_queues.keys().copied().collect();

        for handle in handles {
            let queue = match self.tx_queues.get_mut(&handle) {
                Some(q) if !q.is_empty() => q,
                _ => continue,
            };

            let socket = self.sockets.get_mut::<tcp::Socket>(handle);

            if !socket.can_send() {
                continue;
            }

            let sent = socket.send_slice(queue).unwrap_or(0);
            if sent > 0 {
                queue.drain(..sent);
            }
        }
    }

    /// Take all newly accepted connections.
    pub fn take_accepted(&mut self) -> Vec<AcceptedConnection> {
        std::mem::take(&mut self.accepted)
    }

    /// Read available data from a connection.
    pub fn recv(&mut self, handle: SocketHandle, buf: &mut [u8]) -> usize {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        if !socket.can_recv() {
            return 0;
        }
        socket.recv_slice(buf).unwrap_or(0)
    }

    /// Queue data to be sent on a connection.
    pub fn queue_send(&mut self, handle: SocketHandle, data: &[u8]) {
        self.tx_queues
            .entry(handle)
            .or_insert_with(Vec::new)
            .extend_from_slice(data);
    }

    /// Check if a connection is still open.
    pub fn is_active(&mut self, handle: SocketHandle) -> bool {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        socket.is_active()
    }

    /// Close a connection.
    pub fn close(&mut self, handle: SocketHandle) {
        let socket = self.sockets.get_mut::<tcp::Socket>(handle);
        socket.close();
        self.tx_queues.remove(&handle);
    }
}

/// Temporary wrapper for Interface::new capability probing.
struct DpdkDeviceRef<'a>(&'a DpdkDevice);

impl<'a> smoltcp::phy::Device for DpdkDeviceRef<'a> {
    type RxToken<'b>
        = crate::device::DpdkRxToken
    where
        Self: 'b;
    type TxToken<'b>
        = crate::device::DpdkTxToken
    where
        Self: 'b;

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        None
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        None
    }

    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        self.0.capabilities()
    }
}
