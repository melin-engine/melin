//! The clients' streams on one DPDK NIC: the handle, the streams it
//! lends, and its clock. Compiled only with the `dpdk` feature.

use std::net::SocketAddr;

use melin_client::{DEFAULT_TIMEOUT, NicClock, NicHandle, NicStream, StreamId, dial_addr};
use melin_dpdk::DpdkArgs;

use crate::Error;
use crate::load::{Streams, nanos};

/// Each stream's receive and transmit buffer: room for a window of
/// requests ahead of the wire, and a burst of replies behind it.
const SOCKET_BUFFER: usize = 256 * 1024;

pub struct Nic {
    nic: NicHandle,
    clock: NicClock,
    // A `Vec` indexed by client, as the kernel's sockets are.
    streams: Vec<StreamId>,
}

impl Nic {
    /// Bring the NIC up from `args` and dial `clients` streams to `addr`,
    /// one at a time, as a DPDK node takes them.
    pub fn connect(args: &DpdkArgs, addr: SocketAddr, clients: usize) -> Result<Self, Error> {
        let addr = dial_addr(addr).map_err(|why| format!("--addr {addr}: {why}"))?;
        let config = args.nic_config()?;
        let mut nic = NicHandle::open(&config).map_err(melin_client::Error::Nic)?;
        eprintln!("{}", nic.connect_line(addr));
        if let Some(warning) = nic.next_hop(*addr.ip()).warning() {
            eprintln!("warning: {warning}");
        }
        let clock = nic.clock();
        let mut streams = Vec::with_capacity(clients);
        for client in 0..clients {
            let deadline_ns = clock.now_ns() + nanos(DEFAULT_TIMEOUT);
            let id = nic
                .dial(addr, SOCKET_BUFFER, deadline_ns, || clock.now_ns())
                .map_err(|e| format!("client {client}: cannot connect to {addr}: {e}"))?;
            streams.push(id);
        }
        Ok(Self {
            nic,
            clock,
            streams,
        })
    }
}

impl Streams for Nic {
    type Stream<'a> = NicStream<'a>;

    fn now_ns(&self) -> u64 {
        self.clock.now_ns()
    }

    fn service(&mut self, now_ns: u64) {
        self.nic.service(now_ns);
    }

    fn stream(&mut self, client: usize) -> NicStream<'_> {
        self.nic.stream(&self.streams[client])
    }

    // A stream's sends wait in its socket's buffer until the next
    // `service`, which sends every stream's at once.
    fn end_sends(&mut self, _client: usize) -> std::io::Result<()> {
        Ok(())
    }
}
