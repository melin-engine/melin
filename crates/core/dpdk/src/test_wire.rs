//! An in-memory Ethernet wire between two smoltcp interfaces, for the
//! tests of the crate's NIC-free TCP logic (`peer_liveness`, `nic`).
//!
//! Frames go straight into the other end's inbox when the stack hands
//! them to the device, so a test sees on the wire exactly what an egress
//! pass produced, and nothing is batched behind a flush.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

use smoltcp::iface::{Config, Interface};
use smoltcp::phy::{Device, DeviceCapabilities, Medium};
use smoltcp::time::Instant;
use smoltcp::wire::{EthernetAddress, HardwareAddress, IpAddress, IpCidr, Ipv4Address};

/// Two nodes' frames in flight. `VecDeque`: frames are delivered in the
/// order they were sent, from the front.
#[derive(Default)]
pub(crate) struct Wire {
    pub(crate) inbox: [VecDeque<Vec<u8>>; 2],
    /// Frames sent while set are lost: a cut link.
    pub(crate) cut: bool,
}

/// One node's end of the wire.
pub(crate) struct End {
    wire: Rc<RefCell<Wire>>,
    side: usize,
}

pub(crate) struct Rx(Vec<u8>);

pub(crate) struct Tx {
    wire: Rc<RefCell<Wire>>,
    to: usize,
}

impl smoltcp::phy::RxToken for Rx {
    fn consume<R, F: FnOnce(&[u8]) -> R>(self, f: F) -> R {
        f(&self.0)
    }
}

impl smoltcp::phy::TxToken for Tx {
    fn consume<R, F: FnOnce(&mut [u8]) -> R>(self, len: usize, f: F) -> R {
        let mut frame = vec![0u8; len];
        let r = f(&mut frame);
        let mut wire = self.wire.borrow_mut();
        if !wire.cut {
            wire.inbox[self.to].push_back(frame);
        }
        r
    }
}

impl Device for End {
    type RxToken<'a> = Rx;
    type TxToken<'a> = Tx;

    fn receive(&mut self, _: Instant) -> Option<(Rx, Tx)> {
        let frame = self.wire.borrow_mut().inbox[self.side].pop_front()?;
        Some((Rx(frame), self.tx()))
    }

    fn transmit(&mut self, _: Instant) -> Option<Tx> {
        Some(self.tx())
    }

    fn capabilities(&self) -> DeviceCapabilities {
        let mut caps = DeviceCapabilities::default();
        caps.medium = Medium::Ethernet;
        caps.max_transmission_unit = 1514;
        caps
    }
}

impl End {
    pub(crate) fn new(wire: &Rc<RefCell<Wire>>, side: usize) -> Self {
        Self {
            wire: Rc::clone(wire),
            side,
        }
    }

    fn tx(&self) -> Tx {
        Tx {
            wire: Rc::clone(&self.wire),
            to: 1 - self.side,
        }
    }
}

/// The IPv4 address of the node on `side`: 10.0.0.1 or 10.0.0.2, one
/// subnet, so neither needs a route.
pub(crate) fn ip(side: usize) -> Ipv4Address {
    Ipv4Address::new(10, 0, 0, side as u8 + 1)
}

/// An interface for the node on `side`, addressed as [`ip`] says.
pub(crate) fn interface(device: &mut End, side: usize, now: Instant) -> Interface {
    let mac = EthernetAddress([0x02, 0, 0, 0, 0, side as u8 + 1]);
    let mut iface = Interface::new(Config::new(HardwareAddress::Ethernet(mac)), device, now);
    iface.update_ip_addrs(|addrs| {
        addrs
            .push(IpCidr::new(IpAddress::Ipv4(ip(side)), 24))
            .unwrap();
    });
    iface
}
