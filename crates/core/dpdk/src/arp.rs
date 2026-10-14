//! The two ARP frames a DPDK transport sends and injects.
//!
//! A userspace stack on a NIC the kernel does not own resolves nothing by
//! itself at startup: an SR-IOV VF receives no broadcast, and in
//! bifurcated mode the `rte_flow` rule steers no ARP into DPDK. So the
//! transport tells the switch where it is with a gratuitous request, and
//! tells its own stack where a peer is with a reply it crafts and feeds
//! back into its receive path. An Ethernet header and a 28-byte ARP body,
//! fixed layout, no library.
//!
//! Plain functions with no libdpdk dependency, so they are tested on every
//! host, like `mac` and `rx_checksum`.

use std::net::Ipv4Addr;

use crate::mac::MacAddr;

/// Length of either frame: the Ethernet header (14 bytes) and the
/// IPv4-over-Ethernet ARP body (28 bytes).
pub const FRAME_LEN: usize = 42;

/// ARP's EtherType: what these frames carry in their Ethernet header, and
/// what a steering rule matches ARP by (`steering::EtherTypeRule`).
pub const ETHER_TYPE: u16 = 0x0806;

const BROADCAST: MacAddr = [0xff; 6];
const ETHERTYPE_ARP: [u8; 2] = ETHER_TYPE.to_be_bytes();
const HARDWARE_ETHERNET: [u8; 2] = [0x00, 0x01];
const PROTOCOL_IPV4: [u8; 2] = [0x08, 0x00];
const OP_REQUEST: [u8; 2] = [0x00, 0x01];
const OP_REPLY: [u8; 2] = [0x00, 0x02];

/// A gratuitous ARP request: "who has `our_ip`? tell `our_ip`", broadcast
/// from `our_mac`, so the switch learns which port `our_mac` is behind.
///
/// The target hardware address is zeroed, as RFC 5227 writes an ARP
/// announcement. Receivers ignore that field in a request, so a broadcast
/// one would be equally valid; zero is what the node has always sent.
pub fn gratuitous_request(our_mac: MacAddr, our_ip: Ipv4Addr) -> [u8; FRAME_LEN] {
    frame(
        BROADCAST, our_mac, OP_REQUEST, our_mac, our_ip, [0; 6], our_ip,
    )
}

/// An ARP reply from `peer_ip` at `peer_mac`, addressed to us, for
/// injection into our own stack's receive path: its neighbour cache learns
/// the peer without an exchange on the wire.
pub fn reply(
    our_mac: MacAddr,
    our_ip: Ipv4Addr,
    peer_mac: MacAddr,
    peer_ip: Ipv4Addr,
) -> [u8; FRAME_LEN] {
    frame(
        our_mac, peer_mac, OP_REPLY, peer_mac, peer_ip, our_mac, our_ip,
    )
}

/// The one layout both frames share, field by field.
fn frame(
    dst_mac: MacAddr,
    src_mac: MacAddr,
    op: [u8; 2],
    sender_mac: MacAddr,
    sender_ip: Ipv4Addr,
    target_mac: MacAddr,
    target_ip: Ipv4Addr,
) -> [u8; FRAME_LEN] {
    let mut f = [0u8; FRAME_LEN];
    // Ethernet header.
    f[0..6].copy_from_slice(&dst_mac);
    f[6..12].copy_from_slice(&src_mac);
    f[12..14].copy_from_slice(&ETHERTYPE_ARP);
    // ARP body: Ethernet hardware, IPv4 protocol, their address lengths.
    f[14..16].copy_from_slice(&HARDWARE_ETHERNET);
    f[16..18].copy_from_slice(&PROTOCOL_IPV4);
    f[18] = 6;
    f[19] = 4;
    f[20..22].copy_from_slice(&op);
    f[22..28].copy_from_slice(&sender_mac);
    f[28..32].copy_from_slice(&sender_ip.octets());
    f[32..38].copy_from_slice(&target_mac);
    f[38..42].copy_from_slice(&target_ip.octets());
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    const US: MacAddr = [0x06, 0x01, 0x02, 0x03, 0x04, 0x05];
    const OUR_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 1, 40);
    const PEER: MacAddr = [0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80];
    const PEER_IP: Ipv4Addr = Ipv4Addr::new(10, 0, 1, 30);

    /// The whole frame, byte for byte: what `send_gratuitous_arp` built
    /// inline before the builder was hoisted, written out by hand.
    #[test]
    fn the_gratuitous_request_is_the_frame_the_node_has_always_sent() {
        #[rustfmt::skip]
        let expected: [u8; FRAME_LEN] = [
            // Ethernet: broadcast destination, our source, ARP.
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0x06, 0x01, 0x02, 0x03, 0x04, 0x05,
            0x08, 0x06,
            // ARP: Ethernet, IPv4, 6, 4, request.
            0x00, 0x01, 0x08, 0x00, 6, 4, 0x00, 0x01,
            // Sender: us.
            0x06, 0x01, 0x02, 0x03, 0x04, 0x05, 10, 0, 1, 40,
            // Target: a zeroed hardware address, our own IP.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 10, 0, 1, 40,
        ];
        assert_eq!(gratuitous_request(US, OUR_IP), expected);
    }

    /// The whole frame, byte for byte: what `seed_neighbor` built inline
    /// before the builder was hoisted, written out by hand.
    #[test]
    fn the_injected_reply_is_the_frame_the_node_has_always_injected() {
        #[rustfmt::skip]
        let expected: [u8; FRAME_LEN] = [
            // Ethernet: delivered to us, from the peer, ARP.
            0x06, 0x01, 0x02, 0x03, 0x04, 0x05,
            0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80,
            0x08, 0x06,
            // ARP: Ethernet, IPv4, 6, 4, reply.
            0x00, 0x01, 0x08, 0x00, 6, 4, 0x00, 0x02,
            // Sender: the peer.
            0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80, 10, 0, 1, 30,
            // Target: us.
            0x06, 0x01, 0x02, 0x03, 0x04, 0x05, 10, 0, 1, 40,
        ];
        assert_eq!(reply(US, OUR_IP, PEER, PEER_IP), expected);
    }

    /// The four addresses of a reply are not interchangeable: swapping
    /// ours and the peer's would teach the stack that it is its own peer.
    #[test]
    fn a_reply_keeps_the_peer_and_us_apart() {
        let ours = reply(US, OUR_IP, PEER, PEER_IP);
        let swapped = reply(PEER, PEER_IP, US, OUR_IP);
        assert_ne!(ours, swapped);
        assert_eq!(&ours[0..6], &swapped[6..12]);
        assert_eq!(&ours[22..32], &swapped[32..42]);
    }

    /// Both frames parse as what they claim to be, by the stack that
    /// consumes the injected one: an independent reading of the layout.
    #[test]
    fn the_stack_parses_both_frames_as_written() {
        use smoltcp::wire::{
            ArpOperation, ArpPacket, ArpRepr, EthernetAddress, EthernetFrame, EthernetProtocol,
        };

        let request = gratuitous_request(US, OUR_IP);
        let eth = EthernetFrame::new_checked(&request[..]).expect("ethernet frame");
        assert_eq!(eth.dst_addr(), EthernetAddress(BROADCAST));
        assert_eq!(eth.src_addr(), EthernetAddress(US));
        assert_eq!(eth.ethertype(), EthernetProtocol::Arp);
        let arp = ArpRepr::parse(&ArpPacket::new_checked(eth.payload()).expect("arp packet"))
            .expect("arp repr");
        assert_eq!(
            arp,
            ArpRepr::EthernetIpv4 {
                operation: ArpOperation::Request,
                source_hardware_addr: EthernetAddress(US),
                source_protocol_addr: OUR_IP,
                target_hardware_addr: EthernetAddress([0; 6]),
                target_protocol_addr: OUR_IP,
            }
        );

        let reply = reply(US, OUR_IP, PEER, PEER_IP);
        let eth = EthernetFrame::new_checked(&reply[..]).expect("ethernet frame");
        assert_eq!(eth.dst_addr(), EthernetAddress(US));
        assert_eq!(eth.src_addr(), EthernetAddress(PEER));
        assert_eq!(eth.ethertype(), EthernetProtocol::Arp);
        let arp = ArpRepr::parse(&ArpPacket::new_checked(eth.payload()).expect("arp packet"))
            .expect("arp repr");
        assert_eq!(
            arp,
            ArpRepr::EthernetIpv4 {
                operation: ArpOperation::Reply,
                source_hardware_addr: EthernetAddress(PEER),
                source_protocol_addr: PEER_IP,
                target_hardware_addr: EthernetAddress(US),
                target_protocol_addr: OUR_IP,
            }
        );
    }
}
