//! The two ARP frames the DPDK transport sends and injects.
//!
//! A userspace stack on a NIC the kernel does not own resolves nothing by
//! itself at startup, so the client tells the switch where it is with a
//! gratuitous request, and tells its own stack where the server is with a
//! reply it crafts and feeds back in. Ethernet header plus a 28-byte ARP
//! body, fixed layout, no library.

/// Ethernet header (14) plus the IPv4-over-Ethernet ARP body (28).
pub const FRAME_LEN: usize = 42;

const ETHERTYPE_ARP: [u8; 2] = [0x08, 0x06];
const HARDWARE_ETHERNET: [u8; 2] = [0x00, 0x01];
const PROTOCOL_IPV4: [u8; 2] = [0x08, 0x00];
const OP_REQUEST: [u8; 2] = [0x00, 0x01];
const OP_REPLY: [u8; 2] = [0x00, 0x02];

/// "Who has `our_ip`? Tell `our_ip`", broadcast: announces `our_mac` to
/// every switch and neighbour on the segment.
pub fn gratuitous_request(our_mac: [u8; 6], our_ip: [u8; 4]) -> [u8; FRAME_LEN] {
    frame(
        [0xFF; 6], our_mac, OP_REQUEST, our_mac, our_ip, [0xFF; 6], our_ip,
    )
}

/// A reply from `from_ip` at `from_mac` addressed to us, for injection
/// into our own stack's receive path so its neighbour cache learns the
/// peer without a round trip.
pub fn reply(
    to_mac: [u8; 6],
    to_ip: [u8; 4],
    from_mac: [u8; 6],
    from_ip: [u8; 4],
) -> [u8; FRAME_LEN] {
    frame(to_mac, from_mac, OP_REPLY, from_mac, from_ip, to_mac, to_ip)
}

fn frame(
    dst_mac: [u8; 6],
    src_mac: [u8; 6],
    op: [u8; 2],
    sender_mac: [u8; 6],
    sender_ip: [u8; 4],
    target_mac: [u8; 6],
    target_ip: [u8; 4],
) -> [u8; FRAME_LEN] {
    let mut f = [0u8; FRAME_LEN];
    f[0..6].copy_from_slice(&dst_mac);
    f[6..12].copy_from_slice(&src_mac);
    f[12..14].copy_from_slice(&ETHERTYPE_ARP);
    f[14..16].copy_from_slice(&HARDWARE_ETHERNET);
    f[16..18].copy_from_slice(&PROTOCOL_IPV4);
    f[18] = 6;
    f[19] = 4;
    f[20..22].copy_from_slice(&op);
    f[22..28].copy_from_slice(&sender_mac);
    f[28..32].copy_from_slice(&sender_ip);
    f[32..38].copy_from_slice(&target_mac);
    f[38..42].copy_from_slice(&target_ip);
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    const US: [u8; 6] = [0x06, 1, 2, 3, 4, 5];
    const OUR_IP: [u8; 4] = [10, 0, 1, 40];
    const PEER: [u8; 6] = [0x06, 9, 8, 7, 6, 5];
    const PEER_IP: [u8; 4] = [10, 0, 1, 30];

    #[test]
    fn the_gratuitous_request_is_broadcast_and_asks_about_itself() {
        let f = gratuitous_request(US, OUR_IP);
        assert_eq!(&f[0..6], &[0xFF; 6], "broadcast destination");
        assert_eq!(&f[6..12], &US);
        assert_eq!(&f[12..14], &ETHERTYPE_ARP);
        assert_eq!(&f[20..22], &OP_REQUEST);
        assert_eq!(&f[22..28], &US, "sender MAC");
        assert_eq!(&f[28..32], &OUR_IP, "sender IP");
        assert_eq!(&f[38..42], &OUR_IP, "target IP is our own");
    }

    #[test]
    fn the_injected_reply_names_the_peer_as_sender_and_us_as_target() {
        let f = reply(US, OUR_IP, PEER, PEER_IP);
        assert_eq!(&f[0..6], &US, "delivered to us");
        assert_eq!(&f[6..12], &PEER, "from the peer");
        assert_eq!(&f[20..22], &OP_REPLY);
        assert_eq!(&f[22..28], &PEER, "sender MAC: the peer");
        assert_eq!(&f[28..32], &PEER_IP, "sender IP: the peer");
        assert_eq!(&f[32..38], &US, "target MAC: us");
        assert_eq!(&f[38..42], &OUR_IP, "target IP: us");
        assert_eq!(f[18], 6, "hardware address length");
        assert_eq!(f[19], 4, "protocol address length");
    }
}
