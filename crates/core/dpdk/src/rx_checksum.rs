//! Receive-side checksum admission for the DPDK device.
//!
//! With receive checksum offload enabled the userspace TCP stack is told
//! not to verify IPv4 or TCP checksums itself — the NIC does. But a NIC
//! does not drop a frame whose checksum fails: it delivers it and records
//! the verdict in the mbuf's offload flags, and for some frames it records
//! no verdict at all. Unless something reads those flags, a corrupted
//! segment reaches the stack unverified, and its bytes reach client
//! ingress and replication as if they were sound.
//!
//! This module is that something. [`RxChecksumPolicy::admit`] decides per
//! frame: a frame the NIC flags bad is dropped (TCP retransmits it), a
//! frame the NIC vouches for passes, and a frame the NIC left unverified
//! is verified here in software — the work the stack would have done.
//!
//! Pure logic with no libdpdk dependency, so it lives outside the
//! `dpdk-sys` gate and is tested on any build host. The flag values are
//! supplied by the caller: the device reads them from the DPDK headers it
//! was compiled against, and [`RxChecksumFlags::DPDK`] spells out the
//! documented values for tests and as a cross-check.

/// Where the receive checksum verdicts live in an mbuf's `ol_flags`.
///
/// `u64` because that is the width of `rte_mbuf::ol_flags`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RxChecksumFlags {
    /// `RTE_MBUF_F_RX_IP_CKSUM_MASK`.
    pub ip_mask: u64,
    /// `RTE_MBUF_F_RX_IP_CKSUM_GOOD`.
    pub ip_good: u64,
    /// `RTE_MBUF_F_RX_IP_CKSUM_BAD`.
    pub ip_bad: u64,
    /// `RTE_MBUF_F_RX_IP_CKSUM_NONE`.
    pub ip_none: u64,
    /// `RTE_MBUF_F_RX_L4_CKSUM_MASK`.
    pub l4_mask: u64,
    /// `RTE_MBUF_F_RX_L4_CKSUM_GOOD`.
    pub l4_good: u64,
    /// `RTE_MBUF_F_RX_L4_CKSUM_BAD`.
    pub l4_bad: u64,
    /// `RTE_MBUF_F_RX_L4_CKSUM_NONE`.
    pub l4_none: u64,
}

impl RxChecksumFlags {
    /// The values `rte_mbuf_core.h` defines. Within each mask, the
    /// all-clear value is `UNKNOWN`: the NIC did not check.
    pub const DPDK: Self = Self {
        ip_mask: (1 << 4) | (1 << 7),
        ip_good: 1 << 7,
        ip_bad: 1 << 4,
        ip_none: (1 << 4) | (1 << 7),
        l4_mask: (1 << 3) | (1 << 8),
        l4_good: 1 << 8,
        l4_bad: 1 << 3,
        l4_none: (1 << 3) | (1 << 8),
    };

    fn ip_status(&self, ol_flags: u64) -> ChecksumStatus {
        status(
            ol_flags & self.ip_mask,
            self.ip_good,
            self.ip_bad,
            self.ip_none,
        )
    }

    fn l4_status(&self, ol_flags: u64) -> ChecksumStatus {
        status(
            ol_flags & self.l4_mask,
            self.l4_good,
            self.l4_bad,
            self.l4_none,
        )
    }
}

/// What the NIC reported for one checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumStatus {
    /// Verified and correct.
    Good,
    /// Verified and wrong.
    Bad,
    /// The checksum field is not valid but the data's integrity was
    /// verified by other means (DPDK's `NONE`).
    IntegrityVerified,
    /// Not checked.
    Unknown,
}

fn status(masked: u64, good: u64, bad: u64, none: u64) -> ChecksumStatus {
    // `none` is `good | bad`, so it has to be matched first.
    if masked == none {
        ChecksumStatus::IntegrityVerified
    } else if masked == good {
        ChecksumStatus::Good
    } else if masked == bad {
        ChecksumStatus::Bad
    } else {
        ChecksumStatus::Unknown
    }
}

/// What to do with a received frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RxVerdict {
    /// Hand it to the stack.
    Accept,
    /// Free it: a checksum the NIC verified is wrong.
    Drop,
    /// The NIC left a checksum the stack relies on it for unchecked;
    /// verify the listed ones in software before handing it over.
    Verify { ip: bool, l4: bool },
}

/// Receive checksum admission for one device: which checksums the NIC
/// was asked to verify (and the stack therefore skips), and where the
/// NIC's verdicts are recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RxChecksumPolicy {
    flags: RxChecksumFlags,
    /// The stack skips IPv4 header checksums: the NIC owns them.
    ip_offloaded: bool,
    /// The stack skips TCP checksums: the NIC owns them.
    l4_offloaded: bool,
}

impl RxChecksumPolicy {
    /// A policy for a device whose receive offloads are as given, or
    /// `None` when neither is offloaded — the stack then verifies every
    /// checksum itself and there is nothing for the driver to do.
    pub fn new(flags: RxChecksumFlags, ip_offloaded: bool, l4_offloaded: bool) -> Option<Self> {
        (ip_offloaded || l4_offloaded).then_some(Self {
            flags,
            ip_offloaded,
            l4_offloaded,
        })
    }

    /// Classify a frame from its offload flags alone.
    pub fn verdict(&self, ol_flags: u64) -> RxVerdict {
        let mut verify_ip = false;
        let mut verify_l4 = false;
        if self.ip_offloaded {
            match self.flags.ip_status(ol_flags) {
                ChecksumStatus::Bad => return RxVerdict::Drop,
                ChecksumStatus::Unknown => verify_ip = true,
                ChecksumStatus::Good | ChecksumStatus::IntegrityVerified => {}
            }
        }
        if self.l4_offloaded {
            match self.flags.l4_status(ol_flags) {
                ChecksumStatus::Bad => return RxVerdict::Drop,
                ChecksumStatus::Unknown => verify_l4 = true,
                ChecksumStatus::Good | ChecksumStatus::IntegrityVerified => {}
            }
        }
        if verify_ip || verify_l4 {
            RxVerdict::Verify {
                ip: verify_ip,
                l4: verify_l4,
            }
        } else {
            RxVerdict::Accept
        }
    }

    /// Whether `frame`, received with `ol_flags`, may reach the stack.
    /// Software verification runs only for checksums the NIC left
    /// unchecked, so a NIC that reports verdicts costs one flag test.
    pub fn admit(&self, ol_flags: u64, frame: &[u8]) -> bool {
        match self.verdict(ol_flags) {
            RxVerdict::Accept => true,
            RxVerdict::Drop => false,
            RxVerdict::Verify { ip, l4 } => checksums_valid(frame, ip, l4),
        }
    }
}

const ETH_HEADER_LEN: usize = 14;
const ETHERTYPE_IPV4: [u8; 2] = [0x08, 0x00];
const IPV4_MIN_HEADER_LEN: usize = 20;
const IP_PROTO_TCP: u8 = 6;
const TCP_MIN_HEADER_LEN: usize = 20;

/// Verify the IPv4 header checksum (`ip`) and the TCP checksum (`l4`) of
/// an Ethernet frame in software.
///
/// Only what the NIC was trusted with is checked here; everything else is
/// left to the stack, which verifies or rejects it on its own:
///
/// - a frame that is not IPv4 (ARP, say) passes untouched;
/// - the TCP checksum of a non-TCP datagram or of an IPv4 fragment is not
///   checked — the stack verifies other protocols itself and does not
///   reassemble fragments, so it drops them anyway.
///
/// An IPv4 frame too malformed to locate the checksummed bytes in fails:
/// whatever it is, it cannot have been verified.
pub fn checksums_valid(frame: &[u8], ip: bool, l4: bool) -> bool {
    if frame.len() < ETH_HEADER_LEN || frame[12..14] != ETHERTYPE_IPV4 {
        return true;
    }
    let packet = &frame[ETH_HEADER_LEN..];
    if packet.len() < IPV4_MIN_HEADER_LEN || packet[0] >> 4 != 4 {
        return false;
    }
    let header_len = usize::from(packet[0] & 0x0F) * 4;
    let total_len = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    // Ethernet pads short frames, so the datagram is `total_len`, not the
    // rest of the frame.
    if header_len < IPV4_MIN_HEADER_LEN || total_len < header_len || total_len > packet.len() {
        return false;
    }
    if ip && fold(sum_words(&packet[..header_len], 0)) != 0xFFFF {
        return false;
    }
    let fragmented = u16::from_be_bytes([packet[6], packet[7]]) & 0x3FFF != 0;
    if l4 && packet[9] == IP_PROTO_TCP && !fragmented {
        let segment = &packet[header_len..total_len];
        if segment.len() < TCP_MIN_HEADER_LEN {
            return false;
        }
        // Pseudo-header: source, destination, zero + protocol, TCP length.
        let mut sum = sum_words(&packet[12..20], 0);
        sum += u32::from(IP_PROTO_TCP);
        // `segment.len()` fits: it is bounded by the 16-bit total length.
        sum += segment.len() as u32;
        if fold(sum_words(segment, sum)) != 0xFFFF {
            return false;
        }
    }
    true
}

/// Add `bytes` to `sum` as big-endian 16-bit words, an odd trailing byte
/// padded with zero. `u32` holds the running sum with room for the
/// carries of any datagram (at most 32 Ki words of at most 0xFFFF).
fn sum_words(bytes: &[u8], mut sum: u32) -> u32 {
    let (words, remainder) = bytes.as_chunks::<2>();
    for w in words {
        sum += u32::from(u16::from_be_bytes(*w));
    }
    if let [last] = remainder {
        sum += u32::from(*last) << 8;
    }
    sum
}

/// Fold a one's-complement sum to 16 bits. A region whose checksum field
/// is correct folds to `0xFFFF`.
fn fold(mut sum: u32) -> u16 {
    while sum > 0xFFFF {
        sum = (sum & 0xFFFF) + (sum >> 16);
    }
    // The loop leaves `sum <= 0xFFFF`.
    sum as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    const F: RxChecksumFlags = RxChecksumFlags::DPDK;

    fn policy() -> RxChecksumPolicy {
        RxChecksumPolicy::new(F, true, true).expect("offloaded")
    }

    /// An Ethernet/IPv4/TCP frame carrying `payload`, both checksums
    /// correct, padded to Ethernet's minimum when short.
    fn tcp_frame(payload: &[u8]) -> Vec<u8> {
        let tcp_len = TCP_MIN_HEADER_LEN + payload.len();
        let total_len = IPV4_MIN_HEADER_LEN + tcp_len;
        let mut f = vec![0u8; ETH_HEADER_LEN];
        f[12..14].copy_from_slice(&ETHERTYPE_IPV4);
        let mut ip = [0u8; IPV4_MIN_HEADER_LEN];
        ip[0] = 0x45;
        ip[2..4].copy_from_slice(&(total_len as u16).to_be_bytes());
        ip[6] = 0x40; // don't fragment
        ip[8] = 64;
        ip[9] = IP_PROTO_TCP;
        ip[12..16].copy_from_slice(&[10, 0, 0, 1]);
        ip[16..20].copy_from_slice(&[10, 0, 0, 2]);
        let ip_cksum = !fold(sum_words(&ip, 0));
        ip[10..12].copy_from_slice(&ip_cksum.to_be_bytes());
        f.extend_from_slice(&ip);

        let mut tcp = vec![0u8; TCP_MIN_HEADER_LEN];
        tcp[0..2].copy_from_slice(&40_000u16.to_be_bytes());
        tcp[2..4].copy_from_slice(&9_000u16.to_be_bytes());
        tcp[12] = 0x50; // data offset 5
        tcp[13] = 0x18; // PSH | ACK
        tcp.extend_from_slice(payload);
        let mut sum = sum_words(&ip[12..20], 0);
        sum += u32::from(IP_PROTO_TCP) + tcp_len as u32;
        let tcp_cksum = !fold(sum_words(&tcp, sum));
        tcp[16..18].copy_from_slice(&tcp_cksum.to_be_bytes());
        f.extend_from_slice(&tcp);
        if f.len() < 60 {
            f.resize(60, 0);
        }
        f
    }

    #[test]
    fn the_documented_flag_values_are_self_consistent() {
        assert_eq!(F.ip_none, F.ip_good | F.ip_bad);
        assert_eq!(F.l4_none, F.l4_good | F.l4_bad);
        assert_eq!(F.ip_mask, F.ip_none);
        assert_eq!(F.l4_mask, F.l4_none);
        assert_eq!(F.ip_mask & F.l4_mask, 0, "the two verdicts do not overlap");
    }

    #[test]
    fn verdicts_follow_the_nic() {
        let p = policy();
        assert_eq!(p.verdict(F.ip_good | F.l4_good), RxVerdict::Accept);
        assert_eq!(p.verdict(F.ip_none | F.l4_none), RxVerdict::Accept);
        assert_eq!(p.verdict(F.ip_bad | F.l4_good), RxVerdict::Drop);
        assert_eq!(p.verdict(F.ip_good | F.l4_bad), RxVerdict::Drop);
        assert_eq!(
            p.verdict(F.ip_good),
            RxVerdict::Verify {
                ip: false,
                l4: true
            }
        );
        assert_eq!(
            p.verdict(F.l4_good),
            RxVerdict::Verify {
                ip: true,
                l4: false
            }
        );
        assert_eq!(p.verdict(0), RxVerdict::Verify { ip: true, l4: true });
        // Unrelated flags (VLAN stripped, RSS hash present) change nothing.
        let unrelated = (1 << 0) | (1 << 1) | (1 << 6);
        assert_eq!(
            p.verdict(unrelated | F.ip_good | F.l4_good),
            RxVerdict::Accept
        );
        assert_eq!(p.verdict(unrelated | F.l4_bad), RxVerdict::Drop);
    }

    /// A checksum the NIC was not asked to verify is the stack's to check,
    /// so its flag — whatever it says — is ignored.
    #[test]
    fn only_offloaded_checksums_are_consulted() {
        let ip_only = RxChecksumPolicy::new(F, true, false).expect("offloaded");
        assert_eq!(ip_only.verdict(F.ip_good | F.l4_bad), RxVerdict::Accept);
        assert_eq!(
            ip_only.verdict(0),
            RxVerdict::Verify {
                ip: true,
                l4: false
            }
        );
        let l4_only = RxChecksumPolicy::new(F, false, true).expect("offloaded");
        assert_eq!(l4_only.verdict(F.ip_bad | F.l4_good), RxVerdict::Accept);
        assert!(RxChecksumPolicy::new(F, false, false).is_none());
    }

    #[test]
    fn a_sound_frame_verifies() {
        let frame = tcp_frame(b"an InputBatch, say");
        assert!(checksums_valid(&frame, true, true));
        assert!(policy().admit(0, &frame));
        // Odd payload length exercises the padded last word.
        assert!(checksums_valid(&tcp_frame(b"odd"), true, true));
    }

    /// The case the device must not let through: the NIC did not check,
    /// and a payload bit flipped.
    #[test]
    fn an_unverified_frame_with_a_damaged_payload_is_refused() {
        let mut frame = tcp_frame(b"an InputBatch, say");
        let last = ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN + TCP_MIN_HEADER_LEN + 3;
        frame[last] ^= 0x04;
        assert!(!checksums_valid(&frame, false, true));
        assert!(!policy().admit(F.ip_good, &frame));
        // The NIC's own verdicts still win when it gives them.
        assert!(!policy().admit(F.ip_good | F.l4_bad, &tcp_frame(b"x")));
        assert!(policy().admit(F.ip_good | F.l4_good, &frame));
    }

    #[test]
    fn an_unverified_frame_with_a_damaged_ip_header_is_refused() {
        let mut frame = tcp_frame(b"payload");
        frame[ETH_HEADER_LEN + 8] ^= 0x01; // TTL
        assert!(!checksums_valid(&frame, true, false));
        assert!(!policy().admit(F.l4_good, &frame));
        // Not checked when the NIC vouched for the IP header.
        assert!(policy().admit(F.ip_good | F.l4_good, &frame));
    }

    #[test]
    fn every_single_bit_flip_in_the_tcp_segment_is_caught() {
        let frame = tcp_frame(b"0123456789abcdef");
        let start = ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN;
        let end = start + TCP_MIN_HEADER_LEN + 16;
        for at in start..end {
            for bit in 0..8 {
                let mut damaged = frame.clone();
                damaged[at] ^= 1 << bit;
                assert!(
                    !checksums_valid(&damaged, false, true),
                    "flip of bit {bit} at byte {at} passed"
                );
            }
        }
    }

    #[test]
    fn ethernet_padding_is_not_part_of_the_datagram() {
        let mut frame = tcp_frame(b"");
        assert_eq!(frame.len(), 60, "padded to the Ethernet minimum");
        let tail = frame.len() - 1;
        frame[tail] = 0xAB;
        assert!(checksums_valid(&frame, true, true));
    }

    #[test]
    fn frames_the_nic_does_not_own_pass_through() {
        // ARP.
        let mut arp = vec![0u8; 42];
        arp[12..14].copy_from_slice(&[0x08, 0x06]);
        assert!(checksums_valid(&arp, true, true));
        // A runt with no EtherType.
        assert!(checksums_valid(&[0u8; 10], true, true));
        // A non-first IPv4 fragment: its TCP checksum cannot be checked
        // alone; the IP header still is.
        let mut frag = tcp_frame(b"fragment");
        frag[ETH_HEADER_LEN + 6] = 0x00;
        frag[ETH_HEADER_LEN + 7] = 0x10; // offset 16 × 8 bytes
        let ip = ETH_HEADER_LEN..ETH_HEADER_LEN + IPV4_MIN_HEADER_LEN;
        frag[ETH_HEADER_LEN + 10] = 0;
        frag[ETH_HEADER_LEN + 11] = 0;
        let cksum = !fold(sum_words(&frag[ip.clone()], 0));
        frag[ETH_HEADER_LEN + 10..ETH_HEADER_LEN + 12].copy_from_slice(&cksum.to_be_bytes());
        let payload_at = ip.end + TCP_MIN_HEADER_LEN;
        frag[payload_at] ^= 0xFF;
        assert!(checksums_valid(&frag, true, true));
    }

    #[test]
    fn a_malformed_ipv4_frame_is_refused() {
        let good = tcp_frame(b"x");
        // IHL below the minimum.
        let mut bad_ihl = good.clone();
        bad_ihl[ETH_HEADER_LEN] = 0x44;
        assert!(!checksums_valid(&bad_ihl, true, true));
        // Total length past the end of the frame.
        let mut long = good.clone();
        long[ETH_HEADER_LEN + 2..ETH_HEADER_LEN + 4].copy_from_slice(&1500u16.to_be_bytes());
        assert!(!checksums_valid(&long, false, true));
        // Truncated inside the IP header.
        assert!(!checksums_valid(&good[..ETH_HEADER_LEN + 10], true, true));
        // Not version 4.
        let mut v6 = good.clone();
        v6[ETH_HEADER_LEN] = 0x65;
        assert!(!checksums_valid(&v6, true, true));
    }
}
