//! The local ports dials take: the client NIC handle's, and a node's
//! replica's towards its primary.
//!
//! Plain logic with no libdpdk dependency, so it is tested on every host.
//! Ungated, unlike the rest of the client handle's logic (`nic`): a node
//! maps its replica's dial ports from its configuration in any build.

/// The first port of the IANA ephemeral range, where dials take their
/// local ports.
#[cfg(any(test, feature = "dpdk-sys"))]
const EPHEMERAL_FIRST: u16 = 49_152;
/// Where in the range a process starts: one of this many ports, picked
/// from the process ID, so that a process restarted on the same host does
/// not dial from the four-tuple its predecessor's connection may still
/// hold on the node. The shm-proxy and the exchange's bench have always
/// started there.
#[cfg(any(test, feature = "dpdk-sys"))]
const EPHEMERAL_SPREAD: u32 = 8_192;

/// Local ports for dials: a fresh one per socket, not per process.
///
/// A node may still hold a connection whose close it has not yet taken in
/// (or that was never closed), and a reconnect from that connection's
/// four-tuple would be taken for it. Each dial therefore takes the next port
/// along a range that ends at the last port, wrapping at its end and
/// skipping any port the caller says it may not take (one an open stream
/// holds, one the NIC steers elsewhere), so a port comes round again only
/// after the whole range has been dialed from.
///
/// The client NIC handle's range is the IANA ephemeral one, entered at a
/// point picked from the process ID; a node's replica dials its primary
/// along a range of its own ([`LocalPorts::from_first`]).
#[derive(Debug)]
pub struct LocalPorts {
    /// The range's first port, where it wraps back to.
    first: u16,
    next: u16,
}

impl LocalPorts {
    /// Ports along the ephemeral range, starting at a point picked from
    /// `seed`, the process ID. The client NIC handle's.
    #[cfg(any(test, feature = "dpdk-sys"))]
    pub(crate) fn starting_from(seed: u32) -> Self {
        // In range by construction: the spread is under the range's size.
        let offset = (seed % EPHEMERAL_SPREAD) as u16;
        Self {
            first: EPHEMERAL_FIRST,
            next: EPHEMERAL_FIRST + offset,
        }
    }

    /// Ports `first` to the last port, starting at `first`. Port 0 is no
    /// port to dial from: a range from it starts at 1.
    pub fn from_first(first: u16) -> Self {
        let first = first.max(1);
        Self { first, next: first }
    }

    /// The next port the caller may take (`excluded` says which it may
    /// not), or `None` if it may take none of the range.
    pub fn take(&mut self, excluded: impl Fn(u16) -> bool) -> Option<u16> {
        // Every port of the range once: `u16::MAX` is its last port.
        for _ in self.first..=u16::MAX {
            let port = self.next;
            self.next = if port == u16::MAX {
                self.first
            } else {
                port + 1
            };
            if !excluded(port) {
                return Some(port);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_port_is_picked_from_the_process_id() {
        // As the shm-proxy and the exchange's bench always picked theirs.
        let seed = 123_456;
        let mut ports = LocalPorts::starting_from(seed);
        assert_eq!(ports.take(|_| false), Some(49_152 + (seed % 8_192) as u16));
        let mut highest = LocalPorts::starting_from(EPHEMERAL_SPREAD - 1);
        assert_eq!(highest.take(|_| false), Some(49_152 + 8_191));
    }

    /// A reconnect never reuses the four-tuple of the connection before it.
    #[test]
    fn every_dial_takes_a_fresh_port() {
        let mut ports = LocalPorts::starting_from(7);
        let first = ports.take(|_| false).expect("a port");
        let second = ports.take(|_| false).expect("a port");
        let third = ports.take(|_| false).expect("a port");
        assert_eq!(second, first + 1);
        assert_eq!(third, first + 2);
    }

    #[test]
    fn ports_wrap_at_the_end_of_the_range_and_skip_those_in_use() {
        let mut ports = LocalPorts {
            first: EPHEMERAL_FIRST,
            next: u16::MAX - 1,
        };
        assert_eq!(ports.take(|_| false), Some(u16::MAX - 1));
        assert_eq!(ports.take(|p| p == u16::MAX), Some(EPHEMERAL_FIRST));
        assert_eq!(ports.take(|_| false), Some(EPHEMERAL_FIRST + 1));
    }

    /// A range of the caller's own starts at its first port, and wraps back
    /// to it rather than to the ephemeral range's.
    #[test]
    fn a_range_from_a_first_port_starts_and_wraps_there() {
        let mut ports = LocalPorts::from_first(40_000);
        assert_eq!(ports.take(|_| false), Some(40_000));
        assert_eq!(ports.take(|p| p == 40_001), Some(40_002));
        ports.next = u16::MAX;
        assert_eq!(ports.take(|_| false), Some(u16::MAX));
        assert_eq!(ports.take(|_| false), Some(40_000));
    }

    /// Every port of the range comes round once before any comes round
    /// twice, an excluded one never.
    #[test]
    fn a_full_round_takes_every_port_not_excluded_once() {
        let first = u16::MAX - 9;
        let mut ports = LocalPorts::from_first(first);
        let excluded = [first + 3, u16::MAX];
        let round: Vec<u16> = (0..8)
            .map(|_| ports.take(|p| excluded.contains(&p)).expect("a port"))
            .collect();
        let expected: Vec<u16> = (first..=u16::MAX)
            .filter(|p| !excluded.contains(p))
            .collect();
        assert_eq!(round, expected);
        assert_eq!(ports.take(|p| excluded.contains(&p)), Some(first));
        // A range wholly excluded has no port.
        assert_eq!(ports.take(|_| true), None);
    }

    #[test]
    fn a_range_from_port_zero_starts_at_one() {
        let mut ports = LocalPorts::from_first(0);
        assert_eq!(ports.take(|_| false), Some(1));
        ports.next = u16::MAX;
        assert_eq!(ports.take(|_| false), Some(u16::MAX));
        assert_eq!(ports.take(|_| false), Some(1));
    }

    #[test]
    fn a_range_held_entirely_has_no_port() {
        let mut ports = LocalPorts::starting_from(0);
        assert_eq!(ports.take(|_| true), None);
        // And the range is still served once a port frees up.
        assert_eq!(ports.take(|p| p != 60_000), Some(60_000));
    }
}
