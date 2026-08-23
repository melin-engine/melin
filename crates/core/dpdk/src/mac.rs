//! Ethernet address parsing and peer-MAC resolution.
//!
//! Deliberately outside the `dpdk-sys` feature gate. None of this needs
//! libdpdk — it is string parsing and a byte-layout convention — and
//! keeping it ungated means the unit tests below run on any machine,
//! including CI hosts with no DPDK installed.

use std::net::Ipv4Addr;

/// An Ethernet address (EUI-48). A fixed `[u8; 6]` rather than a named
/// struct: it is what smoltcp's `EthernetAddress` and DPDK's
/// `rte_ether_addr` both wrap, so staying with the raw array avoids a
/// conversion at every boundary.
pub type MacAddr = [u8; 6];

/// A `--dpdk-*-mac` value that could not be parsed as an Ethernet address.
///
/// Owns its strings rather than borrowing the input: the error is
/// returned up through startup and formatted after the argv borrow it
/// came from has ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MacParseError {
    input: String,
    kind: MacParseErrorKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MacParseErrorKind {
    /// The value did not have exactly six colon-separated fields.
    FieldCount(usize),
    /// A field was not a one- or two-digit hex byte.
    Field(String),
}

impl std::fmt::Display for MacParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            MacParseErrorKind::FieldCount(n) => write!(
                f,
                "invalid MAC '{}': expected 6 colon-separated octets, found {n}",
                self.input
            ),
            MacParseErrorKind::Field(field) => write!(
                f,
                "invalid MAC '{}': '{field}' is not a hex octet",
                self.input
            ),
        }
    }
}

impl std::error::Error for MacParseError {}

/// Parse an Ethernet address of the form `aa:bb:cc:dd:ee:ff` (upper or
/// lower case).
///
/// Strict by intent: fields must be one or two hex digits, so the sign
/// and radix prefixes `u8::from_str_radix` would otherwise accept
/// (`+a`, for instance) are rejected. A MAC that parses to the wrong
/// value does not fail loudly — it silently addresses frames to someone
/// who never answers — so it is worth refusing anything that is not
/// plainly an address.
pub fn try_parse_mac(s: &str) -> Result<MacAddr, MacParseError> {
    let mut out = [0u8; 6];
    let mut fields = 0usize;

    for field in s.split(':') {
        // Keep counting past the end so the error can report how many
        // fields were actually supplied rather than just "not 6".
        if fields < out.len() {
            let plausible =
                (1..=2).contains(&field.len()) && field.bytes().all(|b| b.is_ascii_hexdigit());
            let parsed = if plausible {
                u8::from_str_radix(field, 16).ok()
            } else {
                None
            };
            out[fields] = parsed.ok_or_else(|| MacParseError {
                input: s.to_string(),
                kind: MacParseErrorKind::Field(field.to_string()),
            })?;
        }
        fields += 1;
    }

    if fields != out.len() {
        return Err(MacParseError {
            input: s.to_string(),
            kind: MacParseErrorKind::FieldCount(fields),
        });
    }

    Ok(out)
}

/// Panicking wrapper around [`try_parse_mac`].
///
/// Retained for callers that parse at startup and have no error channel.
/// Prefer [`try_parse_mac`] on any path that runs after the server is
/// live — a panic there takes down a running node over a typo.
pub fn parse_mac(s: &str) -> MacAddr {
    match try_parse_mac(s) {
        Ok(mac) => mac,
        Err(e) => panic!("{e}"),
    }
}

/// Where the MAC used to address a peer came from.
///
/// Carried alongside the address purely so the seeding log can name its
/// source. The two cases fail in completely different ways — a wrong
/// override is a typo, a wrong derivation means the fallback convention
/// does not hold on this NIC — and the log line is the only thing that
/// distinguishes them before the symptom (a connect that never
/// completes) appears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerMacSource {
    /// Taken from an operator-supplied `--dpdk-peer-mac`.
    Supplied,
    /// Derived from the peer's IPv4 address via the SR-IOV convention.
    DerivedSrIov,
}

impl PeerMacSource {
    /// Short tag for structured logs.
    pub fn as_str(self) -> &'static str {
        match self {
            PeerMacSource::Supplied => "supplied",
            PeerMacSource::DerivedSrIov => "derived-sriov",
        }
    }
}

impl std::fmt::Display for PeerMacSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Resolve the Ethernet address used to reach `peer_ip`, preferring an
/// operator-supplied value over the derived fallback.
///
/// The fallback builds `02:00:<the four IPv4 octets>`. That is the
/// address `dpdk-setup.sh` assigns to SR-IOV VFs (`0x02` marks it
/// locally administered and unicast), so it is correct on that path and
/// only on that path. A port that keeps a real hardware address — an
/// mlx5 in bifurcated mode shares the kernel netdev's — needs the
/// override, or frames go to an address nothing on the segment answers
/// for.
///
/// ARP cannot cover for a wrong guess in bifurcated mode: the `rte_flow`
/// steering rule matches on IPv4 source address, so ARP (EtherType
/// 0x0806) is never delivered into DPDK at all. This is the same reason
/// the gateway MAC has to be seeded statically.
pub fn resolve_peer_mac(supplied: Option<MacAddr>, peer_ip: Ipv4Addr) -> (MacAddr, PeerMacSource) {
    match supplied {
        Some(mac) => (mac, PeerMacSource::Supplied),
        None => {
            let o = peer_ip.octets();
            (
                [0x02, 0x00, o[0], o[1], o[2], o[3]],
                PeerMacSource::DerivedSrIov,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_lower_case_mac() {
        assert_eq!(
            try_parse_mac("aa:bb:cc:dd:ee:ff"),
            Ok([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff])
        );
    }

    #[test]
    fn parses_an_upper_case_mac() {
        assert_eq!(
            try_parse_mac("AA:BB:CC:DD:EE:FF"),
            Ok([0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff])
        );
    }

    #[test]
    fn parses_single_digit_fields() {
        // `ip neigh` pads to two digits, but a hand-typed `0:0:0:0:0:1`
        // is unambiguous and there is no reason to refuse it.
        assert_eq!(try_parse_mac("0:0:0:0:0:1"), Ok([0, 0, 0, 0, 0, 1]));
    }

    #[test]
    fn rejects_too_few_fields() {
        let err = try_parse_mac("aa:bb:cc:dd:ee").expect_err("5 fields must not parse");
        assert!(err.to_string().contains("found 5"), "{err}");
    }

    #[test]
    fn rejects_too_many_fields() {
        let err = try_parse_mac("aa:bb:cc:dd:ee:ff:00").expect_err("7 fields must not parse");
        assert!(err.to_string().contains("found 7"), "{err}");
    }

    #[test]
    fn rejects_a_non_hex_field() {
        let err = try_parse_mac("aa:bb:cc:dd:ee:gg").expect_err("'gg' is not hex");
        assert!(err.to_string().contains("'gg'"), "{err}");
    }

    #[test]
    fn rejects_a_signed_field() {
        // `u8::from_str_radix` accepts a leading '+'; the length/hex-digit
        // guard is what keeps it out.
        try_parse_mac("+a:bb:cc:dd:ee:ff").expect_err("'+a' is not a hex octet");
    }

    #[test]
    fn rejects_an_oversized_field() {
        try_parse_mac("aaa:bb:cc:dd:ee:ff").expect_err("'aaa' is not a hex octet");
    }

    #[test]
    fn rejects_an_empty_field() {
        try_parse_mac("aa::cc:dd:ee:ff").expect_err("an empty field is not a hex octet");
    }

    #[test]
    fn rejects_an_empty_string() {
        try_parse_mac("").expect_err("the empty string is not a MAC");
    }

    #[test]
    fn error_names_the_offending_input() {
        let err = try_parse_mac("nope").expect_err("'nope' is not a MAC");
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn a_supplied_mac_wins() {
        let supplied = [0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80];
        let (mac, source) = resolve_peer_mac(Some(supplied), Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(mac, supplied);
        assert_eq!(source, PeerMacSource::Supplied);
    }

    #[test]
    fn an_absent_mac_falls_back_to_the_sriov_convention() {
        let (mac, source) = resolve_peer_mac(None, Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(mac, [0x02, 0x00, 10, 0, 0, 2]);
        assert_eq!(source, PeerMacSource::DerivedSrIov);
    }

    #[test]
    fn the_derived_mac_carries_every_ip_octet() {
        // Pins the byte order: a transposition here would produce a
        // plausible-looking address that silently reaches nobody.
        let (mac, _) = resolve_peer_mac(None, Ipv4Addr::new(192, 168, 7, 31));
        assert_eq!(mac, [0x02, 0x00, 192, 168, 7, 31]);
    }

    #[test]
    fn the_derived_mac_is_locally_administered_and_unicast() {
        let (mac, _) = resolve_peer_mac(None, Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(mac[0] & 0x02, 0x02, "locally-administered bit must be set");
        assert_eq!(mac[0] & 0x01, 0x00, "multicast bit must be clear");
    }

    #[test]
    fn a_parsed_override_round_trips_through_resolution() {
        let supplied = try_parse_mac("0c:42:a1:5b:2e:80").expect("valid MAC");
        let (mac, source) = resolve_peer_mac(Some(supplied), Ipv4Addr::new(10, 0, 0, 2));
        assert_eq!(mac, [0x0c, 0x42, 0xa1, 0x5b, 0x2e, 0x80]);
        assert_eq!(source, PeerMacSource::Supplied);
    }
}
