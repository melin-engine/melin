pub mod device;
pub mod eal;
pub(crate) mod ffi;
pub mod mempool;
pub mod port;
pub mod transport;

pub use eal::{Eal, EalError};
pub use mempool::{Mempool, MempoolError};
pub use port::{Port, PortError};
pub use smoltcp::iface::SocketHandle;
pub use transport::{AcceptedConnection, DpdkConfig, DpdkShared, DpdkTransport, MAX_CONNECTIONS};

/// Parse an Ethernet MAC of the form `aa:bb:cc:dd:ee:ff` (lower or upper
/// case, colon-separated). Used to surface `--dpdk-gateway-mac` from the
/// CLI. Panics on malformed input — these are operator-supplied values
/// read from `ip neigh` and any error means a serious configuration
/// mistake worth failing fast on.
pub fn parse_mac(s: &str) -> [u8; 6] {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 6 {
        panic!("invalid MAC '{s}': expected 6 colon-separated octets");
    }
    let mut out = [0u8; 6];
    for (i, p) in parts.iter().enumerate() {
        out[i] = u8::from_str_radix(p, 16)
            .unwrap_or_else(|_| panic!("invalid MAC octet '{p}' in '{s}'"));
    }
    out
}
