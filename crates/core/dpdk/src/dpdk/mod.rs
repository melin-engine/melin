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

// MAC parsing lives in `crate::mac`, outside the `dpdk-sys` gate, and is
// re-exported from the crate root — so `melin_dpdk::parse_mac` still
// resolves for callers that used it from here.
