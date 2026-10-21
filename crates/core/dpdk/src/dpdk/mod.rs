// `device`, `eal`, `mempool` and `port` are public as modules: a program
// that runs its own TCP stack on the NIC (a bench client, say) builds on
// them directly. The transport and the NIC handle are reached through
// the types re-exported below.
pub mod device;
pub mod eal;
pub(crate) mod ffi;
pub mod mempool;
pub(crate) mod nic_handle;
pub mod port;
pub(crate) mod transport;

pub use crate::nic::{NextHop, NicClock};
pub use eal::{Eal, EalError};
pub use mempool::{Mempool, MempoolError};
pub use nic_handle::{NicHandle, NicStream, StreamId};
pub use port::{Port, PortError};
pub use smoltcp::iface::SocketHandle;
pub use transport::{AcceptedConnection, DpdkShared, DpdkTransport, MAX_CONNECTIONS};
