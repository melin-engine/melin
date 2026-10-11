//! The polled connection over kernel bypass: `melin-dpdk`'s client NIC
//! handle as a [`ByteStream`], and [`StreamConnection`]'s constructors on
//! it. Compiled only with the `dpdk` feature, which links `melin-dpdk`
//! (BUSL-1.1) and needs libdpdk on the build host.
//!
//! A NIC handle owns the socket set its streams live in, so a stream is
//! not an owned value but a view ([`NicStream`]) the handle lends by
//! [`StreamId`] for one call or a few. Two shapes follow, one per kind of
//! caller:
//!
//! - **Several connections on one NIC** (a load generator): the caller
//!   keeps the [`NicHandle`] and one [`PolledConnection`](crate::PolledConnection)
//!   per stream, and hands each call the stream's view,
//!   `connection.poll(&mut nic.stream(&id), now_ns)`. [`NicStream`] is a
//!   [`ByteStream`] for that. The handle's service step advances every
//!   stream at once, so such a loop calls [`NicHandle::service`] once per
//!   iteration and not through each view.
//! - **One connection** ([`StreamConnection`]): the wrapper owns its
//!   stream, which a borrowed view cannot be, so [`DpdkStream`] holds the
//!   handle (owned, or lent as `&mut`) and the stream's id, and lends
//!   itself the view on every call.
//!
//! Both send a request where the stack sends it from: the binding's
//! in-place send is the socket's own ([`NicStream::send_in_place`]), not
//! the trait's default, so a request is framed straight into the socket's
//! transmit buffer.
//!
//! # One clock
//!
//! A NIC handle's stack takes a monotonic nanosecond count whose origin is
//! fixed for the handle's life, and clamps a reading that goes backwards.
//! A [`StreamConnection`] on a handle therefore keeps its cached clock on
//! the handle's own origin ([`NicClock::origin`](melin_dpdk::NicClock::origin)): the dial reads
//! [`NicHandle::clock`], the wrapper's cached readings count from the same
//! instant, and a reconnect on the same handle carries on where the last
//! connection's readings stopped. A wrapper counting from its own creation
//! would hand the stack readings behind the ones the dial gave it, and the
//! stack's timers would stand still until they caught up.

use std::borrow::BorrowMut;
use std::fmt;
use std::io;
use std::net::{SocketAddr, SocketAddrV4};
use std::time::Duration;

use melin_dpdk::{NicConfig, NicHandle, NicStream, StreamId};

use crate::polled::{CachedClock, duration_ns};
use crate::{ByteStream, Error, SigningKey, StreamConnection};

/// The NIC stream as a [`ByteStream`], for a
/// [`PolledConnection`](crate::PolledConnection) driven one call at a
/// time: the shape of a caller with several streams on one [`NicHandle`],
/// polled from one thread on a clock of its own.
///
/// Each step maps onto the view's own: [`send`](NicStream::send),
/// [`recv`](NicStream::recv), [`service`](NicStream::service) (the whole
/// handle's step, every stream on it advanced), and
/// [`send_in_place`](NicStream::send_in_place), which lends the socket's
/// transmit buffer so a request is framed where the stack sends it from.
///
/// The view borrows the handle for as long as it is held, so it is taken
/// per call, and the handle's service step is called once per loop
/// iteration rather than through every stream's view:
///
/// ```no_run
/// use melin_client::{Frame, NicConfig, NicHandle, PolledConnection, SigningKey};
///
/// # fn run(keys: &[SigningKey], config: &NicConfig) -> Result<(), Box<dyn std::error::Error>> {
/// let mut nic = NicHandle::open(config)?;
/// // Any monotonic nanosecond count with an origin fixed for the handle's
/// // life will do (a calibrated TSC, say); the handle's own clock here.
/// let clock = nic.clock();
/// let node = "10.0.0.1:9876".parse()?;
///
/// // One stream and one connection per key, dialed in turn, and whether a
/// // request is owed its reply.
/// let mut connections = Vec::new();
/// for key in keys {
///     let deadline_ns = clock.now_ns() + 5_000_000_000;
///     let id = nic.dial(node, 256 * 1024, deadline_ns, || clock.now_ns())?;
///     connections.push((id, PolledConnection::new(key), false));
/// }
///
/// loop {
///     let now_ns = clock.now_ns();
///     nic.service(now_ns);
///     for (id, connection, owed) in &mut connections {
///         // The handshake runs inside `poll`; a request waits for it.
///         if connection.is_ready() && !*owed {
///             connection.send(&mut nic.stream(id), now_ns, b"request")?;
///             *owed = true;
///         }
///         while let Some(frame) = connection.poll(&mut nic.stream(id), now_ns)? {
///             if frame == Frame::BatchEnd {
///                 *owed = false;
///             }
///         }
///     }
/// }
/// # }
/// ```
impl ByteStream for NicStream<'_> {
    #[inline]
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        NicStream::send(self, data)
    }

    #[inline]
    fn service(&mut self, now_ns: u64) {
        NicStream::service(self, now_ns);
    }

    #[inline]
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        NicStream::recv(self, buf)
    }

    #[inline]
    fn send_in_place(&mut self, fill: impl FnOnce(&mut [u8]) -> usize) -> io::Result<usize> {
        NicStream::send_in_place(self, fill)
    }
}

/// One stream of a [`NicHandle`], owned: the handle (`H`, the handle
/// itself or a `&mut` to one) and the id of a stream dialed on it. The
/// stream a [`StreamConnection`] over DPDK owns.
///
/// Every [`ByteStream`] call lends itself the stream's [`NicStream`] view
/// for that call, so the binding is the view's: the socket's own in-place
/// send, and the handle's service step.
///
/// A view cannot be the stream a [`StreamConnection`] owns, since it
/// borrows the handle; this type is the single-stream case's owned form.
/// A caller with several streams on one handle uses the view itself, per
/// call (see [`ByteStream`]'s implementation for [`NicStream`]).
///
/// Obtained only from [`StreamConnection::connect_dpdk`] and
/// [`StreamConnection::connect_dpdk_on`], which put the wrapper's clock on
/// the handle's origin, so that one clock drives the handle's stack (see
/// [`connect_dpdk_on`](StreamConnection::connect_dpdk_on)).
///
/// Dropping it closes the stream ([`NicHandle::close`]): the socket is
/// reset, so the node frees the connection's slot at once, and freed, so a
/// client that reconnects on a lent handle does not fill its socket set
/// with dead streams. An owned handle then goes too, and with it an EAL it
/// initialised.
pub struct DpdkStream<H: BorrowMut<NicHandle> = NicHandle> {
    nic: H,
    /// The stream's id, `Some` for as long as the stream is open: taken
    /// only by `Drop`, since closing a stream consumes its id. An `Option`
    /// rather than the bare id for that reason alone; a view asked for
    /// after the take, which nothing does, is an error, not a panic.
    id: Option<StreamId>,
    /// The id's peer and local port, copied out so neither needs the id.
    peer: SocketAddrV4,
    local_port: u16,
}

impl<H: BorrowMut<NicHandle>> DpdkStream<H> {
    /// The NIC the stream is on: its MAC, the peer MAC a dial seeds, its
    /// clock.
    pub fn nic(&self) -> &NicHandle {
        self.nic.borrow()
    }

    /// The node this stream is connected to.
    pub fn peer(&self) -> SocketAddrV4 {
        self.peer
    }

    /// The local port this stream was dialed from.
    pub fn local_port(&self) -> u16 {
        self.local_port
    }

    /// The stream's view, for one call.
    #[inline]
    fn view(&mut self) -> io::Result<NicStream<'_>> {
        let id = self
            .id
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "the DPDK stream is closed"))?;
        let nic: &mut NicHandle = self.nic.borrow_mut();
        Ok(nic.stream(id))
    }
}

impl<H: BorrowMut<NicHandle>> Drop for DpdkStream<H> {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let nic: &mut NicHandle = self.nic.borrow_mut();
            nic.close(id);
        }
    }
}

impl<H: BorrowMut<NicHandle>> fmt::Debug for DpdkStream<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DpdkStream")
            .field("peer", &self.peer)
            .field("local_port", &self.local_port)
            .field("open", &self.id.is_some())
            .finish_non_exhaustive()
    }
}

impl<H: BorrowMut<NicHandle>> ByteStream for DpdkStream<H> {
    #[inline]
    fn send(&mut self, data: &[u8]) -> io::Result<usize> {
        self.view()?.send(data)
    }

    /// The handle's service step: every stream on the handle advances, not
    /// only this one.
    #[inline]
    fn service(&mut self, now_ns: u64) {
        let nic: &mut NicHandle = self.nic.borrow_mut();
        nic.service(now_ns);
    }

    #[inline]
    fn recv(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.view()?.recv(buf)
    }

    #[inline]
    fn send_in_place(&mut self, fill: impl FnOnce(&mut [u8]) -> usize) -> io::Result<usize> {
        self.view()?.send_in_place(fill)
    }
}

impl StreamConnection<DpdkStream> {
    /// Bring a DPDK NIC up from `config`, dial `addr` on it and
    /// authenticate with `key`: [`StreamConnection::connect_timeout`] over
    /// kernel bypass, the connection owning the NIC.
    ///
    /// `socket_buffer` is the stream's receive and transmit buffer, in
    /// bytes: what one in-place send can lend, and how far requests can
    /// run ahead of the wire. `timeout` bounds the dial, the handshake, and
    /// every wait after, as on the kernel path.
    ///
    /// EAL is initialised from `config.eal_args` (with a file prefix of the
    /// client's own when they name none; see [`NicConfig::eal_args`]) and
    /// cleaned up when the connection drops, after which EAL cannot be
    /// initialised again in this process: a client that will reconnect
    /// opens a [`NicHandle`] itself and dials with
    /// [`connect_dpdk_on`](StreamConnection::connect_dpdk_on), lending it.
    /// In a process that runs a process-wide EAL the handle shares it, and
    /// `config.eal_args` must be empty.
    ///
    /// # Errors
    ///
    /// [`Error::Nic`] if the NIC cannot be brought up (EAL initialisation,
    /// a missing port, the pool, the port's configuration); otherwise as
    /// [`connect_dpdk_on`](StreamConnection::connect_dpdk_on).
    pub fn connect_dpdk(
        config: &NicConfig,
        addr: SocketAddrV4,
        key: &SigningKey,
        socket_buffer: usize,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let nic = NicHandle::open(config).map_err(Error::Nic)?;
        Self::connect_dpdk_on(nic, addr, key, socket_buffer, timeout)
    }
}

impl<H: BorrowMut<NicHandle>> StreamConnection<DpdkStream<H>> {
    /// Dial `addr` on a NIC already up and authenticate with `key`: for a
    /// client that reconnects without touching EAL (which initialises
    /// once per process), and for a process that already holds the
    /// process-wide EAL (a test hosting nodes) and opened the handle on it.
    ///
    /// `nic` is the handle or a `&mut` to one. Lent, the handle outlives
    /// the connection and a failed dial alike, and the next connection
    /// dials on it again; owned, it goes with the connection, and with a
    /// connection that fails to come up. `socket_buffer` and `timeout` are
    /// as on [`connect_dpdk`](StreamConnection::connect_dpdk).
    ///
    /// One clock drives the handle's stack: the handle's own
    /// ([`NicHandle::clock`]). The dial reads it on every iteration, and the
    /// connection's cached clock counts from its origin
    /// ([`NicClock::origin`](melin_dpdk::NicClock::origin)) rather than
    /// from the connection's creation, so the service step and the read
    /// deadline take readings on the same scale as the dial's, and a
    /// reconnect on the handle carries on from where the last connection's
    /// readings stopped. The stack clamps a reading that goes backwards, so
    /// a connection counting from its own creation would hold the stack's
    /// timers (retransmissions among them) still until its readings caught
    /// up. For the same reason, a handle that another loop drives on a
    /// clock of its own (a TSC) is not for this wrapper: that loop drives
    /// its streams through [`PolledConnection`](crate::PolledConnection)
    /// and [`NicStream`] on its own readings instead.
    ///
    /// Dials on one handle are sequential, as the handle's documentation
    /// explains: this returns once the connection is established and
    /// authenticated, or has failed.
    ///
    /// # Errors
    ///
    /// - [`Error::Connect`] if the dial fails. Its source is the handle's
    ///   error, unchanged: [`io::ErrorKind::TimedOut`] for no answer by the
    ///   deadline, whose message names the MAC the SYN was addressed to as
    ///   the first thing to check (a wrong one is silent);
    ///   [`io::ErrorKind::ConnectionRefused`] for a reset (nothing listens
    ///   there, or a DPDK node was in another client's handshake);
    ///   [`io::ErrorKind::InvalidInput`] for a zero `socket_buffer`;
    ///   [`io::ErrorKind::AddrInUse`] for a handle with no room for
    ///   another stream.
    /// - From the handshake, as [`StreamConnection::handshake`]:
    ///   [`Error::NoReply`] for a node that does not answer within
    ///   `timeout`, [`Error::AuthFailed`] for a refused key. The stream is
    ///   closed.
    pub fn connect_dpdk_on(
        mut nic: H,
        addr: SocketAddrV4,
        key: &SigningKey,
        socket_buffer: usize,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let handle: &mut NicHandle = nic.borrow_mut();
        let clock = handle.clock();
        let deadline_ns = clock.now_ns().saturating_add(duration_ns(timeout));
        let id = handle
            .dial(addr, socket_buffer, deadline_ns, || clock.now_ns())
            .map_err(|source| Error::Connect {
                addr: SocketAddr::V4(addr),
                source,
            })?;
        let stream = DpdkStream {
            peer: id.peer(),
            local_port: id.local_port(),
            nic,
            id: Some(id),
        };
        Self::handshake_on(
            stream,
            key,
            timeout,
            CachedClock::counting_from(clock.origin()),
        )
    }
}
