#![cfg_attr(not(test), deny(clippy::unwrap_used))]

//! A TCP connection to a Melin server, owned by this process and offered
//! to another through shared memory.
//!
//! The Aeron benchmark harness measures from a Java process; Melin's
//! kernel-bypass transport lives in a Rust one, needs root and a NIC of
//! its own, and is not something to load into a JVM. This is the seam
//! between them, shaped like the seam Aeron itself uses: the measuring
//! process writes into shared memory, and a separate process owns the
//! NIC. The Java side keeps its own framing, its own handshake and its
//! own clock; what crosses the seam is bytes.
//!
//! The file holds two rings and a state word (see `proxy/shm.rs` for the
//! layout). The loop here is one thread, pinned if asked, busy-spinning:
//! whatever the client has written goes to the socket, the stack is
//! serviced, whatever the server has answered goes back. Nothing is
//! framed and nothing is timed; a request written by the client is on
//! the wire in the iteration that finds it, and several written together
//! travel as one segment -- which is the coalescing a kernel does under
//! load, and what makes the packet rate a function of the backlog rather
//! than of the message rate.
//!
//! ```sh
//! shm-proxy --server 10.0.1.30:9876 --shm /dev/shm/melin-client.shm \
//!     --transport dpdk --dpdk-ip 10.0.1.33 --dpdk-peer-mac 02:...
//! ```
//!
//! Kernel TCP by default, which is how the link is tested on a machine
//! with no DPDK. Exit status: 0 when the client asked for the close or
//! the server ended the connection cleanly, 1 when the connection
//! failed mid-run, 2 when it could not be set up.

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, ValueEnum};

// A binary's root resolves `mod` against its own directory, so the
// modules under `proxy/` are named by path. The clock carries a few
// conversions the loop here has no use for.
#[cfg(any(feature = "dpdk", test))]
#[path = "proxy/arp.rs"]
mod arp;
#[cfg(feature = "dpdk")]
#[path = "proxy/dpdk.rs"]
mod dpdk;
#[path = "proxy/shm.rs"]
mod shm;
#[path = "proxy/transport.rs"]
mod transport;
#[allow(dead_code)]
#[path = "proxy/tsc.rs"]
mod tsc;

use shm::{SharedMemory, State};
use transport::{KernelTcp, Transport};
use tsc::{TscClock, rdtscp};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum TransportKind {
    Kernel,
    Dpdk,
}

#[derive(Parser)]
#[command(
    name = "shm-proxy",
    about = "Hold a connection to a Melin server and bridge it to shared memory"
)]
struct Cli {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:9876")]
    server: SocketAddr,
    /// The shared-memory file to create. Its directory should be a
    /// tmpfs; the file is world read-write so the client may run under
    /// another account.
    #[arg(long, default_value = "/dev/shm/melin-client.shm")]
    shm: PathBuf,
    #[arg(long, value_enum, default_value_t = TransportKind::Kernel)]
    transport: TransportKind,
    /// Capacity of the client-to-server ring, in KiB, a power of two.
    #[arg(long, default_value_t = 1024)]
    to_wire_kib: usize,
    /// Capacity of the server-to-client ring, in KiB, a power of two.
    #[arg(long, default_value_t = 1024)]
    from_wire_kib: usize,
    /// Pin the loop to this core.
    #[arg(long)]
    core: Option<usize>,
    #[cfg(feature = "dpdk")]
    #[command(flatten)]
    dpdk: dpdk::DpdkArgs,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(Failure::Setup(e)) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
        Err(Failure::Lost(e)) => {
            eprintln!("error: connection lost: {e}");
            ExitCode::from(1)
        }
    }
}

enum Failure {
    Setup(String),
    Lost(io::Error),
}

fn run(cli: Cli) -> Result<(), Failure> {
    if let Some(core) = cli.core {
        match melin_app::affinity::pin_to_core(core) {
            Ok(core) => eprintln!("pinned to core {core}"),
            Err(e) => eprintln!("warning: not pinned: {e}"),
        }
    }
    let clock = TscClock::calibrate();
    let mut link = SharedMemory::create(&cli.shm, cli.to_wire_kib * 1024, cli.from_wire_kib * 1024)
        .map_err(Failure::Setup)?;
    eprintln!(
        "link at {} ({} KiB to the wire, {} KiB from it)",
        cli.shm.display(),
        cli.to_wire_kib,
        cli.from_wire_kib
    );

    let result = match cli.transport {
        TransportKind::Kernel => match KernelTcp::connect(cli.server, CONNECT_TIMEOUT) {
            Ok(transport) => bridge(transport, &mut link, &cli, &clock),
            Err(e) => Err(Failure::Setup(format!(
                "cannot connect to {}: {e}",
                cli.server
            ))),
        },
        TransportKind::Dpdk => match connect_dpdk(&cli, &clock) {
            Ok(transport) => bridge(transport, &mut link, &cli, &clock),
            Err(e) => Err(Failure::Setup(e)),
        },
    };
    link.set_state(match result {
        Ok(()) => State::Closed,
        Err(Failure::Setup(_)) => State::Failed,
        Err(Failure::Lost(_)) => State::Closed,
    });
    result
}

#[cfg(feature = "dpdk")]
fn connect_dpdk(cli: &Cli, clock: &TscClock) -> Result<dpdk::DpdkTcp, String> {
    let server = match cli.server {
        SocketAddr::V4(v4) => v4,
        SocketAddr::V6(_) => return Err("the DPDK transport is IPv4 only".into()),
    };
    dpdk::DpdkTcp::connect(&cli.dpdk, server, clock)
}

#[cfg(not(feature = "dpdk"))]
fn connect_dpdk(_cli: &Cli, _clock: &TscClock) -> Result<KernelTcp, String> {
    Err("this shm-proxy was built without the dpdk feature; rebuild with --features dpdk".into())
}

/// Bridge until the client asks for the close or the server ends the
/// connection. `Ok` for either of those; `Err` when the connection
/// broke.
fn bridge<T: Transport>(
    mut transport: T,
    link: &mut SharedMemory,
    cli: &Cli,
    clock: &TscClock,
) -> Result<(), Failure> {
    link.set_state(State::Connected);
    eprintln!("connected to {} over {}", cli.server, transport.name());

    loop {
        // Client to server first, so a request found on this turn is on
        // the wire when the stack is serviced, not on the next.
        {
            let ring = link.outbound();
            let (first, second) = ring.readable();
            if !first.is_empty() {
                let mut sent = transport.send(first).map_err(Failure::Lost)?;
                if sent == first.len() && !second.is_empty() {
                    sent += transport.send(second).map_err(Failure::Lost)?;
                }
                ring.consumed(sent);
            }
        }

        transport.service(clock.unix_ns(rdtscp()));

        {
            let ring = link.inbound();
            let (first, second) = ring.writable();
            if !first.is_empty() {
                match receive(&mut transport, first, second) {
                    Ok(n) => ring.produced(n),
                    Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                        eprintln!("the server closed the connection");
                        return Ok(());
                    }
                    Err(e) => return Err(Failure::Lost(e)),
                }
            }
        }

        if link.close_requested() {
            eprintln!("close requested by the client");
            return Ok(());
        }
    }
}

/// Fill `first`, and `second` only if `first` filled entirely.
fn receive<T: Transport>(
    transport: &mut T,
    first: &mut [u8],
    second: &mut [u8],
) -> io::Result<usize> {
    let mut n = transport.recv(first)?;
    if n == first.len() && !second.is_empty() {
        n += transport.recv(second)?;
    }
    Ok(n)
}
