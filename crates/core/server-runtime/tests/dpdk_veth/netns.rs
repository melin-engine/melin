//! The namespace's plumbing, with nothing but `libc`: a veth pair over
//! rtnetlink, TX checksum offload switched off through the ethtool ioctl,
//! and a private tmpfs for DPDK's runtime directory.
//!
//! Hand-rolled rather than through a netlink crate because the whole of it
//! is three requests and one ioctl, and a dependency for that would carry
//! more surface than it saves. Hand-rolled rather than through `ip` and
//! `ethtool` because neither is guaranteed on a test host, and a test that
//! needs a tool the host may lack fails for the wrong reason.
//!
//! Only ever called in the re-executed child, which is the user namespace's
//! root and owns the network namespace it runs in: nothing here can touch
//! the host's own interfaces.

use std::ffi::CString;
use std::io;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

/// The DPDK side of the pair: no address, as the userspace stack on top
/// of it owns its IP.
pub const DPDK_IFACE: &str = "veth0";
/// The kernel side of the pair, carrying the client's address.
pub const KERNEL_IFACE: &str = "veth1";

/// `VETH_INFO_PEER` from `<linux/veth.h>`, which `libc` does not carry.
const VETH_INFO_PEER: u16 = 1;
/// `ETHTOOL_STXCSUM` from `<linux/ethtool.h>`, which `libc` does not carry.
const ETHTOOL_STXCSUM: u32 = 0x17;
/// `struct nlmsghdr` is four 32-bit-aligned fields: 16 bytes.
const NLMSG_HDRLEN: usize = 16;

/// Build the network the test runs on: `lo` up (the health endpoint and
/// nothing else uses it), a veth pair with both ends up, `client_ip` on
/// the kernel end, and TX checksum offload off on both ends.
///
/// The offload matters: with it on, the kernel hands frames to af_packet
/// with the TCP checksum only partially computed (the NIC was meant to
/// finish it), a userspace stack reading them as wire frames drops them,
/// and every connect times out.
pub fn build(client_ip: Ipv4Addr, prefix_len: u8) -> Result<(), String> {
    let mut nl = Netlink::open().map_err(|e| format!("rtnetlink socket: {e}"))?;
    nl.create_veth_pair(DPDK_IFACE, KERNEL_IFACE).map_err(|e| {
        format!(
            "creating the {DPDK_IFACE}/{KERNEL_IFACE} veth pair: {e} (is the veth driver \
             available, and does the namespace grant CAP_NET_ADMIN?)"
        )
    })?;
    for name in ["lo", DPDK_IFACE, KERNEL_IFACE] {
        nl.set_up(name)
            .map_err(|e| format!("bringing {name} up: {e}"))?;
    }
    nl.add_ipv4(KERNEL_IFACE, client_ip, prefix_len)
        .map_err(|e| format!("adding {client_ip}/{prefix_len} to {KERNEL_IFACE}: {e}"))?;
    for name in [DPDK_IFACE, KERNEL_IFACE] {
        tx_checksum_off(name)
            .map_err(|e| format!("switching TX checksum offload off on {name}: {e}"))?;
    }
    Ok(())
}

/// Mount a private tmpfs on `/var/run`. EAL insists on creating
/// `/var/run/dpdk`, and inside the user namespace it believes it is root,
/// so it does not fall back to a per-user directory; the host's own
/// `/var/run` is not ours to write. The mount namespace keeps this mount
/// invisible to the host and gone with the process.
pub fn private_var_run() -> Result<(), String> {
    let target = c"/var/run";
    let fstype = c"tmpfs";
    // SAFETY: every pointer is a valid NUL-terminated string that outlives
    // the call; a null `data` asks for the filesystem's defaults.
    let rc = unsafe {
        libc::mount(
            fstype.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            0,
            std::ptr::null(),
        )
    };
    if rc != 0 {
        return Err(format!(
            "mounting a tmpfs on /var/run: {} (does the namespace include a mount namespace?)",
            io::Error::last_os_error()
        ));
    }
    Ok(())
}

/// One rtnetlink socket, talking to the kernel.
struct Netlink {
    fd: OwnedFd,
    /// Sequence number of the last request. `u32` because that is the
    /// width of `nlmsg_seq`; a handful of requests never wraps it.
    seq: u32,
}

impl Netlink {
    fn open() -> io::Result<Self> {
        // SAFETY: plain socket(2); the result is checked before use.
        let fd = unsafe {
            libc::socket(
                libc::AF_NETLINK,
                libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                libc::NETLINK_ROUTE,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `fd` is a freshly opened descriptor that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        Ok(Netlink { fd, seq: 0 })
    }

    /// `ip link add <a> type veth peer name <b>`.
    fn create_veth_pair(&mut self, a: &str, b: &str) -> io::Result<()> {
        let mut peer = ifinfomsg(0, 0, 0);
        push_attr(&mut peer, libc::IFLA_IFNAME, &nul_terminated(b));
        let mut info_data = Vec::new();
        push_attr(&mut info_data, VETH_INFO_PEER, &peer);
        let mut link_info = Vec::new();
        push_attr(&mut link_info, libc::IFLA_INFO_KIND, b"veth");
        push_attr(&mut link_info, libc::IFLA_INFO_DATA, &info_data);

        let mut body = ifinfomsg(0, 0, 0);
        push_attr(&mut body, libc::IFLA_IFNAME, &nul_terminated(a));
        push_attr(&mut body, libc::IFLA_LINKINFO, &link_info);
        self.request(
            libc::RTM_NEWLINK,
            (libc::NLM_F_CREATE | libc::NLM_F_EXCL) as u16,
            &body,
        )
    }

    /// `ip link set <name> up`.
    fn set_up(&mut self, name: &str) -> io::Result<()> {
        let index = if_index(name)?;
        let up = libc::IFF_UP as u32;
        self.request(libc::RTM_NEWLINK, 0, &ifinfomsg(index, up, up))
    }

    /// `ip addr add <ip>/<prefix_len> dev <name>`.
    fn add_ipv4(&mut self, name: &str, ip: Ipv4Addr, prefix_len: u8) -> io::Result<()> {
        let index = if_index(name)?;
        // struct ifaddrmsg: family, prefixlen, flags, scope (u8 each), index (u32).
        let mut body = vec![libc::AF_INET as u8, prefix_len, 0, 0];
        body.extend_from_slice(&(index as u32).to_ne_bytes());
        push_attr(&mut body, libc::IFA_LOCAL, &ip.octets());
        push_attr(&mut body, libc::IFA_ADDRESS, &ip.octets());
        self.request(
            libc::RTM_NEWADDR,
            (libc::NLM_F_CREATE | libc::NLM_F_EXCL) as u16,
            &body,
        )
    }

    /// Send one request with `NLM_F_ACK` and wait for the kernel's answer,
    /// an `NLMSG_ERROR` whose errno is zero on success.
    fn request(&mut self, msg_type: u16, flags: u16, body: &[u8]) -> io::Result<()> {
        self.seq += 1;
        let len = NLMSG_HDRLEN + body.len();
        let mut msg = Vec::with_capacity(len);
        msg.extend_from_slice(&(len as u32).to_ne_bytes());
        msg.extend_from_slice(&msg_type.to_ne_bytes());
        let flags = flags | (libc::NLM_F_REQUEST | libc::NLM_F_ACK) as u16;
        msg.extend_from_slice(&flags.to_ne_bytes());
        msg.extend_from_slice(&self.seq.to_ne_bytes());
        // nlmsg_pid: 0 lets the kernel assign the port.
        msg.extend_from_slice(&0u32.to_ne_bytes());
        msg.extend_from_slice(body);

        // An unconnected netlink socket sends to the kernel (port 0).
        // SAFETY: `msg` is a live buffer of `msg.len()` bytes.
        let sent = unsafe { libc::send(self.fd.as_raw_fd(), msg.as_ptr().cast(), msg.len(), 0) };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }

        // The ack is a header and a `struct nlmsgerr` (errno, then the
        // request's header echoed back, and our body after it unless the
        // kernel caps it): a page holds it.
        let mut reply = [0u8; 4096];
        // SAFETY: `reply` is a live, writable buffer of its full length.
        let received = unsafe {
            libc::recv(
                self.fd.as_raw_fd(),
                reply.as_mut_ptr().cast(),
                reply.len(),
                0,
            )
        };
        if received < 0 {
            return Err(io::Error::last_os_error());
        }
        let reply = &reply[..received as usize];
        let field = |at: usize, what: &str| -> io::Result<[u8; 4]> {
            reply
                .get(at..at + 4)
                .and_then(|b| b.try_into().ok())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("netlink reply of {received} bytes has no {what}"),
                    )
                })
        };
        let header = field(4, "message type")?;
        let reply_type = u16::from_ne_bytes([header[0], header[1]]);
        if reply_type != libc::NLMSG_ERROR as u16 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("expected an NLMSG_ERROR ack, got message type {reply_type}"),
            ));
        }
        match i32::from_ne_bytes(field(NLMSG_HDRLEN, "errno")?) {
            0 => Ok(()),
            negative_errno => Err(io::Error::from_raw_os_error(-negative_errno)),
        }
    }
}

/// `struct ifinfomsg`: family, pad (u8 each), type (u16), index (i32),
/// flags, change (u32 each) — 16 bytes, so attributes after it stay
/// 4-byte aligned.
fn ifinfomsg(index: i32, flags: u32, change: u32) -> Vec<u8> {
    let mut msg = vec![libc::AF_UNSPEC as u8, 0];
    msg.extend_from_slice(&0u16.to_ne_bytes());
    msg.extend_from_slice(&index.to_ne_bytes());
    msg.extend_from_slice(&flags.to_ne_bytes());
    msg.extend_from_slice(&change.to_ne_bytes());
    msg
}

/// Append one `struct rtattr` (length, type, payload) and pad to the
/// 4-byte boundary the next one must start on. Every buffer this is
/// called on starts aligned, so padding on its own length is enough.
fn push_attr(buf: &mut Vec<u8>, kind: u16, payload: &[u8]) {
    let len = 4 + payload.len();
    buf.extend_from_slice(&(len as u16).to_ne_bytes());
    buf.extend_from_slice(&kind.to_ne_bytes());
    buf.extend_from_slice(payload);
    buf.resize(buf.len().next_multiple_of(4), 0);
}

fn nul_terminated(name: &str) -> Vec<u8> {
    let mut bytes = name.as_bytes().to_vec();
    bytes.push(0);
    bytes
}

fn if_index(name: &str) -> io::Result<i32> {
    let c_name = CString::new(name)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
    // SAFETY: `c_name` is a valid NUL-terminated string for the call.
    match unsafe { libc::if_nametoindex(c_name.as_ptr()) } {
        0 => Err(io::Error::last_os_error()),
        index => Ok(index as i32),
    }
}

/// `ethtool -K <name> tx off`, through `SIOCETHTOOL`.
fn tx_checksum_off(name: &str) -> io::Result<()> {
    /// `struct ethtool_value`: a command and its value.
    #[repr(C)]
    struct EthtoolValue {
        cmd: u32,
        data: u32,
    }

    // SAFETY: plain socket(2); the result is checked before use.
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fd` is a freshly opened descriptor that nothing else owns.
    let sock = unsafe { OwnedFd::from_raw_fd(fd) };

    let mut value = EthtoolValue {
        cmd: ETHTOOL_STXCSUM,
        data: 0,
    };
    // SAFETY: `ifreq` is plain old data; all-zero is a valid value.
    let mut ifr: libc::ifreq = unsafe { std::mem::zeroed() };
    if name.len() >= ifr.ifr_name.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("interface name {name} is too long"),
        ));
    }
    for (dst, &src) in ifr.ifr_name.iter_mut().zip(name.as_bytes()) {
        *dst = src as libc::c_char;
    }
    ifr.ifr_ifru.ifru_data = (&raw mut value).cast();
    // SAFETY: `ifr` names the interface and points at `value`, both live
    // for the call; SIOCETHTOOL reads the command and writes nothing back
    // for a set.
    let rc = unsafe { libc::ioctl(sock.as_raw_fd(), libc::SIOCETHTOOL as _, &mut ifr) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A nested attribute's length covers its padded children, and every
    /// attribute starts on a 4-byte boundary.
    #[test]
    fn attributes_are_padded_to_four_bytes() {
        let mut buf = Vec::new();
        push_attr(&mut buf, 3, b"veth1\0");
        assert_eq!(buf.len(), 12, "4 header + 6 payload, padded to 12");
        assert_eq!(
            u16::from_ne_bytes([buf[0], buf[1]]),
            10,
            "length excludes padding"
        );
        push_attr(&mut buf, 1, b"veth");
        assert_eq!(buf.len(), 20);
    }

    #[test]
    fn ifinfomsg_is_sixteen_bytes() {
        assert_eq!(ifinfomsg(7, 1, 1).len(), 16);
    }
}
