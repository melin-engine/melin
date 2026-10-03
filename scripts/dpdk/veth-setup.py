#!/usr/bin/env python3
# Build a veth pair for DPDK inside a network namespace, with nothing but
# Python's standard library: rtnetlink over AF_NETLINK, and the ethtool
# ioctl. Neither iproute2 nor ethtool is needed.
#
# Shared by the scripts that run DPDK on veth (dpdk-veth.sh,
# netns-runner.sh). Run it only inside a network namespace of your own
# (`unshare -rnm`): it brings `lo` up and creates interfaces, and on the
# host that would be the host's.
#
# What it builds:
#
#   DPDK_IFACE  (no address: DPDK's userspace stack owns the node's IP)
#     <-> KERNEL_IFACE  (CLIENT_IP/PREFIX_LEN, for kernel-TCP clients)
#
# both ends up, and `lo` up for the endpoints that stay on kernel TCP.
#
# TX checksum offload is switched off on both ends. With it on, the kernel
# hands frames to af_packet with the TCP checksum only partially computed
# (the NIC was meant to finish it), the userspace stack drops them, and
# every connect times out.
#
# Usage:
#   veth-setup.py DPDK_IFACE KERNEL_IFACE CLIENT_IP/PREFIX_LEN
#
# Exits non-zero, saying which step failed, on any error.

import ctypes
import fcntl
import socket
import struct
import sys

RTM_NEWLINK, RTM_NEWADDR, NLMSG_ERROR = 16, 20, 2
NLM_F_REQUEST, NLM_F_ACK, NLM_F_EXCL, NLM_F_CREATE = 0x1, 0x4, 0x200, 0x400
IFLA_IFNAME, IFLA_LINKINFO, IFLA_INFO_KIND, IFLA_INFO_DATA = 3, 18, 1, 2
VETH_INFO_PEER = 1
IFA_ADDRESS, IFA_LOCAL = 1, 2
IFF_UP = 0x1
SIOCETHTOOL, ETHTOOL_STXCSUM = 0x8946, 0x17


def attr(kind, data):
    length = 4 + len(data)
    return struct.pack("HH", length, kind) + data + b"\0" * ((4 - length % 4) % 4)


def ifinfo(index=0, flags=0, change=0):
    return struct.pack("BxHiII", socket.AF_UNSPEC, 0, index, flags, change)


class Netlink:
    def __init__(self):
        self.sock = socket.socket(socket.AF_NETLINK, socket.SOCK_RAW, socket.NETLINK_ROUTE)
        self.sock.bind((0, 0))
        self.seq = 0

    def request(self, what, msg_type, flags, body):
        self.seq += 1
        flags |= NLM_F_REQUEST | NLM_F_ACK
        self.sock.send(struct.pack("IHHII", 16 + len(body), msg_type, flags, self.seq, 0) + body)
        reply = self.sock.recv(65536)
        if struct.unpack_from("H", reply, 4)[0] != NLMSG_ERROR:
            sys.exit(f"{what}: unexpected netlink reply")
        errno = -struct.unpack_from("i", reply, 16)[0]
        if errno:
            sys.exit(f"{what}: errno {errno}")


def main():
    if len(sys.argv) != 4 or "/" not in sys.argv[3]:
        sys.exit("usage: veth-setup.py DPDK_IFACE KERNEL_IFACE CLIENT_IP/PREFIX_LEN")
    dpdk_iface, kernel_iface = sys.argv[1], sys.argv[2]
    client_ip, prefix_len = sys.argv[3].split("/")
    prefix_len = int(prefix_len)

    nl = Netlink()
    peer = ifinfo() + attr(IFLA_IFNAME, kernel_iface.encode() + b"\0")
    linkinfo = attr(IFLA_INFO_KIND, b"veth") + attr(IFLA_INFO_DATA, attr(VETH_INFO_PEER, peer))
    nl.request(
        f"create {dpdk_iface}/{kernel_iface} (is the veth driver available?)",
        RTM_NEWLINK,
        NLM_F_CREATE | NLM_F_EXCL,
        ifinfo() + attr(IFLA_IFNAME, dpdk_iface.encode() + b"\0") + attr(IFLA_LINKINFO, linkinfo),
    )
    for name in ("lo", dpdk_iface, kernel_iface):
        nl.request(
            f"bring {name} up", RTM_NEWLINK, 0, ifinfo(socket.if_nametoindex(name), IFF_UP, IFF_UP)
        )
    addr = socket.inet_aton(client_ip)
    nl.request(
        f"address {kernel_iface}",
        RTM_NEWADDR,
        NLM_F_CREATE | NLM_F_EXCL,
        struct.pack("BBBBI", socket.AF_INET, prefix_len, 0, 0, socket.if_nametoindex(kernel_iface))
        + attr(IFA_LOCAL, addr)
        + attr(IFA_ADDRESS, addr),
    )

    ioctl_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for name in (dpdk_iface, kernel_iface):
        value = ctypes.create_string_buffer(struct.pack("II", ETHTOOL_STXCSUM, 0))
        try:
            fcntl.ioctl(
                ioctl_sock, SIOCETHTOOL, struct.pack("16sP", name.encode(), ctypes.addressof(value))
            )
        except OSError as e:
            sys.exit(f"switch TX checksum offload off on {name}: {e}")


if __name__ == "__main__":
    main()
