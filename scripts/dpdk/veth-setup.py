#!/usr/bin/env python3
# Build veth links for DPDK inside a network namespace, with nothing but
# Python's standard library: rtnetlink over AF_NETLINK, and the ethtool
# ioctl. Neither iproute2 nor ethtool is needed.
#
# Shared by the scripts that run DPDK on veth (dpdk-veth.sh,
# netns-runner.sh). Run it only inside a network namespace of your own
# (`unshare -rnm`): it brings `lo` up and creates interfaces, and on the
# host that would be the host's.
#
# Two shapes:
#
#   pair: one node, one client.
#
#     DPDK_IFACE  (no address: DPDK's userspace stack owns the node's IP)
#       <-> KERNEL_IFACE  (CLIENT_IP/PREFIX_LEN, for kernel-TCP clients)
#
#   bridge: several nodes and the client on one L2 segment, so the nodes
#   reach each other (replication) and the client reaches every node.
#
#     BRIDGE  (CLIENT_IP/PREFIX_LEN, MAC CLIENT_MAC: the kernel side)
#       +-- <DPDK_IFACE>-br <-> DPDK_IFACE (MAC as given, no address)
#       +-- ...one veth pair per node slot
#
#     MACs are set rather than left random so the layout can be published
#     before any node runs: a DPDK replica dials its primary with the
#     primary's MAC already known.
#
# Every interface comes up, and `lo` too, for the endpoints that stay on
# kernel TCP.
#
# TX checksum offload is switched off on both ends of every veth. With it
# on, the kernel hands frames to af_packet with the TCP checksum only
# partially computed (the NIC was meant to finish it), the userspace stack
# drops them, and every connect times out. A bridge port with the offload
# off makes the kernel finish the checksum of whatever the bridge forwards
# through it, so the bridge itself needs nothing.
#
# Usage:
#   veth-setup.py DPDK_IFACE KERNEL_IFACE CLIENT_IP/PREFIX_LEN
#   veth-setup.py --bridge BRIDGE CLIENT_IP/PREFIX_LEN CLIENT_MAC DPDK_IFACE=MAC...
#
# Exits non-zero, saying which step failed, on any error.

import ctypes
import fcntl
import socket
import struct
import sys

RTM_NEWLINK, RTM_NEWADDR, NLMSG_ERROR = 16, 20, 2
NLM_F_REQUEST, NLM_F_ACK, NLM_F_EXCL, NLM_F_CREATE = 0x1, 0x4, 0x200, 0x400
IFLA_ADDRESS, IFLA_IFNAME, IFLA_MASTER, IFLA_LINKINFO = 1, 3, 10, 18
IFLA_INFO_KIND, IFLA_INFO_DATA = 1, 2
VETH_INFO_PEER = 1
IFA_ADDRESS, IFA_LOCAL = 1, 2
IFF_UP = 0x1
SIOCETHTOOL, ETHTOOL_STXCSUM = 0x8946, 0x17
# Linux's IFNAMSIZ, NUL included.
IFNAMSIZ = 16
# Appended to a node slot's interface to name its bridge-side peer.
BRIDGE_PORT_SUFFIX = "-br"


def attr(kind, data):
    length = 4 + len(data)
    return struct.pack("HH", length, kind) + data + b"\0" * ((4 - length % 4) % 4)


def ifinfo(index=0, flags=0, change=0):
    return struct.pack("BxHiII", socket.AF_UNSPEC, 0, index, flags, change)


def ifname(name):
    return attr(IFLA_IFNAME, name.encode() + b"\0")


def mac_bytes(mac):
    try:
        octets = bytes(int(octet, 16) for octet in mac.split(":"))
    except ValueError:
        octets = b""
    if len(octets) != 6:
        sys.exit(f"'{mac}' is not a MAC address")
    return octets


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

    def create_veth_pair(self, name, peer, mac=None):
        """`ip link add NAME [address MAC] type veth peer name PEER`."""
        peer_info = ifinfo() + ifname(peer)
        linkinfo = attr(IFLA_INFO_KIND, b"veth") + attr(
            IFLA_INFO_DATA, attr(VETH_INFO_PEER, peer_info)
        )
        body = ifinfo() + ifname(name)
        if mac is not None:
            body += attr(IFLA_ADDRESS, mac_bytes(mac))
        self.request(
            f"create {name}/{peer} (is the veth driver available?)",
            RTM_NEWLINK,
            NLM_F_CREATE | NLM_F_EXCL,
            body + attr(IFLA_LINKINFO, linkinfo),
        )

    def create_bridge(self, name, mac):
        """`ip link add NAME address MAC type bridge` (STP off, the default)."""
        self.request(
            f"create bridge {name} (is the bridge driver loaded? a user namespace cannot load it)",
            RTM_NEWLINK,
            NLM_F_CREATE | NLM_F_EXCL,
            ifinfo()
            + ifname(name)
            + attr(IFLA_ADDRESS, mac_bytes(mac))
            + attr(IFLA_LINKINFO, attr(IFLA_INFO_KIND, b"bridge")),
        )

    def enslave(self, name, bridge):
        """`ip link set NAME master BRIDGE`."""
        self.request(
            f"add {name} to {bridge}",
            RTM_NEWLINK,
            0,
            ifinfo(socket.if_nametoindex(name))
            + attr(IFLA_MASTER, struct.pack("I", socket.if_nametoindex(bridge))),
        )

    def set_up(self, name):
        """`ip link set NAME up`."""
        self.request(
            f"bring {name} up", RTM_NEWLINK, 0, ifinfo(socket.if_nametoindex(name), IFF_UP, IFF_UP)
        )

    def add_ipv4(self, name, ip, prefix_len):
        """`ip addr add IP/PREFIX_LEN dev NAME`."""
        addr = socket.inet_aton(ip)
        self.request(
            f"address {name}",
            RTM_NEWADDR,
            NLM_F_CREATE | NLM_F_EXCL,
            struct.pack("BBBBI", socket.AF_INET, prefix_len, 0, 0, socket.if_nametoindex(name))
            + attr(IFA_LOCAL, addr)
            + attr(IFA_ADDRESS, addr),
        )


def tx_checksum_off(names):
    ioctl_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for name in names:
        value = ctypes.create_string_buffer(struct.pack("II", ETHTOOL_STXCSUM, 0))
        try:
            fcntl.ioctl(
                ioctl_sock, SIOCETHTOOL, struct.pack("16sP", name.encode(), ctypes.addressof(value))
            )
        except OSError as e:
            sys.exit(f"switch TX checksum offload off on {name}: {e}")


def client_address(arg):
    if "/" not in arg:
        sys.exit(f"'{arg}' is not CLIENT_IP/PREFIX_LEN")
    ip, prefix_len = arg.split("/")
    return ip, int(prefix_len)


def pair(dpdk_iface, kernel_iface, client):
    client_ip, prefix_len = client_address(client)
    nl = Netlink()
    nl.create_veth_pair(dpdk_iface, kernel_iface)
    for name in ("lo", dpdk_iface, kernel_iface):
        nl.set_up(name)
    nl.add_ipv4(kernel_iface, client_ip, prefix_len)
    tx_checksum_off((dpdk_iface, kernel_iface))


def bridge(name, client, client_mac, slots):
    client_ip, prefix_len = client_address(client)
    nodes = []
    for slot in slots:
        if "=" not in slot:
            sys.exit(f"'{slot}' is not DPDK_IFACE=MAC")
        iface, mac = slot.split("=", 1)
        port = iface + BRIDGE_PORT_SUFFIX
        if len(port) >= IFNAMSIZ:
            sys.exit(f"interface name {iface} is too long to name its bridge port {port}")
        nodes.append((iface, mac, port))

    veths = [end for iface, _, port in nodes for end in (iface, port)]
    nl = Netlink()
    nl.create_bridge(name, client_mac)
    for iface, mac, port in nodes:
        nl.create_veth_pair(iface, port, mac)
        nl.enslave(port, name)
    for link in ["lo", name] + veths:
        nl.set_up(link)
    nl.add_ipv4(name, client_ip, prefix_len)
    tx_checksum_off(veths)


def main():
    args = sys.argv[1:]
    if len(args) >= 5 and args[0] == "--bridge":
        bridge(args[1], args[2], args[3], args[4:])
    elif len(args) == 3 and not args[0].startswith("-"):
        pair(*args)
    else:
        sys.exit(
            "usage: veth-setup.py DPDK_IFACE KERNEL_IFACE CLIENT_IP/PREFIX_LEN\n"
            "       veth-setup.py --bridge BRIDGE CLIENT_IP/PREFIX_LEN CLIENT_MAC DPDK_IFACE=MAC..."
        )


if __name__ == "__main__":
    main()
