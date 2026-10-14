/* Wrapper functions for DPDK static inline functions.
 *
 * DPDK performance-critical functions (rx_burst, tx_burst, pktmbuf_alloc,
 * pktmbuf_free) are defined as static inline in headers. bindgen cannot
 * generate bindings for these. We wrap them in non-inline C functions
 * that get compiled and linked into the Rust binary.
 */

#include <rte_errno.h>
#include <rte_ethdev.h>
#include <rte_flow.h>
#include <rte_lcore.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "steering_rule.h"

uint16_t dpdk_eth_rx_burst(uint16_t port_id, uint16_t queue_id,
                           struct rte_mbuf **rx_pkts, uint16_t nb_pkts) {
    return rte_eth_rx_burst(port_id, queue_id, rx_pkts, nb_pkts);
}

uint16_t dpdk_eth_tx_burst(uint16_t port_id, uint16_t queue_id,
                           struct rte_mbuf **tx_pkts, uint16_t nb_pkts) {
    return rte_eth_tx_burst(port_id, queue_id, tx_pkts, nb_pkts);
}

struct rte_mbuf *dpdk_pktmbuf_alloc(struct rte_mempool *mp) {
    return rte_pktmbuf_alloc(mp);
}

void dpdk_pktmbuf_free(struct rte_mbuf *m) {
    rte_pktmbuf_free(m);
}

void dpdk_mbuf_refcnt_update(struct rte_mbuf *m, int16_t value) {
    rte_mbuf_refcnt_update(m, value);
}

void dpdk_mempool_free(struct rte_mempool *mp) {
    rte_mempool_free(mp);
}

/* Return RTE_MBUF_DEFAULT_BUF_SIZE as a function since it's a macro. */
uint16_t dpdk_mbuf_default_buf_size(void) {
    return RTE_MBUF_DEFAULT_BUF_SIZE;
}

/* Accessors for rte_mbuf fields that may be in unions/bitfields. */
uint16_t dpdk_mbuf_data_off(const struct rte_mbuf *m) {
    return m->data_off;
}

uint16_t dpdk_mbuf_data_len(const struct rte_mbuf *m) {
    return m->data_len;
}

void dpdk_mbuf_set_data_len(struct rte_mbuf *m, uint16_t len) {
    m->data_len = len;
}

uint32_t dpdk_mbuf_pkt_len(const struct rte_mbuf *m) {
    return m->pkt_len;
}

void dpdk_mbuf_set_pkt_len(struct rte_mbuf *m, uint32_t len) {
    m->pkt_len = len;
}

/* Return buf_addr as void* to avoid platform-specific char signedness
 * issues (char is signed on x86_64, unsigned on aarch64). Rust callers
 * cast to *mut u8 / *const u8 as needed. */
void *dpdk_mbuf_buf_addr(const struct rte_mbuf *m) {
    return m->buf_addr;
}

/* --- Offload flag accessors ---
 * ol_flags is a uint64_t but lives alongside bitfields in a union,
 * so we wrap it to avoid bindgen layout issues. */
uint64_t dpdk_mbuf_ol_flags(const struct rte_mbuf *m) {
    return m->ol_flags;
}

void dpdk_mbuf_set_ol_flags(struct rte_mbuf *m, uint64_t flags) {
    m->ol_flags = flags;
}

/* --- TX offload header length setters ---
 * l2_len and l3_len are bitfields inside a union — the NIC needs these
 * to locate IP/TCP headers for hardware checksum computation. */
void dpdk_mbuf_set_tx_offload(struct rte_mbuf *m,
                               uint64_t l2_len, uint64_t l3_len,
                               uint64_t l4_len) {
    m->l2_len = l2_len;
    m->l3_len = l3_len;
    m->l4_len = l4_len;
}

/* --- TX offload flag constants ---
 * Macros can't be accessed by bindgen, so expose them as functions. */
uint64_t dpdk_tx_offload_ipv4_cksum(void) {
    return RTE_MBUF_F_TX_IP_CKSUM | RTE_MBUF_F_TX_IPV4;
}

uint64_t dpdk_tx_offload_tcp_cksum(void) {
    return RTE_MBUF_F_TX_TCP_CKSUM;
}

/* --- RX checksum verdict flags ---
 * With RX checksum offload on, the NIC does not drop a frame whose
 * checksum fails: it delivers it and records the verdict in ol_flags,
 * under these masks. Within each mask, zero means UNKNOWN (not checked). */
uint64_t dpdk_rx_ip_cksum_mask(void) {
    return RTE_MBUF_F_RX_IP_CKSUM_MASK;
}

uint64_t dpdk_rx_ip_cksum_good(void) {
    return RTE_MBUF_F_RX_IP_CKSUM_GOOD;
}

uint64_t dpdk_rx_ip_cksum_bad(void) {
    return RTE_MBUF_F_RX_IP_CKSUM_BAD;
}

uint64_t dpdk_rx_ip_cksum_none(void) {
    return RTE_MBUF_F_RX_IP_CKSUM_NONE;
}

uint64_t dpdk_rx_l4_cksum_mask(void) {
    return RTE_MBUF_F_RX_L4_CKSUM_MASK;
}

uint64_t dpdk_rx_l4_cksum_good(void) {
    return RTE_MBUF_F_RX_L4_CKSUM_GOOD;
}

uint64_t dpdk_rx_l4_cksum_bad(void) {
    return RTE_MBUF_F_RX_L4_CKSUM_BAD;
}

uint64_t dpdk_rx_l4_cksum_none(void) {
    return RTE_MBUF_F_RX_L4_CKSUM_NONE;
}

/* --- RX/TX ethdev offload capability constants --- */
uint64_t dpdk_rx_offload_checksum(void) {
    return RTE_ETH_RX_OFFLOAD_IPV4_CKSUM |
           RTE_ETH_RX_OFFLOAD_TCP_CKSUM;
}

uint64_t dpdk_tx_offload_checksum(void) {
    return RTE_ETH_TX_OFFLOAD_IPV4_CKSUM |
           RTE_ETH_TX_OFFLOAD_TCP_CKSUM;
}

/* --- VLAN offload constants --- */
uint64_t dpdk_rx_offload_vlan_strip(void) {
    return RTE_ETH_RX_OFFLOAD_VLAN_STRIP;
}

uint64_t dpdk_tx_offload_vlan_insert(void) {
    return RTE_ETH_TX_OFFLOAD_VLAN_INSERT;
}

/* TX VLAN flag for ol_flags — tells the NIC to insert a VLAN tag. */
uint64_t dpdk_tx_vlan_flag(void) {
    return RTE_MBUF_F_TX_VLAN;
}

/* Set the VLAN TCI (Tag Control Information) on an mbuf for TX insert. */
void dpdk_mbuf_set_vlan_tci(struct rte_mbuf *m, uint16_t vlan_tci) {
    m->vlan_tci = vlan_tci;
}

/* RSS (Receive Side Scaling) constants — exposed as functions because
   bindgen cannot access C preprocessor macros. */
uint64_t dpdk_eth_rss_ip(void) {
    return RTE_ETH_RSS_IP;
}

uint64_t dpdk_eth_rss_tcp(void) {
    return RTE_ETH_RSS_TCP;
}

uint64_t dpdk_eth_mq_rx_rss(void) {
    return RTE_ETH_MQ_RX_RSS;
}

/* --- rte_flow wrappers ---
 * Used for bifurcated PMD mode (mlx5) where DPDK coexists with the
 * kernel netdev. The PMD must be put in isolated mode before configure
 * so it does NOT install its default RSS catch-all; specific flow rules
 * then steer matching traffic into DPDK queues, and everything else
 * stays with the kernel. */

/* Enable flow isolation on a port. Must be called BEFORE
 * rte_eth_dev_configure() for PMDs like mlx5 that apply the
 * restriction at device-open time. Returns 0 on success or a positive
 * DPDK error code. */
int dpdk_flow_isolate(uint16_t port_id) {
    struct rte_flow_error err;
    memset(&err, 0, sizeof(err));
    return rte_flow_isolate(port_id, 1, &err);
}

/* Install `rule` (see steering_rule.h) on `port_id`. The pattern is ETH (any) / IPV4 (the
 * addresses given, or any) / TCP (the ports given; only when a port is)
 * / END, and the one action QUEUE. Without TCP ports this is the rule
 * bifurcated transports have always installed: IPv4 from the peer into
 * queue 0, so the kernel keeps SSH (different source IP), ARP, and
 * anything else.
 *
 * The flow handle is not kept: the rule lives as long as the port, and
 * goes when the port is closed.
 *
 * Returns 0 on success, a negative value on failure; on failure stores
 * the rte_flow error type in *err_type and its message (empty when the
 * PMD gives none) in err_msg, NUL-terminated and truncated to
 * err_msg_len bytes. */
int dpdk_install_steering_rule(uint16_t port_id,
                               const struct dpdk_steering_rule *rule,
                               int *err_type, char *err_msg,
                               size_t err_msg_len) {
    struct rte_flow_attr attr;
    struct rte_flow_item_eth eth_spec, eth_mask;
    struct rte_flow_item_ipv4 ipv4_spec, ipv4_mask;
    struct rte_flow_item_tcp tcp_spec, tcp_mask;
    struct rte_flow_item pattern[4];
    struct rte_flow_action_queue queue;
    struct rte_flow_action actions[2];
    struct rte_flow_error err;
    struct rte_flow *flow;
    int n = 0;

    memset(&attr, 0, sizeof(attr));
    attr.ingress = 1;
    attr.priority = rule->priority;

    memset(&eth_spec, 0, sizeof(eth_spec));
    memset(&eth_mask, 0, sizeof(eth_mask));

    memset(&ipv4_spec, 0, sizeof(ipv4_spec));
    memset(&ipv4_mask, 0, sizeof(ipv4_mask));
    if (rule->has_src_ipv4) {
        ipv4_spec.hdr.src_addr = rule->src_ipv4_be;
        ipv4_mask.hdr.src_addr = 0xffffffff;
    }
    if (rule->has_dst_ipv4) {
        ipv4_spec.hdr.dst_addr = rule->dst_ipv4_be;
        ipv4_mask.hdr.dst_addr = 0xffffffff;
    }

    memset(&tcp_spec, 0, sizeof(tcp_spec));
    memset(&tcp_mask, 0, sizeof(tcp_mask));
    if (rule->has_tcp_src_port) {
        tcp_spec.hdr.src_port = rule->tcp_src_port_be;
        tcp_mask.hdr.src_port = 0xffff;
    }
    if (rule->has_tcp_dst_port) {
        tcp_spec.hdr.dst_port = rule->tcp_dst_port_be;
        tcp_mask.hdr.dst_port = 0xffff;
    }

    memset(pattern, 0, sizeof(pattern));
    pattern[n].type = RTE_FLOW_ITEM_TYPE_ETH;
    pattern[n].spec = &eth_spec;
    pattern[n].mask = &eth_mask;
    n++;
    pattern[n].type = RTE_FLOW_ITEM_TYPE_IPV4;
    pattern[n].spec = &ipv4_spec;
    pattern[n].mask = &ipv4_mask;
    n++;
    if (rule->has_tcp_src_port || rule->has_tcp_dst_port) {
        pattern[n].type = RTE_FLOW_ITEM_TYPE_TCP;
        pattern[n].spec = &tcp_spec;
        pattern[n].mask = &tcp_mask;
        n++;
    }
    pattern[n].type = RTE_FLOW_ITEM_TYPE_END;

    memset(&queue, 0, sizeof(queue));
    queue.index = rule->queue;

    memset(actions, 0, sizeof(actions));
    actions[0].type = RTE_FLOW_ACTION_TYPE_QUEUE;
    actions[0].conf = &queue;
    actions[1].type = RTE_FLOW_ACTION_TYPE_END;

    memset(&err, 0, sizeof(err));
    flow = rte_flow_create(port_id, &attr, pattern, actions, &err);
    if (!flow) {
        if (err_type) {
            *err_type = (int)err.type;
        }
        if (err_msg && err_msg_len > 0) {
            snprintf(err_msg, err_msg_len, "%s", err.message ? err.message : "");
        }
        return -1;
    }
    return 0;
}

/* Point every entry of `port_id`'s RSS redirection table at `queue`, so
 * that whatever the default RSS receives lands there, however it hashes.
 * Call after rte_eth_dev_start(): the table's size is known then on every
 * PMD (mlx5 reports its real size only once started), and a PMD that
 * rebuilds the table on start cannot undo the update. Needs the port
 * configured with RTE_ETH_MQ_RX_RSS, which ethdev requires for any RETA
 * update.
 *
 * Returns 0 on success, -ENOTSUP when the port has no redirection table
 * to update, or the negative error of rte_eth_dev_info_get() or
 * rte_eth_dev_rss_reta_update(). */
int dpdk_pin_rss_reta(uint16_t port_id, uint16_t queue) {
    struct rte_eth_dev_info info;
    struct rte_eth_rss_reta_entry64 *conf;
    uint16_t groups, g, i;
    int ret;

    ret = rte_eth_dev_info_get(port_id, &info);
    if (ret != 0) {
        return ret;
    }
    if (info.reta_size == 0) {
        return -ENOTSUP;
    }
    groups = (info.reta_size + RTE_ETH_RETA_GROUP_SIZE - 1) / RTE_ETH_RETA_GROUP_SIZE;
    conf = calloc(groups, sizeof(*conf));
    if (!conf) {
        return -ENOMEM;
    }
    for (g = 0; g < groups; g++) {
        conf[g].mask = ~0ULL;
        for (i = 0; i < RTE_ETH_RETA_GROUP_SIZE; i++) {
            conf[g].reta[i] = queue;
        }
    }
    ret = rte_eth_dev_rss_reta_update(port_id, conf, info.reta_size);
    free(conf);
    return ret;
}

/* Register the calling thread with EAL as a non-EAL lcore, so that it has
 * an lcore id and with it a mempool cache of its own. Its cpuset is read
 * from the thread's affinity, so call it once the thread is pinned.
 *
 * Returns 0 on success, or rte_errno (positive) on failure: ENOMEM when
 * every lcore id is taken, EINVAL before EAL init or when multi-process
 * is in use (DPDK 22.11, eal_common_thread.c). rte_errno is a per-lcore
 * variable, which is why this is not left to the bindings. */
int dpdk_thread_register(void) {
    if (rte_thread_register() == 0) {
        return 0;
    }
    return rte_errno;
}

/* The calling thread's lcore id (rte_lcore_id(), an inline read of a
 * per-thread variable), or LCORE_ID_ANY when it has none. */
unsigned int dpdk_lcore_id(void) {
    return rte_lcore_id();
}
