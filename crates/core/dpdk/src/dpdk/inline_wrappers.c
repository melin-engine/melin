/* Wrapper functions for DPDK static inline functions.
 *
 * DPDK performance-critical functions (rx_burst, tx_burst, pktmbuf_alloc,
 * pktmbuf_free) are defined as static inline in headers. bindgen cannot
 * generate bindings for these. We wrap them in non-inline C functions
 * that get compiled and linked into the Rust binary.
 */

#include <rte_ethdev.h>
#include <rte_flow.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>

#include <string.h>

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

/* Install a flow rule that captures all IPv4 packets with the given
 * source IPv4 address (in network byte order) into RX queue 0. Used
 * by bifurcated transports: the peer's public IP is the trigger, so
 * the kernel keeps SSH (different source IP), ARP, and anything else.
 * Returns 0 on success, a negative value on failure. Stores the
 * DPDK error type in *err_type when non-NULL on failure (for diagnostics). */
int dpdk_install_src_ipv4_steering(uint16_t port_id, uint32_t src_ipv4_be,
                                    int *err_type) {
    struct rte_flow_attr attr;
    struct rte_flow_item_eth eth_spec, eth_mask;
    struct rte_flow_item_ipv4 ipv4_spec, ipv4_mask;
    struct rte_flow_item pattern[3];
    struct rte_flow_action_queue queue;
    struct rte_flow_action actions[2];
    struct rte_flow_error err;
    struct rte_flow *flow;

    memset(&attr, 0, sizeof(attr));
    attr.ingress = 1;

    memset(&eth_spec, 0, sizeof(eth_spec));
    memset(&eth_mask, 0, sizeof(eth_mask));

    memset(&ipv4_spec, 0, sizeof(ipv4_spec));
    memset(&ipv4_mask, 0, sizeof(ipv4_mask));
    ipv4_spec.hdr.src_addr = src_ipv4_be;
    ipv4_mask.hdr.src_addr = 0xffffffff;

    memset(pattern, 0, sizeof(pattern));
    pattern[0].type = RTE_FLOW_ITEM_TYPE_ETH;
    pattern[0].spec = &eth_spec;
    pattern[0].mask = &eth_mask;
    pattern[1].type = RTE_FLOW_ITEM_TYPE_IPV4;
    pattern[1].spec = &ipv4_spec;
    pattern[1].mask = &ipv4_mask;
    pattern[2].type = RTE_FLOW_ITEM_TYPE_END;

    memset(&queue, 0, sizeof(queue));
    queue.index = 0;

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
        return -1;
    }
    return 0;
}
