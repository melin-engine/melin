/* Wrapper functions for DPDK static inline functions.
 *
 * DPDK performance-critical functions (rx_burst, tx_burst, pktmbuf_alloc,
 * pktmbuf_free) are defined as static inline in headers. bindgen cannot
 * generate bindings for these. We wrap them in non-inline C functions
 * that get compiled and linked into the Rust binary.
 */

#include <rte_ethdev.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>

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
