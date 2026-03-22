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

char *dpdk_mbuf_buf_addr(const struct rte_mbuf *m) {
    return (char *)m->buf_addr;
}
