/* The steering shim's interface, shared by inline_wrappers.c, which
 * defines it, and build.rs's bindgen header, which is how Rust sees it:
 * one declaration, so the two cannot disagree on the struct's layout. */

#ifndef MELIN_DPDK_STEERING_RULE_H
#define MELIN_DPDK_STEERING_RULE_H

#include <stddef.h>
#include <stdint.h>

/* One steering rule, as `melin_dpdk::SteeringRule` describes it: ingress
 * packets matching every field whose `has_` flag is set go to `queue`, at
 * `priority` in group 0 (lower is higher priority). Addresses and ports
 * in network byte order. */
struct dpdk_steering_rule {
    uint32_t priority;
    uint32_t src_ipv4_be;
    uint32_t dst_ipv4_be;
    uint16_t tcp_src_port_be;
    uint16_t tcp_dst_port_be;
    uint16_t queue;
    uint8_t has_src_ipv4;
    uint8_t has_dst_ipv4;
    uint8_t has_tcp_src_port;
    uint8_t has_tcp_dst_port;
};

int dpdk_install_steering_rule(uint16_t port_id,
                               const struct dpdk_steering_rule *rule,
                               int *err_type, char *err_msg,
                               size_t err_msg_len);

int dpdk_pin_rss_reta(uint16_t port_id, uint16_t queue);

#endif
