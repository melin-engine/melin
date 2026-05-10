# Security Audit

**Date**: 2026-03-16
**Scope**: Full codebase — matching engine, account management, journal/snapshot, wire protocol, authentication, networking, resource management.

## Summary

The engine is **fundamentally sound** on the critical path: price-time priority, balance conservation, dedup, and STP are correctly implemented. Proptest found and fixed a reservation leak (price-improved fills) during this review cycle. No remotely exploitable vulnerability was found that allows fund extraction.

The primary risks are:
- **Denial of service** via resource exhaustion (connections, memory, disk)
- **Missing operational limits** (max orders, per-IP connections, order throttling)

---

## Findings

### SEC-02: No connection limits or rate limiting (HIGH)

**File**: `crates/server/src/server.rs:270-369`

The accept loop has no limit on concurrent connections, no per-IP rate limiting, and no backoff on repeated connection attempts. Authentication runs on the accept thread with a 5-second timeout per connection.

**Impact**: An attacker can exhaust file descriptors (EMFILE), saturate the accept thread with slow auth handshakes, or overwhelm the reader thread with thousands of idle connections.
**Exploitable remotely**: Yes — open many connections slowly.
**Status**: **PARTIALLY FIXED** — added `--max-connections` flag (default 1024). Connections beyond the limit are rejected before auth. Remaining: per-IP connection cap and exponential backoff on auth failures.

---

### SEC-03: Unbounded order book growth (HIGH)

**Files**: `crates/engine/src/orderbook.rs:225,233`, `crates/engine/src/account.rs:100`

No limit on resting orders, pending stops, or price levels per instrument. An attacker can place millions of limit orders at different prices, causing unbounded HashMap and BTreeMap growth.

- `order_index`: HashMap grows without bound
- `stop_index`: HashMap grows without bound
- `levels`: BTreeMap grows without bound (one node per price)
- `reservations`: Pre-allocated for 2M entries; the 2,000,001st triggers an expensive resize on the hot path

**Impact**: Memory exhaustion (OOM kill) or latency spikes from HashMap resizes.
**Exploitable remotely**: Yes — submit many resting orders.
**Status**: **PARTIALLY FIXED** — added `--max-orders-per-account` flag (default 10 000). Submissions beyond the cap reject with `ExceedsMaxOpenOrders` before reservation. The cap is operator policy and must match across primary and replicas (replay determinism). Remaining: per-instrument max price levels, and a global ceiling on total resting orders so a horde of accounts can't collectively exhaust the maps.

---

### SEC-04: No order throttling (MEDIUM)

**File**: `crates/engine/src/exchange.rs`

No per-account or per-connection rate limiting on order submissions. A single client can flood the disruptor with orders at wire speed, starving other clients.

**Impact**: One client monopolizes matching throughput.
**Exploitable remotely**: Yes.
**Status**: **FIXED** — added `--max-orders-per-second` (default 1000) and `--max-orders-burst` (default 5000). Per-account token bucket runs inside the matching engine using the journaled event timestamp, so primary and replicas see identical accept/reject decisions. Submissions beyond the bucket reject with `ExceedsOrderRate` before reservation. `0` for either knob disables the limiter. Both values are operator policy and must match across the cluster (same shape as `--max-orders-per-account`). Snapshot format v18 carries per-account bucket state, so a replica restoring from a snapshot taken mid-throttle converges bit-for-bit on the very next event — closing the divergence window the initial landing left open.

**Operator note — defaults are conservative**: the 1000/s sustained rate and 5000-order burst are sized for retail and algo flow. Market-maker accounts typically require limits an order of magnitude higher than this, or more. Operators onboarding market-maker flow must raise these limits accordingly — or set either knob to `0` to disable per-account throttling and apply controls at an external gateway in front of Melin. Per-account scoping (rather than per-session) means a single account with multiple gateway sessions draws from one shared bucket; size for the aggregate.

---

### SEC-05: Journal disk exhaustion hangs the server (MEDIUM)

**File**: `crates/engine/src/journal/writer.rs:265-275`

When the journal disk fills, `posix_fallocate` returns ENOSPC. The error propagates to the journal stage, but there is no graceful degradation — the journal stage stops advancing its cursor, the response stage stops sending, and the server effectively hangs.

**Impact**: Server becomes unresponsive. Requires manual intervention.
**Exploitable remotely**: Indirectly — submit enough orders to fill the disk.
**Mitigation**: Monitor disk usage, reject new orders when disk exceeds a threshold. Journal rotation is now implemented (`--max-journal-mib`, default 256 MiB) — triggers at startup when the journal exceeds the threshold.

---

### SEC-06: Disruptor backpressure spins at 100% CPU (MEDIUM)

**File**: `crates/disruptor/src/ring.rs:216-223`

When the input ring buffer is full, `publish()` spins in a tight loop calling `std::hint::spin_loop()`. If the matching stage falls behind (e.g., processing a large stop cascade), the reader thread burns 100% CPU spinning.

**Impact**: CPU waste, increased power consumption, reduced responsiveness.
**Exploitable remotely**: Partially — crafted order sequences that cause slow matching.
**Mitigation**: Bounded spin count then yield, or return a backpressure error to the client.

---

### SEC-07: Saturating arithmetic masks balance errors (MEDIUM)

**File**: `crates/engine/src/account.rs:294,306-328,342-343`

All balance operations use `saturating_add`/`saturating_sub`. While this prevents panics, it silently masks bugs that could create or destroy money. If a fill cost overflows u64 (clamped via `u64::try_from(cost).unwrap_or(u64::MAX)`), the subsequent `saturating_sub` silently zeros the balance.

**Impact**: Silent balance corruption — money destroyed without audit trail.
**Exploitable remotely**: No (requires upstream overflow bug in price x quantity, which is currently prevented by u128 intermediate).
**Mitigation**: Replace `saturating_sub` in financial paths with `checked_sub` + error logging. Keep saturating for non-critical paths.

---

### SEC-08: No TLS — wire protocol in plaintext (MEDIUM)

**Files**: `crates/protocol/src/tcp.rs`, `crates/protocol/src/blocking.rs`

All traffic including auth signatures, order data, and fill reports is sent unencrypted over TCP. An attacker with network position can observe and tamper with traffic.

**Impact**: Information disclosure, order manipulation on untrusted networks.
**Exploitable remotely**: Yes, with network position (MITM).
**Mitigation**: TLS wrapper for production. Document that plaintext is only acceptable on isolated VLANs.

---

### SEC-09: Snapshot file tampering — cross-invariant validation (MEDIUM)

**Files**: `crates/engine/src/journal/snapshot.rs:418-571`

The OOM-via-large-count vector was closed (counts are now bounded against remaining buffer before `Vec::with_capacity`). Remaining issues:

- **Dedup bypass**: A tampered snapshot can reset per-account OrderId high-water marks, allowing previously-executed orders to be replayed.
- **Balance forgery**: Balances and reservations are loaded without cross-validation against each other or the order book.

**Impact**: State corruption on recovery from a tampered snapshot.
**Exploitable remotely**: No — requires write access to the snapshot file.
**Mitigation**: Validate snapshot invariants after loading (reservation↔book consistency, balance conservation).

---

## Severity Summary

| ID | Issue | Severity | Remote |
|----|-------|----------|--------|
| SEC-02 | No connection limits / rate limiting (PARTIAL) | HIGH | Yes |
| SEC-03 | Unbounded order book growth (PARTIAL) | HIGH | Yes |
| SEC-04 | No order throttling (FIXED) | MEDIUM | Yes |
| SEC-05 | Journal disk exhaustion hangs server | MEDIUM | Indirect |
| SEC-06 | Disruptor backpressure spins CPU | MEDIUM | Partial |
| SEC-07 | Saturating arithmetic masks errors | MEDIUM | No |
| SEC-08 | No TLS | MEDIUM | MITM |
| SEC-09 | Snapshot cross-invariant validation | MEDIUM | No |

## Recommended Priority

1. **SEC-02** — per-IP connection cap + auth-failure backoff (`--max-connections` already landed)
2. **SEC-03** — per-instrument max price levels (per-account cap already landed)
3. **SEC-08** — TLS support
4. **SEC-05** — disk usage monitoring (rotation already landed)
5. **SEC-07** — checked arithmetic in financial paths
6. **SEC-09** — snapshot invariant validation on load
