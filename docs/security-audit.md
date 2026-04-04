# Security Audit

**Date**: 2026-03-16
**Scope**: Full codebase — matching engine, account management, journal/snapshot, wire protocol, authentication, networking, resource management.

## Summary

The engine is **fundamentally sound** on the critical path: price-time priority, balance conservation, dedup, and STP are correctly implemented. Proptest found and fixed a reservation leak (price-improved fills) during this review cycle. No remotely exploitable vulnerability was found that allows fund extraction.

The primary risks are:
- **Denial of service** via resource exhaustion (connections, memory, disk)
- **Slow-client attacks** blocking the single-threaded response stage
- **Missing operational limits** (max orders, max connections, order throttling)

---

## Findings

### SEC-01: Response stage blocks on slow clients (HIGH)

**File**: `crates/server/src/response.rs`

The response stage writes to client sockets on a **single thread** serving all clients. If a client stops reading, one slow client could stall responses to every other client.

**Impact**: Latency spike or complete stall for all clients.
**Exploitable remotely**: Yes — connect, authenticate, stop reading.
**Status**: **FIXED** — 5-second `SO_SNDTIMEO` set on response sockets before handoff to response stage. Timed-out writes return an error, and the response stage already drops connections on write error.

---

### SEC-02: No connection limits or rate limiting (HIGH)

**File**: `crates/server/src/server.rs:270-369`

The accept loop has no limit on concurrent connections, no per-IP rate limiting, and no backoff on repeated connection attempts. Authentication runs on the accept thread with a 5-second timeout per connection.

**Impact**: An attacker can exhaust file descriptors (EMFILE), saturate the accept thread with slow auth handshakes, or overwhelm the reader pool with thousands of idle connections.
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
**Mitigation**: Per-account max open orders limit. Per-instrument max price levels. Reject orders that would exceed limits.

---

### SEC-04: No order throttling (MEDIUM)

**File**: `crates/engine/src/exchange.rs`

No per-account or per-connection rate limiting on order submissions. A single client can flood the disruptor with orders at wire speed, starving other clients.

**Impact**: One client monopolizes matching throughput.
**Exploitable remotely**: Yes.
**Mitigation**: Per-account order-per-second rate limiter in the reader or exchange.

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

When the input ring buffer is full, `publish()` spins in a tight loop calling `std::hint::spin_loop()`. If the matching stage falls behind (e.g., processing a large stop cascade), reader threads burn 100% CPU spinning.

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

### SEC-09: Snapshot file tampering (MEDIUM)

**Files**: `crates/engine/src/journal/snapshot.rs:418-571`

A corrupted or malicious snapshot file can:
- **OOM**: Large count fields (e.g., `n_balances = 1_000_000_000`) cause `Vec::with_capacity()` to allocate gigabytes before reading data. The `validate_count` check bounds against remaining buffer but the Vec is allocated first.
- **Dedup bypass**: A tampered snapshot can reset per-account OrderId high-water marks, allowing previously-executed orders to be replayed.
- **Balance forgery**: Balances and reservations are loaded without cross-validation against each other or the order book.

**Impact**: Server crash (OOM) or state corruption on recovery.
**Exploitable remotely**: No — requires write access to the snapshot file.
**Mitigation**: Validate snapshot invariants after loading (reservation↔book consistency, balance conservation). Cap count fields to reasonable maximums before allocation.

---

### SEC-10: Nonce RNG not explicitly cryptographic (LOW)

**File**: `crates/server/src/server.rs:482`

Auth nonce uses `rand::fill(&mut nonce)` which defaults to the thread-local CSPRNG. This is secure in practice, but doesn't explicitly specify `OsRng` for cryptographic material.

**Status**: **FIXED** — replaced `rand::fill()` with `getrandom::fill()` which calls the OS CSPRNG directly.

---

### SEC-11: No explicit auth state on connections (LOW)

**File**: `crates/server/src/reader.rs`

Connection state has no `is_authenticated` field. Authentication is enforced structurally (only authenticated connections reach the reader), but a future refactor could accidentally allow unauthenticated traffic.

**Status**: **N/A** — on closer inspection, `ConnectionState` already carries a typed `permission: Permission` field set only from the auth handshake. This is stronger than a boolean — unauthenticated connections never reach the reader.

---

### SEC-12: Market orders bypass price bands by design (LOW)

**File**: `crates/engine/src/exchange.rs:252-278`

Market and stop orders skip price band checks because they have no submission-time price. A large market order can fill far outside the intended price bands.

**Status**: **DOCUMENTED** — added comment in `exchange.rs` explaining the design choice and pointing to Phase 3 (automatic volatility halts) as the proper mitigation.

---

## Severity Summary

| ID | Issue | Severity | Remote |
|----|-------|----------|--------|
| SEC-01 | ~~Response stage blocks on slow client~~ | ~~HIGH~~ FIXED | Yes |
| SEC-02 | ~~No connection limits~~ / rate limiting | ~~HIGH~~ PARTIAL | Yes |
| SEC-03 | Unbounded order book growth | HIGH | Yes |
| SEC-04 | No order throttling | MEDIUM | Yes |
| SEC-05 | Journal disk exhaustion hangs server | MEDIUM | Indirect |
| SEC-06 | Disruptor backpressure spins CPU | MEDIUM | Partial |
| SEC-07 | Saturating arithmetic masks errors | MEDIUM | No |
| SEC-08 | No TLS | MEDIUM | MITM |
| SEC-09 | Snapshot file tampering | MEDIUM | No |
| SEC-10 | ~~Nonce RNG not explicit~~ | ~~LOW~~ FIXED | No |
| SEC-11 | ~~No explicit auth state field~~ | ~~LOW~~ N/A | No |
| SEC-12 | ~~Market orders bypass price bands~~ | ~~LOW~~ DOCUMENTED | N/A |

## Recommended Priority

1. **SEC-01** — write timeout on response sockets (quick fix, high impact)
2. **SEC-02** — connection limits + per-IP throttling
3. **SEC-03** — per-account max open orders
4. **SEC-04** — per-account order rate limiter
5. **SEC-08** — TLS support
6. **SEC-05** — journal rotation + disk usage monitoring
7. **SEC-07** — checked arithmetic in financial paths
8. **SEC-09** — snapshot invariant validation on load
