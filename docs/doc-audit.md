# Documentation Audit

Logical errors, security gaps, and design concerns found during review of all documentation against the codebase.

---

## Critical

### ~~1. admin-guide.md claims negative maker fees (rebates) are supported~~

**Status**: **FIXED** — `FeeSchedule` now uses `i16` for both `maker_fee_bps` and `taker_fee_bps`. Rebates (negative fees) are fully supported. `ExecutionReport::Fill` uses `i64` for `maker_fee` and `taker_fee`. Fees are credited to `FEE_ACCOUNT` (AccountId 0). Docs updated accordingly.

---

### 2. No per-account authorization — any Trader key can act on any account

**Location**: `docs/wire-protocol.md` (SubmitOrder, CancelAll), `docs/risk-checks.md`, `docs/admin-guide.md`

**Description**: The `SubmitOrder` wire message includes `account_id` as a client-provided field. Any authenticated `Trader`-level key can submit orders for any account by specifying any `account_id`. Similarly, `CancelAll` accepts an `account_id` and any Trader can cancel any account's orders. There is no validation that the authenticated key is authorized to act on the specified account.

**Risk**: In a multi-tenant deployment (multiple trading firms sharing one engine), a malicious or compromised Trader key for firm A can submit orders using firm B's account, drain firm B's balances, or cancel all of firm B's resting orders. The audit trail records the order but does not identify which key submitted it.

**Affected docs**: None of the docs flag this as a security boundary. The README lists "Per-account trading permissions" as unimplemented (`[ ]`), but the operational docs don't warn about the gap.

**Mitigation**: Until per-account key binding is implemented, operators should issue separate engine instances per tenant, or use a single trusted Admin key with a gateway that enforces account-level authorization.

---

### 3. Fee schedule increase silently under-charges resting orders

**Location**: `docs/fee-model.md`, line 118

**Claim**: "a fee schedule increase could theoretically cause a resting buy order's reservation to be insufficient for the new fee. In practice this is handled by the `saturating_sub`/`saturating_add` arithmetic in the balance manager -- the reservation covers whatever it can, and any shortfall is absorbed."

**Reality**: The word "handled" implies correctness, but what actually happens is the exchange collects less fee than configured. The buyer pays `min(actual_fee, cushion_at_placement_time)` rather than the configured fee rate. There is no log, no error, and no audit trail entry for the shortfall.

**Risk**: If an operator increases fees during active trading (e.g., from 5 bps to 50 bps), all resting buy orders placed under the old fee schedule will be under-charged on fill. The exchange loses revenue with no visibility into the loss.

**Fix**: Document this explicitly as: "The exchange absorbs the fee shortfall. To avoid this, increase fee schedules only when no resting buy orders exist for the instrument (e.g., after a trading halt and CancelAll)."

---

## Important

### 4. No wire-level integrity after authentication handshake

**Location**: `docs/wire-protocol.md`, line 5

**Claim**: "No CRC on the wire -- TCP handles integrity."

**Reality**: TCP protects against accidental transmission errors (bit flips, reordering). It does NOT protect against active man-in-the-middle attacks. After the Ed25519 challenge-response handshake, all messages are transmitted in plaintext with no message authentication codes (MAC) and no per-message sequence numbers.

**Risk**: An attacker with network access (e.g., compromised switch, ARP spoofing on the VLAN) could:
- Inject fabricated order submissions or cancellations.
- Modify order prices or quantities in transit.
- Replay captured messages (no sequence numbers to detect replays).
- Drop messages without detection (client and server lose sync).

**Mitigation**: The README notes TLS is deferred for VLAN deployments. The wire protocol doc should explicitly state the security boundary: "The protocol assumes a trusted network. It is NOT safe over untrusted networks without TLS or an equivalent transport-layer encryption."

---

### 5. No global trading halt command

**Location**: `docs/operations.md`, line 382

**Description**: "There is no single 'halt everything' command. You must send `SetCircuitBreaker` with `halted=true` for each instrument individually."

**Risk**: In an emergency (detected breach, systemic error, regulatory order to halt), the operator must issue N separate admin commands, one per instrument. With 100 instruments, this takes 100 sequential round-trips through the admin TUI. During this time, some instruments continue trading while others are halted — creating an inconsistent state.

**Recommendation**: Implement a `HaltAll` command that atomically sets `halted=true` on all instruments in a single journal entry. Until then, the operations runbook should include a scripted halt procedure using the client library.

---

### 6. Authorized keys loaded once at startup — no hot reload

**Location**: `docs/operations.md` (startup sequence), `docs/wire-protocol.md` (permission model)

**Description**: The `authorized_keys` file is loaded at server startup and cached in memory. There is no mechanism to reload it without restarting the server.

**Risk**: If a Trader key is compromised, the only way to revoke access is to restart the server. During the restart window (recovery + rotation), no trading occurs. In a production environment, this could mean minutes of downtime to revoke a single key.

**Mitigation**: Document this limitation in the operations runbook. Note that the `--max-connections` flag can limit damage (attacker can't open unlimited connections), and `CancelAll` can neutralize orders placed by the compromised key. Long-term, implement SIGHUP-triggered key reload.

---

### ~~7. Large AccountId or CurrencyId values cause unbounded memory allocation~~

**Status**: **FIXED** — flat `Vec` storage was replaced with sparse `astenn::HashMap` (extendible hashing). Memory is proportional to the number of active accounts/currencies, not to `max(account_id) * max(currency_id)`. Large IDs are no longer a concern. The referenced `docs/balance-management.md` file never existed — the relevant doc is `docs/account-lifecycle.md`.

---

## Minor

### 8. Stop order cascade limited to one level deep

**Location**: `docs/matching-engine.md`, line 303

**Description**: "Stops whose trigger conditions are met by fills from other triggered stops will fire on the next incoming order." This is correctly documented as behavior, but the operational consequence is not called out: stop B's execution may be delayed by milliseconds or seconds (until the next order arrives), creating a gap between the trigger condition being met and the stop actually executing.

**Impact**: In fast-moving markets, this delay could cause stop orders to execute at significantly worse prices than expected. Operators and traders relying on stop cascades for risk management should be aware of this limitation.

---

### 9. FOK pre-check is overly conservative with CancelOldest STP

**Location**: `docs/matching-engine.md` (FOK section)

**Description**: The FOK pre-check excludes same-account orders when STP is active. With `CancelOldest` mode, the taker would cancel the same-account maker and continue matching against other orders. The pre-check doesn't account for this — it sees less available quantity than reality and may reject orders that could actually fill.

**Impact**: FOK orders with `CancelOldest` STP may be unnecessarily rejected. This is the safe direction (reject rather than partially fill a FOK), but could surprise market makers using CancelOldest with FOK orders.

---

### 10. Response stage spin-wait burns CPU during journal stalls

**Location**: `docs/pipeline-architecture.md` (response stage section)

**Description**: When the journal stage stalls (NVMe latency spike, extent allocation), the response stage spin-waits on the journal cursor consuming 100% of its pinned core. There is no adaptive backoff or yield in this specific spin-wait path.

**Impact**: During rare journal stalls (typically <1ms), one CPU core is wasted on spinning. This doesn't affect correctness but wastes power and could cause thermal throttling on sustained stalls.

---

### 11. Throughput calculation includes warmup orders

**Location**: `docs/benchmarking.md`, line 225

**Description**: "computed as `(measured_orders + warmup_orders) / wall_time`." For short runs (e.g., 1,000 pairs with 100,000 warmup per client), warmup dominates the order count, inflating the reported throughput. For the published 100M-pair benchmarks, the error is <0.1%.

**Impact**: Users running short benchmark tests may see misleadingly high throughput numbers.

---

## Summary

| # | Severity | Category | Description |
|---|----------|----------|-------------|
| 1 | ~~Critical~~ | ~~Doc error~~ | ~~admin-guide claims rebates supported~~ FIXED — fees are now i16, rebates supported |
| 2 | Critical | Security | Any Trader key can act on any account (no per-account auth) |
| 3 | Critical | Logic | Fee increase silently under-charges resting orders |
| 4 | Important | Security | No wire integrity after auth (MITM possible on untrusted networks) |
| 5 | Important | Operations | No global halt command (must halt instruments one by one) |
| 6 | Important | Security | Keys can't be revoked without restart |
| 7 | ~~Important~~ | ~~Security~~ | ~~Large IDs cause unbounded memory allocation~~ FIXED — sparse HashMap storage |
| 8 | Minor | Logic | Stop cascade depth=1 delays secondary triggers |
| 9 | Minor | Logic | FOK + CancelOldest STP overly conservative |
| 10 | Minor | Efficiency | Response stage spin-wait during journal stalls |
| 11 | Minor | Doc accuracy | Throughput includes warmup (misleading for short runs) |
