# Account Lifecycle and Sparse Storage

## Problem

The original engine used flat `Vec` storage for account balances and order ID high-water marks, indexed by `account_id * currency_stride + currency_id`. This gives O(1) lookups with no hashing, but allocates memory proportional to `max(account_id) * max(currency_id)` regardless of actual usage.

At 1M accounts x 200 currencies x 16 bytes = **3.2 GB for balances alone**. This doesn't scale to 10M+ accounts, and causes deterministic latency spikes when Vec resizes are triggered by previously-unseen account IDs (e.g., a ~1.5ms spike at ~18M orders from `max_order_id` resizing from 500K to 1M entries).

## Solution

Replace flat `Vec` with sparse `HashMap` (via `astenn` extendible hashing — grows one bucket at a time, no full-table rehash spikes):

- **Balances**: `HashMap<(AccountId, CurrencyId), Balance>` — only accounts with non-zero balances consume memory
- **Order ID HWM**: `HashMap<AccountId, u64>` — only accounts that have submitted orders
- **Order counts**: `HashMap<AccountId, u32>` — tracks resting orders per account for withdrawal safety

The reservation slab (`Vec<Reservation>` with free list) remains unchanged — it's indexed by opaque `ReservationSlot(u32)`, not by account ID, and is already efficiently managed.

### Performance tradeoff

`astenn::HashMap` lookups (~20-50ns) are slower than flat Vec indexing (~1-3ns), but unlike `FxHashMap` (hashbrown), growth is amortized — each insert that triggers growth only touches entries in the splitting bucket, not the entire table. This trades ~19% throughput regression for predictable, spike-free latency. With 4+ balance lookups per order, the engine is still sub-microsecond per order.

The self-contained design (no gateway cooperation needed for correctness) is the right commercial tradeoff.

### Memory cleanup

Balance entries are automatically removed when both `available` and `reserved` reach zero, triggered by `Withdraw` events. HWM entries (`max_order_id` and `key_hwm`) are **never evicted** — this prevents order ID replay and request replay after account withdrawal and re-deposit. At ~12 bytes per account, HWM storage is negligible up to ~1B accounts (~32 GB). Beyond that, implement the `DeactivateAccount`/`ReactivateAccount` commands described below to evict and restore HWMs as part of the cold storage lifecycle.

## Withdraw Event

The `Withdraw` journal event enables explicit account lifecycle management:

```
Withdraw { account: AccountId, currency: CurrencyId, amount: u64 }
```

**Preconditions:**
- Account must have no resting orders (enforced by `order_counts` check). Caller must `CancelAll` first.
- Sufficient available balance (not reserved).

**Effects:**
- Debits `amount` from available balance
- Removes the `(account, currency)` balance entry if both available and reserved reach zero
- Journaled before execution (persist-before-ack guarantee)

## Gateway Deposit/Withdraw Pattern (Future)

For extreme scale (1B+ accounts), the gateway can manage account lifecycle:

1. Client connects to gateway, authenticates
2. Gateway loads balances from cold storage (database), sends `Deposit` to engine
3. Client trades normally — engine has their balances in memory
4. Client disconnects — gateway sends `CancelAll` + `Withdraw` to engine
5. Gateway persists final balances to cold storage

The engine only holds concurrently connected accounts in memory. Even at peak, that's ~100K-500K simultaneous sessions.

### Journal recovery

Recovery is consistent because the journal records the full `Deposit -> trade -> Withdraw` sequence. On replay, the engine reconstructs the same state — only accounts that were active at crash time remain in memory. The gateway reconnects those clients and re-deposits if needed.

### Gateway crash reconciliation

If the engine crashes between the gateway sending a `Withdraw` and persisting to cold storage, the funds exist in neither place. The gateway must read the journal (or a replica) to reconcile — find accounts that were deposited but never withdrawn.

### HWM retention

The order ID high-water mark (`max_order_id`) and per-key request sequence high-water mark (`key_hwm`) are **never evicted**, even after full withdrawal. This is a safety mechanism: if an account is withdrawn and later re-deposited, old order IDs and request sequences are still rejected. Without this, a client could replay stale order IDs or duplicate requests after re-deposit.

For deployments exceeding ~1B accounts, the gateway cold storage lifecycle should be extended with `DeactivateAccount` and `ReactivateAccount(saved_hwm)` commands that evict and restore the HWM entries alongside the balance data. This keeps in-memory HWM storage proportional to active accounts rather than all-time accounts.

## Institutional considerations

Institutional clients (market makers, HFT firms) maintain persistent connections and expect always-on accounts. The sparse storage model supports this without requiring gateway-driven lifecycle management. Accounts simply persist in the engine's HashMaps as long as they have non-zero balances or HWM entries.

The `Withdraw` event is optional — it's an explicit cleanup mechanism for when accounts truly leave the system, not a required lifecycle step for normal operation.
