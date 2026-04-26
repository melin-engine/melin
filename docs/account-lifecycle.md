# Account Lifecycle and Sparse Storage

## Problem

The original engine used flat `Vec` storage for account balances and order ID high-water marks, indexed by `account_id * currency_stride + currency_id`. This gives O(1) lookups with no hashing, but allocates memory proportional to `max(account_id) * max(currency_id)` regardless of actual usage.

At 1M accounts x 200 currencies x 16 bytes = **3.2 GB for balances alone**. This doesn't scale to 10M+ accounts, and causes deterministic latency spikes when Vec resizes are triggered by previously-unseen account IDs (e.g., a ~1.5ms spike at ~18M orders from per-account state maps resizing from 500K to 1M entries).

## Solution

Replace flat `Vec` with sparse `HashMap` (via `astenn` extendible hashing — grows one bucket at a time, no full-table rehash spikes):

- **Balances**: `HashMap<(AccountId, CurrencyId), Balance>` — only accounts with non-zero balances consume memory
- **Live order IDs**: `HashMap<(AccountId, OrderId), ()>` — currently-resting orders, used to reject duplicate IDs while the original is live; entries removed on close
- **Order counts**: `HashMap<AccountId, u32>` — tracks resting orders per account for withdrawal safety
- **Per-key request seq HWM**: `HashMap<u64, u64>` — keyed by `key_hash`, never evicted (request idempotency across reconnects)

The reservation slab (`Vec<Reservation>` with free list) remains unchanged — it's indexed by opaque `ReservationSlot(u32)`, not by account ID, and is already efficiently managed.

### Performance tradeoff

`astenn::HashMap` lookups (~20-50ns) are slower than flat Vec indexing (~1-3ns), but unlike `FxHashMap` (hashbrown), growth is amortized — each insert that triggers growth only touches entries in the splitting bucket, not the entire table. This trades ~19% throughput regression for predictable, spike-free latency. With 4+ balance lookups per order, the engine is still sub-microsecond per order.

The self-contained design (no gateway cooperation needed for correctness) is the right commercial tradeoff.

### Memory cleanup

Balance entries are automatically removed when both `available` and `reserved` reach zero, triggered by `Withdraw` events. The live-order-ID set is naturally bounded by concurrent open orders -- entries leave on close (full fill, cancel, expiry, etc.) and are not retained after the order's lifetime. The per-key request-seq HWM (`key_hwm`) is **never evicted**, since duplicate-request rejection has to remain correct across arbitrarily-long reconnect gaps; at ~16 bytes per key and a small key population (one per authenticated client identity), the cost is negligible.

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

### Request-seq HWM retention

The per-key request sequence HWM (`key_hwm`) is **never evicted**, even after full account withdrawal. It's keyed by client public-key hash, not by account, so it survives the account lifecycle by design: a reconnecting client must not be able to replay stale requests after re-deposit. Without this, idempotency across reconnects would not be sound.

The live-order-ID set behaves differently -- entries are short-lived, scoped to the lifetime of an individual order, and the set is fully reconstructible from the order books on snapshot restore. There is no equivalent persistence concern for it.

## Institutional considerations

Institutional clients (market makers, HFT firms) maintain persistent connections and expect always-on accounts. The sparse storage model supports this without requiring gateway-driven lifecycle management. Accounts simply persist in the engine's HashMaps as long as they have non-zero balances or live orders.

The `Withdraw` event is optional — it's an explicit cleanup mechanism for when accounts truly leave the system, not a required lifecycle step for normal operation.

For deployments exceeding ~1B distinct authenticated keys, the gateway cold storage lifecycle could be extended with `DeactivateKey` / `ReactivateKey(saved_hwm)` commands that evict and restore `key_hwm` entries alongside the account data, keeping in-memory storage proportional to currently-active keys rather than all-time keys.
