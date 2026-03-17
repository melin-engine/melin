# Balance Management

The `AccountManager` (in `crates/engine/src/account.rs`) tracks per-account, per-currency balances, reserves funds on order placement, updates balances on fills, and releases reserves on cancellation. It runs on the same single thread as the matching engine -- no locks are needed.

## Balance Model

Each (account, currency) pair holds a `Balance`:

```rust
pub struct Balance {
    pub available: u64,  // funds free to use for new orders
    pub reserved: u64,   // funds locked by open orders
}
```

`available` represents funds that can be withdrawn or reserved for new orders. `reserved` represents funds locked by resting orders that have not yet been filled or cancelled. The `total()` method returns `available + reserved` (saturating).

Both fields use `u64` to match the scale of `Price` and `Quantity`. Overflow-prone calculations (price times quantity) use `u128` intermediates.

## Data Structure: Flat Vec Indexing

Balances are stored in a flat `Vec<Balance>` rather than a nested map. The index for a given (account, currency) pair is:

```
index = account_id * currency_stride + currency_id
```

where `currency_stride` equals `max_currency_id + 1` -- the number of currency slots per account row.

### Why a flat Vec

- **O(1) lookups** -- direct array indexing, no hashing or tree traversal.
- **Cache-friendly** -- all currencies for one account are contiguous in memory, maximizing spatial locality.
- **No hash collisions** -- deterministic access pattern with no worst-case degradation.
- **Bulk provisioning** -- a single allocation and sequential writes to seed all accounts at startup.
- **No prefault needed** -- the Vec pages are sequentially faulted on first write, unlike a HashMap which scatters across memory.

### ensure_capacity growth

The `ensure_capacity` method handles two growth cases when a deposit introduces a previously unseen account or currency:

1. **Stride increase** (new currency) -- allocates a new, wider Vec and reshuffles all existing rows into the new layout, copying each old row into its wider slot.
2. **Row extension** (new account, same currencies) -- calls `resize()` to append zeroed rows.

After startup seeding, `ensure_capacity` is a no-op: two comparisons that return immediately. Only admin `deposit` operations can trigger growth -- the order-matching hot path never allocates.

## Reservation Model

Open-order reservations are tracked in a `HashMap<(AccountId, OrderId), Reservation>`:

```rust
struct Reservation {
    account: AccountId,
    currency: CurrencyId,  // quote for buys, base for sells
    remaining: u64,        // decremented on each partial fill
}
```

### Why keyed by (AccountId, OrderId)

`OrderId` values are client-assigned and only guaranteed to be unique within a single account. Different accounts can independently use the same `OrderId`. The composite key `(AccountId, OrderId)` ensures reservations are unambiguous.

A `HashMap` is used because reservation keys are sparse (not dense sequential integers), making flat-Vec indexing impractical.

### Pre-allocation

`AccountManager::with_capacity()` pre-allocates the HashMap for 2,000,000 entries (one per resting order across all instruments), sized for production workloads.

## Reserve Operation

`try_reserve` is called when a new order arrives. It computes the required reserve amount, moves funds from `available` to `reserved`, and inserts a `Reservation` entry.

The reserved currency and amount depend on order type and side:

| Side | Order Type | Reserved Currency | Reserved Amount |
|------|-----------|-------------------|-----------------|
| Buy  | Limit / StopLimit | Quote (`spec.quote`) | `price * quantity + fee_cushion` |
| Buy  | Market / Stop | Quote (`spec.quote`) | Entire `available` quote balance |
| Sell | Any | Base (`spec.base`) | `quantity` |

### Fee cushion for limit buys

For buy limit and stop-limit orders, the reservation includes a fee cushion computed as:

```
fee_cushion = (price * quantity) * max_fee_bps / 10_000
```

where `max_fee_bps` is the higher of maker and taker fee rates. This ensures that fees can always be charged from the reservation even when filling at the exact limit price. The intermediate calculation uses `u128` to avoid overflow.

### Market buy reservation

Since the final execution price of a market buy is unknown at placement time, the entire available quote balance is reserved. After execution, the unused portion is refunded via `release`.

### Rejection

`try_reserve` returns `Err(RejectReason::InsufficientBalance)` if:
- The account has no balance entry for the required currency.
- `available` is less than the computed reserve amount.
- The computed amount is zero (e.g., market buy with zero quote balance).
- `price * quantity` overflows `u64` (limit buy with extreme values).

## Fill Operation

`fill` is called once per `ExecutionReport::Fill` to transfer assets between buyer and seller. It determines buyer and seller from the `maker_side` parameter:

- If `maker_side == Buy`, the maker is the buyer and the taker is the seller.
- If `maker_side == Sell`, the taker is the buyer and the maker is the seller.

The cost is computed as `price * quantity` using `u128` to avoid overflow, then truncated to `u64` (with a debug assertion that no truncation occurs).

### Buyer balance update

1. Deduct `cost + buyer_fee` from the buyer's `Reservation.remaining`.
2. Deduct `cost + buyer_fee` from the buyer's quote currency `reserved`.
3. Credit `quantity` to the buyer's base currency `available`.

The fee is covered by the fee cushion included at reservation time.

### Seller balance update

1. Deduct `quantity` from the seller's `Reservation.remaining`.
2. Deduct `quantity` from the seller's base currency `reserved`.
3. Credit `cost - seller_fee` to the seller's quote currency `available`.

### Partial fills

After a partial fill, `Reservation.remaining` reflects the leftover reserved amount. The order stays on the book and subsequent fills continue to decrement the same reservation. Reservation cleanup (removal from the HashMap) is deferred to `process_reports`.

## Release Operation

`release` handles cancellation and rejection. It removes the `Reservation` entry from the HashMap and moves `remaining` back from `reserved` to `available`:

```rust
pub fn release(&mut self, account: AccountId, order_id: OrderId) {
    if let Some(res) = self.reservations.remove(&(account, order_id)) {
        bal.reserved -= res.remaining;
        bal.available += res.remaining;
    }
}
```

Releasing an unknown order (no reservation entry) is a no-op -- this is safe because the reservation may have already been cleaned up by `process_reports`.

## process_reports: Batch Report Processing

`process_reports` iterates over a slice of `ExecutionReport` values and dispatches balance updates:

- **Fill** -- calls `self.fill(...)` with the maker side looked up from the `maker_sides` map. After each fill, checks both the maker and taker reservations: if `remaining == 0`, removes the reservation and appends the `(AccountId, OrderId)` to the `consumed` output vec.
- **Cancelled** -- calls `self.release(...)` and appends to `consumed`.
- **Rejected** -- calls `self.release(...)` and appends to `consumed`.
- **Placed / Triggered / Replaced** -- no balance action needed.

The `consumed` output vec is used by the caller (`Exchange`) to clean up its own per-order tracking maps (e.g., `order_sides`). This is why `fill` itself does not remove zero-remaining reservations -- `process_reports` needs the entry to still exist so it can report the consumed ID back.

## Cancel-Replace: Reservation Adjustment

`try_adjust_reservation` modifies an existing reservation in place for cancel-replace (amend) operations:

- If the new amount is **higher**, checks that the account has sufficient `available` balance for the delta. Moves the delta from `available` to `reserved`. Returns `Err(InsufficientBalance)` if insufficient, leaving the reservation unchanged.
- If the new amount is **lower or equal**, always succeeds. Moves the delta from `reserved` back to `available`.

## Pre-faulting

`prefault()` touches all pre-allocated HashMap pages at startup so that page faults happen during initialization, not on the hot path during order matching.

The method fills the HashMap to its pre-allocated capacity with dummy entries, then clears them:

```rust
pub fn prefault(&mut self) {
    if self.reservations.is_empty() {
        let cap = self.reservations.capacity();
        for i in 0..cap {
            self.reservations.insert(
                (AccountId(0), OrderId(i as u64)),
                Reservation::new(AccountId(0), CurrencyId(0), 0),
            );
        }
        self.reservations.clear();
    }
}
```

The flat `Vec<Balance>` does not need prefaulting because it is contiguous memory that gets sequentially faulted during the startup deposit seeding phase.

## Snapshot and Restore

The `AccountManager` supports serialization for crash recovery via event-sourcing snapshots.

### Saving

- `snapshot_balances()` -- iterates the flat Vec and collects all non-zero `((AccountId, CurrencyId), Balance)` entries. Account and currency IDs are recovered from the flat index using `account = index / currency_stride` and `currency = index % currency_stride`.
- `snapshot_reservations()` -- collects all entries from the reservations HashMap as `(OrderId, AccountId, CurrencyId, remaining)` tuples.

### Restoring

`from_parts` reconstructs the `AccountManager` from saved data:

1. Scans the balance entries to find `max_account` and `max_currency`, deriving the stride and row count.
2. Allocates a flat Vec of the correct dimensions and populates it from the entries.
3. Rebuilds the reservations HashMap from the saved tuples.

This avoids replaying every deposit and order from the beginning of the journal -- only events after the snapshot need to be replayed.
