//! Property-based tests for BookMirror correctness.
//!
//! Generates random ExecutionReport sequences (constrained to valid
//! state transitions) and asserts the mirror matches a naive reference
//! after every event.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use proptest::prelude::*;

    use melin_types::types::{AccountId, ExecutionReport, OrderId, Price, Quantity, Side, Symbol};

    use crate::mirror::{BookMirror, Level};

    const SYM: Symbol = Symbol(1);
    const ACCT: AccountId = AccountId(1);

    /// Naive reference book: tracks levels and resting orders in the
    /// simplest possible way (no optimization, no caching).
    #[derive(Default)]
    struct ReferenceBook {
        /// (order_id → (side, price, remaining))
        orders: BTreeMap<u64, (Side, u64, u64)>,
    }

    impl ReferenceBook {
        fn place(&mut self, id: u64, side: Side, price: u64, qty: u64) {
            self.orders.insert(id, (side, price, qty));
        }

        fn fill(&mut self, maker_id: u64, fill_qty: u64) {
            if let Some((_side, _price, remaining)) = self.orders.get_mut(&maker_id) {
                *remaining = remaining.saturating_sub(fill_qty);
                if *remaining == 0 {
                    self.orders.remove(&maker_id);
                }
            }
        }

        fn cancel(&mut self, id: u64) {
            self.orders.remove(&id);
        }

        fn replace(&mut self, id: u64, new_price: u64, new_qty: u64) {
            if let Some((_side, price, remaining)) = self.orders.get_mut(&id) {
                *price = new_price;
                *remaining = new_qty;
            }
        }

        /// Reconstruct L2 levels from scratch.
        fn levels(&self, side: Side) -> BTreeMap<Price, Level> {
            let mut result = BTreeMap::new();
            for &(s, price, remaining) in self.orders.values() {
                if s == side {
                    let level = result
                        .entry(Price(NonZeroU64::new(price).unwrap()))
                        .or_insert(Level {
                            total_qty: 0,
                            order_count: 0,
                        });
                    level.total_qty += remaining;
                    level.order_count += 1;
                }
            }
            result
        }
    }

    /// Actions the property test can generate.
    #[derive(Debug, Clone)]
    enum Action {
        Place {
            id: u64,
            side: Side,
            price: u64,
            qty: u64,
        },
        Fill {
            maker_id: u64,
            qty: u64,
        },
        Cancel {
            id: u64,
        },
        Replace {
            id: u64,
            new_price: u64,
            new_qty: u64,
        },
    }

    fn arb_side() -> impl Strategy<Value = Side> {
        prop_oneof![Just(Side::Buy), Just(Side::Sell)]
    }

    fn arb_action() -> impl Strategy<Value = Action> {
        prop_oneof![
            // Place: id in [1,200], price in [1,50], qty in [1,100]
            (1u64..=200, arb_side(), 1u64..=50, 1u64..=100).prop_map(|(id, side, price, qty)| {
                Action::Place {
                    id,
                    side,
                    price,
                    qty,
                }
            }),
            // Fill: maker in [1,200], qty in [1,50]
            (1u64..=200, 1u64..=50).prop_map(|(maker_id, qty)| Action::Fill { maker_id, qty }),
            // Cancel: id in [1,200]
            (1u64..=200).prop_map(|id| Action::Cancel { id }),
            // Replace: id in [1,200], new_price in [1,50], new_qty in [1,100]
            (1u64..=200, 1u64..=50, 1u64..=100).prop_map(|(id, new_price, new_qty)| {
                Action::Replace {
                    id,
                    new_price,
                    new_qty,
                }
            }),
        ]
    }

    fn price(n: u64) -> Price {
        Price(NonZeroU64::new(n).unwrap())
    }

    fn qty(n: u64) -> Quantity {
        Quantity(NonZeroU64::new(n).unwrap())
    }

    /// Apply an action to both the mirror and the reference, then assert
    /// they agree on all bid/ask levels.
    fn apply_and_check(mirror: &mut BookMirror, reference: &mut ReferenceBook, action: &Action) {
        match action {
            Action::Place {
                id,
                side,
                price: p,
                qty: q,
            } => {
                // Skip if this order ID already exists (would be a
                // duplicate — the engine rejects these, so the mirror
                // never sees them).
                if reference.orders.contains_key(id) {
                    return;
                }
                reference.place(*id, *side, *p, *q);
                mirror.apply(&ExecutionReport::Placed {
                    order_id: OrderId(*id),
                    symbol: SYM,
                    account: ACCT,
                    side: *side,
                    price: price(*p),
                    quantity: qty(*q),
                });
            }
            Action::Fill { maker_id, qty: q } => {
                // Only fill if the maker exists and has enough remaining.
                let fill_qty =
                    if let Some(&(_side, _price, remaining)) = reference.orders.get(maker_id) {
                        (*q).min(remaining)
                    } else {
                        return;
                    };
                if fill_qty == 0 {
                    return;
                }
                let maker = reference.orders[maker_id];
                reference.fill(*maker_id, fill_qty);
                mirror.apply(&ExecutionReport::Fill {
                    maker_order_id: OrderId(*maker_id),
                    taker_order_id: OrderId(10_000),
                    symbol: SYM,
                    maker_account: ACCT,
                    taker_account: AccountId(2),
                    price: price(maker.1),
                    quantity: qty(fill_qty),
                    maker_fee: 0,
                    taker_fee: 0,
                });
            }
            Action::Cancel { id } => {
                if let Some(&(_side, _price, remaining)) = reference.orders.get(id) {
                    reference.cancel(*id);
                    mirror.apply(&ExecutionReport::Cancelled {
                        order_id: OrderId(*id),
                        symbol: SYM,
                        account: ACCT,
                        remaining_quantity: qty(remaining),
                    });
                }
            }
            Action::Replace {
                id,
                new_price,
                new_qty,
            } => {
                if let Some(&(side, old_price, old_remaining)) = reference.orders.get(id) {
                    reference.replace(*id, *new_price, *new_qty);
                    mirror.apply(&ExecutionReport::Replaced {
                        order_id: OrderId(*id),
                        symbol: SYM,
                        account: ACCT,
                        side,
                        old_price: price(old_price),
                        new_price: price(*new_price),
                        old_remaining: qty(old_remaining),
                        new_remaining: qty(*new_qty),
                    });
                }
            }
        }

        // Assert mirror matches reference.
        let ref_bids = reference.levels(Side::Buy);
        let ref_asks = reference.levels(Side::Sell);
        assert_eq!(
            mirror.bids(),
            &ref_bids,
            "bid levels diverged after {action:?}"
        );
        assert_eq!(
            mirror.asks(),
            &ref_asks,
            "ask levels diverged after {action:?}"
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2048))]

        #[test]
        fn mirror_matches_reference(
            actions in proptest::collection::vec(arb_action(), 1..200)
        ) {
            let mut mirror = BookMirror::new(SYM);
            let mut reference = ReferenceBook::default();

            for action in &actions {
                apply_and_check(&mut mirror, &mut reference, action);
            }
        }
    }
}
