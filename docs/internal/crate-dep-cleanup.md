# Crate dependency cleanup

Findings from auditing the internal crate dependency graph (post-v0.7.0).

## Current graph (build deps only — dev-deps excluded)

```
admin            → client, protocol
bench            → app, disruptor, dpdk, engine, protocol, rumcast, server, trading, transport-core
client           → protocol, rumcast (optional)
engine           → app, disruptor, journal, trading, transport-core
journal          → app, disruptor
market-data      → engine, protocol, trading
md-gateway       → engine, gateway-core, market-data, protocol, trading
noop             → app, trading
oe-gateway       → engine, gateway-core, protocol, trading
protocol         → trading
server           → app, disruptor, dpdk, engine, journal, market-data, noop, protocol, rumcast, trading, transport-core
trading          → app
transport-core   → app, disruptor, journal
tui              → client, protocol
tui-fix-client   → gateway-core

leaves (no internal build deps): app, disruptor, dpdk, gateway-core, rumcast
```

## Findings

### `protocol → trading` is a layering inversion (the only real finding)

`protocol` is the wire codec, but it imports from `melin_trading::types::*`
(`Side`, `TimeInForce`, `AccountId`, `CurrencyId`, …) and `melin_trading::le`.
That makes `trading` do double duty: business logic *and* shared domain
types. The conventional shape is the opposite — domain types in a small
leaf crate, and both `protocol` and `trading` depend on it.

Effect: anyone touching the protocol pulls in the entire trading engine's
types module; `trading` is hard to evolve without rippling through
protocol. The same root cause is why `market-data`, `md-gateway`,
`oe-gateway`, and `noop` all carry a build dep on `trading` — they need
the domain types, not the business logic.

Fix options:
- Extract a `melin-types` (or `melin-domain`) leaf crate containing the
  shared types and the `le` module. Repoint `protocol`, `market-data`,
  `md-gateway`, `oe-gateway`, and `noop` at it. `trading` keeps the
  business logic.
- Or move the shared types into `melin-app` (already a zero-dep leaf),
  if they fit that crate's charter. `melin-app` is currently the
  application-trait crate — mixing domain primitives in is borderline,
  but defensible if we want to avoid adding a new crate.

Risk: medium — touches several crates, but the moves are mechanical.

## Not actually issues (initially flagged, dismissed on closer look)

- **`server → melin-client` as a regular dep.** False alarm: it's already
  in `[dev-dependencies]`. The only residue is the `"melin-client/rumcast"`
  passthrough inside server's `rumcast` feature, which is a slightly
  unusual way to express "the integration test's dev-dep needs the
  rumcast feature." Functionally harmless: when external consumers
  enable `melin-server/rumcast`, the passthrough is a no-op because
  dev-deps aren't in their build graph; when server's own tests run
  with `rumcast`, the passthrough fires and the client is built with
  rumcast support. Cosmetic at best.

- **`noop → melin-trading`.** Architecturally required, not a bug.
  `crates/server/src/lib.rs:27-29` documents that the server operates
  on the trading wire format regardless of which matcher is plugged in
  — that is the entire point of the `noop` feature. For the swap to
  type-check, `NoopApp::Event` must equal `TradingEvent`, so `noop`
  must depend on `melin-trading` to import the type. Renaming the
  crate to reflect this (e.g. `null-matcher`) would be more honest,
  but is cosmetic and has wide blast radius (the server's `noop`
  feature, docs, bench targets all reference the name).

- **`engine → melin-journal` direct, alongside `transport-core`.** Engine
  uses many concrete journal symbols (`JournalEvent`, `JournalError`,
  `JournalWrite`, `codec::FILE_HEADER_SIZE`, `segment::list_archives`)
  that `transport-core` does not re-export.

- **`transport-core → melin-journal`.** Transport-core's `journaled_app`
  module composes the journal as a building block; direction is correct.

- **`market-data`/`md-gateway`/`oe-gateway` → trading.** Same root cause
  as the only real finding; fixing it cleans these up automatically.
