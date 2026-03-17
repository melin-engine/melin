# Admin CLI Guide

The `trading-admin` tool is a terminal UI (TUI) for operating a running trading engine instance. It provides a wizard-style menu system for all administrative and trading operations, plus a live dashboard showing server health metrics.

A companion tool, `trading-keygen`, generates the Ed25519 keypairs required for authentication.

## Key Generation

Before connecting, you need an Ed25519 keypair. The `trading-keygen` binary creates one:

```sh
trading-keygen <name> <permission>
```

**Permissions** are one of:

- `admin` -- full access (instrument setup, deposits, risk config, trading)
- `trader` -- trading operations only
- `readonly` -- observation only

**Example:**

```sh
trading-keygen ops admin
```

This produces three artifacts:

| File | Contents |
|------|----------|
| `ops.key` | 32-byte raw Ed25519 private seed (keep secret) |
| `ops.pub` | Base64-encoded public key |
| stdout | An `authorized_keys` line to add to the server |

The stdout line looks like:

```
admin AAAA...base64...== ops
```

Append that line to the server's `authorized_keys` file before starting the server. The tool refuses to overwrite an existing `.key` file to prevent accidental key loss.

## Connecting

```sh
trading-admin <addr> <key-file>
```

- `<addr>` -- server socket address, e.g. `127.0.0.1:9000` or `10.0.0.1:9000`
- `<key-file>` -- path to the `.key` file (32-byte raw seed)

**Example:**

```sh
trading-admin 127.0.0.1:9000 ops.key
```

On launch, the admin tool:

1. Loads the Ed25519 signing key from disk.
2. Spawns a background client thread that connects via TCP.
3. Performs the Ed25519 challenge-response handshake with the server.
4. Displays "Connected to \<addr\>" in the log on success.

If authentication fails, the log shows the error and the tool remains open (you can quit with `q` or `Esc`).

## TUI Layout

The screen is divided into three regions:

- **Dashboard** (top bar) -- live server stats, auto-refreshes every second.
- **Log** (center) -- scrolling history of sent commands and received responses. Color-coded: green for fills, cyan for placements, yellow for cancels, red for rejects/errors.
- **Status bar** (bottom) -- shows the current wizard step and keyboard shortcuts.

### Keyboard Controls

| Key | Action |
|-----|--------|
| Up/Down arrows | Navigate menu items |
| Enter | Select / confirm |
| Esc | Go back one step (quit from main menu) |
| Tab | Toggle between menu mode and text command mode |
| q | Quit (from main menu only) |
| 0-9 | Type digits in number input fields |
| Backspace | Delete last character |

## Menu Overview

The main action menu lists all operations, grouped by category:

| # | Action | Category |
|---|--------|----------|
| 0 | Limit Buy | Trading |
| 1 | Limit Sell | Trading |
| 2 | Market Buy | Trading |
| 3 | Market Sell | Trading |
| 4 | Stop Buy | Trading |
| 5 | Stop Sell | Trading |
| 6 | Stop-Limit Buy | Trading |
| 7 | Stop-Limit Sell | Trading |
| 8 | Cancel Order | Cancel / Amend |
| 9 | Cancel All | Cancel / Amend |
| 10 | Cancel-Replace | Cancel / Amend |
| 11 | Add Instrument | Admin |
| 12 | Deposit | Admin |
| 13 | Set Risk Limits | Admin |
| 14 | Set Circuit Breaker | Admin |
| 15 | Set Fee Schedule | Admin |

Navigate with arrow keys and press Enter to start the corresponding wizard.

## Instrument Setup (Add Instrument)

Before any trading can happen, you must register instruments. Select **Add Instrument** (menu item 11).

The wizard prompts for three numeric IDs:

1. **Symbol ID** -- a unique integer identifying this trading pair (e.g., `1` for BTC/USD).
2. **Base Currency ID** -- the currency being traded (e.g., `1` for BTC).
3. **Quote Currency ID** -- the currency used for pricing/settlement (e.g., `2` for USD).

All IDs must be greater than zero. The engine uses raw integer IDs rather than string tickers -- your application layer maps human-readable names to these IDs.

**Typical bootstrap sequence:**

1. Add Instrument (symbol 1, base 1, quote 2)
2. Deposit funds for trading accounts
3. Set risk limits and fee schedules
4. Begin trading

## Account Funding (Deposit)

Accounts need balances before they can trade. Select **Deposit** (menu item 12).

The wizard prompts for:

1. **Account ID** -- the account to credit (integer, must be > 0).
2. **Currency ID** -- which currency to deposit (must match a base or quote currency of an instrument).
3. **Amount** -- quantity to deposit in the smallest unit (integer, must be > 0).

The engine creates accounts implicitly on first deposit -- there is no separate "create account" step.

**Example:** To fund account 1 with 1,000,000 units of currency 2 (e.g., USD cents):

- Account ID: `1`
- Currency ID: `2`
- Amount: `1000000`

## Order Submission

Select any of the trading actions (menu items 0-7). Each walks through a wizard that collects the required fields step by step.

### Common Fields (All Order Types)

Every order wizard starts with:

1. **Symbol ID** -- which instrument to trade on.
2. **Account ID** -- which account places the order.

### Limit Orders (items 0-1)

After the common fields:

3. **Limit Price** -- the price level for the order.
4. **Quantity** -- number of lots.
5. **Time in Force** -- pick from a submenu:
   - **GTC** (Good-Til-Cancelled) -- stays on the book until filled or cancelled.
   - **IOC** (Immediate-Or-Cancel) -- fills what it can immediately, cancels the rest.
   - **FOK** (Fill-Or-Kill) -- fills entirely or rejects entirely.
6. **Self-Trade Prevention** -- pick from a submenu:
   - **Cancel Newest** (default) -- cancels the incoming order if it would self-trade.
   - **Cancel Oldest** -- cancels the resting order.
   - **Cancel Both** -- cancels both sides.
   - **Allow** -- permits self-trades.

### Market Orders (items 2-3)

After the common fields:

3. **Quantity** -- number of lots.

Market orders skip TIF and STP selection (they execute immediately against available liquidity).

### Stop Orders (items 4-5)

After the common fields:

3. **Trigger Price** -- the price that activates the stop.
4. **Quantity** -- number of lots.

Once the market reaches the trigger price, the stop converts to a market order.

### Stop-Limit Orders (items 6-7)

After the common fields:

3. **Trigger Price** -- the price that activates the stop.
4. **Limit Price** -- the price for the resulting limit order.
5. **Quantity** -- number of lots.
6. **Time in Force** -- same submenu as limit orders.
7. **Self-Trade Prevention** -- same submenu as limit orders.

### Order IDs

The admin tool auto-assigns sequential order IDs starting from 1. Each order gets the next available ID. The log shows the assigned ID (e.g., `order #3`) so you can reference it for cancels or replaces.

## Cancel Operations

### Cancel Order (item 8)

Cancels a single resting order.

1. **Symbol ID** -- which instrument the order is on.
2. **Order ID** -- the ID of the order to cancel.

### Cancel All / Kill Switch (item 9)

Cancels all resting orders for an account across all instruments. This is the kill switch for emergencies.

1. **Account ID** -- the account whose orders to cancel.

### Cancel-Replace (item 10)

Atomically modifies a resting order's price and quantity. The order keeps its ID but loses time priority (it goes to the back of the queue at the new price level).

1. **Symbol ID** -- which instrument.
2. **Order ID** -- the order to replace.
3. **New Price** -- the updated limit price.
4. **New Quantity** -- the updated remaining quantity.

## Risk Configuration

### Set Risk Limits (item 13)

Configures per-instrument order size limits. These are checked on the hot path for every incoming order.

1. **Symbol ID** -- which instrument.
2. **Max Order Qty** -- maximum quantity per order. Enter `0` for no limit.
3. **Max Notional** -- maximum notional value (price * quantity) per order. Enter `0` for no limit.

Orders exceeding either limit are rejected with `exceeds max order size` or `exceeds max notional`.

### Set Circuit Breaker (item 14)

Configures per-instrument price bands and trading halts.

1. **Symbol ID** -- which instrument.
2. **Lower Price Band** -- minimum acceptable price. Enter `0` for no lower bound.
3. **Upper Price Band** -- maximum acceptable price. Enter `0` for no upper bound.
4. **Halted?** -- enter `1` to halt trading entirely, `0` to allow trading.

When halted, all new orders for that instrument are rejected with `trading halted`. Orders outside the price bands are rejected with `outside price band`.

**Example -- halt trading on symbol 1:**

- Symbol ID: `1`
- Lower Price Band: `0`
- Upper Price Band: `0`
- Halted: `1`

**Example -- set 10% price bands around 10000:**

- Symbol ID: `1`
- Lower Price Band: `9000`
- Upper Price Band: `11000`
- Halted: `0`

## Fee Configuration

### Set Fee Schedule (item 15)

Configures per-instrument maker/taker fees.

1. **Symbol ID** -- which instrument.
2. **Maker Fee** -- fee in basis points (0-10000). A value of `0` means no maker fee. `10` means 0.10%.
3. **Taker Fee** -- fee in basis points (0-10000).

Fees are applied at fill time and reported in the execution report. Typical exchange configurations use negative maker fees (rebates) and positive taker fees, but the admin tool accepts any value in the 0-10000 range.

## Live Dashboard

The top bar of the TUI displays four live metrics, refreshed every second via `QueryStats` requests:

| Metric | Description |
|--------|-------------|
| **Connections** | Number of currently active client connections to the server. |
| **Events** | Total number of events processed by the matching engine since server start. |
| **Throughput** | Computed orders/sec rate based on the delta between consecutive stats snapshots. Displayed as raw number, K/s, or M/s depending on magnitude. |
| **Journal** | Current journal sequence number -- the latest durably written event. Useful for verifying that journaling is keeping up with the engine. |

Before the first stats response arrives, the dashboard shows "Waiting for stats...".

## Text Command Mode

Press **Tab** from the main menu to switch to text command mode. This is a power-user shortcut for quick operations without navigating the wizard.

Available text commands:

| Command | Description |
|---------|-------------|
| `cancel <symbol_id> <order_id>` | Cancel a single order |
| `cancel-all <account_id>` | Cancel all orders for an account |

Press **Tab** again to return to the menu.

## Response Log

All server responses appear in the scrolling center log with color coding:

| Color | Meaning |
|-------|---------|
| Green (bold) | Fill -- a trade occurred |
| Cyan | Placed -- order resting on the book |
| Yellow | Cancelled -- order removed |
| Magenta | Triggered -- stop order activated |
| Red | Reject or error |
| Gray | Outgoing commands (prefixed with an arrow) |

Each response includes the round-trip latency in brackets, e.g. `[1.234ms]`.

The log retains up to 10,000 entries before pruning older messages.

## Typical Workflow: Setting Up a New Market

```
1. Generate keys:         trading-keygen ops admin
2. Add key to server:     (append authorized_keys line to server config)
3. Start the server:      trading-server --addr 0.0.0.0:9000 ...
4. Connect admin:         trading-admin 127.0.0.1:9000 ops.key
5. Add Instrument:        symbol=1, base=1, quote=2
6. Set Fee Schedule:      symbol=1, maker=2bps, taker=5bps
7. Set Risk Limits:       symbol=1, max_qty=10000, max_notional=0
8. Set Circuit Breaker:   symbol=1, lower=9000, upper=11000, halted=0
9. Deposit:               account=1, currency=1, amount=1000000
10. Deposit:              account=1, currency=2, amount=1000000
11. Begin trading:        place limit/market orders as needed
```
